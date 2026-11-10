package rtmoinit

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func redisConfig(t *testing.T) Config {
	t.Helper()
	address := os.Getenv("RTMOINIT_TEST_REDIS_ADDR")
	if address == "" {
		t.Skip("set RTMOINIT_TEST_REDIS_ADDR to a dedicated test Redis")
	}
	client := redis.NewClient(&redis.Options{Addr: address, ContextTimeoutEnabled: true, MaxRetries: -1, DialTimeout: time.Second})
	cfg := testConfig()
	cfg.Redis = client
	cfg.Namespace = fmt.Sprintf("rtmoinit-test-%d", time.Now().UnixNano())
	cfg.FlushInterval = time.Hour
	t.Cleanup(func() { _ = client.Del(context.Background(), cfg.Namespace+":checkpoint").Err(); _ = client.Close() })
	return cfg
}

func TestRedisRoundTrip(t *testing.T) {
	cfg := redisConfig(t)
	m, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = m.Add("count", 12); err != nil {
		t.Fatal(err)
	}
	if err = m.Set("load", 2.5); err != nil {
		t.Fatal(err)
	}
	if err = m.SetRows("jobs", []Row{{"name": "restored", "active": true}}); err != nil {
		t.Fatal(err)
	}
	m.sample(time.Now().Add(-time.Second))
	if err = m.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	raw, err := cfg.Redis.Get(context.Background(), m.RedisKey()).Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) < 2 || raw[0] != 0x1f || raw[1] != 0x8b {
		t.Fatal("checkpoint must be gzip JSON")
	}
	restored, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := restored.Snapshot()
	if *snapshot.Series["count"].Value != 12 || *snapshot.Series["load"].Value != 2.5 || len(snapshot.Series["load"].History) != 1 || snapshot.Tables["jobs"][0]["name"] != "restored" {
		t.Fatalf("restore mismatch: %+v", snapshot)
	}
	if err = restored.Add("count", 1); err != nil {
		t.Fatal(err)
	}
	if *restored.Snapshot().Series["count"].Value != 13 {
		t.Fatal("counter reset after restore")
	}
	if err = restored.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	cfg.Definition.Dashboards[0].Title = "changed"
	if unexpected, err := New(context.Background(), cfg); err == nil {
		_ = unexpected.Close(context.Background())
		t.Fatal("accepted incompatible definition")
	}
	before, err := cfg.Redis.Get(context.Background(), m.RedisKey()).Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if err = cfg.Redis.Set(context.Background(), m.RedisKey(), "corrupt", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if unexpected, err := New(context.Background(), cfg); err == nil {
		_ = unexpected.Close(context.Background())
		t.Fatal("accepted corrupt checkpoint")
	}
	after, err := cfg.Redis.Get(context.Background(), m.RedisKey()).Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != "corrupt" || bytes.Equal(before, after) {
		t.Fatal("failed startup overwrote stored checkpoint")
	}
}

// The hook holds the actual Redis SET while the test exercises the RAM write path.
type delayedSet struct {
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (h *delayedSet) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *delayedSet) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *delayedSet) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == "set" && h.armed.CompareAndSwap(true, false) {
			close(h.entered)
			select {
			case <-h.release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return next(ctx, cmd)
	}
}

func TestRedisSlowCheckpointDoesNotBlockWritesOrSampling(t *testing.T) {
	cfg := redisConfig(t)
	cfg.SampleInterval = 10 * time.Millisecond
	hook := &delayedSet{entered: make(chan struct{}), release: make(chan struct{})}
	cfg.Redis.AddHook(hook)
	m := newTestMonitor(t, cfg)
	if err := m.Add("count", 1); err != nil {
		t.Fatal(err)
	}
	hook.armed.Store(true)
	flushed := make(chan error, 1)
	go func() { flushed <- m.Flush(context.Background()) }()
	select {
	case <-hook.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("checkpoint did not reach Redis")
	}
	defer close(hook.release)
	wrote := make(chan error, 1)
	go func() {
		for range 1000 {
			if err := m.Add("count", 1); err != nil {
				wrote <- err
				return
			}
		}
		wrote <- nil
	}()
	select {
	case err := <-wrote:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("Redis blocked RAM writes")
	}
	deadline := time.Now().Add(time.Second)
	for len(m.Snapshot().Series["count"].History) < 3 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if len(m.Snapshot().Series["count"].History) < 3 {
		t.Fatal("Redis blocked sampling")
	}
	short, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := m.Flush(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("concurrent flush ignored context: %v", err)
	}
	// Leave the held flush to finish on release before cleanup closes the monitor.
	t.Cleanup(func() {
		if err := <-flushed; err != nil {
			t.Error(err)
		}
	})
}

func TestRedisTimeoutAndRecovery(t *testing.T) {
	cfg := redisConfig(t)
	cfg.FlushTimeout = 80 * time.Millisecond
	m := newTestMonitor(t, cfg)
	if err := m.Add("count", 1); err != nil {
		t.Fatal(err)
	}
	if err := m.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	var err error
	if err = cfg.Redis.ClientPause(context.Background(), 300*time.Millisecond).Err(); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err = m.Flush(context.Background()); err == nil {
		t.Fatal("expected checkpoint timeout")
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("checkpoint ignored timeout")
	}
	if m.Status().LastError == "" {
		t.Fatal("checkpoint failure invisible")
	}
	if err = m.Add("count", 1); err != nil {
		t.Fatal(err)
	}
	if unexpected, err := New(context.Background(), cfg); err == nil {
		_ = unexpected.Close(context.Background())
		t.Fatal("startup accepted unavailable Redis")
	}
	time.Sleep(350 * time.Millisecond)
	after, err := cfg.Redis.Get(context.Background(), m.RedisKey()).Bytes()
	if err != nil {
		t.Fatal(err)
	}
	// A timeout after SET was sent has an ambiguous outcome; either complete
	// snapshot is valid, but a partial or corrupt value is never acceptable.
	reader, err := gzip.NewReader(bytes.NewReader(after))
	if err != nil {
		t.Fatal(err)
	}
	var saved checkpoint
	err = json.NewDecoder(reader).Decode(&saved)
	reader.Close()
	if err != nil || saved.Version != 1 || saved.Signature != m.signature || saved.Series["count"].Value == nil || *saved.Series["count"].Value != 1 {
		t.Fatalf("invalid checkpoint after timeout: %v", err)
	}
	if err = m.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if m.Status().LastError != "" || m.Status().LastSuccess == nil {
		t.Fatal("recovery not reported")
	}
}

func TestRedisUnavailableAtStartup(t *testing.T) {
	// Reserve and release a port instead of assuming a particular unused port.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	cfg := testConfig()
	cfg.FlushTimeout = 100 * time.Millisecond
	cfg.Redis = redis.NewClient(&redis.Options{Addr: address, ContextTimeoutEnabled: true, MaxRetries: -1, DialTimeout: 100 * time.Millisecond})
	defer cfg.Redis.Close()
	if m, err := New(context.Background(), cfg); err == nil {
		_ = m.Close(context.Background())
		t.Fatal("startup succeeded without configured Redis")
	}
}

func TestRedisRequiresContextTimeouts(t *testing.T) {
	cfg := testConfig()
	cfg.Redis = redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	defer cfg.Redis.Close()
	if _, err := New(context.Background(), cfg); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unsafe client accepted: %v", err)
	}
}

func TestRedisCheckpointLimitsPreserveStoredValue(t *testing.T) {
	cfg := redisConfig(t)
	cfg.MaxCheckpointBytes = 1024
	m, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close(context.Background()) })
	if err = m.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	previous, err := cfg.Redis.Get(context.Background(), m.RedisKey()).Bytes()
	if err != nil {
		t.Fatal(err)
	}
	s := m.series["load"]
	s.mu.Lock()
	s.hasValue = true
	for i := 0; i < 1000; i++ {
		s.points = append(s.points, Point{float64(time.Now().Add(time.Duration(i-1000) * time.Second).UnixMilli()), math.Sin(float64(i))})
	}
	s.next = len(s.points)
	s.mu.Unlock()
	if err = m.Flush(context.Background()); !errors.Is(err, ErrLimit) {
		t.Fatalf("checkpoint limit ignored: %v", err)
	}
	after, err := cfg.Redis.Get(context.Background(), m.RedisKey()).Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(previous, after) {
		t.Fatal("oversized checkpoint replaced stored value")
	}
	if err = m.Close(context.Background()); !errors.Is(err, ErrLimit) {
		t.Fatalf("final checkpoint failure was hidden: %v", err)
	}
	if err = cfg.Redis.Set(context.Background(), m.RedisKey(), make([]byte, 1025), 0).Err(); err != nil {
		t.Fatal(err)
	}
	if unexpected, err := New(context.Background(), cfg); !errors.Is(err, ErrLimit) {
		if unexpected != nil {
			_ = unexpected.Close(context.Background())
		}
		t.Fatalf("restore accepted oversized value: %v", err)
	}
}
