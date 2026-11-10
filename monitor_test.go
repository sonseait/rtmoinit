package rtmoinit

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"
)

func testConfig() Config {
	return Config{Definition: Definition{
		Series: []Series{{ID: "count", Kind: Counter}, {ID: "load", Kind: Gauge}},
		Tables: []Table{{ID: "jobs", Columns: []Column{{Key: "name", Label: "Name"}, {Key: "active", Label: "Active"}}}},
		Dashboards: []Dashboard{{ID: "main", Title: "Test", Panels: []Panel{
			{ID: "count", Type: Stat, Series: []string{"count"}},
			{ID: "load", Type: Line, Series: []string{"load"}},
			{ID: "jobs", Type: TablePanel, Table: "jobs"},
		}}},
	}}
}

func newTestMonitor(t *testing.T, cfg Config) *Monitor {
	t.Helper()
	m, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := m.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return m
}

func TestValidation(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*Config)
	}{
		{"duplicate series", func(c *Config) { c.Definition.Series = append(c.Definition.Series, c.Definition.Series[0]) }},
		{"unknown series", func(c *Config) { c.Definition.Dashboards[0].Panels[0].Series = []string{"missing"} }},
		{"duplicate panel", func(c *Config) { c.Definition.Dashboards[0].Panels[1].ID = "count" }},
		{"invalid width", func(c *Config) { c.Definition.Dashboards[0].Panels[0].Width = 13 }},
		{"limit", func(c *Config) { c.MaxSeries = 1 }},
		{"interval", func(c *Config) { c.SampleInterval = -time.Second }},
		{"table collision", func(c *Config) { c.Definition.Tables[0].ID = "load" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := testConfig()
			test.change(&cfg)
			if m, err := New(context.Background(), cfg); err == nil {
				_ = m.Close(context.Background())
				t.Fatal("accepted invalid definition")
			}
		})
	}
	cfg := testConfig()
	m := newTestMonitor(t, cfg)
	cfg.Definition.Dashboards[0].Panels[0].Series[0] = "changed"
	if m.cfg.Definition.Dashboards[0].Panels[0].Series[0] != "count" {
		t.Fatal("definition aliases caller")
	}
	if cfg.Definition.Dashboards[0].Panels[0].Width != 0 {
		t.Fatal("default modified caller")
	}
}

func TestWritesAndOwnership(t *testing.T) {
	m := newTestMonitor(t, testConfig())
	for _, err := range []error{m.Set("count", 1), m.Add("load", 1), m.Add("count", -1), m.Add("missing", 1), m.Set("load", math.NaN()), m.Set("load", math.Inf(1)), m.SetRows("jobs", []Row{{"name": []int{1}}}), m.SetRows("jobs", []Row{{"unknown": 1}}), m.SetRows("jobs", []Row{{"name": math.Inf(1)}})} {
		if !errors.Is(err, ErrInvalid) {
			t.Fatalf("expected invalid: %v", err)
		}
	}
	rows := []Row{{"name": "worker", "active": true}}
	if err := m.SetRows("jobs", rows); err != nil {
		t.Fatal(err)
	}
	rows[0]["name"] = "modified"
	snapshot := m.Snapshot()
	if snapshot.Tables["jobs"][0]["name"] != "worker" {
		t.Fatal("rows alias caller")
	}
	snapshot.Tables["jobs"][0]["name"] = "changed snapshot"
	if m.Snapshot().Tables["jobs"][0]["name"] != "worker" {
		t.Fatal("snapshot aliases internal data")
	}
	if err := m.Set("load", math.MaxFloat64); err != nil {
		t.Fatal(err)
	}
	if err := m.Add("count", math.MaxFloat64); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(m.Add("count", math.MaxFloat64), ErrInvalid) {
		t.Fatal("accepted overflow")
	}
	if err := m.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(m.Set("load", 1), ErrClosed) {
		t.Fatal("accepted write after close")
	}
}

func TestRingAndGaps(t *testing.T) {
	cfg := testConfig()
	cfg.History = 3 * time.Second
	cfg.SampleInterval = time.Second
	m := newTestMonitor(t, cfg)
	now := time.Now().Truncate(time.Second)
	if err := m.Set("load", 1); err != nil {
		t.Fatal(err)
	}
	for i := 4; i >= 1; i-- {
		m.sample(now.Add(-time.Duration(i) * time.Second))
	}
	points := m.Snapshot().Series["load"].History
	if len(points) != 2 || points[0][0] != float64(now.Add(-2*time.Second).UnixMilli()) {
		t.Fatalf("wrong retained history: %v", points)
	}
	m.sample(now)
	if err := m.Set("load", 7); err != nil {
		t.Fatal(err)
	}
	m.sample(now)
	points = m.Snapshot().Series["load"].History
	if len(points) != 3 || points[2][1] != 7 {
		t.Fatalf("same bucket not replaced: %v", points)
	}
	// A jump must not synthesize samples during downtime.
	m.sample(now.Add(10 * time.Second))
	if got := len(m.series["load"].points); got != 3 {
		t.Fatalf("ring capacity: %d", got)
	}
}

func TestConcurrentSnapshots(t *testing.T) {
	m := newTestMonitor(t, testConfig())
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 1000 {
				if err := m.Add("count", 1); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Go(func() {
		for range 100 {
			m.sample(time.Now())
			_ = m.Snapshot()
			if err := m.SetRows("jobs", []Row{{"name": "x"}}); err != nil {
				t.Error(err)
			}
		}
	})
	wg.Wait()
	if value := *m.Snapshot().Series["count"].Value; value != 8000 {
		t.Fatalf("lost increments: %v", value)
	}
}

func TestTableLimits(t *testing.T) {
	cfg := testConfig()
	cfg.MaxRows = 1
	cfg.MaxCheckpointBytes = 1024
	m := newTestMonitor(t, cfg)
	if !errors.Is(m.SetRows("jobs", []Row{{}, {}}), ErrLimit) {
		t.Fatal("row limit ignored")
	}
	if !errors.Is(m.SetRows("jobs", []Row{{"name": string(make([]byte, 1024))}}), ErrLimit) {
		t.Fatal("byte limit ignored")
	}
}

func TestMountPrefixAndAuth(t *testing.T) {
	m := newTestMonitor(t, testConfig())
	app := fiber.New()
	m.Mount(app.Group("/private/monitor", func(c fiber.Ctx) error {
		if c.Get("Authorization") != "Bearer test" {
			return fiber.ErrUnauthorized
		}
		return c.Next()
	}))
	for _, path := range []string{"/", "/definition", "/snapshot", "/status", "/events"} {
		url := "http://example.com/private/monitor" + path
		response, err := app.Test(httptest.NewRequest("GET", url, nil))
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != 401 {
			t.Fatalf("unprotected %s: %d", path, response.StatusCode)
		}
		if path == "/events" {
			continue
		}
		req := httptest.NewRequest("GET", url, nil)
		req.Header.Set("Authorization", "Bearer test")
		response, err = app.Test(req)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != 200 {
			t.Fatalf("failed %s: %d", path, response.StatusCode)
		}
	}
}

func BenchmarkAdd(b *testing.B) {
	m, err := New(context.Background(), testConfig())
	if err != nil {
		b.Fatal(err)
	}
	defer m.Close(context.Background())
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := m.Add("count", 1); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestIncrementalWindow(t *testing.T) {
	m := newTestMonitor(t, testConfig())
	if err := m.Set("load", 2); err != nil {
		t.Fatal(err)
	}
	now := time.Now().Truncate(time.Second)
	for i := 10; i >= 0; i-- {
		m.sample(now.Add(-time.Duration(i) * time.Second))
	}
	full := m.Snapshot().Series["load"].History
	delta := m.snapshot(now.Add(-3 * time.Second).UnixMilli()).Series["load"].History
	if len(full) != 11 || len(delta) != 3 {
		t.Fatalf("wrong window lengths: full=%d delta=%d", len(full), len(delta))
	}
	for i, p := range delta {
		if p != full[len(full)-len(delta)+i] {
			t.Fatal("window order mismatch")
		}
	}
}

func BenchmarkAddDuringSnapshot(b *testing.B) {
	cfg := testConfig()
	for i := 2; i < 100; i++ {
		cfg.Definition.Series = append(cfg.Definition.Series, Series{ID: fmt.Sprintf("series-%d", i), Kind: Gauge})
	}
	m, err := New(context.Background(), cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer m.Close(context.Background())
	for _, s := range m.series {
		s.mu.Lock()
		s.hasValue = true
		for i := 0; i < 3600; i++ {
			s.points = append(s.points, Point{float64(time.Now().Add(time.Duration(i-3599) * time.Second).UnixMilli()), float64(i)})
		}
		s.mu.Unlock()
	}
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				_ = m.Snapshot()
			}
		}
	}()
	defer func() { close(stop); <-done }()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := m.Add("count", 1); err != nil {
				b.Fatal(err)
			}
		}
	})
}
