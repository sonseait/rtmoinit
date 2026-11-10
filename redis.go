package rtmoinit

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"time"
)

type checkpoint struct {
	Version   int                   `json:"version"`
	Signature string                `json:"signature"`
	CreatedAt int64                 `json:"createdAt"`
	Series    map[string]SeriesData `json:"series"`
	Tables    map[string][]Row      `json:"tables"`
}

// RedisKey is the single atomic snapshot key. Use a unique namespace per instance.
func (m *Monitor) RedisKey() string { return m.cfg.Namespace + ":checkpoint" }

// Flush serializes outside data locks and atomically replaces the Redis snapshot.
// Concurrent flushes wait for one slot with context cancellation; none are queued by the worker.
func (m *Monitor) Flush(ctx context.Context) (err error) {
	if m.cfg.Redis == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, m.cfg.FlushTimeout)
	defer cancel()
	select {
	case m.flushSlot <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-m.flushSlot }()
	start := time.Now()
	defer func() {
		finished := time.Now()
		m.statusMu.Lock()
		defer m.statusMu.Unlock()
		m.status.LastAttempt = &start
		m.status.LastDurationMS = finished.Sub(start).Milliseconds()
		if err != nil {
			m.status.LastError = err.Error()
		} else {
			m.status.LastSuccess = &finished
			m.status.LastError = ""
		}
	}()
	snapshot := m.Snapshot()
	var buffer bytes.Buffer
	compressed, err := gzip.NewWriterLevel(&buffer, gzip.BestSpeed)
	if err != nil {
		return err
	}
	err = json.NewEncoder(&limitedWriter{writer: compressed, remaining: int64(m.cfg.MaxCheckpointBytes) * 16}).Encode(checkpoint{Version: 1, Signature: m.signature, CreatedAt: snapshot.Time, Series: snapshot.Series, Tables: snapshot.Tables})
	closeErr := compressed.Close()
	if err != nil {
		return fmt.Errorf("rtmoinit: encode checkpoint: %w", err)
	}
	if closeErr != nil {
		return closeErr
	}
	raw := buffer.Bytes()
	if len(raw) > m.cfg.MaxCheckpointBytes {
		return fmt.Errorf("%w: compressed checkpoint is %d bytes (maximum %d)", ErrLimit, len(raw), m.cfg.MaxCheckpointBytes)
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = m.cfg.Redis.Set(ctx, m.RedisKey(), raw, 0).Err(); err != nil {
		return fmt.Errorf("rtmoinit: checkpoint: %w", err)
	}
	return nil
}

func (m *Monitor) restore(ctx context.Context) error {
	// GETRANGE bounds allocation even when another client has stored an oversized value.
	raw, err := m.cfg.Redis.GetRange(ctx, m.RedisKey(), 0, int64(m.cfg.MaxCheckpointBytes)).Bytes()
	if err != nil {
		return fmt.Errorf("rtmoinit: restore: %w", err)
	}
	if len(raw) == 0 {
		exists, err := m.cfg.Redis.Exists(ctx, m.RedisKey()).Result()
		if err != nil {
			return fmt.Errorf("rtmoinit: restore: %w", err)
		}
		if exists == 0 {
			return nil
		}
	}
	if len(raw) > m.cfg.MaxCheckpointBytes {
		return fmt.Errorf("%w: stored checkpoint bytes", ErrLimit)
	}
	var saved checkpoint
	reader, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("rtmoinit: corrupt checkpoint: %w", err)
	}
	defer reader.Close()
	decoded, err := io.ReadAll(io.LimitReader(reader, int64(m.cfg.MaxCheckpointBytes)*16+1))
	if err != nil {
		return fmt.Errorf("rtmoinit: corrupt checkpoint: %w", err)
	}
	if int64(len(decoded)) > int64(m.cfg.MaxCheckpointBytes)*16 {
		return fmt.Errorf("%w: expanded checkpoint", ErrLimit)
	}
	if err = json.Unmarshal(decoded, &saved); err != nil {
		return fmt.Errorf("rtmoinit: corrupt checkpoint: %w", err)
	}
	if saved.Version != 1 || saved.Signature != m.signature {
		return fmt.Errorf("%w: checkpoint version or definition mismatch; use a new namespace or explicitly delete %s", ErrInvalid, m.RedisKey())
	}
	if len(saved.Series) != len(m.series) || len(saved.Tables) != len(m.tables) {
		return fmt.Errorf("%w: checkpoint data keys", ErrInvalid)
	}
	now := time.Now()
	cutoff := float64(now.Add(-m.cfg.History).UnixMilli())
	capacity := int(m.cfg.History / m.cfg.SampleInterval)
	for id, data := range saved.Series {
		s := m.series[id]
		if s == nil || len(data.History) > capacity {
			return fmt.Errorf("%w: checkpoint series %q", ErrInvalid, id)
		}
		if data.Value != nil {
			v := *data.Value
			if math.IsNaN(v) || math.IsInf(v, 0) || (s.kind == Counter && v < 0) {
				return fmt.Errorf("%w: checkpoint value", ErrInvalid)
			}
			s.value, s.hasValue = v, true
		} else if len(data.History) > 0 {
			return fmt.Errorf("%w: history without value", ErrInvalid)
		}
		previous := float64(0)
		for _, p := range data.History {
			if math.IsNaN(p[0]) || math.IsInf(p[0], 0) || math.IsNaN(p[1]) || math.IsInf(p[1], 0) || p[0] <= previous || p[0] > float64(now.Add(m.cfg.SampleInterval).UnixMilli()) || (s.kind == Counter && p[1] < 0) {
				return fmt.Errorf("%w: checkpoint history", ErrInvalid)
			}
			previous = p[0]
			if p[0] > cutoff {
				s.points = append(s.points, p)
			}
		}
		s.next = len(s.points) % capacity
	}
	for id, rows := range saved.Tables {
		if err = m.SetRows(id, rows); err != nil {
			return fmt.Errorf("rtmoinit: restore table: %w", err)
		}
	}
	if saved.CreatedAt <= 0 || saved.CreatedAt > now.Add(m.cfg.SampleInterval).UnixMilli() {
		return fmt.Errorf("%w: checkpoint timestamp", ErrInvalid)
	}
	last := time.UnixMilli(saved.CreatedAt)
	m.status.LastSuccess = &last
	return nil
}

type limitedWriter struct {
	writer    io.Writer
	remaining int64
}

func (w *limitedWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > w.remaining {
		return 0, fmt.Errorf("%w: expanded checkpoint", ErrLimit)
	}
	n, err := w.writer.Write(p)
	w.remaining -= int64(n)
	return n, err
}
