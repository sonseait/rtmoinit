package rtmoinit

import (
	"bufio"
	_ "embed"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gofiber/fiber/v3"
)

//go:embed ui.html
var ui []byte

// Mount adds read-only routes relative to router. Install authentication on the
// parent group first. Mount once, before the Fiber application starts serving.
func (m *Monitor) Mount(router fiber.Router) {
	router.Get("/", func(c fiber.Ctx) error {
		c.Set("Cache-Control", "no-store")
		c.Set("X-Content-Type-Options", "nosniff")
		c.Set("Content-Security-Policy", "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; base-uri 'none'; frame-ancestors 'self'")
		return c.Type("html").Send(ui)
	})
	router.Get("/definition", func(c fiber.Ctx) error {
		c.Set("Cache-Control", "no-store")
		return c.JSON(struct {
			Definition
			HistoryMS        int64 `json:"historyMs"`
			SampleIntervalMS int64 `json:"sampleIntervalMs"`
		}{m.cfg.Definition, m.cfg.History.Milliseconds(), m.cfg.SampleInterval.Milliseconds()})
	})
	router.Get("/snapshot", func(c fiber.Ctx) error { c.Set("Cache-Control", "no-store"); return c.JSON(m.Snapshot()) })
	router.Get("/status", func(c fiber.Ctx) error { c.Set("Cache-Control", "no-store"); return c.JSON(m.Status()) })
	router.Get("/events", m.events)
}

func (m *Monitor) events(c fiber.Ctx) error {
	select {
	case <-m.stop:
		return fiber.ErrServiceUnavailable
	default:
	}
	select {
	case m.clients <- struct{}{}:
	default:
		return fiber.ErrTooManyRequests
	}
	c.Set("Content-Type", "text/event-stream")
	c.Set("Cache-Control", "no-cache, no-transform")
	c.Set("X-Accel-Buffering", "no")
	conn := c.RequestCtx().Conn()
	return c.SendStreamWriter(func(w *bufio.Writer) {
		if conn != nil {
			defer func() { _ = conn.SetWriteDeadline(time.Time{}) }()
		}
		defer func() { <-m.clients }()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		var lastSent int64
		for {
			event := "update"
			payload, at, err := m.liveFrame()
			if lastSent == 0 || lastSent < at-3000 {
				snapshot := m.Snapshot()
				payload, err = json.Marshal(snapshot)
				at, event = snapshot.Time, "snapshot"
			}

			if err != nil {
				return
			}
			if conn != nil {
				if err = conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
					return
				}
			}
			if _, err = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, payload); err != nil {
				return
			}
			if err = w.Flush(); err != nil {
				return
			}
			lastSent = at
			select {
			case <-m.stop:
				return
			case <-ticker.C:
			}
		}
	})
}

// Share a short overlapping history window. Slow/reconnecting clients receive
// a full snapshot; regular clients avoid downloading an hour of history every second.
func (m *Monitor) liveFrame() ([]byte, int64, error) {
	m.frameMu.Lock()
	defer m.frameMu.Unlock()
	now := time.Now()
	if m.frameAt.Unix() == now.Unix() {
		return m.frame, m.frameTimestamp, nil
	}
	snapshot := m.snapshot(time.Now().Add(-3 * time.Second).UnixMilli())
	frame, err := json.Marshal(snapshot)
	if err != nil {
		return nil, 0, err
	}
	m.frame, m.frameAt, m.frameTimestamp = frame, now, snapshot.Time
	return frame, snapshot.Time, nil
}
