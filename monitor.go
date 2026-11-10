package rtmoinit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"slices"
	"sync"
	"time"
)

type seriesState struct {
	mu       sync.Mutex
	kind     Kind
	value    float64
	hasValue bool
	points   []Point
	next     int
}

type tableState struct {
	columns map[string]bool
	rows    []Row
	bytes   int
}

type Monitor struct {
	cfg            Config
	signature      string
	series         map[string]*seriesState
	tables         map[string]*tableState
	tableMu        sync.Mutex
	tableBytes     int
	gate           sync.RWMutex
	closed         bool
	stop           chan struct{}
	done           chan struct{}
	closeErr       error
	flushSlot      chan struct{}
	clients        chan struct{}
	frameMu        sync.Mutex
	frameAt        time.Time
	frameTimestamp int64
	frame          []byte
	statusMu       sync.Mutex
	status         Status
}

var identifier = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$`)

// New validates the entire definition and restores Redis before starting workers.
// ctx bounds startup only; Close owns the worker lifecycle.
func New(ctx context.Context, cfg Config) (*Monitor, error) {
	if err := defaults(&cfg); err != nil {
		return nil, err
	}
	// Detach nested slices from the caller before applying defaults or serving HTTP.
	raw, err := json.Marshal(cfg.Definition)
	if err != nil {
		return nil, fmt.Errorf("%w: definition: %v", ErrInvalid, err)
	}
	cfg.Definition = Definition{}
	if err = json.Unmarshal(raw, &cfg.Definition); err != nil {
		return nil, err
	}
	m := &Monitor{cfg: cfg, series: make(map[string]*seriesState), tables: make(map[string]*tableState), stop: make(chan struct{}), done: make(chan struct{}), flushSlot: make(chan struct{}, 1), clients: make(chan struct{}, cfg.MaxClients)}
	if err = m.validate(); err != nil {
		return nil, err
	}
	raw, err = json.Marshal(struct {
		Definition              Definition
		History, SampleInterval time.Duration
	}{m.cfg.Definition, cfg.History, cfg.SampleInterval})
	if err != nil {
		return nil, err
	}
	hash := sha256.Sum256(raw)
	m.signature = hex.EncodeToString(hash[:])
	m.status.Persistence = cfg.Redis != nil
	if cfg.Redis != nil {
		restoreCtx, cancel := context.WithTimeout(ctx, cfg.FlushTimeout)
		err = m.restore(restoreCtx)
		cancel()
		if err != nil {
			return nil, err
		}
	}
	go m.run()
	return m, nil
}

func defaults(c *Config) error {
	if c.Namespace == "" {
		c.Namespace = "rtmoinit"
	}
	if !identifier.MatchString(c.Namespace) {
		return fmt.Errorf("%w: namespace", ErrInvalid)
	}
	if c.Redis != nil && !c.Redis.Options().ContextTimeoutEnabled {
		return fmt.Errorf("%w: Redis client requires ContextTimeoutEnabled=true", ErrInvalid)
	}
	if c.History == 0 {
		c.History = time.Hour
	}
	if c.SampleInterval == 0 {
		c.SampleInterval = time.Second
	}
	if c.FlushInterval == 0 {
		c.FlushInterval = 10 * time.Second
	}
	if c.FlushTimeout == 0 {
		c.FlushTimeout = 3 * time.Second
	}
	if c.MaxSeries == 0 {
		c.MaxSeries = 100
	}
	if c.MaxRows == 0 {
		c.MaxRows = 1000
	}
	if c.MaxCheckpointBytes == 0 {
		c.MaxCheckpointBytes = 10 << 20
	}
	if c.MaxClients == 0 {
		c.MaxClients = 100
	}
	if c.SampleInterval < time.Millisecond || c.History < c.SampleInterval || c.History/c.SampleInterval > 1_000_000 || c.FlushInterval <= 0 || c.FlushTimeout <= 0 || c.MaxSeries < 1 || c.MaxRows < 1 || c.MaxCheckpointBytes < 1024 || c.MaxCheckpointBytes > 1<<30 || c.MaxClients < 1 {
		return fmt.Errorf("%w: invalid duration or resource limit", ErrInvalid)
	}
	return nil
}

func (m *Monitor) validate() error {
	d := &m.cfg.Definition
	if len(d.Dashboards) == 0 {
		return fmt.Errorf("%w: at least one dashboard required", ErrInvalid)
	}
	if len(d.Series) > m.cfg.MaxSeries {
		return fmt.Errorf("%w: series", ErrLimit)
	}
	for _, s := range d.Series {
		if !identifier.MatchString(s.ID) || m.series[s.ID] != nil || (s.Kind != Gauge && s.Kind != Counter) {
			return fmt.Errorf("%w: series %q", ErrInvalid, s.ID)
		}
		m.series[s.ID] = &seriesState{kind: s.Kind}
	}
	for _, t := range d.Tables {
		if !identifier.MatchString(t.ID) || m.tables[t.ID] != nil || m.series[t.ID] != nil || len(t.Columns) == 0 {
			return fmt.Errorf("%w: table %q", ErrInvalid, t.ID)
		}
		columns := make(map[string]bool)
		for _, c := range t.Columns {
			if !identifier.MatchString(c.Key) || columns[c.Key] {
				return fmt.Errorf("%w: column %q", ErrInvalid, c.Key)
			}
			columns[c.Key] = true
		}
		m.tables[t.ID] = &tableState{columns: columns, rows: []Row{}}
	}
	seen := make(map[string]bool)
	for i := range d.Dashboards {
		dashboard := &d.Dashboards[i]
		if !identifier.MatchString(dashboard.ID) || seen[dashboard.ID] || len(dashboard.Panels) == 0 {
			return fmt.Errorf("%w: dashboard %q", ErrInvalid, dashboard.ID)
		}
		seen[dashboard.ID] = true
		panels := make(map[string]bool)
		for j := range dashboard.Panels {
			p := &dashboard.Panels[j]
			if p.Width == 0 {
				p.Width = 6
			}
			if !identifier.MatchString(p.ID) || panels[p.ID] || p.Width < 1 || p.Width > 12 {
				return fmt.Errorf("%w: panel %q", ErrInvalid, p.ID)
			}
			panels[p.ID] = true
			switch p.Type {
			case TablePanel:
				if m.tables[p.Table] == nil || len(p.Series) != 0 {
					return fmt.Errorf("%w: table panel %q", ErrInvalid, p.ID)
				}
			case Stat, Line, Bar:
				if len(p.Series) == 0 || p.Table != "" || (p.Type == Stat && len(p.Series) != 1) {
					return fmt.Errorf("%w: series panel %q", ErrInvalid, p.ID)
				}
				used := make(map[string]bool)
				for _, id := range p.Series {
					if m.series[id] == nil || used[id] {
						return fmt.Errorf("%w: panel series %q", ErrInvalid, id)
					}
					used[id] = true
				}
			default:
				return fmt.Errorf("%w: panel type %q", ErrInvalid, p.Type)
			}
		}
	}
	return nil
}

func (m *Monitor) Set(id string, value float64) error { return m.write(id, value, false) }
func (m *Monitor) Add(id string, delta float64) error { return m.write(id, delta, true) }

func (m *Monitor) write(id string, value float64, add bool) error {
	m.gate.RLock()
	defer m.gate.RUnlock()
	if m.closed {
		return ErrClosed
	}
	s := m.series[id]
	if s == nil || math.IsNaN(value) || math.IsInf(value, 0) || (add && (s.kind != Counter || value < 0)) || (!add && s.kind != Gauge) {
		return fmt.Errorf("%w: series update %q", ErrInvalid, id)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if add {
		value += s.value
	}
	if math.IsInf(value, 0) {
		return fmt.Errorf("%w: counter overflow", ErrInvalid)
	}
	s.value, s.hasValue = value, true
	return nil
}

func cloneRows(rows []Row) []Row {
	out := make([]Row, len(rows))
	for i, row := range rows {
		out[i] = make(Row, len(row))
		for key, value := range row {
			out[i][key] = value
		}
	}
	return out
}

func (m *Monitor) SetRows(id string, rows []Row) error {
	m.gate.RLock()
	defer m.gate.RUnlock()
	if m.closed {
		return ErrClosed
	}
	t := m.tables[id]
	if t == nil {
		return fmt.Errorf("%w: table %q", ErrInvalid, id)
	}
	if len(rows) > m.cfg.MaxRows {
		return fmt.Errorf("%w: table rows", ErrLimit)
	}
	for _, row := range rows {
		for key, value := range row {
			if !t.columns[key] {
				return fmt.Errorf("%w: column %q", ErrInvalid, key)
			}
			switch value.(type) {
			case nil, string, bool, float64, float32, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, json.Number:
			default:
				return fmt.Errorf("%w: table cells must be JSON scalars", ErrInvalid)
			}
		}
	}
	raw, err := json.Marshal(rows)
	if err != nil {
		return fmt.Errorf("%w: table: %v", ErrInvalid, err)
	}
	// ponytail: tables share half the checkpoint budget; add paged storage for larger datasets.
	m.tableMu.Lock()
	defer m.tableMu.Unlock()
	if len(raw) > m.cfg.MaxCheckpointBytes/2 || m.tableBytes-t.bytes+len(raw) > m.cfg.MaxCheckpointBytes/2 {
		return fmt.Errorf("%w: table bytes", ErrLimit)
	}
	// Normalize numbers and detach all cells from caller-owned values.
	var copied []Row
	if err = json.Unmarshal(raw, &copied); err != nil {
		return fmt.Errorf("%w: table: %v", ErrInvalid, err)
	}
	if copied == nil {
		copied = []Row{}
	}
	m.tableBytes += len(raw) - t.bytes
	t.rows, t.bytes = copied, len(raw)
	return nil
}

func (m *Monitor) sample(now time.Time) {
	at := float64(now.Truncate(m.cfg.SampleInterval).UnixMilli())
	capacity := int(m.cfg.History / m.cfg.SampleInterval)
	for _, s := range m.series {
		s.mu.Lock()
		if s.hasValue {
			p := Point{at, s.value}
			n := len(s.points)
			last := (s.next + n - 1) % max(n, 1)
			if n > 0 && s.points[last][0] == at {
				s.points[last] = p
			} else if n < capacity {
				s.points = append(s.points, p)
				s.next = len(s.points) % capacity
			} else {
				s.points[s.next] = p
				s.next = (s.next + 1) % capacity
			}
		}
		s.mu.Unlock()
	}
}

// Snapshot copies data one series at a time; it is not a cross-series transaction.
func (m *Monitor) Snapshot() Snapshot { return m.snapshot(0) }

func (m *Monitor) snapshot(since int64) Snapshot {
	now := time.Now()
	out := Snapshot{Time: now.UnixMilli(), Series: make(map[string]SeriesData, len(m.series)), Tables: make(map[string][]Row, len(m.tables)), Status: m.Status()}
	cutoff := float64(max(since, now.Add(-m.cfg.History).UnixMilli()))
	capacity := int(m.cfg.History / m.cfg.SampleInterval)
	for id, s := range m.series {
		s.mu.Lock()
		allocation := len(s.points)
		if since > 0 {
			allocation = min(allocation, 10)
		}
		data := SeriesData{History: make([]Point, 0, allocation)}
		if s.hasValue {
			value := s.value
			data.Value = &value
		}
		start := 0
		if len(s.points) == capacity {
			start = s.next
		}
		for i := len(s.points) - 1; i >= 0; i-- {
			p := s.points[(start+i)%len(s.points)]
			if p[0] <= cutoff {
				break
			}
			data.History = append(data.History, p)
		}
		slices.Reverse(data.History)
		s.mu.Unlock()
		out.Series[id] = data
	}
	m.tableMu.Lock()
	for id, t := range m.tables {
		out.Tables[id] = cloneRows(t.rows)
	}
	m.tableMu.Unlock()
	return out
}

func (m *Monitor) Status() Status {
	m.statusMu.Lock()
	defer m.statusMu.Unlock()
	out := m.status
	if out.LastSuccess != nil {
		t := *out.LastSuccess
		out.LastSuccess = &t
	}
	if out.LastAttempt != nil {
		t := *out.LastAttempt
		out.LastAttempt = &t
	}
	return out
}

func (m *Monitor) run() {
	defer close(m.done)
	ticker := time.NewTicker(m.cfg.SampleInterval)
	defer ticker.Stop()
	// Persistence has its own goroutine so slow Redis cannot stop sampling.
	persistenceDone := make(chan struct{})
	go func() {
		defer close(persistenceDone)
		if m.cfg.Redis == nil {
			return
		}
		ticker := time.NewTicker(m.cfg.FlushInterval)
		defer ticker.Stop()
		for {
			select {
			case <-m.stop:
				return
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(context.Background(), m.cfg.FlushTimeout)
				_ = m.Flush(ctx)
				cancel()
			}
		}
	}()
	for {
		select {
		case now := <-ticker.C:
			m.sample(now)
		case <-m.stop:
			<-persistenceDone
			ctx, cancel := context.WithTimeout(context.Background(), m.cfg.FlushTimeout)
			m.closeErr = m.Flush(ctx)
			cancel()
			return
		}
	}
}

// Close rejects new writes, stops sampling, and attempts a final checkpoint.
// A deadline can stop waiting; the bounded shutdown continues in the background.
func (m *Monitor) Close(ctx context.Context) error {
	m.gate.Lock()
	if !m.closed {
		m.closed = true
		close(m.stop)
	}
	m.gate.Unlock()
	select {
	case <-m.done:
		return m.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}
