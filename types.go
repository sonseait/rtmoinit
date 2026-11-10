// Package rtmoinit provides embedded, RAM-first realtime dashboards for Fiber v3.
package rtmoinit

import (
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	ErrClosed  = errors.New("rtmoinit: closed")
	ErrInvalid = errors.New("rtmoinit: invalid input")
	ErrLimit   = errors.New("rtmoinit: limit exceeded")
)

type Kind string

const (
	Gauge   Kind = "gauge"
	Counter Kind = "counter"
)

type PanelType string

const (
	Stat       PanelType = "stat"
	Line       PanelType = "line"
	Bar        PanelType = "bar"
	TablePanel PanelType = "table"
)

type Series struct {
	ID    string `json:"id"`
	Title string `json:"title"`
	Unit  string `json:"unit,omitempty"`
	Kind  Kind   `json:"kind"`
}

type Column struct {
	Key   string `json:"key"`
	Label string `json:"label"`
}

type Table struct {
	ID      string   `json:"id"`
	Columns []Column `json:"columns"`
}

type Panel struct {
	ID     string    `json:"id"`
	Title  string    `json:"title"`
	Type   PanelType `json:"type"`
	Series []string  `json:"series,omitempty"`
	Table  string    `json:"table,omitempty"`
	// Width is 1..12 grid columns; zero defaults to 6.
	Width int `json:"width"`
}

type Dashboard struct {
	ID     string  `json:"id"`
	Title  string  `json:"title"`
	Panels []Panel `json:"panels"`
}

// Definition is immutable after New. Series and table IDs are globally unique.
type Definition struct {
	Dashboards []Dashboard `json:"dashboards"`
	Series     []Series    `json:"series"`
	Tables     []Table     `json:"tables"`
}

// Config owns no external resources. The caller closes Redis after Monitor.Close.
type Config struct {
	Definition         Definition
	Redis              *redis.Client
	Namespace          string
	History            time.Duration
	SampleInterval     time.Duration
	FlushInterval      time.Duration
	FlushTimeout       time.Duration
	MaxSeries          int
	MaxRows            int
	MaxCheckpointBytes int
	MaxClients         int
}

// Point encodes [Unix milliseconds, value].
type Point [2]float64

type SeriesData struct {
	Value   *float64 `json:"value"`
	History []Point  `json:"history"`
}

// Row accepts JSON scalar cells: strings, booleans, finite numbers, and nil.
type Row map[string]any

type Status struct {
	Persistence    bool       `json:"persistence"`
	LastSuccess    *time.Time `json:"lastSuccess"`
	LastAttempt    *time.Time `json:"lastAttempt"`
	LastDurationMS int64      `json:"lastDurationMs"`
	LastError      string     `json:"lastError,omitempty"`
}

type Snapshot struct {
	Time   int64                 `json:"time"`
	Series map[string]SeriesData `json:"series"`
	Tables map[string][]Row      `json:"tables"`
	Status Status                `json:"status"`
}
