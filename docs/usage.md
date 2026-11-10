# Usage and operations

rtmoinit embeds live dashboards in a Go application using Fiber v3. Start with the [README](../README.md) for installation and a minimal integration, or run the [complete example](../examples/basic/main.go).

## Dashboard definitions and updates

Declare all dashboards, series, and tables in `Config.Definition` before calling `New(ctx, cfg)`. The definition is copied, validated, and fixed at initialization; dynamic registration is not supported. The startup context bounds initialization and Redis restore only. Call `Close(ctx)` to stop background workers.

| API | Behavior |
| --- | --- |
| `Set(seriesID, value)` | Set a gauge to a finite numeric value. |
| `Add(seriesID, delta)` | Increase a counter by a finite, nonnegative delta. Restored counters continue from their saved values. |
| `SetRows(tableID, rows)` | Replace a table's entire row set. |
| `Snapshot()` | Return an independent copy of current values, retained history, tables, and checkpoint status. |
| `Status()` | Return persistence state, the latest checkpoint error, attempt time, duration, and last success. |
| `Flush(ctx)` | Attempt a manual checkpoint; a no-op without Redis. |
| `Close(ctx)` | Reject new writes, stop workers, and attempt a final checkpoint. |

Handle update errors in your application. Use `errors.Is` with `ErrInvalid`, `ErrLimit`, and `ErrClosed` to distinguish invalid input, capacity limits, and writes after shutdown.

Numeric values use `float64` for display. They are unsuitable for accounting or integers requiring exact representation beyond IEEE 754 precision. Counters retain cumulative totals; rtmoinit does not calculate rates automatically.

Input data is copied. A returned snapshot can be read or modified independently, but it is not a transaction across multiple series.

### IDs and panels

IDs contain 1–128 characters, start with a letter or digit, and use only ASCII letters, digits, `_`, `.`, and `-`. Series and table IDs share a global namespace. Dashboard IDs are unique, and panel IDs are unique within each dashboard.

| Panel type | Definition |
| --- | --- |
| `Stat` | Reference exactly one series with `Series`. |
| `Line` | Reference one or more series with `Series`. |
| `Bar` | Reference one or more series with `Series`. |
| `TablePanel` | Reference a declared table with `Table`. |

Panel `Width` ranges from 1 to 12 grid columns and defaults to 6. Panels fill the available width on mobile. Stats show `—` until a value is supplied. Charts use the browser's local time zone. Bar charts display at most 120 bars per series by selecting one sample from each group, without summing groups.

### Tables

Declare table columns in `Definition.Tables`. Each `Column` has a `Key` and display `Label`. Use `SetRows(tableID, []rtmoinit.Row{...})` to replace rows; row keys must match declared columns.

Cells accept strings, booleans, finite numbers, or `nil`. Objects and arrays are rejected. Integer values are normalized to `float64`. Row or byte limit violations reject the entire update and preserve the previous table contents.

## Configuration and capacity

| Field | Default |
| --- | --- |
| `History` | 1 hour |
| `SampleInterval` | 1 second |
| `FlushInterval` | 10 seconds |
| `FlushTimeout` | 3 seconds, including time waiting for the checkpoint slot |
| `MaxSeries` | 100 |
| `MaxRows` | 1,000 rows per table |
| `MaxCheckpointBytes` | 10 MiB after gzip compression |
| `MaxClients` | 100 concurrent SSE connections |
| `Namespace` | `rtmoinit` |

Zero values select defaults; negative durations and limits are rejected. `SampleInterval` must be at least one millisecond. `History` must cover at least one sample and at most 1,000,000 samples per series. `MaxCheckpointBytes` must be between 1 KiB and 1 GiB. Namespace values follow the ID rules above.

Each series uses a ring buffer holding at most `floor(History / SampleInterval)` samples. Sampling records the latest gauge value or cumulative counter total. No samples are interpolated for periods when the process is stopped.

The combined JSON size of all tables cannot exceed half of `MaxCheckpointBytes`. Checkpoints exceeding the compressed limit or 16 times that limit before compression are rejected. The stored checkpoint is preserved, and runtime checkpoint failures appear in the dashboard and status API. Tune retention and capacity together as data volume increases.

## Redis checkpoints

Redis is optional. Without it, all dashboard state stays in memory. To enable it, pass a `*redis.Client` in `Config.Redis` and choose a unique `Namespace` for each application instance.

Create the client with `github.com/redis/go-redis/v9` and the standard `time` package:

```go
client := redis.NewClient(&redis.Options{
    Addr:                  "127.0.0.1:6379",
    ContextTimeoutEnabled: true,
    MaxRetries:            -1,
    DialTimeout:           3 * time.Second,
})
```

Set `Redis: client` and `Namespace: "my-service-instance-1"` in the configuration passed to `New`. The application owns the client and closes it after the monitor has finished shutting down.

`ContextTimeoutEnabled: true` is required. A dedicated client/pool with retries disabled and a finite dial timeout is recommended so checkpoints do not compete with business operations for connections. A separate Redis server provides CPU, memory, and eviction isolation; a separate database index still shares server resources.

### Write path and persistence

`Set` and `Add` only update RAM. Sampling and persistence use independent workers. A checkpoint copies each series under its own lock, then encodes JSON and compresses it outside those locks. One Redis `SET` atomically replaces `<namespace>:checkpoint`, without a TTL. `RedisKey()` returns that key.

Only one checkpoint runs at a time. The background worker does not queue work while Redis is slow. Manual `Flush(ctx)` calls wait for the slot within the configured timeout and caller deadline.

### Startup, failures, and shutdown

- **Startup:** A missing key starts an empty dashboard. An unreadable or corrupt checkpoint, incompatible version, or mismatched definition, history, or sample interval makes `New` return an error. Restore failures never overwrite the stored checkpoint.
- **Runtime:** Redis failures leave RAM updates and sampling running. The next scheduled checkpoint retries. `Status()` and `/status` expose checkpoint outcomes.
- **Shutdown:** Stop application goroutines that update the monitor, call `Close(ctx)`, then close the Redis client. `Close` waits for an in-flight checkpoint and attempts a final save. Allow at least `2 * FlushTimeout` for shutdown. An expired caller context stops waiting; cleanup continues in the background, so wait for completion before closing Redis.
- **Definition changes:** Use a new namespace to start fresh, or explicitly delete the previous checkpoint key if losing its history is acceptable. Checkpoints are not automatically migrated or deleted.

Use **one active writer per namespace**. Distributed aggregation and concurrent writers to the same key are not supported.

A crash may lose RAM updates since the last successful checkpoint. Extended Redis failures increase that window. A timeout after sending `SET` has an uncertain outcome: Redis may already have stored the complete snapshot. Durability across Redis restarts depends on its AOF/RDB and storage configuration.

## HTTP routes and authentication

Call `Mount` once, before the Fiber server starts. Every route is relative to the supplied group:

| Method | Route | Response |
| --- | --- | --- |
| GET | `/` | Embedded dashboard UI |
| GET | `/definition` | Dashboard, series, and table definitions, plus `historyMs` and `sampleIntervalMs` |
| GET | `/snapshot` | Current values, history, tables, and checkpoint status |
| GET | `/status` | Checkpoint status |
| GET | `/events` | SSE events: a full `snapshot` or a short-window `update` |

Attach your application's authentication middleware to the parent group to protect every route. rtmoinit provides read-only HTTP access; it has no built-in user accounts or remote write API. The browser's EventSource uses same-origin cookies.

When using a reverse proxy, disable buffering and compression for SSE and set the read timeout longer than the update interval.

### Live updates

SSE sends updates every second. A new connection receives a full snapshot. Subsequent updates contain current values for all series, a short overlapping history window, and complete current tables. Slow or reconnecting clients receive a fresh snapshot rather than event-log replay.

History points encode `[unixMilliseconds, value]`. Custom clients should merge points by timestamp and discard history older than `historyMs`. The embedded UI implements this behavior.

Serialized update frames are shared between clients. Connections have a five-second write deadline. Tables are sent in full, making this approach suitable for small monitoring tables.

The UI uses plain HTML, CSS, JavaScript, and SVG embedded in the Go binary, with no frontend build or CDN dependencies.

## Testing

Run from the repository root:

```sh
go mod download
gofmt -w .
test -z "$(gofmt -l .)"
go vet ./...
go test -race ./...
go build ./...
```

Go compilation checks types; `go vet` is the static check. There is no separate frontend package manager or lint command.

To run the update benchmark:

```sh
go test -run '^$' -bench BenchmarkAdd -benchmem
```

Redis integration tests skip unless `RTMOINIT_TEST_REDIS_ADDR` is set. Use a dedicated test Redis instance: the suite issues `CLIENT PAUSE` and must never target shared or production Redis. Tests use unique namespaces and remove their own keys.

```sh
RTMOINIT_TEST_REDIS_ADDR=127.0.0.1:6379 go test -race ./...
```
