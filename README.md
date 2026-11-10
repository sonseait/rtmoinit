# rtmoinit

**Live dashboards, built into your Go application.**

rtmoinit is an embeddable monitoring dashboard for Go and Fiber v3. Turn application metrics and operational data into live stats, charts, and tables, served directly from your application.

Define dashboards in Go, update values in memory, and open them in a browser. The UI ships inside your binary, with no frontend build, CDN, or separate dashboard server. Add Redis when you want to restore dashboard state after a restart.

## What you can build

- **Service overviews** with request counters, latency trends, and current health indicators supplied by your application.
- **Worker dashboards** showing job totals, queue depth, and worker status tables.
- **Internal operations views** with multiple dashboards and layouts defined alongside your application code.

Your application supplies the data through a small Go API; rtmoinit handles sampling, recent history, and live browser updates.

## Features

- **Stats, line charts, bar charts, and tables.** Combine gauges, cumulative counters, and structured rows in compact, orange/black terminal-style dashboards.
- **Live browser updates.** Server-Sent Events refresh connected dashboards every second and restore the view after reconnecting.
- **Memory-first writes.** Metric updates stay in RAM; optional Redis checkpoints run independently of sampling.
- **Bounded retention and capacity.** Configure history, series, table, checkpoint, and connection limits.
- **Optional restart recovery.** Save compressed snapshots to Redis and restore compatible dashboard state at startup.
- **Fits your existing app.** Mount read-only UI and JSON routes under a Fiber group protected by your authentication middleware.

## Try it locally

Requires Go 1.27.4 or later.

```sh
git clone https://github.com/sonseait/rtmoinit.git
cd rtmoinit
go mod download
go run ./examples/basic
```

Open **[http://127.0.0.1:3000/monitor/](http://127.0.0.1:3000/monitor/)** to see request stats, a latency chart, a bar chart, and a worker table populated with demo data.

To try restart recovery with a running Redis instance:

```sh
REDIS_ADDR=127.0.0.1:6379 go run ./examples/basic
```

The example also accepts `REDIS_USERNAME`, `REDIS_PASSWORD`, and `LISTEN_ADDR`. It binds to localhost by default and handles shutdown with Ctrl+C.

## Add it to your application

```sh
go get github.com/sonseait/rtmoinit
```

Inside your Fiber application, import `github.com/sonseait/rtmoinit`, then define and mount a dashboard. This integration fragment assumes an existing `ctx`, Fiber `app`, and `yourAuthMiddleware`:

```go
monitor, err := rtmoinit.New(ctx, rtmoinit.Config{
    Definition: rtmoinit.Definition{
        Series: []rtmoinit.Series{
            {ID: "requests", Title: "Requests", Kind: rtmoinit.Counter},
            {ID: "latency", Title: "Latency", Unit: "ms", Kind: rtmoinit.Gauge},
        },
        Dashboards: []rtmoinit.Dashboard{{
            ID: "overview", Title: "Service overview",
            Panels: []rtmoinit.Panel{
                {ID: "requests", Title: "Total requests", Type: rtmoinit.Stat, Series: []string{"requests"}},
                {ID: "latency", Title: "Latency over time", Type: rtmoinit.Line, Series: []string{"latency"}},
            },
        }},
    },
})
if err != nil {
    return err
}

monitor.Mount(app.Group("/monitor", yourAuthMiddleware))

if err := monitor.Add("requests", 1); err != nil {
    return err
}
if err := monitor.Set("latency", 42.5); err != nil {
    return err
}
```

Call `Add` for counters, `Set` for gauges, and `SetRows` for tables from your application code. Define the complete dashboard before calling `New`, mount it before starting the server, and call `Close(ctx)` during shutdown to stop workers and attempt a final checkpoint.

See the [complete runnable example](examples/basic/main.go) for tables, all panel types, optional Redis, and graceful shutdown.

## How it fits

rtmoinit is designed for visibility into an individual application instance: recent trends, current values, and small operational tables. Defaults include one hour of history, one-second sampling, and Redis checkpoints every ten seconds when enabled.

Each Redis namespace supports one active writer. Counters display cumulative totals; calculate rates in your application when needed. Protect the entire mounted route group with authentication before exposing it beyond localhost. Checkpoints provide periodic recovery; a crash can lose updates since the last successful save.

## Documentation

- [Usage and operations guide](docs/usage.md): data model, configuration limits, Redis lifecycle, HTTP endpoints, and SSE behavior.
- [Integration example](examples/basic/main.go): a working Fiber application with generated data.
- [Contributor instructions](AGENTS.md): project conventions and development checks.

## Development

```sh
go mod download
test -z "$(gofmt -l .)"
go vet ./...
go test -race ./...
go build ./...
```

Run `node ui_test.cjs` with Node.js to check responsive SVG chart geometry and dashboard navigation without browser or npm dependencies.

Redis integration tests are opt-in and require a dedicated test instance. See the [testing guide](docs/usage.md#testing) before enabling them.
