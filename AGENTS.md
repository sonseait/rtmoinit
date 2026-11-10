# rtmoinit

Global user Codex rules continue to apply. Keep this project a small embeddable Go library.

## Stack and layout

- Module: `github.com/sonseait/rtmoinit`, Go 1.27.4 (stable release verified at creation).
- Fiber v3 serves read-only routes; go-redis/v9 is the optional checkpoint client.
- Root Go package contains definition validation, bounded RAM storage, persistence, and HTTP mounting.
- `ui.html` is embedded with `go:embed`: plain HTML/CSS/JavaScript and SVG, no frontend build or CDN.
- `examples/basic` is the runnable integration example. `*_test.go` uses the standard testing package.
- Keep `README.md` in English and focused on the product, use cases, and quick start. Maintain configuration, API, persistence, and operations details in `docs/usage.md`; keep both consistent with the implementation.
- No container delivery, database, standalone frontend, or extra telemetry stack is required.

## Setup and exact checks

```sh
go mod download
gofmt -w .
test -z "$(gofmt -l .)"
go vet ./...
go test -race ./...
go build ./...
go run ./examples/basic
```

`go vet ./...` is the lint/static check; Go compilation performs type checking. There is no separate frontend package manager or lint command.

Redis integration tests skip unless a dedicated Redis address is set:

```sh
RTMOINIT_TEST_REDIS_ADDR=127.0.0.1:6379 go test -race ./...
go test -run '^$' -bench BenchmarkAdd -benchmem
```

The integration suite issues `CLIENT PAUSE`; never point it at shared or production Redis. Tests use unique namespaces and remove their own keys. For a persistence-enabled example, set `REDIS_ADDR`; optional credentials are `REDIS_USERNAME` and `REDIS_PASSWORD`.

## Coding and behavior

- Follow gofmt, standard Go error handling, and existing names. Prefer stdlib and the two existing dependencies; keep functions cohesive and avoid speculative abstractions.
- `Config.Definition` is copied and fixed at initialization. Validate IDs, references, finite numbers, scalar table cells, and resource limits at API boundaries.
- RAM writes must never call Redis, wait for serialization, or queue network work. Copy under per-series locks; compress/serialize after releasing them.
- Sampling and persistence run independently. One checkpoint at a time; Redis operations must respect context deadlines. The caller owns the Redis client.
- Bound history, table rows/bytes, checkpoint bytes, and SSE clients. Keep background failures visible in status; never erase a stored checkpoint because restore failed.
- One active writer per Redis namespace. Do not add distributed aggregation implicitly.
- Mount routes under the application's authentication middleware. Render supplied text safely; preserve keyboard access, semantic tables, and responsive layouts.
- Add focused standard-library tests for non-trivial changes. Run relevant checks and report any that could not run. Document any new commands or lifecycle behavior here and in README.
