# AGENTS.md

This is a fork of [OpenFGA](https://github.com/openfga/openfga) that adds an audit logging layer and a small set of storage and cache patches. See upstream's `AGENTS.md` conventions (import aliases, naming, error handling, testing) — they all apply here. This file covers what the fork adds.

## Design Goals

- **Thin audit wrapper** — minimize changes to upstream code so rebasing on new OpenFGA releases is trivial.
- **Audit is purely additive** — the audit layer changes no upstream logic. The fork must pass all upstream conformance and matrix tests unmodified.
- **Patches stay few and tested** — upstream logic changes are limited to the storage and cache patches listed below, each a separate commit with its tests.

## Upstream Logic Patches

- **Buffered SQL tuple rows** (`pkg/storage/sqlcommon/buffered_rows.go`) — tuple iterators drain their result and release the pooled connection before the first tuple reaches the consumer (no hold-and-wait on a saturated pool).
- **ReadUserTuple caching** (`pkg/storage/storagewrappers/cached_datastore.go`) — the v1 iterator cache also caches point lookups, invalidated by the cache controller's store-wide and (object, relation) markers.
- **Check cache stamps** (`pkg/storage/cache_freshness.go`, `internal/graph/cached_resolver.go`, `internal/check/check.go`) — check-query cache entries are stamped at computation start, lowered to the oldest cache entry the computation consumed.
- **Synchronous cache freshness** (`internal/cachecontroller/cache_controller.go`) — a request whose store's changelog was last read more than the controller TTL ago waits for a re-read (shared per store, bounded by 1s) instead of answering from the caches; a read that fails or times out invalidates the whole store. Check results are invalidated from when a change was observed, not from its changelog timestamp.
- **Invalidation state outside the result cache** (`pkg/storage/invalidation.go`, `internal/cachecontroller/cache_controller.go`) — iterator invalidation markers and the controller's per-store state live in dedicated storage that expires by TTL and is never evicted by size pressure; iterator and ReadUserTuple entries expire their TTL after their stamp, so no entry outlives a marker that invalidates it.

## Key Design Decisions

- **gRPC interceptors** — all audit capture happens in `witness/interceptor/`. Because the HTTP gateway lowers to gRPC, interceptors cover both transports with a single code path.
- **Existing config mechanism** — audit settings are wired through OpenFGA's existing Viper/Cobra config in `witness/config/`, avoiding a separate config file.
- **Pluggable sink architecture** — `witness/sink/sink.go` defines the `AuditSink` interface. Implementations (Firehose, stdout, memory) are selected at startup. Adding a new sink means implementing one interface.

## Key Requirements

- **OCSF compliance** — all audit events use OCSF API Activity (class 6003) schema, built in `witness/ocsf/`.
- **High performance** — sinks are async and batch-based. Audit capture must not add measurable latency to authorization requests. The interceptor hands off events without blocking the RPC.

## Branch Structure

- **`main`** — tracks upstream `openfga/openfga:main`. Never commit fork changes here.
- **`witness`** — the audit layer rebased on `main` by the manually dispatched sync workflow (`.github/workflows/witness-sync-upstream.yaml`).
- **`witness-v<tag>`** (e.g. `witness-v1.21.0`) — release branches: the fork commits rebased onto upstream release tag `<tag>`. Consumers pin these. They are never rebased onto `main`; a new upstream release gets a new branch.

## Directory Structure (fork additions)

| Directory | Purpose |
|---|---|
| `cmd/witness/` | Fork entrypoint |
| `witness/config/` | Audit config (Viper/Cobra bindings) |
| `witness/interceptor/` | gRPC unary interceptor for audit capture |
| `witness/ocsf/` | OCSF event structs and builders |
| `witness/sink/` | `AuditSink` interface + implementations (Firehose, stdout, memory) |
| `witness/metrics/` | Prometheus counters for audit pipeline health |
| `witness_tests/` | Conformance and integration tests for audit layer |
