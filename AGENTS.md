# AGENTS.md

This is a fork of [OpenFGA](https://github.com/openfga/openfga) that adds an audit logging layer. See upstream's `AGENTS.md` conventions (import aliases, naming, error handling, testing) — they all apply here. This file covers what the fork adds.

## Design Goals

- **Thin audit wrapper** — minimize changes to upstream code so rebasing on new OpenFGA releases is trivial.
- **No upstream logic changes** — audit is purely additive. The fork must pass all upstream conformance and matrix tests unmodified.

## Key Design Decisions

- **gRPC interceptors** — all audit capture happens in `witness/interceptor/`. Because the HTTP gateway lowers to gRPC, interceptors cover both transports with a single code path.
- **Existing config mechanism** — audit settings are wired through OpenFGA's existing Viper/Cobra config in `witness/config/`, avoiding a separate config file.
- **Pluggable sink architecture** — `witness/sink/sink.go` defines the `AuditSink` interface. Implementations (Firehose, stdout, memory) are selected at startup. Adding a new sink means implementing one interface.

## Key Requirements

- **OCSF compliance** — all audit events use OCSF API Activity (class 6003) schema, built in `witness/ocsf/`.
- **High performance** — sinks are async and batch-based. Audit capture must not add measurable latency to authorization requests. The interceptor hands off events without blocking the RPC.

## Branch Structure

- **`main`** — tracks upstream `openfga/openfga:main`. Kept in sync via automated workflow (`.github/workflows/witness-sync-upstream.yaml`). Never commit fork changes here.
- **`witness`** — default development branch. All audit layer work lives here, rebased on `main`.

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
