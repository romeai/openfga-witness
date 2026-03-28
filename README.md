<div align="center">

# openfga-witness

**Structured OCSF audit logging for OpenFGA**

A thin fork of [OpenFGA](https://github.com/openfga/openfga) that emits [OCSF](https://schema.ocsf.io/) API Activity events for every API call via a pluggable sink architecture. Deploys as a drop-in replacement for the `openfga` binary.

</div>

---

## How It Works

openfga-witness adds a gRPC interceptor layer to OpenFGA that captures every API call and emits a structured [OCSF API Activity (class 6003)](https://schema.ocsf.io/1.4.0/classes/api_activity) audit event. The interceptor sits at the end of OpenFGA's middleware chain — after authentication — so it captures the caller identity, request details, authorization decisions, and latency for each RPC.

```
gRPC request
  → OpenFGA middleware (auth, request ID, logging, ...)
  → Audit interceptor (captures request + response)
       → type-switch on request
            Check, Write, Read, ...  → rich OCSF event with resources + authorizations
            unknown/future methods   → generic OCSF event
       → sink.Emit(event)  [best-effort, never fails the RPC]
  → response returned unchanged
```

**The fork is minimal.** Only one upstream file is modified (`cmd/run/run.go`, ~30 lines) to expose interceptor hooks on the gRPC server. All other code lives under `witness/`, cleanly separated from upstream. This makes rebasing against upstream OpenFGA straightforward — only one commit can ever conflict.

### Audit Events

Every audited RPC produces an OCSF API Activity 6003 event containing:

- **Who called the API** — service identity from auth claims (`actor`)
- **What was requested** — method, resource details, tuple keys (`api`, `resources`)
- **What happened** — authorization decision, status, latency (`authorizations`, `disposition`, `duration`)
- **Where from** — source IP and port (`src_endpoint`)
- **Tenant context** — store ID, authorization model ID (`metadata.tenant_uid`, `unmapped`)

Check and BatchCheck events include the authorization decision (Allowed/Denied) and disposition. Write events distinguish creates from deletes. ListObjects and ListUsers capture query parameters and result counts. All other RPCs get a generic event with method, status, and latency.

### Pluggable Sinks

Audit events are emitted to a pluggable `AuditSink` interface. Two production sinks are included:

| Sink | Transport | Use Case |
|---|---|---|
| **stdout** (default) | JSON lines to stdout | Development, log aggregators, `kubectl logs` pipelines |
| **firehose** | AWS Data Firehose `PutRecordBatch` | AWS Security Lake (Firehose → Parquet → S3 → Security Lake) |

A `MemorySink` is also available for testing.

## Quickstart

### Docker

```shell
docker build -f Dockerfile.witness -t openfga-witness .
docker run -p 8080:8080 -p 3000:3000 openfga-witness run --audit-sink=stdout
```

### Build from Source

```shell
go build -o openfga-witness ./cmd/witness
./openfga-witness run
```

Audit events appear on stdout as JSON lines (one per API call):

```json
{"class_uid":6003,"category_uid":6,"activity_id":2,"type_name":"API Activity: Read","api":{"operation":"Check","service":{"name":"openfga.v1.OpenFGAService"}},"resources":[{"data":{"user":"user:alice","relation":"viewer","object":"document:1"}}],"authorizations":[{"decision":"Allowed"}],"duration":3,"status_id":1,"status":"Success",...}
```

## Configuration

Configuration uses OpenFGA's existing viper/cobra system. All standard OpenFGA flags, environment variables, and config file options work unchanged.

### Audit-Specific Options

| Flag | Env Var | Default | Description |
|---|---|---|---|
| `--audit-sink` | `OPENFGA_AUDIT_SINK` | `stdout` | Sink type: `stdout` or `firehose` |
| `--audit-firehose-stream-name` | `OPENFGA_AUDIT_FIREHOSE_STREAMNAME` | — | Firehose delivery stream name |
| `--audit-firehose-batch-size` | `OPENFGA_AUDIT_FIREHOSE_BATCHSIZE` | `500` | Records per `PutRecordBatch` call (max 500) |
| `--audit-firehose-flush-interval` | `OPENFGA_AUDIT_FIREHOSE_FLUSHINTERVAL` | `5s` | Max time before flushing a partial batch |
| `--audit-firehose-region` | `OPENFGA_AUDIT_FIREHOSE_REGION` | SDK default | AWS region for Firehose |

### Config File

```yaml
audit:
  sink: firehose
  firehose:
    streamName: openfga-audit
    batchSize: 500
    flushInterval: 5s
    region: us-east-1
```

AWS credentials use the standard SDK credential chain (env vars, instance profile, ECS task role).

## Metrics

openfga-witness exposes Prometheus metrics alongside OpenFGA's existing metrics:

| Metric | Type | Labels | Description |
|---|---|---|---|
| `openfga_witness_audit_events_total` | Counter | `method`, `status` | Total audit events emitted |
| `openfga_witness_audit_events_dropped_total` | Counter | — | Events that failed to emit (sink error) |

## Upstream Compatibility

openfga-witness tracks upstream OpenFGA via `git remote`. The API is unchanged — all OpenFGA clients, SDKs, and tools work without modification.

```bash
# Sync with upstream
git fetch upstream
git rebase upstream/main
# Only one commit modifies upstream code — conflicts are rare and small
```

Conformance is verified by running OpenFGA's full matrix test suite (~12,500 lines of YAML test cases) against the witness binary.

## Repository Structure

```
├── witness/                    Our code (cleanly separated)
│   ├── ocsf/                   OCSF API Activity 6003 structs + builders
│   ├── interceptor/            gRPC audit interceptors
│   ├── sink/                   AuditSink interface + implementations
│   ├── config/                 Audit config (viper/cobra bindings)
│   └── metrics/                Prometheus counters
├── witness_tests/              Integration + conformance tests
├── cmd/witness/main.go         Our entrypoint
├── cmd/run/run.go              Modified upstream (interceptor hooks)
├── Dockerfile.witness          Container build
└── (everything else)           Upstream OpenFGA, untouched
```

## OpenFGA

This project is a fork of [OpenFGA](https://github.com/openfga/openfga), a high-performance authorization engine inspired by [Google Zanzibar](https://research.google/pubs/pub48190/). For OpenFGA documentation, see [openfga.dev](https://openfga.dev/).

## License

Same license as OpenFGA. See [LICENSE](LICENSE).

---

<div align="center">

This is not an officially supported Rome AI product.

Made with 🖤 in New York.

</div>
