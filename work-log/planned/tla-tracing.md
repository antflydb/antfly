# TLA+ Trace Validation for Antfly

## Context

Antfly has 3 TLA+ formal specs (transactions, shard splits, snapshot transfer) that have already caught real bugs, but they're only used for offline model checking. Meanwhile, etcd/raft v3.6.0 ships a trace validation system (`with_tla` build tag) that's been sitting disabled in Antfly since adoption (`src/raft/raft.go:683`: `TraceLogger: nil`). This plan wires up etcd/raft's existing infrastructure and extends the pattern to Antfly's own specs, creating a spec-to-implementation conformance bridge.

## Package Structure

**`src/tracing/`** — shared infrastructure, no domain imports:
- Raft `TraceLogger` adapter (wraps zap, implements `raft.TraceLogger`)
- `AntflyTraceWriter` interface + zap-backed impl for Antfly-level events
- Generic `AntflyTracingEvent` struct (`Name`, `State map[string]any`, etc.)

**Domain packages** — build-tagged helpers that construct events from internal state:
- `src/store/db/tla_trace.go` / `_nop.go` — reads local types (intents, txn records), builds events, calls `tracing.AntflyTraceWriter`
- `src/metadata/tla_trace.go` / `_nop.go` — orchestrator-level events

**Why this split:** Avoids import cycles. Transaction trace helpers need `src/store/db/` internal types. If those helpers lived in `src/tracing/`, we'd get `src/tracing/` → `src/store/db/` → `src/tracing/` cycle. Instead, `src/tracing/` defines the interfaces, domain packages implement the event construction.

---

## Phase 1: Enable etcd/raft Trace Validation

**The lowest-hanging fruit.** etcd/raft already has everything — we just need to implement `TraceLogger` and wire it in.

### Files to create

**`src/tracing/raft_tracelogger.go`** (`//go:build with_tla`)
- Implement `raft.TraceLogger` interface wrapping `*zap.Logger`
- `TraceEvent(*raft.TracingEvent)` emits: `logger.Debug("trace", zap.String("tag", "trace"), zap.Any("event", ev))`
- This matches the ndjson format `Traceetcdraft.tla` expects
- Constructor: `func NewRaftTraceLogger(logger *zap.Logger) raft.TraceLogger`

**`src/tracing/raft_tracelogger_nop.go`** (`//go:build !with_tla`)
- Stub: `func NewRaftTraceLogger(_ *zap.Logger) raft.TraceLogger { return nil }`
- When nil, etcd/raft short-circuits all trace calls — zero overhead

### Files to modify

**`src/raft/raft.go:683`**
- Change: `TraceLogger: nil` → `TraceLogger: tracing.NewRaftTraceLogger(raftLogger)`
- Import `src/tracing`
- Without `with_tla`: returns nil (identical to today)
- With `with_tla`: emits 19 event types (InitState, BecomeLeader, Commit, SendAppendEntries*, etc.)

### Risks
- etcd/raft's `traceReceiveMessage` has `time.Sleep(1ms)` per received message to order events. In multi-raft (many shard groups), this adds latency. Acceptable for test builds behind `with_tla` tag, not for production.
- Multi-raft trace interleaving: different shard raft groups write to same log. Post-process by filtering on `nid` values per shard group before feeding to TLC.

### Verify
```bash
# Build with trace support
GOEXPERIMENT=simd go test -tags with_tla -run TestHarness_FollowerSnapshot -v ./src/sim/ 2>&1 | grep '"tag":"trace"' > /tmp/raft-trace.ndjson
# Validate against etcd/raft TLA+ spec
make tla-trace-raft TRACE_FILES=/tmp/raft-trace.ndjson
```

---

## Phase 2: Makefile Targets + TLA+ Tooling

### Files to create

**`scripts/tla-tools.sh`**
- Downloads `tla2tools.jar` + `CommunityModules-deps.jar` to `$HOME/.tla-tools/`
- Stamp files skip re-download
- Detects Java: `$JAVA_HOME`, system `java`, macOS Toolbox bundled JRE

**`scripts/tla-validate-trace.sh`**
- Adapted from etcd/raft's `validate.sh` (cleaner version)
- Sources `tla-tools.sh` for jar locations
- Accepts `-s spec -c config <ndjson files>`
- Preprocesses ndjson, runs TLC per trace file, supports `-p` parallelism

### Files to modify

**`Makefile`** — new section:
```makefile
# TLA+ Verification
tla-tools:           # Download tla2tools.jar
tla-check:           # Run TLC on all 3 Antfly specs
tla-check-txn:       # Transaction spec only (~2.3K states, ~10s)
tla-check-split:     # Shard split spec only
tla-check-snap:      # Snapshot transfer spec only (~27M states, ~90s)
tla-trace-raft:      # Validate raft ndjson traces against Traceetcdraft.tla
```

**`Makefile` help target** — add TLA+ entries

**`.gitignore`** — add `.tla-tools/`, `specs/tla/states/`

### Verify
```bash
make tla-check-txn  # "Model checking completed. No error has been found. 2362 states..."
make tla-check-snap # ~90s, 27M states
```

---

## Phase 3: Instrument Antfly Transaction Code for Trace Events

**Start with transactions** — the most bug-prone protocol, cleanest code boundaries, sequential per-txn.

### Files to create

**`src/tracing/antfly_writer.go`** (`//go:build with_tla`)
- `AntflyTracingEvent` struct: `Name`, `TxnID`, `ShardID`, `State map[string]any`
- `AntflyTraceWriter` interface: `TraceAntflyEvent(*AntflyTracingEvent)`
- `ZapAntflyTraceWriter` impl: emits `logger.Debug("trace", zap.String("tag", "antfly-trace"), zap.Any("event", ev))`

**`src/tracing/antfly_writer_nop.go`** (`//go:build !with_tla`)
- Stub `AntflyTraceWriter` that discards all events
- `NewAntflyTraceWriter` returns the no-op

**`src/store/db/tla_trace.go`** (`//go:build with_tla`)
- Trace helpers that read local types and build `AntflyTracingEvent`:

| TLA+ Action | Go instrumentation point | Event name |
|---|---|---|
| `InitTransaction` | `db.go` `InitTransaction` | `"InitTransaction"` |
| `CheckPredicates` | `transaction.go` before WriteIntent RPCs | `"CheckPredicates"` |
| `WriteIntentOnShard` | `db.go` `WriteIntent` (success) | `"WriteIntentOnShard"` |
| `WriteIntentFails` | `db.go` `WriteIntent` (failure) | `"WriteIntentFails"` |
| `CommitTransaction` | `helpers.go` `finalizeTransaction` (status=1) | `"CommitTransaction"` |
| `AbortTransaction` | `helpers.go` `finalizeTransaction` (status=2) | `"AbortTransaction"` |
| `ResolveIntentsOnShard` | `db.go` `ResolveIntents` | `"ResolveIntentsOnShard"` |
| `RecoveryResolve` | `db.go` `transactionRecoveryLoop` | `"RecoveryResolve"` |
| `CleanupTxnRecord` | `db.go` cleanup after allResolved | `"CleanupTxnRecord"` |

**`src/store/db/tla_trace_nop.go`** (`//go:build !with_tla`) — no-op stubs

**`src/metadata/tla_trace.go`** + **`src/metadata/tla_trace_nop.go`** — orchestrator-level traces

### Files to modify

- `src/store/db/db.go` — add `trace*()` calls at the instrumentation points above
- `src/store/db/helpers.go` — trace in `finalizeTransaction`
- `src/metadata/transaction.go` — trace in `ExecuteTransaction`

### State captured per event
Each ndjson line includes enough TLA+ variable state for the trace spec to match:
- `txnStatus[t]` — orchestrator state
- `txnRecords[t]` — coordinator record status
- `intents[t, s]` — intent state on the relevant shard
- `clock` — HLC timestamp

### Verify
Run `src/sim/transaction_scenario_test.go` with `-tags with_tla`, extract `antfly-trace` lines, manually verify events match expected TLA+ action sequence.

---

## Phase 4: Write TraceAntflyTransaction.tla

**The hard part.** Create a trace validation spec analogous to `Traceetcdraft.tla`.

### Files to create

**`specs/tla/TraceAntflyTransaction.tla`**
- `EXTENDS AntflyTransaction, Json, IOUtils, Sequences, TLC`
- Reads ndjson via `ndJsonDeserialize(IOEnv.JSON)`
- Filters `.tag == "antfly-trace"`
- For each log line, matches `.event.name` to TLA+ action
- Validates pre/post state matches
- `TraceSpec == TraceInit /\ [][TraceNext]_<<l, pl, vars>>`
- Checks `TraceMatched` temporal property

**`specs/tla/TraceAntflyTransaction.cfg`** — TLC config for trace validation

**Challenge**: Unlike etcd/raft (fixed node set), transactions span dynamically determined shards/keys. The trace spec must derive constants from the trace itself. Start with traces from `transaction_scenario_test.go` where the set is small and known.

### Future work (Phase 4b, 4c)
- `TraceAntflyShardSplit.tla` — instrument `storedb.go` split lifecycle
- `TraceAntflySnapshotTransfer.tla` — instrument `raft.go` snapshot lifecycle

### Verify
```bash
make tla-trace-txn TRACE_FILES=/tmp/txn-trace.ndjson
```

---

## Phase 5: CI Integration

### Files to create

**`.github/workflows/tla-check.yml`**
- Trigger: push/PR touching `specs/tla/**`, `src/tracing/**`, `src/store/db/tla_trace*.go`
- Job: install Java, `make tla-check` (~3 minutes total)

**`.github/workflows/tla-trace.yml`** (scheduled/manual)
- Weekly or `workflow_dispatch`
- Build with `-tags with_tla`, run sim scenarios, extract traces, validate
- Expensive — not per-PR

---

## Phasing

```
Phase 1 (raft TraceLogger in src/tracing/)  ← ship alone, immediate value
Phase 2 (Makefile + scripts)                ← ship with Phase 1
Phase 3 (AntflyTraceWriter + txn events)    ← next iteration
Phase 4 (Trace*.tla)                        ← requires TLA+ writing, ~1-2 weeks
Phase 5 (CI)                                ← after Phase 4 stabilizes
```

**Phases 1+2 are the starting deliverable.** They enable `make tla-check` (model checking all specs) and raft trace validation from sim tests with zero changes to production code paths.
