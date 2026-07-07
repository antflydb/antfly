# Relational Indexes Remaining Slices

This is a live backlog, not a progress log. Keep durable design rules in
`RELATIONAL.md` and `SQL.md`; keep completed proof in code, tests, benchmark
output, and git history. Delete completed bullets instead of checking them off
or appending progress notes.

Each checkbox must be closable by one focused patch. The patch must include
implementation, fail-closed correctness tests, public/API or fixture evidence
where relevant, and benchmark or profile evidence when the change affects
performance. Prefer fixture-backed or table-driven tests when the behavior has
multiple equivalent cases, such as lifecycle states, malformed metadata,
fallback reasons, selectivity bands, or access-method variants. If a checkbox
needs multiple unrelated patches, split it before starting.

Keep this ordered by largest runtime/product gaps first. Do not add notes under
completed work; remove the checkbox once the patch lands.

## Shared Lifecycle Model

- [ ] Write text-search rebuild cursor, lag, and ready watermark into the shared
  generation record during create and rebuild jobs.
- [ ] Promote text-search ranges to ready with compare-and-swap generation
  checks and deterministic stale-generation diagnostics.
- [ ] Gate text-search query and write maintenance on generation-record
  readiness instead of method-local readiness state.
- [ ] Write algebraic dictionary rebuild cursor, lag, and ready watermark into
  the shared generation record during create and rebuild jobs.
- [ ] Write algebraic fact, path, and postings rebuild cursor, lag, and ready
  watermark into the shared generation record during create and rebuild jobs.
- [ ] Load algebraic catch-up progress, lag, and ready watermark from the shared
  generation record in query planning decisions.
- [ ] Promote algebraic ranges to ready with compare-and-swap generation checks
  and deterministic stale-generation diagnostics.
- [ ] Gate algebraic query and write maintenance on generation-record readiness
  instead of method-local readiness state.
- [ ] Add owner-range rebuild leases with persisted holder, cursor, and expiry.
- [ ] Add data-driven stale-lease takeover coverage for live, expired,
  wrong-generation, and missing-holder lease states.
- [ ] Gate split and merge on access-method generation readiness, lag, and held
  rebuild leases.
- [ ] Gate range movement on access-method generation readiness, lag, and held
  rebuild leases.
- [ ] Gate foreground writes that depend on stale or malformed derived
  access-method state before staging mutations.

## Repair And Drop Workers

- [ ] Add a shared repair job ledger for derived access methods with target,
  cursor, attempts, typed failure reason, and pass counters.
- [ ] Implement text-search repair from authoritative rows without trusting
  segment or posting artifacts.
- [ ] Implement algebraic repair from authoritative rows without trusting
  dictionary, fact, path, or posting artifacts.
- [ ] Add repair scheduling backpressure so repair work cannot starve foreground
  write or query hydration.
- [ ] Add a shared drop job ledger for derived access methods with target,
  cursor, attempts, typed failure reason, and pass counters.
- [ ] Implement nonblocking text-search artifact drop with foreground-write
  coexistence coverage.
- [ ] Implement nonblocking algebraic artifact drop with foreground-write
  coexistence coverage.

## Algebraic Access Method

- [ ] Build dictionary entries from committed rows during create and rebuild.
- [ ] Build fact, path, and postings entries from committed rows during create
  and rebuild.
- [ ] Catch up dictionary entries from committed-row generations.
- [ ] Catch up fact, path, and postings entries from committed-row generations.
- [ ] Maintain algebraic entries from foreground upsert old/new row deltas.
- [ ] Maintain algebraic entries from foreground delete deltas without leaving
  stale facts, paths, or postings.
- [ ] Plan equality predicates through algebraic streams with authoritative row
  rechecks and deterministic fallback reasons.
- [ ] Plan prefix predicates through algebraic streams with authoritative row
  rechecks and deterministic fallback reasons.
- [ ] Prove algebraic range semantics or force deterministic fallback to
  `ordered_tuple`.

## Planner Policy

- [ ] Add algebraic streams to the mixed candidate planner with selected and
  rejected plan profile counters.
- [ ] Replace fixed candidate gates with selectivity inputs from index entries
  scanned, candidate rows, hydrated rows, and projected rows.
- [ ] Include `total_mode`, count-only reads, and page shape in selectivity
  decisions.
- [ ] Surface public fallback reasons for rejected algebraic, text-search, and
  ordered-tuple plans.

## Write Path Performance

- [ ] Add request-local scratch storage for scalar secondary key construction
  and value serialization.
- [ ] Add request-local scratch storage for ordered-tuple key construction and
  payload value serialization.
- [ ] Batch algebraic staged mutations per row update instead of per fact or
  path entry.
- [ ] Batch text-search staged mutations per row update instead of per token or
  document entry.
- [ ] Add a schema-backed write fast path for rows with no secondary artifacts.
- [ ] Add a schema-backed write fast path for scalar-only secondary indexes.
- [ ] Publish write-amplification, allocation, and staged-mutation counters for
  overwrite-heavy workloads.
- [ ] Add allocation-count tests for scalar, ordered-tuple, algebraic, and
  text-search overwrite paths.
- [ ] Add staged-mutation-count tests for multi-row delete and routed prepare
  paths.

## Benchmark Evidence

- [ ] Run the reproducible 10k-row matrix for low/high selectivity with exact,
  bounded, and no-total reads.
- [ ] Run the reproducible 100k-row matrix for low/high selectivity with exact,
  bounded, and no-total reads.
- [ ] Run the reproducible 1M-row matrix after the 100k run has stable resource
  usage.
- [ ] Compare no-index, scalar-index, ordered-tuple, and document-table modes in
  the same benchmark output.
- [ ] Add algebraic mode to the benchmark comparison once algebraic reads are
  planner-admitted.
- [ ] Compare ordered KV against dictionary/FST/trie equality scans.
- [ ] Compare ordered KV against dictionary/FST/trie prefix scans and any
  admitted range scans.
- [ ] Record update churn, delete cleanup, write amplification, memory, rebuild
  cost, count-only cost, and public query latency in benchmark output.
