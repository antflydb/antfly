# Relational Indexes Remaining Slices

This is a live backlog, not a progress log. Keep durable design rules in
`RELATIONAL.md` and `SQL.md`; keep completed proof in code, tests, benchmark
output, and git history. Delete completed checkboxes instead of checking them
off or appending progress notes.

Each checkbox must be closable by one focused patch. The patch should include
implementation, fail-closed correctness coverage, public/API or fixture evidence
where relevant, and benchmark or profile evidence when the change affects
performance. Use data-driven tests where behavior varies by lifecycle state,
generation record shape, access method, fallback reason, malformed metadata, or
selectivity band. If a checkbox needs unrelated patches, split it before
starting.

Keep this ordered by largest runtime/product gaps first.

## Shared Lifecycle Model

- [ ] Persist text-search rebuild progress in the shared generation record:
  cursor, lag, ready watermark, and lifecycle for create and rebuild jobs. Cover
  fresh, stale, missing, malformed, and generation-mismatch records with a
  data-driven fixture.
- [ ] Promote text-search ranges to ready with compare-and-swap generation
  checks. Cover current, stale, wrong-generation, and concurrently-promoted
  records with deterministic diagnostics.
- [ ] Persist algebraic dictionary rebuild progress in the shared generation
  record. Cover the same generation-record matrix as text search.
- [ ] Persist algebraic fact, path, and postings rebuild progress in the shared
  generation record. Cover partial rebuild, interrupted rebuild, and completed
  rebuild fixtures.
- [ ] Load algebraic catch-up progress, lag, and ready watermark from the shared
  generation record during query planning. Cover admitted, fallback, and
  fail-closed cases for required and optional algebraic resolution.
- [ ] Promote algebraic ranges to ready with compare-and-swap generation checks.
  Cover dictionary-only, postings-only, complete, stale, and wrong-generation
  fixtures.
- [ ] Add owner-range rebuild leases with persisted holder, cursor, expiry, and
  generation. Cover live, expired, wrong-generation, missing-holder, and
  malformed lease rows with a data-driven test.
- [ ] Gate split, merge, and range movement on access-method generation
  readiness, lag, and held rebuild leases. Cover text-search and algebraic
  access methods with the same fixture table.
- [ ] Gate foreground writes that depend on stale or malformed derived
  access-method state before staging mutations. Cover write, delete, routed
  prepare, and batch paths.

## Repair And Drop Workers

- [ ] Add a shared repair job ledger for derived access methods with target,
  cursor, attempts, typed failure reason, pass counters, and generation. Cover
  resume, retry, success, and stale-generation rows.
- [ ] Implement text-search repair from authoritative rows without trusting
  segment or posting artifacts. Cover missing artifacts, corrupt artifacts,
  extra stale artifacts, and foreground writes during repair.
- [ ] Implement algebraic repair from authoritative rows without trusting
  dictionary, fact, path, or posting artifacts. Cover the same artifact matrix
  with data-driven fixtures.
- [ ] Add repair scheduling backpressure so repair work cannot starve foreground
  write or query hydration. Include profile counters for queued, running,
  throttled, and completed repair units.
- [ ] Add a shared drop job ledger for derived access methods with target,
  cursor, attempts, typed failure reason, pass counters, and generation. Cover
  resume, retry, success, and stale-generation rows.
- [ ] Implement nonblocking text-search artifact drop with foreground-write
  coexistence coverage.
- [ ] Implement nonblocking algebraic artifact drop with foreground-write
  coexistence coverage.

## Algebraic Access Method

- [ ] Build algebraic dictionary entries from committed rows during create and
  rebuild. Cover scalar, null, array, nested object, and missing-field inputs
  with fixture-driven expectations.
- [ ] Build algebraic fact, path, and postings entries from committed rows during
  create and rebuild. Cover the same input matrix and generation-record states.
- [ ] Catch up dictionary entries from committed-row generations after rebuild.
  Cover no-op, append-only, overwrite, and delete cases.
- [ ] Catch up fact, path, and postings entries from committed-row generations
  after rebuild. Cover no-op, append-only, overwrite, and delete cases.
- [ ] Maintain algebraic entries from foreground upsert old/new row deltas.
  Cover unchanged values, changed values, null transitions, array changes, and
  nested-path changes.
- [ ] Maintain algebraic entries from foreground delete deltas without leaving
  stale facts, paths, or postings. Cover single-row, multi-row, and routed
  delete batches.
- [ ] Plan equality predicates through algebraic streams with authoritative row
  rechecks and public fallback reasons. Cover required and optional resolution.
- [ ] Plan prefix predicates through algebraic streams with authoritative row
  rechecks and public fallback reasons. Cover required and optional resolution.
- [ ] Prove algebraic range semantics against ordered-tuple semantics or force a
  deterministic `ordered_tuple` fallback. Cover inclusive, exclusive, open-ended,
  and mixed-type ranges.

## Planner Policy

- [ ] Add algebraic streams to the mixed candidate planner with selected and
  rejected plan profile counters.
- [ ] Replace fixed candidate gates with selectivity inputs from index entries
  scanned, candidate rows, hydrated rows, and projected rows.
- [ ] Include `total_mode`, count-only reads, and page shape in selectivity
  decisions. Cover exact, bounded, none, and count-only modes.
- [ ] Surface public fallback reasons for rejected algebraic, text-search, and
  ordered-tuple plans. Cover unavailable, stale, malformed, too-expensive, and
  unsupported-shape reasons with data-driven assertions.

## Write Path Performance

- [ ] Add request-local scratch storage for scalar secondary key construction and
  value serialization. Include allocation-count tests for insert, unchanged
  overwrite, changed overwrite, and delete.
- [ ] Add request-local scratch storage for ordered-tuple key construction and
  payload value serialization. Include allocation-count tests for insert,
  unchanged overwrite, changed overwrite, and delete.
- [ ] Batch algebraic staged mutations per row update instead of per fact or path
  entry. Include staged-mutation-count tests for scalar, array, nested, and
  delete deltas.
- [ ] Batch text-search staged mutations per row update instead of per token or
  document entry. Include staged-mutation-count tests for insert, overwrite, and
  delete.
- [ ] Add a schema-backed write fast path for rows with no secondary artifacts.
  Publish write-amplification and allocation counters.
- [ ] Add a schema-backed write fast path for scalar-only secondary indexes.
  Publish write-amplification, allocation, and staged-mutation counters for
  overwrite-heavy workloads.

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
