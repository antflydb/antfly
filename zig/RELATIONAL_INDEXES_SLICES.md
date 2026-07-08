# Relational Indexes Remaining Slices

This is a live backlog, not a progress log. Keep durable design rules in
`RELATIONAL.md` and `SQL.md`; keep completed proof in code, tests, benchmark
output, and git history. Delete completed checkboxes instead of checking them
off or appending progress notes.

Each checkbox must be closable by one focused patch. The patch should include
implementation, fail-closed correctness coverage, public/API or fixture evidence
where relevant, and benchmark or profile evidence when the change affects
performance. If a checkbox needs unrelated patches, split it before starting.
Delete completed work from this file in the same patch that closes it.

Use data-driven tests when the behavior is a matrix: lifecycle state,
generation-record shape, access method, operation kind, fallback reason,
malformed metadata, selectivity band, total mode, row shape, or old/new write
delta. Prefer one table of cases over repeated one-off tests when the setup and
assertions are the same. If a slice has more than two meaningful cases, include
a named table-driven test or benchmark matrix in the patch. One-off tests are
still fine for single invariants with no meaningful case matrix.

Keep this ordered by largest runtime/product gaps first.

## Query Path Runtime

- [ ] Calibrate the default ordered-tuple candidate policy with benchmark
  evidence. Use one data-driven benchmark/report table covering low/high
  selectivity, exact/bounded/no-total, count-only, and page shape to choose
  stream vs doc-set vs base-scan plans from measured planner counters,
  latency, and memory.

## Shared Lifecycle Model

- [ ] Execute algebraic dictionary, fact, path, and postings rebuilds as
  independently resumable phase workers using the durable component cursors.
  Use one fixture matrix for fact-only, path-only, postings-only, interrupted,
  malformed, and completed phase records.

## Repair And Drop Workers

- [ ] Implement nonblocking text-search artifact drop. Cover foreground writes,
  interrupted drops, retry, and stale-generation cases with one drop-state
  table.
- [ ] Implement nonblocking algebraic artifact drop. Cover dictionary, fact,
  path, postings, interrupted drops, retry, and stale-generation cases with one
  drop-state table.

## Algebraic Access Method

- [ ] Build algebraic dictionary entries from committed rows during create and
  rebuild. Use one row-shape table for scalar, null, array, nested object, and
  missing-field inputs.
- [ ] Build algebraic fact, path, and postings entries from committed rows during
  create and rebuild. Reuse the row-shape table and add generation-record
  states.
- [ ] Catch up algebraic entries from committed-row generations after rebuild.
  Cover dictionary, fact, path, postings, no-op, append-only, overwrite, and
  delete cases with a data-driven matrix.
- [ ] Maintain algebraic entries from foreground upsert old/new row deltas.
  Use one old/new delta table for unchanged values, changed values, null
  transitions, array changes, and nested-path changes.
- [ ] Maintain algebraic entries from foreground delete deltas without leaving
  stale facts, paths, or postings. Use one delete-shape table for single-row,
  multi-row, and routed delete batches.
- [ ] Plan equality predicates through algebraic streams with authoritative row
  rechecks and public fallback reasons. Cover required/optional resolution and
  admitted/rejected cases with one table.
- [ ] Plan prefix predicates through algebraic streams with authoritative row
  rechecks and public fallback reasons. Cover required/optional resolution and
  admitted/rejected cases with one table.
- [ ] Prove algebraic range semantics against ordered-tuple semantics or force a
  deterministic `ordered_tuple` fallback. Use one range-case table for
  inclusive, exclusive, open-ended, and mixed-type ranges.

## Planner Policy

- [ ] Add algebraic streams to the mixed candidate planner with selected and
  rejected plan profile counters. Use one planner-case table for selected,
  rejected, required, optional, stale, and malformed algebraic streams.
- [ ] Surface public fallback reasons for rejected algebraic, text-search, and
  ordered-tuple plans. Use one fallback-reason table for unavailable, stale,
  malformed, too-expensive, and unsupported-shape cases.

## Write Path Performance

- [ ] Add request-local scratch storage for scalar secondary key construction and
  value serialization. Use one allocation-count table for insert, unchanged
  overwrite, changed overwrite, and delete.
- [ ] Add request-local scratch storage for ordered-tuple key construction and
  payload value serialization. Reuse the allocation-count table for insert,
  unchanged overwrite, changed overwrite, and delete.
- [ ] Batch algebraic staged mutations per row update instead of per fact or path
  entry. Use one staged-mutation table for scalar, array, nested, and delete
  deltas.
- [ ] Batch text-search staged mutations per row update instead of per token or
  document entry. Use one staged-mutation table for insert, overwrite, and
  delete.
- [ ] Add a schema-backed write fast path for rows with no derived artifacts.
  Use one schema-shape table for no-index, generated-only, FK-only, and
  no-derived-artifact rows. Publish write-amplification and allocation counters
  with a before/after benchmark row.
- [ ] Add a schema-backed write fast path for scalar-only secondary indexes.
  Use one scalar-index table for insert, unchanged overwrite, changed overwrite,
  delete, nullable, and multi-index schemas. Publish write-amplification,
  allocation, and staged-mutation counters for overwrite-heavy workloads with a
  before/after benchmark row.

## Benchmark Evidence

- [ ] Run and archive `relational-read-bench` output for 10k/100k row low/high
  selectivity matrices with exact, bounded, no-total, and count-only modes.
- [ ] Run and archive the same `relational-read-bench` matrix at 1M rows after
  the 100k run has stable resource usage and bounded memory.
- [ ] Extend `relational-read-bench` to include algebraic mode once algebraic
  reads are planner-admitted.
- [ ] Compare ordered KV against dictionary/FST/trie candidates for equality,
  prefix, and any admitted range scans before changing the durable structure.
- [ ] Record update churn, delete cleanup, write amplification, memory, rebuild
  cost, count-only cost, and public query latency in benchmark output using a
  stable JSONL schema that can be compared across runs.
