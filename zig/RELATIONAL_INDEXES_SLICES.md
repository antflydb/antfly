# Relational Indexes Remaining Slices

This is the tracking document for relational index access-method work. It lists
only remaining implementation work and should stay checklist-shaped. Keep
architecture decisions, ownership boundaries, and long-lived invariants in
`RELATIONAL.md` and SQL surface rules in `SQL.md`. Completed history that is
deducible from code, tests, fixtures, benchmarks, or git history should stay out
of this document.

Feature work is listed first, followed by planner integration, lifecycle,
observability, and evidence/performance hardening. Check a slice only when the
implementation, fail-closed behavior, diagnostics, tests, and benchmark evidence
are all in place.

When a remaining slice admits new SQL syntax, index options, access-method
names, catalog fields, or public plan shapes, keep parser/binder/lowerer
changes, native catalog validation, public SQL/API fixtures, and unsupported
diagnostics in the same patch. Do not add syntax that stores backend SQL text or
physical data-structure names in durable metadata.

Every slice that changes write-path maintenance or query planning must include
overwrite/delete cleanup coverage, generation/readiness checks, routed or Lite
behavior where applicable, and benchmark deltas against the previous relational
row/index baseline.

- [ ] Ordered tuple compound index storage
  - [ ] Extend the current durable column-backed secondary-index metadata for
    `ordered_tuple` access method, schema fingerprint, generation, lifecycle,
    key columns, direction/null ordering, include columns, and predicates into
    a first-class index catalog that also covers expression keys, uniqueness,
    method-specific options, and non-column access methods. The existing
    column-backed path now persists access method and schema fingerprint and
    the write/query paths honor that access method instead of inferring ordered
    tuple or scalar behavior only from key shape.
  - [ ] Extend ordered tuple value encoding beyond scalar column keys to cover
    expression keys and planner coverage for descending/null-order-specific
    bounds.
  - [ ] Support included payload columns only as optional index-local payload
    for covering reads; the packed row remains authoritative and covering reads
    must recheck row generation before returning user-visible data.

- [ ] Constraint-grade unique and foreign-key lookup on ordered indexes
  - [ ] Route unique-owner lookup for compound unique constraints through
    `ordered_tuple` metadata instead of single-column-only helpers.
  - [ ] Enforce partial unique constraints by proving the proposed row satisfies
    the stored predicate, then probing only the matching ordered tuple owner.
  - [ ] Route FK parent existence checks to ordered tuple primary/unique owners
    for compound parent keys, including hosted participant routing where owner
    topology is required.
  - [ ] Route restrict parent-delete checks through child ordered indexes when
    a child FK maps to compound parent keys; fail closed when the matching child
    index is not ready or cannot prove predicate compatibility.
  - [ ] Add concurrency tests for conflicting prepares on the same unique/FK
    owner tuple and stale generation tests that reject old index state.

- [ ] Query planner ordered-index selection
  - [ ] Support ordered index range/order scans for non-default null ordering
    and descending keys; when any component is not covered, surface
    deterministic fail-closed diagnostics.
  - [ ] Satisfy `ORDER BY` from ordered indexes only when direction, nulls,
    collation, predicate domain, and tie-breaker semantics are exact.
  - [ ] Use selectivity estimates or bounded probe accounting to choose between
    ordered index scan, algebraic/text doc-set intersection, and base-row scan.
  - [ ] Make residual predicate recheck explicit in the plan for every ordered
    scan where the index does not fully prove the typed filter.
  - [ ] Add plan-summary evidence that names the selected access method,
    generation, predicate proof, scan bounds, estimated candidates, residual
    predicates, and fallback reason.

- [ ] Access-method catalog and SQL lowering
  - [ ] Lower ordinary `CREATE INDEX` and `CREATE UNIQUE INDEX` to
    `ordered_tuple` by default, including key expressions, include columns,
    partial predicates, direction, collation, null ordering, and lifecycle
    options.
  - [ ] Lower `USING antfly_algebraic` to `algebraic_filter` access metadata or
    native algebraic materialization metadata, depending on whether the DDL asks
    for a filter index or a fold/materialization.
  - [ ] Lower `CREATE TEXT SEARCH` and `USING antfly_full_text` to the
    `text_search` access method with analyzer, scoring, highlight/snippet, and
    segment lifecycle config.
  - [ ] Reject physical backend spellings unless they map to a proven native
    access-method capability. Default and explicit SQL `btree` currently lower
    to durable `ordered_tuple` metadata, `gin` lowers to algebraic JSON/array
    index metadata where validated, and unsupported names such as `gist`/`trie`
    fail before they can enter durable catalog state.
  - [ ] Add catalog JSON/binary round-trip tests and SQL/API parity fixtures for
    each admitted access method and each unsupported option family.

- [ ] Algebraic and text-search planner integration
  - [ ] Expose `algebraic_filter` capability metadata for fact/path/dictionary
    selectors, including supported predicates, generation, schema capability
    fingerprint, and stale-domain fail-closed reasons.
  - [ ] Expose `text_search` capability metadata for analyzed text predicates,
    ranking, highlights/snippets, generation, and field-path resolution.
  - [ ] Intersect ordered tuple candidates, algebraic doc sets, and text-search
    result sets through one doc-set interface with bounded memory accounting.
  - [ ] Hydrate and recheck from committed relational rows when combining any
    derived method with scalar predicates, partial indexes, or user-visible
    projection.
  - [ ] Add tests for text-search plus tenant/status filters, JSON path facts
    plus ordered scalar filters, stale algebraic capability fallback, stale
    full-text generation fallback, and mixed-method pagination stability.

- [ ] Lifecycle, rebuild, and range movement
  - [ ] Add compare-and-swap catalog promotion for index lifecycle transitions.
    Durable column-backed secondary-index metadata now carries `ready`,
    `building`, `catching_up`, `stale`, `rebuild_required`, `dropping`,
    `failed`, and legacy `invalid`; reads only plan against `ready` indexes,
    writes maintain `ready`/`building`/`catching_up` ordered-tuple entries, and
    stale/rebuild-required/failed/dropping/invalid states fail closed. Schema
    promotion helpers can now verify expected generation, access method, and
    schema fingerprint before marking an index ready, and Lite local rebuild
    uses that checked promotion after rebuilding.
  - [ ] Rebuild ordered indexes from committed packed rows using bounded,
    resumable cursors and publish only after all owner ranges reach the target
    generation. Scalar secondary-index rebuild workers now page local rebuild
    work from committed packed rows through a streaming store cursor, retain
    only the bounded page plus the next resume key, save durable
    `progress_row_key` / `completed_row_count` state, release leases between
    pages, resume from the saved cursor, and promote only after both worker
    planning and raft apply observe every current owner range ready for the
    target generation. Ordered-tuple rebuild pages now use the same committed
    row cursor, keep the full catalog available for tuple/include evaluation,
    delete stale tuple entries through the reverse by-doc namespace for only the
    bounded page span, and rewrite matching tuple entries from packed rows.
    Worker-level coverage now exercises bounded ordered-tuple progress, resume,
    reverse cleanup, include payloads, and named-index promotion; the remaining
    work is routed/range-movement integration and distributed durability
    evidence across remote workers.
  - [ ] Integrate ordered tuple entries with split/merge/range movement,
    donor fencing, rollback, and receiver cleanup. Reverse by-doc entries must
    be sufficient to clean outgoing index rows without scanning every index.
  - [ ] Add repair jobs that can verify and rebuild missing/stale index entries
    from packed rows without trusting derived artifacts.
  - [ ] Keep Lite/local rebuild behavior equivalent, minus distributed movement
    and hosted participant routing.

- [ ] Write-path performance hardening
  - [ ] Parse/destructure the packed old and new row once per upsert/delete and
    reuse cells across every row-derived write concern: ordered tuple keys,
    single-column predicates, unique/FK checks, partial predicates, generated
    expressions, algebraic projection, and text-search projection.
  - [ ] Batch key/value construction with scratch arenas or reusable buffers for
    tuple keys, reverse keys, serialized scalar cells, and mutation arrays while
    preserving ownership across transaction staging. Current ordered-tuple
    staging transfers generated forward/reverse keys and forward payload values
    into transaction-owned mutation buffers instead of duplicating them again,
    with a focused zero-allocation assertion around the owned staging helper.
  - [ ] Add fast paths for schemas with scalar ordered indexes, no partial
    predicates, no generated columns, and no FK/unique checks beyond primary
    identity; the current exact-row unchanged overwrite fast path only skips
    row-derived index maintenance when all indexed catalog columns are ready.
  - [ ] Extend selective overwrite maintenance from ordered tuple keys to
    include generated values, algebraic/text projections, and predicate
    membership before staging deletes/writes.
  - [ ] Extend the current ordered-tuple and scalar-index mutation-count
    assertions with allocation-count assertions and coverage for generated
    values, algebraic/text projections, multi-row delete batches, and routed
    prepare paths.

- [ ] Benchmark suite
  - [ ] Extend the `relational-index-bench-matrix` target beyond its current
    base-row, single-column, ordered-tuple, document-table, changed-overwrite,
    unchanged-overwrite, delete, partial single-column, partial ordered-tuple,
    partial-index membership-change, relational identity-rewrite, range,
    equality, ordered-pagination,
    insert-only, unique constraint probe, self-FK parent probe,
    low/high-selectivity, and exact/bounded/none-total dimensions to cover wider
    range-scan shapes and mixed text/algebraic/scalar filters. The full
    write-shape matrix is opt-in with `--matrix-write-scenarios all`; focused
    runs can use `--matrix-write-scenario`. The constraint-probe matrix is
    opt-in with `--matrix-constraint-probes all`; focused runs can use
    `--constraint-probe`.
  - [ ] Extend comparisons to algebraic filter doc-set scans and text-search
    intersections once those access-method planner capabilities are exposed.
  - [ ] Extend iterator-seek coverage from the current base-row and ordered
    tuple range-scan counters into search-runtime doc-set internals; current
    coverage includes wall time, stage/query allocation events and bytes,
    staged store writes/deletes, staged relational writes/deletes, staged
    write/delete bytes, iterator seeks for owned relational scans, index
    entries, candidate rows, candidate-gate limit/observed counts, hydrated
    rows, residual rechecks, ordered-tuple covering-payload projections,
    projected rows, selected access-method query counts in single-run and
    matrix output, ordered-tuple plan-shape counters for selected plan count,
    max key count, max equality-prefix length, max filter/proven/residual
    predicate counts, range-plan count, prefix-scan count, fallback flags
    including candidate gates, materialization caps, order mismatch, index not
    ready, stale generation, predicate-not-proven, and no-usable-bounds, and
    write-amplification ratios.
    Ordered tuple doc-set planning no longer uses the old exact-query
    `limit * 4` / 32-row cutoff as the fallback decision; it probes to the
    bounded materialization cap and reports the observed candidate count with a
    distinct `ordered_tuple_materialization_cap` fallback reason. Public
    row-query JSON accepts `total_mode: "exact" | "bounded" | "none"` and
    inexact responses include `total_exact: false`; requests can also set
    `profile: true` to include access method, fallback reason, counters, and
    ordered-tuple plan shape with predicate proof/residual counts in the
    response. Ordered tuple capability rejections are surfaced as deterministic
    fallback reasons for index-not-ready, stale-generation, predicate-not-proven,
    no-usable-bounds, and order field/direction/null-order/collation mismatch
    cases. Unordered non-partial ordered-tuple candidate-pipeline queries stream
    matching index entries directly into row recheck/candidate construction and
    stop at the requested page width when total mode is bounded or none.
  - [ ] Run and publish the full 10k/100k/1M low/high-selectivity matrix with
    exact/bounded/none totals before checking off any planner crossover claim.
    Document-table identity-rewrite equivalents still need a supported public
    write shape before that comparison can be included instead of explicitly
    skipped.
  - [ ] Expand the current opt-in matrix guardrails beyond ordered tuple
    fallback, write-amplification ratio, byte-write ratio, stage/total wall-time
    ratios, predicate-latency ratio, allocation count/byte ratios,
    iterator-seek ratios, residual recheck ratios, selected access-method
    presence, and staged mutation-array count ratios to cover any additional
    agreed production thresholds.

- [ ] Correctness and fail-closed evidence
  - [ ] Add focused storage tests for routed/Lite cleanup behavior.
  - [ ] Add malformed/stale catalog tests for missing access-method fields,
    unknown methods, mismatched schema fingerprints, lifecycle state handling,
    and remaining incompatible method option or partial-predicate semantics
    beyond the current column-backed secondary-index coverage for
    missing/unknown access methods, mismatched schema fingerprints, access-method
    mismatch fallback, durable lifecycle parsing/promotion/write-maintenance
    behavior, missing catalog-column references, and unsupported key-column
    collations.
  - [ ] Add public SQL/API parity rows for admitted index DDL and indexed query
    behavior, plus unsupported-reason rows for shapes that intentionally remain
    fail-closed.
  - [ ] Keep deterministic diagnostics tied to the native gap:
    unsupported-access-method, index-not-ready, stale-generation,
    predicate-not-proven, ordering-not-covered, or
    access-method-capability-mismatch.

- [ ] Observability and operational controls
  - [ ] Emit index-build, catch-up, repair, drop, and query-planner metrics with
    table, index, access method, generation, range, and fallback reason labels.
  - [ ] Add explain/plan output for index eligibility, rejection reasons,
    selected scan bounds, doc-set intersections, residual rechecks, hydration
    counts, and lifecycle generation.
  - [ ] Add admin/status APIs that list relational indexes, access methods,
    lifecycle state, generation, rebuild progress, stale ranges, error reason,
    and last benchmark/repair evidence where available.
  - [ ] Add rate limits/backpressure for rebuild and repair jobs so large index
    work cannot starve foreground writes or query hydration.
  - [ ] Add corruption/repair observability that distinguishes authoritative row
    corruption from disposable derived index corruption.

- [ ] Implementation comparison for trie/FST backends
  - [ ] Build a spike backend or benchmark harness that compares ordered KV
    tuple scans with algebraic dictionary/FST/trie-style lookup for equality,
    prefix, range, order-by, update churn, and delete cleanup.
  - [ ] Require the trie/FST candidate to prove ordered SQL semantics before it
    can implement `ordered_tuple`; otherwise keep it under `algebraic_filter` or
    `text_search`.
  - [ ] Compare memory, write amplification, read latency, rebuild cost,
    iterator behavior, and operational complexity against the ordered KV
    baseline.
  - [ ] Record the decision in `RELATIONAL.md` only after the benchmark and
    correctness evidence exists; do not track completed spike history here.
