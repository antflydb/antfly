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
  - [x] Add the first-class durable/runtime relational index catalog foundation.
    `TableSchema.relational_indexes` now persists index identity, owner kind,
    owner name, access method, uniqueness, key columns, expression keys,
    included columns, ordered key parts, lifecycle, generation, schema
    fingerprint, field predicates, and expression predicates. Runtime schema
    derivation populates catalog entries from indexed relational columns and
    unique constraints, with binary round-trip and runtime derivation coverage
    for scalar, ordered tuple, algebraic, text-search, and unique-constraint
    owner entries.
  - [ ] Migrate write/query/rebuild call sites to consume the first-class
    relational index catalog instead of reading column-backed or
    unique-constraint-backed metadata directly. Keep the old owner fields only
    as schema input/compatibility projection until every planner, maintenance,
    lifecycle, and repair path uses catalog entries. Write maintenance and
    rebuild policy construction now carry `TableSchema.relational_indexes` into
    `ColumnIndexPolicy`, synthesize effective scalar/ordered-tuple column
    metadata from catalog entries, and have focused coverage for catalog-only
    scalar rebuild plus catalog-only ordered-tuple write/rebuild behavior.
    Remaining consumers include query planning, unique/FK owner lookups, public
    schema JSON/API surfaces, lifecycle repair status, and legacy embedded-field
    removal.
  - [ ] Add method-specific catalog options once planner/runtime consumers need
    them, without storing physical backend names or SQL text in durable
    metadata.
  - [ ] Extend ordered tuple value encoding beyond scalar column keys to cover
    expression keys and planner coverage for descending/null-order-specific
    bounds.
  - [ ] Support included payload columns only as optional index-local payload
    for covering reads; the packed row remains authoritative and covering reads
    must recheck row generation before returning user-visible data. Query
    profiles and the benchmark matrix now distinguish rows projected from
    covering payloads from candidate payload entries rechecked against
    authoritative packed rows, so stale tuple/payload artifacts are observable
    as rechecks without trusted projection.

- [ ] Constraint-grade unique and foreign-key lookup on ordered indexes
  - [ ] Route unique-owner lookup for compound unique constraints through
    `ordered_tuple` metadata instead of single-column-only helpers.
    Participant write maintenance now keeps ordered-tuple entries for
    non-temporal, column-keyed unique constraints that carry `ordered_tuple`
    metadata, including partial predicate membership, include-payload rewrites,
    key-change cleanup, delete cleanup, identity rewrite cleanup, and
    ready/building/catching-up lifecycle gating; uniqueness enforcement still
    uses the dedicated owner namespace until write-time owner enforcement is
    replaced. Query candidate resolution now routes ready, non-temporal,
    column-keyed compound unique constraints through ordered-tuple metadata
    before the dedicated owner namespace, with profile evidence and a regression
    test that removes the legacy owner key and scalar indexes.
  - [ ] Enforce partial unique constraints by proving the proposed row satisfies
    the stored predicate, then probing only the matching ordered tuple owner.
  - [ ] Route FK parent existence checks to ordered tuple primary/unique owners
    for compound parent keys, including hosted participant routing where owner
    topology is required. Local participant FK parent checks now precompute the
    parent ordered tuple from decoded child cells for ready, non-temporal,
    column-keyed compound unique parents; commit validation probes ordered-tuple
    metadata and rechecks the authoritative packed parent row before accepting
    the child. Regression coverage removes the legacy unique-owner key to prove
    committed FK checks use ordered-tuple metadata, removes ordered-tuple entries
    to prove the ready ordered path fails closed, and proves same-batch
    parent/child inserts see staged ordered-tuple writes after removing the
    staged legacy owner write.
  - [ ] Route restrict parent-delete checks through child ordered indexes when
    a child FK maps to compound parent keys; fail closed when the matching child
    index is not ready or cannot prove predicate compatibility. Local
    participant restrict delete checks now use a ready, non-partial ordered
    child index that covers all FK child columns, encode the lookup tuple from
    authoritative parent row cells, and hydrate/recheck each candidate child row
    against the FK parent key before blocking the delete. Regression coverage
    removes legacy FK reference rows to prove the ordered child index path
    blocks a parent delete, and verifies a covering but stale child ordered
    index fails closed instead of trusting missing derived state. Simple partial
    child index predicates over FK child columns can now be proven from the
    mapped parent key before using the ordered child index; expression predicates
    or predicates over non-FK columns still fail closed, with focused coverage
    for both the proven and unproved partial-predicate cases.
  - [ ] Add concurrency tests for conflicting prepares on the same unique/FK
    owner tuple and stale generation tests that reject old index state. DB
    transaction coverage now proves conflicting prepares on the same compound
    ordered-tuple unique owner intent conflict before commit and reject
    duplicate writes after the winner commits. DB transaction coverage also
    proves compound FK child-reference and parent-delete prepares conflict on
    the same encoded parent tuple before commit and reject parent deletes after
    the child reference commits. Constraint lookup now rejects ready
    ordered-tuple metadata that lacks a current generation/fingerprint instead
    of trusting stale tuple state; focused FK parent coverage keeps the legacy
    owner absent and proves the stale ordered tuple is not accepted.

- [ ] Query planner ordered-index selection
  - [ ] Support ordered index range/order scans for non-default null ordering
    and descending keys; when any component is not covered, surface
    deterministic fail-closed diagnostics. Current storage coverage proves
    order-satisfying ordered-tuple pagination for default ascending keys,
    descending keys, and the lowered explicit `NULLS FIRST/LAST` shape where
    null placement appears as a companion null-test order key before the real
    field order. It also proves deterministic fallback reasons for direction,
    null-order, and collation mismatches. Ordered tuple range scans now
    constrain every range key to the encoded non-null component domain before
    applying scalar bounds, with focused coverage for one-sided ranges over
    `ASC NULLS FIRST` and `DESC NULLS LAST` indexes that keep null rows out of
    the proven candidate set.
  - [ ] Satisfy `ORDER BY` from ordered indexes only when direction, nulls,
    collation, predicate domain, and tie-breaker semantics are exact.
  - [ ] Use selectivity estimates or bounded probe accounting to choose between
    ordered index scan, algebraic/text doc-set intersection, and base-row scan.
  - [ ] Make residual predicate recheck explicit in the plan for every ordered
    scan where the index does not fully prove the typed filter.
  - [ ] Add plan-summary evidence that names the selected access method,
    generation, predicate proof, scan bounds, estimated candidates, residual
    predicates, and fallback reason. Rows query profiles now carry and serialize
    `estimated_candidate_rows` alongside measured candidate rows, gate
    observations, residual rechecks, ordered-tuple plan metadata, and a stable
    `plan_summary` string naming access method, fallback reason, generation,
    predicate proof counts, scan-bound byte widths, estimated candidates, and
    measured row counters; remaining work is cross-method selectivity evidence.

- [ ] Access-method catalog and SQL lowering
  - [ ] Lower ordinary `CREATE INDEX` and `CREATE UNIQUE INDEX` to
    `ordered_tuple` by default, including key expressions, include columns,
    partial predicates, direction, collation, null ordering, and lifecycle
    options.
  - [ ] Lower `USING antfly_algebraic` to `algebraic_filter` access metadata or
    native algebraic materialization metadata, depending on whether the DDL asks
    for a filter index or a fold/materialization. `USING antfly_algebraic ()
    WITH (derive_from_schema = true)` now has SQL/API parity evidence as
    native `algebraic_filter`; unknown public SQL options such as internal
    `materializations` now fail closed with unsupported fixture evidence, and
    fold/materialization-specific SQL options remain tracked here.
  - [ ] Lower `CREATE TEXT SEARCH` and `USING antfly_full_text` to the
    `text_search` access method with analyzer, scoring, highlight/snippet, and
    segment lifecycle config. `CREATE TEXT SEARCH [IF NOT EXISTS] name ON table
    (field) [WITH (...)]` now parses and lowers to the same durable full-text
    derived-index metadata as `USING antfly_full_text`, with SQL/API parity
    coverage. The lowerer persists typed `analyzer`, `scoring`, `highlight`,
    `snippet`, and `segment_lifecycle` metadata and rejects unknown or mistyped
    full-text options, with SQL/API parity fixtures for both admitted metadata
    and rejected option families; planner/runtime ranking, highlight/snippet
    production, and segment lifecycle execution remain tracked here.
  - [ ] Reject physical backend spellings unless they map to a proven native
    access-method capability. Default and explicit SQL `btree` currently lower
    to durable `ordered_tuple` metadata, non-unique generated-expression
    secondary indexes lower through stored generated columns that carry
    `ordered_tuple` self-key metadata including direction/null ordering, `gin`
    lowers to algebraic JSON/array index metadata where validated, public
    SQL/API fingerprints report native access-method names rather than physical
    SQL spellings for these cases, and unsupported names such as `gist`/`trie`
    fail before they can enter durable catalog state. Remaining expression-key
    catalog work is compound expression-key coverage, unique expression-key
    ordered owners, and planner use of expression-key bounds beyond the stored
    generated-column representation.
  - [ ] Add catalog JSON/API round-trip tests and SQL/API parity fixtures for
    each admitted access method and each unsupported option family. Binary
    runtime catalog round-trip coverage now exists for the first-class
    `relational_indexes` shape, and runtime derivation coverage exists for
    admitted `scalar_column`, `ordered_tuple`, `algebraic_filter`, and
    `text_search` methods; remaining work is exposing/validating the same
    first-class shape through public schema JSON and SQL/API fixtures rather
    than only deriving it from existing column/constraint inputs.

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
    DB split lifecycle coverage now proves ordered-tuple entries are rebuilt for
    the split destination, restored for retained source rows after physical
    left-range rewrite, and removed from the source for moved rows using the
    captured tuple plus reverse-by-doc namespace. Merge-style cutover coverage
    now proves ordered-tuple entries survive receiver reopen and are absent from
    the fenced donor after reopen. Remaining work is distributed donor rollback,
    receiver cleanup under failed movement, and hosted routed repair evidence.
  - [ ] Add repair jobs that can verify and rebuild missing/stale index entries
    from packed rows without trusting derived artifacts. Local storage now has
    a range-scoped column-backed repair primitive that deletes reverse-linked
    derived entries, scans ordered-tuple forward entries to catch missing
    reverse-by-doc corruption, prunes missing-row tuple/scalar artifacts, and
    rebuilds scalar plus ordered-tuple entries only from committed packed rows;
    the concrete DB runtime now exposes that repair through the same durable
    mutation gate as secondary-index rebuild, with focused coverage for scalar
    and ordered-tuple corruption repaired from authoritative packed rows plus
    read-only fail-closed behavior. Table-write sources now expose a typed local
    group repair hook for provisioned and hosted-local execution, and hosted
    remote group owners route through the same internal HTTP group-write surface
    with focused route/client coverage for bounded repair requests. A bounded
    table-write repair worker pass now iterates authoritative catalog owner
    ranges, invokes the local/hosted group repair hook, aggregates repair
    counters, reports missing owners, and honors `max_work_units` admission so
    service maintenance can throttle repair work. The public table repair route
    now exposes that bounded worker pass, dispatches through the qualified
    `database / namespace / table` catalog target instead of a bare table name,
    caps per-request work units so operator-initiated repair cannot request
    unbounded foreground work, declares admin permissions for both default and
    namespace-qualified repair routes, and is present in the modular/root
    OpenAPI contract plus generated public/metadata/client bindings. The DB
    runtime now has a durable relational-index repair job record keyed by job id
    that persists target database/namespace/table, worker intent, bounded pass
    limits, resume cursor, last pass counters, aggregate repair report, status,
    completion, attempts, and last error across close/reopen, with read-only and
    HA standby mutation-gate coverage. Public repair requests now accept an
    optional `job_id`; when present, the handler fails closed unless the source
    persists the job begin record, runs the bounded repair pass, and records pass
    counters/resume cursor back to the DB-backed ledger. Provisioned source
    coverage proves that ledger state survives owner-group DB reopen. Public
    default and namespace-qualified repair-job status routes now read durable
    ledger records through the table-write source, verify the persisted
    database/namespace/table target before returning status, require table-admin
    permissions, and are present in root/modular OpenAPI plus generated
    public/metadata/client bindings. Remaining work is production queue-level
    rate limiting/backpressure and aggregate repair metrics in broader
    public/admin status surfaces.
  - [ ] Keep Lite/local rebuild behavior equivalent, minus distributed movement
    and hosted participant routing. Lite native `.aflite` coverage now proves
    ordered-tuple document-range cleanup and local column-backed repair survive
    close/reopen, including repair of missing ordered-tuple/scalar entries and
    cleanup of stale forward-only tuple corruption from committed packed rows.
    Remaining work is Lite evidence for every future local rebuild/repair
    operation added under this slice.

- [ ] Write-path performance hardening
  - [ ] Parse/destructure the packed old and new row once per upsert/delete and
    reuse cells across every row-derived write concern: ordered tuple keys,
    single-column predicates, unique/FK checks, partial predicates, generated
    expressions, algebraic projection, and text-search projection. Current
    participant writes reuse decoded cells for primary/unique/FK checks,
    scalar/ordered column index maintenance, and ordered-tuple unique constraint
    maintenance; generated values plus algebraic/text projections remain
    tracked here.
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
    entries, candidate rows, candidate-gate limit/observed counts,
    candidate-gate exceeded counts, hydrated rows, residual rechecks,
    ordered-tuple covering-payload projections, projected rows, selected
    access-method query counts in single-run and
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
    response. Candidate-gate profile output now distinguishes measured probes
    from cap-exceeded probes through `candidate_gate_exceeded` and plan-summary
    `candidate_gate=observed/limit/status` evidence; the matrix carries an
    explicit `--matrix-max-ordered-tuple-candidate-gate-exceeded-count`
    guardrail, emits the threshold at matrix start, includes the observed count
    in ordered-vs-single comparisons, and reports the observed/max pair on
    regression rows. `zig build batch-bench-build` now provides a compile-only
    guard for benchmark changes without running the workload. Ordered tuple
    capability rejections are surfaced as deterministic fallback reasons for
    index-not-ready, stale-generation, predicate-not-proven, no-usable-bounds,
    and order field/direction/null-order/collation mismatch cases. Unordered
    non-partial ordered-tuple candidate-pipeline queries stream
    matching index entries directly into row recheck/candidate construction and
    stop at the requested page width when total mode is bounded or none. Query
    profiles and benchmark output now also report covering-payload recheck
    counts separately from covering-payload projections, proving covering rows
    are still gated by authoritative packed-row validation.
  - [ ] Run and publish the full 10k/100k/1M low/high-selectivity matrix with
    exact/bounded/none totals before checking off any planner crossover claim.
    Document-table identity-rewrite equivalents still need a supported public
    write shape before that comparison can be included instead of explicitly
    skipped.
  - [ ] Expand the current opt-in matrix guardrails beyond ordered tuple
    fallback, write-amplification ratio, byte-write ratio, stage/total wall-time
    ratios, predicate-latency ratio, allocation count/byte ratios,
    iterator-seek ratios, residual recheck ratios, candidate-gate exceeded
    counts, selected access-method presence, and staged mutation-array count
    ratios to cover any additional agreed production thresholds.

- [ ] Correctness and fail-closed evidence
  - [ ] Add focused storage tests for routed/Lite cleanup behavior. Direct
    storage coverage now proves document-range cleanup follows ordered-tuple
    reverse-by-doc entries and removes both forward and reverse tuple rows while
    preserving adjacent documents; Lite native `.aflite` coverage now proves
    ordered-tuple document-range cleanup survives close/reopen and preserves
    adjacent forward/reverse tuple rows. DB split/merge lifecycle coverage now
    proves routed ordered-tuple entries are rebuilt, retained, or removed across
    split source/destination and merge receiver/donor reopen flows; distributed
    rollback/failure-path movement evidence remains tracked here.
  - [ ] Add malformed/stale catalog tests for missing access-method fields,
    unknown methods, mismatched schema fingerprints, lifecycle state handling,
    and remaining incompatible method option or partial-predicate semantics
    beyond the current column-backed secondary-index coverage for
    missing/unknown access methods, missing generations, half-declared shared
    index peers, query-side rejection of ready ordered-tuple metadata missing a
    generation or schema fingerprint, mismatched schema fingerprints,
    access-method mismatch fallback, write-side ordered-tuple column
    maintenance rejection before staging any mutations when ready/building
    metadata lacks a generation or schema fingerprint, durable lifecycle
    parsing/promotion/write-maintenance behavior, missing catalog-column
    references, and unsupported key-column collations.
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
    and last benchmark/repair evidence where available. The existing table
    index list/detail encoder now appends schema-backed relational index entries
    from authoritative table schema metadata, including source, access method,
    lifecycle, generation, schema fingerprint, keys, include columns, predicate
    counts, rebuild range counts, stale-generation range counts, completed-row
    progress, resume key, per-range state, and last error. Schema-backed
    relational single-index detail now resolves through table schema metadata
    instead of requiring legacy `indexes_json` entries. Public index status now
    has a typed `index_type: "relational"` OpenAPI stats variant and table
    index list/detail responses include aggregate durable repair-job evidence:
    job counts, active/completed/failed counts, total scanned/repaired/missing
    ranges, latest job status, and aggregate repair counters. Default and
    namespace-qualified repair-job status endpoints still expose explicit job
    detail with OpenAPI/client coverage. Remaining work is last benchmark
    evidence, metrics labels, and broader admin/status API promotion.
  - [ ] Add rate limits/backpressure for rebuild and repair jobs so large index
    work cannot starve foreground writes or query hydration. Public repair
    requests now have capped per-request admission, but shared queue scheduling,
    tenant/table-level budgets, and foreground latency feedback remain open.
    Secondary-index rebuild and column-backed repair worker passes now share the
    same admission-policy surface as schema rewrite and table-emptying passes,
    including invalid-policy rejection, bounded work units, and stale-lease
    takeover gating where the job type has lease state. API catalog maintenance
    wake jobs now use a shared in-flight table registry for schema rewrite and
    table-emptying catch-up, so one table cannot queue or run both maintenance
    job classes at once through the session-maintenance scheduler.
  - [ ] Add corruption/repair observability that distinguishes authoritative row
    corruption from disposable derived index corruption. Local storage coverage
    now proves disposable ordered-tuple corruption with missing reverse entries
    and missing valid tuple entries can be repaired from packed rows, and DB
    runtime coverage proves missing scalar entries plus ordered-tuple artifacts
    are repaired through the public storage boundary. Public relational index
    list/detail status now surfaces aggregate durable repair-job counters for
    disposable derived-index repair evidence; durable metrics labels and
    authoritative-row corruption reporting remain open.

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
