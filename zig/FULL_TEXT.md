# Full-Text Indexing

## Goal

Full-text visibility semantics stay aligned with the LSM path:

- write/sync waits for search visibility, not full compaction
- segment merges run as background maintenance
- explicit force-compaction stays available as an admin/test hammer

Benchmark methodology and the Tantivy comparison contract live in
[bench/full_text/BENCHMARK.md](bench/full_text/BENCHMARK.md).

## Current Policy

1. `SyncLevel.full_text` means the affected documents are searchable in the
   relevant full-text indexes.
2. Scheduled text merges are background work owned by the text-merge runtime and
   its resource budgets.
3. `runUntilIdle()` drains scheduled text merges as part of idle maintenance.
4. `drainScheduledTextMerges()` is the explicit blocking API for "finish the
   merges that are already scheduled".
5. `forceCompactTextIndexes()` remains the heavy hammer that aggressively
   rewrites text indexes beyond the normal merge scheduler. It reserves
   text-merge buffers through the normal resource-budget path (`.strict`
   mode), so its memory behavior is observable alongside scheduled merges
   instead of silently bypassing the budget.
6. `bestEffortForceCompactTextIndexes()` runs the same merge loop in
   `.best_effort` mode: it defers or stops as soon as it would exceed the
   text-merge resource budget or the scheduler reports resource pressure,
   leaving the remaining merge debt scheduled (`TextMergeScheduler.schedule`)
   instead of forcing all the way to a single segment.

## Why

The old shape mixed two different concerns:

- search visibility / replay catch-up
- segment merge compaction

That made `full_text` sync heavier than it needed to be and hid the real cost of
forced compaction in normal maintenance timings. Splitting them keeps sync
latency tied to visibility only, while merge cost is amortized as background
maintenance.

## Field Layout

Full-text field names follow Elasticsearch-style multi-field naming rather than
the older Zig-only convention.

- Exact string companions use `.keyword`.
- `search_as_you_type` emits `._2gram`, `._3gram`, and `._index_prefix`.
- `._2gram` and `._3gram` are shingle fields for multi-token
  search-as-you-type matching.
- Prefix/autocomplete matching targets `._index_prefix`; it is
  phrase-prefix oriented and indexes edge n-grams over
  one-, two-, and three-token shingles so prefixes such as `brown f` and
  `quick brown f` can match without scanning stored documents. This is closer
  to Elasticsearch's `search_as_you_type` prefix behavior than plain per-token
  edge n-grams.
- Schema-less string indexing emits both the analyzed field and a bounded
  `.keyword` companion so term/terms filters can use postings without widening
  vector queries through stored-document scans.
- Public filter rewriting maps term/terms filters to `.keyword` only when
  the target text snapshot contains that field. If the postings are absent, the
  query falls back or fails closed rather than treating the missing field as
  a valid empty result.

This intentionally breaks from the old Zig-only `__keyword` / `__2gram` suffixes
and from the Go/Bleve naming: the public query surface, schema-derived fields,
dynamic-template variants, and schema-less exact fields all use one familiar
subfield convention, adopted before compatibility constraints would have made
the layout expensive to change.

## Search-As-You-Type Design

The JSON Schema surface stays valid JSON Schema; the standard `type` field is
not overloaded with Elasticsearch field types. The shorthand is:

```json
{
  "type": "string",
  "x-antfly-types": ["search_as_you_type"]
}
```

When configuration is needed, a separate extension object is used instead of a
list of typed objects inside `x-antfly-types`:

```json
{
  "type": "string",
  "x-antfly-field": {
    "type": "search_as_you_type",
    "analyzer": "standard",
    "max_shingle_size": 3,
    "fields": {
      "keyword": { "type": "keyword" }
    }
  }
}
```

`x-antfly-types` stays a compact compatibility shorthand that desugars to
`x-antfly-field` defaults. A list of objects inside `x-antfly-types` would turn
the extension into a second mapping DSL and make validation, schema diffing,
dynamic templates, and compatibility harder — that shape was rejected.

`search_as_you_type` uses the Elasticsearch default `max_shingle_size = 3`
internally and does not expose a public knob.

The query surface uses an Elasticsearch-style `multi_match` query with
`type: "bool_prefix"` rather than an Antfly-only standalone bool-prefix
operator:

```json
{
  "full_text_search": {
    "multi_match": {
      "query": "quick brown f",
      "type": "bool_prefix",
      "fields": ["name"]
    }
  }
}
```

For `search_as_you_type` root fields, Antfly expands the root field to the
generated autocomplete fields:

```json
{
  "full_text_search": {
    "multi_match": {
      "query": "quick brown f",
      "type": "bool_prefix",
      "fields": ["name", "name._2gram", "name._3gram"]
    }
  }
}
```

The explicit generated-field form is also accepted, for Elasticsearch
familiarity and advanced scoring control. The shorthand `fields: ["name"]`
remains the normal Antfly path so users do not have to manually list generated
subfields for autocomplete.

Internally this lowers to the existing boolean, term, and prefix query
machinery. Completed terms match the root and shingle fields, and the final
partial phrase is satisfied through `._index_prefix`.

## Segment Build

The segment builder writes stored docs, ordinal sidecars, inverted sections, and
typed doc values directly. It does not retain a full intermediate `Batch`.
Temporary per-document term maps borrow analyzer token slices until the document
has been added to each field's inverted builder; the inverted builder then
re-keys terms into its own arena for segment lifetime ownership.

When the mapper has already parsed and sanitized a document for full-text
projection, it passes that parsed typed source into the segment builder instead
of materializing an intermediate typed-field array. Raw schemaless fast-path
documents explicitly skip typed collection.

Built per-field inverted sections are transferred into the segment writer
without a second section-buffer copy. Stored JSON is borrowed by the segment
writer until `build()` finishes.

Stored fields use a v3 offset-table layout that keeps O(1) doc lookup but writes
raw stored JSON instead of compressing each tiny document independently. Older
v2 per-doc Snappy segments still read and merge correctly.

Text analysis and per-document term accumulation use a reusable
document-local arena. Analyzer tokens, temporary per-field term maps, position
lists, and materialized `TermHit` slices are released by resetting the arena
between documents after the persistent inverted builders have copied the needed
terms and positions. This keeps allocator churn out of the large schemaless
replay path without changing term ownership in the durable segment.

Documents whose text projection has no duplicate field names use a direct field
path: analyze one field, aggregate that field's terms, and add it to the
persistent inverted builder immediately. The older per-document field map path
is still used when repeated field names must be concatenated into one logical
field with shared positions.

For short fields whose analyzed token list has no repeated terms, the direct
field path skips the document-local term hash map entirely and emits one
`TermHit` per token. This follows the Lucene/Tantivy shape of avoiding generic
maps for the common "few unique tokens" document path, while keeping the
hash-map path for repeated terms and unusual analyzers.

Segment assembly stores each section's offset/length directly on the writer's
section records while appending section bytes. The final section index does not
build a separate location list or scan that list for every field section.

Field analyzer resolution is cached for the duration of one segment build, so
repeated dynamic field names do not rescan the text-analysis config for every
document.

Analyzer token-list builders return owned slices directly instead of
duplicating the temporary token array. The lowercase filter also skips allocation
for tokens that contain no ASCII uppercase bytes, and the stop-word filter
passes through the original token slice when it removes nothing.

The default English analyzer has a fused `unicode_words -> lowercase ->
stop_words -> Porter2` path. It tokenizes once, drops stop words before owning
term bytes, and only allocates the output terms that survive analysis. Porter2
also has a conservative final-byte precheck so generated terms whose suffix
cannot be modified skip the stemmer's region scans.

`IndexWriter.addSegmentWithIdData` updates append-only BM25 field-length stats
incrementally by cloning the previous snapshot stats and reading only the new
segment's inverted-section headers. Segment replacement and merge paths still
rebuild stats from the replacement segment list because those operations remove
or reorder existing segment entries.

File-backed persistent text segments do not write the full segment bytes to
the segment WAL. The segment file is atomically published before active metadata
is committed, so crash recovery can treat pre-commit files as harmless orphans
and post-commit metadata as authoritative. Inline segment storage still uses the
WAL because the segment bytes live inside the metadata store transaction.

## Observability

Segment-builder internals are reported through the `antfly_bench_text_index`
benchmark log when `ANTFLY_BENCH_METRICS=1` is enabled, including analyzer time,
term accumulation, term-hit materialization, typed-field collection/build,
segment encoding, token counts, term-hit counts, and emitted segment bytes. The
same log separates section attachment, stored-doc attachment, stored-doc
compression, and final segment assembly from the broader `segment_encode_ms`
bucket. `antfly_bench_text_publish` logs WAL, metadata, writer-publish, and
truncate timings for the file-backed segment publish path.

`replay_bench.zig` reports per-index segment counts and bytes at each stage of
a replay/catch-up run (`text_index`, `text_index_before`,
`text_index_after_idle`, `text_index_after_force_compact`), plus text-merge
scheduler counters (pending segments, pending bytes, completed merges,
backpressure events) and resource-manager slice stats for
`full_text_pending_segments` (used/peak bytes, soft-limit events, hard-limit
rejections, pressure state).

Use this breakdown before changing analyzer or segment layout code. The
optimization order, highest-leverage first, is:

1. remove duplicated typed-field work and stored-JSON reparsing
2. reduce analyzer token allocation/copying
3. reduce per-document term-map churn
4. improve segment encoding pre-sizing and remaining final-output copy behavior

## Search Execution Architecture

The boolean iterator tree, the competitive two-phase phrase executor, and the
segment/codec/allocation optimizations described in Search Execution Design below
are implemented, in `pkg/antfly/src/search/scorer.zig` and
`pkg/antfly/src/section/inverted.zig`.

> **Relocated:** The dated implementation-progress log that previously lived here (2,310 lines, 2026-07-12 through 2026-07-16) is preserved verbatim in [work-log/completed/full-text/implementation-progress-2026-07.md](../work-log/completed/full-text/implementation-progress-2026-07.md). Durable decisions from it are in Search Execution Design, Decisions, and Design Constraints in this document.

### Capabilities already present

- `pkg/antfly/src/search/scorer.zig` contains `WANDScorer`, a shared top-k
  collector interface, block-max impact evaluation, and chunk skipping.
- Ranking-only term iterators disable position decoding.
- WAND advancement calls the postings iterator's `advanceTo` implementation.
- `pkg/antfly/src/index.zig` computes global BM25 statistics and searches each
  segment against a shared global collector. Deleted documents are rejected by
  the live-doc collector.
- `pkg/antfly/src/section/inverted.zig` owns the inverted-index encoding,
  postings iterators, norms, positions, block-max data, term dictionary, and
  segment merge implementation.
- The embedded search-kernel benchmark (see [bench/full_text/BENCHMARK.md](bench/full_text/BENCHMARK.md))
  drives these components directly through dedicated indexing and query
  executables, without a separate search implementation.
- `DB.forceCompactTextIndexes()` and scheduled-merge drains provide separate
  maintenance controls.

### Execution status

- V1 term, union, intersection, and phrase queries use the production postings
  iterators and bounded global top-k collector. They do not use the
  `executeQueryAllScored` hash-map/full-sort fallback reserved for unsupported
  product shapes.
- Boolean advancement delegates to the postings iterator's seek/skip path;
  fixed-size stack workspaces cover normal small queries.
- Phrase execution uses competitive BM25 scoring and defers position decoding
  until a document survives the cheaper term-level tests. It does not
  materialize the complete phrase hit set.
- Block-Max WAND shares its threshold across segments. Highly fragmented
  snapshots additionally compute query-specific segment bounds, order segments,
  and reject segments whose strict upper bound cannot enter the result.
- Exact counts and bounded top-k are separate plans. Competitive pruning may
  honestly return only a lower-bound total relation; it is never promoted to an
  exact count.

## Search Execution Design

Correctness and benchmark changes land before interpreting optimization
results. The engine changes below are organized in implementation order.

### Profiles and Regression Gates

Add query-class-specific baselines and phase counters before changing executor
architecture. Capture CPU profiles and allocation profiles for representative
term, union, intersection, phrase, and mixed boolean queries in both segment
modes.

Acceptance:

- full-corpus correctness preflight passes;
- raw samples and environment metadata are retained;
- each target query has a dominant-cost explanation; and
- a regression threshold can be evaluated independently per query class.

### Kernel Search Boundary

Expose a narrow internal search API that acquires an immutable text snapshot,
executes a typed query, and returns native corpus ordinals and scores. It must
bypass DB query-envelope processing, MVCC constraint derivation, public hit
projection, and stored-body loading while still calling the production search
and scorer code.

Do not remove those concerns from the public server path. Their cost belongs in
Benchmark B.

Acceptance:

- the kernel output matches the DB path for a static, fully visible index;
- no stored JSON decompression occurs in the kernel query path;
- stable IDs are returned without per-hit key lookup where possible; and
- the same scorer and postings code serves kernel and DB execution.

### Boolean Query Execution

Introduce composable iterator/scorer primitives:

- `ConjunctionScorer`: lead with the rarest required iterator and seek all
  other required iterators to its candidate;
- `DisjunctionScorer`: Block-Max WAND over optional terms;
- `ReqOptScorer`: required match with optional score contribution;
- `ExclusionScorer`: seek a prohibited iterator or consult a prepared bitmap;
- `MinShouldMatchScorer`: track optional matches without per-document hash-map
  materialization; and
- a shared top-k collector with a live competitive threshold.

First, change the existing simple boolean fast path to use the underlying
postings `advanceTo` operation. Then lower all benchmark `UNION` and
`INTERSECTION` queries into the iterator tree. Retain a correctness-first
fallback for unsupported public query shapes until each shape has equivalent
tests.

Exact count and top-k should be separate plans. A top-k scorer may use
competitive pruning and return `total_hits_relation = gte`; an exact count plan
must visit or bitmap-combine enough postings to prove the exact total. Do not
silently report a pruned WAND count as exact.

Acceptance:

- union/intersection golden results and scores match the old executor;
- the benchmark grammar never calls `executeQueryAllScored`;
- top-k memory is bounded by query/segment state plus `O(k)`, not match count;
- exact counts retain exact semantics; and
- term-query performance does not regress outside its agreed threshold.

### Two-Phase Phrase Execution

Phrase execution should use:

```text
rarest-term or conjunction approximation
              |
              v
       candidate document
              |
              v
      position verification
              |
              v
       BM25 score/top-k
```

Decode positions only for candidate documents that survive the approximation.
Define the score semantics explicitly and match the configured Tantivy phrase
behavior. Phrase counts may use the same verifier without allocating scored
hits. Phrase top-k should feed the global collector and avoid sorting all
matches.

Acceptance:

- exact phrase IDs/counts pass cross-engine fixtures;
- phrase score behavior is documented and verified;
- position-decode counters fall in selective workloads;
- memory no longer scales with total phrase matches for top-k; and
- phrase, repeated-term phrase, and cutoff-tie cases are covered.

### Segment-Level Competitive Pruning

Compute conservative segment score upper bounds for the active query. Order
segments by likely competitiveness and skip a segment only when its upper bound
cannot beat the global collector threshold. Continue using global document
frequency and average-length statistics for BM25 consistency.

Cache immutable per-snapshot term statistics and query-independent segment
metadata. Do not cache final query results in the kernel benchmark.

Acceptance:

- upper bounds are proven conservative by tests;
- reordered/skipped execution produces identical top-k results;
- multi-segment work counters decrease on selective workloads; and
- single-segment behavior remains unchanged.

### Postings and Block-Max Layout

Use `search_benchmark_codec_bench.zig`, `wand_skip_bench.zig`, full-corpus
profiles, and index-size measurements to evaluate:

- postings/block size;
- StreamVByte or alternative vectorized decode paths;
- skip metadata density;
- block-impact representation and quantization;
- norm access locality;
- memory mapping and prefault behavior; and
- term-dictionary lookup/cache locality.

Every format change must version the persisted section, retain corruption
checks, include merge/reopen tests, and report both speed and size. A microbench
improvement is insufficient if full-corpus latency, RSS, or index size regresses
materially.

### Query Setup and Allocation Cost

Once the iterator architecture is stable:

- reuse query-local scratch buffers;
- avoid sorting term-state indices from scratch when a small incremental
  structure performs better;
- cache immutable analyzer and global-stat data at snapshot scope;
- keep ownership explicit across snapshot replacement;
- avoid per-hit hash entries and temporary scored arrays; and
- distinguish parser/analyzer cost from postings execution in reporting.

Do not parse queries ahead of the timed region unless all compared engines are
also given pre-parsed queries. Server benchmarks always include normal request
parsing.

### Merge Policy and Observability

- Expose per-index segment count and per-segment sizes through a read-only
  internal status surface.
- Make force-compaction completion and its resulting invariant observable.
- Report merge bytes read/written, elapsed time, peak memory, fan-in, and debt.
- Tune production tiering using both write amplification and multi-segment
  search cost.
- Keep scheduled maintenance distinct from explicit force compaction, as
  defined in `FULL_TEXT.md`.

### Compaction Candidate Scoring

LSM compaction candidate scoring uses normalized `current / target` pressure
per level, for L0 and lower levels alike, rather than absolute run-count debt;
the maintenance entry point applies this comparison globally instead of
special-casing L0 once it exceeds its soft limit. Overlap compaction remains
eligible at the four-L0-run soft bound, but its pressure score is computed
against that same bound so plain eligibility cannot outrank a level that is
further over target. L0 compaction windows drain toward half the trigger,
bounded by `max_compaction_input_bytes`, instead of the full `2 * l0_limit`
source cap; the `l0_limit = 0` repair case keeps oldest-pair selection.

### Deletion and Merge Concurrency

Full-text replay mutates each segment's shared Roaring deletion bitmap while
holding the per-index apply mutex. Background merge-task creation takes that
same per-index apply mutex before reading deletion cardinalities or cloning
deletion metadata, rather than relying on the DB-wide apply lock alone; if
replay is active for an index, task creation defers that index and tries
another rather than blocking while holding the DB-wide lock, and releases the
per-index mutex before merge work runs. Merge execution operates only on the
task-owned bitmap clone, never the live shared bitmap. Publication reacquires
the per-index mutex, validates that the frozen source view is still current,
and atomically swaps in the merged segments only if so, before releasing the
mutex — keeping expensive merges fully concurrent with indexing without losing
deletion consistency.

## Design Constraints

### Exact counts versus WAND

Competitive pruning can prove that a document cannot enter top-k without
proving whether it matched. Therefore a WAND top-k result may only have a lower
bound for total hits. Exact count requests need a separate exact plan or a
combined plan that performs the required additional work. The API relation must
remain honest.

### Cutoff ties

Equal BM25 scores can produce different but equally valid kth documents when
engines use different internal document orders. Verification must compare the
tie equivalence set rather than weakening all top-k checks to an arbitrary
overlap percentage.

### Benchmark-only fast paths

A narrow kernel API is acceptable; a separate scorer is not. Any optimization
used to claim Antfly kernel performance must be reachable by the production
search path under equivalent query semantics.

### Segment identity and merging

Internal segment doc numbers can change after merging. Stable benchmark
ordinals must survive merge/reopen and be resolved without stored-body loading.
Tests must cover deletes and updates so an ordinal never points to an obsolete
version.

### Visibility

The kernel benchmark freezes a fully visible snapshot and excludes MVCC work.
The server benchmark must retain normal visibility semantics. Improvements to
native live-document/ordinal masks should benefit the server benchmark without
weakening transaction behavior.

### Schema-progress and table-status reads

Schema-progress reconciliation consults the target generation's durable
`rebuild.state` marker before opening the DB. The marker's presence is an
authoritative not-ready result, so the normal multi-minute migration costs
only a tiny marker read per lifecycle round instead of a full DB open; a
regression that creates only the marker with no DB beneath it must fail the
probe rather than allow a DB open to proceed. Table-status endpoints (`GET
/tables`, `GET /tables/:name`) read only the already-published runtime-status
LSM snapshot and never acquire a normal table-read lease for observability, so
normal read admission during schema backfill cannot block them; a temporarily
stale or absent optional LSM field is preferable to making catalog status
unavailable during maintenance. A nonzero cached status count proves
non-emptiness even when stale, but only a fresh zero snapshot proves an empty
table — an explicit `startup_catch_up/opening` zero snapshot must omit
optional storage status rather than render a false `empty=true`.

### Format evolution

Postings and block-max changes affect persistent compatibility. All experiments
must retain version dispatch for formats that have actually shipped. At the
start of this work, `origin/main` both writes and accepts exactly inverted-index
wire format v23. The production upgrade contract is therefore v23 to the
accepted v38 layout. Intermediate v24-v37 formats created only during this
branch's experiments are not release contracts: the production reader rejects
them rather than carrying their codecs indefinitely. Benchmark artifacts that
use those formats may be inspected with the corresponding historical binary or
an isolated analysis tool.

## Decisions

These were resolved explicitly rather than implicitly in code:

1. The external `search-benchmark-game` protocol was extended with
   verification commands (`VERIFY_TOP_N`, `VERIFY_TOP_N_COUNT`), backed by a
   companion Python verifier (`tools/verify_search_benchmark.py`); see
   [Correctness protocol](bench/full_text/BENCHMARK.md#correctness-protocol).
2. Stable corpus ordinals use a dedicated benchmark-visible native
   ordinal/doc-value mapping (`corpus_ordinal`), also used in production; see
   [Contract](bench/full_text/BENCHMARK.md#contract).
3. The analyzer configuration is explicit and declared in the manifest (for
   example `ascii_lowercase`), and the `ANALYZE` protocol compares exact
   token, position, and byte-offset output between engines; see [Analyzer and
   scoring equivalence](bench/full_text/BENCHMARK.md#analyzer-and-scoring-equivalence).
4. Phrase frequency contributes to BM25: phrase scoring matches Tantivy's
   semantics (sum constituent-term IDFs including repeated terms, use exact
   phrase occurrence count as BM25 frequency, apply the field norm once).
5. The primary top-k benchmark permits `total_hits_relation = gte`; exact
   totals are a separate operation and are never inferred from competitively
   pruned execution.
6. Both `single` and `production` segment modes are used for cross-engine
   comparison, each explicitly declared and never mixed; see [Segment
   modes](bench/full_text/BENCHMARK.md#segment-modes).
7. Hardware class and noise controls are standardized by the [Timing
   procedure](bench/full_text/BENCHMARK.md#timing-procedure) (pinned/recorded hardware and build
   settings, declared warmup, at least five repetitions).
8. Server comparators are mapped to the three named durability profiles
   (`unsafe-throughput`, `process-durable`, `machine-durable`); see
   [Durability and recovery](bench/full_text/BENCHMARK.md#durability-and-recovery).
9. Positions within a posting share one bit width per group of eight
   documents, packed as one contiguous bitstream rather than rounded to
   per-document bytes; the frequency column carries each document's value
   offset so phrase seeks decode only the candidate document. See [Postings
   and Block-Max Layout](#postings-and-block-max-layout).
10. Posting blocks are fixed at 128 documents. Per-block document counts are
    derived (block ordinal is its metadata-array index; the final block's
    count is the exact term document frequency) rather than persisted, and
    only maximum document ID and payload-end delta are stored per block.
11. A term occurring in exactly one document uses an inline compact posting
    record — a discriminating zero document-frequency value, then absolute
    document ID, encoded frequency/location flag, position bit width, and
    packed position deltas — instead of a full chunk envelope, while still
    supporting exact frequency and phrase positions.
12. A posting block whose frequency/location value is constant (commonly
    `freq=1, has_positions=true`) marks that value in its frequency control
    byte instead of storing a redundant packed frequency column.
13. Block-Max impact bounds use a differentiated eight-bit minimum
    field-norm ID per 1,024-document range plus a five-bit conservative
    maximum-frequency bucket (32 monotonic upper bounds, with an escape
    bucket of `u16::max`), rather than a full eight-bit maximum-frequency ID;
    a term contained in a single posting block uses its payload-local impact
    bound without a range-ID sidecar.
14. Non-inline postings headers omit every derivable field — block count
    (from document frequency), metadata length (from the compact-metadata
    width bytes), and skip length (from the fixed checkpoint stride); a term
    contained in one posting block additionally omits impact count (implicitly
    one) and range-ID length (implicitly zero).

## Open work

- `generated_chunked_full_text` still piggybacks on dense-generator config
  (a `dense_embedding` generator with a chunker that sets `full_text_index`)
  rather than having a first-class chunked full-text generator kind.
- A future Lucene-style stored-field format could compress blocks of docs,
  not individual small docs, so the write path does not spend most of segment
  assembly in per-document compression.
- Expose `max_shingle_size` as a public knob (values `2..4`) instead of the
  fixed default of 3.
