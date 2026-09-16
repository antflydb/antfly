# Full-Text Performance and Benchmarking

## Purpose

This document defines how to make Antfly Zig full-text performance work
measurable, comparable, and implementable. It covers two deliberately separate
benchmark products:

1. an embedded search-kernel comparison against Tantivy, used for engineering
   and regression work; and
2. a database/server comparison, used to evaluate the public Antfly product
   under realistic transport, concurrency, write, durability, and recovery
   conditions.

It also describes the engine work most likely to close the search-kernel gap.
Benchmark credibility comes before performance claims: a timing is not accepted
unless the compared engines demonstrably executed equivalent queries over the
same corpus and produced equivalent results.

This document complements [FULL_TEXT.md](FULL_TEXT.md). `FULL_TEXT.md` remains the
source for visibility, maintenance, field-layout, and product semantics. This
document owns performance methodology and the search execution roadmap.

## Background

Earlier embedded experiments reported Antfly improving from approximately
1,349 us to 674 us median while Tantivy completed the tested operation in about
18--19 us. A later four-way experiment reported approximately:

| Engine and path | Median latency |
| --- | ---: |
| Tantivy embedded | 19 us |
| Bleve embedded | 61 us |
| Antfly HTTP/Bleve | 397 us |
| Antfly Zig embedded | 914 us |

Those measurements were useful for locating architectural costs, but they are
historical evidence rather than a current performance claim. Since then, the
Zig implementation has gained block-max metadata, Block-Max WAND, postings
`advanceTo`, cross-segment global top-k collection, deleted-document filtering,
position-decoding avoidance for ranking-only queries, and an embedded
`search-benchmark-game` adapter. A new verified baseline has since been
established (see [Implementation Progress](#implementation-progress)); it
supersedes the ratio above for any public comparison.

The earlier investigation identified these likely costs:

- Block-Max WAND was unavailable or ineffective across multiple segments.
- Boolean queries scored all matches and combined them through hash maps.
- Search results caused stored-document materialization.
- MVCC visibility and document-identity work remained in the measured path.
- Postings decoding was less optimized.
- Query setup and allocations remained in the hot path.
- Segment merging did not produce a controlled comparison state.

Those findings are now addressed for every query class in the V1 kernel
grammar. The implementation and qualification history below records the format,
execution, LSM, and server work that closed them. Query shapes outside that
explicit grammar remain product features rather than inputs to the kernel
comparison and may retain correctness-first fallback plans.

## Goals

- Produce reproducible and correctness-gated Antfly/Tantivy kernel results.
- Preserve one benchmark path that uses Antfly's real production postings and
  scoring implementation without HTTP, MVCC, projection, or body loading.
- Measure normal Antfly product behavior separately over its public API.
- Report query classes independently instead of hiding them in a blended
  median.
- Make regression results explainable with work counters and phase timings.
- Replace all-hit boolean and phrase execution with iterator-based competitive
  scoring where semantics permit it.
- Measure segment, codec, memory, indexing, and recovery tradeoffs rather than
  optimizing query latency in isolation.

## Non-goals

- The kernel benchmark is not a public product comparison.
- The server benchmark is not a pure postings implementation comparison.
- We will not create a benchmark-only search algorithm that diverges from the
  production search implementation.
- We will not disable correctness, visibility, or durability in the server
  benchmark merely to match an embedded library.
- We will not publish a single "search latency" number that blends terms,
  unions, intersections, phrases, counts, and top-k operations.
- We will not claim parity based only on similar hit counts. Result identity,
  ordering, cutoff ties, and score behavior must also be checked.

## Current Zig Architecture

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
- `bench/full_text/wand_skip_bench.zig` exposes WAND work counters on controlled
  distributions.
- `bench/full_text/search_benchmark_index.zig` and
  `bench/full_text/search_benchmark_query.zig` provide embedded indexing and
  query executables using stdin/stdout.
- `search-benchmark-game/engines/antfly-zig` integrates those executables with
  the external harness.
- `DB.forceCompactTextIndexes()` and scheduled-merge drains provide separate
  maintenance controls.

### Resolved benchmark gaps

- Timed `TOP_N` deliberately returns only an acknowledgement so stdout and JSON
  serialization are outside the timing. Before timing, the runner sends
  `VERIFY_TOP_N_COUNT` to both engines and compares exact counts, stable corpus
  ordinals, cutoff ties, ordering, and scores. A run cannot reach its timing
  phase if this verification fails.
- Exact-count work remains a separate operation. `TOP_N_COUNT` is never labeled
  as plain top-k latency, and the competitive top-k result does not claim an
  exact total.
- The accepted input language is the explicitly versioned V1 query grammar,
  shared by both adapters. Unsupported/skipped counts are recorded and a
  declared V1 query rejected by either adapter fails the run.
- The kernel API returns native ordinal/score pairs without stored-body or
  public-ID projection. Product HTTP results retain normal identity and MVCC
  semantics in the separate server benchmark.
- Index manifests declare production or single-segment mode, enumerate the
  actual layout, and reject unsettled merge debt. Cross-engine preflight
  requires the same declared mode while preserving each engine's documented
  production segment policy.
- Golden analyzer streams, corpus hash/count, BM25 parameters, and the grammar
  version are checked before correctness or timing.
- The runner emits the complete machine-readable bundle described below,
  including raw per-query samples, layout, indexing, memory, resource profiles,
  warmup settings, build identity, and correctness diagnostics. Reused indexes
  retain their original indexing elapsed/CPU/RSS measurements from the archived
  index manifest.

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

## Benchmark A: Embedded Search Kernel

### Contract

The kernel benchmark measures analysis, query construction, postings lookup,
iterator execution, scoring, and top-k collection. It excludes:

- HTTP/gRPC parsing and serialization;
- MVCC constraint derivation and late visibility filtering;
- public result projection;
- stored JSON/body decompression;
- distributed fan-out and merge; and
- background writes or maintenance during the timed query window.

It must use the same inverted sections, postings iterators, scorer
implementations, deletion masks, BM25 implementation, and merge output as
production. The benchmark boundary may be a narrow internal API, but it must
not contain a separate search implementation.

The kernel result is:

```zig
pub const KernelHit = struct {
    corpus_ordinal: u32,
    score: f32,
};

pub const KernelResult = struct {
    hits: []KernelHit,
    total_hits: u32,
    total_hits_relation: enum { exact, gte },
};
```

`corpus_ordinal` is the stable input ordinal shared by all engines. It is not a
stored body and must be obtainable without decompressing stored JSON. If the
production segment format cannot currently expose it cheaply, add a native
ordinal/doc-value mapping and use that mapping in production as well.

### Query grammar

Define and version a deliberately small benchmark grammar instead of claiming
general Lucene compatibility:

```text
TERM <field> <term>
UNION <field> <term>...
INTERSECTION <field> <term>...
PHRASE <field> <term>...
```

The query corpus may have a text serialization for compatibility with the
external harness, but every accepted expression must lower exactly to one of
these typed operations. Unknown operators, unmatched quotes, unexpected field
syntax, and unsupported escaping must produce `UNSUPPORTED`; they must never be
approximated.

Keep these operations separate:

- exact count;
- top-k without exact count;
- top-k plus exact count; and
- correctness inspection.

### Correctness protocol

Add an untimed verification command, for example:

```text
VERIFY_TOP_10\t<query>
```

with a compact response such as:

```json
{"total_hits":1234,"relation":"exact","hits":[{"id":42,"score":7.31}]}
```

Before any timing is accepted, the runner must:

1. assert exact count equality for operations that promise an exact count;
2. assert identical top-k IDs when the cutoff is not tied;
3. compare scores with a documented absolute/relative floating-point tolerance;
4. treat all documents tied at the kth score as one cutoff equivalence set;
5. report both strict overlap and tie-aware overlap;
6. fail closed on unsupported or partially translated queries; and
7. retain a small diagnostic artifact containing mismatched queries and both
   result sets.

The timed protocol may retain a minimal numeric acknowledgement if required by
`search-benchmark-game`. Correctness must be established in a separate preflight
using the same index artifacts and query translator.

### Analyzer and scoring equivalence

The compared configurations must state and test:

- tokenizer and Unicode behavior;
- case normalization;
- maximum-token behavior;
- stop-word behavior;
- stemming or its absence;
- position increments and phrase gaps;
- repeated-term handling;
- BM25 `k1` and `b`;
- document length/norm semantics;
- query boosts; and
- boolean minimum-should-match semantics.

Create a shared analyzer fixture with punctuation, mixed case, non-ASCII text,
emoji boundaries, combining characters, numbers, long tokens, repeated terms,
and empty text. Export the token and position stream from both engines and
compare it before indexing the full corpus.

BM25 parameters must be explicit command-line or manifest values. Defaults may
match today, but benchmark reproducibility must not depend on an implicit
default remaining unchanged.

### Corpus and document identity

- Use the full declared corpus; record its content hash, compressed and
  uncompressed byte counts, and document count.
- Do not use `--max-text-bytes` in the primary comparison.
- Normalize input once into a shared benchmark artifact rather than giving each
  engine a different JSON extraction path.
- Assign a stable `u32` ordinal in input order and reject corpora that exceed
  that identity space.
- Record rejected/empty documents and require the same indexed-document count
  from both engines.

### Segment modes

Every kernel run declares one of two modes:

`single`
: Force-merge both engines to exactly one searchable segment. Antfly should
  invoke explicit force compaction until the invariant is satisfied, then fail
  if the index still contains more than one segment. The benchmark needs a
  read-only segment-layout inspection API rather than inferring success from a
  completed maintenance call.

`production`
: Use a documented ingestion batch size and each engine's documented
  production merge policy. Freeze maintenance before query timing and emit the
  final segment count, per-segment document counts, byte sizes, deletion counts,
  and merge-policy parameters.

Never compare a force-merged Tantivy index with an uncontrolled Antfly segment
state, or vice versa.

### Timing procedure

- Build optimized release binaries once outside measured runs.
- Pin or record CPU model, logical CPU count, OS, compiler, optimization mode,
  filesystem, power mode, and relevant allocator configuration.
- Keep the query process persistent; do not include process startup per query.
- Perform a declared warmup of both query count and minimum wall time.
- Use the same deterministic shuffled query order for both engines.
- Run at least five independent measured repetitions.
- Do not interleave indexing or merge work with the read-only query window.
- Record wall time with sufficient resolution and retain raw samples.
- Report median, p50, p95, p99, minimum, maximum, and sample count per operation
  and query class. Do not publish a blended median as the primary result.
- Run cold/reopen behavior as a separate test. Do not mix it into the warm
  steady-state distribution.

### Kernel metrics

For both engines, record:

- indexing wall time and throughput;
- final index bytes and bytes/document;
- peak RSS during indexing;
- steady and peak RSS during querying;
- reopen time;
- query latency per class and operation; and
- final segment layout.

For Antfly diagnostics, additionally record when available:

- terms and postings iterators opened;
- postings hits decoded;
- bytes/blocks decoded;
- `next` and `advanceTo` calls;
- WAND pivots advanced and scored;
- blocks/chunks skipped;
- position lists decoded;
- candidates admitted and fully scored;
- deleted/non-visible candidates rejected;
- allocations and allocated bytes per query; and
- phase times for parse, analyze, plan, term lookup, execute, result mapping,
  and serialization.

Diagnostic counters are not directly compared as product scores. They explain
why latency changes and guard against optimizations that merely move work.

## Benchmark B: Database and Server

### Contract

The server benchmark measures the products through their normal public
interfaces. Every comparator must run as a persistent server and receive
requests over persistent HTTP or gRPC connections. An embedded library behind
a one-request process wrapper is not a server comparison.

Quickwit is a reasonable Tantivy-derived server comparator. If a custom
Tantivy service is retained, it must be a minimal persistent service with
documented request, result, caching, merge, and durability behavior. Label it
as a custom Tantivy server rather than Tantivy itself.

### Request and result shape

- Request the same logical query and top-k.
- Request only stable document IDs and scores unless a separate stored-source
  workload is under test.
- Disable highlights, explanations, aggregations, and source bodies in the
  baseline.
- Verify server results using the same count and top-k preflight principles as
  the kernel benchmark.
- Keep response encoding comparable and report response bytes.

### Load matrix

Run concurrency sweeps such as `1, 2, 4, 8, 16, 32, 64` with enough duration to
reach steady state. At each point report:

- offered and achieved requests/second;
- p50, p95, p99, and maximum latency;
- error, timeout, and rejection counts;
- server CPU utilization;
- server RSS and peak RSS; and
- client CPU utilization, so client saturation is visible.

Use an open-loop or otherwise coordinated-omission-safe load generator for
tail-latency results. A serial closed-loop client remains useful as a diagnostic
but is not the product throughput benchmark.

### Writes and freshness

Run read-only and mixed workloads separately. Mixed cases should include
declared write rates and batch sizes. Measure searchable freshness by writing a
unique marker term and timing from acknowledged durability boundary to the
first successful query observation.

Report:

- write throughput and acknowledgement latency;
- read throughput and latency during writes;
- p50/p95/p99 searchable freshness;
- merge/compaction debt growth;
- disk amplification; and
- recovery behavior if the process stops during outstanding maintenance.

### Durability and recovery

Define named profiles based on guarantees rather than vendor-specific flags:

- `unsafe-throughput`: data may be lost on process or machine failure;
- `process-durable`: acknowledged data survives process restart; and
- `machine-durable`: acknowledged data survives the declared machine/storage
  failure model.

Map each product's WAL, fsync, commit, replication, refresh, and acknowledgement
settings into those profiles and print the exact configuration with results.
Do not compare differently durable configurations under one label.

For each applicable profile measure:

- initial load/index time;
- disk footprint after maintenance quiescence;
- graceful restart time to readiness;
- crash restart time to readiness;
- time until the expected document count is searchable; and
- query latency immediately after restart and after warmup.

## Engine Optimizations

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

## Status

The eight-milestone implementation plan above is complete: the benchmark
specification (grammar, corpus normalization, score/cutoff-tie rules, segment
modes, manifest schema) is frozen and checked in; the embedded adapter
(`bench/full_text/search_benchmark_*.zig`, `search-benchmark-game/engines/antfly-zig/`)
is correct and correctness-gated; the production-backed kernel API
(`pkg/antfly/src/index.zig`, `pkg/antfly/src/search/search.zig`) returns
native ordinals/scores without MVCC or body-loading overhead; the runner
(`tools/run_search_kernel_benchmark.py`, `tools/run_search_server_benchmark.py`)
is reproducible with an archived baseline; the boolean iterator tree, scored
two-phase phrase executor, and segment/codec/allocation optimizations
described in the Engine Optimization Roadmap above are implemented in
`pkg/antfly/src/search/scorer.zig` and `pkg/antfly/src/section/inverted.zig`;
and the product/server benchmark (persistent-client concurrency sweeps, mixed
read/write, freshness, durability-profile, and restart/recovery measurements)
is implemented and compared against Quickwit. The dated log below records the
ongoing measurement history against that completed harness; it is not a list
of outstanding milestone work.

## Implementation Progress

> **Relocated:** The dated implementation-progress log that previously lived here (2,310 lines, 2026-07-12 through 2026-07-16) is preserved verbatim in [work-log/completed/full-text/implementation-progress-2026-07.md](../work-log/completed/full-text/implementation-progress-2026-07.md). Durable decisions from it are in Engine Optimizations, Decisions, and Risks and Design Constraints in this document.

## Result Artifact

Each run should produce one directory containing at least:

```text
manifest.json
correctness.json
indexing.json
segments.json
memory.json
resources.json
queries-term.jsonl
queries-union.jsonl
queries-intersection.jsonl
queries-phrase.jsonl
summary.json
```

`manifest.json` should include:

- benchmark schema and query-grammar versions;
- engine name and commit;
- dirty-worktree state;
- compiler/build settings;
- corpus hash and document count;
- analyzer and BM25 configuration;
- segment mode and merge policy;
- durability profile for server runs;
- hardware/OS/filesystem metadata;
- warmup and measurement configuration; and
- all unsupported/skipped query counts.

Raw samples are the source of truth. Summaries must be reproducible from the
checked-in or archived result bundle.

## Regression Policy

- Correctness regressions always fail, regardless of performance improvement.
- Query classes have separate performance thresholds.
- Indexing, index size, and peak memory have independent guardrails.
- A lower median does not excuse a material p99 regression without an explicit
  decision.
- Microbench improvements require confirmation in a representative full-corpus
  query class.
- Benchmark format or methodology changes start a new baseline series; do not
  splice incompatible samples into an old graph.
- Public claims must link to the exact manifest and raw result artifact.

Initial thresholds are chosen only after a stable-variance baseline is
established on the target machines.

## Risks and Design Constraints

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
   [Correctness protocol](#correctness-protocol).
2. Stable corpus ordinals use a dedicated benchmark-visible native
   ordinal/doc-value mapping (`corpus_ordinal`), also used in production; see
   [Contract](#contract).
3. The analyzer configuration is explicit and declared in the manifest (for
   example `ascii_lowercase`), and the `ANALYZE` protocol compares exact
   token, position, and byte-offset output between engines; see [Analyzer and
   scoring equivalence](#analyzer-and-scoring-equivalence).
4. Phrase frequency contributes to BM25: phrase scoring matches Tantivy's
   semantics (sum constituent-term IDFs including repeated terms, use exact
   phrase occurrence count as BM25 frequency, apply the field norm once).
5. The primary top-k benchmark permits `total_hits_relation = gte`; exact
   totals are a separate operation and are never inferred from competitively
   pruned execution.
6. Both `single` and `production` segment modes are used for cross-engine
   comparison, each explicitly declared and never mixed; see [Segment
   modes](#segment-modes).
7. Hardware class and noise controls are standardized by the [Timing
   procedure](#timing-procedure) (pinned/recorded hardware and build
   settings, declared warmup, at least five repetitions).
8. Server comparators are mapped to the three named durability profiles
   (`unsafe-throughput`, `process-durable`, `machine-durable`); see
   [Durability and recovery](#durability-and-recovery).
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

## Completion Criteria

The benchmark suite satisfies these criteria (see [Status](#status)):

- kernel and server benchmarks are separate binaries/workflows and reports;
- the kernel comparison verifies analyzer output, counts, IDs, cutoff ties, and
  scores before timing;
- both engines use the full corpus and equivalent declared segment states;
- results include indexing, size, memory, reopen/recovery, and per-query-class
  latency rather than one blended number;
- the benchmark grammar's union, intersection, and phrase paths avoid Antfly's
  all-hit/hash-map execution;
- top-k operations have bounded memory relative to `k` and query state;
- exact totals are never inferred from competitively pruned execution;
- server results cover concurrency, tail latency, writes, freshness,
  durability, and recovery; and
- any claim that Antfly has closed or won a gap is backed by archived raw
  samples and a reproducible manifest.
