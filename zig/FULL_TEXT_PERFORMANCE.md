# Full-Text Performance and Benchmark Plan

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

This plan complements [FULL_TEXT.md](FULL_TEXT.md). `FULL_TEXT.md` remains the
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
`search-benchmark-game` adapter. We must establish a new verified baseline
before repeating the old ratio or setting a public target.

The earlier investigation identified these likely costs:

- Block-Max WAND was unavailable or ineffective across multiple segments.
- Boolean queries scored all matches and combined them through hash maps.
- Search results caused stored-document materialization.
- MVCC visibility and document-identity work remained in the measured path.
- Postings decoding was less optimized.
- Query setup and allocations remained in the hot path.
- Segment merging did not produce a controlled comparison state.

Some of those findings have been addressed for simple ranked term queries.
Others remain, particularly compound boolean and phrase execution, benchmark
result validation, native result identity, and explicit segment-state control.

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

### Known benchmark gaps

- The current `TOP_N` command executes a query, discards its hits and scores,
  and emits `1`. It cannot prove top-k equivalence.
- `TOP_N_COUNT` performs top-k search and exact count as two separate queries.
  Its latency must not be presented as plain top-k latency.
- The current Lucene-like parser recognizes a small whitespace/`+`/`-`/quote
  subset. It is not a complete or explicitly versioned query grammar.
- `include_stored = false` avoids stored bodies, but the DB path still resolves
  result IDs and document ordinals after search.
- The indexer drains scheduled merges but does not select or assert an exact
  segment layout.
- Analyzer and BM25 compatibility are described in adapter metadata but are not
  enforced by cross-engine golden tests.
- Index size, segment layout, peak memory, warmup behavior, and run metadata are
  not emitted as one machine-readable result bundle.

### Known execution gaps

- `executeBool` has a simple-text fast path, but unsupported shapes fall back to
  `executeQueryAllScored`, set `k` to the global document count, combine results
  in hash maps, and sort the full match set.
- The simple boolean fast path merges postings directly, but its local
  `FastTermState.advanceTo` currently loops over `next()` rather than using the
  postings iterator's seek implementation.
- Phrase search is a filter-first path: it materializes all matching document
  IDs, creates a scored-hit array, and uses a constant boost as the score. It is
  not a competitive BM25 phrase scorer.
- Each segment benefits from the shared collector threshold, but there is no
  query-specific segment ordering or segment-level maximum-score rejection.
- Exact counts and top-k collection are not always planned as independent
  operations, so callers can accidentally pay for more work than their result
  contract requires.

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

## Engine Optimization Roadmap

Correctness and benchmark changes should land before interpreting optimization
results. Engine changes then proceed in the following order.

### 1. Establish profiles and regression gates

Add query-class-specific baselines and phase counters before changing executor
architecture. Capture CPU profiles and allocation profiles for representative
term, union, intersection, phrase, and mixed boolean queries in both segment
modes.

Acceptance:

- full-corpus correctness preflight passes;
- raw samples and environment metadata are retained;
- each target query has a dominant-cost explanation; and
- a regression threshold can be evaluated independently per query class.

### 2. Remove native ID and projection work from the kernel boundary

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

### 3. Replace boolean all-hit/hash-map execution

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

### 4. Add a two-phase scored phrase executor

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

### 5. Add segment-level competitive pruning

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

### 6. Tune postings and block-max layout from evidence

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

### 7. Reduce query setup and allocation cost

After iterator architecture is stable:

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

### 8. Improve merge policy and observability

- Expose per-index segment count and per-segment sizes through a read-only
  internal status surface.
- Make force-compaction completion and its resulting invariant observable.
- Report merge bytes read/written, elapsed time, peak memory, fan-in, and debt.
- Tune production tiering using both write amplification and multi-segment
  search cost.
- Keep scheduled maintenance distinct from explicit force compaction, as
  defined in `FULL_TEXT.md`.

## Implementation Milestones

### Milestone 0: Freeze the benchmark specification

- [x] Define the typed query grammar and version.
- [x] Define corpus normalization and stable ordinal assignment.
- [x] Define score tolerance and cutoff-tie rules.
- [x] Define single and production segment modes.
- [x] Define the machine-readable run manifest and result schema.
- [x] Check in a small deterministic correctness corpus and query suite.

Exit gate: two independent engine adapters can consume the same artifacts and
the verifier can deliberately detect injected count, ID, and score mismatches.

### Milestone 1: Correct embedded adapter

Likely files:

- `bench/full_text/search_benchmark_common.zig`
- `bench/full_text/search_benchmark_index.zig`
- `bench/full_text/search_benchmark_query.zig`
- `search-benchmark-game/engines/antfly-zig/Makefile`
- `search-benchmark-game/engines/antfly-zig/details.json`
- `build.zig`

Tasks:

- [x] Add verification output containing IDs, scores, count, and relation.
- [x] Replace ambiguous query parsing with the benchmark grammar.
- [x] Make analyzer and BM25 configuration explicit.
- [x] Add stable corpus ordinals.
- [x] Add segment-mode selection and post-index assertions.
- [x] Emit index, segment, corpus, build, and environment manifests.
- [x] Add adapter protocol and golden tests.

Exit gate: no timed sample is recorded when correctness preflight fails.

### Milestone 2: Add the production-backed kernel API

Likely files:

- `pkg/antfly/src/index.zig`
- `pkg/antfly/src/search/search.zig`
- `pkg/antfly/src/embedded/db.zig`, only for snapshot/access plumbing
- segment ordinal/doc-value sections if native identity needs format work

Tasks:

- [x] Return corpus ordinals and scores without body loading.
- [x] Bypass MVCC/public projection in kernel mode.
- [x] Prove equivalence with a static fully visible DB search.
- [x] Add phase and work counters without affecting default production cost.

Exit gate: profiles show the timed kernel path is limited to declared kernel
work, and the server path remains unchanged.

### Milestone 3: Reproducible runner and baseline

- [x] Integrate Tantivy correctness output.
- [x] Add analyzer-token-stream comparison.
- [x] Run the full corpus in both segment modes.
- [x] Run at least five warm repetitions per class.
- [x] Record index time/size, RSS, reopen time, latency, and raw samples.
- [x] Publish an internal baseline report with commit hashes and manifests.

Exit gate: replace the historical latency table with a reproducible current
baseline, including failed/unsupported query counts.

### Milestone 4: Boolean iterator tree

- [x] Route simple fast-path seeks through postings `advanceTo`.
- [x] Implement conjunction, disjunction, required/optional, exclusion, and
      minimum-should-match scorers.
- [x] Separate exact-count and competitive top-k plans.
- [x] Remove benchmark query shapes from the all-hit/hash-map fallback.
- [x] Add randomized differential tests against the old executor.

Exit gate: correctness is unchanged, top-k memory does not scale with match
count, and union/intersection latency is reported independently.

### Milestone 5: Scored two-phase phrases

- [x] Add approximation and positional verification interfaces.
- [x] Define and test phrase scoring semantics.
- [x] Add exact count without scored-hit materialization.
- [x] Feed phrase top-k into the global competitive collector.
- [x] Add phrase work counters and randomized differential tests.

Exit gate: phrase correctness passes against the comparator and top-k no longer
allocates one hit per phrase match.

### Milestone 6: Segment, codec, and allocation optimization

- [x] Add conservative segment upper bounds and search ordering.
- [x] Evaluate persisted postings/block-max layouts.
- [x] Reduce query-local allocations and repeated sorting.
- [x] Tune production merge policy with read/write tradeoff measurements.

Exit gate: accepted improvements pass format, reopen, corruption, merge,
correctness, index-size, RSS, and query-class regression gates.

### Milestone 7: Product/server benchmark

- [x] Add persistent-client concurrency sweeps.
- [x] Add read-only and mixed read/write workloads.
- [x] Add searchable-freshness markers.
- [x] Define and implement durability-profile manifests.
- [x] Add graceful/crash restart and recovery measurements.
- [ ] Compare Antfly with a normal server comparator such as Quickwit.

Exit gate: the public report clearly separates kernel results from product
results and includes throughput, tail latency, freshness, durability, memory,
disk, indexing, and recovery.

## Implementation Progress

### 2026-07-12

Implemented the first correctness and kernel-performance slice:

- added the strict `V1` `TERM`/`UNION`/`INTERSECTION`/`PHRASE` grammar;
- retained legacy external-harness syntax only behind the explicit
  `--allow-legacy-query-syntax` flag;
- added `VERIFY_TOP_N` and `VERIFY_TOP_N_COUNT` JSON responses with stable
  zero-based corpus ordinals, scores, totals, and honest total-hit relations;
- made benchmark document keys deterministic from input order;
- added full-corpus SHA-256, elapsed indexing time, and physical segment-layout
  manifests;
- added `single` and `production` segment modes, with `single` force-compacting
  and failing unless exactly one segment remains;
- added a read-only text-index layout API with per-segment document, deletion,
  byte-size, and file-backing data;
- added a production-backed text-kernel API that uses normal query lowering,
  snapshots, postings, deletion masks, scorers, and collectors while bypassing
  MVCC derivation, public projection, and stored bodies;
- added a native exact-count path that does not allocate public hits;
- changed the simple boolean scorer to use postings `advanceTo` and skip
  position decoding;
- routed compatible pure should/union top-k queries through Block-Max WAND; and
- added `search-bench-test` and `search-performance-test` focused build steps,
  including kernel/projected equivalence and WAND-routing coverage.
- added `tools/verify_search_benchmark.py`, which fails closed on incompatible
  schemas/grammars, inexact totals, non-finite or misordered scores, duplicate
  IDs, score-tolerance violations, and non-cutoff identity mismatches while
  permitting documented cutoff-tie substitutions; and
- added focused verifier tests covering tolerant strict matches, cutoff ties,
  identity mismatches, and total-count mismatches.

The analyzer preflight also exposed that the current production `lowercase`
filter lowercases ASCII only (`CAFÉ` becomes `cafÉ`). The manifest now states
`ascii_lowercase`, and the `ANALYZE` protocol returns exact term, position, and
byte-offset data so a comparator cannot accidentally run Unicode lowercasing
under the same label.

The next slice completed the benchmark query paths and both runners:

- propagated explicit BM25 `k1` and `b` through the adapter, kernel API,
  boolean scorer, Block-Max WAND, phrase scorer, Makefile, and manifests;
- replaced global exact-count hit arrays with per-segment live bitmap
  cardinality sums;
- implemented streaming conjunction, disjunction, required/optional,
  exclusion, and minimum-should-match execution with bounded top-k state;
- retained the former all-hit/hash-map boolean implementation as a differential
  oracle and added a seeded randomized equivalence test;
- changed phrase verification to seek candidate documents and skip packed
  position records without unpacking non-candidate deltas;
- matched Tantivy's exact phrase score semantics: sum constituent-term IDFs
  (including repeated terms), use exact phrase occurrence count as BM25
  frequency, and apply the field norm once, as implemented by Tantivy's
  [`PhraseWeight`](https://github.com/quickwit-oss/tantivy/blob/main/src/query/phrase_query/phrase_weight.rs),
  [`PhraseScorer`](https://github.com/quickwit-oss/tantivy/blob/main/src/query/phrase_query/phrase_scorer.rs),
  and [`Bm25Weight`](https://github.com/quickwit-oss/tantivy/blob/main/src/query/bm25.rs);
- added conservative whole-segment score upper bounds, descending competitive
  segment order, and strict-below-threshold pruning so cutoff ties remain safe;
- corrected WAND pivot bounds to use each term's global theoretical maximum
  rather than the current block maximum (a later, higher-impact block must not
  be pruned), with a focused late-high-impact-block regression;
- exposed merge policy and cumulative merge work beside physical layout,
  including input/output bytes and segments, elapsed nanoseconds, and peak
  task-allocator bytes for in-memory merge builders;
- added opt-in `PROFILE_TOP_N` phase timing and per-query segment, postings,
  WAND, boolean-candidate, and phrase-verification counters without global
  atomics on normal requests;
- added `tools/run_search_kernel_benchmark.py`, which performs analyzer and
  correctness preflight, compares fresh or archived engine manifests before
  starting adapters, enforces at least five repetitions, retains shuffled raw
  samples per query class, and emits the complete result bundle;
- added a pinned Tantivy 0.25 comparator with the same V1 protocol, an exact
  Antfly-compatible tokenizer, native fast-field ordinals, explicit segment
  modes, and fixed-default BM25 validation; the runner can build both indexes,
  rejects manifest differences, and correctness-gates before timing;
- added `tools/run_search_server_benchmark.py`, a separate persistent-HTTP,
  open-loop product runner with concurrency sweeps, mixed writes, freshness
  markers, durability manifests, timed corpus loading, server RSS/CPU and disk
  sampling, raw NDJSON support, and graceful/crash restart measurements that
  include readiness plus the first successful post-restart search;
  and
- passed a complete single-segment smoke bundle on the checked-in corpus with
  all query classes, analyzer edge cases, and exact verification output.

After the WAND bound correction, the focused suite passes 22/22 tests and the
late-high-impact regression. Persisted-layout evaluation then found that WAND
was repeatedly random-accessing delta-coded sparse chunk metadata through an
O(chunk ordinal) decoder. The first fix reused the postings iterator's decoded
chunk table and reduced the skewed-norm two-term 1M/k=10 diagnostic from
approximately 604 ms to 4.81 ms (about 125x) on the development machine. The
balanced two-term 100K/k=10 case remains approximately 1.04 ms. The
StreamVByte microbench reports
approximately 0.90 ns/value encoding and 0.38 ns/value decoding at 1.686x
compression. These are diagnostic measurements, not release baselines. Fresh
single- and production-mode Antfly/Tantivy smoke bundles both pass analyzer and
five-query correctness preflight before five shuffled repetitions per query
class. The Python runner/verifier suite passes 9/9 focused tests.

With on-disk evolution explicitly allowed, the branch-local v24 experiment adds an
explicit postings-payload length and 16-byte absolute checkpoints every 16
stored chunks. This removes O(chunks) payload-length reconstruction during term
lookup and lets normal next/advance execution avoid eagerly decoding four
metadata columns. WAND addresses block-max records directly by the iterator's
current stored-chunk ordinal. On the skewed-norm two-term 1M/k=10 diagnostic,
the first v24 implementation completed in approximately 3.97 ms, retained zero
decoded chunk-metadata heap instead of v23's minimum approximately 117 KB, and
used 1,952 checkpoint bytes in a 1.73 MiB section. The reader retains v23 reopen
support. A first bounded-checkpoint prototype was rejected because repeated
delta decoding regressed the case to approximately 11.8 ms.

The accepted WAND follow-up now scans those compact metadata records with a
per-term cursor and loads postings only at the first competitive logical block.
It also applies an ordered cutoff-tie check, so equal-score blocks are skipped
only when they cannot replace the earliest retained document. On the same
development machine, the focused ReleaseFast diagnostic now reports:

- balanced two-term 100K/k=10: approximately 0.093 ms, down from 0.326 ms;
- skewed-norm two-term 1M/k=10: approximately 0.099 ms, down from 1.62 ms;
- skewed-norm single-term 1M/k=10: approximately 0.006 ms; and
- zero decoded chunk-metadata heap in every reported case.

The focused search suite passes 30/30 tests, including v23 reopen, sparse
block-max, seek, late-high-impact, cutoff-tie, randomized boolean/phrase, and
production-kernel equivalence regressions. A dedicated 17-segment regression
also proves fragmented snapshots still activate query-specific segment-bound
ordering and prune lower-impact segments.

`tools/generate_search_benchmark_corpus.py` provides a deterministic synthetic
corpus for repeatable local tuning. A settled 60K-document production-segment
Antfly/Tantivy comparison passed analyzer and five-query correctness preflight
(all differences were documented cutoff-tie substitutions). With seven warm
repetitions, Antfly median latency was approximately 36.7 microseconds for
terms and 90.7 microseconds for union, versus Tantivy's 78.8 and 34.7
microseconds. A subsequent pure-conjunction block-max pass first reduced
Antfly intersection from approximately 858 to 554 microseconds and the number
of scored candidates from 28,000 to 477. Replacing its one-block payload loads
with a compact metadata-cursor merge then reduced intersection to approximately
17.3 microseconds in single mode and 66.3 microseconds in the 10-segment
production layout, versus Tantivy's approximately 277 and 286 microseconds.
The scanner preserves later competitive sparse blocks and ordered cutoff ties,
and exact verification still uses the separate exact-count path. Phrase
remained approximately 999 microseconds versus Tantivy's approximately 890
microseconds. These synthetic
measurements guide engineering work; they are not a replacement for the
required archived full-corpus baseline.

The same single/production split showed that healthy production segmentation
added approximately 50 microseconds to union primarily through a duplicate
query-specific segment-bound prepass, not through additional postings work.
Bound ordering is now reserved for snapshots with more than 16 segments;
healthy tiered layouts rely on in-segment WAND and avoid opening every term
dictionary twice. Together with stack-resident WAND ordering for the common
small-term case, the final 11-repetition 10-segment run measured approximately
33.3 microseconds for term and 88.5 microseconds for union, versus Tantivy's
75.8 and 35.3 microseconds, without changing scorer work or correctness.
Fragmented snapshots retain segment ordering and competitive pruning.

Production merge-policy sweeps used deterministic bulk ingestion followed by
a synchronous maintenance drain, and now record merge debt, elapsed time, and
bytes. On 60K documents, tier limits of 10, 16, and 24 produced respectively
10, 16, and 24 settled segments; the wider limits reduced indexing/merge work
but changed query cost by less than about five percent. On 120K documents, the
same limits produced 10, 16, and 24 segments, indexing in approximately 18.45,
18.76, and 17.96 seconds while reading approximately 48.8, 54.2, and 43.7 MB of
merge input. Query results varied by single-digit percentages without a
consistent wider-tier win. The production default therefore remains 10: the
available evidence does not justify trading its tighter search bound for the
small synthetic indexing gain. The override remains available for future
full-corpus sweeps. The runner now refuses timing if in-flight, pending,
quarantined, or failed merge work remains.

Both production-state and single-segment full-corpus kernel baselines are
complete below. A Quickwit 0.8.2 server run is also archived separately, but it
must not be substituted for the persisted search-index comparison: Quickwit's
data directory includes server state and Antfly's public product directory
includes its primary LSM, WAL, stored documents, and transient maintenance
files. Two attempted full Antfly product loads were stopped after exposing
unbounded primary-generation retention; the later run reached 26 GiB at
3,605,808 of 5,032,105 documents, with 760 physical primary run files and 46
WAL files after interruption. They are invalid evidence for codec density or
embedded query memory, but valid defect evidence for the online ingestion path.
The size comparison below therefore uses only settled search-index artifacts
from the kernel adapters, while the product defect and its separate acceptance
gate are documented below.

The first full-corpus attempt exposed a benchmark-boundary error before it
could produce a valid baseline. The Antfly kernel indexer was routing every
article through the primary database write/LSM pipeline and retaining the full
source a second time, while the Tantivy comparator built only its search index.
After 2 hours 35 minutes it was still ingesting (not settling merges), had
created 1,926 fresh 3--12 MB text segments, occupied roughly 23 GB across the
primary and text stores, and had not emitted a manifest. That run was stopped
and is invalid for comparison.

The kernel indexer now uses an explicit internal index-only ingestion boundary.
It still uses the production analyzer, text projection, segment sink, persisted
segment format, native ordinal sidecar, atomic publisher, reopen path, merge
planner, and query implementation, but stores only `{}` as the minimal valid
stored source and never retains the corpus body. Long loads settle the normal
merge policy every 100,000 documents so a deterministic synchronous adapter
cannot manufacture an unbounded end-of-load backlog. The runner relays indexer
stderr live, and the manifest names the indexing scope, stored-source contract,
batch size, and settlement interval.

On the existing deterministic 120K tuning corpus, this correction reduced the
settled production build from approximately 18.45 seconds to 1.28 seconds,
with 10 segments, zero merge debt, and a 14 MB Antfly index. A complete runner
pass reopened both indexes, passed all five Antfly/Tantivy correctness queries
(one strict and four cutoff-tie-aware matches), and retained the same query
protocol. This is diagnostic evidence for the boundary fix, not a public corpus
performance claim.

The first valid 5.03M-document production comparison then exposed a real BM25
compatibility gap: Antfly stored exact `u32` field lengths while Tantivy 0.25
stores one quantized field-norm byte per document. Counts matched, but long
documents therefore received different scores. The v25 inverted-section format
now writes Tantivy-compatible norm IDs, decodes them through the same 256-value
small-float mapping. During the branch-local format sequence its reader retained
v23/v24 packed-norm compatibility; the final production compatibility boundary
is stated under Format evolution below. The
benchmark protocol also has an untimed `EXPLAIN_<ordinal>` diagnostic that
reports the exact document count, total field length, average length, document
frequency, decoded norm, term frequency, and score without loading stored
source. On a 5K Wikipedia differential, both engines reported 830,913 tokens
and an average length of 166.1826; the problematic 390-token document decoded
to 376 in both engines and all five queries became strict matches. The focused
search suite passes 36/36 tests after the subsequent v26/v27 compatibility
coverage described below.

The corrected full production run used the canonical 5,032,105-document,
8,473,678,432-byte corpus with SHA-256
`9078d9ea4783ab764fe246046efdc8062692f07afdd7c8749a74ebfe05c51ff4`.
Antfly indexed it in 611.9 seconds into 10 file-backed production segments
(5.951 GB), with 267 completed merges and zero pending, in-flight, failed, or
quarantined work. The preserved Tantivy 0.25 production comparator had 20
segments, indexed in 555.9 seconds, and occupied 3.251 GB. Analyzer preflight
and all five full-corpus correctness queries were strict matches before timing.
With at least one second and 1,569 paired warmup queries followed by five
shuffled repetitions per query, median adapter-observed top-k latency was:

| Query class | Antfly production | Tantivy production |
| --- | ---: | ---: |
| term | 90.4 us | 50.0 us |
| union | 285.8 us | 268.3 us |
| intersection | 313.5 us | 287.5 us |
| phrase | 1,244.0 us | 490.8 us |

These are engineering kernel measurements from a dirty development worktree,
not a public product claim. They establish that the former approximately 35x
gap is no longer present for this verified production-state workload: term is
about 1.8x slower, union and intersection are about 1.1x slower, and phrase is
about 2.5x slower. The raw result bundle is the source of truth.

The full-corpus single-segment run force-merged the preserved Antfly production
index after fixing the force-compaction boundary that had incorrectly treated a
settled ten-segment production tier as satisfying an explicit single-segment
request. Antfly's combined ingestion and force-merge time was 754.1 seconds;
the resulting segment occupied 5.857 GB. Tantivy indexed and force-merged in
804.3 seconds and occupied 3.171 GB. Analyzer preflight and all five queries
were strict matches. After at least one second and 1,686 paired warmup queries,
followed by five shuffled repetitions per query, median latency was:

| Query class | Antfly single segment | Tantivy single segment |
| --- | ---: | ---: |
| term | 89.0 us | 15.6 us |
| union | 294.3 us | 196.5 us |
| intersection | 310.5 us | 245.7 us |
| phrase | 1,291.6 us | 472.5 us |

The single-segment size ratio is 1.85x, so segment fragmentation is not the
principal disk explanation: force-merging Antfly's production layout removed
only 94.5 MB, about 1.6 percent. Exact Antfly v25 attribution was 207.3 MB of
stored fields, 5.032 MB of norms, 41.5 MB of term dictionary, 8.4 MB of bloom,
20.1 MB of ordinals, and 5.574 GB of postings. The postings portion comprised
773.5 MB of block-max records, 437.5 MB of chunk metadata, 1.309 GB of posting
payload, 2.906 GB of positions, and 111.6 MB of checkpoints. Tantivy's largest
files were 1.933 GB of positions and 1.131 GB of postings/index data, plus
72.4 MB of term data, 20.2 MB of stored data, 8.4 MB of fast fields, and 5.0 MB
of field norms. This comparison is byte attribution, not a claim that similarly
named components use identical boundaries.

Two versioned format changes address the clearest redundant bytes while
retaining v23-v26 readers. v26 replaces each six-byte block-max tuple
`(max_freq, min_norm, max_norm)` with the three bytes actually consumed by the
scorer: a `u16` maximum frequency and Tantivy-compatible `u8` minimum norm ID.
The removed maximum norm was unused. On the 5K Wikipedia differential this cut
the index from 4,917,632 to 4,616,785 bytes (6.1 percent), halved block-max bytes
exactly, and retained five strict matches. Applied to the full single segment,
the direct block-max saving is approximately 386.7 MB.

v27 frames positions once per stored postings chunk and removes the redundant
position count from every document record; term frequency already supplies the
count. The chunk length also lets phrase seeking jump over a skipped positions
chunk instead of walking every document record. On the deterministic 120K
production-layout corpus, v27 reduced the index from v26's 13,363,420 bytes to
12,723,381 bytes (another 4.8 percent); positions alone fell from 2,948,862 to
2,306,452 bytes. Against the pre-v26 14,457,514-byte layout the combined saving
was 12.0 percent. Twenty-one-repetition medians were 35.7 us term, 87.8 us
union, 67.6 us intersection, and 1,805.8 us phrase, with no broad latency
regression versus v26.

The full-corpus v27 production rebuild confirmed the format changes at scale.
It indexed all 5,032,105 documents in 615.2 seconds into 10 settled segments,
with 267 completed merges and no pending, in-flight, failed, or quarantined
maintenance. The index occupied 5,065,897,930 bytes, 885,145,421 bytes (14.9
percent) below the equivalent v25 production run. Relative to Tantivy's
3,251,494,143-byte production index, the ratio fell from 1.83x to 1.56x. Exact
v25-to-v27 attribution shows block-max bytes falling from 773,750,310 to
386,818,149 and positions from 2,906,254,809 to 2,414,333,782. Merge input and
output bytes fell from 31.85/30.76 GB to 28.17/27.09 GB. Total indexing time was
effectively flat (611.9 versus 615.2 seconds), so the smaller format reduced I/O
without hiding a material ingestion regression.

All five full-corpus differential queries remained strict matches. Median v27
latency was 87.3 us term, 266.8 us union, 270.2 us intersection, and 1,249.2 us
phrase. Relative to v25, term, union, and intersection improved by approximately
3, 7, and 14 percent respectively; phrase was within one percent. These results
accept v26/v27 as clear disk and I/O wins with no demonstrated query-latency,
memory, or indexing regression. The remaining approximately 1.81 GB absolute
gap to Tantivy still warrants chunk-metadata, posting-payload, checkpoint, and
minimal stored-record investigation rather than being dismissed as segment
layout noise.

A controlled postings-chunk sweep rejected increasing the production chunk
size as a disk solution. On the same deterministic 120K production-layout
corpus, moving from 1,024 to 2,048 documents per chunk reduced the v27 index
from 12,723,381 to 11,247,242 bytes, but term, union, and intersection medians
regressed from 35.7/87.8/67.6 us to 61.1/169.3/123.7 us. Sizes at 4,096 and
8,192 were 10,526,591 and 10,145,646 bytes, while non-phrase latency continued
to degrade sharply. The 1,024-document default therefore remains the measured
CPU/I/O tradeoff; larger chunks merely buy space by making selective queries
decode much more irrelevant data.

A second experiment encoded positions as a continuous sequence of 128-value
blocks. It reduced the same 120K index by 5.3 percent and its position stream
by 29 percent, but made the same-reader phrase median 11 percent slower. That
format is rejected and was removed. Its independent packed-integer decoder
improvement was retained: a `u64` streaming bit reservoir replaces the former
byte-at-a-time inner loop without changing v27 bytes. Reusing the exact
5,065,897,930-byte full-corpus index, all five differential queries remained
strict matches and median latency became 77.9 us term, 219.4 us union, 256.0 us
intersection, and 1,121.6 us phrase. Versus the original v27 run, those are
approximately 11, 18, 5, and 10 percent reductions respectively. The focused
performance and compatibility suite passes 36/36 tests after the rollback.

### Persisted-size and resource audit

The exact v27 production search-index boundary is 5,065,897,930 bytes for
Antfly versus 3,251,494,143 bytes for Tantivy 0.25: a 1,814,403,787-byte gap,
or 1.558x. This remains a substantial codec-density defect even after the 885
MB v26/v27 saving. The component boundaries are not semantically identical--in
particular Tantivy's `.idx` combines postings and index structures--but the
physical file/section accounting reconciles the entire gap within 6,448 bytes:

| Boundary-aligned component | Antfly v27 | Tantivy 0.25 | Antfly delta | Share of gap |
| --- | ---: | ---: | ---: | ---: |
| postings/index structures excluding positions | 2,312.1 MB | 1,139.4 MB | +1,172.6 MB | 64.6% |
| positions | 2,414.3 MB | 1,930.2 MB | +484.1 MB | 26.7% |
| stored records | 207.3 MB | 20.3 MB | +187.1 MB | 10.3% |
| term dictionary plus bloom / `.term` | 107.0 MB | 150.5 MB | -43.5 MB | -2.4% |
| norms | 5.032 MB | 5.034 MB | -0.002 MB | approximately 0% |
| ordinal / fast field | 20.1 MB | 6.0 MB | +14.1 MB | 0.8% |

This changes the codec priority. Dictionary work is explicitly not first: its
section is already smaller than Tantivy's term data, and norms are at parity.
The first target is the 1.173 GB postings/index-structure delta (block-max,
chunk index, frequency payload, checkpoints, and framing), followed by the 484
MB positions delta and the 187 MB minimal stored-record delta. A candidate
format change must report persisted bytes, bytes decoded for each query class,
CPU/query, query RSS, indexing peak RSS, and merge I/O; it is rejected if it
merely trades the disk gap for selective-query CPU or memory amplification.

The v27 reader's detailed physical scan found 12,123,285 postings terms and
128,939,383 stored document-range chunks, an average of 10.64 chunks per term.
Postings headers, three-byte block-max records, chunk metadata, and sparse
checkpoints consume 1,002,769,503 bytes--82.7 bytes per postings term--before
the 1.309 GB doc/frequency payload. This makes chunk cardinality, rather than
the already-compact dictionary, the leading structural hypothesis. The next
format experiment should compare posting-count blocks or a sparse-term inline
representation with the current global 1,024-document ranges. In particular,
measure location-aware singleton terms before implementing them: the legacy
single-hit optimization cannot activate for indexed text because it requires
no positions. Any replacement must retain phrase positions and bounded seek
without reintroducing the rejected large-chunk selective-query regression.
The scan found 7,509,083 location-bearing single-document lists, 61.9 percent
of terms but only 5.8 percent of chunks. Inline singletons are therefore a
validated secondary saving; reducing repeated chunks for non-singleton terms
is still required to address most metadata bytes.

Summed document frequency is 622,897,189 postings. Partitioning each term by
posting count would require 21,072,316 blocks at 64 postings, 16,482,197 at
128, or 14,225,415 at 256, versus 128,939,383 current document-range chunks.
The 128-posting case reduces block cardinality by 87.2 percent (7.82x). Keeping
the existing three-byte block-max record would cut that component alone from
386,818,149 to approximately 49,446,591 bytes, a 337 MB saving, before chunk
metadata and checkpoint reductions. This is a projection, not an accepted
format result: posting-count blocks span wider document-ID ranges and must
prove that `advanceTo`, WAND pruning, phrase candidate seeking, CPU/query, and
RSS do not regress. Evaluate 128 first; the earlier global-range chunk sweep
already showed that reducing block count by making ranges larger is the wrong
tradeoff.

The first v28 implementation now provides tuning-corpus evidence for that
hypothesis. On the deterministic 120K production-segment corpus, 128-posting
blocks reduced Antfly's settled index from 12,723,381 to 9,773,605 bytes, a
2,949,776-byte or 23.18 percent saving. Correctness passed all five comparator
queries. Tantivy remained much smaller at 3,124,851 bytes: v28 was still 3.13x
its size, so this result validates the structural direction but does not close
the density gap.

The initial v28 query pass also caught a real rejection condition rather than
being accepted on size alone. Because posting-count blocks are not aligned to
shared document-ID ranges, disabling the aligned front-block WAND optimization
made union score 56,001 pivots, skip no blocks, and regress from roughly 88 us
to 1.847 ms. A conservative interval-sweep implementation now sums every term
block that could overlap each candidate document interval. It restored 1,136
block skips and reduced union median to 37.6 us while retaining randomized
all-hit equivalence and five-query comparator correctness. The same run measured
22.96 us term, 38.17 us intersection, and 1.666 ms phrase medians. These are
useful acceptance checks on the small deterministic corpus, not substitutes for
the full-corpus CPU/RSS qualification. A separate three-second-per-class process
profile measured v28 CPU/query at 16.0 us term, 33.3 us union, 34.3 us
intersection, and 1.630 ms phrase, versus Tantivy's 127.3 us, 39.4 us, 529.3 us,
and 1.785 ms. Query peak RSS was 10.9 MB versus 6.6 MB (1.65x, but only a 4.3 MB
absolute delta). A matched fresh indexing sample measured Antfly at 1.14 CPU
seconds and 53.8 MB peak RSS versus Tantivy at 1.86 CPU seconds and 297.0 MB.
Those small-corpus builder numbers must not be extrapolated over the known
full-corpus merge working set: v28 remains a candidate until the persistent
full-corpus resource harness measures indexing CPU, peak RSS, query CPU, and
query RSS. The exact tuning evidence is checked in as
`bench/full_text/results/synthetic-120k-v28-posting-blocks.json`.

The full-corpus gate subsequently rejected v28/128 as the production default.
It retained strict 5/5 correctness, 10 settled segments, and zero merge debt.
It reduced the Antfly index from 5,065,897,930 to 4,088,444,648 bytes--a 977.5
MB or 19.29 percent saving--leaving an 836.95 MB gap to Tantivy and improving
the density ratio from 1.558x to 1.257x. Build wall time improved from 611.4 to
545.9 seconds, effectively matching Tantivy's 550.2 seconds. Peak indexing RSS,
however, fell only from 4.602 to 4.292 GB and remained 1.527x Tantivy, an excess
of 1.482 GB. Query RSS remained near parity at 23.0 versus 21.7 MB.

Selective query CPU failed the non-regression gate. Relative to v27, v28 CPU per
query regressed 2.61x for term (185.8 us), 3.43x for union (679.7 us), and 1.91x
for intersection (407.9 us); phrase improved 2.28x to 476.3 us. Diagnostics
explain the result: v27's aligned document ranges skipped 4,141 blocks for the
`alpha` term while v28's wide posting-count ranges skipped only eight and scored
15,250 rather than 1,654 pivots. The `alpha beta` union similarly fell from
4,829 to 177 skipped blocks. This is not solved by minor block-size tuning. The
next candidate should retain 128-posting compression blocks but add a compact,
finer document-range impact index for WAND, separating payload compression
granularity from competitive-pruning granularity. It must recover v27-level
term/union/intersection CPU while retaining most of the v28 size saving. Exact
evidence is checked in as
`bench/full_text/results/full-corpus-v28-posting-blocks-rejected.json`.

The persistent embedded query resource pass runs each engine/query class for
at least three seconds after correctness and warmup, reads per-process
user+system CPU before and after each class, and samples RSS every 10 ms. It is
separate from the latency samples. Against the exact indexes above:

| Query class | Antfly CPU/query | Tantivy CPU/query | Antfly/Tantivy |
| --- | ---: | ---: | ---: |
| term | 71.2 us | 38.6 us | 1.84x |
| union | 198.2 us | 229.7 us | 0.86x |
| intersection | 213.6 us | 249.1 us | 0.86x |
| phrase | 1,084.9 us | 344.0 us | 3.15x |

Steady/peak RSS during that selective query pass was 23.5/23.5 MB for Antfly
and 22.0/22.0 MB for Tantivy, a 1.07x ratio. This is the expected demand-paged
embedded working set, not the multi-gigabyte RSS seen in the invalid product
load. The CPU result also gives a narrow execution priority: preserve the
current union/intersection advantage, reduce term setup/lookup CPU, and focus
phrase work on candidate reduction and position decoding. The checked-in
`bench/full_text/results/full-corpus-v27-production-size-attribution.json` and
`full-corpus-v27-production-query-resources.json` retain the exact inputs and
measurements.

A clean full rebuild measured indexing memory separately. Antfly reproduced
5,065,897,930 bytes in 611.4 seconds and peaked at 4,601,970,688 bytes RSS.
Tantivy produced 3,252,077,340 bytes in 550.2 seconds and peaked at
2,810,429,440 bytes RSS. (Its production merge schedule varied by 583,197
bytes, 0.018 percent, from the preserved 3,251,494,143-byte size baseline.)
Thus Antfly indexing is 1.11x slower and uses 1.64x peak RSS, an excess of
1,791,541,248 bytes. This is a genuine build/merge working-set problem even
though query RSS is near parity. Format work must track builder live postings,
position scratch, section assembly, merge fan-in, and simultaneous input/output
mapping so a smaller index does not merely move the gap into peak memory.

### v29 density and resource qualification

The accepted v29 candidate addresses the persisted-size audit without changing
the product path's normal stored-document behavior. Kernel segments use a new
explicit omitted-stored-fields version, persist only the corpus ordinal plus a
segment key-range summary, and preserve that mode through merges. Normal
product segments still store documents. The merge path assembles each inverted
section in one output buffer rather than allocating and copying a second full
section. Positions share one bit width across groups of eight documents while
remaining byte-aligned per document. Posting payloads use 128-posting blocks;
single-block lists use their payload-local impact bound without a range-ID
sidecar, while larger lists retain 1,024-document impact ranges. Range IDs are
chosen per term from packed deltas, varints, and run encoding.

On the 5,032,105-document full corpus, the accepted index is 3,905,870,509
bytes versus Tantivy's 3,251,494,143 bytes: a 654,376,366-byte gap and 1.201x
ratio. This removes 635,124,487 bytes (13.99 percent) from the initial
4,540,994,996-byte v29 layout and is 182,574,139 bytes smaller than v28. All
five comparator queries are strict matches, with ten settled segments, 267
completed merges, and no pending, failed, or quarantined merge work.

The accepted physical layout is now:

| Antfly v29 component | Bytes |
| --- | ---: |
| positions | 2,149,191,067 |
| doc/frequency payload | 1,176,236,777 |
| impact records | 192,074,486 |
| chunk metadata | 126,232,145 |
| postings headers | 96,633,106 |
| impact range IDs and postings residual | 32,355,560 |
| term dictionary plus bloom | 104,787,897 |
| norms | 5,032,155 |
| ordinal sidecar | 20,128,470 |
| stored fields | 50 |

Relative to Tantivy's boundary-aligned files, the remaining gap is primarily
approximately 487 MB in postings/index structures and 219 MB in positions,
partly offset by Antfly's approximately 46 MB smaller dictionary/bloom and the
now-omitted 20 MB Tantivy store. It is no longer attributable to an unknown
filesystem component. The 20.1 MB ordinal sidecar remains approximately 14 MB
larger than Tantivy's fast field.

The accepted build used 448.1 CPU seconds over 483.9 wall seconds and peaked at
3,922,395,136 bytes RSS. The matched Tantivy reference took 550.2 wall seconds
and peaked at 2,810,429,440 bytes. Thus indexing wall time is now 12 percent
lower, but Antfly still has an unacceptable 1.112 GB / 1.40x peak-RSS gap. The
single-buffer merge removed one known full-section copy, but input mmap
residency, output capacity, and merge fan-in still require direct resident-set
attribution.

In-process `getrusage` counters now make Antfly CPU and peak RSS reliable even
when the parent process cannot inspect `ps`. The accepted full-corpus CPU/query
measurements are 103.3 us term, 290.0 us union, 261.7 us intersection, and
549.0 us phrase. Against the preserved Tantivy reference they are 2.58x,
1.36x, 1.05x, and 1.59x respectively. Antfly query peak RSS is 23.8 MB versus
20.8 MB. Query CPU therefore remains a production-performance target even
though the architectural all-hit/hash-map and stored-body defects are removed.

Two tempting follow-ups were measured and rejected. Aligning impact bounds to
payload blocks for every term reduced the index to 3,730,205,166 bytes but made
term/union/intersection CPU 1.98x/2.45x/1.62x worse than accepted v29. Allowing
that representation only through 1,024 postings produced 3,813,870,561 bytes,
but still regressed term and union CPU by 26 and 24 percent and returned peak
indexing RSS to 4.47 GB. Both formats are removed. Similarly, independent
per-document position widths grew the tuning index and slowed phrases; an
adaptive shared/independent marker saved no tuning bytes and still added about
eight percent phrase CPU. The accepted group-of-eight shared-width codec is
retained.

Exact accepted and rejected evidence is checked in as
`bench/full_text/results/full-corpus-v29-density-resource-qualification.json`.

### v30 position density and bounded merge memory

The next accepted format removes the two remaining degenerate resource costs.
File-backed merges no longer assemble a complete inverted field in an
`ArrayList` before writing it. They serialize one term at a time through a
bounded 1 MiB append buffer, patch section headers in place, fsync, and publish
with the existing atomic rename. This preserves the exact segment layout while
bounding merge heap by the largest term posting list plus compact dictionary
metadata.

Position groups still share one width across eight documents, but v30 packs the
group as one contiguous bitstream instead of rounding every document to a byte.
The frequency column supplies the selected document's value offset, so phrase
seeks decode only the candidate document. The production reader accepts the
origin/main v23 format and v30; branch-only v24--v29 formats are deliberately
rejected and need no compatibility support.

On the same 5,032,105-document corpus, v30 is 3,614,059,644 bytes versus
Tantivy's 3,251,494,143 bytes: a 362,565,501-byte gap and 1.112x ratio. It
removes another 291,810,865 bytes (7.47 percent) from accepted v29. Positions
fall from 2,149,191,067 to 1,852,638,998 bytes and are now approximately 77.6
MB smaller than Tantivy's 1,930.2 MB position file. The remaining gap is no
longer positional: Antfly's postings excluding positions are approximately 490
MB larger, led by 191.0 MB of impact records, 128.6 MB of chunk metadata, 98.5
MB of per-term postings headers, and a doc/frequency payload that is about 46 MB
larger than Tantivy's postings/index boundary.

The streamed/buffered v30 build takes 443.0 wall seconds and 427.5 CPU seconds,
with a 2,118,975,488-byte peak RSS. Relative to the equivalently instrumented
v29 build, wall time improves 7.6 percent and CPU improves 6.0 percent. Relative
to Tantivy, Antfly indexes 19.5 percent faster and peaks 691 MB lower. Thus the
former indexing RSS failure is closed; the peak was a transient full-field
merge allocation, not retained index state or unexplained filesystem overhead.

The stable three-second query resource pass measures 102.3 us term, 265.2 us
union, 241.0 us intersection, and 615.5 us phrase CPU per query. Term is flat
versus v29, union and intersection improve by 8.6 and 7.9 percent, and phrase
regresses by 12.1 percent because arbitrary-bit position starts cost more than
byte-aligned starts. A binary-search/prefix seek experiment made short phrase
jumps worse and was removed. Follow-up phrase work must retain v30 density and
optimize the arbitrary-bit decoder; restoring per-document byte padding would
reintroduce the measured 291.8 MB loss.

Exact evidence is checked in as
`bench/full_text/results/full-corpus-v30-density-memory-qualification.json`.

### v31 inline single-document postings

The next accepted format removes the full chunk envelope from terms that occur
in exactly one document. This is not the older no-positions one-hit shortcut:
v31 retains the exact frequency and packed position deltas needed by phrase
queries. A zero document-frequency value, which cannot represent a real
posting list, discriminates the compact record. It is followed by the absolute
document ID, encoded frequency/location flag, one position bit width, and the
packed deltas. Ranking reads the record without allocating chunk decode
buffers; phrase execution decodes its positions through the normal iterator
interface. The dictionary format is unchanged, and merges emit the same
compact record after applying deletions and document remapping.

This targets 7,957,061 single-document posting lists in the settled full-corpus
production layout. v31 is 3,459,488,164 bytes versus Tantivy's 3,251,494,143
bytes: a 207,994,021-byte gap and 1.064x ratio. It removes 154,571,480 bytes
(4.28 percent) from v30. The largest reductions are 64.8 MB of chunk metadata,
46.2 MB of posting payload, 20.0 MB of impact records, and 18.9 MB of per-term
headers. Positions also fall by 7.1 MB. The production reader now accepts only
origin/main v23 and current v31; branch-only v24--v30 formats are rejected.

The full build takes 429.8 wall seconds and 412.4 CPU seconds with a
1,762,033,664-byte peak RSS. Relative to v30, wall time improves 3.0 percent,
CPU improves 3.5 percent, and peak RSS improves 16.8 percent. Relative to the
matched Tantivy reference, Antfly indexes 21.9 percent faster and peaks 1.048
GB lower. The settled index has ten segments, 267 completed merges, and no
pending, failed, or quarantined work.

Stable query CPU improves in every class versus v30: 98.8 us term, 253.3 us
union, 234.9 us intersection, and 591.2 us phrase. Against Tantivy those are
2.47x, 1.19x, 0.94x, and 1.71x. All five full-corpus comparator queries are
strict ID/score matches. The remaining 208.0 MB disk gap and term/phrase CPU
ratios are therefore the next engine targets; v31 is an accepted intermediate
format, not the final parity claim.

Exact evidence is checked in as
`bench/full_text/results/full-corpus-v31-inline-single-doc-qualification.json`.

### v32 two-column posting-count metadata

Fixed 128-posting blocks do not need to persist their ordinal or document
count. The block ordinal is its metadata-array index; every block except the
last contains 128 documents, and the last count is derived exactly from term
document frequency. v32 therefore reduces compact chunk metadata from four
bit-packed columns to two: absolute maximum document ID and payload-end delta.
Seek, payload framing, and WAND bounds remain exact. The reader continues to
accept origin/main v23 and current v32 only; branch-only v24--v31 formats are
rejected.

The full v32 index is 3,440,527,184 bytes versus Tantivy's 3,251,494,143:
a 189,033,041-byte gap and 1.058x ratio. It removes another 18,960,980 bytes
from v31. Chunk metadata falls from 63.9 MB to 43.8 MB despite a different
valid production segment distribution. The dense 120K control index showed a
larger 4.3 percent reduction, 7.7 percent lower indexing CPU, and improvements
in every query class, confirming the change is valuable when multi-block lists
dominate.

The uninstrumented full build takes 423.0 wall seconds and 409.3 CPU seconds,
improving 1.6 and 0.8 percent over v31. Its conservative peak RSS is 2.251 GB,
still 559 MB below Tantivy. An identical-layout attribution repeat peaked at
2.098 GB. At the repeat peak, a 780.2 MB file-backed merge had only 310.7 MB of
physical footprint; RSS rose with mapped input/output residency and fell by
roughly 900 MB after publication. Thus the run-to-run RSS spread is merge page
residency, not a larger v32 metadata heap. Both measurements remain below the
Tantivy reference.

Full-corpus query CPU is 94.9 us term, 268.0 us union, 245.4 us intersection,
and 601.3 us phrase. These are 2.37x, 1.26x, 0.99x, and 1.74x Tantivy. Five of
five comparator queries remain strict ID/score matches. The next phase should
profile term dictionary/setup and phrase positional decode rather than infer a
query win from this size-oriented change.

Exact evidence is checked in as
`bench/full_text/results/full-corpus-v32-two-column-meta-qualification.json`.

### v33 constant-frequency posting blocks

Most posting blocks repeat the same encoded frequency/location value, commonly
`freq=1, has_positions=true`. v33 marks such a block in its frequency control
byte and stores the value there instead of writing a redundant packed frequency
column. Document IDs, positions, scoring, and impact bounds are unchanged. The
reader accepts origin/main v23 and current v33 only; branch-only v24--v32 are
rejected.

On the full corpus, v33 produces 3,435,521,272 bytes, 184,027,129 bytes above
Tantivy (1.0566x). The saving over v32 is only 5.0 MB at full scale, much less
than the dense 120K control suggested, but it also improves the full build to
431.5 wall seconds, 415.4 CPU seconds, and 2,057,519,104 bytes peak RSS. Stable
query CPU is 104.4 us term, 293.4 us union, 253.6 us intersection, and 591.5 us
phrase. This is a useful payload optimization, but the remaining size gap is
still dominated by the separate 174.9 MB impact map rather than the frequency
payload.

### v34 five-bit conservative impact frequencies

The accepted v34 layout keeps every differentiated 1,024-document impact range
and its exact eight-bit minimum field-norm ID, but replaces the eight-bit
maximum-frequency ID with a five-bit conservative bucket. Thirty-two monotonic
upper bounds are exact through the common low frequencies and increasingly
coarse above them; the escape bucket remains `u16::max`, so every BM25
configuration still receives a safe upper bound. The reader extracts a fixed
five-bit value with one bounded 16-bit window/shift/mask operation rather than
the generic arbitrary-width decoder. Writers and merges reuse existing
per-term scratch and do not retain a second impact buffer.

The full v34 index is 3,406,622,680 bytes versus Tantivy's 3,251,494,143:
a 155,128,537-byte gap and 1.0477x ratio. It removes 28,898,592 bytes from v33
despite a less favorable production segment distribution. Impact metadata is
141,577,908 bytes, down from v33's 174,906,554 bytes; positions remain
1,845,486,240 bytes, about 84.7 MB smaller than Tantivy's position file. The
remaining gap is therefore known postings/index metadata, led by 141.6 MB of
impact data, 78.9 MB of postings headers, and 44.1 MB of chunk metadata, partly
offset by Antfly's smaller positions/dictionary and omitted store.

Indexing takes 421.9 wall seconds and 407.9 CPU seconds with a
2,141,044,736-byte peak RSS. This is 23.3 percent faster and 669.4 MB lower RSS
than the matched Tantivy reference. After specializing the fixed-width decoder,
stable query CPU is 104.9 us term, 302.8 us union, 262.2 us intersection, and
576.2 us phrase. Relative to v33 these are +0.5, +3.2, +3.4, and -2.6 percent;
the disk/indexing win is accepted without a material ranking-path regression.
All five full-corpus comparator queries are strict ID/score matches.

Three smaller-looking alternatives were measured and rejected. Collapsing
differentiated low-DF ranges to one global bound reduced the index to
3,279,495,439 bytes—only 28.0 MB above Tantivy—but raised sustained term/union/
intersection CPU to 216.3/742.9/430.1 us by eliminating useful pruning. A
shared constant-frequency column was 24,744 bytes larger than same-layout v33.
Four-bit frequency buckets saved another 10.2 MB but raised sustained ranking
CPU by 8--12 percent before decoder specialization. These are explicit
non-goals for follow-up work: reduce header/range-ID representation without
removing differentiated WAND bounds.

Exact accepted and rejected evidence is checked in as
`bench/full_text/results/full-corpus-v34-five-bit-impact-qualification.json`.

### v34 query-path follow-up: dedicated single-term Block-Max

A native sample of the preserved full-corpus v34 index rejected the initial
term-dictionary hypothesis as the dominant gap: lookup represented only about
1.3 percent of active term-query samples. The lookup still no longer allocates
for normal query terms and now stops once a front-coded block passes the target,
with a failing-allocator regression proving the common path remains on stack.
Most active samples were instead in generic WAND control, postings iteration,
packed document decode, and impact-bound evaluation.

Single-term top-k now has a dedicated Block-Max loop. It scores the current
posting directly and feeds the collector threshold into the existing
conservative competitive-block advance, without maintaining a term-order array,
finding a WAND pivot, summing a one-element bound, or decoding that bound again
after collection. Later high-impact blocks and ordered cutoff ties retain their
focused regressions. The persisted format and source index are unchanged.

Two independent five-second-per-class resource passes on the settled
5,032,105-document, ten-segment v34 index measured 93.492 and 93.012 us CPU per
term query, versus 110.665 us in the matched pre-specialization pass. The mean
93.252 us is a 15.7 percent reduction. Union, intersection, and phrase CPU
changed by -0.9, -0.8, and -1.1 percent respectively. Eleven-repetition term
latency medians were 108.8 and 104.3 us versus 120.4 us in the matched baseline.
Against the archived Tantivy reference, term CPU improves from the original
v34 ratio of 2.62x to about 2.33x; it remains a priority rather than a parity
claim.

A machine-word refill prototype for the packed-u32 reservoir was explicitly
rejected. On the same artifact it left term CPU flat, moved union/intersection
by less than one percent, and regressed phrase CPU by 4.3 percent. The existing
byte-fed reservoir remains production. Exact evidence is checked in as
`bench/full_text/results/full-corpus-v34-single-term-specialization.json`.

### v34 phrase follow-up: deferred positional verification

The scored phrase path previously decoded positions during document-ID
alignment. A document rejected because another term advanced beyond it had
already paid position-frame decode, delta unpacking, and scratch-list growth.
The iterator now exposes a deferred positional seek: it skips framed position
records while aligning the term iterators, leaves the shared candidate pending,
and unpacks positions only after the document-level conjunction succeeds. This
is a true two-phase phrase executor; it does not alter exact phrase frequency,
BM25 scoring, or the persisted format.

The focused suite passes 50 tests with zero failures and leaks, including a
failing-allocation dictionary regression, randomized positional-reference
equivalence, and a direct iterator test proving skipped documents are not
decoded. Query diagnostics now report decoded position records. On the settled
full-corpus `alpha beta` phrase, 3,571 document candidates caused exactly 7,142
position-record decodes--two per candidate--and produced the same 525 exact
matches and ordered top hits.

Two independent matched resource passes measured 577.131 and 551.702 us CPU
per phrase query, versus the immediately preceding 588.260 and 580.972 us
runs. Mean CPU improves 3.46 percent. Eleven-repetition phrase latency medians
were 612.875 and 555.209 us versus 625.292 and 644.916 us, an 8.04 percent mean
reduction. Exact evidence is checked in as
`bench/full_text/results/full-corpus-v34-deferred-phrase-positions.json`.

### v34 query-path follow-up: direct scoring advancement

A new native profile after the single-term specialization showed that
`PostingsIterator.advanceTo` still walked rejected documents through generic
`next()`. That path decoded frequency and norm state, cleared positional
scratch, and updated impact bookkeeping for every skipped posting even though
only the selected document could be scored. The iterator now scans decoded
document IDs directly to the target and decodes exactly one selected scoring
hit. WAND also uses a dedicated position-free `nextScoring` entry point, which
removes its positional branch and scratch touch. The persisted format and
scored-document set are unchanged.

The focused suite passes 50 tests with zero failures or leaks. Two independent
full-corpus resource passes measured mean CPU/query of 72.700 us term, 250.710
us union, 208.405 us intersection, and 546.245 us phrase. Relative to the
immediately preceding matched runs, those are reductions of 23.0, 21.8, 24.9,
and 3.2 percent. Mean latency medians improved by 34.2, 19.4, 22.6, and 1.1
percent. Query work counters and the strict comparator results are unchanged.

Against the archived Tantivy CPU reference, the current ratios are 1.82x term,
1.18x union, 0.84x intersection, and 1.58x phrase. Antfly therefore wins the
measured intersection CPU case and is close on union, while term and phrase
remain explicit optimization targets. Exact evidence is checked in as
`bench/full_text/results/full-corpus-v34-direct-scoring-advance.json`.

### Remaining term and phrase CPU gap: profile-directed plan

Fresh native samples after direct scoring advancement show that the remaining
term gap is no longer query parsing, allocation, stored-document loading, or
generic boolean composition. The dominant term leaves are packed postings
decode, payload-block loading, compact-metadata reconstruction during seeks,
and block-max ceiling evaluation. Phrase shares the postings/seek costs, then
adds deferred position-cursor advancement, selected position-record unpacking,
and skipped-position bookkeeping. The measured `alpha beta` phrase verifies
3,571 candidate documents, decodes exactly 7,142 selected position records, and
scores 525 exact matches.

Four plausible scalar shortcuts were measured and rejected rather than being
carried as speculative complexity:

- Random-access frequency extraction avoided full frequency-column decode but
  made term, union, and intersection CPU 5.0, 3.9, and 4.0 percent worse. The
  sequential 128-value reservoir is cheaper than repeated bit addressing.
- Caching the current single-term impact ceiling did not beat baseline. Sparse
  hits do not reuse a 1,024-document impact range enough to repay its branch.
- Binary-searching v34's standalone max-document metadata column did not beat
  the checkpoint-window scan. Existing skip records usually leave too little
  search work to amortize packed-layout parsing.
- Fusing delta unpack and prefix accumulation was tested with a matched A/B.
  Baseline term/union/intersection/phrase CPU was 67.067/227.567/194.288/588.384
  us per query; fused CPU was 71.759/246.150/201.720/588.225 us. The scalar
  fusion regressed the first three by 7.0, 8.2, and 3.8 percent and left phrase
  flat. The compiler's existing decode-then-prefix loops remain in production.

The next term work therefore targets blocks, not individual values:

1. Parse and retain the compact metadata-column layout once per postings view,
   then pass that view through chunk seek/load instead of reparsing it at every
   metadata access. This is format-neutral and must preserve the zero-copy
   mmap ownership boundary.
2. Add a portable packed-block decoder using Zig `@Vector` operations and a
   scalar tail/reference path, specialized by observed bit width. Decode
   complete 128-value document and frequency columns; do not reintroduce
   per-hit extraction. Let Zig/LLVM lower the same source to NEON, SSE/AVX,
   WASM SIMD, or scalar code for the selected target. Measure CPU,
   instructions, branch misses, code size, and RSS, and inspect generated code
   on arm64 and x86_64 to ensure the intended vectorization occurred.
3. Compare the current bit-packed payload with a vector-friendly block codec
   only if the SIMD decoder remains dominant. A v35 format is acceptable when
   the end-to-end CPU win survives its disk, page-touch, and merge-cost impact.
   Production compatibility remains origin/main v23 plus the new current
   writer format; development-only v24-v34 and rejected v36-v37 formats need not become a permanent
   compatibility chain.
4. Replace repeated block-max BM25 arithmetic with a snapshot/field-scoped
   lookup over the existing five-bit frequency bucket and eight-bit norm ID.
   IDF remains a per-term multiplier. Accept only if building and touching the
   table costs less than direct arithmetic across the mixed query set.

Architecture-specific intrinsics, inline assembly, and target-name branches are
outside the default plan. They may be considered only as a separately measured
fallback if the portable `@Vector` implementation cannot express a required
operation or produces demonstrably inadequate code on a supported production
target. Such an exception must retain the same scalar reference tests and may
not change the portable on-disk format.

The first two shared-path candidates are now resolved. Retaining a parsed
compact-metadata layout measured 74.2/257.9/220.0/619.6 us term/union/
intersection/phrase CPU and was rejected. Portable vertical BP128 is implemented
with `@Vector(4, u32)`, a scalar reference decoder, and exhaustive bit-width
round trips. Its 128-value microbenchmark takes 17.5--22.2 ns per block versus
61.3--228.7 ns for the horizontal reservoir, but the isolated v35 full-corpus
run still measured 72.8 us term latency and grew the index by 2.75 MB. The codec
is retained as the portable block foundation, not claimed as the end-to-end
term-gap fix.

v36 tested the structural cost exposed by the profile and size attribution.
It writes exactly one compact conservative impact bound per 128-posting payload
block. The bound and payload share an ordinal and exact `[min_doc,max_doc]`
interval; `impact_ids_len` is zero, so the separate 1,024-document impact map,
range-ID sidecar, and query-time coordinate translation disappear. Single-term
Block-Max scans advance directly by payload ordinal, and multi-term WAND and
conjunctions reuse the existing conservative posting-interval sweep. The
five-bit frequency ceiling and exact norm ID remain unchanged, preserving
correctness. It produced a 3,254,674,194-byte index--only 3,180,051 bytes above
Tantivy--and reduced indexing CPU 11.3 percent versus v35. It was nevertheless
rejected: sparse terms lost document-local selectivity. `alpha` pruned only 6
payload blocks and scored 15,459 candidates versus v35's 4,067 pruned ranges
and 2,179 scored candidates. Term/union/intersection CPU regressed to
128.7/624.5/313.2 us despite phrase improving to 494.0 us.

v37 tested restoring the selective 1,024-document bounds and their adaptive
range IDs, but compresses repeated exact `(five-bit frequency ceiling,
eight-bit minimum norm)` pairs through a per-term palette. A palette is used
only when it is smaller than the direct 13-bit columns; fewer than eight records
bypass palette construction, and more than 64 distinct pairs fall back to the
direct representation. Palette lookup is allocation-free and O(1) on the query
path. This preserves v35 pruning coordinates while attacking their measured
storage rather than trading them away. It restored the v35 work counters
exactly, but produced a 3,409,090,313-byte index--only 278,776 bytes below v35--
and raised term CPU from 64.4 to 85.8 us. The bound-pair distribution is too
high-entropy for a per-term palette, so v37 is rejected. Production remains on
v35 while the next storage design is developed from measured range data. The
experimental reader/writer and its query-time branch were removed after
measurement; the retained evidence records the exact format result without
making v37 part of the compatibility surface.

The first format-neutral term follow-up is accepted. A `BM25TermScorer` now
retains `idf * (k1 + 1)`, `k1 * (1 - b)`, and `k1 * b / avg_field_length` once
per WAND term state. Document scoring and conservative block ceilings use the
same object, removing repeated query-invariant arithmetic from both alpha's
2,179 scored postings and its 4,067 rejected impact ranges. In an adjacent
candidate/baseline/candidate/baseline bracket, baseline term CPU averaged
65.801 us and the candidate averaged 63.255 us, a repeatable 3.87 percent
reduction. Union CPU improved 1.77 percent; intersection was flat within 0.12
percent. Phrase is not on this WAND path and its 1.20 percent mean movement is
treated as run noise. Work counters were identical, query RSS remained about
23 MB, and all five queries strictly matched the archived Tantivy results at
the benchmark's `1e-5` absolute and relative score tolerances. The persisted
v35 format and index size are unchanged. The focused suite passes 53 tests
with zero failures or leaks, including direct equivalence checks across the
packed impact frequency and norm ranges.

The planned bound lookup is also accepted, layered on that scorer. Each
snapshot lazily caches the IDF-independent TF ceiling for the current 32 packed
frequency IDs and 256 norm IDs. The key is the exact bit representation of
average field length, `k1`, and `b`; the cache is thread-safe and capped at four
tables (128 KiB of values) per snapshot. A fifth distinct configuration and
origin/main v23 metadata use the scalar fallback, so query-controlled BM25
parameters cannot grow an unbounded cache. This small fixed snapshot cache is
below the resource manager's large-buffer/mmap budget boundary; its explicit
cap is the governing resource policy.

Reassociating the IDF multiply can differ from direct scoring by several
`f32` ulps, so table values receive an eight-ulp upward bias once at
construction. Runtime remains one indexed load and one multiply. The focused
suite passed 55 tests at this stage, including an exhaustive check that every one of the
8,192 packed frequency/norm pairs remains at or above direct scoring across six
IDFs. As expected for a deliberately conservative rounding change, alpha
scores 2,196 cutoff candidates instead of 2,179 and gamma scores 488 instead
of 482; all ordered hits and scores still strictly match archived Tantivy.

Two full-corpus passes measured 56.109 and 56.014 us term CPU, averaging
56.062 us: 11.37 percent below the already accepted precomputed-scorer mean and
about 1.40x the archived Tantivy 40.028 us result. Union CPU improved another
4.59 percent to a 214.556 us mean; intersection remained flat. Query peak RSS
was 23.35 MB, the persisted v35 index is unchanged, and the one table used by
this workload accounts for only 32 KiB of snapshot-owned values.

Phrase now has a format-neutral positional fast path after those shared wins.
For exact two-term phrases over the current contiguous grouped-position layout,
each iterator returns a validated bounded view of the immutable packed bits.
Two cumulative-delta cursors then intersect the monotonic position streams
directly, without allocating or filling absolute-position arrays. Generic
N-term phrases, slop, origin/main v23 data, and any noncontiguous layout retain
the existing materialized verifier as the correctness fallback.

Two independent full-corpus passes measured 425.670 and 423.973 us phrase CPU,
averaging 424.822 us. That is 17.97 percent below the accepted 517.872 us
baseline and about 1.23x the archived Tantivy 344.910 us result. Both passes
strictly matched all five archived Tantivy queries and preserved exactly 3,571
phrase candidates, 7,142 decoded position records, and 525 matches. Peak query
RSS remained 23.28 MB, and the persisted v35 index is unchanged. The focused
suite now passes 56 tests with zero failures or leaks, including arbitrary
in-group starts for the bounded packed cursor.

Two cheaper-looking phrase variants did not survive measurement. A monotonic
merge over already decoded absolute-position arrays measured 517.442 us versus
the 517.872 us baseline, a statistically flat 0.08 percent movement. A hybrid
linear/binary lower-bound search inside decoded document blocks regressed
phrase CPU to 599.483 us, or 15.76 percent. Both retained identical correctness
and work counters and were removed. If cursor advancement becomes material in
a future profile, sparse per-group bit-offset checkpoints remain a possible
format experiment; a per-posting offset is excluded unless it demonstrates a
net CPU/page-touch win within the storage budget.

The removed first cursor prototype scanned document IDs to a target ordinal and
jumped complete eight-document packed position groups in one cursor update.
It preserved the 3,571 candidates, 7,142 decoded records, 525 matches, and all
top hits, but phrase CPU rose from 556.929 to 573.291 us (2.94 percent).
Term/union/intersection CPU did not show a compensating shared-path win. The
extra target scan, branch, and group-state writes cost more than the rejected
documents saved for this candidate distribution, so the prototype and its hot
branch were removed.

The final shared-path profile showed why vertical BP128 alone had not closed
the term gap: document deltas were decoded four at a time, then converted to
absolute IDs through a scalar 127-add dependency chain. `loadChunk` was the
largest active top-of-stack symbol at 1,705 samples; portable BP128 decode
itself accounted for only 397. The retained decoder now fuses delta decode with
a four-lane inclusive scan and a carry between vectors using
`@Vector(4, u32)`. There are no target-specific intrinsics, assembly, or
architecture branches. Origin/main v23, partial blocks, and nonvertical data
retain the scalar path, and the v35 bytes are unchanged.

Two full-corpus passes measured mean CPU/query of 47.760 us term, 190.900 us
union, 161.532 us intersection, and 400.633 us phrase. Relative to the packed
phrase-stream baseline, those are reductions of 13.41, 7.76, 11.28, and 5.69
percent respectively. Term is now about 1.19x the archived Tantivy 40.028 us
result and phrase is about 1.16x its 344.910 us result. Both passes strictly
matched all five comparator queries with unchanged work counters; peak query
RSS was 23.64 MB. Randomized tests cover every bit width from zero through 32
and compare the fused output with scalar wrapping prefix sums. Both codec tests
are part of the focused ReleaseFast gate, which now passes 58/58 with no leaks.

Every candidate uses the preserved full corpus and strict IDs/scores/counts and
work-counter checks. Acceptance uses interleaved matched A/B runs; archived
Tantivy and Quickwit artifacts are reused, not recollected. Exact rejected
prototype evidence is summarized in
`bench/full_text/results/full-corpus-v34-term-phrase-cpu-followup.json`.

### v38 compact postings headers

The next persisted-size pass measured the remaining structure before changing
it. Across the settled v35 index, exact per-term range packing of the five-bit
frequency and eight-bit norm columns would reduce 141,577,404 impact bytes by
only 18,296,976 bytes. Only 259,540 terms selected the adaptive form while
4,521,636 retained the raw columns. That potential 0.54 percent total-index
saving does not justify another bound-decoder branch after v37 already showed
the sensitivity of this hot path, so the projection is retained as evidence
rather than promoted to a wire format.

The larger safe redundancy was in every non-inline postings header. Fixed-count
blocks derive their count from document frequency, the two compact-metadata
width bytes determine the metadata length, and the fixed checkpoint stride
determines skip length. v38 omits all three fields. Terms contained in one
posting block also omit the known impact count of one and range-ID length of
zero. Payloads, positions, exact differentiated 1,024-document impact bounds,
and range IDs are byte-for-byte unchanged. The reader accepts only origin/main
v23 and current v38; branch-only v24-v37 remain outside the release contract.

The deterministic 120K matched A/B reduced the index from 3,238,044 to
3,027,962 bytes, passed all five queries strictly, and moved sustained CPU by
less than one percent in every class. On the 5,032,105-document corpus, v38 is
3,385,816,859 bytes, 23,552,230 bytes below v35. Postings headers fall from
78,864,502 to 55,444,699 bytes. The remaining gap to Tantivy is 134,322,716
bytes, or a 1.0413x ratio.

The full v38 build completed in 421.345 wall seconds and 407.516 CPU seconds,
improving 5.64 and 5.35 percent versus v35. Peak RSS was 1.925 GB versus v35's
1.658 GB, within the already measured merge-residency variance and still 31.5
percent below Tantivy's 2.810 GB; it is not presented as an RSS improvement.
Three v38 query passes averaged 48.981/193.827/165.261/441.923 us CPU for
term/union/intersection/phrase. Against an adjacent v35 control they moved
+0.43/-3.85/-6.45/-3.31 percent. Every pass strictly matched all five archived
Tantivy queries and preserved the phrase work counts. Exact evidence is in
`bench/full_text/results/full-corpus-v38-compact-postings-header-qualification.json`.

### Public product storage, source, and ingestion amplification

The public server comparison now stores document source exactly once. Text
segments created by normal ingestion and backfill set
`store_document_source=false`; result projection hydrates only the selected
page from the primary document store for both score-order and field-order
queries. Background statistics use postings rather than stored JSON, and shard
split rebuilds merge existing segment inputs rather than reanalyzing stored
text bodies. A score-order regression verifies primary-source hydration while
the focused search suite verifies the index-only segment contract.

On the deterministic 120K product workload, the old source-duplicating path
loaded in 35.427 seconds at 3,387 documents/second and occupied 424,496,866
bytes immediately after load. The index-only text path loaded in 28.164 seconds
at 4,261 documents/second and occupied 280,501,836 bytes: 20.5 percent faster
and 33.9 percent smaller. Settled size fell from 373,171,053 to 269,640,058
bytes. Load peak RSS was effectively unchanged (1,039,826,944 versus
1,044,725,760 bytes), so this is a disk/CPU win rather than a memory claim. The
new settled root attributes 132,990,610 bytes to text, 116,035,006 to primary,
and 20,612,906 to WAL.

The first full online-ingestion attempts then exposed an independent LSM
generation-lifetime bug. Read transactions borrowed run metadata, but retired
compaction inputs were protected by one backend-wide `active_readers` counter.
All obsolete generations were retained until the entire backend happened to
have zero readers at once. Continuous, overlapping derived scans can prevent
that global quiescent instant indefinitely, so obsolete primary generations
and their table files grew without bound even though each reader needed only
one finite generation.

The stopped 3,605,808-document artifact reconciles the amplification exactly.
Its primary `runs/` directory contained 22,573,154,374 bytes in 760 files, but
the manifest referenced only 3,907,080,998 live bytes in 137 files. Another
17,041,945,307 bytes in 618 files were explicitly recorded obsolete
generations, and 1,624,128,069 bytes in five untracked files were in-flight
outputs left by the forced stop. The primary WAL contained 2,645,367,909 bytes
across 46 physical files. Thus about 75 percent of the primary run bytes were
known obsolete history; this was not live document size, table-codec density,
or unexplained filesystem overhead. Recovery already removes orphaned atomic
outputs, while the normal-running leak required the generation-lifetime fix.

The production fix gives every read snapshot explicit per-run pins in a
hash-indexed registry. Compaction releases the backend's ownership at publish;
retired run metadata and files become reclaimable as soon as that run's own last
snapshot exits, even while readers of newer generations remain active.
Manifest publication and obsolete-file reconciliation no longer require
backend-wide reader quiescence. Open-handle version pins remain separate, so a
second backend handle still prevents deletion of files referenced by its
manifest. Obsolete-path durability and reclamation scheduling are also
separate states: once a queued path is present in a durable manifest it is no
longer marked dirty merely because a reader pins it or its retention deadline
has not arrived. The retention timer and per-generation pin release wake
maintenance when deletion can make progress, avoiding identical manifest
rewrites on every scheduler pass. Unit coverage holds an old reader across
compaction, opens a newer reader, releases only the old reader, verifies
immediate old-generation reclamation, proves the newer reader remains correct,
and asserts that an independently pinned generation causes no repeated
maintenance or manifest publication.

Primary WAL retention also has an explicit bounded fallback. Background
maintenance remains the preferred checkpoint path, and the 512 MiB soft / 2
GiB hard durability limits are unchanged. If background flushing is unable to
keep up past the soft limit, the primary store performs at most one
checkpoint-producing immutable flush per 250 ms enforcement interval. This
incremental fallback prevents normal ingestion from drifting to the hard limit
and then charging a single multi-generation drain to one request. It is
per-store opt-in and preserves WAL replay, manifest durability, and crash
recovery semantics.

No new full-corpus server result is acceptable until a bounded online-load gate
shows all of the following:

- physical primary bytes stay close to live manifest-referenced bytes rather
  than accumulating obsolete compaction history;
- obsolete paths fall promptly as their individual readers exit under
  overlapping read/write activity;
- retained WAL stays near the soft limit and never exceeds the documented hard
  bound;
- primary table-write and compaction amplification, load CPU/wall time, and
  peak RSS are reported alongside text bytes;
- graceful and crash restart both preserve document count, text correctness,
  and source hydration.

Only after this bounded gate passes should the 5.03M public HTTP concurrency,
freshness, mixed-write, and recovery matrix be rerun. An explicit import API may
later add sorted final-state publication for known-new keys, but it is not a
substitute for keeping ordinary durable online writes bounded.

### Bounded online-load follow-up

The generation/WAL fix passed a fresh 120K public-server gate: 120,000
documents loaded in 33.666 seconds, peak server RSS was 1.032 GB, final primary
runs were 109.85 MB, final WAL was 19.52 MB, and both manifest inventories had
zero obsolete or untracked files. Search, freshness, graceful restart, and
crash restart all remained correct.

A 300K memory-attribution run then completed in 150.924 seconds with a 1.783 GB
sampled RSS peak. Primary manifests again retained zero obsolete bytes; WAL
peaked at 78.92 MB and ended at 45.86 MB. This run exposed a separate text
merge working-set cost: the merge retained an allocation and 16 bytes of bloom
hashes for every unique term, encoded the complete dictionary, and copied that
dictionary once more into the segment. Merge dictionary construction now keeps
only one 48-term source block, emits blocked dictionary components directly to
the file-backed segment sink, and reconstructs the exact-size bloom filter in
one sequential pass over compact encoded blocks. Multi-block, overlap, bloom,
delete, and one-hit merge tests cover the new path.

The next fresh 1M attempt reached 980,610 submitted documents in 159.99 seconds
at 6,129 docs/s. Its RSS peak was 4.004 GB, down from the previous 6.9 GB, while
the peak merge sample reported only about 698 MB of physical footprint and 672
MB of live allocator bytes; most of the macOS RSS was clean/reclaimable mapped
residency. The run was deliberately stopped instead of accepted because final
settlement reproduced a pathological primary layout: 47 L0 runs (136.17 MB)
plus four L1 runs (1.313 GB), with L1 1.179 GB over its target. WAL was already
zero and obsolete paths remained zero, proving this was neither the old
generation leak nor WAL retention.

The compaction planner was prioritizing absolute L0 run debt over proportional
level pressure. As L1 grew to roughly ten times its byte target, each subsequent
L0-to-L1 compaction could rewrite the oversized L1 again; L1-to-L2 promotion
remained starved until L0 nearly drained. Candidate scoring now uses normalized
`current / target` pressure for L0 and lower levels. The maintenance entry point
also no longer bypasses that global comparison merely because L0 exceeds its
soft limit. Overlap remains eligible at four L0 runs, but its pressure score uses
the 32-run soft bound, so eligibility cannot masquerade as 25x urgency. A
regression fixture matching 102 L0 runs plus four 284 MB L1 runs selects L1 to
L2 because the two sources are 3.2x and 8.5x over target respectively.

L0 compaction windows were widened at the same time. The old `2 * l0_limit`
source cap could require many publications to close a hard backlog and repeatedly
touch the same lower-level overlap. One overlap closure now drains toward half
the trigger, bounded by `max_compaction_input_bytes`; the `l0_limit = 0` repair
case retains its oldest-pair behavior.

The first fresh run with this wider fan-in exposed a separate memory defect and
was stopped. Every `PersistedRunCursor` retained a complete point-query
`TableIndex` for every input run: entry-offset arrays, global and block blooms,
point hash slots, bounds, and prefix filters. Physical memory climbed to about
6.4 GB while the hot stack was
`buildCompactedRunsFromSnapshots -> StreamingRunFileWriter`. Compaction now
loads a sequential index containing only each block's logical/physical window,
codec, and entry count. It validates encoded records while scanning and skips
all point-query acceleration structures. This does not change the table wire
format.

With the sequential cursor, the next upload reached 980,610 documents in
128.181 seconds, or 7,650 docs/s. The primary LSM compaction resource peak was
about 1.695 GB instead of the earlier multi-gigabyte cursor amplification. At
the last preserved status snapshot, primary state had settled to five runs /
1,489,519,632 bytes, zero L0 runs, zero obsolete paths, one 54,376,483-byte
immutable memtable, and 52,096,902 bytes of WAL. The entire preserved server
root was about 2.5 GB, so the original 12 GiB generation plus 1.9 GiB WAL
failure was no longer present.

That run was still rejected rather than reported as a completed benchmark. Its
server process later reached 6,933,269,776 bytes of physical footprint while
waiting for derived text catch-up. A live profile reconciled this to two
independent observability/runtime costs, not primary compaction:

- the threaded durable-job reaper removed only 32 completions every 50 ms, so
  its maximum retirement rate was 640 jobs/s, far below ingestion; each removal
  used `orderedRemove`, shifting the remaining pointer array and retaining the
  completed job payload;
- detailed memory-layout metrics walked every term and posting in every segment
  during normal health refreshes and benchmark batch attribution, repeatedly
  faulting clean mapped index pages into RSS.

The durable-job lane now detaches up to 4,096 completions in one partitioning
pass, uses O(1) unordered removals, and drains without sleeping while completion
debt remains. Periodic health metrics use the already cached basic per-segment
layout. Detailed term/posting statistics remain an explicit, sampled diagnostic
and default to one sample per 64 text batches when enabled. The server also
exports lifetime peak physical footprint, and the benchmark sampler records it
alongside sampled RSS and allocator bytes so clean mmaps cannot hide anonymous
pressure or vice versa.

A fresh gate with those changes uploaded 980,610 documents in 193.378 seconds.
It validated the final compaction ordering: the upload-end shape of 79 L0 runs /
245,908,417 bytes plus four L1 runs / 1,209,643,633 bytes promoted downstream
first and settled to zero L0 runs and five lower-level runs / 1,509,364,921
bytes at level 2, with zero obsolete paths and 63,504,032 bytes of WAL. The run
was still stopped rather than accepted because physical footprint peaked at
4,201,635,880 bytes. Crucially, it then collapsed from 3,714,358,760 bytes to
314,628,744 bytes while a text merge ran and the bulk reaper caught up. The old
reaper `memmove` stack was absent. This isolated the remaining peak to retaining
large completed job payloads until their small future/entry shells were joined.

The worker now invokes an atomic once-only payload destructor immediately after
`job.run` returns. Concurrent owner drains and the reaper still await the future
and destroy its entry, but cannot double-destroy the payload. A focused test
holds the reaper mutex across a completed job and proves the payload is released
before any join can occur. This gives large transaction buffers their actual
worker lifetime rather than queue-backlog lifetime.

The complete LSM backend suite now passes 244 tests with one platform skip,
zero failures, and zero leaks. The root suite passes 193 tests with zero
failures or leaks, and the server benchmark tool suite passes 12 tests. A new
fresh 1M gate built with the final global planner, sequential cursor,
worker-lifetime payload release, cheap health metrics, and peak-footprint sampling is still required
before the bounded online-load milestone is accepted.

The worker-lifetime change passed a fresh 300K gate: the complete load and final
`full_index` synchronization took 181.8 seconds, lifetime physical footprint
peaked at 680,577,808 bytes, and the final primary state was two active runs /
387,312,807 bytes with zero obsolete bytes and 45.87 MB of WAL. Sampled RSS
still reached about 1.86 GB because clean text-segment mappings remain resident;
on macOS, physical footprint is the acceptance metric for anonymous pressure.

The next 980,610-document gate uploaded at about 3,300 docs/s but was rejected
when the final synchronization retained 5,598,766,248 bytes of physical memory.
This exposed an admission bug: ordinary `.write` requests bypassed the existing
128 MiB soft / 192 MiB hard derived-backlog policy. Durable writes now
participate in that pressure gate, while `.propose` remains the explicitly weak
admission mode. A corrected 300K gate then completed in 96.511 seconds (3,108
docs/s including final synchronization), with two primary runs / 387,454,987
bytes, no obsolete generations, 45.87 MB WAL, and a 922,602,952-byte lifetime
physical-footprint peak.

The corrected million-document gate still failed, which separated backlog
admission from the remaining LSM allocation defect. It uploaded 980,610
documents in 205.143 seconds, then reached 6,074,703,168 bytes of physical
footprint and about 6.21 GB reported live allocator bytes during catch-up. At
the same sample, resource accounting reported only about 921 MB total LSM
resources, including 803 MB of in-memory state, while mapped text segments were
about 872 MB. Several gigabytes then disappeared in large steps as mutable
generations were released. The run was stopped rather than accepted.

Native LSM run files are not memory mapped: they use cached file descriptors,
range reads, bounded decoded blocks, and resource-accounted caches. There is no
LSM mapping on which to apply the text segment `madvise(DONTNEED)` policy.
Post-compaction file-cache advice remains an optional system page-cache tuning
only if host-wide evidence calls for it; it does not address process physical
footprint and can damage warm-read latency.

The actual undercount was in `ActiveMemTable`. Its hash index stored an
independently allocated `ArrayList` for every key even though almost all hash
buckets contained one entry, and the 32--64 MiB byte flush limits did not count
that index. Arena capacity was also inferred from currently visible values, so
superseded variable-size values and allocator slack were invisible until the
whole generation was released. The index now stores one inline entry number per
hash and allocates a side bucket only for true 64-bit hash collisions. Mutable
memory accounting includes entry-array capacity, the exact arena capacity, hash
table capacity, and collision storage. Actual retained bytes drive resource
accounting, read-snapshot limits, and a 2x mutable-memory safety guard. Logical
live-entry bytes remain the normal flush/run-sizing control and the serialized
I/O estimate, so allocator slack and overwritten arena data do not fragment the
on-disk LSM or distort disk-work accounting.

Recomputing logical size by scanning the growing table at every write initially
made that separation CPU-degenerate. `ActiveMemTable` now maintains logical
bytes incrementally. A 200K isolation run completed in 44.95 seconds (4,449
docs/s), with 592.7 MB peak disk and one 206.35 MB final primary run. A fresh
300K run completed in 146.559 seconds (2,047 docs/s), with 920.9 MB lifetime
physical footprint, 1.024 GB peak disk, and two final primary runs / 386.5 MB.
That run was slower than the accepted 96.511-second 300K baseline, so the
throughput result remains a regression signal even though the allocation model
is now correct.

The next guarded million-document run reached all 980,610 upload documents in
232.325 seconds (4,221 docs/s) but was stopped during final synchronization at
2.647 GB physical footprint. Resource accounting identified 1.073 GB of live
`lsm.in_memory_state`: eight count-admitted immutable generations could each be
large enough to hit the actual-memory guard while one unlocked flush build was
in flight. At stop time the root was 2.443 GB: about 1.987 GB primary runs,
444.5 MB text segments, and only 11.5 MB WAL. This is no longer an unexplained
mmap or WAL-retention peak.

The production fix has two layers. Each backend has an aggregate immutable-byte
limit in addition to its generation-count limit (primary 256 MiB, dense 512
MiB, and text/sparse/graph 128 MiB), preventing one store from monopolizing the
global pool. `ResourceManager` remains the global authority: it evaluates
projected LSM state before WAL append, applies the configured pressure action,
and provides a shared wakeable epoch for callers that can safely wait. A hard
pressure decision therefore rejects before durability/apply. At the backend
commit boundary, soft `throttle_writes` drains local mutable/immutable state
but does not sleep on usage owned by another backend: this boundary may be
entered while its caller holds a higher-level DB apply lock. Writers that
encounter an in-flight unlocked local flush release the backend mutex and wait
on its completion condition because that flush can progress independently.
Observation and admission use the same global LSM-state budget.

The first gate with local byte caps and admission completed 300K documents in
121.914 seconds (2,462 docs/s including final synchronization). Peak sampled RSS
was 1.665 GB, final disk was 843.9 MB, peak disk was 1.084 GB, and the final
primary state was two runs / 393.3 MB plus 14.7 MB WAL. Lifetime physical
footprint was 1.328 GB and peak LSM state was 331.7 MB. This improved the prior
v34 run's 146.559 seconds and 2.586 GB RSS, but regressed against its 920.9 MB
physical footprint and against the accepted 96.511-second baseline, so it is a
bounded diagnostic result rather than an accepted performance win.

The guarded million follow-up was stopped when physical footprint crossed the
2.5 GB limit and reached 3.402 GB. Because of sampling/stop latency, upload had
advanced to 934,931 documents in 156.451 seconds before disconnect. The last
resource sample contained 881.1 MB LSM state, 17.5 MB table-builder state, and
17.6 MB combined derived/replay state. Disk at stop was 2.618 GB: 1.895 GB
primary runs, 488.8 MB WAL, and 234.4 MB text segments. Adaptive provisioning
had scaled `lsm.in_memory_state` to a 1.5 GB soft / 2.0 GB hard slice, so 881 MB
was still classified normal and no admission action fired. Local fairness caps
alone are therefore insufficient.

Adaptive provisioning now caps aggregate LSM state at 768 MiB on large hosts
and starts throttling at its 576 MiB soft boundary; smaller hosts still scale
below that ceiling. The raw ResourceManager default remains 512 MiB soft / 768
MiB hard when adaptive provisioning is not used. This keeps the production
decision in ResourceManager while retaining local caps only for per-backend
fairness.

The bounded follow-up proved that admission boundary but did not pass the
process-memory gate. ResourceManager held `lsm.in_memory_state` to a 601.9 MB
peak, immediately below the 604.0 MB soft boundary, and the slice subsequently
drained to 186.9 MB. Stop latency allowed 775,886 documents to upload in
159.804 seconds before the guard disconnected the client. Lifetime physical
footprint still reached 3.004 GB before falling to 771.4 MB; the final sample
had 2.399 GB RSS, 1.030 GB live malloc allocations, 387.8 MB of mmap-backed
full-text segments, and 346.4 MB of other current resource-accounted work.
Primary runs occupied 1.738 GB and primary WAL 44.6 MB on disk, but native LSM
runs are range-read rather than mmap-backed. This result rejects the memory
gate while confirming that another immutable-state cap is not the fix. The next
run must capture peak-time allocator, full-text residency, compaction/build,
and completed-job attribution at the moment footprint rises.

A coherent 650K-document attribution run completed the load and final text
synchronization in 542.4 seconds. Lifetime physical footprint peaked at 3.107
GB. At the coherent high-footprint sample, mapped text segments were only about
300 MB and the known ResourceManager LSM slices accounted for about 436 MB;
`lsm.in_memory_state` itself was 346 MB. At a separate peak-RSS sample, RSS was
3.452 GB while physical footprint was 1.163 GB and ten mmap-backed text
segments accounted for 366 MB. These samples reject the hypothesis that native
LSM run files need the text-segment mmap eviction policy: native runs are not
mmap-backed, and the failing physical-footprint peak was allocator-backed
generation lifetime rather than clean file residency.

The missing LSM owner was a second backend-wide reader-lifetime bug. Once an
immutable memtable was flushed, `retireImmutableMemtable` retained it whenever
*any* backend reader existed. The retired generation was omitted from
`lsm.in_memory_state`, and it could be freed only when the backend-wide reader
count reached zero. Continuous overlapping derived scans could therefore keep
successive allocator-backed immutable generations alive while ResourceManager
reported only active mutable/immutable state. This explains both the
multi-gigabyte accounting gap and its eventual collapse in generation-sized
steps.

The production fix pins the exact immutable generations captured by each read
or scan snapshot. A flushed generation is reclaimed when its own final reader
exits even if newer readers remain active. Probe transactions now copy point
results into transaction-owned storage instead of returning values borrowed
from mutable or immutable generations. Their temporary current-layout snapshot
can therefore release exact pins before returning, without a backend-wide
reader fallback. Normal bound reads, namespace reads, current scans, and
write-cursor snapshots continue to use exact pins. Retired immutable bytes are
now part of `lsm.in_memory_state`, so projected admission, soft throttling, and
the shared pressure epoch govern the complete allocator-backed LSM state rather
than only the active queue. New maintenance metrics report retired generation
count/entries/bytes plus exact pinned-generation and reference counts. Focused
regressions prove that an old generation is reclaimed while a newer reader
remains correct and that probe results survive later writes without retaining
their source generations. The complete LSM backend suite passes 247 tests with
one platform skip, zero failures, and zero leaks.

The first release-server validation of exact pins exposed a separate admission
lock inversion after 271,876 documents. Live counters showed 600,455,066 bytes
of aggregate `lsm.in_memory_state` against a 603,979,776-byte soft boundary;
retired immutable count/bytes, exact immutable pins, and active readers were all
zero. The incoming batch crossed soft pressure while its primary commit held
the DB apply lock. Backend admission then slept for global pressure to change,
but derived and LSM maintenance workers needed that same apply lock to publish
or flush the state that could lower global usage. Two workers spun on the lock
and consumed roughly two CPU cores while the request made no progress.

The corrected contract keeps the global budget and decision in
`ResourceManager`, while making the backend admission boundary lock-safe. Soft
pressure first performs all available local reclamation and then admits one
bounded write if only other backends still own the pressure. Hard pressure is a
non-blocking `ResourceBudgetExceeded` before WAL append. Queueing or waiting on
aggregate pressure happens above the DB apply lock through
`ResourceManager.awaitAdmission`; the backend repeats the projected check before
WAL append because the upper wait is deliberately not a reservation. A second
release validation demonstrated why both levels are necessary: without the
upper safe wait, aggregate LSM state peaked at 810,410,054 bytes against the
805,306,368-byte hard limit and the request failed near 250K documents, then
drained immediately after releasing the apply lock. Regression tests cover
work-conserving aggregate soft admission, non-blocking aggregate hard rejection,
and the safe higher-level wait contract; the complete LSM suite passes 247 tests
with one platform skip, zero failures, and zero leaks.

The next release validation passed the earlier admission stalls but exposed the
remaining probe fallback at production scale. At roughly 390K documents, two
overlapping probe transactions retained 62 already-flushed immutable
generations: 696,264 entries and 495,115,914 bytes, despite zero exact immutable
pins. RSS reached about 1.98 GB while the safe ResourceManager wait correctly
held aggregate state near its soft boundary. This was not an accounting-policy
failure; it was an ownership contract that made a short point lookup retain
every generation visible during its lifetime. Making probe results owned removes
that fallback entirely.

The fresh 650K-document release validation completed upload and full-index
synchronization in 409.261 seconds (1,588 docs/s), versus 542.442 seconds
(1,198 docs/s) in the prior coherent attribution run. Peak physical footprint
fell from 3.107 GB to 1.486 GB, peak RSS from 3.452 GB to 2.804 GB, peak live
malloc allocation from 2.884 GB to 1.909 GB, and peak `lsm.in_memory_state`
from 607.0 MB to 355.8 MB. Mean CPU utilization rose from 215% to 235% while
wall time fell 24.5%, reducing approximate server CPU time from 1,168 to 961
CPU-seconds. No ResourceManager pressure event or hard rejection occurred. At
peak RSS, retired immutable bytes, exact immutable pins, and probe readers were
all zero; current LSM state was only 37.7 MB, while mmap-backed text segments
were 755.3 MB. Final disk was 1.938 GB: 953.6 MB primary runs, 941.3 MB text
segments, and 43.2 MB primary WAL, with no obsolete or untracked LSM files.
This accepts the generation-lifetime and two-level admission fix. The remaining
RSS work is now attributable to text-segment residency and allocator retention,
not hidden LSM state.

This establishes the abstraction boundary. `ResourceManager` owns global
accounting, budgets, projected admission decisions, cross-backend fairness,
pressure actions, and the wakeable epoch used by safe waiters. The storage
backend publishes actual retained bytes and owns exact generation pins and safe
reclamation because only it knows object lifetime. The layer that owns a
higher-level critical section owns any safe wait/queue; ResourceManager cannot
assume that a low-level caller may block. Evictable caches or mappings should be
registered as separately reclaimable resource slices, allowing ResourceManager
to select cache shrink or mmap eviction as a pressure response while the owning
component performs the actual operation. Native LSM run files are range-read,
not mmap-backed, so text-segment `madvise` cannot reclaim allocator-backed LSM
generations. OS-specific mmap eviction or allocator pressure relief therefore
cannot substitute for complete accounting or correct per-generation ownership.
The full-text residency controller now follows that boundary. Every persistent
text writer publishes a conservative resident estimate to the separate
`full_text.segment_residency` slice while also exposing virtual mapped,
recently touched, cold mapped, and eviction counters in memory attribution.
Virtual mapping length is not charged after clean pages are advised cold. Query
and filter entry points mark the segment resident and maintain an active-reader
pin; the estimator is refreshed at most once per second from normal snapshot
acquisition.

`ResourceManager` returns `shrink_cache` at both limits but never invokes owner
code under its mutex. The writer responds after the decision by selecting the
coldest clean live mapping. Soft pressure preserves every segment touched in
the previous 30 seconds; hard pressure lowers that threshold to five seconds,
and active readers are never selected. Eviction is race-safe: an evicting state
prevents a concurrent touch from being overwritten after `madvise(DONTNEED)`.
The virtual mapping and snapshot pins remain intact, so a later access faults
pages back normally and restores conservative accounting. Retired mappings
retain the existing immediate advice-before-release path and therefore remain
the first reclamation tier.

Default residency limits are 512 MiB soft and 768 MiB hard. Provisioned storage
scales the hard limit to one eighth of detected memory, clamped between 256 MiB
and 2 GiB, with a 75 percent soft watermark. Multiple indexes aggregate through
the shared manager while retaining per-writer ownership of selection and
advice. Closing or replacing a writer releases its contribution. The focused
ReleaseFast resource gate covers coldest-first eviction, soft-window
hysteresis, active-reader protection, rewarming, aggregate accounting, and
accounting release; it passes 19/19 without leaks.

The validated 650K release load accepts the LSM process-memory gate. The next
production load must quantify the new text-residency counters and final RSS;
merge allocator retention remains a separately measured optimization target.

### Production admission, recovery, and mutable-snapshot lifetime follow-up

The subsequent full-server qualification hardened failure and recovery paths
before another public result was accepted. Unlocked compaction now pins every
source run path for the complete build and deletes any completed output when a
later publication step fails. Production open removes committed `.tbl` files
that are absent from the recovered manifest as interrupted-publication orphans;
one preserved failed root reclaimed 11 files / 4,297,896,878 bytes. Derived
workers treat `ResourceBudgetExceeded` as recoverable at session open, replay,
apply, publication, and persistence boundaries. They restart from the last
applied sequence with exponential backoff from 10 ms to 250 ms rather than
poisoning the runtime or losing durable work.

Public write admission now has an explicit two-level contract. Before taking
the DB apply lock, a write estimates its expanded LSM cost as two copies of
payload plus 512 bytes per operation and waits on the shared
`lsm.in_memory_state` slice. The backend still repeats the exact nonblocking
pre-WAL check. An individual request larger than the hard slice is rejected
immediately because waiting cannot make it fit. Budget rejection maps to HTTP
429 `Backpressured`, not HTTP 500, and the benchmark loader retries only that
explicit status with bounded 10--250 ms exponential delay while reporting retry
count and wait time.

A fresh 450K acceptance gate crossed the former deterministic 411,354-document
failure and completed full-index synchronization in 243.875 seconds with zero
retries. Final primary state was three active runs / 632,446,662 bytes, zero
obsolete and untracked files, and 23,019,898 bytes of WAL. Text occupied about
606 MB across 18 segments. Settled RSS was 1.845 GB, while physical footprint
was 164.4 MB and live malloc allocation 195.6 MB, confirming that most RSS was
clean file-backed residency rather than anonymous retained state.

The next fresh full-corpus attempt exposed one remaining allocator lifetime bug
at 644,596 documents. Ingestion stopped with `lsm.in_memory_state` fixed at
800,843,922 bytes against an 805,306,368-byte hard limit. A roughly 1 GB
compaction completed and L0 drained, but the accounted state did not fall, so
no future batch could be admitted. The primary run set itself was about 1.014
GB and native runs remained range-read rather than mmap-backed. Two long-lived
probe readers were active. The retained bytes were cached mutable read
snapshots: invalidating a snapshot retired it whenever *any* backend reader was
active, so unrelated probes retained every old mutable generation.

Mutable snapshots now use exact per-generation reference pins. Bound reads,
namespace reads, and current scans release the specific shared snapshot they
borrowed on every success and error path. Invalidation destroys an unpinned
generation immediately; a pinned retired generation is reclaimed when its own
last borrower exits, independently of probes or readers of other generations.
The current generation may remain cached without a pin, and the empty snapshot
requires no bookkeeping. ResourceManager accounting includes current and
retired mutable generations and drops synchronously on exact release. A
regression holds an unrelated probe open while retiring two different mutable
snapshots and proves each is reclaimed as its own reader closes. The complete
LSM backend suite passes 252 tests with one platform skip, zero failures, and
zero leaks.

The fresh 750K release gate validates the production fix. Canonical 4 MiB
requests crossed 411,354 documents in 60.554 seconds and reached 689,615 in
91.128 seconds. All 750,000 documents uploaded in 344 requests and full-index
synchronization completed in 650.189 seconds with zero backpressure retries,
zero soft-pressure events, and zero hard rejections. During a 683 MB primary
compaction, active primary runs reached 1.065 GB while
`lsm.in_memory_state` was only 5.76 MB; its lifetime peak was 79.94 MB. At one
settled compaction sample RSS fell to about 81 MB with 1.10 GB of runs still on
disk, directly demonstrating that run-file residency is reclaimable and not
heap state.

After full synchronization, the primary contained 1,099,698,324 bytes of runs,
941,169,722 bytes of physical entry payload, 35,419,886 bytes of retained WAL,
zero active readers, and 5,843,186 bytes of LSM in-memory state. Text contained
18 mmap-backed segments / 1,004,280,431 bytes with zero heap segments. The
complete root occupied 2,159,152 KiB: 1,103,444 KiB primary runs, 35,592 KiB
WAL, and 1,026,780 KiB indexes. Final RSS was 1.444 GB with 288.4 MB live malloc
allocation. This accepts mutable-generation lifetime and admission correctness;
the long final-sync wall time remains a product throughput cost to report, not
a memory-safety failure.

### Primary run geometry and replay-CPU follow-up

The next scale attempts separated primary compaction geometry from derived text
CPU. A static-level full-corpus attempt was stopped after roughly 4.636 million
documents when fixed lower-level targets scheduled an unnecessary downstream
rewrite. Level byte targets now derive from the live data scale. On a fresh
2.2-million-document gate, upload completed in about 351 seconds and primary
state settled without the former L3 cascade: one 296 MB L1 run plus eight L2
runs totaling about 3.18 GB. The run was still rejected because `full_index`
did not complete within 600 seconds.

Exact manifest bounds identified why the remaining compaction closures could
still be oversized. A persisted primary run bundled several binary key
families in the same `docs` namespace: ordinary document rows beginning with
`0x01`, ordinal/identity metadata beginning with `0x02`, and replay/internal
rows beginning with `0x03`, plus metadata rows. Run-level bounds therefore made
otherwise disjoint lower-level partitions appear to overlap. New primary runs
are now cut whenever namespace or the configured first key byte changes.
Streaming compaction applies the same boundary before appending an entry to its
output. This changes only run layout; manifest and table encodings remain v8,
so the branch opens the format on `main` and narrows legacy broad runs as normal
compaction rewrites them.

The complete LSM suite passes 255 tests with one platform skip, zero failures,
and zero leaks. A fresh 750K gate confirmed the layout under production writes:
30 early flushes produced 60 family-partitioned runs. The preserved final
primary manifest contains 20 active runs / 1,080,827,659 bytes, zero obsolete
files, and zero untracked files. Every primary run has equal smallest/largest
namespace and equal first key-family byte. The server benchmark manifest
decoder now emits each run's level, size, entry count, namespace/key bounds,
and `partition_prefix_equal`, so future gates assert this property directly
rather than inferring it from aggregate bytes.

That 750K run rejected the first replay-CPU fix. Upload reached 735,871
documents in 370.349 seconds, but the final `full_index` barrier remained active
for more than five additional minutes and was deliberately stopped. Memory was
bounded: lifetime physical footprint peaked at 778,784,056 bytes, live
`lsm.in_memory_state` peaked at 86,777,504 bytes, and the online replay-key
window peaked near 538 KB. A live profile nevertheless found a derived worker
inside one text apply while a merge worker attempted to build and publish
segments. Restarting the rejected durable root exposed an 18,989,198-byte
single replay record. The existing first-record forward-progress rule correctly
allowed it to exceed the nominal 16 MiB collection target, so a collection
item limit alone could not bound its apply latency.

The dominant CPU defect was more fundamental. Replay deliberately tombstones
upsert IDs before adding their new versions so a crash between segment publish
and applied-sequence persistence remains idempotent. `deleteById`, however,
scanned every stored document in every active segment for each ID. An 8K replay
batch against a 750K-document index therefore performed approximately 8K full
index scans before tokenization. Deletion now builds one borrowed ID hash set,
traverses each segment once, updates one deletion bitmap per affected segment,
and commits all affected bitmaps in one storage transaction. Failure rolls the
in-memory bitmaps back. The format and replay semantics are unchanged. The
storage/index suite passes 659 tests with one platform skip, including
multi-segment duplicates, duplicate requested IDs, atomic rollback, reopen, and
the new batched path.

Replaying the exact rejected root with that binary applied 157 pending replay
entries in two windows with 76.694 seconds of measured apply time, instead of
remaining inside one apply for several minutes. Phase metrics put individual
4K-document text builds at roughly 55--208 ms depending on document length;
analysis/stemming and inverted construction are now the visible CPU work, not
repeated stored-document scans.

Oversized durable records are additionally subchunked at the apply boundary.
Explicit deletes and overwrites run first, followed by bounded document slices;
the managed-index apply lock is released between slices. The durable sequence
is persisted only after every slice succeeds. A crash during the record may
redo completed slices, but the same tombstone-before-add rule makes that replay
safe. A focused regression proves that one five-document record with a
two-document limit invokes three applies while exposing one scanned/applied
record and advances the same sequence only after all three calls. Collection
memory remains governed by `ResourceManager`; apply CPU and lock hold time are
now independently bounded.

The subsequent fresh ReleaseFast 750K gate accepted both changes. Upload plus
the `full_index` barrier completed in 124.784 seconds at 6,010 documents/second
with zero backpressure retries, 5.21x faster than the earlier accepted
650.189-second gate. It reached 735,871 documents in 108.411 seconds versus
370.349 seconds in the rejected per-ID-delete run; the remaining documents and
full synchronization added about 16.4 seconds.

Lifetime physical footprint peaked at 660,637,664 bytes. Attributed peaks were
300,045,666 bytes for `lsm.in_memory_state`, 100,663,296 bytes for
`lsm.compaction_work`, 537,818 bytes for `derived.replay_window`, 73,721,592
bytes for `full_text.build_working_set`, and 1,016,156,206 mmap-resident bytes
for `full_text.segment_residency`. The mmap residency is file-backed and
resource-managed rather than anonymous heap. The text index merged from 29
segments / 1,016,156,206 bytes to 10 segments / 990,867,656 bytes. Primary LSM
state settled within about 30 seconds from 107 active runs to 13 and later 11,
with no active compaction left at measurement time.

The settled pre-restart root occupied 2,107,602,361 bytes: 1,097,032,782 bytes
in 11 primary runs, 19,574,183 bytes of primary WAL, 990,867,656 bytes in 10
text segments, and 64,660 bytes in index runs. All 11 primary runs obeyed the
namespace/key-family partition boundary, with zero obsolete or untracked
files. A graceful restart retained the exact 2,700-hit `alpha` result. A forced
process-loss restart then recovered an exact 750,000-document `match_all` and
the same exact 2,700-hit `alpha` result. Recovery checkpointed the WAL to 56
bytes and flushed mutable state, leaving 15 partition-conforming primary runs,
zero obsolete files, and zero untracked files.

This accepts the 750K production gate for batched deletion, record-internal
subchunking, family-partitioned primary runs, bounded memory, disk accounting,
and both graceful and crash recovery.

The first fresh 2.2M attempt then exposed an independent accounting CPU bug.
`StreamingEncoder.workingSetBytes()` was invoked after each appended LSM table
entry, but recomputed completed-block-owned heap by traversing every accumulated
block. A live sample at 483,455 documents put 1,187 of 2,088 samples in that
accounting function. Thus a production `ResourceManager` observation had made
large flush and compaction builders quadratic. The encoder now increments the
completed-block heap subtotal exactly when it publishes a block, making each
observation O(1) without weakening or sampling resource accounting. The full
LSM suite still passes 255 tests with one platform skip, zero failures, and zero
leaks. A verification sample at the same scale contained no
`observeBuilderWorkingSet` hot-stack entries.

The clean post-fix 2.2M gate completed upload plus `full_index` in 357.528
seconds at 6,153 documents/second with zero backpressure. The timed run included
a diagnostic stack sample; its progress had reached 2,155,897 documents in
306.145 seconds before the final batches and synchronization. This is a strict
improvement over the earlier roughly 351-second upload whose final synchronization
then remained incomplete for more than 600 additional seconds.

Before restart, lifetime physical footprint peaked at 1,437,079,384 bytes.
Attributed peaks included 373,466,878 bytes of `lsm.in_memory_state`,
215,536,079 bytes of `lsm.table_builder_working_set`, 100,663,296 bytes of
`lsm.compaction_work`, 546,083 bytes of `derived.replay_window`, 75,697,562
bytes of `full_text.build_working_set`, 1,623,217,985 bytes of evictable
file-backed `full_text.segment_residency`, and 246,270,052 bytes of text-merge
buffers. The settled production-tier segment state occupied 6,103,342,883
bytes: 3,172,168,491 bytes in 15 primary runs, 39,091,202 bytes of primary WAL,
2,891,854,257 bytes in 65 text segments, and 171,970 bytes in text-index runs.
All primary runs obeyed the namespace/key-family partition, with zero obsolete
or untracked files.

Exact correctness was 2,200,000 `match_all` hits and 7,875 `alpha` hits before
restart and after both graceful and forced process-loss restart. Crash recovery
checkpointed the primary WAL to 56 bytes and flushed mutable state, leaving a
6,073,791,604-byte root with 19 partition-conforming primary runs and still
zero obsolete or untracked files. Recovery's process physical-footprint peak
was 2,632,374,032 bytes and settled to 790,319,616 bytes; the full-corpus gate
must preserve this restart measurement rather than reporting ingest memory
alone.

This accepts the 2.2M production gate. The next scale validation is the full
5.03M product gate, followed by the planned concurrency/freshness matrix. No
Quickwit corpus or comparator recollection is required; its archived
authoritative result remains the comparison baseline.

### Full 5.03M product-gate diagnosis (2026-07-15)

The first complete product load was correct (`5,032,105` match-all hits and
`15,818` hits for `alpha`) but was not acceptable as a performance result. It
exposed three production-path costs that the kernel benchmark deliberately
does not contain:

1. The benchmark table had been created with schema-less dynamic indexing.
   String inference indexed both analyzed `body` text and a `body.exact`
   keyword for values up to 1 KiB. Most corpus passages qualify, so the product
   index contained an additional mostly-unique whole-passage term stream. The
   resulting v0 text index settled near 6.10 GB even though the equivalent v38
   kernel index was 3.386 GB. This was a schema-equivalence failure, not an
   unexplained format regression. The server fixture now declares a strict
   schema containing only numeric `corpus_ordinal` and analyzed `body`, with no
   `_all`, stored source, or inferred exact subfield.
2. Schema backfill bypassed the bounded file-backed production segment sink and
   flushed every 1,024 documents. An interrupted 2.55M-document migration had
   created 2,855 files and roughly 4.4 GB in `full_text_index_v1`, leaving
   extreme merge debt. Backfill now batches by the normal 8 MiB source budget
   (with a 65,536-document ceiling), writes through the ResourceManager-aware
   final-file sink, and applies the normal tiered merge policy at every durable
   checkpoint. The asynchronous merge scheduler is gated during the bulk build
   so it cannot compact the generation being replaced or compete for the merge
   working-set budget. A forced one-document-per-checkpoint regression test
   finishes at or below the production tier limit with exact search results.
3. Operational `DB.stats()` contradicted its bounded-status contract by calling
   `scanPrimaryDocCount()` unconditionally. Metadata schema-progress polling
   therefore scanned and Snappy-decoded the entire 6.65 GB primary LSM every
   round, holding one core continuously and driving transient RSS/footprint.
   The durable document-identity counter is now the O(1) normal source; the
   primary scan is lazy and is used only for the legacy incomplete-identity
   sparse-index fallback. A second sample found that the same status call
   derived full-text term counts with `layoutStatsWithInvertedDetails()`, which
   iterated every term and decoded every posting in every segment each round.
   Segment open now caches an exact term count by summing the entry-count
   headers of 25-48-term dictionary blocks; operational status only sums those
   cached values. Full primary scans and detailed postings attribution remain
   available through diagnostic stats.

The HTTP listener itself remained healthy during the reproduced restart; both
the health and public endpoints responded. Earlier loopback refusals came from
the command sandbox, while request delay came from the synchronous structural
schema/rebuild path and the pathological status scans above. Treat availability
of the listener, table-generation admission, and migration completion as three
separate signals in future server runs. The optimized v2 rebuild reconfirmed
this explicitly: a live process sample showed one OS thread blocked in the
health server's `accept` and a separate thread blocked in
`httpx.Server.listen`'s `accept`, while a sandboxed curl reported connection
refused. The same curl from the host network namespace immediately returned
`{"status":"ok"}`. A per-command sandbox reachability failure is not evidence
that either event loop or listener has exited.

A later thread sample isolated an additional public-status stall: `GET /tables`
and `GET /tables/:name` called a field named
`bestEffortSingleTableStorageStatus`, but after reading the published runtime
snapshot it opened a direct LSM status read. Normal read admission waits while
schema backfill owns structural/group activity, so an optional response field
blocked an HTTP worker indefinitely even though the listener and health server
were live. Table status now uses the already-published runtime-status LSM
snapshot only. It never acquires a normal table-read lease for observability;
temporarily stale or absent optional LSM detail is preferable to making catalog
status unavailable during maintenance. ReleaseFast validation against the
preserved 5.03M-document corpus confirmed the distinction under active v1
backfill: the process was using about 146% CPU, `/healthz` remained healthy,
and `GET /db/v1/tables/antfly-benchmark` returned HTTP 200 in 0.8 ms with
`migration.state = rebuilding`. The same run exposed that an explicit
`startup_catch_up/opening` placeholder carried zero counters, which the cached
table-status path initially rendered as `empty=true`. A nonzero cached count
can prove non-emptiness even when stale, but only a fresh zero can prove an
empty table. The endpoint now omits optional storage status for an
opening/catching-up zero snapshot instead of blocking or publishing a false
empty state.

The same distinction applies to schema-progress reconciliation. An explicit
runtime report with `startup_catch_up/opening` means the live shard owner has
been observed but is not ready yet; it does not mean runtime status is
unavailable. Both metadata service variants previously treated an empty set of
*ready* schema-progress records as absence of any runtime observation and fell
back to opening the shard DB from the filesystem. Runtime coverage is now
checked separately from readiness where projected observations exist. A later
sample proved that this was necessary but not sufficient: standalone's
synchronous provisioner does not publish a projected runtime observation while
it is inside the backfill. Its lifecycle still entered
`collectLocalSchemaProgressWithOptions` once per second, reopened the 6.65 GB
primary state and retained v1 full-text generation, and spent about 250-285 ms
per poll. The stack was metadata lifecycle -> local schema progress ->
`DB.open(query_readonly)`; no HTTP listener or event-loop work was involved.

Schema progress now consults the target generation's durable `rebuild.state`
before any DB open. Presence is an authoritative not-ready result, so the
normal multi-minute migration performs only a tiny marker read per lifecycle
round. Once the marker is atomically removed, reconciliation performs one
`status_only` catalog verification rather than loading or mapping query
segments. A regression creates only the v2 marker with no DB beneath it and
requires the probe to return not-ready; any attempted DB open therefore fails
the test. The lifecycle store-status paths also use the registered local data
runtime provider, and their provider-less fallback is catalog-only. Together
these rules remove recurring CPU, allocation, mmap, and I/O pressure without
conflating schema progress with HTTP availability.

The completed v1 migration was logically correct (`5,032,105` documents and
`15,818` exact `alpha` matches), but its 10 segment files occupied exactly
`6,054,355,534` bytes and are not benchmark-valid. Segment footer inspection
proved that v1 contains `body.keyword` and `_all` in addition to `body`,
`corpus_ordinal`, and the native ordinal sidecar. The metadata provisioner had
opened the configured target generation synchronously and only applied the
table schema after `DB.open` returned. An interrupted generation therefore
resumed and completed under the old schema-less mapper before the authoritative
schema could be installed.

Provisioned DB opens now accept an owned-for-the-call
`schema_before_index_load` option. `DB.open` persists that schema after core
initialization but before configured indexes are loaded or resumed, and the
metadata provisioner parses and supplies the authoritative target schema before
calling it. A regression creates an incomplete schema-less full-text generation,
reopens it through the provisioning option, writes a document, and proves that
`body` has one posting while `body.keyword` and `_all` have none. The focused DB
test passes 11/11 and the metadata test target compiles and passes 10/10. Because
schema versions are immutable in production, the invalid v1 files are retained
as the read generation while the corrected fixture creates
`full_text_index_v2`; no branch-only format or schema is rewritten in place.
The migration API derives versions by comparing `document_schemas` and ignores
a caller-supplied version. The v2 fixture therefore reverses the order of the
two names in the JSON Schema `required` set, a validation- and indexing-neutral
change that legitimately creates the next immutable generation.

After a clean restart, the first exact-count query exposed a second ownership
problem: it filled the query-readonly cache instead of establishing the normal
data runtime as the group owner. Opening v1 took 1.995 seconds and the complete
DB open took 2.275 seconds; the immediately repeated request then completed in
7.1 ms. Full search now uses the same lifetime-pinned resident DB lease as
primary lookups. The lease is matched by group, table, identity namespace, and
visible LSM root generation, and holds read activity so structural maintenance
cannot retire the DB during the query. On a cold normal data runtime, the lease
coalesces the open into the writer cache and then searches that DB. Only an
explicit query-only source with no writer cache falls back to a query-readonly
open. This avoids duplicate multi-gigabyte mappings and cold-start memory/I/O
amplification on data servers while retaining query-only availability.

The preserved corpus also reports 6.655 GB across 30 active primary LSM runs.
This is not retained-generation or WAL amplification in this run: the manifest
has no obsolete or untracked runs, and the WAL is only about 12 KiB. The active
table files contain 9.837 GB of logical entry payload compressed to 5.621 GB,
plus 1.033 GB of table metadata/non-entry bytes. The earlier observation of
roughly 12 GiB of compaction generations and 1.9 GiB of WAL therefore describes
a different ingestion state and must not be used to explain the preserved
corpus.

Two table-format costs in that 1.033 GB are now fixed without changing the v9
wire version or dropping compatibility with runs written by `origin/main`:

- Every block encoded a 50%-load open-addressed exact-key hash table, requiring
  at least eight bytes per entry (at least 241.6 MB for 30.20M entries, and more
  after per-block power-of-two rounding). No read path consulted those slots;
  exact lookup already binary-searches the block entry offsets. New v9 writers
  encode the existing hash section with zero slots. The compatibility reader
  validates and skips old v9 slot-array framing without allocating it, avoiding
  hundreds of MiB of dead heap while opening `origin/main` runs.
- Global and per-block prefix Bloom filters were sized by entry count even when
  binary identity keys had no extractable prefix, or millions of document keys
  shared the same `doc:` prefix. New writers collect one hash pair per distinct
  consecutive prefix and size the filter from that count. This preserves Bloom
  membership semantics and the encoded section layout while eliminating the
  entry-proportional disk and builder-memory cost.

The next writer revision addresses the separate fixed-width offset cost. LSM
table v10 stores a `u16` block-local entry offset instead of a `u32` table-global
offset. Logical blocks are capped at 32 KiB, while an oversized entry is a
singleton at local offset zero, so the representation is exact. Footer-only
readers dispatch from an explicit metadata marker and full readers accept both
the shipped v9 layout and v10. Readers expand once to the existing global
`u32` lookup array, preserving point/range CPU behavior; streaming builders
retain the packed form. Across the preserved 30,199,943 entries and 30 runs,
this projects a net 60,399,646-byte disk reduction after the eight-byte marker
per run, and halves
the offset portion of table-builder memory. A generated origin/main v9 fixture
proves full-file, footer-only, sequential, and exact-lookup compatibility.

The remaining 5.621 GB compressed entry payload includes product source records
and durable document-identity metadata that the kernel comparator intentionally
does not store. Product comparisons must normalize requested/stored payloads
before attributing this entire difference to the search index. Remaining LSM
format work should separately quantify the 4-byte entry-offset array (about
120.8 MB at this corpus size) and the value/identity payload; it must not remove
metadata that is actually used merely to improve a benchmark number.

On macOS, do not equate `ps` RSS with unreclaimable heap for this workload. In
the reproduced interrupted migration, RSS was 4.88-7.48 GB while physical
footprint was 945 MB (1.56 GB peak); clean file-backed full-text pages accounted
for most of the difference. ResourceManager segment-residency accounting had
already fallen from a 6.09 GB peak to 1.28 GB, below its 2 GiB hard limit, and
the segment mmap eviction path issues clean-page discard advice. Report RSS,
physical footprint, malloc live bytes, mmap logical bytes, and ResourceManager
residency together rather than treating any one of them as heap usage.

The corrected v2 rebuild then exposed a separate CPU degeneracy at roughly 66%
of the corpus. Physical footprint remained near 1 GiB, but a four-minute sample
placed about 92% of worker samples in Snappy decode under
`mergeTypedDocValuesSections`. The append merge iterated every source document
and used the point-lookup accessor for its typed doc value. That accessor starts
at chunk zero, so document `n` repeatedly decompressed and scanned all preceding
chunks. Merge work was therefore quadratic in chunk count even for the dense
corpus-ordinal column.

Append merges now use an owning decoded-chunk handle and a validated sequential
iterator. Each compressed chunk is decoded exactly once, sparse values retain
their source document IDs, deletions are skipped, and output IDs are remapped by
the deletion rank before the next segment's live-document base is applied. Byte
values are borrowed only for the duration of the decoded chunk; the existing
writer takes its own copy. Focused tests cover sparse numeric values spanning
multiple chunks, borrowed byte values, byte-column append merge, and
multi-segment deletion remapping. The durable rebuild checkpoint was preserved
so the replacement binary can resume at the same point and directly measure the
removed stall. The ReleaseFast replacement crossed that checkpoint and emitted
58 segment batches in 87 seconds instead of going silent for more than four
minutes. A three-second post-fix sample found only 26 top-of-stack samples in
generic Snappy decode and no `mergeTypedDocValuesSections` entry in the
top-of-stack table; the pre-fix sample placed roughly 92% of worker samples in
Snappy decode below that merge. During the confirmation interval physical
footprint was 0.88--0.91 GiB with a 1.00 GiB peak. The diagnostic process was
then restarted with memory attribution every 64 batches rather than every batch
so repeatedly walking the multi-gigabyte v1 layout does not dominate the
remaining rebuild wall time.

### Analyzer-equivalence correction and online migration admission (2026-07-16)

The completed `full_text_index_v2` rebuild was operationally healthy but is not
an acceptable Tantivy/Quickwit comparison. It contained all 5,032,105
documents in 10 segments and occupied 2,602,844,009 bytes, but the benchmark
fixture had omitted `x-antfly-analyzer`. Antfly therefore inherited its
production `standard` pipeline: Unicode words, ASCII lowercase, English stop
words, and Porter2 stemming. The embedded Tantivy comparator uses the Antfly
simple contract with no stop-word removal or stemming, while the archived
Quickwit 0.8.2 run used Tantivy's built-in simple tokenizer, 255-byte length
filter, and lowercase filter. The apparent size win was reduced semantic work,
not a storage-format win, and must not be published as such.

Detailed v2 section accounting reconciles 2,602,843,609 of the 2,602,844,009
segment bytes, leaving only 400 framing bytes unattributed. Stored fields use
177,122,512 bytes; the inverted `body` field uses 2,366,293,153 bytes, including
71,707,015 dictionary bytes, 22,020,296 Bloom bytes, 2,267,533,357 postings
bytes, and 1,158,722,584 position bytes; typed values use 39,298,394 bytes;
ordinals use 20,128,470 bytes; and section indexes use 1,080 bytes. The analyzer
produced 10,503,292 posting terms, versus roughly 12.1 million in the valid v38
kernel artifact. This directly explains why v2 cannot answer the remaining
index-size question.

The server fixture now declares `simple` in the initial schema and the v2
migration fixture. Existing preserved generations are not rewritten. A
corrective `schema-v3-simple.json` creates a new immutable generation for the
diagnostic corpus, retains v2 as the read generation until readiness, and will
provide the honest product-size/CPU/RSS result. Antfly simple and the embedded
kernel contract both use ASCII lowercase with no stop words or stemming. The
server comparison records Quickwit's remaining Unicode-lowercase/length-filter
boundary explicitly instead of silently attributing analyzer differences to
the format.

Starting that corrective migration exposed a distinct availability defect.
`PUT /schema` committed metadata and then synchronously applied and drained the
corpus-sized rebuild on an HTTP worker. The listener and health server remained
healthy, but table status and normal read admission waited behind structural
and group activity. Provisioned schema updates now follow the already-existing
index-create production pattern: after exact metadata projection they enqueue
structural reconciliation and return; embedded/direct DB sources without a
background owner retain their synchronous contract. Structural reconciliation
uses a dedicated read-compatible group-operation class because it mutates only
the unpublished target index. Queries routed to the retained read-schema
generation and immutable runtime-status snapshots remain admitted, while
ordinary writes, restore, drop, and root-generation transitions retain the
existing exclusive read fence. Focused admission coverage distinguishes the
two operation classes. Reconciliation retains the last published runtime-status
snapshot instead of invalidating it and forcing a status-only DB reopen. During
the brief stale-writer retirement/new-writer open handoff, resident-DB leasing
returns control to the generation-pinned readonly query cache rather than
racing to create a second writer owner; once the replacement writer is
resident, reads lease it normally. Full HTTP validation is required against the
corrective generation before accepting the product gate.

The preserved accepted v38 kernel artifact provides the corrective semantic
oracle: exact `alpha` count is 15,753 and exact phrase `alpha beta` count is
525. The v2 `standard` generation returned 15,818 and 909 respectively because
stemming changes both term membership and phrase candidates. Promotion of v3
is accepted only if its full count is 5,032,105 and these simple-analyzer counts
match exactly.

A live v3 sample also found repeated LSM observability work rather than index
construction at the top of one core: `maintenanceScoreLocked` called the
quadratic `largestL0OverlapRunCount` on every runtime-status/ResourceManager
sample even though the immutable L0 run set had not changed. The backend now
caches that exact overlap result for the normal bounded-L0 case. Cache hits
compare the complete ordered run-ID sequence and configured threshold, so a
same-length publication cannot reuse stale scheduling state; oversized L0 sets
fall back to the uncached exact calculation. Actual compaction-plan selection
remains exact and unchanged. Focused coverage changes both an ID and the L0 run
count and requires the cached result to refresh.

The same v3 profile accounted for the large gap between segment-construction
CPU and end-to-end migration wall time. The backfill cursor performed a sparse
identity-map point read for every source document. On the production LSM this
repeatedly acquired the backend lock and searched the same immutable run set;
the sampled cursor path was dominated by `lookupOrdinalTxn` and LSM point-read
lock waits. Backfill now collects document IDs with each byte-bounded page and
resolves their ordinals through the existing sorted `getManySorted` path. The
repair/read-transaction variant performs that batch lookup against its pinned
transaction, preserving generation consistency; the current-state variant
uses the normal projection batch lookup. Stable document ordinals and index
semantics are unchanged.

Instrumentation also showed exactly one `source_docs=1` segment build after
almost every normal 8 MiB page. The scanner appended the document that crossed
the byte target and the segment splitter consequently emitted a normal segment
plus a one-document tail. The cursor now stops before consuming that crossing
key, resumes from the preceding key on the next page, and permits a single
oversized document only when the page is otherwise empty. This removes roughly
half the segment publish operations in the observed rebuild without dropping
or duplicating the boundary document. Finally, an unpublished rebuild may now
accumulate at most one bounded extra merge-policy tier before synchronous
compaction, instead of checking and settling policy after every page;
completion still performs a policy-settling merge before clearing the durable
rebuild marker and publishing the generation. The interrupted-resume and
final-fanout tests remain the correctness gates for these changes.

The corrective v3 generation completed on the preserved full corpus and is the
first valid product-format size result. It indexed exactly 5,032,105 documents,
promoted 10 segments, and passed the three semantic gates: 5,032,105
`match_all` hits, 15,753 `alpha` hits, and 525 `alpha beta` phrase hits. The old
binary took approximately 37 minutes 54 seconds from schema request to
promotion; that timing includes the per-document ordinal reads, 1,054 observed
one-document tail builds, and eager page-boundary merge behavior removed above,
so it is a diagnostic baseline rather than the post-fix indexing result.

The 10 v3 segment files occupy 3,599,478,976 logical bytes, of which all but 400
bytes are attributed: 177,122,512 stored-field bytes, 3,362,929,083 inverted
bytes, 39,297,431 typed-value bytes, 20,128,470 ordinal bytes, and 1,080 section
index bytes. The inverted section contains 79,922,010 dictionary bytes,
20,512,968 Bloom bytes, and 3,257,461,620 postings bytes, including
1,847,106,891 position bytes. Removing the product-only stored and typed
sections leaves 3,383,059,033 bytes. That is 2.76 MB smaller than the accepted
3,385,816,859-byte v38 Antfly kernel artifact and 131,564,890 bytes (4.05%)
larger than the 3,251,494,143-byte Tantivy kernel artifact. The honest remaining
kernel-format gap is therefore about 132 MB, not the former 1.3--1.5 GB.

The published Quickwit split inventory remains approximately 3.138 GB, but its
archived profile stores the ordinal as a fast field and disables source storage;
the Antfly product artifact additionally carries 216.4 MB of stored/typed
product payload. Public reporting must show both the complete 3.599 GB product
footprint and the 3.383 GB index-only normalization, rather than comparing one
against a source-disabled server and calling the full difference postings
overhead. No Quickwit rerun is needed because the archived result already
contains the full persistent-HTTP load matrix, mixed writes, freshness, CPU,
RSS, disk, and recovery measurements.

The reusable `tools/verify_antfly_schema_availability.py` gate drives a schema
PUT concurrently with health, table-status, and exact term queries until the
target generation is solely published. On a 50,000-document fixture, the first
run exposed one five-second status timeout during the metadata structural
handoff even though all 973 search polls succeeded. Status admission therefore
now serves the immutable snapshot during both metadata structural activity and
background reconciliation; only restore preparation retains its status fence.
The corrected v1-to-v2 run observed migration and promotion with zero health,
status, or query errors. All three status samples stayed below 0.9 ms. The
schema response and first cold query both took approximately 2.01 seconds,
which identified one final synchronous coupling: the HTTP handler drove up to
three metadata rounds after successfully enqueueing reconciliation. A
provisioned background owner now skips those rounds and returns after enqueue;
embedded/direct sources retain their synchronous update and round-driving
contract. The final v2-to-v3 availability acceptance also completed with zero
health, status, or query failures. Health peaked at 6.1 ms, table status at
0.93 ms, the schema PUT at 3.78 seconds, and the first cold query at 3.80
seconds. Removing the explicit post-enqueue rounds did not reduce the local
schema response, locating the remaining delay in committed metadata projection
and the cold owner handoff rather than the listener or event loop. Returning a
subsecond asynchronous schema job would be a separate API-contract change;
the current PUT continues to guarantee metadata projection before success.

The first full-corpus persistent-HTTP concurrency run then exposed a separate
native lifetime bug. Concurrency 1, 2, and 4 completed without errors, but the
process crashed 3.5 seconds into the 8-client/200-RPS mixed read/write point.
The macOS crash report recorded `EXC_BAD_ACCESS` at address `0x15` while
`IndexSnapshot.termDocFreq` grew its hash-map cache. The public accept loop was
normally blocked in `accept`; a worker's SIGSEGV terminated the whole process.
Full-text projection can publish a replacement snapshot independently of a
query, but the query executor had borrowed the old pointer without retaining
it. Production text searches, structured-filter executions, and distributed
term-stat collection now acquire and release a ref-counted snapshot for their
complete operation. The failed run is diagnostic only; the server comparison
is accepted only after the same mixed-load point and then the complete matrix,
freshness, and recovery gates pass with the rebuilt binary.

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

Initial thresholds should be chosen only after Milestone 3 establishes stable
variance on the target machines.

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

## Decision Points

Resolve these during Milestone 0 rather than implicitly in code:

1. Whether the external `search-benchmark-game` protocol can be extended with a
   verification command or needs a companion verifier executable.
2. Whether stable corpus ordinals belong in an existing ordinal section or a
   dedicated benchmark-visible native doc-value field.
3. The exact analyzer configuration both engines can implement identically.
4. Phrase scoring semantics and whether phrase frequency contributes to BM25.
5. Whether the primary top-k benchmark requires exact totals or permits `gte`.
6. The production segment policy used for the cross-engine comparison.
7. The hardware class and noise controls for regression gating.
8. Which durability profiles are meaningfully supported by every server
   comparator.

## Definition of Done

This plan is complete when:

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
