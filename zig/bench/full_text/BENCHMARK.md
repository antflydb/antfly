# Full-Text Benchmark Protocol

Design: [FULL_TEXT.md](../../FULL_TEXT.md)

## Purpose

This document defines how to make Antfly Zig full-text performance work
measurable, comparable, and implementable. It covers two deliberately separate
benchmark products:

1. an embedded search-kernel comparison against Tantivy, used for engineering
   and regression work; and
2. a database/server comparison, used to evaluate the public Antfly product
   under realistic transport, concurrency, write, durability, and recovery
   conditions.

The engine work that closes the search-kernel gap is described in
[FULL_TEXT.md](../../FULL_TEXT.md) under Search Execution Design.
Benchmark credibility comes before performance claims: a timing is not accepted
unless the compared engines demonstrably executed equivalent queries over the
same corpus and produced equivalent results.

This document complements [FULL_TEXT.md](../../FULL_TEXT.md). `FULL_TEXT.md` owns the
full-text design: visibility, maintenance, field layout, product semantics, and
search execution architecture. This document owns benchmark methodology: the Tantivy
kernel-comparison contract, the database/server-comparison contract, and the
correctness gates that must pass before any timing is accepted.

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
established (see [Search Execution Architecture](../../FULL_TEXT.md#search-execution-architecture)); it
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

## Resolved benchmark gaps

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
