# Zig tokenizer implementation and performance

This document records the native tokenizer architecture, its correctness
contract, reproducible benchmarks, and performance experiments. Update it when
changing `src/hf_tokenizer.zig`, the tokenizer interface, or tokenizer
benchmarks.

## Scope

The native implementation supports Hugging Face `tokenizer.json` models using:

- WordPiece
- BPE, including GPT-2 ByteLevel, CLIP-style suffix BPE, and metaspace variants
- Unigram
- added and special tokens
- model wrapping, padding, generation encoding, offsets, and decoding

The performance work below currently targets GPT-2 ByteLevel BPE. WordPiece and
Unigram share infrastructure but do not allocate the BPE pretoken cache.

## Correctness contract

Optimization must not change token IDs. Performance results are accepted only
after checking the complete output sequence, not merely its length.

The primary benchmark fixture is:

- tokenizer: `openai-community/gpt2` `tokenizer.json`
- corpus: Project Gutenberg's *Pride and Prejudice*, 738,046 input bytes
- expected token count: 191,673
- expected token-sequence FNV hash: `36c4edb81523489c`

This output matches Hugging Face `tokenizers` and Gigatoken for the fixture.
Focused tests also cover GPT-2 contractions, leading spaces, digit runs,
multi-newline behavior, curly quotes, and non-ASCII letters.

## Reproducible benchmark

The benchmark is built in `ReleaseFast` and keeps the tokenizer and output
buffers alive across iterations:

```sh
cd zig
zig build bench-tokenizer -- /path/to/tokenizer.json /path/to/corpus.txt \
  --warmup 2 --iterations 100 --threads 1
```

Use `--warmup 0 --iterations 1` for a cold first pass. `--threads N` runs
concurrent `std.Io` tasks against the same tokenizer and cache. The benchmark
reports the token count and sequence hash so invalid performance results are
visible. `--internal-threads N` emits up to N chunk tasks for one sufficiently
large ByteLevel document and gathers the results in order. `--repeat N` repeats
the corpus in memory before timing, which is useful for measuring internal
parallelism without changing the fixture. `--profile-bpe` enables atomic
cache-hit counters after warmup and reports key-length and result-size
histograms. Profiling is for attribution rather than throughput measurement
because the counters intentionally add work to the hot path. No benchmark or
tokenizer path creates an OS thread directly.

## Baseline and current results

Measured on an Apple M4 Max. Throughput is decimal MB/s.

| Implementation | Mode | Throughput |
|---|---|---:|
| Original Zig implementation | steady, 1 thread | 17.75 MB/s |
| Packed merges + initial cache | steady, 1 thread | 43.57 MB/s |
| Streaming ByteLevel pretokens | steady, 1 thread | 98.34 MB/s |
| Added-token scan fast path | steady, 1 thread | 114.62 MB/s |
| Lock-free open-address cache | steady, 1 thread | 121.62 MB/s |
| Raw-byte vocab + ASCII vector scanner | cold, 1 task | 101.04 MB/s |
| Current | steady, 1 task | 294–295 MB/s |
| Current | steady, 14 concurrent `std.Io` tasks | 3.03–3.09 GB/s |
| Current, 738 KiB corpus | cold, 14 internal tasks | 200.65 MB/s |
| Current, 738 KiB corpus | steady, 14 internal tasks | 1.614 GB/s |
| Current, 11.8 MB repeated corpus | cold, 14 internal tasks | 1.204 GB/s |
| Current, 11.8 MB repeated corpus | steady, 14 internal tasks | 1.847 GB/s |

The current implementation is approximately 16.6 times faster than the
original single-thread steady-state implementation while also correcting the
original ByteLevel boundary behavior.

Gigatoken's published large-corpus M4 Max result is 8.79 GB/s. The measurements
are not directly interchangeable: this benchmark includes complete BPE token
ID generation and hashes the full output, while Gigatoken's headline workload
differs. Moving dispatch to the application's persistent `std.Io` runtime
removes per-call OS thread creation. Reusing the tokenizer's task workspaces
then removes repeated chunk-output allocation and materially helps both the
738 KiB and 11.8 MB internally parallel workloads. The remaining gap is
principally in cache hashing, output copying, and BPE work on cold or uncommon
pretokens. See Gigatoken's
[design document](https://github.com/marcelroed/gigatoken/blob/main/design_doc.md)
and
[pretokenizer optimization log](https://github.com/marcelroed/gigatoken/blob/main/pretokenizer_optimization_log.md).

## Current BPE data path

1. Normalization and added-token segmentation.
2. A 64-byte ASCII vector scan, falling back to a scalar exact-Unicode scanner.
3. Direct-address vocabulary lookup for one- and two-byte ByteLevel pretokens.
4. Pretoken-cache lookup on longer borrowed raw input slices using one Wyhash
   and open addressing.
5. On a miss, BPE merge candidates use packed `(left_id, right_id)` keys.
6. The final token IDs are published to the cache and appended to the caller's
   reusable output buffer. Single-ID hits use a specialized append path.

ByteLevel vocabulary and merge pieces are decoded from GPT-2's byte-to-Unicode
alphabet once while loading `tokenizer.json`. The hot encoder therefore uses
raw input bytes directly, while `id_to_token` retains the original display
strings for decoding.

The pretoken cache has 64 shards and 2,048 slots per shard. Reads are lock-free;
only insertion takes a shard lock. Each shard stops inserting at 75 percent load
to preserve bounded probe lengths, for a maximum of 98,304 cached pretokens.
Entries are immutable after publication and are freed when the tokenizer is
deinitialized.

## Accepted optimizations

### Packed integer merge lookup

Merge parsing builds a map keyed by two vocabulary IDs packed into a `u64`.
The merger carries each live symbol's token ID, avoiding repeated construction
and hashing of `"left right"` strings. The original string map remains a
compatibility fallback for unusual merge tables.

### Persistent pretoken cache

Natural-language pretokens repeat heavily. Caching their final token IDs avoids
symbol-list construction and priority-queue BPE work on hits. The cache is
bounded, concurrency-safe, and allocated only for BPE tokenizers.

### Streaming ByteLevel pretokenization

The old implementation allocated every encoded pretoken and an outer slice
before BPE began. The current scanner finds boundaries in place and immediately
performs cache lookups on borrowed input slices.

### Added-token root-byte filter

GPT-2 has an added special token even when normal text contains none. A
root-byte bitmap, and a scalar search when all added tokens share one initial
byte, avoids a trie hash lookup at every input byte.

### Raw-byte ByteLevel vocabulary

GPT-2's JSON represents every byte as a Unicode codepoint. Decoding vocabulary
and merge pieces during tokenizer construction removes ByteLevel conversion
from every pretoken, shortens cache keys, and lets BPE symbols reference the
original input bytes.

### ASCII vector pretoken scanner

The hot scanner classifies 64 bytes at a time with Zig vectors and derives
letter, number, whitespace, punctuation, and contraction boundaries as bit
masks. A batch containing non-ASCII data or an unsafe edge falls back to the
scalar scanner. A dedicated test compares all vectorized boundaries with the
scalar implementation over a long mixed ASCII sample.

### Exact compact Unicode classes

The scalar fallback uses generated Unicode 16.0.0 General Category and
White_Space data. Four 2-bit classes are packed per byte, and identical
256-codepoint pages are deduplicated; the resulting lookup data is about
16 KiB. It distinguishes letters, numbers, whitespace, and other characters
without broad block heuristics.

Regenerate `src/unicode_classes.zig` from official Unicode data with:

```sh
python3 lib/tokenizer/tools/generate_unicode_classes.py \
  16.0.0 /path/to/UnicodeData.txt /path/to/PropList.txt \
  lib/tokenizer/src/unicode_classes.zig
zig fmt lib/tokenizer/src/unicode_classes.zig
```

### Ordered internal parallel encoding

`Tokenizer.encodeIntoParallel` is an optional backend operation. GPT-2
ByteLevel BPE splits documents of at least 256 KiB at safe ASCII whitespace
boundaries, encodes chunks concurrently, and gathers IDs in source order.
Normalization and inputs containing added tokens remain serial because their
semantics may cross chunk boundaries.

Parallel chunks are submitted with `std.Io.Group.async`; the final chunk runs
on the calling task before the group is awaited. This is the same composition
pattern used by `lib/linalg`: production callers pass their long-lived runtime
Io, while callers without an Io retain the serial `encodeInto` escape hatch.
For Antfly's backend runtime:

```zig
if (backend_runtime.io()) |io| {
    try tokenizer.encodeIntoParallel(io, allocator, text, &ids, max_tasks);
} else {
    try tokenizer.encodeInto(allocator, text, &ids);
}
```

This avoids a tokenizer-owned thread pool, respects runtime scheduling and
cancellation, and prevents independent subsystems from oversubscribing the
machine. `lib/tokenizer` deliberately depends only on `std.Io`, not Antfly's
storage package; `BackendRuntime.io()` is the layering boundary, just as it is
for the Io-aware matrix multiplication path. A tokenizer used in parallel must
still be constructed with an allocator safe for concurrent use.

### Reusable parallel workspaces

Each tokenizer retains a free list of parallel workspaces. A workspace contains
the fixed worker records and their reusable token-ID buffers, so repeated
`encodeIntoParallel` calls do not allocate and destroy chunk outputs. Chunk
boundaries use a fixed stack array because internal parallelism is capped at 64
tasks. Workspaces are acquired per concurrent call and returned after the
`std.Io.Group` is joined; concurrent requests therefore do not share mutable
output state.

This changed the 738 KiB steady internally parallel result from about
1.30 GB/s to 1.61 GB/s and the 11.8 MB result from about 1.43 GB/s to
1.85 GB/s in the representative runs above.

### ByteLevel direct-address IDs and single-result appends

ByteLevel vocabularies build direct-address tables for all one- and two-byte
raw keys while loading the tokenizer. The tables cost about 257 KiB and are
allocated only for ByteLevel tokenizers. They bypass hashing, probing, and
pointer chasing for these exact tokens.

The remaining cache hit path directly appends its ID when the cached result has
one token instead of entering the slice-copy path. Profiling the GPT-2 fixture
after the direct maps showed 1,404,645 measured cache hits over ten iterations,
no steady-state misses, and 1,243,847 single-ID results: approximately 88.6
percent of the remaining hits.

### Opt-in hit-path profiling

`HfTokenizer.setBpeProfiling(true)` resets and enables atomic counters, and
`bpeProfileSnapshot()` reads a consistent-enough diagnostic snapshot after
workers finish. The benchmark exposes this through `--profile-bpe`. Counters
cover cache hits, misses, probes, key bytes, emitted IDs, and bounded
key-length/result-size histograms. They remain disabled by default so normal
encoding pays only one predictable boolean check on cache hits and misses.

CPU sampling before the direct maps attributed about 21 percent of observed
stacks to Wyhash. After one- and two-byte keys bypassed the cache, that fell to
about 13 percent. The result supports targeting key representation and
avoidable lookups rather than replacing the proven hash with an ad hoc one.

## Rejected or inconclusive experiments

### Fixed-size inline cache entries

Both a larger hybrid entry and a true 32-byte entry were tested. The compact
version stored an atomic tag, a 15-byte packed key, and up to four `u16` token
IDs. Tables with 2,048 and 512 slots per shard measured roughly 149–151 MB/s
steady state, below the pointer table's 158–163 MB/s at that stage, and were
removed. A later integrated 40-byte design stored a 15-byte key and four
`i32` IDs in each entry. At 256 slots per shard it regressed, and at 512 slots
per shard it only tied the pointer table while reserving about 1.3 MiB. It was
also removed. Gigatoken's entry succeeds as part of an integrated table,
probing, key, and value design; copying the layout alone did not help here.

### Worker-local direct-mapped hot cache

A 512-entry thread-local cache with generation identity, packed keys, and
inline IDs measured 131.9 MB/s on one thread and 1.199 GB/s on fourteen,
compared with about 163 MB/s and 1.54 GB/s for the shared cache at that stage.
The extra hash, packing, and lookup cost exceeded the avoided shared read
traffic, so it was removed.

### Specialized short-key hash

A lightweight FNV-style hash for short cache keys replaced Wyhash in an
experiment. Serial throughput fell from roughly 271 MB/s to 224 MB/s. The
workload is dominated by short strings, but Wyhash remains a better hash for
this table and target CPU. Direct-addressing the shortest exact keys provided
the useful version of this optimization.

## Remaining work

The initially identified Gigatoken techniques and the subsequent follow-ups
have now been implemented or measured: vector ASCII scanning, compact inline
cache entries, worker-local caching, ordered chunk parallelism, packed exact
Unicode classes, reusable task workspaces, hit-path profiling, short-key
specialization, and a cache layout co-designed with inline keys and values.
The application's persistent `std.Io` worker runtime owns scheduling, so a
tokenizer-private persistent thread pool is not desirable.

Future profiling should focus on the remaining longer-key Wyhash cost, cache
result memory traffic, and cold BPE merges. A different cache should replace
the current table only when an end-to-end key/probe/value design beats it;
smaller entries or a faster-looking hash are not sufficient in isolation.

Any future parallel change must preserve pretoken and added-token boundaries
and reproduce the exact serial token sequence before its throughput result is
accepted.

## Validation

Focused validation commands:

```sh
cd zig
zig test -OReleaseFast \
  --dep sentencepiece_proto \
  -Mroot=lib/tokenizer/src/hf_tokenizer.zig \
  --dep protobuf \
  -Msentencepiece_proto=/path/to/generated/sentencepiece_proto/root.zig \
  -Mprotobuf=lib/protobuf/src/root.zig -lc

cd pkg/inference
zig build test-tokenizer
zig build test-tokenizer-batch
```

The Hugging Face tokenizer suite currently passes 27 tests with one optional
external-model test skipped. The SentencePiece and tokenizer-batch targets also
pass.
