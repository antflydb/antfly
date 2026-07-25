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
- expected token-sequence BLAKE3:
  `8310f7a8fa0e0daf5354feb8810a80b11ed010165d8a1a4c968afb3353e53d52`

This output matches Hugging Face `tokenizers` and Gigatoken for the fixture.
Focused tests also cover GPT-2 contractions, leading spaces, digit runs,
multi-newline behavior, curly quotes, and non-ASCII letters.

## Reproducible benchmark

Always pass `-Doptimize=ReleaseFast`; the tokenizer is an imported module and
must be optimized along with the benchmark executable:

```sh
cd zig
zig build -Doptimize=ReleaseFast bench-tokenizer -- \
  /path/to/tokenizer.json /path/to/corpus.txt \
  --warmup 2 --iterations 100 --threads 1
```

Use `--warmup 0 --iterations 1` for a cold first pass. `--threads N` runs
concurrent `std.Io` tasks against the same tokenizer and cache. The benchmark
reports the token count, legacy FNV hash, and complete BLAKE3. After the timed
interval, it builds
an independent serial reference with a fresh, cache-disabled tokenizer and
compares the final complete sequence retained by every timed worker
byte-for-byte. It then releases those buffers, repeats the requested external
and internal concurrency against the measured tokenizer, and compares every
replay sequence byte-for-byte. This validation is outside the measured
interval, so concurrency correctness cannot be hidden by a same-length
corruption and does not reduce the reported throughput.

For multi-gigabyte corpora, `--validation hash` computes BLAKE3 over the final
complete output retained by every timed worker, releases those outputs, builds
the independent serial cache-disabled reference, and compares both the digest
and token count. This avoids retaining the reference and multiple
multi-gigabyte outputs simultaneously. Normal regression fixtures retain the
default `--validation exact`, including byte-for-byte final timed and replay
comparisons. Warmup and diagnostic outputs are released before the timed run
in both modes.

`--mmap-corpus --prefault-corpus` avoids a second 11.9 GB input copy while
touching every mapped page before the timer. Gigatoken reads the complete file
before its encode timer, so prefaulting is required for an apples-to-apples
in-memory comparison. Corpus mapping, prefaulting, tokenizer loading, warmup,
validation, and output hashing all remain outside the reported interval.

`--internal-threads N` permits up to N active queue consumers for one
sufficiently large ByteLevel document. The encoder creates 4–8 chunks per
consumer for ordinary inputs and 16 for inputs of at least 1 GiB, capped at
256, so runtime tasks can pull another chunk when work is uneven without
exceeding the requested concurrency. `--repeat N` repeats the
corpus in memory before timing, which is useful for measuring internal
parallelism without changing the fixture. `--cache-max-mb`,
`--chunks-per-task`, `--max-chunks`, `--worker-cache-count`, and
`--worker-cache-slots` make cache-capacity and scheduling sweeps reproducible
without changing production defaults. `--diagnostics`
reports scanner-only, serial cache-disabled, and serial warm throughput.
`--profile-bpe` enables atomic cache-hit counters after warmup and reports
direct hits, cache hits/misses, probe distribution, key lengths, and result
sizes. Profiling and cache statistics are snapshotted before validation.
Profiling is for attribution rather than throughput measurement because the
counters intentionally add work to the hot path.

Every run also reports process CPU time, average utilized cores, CPU
nanoseconds per byte, phase peak-RSS high-water marks, cache admissions,
evictions, and rejected reservations. `zig build
-Doptimize=ReleaseFast bench-tokenizer-build` installs the standalone binary
at `zig-out/bin/tokenizer_benchmark` for `perf`, Instruments, or another
external hardware-counter profiler. The checked-in experiment driver runs the
stage, cache, task-count, and chunk sweeps:

```sh
zig/bench/run_tokenizer_experiments.sh \
  /path/to/tokenizer.json /path/to/corpus.txt 1 exact
```

Use `hash` as its final argument for the full OpenWebText file. No benchmark or
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
| Current | steady, 1 task | 291–295 MB/s |
| Current | steady, 14 concurrent `std.Io` tasks | 2.86–3.09 GB/s |
| Current, 738 KiB corpus | cold, 14 internal tasks | 497.04 MB/s |
| Current, 738 KiB corpus | steady, 16 internal tasks | 2.57 GB/s |
| Current, 11.8 MB repeated corpus | cold, 16 internal tasks | 1.69 GB/s |
| Current, 11.8 MB repeated corpus | steady, 16 internal tasks | 3.01 GB/s |
| Residual experiments, 118 MB repeated corpus | steady, 16 internal tasks, cache disabled | 0.607 GB/s |
| Residual experiments, 118 MB repeated corpus | steady, 16 internal tasks, 2 MiB cache | 2.73 GB/s |
| Residual experiments, 118 MB repeated corpus | steady, 16 internal tasks, 128 chunks | 2.92 GB/s |
| Residual experiments, first 1.0 GB OpenWebText | cold, front cache only, 64 MiB limit | 0.384 GB/s |
| Residual experiments, first 1.0 GB OpenWebText | cold, front + bulk cache, 64 MiB limit | 1.037 GB/s |
| Residual experiments, complete 11.9 GB OpenWebText | cold, front + bulk, 128 MiB limit | 0.585 GB/s |
| Residual experiments, complete 11.9 GB OpenWebText | cold, front + bulk, 512 MiB control | 0.691 GB/s |

The current implementation is approximately 16.4–16.6 times faster than the
original single-thread steady-state implementation while also correcting the
original ByteLevel boundary behavior.

Gigatoken publishes 8.79 GB/s for GPT-2 on the 11.9 GB OpenWebText corpus on
the same CPU class. The complete Zig run now measures 0.585 GB/s under a
128 MiB local cache limit, so the real large-corpus gap is about 15.0x, not the
2.9x suggested by comparing Gigatoken's full corpus with Zig's small repeated
fixture. A 512 MiB Zig control improves only to 0.691 GB/s, reducing the gap to
12.7x; capacity by itself is not the remaining answer.

The contracts are also similar rather than identical. Gigatoken's fastest API
uses a `TextFileSource` document separator and reports about 2,701.65 million
GPT-2 tokens. This benchmark encodes every literal byte in the file, produces
2,704,046,552 IDs, and hashes all of them. The Zig implementation now shares
the application's persistent `std.Io` runtime, reuses task workspaces, pulls
over-decomposed chunks, overlaps ordered gather, and retains long-tail
pretokens. Gigatoken still has a much more integrated SIMD scanner/cache
hierarchy and minimizes communication between workers. Future comparisons
must use the full corpus and state the token contract and memory envelope. See
Gigatoken's
[benchmark and architecture summary](https://github.com/marcelroed/gigatoken)
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

The pretoken cache has a 64-shard front table with 2,048 slots per shard. Reads
remain lock-free; admission, replacement, and table maintenance take only the
affected shard lock. Each table stays at or below 75 percent load to preserve
bounded probe lengths. The front can retain 98,304 pretokens and remains the
only table touched by its hits.

An optional second tier allocates a contiguous dynamic slot array behind the
front. Antfly standalone requests 16,384 slots per shard: 1,048,576 slots,
about 8 MiB of fixed table storage, with a 786,432-entry load bound. Once a
front shard is full, new repeated candidates enter its bulk shard; front
entries remain stable instead of being churned by a long-tail scan. Bulk probes
occur only after a front miss. Both tiers use the same immutable entry
representation, read epoch, per-shard insertion lock, and second-chance CLOCK
policy.

A rotating two-hash doorkeeper requires a repeated observation before either
tier allocates an entry, which prevents a one-pass long tail from consuming the
memory envelope. Hits normally only read the CLOCK bit; they write it only
after an eviction scan has cleared it.

Removed entries are reclaimed after all active encode calls leave a lightweight
read epoch. This keeps lookup pointer loads lock-free without leaking evicted
keys or risking use-after-free. Tombstones preserve probe chains and are
periodically rebuilt while readers are gated. The front and optional bulk
tables, admission filter, live entries, and not-yet-reclaimed entries share a
64 MiB
per-tokenizer hard byte limit, so variable-length keys and results cannot
exceed the memory envelope before the slot-count bound is reached.

Every BPE entry point participates in the read epoch, including generation
encoding's BOS-aware Metaspace override. That override cannot route through the
normal `encodeInto` wrapper because it must suppress Metaspace's implicit
prefix, so it establishes the same epoch explicitly. A focused pressure test
forces replacement through this path and verifies that retired entries are
reclaimed before the call returns.

`BpeCacheConfig.resource_budget` optionally supplies cold-path `try_reserve`
and `release` callbacks. Antfly standalone connects these callbacks to the
node `ResourceManager`'s `inference.tokenizer_cache` slice, which enforces a
64 MiB aggregate soft target and a 128 MiB emergency hard limit across loaded
tokenizers. The standalone adapter stops admitting optional cache growth when
the projected allocation reaches the slice's `shrink_cache` pressure state;
the atomic hard guard closes races between producers. Cache hits never call the
manager. The optional bulk slot allocation is reserved through the same
interface; if its reservation is denied, the tokenizer keeps the front cache
and model warmup succeeds. A rejected entry reservation simply leaves that
pretoken uncached, so resource pressure never makes model loading or
tokenization fail. Parallel workspace retention uses the same budget even when
an optional table could not be allocated.

Standalone installs the budget before warming configured models. Shutdown is
explicitly staged: `DataServer.quiesceBackgroundWork()` closes request
admission and joins durable/background users of the local inference provider;
the inference node is then destroyed and releases every tokenizer reservation
while the manager context is alive; final `DataServer.deinit()` storage and
resource-manager teardown happens last.

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

### Long-tail bulk cache

The optional bulk table was accepted only after a real OpenWebText capacity
run. On the first 1,000,000,000 bytes, the front-only cache reached its 98,304
entry bound, performed 799,241 evictions during the read epoch, rejected
929,345 byte reservations, and measured 384 MB/s cold. A 1,048,576-slot bulk
tier under the normal 64 MiB hard limit retained 587,096 entries with no
evictions or rejected reservations and measured 1.037 GB/s cold, a 2.70x
speedup. Raising only the experiment's local hard limit to 128 MiB measured
1.169 GB/s cold and 1.465 GB/s after one warmup; it retained 583,212 and
658,371 entries respectively.

The 118 MB repeated-Pride guardrail does not use the second tier. Three paired
ten-iteration runs measured medians of 2.900 GB/s without it and 2.926 GB/s
with it, while the complete BLAKE3 remained
`64b4dd4e54e19c5ca52064651ccf663b1dff156f240128748b24e229f6426443`.
The tier therefore preserves the small hot front's lookup path. Its fixed
8 MiB allocation remains optional and `ResourceManager`-accounted instead of
being imposed on every standalone tokenizer library user.

On the complete 11,920,511,059-byte file, a 2,097,152-slot bulk table under a
128 MiB limit measured 585 MB/s and retained 1,518,409 entries. A high-memory
8,388,608-slot, 512 MiB-limit control measured 691 MB/s and retained 2,525,760
entries. Both produced 2,704,046,552 token IDs and BLAKE3
`66cc8eb56e955f8669417b549d831a55418664ec337e16d5f9cb0b6ae5617a5a`.
The modest 18 percent high-memory gain rejects cache capacity as a sufficient
explanation for Gigatoken's remaining throughput advantage.

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
Normalization remains serial. Added-token sets containing whitespace after
their first byte also remain serial when such a token occurs because a generic
whitespace chunk boundary could bisect them. Boundary-safe sets such as
GPT-2's `<|endoftext|>` are segmented inside each parallel chunk. This is
required for OpenWebText: otherwise the document delimiter caused the entire
11.9 GB input to fall back to the serial encoder.

Queue-consumer tasks are submitted with `std.Io.Group.async`; the calling task
is also a consumer before the group is awaited. This is the same composition
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

Antfly standalone attaches its inference node to the shared
`BackendRuntime.io()` before model warmup. Production API token accounting
uses `encodeIntoParallel` with at most sixteen queued consumers when this Io is
available. The tokenizer's 256 KiB semantic threshold keeps normal prompts on
the allocation-reusing serial path; the shared Io worker pool bounds actual
CPU concurrency for large documents without creating or oversubscribing an
independent thread pool.

### Reusable parallel workspaces

Each tokenizer retains a free list of parallel workspaces. A workspace contains
the fixed chunk records and their reusable token-ID and BPE-merge buffers, so
repeated `encodeIntoParallel` calls do not allocate and destroy chunk state.
Chunk boundaries use a fixed stack array because internal chunking is capped at
256. Workspaces are acquired per concurrent call and returned after the
`std.Io.Group` is joined; concurrent requests therefore do not share mutable
output state. The free list retains at most four workspaces, and only
workspaces whose complete retained capacity is at most 64 MiB. Larger
large-corpus workspaces and excess burst-concurrency workspaces are destroyed
on return instead of pinning the process high-water mark. When a resource
budget is configured, retained workspaces use the same cold-path admission
interface as BPE entries.

This changed the 738 KiB steady internally parallel result from about
1.30 GB/s to 1.61 GB/s and the 11.8 MB result from about 1.43 GB/s to
1.85 GB/s in the representative runs above.

### Reusable BPE merge scratch

Each serial encode call and persistent parallel chunk owns reusable symbol-list
and priority-queue storage. Cache misses clear these buffers while retaining
capacity instead of allocating both structures for every previously unseen
pretoken. The priority queue is initialized lazily, so a fully warm call does
not allocate unused miss-path state. This principally improves cold encoding;
the representative cold 738 KiB internal result is now about 497 MB/s.

### Bounded pull scheduling

Large documents are divided into 4 chunks per requested consumer below 4 MiB,
8 above it, and 16 at or above 1 GiB, capped at 256 chunks. At most
`max_tasks` `std.Io` consumers pull indices from one atomic queue. This keeps
the public concurrency limit meaningful while allowing a fast consumer to take
more work instead of waiting for the slowest fixed partition. The 16-chunk
large-corpus tier was added after the complete private-cache run exposed a
full-materialization occupancy cliff that the smaller scheduler sweep could
not reveal.

Combined with the reusable workspace, this moves steady internal throughput to
about 2.72 GB/s for 738 KiB and 3.16 GB/s for 11.8 MB. An
experimental descending-size LPT layout was slower than uniform chunks on the
M4 Max, so the accepted scheduler uses uniform byte targets and dynamic
pulling.

A controlled 118 MB sweep with sixteen consumers measured median throughput of
2.823 GB/s at the previous 64-chunk cap and 2.884 GB/s at 128 chunks, a 2.2
percent improvement with identical token count and BLAKE3. That result selected
the former 128-chunk cap; the large-corpus tier now permits 256. One, two, four,
and eight chunks per
consumer measured 2.245, 2.658, 2.768, and 2.801 GB/s respectively in the
initial sweep; task counts from one through sixteen scaled from 305 MB/s to
2.92 GB/s.

### Bounded parallel-boundary planning

Chunk planning probes forward from monotonically increasing byte targets for
the next safe whitespace-run boundary. Once a boundary is found, every target
that resolves to it is skipped. The first EOF result terminates planning.
Normal prose therefore keeps the cheap few-byte targeted probes, while a
whitespace-free or minified document scans its remaining suffix once instead
of up to 63 times before taking the required serial fallback. A focused test
compares the optimized collector against independent scans across empty,
whitespace-free, repeated-whitespace, and mixed ASCII-whitespace inputs.

### Overlapped ordered gather

The caller reserves a one-token-per-three-input-bytes density estimate before
launch. Completed chunks publish a release flag, and whichever queue consumer
can acquire the commit mutex copies the longest completed prefix that fits
without allocation. After joining, the caller computes the exact residual
token count, grows once if needed, and drains the suffix. Typical GPT-2 text
keeps the fully overlapped path, while worst-case byte-per-token input allocates
only the output capacity it actually needs instead of reserving four output
bytes for every input byte. Source order is preserved and errors roll the
caller's output length back to its entry value.

On Linux, newly grown output allocations of at least 2 MiB receive a
best-effort `MADV_HUGEPAGE` hint over their page-aligned interior before first
touch. It is a no-op on macOS and other targets. The hint is applied to the
large contiguous output where it is safe and useful; the current sharded cache
contains allocator-owned objects and is not falsely treated as one huge-page
allocation.

Post-hardening validation retained the complete hashes and measured 2.57 GB/s
for 100 iterations of the 738 KiB internal-task workload and 3.01 GB/s for ten
iterations of the 11.8 MB workload. Both reported 9,571 live entries, 1,795,976
accounted cache bytes, and zero rejected reservations. Four concurrent
requests, each using up to four consumers, measured 3.21 GB/s on the 738 KiB
fixture.

### ByteLevel direct-address IDs and single-result appends

ByteLevel vocabularies build direct-address tables for all one- and two-byte
raw keys while loading the tokenizer. The tables cost about 257 KiB and are
allocated only for ByteLevel tokenizers. They bypass hashing, probing, and
pointer chasing for these exact tokens.

The direct lookup is used only when the model has no end-of-word suffix.
Suffix-aware BPE must construct the word-final lookup key even for a one-byte
pretoken; bypassing that step would select the raw token instead of its
word-final vocabulary entry. Regression coverage keeps both the suffix-free
fast path and suffix-aware result exact.

The remaining cache hit path directly appends its ID when the cached result has
one token instead of entering the slice-copy path. Profiling the GPT-2 fixture
after the direct maps showed 1,404,645 measured cache hits over ten iterations,
no steady-state misses, and 1,243,847 single-ID results: approximately 88.6
percent of the remaining hits.

### Opt-in hit-path profiling

`HfTokenizer.setBpeProfiling(true)` atomically disables, resets, and re-enables
the counters, and
`bpeProfileSnapshot()` reads a consistent-enough diagnostic snapshot after
workers finish. The benchmark exposes this through `--profile-bpe`. Counters
cover total pretokens, direct-address hits, cache hits, misses, probes, key
bytes, emitted IDs, and bounded key-length/result-size/probe histograms. They
remain disabled by default so normal encoding pays only one predictable
boolean check on cache hits and misses.

On the first 1 GB of OpenWebText with the 64 MiB bulk configuration, the
profile recorded 207,448,512 pretokens, 44,290,215 direct-address hits,
160,757,188 cache hits, and 2,401,096 cache misses: a 98.53 percent cache hit
rate after direct lookup. Atomic profiling reduced throughput to 15.1 MB/s,
confirming that this mode is diagnostic only; all performance numbers above
come from profiling-disabled runs.

CPU sampling before the direct maps attributed about 21 percent of observed
stacks to Wyhash. After one- and two-byte keys bypassed the cache, that fell to
about 13 percent. The result supports targeting key representation and
avoidable lookups rather than replacing the proven hash with an ad hoc one.

### Pollution-resistant bounded cache

Cold misses first pass through a 64 KiB, two-generation doorkeeper. Two
independent bits share one atomic word, so observation needs one read-modify-
write instead of contending on two cache lines. A key must be observed twice
within the rolling window before the cache allocates its immutable key and
token-ID result. Rotation clears an inactive generation before publishing it,
so an unbounded stream of unique pretokens cannot saturate admission forever.

At the 75-percent shard limit, insertion gives entries a second chance with a
CLOCK bit and replaces a cold victim. A read-side epoch permits the hit path to
load immutable entry pointers without locks; retired entries are freed and
their local and `ResourceManager` bytes released only after prior readers
drain. If a byte reservation is denied, an eligible victim can be retired so a
later repeated candidate can use the released capacity. Duplicate checks under
the shard lock prevent a racing insertion from causing needless eviction.

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

A later persistent pointer-cache variant used the reusable `std.Io` workspace
and held either 4,096 or 32,768 entry pointers per chunk. The 11.8 MB internal
result fell to 1.64 GB/s and 1.81 GB/s respectively, versus approximately
2.0–2.2 GB/s for the shared-cache path at that experiment stage. Shared hits
already require no lock; an additional table lookup did not repay the atomic
pointer load it avoided.

### Two-phase cache prefetch pipeline

A 256-pretoken pipeline separated span discovery/hash computation from cache
probe and emission, prefetched home entries during discovery, and prefetched
token-ID storage twelve probes ahead. It reproduced the full token hash but
reduced serial throughput from 291 MB/s to 259 MB/s and concurrent throughput
from 2.86 GB/s to 2.55 GB/s on the Pride fixture. Its approximately 9,700-entry
working set is already cache-resident, so the extra span materialization and
second pass cannot hide a DRAM stall that is not present.

Gigatoken's pipeline addresses a roughly 64 MiB, 1.3-million-entry table where
tail probes are random DRAM accesses. Reconsider prefetching only together with
a scalable large-corpus cache and an OpenWebText-sized benchmark.

### Multi-cursor pretoken scanning

Gigatoken's historical optimization log reports a dual-cursor gain for
pretoken *counting*. Its current production r50k scanner says the windowed
2–4-cursor streaming variants measured 0.80–0.95 times the single cursor due
to queue traffic and interleaved branch history. The Zig encoder already uses
a 64-byte boundary mask and consumes its bits without per-token classifier
dispatch, so no multi-cursor variant was retained.

### Descending LPT chunk sizes

An 80-percent large-head/20-percent small-tail layout, modeled on Gigatoken's
asymmetric-core tail mitigation, reduced the 11.8 MB result from about
2.69 GB/s to 2.40 GB/s and the 738 KiB result from 2.34 GB/s to 2.08 GB/s.
Dynamic pulling over uniform chunks balances this much smaller workload better.

### Specialized short-key hash

A lightweight FNV-style hash for short cache keys replaced Wyhash in an
experiment. Serial throughput fell from roughly 271 MB/s to 224 MB/s. The
workload is dominated by short strings, but Wyhash remains a better hash for
this table and target CPU. Direct-addressing the shortest exact keys provided
the useful version of this optimization.

## Gigatoken parity plan

The earlier experiments tested individual Gigatoken ideas against Antfly's
shared pointer cache. That is not the architecture behind Gigatoken's published
8.79 GB/s result. At upstream commit
`0d9765fa7312af7534535e6315a5c49d74807b2a`, its complete fast path combines:

- one persistent worker pool with about sixteen continuously useful consumers;
- one private, pre-sized short-pretoken table per consumer (normally 64 MiB),
  so hits perform no atomic operations, pointer chasing, allocation, or shared
  cache-line writes;
- a 32-byte inline entry containing a 128-bit length-tagged key and up to four
  token IDs, with paired linear probes at 75 percent maximum load;
- batches of 256 pretokens, with L2 prefetch during key preparation, L1
  prefetch sixteen probes ahead, and four speculative output stores;
- a two-phase 64-byte SIMD GPT-2 scanner whose uncommon Unicode and ambiguous
  boundaries use a separate cold path;
- at least 1 MiB chunks, about sixteen chunks per consumer, dynamic in-order
  pull, bounded prefix commits, and deferred release of large chunk buffers.

Its campaign report attributes roughly 1.5 CPU ns/input-byte to the final
encoder. The qualified Zig run currently needs approximately 8 CPU
ns/input-byte and keeps only 4.8--5.5 cores useful. Reaching parity therefore
requires both a 5--6x per-core hot-path reduction and roughly 3x higher useful
occupancy. Enlarging the existing shared cache cannot close either gap: the
512 MiB control improved the complete corpus by only 18 percent.

### Production implementation

The parity path is deliberately opt-in because sixteen 64 MiB private tables
consume about 1 GiB. `ParallelBpeConfig` controls the number and size of
persistent worker-local tables. Tables are acquired by a `std.Io` consumer for
the duration of its pull loop, survive across encode calls, and are independent
of OS-thread identity. Lazy table creation happens in parallel and every byte
is reserved through the tokenizer's `BpeCacheResourceBudget`; denial or
allocation failure falls back to the bounded shared cache without affecting
correctness.

Short keys of 3--15 bytes use the private inline table. One- and two-byte
vocabulary hits retain the direct-address tables, while longer keys and outputs
larger than four IDs use the existing exact slow path. A 256-entry preparation
batch separates scanning/key construction from probes, issues staged
prefetches, and writes cached IDs directly to reserved output storage. Misses
run BPE once and populate the local table. This is materially different from
the rejected 512-entry local pointer cache: it replaces the shared pointer
representation and its second dependent load instead of adding another lookup
in front of it.

The scheduler limits each opportunistic prefix commit to eight chunks. This
prevents a finishing encoder from becoming a long serial gather worker while
other consumers go idle. Chunk output and BPE scratch remain reusable through
the existing workspace pool. After encode workers join, residual gathers of at
least 64 MiB use the same bounded `std.Io` runtime to copy disjoint output
ranges in parallel; smaller results avoid the task-launch overhead. Workspace
retention and local-cache retention share the same process resource budget.

The current 64-byte boundary-mask scanner remains the exact scanner used by
both serial and local-cache paths. The next scanner milestone is not another
multi-cursor loop: it is a generated two-phase ASCII/Unicode mask kernel with
explicit usable and bad zones, plus architecture-specific NEON and AVX
implementations. It must first exceed 2.4 GB/s on the scanner-only full-corpus
stage and reproduce every existing boundary test. Until that gate passes, the
portable mask implementation remains production code.

### Qualification gates

Performance changes are accepted only when all of the following hold:

1. The 11,920,511,059-byte OpenWebText result contains exactly 2,704,046,552
   tokens and has BLAKE3
   `66cc8eb56e955f8669417b549d831a55418664ec337e16d5f9cb0b6ae5617a5a`.
2. A fresh tokenizer with caching disabled independently produces the reference
   sequence; concurrent timed output is checked exactly or with complete
   BLAKE3 validation.
3. The Pride-and-Prejudice small-corpus guardrail does not regress by more than
   3 percent from the shared-cache path.
4. No-cache, denied-budget, allocation-failure, added-token boundary, and
   concurrent shared-tokenizer tests pass without leaks.
5. Full-corpus reporting includes wall throughput, CPU ns/input-byte, average
   useful cores, peak RSS, table bytes, cache occupancy, and scanner-only
   throughput. A speedup obtained solely from unreported memory growth is not a
   parity result.

The benchmark exposes `--worker-cache-count` and `--worker-cache-slots`.
`--worker-cache-count 16 --worker-cache-slots 2097152` reproduces Gigatoken's
approximately 1 GiB private-cache geometry. Production defaults keep this
disabled; a backend can enable it only when its resource manager admits the
retained allocation. The published shared-cache configuration remains the
memory-bounded baseline, not a claimed Gigatoken-equivalent configuration.

### Inline-cache results

The implemented path uses one padded 128-bit key load, ARM CRC32C when
available (with a portable multiply-fold fallback), one prepared hash reused by
both prefetch stages and the final paired probe, and four unconditional output
stores. Large tables are seeded from exact vocabulary entries before their
first use. A small table skips seeding when the vocabulary would consume more
than half its capacity, preserving space for observed pretokens.

On the M4 qualification host, ReleaseFast results were:

| Corpus/configuration | Throughput | CPU ns/byte | Useful cores | Private table bytes |
| --- | ---: | ---: | ---: | ---: |
| 11.8 MB guardrail, shared cache | 2.575 GB/s | 4.23 | 10.88 | 0 |
| 11.8 MB guardrail, 14 x 32K private entries | 2.507 GB/s | 4.30 | 10.77 | 14 MiB |
| 1 GB OpenWebText prefix, default shared cache | 0.339 GB/s | 34.69 | 11.75 | 0 |
| 1 GB OpenWebText prefix, 14 x 2M private entries | 1.694 GB/s | 6.14 | 10.40 | 896 MiB |
| Complete OpenWebText, former shared baseline | 0.585 GB/s | 7.90 | 4.84 | 0 |
| Complete OpenWebText, 14 x 2M private entries | 0.899 GB/s | 5.93 | 5.34 | 896 MiB |

The complete result is 54 percent faster than the former qualified baseline
and reproduces 2,704,046,552 tokens with BLAKE3
`66cc8eb56e955f8669417b549d831a55418664ec337e16d5f9cb0b6ae5617a5a`.
It used 256 chunks, peaked at 23.7 GB RSS, and filled 22,020,096 of
29,360,128 private slots. The small guardrail is 1.0 percent slower, inside the
3 percent acceptance bound in the initial paired run and 2.65 percent slower
in the final paired run shown above. The large-corpus result is therefore a real
improvement, but it is not yet Gigatoken parity.

The data isolates the remaining work:

- Reduce the complete hit path from 5.93 toward 1.5 CPU ns/input-byte. Hardware
  counters must separate scanner/key preparation, paired random probes,
  prepared-span traffic, output stores, and BPE misses before another hot-path
  rewrite is accepted.
- Replace the portable mixed-block scanner with the gated NEON/AVX two-phase
  usable/bad-zone kernel. The current scanner retains safe ASCII prefixes
  around Unicode but measures only about 0.66 GB/s, versus Gigatoken's
  2.46--2.60 GB/s scanner.
- Remove the full-materialization occupancy cliff. Sixteen chunks per consumer
  and the 256-chunk cap raised complete useful occupancy from 4.83 to 5.34, but
  the 1 GB prefix sustains more than twelve. Page faults, memory pressure,
  ordered gather copies, allocator release, and `std.Io` task residency need
  separate timing/counters on a host with enough RAM to avoid compression.
- Add a high-memory output mode that reserves the one-token-per-input-byte
  upper bound, permits unconditional writes without batch capacity checks, and
  performs the suffix gather in parallel. It must remain resource-budgeted and
  opt-in because the complete fixture alone would reserve roughly 48 GB for
  final IDs.

The target is first less than 3 CPU ns/input-byte with at least twelve useful
cores on the complete corpus, then the approximately 1.5 CPU ns/input-byte /
8.79 GB/s upstream envelope on comparable M4 Max hardware. The often-quoted
28 GB/s figure is a pretoken-counting or synthetic substage, not Gigatoken's
materialized complete-BPE OpenWebText result.

The 4.4 GB compressed OpenWebText fixture is intentionally not a normal unit or
CI dependency. Its decompressed input is 11,920,511,059 bytes and the reference
output is 2,704,046,552 GPT-2 tokens with BLAKE3
`66cc8eb56e955f8669417b549d831a55418664ec337e16d5f9cb0b6ae5617a5a`.
Retain this count and digest as the external qualification contract.

Any future parallel change must preserve pretoken and added-token boundaries
and reproduce the exact serial token sequence before its throughput result is
accepted.

## Validation

Focused validation commands:

```sh
cd zig/pkg/inference
zig build test-tokenizer
zig build test-tokenizer-batch
zig build test

cd ../..
zig build root-test
zig build resource-budget-test
zig build -Doptimize=ReleaseFast bench-tokenizer-build
```

`test-tokenizer` runs both the Hugging Face and SentencePiece implementations;
the tokenizer-batch target covers its inference adapter. `zig build test`
currently selects 2,035 inference tests: 2,024 pass and 11 optional tests skip.
`zig build root-test` passes all 222 root compile/unit tests. The focused
`zig build resource-budget-test` gate passes both filesystem tests and all 28
resource-manager tests without leaks. The ReleaseFast build step verifies the
installed benchmark artifact used by the external experiments.
