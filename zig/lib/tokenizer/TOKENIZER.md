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

The explicit high-memory qualification command is:

```sh
./zig-out/bin/tokenizer_benchmark tokenizer.json owt_train.txt \
  --warmup 2 --iterations 3 \
  --internal-threads 16 --chunks-per-task 16 --max-chunks 256 \
  --worker-cache-count 16 --worker-cache-slots 2097152 \
  --workspace-retain-max-mb 8192 \
  --mmap-corpus --prefault-corpus --stable-input \
  --segmented-output --packed-u16-output --validation hash
```

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

The current qualification host is an Apple M4 Max with 14 logical cores
(10 performance and 4 efficiency cores). Gigatoken's published 8.79 GB/s row
uses the 16-core M4 Max, so wall-rate comparisons must state the core count.
Throughput below is decimal GB/s.

| Corpus/profile | Throughput | CPU ns/byte | Useful cores |
| --- | ---: | ---: | ---: |
| 1 GB OpenWebText, 16 x 2M private entries | 8.80–9.44 GB/s | 1.300–1.302 | 11.44–12.29 |
| Complete OpenWebText, 16 x 2M private entries | 7.33–7.69 GB/s | 1.600–1.609 | 11.80–12.31 |
| 11.8 MB Pride guard, bounded shared cache | 4.27 GB/s | 2.994 | 12.79 |
| 11.8 MB Pride guard, high-memory profile | 8.83 GB/s | 1.286 | 11.36 |

The complete result is exact: 11,920,511,059 input bytes produce
2,704,046,552 IDs with BLAKE3
`66cc8eb56e955f8669417b549d831a55418664ec337e16d5f9cb0b6ae5617a5a`.
The high-memory path is faster rather than regressed on the small-corpus
guard, so it passes the 3-percent guardrail. The best complete run meets the
CPU-efficiency, useful-core, correctness, and memory-observability gates, but
does not yet meet the 8 GB/s wall-rate gate on this 14-core host.

The full run reports approximately 18.8 GB peak RSS, 1.11 GB of private cache
storage, 5.408 GB of logical packed output, 6.654 GB of output capacity, and
6.686 GB in the active reusable workspace. These figures are part of the
result, not hidden setup costs. Normal production defaults do not allocate
this high-memory profile.

Gigatoken's fastest API treats the document separator specially and currently
reports about 2,701.65 million GPT-2 tokens; Antfly's qualification encodes
every literal byte and hashes all 2,704,046,552 IDs. See Gigatoken's
[benchmark and architecture summary](https://github.com/marcelroed/gigatoken)
and
[pretokenizer optimization log](https://github.com/marcelroed/gigatoken/blob/main/pretokenizer_optimization_log.md).

## Current BPE data path

1. Normalization and added-token segmentation.
2. An exact fixed-grid GPT-2 scanner classifies 64 bytes at a time, derives
   usable and ambiguous masks, and sends only Unicode/edge ambiguity through
   the scalar ground truth.
3. A two-phase fill harvests boundaries, then prepares 256 length-tagged
   128-bit keys. L2 prefetches issue during preparation; L1 prefetches issue
   sixteen probes ahead.
4. One- and two-byte ByteLevel pretokens use direct-address vocabulary tables.
   Other short pretokens probe an aligned private 64-byte pair containing two
   exact 32-byte entries.
5. A warm hit already contains four final-form `u16` output lanes. The count
   is decoded as `4 - @clz(~value) / 16`, and one 64-bit store writes packed
   output. Larger or non-u16 results use an exact resource-budgeted spill
   arena.
6. A miss runs packed-key BPE once, admits the result to the private table when
   allowed, and otherwise falls back to the bounded shared cache.
7. Ordered per-chunk `u16` segments remain in the reusable workspace; the
   high-memory benchmark does not require a multi-gigabyte flatten copy.

ByteLevel vocabulary and merge pieces are decoded from GPT-2's byte-to-Unicode
alphabet once while loading `tokenizer.json`. The hot encoder therefore uses
raw input bytes directly, while `id_to_token` retains the original display
strings for decoding.

The pretoken cache has a 64-shard front table with 2,048 slots per shard. Reads
remain lock-free; admission, replacement, and table maintenance take only the
affected shard lock. Each table stays at or below 75 percent load to preserve
bounded probe lengths. The front can retain 98,304 pretokens and remains the
only table touched by its hits.

The opt-in high-memory path adds persistent private tables acquired by
`std.Io` consumers. A table is not tied to an OS thread. Stable-input replay
freezes admitted entries and lets any queue consumer execute a chunk against
its learned owner table without locks or atomics. This composes with
`BackendRuntime.io()`; tokenizer code owns neither an executor nor an OS
thread.

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
The current private-table path is that integrated design and supersedes this
older shared-cache experiment.

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
That condition is now satisfied by the opt-in private tables; their accepted
pipeline uses L2 prefetch during the 256-entry fill and L1 prefetch sixteen
probes ahead. The rejection above applies only to the former small shared
pointer cache.

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

## Gigatoken-class implementation

Gigatoken's complete fast path combines:

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

Antfly now implements each of those architectural elements. The remaining
measured gap is small: 1.600--1.609 CPU ns/input-byte is in the upstream
per-core envelope, while full-corpus wall throughput is 7.33--7.69 GB/s on a
14-logical-core host. The published 8.79 GB/s comparison was measured on the
16-core M4 Max.

### Production implementation

The high-memory path is deliberately opt-in because sixteen 64 MiB private tables
consume about 1 GiB. `ParallelBpeConfig` controls the number and size of
persistent worker-local tables. Tables are acquired by a `std.Io` consumer for
the duration of its pull loop, survive across encode calls, and are independent
of OS-thread identity. Lazy table creation happens in parallel and every byte
is reserved through the tokenizer's `BpeCacheResourceBudget`; denial or
allocation failure falls back to the bounded shared cache without affecting
correctness.

Short keys of 3--15 bytes use the private inline table. One- and two-byte
vocabulary hits retain direct-address tables. The entry value is the final four
`u16` lanes, padded with `0xffff`; `@clz(~value)` recovers the lane count in one
native instruction. Results that do not fit spill to a budgeted per-table i32
arena. A 256-entry preparation batch separates scanning/key construction from
probes, issues staged prefetches, and writes cached IDs directly to reserved
output storage. Misses run BPE once and populate the local table.

The scanner is an exact two-phase 64-byte fixed-grid implementation. Byte
classification and boundary-table adjustment use Zig `@Vector` operations so
LLVM selects NEON, AVX, or another target ISA from one source implementation.
Boundary flattening computes all eight octet write offsets with a scalar SWAR
prefix sum and performs eight independent `@Vector(8, u16)` stores. The only
architecture-specific scanner fragment is a small AArch64 ADDP movemask
reduction retained because LLVM's generic lowering was measurably inferior;
classification and data movement remain portable Zig vectors.

The scheduler creates up to 256 chunks and submits bounded consumers with
`std.Io.Group.async`; the caller is also a consumer. Chunk output, boundary
metadata, and BPE scratch remain reusable through the workspace pool. Packed
segmented output avoids a full gather. Workspace, stable metadata, private
tables, and spill arenas share the tokenizer resource budget.

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

### Qualification result

The implemented path uses one padded 128-bit key load, ARM CRC32C when
available (with a portable multiply-fold fallback), one prepared hash reused by
both prefetch stages and the final paired probe, and one final-form packed
output store. Large tables are seeded from exact vocabulary entries before
their first use.

On the 14-core M4 Max qualification host, ReleaseFast results were:

| Corpus/configuration | Throughput | CPU ns/byte | Useful cores | Private table bytes |
| --- | ---: | ---: | ---: | ---: |
| 1 GB prefix, 16 x 2M entries | 8.80–9.44 GB/s | 1.300–1.302 | 11.44–12.29 | 1.077 GB |
| Complete OWT, best accepted sample | 7.69 GB/s | 1.600 | 12.31 | 1.111 GB |
| Complete OWT, final clean rebuild | 7.33 GB/s | 1.609 | 11.80 | 1.114 GB |

Both complete samples reproduce the exact token count and BLAKE3 contract.
The variation is useful-core availability rather than per-byte work: CPU cost
stays within 0.009 ns/byte. The 8 GB/s wall gate remains open, while the
approximately 1.7 CPU ns/byte target is met.

### Resource-manager behavior

`BpeCacheResourceBudget` covers private tables, spill arenas, retained stable
metadata, and reusable workspace capacity. The standalone adapter maps it to
the ResourceManager's `inference_tokenizer_cache` category. Active segmented
output is request-owned rather than retained cache, so the benchmark reports
both logical output bytes and reserved capacity explicitly.

Focused denial tests reject every optional reservation, verify that no private
table is retained, compare packed segmented output with the serial i32 result,
and confirm that all workspace and tokenizer bytes are released at teardown.
Denial therefore changes only speed and retention, never tokenization success
or output.

### Accepted and rejected residual experiments

Accepted:

- fixed-grid exact Unicode-aware masks and portable Zig-vector
  classification;
- SWAR octet prefix sums plus eight unconditional
  `@Vector(8, u16)` boundary stores;
- final-form four-lane u16 cache values with exact spill;
- `@clz(~value)` inline-result counts;
- packed segmented u16 output and explicit reservation reporting;
- sixteen-probe L1 distance and fill-stage L2 prefetch;
- persistent `std.Io` workers, stable cache affinity, and 256 pull-scheduled
  chunks.

Rejected because the exact control regressed or remained neutral:

- 24-byte cache entries: reduced residency but made aligned home pairs cross
  cache lines, dropping the 1 GB control from 8.78 to 7.63 GB/s;
- a 24-byte prepared-span record;
- branchless phase-B key packing, including a repeat after the SIMD phase-A
  rewrite;
- count-only/prebalanced cache ownership, which duplicated keys and reduced
  complete throughput;
- a dense one-bit-per-byte stable boundary index;
- a resident corpus copy and `mlock` corpus mode;
- 32-probe prefetch distance, conditional long-key prefetch, a long-pretoken
  side cache, and extra branch hints;
- seventeen consumers on the 14-core host;
- a compact-cache superpage request on Darwin, which the kernel rejects and
  therefore safely falls back to the normal aligned allocator.

### Remaining work

The code no longer has a 5–15x algorithmic gap. Concrete remaining work is:

1. Re-run the exact qualification on the same 16-core M4 Max class as the
   published 8.79 GB/s row. This 14-core host cannot establish that
   apples-to-apples wall-rate claim.
2. Profile `std.Io` runnable residency and cache/TLB stalls on that host. At
   1.60 CPU ns/byte, sustaining 12.8–13.0 useful cores is enough to cross
   8 GB/s; a change must improve measured residency, not merely add tasks.
3. Add and qualify huge-page-backed private tables on Linux using a portable
   allocation/fallback abstraction. Darwin's explicit 2 MiB mapping request is
   unavailable on this host.
4. Run the same exact fixtures on x86-64 so Zig's generic `@Vector` lowering,
   CRC/fold hash selection, prefetch ladder, and fallback paths have published
   AVX2/AVX-512 evidence.

The often-quoted 28 GB/s figure referred to a scanner/counting or synthetic
substage, not the materialized complete-BPE M4 result. Gigatoken's published
complete GPT-2 row remains 8.79 GB/s on the 16-core M4 Max.

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
