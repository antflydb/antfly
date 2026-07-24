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
concurrent callers against the same tokenizer and cache. The benchmark reports
the token count and sequence hash so invalid performance results are visible.
`--internal-threads N` splits one sufficiently large ByteLevel document at
pretoken-safe boundaries and gathers the results in order. `--repeat N` repeats
the corpus in memory before timing, which is useful for measuring internal
parallelism without changing the fixture.

## Baseline and current results

Measured on an Apple M4 Max. Throughput is decimal MB/s.

| Implementation | Mode | Throughput |
|---|---|---:|
| Original Zig implementation | steady, 1 thread | 17.75 MB/s |
| Packed merges + initial cache | steady, 1 thread | 43.57 MB/s |
| Streaming ByteLevel pretokens | steady, 1 thread | 98.34 MB/s |
| Added-token scan fast path | steady, 1 thread | 114.62 MB/s |
| Lock-free open-address cache | steady, 1 thread | 121.62 MB/s |
| Raw-byte vocab + ASCII vector scanner | cold, 1 thread | 93.82 MB/s |
| Current | steady, 1 thread | 268.92 MB/s |
| Current | steady, 8 concurrent callers | 1.982 GB/s |
| Current | steady, 14 concurrent callers | 2.482 GB/s |
| Current, 11.8 MB repeated corpus | cold, 14 internal workers | 1.109 GB/s |
| Current, 11.8 MB repeated corpus | steady, 14 internal workers | 1.478 GB/s |

The current implementation is approximately 15.2 times faster than the
original single-thread steady-state implementation while also correcting the
original ByteLevel boundary behavior.

Gigatoken's published large-corpus M4 Max result is 8.79 GB/s. The measurements
are not directly interchangeable: this benchmark includes complete BPE token
ID generation and hashes the full output, while Gigatoken's headline workload
and its persistent worker runtime differ. The remaining gap is real enough to
keep profiling, especially around internal worker startup and cache layout.
See Gigatoken's
[design document](https://github.com/marcelroed/gigatoken/blob/main/design_doc.md)
and
[pretokenizer optimization log](https://github.com/marcelroed/gigatoken/blob/main/pretokenizer_optimization_log.md).

## Current BPE data path

1. Normalization and added-token segmentation.
2. A 64-byte ASCII vector scan, falling back to a scalar exact-Unicode scanner.
3. Pretoken-cache lookup on a borrowed raw input slice using one Wyhash and
   open addressing.
4. On a miss, BPE merge candidates use packed `(left_id, right_id)` keys.
5. The final token IDs are published to the cache and appended to the caller's
   reusable output buffer.

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
semantics may cross chunk boundaries. Worker creation currently happens on
every call, so the measured throughput includes thread startup. A tokenizer
used this way must be constructed with an allocator safe for concurrent use.

## Rejected or inconclusive experiments

### Fixed-size inline cache entries

Both a larger hybrid entry and a true 32-byte entry were tested. The compact
version stored an atomic tag, a 15-byte packed key, and up to four `u16` token
IDs. Tables with 2,048 and 512 slots per shard measured roughly 149–151 MB/s
steady state, below the pointer table's 158–163 MB/s at that stage, and were
removed. Gigatoken's entry succeeds as part of an integrated table, probing,
key, and value design; copying the layout alone did not help here.

### Worker-local direct-mapped hot cache

A 512-entry thread-local cache with generation identity, packed keys, and
inline IDs measured 131.9 MB/s on one thread and 1.199 GB/s on fourteen,
compared with about 163 MB/s and 1.54 GB/s for the shared cache at that stage.
The extra hash, packing, and lookup cost exceeded the avoided shared read
traffic, so it was removed.

## Remaining work

The five initially identified Gigatoken techniques have now been implemented
or measured: vector ASCII scanning, compact inline cache entries, worker-local
caching, ordered chunk parallelism, and packed exact Unicode classes. The
highest-value next experiments are a persistent internal worker pool, profiling
the remaining BPE/cache cost on hits, and a cache design co-developed with its
key/value encoding rather than transplanted in isolation.

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

The Hugging Face tokenizer suite currently passes 26 tests with one optional
external-model test skipped. The SentencePiece and tokenizer-batch targets also
pass.
