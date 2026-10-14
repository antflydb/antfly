# Media and OCR optimization, October 2026

Current contracts: [batching](../../../../../zig/pkg/inference/BATCHING.md#gpu-residency-for-media-embeddings)
and [generative OCR](../../gemma4-ocr-optimization.md). The single
[results JSON](../../media-throughput-results.json) retains the final timing
samples, source/binary/harness pins, corpus hashes, exact OCR flags and gates.

Detailed reports and former guides are preserved in the [pinned evidence snapshot](https://github.com/antflydb/antfly/tree/01cb3636fe502a48a83f9881bd8df9d925d36a70/docs/design/inference).
[Download the repository snapshot ZIP](https://github.com/antflydb/antfly/archive/01cb3636fe502a48a83f9881bd8df9d925d36a70.zip)
and inspect `docs/design/inference/` for the detailed reports. A separate
219,486-byte evidence-only ZIP was generated locally as
`.benchmark-results/pr-1088-evidence-01cb3636fe.zip`; its checksum is in the results
JSON. It contains published reports/guides and the unabridged diaries, not model
assets or ignored raw transcripts/logs. Original full diary:
[OCR at 144f7dc](https://github.com/antflydb/antfly/blob/144f7dc49c17bb4b9b747b58d8ae2a81b918f3e6/docs/design/inference/gemma4-ocr-optimization.md).

## Final results

M4 Max (32 GPU cores, 36 GiB). These are distinct local campaigns and timing
scopes, not a deployment SLA or cross-runtime ranking.

| Workload | Before → after | Qualified conclusion |
| --- | --- | --- |
| Eight square-image embeddings | 11.827 → 2.377 s p50 | Flash + frames + resident handoff; final cosine >= 0.999999987 |
| Eight photo-image embeddings | 12.128 → 2.388 s p50 | Final cosine >= 0.999999962; borrowed raster scope |
| Gemma4 eight short audio clips | 1.268 → 1.168 s p50 | Downstream embedding encoder batches; conformer stays per clip |
| CLAP sixteen short audio clips | 1.018 → 0.900 s p50 | Native batches plus bounded CPU preparation overlap |
| Eight-page Gemma4 OCR | Ordinary brackets 55.339–57.572 s → 48.819–49.304 s | 12.8% less mean recognition time; all 4,057 tokens/transcripts exact |
| Earlier Apple accurate OCR | 2.087 s for the same eight pages | Warm recognition baseline excludes PNG preparation/HTTP |

## Image and audio embeddings

Retain numeric intermediates and final prompt/encoder handoffs on the admitted
backend; only final vectors download in qualified paths. CPU codecs, exact resize,
audio resampling/frame packing and unsupported provider operations remain host
boundaries. Synthetic native/Metal tests establish parity/ownership/transfer
behavior; CUDA cross-compilation does not qualify CUDA throughput.

Initial same-grid image batching gained ~11% at four rows; eight rows were slower
and doubled activation storage. Serving stays at four. Model-scoped vision Flash
and per-layer command frames then yielded the larger embedding gains above.
Geometry-qualified brokers preserve model generation, grid axes, task/dimensions,
source/options and original indexes. Bounded lookahead joins on every exit and
acquires no speculative GPU lease. Lookahead's independent gain and saturated
HTTP throughput remain unqualified because of drift/low coalescing occupancy.

Audio-only Gemma encoder groups use at most four rows, <=25% padding and a
1,024-token cap. CLAP retains native batches up to eight and fusion/fallback.
Mixed lengths, stereo, PCM, MP3/AAC/Ogg and indexed failures preserve reference
vectors. Concurrent compilation limits the short-clip timing claims; these
are not audio text-generation or GPU codec-decoding measurements.

## PDF rendering and codecs

Production PDF rendering stays CPU. Resident model handoff cut four-page tensor
downloads by ~99.8%, but full PDF embedding comparisons varied/reversed by process
order. **No overall PDF throughput improvement is qualified.**

The benchmark-only Metal painter supports simple opaque fills/images with exact
CPU fallback for unsupported pages. Snapping fractional raster extent fixed an
initial scan pixel failure. Tile culling, preserved paint order, bounded reusable
buffers and overlapped private-reader preparation improved mixed render-only
~62→38 ms, roughly matching parallel CPU. Synthetic fill gains do not establish
real-document coverage. CPU parsing/codecs and the host raster copy remain.

Borrowed JPEG Huffman prefix tables improved isolated scan decode ~9%, with exact
RGBA at four resolutions. WebP lossless/compressed-alpha prefix changes retained
exact fixture pixels; ordinary VP8 is unchanged. A VP8 normalization trial was
slower and rejected. Codec gains did not qualify end-to-end embedding speedups.

## OCR correctness and rejected speedups

Row-dependent matrix arithmetic intermittently changed a fragile newline/EOS
choice, omitting a paragraph despite normal stop status. At sequence length
1,011 the free-running trace selected EOS rather than newline, losing 204
characters/41 tokens. Canonical-prefix diagnostics concealed accumulated drift;
scoped rollbacks/EOS confirmation did not establish a robust repair.
Retaining the complete prepared singleton operation family fixed the serving
path. Faster incomplete matrix runs are excluded from speed claims.

The final invariant Q4/tail/QKV/carrier/head-frame configuration reduced mean
recognition time by 12.8% with fresh ordinary brackets. Shared-weight kernels were
mostly slower and excluded; widened accumulators changed bits and were rejected.
Recorded native conditions match 128 page observations/79,222 completion tokens,
including reversed order and memory instrumentation. These are transcript parity
gates against ordinary output, not certified OCR accuracy. Ordinary itself failed
one diagram page; the expanded text corpus does not erase that failure.

No Antfly full-corpus MTP gain is qualified. Less drafting or fewer submissions
still left verification/recovery cost; cached seeds initially failed paragraph
parity, and deferred recovery increased verification work. MLX F32 improved a
restricted seven-page workload with overlapping ranges; different weights,
precision/output and a capped eighth page prevent runtime ranking.

## Qualification boundaries

Rendering/startup are excluded from OCR recognition. Embedding benchmarks use
separate checkpoints/scopes. Amortized seconds/page is not individual latency.
Diagnostics/probes are excluded from timing; sampled RSS and Metal allocations
overlap and cannot be added. Frozen-runtime gates remain historical after merges.
All OCR experiments stay opt-in. Replicate gains on idle hosts and representative,
manually checked pages/audio before broader promotion; Apple generation is unmeasured.
