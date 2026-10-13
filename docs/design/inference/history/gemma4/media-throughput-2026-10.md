# Media embedding throughput investigation, October 2026

> Summary of the dated media diary in [BATCHING.md@144f7dc](https://github.com/antflydb/antfly/blob/144f7dc49c17bb4b9b747b58d8ae2a81b918f3e6/zig/pkg/inference/BATCHING.md#gpu-residency-for-media-embeddings) and [PDF report@144f7dc](https://github.com/antflydb/antfly/blob/144f7dc49c17bb4b9b747b58d8ae2a81b918f3e6/docs/design/inference/gemma4-pdf-performance.md). Living contracts: [BATCHING.md](../../../../../zig/pkg/inference/BATCHING.md#gpu-residency-for-media-embeddings). Current benchmark guides: [vision](../../gemma4-vision-throughput.md), [PDF](../../gemma4-pdf-performance.md), [audio](../../audio-throughput.md), [WebP](../../webp-preparation-performance.md). Generative OCR has [separate history](ocr-throughput-2026-10.md).

Outcome: resident activation/prompt handoffs remove avoidable transfers; qualified
vision attention/frame changes substantially improve embedding throughput.
GPU PDF rendering remains a benchmark prototype, and no controlled full PDF
embedding win follows from its render-only or codec results. Audio gains are
separately qualified and do not imply batched conformer execution.

## Residency contracts and early qualification

CLIP's eager tensor path retains patch/CLS/position/head layouts and projection,
exporting only the final vector. Small native/Metal fixtures at batches 1/2/4
check ownership, host-export rejection, parity and zero intermediate readbacks.
Gemma vision similarly retains normalization, spatial rotary transforms, pooling
and projection; its learned offsets may be CPU metadata. Early Gemma tests
passed 118 checks with three optional skips; this was synthetic residency evidence,
not end-to-end throughput. CUDA fixtures cross-compiled without hardware execution.

Gemma audio added resident DFT/magnitude/mel/log and masks, preserving the CPU
feature convention and 480,000-sample bound. CLAP retains the Swin chain after
its host feature/AFF frontend. CTC exports compact choices/confidences, preserving
blank/repeat/dictionary semantics. Qwen2 uses spatial/temporal vision RoPE;
Qwen3 keeps position/prompt activation operations resident while public deepstack
arrays and opt-in hashes still export. The initial combined regression selection
passed 320 checks with fifteen optional skips; eleven native/Metal media fixtures
passed and five CUDA fixtures skipped. These are operation/transfer checks.

## Image residency and initial batches

Apple M4 Max (32 GPU cores, 36 GiB), BF16 HF EmbeddingGemma2; ten warmups and
100 synchronized samples per case. Prepared square geometry: 2,304 patch tokens,
256 projected tokens per image. Each batch used its own paired singleton control.

| Requested/actual vision rows | Batch p50 / p95 | Paired throughput ratio | Device-owned tensor peak |
| --- | --- | --- | --- |
| 1 / 1 | 1.459 / 1.517 s | 1.015× | 155.3 MiB |
| 2 / 2 | 2.868 / 3.211 s | 1.026× | 310.5 MiB |
| 4 / 4 | 5.287 / 5.350 s | 1.109× | 621.0 MiB |
| 8 / 8 | 12.449 / 12.978 s | 0.971× | 1,242.1 MiB |

Decision: keep serving at four. Preliminary residency alone improved singleton
p50 by 1.35–1.42× and reduced vision readbacks 47→1 final projected export.
Later full prompt/encoder residency removes that export too. Early image/photo
and borrowed-raster embedding observations included host contention, particularly
the four-row full-embedding comparison; they are local observations, not universal
latency/throughput claims. All offline pairs preserved finite output/shape and
cosine >= 0.9999. [Image evidence](../../gemma4-image-performance.json).

The early HTTP sweep completed 400/400 requests at client concurrency 1/2/4/8,
matching singleton vectors. Throughput stayed ~0.49–0.56 images/s while p95 grew
2.19→37.57 s; additional clients did not demonstrate a throughput win. Per-item
truncated-codec bodies retained healthy indexes; malformed headers failed request
validation. Cold observations had existing system caches and were not machine-cold
qualification. The selected native/Metal tests passed 165 cases with ten skips.

## Vision Flash, frames and preparation overlap

Model-qualified head-dimension-64 Flash attention and sixteen per-layer Metal
frames improved projector p50 by 4.29–4.86× across square/landscape rows 1/4.
Frames alone were exact; Flash projected cosine was >= 0.99999957.
Eight-row borrowed-raster full embedding improved 11.827→2.377 s for square PNG
and 12.128→2.388 s for photo JPEG, with final cosine >= 0.999999962.
Only eight final vectors downloaded (24,576 bytes). These use two warmups/five
samples, not the earlier 100-sample campaign, and do not qualify PDF throughput.

One bounded future CPU wave overlaps preparation under the owned model gate;
no speculative GPU reservation is acquired and every exit joins the producer.
Lookahead on/off vectors matched exactly. Independent JPEG decoding showed
large timing drift; the observed pairwise gains did not establish a stable
percentage for overlap. HTTP checks at default/larger scratch budgets passed
37 requests each and verified actual two-/four-row cohorts, indexed codec errors,
geometry identity and reduced-dimensional vectors. Many concurrent arrivals
remained singletons at the 200-µs broker window; no saturation claim follows.
204 tests passed, seven optional fixtures skipped. [Vision evidence](../../gemma4-vision-throughput.json).

## PDF resident handoff

Round-robin real/synthetic pages tested parser/rendering through final vectors,
with independent bounded readers and no image encode/decode round trip. Real
inputs included Attention and the W3C scanned OCR example; synthetic fills/images
are separate GPU-eligibility fixtures. The initial scan pixel gate failed
(maximum byte error 255, MAE 4.12): fractional page extent differed from CPU's
integer snapped raster plan. Matching the snapped origin/extent fixed that gate;
subsequent selected GPU pixels matched exactly, paper text fell back to CPU.

Four-page resident handoff reduced downloads to 12,288 bytes/four readbacks
from ~7.4–7.5 MB; every vector cosine >= 0.9999999999992. Initial batch-four
CPU-host/resident p50 11.434/16.744 s moved to 8.743/9.133 s in the reverse-order
repeat. Contention and three samples preclude declaring a handoff regression or
speedup. Initial render-only parallel CPU/GPU was 42.6/77.6 ms. Rendering was a
small part of multi-second embedding. [PDF evidence](../../gemma4-pdf-performance.json).

## GPU renderer optimization

32×32 tiles with conservative bounds preserve paint order; list overflow uses
the bounds-culled kernel. Four reusable synchronous buffer slots are capped at
512 MiB. Independent workers overlap published GPU scenes with CPU fallback,
retaining bounded scratch through cleanup. No prepared-scene/raster cache removes
work from timing. Production CPU rendering was not replaced.

Two warmups/20 passes, both process orders, old/new pinned binaries:

| Input group | Old GPU p50 | New GPU p50 | Parallel CPU p50 |
| --- | --- | --- | --- |
| Mixed four pages | 61.6–62.0 ms | 38.0–38.1 ms | 38.7–39.3 ms |
| Synthetic fill grid | 7.4–7.5 ms | 0.8–1.0 ms | 6.6 ms |
| Real scan | 35.5 ms | 37.2–37.5 ms | 38.7–39.0 ms |

Mixed GPU command time dropped ~10.5→0.67 ms. The scan remained CPU-preparation
bound (~36.6 ms); the new bounded private reader added work relative to the old
retained-reader path. The mixed render batch improved ~1.6× vs old GPU and
roughly matched parallel CPU, with exact selected pixels. Four actual GPU tests
and 24 Python checks passed. No full embedding speedup was qualified.
[Renderer evidence](../../gemma4-pdf-render-performance.json).

## JPEG and WebP preparation

CPU sampling found JPEG Huffman lookup/reconstruction dominated scan preparation.
Borrowed eight-bit prefix tables avoid per-symbol table copies; long codes,
short tails, stuffing and restart behavior retain canonical semantics. A real
scan ABBA decode comparison was ~9% faster (40.8–44.5→37.1–40.5 ms) with exact
RGBA at full/1536/768/384 resolutions. Paired render-only scan reductions were
12–24%, but embedding times reversed direction by process order (10.58→8.76 s,
then 9.86→10.64 s); no overall PDF embedding gain. All 172 direct JPEG tests
passed in ReleaseSafe. [Preparation evidence](../../gemma4-pdf-preparation-performance.json).

WebP VP8L/compressed-alpha decoding borrowed prefix tables, added eight-bit
short-code lookup/canonical long ranges and bounded byte-fragment bit reads.
Ordinary VP8 remained unchanged. Across twelve valid fixtures in both orders,
all 96 cases retained exact RGBA/dimensions; tiny lossless fixture gains were
41–50× at one worker, while compressed-alpha gains were 3.8–4.2×. These are
fixture codec timings with thread startup included, not full embedding gains.
A VP8 leading-zero normalization trial regressed some cases and was rejected.
91 direct tests passed, including exhaustive 16-bit prefix inputs.
[WebP evidence](../../webp-preparation-performance.json).

## Audio preparation and embedding batches

The baseline already included media residency. Gemma audio projected rows remain
resident through prompt packing; **downstream embedding encoder** cohorts are
length-qualified at four rows, <=25% padding, <=1,024 tokens. The conformer still
executes per clip. CLAP retains native batches up to eight and its singleton
fusion/fallback. Both prepare one bounded future CPU wave and join on every exit.

M4 Max/36 GiB, real EmbeddingGemma2 and clipclap, alternating distinct 0.4/0.5-s
WAVs, two warmups/five measurements, decode through final vectors:

| Clips | Gemma baseline→candidate p50 | CLAP baseline→candidate p50 |
| --- | --- | --- |
| 1 | 0.1523→0.1527 s | 0.2034→0.2080 s |
| 4 | 0.6276→0.5831 s | 0.3964→0.3953 s |
| 8 | 1.2679→1.1677 s | 0.6293→0.6194 s |
| 16 | 2.5327→2.3404 s | 1.0184→0.8998 s |

Gemma vectors cosine >= 0.999999999999; CLAP exact. Mixed lengths up to 25 s,
48 kHz stereo, PCM/encoded/lookahead variants, MP3/AAC/Ogg and indexed codec/
duration errors preserved results. 39 HTTP requests passed with increased weighted
request admission; lower capacity produced expected 503s. CPU compilation was
active: retain the local limits and exclude an earlier unpaired apparent large
gain. No CUDA or audio-generation claim. [Audio evidence](../../audio-throughput.json).

## Decisions and remaining qualification

Keep exact indexing, bounded leases/worker joins and backend fallbacks in the
living design. Keep production PDF rendering CPU and distinguish parse/decode,
render-only, projector, embedding and text-generation measurements. Future GPU
raster/preprocessing must eliminate host copies without changing model pixels;
fonts/transparency/complex clipping need their own coverage/pixel gates.
Replicate the vision/audio gains and isolate overlap on an idle host before
claiming serving-wide gains. Machine-readable reports remain intact; the full
pre-cleanup prose is available at the pinned links above.
