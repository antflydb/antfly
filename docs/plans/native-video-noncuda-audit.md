# Native video non-CUDA completion audit

This tracks the original eight requested work groups against implementation and
qualification evidence. It is an incomplete audit, not a completion declaration.
Receipts describe the exact source/binary they measured; later green unit tests
do not automatically refresh older pretrained or performance measurements.
Application relevance is evaluated by Antfly users; the model gates here require
preservation of pinned reference vectors, scores, rankings and top-k sets.

| Requested work | Current implementation and evidence | Qualification still needed |
|---|---|---|
| WebM model ingestion, sampling and pretrained CPU/Metal parity | `embedding_gemma2_video.zig` connects WebM logical indexing and native VP8/VP9/AV1 reconstruction to preparation, batched vision and the projector. [WebM evidence](../../zig/lib/video/testdata/webm-validation/README.md) includes eight vectors and three rankings on CPU/Metal/Linux. [Expanded formats](../../zig/lib/video/testdata/gbr-model-validation/README.md) include 12 video and two query vectors. | The refreshed full-F32 Metal gate passes for the five-video WebM corpus (eight vectors/three rankings) and twelve-video depth/chroma corpus (14 vectors/two rankings); retain historical and measured binary source scopes. |
| Broader streams, sustained coverage-guided fuzzing and corruption recovery | [Sequence campaigns](../../zig/lib/video/testdata/sequence-fuzz-validation/README.md), [VP8 campaign](../../zig/lib/video/testdata/vp8-sequence-validation/README.md), H.264 real-picture differentials and [explicit restart recovery](../../zig/lib/video/testdata/recovery-validation/README.md) exist. The recovery walker reports gaps; model inference stays strict. | [Independent Big Buck Bunny content](../../zig/lib/video/testdata/real-content/README.md) adds three movie scenes across VP8/VP9/AV1. Both macOS and actual Linux match every sample of 324 pictures; all streams/hashes reproduce exactly. This broadens source diversity while retaining bounded campaign scopes. |
| AV1 profiles, 12-bit, monochrome, 4:2:2/4:4:4, Annex B and scalable layers | The maintained Zig decoder exposes profiles 0–2, native-depth/chroma output, Annex B normalization and operating-point/layer selection. [Extended decoder goldens](../../zig/lib/video/testdata/av1-extended/README.md), [chroma model qualification](../../zig/lib/video/testdata/av1-chroma-model-validation/README.md) and [scalable presentation](../../zig/lib/video/testdata/av1-layer-model-validation/README.md) record independent decoded outputs and model preservation. | The twelve-video 8/10/12-bit GBR corpus passes the explicit full-F32 Metal gate (maximum error 3.7626e-7). Historical failing preparation reports remain historical; separate monochrome/chroma/layer receipts retain their recorded source scope. |
| Realistic benchmarks, measured portable SIMD and less reference copying | [Large codec measurements](../../zig/lib/video/testdata/benchmarks/webm/README.md), [SIMD qualification](../../zig/lib/video/testdata/simd-validation/README.md), [reference sharing](../../zig/lib/video/testdata/reference-storage-validation/README.md), selected-output materialization and [attention measurements](../../zig/lib/video/testdata/attention-model-validation/README.md) exist. Runtime buffer lifetime/initialization telemetry now measures previously excluded allocations. | The initial telemetry experiment reports about 9.76 GB of logical initialized-buffer copies for the 52-second clip. Trace and reduce repeated media-weight staging while retaining bounded, model-owned admission. The new F32 route passes the complete reference/load gate; shared-host measurements are diagnostic rather than a speed claim. |
| Actual Linux decoder execution | Decoder/model policies run as static ARM64 Linux executables in the recorded offline VM; these are execution results, not cross-compilation claims. Current full-F32 portable fallback qualification passes 52 tests with five platform skips, plus three telemetry tests. | Preserve the architecture scope: native x86 hardware performance is not proved by ARM64 execution or x86 emulation. Re-run affected pretrained gates when native arithmetic or preparation changes. |
| Logical fields/partitions, timestamp sampling and long windows | Logical AVC/WebM picture indexes, `ReaderSelection`, timestamp/source-time policies, rolling windows and pending-field frontiers are integrated. [Frontier evidence](../../zig/lib/video/testdata/live/field-frontier-validation/README.md), [window coordinator](../../zig/lib/video/testdata/live/coordinator-validation/README.md) and later incremental/dynamic-source receipts qualify ownership, source clocks and commit boundaries. | Current streaming/model measurements must continue to cover these policies; a short generated fixture alone is not a broad long-video qualification. |
| Device normalization and scheduling across requests | Metal token composition, backbone inputs and `maskedMeanNormalizedRows` keep the response boundary on device until final readback. Bounded preparation starts before the model gate and drains on cancellation/abandonment. [Concurrent attention/model evidence](../../zig/lib/video/testdata/attention-model-validation/README.md) and [cancellation evidence](../../zig/lib/video/testdata/cancellation-model-validation/README.md) cover shared admission and correct retry. | The fresh full-clip end-to-end numerical/load gate passes. Runtime-only buffer gauges exclude preparation textures/buffers, decoder surfaces and opaque driver storage; report that scope rather than a physical whole-pipeline peak. |
| Inherited DASH addressing, periods/representations and broader live/HLS | Native DASH inheritance, templates, selected representations, stable period/live cursors, partial fMP4 growth and authenticated range transport are implemented. HLS includes master selection, delta/blocking reloads, native AES-CBC, fMP4 part/parent reconciliation and streamed normalized AVC TS. [Media design](../../zig/lib/media/MEDIA.md) links exact receipts; [TS model delivery](../../zig/lib/video/testdata/ts-http/README.md) preserves six streamed windows on CPU/Metal/Linux. | Keep source/origin and codec restrictions explicit. TS LL-HLS parts remain unsupported; current transport gates do not establish arbitrary origin interoperability. |

## Current precision and measurement work

The primary Metal bridge now observes all 712 maintained buffer creation
expressions in `metal_kernels.m`. Associated ARC tokens follow actual object
lifetime, including retained views/commands. Owned allocatedSize and logical
aliases have distinct live/peak gauges. Initialized-buffer bytes are separate
from explicit upload/download/copy counters. These are allocated-resource
observations, not physical residency or measured bus traffic.

The first current-production 52-second clip measurement completed all lifecycle
and concurrency checks but failed the unchanged reference gate: maximum video
coordinate error was 0.00022155. Disabling only the newly merged Gemma4 half-input
vision Flash route on the same binary restored the complete gate (1.1791e-6).
Both reports and the exact measured tool snapshot are retained in
[buffer qualification](../../zig/lib/video/testdata/metal-buffer-validation/README.md).

EmbeddingGemma 2 now selects a separate full-F32 32-query tiled attention route.
It preserves float Q/K/V and softmax probabilities, reuses Q scratch for tail
output, and uses 25.25 KiB of threadgroup storage. Other Gemma4 callers keep their
existing policy. The GPU oracle compares generic F32 and independent F64 results
for two cohorts, both heads and first/middle/final rows. The post-merge 60-case device suite
passes. The production end-to-end reference/load rerun passes without an override: video
maximum coordinate error is 8.7964e-7, all four score errors are below 1.34e-7,
and 256 full-F32 tiled dispatches are observed. Four requests at concurrency two
complete with exact repeat vectors, matched server completion accounting and
released admission. The warmed load has zero net live-buffer bytes or objects.
This is shared-host diagnostic timing, not a capacity claim. The measured binary
predates the subsequent GLiNER-only main merge; exact source scopes are recorded
with the receipt. The refreshed WebM and expanded-format gates also pass, including lifecycle and
concurrency checks: maximum errors 6.2213e-7 and 3.7626e-7 respectively,
with exact rankings and full top-k overlap for all three/two query sets.

The production benchmark build also now honors the existing `-Dstrip=true`
option. An active compiler sample identified LLVM debug-record processing; the
original unstripped build was intentionally stopped after this diagnosis. The
replacement stripped build preserves runtime code and numerical tolerances.

## Next acceptance work

1. Preserve the completed production F32 vector/score, lifecycle, admission and
   completion gates without a disabling environment override.
2. Preserve the refreshed Metal WebM and expanded-format comparisons after the
   precision policy change, keeping CPU/Metal/Linux scope explicit.
3. Reduce measured repeated media-weight staging. The existing immutable cache
   admits language weights and explicitly excludes media; extending residency
   requires checkpoint ownership, shared admission, cancellation and unload tests.
4. Preserve the new independently sourced real-content qualification (324 exact
   pictures on macOS/Linux) and use actual source/seed bindings for further campaigns.

The full non-CUDA objective remains active. This audit does not promote bounded
fixtures to arbitrary codec conformance, historical binaries to current ones,
logical transfer counts to bus traffic, or reference preservation to relevance.
