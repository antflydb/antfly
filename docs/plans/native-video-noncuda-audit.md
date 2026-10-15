# Native video non-CUDA completion audit

The eight requested work groups are implemented and qualified within their
documented codec, transport, fixture and platform contracts. This audit maps the
original scope to reviewable evidence. Receipts bind the exact source/binary they
measured; later unit tests do not silently refresh earlier performance or
pretrained epochs. Application relevance belongs to Antfly users; these model
gates preserve pinned reference vectors, cosine scores, rankings and top-k sets.

| Requested work | Current implementation and evidence | Acceptance and scope |
|---|---|---|
| WebM model ingestion, sampling and pretrained CPU/Metal parity | `embedding_gemma2_video.zig` connects WebM logical indexing and native VP8/VP9/AV1 reconstruction to preparation, batched vision and the projector. [WebM evidence](../../zig/lib/video/testdata/webm-validation/README.md) includes eight vectors and three rankings on CPU/Metal/Linux. [Expanded formats](../../zig/lib/video/testdata/gbr-model-validation/README.md) include 12 video and two query vectors. | The refreshed full-F32 Metal gate passes for the five-video WebM corpus (eight vectors/three rankings) and twelve-video depth/chroma corpus (14 vectors/two rankings); retain historical and measured binary source scopes. |
| Broader streams, sustained coverage-guided fuzzing and corruption recovery | [Sequence campaigns](../../zig/lib/video/testdata/sequence-fuzz-validation/README.md), [VP8 campaign](../../zig/lib/video/testdata/vp8-sequence-validation/README.md), H.264 real-picture differentials and [explicit restart recovery](../../zig/lib/video/testdata/recovery-validation/README.md) exist. The recovery walker reports gaps; model inference stays strict. | [Independent Big Buck Bunny content](../../zig/lib/video/testdata/real-content/README.md) adds three movie scenes across VP8/VP9/AV1. Both macOS and actual Linux match every sample of 324 pictures; all streams/hashes reproduce exactly. This broadens source diversity while retaining bounded campaign scopes. |
| AV1 profiles, 12-bit, monochrome, 4:2:2/4:4:4, Annex B and scalable layers | The maintained Zig decoder exposes profiles 0–2, native-depth/chroma output, Annex B normalization and operating-point/layer selection. [Extended decoder goldens](../../zig/lib/video/testdata/av1-extended/README.md), [chroma model qualification](../../zig/lib/video/testdata/av1-chroma-model-validation/README.md) and [scalable presentation](../../zig/lib/video/testdata/av1-layer-model-validation/README.md) record independent decoded outputs and model preservation. | The twelve-video 8/10/12-bit GBR corpus passes the explicit full-F32 Metal gate (maximum error 3.7626e-7). The historical nine-video chroma and ten-video scalable-layer reports preserve vectors but fail the overall Metal retry gate under a 4 GiB process budget. The final [vision-weight gate](../../zig/lib/video/testdata/vision-weight-validation/README.md) closes both original 4096 MiB suites with unchanged numerical and lifecycle/load requirements. |
| Realistic benchmarks, measured portable SIMD and less reference copying | [Large codec measurements](../../zig/lib/video/testdata/benchmarks/webm/README.md), [SIMD qualification](../../zig/lib/video/testdata/simd-validation/README.md), [reference sharing](../../zig/lib/video/testdata/reference-storage-validation/README.md), selected-output materialization and [attention measurements](../../zig/lib/video/testdata/attention-model-validation/README.md) exist. Runtime buffer lifetime/initialization telemetry now measures previously excluded allocations. | Bounded model-owned vision/projector reuse passes final production gates: warm clip initialization falls from 9.695 GB to 27.477 MB, with zero net live owned bytes/objects across four requests. The new F32 route passes the complete reference/load gate; shared-host measurements are diagnostic rather than a speed claim. |
| Actual Linux decoder execution | Decoder/model policies run as static ARM64 Linux executables in the recorded offline VM; these are execution results, not cross-compilation claims. The complete decoder suite passes 252 tests (29 platform skips); final portable model/admission checks pass 54 (eight Metal skips). The separate pretrained WebM execution passes all eight vectors, three rankings and lifecycle/load gates. | Preserve the architecture scope: native x86 hardware performance is not proved by ARM64 execution or x86 emulation. Re-run affected pretrained gates when native arithmetic or preparation changes. |
| Logical fields/partitions, timestamp sampling and long windows | Logical AVC/WebM picture indexes, `ReaderSelection`, timestamp/source-time policies, rolling windows and pending-field frontiers are integrated. [Frontier evidence](../../zig/lib/video/testdata/live/field-frontier-validation/README.md), [window coordinator](../../zig/lib/video/testdata/live/coordinator-validation/README.md) and later incremental/dynamic-source receipts qualify ownership, source clocks and commit boundaries. | The 52-second reference clip and final configured streamed TS checks exercise long-video and source-time paths; six streamed CPU/Metal vectors are byte-exact against buffered inference, including retained-output/prefix retry. Earlier field/partition policy receipts retain their source scopes. |
| Device normalization and scheduling across requests | Metal token composition, backbone inputs and `maskedMeanNormalizedRows` keep the response boundary on device until final readback. Bounded preparation starts before the model gate and drains on cancellation/abandonment. [Concurrent attention/model evidence](../../zig/lib/video/testdata/attention-model-validation/README.md) and [cancellation evidence](../../zig/lib/video/testdata/cancellation-model-validation/README.md) cover shared admission and correct retry. | The fresh full-clip end-to-end numerical/load gate passes. Runtime-only buffer gauges exclude preparation textures/buffers, decoder surfaces and opaque driver storage; report that scope rather than a physical whole-pipeline peak. |
| Inherited DASH addressing, periods/representations and broader live/HLS | Native DASH inheritance, templates, selected representations, stable period/live cursors, partial fMP4 growth and authenticated range transport are implemented. HLS includes master selection, delta/blocking reloads, native AES-CBC, fMP4 part/parent reconciliation and streamed normalized AVC TS. [Media design](../../zig/lib/media/MEDIA.md) links exact receipts; [TS model delivery](../../zig/lib/video/testdata/ts-http/README.md) preserves six streamed windows on CPU/Metal/Linux. | Keep source/origin and codec restrictions explicit. TS LL-HLS parts remain unsupported; current transport gates do not establish arbitrary origin interoperability. |

## Final acceptance

The [final Metal constants qualification](../../zig/lib/video/testdata/vision-weight-validation/README.md)
closes the last constrained production failures. All seven final checks pass:
both original 4096 MiB AV1 suites, full-clip/WebM/depth-chroma comparisons, and
native CPU/Metal streamed TS. Numerical, rankings/top-k, cancellation, retry,
half-close, concurrent completion and cleanup gates remain unchanged. All 66
actual Metal device tests pass. Actual offline Linux ARM64 executes 54 final
portable model tests with eight Metal skips. Source/binary fingerprints and raw
logs are in `vision-weight-validation/qualification.json`.

EmbeddingGemma 2 uses full-F32 tiled vision attention, preserving float inputs and
softmax probabilities. The 52-second clip's maximum coordinate error is
8.7964e-7, with 256 dedicated tiled dispatches. Four requests at concurrency two
return exact repeat vectors and release admission. WebM and depth/chroma maxima
are 6.2213e-7 and 3.7626e-7; both original 4096 MiB suites pass at 3.9023e-7.
No injected library or disabling precision override is used in accepted runs.

The Metal provider retains 114 immutable vision/projector matrices. Vision and
language each have a 640 MiB ceiling, share the model/process controller and
combined 512-entry cap. Relevant pressure reclaims idle vision slots first and
language slots only if the unchanged request remains denied. Each class is
attempted once with nonblocking native locks and cancellation checks. Exclusive
owned weights are discarded and exact credits released; checkpoint aliases,
shared zero biases and active frames stay valid. Unified-memory BF16 widens
with portable Zig SIMD directly into the final shared allocation. Owned HF vision
frames retire activation views after their existing wait; outer frame owners
retain their completion boundary. No process cap, reservation, tolerance or
external retry limit changes.

First long-clip initialized bytes fall from 9,758,012,520 to 698,315,880; warm
clips fall from 9,695,085,672 to 27,477,096 (99.72% less). Steady owned allocation
increases from 632,222,724 to 1,240,134,660 bytes. Four warm requests have zero
net live owned bytes/objects. Isolated vision-view retirement lowers the first
AV1 tracked owned peak 56.0%, retaining a packed-F32-identical vector. These
are logical initialization and runtime allocated-buffer observations, excluding
preparation textures/buffers, decoder surfaces and opaque driver storage.
They do not establish physical residency, bus traffic or shared-host speedup.

## Evidence boundaries

The complete actual Linux decoder receipt has 252 passes, 29 platform skips and
zero failures. Its source audit matches 255 of 256 library files exactly; the
sole difference moves unchanged fixture paths out of a test helper for license
inspection. The full diff is retained; decoder runtime is unchanged. The Linux
pretrained WebM server has its own earlier epoch, with eight vectors, three
rankings and lifecycle/load gates passing at maximum error 7.8232e-7. Later
Metal arithmetic does not execute on Linux; the final generic admission changes
are covered by the fresh 54-pass Linux suite. The executed pretrained AArch64
ELF has no interpreter or DT_NEEDED libraries. ARM64 execution does not establish
native x86 hardware performance.

Historical failed/control results remain failed, including the half-input
precision regression, early cache variants and partial pressure fixes. Debug
success does not qualify production. The final accepted source is explicitly
bound separately. The diagnostic loader audit rejects inactive shell-injected
controls; no Darwin allocator-relief hook ships.

Qualification covers bounded corpora, campaigns and declared supported subsets,
not arbitrary codec conformance or origin interoperability. TS LL-HLS parts
remain outside the declared streamed TS contract. CUDA is excluded. Reference
preservation does not establish end-user application relevance. These limits
are retained without treating them as unimplemented requirements of the eight
requested work groups.
