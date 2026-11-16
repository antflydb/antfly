# EmbeddingGemma2 end-to-end media on L4

This campaign compares the production native CUDA media pipeline, with and
without the opt-in global-attention GEMM route, against Antfly's experimental
native PJRT CUDA artifact runner. It covers encoded file read and decode,
preprocessing, tokenization, media tower, shared text encoder, pooling,
normalization and synchronized 768-dimensional vector readback. These are direct
pipeline measurements; HTTP and server scheduling are excluded.

The reference additionally includes Python CPU preprocessing and worker IPC.
Its GPU model executes through the native Zig PJRT C API, with BF16 checkpoint
parameters resident across calls. It does not time a Python GPU model. CPU
preprocessing, worker receive/upload and worker execute/readback are retained
separately. Neither side caches decoded media or projected features.

## Method

NVIDIA L4 SM89, 23,034 MiB, driver 580.159.03. Three fresh-process rounds rotate
the order of native control, native candidate and PJRT. Each case gets three
warmups and eight measured calls per round: 24 samples per case per arm. The OS
file cache is warm. Loading, compilation/deserialization, warmup and request
cleanup are excluded from latency. Process-tree PSS plus per-process NVML are
sampled every 0.2 seconds, including startup. Only one arm executes on the GPU
at a time. The native rebuild and offline oracles completed before measurement.

The native arms hold `EXTENDED_FLASH`, `PACKED_PROJECTIONS`, `GRAPH_REPLAY` and
`FUSED_BACKBONE` at 1 and `TUNED_GEMM` at 0. Only the candidate sets
`ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_GLOBAL_GEMM=1`. All other EmbeddingGemma2
CUDA overrides are removed. The route is eligible only for caption plus image
(1157 expanded encoder tokens) and 30-second audio (754 tokens). The shorter
cases exercise fallback behavior.

The six deterministic fixtures are a 64×96 PNG pattern, a 768×1024 JPEG pattern,
the JPEG with a repeated natural-language caption, and nonzero 1/12/30-second
mono 16 kHz PCM16 WAV signals. This qualifies these geometries and file formats;
it is not a natural-media retrieval-quality evaluation, batching/concurrency
qualification, or a test of video, resampling, additional codecs or all sizes.

The complete 1,376-tensor BF16 checkpoint has SHA256
`197a32965d4b1105faf060417baa899e193fb73cd401f42ec9295234d5553d79`.
Both media towers are included in the reference inventory. Transformers 5.23.0
source was verified against the installed wheel RECORD. Its actual source
hashes are retained; the older project reference source pin is not asserted.

PJRT's full tower/encoder graphs resolve shape-dependent padding removal and
placeholder positions from processor metadata. All layers remain in HLO.
The shape wrapper matches the upstream CPU forward exactly on all six cases,
in BF16 and FP32. Its RMSNorm `type_as` → `to(dtype=...)` rewrite also matches
upstream bitwise in eight separate BF16/FP32 checks. Six GPU executables are
compiled and serialized in the first reference round. Later rounds start new
processes, upload weights anew and deserialize those same hashed executables.

## End-to-end latency

Medians in milliseconds across all 24 measured calls per arm. Lower is better.

| Input | Expanded tokens | Native control | Native candidate | PJRT | PJRT time / candidate time |
|---|---:|---:|---:|---:|---:|
| PNG, 64×96 | 264 | 108.54 | 108.72 | 90.34 | 0.83× |
| JPEG, 768×1024 | 270 | 138.58 | 138.69 | 101.74 | 0.73× |
| Caption + JPEG | 1,157 | 154.33 | 145.34 | 117.02 | 0.81× |
| Audio, 1 second | 29 | 9.29 | 9.29 | 13.60 | 1.46× |
| Audio, 12 seconds | 304 | 21.58 | 22.06 | 31.32 | 1.42× |
| Audio, 30 seconds | 754 | 54.85 | 51.55 | 59.08 | 1.15× |

The new route reduces caption/image latency by 5.8% and 30-second audio by
6.0% (1.062× and 1.064× speedups). It is ineligible for the other four cases.
Their outputs remain bitwise identical; the observed 12-second timing is 2.2%
slower in the candidate. Round medians and individual samples are retained.
Native CUDA wins the audio pipeline comparison, while PJRT wins images by
1.20–1.36×. These results do not establish a universal native media speedup.

The reference's median component observations explain why pipeline performance
must be distinguished from GPU execution:

| Input | CPU preprocessing | Worker receive/upload | Worker execute/readback |
|---|---:|---:|---:|
| PNG | 11.38 | 7.85 | 68.63 |
| JPEG | 22.91 | 7.66 | 68.56 |
| Caption + JPEG | 26.40 | 7.91 | 79.62 |
| Audio, 1 second | 3.17 | 0.49 | 9.51 |
| Audio, 12 seconds | 16.91 | 0.78 | 13.28 |
| Audio, 30 seconds | 35.94 | 1.42 | 19.94 |

These are separate medians, not an additive decomposition of the median total.
Receive/upload includes pipe reads and allocations; execute/readback includes
synchronization and host readback. The native pipeline performs its audio
frontend on CUDA. In particular, its audio end-to-end win does not demonstrate
that its tower/encoder kernels beat XLA: the reference spends about 36 ms on
CPU preprocessing for the 30-second clip and 20 ms in worker execution/readback.

## Memory

GiB, sampled over complete process lifetimes, including loading and compilation
or deserialization. Combined footprint is the maximum simultaneous PSS + NVML,
not the sum of the two independently observed peaks.

| Arm | Peak GPU | Peak combined footprint |
|---|---:|---:|
| Native control, maximum of three rounds | 3.686 | 4.878 |
| Native candidate, maximum of three rounds | 3.695 | 4.890 |
| PJRT, compiling round | 2.314 | 5.292 |
| PJRT, deserializing rounds, maximum | 2.311 | 4.355 |

The new route adds 10 MiB of peak GPU memory in this suite. Native CUDA still
uses about 1.60× PJRT's GPU memory. Its combined footprint is also higher than
the deserializing reference rounds. PJRT's compiling-round host peak includes
compiler subprocesses; using that peak alone would obscure the memory gap.
The two implementations have different media-tower arithmetic/storage and
runtime inventories, so this comparison does not isolate allocator efficiency.

## Numerical qualification

The [original acceptance criteria](media-acceptance.json) were frozen before
collecting the campaign. Every vector must have 768 finite elements and norm
within 0.001 of one. Candidate versus native control requires cosine ≥0.9999
and maximum absolute difference ≤0.003. Both GPU paths are compared against
the independent upstream CPU BF16 oracle at cosine ≥0.999 and maximum absolute
difference ≤0.01. A failed case makes the campaign exit nonzero after saving
all measurements and its per-case status.

Native CUDA uses FP32 media-tower arithmetic with values from the BF16
checkpoint, followed by the BF16 shared encoder. PJRT uses BF16 tower arithmetic
with upstream FP32 boundaries. The [additional CPU FP32 diagnostic](media-fp32-diagnostic.json)
uses the same cosine/absolute thresholds and does not replace a failed BF16
gate. Precision differences must be considered when comparing speed, memory or
vector agreement.

All 432 measured vectors pass finiteness, dimension and norm checks. All native
candidate/control checks pass; the minimum cosine is 0.99991777 and maximum
absolute difference 0.00141909. The strict BF16 comparison passes four of six
cases across both GPU paths:

| Input | Native candidate / CPU BF16 cosine | PJRT / CPU BF16 cosine | BF16 result |
|---|---:|---:|---|
| PNG | 0.999961 | 0.999979 | Pass |
| JPEG | 0.999887 | 0.999908 | Pass |
| Caption + JPEG | 0.999906 | 0.999856 | Pass |
| Audio, 1 second | 0.999800 | 0.999869 | Pass |
| Audio, 12 seconds | 0.998736 | 0.997721 | Fail, both paths |
| Audio, 30 seconds | 0.999234 | 0.998815 | Fail, PJRT |

These failures are cosine failures; all maximum absolute differences stay below
0.01. All six cases on both paths pass the separate FP32 diagnostic. Native's
worst FP32 cosine is 0.999733; PJRT's is 0.999194. The 12-second native failure
also occurs in the control, whose vector is bitwise identical to the candidate.
The results are consistent with arithmetic precision differences; this campaign
does not isolate their complete numerical cause. The original BF16 gates stay
failed, and the campaign exits nonzero. **Full BF16 media qualification is not
claimed.** This campaign used explicit flags; [automatic default selection](AUTOMATIC_DEFAULTS.md)
is characterized separately.

## Dispatch traces

Separate Nsight Systems traces cover three forwards per selected input and are
excluded from the latency table. The candidate launches the dedicated normalized
global softmax 60 times for caption/image and 36 times for 30-second audio,
exactly four global layers × query tiles × three forwards. The control launches
it zero times. The existing vision softmax runs 144 times in both caption/image
traces; its accumulated profiled time is about 28.5 ms per forward in both.
That vision work remains a separate performance target. These traces establish
dispatch, not hardware-counter utilization or independent unprofiled timings.

## Reproduction and evidence

Build `build-embeddinggemma2-media-bench` from `zig/pkg/inference/build.zig` with
`-Doptimize=ReleaseFast -Dcuda=true -Dcuda-artifacts=sm89`; build `media-runner`
from `zig/lib/pjrt/build.zig`. Keep the worktree's `.zig-cache` and `zig-out`.
The clean native build completed all 15 steps; the separate PJRT runner build
completed all three. An earlier watch build installed a working artifact but
the watch service panicked afterward; the campaign uses the successful clean
rebuild. No build overlaps the measured campaign.

See [the media tool instructions](../../../../../lib/pjrt/tools/README.md#full-embeddinggemma2-imageaudio-pipeline)
for fixture generation, CPU oracle, HLO export and campaign commands. Raw logs,
vectors, inputs, HLO, compiled artifacts, frozen gates and memory timelines are
retained in `evidence/media-qualification/` in the experiment worktree. The
campaign hashes binaries, plugin, checkpoint, model/processor configuration,
tokenizer, sources, media and programs before and after execution. Protocol
fragmentation, truncation and timeout checks pass independently.

[Machine-readable results](media-l4.json) include all per-round medians,
BF16/FP32 comparisons, memory peaks, frozen acceptance criteria and input/program
identities. The raw campaign is `evidence/media-qualification/run-final/`.
