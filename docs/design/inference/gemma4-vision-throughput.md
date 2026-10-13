# Gemma4 vision throughput: attention, submission, occupancy and preparation

Dated decisions and rejected trials: [media throughput history](history/gemma4/media-throughput-2026-10.md#vision-flash-frames-and-preparation-overlap).

Implementation and local qualification, 2026-10-11. This follows the resident
multimodal work in PR #1086. The branch starts from `origin/main` at
`d3bbb9a0c6`, merges that PR, and measures the merged baseline `14f39270e8`.
Measurements use the local HF `google/embeddinggemma-2` checkpoint on Metal,
an Apple M4 Max with 36 GiB unified memory, and Zig 0.17 ReleaseFast.

## Execution changes

1. **Qualified Gemma4 Flash attention.** Same-grid vision cohorts have no
   padded patches, so their all-valid mask is unnecessary. A model-specific
   backend hook calls existing token-major, head-dimension-64 Flash attention
   for eligible sequences (257–4096 patches). Unsupported shapes/backends
   retain portable unmasked attention. Qwen's selection and dispatch counter
   stay independent. `TERMITE_METAL_DISABLE_GEMMA4_VISION_FLASH=1` allows A/B
   comparison; the existing global Flash disable also applies.
2. **One Metal command frame per vision layer.** Sixteen layer frames amortize
   submission/wait overhead and use the runtime's existing deferred buffer
   leases and reuse pool. A caller's active frame retains ownership. Failures
   cancel an owned frame before releasing intermediates. Set
   `TERMITE_METAL_DISABLE_GEMMA4_VISION_FRAMES=1` to compare standalone dispatch.
   This does not change precision or add packed QKV/FFN fusion.
3. **Geometry-qualified image-only broker.** Ordered image groups and direct
   borrowed raster/image calls can gather compatible rows across requests and
   documents on accelerated sessions. Header inspection computes both target
   grid axes without decoding.
   The queue matches loaded generation, task prefix, dimensions, source kind,
   resident/preparation options and grid. It retains the existing bounded wait
   (200 microseconds by default), earliest deadline, original indexes and raster
   provenance, at most four rows, resource limits and per-item codec failures.
   Policy fit checks decrease occupancy before enrollment: the local default
   1.5 GiB scratch policy fits two square rows; a 4 GiB policy fits four. A
   singleton that cannot use the optimized envelope keeps its original executor.
   Geometry is excluded from shard hashing so heterogeneous array enrollment
   remains atomic and cannot introduce cyclic leader/follower waits. Mixed or
   titled prompts retain their compatibility executor and published capability.
   `ANTFLY_GEMMA4_DISABLE_IMAGE_BROKER=1` disables this optimization;
   `ANTFLY_GEMMA4_IMAGE_BROKER_TRACE=1` reports broker cohort sizes and grids.
4. **Bounded CPU preparation lookahead.** Accelerator image/raster pipelines
   prepare at most one future wave while consuming the current GPU wave.
   Each stable slot has a 512 MiB bounded host allocator and host admission;
   speculative work never reserves GPU scratch. Encoded rows decode independently;
   borrowed rasters normalize/resize independently. The GPU permit is acquired
   under the model execution gate. The active call holds that gate while
   preparing and consuming its waves; queued callers reserve no preprocessing
   memory. Its future CPU producer works inside this owned lifetime. Vision scratch is estimated from prepared
   geometry; vision and encoder temporaries have disjoint lifetimes, so admission
   reserves their larger phase workspace plus retained/output storage. If
   speculation prevents the current wave from being admitted, it is drained
   and freed before retrying the current admission. On speculative admission
   failure, the next
   wave retries once after draining the current wave. Codec/control/runtime
   failures do not retry. Every exit joins the worker before borrowed sources,
   controls and allocators can expire. CPU and single-threaded execution remain
   sequential. Pipeline option `prepare_ahead=false` or benchmark flag
   `--prepare-ahead off` supplies the comparison path.

## Warm projector qualification

Two warmups and five synchronized measured iterations per case; file reads,
model load and image preparation are outside this projector scope.

| Image / rows | Baseline p50 | Flash only | Frames only | Flash + frames | Speedup |
| --- | ---: | ---: | ---: | ---: | ---: |
| Square / 1 | 1.475 s | 0.590 s | 1.332 s | 0.343 s | 4.29× |
| Square / 4 | 5.228 s | 1.242 s | 5.152 s | 1.179 s | 4.44× |
| Landscape / 1 | 1.592 s | 0.511 s | 1.500 s | 0.353 s | 4.50× |
| Landscape / 4 | 5.899 s | 1.258 s | 5.975 s | 1.215 s | 4.86× |

All projected outputs have baseline cosine at least 0.99999957; maximum absolute
projected-value difference is 0.02331. Frames alone match exactly. The independent
raw attention test uses distinct batched rows and compares Flash against generic
attention. Final embedding parity is assessed separately below: projected-vector
agreement alone does not establish final retrieval-quality agreement.

Four-row device-owned tensor peaks are approximately 621 MiB (square) and
631 MiB (landscape), essentially unchanged in the reported counter. Host bounded
workspace peaks are 174–175 MiB. These counters exclude model/RSS and some
provider allocations; they are not total GPU-memory measurements. The optimized
projector reports sixteen submissions and median measured frame GPU time of
756–773 ms for four rows. GPU frame time covers framed work only; wall time also
includes unframed projection, CPU work, submission and synchronized readback.

## Final embeddings and preparation overlap

Eight-row borrowed-raster calls execute two four-row GPU waves. The same two
warmups/five measurements compare the baseline with Flash, frames and lookahead:

| Input | Baseline p50 | Optimized p50 | Speedup | Minimum final cosine |
| --- | ---: | ---: | ---: | ---: |
| Square PNG | 11.827 s | 2.377 s | 4.98× | 0.999999987 |
| Photo JPEG | 12.128 s | 2.388 s | 5.08× | 0.999999962 |

Maximum final-vector differences are 2.06e-5 and 3.57e-5 respectively.
Only the eight final 768-float vectors are read back (24,576 bytes total);
there are no projected/intermediate downloads. Turning preparation lookahead
on/off preserves final vectors exactly and records one producer job and at most
eight queued image slots. Raster normalization takes approximately 23–47 ms
across eight rows, making its expected overlap benefit small relative to GPU time.

The independent encoded scan workload uses eight separately decoded 3325×1708
JPEG rows. In paired off/on/on/off runs, p50 was 3.685 / 3.634 / 4.168 / 4.673 s;
lookahead final vectors match exactly. CPU preparation was 0.62–1.07 s in sampled
passes. Lookahead is faster within both pairs (about 1.4% and 10.8%), but the
large drift means this is evidence of correct bounded overlap rather than a
stable percentage claim. Other local compilation/test activity was present;
longer alternating runs on an idle host are needed for an isolated overlap gain.
These are five-sample local qualification runs, not an SLA or cold-start claim.

## Validation

The native/Metal regression selection passes 204 tests, with seven optional
fixtures skipped. It covers independent Flash parity with distinct batched rows,
model-scoped Qwen isolation, resident intermediate/readback behavior, interleaved
geometry identity, deadline/cancellation cleanup, allocation failures and static
serving-budget occupancy. The lookahead cleanup test additionally expires a
caller while the model gate is held and verifies the gate remains owned by its
original caller, before any image work begins.

Production HTTP qualification repeats 37 requests under the default policy and
37 with `--scratch-budget-mb 4096`; all return 200, including the intentional
per-item codec failure. The trace confirms two-row cohorts under the default
policy and a four-row cohort under the larger policy. Both runs compare four
distinct same-grid images and an eight-item mixed-grid array with singleton
references (minimum cosine above 0.999999999999). The truncated PNG body preserves
successful indexes 0 and 2 and the error at 1. A query requesting 128 dimensions
also succeeds. Concurrent single-image clients at 2, 4 and 8 all complete.

At the 200-microsecond queue window, many concurrent arrivals remain singletons:
28 concurrent rows use 25 broker executions under the default policy and 27
under the larger policy in these short samples. This verifies cross-request
coalescing without claiming saturated occupancy or a stable HTTP throughput gain.
Header/codec planning and tighter admission can reduce physical batch size;
broker trace counts are candidate rows before indexed codec failures are removed.
The final-source independent JPEG run has p50 2.907 s for eight rows, exactly the
same vectors as the earlier off/on runs, one producer, no backpressure retry and
24,576 readback bytes. This additional observation is not a paired overlap estimate.

## Profiling and reproduction

`ANTFLY_GEMMA4_VISION_PROFILE=1` flushes at phase boundaries and prints per-layer
wall time, command-buffer GPU time and download bytes for norms, Q/K/V
projections plus RoPE, attention, attention output/residual and FFN.
`TERMITE_METAL_TRACE_DISPATCH_PROFILE=1` adds pipeline dispatch counts/names.
These additional flushes perturb timing: run diagnosis separately from throughput.
The warm diagnostic pass reports approximately 138 ms in attention and 113 ms
in FFN across sixteen layers, with zero phase downloads. These are diagnostic
phase totals rather than an attribution of the uninstrumented 343 ms run.

Build in `zig/pkg/inference` with Zig 0.17:

```sh
zig build build-gemma4-image-bench bench-server -Doptimize=ReleaseFast
zig-out/bin/antfly-gemma4-image-bench \
  --model /path/to/google/embeddinggemma-2 --image /path/to/image.png \
  --scope projector --batch 4 --batching on \
  --warmups 2 --iterations 5 --output /tmp/projector.json
```

Use `--scope embedding --batch 8` for two production borrowed-raster waves,
and `--scope encoded-embedding --batch 8` for independently decoded encoded
rows. Compare preparation on/off in both orders; small preparation changes need
longer paired runs to distinguish host/GPU contention from overlap. The benchmark
reports final vectors, stage timings, transfer counters, frame timing, producer
jobs, admission retries and `max_prepared_images`. Embedding scopes expose transfer/device counters in the nested `media` record;
the external backend snapshot and top-level workspace/device counters are
unavailable for those scopes and must not be interpreted as total memory.
The latter preparation count is the maximum
queued slot capacity (at most eight), not a sampled count of ready pixels.

Raw reports, vectors, logs, frozen binaries and campaign scripts are retained
locally in `.benchmark-results/gemma4-next-2026-10-11/`. The [compact JSON report](gemma4-vision-throughput.json)
beside this document records hashes and summarized measurements. PDF rendering
and codec decode remain on CPU; this change overlaps image preparation, not the
PDF renderer itself. These vision results do not establish a PDF throughput gain
until a full PDF render-plus-embedding workload is measured.

The same PR now includes [audio throughput](audio-throughput.md): resident
Gemma4 audio prompt packing and length-qualified embedding encoder batches, plus
bounded CLAP preparation overlap. The vision Flash kernel remains vision-only.
