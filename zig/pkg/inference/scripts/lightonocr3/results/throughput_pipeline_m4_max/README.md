# OCR pipeline throughput on M4 Max

This pass follows `e7a0d695d77cc14dba96a942b7fc8b09ee70c13b` on a shared
36 GiB M4 Max. Full-reader parity with production vLLM Metal remains unfinished.
The preceding kernel measurements remain in `../throughput_recurrent_m4_max`.

## Shipping changes

- Warm recurrent pages use one temporary workspace and one output allocation.
  Each page retains independent convolution and recurrent histories. The fast
  path preflights every input and state before collecting work; cold states,
  unsupported layouts and unavailable pipelines retain the existing path.
- Other collected recurrent cores can write directly into their final output
  rows, avoiding repeated concatenation and copying of earlier rows.
- Small, aligned, nonoverlapping device copies use a bit-preserving `uint4`
  compute kernel inside a tracked serial decode encoder. Large, unaligned,
  overlapping, standalone and prefill copies retain their established route.
  Resource ranges, producer ordering, frame retention and cancellation apply.
- Prepared integrated vision slots can survive projector wrapper destruction
  when the compute provider's dense cache owns the exact immutable backing
  bytes and source pin. Transient tensors still retire their slots before their
  addresses can be reused. This avoids preparing the same vision weights for
  every page without retaining another copy of those weights.
- An opt-in cache lets immutable decoder layer weight descriptors remain owned by the compute
  epoch when their backing storage is already provider-cache owned. This avoids
  allocating wrappers for the same weights on every token. Unshared, mutable
  and separately pinned sources keep their old lifetime. Temporary F32 host
  mirrors are released after the last actual caller, while independently owned
  host aliases remain valid.
- Host-view construction now unwinds each shape/stride allocation separately.
  Allocation-failure injection exposed a pre-existing leak when a later view
  metadata allocation failed; the new owner/alias tests cover that path.
- Rollback flags for the Zig paths are snapshotted once per provider epoch.
  This avoids inner-loop environment scans while allowing a subsequent epoch
  to select different controls.
- Benchmark output now includes framed GPU/wait totals and observed packed
  batch sizes. These counters exclude unframed operations and are not a complete
  GPU attribution of reader wall time.

BF16 checkpoint weights, F32 decoder activations/probabilities/accumulation/outputs/recurrent state,
and the original F16 paged KV remain unchanged. Logical windows and physical
rows remain eight by default. Existing admission and cancellation contracts
remain in force; shared scratch replaces a larger set of temporary allocations.

Rollback controls:

```text
TERMITE_METAL_DISABLE_LIGHTONOCR3_DIRECT_CORE_OUTPUT=1
TERMITE_METAL_DISABLE_LIGHTONOCR3_COMPUTE_COPIES=1
TERMITE_METAL_DISABLE_LIGHTONOCR3_PACKED_CORE=1
TERMITE_METAL_DISABLE_LIGHTONOCR3_VISION_SLOT_REUSE=1
TERMITE_METAL_DISABLE_LIGHTONOCR3_WEIGHT_HANDLES=1
```

Decoder descriptor retention remains opt-in: set
`TERMITE_METAL_ENABLE_LIGHTONOCR3_WEIGHT_HANDLES=1`. An explicit disable switch
overrides it. The benchmark can force this candidate on/off with its rollback
value; ordinary production contexts leave it disabled. `default` means shipping
behavior, while `on` includes this experimental cache.

Disabling direct output also declines the packed warm-state route. The ready-only
cohort experiment is opt-in with `ANTFLY_LIGHTONOCR3_READY_COHORTS=1`;
`ANTFLY_LIGHTONOCR3_WAIT_FOR_PREFILL_PEERS=1` restores the ordinary cohort target.
It excludes prefill, unknown next work, incompatible geometry and exclusive work
when calculating a wait target. It passed correctness checks but increased CPU
cost without a consistent throughput gain.

## Reader results

Sixteen mixed pages, C8, eight physical rows and an eight-page window, greedy
sampling, maximum 256 generated tokens. Timings include queueing, image
preparation and inference, and exclude loading and serial reference generation.
Our benchmark GPU workloads run sequentially; other machine activity remains
uncontrolled. Some exploratory readers overlap CPU compilation.

`native_optimized_*_rounds` uses working per-epoch rollback controls:

| Model | Previous behavior / new / new / previous, pages/s | Fresh vLLM Metal, pages/s |
|---|---:|---:|
| LightOnOCR-3 0.8B | 2.0813 / 2.4174 / 2.4649 / 2.0577 | 2.8851 |
| LightOnOCR-3 4B | 0.6522 / 0.6784 / 0.6533 / 0.5826 | 0.8064 |

The 0.8B bracket supports a 16–20% gain. The 4B control drifts substantially;
these rounds do not establish a stable reader speedup. Later default 0.8B
rounds reach 2.4714–2.4772 pages/s. Disabling vision slot reuse gives 2.3520;
disabling packed scratch gives 2.3125, versus adjacent defaults near 2.47.
Ready-only scheduling gives 2.4413 with 12.835 CPU seconds, versus neighboring
defaults at 2.4714/2.4744 with 9.162/10.003 CPU seconds, so remains opt-in.

The final `shipping` executable checks the production default against optional
descriptor retention, observing its actual activation separately in every round:

| Model | Default, first / last | Optional retention, middle rounds | Later vLLM reference |
|---|---:|---:|---:|
| 0.8B | 2.2893 / 2.1926 | 2.3581 / 2.2929 | 2.2170 |
| 4B | 0.6691 / 0.5346 | 0.6355 / 0.6074 | 0.6891 |

All values are pages/s. The late 0.8B default straddles its reference, but the
earlier comparison retains a gap and the 4B control itself drifts by 20%.
These shared-machine measurements do not establish reliable universal parity.
Retention has no consistent advantage, particularly for 4B, so stays opt-in.
Earlier retention profiles and controls remain recorded; their builds predate
the activation marker and a false observation field means no marker was logged.
The final shipping rounds explicitly verify enabled/disabled marker states.

One intermediate 4B launch was denied by the unchanged live-memory guard
(11.35 GB requested versus 8.6 GB admissible after reserve). The retry and final
shipping runs completed after capacity returned; no cap or reserve was lowered.

Every timed result matches its same-model serial output. The three LightOn
serial outputs also match the fresh vLLM references exactly, including markup;
this small synthetic corpus does not establish broad OCR accuracy equivalence.
vLLM uses its production BF16 path, while native decoder activations remain
F32. References disable prefix/processor caches and use distinct media UUIDs.

Florence batch-size checks retain eight as the default:

| Physical pages | Native pages/s |
|---|---:|
| 8, first / last | 4.3661 / 4.3345 |
| 4, first / last | 3.7147 / 3.6154 |
| 2 | 2.6562 |

Fresh MLX-VLM serial Florence measures 4.9724 pages/s. Native uses Q4_K weights,
F16 encoder mirrors and its existing no-repeat policy; the reference uses F16
weights and greedy decoding. It is a separate reference, not an equal-precision
comparison. The installed vLLM Metal has no Florence adapter. Native outputs
match their serial references; whitespace/markup-normalized content matches MLX.

## Dispatch profile and rejected experiments

`profile_copy_summary.json` records a paired eight-page run with frame lifecycle
tracing. Compute-copy rounds use approximately 10–11k compute encoders and 776
blit encoders, versus 60–61k and 31.5k in the controls. CPU encoding totals fall
from 1.46–1.65 seconds to 1.18–1.24 seconds. These instrumented runs are not the
primary throughput numbers, and cohort timing changes their frame counts.

The earlier `native_pipeline_*` and `native_packed_*` exploratory rounds exposed
process-global cached Zig environment flags: the initial direct-output rollback
persisted into subsequent rounds. Their direct/packed ablations are invalid and
must not be interpreted as measurements of those features. The compute-copy
control reads the C environment directly and remains valid. A separate fresh-on
process observes packed rows 2–8. The `optimized` build replaces those global
flag reads with independently tested epoch snapshots; its alternating controls
observe packed rows only where enabled.

`recurrent_tiles/` retains full numerical conformance and balanced separate-
process GPU trials for eight-, sixteen- and thirty-two-value tiles. Wider tiles
regress many batch sizes, particularly 4B, and some individual medians vary
substantially between rounds. The production tile remains eight. Aggregate
medians must not be used to select a best-looking individual case.

`nativepatch_runtime.patch.gz` retains an excluded experiment that consumes the
original BF16 Conv3D patch weights directly. The GPU trace confirms the native
BF16 source and F32 input storage, and the invoice text is correct, but the
existing reordered-image full-vision tolerance check fails. Its 137-test GPU
run has 135 passes, one skip and one failure. It is not present in the shipping
runtime; the tolerance was not relaxed and no throughput claim is made for it.

The prior 146-case projection suite uses command-buffer GPU timestamps for both
participants, not synchronized host wall time. Those isolated operations do not
establish full-reader parity; native F32 and MLX BF16 are different precisions.
The earlier report and PR description are corrected accordingly.

## Validation and reproduction

The final `shipping` runtime passes 138 selected GPU tests with one fixture-
dependent skip, plus a separate default checkpoint/HTTP run. CPU-only execution
passes 128 with eleven GPU/fixture skips. Allocation-failure injection passes
with both retained and ordinary descriptor lifetimes, without leaks. The earlier
`optimized` runtime separately passes 84/84 GPU tests. C
conformance passes for projections, shared recurrent output/workspace offsets,
independent histories and continuation, missing-pipeline/rollback fallbacks,
bitwise copy payloads including nonfinite bit patterns, cancellation, producer
ordering, paged attention and packed vision.

Each build manifest records the executable hash and exact runtime file hashes.
Its compressed runtime patch applies to the base commit above. Reader reports
bind to that manifest even when later experiments change the working tree.
`conformance_manifest.json` binds the C harnesses and runtime source. Raw logs,
experimental sources, helper scripts and their hashes are retained losslessly.
The default executable is ReleaseFast with stripping and uses the repository's
filtered inference-test build, not the full runtime-test switch.

Remaining work includes the BF16 attention gap, broader graph/dispatch overhead,
vision preprocessing within the execution lock and recurrent memory access.
The checked wider tiles and native patch candidate do not justify changing the
shipping paths. These results do not claim parity for unmeasured hardware,
workloads or all OCR models.
