# Qualified Decide BF16 on NVIDIA L4

The original dense `fastino/GLiNER2.5-Decide` artifact qualifies against upstream
Python CUDA BF16 with the user-approved bounded-difference policy. It is selected
automatically on NVIDIA L4 SM89, driver API >=13000 and cuBLAS 130400 or 130401.
Base BF16 remains unqualified; Base keeps its accurate FP32 default.

## Numerical qualification

- 89 deterministic classification cases pass in three fresh processes on each
  of cuBLAS 13.4.0 and 13.4.1, with two native and two Python samples per case.
- Maximum absolute confidence difference: **0.009571**, below the fixed 0.02 band.
- Two fixture contexts exhibit bounded urgency argmax changes: reference
  probability gaps 0.001343 and 0.002198. Exact decisions are required outside
  the approved band; this is not a claim of identical decisions on every case.
- The loaded-model pipeline separately passes all 89 cases. Prepared token IDs
  equal Python; batch 32 executes four physical forwards of at most eight rows.
- Final automatic-constructor validation also passes all 89 cases on the system
  cuBLAS 13.4.0 runtime with BF16 encoder weights and active BF16 attention/FFN.
- Automatic selection ignores diagnostic semantic flags. The FP32 opt-out
  selects FP32 weights and records zero BF16 attention/FFN calls.
- Base's unresolved confidence outliers are approximately 0.025–0.029; entity
  selections also differ. These failures were not waived or called qualified.

The reference uses the unchanged checkpoint, stock Python CUDA BF16 reduced
precision reduction, inference mode and no autocast. Qualification measures
confidence drift, not FP32 raw-logit equality or task accuracy. The threshold
band is 0.02; an argmax change permits a reference gap <= 0.04 because each label
may move by 0.02. See [GLINER25.md](GLINER25.md#automatic-decide-bf16-qualification)
for reproducible fixture preparation and qualification commands.

## Latency

Medians in milliseconds on L4 with cuBLAS 13.4.1. Three rotated fresh-process
rounds, eight measured forwards per geometry per arm. Both native arms use the
same release executable: BF16 automatic selection versus the existing accurate
FP32 fallback selected with the BF16 opt-out. This FP32 control is not the
original release/main benchmark baseline.

Prepared encoder/classifier execution includes input upload, forward temporary
allocations and synchronized FP32 readback; excludes tokenization, final decoding,
HTTP, loading and warmup. PJRT runs through the native Zig C API with precompiled
executables. All parameters remain resident, including unused heads. A shared
lock and process monitor reject overlapping GPU work.

| Tokens | Batch | FP32 fallback | BF16 auto | PJRT BF16 | vs FP32 | vs PJRT |
|---:|---:|---:|---:|---:|---:|---:|
| 128 | 1 | 14.93 | 4.78 | 7.84 | 3.12× | 1.640× |
| 128 | 8 | 73.19 | 24.01 | 25.44 | 3.05× | 1.060× |
| 512 | 1 | 49.33 | 16.14 | 16.25 | 3.06× | 1.006× |
| 512 | 8 | 342.73 | 118.11 | 128.81 | 2.90× | 1.091× |

The 512-token single-item case is effectively parity: a 0.6% pooled median edge,
with native faster in two of three rounds. The other cases improve over PJRT
by 6–64%. These prepared-input timings do not establish HTTP speedups.

## Memory

Median of three whole-campaign process peaks, in MiB. Includes startup/loading,
warmup and inference; sampled every 0.2 seconds using process-tree PSS plus NVML.
The common Python coordinator/parameter feeder is included for each arm.

| Arm | GPU peak | Simultaneous PSS+GPU peak |
|---|---:|---:|
| FP32 fallback | 2440 | 3373 |
| BF16 auto | 1866 | 3555 |
| PJRT BF16 | 2362 | 3456 |

Native BF16 uses 21% less GPU memory than PJRT and 24% less than its FP32
fallback. Its combined startup peak is about 3% higher than PJRT and 5% higher
than the fallback. There is no combined-memory win; startup sampling and
runtime inventories remain relevant to that comparison.

## Validation and scope

All 40 focused CUDA-enabled tests and 18 Python tests pass. The CUDA suite covers attention masks/ragged score strides, FFN and
projection oracles, fused/separate pointer preparation, cache bounds/admission,
source/runtime gating and the FP32 regression paths. The CUDA server type check,
Python protocol/qualification tests and reproducible cubin check pass. The
loaded pipeline covers rendering/tokenization/decoding, while the performance
campaign above measures prepared inputs. No new HTTP performance campaign,
other-GPU qualification or Gemma changes are included.

Compact source, executable, runtime, numerical and performance evidence:
[gliner25-bf16-qualified-l4.json](gliner25-bf16-qualified-l4.json). Raw vectors,
HLO and executable caches remain ignored local evidence. The 13.4.0 runtime was
qualified numerically; the latency campaign used 13.4.1. The final dispatch-only
addition of 13.4.0 does not change the measured inference arithmetic.
