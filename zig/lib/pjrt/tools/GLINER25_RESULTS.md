# GLiNER2.5 CUDA qualification and performance

The original dense `fastino/GLiNER2.5-Decide` artifact qualifies against upstream
Python CUDA BF16 with the user-approved bounded-difference policy. It is selected
automatically on NVIDIA L4 SM89, driver API >=13000 and cuBLAS 130400 or 130401.
Base BF16 remains unqualified; Base keeps its accurate FP32 default.

## Base: accurate FP32 default

Base retains FP32 weights and state with compensated BF16x3/TF32x3 projections.
Across the FP32 profiles, 115 varied-input cases pass an independent CPU FP32
oracle with a raw-output limit of 0.002 and exact classifications/canonical entity
routing. Largest error: 0.000241. These campaigns predate the main merge below.

Medians in milliseconds; baseline is main `e86f0d2756`. Both campaigns use the
same checkpoints and prepared inputs, three fresh processes and eight measured
forwards per geometry/arm. Before/after samples span separate campaigns.

| Base workload | Tokens | Batch | Main FP32 | Optimized FP32 | Speedup |
|---|---:|---:|---:|---:|---:|
| Classification | 128 | 1 | 8.88 | 4.18 | 2.13× |
| Classification | 128 | 8 | 41.11 | 20.24 | 2.03× |
| Classification | 512 | 1 | 49.46 | 14.88 | 3.32× |
| Classification | 512 | 8 | 358.75 | 107.76 | 3.33× |
| Entity extraction | 128 | 1 | 10.02 | 5.15 | 1.95× |
| Entity extraction | 512 | 1 | 50.45 | 15.90 | 3.17× |

Base remains slower than PJRT BF16: a separate classification characterization
measured a 1.5–2.9× PJRT advantage. Base BF16 still exceeds the approved confidence
band and changes some entity selections; it remains gated.

## Historical tuned Decide BF16 qualification

This initial campaign predates heuristic selection; current revalidation is
reported under [heuristic serving](#heuristic-serving-and-first-seen-shapes).

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

## Heuristic serving and first-seen shapes

BF16 load-time preparation and projection/attention dispatch now use the same
complete capability check. Missing the optional FP32 softmax symbol keeps
automatic loading on FP32 before weights are uploaded; the GPU regression
explicitly removes that symbol and checks rejection before operand access.

GLiNER BF16 projection, FFN and classifier GEMM misses use cuBLASLt heuristics.
They no longer time candidate GEMMs while holding the serving execution lane.
Ragged-shape and full-cache regressions assert zero timed searches. The public
pipeline benchmark preserves `first_forward_ms` and reports plan-cache misses
and timed-search counters instead of hiding first-use costs in warmup.

An exploratory loaded-pipeline comparison uses one fresh **Debug** process per
arm, 19 ordered request shapes (batch 1/5/8/32, varying lengths and schemas), one
first call and two immediate repeats per shape. Includes rendering, tokenization,
decoding and synchronized readback; excludes HTTP and model loading.

| Variable-request metric | Frozen tuned profile | Heuristic serving |
|---|---:|---:|
| Median first forward | 23.87 ms | 14.05 ms |
| Maximum first forward | 318.50 ms | 154.17 ms |
| Timed tuning searches | 84 | 0 |

The new arm records 84 actual plan-cache misses. These are diagnostic cold-shape
results, separate from the ReleaseFast warm measurements. All 89 public-pipeline
cases still pass the fixed Python CUDA BF16 gate (two outputs per case), maximum
confidence drift 0.009570; token IDs match and batch 32 uses four physical forwards.
Base's 42 CPU FP32 regression cases pass with maximum raw error 0.00011492.
Decide also passes all 89 cases in three fresh ReleaseFast processes on each of
cuBLAS 13.4.0 and 13.4.1, with two outputs per case. Across all six accepted runs,
maximum confidence drift is 0.010364, below the unchanged 0.02 limit; decisions
outside the approved threshold/tie band match. A run with overlapping Gemma GPU
work was rejected and replaced. All accepted runs record zero timed searches.
All 41 focused GPU tests, 29 Python tests, CUDA type checking, source-policy and
format/whitespace checks pass.

The fresh paired ReleaseFast warm/PJRT campaign is not yet qualified: independent
Gemma jobs repeatedly overlapped GPU execution without taking the shared lock.
Affected attempts were rejected and are excluded. The historical warm tables
below must not be treated as measurements of the new heuristic default.

## Historical tuned Decide BF16 latency

The following retained campaign predates heuristic serving. Its warm timings
exclude synchronous first-shape tuning and do not describe serving cache misses.

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

## Historical tuned Decide BF16 memory

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

After merging main `dc9064638a` at `b8c7ae5e50`, all 40 focused CUDA-enabled
tests, 29 Python tests and 38 PJRT library tests pass; all three PJRT workers
build. Live-plugin unit smoke checks were skipped. The CUDA suite covers
attention masks/ragged score strides, FFN and
projection oracles, fused/separate pointer preparation, cache bounds/admission,
source/runtime gating and the FP32 regression paths. The CUDA server type check,
Python protocol/qualification tests and reproducible cubin check pass. The
loaded pipeline covers rendering/tokenization/decoding, while the performance
campaign above measures prepared inputs. No new HTTP performance campaign,
other-GPU qualification or Gemma changes are included.

Compact source, executable, runtime, numerical and performance evidence:
[gliner25-l4.json](gliner25-l4.json). Raw vectors,
HLO and executable caches remain ignored local evidence. The 13.4.0 runtime was
qualified numerically; the latency campaign used 13.4.1. The final dispatch-only
addition of 13.4.0 does not change the measured inference arithmetic.

The original multi-round numerical and performance campaigns predate the main
merge. A cleanup regression rerun passes all 42 Base CPU FP32 cases (maximum
raw error 0.00011492) and all 89 Decide Python CUDA BF16 cases
(maximum confidence drift 0.009570, batch 32 uses four forwards). The
trimmed core benchmark builds, all 29 Python tests and artifact source-policy
checks pass. That cleanup did not rerun performance or change production kernels;
the subsequent serving changes and their validation are described above.

## Latest published CUDA card: snapshot comparison

Source: [latest inference benchmarks](https://circus.antfly.io/latest/#group-inference),
[CUDA model card](https://circus.antfly.io/latest/#inference-v03-cloud-model-card-cuda),
retrieved 2026-11-18. The latest page currently carries Antfly rc.18 / `727b70d1`
for this L4 cohort, measured 2026-11-13. All embedded inference evidence objects
match the [versioned snapshot](https://circus.antfly.io/v0.3.0/#inference-v03-cloud-model-card-cuda),
so switching to latest does not change the GLiNER comparison values.
Latest-page HTML SHA-256:
`91c930c75663573ce5ebbf8b3b371ac68fe3c9421af323f9cef7798c92343510`.
The compact JSON retains both URLs, retrieval time and page hashes for posterity.
GLiNER checkpoint weight hashes match our retained models.

The comparison column uses the completed 2026-11-17 Termite Circus **FP32 HTTP**
campaign, based on `4ab0963758` plus the saved native CUDA changes. It predates
compensated Tensor Core and automatic BF16 promotion. It is not a fresh result
for this branch's final BF16 default. Both campaigns use serial native HTTP and
synchronized in-process Python, with medians of three lifecycle request p50s.
The published reference is FP32; native arithmetic was not instrumented there.

| Workload | Published Antfly ms | Published Python ms | Retained FP32 HTTP ms | Retained Python ms |
|---|---:|---:|---:|---:|
| base/ent_sentence_x1 | 12.06 | 35.75 | 9.95 | 37.11 |
| base/ent_paragraph_x1 | 19.61 | 37.27 | 14.13 | 38.53 |
| base/ent_page_x1 | 43.81 | 39.10 | 22.91 | 39.96 |
| base/ent_wiki_x1 | 21.71 | 37.83 | 14.99 | 39.30 |
| base/mixed_paragraph_x1 | 23.13 | 42.51 | 15.57 | 45.08 |
| base/ent_paragraph_b8 | 117.29 | 103.47 | 84.16 | 102.43 |
| base/ent_paragraph_b32 | 486.04 | 497.13 | 342.91 | 488.75 |
| base/long_book_w384 | 1268.20 | 994.01 | Not run | — |
| decide/decide_x1 | 62.19 | 49.82 | 19.75 | 51.41 |
| decide/decide_b8 | 225.44 | 98.45 | 81.45 | 97.56 |
| decide/decide_b32 | HTTP 413 | — | 318.37 | 369.03 |

The published Decide ratios were 0.80× (single input) and 0.44× (batch 8),
with batch 32 rejected. In the retained FP32 campaign, native beats Python on
all seven Base cases (1.21–3.76×) and all three Decide cases (1.16–2.59×).
Ratios are upstream/native and retain each campaign's median paired ratio;
they are not recomputed from the rounded columns above.

Both hosts use L4, but the driver changes from 580.178.04 to 580.159.03 and the
corpus SHA changes from `be16dc32…` to `c237bb1c…`. The current harness omits
whole-book windowing. These cohorts do not isolate a code-only release speedup.
The newer BF16/PJRT core results above exclude HTTP and cannot be substituted
for these end-to-end cells.

Combined memory below is simultaneous process-tree PSS+NVML in decimal GB,
including startup; each retained memory cell has one isolated lifecycle, rather
than repeated-trial variance estimates. Published batch-32 refusal has no native
memory measurement. All Base/Decide memory cells and provenance remain in the
compact JSON record.

| Decide workload | Published native GB | Retained FP32 native GB | Retained Python GB |
|---|---:|---:|---:|
| decide/decide_x1 | 4.086 | 2.773 | 4.842 |
| decide/decide_b8 | 4.065 | 3.478 | 4.975 |
| decide/decide_b32 | — | 4.213 | 5.706 |

Published refusals and missing workloads remain explicit. No failed cell is
counted as a timing or memory win, and no new GPU campaign was run here.
