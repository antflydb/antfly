# Laya Metal residency

Status: implementation and local correctness qualification complete. Released
FP16 passes the full performance matrix. FP32 bulk passes, but batch-8 mixed
inputs lack two clean pairs because of paging. Cross-precision production/default
promotion is **not qualified**. Keep this path opt-in. Enable with
`ANTFLY_LAYA_METAL_RESIDENT=1`; `TERMITE_METAL_DISABLE_LAYA_RESIDENT=1` takes
precedence. The existing inference path remains the default.

## Contract

CPU tokenization and validation precede device execution. The resident path owns
immutable projection slots, embedding and normalization constants, and resolved
calibration scales for the loaded model's lifetime. Activations stay on Metal
through both heads and probability/decision decoding. Only final numeric results
return to the CPU for label selection and JSON formatting. Raw-logit session
callers retain their existing output contract. This implementation accepts the
single-file safetensors Laya artifacts, preserving native F16/BF16/F32 projection
storage; unsupported artifact layouts fail explicitly.

The path must reject unsupported device operations and memory admission failures;
it must not execute a CPU fallback. Command-buffer completion boundaries preserve
cooperative cancellation without downloading activations. Model resources are
released before the owning Metal provider is destroyed.

## Qualification gates

- Match released-checkpoint and FP32 finetuned-export CPU/PyTorch predictions;
  probability tolerance remains 5e-5 and miniature raw logits 2e-4.
- After preparation, demonstrate zero weight uploads, intermediate activation
  readbacks, and host fallbacks across repeated requests and shape changes.
- Exercise all primitives, option counts, calibration buckets, padding, ties,
  cancellation, allocation denial, repeated/unloaded sessions, and managed API
  requests. Preserve model, workspace, and preparation memory admission.
- Compare matched baseline/candidate runs, released FP16 and exported FP32
  separately, in three alternating pairs. Interactive profiles use batches
  1/2/4/8, 10 warmups, 100 observations; stress/throughput profiles use 25.
- Default promotion requires at least 10% geometric-mean interactive p50
  improvement, no per-profile p50 regression above 5%, p95 above 10%, or batch
  throughput regression above 5%. Measurement windows must be paging-free.
- Retain binaries, commands, input/source hashes, counters and raw timing data
  beneath `.benchmark-results/laya-resident-20260922/`. Remote CI is not a gate.

## Local evidence (2026-09-22)

Hardware: Apple M4 Pro, 24 GiB, 12 CPU cores. Artifacts are retained beneath
`.benchmark-results/laya-resident-20260922/`. These are local qualification
artifacts, not CI results. The released artifact contains FP16 projections;
finetuned exports contain FP32 projections. Neither path creates reduced-precision
weight mirrors.

| Checkpoint / boundary | Maximum probability error versus PyTorch |
| --- | ---: |
| Released FP16, 20 decisions, batches 1 and 4 | 0.000002026558 |
| Metal soft-CE FP32 export, 20 decisions | 0.000000447035 |
| CPU-trained FP32 export, 20 decisions | 0.000001132488 |
| RLCD FP32 export, 20 decisions | 0.000000834465 |
| Released FP16, 512 tokens | 0.000000923872 |
| Metal-trained FP32, 512 tokens | 0.000000178814 |
| Tiny FP16 / BF16 fixtures | 0.000000029803 / 0.000000059605 |

The miniature fixture also checks raw logits, mixed padding and batches through
512, cancellation/retry, and warm weight reuse. Numeric decisions are packed into
one final readback. Transfer counters audit the MetalTensor boundary on the
request thread; weight-upload counters count preparation payload bytes, not
hardware bus traffic. Apple unified memory does not remove synchronization and
CPU materialization costs, which these counters are intended to expose.

Prepared projections use native F16/BF16/F32 storage. Embedding/norm constants
expand losslessly to F32 once. Preparation drops temporary host projection caches
after each upload. Model-load admission reserves preparation peak and retained
model bytes; request admission reserves a conservative two-layer workspace bound.
Execution submits and drains device work every two encoder layers and every head
layer so cancellation and request cleanup never free in-flight buffers.

The miniature GPU regression suite passed 23 selected tests with two optional
real-model skips. It includes training regression coverage, warm allocation
failure/retry, calibrated GPU decisions, cancellation, and an embedded/HTTP test
that confirms the resident Metal owner was used. Additional lifecycle coverage
passed cold preparation failures, independent owners, and repeated unloads.
The final portable fixture passed 22 selected tests with three optional/GPU
skips; CPU behavior is unchanged when the residency flag is set.

MPS matrix views are cleared after each completed frame and on error cleanup;
weights and multiplication plans remain resident. This prevents matrix-view
caches from retaining activations beyond the admitted request lifetime. Quiescent
snapshots and the benchmark require zero cached activation bytes.

The managed route also proves a one-byte backend budget rejects before device
allocation, even when CPU appears after Metal in the backend preference list.
For full-model API qualification the test configures 6 GiB host, 12 GiB backend,
18 GiB combined admission capacity, and 8 GiB scratch capacity, including a
192-question expanded batch.
Default budgets may reject that batch or FP32 preparation; operators must size
budgets for the model and request geometry. Rejection never selects CPU after
an opted-in resident Metal load attempt.

Full-model embedded and HTTP qualification passed for both released FP16 and
trained FP32 artifacts, including expanded batches and admission rejection.
Resident model payloads were 947,089,468 bytes (FP16 source) and 1,686,340,668 bytes
(FP32 export). These figures exclude request workspace and host artifact storage.
The final portable fixture run passed 22 tests, with three optional/GPU skips.

The first candidate passed the released-FP16 interactive gate: 48 paging-free
runs, with a geometric-mean p50 ratio of 0.734 (26.6% lower latency). It failed
the batch-64 throughput gate: resident p50 was about 911 ms versus 816 ms for
legacy. That candidate and its measurements remain retained under
`bin/pre-workspace-inference-test`, `benchmark-fp16-interactive`, and
`benchmark-fp16-throughput`; the incomplete throughput campaign has a stop record.

The replacement candidate uses a request-owned, bounded GPU buffer pool. Buffers
become reusable only after their frame completes; carried hidden states and the
local-attention bias remain pinned. All pool buffers are freed before the request
lease ends. MPS activation views are still released at each boundary. The pool
uses at most half the conservative workspace reservation, leaving capacity for
attention scratch and other transient buffers. `workspace_pool_*` counters expose
reuse and peak pooled bytes. `TERMITE_METAL_DISABLE_LAYA_WORKSPACE_REUSE=1` disables
this optimization for diagnostics without enabling a CPU fallback.

The replacement passed released FP16, three FP32 export, and tiny FP16/BF16
PyTorch parity checks, plus full-model FP16/FP32 HTTP checks (`pool-*-parity.log`,
`pool-*-api.log`). The final test binary additionally passed actual model-allocator
cold-preparation failure/retry and cancellation before and after frame completion;
`final-gpu.log` has 23 passes and two optional skips. The CPU-only fixture has
22 passes and three optional/GPU skips in `final-portable-fixture.log`.

Three paging-free FP16 pairs for the pooled candidate measured 196.46 ms versus
246.19 ms at batch 16, and 746.41 ms versus 816.38 ms at batch 64. Both pass their
latency and throughput gates (`pool-throughput-partial.json`). The subsequent
batch-128 baseline incurred 14 pageouts and was rejected, with no swapouts; that
partial campaign cannot qualify promotion. The final campaign below uses the
frozen `bin/final-inference-test`. `final-manifest.json` fingerprints binaries and
the retained `source-snapshot/`; the final lifecycle build only strengthened tests
after the pooled runtime was frozen.

## Final performance matrix

The final campaign retained **128 paging-free windows, 64 of 66 required pairs,
and 10,100 timed observations**. Released FP16 passes every gate: geometric-mean
interactive p50 is 38.1% lower, and bulk throughput improves 26.5%, 8.4%, and 6.8%
at batches 16, 64, and 128. FP32 bulk also passes, improving throughput 26.8%, 7.3%,
and 4.3%. All measured latency/throughput ratios are within their numeric limits.

FP32 batch-8 mixed inputs have only one accepted pair. That campaign exhausted
its two automatic paging retries; after completing FP16, a separate continuation
preserved the 22 accepted interactive pairs and attempted each missing pair once.
Those attempts incurred 4 pageouts in the resident window and 621 in the legacy
window, respectively, with no swapouts. They are excluded. Twelve rejected pair
attempts are retained across the final campaign and continuation. The overall
promotion result is therefore **not qualified**, despite the observed speedups.
The remaining gate is three paging-free pairs for FP32 batch-8 mixed inputs under
controlled memory conditions; do not relax the gate or promote from this evidence.

Each table entry is the median across three accepted windows per mode, except
the explicitly marked FP32 batch-8 mixed profile. Its observed aggregate is
provisional. Raw results and verification are in `final-qualification.json`,
`final-matrix.md`, and `final-report.log` under the artifact directory.
`resume_fp32.py` records the continuation's source/result hashes; the shipping
benchmark harness and runtime were unchanged throughout timing.

### Released FP16

Interactive geometric-mean p50 reduction: **38.1%**. Precision gate: **PASS**.

| Batch | Profile | Legacy p50 ms | Resident p50 ms | Legacy p95 ms | Resident p95 ms | Throughput change | Clean pairs | Gate |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | fixed | 65.91 | 25.05 | 66.31 | 25.32 | +162.7% | 3/3 | PASS |
| 1 | mixed | 178.03 | 99.90 | 237.67 | 144.94 | +70.0% | 3/3 | PASS |
| 2 | fixed | 78.78 | 36.89 | 79.31 | 37.17 | +113.5% | 3/3 | PASS |
| 2 | mixed | 306.88 | 217.65 | 560.25 | 389.31 | +41.2% | 3/3 | PASS |
| 4 | fixed | 104.11 | 60.54 | 104.78 | 60.76 | +71.9% | 3/3 | PASS |
| 4 | mixed | 610.29 | 512.28 | 944.34 | 791.84 | +20.3% | 3/3 | PASS |
| 8 | fixed | 150.91 | 104.44 | 151.65 | 104.92 | +44.2% | 3/3 | PASS |
| 8 | mixed | 1128.10 | 1010.81 | 1721.00 | 1571.69 | +10.9% | 3/3 | PASS |
| 16 | fixed | 248.85 | 196.69 | 249.42 | 197.06 | +26.5% | 3/3 | PASS |
| 64 | fixed | 818.43 | 755.34 | 819.16 | 758.33 | +8.4% | 3/3 | PASS |
| 128 | fixed | 1596.02 | 1493.79 | 1600.47 | 1495.83 | +6.8% | 3/3 | PASS |

### Finetuned FP32

Interactive geometric-mean p50 reduction: **38.2%**. Precision gate: **FAIL**.

Coverage is incomplete: the observed aggregate includes a profile with only one
clean pair and does not qualify this precision for promotion.

| Batch | Profile | Legacy p50 ms | Resident p50 ms | Legacy p95 ms | Resident p95 ms | Throughput change | Clean pairs | Gate |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | fixed | 343.65 | 135.48 | 348.10 | 135.86 | +153.6% | 3/3 | PASS |
| 1 | mixed | 544.69 | 293.79 | 662.29 | 386.03 | +88.8% | 3/3 | PASS |
| 2 | fixed | 472.97 | 257.74 | 478.19 | 258.11 | +83.6% | 3/3 | PASS |
| 2 | mixed | 890.20 | 627.42 | 1388.35 | 1058.20 | +44.0% | 3/3 | PASS |
| 4 | fixed | 439.53 | 223.48 | 443.74 | 224.20 | +96.6% | 3/3 | PASS |
| 4 | mixed | 1731.98 | 1455.85 | 2422.55 | 2098.81 | +20.4% | 3/3 | PASS |
| 8 | fixed | 625.89 | 416.67 | 636.66 | 417.15 | +50.4% | 3/3 | PASS |
| 8 | mixed | 3187.50 | 2915.40 | 4484.51 | 4140.00 | +9.4% | 1/3 | UNQUALIFIED |
| 16 | fixed | 1023.55 | 807.28 | 1027.16 | 807.76 | +26.8% | 3/3 | PASS |
| 64 | fixed | 3441.98 | 3205.97 | 3445.51 | 3208.39 | +7.3% | 3/3 | PASS |
| 128 | fixed | 6660.69 | 6389.07 | 6665.42 | 6391.99 | +4.3% | 3/3 | PASS |

The maximum whole-process footprint across accepted windows, including model
preparation, was 5.59 GB resident versus 5.08 GB legacy for FP16, and 6.22 GB versus
7.36 GB for FP32. Residency and the workspace pool do not guarantee lower peak
memory for every precision. These decimal-GB figures are process footprints,
not just the resident model payload or physical CPU/GPU bus traffic.

Final Metal binary SHA-256:
`ffe75f0ff408ba9cb144daafbeb3ee01679f9d9e082fbb2a95b7885857db30e9`.

## Reproduce and rollback

Set `ANTFLY_LAYA_METAL_RESIDENT=1` before loading a Laya model with the Metal
backend. The flag applies to both raw-logit sessions and typed extraction.
Unset it, or set `TERMITE_METAL_DISABLE_LAYA_RESIDENT=1`, and restart/reload to
return to the existing path. CPU inference and training do not use this switch.

Generate independent references with `scripts/laya_export_reference.py`. Build
`inference-test` with `-Dmetal=true -Dcuda=false -Donnx=false -Doptimize=ReleaseFast`
and the `laya ` test filter. For each precision, run three alternating pairs:

```sh
python3 scripts/benchmark_laya_metal.py --binary <test> \
  --reference <reference-directory> --output <new-interactive-directory> \
  --batches 1 2 4 8 --profiles fixed mixed
python3 scripts/benchmark_laya_metal.py --binary <test> \
  --reference <reference-directory> --output <new-throughput-directory> \
  --batches 16 64 128 --profiles fixed
```

The retained fixed profile repeats a 61-token, four-option decision; the mixed
profile rotates 22 independent reference records, including a 512-token record.
Large-batch gates use the fixed profile. The throughput-only invocation returns
nonzero because it cannot satisfy interactive coverage: assess its
`measurement_runs_valid` and `profile_regression_gates_passed` fields together
with the interactive invocation's `passed` result.

The same binary runs with residency disabled/enabled; a separate baseline
binary is optional. The runner retains individual samples, logs, SHA-256 hashes,
and paging counters and fails closed on missing counters or failed processes.
Paging-contaminated pairs have at most two automatic retries (override with
`--paging-retries`). Each retry repeats both modes in the same alternating order;
rejected attempts and their raw samples are retained separately. Only complete,
paging-free pairs enter the comparisons. Correctness, residency, process, and
identity failures never qualify for a retry. Timeouts terminate the entire timing
process group, including the GPU child. Seven harness tests cover validation,
paired retries, and timeout cleanup.

These measurements include tokenization and decision decoding at the pipeline
boundary; they exclude transport and JSON serialization.
