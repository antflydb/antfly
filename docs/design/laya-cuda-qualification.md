# Laya CUDA implementation and qualification

CUDA execution is implemented for the ModernBERT Laya encoder and decision heads.
The release target is an NVIDIA L4 (24 GB) with the English checkpoint
`convaiinnovations/laya` at `c5d78730f3493e4fe16d61507ef4b78eef7318cf`.
Upstream `common.py` is pinned to `6a5819129eb220570792e417e49723d697efd76f`.
The L4 fatbin qualification gate passed on 2026-09-22. The
[machine-readable report](laya-cuda-l4-fatbin-report.json) records the exact
binary, checkpoint, fixture, and CUDA artifact hashes. The local run used
Zig 0.16.0 `ReleaseFast`, CUDA 13.2 artifacts, driver 580.159.03, four vCPUs,
and 16 GiB host RAM. It tested the focused production HTTP server and the
`x86_64_v3` qualification executable; the full-CLI `ReleaseSafe` workflow is
provided separately below.

Portable PTX remains unqualified on this host. The driver rejects the CUDA 13.2
PTX artifact
with `CUDA_ERROR_UNSUPPORTED_PTX_VERSION`; portable qualification requires a
driver compatible with the pinned toolkit.

## Measured L4 fatbin results

These are the original implementation's measurements. See
[CUDA performance work](laya-cuda-performance.md) for the subsequent optimizations
and matched full-pipeline comparison.

- Focused suite: **50 selected, 50 passed, zero skipped**.
- Released checkpoint: **192/192 decisions agree with the CPU oracle**;
  reported maximum probability error `0.0000117`, below the `0.00005` bound.
  Action probabilities also passed that bound. This measures agreement with the
  oracle, not perfect accuracy against dataset labels.
- Synthetic encoder/head maximum errors: `0.0000005` / `0.0000010`;
  raw logits passed `2e-4`, including the final-output-only readback checks.
- Pipeline and HTTP 512-task stress passed. HTTP concurrency 1/2/4,
  eviction/reload, and disconnect during observed GPU activity all passed.
  A subsequent request recovered with matching output after 5.69 seconds.
- The 30-minute soak completed **7,961 checked requests** in 1,800.19 seconds.
  Mixed-request HTTP p50/p95 were 147.07 / 600.52 ms.
- Retained host RSS grew from 426.07 to 467.32 MiB (**+41.25 MiB**).
  Device use grew from 2,934 to 2,944 MiB (**+10 MiB**).
  Both passed the 256 MiB growth gate.
- Sampled whole-GPU peak: **5,418 MiB (5.29 GiB)** across cold start, reload,
  stress, and soak. The first HTTP request, including model load, took 5.45 seconds.

Selected p50 measurements in milliseconds, using five warm-ups and 30 samples:

| Input profile | Tasks | Native CPU pipeline | CUDA pipeline | PyTorch CUDA forward only |
| --- | ---: | ---: | ---: | ---: |
| Fixed, 61 tokens | 1 | 2,267.585 | 16.373 | 31.120 |
| Fixed, 61 tokens | 8 | 9,665.956 | 83.112 | 50.881 |
| Mixed | 1 | 2,269.563 | 16.641 | 31.078 |
| Mixed | 8 | 27,898.042 | 261.752 | 134.397 |

The report includes the full CUDA sweep through batch 128. CUDA pipeline
latency was lower than the PyTorch forward-only comparison at batch one;
PyTorch was faster at batch eight. The timing scopes differ as described below.
These results qualify the tested fatbin configuration; portable PTX and the
separate `ReleaseSafe` CI configuration still need their own passing reports.

## PR review validation

The follow-up review built the focused `x86_64_v3` CUDA test executable in
`ReleaseSafe` and passed **49 selected tests with zero skips**. This run covered
kernel differentials, synthetic parity/batching, HTTP extraction, admission,
and readiness; it explicitly excluded the released-checkpoint benchmark test.
It does not replace the full `ReleaseFast` qualification report above or qualify
the full CLI's `ReleaseSafe` configuration.

NVIDIA Compute Sanitizer `memcheck` passed all three direct CUDA kernel tests
with **zero memory errors**. Six native regression tests, seven qualification
harness tests, and three CUDA artifact tooling tests also passed. Artifact source
policy, Python lint/format, Zig formatting, workflow syntax, and diff whitespace
checks passed.

Review tightened the harness to reject missing/skipped or incomplete test runs
and to reject cancellation recovery beyond 120 seconds. The saved CPU/CUDA test
logs satisfy the stronger selection checks, and the recorded 5.69-second recovery
satisfies the deadline. The original full qualification report remains unchanged;
the full soak was not repeated during this review. Metal hardware regression
was not rerun on this Linux host.

## Runtime

The Laya capability profile requires the encoder, RoPE, exact GELU, slicing,
integer embedding lookup, local attention, and action-feature kernels. Plain
ModernBERT remains outside the CUDA model support gate. Laya checkpoints with
sequences beyond 512 or encoder head dimensions beyond 128 are rejected.

Weights are converted once from their checkpoint dtype to resident FP32. The
initial implementation uses FP32 activations and existing strict FP32 matrix
multiplication. The generic CUDA fused activation API declines exact GELU because
its legacy kernel only understands activation IDs 0–5. Laya uses a dedicated
packed exact-GELU kernel, with resident exact GELU and multiplication as fallback.
Laya also uses strict FP32 cuBLASLt projections below the general 128-row
threshold, avoiding slow short-sequence projections at batch one.
No reduced-precision Laya weight policy is enabled. Batched RoPE
uses the number of heads per token to derive positions, independently of batch
size. Local attention applies an inclusive symmetric window and key padding
inside the CUDA kernel, without allocating a dense window-bias tensor per layer.

The head gathers integer marker positions on device, runs the scorer, and builds
the uncalibrated action features on device. Only decision logits and action
logits return to the host. Calibration and public decision decoding retain the
existing CPU/Metal semantics. Padding uses the upstream `-1e4` sentinel, entropy
uses its `1e-9` clamp, and valid-option count is divided by 255.

Every task is validated and tokenized before execution. CUDA groups profitable
64-token length buckets and executes chunks of at most 128 tasks, bounded by
shape-specific host and device admission. Disabling bucketing restores contiguous
chunks.
The workspace estimate covers simultaneously live encoder/head/scorer tensors;
it does not charge a dense quadratic attention allocation. Smaller chunks are
tried only when admission fails before execution, including temporary pressure
from an outer HTTP scratch lease or other live reservations. Kernel failures
are propagated without replay.
Results retain input and question order through the public 512-task limit.
Chunk scratch uses the freeing allocator, and execution permits are released
between chunks. Existing process isolation remains responsible for hard CUDA
cancellation; the new pipeline checks cancellation before preparation/chunks and
through the existing backend execution control.

HTTP qualification exposed a startup issue on the four-core L4 host: the
readiness refresh loop used `Io.Group.async`, which may execute inline when
the async worker pool is full. The loop now requires concurrent execution and
propagates launch failure, so it cannot trap startup before listener publication.
A regression exercises readiness startup with no async worker capacity.

## Hardware gate

Dispatch `.github/workflows/zig-inference-l4-spot.yml` with scope `laya` for the
fatbin path and `laya-portable` for PTX JIT. Both compile the CLI and a focused
qualification executable on the build runner, then execute on the L4 runner.
The report includes binary hashes, artifact identity, GPU/driver identity, pinned
oracle metadata, test logs, benchmark measurements, and HTTP memory samples.
The CUDA artifacts are regenerated with the repository's CUDA 13.2 script.

To run manually, first build from `zig/`:

```sh
python3 tools/run_bounded_zig_build.py build antfly inference-laya-cuda-test-build \
  -Doptimize=ReleaseSafe -Dcpu=x86_64_v3 -Dcuda=true -Dcuda-artifacts=fatbin \
  -Dmetal=false -Donnx=false
```

Then, from the repository root on an L4 machine with `uv` and `nvidia-smi`:

```sh
python3 scripts/laya_cuda_qualify.py \
  --binary zig/zig-out/bin/antfly --tests zig/zig-out/bin/laya-cuda-tests \
  --work-dir /tmp/laya-cuda-qualification --prepare \
  --report /tmp/laya-cuda-qualification/report.json
```

For a standalone `antfly-inference` binary, also pass `--standalone-inference`.
On a memory-constrained build host, `inference-bench-server` builds the same
production HTTP server without unrelated CLI commands. Its binary is
`zig/zig-out/bin/antfly-inference-bench-server`; use `--standalone-inference`
with that binary as well. It exposes `cuda-info` so the gate verifies the
artifact identity and Laya capability of the exact server under test.
Repeat with `-Dcuda-artifacts=portable` and a separate report. Missing CUDA,
missing fixtures, unexpected backend selection, numerical failure, skipped
qualification tests, or a soak shorter than 30 minutes produces a failing exit
status. Both CPU and CUDA runs must report a nonempty passing suite with zero
skips and the expected backend. The gate checks that the released-checkpoint test
ran, and additionally requires the CUDA kernel, synthetic parity, HTTP, and
readiness tests. Cancellation recovery must finish within 120 seconds; its HTTP
timeout is capped by the remaining recovery time. The workflow runs the harness
regression tests before hardware qualification.
Preparation downloads pinned inputs; it does not dispatch a workflow or
provision a GPU. Large checkpoint/data downloads and two PyTorch environments
(CPU oracle and CUDA comparison) require disk space and network access.

The gate covers:

- Split and interleaved batched RoPE, including offsets and multiple heads.
- Local attention at window/sequence boundaries through 512 tokens, padding,
  finite all-masked outputs, and comparison with native attention.
- Integer gather, action features, padded markers, option-count boundaries,
  and batches around 127/128/129/256 scorer dispatch sizes.
- Independent synthetic encoder/head/action-feature references and complete
  raw logits within `2e-4`; final-output-only CUDA readbacks.
- All 192 released-checkpoint examples, zero decision disagreements, and
  probability/action absolute error at most `5e-5`.
- Fixed and reversed mixed batches 1/2/4/8/16/32/64/128, plus 512-task stress;
  forced small-budget chunk planning and cancellation before execution.
- HTTP parity, grouping, per-input schemas, derived fields, unsupported-mode
  rejection, 512 tasks, concurrency 1/2/4 with batches of one and eight, model
  eviction/reload with a one-model cache, client disconnect during observed GPU
  activity followed by recovery,
  and a 30-minute mixed-request soak.
  The server uses four request slots and an explicit 3 GiB scratch admission
  budget: each HTTP preprocessing lease reserves 512 MiB, so the four-core
  host's automatic scratch budget otherwise correctly rejects concurrent work.
  Retained host RSS includes worker processes; host/device growth after warm-up
  must be at most 256 MiB. This is a retained-memory check, not a peak measurement.
  A separate 200 ms sampler records whole-GPU memory use during HTTP cold start,
  reload, stress, and soak, including the observed peak. Use an otherwise idle GPU;
  sampling can miss transients shorter than its interval.

Pipeline benchmarks use five warm-ups and 30 samples per shape and report median,
nearest-rank p95, and questions/second. Native CPU comparisons use batches 1 and
8. The L4 qualification executable targets `x86_64_v3`, supported by the GCP
runner CPU, so native comparisons use AVX2 and hardware FMA. A baseline x86
build can otherwise spend most of its CPU time emulating FMA in software.
The local comparison uses four vCPUs and the native kernels without system BLAS;
its timings do not describe the earlier Apple Accelerate CPU configuration.
The historical report above used PyTorch resident-forward timing. The updated
benchmark records full pipeline, prepared host inputs through host logits, and
resident-forward scopes separately. PyTorch uses eager FP32 with TF32 disabled;
full pipeline timing includes tokenization, padding, transfers, calibration and
decision decoding on both implementations. Assertions are outside the timer.
HTTP timings additionally include transport.

## Matched performance gate

After preparing the pinned fixtures, run:

```sh
python3 scripts/laya_cuda_performance.py \
  --tests zig/zig-out/bin/laya-cuda-tests \
  --work-dir /tmp/laya-cuda-qualification \
  --report /tmp/laya-performance/report.json
```

This requires an L4 and a ReleaseSafe, x86_64_v3, fatbin test executable. It runs
three rounds with ten warm-ups and 100 samples, reversing implementation order
between rounds. For both fixed and mixed batches of eight, candidate full-pipeline
p50 and p95 must be at most 1.10 times PyTorch. Batch-one p50 and p95 must remain
within 1.05 times the original path in the same binary. Every round must pass;
the bucketed mixed batch must also improve both percentiles by at least 5%
against fusion without bucketing, which the default invocation measures.
Prepared-input measurements are diagnostic, not substitutes for pipeline results.
Missing measurements, unexpected skips, and inactive optimized routes fail the
gate. The L4 `laya` workflow runs this gate after correctness qualification.

The new Laya attention path distributes key scoring across four warps, retaining
FP32 softmax and value accumulation. It applies to SM 8.9, batches of at least two,
head dimensions 64/128, and sequences through 512; other shapes retain the prior
kernel. Packed exact GELU multiplies the two projection halves in one launch,
eliminating two slices and a separate multiply. Optional stable 64-token length
buckets apply only to at least eight tasks with at least 20% padding savings and
restore original output order. Admission and the 128-task chunk bound still apply.

Use `ANTFLY_CUDA_LAYA_OPTIMIZATIONS=0` for the original kernels and contiguous
batching. `ANTFLY_CUDA_LAYA_FUSION=0` disables only packed GELU;
`ANTFLY_CUDA_LAYA_BUCKETING=0` disables length bucketing. Run
`--variants baseline attention fused bucketed candidate` to
measure the contributions separately. Short exploratory runs cannot produce a
passing qualification report. No reduced precision or dense quadratic attention
workspace is introduced. The historical report does not qualify these changes.

Qualification applies to the tested hardware, build, and artifact mode. Review
the corresponding report before enabling another configuration. The HTTP
cancellation test resets the client
after observing GPU activity and requires subsequent parity; it does not simulate
a permanently wedged kernel. Existing process-cancellation E2E tests cover the
watchdog itself. Neither runtime support nor a compile-only result establishes
the numerical or performance release gates.
