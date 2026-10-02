# GLiNER2.5 CPU comparison

For the matched Antfly Metal versus Fastino MPS/CPU campaign, see
[METAL_BENCHMARK.md](METAL_BENCHMARK.md).

This harness compares the native **direct core** with pinned Fastino CPU FP32
inference. It does not exercise HTTP, model resolution, admission, batching,
serving cancellation, Metal, quantization, or training. A completed report is
not a production performance qualification.

The native worker and Python worker load the same immutable model bundle once.
The driver starts them serially, then executes one worker at a time in balanced
AB/BA pairs. Both model copies remain resident. Default CPU math budget is one
thread: native scheduling uses serial Io; BLAS environment controls and Torch
intra/inter-op settings are recorded. With system BLAS disabled, the native
worker admits only the one-thread profile.

Each measured call includes schema parsing and compilation, preprocessing,
encoder, all requested learned heads, decoding, and temporary cleanup. Model
loading, protocol parsing, wire serialization, and destruction of the returned
result are excluded. Both implementations rebuild their schema inside every
measured call; this harness does not compare a cached schema against a fresh
schema. Native execution also retains cooperative deadline checks.

Before timing a workload, the driver requires exact encoder token IDs, source
spans, text, labels, selected records, ordinary relations, and typed JointIE
endpoints against the checked-in variant reference. Only confidence tolerates
an absolute FP32 difference of `5e-4`. Each warmup and measured output is checked
again outside the clock. Any mismatch prevents a successful report. Startup and
shutdown rehash the actual model bytes; the driver also rechecks the executable
and source checkout. No model download is performed.

Build from `zig/`, through the repository graph:

```sh
zig build inference-bench-gliner25-cpu-build -Doptimize=ReleaseFast -Dmetal=false -Dcuda=false -Donnx=false -Dpjrt=false -j1
```

Pass these flags explicitly: the modular repository build uses one shared
dependency graph, and the worker rejects other optimization/backend profiles.
The executable is installed at
`zig/zig-out/bin/antfly-inference-gliner25-cpu-bench` relative to the repository
root. A standalone build from `zig/pkg/inference/` uses the unprefixed target
`bench-gliner25-cpu-build` and its package-local `zig-out/bin/` instead.

Run an initial bounded smoke from the repository root with the pinned oracle
venv and already verified model directory:

```sh
PYTHONDONTWRITEBYTECODE=1 /private/tmp/antfly-gliner25-oracle-venv/bin/python zig/pkg/inference/scripts/gliner25/benchmark_cpu.py run --native-bin zig/zig-out/bin/antfly-inference-gliner25-cpu-bench --model small --model-root /private/tmp/antfly-gliner25-models --upstream /private/tmp/antfly-gliner25-upstream --cases mixed_tasks --threads 1 --warmup 1 --pairs 2 --max-rss-mib 8192 --output /private/tmp/gliner25-cpu-smoke
```

Use `--model base`, `--model multi`, or explicitly `--model all` for serial
variant comparisons. Omitting `--cases` selects all ten captured task requests.
The default is two warmups and six measured pairs per case. These short curated
requests cover task correctness, not a representative throughput corpus.

Workers have command/response size caps, per-call deadlines, explicit stop
handshakes, startup timeouts, and a shared RSS ceiling sampled every 100 ms.
This is an observed RSS guard, not a kernel memory reservation; transient peaks
between samples may be missed. On failure, only spawned workers are terminated,
`report.json` is marked failed, and diagnostic logs are retained. Successful
reports retain actual preflight outputs/tokens, immutable artifact identities,
thread/build profiles, raw paired timings, paired latency-ratio confidence
intervals, observed RSS, and a content manifest. Ratios below one mean native
latency was lower on that particular workload.

These benchmark scripts and outputs are intentionally outside the oracle
reference-generator closure.

## Recorded CPU run, 2026-09-09

All three immutable FP32 checkpoints completed all ten requests, with exact
encoder IDs and output decisions checked before timing and output parity
checked again for every warmup and measured call. The run used macOS arm64,
Zig 0.16.0, ReleaseFast throughout the native graph, system BLAS, one math
thread, three warmups and twenty balanced measurement pairs per request.
Both model copies remained resident; the combined RSS ceiling was 6144 MiB.

Values below are the paired native/Python latency ratio and a 95% bootstrap
interval over the twenty pairs. A ratio below one favors native execution.

| Task | Small ratio [95% CI] | Base ratio [95% CI] | Multi ratio [95% CI] |
| --- | --- | --- | --- |
| Constrained classification | 0.609 [0.564, 0.795] | 0.653 [0.639, 0.664] | 0.716 [0.672, 0.732] |
| Entity attributes | 0.767 [0.751, 0.825] | 0.777 [0.768, 0.781] | 0.790 [0.771, 0.796] |
| Enum field | 0.582 [0.564, 0.724] | 0.627 [0.619, 0.648] | 0.694 [0.681, 0.712] |
| JointIE | 0.577 [0.572, 0.623] | 0.669 [0.665, 0.673] | 0.692 [0.678, 0.710] |
| Legacy structure | 0.537 [0.519, 0.674] | 0.591 [0.584, 0.607] | 0.614 [0.606, 0.623] |
| Mixed tasks | 0.773 [0.751, 0.897] | 0.801 [0.785, 0.806] | 0.798 [0.790, 0.818] |
| Anchorless records | 0.649 [0.546, 0.713] | 0.619 [0.612, 0.626] | 0.634 [0.626, 0.642] |
| Latent records | 0.529 [0.514, 0.653] | 0.583 [0.566, 0.594] | 0.562 [0.540, 0.570] |
| Natural records | 0.639 [0.527, 0.679] | 0.605 [0.590, 0.618] | 0.611 [0.596, 0.621] |
| Unicode offsets | 0.791 [0.781, 0.917] | 0.786 [0.769, 0.800] | 0.791 [0.784, 0.796] |

Mixed-task median native/Python latency was 14.469/18.692 ms for small,
36.174/45.159 ms for base, and 41.093/51.173 ms for multilingual. Combined
sampled worker RSS peaked at 1389.7, 2297.8 and 2925.1 MiB respectively; these
are two-process observations, not individual inference memory requirements.

The local evidence directory is `/private/tmp/gliner25-cpu-all-tasks-v2`.
`report.json` SHA-256 is
`fd6aa011026bcd6cef008eb46205cc09ffe82c8a83a3c0c1051064951a250776`;
the measured native executable SHA-256 is
`defa2bc50b4204792b3c563cd61fc43c0a9d714153fb58d0fda3928c118d7394`.
The report includes exact artifact/runtime pins, every preflight output, raw
pairs, worker commands and the evidence content manifest. Preserve that
directory with any qualification review; a rebuild is a different artifact.

This is one host/session and ten short curated requests per variant. The
report identifies arm64 but does not capture the exact CPU chip, affinity,
thermal state or power state. Small-model Python variability was 8.6–16.6%
coefficient of variation across these tasks, so bootstrap intervals describe
this sample rather than independent host-session reproducibility. The run
does not establish production p95, concurrency, corpus quality, long-document,
Metal, reduced-precision, training or serving performance. Its
`performance_release_qualified` and `serving_qualified` fields remain false.

## Portable Linux x86 CPU execution

Linux x86-64 release binaries retain their baseline CPU requirement and do not
require OpenBLAS. FP32/F16-weight GEMM selects an isolated AVX2/FMA/F16C object
only after CPUID and OS XMM/YMM-state checks. Sequential attention uses the same
selection. The baseline kernels use separate multiply/add rather than scalar
software FMA. macOS, Accelerate, and ARM arithmetic are unchanged.

Linux x86 GNU builds now prefer optional runtime OpenBLAS for FP32 GEMM when a
compatible LP64 pthread library is installed. The amd64 runtime image includes
it; other installations retain native fallback when it is absent. See
[`NATIVE.md`](../../NATIVE.md) for installation, thread limits, and opt-outs.
Set `ANTFLY_INFERENCE_BLAS=off` when comparing the native kernel routes below.

`ANTFLY_INFERENCE_X86_KERNEL=auto|portable|avx2` selects a diagnostic route on
Linux x86. The default is `auto`; unsupported forced AVX2 rejects before entering
an optional-instruction kernel. `ANTFLY_INFERENCE_CPU_THREADS=1..8` caps native
math workers, additionally bounded by affinity and whole-core cgroup quota.
Both settings are read once per process. Restart after changing them or moving
a process to a different CPU quota. These settings do not retune macOS kernels.
The environment overrides require a libc-linked build; libc-free builds use
automatic dispatch and the affinity/cgroup budget.

The CPU comparison worker now accepts bounded native math pools without BLAS.
The driver sets the native thread cap along with BLAS/Torch thread variables,
keeping the native fallback at at most eight workers while preserving the
requested BLAS/Torch budget of up to 32 threads. It records selected kernels
and effective thread counts. Runtime-loaded OpenBLAS additionally clamps its
thread count to the native CPU budget; the worker reports `openblas_threads`
and rejects a requested count that does not match the actual limit. Explicitly
linked BLAS retains its existing thread controls. Request timeouts can
be increased to 300000 ms to capture the old Linux baseline. Kernel benchmark
`*_ms` fields now report time per measured iteration, rather than the sum of
all iterations (primitive fields still include `primitive_repeats`).

Run the model-independent dispatch, tail, attention, concurrency, numerical,
and GNU/musl checks from the repository root:

```sh
python3 zig/tools/verify_linux_x86_kernels.py --zig /path/to/zig
```

Install `qemu-x86_64` and pass `--require-qemu` to require no-AVX execution and
unsupported-force rejection. The checker also audits assembly for software
FMA in portable GEMM and vector FMA in the accelerated object.

For end-to-end HTTP evidence, `benchmark_linux_cpu_http.py` compares already
running CPU-only baseline/candidate servers on loopback, with an optional
same-precision reference. Its case manifest contains `cases` (each with `id`,
`path`, `request`, the complete qualified `expected` response, and the existing
`confidence_tolerance`) and `artifacts` (paths to server binaries and model
files). It hashes artifacts before/after, validates every response, alternates
arm order, retains raw outputs/timings, and computes paired confidence intervals.
An HTTP error, output mismatch, timeout, or changed artifact marks the report
failed. Use separate output directories for the three qualification sessions.

```sh
python3 zig/pkg/inference/scripts/gliner25/benchmark_linux_cpu_http.py \
  --cases /tmp/linux-gliner-cases.json \
  --baseline http://127.0.0.1:8081 --candidate http://127.0.0.1:8082 \
  --reference http://127.0.0.1:8083 \
  --warmup 3 --pairs 20 --output /tmp/linux-gliner-http-session1
```

This harness records evidence, not automatic release approval. Confirm CPU-only
server configuration, equal resource budgets, model identities, and matching
precision before interpreting ratios. Release qualification requires the full
Intel/AMD workload matrix, exact qualified outputs, >=10x improvement on the
reproduced slow cases, and <=1.5x median latency versus a matched optimized
same-host reference. Report p95, concurrent throughput, and RSS separately;
a kernel microbenchmark or successful fixture smoke does not establish those
serving properties.

### Local Linux diagnostic, 2026-10-01

On the available Intel Xeon 2.2 GHz host, baseline-x86 ReleaseFast binaries
without system BLAS selected AVX2 at runtime. With two math threads, three
warmups, and twenty measured requests per case:

| Model / workload | Native warm median | Same-host PyTorch FP32 | Native / reference |
| --- | ---: | ---: | ---: |
| GLiNER2 Q4_K, four-entity request | 272.940 ms | No matching quantized reference | — |
| GLiNER2.5 base, ten task fixtures | 136.002–225.487 ms | 154.200–189.298 ms | 0.879–1.221 per case |
| GLiNER2.5-Decide, two questions | 674.885 ms | 591.277 ms | 1.141 |

All ten base fixtures passed in both automatic and portable modes. Decide's
classifier and CountLSTM entity parity tests passed in both modes; maximum
classifier logit error was 6.2e-6. The Q4_K request returned the same four
entities, offsets, ordering, and printed confidence values in both modes;
portable warm median was 585.060 ms. This portable route includes the software
FMA fix and is **not** the original slow release baseline.

The Python comparison used the pinned upstream source with Python 3.11.2,
Torch 2.9.1+cpu, and Transformers 4.55.4; it does not satisfy the frozen oracle
runtime identity.
These are sequential direct-model diagnostics, not paired HTTP qualification.
No matched original-release baseline or matched optimized Q4_K reference was
measured in this campaign, so it does not establish the 10x release gate.
The raw local reports, binary hashes, and host provenance are in
`/tmp/antfly-linux-cpu-evidence-20261001/report.json`. Intel/AMD repeat sessions,
concurrency, and cold-build memory qualification remain outstanding.

Model-independent verification passed all 45 tests for GNU and musl in both
automatic and forced-portable modes, plus all 45 under no-AVX QEMU. Forced
AVX2 under no-AVX QEMU rejected safely. The tested macOS ARM GEMM object was
byte-identical to the unchanged source's object.
The focused production inference executable also built and passed its `--help`
startup smoke with baseline x86-64, GNU 2.28, ReleaseFast, stripped symbols, and
all GPU/system-BLAS backends disabled. This is not a full standalone/CAPI build.
