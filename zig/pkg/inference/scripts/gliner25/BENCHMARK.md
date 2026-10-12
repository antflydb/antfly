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
zig build inference-bench-gliner25-cpu-build -Doptimize=fast -Dmetal=false -Dcuda=false -Donnx=false -Dpjrt=false -j1
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

## Reference results

The recorded macOS arm64 run (one CPU math thread, all three FP32 checkpoints,
all ten requests) measured paired native/Python latency ratios of 0.53–0.80
(0.58–0.80 for base). Mixed-task medians were 14.5/18.7 ms (small),
36.2/45.2 ms (base), and 41.1/51.2 ms (multi). The report's
`performance_release_qualified` and `serving_qualified` fields remain false.

> **Relocated:** the 2026-09-09 recorded CPU run → [CPU benchmark runs](../../../../../docs/design/inference/history/gliner2/gliner25-cpu-benchmark-runs.md#recorded-cpu-run-2026-09-09).

## Portable Linux x86 CPU execution

Linux x86-64 release binaries retain their baseline CPU requirement and do not
require OpenBLAS. FP32/F16-weight GEMM selects an isolated AVX2/FMA/F16C object
only after CPUID and OS XMM/YMM-state checks. Sequential attention uses the same
selection. The baseline kernels use separate multiply/add rather than scalar
software FMA. macOS, Accelerate, and ARM arithmetic are unchanged.

Use `-Dblas=auto` (the default) for optional BLAS, `-Dblas=linked` to require
link-time BLAS, or `-Dblas=off` for a binary with both BLAS paths disabled.
For same-binary runtime comparisons, use `ANTFLY_INFERENCE_BLAS=off` versus
`auto` on an auto-policy build without `-Dblas-root`. The old `-Dsystem-blas`
and `-Druntime-openblas` build flags are compatibility options; do not mix them
with `-Dblas`.

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
unsupported-force rejection locally. CI does not install or require QEMU;
these execution checks are skipped when it is unavailable. Native GNU/musl
checks, including libc-free builds, still run. The checker also audits assembly for software
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

### Open work

Intel/AMD repeat sessions, concurrency, and cold-build memory qualification
remain outstanding. The 2026-10-01 single-host Xeon diagnostic ran
sequential direct-model requests only and did not measure a matched
original-release baseline, so it does not establish the 10x release gate.

> **Relocated:** the 2026-10-01 local Linux diagnostic → [CPU benchmark runs](../../../../../docs/design/inference/history/gliner2/gliner25-cpu-benchmark-runs.md#local-linux-diagnostic-2026-10-01).
