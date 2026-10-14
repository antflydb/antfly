# GLiNER2.5 CPU benchmark runs

> Relocated verbatim from `zig/pkg/inference/scripts/gliner25/BENCHMARK.md` (lines 71–118 and 201–236 at commit 3ce5aff4ea) on 2026-10-12. Durable decisions were folded into [BENCHMARK.md](../../../../../zig/pkg/inference/scripts/gliner25/BENCHMARK.md) and [GLINER25.md](../../../../../zig/pkg/inference/models/gliner2/GLINER25.md) before the move.

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
