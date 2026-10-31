# AU: CUDA projection launch families

Baseline: `ca07c2846a`. Implementation: `e5fd7dc9ff`.

**25 public launch adapters now share three compile-time owners:** nine argmax,
eight paired projections, and eight QKV projections. Net maintained production
reduction is **24,575 bytes / 426 lines**. New test/benchmark tooling and inline
tests are accounted separately. CUDA device source, compiled images, generated
regions, CPU production and Metal production are unchanged. This is a CUDA
host-launch reduction, not a reduction of device arithmetic.

`QuantProjectionLaunch` carries concrete kernel-field identity, codec block
geometry, column tile, thread count, activation representation and optional
four-row QKV kernel. Every caller supplies a compile-time value. `@field` resolves
the named handle directly; there is no runtime descriptor interpreter, format map,
allocation, or additional driver call. The existing `launch_abi` machinery and
`kernel_abi.cuh` remain the native wire-order authority.

The owners preserve the original differences:

- Last-row and per-row argmax retain their distinct partial sizes, reduction
  records and grids. Both remain two native kernel launches.
- Float and Q8_1 activation buffers retain their byte extents and native ABIs.
- Untiled Q4_K QKV retains its element grid. Tiled adapters retain their original
  column geometry and thread counts, including the optional four-row route.
- The four-row QKV adapter retains its late availability check: shape/buffer
  admission and empty-output handling precede fallback-kernel availability.
  Other adapters retain their earlier availability checks.
- Public signatures, validation order, checked dimension conversions and empty
  behavior are preserved. Kernel selection and arithmetic schedules are unchanged.

## Correctness and source qualification

The native-wire fixture runs without CUDA installed. It records the real launch
boundary: function identity, stream, grid/block dimensions, shared memory and
all pointer/scalar arguments. **1,458 cases** cover all 25 adapters, one/three/five
rows, optional four-row availability/fallback, empty extents, missing kernels or
reducers, invalid input dimensions, every relevant short buffer and dimensions
wider than the native scalar ABI. Launch counts and context-current calls must
also match. Every case passes against the frozen original adapters and the
independent explicit native contract.

On Glowworm's NVIDIA GB10 (compute capability 12.1, driver 580.178.04), the
independent device fixture passes **156 profiles**, both for the candidate and
for the frozen-original comparison. It covers:

- Q8_0, Q4_0, Q4_K and Q6_K, float/Q8_1 activations, paired outputs, unequal Q/KV
  output widths, and last-row/per-row suppressed argmax;
- K=256, output widths 17/32, KV widths 7/16 and one/two/five rows, including
  the optional four-row route and explicitly forced fallback;
- exact independently encoded arithmetic, prefix/suffix output canaries,
  read-only inputs/weights/suppression and exact direct launch counts;
- two graph replays per profile, changing input signs and suppression IDs on
  the device after capture. Adapter-local native records have gone out of scope
  before replay, exercising the actual copied CUDA parameter lifetime.

All three selected qualification tests pass with **zero skips**. The final
permanent native-contract suite passes with CUDA disabled too. CPU/Metal/CUDA
compilation and generated freshness (70 files + 129 native regions) pass.

Source proof verifies unchanged public signatures and no production edits
outside the selected adapters and their new owners. Qualified and final
production source match after excluding test blocks and normalizing blank-line
separation. Device sources and compiled images are byte-identical to baseline.
Frozen adapters are present only in the qualification test scope; they are not
retained as a second production implementation.

## Host submission qualification

The single declared timing experiment passes **all 150 profile/order checks**,
including all AA controls. Candidate median ratios range **0.988–1.012**;
maximum candidate p95 is **1.056**. Every profile/order also passes four
consecutive 16-pair trial medians (maximum across all trials: **1.018**).
Preset caps are 1.05 for paired median/trial medians and 1.10 for paired p95.

The host is idle for 60 seconds before the assay: no compiler or CPU-heavy test
processes, and zero reported GPU utilization. Each adapter is measured at one
and eight rows, K=1536, N=1025 and KV=257, with AA/AB/BA ordering and 64 adjacent
pairs. Each arm measures **32 native adapter submissions**. Stream synchronization
before and after each arm is outside the measured interval.

This qualifies host submission overhead for these profiles. It is not a GPU
arithmetic speedup or a model-throughput claim. Device work is independently held
constant by wire traces, output/replay checks and unchanged device images. The
separate AS Metal host-tail limitation is unaffected. No failed timing experiment
was automatically retried.

## Reproduction and evidence

`prepare_cuda_projection_qualification.py --baseline <revision> --output <path>`
prepares frozen original adapters in a disposable worktree. It rejects changed
public signatures, shared launch helpers, referenced scalar constants, native
ABI, driver/buffer/context code, device sources or images. The generated test
runs the wire and device suites; `ANTFLY_CUDA_PROJECTION_BENCH=1` enables the
explicit AA/AB/BA submission assay. Its module docstring gives build/run steps.

Evidence is retained in `.benchmark-assets/1166/au-campaign/`, including source
pins, original adapters, Linux ARM64 qualification binary, independent tests,
source accounting/proof, raw timings and `collected/timing.json`.
The older partial Glowworm snapshot rejected a source patch before application;
the exact current sources were instead cross-compiled locally and the pinned
binary executed in the owned remote worktree. Initial fixture compile errors
are retained in the build logs. The temporary remote worktree is removed after
collecting its results; other remote jobs and worktrees are untouched.
