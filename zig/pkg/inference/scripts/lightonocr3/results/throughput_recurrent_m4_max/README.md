# Recurrent and paged-attention throughput, M4 Max

This follow-up qualifies the next kernel and scheduling changes against commit
`fd99578cf8832da9493026ccaa12ace48d32f1b8`. The machine is a shared 36 GiB Apple
M4 Max. Timings establish scale and regressions; they do not establish universal
parity with vLLM. The original evidence in `../throughput_parity_m4_max` is retained.

## Integrated behavior

- Recurrent decode reduces over keys across SIMD lanes. Each lane loads its part
  of the F32 state once, updates it in registers, and stores it once. One dispatch
  covers one to eight pages, with independent histories and state. An inline
  argument table avoids additional GPU allocations.
- Recurrent prefill prepares Q/K and gates, and normalizes the output, with one
  SIMDgroup per token/head. The existing serial recurrence and inter-stage
  barriers remain intact.
- Language prefill attention uses F32 query/probability tiles and the existing F16
  paged KV. The preferred 16-query/64-key TensorOps kernel reads page-sized KV
  segments directly, removing threadgroup KV staging. Unsupported page sizes or
  missing pipelines use the independently checked staged kernels or old route.
- Page workers and benchmark request workers claim pending work from a shared
  atomic index, preserving original result order. This avoids leaving short-page
  workers idle behind a statically assigned long page.
- Logical active windows can reach sixteen pages, independently of the physical
  decoder's eight-row limit. Each page still takes its own admission lease. The
  default stays eight; larger occupancy did not reliably improve the measured workload.
- LightOn scheduler turn waiters park for 100 microseconds after a failed claim,
  instead of repeatedly yielding with zero duration. Uncontended claims return
  immediately. Other model policies preserve their existing behavior.
- Additional decode encoder reuse tracks all indirect recurrent accesses and
  norm/residual/split-down dependencies. It remains opt-in because reader gains
  were inconsistent. Vision/prefill batching and longer waits also remain opt-in.

The three kernel families default on only behind the qualified LightOn/M4 Max
model/device gates. Original BF16 weights and F32 decoder activations,
probabilities, accumulation, outputs and recurrent state are preserved. No new
persistent GPU allocations or memory-admission exemptions are introduced.

Rollback switches:

```text
TERMITE_METAL_DISABLE_LIGHTONOCR3_RECURRENT_PARALLEL=1
TERMITE_METAL_DISABLE_LIGHTONOCR3_PREFILL_CORE_NORM=1
TERMITE_METAL_DISABLE_LIGHTONOCR3_PREFILL_ATTENTION_MPP=1
TERMITE_METAL_DISABLE_LIGHTONOCR3_PREFILL_DIRECT_KV=1
ANTFLY_LIGHTONOCR3_CONTENTION_WAIT_US=0
ANTFLY_LIGHTONOCR3_STATIC_PAGE_ASSIGNMENT=1
```

Each kernel family's matching `TERMITE_METAL_ENABLE_...=0` also rolls it back.
Direct-KV rollback retains the staged TensorOps path. Encoder reuse is enabled by
`TERMITE_METAL_ENABLE_LIGHTONOCR3_RECURRENT_COALESCING=1`.

## Kernel measurements

`shipping_kernel_summary.json` contains balanced old/new GPU medians, raw-log
hashes and source identity. Thirty trials cover each prefill case; recurrent
cases rotate three variants for 36 trials and average sixteen steps per command
buffer, retaining both CPU encoding and GPU timings. GPU workloads from this
benchmark run sequentially; other machine workloads remain uncontrolled.

| Operation | Qualified cases | New / old GPU latency |
|---|---:|---:|
| Recurrent decode, heads 16/32, pages 1–8 | 16 | 0.409–0.848 |
| Full recurrent prefill core, lengths 128/432/662/2048 | 8 | 0.214–0.892 |
| Paged language attention, lengths 128–2048, heads 8/16 | 12 | 0.080–0.573 |

These compare to the preceding native kernels, not vLLM. In particular the
attention speedups are against the old scalar path; they are not gains over the
previously explored staged TensorOps prototype.

`shipping_prefill_attention_page16_mlx_pair.json` compares the actual production
attention path with MLX, alternating all six participant orders for 36 trials.
Native uses permuted 16-token pages; MLX uses contiguous KV. Native queries,
probabilities and accumulation remain F32; the production-style MLX participant
uses BF16 and a second participant uses F32. Both share identical numeric source
values, with existing half KV on the native side. Full F32-reference output
errors are below 1.2e-7. The 4B/1024 case reaches F32 latency parity, but other
shapes and the BF16 comparison retain gaps. This is not a precision-matched
BF16-parity claim. `shipping_prefill_attention_mlx_pair.json` separately uses
128-token pages, as did the exploratory candidates.

## Reader measurements

Same-process C8/rows8/window8 rounds use sixteen mixed pages, original BF16
weights with native weight residency, greedy generation and a 256-token ceiling.
All timed outputs match their serial native references, including the exact
saved native output from the preceding PR version. Markup-stripped content also
matches the fresh vLLM references. Three fixtures do not establish general OCR
accuracy equivalence.

| Model | Native kernel off / on / off / on, pages/s | Fresh reference, pages/s |
|---|---:|---:|
| LightOnOCR-3 0.8B | 1.9333 / 2.0396 / 1.9114 / 2.0945 | vLLM Metal 3.0045 |
| LightOnOCR-3 4B | 0.5022 / 0.4658 / 0.4018 / 0.3919 | vLLM Metal 0.7799 |
| Florence-2 base | 3.7687 | MLX-VLM serial 4.7699 |

The 0.8B bracket supports a roughly 6–10% gain from this kernel pass. The 4B
control itself drifts by 20%; these rounds do not establish a reader gain.
Some reader rounds overlap CPU compilation, and other machine activity is not
controlled. A final 4B off/on/on/off bracket after our compilation ends
gives 0.4454 / 0.4393 / 0.4996 / 0.4285 pages/s, still short of parity. Full-reader parity remains unfinished. Florence has no adapter in
the installed vLLM Metal version, so its reference is MLX-VLM serial execution.
Native Florence uses Q4_K weights with F16 encoder mirrors and its existing
no-repeat policy; the MLX checkpoint uses F16 and greedy decoding. This is not
an equal-precision or equal-policy comparison.

In the same resident 0.8B run, zero-duration turn polling used 18.269 CPU seconds
at 2.0338 pages/s; the adjacent 100-us wait used 11.828 CPU seconds at 2.0450
pages/s. The 4B counterparts used 74.331 / 51.975 CPU seconds at 0.4232 / 0.4505
pages/s. These counters cover all process threads, excluding model loading and
reference generation. The wait reduces CPU contention without introducing an
unconditional delay before uncontended work.

A separate eight-page request uses three workers, three physical rows and a
three-page window. Static strides assign all long receipt pages to one worker.
The shared queue allows earlier finishers to claim pending pages:

| Adjacent round | Static pages/s | Shared queue pages/s |
|---|---:|---:|
| 1 | 0.6651 | 1.0904 |
| 2 | 0.8001 | 1.1731 |
| 3 | 0.9110 | 1.0729 |

This is a constrained-worker workload, not a 64% gain for the default eight-page
window. The three comparisons retain substantial drift but all favor the queue.

The larger-window checks prove sixteen registered pages and an eight-row
physical limit. An earlier repeated C16 control used eight-page windows at
2.10–2.14 pages/s versus sixteen-page windows at 1.81–1.87, with 30–38 versus
104–111 CPU seconds per 32 pages. With the new wait, later C16 controls remain
noisy (window8 1.4872; window16 1.2912–1.6070 pages/s), and combined vision/prefill
batching gives 1.5599. Bigger windows and longer waits are not promoted as faster
defaults. Extra encoder reuse is also inconsistent (0.8B 1.8974 versus adjacent
2.0450 and 2.0298), so remains opt-in.

The earlier 146-case synchronized per-operation wall-time sweep is not evidence
of whole-graph throughput or universal GPU-kernel parity.

The unresolved throughput work includes the remaining attention gap against MLX
BF16, noncontiguous recurrent-state access, CPU dispatch/allocation/environment
lookup costs, and vision preprocessing inside the execution lock. A sampled 4B
process includes those host operations but does not isolate their causal cost;
the profile is retained for follow-up. Moving preprocessing outside the lock
requires preserving cancellation and per-page memory charges. Physical batches
above eight require broader runtime qualification. No unqualified precision or
admission shortcut is enabled to claim parity.

## Correctness and reproducibility

The final selected Zig suite passes 81/81 tests with real checkpoint GPU work,
including independent pages, cancellation, memory accounting and HTTP behavior.
A separate default-policy real-checkpoint/HTTP run passes as well. The CPU-only
build passes 75 tests and skips the six GPU-dependent tests.

C conformance covers every recurrent batch size, multi-step continuation,
history swaps, cold/continued state, shared weights, nonzero offsets, full output
and persistent buffers, canaries, planned producers and consumers, cancellation,
missing-pipeline and explicit-disable fallbacks, and encoder ownership. Prefill
attention also covers ragged queries, causal prefixes, reversed page tables,
page sizes 8/16/128, padded strides, both encode APIs and NaN/Inf Q/K/V. Vision
conformance is rerun against the integrated source.

`verified_build_manifest.json` identifies the final Zig binary, base commit,
runtime patch and source hashes; `verified_runtime.patch.gz` captures the exact
runtime delta. `shipping_build_manifest.json` identifies the main reader and
kernel benchmark build. The final rebuild only strengthens a test's assertions
so they execute in ReleaseFast; production functions are unchanged. Earlier
manifests/patches identify intermediate builds. Saved
reports take runtime identity from their build manifest, even if later source
edits exist in the worktree. Logs and raw JSONL samples are deterministic gzip
files under `logs/`; benchmark helper/prototype sources and their checksums are
under `experiments/`.

To reproduce the checks, build the filtered `inference-test` target in
ReleaseFast, then compile `check_metal_projections.m`,
`check_metal_prefill_attention.m` and `check_metal_vision.m` with Clang ARC and the
Foundation, Metal, MetalPerformanceShaders and Accelerate frameworks. The first
harness accepts `--benchmark-recurrent` and `--benchmark-prefill-core`; attention
accepts `--benchmark`. `benchmark_concurrency.py --policy-rounds off on off on`
measures the kernel bracket inside one resident checkpoint. The archived
`run_reader_matrix.py` records the exact model, fixture and budget arguments
used here. The native API timing excludes loading, serial reference reads,
HTTP/PDF/indexing; vLLM includes loopback HTTP. Every timed native output must
match the same-model serial reference. These three synthetic fixtures are a
consistency gate, not a broad OCR accuracy evaluation.

Rejected experiments (F32 staged KV, SIMDgroup-only matrices, wider cooperative
tiles and alternate query tile sizes) are explicitly labeled `candidate`.
Their raw reports and archived sources are evidence of experiments, not
shipping performance. The first direct-attention timing attempt overlapped a
conformance run; use `prefill-final-benchmark`, not `prefill-direct-benchmark`,
for the final old/new table.
