# OCR execution contention on M4 Max

This pass follows `73a0e03c048f34856d036c969b2921bc01ac3abd` and the
[previous pipeline pass](../throughput_pipeline_m4_max/README.md).
Full-reader parity with production vLLM Metal remains unfinished.

## Shared execution gate

The shared LightOn execution gate can sleep between failed lock attempts after
64 brief spins. Its epoch policy selects the interval, capped at 100 microseconds;
zero retains the existing yield loop. Cancellation is checked on every attempt
and after acquisition, releasing the newly acquired gate if that check fails.
The per-epoch gate interval is reported by the benchmark. Generic callers keep
the existing yielding behavior. `ANTFLY_LIGHTONOCR3_DISABLE_GATE_PARK=1` restores
that behavior for the shared LightOn reader too.

BF16 checkpoint storage, F32 decoder activations/accumulation/recurrent state,
and F16 paged KV are unchanged. Memory ownership and admission are unchanged.
Florence additionally tests checked device-side no-repeat selection below; its
precision/policy limitations remain documented in the prior report.

The paired C8/R16 gate controls measure:

| Model | Yield / sleep / sleep / yield, pages/s | Process CPU seconds, same order |
|---|---:|---:|
| 0.8B | 2.2698 / 2.3323 / 2.2925 / 2.2617 | 10.149 / 5.389 / 5.970 / 9.828 |
| 4B | 0.6442 / 0.6248 / 0.5877 / 0.5392 | 29.351 / 18.265 / 19.213 / 37.925 |

The 0.8B bracket supports roughly 40–46% less process CPU time and a small
throughput improvement. The 4B CPU reduction persists, but wall time and framed
GPU time drift strongly across its controls, so these rounds do not establish
a 4B throughput gain. Both sizes preserve exact serial/cross-runtime fixture
outputs. Reported gate intervals are zero only in the controls and 100000 ns
in the enabled rounds. Optional decoder descriptor retention stays disabled.

## Excluded vision-cache experiments

`epoch` retained immutable split patch weights, position tables and vision
handles across pages in a shared reader epoch. It preserved the original split
F32 patch projections and learned-position math, with separate epoch-long
host/backend reservations before allocation and release after backend retirement.
Per-image activations and outputs kept their individual ownership. Full-vision
reordered-image outputs were bit-for-bit identical for both model sizes, and
cancellation/recovery and default HTTP batch checks passed.

The first implementation used a separate hook to encode vision before prompt
assembly. `epoch_locked` instead kept both stages within the existing critical
section. Neither demonstrated a consistent reader gain, so both are excluded
from the production runtime rather than adding retained data without evidence
of benefit. Exact source patches, build fingerprints, benchmark source and
lossless logs preserve both experiments.

Sixteen mixed pages, C8, eight physical rows, an eight-page window, greedy
sampling and maximum 256 generated tokens:

| Candidate | Model | Cache off / on / on / off, pages/s |
|---|---|---:|
| Separate hook | 0.8B | 2.4399 / 2.4395 / 2.4247 / 2.4521 |
| Separate hook | 4B | 0.6856 / 0.6849 / 0.6695 / 0.6505 |
| Existing critical section | 0.8B | 2.4718 / 2.4403 / 2.4610 / 2.4431 |
| Existing critical section | 4B | 0.6924 / 0.6584 / 0.6753 / 0.6323 |

All timed outputs match their same-model serial references and all three
LightOn serial texts match fresh vLLM exactly, including markup. The 4B controls
drift substantially. The candidate's `default` policy enabled the experimental
cache in these archived builds; it does not describe the production runtime.
Each report records the build and observed cache activation for every round.

An eight-page instrumented run (`vision_stage_profile.json`) shows 0.8B patch
embedding plus position work averaging about 15 ms uncached and 10 ms cached,
while the transformer blocks take about 120 ms. These are wall-stage timings;
per-operation enqueue timers inside a frame are not GPU kernel timings. The
profiled runs overlap CPU compilation and are separate from the primary paired
reader measurements. They support avoiding disproportionate work on this small
fraction of total reader cost.

## Where the remaining 0.8B gap occurs

A benchmark-only Objective-C observer captured completed Metal command-buffer
GPU start/end timestamps, dispatch names and counts, and blit sizes in both
runtimes. It does not alter kernels, buffer arguments or dispatch geometry.
The v2 observer preserves Objective-C retained-return ownership, copies its
marker path, and explicitly loads in vLLM workers. Raw traces and the exact
monotonic measurement windows are under `work_profiles/*_default_v2`.

Eight mixed pages, C8, exact matching fixture outputs:

| Instrumented runtime | Client window | GPU interval union | Remainder |
|---|---:|---:|---:|
| Native 0.8B, checked_fixed | 5.100 s | 2.752 s | 2.347 s |
| vLLM Metal 0.8B | 4.344 s | 2.811 s | 1.533 s |

GPU work is already similar in this sample. The approximately 0.8 s difference
outside GPU execution is the stronger lead for this reader gap. The remainder
includes CPU submission, synchronization, queue scheduling and HTTP/client work;
it does not prove that all of it is CPU encoding. Both observers add overhead,
so these are diagnostic timings, not primary throughput claims. GPU busy time
uses the union of intervals, not the sum of overlapping buffers. Neither trace
contains duplicate GPU intervals. Native has 110094 dispatches; vLLM has 262533,
so dispatch count alone does not explain the speed difference.

Local installed vLLM Metal defaults enable lazy GDN kernels and decode
pipelining; compiled MLP is disabled. The decode pipeline gate rejects requests
carrying multimodal decode state. A separate conditioned gate diagnostic counts
618 such rejections and only two eligible decisions, plus fourteen new-request
rejections. Thus one-token-ahead speculation is not established as vLLM's OCR
advantage on this workload. That diagnostic used an oversized reference cache
and slowed severely; its throughput is excluded from speed comparisons.

The reference cache probe reported paging, then allocated 611 grouped blocks,
roughly 4.09 GB and 178761 token capacity for a C8, max-2048 context server.
Only 6.1% of that cache was used. Its paired off run was explicitly aborted.
`benchmark_vllm_metal.py --kv-cache-blocks` now supports a workload-sized capacity
without altering the default reference behavior or native memory admission.
Fresh bounded-cache comparisons must report capacity and absence of preemption.
The native 4B guard also denied one later run under shared-machine pressure;
that failed report is retained and is not scored as throughput.

A subsequent matched eight-page 4B trace uses the same observer, bounded
reference cache and exact output checks:

| Instrumented runtime | Client window | GPU interval union | Remainder |
|---|---:|---:|---:|
| Native 4B, checked_fixed | 14.453 s | 11.464 s | 2.989 s |
| vLLM Metal 4B, 128 cache blocks | 11.624 s | 10.697 s | 0.927 s |

Here 2.062 s of the 2.829 s elapsed difference (about 73%) is outside recorded
GPU execution. GPU work itself is approximately 7% slower in native. This
supports prioritizing execution overhead while retaining a separate 4B kernel
optimization target. The vLLM cache uses 2.06 GiB / 128 grouped blocks, with
reported 18.29x max-2048 capacity. Its startup probe reports compression but zero
additional swap-out. Instrumentation remains diagnostic, with raw frames,
interval-union recomputation and unique-interval checks preserved.

## Rejected four-layer decoder streaming

The opt-in `streamed` build submitted a decoder chunk after each three-recurrent,
one-full-attention group, retaining at most one submitted and one active frame.
It preserved same-queue dependencies, drained before full-attention binding
reuse, and tested cancellation and failed frame acquisition. CPU tests passed
150/164 with fourteen skips; GPU tests passed 163/164 with one skip. A separate
default-path HTTP check passed. The feature counters prove the candidate ran.

The first C8/R8 default / stream / stream / default comparison produced
1.6069 / 1.4395 / 1.4225 / 1.5633 pages/s, with exact serial and vLLM output
identity. More submissions increased cost: the candidate is excluded from
production, with its source patch, fingerprint and benchmark archived.
Overlap is not beneficial unless the cost of splitting submissions is recovered.

## Rejected unified decoder and output frame

`unified` extends a caller-owned frame from embedding/decoder work through the
unchanged vocabulary projection and suppressed argmax, reading token IDs once.
It preserves exact outputs, passes 148 CPU tests with fourteen skips, 161 GPU
tests with one skip, and the separate default HTTP check. Failure cancels the
owned frame and never replays KV/recurrent updates. A selector fallback reuses
the already-produced logits rather than forwarding again.

The C8/R8 default / unified / unified / default comparison gives
1.7106 / 1.7093 / 1.7636 / 1.7334 pages/s. Enabled counters show 205/211 unified
steps; controls show zero. This does not establish a substantial repeatable gain.
The candidate is excluded from production, with its source and evidence archived.
Its framed-GPU counters include vocabulary work that the control's framed
counters exclude; comparing those partial counters as total GPU time is invalid.

## CPU submission profile

`native_cpu_profile` samples only the benchmark's verified descendant process,
using macOS `sample` during a C8/R64 timed default round. This is diagnostic,
with sampling overhead and a stripped executable, not a primary speed result.
Metal C entry points and system-library symbols remain available. Repeated
`getenv`/`__findenv_locked` calls appear inside hot LightOn projection selection
and planned buffer-access preparation. There are 55 leaf samples in environment
lookup, but mixed sleeping worker threads make aggregate sample percentages
unsuitable as a wall-time estimate.

The next experiment caches twenty-two projection switches plus planned-access
profiling and barrier switches for the lifetime of an active LightOn frame.
It refreshes at each frame begin and uses original dynamic reads outside a frame.
Legacy nonempty/nonzero switches retain their exact parsing; concurrent dispatch
still forces dependency barriers. A standalone conformance check covers refresh,
in-frame stability, inactive fallback, falsy values and concurrent barrier safety.

Frame-policy caching remains opt-in:
`TERMITE_METAL_ENABLE_LIGHTONOCR3_FRAME_POLICY=1`. It adds no retained tensor
storage and preserves computation. The first C8/R16 bracket measures
2.2529 / 2.3048 / 2.3156 / 2.2408 pages/s. Its harness rejected the result
because it summed lifetime counters; the raw values remain unchanged and that
failed status is retained. The corrected parser reports increments from the
previous footer (including untimed references), preserving raw lifetime totals.
Both original and corrected benchmark sources are archived. New reports record
the actual harness source hash independently of the build manifest.

The corrected C8/R16 controls give:

| Model | Off / on / on / off pages/s | Process CPU seconds |
|---|---:|---:|
| 0.8B | 1.9836 / 2.1908 / 2.2519 / 2.2364 | 6.580 / 6.387 / 7.132 / 5.986 |
| 4B | 0.6383 / 0.6503 / 0.6421 / 0.6157 | 16.288 / 15.129 / 12.650 / 17.474 |

All outputs match native serial and vLLM exactly. Enabled rounds observe
483/481 snapshots for 0.8B and 688/692 for 4B; off rounds observe zero increments.
The 0.8B first control drifts, and its CPU reduction does not repeat. The 4B
CPU reduction is useful, but throughput improvement is small. This does not
establish a substantial universal gain and does not change the production default.
The build passes 148 CPU tests with fourteen skips, 161 GPU tests with one skip,
and a separate default HTTP checkpoint test, plus the policy conformance check.

## Checked Florence device selection

The native cached Florence batch decoder tries unmasked device argmax before
copying full logits for no-repeat masking. Each winner is checked against that
page's existing history. When every winner is legal, those winners also maximize
the masked logits. Otherwise the unchanged host masked-selection path runs.
`ANTFLY_FLORENCE2_DISABLE_NO_REPEAT_ARGMAX=1` restores the original path.
Counters distinguish accepted device steps from CPU fallback steps.

Tests cover reordered page histories, ties, invalid shapes, banned winners,
nonfinite logits, small and multipart GPU reductions, and absence of full-logit
host downloads on accepted steps. They also exposed a small-vocabulary GPU
argmax seed bug: a leading NaN poisoned comparison. Initializing the maximum to
negative infinity and scanning from zero matches existing strict host comparison,
including ties, NaN/infinity and all-nonfinite default-zero behavior. The failed
`checked` candidate remains archived; `checked_fixed` passes 148 CPU tests with
fourteen skips, 161 GPU tests with one skip, and the separate default HTTP check.

An initial C8/R16 disabled / enabled / enabled / disabled comparison gives
3.5716 / 3.9222 / 4.0437 / 3.9616 pages/s. Enabled rounds accept 98 device steps
with zero CPU fallbacks; disabled rounds use 98 CPU steps. Outputs match native
serial and normalized MLX content. The controls drift, so this is not robust
evidence of a 10–13% throughput gain. Fresh MLX serial is 4.0584 pages/s, using a
different precision and decoding policy. Subsequent comparisons condition each
process with a complete same-policy wave before the scored round.

## Larger remaining opportunities

The native LightOn reader admits a sixteen-page window but clamps physical GPU
decode cohorts to eight rows. Those are different limits: a wider window can
hide refill delays without increasing weight reuse in a single dispatch.
The new bounded-cache vLLM 0.8B reference measures 2.8163 pages/s at C8 and
3.1071 at C16, R32, with 128 blocks / 0.80 GiB and reported 18.29x capacity at
2048 tokens. Its startup commit probe still reports paging; the final cache is
bounded, not a claim of a clean machine. This reference overlaps CPU compilation.

Native's C16/R32/window16/rows8 follow-up measures 2.1685 and 2.1454 pages/s,
with exact outputs and observed sixteen registered pages but only eight physical
rows. It does not recover vLLM's C16 throughput. These runs are separated in time
from the reference and shared load varies; they are not a clean-machine scaling
curve. Merely widening the admission window is not the missing solution.

The next major engine change to investigate is a prepared Qwen3.5 decode plan
with persistent tensor views, buffer slots, selected pipelines and scratch,
updating only page row tables, token IDs and positions per step. That directly
targets work currently repeated through the eager model/operation wrappers.
Expanding physical cohorts to sixteen requires qualifying recurrent row tables,
projections, scratch, cancellation and admission together; increasing only the
request window or removing the published row cap does not implement that path.
A dedicated executor with event-driven request wakeups is another target:
request threads currently poll/compete around a serialized provider, and the
C16 run spends 17.5–17.9 process CPU seconds in 14.8–14.9 wall seconds. Those
CPU totals include useful encoding work and do not attribute all excess to polls.
Neither planned replay, a new executor architecture nor sixteen-row native
kernels is implemented by this pass.

## Method and comparison limits

Reader timing includes queueing, image preparation and inference, excluding
loading and serial reference generation. Benchmark GPU workloads run sequentially;
unrelated shared-machine activity is uncontrolled. Fresh vLLM Metal references
use the production BF16 path, loopback HTTP, distinct media UUIDs, and disabled
prefix/processor caches. This is a production reference, not an equal-precision
kernel comparison. The three synthetic fixtures check consistency, not broad
OCR accuracy equivalence.

The conditioned Florence follow-up uses two full same-policy C8/R16 rounds per
process, preserving the first as conditioning and scoring the second. The scored
disabled / enabled / enabled / disabled values are 4.3264 / 4.7214 / 4.8143 /
4.7177 pages/s. Every output remains identical; enabled processes accept 196
device steps across both rounds with zero CPU fallback. The final control nearly
matches the first enabled result, so the drift still prevents a stable 10% gain
claim. This path reduces full-logit readback; its throughput benefit remains
workload- and load-dependent.

Final source adds a Metal-disabled guard to the new host-argmax test. The final
Metal build repeats 148 CPU passes with fourteen skips; the Metal-disabled
Florence suite passes eighteen with one skip. The initial Metal-disabled
host-only filter matched no tests because that module is excluded; its failed
filter log is preserved. `final_guard_build_manifest.json` binds the final source
and binary; all timed reports retain their original tested build identities.
