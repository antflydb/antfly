# OCR throughput and kernel qualification on M4 Max

This continues `../kernel_saturation_m4_max`. Measurements use a shared, busy
36 GiB M4 Max; our GPU benchmark processes run sequentially. Native builds use
stripped ReleaseFast. Source and executable hashes bind every build manifest.
**Full-reader LightOnOCR parity with vLLM Metal is not yet demonstrated.**

## Reader changes and defaults

Continuous refill keeps the shared backend/coordinator alive while completed
pages release slots for new arrivals. Each page retains independent paged KV,
recurrent history, resource admission, cancellation and deadline. Occupancy is
bounded at sixteen pages; an epoch accepts at most 64 joins before yielding the
model lane. The backend is created lazily after admission. Transient live-memory
pressure retries for at most 30 seconds without a per-page resource lease; hard limits
remain errors. Epoch-owned workspace reservations stay charged. Full prepared prompts must fit their admitted prompt envelope.

Qualified M4 devices default to refill and eight decode rows. Other devices
retain the earlier defaults. `ANTFLY_LIGHTONOCR3_CONTINUOUS=0` restores fixed
waves. `ANTFLY_LIGHTONOCR3_BATCH_ROWS` and `ANTFLY_LIGHTONOCR3_BATCH_WINDOW` control
cohort size and occupancy. Weight residency remains an explicit option:
`TERMITE_METAL_ENABLE_LIGHTONOCR3_WEIGHT_RESIDENCY=1` wires existing admitted
buffers, capped at 12 GiB, without additional weight copies.

Equal-geometry vision batching (at most four pages) and equal-length prepared
prefill batching are implemented and tested, but remain **off by default**:
`ANTFLY_LIGHTONOCR3_VISION_BATCH=1` and
`ANTFLY_LIGHTONOCR3_PREFILL_BATCH=1`. Independent attention lanes, complete
axis-major M-RoPE positions and per-page recurrent state are preserved. Query
padding is not supported. Collection waits are capped at 100,000 microseconds.
Short waits often produced singletons; longer waits did not establish a stable
throughput improvement. A C16/window16 run and direct-KV prefill also lost to
C8 refill. More concurrency is not assumed to be faster.

## Shared serialized vision admission

The default refill path owns one maximum backend vision workspace reservation
for its entire epoch. Page-local KV, recurrent histories, projected embeddings
and host working memory remain additive. A larger image must acquire the
increment before running. Each exact increment lease is held until backend
teardown, including after the contributing page retires. Failed growth leaves
accounting unchanged. The fixed 64-lease bound matches the epoch's admission
bound. Batched vision retains conservative per-page peaks because it can use
several vision workspaces together.

`ANTFLY_LIGHTONOCR3_DISABLE_SHARED_VISION_PEAK=1` restores the earlier per-page
peak policy; the runner exposes `--disable-shared-vision-peak` for same-binary
comparisons. Unit coverage uses the real admission controller to fill a budget,
reject growth at capacity, permit growth after page retirement, retain the
workspace until teardown, and check hard caps and the bounded lease count.

## Rejected immutable BF16 binding cache

The opt-in prototype retained eligible immutable checkpoint aliases across
requests, bounded to 512 bindings / 12 GiB, with source pins until model unload
and admission-charged bias buffers. It passed ownership, source rejection,
capacity denial, descriptor restoration and real-checkpoint tests. The 0.8B
reader reused four bindings (16,842,752 bytes). The 4B run emitted no eligible
binding-cache entries, so it does not demonstrate model-cache reuse there.
Off/on/off controls below established no throughput advantage. The prototype
was removed from production, including its environment switch and runner flag.

`rejected_weight_binding_cache.patch.gz` preserves the complete runtime and harness
experiment relative to the shipping code; decompress and apply it to reproduce the experimental
source identities and `--weight-binding-cache` runs. Its `binding_final_*`
manifests and logs describe that experimental binary, not the shipping runtime.
`shipping_runtime_validation.json` verifies the restored nine source hashes and
runtime diff against the GPU-tested shared-peak build.

## Reader measurements

The final binding-cache experiment (`binding_final_build_manifest.json`) uses
an off/on/off bracket. All native columns use the same experimental executable,
continuous refill, weight residency and C8. The cache was not promoted; shipping
runtime hashes match `shared_peak_build_manifest.json` exactly. Nearby references
are independent vLLM Metal runs. This shared-machine snapshot does not establish
stable end-to-end parity.

| Model | Native cache off / on / off, pages/s | vLLM Metal pages/s |
|---|---:|---:|
| LightOnOCR-3 0.8B | 1.3626 / 1.1399 / 1.1416 | 1.6193 |
| LightOnOCR-3 4B | 0.3096 / 0.2861 / 0.2836 | 0.4578 |

The preceding shared-vision-peak build (`shared_peak_build_manifest.json`)
measured 0.8B 1.7581/1.8266 pages/s and 4B 0.3414/0.3008 pages/s, versus nearby
vLLM 2.9259/0.8312. Florence measured 4.1025 versus MLX-VLM serial 4.9200.
On 64-page sustained runs, native 0.8B/4B measured 1.9787/0.3651 versus
vLLM 3.1748/0.8306. These longer runs improve amortization but retain a gap.
Earlier tail-qualified snapshots measured 1.8134/0.5242/4.4635 for
0.8B/4B/Florence versus 2.1124/0.6107/4.8911, illustrating substantial drift.
No historical best is substituted for a same-build control.

Sharing the serialized vision peak avoids duplicated reservations and preserved
hard caps in the controller tests. Same-build shared-peak enabled/disabled
trials measured 0.8B 1.7581/1.8327 and 4B 0.3414/0.2809; this is primarily an
admission-capacity fix rather than a demonstrated universal speedup. C16 was
slower than C8 for 0.8B and did not close the 4B gap. Combined vision/prefill
batching measured 1.8479 versus 1.7581/1.8266 controls and remains optional.
Instrumented profiles are reachability evidence, not performance controls.

Earlier same-build 0.8B refill/wave comparisons measured 1.5557/1.2702,
1.0869/0.8807 and 1.0324/0.9824 pages/s. The early 4B comparison was
0.4830/0.3103 pages/s, **across different hardened/prototype builds**. An
old/new/old 0.8B bracket rose 1.3634/1.5348/1.6137, showing machine-load drift;
it does not establish a causal gain from those kernel changes.

Primary throughput controls use sixteen repeated pages, C8/rows8/window8 and
greedy output with a 256-token ceiling. Named `_64`, C16 and instrumented cases
record their different settings in their reports. All use three synthetic PNG
fixtures and explicit
24/28/16/4/4 GiB total/shared/backend/KV/scratch budgets. Loading and serial
reference generation are outside timing. Each timed output must match its own
serial output exactly. `output_validation.json` additionally checks exact text
against the earlier native baseline and markup-stripped cross-runtime content.
This is consistency coverage, not a real-document OCR quality evaluation.

Native timing includes preprocessing, queueing, inference and decoding, but
excludes HTTP/PDF rendering/indexing. vLLM timing includes loopback HTTP, with
prefix and processor caches disabled. Native LightOn retains F32 activations,
accumulation/output and recurrent state with original BF16 weights; MLX uses
BF16 activation/output storage. Florence native uses Q4_K weights, F16 encoder
mirrors and no-repeat n-gram size three, versus original FP16 and the reference
greedy policy. Comparisons are not fully precision matched. The pinned vLLM
Metal has no Florence adapter; MLX-VLM's batch-eight attempt failed attention
mask broadcasting and is retained separately without a throughput claim.

## Qualified kernels and remaining gaps

Four-way split-K fused gate/up plus SiLU-times-up uses the current frame's
bounded attention scratch: 0.8B rows two through eight and 4B rows two through
seven. Small tails require available split resources; larger cohorts retain
ordinary fusion as a fallback. Missing PSOs, insufficient physical or logical
capacity, aliases, precision/master switches and unsupported shapes preserve
working paths. No additional persistent scratch is allocated. Planned encoders
transition only after eligibility/resource checks, preserving prior writes and
the caller's scope lifetime. The architecture gate includes rows two/three.

Partial 0.8B QKV rows five through seven use eight-way splitting. Down projection
rows five through seven use sixteen splits, falling back to eight if scratch
is smaller. TensorOps supports qualified larger prefill rows up to 16,384.
Existing SIMD recurrent decode, four-column recurrent prefill, cached F32 norm
and Florence encoder optimizations remain active. The additional `splittails`
experiment evaluates all 36 small-cohort projection cases. Nineteen shapes beat
the prior path under both cache policies; the production table selects those
shapes only. Two-row 0.8B QKV tied and retains its prior route. The nineteen
include three-row 0.8B QKV/FFN, partial 0.8B FFN, two-/three-/five-/six-/seven-row
4B QKV/FFN, and three-/five-/six-/seven-row 4B down projections.
`TERMITE_METAL_DISABLE_LIGHTONOCR3_TAIL_SPLIT=1` restores their previous routes.

Balanced candidate/production/MLX trials rotate/reverse all six participant
orders and check complete outputs plus independent CPU samples. Representative
candidate/previous-production median latency ratios:

| Change | Resident cache | 64 MiB cache flush |
|---|---:|---:|
| 0.8B gate/up, rows five/six | 0.448 / 0.353 | 0.486 / 0.383 |
| 0.8B gate/up, rows two/three/four | 0.722 / 0.530 / 0.676 | 0.860 / 0.642 / 0.749 |
| 4B gate/up, rows two/three | 0.767 / 0.695 | 0.748 / 0.699 |
| 4B gate/up, rows five/six/seven | 0.730 / 0.755 / 0.801 | 0.773 / 0.747 / 0.764 |
| Three-row 0.8B QKV | 0.750 | 0.857 |
| Two-/three-row 4B QKV | 0.772 / 0.677 | 0.853 / 0.787 |
| Partial QKV, rows five/six/seven | 0.707 / 0.699 / 0.712 | 0.718 / 0.724 / 0.726 |
| Partial down, rows five/six/seven | 0.889 / 0.885 / 0.894 | 0.921 / 0.954 / 0.944 |

The final production sweeps (`tail_qualified_all_rows_*.json`) cover **86 cases**,
covering six selected decoder projection dimensions, vocab heads, normalization,
recurrent kernels and fused gate/up, including every row count one through eight: **75/86 within 20% of MLX or faster
by median latency under both cache policies**. Mean latency reaches 74/86
resident and 73/86 with a cache flush; all samples and outliers are retained
in the raw reports (`qualified_sweep_summary.json`). Native median latency is
at least as fast as MLX in 40/86 resident cases and 34/86 cache-flushed cases. The preceding planned-encoder sweeps measured
65/86 resident and 70/86 with a 64 MiB flush. Cache policy and shared-machine load
affect these small cases. Earlier 43/45 and 25/27 narrow sweeps omitted
intermediate cohorts and must not substitute for the broader coverage. Small
QKV cohorts, some down/FFN projections and partial 4B cohorts retain gaps.
Standalone FFN projection cases may be replaced by fused FFN in real execution;
primitive comparisons do not establish full-reader parity. They also do not
cover every reader projection. `shared_peak_actual_dispatches.json` indexes
actual reader dispatch shapes, including decoder prefill rows 432/554/662 and
vision rows 1672/2160/2592. Vision projections and additional small decoder
projections require separate qualification beyond the 86-case sweep.

Packed streaming warps, vec2/vec3, N32/N64 fusion, finer K8 gate splitting and
other losing candidates remain benchmark-only. The smaller existing TensorOps
tile lost all thirteen tested tail cases under both cache policies. Decode waits
of 0/100/1000 microseconds measured 1.2838/1.3553/1.3973 pages/s in one frozen
intermediate build; the existing 1000-microsecond setting remains. The SDK rejected an N8 tile.
Failed first attempts and corrected reruns are retained with separate names.

## Remaining throughput work

The CPU samples (`native_*_shared_peak_cpu_*`) are separate instrumented runs,
excluded from throughput controls. They show buffer preparation, residency
registration and Metal waits across active reader stacks. Counts include waiting
threads and cannot attribute a timed-reader percentage or prove a CPU math
fallback. The binding-cache experiment tests one narrow part of that setup.

Full-reader parity still needs work on the additional vision/prefill projection
shapes, model-scoped preparation of packed projections, and scheduling mixed
prefill/decode work without serial stalls. Longer collection waits, C16 and the
narrow binding cache have no established advantage in these controls. BF16
activation storage could reduce bandwidth but changes the current precision
policy and requires separate output/quality qualification. Real-document,
mixed-length workloads remain necessary beyond these three fixtures.

## Validation and reproduction

The shipping shared-peak build passed **78 focused GPU tests, zero skips**,
plus a separate default real-checkpoint test. The rejected binding-cache build
passed 79 focused GPU tests and two separate real-checkpoint tests, zero skips.
Its cache-enabled checkpoint test verifies reuse across request wrappers.
The planned-encoder build passed **81 focused GPU tests, zero skips**, and
its actual reader profile recorded 3,432 split FFN dispatches at rows two/three/four.
The earlier full-cohort build passed 77 tests, zero skips. All 71 successful
native reports match the original native text exactly, their timed own-serial
outputs exactly, and declared cross-runtime content after markup removal.
Seven failed native reports remain diagnostic evidence. Coverage includes real-checkpoint/HTTP output, cancellation/admission,
reordered distinct-image vision batches, multi-token recurrent histories,
complete M-RoPE, wrapper dispatch, offsets/canaries, scratch interleaving and
overlapping frames. Final C conformance additionally produces input inside a
planned encoder before both split families consume it, and verifies a valid
outer scope close. `benchmark_concurrency.py --require-small-tail-fusion`
rejects reader runs without actual two-/three-/four-row split FFN dispatches;
these traced runs include warmup and are not throughput controls.

See `docs/guides/lightonocr3.mdx` for the runnable reader command. Reproduce a
complete primitive sweep with `benchmark_metal_pair.py --suite all-rows`, the
benchmark bridge and timing observer; use `--cache-flush-mib 64` for the second
cache policy. Candidate comparisons require `--compare-production` and thirty
balanced trials. The bridge rejects stale production/bridge source hashes. Exact runner and
bridge bytes for the final sweeps are archived in `qualified_benchmark_sources/`;
restore their original paths before reproducing those source hashes.

`*_build_manifest.json` holds source/diff/binary identities. Raw reports retain
checkpoint, input, software, hardware and executable fingerprints. Original and
stored log hashes are recorded in `log_normalization.json` (LF, stripped trailing
whitespace, one terminal newline for nonempty logs). Large traces are stored as
`.txt.gz` companions to the original names in reports; decompress them before
using those uncompressed paths. `compressed_logs.json` records compressed and
uncompressed hashes. Failed admission, early vision ownership,
outdated query-length fixture, source-changed build, missing observer symbols,
unsupported tile and failed reference adapter attempts remain diagnostics.
