# OCR execution engine experiments on M4 Max

Work in progress following `3e21b5a5636ef8a45328a2b1e805108277c7b304`.
These measurements do not yet establish sustained parity with vLLM.

## Current comparison status

| Model | Last repeated matched comparison (native / reference pages/s) | Scope |
| --- | --- | --- |
| LightOn 0.8B | 4.0209 / 3.3746; reverse 3.9235 / 3.2491 | Engine64 paired35: actual vLLM API, 19.2% and 20.8% leads. |
| LightOn 4B | 0.8839 / 0.83361; reverse 0.8882 / 0.88848 | Engine69 paired52: forward +6.0%, reverse effectively tied (−0.03%). No repeated win. |
| Florence-2 | 4.8168 / 4.6613; reverse 4.8171 / 4.6885 | Engine64 paired38: small MLX-VLM proxy lead, different weights/decoding, normalized content comparison. |

Engine70 extends the existing per-frame settings cache for recurrent,
normalization and gate/up dispatch. Its 245 Metal and 191 CPU tests pass
(one and 22 skips respectively). All Metal source-producing functions are
byte-identical to engine69; the change targets host encoding. All nineteen C suites also pass after final binary identity verification.
The default-memory throughput comparison is running.

Engine69 corrects a 64-row trace-mask boundary and exposes observed prompt-stream
counters in policy reports. Its Debug boundary regression passes, as do 245
Metal tests (one skip) and 191 CPU tests (22 skips). Its nineteen C suites are
reused only after exact input/binary hash verification. Fresh R128 screens
selected synchronous VF3 vision and nonstreamed prefill. Both paired52 native
runs preserve every output and complete without admission pressure; they do
**not** establish a repeated speed lead. Forward vLLM cache capacity varied
around 26–27 nominal full contexts, while the closing C64 server reported 389
shared cache blocks and capacity for 77.80 full 1,024-token contexts. Capacity
must be read separately for each leg. A stronger default-memory concurrency
search remains required.

The smaller-model rates above bind their historical engine64 source; they are
not measurements of the current candidate. Engine68's primary comparison was
held before any leg ran because of the trace-mask defect.
All primary pages use exact same-model serial text; the three synthetic fixtures
provide consistency coverage rather than a broad OCR accuracy evaluation.

The candidates preserve BF16 checkpoint weights, F32 decoder activation,
accumulation and recurrent state, and F16 paged KV. Correctness comparisons use
all three synthetic invoice/receipt/shipping fixtures, exact same-model serial
text, and exact saved vLLM text. This is consistency coverage, not a broad OCR
quality evaluation.

## Candidates and qualification

- Event-based cooperative waits retain stack-backed pending work until it is
  cancelled while idle or its result has been published. Queue changes and
  result publication notify under the coordinator lock. Ready predicates are
  checked again after resetting a waiter's event.
- The bounded host metadata workspace supports up to sixteen decode pages using
  the existing mixed-batch context builder. Prefill and other model families
  retain their original allocation path.
- Physical batches of up to sixteen pages have independent convolution and
  recurrent states. The original eight-row ceiling remains the default.
- Prepared recurrent blocks retain immutable weight identities, linear slots,
  adjusted normalization weights and fifteen activation buffers in the shared
  backend epoch. They use the canonical Metal primitives. Each read page
  reserves an additional 8 MiB backend and 1 MiB host scratch before enabling
  the path. Unsupported shapes decline before encoding; errors after encoding
  fail the forward rather than replaying a partially advanced state.

`engine0` and `engine1` each pass 149 selected CPU tests with fourteen skips,
then 162 GPU tests with one skip. The true feature-disabled default HTTP reader
also passes separately. Lossless qualification logs live under `qualification`.
Build manifests bind binaries, runtime source hashes, base commit and source
patches. Intermediate performance reports retain their own tested identities.

The first engine1 wide conformance wrapper exited before GPU execution because
its test helper still expected the old eight-row gate/up eligibility ceiling.
The corrected wrapper explicitly expects sixteen rows; it passes projection,
fused gate/up, offset/canary, independent-state continuation, and fallback checks
at 9, 12 and 16 pages for both value-head geometries. Its exact copied test source
and wrapper are archived with the drivers.

## Measurement limits

GPU workloads are run sequentially. This is a shared machine; other builds and
processes remain active. Some diagnostic reader measurements also overlap our
own CPU compilation. Those runs must not be described as clean or used alone
to claim a final throughput win.

The load sensitivity is large: the earlier bounded vLLM reference measured
2.8163 pages/s at concurrency 8 and 3.1071 at concurrency 16; the first reference
in this directory measured 0.7285 and 0.8634 with the same three fixtures,
32 requests, maximum 256 tokens and 128 KV cache blocks. Native controls also
drift substantially. All comparisons require current, matched workload controls.

The initial engine1 prepared path bypasses the old Zig packed-core trace. Its
first reports therefore say `missing_packed_core` despite successful exact text
checks and nonzero prepared-block counters. These reports are retained as
rejected qualification, not silently relabeled. Later engine1tuned diagnostics
explicitly omit that trace requirement for prepared variants while retaining
recurrent-kernel and output requirements. The next build emits prepared row
telemetry and fixes the sixteen-row recurrent trace bitmask.

`default` benchmark policy resets several optional throughput switches. The
`engine1tuned` diagnostics use `current` policy with frame-policy snapshots,
weight descriptors, recurrent encoder coalescing and ready-cohort scheduling
enabled. Two same-policy rounds condition each process. The later alternating
engine policies are intended to isolate prepared blocks and the dedicated worker
within one resident checkpoint while preserving controls around each candidate.

## Later engine qualification and current limits

`engine6b` fixes two activation-pool ownership defects uncovered by the reader:
retained handles to one physical allocation must not create duplicate cache
entries, and runtime-owned hidden/video buffers must release retained handles
without returning their still-owned storage to the temporary allocation pool.
The new `deviceRetainedExternal` ownership mode preserves ordinary allocation
recycling. The full Metal conformance checker passes; the selected GPU run has
172 passes and one skip, including hidden-buffer isolation and video lifetime
coverage. Its 0.8B concurrency-8/32-page combined candidate measures 2.664–2.674
pages/s against controls at 2.593–2.626. This is a small improvement, not parity.
The 4B controls drift substantially and do not establish a candidate win.

`engine7` adds opt-in parking for the remaining page phase locks, an atomic
publication of the already-admitted shared vision peak, and bounded admission
retry backoff. It preserves the original resource caps and admission timeout.
It passes the same 172 selected GPU tests with one skip. Its alternating reader
measurements show lower CPU use in some rounds but no reliable throughput gain.

Fresh vLLM references with the same three fixtures, concurrency 8, 32 pages,
256 output-token ceiling and 128 KV blocks measure **3.1309 pages/s for 0.8B**
and **0.8710 for 4B** (`primary_reference1_32`). Our own compilation is stopped
through these references and primary native measurements. Other machine work
continues. Native uses F32 activations/state while the MLX reference uses BF16
activations; these are the actual implementations, not equal-activation-dtype
kernel comparisons.

`engine8b` adds two separately gated candidates:

- `ANTFLY_LIGHTONOCR3_EXECUTOR_TAIL=1` keeps a non-streaming, unconstrained greedy
  page on the dedicated executor until EOS, length limit, failure or cancellation.
  Only epoch-owned pipeline objects execute GPU work. Stack-backed page state
  stays borrowed until terminal publication or idle cancellation. Intermediate
  token continuation requeues as its final action, so cancellation can safely
  remove the borrow immediately afterward.
- `ANTFLY_LIGHTONOCR3_COMPACT_KV=1` physically packs only the attention layers into
  the continuous reader's paged KV pool: six layers for 0.8B and eight for 4B.
  Their separate recurrent histories remain present. Admission uses the same
  physical layout, reducing both actual KV storage and its reservation to one
  quarter. Context length, page geometry, state precision, scratch reservations
  and resource caps are unchanged. The flag modifies only the reader's runtime
  config copy and is not read from model artifacts.

The selected engine8b GPU run passes 175 tests with one skip. New tests cover
independent page EOS/length limits, continuation KV geometry, claimed-versus-idle
cancellation, and physical storage/admission agreement for both official configs.
The real checkpoint HTTP reader completes two pages exactly with the executor
tail active and a six-layer KV pool. The separate feature-disabled HTTP test
passes with all 24 KV layers and zero tail steps.

The first tail measurements **regress**, rather than demonstrate success: the
0.8B workload takes 715–816 decode steps versus 502 in its controls, despite the
same completed pages and tokens. This is consistent with request vision and
cleanup phases losing the unfair execution gate to immediately requeued decode.
A phase-priority candidate is being qualified; the tail remains disabled by
default. These intermediate candidates must not be described as faster than
vLLM or substituted for final production measurements.

`engine9` gives page phases priority over the decode worker when they are
waiting for the shared execution lock. This avoids immediately requeued decode
starving vision, prefill and cleanup. With compact KV and eight physical rows,
0.8B measures 2.64–2.66 pages/s over alternating control/tail rounds. A sixteen-row
attempt fails admission for two of 32 4B pages and is excluded from throughput
claims; memory admission remains enforced.

`engine10` adds a qualified projection adapter for nine through sixteen rows.
It splits eligible dense projections into the existing eight-row kernel and an
exact tail within the caller's command buffer. It preserves F32 activations and
BF16 weights. An ABBA microbenchmark checks all outputs and records 2–6x
improvements for many projection geometries. Tiny recurrent gates regress and
are deliberately excluded. The integrated checker covers 39 shape/row cases,
canaries, CPU samples, rollback flags and a GPU-produced input in the same
planned encoder. The full Metal conformance checker also passes.

The optional `ANTFLY_LIGHTONOCR3_EPOCH_AUX=1` assigns the existing conservative
34 MiB host / 16 MiB backend envelopes for the shared worker, prepared plan and
activation pool to their shared context. Its lease is acquired before those
resources are created and released after the worker joins and backend dies.
Every page retains its own decoder, KV, recurrent state and image reservation.
An admission waiter can retain the shared context after the last page retires;
the auxiliary reservation remains owned through that interval.

The selected engine10 suite passes 176 tests with one skip. Its real checkpoint
HTTP check completes two pages with one auxiliary acquisition and release after
backend teardown. All primary GPU workloads are sequential, with our own CPU
compilation stopped. Fresh concurrency-16, 32-page vLLM references measure
**3.1819 pages/s (0.8B)** and **0.8943 (4B)**. The corresponding native engine10
rounds measure **2.7431 / 2.6897 (0.8B)** and **0.7071 (4B control)**. These
results still fall short of vLLM. In particular the 0.8B improvement from the
previous wide control, 1.8327 pages/s, does not establish parity.

Admission diagnostics identify the 4 GiB scratch sublimit as the 4B pressure
source. Raising that sublimit to 8 GiB, within the unchanged 16 GiB backend and
28 GiB shared caps, completes all 32 pages without admission retries but only
reaches **0.7225 / 0.6966 pages/s**. This separates a real capacity constraint
from the remaining execution bottleneck. Exact saved reference text and native
serial text comparisons pass for both rounds. No output caching is enabled.

`engine11` qualifies an optional single-command-buffer decode, vocabulary head
and argmax path. Counters prove one submission and one in-frame argmax per
measured step; the real checkpoint and shared auxiliary admission ownership
regression pass (177 selected tests pass, one skip). Throughput remains flat:
2.6516 pages/s for 0.8B and 0.7114 for 4B. It stays disabled by default. The
existing BF16 projection-input profile is slower at 2.0536 / 0.5099 pages/s,
despite passing text parity. Neither experiment establishes parity with vLLM.

The next matrix-tile experiment identifies a larger gap in the vocabulary head.
At sixteen rows its isolated GPU duration drops from about 9.2 ms to 1.8 ms for
0.8B, and from 29–30 ms to 4.5 ms for 4B, without rounding F32 activations or
changing checkpoint weight precision. ABBA comparisons cover every row count
from two to sixteen, full outputs, independent CPU dot products and output
canaries. Smaller batches that regress are excluded. Wider register-vector
kernels regress across all tested shapes and are rejected.

`engine12` is being qualified with three optional matrix-tile pipelines. It
selects only winning row/shape ranges, preserves the old fallback kernels, and
has a separate `TERMITE_METAL_DISABLE_LIGHTONOCR3_WIDE_MPP` rollback. Its
vocabulary head can run in the existing standalone head command buffer or an
active decode frame. Decoder projection promotions require an active decode
frame. The new conformance check includes a GPU-produced input consumed in the
same planned encoder, missing-pipeline fallback and rollback comparisons. These
isolated kernel gains must not be reported as end-to-end vLLM parity until the
new runtime completes primary throughput measurements.

The complete engine12 Metal checker passes (matrix paths, the previous adapter,
wide gate/up, and the full kernel suite). Its runtime suite passes 177 tests
with one skip. Primary 32-page, concurrency-16 results are **3.0058 / 3.0269
pages/s for 0.8B** and **0.7491 / 0.7017 for 4B** (control / executor tail).
All pages complete and text parity checks pass. New nonzero counters verify
that both matrix projections and vocabulary heads execute in the timed work.
The same total resource caps remain in effect. A fresh vLLM reference taken
immediately afterward reaches **3.4046 / 0.9493 pages/s**, so parity remains
unachieved. The isolated head gain is not an equivalent page-throughput gain.

A separate BF16-prefill-only profile preserves the optimized F32 decoder and
measures 2.8930 / 0.7986 pages/s. It passes the small consistency corpus, but
changes prefill projection-input rounding and remains a distinct opt-in profile.
Matched BF16/F32 matrix-tile probes find no broad dtype speedup for decode on
this hardware. A new 0.8B feed-forward tile fails an all-row confirmation run
and is not promoted.

The diagnostic `engine12_4b_stage_profile_c16_r32_0` enables stage-boundary GPU
timestamps and changes encoder topology to obtain attribution. It is excluded
from primary throughput claims. Its sampled batched decoder frames spend
roughly 20–22 ms in feed-forward work, 15–17 ms in attention and 4–5 ms in the
other bucket, within 44–49 ms total frames. Dispatch-boundary timestamps are
unsupported on this device; no unsupported counter commands are injected.

Engine13 caches immutable recurrent side weights by compute-owned handle. Each
page retains its own mutable recurrent state. The cache is opt-in, capped at
128 entries and 8 MiB of actual Metal allocation, and charged once to the epoch.
At C16/R32 it measured 2.7869 pages/s for 0.8B and 0.7922 pages/s for 4B. Cache
hits and retained bytes are recorded in the reports; these results do not close
the vLLM gap. The separate mixed F32/BF16 streaming-GEMV prototype was rejected:
all tested cases were slower. Its experimental source retains the upstream
Apple MIT attribution and is not used by production shaders.

Engine14 adds one optional prefix/suffix command-buffer split per batched
forward. Existing double staging banks and separate attention scratch banks
allow the prefix to execute while the CPU encodes the suffix. With a four-layer
prefix and the side-weight cache disabled, C16/R32 measured 2.9293 pages/s for
0.8B and 0.7603 pages/s for 4B. Exact output checks passed and telemetry proves
the split executed, but the experiment did not beat vLLM. Both features remain
opt-in. The latest independently refreshed C16/R32 vLLM controls are 3.404642
and 0.949304 pages/s, respectively. Source patches, build manifests, frozen
drivers and qualification logs accompany these reports.

Engine15 qualifies a bounded concurrent full-attention collector and direct
writes into the batched output. Isolated repeated tests show 3.3x / 2.1x lower
attention GPU time at 16 pages for 0.8B / 4B, with identical full kernel outputs
and CPU softmax corner checks. Missing-pipeline and disabled-kernel fallback
checks also pass. The native regression build passes 182 selected tests with
one skip. An activation-guard audit found that mixed batches retain a global
prefill tag while their pages can all be decoding; engine15's extra global-tag
check prevents activation in that production path. Consequently, there is no
engine15 end-to-end throughput claim. Engine16 removes that redundant guard
and adds an explicit production dispatch-counter requirement.

Engine16 executes full attention for all selected pages in two dispatches,
using an explicitly declared GPU address table. It passes 182 native tests with
one skip, plus GPU kernel output, CPU softmax, canary, producer-dependency and
rollback checks. The real reader test confirms 126 production kernel calls at
two pages, including the prefix/suffix overlap mode. C16/R32 F32 measurements
reach 3.1851 pages/s for 0.8B and 0.8079 for 4B. These still trail the earlier
short vLLM controls.

Matching sustained C16/R128 controls measured 2.4622866 / 0.7003823 pages/s for
vLLM. Native 0.8B measured 2.9936 pages/s with F32 activations; native 4B measured
0.6692 with the side-weight cache and BF16 prefill inputs. Every page completed
and matched its serial reference. The 0.8B result is a first sustained win,
not a repeated confidence result or a claim that all workloads beat vLLM. The
4B combination did not win and should not be promoted from short-run evidence.
Both engines used the same three fixtures, 256-token ceiling and disabled
prefix/media caches; native excludes loopback HTTP and vLLM includes it.

An epoch-lifetime audit after this run found that engine16's attention counters
were cumulative on a provider that can outlive its backend epoch, and its
scratch banks could also survive that epoch. These engine16 results remain
experimental. Engine17 adds per-epoch counter baselines, explicit scratch
release after draining work, and scratch enumeration in memory snapshots. GPU
checks confirm release fails while a frame is active and succeeds after
cancellation. A wider opt-in logical window is also under qualification;
physical forwards remain capped at sixteen rows and page admission stays
additive. Neither the lifetime fix nor the wider window is credited with a
throughput improvement before a new measured run.


Engine17 fixes cross-epoch attention counter baselines and releases both batch
scratch banks before their admission lease retires. It qualifies 183 tests with
one skip. Its logical window of 32 retains sixteen physical GPU rows; it does
not improve sustained throughput: 0.8B / 4B C32/R128 measured 1.6824 / 0.4924
pages/s. A sampled CPU run is diagnostic and excluded from primary timings.

**Reference5 4B timing is excluded.** Its explicit 256-block KV override exceeded
the server's probed automatic capacity of 221 blocks. The original report and
server log remain immutable; `capacity_audit.json` binds their hashes and records
this exclusion. The 0.8B override stayed below its probed capacity. The benchmark
now requires an explicit override to remain within every reported automatic
capacity, and fails before reference requests or timing if it cannot verify that.

Engine18 qualifies 188 tests with one skip. It adds cancellation-aware prefill
turn events, an explicit 256-join epoch limit (live page and GPU row limits stay
bounded), and serial in-frame reuse of BF16 prefill packing storage. The packing
change preserves complete kernel outputs bit for bit and uses the existing
allocator. Rollback, producer dependencies, guards and scratch lifetime checks
pass. The default epoch limit remains 64; extended epochs are opt-in.

Its first C16/R128 sustained round measured 3.2879 pages/s for 0.8B with F32
projection inputs, and 0.6216 for 4B with shared side weights and BF16 prefill
projection inputs. All 128 pages matched their serial references, with one epoch
and no admission pressure. The last valid matching vLLM control was 2.4623 /
0.7004 pages/s: the 4B target remains unmet, and these native runs do not establish
repeatable parity. Fresh controls are being collected. The extended epoch reduced
CPU work but a remaining shared lock clamp still limited configured 1ms waits to
100us; the next candidate corrects it.

An alternating prefill matrix aspect-ratio probe tested twelve official projection
shapes at 128/432/864 rows with F32 and BF16 inputs. Its M64N32 candidate preserved
complete outputs but showed no broad performance improvement over M32N64. It is
not promoted.


Reference6 refreshes the matching C16/R128 controls with 128 KV blocks, each
below its probed automatic capacity. It measures 3.1546 / 0.6540 pages/s for
0.8B / 4B. Every output matches the serial reference.

Engine19 corrects the final 100us clamp inside `lockWithPoll`, honors configured
parking in cleanup, and removes the worker's fixed 100us phase loop. Existing
100us callers keep their interval; all requested intervals remain capped at 1ms.
It passes 196 selected tests with one skip, including cancellation and retained
lock ownership. C shader sources are identical to the qualified engine18 build.

Its first C16/R128 sustained results are 3.0614 / 0.6992 pages/s. Process CPU work
falls to 15.0 / 49.1 seconds from engine18's 20.7 / 87.8 seconds. All 128 outputs
remain identical, without admission pressure. The 4B result beats reference6 by
6.9%; 0.8B trails its control by 3.0%. These single shared-machine rounds do not
establish repeatable parity for both models. The next candidate replaces phase
polling with the existing cancellation-aware event/FIFO execution lane.


Engine20's first FIFO-gate experiment regresses 0.8B to 2.3193 pages/s. Its worker
claims smaller decode cohorts (11.48 rows on average); phase priority must be
preserved before claiming work. Its 4B run is excluded: model loading is rejected
by the live-memory guard before inference. The requested 11.35 GB exceeds the
8.63 GB admitted capacity after the existing 6.44 GB reserve. No limit is relaxed.
This experimental build also precedes the correction for cancellation of cleanup's
own Io task, so its report explicitly marks it as unqualified for production.

Engine21 restores phase priority with an event notification at the last waiter's
exit, including cancellation and cleanup. Cleanup uses an uncancellable event
wait until the backend owner releases its state. It passes 199 selected tests
with one skip, including cancellation of an actual Io task while cleanup is
already parked. Full C conformance, batched attention and prefill-pack checks
pass; these functional timings overlap CPU compilation and are excluded from
primary throughput measurements.

An opt-in `ANTFLY_LIGHTONOCR3_THROUGHPUT=1` preset selects the admitted M4 execution
features, sixteen physical rows/window slots and an explicitly bounded extended
epoch. It honors individual false values and leaves precision and memory limits
unchanged. A reader/HTTP fixture using only this preset passes and reaches the
prepared executor, batched attention, event gates and compact KV. Separate default
and polling-rollback reader checks also pass.

The first engine21 C16/R128 primary round measures 3.2274 / 0.6600 pages/s for
0.8B / 4B, with all pages identical and no admission pressure. Reference6 measures
3.1546 / 0.6540. These 2.3% / 0.9% margins are too small to establish a repeatable
win on this shared machine. The wider logical-window experiment and fresh
vLLM controls remain necessary.


Engine21's logical-window32 experiment completes with exact outputs. 0.8B measures
2.7225 pages/s; its average decode batch grows from 12.90 to 13.73 rows but whole
reader throughput falls. The 4B run is excluded from primary timing because it
was sampled diagnostically. The sample captured no thread call graph near the
process's completion, so no causal attribution is made; its hash-bound sidecar
records the instrumentation and exclusion. The preset retains window16.

The subsequent worker-continuation experiment also completes with exact outputs,
but measures only 0.9185 / 0.3436 pages/s in these later shared-machine runs.
GPU and host wait times both grow. This does not establish a causal slowdown from
continuations alone, since a close control is absent; it provides no reason to
enable the option. It remains disabled in the preset. Fresh paired controls use
one automatic-capacity vLLM server per concurrency/model, followed immediately
by the ordinary native executor at C16/R128.


Completed raw text logs are stored as lossless `.txt.gz` files.
`raw_logs_manifest.json` maps each original report filename to its compressed
artifact and records both hashes. Decompression restores the exact original bytes,
including server carriage returns and whitespace; historical report hashes are
unchanged. Qualification/helper archives have their own manifests.


Reference7 and engine21 paired runs complete without our CPU compilation or
diagnostic sampling. At C16, vLLM measures 2.6506 / 0.7719 pages/s and native
measures 2.5357 / 0.7021 for 0.8B / 4B. At C32, vLLM improves to
2.9924 / 0.8496; the following native C16 runs measure 2.6505 / 0.6765.
Every timed output matches the model's serial reference, and the three full
fixture strings also match across runtimes. `paired7_comparison_audit.json` binds
the reports and comparisons. These fresh controls do not establish parity.

Engine22 passes 201 selected tests with one skip. It moves CPU image preparation outside the
shared GPU gate and introduces batching of already prepared images. A vision
batch reserves one shared workspace bounded to four images; each page retains
its full additive host, KV, prompt and recurrent-state admission. No speed result
was available at qualification. Its initial build failed on an incorrect work-control import,
which was corrected before restarting qualification.


The first CPU-preparation-only engine22 C16/R128 0.8B run measures 3.1190 pages/s
with all 128 pages identical, 16.1 seconds of process CPU work and 24.7 seconds of
framed GPU work. It exceeds the previous C32 vLLM reference by 4.2%, which still
requires a fresh control and repetition. The preset-only and feature-off reader/HTTP checks both pass.
The corresponding 4B CPU-only run measures 0.6853 pages/s, below reference7.


Ready-image batching is not promoted. At C16/R128 it measures 2.5295 / 0.6783
pages/s for 0.8B / 4B; process CPU time rises to 40.8 / 104.7 seconds. At C32,
0.8B measures 2.8507 pages/s. Actual image batches are mostly singletons. The 4B
C32 epoch completes 123 pages before the remaining admissions hit the unchanged
scratch cap and retry bound; this failed run has no primary throughput result.
Its hash-bound admission audit records the exclusion.
`engine22_comparison_audit.json` records the five completed runs and their exact
fixture-string comparisons against reference7.

The next candidate adds explicit M4-only physical32 decode. It widens shared
projections and prepared workspace, while each recurrent and attention argument
table remains bounded to sixteen entries. Its prepared workspace acquires twelve
MiB instead of eight before use; every per-page state and existing memory cap
remains additive. The default is sixteen rows pending qualification and timing.


Engine23 passes 203 selected tests with one skip and full C conformance,
including the new 17/24/31/32-row matrices. Preset-only, explicit physical32 and
feature-off reader/HTTP checks pass. Its primary C32/R128 runs reach actual
32-row forwards and produce identical text, but measure 2.1717 / 0.6164 pages/s.
The source then exposed a dispatch gap: ordinary greedy heads run outside the
decoder frame, while the new matrix head was selected only inside that frame.
Only 176 / 148 wide head calls occur over 610 / 605 decode steps.

The 4B prefill precision flag also used row count to identify prefill, which
could affect thirty-two-row decode. The engine23 reports explicitly record
these limitations and are excluded from promotion; the 4B result does not
represent uniformly F32 decode. The hash-bound engine23 audit preserves the
measurements and output comparisons.

Engine24 fixes the owned-command-buffer head selection, adds its own runtime
counter, and keeps the prefill precision setting out of decoder frames and
standalone greedy heads. Explicit BF16 decode inputs remain a separate opt-in
profile. New C checks exercise GPU-produced standalone head inputs, separate
rounded-input references, full outputs, canaries and the rollback flag. Its
qualification passes 203 selected tests with one skip, all C conformance,
and preset-only / explicit32 / feature-off reader checks. Primary results:

| Engine24 C32/R128 profile | 0.8B pages/s | 4B pages/s |
| --- | ---: | ---: |
| Physical32, F32 decoder | 3.0119 | 0.7126 |
| Physical32, explicit BF16 decoder | 2.5848 | 0.7247 |

Every completed primary run preserves the full OCR strings for all 128 pages.
The owned-head counter proves the new standalone path actually executes.
Global BF16 regresses the small model and offers little benefit on the larger
model; it remains opt-in.

Fresh paired8 automatic-capacity vLLM controls measure 0.84419 at C16 and
0.87968 at C32 for 4B. The following native BF16 C32 run measures 0.6995,
20.5% behind the faster control. The paired driver intentionally stops on
that loss; the 0.8B controls and reverse-order repeats did not run.
`paired8_comparison_audit.json` binds those reports and records the loss.

An additional qualified-engine24 prefill-only experiment keeps CPU image
preparation on and vision batching off. With a 100-ms coalescing window it
measures **0.8859 pages/s**, versus **0.8316** with zero wait. Both use F32
decoder inputs, BF16 prefill inputs, C32/R128, the same caps, and exact output
checks. The 100-ms run contains 99 singleton, five four-page, three two-page,
and one three-page prefill groups. The zero-wait run contains 128 singletons.
The hook also changes orchestration and final-row handling. These unpaired
runs do not isolate batching or eliminate variation on the shared machine. The 0.7% margin over the
previous fresh vLLM control is **not a repeatable win**; fresh paired repeats
are still required. `engine24_comparison_audit.json` covers seven completed
primary native reports, with lossless logs in `raw_logs_manifest.json`.

The separate engine24debug ReleaseFast build retains symbols and has the
same 18 frozen source hashes, with the same 203 passing tests and one skip.
Its instrumented R64 run is excluded from primary throughput evidence.
Three CPU samples mostly show event parking, GPU completion waits and Metal
submission. A few malformed stacks recursively repeat an unknown PC into
libBLAS; they do not establish a CPU BLAS fallback.

Isolated recurrent-prefill core timings at 432/662 tokens and 32 value heads
are approximately 1.22/1.74 ms per layer. Their total scope is too small to
explain the earlier end-to-end gap. Matched GEMM probes are diagnostic:
native GPU times include a GPU input producer and BF16 packing; MLX wall
times prepare BF16 inputs outside the timer and include F32 output casting.
The native pack benchmark has six projections per frame and CPU canary
fills inside its wall timer, so GPU time divided by six is the cleaner
native measurement. At 432/864 rows, hidden2560/output4096, those native GPU
times are approximately 1191/1647 us per projection; MLX BF16-input wall
medians are 1014/1813 us. These measurements support investigating batching;
they do not establish a large dense-kernel deficit or numerical equivalence.

The submission hypothesis is informed by the installed-version
[MLX Metal command-buffer implementation](https://github.com/ml-explore/mlx/blob/v0.32.2/mlx/backend/metal/device.cpp),
which commits command buffers at operation/byte thresholds without waiting
at each commit. This is an inference about a possible optimization, not a
measured causal explanation. Native vision currently waits once per block;
4B has 24 vision blocks and 0.8B has twelve. Engine25 is being qualified to
experiment with bounded groups of vision blocks, charging larger complete
host/device envelopes before execution, and single-frame decoding through
32 rows. Those changes remain opt-in pending correctness and timing.


Engine25 passes 204 selected tests (203 pass, one skip), including real
four-block vision groups, distinct reordered vision inputs with zero output
difference, and single-frame decode with zero topology mismatches. Separate
preset-only, explicit32 and feature-off reader/HTTP checks pass. Complete
verification logs use distinct driver/native paths; earlier functional logs
may have their leading bytes overwritten by a shared stdout redirection and
are superseded by the `ocr-engine25-verify-*` logs.

The C32/R128 fused-decode run measures 0.8801 pages/s; all 570 decode steps
use one frame, standalone wide heads are zero, and all outputs match. This
is flat against the earlier prefill-only candidate. The one-second prefill
window with ordinary vision measures 0.7313 pages/s despite batching 37 of
128 pages. Four-block vision at C32 fails scratch admission after 120 pages;
two-block vision with the longer prefill window fails after 123. The 8 GiB
scratch cap and existing admission timeout remain unchanged. Both failures
have hash-bound admission audits and no partial throughput result.
`engine25_comparison_audit.json` covers all four reports.

The collection regression exposed an orchestration issue: the prefill hook
publishes a manual prefill turn before its broker finishes collecting.
Scheduler fairness can then select work that is still unavailable, leaving
the decode executor idle. Engine26 moves readiness publication into the
sealed-group executor and adds a regression covering unsealed groups,
ready decode, and a younger group becoming ready before an older one.
It also extends existing serial prefill pack-buffer reuse from 2,048 to
4,096 rows, covering four-page groups whose projections already support
those sizes. Standalone, submitted-frame, concurrent and explicit-off guards
remain intact. New C checks cover 2,047/2,048/2,049/2,648/4,096/4,097 rows,
including the non-reuse upper boundary and the 9,216-column down projection.
Engine26 and its formatting-only engine27 revision pass 204 selected tests
with one skip. C conformance, wide projection, batch attention, six original
pack-reuse cases, and nine extended-boundary cases pass. The isolated large
pack benchmark improves GPU time by approximately 1–3%; this does not
establish an end-to-end throughput gain.


Engine27 primary 4B C32/R128 profiles measure 0.6910 and 0.6077 pages/s
with one-second and 100-ms prefill windows. A C24/two-block vision profile
measures 0.6632. All 128 outputs match in each run. A fresh load-check vLLM
C32 control measures 0.8445747 pages/s; the following original-path native
control measures 0.6768. These are shared-machine measurements, not a
formal forward/reverse repeated comparison. The load-check audit records
exact strings, automatic vLLM KV capacity, and the absence of our own CPU
compilation during primary timing.

Engine28 passes 205 selected tests with one skip, plus preset-only,
explicit32, and feature-off reader/HTTP checks. It publishes only the
sealed prefill leader and waits on the coordinator event; followers remain
unpublished until their group completes. Completion transitions the turn
to decode directly. Prefill/decode fairness is configurable; its default
stays at four decode turns. Additional grouped-vision host scratch is held
once at the shared GPU gate, while every page retains its full base host,
output, KV and recurrent reservations. Device and host peaks grow under
admission before execution and remain charged through backend teardown.
The combined-cap, failed-growth, retirement and teardown checks pass.

| Engine28 4B profile, R128 | Pages/s |
| --- | ---: |
| C32, vision1, streak4, F32 | 0.6857 |
| C32, vision1, streak0, F32 | 0.6800 |
| C24, vision4, streak0, F32 | 0.7171 |
| C28, vision2, streak0, F32 | 0.7013 |
| C32, vision4, device17/scratch10, F32 | 0.8035 |
| Same budgets and fused decode, F32 | 0.6873 |
| Same budgets and fused decode, BF16 | 0.5954 |

The first four profiles use device16/scratch8 GiB; the last three explicitly
use device17/scratch10 GiB. All retain the 28 GiB combined cap, live reserve,
4 GiB KV cap and per-page state accounting. The latter profiles encounter
one or two bounded shared-memory admission waits and complete successfully;
those waits remain part of throughput. Every run preserves all 128 complete
OCR strings. `engine28_comparison_audit.json` binds the seven reports and
lossless logs. The best result is approximately 19% above the preceding
native control and 4.9% below the fresh vLLM control. Different ordering,
budgets and shared-machine variation prevent attributing that difference
to a single change. No repeated vLLM win has been established.


Engine29 extends single-consumer BF16 input-buffer reuse to serial decode
rows17–32 and vision rows32–4096. Owned, submitted-prefix, concurrent and
explicit-off cases retain the allocation path. Precision settings and
existing prefill counters keep their previous scope. Independent decode and
vision counters prove reuse without mixing the phases. Qualification passes
205 tests with one skip, full C conformance, 22 decode cases and eight vision
cases with complete byte comparisons, finite-output checks and canaries.
Preset16, physical32 and feature-off reader/HTTP checks pass. The first
qualification incorrectly enabled global BF16 for an existing F32-specific
capability test; its failed log is preserved, and the corrected F32 run passes.

Twelve-trial interleaved microbenchmarks show vision projection GPU times of
12,051/10,831 us and 12,849/12,791 us without/with reuse, for six projections
per frame. Corresponding wall times are 16,634/15,540 us and 17,498/14,648 us;
wall scope includes CPU canary fills. Decode GPU improvements are small or
flat across the four tested shape/pool combinations. These microbenchmarks
are not evidence of complete reader speedup.

| Engine29 0.8B C16/R128, vision4, shared core cache | Pages/s |
| --- | ---: |
| Original prefill, F32 inputs | 2.9159 |
| Original prefill, BF16 vision/prefill inputs | 2.8067 |
| Grouped prefill, F32 inputs | 2.5109 |
| Grouped prefill, BF16 vision/prefill inputs | 2.4569 |

All four complete runs preserve every page's full OCR string and encounter
no admission pressure. They do not include a fresh repeated vLLM comparison.
The attempted 4B profile is excluded: live-memory admission rejects model
loading before timing, requesting 11,348,275,670 bytes against a
6,700,148,981-byte capacity after the existing 6 GiB reserve. No throughput
is calculated from this failure, and the remaining three large profiles do
not run. `engine29_qualification_audit.json`,
`engine29_small_comparison_audit.json`, and
`engine29_large_admission_audit.json` bind these results and limitations.

Engine30 passes 206 selected tests with one skip, plus all three reader/HTTP
functional checks. Its reader test executes twenty actual splits. C conformance
is reused by exact implementation/checker hashes from engine29. It permits
prefix/suffix streaming through
32 rows only after all linear-layer plans and persistent workspace are ready.
The first ordinary forward prepares the suffix; a submitted prefix cannot
trigger plan upload or workspace allocation. The physical32 prefix overlap
reservation grows from 8 to 12 MiB; two disjoint 9 MiB attention banks remain
charged. The following complete native profiles and paired controls test this experiment.


| Engine30 0.8B C16/R128, F32, original prefill | Pages/s |
| --- | ---: |
| GPU image prep, vision1, cache off, stream off | 2.9009 |
| Same, split after eight decoder layers | 3.0929 |
| CPU image prep, vision4, cache off, split8 | 3.3677 |
| CPU image prep, vision4, cache on, split8 | 2.5907 |
| CPU image prep, vision4, cache off, split12 | 2.7697 |
| CPU image prep, vision4, cache off, split16 | 3.5462 |
| CPU image prep, vision4, cache off, fused decode, stream off | 3.7439 |

A separate physical32/C32 split8 profile measures 2.7200 pages/s. All runs
complete 128 pages with exact fixture strings. Every split profile retains
18 prepared linear blocks per decode step; suffix streaming does not fall
back to the generic linear-block path. The fused profile has 905/905
single-frame steps and zero topology mismatches. These are successive
shared-machine profiles, not isolated causal effects.

Fresh paired10 tests the C16 split8 candidate. vLLM C16/C32 controls measure
3.4022/2.8365 pages/s, followed by native at 1.6334. Reversing order measures
native at 1.3019 followed by fresh vLLM C16 at 1.5156. Native remains behind
in both orders, including 14.1% in the latter pair. The large variation also
affects vLLM; no leftover benchmark workers are found, and no own CPU
compilation or sampling overlaps timing. All 128 outputs match in every
run. This is **not a parity win**. `paired10_08_comparison_audit.json` binds
all comparisons. `engine30_small_comparison_audit.json` and
`engine30_stream_sweep_audit.json` cover the candidate profiles. Fresh paired11 tests the fused C16 candidate with CPU image preparation,
vision groups of four, shared core cache off, F32 decoder and prefill inputs,
original prefill, and streaming off. Fresh vLLM C16/C32 controls measure
2.9041/3.0204 pages/s, followed by native at 3.2209: **6.64% faster than the
faster control**. In reverse order native measures 3.3710, followed by fresh
vLLM C32 at 2.9709: **13.47% faster**. All 128 pages match their serial
references and all fixture strings match across runtimes in both orders.
Single-frame topology mismatches and admission pressure are zero. This is a
repeated win for this build, hardware, corpus and configuration; it does not
establish a general model or hardware speed advantage.
`paired11_08_comparison_audit.json` binds both comparisons.

Engine30 4B C32/R128 uses CPU image preparation, vision4, shared core cache,
F32 decoder inputs, BF16 vision/prefill inputs, and unchanged 17/10/28 GiB
backend/scratch/combined caps:

| Profile | Pages/s |
| --- | ---: |
| Single frame, streaming off, original prefill | 0.7535 |
| Split16, original prefill | 0.7751 |
| Split8, original prefill | 0.6802 |
| Single frame, grouped prefill at 100 ms | 0.6348 |

All four complete 128 exact pages; bounded live-memory backpressure remains
included in throughput. A C16 attempt is rejected at model loading by the
existing 6 GiB live OS reserve and is excluded. C24 is unrun. These profiles
have no fresh engine30 vLLM controls and remain below the last fresh 4B
control of 0.8446 pages/s. `engine30_4b_comparison_audit.json` preserves the
complete results and excluded attempt.

Florence still has no installed vLLM Metal adapter. A fresh valid MLX-VLM
serial proxy measures 4.1319 pages/s; native C8/batch8 measures
3.9788/4.2509 and 4.3793/4.3594 across two processes and two 64-page rounds.
Every native page exactly matches its native serial reference; cross-runtime
content matches after whitespace removal (line breaks differ). This is not a clear repeated win. Native uses publisher Q4_K and
no-repeat trigrams, while MLX uses original FP16 and default greedy decoding,
so precision and generation policy differ. The attempted MLX batch2 control
fails content parity and is excluded; batch4/8 are unrun.
`engine30_florence_comparison_audit.json` and
`engine30_florence_batch_mismatch_audit.json` preserve these limits.

Engine31 joins the general BF16 MPP path to an existing planned compute
encoder and adds a fused Florence decoder/head/argmax frame. Forty new C
conformance cases cover dedicated, planned, rollback and concurrent
execution with both activation-pool settings. All eight C suites pass.
Florence frame on/off qualification preserves native fixture strings.
Fresh paired13 4B nevertheless remains behind vLLM in both orders:
0.7470 versus 0.8493 pages/s, then 0.7133 versus 0.7795.
The paired13_4b_comparison_audit.json file preserves the losses. Florence
paired14 also fails to establish a repeated proxy win; its raw cross-runtime
strings differ in whitespace. The strict native serial checks still pass.
The paired14_florence_comparison_audit.json file records both measures.

Engine32 enters the planned encoder scope at the start of an outer-owned
LightOn decode frame. Its counters prove scope entry and matching
single-frame step counts, not uninterrupted encoder ownership across every
operation. The recurrent fast path already joins planned encoders and is
not a newly discovered CPU fallback. Diagnostic phase traces are excluded
from primary throughput.

Engines33–34 introduce an opt-in logical window of 64 independent page
leases while GPU forwards remain bounded at 32 rows. The public model batch
contract remains 32. Worker/control arrays cover all 64 logical slots;
engine34 also fixes the benchmark task's image array. The separate request
admission capacity is explicitly 512 units for these tests. Memory caps and
the live OS reserve remain enforced. Public32 and external64 qualification
preserve all fixture strings; an oversized public64 request is rejected
cleanly by the earlier decoded-image pixel guard. The failed engine33 queue
capacity and benchmark-array attempts remain archived. An engine34 C48 4B
attempt fails combined-memory admission and has no complete throughput.

Engine35 omits actual unused CPU recurrent matrices in an explicit GPU-only
mode. A 4B page avoids 51 MiB of CPU shadow arrays while retaining its GPU
histories, side weights, KV, descriptors, projected inputs and outputs.
CPU fallback fails closed in that mode; the ordinary host cache remains the
default. Actual reader counters report zero host recurrent allocation,
and rollback qualification reports positive host allocation with exact
output. The complete C32/R128 4B profile measures 0.8124 pages/s; this is not
a fresh vLLM win. The engine35_qualification_audit.json and
engine35_4b_comparison_audit.json files bind these checks and results.

Engine36 gives serialized decoder temporary workspace an epoch owner.
Every page retains its complete persistent state and escaping hidden/logit
outputs. A checked, monotonic power-of-two reservation covers two temporary
working sets plus the physical batch head. It remains held through backend
teardown. Shared scratch requires prepared execution, epoch admission,
GPU-only recurrent state and unbatched prefill; incompatible configurations
decline it. The reader proves a 128 MiB peak for 0.8B, and the 4B profiles
reserve 256 MiB. All 216 selected tests pass with one skip, plus preset16,
physical32, feature-off, public32, external64 and explicit sharing rollback
reader checks. Two rejected test-fixture revisions are preserved.
The engine36_qualification_audit.json file binds the qualification.

| Engine36 4B candidate, R128 | Pages/s |
| --- | ---: |
| C48, four-block vision groups | 0.9502 |
| C64, two-block vision groups | 0.8758 |
| C48, up to four images per vision batch, one-block groups | 0.9208 |
| C64, four-block vision groups | Excluded: backend admission timeout |
| C64, three-block vision groups | Excluded: backend/combined admission timeout |

Every completed candidate preserves all 128 native serial outputs and the
full vLLM fixture strings. The unchanged limits are backend17/scratch10/KV4/
combined28 GiB with the existing 6 GiB live OS reserve. The two failed
attempts have no throughput result. Image batching consumes 165.421 CPU
seconds, compared with 17.735 for the fastest candidate; its separate
32-request CPU-sampling diagnostic is excluded from primary comparisons.
Stripped symbols and malformed repeated BLAS frames prevent causal
attribution from that sample. The engine36_4b_comparison_audit.json file
binds the complete candidates and exclusions. These successive measurements
do not establish a causal speedup or a fresh vLLM win; paired16 performs
fresh C16/C32/C48/C64 automatic-cache controls and reverse-order repetition.

Fresh paired16 4B completes without a win. Automatic-cache vLLM controls at
C16/C32/C48/C64 measure 0.9122/1.0094/1.0668/1.1040 pages/s. Native C48 with
four-block vision groups then measures 0.9649, 12.6% behind the fastest
control. Reversing order measures native at 0.9502, followed by fresh vLLM
C64 at 1.0585, leaving native 10.2% behind. All 128 native pages equal
their serial references, and full fixture strings match across runtimes.
The paired16_4b_control_audit.json file binds all five reference reports;
paired16_4b_comparison_audit.json preserves both losses.

The engine36 BF16 decoder-input candidate completes 128 exact pages at
0.9127 pages/s, with 65,091 packing calls and 64,496 reuse hits. Reuse is
active, but this candidate is slower than F32. Its evidence is bound by
engine36_4b_bf16_decode_audit.json.

The 0.8B geometry sweep completes 128 exact pages per configuration:
C16/rows16 = 3.9055, C32/rows16 = 3.9849, C64/rows16 = 4.0078, and
C64/rows32 = 4.0342 pages/s. All retain F32 decoder/prefill inputs, CPU
image preparation, four-block vision groups and no shared core-weight cache.
The engine36_08_geometry_comparison_audit.json file binds these profiles.

Fresh paired18 tests the C64/rows32 candidate against automatic-cache vLLM
C16/C32/C48/C64 controls at 3.4491/3.7125/3.8653/3.9267 pages/s.
Native measures 4.0501, a 3.14% lead over the fastest control. Reversing
order measures native 3.8527 and fresh vLLM C64 3.7167, a 3.66% lead.
All 128 pages equal native serial output and full cross-runtime fixture
strings in both pairs. This is a small lead in both tested orders; it does
not meet the predeclared 5% clear-win threshold or establish a general
speed advantage. The paired18_08_control_audit.json and
paired18_08_comparison_audit.json files preserve the evidence and limits.

Engine37 adds optional encoder scopes around Qwen vision/prefill layers and
Florence encoder blocks. A scope is closed only by its owner; unsupported
compute wrappers decline it before casting their context. All 217 selected
tests pass with one skip, and reader checks preserve full native strings.
The 4B C48/R128 candidate measures 1.0175 pages/s; the 0.8B C64/R128
candidate measures 4.0606. These are unpaired candidates, not fresh wins.
The engine37_qualification_audit.json and engine37_encoder_comparison_audit.json
files bind the checks and results.

Fresh Florence paired19 still trails its MLX proxy: the serial FP16 controls
measure 5.0863 and 5.0815 pages/s, while native scopes-on measures 4.8700
and 4.9887. Native scopes-off reaches 5.0088 in the reverse order. The
geometry sweep also leaves C8/batch8 best, with C16/batch8 at 4.9653 and
C32/batch8 at 4.7124. Every native page matches its native serial output.
Cross-runtime agreement requires whitespace normalization: raw strings differ.
Native uses publisher Q4_K weights with F16 encoder mirrors and no-repeat-3;
the proxy uses original FP16 weights and default greedy decoding. There is
no Florence adapter in the installed vLLM Metal runtime. The paired19 and
engine37_florence_geometry_comparison_audit.json files retain these limits.

Engine38 qualifies explicit physical64 execution, separately from logical64
request admission. Prepared scratch grows from 12 to 24 MiB; per-page state,
KV and outputs remain additive. The 16-row attention argument banks are
reused in ordered chunks. Actual row policy is immutable within an epoch.
The new 64-row projection tile has an explicit rollback, and narrow recurrent
gates can retain the planned encoder with checked resource ranges.
All 219 selected integration tests pass with one skip. Nine C suites pass,
including 140 new wide-batch cases and the unchanged 40 strict planned-MPP
cases. New cases require exact same-tile outputs, bounded cross-tile/CPU
error, canaries, repeated GPU producers, and bounded buffer retirement.
Both LightOnOCR sizes execute actual 64-row cores with exact fixture strings;
public32 and public64-invoice requests pass, while oversized mixed public64
requests retain the decoded-pixel rejection. Qualification keeps the combined
28 GiB cap and live OS reserve. The engine38_qualification_audit.json file
binds the build, source hashes, numerical checks and reader evidence.

| Engine38 4B candidate, R128 | Pages/s |
| --- | ---: |
| C64, two-block vision groups, 64-row tile, F32 decoder inputs | 1.0941 |
| C56, four-block vision groups, 64-row tile, F32 decoder inputs | 0.9689 |
| C56, three-block vision groups, 64-row tile, F32 decoder inputs | 0.9972 |
| C64, two-block vision groups, previous tile, F32 decoder inputs | 1.0104 |
| C64, two-block vision groups, 64-row tile, BF16 decoder inputs | 0.9530 |

All five candidates complete 128 exact pages without admission pressure.
The fastest averages 41.46 pages per decode step, with 290 steps and
103.166 seconds of measured GPU work. BF16 packing is active in its
candidate (35,883 calls, 35,629 reuse hits), but is slower. These are
unpaired measurements and do not establish a fresh vLLM win.
The engine38_4b_comparison_audit.json file binds all five profiles.

The 0.8B physical64 sweep measures 4.1745 pages/s with the new tile and
4.2079 with the previous tile. Both complete 128 exact pages without
admission pressure. The selected tile therefore differs by model.
The engine38_08_comparison_audit.json file binds these candidates.

Fresh paired20 repeats automatic-cache vLLM C64 controls, the winning
concurrency from the earlier C16/C32/C48/C64 searches. For 0.8B, native
measures 4.3559 versus vLLM 3.9361 pages/s, a 10.67% lead. Reversing
order measures native 4.2043 versus fresh vLLM 4.0078, a 4.90% lead.
All 128 pages match native serial output and full cross-runtime fixture
strings in both orders. This is a repeated measured lead; the second pair
falls just below the predeclared 5% clear-win threshold.

The 4B model still loses both fresh pairs: native 1.0175 versus vLLM
1.1386, then native 0.9976 versus fresh vLLM 1.1038 pages/s. These leave
native 10.63% and 9.62% behind, despite the best unpaired candidate at
1.0941. Both pairs retain exact output. No compilation or CPU sampling
overlaps these comparisons. The paired20_08_comparison_audit.json,
paired20_4b_comparison_audit.json and combined paired20 audit retain all
reports, source/binary hashes and limits. The overall vLLM goal remains
unmet for 4B.

Engine39 tested overlapping vision-block recording with a submitted block.
Its 221 selected integration tests pass with one skip, including explicit
frame lifetime/failure tests, and both models pass actual 64-page on/off
qualification with exact strings and complete memory reservations. However,
every sustained candidate is slower: 4B off/C64/VF2 measures 1.0930 pages/s,
on/C64/VF1 0.9734, and on/C56/VF2 0.9322; 0.8B off/C64/VF4 measures 3.9809,
on/C64/VF2 3.9467, and on/C64/VF1 3.7929. The experimental runtime path is
removed from the following revision. Its source patch, qualification and
failed throughput candidates remain available for review.

The separate engine39 GPU-stage diagnostic is excluded from throughput
comparisons. It captures actual timed-cohort frames, but its original
prefill “attention” bucket includes FFN work in linear-attention layers,
and non-roofline frame IDs are not copied at submission. Exact per-sample
row counts are also unavailable. The engine39_4b_gpu_stage_diagnostic_limitations.json
file records these limits; the samples do not establish a pure recurrent
kernel bottleneck.

Engine40 removes the unsuccessful engine39 asynchronous vision path and
adds bounded vision/prefill batch sizes. Shared decoder scratch now scales
with the captured maximum prefill group size; page KV, recurrent history,
inputs and outputs remain additive. CPU prefill temporaries acquire their
own host reservation before allocation and release it after group use.
All 222 selected integration tests complete: 221 pass and one skips.
Ten fresh C suites pass, including frame metadata propagation. Three
functional checks and six mixed-document qualifications pass with exact
strings, matched scratch permits, the 28 GiB combined cap, and live OS
reserve. The first rollback harness incorrectly required more than 32
active rows at concurrency 16; its original failure is preserved, and
the corrected harness conditions that check on concurrency above 32.

| Engine40 R128 candidate | 4B pages/s | 0.8B pages/s |
| --- | ---: | ---: |
| Unbatched vision/prefill, C64 | 1.0874 | 4.0869 |
| Two-page vision, single-page prefill, C64 | 0.8757 | 3.6174 |
| Two-page vision/prefill, C60 for 4B or C64 for 0.8B | 0.9328 | 3.6341 |
| Two-page vision/prefill, C56 | 0.9277 | — |

Every candidate completes all 128 pages with exact native serial and full
fixture identity, without admission pressure. These unpaired results show
a regression from batching; no new vLLM win is claimed. The batch callbacks
construct group cancellation controls without an Io context, which makes
their contended GPU gate yield repeatedly instead of waiting on its event.
The next revision restores the borrowed context and adds a contention test;
its performance still requires measurement.

Engine41 restores the borrowed Io context in all three LightOn batch
callbacks. Contended GPU gates now use event waits while retaining group
cancellation semantics. The new regression test exercises an actual parked
waiter, successful handoff while one peer is cancelled, and removal when
every peer cancels. All 223 selected integration tests complete: 222 pass
and one skips. Three functional checks and six mixed-document checks pass.
C kernel/checker hashes are identical to engine40's ten passing suites.

The fix removes excessive CPU use but does not make batching the fastest
configuration. Sustained 4B vision batching drops from 357.5 to 29.1 CPU
seconds; vision plus prefill at C60 drops from 330.2 to 30.2 CPU seconds.
These are separate shared-machine runs, with no own compilation or sampling.

| Engine41 R128 candidate | 4B pages/s | 0.8B pages/s |
| --- | ---: | ---: |
| Unbatched vision/prefill, C64 | 1.0410 | 4.0968 |
| Two-page vision, single-page prefill, C64 | 0.9164 | 3.9817 |
| Two-page vision/prefill, C60 for 4B or C64 for 0.8B | 0.9380 | 3.9999 |
| Two-page vision/prefill, C56 | 0.9474 | — |

All seven candidates complete 128 exact pages without admission pressure.
The unbatched profiles remain selected. These results do not establish a
new vLLM win; paired20 remains the latest fresh cross-runtime comparison.

Engine42 integrates the isolated wide gate/up MPP candidate. Both isolated
ABBA runs pass full numerical checks, GPU producers, offsets and canaries;
the repeat selects a 32x16 tile with two SIMD groups. Eleven fresh C suites
pass, including dedicated wide gate/up cases, and all 223 selected
integration tests complete: 222 pass and one skips. Three functional
checks and actual 64-page on/off checks pass with exact output and positive
dispatch counts only when enabled.

The sustained measurements do not support enabling it for 4B:

| Engine42 R128 | Rollback pages/s | MPP pages/s |
| --- | ---: | ---: |
| 4B | 1.0877 | 1.0282 |
| 0.8B | 3.8006 | 3.8781 |

Both sizes complete all 128 pages with full string identity and unchanged
memory caps. The 4B candidate regresses about 5.5%; the 0.8B candidate improves
about 2% in this unpaired comparison. A faster isolated kernel is insufficient
evidence for a full-model improvement. The next revision must retain the
4B rollback until a replacement wins end to end. No paired23 vLLM run was
performed for these candidates.

The corrected engine41 stage diagnostic is excluded from throughput
comparisons. It records four timed prefill frames and thirteen decode
frames with actual shapes and positive frame IDs. Median prefill GPU time
is 415.0 ms, including 218.6 ms attributed to FFN work; median sampled
decode GPU time is 96.0 ms, including 48.2 ms attributed to FFN work.
Per-frame row counts are retained; decode samples cover different batch
sizes, so these medians do not describe a fixed-size decode kernel.

Engine43 rolls back the regressing wide gate/up MPP default for 4B. The
0.8B selector retains its qualified tile; 4B retains the preceding generic
fused kernel. It also adds explicit GPU-greedy prefix streaming. This
submits a bounded prefix while encoding the tail, then computes the
vocabulary head and checked argmax on the tail frame. Its counters are
separate from the original strictly single-frame counter. Both frame
scratch envelopes remain admitted, and a new test checks cap-minus-one
denial, exact-cap admission and release at physical64.

All eleven fresh C suites pass. Integration selects 224 tests: 223 pass
and one skips. Three existing reader-path functional checks pass. Actual
64-page checks pass on both models with streaming off/on, full raw string
identity, positive two-frame/stream counters only when enabled, zero
topology mismatches and no admission pressure.

| Engine43 sustained R128 / C64 | 4B pages/s | 0.8B pages/s |
| --- | ---: | ---: |
| Single frame, streaming off | 1.0606 | 4.1133 |
| GPU-greedy stream, prefix 4 | 0.9785 | 4.2111 |
| GPU-greedy stream, prefix 8 | 0.9776 | 4.1975 |
| GPU-greedy stream, prefix 16 | 1.0189 | 4.2430 |

Every candidate completes 128 pages with exact own-reference and
cross-runtime strings under unchanged budgets. No own compilation or
sampling overlaps the primary timings. Streaming regresses 4B and stays
off there. The best 0.8B candidate improves about 3.2% over its native
control, but this does not establish a fresh vLLM comparison. Prepared
paired24 has not been executed; paired20 remains the latest fresh
cross-runtime evidence.

The engine43 prefill tile probes are also excluded from throughput
claims. An initial short isolated probe suggested large gains, but a
representative probe using 32 distinct weights, 32 matrix operations in
one planned frame and validation after each timed leg reduced the
differences to small or mixed results. Neither the alternate 64x32 tile,
64x64 tile nor swizzled dispatch is promoted. The complete inputs,
comparisons and raw logs are retained.

Engine44 adds CPU preprocessing overlap for native Florence and evaluates a
64x32 gate/up tile for 4B. The tile passes representative working-set
microbenchmarks, but sustained single-page-prefill throughput falls from
1.0883 to 0.9620 pages/s. It remains opt-in. Two-page prefill at C60 reaches
1.0937 pages/s with the tile versus 1.0181 without it. The C64 two-page
candidate completes all 128 exact pages but records two shared-memory
admission pressures and is excluded from primary comparisons. No caps
are raised. The aligned-checkpoint diagnostic preserves every tensor byte
and does not beat the original checkpoint's control.

Engine45 keeps the old 4B kernel at exactly 64 rows and uses the alternative
tiles only for partial batches when explicitly enabled. Wide FFN dispatch
also reaches the eligible attention layers. Native Florence capability
publication now reflects its configured batch size, and the benchmark
accepts batches through the existing runtime ceiling of 64.

All twelve C conformance suites pass. Of 230 selected Zig tests, 229 pass
and one skips. The existing three reader functional checks pass. Borrowed
raster checks cover all three fixtures with preprocessing overlap off/on,
one/two pages, padded RGBA strides, unchanged caller bytes and exact native
output. Native Florence batch16 initially fails run admission before batch
GPU execution: the conservative full-prefix logits reservation exceeds
the unchanged scratch cap. Batch12 qualifies with actual groups 12/12/8.

| Engine45 sustained R128 | Control pages/s | Candidate pages/s |
| --- | ---: | ---: |
| 4B C64, single-page prefill, partial-batch gate/up | 1.0597 | 1.0484 |
| 0.8B C64, GPU-greedy prefix16 | 3.8453 | 3.9062 |
| Florence, CPU overlap, B8/C16 versus B12/C24 | 4.7407 | 4.9433 |

These six candidates complete all 128 pages with exact native strings,
unchanged budgets and no admission pressure. LightOn cross-runtime strings
are also exact; Florence's MLX proxy comparison uses whitespace-normalized
content because its precision and decoding policy differ. These unpaired
rates do not establish a fresh vLLM win. Fresh paired26 controls are
recorded separately; incomplete audits must not be reported as wins.

Fresh paired26 LightOn comparisons complete in both execution orders:

| Model / order | Native pages/s | vLLM pages/s | Native / vLLM |
| --- | ---: | ---: | ---: |
| 0.8B, vLLM then native | 4.4744 | 3.8861 | 1.1514 |
| 0.8B, native then fresh vLLM | 4.4384 | 4.0150 | 1.1054 |
| 4B, vLLM then native | 0.9799 | 1.1071 | 0.8851 |
| 4B, native then fresh vLLM | 0.9964 | 1.0936 | 0.9111 |

All eight runs complete 128 pages with exact full strings. Native has no
admission pressure and retains its fixed caps; vLLM uses its automatic
cache at C64. Native 0.8B enables prefix16 GPU-greedy streaming; native 4B
uses single-page prefill and the preceding 64-row kernel. No own compilation
or sampling overlaps these timings. The shared machine is uncontrolled.
The 0.8B profile wins repeatedly; the 4B profile does not reach parity.
See paired26_4b_08_comparison_audit.json for report hashes and settings.

Florence paired26 also completes both orders at native B12/C24 with CPU
preprocessing overlap: native 5.4539 versus MLX 5.1288 pages/s, then native
5.4460 versus fresh MLX 5.1302 pages/s. These are repeated 6.2–6.3% wins
against the valid serial MLX proxy, not vLLM Florence results. All native
strings remain identical to the preceding native reference; cross-runtime
strings match after whitespace normalization. Actual native batch sizes,
single-frame counters, raw logs, caps and report hashes are retained in
paired26_florence_comparison_audit.json.

The subsequent engine45 4B prefill search preserves the same caps. The C64
two-page-prefill control finishes 128 exact pages at 1.0874 pages/s but
records two shared-memory admission pressures, so it is excluded. At C60,
the opt-in partial-batch gate/up candidate with two-page prefill reaches
1.1108 pages/s without pressure, with all 128 pages exact. Its 83 host
scratch acquisitions/releases balance and the peak is 4,021,776 bytes.
This is a candidate result; fresh paired27 determines its vLLM comparison.
The prefill collector allows up to one second to form groups in these
throughput profiles; single-page prefill remains available for latency.

Disabling the ordinary decoder FFN selector gives 0.9811 pages/s with
the preceding recurrent FFN kernel and 0.9660 with the optional partial
tile. The prepared recurrent FFNs use the C path directly and remain
enabled in this ablation. Both runs complete all 128 exact pages without
pressure; neither establishes an improvement.

The engine46 private probes do not change the runtime. Four larger
prefill matrix tiles pass 96 representative ABBA records, including
full GPU outputs, CPU corners, canaries and GPU-produced inputs. Their
improvements are small or mixed, with the largest tile often regressing.
Recurrent prefill column counts 8 and 16 each pass 600 timed records and
ten full output/state checks. Eight columns are mixed and sixteen regress
consistently. Neither probe is promoted. Combined stdout/stderr interrupted
some JSON records; the verified parser removes only the exact ten checked
conformance lines and recovers every unique record. Original raw logs,
both parsers and their hashes remain archived.

Four-page prefill at C56 also qualifies and completes 128 exact pages
without pressure, at 1.0571 pages/s. Fresh paired27 therefore selects the
preceding two-page/C60 candidate. It remains behind vLLM in both orders:
0.9628 versus 0.9969 pages/s, then 0.9467 versus 1.0138. Ratios are 0.9658
and 0.9338. These fresh controls supersede any inference of a win from
the unpaired 1.1108 result. Every run completes 128 exact pages.

Two engine47 isolated FFN probes pass 48 ABBA records each, using 32
distinct weight pairs, full GPU output checks, CPU corners, canaries and
GPU-produced inputs. BF16-input MPP fusion beats generic F32-input fusion
by 24–49% for the tested rows. A direct comparison using the identical
MPP tile shows only a further 4–6% gain from BF16 inputs, including its
32 GPU input packs per timed frame. These are same-math checks with
exactly BF16-representable source values, not a full-model quality or
throughput result. Runtime precision is unchanged.

Engine46's incremental Florence admission experiment passes its four
boundary tests after correcting the expected supplemental-denial category:
the ordinary API returns ResourceTemporarilyUnavailable while its base
permit is retained. The complete retry selects 234 tests: 233 pass and
one skips. It is not promoted. Shrinking decoder logits does not by itself
prove the separate vision encoder and host temporary peak. Engine48
restores the exact engine45 Florence reading source and its conservative
reservation; batch12 remains the qualified profile. Both the experimental
and restored source hashes and the review decision are preserved.

Engine48 retains an explicit F32 full64 FFN tile experiment. Both rollout
switches are required; other configurations retain the preceding selector.
A dedicated counter distinguishes actual full64 dispatch from partial
cohorts. The eligible attention layers can also use fusion at exactly
64 rows when explicitly enabled. All thirteen C suites pass, including
the new full64 mode, rollback/missing-PSO checks, offsets, canaries and
actual counters. Engine48 reuses those suites only after checking that
every C/header/checker input is byte-identical. Integration selects 230
tests: 229 pass and one skips. Full-model throughput is evaluated separately.

Engine48 full64 controls and fresh paired28 do not establish 4B parity.
In native ON/OFF/OFF/ON order, full64 F32 matrix fusion measures
1.0465 / 0.9549 / 0.9075 / 0.8465 pages/s. The enabled mean is 0.9465
versus 0.9312 disabled, about 1.6% higher, with substantial monotonic
variation across the sequence. All four complete 128 exact pages without
admission pressure; only enabled runs increment the full64 dispatch counter.
The engine48_4b_full64_gate_up_primary_audit.json file binds these controls.

Selecting the enabled profile by its mean, fresh vLLM then native measures
0.9558 versus 0.8024 pages/s. In reverse order, native measures 0.7926
versus fresh vLLM at 0.8518. Native remains 16.1% and 6.9% behind,
respectively. Every native page equals its serial reference, full fixture
strings match across runtimes, and the existing memory caps and reserve
remain enforced without admission pressure. The paired28_4b_comparison_audit.json
file preserves both losses. No own compilation or CPU sampling overlaps
these primary timings. Read-only power metadata reports AC power and no
recorded thermal warning; it does not establish the cause of timing drift.

The engine48 capacity sweep completes C63/P2/VF2 at 0.6919 pages/s
with 128 exact pages and no admission pressure. C60/P2/VF4 fails and is
excluded. Three-page prefill at C60/VF2 passes its R64 qualification,
but its R128 run has four backend pressure events; it too is excluded.
These unpaired capacity trials do not establish a speed improvement.

Private engine49 attention probes preserve F32 query/probability/accumulation/
output and the F16 KV cache. All 30 balanced comparisons and all five
standalone conformance suites pass. Reusing score/probability storage is
roughly neutral to 5.5% faster across the tested shapes; larger tiles and
cached queries mostly regress. None is promoted. The preparation records,
build manifest, and engine49_attention_probe_audit.json bind the exact inputs.

Private engine50 tests three recurrent lane mappings with the existing F32
state layout and arithmetic precision. The full staged checks pass, and
24-layer working sets at 64/48/33 pages retain full state/output parity.
All candidates regress: about 2–3%, 12–13%, or 20%, depending on the mapping.
No shader is promoted. Initial ARC build errors and a singleton fixture
error remain preserved; only the missing 33-page cases are rerun after
routing the singleton through the existing direct API. The final verified
audit binds both original and continuation logs.

The engine51 reference diagnostic uses a private Python startup hook;
installed vLLM/MLX packages remain unchanged. It observes 80 mixed
prefill/decode steps, up to nine prefill segments and eight vision images.
Synchronized stage-wall totals include serial references and are not pure
GPU times. Extra synchronization alters overlap, so its rate is excluded
from primary comparisons. Engine48 CPU stack samples are also excluded;
their stripped Zig symbols limit attribution. Metal resource-list handling
appears in active recurrent-encoding stacks, motivating a separately
controlled host resource-declaration experiment. This does not establish
its causal contribution to the throughput gap.

Engine52 tests deduplicating resource declarations within each recurrent
kernel dispatch. Native integration, fourteen fresh C suites and the three
functional profiles pass. The declarations drop by about 64%, but small-model
R128 ON/OFF/OFF/ON controls average 2.9618 versus 3.2313 pages/s: about 8.3%
slower. The experimental runtime path is removed. Both attempts to qualify
the large model stop at the unchanged live OS memory reserve before model
loading; they are excluded rather than treated as numerical failures or
throughput results. A read-only audit finds no surviving reference worker
among the recorded process lifetimes. Other workloads are left untouched.

Engine53 evaluates rolling decoder layer submissions with at most one
submitted and one active frame, reusing the existing two storage banks.
It preserves token order, arithmetic precision and state updates, and checks
submission counts against the configured layer groups with exactly one
argmax. This remains an opt-in experiment pending conformance, full-page
parity and controlled throughput qualification; it is not a parity claim.

Engine54 is an isolated batched argmax prototype. The existing row-wise
path dispatches partial and final reductions separately for each row; the
prototype combines all rows into two dispatches with the same F32 comparison,
suppression and tie arithmetic. Fifty boundary/producer/CPU/legacy-GPU cases
and 48 ABBA timing records pass after correcting the private Metal grid
argument types (the initial compiler failure is preserved). At 64 rows,
the candidate averages 30% of baseline GPU time and 43% of baseline wall
time. This microbenchmark is not a full-model speed result, and no production
argmax source changes have been applied.

Engine53 selects 232 integration tests: 231 pass and one skips. All thirteen
fresh C suites and three functional profiles pass. Small-model R64 controls
at chunk sizes 0/4/2/8/0 measure 3.4423/3.5557/3.3418/3.4689/3.2625 pages/s.
Every page matches its serial reference without admission pressure. The
four-layer option is a candidate for repeated controls, not a new paired win.
Large-model C60/P2/VF2 controls at 0/4/8/0 measure
0.7885/0.6866/0.7293/0.7154 pages/s. GPU time changes little while CPU time
increases; rolling chunks do not establish an improvement for 4B.

Additional engine53 R64 profiles retain exact page parity and all memory
bounds. Three-page prefill at C48/VF2 reaches 0.8045 pages/s without pressure.
Four-image vision batching at C48/P1/VF1 reaches 0.7379 pages/s but averages
only 1.45 images per batch. Increasing its bounded formation window to
100 ms fills an average 2.56 images and reaches 0.7879 pages/s. Combining
that vision setting with three-page prefill reaches 0.7914 pages/s. These
are unpaired qualification trials, not fresh vLLM comparisons.

The production-form engine54 argmax proposal passes its additional C check:
rows 1/2/16/33/64/65, both missing-pipeline fallbacks, a different vocabulary,
private GPU producers, ties/nonfinite values, token canaries, cancellation,
and planned versus standalone execution. Its native build stops at a Zig
syntax error before runtime tests. Engine55 corrects that error and replaces
a resettable shared event in the proposed batching queue with an atomic
readiness epoch. Queued groups remain mergeable while their stage executor
is busy; cancellation and deadlines remain per ticket. Both queue switches
and batched argmax remain independently opt-in pending full qualification.

Engine55 completes 238 integration tests (237 pass, one skips), fourteen
C suites and three functional profiles. Its sixteen-page smoke run passes;
a wrapper incorrectly expected wide-row projection calls at concurrency
eight. The verify2 checker validates the original saved run and retains its
execution-driver hash. No timing is rerun to correct that assertion.
Large-model stage-queue trials measure 0.6750/0.6920/0.6567 pages/s.
A fresh disabled control reaches 0.7802 pages/s with lower CPU use.
A separate fresh vLLM R64 diagnostic reaches 0.9240 pages/s; that screen
remains a loss and is not presented as a paired R128 result.

Engine56 reduces periodic queue checks while retaining readiness wakeups and
per-ticket deadlines. It also adds independently optional storage reuse.
Two vision SDPA packing banks may survive frame completion only while the
existing shared epoch vision reservation remains held. Backend teardown
clears both banks before releasing that reservation. Larger BF16 projection
inputs reuse only frame-local storage, after its sole consumer; the former
fresh buffers were already retained through the frame, so reuse adds no
persistent allocation. All MSL source functions remain byte-identical.
The caps and reservation sizes are unchanged.

Engine56 passes 238 integration tests (237 pass, one skips), all sixteen C
suites and three functional profiles. The added suites cover bank reuse,
growth, two in-flight owners, cancellation, output canaries, busy-clear
rejection and final release, plus large projection inputs at 4097/8192/16384
rows with complete legacy-GPU and CPU-corner checks. The small end-to-end
smoke completes sixteen exact pages without pressure.

R64 large-model qualification reaches 0.8719 pages/s at C48/P3/V1/VF2,
0.7449 at C48/P1/V4/VF1, 0.9452 at C48/P3/V4/VF1, and 0.9423 at
C36/P1/V4/VF2. Every successful run retains exact page parity and has no
admission pressure. C48/P1/V4/VF2 hits the backend budget, fails, and is
excluded without a throughput result. The verified audit preserves that
failed attempt alongside both original and continuation runs. The combined
C48 profile is fixed before the independent paired29 R128 comparison.

The Metal-disabled check exposes missing fallback methods for encoder scopes,
projector cleanup and a pure prepared-config predicate. Engine57 adds only
those declarations: the complete Metal-enabled implementation prefix and
all other runtime source files remain byte-identical to engine56. Metal
integration again passes 237 tests with one skip. With Metal disabled, 184
pass and 21 skip. All sixteen C suites are reused only after checking every
C/header/checker input hash; license and Apache boundary checks also pass.

Paired29 does **not** meet the 4B throughput goal. With 128 requests, native
reaches 0.8802 versus fresh vLLM Metal 0.9499 pages/s; the reverse order
reaches 0.8592 versus 0.9777. Both comparisons retain complete fixture string
identity, all native pages match serial references, and neither native run
has admission pressure. The source profile was selected before these runs.

Further engine57 R64 screens retain exact outputs and the same caps:

| Concurrency / prefill / vision / frame blocks | Late vision / prefill seal | Pages/s |
| --- | --- | ---: |
| 56 / 4 / 1 / 2 | off / off | 0.8926 |
| 48 / 4 / 1 / 2 | off / off | 0.8472 |
| 56 / 4 / 1 / 2 | off / on | 0.6786 |
| 48 / 3 / 4 / 1 | on / on | 0.7696 |
| 36 / 3 / 4 / 2 | on / on | 0.8069 |

These screens do not establish a vLLM win. Keeping prefill groups mergeable
improves batch fill but regresses throughput on its own. Engine59 tests an
independent queued-phase priority switch and a uniform prefill ceiling of
eight pages. The complete group scratch reservation scales with that ceiling;
vision remains bounded at four. Queued prefill priority applies only when
the scheduler requires no decode progress before prefill. Engine58 was
stopped before qualification after source review caught an incorrect ceiling
replacement; its frozen source and failed-build record are retained.

Engine59 passes 238 Metal tests with one skip and 185 Metal-disabled tests
with 21 skips. All sixteen C suites remain applicable after exact input-hash
checks. Its C64/P1/V1/VF2 R64 control reaches 0.8820 pages/s, with exact
outputs and no admission pressure. Matching four-page vision/prefill groups
with queued-phase priority reaches only 0.6473; C36/P4/V4/VF2 hits the
backend admission budget and is excluded. Engine60 changes only the Python
argument bound to permit the already-tested eight-item runtime ceiling and
reuses the exact qualified binary. C56/P6/V1/VF2 hits admission pressure;
C48/P6/V1/VF2 with late prefill seal and queued priority completes exactly
at 0.5632 pages/s. These are regressions, not recommended profiles.

The planned eight-page case was stopped before execution because its combined
prompt rows could exceed the qualified 4096-row input-pack reuse scope.
Engine61 caps each uniform group by both eight pages and 4096 prompt rows:
the fixture lengths 432/554/662 permit at most 8/7/6 pages. The pipeline also
rejects a multi-page group that bypasses this row limit, before allocating
its combined positions or device tensors. Admission retains the complete
configured maximum group reservation. This leaves all C/shader code unchanged.
The fixed C64/P1 candidate is retained for fresh R128 controls independently
of the one-window R64 ranking, which cannot measure its refill advantage.

Engine61's paired30 forward comparisons remain losses: C64/P1/V1/VF2
reaches 0.8054 versus fresh vLLM 0.8189 pages/s, and C56/P1/V1/VF4 reaches
0.8017. The already-running reverse C56 native leg completes at 0.8374;
there is no matching reverse reference. The verified partial audit preserves
that diagnostic result and records the remaining unrun legs.

Engine63 adds an optional closed resident vision contract. Supported Qwen
vision shapes must complete normalization, projection, activation, residual,
slicing, and attention on the qualified GPU paths. Device projections must
use the existing MPP implementation; an unavailable pipeline or unsupported
shape fails before a fallback. Only this closed path admits the conservative
linear attention workspace instead of a quadratic score matrix. Its bound
retains independent page histories, outputs, weights, CPU preparation,
frame lifetimes, and both packing banks. Default admission is unchanged.
Engine62 was stopped before qualification when review found its device
fallback closure incomplete.

Engine63 passes all seventeen C suites, 241 Metal tests with one skip, and
187 Metal-disabled tests with 22 skips. The original checkpoint's vision
weights are BF16; no shader arithmetic or checkpoint tensors change.
C64 and C60/P4/V4/VF1, C56/P4/V4/VF2, and C60/P4/V4/VF1 with queued
priority hit the unchanged backend cap and are excluded. The admitted
C56 and C52/P4/V4/VF1 R64 screens reach only 0.3112 and 0.5647 pages/s
on the shared machine, with all outputs exact and no admission pressure.
These screens establish no speedup; no paired31 reference ran for engine63.
A prior model-load failure at the live OS reserve is preserved separately.

Engine64 experiments with optional two-step GPU token handoff. It submits a
complete greedy step, encodes the next step while that producer runs, and
embeds the GPU argmax directly from the original resident BF16 embedding
weight. It reads the completed first result before submitting the second
argmax producer. Both steps drain before publishing either result. The
coordinator retains page borrows throughout; EOS discards extra output,
cancellation drains submitted work, and publication advances KV accounting
once per accepted token. Every page must have two output slots remaining.
The existing two frame banks and an additional bounded auxiliary envelope
remain charged under the original caps. Decoder and recurrent precision stay
unchanged. The flag remains opt-in; it is not a default throughput claim.

All eighteen engine64 C suites pass, including GPU-produced token handoff,
ordered two-frame readback, exact BF16 embeddings, canaries, unavailable
pipeline/dtype/shape rejection, and unsubmitted cancellation for both model
widths at 2, 16, and 64 rows. Integration passes 242 Metal tests with one skip
and 188 Metal-disabled tests with 22 skips. The qualified binary and source
fingerprints are recorded in `engine64_build_manifest.json`.

The engine64 C64/P1/V1/VF3 R128 native profile reaches 0.9984 pages/s,
with all 128 full outputs identical to the serial reference and no admission
pressure. Its two-step pipeline runs 142 bursts. A V2/VF1 late-vision-seal
alternative reaches only 0.7485. The prior VF4 R128 run reaches 0.9538 but
encounters transient admission pressure and is excluded from primary results.
None of these profiles alone establishes a fresh repeated vLLM win.

Reference31 completes at 0.5886 pages/s with only 206 automatic cache blocks;
it is a constrained control. Reference32's automatic 825-block allocation
exceeds available memory and is stopped without a completed rate. Reference33
requests 320 blocks but the automatic probe permits only 36; the benchmark
rejects it before timed requests. The first direct in-process 4B vLLM attempt
likewise rejects 320 blocks against an automatic capacity of 156. These runs
are not counted as wins or reference performance measurements.

`benchmark_vllm_offline.py` adds an actual vLLM V1 InprocClient control with
the original BF16 weights, image-only prompts, thinking off, unique media
UUIDs, and prefix/processor caches disabled. Its timing includes chat rendering,
image preprocessing, queueing, inference, detokenization, and a final GPU drain.
It checks all concurrent outputs against fresh serial outputs and requires
explicit cache capacity to fit every automatic capacity probe. The API server
control remains necessary: the offline chat path can serialize preprocessing
that the server overlaps across requests. Use the faster qualified control
when assessing parity.

Engine64's fresh paired35 0.8B API comparisons establish a repeated win:
native reaches 4.0209 versus vLLM 3.3746 pages/s, then 3.9235 versus 3.2491
in reverse order (19.2% and 20.8% leads). The fixed native profile is C64,
VF4, prefill1, F32 decode, shared recurrent scratch, and a 16-layer GPU greedy
stream prefix; it does not enable the two-complete-step token burst. Both
128-page native runs have full serial and cross-runtime string identity and
zero admission pressure. Both reference processes use automatic cache sizing;
their capacity supports the configured 64 concurrent requests.

Paired34 separately compares the actual offline InprocClient: native 2.7254
versus 2.0643 pages/s, then 2.8298 versus 2.5509 in reverse order. All full
outputs match, explicit 320-block caches pass admission, and native has no
admission pressure. The different absolute rates demonstrate substantial
shared-machine drift; compare each pair rather than mixing historical rates.
The faster API results above remain the primary 0.8B control.

The separate 0.8B two-step GPU token profile (profile36) completes R128 at
4.2056 pages/s with 143 bursts, exact outputs, and no admission pressure.
This is an unpaired profile; it does not replace the paired35 serving recipe
or establish a causal improvement over layer streaming.

Florence's fresh paired34 batch12/C24 comparison is mixed: native 4.1247
versus MLX-VLM 4.3723 pages/s, then 4.2732 versus 3.9143. A batch14/C28
attempt is rejected by the unchanged resource limits and has no eligible rate.
Profile37 qualifies batch10/C20 with CPU preparation overlap at 4.9891 pages/s.
The subsequent fresh paired38 comparison gives a small repeated proxy lead:
native 4.8168 versus 4.6613, then 4.8171 versus 4.6885 (3.3% and 2.7%).
All 128 native outputs equal the native serial strings; cross-runtime
comparison is whitespace-normalized content, not full string identity.
This remains an MLX-VLM proxy comparison, not actual vLLM Florence support.

The full-capacity 4B offline paired39 reference passes its cache guard:
320 requested blocks versus 825 automatic blocks, 5.16 GiB backing, and
reported concurrency 64 at context 1024. Its load probe records no compression
or swap-out. It completes all 128 pages exactly at 0.92265 pages/s. The
matching engine64 native leg reaches only 0.8614 with exact outputs and no
admission pressure, a 6.6% loss. The predeclared forward-loss stop leaves the
reverse and proposed API follow-up unrun. This result supersedes any inference
of parity from the earlier unpaired 0.9984 native profile.

Engine65 extends the opt-in token pipeline to a maximum of two, three, or
four complete steps (`--gpu-token-burst-width` in the benchmark and
`ANTFLY_LIGHTONOCR3_GPU_TOKEN_BURST_WIDTH` in serving). The default width is
two. Every cohort is bounded by each page's remaining output and token-buffer
capacity. It retains one submitted and one active frame, reuses the same two
logits allocations, and drains producers before publication or page release.
The incremental head writes use the existing dense projection dispatcher.
Widths above two reserve an extra 4 MiB of scratch under the original caps.
Telemetry separately counts groups, extra GPU forwards, extra rows, and the
maximum width so multiple forwards are not reported as one GPU step.
All nineteen C suites pass, followed by 242 Metal tests with one skip and
188 Metal-disabled tests with 22 skips. The Metal integration exercises actual
four-step bursts and validates the extra-forward/topology counters. Its first
C64/P1/V1/VF3 R128 profile completes at 0.9691 pages/s with exact full outputs
and no admission pressure. This unpaired profile is not a vLLM win; the subsequent controls below
determine whether it beats the reference.

Private BF16 prefill FFN fusion probes preserve the existing prefill precision
and pass full-output, CPU-corner, and canary checks. The best full-K variants
reduce isolated 4B GEMM-plus-activation GPU time by roughly 3–6%; larger tiles
offer little improvement, and explicitly sharing input through threadgroup
memory regresses performance. These shaders are not in the production runtime
and establish no end-to-end OCR improvement.

Engine65's paired42 offline control requests 320 cache blocks against an
automatic capacity of 143 and is rejected before timed requests. Paired43
then uses actual API controls at C32 and C64 with automatic cache sizing,
context 1024, and GPU memory utilization 0.65. They complete at 0.89727 and
0.89831 pages/s (184 and 189 blocks; reported supported concurrency 36.8
and 37.8). Native completes at 0.8182 pages/s with exact outputs and no
admission pressure, an 8.9% loss against the faster control. The predeclared
forward-loss stop leaves the reverse order unrun. Profile44's C16, C32,
and C48 screens complete exactly without pressure at 0.7165, 0.7726, and
0.8200 pages/s. These results do not establish a 4B win.

Engine66 adds opt-in streamed vision (`--pipeline-vision`, serving environment
`ANTFLY_LIGHTONOCR3_PIPELINE_VISION=1`). One submitted block group runs while
the CPU encodes the next group. Both complete host and backend working sets
remain charged under the original caps. It requires the closed resident
vision path, one image at a time, and one- or two-block groups. Cancellation
releases active work and drains submitted producers before releasing tensors.
Defaults and model arithmetic are unchanged. The new ownership tests cover
success, cancellation, submit/wait failure, unavailable frame support, and
the existing synchronous path. Qualification passes 243 Metal tests with
one skip and 189 Metal-disabled tests with 22 skips; the 19 C suites are
reused only after exact input and tested-binary hash verification.

Engine66 profile46 completes C64/P1/V1/VF1 with four-step decode at 0.9551
pages/s. All 128 full outputs match serial and saved cross-runtime strings,
with zero admission pressure. It records 3,072 vision submits and waits and
2,944 CPU/GPU encoding overlaps. The two-block alternative encounters
admission pressure and is excluded. Profile46 is unpaired. Fresh paired47 API controls complete at 0.94093
(C32) and 0.86453 (C64) pages/s. The fixed native profile reaches 0.7115
with exact outputs and no pressure, a 24.4% loss against the faster control.
The predeclared forward-loss stop leaves reverse comparisons unrun, and the
conditional smaller-model follow-up is skipped. This build has no 4B win.
Its timed process CPU usage rises from 35.0 s in the profile to 59.0 s in the
primary run, while recorded GPU work rises from 109.9 s to 133.2 s; these
shared-machine measurements cannot establish a causal vision-pipeline gain.

Engine67 introduces opt-in prompt layer streaming after one full synchronous
prompt. One submitted and one active group retain the prefill regime; decode
stream counters stay separate. It passes 244 Metal tests with one skip and
190 Metal-disabled tests with 22 skips. Source review then finds that the
existing BF16 projection packer declines reuse whenever a prefix is submitted.
That can retain fresh packs across every layer in a group, so the existing
two-working-set admission envelope alone is insufficient evidence for this
variant. Its queued R128 screens are stopped before any child benchmark starts;
`engine67_prefill_stream_primary_eligibility.json` excludes it before timing.

Engine68 adds a narrow runtime permission for active-frame packing reuse during
admitted prompt streaming. A submitted prefix drains its own pool into retained
resources; the active group starts with independent storage and recycles only
after each serial projection's sole consumer. Default pending-prefix callers
still decline reuse. The permission cannot change while either frame exists,
and streaming declines explicit pack-reuse rollback or concurrent planned
encoding. C checks add a real packed projection in the submitted prefix,
bitwise complete-output and canary checks for both groups, and cancellation of
an encoded but unsubmitted successor. New C, Metal and CPU qualification is
required before R128 profiling; no engine68 throughput claim is made yet.

Engine68 completes all nineteen C suites, 244 Metal tests with one skip, and
190 Metal-disabled tests with 22 skips. Profile49 completes all 128 outputs
exactly without admission pressure for each candidate: eight-layer prompt
streaming with synchronous VF3 vision reaches 0.8714 pages/s; four-layer
streaming reaches 0.7269; eight-layer prompt plus VF1 vision streaming reaches
0.7412. The first two record essentially identical GPU time (122.10 and
122.42 s), but wall time differs (146.89 and 176.08 s); this screen does not
isolate shared-machine host-load changes from submission overhead.

Review then finds two packed-core reporting sites that use the one-based row
count as a u64 bit index. A 64-row cohort requires bit 63. The waiting paired50
controller is stopped child-free before any native or reference leg runs;
`engine68_primary_eligibility_after_row_mask_review.json` records the hold.
The completed screens remain diagnostic data, not a promoted primary claim.

Engine69 maps rows 1–64 to bits 0–63 and safely declines invalid dimensions.
A regression test covers all 64 disjoint bits, the complete u64 mask, and
zero/65/maximum-usize inputs. The exact production helper and test fragments
also pass with Debug safety checks enabled. The public prompt-counter parser
is independently checked against the three real R128 logs: 381, 889 and 381
splits, with their observed group sizes. C runtime/shader/checker inputs and
the previously tested C binary are unchanged from engine68. Profile51 will
compare coarser sixteen-layer groups and a same-build nonstreamed control
before fresh paired52 API comparisons; no new speed claim is made here.


## Engine69 completed comparisons and CPU diagnostic

All engine69 qualifications finished before its timing runs. Profile51 compares
16-layer prompt groups (0.6930 pages/s) with nonstreamed prefill (0.7751 pages/s),
both at C64/P1/V1/VF3 with four-step decode. Both preserve all 128 outputs without
admission pressure; this unpaired screen selects nonstreamed prefill.

Paired52 runs actual vLLM API controls at C32 and C64, native twice, then reverses
the reference order. The faster fresh control is selected separately on each
side: native 0.8839 versus C32 0.8336099612, then native 0.8882 versus C64
0.8884795554 pages/s. The first leg leads by 6.03%; the second is 0.0315% behind.
This is a mixed result, not a repeated win. The default-memory comparison will
search C16/C32/C64 and preserve each server's observed cache capacity.

Diagnostic53 is a separate sampled R64 run, explicitly excluded from primary
throughput evidence. All 64 outputs pass. Three macOS samples target only its
own native process. The late sample's GPU worker predominantly waits for GPU
completion, with smaller command-encoding and publication portions. Blocked
page threads do not represent CPU consumption. The stripped binary's nearest
C export names do not identify Zig functions or imply video-codec execution.
Visible environment lookups in recurrent, norm and gate/up selection motivated
the engine70 cache extension; the samples do not establish its speedup.
Completed qualification, sample and controller logs are losslessly archived.
