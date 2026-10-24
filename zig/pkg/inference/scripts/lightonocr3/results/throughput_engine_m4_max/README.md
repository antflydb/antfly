# OCR execution engine experiments on M4 Max

Work in progress following `3e21b5a5636ef8a45328a2b1e805108277c7b304`.
These measurements do not yet establish sustained parity with vLLM.

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
enable the option. It remains disabled in the preset. Fresh paired controls now
run one automatic-capacity vLLM server per concurrency/model, followed immediately
by the ordinary native executor at C16/R128.


Completed raw text logs are stored as lossless `.txt.gz` files.
`raw_logs_manifest.json` maps each original report filename to its compressed
artifact and records both hashes. Decompression restores the exact original bytes,
including server carriage returns and whitespace; historical report hashes are
unchanged. Qualification/helper archives have their own manifests.
