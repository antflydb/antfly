# GLiNER2.5 native training implementation contract

GLiNER2.5 training is a separate qualification milestone from inference. The
existing GLiNER2 span-grid objective and cached top-layer boundary trainer do not
train the GLiNER2.5 boundary architecture. Native CPU encoder/head gradients,
PEFT LoRA/DoRA math, optimizer accumulation and durable state now have focused
reference evidence. A tiny complete mixed-task CPU training step also passed
all four profiles with both zero dropout and source-matched explicit dropout
masks, including durable mid-window restore. The native job/CLI/export path is
implemented, with a real-small head-only integration probe, strict upstream
reload, and a fresh supervised CPU CLI pause/resume with complete redirected
stdout and exact final state/model agreement. All eight tiny Metal mixed-step profiles now match the pinned
losses and gradients at unchanged tolerances. Separate resident optimizer and
managed tiny full/head-only jobs prove updates and exact durable resume.
Published-small CPU LoRA/DoRA jobs also pass two updates and exact partial
resume for all 59 task-head Linear targets and, separately, all 131 encoder
plus task-head Linear targets. Their standard adapters load with
a separately pinned official PEFT 0.18.0 export profile, preserving every
loaded parameter byte. The two all-131-target materialized FP32 artifacts also
pass ten-task Zig CPU and Metal output/token comparisons against all three
Python forms. This qualifies their artifact execution, not general training
update behavior. The separate inactive-adapter correction now passes all eight
tiny source profiles through NativeTrainer on CPU and Metal, including actual
gradients, optimizer updates and durable resume. Separate production CPU and
resident Metal CLI jobs now pass the five-row active/inactive classifier
sequence for published small LoRA and DoRA, with three updates and
byte-identical fresh-owner resume within each backend. All four final
classifier-only adapters also pass exact 334-base/four-or-six-adapter tensor
loading through pinned Fastino plus isolated PEFT 0.18.0 on CPU, then ten
fixed requests each with complete process and private-copy cleanup. Published-source
numerical VJPs, other target/backbone/rank jobs, convergence and release
qualification remain open.

The reference is Fastino commit
`3c913c7369301133d3b7699252074c4303ada50e`, with the Python, Torch, PEFT, tokenizer,
and model-file identities recorded alongside each fixture or training run.
Training accepts floating-point model weights. Quantized training is outside
this milestone; quantizing an exported trained checkpoint requires a separate
inference qualification pass.

Jobs default to `attention_profile: "materialized_v1"` and
`activation_profile: "retained_v1"`. The independent opt-ins are
`"replay_tiled_v1"` for attention and `"layer_recompute_v1"` for regional
encoder activation replay. Both enter durable run identity and retain the
job's bounded host, backend and combined admission. The
[regional training guide](GLINER25_RECOMPUTED_TRAINING.md) describes the
additional scratch, tape and optimizer reservations. Its v3 CPU checkpoint
passes eight tiny regional source profiles and four retained controlled-dropout
regressions. The later Metal-enabled checkpoint passes 43 selected tests,
including the eight regional Step profiles on actual Metal, full/head-only
NativeTrainer resume on both backends, and all eight inactive-adapter source
profiles per backend with native-owned updates and durable resume.
No published checkpoint has run the regional activation profile;
the published results above use retained activations.

## Proved foundations

These results use exact pinned source or library fixtures and bounded inputs;
the real-small job uses four authored examples. Fixture identities are recorded
in [the oracle contract](GLINER25_ORACLE.md).

| Component | Completed evidence | Remaining composition |
| --- | --- | --- |
| Losses, targets and detached decisions | Five loss tests, seven target tests, six selection tests and six matching tests passed, including 10 target, 42 selection and 18 matching oracle cases | Representative supervision/capacity qualification and published-model CPU/Metal composition |
| DeBERTa encoder graph | Two tiny shapes, including padding and longer relative buckets, passed train/eval forward and all 38 parameter VJPs in training mode; repeated word/query/classification/relation routing and four structural/admission/replay/OOM checks passed | Evaluation-mode VJPs and real-checkpoint encoder/task composition |
| Boundary and task graphs | All 11 graph tests passed: stage-one 9 outputs/46 parameter/3 input VJPs; shared pool 14/72/3; explicit spans 2/28/6; relations 6/12/2; dense records 18 parameter/3 input VJPs in all three modes | Published-model training/inference across all profiles and backends |
| Dense record loss | All four tests and 10 actual batched loss/logit-gradient cases passed, including global denominators, masked columns and alternative occurrence mass | Representative record supervision/quality and published-model composition |
| Retained staged execution | Three tests passed, including live relation selection after shared-pool scoring and allocation-failure cleanup; the tiny complete Metal step also passes | Published-model memory, cancellation and admission behavior |
| LoRA/DoRA graph | Three tests passed, including 8 PEFT 0.17.1 train/eval/dropout cases, shared projection calls, and every adapter/magnitude/input VJP; tiny complete Metal losses/VJPs pass separately | Other ranks/target sets/variants and published-model Metal updates |
| AdamW and checkpoint state | The CPU Torch fixture passed 3 parameters, 3 microbatches and 2 flushes, including clipping, partial-window correction and absent/zero gradients; mid-window resume, exact large counters and ordered epoch resume checks passed | Published-model/profile job lifecycle and recovery |
| Resident Metal optimizer owner | Two actual GPU Controller tests passed unchanged CPU fixture tolerances for weights, both moments, counters and norms after both flushes; uninterrupted and fresh-owner resumed states match exactly. Wrong pins, stale identities, cancellation, busy bindings and failed partial host-state copy preserve the authoritative epoch | Published-model admission/update/recovery and managed adapter jobs |
| Adapter artifacts | Three tests passed, including six variant/mode roundtrips, synthetic mathematical merge equality, identity/tamper/cancellation/OOM checks. The two small all-131-target materialized FP32 adapters pass ten-task Zig CPU/Metal execution against all three Python forms | Other artifacts/profiles and quality |
| Complete tiny CPU mixed step | All four profiles passed with zero dropout and explicit masks at dropout 0.125: exact preprocessing/mention targets, every component loss, every trainable gradient's absence/zero/value, durable fresh-controller mid-window restore and two source AdamW flushes on native updated weights | Published-model/profile jobs and convergence |
| Tiny inactive adapter CPU/Metal jobs | All eight tiny LoRA/DoRA source profiles passed through NativeTrainer on both CPU and actual Metal: exact tokens and gradient absence/zero, fallback objective, component losses and VJPs, native AdamW weights/moments/counters, and exact fresh-owner durable resume. The test-only fixture binds captured initial A/B/magnitude values before microbatch zero; every subsequent update is native | Other ranks/dropout settings and resource/quality qualification |
| Published-small inactive classifier CPU CLI | Six production invocations passed: uninterrupted, pause-after-one and fresh resume for rank-2 LoRA/DoRA. The five ordered active/inactive rows produce three updates, exact fallback reports, independently reconstructed checkpoint/state hashes and byte-identical final results/checkpoints/all four export files | Published-source numerical VJP/quality comparison, other target families/backbones and long context |
| Published-small inactive classifier Metal CLI | Six separate production invocations passed on strict resident Metal using the same frozen executable and authored recipe. All five microbatches/three updates, inactive zero-loss policy, raw terms and all-slot counters pass; fresh-owner resume matches uninterrupted progress semantics and final result/checkpoint/all four export-file bytes exactly. Source/binary pins, Controller/state hash reconstruction, complete resident admission and process cleanup pass | Published-source and CPU–Metal numerical gradients, other target families/backbones/ranks, long context, quality and performance |
| Complete tiny Metal mixed step | Eight GPU tests passed: full/heads/LoRA/DoRA with zero and controlled dropout, all component losses and gradient absence/zero/value against pinned source and native references; exact semantic relation membership and backend-local controlled-dropout replay at unchanged numeric tolerances | These consumers bind captured initial/post-update weights; they do not prove GPU optimizer updates or resume by themselves |
| Managed tiny Metal jobs | The five-row full/head-only regression passed six microbatches and four resident optimizer updates per mode, exact fresh-owner partial resume, shuffled batch order, classifier `None`, busy/cancel/OOM retry, scalar optimizer receipts and explicit host synchronization. A separate restored epoch-end flush admission/retry test passed before mutation | Published-model GPU jobs and GPU CLI, managed LoRA/DoRA updates and production resource/quality qualification |
| Published source admission | All three original FP32 checkpoints passed exact five-file identities, complete 334-tensor inventory, native tokenizer loading and bounded immutable source ownership | Other model/profile job and trained-artifact qualification |
| Real-small head-only job and export | Four microbatches/two updates, pause-after-one/resume and exact state/model equality passed. A persistent CLI run produced the same hashes; pinned Fastino strictly loaded all 334 export tensors and executed ten bounded task requests. A fresh supervised CPU CLI then repeated the paused/resumed job with identical final hashes and all six redirected stdout events preserved | Trained native/Python output parity, full/other-model jobs and held-out quality |
| Real-small adapters | Separate 59-task-head and 131-encoder-plus-head CPU LoRA/DoRA jobs each completed four microbatches/two updates over two training rows. Fresh resume after microbatch one produced byte-identical final results, checkpoints and exports. Pinned Fastino plus isolated official PEFT 0.18.0 verified all 334 base tensors and all 118/177 or 262/393 adapter tensors, then executed ten requests per export. The all-131-target materialized artifacts additionally pass Zig CPU/Metal token/output parity | Broader inactive-adapter cases, task-head-only trained execution comparisons, other ranks/targets/variants, published GPU jobs and quality; PEFT 0.17.1's loader is incompatible with the `inside_weight` target |
| Training command supervision | Six pure unit tests and eight process tests passed alongside the four existing server process tests. Single-worker ownership, exact argv/config binding, safe signal pauses, hard deadlines, parent-loss cleanup and immediate exit past blocking C shutdown handlers are covered. Three focused CLI tests passed for streaming output, config fingerprinting and allocation-failure cleanup | Actual-model signal/deadline recovery beyond the proved planned CPU pause; published-model GPU command qualification |

Six actual Metal primitive tests also passed without skips: physical i32
values around `2^24` in a 64 MiB device buffer, repeated and negative
gather/scatter indices, independently retained interpreter captures after source
overwrite, no hidden activation downloads, and strict admission/allocation
checks. These establish device primitives; the complete-step and managed-job
evidence below is independently scoped.

The expanded resident primitive suite subsequently passed all 13 tests on
Metal. Three compiled Metal oracle tests then passed without skips: both
encoder fixtures in train/eval forward, all 38 training parameter VJPs,
repeated typed routing cotangents, and all eight pinned LoRA/DoRA cases with
unchanged tolerances. The compiled programs do not read back activations before
the numerical comparisons. The opt-in resident Session/staged integration then
passed five actual GPU tests covering direct, two-stage and three-stage leased
inputs, finite cotangents, stale-state/cancellation recovery and runtime-array
allocation failures. The complete mixed-task Metal consumers subsequently
passed all eight profile/dropout combinations. They bind captured initial or
post-update parameter inputs for each of three microbatches, so their direct
proof is forward/objective/gradient composition; GPU optimizer updates and
resume are established by separate owner and managed-run tests.

The complete-step consumer now distinguishes exact selected relation
identities from cross-backend ranking order at nearly tied float32 products.
It validates every pair and label through a bijection, checks all padding,
and transports the captured relation hidden-dropout rows by pair identity
before replay. Exact backend-local ordered decisions remain required on
replay, and production ranking and retained input seals are unchanged. The
[oracle contract](../zig/pkg/inference/scripts/gliner25/TRAIN_STEP_DROPOUT_ORACLE.md)
documents the near-tie evidence and unchanged numeric tolerances. All eight
corrected GPU comparisons passed. Cross-backend pair membership and labels are
exact; bit-identical cross-backend ranking order is not claimed.

The separate resident optimizer Controller then passed two actual GPU tests
against the existing three-parameter AdamW fixture, without loosening the CPU
tolerances. Both flushes match weights, first/second moments, parameter/global
counters and clipping norms. Saving unfinished accumulation and restoring into
a fresh GPU owner preserves the exact canonical state digest and produces the
same final digest as uninterrupted execution. Submissions transfer scalar
finite/norm summaries without generic host mirrors. Host mirrors remain
uncertified after device updates and after cancellation during a partially
completed readback; a complete checkpoint/diagnostic retry certifies them.
Wrong run/state pins, stale update identity, cancellation and live binding
leases preserve device handles and counters, followed by numerical state
verification.

The earlier hardware checkpoint completed successfully with **15 selected,
15 passed, zero skips and zero leaks**. It includes all eight complete mixed
Metal profiles, both resident optimizer Controller tests, the managed tiny
full/head-only regression, restored epoch-end flush admission/retry, large
budget arithmetic, job admission rejection, and preservation of two independent
cancellation controls. The managed run keeps frozen tensors resident, consumes
GPU VJPs directly, and checks scalar-only optimizer receipts. Host mirrors are
synchronized explicitly for checkpoint/final state comparisons, never as a
per-step side effect. Its full/head-only modes each use five authored rows over
two epochs, six microbatches, two epoch-end partial flushes and four updates.
The restored-flush admission regression separately uses one synthetic 2 MiB
parameter; it does not execute a published encoder.

| Checkpoint | Exact result | Log SHA-256 |
| --- | --- | --- |
| `/private/tmp/gliner25-managed-metal-integration-v3.log` | Process exit 0; 15/15 selected passed, no skips or leaks | `aa17d7b1439b18324f5caafb5aaf5e2c40492f6b8395aad153d6e94c5fcb39b2` |
| `/private/tmp/gliner25-managed-metal-integration-v2.log` | First 30 tests passed, then the managed Metal test aborted on budget-arithmetic overflow; this process was not a suite pass | `710212ea307f40e6bb5a94eb949dd804a4452dd29a1eef4d7760b02cab2c44a2` |

The earlier 30 successes cover eight optimizer transaction tests, six backend
ownership tests, six native Controller regressions, two snapshot tests, five
device-state tests, two GPU Controller tests and one native managed-run test.
The overflow was corrected with shared full-width `usize` budget arithmetic;
the successful v3 checkpoint includes its regression and the previously blocked
managed GPU run. These are separate checkpoint results, not a claimed aggregate
45-test pass. Earlier observer/fixture-path failures are superseded by the
specific successful tests recorded here.

Published-model GPU training beyond the separate classifier-only small jobs
below, other-variant/rank jobs, and trained-artifact comparisons beyond the two
small all-target merged files remain unqualified. No convergence,
throughput, peak-RSS benchmark or production training qualification follows
from these tiny fixtures. The separate supervised published-small CPU heads
pause/resume is recorded in [the job evidence](GLINER25_TRAINING_JOB.md): binary
SHA-256 `73695721460619998009dc6ea10e8cd436616c36897e1732d99cab57009d1c0f`,
four total microbatches, two optimizer updates, exact earlier final state/model
hashes, and all two paused plus four resumed stdout events. This used explicit
256 MiB trainer-host and 128 MiB backend limits after the previous profile was
denied by unchanged live-memory admission. The measured resumed allocator
peaks were 155,626,435 and 63,338,112 bytes, respectively; these are not RSS.

Separate published-small CPU task-head LoRA and DoRA jobs now completed under
the same explicit 256 MiB host/128 MiB backend ceilings. Both use rank 2,
alpha 4, adapter dropout zero, 59 task-head Linear modules, two training and
two disjoint validation rows, and four microbatches/two updates. Every training
row retained all six gold mentions and its one gold relation. Validation was
preflight only. Each job paused after one microbatch and resumed into a new
directory with identical final result, checkpoint and all exported file bytes;
the redirected uninterrupted/pause/resume streams contain 5/2/4 events.
Observed host/backend allocator peaks were 36,273,111/50,741,560 bytes for
LoRA and 38,373,962/60,666,312 bytes for DoRA. These are bounded integration
measurements, not RSS, performance results or encoder-adapter memory estimates.

Strict upstream checks independently loaded all 334 frozen source tensors
plus every one of the 118 LoRA or 177 DoRA tensors byte-exactly and ran the
ten fixed extraction requests. The original PEFT 0.17.1 loader failed because
its global adapter-name replacement corrupts the `inside_weight` module path.
The official PEFT 0.18.0 export profile fixes this through its released loader,
with exact wheel/file pins and a private import overlay. The training oracle
remains pinned to PEFT 0.17.1. No target is omitted or renamed, and the original
failure is retained. [Export evidence and commands](GLINER25_TRAINING_EXPORT.md)
record both runtime profiles, job/resume digests and the unchanged numerical
scope. Successful request execution does not imply useful predictions: the
anchorless example and relation examples can have empty outputs.

The public CPU training route also completed separate small-model LoRA/DoRA
jobs for all 72 encoder and 59 task-head Linear modules. Both retained the
same rank-2/alpha-4/zero-dropout settings, four microbatches and two updates,
and passed exact final-result/checkpoint/export comparison after a fresh
pause-after-one resume. Their 262/393-tensor exports then passed the same
strict PEFT 0.18.0 load over all 334 source tensors and ten bounded requests.
LoRA used explicit 128 MiB host/384 MiB backend ceilings and reached
44,640,688/282,550,704 measured owner bytes. DoRA used 128/512 MiB and reached
50,930,139/484,999,240 bytes. These limits cover the named owners; source,
optimizer and other owners have separate admitted reservations. Smaller
backend profiles returned resource denials before completing a step. The
[all-target export evidence](GLINER25_TRAINING_EXPORT.md#complete-encoder-plus-head-jobs)
records the executable, every artifact/receipt digest and failed-profile
scope. Both all-target adapters subsequently passed native materialization and
[the fixed-tolerance three-form merge checker](../zig/pkg/inference/scripts/gliner25/TRAINING_MERGE_CHECK.md): all 334 tensors, zero adapted violations, exact untouched/bias/sidecar
bytes, and ten matching requests across unmerged PEFT, official PEFT merge
and the Python-loaded native-produced artifact. A subsequent separate
[trained-execution checker](../zig/pkg/inference/scripts/gliner25/TRAINED_EXECUTION_CHECK.md)
passed Zig CPU and Metal for both all-target merged artifacts: 40 executed
requests, 120 comparisons against the three Python forms and 20 CPU–Metal
comparisons. Token IDs, selected outputs and source coordinates match exactly
with the unchanged `5e-4` confidence bound. The independently rederived audit
is `/private/tmp/antfly-gliner25-materialization-v1/trained-execution-validation-v1.json`,
SHA-256 `ea3c2fb430d10d68fd5a0db0e5449ebae76008600698868147508db4f4e826eb`.
The earlier `validation.json` remains the historical Python-only checkpoint.
This inference proof does not establish update parity for arbitrary training
batches. Exact receipts and resource observations are in
[the merge evidence](GLINER25_MERGE.md#actual-small-all-target-materialization).

NativeTrainer now preserves the pinned inactive-adapter behavior for
heterogeneous data and narrow adapter targets. A loss without any trainable
path is replaced by a zero term touching every selected parameter:
classifier-only adapters on an entity-only batch therefore report zero loss
and produce present zero gradients. Those rows still count in accumulation;
AdamW applies weight/moment decay and advances slot/scheduler steps at a flush.
Absent record/relation heads are already zero-touched by the model, so their
reported frozen-task loss is retained. An absent classifier stays `None` when
another selected path is live. Raw component terms remain available as model
diagnostics; the submitted optimizer objective records the zero-loss fallback.
The run fingerprint includes this semantic version so older checkpoints cannot
silently resume under the changed rule. Cancellation, invalid inputs and
allocation failures remain errors and cannot trigger the fallback.

The separate [source fixtures](../zig/pkg/inference/scripts/gliner25/TRAINING_INACTIVE_ADAPTERS.md)
cover LoRA and DoRA with classifier-only, record-only, relation-only and
encoder-plus-classifier targets. The immutable control fixture flushes its
classifier sequence at `[2,3,5]`, including an all-inactive two-row window after
moments have been learned. A companion captures the ordinary five-row native
epoch at `[2,4,5]`, with windows `[2,2,1]` and a final inactive partial flush.
Both have byte-identical source repeats; their schedules are not interchangeable.
The original control has a separate passing CPU optimizer consumer using
captured gradients. That test proves optimizer handling, not native gradient
generation.

The companion now passes all eight profiles through the actual CPU and Metal
NativeTrainer, from its versioned JSONL inputs through token preparation,
forward/backward, accumulation and AdamW. This uses the source H=16,
two-layer, vocabulary-192 baseline, rank 2, alpha 3 and dropout zero, with
at most 128 encoded tokens. A compile-time test-only constructor validates the
exact tiny base/adapter descriptors and copies captured initial A/B/magnitude
values before microbatch zero, with zero optimizer counters and moments.
Every later weight update is native; no source post-update values are injected.
Production construction retains the published inventory checks and exposes
no synthetic layout through options, JSON or environment variables.

The consumer compares all eight prepared token/routing tensors exactly,
gradient `None` versus present zero exactly, fallback flags/objectives,
component losses and every live VJP. It compares weights, both moments,
accumulated gradients and slot/global counters at every flush. Numerical
checks use the existing complete-step tolerances; zeros and presence are
exact. A fresh owner restores the durable checkpoint after the first
microbatch and produces the same canonical final state as uninterrupted
execution. A failing pre-update observer preserves the state and permits a
retry without advancing the optimizer or cursor.

The subsequent hardware checkpoint passed **17 selected tests, zero skips
and zero leaks**, including the actual source NativeTrainer consumers and
supplemental control consumers on both CPU and Metal, managed classifier-only
LoRA/DoRA regressions on both backends, existing managed full/head jobs,
forward-only gradient tapes and socket regressions. The source Metal consumer
uses the same numerical tolerances as CPU, takes explicitly bounded gradient
readbacks only for diagnostic comparisons, and performs the actual optimizer
updates on resident tensors. Host state synchronization occurs explicitly at
checkpoint/state comparisons. This is update/resume correctness evidence,
not a GPU throughput or transfer-performance claim.

| Checkpoint | Exact result | Log SHA-256 |
| --- | --- | --- |
| `/private/tmp/gliner25-source-socket-metal-v1.log` | Process exit 0; 17/17 selected passed, zero skips or leaks; all eight source profiles exercised by each actual CPU/Metal NativeTrainer consumer | `7960df3ce368de2937e397b819f4e5f239845287fa8b422ff7b3b8c3cbf8481b` |
| `/private/tmp/gliner25-source-socket-cpu-v3.log` | Earlier individual CPU NativeTrainer/control and synthetic-descriptor tests passed; an unrelated socket MIME assertion failed, so this was not an aggregate suite pass | Earlier diagnostic checkpoint; superseded for these selected tests by the hardware run above |

These synthetic CPU/Metal results do not qualify published-model inactive
batches, long-context training, convergence or throughput. Earlier completed
training recipes and trained-artifact execution receipts remain valid
historical evidence with their original scope.

The separate published-small classifier-only CPU recipe now passes through
the ordinary production constructor and public CLI. The ReleaseFast binary is
32,697,376 bytes, SHA-256
`9f0d349efa3f33dd29b6c3dd26a12a8bcf2babf2bc78400c2b84d87d58d786f9`;
its build receipt verifies all 2,753 recorded repository source files remained
unchanged across compilation. Each mode uses the actual five-file small
checkpoint/tokenizer, rank 2, alpha 3, adapter dropout zero and the published
encoder/head dropout of 0.1. No synthetic layout or source parameter injection
is used. The authored rows are active, inactive, active, inactive, inactive;
one epoch with accumulation two flushes at `[2,4,5]` and completes five
microbatches/three updates. Inactive optimizer losses are zero while positive
raw frozen-task terms remain available. All four LoRA or six DoRA slots reach
three Adam steps, including the final zero-gradient partial window.

Uninterrupted, paused and fresh-owner resumed invocations all exit cleanly.
Exact final result, checkpoint and all four exported file bytes agree after
resume. The checker independently reconstructs the Controller fingerprint
from the outer run digest, optimizer settings and ordered logical slot shapes,
then reconstructs the owned-state SHA256 from weights, both moments, pending
gradients, presence and counters. Source/config/binary identities remain exact.
The [compact evidence ledger](../zig/pkg/inference/testdata/gliner25/published_inactive_classifier_cpu_v1/manifest.json)
archives configuration, all six process/progress/result receipts, the build
inventory, both checker revisions and final tensor-file digests.

| Mode | Resume-validation SHA-256 | Largest host/backend owner bytes | Sampled child-tree RSS bytes |
| --- | --- | --- | --- |
| LoRA | `a0d46d07a1a1c8f55967c7193d5ec65c8117e523df5ac32edefa627c70336516` | 25,589,151 / 30,631,060 | 466,141,184 |
| DoRA | `81106db7aaf6eac4512d367ee29ea96e33c061173a8de253ffce7986d70348fe` | 25,620,123 / 30,659,864 | 466,305,024 |

The enforced trainer ceilings are 128 MiB each for host/backend and 1 GiB
combined, with source/job/dataset owners separately charged by admission. The
outer 2 GiB child-tree RSS guard samples every 50 ms using pinned psutil 7.1.3;
shared pages can be counted twice, and the measurement is not device residency.
The first paused CLI process already succeeded before its checker rejected
declared-f32 serialization. Its original failure and helper bytes are retained;
new offline evidence fixes only typed representations, the Controller hash
contract and flattened checkpoint shapes, without rerunning or rewriting the
job. Nineteen revised pure checker tests pass; the unchanged supervisor has
eight prior lifetime passes. These are CPU job continuity and policy checks,
separate from published-source numerical gradients, quality, throughput and
the independently executed Metal jobs below.

The same frozen executable now also passes six separate resident Metal CLI
invocations for this classifier-only recipe. Original pauses and both fresh
resume/uninterrupted pairs all exit zero with complete process ownership
cleanup. Their final results, checkpoints and four export files match exactly
within Metal; all five progress rows retain the expected fallback and raw-term
behavior, and all selected slots reach three updates. Both final reports and
the four continuation phase reports were independently reconstructed read-only
from the original files, including exact Controller and owned-state hashes.
The [Metal evidence ledger](../zig/pkg/inference/testdata/gliner25/published_inactive_classifier_metal_v1/manifest.json)
preserves 86 source/config/helper/build/process/output files and the final
tensor-file pins; it does not modify the earlier CPU ledger.

| Mode | Metal resume-validation SHA-256 | Host / backend-metadata peak bytes | Resident admitted upper bound bytes | Largest sampled child-tree RSS bytes across all three phases |
| --- | --- | --- | --- | --- |
| LoRA | `93e7e83ca915bbcff5f1a45b5437f8cb00d24e3b69a9f71a140598d658e4b018` | 27,969,855 / 1,265,664 | 775,275,748 | 581,730,304 |
| DoRA | `d723cb5b9be072e767885f9527cd3e631739c9905f642f149fd2e0d668fd7912` | 28,060,948 / 1,278,148 | 775,343,792 | 661,929,984 |

This separate explicit profile admits 128 MiB host, 1 GiB backend including
64 MiB metadata, and 2 GiB combined, with other owners separately charged and
the live physical-memory guard unchanged. The outer sampled process-tree RSS
ceiling is 3 GiB. Resident admission is a conservative allocation bound, not a
measured device peak; metadata/host allocator peaks and process RSS remain
separate. These jobs use the existing materialized training profile and do not
qualify replay-tiled attention or activation recomputation. They prove
same-backend policy and durable continuity, not published Fastino or CPU–Metal
numerical gradients, broader target families, long context or convergence.

The four classifier-only final exports subsequently passed the unchanged
upstream export checker on CPU, including the two Metal-trained artifacts.
Every original base tensor and all four/six adapter tensors loaded
byte-exactly, matched the final checkpoint and separately resumed exports,
and executed all ten fixed requests. Four bounded processes exited and were
reaped with their private source/adapter/wheel copies removed. A separate
[reload ledger](../zig/pkg/inference/testdata/gliner25/published_inactive_classifier_export_reload_v1/manifest.json)
archives the complete reports, portable adapter bytes, supervised cleanup and
unchanged input/helper pins. This is artifact interoperability; it adds no
published CPU–Metal update equality or trained-quality claim.

The [mixed-step oracle](../zig/pkg/inference/scripts/gliner25/TRAIN_STEP_ORACLE.md)
now captures the actual pinned source model in all four training profiles with
zero dropout, complete mixed-task supervision, three microbatches and two
AdamW updates. Two captures are byte-identical, including an in-memory
mid-window resume. Ordered schemas and original annotations let the native
consumer prepare tokens and targets independently. All four native CPU
consumers passed the composed forward, gradients and updates. A fresh native
controller also restores weights, pending accumulators, moments, gradient
presence and counters byte-exactly from a durable mid-window checkpoint,
then matches both source optimizer updates. Native explicit dropout-mask
admission rejects incomplete/unknown/duplicate/invalid sets, replays all
gradients when a complete set is reordered, and changes the loss when a live
marginal mask changes.

The separate [controlled-dropout source capture](../zig/pkg/inference/scripts/gliner25/TRAIN_STEP_DROPOUT_ORACLE.md)
completed all four profiles and a byte-identical repeat. All four native CPU
consumers subsequently passed every component loss and gradient, durable
fresh-controller restore, and both AdamW updates. This explicit-mask contract
does not equate native and Torch random-number streams. Both composition
fixtures are synthetic and do not establish real-model fine-tuning quality or
convergence.

The [four-row job fixture](../zig/pkg/inference/scripts/gliner25/TRAINING_JOB_FIXTURE.md)
provides two training and two disjoint validation examples with one complete
fixed schema. Its preparation is deterministic and uses no model. The subsequent
real-small head-only native job and independent strict Fastino export load now
have narrow integration evidence in [the job guide](GLINER25_TRAINING_JOB.md)
and [export checker](../zig/pkg/inference/scripts/gliner25/TRAINING_EXPORT_CHECK.md).
The same fixed data now has separate task-head and encoder-plus-head LoRA/DoRA job, exact resume and
standard PEFT load evidence in [the adapter export guide](GLINER25_TRAINING_EXPORT.md).
Held-out preflight runs no evaluation; representative quality, convergence
and trained-output parity outside the specifically proved artifacts remain
separate checks.

## Implementation sequence

| Stage | Concrete implementation | Required proof |
| --- | --- | --- |
| Losses and supervision primitives | `finetune/gliner_boundary_losses.zig`: masked BCE, asymmetric focal, pair BCE, listwise gold-mass, inside BCE, noisy-OR consistency, abstention, Poisson counts, detached hard negatives, exact/IoU candidate labels, and dense start/end/inside targets | Compare each scalar loss, every input-logit gradient, masks, axis order, and targets against pinned Torch; prove empty/all-masked behavior, limits, cancellation, and allocation cleanup |
| Canonical training data | `finetune/gliner_boundary_targets.zig`: compile immutable document offsets, full declared query schemas, positive/negative annotations, classification labels, record identities/alternatives, and typed relation pairs into bounded target batches | Differential preprocessing and target fixtures for every task; reject malformed labels, ambiguous occurrences, and capacity overflow before a step; preserve the full evaluation schema independently of gold labels |
| Detached candidate and record decisions | `finetune/gliner_boundary_selection.zig` and `gliner_boundary_matching.zig`: query/shared pools, gold injection, exact record target bindings, natural anchors, and bounded Hungarian matching | Pinned proposal membership and cost/assignment fixtures, explicit caller-owned random draws, complete retained gold, immutable binding fingerprints, and allocation/cancellation checks |
| Differentiable boundary forward | Build the boundary, shared-pool, classifier, relation, and record operations on the existing graph/autodiff infrastructure; bind discrete selected indices separately from differentiable re-scoring | Per-operation and per-head gradients against Torch, then all-task loss and every trainable gradient on a tiny real checkpoint |
| Head-only training | Freeze the encoder and enroll the explicitly selected boundary/classifier/relation/record tensors as regular trainables; cache encoder states only under exact model/tokenizer/schema/augmentation identities | Frozen tensor hashes remain unchanged; native versus Torch optimizer updates match; cached and uncached head steps agree |
| Full training | Enroll encoder embeddings, relative embeddings, normalization, projections, and all selected task parameters; add explicit encoder/task optimizer groups and learning rates | Gradient completeness, distinct LR/decay groups, clipping, accumulation, final partial-window flush, nonfinite atomicity, and memory-admitted CPU/Metal steps |
| LoRA and DoRA | Resolve public aliases to exact architecture-specific linear modules; add PEFT-compatible DoRA graph semantics and magnitude optimizer/checkpoint state | Exact selected parameter inventory, shared-weight behavior, initial no-op output, A/B/magnitude gradients, one-step update, adapter import/export, and merged versus unmerged inference |
| Durable training runs | Reuse managed execution, bounded graph caches, optimizer-state persistence, explicit dataset/model/schema hashes, deterministic random-stream state, and atomic artifact publication | Interrupted/resumed and uninterrupted runs produce equivalent next-step inputs, gradients, optimizer counters, final adapters/checkpoints, and inference results |
| Release qualification | Run all three published models on CPU and Metal, then evaluate held-out data with every supported task and adapter mode | Independent Python reference runs, repeated seeds, disjoint validation/test data, per-task quality and calibration, training throughput/peak memory, and failure recovery |

## Existing infrastructure to reuse

`real_autodiff_trainer.zig` owns regular parameters and LoRA parameters,
supports gradient accumulation and clipping, provides compiled device execution
and resident optimizer state, bounds shape-specific graph caching, and saves
optimizer/checkpoint state. `architectures/deberta_graph.zig` provides the
encoder graph. These components should be extended through their existing
interfaces rather than introducing a second optimizer or checkpoint owner.
`seeded_gradient_trainer.zig` now supplies explicit-gradient optimizer groups
and staged update validation while retaining `RealAutodiffTrainer` as the slot
and Adam-state owner. Native CPU is the default; explicit `resident_metal`
Controller updates and managed full/head-only runs have the independently
scoped tiny GPU evidence described above. Managed configuration defaults to
native CPU and accepts explicit `resident_metal` with a Metal-enabled build and
separate metadata/device admission. The resident job option now has the
separate published-small classifier-only LoRA/DoRA CLI continuity evidence
above; other published target families/backbones remain unqualified. The CPU
CLI also has the separate head-only and all-Linear adapter evidence above.
`graph/seeded_training.zig` and `multi_stage_training.zig` retain activations
across detached candidate/relation decisions without replaying the encoder or
dropout. Their tapes bind the parameter epoch and exact input identity.

`gliner2_real_autodiff.zig` supplies useful integration patterns and existing
checkpoint/evaluation workflows, but its `gliner2_total_loss` consumes the older
span and count-embedding architecture. `gliner2_boundary.zig` stores cached
top-layer span-training artifacts with a separate versioned family. Neither
artifact nor objective should be accepted as a GLiNER2.5 training run.

## Loss and selection semantics

The boundary model combines start, end, pair, inside, soft-IoU, reranker-listwise,
proposal-listwise, marginal-consistency, abstention, and count losses, then adds
classification, relation, and record losses. Every published checkpoint's exact
weights, annealing schedules, negative-query sampling, and reduction mode must
be retained. Published profiles use settings such as focal marginals, summed
reduction, and gold-inclusive hard-negative pools; generic BCE defaults are
insufficient.

Candidate indices, hard-negative ranking, gold-injection decisions, and record
Hungarian assignments are detached. Selected proposal logits must be recomputed
through the differentiable path. Detaching those logits would remove proposal
training; differentiating through candidate membership would change the
reference objective.

Record matching also uses a different normalization from its final loss:
Hungarian costs sum list-field BCE terms, while the matched field loss averages
candidate terms, then fields, then matched records. Natural records identify
instances through their anchor candidates; latent and anchorless modes match
gold identities to instance hypotheses. These paths need separate target and
gradient fixtures, including absent fields, alternative occurrences, empty
records, duplicate spans, and insufficient capacity.

The native loss module returns owned scalar losses and gradients in the input's
explicit `[B,Q,C]` or `[B,C,Q]` order. It supports graph/custom-VJP integration
without assuming a particular optimizer owner. Reductions retain two easily
missed source behaviors: negative weights do not alter the normalization
denominator, and generic `global` reduction does not apply an additional query
mask. Its caller must include query validity in the element mask. `sum` and
`per_query` apply the query mask explicitly.

Listwise losses preserve the finite `-1e4` masking sentinel, including its effect
when valid logits are even smaller. Noisy-OR consistency sends gradients to
both candidate and marginal logits and preserves the upper probability clamp.
Poisson counts use `log_input=true` and `full=false`. All-masked and empty
supervision is finite with zero gradients. Invalid active targets, nonfinite
results, budget exhaustion, cancellation, and allocation failure remain typed
errors; they cannot become skipped targets or successful zero-loss steps.

`scripts/gliner25/capture_training_losses.py` imports the actual pinned loss
functions and records high-precision values and gradients from exact float32
inputs. It loads no model and restricts Torch to one CPU thread. Its fixture
records source-file digests and software versions. End-to-end tests must also
cover the production floating-point dtype and backend kernels.

Kernel limits independently bound batch, query and candidate dimensions,
elements per buffer, and work. Empty dimensions do not bypass scratch-allocation
limits. Consistency and dense-target scratch is reclaimed before returning;
only the result vectors remain owned by the caller. Service-level admission
must additionally bound the complete training step across all component losses.

## Adapter and optimizer compatibility traps

The boundary-specific `gliner_boundary_peft_graph.zig` implements LoRA and
DoRA using PEFT-style `[out,in]` weights. DoRA recomputes the live row norm and
detaches it in the graph, preserves bias/scaling semantics, and shares adapter
weights while assigning dropout to each module use. Its pinned PEFT fixture
passed all eight train/eval/dropout cases. The standalone compatibility
`lora.zig` helper differentiates its differently oriented norm and must not
replace this boundary path. Allocation errors propagate through the new graph
and its adapter scratch.

Architecture-aware aliases must include `encoder`, `classification_head`,
`extractive_head`, `relation_head`, `record_head`, and `all_task_heads`. Resolve
them against the exact checkpoint inventory, preserve tied/shared weights, and
reject selectors that unexpectedly match no modules. Pinned trainer defaults
still contain legacy span/count names, so they must not silently define the
native boundary training target set.

Full upstream training separates encoder and task learning rates using the
literal test `"encoder" in name`. Boundary-encoder and candidate-encoder
parameters therefore receive `encoder_lr` too. Head-only freezing excludes
only the top-level encoder subtree. Every source group applies the configured
weight decay, including biases and normalization parameters. In LoRA mode,
every trainable parameter receives `task_lr`, including DoRA magnitude and
separately unfrozen heads. The exact source AST grouping fixture records these
rules; its native consumer passed all four supported CPU run profiles (full,
heads, LoRA and DoRA). Symbolic CUDA/MPS constructor flags and an additional
unfrozen base parameter remain source-only cases.
Checkpoints must preserve group
membership, schedules, per-parameter Adam counters, gradient-accumulation
position, and conditional task-family state. A missing task in a microbatch
must not accidentally decay or advance a parameter that received no gradient.
In particular, PyTorch gradient `None` skips AdamW state and decay, while an
explicit zero gradient participates. The shared-pool path leaves legacy
explicit scorer parameters unused; upstream `_head_touch` intentionally gives
zero gradients to optional relation/record heads. These cases cannot be merged.

## Data, reproducibility, and resource admission

The canonical target compiler accepts exact source offsets first.
String-only imported annotations need an explicit occurrence policy and must
report ambiguity. Record identity and alternative occurrences must survive
lowering; they cannot be flattened into independent field spans. Native entity
attribute supervision requires an explicit annotation contract lowered to the
same hidden query labels used during inference, since the pinned training data
helpers do not expose a dedicated attribute helper.

`gliner_boundary_targets.compileBatch` consumes processor samples, their compiled
schemas, and one annotation object per sample. Each annotation carries the exact
schema fingerprint. Source spans declare UTF-8 bytes, Unicode codepoints, or
UTF-16 code units and must align exactly to original-text word boundaries. The
compiler rejects invalid UTF-8, split surrogate pairs, offsets inside a word,
and references to synthetic punctuation. This deliberately tightens the pinned
character-to-word helper, which can expand a partially aligned source span.

Entity annotations retain their declared type and explicit attribute labels.
Every applicable attribute group must be supplied, including an empty label
list for a negative multilabel group. Extractive annotations are complete for
the declared schema, so omitted entity, attribute, field, or relation labels
are negative targets. Classification has a separate supervision mask: an
omitted task is unsupervised; a present empty label set is supervised negative
and must satisfy its cardinality. Cross-task classification constraints require
complete classification task supervision and a valid gold assignment.

Records have explicit IDs scoped to their declared structure. A field contains
distinct gold values; each value retains its explicit alternative source
occurrences. Declared enum choices have their own identity and point to the
processor's synthetic scoring position. Their canonical choice value is
validated before training. Required fields, duplicate annotations, inconsistent
enum alternatives, and insufficient target capacity are errors. JointIE edges
reference exact typed entity annotations. The compiler requires the complete
gold graph to satisfy endpoint, overlap, degree, symmetry/inverse, and global
graph constraints; a solver cannot drop invalid gold to make it trainable.

The result owns record IDs, grouped mention pairs, target arrays, and schema/text
hashes. Packed `[B,Q,G]` pairs use the same word/query coordinates as the native
processor, including enum prefixes. Stable deduplication preserves all distinct
gold mentions. Dynamic padding uses the observed maximum, with a minimum gold
capacity of one; fixed padding fails if gold exceeds the requested capacity.
Batch size, source bytes, routed widths, annotations, per-query gold, padded
elements, work, and cancellation are bounded before a training step. The next
candidate stage must preserve these identities while keeping discrete proposal
membership and assignment decisions outside the differentiable graph.

`capture_training_targets.py` exercises the actual pinned schema transformer's
word-label path and boundary target packer without loading a model or a subword
tokenizer. Its ten fixtures cover mixed tasks, lowered attributes, enum prefixes,
ragged and fixed padding, natural/latent/anchorless record identity, explicit
alternative occurrences, negative extraction, classification-only samples, both
Unicode splitters, and strict gold-capacity overflow. Full tokenizer and encoder
parity remain separate fixture and runtime checks.

`gliner_boundary_selection` requires an explicit training or evaluation phase.
Probabilistic gold injection consumes caller-owned uniform draws; evaluation
never consumes gold-injection draws and cannot change its proposed spans based
on evaluation answers. Query pools and document pools preserve their separate
recall denominators and layout. The native path retains the pinned gold/query
quota priority bands and finite invalid-score sentinel, but raises a capacity
error if a requested gold injection disappears from the retained pool. A
successful training step must not silently lose supervision because its pool
was too small. The 42-case fixture calls the pinned query selector and actual
document-pool forward implementation without loading a model.

`gliner_boundary_matching.compile` binds each canonical record identity to the
exact retained candidate columns and declared field membership. Every distinct
gold field value must retain at least one of its annotated alternative
occurrences. Its owned target map and fingerprint survive release of the
annotations and candidate inputs. `buildCosts` is a high-precision reference
over float32 logits; the production graph supplies its own float32 costs through
`fromScoredCosts`, which widens those exact values for the Hungarian solver.
Matching costs are detached decisions, not the reported training loss.

Natural records bind explicit anchor candidates. Latent and anchorless modes
match only active instance hypotheses, and every gold record must receive an
assignment. This deliberately corrects the pinned finite-sentinel path, where
an inactive hypothesis can win a gold column when valid costs exceed `1e4`,
then silently discard that gold during filtering. Empty supervision preserves
negative object labels for active non-natural hypotheses. The 18-case matching
fixture records pinned target indicators, float64 reference costs, actual
float32 graph costs, and both resulting assignments; exact ties are evaluated
within the active-hypothesis domain.

The complete declared schema belongs in evaluation input. Inferring the schema
from labels present in the gold document leaks supervision and changes negative
queries. Dataset validation must report and reject invalid records or labels by
default. The pinned source sometimes skips malformed classification shapes or
sanitizes records despite stronger documentation; native strict validation
must be explicit and covered by compatibility tests.

Randomness must be reproducible across resume: schema/task order, data order,
dropout per use site, negative-query sampling, proposal/gold injection, and
optimizer steps all need stable counters or serialized states. Tiny deterministic
fixtures should inject identical masks/selected indices into both runtimes
before broader stochastic equivalence tests.

Admission must account for parameters, gradients, Adam state, activation and
checkpoint recomputation buffers, candidate/record capacities, and device
workspace together. Full training must not use an inference-only memory
estimate. Overflow of gold mention, candidate, relation, or record capacity is
an error by default. This protects supervision integrity and prevents an
apparently successful run from silently training on less data than requested.
