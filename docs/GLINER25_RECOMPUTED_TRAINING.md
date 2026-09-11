# GLiNER2.5 regional activation recomputation

`activation_profile: "layer_recompute_v1"` is an opt-in training implementation.
The completed local evidence covers eight tiny pinned-source regional Step
profiles on CPU and actual Metal, full/head-only NativeTrainer resume, and
eight inactive-adapter NativeTrainer source profiles per backend with actual
optimizer updates. A separately pinned published-small native CLI campaign
also passes all-131-target LoRA/DoRA training and exact partial-window resume.
The [fixture ledger](../zig/pkg/inference/testdata/gliner25/recomputed_training_execution_v1/manifest.json)
and [published native ledger](../zig/pkg/inference/testdata/gliner25/recomputed_training_published_small_execution_v1/README.md)
keep those scopes and source identities separate. Published regional Metal,
long-context numerical execution and the broader training matrix remain open;
the public `runtime_available: false` gate is unchanged.

## Two independent profiles

| Job field | Default | Opt-in behavior |
| --- | --- | --- |
| `attention_profile` | `materialized_v1` | `replay_tiled_v1` uses the dedicated replay-tiled DeBERTa attention primitive and compact physical i32 dropout controls. |
| `activation_profile` | `retained_v1` | `layer_recompute_v1` retains encoder boundaries and rebuilds one local encoder tape at a time during backward. |

Both profiles are included in durable run identity; restoring with a different
profile is rejected. Changing resource ceilings does not change the model's
mathematics. Regional activation recomputation alone does not remove a
materialized attention matrix. The combination intended for longer sequences
is `replay_tiled_v1` with `layer_recompute_v1`, subject to all memory and work
limits. The [attention evidence](../zig/pkg/inference/scripts/gliner25/TRAINING_ATTENTION_ORACLE.md)
qualifies the dedicated primitive separately.

## Encoder regions and one task head

The graph records an embedding/prelude region that produces the embedding
output and normalized relative table, followed by one region per encoder
layer. Each layer consumes the previous hidden state and the shared relative
table. Region boundaries are semantic graph outputs, preserved across lowering
and the single LoRA/DoRA rewrite; they are not guessed from node numbers.

The complete encoder is cut out before the task head is differentiated. The
head uses its existing direct or staged proposal/candidate/relation execution
once. Gold injection, hard negatives, relation identities and Hungarian
assignments remain detached decisions. Live head logits produce the actual
loss and cotangents. The head's final-hidden cotangent seeds reverse encoder
replay; it is not an optimizer parameter or a fabricated scalar loss.

Backward visits encoder regions in a fixed reverse order. It accumulates
shared relative-table and parameter gradients in the original graph's
namespace, releases each local tape, then merges head and encoder results into
canonical selected parameter IDs. Duplicate/unselected IDs, incompatible
shapes or loss identities are rejected. Returned gradients own independent
leases or sums; their borrowed inputs remain valid through the merge.

Absent gradients stay absent. Explicit zero gradients retain optimizer
presence, including the source's optional-head touch behavior. If the complete
objective has no trainable path, NativeTrainer applies the separately verified
source all-trainable-zero fallback. Head-only training does not need encoder
backward; a missing final-hidden cotangent does not create encoder zeros.

## Replay, ownership and cancellation

Named encoder/PEFT dropout recipes are resolved after the graph rewrite,
including VJP alternatives. Each occurrence receives the same typed mask on
initial execution and replay. Recipe names, shapes, probabilities and identities
are sealed. Explicit masks require complete immutable coverage; mutation,
missing/duplicate names or changed replay identity are errors. Generated masks
use the existing seed/microbatch/replica/site counters. This deterministic RNG
is not PyTorch's RNG. Tiled attention uses its own sealed compact control; the
composed Step rejects explicit mask overrides with that attention profile.

The source graph, parameter bindings, recipe context and backend outlive both
the regional and head tapes. A live tape prevents parameter-epoch changes;
backward consumes its tape on success and failure. All child epochs are checked
before publication. Cached recipes discard construction-time controls and
validate with the current request. Control composition must preserve both
cancellation sources, the earliest deadline, IO and the process-watchdog
carrier. Cooperative checks do not promise interruption inside a driver call;
the existing supervised-process boundary remains required there.

## Admission contract

Execution requires a sealed aggregate admission after both regional programs
and the head have been compiled. It includes compiled owners, immutable
bindings, retained boundaries, gradients, the largest local tape and scratch,
replay uploads, head stages, canonical gradient merging, and a complete pending
optimizer transaction. Replay and optimizer work count toward the enclosing
work ceiling. Tiled training with activation recomputation admits four attention
sweeps rather than the retained profile's three.

The trainer counts already-live parent memory once. For parent live bytes `H`,
regional live bytes `R` already inside `H`, and regional cap `Rcap`, its future
host reservation starts with `H + (Rcap - R)`. Source/dataset owners remain
separately admitted. Caller batch scratch, Step scratch, head runtime metadata,
native binding/readback storage and optimizer staging need their own additive
bounds. Cache hits recheck current live bytes because matching graph geometry
does not imply equal text/schema allocations. A restored epoch-end flush needs
independent host/device/work admission before an optimizer update.

`NativeTrainer.Limits.max_recomputed_batch_scratch_bytes` bounds the separate
caller arena only for `layer_recompute_v1`. Its current default is 512 MiB;
the tiny integration fixture explicitly uses 16 MiB. Admission reserves the full cap,
including possible arena overhead, before execution. The existing
`memory.host_bytes`, `memory.backend_bytes`,
`memory.optimizer_transaction_bytes` and `memory.combined_bytes` job ceilings
still apply. Raising an outer reservation does not implicitly raise a tape,
replay or work limit. The explicit versioned resource mapping exposes these
limits below.

The Step's aggregate regional host ceiling defaults to 6 GiB; the generic
graph core retains its 1 GiB default. These Step/caller defaults changed after
integration v3, whose archived source used 1 GiB and 256 MiB respectively.
The old default aggregate could reject even a small job's complete owner
reservation. The correction does not raise the enclosing job budgets or
change the retained path. The successful tiny NativeTrainer tests supplied
their smaller plan, Step and caller caps explicitly.

The new version-1 resource paths are
`training_limits.max_recomputed_batch_scratch_bytes`,
`training_limits.step.recomputation` and `training_limits.step.replay`. They
accept positive JSON integers under the
[job resource ceilings](GLINER25_TRAINING_JOB.md#explicit-training-resource-limits).
This fragment makes selected current defaults explicit; it is not a complete
hardware profile or a published-model admission result:

```json
{
  "attention_profile": "replay_tiled_v1",
  "activation_profile": "layer_recompute_v1",
  "training_limits": {
    "version": 1,
    "max_recomputed_batch_scratch_bytes": 536870912,
    "step": {
      "recomputation": {
        "max_plan_host_bytes": 268435456,
        "max_host_bytes": 6442450944
      },
      "replay": {
        "max_host_bytes": 67108864,
        "max_mask_bytes": 67108864
      }
    }
  }
}
```

This resource-wire addition and the revised defaults postdate native v3.
They passed seven numeric-wire tests and the job mapping test in the later
Metal-enabled integration checkpoint.

Native optimizer staging uses reclaimed allocations for an exact
allocator-visible metadata-plus-four-arrays upper bound. The resident estimate
includes finite/norm reductions, snapshot/accumulation/AdamW work and private
pending tensors. Work and internal scalar-transfer ceilings are bounds, not
measured GPU operations, RSS or timing. Declared-limit failures remain distinct
from backing allocator OOM; rejection/cancellation must leave optimizer state
and retry ownership intact.

## Completed evidence and limits

| Checkpoint | Result | Scope |
| --- | --- | --- |
| Core native v1 | Compile failed | Invalid assignment syntax; requested tests did not run. |
| Core native v2 | 13 selected: 8 passed, 5 failed, 0 skips/leaks | Synthetic core fixture reached an unsupported resident instruction; recipe/head-wrapper tests passed. |
| Core native v3 | 13 selected: 13 passed, 0 skips | Full-versus-regional synthetic gradients, shared cotangents, recipe identity, allocation/cancellation recovery and admission. |
| Integration native v1 | Compile failed | Mutable-slice mismatch in the new merge test; requested tests did not run. |
| Integration native v2 | 36 selected: 34 passed, 2 expected Metal skips | Regional head/merge/replay helpers, native/resident transaction contracts, exact native staging bounds, and tiny two-layer full/head-only NativeTrainer cancellation and partial resume. |
| Integration native v3 | 63 selected: 61 passed, 2 expected Metal skips, 0 failures/leaks | Eight pinned-source regional CPU profiles, four retained controlled-dropout regressions, native full/head-only resume and bounded scratch/cache-hit/flush retry, and control/ownership regressions. |
| Numeric-wire native v1 | Wrapper stopped before tests | Optional executable-argv inspection failed on transient compiler processes. Known child identities were gone, but cleanup inspection errors prevented complete cleanup proof. |
| Integration Metal v1 | 43 selected: 43 passed, 0 skips/failures/leaks | Eight regional CPU and eight actual-Metal Step source cases; full/head-only NativeTrainer resume on both backends; all eight inactive-adapter NativeTrainer source profiles per backend with native-owned optimizer updates and resume; resource-wire/job and optimizer/control tests. |

The NativeTrainer fixture uses five authored rows, hidden width 4, two encoder
layers, batch size 2, two epochs and accumulation 2. Each mode completes six
microbatches and four optimizer updates, including epoch-end partial flushes.
Its dropout probabilities are zero. It explicitly uses 256 MiB host and backend
caps, a 1 GiB combined cap, 32 MiB regional-plan and Step caps, and 16 MiB
caller scratch. Source/dataset reservations remain separately counted.

The separate v3 Step/Controller source consumers reuse the existing mixed-task
fixtures. Full, heads, LoRA and DoRA each pass with zero dropout and
`replay_tiled_v1` attention, then with explicit dropout at 0.125 and
`materialized_v1` attention. The latter preserves the source's dense attention
probability masks while replaying encoder and PEFT activations. Each case
checks all loss terms, every trainable gradient including absence, three
microbatches, two AdamW updates and a durable fresh-Controller mid-window
restore at the unchanged fixture tolerances. Four retained/materialized
controlled-dropout cases also pass. These v3 comparisons use CPU execution.
Metal v1 reruns the eight regional CPU cases and adds the corresponding eight
resident-Metal Step comparisons. Those GPU Step consumers bind captured
initial/post-update weights; optimizer updates are proved separately.

The Metal-enabled run also executes the tiny full/head-only NativeTrainer on
CPU and Metal. Its separate inactive-adapter NativeTrainer source fixture
covers LoRA and DoRA for classifier-only, encoder-plus-classifier, record-only
and relation-only targets: eight profiles per backend. Each uses zero dropout,
replay-tiled attention and regional activation recomputation, checks source
tokens, objective terms, gradients and absence/zero semantics, performs actual
native-owned optimizer updates, and compares uninterrupted execution with a
fresh-owner durable partial-window restore. That fixture retains 128 MiB host,
256 MiB backend and 512 MiB combined caps, with 32 MiB plan/Step and 16 MiB
caller caps. Its captured initial adapter values are a test-only fixture;
subsequent updates are native.

The v3 result is bound to 30 named source files captured during the root
agent's freeze. This is a partial relevant-source archive; exact test
executable identity remains unrecorded and is not inferred from cache. Earlier
core/v2 results do not acquire that later source identity. Caller scratch,
restored-flush and watchdog-carrier tests belong to v3, while the subsequent
default/resource-wire changes belong to the later Metal v1 snapshot.

Metal v1 records a live owned test-process identity and the exact 52,470,600-byte
test executable, SHA-256
`f366f03f65ec494a00c3f385d7c182b324f2b3957469d4f8d007ff82e8918b83`.
Its 2,544-file source inventory was unchanged; 38 relevant source files are
copied in the repository ledger and the full recorded selection and executable
are archived locally. This is not a complete external dependency closure.
The bounded wrapper reported 602.390 seconds for compilation plus tests,
2,544,402,432 bytes sampled peak child-tree RSS, and complete owned-process
cleanup. Shared pages may be counted more than once; these figures are process
evidence, not a training benchmark. The runner selected 43 tests; its `/75`
progress denominator is not a test count.

The earlier failed wrapper and its exact reconstructed source remain archived.
The successful wrapper treats optional argv inspection errors separately from
mandatory creation-identity, RSS and cleanup checks; the live executable
receipt was captured independently. No product result is attributed to the
failed attempt.

## Published-small native campaign

A separate production standalone CLI (`92a2816c…`, full identity in the
[published ledger](../zig/pkg/inference/testdata/gliner25/recomputed_training_published_small_execution_v1/manifest.json))
completed six phases: pause after one microbatch, fresh-owner resume, and an
uninterrupted run for both rank-2 LoRA and DoRA. It used the original published
small FP32 source, the ordinary initializer/inventory, all 131 encoder/task-head
Linear targets, `replay_tiled_v1` attention and `layer_recompute_v1` activation
recomputation. There was no synthetic-model or initial-state injection seam.

Each mode used five short authored rows, one epoch and accumulation two,
completing five microbatches and three updates at rows 2, 4 and 5. Source dropout
remained 0.1. The final result, checkpoint and all four adapter-export files
match byte for byte between resumed and uninterrupted runs. The independent
checker streamed the checkpoint and reconstructed Controller/state hashes,
validating 262 LoRA or 393 DoRA slots and their exact per-slot counters. Encoder
adapters remain active on rows without classification, so the global zero-loss
fallback is false throughout this all-target campaign. This differs from the
separately proved classifier-only fallback sequence.

| Mode | Largest trainer host allocation peak | Largest native backend allocation peak | Largest sampled child-tree RSS |
| --- | ---: | ---: | ---: |
| LoRA | 22,672,679 B | 21,976,220 B | 463,323,136 B |
| DoRA | 32,805,432 B | 33,032,120 B | 486,211,584 B |

Every phase admitted 2,354,931,717 bytes including separate source/dataset/job
owners. Explicit host/backend/combined caps were 768 MiB / 1 GiB / 3 GiB, with
32 MiB optimizer transaction, 128 MiB each regional-plan/Step, and 64 MiB caller
scratch caps. The outer sampled tree cap remained 4 GiB. All six processes
exited 0 with complete owned-child cleanup. Allocation peaks, reserved amounts
and sampled RSS measure different things; these short runs are not a benchmark.

The ledger also preserves v1's passing LoRA pause and clean
`TrainingOptimizerLimitExceeded` resume failure. Its restore path charged the
default 64 MiB parser ceiling against the whole 32 MiB transaction. V2 reserves
staged state and the actual immutable checkpoint first, then clamps parser
capacity to the remainder. Every v1/v2 memory cap stayed unchanged. Both frozen
Controller sources, their exact diff, helper bytes and failure/success receipts
are retained. The full binary and source archive remain external pinned
artifacts; the repository carries compact receipts and selected source bytes.

The source remains original FP32; this profile does not enable quantized
training. This campaign proves native published-small restart/artifact
consistency. Published regional Metal, published-model PyTorch gradient/update
parity, full 512-token numerical execution, other backbone/mode combinations,
memory/performance benchmarks, convergence and release qualification remain
open. Existing retained-profile results are documented separately in
[the training contract](GLINER25_TRAINING.md).
