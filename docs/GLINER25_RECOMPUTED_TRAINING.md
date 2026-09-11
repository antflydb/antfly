# GLiNER2.5 regional activation recomputation

`activation_profile: "layer_recompute_v1"` is an opt-in training implementation.
The completed local evidence covers eight tiny pinned-source regional Step
profiles on CPU and actual Metal, full/head-only NativeTrainer resume, and
eight inactive-adapter NativeTrainer source profiles per backend with actual
optimizer updates. A separately pinned published-small native CLI campaign
also passes all-131-target LoRA/DoRA training and exact partial-window resume.
Those fixture and published-artifact scopes remain separate. Published regional Metal,
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

## Verification scope and limits

The retained tests compare regional and retained execution with shared
parameter/cotangent handling, typed dropout replay, explicit admission,
allocation failure and cancellation recovery. Tiny pinned-source full/head/
LoRA/DoRA profiles cover CPU and resident Metal. Zero-dropout cases use tiled
attention; supplied-mask cases retain materialized attention and its original
mask semantics. Existing numerical tolerances remain unchanged.

Managed NativeTrainer tests perform actual later optimizer updates and
fresh-owner partial-window resume. The inactive-adapter fixture initializes
only captured adapter values through a test-only constructor; it does not copy
source post-update state into the trainer. Absent, zero and active gradients
retain distinct optimizer semantics.

## Published-small native campaign

Local all-131-target small LoRA/DoRA jobs also exercised the ordinary production
constructor with both opt-in profiles. Each five-row epoch used accumulation
two and compared pause-after-one/fresh resume with uninterrupted execution:
final result, checkpoint and exported adapter bytes matched exactly. This is
published native restart/artifact consistency, separate from source VJP parity.

A restore-cap regression now reserves staged state and the actual immutable
checkpoint before assigning the remaining transaction bytes to header parsing.
It preserves the caller's caps and returns a typed denial when the full restore
cannot fit; increasing memory limits is not part of the fix.

Published regional Metal, published-model PyTorch gradient/update parity,
full 512-token numerical training, other backbone/mode/rank combinations,
memory/performance benchmarks and convergence remain open. Source weights stay
FP32; quantized training and release qualification are excluded. Reusable
fixtures remain in testdata; per-attempt logs, binaries and source copies are
external artifacts under the [fixture policy](../zig/pkg/inference/testdata/gliner25/README.md).
