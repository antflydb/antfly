# XLA-like Computation Graph IR for antfly-inference-zig (superseded)

This was the original proposal for layering a computation graph IR (tracing,
optimization passes, backend-agnostic execution, and training/autodiff) on
top of antfly-inference-zig's eager `ComputeBackend` vtable. That design
shipped; [GRAPH.md](GRAPH.md) describes the graph IR, tracing/cache/replay
architecture, op system, compiler passes, execution backends, and partitioning
as implemented today, and is the authoritative reference. The autodiff/training
path this proposal called for is wired into `src/finetune/` (see
`src/finetune/graph_bridge.zig`, which imports the `ml` module directly, and
per-architecture files such as `gemma4.zig`, `colqwen2_real_autodiff.zig`, and
`gliner2_real_autodiff.zig`), not just present as unused library code.

The rest of this document is kept only for design rationale that GRAPH.md
does not restate: the external patterns this proposal drew from when deciding
the shape of the graph IR.

## GoMLX Patterns Adopted

The graph IR borrows several patterns from GoMLX rather than designing them
from scratch:

- **Buffer donation.** A `donate: []bool` parameter lets the interpreter
  reuse input buffers for outputs, avoiding allocation in the hot decode
  loop where tensors are the same size every step.
- **Decompose-first, fuse-second (GoMLX's `InternalFusedOpCaller`
  pattern).** The builder emits the decomposed primitive subgraph first,
  then the fused node, and stores the decomposed root as `vjp_alternate`.
  This keeps fused ops differentiable without a separate lowering pass:
  autodiff walks the decomposed shadow graph instead of needing a
  hand-written VJP for every fused op. `fused_disentangled_attention` is
  the one deliberate exception in `lib/ml/src/graph/autodiff.zig` — it has
  a hand-written `fused_disentangled_attention_backward` op rather than
  differentiating through a fully decomposed attention subgraph.
- **Scalar/constant caching.** A per-graph `(dtype, value) -> NodeId` cache
  avoids duplicate constant nodes, which matters because eps/scale-style
  constants recur constantly in a transformer graph.
- **Variable/Context for training.** GoMLX's scoped-naming, initialization,
  and train/eval/inference mode `Context` is the model this repo's
  `lib/ml/src/context.zig` follows.
- **Deferred:** GoMLX's first-class control-flow ops (`While`, `If`, `Call`)
  and its distributed/sharding model (`DeviceMesh`, `ShardingSpec`,
  `AutoSharding`) were deliberately not adopted. The generation loop stays
  outside the traced graph (in `generation.zig`), and multi-GPU training
  placement was out of scope for antfly-inference-zig's single-device
  inference focus at the time this was written. See
  [MULTIDEVICE.md](MULTIDEVICE.md) for the current multi-device design.

## Open work

- Confirm current VJP/backward-op coverage against `lib/ml/src/graph/autodiff.zig`
  and `src/finetune/` directly before relying on specifics beyond what is
  stated above — this document does not attempt a full re-audit of every
  training-path detail GRAPH.md doesn't cover.
