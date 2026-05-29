# XLA-like Computation Graph IR for antfly-inference-zig

## Context

antfly-inference-zig currently uses **eager execution** with a `ComputeBackend` VTable of ~45 fused ops (`src/ops/ops.zig:118-345`). Model architectures (`gpt.zig`, `bert.zig`, etc.) call `cb.linear(...)`, `cb.rmsNorm(...)` directly, and each backend (BLAS, MLX, WASM) implements these ops immediately.

This works well for inference but prevents:
- **Graph optimizations** (op fusion, memory planning, dead code elimination)
- **Automatic differentiation** (no graph to differentiate = no training)
- **Backend-agnostic compilation** (can't emit CUDA/TPU/WGSL from a single representation)

**Goal:** Layer a computation graph IR on top of the existing backend vtable using a **tracing approach** (like JAX). Model code continues calling `cb.linear(...)` etc., but when `cb` is a tracing backend, those calls build a graph instead of executing. The graph is then optimized and executed through existing backends.

## GoMLX Patterns to Emulate

### 1. Buffer Donation (High value)
GoMLX's `donate []bool` parameter lets compiled executables reuse input buffers for outputs — avoids allocation in the hot path. For antfly-inference-zig's decode loop (same-size tensors every step), this is a significant memory optimization. The interpreter should support marking inputs as "donated" so backends can overwrite them.

### 2. ErrNotImplemented Fallback (High value, changes the design)
GoMLX's `InternalFusedOpCaller` pattern is cleaner than pure pattern-matching. Instead of pattern-matching fused ops *after* tracing, GoMLX:
1. Builds the **decomposed** subgraph first (lower node IDs)
2. Tries the fused version
3. If backend returns `ErrNotImplemented` → uses decomposed
4. If fused succeeds → stores decomposed as `vjpAlternateOutputs` for autograd

This means fused ops are **always available for autograd** without needing a separate lowering pass. We should adopt this: the builder emits both fused and decomposed, and the decomposed version is retained as a shadow graph for differentiation.

### 3. Scalar/Constant Caching (Medium value)
Per-graph caches for `(dtype, value) → NodeId` prevent duplicate constant nodes. Cheap to implement, reduces graph bloat (eps constants, scale factors, etc. appear many times in a transformer).

### 4. Variable/Context for Training (Needed for Phase 6)
GoMLX's `Context` with scoped naming, variable initialization, and train/eval/inference mode flags is essential for training. Without it, managing weights + gradients + optimizer state becomes ad-hoc. Worth designing when we reach Phase 6.

### 5. Control Flow Ops (Defer)
GoMLX supports `While`, `If`, `Call` as first-class graph ops. This matters for dynamic-length generation loops inside the graph, but antfly-inference-zig currently handles the generation loop outside the model forward pass (in `generation.zig`). Can defer this.

### 6. Distributed/Sharding (Defer)
GoMLX's DeviceMesh + ShardingSpec + AutoSharding is powerful for multi-GPU training but premature for antfly-inference-zig's single-device inference focus. Worth noting for the future.

---

## Design: Two-Level Op Granularity

**Key decision: the graph IR has two op levels.**

1. **High-level fused ops** (~45, mirroring the current VTable): `linear`, `rmsNorm`, `gqaCausalAttention`, `rope`, etc. These are what the tracing backend records. Backends that support them execute them directly.

2. **Primitive ops** (~30): `add`, `mul`, `dot_general`, `reduce_sum`, `reshape`, `transpose`, `gather`, `exp`, `rsqrt`, etc. Fused ops can be **lowered** to primitives via decomposition rules when needed (for autograd, for backends that don't support a fused op, or for cross-op optimization).

This avoids the false choice between "keep fused ops" and "decompose everything." The graph captures fused ops by default (fast inference path), and lowers to primitives on demand (training path, optimization path).

## Phase 0: Graph IR Core Data Structures ✅

**New files in `lib/ml/` — a reusable ML library, no changes to existing code.**

The pure graph IR lives in `lib/ml/` so it's reusable beyond termite (like `lib/jinja/`, `lib/tokenizer/`). Antfly inference-specific bridge code (tracing backend, interpreter that dispatches to `ComputeBackend`) lives in `src/graph/`.

### `lib/ml/src/graph/shape.zig`
- `DType` enum (f32, f16, bf16, i32, i64, bool)
- `Shape` struct: `dtype` + `dims: [8]i64` + `rank: u8`. Value type, max rank 8.
- Shape arithmetic helpers: `numElements`, `broadcastWith`, `eq`
- Scalar/constant cache: `ConstantCache` mapping `(dtype, f64) → NodeId` to deduplicate constants

### `lib/ml/src/graph/node.zig`
- `NodeId = u32` (index into `Graph.nodes` array — cache-friendly, serializable)
- `OpCode` tagged union with ~45 high-level variants (matching VTable) + ~30 primitive variants
- Each variant stores its integer metadata (rows, dims, eps, etc.) + `NodeId` references to inputs
- Up to 4 inline input slots per node (covers all current ops); overflow array for rare cases
- `Node` struct: `op: OpCode`, `output_shape: Shape`, `inputs: [4]NodeId`, `num_inputs: u8`
- `vjp_alternate: ?NodeId` — pointer to decomposed subgraph root for autograd (GoMLX pattern)

### `lib/ml/src/graph/graph.zig`
- `Graph` struct: owns `ArrayList(Node)`, constant pool (`ArrayList(f32)`), string table for parameter names, output node list, parameter node list
- Append-only (SSA-style) — nodes are never mutated after creation
- `addNode()`, `markOutput()`, `parameterName()`
- `ConstantCache` for scalar/tensor deduplication

### `lib/ml/src/graph/builder.zig`
- High-level builder API: `linear()`, `rmsNorm()`, `gelu()`, etc.
- Each fused builder method emits **both** the decomposed primitive subgraph and the fused node, storing the decomposed root in `vjp_alternate` (GoMLX's `InternalFusedOpCaller` pattern)
- Pure primitive builders: `add()`, `mul()`, `dot_general()`, `reshape()`, etc.

### `lib/ml/src/graph/lower.zig`
- Fused op → primitive decomposition rules (used by builder and by autograd)

### `lib/ml/src/graph/autodiff.zig`
- Reverse-mode AD with VJP table over primitives
- Fused ops use `vjp_alternate` to differentiate through decomposed subgraph

### `lib/ml/src/graph/passes/`
- `dce.zig`, `memory.zig`, `fuse.zig` — optimization passes

### `lib/ml/src/root.zig`
- Library module root, re-exports

**Verification:** Unit tests for shape inference on every op type.

---

## Phase 1: Tracing Backend ✅

**Record existing forward passes into graphs without changing model code.**

### `src/graph/tracing_compute.zig` (termite-specific, imports `lib/ml`)
A `ComputeBackend` VTable implementation where every op appends a node to a `Graph` and returns a `CT` handle wrapping a `NodeId`.

**The CT trick:** Allocate `TracingHandle` structs (`{ node_id: NodeId }`) and cast to `CT` (`*anyopaque`). The existing architecture code already passes `CT` values opaquely, so this is transparent.

**Key ops:**
- `getWeight("name")` → creates a `Parameter` node with that name + shape (looked up from a weight manifest passed at init time). Returns handle to it.
- `linear(input, weight, bias, rows, in_dim, out_dim)` → creates a `fused_linear` node referencing the input/weight/bias NodeIds. Returns handle.
- `rmsNorm(input, weight, dim, eps)` → creates a `fused_rms_norm` node. Returns handle.
- `toFloat32(tensor)` → marks node as graph output. Returns dummy f32 zeros (tracing, not executing).
- `fromFloat32(data)` / `fromFloat32Shape(data, shape)` → creates a `Constant` node.
- `free(tensor)` → records last-use metadata (useful for memory planning). No-op otherwise.
- `prefetchWeightHint`, `drainPrefetchBudget` → no-op (scheduling hints, not computation).
- `evalTensor` → no-op or records a scheduling barrier.
- `argmaxLastRow` → returns `null` (existing callers already handle this fallback).

**One-line change to existing code:**
- `src/ops/ops.zig:17-21`: Add `graph` to `BackendKind` enum.

**Verification:** 
1. Create `TracingCompute` with weight shapes from a real model manifest
2. Call `gpt.forward(tracing_cb, ...)` with a small config (1 layer, 4 heads, 32 hidden)
3. Assert the resulting `Graph` has correct node count, op types, and shape annotations

---

## Phase 2: Eager Graph Interpreter ✅

**Execute a traced graph node-by-node through an existing ComputeBackend.**

### `src/graph/interpreter.zig` (termite-specific, imports `lib/ml`)
- Maintains `values: []?CT` indexed by `NodeId`
- Walks nodes in topological order (which is just array order, since the graph is append-only)
- For each node: look up input CTs, call corresponding VTable function on real backend, store result CT
- For `Parameter` nodes: call `backend.getWeight(name)` to get the real weight tensor
- For graph outputs: collect and return the result CTs
- Liveness: compute last-use per node, call `backend.free()` after last consumer
- **Buffer donation** (GoMLX pattern): `execute()` accepts `donate: []bool` — donated input buffers can be reused by the backend for outputs, avoiding allocation in the hot decode loop

**The golden correctness test:**
```
For each architecture (GPT, BERT, T5, ...):
  1. eager_logits  = gpt.forward(blas_cb, ...)           // existing path
  2. graph         = gpt.forward(tracing_cb, ...)         // tracing
  3. interp_logits = interpreter.execute(graph, blas_cb)  // interpret
  4. assert(eager_logits == interp_logits)                // bit-exact
```

This must be bit-exact since the same backend + same weights + same op sequence = identical results.

---

## Phase 3: Stateful Op Handling (KV Cache, MoE) ✅

**Make the graph work with paged attention and MoE routing.**

The KV cache (`runtime/kv/`) and MoE runtime (`runtime/moe/`) are external mutable state. Rather than modeling them as graph nodes, treat them as **side-channel parameters** passed at execution time.

### Design
- `gqaPagedAttention` node records static metadata (num_heads, head_dim, etc.) but the live `AttentionContext` (with `KvManager` pointer, sequence IDs, etc.) is passed to the interpreter at `execute()` time
- Same for `moeSelectRoutes`, `moeLinearNoBias` — the `MoeRuntime` is a side-channel
- The interpreter's `execute()` signature:
  ```zig
  pub fn execute(
      graph: *const Graph,
      backend: *const ComputeBackend,
      inputs: []const CT,
      side_channels: SideChannels, // AttentionContext, MoeRuntime, etc.
  ) ![]CT
  ```

**Verification:** Run full generation (prefill + decode loop) with paged KV cache through trace-then-interpret path. Compare token-by-token against eager execution.

---

## Phase 4: Compilation Cache ✅

**Cache traced graphs by shape signature so the decode loop traces once.**

### `src/graph/cache.zig` (termite-specific)
- Cache key: `hash(model_config_hash ++ batch ++ seq_len ++ attention_mode)`
- For autoregressive decoding, the decode step graph (`batch=1, seq_len=1, mode=paged_decode`) is identical every step — trace once, reuse forever
- Prefill graphs vary by prompt length; bucket to powers of 2 for better cache hits
- Max cache entries: 32 (matching GoMLX's default)
- Invalidation: only on model reload

### Integration with `src/pipelines/generation.zig` ✅
- `graphForward()` method: trace → fuse pass → cache → interpret
- Feature-flagged via `graph_cache: ?*GraphCache = null` on `NativeGenerationPipeline`
- Opt-in via `ANTFLY_INFERENCE_GRAPH_MODE=1` env var in `native_generate.zig` and `server.zig`
- MoE models supported: tracer returns dummy routing so the grouped MoE path is traced; interpreter resolves routing dynamically at execution time via the real backend
- `TracingCompute.extractGraph()` transfers graph ownership cleanly to cache
- Verified bit-exact parity: eager vs graph mode on Gemma-3-270M (identical token IDs)

**Verification:** ✅ Bit-exact token-level match confirmed with real model. Cache hit/miss works correctly.

---

## Phase 5: Graph Optimization Passes ✅

**Optimizations impossible in eager mode.**

### `lib/ml/src/graph/passes/dce.zig` — Dead code elimination ✅
- Walk backward from outputs, mark reachable nodes, remove unreachable

### `lib/ml/src/graph/passes/memory.zig` — Memory planning ✅
- Liveness analysis → buffer reuse plan
- Allocate from a pre-allocated pool instead of per-op alloc/free
- Eliminates allocator overhead in the hot decode loop

### `lib/ml/src/graph/passes/fuse.zig` — Cross-op fusion ✅
- **Linear pair fusion:** 2+ `linearNoBias` on same input with identical dims → `linearNoBiasPair` + `toFloat32`
- **Algebraic simplifications:** `add(x,0)→x`, `sub(x,0)→x`, `mul(x,1)→x`, `mul(x,0)→0`, `div(x,1)→x`, `abs(abs(x))→abs(x)`, `convert_dtype(x, same)→x`, `broadcast(x, same_shape)→x`
- Applied automatically in `graphForward()` before caching

**Verification:** ✅ Optimized graph outputs bit-exact with unoptimized. Tests cover all algebraic patterns + linear pair fusion.

### MoE Graph Mode Support ✅
MoE routing (expert selection) is input-dependent, but the graph structure is static. The solution threads live routing through the interpreter:
1. **Tracing:** `moeSelectRoutes` returns dummy routing (all tokens → expert 0) so `moeFeedForward` takes the grouped path, tracing `fused_take_rows` → `fused_moe_linear_no_bias` → `fused_moe_scatter_add`
2. **Interpretation:** When the interpreter hits `fused_moe_select_routes`, it calls the real backend's `moeSelectRoutes` for actual routing. The `MoeGroupedState` (sorted by expert) is threaded to subsequent MoE ops via `ExecState`
3. **Dimension override:** The `attrs.rows` baked into MoE nodes reflects dummy routing; the interpreter uses actual grouped row counts from live routing. `in_dim`/`out_dim` are architecture constants and stay correct
4. **Multi-layer:** Each `fused_moe_select_routes` replaces the previous layer's routing state. Cleanup happens automatically at end of `execute()`

---

## Phase 6: Primitive Lowering + Autograd (Training)

**Lower fused ops to primitives, then differentiate.**

### Decomposition rules (already in `lib/ml/src/graph/lower.zig` from Phase 0)
Each fused op gets a decomposition rule:
- `fused_linear(x, w, b)` → `add(dot_general(x, transpose(w)), broadcast(b))`
- `fused_rms_norm(x, w, eps)` → `mul(mul(x, rsqrt(add(reduce_mean(mul(x,x)), const(eps)))), w)`
- `fused_gelu(x)` → `mul(mul(x, 0.5), add(1, tanh(mul(sqrt(2/pi), add(x, mul(0.044715, mul(x, mul(x, x))))))))`
- `fused_sdpa(Q,K,V)` → `dot_general(softmax(mul(dot_general(Q, transpose(K)), scale) + mask), V)`

### `lib/ml/src/graph/autodiff.zig` — Reverse-mode automatic differentiation
- VJP (vector-Jacobian product) function registered per primitive OpCode via comptime array
- `gradient(graph, builder, loss_node, param_nodes) -> []NodeId`
- Walk graph backward from loss, accumulate gradients per the chain rule
- Fused ops differentiate through their `vjp_alternate` decomposed subgraph (GoMLX's proven pattern — no hand-written VJPs for fused ops)

### `lib/ml/src/context.zig` — Variable/Context for training (GoMLX pattern)
- Scoped namespace for model variables (weights) with hierarchical naming (`model.layers.0.attn.q_proj`)
- Variable initialization strategies (xavier, kaiming, zeros, etc.)
- Train/eval/inference mode flags (affects dropout, batch norm behavior)
- Checkpoint save/load interface

**Minimum viable training subset (VJPs for):**
- `add`, `mul`, `sub`, `div`, `dot_general`, `transpose`, `reshape`
- `reduce_sum`, `reduce_mean`, `exp`, `rsqrt`, `tanh`, `erf`
- `gather` (for embeddings)
- This covers LoRA-style fine-tuning (FFN + norm layers)
- Attention VJPs added later

**Verification:** Numerical gradient checking: `(f(x+eps) - f(x-eps)) / 2*eps` vs AD gradient.

---

## File Organization

```
lib/ml/                         -- Reusable ML library (backend-agnostic)
  src/
    root.zig                    -- Library module root
    context.zig                 -- Variable/Context for training (Phase 6)
    graph/
      root.zig                  -- Graph module root
      shape.zig                 -- Shape, DType, ConstantCache
      node.zig                  -- Node, NodeId, OpCode (fused + primitive)
      graph.zig                 -- Graph container (append-only DAG)
      builder.zig               -- Builder API (emits fused + decomposed shadow)
      lower.zig                 -- Fused op → primitive decomposition rules
      autodiff.zig              -- Reverse-mode AD with VJP table (Phase 6)
      passes/
        root.zig
        dce.zig                 -- Dead code elimination
        memory.zig              -- Liveness analysis, buffer reuse
        fuse.zig                -- Cross-op fusion patterns

src/graph/                      -- Antfly inference-specific bridge (imports lib/ml)
  root.zig                      -- Bridge module root
  tracing_compute.zig           -- ComputeBackend VTable impl that builds graph
  interpreter.zig               -- Execute graph via real ComputeBackend
  cache.zig                     -- JIT cache keyed on input shapes
```

## Critical Files

| File | Role |
|------|------|
| `src/ops/ops.zig:110-345` | ComputeBackend VTable — the tracing target |
| `src/architectures/gpt.zig:92-114` | GPT forward pass — primary test subject |
| `src/ops/blas_compute.zig` | BLAS backend — reference interpreter target |
| `src/ops/mlx_compute.zig` | MLX backend — fused kernel targets for optimization |
| `src/pipelines/generation.zig` | Generation pipeline — integration point for graph mode |
| `src/backends/backends.zig:17-21` | BackendKind enum — needs `graph` variant |
| `build.zig` | Build system — add `lib/ml` and `src/graph` to module resolution |

## Phase Dependencies

```
Phase 0 (data structures)  ─┬─→ Phase 1 (tracing) ──┬─→ Phase 3 (stateful) ──→ Phase 4 (cache)
                             │                        │
                             └─→ Phase 2 (interpreter)┘─→ Phase 5 (optimization)
                             │
                             └─→ Phase 6 (autodiff) — mostly independent, needs Phase 2 for testing
```

Phases 0+1+2 are the foundation. After that, Phases 3-6 can proceed somewhat independently.

## Verification Strategy

1. **Phase 0:** Unit tests for shape inference per op type
2. **Phase 1:** Trace GPT/BERT forward pass, assert graph structure matches expected node sequence
3. **Phase 2:** Bit-exact comparison: `eager(model) == interpret(trace(model))` for all architectures
4. **Phase 3:** Full generation with paged KV cache through graph path, token-by-token match vs eager
5. **Phase 4:** Benchmark token/s, cache hit rate metrics, no regression vs eager
6. **Phase 5:** Bit-exact outputs after optimization; benchmark speedup
7. **Phase 6:** Numerical gradient checking for each VJP rule
