// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Architecture-agnostic "Phase R3" training harness.
//
// Given a user-supplied forward-graph builder callback, this harness:
//   1. Lazily constructs a forward graph via the callback.
//   2. Injects LoRA adapters for matching linear layers (lora_mod.injectLoRA).
//   3. Runs ml.graph.autodiff.gradient to build a backward graph covering
//      every LoRA parameter.
//   4. On each step, runs the combined forward+backward through a real
//      ComputeBackend via the existing graph/training.zig `trainStep`
//      helper, which in turn calls the eager graph interpreter.
//   5. Reads the loss and per-parameter gradients out of the execution
//      result and drives AdamW updates over the LoRA parameter slices
//      owned by this trainer.
//
// The harness is architecture agnostic: BERT/Qwen2/LayoutLMv3 all plug in
// via the BuildForwardFn / BuildLossFn callback pair. Input placeholders
// are created by the harness (on first step) and fed into the callback so
// the user code only constructs the forward DAG.
//
// Optional Hypura `TrainingMemoryCoordinator` integration registers each
// LoRA gradient block with the residency tracker, pins all blocks for the
// duration of the step, and unpins after the optimizer step. A rough
// activation budget is reserved at the start of every step.

const std = @import("std");
const ml = @import("ml");
const platform = @import("antfly_platform");

const Graph = ml.graph.Graph;
const Builder = ml.graph.Builder;
const NodeId = ml.graph.NodeId;
const null_node = ml.graph.null_node;
const Shape = ml.graph.Shape;
const DType = ml.graph.shape.DType;
const lora_mod = ml.graph.lora;
const autodiff = ml.graph.autodiff;
const optimizers = ml.graph.optimizers;

const ops_mod = @import("../ops/ops.zig");
const CT = ops_mod.CT;
const ComputeBackend = ops_mod.ComputeBackend;
const build_options = @import("build_options");
const metal_compute = if (build_options.enable_metal) @import("../ops/metal_compute.zig") else struct {
    pub const MetalCompute = opaque {};
};
const mlx_compute = if (build_options.enable_mlx) @import("../ops/mlx_compute.zig") else struct {
    pub const MlxCompute = opaque {};
};

const training = @import("../graph/training.zig");

const coord_mod = @import("training_memory_coordinator.zig");
const TrainingMemoryCoordinator = coord_mod.TrainingMemoryCoordinator;
const grad_residency = @import("grad_residency.zig");
const compat = @import("../io/compat.zig");
const graph_input_binder = @import("graph_input_binder.zig");
const graph_weight_bridge = @import("graph_weight_bridge.zig");

// ── Public callbacks ─────────────────────────────────────────────────────────

/// Architecture-agnostic forward graph builder callback.
///
/// Given a fresh Builder and the per-step input shapes, construct the forward
/// pass through the architecture. Return the NodeId of the final output
/// (typically a [batch, seq, hidden] hidden state or [batch, vocab] logits
/// tensor, depending on the task). Input placeholders are created by the
/// caller and passed in — the callback just consumes them and wires the
/// graph.
pub const BuildForwardFn = *const fn (
    ctx: *anyopaque,
    bld: *Builder,
    input_ids: NodeId,
    attention_mask: NodeId,
    batch: u32,
    seq_len: u32,
) anyerror!NodeId;

/// Task-specific loss builder. Called after `buildForward` with the forward's
/// output and the target tensor. Returns a scalar loss NodeId. Typical:
///   - classification: `b.crossEntropyLoss(logits, targets)`
///   - regression / score-matching: `b.mseLoss(output, targets)`
pub const BuildLossFn = *const fn (
    ctx: *anyopaque,
    bld: *Builder,
    forward_output: NodeId,
    targets: NodeId,
) anyerror!NodeId;

// ── Distributed gradient reduction hook ─────────────────────────────────────

/// A single LoRA parameter's gradient block, handed to the distributed
/// reduction hook. `data` is the in-place mutable accumulator that the hook
/// is expected to all-reduce across ranks.
pub const GradBlock = struct {
    name: []const u8,
    data: []f32,
};

/// Device-resident gradient block handed to `ReduceDeviceGradsFn`. `data`
/// is the mutable backend tensor for the accumulated gradient. Hooks should
/// mutate it in place or replace its storage through backend-specific APIs.
pub const DeviceGradBlock = struct {
    name: []const u8,
    data: CT,
    elem_count: usize,
    dims: []const i32,
};

/// Optional hook called once per accumulation flush, after `grad_accum` is
/// finalized but before the optimizer step. The harness passes a slice of
/// `(name, data)` pairs — one per LoRA parameter block — and the hook is
/// expected to mutate each `data` slice in place with the all-reduced
/// gradient. When null (default), no reduction is performed (single-rank
/// training). The harness does not know about MLX / NCCL / any specific
/// transport — the caller is responsible for plumbing the real primitive.
pub const ReduceGradsFn = *const fn (
    ctx: *anyopaque,
    grads: []const GradBlock,
) anyerror!void;

/// Device-resident equivalent of `ReduceGradsFn`. This is preferred by the
/// compiled Metal path because it avoids materializing full accumulated
/// gradients on the host for distributed/custom reductions.
pub const ReduceDeviceGradsFn = *const fn (
    ctx: *anyopaque,
    cb: *const ComputeBackend,
    grads: []const DeviceGradBlock,
) anyerror!void;

// ── Config ───────────────────────────────────────────────────────────────────

pub const TrainerConfig = struct {
    /// LoRA injection config (target patterns + rank + alpha).
    lora: lora_mod.LoRAConfig,
    /// AdamW optimizer config for all LoRA parameters.
    optimizer: optimizers.AdamWConfig = .{},
    /// LR schedule.
    lr_schedule: optimizers.LearningRateSchedule = .{ .constant = 1e-4 },
    /// Gradient clipping global norm. 0 = disabled.
    max_grad_norm: f32 = 1.0,
    /// Number of micro-batches to accumulate before stepping the optimizer.
    grad_accum_steps: u32 = 1,
    /// Optional hint used when estimating the activation budget that the
    /// Hypura coordinator should reserve at the start of each step. If 0 the
    /// trainer falls back to a reasonable heuristic.
    hidden_size_hint: u32 = 0,
    num_layers_hint: u32 = 0,

    /// Initial values for the LoRA A matrices (one fan-in std-dev each).
    /// B is always zero-initialized so the LoRA path is a no-op at step 0.
    /// When `null`, A matrices are drawn from a Kaiming-scaled Gaussian.
    lora_a_init_std: f32 = 0.02,
    /// RNG seed used to initialize A matrices.
    seed: u64 = 42,
    /// Regular graph parameters that should be owned and updated by the
    /// trainer in addition to LoRA adapter parameters. Names must match
    /// parameter nodes in the post-LoRA graph. Initial values are copied
    /// from the backend weight store during graph construction and then
    /// rebound from trainer-owned buffers on each step.
    regular_trainable_params: []const []const u8 = &.{},

    /// Optional distributed gradient-reduction hook. Called exactly once per
    /// accumulation flush, immediately before the optimizer step, with a
    /// GradBlock per LoRA parameter. A typical DDP caller would wire this to
    /// `mlx_compute.allSumFloat32InPlaceOnStream` (one all-reduce per block)
    /// to fold LoRA gradients across ranks before AdamW updates.
    reduce_grads: ?ReduceGradsFn = null,
    /// Opaque context pointer forwarded to `reduce_grads` on each call.
    reduce_grads_ctx: ?*anyopaque = null,
    /// Optional device-resident gradient-reduction hook. Used by compiled
    /// Metal training when provided; unlike `reduce_grads`, this does not
    /// require full gradient accumulator downloads.
    reduce_device_grads: ?ReduceDeviceGradsFn = null,
    /// Opaque context pointer forwarded to `reduce_device_grads`.
    reduce_device_grads_ctx: ?*anyopaque = null,
    /// Execution engine for the gradient graph. The interpreter preserves
    /// historical behavior. `compiled_metal` and `compiled_mlx` cache the
    /// autodiff graph and keep trainable optimizer state on their device.
    execution_engine: TrainingExecutionEngine = .interpreter,
    /// When true, fail instead of silently falling back to interpreter if the
    /// requested compiled engine cannot be prepared.
    compiled_required: bool = false,
};

pub const TrainingExecutionEngine = enum {
    interpreter,
    compiled_metal,
    compiled_mlx,
};

// ── Step I/O ─────────────────────────────────────────────────────────────────

/// Optional hook that binds architecture-specific input placeholders
/// (position_ids, attn_bias, RoPE cos/sin, bbox components, etc.) into the
/// runtime-input map before graph execution. Without this, only the 3
/// standard placeholders (__input_ids, __attention_mask, __targets) + LoRA
/// params are bound — any extra placeholders the architecture callback
/// created would go unbound and execution would fail.
///
/// The hook receives:
///   - `ctx`:      same opaque pointer as TrainerInput.ctx
///   - `cb`:       the compute backend (for `fromFloat32Shape`)
///   - `allocator`: for scratch allocations
///   - `graph`:    the post-injection graph (to discover placeholder names)
///   - `rt_map`:   the runtime-input map to INSERT additional bindings into
///   - `batch`, `seq_len`: current step's dimensions
///
/// Architecture-specific trainers implement this using helpers from
/// `graph_input_binder.zig` (e.g. `BertPlaceholderPrep.buildPositionIds`).
pub const BindArchInputsFn = *const fn (
    ctx: *anyopaque,
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    graph: *const Graph,
    rt_map: *std.AutoHashMapUnmanaged(NodeId, CT),
    batch: u32,
    seq_len: u32,
    attention_mask: []const f32,
) anyerror!void;

pub const RemapGraphNodesFn = *const fn (
    ctx: *anyopaque,
    id_map: []const NodeId,
) anyerror!void;

pub const TrainerInput = struct {
    /// User-opaque context passed to buildForward / buildLoss.
    ctx: *anyopaque,
    build_forward: BuildForwardFn,
    build_loss: BuildLossFn,
    /// Actual [batch, seq] input IDs for this step.
    input_ids: []const i64,
    /// Actual [batch, seq] attention mask for this step.
    attention_mask: []const f32,
    /// Actual target tensor for the loss (shape/contents task-specific).
    targets: []const f32,
    targets_shape: Shape,
    batch: u32,
    seq_len: u32,
    /// Optional architecture-specific input binder. When non-null, called
    /// during `step()` after binding the 3 standard inputs + LoRA params and
    /// before graph execution. This is how BERT position_ids, Qwen2 RoPE
    /// tables, LayoutLMv3 bbox components, etc. get wired into the graph.
    bind_arch_inputs: ?BindArchInputsFn = null,
    /// Optional callback invoked if the trainer reorders the graph after
    /// construction, so architecture-specific cached NodeIds can be remapped.
    remap_graph_nodes: ?RemapGraphNodesFn = null,
};

pub const StepResult = struct {
    loss: f32,
    grad_norm: f32,
    step: u64,
    /// True if this step actually dispatched an optimizer update (i.e. the
    /// gradient-accumulation window closed). False when the step only
    /// accumulated gradients for a later flush.
    optimizer_stepped: bool,
    profile: StepProfile = .{},
};

pub const StepProfile = struct {
    graph_build_ns: u64 = 0,
    runtime_input_ns: u64 = 0,
    train_step_ns: u64 = 0,
    compile_ns: u64 = 0,
    autodiff_ns: u64 = 0,
    execute_ns: u64 = 0,
    extract_ns: u64 = 0,
    optimizer_update_ns: u64 = 0,
    device_optimizer_ns: u64 = 0,
    total_ns: u64 = 0,
    peak_resident_bytes: usize = 0,
    optimizer_backend: OptimizerBackend = .host,
    device_resident_transfer_count: u64 = 0,
    device_trainable_bytes: usize = 0,
};

pub const OptimizerBackend = enum { host, metal, mlx };

const ExecutionMode = enum {
    train,
    eval,
};

// ── Trainer ──────────────────────────────────────────────────────────────────

pub const RealAutodiffTrainer = struct {
    allocator: std.mem.Allocator,
    compute_backend: *const ComputeBackend,
    /// Post-injection graph owned by the trainer. Built lazily on first step.
    graph_state: ?GraphState = null,
    config: TrainerConfig,
    optimizer_state: optimizers.OptimizerState,
    step_count: u64 = 0,
    /// Number of micro-batches already accumulated into `grad_accum` in the
    /// current accumulation window.
    accum_count: u32 = 0,
    device_optimizer_transfers: u64 = 0,
    device_trainable_bytes: usize = 0,
    runtime_input_cache: RuntimeInputCache = .{},
    /// Optional Hypura coordinator.
    coord: ?*TrainingMemoryCoordinator = null,
    compiled_session: ?training.CompiledTrainSession = null,

    /// Per-LoRA-parameter state. Indices match `graph_state.lora_adapter.adapters`:
    /// `lora_params[i*2]` = A matrix, `lora_params[i*2+1]` = B matrix.
    lora_params: std.ArrayListUnmanaged(ParamSlot) = .empty,
    /// Per-regular-parameter state for non-LoRA trainables such as task heads.
    regular_params: std.ArrayListUnmanaged(ParamSlot) = .empty,

    pub const ParamSlot = struct {
        /// Parameter name as stored in the graph string table (borrowed).
        name: []const u8,
        /// Owned mutable weight buffer updated by the optimizer.
        weights: []f32,
        /// Owned accumulator buffer (same length as `weights`). Holds the
        /// running mean gradient over the current accumulation window.
        grad_accum: []f32,
        /// NodeId of this parameter in `graph_state.graph`.
        node_id: NodeId,
        /// Shape used when binding the parameter tensor at runtime.
        dims: []i32,
        /// Residency-tracker block id, when the coordinator is attached.
        block_id: ?grad_residency.GradBlockId = null,
        /// True once the block has been registered with the coordinator.
        block_registered: bool = false,
        device: ?DeviceOptimizerSlot = null,
    };

    pub const DeviceOptimizerSlot = struct {
        weight: CT,
        grad_accum: CT,
        m: CT,
        v: CT,
    };

    pub const RuntimeInputCache = struct {
        input_ids: CachedRuntimeTensor = .{},
        attention_mask: CachedRuntimeTensor = .{},
        targets: CachedRuntimeTensor = .{},
        gliner2_attn_bias: CachedRuntimeTensor = .{},
    };

    pub const CachedRuntimeTensor = struct {
        tensor: ?CT = null,
        dims: []i32 = &.{},
        elem_count: usize = 0,
    };

    pub const GraphState = struct {
        /// Immutable template graph with LoRA injected. The loss node is
        /// already part of this graph; the gradient graph is rebuilt on each
        /// call to `training.trainStep` because autodiff.gradient appends
        /// gradient nodes and we must run it against the original (loss)
        /// graph so LoRA parameter NodeIds stay stable across steps.
        graph: Graph,
        input_ids_node: NodeId,
        attention_mask_node: NodeId,
        targets_node: NodeId,
        loss_node: NodeId,
        forward_output_node: NodeId,
        lora_adapter: lora_mod.LoRAAdapter,
    };

    pub fn init(
        allocator: std.mem.Allocator,
        compute_backend: *const ComputeBackend,
        config: TrainerConfig,
    ) !RealAutodiffTrainer {
        return .{
            .allocator = allocator,
            .compute_backend = compute_backend,
            .config = config,
            .optimizer_state = optimizers.OptimizerState.init(allocator),
        };
    }

    pub fn deinit(self: *RealAutodiffTrainer) void {
        if (self.compiled_session) |*session| {
            session.deinit();
            self.compiled_session = null;
        }
        if (self.graph_state) |*gs| {
            gs.graph.deinit();
            gs.lora_adapter.deinit();
            self.graph_state = null;
        }
        for (self.lora_params.items) |*slot| {
            self.deinitDeviceOptimizerSlot(slot);
            self.allocator.free(slot.weights);
            self.allocator.free(slot.grad_accum);
            self.allocator.free(slot.dims);
        }
        self.lora_params.deinit(self.allocator);
        for (self.regular_params.items) |*slot| {
            self.deinitDeviceOptimizerSlot(slot);
            self.allocator.free(slot.weights);
            self.allocator.free(slot.grad_accum);
            self.allocator.free(slot.dims);
        }
        self.regular_params.deinit(self.allocator);
        self.deinitRuntimeInputCache();
        self.optimizer_state.deinit();
        self.* = undefined;
    }

    /// Optional: attach a Hypura coordinator for memory-aware training.
    pub fn attachCoordinator(self: *RealAutodiffTrainer, coord: *TrainingMemoryCoordinator) void {
        self.coord = coord;
    }

    /// Materialize the post-LoRA graph and parameter slots without running
    /// an optimizer step. This lets higher-level trainers seed LoRA weights
    /// from an external adapter checkpoint before training begins.
    pub fn ensureGraphBuilt(self: *RealAutodiffTrainer, input: TrainerInput) !void {
        if (self.graph_state == null) {
            compiledDiag(
                "graph build begin batch={} seq_len={} targets={} rss={}",
                .{ input.batch, input.seq_len, input.targets.len, currentResidentBytes() },
            );
            try self.buildGraphState(input);
            const gs = &self.graph_state.?;
            compiledDiag(
                "graph build done nodes={} params={} lora_slots={} regular_slots={} rss={}",
                .{
                    gs.graph.nodeCount(),
                    gs.graph.parameters.items.len,
                    self.lora_params.items.len,
                    self.regular_params.items.len,
                    currentResidentBytes(),
                },
            );
        }
    }

    pub fn ensureCompiledSessionBuilt(self: *RealAutodiffTrainer, trainable: []const []const u8) !bool {
        if (self.config.execution_engine == .interpreter) return false;
        if (self.config.execution_engine == .compiled_metal and self.compute_backend.kind() != .metal) {
            if (self.config.compiled_required) return error.CompiledMetalRequiresMetalBackend;
            return false;
        }
        if (self.config.execution_engine == .compiled_mlx and self.compute_backend.kind() != .mlx) {
            if (self.config.compiled_required) return error.CompiledMlxRequiresMlxBackend;
            return false;
        }
        if (self.compiled_session == null) {
            const gs = &(self.graph_state orelse return error.GraphNotBuilt);
            compiledDiag(
                "compiled session build begin trainable={} graph_nodes={} rss={}",
                .{ trainable.len, gs.graph.nodeCount(), currentResidentBytes() },
            );
            self.compiled_session = try training.CompiledTrainSession.init(
                self.allocator,
                &gs.graph,
                gs.loss_node,
                .{ .trainable_params = trainable },
            );
            compiledDiag(
                "compiled session build done compiled_nodes={} outputs={} compile_ms={d:.3} peak_rss={}",
                .{
                    self.compiled_session.?.graph.nodeCount(),
                    self.compiled_session.?.graph.outputs.items.len,
                    nsToMs(self.compiled_session.?.build_profile.total_ns),
                    self.compiled_session.?.build_profile.peak_resident_bytes,
                },
            );
        }
        return true;
    }

    /// Run one training step.
    pub fn step(self: *RealAutodiffTrainer, input: TrainerInput) !StepResult {
        return self.runStep(input, .train);
    }

    pub fn evaluate(self: *RealAutodiffTrainer, input: TrainerInput) !StepResult {
        return self.runStep(input, .eval);
    }

    fn runStep(self: *RealAutodiffTrainer, input: TrainerInput, mode: ExecutionMode) !StepResult {
        const total_start_ns = monotonicNowNs();
        var profile = StepProfile{};

        // 1. Lazy graph construction.
        const graph_build_start_ns = monotonicNowNs();
        try self.ensureGraphBuilt(input);
        profile.graph_build_ns = elapsedNs(graph_build_start_ns, monotonicNowNs());
        const gs = &self.graph_state.?;
        const use_device_optimizer = mode == .train and self.deviceOptimizerRequested();
        const use_cached_runtime_inputs = use_device_optimizer and self.compute_backend.kind() == .metal;
        if (use_device_optimizer) {
            compiledDiag(
                "device optimizer slots begin lora_slots={} regular_slots={} rss={}",
                .{ self.lora_params.items.len, self.regular_params.items.len, currentResidentBytes() },
            );
            try self.ensureDeviceOptimizerSlots();
            compiledDiag(
                "device optimizer slots done trainable_bytes={} transfers={} rss={}",
                .{ self.device_trainable_bytes, self.device_optimizer_transfers, currentResidentBytes() },
            );
            profile.optimizer_backend = self.deviceOptimizerBackend();
            profile.device_resident_transfer_count = self.device_optimizer_transfers;
            profile.device_trainable_bytes = self.device_trainable_bytes;
        }

        // 2. Optional activation-budget reservation via the coordinator.
        if (self.coord != null) {
            try self.reserveActivationBudget(input);
            try self.pinAllLoraBlocks();
        }

        // 3. Build the runtime input map: input_ids + mask + targets + all
        //    LoRA parameter tensors. The underlying interpreter will free the
        //    CT handles after execution — we re-upload them on every step.
        const runtime_input_start_ns = monotonicNowNs();
        var rt = std.AutoHashMapUnmanaged(NodeId, CT).empty;
        var borrowed_rt = std.AutoHashMapUnmanaged(NodeId, void).empty;
        defer {
            var it = rt.iterator();
            while (it.next()) |e| {
                if (!borrowed_rt.contains(e.key_ptr.*)) self.compute_backend.free(e.value_ptr.*);
            }
            borrowed_rt.deinit(self.allocator);
            rt.deinit(self.allocator);
        }

        // input_ids as f32 (graph represents them as f32 placeholders to
        // match the Builder parameter API; the callback is free to gather
        // from an embedding table inside the forward pass).
        const input_ids_f32 = try self.allocator.alloc(f32, input.input_ids.len);
        defer self.allocator.free(input_ids_f32);
        for (input.input_ids, 0..) |id, i| input_ids_f32[i] = @floatFromInt(id);

        const ids_dims = [_]i32{ @intCast(input.batch), @intCast(input.seq_len) };
        if (use_cached_runtime_inputs) {
            try self.putCachedRuntimeInput(&rt, &borrowed_rt, &self.runtime_input_cache.input_ids, gs.input_ids_node, input_ids_f32, &ids_dims);
            try self.putCachedRuntimeInput(&rt, &borrowed_rt, &self.runtime_input_cache.attention_mask, gs.attention_mask_node, input.attention_mask, &ids_dims);
        } else {
            try putRuntimeInput(self.allocator, self.compute_backend, &rt, gs.input_ids_node, input_ids_f32, &ids_dims);
            try putRuntimeInput(self.allocator, self.compute_backend, &rt, gs.attention_mask_node, input.attention_mask, &ids_dims);
        }

        const target_dims = try shapeToDims(self.allocator, input.targets_shape);
        defer self.allocator.free(target_dims);
        if (use_cached_runtime_inputs) {
            try self.putCachedRuntimeInput(&rt, &borrowed_rt, &self.runtime_input_cache.targets, gs.targets_node, input.targets, target_dims);
        } else {
            try putRuntimeInput(self.allocator, self.compute_backend, &rt, gs.targets_node, input.targets, target_dims);
        }

        // LoRA parameter slices.
        for (self.lora_params.items) |*slot| {
            if (slot.device) |device| {
                try rt.put(self.allocator, slot.node_id, device.weight);
                try borrowed_rt.put(self.allocator, slot.node_id, {});
            } else {
                try putRuntimeInput(self.allocator, self.compute_backend, &rt, slot.node_id, slot.weights, slot.dims);
            }
        }
        for (self.regular_params.items) |*slot| {
            if (slot.device) |device| {
                try rt.put(self.allocator, slot.node_id, device.weight);
                try borrowed_rt.put(self.allocator, slot.node_id, {});
            } else {
                try putRuntimeInput(self.allocator, self.compute_backend, &rt, slot.node_id, slot.weights, slot.dims);
            }
        }

        if (use_cached_runtime_inputs) {
            try self.putKnownCachedArchInputs(&rt, &borrowed_rt, &gs.graph, input.batch, input.seq_len, input.attention_mask);
        }

        // 3b. Architecture-specific input binding. This is how BERT
        //     position_ids, Qwen2 RoPE tables, LayoutLMv3 bbox components,
        //     etc. get into the runtime map. Without this, only the 3
        //     standard placeholders + LoRA params are bound.
        if (input.bind_arch_inputs) |bind_fn| {
            if (!use_cached_runtime_inputs or self.hasUnboundRuntimePlaceholders(&gs.graph, &rt)) {
                try bind_fn(input.ctx, self.compute_backend, self.allocator, &gs.graph, &rt, input.batch, input.seq_len, input.attention_mask);
            }
        }
        profile.runtime_input_ns = elapsedNs(runtime_input_start_ns, monotonicNowNs());
        if (use_device_optimizer) {
            compiledDiag(
                "runtime inputs ready entries={} borrowed={} runtime_ms={d:.3} transfers={} rss={}",
                .{ rt.count(), borrowed_rt.count(), nsToMs(profile.runtime_input_ns), self.device_optimizer_transfers, currentResidentBytes() },
            );
        }

        // 4. Run the graph training step (autodiff + execute + extract).
        if (mode == .train) {
            self.optimizer_state.step_count = @intCast(self.step_count + 1);
        }

        // Collect the list of trainer-owned parameter names we want
        // gradients for: LoRA adapters plus explicitly enrolled regular
        // trainables such as task heads.
        var trainable = try self.allocator.alloc([]const u8, self.lora_params.items.len + self.regular_params.items.len);
        defer self.allocator.free(trainable);
        var trainable_idx: usize = 0;
        for (self.lora_params.items) |slot| {
            trainable[trainable_idx] = slot.name;
            trainable_idx += 1;
        }
        for (self.regular_params.items) |slot| {
            trainable[trainable_idx] = slot.name;
            trainable_idx += 1;
        }

        const train_step_start_ns = monotonicNowNs();
        const use_compiled = try self.ensureCompiledSessionBuilt(trainable);
        if (use_compiled) {
            compiledDiag(
                "compiled execute dispatch device_optimizer={} trainable={} runtime_inputs={} rss={}",
                .{ use_device_optimizer, trainable.len, rt.count(), currentResidentBytes() },
            );
        }
        var step_result = if (use_compiled and use_device_optimizer)
            try self.compiled_session.?.executeDeviceGradients(self.compute_backend, rt)
        else if (use_compiled)
            try self.compiled_session.?.execute(self.compute_backend, rt)
        else
            try training.trainStep(
                self.allocator,
                &gs.graph,
                gs.loss_node,
                self.compute_backend,
                rt,
                .{ .trainable_params = trainable },
            );
        profile.train_step_ns = elapsedNs(train_step_start_ns, monotonicNowNs());
        if (use_compiled) {
            compiledDiag(
                "compiled execute complete train_step_ms={d:.3} loss={d:.6} rss={}",
                .{ nsToMs(profile.train_step_ns), step_result.loss, currentResidentBytes() },
            );
        }
        profile.autodiff_ns = if (use_compiled) 0 else step_result.profile.autodiff_ns;
        profile.compile_ns = if (use_compiled) self.compiled_session.?.build_profile.total_ns else 0;
        profile.execute_ns = step_result.profile.execute_ns;
        profile.extract_ns = step_result.profile.extract_ns;
        profile.peak_resident_bytes = step_result.profile.peak_resident_bytes;
        defer step_result.deinit();

        const loss_value = step_result.loss;

        var grad_norm: f32 = 0.0;
        var stepped = false;
        const optimizer_update_start_ns = monotonicNowNs();
        switch (mode) {
            .train => {
                // 5. Accumulate gradients (mean over the accumulation window).
                const accum_steps: u32 = @max(self.config.grad_accum_steps, 1);
                const scale: f32 = 1.0 / @as(f32, @floatFromInt(accum_steps));
                for (self.lora_params.items) |*slot| {
                    if (use_device_optimizer) {
                        const g_ct = step_result.device_gradients.get(slot.name) orelse return error.MissingTrainableGradient;
                        try self.accumulateDeviceGradientCt(slot, g_ct, scale, self.accum_count == 0);
                        continue;
                    }
                    const g = step_result.gradients.get(slot.name) orelse return error.MissingTrainableGradient;
                    if (g.len != slot.grad_accum.len) return error.GradientShapeMismatch;
                    if (self.accum_count == 0) {
                        for (slot.grad_accum, g) |*a, v| a.* = v * scale;
                    } else {
                        for (slot.grad_accum, g) |*a, v| a.* += v * scale;
                    }
                }
                for (self.regular_params.items) |*slot| {
                    if (use_device_optimizer) {
                        const g_ct = step_result.device_gradients.get(slot.name) orelse return error.MissingTrainableGradient;
                        try self.accumulateDeviceGradientCt(slot, g_ct, scale, self.accum_count == 0);
                        continue;
                    }
                    const g = step_result.gradients.get(slot.name) orelse return error.MissingTrainableGradient;
                    if (g.len != slot.grad_accum.len) return error.GradientShapeMismatch;
                    if (self.accum_count == 0) {
                        for (slot.grad_accum, g) |*a, v| a.* = v * scale;
                    } else {
                        for (slot.grad_accum, g) |*a, v| a.* += v * scale;
                    }
                }
                self.accum_count += 1;

                // 6. If the accumulation window is full, clip + step the optimizer.
                if (self.accum_count >= accum_steps) {
                    if (use_device_optimizer and self.config.reduce_device_grads != null) {
                        try self.reduceDeviceGradAccum();
                    } else if (self.config.reduce_grads) |reduce_fn| {
                        if (use_device_optimizer) try self.syncDeviceGradAccumToHost();
                        var blocks = try self.allocator.alloc(GradBlock, self.lora_params.items.len + self.regular_params.items.len);
                        defer self.allocator.free(blocks);
                        var block_idx: usize = 0;
                        for (self.lora_params.items) |*slot| {
                            blocks[block_idx] = .{ .name = slot.name, .data = slot.grad_accum };
                            block_idx += 1;
                        }
                        for (self.regular_params.items) |*slot| {
                            blocks[block_idx] = .{ .name = slot.name, .data = slot.grad_accum };
                            block_idx += 1;
                        }
                        const ctx = self.config.reduce_grads_ctx orelse @as(*anyopaque, @ptrFromInt(@alignOf(usize)));
                        try reduce_fn(ctx, blocks);
                        if (use_device_optimizer) try self.replaceDeviceGradAccumFromHost();
                    }

                    grad_norm = if (use_device_optimizer)
                        try self.deviceGlobalGradNorm()
                    else
                        self.globalGradNorm();
                    if (!use_device_optimizer and self.config.max_grad_norm > 0.0 and grad_norm > self.config.max_grad_norm) {
                        const clip = self.config.max_grad_norm / (grad_norm + 1e-6);
                        for (self.lora_params.items) |*slot| {
                            for (slot.grad_accum) |*v| v.* *= clip;
                        }
                        for (self.regular_params.items) |*slot| {
                            for (slot.grad_accum) |*v| v.* *= clip;
                        }
                    }

                    const lr = self.config.lr_schedule.lr(@intCast(self.step_count));
                    if (use_device_optimizer) {
                        const device_opt_start_ns = monotonicNowNs();
                        const clip_scale = if (self.config.max_grad_norm > 0.0 and grad_norm > self.config.max_grad_norm)
                            self.config.max_grad_norm / (grad_norm + 1e-6)
                        else
                            1.0;
                        try self.stepDeviceAdamW(lr, clip_scale);
                        profile.device_optimizer_ns = elapsedNs(device_opt_start_ns, monotonicNowNs());
                        profile.optimizer_backend = self.deviceOptimizerBackend();
                    } else {
                        const opt_config = optimizers.Optimizer{ .adamw = self.config.optimizer };
                        for (self.lora_params.items) |*slot| {
                            try optimizers.step(
                                opt_config,
                                &self.optimizer_state,
                                lr,
                                slot.name,
                                slot.weights,
                                slot.grad_accum,
                            );
                            @memset(slot.grad_accum, 0);
                        }
                        for (self.regular_params.items) |*slot| {
                            try optimizers.step(
                                opt_config,
                                &self.optimizer_state,
                                lr,
                                slot.name,
                                slot.weights,
                                slot.grad_accum,
                            );
                            @memset(slot.grad_accum, 0);
                        }
                    }
                    self.accum_count = 0;
                    stepped = true;
                }
            },
            .eval => {
                grad_norm = try self.gradientNormFromResult(&step_result);
            },
        }
        profile.optimizer_update_ns = elapsedNs(optimizer_update_start_ns, monotonicNowNs());

        // 7. Release pinned LoRA blocks.
        if (self.coord != null) {
            try self.unpinAllLoraBlocks();
        }

        if (mode == .train) self.step_count += 1;
        profile.device_resident_transfer_count = self.device_optimizer_transfers;
        profile.device_trainable_bytes = self.device_trainable_bytes;
        profile.total_ns = elapsedNs(total_start_ns, monotonicNowNs());
        return .{
            .loss = loss_value,
            .grad_norm = grad_norm,
            .step = self.step_count,
            .optimizer_stepped = stepped,
            .profile = profile,
        };
    }

    /// Save LoRA adapter weights to the given directory.
    ///
    /// Writes one `.bin` file per adapter parameter containing a tiny header
    /// plus raw f32 data. This is intentionally a dumb format — callers
    /// needing HF/safetensors compatibility should build on top of this.
    ///
    /// File layout (all little-endian):
    ///     "LORA"         (4 bytes magic)
    ///     ndims          (u32)
    ///     dims[ndims]    (i32 each)
    ///     n_elements     (u64)
    ///     raw f32 data   (n_elements * 4 bytes)
    pub fn saveAdapters(self: *const RealAutodiffTrainer, out_dir: []const u8) !void {
        try compat.cwd().createDirPath(compat.io(), out_dir);
        for (self.lora_params.items) |slot| {
            const file_name = try std.fmt.allocPrint(self.allocator, "{s}/{s}.bin", .{ out_dir, slot.name });
            defer self.allocator.free(file_name);

            // Build the payload in memory then write in one call — keeps us
            // clear of the 0.16 streaming writer API differences.
            var buf = std.ArrayListUnmanaged(u8).empty;
            defer buf.deinit(self.allocator);

            try buf.appendSlice(self.allocator, "LORA");

            var u32_buf: [4]u8 = undefined;
            std.mem.writeInt(u32, &u32_buf, @intCast(slot.dims.len), .little);
            try buf.appendSlice(self.allocator, &u32_buf);

            for (slot.dims) |d| {
                var i32_buf: [4]u8 = undefined;
                std.mem.writeInt(i32, &i32_buf, d, .little);
                try buf.appendSlice(self.allocator, &i32_buf);
            }

            var u64_buf: [8]u8 = undefined;
            std.mem.writeInt(u64, &u64_buf, slot.weights.len, .little);
            try buf.appendSlice(self.allocator, &u64_buf);

            // Raw f32 data. Correct on little-endian hosts; big-endian hosts
            // (none in our target set) would need a per-element swap.
            try buf.appendSlice(self.allocator, std.mem.sliceAsBytes(slot.weights));

            try compat.cwd().writeFile(compat.io(), .{
                .sub_path = file_name,
                .data = buf.items,
            });
        }
    }

    pub fn syncDeviceTrainablesToHost(self: *RealAutodiffTrainer) !void {
        for (self.lora_params.items) |*slot| try self.syncDeviceSlotToHost(slot);
        for (self.regular_params.items) |*slot| try self.syncDeviceSlotToHost(slot);
    }

    // ── Internals ────────────────────────────────────────────────────────

    fn metalCompute(self: *RealAutodiffTrainer) !*metal_compute.MetalCompute {
        if (comptime !build_options.enable_metal) return error.MetalBackendUnavailable;
        if (self.compute_backend.kind() != .metal) return error.MetalBackendUnavailable;
        return @ptrCast(@alignCast(self.compute_backend.ptr));
    }

    fn mlxCompute(self: *RealAutodiffTrainer) !*mlx_compute.MlxCompute {
        if (comptime !build_options.enable_mlx) return error.MlxBackendUnavailable;
        if (self.compute_backend.kind() != .mlx) return error.MlxBackendUnavailable;
        return @ptrCast(@alignCast(self.compute_backend.ptr));
    }

    fn deviceOptimizerRequested(self: *const RealAutodiffTrainer) bool {
        return switch (self.config.execution_engine) {
            .interpreter => false,
            .compiled_metal => self.compute_backend.kind() == .metal,
            .compiled_mlx => self.compute_backend.kind() == .mlx,
        };
    }

    fn deviceOptimizerBackend(self: *const RealAutodiffTrainer) OptimizerBackend {
        return switch (self.compute_backend.kind()) {
            .metal => .metal,
            .mlx => .mlx,
            else => .host,
        };
    }

    fn putCachedRuntimeInput(
        self: *RealAutodiffTrainer,
        rt: *std.AutoHashMapUnmanaged(NodeId, CT),
        borrowed_rt: *std.AutoHashMapUnmanaged(NodeId, void),
        cache: *CachedRuntimeTensor,
        node_id: NodeId,
        data: []const f32,
        dims: []const i32,
    ) !void {
        const ct = try self.ensureCachedRuntimeTensor(cache, data, dims);
        try rt.put(self.allocator, node_id, ct);
        try borrowed_rt.put(self.allocator, node_id, {});
    }

    fn ensureCachedRuntimeTensor(
        self: *RealAutodiffTrainer,
        cache: *CachedRuntimeTensor,
        data: []const f32,
        dims: []const i32,
    ) !CT {
        if (comptime !build_options.enable_metal) return error.MetalBackendUnavailable;
        const metal = try self.metalCompute();
        if (cache.tensor) |ct| {
            if (cache.elem_count == data.len and std.mem.eql(i32, cache.dims, dims)) {
                try metal.trainingOverwriteF32(ct, data, dims);
                return ct;
            }
            self.compute_backend.free(ct);
            cache.tensor = null;
            if (cache.dims.len > 0) self.allocator.free(cache.dims);
            cache.dims = &.{};
            cache.elem_count = 0;
        }
        const ct = try metal.trainingUploadF32(data, dims);
        errdefer self.compute_backend.free(ct);
        cache.dims = try self.allocator.dupe(i32, dims);
        cache.elem_count = data.len;
        cache.tensor = ct;
        return ct;
    }

    fn deinitRuntimeInputCache(self: *RealAutodiffTrainer) void {
        self.deinitCachedRuntimeTensor(&self.runtime_input_cache.input_ids);
        self.deinitCachedRuntimeTensor(&self.runtime_input_cache.attention_mask);
        self.deinitCachedRuntimeTensor(&self.runtime_input_cache.targets);
        self.deinitCachedRuntimeTensor(&self.runtime_input_cache.gliner2_attn_bias);
    }

    fn deinitCachedRuntimeTensor(self: *RealAutodiffTrainer, cache: *CachedRuntimeTensor) void {
        if (cache.tensor) |ct| self.compute_backend.free(ct);
        if (cache.dims.len > 0) self.allocator.free(cache.dims);
        cache.* = .{};
    }

    fn putKnownCachedArchInputs(
        self: *RealAutodiffTrainer,
        rt: *std.AutoHashMapUnmanaged(NodeId, CT),
        borrowed_rt: *std.AutoHashMapUnmanaged(NodeId, void),
        graph: *const Graph,
        batch: u32,
        seq_len: u32,
        attention_mask: []const f32,
    ) !void {
        if (graph_weight_bridge.findParameterByName(graph, "__gliner2_attn_bias")) |node_id| {
            if (rt.contains(node_id)) return;
            const node = graph.node(node_id);
            if (node.output_shape.rank() != 3) return error.InvalidTensorShape;
            const dim0 = node.output_shape.dim(0);
            const dim1 = node.output_shape.dim(1);
            const dim2 = node.output_shape.dim(2);
            if (dim1 != seq_len or dim2 != seq_len) return error.InvalidTensorShape;
            const batch_i64: i64 = @intCast(batch);
            if (batch_i64 == 0 or @mod(dim0, batch_i64) != 0) return error.InvalidTensorShape;
            const num_heads: u32 = @intCast(@divExact(dim0, batch_i64));
            const bias = try graph_input_binder.BertPlaceholderPrep.buildAttnBias(
                self.allocator,
                attention_mask,
                batch,
                seq_len,
                num_heads,
            );
            defer self.allocator.free(bias);
            var dims = [_]i32{ @intCast(dim0), @intCast(dim1), @intCast(dim2) };
            try self.putCachedRuntimeInput(
                rt,
                borrowed_rt,
                &self.runtime_input_cache.gliner2_attn_bias,
                node_id,
                bias,
                &dims,
            );
        }
    }

    fn hasUnboundRuntimePlaceholders(
        self: *RealAutodiffTrainer,
        graph: *const Graph,
        rt: *const std.AutoHashMapUnmanaged(NodeId, CT),
    ) bool {
        _ = self;
        for (graph.parameters.items) |param_id| {
            const node = graph.node(param_id);
            if (node.op != .parameter) continue;
            const name = graph.parameterName(node);
            if (name.len < 2 or name[0] != '_' or name[1] != '_') continue;
            if (!rt.contains(param_id)) return true;
        }
        return false;
    }

    fn ensureDeviceOptimizerSlots(self: *RealAutodiffTrainer) !void {
        switch (self.compute_backend.kind()) {
            .metal => {
                if (comptime !build_options.enable_metal) return error.MetalBackendUnavailable;
                const metal = try self.metalCompute();
                for (self.lora_params.items) |*slot| try self.ensureMetalDeviceOptimizerSlot(metal, slot);
                for (self.regular_params.items) |*slot| try self.ensureMetalDeviceOptimizerSlot(metal, slot);
            },
            .mlx => {
                if (comptime !build_options.enable_mlx) return error.MlxBackendUnavailable;
                const mlx = try self.mlxCompute();
                for (self.lora_params.items) |*slot| try self.ensureMlxDeviceOptimizerSlot(mlx, slot);
                for (self.regular_params.items) |*slot| try self.ensureMlxDeviceOptimizerSlot(mlx, slot);
            },
            else => return error.DeviceOptimizerBackendUnavailable,
        }
    }

    fn ensureMetalDeviceOptimizerSlot(self: *RealAutodiffTrainer, metal: *metal_compute.MetalCompute, slot: *ParamSlot) !void {
        if (slot.device != null) return;
        const weight = try metal.trainingUploadF32(slot.weights, slot.dims);
        errdefer self.compute_backend.free(weight);
        const grad_accum = try metal.trainingZeroF32(slot.weights.len, slot.dims);
        errdefer self.compute_backend.free(grad_accum);
        const m = try metal.trainingZeroF32(slot.weights.len, slot.dims);
        errdefer self.compute_backend.free(m);
        const v = try metal.trainingZeroF32(slot.weights.len, slot.dims);
        errdefer self.compute_backend.free(v);
        slot.device = .{
            .weight = weight,
            .grad_accum = grad_accum,
            .m = m,
            .v = v,
        };
        self.device_trainable_bytes += slot.weights.len * @sizeOf(f32) * 4;
    }

    fn ensureMlxDeviceOptimizerSlot(self: *RealAutodiffTrainer, mlx: *mlx_compute.MlxCompute, slot: *ParamSlot) !void {
        if (slot.device != null) return;
        const weight = try mlx.trainingUploadF32(slot.weights, slot.dims);
        errdefer self.compute_backend.free(weight);
        const grad_accum = try mlx.trainingZeroF32(slot.weights.len, slot.dims);
        errdefer self.compute_backend.free(grad_accum);
        const m = try mlx.trainingZeroF32(slot.weights.len, slot.dims);
        errdefer self.compute_backend.free(m);
        const v = try mlx.trainingZeroF32(slot.weights.len, slot.dims);
        errdefer self.compute_backend.free(v);
        slot.device = .{
            .weight = weight,
            .grad_accum = grad_accum,
            .m = m,
            .v = v,
        };
        self.device_trainable_bytes += slot.weights.len * @sizeOf(f32) * 4;
    }

    fn deinitDeviceOptimizerSlot(self: *RealAutodiffTrainer, slot: *ParamSlot) void {
        if (slot.device) |device| {
            self.compute_backend.free(device.weight);
            self.compute_backend.free(device.grad_accum);
            self.compute_backend.free(device.m);
            self.compute_backend.free(device.v);
            slot.device = null;
        }
    }

    fn syncDeviceSlotToHost(self: *RealAutodiffTrainer, slot: *ParamSlot) !void {
        const device = slot.device orelse return;
        const weights = try self.compute_backend.toFloat32(device.weight, self.allocator);
        defer self.allocator.free(weights);
        if (weights.len != slot.weights.len) return error.TrainableParameterShapeMismatch;
        @memcpy(slot.weights, weights);
        self.device_optimizer_transfers += 1;
    }

    fn accumulateDeviceGradient(
        self: *RealAutodiffTrainer,
        slot: *ParamSlot,
        grad: []const f32,
        scale: f32,
        first: bool,
    ) !void {
        if (comptime !build_options.enable_metal) return error.MetalBackendUnavailable;
        const device = slot.device orelse return error.DeviceOptimizerNotInitialized;
        const metal = try self.metalCompute();
        const grad_ct = try metal.trainingUploadF32(grad, slot.dims);
        defer self.compute_backend.free(grad_ct);
        try metal.trainingAccumulateF32(device.grad_accum, grad_ct, slot.weights.len, scale, first);
        self.device_optimizer_transfers += 1;
    }

    fn accumulateDeviceGradientCt(
        self: *RealAutodiffTrainer,
        slot: *ParamSlot,
        grad_ct: CT,
        scale: f32,
        first: bool,
    ) !void {
        const device = slot.device orelse return error.DeviceOptimizerNotInitialized;
        switch (self.compute_backend.kind()) {
            .metal => {
                if (comptime !build_options.enable_metal) return error.MetalBackendUnavailable;
                const metal = try self.metalCompute();
                try metal.trainingAccumulateF32(device.grad_accum, grad_ct, slot.weights.len, scale, first);
            },
            .mlx => {
                if (comptime !build_options.enable_mlx) return error.MlxBackendUnavailable;
                const mlx = try self.mlxCompute();
                const next = try mlx.trainingAccumulateF32Replace(device.grad_accum, grad_ct, scale, first);
                self.compute_backend.free(device.grad_accum);
                slot.device.?.grad_accum = next;
            },
            else => return error.DeviceOptimizerBackendUnavailable,
        }
    }

    fn syncDeviceGradAccumToHost(self: *RealAutodiffTrainer) !void {
        for (self.lora_params.items) |*slot| try self.syncDeviceGradAccumSlotToHost(slot);
        for (self.regular_params.items) |*slot| try self.syncDeviceGradAccumSlotToHost(slot);
    }

    fn syncDeviceGradAccumSlotToHost(self: *RealAutodiffTrainer, slot: *ParamSlot) !void {
        const device = slot.device orelse return;
        const grad = try self.compute_backend.toFloat32(device.grad_accum, self.allocator);
        defer self.allocator.free(grad);
        if (grad.len != slot.grad_accum.len) return error.GradientShapeMismatch;
        @memcpy(slot.grad_accum, grad);
        self.device_optimizer_transfers += 1;
    }

    fn replaceDeviceGradAccumFromHost(self: *RealAutodiffTrainer) !void {
        for (self.lora_params.items) |*slot| try self.replaceDeviceGradAccumSlotFromHost(slot);
        for (self.regular_params.items) |*slot| try self.replaceDeviceGradAccumSlotFromHost(slot);
    }

    fn replaceDeviceGradAccumSlotFromHost(self: *RealAutodiffTrainer, slot: *ParamSlot) !void {
        if (comptime !build_options.enable_metal) return error.MetalBackendUnavailable;
        const device = slot.device orelse return;
        const metal = try self.metalCompute();
        const host_ct = try metal.trainingUploadF32(slot.grad_accum, slot.dims);
        defer self.compute_backend.free(host_ct);
        try metal.trainingAccumulateF32(device.grad_accum, host_ct, slot.weights.len, 1.0, true);
        self.device_optimizer_transfers += 1;
    }

    fn reduceDeviceGradAccum(self: *RealAutodiffTrainer) !void {
        const reduce_fn = self.config.reduce_device_grads orelse return;
        var blocks = try self.allocator.alloc(DeviceGradBlock, self.lora_params.items.len + self.regular_params.items.len);
        defer self.allocator.free(blocks);
        var block_idx: usize = 0;
        for (self.lora_params.items) |*slot| {
            const device = slot.device orelse return error.DeviceOptimizerNotInitialized;
            blocks[block_idx] = .{
                .name = slot.name,
                .data = device.grad_accum,
                .elem_count = slot.weights.len,
                .dims = slot.dims,
            };
            block_idx += 1;
        }
        for (self.regular_params.items) |*slot| {
            const device = slot.device orelse return error.DeviceOptimizerNotInitialized;
            blocks[block_idx] = .{
                .name = slot.name,
                .data = device.grad_accum,
                .elem_count = slot.weights.len,
                .dims = slot.dims,
            };
            block_idx += 1;
        }
        const ctx = self.config.reduce_device_grads_ctx orelse @as(*anyopaque, @ptrFromInt(@alignOf(usize)));
        try reduce_fn(ctx, self.compute_backend, blocks);
    }

    fn deviceGlobalGradNorm(self: *RealAutodiffTrainer) !f32 {
        var total: f64 = 0.0;
        for (self.lora_params.items) |*slot| {
            const device = slot.device orelse continue;
            const sumsq = try self.deviceSumSquares(device.grad_accum, slot.weights.len);
            total += @as(f64, sumsq);
        }
        for (self.regular_params.items) |*slot| {
            const device = slot.device orelse continue;
            const sumsq = try self.deviceSumSquares(device.grad_accum, slot.weights.len);
            total += @as(f64, sumsq);
        }
        return @floatCast(@sqrt(total));
    }

    fn deviceSumSquares(self: *RealAutodiffTrainer, input: CT, elem_count: usize) !f32 {
        return switch (self.compute_backend.kind()) {
            .metal => blk: {
                if (comptime !build_options.enable_metal) return error.MetalBackendUnavailable;
                const metal = try self.metalCompute();
                break :blk try metal.trainingSumSquaresF32(input, elem_count);
            },
            .mlx => blk: {
                if (comptime !build_options.enable_mlx) return error.MlxBackendUnavailable;
                const mlx = try self.mlxCompute();
                break :blk try mlx.trainingSumSquaresF32(input);
            },
            else => error.DeviceOptimizerBackendUnavailable,
        };
    }

    fn stepDeviceAdamW(self: *RealAutodiffTrainer, lr: f32, grad_scale: f32) !void {
        const opt = self.config.optimizer;
        const t: f32 = @floatFromInt(self.optimizer_state.step_count);
        const bias_correction1 = 1.0 - std.math.pow(f32, opt.beta1, t);
        const bias_correction2 = 1.0 - std.math.pow(f32, opt.beta2, t);
        for (self.lora_params.items) |*slot| {
            try self.stepDeviceAdamWSlot(slot, lr, grad_scale, bias_correction1, bias_correction2);
        }
        for (self.regular_params.items) |*slot| {
            try self.stepDeviceAdamWSlot(slot, lr, grad_scale, bias_correction1, bias_correction2);
        }
    }

    fn stepDeviceAdamWSlot(
        self: *RealAutodiffTrainer,
        slot: *ParamSlot,
        lr: f32,
        grad_scale: f32,
        bias_correction1: f32,
        bias_correction2: f32,
    ) !void {
        const device = slot.device orelse return error.DeviceOptimizerNotInitialized;
        const opt = self.config.optimizer;
        switch (self.compute_backend.kind()) {
            .metal => {
                if (comptime !build_options.enable_metal) return error.MetalBackendUnavailable;
                const metal = try self.metalCompute();
                try metal.trainingAdamWF32(device.weight, device.grad_accum, device.m, device.v, slot.weights.len, .{
                    .lr = lr,
                    .beta1 = opt.beta1,
                    .beta2 = opt.beta2,
                    .eps = opt.eps,
                    .weight_decay = opt.weight_decay,
                    .bias_correction1 = bias_correction1,
                    .bias_correction2 = bias_correction2,
                    .grad_scale = grad_scale,
                });
            },
            .mlx => {
                if (comptime !build_options.enable_mlx) return error.MlxBackendUnavailable;
                const mlx = try self.mlxCompute();
                const next = try mlx.trainingAdamWF32Replace(device.weight, device.grad_accum, device.m, device.v, .{
                    .lr = lr,
                    .beta1 = opt.beta1,
                    .beta2 = opt.beta2,
                    .eps = opt.eps,
                    .weight_decay = opt.weight_decay,
                    .bias_correction1 = bias_correction1,
                    .bias_correction2 = bias_correction2,
                    .grad_scale = grad_scale,
                });
                self.compute_backend.free(device.weight);
                self.compute_backend.free(device.grad_accum);
                self.compute_backend.free(device.m);
                self.compute_backend.free(device.v);
                slot.device = .{
                    .weight = next.weight,
                    .grad_accum = next.grad_accum,
                    .m = next.m,
                    .v = next.v,
                };
            },
            else => return error.DeviceOptimizerBackendUnavailable,
        }
        @memset(slot.grad_accum, 0);
    }

    fn buildGraphState(self: *RealAutodiffTrainer, input: TrainerInput) !void {
        // 1. Build the bare forward graph.
        var base_graph = Graph.init(self.allocator);
        errdefer base_graph.deinit();

        var bld = Builder.init(&base_graph);

        // Placeholders. Input IDs are bound at runtime as f32 (the user
        // forward callback typically gathers them into an embedding table,
        // which is happy to consume a flat float view).
        const ids_shape = Shape.init(.f32, &.{ @intCast(input.batch), @intCast(input.seq_len) });
        var input_ids_node = try bld.parameter("__input_ids", ids_shape);

        const mask_shape = Shape.init(.f32, &.{ @intCast(input.batch), @intCast(input.seq_len) });
        var attention_mask_node = try bld.parameter("__attention_mask", mask_shape);

        // Forward pass via callback.
        var forward_output = try input.build_forward(
            input.ctx,
            &bld,
            input_ids_node,
            attention_mask_node,
            input.batch,
            input.seq_len,
        );

        // Targets placeholder.
        var targets_node = try bld.parameter("__targets", input.targets_shape);

        // Loss.
        var loss_node = try input.build_loss(input.ctx, &bld, forward_output, targets_node);
        try base_graph.markOutput(loss_node);

        // 2. Inject LoRA. This clones the graph.
        var lora_result = try lora_mod.injectLoRA(self.allocator, &base_graph, self.config.lora);
        // The original graph is no longer needed — LoRAResult owns a clone.
        base_graph.deinit();
        errdefer {
            lora_result.graph.deinit();
            lora_result.adapter.deinit();
        }

        // Placeholder nodes retain their IDs because injectLoRA only
        // *appends* new parameter / op nodes; it never reorders or renumbers
        // the pre-existing nodes. See the redirectConsumers scan in lora.zig
        // — original nodes in [0..node_count) are updated in place, not
        // moved.

        const sorted = try topologicallySortGraph(self.allocator, &lora_result.graph);
        lora_result.graph.deinit();
        lora_result.graph = sorted.graph;

        input_ids_node = sorted.id_map[input_ids_node];
        attention_mask_node = sorted.id_map[attention_mask_node];
        targets_node = sorted.id_map[targets_node];
        loss_node = sorted.id_map[loss_node];
        forward_output = sorted.id_map[forward_output];

        for (lora_result.adapter.adapters.items) |*info| {
            info.lora_a_id = sorted.id_map[info.lora_a_id];
            info.lora_b_id = sorted.id_map[info.lora_b_id];
        }
        if (input.remap_graph_nodes) |remap| try remap(input.ctx, sorted.id_map);

        self.allocator.free(sorted.id_map);

        // 3. Allocate per-LoRA-parameter slots.
        var rng = std.Random.DefaultPrng.init(self.config.seed);
        var rnd = rng.random();

        for (lora_result.adapter.adapters.items) |info| {
            const a_node = lora_result.graph.node(info.lora_a_id);
            const b_node = lora_result.graph.node(info.lora_b_id);
            try self.appendParamSlot(info.lora_a_name, info.lora_a_id, a_node.output_shape, true, &rnd);
            try self.appendParamSlot(info.lora_b_name, info.lora_b_id, b_node.output_shape, false, &rnd);
        }
        for (self.config.regular_trainable_params) |name| {
            const node_id = findParameterByName(&lora_result.graph, name) orelse return error.TrainableParameterNotFound;
            const node = lora_result.graph.node(node_id);
            try self.appendRegularParamSlotFromBackend(name, node_id, node.output_shape);
        }

        // 4. Register residency blocks with the coordinator, if attached.
        if (self.coord) |coord| {
            for (self.lora_params.items, 0..) |*slot, i| {
                const id = grad_residency.GradBlockId{
                    .layer_idx = @intCast(i / 2),
                    .module_idx = @intCast(i % 2),
                };
                const bytes: u64 = @intCast(slot.weights.len * @sizeOf(f32));
                coord.registerGradBlock(id, bytes) catch |err| switch (err) {
                    // Budget denied is not fatal here — we simply won't
                    // track this block. The training step itself still works.
                    error.BudgetDenied => continue,
                    else => return err,
                };
                slot.block_id = id;
                slot.block_registered = true;
            }
        }

        self.graph_state = .{
            .graph = lora_result.graph,
            .input_ids_node = input_ids_node,
            .attention_mask_node = attention_mask_node,
            .targets_node = targets_node,
            .loss_node = loss_node,
            .forward_output_node = forward_output,
            .lora_adapter = lora_result.adapter,
        };
    }

    fn appendParamSlot(
        self: *RealAutodiffTrainer,
        name: []const u8,
        node_id: NodeId,
        shape: Shape,
        init_random: bool,
        rnd: *std.Random,
    ) !void {
        const n_elems: usize = @intCast(shape.numElements() orelse return error.DynamicShapeNotAllowed);

        const weights = try self.allocator.alloc(f32, n_elems);
        errdefer self.allocator.free(weights);
        if (init_random) {
            const std_dev = self.config.lora_a_init_std;
            for (weights) |*w| w.* = rnd.floatNorm(f32) * std_dev;
        } else {
            @memset(weights, 0.0);
        }

        const grad_accum = try self.allocator.alloc(f32, n_elems);
        errdefer self.allocator.free(grad_accum);
        @memset(grad_accum, 0.0);

        const rank = shape.rank();
        const dims = try self.allocator.alloc(i32, rank);
        errdefer self.allocator.free(dims);
        for (0..rank) |i| dims[i] = @intCast(shape.dim(@intCast(i)));

        try self.lora_params.append(self.allocator, .{
            .name = name,
            .weights = weights,
            .grad_accum = grad_accum,
            .node_id = node_id,
            .dims = dims,
        });
    }

    fn appendRegularParamSlotFromBackend(
        self: *RealAutodiffTrainer,
        name: []const u8,
        node_id: NodeId,
        shape: Shape,
    ) !void {
        if (isLoraParamName(name)) return error.InvalidRegularTrainableParameter;
        for (self.lora_params.items) |slot| {
            if (std.mem.eql(u8, slot.name, name)) return error.DuplicateTrainableParameter;
        }
        for (self.regular_params.items) |slot| {
            if (std.mem.eql(u8, slot.name, name)) return error.DuplicateTrainableParameter;
        }

        const n_elems: usize = @intCast(shape.numElements() orelse return error.DynamicShapeNotAllowed);
        const ct = try self.compute_backend.getWeight(name);
        defer self.compute_backend.free(ct);
        const weights = try self.compute_backend.toFloat32(ct, self.allocator);
        errdefer self.allocator.free(weights);
        if (weights.len != n_elems) return error.TrainableParameterShapeMismatch;

        const grad_accum = try self.allocator.alloc(f32, n_elems);
        errdefer self.allocator.free(grad_accum);
        @memset(grad_accum, 0.0);

        const rank = shape.rank();
        const dims = try self.allocator.alloc(i32, rank);
        errdefer self.allocator.free(dims);
        for (0..rank) |i| dims[i] = @intCast(shape.dim(@intCast(i)));

        try self.regular_params.append(self.allocator, .{
            .name = name,
            .weights = weights,
            .grad_accum = grad_accum,
            .node_id = node_id,
            .dims = dims,
        });
    }

    fn reserveActivationBudget(self: *RealAutodiffTrainer, input: TrainerInput) !void {
        const coord = self.coord orelse return;
        _ = coord;
        // Rough estimate: batch * seq * hidden * 4 bytes * num_layers * 2
        // (forward + backward). This is advisory only — the coordinator's
        // budget machinery is the ground truth. We invoke touchGradBlock on
        // each LoRA block so the residency tracker sees recent activity.
        const hidden: u64 = if (self.config.hidden_size_hint > 0) self.config.hidden_size_hint else 768;
        const layers: u64 = if (self.config.num_layers_hint > 0) self.config.num_layers_hint else 12;
        const est: u64 = @as(u64, input.batch) * @as(u64, input.seq_len) * hidden * 4 * layers * 2;
        _ = est; // Reserved for future fine-grained reservation.
    }

    fn pinAllLoraBlocks(self: *RealAutodiffTrainer) !void {
        const coord = self.coord orelse return;
        for (self.lora_params.items) |slot| {
            if (!slot.block_registered) continue;
            const id = slot.block_id orelse continue;
            try coord.pinGradBlock(id);
        }
    }

    fn unpinAllLoraBlocks(self: *RealAutodiffTrainer) !void {
        const coord = self.coord orelse return;
        for (self.lora_params.items) |slot| {
            if (!slot.block_registered) continue;
            const id = slot.block_id orelse continue;
            try coord.unpinGradBlock(id);
        }
    }

    fn globalGradNorm(self: *const RealAutodiffTrainer) f32 {
        var total: f64 = 0.0;
        for (self.lora_params.items) |slot| {
            for (slot.grad_accum) |g| total += @as(f64, g) * @as(f64, g);
        }
        for (self.regular_params.items) |slot| {
            for (slot.grad_accum) |g| total += @as(f64, g) * @as(f64, g);
        }
        return @floatCast(@sqrt(total));
    }

    fn gradientNormFromResult(self: *const RealAutodiffTrainer, step_result: *const training.TrainStepResult) !f32 {
        var total: f64 = 0.0;
        for (self.lora_params.items) |slot| {
            const g = step_result.gradients.get(slot.name) orelse continue;
            for (g) |value| total += @as(f64, value) * @as(f64, value);
        }
        for (self.regular_params.items) |slot| {
            const g = step_result.gradients.get(slot.name) orelse continue;
            for (g) |value| total += @as(f64, value) * @as(f64, value);
        }
        return @floatCast(@sqrt(total));
    }
};

// ── Helpers ──────────────────────────────────────────────────────────────────

fn putRuntimeInput(
    allocator: std.mem.Allocator,
    cb: *const ComputeBackend,
    rt: *std.AutoHashMapUnmanaged(NodeId, CT),
    id: NodeId,
    data: []const f32,
    dims: []const i32,
) !void {
    const ct = try cb.fromFloat32Shape(data, dims);
    errdefer cb.free(ct);
    try rt.put(allocator, id, ct);
}

fn shapeToDims(allocator: std.mem.Allocator, shape: Shape) ![]i32 {
    const rank = shape.rank();
    const dims = try allocator.alloc(i32, rank);
    for (0..rank) |i| dims[i] = @intCast(shape.dim(@intCast(i)));
    return dims;
}

fn monotonicNowNs() u64 {
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(.MONOTONIC, &ts))) {
        .SUCCESS => return @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec),
        else => return 0,
    }
}

fn elapsedNs(start_ns: u64, end_ns: u64) u64 {
    if (end_ns <= start_ns) return 0;
    return end_ns - start_ns;
}

fn nsToMs(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / 1_000_000.0;
}

fn currentResidentBytes() usize {
    const usage = std.posix.getrusage(std.posix.rusage.SELF);
    if (usage.maxrss <= 0) return 0;
    const maxrss: usize = @intCast(usage.maxrss);
    return switch (@import("builtin").os.tag) {
        .driverkit, .ios, .maccatalyst, .macos, .tvos, .visionos, .watchos => maxrss,
        .linux => std.math.mul(usize, maxrss, 1024) catch std.math.maxInt(usize),
        else => maxrss,
    };
}

fn compiledDiag(comptime fmt: []const u8, args: anytype) void {
    if (!platform.env.getenvBoolDefault("TERMITE_COMPILED_TRAIN_TRACE", false)) return;
    std.debug.print("[real-autodiff] " ++ fmt ++ "\n", args);
}

fn findParameterByName(graph: *const Graph, name: []const u8) ?NodeId {
    for (graph.parameters.items) |param_id| {
        const node = graph.node(param_id);
        if (node.op != .parameter) continue;
        if (std.mem.eql(u8, graph.parameterName(node), name)) return param_id;
    }
    return null;
}

fn isLoraParamName(name: []const u8) bool {
    return std.mem.indexOf(u8, name, ".lora_A") != null or
        std.mem.indexOf(u8, name, ".lora_B") != null;
}

const TopologicalSortResult = struct {
    graph: Graph,
    id_map: []NodeId,
};

fn topologicallySortGraph(allocator: std.mem.Allocator, graph: *const Graph) !TopologicalSortResult {
    const count = graph.nodeCount();
    const in_degree = try allocator.alloc(u32, count);
    defer allocator.free(in_degree);
    @memset(in_degree, 0);

    for (0..count) |i| {
        const node = graph.node(@intCast(i));
        for (node.getInputs()) |input_id| {
            if (input_id != null_node) in_degree[i] += 1;
        }
    }

    var queue = std.ArrayListUnmanaged(NodeId).empty;
    defer queue.deinit(allocator);
    for (0..count) |i| {
        if (in_degree[i] == 0) try queue.append(allocator, @intCast(i));
    }

    const id_map = try allocator.alloc(NodeId, count);
    errdefer allocator.free(id_map);
    @memset(id_map, null_node);

    var sorted_nodes = try std.ArrayListUnmanaged(ml.graph.Node).initCapacity(allocator, count);
    errdefer sorted_nodes.deinit(allocator);

    var head: usize = 0;
    while (head < queue.items.len) {
        const old_id = queue.items[head];
        head += 1;

        id_map[old_id] = @intCast(sorted_nodes.items.len);
        sorted_nodes.appendAssumeCapacity(graph.node(old_id).*);

        for (0..count) |j| {
            const node = graph.node(@intCast(j));
            for (node.getInputs()) |input_id| {
                if (input_id == old_id) {
                    in_degree[j] -= 1;
                    if (in_degree[j] == 0) try queue.append(allocator, @intCast(j));
                }
            }
        }
    }

    if (sorted_nodes.items.len != count) return error.CycleDetected;

    for (sorted_nodes.items) |*node| {
        for (&node.inputs, 0..) |*input_id, j| {
            if (j >= node.num_inputs) break;
            if (input_id.* != null_node) input_id.* = id_map[input_id.*];
        }
        if (node.vjp_alternate != null_node) node.vjp_alternate = id_map[node.vjp_alternate];
    }

    var sorted_graph = Graph.init(allocator);
    errdefer sorted_graph.deinit();
    try sorted_graph.string_table.appendSlice(allocator, graph.string_table.items);
    try sorted_graph.constant_pool.appendSlice(allocator, graph.constant_pool.items);
    try sorted_graph.nodes.appendSlice(allocator, sorted_nodes.items);
    for (graph.outputs.items) |output_id| try sorted_graph.outputs.append(allocator, id_map[output_id]);
    for (graph.parameters.items) |parameter_id| {
        if (id_map[parameter_id] != null_node) try sorted_graph.parameters.append(allocator, id_map[parameter_id]);
    }
    sorted_nodes.deinit(allocator);

    return .{
        .graph = sorted_graph,
        .id_map = id_map,
    };
}

// ── Tests ────────────────────────────────────────────────────────────────────

const testing = std.testing;

/// Minimal forward graph used in the unit test: a single frozen linear
/// layer + a ReLU. LoRA will get injected into the `test.linear.weight`
/// parameter because the pattern matches.
const TestCtx = struct {
    hidden: u32,

    fn buildForward(
        ctx: *anyopaque,
        bld: *Builder,
        input_ids: NodeId,
        attention_mask: NodeId,
        batch: u32,
        seq_len: u32,
    ) anyerror!NodeId {
        const self: *TestCtx = @ptrCast(@alignCast(ctx));
        _ = attention_mask;

        const rows: u32 = batch * seq_len;
        const hidden = self.hidden;

        // The `input_ids` placeholder is [batch, seq_len] f32. Reshape to
        // [rows, 1] and embed it into a [rows, hidden] tensor via a
        // (frozen) linear layer.
        const flat_shape = Shape.init(.f32, &.{ @intCast(rows), 1 });
        const flat = try bld.reshape(input_ids, flat_shape);

        // Frozen linear weight: [hidden, 1]
        const weight = try bld.parameter(
            "test.linear.weight",
            Shape.init(.f32, &.{ @intCast(hidden), 1 }),
        );
        const linear = try bld.linearNoBias(flat, weight, rows, 1, hidden);
        return try bld.relu(linear);
    }

    fn buildLoss(
        ctx: *anyopaque,
        bld: *Builder,
        forward_output: NodeId,
        targets: NodeId,
    ) anyerror!NodeId {
        _ = ctx;
        return try bld.mseLoss(forward_output, targets);
    }
};

test "RealAutodiffTrainer: graph construction injects LoRA adapters" {
    const allocator = testing.allocator;

    // Build the post-injection graph directly so we can inspect the
    // adapter list without needing a live ComputeBackend. This exercises
    // the same buildGraphState pre-conditions the trainer would hit.
    var g = Graph.init(allocator);
    defer g.deinit();

    var bld = Builder.init(&g);
    var ctx = TestCtx{ .hidden = 8 };

    const batch: u32 = 1;
    const seq_len: u32 = 4;

    const ids_shape = Shape.init(.f32, &.{ @intCast(batch), @intCast(seq_len) });
    const input_ids_node = try bld.parameter("__input_ids", ids_shape);
    const mask_shape = Shape.init(.f32, &.{ @intCast(batch), @intCast(seq_len) });
    const mask_node = try bld.parameter("__attention_mask", mask_shape);

    const fwd = try TestCtx.buildForward(@ptrCast(&ctx), &bld, input_ids_node, mask_node, batch, seq_len);

    const target_shape = Shape.init(.f32, &.{ @intCast(batch * seq_len), @intCast(ctx.hidden) });
    const targets_node = try bld.parameter("__targets", target_shape);
    const loss_node = try TestCtx.buildLoss(@ptrCast(&ctx), &bld, fwd, targets_node);
    try g.markOutput(loss_node);

    var lora_result = try lora_mod.injectLoRA(allocator, &g, .{
        .rank = 2,
        .alpha = 2.0,
        .target_patterns = &.{"linear.weight"},
    });
    defer lora_result.deinit();

    // Expect exactly one adapter pair for `test.linear.weight`.
    try testing.expectEqual(@as(usize, 1), lora_result.adapter.adapters.items.len);
    const info = lora_result.adapter.adapters.items[0];
    try testing.expectEqualStrings("test.linear.weight", info.base_name);

    // Autodiff should succeed on the injected graph w.r.t. the two LoRA
    // parameters. (We do not execute; the interpreter needs a backend.)
    var grad_result = try autodiff.gradient(
        allocator,
        &lora_result.graph,
        loss_node,
        &.{ info.lora_a_id, info.lora_b_id },
    );
    defer grad_result.deinit();

    try testing.expectEqual(@as(usize, 2), grad_result.param_grads.len);
    try testing.expect(grad_result.param_grads[0] != null_node);
    try testing.expect(grad_result.param_grads[1] != null_node);
}

// Mock reduce hook for the DDP hook test. Writes a sentinel value into
// each grad block and bumps a counter via the opaque context so the test
// can assert the harness actually invoked it.
const MockReduceCtx = struct {
    call_count: u32 = 0,
    last_block_count: usize = 0,
    sentinel: f32 = 7.0,
};

fn mockReduceGrads(ctx_opaque: *anyopaque, grads: []const GradBlock) anyerror!void {
    const ctx: *MockReduceCtx = @ptrCast(@alignCast(ctx_opaque));
    ctx.call_count += 1;
    ctx.last_block_count = grads.len;
    for (grads) |block| {
        for (block.data) |*v| v.* = ctx.sentinel;
    }
}

fn mockReduceDeviceGrads(ctx_opaque: *anyopaque, cb: *const ComputeBackend, grads: []const DeviceGradBlock) anyerror!void {
    _ = cb;
    const ctx: *MockReduceCtx = @ptrCast(@alignCast(ctx_opaque));
    ctx.call_count += 1;
    ctx.last_block_count = grads.len;
}

test "RealAutodiffTrainer: reduce_grads hook type wiring" {
    // Compile-time verification that the hook type + config fields line up,
    // plus a runtime check that a caller-provided hook can be invoked
    // directly with a synthetic GradBlock list. The full flush path is not
    // exercised here because it requires a live ComputeBackend — the hook
    // itself only cares about f32 slices, and its invocation inside `step`
    // is a straight-line call site verified by `zig test` compilation.
    const allocator = testing.allocator;

    comptime {
        const F: ?ReduceGradsFn = mockReduceGrads;
        _ = F;
        // Ensure the TrainerConfig field exists and has the right type.
        const C = TrainerConfig{
            .lora = .{ .rank = 1, .alpha = 1.0, .target_patterns = &.{"x"} },
        };
        _ = @TypeOf(C.reduce_grads);
        _ = @TypeOf(C.reduce_grads_ctx);
        _ = @TypeOf(C.reduce_device_grads);
        _ = @TypeOf(C.reduce_device_grads_ctx);
    }

    var ctx = MockReduceCtx{};
    var buf_a = [_]f32{ 1.0, 2.0, 3.0 };
    var buf_b = [_]f32{ 4.0, 5.0 };
    const blocks = [_]GradBlock{
        .{ .name = "a", .data = &buf_a },
        .{ .name = "b", .data = &buf_b },
    };

    const reduce: ReduceGradsFn = mockReduceGrads;
    try reduce(@ptrCast(&ctx), &blocks);

    try testing.expectEqual(@as(u32, 1), ctx.call_count);
    try testing.expectEqual(@as(usize, 2), ctx.last_block_count);
    for (buf_a) |v| try testing.expectEqual(ctx.sentinel, v);
    for (buf_b) |v| try testing.expectEqual(ctx.sentinel, v);

    // Also verify a TrainerConfig can actually carry the hook + ctx.
    const cfg = TrainerConfig{
        .lora = .{ .rank = 2, .alpha = 2.0, .target_patterns = &.{"q_proj"} },
        .reduce_grads = mockReduceGrads,
        .reduce_grads_ctx = @ptrCast(&ctx),
    };
    try testing.expect(cfg.reduce_grads != null);
    try testing.expect(cfg.reduce_grads_ctx != null);

    const dummy_cb: *const ComputeBackend = @ptrFromInt(@alignOf(ComputeBackend));
    var trainer = try RealAutodiffTrainer.init(allocator, dummy_cb, cfg);
    defer trainer.deinit();
    try testing.expect(trainer.config.reduce_grads != null);
}

test "RealAutodiffTrainer: reduce_device_grads hook type wiring" {
    const allocator = testing.allocator;

    comptime {
        const F: ?ReduceDeviceGradsFn = mockReduceDeviceGrads;
        _ = F;
    }

    var ctx = MockReduceCtx{};
    const fake_ct_a: CT = @ptrFromInt(0x1000);
    const fake_ct_b: CT = @ptrFromInt(0x2000);
    const dims_a = [_]i32{3};
    const dims_b = [_]i32{2};
    const blocks = [_]DeviceGradBlock{
        .{ .name = "a", .data = fake_ct_a, .elem_count = 3, .dims = &dims_a },
        .{ .name = "b", .data = fake_ct_b, .elem_count = 2, .dims = &dims_b },
    };
    const dummy_cb: *const ComputeBackend = @ptrFromInt(@alignOf(ComputeBackend));
    const reduce: ReduceDeviceGradsFn = mockReduceDeviceGrads;
    try reduce(@ptrCast(&ctx), dummy_cb, &blocks);
    try testing.expectEqual(@as(u32, 1), ctx.call_count);
    try testing.expectEqual(@as(usize, 2), ctx.last_block_count);

    const cfg = TrainerConfig{
        .lora = .{ .rank = 2, .alpha = 2.0, .target_patterns = &.{"q_proj"} },
        .reduce_device_grads = mockReduceDeviceGrads,
        .reduce_device_grads_ctx = @ptrCast(&ctx),
    };
    try testing.expect(cfg.reduce_device_grads != null);
    try testing.expect(cfg.reduce_device_grads_ctx != null);

    var trainer = try RealAutodiffTrainer.init(allocator, dummy_cb, cfg);
    defer trainer.deinit();
    try testing.expect(trainer.config.reduce_device_grads != null);
}

test "RealAutodiffTrainer: init and deinit are clean with no graph built" {
    const allocator = testing.allocator;

    // A dummy backend pointer — we never call into it because we never
    // call `.step`. init/deinit must tolerate the graph_state being null.
    const dummy_cb: *const ComputeBackend = @ptrFromInt(@alignOf(ComputeBackend));

    var trainer = try RealAutodiffTrainer.init(allocator, dummy_cb, .{
        .lora = .{
            .rank = 2,
            .alpha = 2.0,
            .target_patterns = &.{"q_proj"},
        },
    });
    defer trainer.deinit();

    try testing.expectEqual(@as(u64, 0), trainer.step_count);
    try testing.expect(trainer.graph_state == null);
    try testing.expectEqual(@as(usize, 0), trainer.lora_params.items.len);
}
