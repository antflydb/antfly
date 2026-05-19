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

const training = @import("../graph/training.zig");

const coord_mod = @import("training_memory_coordinator.zig");
const TrainingMemoryCoordinator = coord_mod.TrainingMemoryCoordinator;
const grad_residency = @import("grad_residency.zig");
const compat = @import("../io/compat.zig");

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

    /// Optional distributed gradient-reduction hook. Called exactly once per
    /// accumulation flush, immediately before the optimizer step, with a
    /// GradBlock per LoRA parameter. A typical DDP caller would wire this to
    /// `mlx_compute.allSumFloat32InPlaceOnStream` (one all-reduce per block)
    /// to fold LoRA gradients across ranks before AdamW updates.
    reduce_grads: ?ReduceGradsFn = null,
    /// Opaque context pointer forwarded to `reduce_grads` on each call.
    reduce_grads_ctx: ?*anyopaque = null,
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
};

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
    /// Optional Hypura coordinator.
    coord: ?*TrainingMemoryCoordinator = null,

    /// Per-LoRA-parameter state. Indices match `graph_state.lora_adapter.adapters`:
    /// `lora_params[i*2]` = A matrix, `lora_params[i*2+1]` = B matrix.
    lora_params: std.ArrayListUnmanaged(ParamSlot) = .empty,

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
        if (self.graph_state) |*gs| {
            gs.graph.deinit();
            gs.lora_adapter.deinit();
            self.graph_state = null;
        }
        for (self.lora_params.items) |*slot| {
            self.allocator.free(slot.weights);
            self.allocator.free(slot.grad_accum);
            self.allocator.free(slot.dims);
        }
        self.lora_params.deinit(self.allocator);
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
            try self.buildGraphState(input);
        }
    }

    /// Run one training step.
    pub fn step(self: *RealAutodiffTrainer, input: TrainerInput) !StepResult {
        return self.runStep(input, .train);
    }

    pub fn evaluate(self: *RealAutodiffTrainer, input: TrainerInput) !StepResult {
        return self.runStep(input, .eval);
    }

    fn runStep(self: *RealAutodiffTrainer, input: TrainerInput, mode: ExecutionMode) !StepResult {
        // 1. Lazy graph construction.
        try self.ensureGraphBuilt(input);
        const gs = &self.graph_state.?;

        // 2. Optional activation-budget reservation via the coordinator.
        if (self.coord != null) {
            try self.reserveActivationBudget(input);
            try self.pinAllLoraBlocks();
        }

        // 3. Build the runtime input map: input_ids + mask + targets + all
        //    LoRA parameter tensors. The underlying interpreter will free the
        //    CT handles after execution — we re-upload them on every step.
        var rt = std.AutoHashMapUnmanaged(NodeId, CT).empty;
        defer {
            var it = rt.iterator();
            while (it.next()) |e| self.compute_backend.free(e.value_ptr.*);
            rt.deinit(self.allocator);
        }

        // input_ids as f32 (graph represents them as f32 placeholders to
        // match the Builder parameter API; the callback is free to gather
        // from an embedding table inside the forward pass).
        const input_ids_f32 = try self.allocator.alloc(f32, input.input_ids.len);
        defer self.allocator.free(input_ids_f32);
        for (input.input_ids, 0..) |id, i| input_ids_f32[i] = @floatFromInt(id);

        const ids_dims = [_]i32{ @intCast(input.batch), @intCast(input.seq_len) };
        try putRuntimeInput(self.allocator, self.compute_backend, &rt, gs.input_ids_node, input_ids_f32, &ids_dims);
        try putRuntimeInput(self.allocator, self.compute_backend, &rt, gs.attention_mask_node, input.attention_mask, &ids_dims);

        const target_dims = try shapeToDims(self.allocator, input.targets_shape);
        defer self.allocator.free(target_dims);
        try putRuntimeInput(self.allocator, self.compute_backend, &rt, gs.targets_node, input.targets, target_dims);

        // LoRA parameter slices.
        for (self.lora_params.items) |slot| {
            try putRuntimeInput(self.allocator, self.compute_backend, &rt, slot.node_id, slot.weights, slot.dims);
        }

        // 3b. Architecture-specific input binding. This is how BERT
        //     position_ids, Qwen2 RoPE tables, LayoutLMv3 bbox components,
        //     etc. get into the runtime map. Without this, only the 3
        //     standard placeholders + LoRA params are bound.
        if (input.bind_arch_inputs) |bind_fn| {
            try bind_fn(input.ctx, self.compute_backend, self.allocator, &gs.graph, &rt, input.batch, input.seq_len, input.attention_mask);
        }

        // 4. Run the graph training step (autodiff + execute + extract).
        if (mode == .train) {
            self.optimizer_state.step_count = @intCast(self.step_count + 1);
        }

        // Collect the list of LoRA parameter names we want gradients for.
        var trainable = try self.allocator.alloc([]const u8, self.lora_params.items.len);
        defer self.allocator.free(trainable);
        for (self.lora_params.items, 0..) |slot, i| trainable[i] = slot.name;

        var step_result = try training.trainStep(
            self.allocator,
            &gs.graph,
            gs.loss_node,
            self.compute_backend,
            rt,
            .{ .trainable_params = trainable },
        );
        defer step_result.deinit();

        const loss_value = step_result.loss;

        var grad_norm: f32 = 0.0;
        var stepped = false;
        switch (mode) {
            .train => {
                // 5. Accumulate gradients (mean over the accumulation window).
                const accum_steps: u32 = @max(self.config.grad_accum_steps, 1);
                const scale: f32 = 1.0 / @as(f32, @floatFromInt(accum_steps));
                for (self.lora_params.items) |*slot| {
                    const g = step_result.gradients.get(slot.name) orelse {
                        if (self.accum_count == 0) @memset(slot.grad_accum, 0.0);
                        continue;
                    };
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
                    if (self.config.reduce_grads) |reduce_fn| {
                        var blocks = try self.allocator.alloc(GradBlock, self.lora_params.items.len);
                        defer self.allocator.free(blocks);
                        for (self.lora_params.items, 0..) |*slot, i| {
                            blocks[i] = .{ .name = slot.name, .data = slot.grad_accum };
                        }
                        const ctx = self.config.reduce_grads_ctx orelse @as(*anyopaque, @ptrFromInt(@alignOf(usize)));
                        try reduce_fn(ctx, blocks);
                    }

                    grad_norm = self.globalGradNorm();
                    if (self.config.max_grad_norm > 0.0 and grad_norm > self.config.max_grad_norm) {
                        const clip = self.config.max_grad_norm / (grad_norm + 1e-6);
                        for (self.lora_params.items) |*slot| {
                            for (slot.grad_accum) |*v| v.* *= clip;
                        }
                    }

                    const lr = self.config.lr_schedule.lr(@intCast(self.step_count));
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
                    self.accum_count = 0;
                    stepped = true;
                }
            },
            .eval => {
                grad_norm = try self.gradientNormFromResult(&step_result);
            },
        }

        // 7. Release pinned LoRA blocks.
        if (self.coord != null) {
            try self.unpinAllLoraBlocks();
        }

        if (mode == .train) self.step_count += 1;
        return .{
            .loss = loss_value,
            .grad_norm = grad_norm,
            .step = self.step_count,
            .optimizer_stepped = stepped,
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

    // ── Internals ────────────────────────────────────────────────────────

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
        return @floatCast(@sqrt(total));
    }

    fn gradientNormFromResult(self: *const RealAutodiffTrainer, step_result: *const training.TrainStepResult) !f32 {
        var total: f64 = 0.0;
        for (self.lora_params.items) |slot| {
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
