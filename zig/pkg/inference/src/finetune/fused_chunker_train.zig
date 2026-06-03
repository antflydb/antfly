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

// Full training loop for the fused chunker-embedder model.
//
// Orchestrates:
//   1. Graph-based boundary head training step (cross-entropy loss, autodiff)
//   2. CPU InfoNCE contrastive loss
//   3. AdamW optimizer steps for all trainable parameters
//   4. Evaluation (micro-F1)
//   5. Checkpoint save/load (simple binary format)

const std = @import("std");
const ml = @import("ml");
const Graph = ml.graph.Graph;
const NodeId = ml.graph.NodeId;
const Shape = ml.graph.Shape;
const Builder = ml.graph.Builder;
const optimizers = ml.graph.optimizers;
const ops_mod = @import("../ops/ops.zig");
const ComputeBackend = ops_mod.ComputeBackend;
const CT = ops_mod.CT;
const training = @import("../graph/training.zig");
const compat = @import("../io/compat.zig");
const fused_chunker_data = @import("fused_chunker_data.zig");
const fused_chunker = @import("fused_chunker.zig");
const fused_chunker_loss = @import("fused_chunker_loss.zig");
const infonce_cpu = @import("infonce_cpu.zig");
const fused_chunker_lora = @import("lora_adapter_set.zig");
const LoRAAdapterSet = fused_chunker_lora.LoRAAdapterSet;

// ----------------------------------------------------------------------------
// FusedTrainingConfig
// ----------------------------------------------------------------------------

pub const FusedTrainingConfig = struct {
    // Model
    max_seq_len: u32 = 384,
    embedding_dim: u32 = 768,
    hidden_size: u32 = 768,
    boundary_mlp_dim: u32 = 256,
    max_chunks: u32 = 32,

    // Training
    batch_size: u32 = 16,
    num_epochs: u32 = 10,
    learning_rate: f32 = 1e-4,
    warmup_steps: u32 = 50,
    total_steps: u32 = 1000,
    weight_decay: f32 = 0.01,
    max_grad_norm: f32 = 1.0,
    seed: u64 = 42,

    // AdamW
    beta1: f32 = 0.9,
    beta2: f32 = 0.999,
    adam_epsilon: f32 = 1e-8,

    // Loss (delegates to FusedLossConfig)
    lambda_chunk: f32 = 1.0,
    lambda_embed: f32 = 0.5,
    temperature: f32 = 0.07,
    focal_gamma: f32 = 0.0,
    focal_alpha: f32 = 0.75,
    pos_weight: f32 = 5.0,

    // Curriculum
    boundary_focus_epochs: u32 = 3,

    // Checkpointing
    checkpoint_every: u32 = 0,
    checkpoint_every_steps: u32 = 0,

    // Gradient accumulation (Feature 2)
    grad_accum_steps: u32 = 1,

    // Schedule-Free AdamW (Feature 3)
    use_schedule_free: bool = false,

    // Cross-Batch Memory (Feature 1)
    xbm_capacity: usize = 0,

    // NEFTune noise (Feature 5)
    neftune_alpha: f32 = 0.0,

    // Layer-wise LR decay (Feature 6)
    llrd_decay: f32 = 1.0,

    // Length bucketing (Feature 8)
    length_bucketing: bool = false,
    bucket_size: usize = 256,

    // Mixed precision (Feature 9 CLI flag — stored for downstream use)
    mixed_precision: bool = false,

    // SPLADE sparse embedding head
    enable_splade: bool = false,
    lambda_splade: f32 = 0.15,
    lambda_flops: f32 = 3e-5,
    splade_focus_epoch: u32 = 4,

    // Matryoshka Representation Learning
    use_mrl: bool = false,

    // LoRA+ ratio: multiplier on the LoRA B-matrix learning rate relative to A
    lora_plus_ratio: f32 = 1.0,

    pub fn lrSchedule(self: FusedTrainingConfig) optimizers.LearningRateSchedule {
        return .{ .warmup_cosine = .{
            .initial_lr = self.learning_rate,
            .min_lr = self.learning_rate * 0.1,
            .warmup_steps = self.warmup_steps,
            .total_steps = self.total_steps,
        } };
    }
};

// ----------------------------------------------------------------------------
// CrossBatchMemory (Feature 1)
// ----------------------------------------------------------------------------

/// Ring buffer of chunk embeddings from recent batches.
/// Expands the effective negative set for InfoNCE contrastive learning.
pub const CrossBatchMemory = struct {
    allocator: std.mem.Allocator,
    capacity: usize,
    embed_dim: usize,
    embeddings: []f32, // [capacity, embed_dim] circular buffer
    doc_ids: []u32, // [capacity]
    count: usize, // number of valid entries
    head: usize, // next write position

    pub fn init(allocator: std.mem.Allocator, capacity: usize, embed_dim: usize) !CrossBatchMemory {
        const embeddings = try allocator.alloc(f32, capacity * embed_dim);
        errdefer allocator.free(embeddings);
        const doc_ids = try allocator.alloc(u32, capacity);
        errdefer allocator.free(doc_ids);
        @memset(embeddings, 0);
        @memset(doc_ids, 0);
        return .{
            .allocator = allocator,
            .capacity = capacity,
            .embed_dim = embed_dim,
            .embeddings = embeddings,
            .doc_ids = doc_ids,
            .count = 0,
            .head = 0,
        };
    }

    pub fn deinit(self: *CrossBatchMemory) void {
        self.allocator.free(self.embeddings);
        self.allocator.free(self.doc_ids);
        self.* = undefined;
    }

    /// Add a batch of chunk embeddings to the memory.
    /// embeddings: [num_chunks * embed_dim], mask: [num_chunks]
    /// doc_id_offset is added to each doc_id before storing, so that entries from
    /// different batches have globally unique IDs (Fix 1: XBM doc_id collision).
    pub fn add(self: *CrossBatchMemory, embeddings: []const f32, doc_ids: []const u32, chunk_mask: []const f32, num_chunks: usize, doc_id_offset: u64) void {
        for (0..num_chunks) |ci| {
            if (chunk_mask[ci] <= 0.5) continue;
            const src = embeddings[ci * self.embed_dim .. (ci + 1) * self.embed_dim];
            const dst = self.embeddings[self.head * self.embed_dim .. (self.head + 1) * self.embed_dim];
            @memcpy(dst, src);
            self.doc_ids[self.head] = @truncate(doc_ids[ci] + doc_id_offset);
            self.head = (self.head + 1) % self.capacity;
            if (self.count < self.capacity) self.count += 1;
        }
    }

    /// Get all valid stored embeddings and their doc_ids.
    /// Returns slices into internal storage (valid until next add call).
    pub fn getStored(self: *CrossBatchMemory) struct { embeddings: []const f32, doc_ids: []const u32, count: usize } {
        return .{
            .embeddings = self.embeddings[0 .. self.count * self.embed_dim],
            .doc_ids = self.doc_ids[0..self.count],
            .count = self.count,
        };
    }
};

// ----------------------------------------------------------------------------
// ScheduleFreeAdamW (Feature 3)
// ----------------------------------------------------------------------------

pub const ScheduleFreeAdamWState = struct {
    allocator: std.mem.Allocator,
    z: []f32, // base iterate (same shape as parameter)
    v: []f32, // second moment
    step: u64,

    pub fn init(allocator: std.mem.Allocator, initial_weights: []const f32) !ScheduleFreeAdamWState {
        const z = try allocator.dupe(f32, initial_weights);
        errdefer allocator.free(z);
        const v = try allocator.alloc(f32, initial_weights.len);
        errdefer allocator.free(v);
        @memset(v, 0);
        return .{
            .allocator = allocator,
            .z = z,
            .v = v,
            .step = 0,
        };
    }

    pub fn deinit(self: *ScheduleFreeAdamWState) void {
        self.allocator.free(self.z);
        self.allocator.free(self.v);
        self.* = undefined;
    }
};

/// Schedule-Free AdamW step (Defazio et al. 2024).
/// weights (x = Polyak average) is updated in place.
/// lr is used directly — caller's LR schedule already handles warmup (Fix 3).
pub fn scheduleFreeAdamWStep(
    weights: []f32, // x (Polyak average) — updated in place
    grad: []const f32,
    state: *ScheduleFreeAdamWState,
    lr: f32,
    beta1: f32,
    beta2: f32,
    epsilon: f32,
    weight_decay: f32,
    warmup_steps: u32, // retained for API compatibility; no longer used internally
) void {
    _ = warmup_steps;
    state.step += 1;
    const lr_t = lr;
    // Polyak mixing coefficient: c = min(β₁, 1/t). Decreases as 1/t so the
    // running average converges; β₁ caps it from above on the first few steps.
    const c = @min(beta1, 1.0 / @as(f32, @floatFromInt(state.step)));

    for (0..weights.len) |i| {
        const g = grad[i];
        // Update second moment
        state.v[i] = beta2 * state.v[i] + (1.0 - beta2) * g * g;
        // Update z (base iterate)
        const denom = @sqrt(state.v[i]) + epsilon;
        state.z[i] = state.z[i] - lr_t * g / denom - lr_t * weight_decay * state.z[i];
        // Update x (Polyak average)
        weights[i] = (1.0 - c) * weights[i] + c * state.z[i];
    }
}

// ----------------------------------------------------------------------------
// BoundaryHead
// ----------------------------------------------------------------------------

pub const BoundaryHead = struct {
    allocator: std.mem.Allocator,
    w1: []f32, // [mlp_dim, hidden_dim]
    b1: []f32, // [mlp_dim]
    w2: []f32, // [2, mlp_dim]
    b2: []f32, // [2]
    hidden_dim: usize,
    mlp_dim: usize,

    /// Initialise weights with small deterministic values:
    ///   angle = (row+1)*(col+5)
    ///   w[idx] = (sin(0.11*angle) + cos(0.07*angle)) * 0.05
    /// All biases are zero.
    pub fn init(allocator: std.mem.Allocator, hidden_dim: usize, mlp_dim: usize) !BoundaryHead {
        const w1 = try allocator.alloc(f32, mlp_dim * hidden_dim);
        errdefer allocator.free(w1);
        const b1 = try allocator.alloc(f32, mlp_dim);
        errdefer allocator.free(b1);
        const w2 = try allocator.alloc(f32, 2 * mlp_dim);
        errdefer allocator.free(w2);
        const b2 = try allocator.alloc(f32, 2);
        errdefer allocator.free(b2);

        // w1: [mlp_dim, hidden_dim]
        for (0..mlp_dim) |row| {
            for (0..hidden_dim) |col| {
                const idx = row * hidden_dim + col;
                const angle = @as(f32, @floatFromInt((row + 1) * (col + 5)));
                w1[idx] = (@sin(angle * 0.11) + @cos(angle * 0.07)) * 0.05;
            }
        }
        @memset(b1, 0);

        // w2: [2, mlp_dim]
        for (0..2) |row| {
            for (0..mlp_dim) |col| {
                const idx = row * mlp_dim + col;
                const angle = @as(f32, @floatFromInt((row + 1) * (col + 5)));
                w2[idx] = (@sin(angle * 0.11) + @cos(angle * 0.07)) * 0.05;
            }
        }
        @memset(b2, 0);

        return .{
            .allocator = allocator,
            .w1 = w1,
            .b1 = b1,
            .w2 = w2,
            .b2 = b2,
            .hidden_dim = hidden_dim,
            .mlp_dim = mlp_dim,
        };
    }

    pub fn deinit(self: *BoundaryHead) void {
        self.allocator.free(self.w1);
        self.allocator.free(self.b1);
        self.allocator.free(self.w2);
        self.allocator.free(self.b2);
        self.* = undefined;
    }
};

// ----------------------------------------------------------------------------
// TrainStepSummary
// ----------------------------------------------------------------------------

pub const TrainStepSummary = struct {
    boundary_loss: f32 = 0,
    contrastive_loss: f64 = 0,
    total_loss: f32 = 0,
    boundary_tp: u64 = 0,
    boundary_fp: u64 = 0,
    boundary_fn: u64 = 0,
    step: u32 = 0,
    learning_rate: f32 = 0,
};

pub const TrainStepWithGradSummary = struct {
    summary: TrainStepSummary,
    /// dL/d(features): [total_tokens * hidden_size] — owned, caller must free.
    /// null if no gradient was available.
    features_grad: ?[]f32,

    pub fn deinit(self: *TrainStepWithGradSummary, allocator: std.mem.Allocator) void {
        if (self.features_grad) |g| allocator.free(g);
        self.* = undefined;
    }
};

// ----------------------------------------------------------------------------
// EvalSummary
// ----------------------------------------------------------------------------

pub const EvalSummary = struct {
    boundary_f1: f32 = 0,
    boundary_precision: f32 = 0,
    boundary_recall: f32 = 0,
    num_batches: u32 = 0,
};

// ----------------------------------------------------------------------------
// FusedTrainer
// ----------------------------------------------------------------------------

pub const FusedTrainer = struct {
    allocator: std.mem.Allocator,
    config: FusedTrainingConfig,
    loss_config: fused_chunker_loss.FusedLossConfig,
    cb: *const ComputeBackend,

    // Head weights
    boundary_head: BoundaryHead,

    // Optimizer
    optimizer: optimizers.Optimizer,
    optimizer_state: optimizers.OptimizerState,
    step_count: u32 = 0,
    lr_schedule: optimizers.LearningRateSchedule,

    // LoRA adapters (optional — set externally before training begins)
    lora_adapters: ?LoRAAdapterSet = null,

    // Cross-Batch Memory (Feature 1)
    xbm: ?CrossBatchMemory = null,
    // Monotonic base added to doc_ids before XBM storage; incremented by a large
    // prime each batch to guarantee globally unique IDs across stored generations
    // (Fix 1: XBM doc_id collision).
    xbm_doc_id_base: u64 = 0,

    // Gradient accumulation (Feature 2)
    accum_count: u32 = 0,
    grad_accum_w1: []f32 = &.{},
    grad_accum_b1: []f32 = &.{},
    grad_accum_w2: []f32 = &.{},
    grad_accum_b2: []f32 = &.{},

    // Schedule-Free AdamW states (Feature 3)
    sf_state_w1: ?ScheduleFreeAdamWState = null,
    sf_state_b1: ?ScheduleFreeAdamWState = null,
    sf_state_w2: ?ScheduleFreeAdamWState = null,
    sf_state_b2: ?ScheduleFreeAdamWState = null,

    // LoRA features_grad accumulator for grad_accum_steps > 1 (Fix 4)
    grad_accum_lora_features_grad_accum: ?[]f32 = null,

    pub fn init(
        allocator: std.mem.Allocator,
        config: FusedTrainingConfig,
        cb: *const ComputeBackend,
    ) !FusedTrainer {
        const loss_config = fused_chunker_loss.FusedLossConfig{
            .lambda_chunk = config.lambda_chunk,
            .lambda_embed = config.lambda_embed,
            .focal_gamma = config.focal_gamma,
            .focal_alpha = config.focal_alpha,
            .temperature = config.temperature,
            .pos_weight = config.pos_weight,
            .enable_splade = config.enable_splade,
            .lambda_splade = config.lambda_splade,
            .lambda_flops = config.lambda_flops,
            .splade_focus_epoch = config.splade_focus_epoch,
            .use_mrl = config.use_mrl,
        };

        var head = try BoundaryHead.init(
            allocator,
            @intCast(config.hidden_size),
            @intCast(config.boundary_mlp_dim),
        );
        errdefer head.deinit();

        const optimizer = optimizers.Optimizer{ .adamw = .{
            .beta1 = config.beta1,
            .beta2 = config.beta2,
            .eps = config.adam_epsilon,
            .weight_decay = config.weight_decay,
        } };

        const opt_state = optimizers.OptimizerState.init(allocator);
        const lr_schedule = config.lrSchedule();

        // Gradient accumulation buffers (Feature 2)
        const accum_w1 = try allocator.alloc(f32, head.w1.len);
        errdefer allocator.free(accum_w1);
        @memset(accum_w1, 0);
        const accum_b1 = try allocator.alloc(f32, head.b1.len);
        errdefer allocator.free(accum_b1);
        @memset(accum_b1, 0);
        const accum_w2 = try allocator.alloc(f32, head.w2.len);
        errdefer allocator.free(accum_w2);
        @memset(accum_w2, 0);
        const accum_b2 = try allocator.alloc(f32, head.b2.len);
        errdefer allocator.free(accum_b2);
        @memset(accum_b2, 0);

        // Schedule-Free AdamW states (Feature 3)
        var sf_w1: ?ScheduleFreeAdamWState = null;
        var sf_b1: ?ScheduleFreeAdamWState = null;
        var sf_w2: ?ScheduleFreeAdamWState = null;
        var sf_b2: ?ScheduleFreeAdamWState = null;
        if (config.use_schedule_free) {
            sf_w1 = try ScheduleFreeAdamWState.init(allocator, head.w1);
            errdefer if (sf_w1) |*s| s.deinit();
            sf_b1 = try ScheduleFreeAdamWState.init(allocator, head.b1);
            errdefer if (sf_b1) |*s| s.deinit();
            sf_w2 = try ScheduleFreeAdamWState.init(allocator, head.w2);
            errdefer if (sf_w2) |*s| s.deinit();
            sf_b2 = try ScheduleFreeAdamWState.init(allocator, head.b2);
            errdefer if (sf_b2) |*s| s.deinit();
        }

        // Cross-Batch Memory (Feature 1)
        var xbm: ?CrossBatchMemory = null;
        if (config.xbm_capacity > 0) {
            xbm = try CrossBatchMemory.init(allocator, config.xbm_capacity, config.embedding_dim);
            errdefer if (xbm) |*x| x.deinit();
        }

        return .{
            .allocator = allocator,
            .config = config,
            .loss_config = loss_config,
            .cb = cb,
            .boundary_head = head,
            .optimizer = optimizer,
            .optimizer_state = opt_state,
            .step_count = 0,
            .lr_schedule = lr_schedule,
            .xbm = xbm,
            .accum_count = 0,
            .grad_accum_w1 = accum_w1,
            .grad_accum_b1 = accum_b1,
            .grad_accum_w2 = accum_w2,
            .grad_accum_b2 = accum_b2,
            .sf_state_w1 = sf_w1,
            .sf_state_b1 = sf_b1,
            .sf_state_w2 = sf_w2,
            .sf_state_b2 = sf_b2,
        };
    }

    pub fn deinit(self: *FusedTrainer) void {
        self.boundary_head.deinit();
        self.optimizer_state.deinit();
        // Gradient accumulation buffers (Feature 2)
        self.allocator.free(self.grad_accum_w1);
        self.allocator.free(self.grad_accum_b1);
        self.allocator.free(self.grad_accum_w2);
        self.allocator.free(self.grad_accum_b2);
        // Schedule-Free states (Feature 3)
        if (self.sf_state_w1) |*s| s.deinit();
        if (self.sf_state_b1) |*s| s.deinit();
        if (self.sf_state_w2) |*s| s.deinit();
        if (self.sf_state_b2) |*s| s.deinit();
        // Cross-Batch Memory (Feature 1)
        if (self.xbm) |*x| x.deinit();
        // LoRA features_grad accumulator (Fix 4)
        if (self.grad_accum_lora_features_grad_accum) |buf| self.allocator.free(buf);
        self.* = undefined;
    }

    /// Run one training step.
    ///
    /// features:          [total_tokens * hidden_size] encoder hidden states
    /// boundary_labels:   [total_tokens * 2] one-hot
    /// attention_mask:    [total_tokens] 0 or 1
    /// chunk_embeddings:  [B*C*E] late-chunked embeddings
    /// chunk_mask:        [B*C] valid chunk mask
    /// doc_ids:           [B*C] document index per chunk
    pub fn trainStep(
        self: *FusedTrainer,
        allocator: std.mem.Allocator,
        features: []const f32,
        boundary_labels: []const f32,
        attention_mask: []const f32,
        chunk_embeddings: []const f32,
        chunk_mask: []const f32,
        doc_ids: []const u32,
        total_tokens: usize,
        B: usize,
        C: usize,
        E: usize,
    ) !TrainStepSummary {
        _ = attention_mask;

        // 1. Build graph on-the-fly (total_tokens varies per batch)
        var graph = try fused_chunker_loss.BoundaryHeadGraph.init(
            allocator,
            total_tokens,
            self.config.hidden_size,
            self.config.boundary_mlp_dim,
        );
        defer graph.deinit();

        // 2. Build runtime map
        var rt = std.AutoHashMapUnmanaged(NodeId, CT){};
        defer {
            var it = rt.iterator();
            while (it.next()) |e| self.cb.free(e.value_ptr.*);
            rt.deinit(allocator);
        }

        try putRuntimeInput(allocator, self.cb, &rt, graph.feature_id, features, &.{
            @intCast(total_tokens),
            @intCast(self.config.hidden_size),
        });
        try putRuntimeInput(allocator, self.cb, &rt, graph.target_id, boundary_labels, &.{
            @intCast(total_tokens),
            2,
        });
        try putRuntimeInput(allocator, self.cb, &rt, graph.w1_id, self.boundary_head.w1, &.{
            @intCast(self.config.boundary_mlp_dim),
            @intCast(self.config.hidden_size),
        });
        try putRuntimeInput(allocator, self.cb, &rt, graph.b1_id, self.boundary_head.b1, &.{
            @intCast(self.config.boundary_mlp_dim),
        });
        try putRuntimeInput(allocator, self.cb, &rt, graph.w2_id, self.boundary_head.w2, &.{
            2,
            @intCast(self.config.boundary_mlp_dim),
        });
        try putRuntimeInput(allocator, self.cb, &rt, graph.b2_id, self.boundary_head.b2, &.{2});

        // 3. Training step (autodiff + execute)
        self.optimizer_state.step_count = self.step_count + 1;
        var step_result = try training.trainStep(
            allocator,
            &graph.graph,
            graph.loss_id,
            self.cb,
            rt,
            .{ .trainable_params = &.{ "w1", "b1", "w2", "b2" } },
        );
        defer step_result.deinit();

        const boundary_loss = step_result.loss;

        // 4. Get current learning rate
        const lr = self.lr_schedule.lr(self.step_count);

        // 5. Accumulate gradients — scale by 1/accum_steps before adding
        const w1_grad = step_result.gradients.get("w1") orelse return error.MissingGradient;
        const b1_grad = step_result.gradients.get("b1") orelse return error.MissingGradient;
        const w2_grad = step_result.gradients.get("w2") orelse return error.MissingGradient;
        const b2_grad = step_result.gradients.get("b2") orelse return error.MissingGradient;

        const accum_steps = self.config.grad_accum_steps;
        const scale: f32 = 1.0 / @as(f32, @floatFromInt(@max(accum_steps, 1)));
        for (w1_grad, self.grad_accum_w1) |g, *a| a.* += g * scale;
        for (b1_grad, self.grad_accum_b1) |g, *a| a.* += g * scale;
        for (w2_grad, self.grad_accum_w2) |g, *a| a.* += g * scale;
        for (b2_grad, self.grad_accum_b2) |g, *a| a.* += g * scale;
        self.accum_count += 1;

        // Apply optimizer step only when accumulation window is full
        var applied_lr: f32 = 0.0;
        if (self.accum_count >= accum_steps) {
            // Clip the accumulated gradient (global norm across all four tensors).
            {
                var total_sq: f64 = 0;
                for (self.grad_accum_w1) |g| total_sq += @as(f64, g * g);
                for (self.grad_accum_b1) |g| total_sq += @as(f64, g * g);
                for (self.grad_accum_w2) |g| total_sq += @as(f64, g * g);
                for (self.grad_accum_b2) |g| total_sq += @as(f64, g * g);
                const total_norm: f32 = @floatCast(@sqrt(total_sq));
                if (total_norm > self.config.max_grad_norm) {
                    const gscale = self.config.max_grad_norm / (total_norm + 1e-6);
                    for (self.grad_accum_w1) |*g| g.* *= gscale;
                    for (self.grad_accum_b1) |*g| g.* *= gscale;
                    for (self.grad_accum_w2) |*g| g.* *= gscale;
                    for (self.grad_accum_b2) |*g| g.* *= gscale;
                }
            }
            applied_lr = lr;
            if (self.config.use_schedule_free) {
                scheduleFreeAdamWStep(self.boundary_head.w1, self.grad_accum_w1, &self.sf_state_w1.?, lr, self.config.beta1, self.config.beta2, self.config.adam_epsilon, self.config.weight_decay, self.config.warmup_steps);
                scheduleFreeAdamWStep(self.boundary_head.b1, self.grad_accum_b1, &self.sf_state_b1.?, lr, self.config.beta1, self.config.beta2, self.config.adam_epsilon, self.config.weight_decay, self.config.warmup_steps);
                scheduleFreeAdamWStep(self.boundary_head.w2, self.grad_accum_w2, &self.sf_state_w2.?, lr, self.config.beta1, self.config.beta2, self.config.adam_epsilon, self.config.weight_decay, self.config.warmup_steps);
                scheduleFreeAdamWStep(self.boundary_head.b2, self.grad_accum_b2, &self.sf_state_b2.?, lr, self.config.beta1, self.config.beta2, self.config.adam_epsilon, self.config.weight_decay, self.config.warmup_steps);
            } else {
                try optimizers.step(self.optimizer, &self.optimizer_state, lr, "w1", self.boundary_head.w1, self.grad_accum_w1);
                try optimizers.step(self.optimizer, &self.optimizer_state, lr, "b1", self.boundary_head.b1, self.grad_accum_b1);
                try optimizers.step(self.optimizer, &self.optimizer_state, lr, "w2", self.boundary_head.w2, self.grad_accum_w2);
                try optimizers.step(self.optimizer, &self.optimizer_state, lr, "b2", self.boundary_head.b2, self.grad_accum_b2);
            }
            // Reset accumulation
            @memset(self.grad_accum_w1, 0);
            @memset(self.grad_accum_b1, 0);
            @memset(self.grad_accum_w2, 0);
            @memset(self.grad_accum_b2, 0);
            self.accum_count = 0;
        }

        // 6. CPU InfoNCE contrastive loss — with optional XBM expansion (Feature 1)
        var contrastive_result: infonce_cpu.ContrastiveLossResult = undefined;
        var xbm_expanded_embeddings: ?[]f32 = null;
        var xbm_expanded_doc_ids: ?[]u32 = null;
        defer if (xbm_expanded_embeddings) |e| allocator.free(e);
        defer if (xbm_expanded_doc_ids) |d| allocator.free(d);

        const eff_embeddings: []const f32 = blk: {
            if (self.xbm) |*xbm| {
                const stored = xbm.getStored();
                if (stored.count > 0) {
                    const n_current = B * C;
                    const n_total = n_current + stored.count;
                    const expanded = try allocator.alloc(f32, n_total * E);
                    xbm_expanded_embeddings = expanded;
                    @memcpy(expanded[0 .. n_current * E], chunk_embeddings);
                    @memcpy(expanded[n_current * E ..], stored.embeddings[0 .. stored.count * E]);
                    // Stored doc_ids already carry globally unique offsets (Fix 1: each
                    // generation was stored with xbm_doc_id_base applied). Copy as-is.
                    const exp_ids = try allocator.alloc(u32, n_total);
                    xbm_expanded_doc_ids = exp_ids;
                    @memcpy(exp_ids[0..n_current], doc_ids);
                    @memcpy(exp_ids[n_current..], stored.doc_ids[0..stored.count]);
                    // Expanded chunk mask: copy current batch mask then fill rest with 1.0
                    const exp_mask = try allocator.alloc(f32, n_total);
                    defer allocator.free(exp_mask);
                    @memcpy(exp_mask[0..n_current], chunk_mask[0..n_current]);
                    @memset(exp_mask[n_current..], 1.0);
                    contrastive_result = if (self.loss_config.use_mrl) mrl_blk: {
                        const mrl_config = infonce_cpu.MatryoshkaConfig{
                            .dims = self.loss_config.mrl_dims,
                            .weights = self.loss_config.mrl_weights,
                        };
                        const mrl = try infonce_cpu.computeMatryoshkaLossAndGrad(
                            allocator,
                            expanded,
                            exp_mask,
                            exp_ids,
                            n_total,
                            E,
                            mrl_config,
                            self.loss_config.temperature,
                            self.loss_config.focal_gamma,
                            self.loss_config.focal_alpha,
                        );
                        allocator.free(mrl.per_scale_loss);
                        break :mrl_blk infonce_cpu.ContrastiveLossResult{
                            .contrastive_loss = mrl.total_loss,
                            .total_loss = mrl.total_loss * @as(f64, self.loss_config.lambda_embed),
                            .grad = mrl.grad,
                        };
                    } else try infonce_cpu.computeContrastiveLossOnCPU(
                        allocator,
                        expanded,
                        exp_mask,
                        exp_ids,
                        @as(f64, self.loss_config.temperature),
                        @as(f64, self.loss_config.lambda_embed),
                        1,
                        n_total,
                        E,
                        @as(f64, self.loss_config.focal_gamma),
                        @as(f64, self.loss_config.focal_alpha),
                    );
                    break :blk expanded;
                }
            }
            contrastive_result = if (self.loss_config.use_mrl) mrl_blk: {
                const mrl_config = infonce_cpu.MatryoshkaConfig{
                    .dims = self.loss_config.mrl_dims,
                    .weights = self.loss_config.mrl_weights,
                };
                const mrl = try infonce_cpu.computeMatryoshkaLossAndGrad(
                    allocator,
                    chunk_embeddings,
                    chunk_mask,
                    doc_ids,
                    B * C,
                    E,
                    mrl_config,
                    self.loss_config.temperature,
                    self.loss_config.focal_gamma,
                    self.loss_config.focal_alpha,
                );
                allocator.free(mrl.per_scale_loss);
                break :mrl_blk infonce_cpu.ContrastiveLossResult{
                    .contrastive_loss = mrl.total_loss,
                    .total_loss = mrl.total_loss * @as(f64, self.loss_config.lambda_embed),
                    .grad = mrl.grad,
                };
            } else try infonce_cpu.computeContrastiveLossOnCPU(
                allocator,
                chunk_embeddings,
                chunk_mask,
                doc_ids,
                @as(f64, self.loss_config.temperature),
                @as(f64, self.loss_config.lambda_embed),
                B,
                C,
                E,
                @as(f64, self.loss_config.focal_gamma),
                @as(f64, self.loss_config.focal_alpha),
            );
            break :blk chunk_embeddings;
        };
        _ = eff_embeddings;
        defer contrastive_result.deinit(allocator);

        // Add current batch to XBM after computing loss (Feature 1 / Fix 1).
        // Store doc_ids with the current global base so each generation's IDs are unique.
        if (self.xbm) |*xbm| {
            xbm.add(chunk_embeddings, doc_ids, chunk_mask, B * C, self.xbm_doc_id_base);
            self.xbm_doc_id_base +%= 100003;
        }

        // Note: contrastive gradients w.r.t. encoder weights are not applied here;
        // segmented backprop through the encoder is a future feature.

        // 7. Total loss
        const total_loss = self.loss_config.lambda_chunk * boundary_loss +
            @as(f32, @floatCast(contrastive_result.total_loss));

        // 8. Increment step count
        self.step_count += 1;
        std.log.info("fused_chunker step={d} boundary_loss={d:.4} total_loss={d:.4} lr={d}", .{ self.step_count, boundary_loss, total_loss, applied_lr });

        return TrainStepSummary{
            .boundary_loss = boundary_loss,
            .contrastive_loss = contrastive_result.contrastive_loss,
            .total_loss = total_loss,
            .boundary_tp = 0,
            .boundary_fp = 0,
            .boundary_fn = 0,
            .step = self.step_count,
            .learning_rate = applied_lr,
        };
    }

    /// Run one training step and also return dL/d(features) for encoder LoRA backprop.
    ///
    /// Identical to trainStep except "features" is added to the trainable_params
    /// list so that autodiff computes the gradient w.r.t. the input features tensor.
    /// The gradient is duped into an owned slice before the training.trainStep result
    /// is released; the caller is responsible for freeing it via
    /// TrainStepWithGradSummary.deinit().
    ///
    /// If training.trainStep does not produce a "features" gradient (e.g. the
    /// parameter is not present in the graph), features_grad is set to null and
    /// no error is returned.
    pub fn trainStepWithEncoderGrad(
        self: *FusedTrainer,
        allocator: std.mem.Allocator,
        features: []const f32,
        boundary_labels: []const f32,
        attention_mask: []const f32,
        chunk_embeddings: []const f32,
        chunk_mask: []const f32,
        doc_ids: []const u32,
        total_tokens: usize,
        B: usize,
        C: usize,
        E: usize,
    ) !TrainStepWithGradSummary {
        _ = attention_mask;

        // 1. Build graph on-the-fly (total_tokens varies per batch)
        var graph = try fused_chunker_loss.BoundaryHeadGraph.init(
            allocator,
            total_tokens,
            self.config.hidden_size,
            self.config.boundary_mlp_dim,
        );
        defer graph.deinit();

        // 2. Build runtime map
        var rt = std.AutoHashMapUnmanaged(NodeId, CT){};
        defer {
            var it = rt.iterator();
            while (it.next()) |e| self.cb.free(e.value_ptr.*);
            rt.deinit(allocator);
        }

        try putRuntimeInput(allocator, self.cb, &rt, graph.feature_id, features, &.{
            @intCast(total_tokens),
            @intCast(self.config.hidden_size),
        });
        try putRuntimeInput(allocator, self.cb, &rt, graph.target_id, boundary_labels, &.{
            @intCast(total_tokens),
            2,
        });
        try putRuntimeInput(allocator, self.cb, &rt, graph.w1_id, self.boundary_head.w1, &.{
            @intCast(self.config.boundary_mlp_dim),
            @intCast(self.config.hidden_size),
        });
        try putRuntimeInput(allocator, self.cb, &rt, graph.b1_id, self.boundary_head.b1, &.{
            @intCast(self.config.boundary_mlp_dim),
        });
        try putRuntimeInput(allocator, self.cb, &rt, graph.w2_id, self.boundary_head.w2, &.{
            2,
            @intCast(self.config.boundary_mlp_dim),
        });
        try putRuntimeInput(allocator, self.cb, &rt, graph.b2_id, self.boundary_head.b2, &.{2});

        // 3. Training step (autodiff + execute) — include "features" so autodiff
        //    computes dL/d(features) in addition to the head parameter gradients.
        self.optimizer_state.step_count = self.step_count + 1;
        var step_result = try training.trainStep(
            allocator,
            &graph.graph,
            graph.loss_id,
            self.cb,
            rt,
            .{ .trainable_params = &.{ "features", "w1", "b1", "w2", "b2" } },
        );
        defer step_result.deinit();

        const boundary_loss = step_result.loss;

        // 4. Get current learning rate
        const lr = self.lr_schedule.lr(self.step_count);

        // 5. Extract and dupe the features gradient before step_result.deinit().
        //    The gradient slice is owned by step_result and will be freed by deinit(),
        //    so we must copy it here.
        //    We use a var so we can null it out once ownership is transferred (Fix 4).
        var features_grad_owned: ?[]f32 = if (step_result.gradients.get("features")) |g|
            try allocator.dupe(f32, g)
        else
            null;
        errdefer if (features_grad_owned) |g| allocator.free(g);

        // 6. Accumulate gradients — scale by 1/accum_steps before adding
        const w1_grad = step_result.gradients.get("w1") orelse return error.MissingGradient;
        const b1_grad = step_result.gradients.get("b1") orelse return error.MissingGradient;
        const w2_grad = step_result.gradients.get("w2") orelse return error.MissingGradient;
        const b2_grad = step_result.gradients.get("b2") orelse return error.MissingGradient;

        const accum_steps = self.config.grad_accum_steps;
        const scale: f32 = 1.0 / @as(f32, @floatFromInt(@max(accum_steps, 1)));
        for (w1_grad, self.grad_accum_w1) |g, *a| a.* += g * scale;
        for (b1_grad, self.grad_accum_b1) |g, *a| a.* += g * scale;
        for (w2_grad, self.grad_accum_w2) |g, *a| a.* += g * scale;
        for (b2_grad, self.grad_accum_b2) |g, *a| a.* += g * scale;
        self.accum_count += 1;

        // Accumulate features_grad across microbatches for LoRA (Fix 4).
        // We null features_grad_owned after consuming it so the errdefer above
        // doesn't double-free on any subsequent error.
        if (accum_steps > 1) {
            if (features_grad_owned) |fgo| {
                const lora_scale = 1.0 / @as(f32, @floatFromInt(accum_steps));
                if (self.grad_accum_lora_features_grad_accum == null or
                    self.grad_accum_lora_features_grad_accum.?.len != fgo.len)
                {
                    if (self.grad_accum_lora_features_grad_accum) |old| self.allocator.free(old);
                    self.grad_accum_lora_features_grad_accum = try self.allocator.alloc(f32, fgo.len);
                    @memset(self.grad_accum_lora_features_grad_accum.?, 0);
                }
                for (self.grad_accum_lora_features_grad_accum.?, fgo) |*a, g| {
                    a.* += g * lora_scale;
                }
                // Free the per-step copy and null the variable so errdefer won't fire.
                self.allocator.free(fgo);
                features_grad_owned = null;
            }
        }

        var applied_lr: f32 = 0.0;
        if (self.accum_count >= accum_steps) {
            // Clip the accumulated gradient (global norm across all four tensors).
            {
                var total_sq: f64 = 0;
                for (self.grad_accum_w1) |g| total_sq += @as(f64, g * g);
                for (self.grad_accum_b1) |g| total_sq += @as(f64, g * g);
                for (self.grad_accum_w2) |g| total_sq += @as(f64, g * g);
                for (self.grad_accum_b2) |g| total_sq += @as(f64, g * g);
                const total_norm: f32 = @floatCast(@sqrt(total_sq));
                if (total_norm > self.config.max_grad_norm) {
                    const gscale = self.config.max_grad_norm / (total_norm + 1e-6);
                    for (self.grad_accum_w1) |*g| g.* *= gscale;
                    for (self.grad_accum_b1) |*g| g.* *= gscale;
                    for (self.grad_accum_w2) |*g| g.* *= gscale;
                    for (self.grad_accum_b2) |*g| g.* *= gscale;
                }
            }
            applied_lr = lr;
            if (self.config.use_schedule_free) {
                scheduleFreeAdamWStep(self.boundary_head.w1, self.grad_accum_w1, &self.sf_state_w1.?, lr, self.config.beta1, self.config.beta2, self.config.adam_epsilon, self.config.weight_decay, self.config.warmup_steps);
                scheduleFreeAdamWStep(self.boundary_head.b1, self.grad_accum_b1, &self.sf_state_b1.?, lr, self.config.beta1, self.config.beta2, self.config.adam_epsilon, self.config.weight_decay, self.config.warmup_steps);
                scheduleFreeAdamWStep(self.boundary_head.w2, self.grad_accum_w2, &self.sf_state_w2.?, lr, self.config.beta1, self.config.beta2, self.config.adam_epsilon, self.config.weight_decay, self.config.warmup_steps);
                scheduleFreeAdamWStep(self.boundary_head.b2, self.grad_accum_b2, &self.sf_state_b2.?, lr, self.config.beta1, self.config.beta2, self.config.adam_epsilon, self.config.weight_decay, self.config.warmup_steps);
            } else {
                try optimizers.step(self.optimizer, &self.optimizer_state, lr, "w1", self.boundary_head.w1, self.grad_accum_w1);
                try optimizers.step(self.optimizer, &self.optimizer_state, lr, "b1", self.boundary_head.b1, self.grad_accum_b1);
                try optimizers.step(self.optimizer, &self.optimizer_state, lr, "w2", self.boundary_head.w2, self.grad_accum_w2);
                try optimizers.step(self.optimizer, &self.optimizer_state, lr, "b2", self.boundary_head.b2, self.grad_accum_b2);
            }
            @memset(self.grad_accum_w1, 0);
            @memset(self.grad_accum_b1, 0);
            @memset(self.grad_accum_w2, 0);
            @memset(self.grad_accum_b2, 0);
            self.accum_count = 0;
        }

        // Resolve the features_grad to return to the caller (Fix 4).
        // When grad_accum_steps > 1, return the accumulated buffer only on the
        // optimizer step boundary; otherwise return nil so LoRA skips this step.
        const final_features_grad: ?[]f32 = if (accum_steps > 1) blk: {
            if (self.accum_count == 0) {
                // We just reset accum_count — this was the optimizer step boundary.
                // Hand ownership of the accumulated buffer to the caller.
                const buf = self.grad_accum_lora_features_grad_accum;
                self.grad_accum_lora_features_grad_accum = null;
                break :blk buf;
            }
            // Not yet at optimizer step — LoRA should skip this microbatch.
            break :blk null;
        } else features_grad_owned; // accum_steps == 1: return as-is

        // 7. CPU InfoNCE contrastive loss — with optional XBM expansion (Feature 1)
        var contrastive_result: infonce_cpu.ContrastiveLossResult = undefined;
        var xbm_expanded_embeddings: ?[]f32 = null;
        var xbm_expanded_doc_ids: ?[]u32 = null;
        defer if (xbm_expanded_embeddings) |e| allocator.free(e);
        defer if (xbm_expanded_doc_ids) |d| allocator.free(d);

        const eff_embeddings: []const f32 = blk: {
            if (self.xbm) |*xbm| {
                const stored = xbm.getStored();
                if (stored.count > 0) {
                    const n_current = B * C;
                    const n_total = n_current + stored.count;
                    const expanded = try allocator.alloc(f32, n_total * E);
                    xbm_expanded_embeddings = expanded;
                    @memcpy(expanded[0 .. n_current * E], chunk_embeddings);
                    @memcpy(expanded[n_current * E ..], stored.embeddings[0 .. stored.count * E]);
                    // Stored doc_ids already carry globally unique offsets (Fix 1).
                    const exp_ids = try allocator.alloc(u32, n_total);
                    xbm_expanded_doc_ids = exp_ids;
                    @memcpy(exp_ids[0..n_current], doc_ids);
                    @memcpy(exp_ids[n_current..], stored.doc_ids[0..stored.count]);
                    const exp_mask = try allocator.alloc(f32, n_total);
                    defer allocator.free(exp_mask);
                    @memcpy(exp_mask[0..n_current], chunk_mask[0..n_current]);
                    @memset(exp_mask[n_current..], 1.0);
                    contrastive_result = if (self.loss_config.use_mrl) mrl_blk: {
                        const mrl_config = infonce_cpu.MatryoshkaConfig{
                            .dims = self.loss_config.mrl_dims,
                            .weights = self.loss_config.mrl_weights,
                        };
                        const mrl = try infonce_cpu.computeMatryoshkaLossAndGrad(
                            allocator,
                            expanded,
                            exp_mask,
                            exp_ids,
                            n_total,
                            E,
                            mrl_config,
                            self.loss_config.temperature,
                            self.loss_config.focal_gamma,
                            self.loss_config.focal_alpha,
                        );
                        allocator.free(mrl.per_scale_loss);
                        break :mrl_blk infonce_cpu.ContrastiveLossResult{
                            .contrastive_loss = mrl.total_loss,
                            .total_loss = mrl.total_loss * @as(f64, self.loss_config.lambda_embed),
                            .grad = mrl.grad,
                        };
                    } else try infonce_cpu.computeContrastiveLossOnCPU(
                        allocator,
                        expanded,
                        exp_mask,
                        exp_ids,
                        @as(f64, self.loss_config.temperature),
                        @as(f64, self.loss_config.lambda_embed),
                        1,
                        n_total,
                        E,
                        @as(f64, self.loss_config.focal_gamma),
                        @as(f64, self.loss_config.focal_alpha),
                    );
                    break :blk expanded;
                }
            }
            contrastive_result = if (self.loss_config.use_mrl) mrl_blk: {
                const mrl_config = infonce_cpu.MatryoshkaConfig{
                    .dims = self.loss_config.mrl_dims,
                    .weights = self.loss_config.mrl_weights,
                };
                const mrl = try infonce_cpu.computeMatryoshkaLossAndGrad(
                    allocator,
                    chunk_embeddings,
                    chunk_mask,
                    doc_ids,
                    B * C,
                    E,
                    mrl_config,
                    self.loss_config.temperature,
                    self.loss_config.focal_gamma,
                    self.loss_config.focal_alpha,
                );
                allocator.free(mrl.per_scale_loss);
                break :mrl_blk infonce_cpu.ContrastiveLossResult{
                    .contrastive_loss = mrl.total_loss,
                    .total_loss = mrl.total_loss * @as(f64, self.loss_config.lambda_embed),
                    .grad = mrl.grad,
                };
            } else try infonce_cpu.computeContrastiveLossOnCPU(
                allocator,
                chunk_embeddings,
                chunk_mask,
                doc_ids,
                @as(f64, self.loss_config.temperature),
                @as(f64, self.loss_config.lambda_embed),
                B,
                C,
                E,
                @as(f64, self.loss_config.focal_gamma),
                @as(f64, self.loss_config.focal_alpha),
            );
            break :blk chunk_embeddings;
        };
        _ = eff_embeddings;
        defer contrastive_result.deinit(allocator);

        // Add current batch to XBM after computing loss (Feature 1 / Fix 1).
        if (self.xbm) |*xbm| {
            xbm.add(chunk_embeddings, doc_ids, chunk_mask, B * C, self.xbm_doc_id_base);
            self.xbm_doc_id_base +%= 100003;
        }

        // 8. Total loss
        const total_loss = self.loss_config.lambda_chunk * boundary_loss +
            @as(f32, @floatCast(contrastive_result.total_loss));

        // 9. Increment step count
        self.step_count += 1;
        std.log.info("fused_chunker step={d} boundary_loss={d:.4} total_loss={d:.4} lr={d}", .{ self.step_count, boundary_loss, total_loss, applied_lr });

        const summary = TrainStepSummary{
            .boundary_loss = boundary_loss,
            .contrastive_loss = contrastive_result.contrastive_loss,
            .total_loss = total_loss,
            .boundary_tp = 0,
            .boundary_fp = 0,
            .boundary_fn = 0,
            .step = self.step_count,
            .learning_rate = applied_lr,
        };

        return TrainStepWithGradSummary{
            .summary = summary,
            .features_grad = final_features_grad,
        };
    }

    /// Evaluate micro-F1 over a list of feature/label batches.
    ///
    /// features_list:      slice of [total_tokens * hidden_size] batches
    /// labels_list:        slice of [total_tokens * 2] one-hot batches
    /// mask_list:          slice of [total_tokens] attention mask batches
    /// total_tokens_list:  total tokens per batch
    pub fn evaluate(
        self: *FusedTrainer,
        allocator: std.mem.Allocator,
        features_list: []const []const f32,
        labels_list: []const []const f32,
        mask_list: []const []const f32,
        total_tokens_list: []const usize,
    ) !EvalSummary {
        var agg_tp: u64 = 0;
        var agg_fp: u64 = 0;
        var agg_fn: u64 = 0;
        var num_batches: u32 = 0;

        for (features_list, labels_list, mask_list, total_tokens_list) |features, labels, mask, total| {
            // Run forward-only: build graph, populate runtime inputs, run trainStep
            // with no trainable params to get loss (we only need logits here).
            // Use the simple CPU forward pass to get logits directly.
            const logits = try evaluateBoundaryLogitsSimple(
                allocator,
                &self.boundary_head,
                features,
                total,
            );
            defer allocator.free(logits);

            // Convert one-hot labels [total*2] -> scalar labels [total]
            const scalar_labels = try allocator.alloc(f32, total);
            defer allocator.free(scalar_labels);
            for (0..total) |i| {
                scalar_labels[i] = if (labels[i * 2 + 1] > 0.5) 1.0 else 0.0;
            }

            const metrics = fused_chunker_loss.computeBoundaryMetrics(logits, scalar_labels, mask);
            agg_tp += metrics.tp;
            agg_fp += metrics.fp;
            agg_fn += metrics.fn_;
            num_batches += 1;
        }

        const agg = fused_chunker_loss.BoundaryMetrics{
            .tp = agg_tp,
            .fp = agg_fp,
            .fn_ = agg_fn,
        };

        return EvalSummary{
            .boundary_f1 = agg.f1(),
            .boundary_precision = agg.precision(),
            .boundary_recall = agg.recall(),
            .num_batches = num_batches,
        };
    }

    /// Flush any partial gradient accumulation window at epoch end.
    ///
    /// Call this after the last `trainStep`/`trainStepWithEncoderGrad` of each epoch.
    /// If the number of training steps is not divisible by `grad_accum_steps`, the
    /// remaining accumulated gradients would otherwise be silently discarded.
    /// Has no effect when `accum_count == 0` (window is already empty).
    pub fn flushEpochEnd(self: *FusedTrainer, allocator: std.mem.Allocator) !void {
        if (self.accum_count == 0) return;
        const lr = self.config.lrSchedule().lr(self.step_count);
        // Clip accumulated gradients before the optimizer step.
        {
            var total_sq: f64 = 0;
            for (self.grad_accum_w1) |g| total_sq += @as(f64, g * g);
            for (self.grad_accum_b1) |g| total_sq += @as(f64, g * g);
            for (self.grad_accum_w2) |g| total_sq += @as(f64, g * g);
            for (self.grad_accum_b2) |g| total_sq += @as(f64, g * g);
            const total_norm: f32 = @floatCast(@sqrt(total_sq));
            if (total_norm > self.config.max_grad_norm) {
                const gscale = self.config.max_grad_norm / (total_norm + 1e-6);
                for (self.grad_accum_w1) |*g| g.* *= gscale;
                for (self.grad_accum_b1) |*g| g.* *= gscale;
                for (self.grad_accum_w2) |*g| g.* *= gscale;
                for (self.grad_accum_b2) |*g| g.* *= gscale;
            }
        }
        _ = allocator;
        if (self.config.use_schedule_free) {
            scheduleFreeAdamWStep(self.boundary_head.w1, self.grad_accum_w1, &self.sf_state_w1.?, lr, self.config.beta1, self.config.beta2, self.config.adam_epsilon, self.config.weight_decay, self.config.warmup_steps);
            scheduleFreeAdamWStep(self.boundary_head.b1, self.grad_accum_b1, &self.sf_state_b1.?, lr, self.config.beta1, self.config.beta2, self.config.adam_epsilon, self.config.weight_decay, self.config.warmup_steps);
            scheduleFreeAdamWStep(self.boundary_head.w2, self.grad_accum_w2, &self.sf_state_w2.?, lr, self.config.beta1, self.config.beta2, self.config.adam_epsilon, self.config.weight_decay, self.config.warmup_steps);
            scheduleFreeAdamWStep(self.boundary_head.b2, self.grad_accum_b2, &self.sf_state_b2.?, lr, self.config.beta1, self.config.beta2, self.config.adam_epsilon, self.config.weight_decay, self.config.warmup_steps);
        } else {
            try optimizers.step(self.optimizer, &self.optimizer_state, lr, "w1", self.boundary_head.w1, self.grad_accum_w1);
            try optimizers.step(self.optimizer, &self.optimizer_state, lr, "b1", self.boundary_head.b1, self.grad_accum_b1);
            try optimizers.step(self.optimizer, &self.optimizer_state, lr, "w2", self.boundary_head.w2, self.grad_accum_w2);
            try optimizers.step(self.optimizer, &self.optimizer_state, lr, "b2", self.boundary_head.b2, self.grad_accum_b2);
        }
        @memset(self.grad_accum_w1, 0);
        @memset(self.grad_accum_b1, 0);
        @memset(self.grad_accum_w2, 0);
        @memset(self.grad_accum_b2, 0);
        self.accum_count = 0;
    }

    /// Save boundary head weights to a SafeTensors checkpoint.
    ///
    /// The file is written using the SafeTensors format (see
    /// src/finetune/safetensors_checkpoint.zig) so that it can be read by
    /// standard tooling (Python safetensors library, Go readers, etc.).
    pub fn saveCheckpoint(self: *FusedTrainer, allocator: std.mem.Allocator, path: []const u8) !void {
        const st = @import("safetensors_checkpoint.zig");
        const tensors = [_]st.NamedTensor{
            .{
                .name = "w1",
                .data = self.boundary_head.w1,
                .shape = &.{ self.boundary_head.mlp_dim, self.boundary_head.hidden_dim },
            },
            .{
                .name = "b1",
                .data = self.boundary_head.b1,
                .shape = &.{self.boundary_head.mlp_dim},
            },
            .{
                .name = "w2",
                .data = self.boundary_head.w2,
                .shape = &.{ 2, self.boundary_head.mlp_dim },
            },
            .{
                .name = "b2",
                .data = self.boundary_head.b2,
                .shape = &.{2},
            },
        };
        try st.save(allocator, path, &tensors);
    }

    /// Load boundary head weights from a checkpoint file.
    ///
    /// Tries SafeTensors format first. If the file does not parse as SafeTensors
    /// (e.g. it was created by an older build using the legacy binary format),
    /// falls back to the original binary reader so that existing checkpoints
    /// remain usable.
    pub fn loadCheckpoint(self: *FusedTrainer, allocator: std.mem.Allocator, path: []const u8) !void {
        // Try SafeTensors format.
        if (self.loadCheckpointSafetensors(allocator, path)) |_| {
            return;
        } else |_| {}

        // Fallback: legacy binary format written by the old saveCheckpoint.
        // This keeps existing .bin checkpoints loadable after the format migration.
        return self.loadCheckpointBinary(allocator, path);
    }

    fn loadCheckpointSafetensors(self: *FusedTrainer, allocator: std.mem.Allocator, path: []const u8) !void {
        const safetensors = @import("../models/safetensors.zig");

        // Read the whole file into memory and hand ownership to MMapReader.
        // MMapReader.fromBytes stores the slice and frees it in deinit(), so we
        // must NOT also free it ourselves.
        const file_bytes = try compat.cwd().readFileAlloc(compat.io(), path, allocator, .unlimited);
        errdefer allocator.free(file_bytes);

        // fromBytes takes ownership of file_bytes (freed via deinit).
        var reader = try safetensors.MMapReader.fromBytes(allocator, file_bytes);
        defer reader.deinit();

        // Helper to copy tensor data into a pre-allocated destination slice.
        const targets = [_]struct {
            name: []const u8,
            dest: []f32,
        }{
            .{ .name = "w1", .dest = self.boundary_head.w1 },
            .{ .name = "b1", .dest = self.boundary_head.b1 },
            .{ .name = "w2", .dest = self.boundary_head.w2 },
            .{ .name = "b2", .dest = self.boundary_head.b2 },
        };

        for (targets) |tgt| {
            var tensor = try reader.readTensor(tgt.name);
            defer tensor.deinit();
            const src = tensor.asFloat32();
            if (src.len != tgt.dest.len) return error.CheckpointSizeMismatch;
            @memcpy(tgt.dest, src);
        }
    }

    fn loadCheckpointBinary(self: *FusedTrainer, allocator: std.mem.Allocator, path: []const u8) !void {
        const data = try compat.cwd().readFileAlloc(compat.io(), path, allocator, .unlimited);
        defer allocator.free(data);
        var offset: usize = 0;

        const tensor_targets = [_]struct {
            name: []const u8,
            dest: *[]f32,
        }{
            .{ .name = "w1", .dest = &self.boundary_head.w1 },
            .{ .name = "b1", .dest = &self.boundary_head.b1 },
            .{ .name = "w2", .dest = &self.boundary_head.w2 },
            .{ .name = "b2", .dest = &self.boundary_head.b2 },
        };

        for (tensor_targets) |tgt| {
            const name_len = try readU32(data, &offset);
            if (name_len > 256) return error.InvalidCheckpoint;
            if (offset + name_len > data.len) return error.IncompleteRead;
            var name_buf: [256]u8 = undefined;
            @memcpy(name_buf[0..name_len], data[offset .. offset + name_len]);
            offset += name_len;
            const name = name_buf[0..name_len];
            if (!std.mem.eql(u8, name, tgt.name)) return error.CheckpointNameMismatch;

            const size = try readU32(data, &offset);
            if (size != tgt.dest.*.len) return error.CheckpointSizeMismatch;
            for (tgt.dest.*) |*val| {
                val.* = @bitCast(try readU32(data, &offset));
            }
        }
    }

    fn readU32(data: []const u8, offset: *usize) !u32 {
        if (offset.* + 4 > data.len) return error.IncompleteRead;
        const value = std.mem.readInt(u32, data[offset.*..][0..4], .little);
        offset.* += 4;
        return value;
    }

    /// Save Adam optimizer state to a SafeTensors file.
    ///
    /// Saves m/v moment buffers for w1/b1/w2/b2 under names "adam_m_w1",
    /// "adam_v_w1", etc., plus "adam_step" as a 1-element f32 tensor.
    ///
    /// For Schedule-Free AdamW, also saves the z and v buffers as
    /// "sf_z_w1", "sf_v_w1", etc.
    pub fn saveOptimizerState(self: *FusedTrainer, allocator: std.mem.Allocator, path: []const u8) !void {
        const st = @import("safetensors_checkpoint.zig");

        var tensor_list = std.ArrayListUnmanaged(st.NamedTensor).empty;
        defer tensor_list.deinit(allocator);

        // Heap-allocated name strings that must outlive tensor_list.
        // We collect them here and free them all after save().
        var name_storage = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (name_storage.items) |n| allocator.free(n);
            name_storage.deinit(allocator);
        }

        // adam_step as a 1-element f32 scalar.
        const step_val = [1]f32{@as(f32, @floatFromInt(self.optimizer_state.step_count))};
        try tensor_list.append(allocator, .{
            .name = "adam_step",
            .data = &step_val,
            .shape = &.{1},
        });

        // AdamW moment buffers for each head parameter.
        const param_names = [_][]const u8{ "w1", "b1", "w2", "b2" };
        for (param_names) |pname| {
            if (self.optimizer_state.param_states.get(pname)) |ps| {
                const m_name = try std.fmt.allocPrint(allocator, "adam_m_{s}", .{pname});
                try name_storage.append(allocator, m_name);
                try tensor_list.append(allocator, .{
                    .name = m_name,
                    .data = ps.m,
                    .shape = &.{ps.m.len},
                });
                if (ps.v.len > 0) {
                    const v_name = try std.fmt.allocPrint(allocator, "adam_v_{s}", .{pname});
                    try name_storage.append(allocator, v_name);
                    try tensor_list.append(allocator, .{
                        .name = v_name,
                        .data = ps.v,
                        .shape = &.{ps.v.len},
                    });
                }
            }
        }

        // Schedule-Free states (if active).
        const sf_pairs = [_]struct {
            state: ?ScheduleFreeAdamWState,
            pname: []const u8,
        }{
            .{ .state = self.sf_state_w1, .pname = "w1" },
            .{ .state = self.sf_state_b1, .pname = "b1" },
            .{ .state = self.sf_state_w2, .pname = "w2" },
            .{ .state = self.sf_state_b2, .pname = "b2" },
        };
        for (sf_pairs) |pair| {
            if (pair.state) |sf| {
                const z_name = try std.fmt.allocPrint(allocator, "sf_z_{s}", .{pair.pname});
                try name_storage.append(allocator, z_name);
                const v_name = try std.fmt.allocPrint(allocator, "sf_v_{s}", .{pair.pname});
                try name_storage.append(allocator, v_name);
                try tensor_list.append(allocator, .{
                    .name = z_name,
                    .data = sf.z,
                    .shape = &.{sf.z.len},
                });
                try tensor_list.append(allocator, .{
                    .name = v_name,
                    .data = sf.v,
                    .shape = &.{sf.v.len},
                });
            }
        }

        try st.save(allocator, path, tensor_list.items);
    }

    /// Load Adam optimizer state from a SafeTensors file written by saveOptimizerState.
    pub fn loadOptimizerState(self: *FusedTrainer, allocator: std.mem.Allocator, path: []const u8) !void {
        const safetensors = @import("../models/safetensors.zig");

        const file_bytes = try compat.cwd().readFileAlloc(compat.io(), path, allocator, .unlimited);
        errdefer allocator.free(file_bytes);

        // fromBytes takes ownership of file_bytes (freed via deinit).
        var reader = try safetensors.MMapReader.fromBytes(allocator, file_bytes);
        defer reader.deinit();

        // Restore step count from the scalar tensor.
        if (reader.header.tensors.get("adam_step")) |_| {
            var step_tensor = try reader.readTensor("adam_step");
            defer step_tensor.deinit();
            const step_f32 = step_tensor.asFloat32();
            if (step_f32.len > 0) {
                const restored_step: u32 = @intFromFloat(step_f32[0]);
                self.optimizer_state.step_count = restored_step;
                // Keep the trainer-level step counter in sync.
                self.step_count = restored_step;
            }
        }

        // Restore AdamW moment buffers for each head parameter.
        const param_names = [_][]const u8{ "w1", "b1", "w2", "b2" };
        const param_sizes = [_]usize{
            self.boundary_head.w1.len,
            self.boundary_head.b1.len,
            self.boundary_head.w2.len,
            self.boundary_head.b2.len,
        };
        for (param_names, param_sizes) |pname, psize| {
            var m_name_buf: [32]u8 = undefined;
            var v_name_buf: [32]u8 = undefined;
            const m_name = try std.fmt.bufPrint(&m_name_buf, "adam_m_{s}", .{pname});
            const v_name = try std.fmt.bufPrint(&v_name_buf, "adam_v_{s}", .{pname});

            const has_m = reader.header.tensors.get(m_name) != null;
            const has_v = reader.header.tensors.get(v_name) != null;
            if (!has_m) continue;

            const ps = try self.optimizer_state.getOrCreate(pname, psize, has_v);

            var m_tensor = try reader.readTensor(m_name);
            defer m_tensor.deinit();
            const m_src = m_tensor.asFloat32();
            if (m_src.len == ps.m.len) @memcpy(ps.m, m_src);

            if (has_v) {
                var v_tensor = try reader.readTensor(v_name);
                defer v_tensor.deinit();
                const v_src = v_tensor.asFloat32();
                if (v_src.len == ps.v.len) @memcpy(ps.v, v_src);
            }
        }

        // Restore Schedule-Free states if present.
        const sf_entries = [_]struct {
            state: *?ScheduleFreeAdamWState,
            weights: []f32,
            pname: []const u8,
        }{
            .{ .state = &self.sf_state_w1, .weights = self.boundary_head.w1, .pname = "w1" },
            .{ .state = &self.sf_state_b1, .weights = self.boundary_head.b1, .pname = "b1" },
            .{ .state = &self.sf_state_w2, .weights = self.boundary_head.w2, .pname = "w2" },
            .{ .state = &self.sf_state_b2, .weights = self.boundary_head.b2, .pname = "b2" },
        };
        for (sf_entries) |entry| {
            var z_name_buf: [32]u8 = undefined;
            var v_name_buf: [32]u8 = undefined;
            const z_name = try std.fmt.bufPrint(&z_name_buf, "sf_z_{s}", .{entry.pname});
            const v_name = try std.fmt.bufPrint(&v_name_buf, "sf_v_{s}", .{entry.pname});

            const has_z = reader.header.tensors.get(z_name) != null;
            const has_v = reader.header.tensors.get(v_name) != null;
            if (!has_z or !has_v) continue;

            // Initialise the SF state if it doesn't exist yet.
            if (entry.state.* == null) {
                entry.state.* = try ScheduleFreeAdamWState.init(self.allocator, entry.weights);
            }

            const sf = &entry.state.*.?;
            var z_tensor = try reader.readTensor(z_name);
            defer z_tensor.deinit();
            const z_src = z_tensor.asFloat32();
            if (z_src.len == sf.z.len) @memcpy(sf.z, z_src);

            var v_tensor = try reader.readTensor(v_name);
            defer v_tensor.deinit();
            const v_src = v_tensor.asFloat32();
            if (v_src.len == sf.v.len) @memcpy(sf.v, v_src);
        }
    }
};

// ----------------------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------------------

/// Copy f32 data into a ComputeBackend tensor and insert it into the runtime map.
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

/// Per-tensor gradient L2 norm clipping (in-place).
fn clipGradient(grad: []f32, max_norm: f32) void {
    var norm_sq: f32 = 0;
    for (grad) |g| norm_sq += g * g;
    const norm = @sqrt(norm_sq);
    if (norm > max_norm) {
        const scale = max_norm / norm;
        for (grad) |*g| g.* *= scale;
    }
}

/// Write one tensor to a writer in the checkpoint binary format:
///   [name_len: u32 LE][name bytes][elem_count: u32 LE][f32 data as u32 LE...]
fn writeTensor(w: anytype, name: []const u8, data: []const f32) !void {
    try w.writeInt(u32, @intCast(name.len), .little);
    try w.writeAll(name);
    try w.writeInt(u32, @intCast(data.len), .little);
    for (data) |val| {
        try w.writeInt(u32, @bitCast(val), .little);
    }
}

/// Pure CPU forward pass through the 2-layer MLP boundary head.
///
/// Returns a freshly allocated [total * 2] f32 logit array (caller owns).
///
/// Architecture:
///   dense1 = features @ w1^T + b1    [total, mlp_dim]
///   hidden = gelu(dense1)
///   logits = hidden @ w2^T + b2      [total, 2]
fn evaluateBoundaryLogitsSimple(
    allocator: std.mem.Allocator,
    head: *const BoundaryHead,
    features: []const f32,
    total: usize,
) ![]f32 {
    const hidden_dim = head.hidden_dim;
    const mlp_dim = head.mlp_dim;

    // dense1 = features @ w1^T + b1   [total, mlp_dim]
    const dense1 = try allocator.alloc(f32, total * mlp_dim);
    defer allocator.free(dense1);

    for (0..total) |i| {
        for (0..mlp_dim) |j| {
            var acc: f32 = head.b1[j];
            for (0..hidden_dim) |k| {
                acc += features[i * hidden_dim + k] * head.w1[j * hidden_dim + k];
            }
            dense1[i * mlp_dim + j] = acc;
        }
    }

    // hidden = gelu(dense1)            [total, mlp_dim]
    const hidden = try allocator.alloc(f32, total * mlp_dim);
    defer allocator.free(hidden);

    for (dense1, hidden) |x, *h| {
        h.* = geluF32(x);
    }

    // logits = hidden @ w2^T + b2     [total, 2]
    const logits = try allocator.alloc(f32, total * 2);
    for (0..total) |i| {
        for (0..2) |j| {
            var acc: f32 = head.b2[j];
            for (0..mlp_dim) |k| {
                acc += hidden[i * mlp_dim + k] * head.w2[j * mlp_dim + k];
            }
            logits[i * 2 + j] = acc;
        }
    }

    return logits;
}

/// GELU activation: 0.5 * x * (1 + tanh(sqrt(2/pi) * (x + 0.044715 * x^3)))
inline fn geluF32(x: f32) f32 {
    const c: f32 = 0.7978845608028654; // sqrt(2/pi)
    const inner = c * (x + 0.044715 * x * x * x);
    return 0.5 * x * (1.0 + std.math.tanh(inner));
}

// ----------------------------------------------------------------------------
// Tests
// ----------------------------------------------------------------------------

test "BoundaryHead init and deinit" {
    const allocator = std.testing.allocator;

    var head = try BoundaryHead.init(allocator, 16, 8);
    defer head.deinit();

    try std.testing.expectEqual(@as(usize, 16), head.hidden_dim);
    try std.testing.expectEqual(@as(usize, 8), head.mlp_dim);
    try std.testing.expectEqual(@as(usize, 8 * 16), head.w1.len);
    try std.testing.expectEqual(@as(usize, 8), head.b1.len);
    try std.testing.expectEqual(@as(usize, 2 * 8), head.w2.len);
    try std.testing.expectEqual(@as(usize, 2), head.b2.len);

    // Biases should be zero
    for (head.b1) |v| try std.testing.expectEqual(@as(f32, 0.0), v);
    for (head.b2) |v| try std.testing.expectEqual(@as(f32, 0.0), v);

    // Weights should be non-zero (deterministic init)
    var any_nonzero = false;
    for (head.w1) |v| {
        if (v != 0.0) {
            any_nonzero = true;
            break;
        }
    }
    try std.testing.expect(any_nonzero);
}

test "FusedTrainingConfig lrSchedule warmup" {
    const config = FusedTrainingConfig{
        .learning_rate = 1e-4,
        .warmup_steps = 50,
        .total_steps = 1000,
    };

    const schedule = config.lrSchedule();

    const lr0 = schedule.lr(0);
    const lr25 = schedule.lr(25);
    const lr50 = schedule.lr(50);

    // During warmup: lr increases linearly from 0 to learning_rate
    try std.testing.expect(lr0 < lr25);
    try std.testing.expect(lr25 < lr50);

    // At step 0: lr should be 0 (linear warmup from 0)
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), lr0, 1e-8);

    // At end of warmup (step 50): lr should equal initial_lr
    try std.testing.expectApproxEqAbs(config.learning_rate, lr50, 1e-6);
}
