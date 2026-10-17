// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

//! Resident BF16 EmbeddingGemma2 LoRA encoder orchestration.
//!
//! `CudaCompute` owns verified frozen base tensors. `ResidentAdapterState`
//! owns every trainable tensor; this layer only coordinates their lifetimes.

const std = @import("std");
const platform = @import("antfly_platform");
const model = @import("../models/embedding_gemma2.zig");
const cuda_compute = @import("../ops/cuda/cuda_compute.zig");
const buffer_mod = @import("../ops/cuda/buffer.zig");
const control_mod = @import("../execution_control.zig");
const training = @import("embedding_gemma2_training.zig");
const planning = @import("embedding_gemma2_cuda.zig");
const session_mod = @import("embedding_gemma2_cuda_session.zig");
const tape_mod = @import("embedding_gemma2_cuda_tape.zig");
const projection = @import("embedding_gemma2_cuda_projection.zig");
const attention = @import("embedding_gemma2_cuda_attention.zig");
const arch = @import("../architectures/embedding_gemma2_reference.zig");

const CudaCompute = cuda_compute.CudaCompute;
const DeviceBuffer = buffer_mod.DeviceBuffer;
const InferenceExecutionControl = control_mod.InferenceExecutionControl;

pub const PaddedInput = struct {
    token_ids: []const i64,
    attention_mask: []const i64,
    batch: u32,
    sequence: u32,

    pub fn validate(self: PaddedInput, plan: training.TrainingPlan, vocab_size: u32) !void {
        if (self.batch == 0 or self.batch > 2 or self.sequence == 0 or self.sequence > plan.max_sequence)
            return error.InvalidEmbeddingGemma2EncoderInput;
        const rows = std.math.mul(usize, self.batch, self.sequence) catch return error.InvalidEmbeddingGemma2EncoderInput;
        if (self.token_ids.len != rows or self.attention_mask.len != rows)
            return error.InvalidEmbeddingGemma2EncoderInput;
        for (0..self.batch) |batch_index| {
            const start = batch_index * self.sequence;
            var saw_padding = false;
            var valid_tokens: u32 = 0;
            for (self.token_ids[start..][0..self.sequence], self.attention_mask[start..][0..self.sequence]) |token, mask| {
                if (mask != 0 and mask != 1) return error.InvalidEmbeddingGemma2EncoderInput;
                if (mask == 0) {
                    saw_padding = true;
                    continue;
                }
                if (saw_padding or token < 0 or token >= vocab_size)
                    return error.InvalidEmbeddingGemma2EncoderInput;
                valid_tokens += 1;
            }
            if (valid_tokens == 0) return error.InvalidEmbeddingGemma2EncoderInput;
        }
    }
};

pub const ResidentOutput = struct {
    buffer: DeviceBuffer,
    batch: u32,
    width: u32 = 768,
};

pub const Encoder = struct {
    allocator: std.mem.Allocator,
    compute: *CudaCompute,
    config: model.Config,
    training_plan: training.TrainingPlan,
    adapters: *session_mod.ResidentAdapterState,
    limits: planning.Limits,

    pub fn init(
        allocator: std.mem.Allocator,
        compute: *CudaCompute,
        config: model.Config,
        training_plan: training.TrainingPlan,
        adapters: *session_mod.ResidentAdapterState,
        limits: planning.Limits,
    ) !Encoder {
        try training_plan.validate(config);
        if (training_plan.checkpoint_every_n_layers != 2 or adapters.compute != compute or
            adapters.layout.rank != training_plan.rank or adapters.admission.adapter_count != 169)
            return error.InvalidEmbeddingGemma2EncoderState;
        try adapters.layout.validate();
        return .{
            .allocator = allocator,
            .compute = compute,
            .config = config,
            .training_plan = training_plan,
            .adapters = adapters,
            .limits = limits,
        };
    }

    pub fn releaseOutput(self: *Encoder, output: *ResidentOutput) void {
        self.compute.releaseEmbeddingGemma2TrainingBuffer(&output.buffer);
        output.* = undefined;
    }

    pub fn encode(self: *Encoder, input: PaddedInput, control: ?InferenceExecutionControl) !ResidentOutput {
        try self.checkInput(input, control);
        const rows = try checkedMul(input.batch, input.sequence);
        var tape = try tape_mod.ResidentTape.init(self.compute, input.batch, input.sequence);
        defer tape.deinit();
        var scratch = try ForwardScratch.init(self, rows, input.batch, input.sequence);
        defer scratch.deinit();
        errdefer self.compute.synchronizeEmbeddingGemma2Training() catch {};
        try scratch.uploadInput(self, input, try tape.layerCheckpoint(0));
        var segment: u32 = 0;
        while (segment < 12) : (segment += 1) {
            if (control) |value| try value.check();
            var view = try tape.rewind(segment * 2);
            const checkpoint = (try tape.layerCheckpoint(segment)).buffer;
            try forwardLayer(self, &scratch, &view, 0, segment * 2, checkpoint, (try tape.layerCheckpoint(0)).buffer);
            const second_input = (try view.slot(0, .layer_output_after_scalar)).buffer;
            try forwardLayer(self, &scratch, &view, 1, segment * 2 + 1, second_input, (try tape.layerCheckpoint(0)).buffer);
            const layer_output = (try view.slot(1, .layer_output_after_scalar)).buffer;
            const next = (try tape.layerCheckpoint(segment + 1)).buffer;
            try next.copyFromDevice(self.compute.embeddingGemma2TrainingContext(), layer_output, next.len);
        }
        const final_hidden = (try tape.layerCheckpoint(12)).buffer;
        const module = try self.compute.embeddingGemma2Module();
        const ctx = self.compute.embeddingGemma2TrainingContext();
        try module.launchTrainingRmsNorm(ctx, scratch.final_norm, final_hidden, try self.compute.embeddingGemma2Bf16Weight(arch.WeightNames.final_norm), @intCast(rows), 512, self.config.rms_norm_eps);
        try projection.forward(self.compute, self.adapters, embedding_projection_adapter_index, arch.WeightNames.output_projection, scratch.final_norm, rows, 512, 768, self.training_plan.alpha, .{
            .output_bf16 = scratch.projected,
            .low_rank_bf16 = scratch.low_rank,
            .delta_bf16 = scratch.delta,
            .workspace = scratch.workspace,
        });
        var output = ResidentOutput{
            .buffer = try self.compute.allocEmbeddingGemma2TrainingBuffer(try checkedBytes(input.batch, 768, @sizeOf(f32))),
            .batch = input.batch,
        };
        errdefer self.compute.releaseEmbeddingGemma2TrainingBuffer(&output.buffer);
        try module.launchTrainingMeanL2(ctx, output.buffer, scratch.projected, scratch.mask, input.batch, input.sequence, 768);
        if (control) |value| try value.check();
        try self.compute.synchronizeEmbeddingGemma2Training();
        if (control) |value| try value.check();
        return output;
    }

    /// Recompute the bounded forward graph and accumulate all 169 LoRA
    /// gradients into `current_grad`. `d_embedding_f32` is resident [B,768].
    pub fn replayBackward(self: *Encoder, input: PaddedInput, d_embedding_f32: DeviceBuffer, control: ?InferenceExecutionControl) !void {
        try self.checkInput(input, control);
        const rows = try checkedMul(input.batch, input.sequence);
        if (d_embedding_f32.ptr == 0 or d_embedding_f32.len < try checkedBytes(input.batch, 768, @sizeOf(f32)))
            return error.InvalidEmbeddingGemma2EncoderGradient;
        var tape = try tape_mod.ResidentTape.init(self.compute, input.batch, input.sequence);
        defer tape.deinit();
        var forward = try ForwardScratch.init(self, rows, input.batch, input.sequence);
        defer forward.deinit();
        var backward = try BackwardScratch.init(self, rows, input.batch, input.sequence);
        defer backward.deinit();
        errdefer self.compute.synchronizeEmbeddingGemma2Training() catch {};
        try forward.uploadInput(self, input, try tape.layerCheckpoint(0));
        try buildCheckpoints(self, &forward, &tape, control);
        const module = try self.compute.embeddingGemma2Module();
        const ctx = self.compute.embeddingGemma2TrainingContext();
        const final_hidden = (try tape.layerCheckpoint(12)).buffer;
        try module.launchTrainingRmsNorm(ctx, forward.final_norm, final_hidden, try self.compute.embeddingGemma2Bf16Weight(arch.WeightNames.final_norm), @intCast(rows), 512, self.config.rms_norm_eps);
        try projection.forward(self.compute, self.adapters, embedding_projection_adapter_index, arch.WeightNames.output_projection, forward.final_norm, rows, 512, 768, self.training_plan.alpha, .{ .output_bf16 = forward.projected, .low_rank_bf16 = forward.low_rank, .delta_bf16 = forward.delta, .workspace = forward.workspace });
        try module.launchMeanL2Vjp(ctx, backward.g[0], d_embedding_f32, forward.projected, forward.mask, input.batch, input.sequence, 768);
        try projection.backward(self.compute, self.adapters, embedding_projection_adapter_index, arch.WeightNames.output_projection, forward.final_norm, forward.low_rank, backward.g[0], rows, 512, 768, self.training_plan.alpha, backward.projectionBuffers(1, 2, 3, 4, 5, 6, forward.workspace));
        try module.launchTrainingRmsNormInputVjp(ctx, backward.g[0], backward.g[1], final_hidden, try self.compute.embeddingGemma2Bf16Weight(arch.WeightNames.final_norm), @intCast(rows), 512, self.config.rms_norm_eps);
        var segment: i32 = 11;
        while (segment >= 0) : (segment -= 1) {
            if (control) |value| try value.check();
            const start: u32 = @intCast(segment * 2);
            var view = try tape.rewind(start);
            const checkpoint = (try tape.layerCheckpoint(@intCast(segment))).buffer;
            try forwardLayer(self, &forward, &view, 0, start, checkpoint, (try tape.layerCheckpoint(0)).buffer);
            try forwardLayer(self, &forward, &view, 1, start + 1, (try view.slot(0, .layer_output_after_scalar)).buffer, (try tape.layerCheckpoint(0)).buffer);
            const immutable = (try tape.layerCheckpoint(0)).buffer;
            try backwardLayer(self, &forward, &backward, &view, 1, start + 1, immutable, backward.g[0]);
            try backwardLayer(self, &forward, &backward, &view, 0, start, immutable, backward.g[0]);
        }
        try self.compute.synchronizeEmbeddingGemma2Training();
        if (control) |value| try value.check();
    }

    pub fn deinit(self: *Encoder) void {
        self.compute.synchronizeEmbeddingGemma2Training() catch {};
        self.* = undefined;
    }

    fn checkInput(self: *const Encoder, input: PaddedInput, control: ?InferenceExecutionControl) !void {
        try input.validate(self.training_plan, self.config.vocab_size);
        if (control) |value| try value.check();
        _ = try planning.plan(self.config, self.training_plan, input.batch, input.sequence, self.limits);
    }
};

fn buildCheckpoints(encoder: *Encoder, scratch: *ForwardScratch, tape: *tape_mod.ResidentTape, control: ?InferenceExecutionControl) !void {
    var segment: u32 = 0;
    while (segment < 12) : (segment += 1) {
        if (control) |value| try value.check();
        var view = try tape.rewind(segment * 2);
        try forwardLayer(encoder, scratch, &view, 0, segment * 2, (try tape.layerCheckpoint(segment)).buffer, (try tape.layerCheckpoint(0)).buffer);
        try forwardLayer(encoder, scratch, &view, 1, segment * 2 + 1, (try view.slot(0, .layer_output_after_scalar)).buffer, (try tape.layerCheckpoint(0)).buffer);
        const next = (try tape.layerCheckpoint(segment + 1)).buffer;
        try next.copyFromDevice(encoder.compute.embeddingGemma2TrainingContext(), (try view.slot(1, .layer_output_after_scalar)).buffer, next.len);
    }
}

const BackwardScratch = struct {
    encoder: *Encoder,
    owned: std.ArrayListUnmanaged(DeviceBuffer) = .empty,
    g: [10]DeviceBuffer = @splat(.{}),
    score: DeviceBuffer = .{},

    fn init(encoder: *Encoder, rows: usize, batch: u32, sequence: u32) !BackwardScratch {
        var self = BackwardScratch{ .encoder = encoder };
        errdefer self.deinit();
        for (&self.g) |*value| value.* = try self.alloc(try checkedBytes(rows, 2048, @sizeOf(f32)));
        self.score = try self.alloc(try checkedBytes(try checkedMul(try checkedMul(batch, 4), sequence), sequence, @sizeOf(f32)));
        return self;
    }
    fn alloc(self: *BackwardScratch, bytes: usize) !DeviceBuffer {
        var value = try self.encoder.compute.allocEmbeddingGemma2TrainingBuffer(bytes);
        errdefer self.encoder.compute.releaseEmbeddingGemma2TrainingBuffer(&value);
        try self.owned.append(self.encoder.allocator, value);
        return value;
    }
    fn projectionBuffers(self: *BackwardScratch, dx: usize, scaled: usize, dy_bf16: usize, input: usize, low_rank: usize, dz: usize, workspace: DeviceBuffer) projection.BackwardBuffers {
        return .{ .dx_f32 = self.g[dx], .dy_scaled_f32 = self.g[scaled], .dy_bf16 = bf16View(self.g[dy_bf16]), .input_f32 = self.g[input], .low_rank_f32 = self.g[low_rank], .dz_f32 = self.g[dz], .workspace = workspace };
    }
    fn deinit(self: *BackwardScratch) void {
        self.encoder.compute.synchronizeEmbeddingGemma2Training() catch {};
        for (self.owned.items) |*value| self.encoder.compute.releaseEmbeddingGemma2TrainingBuffer(value);
        self.owned.deinit(self.encoder.allocator);
    }
};

fn backwardLayer(encoder: *Encoder, forward: *ForwardScratch, backward: *BackwardScratch, segment: *tape_mod.SegmentView, relative: u32, layer: u32, immutable: DeviceBuffer, dout: DeviceBuffer) !void {
    const rows = forward.rows;
    const count: u32 = @intCast(rows * 512);
    const geometry = try training.officialLayerGeometry(layer);
    const qw: usize = geometry.queryWidth();
    const kvw: usize = geometry.kvWidth();
    const module = try encoder.compute.embeddingGemma2Module();
    const ctx = encoder.compute.embeddingGemma2TrainingContext();
    var name: [256]u8 = undefined;
    const layer_input = (try segment.slot(relative, .layer_input)).buffer;
    const normed = (try segment.slot(relative, .attention_norm_input)).buffer;
    const ffn_input = (try segment.slot(relative, .ffn_norm_input)).buffer;
    const ffn_branch = (try segment.slot(relative, .ffn_residual)).buffer;
    const ple_activation = (try segment.slot(relative, .ple_gate_post_activation)).buffer;
    const scalar = try encoder.compute.embeddingGemma2Bf16Weight(try arch.layerWeightName(&name, layer, "layer_scalar"));
    try module.launchScaleDeviceF32Bf16(ctx, backward.g[1], dout, scalar, count);

    // Reconstruct the frozen PLE tail, then propagate its branch and residual.
    try module.launchTrainingRmsNorm(ctx, forward.a, ffn_branch, try encoder.compute.embeddingGemma2Bf16Weight(try arch.layerWeightName(&name, layer, "post_feedforward_layernorm.weight")), @intCast(rows), 512, encoder.config.rms_norm_eps);
    try module.launchResidual(ctx, forward.b, ffn_input, forward.a, null, count, 1, 0);
    const ple_all = try encoder.compute.embeddingGemma2Bf16Weight(arch.WeightNames.ple_projection);
    const ple_weight = DeviceBuffer{ .ptr = ple_all.ptr + @as(usize, layer) * 512 * 512 * 2, .len = 512 * 512 * 2 };
    try projection.baseForward(encoder.compute, ple_weight, immutable, forward.a, forward.workspace, rows, 512, 512);
    try module.launchScale(ctx, forward.a, forward.a, count, 1.0 / @sqrt(@as(f32, 512)));
    try module.launchTrainingRmsNorm(ctx, forward.c, forward.a, try encoder.compute.embeddingGemma2Bf16Weight(arch.WeightNames.ple_norm), @intCast(rows), 512, encoder.config.rms_norm_eps);
    const ple_gate_weight = try encoder.compute.embeddingGemma2Bf16Weight(try arch.layerWeightName(&name, layer, "ple_block.per_layer_input_gate.weight"));
    try projection.baseForward(encoder.compute, ple_gate_weight, forward.b, forward.a, forward.workspace, rows, 512, 512);
    const ple_proj_weight = try encoder.compute.embeddingGemma2Bf16Weight(try arch.layerWeightName(&name, layer, "ple_block.per_layer_projection.weight"));
    try projection.baseForward(encoder.compute, ple_proj_weight, ple_activation, forward.d, forward.workspace, rows, 512, 512);
    try module.launchResidualVjp(ctx, backward.g[2], backward.g[3], backward.g[1], null, count, 1, 0);
    try module.launchTrainingRmsNormInputVjp(ctx, backward.g[4], backward.g[3], forward.d, try encoder.compute.embeddingGemma2Bf16Weight(try arch.layerWeightName(&name, layer, "ple_block.post_per_layer_input_norm.weight")), @intCast(rows), 512, encoder.config.rms_norm_eps);
    try projection.baseInputVjp(encoder.compute, ple_proj_weight, backward.g[4], backward.g[5], bf16View(backward.g[6]), forward.workspace, rows, 512, 512);
    try module.launchGeluMulVjp(ctx, backward.g[6], backward.g[7], backward.g[5], forward.a, forward.c, count);
    try projection.baseInputVjp(encoder.compute, ple_gate_weight, backward.g[6], backward.g[8], bf16View(backward.g[9]), forward.workspace, rows, 512, 512);
    try module.launchAccumulateBf16BoundaryF32(ctx, backward.g[2], backward.g[8], count);

    // FFN residual, trained down/gate/up projections, and input norm.
    try module.launchResidualVjp(ctx, backward.g[3], backward.g[4], backward.g[2], null, count, 1, 0);
    try module.launchTrainingRmsNormInputVjp(ctx, backward.g[5], backward.g[4], ffn_branch, try encoder.compute.embeddingGemma2Bf16Weight(try arch.layerWeightName(&name, layer, "post_feedforward_layernorm.weight")), @intCast(rows), 512, encoder.config.rms_norm_eps);
    try module.launchTrainingRmsNorm(ctx, forward.a, ffn_input, try encoder.compute.embeddingGemma2Bf16Weight(try arch.layerWeightName(&name, layer, "pre_feedforward_layernorm.weight")), @intCast(rows), 512, encoder.config.rms_norm_eps);
    try loraBackward(encoder, forward, backward, layer, .down_proj, (try segment.slot(relative, .gate_post_activation)).buffer, backward.g[5], rows, 2048, 512, 6, .{ 4, 7, 8, 2, 9 }, &name);
    try module.launchGeluMulVjp(ctx, backward.g[4], backward.g[5], backward.g[6], (try segment.slot(relative, .gate_pre_activation)).buffer, (try segment.slot(relative, .up_projection)).buffer, @intCast(rows * 2048));
    try loraBackward(encoder, forward, backward, layer, .gate_proj, forward.a, backward.g[4], rows, 512, 2048, 6, .{ 7, 8, 9, 2, 4 }, &name);
    try loraBackward(encoder, forward, backward, layer, .up_proj, forward.a, backward.g[5], rows, 512, 2048, 7, .{ 8, 9, 2, 4, 5 }, &name);
    try module.launchAccumulateBf16BoundaryF32(ctx, backward.g[6], backward.g[7], count);
    try module.launchTrainingRmsNormInputVjp(ctx, backward.g[7], backward.g[6], ffn_input, try encoder.compute.embeddingGemma2Bf16Weight(try arch.layerWeightName(&name, layer, "pre_feedforward_layernorm.weight")), @intCast(rows), 512, encoder.config.rms_norm_eps);
    try module.launchAccumulateBf16BoundaryF32(ctx, backward.g[3], backward.g[7], count);

    // Attention residual and output projection.
    const attention_branch = (try segment.slot(relative, .attention_residual)).buffer;
    try module.launchResidualVjp(ctx, backward.g[8], backward.g[5], backward.g[3], null, count, 1, 0);
    try module.launchTrainingRmsNormInputVjp(ctx, backward.g[6], backward.g[5], attention_branch, try encoder.compute.embeddingGemma2Bf16Weight(try arch.layerWeightName(&name, layer, "post_attention_layernorm.weight")), @intCast(rows), 512, encoder.config.rms_norm_eps);
    try loraBackward(encoder, forward, backward, layer, .o_proj, (try segment.slot(relative, .attention_context)).buffer, backward.g[6], rows, qw, 512, 7, .{ 3, 9, 1, 2, 6 }, &name);
    const shape = attention.Shape{ .batch = forward.batch, .sequence = forward.sequence, .kv_heads = geometry.kv_heads, .head_dim = geometry.head_dim, .local_radius = if (geometry.kind == .sliding_attention) 512 else 0 };
    try attention.backward(encoder.compute, (try segment.slot(relative, .q_post_rope)).buffer, (try segment.slot(relative, .k_post_rope)).buffer, (try segment.slot(relative, .v_post_norm)).buffer, (try segment.slot(relative, .attention_probabilities)).buffer, backward.g[7], shape, .{ .q_packed_f32 = forward.q_packed, .k_packed_f32 = forward.k_packed, .v_packed_f32 = forward.v_packed, .dcontext_packed_f32 = forward.context_packed, .dprobabilities_f32 = forward.scores, .dscores_f32 = backward.score, .dq_packed_f32 = backward.g[1], .dk_packed_f32 = backward.g[2], .dv_packed_f32 = backward.g[3], .dq_f32 = backward.g[4], .dk_f32 = backward.g[5], .dv_f32 = backward.g[6], .workspace = forward.workspace });
    const layer_config = try encoder.config.layer(layer);
    try module.launchTrainingRopeVjp(ctx, backward.g[1], backward.g[2], backward.g[4], backward.g[5], forward.mask, .{ .batch = shape.batch, .seq = shape.sequence, .q_heads = 4, .kv_heads = shape.kv_heads, .head_dim = shape.head_dim, .local_radius = shape.local_radius }, layer_config.rope_theta, 1);
    try module.launchTrainingRmsNormInputVjp(ctx, backward.g[3], backward.g[1], (try segment.slot(relative, .q_pre_norm)).buffer, try encoder.compute.embeddingGemma2Bf16Weight(try arch.layerWeightName(&name, layer, "self_attn.q_norm.weight")), @intCast(rows * 4), geometry.head_dim, encoder.config.rms_norm_eps);
    try module.launchTrainingRmsNormInputVjp(ctx, backward.g[4], backward.g[2], (try segment.slot(relative, .k_pre_norm)).buffer, try encoder.compute.embeddingGemma2Bf16Weight(try arch.layerWeightName(&name, layer, "self_attn.k_norm.weight")), @intCast(rows * geometry.kv_heads), geometry.head_dim, encoder.config.rms_norm_eps);
    try loraForward(encoder, forward, layer, .v_proj, normed, forward.d, rows, 512, kvw, &name);
    try module.launchTrainingRmsNormInputVjp(ctx, backward.g[5], backward.g[6], forward.d, null, @intCast(rows * geometry.kv_heads), geometry.head_dim, encoder.config.rms_norm_eps);
    try loraBackward(encoder, forward, backward, layer, .q_proj, normed, backward.g[3], rows, 512, qw, 6, .{ 1, 2, 7, 9, 3 }, &name);
    try loraBackward(encoder, forward, backward, layer, .k_proj, normed, backward.g[4], rows, 512, kvw, 7, .{ 1, 2, 9, 3, 4 }, &name);
    try loraBackward(encoder, forward, backward, layer, .v_proj, normed, backward.g[5], rows, 512, kvw, 9, .{ 1, 2, 3, 4, 5 }, &name);
    // Eager autograd schedules the shared Q/K/V projection input in reverse
    // forward order: V, K, then Q. BF16 addition is non-associative, so retain
    // the measured `(V + K) + Q` grouping rather than `(Q + K) + V`.
    try module.launchAccumulateBf16BoundaryF32(ctx, backward.g[7], backward.g[9], count);
    try module.launchAccumulateBf16BoundaryF32(ctx, backward.g[6], backward.g[7], count);
    try module.launchTrainingRmsNormInputVjp(ctx, backward.g[7], backward.g[6], layer_input, try encoder.compute.embeddingGemma2Bf16Weight(try arch.layerWeightName(&name, layer, "input_layernorm.weight")), @intCast(rows), 512, encoder.config.rms_norm_eps);
    try module.launchAccumulateBf16BoundaryF32(ctx, backward.g[8], backward.g[7], count);
    if (dout.ptr != backward.g[8].ptr) try dout.copyFromDevice(ctx, backward.g[8], try checkedBytes(rows, 512, @sizeOf(f32)));
}

const ProjectionTemps = [5]usize;

fn loraBackward(encoder: *Encoder, forward: *ForwardScratch, backward: *BackwardScratch, layer: u32, target: training.Target, input: DeviceBuffer, dy: DeviceBuffer, rows: usize, in_dim: usize, out_dim: usize, dx: usize, temps: ProjectionTemps, name_buffer: []u8) !void {
    const specs = try training.projectionSpecs(layer);
    const spec = specs[@backingInt(target)];
    const name = try spec.name(name_buffer, layer);
    try projection.forward(encoder.compute, encoder.adapters, try layerAdapterIndex(layer, target), name, input, rows, in_dim, out_dim, encoder.training_plan.alpha, .{ .output_bf16 = forward.d, .low_rank_bf16 = forward.low_rank, .delta_bf16 = forward.delta, .workspace = forward.workspace });
    try projection.backward(encoder.compute, encoder.adapters, try layerAdapterIndex(layer, target), name, input, forward.low_rank, dy, rows, in_dim, out_dim, encoder.training_plan.alpha, backward.projectionBuffers(dx, temps[0], temps[1], temps[2], temps[3], temps[4], forward.workspace));
}

fn bf16View(buffer: DeviceBuffer) DeviceBuffer {
    return .{ .ptr = buffer.ptr, .len = buffer.len };
}

const ForwardScratch = struct {
    encoder: *Encoder,
    batch: u32,
    sequence: u32,
    rows: usize,
    owned: std.ArrayListUnmanaged(DeviceBuffer) = .empty,
    mask: DeviceBuffer,
    ids: DeviceBuffer,
    workspace: DeviceBuffer,
    low_rank: DeviceBuffer,
    delta: DeviceBuffer,
    a: DeviceBuffer,
    b: DeviceBuffer,
    c: DeviceBuffer,
    d: DeviceBuffer,
    q_packed: DeviceBuffer,
    k_packed: DeviceBuffer,
    v_packed: DeviceBuffer,
    scores: DeviceBuffer,
    context_packed: DeviceBuffer,
    final_norm: DeviceBuffer,
    projected: DeviceBuffer,

    fn init(encoder: *Encoder, rows: usize, batch: u32, sequence: u32) !ForwardScratch {
        var self = ForwardScratch{ .encoder = encoder, .batch = batch, .sequence = sequence, .rows = rows, .mask = .{}, .ids = .{}, .workspace = .{}, .low_rank = .{}, .delta = .{}, .a = .{}, .b = .{}, .c = .{}, .d = .{}, .q_packed = .{}, .k_packed = .{}, .v_packed = .{}, .scores = .{}, .context_packed = .{}, .final_norm = .{}, .projected = .{} };
        errdefer self.deinit();
        self.mask = try self.alloc(try checkedBytes(rows, 1, @sizeOf(i64)));
        self.ids = try self.alloc(try checkedBytes(rows, 1, @sizeOf(i64)));
        self.workspace = try self.alloc(32 * 1024 * 1024);
        self.low_rank = try self.alloc(try checkedBytes(rows, encoder.training_plan.rank, @sizeOf(u16)));
        self.delta = try self.alloc(try checkedBytes(rows, 2048, @sizeOf(u16)));
        self.a = try self.alloc(try checkedBytes(rows, 2048, @sizeOf(u16)));
        self.b = try self.alloc(try checkedBytes(rows, 2048, @sizeOf(u16)));
        self.c = try self.alloc(try checkedBytes(rows, 2048, @sizeOf(u16)));
        self.d = try self.alloc(try checkedBytes(rows, 2048, @sizeOf(u16)));
        self.q_packed = try self.alloc(try checkedBytes(rows, 2048, @sizeOf(f32)));
        self.k_packed = try self.alloc(try checkedBytes(rows, 512, @sizeOf(f32)));
        self.v_packed = try self.alloc(try checkedBytes(rows, 512, @sizeOf(f32)));
        self.scores = try self.alloc(try checkedBytes(try checkedMul(try checkedMul(batch, 4), sequence), sequence, @sizeOf(f32)));
        self.context_packed = try self.alloc(try checkedBytes(rows, 2048, @sizeOf(f32)));
        self.final_norm = try self.alloc(try checkedBytes(rows, 512, @sizeOf(u16)));
        self.projected = try self.alloc(try checkedBytes(rows, 768, @sizeOf(u16)));
        return self;
    }

    fn alloc(self: *ForwardScratch, bytes: usize) !DeviceBuffer {
        var value = try self.encoder.compute.allocEmbeddingGemma2TrainingBuffer(bytes);
        errdefer self.encoder.compute.releaseEmbeddingGemma2TrainingBuffer(&value);
        try self.owned.append(self.encoder.allocator, value);
        return value;
    }
    fn deinit(self: *ForwardScratch) void {
        self.encoder.compute.synchronizeEmbeddingGemma2Training() catch {};
        for (self.owned.items) |*value| self.encoder.compute.releaseEmbeddingGemma2TrainingBuffer(value);
        self.owned.deinit(self.encoder.allocator);
    }
    fn uploadInput(self: *ForwardScratch, encoder: *Encoder, input: PaddedInput, checkpoint: tape_mod.TypedView) !void {
        const ctx = encoder.compute.embeddingGemma2TrainingContext();
        try self.mask.copyFromHost(ctx, std.mem.sliceAsBytes(input.attention_mask));
        try self.ids.copyFromHost(ctx, std.mem.sliceAsBytes(input.token_ids));
        const module = try encoder.compute.embeddingGemma2Module();
        try module.launchLookup(ctx, checkpoint.buffer, try encoder.compute.embeddingGemma2Bf16Weight(arch.WeightNames.token_embedding), self.ids, @intCast(input.token_ids.len), encoder.config.vocab_size, 512, 22.625);
    }
};

fn forwardLayer(encoder: *Encoder, scratch: *ForwardScratch, segment: *tape_mod.SegmentView, relative: u32, layer: u32, input: DeviceBuffer, immutable: DeviceBuffer) !void {
    const rows = scratch.rows;
    const geometry = try training.officialLayerGeometry(layer);
    const query_width: usize = geometry.queryWidth();
    const kv_width: usize = geometry.kvWidth();
    const module = try encoder.compute.embeddingGemma2Module();
    const ctx = encoder.compute.embeddingGemma2TrainingContext();
    const layer_input = (try segment.slot(relative, .layer_input)).buffer;
    try layer_input.copyFromDevice(ctx, input, try checkedBytes(rows, 512, @sizeOf(u16)));
    const normed = (try segment.slot(relative, .attention_norm_input)).buffer;
    var name: [256]u8 = undefined;
    try module.launchTrainingRmsNorm(ctx, normed, layer_input, try encoder.compute.embeddingGemma2Bf16Weight(try arch.layerWeightName(&name, layer, "input_layernorm.weight")), @intCast(rows), 512, encoder.config.rms_norm_eps);
    const q_pre = (try segment.slot(relative, .q_pre_norm)).buffer;
    const k_pre = (try segment.slot(relative, .k_pre_norm)).buffer;
    const q_rot = (try segment.slot(relative, .q_post_rope)).buffer;
    const k_rot = (try segment.slot(relative, .k_post_rope)).buffer;
    const v_norm = (try segment.slot(relative, .v_post_norm)).buffer;
    try loraForward(encoder, scratch, layer, .q_proj, normed, q_pre, rows, 512, query_width, &name);
    try loraForward(encoder, scratch, layer, .k_proj, normed, k_pre, rows, 512, kv_width, &name);
    try loraForward(encoder, scratch, layer, .v_proj, normed, scratch.a, rows, 512, kv_width, &name);
    try module.launchTrainingRmsNorm(ctx, q_rot, q_pre, try encoder.compute.embeddingGemma2Bf16Weight(try arch.layerWeightName(&name, layer, "self_attn.q_norm.weight")), @intCast(rows * 4), geometry.head_dim, encoder.config.rms_norm_eps);
    try module.launchTrainingRmsNorm(ctx, k_rot, k_pre, try encoder.compute.embeddingGemma2Bf16Weight(try arch.layerWeightName(&name, layer, "self_attn.k_norm.weight")), @intCast(rows * geometry.kv_heads), geometry.head_dim, encoder.config.rms_norm_eps);
    try module.launchTrainingRmsNorm(ctx, v_norm, scratch.a, null, @intCast(rows * geometry.kv_heads), geometry.head_dim, encoder.config.rms_norm_eps);
    const shape = attention.Shape{ .batch = scratch.batch, .sequence = scratch.sequence, .kv_heads = geometry.kv_heads, .head_dim = geometry.head_dim, .local_radius = if (geometry.kind == .sliding_attention) 512 else 0 };
    const layer_config = try encoder.config.layer(layer);
    try module.launchTrainingRope(ctx, q_rot, k_rot, scratch.mask, .{ .batch = shape.batch, .seq = shape.sequence, .q_heads = 4, .kv_heads = shape.kv_heads, .head_dim = shape.head_dim, .local_radius = shape.local_radius }, layer_config.rope_theta, 1);
    const probabilities = (try segment.slot(relative, .attention_probabilities)).buffer;
    const context = (try segment.slot(relative, .attention_context)).buffer;
    try attention.forward(encoder.compute, q_rot, k_rot, v_norm, scratch.mask, shape, .{ .q_packed_f32 = scratch.q_packed, .k_packed_f32 = scratch.k_packed, .v_packed_f32 = scratch.v_packed, .scores_f32 = scratch.scores, .probabilities_f32 = probabilities, .context_packed_f32 = scratch.context_packed, .context_bf16 = context, .workspace = scratch.workspace });
    const attention_branch = (try segment.slot(relative, .attention_residual)).buffer;
    try loraForward(encoder, scratch, layer, .o_proj, context, attention_branch, rows, query_width, 512, &name);
    try module.launchTrainingRmsNorm(ctx, scratch.a, attention_branch, try encoder.compute.embeddingGemma2Bf16Weight(try arch.layerWeightName(&name, layer, "post_attention_layernorm.weight")), @intCast(rows), 512, encoder.config.rms_norm_eps);
    const ffn_input = (try segment.slot(relative, .ffn_norm_input)).buffer;
    try module.launchResidual(ctx, ffn_input, layer_input, scratch.a, null, @intCast(rows * 512), 1, 0);
    try module.launchTrainingRmsNorm(ctx, scratch.a, ffn_input, try encoder.compute.embeddingGemma2Bf16Weight(try arch.layerWeightName(&name, layer, "pre_feedforward_layernorm.weight")), @intCast(rows), 512, encoder.config.rms_norm_eps);
    const gate = (try segment.slot(relative, .gate_pre_activation)).buffer;
    const up = (try segment.slot(relative, .up_projection)).buffer;
    try loraForward(encoder, scratch, layer, .gate_proj, scratch.a, gate, rows, 512, 2048, &name);
    try loraForward(encoder, scratch, layer, .up_proj, scratch.a, up, rows, 512, 2048, &name);
    const activated = (try segment.slot(relative, .gate_post_activation)).buffer;
    try module.launchGeluMul(ctx, activated, gate, up, @intCast(rows * 2048));
    const ffn_branch = (try segment.slot(relative, .ffn_residual)).buffer;
    try loraForward(encoder, scratch, layer, .down_proj, activated, ffn_branch, rows, 2048, 512, &name);
    try module.launchTrainingRmsNorm(ctx, scratch.a, ffn_branch, try encoder.compute.embeddingGemma2Bf16Weight(try arch.layerWeightName(&name, layer, "post_feedforward_layernorm.weight")), @intCast(rows), 512, encoder.config.rms_norm_eps);
    try module.launchResidual(ctx, scratch.b, ffn_input, scratch.a, null, @intCast(rows * 512), 1, 0);
    const ple_all = try encoder.compute.embeddingGemma2Bf16Weight(arch.WeightNames.ple_projection);
    const ple_weight = DeviceBuffer{ .ptr = ple_all.ptr + @as(usize, layer) * 512 * 512 * 2, .len = 512 * 512 * 2 };
    try projection.baseForward(encoder.compute, ple_weight, immutable, scratch.a, scratch.workspace, rows, 512, 512);
    try module.launchScale(ctx, scratch.a, scratch.a, @intCast(rows * 512), 1.0 / @sqrt(@as(f32, 512)));
    try module.launchTrainingRmsNorm(ctx, scratch.c, scratch.a, try encoder.compute.embeddingGemma2Bf16Weight(arch.WeightNames.ple_norm), @intCast(rows), 512, encoder.config.rms_norm_eps);
    try projection.baseForward(encoder.compute, try encoder.compute.embeddingGemma2Bf16Weight(try arch.layerWeightName(&name, layer, "ple_block.per_layer_input_gate.weight")), scratch.b, scratch.a, scratch.workspace, rows, 512, 512);
    const ple_activation = (try segment.slot(relative, .ple_gate_post_activation)).buffer;
    try module.launchGeluPle(ctx, ple_activation, scratch.a, scratch.c, @intCast(rows * 512));
    try projection.baseForward(encoder.compute, try encoder.compute.embeddingGemma2Bf16Weight(try arch.layerWeightName(&name, layer, "ple_block.per_layer_projection.weight")), ple_activation, scratch.a, scratch.workspace, rows, 512, 512);
    try module.launchTrainingRmsNorm(ctx, scratch.c, scratch.a, try encoder.compute.embeddingGemma2Bf16Weight(try arch.layerWeightName(&name, layer, "ple_block.post_per_layer_input_norm.weight")), @intCast(rows), 512, encoder.config.rms_norm_eps);
    const output = (try segment.slot(relative, .layer_output_after_scalar)).buffer;
    try module.launchResidual(ctx, output, scratch.b, scratch.c, null, @intCast(rows * 512), 1, 0);
    try module.launchScaleDevice(ctx, output, output, try encoder.compute.embeddingGemma2Bf16Weight(try arch.layerWeightName(&name, layer, "layer_scalar")), @intCast(rows * 512));
}

fn loraForward(encoder: *Encoder, scratch: *ForwardScratch, layer: u32, target: training.Target, input: DeviceBuffer, output: DeviceBuffer, rows: usize, input_dim: usize, output_dim: usize, name_buffer: []u8) !void {
    const specs = try training.projectionSpecs(layer);
    const spec = specs[@backingInt(target)];
    const name = try spec.name(name_buffer, layer);
    try projection.forward(encoder.compute, encoder.adapters, try layerAdapterIndex(layer, target), name, input, rows, input_dim, output_dim, encoder.training_plan.alpha, .{ .output_bf16 = output, .low_rank_bf16 = scratch.low_rank, .delta_bf16 = scratch.delta, .workspace = scratch.workspace });
}

fn checkedMul(a: anytype, b: anytype) !usize {
    return std.math.mul(usize, @intCast(a), @intCast(b)) catch error.EmbeddingGemma2EncoderOverflow;
}
fn checkedBytes(rows: anytype, width: anytype, size: usize) !usize {
    return checkedMul(try checkedMul(rows, width), size);
}

pub fn layerAdapterIndex(layer: u32, target: training.Target) !usize {
    if (layer >= 24 or target == .embedding_projection) return error.InvalidEmbeddingGemma2AdapterIndex;
    const offset: usize = switch (target) {
        .q_proj => 0,
        .k_proj => 1,
        .v_proj => 2,
        .o_proj => 3,
        .gate_proj => 4,
        .up_proj => 5,
        .down_proj => 6,
        .embedding_projection => unreachable,
    };
    return @as(usize, layer) * 7 + offset;
}

pub const embedding_projection_adapter_index: usize = 24 * 7;

test "EmbeddingGemma2 resident encoder validates padded batches" {
    const plan = training.TrainingPlan{};
    const valid = PaddedInput{
        .token_ids = &.{ 2, 9, 1, 0, 2, 8, 1, 0 },
        .attention_mask = &.{ 1, 1, 1, 0, 1, 1, 1, 0 },
        .batch = 2,
        .sequence = 4,
    };
    try valid.validate(plan, 32);
    var bad = valid;
    bad.attention_mask = &.{ 1, 0, 1, 0, 1, 1, 1, 0 };
    try std.testing.expectError(error.InvalidEmbeddingGemma2EncoderInput, bad.validate(plan, 32));
    bad = valid;
    bad.token_ids = &.{ 2, 33, 1, 0, 2, 8, 1, 0 };
    try std.testing.expectError(error.InvalidEmbeddingGemma2EncoderInput, bad.validate(plan, 32));
}

test "EmbeddingGemma2 resident encoder adapter ordering is exact" {
    try std.testing.expectEqual(@as(usize, 0), try layerAdapterIndex(0, .q_proj));
    try std.testing.expectEqual(@as(usize, 6), try layerAdapterIndex(0, .down_proj));
    try std.testing.expectEqual(@as(usize, 167), try layerAdapterIndex(23, .down_proj));
    try std.testing.expectEqual(@as(usize, 168), embedding_projection_adapter_index);
    try std.testing.expectError(error.InvalidEmbeddingGemma2AdapterIndex, layerAdapterIndex(24, .q_proj));
}

pub fn compileProbe() !void {
    // This branch is runtime-false but intentionally not comptime-known: Zig
    // must analyze the complete resident path without requiring a CUDA device.
    if (platform.env.getenvBoolDefault("ANTFLY_TEST_INSTANTIATE_EG2_ENCODER", false)) {
        var encoder: Encoder = undefined;
        var output = try encoder.encode(undefined, null);
        try encoder.replayBackward(undefined, undefined, null);
        encoder.releaseOutput(&output);
        encoder.deinit();
    }
}

test "EmbeddingGemma2 resident encoder public methods remain fully instantiated" {
    try compileProbe();
}
