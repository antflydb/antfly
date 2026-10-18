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

//! Admission and identity contracts for the resident CUDA EmbeddingGemma 2
//! trainer.  Device execution is attached to CudaCompute; this module stays
//! independent of the CUDA driver so job planning can reject an impossible
//! run before loading the checkpoint.

const std = @import("std");
const model = @import("../models/embedding_gemma2.zig");
const training = @import("embedding_gemma2_training.zig");

pub const projection_count_per_layer = 7;
pub const checkpoint_layers = 2;

pub const Limits = struct {
    max_device_bytes: usize = 20 * 1024 * 1024 * 1024,
    max_host_bytes: usize = 8 * 1024 * 1024 * 1024,
    max_logical_batch: u32 = 64,
    max_microbatch: u32 = 2,
    max_candidates_per_query: u32 = 6,
    /// Already-resident immutable model weights and mirrors. The job driver
    /// should replace this conservative default with backend allocation stats.
    resident_base_bytes: usize = 768 * 1024 * 1024,
};

pub const Admission = struct {
    adapter_count: usize,
    adapter_elements: usize,
    persistent_adapter_bytes: usize,
    checkpoint_bytes: usize,
    attention_workspace_bytes: usize,
    segment_workspace_bytes: usize,
    executor_workspace_bytes: usize,
    objective_workspace_bytes: usize,
    device_upper_bound_bytes: usize,
    resident_base_bytes: usize,
};

fn add(a: usize, b: usize) !usize {
    return std.math.add(usize, a, b) catch error.EmbeddingGemma2TrainingLimitExceeded;
}
fn mul(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b) catch error.EmbeddingGemma2TrainingLimitExceeded;
}
fn adapterElements(input: usize, output: usize, rank: usize) !usize {
    return mul(rank, try add(input, output));
}

/// Exact persistent payload for each trainable value: FP32 master, FP32
/// gradient accumulator, FP32 Adam m/v, FP32 current-step gradient, and one
/// BF16 compute mirror.  Base weights are immutable and admitted separately by
/// the inference session.
pub fn plan(config: model.Config, train: training.TrainingPlan, microbatch: u32, sequence: u32, limits: Limits) !Admission {
    try train.validate(config);
    if (microbatch == 0 or microbatch > limits.max_microbatch or sequence == 0 or sequence > train.max_sequence)
        return error.InvalidEmbeddingGemma2TrainingShape;
    const rank: usize = train.rank;
    const hidden: usize = config.hidden_size;
    const intermediate: usize = config.intermediate_size;
    var elements: usize = 0;
    for (0..config.num_hidden_layers) |layer| {
        const lc = try config.layer(layer);
        elements = try add(elements, try adapterElements(hidden, lc.queryWidth(), rank));
        elements = try add(elements, try adapterElements(hidden, lc.keyValueWidth(), rank));
        elements = try add(elements, try adapterElements(hidden, lc.keyValueWidth(), rank));
        elements = try add(elements, try adapterElements(lc.queryWidth(), hidden, rank));
        elements = try add(elements, try adapterElements(hidden, intermediate, rank));
        elements = try add(elements, try adapterElements(hidden, intermediate, rank));
        elements = try add(elements, try adapterElements(intermediate, hidden, rank));
    }
    elements = try add(elements, try adapterElements(hidden, config.embedding_dim, rank));
    const persistent = try mul(elements, 22);
    const rows = try mul(microbatch, sequence);
    const hidden_checkpoint = try mul(try mul(rows, hidden), @sizeOf(u16));
    const boundaries = try std.math.divCeil(usize, config.num_hidden_layers, checkpoint_layers);
    const checkpoints = try mul(try add(boundaries, 1), hidden_checkpoint);
    // Probabilities live in the segment tape. Keep the recomputed scores and
    // both FP32 cotangents alongside it during backward.
    const score_elements = try mul(try mul(try mul(microbatch, 4), sequence), sequence);
    const attention = try mul(try mul(score_elements, @sizeOf(f32)), 3);
    const segment = try maxSegmentBytes(config, rows, sequence);
    const executor = try executorWorkspaceBytes(rows, rank);
    // Objective buffers stay resident across all encoder microbatches. Charge
    // the admitted logical batch, not the encoder replay microbatch size.
    const candidates = try mul(limits.max_logical_batch, limits.max_candidates_per_query);
    const objective_rows = try add(limits.max_logical_batch, candidates);
    const objective_vectors = try mul(try mul(objective_rows, 768), 4 * @sizeOf(f32));
    const objective_pairs = try mul(limits.max_logical_batch, candidates);
    const objective_matrices = try mul(objective_pairs, 2 * @sizeOf(f32) + @sizeOf(u8));
    const objective_scalars = try mul(limits.max_logical_batch, 4 * @sizeOf(f32) + @sizeOf(u32));
    const objective = try add(objective_vectors, try add(objective_matrices, objective_scalars));
    const upper = try add(limits.resident_base_bytes, try add(persistent, try add(checkpoints, try add(attention, try add(segment, try add(executor, objective))))));
    if (upper > limits.max_device_bytes) return error.EmbeddingGemma2TrainingLimitExceeded;
    return .{
        .adapter_count = @as(usize, config.num_hidden_layers) * projection_count_per_layer + 1,
        .adapter_elements = elements,
        .persistent_adapter_bytes = persistent,
        .checkpoint_bytes = checkpoints,
        .attention_workspace_bytes = attention,
        .segment_workspace_bytes = segment,
        .executor_workspace_bytes = executor,
        .objective_workspace_bytes = objective,
        .device_upper_bound_bytes = upper,
        .resident_base_bytes = limits.resident_base_bytes,
    };
}

fn executorWorkspaceBytes(rows: usize, rank: usize) !usize {
    // ForwardScratch excluding score matrices already charged to attention:
    // BF16 low-rank/delta/five 2048-wide work arrays/final/projected, F32
    // Q/K/V/context packs, mask+ids, and a fixed 32 MiB Lt workspace.
    var per_row = try mul(rank, @sizeOf(u16));
    per_row = try add(per_row, try mul(5 * 2048, @sizeOf(u16)));
    per_row = try add(per_row, try mul(2048 + 512 + 512 + 2048, @sizeOf(f32)));
    per_row = try add(per_row, (512 + 768) * @sizeOf(u16) + 2 * @sizeOf(i64));
    const forward = try add(32 * 1024 * 1024, try mul(rows, per_row));
    // Ten F32 rows×2048 cotangent buffers are live during replay.
    const backward = try mul(try mul(rows, 10 * 2048), @sizeOf(f32));
    return try add(forward, backward);
}

fn maxSegmentBytes(config: model.Config, rows: usize, sequence: usize) !usize {
    var maximum: usize = 0;
    var start: usize = 0;
    const layer_count: usize = @intCast(config.num_hidden_layers);
    while (start < layer_count) : (start += checkpoint_layers) {
        var bytes: usize = 0;
        for (start..@min(start + checkpoint_layers, layer_count)) |layer| {
            const geometry = try config.layer(layer);
            for (training.layer_tape_order) |slot| {
                const elements = if (slot == .attention_probabilities)
                    try mul(try mul(rows, @as(usize, geometry.num_attention_heads)), sequence)
                else
                    try mul(rows, switch (slot) {
                        .layer_input, .attention_norm_input, .attention_residual, .ffn_norm_input, .ffn_residual, .ple_gate_post_activation, .layer_output_after_scalar => @as(usize, config.hidden_size),
                        .q_pre_norm, .q_post_rope, .attention_context => @as(usize, geometry.queryWidth()),
                        .k_pre_norm, .k_post_rope, .v_post_norm => @as(usize, geometry.keyValueWidth()),
                        .gate_pre_activation, .gate_post_activation, .up_projection => @as(usize, config.intermediate_size),
                        .attention_probabilities => unreachable,
                    });
                bytes = try add(bytes, try mul(elements, if (slot == .attention_probabilities) @sizeOf(f32) else @sizeOf(u16)));
                bytes = try alignForward(bytes, 256);
            }
        }
        maximum = @max(maximum, bytes);
    }
    return maximum;
}

fn alignForward(value: usize, boundary: usize) !usize {
    const padded = try add(value, boundary - 1);
    return padded & ~(boundary - 1);
}

pub const GradCacheKey = struct {
    base_model: [32]u8,
    adapter_identity: [32]u8,
    optimizer_step: u64,
    microbatch_step: u64,
    token_digest: [32]u8,
    mask_digest: [32]u8,
    objective_digest: [32]u8,
    sequence: u32,
    batch: u32,

    pub fn eql(a: GradCacheKey, b: GradCacheKey) bool {
        return std.meta.eql(a, b);
    }

    pub fn fingerprint(self: GradCacheKey) [32]u8 {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update(&self.base_model);
        hash.update(&self.adapter_identity);
        var scalar: [8]u8 = undefined;
        std.mem.writeInt(u64, &scalar, self.optimizer_step, .little);
        hash.update(&scalar);
        std.mem.writeInt(u64, &scalar, self.microbatch_step, .little);
        hash.update(&scalar);
        hash.update(&self.token_digest);
        hash.update(&self.mask_digest);
        hash.update(&self.objective_digest);
        var shape: [8]u8 = undefined;
        std.mem.writeInt(u32, shape[0..4], self.sequence, .little);
        std.mem.writeInt(u32, shape[4..8], self.batch, .little);
        hash.update(&shape);
        return hash.finalResult();
    }
};

test "EmbeddingGemma2 CUDA cache identity includes optimizer epoch and objective" {
    var a = GradCacheKey{ .base_model = @splat(1), .adapter_identity = @splat(2), .optimizer_step = 3, .microbatch_step = 4, .token_digest = @splat(5), .mask_digest = @splat(6), .objective_digest = @splat(7), .sequence = 128, .batch = 2 };
    var b = a;
    try std.testing.expect(a.eql(b));
    b.optimizer_step += 1;
    try std.testing.expect(!a.eql(b));
    const af = a.fingerprint();
    const bf = b.fingerprint();
    try std.testing.expect(!std.mem.eql(u8, &af, &bf));
    b = a;
    b.objective_digest[0] ^= 1;
    try std.testing.expect(!a.eql(b));
}
