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

//! Exact bounded resident attention forward/VJP for EG2 training (S <= 512).

const std = @import("std");
const cuda_compute = @import("../ops/cuda/cuda_compute.zig");
const buffer_mod = @import("../ops/cuda/buffer.zig");
const module_mod = @import("../ops/cuda/embedding_gemma2.zig");

const CudaCompute = cuda_compute.CudaCompute;
const DeviceBuffer = buffer_mod.DeviceBuffer;

pub const Shape = struct {
    batch: u32,
    sequence: u32,
    query_heads: u32 = 4,
    kv_heads: u32,
    head_dim: u32,
    local_radius: u32,

    pub fn validate(self: Shape) !void {
        if (self.batch == 0 or self.batch > 2 or self.sequence == 0 or self.sequence > 512 or self.query_heads != 4 or
            !((self.kv_heads == 1 and self.head_dim == 512 and self.local_radius == 0) or
                (self.kv_heads == 2 and self.head_dim == 256 and self.local_radius == 512))) return error.InvalidEmbeddingGemma2TrainingAttention;
    }
};

pub const ForwardBuffers = struct {
    q_packed_f32: DeviceBuffer,
    k_packed_f32: DeviceBuffer,
    v_packed_f32: DeviceBuffer,
    scores_f32: DeviceBuffer,
    probabilities_f32: DeviceBuffer,
    context_packed_f32: DeviceBuffer,
    context_bf16: DeviceBuffer,
    workspace: DeviceBuffer,
};

pub fn forward(compute: *CudaCompute, q_bf16: DeviceBuffer, k_bf16: DeviceBuffer, v_bf16: DeviceBuffer, mask_i64: DeviceBuffer, shape: Shape, buffers: ForwardBuffers) !void {
    try shape.validate();
    try validateForward(q_bf16, k_bf16, v_bf16, mask_i64, shape, buffers);
    return forwardPacked(compute, q_bf16, k_bf16, v_bf16, mask_i64, shape, buffers);
}

/// Both official attention topologies match eager's flattened B*H GEMMs.
/// Per-head contractions can choose different BF16 reductions by shape.
/// Reuse existing arenas: Q occupies the output until QK finishes, K and V
/// share the F32-sized context arena, and PV overwrites the dead packed K.
fn forwardPacked(compute: *CudaCompute, q: DeviceBuffer, k: DeviceBuffer, v: DeviceBuffer, mask: DeviceBuffer, shape: Shape, buffers: ForwardBuffers) !void {
    const module = try compute.embeddingGemma2Module();
    const ctx = compute.embeddingGemma2TrainingContext();
    const lt = try compute.embeddingGemma2TrainingCublasLt();
    const elements = try tensorElements(shape.batch, shape.sequence, shape.query_heads, shape.head_dim);
    const bytes = try mul(elements, @sizeOf(u16));
    const head_elements = try mul(shape.sequence, shape.head_dim);
    const score_stride = try mul(shape.sequence, shape.sequence);
    const scores = try scoreElements(shape);
    const batches = try mul(shape.batch, shape.query_heads);
    const packed_q = try region(buffers.context_bf16, 0, bytes);
    const packed_k = try region(buffers.context_packed_f32, 0, bytes);
    const packed_v = try region(buffers.context_packed_f32, bytes, bytes);
    const packed_scores = try region(buffers.q_packed_f32, 0, try mul(scores, @sizeOf(u16)));
    try module.launchTrainingPackBhsdBf16(ctx, packed_q, q, shape.batch, shape.sequence, shape.query_heads, shape.query_heads, shape.head_dim, false);
    try module.launchTrainingPackBhsdBf16(ctx, packed_k, k, shape.batch, shape.sequence, shape.kv_heads, shape.query_heads, shape.head_dim, false);
    try module.launchTrainingPackBhsdBf16(ctx, packed_v, v, shape.batch, shape.sequence, shape.kv_heads, shape.query_heads, shape.head_dim, false);
    try lt.matmulEmbeddingGemma2Attention(ctx, packed_scores, packed_q, packed_k, buffers.workspace, shape.sequence, shape.sequence, shape.head_dim, shape.head_dim, shape.head_dim, shape.sequence, batches, head_elements, head_elements, score_stride, false, true, false, 1.0);
    try module.launchBf16ToF32(ctx, buffers.scores_f32, packed_scores, try count32(scores));
    try module.launchTrainingSoftmax(ctx, buffers.probabilities_f32, buffers.scores_f32, mask, attentionShape(shape));
    try module.launchF32ToBf16(ctx, packed_scores, buffers.probabilities_f32, try count32(scores));
    try lt.matmulEmbeddingGemma2Attention(ctx, packed_k, packed_scores, packed_v, buffers.workspace, shape.sequence, shape.head_dim, shape.sequence, shape.sequence, shape.head_dim, shape.head_dim, batches, score_stride, head_elements, head_elements, false, false, false, 1.0);
    try module.launchTrainingUnpackBhsdBf16(ctx, buffers.context_bf16, packed_k, shape.batch, shape.sequence, shape.query_heads, shape.query_heads, shape.head_dim, false);
    module.noteAttentionLaunch();
}

pub const BackwardBuffers = struct {
    q_packed_f32: DeviceBuffer,
    k_packed_f32: DeviceBuffer,
    v_packed_f32: DeviceBuffer,
    dcontext_packed_f32: DeviceBuffer,
    dprobabilities_f32: DeviceBuffer,
    dscores_f32: DeviceBuffer,
    dq_packed_f32: DeviceBuffer,
    dk_packed_f32: DeviceBuffer,
    dv_packed_f32: DeviceBuffer,
    dq_f32: DeviceBuffer,
    dk_f32: DeviceBuffer,
    dv_f32: DeviceBuffer,
    workspace: DeviceBuffer,
};

pub fn backward(compute: *CudaCompute, q_bf16: DeviceBuffer, k_bf16: DeviceBuffer, v_bf16: DeviceBuffer, probabilities_f32: DeviceBuffer, dcontext_f32: DeviceBuffer, shape: Shape, buffers: BackwardBuffers) !void {
    try shape.validate();
    try validateBackward(q_bf16, k_bf16, v_bf16, probabilities_f32, dcontext_f32, shape, buffers);
    return backwardPacked(compute, q_bf16, k_bf16, v_bf16, probabilities_f32, dcontext_f32, shape, buffers);
}

fn backwardPacked(compute: *CudaCompute, q: DeviceBuffer, k: DeviceBuffer, v: DeviceBuffer, probabilities: DeviceBuffer, dcontext: DeviceBuffer, shape: Shape, buffers: BackwardBuffers) !void {
    const module = try compute.embeddingGemma2Module();
    const ctx = compute.embeddingGemma2TrainingContext();
    const lt = try compute.embeddingGemma2TrainingCublasLt();
    const elements = try tensorElements(shape.batch, shape.sequence, shape.query_heads, shape.head_dim);
    const bytes = try mul(elements, @sizeOf(u16));
    const head_elements = try mul(shape.sequence, shape.head_dim);
    const score_stride = try mul(shape.sequence, shape.sequence);
    const scores = try scoreElements(shape);
    const batches = try mul(shape.batch, shape.query_heads);
    const packed_dc = try region(buffers.dcontext_packed_f32, 0, bytes);
    const packed_v = try region(buffers.dcontext_packed_f32, bytes, bytes);
    const packed_scores = try region(buffers.q_packed_f32, 0, try mul(scores, @sizeOf(u16)));
    const packed_q = try region(buffers.dq_packed_f32, 0, bytes);
    // KV-sized arenas cannot hold the four repeated-head contributions. Use
    // the not-yet-produced dQ output as sequential dV/dK BF16 scratch instead.
    const packed_grad = try region(buffers.dq_f32, 0, bytes);
    try module.launchTrainingPackBhsdBf16(ctx, packed_dc, dcontext, shape.batch, shape.sequence, shape.query_heads, shape.query_heads, shape.head_dim, true);
    try module.launchTrainingPackBhsdBf16(ctx, packed_v, v, shape.batch, shape.sequence, shape.kv_heads, shape.query_heads, shape.head_dim, false);
    try module.launchF32ToBf16(ctx, packed_scores, probabilities, try count32(scores));
    // P and dC are BF16; repeat_kv's VJP sums the BF16 per-head outputs
    // in FP32 and casts only once when returning to the original KV tensor.
    try lt.matmulEmbeddingGemma2Attention(ctx, packed_grad, packed_scores, packed_dc, buffers.workspace, shape.sequence, shape.head_dim, shape.sequence, shape.sequence, shape.head_dim, shape.head_dim, batches, score_stride, head_elements, head_elements, true, false, false, 1.0);
    try module.launchTrainingUnpackBhsdBf16(ctx, buffers.dv_f32, packed_grad, shape.batch, shape.sequence, shape.query_heads, shape.kv_heads, shape.head_dim, true);
    // P is dead after dV, so use its arena for dP and subsequently dS.
    try lt.matmulEmbeddingGemma2Attention(ctx, packed_scores, packed_dc, packed_v, buffers.workspace, shape.sequence, shape.sequence, shape.head_dim, shape.head_dim, shape.head_dim, shape.sequence, batches, head_elements, head_elements, score_stride, false, true, false, 1.0);
    try module.launchBf16ToF32(ctx, buffers.dprobabilities_f32, packed_scores, try count32(scores));
    try module.launchTrainingSoftmaxVjp(ctx, buffers.dscores_f32, buffers.dprobabilities_f32, probabilities, attentionShape(shape));
    try module.launchF32ToBf16(ctx, packed_scores, buffers.dscores_f32, try count32(scores));
    // dC is dead. Reuse it for repeated K. Keep packed Q until dK finishes,
    // then overwrite that dead Q with packed dQ and unpack to its final output.
    try module.launchTrainingPackBhsdBf16(ctx, packed_dc, k, shape.batch, shape.sequence, shape.kv_heads, shape.query_heads, shape.head_dim, false);
    try module.launchTrainingPackBhsdBf16(ctx, packed_q, q, shape.batch, shape.sequence, shape.query_heads, shape.query_heads, shape.head_dim, false);
    try lt.matmulEmbeddingGemma2Attention(ctx, packed_grad, packed_scores, packed_q, buffers.workspace, shape.sequence, shape.head_dim, shape.sequence, shape.sequence, shape.head_dim, shape.head_dim, batches, score_stride, head_elements, head_elements, true, false, false, 1.0);
    try module.launchTrainingUnpackBhsdBf16(ctx, buffers.dk_f32, packed_grad, shape.batch, shape.sequence, shape.query_heads, shape.kv_heads, shape.head_dim, true);
    try lt.matmulEmbeddingGemma2Attention(ctx, packed_q, packed_scores, packed_dc, buffers.workspace, shape.sequence, shape.head_dim, shape.sequence, shape.sequence, shape.head_dim, shape.head_dim, batches, score_stride, head_elements, head_elements, false, false, false, 1.0);
    try module.launchTrainingUnpackBhsdBf16(ctx, buffers.dq_f32, packed_q, shape.batch, shape.sequence, shape.query_heads, shape.query_heads, shape.head_dim, true);
}

fn region(buffer: DeviceBuffer, offset: usize, bytes: usize) !DeviceBuffer {
    const end = std.math.add(usize, offset, bytes) catch return error.InvalidEmbeddingGemma2TrainingAttention;
    try require(buffer, end);
    const ptr = std.math.add(@TypeOf(buffer.ptr), buffer.ptr, @intCast(offset)) catch return error.InvalidEmbeddingGemma2TrainingAttention;
    return .{ .ptr = ptr, .len = bytes };
}

fn attentionShape(shape: Shape) module_mod.Attention {
    return .{ .batch = shape.batch, .seq = shape.sequence, .q_heads = shape.query_heads, .kv_heads = shape.kv_heads, .head_dim = shape.head_dim, .local_radius = shape.local_radius };
}
fn tensorElements(batch: anytype, sequence: anytype, heads: anytype, dim: anytype) !usize {
    return mul(mul(mul(batch, sequence) catch return error.InvalidEmbeddingGemma2TrainingAttention, heads) catch return error.InvalidEmbeddingGemma2TrainingAttention, dim);
}
fn scoreElements(shape: Shape) !usize {
    return mul(try mul(shape.batch, shape.query_heads), try mul(shape.sequence, shape.sequence));
}
fn count32(count: usize) !u32 {
    return std.math.cast(u32, count) orelse error.InvalidEmbeddingGemma2TrainingAttention;
}
fn mul(a: anytype, b: anytype) !usize {
    return std.math.mul(usize, @intCast(a), @intCast(b)) catch error.InvalidEmbeddingGemma2TrainingAttention;
}
fn require(buffer: DeviceBuffer, size: usize) !void {
    if (buffer.ptr == 0 or buffer.len < size) return error.InvalidEmbeddingGemma2TrainingAttention;
}
fn validateForward(q: DeviceBuffer, k: DeviceBuffer, v: DeviceBuffer, mask: DeviceBuffer, shape: Shape, buffers: ForwardBuffers) !void {
    const q_elements = try tensorElements(shape.batch, shape.sequence, shape.query_heads, shape.head_dim);
    const kv_elements = try tensorElements(shape.batch, shape.sequence, shape.kv_heads, shape.head_dim);
    const scores = try scoreElements(shape);
    try require(q, try mul(q_elements, @sizeOf(u16)));
    try require(k, try mul(kv_elements, @sizeOf(u16)));
    try require(v, try mul(kv_elements, @sizeOf(u16)));
    try require(mask, try mul(try mul(shape.batch, shape.sequence), @sizeOf(i64)));
    for ([_]DeviceBuffer{ buffers.q_packed_f32, buffers.context_packed_f32 }) |buffer| try require(buffer, try mul(q_elements, @sizeOf(f32)));
    for ([_]DeviceBuffer{ buffers.k_packed_f32, buffers.v_packed_f32 }) |buffer| try require(buffer, try mul(kv_elements, @sizeOf(f32)));
    try require(buffers.scores_f32, try mul(scores, @sizeOf(f32)));
    try require(buffers.probabilities_f32, try mul(scores, @sizeOf(f32)));
    try require(buffers.context_bf16, try mul(q_elements, @sizeOf(u16)));
}
fn validateBackward(q: DeviceBuffer, k: DeviceBuffer, v: DeviceBuffer, probabilities: DeviceBuffer, dcontext: DeviceBuffer, shape: Shape, buffers: BackwardBuffers) !void {
    const q_elements = try tensorElements(shape.batch, shape.sequence, shape.query_heads, shape.head_dim);
    const kv_elements = try tensorElements(shape.batch, shape.sequence, shape.kv_heads, shape.head_dim);
    const scores = try scoreElements(shape);
    try require(q, try mul(q_elements, @sizeOf(u16)));
    try require(k, try mul(kv_elements, @sizeOf(u16)));
    try require(v, try mul(kv_elements, @sizeOf(u16)));
    try require(probabilities, try mul(scores, @sizeOf(f32)));
    try require(dcontext, try mul(q_elements, @sizeOf(f32)));
    for ([_]DeviceBuffer{ buffers.q_packed_f32, buffers.dcontext_packed_f32, buffers.dq_packed_f32, buffers.dq_f32 }) |buffer| try require(buffer, try mul(q_elements, @sizeOf(f32)));
    for ([_]DeviceBuffer{ buffers.k_packed_f32, buffers.v_packed_f32, buffers.dk_packed_f32, buffers.dv_packed_f32, buffers.dk_f32, buffers.dv_f32 }) |buffer| try require(buffer, try mul(kv_elements, @sizeOf(f32)));
    for ([_]DeviceBuffer{ buffers.dprobabilities_f32, buffers.dscores_f32 }) |buffer| try require(buffer, try mul(scores, @sizeOf(f32)));
}

test "EmbeddingGemma2 training attention shape gates exact official topologies" {
    try (Shape{ .batch = 2, .sequence = 512, .kv_heads = 2, .head_dim = 256, .local_radius = 512 }).validate();
    try (Shape{ .batch = 1, .sequence = 17, .kv_heads = 1, .head_dim = 512, .local_radius = 0 }).validate();
    try std.testing.expectError(error.InvalidEmbeddingGemma2TrainingAttention, (Shape{ .batch = 1, .sequence = 513, .kv_heads = 2, .head_dim = 256, .local_radius = 512 }).validate());
    try std.testing.expectError(error.InvalidEmbeddingGemma2TrainingAttention, (Shape{ .batch = 1, .sequence = 17, .kv_heads = 2, .head_dim = 512, .local_radius = 0 }).validate());
}
