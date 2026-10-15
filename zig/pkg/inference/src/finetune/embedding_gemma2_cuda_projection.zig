// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Frozen-base plus LoRA projection primitives for the resident EG2 trainer.

const std = @import("std");
const cuda_compute = @import("../ops/cuda/cuda_compute.zig");
const buffer_mod = @import("../ops/cuda/buffer.zig");
const session_mod = @import("embedding_gemma2_cuda_session.zig");

const CudaCompute = cuda_compute.CudaCompute;
const DeviceBuffer = buffer_mod.DeviceBuffer;

pub const ForwardBuffers = struct {
    output_bf16: DeviceBuffer,
    low_rank_bf16: DeviceBuffer,
    delta_bf16: DeviceBuffer,
    workspace: DeviceBuffer,
};

pub fn baseForward(compute: *CudaCompute, weight_bf16: DeviceBuffer, input_bf16: DeviceBuffer, output_bf16: DeviceBuffer, workspace: DeviceBuffer, rows: usize, input_dim: usize, output_dim: usize) !void {
    try require(weight_bf16, try bytes(output_dim, input_dim, @sizeOf(u16)));
    try require(input_bf16, try bytes(rows, input_dim, @sizeOf(u16)));
    try require(output_bf16, try bytes(rows, output_dim, @sizeOf(u16)));
    try (try compute.embeddingGemma2TrainingCublasLt()).matmulBf16WeightBf16Out(compute.embeddingGemma2TrainingContext(), output_bf16, input_bf16, weight_bf16, workspace, rows, input_dim, output_dim);
    (try compute.embeddingGemma2Module()).noteCublasLtMatmul();
}

pub fn baseInputVjp(compute: *CudaCompute, weight_bf16: DeviceBuffer, dy_f32: DeviceBuffer, dx_f32: DeviceBuffer, dy_bf16: DeviceBuffer, workspace: DeviceBuffer, rows: usize, input_dim: usize, output_dim: usize) !void {
    const output_elements = try elementCount(rows, output_dim);
    try require(weight_bf16, try bytes(output_dim, input_dim, @sizeOf(u16)));
    try require(dy_f32, try std.math.mul(usize, output_elements, @sizeOf(f32)));
    try require(dx_f32, try bytes(rows, input_dim, @sizeOf(f32)));
    try require(dy_bf16, try std.math.mul(usize, output_elements, @sizeOf(u16)));
    const dx_bf16_bytes = try bytes(rows, input_dim, @sizeOf(u16));
    const lt_offset = try align256(dx_bf16_bytes);
    try require(workspace, lt_offset);
    const dx_bf16 = DeviceBuffer{ .ptr = workspace.ptr, .len = dx_bf16_bytes };
    const lt_workspace = DeviceBuffer{ .ptr = workspace.ptr + lt_offset, .len = workspace.len - lt_offset };
    const output_count = std.math.cast(u32, output_elements) orelse return error.InvalidEmbeddingGemma2Projection;
    const module = try compute.embeddingGemma2Module();
    const ctx = compute.embeddingGemma2TrainingContext();
    try module.launchF32ToBf16(ctx, dy_bf16, dy_f32, output_count);
    try (try compute.embeddingGemma2TrainingCublasLt()).matmulEmbeddingGemma2Attention(ctx, dx_bf16, dy_bf16, weight_bf16, lt_workspace, rows, input_dim, output_dim, output_dim, input_dim, input_dim, 1, 0, 0, 0, false, false, false, 1);
    try module.launchBf16ToF32(ctx, dx_f32, dx_bf16, std.math.cast(u32, try elementCount(rows, input_dim)) orelse return error.InvalidEmbeddingGemma2Projection);
    module.noteCublasLtMatmul();
}

pub fn forward(
    compute: *CudaCompute,
    state: *const session_mod.ResidentAdapterState,
    adapter_index: usize,
    base_weight_name: []const u8,
    input_bf16: DeviceBuffer,
    rows: usize,
    input_dim: usize,
    output_dim: usize,
    alpha: f32,
    buffers: ForwardBuffers,
) !void {
    const descriptor = try descriptorFor(state, adapter_index, input_dim, output_dim, alpha);
    const rank: usize = descriptor.rank;
    try require(input_bf16, try bytes(rows, input_dim, @sizeOf(u16)));
    try require(buffers.output_bf16, try bytes(rows, output_dim, @sizeOf(u16)));
    try require(buffers.low_rank_bf16, try bytes(rows, rank, @sizeOf(u16)));
    try require(buffers.delta_bf16, try bytes(rows, output_dim, @sizeOf(u16)));
    const base_weight = try compute.embeddingGemma2Bf16Weight(base_weight_name);
    try require(base_weight, try bytes(output_dim, input_dim, @sizeOf(u16)));
    const adapter = try state.adapterSlice(adapter_index);
    const lt = try compute.embeddingGemma2TrainingCublasLt();
    const ctx = compute.embeddingGemma2TrainingContext();
    try lt.matmulBf16WeightBf16Out(ctx, buffers.output_bf16, input_bf16, base_weight, buffers.workspace, rows, input_dim, output_dim);
    try lt.matmulBf16WeightBf16Out(ctx, buffers.low_rank_bf16, input_bf16, adapter.a_bf16, buffers.workspace, rows, input_dim, rank);
    try lt.matmulBf16WeightBf16Out(ctx, buffers.delta_bf16, buffers.low_rank_bf16, adapter.b_bf16, buffers.workspace, rows, rank, output_dim);
    const module = try compute.embeddingGemma2Module();
    const output_count = std.math.cast(u32, try elementCount(rows, output_dim)) orelse return error.InvalidEmbeddingGemma2Projection;
    const adapter_scale = alpha / @as(f32, @floatFromInt(rank));
    if (isNormalPowerOfTwo(adapter_scale)) {
        // Scaling by an exact power of two cannot introduce an intermediate
        // BF16 rounding difference, so retain the fused default-rank path.
        try module.launchResidual(ctx, buffers.output_bf16, buffers.output_bf16, buffers.delta_bf16, null, output_count, adapter_scale, 0);
    } else {
        // PEFT evaluates the BF16 multiply and BF16 residual add as distinct
        // operations. Preserve that cast boundary for fractional scales.
        try module.launchScale(ctx, buffers.delta_bf16, buffers.delta_bf16, output_count, adapter_scale);
        try module.launchResidual(ctx, buffers.output_bf16, buffers.output_bf16, buffers.delta_bf16, null, output_count, 1, 0);
    }
    module.noteCublasLtMatmul();
    module.noteCublasLtMatmul();
    module.noteCublasLtMatmul();
}

pub const BackwardBuffers = struct {
    dx_f32: DeviceBuffer,
    dy_scaled_f32: DeviceBuffer,
    dy_bf16: DeviceBuffer,
    input_f32: DeviceBuffer,
    low_rank_f32: DeviceBuffer,
    dz_f32: DeviceBuffer,
    workspace: DeviceBuffer,
};

pub fn backward(
    compute: *CudaCompute,
    state: *const session_mod.ResidentAdapterState,
    adapter_index: usize,
    base_weight_name: []const u8,
    input_bf16: DeviceBuffer,
    low_rank_bf16: DeviceBuffer,
    dy_f32: DeviceBuffer,
    rows: usize,
    input_dim: usize,
    output_dim: usize,
    alpha: f32,
    buffers: BackwardBuffers,
) !void {
    const descriptor = try descriptorFor(state, adapter_index, input_dim, output_dim, alpha);
    const rank: usize = descriptor.rank;
    const input_elements = try elementCount(rows, input_dim);
    const output_elements = try elementCount(rows, output_dim);
    const low_rank_elements = try elementCount(rows, rank);
    try require(input_bf16, try std.math.mul(usize, input_elements, @sizeOf(u16)));
    try require(low_rank_bf16, try std.math.mul(usize, low_rank_elements, @sizeOf(u16)));
    try require(dy_f32, try std.math.mul(usize, output_elements, @sizeOf(f32)));
    try require(buffers.dx_f32, try std.math.mul(usize, input_elements, @sizeOf(f32)));
    try require(buffers.dy_scaled_f32, try std.math.mul(usize, output_elements, @sizeOf(f32)));
    try require(buffers.dy_bf16, try std.math.mul(usize, output_elements, @sizeOf(u16)));
    try require(buffers.input_f32, try std.math.mul(usize, input_elements, @sizeOf(f32)));
    // This arena first holds widened low-rank values and later serves as the
    // BF16 cast scratch for the full input-gradient branch.
    try require(buffers.low_rank_f32, @max(try std.math.mul(usize, low_rank_elements, @sizeOf(f32)), try std.math.mul(usize, input_elements, @sizeOf(f32))));
    try require(buffers.dz_f32, try std.math.mul(usize, low_rank_elements, @sizeOf(f32)));
    const adapter_elements = @max(try elementCount(output_dim, rank), try elementCount(rank, input_dim));
    const weight_grad_scratch = try weightGradScratch(buffers.workspace, adapter_elements);
    const adapter = try state.adapterSlice(adapter_index);
    const gradients = try state.gradientSlice(adapter_index, false);
    const base_weight = try compute.embeddingGemma2Bf16Weight(base_weight_name);
    try require(base_weight, try bytes(output_dim, input_dim, @sizeOf(u16)));
    const module = try compute.embeddingGemma2Module();
    const ctx = compute.embeddingGemma2TrainingContext();
    const output_count = std.math.cast(u32, output_elements) orelse return error.InvalidEmbeddingGemma2Projection;
    try module.launchF32ToBf16(ctx, buffers.dy_bf16, dy_f32, output_count);
    const base_dx_bf16 = DeviceBuffer{ .ptr = buffers.input_f32.ptr, .len = input_elements * @sizeOf(u16) };
    const lt = try compute.embeddingGemma2TrainingCublasLt();
    try lt.matmulEmbeddingGemma2Attention(ctx, base_dx_bf16, buffers.dy_bf16, base_weight, buffers.workspace, rows, input_dim, output_dim, output_dim, input_dim, input_dim, 1, 0, 0, 0, false, false, false, 1);
    try module.launchBf16ToF32(ctx, buffers.dx_f32, base_dx_bf16, std.math.cast(u32, input_elements) orelse return error.InvalidEmbeddingGemma2Projection);
    // Autocast linear outputs receive a BF16-typed cotangent. Reuse the same
    // BF16 rounding applied to the frozen-base dX before widening/scaling for
    // LoRA dB and dz; consuming the original F32 dy diverges from Torch.
    try module.launchBf16ToF32(ctx, buffers.dy_scaled_f32, buffers.dy_bf16, output_count);
    try compute.scaleEmbeddingGemma2TrainingF32(buffers.dy_scaled_f32, buffers.dy_scaled_f32, output_elements, alpha / @as(f32, @floatFromInt(rank)));
    // The LoRA branch receives the scaled cotangent through a BF16 linear
    // output, so round again after applying alpha/rank.
    try module.launchF32ToBf16(ctx, buffers.dy_bf16, buffers.dy_scaled_f32, output_count);
    try module.launchBf16ToF32(ctx, buffers.dy_scaled_f32, buffers.dy_bf16, output_count);
    const db_elements = try elementCount(output_dim, rank);
    try lt.matmulEmbeddingGemma2Attention(ctx, weight_grad_scratch.bf16, buffers.dy_bf16, low_rank_bf16, weight_grad_scratch.lt_workspace, output_dim, rank, rows, output_dim, rank, rank, 1, 0, 0, 0, true, false, false, 1.0);
    try module.launchBf16ToF32(ctx, weight_grad_scratch.f32, weight_grad_scratch.bf16, std.math.cast(u32, db_elements) orelse return error.InvalidEmbeddingGemma2Projection);
    try module.launchAccumulateF32(ctx, gradients.b, weight_grad_scratch.f32, db_elements);
    module.noteCublasLtMatmul();

    // Input VJPs consume the BF16 adapter mirrors used by the forward rather
    // than the F32 masters. Each linear VJP output crosses a BF16 cast node.
    const dz_bf16 = DeviceBuffer{ .ptr = buffers.low_rank_f32.ptr, .len = low_rank_elements * @sizeOf(u16) };
    try lt.matmulEmbeddingGemma2Attention(ctx, dz_bf16, buffers.dy_bf16, adapter.b_bf16, buffers.workspace, rows, rank, output_dim, output_dim, rank, rank, 1, 0, 0, 0, false, false, false, 1);
    try module.launchBf16ToF32(ctx, buffers.dz_f32, dz_bf16, std.math.cast(u32, low_rank_elements) orelse return error.InvalidEmbeddingGemma2Projection);

    const da_elements = try elementCount(rank, input_dim);
    try lt.matmulEmbeddingGemma2Attention(ctx, weight_grad_scratch.bf16, dz_bf16, input_bf16, weight_grad_scratch.lt_workspace, rank, input_dim, rows, rank, input_dim, input_dim, 1, 0, 0, 0, true, false, false, 1.0);
    try module.launchBf16ToF32(ctx, weight_grad_scratch.f32, weight_grad_scratch.bf16, std.math.cast(u32, da_elements) orelse return error.InvalidEmbeddingGemma2Projection);
    try module.launchAccumulateF32(ctx, gradients.a, weight_grad_scratch.f32, da_elements);
    module.noteCublasLtMatmul();

    const lora_dx_bf16 = DeviceBuffer{ .ptr = buffers.input_f32.ptr, .len = input_elements * @sizeOf(u16) };
    try lt.matmulEmbeddingGemma2Attention(ctx, lora_dx_bf16, dz_bf16, adapter.a_bf16, buffers.workspace, rows, input_dim, rank, rank, input_dim, input_dim, 1, 0, 0, 0, false, false, false, 1);
    try module.launchBf16ToF32(ctx, buffers.low_rank_f32, lora_dx_bf16, std.math.cast(u32, input_elements) orelse return error.InvalidEmbeddingGemma2Projection);
    try module.launchAccumulateBf16BoundaryF32(ctx, buffers.dx_f32, buffers.low_rank_f32, input_elements);
}

const WeightGradScratch = struct {
    bf16: DeviceBuffer,
    f32: DeviceBuffer,
    lt_workspace: DeviceBuffer,
};

fn weightGradScratch(workspace: DeviceBuffer, max_elements: usize) !WeightGradScratch {
    const bf16_bytes = std.math.mul(usize, max_elements, @sizeOf(u16)) catch return error.InvalidEmbeddingGemma2Projection;
    const f32_offset = try align256(bf16_bytes);
    const f32_bytes = std.math.mul(usize, max_elements, @sizeOf(f32)) catch return error.InvalidEmbeddingGemma2Projection;
    const lt_offset_unaligned = std.math.add(usize, f32_offset, f32_bytes) catch return error.InvalidEmbeddingGemma2Projection;
    const lt_offset = try align256(lt_offset_unaligned);
    if (workspace.ptr == 0 or workspace.len < lt_offset) return error.InvalidEmbeddingGemma2Projection;
    return .{
        .bf16 = .{ .ptr = workspace.ptr, .len = bf16_bytes },
        .f32 = .{ .ptr = workspace.ptr + f32_offset, .len = f32_bytes },
        .lt_workspace = .{ .ptr = workspace.ptr + lt_offset, .len = workspace.len - lt_offset },
    };
}

fn align256(value: usize) !usize {
    const rounded = std.math.add(usize, value, 255) catch return error.InvalidEmbeddingGemma2Projection;
    return rounded & ~@as(usize, 255);
}

fn isNormalPowerOfTwo(value: f32) bool {
    if (!std.math.isFinite(value) or value <= 0) return false;
    const bits: u32 = @bitCast(value);
    const exponent = (bits >> 23) & 0xff;
    return exponent != 0 and exponent != 0xff and (bits & 0x7fffff) == 0;
}

fn roundF32(module: anytype, ctx: anytype, values: DeviceBuffer, scratch_bf16: DeviceBuffer, count: usize) !void {
    try require(values, try std.math.mul(usize, count, @sizeOf(f32)));
    try require(scratch_bf16, try std.math.mul(usize, count, @sizeOf(u16)));
    const n = std.math.cast(u32, count) orelse return error.InvalidEmbeddingGemma2Projection;
    try module.launchF32ToBf16(ctx, scratch_bf16, values, n);
    try module.launchBf16ToF32(ctx, values, scratch_bf16, n);
}

fn descriptorFor(state: *const session_mod.ResidentAdapterState, index: usize, input_dim: usize, output_dim: usize, alpha: f32) !@import("embedding_gemma2_adapter_state.zig").AdapterDescriptor {
    if (index >= state.layout.descriptors.len or !std.math.isFinite(alpha) or alpha <= 0) return error.InvalidEmbeddingGemma2Projection;
    const descriptor = state.layout.descriptors[index];
    if (descriptor.input_dim != input_dim or descriptor.output_dim != output_dim or descriptor.rank == 0)
        return error.InvalidEmbeddingGemma2Projection;
    return descriptor;
}

fn elementCount(rows: usize, width: usize) !usize {
    return std.math.mul(usize, rows, width) catch error.InvalidEmbeddingGemma2Projection;
}
fn bytes(rows: usize, width: usize, element_bytes: usize) !usize {
    return std.math.mul(usize, try elementCount(rows, width), element_bytes) catch error.InvalidEmbeddingGemma2Projection;
}
fn require(buffer: DeviceBuffer, minimum: usize) !void {
    if (buffer.ptr == 0 or buffer.len < minimum) return error.InvalidEmbeddingGemma2Projection;
}

test "EmbeddingGemma2 projection element bounds reject overflow" {
    try std.testing.expectEqual(@as(usize, 1024), try elementCount(2, 512));
    try std.testing.expectError(error.InvalidEmbeddingGemma2Projection, elementCount(std.math.maxInt(usize), 2));
}

test "EmbeddingGemma2 projection recognizes exact normal power-of-two scales" {
    try std.testing.expect(isNormalPowerOfTwo(2));
    try std.testing.expect(isNormalPowerOfTwo(0.5));
    try std.testing.expect(!isNormalPowerOfTwo(1.3));
    try std.testing.expect(!isNormalPowerOfTwo(0));
    try std.testing.expect(!isNormalPowerOfTwo(std.math.inf(f32)));
}
