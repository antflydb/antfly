// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Resident CUDA activation tape for two-layer EG2 recomputation segments.

const std = @import("std");
const cuda_compute = @import("../ops/cuda/cuda_compute.zig");
const buffer_mod = @import("../ops/cuda/buffer.zig");
const training = @import("embedding_gemma2_training.zig");

const CudaCompute = cuda_compute.CudaCompute;
const DeviceBuffer = buffer_mod.DeviceBuffer;

pub const segment_layers: u32 = 2;
pub const checkpoint_count: u32 = 24 / segment_layers + 1;
pub const alignment: usize = 256;

pub const DType = enum {
    bf16,
    f32,

    pub fn bytes(self: DType) usize {
        return switch (self) {
            .bf16 => @sizeOf(u16),
            .f32 => @sizeOf(f32),
        };
    }
};

pub const SlotLayout = struct {
    slot: training.TapeSlot,
    dtype: DType,
    elements: usize,
    byte_offset: usize,
    byte_len: usize,
};

pub const slots_per_layer = training.layer_tape_order.len;

pub const SegmentLayout = struct {
    start_layer: u32,
    layer_count: u32,
    layers: [segment_layers][slots_per_layer]SlotLayout,
    total_bytes: usize,

    pub fn init(batch: u32, sequence: u32, start_layer: u32) !SegmentLayout {
        if (batch == 0 or batch > 2 or sequence == 0 or sequence > training.training_sequence_cap or
            start_layer >= 24 or start_layer % segment_layers != 0)
            return error.InvalidEmbeddingGemma2TapeShape;
        var result = SegmentLayout{
            .start_layer = start_layer,
            .layer_count = @min(segment_layers, 24 - start_layer),
            .layers = undefined,
            .total_bytes = 0,
        };
        var offset: usize = 0;
        for (0..segment_layers) |relative| {
            if (relative >= result.layer_count) {
                result.layers[relative] = @splat(.{ .slot = .layer_input, .dtype = .bf16, .elements = 0, .byte_offset = 0, .byte_len = 0 });
                continue;
            }
            const geometry = try training.officialLayerGeometry(start_layer + @as(u32, @intCast(relative)));
            for (training.layer_tape_order, 0..) |tape_slot, slot_index| {
                // Autocast persists BF16 activations except for softmax's F32
                // result.  The explicit BF16 cast feeds PV, while autograd
                // retains the pre-cast F32 probabilities for softmax VJP.
                const dtype: DType = if (tape_slot == .attention_probabilities) .f32 else .bf16;
                const elements = try slotElements(tape_slot, batch, sequence, geometry);
                offset = try alignForward(offset, alignment);
                const byte_len = std.math.mul(usize, elements, dtype.bytes()) catch return error.EmbeddingGemma2TapeOverflow;
                result.layers[relative][slot_index] = .{
                    .slot = tape_slot,
                    .dtype = dtype,
                    .elements = elements,
                    .byte_offset = offset,
                    .byte_len = byte_len,
                };
                offset = std.math.add(usize, offset, byte_len) catch return error.EmbeddingGemma2TapeOverflow;
            }
        }
        result.total_bytes = try alignForward(offset, alignment);
        return result;
    }

    pub fn slot(self: *const SegmentLayout, relative_layer: u32, tape_slot: training.TapeSlot) !SlotLayout {
        if (relative_layer >= self.layer_count) return error.InvalidEmbeddingGemma2TapeLayer;
        for (self.layers[relative_layer]) |candidate| if (candidate.slot == tape_slot) return candidate;
        return error.InvalidEmbeddingGemma2TapeSlot;
    }
};

pub const TypedView = struct {
    buffer: DeviceBuffer,
    dtype: DType,
    elements: usize,
};

pub const SegmentView = struct {
    layout: SegmentLayout,
    arena: DeviceBuffer,

    pub fn slot(self: *const SegmentView, relative_layer: u32, tape_slot: training.TapeSlot) !TypedView {
        const planned = try self.layout.slot(relative_layer, tape_slot);
        if (planned.byte_offset > self.arena.len or planned.byte_len > self.arena.len - planned.byte_offset)
            return error.InvalidEmbeddingGemma2TapeView;
        return .{
            .buffer = .{ .ptr = self.arena.ptr + planned.byte_offset, .len = planned.byte_len },
            .dtype = planned.dtype,
            .elements = planned.elements,
        };
    }
};

pub const ArenaPlan = struct {
    batch: u32,
    sequence: u32,
    max_segment_bytes: usize,
    checkpoint_stride_bytes: usize,
    checkpoint_bytes: usize,

    pub fn init(batch: u32, sequence: u32) !ArenaPlan {
        if (batch == 0 or batch > 2 or sequence == 0 or sequence > training.training_sequence_cap)
            return error.InvalidEmbeddingGemma2TapeShape;
        var maximum: usize = 0;
        var start: u32 = 0;
        while (start < 24) : (start += segment_layers) {
            const layout = try SegmentLayout.init(batch, sequence, start);
            maximum = @max(maximum, layout.total_bytes);
        }
        const rows = std.math.mul(usize, batch, sequence) catch return error.EmbeddingGemma2TapeOverflow;
        const hidden_elements = std.math.mul(usize, rows, 512) catch return error.EmbeddingGemma2TapeOverflow;
        const stride_raw = std.math.mul(usize, hidden_elements, @sizeOf(u16)) catch return error.EmbeddingGemma2TapeOverflow;
        const stride = try alignForward(stride_raw, alignment);
        return .{
            .batch = batch,
            .sequence = sequence,
            .max_segment_bytes = maximum,
            .checkpoint_stride_bytes = stride,
            .checkpoint_bytes = std.math.mul(usize, stride, checkpoint_count) catch return error.EmbeddingGemma2TapeOverflow,
        };
    }

    pub fn totalBytes(self: ArenaPlan) !usize {
        return std.math.add(usize, self.max_segment_bytes, self.checkpoint_bytes) catch error.EmbeddingGemma2TapeOverflow;
    }
};

pub const ResidentTape = struct {
    compute: *CudaCompute,
    plan: ArenaPlan,
    segment_buffer: DeviceBuffer,
    checkpoint_buffer: DeviceBuffer,
    current_start_layer: ?u32 = null,

    pub fn init(compute: *CudaCompute, batch: u32, sequence: u32) !ResidentTape {
        const plan = try ArenaPlan.init(batch, sequence);
        var result = ResidentTape{
            .compute = compute,
            .plan = plan,
            .segment_buffer = .{},
            .checkpoint_buffer = .{},
        };
        errdefer result.deinit();
        result.segment_buffer = try compute.allocEmbeddingGemma2TrainingBuffer(plan.max_segment_bytes);
        result.checkpoint_buffer = try compute.allocEmbeddingGemma2TrainingBuffer(plan.checkpoint_bytes);
        return result;
    }

    /// Reuse the consolidated segment arena for the requested two-layer pair.
    /// Callers must preserve stream order; no host synchronization is inserted.
    pub fn rewind(self: *ResidentTape, start_layer: u32) !SegmentView {
        const layout = try SegmentLayout.init(self.plan.batch, self.plan.sequence, start_layer);
        if (layout.total_bytes > self.segment_buffer.len) return error.InvalidEmbeddingGemma2TapeView;
        self.current_start_layer = start_layer;
        return .{
            .layout = layout,
            .arena = .{ .ptr = self.segment_buffer.ptr, .len = layout.total_bytes },
        };
    }

    /// Boundary is 0 for encoder input, then 1 after layer 1, ... 12 after
    /// layer 23. Every view is a BF16 [B,S,512] tensor.
    pub fn layerCheckpoint(self: *const ResidentTape, boundary: u32) !TypedView {
        if (boundary >= checkpoint_count) return error.InvalidEmbeddingGemma2CheckpointBoundary;
        const offset = std.math.mul(usize, boundary, self.plan.checkpoint_stride_bytes) catch
            return error.EmbeddingGemma2TapeOverflow;
        const rows = std.math.mul(usize, self.plan.batch, self.plan.sequence) catch return error.EmbeddingGemma2TapeOverflow;
        const elements = std.math.mul(usize, rows, 512) catch return error.EmbeddingGemma2TapeOverflow;
        const bytes = std.math.mul(usize, elements, @sizeOf(u16)) catch return error.EmbeddingGemma2TapeOverflow;
        if (offset > self.checkpoint_buffer.len or bytes > self.checkpoint_buffer.len - offset)
            return error.InvalidEmbeddingGemma2TapeView;
        return .{ .buffer = .{ .ptr = self.checkpoint_buffer.ptr + offset, .len = bytes }, .dtype = .bf16, .elements = elements };
    }

    /// Training kernels are asynchronous. Drain before releasing either slab
    /// so an error path cannot recycle memory still referenced by the stream.
    pub fn deinit(self: *ResidentTape) void {
        self.compute.synchronizeEmbeddingGemma2Training() catch {};
        self.compute.releaseEmbeddingGemma2TrainingBuffer(&self.checkpoint_buffer);
        self.compute.releaseEmbeddingGemma2TrainingBuffer(&self.segment_buffer);
        self.* = undefined;
    }
};

fn slotElements(slot: training.TapeSlot, batch: u32, sequence: u32, geometry: training.LayerGeometry) !usize {
    const rows = std.math.mul(usize, batch, sequence) catch return error.EmbeddingGemma2TapeOverflow;
    const width: usize = switch (slot) {
        .layer_input,
        .attention_norm_input,
        .attention_residual,
        .ffn_norm_input,
        .ffn_residual,
        .ple_gate_post_activation,
        .layer_output_after_scalar,
        => 512,
        .q_pre_norm, .q_post_rope, .attention_context => geometry.queryWidth(),
        .k_pre_norm, .k_post_rope, .v_post_norm => geometry.kvWidth(),
        .gate_pre_activation, .gate_post_activation, .up_projection => 2048,
        .attention_probabilities => return std.math.mul(usize, try std.math.mul(usize, rows, geometry.query_heads), sequence) catch error.EmbeddingGemma2TapeOverflow,
    };
    return std.math.mul(usize, rows, width) catch error.EmbeddingGemma2TapeOverflow;
}

fn alignForward(value: usize, boundary: usize) !usize {
    const with_padding = std.math.add(usize, value, boundary - 1) catch return error.EmbeddingGemma2TapeOverflow;
    return with_padding & ~(boundary - 1);
}

test "EmbeddingGemma2 CUDA tape uses exact local and global geometry" {
    const local = try SegmentLayout.init(2, 512, 0);
    const global_pair = try SegmentLayout.init(2, 512, 4);
    const local_q = try local.slot(0, .q_post_rope);
    const global_q = try global_pair.slot(1, .q_post_rope);
    try std.testing.expectEqual(@as(usize, 2 * 512 * 1024), local_q.elements);
    try std.testing.expectEqual(@as(usize, 2 * 512 * 2048), global_q.elements);
    try std.testing.expectEqual(DType.f32, (try local.slot(0, .attention_probabilities)).dtype);
    try std.testing.expectEqual(DType.bf16, global_q.dtype);
    for (local.layers[0]) |slot| try std.testing.expectEqual(@as(usize, 0), slot.byte_offset % alignment);
}

test "EmbeddingGemma2 CUDA tape admission covers all checkpoint boundaries" {
    const plan = try ArenaPlan.init(2, 512);
    try std.testing.expectEqual(@as(u32, 13), checkpoint_count);
    // The largest segment contains one local and one global layer. Softmax's
    // pre-cast F32 probabilities add 4 MiB per layer at B2/S512.
    try std.testing.expectEqual(@as(usize, 78 * 1024 * 1024), plan.max_segment_bytes);
    try std.testing.expectEqual(@as(usize, 2 * 512 * 512 * 2), plan.checkpoint_stride_bytes);
    try std.testing.expectEqual(plan.checkpoint_stride_bytes * 13, plan.checkpoint_bytes);
    try std.testing.expectEqual(@as(usize, 91 * 1024 * 1024), try plan.totalBytes());
}

test "EmbeddingGemma2 CUDA tape rejects invalid shapes and offsets" {
    try std.testing.expectError(error.InvalidEmbeddingGemma2TapeShape, ArenaPlan.init(3, 512));
    try std.testing.expectError(error.InvalidEmbeddingGemma2TapeShape, ArenaPlan.init(1, 513));
    try std.testing.expectError(error.InvalidEmbeddingGemma2TapeShape, SegmentLayout.init(1, 8, 1));
    try std.testing.expectError(error.InvalidEmbeddingGemma2TapeLayer, (try SegmentLayout.init(1, 8, 22)).slot(2, .layer_input));
    try std.testing.expectError(error.EmbeddingGemma2TapeOverflow, alignForward(std.math.maxInt(usize), alignment));
}
