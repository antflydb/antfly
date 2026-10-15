// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Allocation-free host layout and checkpoint contracts for EG2 adapters.

const std = @import("std");
const training = @import("embedding_gemma2_training.zig");

pub const adapter_count = 24 * 7 + 1;
pub const checkpoint_version: u32 = 1;
pub const header_bytes: usize = 112;
pub const magic = "EG2LORA\x00";

pub const AdapterDescriptor = struct {
    target: training.Target,
    layer: ?u8,
    input_dim: u32,
    output_dim: u32,
    rank: u32,
    a_offset: u64,
    b_offset: u64,

    pub fn aElements(self: AdapterDescriptor) u64 {
        return @as(u64, self.rank) * self.input_dim;
    }
    pub fn bElements(self: AdapterDescriptor) u64 {
        return @as(u64, self.output_dim) * self.rank;
    }
    pub fn writeBaseName(self: AdapterDescriptor, buffer: []u8) ![]const u8 {
        if (self.target == .embedding_projection) return "language_model.embedding_projection.weight";
        const layer = self.layer orelse return error.InvalidEmbeddingGemma2AdapterLayout;
        const specs = try training.projectionSpecs(layer);
        for (specs) |spec| if (spec.target == self.target) return spec.name(buffer, layer);
        return error.InvalidEmbeddingGemma2AdapterLayout;
    }
};

pub const Layout = struct {
    descriptors: [adapter_count]AdapterDescriptor,
    total_parameter_elements: u64,
    rank: u32,

    pub fn init(plan: training.TrainingPlan) !Layout {
        if (plan.rank == 0) return error.InvalidLoRARank;
        var result = Layout{ .descriptors = undefined, .total_parameter_elements = 0, .rank = plan.rank };
        var index: usize = 0;
        var offset: u64 = 0;
        for (0..24) |layer_usize| {
            const layer: u32 = @intCast(layer_usize);
            for (try training.projectionSpecs(layer)) |spec| {
                const a_elements = std.math.mul(u64, plan.rank, spec.input_dim) catch return error.AdapterLayoutOverflow;
                const b_offset = std.math.add(u64, offset, a_elements) catch return error.AdapterLayoutOverflow;
                const b_elements = std.math.mul(u64, spec.output_dim, plan.rank) catch return error.AdapterLayoutOverflow;
                result.descriptors[index] = .{
                    .target = spec.target,
                    .layer = @intCast(layer),
                    .input_dim = spec.input_dim,
                    .output_dim = spec.output_dim,
                    .rank = plan.rank,
                    .a_offset = offset,
                    .b_offset = b_offset,
                };
                offset = std.math.add(u64, b_offset, b_elements) catch return error.AdapterLayoutOverflow;
                index += 1;
            }
        }
        const spec = training.embeddingProjectionSpec;
        const a_elements = std.math.mul(u64, plan.rank, spec.input_dim) catch return error.AdapterLayoutOverflow;
        const b_offset = std.math.add(u64, offset, a_elements) catch return error.AdapterLayoutOverflow;
        const b_elements = std.math.mul(u64, spec.output_dim, plan.rank) catch return error.AdapterLayoutOverflow;
        result.descriptors[index] = .{
            .target = spec.target,
            .layer = null,
            .input_dim = spec.input_dim,
            .output_dim = spec.output_dim,
            .rank = plan.rank,
            .a_offset = offset,
            .b_offset = b_offset,
        };
        index += 1;
        result.total_parameter_elements = std.math.add(u64, b_offset, b_elements) catch return error.AdapterLayoutOverflow;
        if (index != adapter_count) return error.InvalidEmbeddingGemma2AdapterLayout;
        return result;
    }

    pub fn validate(self: *const Layout) !void {
        if (self.rank == 0) return error.InvalidEmbeddingGemma2AdapterLayout;
        var expected: u64 = 0;
        for (self.descriptors) |descriptor| {
            if (descriptor.rank != self.rank or descriptor.a_offset != expected)
                return error.InvalidEmbeddingGemma2AdapterLayout;
            expected = std.math.add(u64, descriptor.b_offset, descriptor.bElements()) catch
                return error.AdapterLayoutOverflow;
            if (descriptor.b_offset != descriptor.a_offset + descriptor.aElements())
                return error.InvalidEmbeddingGemma2AdapterLayout;
        }
        if (expected != self.total_parameter_elements) return error.InvalidEmbeddingGemma2AdapterLayout;
    }
};

pub const Header = struct {
    base_model_sha256: [32]u8,
    plan_fingerprint: [32]u8,
    rank: u32,
    total_parameter_elements: u64,
    optimizer_step: u64,
    generation: u64,
};

pub fn planFingerprint(plan: training.TrainingPlan) [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    var fixed: [24]u8 = undefined;
    std.mem.writeInt(u32, fixed[0..4], plan.rank, .little);
    std.mem.writeInt(u32, fixed[4..8], @bitCast(plan.alpha), .little);
    std.mem.writeInt(u32, fixed[8..12], @bitCast(plan.dropout), .little);
    std.mem.writeInt(u32, fixed[12..16], plan.max_sequence, .little);
    std.mem.writeInt(u32, fixed[16..20], plan.checkpoint_every_n_layers, .little);
    std.mem.writeInt(u32, fixed[20..24], @intCast(plan.mrl_dimensions.len), .little);
    hash.update(&fixed);
    for (plan.mrl_dimensions) |dimension| {
        var encoded: [4]u8 = undefined;
        std.mem.writeInt(u32, &encoded, dimension, .little);
        hash.update(&encoded);
    }
    hash.update("language_model:24x(q,k,v,o,gate,up,down)+embedding_projection");
    return hash.finalResult();
}

pub fn encodeHeader(destination: *[header_bytes]u8, header: Header) void {
    @memset(destination, 0);
    @memcpy(destination[0..8], magic);
    std.mem.writeInt(u32, destination[8..12], checkpoint_version, .little);
    std.mem.writeInt(u32, destination[12..16], header_bytes, .little);
    @memcpy(destination[16..48], &header.base_model_sha256);
    @memcpy(destination[48..80], &header.plan_fingerprint);
    std.mem.writeInt(u32, destination[80..84], header.rank, .little);
    std.mem.writeInt(u32, destination[84..88], adapter_count, .little);
    std.mem.writeInt(u64, destination[88..96], header.total_parameter_elements, .little);
    std.mem.writeInt(u64, destination[96..104], header.optimizer_step, .little);
    std.mem.writeInt(u64, destination[104..112], header.generation, .little);
}

pub fn decodeAndValidateHeader(
    source: *const [header_bytes]u8,
    layout: *const Layout,
    expected_base_sha256: [32]u8,
    expected_plan_fingerprint: [32]u8,
) !Header {
    if (!std.mem.eql(u8, source[0..8], magic) or
        std.mem.readInt(u32, source[8..12], .little) != checkpoint_version or
        std.mem.readInt(u32, source[12..16], .little) != header_bytes or
        std.mem.readInt(u32, source[84..88], .little) != adapter_count)
        return error.InvalidEmbeddingGemma2CheckpointHeader;
    var header: Header = .{
        .base_model_sha256 = source[16..48].*,
        .plan_fingerprint = source[48..80].*,
        .rank = std.mem.readInt(u32, source[80..84], .little),
        .total_parameter_elements = std.mem.readInt(u64, source[88..96], .little),
        .optimizer_step = std.mem.readInt(u64, source[96..104], .little),
        .generation = std.mem.readInt(u64, source[104..112], .little),
    };
    if (!std.mem.eql(u8, &header.base_model_sha256, &expected_base_sha256)) return error.BaseModelHashMismatch;
    if (!std.mem.eql(u8, &header.plan_fingerprint, &expected_plan_fingerprint)) return error.TrainingPlanMismatch;
    if (header.rank != layout.rank or header.total_parameter_elements != layout.total_parameter_elements)
        return error.InvalidEmbeddingGemma2CheckpointLayout;
    try layout.validate();
    return header;
}

pub const StateKind = enum { master, first_moment, second_moment };
pub const ConstState = struct {
    master: []const f32,
    first_moment: []const f32,
    second_moment: []const f32,
};
pub const MutableState = struct {
    master: []f32,
    first_moment: []f32,
    second_moment: []f32,
};
pub const WriteFn = *const fn (ctx: *anyopaque, bytes: []const u8) anyerror!void;
pub const ReadFn = *const fn (ctx: *anyopaque, bytes: []u8) anyerror!void;

pub fn writeCheckpoint(layout: *const Layout, header: Header, state: ConstState, max_chunk_elements: usize, ctx: *anyopaque, write: WriteFn) !void {
    try layout.validate();
    if (header.rank != layout.rank or header.total_parameter_elements != layout.total_parameter_elements)
        return error.InvalidEmbeddingGemma2CheckpointLayout;
    try validateStateLengths(layout, state.master.len, state.first_moment.len, state.second_moment.len, max_chunk_elements);
    var encoded: [header_bytes]u8 = undefined;
    encodeHeader(&encoded, header);
    try write(ctx, &encoded);
    inline for ([_]StateKind{ .master, .first_moment, .second_moment }) |kind| {
        const values = switch (kind) {
            .master => state.master,
            .first_moment => state.first_moment,
            .second_moment => state.second_moment,
        };
        var offset: usize = 0;
        while (offset < values.len) {
            const end = @min(values.len, offset + max_chunk_elements);
            try write(ctx, std.mem.sliceAsBytes(values[offset..end]));
            offset = end;
        }
    }
}

pub fn readCheckpoint(layout: *const Layout, expected_base: [32]u8, expected_plan: [32]u8, state: MutableState, max_chunk_elements: usize, ctx: *anyopaque, read: ReadFn) !Header {
    try validateStateLengths(layout, state.master.len, state.first_moment.len, state.second_moment.len, max_chunk_elements);
    var encoded: [header_bytes]u8 = undefined;
    try read(ctx, &encoded);
    const header = try decodeAndValidateHeader(&encoded, layout, expected_base, expected_plan);
    inline for ([_]StateKind{ .master, .first_moment, .second_moment }) |kind| {
        const values = switch (kind) {
            .master => state.master,
            .first_moment => state.first_moment,
            .second_moment => state.second_moment,
        };
        var offset: usize = 0;
        while (offset < values.len) {
            const end = @min(values.len, offset + max_chunk_elements);
            try read(ctx, std.mem.sliceAsBytes(values[offset..end]));
            offset = end;
        }
    }
    return header;
}

fn validateStateLengths(layout: *const Layout, master: usize, first: usize, second: usize, max_chunk: usize) !void {
    const expected = std.math.cast(usize, layout.total_parameter_elements) orelse return error.AdapterLayoutOverflow;
    if (max_chunk == 0 or master != expected or first != expected or second != expected)
        return error.InvalidEmbeddingGemma2CheckpointLayout;
}

const TestStream = struct {
    bytes: std.ArrayListUnmanaged(u8) = .empty,
    cursor: usize = 0,
    allocator: std.mem.Allocator,
    fn write(ctx: *anyopaque, bytes: []const u8) !void {
        const self: *TestStream = @ptrCast(@alignCast(ctx));
        try self.bytes.appendSlice(self.allocator, bytes);
    }
    fn read(ctx: *anyopaque, destination: []u8) !void {
        const self: *TestStream = @ptrCast(@alignCast(ctx));
        if (destination.len > self.bytes.items.len - self.cursor) return error.TruncatedCheckpoint;
        @memcpy(destination, self.bytes.items[self.cursor..][0..destination.len]);
        self.cursor += destination.len;
    }
};

test "EmbeddingGemma2 adapter layout enumerates exactly 169 deterministic adapters" {
    const layout = try Layout.init(.{});
    try layout.validate();
    try std.testing.expectEqual(@as(usize, adapter_count), layout.descriptors.len);
    try std.testing.expectEqual(training.Target.q_proj, layout.descriptors[0].target);
    try std.testing.expectEqual(@as(?u8, 0), layout.descriptors[0].layer);
    try std.testing.expectEqual(training.Target.embedding_projection, layout.descriptors[168].target);
    try std.testing.expectEqual(@as(?u8, null), layout.descriptors[168].layer);
}

test "EmbeddingGemma2 checkpoint roundtrip is chunked and fail closed" {
    const layout = try Layout.init(.{ .rank = 1, .alpha = 2 });
    const count: usize = @intCast(layout.total_parameter_elements);
    const values = try std.testing.allocator.alloc(f32, count);
    defer std.testing.allocator.free(values);
    for (values, 0..) |*value, index| value.* = @floatFromInt(index % 17);
    const base: [32]u8 = @splat(7);
    const fingerprint: [32]u8 = @splat(9);
    const header = Header{ .base_model_sha256 = base, .plan_fingerprint = fingerprint, .rank = 1, .total_parameter_elements = layout.total_parameter_elements, .optimizer_step = 12, .generation = 3 };
    var stream = TestStream{ .allocator = std.testing.allocator };
    defer stream.bytes.deinit(std.testing.allocator);
    try writeCheckpoint(&layout, header, .{ .master = values, .first_moment = values, .second_moment = values }, 4093, &stream, TestStream.write);
    const a = try std.testing.allocator.alloc(f32, count);
    defer std.testing.allocator.free(a);
    const first = try std.testing.allocator.alloc(f32, count);
    defer std.testing.allocator.free(first);
    const second = try std.testing.allocator.alloc(f32, count);
    defer std.testing.allocator.free(second);
    const decoded = try readCheckpoint(&layout, base, fingerprint, .{ .master = a, .first_moment = first, .second_moment = second }, 4093, &stream, TestStream.read);
    try std.testing.expectEqual(@as(u64, 12), decoded.optimizer_step);
    try std.testing.expectEqualSlices(f32, values, a);
    stream.cursor = 0;
    try std.testing.expectError(error.BaseModelHashMismatch, readCheckpoint(&layout, @splat(8), fingerprint, .{ .master = a, .first_moment = first, .second_moment = second }, 4093, &stream, TestStream.read));
    stream.cursor = 0;
    try std.testing.expectError(error.TrainingPlanMismatch, readCheckpoint(&layout, base, @splat(8), .{ .master = a, .first_moment = first, .second_moment = second }, 4093, &stream, TestStream.read));
    stream.cursor = 0;
    stream.bytes.items.len -= 1;
    try std.testing.expectError(error.TruncatedCheckpoint, readCheckpoint(&layout, base, fingerprint, .{ .master = a, .first_moment = first, .second_moment = second }, 4093, &stream, TestStream.read));
}
