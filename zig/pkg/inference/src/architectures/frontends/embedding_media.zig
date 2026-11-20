// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Captured media towers and ordered text/media composition. Processor outputs
//! remain runtime operands. Layout, validity masks and row maps specialize.
const std = @import("std");
const capture = @import("../../execution/capture.zig");
const Tensor = @import("../../backends/tensor.zig").Tensor;
const ops = @import("../../ops/ops.zig");
const encoder = @import("../embedding_gemma2.zig");
const projector = @import("../gemma4_projector.zig");

pub const Kind = enum(i64) { image = 1, audio = 2, video = 3 };
const Media = struct {
    kind: Kind,
    source: usize,
    values: Tensor,
    mask: ?Tensor = null,
    width: usize,
    height: usize,
    tokens: usize,
    fn token(self: Media) i64 {
        return switch (self.kind) {
            .image => 258880,
            .audio => 258881,
            .video => 258884,
        };
    }
};
const named = capture.findTensor;
const Inputs = struct {
    ids: Tensor,
    mask: Tensor,
    rows: Tensor,
    dimensions: usize,
    source: []const capture.Input,
    layout: ?Tensor,
    fn count(self: Inputs) usize {
        return if (self.layout) |l| @intCast(l.shape[0]) else 1;
    }
    fn media(self: Inputs, index: usize) !Media {
        if (self.layout) |layout| {
            const row = layout.asInt64()[index * 5 ..][0..5];
            const kind = std.enums.fromInt(Kind, row[0]) orelse return error.InvalidMediaLayout;
            const width = std.math.cast(usize, row[1]) orelse return error.InvalidMediaLayout;
            const height = std.math.cast(usize, row[2]) orelse return error.InvalidMediaLayout;
            const tokens = std.math.cast(usize, row[3]) orelse return error.InvalidMediaLayout;
            const source = std.math.cast(usize, row[4]) orelse return error.InvalidMediaLayout;
            if (source > index) return error.InvalidMediaLayout;
            if (source != index) {
                const original = layout.asInt64()[source * 5 ..][0..5];
                if (original[4] != source or !std.mem.eql(i64, row[0..4], original[0..4])) return error.InvalidMediaLayout;
            }
            var buf: [64]u8 = undefined;
            const values = named(self.source, try std.fmt.bufPrint(&buf, "media_values_{d}", .{source})) orelse return error.MissingInputs;
            const mask = named(self.source, try std.fmt.bufPrint(&buf, "media_mask_{d}", .{source}));
            return .{ .kind = kind, .source = source, .values = values, .mask = mask, .width = width, .height = height, .tokens = tokens };
        }
        const pixels = named(self.source, "pixel_values") orelse return error.MissingInputs;
        if (pixels.shape.len != 4 or pixels.shape[2] <= 0 or pixels.shape[3] <= 0) return error.InvalidInputShape;
        const width: usize = @intCast(pixels.shape[3]);
        const height: usize = @intCast(pixels.shape[2]);
        return .{ .kind = .image, .source = 0, .values = pixels, .width = width, .height = height, .tokens = (width / 48) * (height / 48) };
    }
};
pub fn hasMedia(inputs: []const capture.Input) bool {
    return named(inputs, "pixel_values") != null or named(inputs, "media_layout") != null;
}
fn parse(cfg: encoder.Config, inputs: []const capture.Input) !Inputs {
    try capture.validateHostInputs(inputs);
    const id = named(inputs, "input_ids") orelse return error.MissingInputs;
    const m = named(inputs, "attention_mask") orelse return error.MissingInputs;
    const r = named(inputs, "media_rows") orelse return error.MissingInputs;
    const d = named(inputs, "output_dimensions") orelse return error.MissingInputs;
    const layout = named(inputs, "media_layout");
    if (id.dtype != .i64 or m.dtype != .i64 or r.dtype != .i64 or d.dtype != .i64 or !id.isAlignedFor(i64) or !m.isAlignedFor(i64) or !r.isAlignedFor(i64) or !d.isAlignedFor(i64)) return error.InvalidInputShape;
    if (id.shape.len != 2 or id.shape[0] != 1 or id.shape[1] <= 0 or !std.mem.eql(i64, id.shape, m.shape) or !std.mem.eql(i64, id.shape, r.shape) or d.shape.len != 1 or d.shape[0] != 1) return error.InvalidInputShape;
    if (layout) |l| {
        if (l.dtype != .i64 or !l.isAlignedFor(i64) or l.shape.len != 2 or l.shape[0] <= 0 or l.shape[0] > id.shape[1] or l.shape[1] != 5 or named(inputs, "pixel_values") != null) return error.InvalidMediaLayout;
    }
    const width = std.math.cast(usize, d.asInt64()[0]) orelse return error.InvalidEmbeddingDimensions;
    if (!encoder.validDimension(width)) return error.InvalidEmbeddingDimensions;
    _ = try encoder.validateTextInputs(cfg, id.asInt64(), m.asInt64(), 1, @intCast(id.shape[1]));
    const result = Inputs{ .ids = id, .mask = m, .rows = r, .dimensions = width, .source = inputs, .layout = layout };
    var masks: usize = 0;
    var owners: usize = 0;
    for (0..result.count()) |index| {
        const item = try result.media(index);
        const v = item.values;
        owners += @intFromBool(item.source == index);
        if (v.dtype != .f32 or !v.isAlignedFor(f32) or item.width == 0 or item.height == 0 or item.tokens == 0) return error.InvalidInputShape;
        switch (item.kind) {
            .image, .video => {
                if (!cfg.vision or item.width > std.math.maxInt(u32) or item.height > std.math.maxInt(u32) or item.width % 48 != 0 or item.height % 48 != 0 or item.mask != null) return error.InvalidInputShape;
                const tokens = try std.math.mul(usize, item.width / 48, item.height / 48);
                if (tokens != item.tokens or tokens > (if (item.kind == .image) @as(usize, 280) else 140)) return error.InvalidInputShape;
                if (item.kind == .image) {
                    if (!std.mem.eql(i64, v.shape, &.{ 1, 3, @intCast(item.height), @intCast(item.width) })) return error.InvalidInputShape;
                } else if (!std.mem.eql(i64, v.shape, &.{ @intCast(tokens * 9), 768 })) return error.InvalidInputShape;
            },
            .audio => {
                if (!cfg.audio or item.width != 128 or item.height > 32768 or !std.mem.eql(i64, v.shape, &.{ @intCast(item.height), 128 })) return error.InvalidInputShape;
                const valid = item.mask orelse return error.MissingInputs;
                masks += @intFromBool(item.source == index);
                if (valid.dtype != .i64 or !valid.isAlignedFor(i64) or !std.mem.eql(i64, valid.shape, &.{@intCast(item.height)})) return error.InvalidInputShape;
                var tokens: usize = 0;
                for (valid.asInt64(), 0..) |bit, row| {
                    if (bit != 0 and bit != 1) return error.InvalidAttentionMask;
                    if (row % 4 == 0 and bit == 1) tokens += 1;
                    if (bit == 0) for (v.asFloat32()[row * 128 ..][0..128]) |value| {
                        if (value != 0) return error.InvalidAudioFeatures;
                    };
                }
                if (tokens != item.tokens) return error.InvalidMediaLayout;
            },
        }
        for (v.asFloat32()) |value| {
            if (!std.math.isFinite(value)) return error.InvalidMediaValues;
            if (item.kind == .image and (value < 0 or value > 1)) return error.InvalidImagePixels;
        }
    }
    if (inputs.len != (if (layout != null) 5 + owners + masks else @as(usize, 5))) return error.UnexpectedModelProgramInput;
    var media_index: usize = 0;
    var offset: usize = 0;
    var within: usize = 0;
    for (id.asInt64(), m.asInt64(), r.asInt64(), 0..) |token, visible, row, index| {
        if (visible != 0 and visible != 1) return error.InvalidAttentionMask;
        if (token == 258880 or token == 258881 or token == 258884) {
            if (media_index >= result.count()) return error.InvalidMediaRows;
            const item = try result.media(media_index);
            if (token != item.token() or visible != 1 or row != id.shape[1] + @as(i64, @intCast(offset + within))) return error.InvalidMediaRows;
            within += 1;
            if (within == item.tokens) {
                offset += within;
                within = 0;
                media_index += 1;
            }
        } else if (row != index) return error.InvalidMediaRows;
    }
    if (media_index != result.count() or within != 0) return error.InvalidMediaRows;
    return result;
}
pub fn validate(cfg: encoder.Config, inputs: []const capture.Input) !void {
    _ = try parse(cfg, inputs);
}
pub fn compose(cfg: encoder.Config, cb: *const ops.ComputeBackend, a: std.mem.Allocator, inputs: []const capture.Input) !ops.CT {
    const in = try parse(cfg, inputs);
    const seq: usize = @intCast(in.ids.shape[1]);
    var combined = try encoder.textEmbeddings(cb, a, cfg, in.ids.asInt64(), in.mask.asInt64(), 1, seq);
    defer cb.free(combined);
    const projections = try a.alloc(?ops.CT, in.count());
    @memset(projections, null);
    defer {
        for (projections) |tensor| if (tensor) |value| cb.free(value);
        a.free(projections);
    }
    var count = seq;
    for (0..in.count()) |index| {
        const item = try in.media(index);
        if (projections[index] == null) {
            if (item.source < index) {
                projections[index] = try cb.primReshape(projections[item.source].?, &.{ @intCast(item.tokens), @intCast(cfg.hidden_size) });
            } else if (item.kind == .audio) {
                const values = try cb.fromFloat32Shape(item.values.asFloat32(), &.{ @intCast(item.height), 128 });
                defer cb.free(values);
                const mask = try a.alloc(bool, item.height);
                defer a.free(mask);
                for (mask, item.mask.?.asInt64()) |*dst, value| dst.* = value != 0;
                projections[index] = (try projector.encodeEmbeddingGemma2AudioFeatureTensorResident(cb, a, values, mask, item.height, item.width)).embeddings;
            } else {
                // Equal geometry shares projection/FFN work, while attention
                // retains an independent batch axis. Repeated presentation slots
                // refer to their earlier owner and never enter a second tower.
                var cohort: [8]usize = undefined;
                var batch: usize = 0;
                const capacity = try projector.embeddingGemma2VideoBatchCapacityForBackend(@intCast(item.width), @intCast(item.height), 768 * 1024 * 1024, false);
                for (index..in.count()) |candidate| {
                    const other = try in.media(candidate);
                    if (other.source != candidate or projections[candidate] != null or other.kind != item.kind or other.width != item.width or other.height != item.height) continue;
                    cohort[batch] = candidate;
                    batch += 1;
                    if (batch == @min(capacity, cohort.len)) break;
                }
                var joined: ?ops.CT = null;
                defer if (joined) |value| cb.free(value);
                for (cohort[0..batch], 0..) |candidate, offset| {
                    const other = try in.media(candidate);
                    const shape = try a.alloc(i32, other.values.shape.len);
                    defer a.free(shape);
                    for (shape, other.values.shape) |*dst, value| dst.* = @intCast(value);
                    const values = try cb.fromFloat32Shape(other.values.asFloat32(), shape);
                    if (joined) |previous| {
                        defer cb.free(values);
                        var left: [4]i64 = undefined;
                        @memcpy(left[0..other.values.shape.len], other.values.shape);
                        left[0] *= @intCast(offset);
                        const next = try cb.primConcatPrim(previous, values, 0, left[0..other.values.shape.len], other.values.shape);
                        cb.free(previous);
                        joined = next;
                    } else joined = values;
                }
                const encoded = if (item.kind == .image) try projector.encodeEmbeddingGemma2PixelBatchTensorResident(cb, a, joined.?, item.width, item.height, batch) else try projector.encodeEmbeddingGemma2VideoBatchTensorResident(cb, a, joined.?, @intCast(item.width), @intCast(item.height), batch);
                defer cb.free(encoded.tensor);
                if (encoded.tokens != batch * item.tokens) return error.InvalidMediaLayout;
                for (cohort[0..batch], 0..) |candidate, offset| projections[candidate] = try cb.sliceRows2D(a, encoded.tensor, offset * item.tokens, item.tokens, cfg.hidden_size);
            }
        }
        const projected = projections[index].?;
        const next = try cb.primConcatPrim(combined, projected, 0, &.{ @intCast(count), @intCast(cfg.hidden_size) }, &.{ @intCast(item.tokens), @intCast(cfg.hidden_size) });
        cb.free(combined);
        combined = next;
        count += item.tokens;
    }
    const rows = try a.alloc(u32, seq);
    defer a.free(rows);
    for (rows, in.rows.asInt64()) |*row, value| row.* = @intCast(value);
    return (try cb.takeRows(combined, rows, seq, cfg.hidden_size)) orelse error.UnsupportedPrimitiveOp;
}
pub fn run(cfg: encoder.Config, cb: *const ops.ComputeBackend, a: std.mem.Allocator, inputs: []const capture.Input) ![]capture.Output {
    const in = try parse(cfg, inputs);
    const composed = try compose(cfg, cb, a, inputs);
    defer cb.free(composed);
    const output = try encoder.forwardPooledEmbeddingsCT(cb, a, cfg, composed, in.mask.asInt64(), 1, @intCast(in.ids.shape[1]), in.dimensions);
    errdefer cb.free(output);
    const outputs = try a.alloc(capture.Output, 1);
    outputs[0] = .{ .name = "sentence_embedding", .value = output };
    return outputs;
}

test "compiled image inputs validate composition again on cache hits" {
    const a = std.testing.allocator;
    var ids = try Tensor.initInt64(a, "input_ids", &.{ 1, 3 }, &.{ 2, 258880, 3 });
    defer ids.deinit();
    var mask = try Tensor.initInt64(a, "attention_mask", &.{ 1, 3 }, &.{ 1, 1, 1 });
    defer mask.deinit();
    var rows = try Tensor.initInt64(a, "media_rows", &.{ 1, 3 }, &.{ 0, 3, 2 });
    defer rows.deinit();
    const values = try a.alloc(f32, 3 * 48 * 48);
    defer a.free(values);
    @memset(values, 0.5);
    var pixels = try Tensor.initFloat32(a, "pixel_values", &.{ 1, 3, 48, 48 }, values);
    defer pixels.deinit();
    var width = try Tensor.initInt64(a, "output_dimensions", &.{1}, &.{128});
    defer width.deinit();
    const inputs = [_]capture.Input{ .{ .tensor = ids }, .{ .tensor = mask }, .{ .tensor = rows, .specialize = true }, .{ .tensor = pixels }, .{ .tensor = width, .specialize = true } };
    const cfg = encoder.Config{ .vision = true };
    try validate(cfg, &inputs);
    @as([*]i64, @ptrCast(@alignCast(rows.data.ptr)))[1] = 4;
    try std.testing.expectError(error.InvalidMediaRows, validate(cfg, &inputs));
    @as([*]i64, @ptrCast(@alignCast(rows.data.ptr)))[1] = 3;
    @as([*]i64, @ptrCast(@alignCast(mask.data.ptr)))[1] = 0;
    try std.testing.expectError(error.InvalidMediaRows, validate(cfg, &inputs));
    @as([*]i64, @ptrCast(@alignCast(mask.data.ptr)))[1] = 1;
    pixels.asFloat32Mut()[0] = std.math.nan(f32);
    try std.testing.expectError(error.InvalidMediaValues, validate(cfg, &inputs));
}

test "compiled media inputs validate ordered audio video and duplicate projection ownership" {
    const a = std.testing.allocator;
    var ids = try Tensor.initInt64(a, "input_ids", &.{ 1, 7 }, &.{ 2, 258880, 258881, 258881, 258884, 258884, 1 });
    defer ids.deinit();
    var mask = try Tensor.initInt64(a, "attention_mask", &.{ 1, 7 }, &.{ 1, 1, 1, 1, 1, 1, 1 });
    defer mask.deinit();
    var rows = try Tensor.initInt64(a, "media_rows", &.{ 1, 7 }, &.{ 0, 7, 8, 9, 10, 11, 6 });
    defer rows.deinit();
    var layout = try Tensor.initInt64(a, "media_layout", &.{ 4, 5 }, &.{ 1, 48, 48, 1, 0, 2, 128, 5, 2, 1, 3, 48, 48, 1, 2, 3, 48, 48, 1, 2 });
    defer layout.deinit();
    var dimensions = try Tensor.initInt64(a, "output_dimensions", &.{1}, &.{128});
    defer dimensions.deinit();
    const values = try a.alloc(f32, 3 * 48 * 48);
    defer a.free(values);
    @memset(values, 0.2);
    var image = try Tensor.initFloat32(a, "media_values_0", &.{ 1, 3, 48, 48 }, values);
    defer image.deinit();
    @memset(values, 0);
    var video = try Tensor.initFloat32(a, "media_values_2", &.{ 9, 768 }, values);
    defer video.deinit();
    var audio = try Tensor.initFloat32(a, "media_values_1", &.{ 5, 128 }, values[0 .. 5 * 128]);
    defer audio.deinit();
    var audio_mask = try Tensor.initInt64(a, "media_mask_1", &.{5}, &.{ 1, 1, 0, 1, 1 });
    defer audio_mask.deinit();
    const inputs = [_]capture.Input{ .{ .tensor = ids }, .{ .tensor = mask }, .{ .tensor = rows }, .{ .tensor = layout }, .{ .tensor = dimensions }, .{ .tensor = image }, .{ .tensor = audio }, .{ .tensor = video }, .{ .tensor = audio_mask } };
    const cfg = encoder.Config{ .vision = true, .audio = true };
    try validate(cfg, &inputs);
    const mutable: []i64 = @alignCast(std.mem.bytesAsSlice(i64, layout.data));
    mutable[19] = 0;
    try std.testing.expectError(error.InvalidMediaLayout, validate(cfg, &inputs));
    mutable[19] = 2;
    audio.asFloat32Mut()[2 * 128] = 1;
    try std.testing.expectError(error.InvalidAudioFeatures, validate(cfg, &inputs));
    audio.asFloat32Mut()[2 * 128] = 0;
    mutable[8] = 1;
    try std.testing.expectError(error.InvalidMediaLayout, validate(cfg, &inputs));
}
