// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Image tower, indexed text/media composition and pooled embedding forward.
//! Pixels and token IDs stay runtime operands; only the explicit row map and
//! attention layout specialize the captured program.
const std = @import("std");
const capture = @import("capture.zig");
const Tensor = @import("../backends/tensor.zig").Tensor;
const ops = @import("../ops/ops.zig");
const encoder = @import("../architectures/embedding_gemma2.zig");
const projector = @import("../architectures/gemma4_projector.zig");

const Inputs = struct { ids: Tensor, mask: Tensor, pixels: Tensor, rows: Tensor, dimensions: usize };
pub fn hasPixels(inputs: []const capture.Input) bool {
    for (inputs) |input| if (std.mem.eql(u8, input.tensor.name, "pixel_values")) return true;
    return false;
}
fn parse(cfg: encoder.Config, inputs: []const capture.Input) !Inputs {
    var ids: ?Tensor = null;
    var mask: ?Tensor = null;
    var pixels: ?Tensor = null;
    var rows: ?Tensor = null;
    var dimensions: ?Tensor = null;
    for (inputs) |input| {
        const t = input.tensor;
        const slot = if (std.mem.eql(u8, t.name, "input_ids")) &ids else if (std.mem.eql(u8, t.name, "attention_mask")) &mask else if (std.mem.eql(u8, t.name, "pixel_values")) &pixels else if (std.mem.eql(u8, t.name, "media_rows")) &rows else if (std.mem.eql(u8, t.name, "output_dimensions")) &dimensions else return error.UnexpectedModelProgramInput;
        if (slot.* != null) return error.DuplicateInputs;
        _ = try capture.inputShape(t);
        slot.* = t;
    }
    const id = ids orelse return error.MissingInputs;
    const m = mask orelse return error.MissingInputs;
    const p = pixels orelse return error.MissingInputs;
    const r = rows orelse return error.MissingInputs;
    const d = dimensions orelse return error.MissingInputs;
    if (!cfg.vision or id.dtype != .i64 or m.dtype != .i64 or r.dtype != .i64 or d.dtype != .i64 or p.dtype != .f32) return error.InvalidInputShape;
    if (!id.isAlignedFor(i64) or !m.isAlignedFor(i64) or !r.isAlignedFor(i64) or !d.isAlignedFor(i64) or !p.isAlignedFor(f32)) return error.InvalidInputShape;
    if (id.shape.len != 2 or id.shape[0] != 1 or id.shape[1] <= 0 or !std.mem.eql(i64, id.shape, m.shape) or !std.mem.eql(i64, id.shape, r.shape)) return error.InvalidInputShape;
    if (p.shape.len != 4 or p.shape[0] != 1 or p.shape[1] != 3 or p.shape[2] <= 0 or p.shape[3] <= 0 or @mod(p.shape[2], 48) != 0 or @mod(p.shape[3], 48) != 0) return error.InvalidInputShape;
    const tokens: usize = @intCast(@divExact(p.shape[2], 48) * @divExact(p.shape[3], 48));
    if (tokens > 280 or d.shape.len != 1 or d.shape[0] != 1) return error.InvalidInputShape;
    const width = std.math.cast(usize, d.asInt64()[0]) orelse return error.InvalidEmbeddingDimensions;
    if (!encoder.validDimension(width)) return error.InvalidEmbeddingDimensions;
    _ = try encoder.validateTextInputs(cfg, id.asInt64(), m.asInt64(), 1, @intCast(id.shape[1]));
    var media: usize = 0;
    for (id.asInt64(), m.asInt64(), r.asInt64(), 0..) |token, visible, row, index| {
        if (visible != 0 and visible != 1) return error.InvalidAttentionMask;
        if (token == 258880) {
            if (visible != 1 or row != id.shape[1] + @as(i64, @intCast(media))) return error.InvalidMediaRows;
            media += 1;
        } else if (row != index) return error.InvalidMediaRows;
    }
    if (media != tokens) return error.InvalidMediaRows;
    for (p.asFloat32()) |value| if (!std.math.isFinite(value) or value < 0 or value > 1) return error.InvalidImagePixels;
    return .{ .ids = id, .mask = m, .pixels = p, .rows = r, .dimensions = width };
}
pub fn validate(cfg: encoder.Config, inputs: []const capture.Input) !void {
    _ = try parse(cfg, inputs);
}
pub fn compose(cfg: encoder.Config, cb: *const ops.ComputeBackend, a: std.mem.Allocator, inputs: []const capture.Input) !ops.CT {
    const in = try parse(cfg, inputs);
    const pixels = try cb.fromFloat32Shape(in.pixels.asFloat32(), &.{ 1, 3, @intCast(in.pixels.shape[2]), @intCast(in.pixels.shape[3]) });
    defer cb.free(pixels);
    const image = try projector.encodeEmbeddingGemma2PixelTensorResident(cb, a, pixels, @intCast(in.pixels.shape[3]), @intCast(in.pixels.shape[2]));
    defer cb.free(image.tensor);
    const seq: usize = @intCast(in.ids.shape[1]);
    const text = try encoder.textEmbeddings(cb, a, cfg, in.ids.asInt64(), in.mask.asInt64(), 1, seq);
    defer cb.free(text);
    const combined = try cb.primConcatPrim(text, image.tensor, 0, &.{ @intCast(seq), @intCast(cfg.hidden_size) }, &.{ @intCast(image.tokens), @intCast(cfg.hidden_size) });
    defer cb.free(combined);
    const rows = try a.alloc(u32, seq);
    defer a.free(rows);
    for (rows, in.rows.asInt64()) |*row, value| row.* = @intCast(value);
    return (try cb.takeRows(combined, rows, seq, cfg.hidden_size)) orelse error.UnsupportedPrimitiveOp;
}
pub fn run(cfg: encoder.Config, cb: *const ops.ComputeBackend, a: std.mem.Allocator, inputs: []const capture.Input) ![]capture.Output {
    const in = try parse(cfg, inputs);
    const composed = try compose(cfg, cb, a, inputs);
    defer cb.free(composed);
    const seq: usize = @intCast(in.ids.shape[1]);
    const output = try encoder.forwardPooledEmbeddingsCT(cb, a, cfg, composed, in.mask.asInt64(), 1, seq, in.dimensions);
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
    try std.testing.expectError(error.InvalidImagePixels, validate(cfg, &inputs));
}
