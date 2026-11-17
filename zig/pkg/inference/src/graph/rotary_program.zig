// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Shared rotary coefficients and explicit low-precision arithmetic. Layouts
//! use slices/concatenation; no dense permutation or activation host readback.
const std = @import("std");
const ml = @import("ml");
const Allocator = std.mem.Allocator;
const Graph = ml.graph.Graph;
const Builder = ml.graph.Builder;
const Shape = ml.graph.Shape;
const Id = ml.graph.NodeId;

pub const Kind = enum { cos, sin };

/// Axis-major positions, half-head pairing. The caller owns axis reordering;
/// each token's phase is shared by every head without activation readback.
pub fn emitVision(g: *Graph, input: Id, rows: usize, dim: usize, theta: f32, positions: []const u32) !Id {
    return emitAxis(g, input, rows, dim, theta, 1, positions, .{ 0, 0, 0 }, true, null);
}

/// Interleaved temporal/height/width positions. Sections count rotary pairs;
/// partial-head slicing remains owned by ComputeBackend.partialMrope.
pub fn emitMrope(g: *Graph, input: Id, rows: usize, dim: usize, theta: f32, scale: f32, positions: []const u32, sections: [3]u32) !Id {
    return emitAxis(g, input, rows, dim, theta, scale, positions, sections, false, null);
}

pub fn emitMropeRuntime(g: *Graph, input: Id, rows: usize, dim: usize, theta: f32, scale: f32, positions: []const u32, sections: [3]u32, runtime_positions: Id) !Id {
    return emitAxis(g, input, rows, dim, theta, scale, positions, sections, false, runtime_positions);
}

fn emitAxis(g: *Graph, input: Id, rows: usize, dim: usize, theta: f32, scale: f32, positions: []const u32, sections: [3]u32, comptime vision: bool, runtime_positions: ?Id) !Id {
    const source = g.node(input).output_shape;
    if (rows == 0 or dim == 0 or dim % (if (vision) @as(usize, 4) else 2) != 0 or !std.math.isFinite(theta) or theta <= 0 or !std.math.isFinite(scale) or positions.len != try std.math.mul(usize, rows, if (vision) @as(usize, 2) else 3)) return error.InvalidRoPEInput;
    const elements: usize = @intCast(source.numElements() orelse return error.InvalidRoPEInput);
    const row_width = try std.math.mul(usize, rows, dim);
    if (elements % row_width != 0 or elements == 0) return error.InvalidRoPEInput;
    const heads = elements / row_width;
    const half = dim / 2;
    if (!vision and @as(usize, sections[0]) + sections[1] + sections[2] != half) return error.InvalidRoPEInput;
    const cosines = try g.allocator.alloc(f32, try std.math.mul(usize, rows, half));
    defer g.allocator.free(cosines);
    const sines = try g.allocator.alloc(f32, cosines.len);
    defer g.allocator.free(sines);
    for (0..rows) |row| for (0..half) |pair| {
        const axis: usize = if (vision) pair / (half / 2) else if (pair % 3 == 1 and pair < @as(usize, sections[1]) * 3) 1 else if (pair % 3 == 2 and pair < @as(usize, sections[2]) * 3) 2 else 0;
        const local_pair = if (vision) pair % (half / 2) else pair;
        const frequency_dim = if (vision) half else dim;
        const frequency = 1 / std.math.pow(f32, theta, @as(f32, @floatFromInt(2 * local_pair)) / @as(f32, @floatFromInt(frequency_dim)));
        const position: f32 = @floatFromInt(positions[axis * rows + row]);
        const angle = if (vision) position * frequency else position * scale * frequency;
        cosines[row * half + pair] = @cos(angle);
        sines[row * half + pair] = @sin(angle);
    };
    var b = Builder.init(g);
    const coefficient_shape = Shape.init(.f32, &.{ @intCast(rows), 1, @intCast(half) });
    var c: Id = undefined;
    var sine: Id = undefined;
    if (runtime_positions) |runtime| {
        if (!std.mem.eql(i64, g.node(runtime).output_shape.dims[0..g.node(runtime).output_shape.rank()], &.{ 3, @as(i64, @intCast(rows)) })) return error.InvalidRoPEInput;
        const axes = try g.allocator.alloc(i32, half);
        defer g.allocator.free(axes);
        const frequencies = try g.allocator.alloc(f32, half);
        defer g.allocator.free(frequencies);
        for (axes, frequencies, 0..) |*axis, *frequency, pair| {
            axis.* = if (pair % 3 == 1 and pair < @as(usize, sections[1]) * 3) 1 else if (pair % 3 == 2 and pair < @as(usize, sections[2]) * 3) 2 else 0;
            frequency.* = 1 / std.math.pow(f32, theta, @as(f32, @floatFromInt(2 * pair)) / @as(f32, @floatFromInt(dim)));
        }
        const indices = try b.tensorConstBytes(std.mem.sliceAsBytes(axes), Shape.init(.i32, &.{@intCast(half)}));
        const gathered = try b.gather(runtime, indices, Shape.init(.i64, &.{ @intCast(half), @intCast(rows) }));
        const p = try b.convertDtype(try b.transpose(gathered, &.{ 1, 0 }), .f32);
        const angles = try b.mul(try b.mul(p, try b.scalarConst(.f32, scale)), try b.tensorConst(frequencies, Shape.init(.f32, &.{ 1, @intCast(half) })));
        c = try b.reshape(try b.cosOp(angles), coefficient_shape);
        sine = try b.reshape(try b.sinOp(angles), coefficient_shape);
    } else {
        c = try b.tensorConst(cosines, coefficient_shape);
        sine = try b.tensorConst(sines, coefficient_shape);
    }
    const x = try b.reshape(input, Shape.init(.f32, &.{ @intCast(rows), @intCast(heads), @intCast(dim) }));
    const left = try slice(g, x, 0, @intCast(half), 1);
    const right = try slice(g, x, @intCast(half), @intCast(dim), 1);
    const low = try b.sub(try b.mul(left, c), try b.mul(right, sine));
    const high = try b.add(try b.mul(left, sine), try b.mul(right, c));
    return b.reshape(try b.concat(low, high, 2), source);
}

pub fn coefficients(
    allocator: Allocator,
    rows: usize,
    hidden: usize,
    head_dim: usize,
    rope_dim: usize,
    theta: f32,
    freq_scale: f32,
    position_offset: usize,
    consecutive_pairs: bool,
    kind: Kind,
) ![]f32 {
    const data = try allocator.alloc(f32, rows * hidden);
    @memset(data, switch (kind) {
        .cos => 1.0,
        .sin => 0.0,
    });
    const num_heads = hidden / head_dim;
    const half = rope_dim / 2;
    const head_half = head_dim / 2;
    for (0..rows) |row| {
        const pos = position_offset + row;
        for (0..num_heads) |head| {
            const head_base = row * hidden + head * head_dim;
            for (0..half) |j| {
                const freq = 1.0 / std.math.pow(f32, theta, @as(f32, @floatFromInt(2 * j)) / @as(f32, @floatFromInt(rope_dim)));
                const angle = @as(f32, @floatFromInt(pos)) * freq_scale * freq;
                const value = switch (kind) {
                    .cos => @cos(angle),
                    .sin => @sin(angle),
                };
                const idx0 = if (consecutive_pairs) 2 * j else j;
                const idx1 = if (consecutive_pairs) 2 * j + 1 else j + head_half;
                data[head_base + idx0] = value;
                data[head_base + idx1] = value;
            }
        }
    }
    return data;
}

fn narrow(builder: *Builder, value: Id) !Id {
    return builder.convertDtype(try builder.convertDtype(value, .bf16), .f32);
}
fn slice(g: *Graph, input: Id, start: i64, stop: i64, stride: i64) !Id {
    const source = g.node(input).output_shape;
    var attrs = ml.graph.node.SliceAttrs{};
    attrs.num_axes = source.rank();
    var shape = source;
    for (0..source.rank()) |axis| {
        attrs.limits[axis] = source.dims[axis];
        attrs.strides[axis] = 1;
    }
    const axis = source.rank() - 1;
    attrs.starts[axis] = start;
    attrs.limits[axis] = stop;
    attrs.strides[axis] = stride;
    shape.dims[axis] = @divFloor(stop - start + stride - 1, stride);
    return g.addNode(.{ .op = .{ .slice = attrs }, .output_shape = shape, .inputs = .{ input, ml.graph.null_node, ml.graph.null_node, ml.graph.null_node }, .num_inputs = 1 });
}
fn zeros(g: *Graph, b: *Builder, shape: Shape) !Id {
    return g.addNode(.{ .op = .{ .broadcast_in_dim = .{ .target_shape = shape, .num_axes = 0 } }, .output_shape = shape, .inputs = .{ try b.scalarConst(.f32, 0), ml.graph.null_node, ml.graph.null_node, ml.graph.null_node }, .num_inputs = 1 });
}

/// BF16 coefficients, separately rounded products and a BF16 publication.
/// F32 input/output views keep reduction and host ABI contracts independent.
pub fn emitBf16(g: *Graph, input: Id, attrs: ml.graph.node.RopeAttrs) !Id {
    const source = g.node(input).output_shape;
    if (source.dtype != .f32 or source.rank() != 2) return error.UnsupportedShape;
    const rows: usize = @intCast(source.dims[0]);
    const width: usize = @intCast(source.dims[1]);
    const dim: usize = attrs.head_dim;
    const rotary: usize = if (attrs.rope_dim == 0) dim else attrs.rope_dim;
    if (dim == 0 or width % dim != 0 or rotary == 0 or rotary > dim or rotary % 2 != 0 or rows != attrs.seq_len) return error.UnsupportedShape;
    var b = Builder.init(g);
    const heads: i64 = @intCast(width / dim);
    const r: i64 = @intCast(rows);
    const d: i64 = @intCast(dim);
    const rot: i64 = @intCast(rotary);
    const half = @divExact(rot, 2);
    const shape = Shape.init(.f32, &.{ r, heads, d });
    const x = try b.reshape(input, shape);
    var rotated: Id = undefined;
    if (attrs.consecutive_pairs) {
        const even = try slice(g, x, 0, rot, 2);
        const odd = try b.neg(try slice(g, x, 1, rot, 2));
        const pair_shape = Shape.init(.f32, &.{ r, heads, half, 1 });
        rotated = try b.reshape(try b.concat(try b.reshape(odd, pair_shape), try b.reshape(even, pair_shape), 3), Shape.init(.f32, &.{ r, heads, rot }));
        if (rot != d) rotated = try b.concat(rotated, try zeros(g, &b, Shape.init(.f32, &.{ r, heads, d - rot })), 2);
    } else {
        const head_half = @divExact(d, 2);
        var first = try slice(g, x, 0, half, 1);
        var negative = try b.neg(try slice(g, x, head_half, head_half + half, 1));
        if (half != head_half) {
            const gap = try zeros(g, &b, Shape.init(.f32, &.{ r, heads, head_half - half }));
            first = try b.concat(first, gap, 2);
            negative = try b.concat(negative, gap, 2);
        }
        rotated = try b.concat(negative, first, 2);
    }
    const flat = try b.reshape(rotated, source);
    const cos_values = try coefficients(g.allocator, rows, width, dim, rotary, attrs.theta, attrs.freq_scale, attrs.position_offset, attrs.consecutive_pairs, .cos);
    defer g.allocator.free(cos_values);
    const sin_values = try coefficients(g.allocator, rows, width, dim, rotary, attrs.theta, attrs.freq_scale, attrs.position_offset, attrs.consecutive_pairs, .sin);
    defer g.allocator.free(sin_values);
    const cosine = try narrow(&b, try b.tensorConst(cos_values, source));
    const sine = try narrow(&b, try b.tensorConst(sin_values, source));
    const result = try narrow(&b, try b.add(try narrow(&b, try b.mul(input, cosine)), try narrow(&b, try b.mul(flat, sine))));
    if (rotary == dim) return result;
    const active = try g.allocator.alloc(u8, width);
    defer g.allocator.free(active);
    for (active, 0..) |*flag, column| {
        const lane = column % dim;
        flag.* = @intFromBool(if (attrs.consecutive_pairs) lane < rotary else lane < rotary / 2 or (lane >= dim / 2 and lane < dim / 2 + rotary / 2));
    }
    const flags = try b.tensorConstBytes(active, Shape.init(.bool_, &.{@intCast(width)}));
    var broadcast = ml.graph.node.BroadcastAttrs{ .target_shape = Shape.init(.bool_, source.dims[0..source.rank()]), .num_axes = 1 };
    broadcast.broadcast_axes[0] = 1;
    const predicate = try g.addNode(.{ .op = .{ .broadcast_in_dim = broadcast }, .output_shape = broadcast.target_shape, .inputs = .{ flags, ml.graph.null_node, ml.graph.null_node, ml.graph.null_node }, .num_inputs = 1 });
    return g.addNode(.{ .op = .{ .where_select = {} }, .output_shape = source, .inputs = .{ predicate, result, input, ml.graph.null_node }, .num_inputs = 3 });
}
