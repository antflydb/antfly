// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Packed block semantics shared by CPU evaluation and GPU source lowering.
//! Recipes contain only scalar arithmetic; vectorization, tiles, reductions,
//! dp4a/tensor-core lowering and fusion remain target schedule decisions.
const std = @import("std");
const quant = @import("quant_matmul.zig");
const tensor_types = @import("../gguf/tensor_types.zig");

pub const Id = u16;
const Pair = struct { a: Id, b: Id };
const Choice = struct { condition: Id, yes: Id, no: Id };
pub const Expression = union(enum) {
    lane,
    integer: i32,
    byte: Id,
    half: Id,
    float: Id,
    word: Id,
    to_float: Id,
    add: Pair,
    subtract: Pair,
    multiply: Pair,
    bit_and: Pair,
    bit_or: Pair,
    shift_left: Pair,
    shift_right: Pair,
    less: Pair,
    choose: Choice,
};
pub const Recipe = struct {
    format: quant.Format,
    nodes: [128]Expression = undefined,
    count: u16 = 0,
    result: Id = 0,
    scale_component: ?Id = null,
    value_component: ?Id = null,

    /// Prune a component before lowering it. Target schedules can hoist a
    /// block/subblock scale without maintaining another packed-format decoder.
    pub fn component(self: Recipe, result: Id) Recipe {
        var live: [128]bool = @splat(false);
        live[result] = true;
        var i: usize = self.count;
        while (i > 0) {
            i -= 1;
            if (!live[i]) continue;
            switch (self.nodes[i]) {
                .byte, .half, .float, .word, .to_float => |p| live[p] = true,
                .add, .subtract, .multiply, .bit_and, .bit_or, .shift_left, .shift_right, .less => |p| {
                    live[p.a] = true;
                    live[p.b] = true;
                },
                .choose => |c| {
                    live[c.condition] = true;
                    live[c.yes] = true;
                    live[c.no] = true;
                },
                else => {},
            }
        }
        var reduced = Recipe{ .format = self.format };
        var ids: [128]Id = undefined;
        for (self.nodes[0..self.count], 0..) |node, index| {
            if (!live[index]) continue;
            ids[index] = reduced.count;
            reduced.nodes[reduced.count] = switch (node) {
                inline .byte, .half, .float, .word, .to_float => |p, tag| @unionInit(Expression, @tagName(tag), ids[p]),
                inline .add, .subtract, .multiply, .bit_and, .bit_or, .shift_left, .shift_right, .less => |p, tag| @unionInit(Expression, @tagName(tag), Pair{ .a = ids[p.a], .b = ids[p.b] }),
                .choose => |c| .{ .choose = .{ .condition = ids[c.condition], .yes = ids[c.yes], .no = ids[c.no] } },
                else => node,
            };
            reduced.count += 1;
        }
        reduced.result = ids[result];
        return reduced;
    }

    pub fn tensorType(self: Recipe) tensor_types.TensorType {
        return .{ .known = switch (self.format) {
            .q4_0 => .Q4_0,
            .q4_1 => .Q4_1,
            .q5_0 => .Q5_0,
            .q5_1 => .Q5_1,
            .q8_0 => .Q8_0,
            .q8_1 => .Q8_1,
            .q8_k => .Q8_K,
            .q2_k => .Q2_K,
            .q3_k => .Q3_K,
            .q4_k => .Q4_K,
            .q5_k => .Q5_K,
            .q6_k => .Q6_K,
            else => unreachable,
        } };
    }
    pub fn blockBytes(self: Recipe) usize {
        return tensor_types.bytesPerBlock(self.tensorType()).?;
    }
    pub fn blockValues(self: Recipe) usize {
        return tensor_types.valuesPerBlock(self.tensorType()).?;
    }

    pub fn isFloat(self: *const Recipe, id: Id) bool {
        return switch (self.nodes[id]) {
            .half, .float, .to_float => true,
            .add, .subtract, .multiply => |p| self.isFloat(p.a),
            .choose => |c| self.isFloat(c.yes),
            else => false,
        };
    }
};
const Builder = struct {
    recipe: Recipe,
    fn put(b: *Builder, expression: Expression) Id {
        const id = b.recipe.count;
        b.recipe.nodes[id] = expression;
        b.recipe.count += 1;
        return id;
    }
    fn i(b: *Builder, value: i32) Id {
        return b.put(.{ .integer = value });
    }
    fn pair(b: *Builder, comptime tag: std.meta.Tag(Expression), a: Id, c: Id) Id {
        return b.put(@unionInit(Expression, @tagName(tag), Pair{ .a = a, .b = c }));
    }
    fn add(b: *Builder, a: Id, c: Id) Id {
        return b.pair(.add, a, c);
    }
    fn sub(b: *Builder, a: Id, c: Id) Id {
        return b.pair(.subtract, a, c);
    }
    fn mul(b: *Builder, a: Id, c: Id) Id {
        return b.pair(.multiply, a, c);
    }
    fn mask(b: *Builder, a: Id, c: i32) Id {
        return b.pair(.bit_and, a, b.i(c));
    }
    fn shr(b: *Builder, a: Id, c: i32) Id {
        return b.pair(.shift_right, a, b.i(c));
    }
    fn shl(b: *Builder, a: Id, c: i32) Id {
        return b.pair(.shift_left, a, b.i(c));
    }
    fn lt(b: *Builder, a: Id, c: i32) Id {
        return b.pair(.less, a, b.i(c));
    }
    fn select(b: *Builder, condition: Id, yes: Id, no: Id) Id {
        return b.put(.{ .choose = .{ .condition = condition, .yes = yes, .no = no } });
    }
    fn byte(b: *Builder, offset: Id) Id {
        return b.put(.{ .byte = offset });
    }
    fn half(b: *Builder, offset: i32) Id {
        return b.put(.{ .half = b.i(offset) });
    }
    fn f(b: *Builder, value: Id) Id {
        return b.put(.{ .to_float = value });
    }
    fn signedByte(b: *Builder, offset: Id) Id {
        const v = b.byte(offset);
        return b.select(b.lt(v, 128), v, b.sub(v, b.i(256)));
    }
    fn scaled(b: *Builder, scale: Id, value: Id) Id {
        return b.mul(scale, b.f(value));
    }
    fn nibble(b: *Builder, byte_value: Id, high: Id) Id {
        return b.select(high, b.shr(byte_value, 4), b.mask(byte_value, 15));
    }

    fn scaleMin(b: *Builder, group_index: Id, minimum: bool, base: i32) Id {
        // Select safe addresses before loading: eager expression evaluation
        // never reads the unselected branch outside the block header.
        const low_group = b.lt(group_index, 4);
        const offset = b.select(low_group, b.add(group_index, b.i(if (minimum) base + 4 else base)), b.add(group_index, b.i(base + 4)));
        const packed_value = b.byte(offset);
        const low = if (minimum) b.shr(packed_value, 4) else b.mask(packed_value, 15);
        const high_offset = b.select(low_group, b.i(base), b.add(group_index, b.i(if (minimum) base else base - 4)));
        const high = b.shl(b.shr(b.byte(high_offset), 6), 4);
        const combined = b.pair(.bit_or, low, high);
        return b.select(low_group, b.mask(packed_value, 63), combined);
    }
};

pub fn forFormat(comptime format: quant.Format) Recipe {
    @setEvalBranchQuota(100000);
    var b = Builder{ .recipe = .{ .format = format } };
    const lane = b.put(.lane);
    const result = switch (format) {
        .q4_0, .q4_1, .q5_0, .q5_1 => blk: {
            const affine = format == .q4_1 or format == .q5_1;
            const five = format == .q5_0 or format == .q5_1;
            const header: i32 = (if (affine) @as(i32, 4) else 2) + (if (five) @as(i32, 4) else 0);
            const packed_value = b.byte(b.add(b.i(header), b.mask(lane, 15)));
            var q = b.select(b.lt(lane, 16), b.mask(packed_value, 15), b.shr(packed_value, 4));
            if (five) {
                const high = b.put(.{ .word = b.i(if (affine) 4 else 2) });
                q = b.pair(.bit_or, q, b.shl(b.mask(b.pair(.shift_right, high, lane), 1), 4));
            }
            if (!affine) q = b.sub(q, b.i(if (five) 16 else 8));
            const scaled = b.scaled(b.half(0), q);
            break :blk if (affine) b.add(scaled, b.half(2)) else scaled;
        },
        .q8_0, .q8_1, .q8_k => blk: {
            const scale = if (format == .q8_k) b.put(.{ .float = b.i(0) }) else b.half(0);
            break :blk b.scaled(scale, b.signedByte(b.add(lane, b.i(if (format == .q8_0) 2 else 4))));
        },
        .q2_k, .q3_k => blk: {
            const sub = b.shr(lane, 4);
            const chunk = b.shr(sub, 3);
            const group = b.shr(b.mask(sub, 7), 1);
            const l = b.add(b.shl(b.mask(sub, 1), 4), b.mask(lane, 15));
            const offset = b.add(b.shl(chunk, 5), l);
            const packed_value = b.byte(b.add(offset, b.i(if (format == .q2_k) 20 else 32)));
            const q = b.mask(b.pair(.shift_right, packed_value, b.shl(group, 1)), 3);
            if (format == .q2_k) {
                const scale = b.byte(sub);
                break :blk b.sub(b.scaled(b.scaled(b.half(16), b.mask(scale, 15)), q), b.scaled(b.half(18), b.shr(scale, 4)));
            }
            const high = b.mask(b.pair(.shift_right, b.byte(l), b.add(b.shl(chunk, 2), group)), 1);
            const value = b.sub(b.add(q, b.shl(high, 2)), b.i(4));
            const low_index = b.add(b.mask(sub, 3), b.select(b.lt(b.mask(sub, 7), 4), b.i(96), b.i(100)));
            const low_packed = b.byte(low_index);
            const low = b.select(b.lt(sub, 8), b.mask(low_packed, 15), b.shr(low_packed, 4));
            const high_packed = b.byte(b.add(b.i(104), b.mask(sub, 3)));
            const high_scale = b.mask(b.pair(.shift_right, high_packed, b.shl(b.shr(sub, 2), 1)), 3);
            const scale = b.sub(b.pair(.bit_or, low, b.shl(high_scale, 4)), b.i(32));
            break :blk b.scaled(b.scaled(b.half(108), scale), value);
        },
        .q4_k, .q5_k => blk: {
            const sub = b.shr(lane, 5);
            const i = b.mask(lane, 31);
            const offset = b.add(b.mul(b.shr(sub, 1), b.i(32)), i);
            const packed_value = b.byte(b.add(b.i(if (format == .q4_k) 16 else 48), offset));
            var q = b.nibble(packed_value, b.mask(sub, 1));
            if (format == .q5_k) q = b.add(q, b.shl(b.mask(b.pair(.shift_right, b.byte(b.add(b.i(16), i)), sub), 1), 4));
            break :blk b.sub(b.scaled(b.scaled(b.half(0), b.scaleMin(sub, false, 4)), q), b.scaled(b.half(2), b.scaleMin(sub, true, 4)));
        },
        .q6_k => blk: {
            const sub = b.shr(lane, 4);
            const half_index = b.shr(sub, 3);
            const group = b.shr(b.mask(sub, 7), 1);
            const l = b.add(b.shl(b.mask(sub, 1), 4), b.mask(lane, 15));
            const low_offset = b.add(b.add(b.mul(half_index, b.i(64)), b.mul(b.mask(group, 1), b.i(32))), l);
            const high_offset = b.add(b.add(b.i(128), b.mul(half_index, b.i(32))), l);
            const low = b.mask(b.pair(.shift_right, b.byte(low_offset), b.mul(b.shr(group, 1), b.i(4))), 15);
            const high = b.mask(b.pair(.shift_right, b.byte(high_offset), b.mul(group, b.i(2))), 3);
            const q = b.sub(b.pair(.bit_or, low, b.shl(high, 4)), b.i(32));
            const scale = b.scaled(b.half(208), b.signedByte(b.add(b.i(192), sub)));
            const value = b.f(q);
            b.recipe.scale_component = scale;
            b.recipe.value_component = value;
            break :blk b.mul(scale, value);
        },
        else => @compileError("No packed block recipe for " ++ @tagName(format)),
    };
    b.recipe.result = result;
    return b.recipe;
}

/// Compile-time lane dependence gives headers scalar values and lane arithmetic
/// SIMD values. The expression graph disappears during compilation.
fn varying(comptime recipe: Recipe) [recipe.count]bool {
    var result: [recipe.count]bool = undefined;
    for (recipe.nodes[0..recipe.count], 0..) |node, index| {
        result[index] = switch (node) {
            .lane => true,
            .integer => false,
            .byte, .half, .float, .word, .to_float => |p| result[p],
            .add, .subtract, .multiply, .bit_and, .bit_or, .shift_left, .shift_right, .less => |p| result[p.a] or result[p.b],
            .choose => |c| result[c.condition] or result[c.yes] or result[c.no],
        };
    }
    return result;
}
fn valueType(comptime recipe: Recipe, comptime id: Id, comptime width: usize) type {
    const scalar = if (recipe.isFloat(id)) f32 else i32;
    return if (width > 1 and varying(recipe)[id]) @Vector(width, scalar) else scalar;
}
inline fn widen(comptime T: type, value: anytype) T {
    return if (@typeInfo(T) == .vector and @typeInfo(@TypeOf(value)) != .vector) @splat(value) else value;
}
inline fn load(comptime tag: std.meta.Tag(Expression), block: []const u8, offset: i32) (if (tag == .half or tag == .float) f32 else i32) {
    const bytes = block[@intCast(offset)..];
    return switch (tag) {
        .byte => bytes[0],
        .half => @as(f32, @as(f16, @bitCast(std.mem.readInt(u16, bytes[0..2], .little)))),
        .float => @bitCast(std.mem.readInt(u32, bytes[0..4], .little)),
        .word => @bitCast(std.mem.readInt(u32, bytes[0..4], .little)),
        else => unreachable,
    };
}
/// Scalar oracle/lowering; production block decoding uses explicit SIMD groups.
pub inline fn evaluate(comptime recipe: Recipe, block: []const u8, lane: usize) f32 {
    return evaluateLanes(recipe, 1, block, lane);
}
pub inline fn evaluateLanes(comptime recipe: Recipe, comptime width: usize, block: []const u8, first_lane: usize) (if (width == 1) f32 else @Vector(width, f32)) {
    @setEvalBranchQuota(1000000);
    std.debug.assert(block.len >= recipe.blockBytes() and first_lane + width <= recipe.blockValues());
    const types = comptime blk: {
        var types: [recipe.count]type = undefined;
        for (&types, 0..) |*t, index| t.* = valueType(recipe, @intCast(index), width);
        break :blk types;
    };
    var values: @Tuple(&types) = undefined;
    inline for (recipe.nodes[0..recipe.count], 0..) |node, index| {
        const T = types[index];
        const vector = @typeInfo(T) == .vector;
        values[index] = switch (node) {
            .lane => blk: {
                if (!vector) break :blk @intCast(first_lane);
                var lanes: T = undefined;
                inline for (0..width) |i| lanes[i] = @intCast(first_lane + i);
                break :blk lanes;
            },
            .integer => |v| v,
            .byte, .half, .float, .word => |p| blk: {
                if (!vector) break :blk load(node, block, values[p]);
                var loaded: T = undefined;
                inline for (0..width) |i| loaded[i] = load(node, block, values[p][i]);
                break :blk loaded;
            },
            .to_float => |p| @floatFromInt(values[p]),
            .add => |p| widen(T, values[p.a]) + widen(T, values[p.b]),
            .subtract => |p| widen(T, values[p.a]) - widen(T, values[p.b]),
            .multiply => |p| widen(T, values[p.a]) * widen(T, values[p.b]),
            .bit_and => |p| widen(T, values[p.a]) & widen(T, values[p.b]),
            .bit_or => |p| widen(T, values[p.a]) | widen(T, values[p.b]),
            .shift_left, .shift_right => |p| blk: {
                const U = if (vector) @Vector(width, u32) else u32;
                const S = if (vector) @Vector(width, u5) else u5;
                const a: U = @bitCast(widen(T, values[p.a]));
                const shift: S = @intCast(widen(T, values[p.b]));
                break :blk @bitCast(if (node == .shift_left) a << shift else a >> shift);
            },
            .less => |p| if (vector) @select(i32, widen(T, values[p.a]) < widen(T, values[p.b]), @as(T, @splat(1)), @as(T, @splat(0))) else @intFromBool(values[p.a] < values[p.b]),
            .choose => |c| if (vector) @select(if (recipe.isFloat(@intCast(index))) f32 else i32, widen(@Vector(width, i32), values[c.condition]) != @as(@Vector(width, i32), @splat(0)), widen(T, values[c.yes]), widen(T, values[c.no])) else if (values[c.condition] != 0) values[c.yes] else values[c.no],
        };
    }
    return widen(if (width == 1) f32 else @Vector(width, f32), values[recipe.result]);
}
/// The CPU schedule is independent of format semantics. Eight lanes match the
/// native quantized matmul width; partial tails preserve the public codec API.
pub fn decode(comptime format: quant.Format, data: []const u8, output: []f32) !void {
    const recipe = comptime forFormat(format);
    const bytes = comptime recipe.blockBytes();
    const count = comptime recipe.blockValues();
    if (data.len % bytes != 0) return error.InvalidQuantizedDataSize;
    if (output.len > data.len / bytes * count) return error.InvalidQuantizedOutputSize;
    var start: usize = 0;
    while (start < output.len) : (start += count) {
        const block = data[start / count * bytes ..][0..bytes];
        const end = @min(count, output.len - start);
        if (end == count) {
            inline for (0..count / 8) |group| output[start + group * 8 ..][0..8].* = evaluateLanes(recipe, 8, block, group * 8);
            continue;
        }
        var lane: usize = 0;
        while (lane + 8 <= end) : (lane += 8) output[start + lane ..][0..8].* = evaluateLanes(recipe, 8, block, lane);
        while (lane < end) : (lane += 1) output[start + lane] = evaluate(recipe, block, lane);
    }
}

pub const cuda_half_source =
    \\static __device__ __forceinline__ float antfly_half_le_to_float(const uint8_t *p) {
    \\    const uint16_t bits = (uint16_t)p[0] | ((uint16_t)p[1] << 8);
    \\    return __half2float(__ushort_as_half(bits));
    \\}
;
pub const cuda_word_source =
    \\static __device__ __forceinline__ unsigned antfly_recipe_u32_le(const unsigned char *p) {
    \\    return unsigned(p[0]) | (unsigned(p[1]) << 8) | (unsigned(p[2]) << 16) | (unsigned(p[3]) << 24);
    \\}
;
pub const cuda_float_source =
    \\static __device__ __forceinline__ float antfly_recipe_float_le(const unsigned char *p) {
    \\    return __uint_as_float(antfly_recipe_u32_le(p));
    \\}
;

pub const Target = enum { metal, cuda };
/// Emit the same expression graph into each target's existing source vocabulary.
/// The caller owns headers and schedule selection. No runtime interpreter is used.
pub fn render(allocator: std.mem.Allocator, recipe: Recipe, target: Target, name: []const u8) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(allocator);
    errdefer out.deinit();
    const w = &out.writer;
    try w.print("{s} float {s}({s}const {s} *block, int lane) {{\n", .{ if (target == .metal) "inline" else "__device__ __forceinline__", name, if (target == .metal) "device " else "", if (target == .metal) "uchar" else "unsigned char" });
    for (recipe.nodes[0..recipe.count], 0..) |node, index| {
        try w.print("    {s} v{d} = ", .{ if (recipe.isFloat(@intCast(index))) "float" else "int", index });
        switch (node) {
            .lane => try w.writeAll("lane"),
            .integer => |v| try w.print("{d}", .{v}),
            .byte => |p| try w.print("int(block[v{d}])", .{p}),
            .half => |p| try w.print("{s}(block + v{d})", .{ if (target == .metal) "antfly_qk_half_le_to_float" else "antfly_half_le_to_float", p }),
            .float => |p| try w.print("{s}(block + v{d})", .{ if (target == .metal) "antfly_qk_f32_le_to_float" else "antfly_recipe_float_le", p }),
            .word => |p| try w.print("int({s}(block + v{d}))", .{ if (target == .metal) "antfly_qk_u32_le" else "antfly_recipe_u32_le", p }),
            .to_float => |p| try w.print("float(v{d})", .{p}),
            .choose => |c| try w.print("v{d} ? v{d} : v{d}", .{ c.condition, c.yes, c.no }),
            .shift_left, .shift_right => |p| try w.print("int({s}(v{d}) {s} v{d})", .{ if (target == .metal) "uint" else "unsigned", p.a, if (node == .shift_left) "<<" else ">>", p.b }),
            .add, .subtract, .multiply, .bit_and, .bit_or, .less => |p| try w.print("v{d} {s} v{d}", .{ p.a, switch (node) {
                .add => "+",
                .subtract => "-",
                .multiply => "*",
                .bit_and => "&",
                .bit_or => "|",
                .less => "<",
                else => unreachable,
            }, p.b }),
        }
        try w.writeAll(";\n");
    }
    try w.print("    return v{d};\n}}", .{recipe.result});
    return out.toOwnedSlice();
}

pub const formats = [_]quant.Format{ .q4_0, .q4_1, .q5_0, .q5_1, .q8_0, .q8_1, .q8_k, .q2_k, .q3_k, .q4_k, .q5_k, .q6_k };

test "shared quant recipes match every lane of independent GGUF block decoding" {
    const reference = @import("../gguf/testing/quant_reference.zig");
    inline for (formats) |format| {
        const recipe = comptime forFormat(format);
        var raw: [recipe.blockBytes()]u8 = undefined;
        var expected: [recipe.blockValues()]f32 = undefined;
        for (0..64) |trial| {
            @import("testing/quant_blocks.zig").fill(format, trial, &raw);
            try reference.decode(format, &raw, &expected);
            for (expected, 0..) |value, lane| try std.testing.expectEqual(value, evaluate(recipe, &raw, lane));
            var decoded: [recipe.blockValues()]f32 = undefined;
            try decode(format, &raw, &decoded);
            try std.testing.expectEqualSlices(f32, &expected, &decoded);
            // A partial last block exercises SIMD groups and the scalar tail.
            for ([_]usize{ 0, 1, 7, 8, 17, recipe.blockValues() - 1 }) |count| {
                try decode(format, &raw, decoded[0..count]);
                try std.testing.expectEqualSlices(f32, expected[0..count], decoded[0..count]);
            }
        }
        for ([_]Target{ .metal, .cuda }) |target| {
            const source = try render(std.testing.allocator, recipe, target, "recipe_test");
            defer std.testing.allocator.free(source);
            try std.testing.expect(std.mem.indexOf(u8, source, "return v") != null);
        }
    }
}

/// Shared header extraction for schedules which hoist scales outside lane loops.
pub fn scaleMinHeader(comptime minimum: bool) Recipe {
    var b = Builder{ .recipe = .{ .format = .q4_k } };
    const sub_index = b.put(.lane);
    b.recipe.result = b.f(b.scaleMin(sub_index, minimum, 0));
    return b.recipe;
}
