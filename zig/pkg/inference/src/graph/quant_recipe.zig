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
    signed_byte: Id,
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
    /// Header components expose their real input/view extent. Whole-block
    /// recipes retain the format geometry; no padded temporary block is needed.
    input_geometry: ?struct { bytes: u16, lanes: u16 } = null,
    nodes: [128]Expression = undefined,
    count: u16 = 0,
    result: Id = 0,
    scale_component: ?Id = null,
    value_component: ?Id = null,
    minimum_component: ?Id = null,
    integer_component: ?Id = null,

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
                .byte, .signed_byte, .half, .float, .word, .to_float => |p| live[p] = true,
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
        var reduced = Recipe{ .format = self.format, .input_geometry = self.input_geometry };
        var ids: [128]Id = undefined;
        for (self.nodes[0..self.count], 0..) |node, index| {
            if (!live[index]) continue;
            ids[index] = reduced.count;
            reduced.nodes[reduced.count] = remap(node, &ids);
            reduced.count += 1;
        }
        reduced.result = ids[result];
        return reduced;
    }

    /// Intern identical expressions without reassociating floating arithmetic.
    /// GPU compilers previously had to rediscover these duplicate constants,
    /// loads and address expressions in every generated kernel. Components keep
    /// their own roots so schedules can still hoist scales independently.
    pub fn compact(self: Recipe) Recipe {
        var reduced = Recipe{ .format = self.format, .input_geometry = self.input_geometry };
        var ids: [128]Id = undefined;
        for (self.nodes[0..self.count], 0..) |node, index| {
            const mapped = remap(node, &ids);
            var found: ?Id = null;
            for (reduced.nodes[0..reduced.count], 0..) |existing, candidate| {
                if (std.meta.eql(existing, mapped)) {
                    found = @intCast(candidate);
                    break;
                }
            }
            ids[index] = found orelse blk: {
                const id = reduced.count;
                reduced.nodes[id] = mapped;
                reduced.count += 1;
                break :blk id;
            };
        }
        reduced.result = ids[self.result];
        if (self.scale_component) |id| reduced.scale_component = ids[id];
        if (self.value_component) |id| reduced.value_component = ids[id];
        if (self.minimum_component) |id| reduced.minimum_component = ids[id];
        if (self.integer_component) |id| reduced.integer_component = ids[id];
        return reduced;
    }

    fn remap(node: Expression, ids: *const [128]Id) Expression {
        return switch (node) {
            inline .byte, .signed_byte, .half, .float, .word, .to_float => |p, tag| @unionInit(Expression, @tagName(tag), ids[p]),
            inline .add, .subtract, .multiply, .bit_and, .bit_or, .shift_left, .shift_right, .less => |p, tag| @unionInit(Expression, @tagName(tag), Pair{ .a = ids[p.a], .b = ids[p.b] }),
            .choose => |c| .{ .choose = .{ .condition = ids[c.condition], .yes = ids[c.yes], .no = ids[c.no] } },
            else => node,
        };
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
        return if (self.input_geometry) |g| g.bytes else tensor_types.bytesPerBlock(self.tensorType()).?;
    }
    pub fn blockValues(self: Recipe) usize {
        return if (self.input_geometry) |g| g.lanes else tensor_types.valuesPerBlock(self.tensorType()).?;
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
        return b.put(.{ .signed_byte = offset });
    }
    fn q6Scale(b: *Builder, sub_index: Id) Id {
        return b.scaled(b.half(208), b.signedByte(b.add(b.i(192), sub_index)));
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
            const scale = b.half(0);
            b.recipe.scale_component = scale;
            b.recipe.integer_component = q;
            b.recipe.value_component = b.f(q);
            const scaled = b.mul(scale, b.recipe.value_component.?);
            break :blk if (affine) b.add(scaled, b.half(2)) else scaled;
        },
        .q8_0, .q8_1, .q8_k => blk: {
            const scale = if (format == .q8_k) b.put(.{ .float = b.i(0) }) else b.half(0);
            const q = b.signedByte(b.add(lane, b.i(if (format == .q8_0) 2 else 4)));
            b.recipe.scale_component = scale;
            b.recipe.integer_component = q;
            b.recipe.value_component = b.f(q);
            break :blk b.mul(scale, b.recipe.value_component.?);
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
                const packed_scale = b.byte(sub);
                const scale = b.scaled(b.half(16), b.mask(packed_scale, 15));
                const value = b.f(q);
                const scaled = b.mul(scale, value);
                const minimum = b.scaled(b.half(18), b.shr(packed_scale, 4));
                b.recipe.scale_component = scale;
                b.recipe.minimum_component = minimum;
                b.recipe.integer_component = q;
                b.recipe.value_component = value;
                break :blk b.sub(scaled, minimum);
            }
            const high = b.mask(b.pair(.shift_right, b.byte(l), b.add(b.shl(chunk, 2), group)), 1);
            const value = b.sub(b.add(q, b.shl(high, 2)), b.i(4));
            const low_index = b.add(b.mask(sub, 3), b.select(b.lt(b.mask(sub, 7), 4), b.i(96), b.i(100)));
            const low_packed = b.byte(low_index);
            const low = b.select(b.lt(sub, 8), b.mask(low_packed, 15), b.shr(low_packed, 4));
            const high_packed = b.byte(b.add(b.i(104), b.mask(sub, 3)));
            const high_scale = b.mask(b.pair(.shift_right, high_packed, b.shl(b.shr(sub, 2), 1)), 3);
            const scale = b.sub(b.pair(.bit_or, low, b.shl(high_scale, 4)), b.i(32));
            const block_scale = b.scaled(b.half(108), scale);
            b.recipe.scale_component = block_scale;
            b.recipe.integer_component = value;
            b.recipe.value_component = b.f(value);
            break :blk b.mul(block_scale, b.recipe.value_component.?);
        },
        .q4_k, .q5_k => blk: {
            const sub = b.shr(lane, 5);
            const i = b.mask(lane, 31);
            const offset = b.add(b.mul(b.shr(sub, 1), b.i(32)), i);
            const packed_value = b.byte(b.add(b.i(if (format == .q4_k) 16 else 48), offset));
            var q = b.nibble(packed_value, b.mask(sub, 1));
            if (format == .q5_k) q = b.add(q, b.shl(b.mask(b.pair(.shift_right, b.byte(b.add(b.i(16), i)), sub), 1), 4));
            const scale = b.scaled(b.half(0), b.scaleMin(sub, false, 4));
            const value = b.f(q);
            const scaled = b.mul(scale, value);
            const minimum = b.scaled(b.half(2), b.scaleMin(sub, true, 4));
            b.recipe.scale_component = scale;
            b.recipe.minimum_component = minimum;
            b.recipe.integer_component = q;
            b.recipe.value_component = value;
            break :blk b.sub(scaled, minimum);
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
            const scale = b.q6Scale(sub);
            const value = b.f(q);
            b.recipe.scale_component = scale;
            b.recipe.value_component = value;
            b.recipe.integer_component = q;
            break :blk b.mul(scale, value);
        },
        else => @compileError("No packed block recipe for " ++ @tagName(format)),
    };
    b.recipe.result = result;
    return b.recipe;
}

/// Compile-time lane dependence gives headers scalar values and lane arithmetic
/// SIMD values. The expression graph disappears during compilation.
fn varying(comptime recipe: Recipe, comptime group_width: usize) [recipe.count]bool {
    var result: [recipe.count]bool = undefined;
    for (recipe.nodes[0..recipe.count], 0..) |node, index| {
        result[index] = switch (node) {
            .lane => true,
            .integer => false,
            .byte, .signed_byte, .half, .float, .word, .to_float => |p| result[p],
            .shift_right => |p| if (group_width > 1 and recipe.nodes[p.a] == .lane and recipe.nodes[p.b] == .integer and
                recipe.nodes[p.b].integer >= 0 and recipe.nodes[p.b].integer < 31 and
                (@as(u32, 1) << @intCast(recipe.nodes[p.b].integer)) >= group_width) false else result[p.a] or result[p.b],
            .add, .subtract, .multiply, .bit_and, .bit_or, .shift_left, .less => |p| result[p.a] or result[p.b],
            .choose => |c| result[c.condition] or result[c.yes] or result[c.no],
        };
    }
    return result;
}
// Conservative unsigned bounds select byte-width SIMD arithmetic only when it
// is equivalent to the recipe's logical 32-bit operations. Unknown/signed
// values stay wide. This analysis never changes scalar or device lowering.
fn unsignedMaxima(comptime recipe: Recipe) [recipe.count]?u32 {
    var maxima: [recipe.count]?u32 = @splat(null);
    for (recipe.nodes[0..recipe.count], 0..) |node, index| {
        maxima[index] = switch (node) {
            .lane => @intCast(recipe.blockValues() - 1),
            .integer => |v| if (v >= 0) @intCast(v) else null,
            .byte => 255,
            .less => 1,
            .add, .multiply => |p| if (maxima[p.a] != null and maxima[p.b] != null) blk: {
                const a: u64 = maxima[p.a].?;
                const b: u64 = maxima[p.b].?;
                const maximum = if (node == .add) a + b else a * b;
                break :blk if (maximum <= std.math.maxInt(i32)) @intCast(maximum) else null;
            } else null,
            .bit_and => |p| if (maxima[p.a]) |a| (if (maxima[p.b]) |b| @min(a, b) else a) else maxima[p.b],
            .bit_or => |p| if (maxima[p.a] != null and maxima[p.b] != null) blk: {
                const maximum = @max(maxima[p.a].?, maxima[p.b].?);
                break :blk if (maximum == 0) 0 else std.math.maxInt(u32) >> @intCast(@clz(maximum));
            } else null,
            .shift_right => |p| if (maxima[p.a]) |a| (if (recipe.nodes[p.b] == .integer and recipe.nodes[p.b].integer >= 0 and recipe.nodes[p.b].integer < 32) a >> @intCast(recipe.nodes[p.b].integer) else a) else null,
            .shift_left => |p| if (maxima[p.a] != null and maxima[p.b] != null and maxima[p.b].? < 32) blk: {
                const maximum = @as(u64, maxima[p.a].?) << @intCast(maxima[p.b].?);
                break :blk if (maximum <= std.math.maxInt(i32)) @intCast(maximum) else null;
            } else null,
            .choose => |c| if (maxima[c.yes] != null and maxima[c.no] != null) @max(maxima[c.yes].?, maxima[c.no].?) else null,
            else => null,
        };
    }
    return maxima;
}
fn byteLanes(comptime recipe: Recipe, comptime id: Id) bool {
    const maxima = unsignedMaxima(recipe);
    if (maxima[id] == null or maxima[id].? > 255) return false;
    return switch (recipe.nodes[id]) {
        .byte, .add, .multiply, .bit_and, .bit_or, .choose => true,
        .shift_left, .shift_right => |p| maxima[p.a] != null and maxima[p.a].? <= 255 and maxima[p.b] != null and maxima[p.b].? <= 7,
        else => false,
    };
}

fn valueType(comptime recipe: Recipe, comptime id: Id, comptime width: usize, comptime group_width: usize) type {
    const scalar = if (recipe.isFloat(id)) f32 else i32;
    return if (width > 1 and varying(recipe, group_width)[id])
        @Vector(width, if (group_width > 1 and !recipe.isFloat(id) and byteLanes(recipe, id)) u8 else scalar)
    else
        scalar;
}
inline fn coerceLanes(comptime T: type, value: anytype) T {
    if (@typeInfo(T) == .vector and @typeInfo(@TypeOf(value)) != .vector) {
        if (@typeInfo(T).vector.child == u8 and @TypeOf(value) == i32) return @splat(@as(u8, @truncate(@as(u32, @bitCast(value)))));
        return @splat(value);
    }
    // Alignment analysis proves these lanes equal before narrowing a value.
    if (@typeInfo(T) != .vector and @typeInfo(@TypeOf(value)) == .vector) return value[0];
    if (@typeInfo(T) == .vector and @typeInfo(@TypeOf(value)) == .vector and
        @typeInfo(T).vector.child == u8 and @typeInfo(@TypeOf(value)).vector.child == i32)
    {
        // Narrowing operands of a proven byte expression preserves their low
        // bits (notably a signed word masked to a byte). Wider results retain
        // ordinary signed extension through the return coercion.
        return @truncate(@as(@Vector(@typeInfo(T).vector.len, u32), @bitCast(value)));
    }
    return value;
}
inline fn load(comptime tag: std.meta.Tag(Expression), block: []const u8, offset: i32) (if (tag == .half or tag == .float) f32 else i32) {
    const bytes = block[@intCast(offset)..];
    return switch (tag) {
        .byte => bytes[0],
        .signed_byte => @as(i8, @bitCast(bytes[0])),
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
    const value = evaluateValue(recipe, width, 1, 0, block, .{ .first = first_lane });
    return if (comptime recipe.isFloat(recipe.result)) value else @floatFromInt(value);
}
/// Aligned SIMD groups keep subblock headers scalar and packed bytes contiguous.
/// Callers supply a power-of-two width and a first lane divisible by that width.
pub inline fn evaluateAlignedLanes(comptime recipe: Recipe, comptime width: usize, block: []const u8, first_lane: usize) @Vector(width, f32) {
    comptime {
        if (width < 2 or !std.math.isPowerOfTwo(width)) @compileError("aligned SIMD width must be a power of two");
    }
    std.debug.assert(first_lane % width == 0);
    const value = evaluateValue(recipe, width, width, 0, block, .{ .first = first_lane });
    return if (comptime recipe.isFloat(recipe.result)) value else @floatFromInt(value);
}

/// Retain a native schedule's subblock/offset induction variables. Rebuilding
/// them from a flattened, narrowed lane obscures address strength reduction.
pub inline fn evaluateSubblockLanes(comptime recipe: Recipe, comptime width: usize, comptime group: usize, block: []const u8, subblock: usize, offset: usize) @Vector(width, f32) {
    const value = evaluateSubblockValue(recipe, width, group, block, subblock, offset);
    return if (comptime recipe.isFloat(recipe.result)) value else @floatFromInt(value);
}
/// Integer reductions must not round-trip packed components through F32.
pub inline fn evaluateSubblockIntegers(comptime recipe: Recipe, comptime width: usize, comptime group: usize, block: []const u8, subblock: usize, offset: usize) @Vector(width, i32) {
    if (comptime recipe.isFloat(recipe.result)) @compileError("integer lowering requires an integer recipe component");
    return evaluateSubblockValue(recipe, width, group, block, subblock, offset);
}
inline fn evaluateSubblockValue(comptime recipe: Recipe, comptime width: usize, comptime group: usize, block: []const u8, subblock: usize, offset: usize) @Vector(width, if (recipe.isFloat(recipe.result)) f32 else i32) {
    comptime {
        if (width < 2 or !std.math.isPowerOfTwo(group) or group % width != 0) @compileError("subblock SIMD width must divide a power-of-two group");
    }
    std.debug.assert(offset % width == 0 and offset + width <= group);
    return evaluateValue(recipe, width, width, group, block, .{ .subblock = subblock, .offset = offset });
}

/// Integer components feed packed CPU schedules directly. The recipe is fully
/// unrolled at compile time; no float round trip or runtime interpreter exists.
pub inline fn evaluateInteger(comptime recipe: Recipe, block: []const u8, lane: usize) i32 {
    if (comptime recipe.isFloat(recipe.result)) @compileError("integer lowering requires an integer recipe component");
    return evaluateValue(recipe, 1, 1, 0, block, .{ .first = lane });
}
// A contiguous byte component must stay a vector load in runtime SIMD loops.
// Scalar gathers obscure this from LLVM on some native targets.
fn consecutiveLanes(comptime recipe: Recipe, comptime id: Id, comptime group_width: usize) bool {
    return switch (recipe.nodes[id]) {
        .lane => true,
        .add => |p| (consecutiveLanes(recipe, p.a, group_width) and !varying(recipe, group_width)[p.b]) or
            (!varying(recipe, group_width)[p.a] and consecutiveLanes(recipe, p.b, group_width)),
        .subtract => |p| consecutiveLanes(recipe, p.a, group_width) and !varying(recipe, group_width)[p.b],
        .bit_and => |p| group_width > 1 and recipe.nodes[p.a] == .lane and recipe.nodes[p.b] == .integer and
            recipe.nodes[p.b].integer >= 0 and (@as(u32, @intCast(recipe.nodes[p.b].integer)) & (group_width - 1)) == group_width - 1,
        else => false,
    };
}

const LanePosition = struct { first: usize = 0, subblock: usize = 0, offset: usize = 0 };
inline fn evaluateValue(comptime recipe: Recipe, comptime width: usize, comptime group_width: usize, comptime subblock_size: usize, block: []const u8, position: LanePosition) (if (width == 1) (if (recipe.isFloat(recipe.result)) f32 else i32) else @Vector(width, if (recipe.isFloat(recipe.result)) f32 else i32)) {
    @setEvalBranchQuota(1000000);
    const first_lane = if (subblock_size != 0) position.subblock * subblock_size + position.offset else position.first;
    std.debug.assert(block.len >= recipe.blockBytes() and first_lane + width <= recipe.blockValues());
    const types = comptime blk: {
        var types: [recipe.count]type = undefined;
        for (&types, 0..) |*t, index| t.* = valueType(recipe, @intCast(index), width, group_width);
        break :blk types;
    };
    var values: @Tuple(&types) = undefined;
    inline for (recipe.nodes[0..recipe.count], 0..) |node, index| {
        const T = types[index];
        const vector = @typeInfo(T) == .vector;
        if (comptime subblock_size != 0 and (node == .shift_right or node == .bit_and)) {
            const pair = if (node == .shift_right) node.shift_right else node.bit_and;
            if (comptime recipe.nodes[pair.a] == .lane and recipe.nodes[pair.b] == .integer) {
                const constant = comptime recipe.nodes[pair.b].integer;
                if (comptime node == .shift_right and constant >= std.math.log2_int(usize, subblock_size)) {
                    values[index] = @intCast(position.subblock >> @intCast(constant - std.math.log2_int(usize, subblock_size)));
                    continue;
                }
                if (comptime node == .bit_and and constant == subblock_size - 1) {
                    inline for (0..width) |i| values[index][i] = @intCast(position.offset + i);
                    continue;
                }
            }
        }
        values[index] = switch (node) {
            .lane => blk: {
                if (!vector) break :blk @intCast(first_lane);
                var lanes: T = undefined;
                inline for (0..width) |i| lanes[i] = @intCast(first_lane + i);
                break :blk lanes;
            },
            .integer => |v| v,
            .byte, .signed_byte, .half, .float, .word => |p| blk: {
                if (!vector) break :blk load(node, block, values[p]);
                if (comptime (node == .byte or node == .signed_byte) and consecutiveLanes(recipe, p, group_width)) {
                    const raw: @Vector(width, u8) = block[@intCast(values[p][0])..][0..width].*;
                    break :blk if (node == .signed_byte) @as(@Vector(width, i8), @bitCast(raw)) else raw;
                }
                var loaded: T = undefined;
                inline for (0..width) |i| loaded[i] = if (comptime @typeInfo(T).vector.child == u8) @intCast(load(node, block, values[p][i])) else load(node, block, values[p][i]);
                break :blk loaded;
            },
            .to_float => |p| @floatFromInt(values[p]),
            .add => |p| coerceLanes(T, values[p.a]) + coerceLanes(T, values[p.b]),
            .subtract => |p| coerceLanes(T, values[p.a]) - coerceLanes(T, values[p.b]),
            .multiply => |p| coerceLanes(T, values[p.a]) * coerceLanes(T, values[p.b]),
            .bit_and => |p| coerceLanes(T, values[p.a]) & coerceLanes(T, values[p.b]),
            .bit_or => |p| coerceLanes(T, values[p.a]) | coerceLanes(T, values[p.b]),
            .shift_left, .shift_right => |p| blk: {
                const narrow = vector and @typeInfo(T).vector.child == u8;
                const U = if (vector) @Vector(width, if (narrow) u8 else u32) else u32;
                const S = if (vector) @Vector(width, if (narrow) u3 else u5) else u5;
                const a: U = @bitCast(coerceLanes(T, values[p.a]));
                const shift: S = @intCast(coerceLanes(T, values[p.b]));
                break :blk @bitCast(if (node == .shift_left) a << shift else a >> shift);
            },
            .less => |p| if (vector) @select(i32, coerceLanes(T, values[p.a]) < coerceLanes(T, values[p.b]), @as(T, @splat(1)), @as(T, @splat(0))) else @intFromBool(values[p.a] < values[p.b]),
            .choose => |c| if (vector) @select(@typeInfo(T).vector.child, coerceLanes(@Vector(width, i32), values[c.condition]) != @as(@Vector(width, i32), @splat(0)), coerceLanes(T, values[c.yes]), coerceLanes(T, values[c.no])) else if (values[c.condition] != 0) values[c.yes] else values[c.no],
        };
    }
    return coerceLanes(if (width == 1) (if (recipe.isFloat(recipe.result)) f32 else i32) else @Vector(width, if (recipe.isFloat(recipe.result)) f32 else i32), values[recipe.result]);
}
/// The CPU schedule is independent of format semantics. Eight lanes match the
/// native quantized matmul width; partial tails preserve the public codec API.
pub fn decode(comptime format: quant.Format, data: []const u8, output: []f32) !void {
    const recipe = comptime forFormat(format);
    const bytes = comptime recipe.blockBytes();
    const count = comptime recipe.blockValues();
    // Q5_1 retains the qualified wide-integer decode profile. Its prepared
    // byte staging is a distinct schedule and still benefits from byte lanes.
    const decode_group = comptime if (format == .q5_1) evaluateLanes else evaluateAlignedLanes;
    if (data.len % bytes != 0) return error.InvalidQuantizedDataSize;
    if (output.len > data.len / bytes * count) return error.InvalidQuantizedOutputSize;
    var start: usize = 0;
    while (start < output.len) : (start += count) {
        const block = data[start / count * bytes ..][0..bytes];
        const end = @min(count, output.len - start);
        if (end == count) {
            inline for (0..count / 8) |group| output[start + group * 8 ..][0..8].* = decode_group(recipe, 8, block, group * 8);
            continue;
        }
        var lane: usize = 0;
        while (lane + 8 <= end) : (lane += 8) output[start + lane ..][0..8].* = decode_group(recipe, 8, block, lane);
        while (lane < end) : (lane += 1) output[start + lane] = evaluate(recipe, block, lane);
    }
}

/// Native block-dot schedules lower the same packed semantics as device code.
/// Q4/Q5 visit the low half before the high half; centered formats scale only
/// after summation, while affine formats retain per-term scale and minimum.
/// Q8 keeps the native vector accumulator and deferred scale, with an unrolled
/// 32-value schedule and a runtime 256-value schedule to bound code size.
pub inline fn dotBlock(comptime format: quant.Format, comptime width: usize, input: []const f32, block: []const u8) f32 {
    const recipe = comptime forFormat(format);
    const count = comptime recipe.blockValues();
    comptime {
        if (width < 2 or 16 % width != 0) @compileError("block-dot SIMD width must divide sixteen");
    }
    std.debug.assert(input.len >= count);
    const scale = evaluate(comptime recipe.component(recipe.scale_component.?), block, 0);
    switch (comptime format) {
        .q4_0, .q4_1, .q5_0, .q5_1 => {
            var acc: f32 = 0;
            inline for (0..2) |half| {
                const value_recipe = comptime if (format == .q4_1 or format == .q5_1)
                    recipe
                else
                    recipe.component(recipe.integer_component.?);
                // Retain separate static compiler schedules: explicit Q4_0
                // unrolling avoids an expensive SLP transform; the other
                // formats retain native loop unrolling/register allocation.
                if (comptime format == .q4_0) {
                    inline for (0..16) |i| {
                        const lane = half * 16 + i;
                        acc += input[lane] * evaluate(value_recipe, block, lane);
                    }
                } else {
                    for (0..16) |i| {
                        const lane = half * 16 + i;
                        acc += input[lane] * evaluate(value_recipe, block, lane);
                    }
                }
            }
            return if (comptime format == .q4_1 or format == .q5_1) acc else scale * acc;
        },
        .q8_0, .q8_1, .q8_k => {
            const integer = comptime recipe.component(recipe.integer_component.?);
            const Vec = @Vector(width, f32);
            var acc: Vec = @splat(0);
            if (comptime format == .q8_k) {
                var i: usize = 0;
                while (i < count) : (i += width) {
                    const values = evaluateLanes(integer, width, block, i);
                    const inputs: Vec = input[i..][0..width].*;
                    acc += inputs * values;
                }
            } else {
                comptime var i: usize = 0;
                inline while (i < count) : (i += width) {
                    const values = evaluateLanes(integer, width, block, i);
                    const inputs: Vec = input[i..][0..width].*;
                    acc += inputs * values;
                }
            }
            return scale * @reduce(.Add, acc);
        },
        else => @compileError("block-dot schedule requires a legacy Q4/Q5 or signed-byte recipe"),
    }
}

/// Publish a recipe's integer component into the existing prepared byte ABI.
/// Signed centered values preserve their two's-complement byte representation.
/// Source and destination must be disjoint, as in the prepared-storage callers.
pub inline fn unpackIntegerBlock(comptime format: quant.Format, block: []const u8, output: []u8) void {
    const recipe = comptime forFormat(format);
    const integer = comptime recipe.component(recipe.integer_component.?);
    const group = if (format == .q4_k or format == .q5_k) 32 else 16;
    const halves = if (format == .q4_k) 2 else 1;
    std.debug.assert(output.len == recipe.blockValues());
    for (0..recipe.blockValues() / group / halves) |chunk| {
        inline for (0..halves) |half| {
            const sub = chunk * halves + half;
            inline for (0..group / 16) |part| {
                const values = evaluateSubblockIntegers(integer, 16, group, block, sub, part * 16);
                const bytes: @Vector(16, u8) = @truncate(@as(@Vector(16, u32), @bitCast(values)));
                output[sub * group + part * 16 ..][0..16].* = bytes;
            }
        }
    }
}

// Hoist a scale's native factor and integer code without another format decoder.
// Keeping the raw code separate preserves Q3's integer staging before centering.
const ScaleParts = struct { factor: Recipe, codes: Recipe, center: i32 };
fn splitScale(comptime recipe: Recipe) ScaleParts {
    const product = recipe.nodes[recipe.result].multiply;
    const code = recipe.nodes[product.b].to_float;
    const centered = recipe.nodes[code] == .subtract and recipe.nodes[recipe.nodes[code].subtract.b] == .integer;
    return .{
        .factor = recipe.component(product.a),
        .codes = recipe.component(if (centered) recipe.nodes[code].subtract.a else code),
        .center = if (centered) recipe.nodes[recipe.nodes[code].subtract.b].integer else 0,
    };
}

/// Serialize the existing K-quant prepared header: contiguous LE F32 scales,
/// followed by minima when present. Q3 publishes a zero minimum plane; Q6 has
/// scales only. Integer fields stage before F32 products, as in native writers.
pub inline fn writeScaleHeader(comptime format: quant.Format, block: []const u8, output: []u8) void {
    comptime {
        switch (format) {
            .q2_k, .q3_k, .q4_k, .q5_k, .q6_k => {},
            else => @compileError("prepared scale header requires a K-quant recipe"),
        }
    }
    const recipe = comptime forFormat(format);
    const group = if (format == .q4_k or format == .q5_k) 32 else 16;
    const count = 256 / group;
    const scale = comptime splitScale(recipe.component(recipe.scale_component.?));
    const minimum: ?ScaleParts = comptime if (recipe.minimum_component) |id| splitScale(recipe.component(id)) else null;
    const factor = evaluate(scale.factor, block, 0);
    const min_factor = if (comptime minimum) |m| evaluate(m.factor, block, 0) else 0;
    var codes: [count]i32 = undefined;
    var min_codes: [count]i32 = undefined;
    inline for (0..count) |sub| {
        codes[sub] = evaluateInteger(scale.codes, block, sub * group);
        if (comptime minimum) |m| min_codes[sub] = evaluateInteger(m.codes, block, sub * group);
    }
    std.debug.assert(output.len == count * 4 * (if (format == .q6_k) @as(usize, 1) else 2));
    for (0..count) |sub| {
        const value = factor * @as(f32, @floatFromInt(codes[sub] - scale.center));
        std.mem.writeInt(u32, output[sub * 4 ..][0..4], @bitCast(value), .little);
        if (comptime format != .q6_k) {
            const min_value = if (comptime minimum) |m| min_factor * @as(f32, @floatFromInt(min_codes[sub] - m.center)) else @as(f32, 0);
            std.mem.writeInt(u32, output[(count + sub) * 4 ..][0..4], @bitCast(min_value), .little);
        }
    }
}

/// K-quant native F32 dot: hoist each subblock's scale/minimum, retain the
/// original SIMD accumulator, and lower the packed integer component once.
pub inline fn dotKBlock(comptime format: quant.Format, comptime width: usize, input: []const f32, block: []const u8) f32 {
    comptime {
        switch (format) {
            .q2_k, .q3_k, .q4_k, .q5_k, .q6_k => {},
            else => @compileError("K-dot schedule requires a Q2_K through Q6_K recipe"),
        }
        if (width < 2 or 16 % width != 0) @compileError("K-dot SIMD width must divide sixteen");
    }
    const recipe = comptime forFormat(format);
    const integer = comptime recipe.component(recipe.integer_component.?);
    const group = if (comptime format == .q4_k or format == .q5_k) 32 else 16;
    const Vec = @Vector(width, f32);
    std.debug.assert(input.len >= 256);
    var acc: Vec = @splat(0);
    const scale_recipe = comptime recipe.component(recipe.scale_component.?);
    const scale_code = comptime if (format == .q3_k) splitScale(scale_recipe) else {};
    const factor = if (comptime format == .q3_k) evaluate(scale_code.factor, block, 0) else 0;
    var scales: [256 / group](if (format == .q3_k) i32 else f32) = undefined;
    var minima: [256 / group]f32 = undefined;
    if (comptime format == .q3_k or format == .q4_k) {
        // Match the native header-before-traversal schedule. In particular,
        // Q3's packed scale fields must not be decoded in the dot loop.
        inline for (0..256 / group) |sub| {
            scales[sub] = if (comptime format == .q3_k)
                evaluateInteger(scale_code.codes, block, sub * group)
            else
                evaluate(scale_recipe, block, sub * group);
            if (comptime recipe.minimum_component) |id| minima[sub] = evaluate(comptime recipe.component(id), block, sub * group);
        }
    }
    const halves = if (comptime format == .q4_k) 2 else 1;
    for (0..256 / group / halves) |chunk| {
        inline for (0..halves) |half| {
            const sub = chunk * halves + half;
            const scale: Vec = @splat(if (comptime format == .q3_k)
                factor * @as(f32, @floatFromInt(scales[sub] - scale_code.center))
            else if (comptime format == .q4_k)
                scales[sub]
            else
                evaluate(scale_recipe, block, sub * group));
            const minimum: Vec = if (comptime recipe.minimum_component) |id|
                @splat(if (comptime format == .q4_k) minima[sub] else evaluate(comptime recipe.component(id), block, sub * group))
            else
                @splat(0);
            if (comptime group == 16) {
                comptime var i: usize = 0;
                inline while (i < group) : (i += width) {
                    const q = evaluateSubblockLanes(integer, width, group, block, sub, i);
                    const values = if (comptime recipe.minimum_component != null) scale * q - minimum else scale * q;
                    const x: Vec = input[sub * group + i ..][0..width].*;
                    acc += x * values;
                }
            } else {
                var i: usize = 0;
                while (i < group) : (i += width) {
                    const q = evaluateSubblockLanes(integer, width, group, block, sub, i);
                    const values = scale * q - minimum;
                    const x: Vec = input[sub * group + i ..][0..width].*;
                    acc += x * values;
                }
            }
        }
    }
    return @reduce(.Add, acc);
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
pub fn render(allocator: std.mem.Allocator, raw_recipe: Recipe, target: Target, name: []const u8) ![]u8 {
    const recipe = raw_recipe.compact();
    var out: std.Io.Writer.Allocating = .init(allocator);
    errdefer out.deinit();
    const w = &out.writer;
    try w.print("{s} {s} {s}({s}const {s} *block, int lane) {{\n", .{ if (target == .metal) "inline" else "__device__ __forceinline__", if (recipe.isFloat(recipe.result)) "float" else "int", name, if (target == .metal) "device " else "", if (target == .metal) "uchar" else "unsigned char" });
    for (recipe.nodes[0..recipe.count], 0..) |node, index| {
        try w.print("    {s} v{d} = ", .{ if (recipe.isFloat(@intCast(index))) "float" else "int", index });
        switch (node) {
            .lane => try w.writeAll("lane"),
            .integer => |v| try w.print("{d}", .{v}),
            .byte => |p| try w.print("int(block[v{d}])", .{p}),
            .signed_byte => |p| try w.print("int((({s}const {s} *)block)[v{d}])", .{ if (target == .metal) "device " else "", if (target == .metal) "char" else "signed char", p }),
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
            // Aligned lowering may hoist only values uniform within the group.
            // Include groups spanning packed halves/subblocks, and retain an
            // unaligned call to protect the general gather semantics.
            inline for (.{ 2, 4, 8, 16, 32 }) |width| {
                var first: usize = 0;
                while (first < expected.len) : (first += width) {
                    const lanes = evaluateAlignedLanes(recipe, width, &raw, first);
                    try std.testing.expectEqualSlices(f32, expected[first..][0..width], &@as([width]f32, lanes));
                }
                const lanes = evaluateLanes(recipe, width, &raw, if (width < expected.len) 1 else 0);
                const first_unaligned: usize = if (width < expected.len) 1 else 0;
                try std.testing.expectEqualSlices(f32, expected[first_unaligned..][0..width], &@as([width]f32, lanes));
            }
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

test "native block-dot schedules agree with independent GGUF decoding and f64 dots" {
    const reference = @import("../gguf/testing/quant_reference.zig");
    inline for (formats) |format| {
        const recipe = comptime forFormat(format);
        var raw: [recipe.blockBytes()]u8 = undefined;
        var decoded: [recipe.blockValues()]f32 = undefined;
        var input: [recipe.blockValues()]f32 = undefined;
        for (0..128) |trial| {
            @import("testing/quant_blocks.zig").fill(format, trial, &raw);
            try reference.decode(format, &raw, &decoded);
            var expected: f64 = 0;
            var magnitude: f64 = 0;
            for (&input, decoded, 0..) |*x, weight, lane| {
                x.* = @as(f32, @floatFromInt(@as(i32, @intCast((lane * 79 + trial * 17) % 257)) - 128)) / 32;
                const product = @as(f64, x.*) * weight;
                expected += product;
                magnitude += @abs(product);
            }
            // Includes cancellation, signed/subnormal/zero scales and both
            // packed halves. Bound accumulated F32 error by the dot magnitude;
            // the independent oracle does not duplicate the native reduction.
            inline for (.{ 4, 8, 16 }) |width| {
                const actual = if (comptime format == .q2_k or format == .q3_k or format == .q4_k or format == .q5_k or format == .q6_k)
                    dotKBlock(format, width, &input, &raw)
                else
                    dotBlock(format, width, &input, &raw);
                try std.testing.expect(@abs(@as(f64, actual) - expected) <= 2e-6 * magnitude + 1e-12);
            }
        }
    }
}

test "prepared K headers and byte values reconstruct independent GGUF weights" {
    const reference = @import("../gguf/testing/quant_reference.zig");
    inline for (.{ quant.Format.q2_k, .q3_k, .q4_k, .q5_k, .q6_k }) |format| {
        const recipe = comptime forFormat(format);
        const group = if (format == .q4_k or format == .q5_k) 32 else 16;
        const count = 256 / group;
        const header_bytes = count * 4 * (if (format == .q6_k) @as(usize, 1) else 2);
        var raw: [recipe.blockBytes()]u8 = undefined;
        var header: [header_bytes]u8 = undefined;
        var values: [256]u8 = undefined;
        var expected: [256]f32 = undefined;
        for (0..128) |trial| {
            @import("testing/quant_blocks.zig").fill(format, trial, &raw);
            try reference.decode(format, &raw, &expected);
            writeScaleHeader(format, &raw, &header);
            unpackIntegerBlock(format, &raw, &values);
            for (expected, values, 0..) |weight, byte, lane| {
                const sub = lane / group;
                const scale: f32 = @bitCast(std.mem.readInt(u32, header[sub * 4 ..][0..4], .little));
                const q: i32 = if (format == .q3_k or format == .q6_k) @as(i8, @bitCast(byte)) else byte;
                const product = scale * @as(f32, @floatFromInt(q));
                const actual = if (format == .q6_k) product else product - @as(f32, @bitCast(std.mem.readInt(u32, header[(count + sub) * 4 ..][0..4], .little)));
                try std.testing.expectEqual(weight, actual);
            }
        }
    }
}

/// Shared header extraction for schedules which hoist scales outside lane loops.
pub fn scaleMinHeader(comptime minimum: bool) Recipe {
    var b = Builder{ .recipe = .{ .format = .q4_k, .input_geometry = .{ .bytes = 12, .lanes = 8 } } };
    const sub_index = b.put(.lane);
    b.recipe.result = b.f(b.scaleMin(sub_index, minimum, 0));
    return b.recipe;
}

/// Integer counterpart consumed by CUDA's tuned packed-dot schedules.
pub fn scaleMinHeaderInteger(comptime minimum: bool) Recipe {
    var b = Builder{ .recipe = .{ .format = .q4_k, .input_geometry = .{ .bytes = 12, .lanes = 8 } } };
    const sub_index = b.put(.lane);
    b.recipe.result = b.scaleMin(sub_index, minimum, 0);
    return b.recipe;
}

test "shared quant recipe compaction preserves every packed lane and hoistable component" {
    @setEvalBranchQuota(1000000);
    var original_nodes: usize = 0;
    var compact_nodes: usize = 0;
    inline for (formats) |format| {
        const original = comptime forFormat(format);
        const compacted = comptime original.compact();
        original_nodes += original.count;
        compact_nodes += compacted.count;
        try std.testing.expectEqual(compacted.count, compacted.compact().count);
        for (0..64) |trial| {
            var block: [original.blockBytes()]u8 = undefined;
            @import("testing/quant_blocks.zig").fill(format, trial, &block);
            for (0..original.blockValues()) |lane| {
                try std.testing.expectEqual(evaluate(original, &block, lane), evaluate(compacted, &block, lane));
                if (comptime original.scale_component != null) {
                    try std.testing.expectEqual(evaluate(comptime original.component(original.scale_component.?), &block, lane), evaluate(comptime compacted.component(compacted.scale_component.?), &block, lane));
                    try std.testing.expectEqual(evaluate(comptime original.component(original.value_component.?), &block, lane), evaluate(comptime compacted.component(compacted.value_component.?), &block, lane));
                }
                if (comptime original.minimum_component) |id| {
                    try std.testing.expectEqual(evaluate(comptime original.component(id), &block, lane), evaluate(comptime compacted.component(compacted.minimum_component.?), &block, lane));
                }
            }
        }
    }
    try std.testing.expect(compact_nodes < original_nodes);
}

/// Q6_K schedules address this scale by subblock rather than by value lane.
/// Share the same builder as the full decode; no bit-layout copy is required.
pub fn q6SubblockScale() Recipe {
    var b = Builder{ .recipe = .{ .format = .q6_k } };
    const sub = b.put(.lane);
    b.recipe.result = b.q6Scale(sub);
    return b.recipe;
}

test "scale-min recipes consume twelve-byte headers and preserve every six-bit field" {
    @setEvalBranchQuota(1000000);
    const scale = comptime scaleMinHeaderInteger(false).compact();
    const minimum = comptime scaleMinHeaderInteger(true).compact();
    try std.testing.expectEqual(@as(usize, 12), scale.blockBytes());
    try std.testing.expectEqual(@as(usize, 8), scale.blockValues());
    // Pack independent six-bit fields using the file layout, then decode them
    // through both integer and floating recipe lowerings, with no block padding.
    for (0..64) |seed| {
        var header: [12]u8 = @splat(0);
        var scales: [8]u8 = undefined;
        var minima: [8]u8 = undefined;
        for (0..8) |j| {
            scales[j] = @intCast((seed + j * 13) % 64);
            minima[j] = @intCast((seed * 7 + j * 19) % 64);
            if (j < 4) {
                header[j] |= scales[j];
                header[j + 4] |= minima[j];
            } else {
                header[j + 4] = (scales[j] & 15) | ((minima[j] & 15) << 4);
                header[j - 4] |= (scales[j] >> 4) << 6;
                header[j] |= (minima[j] >> 4) << 6;
            }
        }
        inline for (0..8) |j| {
            try std.testing.expectEqual(@as(i32, scales[j]), evaluateInteger(scale, &header, j));
            try std.testing.expectEqual(@as(i32, minima[j]), evaluateInteger(minimum, &header, j));
            try std.testing.expectEqual(@as(f32, @floatFromInt(scales[j])), evaluate(comptime scaleMinHeader(false).compact(), &header, j));
            try std.testing.expectEqual(@as(f32, @floatFromInt(minima[j])), evaluate(comptime scaleMinHeader(true).compact(), &header, j));
        }
    }
}

/// CPU packed-dot schedules hoist both headers together. Inline scalar lanes
/// retain the old constant-index loads and give the optimizer no dynamic recipe
/// dispatch, vector gathers, temporary block or float/integer round trip.
pub inline fn unpackScaleMinHeader(comptime T: type, header: []const u8, scales: *[8]T, minima: *[8]T) void {
    @setEvalBranchQuota(100000);
    if (T != u8 and T != f32) @compileError("scale-min header output must be U8 or F32");
    const scale = comptime scaleMinHeaderInteger(false).compact();
    const minimum = comptime scaleMinHeaderInteger(true).compact();
    inline for (0..8) |j| {
        const s = evaluateInteger(scale, header, j);
        const m = evaluateInteger(minimum, header, j);
        scales[j] = if (T == f32) @floatFromInt(s) else @intCast(s);
        minima[j] = if (T == f32) @floatFromInt(m) else @intCast(m);
    }
}
