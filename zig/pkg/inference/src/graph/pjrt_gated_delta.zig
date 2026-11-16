// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Compact F32 recurrent lowering. One loop body is independent of prefix
//! length; the provider retains the matrix and publishes rows on device.
const std = @import("std");
const hlo = @import("pjrt").hlo;
const Attrs = @import("ml").graph.node.GatedDeltaAttrs;
const Id = hlo.Id;
const Shape = hlo.Shape;

pub const Program = struct {
    condition: hlo.Builder,
    body: hlo.Builder,
    condition_id: Id,
    body_id: Id,
    condition_name: ?[]u8 = null,
    body_name: ?[]u8 = null,

    pub fn deinit(self: *Program) void {
        const allocator = self.body.allocator;
        self.body.deinit();
        self.condition.deinit();
        if (self.condition_name) |name| allocator.free(name);
        if (self.body_name) |name| allocator.free(name);
    }
    pub fn computations(self: *Program) [2]hlo.Computation {
        var condition = self.condition.build();
        condition.id = self.condition_id;
        var body = self.body.build();
        body.id = self.body_id;
        return .{ condition, body };
    }

    pub fn emit(self: *Program, allocator: std.mem.Allocator, b: *hlo.Builder, mixed: Id, beta: Id, decay: Id, p: Attrs, reducer: Id, ordinal: usize) !Id {
        if (p.rows == 0 or p.key_heads == 0 or p.value_heads == 0 or p.value_heads % p.key_heads != 0 or p.key_dim == 0 or p.value_dim == 0) return error.UnsupportedShape;
        const s: i64 = p.rows;
        const h: i64 = p.value_heads;
        const kh: i64 = p.key_heads;
        const k: i64 = p.key_dim;
        const v: i64 = p.value_dim;
        const c = 2 * kh * k + h * v;
        const matrix_shape = Shape.init(.f32, &.{ h, k, v });
        const output_shape = Shape.init(.f32, &.{ s, h, v });
        const source = try b.convertType(mixed, .f32);
        const betas = try b.convertType(beta, .f32);
        const decays = try b.convertType(decay, .f32);
        if (!std.mem.eql(i64, b.getInst(source).shape.dimensions, &.{ s, c }) or !std.mem.eql(i64, b.getInst(betas).shape.dimensions, &.{ s, h }) or !std.mem.eql(i64, b.getInst(decays).shape.dimensions, &.{ s, h })) return error.UnsupportedShape;
        const zero = try b.constantScalarF32(0);
        const initial = try b.tuple(&.{ try b.constantS32(Shape.scalar(.s32), &.{0}), try b.broadcast(zero, &.{}, matrix_shape), try b.broadcast(zero, &.{}, output_shape), source, betas, decays });
        const state_shapes = b.getInst(initial).tuple_child_shapes.?;
        // XLA indexes some import metadata by the largest wire ID. Keep this
        // range adjacent to the compiler's reducers, and number recurrences
        // rather than sparse source nodes; billion-scale IDs allocate GBs.
        const base: Id = 3_000_000 + @as(Id, @intCast(ordinal)) * 1000;
        self.* = .{ .condition = hlo.Builder.init(allocator, "delta_condition"), .body = hlo.Builder.init(allocator, "delta_body"), .condition_id = base, .body_id = base + 100 };
        self.condition_name = try std.fmt.allocPrint(allocator, "delta_condition_{d}", .{ordinal});
        self.condition.name = self.condition_name.?;
        self.body_name = try std.fmt.allocPrint(allocator, "delta_body_{d}", .{ordinal});
        self.body.name = self.body_name.?;
        self.condition.next_id = base + 1;
        self.condition.base_id = base + 1;
        self.body.next_id = base + 101;
        self.body.base_id = base + 101;
        const cond = &self.condition;
        const cond_state = try cond.tupleParameter(0, state_shapes);
        _ = try cond.lessThan(try cond.tupleElement(cond_state, 0), try cond.constantS32(Shape.scalar(.s32), &.{@intCast(s)}));
        const body = &self.body;
        const state = try body.tupleParameter(0, state_shapes);
        const cursor = try body.tupleElement(state, 0);
        const matrix = try body.tupleElement(state, 1);
        const outputs = try body.tupleElement(state, 2);
        const xs = try body.tupleElement(state, 3);
        const bs = try body.tupleElement(state, 4);
        const ds = try body.tupleElement(state, 5);
        const izero = try body.constantS32(Shape.scalar(.s32), &.{0});
        const row = try body.dynamicSlice(xs, &.{ cursor, izero }, &.{ 1, c });
        const q = try body.reshape(try body.slice(row, &.{ 0, 0 }, &.{ 1, kh * k }, &.{ 1, 1 }, Shape.init(.f32, &.{ 1, kh * k })), Shape.init(.f32, &.{ kh, k }));
        const key = try body.reshape(try body.slice(row, &.{ 0, kh * k }, &.{ 1, 2 * kh * k }, &.{ 1, 1 }, Shape.init(.f32, &.{ 1, kh * k })), Shape.init(.f32, &.{ kh, k }));
        const values = try body.reshape(try body.slice(row, &.{ 0, 2 * kh * k }, &.{ 1, c }, &.{ 1, 1 }, Shape.init(.f32, &.{ 1, h * v })), Shape.init(.f32, &.{ h, v }));
        const indices = try allocator.alloc(i32, p.value_heads);
        defer allocator.free(indices);
        for (indices, 0..) |*index, i| index.* = @intCast(i / (p.value_heads / p.key_heads));
        const map = try body.constantS32(Shape.init(.s32, &.{h}), indices);
        const q_norm = try normalized(body, try body.gather(q, map, Shape.init(.f32, &.{ h, k })), h, k, reducer);
        const keys = try normalized(body, try body.gather(key, map, Shape.init(.f32, &.{ h, k })), h, k, reducer);
        const queries = try body.multiply(q_norm, try body.broadcast(try body.constantScalarF32(1 / @sqrt(@as(f32, @floatFromInt(k)))), &.{}, Shape.init(.f32, &.{ h, k })));
        const d = try body.reshape(try body.dynamicSlice(ds, &.{ cursor, izero }, &.{ 1, h }), Shape.init(.f32, &.{h}));
        const beta_row = try body.reshape(try body.dynamicSlice(bs, &.{ cursor, izero }, &.{ 1, h }), Shape.init(.f32, &.{h}));
        const decayed = try body.multiply(matrix, try body.broadcast(d, &.{0}, matrix_shape));
        const key_matrix = try body.broadcast(keys, &.{ 0, 1 }, matrix_shape);
        const memory = try body.reduce(try body.multiply(decayed, key_matrix), try body.constantScalarF32(0), &.{1}, Shape.init(.f32, &.{ h, v }), reducer);
        const delta = try body.multiply(try body.subtract(values, memory), try body.broadcast(beta_row, &.{0}, Shape.init(.f32, &.{ h, v })));
        const updated = try body.add(decayed, try body.multiply(key_matrix, try body.broadcast(delta, &.{ 0, 2 }, matrix_shape)));
        const result = try body.reduce(try body.multiply(updated, try body.broadcast(queries, &.{ 0, 1 }, matrix_shape)), try body.constantScalarF32(0), &.{1}, Shape.init(.f32, &.{ h, v }), reducer);
        const published = try body.dynamicUpdateSlice(outputs, try body.reshape(result, Shape.init(.f32, &.{ 1, h, v })), &.{ cursor, izero, izero });
        _ = try body.tuple(&.{ try body.add(cursor, try body.constantS32(Shape.scalar(.s32), &.{1})), updated, published, xs, bs, ds });
        const loop = try b.whileLoop(initial, self.condition_id, self.body_id);
        return b.reshape(try b.tupleElement(loop, 2), Shape.init(.f32, &.{ s, h * v }));
    }
};

fn normalized(b: *hlo.Builder, value: Id, heads: i64, width: i64, reducer: Id) !Id {
    const sums = try b.reduce(try b.multiply(value, value), try b.constantScalarF32(0), &.{1}, Shape.init(.f32, &.{heads}), reducer);
    const eps = try b.broadcast(try b.constantScalarF32(1e-6), &.{}, Shape.init(.f32, &.{heads}));
    const inv = try b.rsqrt(try b.add(sums, eps));
    return b.multiply(value, try b.broadcast(inv, &.{0}, Shape.init(.f32, &.{ heads, width })));
}
