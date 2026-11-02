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

//! Checked text input geometry and materialization, shared by task adapters.
//! Declared order/dtypes are preserved. Positions and additive masks are host
//! metadata; token IDs never pass through a floating-point representation.
const std = @import("std");
const tensors = @import("../backends/tensor.zig");
const Tensor = tensors.Tensor;
const max_inputs = 8;
const Role = enum { ids, mask, types, positions, bias };

pub const Options = struct { attention_heads: u32 = 0, token_type_ids: ?[]const i64 = null };
const Spec = struct {
    name: []const u8,
    role: Role,
    dtype: tensors.DType,
    dims: [4]i64 = @splat(1),
    rank: usize,
    count: usize,
    fn shape(self: *const Spec) []const i64 {
        return self.dims[0..self.rank];
    }
};

pub const Set = struct {
    items: [max_inputs]Tensor = undefined,
    owned: u8 = 0,
    len: usize = 0,
    pub fn slice(self: *const Set) []const Tensor {
        return self.items[0..self.len];
    }
    pub fn deinit(self: *Set) void {
        for (self.items[0..self.len], 0..) |*tensor, index| if ((self.owned & (@as(u8, 1) << @intCast(index))) != 0) {
            tensor.deinit();
        };
        self.* = undefined;
    }
    fn append(self: *Set, tensor: Tensor, owned: bool) void {
        self.items[self.len] = tensor;
        if (owned) self.owned |= @as(u8, 1) << @intCast(self.len);
        self.len += 1;
    }
};

pub const Plan = struct {
    specs: [max_inputs]Spec = undefined,
    len: usize = 0,
    batch: usize,
    sequence: usize,
    /// Actual public inputs plus constructor scratch, available for admission
    /// before materializing quadratic attention-bias storage.
    input_bytes: usize = 0,
    temporary_bytes: usize = 0,

    pub fn init(info: []const tensors.TensorInfo, shape: []const i64, options: Options) !Plan {
        if (shape.len != 2 or shape[0] <= 0 or shape[1] <= 0 or info.len == 0 or info.len > max_inputs) return error.InvalidInputShape;
        var plan = Plan{ .batch = @intCast(shape[0]), .sequence = @intCast(shape[1]) };
        const rows = try std.math.mul(usize, plan.batch, plan.sequence);
        for (info) |input| {
            const role: Role = if (std.mem.eql(u8, input.name, "input_ids") or std.mem.eql(u8, input.name, "ids")) .ids else if (std.mem.indexOf(u8, input.name, "token_type") != null) .types else if (std.mem.indexOf(u8, input.name, "position") != null) .positions else if ((std.mem.indexOf(u8, input.name, "mask") != null or std.mem.indexOf(u8, input.name, "bias") != null) and input.dtype == .f32 and input.shape.len >= 3) .bias else if (std.mem.indexOf(u8, input.name, "mask") != null) .mask else return error.UnsupportedTextInput;
            if (input.shape.len == 0 or input.shape.len > 4) return error.InvalidInputShape;
            var spec = Spec{ .name = input.name, .role = role, .dtype = input.dtype, .rank = input.shape.len, .count = 1 };
            if (role == .bias) {
                if (input.dtype != .f32 or (spec.rank != 3 and spec.rank != 4)) return error.UnsupportedTextInputType;
                const heads: i64 = if (spec.rank == 4 and input.shape[1] > 0) input.shape[1] else if (spec.rank == 3 and plan.batch == 1 and input.shape[0] > 0) input.shape[0] else if (options.attention_heads != 0) options.attention_heads else return error.MissingTextAttentionHeads;
                const query = if (input.shape[spec.rank - 2] == 1) @as(i64, 1) else shape[1];
                if (spec.rank == 3) spec.dims = .{ try std.math.mul(i64, shape[0], heads), query, shape[1], 1 } else spec.dims = .{ shape[0], heads, query, shape[1] };
            } else {
                if (spec.rank > 2 or (input.dtype != .i64 and input.dtype != .i32 and !(role == .mask and input.dtype == .f32))) return error.UnsupportedTextInputType;
                if (spec.rank == 1) spec.dims[0] = if (role == .positions) shape[1] else std.math.cast(i64, rows) orelse return error.InvalidInputShape else {
                    spec.dims[0] = if (role == .positions and input.shape[0] == 1) 1 else shape[0];
                    spec.dims[1] = shape[1];
                }
            }
            for (spec.shape(), input.shape) |actual, declared| {
                if (actual <= 0 or (declared > 0 and declared != actual)) return error.InvalidInputShape;
                spec.count = try std.math.mul(usize, spec.count, @intCast(actual));
            }
            const bytes = try input.dtype.byteCount(spec.count);
            plan.input_bytes = try std.math.add(usize, plan.input_bytes, bytes);
            if (role != .bias and !((role == .ids or role == .mask) and input.dtype == .i64 and spec.rank == 2))
                plan.temporary_bytes = @max(plan.temporary_bytes, try std.math.mul(usize, spec.count, @sizeOf(i64) + input.dtype.byteSize()));
            plan.specs[plan.len] = spec;
            plan.len += 1;
        }
        return plan;
    }

    pub fn materialize(self: *const Plan, allocator: std.mem.Allocator, ids: Tensor, mask: Tensor, options: Options) !Set {
        const rows = try std.math.mul(usize, self.batch, self.sequence);
        if (ids.dtype != .i64 or mask.dtype != .i64 or ids.asInt64().len != rows or mask.asInt64().len != rows) return error.InvalidInputShape;
        if (options.token_type_ids) |types| if (types.len != rows) return error.InvalidInputShape;
        var result = Set{};
        errdefer result.deinit();
        for (self.specs[0..self.len]) |*spec| {
            const source: ?Tensor = switch (spec.role) {
                .ids => ids,
                .mask => mask,
                else => null,
            };
            if (source) |tensor| {
                if (tensor.dtype == spec.dtype and std.mem.eql(i64, tensor.shape, spec.shape())) {
                    var borrowed = tensor;
                    borrowed.name = spec.name;
                    result.append(borrowed, false);
                    continue;
                }
            }
            if (spec.role == .bias) {
                const data = try allocator.alloc(f32, spec.count);
                errdefer allocator.free(data);
                const per_batch = spec.count / self.batch;
                for (data, 0..) |*value, index| {
                    const batch_index = index / per_batch;
                    value.* = if (mask.asInt64()[batch_index * self.sequence + index % self.sequence] > 0) 0 else -10000;
                }
                result.append(try Tensor.initFloat32Owned(allocator, spec.name, spec.shape(), data), true);
                continue;
            }
            const values = try allocator.alloc(i64, spec.count);
            defer allocator.free(values);
            for (values, 0..) |*value, index| value.* = switch (spec.role) {
                .ids => ids.asInt64()[index],
                .mask => mask.asInt64()[index],
                .types => if (options.token_type_ids) |types| types[index] else 0,
                .positions => @intCast(index % self.sequence),
                .bias => unreachable,
            };
            const tensor = switch (spec.dtype) {
                .i64 => try Tensor.initInt64(allocator, spec.name, spec.shape(), values),
                .i32 => blk: {
                    const narrowed = try allocator.alloc(i32, values.len);
                    defer allocator.free(narrowed);
                    for (values, narrowed) |value, *destination| destination.* = std.math.cast(i32, value) orelse return error.TextIntegerOutOfRange;
                    break :blk try Tensor.initInt32(allocator, spec.name, spec.shape(), narrowed);
                },
                .f32 => blk: {
                    const floating = try allocator.alloc(f32, values.len);
                    errdefer allocator.free(floating);
                    for (values, floating) |value, *destination| destination.* = if (value > 0) 1 else 0;
                    break :blk try Tensor.initFloat32Owned(allocator, spec.name, spec.shape(), floating);
                },
                else => unreachable,
            };
            result.append(tensor, true);
        }
        return result;
    }
};

pub fn build(allocator: std.mem.Allocator, info: []const tensors.TensorInfo, ids: Tensor, mask: Tensor, shape: []const i64, options: Options) !Set {
    const plan = try Plan.init(info, shape, options);
    return plan.materialize(allocator, ids, mask, options);
}

fn checkDeclaredTextInputs(allocator: std.mem.Allocator) !void {
    const shape = [_]i64{ 2, 3 };
    var ids = try Tensor.initInt64(allocator, "input_ids", &shape, &.{ 16_777_217, 2, 3, 4, 5, 6 });
    defer ids.deinit();
    var mask = try Tensor.initInt64(allocator, "attention_mask", &shape, &.{ 1, 1, 0, 0, 1, 0 });
    defer mask.deinit();
    const info = [_]tensors.TensorInfo{
        .{ .name = "position_ids", .dtype = .i64, .shape = &.{3} },
        .{ .name = "attention_bias", .dtype = .f32, .shape = &.{ 2, 2, 3, 3 } },
        .{ .name = "token_type_ids", .dtype = .i64, .shape = &.{ 2, 3 } },
        .{ .name = "input_ids", .dtype = .i32, .shape = &.{ 2, 3 } },
        .{ .name = "attention_mask", .dtype = .i64, .shape = &.{ 2, 3 } },
    };
    const options = Options{ .attention_heads = 2, .token_type_ids = &.{ 0, 1, 1, 0, 0, 1 } };
    const plan = try Plan.init(&info, &shape, options);
    try std.testing.expectEqual(@as(usize, 288), plan.input_bytes);
    var inputs = try plan.materialize(allocator, ids, mask, options);
    defer inputs.deinit();
    try std.testing.expectEqual(@as(usize, 5), inputs.len);
    for (inputs.slice(), info) |input, declared| {
        try std.testing.expectEqualStrings(declared.name, input.name);
        try std.testing.expectEqual(declared.dtype, input.dtype);
        try std.testing.expectEqualSlices(i64, declared.shape, input.shape);
    }
    try std.testing.expectEqualSlices(i64, &.{ 0, 1, 2 }, inputs.items[0].asInt64());
    for (inputs.items[1].asFloat32(), 0..) |value, index| {
        const b = index / 18;
        const expected: f32 = if (mask.asInt64()[b * 3 + index % 3] > 0) 0 else -10000;
        try std.testing.expectEqual(expected, value);
    }
    try std.testing.expectEqualSlices(i64, options.token_type_ids.?, inputs.items[2].asInt64());
    try std.testing.expectEqual(@as(i32, 16_777_217), std.mem.bytesToValue(i32, inputs.items[3].data[0..4]));
    try std.testing.expect(inputs.items[4].data.ptr == mask.data.ptr);
}

test "declared text inputs preserve order exact integers mask geometry and allocation failure ownership" {
    try checkDeclaredTextInputs(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, checkDeclaredTextInputs, .{});
}

test "declared text input planning rejects missing geometry unknown inputs and overflow before allocation" {
    const shared_positions = try Plan.init(&.{.{ .name = "position_ids", .dtype = .i64, .shape = &.{ 1, -1 } }}, &.{ 2, 3 }, .{});
    try std.testing.expectEqual(@as(usize, 24), shared_positions.input_bytes);
    const ids = [_]tensors.TensorInfo{.{ .name = "input_ids", .dtype = .i64, .shape = &.{ -1, -1 } }};
    const bias = [_]tensors.TensorInfo{.{ .name = "attention_bias", .dtype = .f32, .shape = &.{ -1, -1, -1 } }};
    try std.testing.expectError(error.MissingTextAttentionHeads, Plan.init(&bias, &.{ 2, 3 }, .{}));
    const planned = try Plan.init(&bias, &.{ 2, 3 }, .{ .attention_heads = 2 });
    try std.testing.expectEqual(@as(usize, 144), planned.input_bytes);
    try std.testing.expectError(error.Overflow, Plan.init(&ids, &.{ std.math.maxInt(i64), 2 }, .{}));
    try std.testing.expectError(error.UnsupportedTextInput, Plan.init(&.{.{ .name = "unknown", .dtype = .i64, .shape = &.{ 1, 3 } }}, &.{ 1, 3 }, .{}));
    try std.testing.expectError(error.InvalidInputShape, Plan.init(&.{.{ .name = "input_ids", .dtype = .i64, .shape = &.{ 1, 3 } }}, &.{ 2, 3 }, .{}));
}

test "declared text inputs support broadcast bias and reject lossy token conversion" {
    const a = std.testing.allocator;
    const shape = [_]i64{ 2, 3 };
    var ids = try Tensor.initInt64(a, "input_ids", &shape, &.{ 1, 2, 3, 4, 5, 6 });
    defer ids.deinit();
    var mask = try Tensor.initInt64(a, "attention_mask", &shape, &.{ 1, 1, 0, 0, 1, 0 });
    defer mask.deinit();
    const info = [_]tensors.TensorInfo{.{ .name = "attention_mask", .dtype = .f32, .shape = &.{ 2, 1, 1, 3 } }};
    const plan = try Plan.init(&info, &shape, .{ .attention_heads = 12 });
    try std.testing.expectEqual(@as(usize, 24), plan.input_bytes);
    var inputs = try plan.materialize(a, ids, mask, .{});
    defer inputs.deinit();
    try std.testing.expectEqualSlices(f32, &.{ 0, 0, -10000, -10000, 0, -10000 }, inputs.items[0].asFloat32());
    var wide = try Tensor.initInt64(a, "input_ids", &shape, &.{ 1, 2, 3, 4, 5, 1 << 40 });
    defer wide.deinit();
    const narrow = try Plan.init(&.{.{ .name = "input_ids", .dtype = .i32, .shape = &.{ 2, 3 } }}, &shape, .{});
    try std.testing.expectError(error.TextIntegerOutOfRange, narrow.materialize(a, wide, mask, .{}));
}
