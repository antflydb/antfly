// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const vector_codec = @import("antfly_vector").codec;

pub const DenseEmbeddingQuery = struct {
    vector: []f32,
    k: u32,

    pub fn deinit(self: *DenseEmbeddingQuery, alloc: std.mem.Allocator) void {
        alloc.free(self.vector);
        self.* = undefined;
    }
};

pub const SparseEmbeddingQuery = struct {
    indices: []u32,
    values: []f32,
    k: u32,

    pub fn deinit(self: *SparseEmbeddingQuery, alloc: std.mem.Allocator) void {
        alloc.free(self.indices);
        alloc.free(self.values);
        self.* = undefined;
    }
};

pub const EmbeddingQuery = union(enum) {
    dense: DenseEmbeddingQuery,
    sparse: SparseEmbeddingQuery,

    pub fn deinit(self: *EmbeddingQuery, alloc: std.mem.Allocator) void {
        switch (self.*) {
            .dense => |*dense| dense.deinit(alloc),
            .sparse => |*sparse| sparse.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub fn parseEmbeddingValueAlloc(
    alloc: std.mem.Allocator,
    value: std.json.Value,
    default_k: u32,
) !EmbeddingQuery {
    return switch (value) {
        .array, .string => .{ .dense = try parseDenseEmbeddingAlloc(alloc, value, default_k) },
        .object => if (value.object.get("indices") != null or value.object.get("packed_indices") != null)
            .{ .sparse = try parseSparseEmbeddingAlloc(alloc, value, default_k) }
        else
            .{ .dense = try parseDenseEmbeddingAlloc(alloc, value, default_k) },
        else => error.UnsupportedQueryRequest,
    };
}

pub fn parseDenseEmbeddingAlloc(
    alloc: std.mem.Allocator,
    value: std.json.Value,
    default_k: u32,
) !DenseEmbeddingQuery {
    return switch (value) {
        .array => blk: {
            const vector = try alloc.alloc(f32, value.array.items.len);
            errdefer alloc.free(vector);
            for (value.array.items, 0..) |item, i| vector[i] = try jsonNumberToF32(item);
            break :blk .{
                .vector = vector,
                .k = default_k,
            };
        },
        .string => .{
            .vector = try vector_codec.decodePackedF32Base64Alloc(alloc, value.string),
            .k = default_k,
        },
        else => error.UnsupportedQueryRequest,
    };
}

pub fn parseSparseEmbeddingAlloc(
    alloc: std.mem.Allocator,
    value: std.json.Value,
    default_k: u32,
) !SparseEmbeddingQuery {
    if (value != .object) return error.InvalidQueryRequest;
    const packed_indices = value.object.get("packed_indices");
    const packed_values = value.object.get("packed_values");
    const indices_val = value.object.get("indices");
    const values_val = value.object.get("values");

    const indices = if (packed_indices != null or packed_values != null) blk: {
        if (packed_indices == null or packed_values == null) return error.InvalidQueryRequest;
        if (packed_indices.? != .string or packed_values.? != .string) return error.InvalidQueryRequest;
        break :blk vector_codec.decodePackedU32Base64Alloc(alloc, packed_indices.?.string) catch return error.InvalidQueryRequest;
    } else blk: {
        if (indices_val == null or values_val == null) return error.InvalidQueryRequest;
        if (indices_val.? != .array or values_val.? != .array) return error.InvalidQueryRequest;
        if (indices_val.?.array.items.len != values_val.?.array.items.len) return error.InvalidQueryRequest;

        const out = try alloc.alloc(u32, indices_val.?.array.items.len);
        errdefer alloc.free(out);
        for (indices_val.?.array.items, 0..) |item, i| {
            if (item != .integer) return error.InvalidQueryRequest;
            out[i] = @intCast(item.integer);
        }
        break :blk out;
    };
    errdefer alloc.free(indices);

    const values = if (packed_indices != null or packed_values != null)
        vector_codec.decodePackedF32Base64Alloc(alloc, packed_values.?.string) catch return error.InvalidQueryRequest
    else blk: {
        const out = try alloc.alloc(f32, values_val.?.array.items.len);
        errdefer alloc.free(out);
        for (values_val.?.array.items, 0..) |item, i| out[i] = try jsonNumberToF32(item);
        break :blk out;
    };
    errdefer alloc.free(values);

    return .{
        .indices = indices,
        .values = values,
        .k = if (value.object.get("k")) |k| @intCast(k.integer) else default_k,
    };
}

fn jsonNumberToF32(value: std.json.Value) !f32 {
    return switch (value) {
        .float => @floatCast(value.float),
        .integer => @floatFromInt(value.integer),
        else => error.InvalidQueryRequest,
    };
}

test "parse dense embedding array" {
    const alloc = std.testing.allocator;
    var parsed_json = try std.json.parseFromSlice(std.json.Value, alloc, "[1.0,2]", .{});
    defer parsed_json.deinit();
    var parsed = try parseEmbeddingValueAlloc(alloc, parsed_json.value, 7);
    defer parsed.deinit(alloc);
    try std.testing.expect(parsed == .dense);
    try std.testing.expectEqual(@as(u32, 7), parsed.dense.k);
    try std.testing.expectEqual(@as(usize, 2), parsed.dense.vector.len);
}

test "parse sparse embedding object" {
    const alloc = std.testing.allocator;
    var parsed_json = try std.json.parseFromSlice(std.json.Value, alloc, "{\"indices\":[1,2],\"values\":[0.5,1.5],\"k\":3}", .{});
    defer parsed_json.deinit();
    var parsed = try parseEmbeddingValueAlloc(alloc, parsed_json.value, 10);
    defer parsed.deinit(alloc);
    try std.testing.expect(parsed == .sparse);
    try std.testing.expectEqual(@as(u32, 3), parsed.sparse.k);
    try std.testing.expectEqual(@as(usize, 2), parsed.sparse.indices.len);
    try std.testing.expectEqual(@as(usize, 2), parsed.sparse.values.len);
}

test "parse packed dense embedding string" {
    const alloc = std.testing.allocator;
    var parsed_json = try std.json.parseFromSlice(std.json.Value, alloc, "\"AACAPwAAAEAAAEBA\"", .{});
    defer parsed_json.deinit();
    var parsed = try parseEmbeddingValueAlloc(alloc, parsed_json.value, 9);
    defer parsed.deinit(alloc);
    try std.testing.expect(parsed == .dense);
    try std.testing.expectEqual(@as(u32, 9), parsed.dense.k);
    try std.testing.expectEqual(@as(usize, 3), parsed.dense.vector.len);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), parsed.dense.vector[0], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 2.0), parsed.dense.vector[1], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 3.0), parsed.dense.vector[2], 0.0001);
}

test "parse packed sparse embedding object" {
    const alloc = std.testing.allocator;
    var parsed_json = try std.json.parseFromSlice(std.json.Value, alloc, "{\"packed_indices\":\"AQAAAAUAAAA=\",\"packed_values\":\"AAAAPwAAQD8=\",\"k\":4}", .{});
    defer parsed_json.deinit();
    var parsed = try parseEmbeddingValueAlloc(alloc, parsed_json.value, 10);
    defer parsed.deinit(alloc);
    try std.testing.expect(parsed == .sparse);
    try std.testing.expectEqual(@as(u32, 4), parsed.sparse.k);
    try std.testing.expectEqual(@as(usize, 2), parsed.sparse.indices.len);
    try std.testing.expectEqual(@as(u32, 1), parsed.sparse.indices[0]);
    try std.testing.expectEqual(@as(u32, 5), parsed.sparse.indices[1]);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), parsed.sparse.values[0], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 0.75), parsed.sparse.values[1], 0.0001);
}
