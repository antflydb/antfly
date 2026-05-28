// Copyright 2026 Antfly, Inc.
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

// Binary response format parsing for Antfly inference embedding responses.
//
// Dense format:  uint64(num_vectors) + uint64(dimension) + float32[num_vectors * dimension]
// Sparse format: uint64(num_vectors) + per-vector: [uint32(nnz) + int32[nnz] + float32[nnz]]

const std = @import("std");

pub const SparseVector = struct {
    indices: []const i32,
    values: []const f32,

    pub fn deinit(self: SparseVector, alloc: std.mem.Allocator) void {
        alloc.free(self.indices);
        alloc.free(self.values);
    }
};

pub const DenseEmbeddings = struct {
    dimension: usize,
    vectors: []const []const f32,

    pub fn deinit(self: DenseEmbeddings, alloc: std.mem.Allocator) void {
        for (self.vectors) |v| alloc.free(v);
        alloc.free(self.vectors);
    }
};

pub const SparseEmbeddings = struct {
    vectors: []const SparseVector,

    pub fn deinit(self: SparseEmbeddings, alloc: std.mem.Allocator) void {
        for (self.vectors) |v| v.deinit(alloc);
        alloc.free(self.vectors);
    }
};

fn readLittleEndian(comptime T: type, data: []const u8) T {
    return std.mem.readInt(T, data[0..@sizeOf(T)], .little);
}

fn readF32(data: []const u8) f32 {
    return @bitCast(std.mem.readInt(u32, data[0..4], .little));
}

/// Deserialize dense embeddings from Antfly inference binary format.
/// Format: uint64(num_vectors) + uint64(dimension) + float32[num_vectors * dimension]
pub fn deserializeDense(alloc: std.mem.Allocator, data: []const u8) !DenseEmbeddings {
    if (data.len < 16) return error.InvalidBinaryResponse;

    const num_vectors = readLittleEndian(u64, data[0..]);
    const dimension = readLittleEndian(u64, data[8..]);

    if (num_vectors == 0) return .{ .dimension = @intCast(dimension), .vectors = &.{} };

    const expected_len = 16 + num_vectors * dimension * 4;
    if (data.len < expected_len) return error.InvalidBinaryResponse;

    const vectors = try alloc.alloc([]const f32, @intCast(num_vectors));
    errdefer {
        for (vectors) |v| if (v.len > 0) alloc.free(v);
        alloc.free(vectors);
    }

    var offset: usize = 16;
    for (0..@intCast(num_vectors)) |i| {
        const vec = try alloc.alloc(f32, @intCast(dimension));
        for (0..@intCast(dimension)) |j| {
            vec[j] = readF32(data[offset..]);
            offset += 4;
        }
        vectors[i] = vec;
    }

    return .{ .dimension = @intCast(dimension), .vectors = vectors };
}

/// Deserialize sparse embeddings from Antfly inference binary format.
/// Format: uint64(num_vectors) + per-vector: [uint32(nnz) + int32[nnz] + float32[nnz]]
pub fn deserializeSparse(alloc: std.mem.Allocator, data: []const u8) !SparseEmbeddings {
    if (data.len < 8) return error.InvalidBinaryResponse;

    const num_vectors = readLittleEndian(u64, data[0..]);
    if (num_vectors == 0) return .{ .vectors = &.{} };

    const vectors = try alloc.alloc(SparseVector, @intCast(num_vectors));
    errdefer {
        for (vectors) |v| {
            if (v.indices.len > 0) v.deinit(alloc);
        }
        alloc.free(vectors);
    }

    var offset: usize = 8;
    for (0..@intCast(num_vectors)) |i| {
        if (offset + 4 > data.len) return error.InvalidBinaryResponse;
        const nnz: usize = @intCast(readLittleEndian(u32, data[offset..]));
        offset += 4;

        const needed = nnz * (@sizeOf(i32) + @sizeOf(f32));
        if (offset + needed > data.len) return error.InvalidBinaryResponse;

        const indices = try alloc.alloc(i32, nnz);
        errdefer alloc.free(indices);
        for (0..nnz) |j| {
            indices[j] = @bitCast(readLittleEndian(u32, data[offset..]));
            offset += 4;
        }

        const values = try alloc.alloc(f32, nnz);
        for (0..nnz) |j| {
            values[j] = readF32(data[offset..]);
            offset += 4;
        }

        vectors[i] = .{ .indices = indices, .values = values };
    }

    return .{ .vectors = vectors };
}

test "deserialize dense embeddings" {
    const alloc = std.testing.allocator;

    // 2 vectors, dimension 3: [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]
    var buf: [16 + 6 * 4]u8 = undefined;
    std.mem.writeInt(u64, buf[0..8], 2, .little);
    std.mem.writeInt(u64, buf[8..16], 3, .little);
    inline for (.{ 1.0, 2.0, 3.0, 4.0, 5.0, 6.0 }, 0..) |val, i| {
        std.mem.writeInt(u32, buf[16 + i * 4 ..][0..4], @as(u32, @bitCast(@as(f32, val))), .little);
    }

    var result = try deserializeDense(alloc, &buf);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 3), result.dimension);
    try std.testing.expectEqual(@as(usize, 2), result.vectors.len);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), result.vectors[0][0], 0.001);
    try std.testing.expectApproxEqAbs(@as(f32, 6.0), result.vectors[1][2], 0.001);
}

test "deserialize empty dense" {
    const alloc = std.testing.allocator;
    var buf: [16]u8 = undefined;
    std.mem.writeInt(u64, buf[0..8], 0, .little);
    std.mem.writeInt(u64, buf[8..16], 384, .little);
    const result = try deserializeDense(alloc, &buf);
    try std.testing.expectEqual(@as(usize, 0), result.vectors.len);
}

test "deserialize sparse embeddings" {
    const alloc = std.testing.allocator;

    // 1 vector with 2 non-zero entries: indices=[0, 5], values=[1.0, 2.5]
    var buf: [8 + 4 + 2 * 4 + 2 * 4]u8 = undefined;
    std.mem.writeInt(u64, buf[0..8], 1, .little);
    std.mem.writeInt(u32, buf[8..12], 2, .little);
    std.mem.writeInt(i32, buf[12..16], 0, .little);
    std.mem.writeInt(i32, buf[16..20], 5, .little);
    std.mem.writeInt(u32, buf[20..24], @as(u32, @bitCast(@as(f32, 1.0))), .little);
    std.mem.writeInt(u32, buf[24..28], @as(u32, @bitCast(@as(f32, 2.5))), .little);

    var result = try deserializeSparse(alloc, &buf);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.vectors.len);
    try std.testing.expectEqual(@as(i32, 5), result.vectors[0].indices[1]);
    try std.testing.expectApproxEqAbs(@as(f32, 2.5), result.vectors[0].values[1], 0.001);
}
