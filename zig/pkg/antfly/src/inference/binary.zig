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

pub fn deserializeDense(alloc: std.mem.Allocator, data: []const u8) !DenseEmbeddings {
    if (data.len < 16) return error.InvalidBinaryResponse;

    const num_vectors = std.math.cast(usize, readLittleEndian(u64, data[0..])) orelse return error.InvalidBinaryResponse;
    const dimension = std.math.cast(usize, readLittleEndian(u64, data[8..])) orelse return error.InvalidBinaryResponse;
    if (num_vectors > 0 and dimension == 0) return error.InvalidBinaryResponse;
    const value_count = std.math.mul(usize, num_vectors, dimension) catch return error.InvalidBinaryResponse;
    const payload_bytes = std.math.mul(usize, value_count, 4) catch return error.InvalidBinaryResponse;
    const expected_len = std.math.add(usize, 16, payload_bytes) catch return error.InvalidBinaryResponse;
    if (data.len != expected_len) return error.InvalidBinaryResponse;
    if (num_vectors == 0) return .{ .dimension = dimension, .vectors = &.{} };

    const vectors = try alloc.alloc([]const f32, @intCast(num_vectors));
    @memset(vectors, &.{});
    errdefer {
        for (vectors) |v| if (v.len > 0) alloc.free(v);
        alloc.free(vectors);
    }

    var offset: usize = 16;
    for (0..@intCast(num_vectors)) |i| {
        const vec = try alloc.alloc(f32, @intCast(dimension));
        vectors[i] = vec;
        for (0..@intCast(dimension)) |j| {
            vec[j] = readF32(data[offset..]);
            if (!std.math.isFinite(vec[j])) return error.InvalidBinaryResponse;
            offset += 4;
        }
    }

    return .{ .dimension = @intCast(dimension), .vectors = vectors };
}

pub fn deserializeSparse(alloc: std.mem.Allocator, data: []const u8) !SparseEmbeddings {
    if (data.len < 8) return error.InvalidBinaryResponse;

    const num_vectors = std.math.cast(usize, readLittleEndian(u64, data[0..])) orelse return error.InvalidBinaryResponse;
    // Each row needs at least its nnz field, even for an empty sparse vector.
    if (num_vectors > (data.len - 8) / 4) return error.InvalidBinaryResponse;
    if (num_vectors == 0) {
        if (data.len != 8) return error.InvalidBinaryResponse;
        return .{ .vectors = &.{} };
    }

    const vectors = try alloc.alloc(SparseVector, @intCast(num_vectors));
    var initialized: usize = 0;
    errdefer {
        for (vectors[0..initialized]) |v| v.deinit(alloc);
        alloc.free(vectors);
    }

    var offset: usize = 8;
    for (0..@intCast(num_vectors)) |i| {
        if (offset + 4 > data.len) return error.InvalidBinaryResponse;
        const nnz: usize = @intCast(readLittleEndian(u32, data[offset..]));
        offset += 4;

        const needed = std.math.mul(usize, nnz, @sizeOf(i32) + @sizeOf(f32)) catch return error.InvalidBinaryResponse;
        if (needed > data.len - offset) return error.InvalidBinaryResponse;

        const indices = try alloc.alloc(i32, nnz);
        errdefer alloc.free(indices);
        for (0..nnz) |j| {
            indices[j] = @bitCast(readLittleEndian(u32, data[offset..]));
            offset += 4;
        }

        const values = try alloc.alloc(f32, nnz);
        errdefer alloc.free(values);
        for (0..nnz) |j| {
            values[j] = readF32(data[offset..]);
            if (!std.math.isFinite(values[j])) return error.InvalidBinaryResponse;
            offset += 4;
        }

        vectors[i] = .{ .indices = indices, .values = values };
        initialized += 1;
    }

    if (offset != data.len) return error.InvalidBinaryResponse;

    return .{ .vectors = vectors };
}

test "legacy numeric decoders reject malformed lengths and clean up partial allocations" {
    const Runner = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var dense = [_]u8{0} ** 32;
            std.mem.writeInt(u64, dense[0..8], 2, .little);
            std.mem.writeInt(u64, dense[8..16], 2, .little);
            const decoded = try deserializeDense(alloc, &dense);
            decoded.deinit(alloc);
            var sparse = [_]u8{0} ** 20;
            std.mem.writeInt(u64, sparse[0..8], 1, .little);
            std.mem.writeInt(u32, sparse[8..12], 1, .little);
            const sv = try deserializeSparse(alloc, &sparse);
            sv.deinit(alloc);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
    var malicious = [_]u8{255} ** 24;
    try std.testing.expectError(error.InvalidBinaryResponse, deserializeDense(std.testing.allocator, &malicious));
    try std.testing.expectError(error.InvalidBinaryResponse, deserializeSparse(std.testing.allocator, &malicious));
    @memset(&malicious, 0);
    try std.testing.expectError(error.InvalidBinaryResponse, deserializeDense(std.testing.allocator, &malicious));
    try std.testing.expectError(error.InvalidBinaryResponse, deserializeSparse(std.testing.allocator, &malicious));
}
