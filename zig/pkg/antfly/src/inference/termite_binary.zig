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
