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
const builtin = @import("builtin");
const build_options = @import("build_options");
const Allocator = std.mem.Allocator;
const template_mod = if (builtin.os.tag == .freestanding or builtin.is_test or build_options.bench_minimal_deps)
    @import("../template_stub.zig")
else
    @import("../../../template.zig");

pub const DenseEmbedFn = *const fn (ptr: *anyopaque, alloc: Allocator, embedding_name: []const u8, text: []const u8, dims: u32) anyerror![]f32;
pub const DenseEmbedBatchFn = *const fn (ptr: *anyopaque, alloc: Allocator, embedding_name: []const u8, texts: []const []const u8, dims: u32) anyerror![]const []const f32;
pub const DenseEmbedPartsFn = *const fn (ptr: *anyopaque, alloc: Allocator, embedding_name: []const u8, parts: []const template_mod.ContentPart, dims: u32) anyerror![]f32;
pub const DenseEmbedDeinitFn = *const fn (ptr: *anyopaque, alloc: Allocator) void;
pub const SparseEmbedFn = *const fn (ptr: *anyopaque, alloc: Allocator, embedding_name: []const u8, text: []const u8) anyerror!SparseEmbedding;
pub const SparseEmbedBatchFn = *const fn (ptr: *anyopaque, alloc: Allocator, embedding_name: []const u8, texts: []const []const u8) anyerror![]SparseEmbedding;
pub const SparseEmbedDeinitFn = *const fn (ptr: *anyopaque, alloc: Allocator) void;

pub const SparseEmbedding = struct {
    indices: []u32,
    values: []f32,

    pub fn deinit(self: *SparseEmbedding, alloc: Allocator) void {
        alloc.free(self.indices);
        alloc.free(self.values);
        self.* = undefined;
    }
};

pub const DenseEmbedder = struct {
    ptr: *anyopaque,
    dense_embed_fn: DenseEmbedFn,
    dense_embed_batch_fn: ?DenseEmbedBatchFn = null,
    dense_embed_parts_fn: ?DenseEmbedPartsFn = null,
    deinit_fn: ?DenseEmbedDeinitFn = null,

    pub fn embedDense(self: DenseEmbedder, alloc: Allocator, embedding_name: []const u8, text: []const u8, dims: u32) ![]f32 {
        return try self.dense_embed_fn(self.ptr, alloc, embedding_name, text, dims);
    }

    pub fn embedDenseBatch(
        self: DenseEmbedder,
        alloc: Allocator,
        embedding_name: []const u8,
        texts: []const []const u8,
        dims: u32,
    ) ![]const []const f32 {
        const dense_embed_batch_fn = self.dense_embed_batch_fn orelse return try fallbackDenseBatch(self, alloc, embedding_name, texts, dims);
        return try dense_embed_batch_fn(self.ptr, alloc, embedding_name, texts, dims);
    }

    pub fn supportsParts(self: DenseEmbedder) bool {
        return self.dense_embed_parts_fn != null;
    }

    pub fn embedDenseParts(
        self: DenseEmbedder,
        alloc: Allocator,
        embedding_name: []const u8,
        parts: []const template_mod.ContentPart,
        dims: u32,
    ) ![]f32 {
        const dense_embed_parts_fn = self.dense_embed_parts_fn orelse return error.UnsupportedEmbeddingProvider;
        return try dense_embed_parts_fn(self.ptr, alloc, embedding_name, parts, dims);
    }

    pub fn deinit(self: DenseEmbedder, alloc: Allocator) void {
        const deinit_fn = self.deinit_fn orelse return;
        deinit_fn(self.ptr, alloc);
    }
};

pub const SparseEmbedder = struct {
    ptr: *anyopaque,
    sparse_embed_fn: SparseEmbedFn,
    sparse_embed_batch_fn: ?SparseEmbedBatchFn = null,
    deinit_fn: ?SparseEmbedDeinitFn = null,

    pub fn embedSparse(self: SparseEmbedder, alloc: Allocator, embedding_name: []const u8, text: []const u8) !SparseEmbedding {
        return try self.sparse_embed_fn(self.ptr, alloc, embedding_name, text);
    }

    pub fn embedSparseBatch(
        self: SparseEmbedder,
        alloc: Allocator,
        embedding_name: []const u8,
        texts: []const []const u8,
    ) ![]SparseEmbedding {
        const sparse_embed_batch_fn = self.sparse_embed_batch_fn orelse return try fallbackSparseBatch(self, alloc, embedding_name, texts);
        return try sparse_embed_batch_fn(self.ptr, alloc, embedding_name, texts);
    }

    pub fn deinit(self: SparseEmbedder, alloc: Allocator) void {
        const deinit_fn = self.deinit_fn orelse return;
        deinit_fn(self.ptr, alloc);
    }
};

pub fn freeDenseEmbeddingBatch(alloc: Allocator, batch: []const []const f32) void {
    for (batch) |vector| alloc.free(@constCast(vector));
    alloc.free(@constCast(batch));
}

pub fn freeSparseEmbeddingBatch(alloc: Allocator, batch: []SparseEmbedding) void {
    for (batch) |*embedding| embedding.deinit(alloc);
    alloc.free(batch);
}

fn fallbackDenseBatch(
    self: DenseEmbedder,
    alloc: Allocator,
    embedding_name: []const u8,
    texts: []const []const u8,
    dims: u32,
) ![]const []const f32 {
    const batch = try alloc.alloc([]const f32, texts.len);
    var initialized: usize = 0;
    errdefer {
        for (batch[0..initialized]) |vector| alloc.free(@constCast(vector));
        alloc.free(batch);
    }
    for (texts, 0..) |text, i| {
        batch[i] = try self.embedDense(alloc, embedding_name, text, dims);
        initialized += 1;
    }
    return batch;
}

fn fallbackSparseBatch(
    self: SparseEmbedder,
    alloc: Allocator,
    embedding_name: []const u8,
    texts: []const []const u8,
) ![]SparseEmbedding {
    const batch = try alloc.alloc(SparseEmbedding, texts.len);
    var initialized: usize = 0;
    errdefer {
        for (batch[0..initialized]) |*embedding| embedding.deinit(alloc);
        alloc.free(batch);
    }
    for (texts, 0..) |text, i| {
        batch[i] = try self.embedSparse(alloc, embedding_name, text);
        initialized += 1;
    }
    return batch;
}

pub const DeterministicDenseEmbedder = struct {
    seed: u64 = 0xcbf29ce484222325,

    pub fn embedDense(ptr: *anyopaque, alloc: Allocator, _: []const u8, text: []const u8, dims: u32) ![]f32 {
        const self: *DeterministicDenseEmbedder = @ptrCast(@alignCast(ptr));
        const values = try alloc.alloc(f32, dims);
        errdefer alloc.free(values);

        var hash = self.seed;
        for (text) |byte| {
            hash = (hash ^ byte) *% 0x100000001b3;
        }

        for (values, 0..) |*value, i| {
            hash = (hash ^ @as(u64, @intCast(i + 1))) *% 0x9e3779b185ebca87;
            const lane: u32 = @intCast((hash >> 16) & 0xffff);
            value.* = @as(f32, @floatFromInt(lane)) / 65535.0;
        }
        return values;
    }

    pub fn interface(self: *DeterministicDenseEmbedder) DenseEmbedder {
        return .{
            .ptr = self,
            .dense_embed_fn = embedDense,
            .deinit_fn = null,
        };
    }
};

pub const DeterministicSparseEmbedder = struct {
    seed: u64 = 0xcbf29ce484222325,

    pub fn embedSparse(ptr: *anyopaque, alloc: Allocator, _: []const u8, text: []const u8) !SparseEmbedding {
        const self: *DeterministicSparseEmbedder = @ptrCast(@alignCast(ptr));

        var hash = self.seed;
        for (text) |byte| {
            hash = (hash ^ byte) *% 0x100000001b3;
        }

        const indices = try alloc.alloc(u32, 2);
        errdefer alloc.free(indices);
        const values = try alloc.alloc(f32, 2);
        errdefer alloc.free(values);

        indices[0] = @intCast((hash >> 16) % 1024);
        hash = (hash ^ 0x9e3779b185ebca87) *% 0x100000001b3;
        indices[1] = @intCast((hash >> 24) % 1024);
        if (indices[1] == indices[0]) indices[1] = (indices[1] + 1) % 1024;
        if (indices[1] < indices[0]) std.mem.swap(u32, &indices[0], &indices[1]);

        values[0] = @as(f32, @floatFromInt((hash >> 8) & 0xffff)) / 65535.0;
        hash = (hash ^ 0x517cc1b727220a95) *% 0x9e3779b185ebca87;
        values[1] = @as(f32, @floatFromInt((hash >> 12) & 0xffff)) / 65535.0;
        return .{
            .indices = indices,
            .values = values,
        };
    }

    pub fn interface(self: *DeterministicSparseEmbedder) SparseEmbedder {
        return .{
            .ptr = self,
            .sparse_embed_fn = embedSparse,
            .deinit_fn = null,
        };
    }
};

test "deterministic dense embedder is stable for same input" {
    const alloc = std.testing.allocator;
    var embedder = DeterministicDenseEmbedder{};
    const iface = embedder.interface();

    const a = try iface.embedDense(alloc, "", "hello world", 4);
    defer alloc.free(a);
    const b = try iface.embedDense(alloc, "", "hello world", 4);
    defer alloc.free(b);

    try std.testing.expectEqual(@as(usize, 4), a.len);
    try std.testing.expectEqualSlices(f32, a, b);
}

test "deterministic dense embedder batch fallback is stable" {
    const alloc = std.testing.allocator;
    var embedder = DeterministicDenseEmbedder{};
    const iface = embedder.interface();

    const batch = try iface.embedDenseBatch(alloc, "", &.{ "hello world", "zig batch" }, 4);
    defer freeDenseEmbeddingBatch(alloc, batch);

    try std.testing.expectEqual(@as(usize, 2), batch.len);
    try std.testing.expectEqual(@as(usize, 4), batch[0].len);
    const single = try iface.embedDense(alloc, "", "hello world", 4);
    defer alloc.free(single);
    try std.testing.expectEqualSlices(f32, single, batch[0]);
}

test "deterministic sparse embedder is stable for same input" {
    const alloc = std.testing.allocator;
    var embedder = DeterministicSparseEmbedder{};
    const iface = embedder.interface();

    var a = try iface.embedSparse(alloc, "", "hello world");
    defer a.deinit(alloc);
    var b = try iface.embedSparse(alloc, "", "hello world");
    defer b.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), a.indices.len);
    try std.testing.expectEqualSlices(u32, a.indices, b.indices);
    try std.testing.expectEqualSlices(f32, a.values, b.values);
}

test "deterministic sparse embedder batch fallback is stable" {
    const alloc = std.testing.allocator;
    var embedder = DeterministicSparseEmbedder{};
    const iface = embedder.interface();

    const batch = try iface.embedSparseBatch(alloc, "", &.{ "hello world", "zig batch" });
    defer freeSparseEmbeddingBatch(alloc, batch);

    try std.testing.expectEqual(@as(usize, 2), batch.len);
    var single = try iface.embedSparse(alloc, "", "hello world");
    defer single.deinit(alloc);
    try std.testing.expectEqualSlices(u32, single.indices, batch[0].indices);
    try std.testing.expectEqualSlices(f32, single.values, batch[0].values);
}
