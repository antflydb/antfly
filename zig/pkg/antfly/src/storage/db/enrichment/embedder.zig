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
        const sanitized = try sanitizeUtf8ForEmbeddingAlloc(alloc, text);
        defer if (sanitized) |owned| alloc.free(owned);
        return try self.dense_embed_fn(self.ptr, alloc, embedding_name, sanitized orelse text, dims);
    }

    pub fn embedDenseBatch(
        self: DenseEmbedder,
        alloc: Allocator,
        embedding_name: []const u8,
        texts: []const []const u8,
        dims: u32,
    ) ![]const []const f32 {
        var sanitized = try sanitizeUtf8BatchForEmbeddingAlloc(alloc, texts);
        defer sanitized.deinit(alloc);
        const safe_texts = sanitized.texts();
        const dense_embed_batch_fn = self.dense_embed_batch_fn orelse return try fallbackDenseBatch(self, alloc, embedding_name, safe_texts, dims);
        return try dense_embed_batch_fn(self.ptr, alloc, embedding_name, safe_texts, dims);
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
        var sanitized = try sanitizeContentPartsForEmbeddingAlloc(alloc, parts);
        defer sanitized.deinit(alloc);
        return try dense_embed_parts_fn(self.ptr, alloc, embedding_name, sanitized.partsSlice(), dims);
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
        const sanitized = try sanitizeUtf8ForEmbeddingAlloc(alloc, text);
        defer if (sanitized) |owned| alloc.free(owned);
        return try self.sparse_embed_fn(self.ptr, alloc, embedding_name, sanitized orelse text);
    }

    pub fn embedSparseBatch(
        self: SparseEmbedder,
        alloc: Allocator,
        embedding_name: []const u8,
        texts: []const []const u8,
    ) ![]SparseEmbedding {
        var sanitized = try sanitizeUtf8BatchForEmbeddingAlloc(alloc, texts);
        defer sanitized.deinit(alloc);
        const safe_texts = sanitized.texts();
        const sparse_embed_batch_fn = self.sparse_embed_batch_fn orelse return try fallbackSparseBatch(self, alloc, embedding_name, safe_texts);
        return try sparse_embed_batch_fn(self.ptr, alloc, embedding_name, safe_texts);
    }

    pub fn deinit(self: SparseEmbedder, alloc: Allocator) void {
        const deinit_fn = self.deinit_fn orelse return;
        deinit_fn(self.ptr, alloc);
    }
};

fn sanitizeUtf8ForEmbeddingAlloc(alloc: Allocator, text: []const u8) !?[]u8 {
    if (std.unicode.utf8ValidateSlice(text)) return null;
    return try std.fmt.allocPrint(alloc, "{f}", .{std.unicode.fmtUtf8(text)});
}

const SanitizedTextBatch = struct {
    original: []const []const u8,
    sanitized: ?[][]const u8 = null,
    owned: ?[]?[]u8 = null,

    fn texts(self: @This()) []const []const u8 {
        return self.sanitized orelse self.original;
    }

    fn deinit(self: *@This(), alloc: Allocator) void {
        if (self.owned) |owned| {
            for (owned) |maybe_text| {
                if (maybe_text) |text| alloc.free(text);
            }
            alloc.free(owned);
        }
        if (self.sanitized) |sanitized| alloc.free(sanitized);
        self.* = undefined;
    }
};

const SanitizedContentParts = struct {
    original: []const template_mod.ContentPart,
    parts: ?[]template_mod.ContentPart = null,
    owned_texts: ?[]?[]u8 = null,

    fn partsSlice(self: @This()) []const template_mod.ContentPart {
        return self.parts orelse self.original;
    }

    fn deinit(self: *@This(), alloc: Allocator) void {
        if (self.owned_texts) |owned_texts| {
            for (owned_texts) |maybe_text| {
                if (maybe_text) |text| alloc.free(text);
            }
            alloc.free(owned_texts);
        }
        if (self.parts) |parts| alloc.free(parts);
        self.* = undefined;
    }
};

fn sanitizeUtf8BatchForEmbeddingAlloc(alloc: Allocator, texts: []const []const u8) !SanitizedTextBatch {
    var has_invalid = false;
    for (texts) |text| {
        if (!std.unicode.utf8ValidateSlice(text)) {
            has_invalid = true;
            break;
        }
    }
    if (!has_invalid) return .{ .original = texts };

    const sanitized = try alloc.alloc([]const u8, texts.len);
    errdefer alloc.free(sanitized);
    const owned = try alloc.alloc(?[]u8, texts.len);
    @memset(owned, null);
    errdefer {
        for (owned) |maybe_text| {
            if (maybe_text) |text| alloc.free(text);
        }
        alloc.free(owned);
    }

    for (texts, 0..) |text, i| {
        if (std.unicode.utf8ValidateSlice(text)) {
            sanitized[i] = text;
            continue;
        }
        const safe_text = try std.fmt.allocPrint(alloc, "{f}", .{std.unicode.fmtUtf8(text)});
        owned[i] = safe_text;
        sanitized[i] = safe_text;
    }

    return .{
        .original = texts,
        .sanitized = sanitized,
        .owned = owned,
    };
}

fn sanitizeContentPartsForEmbeddingAlloc(alloc: Allocator, parts: []const template_mod.ContentPart) !SanitizedContentParts {
    var has_invalid = false;
    for (parts) |part| {
        switch (part) {
            .text => |text| {
                if (!std.unicode.utf8ValidateSlice(text)) {
                    has_invalid = true;
                    break;
                }
            },
            .media_url, .binary => {},
        }
    }
    if (!has_invalid) return .{ .original = parts };

    const sanitized_parts = try alloc.alloc(template_mod.ContentPart, parts.len);
    errdefer alloc.free(sanitized_parts);
    const owned_texts = try alloc.alloc(?[]u8, parts.len);
    @memset(owned_texts, null);
    errdefer {
        for (owned_texts) |maybe_text| {
            if (maybe_text) |text| alloc.free(text);
        }
        alloc.free(owned_texts);
    }

    for (parts, 0..) |part, i| {
        switch (part) {
            .text => |text| {
                if (std.unicode.utf8ValidateSlice(text)) {
                    sanitized_parts[i] = part;
                    continue;
                }
                const safe_text = try std.fmt.allocPrint(alloc, "{f}", .{std.unicode.fmtUtf8(text)});
                owned_texts[i] = safe_text;
                sanitized_parts[i] = .{ .text = safe_text };
            },
            .media_url, .binary => sanitized_parts[i] = part,
        }
    }

    return .{
        .original = parts,
        .parts = sanitized_parts,
        .owned_texts = owned_texts,
    };
}

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

const Utf8AssertingEmbedder = struct {
    saw_replacement: bool = false,

    fn dense(ptr: *anyopaque, alloc: Allocator, _: []const u8, text: []const u8, dims: u32) ![]f32 {
        const self: *Utf8AssertingEmbedder = @ptrCast(@alignCast(ptr));
        try std.testing.expect(std.unicode.utf8ValidateSlice(text));
        if (std.mem.indexOf(u8, text, &std.unicode.replacement_character_utf8) != null) self.saw_replacement = true;
        const vector = try alloc.alloc(f32, dims);
        @memset(vector, 1.0);
        return vector;
    }

    fn sparseBatch(ptr: *anyopaque, alloc: Allocator, _: []const u8, texts: []const []const u8) ![]SparseEmbedding {
        const self: *Utf8AssertingEmbedder = @ptrCast(@alignCast(ptr));
        const out = try alloc.alloc(SparseEmbedding, texts.len);
        var initialized: usize = 0;
        errdefer {
            for (out[0..initialized]) |*embedding| embedding.deinit(alloc);
            alloc.free(out);
        }
        for (texts, 0..) |text, i| {
            try std.testing.expect(std.unicode.utf8ValidateSlice(text));
            if (std.mem.indexOf(u8, text, &std.unicode.replacement_character_utf8) != null) self.saw_replacement = true;
            const indices = try alloc.dupe(u32, &.{1});
            const values = alloc.dupe(f32, &.{1.0}) catch |err| {
                alloc.free(indices);
                return err;
            };
            out[i] = .{
                .indices = indices,
                .values = values,
            };
            initialized += 1;
        }
        return out;
    }

    fn denseParts(ptr: *anyopaque, alloc: Allocator, _: []const u8, parts: []const template_mod.ContentPart, dims: u32) ![]f32 {
        const self: *Utf8AssertingEmbedder = @ptrCast(@alignCast(ptr));
        for (parts) |part| {
            switch (part) {
                .text => |text| {
                    try std.testing.expect(std.unicode.utf8ValidateSlice(text));
                    if (std.mem.indexOf(u8, text, &std.unicode.replacement_character_utf8) != null) self.saw_replacement = true;
                },
                .media_url, .binary => {},
            }
        }
        const vector = try alloc.alloc(f32, dims);
        @memset(vector, 1.0);
        return vector;
    }

    fn denseInterface(self: *Utf8AssertingEmbedder) DenseEmbedder {
        return .{
            .ptr = self,
            .dense_embed_fn = dense,
            .dense_embed_parts_fn = denseParts,
        };
    }

    fn sparseInterface(self: *Utf8AssertingEmbedder) SparseEmbedder {
        return .{
            .ptr = self,
            .sparse_embed_fn = sparse,
            .sparse_embed_batch_fn = sparseBatch,
        };
    }

    fn sparse(ptr: *anyopaque, alloc: Allocator, embedding_name: []const u8, text: []const u8) !SparseEmbedding {
        const batch = try sparseBatch(ptr, alloc, embedding_name, &.{text});
        defer alloc.free(batch);
        return batch[0];
    }
};

test "enrichment dense embedder replaces invalid utf8 before provider call" {
    const alloc = std.testing.allocator;
    var embedder = Utf8AssertingEmbedder{};
    const vector = try embedder.denseInterface().embedDense(alloc, "", "alpha\xc2 beta", 1);
    defer alloc.free(vector);

    try std.testing.expect(embedder.saw_replacement);
}

test "enrichment sparse batch embedder replaces invalid utf8 before provider call" {
    const alloc = std.testing.allocator;
    var embedder = Utf8AssertingEmbedder{};
    const batch = try embedder.sparseInterface().embedSparseBatch(alloc, "", &.{ "valid", "alpha\xc2 beta" });
    defer freeSparseEmbeddingBatch(alloc, batch);

    try std.testing.expect(embedder.saw_replacement);
}

test "enrichment dense parts embedder replaces invalid text part utf8 before provider call" {
    const alloc = std.testing.allocator;
    var embedder = Utf8AssertingEmbedder{};
    const parts = [_]template_mod.ContentPart{.{ .text = "alpha\xc2 beta" }};
    const vector = try embedder.denseInterface().embedDenseParts(alloc, "", &parts, 1);
    defer alloc.free(vector);

    try std.testing.expect(embedder.saw_replacement);
}
