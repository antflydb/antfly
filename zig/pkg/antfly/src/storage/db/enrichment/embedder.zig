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
const utf8_text = @import("utf8_text.zig");
const template_mod = if (builtin.os.tag == .freestanding or builtin.is_test or build_options.bench_minimal_deps)
    @import("../template_stub.zig")
else
    @import("../../../template.zig");
const inference_work = @import("../../../inference/work.zig");

pub const DenseEmbedFn = *const fn (ptr: *anyopaque, alloc: Allocator, embedding_name: []const u8, text: []const u8, dims: u32) anyerror![]f32;
pub const DenseEmbedBatchFn = *const fn (ptr: *anyopaque, alloc: Allocator, embedding_name: []const u8, texts: []const []const u8, dims: u32) anyerror![]const []const f32;
pub const DenseEmbedPartsFn = *const fn (ptr: *anyopaque, alloc: Allocator, embedding_name: []const u8, parts: []const template_mod.ContentPart, dims: u32) anyerror![]f32;
/// Embeds each content part as an independently addressable work item. This is
/// the document-page path: a window of page images produces one vector per
/// page rather than an implicit document-level pool.
pub const DenseEmbedPartItemsFn = *const fn (ptr: *anyopaque, alloc: Allocator, embedding_name: []const u8, items: []const template_mod.ContentPart, dims: u32) anyerror![]const []const f32;
pub const DenseMediaPartLimitFn = *const fn (ptr: *anyopaque, embedding_name: []const u8) ?usize;
pub const DenseCapabilitiesFn = *const fn (ptr: *anyopaque, alloc: Allocator, embedding_name: []const u8) anyerror!inference_work.InferenceCapabilities;
pub const DensePartInvocationMemory = inference_work.InvocationMemoryPlan;
pub const DensePartInvocationShape = struct {
    item_count: usize,
    /// Exact fixed JSON envelopes for the concrete part variants, excluding
    /// binary payload data whose representation is charged by the attachment
    /// transport.
    item_envelope_json_bytes: usize = 0,
    /// Sum of conservative JSON-string upper bounds for text, URLs, and MIME
    /// values. Keeping this aggregate preserves heterogeneous accounting
    /// without retaining or allocating a synthetic common MIME type.
    string_json_bytes: usize = 0,
    /// Host-side normalization structures and replacement buffers that can be
    /// live alongside the concrete invocation. This belongs in the route plan,
    /// not in a private execution-only allowance, so scheduler admission and
    /// the bounded executor observe the same peak.
    preparation_bytes: usize = 0,
};
pub const DensePartInvocationMemoryFn = *const fn (
    ptr: *anyopaque,
    embedding_name: []const u8,
    shape: DensePartInvocationShape,
    dims: u32,
) anyerror!DensePartInvocationMemory;
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
    dense_embed_part_items_fn: ?DenseEmbedPartItemsFn = null,
    media_part_limit_fn: ?DenseMediaPartLimitFn = null,
    capabilities_fn: ?DenseCapabilitiesFn = null,
    part_invocation_memory_fn: ?DensePartInvocationMemoryFn = null,
    deinit_fn: ?DenseEmbedDeinitFn = null,
    /// The implementation guarantees that each provider invocation has its
    /// own finite deadline. Foreground post-commit replay rejects legacy
    /// implementations that cannot make this guarantee; background replay
    /// remains source-compatible.
    foreground_bounded: bool = false,

    pub fn embedDense(self: DenseEmbedder, alloc: Allocator, embedding_name: []const u8, text: []const u8, dims: u32) ![]f32 {
        var sanitized = try utf8_text.sanitizeWithoutSourceMapAlloc(alloc, text, "dense embedder");
        defer sanitized.deinit(alloc);
        return try self.dense_embed_fn(self.ptr, alloc, embedding_name, sanitized.text, dims);
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

    pub fn supportsPartItems(self: DenseEmbedder) bool {
        return self.dense_embed_part_items_fn != null;
    }

    pub fn mediaPartLimit(self: DenseEmbedder, embedding_name: []const u8) ?usize {
        const media_part_limit_fn = self.media_part_limit_fn orelse return null;
        return media_part_limit_fn(self.ptr, embedding_name);
    }

    pub fn capabilities(self: DenseEmbedder, alloc: Allocator, embedding_name: []const u8) !inference_work.InferenceCapabilities {
        const capabilities_fn = self.capabilities_fn orelse return error.EmbeddingCapabilitiesUnavailable;
        const result = try capabilities_fn(self.ptr, alloc, embedding_name);
        try result.validate();
        if (result.task != .embed) return error.InvalidInferenceCapabilities;
        return result;
    }

    /// Return the concrete route's complete non-media peak and attachment
    /// representation. A media-capable implementation must publish this
    /// contract: silently assuming borrowed input would make admission depend
    /// on an implementation detail that can change at runtime.
    pub fn partInvocationMemory(
        self: DenseEmbedder,
        embedding_name: []const u8,
        shape: DensePartInvocationShape,
        dims: u32,
    ) !DensePartInvocationMemory {
        const memory_fn = self.part_invocation_memory_fn orelse
            return error.InferenceInvocationMemoryUnavailable;
        const plan = try memory_fn(self.ptr, embedding_name, shape, dims);
        try plan.validate();
        return plan;
    }

    /// Planning helper for document renderers that know the output MIME before
    /// page bytes exist. Runtime execution derives the same shape from the
    /// concrete heterogeneous item list.
    pub fn partInvocationMemoryForMime(
        self: DenseEmbedder,
        embedding_name: []const u8,
        item_count: usize,
        mime_type: []const u8,
        dims: u32,
    ) !DensePartInvocationMemory {
        _ = inference_work.mimeTypeEssence(mime_type) catch
            return error.UnsupportedInferenceMimeType;
        const one_mime = try jsonStringUpperBound(mime_type);
        return try self.partInvocationMemory(embedding_name, .{
            .item_count = item_count,
            .item_envelope_json_bytes = std.math.mul(
                usize,
                item_count,
                "{\"type\":\"media\",\"data\":\"\",\"mime_type\":}".len,
            ) catch return error.InferenceEncodedBytesExceeded,
            .string_json_bytes = std.math.mul(usize, item_count, one_mime) catch
                return error.InferenceEncodedBytesExceeded,
        }, dims);
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

    pub fn embedDensePartItems(
        self: DenseEmbedder,
        alloc: Allocator,
        embedding_name: []const u8,
        items: []const template_mod.ContentPart,
        dims: u32,
    ) ![]const []const f32 {
        const embed_items = self.dense_embed_part_items_fn orelse return error.UnsupportedEmbeddingProvider;
        const invocation_plan = try self.partInvocationMemory(
            embedding_name,
            try densePartInvocationShape(items),
            dims,
        );
        var transport_copy_bytes: usize = 0;
        for (items) |item| switch (item) {
            .binary => |binary| {
                const resident = try invocation_plan.attachment_transport.peakResidentSize(binary.data.len, binary.mime_type.len);
                transport_copy_bytes = std.math.add(
                    usize,
                    transport_copy_bytes,
                    resident - binary.data.len,
                ) catch return error.InferenceEncodedBytesExceeded;
            },
            else => {},
        };
        const invocation_limit = std.math.add(
            usize,
            invocation_plan.allocator_limit_bytes,
            transport_copy_bytes,
        ) catch return error.InferenceEncodedBytesExceeded;
        var bounded = inference_work.BoundedInvocationAllocator.init(alloc, invocation_limit);
        const invocation_alloc = bounded.allocator();
        var sanitized = sanitizeContentPartsForEmbeddingAlloc(invocation_alloc, items) catch |err| {
            if (bounded.limit_exceeded) return error.InferenceInvocationMemoryExceeded;
            return err;
        };
        defer sanitized.deinit(invocation_alloc);
        const safe_items = sanitized.partsSlice();
        const callback_alloc = if (invocation_plan.allocator_owner == .caller)
            invocation_alloc
        else
            alloc;
        const vectors = embed_items(self.ptr, callback_alloc, embedding_name, safe_items, dims) catch |err| {
            if (invocation_plan.allocator_owner == .caller and bounded.limit_exceeded)
                return error.InferenceInvocationMemoryExceeded;
            return err;
        };
        if (vectors.len != items.len) {
            freeDenseEmbeddingBatch(alloc, vectors);
            return error.InvalidEmbeddingResponse;
        }
        var actual_values: usize = 0;
        for (vectors) |vector| {
            if (vector.len != dims) {
                freeDenseEmbeddingBatch(alloc, vectors);
                return error.InvalidEmbeddingDimensions;
            }
            for (vector) |value| if (!std.math.isFinite(value)) {
                freeDenseEmbeddingBatch(alloc, vectors);
                return error.InvalidEmbeddingResponse;
            };
            const item_bytes = std.math.mul(usize, vector.len, @sizeOf(f32)) catch {
                freeDenseEmbeddingBatch(alloc, vectors);
                return error.InvalidEmbeddingResponse;
            };
            if (item_bytes > invocation_plan.max_result_bytes_per_item) {
                freeDenseEmbeddingBatch(alloc, vectors);
                return error.InferenceResultTooLarge;
            }
            actual_values = std.math.add(usize, actual_values, vector.len) catch {
                freeDenseEmbeddingBatch(alloc, vectors);
                return error.InvalidEmbeddingResponse;
            };
        }
        const result_bytes = std.math.mul(usize, actual_values, @sizeOf(f32)) catch {
            freeDenseEmbeddingBatch(alloc, vectors);
            return error.InvalidEmbeddingResponse;
        };
        if (result_bytes > invocation_plan.max_result_bytes) {
            freeDenseEmbeddingBatch(alloc, vectors);
            return error.InferenceResultTooLarge;
        }
        return vectors;
    }

    pub fn deinit(self: DenseEmbedder, alloc: Allocator) void {
        const deinit_fn = self.deinit_fn orelse return;
        deinit_fn(self.ptr, alloc);
    }
};

fn jsonStringUpperBound(value: []const u8) !usize {
    const escaped = std.math.mul(usize, value.len, 6) catch
        return error.InferenceEncodedBytesExceeded;
    return std.math.add(usize, escaped, 2) catch
        error.InferenceEncodedBytesExceeded;
}

fn densePartInvocationShape(parts: []const template_mod.ContentPart) !DensePartInvocationShape {
    var item_envelope_json_bytes: usize = 0;
    var string_json_bytes: usize = 0;
    for (parts) |part| switch (part) {
        .binary => |binary| {
            _ = inference_work.mimeTypeEssence(binary.mime_type) catch
                return error.UnsupportedInferenceMimeType;
            item_envelope_json_bytes = std.math.add(
                usize,
                item_envelope_json_bytes,
                "{\"type\":\"media\",\"data\":\"\",\"mime_type\":}".len,
            ) catch return error.InferenceEncodedBytesExceeded;
            string_json_bytes = std.math.add(
                usize,
                string_json_bytes,
                try jsonStringUpperBound(binary.mime_type),
            ) catch return error.InferenceEncodedBytesExceeded;
        },
        .media_url => |url| {
            item_envelope_json_bytes = std.math.add(
                usize,
                item_envelope_json_bytes,
                "{\"type\":\"image_url\",\"image_url\":{\"url\":}}".len,
            ) catch return error.InferenceEncodedBytesExceeded;
            string_json_bytes = std.math.add(
                usize,
                string_json_bytes,
                try jsonStringUpperBound(url),
            ) catch return error.InferenceEncodedBytesExceeded;
        },
        .text => |text| {
            item_envelope_json_bytes = std.math.add(
                usize,
                item_envelope_json_bytes,
                "{\"type\":\"text\",\"text\":}".len,
            ) catch return error.InferenceEncodedBytesExceeded;
            string_json_bytes = std.math.add(
                usize,
                string_json_bytes,
                try jsonStringUpperBound(text),
            ) catch return error.InferenceEncodedBytesExceeded;
        },
    };
    return .{
        .item_count = parts.len,
        .item_envelope_json_bytes = item_envelope_json_bytes,
        .string_json_bytes = string_json_bytes,
        .preparation_bytes = try contentPartPreparationBytes(parts),
    };
}

fn contentPartPreparationBytes(parts: []const template_mod.ContentPart) !usize {
    var total: usize = 0;
    var has_invalid_text = false;
    for (parts) |part| switch (part) {
        .text => |value| {
            if (std.unicode.utf8ValidateSlice(value)) continue;
            has_invalid_text = true;
            // The repair helper sizes exactly before allocating. Invalid UTF-8
            // can still expand to one three-byte replacement scalar per source
            // byte, so reserve that complete final buffer.
            const sanitized = std.math.mul(usize, value.len, 3) catch
                return error.InferenceEncodedBytesExceeded;
            total = std.math.add(usize, total, sanitized) catch
                return error.InferenceEncodedBytesExceeded;
        },
        .media_url => {},
        .binary => {},
    };
    if (has_invalid_text) {
        total = std.math.add(
            usize,
            total,
            std.math.mul(usize, parts.len, @sizeOf(template_mod.ContentPart)) catch
                return error.InferenceEncodedBytesExceeded,
        ) catch return error.InferenceEncodedBytesExceeded;
        total = std.math.add(
            usize,
            total,
            std.math.mul(usize, parts.len, @sizeOf(?[]u8)) catch
                return error.InferenceEncodedBytesExceeded,
        ) catch return error.InferenceEncodedBytesExceeded;
    }
    return total;
}

pub const SparseEmbedder = struct {
    ptr: *anyopaque,
    sparse_embed_fn: SparseEmbedFn,
    sparse_embed_batch_fn: ?SparseEmbedBatchFn = null,
    deinit_fn: ?SparseEmbedDeinitFn = null,
    foreground_bounded: bool = false,

    pub fn embedSparse(self: SparseEmbedder, alloc: Allocator, embedding_name: []const u8, text: []const u8) !SparseEmbedding {
        var sanitized = try utf8_text.sanitizeWithoutSourceMapAlloc(alloc, text, "sparse embedder");
        defer sanitized.deinit(alloc);
        return try self.sparse_embed_fn(self.ptr, alloc, embedding_name, sanitized.text);
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
        const safe_text = try utf8_text.replacementAlloc(alloc, text, "embedding batch");
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
                const safe_text = try utf8_text.replacementAlloc(alloc, text, "embedding content parts");
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

test "media part item embedding fails closed without an invocation contract" {
    const Stub = struct {
        fn dense(_: *anyopaque, alloc: Allocator, _: []const u8, _: []const u8, dims: u32) ![]f32 {
            return try alloc.alloc(f32, dims);
        }

        fn items(_: *anyopaque, alloc: Allocator, _: []const u8, parts: []const template_mod.ContentPart, dims: u32) ![]const []const f32 {
            const out = try alloc.alloc([]const f32, parts.len);
            errdefer alloc.free(out);
            for (out, 0..) |*vector, i| {
                vector.* = alloc.alloc(f32, dims) catch |err| {
                    for (out[0..i]) |initialized| alloc.free(initialized);
                    return err;
                };
            }
            return out;
        }
    };
    var context: u8 = 0;
    const embedder: DenseEmbedder = .{
        .ptr = &context,
        .dense_embed_fn = Stub.dense,
        .dense_embed_part_items_fn = Stub.items,
    };
    const parts = [_]template_mod.ContentPart{.{
        .binary = .{ .mime_type = "image/png", .data = &.{1} },
    }};
    try std.testing.expectError(
        error.InferenceInvocationMemoryUnavailable,
        embedder.embedDensePartItems(std.testing.allocator, "images", &parts, 1),
    );
    const url_parts = [_]template_mod.ContentPart{.{
        .media_url = "DATA:image/png;BASE64,AQ==",
    }};
    try std.testing.expectError(
        error.InferenceInvocationMemoryUnavailable,
        embedder.embedDensePartItems(std.testing.allocator, "images", &url_parts, 1),
    );
}

test "media part item embedding enforces its result contract" {
    const Stub = struct {
        fn dense(_: *anyopaque, alloc: Allocator, _: []const u8, _: []const u8, dims: u32) ![]f32 {
            return try alloc.alloc(f32, dims);
        }

        fn items(_: *anyopaque, alloc: Allocator, _: []const u8, parts: []const template_mod.ContentPart, dims: u32) ![]const []const f32 {
            const out = try alloc.alloc([]const f32, parts.len);
            errdefer alloc.free(out);
            for (out, 0..) |*vector, i| {
                vector.* = alloc.alloc(f32, dims) catch |err| {
                    for (out[0..i]) |initialized| alloc.free(initialized);
                    return err;
                };
            }
            return out;
        }

        fn memory(_: *anyopaque, _: []const u8, _: DensePartInvocationShape, _: u32) !DensePartInvocationMemory {
            return .{
                .attachment_transport = .borrowed_binary,
                .fixed_bytes = 64,
                .allocator_limit_bytes = 64,
                .max_result_bytes_per_item = 3,
                .max_result_bytes = 3,
            };
        }
    };
    var context: u8 = 0;
    const embedder: DenseEmbedder = .{
        .ptr = &context,
        .dense_embed_fn = Stub.dense,
        .dense_embed_part_items_fn = Stub.items,
        .part_invocation_memory_fn = Stub.memory,
    };
    const parts = [_]template_mod.ContentPart{.{
        .binary = .{ .mime_type = "image/png", .data = &.{1} },
    }};
    try std.testing.expectError(
        error.InferenceResultTooLarge,
        embedder.embedDensePartItems(std.testing.allocator, "images", &parts, 1),
    );
    const url_parts = [_]template_mod.ContentPart{.{
        .media_url = "DATA:image/png;BASE64,AQ==",
    }};
    try std.testing.expectError(
        error.InferenceResultTooLarge,
        embedder.embedDensePartItems(std.testing.allocator, "images", &url_parts, 1),
    );
}

test "media part item embedding validates each vector shape and values" {
    const Context = struct { non_finite: bool = false };
    const Stub = struct {
        fn dense(_: *anyopaque, alloc: Allocator, _: []const u8, _: []const u8, dims: u32) ![]f32 {
            return try alloc.alloc(f32, dims);
        }

        fn items(ptr: *anyopaque, alloc: Allocator, _: []const u8, _: []const template_mod.ContentPart, _: u32) ![]const []const f32 {
            const context: *Context = @ptrCast(@alignCast(ptr));
            const out = try alloc.alloc([]const f32, 2);
            errdefer alloc.free(out);
            out[0] = try alloc.alloc(f32, if (context.non_finite) 2 else 1);
            errdefer alloc.free(@constCast(out[0]));
            out[1] = try alloc.alloc(f32, if (context.non_finite) 2 else 3);
            @memset(@constCast(out[0]), 0);
            @memset(@constCast(out[1]), 0);
            if (context.non_finite) @constCast(out[1])[0] = std.math.nan(f32);
            return out;
        }

        fn memory(_: *anyopaque, _: []const u8, _: DensePartInvocationShape, _: u32) !DensePartInvocationMemory {
            return .{
                .attachment_transport = .borrowed_binary,
                .fixed_bytes = 256,
                .allocator_limit_bytes = 256,
                .max_result_bytes_per_item = 16,
                .max_result_bytes = 32,
            };
        }
    };
    var context = Context{};
    const embedder: DenseEmbedder = .{
        .ptr = &context,
        .dense_embed_fn = Stub.dense,
        .dense_embed_part_items_fn = Stub.items,
        .part_invocation_memory_fn = Stub.memory,
    };
    const parts = [_]template_mod.ContentPart{
        .{ .binary = .{ .mime_type = "image/png", .data = &.{1} } },
        .{ .binary = .{ .mime_type = "image/jpeg", .data = &.{2} } },
    };
    try std.testing.expectError(
        error.InvalidEmbeddingDimensions,
        embedder.embedDensePartItems(std.testing.allocator, "images", &parts, 2),
    );
    context.non_finite = true;
    try std.testing.expectError(
        error.InvalidEmbeddingResponse,
        embedder.embedDensePartItems(std.testing.allocator, "images", &parts, 2),
    );
}

test "media part item embedding uses concrete executor-owned admission" {
    const Stub = struct {
        fn dense(_: *anyopaque, alloc: Allocator, _: []const u8, _: []const u8, dims: u32) ![]f32 {
            return try alloc.alloc(f32, dims);
        }

        fn items(_: *anyopaque, alloc: Allocator, _: []const u8, parts: []const template_mod.ContentPart, dims: u32) ![]const []const f32 {
            const scratch = try alloc.alloc(u8, 4096);
            defer alloc.free(scratch);
            const out = try alloc.alloc([]const f32, parts.len);
            errdefer alloc.free(out);
            for (out, 0..) |*vector, i| {
                vector.* = alloc.alloc(f32, dims) catch |err| {
                    for (out[0..i]) |initialized| alloc.free(initialized);
                    return err;
                };
                @memset(@constCast(vector.*), 0);
            }
            return out;
        }

        fn memory(_: *anyopaque, _: []const u8, shape: DensePartInvocationShape, dims: u32) !DensePartInvocationMemory {
            const per_item = @as(usize, dims) * @sizeOf(f32);
            return .{
                .attachment_transport = .borrowed_binary,
                .fixed_bytes = 64,
                .allocator_limit_bytes = 64,
                .allocator_owner = .executor,
                .max_result_bytes_per_item = per_item,
                .max_result_bytes = per_item * shape.item_count,
            };
        }
    };
    var context: u8 = 0;
    const embedder: DenseEmbedder = .{
        .ptr = &context,
        .dense_embed_fn = Stub.dense,
        .dense_embed_part_items_fn = Stub.items,
        .part_invocation_memory_fn = Stub.memory,
    };
    const parts = [_]template_mod.ContentPart{.{
        .binary = .{ .mime_type = "image/png; charset=binary", .data = &.{1} },
    }};
    const vectors = try embedder.embedDensePartItems(std.testing.allocator, "images", &parts, 1);
    defer freeDenseEmbeddingBatch(std.testing.allocator, vectors);
    try std.testing.expectEqual(@as(usize, 1), vectors.len);
}

test "media part item embedding planning preserves heterogeneous MIME accounting" {
    const parts = [_]template_mod.ContentPart{
        .{ .binary = .{ .mime_type = "image/png", .data = &.{1} } },
        .{ .binary = .{ .mime_type = "audio/wav; codecs=pcm", .data = &.{2} } },
    };
    const shape = try densePartInvocationShape(&parts);
    try std.testing.expectEqual(@as(usize, 2), shape.item_count);
    try std.testing.expect(shape.string_json_bytes >= "image/png".len + "audio/wav; codecs=pcm".len);
    try std.testing.expectEqual(@as(usize, 0), shape.preparation_bytes);

    const invalid_text = [_]template_mod.ContentPart{.{ .text = &.{0xff} }};
    const repaired_shape = try densePartInvocationShape(&invalid_text);
    try std.testing.expect(repaired_shape.preparation_bytes >=
        @sizeOf(template_mod.ContentPart) + @sizeOf(?[]u8) + 3);
}
