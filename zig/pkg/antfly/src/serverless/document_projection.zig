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
const Allocator = std.mem.Allocator;

pub const Projection = struct {
    text: []u8,
    embedding: ?[]f32 = null,
    sparse_embedding: ?[]SparseTermWeight = null,
    named_embeddings: ?[]NamedEmbedding = null,
    named_sparse_embeddings: ?[]NamedSparseEmbedding = null,
    graph_edges_json: ?[]u8 = null,
    chunk_preview: ?[][]u8 = null,
    chunk_embeddings: ?[]ChunkEmbedding = null,
    rerank_terms: ?[][]u8 = null,
    lexical_sparse_version: ?u32 = null,
    chunk_preview_version: ?u32 = null,
    chunk_embeddings_version: ?u32 = null,
    rerank_terms_version: ?u32 = null,

    pub fn deinit(self: *Projection, alloc: Allocator) void {
        alloc.free(self.text);
        if (self.embedding) |embedding| alloc.free(embedding);
        if (self.sparse_embedding) |weights| {
            for (weights) |weight| alloc.free(weight.term);
            alloc.free(weights);
        }
        if (self.named_embeddings) |embeddings| {
            for (embeddings) |*embedding| embedding.deinit(alloc);
            alloc.free(embeddings);
        }
        if (self.named_sparse_embeddings) |embeddings| {
            for (embeddings) |*embedding| embedding.deinit(alloc);
            alloc.free(embeddings);
        }
        if (self.graph_edges_json) |value| alloc.free(value);
        if (self.chunk_preview) |chunks| {
            for (chunks) |chunk| alloc.free(chunk);
            alloc.free(chunks);
        }
        if (self.chunk_embeddings) |chunk_embeddings| {
            for (chunk_embeddings) |*chunk_embedding| chunk_embedding.deinit(alloc);
            alloc.free(chunk_embeddings);
        }
        if (self.rerank_terms) |terms| {
            for (terms) |term| alloc.free(term);
            alloc.free(terms);
        }
        self.* = undefined;
    }

    pub fn vectorSource(self: *const Projection) VectorSource {
        if (self.chunk_embeddings) |chunk_embeddings| return .{ .chunk_embeddings = chunk_embeddings };
        if (self.embedding) |embedding| return .{ .top_level = embedding };
        return .none;
    }

    pub fn findNamedEmbedding(self: *const Projection, name: []const u8) ?[]const f32 {
        const embeddings = self.named_embeddings orelse return null;
        for (embeddings) |embedding| {
            if (std.mem.eql(u8, embedding.name, name)) return embedding.embedding;
        }
        return null;
    }

    pub fn findNamedSparseEmbedding(self: *const Projection, name: []const u8) ?[]const SparseTermWeight {
        const embeddings = self.named_sparse_embeddings orelse return null;
        for (embeddings) |embedding| {
            if (std.mem.eql(u8, embedding.name, name)) return embedding.weights;
        }
        return null;
    }
};

pub const VectorSource = union(enum) {
    none,
    top_level: []const f32,
    chunk_embeddings: []const ChunkEmbedding,

    pub fn dims(self: VectorSource) ?usize {
        return switch (self) {
            .none => null,
            .top_level => |embedding| embedding.len,
            .chunk_embeddings => |chunk_embeddings| if (chunk_embeddings.len == 0) null else chunk_embeddings[0].embedding.len,
        };
    }

    pub fn vectorCount(self: VectorSource) usize {
        return switch (self) {
            .none => 0,
            .top_level => 1,
            .chunk_embeddings => |chunk_embeddings| chunk_embeddings.len,
        };
    }
};

pub const SparseTermWeight = struct {
    term: []u8,
    weight: f32,
};

pub const ChunkEmbedding = struct {
    chunk: []u8,
    embedding: []f32,

    pub fn deinit(self: *ChunkEmbedding, alloc: Allocator) void {
        alloc.free(self.chunk);
        alloc.free(self.embedding);
        self.* = undefined;
    }
};

pub const NamedEmbedding = struct {
    name: []u8,
    embedding: []f32,

    pub fn deinit(self: *NamedEmbedding, alloc: Allocator) void {
        alloc.free(self.name);
        alloc.free(self.embedding);
        self.* = undefined;
    }
};

pub const NamedSparseEmbedding = struct {
    name: []u8,
    weights: []SparseTermWeight,

    pub fn deinit(self: *NamedSparseEmbedding, alloc: Allocator) void {
        alloc.free(self.name);
        for (self.weights) |weight| alloc.free(weight.term);
        alloc.free(self.weights);
        self.* = undefined;
    }
};

pub fn parseAlloc(alloc: Allocator, body: []const u8) !Projection {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{}) catch {
        return .{ .text = try alloc.dupe(u8, body) };
    };
    defer parsed.deinit();

    if (parsed.value != .object) return .{ .text = try alloc.dupe(u8, body) };
    const obj = parsed.value.object;

    const text = if (findString(obj, "text")) |value|
        try alloc.dupe(u8, value)
    else if (findString(obj, "body")) |value|
        try alloc.dupe(u8, value)
    else
        try alloc.dupe(u8, body);
    errdefer alloc.free(text);

    const embedding = if (findEmbedding(obj)) |value| try cloneEmbeddingAlloc(alloc, value) else null;
    errdefer if (embedding) |value| alloc.free(value);
    const sparse_embedding = if (findSparseEmbedding(obj)) |value| try cloneSparseEmbeddingAlloc(alloc, value) else null;
    errdefer if (sparse_embedding) |value| {
        for (value) |weight| alloc.free(weight.term);
        alloc.free(value);
    };
    const named_embeddings = try cloneNamedEmbeddingsAlloc(alloc, obj);
    errdefer if (named_embeddings) |value| {
        for (value) |*entry| entry.deinit(alloc);
        alloc.free(value);
    };
    const named_sparse_embeddings = try cloneNamedSparseEmbeddingsAlloc(alloc, obj);
    errdefer if (named_sparse_embeddings) |value| {
        for (value) |*entry| entry.deinit(alloc);
        alloc.free(value);
    };
    const graph_edges_json = try cloneGraphEdgesJsonAlloc(alloc, obj);
    errdefer if (graph_edges_json) |value| alloc.free(value);
    const chunk_preview = if (findStringArray(obj, "chunk_preview")) |value| try cloneStringArrayAlloc(alloc, value) else null;
    errdefer if (chunk_preview) |value| {
        for (value) |chunk| alloc.free(chunk);
        alloc.free(value);
    };
    const chunk_embeddings = if (findChunkEmbeddings(obj)) |value| try cloneChunkEmbeddingsAlloc(alloc, value) else null;
    errdefer if (chunk_embeddings) |value| {
        for (value) |*chunk_embedding| chunk_embedding.deinit(alloc);
        alloc.free(value);
    };
    const rerank_terms = if (findStringArray(obj, "rerank_terms")) |value| try cloneStringArrayAlloc(alloc, value) else null;
    return .{
        .text = text,
        .embedding = embedding,
        .sparse_embedding = sparse_embedding,
        .named_embeddings = named_embeddings,
        .named_sparse_embeddings = named_sparse_embeddings,
        .graph_edges_json = graph_edges_json,
        .chunk_preview = chunk_preview,
        .chunk_embeddings = chunk_embeddings,
        .rerank_terms = rerank_terms,
        .lexical_sparse_version = findLexicalSparseVersion(obj),
        .chunk_preview_version = findChunkPreviewVersion(obj),
        .chunk_embeddings_version = findChunkEmbeddingsVersion(obj),
        .rerank_terms_version = findRerankTermsVersion(obj),
    };
}

fn cloneNamedEmbeddingsAlloc(alloc: Allocator, obj: std.json.ObjectMap) !?[]NamedEmbedding {
    const named = obj.get("_embeddings") orelse return null;
    if (named != .object) return null;

    var count: usize = 0;
    var iter = named.object.iterator();
    while (iter.next()) |entry| {
        if (entry.value_ptr.* == .array) count += 1;
    }
    if (count == 0) return null;

    const out = try alloc.alloc(NamedEmbedding, count);
    errdefer alloc.free(out);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*embedding| embedding.deinit(alloc);
    }

    iter = named.object.iterator();
    while (iter.next()) |entry| {
        if (entry.value_ptr.* != .array) continue;
        out[initialized] = .{
            .name = try alloc.dupe(u8, entry.key_ptr.*),
            .embedding = try cloneEmbeddingAlloc(alloc, entry.value_ptr.array.items),
        };
        initialized += 1;
    }
    return out;
}

fn cloneNamedSparseEmbeddingsAlloc(alloc: Allocator, obj: std.json.ObjectMap) !?[]NamedSparseEmbedding {
    const named = obj.get("_embeddings") orelse return null;
    if (named != .object) return null;

    var count: usize = 0;
    var iter = named.object.iterator();
    while (iter.next()) |entry| {
        switch (entry.value_ptr.*) {
            .object => count += 1,
            .array => {
                if (entry.value_ptr.array.items.len > 0 and entry.value_ptr.array.items[0] == .object) count += 1;
            },
            else => {},
        }
    }
    if (count == 0) return null;

    const out = try alloc.alloc(NamedSparseEmbedding, count);
    errdefer alloc.free(out);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*embedding| embedding.deinit(alloc);
    }

    iter = named.object.iterator();
    while (iter.next()) |entry| {
        const weights = switch (entry.value_ptr.*) {
            .object => try cloneSparseEmbeddingAlloc(alloc, entry.value_ptr.*),
            .array => if (entry.value_ptr.array.items.len > 0 and entry.value_ptr.array.items[0] == .object)
                try cloneSparseEmbeddingAlloc(alloc, entry.value_ptr.*)
            else
                continue,
            else => continue,
        };
        out[initialized] = .{
            .name = try alloc.dupe(u8, entry.key_ptr.*),
            .weights = weights,
        };
        initialized += 1;
    }
    return out;
}

fn findString(obj: std.json.ObjectMap, key: []const u8) ?[]const u8 {
    const value = obj.get(key) orelse return null;
    if (value != .string) return null;
    return value.string;
}

fn findEmbedding(obj: std.json.ObjectMap) ?[]const std.json.Value {
    const keys = [_][]const u8{ "embedding", "_embedding" };
    for (keys) |key| {
        const value = obj.get(key) orelse continue;
        if (value != .array) continue;
        return value.array.items;
    }
    const named = obj.get("_embeddings") orelse return null;
    if (named != .object) return null;
    var iter = named.object.iterator();
    while (iter.next()) |entry| {
        if (entry.value_ptr.* != .array) continue;
        return entry.value_ptr.array.items;
    }
    return null;
}

fn findStringArray(obj: std.json.ObjectMap, key: []const u8) ?[]const std.json.Value {
    const value = obj.get(key) orelse return null;
    if (value != .array) return null;
    return value.array.items;
}

fn cloneGraphEdgesJsonAlloc(alloc: Allocator, obj: std.json.ObjectMap) !?[]u8 {
    const value = obj.get("graph_edges") orelse return null;
    var writer: std.Io.Writer.Allocating = .init(alloc);
    errdefer writer.deinit();
    try std.json.Stringify.value(value, .{}, &writer.writer);
    const encoded = try writer.toOwnedSlice();
    return encoded;
}

fn cloneEmbeddingAlloc(alloc: Allocator, values: []const std.json.Value) ![]f32 {
    const out = try alloc.alloc(f32, values.len);
    errdefer alloc.free(out);
    for (values, 0..) |value, idx| {
        out[idx] = switch (value) {
            .float => @floatCast(value.float),
            .integer => @floatFromInt(value.integer),
            .number_string => try std.fmt.parseFloat(f32, value.number_string),
            else => return error.InvalidEmbeddingValue,
        };
    }
    return out;
}

fn findSparseEmbedding(obj: std.json.ObjectMap) ?std.json.Value {
    const keys = [_][]const u8{ "sparse_embedding", "_sparse_embedding", "sparse_terms", "_sparse_terms" };
    for (keys) |key| {
        const value = obj.get(key) orelse continue;
        return value;
    }
    const named = obj.get("_embeddings") orelse return null;
    if (named != .object) return null;
    var iter = named.object.iterator();
    while (iter.next()) |entry| {
        if (entry.value_ptr.* == .object) return entry.value_ptr.*;
    }
    iter = named.object.iterator();
    while (iter.next()) |entry| {
        if (entry.value_ptr.* != .array) continue;
        if (entry.value_ptr.array.items.len == 0) continue;
        if (entry.value_ptr.array.items[0] != .object) continue;
        return entry.value_ptr.*;
    }
    return null;
}

fn findLexicalSparseVersion(obj: std.json.ObjectMap) ?u32 {
    const enrichment = obj.get("_enrichment") orelse return null;
    if (enrichment != .object) return null;
    const version = enrichment.object.get("lexical_sparse_version") orelse return null;
    return jsonValueAsU32(version) catch null;
}

fn findChunkPreviewVersion(obj: std.json.ObjectMap) ?u32 {
    const enrichment = obj.get("_enrichment") orelse return null;
    if (enrichment != .object) return null;
    const version = enrichment.object.get("chunk_preview_version") orelse return null;
    return jsonValueAsU32(version) catch null;
}

fn findChunkEmbeddings(obj: std.json.ObjectMap) ?[]const std.json.Value {
    const value = obj.get("chunk_embeddings") orelse return null;
    if (value != .array) return null;
    return value.array.items;
}

fn findChunkEmbeddingsVersion(obj: std.json.ObjectMap) ?u32 {
    const enrichment = obj.get("_enrichment") orelse return null;
    if (enrichment != .object) return null;
    const version = enrichment.object.get("chunk_embeddings_version") orelse return null;
    return jsonValueAsU32(version) catch null;
}

fn findRerankTermsVersion(obj: std.json.ObjectMap) ?u32 {
    const enrichment = obj.get("_enrichment") orelse return null;
    if (enrichment != .object) return null;
    const version = enrichment.object.get("rerank_terms_version") orelse return null;
    return jsonValueAsU32(version) catch null;
}

fn cloneSparseEmbeddingAlloc(alloc: Allocator, value: std.json.Value) ![]SparseTermWeight {
    return switch (value) {
        .object => cloneSparseEmbeddingObjectAlloc(alloc, value.object),
        .array => cloneSparseEmbeddingArrayAlloc(alloc, value.array.items),
        else => error.InvalidSparseEmbeddingValue,
    };
}

fn cloneStringArrayAlloc(alloc: Allocator, values: []const std.json.Value) ![][]u8 {
    const out = try alloc.alloc([]u8, values.len);
    errdefer alloc.free(out);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |item| alloc.free(item);
    }
    for (values, 0..) |value, idx| {
        if (value != .string) return error.InvalidSparseEmbeddingValue;
        out[idx] = try alloc.dupe(u8, value.string);
        initialized += 1;
    }
    return out;
}

fn cloneChunkEmbeddingsAlloc(alloc: Allocator, values: []const std.json.Value) ![]ChunkEmbedding {
    const out = try alloc.alloc(ChunkEmbedding, values.len);
    errdefer alloc.free(out);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*chunk_embedding| chunk_embedding.deinit(alloc);
    }
    for (values) |value| {
        if (value != .object) return error.InvalidSparseEmbeddingValue;
        const chunk = findString(value.object, "chunk") orelse findString(value.object, "text") orelse return error.InvalidSparseEmbeddingValue;
        const embedding = findEmbedding(value.object) orelse return error.InvalidSparseEmbeddingValue;
        out[initialized] = .{
            .chunk = try alloc.dupe(u8, chunk),
            .embedding = try cloneEmbeddingAlloc(alloc, embedding),
        };
        initialized += 1;
    }
    return out;
}

fn cloneSparseEmbeddingObjectAlloc(alloc: Allocator, obj: std.json.ObjectMap) ![]SparseTermWeight {
    const out = try alloc.alloc(SparseTermWeight, obj.count());
    errdefer alloc.free(out);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |weight| alloc.free(weight.term);
    }
    var iter = obj.iterator();
    while (iter.next()) |entry| {
        out[initialized] = .{
            .term = try alloc.dupe(u8, entry.key_ptr.*),
            .weight = try jsonValueAsF32(entry.value_ptr.*),
        };
        initialized += 1;
    }
    return out;
}

fn cloneSparseEmbeddingArrayAlloc(alloc: Allocator, values: []const std.json.Value) ![]SparseTermWeight {
    const out = try alloc.alloc(SparseTermWeight, values.len);
    errdefer alloc.free(out);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |weight| alloc.free(weight.term);
    }
    for (values) |value| {
        if (value != .object) return error.InvalidSparseEmbeddingValue;
        const obj = value.object;
        const term = findString(obj, "term") orelse findString(obj, "token") orelse return error.InvalidSparseEmbeddingValue;
        const weight_value = obj.get("weight") orelse obj.get("score") orelse return error.InvalidSparseEmbeddingValue;
        out[initialized] = .{
            .term = try alloc.dupe(u8, term),
            .weight = try jsonValueAsF32(weight_value),
        };
        initialized += 1;
    }
    return out;
}

fn jsonValueAsF32(value: std.json.Value) !f32 {
    return switch (value) {
        .float => @floatCast(value.float),
        .integer => @floatFromInt(value.integer),
        .number_string => try std.fmt.parseFloat(f32, value.number_string),
        else => error.InvalidSparseEmbeddingValue,
    };
}

fn jsonValueAsU32(value: std.json.Value) !u32 {
    return switch (value) {
        .integer => std.math.cast(u32, value.integer) orelse error.InvalidSparseEmbeddingValue,
        .number_string => try std.fmt.parseInt(u32, value.number_string, 10),
        else => error.InvalidSparseEmbeddingValue,
    };
}

test "document projection keeps plain text bodies" {
    const alloc = std.testing.allocator;
    var projection = try parseAlloc(alloc, "alpha bravo");
    defer projection.deinit(alloc);
    try std.testing.expectEqualStrings("alpha bravo", projection.text);
    try std.testing.expectEqual(@as(?[]f32, null), projection.embedding);
}

test "document projection extracts text and embedding from JSON body" {
    const alloc = std.testing.allocator;
    var projection = try parseAlloc(alloc, "{\"text\":\"alpha\",\"embedding\":[1,2,3]}");
    defer projection.deinit(alloc);
    try std.testing.expectEqualStrings("alpha", projection.text);
    try std.testing.expectEqual(@as(usize, 3), projection.embedding.?.len);
    try std.testing.expectEqual(@as(f32, 2), projection.embedding.?[1]);
}

test "document projection extracts sparse embedding from JSON body" {
    const alloc = std.testing.allocator;
    var projection = try parseAlloc(alloc, "{\"text\":\"alpha\",\"sparse_embedding\":{\"alpha\":1.5,\"bravo\":0.25}}");
    defer projection.deinit(alloc);
    try std.testing.expectEqualStrings("alpha", projection.text);
    try std.testing.expectEqual(@as(usize, 2), projection.sparse_embedding.?.len);
}

test "document projection extracts named dense and sparse embeddings from _embeddings map" {
    const alloc = std.testing.allocator;
    var projection = try parseAlloc(
        alloc,
        "{\"text\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,2,3],\"sparse_idx\":{\"10\":1.5,\"20\":0.25}}}",
    );
    defer projection.deinit(alloc);
    try std.testing.expectEqualStrings("alpha", projection.text);
    try std.testing.expectEqual(@as(usize, 3), projection.embedding.?.len);
    try std.testing.expectEqual(@as(f32, 2), projection.embedding.?[1]);
    try std.testing.expectEqual(@as(usize, 2), projection.sparse_embedding.?.len);
    try std.testing.expectEqual(@as(f32, 3), projection.findNamedEmbedding("dense_idx").?[2]);
    try std.testing.expectEqual(@as(usize, 2), projection.findNamedSparseEmbedding("sparse_idx").?.len);
}

test "document projection extracts lexical sparse enrichment version" {
    const alloc = std.testing.allocator;
    var projection = try parseAlloc(alloc, "{\"text\":\"alpha\",\"sparse_embedding\":{\"alpha\":1.0},\"_enrichment\":{\"lexical_sparse\":true,\"lexical_sparse_version\":2}}");
    defer projection.deinit(alloc);
    try std.testing.expectEqual(@as(?u32, 2), projection.lexical_sparse_version);
}

test "document projection extracts chunk preview enrichment version" {
    const alloc = std.testing.allocator;
    var projection = try parseAlloc(alloc, "{\"text\":\"alpha\",\"_enrichment\":{\"chunk_preview\":true,\"chunk_preview_version\":3}}");
    defer projection.deinit(alloc);
    try std.testing.expectEqual(@as(?u32, 3), projection.chunk_preview_version);
}

test "document projection extracts chunk embeddings" {
    const alloc = std.testing.allocator;
    var projection = try parseAlloc(alloc, "{\"text\":\"alpha\",\"chunk_embeddings\":[{\"chunk\":\"alpha\",\"embedding\":[1,2]}],\"_enrichment\":{\"chunk_embeddings\":true,\"chunk_embeddings_version\":2}}");
    defer projection.deinit(alloc);
    try std.testing.expectEqual(@as(?u32, 2), projection.chunk_embeddings_version);
    try std.testing.expectEqual(@as(usize, 1), projection.chunk_embeddings.?.len);
    try std.testing.expectEqualStrings("alpha", projection.chunk_embeddings.?[0].chunk);
}

test "document projection prefers chunk embeddings as vector source when present" {
    const alloc = std.testing.allocator;
    var projection = try parseAlloc(alloc, "{\"text\":\"alpha\",\"embedding\":[9,9],\"chunk_embeddings\":[{\"chunk\":\"alpha\",\"embedding\":[1,2]}]}");
    defer projection.deinit(alloc);
    const source = projection.vectorSource();
    try std.testing.expectEqual(@as(usize, 1), source.vectorCount());
    try std.testing.expectEqual(@as(?usize, 2), source.dims());
    try std.testing.expect(source == .chunk_embeddings);
}

test "document projection extracts rerank terms enrichment version" {
    const alloc = std.testing.allocator;
    var projection = try parseAlloc(alloc, "{\"text\":\"alpha\",\"_enrichment\":{\"rerank_terms\":true,\"rerank_terms_version\":4}}");
    defer projection.deinit(alloc);
    try std.testing.expectEqual(@as(?u32, 4), projection.rerank_terms_version);
}
