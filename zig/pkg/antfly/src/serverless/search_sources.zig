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
const document_projection = @import("document_projection.zig");
const full_text_indexes = @import("../api/full_text_indexes.zig");
const shared_vector = @import("antfly_vector").vector;

pub const default_full_text_index_name = full_text_indexes.default_full_text_index_name;
pub const default_chunk_embedding_index_name = "serverless_chunk";
pub const default_sparse_embedding_index_name = "serverless_sparse";
pub const default_chunk_preview_output_name = "serverless_chunk_preview";
pub const default_chunk_embeddings_output_name = "serverless_chunk_embeddings";
pub const default_rerank_terms_output_name = "serverless_rerank_terms";

pub const VectorDocumentSource = enum {
    top_level_embedding,
    chunk_embeddings,
    chunk_embeddings_or_top_level,
};

pub const SparseDocumentSource = enum {
    sparse_embedding,
};

pub const DerivedOutputKind = enum {
    chunk_preview,
    chunk_embeddings,
    rerank_terms,
};

pub const VectorSourceDescriptor = struct {
    index_name: []const u8,
    document_source: VectorDocumentSource,
    embedding_name: ?[]const u8 = null,
    distance_metric: ?shared_vector.DistanceMetric = null,
};

pub const SparseSourceDescriptor = struct {
    index_name: []const u8,
    document_source: SparseDocumentSource,
    embedding_name: ?[]const u8 = null,
};

pub const TextSourceDescriptor = struct {
    index_name: []const u8,
};

pub const SearchSourceKind = enum {
    text,
    vector,
    sparse,
};

pub const SearchSourceDescriptor = union(SearchSourceKind) {
    text: TextSourceDescriptor,
    vector: VectorSourceDescriptor,
    sparse: SparseSourceDescriptor,

    pub fn indexName(self: SearchSourceDescriptor) []const u8 {
        return switch (self) {
            .text => |value| value.index_name,
            .vector => |value| value.index_name,
            .sparse => |value| value.index_name,
        };
    }

    pub fn kind(self: SearchSourceDescriptor) SearchSourceKind {
        return std.meta.activeTag(self);
    }
};

pub const DerivedOutputDescriptor = struct {
    name: []const u8,
    kind: DerivedOutputKind,
};

pub const PublishedSearchSources = struct {
    items: ?[]SearchSourceDescriptor = null,
    text: ?TextSourceDescriptor = null,
    vector: ?VectorSourceDescriptor = null,
    sparse: ?SparseSourceDescriptor = null,

    pub fn findText(self: PublishedSearchSources) ?TextSourceDescriptor {
        if (self.items) |items| {
            for (items) |item| switch (item) {
                .text => |value| return value,
                else => {},
            };
        }
        return self.text;
    }

    pub fn findVector(self: PublishedSearchSources) ?VectorSourceDescriptor {
        if (self.items) |items| {
            for (items) |item| switch (item) {
                .vector => |value| return value,
                else => {},
            };
        }
        return self.vector;
    }

    pub fn findSparse(self: PublishedSearchSources) ?SparseSourceDescriptor {
        if (self.items) |items| {
            for (items) |item| switch (item) {
                .sparse => |value| return value,
                else => {},
            };
        }
        return self.sparse;
    }

    pub fn resolveRequested(
        self: PublishedSearchSources,
        indexes: ?[][]u8,
        needs_vector: bool,
        needs_sparse: bool,
    ) !ResolvedSearchSources {
        var resolved: ResolvedSearchSources = .{};
        if (indexes) |names| {
            if (names.len == 0) return error.InvalidQueryRequest;
            for (names) |index_name| {
                if (self.items) |items| {
                    var matched = false;
                    for (items) |item| {
                        if (!std.mem.eql(u8, item.indexName(), index_name)) continue;
                        try resolved.append(item);
                        matched = true;
                        break;
                    }
                    if (matched) {
                        continue;
                    }
                } else {
                    if (self.text) |text| {
                        if (std.mem.eql(u8, text.index_name, index_name)) {
                            try resolved.append(.{ .text = text });
                            continue;
                        }
                    }
                    if (self.vector) |vector| {
                        if (std.mem.eql(u8, vector.index_name, index_name)) {
                            try resolved.append(.{ .vector = vector });
                            continue;
                        }
                    }
                    if (self.sparse) |sparse| {
                        if (std.mem.eql(u8, sparse.index_name, index_name)) {
                            try resolved.append(.{ .sparse = sparse });
                            continue;
                        }
                    }
                }
                return error.InvalidQueryRequest;
            }
        }

        if (needs_vector and indexes != null and resolved.findVector() == null) return error.InvalidQueryRequest;
        if (needs_sparse and indexes != null and resolved.findSparse() == null) return error.InvalidQueryRequest;
        return resolved;
    }
};

pub const MaterializedDerivedOutputs = struct {
    items: ?[]DerivedOutputDescriptor = null,

    pub fn findByKind(self: MaterializedDerivedOutputs, kind: DerivedOutputKind) ?DerivedOutputDescriptor {
        const items = self.items orelse return null;
        for (items) |item| {
            if (item.kind == kind) return item;
        }
        return null;
    }

    pub fn containsKind(self: MaterializedDerivedOutputs, kind: DerivedOutputKind) bool {
        return self.findByKind(kind) != null;
    }
};

pub const ResolvedSearchSources = struct {
    items: [4]SearchSourceDescriptor = undefined,
    len: usize = 0,

    pub fn asSlice(self: *const ResolvedSearchSources) []const SearchSourceDescriptor {
        return self.items[0..self.len];
    }

    pub fn append(self: *ResolvedSearchSources, descriptor: SearchSourceDescriptor) !void {
        for (self.asSlice()) |item| {
            if (item.kind() != descriptor.kind()) continue;
            if (std.mem.eql(u8, item.indexName(), descriptor.indexName())) return;
        }
        if (self.len >= self.items.len) return error.InvalidQueryRequest;
        self.items[self.len] = descriptor;
        self.len += 1;
    }

    pub fn findVector(self: *const ResolvedSearchSources) ?VectorSourceDescriptor {
        for (self.asSlice()) |item| switch (item) {
            .vector => |value| return value,
            else => {},
        };
        return null;
    }

    pub fn findText(self: *const ResolvedSearchSources) ?TextSourceDescriptor {
        for (self.asSlice()) |item| switch (item) {
            .text => |value| return value,
            else => {},
        };
        return null;
    }

    pub fn findSparse(self: *const ResolvedSearchSources) ?SparseSourceDescriptor {
        for (self.asSlice()) |item| switch (item) {
            .sparse => |value| return value,
            else => {},
        };
        return null;
    }
};

const default_published_search_source_items = [_]SearchSourceDescriptor{
    .{ .text = .{
        .index_name = default_full_text_index_name,
    } },
    .{ .vector = .{
        .index_name = default_chunk_embedding_index_name,
        .document_source = .chunk_embeddings_or_top_level,
    } },
    .{ .sparse = .{
        .index_name = default_sparse_embedding_index_name,
        .document_source = .sparse_embedding,
    } },
};

pub fn defaultPublishedSearchSources() PublishedSearchSources {
    return .{
        .items = @constCast(default_published_search_source_items[0..]),
        .text = .{
            .index_name = default_full_text_index_name,
        },
        .vector = .{
            .index_name = default_chunk_embedding_index_name,
            .document_source = .chunk_embeddings_or_top_level,
            .distance_metric = null,
        },
        .sparse = .{
            .index_name = default_sparse_embedding_index_name,
            .document_source = .sparse_embedding,
        },
    };
}

pub fn defaultPublishedSearchSourcesAlloc(alloc: Allocator) !PublishedSearchSources {
    return try clonePublishedSearchSourcesAlloc(alloc, defaultPublishedSearchSources());
}

pub fn publishedSearchSourcesForIndexesJsonAlloc(
    alloc: Allocator,
    indexes_json: []const u8,
) !PublishedSearchSources {
    return try publishedSearchSourcesForTableDefinitionAlloc(alloc, "", "", indexes_json);
}

pub fn publishedSearchSourcesForTableDefinitionAlloc(
    alloc: Allocator,
    schema_json: []const u8,
    read_schema_json: []const u8,
    indexes_json: []const u8,
) !PublishedSearchSources {
    if (indexes_json.len == 0) return .{};

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
    defer parsed.deinit();
    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidTableIndexMetadata,
    };

    var active_text_index_name = try full_text_indexes.selectActiveFullTextIndexNameAlloc(
        alloc,
        schema_json,
        read_schema_json,
        indexes_json,
    );
    defer if (active_text_index_name) |name| alloc.free(name);
    var full_text_index_names = try full_text_indexes.listFullTextIndexNamesAlloc(alloc, indexes_json);
    defer {
        for (full_text_index_names) |name| alloc.free(name);
        alloc.free(full_text_index_names);
    }
    if (full_text_index_names.len == 0) {
        alloc.free(full_text_index_names);
        const fallback_names = try alloc.alloc([]u8, 1);
        errdefer alloc.free(fallback_names);
        fallback_names[0] = try alloc.dupe(u8, default_full_text_index_name);
        full_text_index_names = fallback_names;
        if (active_text_index_name == null) active_text_index_name = try alloc.dupe(u8, default_full_text_index_name);
    }

    var vector_index_names = std.ArrayListUnmanaged([]const u8).empty;
    defer vector_index_names.deinit(alloc);
    var sparse_index_names = std.ArrayListUnmanaged([]const u8).empty;
    defer sparse_index_names.deinit(alloc);
    var it = root.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.* != .object) continue;
        const config = entry.value_ptr.object;
        const type_value = config.get("type") orelse continue;
        if (type_value != .string or !std.mem.eql(u8, type_value.string, "embeddings")) continue;
        const sparse = if (config.get("sparse")) |sparse_value|
            switch (sparse_value) {
                .bool => sparse_value.bool,
                else => return error.InvalidTableIndexMetadata,
            }
        else
            false;
        if (sparse) {
            try sparse_index_names.append(alloc, entry.key_ptr.*);
        } else {
            try vector_index_names.append(alloc, entry.key_ptr.*);
        }
    }

    const sources = try publishedSearchSourcesForDefinitionListsAlloc(
        alloc,
        active_text_index_name,
        full_text_index_names,
        vector_index_names.items,
        sparse_index_names.items,
    );
    if (sources.items) |items| {
        for (items) |*item| switch (item.*) {
            .vector => |*value| {
                if (root.get(value.index_name)) |raw_config| {
                    if (raw_config == .object) {
                        value.distance_metric = try parseEmbeddingsDistanceMetric(raw_config.object);
                    }
                }
            },
            else => {},
        };
    }
    return sources;
}

fn parseEmbeddingsDistanceMetric(config: std.json.ObjectMap) !?shared_vector.DistanceMetric {
    const raw = config.get("distance_metric") orelse return null;
    return switch (raw) {
        .string => |value| std.meta.stringToEnum(shared_vector.DistanceMetric, value) orelse return error.InvalidTableIndexMetadata,
        else => error.InvalidTableIndexMetadata,
    };
}

pub fn defaultDerivedOutputName(kind: DerivedOutputKind) []const u8 {
    return switch (kind) {
        .chunk_preview => default_chunk_preview_output_name,
        .chunk_embeddings => default_chunk_embeddings_output_name,
        .rerank_terms => default_rerank_terms_output_name,
    };
}

pub fn defaultMaterializedDerivedOutputsAlloc(alloc: Allocator) !MaterializedDerivedOutputs {
    const items = try alloc.alloc(DerivedOutputDescriptor, 3);
    errdefer alloc.free(items);
    items[0] = .{
        .name = try alloc.dupe(u8, defaultDerivedOutputName(.chunk_preview)),
        .kind = .chunk_preview,
    };
    errdefer alloc.free(@constCast(items[0].name));
    items[1] = .{
        .name = try alloc.dupe(u8, defaultDerivedOutputName(.chunk_embeddings)),
        .kind = .chunk_embeddings,
    };
    errdefer alloc.free(@constCast(items[1].name));
    items[2] = .{
        .name = try alloc.dupe(u8, defaultDerivedOutputName(.rerank_terms)),
        .kind = .rerank_terms,
    };
    return .{ .items = items };
}

pub fn cloneVectorSourceDescriptorAlloc(
    alloc: Allocator,
    descriptor: VectorSourceDescriptor,
) !VectorSourceDescriptor {
    return .{
        .index_name = try alloc.dupe(u8, descriptor.index_name),
        .document_source = descriptor.document_source,
        .embedding_name = if (descriptor.embedding_name) |name| try alloc.dupe(u8, name) else null,
        .distance_metric = descriptor.distance_metric,
    };
}

pub fn cloneSparseSourceDescriptorAlloc(
    alloc: Allocator,
    descriptor: SparseSourceDescriptor,
) !SparseSourceDescriptor {
    return .{
        .index_name = try alloc.dupe(u8, descriptor.index_name),
        .document_source = descriptor.document_source,
        .embedding_name = if (descriptor.embedding_name) |name| try alloc.dupe(u8, name) else null,
    };
}

pub fn cloneTextSourceDescriptorAlloc(
    alloc: Allocator,
    descriptor: TextSourceDescriptor,
) !TextSourceDescriptor {
    return .{
        .index_name = try alloc.dupe(u8, descriptor.index_name),
    };
}

pub fn cloneSearchSourceDescriptorAlloc(
    alloc: Allocator,
    descriptor: SearchSourceDescriptor,
) !SearchSourceDescriptor {
    return switch (descriptor) {
        .text => |value| .{ .text = try cloneTextSourceDescriptorAlloc(alloc, value) },
        .vector => |value| .{ .vector = try cloneVectorSourceDescriptorAlloc(alloc, value) },
        .sparse => |value| .{ .sparse = try cloneSparseSourceDescriptorAlloc(alloc, value) },
    };
}

fn publishedSearchSourcesFromOwnedItems(items: []SearchSourceDescriptor) PublishedSearchSources {
    var out: PublishedSearchSources = .{ .items = items };
    for (items) |item| switch (item) {
        .text => |value| {
            if (out.text == null) out.text = value;
        },
        .vector => |value| out.vector = value,
        .sparse => |value| out.sparse = value,
    };
    return out;
}

pub fn clonePublishedSearchSourcesAlloc(
    alloc: Allocator,
    sources: PublishedSearchSources,
) !PublishedSearchSources {
    if (sources.items) |items| {
        const cloned = try alloc.alloc(SearchSourceDescriptor, items.len);
        errdefer alloc.free(cloned);
        var initialized: usize = 0;
        errdefer {
            for (cloned[0..initialized]) |*item| deinitSearchSourceDescriptor(alloc, item);
        }
        for (items, 0..) |item, idx| {
            cloned[idx] = try cloneSearchSourceDescriptorAlloc(alloc, item);
            initialized += 1;
        }
        return publishedSearchSourcesFromOwnedItems(cloned);
    }

    var items = std.ArrayListUnmanaged(SearchSourceDescriptor).empty;
    errdefer {
        for (items.items) |*item| deinitSearchSourceDescriptor(alloc, item);
        items.deinit(alloc);
    }
    if (sources.text) |text| {
        try items.append(alloc, .{ .text = try cloneTextSourceDescriptorAlloc(alloc, text) });
    }
    if (sources.vector) |vector| {
        try items.append(alloc, .{ .vector = try cloneVectorSourceDescriptorAlloc(alloc, vector) });
    }
    if (sources.sparse) |sparse| {
        try items.append(alloc, .{ .sparse = try cloneSparseSourceDescriptorAlloc(alloc, sparse) });
    }
    if (items.items.len == 0) return .{};
    return publishedSearchSourcesFromOwnedItems(try items.toOwnedSlice(alloc));
}

pub fn cloneDerivedOutputDescriptorAlloc(
    alloc: Allocator,
    descriptor: DerivedOutputDescriptor,
) !DerivedOutputDescriptor {
    return .{
        .name = try alloc.dupe(u8, descriptor.name),
        .kind = descriptor.kind,
    };
}

pub fn cloneMaterializedDerivedOutputsAlloc(
    alloc: Allocator,
    outputs: MaterializedDerivedOutputs,
) !MaterializedDerivedOutputs {
    const items = outputs.items orelse return .{};
    const cloned = try alloc.alloc(DerivedOutputDescriptor, items.len);
    errdefer alloc.free(cloned);
    var initialized: usize = 0;
    errdefer {
        for (cloned[0..initialized]) |*item| deinitDerivedOutputDescriptor(alloc, item);
    }
    for (items, 0..) |item, idx| {
        cloned[idx] = try cloneDerivedOutputDescriptorAlloc(alloc, item);
        initialized += 1;
    }
    return .{ .items = cloned };
}

pub fn deinitVectorSourceDescriptor(alloc: Allocator, descriptor: *VectorSourceDescriptor) void {
    alloc.free(@constCast(descriptor.index_name));
    if (descriptor.embedding_name) |name| alloc.free(@constCast(name));
    descriptor.* = undefined;
}

pub fn deinitTextSourceDescriptor(alloc: Allocator, descriptor: *TextSourceDescriptor) void {
    alloc.free(@constCast(descriptor.index_name));
    descriptor.* = undefined;
}

pub fn deinitSparseSourceDescriptor(alloc: Allocator, descriptor: *SparseSourceDescriptor) void {
    alloc.free(@constCast(descriptor.index_name));
    if (descriptor.embedding_name) |name| alloc.free(@constCast(name));
    descriptor.* = undefined;
}

pub fn freeVectorSourceDescriptors(alloc: Allocator, items: []VectorSourceDescriptor) void {
    for (items) |*item| deinitVectorSourceDescriptor(alloc, item);
    alloc.free(items);
}

pub fn freeSparseSourceDescriptors(alloc: Allocator, items: []SparseSourceDescriptor) void {
    for (items) |*item| deinitSparseSourceDescriptor(alloc, item);
    alloc.free(items);
}

pub fn listVectorSourcesAlloc(
    alloc: Allocator,
    sources: PublishedSearchSources,
) ![]VectorSourceDescriptor {
    if (sources.items) |items| {
        var matches = std.ArrayListUnmanaged(VectorSourceDescriptor).empty;
        errdefer {
            for (matches.items) |*item| deinitVectorSourceDescriptor(alloc, item);
            matches.deinit(alloc);
        }
        for (items) |item| switch (item) {
            .vector => |value| try matches.append(alloc, try cloneVectorSourceDescriptorAlloc(alloc, value)),
            else => {},
        };
        return try matches.toOwnedSlice(alloc);
    }
    if (sources.vector) |value| {
        const out = try alloc.alloc(VectorSourceDescriptor, 1);
        out[0] = try cloneVectorSourceDescriptorAlloc(alloc, value);
        return out;
    }
    return try alloc.alloc(VectorSourceDescriptor, 0);
}

pub fn listSparseSourcesAlloc(
    alloc: Allocator,
    sources: PublishedSearchSources,
) ![]SparseSourceDescriptor {
    if (sources.items) |items| {
        var matches = std.ArrayListUnmanaged(SparseSourceDescriptor).empty;
        errdefer {
            for (matches.items) |*item| deinitSparseSourceDescriptor(alloc, item);
            matches.deinit(alloc);
        }
        for (items) |item| switch (item) {
            .sparse => |value| try matches.append(alloc, try cloneSparseSourceDescriptorAlloc(alloc, value)),
            else => {},
        };
        return try matches.toOwnedSlice(alloc);
    }
    if (sources.sparse) |value| {
        const out = try alloc.alloc(SparseSourceDescriptor, 1);
        out[0] = try cloneSparseSourceDescriptorAlloc(alloc, value);
        return out;
    }
    return try alloc.alloc(SparseSourceDescriptor, 0);
}

pub fn deinitSearchSourceDescriptor(alloc: Allocator, descriptor: *SearchSourceDescriptor) void {
    switch (descriptor.*) {
        .text => |*value| deinitTextSourceDescriptor(alloc, value),
        .vector => |*value| deinitVectorSourceDescriptor(alloc, value),
        .sparse => |*value| deinitSparseSourceDescriptor(alloc, value),
    }
    descriptor.* = undefined;
}

pub fn deinitPublishedSearchSources(alloc: Allocator, sources: *PublishedSearchSources) void {
    if (sources.items) |items| {
        if (sources.text) |*text| {
            var aliased = false;
            for (items) |item| switch (item) {
                .text => |value| {
                    if (value.index_name.ptr == text.index_name.ptr) aliased = true;
                },
                else => {},
            };
            if (!aliased) deinitTextSourceDescriptor(alloc, text);
        }
        if (sources.vector) |*vector| {
            var aliased = false;
            for (items) |item| switch (item) {
                .vector => |value| {
                    if (value.index_name.ptr == vector.index_name.ptr) aliased = true;
                },
                else => {},
            };
            if (!aliased) deinitVectorSourceDescriptor(alloc, vector);
        }
        if (sources.sparse) |*sparse| {
            var aliased = false;
            for (items) |item| switch (item) {
                .sparse => |value| {
                    if (value.index_name.ptr == sparse.index_name.ptr) aliased = true;
                },
                else => {},
            };
            if (!aliased) deinitSparseSourceDescriptor(alloc, sparse);
        }
        for (items) |*item| deinitSearchSourceDescriptor(alloc, item);
        alloc.free(items);
    } else {
        if (sources.text) |*text| deinitTextSourceDescriptor(alloc, text);
        if (sources.vector) |*vector| deinitVectorSourceDescriptor(alloc, vector);
        if (sources.sparse) |*sparse| deinitSparseSourceDescriptor(alloc, sparse);
    }
    sources.* = undefined;
}

pub fn deinitDerivedOutputDescriptor(alloc: Allocator, descriptor: *DerivedOutputDescriptor) void {
    alloc.free(@constCast(descriptor.name));
    descriptor.* = undefined;
}

pub fn deinitMaterializedDerivedOutputs(alloc: Allocator, outputs: *MaterializedDerivedOutputs) void {
    if (outputs.items) |items| {
        for (items) |*value| deinitDerivedOutputDescriptor(alloc, value);
        alloc.free(items);
    }
    outputs.* = undefined;
}

pub fn publishedSearchSourcesForNames(
    chunk_embedding_index_name: ?[]const u8,
    sparse_embedding_index_name: ?[]const u8,
) PublishedSearchSources {
    return .{
        .text = .{ .index_name = default_full_text_index_name },
        .vector = if (chunk_embedding_index_name) |name| .{
            .index_name = name,
            .document_source = .chunk_embeddings_or_top_level,
        } else null,
        .sparse = if (sparse_embedding_index_name) |name| .{
            .index_name = name,
            .document_source = .sparse_embedding,
        } else null,
    };
}

pub fn withDenseQueryIndexName(
    sources: PublishedSearchSources,
    index_name: []const u8,
) PublishedSearchSources {
    var out = publishedSearchSourcesForNames(
        index_name,
        if (sources.findSparse()) |value| value.index_name else null,
    );
    out.text = sources.findText();
    return out;
}

pub fn publishedSearchSourcesForArtifactKind(
    artifact_kind: anytype,
    published_search_sources: PublishedSearchSources,
) PublishedSearchSources {
    return switch (artifact_kind) {
        .text_segment => .{ .text = published_search_sources.findText() },
        .vector_segment => .{ .vector = published_search_sources.findVector() },
        .sparse_segment => .{ .sparse = published_search_sources.findSparse() },
        else => .{},
    };
}

pub fn materializedDerivedOutputsForArtifactKind(
    artifact_kind: anytype,
    outputs: MaterializedDerivedOutputs,
) MaterializedDerivedOutputs {
    return switch (artifact_kind) {
        .document_segment => outputs,
        else => .{},
    };
}

pub fn publishedSearchSourcesForNamesAlloc(
    alloc: Allocator,
    chunk_embedding_index_name: ?[]const u8,
    sparse_embedding_index_name: ?[]const u8,
) !PublishedSearchSources {
    const default_text_names = [_][]const u8{default_full_text_index_name};
    return try publishedSearchSourcesForDefinitionAlloc(
        alloc,
        default_full_text_index_name,
        default_text_names[0..],
        chunk_embedding_index_name,
        sparse_embedding_index_name,
    );
}

pub fn publishedSearchSourcesForDefinitionAlloc(
    alloc: Allocator,
    active_text_index_name: ?[]const u8,
    full_text_index_names: []const []const u8,
    chunk_embedding_index_name: ?[]const u8,
    sparse_embedding_index_name: ?[]const u8,
) !PublishedSearchSources {
    var vector_name_buf: [1][]const u8 = undefined;
    const vector_names: []const []const u8 = if (chunk_embedding_index_name) |name| blk: {
        vector_name_buf[0] = name;
        break :blk vector_name_buf[0..1];
    } else &.{};
    var sparse_name_buf: [1][]const u8 = undefined;
    const sparse_names: []const []const u8 = if (sparse_embedding_index_name) |name| blk: {
        sparse_name_buf[0] = name;
        break :blk sparse_name_buf[0..1];
    } else &.{};
    return try publishedSearchSourcesForDefinitionListsAlloc(
        alloc,
        active_text_index_name,
        full_text_index_names,
        vector_names[0..],
        sparse_names[0..],
    );
}

pub fn publishedSearchSourcesForDefinitionListsAlloc(
    alloc: Allocator,
    active_text_index_name: ?[]const u8,
    full_text_index_names: []const []const u8,
    chunk_embedding_index_names: []const []const u8,
    sparse_embedding_index_names: []const []const u8,
) !PublishedSearchSources {
    var items = std.ArrayListUnmanaged(SearchSourceDescriptor).empty;
    errdefer {
        for (items.items) |*item| deinitSearchSourceDescriptor(alloc, item);
        items.deinit(alloc);
    }
    if (active_text_index_name) |name| {
        try items.append(alloc, .{ .text = .{
            .index_name = try alloc.dupe(u8, name),
        } });
    }
    for (full_text_index_names) |name| {
        if (active_text_index_name) |active| {
            if (std.mem.eql(u8, active, name)) continue;
        }
        try items.append(alloc, .{ .text = .{
            .index_name = try alloc.dupe(u8, name),
        } });
    }
    for (chunk_embedding_index_names) |name| {
        try items.append(alloc, .{ .vector = .{
            .index_name = try alloc.dupe(u8, name),
            .document_source = .chunk_embeddings_or_top_level,
            .embedding_name = if (std.mem.eql(u8, name, default_chunk_embedding_index_name)) null else try alloc.dupe(u8, name),
            .distance_metric = null,
        } });
    }
    for (sparse_embedding_index_names) |name| {
        try items.append(alloc, .{ .sparse = .{
            .index_name = try alloc.dupe(u8, name),
            .document_source = .sparse_embedding,
            .embedding_name = if (std.mem.eql(u8, name, default_sparse_embedding_index_name)) null else try alloc.dupe(u8, name),
        } });
    }
    if (items.items.len == 0) return .{};
    return publishedSearchSourcesFromOwnedItems(try items.toOwnedSlice(alloc));
}

pub fn selectVectorSource(
    projection: *const document_projection.Projection,
    descriptor: VectorSourceDescriptor,
) document_projection.VectorSource {
    if (descriptor.embedding_name) |name| {
        if (projection.findNamedEmbedding(name)) |embedding| {
            return .{ .top_level = embedding };
        }
    }
    return switch (descriptor.document_source) {
        .top_level_embedding => if (projection.embedding) |embedding|
            .{ .top_level = embedding }
        else
            .none,
        .chunk_embeddings => if (projection.chunk_embeddings) |chunk_embeddings|
            .{ .chunk_embeddings = chunk_embeddings }
        else
            .none,
        .chunk_embeddings_or_top_level => if (projection.chunk_embeddings) |chunk_embeddings|
            .{ .chunk_embeddings = chunk_embeddings }
        else if (projection.embedding) |embedding|
            .{ .top_level = embedding }
        else
            .none,
    };
}

pub fn selectSparseSource(
    projection: *const document_projection.Projection,
    descriptor: SparseSourceDescriptor,
) ?[]const document_projection.SparseTermWeight {
    if (descriptor.embedding_name) |name| {
        if (projection.findNamedSparseEmbedding(name)) |weights| return weights;
    }
    return switch (descriptor.document_source) {
        .sparse_embedding => projection.sparse_embedding,
    };
}

pub fn containsVectorIndexName(
    sources: PublishedSearchSources,
    index_name: []const u8,
) bool {
    if (sources.items) |items| {
        for (items) |item| switch (item) {
            .vector => |value| {
                if (std.mem.eql(u8, value.index_name, index_name)) return true;
            },
            else => {},
        };
        return false;
    }
    if (sources.vector) |value| return std.mem.eql(u8, value.index_name, index_name);
    return false;
}

pub fn containsSparseIndexName(
    sources: PublishedSearchSources,
    index_name: []const u8,
) bool {
    if (sources.items) |items| {
        for (items) |item| switch (item) {
            .sparse => |value| {
                if (std.mem.eql(u8, value.index_name, index_name)) return true;
            },
            else => {},
        };
        return false;
    }
    if (sources.sparse) |value| return std.mem.eql(u8, value.index_name, index_name);
    return false;
}

test "published search sources resolve named dense and sparse indexes" {
    const sources = defaultPublishedSearchSources();
    var indexes = [_][]u8{
        @constCast(default_chunk_embedding_index_name),
        @constCast(default_sparse_embedding_index_name),
    };
    const resolved = try sources.resolveRequested(indexes[0..], true, true);
    try std.testing.expect(resolved.findVector() != null);
    try std.testing.expect(resolved.findSparse() != null);
    try std.testing.expectEqualStrings(default_full_text_index_name, sources.findText().?.index_name);
    try std.testing.expectEqual(VectorDocumentSource.chunk_embeddings_or_top_level, resolved.findVector().?.document_source);
    try std.testing.expectEqual(SparseDocumentSource.sparse_embedding, resolved.findSparse().?.document_source);
    try std.testing.expectEqual(@as(usize, 3), sources.items.?.len);
}

test "published search sources derive embedding names from indexes json" {
    const alloc = std.testing.allocator;
    var sources = try publishedSearchSourcesForIndexesJsonAlloc(
        alloc,
        "{\"full_text_index_v0\":{\"type\":\"full_text\"},\"semantic_idx\":{\"type\":\"embeddings\",\"dimension\":3,\"distance_metric\":\"inner_product\"},\"sparse_idx\":{\"type\":\"embeddings\",\"sparse\":true}}",
    );
    defer deinitPublishedSearchSources(alloc, &sources);
    try std.testing.expectEqualStrings("full_text_index_v0", sources.findText().?.index_name);
    try std.testing.expectEqualStrings("semantic_idx", sources.findVector().?.index_name);
    try std.testing.expectEqualStrings("semantic_idx", sources.findVector().?.embedding_name.?);
    try std.testing.expectEqual(shared_vector.DistanceMetric.inner_product, sources.findVector().?.distance_metric.?);
    try std.testing.expectEqualStrings("sparse_idx", sources.findSparse().?.index_name);
    try std.testing.expectEqualStrings("sparse_idx", sources.findSparse().?.embedding_name.?);
}

test "published search sources preserve multiple named embedding indexes" {
    const alloc = std.testing.allocator;
    var sources = try publishedSearchSourcesForIndexesJsonAlloc(
        alloc,
        "{\"full_text_index_v0\":{\"type\":\"full_text\"},\"semantic_idx\":{\"type\":\"embeddings\",\"dimension\":3},\"semantic_idx_b\":{\"type\":\"embeddings\",\"dimension\":3},\"sparse_idx\":{\"type\":\"embeddings\",\"sparse\":true},\"sparse_idx_b\":{\"type\":\"embeddings\",\"sparse\":true}}",
    );
    defer deinitPublishedSearchSources(alloc, &sources);
    try std.testing.expectEqual(@as(usize, 5), sources.items.?.len);
    var indexes = [_][]u8{
        @constCast("semantic_idx_b"),
        @constCast("sparse_idx_b"),
    };
    const resolved = try sources.resolveRequested(indexes[0..], true, true);
    try std.testing.expectEqualStrings("semantic_idx_b", resolved.findVector().?.index_name);
    try std.testing.expectEqualStrings("semantic_idx_b", resolved.findVector().?.embedding_name.?);
    try std.testing.expectEqualStrings("sparse_idx_b", resolved.findSparse().?.index_name);
    try std.testing.expectEqualStrings("sparse_idx_b", resolved.findSparse().?.embedding_name.?);
}

test "published search sources reject unknown index names" {
    const sources = defaultPublishedSearchSources();
    var indexes = [_][]u8{@constCast("unknown")};
    try std.testing.expectError(error.InvalidQueryRequest, sources.resolveRequested(indexes[0..], true, false));
}

test "published search sources clone alloc produces owned descriptors" {
    const alloc = std.testing.allocator;
    var cloned = try clonePublishedSearchSourcesAlloc(alloc, defaultPublishedSearchSources());
    defer deinitPublishedSearchSources(alloc, &cloned);
    try std.testing.expectEqualStrings(default_full_text_index_name, cloned.findText().?.index_name);
    try std.testing.expectEqualStrings(default_chunk_embedding_index_name, cloned.findVector().?.index_name);
    try std.testing.expectEqualStrings(default_sparse_embedding_index_name, cloned.findSparse().?.index_name);
    try std.testing.expectEqual(@as(usize, 3), cloned.items.?.len);
}

test "published search sources alloc constructor produces owned registry items" {
    const alloc = std.testing.allocator;
    var owned = try publishedSearchSourcesForNamesAlloc(alloc, "dense", "sparse");
    defer deinitPublishedSearchSources(alloc, &owned);
    try std.testing.expectEqual(@as(usize, 3), owned.items.?.len);
    try std.testing.expectEqualStrings(default_full_text_index_name, owned.findText().?.index_name);
    try std.testing.expectEqualStrings("dense", owned.findVector().?.index_name);
    try std.testing.expectEqualStrings("sparse", owned.findSparse().?.index_name);
}

test "published search sources can replace dense name while preserving sparse source" {
    const replaced = withDenseQueryIndexName(defaultPublishedSearchSources(), "managed_dense");
    try std.testing.expectEqualStrings("managed_dense", replaced.findVector().?.index_name);
    try std.testing.expectEqualStrings(default_sparse_embedding_index_name, replaced.findSparse().?.index_name);
}

test "published search sources can check named vector and sparse membership" {
    const alloc = std.testing.allocator;
    var sources = try publishedSearchSourcesForIndexesJsonAlloc(
        alloc,
        "{\"full_text_index_v0\":{\"type\":\"full_text\"},\"semantic_a\":{\"type\":\"embeddings\",\"dimension\":3},\"semantic_b\":{\"type\":\"embeddings\",\"dimension\":3},\"sparse_a\":{\"type\":\"embeddings\",\"sparse\":true}}",
    );
    defer deinitPublishedSearchSources(alloc, &sources);
    try std.testing.expect(containsVectorIndexName(sources, "semantic_a"));
    try std.testing.expect(containsVectorIndexName(sources, "semantic_b"));
    try std.testing.expect(!containsVectorIndexName(sources, "missing"));
    try std.testing.expect(containsSparseIndexName(sources, "sparse_a"));
    try std.testing.expect(!containsSparseIndexName(sources, "semantic_a"));
}

test "published search sources filter by artifact kind" {
    const vector_only = publishedSearchSourcesForArtifactKind(.vector_segment, defaultPublishedSearchSources());
    try std.testing.expect(vector_only.findVector() != null);
    try std.testing.expect(vector_only.findSparse() == null);
    const sparse_only = publishedSearchSourcesForArtifactKind(.sparse_segment, defaultPublishedSearchSources());
    try std.testing.expect(sparse_only.findVector() == null);
    try std.testing.expect(sparse_only.findSparse() != null);
}

test "materialized derived outputs filter by artifact kind" {
    const alloc = std.testing.allocator;
    var outputs = try defaultMaterializedDerivedOutputsAlloc(alloc);
    defer deinitMaterializedDerivedOutputs(alloc, &outputs);

    const doc_outputs = materializedDerivedOutputsForArtifactKind(.document_segment, outputs);
    try std.testing.expect(doc_outputs.containsKind(.chunk_preview));
    try std.testing.expect(doc_outputs.containsKind(.chunk_embeddings));
    try std.testing.expect(doc_outputs.containsKind(.rerank_terms));

    const vector_outputs = materializedDerivedOutputsForArtifactKind(.vector_segment, outputs);
    try std.testing.expect(!vector_outputs.containsKind(.chunk_preview));
    try std.testing.expect(!vector_outputs.containsKind(.chunk_embeddings));
    try std.testing.expect(!vector_outputs.containsKind(.rerank_terms));
}

test "materialized derived outputs clone alloc produces owned descriptors" {
    const alloc = std.testing.allocator;
    var defaults = try defaultMaterializedDerivedOutputsAlloc(alloc);
    defer deinitMaterializedDerivedOutputs(alloc, &defaults);
    var cloned = try cloneMaterializedDerivedOutputsAlloc(alloc, defaults);
    defer deinitMaterializedDerivedOutputs(alloc, &cloned);
    try std.testing.expectEqualStrings(default_chunk_preview_output_name, cloned.findByKind(.chunk_preview).?.name);
    try std.testing.expectEqualStrings(default_chunk_embeddings_output_name, cloned.findByKind(.chunk_embeddings).?.name);
    try std.testing.expectEqualStrings(default_rerank_terms_output_name, cloned.findByKind(.rerank_terms).?.name);
}
