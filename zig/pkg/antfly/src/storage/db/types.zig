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
const graph_mod = @import("../../graph/graph.zig");
const traversal_mod = @import("../../graph/traversal.zig");
const paths_mod = @import("../../graph/paths.zig");
const graph_query_mod = @import("../../graph/query.zig");
const fusion_mod = @import("../../search/fusion.zig");
const distributed_stats_mod = @import("../../search/distributed_stats.zig");
const docstore_mod = @import("../docstore.zig");
const shard_mod = @import("../shard.zig");
const transactions_mod = @import("../transactions.zig");
const reranking_mod = @import("antfly_reranking");
const doc_identity_mod = @import("doc_identity.zig");

pub const GeoPoint = struct {
    lon: f64,
    lat: f64,
};

pub const GeoShapeRelation = enum {
    intersects,
    within,
    contains,
};

pub const SyncLevel = enum {
    propose,
    write,
    full_text,
    enrichments,
    aknn,
    full_index,
};

pub fn parsePublicSyncLevelText(text: []const u8) ?SyncLevel {
    if (std.mem.eql(u8, text, "propose")) return .propose;
    if (std.mem.eql(u8, text, "write")) return .write;
    if (std.mem.eql(u8, text, "full_text")) return .full_text;
    if (std.mem.eql(u8, text, "enrichments")) return .enrichments;
    if (std.mem.eql(u8, text, "aknn")) return .full_index;
    if (std.mem.eql(u8, text, "full_index")) return .full_index;
    return null;
}

pub fn parsePublicSyncLevelJson(value: std.json.Value) ?SyncLevel {
    return switch (value) {
        .string => |text| parsePublicSyncLevelText(text),
        else => null,
    };
}

pub fn publicSyncLevelText(level: SyncLevel) []const u8 {
    return switch (level) {
        .propose => "propose",
        .write => "write",
        .full_text => "full_text",
        .enrichments => "enrichments",
        .aknn, .full_index => "full_index",
    };
}

test "public sync level text treats aknn as deprecated alias for full_index" {
    try std.testing.expectEqual(SyncLevel.full_index, parsePublicSyncLevelText("aknn").?);
    try std.testing.expectEqual(SyncLevel.full_index, parsePublicSyncLevelText("full_index").?);
    try std.testing.expectEqualStrings("full_index", publicSyncLevelText(.aknn));
    try std.testing.expectEqualStrings("full_index", publicSyncLevelText(.full_index));
}

pub const BatchWrite = struct {
    key: []const u8,
    value: []const u8,
};

pub const TransformOpType = enum {
    set,
    unset,
    inc,
    push,
    pull,
    add_to_set,
    pop,
    mul,
    min,
    max,
    current_date,
    rename,
};

pub const TransformOp = struct {
    op: TransformOpType,
    path: []const u8,
    value_json: ?[]const u8 = null,
};

pub const DocumentTransform = struct {
    key: []const u8,
    operations: []const TransformOp = &.{},
    upsert: bool = false,
};

pub const BatchRequest = struct {
    writes: []const BatchWrite = &.{},
    deletes: []const []const u8 = &.{},
    transforms: []const DocumentTransform = &.{},
    graph_writes: []const GraphEdgeWrite = &.{},
    graph_deletes: []const GraphEdgeDelete = &.{},
    predicates: []const TransactionVersionPredicate = &.{},
    timestamp_ns: u64 = 0,
    sync_level: SyncLevel = .write,
};

pub const GraphEdgeWrite = struct {
    index_name: []const u8,
    source: []const u8,
    target: []const u8,
    edge_type: []const u8,
    weight: f64 = 1.0,
    created_at: u64 = 0,
    updated_at: u64 = 0,
    metadata_json: []const u8 = "",
};

pub const GraphEdgeDelete = struct {
    index_name: []const u8,
    source: []const u8,
    target: []const u8,
    edge_type: []const u8,
};

pub const IndexKind = enum {
    full_text,
    dense_vector,
    sparse_vector,
    graph,
    algebraic,
};

pub const IndexConfig = struct {
    name: []const u8,
    kind: IndexKind,
    config_json: []const u8,

    pub fn clone(alloc: Allocator, cfg: IndexConfig) !IndexConfig {
        return .{
            .name = try alloc.dupe(u8, cfg.name),
            .kind = cfg.kind,
            .config_json = try alloc.dupe(u8, cfg.config_json),
        };
    }

    pub fn deinit(self: *IndexConfig, alloc: Allocator) void {
        alloc.free(self.name);
        alloc.free(self.config_json);
        self.* = undefined;
    }
};

pub fn freeIndexConfigs(alloc: Allocator, configs: []IndexConfig) void {
    for (configs) |*cfg| cfg.deinit(alloc);
    if (configs.len > 0) alloc.free(configs);
}

pub const EnrichmentKind = enum {
    chunk,
    summary,
    embedding,
};

pub const ArtifactKind = enum {
    chunk,
    summary,
    embedding,
};

pub const ArtifactSourceRef = struct {
    kind: ArtifactKind,
    name: []u8,
    chunk_id: ?u32 = null,

    pub fn clone(self: ArtifactSourceRef, alloc: Allocator) !ArtifactSourceRef {
        return .{
            .kind = self.kind,
            .name = try alloc.dupe(u8, self.name),
            .chunk_id = self.chunk_id,
        };
    }

    pub fn deinit(self: *ArtifactSourceRef, alloc: Allocator) void {
        alloc.free(self.name);
        self.* = undefined;
    }
};

pub const ArtifactRef = struct {
    document_id: []u8,
    name: []u8,
    kind: ArtifactKind,
    chunk_id: ?u32 = null,
    source: ?ArtifactSourceRef = null,

    pub fn clone(self: ArtifactRef, alloc: Allocator) !ArtifactRef {
        return .{
            .document_id = try alloc.dupe(u8, self.document_id),
            .name = try alloc.dupe(u8, self.name),
            .kind = self.kind,
            .chunk_id = self.chunk_id,
            .source = if (self.source) |source| try source.clone(alloc) else null,
        };
    }

    pub fn deinit(self: *ArtifactRef, alloc: Allocator) void {
        alloc.free(self.document_id);
        alloc.free(self.name);
        if (self.source) |*source| source.deinit(alloc);
        self.* = undefined;
    }
};

pub const EnrichmentConfig = struct {
    name: []const u8,
    kind: EnrichmentKind,
    source_field: []const u8,
    source_template: []const u8 = "",
    source_artifact_name: []const u8 = "",
    expected_dims: u32 = 0,
    chunk_size: u32 = 0,
    chunk_overlap: u32 = 0,
    chunker_json: []const u8 = "",

    pub fn clone(alloc: Allocator, cfg: EnrichmentConfig) !EnrichmentConfig {
        return .{
            .name = try alloc.dupe(u8, cfg.name),
            .kind = cfg.kind,
            .source_field = try alloc.dupe(u8, cfg.source_field),
            .source_template = if (cfg.source_template.len > 0) try alloc.dupe(u8, cfg.source_template) else "",
            .source_artifact_name = if (cfg.source_artifact_name.len > 0) try alloc.dupe(u8, cfg.source_artifact_name) else "",
            .expected_dims = cfg.expected_dims,
            .chunk_size = cfg.chunk_size,
            .chunk_overlap = cfg.chunk_overlap,
            .chunker_json = if (cfg.chunker_json.len > 0) try alloc.dupe(u8, cfg.chunker_json) else "",
        };
    }

    pub fn deinit(self: *EnrichmentConfig, alloc: Allocator) void {
        alloc.free(self.name);
        alloc.free(self.source_field);
        if (self.source_template.len > 0) alloc.free(self.source_template);
        if (self.source_artifact_name.len > 0) alloc.free(self.source_artifact_name);
        if (self.chunker_json.len > 0) alloc.free(self.chunker_json);
        self.* = undefined;
    }
};

pub fn freeEnrichmentConfigs(alloc: Allocator, configs: []EnrichmentConfig) void {
    for (configs) |*cfg| cfg.deinit(alloc);
    if (configs.len > 0) alloc.free(configs);
}

pub const EnrichmentSummaryWrite = struct {
    index_name: []u8,
    doc_key: []u8,
    text: []u8,

    pub fn deinit(self: *EnrichmentSummaryWrite, alloc: Allocator) void {
        alloc.free(self.index_name);
        alloc.free(self.doc_key);
        alloc.free(self.text);
        self.* = undefined;
    }
};

pub const EnrichmentDenseEmbeddingWrite = struct {
    index_name: []u8,
    doc_key: []u8,
    artifact_id: ?[]u8 = null,
    artifact_ref: ?ArtifactRef = null,
    vector: []f32,

    pub fn deinit(self: *EnrichmentDenseEmbeddingWrite, alloc: Allocator) void {
        alloc.free(self.index_name);
        alloc.free(self.doc_key);
        if (self.artifact_id) |artifact_id| alloc.free(artifact_id);
        if (self.artifact_ref) |*artifact_ref| artifact_ref.deinit(alloc);
        alloc.free(self.vector);
        self.* = undefined;
    }
};

pub const EnrichmentSparseEmbeddingWrite = struct {
    index_name: []u8,
    doc_key: []u8,
    indices: []u32,
    values: []f32,

    pub fn deinit(self: *EnrichmentSparseEmbeddingWrite, alloc: Allocator) void {
        alloc.free(self.index_name);
        alloc.free(self.doc_key);
        alloc.free(self.indices);
        alloc.free(self.values);
        self.* = undefined;
    }
};

pub const EnrichmentDocumentWrite = struct {
    key: []u8,
    value: []u8,
    target_index_names: [][]u8 = &.{},

    pub fn deinit(self: *EnrichmentDocumentWrite, alloc: Allocator) void {
        alloc.free(self.key);
        alloc.free(self.value);
        for (self.target_index_names) |name| alloc.free(name);
        if (self.target_index_names.len > 0) alloc.free(self.target_index_names);
        self.* = undefined;
    }
};

pub const ExtractEnrichmentsResult = struct {
    cleaned_writes: []BatchWrite = &.{},
    dense_embeddings: []EnrichmentDenseEmbeddingWrite = &.{},
    sparse_embeddings: []EnrichmentSparseEmbeddingWrite = &.{},
    summaries: []EnrichmentSummaryWrite = &.{},
    graph_writes: []GraphEdgeWrite = &.{},

    pub fn deinit(self: *ExtractEnrichmentsResult, alloc: Allocator) void {
        for (self.cleaned_writes) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        if (self.cleaned_writes.len > 0) alloc.free(self.cleaned_writes);

        for (self.dense_embeddings) |*embedding| embedding.deinit(alloc);
        if (self.dense_embeddings.len > 0) alloc.free(self.dense_embeddings);

        for (self.sparse_embeddings) |*embedding| embedding.deinit(alloc);
        if (self.sparse_embeddings.len > 0) alloc.free(self.sparse_embeddings);

        for (self.summaries) |*summary| summary.deinit(alloc);
        if (self.summaries.len > 0) alloc.free(self.summaries);

        for (self.graph_writes) |*write| {
            alloc.free(@constCast(write.index_name));
            alloc.free(@constCast(write.source));
            alloc.free(@constCast(write.target));
            alloc.free(@constCast(write.edge_type));
            if (write.metadata_json.len > 0) alloc.free(@constCast(write.metadata_json));
        }
        if (self.graph_writes.len > 0) alloc.free(self.graph_writes);

        self.* = undefined;
    }
};

pub const ComputeEnrichmentsResult = struct {
    artifact_writes: []ArtifactWrite = &.{},
    documents: []EnrichmentDocumentWrite = &.{},
    dense_embeddings: []EnrichmentDenseEmbeddingWrite = &.{},
    failed_keys: [][]u8 = &.{},

    pub fn deinit(self: *ComputeEnrichmentsResult, alloc: Allocator) void {
        for (self.artifact_writes) |*write| write.deinit(alloc);
        if (self.artifact_writes.len > 0) alloc.free(self.artifact_writes);

        for (self.documents) |*doc| doc.deinit(alloc);
        if (self.documents.len > 0) alloc.free(self.documents);

        for (self.dense_embeddings) |*embedding| embedding.deinit(alloc);
        if (self.dense_embeddings.len > 0) alloc.free(self.dense_embeddings);

        for (self.failed_keys) |key| alloc.free(key);
        if (self.failed_keys.len > 0) alloc.free(self.failed_keys);

        self.* = undefined;
    }
};

pub const ArtifactWrite = struct {
    id: []u8,
    value: []u8,
    artifact_ref: ArtifactRef,

    pub fn deinit(self: *ArtifactWrite, alloc: Allocator) void {
        alloc.free(self.id);
        alloc.free(self.value);
        self.artifact_ref.deinit(alloc);
        self.* = undefined;
    }
};

pub const ArtifactRecord = ArtifactWrite;

pub const TextBoolQuery = struct {
    must: []const TextQuery = &.{},
    should: []const TextQuery = &.{},
    must_not: []const TextQuery = &.{},
    min_should: u32 = 0,
    boost: f32 = 1.0,
};

pub const TextMultiMatchField = struct {
    field: []const u8,
    boost: f32 = 1.0,
};

pub const TextQuery = union(enum) {
    match_none: void,
    match_all: void,
    phrase: struct {
        field: []const u8,
        terms: []const []const u8,
        max_edits: u8 = 0,
        auto_fuzzy: bool = false,
        boost: f32 = 1.0,
    },
    multi_phrase: struct {
        field: []const u8,
        terms: []const []const []const u8,
        max_edits: u8 = 0,
        auto_fuzzy: bool = false,
        boost: f32 = 1.0,
    },
    term: struct {
        field: []const u8,
        term: []const u8,
        boost: f32 = 1.0,
    },
    match: struct {
        field: []const u8,
        text: []const u8,
        analyzer: ?[]const u8 = null,
        boost: f32 = 1.0,
    },
    multi_match_bool_prefix: struct {
        query: []const u8,
        fields: []const TextMultiMatchField,
        boost: f32 = 1.0,
    },
    match_phrase: struct {
        field: []const u8,
        text: []const u8,
        analyzer: ?[]const u8 = null,
        max_edits: u8 = 0,
        auto_fuzzy: bool = false,
        boost: f32 = 1.0,
    },
    fuzzy: struct {
        field: []const u8,
        term: []const u8,
        max_edits: u8 = 1,
        prefix_len: u8 = 0,
        auto_fuzzy: bool = false,
        boost: f32 = 1.0,
    },
    numeric_range: struct {
        field: []const u8,
        min: ?f64 = null,
        max: ?f64 = null,
        inclusive_min: bool = true,
        inclusive_max: bool = false,
        boost: f32 = 1.0,
    },
    date_range: struct {
        field: []const u8,
        start_ns: ?u64 = null,
        end_ns: ?u64 = null,
        inclusive_start: bool = true,
        inclusive_end: bool = false,
        boost: f32 = 1.0,
    },
    doc_id: struct {
        ids: []const []const u8,
        boost: f32 = 1.0,
    },
    bool_field: struct {
        field: []const u8,
        value: bool,
        boost: f32 = 1.0,
    },
    geo_distance: struct {
        field: []const u8,
        lon: f64,
        lat: f64,
        radius_meters: f64,
        boost: f32 = 1.0,
    },
    geo_bbox: struct {
        field: []const u8,
        min_lat: f64,
        min_lon: f64,
        max_lat: f64,
        max_lon: f64,
        boost: f32 = 1.0,
    },
    prefix: struct {
        field: []const u8,
        prefix: []const u8,
        boost: f32 = 1.0,
    },
    wildcard: struct {
        field: []const u8,
        pattern: []const u8,
        boost: f32 = 1.0,
    },
    regexp: struct {
        field: []const u8,
        pattern: []const u8,
        boost: f32 = 1.0,
    },
    term_range: struct {
        field: []const u8,
        min: ?[]const u8 = null,
        max: ?[]const u8 = null,
        inclusive_min: bool = true,
        inclusive_max: bool = false,
        boost: f32 = 1.0,
    },
    ip_range: struct {
        field: []const u8,
        cidr: []const u8,
        boost: f32 = 1.0,
    },
    geo_shape: struct {
        field: []const u8,
        relation: GeoShapeRelation = .intersects,
        polygons: []const []const GeoPoint,
        boost: f32 = 1.0,
    },
    bool_query: TextBoolQuery,

    pub fn deinit(self: *TextQuery, alloc: Allocator) void {
        switch (self.*) {
            .match_none, .match_all => {},
            .phrase => |phrase| {
                alloc.free(phrase.field);
                for (phrase.terms) |term| alloc.free(term);
                if (phrase.terms.len > 0) alloc.free(phrase.terms);
            },
            .multi_phrase => |multi| {
                alloc.free(multi.field);
                for (multi.terms) |group| {
                    for (group) |term| alloc.free(term);
                    if (group.len > 0) alloc.free(group);
                }
                if (multi.terms.len > 0) alloc.free(multi.terms);
            },
            .term => |term| {
                alloc.free(term.field);
                alloc.free(term.term);
            },
            .match => |match| {
                alloc.free(match.field);
                alloc.free(match.text);
                if (match.analyzer) |analyzer| alloc.free(analyzer);
            },
            .multi_match_bool_prefix => |multi_match| {
                alloc.free(multi_match.query);
                for (multi_match.fields) |field| alloc.free(field.field);
                if (multi_match.fields.len > 0) alloc.free(multi_match.fields);
            },
            .match_phrase => |phrase| {
                alloc.free(phrase.field);
                alloc.free(phrase.text);
                if (phrase.analyzer) |analyzer| alloc.free(analyzer);
            },
            .fuzzy => |fuzzy| {
                alloc.free(fuzzy.field);
                alloc.free(fuzzy.term);
            },
            .numeric_range => |range| alloc.free(range.field),
            .date_range => |range| alloc.free(range.field),
            .geo_distance => |range| alloc.free(range.field),
            .geo_bbox => |range| alloc.free(range.field),
            .doc_id => |doc_id| {
                for (doc_id.ids) |id| alloc.free(id);
                if (doc_id.ids.len > 0) alloc.free(doc_id.ids);
            },
            .bool_field => |field| alloc.free(field.field),
            .prefix => |prefix| {
                alloc.free(prefix.field);
                alloc.free(prefix.prefix);
            },
            .wildcard => |wildcard| {
                alloc.free(wildcard.field);
                alloc.free(wildcard.pattern);
            },
            .regexp => |regexp| {
                alloc.free(regexp.field);
                alloc.free(regexp.pattern);
            },
            .term_range => |range| {
                alloc.free(range.field);
                if (range.min) |min| alloc.free(min);
                if (range.max) |max| alloc.free(max);
            },
            .ip_range => |range| {
                alloc.free(range.field);
                alloc.free(range.cidr);
            },
            .geo_shape => |shape| {
                alloc.free(shape.field);
                for (shape.polygons) |polygon| {
                    if (polygon.len > 0) alloc.free(polygon);
                }
                if (shape.polygons.len > 0) alloc.free(shape.polygons);
            },
            .bool_query => |bool_query| {
                for (bool_query.must) |*query| {
                    var owned = query.*;
                    owned.deinit(alloc);
                }
                if (bool_query.must.len > 0) alloc.free(bool_query.must);
                for (bool_query.should) |*query| {
                    var owned = query.*;
                    owned.deinit(alloc);
                }
                if (bool_query.should.len > 0) alloc.free(bool_query.should);
                for (bool_query.must_not) |*query| {
                    var owned = query.*;
                    owned.deinit(alloc);
                }
                if (bool_query.must_not.len > 0) alloc.free(bool_query.must_not);
            },
        }
        self.* = undefined;
    }
};

pub const DenseKnnQuery = struct {
    vector: []const f32,
    k: u32 = 10,
};

pub const SparseKnnQuery = struct {
    indices: []const u32,
    values: []const f32,
    k: u32 = 10,
};

pub const Query = union(enum) {
    match_none: void,
    match_all: void,
    phrase: struct {
        field: []const u8,
        terms: []const []const u8,
        max_edits: u8 = 0,
        auto_fuzzy: bool = false,
        boost: f32 = 1.0,
    },
    multi_phrase: struct {
        field: []const u8,
        terms: []const []const []const u8,
        max_edits: u8 = 0,
        auto_fuzzy: bool = false,
        boost: f32 = 1.0,
    },
    term: struct {
        field: []const u8,
        term: []const u8,
        boost: f32 = 1.0,
    },
    match: struct {
        field: []const u8,
        text: []const u8,
        analyzer: ?[]const u8 = null,
        boost: f32 = 1.0,
    },
    match_phrase: struct {
        field: []const u8,
        text: []const u8,
        analyzer: ?[]const u8 = null,
        max_edits: u8 = 0,
        auto_fuzzy: bool = false,
        boost: f32 = 1.0,
    },
    fuzzy: struct {
        field: []const u8,
        term: []const u8,
        max_edits: u8 = 1,
        prefix_len: u8 = 0,
        auto_fuzzy: bool = false,
        boost: f32 = 1.0,
    },
    numeric_range: struct {
        field: []const u8,
        min: ?f64 = null,
        max: ?f64 = null,
        inclusive_min: bool = true,
        inclusive_max: bool = false,
        boost: f32 = 1.0,
    },
    date_range: struct {
        field: []const u8,
        start_ns: ?u64 = null,
        end_ns: ?u64 = null,
        inclusive_start: bool = true,
        inclusive_end: bool = false,
        boost: f32 = 1.0,
    },
    doc_id: struct {
        ids: []const []const u8,
        boost: f32 = 1.0,
    },
    bool_field: struct {
        field: []const u8,
        value: bool,
        boost: f32 = 1.0,
    },
    geo_distance: struct {
        field: []const u8,
        lon: f64,
        lat: f64,
        radius_meters: f64,
        boost: f32 = 1.0,
    },
    geo_bbox: struct {
        field: []const u8,
        min_lat: f64,
        min_lon: f64,
        max_lat: f64,
        max_lon: f64,
        boost: f32 = 1.0,
    },
    prefix: struct {
        field: []const u8,
        prefix: []const u8,
        boost: f32 = 1.0,
    },
    wildcard: struct {
        field: []const u8,
        pattern: []const u8,
        boost: f32 = 1.0,
    },
    regexp: struct {
        field: []const u8,
        pattern: []const u8,
        boost: f32 = 1.0,
    },
    term_range: struct {
        field: []const u8,
        min: ?[]const u8 = null,
        max: ?[]const u8 = null,
        inclusive_min: bool = true,
        inclusive_max: bool = false,
        boost: f32 = 1.0,
    },
    ip_range: struct {
        field: []const u8,
        cidr: []const u8,
        boost: f32 = 1.0,
    },
    geo_shape: struct {
        field: []const u8,
        relation: GeoShapeRelation = .intersects,
        polygons: []const []const GeoPoint,
        boost: f32 = 1.0,
    },
    dense_knn: DenseKnnQuery,
    sparse_knn: SparseKnnQuery,
    graph: graph_query_mod.GraphQuery,
};

pub const LookupOptions = struct {
    fields: []const []const u8 = &.{},
    include_all_fields: bool = true,
};

pub const LookupResult = struct {
    json: []u8,

    pub fn deinit(self: *LookupResult, alloc: Allocator) void {
        alloc.free(self.json);
        self.* = undefined;
    }
};

pub const ScanOptions = struct {
    inclusive_from: bool = false,
    exclusive_to: bool = false,
    include_documents: bool = false,
    limit: u32 = 0,
    fields: []const []const u8 = &.{},
    include_all_fields: bool = true,
};

pub const ScanDocument = struct {
    id: []u8,
    json: []u8,

    pub fn deinit(self: *ScanDocument, alloc: Allocator) void {
        alloc.free(self.id);
        alloc.free(self.json);
        self.* = undefined;
    }
};

pub const ScanHash = struct {
    id: []u8,
    hash: u64,

    pub fn deinit(self: *ScanHash, alloc: Allocator) void {
        alloc.free(self.id);
        self.* = undefined;
    }
};

pub const ScanResult = struct {
    hashes: []ScanHash = &.{},
    documents: []ScanDocument = &.{},

    pub fn deinit(self: *ScanResult, alloc: Allocator) void {
        for (self.hashes) |*entry| entry.deinit(alloc);
        if (self.hashes.len > 0) alloc.free(self.hashes);
        for (self.documents) |*doc| doc.deinit(alloc);
        if (self.documents.len > 0) alloc.free(self.documents);
        self.* = undefined;
    }
};

pub const TxnId = transactions_mod.TxnId;
pub const TxnStatus = transactions_mod.TxnStatus;
pub const TxnRecoveryStats = transactions_mod.RecoveryStats;
pub const ByteRange = docstore_mod.ByteRange;
pub const SplitPhase = shard_mod.SplitPhase;
pub const GraphEdge = graph_mod.Edge;
pub const GraphEdgeDirection = graph_mod.EdgeDirection;
pub const GraphTraversalRules = traversal_mod.TraversalRules;
pub const GraphTraversalResult = traversal_mod.TraversalResult;
pub const GraphPathWeightMode = paths_mod.PathWeightMode;
pub const GraphPath = paths_mod.Path;

pub const TransactionWrite = struct {
    key: []const u8,
    value: []const u8,
};

pub const TransactionVersionPredicate = struct {
    key: []const u8,
    expected_version: u64,
};

pub const TransactionIntentRequest = struct {
    writes: []const TransactionWrite = &.{},
    deletes: []const []const u8 = &.{},
    transforms: []const DocumentTransform = &.{},
    predicates: []const TransactionVersionPredicate = &.{},
};

pub const SplitState = struct {
    phase: SplitPhase,
    split_key: []u8,
    new_shard_id: u64,
    started_at: u64,
    original_range_end: []u8,

    pub fn deinit(self: *SplitState, alloc: Allocator) void {
        alloc.free(self.split_key);
        alloc.free(self.original_range_end);
        self.* = undefined;
    }
};

pub fn freeSplitState(alloc: Allocator, state: ?SplitState) void {
    if (state) |owned| {
        var mutable = owned;
        mutable.deinit(alloc);
    }
}

pub const SplitDeltaEntry = struct {
    sequence: u64,
    timestamp: u64,
    writes: []BatchWrite,
    deletes: [][]u8,

    pub fn deinit(self: *SplitDeltaEntry, alloc: Allocator) void {
        for (self.writes) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        if (self.writes.len > 0) alloc.free(self.writes);
        for (self.deletes) |key| alloc.free(key);
        if (self.deletes.len > 0) alloc.free(self.deletes);
        self.* = undefined;
    }
};

pub fn freeSplitDeltaEntries(alloc: Allocator, entries: []SplitDeltaEntry) void {
    for (entries) |*entry| entry.deinit(alloc);
    if (entries.len > 0) alloc.free(entries);
}

pub fn freeParticipantIds(alloc: Allocator, items: [][]u8) void {
    transactions_mod.freeParticipantList(alloc, items);
}

pub const ExecutionContext = struct {
    io: ?std.Io = null,
    max_parallelism: ?usize = null,
};

pub const SearchRequest = struct {
    query: Query = .{ .match_all = {} },
    index_name: ?[]const u8 = null,
    primary_text_index_name: ?[]const u8 = null,
    aggregations_json: []const u8 = "",
    count_only: bool = false,
    profile: bool = false,
    full_text: ?TextQuery = null,
    filter_query_json: []const u8 = "",
    exclusion_query_json: []const u8 = "",
    full_text_queries: []const NamedFullTextQuery = &.{},
    doc_filter_bindings: []const NamedDocFilterBinding = &.{},
    dense: ?DenseKnnQuery = null,
    sparse: ?SparseKnnQuery = null,
    dense_queries: []const NamedDenseQuery = &.{},
    sparse_queries: []const NamedSparseQuery = &.{},
    graph_queries: []const NamedGraphQuery = &.{},
    merge_config: ?MergeConfig = null,
    reranker: ?reranking_mod.Config = null,
    reranker_query_text: []const u8 = "",
    pruner: ?fusion_mod.Pruner = null,
    expand_strategy: ?graph_query_mod.ExpandStrategy = null,
    return_mode: ReturnMode = .parent,
    max_chunks_per_parent: u32 = 0,
    fields: []const []const u8 = &.{},
    include_all_fields: bool = true,
    defer_stored_projection: bool = false,
    limit: u32 = 10,
    offset: u32 = 0,
    include_stored: bool = true,
    search_effort: ?f32 = null,
    filter_prefix: []const u8 = "",
    distance_over: ?f32 = null,
    distance_under: ?f32 = null,
    filter_ids: []const u64 = &.{},
    exclude_ids: []const u64 = &.{},
    filter_doc_ids: []const []const u8 = &.{},
    filter_doc_ids_positive: bool = false,
    exclude_doc_ids: []const []const u8 = &.{},
    // Internal execution hook. Public callers should use raw document IDs,
    // filter JSON, or named bindings instead of constructing this pointer.
    resolved_doc_filter: ?*const anyopaque = null,
    // Internal text-index execution hook. This is request-local state used to
    // avoid converting text-native doc nums through shard ordinals and back.
    resolved_text_doc_filter: ?*const anyopaque = null,
    resolved_doc_filter_owned: bool = false,
    resolved_doc_filter_wire_context: ?ResolvedDocFilterWireContext = null,
    identity_read_generation: ?u64 = null,
    require_algebraic_filter_resolution: bool = false,
    distributed_text_stats: []const distributed_stats_mod.TextFieldStats = &.{},
};

pub const ResolvedDocFilterWireContext = struct {
    namespace: doc_identity_mod.Namespace,
    identity_read_generation: u64,
};

pub const NamedDocFilterBinding = struct {
    name: []const u8,
    filter_query_json: []const u8,
};

pub const NamedGraphInputSet = struct {
    name: []const u8,
    hit_ids: []const []const u8 = &.{},
    total_hits: u32 = 0,
};

pub const ReturnMode = enum {
    parent,
    chunk,
    parent_with_chunks,
};

pub const NamedGraphQuery = struct {
    name: []const u8,
    query: graph_query_mod.GraphQuery,
};

pub const NamedFullTextQuery = struct {
    name: []const u8,
    index_name: []const u8,
    query: TextQuery,
};

pub const NamedDenseQuery = struct {
    name: []const u8,
    index_name: []const u8,
    query: DenseKnnQuery,
};

pub const NamedSparseQuery = struct {
    name: []const u8,
    index_name: []const u8,
    query: SparseKnnQuery,
};

pub const MergeConfig = struct {
    strategy: fusion_mod.FusionStrategy = .rrf,
    rank_constant: f64 = 60.0,
    window_size: u32 = 0,
    weights: []const fusion_mod.NamedWeight = &.{},
};

pub const SearchHit = struct {
    id: []u8,
    doc_ordinal: ?u32 = null,
    score: ?f32 = null,
    stored_data: ?[]u8 = null,
    artifact_ref: ?ArtifactRef = null,
    chunk_hits: []ChunkHit = &.{},

    pub fn clone(self: SearchHit, alloc: Allocator) !SearchHit {
        var cloned = SearchHit{
            .id = try alloc.dupe(u8, self.id),
            .doc_ordinal = self.doc_ordinal,
            .score = self.score,
            .stored_data = if (self.stored_data) |data| try alloc.dupe(u8, data) else null,
            .artifact_ref = if (self.artifact_ref) |artifact_ref| try artifact_ref.clone(alloc) else null,
            .chunk_hits = &.{},
        };
        errdefer {
            alloc.free(cloned.id);
            if (cloned.stored_data) |data| alloc.free(data);
            if (cloned.artifact_ref) |*artifact_ref| artifact_ref.deinit(alloc);
        }

        if (self.chunk_hits.len == 0) return cloned;

        cloned.chunk_hits = try alloc.alloc(ChunkHit, self.chunk_hits.len);
        var initialized: usize = 0;
        errdefer {
            for (cloned.chunk_hits[0..initialized]) |*chunk| chunk.deinit(alloc);
            alloc.free(cloned.chunk_hits);
        }
        for (self.chunk_hits, 0..) |chunk, i| {
            cloned.chunk_hits[i] = try chunk.clone(alloc);
            initialized += 1;
        }
        return cloned;
    }

    pub fn deinit(self: *SearchHit, alloc: Allocator) void {
        alloc.free(self.id);
        if (self.stored_data) |data| alloc.free(data);
        if (self.artifact_ref) |*artifact_ref| artifact_ref.deinit(alloc);
        for (self.chunk_hits) |*chunk| chunk.deinit(alloc);
        if (self.chunk_hits.len > 0) alloc.free(self.chunk_hits);
        self.* = undefined;
    }
};

pub const ChunkHit = struct {
    id: []u8,
    score: ?f32 = null,
    stored_data: ?[]u8 = null,
    artifact_ref: ?ArtifactRef = null,

    pub fn clone(self: ChunkHit, alloc: Allocator) !ChunkHit {
        return .{
            .id = try alloc.dupe(u8, self.id),
            .score = self.score,
            .stored_data = if (self.stored_data) |data| try alloc.dupe(u8, data) else null,
            .artifact_ref = if (self.artifact_ref) |artifact_ref| try artifact_ref.clone(alloc) else null,
        };
    }

    pub fn deinit(self: *ChunkHit, alloc: Allocator) void {
        alloc.free(self.id);
        if (self.stored_data) |data| alloc.free(data);
        if (self.artifact_ref) |*artifact_ref| artifact_ref.deinit(alloc);
        self.* = undefined;
    }
};

pub const TotalHitsRelation = enum {
    exact,
    gte,
};

pub const SearchResult = struct {
    alloc: Allocator,
    hits: []SearchHit,
    total_hits: u32,
    total_hits_relation: TotalHitsRelation = .exact,
    identity_read_generation: ?u64 = null,
    graph_results: []GraphSearchResult = &.{},

    pub fn deinit(self: *SearchResult) void {
        for (self.hits) |*hit| hit.deinit(self.alloc);
        if (self.hits.len > 0) self.alloc.free(self.hits);
        for (self.graph_results) |*graph_result| graph_result.deinit(self.alloc);
        if (self.graph_results.len > 0) self.alloc.free(self.graph_results);
        self.* = undefined;
    }
};

pub const GraphSearchResult = struct {
    name: []u8,
    nodes: []graph_query_mod.GraphResultNode = &.{},
    paths: []GraphPath = &.{},
    matches: []GraphPatternMatch = &.{},
    hits: []SearchHit,
    total_hits: u32,

    pub fn deinit(self: *GraphSearchResult, alloc: Allocator) void {
        alloc.free(self.name);
        for (self.nodes) |*node| node.deinit(alloc);
        if (self.nodes.len > 0) alloc.free(self.nodes);
        for (self.paths) |path| paths_mod.freePath(alloc, path);
        if (self.paths.len > 0) alloc.free(self.paths);
        for (self.matches) |*match| match.deinit(alloc);
        if (self.matches.len > 0) alloc.free(self.matches);
        for (self.hits) |*hit| hit.deinit(alloc);
        if (self.hits.len > 0) alloc.free(self.hits);
        self.* = undefined;
    }
};

pub const GraphPatternBinding = struct {
    alias: []u8,
    node: graph_query_mod.GraphResultNode,

    pub fn deinit(self: *GraphPatternBinding, alloc: Allocator) void {
        alloc.free(self.alias);
        self.node.deinit(alloc);
        self.* = undefined;
    }
};

pub const GraphPatternMatch = struct {
    bindings: []GraphPatternBinding,
    path: []graph_query_mod.PathEdgeInfo,

    pub fn deinit(self: *GraphPatternMatch, alloc: Allocator) void {
        for (self.bindings) |*binding| binding.deinit(alloc);
        if (self.bindings.len > 0) alloc.free(self.bindings);
        for (self.path) |edge| {
            alloc.free(edge.source);
            alloc.free(edge.target);
            alloc.free(edge.edge_type);
        }
        if (self.path.len > 0) alloc.free(self.path);
        self.* = undefined;
    }
};

pub const TTLCleanupStats = struct {
    enabled: bool = false,
    lease_owned: bool = false,
    has_lease: bool = false,
    acquisition_count: u64 = 0,
    runs: u64 = 0,
    scanned_timestamps: u64 = 0,
    deleted_docs: u64 = 0,
    last_run_ns: u64 = 0,
    error_count: u64 = 0,
    lease_acquire_failures: u64 = 0,
    lost_leases: u64 = 0,
    last_acquired_ms: u64 = 0,
};

pub const EnrichmentStats = struct {
    enabled: bool = false,
    lease_owned: bool = true,
    has_lease: bool = false,
    acquisition_count: u64 = 0,
    lease_acquire_failures: u64 = 0,
    lost_leases: u64 = 0,
    last_acquired_ms: u64 = 0,
    target_sequence: u64 = 0,
    applied_sequence: u64 = 0,
    processed_requests: u64 = 0,
    error_count: u64 = 0,
    retryable_error_count: u64 = 0,
    fatal_error_count: u64 = 0,
    retrying: bool = false,
    worker_failed: bool = false,
    skip_by_hash_count: u64 = 0,
    codec_decode_failures: u64 = 0,
    embed_batches_started: u64 = 0,
    embed_batches_completed: u64 = 0,
    embed_items_started: u64 = 0,
    embed_items_completed: u64 = 0,
    active_embed_batch_items: u64 = 0,
    active_embed_batch_bytes: u64 = 0,
    active_embed_batch_max_bytes: u64 = 0,
    active_embed_batch_started_ms: u64 = 0,
    last_embed_batch_items: u64 = 0,
    last_embed_batch_bytes: u64 = 0,
    last_embed_batch_max_bytes: u64 = 0,
    last_embed_batch_ns: u64 = 0,
    total_embed_ns: u64 = 0,
    dense_artifact_bytes_written: u64 = 0,
    sparse_artifact_bytes_written: u64 = 0,
    chunk_artifact_bytes_written: u64 = 0,
    artifact_bytes_written: u64 = 0,
};

pub const TransactionRecoveryStats = struct {
    enabled: bool = false,
    lease_owned: bool = false,
    has_lease: bool = false,
    acquisition_count: u64 = 0,
    lease_acquire_failures: u64 = 0,
    lost_leases: u64 = 0,
    last_acquired_ms: u64 = 0,
    runs: u64 = 0,
    scanned_records: u64 = 0,
    auto_aborted: u64 = 0,
    resolved_finalized: u64 = 0,
    cleaned_records: u64 = 0,
    kept_recent_pending: u64 = 0,
    deferred_unresolved: u64 = 0,
    notification_attempts: u64 = 0,
    notification_successes: u64 = 0,
    notification_failures: u64 = 0,
    last_run_ns: u64 = 0,
    error_count: u64 = 0,
};

pub const TextMergeStats = struct {
    enabled: bool = false,
    pending_indexes: u64 = 0,
    pending_segments: u64 = 0,
    pending_bytes: u64 = 0,
    in_flight_merges: u64 = 0,
    in_flight_segments: u64 = 0,
    completed_merges: u64 = 0,
    skipped_stale_merges: u64 = 0,
    failed_merges: u64 = 0,
    quarantined_merges: u64 = 0,
    quarantined_segments: u64 = 0,
    last_merge_error: []const u8 = "",
    retry_after_ns: u64 = 0,
    deferred_for_pressure: u64 = 0,
    backpressure_events: u64 = 0,
    backpressure_ns: u64 = 0,
    max_pending_segments: u64 = 0,
    max_pending_bytes: u64 = 0,
};

pub const DocIdentityStats = struct {
    namespace_table_id: u64 = 0,
    namespace_shard_id: u64 = 0,
    namespace_range_id: u64 = 0,
    next_ordinal: u32 = 1,
    allocated_ordinals: u64 = 0,
    ordinal_capacity_remaining: u64 = 0,
    ordinal_capacity_exhausted: bool = false,
    rebuild_required: bool = false,
    state_rows: u64 = 0,
    live_ordinals: u64 = 0,
    tombstone_ordinals: u64 = 0,
    min_created_generation: u64 = 0,
    max_created_generation: u64 = 0,
    min_deleted_generation: u64 = 0,
    max_deleted_generation: u64 = 0,
    scanned_primary_docs: u64 = 0,
    primary_docs_missing_ordinals: u64 = 0,
    primary_docs_missing_identity_state: u64 = 0,
    primary_docs_with_tombstone_ordinals: u64 = 0,
    complete: bool = false,
};

pub const DocSetPlanningStats = struct {
    resolved_set_count: u64 = 0,
    all_set_count: u64 = 0,
    none_set_count: u64 = 0,
    doc_key_list_count: u64 = 0,
    ordinal_list_count: u64 = 0,
    ordinal_bitmap_count: u64 = 0,
    doc_key_list_docs: u64 = 0,
    ordinal_list_docs: u64 = 0,
    ordinal_bitmap_docs: u64 = 0,
    missing_ordinal_coverage_count: u64 = 0,
    bitmap_promotion_count: u64 = 0,
    unsupported_filter_shape_count: u64 = 0,
    stale_identity_generation_rejection_count: u64 = 0,
};

pub const DBStats = struct {
    doc_count: u64 = 0,
    index_count: u32 = 0,
    indexes: []DBIndexStats = &.{},
    doc_identity: DocIdentityStats = .{},
    doc_set_planning: DocSetPlanningStats = .{},
    enrichment: EnrichmentStats = .{},
    ttl_cleanup: TTLCleanupStats = .{},
    transaction_recovery: TransactionRecoveryStats = .{},
    text_merge: TextMergeStats = .{},
    term_doc_freq_cache_hits: u64 = 0,
    term_doc_freq_cache_misses: u64 = 0,
    async_indexing: AsyncIndexingStats = .{},
};

pub const AlgebraicCandidateStatus = struct {
    recommendation: []const u8,
    materialization_id: []const u8,
    lifecycle: []const u8,
    decision: []const u8,
    observation_count: u64 = 0,
    estimated_scan_rows_saved: u64 = 0,
    estimated_write_cost: u64 = 0,
    estimated_tensor_rows: u64 = 0,
    estimated_storage_bytes: u64 = 0,
    estimated_write_amplification: u64 = 0,
    score: i128 = 0,
    idle_miss_count: u64 = 0,
    generation: u64 = 0,
};

pub const AlgebraicCandidateDecisionStatus = struct {
    recommendation: []const u8,
    materialization_id: []const u8,
    lifecycle: []const u8,
    previous_decision: []const u8,
    decision: []const u8,
    observation_count: u64 = 0,
    estimated_scan_rows_saved: u64 = 0,
    estimated_write_cost: u64 = 0,
    score: i128 = 0,
    score_delta: i128 = 0,
    idle_miss_count: u64 = 0,
    generation: u64 = 0,
};

pub const AlgebraicProgressStatus = struct {
    recommendation: []const u8,
    materialization_id: []const u8,
    lifecycle: []const u8,
    target_sequence: u64 = 0,
    applied_sequence: u64 = 0,
    rows_processed: u64 = 0,
    target_rows: u64 = 0,
};

pub const DBIndexStats = struct {
    name: []const u8,
    kind: IndexKind,
    doc_count: u64 = 0,
    term_count: u64 = 0,
    edge_count: u64 = 0,
    node_count: u64 = 0,
    root_node: u64 = 0,
    backfill_active: bool = false,
    backfill_progress: f64 = 0.0,
    replay_applied_sequence: u64 = 0,
    replay_target_sequence: u64 = 0,
    replay_catch_up_required: bool = false,
    catch_up_active: bool = false,
    catch_up_phase: DenseCatchUpStats.Phase = .idle,
    catch_up_applied_sequence: u64 = 0,
    catch_up_target_sequence: u64 = 0,
    text_merge: TextMergeStats = .{},
    hbc_cache: HbcCacheStats = .{},
    hbc_posting: HbcPostingStats = .{},
    algebraic_parse_error_count: u64 = 0,
    algebraic_last_error_doc_key: ?[]const u8 = null,
    algebraic_last_error_reason: ?[]const u8 = null,
    algebraic_schema_version: u32 = 0,
    algebraic_capability_fingerprint: ?[]const u8 = null,
    algebraic_capability_lifecycle_status: ?[]const u8 = null,
    algebraic_capability_change_added_fields: u32 = 0,
    algebraic_capability_change_removed_fields: u32 = 0,
    algebraic_capability_change_changed_type_fields: u32 = 0,
    algebraic_skipped_dynamic_fields: u32 = 0,
    algebraic_skipped_complex_fields: u32 = 0,
    algebraic_skipped_unbounded_fields: u32 = 0,
    algebraic_minmax_cache_hits: u64 = 0,
    algebraic_minmax_cache_misses: u64 = 0,
    algebraic_minmax_support_scans: u64 = 0,
    algebraic_planner_selected: u64 = 0,
    algebraic_planner_fallback_count: u64 = 0,
    algebraic_planner_last_decision: ?[]const u8 = null,
    algebraic_planner_last_fallback_reason: ?[]const u8 = null,
    algebraic_planner_last_estimated_scan_rows: ?u64 = null,
    algebraic_planner_last_estimated_result_buckets: ?u64 = null,
    algebraic_planner_lifecycle_ready: bool = true,
    algebraic_planner_lifecycle_blocking_reason: ?[]const u8 = null,
    algebraic_dictionary_registry_claimed_count: u64 = 0,
    algebraic_dictionary_registry_already_owned_count: u64 = 0,
    algebraic_dictionary_registry_owned_by_other_count: u64 = 0,
    algebraic_dictionary_registry_ready_hit_count: u64 = 0,
    algebraic_dictionary_registry_ready_miss_count: u64 = 0,
    algebraic_distributed_partial_validation_proven_count: u64 = 0,
    algebraic_distributed_partial_validation_rejected_count: u64 = 0,
    algebraic_distributed_partial_rows_exported_count: u64 = 0,
    algebraic_vector_filter_attempt_count: u64 = 0,
    algebraic_vector_filter_resolved_count: u64 = 0,
    algebraic_vector_filter_unsupported_count: u64 = 0,
    algebraic_vector_filter_fail_closed_count: u64 = 0,
    algebraic_vector_filter_include_doc_id_count: u64 = 0,
    algebraic_vector_filter_exclude_doc_id_count: u64 = 0,
    algebraic_graph_traversal_attempt_count: u64 = 0,
    algebraic_graph_traversal_proven_count: u64 = 0,
    algebraic_graph_traversal_rejected_count: u64 = 0,
    algebraic_graph_traversal_fallback_count: u64 = 0,
    algebraic_graph_traversal_result_node_count: u64 = 0,
    algebraic_observed_query_shape_count: u64 = 0,
    algebraic_recommendation_count: u64 = 0,
    algebraic_adaptive_candidate_count: u64 = 0,
    algebraic_adaptive_progress_count: u64 = 0,
    algebraic_adaptive_backfilling_count: u64 = 0,
    algebraic_adaptive_ready_count: u64 = 0,
    algebraic_adaptive_stale_count: u64 = 0,
    algebraic_adaptive_dematerialize_recommended_count: u64 = 0,
    algebraic_adaptive_decision_history_count: u64 = 0,
    algebraic_adaptive_policy_drift_count: u64 = 0,
    algebraic_last_observed_query_shape: ?[]const u8 = null,
    algebraic_last_recommended_materialization: ?[]const u8 = null,
    algebraic_top_candidate: ?AlgebraicCandidateStatus = null,
    algebraic_active_progress: ?AlgebraicProgressStatus = null,
    algebraic_candidates: []const AlgebraicCandidateStatus = &.{},
    algebraic_candidate_decision_history: []const AlgebraicCandidateDecisionStatus = &.{},
    algebraic_progress: []const AlgebraicProgressStatus = &.{},
};

pub const AlgebraicMaterializationState = struct {
    index_name: []u8,
    recommendation: []u8,
    lifecycle: []u8,
    observation_count: u64 = 0,

    pub fn deinit(self: *AlgebraicMaterializationState, alloc: Allocator) void {
        alloc.free(self.index_name);
        alloc.free(self.recommendation);
        alloc.free(self.lifecycle);
        self.* = undefined;
    }
};

pub fn freeAlgebraicMaterializationStates(alloc: Allocator, states: []AlgebraicMaterializationState) void {
    for (states) |*state| state.deinit(alloc);
    if (states.len > 0) alloc.free(states);
}

pub const AlgebraicQueryObservation = struct {
    index_name: []u8,
    shape: []u8,
    count: u64 = 0,
    reason: []u8,
    recommendation: ?[]u8 = null,
    lifecycle: []u8,

    pub fn deinit(self: *AlgebraicQueryObservation, alloc: Allocator) void {
        alloc.free(self.index_name);
        alloc.free(self.shape);
        alloc.free(self.reason);
        if (self.recommendation) |value| alloc.free(value);
        alloc.free(self.lifecycle);
        self.* = undefined;
    }
};

pub fn freeAlgebraicQueryObservations(alloc: Allocator, observations: []AlgebraicQueryObservation) void {
    for (observations) |*observation| observation.deinit(alloc);
    if (observations.len > 0) alloc.free(observations);
}

pub const AlgebraicAdaptiveCandidate = struct {
    index_name: []u8,
    recommendation: []u8,
    materialization_id: []u8,
    lifecycle: []u8,
    observation_count: u64 = 0,
    estimated_scan_rows_saved: u64 = 0,
    estimated_write_cost: u64 = 0,
    estimated_doc_rows: u64 = 0,
    estimated_bucket_cardinality: u64 = 0,
    estimated_tensor_rows: u64 = 0,
    estimated_storage_bytes: u64 = 0,
    estimated_write_amplification: u64 = 0,
    score: i128 = 0,
    decision: []u8,
    idle_miss_count: u64 = 0,
    generation: u64 = 0,

    pub fn deinit(self: *AlgebraicAdaptiveCandidate, alloc: Allocator) void {
        alloc.free(self.index_name);
        alloc.free(self.recommendation);
        alloc.free(self.materialization_id);
        alloc.free(self.lifecycle);
        alloc.free(self.decision);
        self.* = undefined;
    }
};

pub fn freeAlgebraicAdaptiveCandidates(alloc: Allocator, candidates: []AlgebraicAdaptiveCandidate) void {
    for (candidates) |*candidate| candidate.deinit(alloc);
    if (candidates.len > 0) alloc.free(candidates);
}

pub const AlgebraicAdaptiveProgress = struct {
    index_name: []u8,
    recommendation: []u8,
    materialization_id: []u8,
    lifecycle: []u8,
    target_sequence: u64 = 0,
    applied_sequence: u64 = 0,
    rows_processed: u64 = 0,
    target_rows: u64 = 0,

    pub fn deinit(self: *AlgebraicAdaptiveProgress, alloc: Allocator) void {
        alloc.free(self.index_name);
        alloc.free(self.recommendation);
        alloc.free(self.materialization_id);
        alloc.free(self.lifecycle);
        self.* = undefined;
    }
};

pub fn freeAlgebraicAdaptiveProgress(alloc: Allocator, progress: []AlgebraicAdaptiveProgress) void {
    for (progress) |*item| item.deinit(alloc);
    if (progress.len > 0) alloc.free(progress);
}

pub const HbcPostingStats = struct {
    scanned_nodes: u64 = 0,
    scanned_postings: u64 = 0,
    dirty_postings: u64 = 0,
    centroid_dirty_postings: u64 = 0,
    payload_dirty_postings: u64 = 0,
    max_centroid_version_lag: u64 = 0,
    max_payload_version_lag: u64 = 0,
    max_mutation_version: u64 = 0,
    skipped_missing: u64 = 0,
    maintenance_scanned_nodes: u64 = 0,
    maintenance_scanned_postings: u64 = 0,
    maintenance_dirty_postings: u64 = 0,
    maintenance_repaired_postings: u64 = 0,
    maintenance_centroid_refreshed: u64 = 0,
    maintenance_payload_refreshed: u64 = 0,
    maintenance_ancestor_refresh_roots: u64 = 0,
    maintenance_split_postings: u64 = 0,
    maintenance_merged_postings: u64 = 0,
    maintenance_boundary_reassigned_vectors: u64 = 0,
    lazy_centroid_deferrals: u64 = 0,
    lazy_payload_deferrals: u64 = 0,
    lazy_ancestor_deferrals: u64 = 0,
};

pub const HbcCacheKindStats = struct {
    used_bytes: u64 = 0,
    peak_bytes: u64 = 0,
    insertions: u64 = 0,
    admission_skips: u64 = 0,
    evictions: u64 = 0,
};

pub const HbcCacheStats = struct {
    total_bytes: u64 = 0,
    accounted_bytes: u64 = 0,
    node: HbcCacheKindStats = .{},
    quantized: HbcCacheKindStats = .{},
    vector: HbcCacheKindStats = .{},
    metadata: HbcCacheKindStats = .{},
};

pub const DBMutexStats = struct {
    lock_calls: u64 = 0,
    contended_calls: u64 = 0,
    max_waiters: u64 = 0,
    spin_loops: u64 = 0,
    yield_loops: u64 = 0,
    sleep_loops: u64 = 0,
    wait_ns: u64 = 0,
    max_wait_ns: u64 = 0,
    hold_ns: u64 = 0,
    max_hold_ns: u64 = 0,
};

pub const AppliedSequenceStats = struct {
    note_calls: u64 = 0,
    forced_flush_calls: u64 = 0,
    skipped_flush_calls: u64 = 0,
    flush_calls: u64 = 0,
    flushed_indexes: u64 = 0,
    sync_ns: u64 = 0,
    save_ns: u64 = 0,
    flush_ns: u64 = 0,
    max_flush_ns: u64 = 0,
};

pub const DenseCatchUpStats = struct {
    pub const Phase = enum(u8) {
        idle = 0,
        replay = 1,
        bulk_finish = 2,
        bulk_split = 3,
        bulk_publish = 4,
        applied_sequence_flush = 5,
    };

    begin_calls: u64 = 0,
    finish_calls: u64 = 0,
    abort_calls: u64 = 0,
    active: bool = false,
    phase: Phase = .idle,
    current_sequence: u64 = 0,
    current_target_sequence: u64 = 0,
    current_scanned_entries: u64 = 0,
    current_applied_entries: u64 = 0,
    progress_updates: u64 = 0,
    bulk_finish_windows: u64 = 0,
    bulk_finish_split_steps: u64 = 0,
    bulk_finish_deferred_leaf_splits: u64 = 0,
    bulk_finish_current_window: u64 = 0,
    bulk_finish_current_window_split_steps: u64 = 0,
    bulk_finish_current_window_ns: u64 = 0,
    bulk_finish_max_window_ns: u64 = 0,
    finish_ns: u64 = 0,
    max_finish_ns: u64 = 0,
    finalize_ns: u64 = 0,
    max_finalize_ns: u64 = 0,
    maintenance_calls: u64 = 0,
    maintenance_steps: u64 = 0,
    maintenance_ns: u64 = 0,
    max_maintenance_ns: u64 = 0,
    manifest_writes: u64 = 0,
    manifest_ns: u64 = 0,
    write_pressure_compactions: u64 = 0,
    write_pressure_ns: u64 = 0,
};

pub const StartupCatchUpPhase = enum(u8) {
    idle = 0,
    opening_db = 1,
    artifact_rebuild = 2,
    startup_catch_up = 3,
};

pub const StartupCatchUpStats = struct {
    active: bool = false,
    phase: StartupCatchUpPhase = .idle,
    wal_retention_known: bool = false,
    wal_retained_segments: u64 = 0,
    wal_retained_bytes: u64 = 0,
    configured_indexes: u32 = 0,
    configured_dense_indexes: u32 = 0,
    configured_sparse_indexes: u32 = 0,
    configured_full_text_indexes: u32 = 0,
    configured_graph_indexes: u32 = 0,
    opened_indexes: u32 = 0,
    db_open_ns: u64 = 0,
    load_indexes_ns: u64 = 0,
    wal_replay_records: u64 = 0,
    wal_replay_entries: u64 = 0,
    wal_replay_bytes: u64 = 0,
    wal_replay_ns: u64 = 0,
};

pub const AsyncIndexingStats = struct {
    apply_mutex: DBMutexStats = .{},
    applied_sequence_mutex: DBMutexStats = .{},
    dense_finish_mutex: DBMutexStats = .{},
    applied_sequence: AppliedSequenceStats = .{},
    startup: StartupCatchUpStats = .{},
    dense_catch_up: DenseCatchUpStats = .{},
    bulk_coalescing: BulkCoalescingStats = .{},
};

pub const BulkCoalescingStats = struct {
    active_session: bool = false,
    staged_keys: u64 = 0,
    stage_batches: u64 = 0,
    stage_writes: u64 = 0,
    stage_deletes: u64 = 0,
    stage_transforms: u64 = 0,
    flush_calls: u64 = 0,
    flushed_keys: u64 = 0,
};

pub fn accumulateDbMutexStats(dst: *DBMutexStats, src: DBMutexStats) void {
    dst.lock_calls += src.lock_calls;
    dst.contended_calls += src.contended_calls;
    dst.max_waiters = @max(dst.max_waiters, src.max_waiters);
    dst.spin_loops += src.spin_loops;
    dst.yield_loops += src.yield_loops;
    dst.sleep_loops += src.sleep_loops;
    dst.wait_ns += src.wait_ns;
    dst.max_wait_ns = @max(dst.max_wait_ns, src.max_wait_ns);
    dst.hold_ns += src.hold_ns;
    dst.max_hold_ns = @max(dst.max_hold_ns, src.max_hold_ns);
}

pub fn accumulateAppliedSequenceStats(dst: *AppliedSequenceStats, src: AppliedSequenceStats) void {
    dst.note_calls += src.note_calls;
    dst.forced_flush_calls += src.forced_flush_calls;
    dst.skipped_flush_calls += src.skipped_flush_calls;
    dst.flush_calls += src.flush_calls;
    dst.flushed_indexes += src.flushed_indexes;
    dst.sync_ns += src.sync_ns;
    dst.save_ns += src.save_ns;
    dst.flush_ns += src.flush_ns;
    dst.max_flush_ns = @max(dst.max_flush_ns, src.max_flush_ns);
}

pub fn accumulateDenseCatchUpStats(dst: *DenseCatchUpStats, src: DenseCatchUpStats) void {
    dst.begin_calls += src.begin_calls;
    dst.finish_calls += src.finish_calls;
    dst.abort_calls += src.abort_calls;
    dst.active = dst.active or src.active;
    if (@intFromEnum(src.phase) > @intFromEnum(dst.phase)) dst.phase = src.phase;
    dst.current_sequence = @max(dst.current_sequence, src.current_sequence);
    dst.current_target_sequence = @max(dst.current_target_sequence, src.current_target_sequence);
    dst.current_scanned_entries += src.current_scanned_entries;
    dst.current_applied_entries += src.current_applied_entries;
    dst.progress_updates += src.progress_updates;
    dst.bulk_finish_windows += src.bulk_finish_windows;
    dst.bulk_finish_split_steps += src.bulk_finish_split_steps;
    dst.bulk_finish_deferred_leaf_splits = @max(dst.bulk_finish_deferred_leaf_splits, src.bulk_finish_deferred_leaf_splits);
    dst.bulk_finish_current_window = @max(dst.bulk_finish_current_window, src.bulk_finish_current_window);
    dst.bulk_finish_current_window_split_steps = @max(dst.bulk_finish_current_window_split_steps, src.bulk_finish_current_window_split_steps);
    dst.bulk_finish_current_window_ns = @max(dst.bulk_finish_current_window_ns, src.bulk_finish_current_window_ns);
    dst.bulk_finish_max_window_ns = @max(dst.bulk_finish_max_window_ns, src.bulk_finish_max_window_ns);
    dst.finish_ns += src.finish_ns;
    dst.max_finish_ns = @max(dst.max_finish_ns, src.max_finish_ns);
    dst.finalize_ns += src.finalize_ns;
    dst.max_finalize_ns = @max(dst.max_finalize_ns, src.max_finalize_ns);
    dst.maintenance_calls += src.maintenance_calls;
    dst.maintenance_steps += src.maintenance_steps;
    dst.maintenance_ns += src.maintenance_ns;
    dst.max_maintenance_ns = @max(dst.max_maintenance_ns, src.max_maintenance_ns);
    dst.manifest_writes += src.manifest_writes;
    dst.manifest_ns += src.manifest_ns;
    dst.write_pressure_compactions += src.write_pressure_compactions;
    dst.write_pressure_ns += src.write_pressure_ns;
}

pub fn accumulateStartupCatchUpStats(dst: *StartupCatchUpStats, src: StartupCatchUpStats) void {
    dst.active = dst.active or src.active;
    if (@intFromEnum(src.phase) > @intFromEnum(dst.phase)) dst.phase = src.phase;
    dst.wal_retention_known = dst.wal_retention_known or src.wal_retention_known;
    dst.wal_retained_segments += src.wal_retained_segments;
    dst.wal_retained_bytes += src.wal_retained_bytes;
    dst.configured_indexes = @max(dst.configured_indexes, src.configured_indexes);
    dst.configured_dense_indexes = @max(dst.configured_dense_indexes, src.configured_dense_indexes);
    dst.configured_sparse_indexes = @max(dst.configured_sparse_indexes, src.configured_sparse_indexes);
    dst.configured_full_text_indexes = @max(dst.configured_full_text_indexes, src.configured_full_text_indexes);
    dst.configured_graph_indexes = @max(dst.configured_graph_indexes, src.configured_graph_indexes);
    dst.opened_indexes = @max(dst.opened_indexes, src.opened_indexes);
    dst.db_open_ns = @max(dst.db_open_ns, src.db_open_ns);
    dst.load_indexes_ns = @max(dst.load_indexes_ns, src.load_indexes_ns);
    dst.wal_replay_records += src.wal_replay_records;
    dst.wal_replay_entries += src.wal_replay_entries;
    dst.wal_replay_bytes += src.wal_replay_bytes;
    dst.wal_replay_ns += src.wal_replay_ns;
}

pub fn accumulateAsyncIndexingStats(dst: *AsyncIndexingStats, src: AsyncIndexingStats) void {
    accumulateDbMutexStats(&dst.apply_mutex, src.apply_mutex);
    accumulateDbMutexStats(&dst.applied_sequence_mutex, src.applied_sequence_mutex);
    accumulateDbMutexStats(&dst.dense_finish_mutex, src.dense_finish_mutex);
    accumulateAppliedSequenceStats(&dst.applied_sequence, src.applied_sequence);
    accumulateStartupCatchUpStats(&dst.startup, src.startup);
    accumulateDenseCatchUpStats(&dst.dense_catch_up, src.dense_catch_up);
    dst.bulk_coalescing.active_session = dst.bulk_coalescing.active_session or src.bulk_coalescing.active_session;
    dst.bulk_coalescing.staged_keys = @max(dst.bulk_coalescing.staged_keys, src.bulk_coalescing.staged_keys);
    dst.bulk_coalescing.stage_batches += src.bulk_coalescing.stage_batches;
    dst.bulk_coalescing.stage_writes += src.bulk_coalescing.stage_writes;
    dst.bulk_coalescing.stage_deletes += src.bulk_coalescing.stage_deletes;
    dst.bulk_coalescing.stage_transforms += src.bulk_coalescing.stage_transforms;
    dst.bulk_coalescing.flush_calls += src.bulk_coalescing.flush_calls;
    dst.bulk_coalescing.flushed_keys += src.bulk_coalescing.flushed_keys;
}

pub fn freeDBStats(alloc: Allocator, stats: DBStats) void {
    for (stats.indexes) |item| {
        alloc.free(item.name);
        if (item.algebraic_last_error_doc_key) |value| alloc.free(value);
        if (item.algebraic_last_error_reason) |value| alloc.free(value);
        if (item.algebraic_capability_fingerprint) |value| alloc.free(value);
        if (item.algebraic_capability_lifecycle_status) |value| alloc.free(value);
        if (item.algebraic_planner_last_decision) |value| alloc.free(value);
        if (item.algebraic_planner_last_fallback_reason) |value| alloc.free(value);
        if (item.algebraic_planner_lifecycle_blocking_reason) |value| alloc.free(value);
        if (item.algebraic_last_observed_query_shape) |value| alloc.free(value);
        if (item.algebraic_last_recommended_materialization) |value| alloc.free(value);
        if (item.algebraic_top_candidate) |candidate| {
            alloc.free(candidate.recommendation);
            alloc.free(candidate.materialization_id);
            alloc.free(candidate.lifecycle);
            alloc.free(candidate.decision);
        }
        if (item.algebraic_active_progress) |progress| {
            alloc.free(progress.recommendation);
            alloc.free(progress.materialization_id);
            alloc.free(progress.lifecycle);
        }
        for (item.algebraic_candidates) |candidate| {
            alloc.free(candidate.recommendation);
            alloc.free(candidate.materialization_id);
            alloc.free(candidate.lifecycle);
            alloc.free(candidate.decision);
        }
        if (item.algebraic_candidates.len > 0) alloc.free(item.algebraic_candidates);
        for (item.algebraic_candidate_decision_history) |entry| {
            alloc.free(entry.recommendation);
            alloc.free(entry.materialization_id);
            alloc.free(entry.lifecycle);
            alloc.free(entry.previous_decision);
            alloc.free(entry.decision);
        }
        if (item.algebraic_candidate_decision_history.len > 0) alloc.free(item.algebraic_candidate_decision_history);
        for (item.algebraic_progress) |progress| {
            alloc.free(progress.recommendation);
            alloc.free(progress.materialization_id);
            alloc.free(progress.lifecycle);
        }
        if (item.algebraic_progress.len > 0) alloc.free(item.algebraic_progress);
    }
    if (stats.indexes.len > 0) alloc.free(stats.indexes);
}
