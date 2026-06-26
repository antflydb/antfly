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
const schema_mod = @import("../schema.zig");
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
    if (std.mem.eql(u8, text, "query")) return .full_text;
    if (std.mem.eql(u8, text, "enrichments")) return .enrichments;
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
        .full_text => "query",
        .enrichments => "enrichments",
        .aknn, .full_index => "full_index",
    };
}

test "public sync level text maps query and rejects deprecated spellings" {
    try std.testing.expectEqual(SyncLevel.full_text, parsePublicSyncLevelText("query").?);
    try std.testing.expect(parsePublicSyncLevelText("full_text") == null);
    try std.testing.expect(parsePublicSyncLevelText("aknn") == null);
    try std.testing.expectEqual(SyncLevel.full_index, parsePublicSyncLevelText("full_index").?);
    try std.testing.expectEqualStrings("query", publicSyncLevelText(.full_text));
    try std.testing.expectEqualStrings("full_index", publicSyncLevelText(.aknn));
    try std.testing.expectEqualStrings("full_index", publicSyncLevelText(.full_index));
}

pub const BatchWrite = struct {
    key: []const u8,
    value: []const u8,
};

pub const TransformOpType = enum {
    set,
    set_on_insert,
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
    relational_identity_rewrites: []const RelationalIdentityRewrite = &.{},
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
    asset,
    embedding,
};

pub const ArtifactKind = enum {
    chunk,
    asset,
    embedding,
};

pub const ArtifactSourceRef = struct {
    kind: ArtifactKind,
    name: []u8,
    chunk_id: ?u32 = null,
    unit_id: ?[]u8 = null,

    pub fn clone(self: ArtifactSourceRef, alloc: Allocator) !ArtifactSourceRef {
        return .{
            .kind = self.kind,
            .name = try alloc.dupe(u8, self.name),
            .chunk_id = self.chunk_id,
            .unit_id = if (self.unit_id) |unit_id| try alloc.dupe(u8, unit_id) else null,
        };
    }

    pub fn deinit(self: *ArtifactSourceRef, alloc: Allocator) void {
        alloc.free(self.name);
        if (self.unit_id) |unit_id| alloc.free(unit_id);
        self.* = undefined;
    }
};

pub const ArtifactRef = struct {
    document_id: []u8,
    name: []u8,
    kind: ArtifactKind,
    chunk_id: ?u32 = null,
    unit_id: ?[]u8 = null,
    source: ?ArtifactSourceRef = null,

    pub fn clone(self: ArtifactRef, alloc: Allocator) !ArtifactRef {
        return .{
            .document_id = try alloc.dupe(u8, self.document_id),
            .name = try alloc.dupe(u8, self.name),
            .kind = self.kind,
            .chunk_id = self.chunk_id,
            .unit_id = if (self.unit_id) |unit_id| try alloc.dupe(u8, unit_id) else null,
            .source = if (self.source) |source| try source.clone(alloc) else null,
        };
    }

    pub fn deinit(self: *ArtifactRef, alloc: Allocator) void {
        alloc.free(self.document_id);
        alloc.free(self.name);
        if (self.unit_id) |unit_id| alloc.free(unit_id);
        if (self.source) |*source| source.deinit(alloc);
        self.* = undefined;
    }
};

pub const EnrichmentConfig = struct {
    name: []const u8,
    kind: EnrichmentKind,
    field: []const u8 = "",
    template: []const u8 = "",
    source_artifact_name: []const u8 = "",
    expected_dims: u32 = 0,
    chunk_size: u32 = 0,
    chunk_overlap: u32 = 0,
    chunker_json: []const u8 = "",
    full_text_index: bool = false,
    content_type: []const u8 = "",
    producer_json: []const u8 = "",

    pub fn clone(alloc: Allocator, cfg: EnrichmentConfig) !EnrichmentConfig {
        return .{
            .name = try alloc.dupe(u8, cfg.name),
            .kind = cfg.kind,
            .field = if (cfg.field.len > 0) try alloc.dupe(u8, cfg.field) else "",
            .template = if (cfg.template.len > 0) try alloc.dupe(u8, cfg.template) else "",
            .source_artifact_name = if (cfg.source_artifact_name.len > 0) try alloc.dupe(u8, cfg.source_artifact_name) else "",
            .expected_dims = cfg.expected_dims,
            .chunk_size = cfg.chunk_size,
            .chunk_overlap = cfg.chunk_overlap,
            .chunker_json = if (cfg.chunker_json.len > 0) try alloc.dupe(u8, cfg.chunker_json) else "",
            .full_text_index = cfg.full_text_index,
            .content_type = if (cfg.content_type.len > 0) try alloc.dupe(u8, cfg.content_type) else "",
            .producer_json = if (cfg.producer_json.len > 0) try alloc.dupe(u8, cfg.producer_json) else "",
        };
    }

    pub fn deinit(self: *EnrichmentConfig, alloc: Allocator) void {
        alloc.free(self.name);
        if (self.field.len > 0) alloc.free(self.field);
        if (self.template.len > 0) alloc.free(self.template);
        if (self.source_artifact_name.len > 0) alloc.free(self.source_artifact_name);
        if (self.chunker_json.len > 0) alloc.free(self.chunker_json);
        if (self.content_type.len > 0) alloc.free(self.content_type);
        if (self.producer_json.len > 0) alloc.free(self.producer_json);
        self.* = undefined;
    }
};

pub fn freeEnrichmentConfigs(alloc: Allocator, configs: []EnrichmentConfig) void {
    for (configs) |*cfg| cfg.deinit(alloc);
    if (configs.len > 0) alloc.free(configs);
}

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

pub const DocumentArtifactChildRange = struct {
    range_id: []u8,
    range_kind: []u8,
    artifact_name: []u8,
    split_boundary: []u8,
    placement: []u8,
    owner_group_id: ?u64 = null,
    placement_generation: ?u64 = null,
    route_status: ?[]u8 = null,
    split_eligible: ?bool = null,
    start_key: []u8,
    end_key_exclusive: []u8,
    last_key: []u8,
    child_count: usize = 0,
    text_bytes: ?usize = null,

    pub fn deinit(self: *DocumentArtifactChildRange, alloc: Allocator) void {
        alloc.free(self.range_id);
        alloc.free(self.range_kind);
        alloc.free(self.artifact_name);
        alloc.free(self.split_boundary);
        alloc.free(self.placement);
        if (self.route_status) |value| alloc.free(value);
        alloc.free(self.start_key);
        alloc.free(self.end_key_exclusive);
        alloc.free(self.last_key);
        self.* = undefined;
    }
};

pub const DocumentArtifactChildRangePlacementUpdate = struct {
    range_id: []const u8,
    placement: []const u8,
    owner_group_id: ?u64 = null,
    placement_generation: ?u64 = null,
    route_status: ?[]const u8 = null,
    split_eligible: ?bool = null,
};

pub const DocumentArtifactManifest = struct {
    document_id: []u8,
    artifact_name: []u8,
    artifact_id: []u8,
    manifest_json: []u8,
    state_json: ?[]u8 = null,
    manifest_version: u64 = 0,
    generation: u64 = 0,
    source_url: []u8 = "",
    source_fingerprint: []u8 = "",
    content_type: []u8 = "",
    route_type: []u8 = "",
    unsupported_reason: ?[]u8 = null,
    unit_count: usize = 0,
    chunk_count: usize = 0,
    child_ranges: []DocumentArtifactChildRange = &.{},
    child_range_count: usize = 0,
    merge_status: []u8 = "",
    merge_from_generation: u64 = 0,
    merge_to_generation: u64 = 0,
    merge_operation_granularity: []u8 = "",
    merge_operation_count: usize = 0,
    last_error_code: ?[]u8 = null,
    last_error_message: ?[]u8 = null,

    pub fn deinit(self: *DocumentArtifactManifest, alloc: Allocator) void {
        alloc.free(self.document_id);
        alloc.free(self.artifact_name);
        alloc.free(self.artifact_id);
        alloc.free(self.manifest_json);
        if (self.state_json) |state_json| alloc.free(state_json);
        if (self.source_url.len > 0) alloc.free(self.source_url);
        if (self.source_fingerprint.len > 0) alloc.free(self.source_fingerprint);
        if (self.content_type.len > 0) alloc.free(self.content_type);
        if (self.route_type.len > 0) alloc.free(self.route_type);
        if (self.unsupported_reason) |unsupported_reason| alloc.free(unsupported_reason);
        for (self.child_ranges) |*child_range| child_range.deinit(alloc);
        if (self.child_ranges.len > 0) alloc.free(self.child_ranges);
        if (self.merge_status.len > 0) alloc.free(self.merge_status);
        if (self.merge_operation_granularity.len > 0) alloc.free(self.merge_operation_granularity);
        if (self.last_error_code) |value| alloc.free(value);
        if (self.last_error_message) |value| alloc.free(value);
        self.* = undefined;
    }
};

pub const DocumentArtifactManifestList = struct {
    document_id: []u8,
    artifacts: []DocumentArtifactManifest,

    pub fn deinit(self: *DocumentArtifactManifestList, alloc: Allocator) void {
        alloc.free(self.document_id);
        for (self.artifacts) |*artifact| artifact.deinit(alloc);
        alloc.free(self.artifacts);
        self.* = undefined;
    }
};

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

pub const DocumentArtifactReprocessShardResume = struct {
    group_id: ?u64 = null,
    next_key: []const u8,
    limit: u32 = 0,
};

pub const DocumentArtifactTableReprocessRequest = struct {
    from_key: []const u8 = "",
    to_key: []const u8 = "",
    limit: u32 = 100,
    shard_cursors: []const DocumentArtifactReprocessShardResume = &.{},
};

pub const DocumentArtifactReprocessFailure = struct {
    key: []u8,
    error_code: []u8,

    pub fn deinit(self: *DocumentArtifactReprocessFailure, alloc: Allocator) void {
        alloc.free(self.key);
        alloc.free(self.error_code);
        self.* = undefined;
    }
};

pub const DocumentArtifactReprocessShardCursor = struct {
    group_id: ?u64 = null,
    next_key: []u8,
    scanned: usize = 0,
    reprocessed: usize = 0,
    skipped: usize = 0,
    failed: usize = 0,
    limit: u32 = 0,

    pub fn deinit(self: *DocumentArtifactReprocessShardCursor, alloc: Allocator) void {
        alloc.free(self.next_key);
        self.* = undefined;
    }
};

pub const DocumentArtifactTableReprocessResult = struct {
    scanned: usize = 0,
    reprocessed: usize = 0,
    skipped: usize = 0,
    failed: usize = 0,
    limit: u32 = 0,
    next_key: ?[]u8 = null,
    failures: []DocumentArtifactReprocessFailure = &.{},
    shard_cursors: []DocumentArtifactReprocessShardCursor = &.{},

    pub fn deinit(self: *DocumentArtifactTableReprocessResult, alloc: Allocator) void {
        if (self.next_key) |value| alloc.free(value);
        for (self.failures) |*failure| failure.deinit(alloc);
        if (self.failures.len > 0) alloc.free(self.failures);
        for (self.shard_cursors) |*cursor| cursor.deinit(alloc);
        if (self.shard_cursors.len > 0) alloc.free(self.shard_cursors);
        self.* = undefined;
    }
};

pub const RelationalRowsQueryOrderDirection = enum {
    asc,
    desc,
};

pub const RelationalRowsQueryOrderNullTest = enum {
    is_null,
    is_not_null,
};

pub const RelationalRowsQueryOrder = struct {
    field: []const u8 = "",
    expression: ?RelationalRowsExpression = null,
    direction: RelationalRowsQueryOrderDirection = .asc,
    null_test: ?RelationalRowsQueryOrderNullTest = null,
};

pub const RelationalRowsArrayAnyPredicate = struct {
    field: []const u8,
    value_json: []const u8,
};

pub const RelationalRowsArrayContainsPredicate = struct {
    field: []const u8,
    value_json: []const u8,
};

pub const RelationalRowsArrayEqPredicate = struct {
    field: []const u8,
    value_json: []const u8,
};

pub const RelationalRowsInPredicate = struct {
    field: []const u8,
    values_json: []const u8,
    negated: bool = false,
};

pub const RelationalRowsJsonContainsPredicate = struct {
    field: []const u8,
    value_json: []const u8,
};

pub const RelationalRowsJsonPathEqPredicate = struct {
    field: []const u8,
    path: []const u8,
    value_json: []const u8,
};

pub const RelationalRowsJsonPathExistsPredicate = struct {
    field: []const u8,
    path: []const u8,
};

pub const RelationalRowsPredicateGroup = struct {
    predicates: []const schema_mod.RelationalCheck = &.{},
};

pub const RelationalRowsAccessPredicateGroup = struct {
    predicates: []const schema_mod.RelationalCheck = &.{},
    array_any: []const RelationalRowsArrayAnyPredicate = &.{},
    array_contains: []const RelationalRowsArrayContainsPredicate = &.{},
    array_eq: []const RelationalRowsArrayEqPredicate = &.{},
    in_predicates: []const RelationalRowsInPredicate = &.{},
    json_contains: []const RelationalRowsJsonContainsPredicate = &.{},
    json_path_eq: []const RelationalRowsJsonPathEqPredicate = &.{},
    json_path_exists: []const RelationalRowsJsonPathExistsPredicate = &.{},
    text_patterns: []const RelationalRowsTextPatternPredicate = &.{},
};

fn freeRelationalRowsAccessPredicateGroup(alloc: Allocator, group: RelationalRowsAccessPredicateGroup) void {
    for (group.predicates) |predicate| {
        alloc.free(predicate.field);
        if (predicate.value_json) |value_json| alloc.free(value_json);
    }
    if (group.predicates.len > 0) alloc.free(group.predicates);
    for (group.array_any) |predicate| {
        alloc.free(predicate.field);
        alloc.free(predicate.value_json);
    }
    if (group.array_any.len > 0) alloc.free(group.array_any);
    for (group.array_contains) |predicate| {
        alloc.free(predicate.field);
        alloc.free(predicate.value_json);
    }
    if (group.array_contains.len > 0) alloc.free(group.array_contains);
    for (group.array_eq) |predicate| {
        alloc.free(predicate.field);
        alloc.free(predicate.value_json);
    }
    if (group.array_eq.len > 0) alloc.free(group.array_eq);
    for (group.in_predicates) |predicate| {
        alloc.free(predicate.field);
        alloc.free(predicate.values_json);
    }
    if (group.in_predicates.len > 0) alloc.free(group.in_predicates);
    for (group.json_contains) |predicate| {
        alloc.free(predicate.field);
        alloc.free(predicate.value_json);
    }
    if (group.json_contains.len > 0) alloc.free(group.json_contains);
    for (group.json_path_eq) |predicate| {
        alloc.free(predicate.field);
        alloc.free(predicate.path);
        alloc.free(predicate.value_json);
    }
    if (group.json_path_eq.len > 0) alloc.free(group.json_path_eq);
    for (group.json_path_exists) |predicate| {
        alloc.free(predicate.field);
        alloc.free(predicate.path);
    }
    if (group.json_path_exists.len > 0) alloc.free(group.json_path_exists);
    for (group.text_patterns) |predicate| {
        alloc.free(predicate.field);
        alloc.free(predicate.pattern);
    }
    if (group.text_patterns.len > 0) alloc.free(group.text_patterns);
}

pub const RelationalRowsJsonExtractProjection = struct {
    output: []const u8,
    field: []const u8,
    path: []const u8,
    as_text: bool = false,
};

pub const RelationalRowsArrayLengthProjection = struct {
    output: []const u8,
    field: []const u8,
};

pub const RelationalRowsCoalesceOperandKind = enum {
    field,
    value,
};

pub const RelationalRowsCoalesceOperand = struct {
    kind: RelationalRowsCoalesceOperandKind,
    field: []const u8 = "",
    value_json: []const u8 = "",
};

pub const RelationalRowsCoalesceProjection = struct {
    output: []const u8,
    operands: []const RelationalRowsCoalesceOperand = &.{},
};

pub const RelationalRowsFieldAliasProjection = struct {
    output: []const u8,
    field: []const u8,
};

pub const RelationalRowsExpressionKind = schema_mod.RelationalRowsExpressionKind;
pub const RelationalRowsExpressionFieldSource = schema_mod.RelationalRowsExpressionFieldSource;
pub const RelationalRowsExpressionCastType = schema_mod.RelationalRowsExpressionCastType;
pub const RelationalRowsExpressionCondition = schema_mod.RelationalRowsExpressionCondition;
pub const RelationalRowsExpressionPredicateGroup = schema_mod.RelationalRowsExpressionPredicateGroup;
pub const RelationalRowsExpressionArrayContainsPredicate = schema_mod.RelationalRowsExpressionArrayContainsPredicate;
pub const RelationalRowsExpressionCaseBranch = schema_mod.RelationalRowsExpressionCaseBranch;
pub const RelationalRowsExpression = schema_mod.RelationalRowsExpression;
pub const RelationalRowsExpressionProjection = schema_mod.RelationalRowsExpressionProjection;
pub const RelationalRowsExpressionAssignment = schema_mod.RelationalRowsExpressionAssignment;

pub const RelationalRowsTextPatternPredicate = struct {
    field: []const u8,
    pattern: []const u8,
    case_insensitive: bool = false,
    negated: bool = false,
};

pub const RelationalRowsDocKeyRange = struct {
    start: []const u8 = "",
    end: []const u8 = "",
};

pub const RelationalRowsCollectedRow = struct {
    key: []const u8,
    json: []const u8,
    version: u64,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(@constCast(self.key));
        alloc.free(@constCast(self.json));
        self.* = undefined;
    }
};

pub fn freeRelationalRowsCollectedRows(alloc: Allocator, rows: []const RelationalRowsCollectedRow) void {
    for (rows) |row_value| {
        var row = row_value;
        row.deinit(alloc);
    }
    if (rows.len > 0) alloc.free(rows);
}

pub const RelationalRowsQueryRequest = struct {
    source_cte: []const u8 = "",
    predicates: []const schema_mod.RelationalCheck = &.{},
    array_any: []const RelationalRowsArrayAnyPredicate = &.{},
    array_contains: []const RelationalRowsArrayContainsPredicate = &.{},
    array_eq: []const RelationalRowsArrayEqPredicate = &.{},
    in_predicates: []const RelationalRowsInPredicate = &.{},
    json_contains: []const RelationalRowsJsonContainsPredicate = &.{},
    json_path_eq: []const RelationalRowsJsonPathEqPredicate = &.{},
    json_path_exists: []const RelationalRowsJsonPathExistsPredicate = &.{},
    text_patterns: []const RelationalRowsTextPatternPredicate = &.{},
    or_predicates: []const RelationalRowsPredicateGroup = &.{},
    not_predicates: []const RelationalRowsPredicateGroup = &.{},
    access_or_predicates: []const RelationalRowsAccessPredicateGroup = &.{},
    access_not_predicates: []const RelationalRowsAccessPredicateGroup = &.{},
    expression_predicates: []const RelationalRowsExpressionCondition = &.{},
    expression_or_predicates: []const RelationalRowsExpressionPredicateGroup = &.{},
    expression_not_predicates: []const RelationalRowsExpressionPredicateGroup = &.{},
    expression_array_contains: []const RelationalRowsExpressionArrayContainsPredicate = &.{},
    select: []const []const u8 = &.{},
    json_extract: []const RelationalRowsJsonExtractProjection = &.{},
    array_length: []const RelationalRowsArrayLengthProjection = &.{},
    coalesce: []const RelationalRowsCoalesceProjection = &.{},
    field_aliases: []const RelationalRowsFieldAliasProjection = &.{},
    expressions: []const RelationalRowsExpressionProjection = &.{},
    select_all: bool = true,
    distinct_on: []const []const u8 = &.{},
    distinct_on_expressions: []const RelationalRowsExpression = &.{},
    order_by: []const RelationalRowsQueryOrder = &.{},
    row_claim: ?RowClaimRequest = null,
    doc_key_range: ?RelationalRowsDocKeyRange = null,
    limit: ?u32 = null,
    offset: u32 = 0,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        if (self.source_cte.len > 0) alloc.free(self.source_cte);
        for (self.predicates) |predicate| {
            alloc.free(predicate.field);
            if (predicate.value_json) |value_json| alloc.free(value_json);
        }
        if (self.predicates.len > 0) alloc.free(self.predicates);
        for (self.array_any) |predicate| {
            alloc.free(predicate.field);
            alloc.free(predicate.value_json);
        }
        if (self.array_any.len > 0) alloc.free(self.array_any);
        for (self.array_contains) |predicate| {
            alloc.free(predicate.field);
            alloc.free(predicate.value_json);
        }
        if (self.array_contains.len > 0) alloc.free(self.array_contains);
        for (self.array_eq) |predicate| {
            alloc.free(predicate.field);
            alloc.free(predicate.value_json);
        }
        if (self.array_eq.len > 0) alloc.free(self.array_eq);
        for (self.in_predicates) |predicate| {
            alloc.free(predicate.field);
            alloc.free(predicate.values_json);
        }
        if (self.in_predicates.len > 0) alloc.free(self.in_predicates);
        for (self.json_contains) |predicate| {
            alloc.free(predicate.field);
            alloc.free(predicate.value_json);
        }
        if (self.json_contains.len > 0) alloc.free(self.json_contains);
        for (self.json_path_eq) |predicate| {
            alloc.free(predicate.field);
            alloc.free(predicate.path);
            alloc.free(predicate.value_json);
        }
        if (self.json_path_eq.len > 0) alloc.free(self.json_path_eq);
        for (self.json_path_exists) |predicate| {
            alloc.free(predicate.field);
            alloc.free(predicate.path);
        }
        if (self.json_path_exists.len > 0) alloc.free(self.json_path_exists);
        for (self.text_patterns) |predicate| {
            alloc.free(predicate.field);
            alloc.free(predicate.pattern);
        }
        if (self.text_patterns.len > 0) alloc.free(self.text_patterns);
        for (self.or_predicates) |group| {
            for (group.predicates) |predicate| {
                alloc.free(predicate.field);
                if (predicate.value_json) |value_json| alloc.free(value_json);
            }
            if (group.predicates.len > 0) alloc.free(group.predicates);
        }
        if (self.or_predicates.len > 0) alloc.free(self.or_predicates);
        for (self.not_predicates) |group| {
            for (group.predicates) |predicate| {
                alloc.free(predicate.field);
                if (predicate.value_json) |value_json| alloc.free(value_json);
            }
            if (group.predicates.len > 0) alloc.free(group.predicates);
        }
        if (self.not_predicates.len > 0) alloc.free(self.not_predicates);
        for (self.access_or_predicates) |group| freeRelationalRowsAccessPredicateGroup(alloc, group);
        if (self.access_or_predicates.len > 0) alloc.free(self.access_or_predicates);
        for (self.access_not_predicates) |group| freeRelationalRowsAccessPredicateGroup(alloc, group);
        if (self.access_not_predicates.len > 0) alloc.free(self.access_not_predicates);
        for (self.expression_predicates) |condition| freeRelationalRowsExpressionCondition(alloc, condition);
        if (self.expression_predicates.len > 0) alloc.free(self.expression_predicates);
        for (self.expression_or_predicates) |group| {
            for (group.conditions) |condition| freeRelationalRowsExpressionCondition(alloc, condition);
            if (group.conditions.len > 0) alloc.free(group.conditions);
        }
        if (self.expression_or_predicates.len > 0) alloc.free(self.expression_or_predicates);
        for (self.expression_not_predicates) |group| {
            for (group.conditions) |condition| freeRelationalRowsExpressionCondition(alloc, condition);
            if (group.conditions.len > 0) alloc.free(group.conditions);
        }
        if (self.expression_not_predicates.len > 0) alloc.free(self.expression_not_predicates);
        for (self.expression_array_contains) |predicate| {
            freeRelationalRowsExpression(alloc, predicate.expression);
            alloc.free(predicate.value_json);
        }
        if (self.expression_array_contains.len > 0) alloc.free(self.expression_array_contains);
        for (self.select) |field| alloc.free(field);
        if (self.select.len > 0) alloc.free(self.select);
        for (self.distinct_on) |field| alloc.free(field);
        if (self.distinct_on.len > 0) alloc.free(self.distinct_on);
        for (self.json_extract) |projection| {
            alloc.free(projection.output);
            alloc.free(projection.field);
            alloc.free(projection.path);
        }
        if (self.json_extract.len > 0) alloc.free(self.json_extract);
        for (self.array_length) |projection| {
            alloc.free(projection.output);
            alloc.free(projection.field);
        }
        if (self.array_length.len > 0) alloc.free(self.array_length);
        for (self.coalesce) |projection| {
            alloc.free(projection.output);
            for (projection.operands) |operand| {
                switch (operand.kind) {
                    .field => if (operand.field.len > 0) alloc.free(operand.field),
                    .value => if (operand.value_json.len > 0) alloc.free(operand.value_json),
                }
            }
            if (projection.operands.len > 0) alloc.free(projection.operands);
        }
        if (self.coalesce.len > 0) alloc.free(self.coalesce);
        for (self.field_aliases) |projection| {
            alloc.free(projection.output);
            alloc.free(projection.field);
        }
        if (self.field_aliases.len > 0) alloc.free(self.field_aliases);
        for (self.expressions) |projection| {
            alloc.free(projection.output);
            freeRelationalRowsExpression(alloc, projection.expression);
        }
        if (self.expressions.len > 0) alloc.free(self.expressions);
        for (self.distinct_on_expressions) |expression| freeRelationalRowsExpression(alloc, expression);
        if (self.distinct_on_expressions.len > 0) alloc.free(self.distinct_on_expressions);
        for (self.order_by) |order| {
            if (order.field.len > 0) alloc.free(order.field);
            if (order.expression) |expression| freeRelationalRowsExpression(alloc, expression);
        }
        if (self.order_by.len > 0) alloc.free(self.order_by);
        if (self.row_claim) |claim| if (claim.owner_id.len > 0) alloc.free(claim.owner_id);
        if (self.doc_key_range) |range| {
            if (range.start.len > 0) alloc.free(range.start);
            if (range.end.len > 0) alloc.free(range.end);
        }
        self.* = undefined;
    }
};

fn freeRelationalRowsExpression(alloc: Allocator, expression: RelationalRowsExpression) void {
    if (expression.field.len > 0) alloc.free(expression.field);
    if (expression.value_json.len > 0) alloc.free(expression.value_json);
    if (expression.json_path.len > 0) alloc.free(expression.json_path);
    for (expression.operands) |operand| freeRelationalRowsExpression(alloc, operand);
    if (expression.operands.len > 0) alloc.free(expression.operands);
    for (expression.case_branches) |branch| {
        freeRelationalRowsExpressionCondition(alloc, branch.when);
        freeRelationalRowsExpression(alloc, branch.then);
    }
    if (expression.case_branches.len > 0) alloc.free(expression.case_branches);
    for (expression.case_else) |fallback| freeRelationalRowsExpression(alloc, fallback);
    if (expression.case_else.len > 0) alloc.free(expression.case_else);
}

fn freeRelationalRowsExpressionCondition(alloc: Allocator, condition: RelationalRowsExpressionCondition) void {
    freeRelationalRowsExpression(alloc, condition.lhs);
    for (condition.rhs) |rhs| freeRelationalRowsExpression(alloc, rhs);
    if (condition.rhs.len > 0) alloc.free(condition.rhs);
}

fn freeRelationalRowsExpressionAssignments(alloc: Allocator, assignments: []const RelationalRowsExpressionAssignment) void {
    for (assignments) |assignment| {
        alloc.free(assignment.field);
        freeRelationalRowsExpression(alloc, assignment.expression);
    }
    if (assignments.len > 0) alloc.free(assignments);
}

fn freeRelationalRowsExpressionProjections(alloc: Allocator, projections: []const RelationalRowsExpressionProjection) void {
    for (projections) |projection| {
        alloc.free(projection.output);
        freeRelationalRowsExpression(alloc, projection.expression);
    }
    if (projections.len > 0) alloc.free(projections);
}

fn freeRelationalRowsTransformOps(alloc: Allocator, operations: []const TransformOp) void {
    for (operations) |op| {
        alloc.free(op.path);
        if (op.value_json) |value_json| alloc.free(value_json);
    }
    if (operations.len > 0) alloc.free(operations);
}

fn freeRelationalRowsOnConflict(alloc: Allocator, conflict: RelationalRowsOnConflict) void {
    freeRelationalRowsTransformOps(alloc, conflict.operations);
    freeRelationalRowsExpressionAssignments(alloc, conflict.patch_expressions);
    freeRelationalRowsExpressionAssignments(alloc, conflict.increment_expressions);
    for (conflict.json_set_expressions) |assignment| {
        alloc.free(assignment.field);
        for (assignment.path) |part| alloc.free(part);
        if (assignment.path.len > 0) alloc.free(assignment.path);
        freeRelationalRowsExpression(alloc, assignment.expression);
    }
    if (conflict.json_set_expressions.len > 0) alloc.free(conflict.json_set_expressions);
    if (conflict.where_expression) |condition| freeRelationalRowsExpressionCondition(alloc, condition);
    for (conflict.where_expressions) |condition| freeRelationalRowsExpressionCondition(alloc, condition);
    if (conflict.where_expressions.len > 0) alloc.free(conflict.where_expressions);
    for (conflict.where_any) |group| {
        for (group.conditions) |condition| freeRelationalRowsExpressionCondition(alloc, condition);
        if (group.conditions.len > 0) alloc.free(group.conditions);
    }
    if (conflict.where_any.len > 0) alloc.free(conflict.where_any);
    for (conflict.where_not) |group| {
        for (group.conditions) |condition| freeRelationalRowsExpressionCondition(alloc, condition);
        if (group.conditions.len > 0) alloc.free(group.conditions);
    }
    if (conflict.where_not.len > 0) alloc.free(conflict.where_not);
}

pub const RelationalRowsCte = struct {
    name: []const u8,
    query: RelationalRowsQueryRequest = .{},
    table_function: ?RelationalRowsTableFunction = null,
    max_rows: ?u32 = null,
    max_bytes: ?u64 = null,
    spill_after_bytes: ?u64 = null,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.name);
        self.query.deinit(alloc);
        if (self.table_function) |*table_function| table_function.deinit(alloc);
        self.* = undefined;
    }
};

pub const RelationalRowsTableFunctionKind = enum {
    graph_query,
    graph_metric_query,
    graph_metric_rerank_query,
};

pub const RelationalRowsTableFunction = union(RelationalRowsTableFunctionKind) {
    graph_query: RelationalRowsGraphTableFunction,
    graph_metric_query: RelationalRowsGraphMetricTableFunction,
    graph_metric_rerank_query: RelationalRowsGraphMetricRerankTableFunction,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        switch (self.*) {
            .graph_query => |*query| query.deinit(alloc),
            .graph_metric_query => |*query| query.deinit(alloc),
            .graph_metric_rerank_query => |*query| query.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const RelationalRowsGraphTableFunction = struct {
    table_name: []const u8,
    query: NamedGraphQuery,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(@constCast(self.table_name));
        freeNamedGraphQuery(alloc, &self.query);
        self.* = undefined;
    }
};

pub const RelationalRowsGraphMetricTableFunction = struct {
    table_name: []const u8,
    query: NamedGraphMetricQuery,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(@constCast(self.table_name));
        freeNamedGraphMetricQuery(alloc, &self.query);
        self.* = undefined;
    }
};

pub const RelationalRowsGraphMetricRerankTableFunction = struct {
    table_name: []const u8,
    request: SearchRequest,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(@constCast(self.table_name));
        if (self.request.primary_text_index_name) |index_name| alloc.free(@constCast(index_name));
        if (self.request.full_text) |*full_text| full_text.deinit(alloc);
        if (self.request.graph_metric_rerank) |rerank| {
            alloc.free(@constCast(rerank.index_name));
            alloc.free(@constCast(rerank.metric_name));
        }
        if (self.request.filter_query_json.len > 0) alloc.free(@constCast(self.request.filter_query_json));
        if (self.request.exclusion_query_json.len > 0) alloc.free(@constCast(self.request.exclusion_query_json));
        self.* = undefined;
    }
};

pub const relational_rows_graph_table_function_fields = [_][]const u8{
    "id",
    "score",
    "graph_name",
    "node_key",
    "depth",
    "distance",
    "path_json",
    "match_json",
    "stored_json",
};

fn freeNamedGraphQuery(alloc: Allocator, named: *NamedGraphQuery) void {
    alloc.free(@constCast(named.name));
    freeGraphQuery(alloc, &named.query);
    named.* = undefined;
}

fn freeNamedGraphMetricQuery(alloc: Allocator, named: *NamedGraphMetricQuery) void {
    alloc.free(@constCast(named.name));
    alloc.free(@constCast(named.query.index_name));
    alloc.free(@constCast(named.query.metric_name));
    named.* = undefined;
}

fn freeGraphQuery(alloc: Allocator, query: *graph_query_mod.GraphQuery) void {
    alloc.free(@constCast(query.index_name));
    freeGraphNodeSelector(alloc, query.start_nodes);
    if (query.target_nodes) |target| freeGraphNodeSelector(alloc, target);
    freeGraphQueryParams(alloc, query.params);
    for (query.pattern) |step| {
        alloc.free(@constCast(step.alias));
        for (step.edge.types) |edge_type| alloc.free(@constCast(edge_type));
        if (step.edge.types.len > 0) alloc.free(@constCast(step.edge.types));
        if (step.node_filter.filter_prefix.len > 0) alloc.free(@constCast(step.node_filter.filter_prefix));
        if (step.node_filter.filter_query_json) |filter| alloc.free(@constCast(filter));
    }
    if (query.pattern.len > 0) alloc.free(@constCast(query.pattern));
    for (query.return_aliases) |alias| alloc.free(@constCast(alias));
    if (query.return_aliases.len > 0) alloc.free(@constCast(query.return_aliases));
    for (query.fields) |field| alloc.free(@constCast(field));
    if (query.fields.len > 0) alloc.free(@constCast(query.fields));
    for (query.metrics) |metric| alloc.free(@constCast(metric.name));
    if (query.metrics.len > 0) alloc.free(@constCast(query.metrics));
    for (query.order_by) |order| alloc.free(@constCast(order.name));
    if (query.order_by.len > 0) alloc.free(@constCast(query.order_by));
    for (query.where_metric) |filter| alloc.free(@constCast(filter.name));
    if (query.where_metric.len > 0) alloc.free(@constCast(query.where_metric));
    query.* = undefined;
}

fn freeGraphQueryParams(alloc: Allocator, params: graph_query_mod.QueryParams) void {
    for (params.edge_types) |edge_type| alloc.free(@constCast(edge_type));
    if (params.edge_types.len > 0) alloc.free(@constCast(params.edge_types));
}

fn freeGraphNodeSelector(alloc: Allocator, selector: graph_query_mod.NodeSelector) void {
    switch (selector) {
        .keys => |keys| {
            for (keys) |key| alloc.free(@constCast(key));
            if (keys.len > 0) alloc.free(@constCast(keys));
        },
        .result_ref => |ref| alloc.free(@constCast(ref.ref)),
    }
}

pub const default_relational_rows_cte_max_rows: u32 = 65_536;
pub const default_relational_rows_cte_max_bytes: u64 = 64 * 1024 * 1024;
pub const default_relational_rows_cte_spill_after_bytes: u64 = default_relational_rows_cte_max_bytes;

pub const RelationalRowsCteMaterializationDecision = enum {
    memory,
    spill,
    reject,
};

pub const RelationalRowsCteMaterializationLimits = struct {
    max_rows: u32,
    max_bytes: u64,
    spill_after_bytes: u64,
};

pub fn relationalRowsCteMaterializationLimits(cte: RelationalRowsCte) RelationalRowsCteMaterializationLimits {
    const max_bytes = cte.max_bytes orelse default_relational_rows_cte_max_bytes;
    const configured_spill_after = cte.spill_after_bytes orelse default_relational_rows_cte_spill_after_bytes;
    return .{
        .max_rows = cte.max_rows orelse default_relational_rows_cte_max_rows,
        .max_bytes = max_bytes,
        .spill_after_bytes = @min(configured_spill_after, max_bytes),
    };
}

pub fn relationalRowsCteMaterializedJsonBytes(rows: []const []const u8) ?u64 {
    var materialized_bytes: u64 = 2; // JSON array brackets around the materialized stream.
    for (rows, 0..) |row, row_index| {
        if (row_index > 0) {
            materialized_bytes = std.math.add(u64, materialized_bytes, 1) catch return null;
        }
        materialized_bytes = std.math.add(u64, materialized_bytes, @intCast(row.len)) catch return null;
    }
    return materialized_bytes;
}

pub fn relationalRowsCteMaterializationDecision(
    cte: RelationalRowsCte,
    observed_rows: usize,
    observed_bytes: u64,
) RelationalRowsCteMaterializationDecision {
    const limits = relationalRowsCteMaterializationLimits(cte);
    if (observed_rows > limits.max_rows) return .reject;
    if (observed_bytes > limits.max_bytes) return .reject;
    if (observed_bytes > limits.spill_after_bytes) return .spill;
    return .memory;
}

test "relational CTE materialization admission distinguishes memory spill and reject" {
    const rows = [_][]const u8{
        "{\"id\":\"1\"}",
        "{\"id\":\"2\"}",
    };
    const observed_bytes = relationalRowsCteMaterializedJsonBytes(&rows) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(u64, 23), observed_bytes);

    const in_memory = RelationalRowsCte{
        .name = "small",
        .max_rows = 2,
        .max_bytes = 23,
        .spill_after_bytes = 23,
    };
    try std.testing.expectEqual(RelationalRowsCteMaterializationDecision.memory, relationalRowsCteMaterializationDecision(in_memory, rows.len, observed_bytes));

    const spill = RelationalRowsCte{
        .name = "spill",
        .max_rows = 2,
        .max_bytes = 23,
        .spill_after_bytes = 22,
    };
    try std.testing.expectEqual(RelationalRowsCteMaterializationDecision.spill, relationalRowsCteMaterializationDecision(spill, rows.len, observed_bytes));

    const too_many_rows = RelationalRowsCte{
        .name = "too_many_rows",
        .max_rows = 1,
        .max_bytes = 23,
    };
    try std.testing.expectEqual(RelationalRowsCteMaterializationDecision.reject, relationalRowsCteMaterializationDecision(too_many_rows, rows.len, observed_bytes));

    const too_many_bytes = RelationalRowsCte{
        .name = "too_many_bytes",
        .max_rows = 2,
        .max_bytes = 22,
    };
    try std.testing.expectEqual(RelationalRowsCteMaterializationDecision.reject, relationalRowsCteMaterializationDecision(too_many_bytes, rows.len, observed_bytes));
}

pub const RelationalRowsQueryPlan = struct {
    ctes: []const RelationalRowsCte = &.{},
    ranges: []const RelationalRowsDocKeyRange = &.{},
    query: RelationalRowsQueryRequest = .{},

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        for (self.ctes) |cte| {
            var owned = cte;
            owned.deinit(alloc);
        }
        if (self.ctes.len > 0) alloc.free(self.ctes);
        freeRelationalRowsDocKeyRanges(alloc, self.ranges);
        self.query.deinit(alloc);
        self.* = undefined;
    }
};

pub const RelationalRowsQueryResult = struct {
    rows: [][]const u8 = &.{},
    total: u32 = 0,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        for (self.rows) |row| alloc.free(@constCast(row));
        if (self.rows.len > 0) alloc.free(self.rows);
        self.* = undefined;
    }
};

pub const RelationalRowsSetOperation = enum {
    union_distinct,
    union_all,
    intersect,
    except,
};

pub const RelationalRowsSetOperationPlan = struct {
    operation: RelationalRowsSetOperation,
    left: RelationalRowsQueryPlan,
    right: RelationalRowsQueryPlan,
    order_by: []const RelationalRowsQueryOrder = &.{},
    limit: ?u32 = null,
    offset: u32 = 0,
    max_rows: ?u32 = null,
    max_bytes: ?u64 = null,
    spill_after_bytes: ?u64 = null,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        self.left.deinit(alloc);
        self.right.deinit(alloc);
        var order_query: RelationalRowsQueryRequest = .{ .order_by = self.order_by };
        order_query.deinit(alloc);
        self.* = undefined;
    }
};

pub const RelationalRowsMutationKind = enum {
    update,
    delete,
};

pub const RelationalRowsJsonSetExpressionAssignment = struct {
    field: []const u8,
    path: []const []const u8,
    expression: RelationalRowsExpression,
};

pub const RelationalRowsTemporalPortion = struct {
    period: []const u8,
    from_json: []const u8,
    to_json: []const u8,
};

pub const RelationalRowsMutationSourceRequest = struct {
    kind: RelationalRowsMutationKind,
    source: RelationalRowsQueryRequest = .{},
    rewrite_identity: bool = false,
    restart_identity: bool = false,
    operations: []const TransformOp = &.{},
    patch_expressions: []const RelationalRowsExpressionAssignment = &.{},
    increment_expressions: []const RelationalRowsExpressionAssignment = &.{},
    json_set_expressions: []const RelationalRowsJsonSetExpressionAssignment = &.{},
    temporal_portion: ?RelationalRowsTemporalPortion = null,
    returning: []const []const u8 = &.{},
    returning_expressions: []const RelationalRowsExpressionProjection = &.{},
    returning_all: bool = false,
};

pub const RelationalRowsConflictAction = enum {
    update,
    nothing,
};

pub const RelationalRowsConflictTargetKind = enum {
    primary,
    unique,
};

pub const RelationalRowsConflictTarget = struct {
    kind: RelationalRowsConflictTargetKind = .primary,
    unique_name: []const u8 = "",
    unique_predicates: []const schema_mod.RelationalCheck = &.{},
    unique_predicate_expressions: []const RelationalRowsExpressionCondition = &.{},
};

pub const RelationalRowsOnConflict = struct {
    target: RelationalRowsConflictTarget = .{},
    action: RelationalRowsConflictAction = .nothing,
    operations: []const TransformOp = &.{},
    patch_expressions: []const RelationalRowsExpressionAssignment = &.{},
    increment_expressions: []const RelationalRowsExpressionAssignment = &.{},
    json_set_expressions: []const RelationalRowsJsonSetExpressionAssignment = &.{},
    where_expression: ?RelationalRowsExpressionCondition = null,
    where_expressions: []const RelationalRowsExpressionCondition = &.{},
    where_any: []const RelationalRowsExpressionPredicateGroup = &.{},
    where_not: []const RelationalRowsExpressionPredicateGroup = &.{},
};

pub const RelationalRowsInsertSourceRequest = struct {
    source_table: []const u8 = "",
    source: RelationalRowsQueryRequest = .{},
    assignments: []const RelationalRowsExpressionAssignment = &.{},
    on_conflict: ?RelationalRowsOnConflict = null,
    returning: []const []const u8 = &.{},
    returning_expressions: []const RelationalRowsExpressionProjection = &.{},
    returning_all: bool = false,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        if (self.source_table.len > 0) alloc.free(self.source_table);
        self.source.deinit(alloc);
        freeRelationalRowsExpressionAssignments(alloc, self.assignments);
        if (self.on_conflict) |conflict| freeRelationalRowsOnConflict(alloc, conflict);
        for (self.returning) |field| alloc.free(field);
        if (self.returning.len > 0) alloc.free(self.returning);
        freeRelationalRowsExpressionProjections(alloc, self.returning_expressions);
        self.* = undefined;
    }
};

pub const RelationalRowsInsertSourcePlan = struct {
    ctes: []const RelationalRowsCte = &.{},
    ranges: []const RelationalRowsDocKeyRange = &.{},
    insert_source: RelationalRowsInsertSourceRequest = .{},
    sync_level: SyncLevel = .write,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        for (self.ctes) |cte| {
            var owned = cte;
            owned.deinit(alloc);
        }
        if (self.ctes.len > 0) alloc.free(self.ctes);
        freeRelationalRowsDocKeyRanges(alloc, self.ranges);
        self.insert_source.deinit(alloc);
        self.* = undefined;
    }
};

pub const RelationalRowsMutationSourceResult = struct {
    matched: u32 = 0,
    staged: u32 = 0,
    returning_rows: [][]const u8 = &.{},
    participant_predicates: []TransactionVersionPredicate = &.{},
    participant_preimages: []TransactionWrite = &.{},
    participant_writes: []TransactionWrite = &.{},
    participant_deletes: [][]const u8 = &.{},
    participant_transforms: []DocumentTransform = &.{},

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        for (self.returning_rows) |row| alloc.free(@constCast(row));
        if (self.returning_rows.len > 0) alloc.free(self.returning_rows);
        for (self.participant_predicates) |predicate| alloc.free(@constCast(predicate.key));
        if (self.participant_predicates.len > 0) alloc.free(self.participant_predicates);
        for (self.participant_preimages) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        if (self.participant_preimages.len > 0) alloc.free(self.participant_preimages);
        for (self.participant_writes) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        if (self.participant_writes.len > 0) alloc.free(self.participant_writes);
        for (self.participant_deletes) |key| alloc.free(key);
        if (self.participant_deletes.len > 0) alloc.free(self.participant_deletes);
        for (self.participant_transforms) |transform| {
            alloc.free(@constCast(transform.key));
            for (transform.operations) |op| {
                alloc.free(@constCast(op.path));
                if (op.value_json) |value_json| alloc.free(@constCast(value_json));
            }
            if (transform.operations.len > 0) alloc.free(transform.operations);
        }
        if (self.participant_transforms.len > 0) alloc.free(self.participant_transforms);
        self.* = undefined;
    }
};

pub const RelationalRowsWindowFunction = enum {
    row_number,
    rank,
    dense_rank,
    percent_rank,
    cume_dist,
    ntile,
    lag,
    lead,
    first_value,
    last_value,
    nth_value,
    count,
    sum,
    avg,
    min,
    max,
    bool_or,
    bool_and,
};

pub const RelationalRowsWindowFrameUnit = enum {
    rows,
    range,
};

pub const RelationalRowsWindowFrameBound = enum {
    unbounded_preceding,
    offset_preceding,
    current_row,
    offset_following,
    unbounded_following,
};

pub const RelationalRowsWindowFrame = struct {
    unit: RelationalRowsWindowFrameUnit = .range,
    start: RelationalRowsWindowFrameBound = .unbounded_preceding,
    start_offset: u32 = 0,
    end: RelationalRowsWindowFrameBound = .current_row,
    end_offset: u32 = 0,
};

pub const RelationalRowsWindowSpec = struct {
    output: []const u8,
    function: RelationalRowsWindowFunction,
    partition_by: []const []const u8 = &.{},
    order_by: []const RelationalRowsQueryOrder = &.{},
    value_expression: ?RelationalRowsExpression = null,
    offset: u32 = 1,
    default_json: []const u8 = "",
    frame: ?RelationalRowsWindowFrame = null,
    filter_predicates: []const schema_mod.RelationalCheck = &.{},
    filter_array_any: []const RelationalRowsArrayAnyPredicate = &.{},
    filter_array_contains: []const RelationalRowsArrayContainsPredicate = &.{},
    filter_array_eq: []const RelationalRowsArrayEqPredicate = &.{},
    filter_in_predicates: []const RelationalRowsInPredicate = &.{},
    filter_json_contains: []const RelationalRowsJsonContainsPredicate = &.{},
    filter_json_path_eq: []const RelationalRowsJsonPathEqPredicate = &.{},
    filter_json_path_exists: []const RelationalRowsJsonPathExistsPredicate = &.{},
    filter_text_patterns: []const RelationalRowsTextPatternPredicate = &.{},
    filter_expressions: []const RelationalRowsExpressionCondition = &.{},
    filter_expression_array_contains: []const RelationalRowsExpressionArrayContainsPredicate = &.{},
    filter_any: []const RelationalRowsExpressionPredicateGroup = &.{},
    filter_not: []const RelationalRowsExpressionPredicateGroup = &.{},
};

pub const RelationalRowsWindowRequest = struct {
    source: RelationalRowsQueryRequest = .{},
    windows: []const RelationalRowsWindowSpec = &.{},
    select: []const []const u8 = &.{},
    select_all: bool = false,
    order_by: []const RelationalRowsQueryOrder = &.{},
    limit: ?u32 = null,
    offset: u32 = 0,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        self.source.deinit(alloc);
        for (self.windows) |window| {
            alloc.free(window.output);
            for (window.partition_by) |field| alloc.free(field);
            if (window.partition_by.len > 0) alloc.free(window.partition_by);
            for (window.order_by) |order| {
                if (order.field.len > 0) alloc.free(order.field);
                if (order.expression) |expression| freeRelationalRowsExpression(alloc, expression);
            }
            if (window.order_by.len > 0) alloc.free(window.order_by);
            if (window.value_expression) |expression| freeRelationalRowsExpression(alloc, expression);
            if (window.default_json.len > 0) alloc.free(window.default_json);
            for (window.filter_predicates) |predicate| {
                alloc.free(predicate.field);
                if (predicate.value_json) |json| alloc.free(json);
            }
            if (window.filter_predicates.len > 0) alloc.free(window.filter_predicates);
            for (window.filter_array_any) |predicate| {
                alloc.free(predicate.field);
                alloc.free(predicate.value_json);
            }
            if (window.filter_array_any.len > 0) alloc.free(window.filter_array_any);
            for (window.filter_array_contains) |predicate| {
                alloc.free(predicate.field);
                alloc.free(predicate.value_json);
            }
            if (window.filter_array_contains.len > 0) alloc.free(window.filter_array_contains);
            for (window.filter_array_eq) |predicate| {
                alloc.free(predicate.field);
                alloc.free(predicate.value_json);
            }
            if (window.filter_array_eq.len > 0) alloc.free(window.filter_array_eq);
            for (window.filter_in_predicates) |predicate| {
                alloc.free(predicate.field);
                if (predicate.values_json.len > 0) alloc.free(predicate.values_json);
            }
            if (window.filter_in_predicates.len > 0) alloc.free(window.filter_in_predicates);
            for (window.filter_json_contains) |predicate| {
                alloc.free(predicate.field);
                alloc.free(predicate.value_json);
            }
            if (window.filter_json_contains.len > 0) alloc.free(window.filter_json_contains);
            for (window.filter_json_path_eq) |predicate| {
                alloc.free(predicate.field);
                alloc.free(predicate.path);
                alloc.free(predicate.value_json);
            }
            if (window.filter_json_path_eq.len > 0) alloc.free(window.filter_json_path_eq);
            for (window.filter_json_path_exists) |predicate| {
                alloc.free(predicate.field);
                alloc.free(predicate.path);
            }
            if (window.filter_json_path_exists.len > 0) alloc.free(window.filter_json_path_exists);
            for (window.filter_text_patterns) |predicate| {
                alloc.free(predicate.field);
                alloc.free(predicate.pattern);
            }
            if (window.filter_text_patterns.len > 0) alloc.free(window.filter_text_patterns);
            for (window.filter_expressions) |condition| freeRelationalRowsExpressionCondition(alloc, condition);
            if (window.filter_expressions.len > 0) alloc.free(window.filter_expressions);
            for (window.filter_expression_array_contains) |predicate| {
                freeRelationalRowsExpression(alloc, predicate.expression);
                alloc.free(predicate.value_json);
            }
            if (window.filter_expression_array_contains.len > 0) alloc.free(window.filter_expression_array_contains);
            for (window.filter_any) |group| {
                for (group.conditions) |condition| freeRelationalRowsExpressionCondition(alloc, condition);
                if (group.conditions.len > 0) alloc.free(group.conditions);
            }
            if (window.filter_any.len > 0) alloc.free(window.filter_any);
            for (window.filter_not) |group| {
                for (group.conditions) |condition| freeRelationalRowsExpressionCondition(alloc, condition);
                if (group.conditions.len > 0) alloc.free(group.conditions);
            }
            if (window.filter_not.len > 0) alloc.free(window.filter_not);
        }
        if (self.windows.len > 0) alloc.free(self.windows);
        for (self.select) |field| alloc.free(field);
        if (self.select.len > 0) alloc.free(self.select);
        for (self.order_by) |order| {
            if (order.field.len > 0) alloc.free(order.field);
            if (order.expression) |expression| freeRelationalRowsExpression(alloc, expression);
        }
        if (self.order_by.len > 0) alloc.free(self.order_by);
        self.* = undefined;
    }
};

pub const RelationalRowsWindowPlan = struct {
    ctes: []const RelationalRowsCte = &.{},
    ranges: []const RelationalRowsDocKeyRange = &.{},
    window: RelationalRowsWindowRequest = .{},

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        for (self.ctes) |cte| {
            var owned = cte;
            owned.deinit(alloc);
        }
        if (self.ctes.len > 0) alloc.free(self.ctes);
        freeRelationalRowsDocKeyRanges(alloc, self.ranges);
        self.window.deinit(alloc);
        self.* = undefined;
    }
};

pub const RelationalRowsWindowResult = struct {
    rows: [][]const u8 = &.{},
    total_rows: u32 = 0,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        for (self.rows) |row| alloc.free(@constCast(row));
        if (self.rows.len > 0) alloc.free(self.rows);
        self.* = undefined;
    }
};

pub const RelationalRowsAggregateOp = enum {
    count,
    sum,
    min,
    max,
    avg,
    percentile_cont,
    percentile_disc,
    mode,
    array_agg,
    string_agg,
    bool_or,
    bool_and,
};

pub const default_relational_rows_aggregate_distinct_max_items: u32 = 65536;
pub const default_relational_rows_percentile_max_items: u32 = 65536;
pub const default_relational_rows_array_agg_max_items: u32 = 1024;

pub const RelationalRowsAggregateSpec = struct {
    name: []const u8,
    op: RelationalRowsAggregateOp,
    field: ?[]const u8 = null,
    expression: ?RelationalRowsExpression = null,
    distinct: bool = false,
    distinct_max_items: u32 = default_relational_rows_aggregate_distinct_max_items,
    percentile: ?f64 = null,
    percentiles: []const f64 = &.{},
    percentile_max_items: u32 = 0,
    percentile_order: RelationalRowsQueryOrderDirection = .asc,
    array_max_items: u32 = 0,
    array_order_by: []const RelationalRowsQueryOrder = &.{},
    string_delimiter: ?[]const u8 = null,
    filter_predicates: []const schema_mod.RelationalCheck = &.{},
    filter_array_any: []const RelationalRowsArrayAnyPredicate = &.{},
    filter_array_contains: []const RelationalRowsArrayContainsPredicate = &.{},
    filter_array_eq: []const RelationalRowsArrayEqPredicate = &.{},
    filter_in_predicates: []const RelationalRowsInPredicate = &.{},
    filter_json_contains: []const RelationalRowsJsonContainsPredicate = &.{},
    filter_json_path_eq: []const RelationalRowsJsonPathEqPredicate = &.{},
    filter_json_path_exists: []const RelationalRowsJsonPathExistsPredicate = &.{},
    filter_text_patterns: []const RelationalRowsTextPatternPredicate = &.{},
    filter_expressions: []const RelationalRowsExpressionCondition = &.{},
    filter_expression_array_contains: []const RelationalRowsExpressionArrayContainsPredicate = &.{},
    filter_any: []const RelationalRowsExpressionPredicateGroup = &.{},
    filter_not: []const RelationalRowsExpressionPredicateGroup = &.{},
};

pub const RelationalRowsAggregateRequest = struct {
    source: RelationalRowsQueryRequest = .{},
    group_by: []const []const u8 = &.{},
    group_expressions: []const RelationalRowsExpressionProjection = &.{},
    aggregations: []const RelationalRowsAggregateSpec = &.{},
    having_predicates: []const schema_mod.RelationalCheck = &.{},
    having_expressions: []const RelationalRowsExpressionCondition = &.{},
    having_any: []const RelationalRowsExpressionPredicateGroup = &.{},
    having_not: []const RelationalRowsExpressionPredicateGroup = &.{},
    order_by: []const RelationalRowsQueryOrder = &.{},
    limit: ?u32 = null,
    offset: u32 = 0,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        self.source.deinit(alloc);
        for (self.group_by) |field| alloc.free(field);
        if (self.group_by.len > 0) alloc.free(self.group_by);
        for (self.group_expressions) |projection| {
            alloc.free(projection.output);
            freeRelationalRowsExpression(alloc, projection.expression);
        }
        if (self.group_expressions.len > 0) alloc.free(self.group_expressions);
        for (self.aggregations) |aggregation| {
            alloc.free(aggregation.name);
            if (aggregation.field) |field| alloc.free(field);
            if (aggregation.percentiles.len > 0) alloc.free(aggregation.percentiles);
            if (aggregation.string_delimiter) |delimiter| alloc.free(delimiter);
            if (aggregation.expression) |expression| freeRelationalRowsExpression(alloc, expression);
            for (aggregation.array_order_by) |order| {
                if (order.field.len > 0) alloc.free(order.field);
                if (order.expression) |expression| freeRelationalRowsExpression(alloc, expression);
            }
            if (aggregation.array_order_by.len > 0) alloc.free(aggregation.array_order_by);
            for (aggregation.filter_predicates) |predicate| {
                alloc.free(predicate.field);
                if (predicate.value_json) |value_json| alloc.free(value_json);
            }
            if (aggregation.filter_predicates.len > 0) alloc.free(aggregation.filter_predicates);
            for (aggregation.filter_array_any) |predicate| {
                alloc.free(predicate.field);
                alloc.free(predicate.value_json);
            }
            if (aggregation.filter_array_any.len > 0) alloc.free(aggregation.filter_array_any);
            for (aggregation.filter_array_contains) |predicate| {
                alloc.free(predicate.field);
                alloc.free(predicate.value_json);
            }
            if (aggregation.filter_array_contains.len > 0) alloc.free(aggregation.filter_array_contains);
            for (aggregation.filter_array_eq) |predicate| {
                alloc.free(predicate.field);
                alloc.free(predicate.value_json);
            }
            if (aggregation.filter_array_eq.len > 0) alloc.free(aggregation.filter_array_eq);
            for (aggregation.filter_in_predicates) |predicate| {
                alloc.free(predicate.field);
                alloc.free(predicate.values_json);
            }
            if (aggregation.filter_in_predicates.len > 0) alloc.free(aggregation.filter_in_predicates);
            for (aggregation.filter_json_contains) |predicate| {
                alloc.free(predicate.field);
                alloc.free(predicate.value_json);
            }
            if (aggregation.filter_json_contains.len > 0) alloc.free(aggregation.filter_json_contains);
            for (aggregation.filter_json_path_eq) |predicate| {
                alloc.free(predicate.field);
                alloc.free(predicate.path);
                alloc.free(predicate.value_json);
            }
            if (aggregation.filter_json_path_eq.len > 0) alloc.free(aggregation.filter_json_path_eq);
            for (aggregation.filter_json_path_exists) |predicate| {
                alloc.free(predicate.field);
                alloc.free(predicate.path);
            }
            if (aggregation.filter_json_path_exists.len > 0) alloc.free(aggregation.filter_json_path_exists);
            for (aggregation.filter_text_patterns) |predicate| {
                alloc.free(predicate.field);
                alloc.free(predicate.pattern);
            }
            if (aggregation.filter_text_patterns.len > 0) alloc.free(aggregation.filter_text_patterns);
            for (aggregation.filter_expressions) |condition| freeRelationalRowsExpressionCondition(alloc, condition);
            if (aggregation.filter_expressions.len > 0) alloc.free(aggregation.filter_expressions);
            for (aggregation.filter_expression_array_contains) |predicate| {
                freeRelationalRowsExpression(alloc, predicate.expression);
                alloc.free(predicate.value_json);
            }
            if (aggregation.filter_expression_array_contains.len > 0) alloc.free(aggregation.filter_expression_array_contains);
            for (aggregation.filter_any) |group| {
                for (group.conditions) |condition| freeRelationalRowsExpressionCondition(alloc, condition);
                if (group.conditions.len > 0) alloc.free(group.conditions);
            }
            if (aggregation.filter_any.len > 0) alloc.free(aggregation.filter_any);
            for (aggregation.filter_not) |group| {
                for (group.conditions) |condition| freeRelationalRowsExpressionCondition(alloc, condition);
                if (group.conditions.len > 0) alloc.free(group.conditions);
            }
            if (aggregation.filter_not.len > 0) alloc.free(aggregation.filter_not);
        }
        if (self.aggregations.len > 0) alloc.free(self.aggregations);
        for (self.having_predicates) |predicate| {
            alloc.free(predicate.field);
            if (predicate.value_json) |value_json| alloc.free(value_json);
        }
        if (self.having_predicates.len > 0) alloc.free(self.having_predicates);
        for (self.having_expressions) |condition| freeRelationalRowsExpressionCondition(alloc, condition);
        if (self.having_expressions.len > 0) alloc.free(self.having_expressions);
        for (self.having_any) |group| {
            for (group.conditions) |condition| freeRelationalRowsExpressionCondition(alloc, condition);
            if (group.conditions.len > 0) alloc.free(group.conditions);
        }
        if (self.having_any.len > 0) alloc.free(self.having_any);
        for (self.having_not) |group| {
            for (group.conditions) |condition| freeRelationalRowsExpressionCondition(alloc, condition);
            if (group.conditions.len > 0) alloc.free(group.conditions);
        }
        if (self.having_not.len > 0) alloc.free(self.having_not);
        for (self.order_by) |order| {
            if (order.field.len > 0) alloc.free(order.field);
            if (order.expression) |expression| freeRelationalRowsExpression(alloc, expression);
        }
        if (self.order_by.len > 0) alloc.free(self.order_by);
        self.* = undefined;
    }
};

pub const RelationalRowsAggregatePlan = struct {
    ctes: []const RelationalRowsCte = &.{},
    ranges: []const RelationalRowsDocKeyRange = &.{},
    aggregate: RelationalRowsAggregateRequest = .{},

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        for (self.ctes) |cte| {
            var owned = cte;
            owned.deinit(alloc);
        }
        if (self.ctes.len > 0) alloc.free(self.ctes);
        freeRelationalRowsDocKeyRanges(alloc, self.ranges);
        self.aggregate.deinit(alloc);
        self.* = undefined;
    }
};

pub const RelationalRowsAggregateResult = struct {
    rows: [][]const u8 = &.{},
    total_groups: u32 = 0,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        for (self.rows) |row| alloc.free(@constCast(row));
        if (self.rows.len > 0) alloc.free(self.rows);
        self.* = undefined;
    }
};

pub const RelationalRowsJoinType = enum {
    inner,
    left,
};

pub const RelationalRowsJoinStrategy = enum {
    auto,
    lookup,
    hash,
    merge,
};

pub const default_relational_rows_lookup_join_max_build_rows: usize = 256;

pub const RelationalRowsJoinStrategySelection = struct {
    requested: RelationalRowsJoinStrategy,
    selected: RelationalRowsJoinStrategy,
};

pub const RelationalRowsJoinOn = struct {
    left_field: []const u8,
    right_field: []const u8,
};

pub const RelationalRowsJoinProjectionSide = enum {
    left,
    right,
};

pub const RelationalRowsJoinedMutationFieldAssignment = struct {
    field: []const u8,
    source_side: RelationalRowsJoinProjectionSide,
    source_field: []const u8,
};

pub const RelationalRowsJoinProjection = struct {
    output: []const u8,
    side: RelationalRowsJoinProjectionSide,
    field: []const u8,
};

pub const RelationalRowsJoinRequest = struct {
    left: RelationalRowsQueryRequest = .{},
    right: RelationalRowsQueryRequest = .{},
    on: []const RelationalRowsJoinOn = &.{},
    on_expression_predicates: []const RelationalRowsExpressionCondition = &.{},
    on_expression_or_predicates: []const RelationalRowsExpressionPredicateGroup = &.{},
    on_expression_not_predicates: []const RelationalRowsExpressionPredicateGroup = &.{},
    on_expression_array_contains: []const RelationalRowsExpressionArrayContainsPredicate = &.{},
    match_expression_predicates: []const RelationalRowsExpressionCondition = &.{},
    match_expression_or_predicates: []const RelationalRowsExpressionPredicateGroup = &.{},
    match_expression_not_predicates: []const RelationalRowsExpressionPredicateGroup = &.{},
    match_expression_array_contains: []const RelationalRowsExpressionArrayContainsPredicate = &.{},
    join_type: RelationalRowsJoinType = .inner,
    strategy: RelationalRowsJoinStrategy = .auto,
    select: []const RelationalRowsJoinProjection = &.{},
    order_by: []const RelationalRowsQueryOrder = &.{},
    limit: ?u32 = null,
    offset: u32 = 0,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        self.left.deinit(alloc);
        self.right.deinit(alloc);
        for (self.on) |join_on| {
            alloc.free(join_on.left_field);
            alloc.free(join_on.right_field);
        }
        if (self.on.len > 0) alloc.free(self.on);
        for (self.on_expression_predicates) |condition| freeRelationalRowsExpressionCondition(alloc, condition);
        if (self.on_expression_predicates.len > 0) alloc.free(self.on_expression_predicates);
        for (self.on_expression_or_predicates) |group| {
            for (group.conditions) |condition| freeRelationalRowsExpressionCondition(alloc, condition);
            if (group.conditions.len > 0) alloc.free(group.conditions);
        }
        if (self.on_expression_or_predicates.len > 0) alloc.free(self.on_expression_or_predicates);
        for (self.on_expression_not_predicates) |group| {
            for (group.conditions) |condition| freeRelationalRowsExpressionCondition(alloc, condition);
            if (group.conditions.len > 0) alloc.free(group.conditions);
        }
        if (self.on_expression_not_predicates.len > 0) alloc.free(self.on_expression_not_predicates);
        for (self.on_expression_array_contains) |predicate| {
            freeRelationalRowsExpression(alloc, predicate.expression);
            alloc.free(predicate.value_json);
        }
        if (self.on_expression_array_contains.len > 0) alloc.free(self.on_expression_array_contains);
        for (self.match_expression_predicates) |condition| freeRelationalRowsExpressionCondition(alloc, condition);
        if (self.match_expression_predicates.len > 0) alloc.free(self.match_expression_predicates);
        for (self.match_expression_or_predicates) |group| {
            for (group.conditions) |condition| freeRelationalRowsExpressionCondition(alloc, condition);
            if (group.conditions.len > 0) alloc.free(group.conditions);
        }
        if (self.match_expression_or_predicates.len > 0) alloc.free(self.match_expression_or_predicates);
        for (self.match_expression_not_predicates) |group| {
            for (group.conditions) |condition| freeRelationalRowsExpressionCondition(alloc, condition);
            if (group.conditions.len > 0) alloc.free(group.conditions);
        }
        if (self.match_expression_not_predicates.len > 0) alloc.free(self.match_expression_not_predicates);
        for (self.match_expression_array_contains) |predicate| {
            freeRelationalRowsExpression(alloc, predicate.expression);
            alloc.free(predicate.value_json);
        }
        if (self.match_expression_array_contains.len > 0) alloc.free(self.match_expression_array_contains);
        for (self.select) |projection| {
            alloc.free(projection.output);
            alloc.free(projection.field);
        }
        if (self.select.len > 0) alloc.free(self.select);
        for (self.order_by) |order| {
            if (order.field.len > 0) alloc.free(order.field);
            if (order.expression) |expression| freeRelationalRowsExpression(alloc, expression);
        }
        if (self.order_by.len > 0) alloc.free(self.order_by);
        self.* = undefined;
    }
};

pub fn relationalRowsSelectJoinStrategy(
    req: RelationalRowsJoinRequest,
    left_rows: usize,
    right_rows: usize,
    inputs_sorted_on_join_keys: bool,
) ?RelationalRowsJoinStrategySelection {
    if (req.on.len == 0) {
        const selected: RelationalRowsJoinStrategy = switch (req.strategy) {
            .merge => return null,
            .lookup => .lookup,
            .hash, .auto => .hash,
        };
        return .{
            .requested = req.strategy,
            .selected = selected,
        };
    }
    const selected: RelationalRowsJoinStrategy = switch (req.strategy) {
        .lookup => .lookup,
        .hash => .hash,
        .merge => if (inputs_sorted_on_join_keys) .merge else return null,
        .auto => if (inputs_sorted_on_join_keys and left_rows > default_relational_rows_lookup_join_max_build_rows and right_rows > default_relational_rows_lookup_join_max_build_rows)
            .merge
        else if (right_rows <= default_relational_rows_lookup_join_max_build_rows)
            .lookup
        else
            .hash,
    };
    return .{
        .requested = req.strategy,
        .selected = selected,
    };
}

pub fn relationalRowsJoinInputsSortedOnJoinKeys(req: RelationalRowsJoinRequest) bool {
    return relationalRowsJoinSourceOrderCoversJoinKeys(req.left.order_by, req.on, .left) and
        relationalRowsJoinSourceOrderCoversJoinKeys(req.right.order_by, req.on, .right);
}

const RelationalRowsJoinOrderSide = enum {
    left,
    right,
};

fn relationalRowsJoinSourceOrderCoversJoinKeys(
    order_by: []const RelationalRowsQueryOrder,
    predicates: []const RelationalRowsJoinOn,
    side: RelationalRowsJoinOrderSide,
) bool {
    if (predicates.len == 0 or order_by.len < predicates.len) return false;
    for (predicates, 0..) |predicate, i| {
        const order = order_by[i];
        if (order.expression != null or order.null_test != null or order.direction != .asc) return false;
        const field = switch (side) {
            .left => predicate.left_field,
            .right => predicate.right_field,
        };
        if (!std.mem.eql(u8, order.field, field)) return false;
    }
    return true;
}

test "relational rows join strategy selection is explicit and fail closed for unproven merge" {
    const join_on = [_]RelationalRowsJoinOn{.{
        .left_field = "customer_id",
        .right_field = "id",
    }};
    const auto_small = RelationalRowsJoinRequest{ .on = join_on[0..] };
    try std.testing.expectEqual(RelationalRowsJoinStrategy.lookup, relationalRowsSelectJoinStrategy(auto_small, 1000, 16, false).?.selected);

    const auto_large = RelationalRowsJoinRequest{ .on = join_on[0..] };
    try std.testing.expectEqual(RelationalRowsJoinStrategy.hash, relationalRowsSelectJoinStrategy(auto_large, 1000, 1000, false).?.selected);
    try std.testing.expectEqual(RelationalRowsJoinStrategy.merge, relationalRowsSelectJoinStrategy(auto_large, 1000, 1000, true).?.selected);

    const explicit_hash = RelationalRowsJoinRequest{ .on = join_on[0..], .strategy = .hash };
    try std.testing.expectEqual(RelationalRowsJoinStrategy.hash, relationalRowsSelectJoinStrategy(explicit_hash, 1, 1, false).?.selected);

    const explicit_merge = RelationalRowsJoinRequest{ .on = join_on[0..], .strategy = .merge };
    try std.testing.expect(relationalRowsSelectJoinStrategy(explicit_merge, 1, 1, false) == null);
    try std.testing.expectEqual(RelationalRowsJoinStrategy.merge, relationalRowsSelectJoinStrategy(explicit_merge, 1, 1, true).?.selected);
}

test "relational rows join sorted input proof requires leading ascending join key order" {
    const join_on = [_]RelationalRowsJoinOn{.{
        .left_field = "customer_id",
        .right_field = "id",
    }};
    const left_order = [_]RelationalRowsQueryOrder{.{
        .field = "customer_id",
    }};
    const right_order = [_]RelationalRowsQueryOrder{.{
        .field = "id",
    }};
    const sorted = RelationalRowsJoinRequest{
        .left = .{ .order_by = left_order[0..] },
        .right = .{ .order_by = right_order[0..] },
        .on = join_on[0..],
    };
    try std.testing.expect(relationalRowsJoinInputsSortedOnJoinKeys(sorted));

    const wrong_right_order = [_]RelationalRowsQueryOrder{.{
        .field = "other_id",
    }};
    const unsorted = RelationalRowsJoinRequest{
        .left = .{ .order_by = left_order[0..] },
        .right = .{ .order_by = wrong_right_order[0..] },
        .on = join_on[0..],
    };
    try std.testing.expect(!relationalRowsJoinInputsSortedOnJoinKeys(unsorted));

    const desc_left_order = [_]RelationalRowsQueryOrder{.{
        .field = "customer_id",
        .direction = .desc,
    }};
    const descending = RelationalRowsJoinRequest{
        .left = .{ .order_by = desc_left_order[0..] },
        .right = .{ .order_by = right_order[0..] },
        .on = join_on[0..],
    };
    try std.testing.expect(!relationalRowsJoinInputsSortedOnJoinKeys(descending));
}

pub const RelationalRowsJoinedMutationSourceRequest = struct {
    kind: RelationalRowsMutationKind,
    ctes: []const RelationalRowsCte = &.{},
    source_table: []const u8 = "",
    target_side: RelationalRowsJoinProjectionSide = .left,
    join: RelationalRowsJoinRequest = .{},
    match_expression_predicates: []const RelationalRowsExpressionCondition = &.{},
    match_expression_or_predicates: []const RelationalRowsExpressionPredicateGroup = &.{},
    match_expression_not_predicates: []const RelationalRowsExpressionPredicateGroup = &.{},
    match_expression_array_contains: []const RelationalRowsExpressionArrayContainsPredicate = &.{},
    rewrite_identity: bool = false,
    source_assignments: []const RelationalRowsJoinedMutationFieldAssignment = &.{},
    operations: []const TransformOp = &.{},
    patch_expressions: []const RelationalRowsExpressionAssignment = &.{},
    increment_expressions: []const RelationalRowsExpressionAssignment = &.{},
    json_set_expressions: []const RelationalRowsJsonSetExpressionAssignment = &.{},
    returning: []const []const u8 = &.{},
    returning_expressions: []const RelationalRowsExpressionProjection = &.{},
    returning_all: bool = false,
};

pub const RelationalRowsJoinPlan = struct {
    ctes: []const RelationalRowsCte = &.{},
    left_table: []const u8 = "",
    right_table: []const u8 = "",
    left_ranges: []const RelationalRowsDocKeyRange = &.{},
    right_ranges: []const RelationalRowsDocKeyRange = &.{},
    join: RelationalRowsJoinRequest = .{},

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        for (self.ctes) |cte| {
            var owned = cte;
            owned.deinit(alloc);
        }
        if (self.ctes.len > 0) alloc.free(self.ctes);
        if (self.left_table.len > 0) alloc.free(self.left_table);
        if (self.right_table.len > 0) alloc.free(self.right_table);
        freeRelationalRowsDocKeyRanges(alloc, self.left_ranges);
        freeRelationalRowsDocKeyRanges(alloc, self.right_ranges);
        self.join.deinit(alloc);
        self.* = undefined;
    }
};

pub const RelationalRowsLateralCorrelation = struct {
    left_field: []const u8,
    right_field: []const u8,
};

pub const RelationalRowsLateralRequest = struct {
    left: RelationalRowsQueryRequest = .{},
    right: RelationalRowsQueryRequest = .{},
    correlations: []const RelationalRowsLateralCorrelation = &.{},
    match_expression_predicates: []const RelationalRowsExpressionCondition = &.{},
    match_expression_or_predicates: []const RelationalRowsExpressionPredicateGroup = &.{},
    match_expression_not_predicates: []const RelationalRowsExpressionPredicateGroup = &.{},
    match_expression_array_contains: []const RelationalRowsExpressionArrayContainsPredicate = &.{},
    select: []const RelationalRowsJoinProjection = &.{},
    order_by: []const RelationalRowsQueryOrder = &.{},
    limit: ?u32 = null,
    offset: u32 = 0,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        self.left.deinit(alloc);
        self.right.deinit(alloc);
        for (self.correlations) |correlation| {
            alloc.free(correlation.left_field);
            alloc.free(correlation.right_field);
        }
        if (self.correlations.len > 0) alloc.free(self.correlations);
        for (self.match_expression_predicates) |condition| freeRelationalRowsExpressionCondition(alloc, condition);
        if (self.match_expression_predicates.len > 0) alloc.free(self.match_expression_predicates);
        for (self.match_expression_or_predicates) |group| {
            for (group.conditions) |condition| freeRelationalRowsExpressionCondition(alloc, condition);
            if (group.conditions.len > 0) alloc.free(group.conditions);
        }
        if (self.match_expression_or_predicates.len > 0) alloc.free(self.match_expression_or_predicates);
        for (self.match_expression_not_predicates) |group| {
            for (group.conditions) |condition| freeRelationalRowsExpressionCondition(alloc, condition);
            if (group.conditions.len > 0) alloc.free(group.conditions);
        }
        if (self.match_expression_not_predicates.len > 0) alloc.free(self.match_expression_not_predicates);
        for (self.match_expression_array_contains) |predicate| {
            freeRelationalRowsExpression(alloc, predicate.expression);
            alloc.free(predicate.value_json);
        }
        if (self.match_expression_array_contains.len > 0) alloc.free(self.match_expression_array_contains);
        for (self.select) |projection| {
            alloc.free(projection.output);
            alloc.free(projection.field);
        }
        if (self.select.len > 0) alloc.free(self.select);
        for (self.order_by) |order| {
            if (order.field.len > 0) alloc.free(order.field);
            if (order.expression) |expression| freeRelationalRowsExpression(alloc, expression);
        }
        if (self.order_by.len > 0) alloc.free(self.order_by);
        self.* = undefined;
    }
};

pub const RelationalRowsLateralPlan = struct {
    ctes: []const RelationalRowsCte = &.{},
    left_table: []const u8 = "",
    right_table: []const u8 = "",
    left_ranges: []const RelationalRowsDocKeyRange = &.{},
    right_ranges: []const RelationalRowsDocKeyRange = &.{},
    lateral: RelationalRowsLateralRequest = .{},

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        for (self.ctes) |cte| {
            var owned = cte;
            owned.deinit(alloc);
        }
        if (self.ctes.len > 0) alloc.free(self.ctes);
        if (self.left_table.len > 0) alloc.free(self.left_table);
        if (self.right_table.len > 0) alloc.free(self.right_table);
        freeRelationalRowsDocKeyRanges(alloc, self.left_ranges);
        freeRelationalRowsDocKeyRanges(alloc, self.right_ranges);
        self.lateral.deinit(alloc);
        self.* = undefined;
    }
};

fn freeRelationalRowsDocKeyRanges(alloc: Allocator, ranges: []const RelationalRowsDocKeyRange) void {
    for (ranges) |range| {
        if (range.start.len > 0) alloc.free(range.start);
        if (range.end.len > 0) alloc.free(range.end);
    }
    if (ranges.len > 0) alloc.free(ranges);
}

pub const RelationalRowsJoinResult = struct {
    rows: [][]const u8 = &.{},
    total_rows: u32 = 0,
    strategy_selection: ?RelationalRowsJoinStrategySelection = null,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        for (self.rows) |row| alloc.free(@constCast(row));
        if (self.rows.len > 0) alloc.free(self.rows);
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

pub const RelationalIdentityRewrite = struct {
    old_key: []const u8,
    new_key: []const u8,
    value: []const u8,
};

pub const TransactionVersionPredicate = struct {
    key: []const u8,
    expected_version: u64,
};

pub const ForeignKeyParentCheck = struct {
    pub const Timing = enum(u8) {
        immediate,
        deferred,
    };

    constraint_name: []const u8,
    child_table: []const u8,
    child_key: []const u8,
    parent_table: []const u8,
    parent_key: []const u8,
    parent_constraint_name: ?[]const u8 = null,
    child_period_start_json: ?[]const u8 = null,
    child_period_end_json: ?[]const u8 = null,
    timing: Timing = .immediate,
};

pub const ForeignKeyConstraintTimingOverride = struct {
    constraint_name: []const u8,
    timing: ForeignKeyParentCheck.Timing,
};

pub const ForeignKeyParentDeleteCheck = struct {
    pub const Operation = enum(u8) {
        delete,
        update,
    };

    constraint_name: []const u8,
    parent_table: []const u8,
    parent_key: []const u8,
    timing: ForeignKeyParentCheck.Timing = .immediate,
    operation: Operation = .delete,
};

pub const ForeignKeyConflictCheck = struct {
    constraint_name: []const u8,
    parent_table: []const u8,
    parent_key: []const u8,
};

pub const ForeignKeyRefMutation = struct {
    constraint_name: []const u8,
    parent_table: []const u8,
    parent_key: []const u8,
    child_table: []const u8,
    child_key: []const u8,
};

pub const ForeignKeyRefChild = struct {
    child_table: []const u8,
    child_key: []const u8,
};

pub const ForeignKeyRefChildrenPage = struct {
    children: []ForeignKeyRefChild,
    complete: bool = true,
    next_child_table: ?[]const u8 = null,
    next_child_key: ?[]const u8 = null,
};

pub const ForeignKeySetNullChildAction = struct {
    pub const Operation = enum(u8) {
        delete,
        update,
    };

    constraint_name: []const u8,
    parent_table: []const u8,
    parent_key: []const u8,
    child_key: []const u8,
    operation: Operation = .delete,
};

pub const ForeignKeyCascadeChildAction = struct {
    pub const Operation = enum(u8) {
        delete,
        update,
    };

    constraint_name: []const u8,
    parent_table: []const u8,
    parent_key: []const u8,
    child_key: []const u8,
    updated_parent_key: ?[]const u8 = null,
    operation: Operation = .delete,
};

pub const ForeignKeyActionScheduleMutation = struct {
    schedule_id: []const u8,
    action_job_id: []const u8,
    action: []const u8,
    worker_id: []const u8,
    constraint_name: []const u8,
    parent_table: []const u8,
    parent_key: []const u8,
    updated_parent_key: ?[]const u8 = null,
    page_limit: usize,
    cascade_depth: u32,
    cascade_max_depth: u32,
};

pub const UniqueConstraintMutation = struct {
    constraint_name: []const u8,
    encoded_value: []const u8,
    owner_key: []const u8,
    temporal_start: ?[]const u8 = null,
    temporal_end: ?[]const u8 = null,
};

pub const TransactionIntentRequest = struct {
    writes: []const TransactionWrite = &.{},
    deletes: []const []const u8 = &.{},
    relational_identity_rewrites: []const RelationalIdentityRewrite = &.{},
    transforms: []const DocumentTransform = &.{},
    predicates: []const TransactionVersionPredicate = &.{},
    foreign_key_parent_checks: []const ForeignKeyParentCheck = &.{},
    foreign_key_parent_delete_checks: []const ForeignKeyParentDeleteCheck = &.{},
    foreign_key_conflict_checks: []const ForeignKeyConflictCheck = &.{},
    foreign_key_set_null_children: []const ForeignKeySetNullChildAction = &.{},
    foreign_key_cascade_children: []const ForeignKeyCascadeChildAction = &.{},
    foreign_key_action_schedules: []const ForeignKeyActionScheduleMutation = &.{},
    foreign_key_ref_writes: []const ForeignKeyRefMutation = &.{},
    foreign_key_ref_deletes: []const ForeignKeyRefMutation = &.{},
    foreign_key_externalized_parent_checks: []const ForeignKeyParentCheck = &.{},
    foreign_key_constraint_timing_overrides: []const ForeignKeyConstraintTimingOverride = &.{},
    unique_constraint_writes: []const UniqueConstraintMutation = &.{},
    unique_constraint_deletes: []const UniqueConstraintMutation = &.{},
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
    graph_metric_queries: []const NamedGraphMetricQuery = &.{},
    graph_metric_rerank: ?GraphMetricRerank = null,
    merge_config: ?MergeConfig = null,
    reranker: ?reranking_mod.Config = null,
    reranker_query_text: []const u8 = "",
    pruner: ?fusion_mod.Pruner = null,
    expand_strategy: ?graph_query_mod.ExpandStrategy = null,
    return_mode: ReturnMode = .parent,
    max_chunks_per_parent: u32 = 0,
    hierarchy_include_source: bool = false,
    hierarchy_include_unit: bool = false,
    fields: []const []const u8 = &.{},
    include_all_fields: bool = true,
    defer_stored_projection: bool = false,
    row_claim: ?RowClaimRequest = null,
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

pub const RowClaimMode = enum {
    for_update,
    for_no_key_update,
    for_share,
    for_key_share,

    pub fn isExclusiveWriteClaim(self: @This()) bool {
        return switch (self) {
            .for_update, .for_no_key_update => true,
            .for_share, .for_key_share => false,
        };
    }

    pub fn usesDurableIntent(self: @This()) bool {
        return switch (self) {
            .for_update,
            .for_no_key_update,
            .for_share,
            .for_key_share,
            => true,
        };
    }
};

pub const RowClaimWaitPolicy = enum {
    wait,
    nowait,
    skip_locked,
};

pub const RowClaimRequest = struct {
    mode: RowClaimMode = .for_update,
    wait_policy: RowClaimWaitPolicy = .wait,
    skip_locked: bool = false,
    lease_ms: u64 = 30_000,
    owner_id: []const u8 = "",
    txn_id: ?TxnId = null,

    pub fn effectiveWaitPolicy(self: @This()) RowClaimWaitPolicy {
        return if (self.skip_locked) .skip_locked else self.wait_policy;
    }

    pub fn effectiveSkipLocked(self: @This()) bool {
        return self.effectiveWaitPolicy() == .skip_locked;
    }
};

pub const NamedGraphQuery = struct {
    name: []const u8,
    query: graph_query_mod.GraphQuery,
};

pub const GraphMetricFreshness = enum {
    published,
    fresh,
};

pub const GraphMetricQuery = struct {
    index_name: []const u8,
    metric_name: []const u8,
    top_k: u32 = 10,
    freshness: GraphMetricFreshness = .published,
};

pub const NamedGraphMetricQuery = struct {
    name: []const u8,
    query: GraphMetricQuery,
};

pub const GraphMetricRerank = struct {
    index_name: []const u8,
    metric_name: []const u8,
    freshness: GraphMetricFreshness = .published,
    base_weight: f64 = 1.0,
    weight: f64 = 1.0,
    missing_score: f64 = 0.0,
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

pub const GraphMetricRerankScoreDetails = struct {
    index_name: []u8,
    metric_name: []u8,
    base_score: f64 = 0.0,
    base_weight: f64 = 1.0,
    metric_score: ?f64 = null,
    metric_score_used: f64 = 0.0,
    metric_weight: f64 = 1.0,
    missing_score_used: bool = false,
    final_score: f64 = 0.0,
    published_generation: u64 = 0,

    pub fn clone(self: GraphMetricRerankScoreDetails, alloc: Allocator) !GraphMetricRerankScoreDetails {
        const index_name = try alloc.dupe(u8, self.index_name);
        errdefer alloc.free(index_name);
        const metric_name = try alloc.dupe(u8, self.metric_name);
        errdefer alloc.free(metric_name);
        return .{
            .index_name = index_name,
            .metric_name = metric_name,
            .base_score = self.base_score,
            .base_weight = self.base_weight,
            .metric_score = self.metric_score,
            .metric_score_used = self.metric_score_used,
            .metric_weight = self.metric_weight,
            .missing_score_used = self.missing_score_used,
            .final_score = self.final_score,
            .published_generation = self.published_generation,
        };
    }

    pub fn deinit(self: *GraphMetricRerankScoreDetails, alloc: Allocator) void {
        alloc.free(self.index_name);
        alloc.free(self.metric_name);
        self.* = undefined;
    }
};

pub const SearchHit = struct {
    id: []u8,
    doc_ordinal: ?u32 = null,
    score: ?f32 = null,
    score_details: ?GraphMetricRerankScoreDetails = null,
    index_scores: []fusion_mod.IndexScore = &.{},
    stored_data: ?[]u8 = null,
    ancestor_source_data: ?[]u8 = null,
    ancestor_unit_data: ?[]u8 = null,
    artifact_ref: ?ArtifactRef = null,
    chunk_hits: []ChunkHit = &.{},

    pub fn clone(self: SearchHit, alloc: Allocator) !SearchHit {
        var cloned = SearchHit{
            .id = try alloc.dupe(u8, self.id),
            .doc_ordinal = self.doc_ordinal,
            .score = self.score,
            .score_details = if (self.score_details) |details| try details.clone(alloc) else null,
            .index_scores = try cloneIndexScores(alloc, self.index_scores),
            .stored_data = if (self.stored_data) |data| try alloc.dupe(u8, data) else null,
            .ancestor_source_data = if (self.ancestor_source_data) |data| try alloc.dupe(u8, data) else null,
            .ancestor_unit_data = if (self.ancestor_unit_data) |data| try alloc.dupe(u8, data) else null,
            .artifact_ref = if (self.artifact_ref) |artifact_ref| try artifact_ref.clone(alloc) else null,
            .chunk_hits = &.{},
        };
        errdefer {
            alloc.free(cloned.id);
            if (cloned.score_details) |*details| details.deinit(alloc);
            freeIndexScores(alloc, cloned.index_scores);
            if (cloned.stored_data) |data| alloc.free(data);
            if (cloned.ancestor_source_data) |data| alloc.free(data);
            if (cloned.ancestor_unit_data) |data| alloc.free(data);
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
        if (self.score_details) |*details| details.deinit(alloc);
        freeIndexScores(alloc, self.index_scores);
        if (self.stored_data) |data| alloc.free(data);
        if (self.ancestor_source_data) |data| alloc.free(data);
        if (self.ancestor_unit_data) |data| alloc.free(data);
        if (self.artifact_ref) |*artifact_ref| artifact_ref.deinit(alloc);
        for (self.chunk_hits) |*chunk| chunk.deinit(alloc);
        if (self.chunk_hits.len > 0) alloc.free(self.chunk_hits);
        self.* = undefined;
    }
};

pub fn cloneIndexScores(alloc: Allocator, scores: []const fusion_mod.IndexScore) ![]fusion_mod.IndexScore {
    if (scores.len == 0) return &.{};
    const cloned = try alloc.alloc(fusion_mod.IndexScore, scores.len);
    var initialized: usize = 0;
    errdefer {
        for (cloned[0..initialized]) |score| alloc.free(score.index_name);
        alloc.free(cloned);
    }
    for (scores, 0..) |score, i| {
        cloned[i] = .{
            .index_name = try alloc.dupe(u8, score.index_name),
            .score = score.score,
        };
        initialized += 1;
    }
    return cloned;
}

pub fn freeIndexScores(alloc: Allocator, scores: []fusion_mod.IndexScore) void {
    for (scores) |score| alloc.free(score.index_name);
    if (scores.len > 0) alloc.free(scores);
}

pub const ChunkHit = struct {
    id: []u8,
    score: ?f32 = null,
    stored_data: ?[]u8 = null,
    ancestor_source_data: ?[]u8 = null,
    ancestor_unit_data: ?[]u8 = null,
    artifact_ref: ?ArtifactRef = null,

    pub fn clone(self: ChunkHit, alloc: Allocator) !ChunkHit {
        return .{
            .id = try alloc.dupe(u8, self.id),
            .score = self.score,
            .stored_data = if (self.stored_data) |data| try alloc.dupe(u8, data) else null,
            .ancestor_source_data = if (self.ancestor_source_data) |data| try alloc.dupe(u8, data) else null,
            .ancestor_unit_data = if (self.ancestor_unit_data) |data| try alloc.dupe(u8, data) else null,
            .artifact_ref = if (self.artifact_ref) |artifact_ref| try artifact_ref.clone(alloc) else null,
        };
    }

    pub fn deinit(self: *ChunkHit, alloc: Allocator) void {
        alloc.free(self.id);
        if (self.stored_data) |data| alloc.free(data);
        if (self.ancestor_source_data) |data| alloc.free(data);
        if (self.ancestor_unit_data) |data| alloc.free(data);
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
    graph_metric_results: []GraphMetricResult = &.{},
    graph_metric_rerank_status: ?GraphMetricStatus = null,

    pub fn deinit(self: *SearchResult) void {
        for (self.hits) |*hit| hit.deinit(self.alloc);
        if (self.hits.len > 0) self.alloc.free(self.hits);
        for (self.graph_results) |*graph_result| graph_result.deinit(self.alloc);
        if (self.graph_results.len > 0) self.alloc.free(self.graph_results);
        for (self.graph_metric_results) |*metric_result| metric_result.deinit(self.alloc);
        if (self.graph_metric_results.len > 0) self.alloc.free(self.graph_metric_results);
        if (self.graph_metric_rerank_status) |*status| status.deinit(self.alloc);
        self.* = undefined;
    }
};

pub const GraphMetricScore = struct {
    node: []u8,
    score: f64,

    pub fn deinit(self: *GraphMetricScore, alloc: Allocator) void {
        alloc.free(self.node);
        self.* = undefined;
    }
};

pub const GraphMetricResult = struct {
    name: []u8,
    index_name: []u8,
    metric_name: []u8,
    scores: []GraphMetricScore,
    status: GraphMetricStatus,

    pub fn deinit(self: *GraphMetricResult, alloc: Allocator) void {
        alloc.free(self.name);
        alloc.free(self.index_name);
        alloc.free(self.metric_name);
        for (self.scores) |*score| score.deinit(alloc);
        if (self.scores.len > 0) alloc.free(self.scores);
        self.status.deinit(alloc);
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
    metric_status: []GraphMetricStatus = &.{},

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
        freeGraphMetricStatuses(alloc, self.metric_status);
        self.* = undefined;
    }
};

pub const GraphMetricStatus = struct {
    name: []u8,
    state: graph_mod.GraphIndex.GraphMetricState = .not_ready,
    phase: graph_mod.GraphIndex.GraphMetricBuildPhase = .idle,
    edge_filter: graph_mod.GraphMetricEdgeFilter = .{},
    metadata_version: u32 = 0,
    maintenance_paused: bool = false,
    build_queued: bool = false,
    published_generation: u64 = 0,
    edge_generation: u64 = 0,
    target_edge_generation: u64 = 0,
    queued_generation: u64 = 0,
    building_generation: u64 = 0,
    build_job_id: u64 = 0,
    build_started_at_ms: u64 = 0,
    build_iteration: u32 = 0,
    build_lease_expires_at_ms: u64 = 0,
    build_worker_id: []const u8 = "",
    build_cursor: []const u8 = "",
    build_completed_units: u64 = 0,
    build_total_units: u64 = 0,
    build_pages: []GraphMetricBuildPageStatus = &.{},
    build_pages_truncated: bool = false,
    retry_count: u64 = 0,
    last_error: []const u8 = "",
    progress: f64 = 0.0,
    converged: bool = false,
    iterations_completed: u32 = 0,
    delta: f64 = 0.0,
    computed_at_ms: u64 = 0,
    last_event: ?graph_mod.GraphIndex.GraphMetricEvent = null,
    recent_events: []graph_mod.GraphIndex.GraphMetricEvent = &.{},

    pub fn deinit(self: *GraphMetricStatus, alloc: Allocator) void {
        alloc.free(self.name);
        self.edge_filter.deinit(alloc);
        if (self.build_worker_id.len > 0) alloc.free(self.build_worker_id);
        if (self.build_cursor.len > 0) alloc.free(self.build_cursor);
        for (self.build_pages) |*page| page.deinit(alloc);
        if (self.build_pages.len > 0) alloc.free(self.build_pages);
        if (self.last_error.len > 0) alloc.free(self.last_error);
        if (self.recent_events.len > 0) alloc.free(self.recent_events);
        self.* = undefined;
    }
};

pub const GraphMetricBuildPageStatus = struct {
    phase: graph_mod.GraphIndex.GraphMetricBuildPhase = .idle,
    iteration: u32 = 0,
    page_id: u64 = 0,
    state: graph_mod.GraphIndex.GraphMetricBuildPageState = .pending,
    range_kind: graph_mod.GraphIndex.GraphMetricBuildPageRangeKind = .full,
    worker_id: []const u8 = "",
    lease_expires_at_ms: u64 = 0,
    attempt: u64 = 0,
    cursor: []const u8 = "",
    completed_units: u64 = 0,
    total_units: u64 = 0,
    last_error: []const u8 = "",

    pub fn deinit(self: *GraphMetricBuildPageStatus, alloc: Allocator) void {
        if (self.worker_id.len > 0) alloc.free(self.worker_id);
        if (self.cursor.len > 0) alloc.free(self.cursor);
        if (self.last_error.len > 0) alloc.free(self.last_error);
        self.* = undefined;
    }
};

pub fn freeGraphMetricStatuses(alloc: Allocator, statuses: []GraphMetricStatus) void {
    for (statuses) |*status| status.deinit(alloc);
    if (statuses.len > 0) alloc.free(statuses);
}

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

pub const ReplayStageStats = struct {
    enabled: bool = false,
    target_sequence: u64 = 0,
    applied_sequence: u64 = 0,
    catch_up_required: bool = false,
    blocked: bool = false,
    blocked_reason: []const u8 = "",
    error_count: u64 = 0,
};

pub const ResolverReplayDiagnostic = struct {
    name: []const u8 = "",
    table: []const u8 = "",
    source_artifact: []const u8 = "",
    resolution_artifact: []const u8 = "",
};

pub const ResolverReplayDiagnostics = struct {
    resolver_count: u64 = 0,
    resolution_runtime_present: bool = false,
    resolution_worker_started: bool = false,
    promotion_runtime_present: bool = false,
    promotion_worker_started: bool = false,
    resolvers: []const ResolverReplayDiagnostic = &.{},
};

pub const TransactionRecoveryStats = struct {
    enabled: bool = false,
    lease_owned: bool = false,
    has_lease: bool = false,
    acquisition_count: u64 = 0,
    takeover_count: u64 = 0,
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
    pending_heap_bytes: u64 = 0,
    pending_mmap_bytes: u64 = 0,
    in_flight_merges: u64 = 0,
    in_flight_segments: u64 = 0,
    completed_merges: u64 = 0,
    skipped_stale_merges: u64 = 0,
    failed_merges: u64 = 0,
    merge_input_segments_total: u64 = 0,
    merge_input_bytes_total: u64 = 0,
    merge_output_segments_total: u64 = 0,
    merge_output_bytes_total: u64 = 0,
    last_merge_input_segments: u64 = 0,
    last_merge_input_bytes: u64 = 0,
    last_merge_output_segments: u64 = 0,
    last_merge_output_bytes: u64 = 0,
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

pub const GraphMetricRuntimeRole = enum {
    combined,
    coordinator,
    worker,
    worker_pool,
};

pub const GraphMetricRuntimeStats = struct {
    enabled: bool = false,
    role: ?GraphMetricRuntimeRole = null,
    runtime_id_hash: u64 = 0,
    owner_id_hash: u64 = 0,
    lease_key_hash: u64 = 0,
    worker_id_hash: u64 = 0,
    worker_count: u64 = 0,
    lease_owned: bool = false,
    has_lease: bool = false,
    acquisition_count: u64 = 0,
    takeover_count: u64 = 0,
    lease_acquire_failures: u64 = 0,
    lost_leases: u64 = 0,
    last_acquired_ms: u64 = 0,
    started: bool = false,
    shutdown: bool = false,
    notified: bool = false,
    ticks_started: u64 = 0,
    ticks_completed: u64 = 0,
    durable_progress_ticks: u64 = 0,
    idle_ticks: u64 = 0,
    error_ticks: u64 = 0,
    last_error_name: ?[]const u8 = null,
    total_metrics_scanned: u64 = 0,
    total_active_builds: u64 = 0,
    total_builds_started: u64 = 0,
    total_worker_steps: u64 = 0,
    total_coordinator_steps: u64 = 0,
    total_pages_claimed: u64 = 0,
    total_pages_completed: u64 = 0,
    total_phases_advanced: u64 = 0,
    total_published: u64 = 0,
    total_failed_builds: u64 = 0,
    last_metrics_scanned: u64 = 0,
    last_active_builds: u64 = 0,
    last_builds_started: u64 = 0,
    last_worker_steps: u64 = 0,
    last_coordinator_steps: u64 = 0,
    last_pages_claimed: u64 = 0,
    last_pages_completed: u64 = 0,
    last_phases_advanced: u64 = 0,
    last_published: u64 = 0,
    last_failed_builds: u64 = 0,
    last_budget_exhausted: bool = false,

    pub fn hasRuntimeFacts(self: @This()) bool {
        return self.enabled or
            self.role != null or
            self.runtime_id_hash != 0 or
            self.owner_id_hash != 0 or
            self.lease_key_hash != 0 or
            self.worker_id_hash != 0 or
            self.worker_count != 0 or
            self.lease_owned or
            self.has_lease or
            self.acquisition_count != 0 or
            self.takeover_count != 0 or
            self.lease_acquire_failures != 0 or
            self.lost_leases != 0 or
            self.last_acquired_ms != 0 or
            self.started or
            self.shutdown or
            self.notified or
            self.ticks_started != 0 or
            self.ticks_completed != 0 or
            self.durable_progress_ticks != 0 or
            self.idle_ticks != 0 or
            self.error_ticks != 0 or
            self.last_error_name != null or
            self.total_metrics_scanned != 0 or
            self.total_active_builds != 0 or
            self.total_builds_started != 0 or
            self.total_worker_steps != 0 or
            self.total_coordinator_steps != 0 or
            self.total_pages_claimed != 0 or
            self.total_pages_completed != 0 or
            self.total_phases_advanced != 0 or
            self.total_published != 0 or
            self.total_failed_builds != 0 or
            self.last_metrics_scanned != 0 or
            self.last_active_builds != 0 or
            self.last_builds_started != 0 or
            self.last_worker_steps != 0 or
            self.last_coordinator_steps != 0 or
            self.last_pages_claimed != 0 or
            self.last_pages_completed != 0 or
            self.last_phases_advanced != 0 or
            self.last_published != 0 or
            self.last_failed_builds != 0 or
            self.last_budget_exhausted;
    }
};

pub fn accumulateTextMergeStats(dst: *TextMergeStats, src: TextMergeStats) void {
    dst.enabled = dst.enabled or src.enabled;
    dst.pending_indexes +|= src.pending_indexes;
    dst.pending_segments +|= src.pending_segments;
    dst.pending_bytes +|= src.pending_bytes;
    dst.pending_heap_bytes +|= src.pending_heap_bytes;
    dst.pending_mmap_bytes +|= src.pending_mmap_bytes;
    dst.in_flight_merges +|= src.in_flight_merges;
    dst.in_flight_segments +|= src.in_flight_segments;
    dst.completed_merges +|= src.completed_merges;
    dst.skipped_stale_merges +|= src.skipped_stale_merges;
    dst.failed_merges +|= src.failed_merges;
    dst.merge_input_segments_total +|= src.merge_input_segments_total;
    dst.merge_input_bytes_total +|= src.merge_input_bytes_total;
    dst.merge_output_segments_total +|= src.merge_output_segments_total;
    dst.merge_output_bytes_total +|= src.merge_output_bytes_total;
    dst.last_merge_input_segments = @max(dst.last_merge_input_segments, src.last_merge_input_segments);
    dst.last_merge_input_bytes = @max(dst.last_merge_input_bytes, src.last_merge_input_bytes);
    dst.last_merge_output_segments = @max(dst.last_merge_output_segments, src.last_merge_output_segments);
    dst.last_merge_output_bytes = @max(dst.last_merge_output_bytes, src.last_merge_output_bytes);
    dst.quarantined_merges +|= src.quarantined_merges;
    dst.quarantined_segments +|= src.quarantined_segments;
    if (src.last_merge_error.len != 0) dst.last_merge_error = src.last_merge_error;
    dst.retry_after_ns = @max(dst.retry_after_ns, src.retry_after_ns);
    dst.deferred_for_pressure +|= src.deferred_for_pressure;
    dst.backpressure_events +|= src.backpressure_events;
    dst.backpressure_ns +|= src.backpressure_ns;
    dst.max_pending_segments = @max(dst.max_pending_segments, src.max_pending_segments);
    dst.max_pending_bytes = @max(dst.max_pending_bytes, src.max_pending_bytes);
}

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

pub const ForeignKeyStats = struct {
    child_write_rejects: u64 = 0,
    parent_delete_rejects: u64 = 0,
    validation_runs: u64 = 0,
    dry_run_runs: u64 = 0,
    repair_runs: u64 = 0,
    scanned_child_rows: u64 = 0,
    referenced_child_rows: u64 = 0,
    scanned_ref_rows: u64 = 0,
    missing_parent_rows: u64 = 0,
    missing_ref_rows: u64 = 0,
    stale_ref_rows: u64 = 0,
    repaired_ref_rows: u64 = 0,
    deleted_stale_ref_rows: u64 = 0,

    pub fn hasRuntimeFacts(self: @This()) bool {
        return self.child_write_rejects != 0 or
            self.parent_delete_rejects != 0 or
            self.validation_runs != 0 or
            self.dry_run_runs != 0 or
            self.repair_runs != 0 or
            self.scanned_child_rows != 0 or
            self.referenced_child_rows != 0 or
            self.scanned_ref_rows != 0 or
            self.missing_parent_rows != 0 or
            self.missing_ref_rows != 0 or
            self.stale_ref_rows != 0 or
            self.repaired_ref_rows != 0 or
            self.deleted_stale_ref_rows != 0;
    }
};

pub const DBStats = struct {
    doc_count: u64 = 0,
    index_count: u32 = 0,
    indexes: []DBIndexStats = &.{},
    doc_identity: DocIdentityStats = .{},
    doc_set_planning: DocSetPlanningStats = .{},
    foreign_keys: ForeignKeyStats = .{},
    enrichment: EnrichmentStats = .{},
    resolution: ReplayStageStats = .{},
    promotion: ReplayStageStats = .{},
    resolver_replay: ResolverReplayDiagnostics = .{},
    ttl_cleanup: TTLCleanupStats = .{},
    transaction_recovery: TransactionRecoveryStats = .{},
    text_merge: TextMergeStats = .{},
    graph_metric_runtime: GraphMetricRuntimeStats = .{},
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
    // Error name recorded when the index's persisted artifacts failed to
    // load (e.g. "UnsupportedVersion"); null for healthy indexes.
    load_error: ?[]const u8 = null,
    doc_count: u64 = 0,
    term_count: u64 = 0,
    edge_count: u64 = 0,
    node_count: u64 = 0,
    root_node: u64 = 0,
    backfill_active: bool = false,
    backfill_progress: f64 = 0.0,
    enrichment_failed: bool = false,
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
    graph_metric_status: []GraphMetricStatus = &.{},
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
    replay_scan_batches: u64 = 0,
    replay_hint_filter_skips: u64 = 0,
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
    wal_checkpoint_oldest_retained_segment: u64 = 0,
    wal_checkpoint_covered_through_segment: u64 = 0,
    wal_checkpoint_current_segment: u64 = 0,
    wal_checkpoint_lag_segments: u64 = 0,
    wal_replay_retained_segments: u64 = 0,
    wal_replay_retained_bytes: u64 = 0,
    wal_replay_current_segment: u64 = 0,
    configured_indexes: u32 = 0,
    configured_dense_indexes: u32 = 0,
    configured_sparse_indexes: u32 = 0,
    configured_full_text_indexes: u32 = 0,
    configured_graph_indexes: u32 = 0,
    opened_indexes: u32 = 0,
    db_open_ns: u64 = 0,
    load_indexes_ns: u64 = 0,
    lsm_open_stores: u64 = 0,
    lsm_open_completed: u64 = 0,
    lsm_open_failed: u64 = 0,
    lsm_open_total_ns: u64 = 0,
    lsm_open_initializing_storage_ns: u64 = 0,
    lsm_open_manifest_ns: u64 = 0,
    lsm_open_ensuring_dirs_ns: u64 = 0,
    lsm_open_wal_replay_ns: u64 = 0,
    lsm_open_mounting_runs_ns: u64 = 0,
    lsm_open_loaded_runs: u64 = 0,
    lsm_open_obsolete_paths: u64 = 0,
    lsm_open_mutable_entries_after_replay: u64 = 0,
    lsm_open_immutable_memtables_after_replay: u64 = 0,
    wal_replay_records: u64 = 0,
    wal_replay_entries: u64 = 0,
    wal_replay_bytes: u64 = 0,
    wal_replay_ns: u64 = 0,
    wal_replay_truncated_tail_bytes: u64 = 0,
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
    dst.replay_scan_batches += src.replay_scan_batches;
    dst.replay_hint_filter_skips += src.replay_hint_filter_skips;
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
    dst.wal_checkpoint_oldest_retained_segment = minNonZeroU64(dst.wal_checkpoint_oldest_retained_segment, src.wal_checkpoint_oldest_retained_segment);
    dst.wal_checkpoint_covered_through_segment = @max(dst.wal_checkpoint_covered_through_segment, src.wal_checkpoint_covered_through_segment);
    dst.wal_checkpoint_current_segment = @max(dst.wal_checkpoint_current_segment, src.wal_checkpoint_current_segment);
    dst.wal_checkpoint_lag_segments += src.wal_checkpoint_lag_segments;
    dst.wal_replay_retained_segments += src.wal_replay_retained_segments;
    dst.wal_replay_retained_bytes += src.wal_replay_retained_bytes;
    dst.wal_replay_current_segment = @max(dst.wal_replay_current_segment, src.wal_replay_current_segment);
    dst.configured_indexes = @max(dst.configured_indexes, src.configured_indexes);
    dst.configured_dense_indexes = @max(dst.configured_dense_indexes, src.configured_dense_indexes);
    dst.configured_sparse_indexes = @max(dst.configured_sparse_indexes, src.configured_sparse_indexes);
    dst.configured_full_text_indexes = @max(dst.configured_full_text_indexes, src.configured_full_text_indexes);
    dst.configured_graph_indexes = @max(dst.configured_graph_indexes, src.configured_graph_indexes);
    dst.opened_indexes = @max(dst.opened_indexes, src.opened_indexes);
    dst.db_open_ns = @max(dst.db_open_ns, src.db_open_ns);
    dst.load_indexes_ns = @max(dst.load_indexes_ns, src.load_indexes_ns);
    dst.lsm_open_stores += src.lsm_open_stores;
    dst.lsm_open_completed += src.lsm_open_completed;
    dst.lsm_open_failed += src.lsm_open_failed;
    dst.lsm_open_total_ns += src.lsm_open_total_ns;
    dst.lsm_open_initializing_storage_ns += src.lsm_open_initializing_storage_ns;
    dst.lsm_open_manifest_ns += src.lsm_open_manifest_ns;
    dst.lsm_open_ensuring_dirs_ns += src.lsm_open_ensuring_dirs_ns;
    dst.lsm_open_wal_replay_ns += src.lsm_open_wal_replay_ns;
    dst.lsm_open_mounting_runs_ns += src.lsm_open_mounting_runs_ns;
    dst.lsm_open_loaded_runs += src.lsm_open_loaded_runs;
    dst.lsm_open_obsolete_paths += src.lsm_open_obsolete_paths;
    dst.lsm_open_mutable_entries_after_replay += src.lsm_open_mutable_entries_after_replay;
    dst.lsm_open_immutable_memtables_after_replay += src.lsm_open_immutable_memtables_after_replay;
    dst.wal_replay_records += src.wal_replay_records;
    dst.wal_replay_entries += src.wal_replay_entries;
    dst.wal_replay_bytes += src.wal_replay_bytes;
    dst.wal_replay_ns += src.wal_replay_ns;
    dst.wal_replay_truncated_tail_bytes += src.wal_replay_truncated_tail_bytes;
}

fn minNonZeroU64(lhs: u64, rhs: u64) u64 {
    if (lhs == 0) return rhs;
    if (rhs == 0) return lhs;
    return @min(lhs, rhs);
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

pub fn freeResolverReplayDiagnostics(alloc: Allocator, stats: ResolverReplayDiagnostics) void {
    for (stats.resolvers) |resolver| {
        alloc.free(resolver.name);
        alloc.free(resolver.table);
        alloc.free(resolver.source_artifact);
        alloc.free(resolver.resolution_artifact);
    }
    if (stats.resolvers.len > 0) alloc.free(stats.resolvers);
}

pub fn freeDBStats(alloc: Allocator, stats: DBStats) void {
    freeResolverReplayDiagnostics(alloc, stats.resolver_replay);
    for (stats.indexes) |item| {
        alloc.free(item.name);
        if (item.load_error) |value| alloc.free(value);
        if (item.algebraic_last_error_doc_key) |value| alloc.free(value);
        if (item.algebraic_last_error_reason) |value| alloc.free(value);
        if (item.algebraic_capability_fingerprint) |value| alloc.free(value);
        if (item.algebraic_capability_lifecycle_status) |value| alloc.free(value);
        if (item.algebraic_planner_last_decision) |value| alloc.free(value);
        if (item.algebraic_planner_last_fallback_reason) |value| alloc.free(value);
        if (item.algebraic_planner_lifecycle_blocking_reason) |value| alloc.free(value);
        if (item.algebraic_last_observed_query_shape) |value| alloc.free(value);
        if (item.algebraic_last_recommended_materialization) |value| alloc.free(value);
        freeGraphMetricStatuses(alloc, @constCast(item.graph_metric_status));
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
