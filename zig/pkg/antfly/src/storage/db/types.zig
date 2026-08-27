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
const resource_manager_mod = @import("../resource_manager.zig");
const index_repair_status = @import("../../common/index_repair_status.zig");
const document_content_hash = @import("document_content_hash.zig");
pub const CancellationToken = @import("../../common/cancellation.zig").CancellationToken;
pub const IndexRepairStatus = index_repair_status.IndexRepairStatus;
pub const DocumentContentHash = document_content_hash.Digest;

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
    full_index,
};

pub fn parsePublicSyncLevelText(text: []const u8) ?SyncLevel {
    if (std.mem.eql(u8, text, "propose")) return .propose;
    if (std.mem.eql(u8, text, "write")) return .write;
    if (std.mem.eql(u8, text, "full_text")) return .full_text;
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
        .full_text => "full_text",
        .enrichments => "enrichments",
        .full_index => "full_index",
    };
}

test "public sync level text accepts full_index and rejects removed aknn alias" {
    try std.testing.expect(parsePublicSyncLevelText("aknn") == null);
    try std.testing.expectEqual(SyncLevel.full_index, parsePublicSyncLevelText("full_index").?);
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

pub const SplitReplicationCheckpoint = struct {
    pub const Kind = enum {
        destination_begin,
        destination_complete,
        source_ack,
    };

    kind: Kind,
    transition_id: u64,
    attempt_epoch: u64,
    source_group_id: u64,
    destination_group_id: u64,
    range_start: []const u8 = "",
    range_end: []const u8 = "",
    delta_sequence: u64,
};

/// Identity inherited by an unpublished split destination. Every replicated
/// destination batch carries this context so all replicas create the physical
/// DB with the same namespace before the destination range is catalog-visible.
pub const SplitReplicationContext = struct {
    pub const Operation = enum {
        bootstrap_chunk,
        delta,
        checkpoint,
    };

    transition_id: u64,
    attempt_epoch: u64,
    source_group_id: u64,
    destination_group_id: u64,
    identity_namespace: doc_identity_mod.Namespace,
    /// Present only while streaming a baseline bootstrap. Catch-up deltas use
    /// null and are fenced by the completed destination marker.
    bootstrap_sequence: ?u64 = null,
    operation: Operation = .bootstrap_chunk,
    /// Source split-delta sequence. Zero for bootstrap chunks.
    sequence: u64 = 0,
};

pub const SplitTransitionMutation = struct {
    pub const Kind = enum {
        prepare,
        start,
        finalize,
        rollback,
    };

    kind: Kind,
    transition_id: u64,
    attempt_epoch: u64,
    destination_group_id: u64,
    split_key: []const u8 = "",
};

/// Private data-Raft command used by the distributed transaction protocol.
/// Every value that can affect durable state is carried in the command so
/// replay is deterministic on followers and after restart.
pub const TransactionMutation = union(enum) {
    begin: struct {
        txn_id: TxnId,
        begin_timestamp: u64,
        created_at_ns: u64,
        topology_epoch: u64,
        /// Long-lived, externally addressable transaction sessions retain a
        /// terminal decision for their full retry window. Anonymous commits
        /// use the shorter ordinary recovery retention.
        retain_terminal: bool = false,
        participants: []const []const u8,
    },
    prepare: struct {
        txn_id: TxnId,
        topology_epoch: u64,
    },
    resolve: struct {
        txn_id: TxnId,
        status: TxnStatus,
        commit_version: u64,
    },
    /// Coordinator-side acknowledgement that one participant has durably
    /// learned the terminal decision. This must be ordered by coordinator
    /// Raft rather than written by a replica-local recovery worker.
    acknowledge: struct {
        txn_id: TxnId,
        participant: []const u8,
    },
    /// Deterministic coordinator/participant metadata cleanup. The cutoff is
    /// carried in the command so every replica evaluates the same predicate.
    cleanup: struct {
        txn_id: TxnId,
        cutoff_timestamp: u64,
        retained_cutoff_timestamp: u64,
    },
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
    /// Internal single-participant transaction contract. Transform expansion
    /// still runs under the DB apply lock, but the batch is rejected before
    /// mutation if it would emit graph projection deltas that the distributed
    /// transaction intent format cannot represent.
    reject_graph_transform_projections: bool = false,
    /// Internal data-Raft transition state. Public batch parsing never sets it.
    split_checkpoint: ?SplitReplicationCheckpoint = null,
    /// Internal identity context for writes to an unpublished split destination.
    split_replication: ?SplitReplicationContext = null,
    /// Internal source lifecycle mutation. It must be ordered with data writes.
    split_transition: ?SplitTransitionMutation = null,
    /// Internal 2PC phase. Public batch parsing never accepts this field.
    transaction: ?TransactionMutation = null,
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

pub const IndexPublicationPolicy = enum {
    progressive,
    atomic,
};

pub fn indexPublicationPolicy(alloc: Allocator, cfg: IndexConfig) !IndexPublicationPolicy {
    if (cfg.kind != .dense_vector and cfg.kind != .sparse_vector) return .atomic;
    const Parsed = struct {
        publication_policy: ?IndexPublicationPolicy = null,
    };
    var parsed = try std.json.parseFromSlice(Parsed, alloc, cfg.config_json, .{
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();
    return parsed.value.publication_policy orelse .progressive;
}

test "embeddings publication policy defaults progressive and preserves atomic" {
    const progressive = IndexConfig{
        .name = "progressive",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3}",
    };
    try std.testing.expectEqual(IndexPublicationPolicy.progressive, try indexPublicationPolicy(std.testing.allocator, progressive));

    var atomic = progressive;
    atomic.config_json = "{\"field\":\"embedding\",\"dims\":3,\"publication_policy\":\"atomic\"}";
    try std.testing.expectEqual(IndexPublicationPolicy.atomic, try indexPublicationPolicy(std.testing.allocator, atomic));
}

pub const IndexConfig = struct {
    name: []const u8,
    kind: IndexKind,
    config_json: []const u8,
    coverage_generation: u64 = 0,
    // Internal semantic identity, validated and computed once when the config
    // enters the catalog. It is derived metadata and is not serialized.
    coverage_config_fingerprint: ?u64 = null,

    pub fn clone(alloc: Allocator, cfg: IndexConfig) !IndexConfig {
        const name = try alloc.dupe(u8, cfg.name);
        errdefer alloc.free(name);
        return .{
            .name = name,
            .kind = cfg.kind,
            .config_json = try alloc.dupe(u8, cfg.config_json),
            .coverage_generation = cfg.coverage_generation,
            .coverage_config_fingerprint = cfg.coverage_config_fingerprint,
        };
    }

    pub fn deinit(self: *IndexConfig, alloc: Allocator) void {
        alloc.free(self.name);
        alloc.free(self.config_json);
        self.* = undefined;
    }
};

pub const PublicIndexConfig = struct {
    name: []const u8,
    kind: IndexKind,
    config_json: []const u8,
};

pub fn publicIndexConfigsAlloc(alloc: Allocator, configs: []const IndexConfig) ![]PublicIndexConfig {
    const public_configs = try alloc.alloc(PublicIndexConfig, configs.len);
    for (configs, 0..) |cfg, i| {
        public_configs[i] = .{
            .name = cfg.name,
            .kind = cfg.kind,
            .config_json = cfg.config_json,
        };
    }
    return public_configs;
}

pub fn indexConfigHash(cfg: IndexConfig) u64 {
    var hasher = std.hash.Wyhash.init(0x41504a4346470001);
    hashLengthPrefixedBytes(&hasher, cfg.name);
    hashLengthPrefixedBytes(&hasher, @tagName(cfg.kind));
    hashLengthPrefixedBytes(&hasher, cfg.config_json);
    return hasher.final();
}

fn hashLengthPrefixedBytes(hasher: *std.hash.Wyhash, bytes: []const u8) void {
    var len_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &len_buf, bytes.len, .little);
    hasher.update(&len_buf);
    hasher.update(bytes);
}

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
    vector_space: []const u8 = "",
    chunk_size: u32 = 0,
    chunk_overlap: u32 = 0,
    chunker_json: []const u8 = "",
    full_text_index: bool = false,
    content_type: []const u8 = "",
    producer_json: []const u8 = "",
    execution: ?EnrichmentExecutionConfig = null,

    pub fn clone(alloc: Allocator, cfg: EnrichmentConfig) !EnrichmentConfig {
        return .{
            .name = try alloc.dupe(u8, cfg.name),
            .kind = cfg.kind,
            .field = if (cfg.field.len > 0) try alloc.dupe(u8, cfg.field) else "",
            .template = if (cfg.template.len > 0) try alloc.dupe(u8, cfg.template) else "",
            .source_artifact_name = if (cfg.source_artifact_name.len > 0) try alloc.dupe(u8, cfg.source_artifact_name) else "",
            .expected_dims = cfg.expected_dims,
            .vector_space = if (cfg.vector_space.len > 0) try alloc.dupe(u8, cfg.vector_space) else "",
            .chunk_size = cfg.chunk_size,
            .chunk_overlap = cfg.chunk_overlap,
            .chunker_json = if (cfg.chunker_json.len > 0) try alloc.dupe(u8, cfg.chunker_json) else "",
            .full_text_index = cfg.full_text_index,
            .content_type = if (cfg.content_type.len > 0) try alloc.dupe(u8, cfg.content_type) else "",
            .producer_json = if (cfg.producer_json.len > 0) try alloc.dupe(u8, cfg.producer_json) else "",
            .execution = cfg.execution,
        };
    }

    pub fn deinit(self: *EnrichmentConfig, alloc: Allocator) void {
        alloc.free(self.name);
        if (self.field.len > 0) alloc.free(self.field);
        if (self.template.len > 0) alloc.free(self.template);
        if (self.source_artifact_name.len > 0) alloc.free(self.source_artifact_name);
        if (self.vector_space.len > 0) alloc.free(self.vector_space);
        if (self.chunker_json.len > 0) alloc.free(self.chunker_json);
        if (self.content_type.len > 0) alloc.free(self.content_type);
        if (self.producer_json.len > 0) alloc.free(self.producer_json);
        self.* = undefined;
    }
};

pub const EnrichmentExecutionConfig = struct {
    batch_items: ?u32 = null,
    batch_bytes: ?u64 = null,
};

pub fn enrichmentConfigHash(cfg: EnrichmentConfig) u64 {
    var hasher = std.hash.Wyhash.init(0x41454a4346470001);
    hashLengthPrefixedBytes(&hasher, cfg.name);
    hashLengthPrefixedBytes(&hasher, @tagName(cfg.kind));
    hashLengthPrefixedBytes(&hasher, cfg.field);
    hashLengthPrefixedBytes(&hasher, cfg.template);
    hashLengthPrefixedBytes(&hasher, cfg.source_artifact_name);
    hashU32(&hasher, cfg.expected_dims);
    hashLengthPrefixedBytes(&hasher, cfg.vector_space);
    hashU32(&hasher, cfg.chunk_size);
    hashU32(&hasher, cfg.chunk_overlap);
    hashLengthPrefixedBytes(&hasher, cfg.chunker_json);
    hashBool(&hasher, cfg.full_text_index);
    hashLengthPrefixedBytes(&hasher, cfg.content_type);
    hashLengthPrefixedBytes(&hasher, cfg.producer_json);
    return hasher.final();
}

fn hashU32(hasher: *std.hash.Wyhash, value: u32) void {
    var buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &buf, value, .little);
    hasher.update(&buf);
}

fn hashBool(hasher: *std.hash.Wyhash, value: bool) void {
    hasher.update(if (value) "\x01" else "\x00");
}

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
    ocr_attempted_count: usize = 0,
    ocr_selected_count: usize = 0,
    ocr_retained_embedded_count: usize = 0,
    ocr_failed_count: usize = 0,
    ocr_failed_page_numbers: []i64 = &.{},
    ocr_failed_pages_truncated: bool = false,
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
        if (self.ocr_failed_page_numbers.len > 0) alloc.free(self.ocr_failed_page_numbers);
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
    /// Distinguishes an explicitly optional pure-`should` query from the
    /// conventional pure disjunction whose implicit minimum is one.
    pure_should_optional: bool = false,
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
    /// Internal, absolute monotonic deadline used by routed lookups. It is not
    /// part of the public lookup projection contract and is never serialized.
    execution_deadline_ns: ?u64 = null,
    /// Borrowed request cancellation source. Callers must keep it alive for
    /// the synchronous lookup call.
    cancellation: ?CancellationToken = null,
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
    filter_query_json: []const u8 = "",
    /// Internal-only response mode used by linear merge. Public scans leave
    /// this false and retain their existing NDJSON shape.
    include_content_hashes: bool = false,
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
    content_hash: ?DocumentContentHash = null,

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
    /// Text-native positive filter. Unlike `full_text`, this constrains every
    /// retrieval source without contributing a score.
    filter_text: ?TextQuery = null,
    /// Text-native negative filter. Matches are removed from every retrieval
    /// source without contributing a score.
    exclusion_text: ?TextQuery = null,
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
    hierarchy_include_source: bool = false,
    hierarchy_include_unit: bool = false,
    hierarchy_omit_implicit_source_ancestor_document: bool = false,
    hierarchy_match_fields: []const []const u8 = &.{},
    hierarchy_match_include_all_fields: bool = true,
    hierarchy_grouped_matches: bool = false,
    hierarchy_group_level: HierarchyGroupLevel = .source,
    hierarchy_children: ?HierarchyChildrenRequest = null,
    /// Internal distributed-planning control. Parent-owned navigation and
    /// globally merged unit groups may return identity-only hits for payloads
    /// routed elsewhere; direct callers fail closed when payload is absent.
    defer_hierarchy_child_hydration: bool = false,
    hierarchy_source_fields: []const []const u8 = &.{},
    hierarchy_source_include_all_fields: bool = true,
    hierarchy_unit_fields: []const []const u8 = &.{},
    hierarchy_unit_include_all_fields: bool = true,
    fields: []const []const u8 = &.{},
    order_by: []const SortField = &.{},
    search_after: []const std.json.Value = &.{},
    search_before: []const std.json.Value = &.{},
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
    /// Request-local authorization hook used only by the distributed graph
    /// coordinator when an edge names a document in another table. The hook is
    /// never serialized to a shard worker; it resolves the target table to a
    /// trusted internal row predicate before owner-routed admission.
    graph_table_read_authorizer: ?GraphTableReadAuthorizer = null,
    identity_read_generation: ?u64 = null,
    execution_deadline_ns: ?u64 = null,
    /// Borrowed listener lifecycle signal. It is request-local and must never
    /// be retained by asynchronous work after query execution returns.
    cancellation: ?CancellationToken = null,
    require_algebraic_filter_resolution: bool = false,
    distributed_text_stats: []const distributed_stats_mod.TextFieldStats = &.{},
};

pub const max_canonical_hierarchy_groups: u32 = 100;
pub const max_canonical_hierarchy_matches_per_group: u32 = 100;
pub const max_canonical_hierarchy_total_matches: u32 = 1000;

// Every SearchRequest field must be deliberately classified for child
// traversal. The compile-time audit prevents newly added retrieval controls
// from becoming silently accepted no-ops at this storage boundary.
const hierarchy_children_validated_fields = [_][]const u8{
    "return_mode",
    "hierarchy_grouped_matches",
    "hierarchy_children",
    "fields",
    "order_by",
    "search_after",
    "include_all_fields",
    "limit",
    "offset",
    "include_stored",
};

const hierarchy_children_supported_internal_fields = [_][]const u8{
    "filter_query_json",
    "exclusion_query_json",
    "defer_hierarchy_child_hydration",
    "defer_stored_projection",
    "filter_doc_ids",
    "filter_doc_ids_positive",
    "exclude_doc_ids",
    "resolved_doc_filter",
    "resolved_doc_filter_owned",
    "resolved_doc_filter_wire_context",
    "identity_read_generation",
    "execution_deadline_ns",
    "cancellation",
};

const hierarchy_children_rejected_fields = [_][]const u8{
    "query",
    "index_name",
    "primary_text_index_name",
    "aggregations_json",
    "count_only",
    "profile",
    "full_text",
    "filter_text",
    "exclusion_text",
    "full_text_queries",
    "doc_filter_bindings",
    "dense",
    "sparse",
    "dense_queries",
    "sparse_queries",
    "graph_queries",
    "merge_config",
    "reranker",
    "reranker_query_text",
    "pruner",
    "expand_strategy",
    "max_chunks_per_parent",
    "hierarchy_include_source",
    "hierarchy_include_unit",
    "hierarchy_omit_implicit_source_ancestor_document",
    "hierarchy_match_fields",
    "hierarchy_match_include_all_fields",
    "hierarchy_group_level",
    "hierarchy_source_fields",
    "hierarchy_source_include_all_fields",
    "hierarchy_unit_fields",
    "hierarchy_unit_include_all_fields",
    "search_before",
    "search_effort",
    "filter_prefix",
    "distance_over",
    "distance_under",
    "filter_ids",
    "exclude_ids",
    "resolved_text_doc_filter",
    "graph_table_read_authorizer",
    "require_algebraic_filter_resolution",
    "distributed_text_stats",
};

fn auditHierarchyChildrenSearchRequestFields() void {
    @setEvalBranchQuota(10_000);
    inline for (@typeInfo(SearchRequest).@"struct".fields) |field| {
        comptime var classifications: usize = 0;
        inline for (hierarchy_children_validated_fields) |name| {
            if (std.mem.eql(u8, field.name, name)) classifications += 1;
        }
        inline for (hierarchy_children_supported_internal_fields) |name| {
            if (std.mem.eql(u8, field.name, name)) classifications += 1;
        }
        inline for (hierarchy_children_rejected_fields) |name| {
            if (std.mem.eql(u8, field.name, name)) classifications += 1;
        }
        if (classifications != 1) {
            @compileError("SearchRequest field must have exactly one hierarchy-children policy: " ++ field.name);
        }
    }
}

/// Validate the fully normalized storage contract for sequential hierarchy
/// traversal. Public parsers enforce the same shape, but typed and internal
/// callers also cross this boundary and must never be able to trigger unchecked
/// cursor indexing or accidentally request unbounded materialization.
pub fn canonicalHierarchyChildrenRequestIsValid(req: SearchRequest) bool {
    @setEvalBranchQuota(100_000);
    comptime auditHierarchyChildrenSearchRequestFields();
    const children = req.hierarchy_children orelse return true;
    if (children.parent_id.len == 0 or children.parent_level != .source or children.level != .unit) return false;
    if (req.return_mode != .unit or req.hierarchy_grouped_matches or req.max_chunks_per_parent != 0) return false;
    // Traversal is deliberately not a retrieval operation. Reject every
    // scoring, ranking, or transform control at this final typed boundary so
    // embedded callers and worker envelopes cannot receive plausible-looking
    // results for options that navigation would otherwise ignore. Internal
    // authorization constraints and lifecycle controls remain supported.
    const defaults = SearchRequest{};
    inline for (hierarchy_children_rejected_fields) |field_name| {
        if (!std.meta.eql(@field(req, field_name), @field(defaults, field_name))) return false;
    }
    if (req.limit == 0 or req.limit > max_canonical_hierarchy_groups) return false;
    if (req.offset != 0 or req.count_only or req.aggregations_json.len > 0) return false;
    if (req.search_before.len > 0 or req.include_all_fields) return false;
    if (req.order_by.len != 2 or
        !std.mem.eql(u8, req.order_by[0].field, "_hierarchy.position") or
        req.order_by[0].desc or
        !std.mem.eql(u8, req.order_by[1].field, "_id") or
        req.order_by[1].desc)
    {
        return false;
    }
    if (req.search_after.len == 0) return true;
    return req.search_after.len == 2 and
        req.search_after[0] == .string and
        req.search_after[1] == .string;
}

pub fn canonicalHierarchyExecutionWithinBudget(req: SearchRequest) bool {
    if (!canonicalHierarchyChildrenRequestIsValid(req)) return false;
    if (!req.hierarchy_grouped_matches) return true;
    if (req.limit == 0 or req.limit > max_canonical_hierarchy_groups) return false;
    if (req.max_chunks_per_parent == 0 or
        req.max_chunks_per_parent > max_canonical_hierarchy_matches_per_group)
    {
        return false;
    }
    return @as(u64, req.limit) * @as(u64, req.max_chunks_per_parent) <=
        max_canonical_hierarchy_total_matches;
}

/// Return the first phase of a canonical grouped hierarchy query. Distributed
/// coordinators use this request on every shard, merge the global group page,
/// and only then issue one bounded expansion for those selected groups.
pub fn canonicalGroupedMatchSelectionRequest(req: SearchRequest) SearchRequest {
    if (!req.hierarchy_grouped_matches or
        (req.return_mode != .parent_with_chunks and req.return_mode != .unit_with_chunks) or
        req.max_chunks_per_parent == 0)
    {
        return req;
    }
    var selection = req;
    selection.return_mode = switch (req.hierarchy_group_level) {
        .source => .parent,
        .unit => .unit,
    };
    selection.max_chunks_per_parent = 0;
    selection.hierarchy_grouped_matches = false;
    return selection;
}

/// Build a bounded grouped-match request. `parent_filter` may contain one
/// source for local expansion or the complete globally selected page for a
/// distributed batch expansion.
pub fn canonicalGroupedMatchExpansionRequest(
    req: SearchRequest,
    parent_filter: []const []const u8,
) SearchRequest {
    var match_req = req;
    match_req.offset = 0;
    match_req.limit = @intCast(parent_filter.len);
    // Top-level cursors page source groups. They must never be applied to the
    // independently ranked descendants within the selected groups.
    match_req.search_after = &.{};
    match_req.search_before = &.{};
    match_req.filter_doc_ids = parent_filter;
    match_req.filter_doc_ids_positive = true;
    match_req.graph_queries = &.{};
    match_req.expand_strategy = null;
    match_req.aggregations_json = "";
    match_req.count_only = false;
    match_req.profile = false;
    return match_req;
}

/// Build the exact per-source descendant query used by a storage shard after
/// its source groups have been selected.
pub fn canonicalGroupedMatchDescendantRequest(
    req: SearchRequest,
    parent_filter: []const []const u8,
) SearchRequest {
    var match_req = canonicalGroupedMatchExpansionRequest(req, parent_filter);
    match_req.return_mode = .chunk;
    match_req.max_chunks_per_parent = 0;
    match_req.hierarchy_grouped_matches = false;
    match_req.limit = req.max_chunks_per_parent;
    match_req.fields = req.hierarchy_match_fields;
    match_req.include_all_fields = req.hierarchy_match_include_all_fields;
    match_req.include_stored = match_req.include_all_fields or
        match_req.fields.len > 0 or
        match_req.reranker != null;
    return match_req;
}

pub const GraphTableReadAuthorization = struct {
    allowed: bool,
    /// Owned by this value when non-null.
    filter_query_json: ?[]u8 = null,

    pub fn deinit(self: *GraphTableReadAuthorization, alloc: std.mem.Allocator) void {
        if (self.filter_query_json) |value| alloc.free(value);
        self.* = undefined;
    }
};

pub const GraphTableReadAuthorizer = struct {
    ctx: ?*const anyopaque,
    authorize_table: *const fn (
        ctx: ?*const anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) anyerror!GraphTableReadAuthorization,

    pub fn authorize(
        self: GraphTableReadAuthorizer,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) !GraphTableReadAuthorization {
        return try self.authorize_table(self.ctx, alloc, table_name);
    }
};

pub const SortField = struct {
    field: []const u8,
    desc: bool = false,
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
    /// Compatibility spelling for raw chunk/member results.
    chunk,
    parent_with_chunks,
    unit,
    unit_with_chunks,
    /// Return each indexed source member without hierarchy grouping. This is
    /// the precise name for raw results from heterogeneous artifact unions.
    /// Appended to preserve the numeric ABI of the established modes.
    member,
};

pub const HierarchyGroupLevel = enum {
    source,
    unit,
};

pub const HierarchyChildrenRequest = struct {
    parent_id: []const u8,
    parent_level: HierarchyGroupLevel = .source,
    level: HierarchyGroupLevel = .unit,
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
    /// Internal graph-hydration namespace. Null means the query's source
    /// table. This is not serialized as part of the public search-hit shape.
    source_table: ?[]u8 = null,
    doc_ordinal: ?u32 = null,
    native_text_doc_id: ?u32 = null,
    /// Higher-is-better relevance score used by every public query path.
    score: ?f32 = null,
    /// Metric-native dense-vector distance. Lower values are better. This is
    /// retained separately so score ordering never depends on the metric.
    distance: ?f32 = null,
    index_scores: []fusion_mod.IndexScore = &.{},
    sort_values: []std.json.Value = &.{},
    stored_data: ?[]u8 = null,
    ancestor_source_data: ?[]u8 = null,
    ancestor_unit_data: ?[]u8 = null,
    artifact_ref: ?ArtifactRef = null,
    chunk_hits: []ChunkHit = &.{},

    pub fn clone(self: SearchHit, alloc: Allocator) !SearchHit {
        var cloned = SearchHit{ .id = try alloc.dupe(u8, self.id) };
        errdefer {
            alloc.free(cloned.id);
            if (cloned.source_table) |table| alloc.free(table);
            freeIndexScores(alloc, cloned.index_scores);
            freeJsonValues(alloc, cloned.sort_values);
            if (cloned.stored_data) |data| alloc.free(data);
            if (cloned.ancestor_source_data) |data| alloc.free(data);
            if (cloned.ancestor_unit_data) |data| alloc.free(data);
            if (cloned.artifact_ref) |*artifact_ref| artifact_ref.deinit(alloc);
        }
        cloned.source_table = if (self.source_table) |table| try alloc.dupe(u8, table) else null;
        cloned.doc_ordinal = self.doc_ordinal;
        cloned.native_text_doc_id = self.native_text_doc_id;
        cloned.score = self.score;
        cloned.distance = self.distance;
        cloned.index_scores = try cloneIndexScores(alloc, self.index_scores);
        cloned.sort_values = try cloneJsonValues(alloc, self.sort_values);
        cloned.stored_data = if (self.stored_data) |data| try alloc.dupe(u8, data) else null;
        cloned.ancestor_source_data = if (self.ancestor_source_data) |data| try alloc.dupe(u8, data) else null;
        cloned.ancestor_unit_data = if (self.ancestor_unit_data) |data| try alloc.dupe(u8, data) else null;
        cloned.artifact_ref = if (self.artifact_ref) |artifact_ref| try artifact_ref.clone(alloc) else null;

        if (self.chunk_hits.len == 0) return cloned;

        const chunk_hits = try alloc.alloc(ChunkHit, self.chunk_hits.len);
        var initialized: usize = 0;
        errdefer {
            for (chunk_hits[0..initialized]) |*chunk| chunk.deinit(alloc);
            alloc.free(chunk_hits);
        }
        for (self.chunk_hits, 0..) |chunk, i| {
            chunk_hits[i] = try chunk.clone(alloc);
            initialized += 1;
        }
        cloned.chunk_hits = chunk_hits;
        return cloned;
    }

    pub fn deinit(self: *SearchHit, alloc: Allocator) void {
        alloc.free(self.id);
        if (self.source_table) |table| alloc.free(table);
        freeIndexScores(alloc, self.index_scores);
        freeJsonValues(alloc, self.sort_values);
        if (self.stored_data) |data| alloc.free(data);
        if (self.ancestor_source_data) |data| alloc.free(data);
        if (self.ancestor_unit_data) |data| alloc.free(data);
        if (self.artifact_ref) |*artifact_ref| artifact_ref.deinit(alloc);
        for (self.chunk_hits) |*chunk| chunk.deinit(alloc);
        if (self.chunk_hits.len > 0) alloc.free(self.chunk_hits);
        self.* = undefined;
    }
};

pub fn cloneOwnedByteSlices(alloc: Allocator, values: []const []const u8) ![]const []u8 {
    if (values.len == 0) return &.{};
    const cloned = try alloc.alloc([]u8, values.len);
    var initialized: usize = 0;
    errdefer {
        for (cloned[0..initialized]) |value| alloc.free(value);
        alloc.free(cloned);
    }
    for (values, 0..) |value, i| {
        cloned[i] = try alloc.dupe(u8, value);
        initialized += 1;
    }
    return cloned;
}

pub fn freeOwnedByteSlices(alloc: Allocator, values: []const []u8) void {
    for (values) |value| alloc.free(value);
    if (values.len > 0) alloc.free(values);
}

pub fn cloneJsonValue(alloc: Allocator, value: std.json.Value) !std.json.Value {
    return switch (value) {
        .null, .bool, .integer, .float => value,
        .number_string => |text| .{ .number_string = try alloc.dupe(u8, text) },
        .string => |text| .{ .string = try alloc.dupe(u8, text) },
        .array => |arr| blk: {
            var cloned = std.json.Array.init(alloc);
            errdefer {
                for (cloned.items) |*item| deinitJsonValue(alloc, item);
                cloned.deinit();
            }
            for (arr.items) |item| {
                var cloned_item = try cloneJsonValue(alloc, item);
                errdefer deinitJsonValue(alloc, &cloned_item);
                try cloned.append(cloned_item);
            }
            break :blk .{ .array = cloned };
        },
        .object => |obj| blk: {
            var cloned = std.json.ObjectMap.empty;
            errdefer {
                var it = cloned.iterator();
                while (it.next()) |entry| {
                    alloc.free(@constCast(entry.key_ptr.*));
                    deinitJsonValue(alloc, entry.value_ptr);
                }
                cloned.deinit(alloc);
            }
            var it = obj.iterator();
            while (it.next()) |entry| {
                const key = try alloc.dupe(u8, entry.key_ptr.*);
                errdefer alloc.free(key);
                var cloned_value = try cloneJsonValue(alloc, entry.value_ptr.*);
                errdefer deinitJsonValue(alloc, &cloned_value);
                try cloned.put(alloc, key, cloned_value);
            }
            break :blk .{ .object = cloned };
        },
    };
}

pub fn deinitJsonValue(alloc: Allocator, value: *std.json.Value) void {
    switch (value.*) {
        .string, .number_string => |text| alloc.free(text),
        .array => |*arr| {
            for (arr.items) |*item| deinitJsonValue(alloc, item);
            arr.deinit();
        },
        .object => |*obj| {
            var it = obj.iterator();
            while (it.next()) |entry| {
                alloc.free(@constCast(entry.key_ptr.*));
                deinitJsonValue(alloc, entry.value_ptr);
            }
            obj.deinit(alloc);
        },
        else => {},
    }
    value.* = .null;
}

pub fn cloneJsonValues(alloc: Allocator, values: []const std.json.Value) ![]std.json.Value {
    if (values.len == 0) return &.{};
    const cloned = try alloc.alloc(std.json.Value, values.len);
    var initialized: usize = 0;
    errdefer {
        for (cloned[0..initialized]) |*value| deinitJsonValue(alloc, value);
        alloc.free(cloned);
    }
    for (values, 0..) |value, i| {
        cloned[i] = try cloneJsonValue(alloc, value);
        initialized += 1;
    }
    return cloned;
}

pub fn freeJsonValues(alloc: Allocator, values: []std.json.Value) void {
    for (values) |*value| deinitJsonValue(alloc, value);
    if (values.len > 0) alloc.free(values);
}

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
    distance: ?f32 = null,
    stored_data: ?[]u8 = null,
    ancestor_source_data: ?[]u8 = null,
    ancestor_unit_data: ?[]u8 = null,
    artifact_ref: ?ArtifactRef = null,

    pub fn clone(self: ChunkHit, alloc: Allocator) !ChunkHit {
        return .{
            .id = try alloc.dupe(u8, self.id),
            .score = self.score,
            .distance = self.distance,
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

pub const SortProfileField = struct {
    bytes: [256]u8 = undefined,
    len: u16 = 0,

    pub fn init(value: []const u8) SortProfileField {
        var out = SortProfileField{};
        out.set(value);
        return out;
    }

    pub fn set(self: *SortProfileField, value: []const u8) void {
        const value_len = @min(value.len, self.bytes.len);
        if (value_len > 0) @memcpy(self.bytes[0..value_len], value[0..value_len]);
        self.len = @intCast(value_len);
    }

    pub fn slice(self: *const SortProfileField) []const u8 {
        return self.bytes[0..self.len];
    }
};

pub const SortProfile = struct {
    plan: []const u8 = "",
    exactness: []const u8 = "",
    source: []const u8 = "",
    candidate_source: []const u8 = "none",
    cursor_support: []const u8 = "",
    source_load: []const u8 = "",
    distributed_behavior: []const u8 = "",
    selection_reason: []const u8 = "",
    require_native: bool = false,
    native_loader: bool = false,
    sort_lifecycle_state: []const u8 = "",
    native_filter_mode: []const u8 = "none",
    native_filter_candidate_count: u64 = 0,
    native_filter_exclusion_count: u64 = 0,
    selective_filter_doc_values_preferred: bool = false,
    cost_model_live_docs: u64 = 0,
    cost_model_candidate_count: u64 = 0,
    cost_model_selective_limit: u64 = 0,
    native_doc_values_coverage: []const u8 = "",
    index_sort_coverage: []const u8 = "",
    index_sort_match: bool = false,
    sorted_segment_executor_available: bool = false,
    sorted_segment_bounds_available: bool = false,
    sorted_segment_scanned_count: u64 = 0,
    sorted_segment_scan_budget: u64 = 0,
    candidate_count: u64 = 0,
    cursor_rejected_count: u64 = 0,
    admitted_count: u64 = 0,
    replaced_count: u64 = 0,
    discarded_count: u64 = 0,
    selected_count: u64 = 0,
    decorate_us: u64 = 0,
    native_doc_value_load_us: u64 = 0,
    native_doc_value_hit_count: u64 = 0,
    native_doc_value_miss_count: u64 = 0,
    stored_json_load_us: u64 = 0,
    stored_json_load_count: u64 = 0,
    projected_source_load_us: u64 = 0,
    projected_source_load_count: u64 = 0,
    final_sort_us: u64 = 0,
    total_us: u64 = 0,
    window_capacity: usize = 0,
    window_len: usize = 0,
    collector_heap_peak: usize = 0,
    distributed_shard_count: usize = 0,
    distributed_shard_window: usize = 0,
    budget_rejection_reason: []const u8 = "",
    sort_rejection_reason: []const u8 = "",
    sort_rejection_detail: []const u8 = "",
    sort_rejection_field: SortProfileField = .{},
};

pub const ShardIdentityReadGeneration = struct {
    group_id: u64,
    generation: u64,
};

pub const SearchResult = struct {
    alloc: Allocator,
    hits: []SearchHit,
    total_hits: u32,
    total_hits_relation: TotalHitsRelation = .exact,
    identity_read_generation: ?u64 = null,
    /// Snapshot vector for a distributed result. Shard generations are
    /// independent, so a multi-shard replay must use these tokens rather than
    /// relying on `identity_read_generation` being globally common.
    shard_identity_read_generations: []ShardIdentityReadGeneration = &.{},
    sort_profile: ?SortProfile = null,
    graph_results: []GraphSearchResult = &.{},

    pub fn deinit(self: *SearchResult) void {
        for (self.hits) |*hit| hit.deinit(self.alloc);
        if (self.hits.len > 0) self.alloc.free(self.hits);
        for (self.graph_results) |*graph_result| graph_result.deinit(self.alloc);
        if (self.graph_results.len > 0) self.alloc.free(self.graph_results);
        if (self.shard_identity_read_generations.len > 0) self.alloc.free(self.shard_identity_read_generations);
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
            if (edge.metadata.len > 0) alloc.free(edge.metadata);
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
    projection_checkpoint_status: []const u8 = "clean",
    projection_checkpoint_applied_sequence: u64 = 0,
    projection_checkpoint_generation: u64 = 0,
    projection_checkpoint_config_hash: u64 = 0,
    projection_checkpoint_identity_consistent: bool = true,
    checkpoint_replay_tail_sequence_count: u64 = 0,
    processed_requests: u64 = 0,
    error_count: u64 = 0,
    retryable_error_count: u64 = 0,
    fatal_error_count: u64 = 0,
    consecutive_retry_count: u32 = 0,
    next_retry_at_ms: u64 = 0,
    retrying: bool = false,
    worker_failed: bool = false,
    worker_started: bool = false,
    stalled: bool = false,
    skip_by_hash_count: u64 = 0,
    skipped_source_count: u64 = 0,
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
    last_embed_batch_completed_ms: u64 = 0,
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
    active_indexes: u64 = 0,
    active_segments: u64 = 0,
    max_active_segments_per_index: u64 = 0,
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
    merge_elapsed_ns_total: u64 = 0,
    merge_peak_task_alloc_bytes: u64 = 0,
    last_merge_input_segments: u64 = 0,
    last_merge_input_bytes: u64 = 0,
    last_merge_output_segments: u64 = 0,
    last_merge_output_bytes: u64 = 0,
    last_merge_elapsed_ns: u64 = 0,
    last_merge_peak_task_alloc_bytes: u64 = 0,
    quarantined_merges: u64 = 0,
    quarantined_segments: u64 = 0,
    last_merge_error: []const u8 = "",
    retry_after_ns: u64 = 0,
    deferred_for_pressure: u64 = 0,
    backpressure_events: u64 = 0,
    backpressure_ns: u64 = 0,
    backpressure_timeouts: u64 = 0,
    backpressure_failures: u64 = 0,
    max_pending_segments: u64 = 0,
    max_pending_bytes: u64 = 0,
};

pub fn accumulateTextMergeStats(dst: *TextMergeStats, src: TextMergeStats) void {
    dst.enabled = dst.enabled or src.enabled;
    dst.active_indexes +|= src.active_indexes;
    dst.active_segments +|= src.active_segments;
    dst.max_active_segments_per_index = @max(dst.max_active_segments_per_index, src.max_active_segments_per_index);
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
    dst.merge_elapsed_ns_total +|= src.merge_elapsed_ns_total;
    dst.merge_peak_task_alloc_bytes = @max(dst.merge_peak_task_alloc_bytes, src.merge_peak_task_alloc_bytes);
    dst.last_merge_input_segments = @max(dst.last_merge_input_segments, src.last_merge_input_segments);
    dst.last_merge_input_bytes = @max(dst.last_merge_input_bytes, src.last_merge_input_bytes);
    dst.last_merge_output_segments = @max(dst.last_merge_output_segments, src.last_merge_output_segments);
    dst.last_merge_output_bytes = @max(dst.last_merge_output_bytes, src.last_merge_output_bytes);
    dst.last_merge_elapsed_ns = @max(dst.last_merge_elapsed_ns, src.last_merge_elapsed_ns);
    dst.last_merge_peak_task_alloc_bytes = @max(dst.last_merge_peak_task_alloc_bytes, src.last_merge_peak_task_alloc_bytes);
    dst.quarantined_merges +|= src.quarantined_merges;
    dst.quarantined_segments +|= src.quarantined_segments;
    if (src.last_merge_error.len != 0) dst.last_merge_error = src.last_merge_error;
    dst.retry_after_ns = @max(dst.retry_after_ns, src.retry_after_ns);
    dst.deferred_for_pressure +|= src.deferred_for_pressure;
    dst.backpressure_events +|= src.backpressure_events;
    dst.backpressure_ns +|= src.backpressure_ns;
    dst.backpressure_timeouts +|= src.backpressure_timeouts;
    dst.backpressure_failures +|= src.backpressure_failures;
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
    visibility_chunk_size: u32 = 0,
    visibility_chunks: u64 = 0,
    visibility_deleted_ordinals: u64 = 0,
    visibility_mask_bytes: u64 = 0,
    visibility_repair_count: u64 = 0,
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

pub const VisibilityStats = struct {
    cache_entries: u64 = 0,
    cache_hits_total: u64 = 0,
    cache_misses_total: u64 = 0,
    mask_build_ns_total: u64 = 0,
    mask_builds_total: u64 = 0,
    full_scan_fallbacks_total: u64 = 0,
    overflow_total: u64 = 0,
};

pub const DBStats = struct {
    /// Process-local fingerprint of physical LSM/WAL publications. Runtime
    /// status uses it only to invalidate cached directory-byte observations.
    storage_change_token: u64 = 0,
    /// Canonical live primary-document cardinality from durable identity metadata.
    /// Unlike doc_count, this is independent of derived index fan-out.
    source_doc_count: u64 = 0,
    doc_count: u64 = 0,
    index_count: u32 = 0,
    indexes: []DBIndexStats = &.{},
    repair_degraded: bool = false,
    repair_issue_count: u64 = 0,
    repair_summary_ready: bool = true,
    repair_issue_count_estimated: bool = false,
    doc_identity: DocIdentityStats = .{},
    doc_set_planning: DocSetPlanningStats = .{},
    visibility: VisibilityStats = .{},
    enrichment: EnrichmentStats = .{},
    resolution: ReplayStageStats = .{},
    promotion: ReplayStageStats = .{},
    resolver_replay: ResolverReplayDiagnostics = .{},
    ttl_cleanup: TTLCleanupStats = .{},
    transaction_recovery: TransactionRecoveryStats = .{},
    text_merge: TextMergeStats = .{},
    term_doc_freq_cache_hits: u64 = 0,
    term_doc_freq_cache_misses: u64 = 0,
    async_indexing: AsyncIndexingStats = .{},
};

pub const ArtifactRepairKind = enum {
    embedding,
    asset,
    chunk,
    graph,
    full_text,
    algebraic,
};

pub const RepairTarget = enum {
    artifact,
    index,
};

pub const IndexRepairControl = enum {
    pause_automatic,
    resume_automatic,
    cancel_current_attempt,
};

pub const ArtifactRepairReason = enum {
    missing_artifact,
    corrupt_artifact,
    unreadable_artifact,
    enrichment_failed,
    resource_limit_exceeded,
};

/// Policy-independent coverage health shared by status and repair reporting.
/// A source is settled once it has any terminal outcome; settled failures are
/// degraded, never pending and never healthy completion.
pub const DerivedCoverageHealth = struct {
    settled: u64,
    pending: ?u64,
    counters_valid: bool,
    all_sources_terminal: bool,
    degraded: bool,
};

pub const DerivedCoveragePolicy = enum {
    strict,
    partial,
    best_effort,
};

/// One authoritative interpretation of durable derived-coverage counters.
/// Status endpoints and repair completion must use the same policy semantics:
/// a settled source that the policy does not cover is degraded debt, not a
/// healthy repair merely because the worker has no pending input.
pub const DerivedCoverageAssessment = struct {
    covered: u64,
    health: DerivedCoverageHealth,
    complete: bool,
    healthy: bool,
    degraded: bool,
};

pub fn evaluateDerivedCoverageHealth(
    source_total: u64,
    produced: u64,
    skipped: u64,
    terminal_failed: u64,
    observation_complete: bool,
    replay_current: bool,
) DerivedCoverageHealth {
    const produced_and_skipped = std.math.add(u64, produced, skipped) catch return .{
        .settled = 0,
        .pending = null,
        .counters_valid = false,
        .all_sources_terminal = false,
        .degraded = false,
    };
    const settled = std.math.add(u64, produced_and_skipped, terminal_failed) catch return .{
        .settled = 0,
        .pending = null,
        .counters_valid = false,
        .all_sources_terminal = false,
        .degraded = false,
    };
    const counters_valid = settled <= source_total;
    const all_sources_terminal = counters_valid and settled == source_total;
    return .{
        .settled = settled,
        .pending = if (observation_complete and counters_valid) source_total - settled else null,
        .counters_valid = counters_valid,
        .all_sources_terminal = all_sources_terminal,
        .degraded = observation_complete and replay_current and all_sources_terminal and terminal_failed > 0,
    };
}

pub fn evaluateDerivedCoverageAssessment(
    policy: DerivedCoveragePolicy,
    source_total: u64,
    produced: u64,
    skipped: u64,
    terminal_failed: u64,
    observation_complete: bool,
    replay_current: bool,
) DerivedCoverageAssessment {
    const covered = switch (policy) {
        .strict => produced,
        .partial => std.math.add(u64, produced, skipped) catch std.math.maxInt(u64),
        .best_effort => blk: {
            const produced_and_skipped = std.math.add(u64, produced, skipped) catch break :blk std.math.maxInt(u64);
            break :blk std.math.add(u64, produced_and_skipped, terminal_failed) catch std.math.maxInt(u64);
        },
    };
    const health = evaluateDerivedCoverageHealth(
        source_total,
        produced,
        skipped,
        terminal_failed,
        observation_complete,
        replay_current,
    );
    const complete = observation_complete and replay_current and health.counters_valid and
        health.all_sources_terminal and covered == source_total;
    return .{
        .covered = covered,
        .health = health,
        .complete = complete,
        .healthy = complete and terminal_failed == 0,
        .degraded = observation_complete and replay_current and health.all_sources_terminal and
            (terminal_failed > 0 or covered != source_total),
    };
}

test "derived coverage assessment honors completion policy" {
    const strict = evaluateDerivedCoverageAssessment(.strict, 2, 1, 1, 0, true, true);
    try std.testing.expect(!strict.complete);
    try std.testing.expect(strict.degraded);

    const partial = evaluateDerivedCoverageAssessment(.partial, 2, 1, 1, 0, true, true);
    try std.testing.expect(partial.complete);
    try std.testing.expect(partial.healthy);
    try std.testing.expect(!partial.degraded);

    const best_effort = evaluateDerivedCoverageAssessment(.best_effort, 2, 1, 0, 1, true, true);
    try std.testing.expect(best_effort.complete);
    try std.testing.expect(!best_effort.healthy);
    try std.testing.expect(best_effort.degraded);
}

pub const ArtifactRepairIssue = struct {
    artifact_kind: ArtifactRepairKind = .embedding,
    index_name: []const u8 = "",
    doc_key: []const u8 = "",
    parent_doc_key: []const u8 = "",
    unit_id: []const u8 = "",
    source_artifact_name: []const u8 = "",
    artifact_name: []const u8 = "",
    artifact_key: []const u8 = "",
    chunk_id: ?u32 = null,
    repairable: bool = true,
    unsupported_reason: []const u8 = "",
    sequence: u64 = 0,
    reason: ArtifactRepairReason = .missing_artifact,
    /// Number of source-generation attempts made before the request was
    /// parked. Kept separate from repair attempts for operational clarity.
    generation_attempts: u64 = 0,
    generation_error: []const u8 = "",
    attempts: u64 = 0,
    first_seen_ns: u64 = 0,
    last_seen_ns: u64 = 0,
    last_error: []const u8 = "",

    pub fn deinit(self: *ArtifactRepairIssue, alloc: Allocator) void {
        if (self.index_name.len > 0) alloc.free(@constCast(self.index_name));
        if (self.doc_key.len > 0) alloc.free(@constCast(self.doc_key));
        if (self.parent_doc_key.len > 0) alloc.free(@constCast(self.parent_doc_key));
        if (self.unit_id.len > 0) alloc.free(@constCast(self.unit_id));
        if (self.source_artifact_name.len > 0) alloc.free(@constCast(self.source_artifact_name));
        if (self.artifact_name.len > 0) alloc.free(@constCast(self.artifact_name));
        if (self.artifact_key.len > 0) alloc.free(@constCast(self.artifact_key));
        if (self.unsupported_reason.len > 0) alloc.free(@constCast(self.unsupported_reason));
        if (self.generation_error.len > 0) alloc.free(@constCast(self.generation_error));
        if (self.last_error.len > 0) alloc.free(@constCast(self.last_error));
        self.* = undefined;
    }
};

pub fn freeArtifactRepairIssues(alloc: Allocator, issues: []ArtifactRepairIssue) void {
    for (issues) |*issue| issue.deinit(alloc);
    if (issues.len > 0) alloc.free(issues);
}

pub const ArtifactRepairListRequest = struct {
    target: RepairTarget = .artifact,
    artifact_kind: ?ArtifactRepairKind = null,
    index_name: ?[]const u8 = null,
    limit: u32 = 50,
    cursor: ?[]const u8 = null,
};

pub const ArtifactRepairListResult = struct {
    issues: []ArtifactRepairIssue = &.{},
    limit: u32 = 0,
    scanned: u64 = 0,
    groups_scanned: u64 = 0,
    next_cursor: ?[]u8 = null,
    has_more: bool = false,

    pub fn deinit(self: *ArtifactRepairListResult, alloc: Allocator) void {
        freeArtifactRepairIssues(alloc, self.issues);
        if (self.next_cursor) |value| alloc.free(value);
        self.* = undefined;
    }
};

pub const ArtifactRepairRunRequest = struct {
    target: RepairTarget = .artifact,
    artifact_kind: ?ArtifactRepairKind = null,
    index_name: ?[]const u8 = null,
    limit: u32 = 100,
    cursor: ?[]const u8 = null,
    force: bool = false,
    control: ?IndexRepairControl = null,
    /// Optional compare-and-set fence for operator controls. When supplied,
    /// the control applies only to the currently durable repair generation.
    repair_id: ?u128 = null,
    repair_job_id: ?u64 = null,
    repair_attempt_id: ?u64 = null,
    repair_job_created_at_ms: ?u64 = null,
    repair_cancel_base_uri: ?[]const u8 = null,
};

pub const RepairCancelCheck = struct {
    ptr: *anyopaque,
    is_requested: *const fn (ptr: *anyopaque) bool,

    pub fn requested(self: RepairCancelCheck) bool {
        return self.is_requested(self.ptr);
    }
};

/// Cooperative scheduler preemption checked only at durable reconstruction
/// boundaries. Unlike cancellation, yielding is a successful partial pass: the
/// candidate remains reopenable and its scan cursor is persisted before the
/// BackendRuntime owner releases the node repair slot.
pub const RepairYieldCheck = struct {
    ptr: *anyopaque,
    is_requested: *const fn (ptr: *anyopaque) bool,

    pub fn requested(self: @This()) bool {
        return self.is_requested(self.ptr);
    }
};

pub const RepairActivationCheck = struct {
    ptr: *anyopaque,
    is_current_owner: *const fn (ptr: *anyopaque) anyerror!bool,

    pub fn current(self: RepairActivationCheck) !bool {
        return try self.is_current_owner(self.ptr);
    }
};

pub const RepairCapacityObservation = resource_manager_mod.CapacityObservation;

/// Storage-owned capacity probe. Implementations identify the actual
/// volume/quota domain and synchronously refresh its capacity observation.
/// The callback must be cheap enough to run at repair window boundaries.
pub const RepairCapacitySource = resource_manager_mod.CapacitySource;

pub const RepairCapacityCheck = struct {
    ptr: *anyopaque,
    reconcile: *const fn (ptr: *anyopaque, candidate_bytes: u64) anyerror!void,
    revalidate: *const fn (ptr: *anyopaque) anyerror!void,
    bind_candidate_root: ?*const fn (ptr: *anyopaque, candidate_root: []const u8) anyerror!void = null,

    pub fn current(self: @This(), candidate_bytes: u64) !void {
        return try self.reconcile(self.ptr, candidate_bytes);
    }

    /// Re-check the live capacity domain before another bounded build batch.
    /// Implementations keep the successful path O(1), but may reconcile exact
    /// candidate usage before returning an otherwise-conservative denial.
    pub fn boundary(self: @This()) !void {
        return try self.revalidate(self.ptr);
    }

    /// Binds the exact shadow-generation root whose materialized bytes consume
    /// this reservation. The path is borrowed for the synchronous repair run.
    pub fn bindCandidateRoot(self: @This(), candidate_root: []const u8) !void {
        if (self.bind_candidate_root) |bind| try bind(self.ptr, candidate_root);
    }
};

pub const ArtifactRepairRunOptions = struct {
    cancel_check: ?RepairCancelCheck = null,
    /// Internal BackendRuntime scheduling policy. This is deliberately not an
    /// API/index setting and is observed only after a bounded candidate batch
    /// can be made durable and restart-reopenable.
    yield_check: ?RepairYieldCheck = null,
    /// Revalidated immediately before a replacement pointer is activated.
    /// Long-running reconstruction may outlive leadership or placement.
    activation_check: ?RepairActivationCheck = null,
    /// Durable ownership claim captured by the node scheduler. Zero means the
    /// caller has no stronger epoch than the DB's replica/root identity (the
    /// standalone and operator-driven case). Managed repair passes the current
    /// Raft term, or the visible root generation when no term source exists.
    owner_epoch: u64 = 0,
    /// Internal bounded-activation policy. These are node scheduling values,
    /// not index configuration or public API controls.
    max_activation_gap_sequences: u64 = 200,
    max_convergence_rounds: u32 = 32,
    max_activation_pause_ms: u64 = 250,
    /// Internal node scheduler estimate persisted into the durable repair
    /// intent before candidate construction. These fields are not API data.
    estimated_candidate_bytes: u64 = 0,
    planned_disk_bytes: u64 = 0,
    /// ResourceManager capacity domain and observation source. These are
    /// backend/runtime integration data, never public index configuration.
    /// The direct observation is a fallback for callers without a live probe.
    capacity_domain_id: u128 = 0,
    capacity_observation: RepairCapacityObservation = .{},
    capacity_source: ?RepairCapacitySource = null,
    /// Installed by the durable owner after admission. Shadow construction
    /// invokes it only at bounded publication/window boundaries.
    capacity_check: ?RepairCapacityCheck = null,
    /// Internal recursion fence: the durable owner has already admitted and
    /// claimed this intent and is invoking the lower-level rebuild engine.
    executing_durable_index_repair: bool = false,
    /// Managed operator requests persist/enqueue intent work and return
    /// immediately. Standalone callers leave this false and advance through
    /// the same state machine synchronously.
    defer_durable_index_repair_execution: bool = false,

    pub fn cancelled(self: ArtifactRepairRunOptions) bool {
        if (self.cancel_check) |check| return check.requested();
        return false;
    }
};

pub const ArtifactRepairResult = struct {
    scanned: u64 = 0,
    groups_scanned: u64 = 0,
    reprocessed: u64 = 0,
    repaired: u64 = 0,
    missing_source_docs: u64 = 0,
    failed: u64 = 0,
    unsupported: u64 = 0,
    unresolved: u64 = 0,
    in_progress: u64 = 0,
    indexes_rebuilt: u64 = 0,
    /// Selected indexes that were degraded when this repair pass began.
    indexes_degraded_before: u64 = 0,
    /// Selected indexes that remain degraded when this repair pass returns.
    indexes_degraded_after: u64 = 0,
    controls_applied: u64 = 0,
    limit: u32 = 0,
    next_cursor: ?[]u8 = null,
    has_more: bool = false,
    debt_remaining: bool = false,

    pub fn deinit(self: *ArtifactRepairResult, alloc: Allocator) void {
        if (self.next_cursor) |value| alloc.free(value);
        self.* = undefined;
    }
};

pub const EmbeddingArtifactRepairReason = enum {
    missing_embedding_artifact,
    corrupt_embedding_artifact,
};

pub const EmbeddingArtifactRepairIssue = struct {
    artifact_kind: ArtifactRepairKind = .embedding,
    index_name: []const u8 = "",
    doc_key: []const u8 = "",
    parent_doc_key: []const u8 = "",
    unit_id: []const u8 = "",
    source_artifact_name: []const u8 = "",
    artifact_name: []const u8 = "",
    artifact_key: []const u8 = "",
    chunk_id: ?u32 = null,
    repairable: bool = true,
    unsupported_reason: []const u8 = "",
    sequence: u64 = 0,
    reason: EmbeddingArtifactRepairReason = .missing_embedding_artifact,
    generation_attempts: u64 = 0,
    generation_error: []const u8 = "",
    attempts: u64 = 0,
    first_seen_ns: u64 = 0,
    last_seen_ns: u64 = 0,
    last_error: []const u8 = "",

    pub fn deinit(self: *EmbeddingArtifactRepairIssue, alloc: Allocator) void {
        if (self.index_name.len > 0) alloc.free(@constCast(self.index_name));
        if (self.doc_key.len > 0) alloc.free(@constCast(self.doc_key));
        if (self.parent_doc_key.len > 0) alloc.free(@constCast(self.parent_doc_key));
        if (self.unit_id.len > 0) alloc.free(@constCast(self.unit_id));
        if (self.source_artifact_name.len > 0) alloc.free(@constCast(self.source_artifact_name));
        if (self.artifact_name.len > 0) alloc.free(@constCast(self.artifact_name));
        if (self.artifact_key.len > 0) alloc.free(@constCast(self.artifact_key));
        if (self.unsupported_reason.len > 0) alloc.free(@constCast(self.unsupported_reason));
        if (self.generation_error.len > 0) alloc.free(@constCast(self.generation_error));
        if (self.last_error.len > 0) alloc.free(@constCast(self.last_error));
        self.* = undefined;
    }
};
pub const EmbeddingArtifactRepairResult = ArtifactRepairResult;

pub fn embeddingArtifactRepairReasonFromArtifact(reason: ArtifactRepairReason) EmbeddingArtifactRepairReason {
    return switch (reason) {
        .missing_artifact => .missing_embedding_artifact,
        .corrupt_artifact, .unreadable_artifact, .enrichment_failed, .resource_limit_exceeded => .corrupt_embedding_artifact,
    };
}

pub fn embeddingArtifactRepairIssueFromArtifactAlloc(alloc: Allocator, issue: ArtifactRepairIssue) !EmbeddingArtifactRepairIssue {
    var out = EmbeddingArtifactRepairIssue{
        .artifact_kind = issue.artifact_kind,
        .chunk_id = issue.chunk_id,
        .repairable = issue.repairable,
        .sequence = issue.sequence,
        .reason = embeddingArtifactRepairReasonFromArtifact(issue.reason),
        .generation_attempts = issue.generation_attempts,
        .attempts = issue.attempts,
        .first_seen_ns = issue.first_seen_ns,
        .last_seen_ns = issue.last_seen_ns,
    };
    errdefer out.deinit(alloc);
    out.index_name = try alloc.dupe(u8, issue.index_name);
    out.doc_key = try alloc.dupe(u8, issue.doc_key);
    out.parent_doc_key = try alloc.dupe(u8, issue.parent_doc_key);
    out.unit_id = try alloc.dupe(u8, issue.unit_id);
    out.source_artifact_name = try alloc.dupe(u8, issue.source_artifact_name);
    out.artifact_name = try alloc.dupe(u8, issue.artifact_name);
    out.artifact_key = try alloc.dupe(u8, issue.artifact_key);
    out.unsupported_reason = try alloc.dupe(u8, issue.unsupported_reason);
    out.generation_error = try alloc.dupe(u8, issue.generation_error);
    out.last_error = try alloc.dupe(u8, issue.last_error);
    return out;
}

pub fn freeEmbeddingArtifactRepairIssues(alloc: Allocator, issues: []EmbeddingArtifactRepairIssue) void {
    for (issues) |*issue| issue.deinit(alloc);
    if (issues.len > 0) alloc.free(issues);
}

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

/// Source-specific replay watermarks for an artifact-backed index. The
/// published watermark is the index's durable applied cursor: it proves that
/// every configured source has been processed through that revision. The
/// target is maintained transactionally per artifact stream by the writer.
pub const IndexSourceReplayStatus = struct {
    artifact_name: []const u8,
    published_sequence: u64 = 0,
    target_sequence: u64 = 0,
    // Internal distributed-status proof; not part of the public contract.
    observation_count: u64 = 1,
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
    coverage_produced_count: u64 = 0,
    coverage_skipped_count: u64 = 0,
    coverage_terminal_failed_count: u64 = 0,
    // Stable across shard-local marker generations for the same stored config.
    coverage_config_hash: u64 = 0,
    coverage_summary_ready: bool = true,
    // Internal identity used while collecting stats. These fields are not part
    // of the public status contract.
    coverage_generation: u64 = 0,
    coverage_identity_ready: bool = false,
    backfill_active: bool = false,
    backfill_progress: f64 = 0.0,
    enrichment_failed: bool = false,
    repair_degraded: bool = false,
    repair_issue_count: u64 = 0,
    repair_summary_ready: bool = true,
    repair_issue_count_estimated: bool = false,
    repair_scan_issue_count: u64 = 0,
    index_repair_id: ?u128 = null,
    index_repair_trigger: []const u8 = "none",
    index_repair_phase: []const u8 = "none",
    index_repair_automation: []const u8 = "none",
    index_repair_attempts: u32 = 0,
    index_repair_started_at_ms: u64 = 0,
    index_repair_updated_at_ms: u64 = 0,
    index_repair_build_floor_sequence: u64 = 0,
    index_repair_applied_sequence: u64 = 0,
    index_repair_target_sequence: u64 = 0,
    index_repair_next_retry_at_ms: u64 = 0,
    index_repair_last_error: ?[]const u8 = null,
    index_repair_wait_reason: []const u8 = "none",
    // Compact lifecycle used when DBIndexStats crosses process boundaries.
    // The full local durable diagnostics remain authoritative when present.
    index_repair_status: ?IndexRepairStatus = null,
    // Internal proof that the active managed-admission generation is safe to
    // query. Under progressive publication it may still have incomplete source
    // coverage; repair intent remains authoritative until full convergence.
    index_repair_active_generation_serviceable: bool = false,
    projection_checkpoint_status: []const u8 = "clean",
    projection_checkpoint_applied_sequence: u64 = 0,
    projection_checkpoint_generation: u64 = 0,
    projection_checkpoint_config_hash: u64 = 0,
    replay_applied_sequence: u64 = 0,
    replay_target_sequence: u64 = 0,
    source_replay: []IndexSourceReplayStatus = &.{},
    checkpoint_replay_tail_sequence_count: u64 = 0,
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

/// Read-only physical layout of a full-text index snapshot. This is an
/// internal diagnostics/benchmark surface: callers must not use segment IDs or
/// positions as durable document identity.
pub const TextSegmentLayoutStats = struct {
    segment_id: u64,
    doc_count: u32,
    live_doc_count: u32,
    deleted_count: u32,
    bytes: u64,
    file_backed: bool,
};

pub const TextMergePolicyStats = struct {
    max_segments_per_tier: u32,
    max_merge_at_once: u32,
    max_segment_size: u64,
    floor_segment_size: u64,
    skew_weight: f64,
    size_weight: f64,
    delete_reclaim_weight: f64,
};

pub const TextIndexLayoutStats = struct {
    global_doc_count: u32,
    total_bytes: u64,
    segments: []TextSegmentLayoutStats,
    merge_policy: TextMergePolicyStats,
    merge_stats: TextMergeStats,

    pub fn deinit(self: *TextIndexLayoutStats, alloc: Allocator) void {
        if (self.segments.len > 0) alloc.free(self.segments);
        self.* = undefined;
    }
};

pub const TextKernelHit = struct {
    /// Stable, one-based native document ordinal. Benchmark adapters may
    /// normalize this to their declared external ordinal base.
    doc_ordinal: u32,
    score: f32,
};

pub const TextBM25Config = struct {
    k1: f32 = 1.2,
    b: f32 = 0.75,
};

pub const TextKernelSearchOptions = struct {
    limit: u32 = 10,
    bm25: TextBM25Config = .{},
    collect_diagnostics: bool = false,
};

pub const TextKernelDiagnostics = struct {
    segments_considered: u64 = 0,
    segments_searched: u64 = 0,
    segments_pruned: u64 = 0,
    postings_iterators_opened: u64 = 0,
    wand_next_in_score: u64 = 0,
    wand_next_in_advance: u64 = 0,
    wand_pivots_scored: u64 = 0,
    wand_pivots_advanced: u64 = 0,
    wand_chunks_skipped: u64 = 0,
    boolean_candidates_scored: u64 = 0,
    boolean_chunks_skipped: u64 = 0,
    phrase_candidates_verified: u64 = 0,
    phrase_position_records_decoded: u64 = 0,
    phrase_matches_scored: u64 = 0,
};

pub const TextKernelResult = struct {
    hits: []TextKernelHit,
    total_hits: u32,
    total_hits_relation: TotalHitsRelation,
    diagnostics: TextKernelDiagnostics = .{},

    pub fn deinit(self: *TextKernelResult, alloc: Allocator) void {
        if (self.hits.len > 0) alloc.free(self.hits);
        self.* = undefined;
    }
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
    hits: u64 = 0,
    misses: u64 = 0,
    insertions: u64 = 0,
    replacements: u64 = 0,
    sampled_admissions: u64 = 0,
    admission_skips: u64 = 0,
    evictions: u64 = 0,
};

pub const HbcCacheStats = struct {
    total_bytes: u64 = 0,
    accounted_bytes: u64 = 0,
    pinned_bytes: u64 = 0,
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
    lsm_open_recovered_temp_cleanup_ns: u64 = 0,
    lsm_open_manifest_ns: u64 = 0,
    lsm_open_ensuring_dirs_ns: u64 = 0,
    lsm_open_wal_replay_ns: u64 = 0,
    lsm_open_mounting_runs_ns: u64 = 0,
    lsm_open_loaded_runs: u64 = 0,
    lsm_open_obsolete_paths: u64 = 0,
    lsm_open_mutable_entries_after_replay: u64 = 0,
    lsm_open_immutable_memtables_after_replay: u64 = 0,
    lsm_open_recovered_temp_files_deleted: u64 = 0,
    lsm_open_recovered_temp_bytes_deleted: u64 = 0,
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
    derived_workers: DerivedWorkerStats = .{},
};

pub const DerivedWorkerStats = struct {
    workers: u64 = 0,
    workers_with_replay_debt: u64 = 0,
    max_replay_lag_sequences: u64 = 0,
    recoverable_retries: u64 = 0,
    writer_locked_retries: u64 = 0,
    resource_budget_retries: u64 = 0,
    replay_document_not_visible_retries: u64 = 0,
    artifact_repair_required_retries: u64 = 0,
    not_found_retries: u64 = 0,
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
    dst.lsm_open_recovered_temp_cleanup_ns += src.lsm_open_recovered_temp_cleanup_ns;
    dst.lsm_open_manifest_ns += src.lsm_open_manifest_ns;
    dst.lsm_open_ensuring_dirs_ns += src.lsm_open_ensuring_dirs_ns;
    dst.lsm_open_wal_replay_ns += src.lsm_open_wal_replay_ns;
    dst.lsm_open_mounting_runs_ns += src.lsm_open_mounting_runs_ns;
    dst.lsm_open_loaded_runs += src.lsm_open_loaded_runs;
    dst.lsm_open_obsolete_paths += src.lsm_open_obsolete_paths;
    dst.lsm_open_mutable_entries_after_replay += src.lsm_open_mutable_entries_after_replay;
    dst.lsm_open_immutable_memtables_after_replay += src.lsm_open_immutable_memtables_after_replay;
    dst.lsm_open_recovered_temp_files_deleted += src.lsm_open_recovered_temp_files_deleted;
    dst.lsm_open_recovered_temp_bytes_deleted += src.lsm_open_recovered_temp_bytes_deleted;
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
    dst.derived_workers.workers += src.derived_workers.workers;
    dst.derived_workers.workers_with_replay_debt += src.derived_workers.workers_with_replay_debt;
    dst.derived_workers.max_replay_lag_sequences = @max(dst.derived_workers.max_replay_lag_sequences, src.derived_workers.max_replay_lag_sequences);
    dst.derived_workers.recoverable_retries += src.derived_workers.recoverable_retries;
    dst.derived_workers.writer_locked_retries += src.derived_workers.writer_locked_retries;
    dst.derived_workers.resource_budget_retries += src.derived_workers.resource_budget_retries;
    dst.derived_workers.replay_document_not_visible_retries += src.derived_workers.replay_document_not_visible_retries;
    dst.derived_workers.artifact_repair_required_retries += src.derived_workers.artifact_repair_required_retries;
    dst.derived_workers.not_found_retries += src.derived_workers.not_found_retries;
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
        for (item.source_replay) |source| alloc.free(source.artifact_name);
        if (item.source_replay.len > 0) alloc.free(item.source_replay);
        if (item.load_error) |value| alloc.free(value);
        if (item.index_repair_last_error) |value| alloc.free(value);
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
