// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the Elastic License 2.0 is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
// the Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Bounded materialization of immutable graph metric vectors. This code is
//! synchronous by design: callers schedule it through their existing std.Io or
//! backend runtime, while the kernel remains deterministic and easy to shard.

const std = @import("std");
const Allocator = std.mem.Allocator;
const CancellationToken = @import("../../common/cancellation.zig").CancellationToken;
const graph_mod = @import("../../graph/graph.zig");
const metrics = @import("../../graph/metrics.zig");
const artifact_ref = @import("../manifest/artifact_ref.zig");
const artifact_store = @import("../artifacts/store.zig");
const fs_artifact_store = @import("../artifacts/fs_store.zig");
const graph_segment = @import("../graph_segment/mod.zig");
const metric_segment = @import("../graph_metric_segment/mod.zig");
const graph_metric_policy = @import("graph_metric_policy.zig");

pub const Limits = graph_metric_policy.Limits;

pub const Provenance = struct {
    published_generation: u64 = 0,
    edge_generation: u64 = 0,
    computed_at_ms: u64 = 0,

    pub fn validate(self: Provenance) !void {
        if (self.published_generation == 0 or self.edge_generation == 0 or self.computed_at_ms == 0 or
            self.edge_generation > self.published_generation)
        {
            return error.InvalidGraphMetricProvenance;
        }
    }
};

pub const BuildOptions = struct {
    graph_index_name: []const u8,
    config: graph_mod.GraphMetricConfig,
    source_graph: artifact_ref.ArtifactRef,
    cancellation: CancellationToken = .none,
    limits: Limits = .{},
    batch_budget: ?*graph_metric_policy.Budget = null,
    provenance: Provenance = .{},
};

pub const BuildResult = struct {
    payload: []u8,
    artifact: artifact_ref.ArtifactRef,

    pub fn deinit(self: *BuildResult, alloc: Allocator) void {
        alloc.free(self.payload);
        freeArtifactRef(alloc, self.artifact);
        self.* = undefined;
    }
};

/// One authenticated and decoded topology artifact. Publication orchestrators
/// may reuse this across graph-index aliases that identify the same immutable
/// payload, avoiding repeated object-store reads and decode allocations while
/// retaining an exact provenance check at every use.
pub const PreparedGraphArtifact = struct {
    source_artifact_id: []u8,
    source_checksum: []u8,
    source_byte_len: u64,
    graph: graph_segment.Segment,

    pub fn deinit(self: *PreparedGraphArtifact, alloc: Allocator) void {
        alloc.free(self.source_artifact_id);
        alloc.free(self.source_checksum);
        self.graph.deinit(alloc);
        self.* = undefined;
    }

    pub fn identifies(self: PreparedGraphArtifact, source_graph: artifact_ref.ArtifactRef) bool {
        return source_graph.kind == .graph_segment and
            source_graph.byte_len == self.source_byte_len and
            std.mem.eql(u8, source_graph.artifact_id, self.source_artifact_id) and
            std.mem.eql(u8, source_graph.checksum, self.source_checksum);
    }
};

pub fn artifactNameAlloc(alloc: Allocator, graph_index_name: []const u8, metric_name: []const u8) ![]u8 {
    return metric_segment.artifactNameAlloc(alloc, graph_index_name, metric_name) catch return error.InvalidGraphMetricBuildOptions;
}

pub fn buildFromGraphPayloadAlloc(alloc: Allocator, graph_payload: []const u8, options: BuildOptions) !BuildResult {
    try validateOptions(graph_payload, options);
    var graph = graph_segment.decodeAllocWithCancellation(alloc, graph_payload, options.cancellation) catch |err| switch (err) {
        error.DecodedArtifactTooLarge => return error.GraphMetricBuildBudgetExceeded,
        else => return err,
    };
    defer graph.deinit(alloc);
    return try buildFromSegmentAlloc(alloc, graph, options);
}

pub fn publishFromGraphPayloadAlloc(alloc: Allocator, artifacts: *artifact_store.ArtifactStore, graph_payload: []const u8, options: BuildOptions) !artifact_ref.ArtifactRef {
    try options.provenance.validate();
    var built = try buildFromGraphPayloadAlloc(alloc, graph_payload, options);
    defer built.deinit(alloc);
    var metadata = try artifacts.putWithCancellation(built.payload, options.cancellation);
    defer metadata.deinit(alloc);
    const name = try alloc.dupe(u8, built.artifact.name);
    errdefer alloc.free(name);
    const artifact_id = try alloc.dupe(u8, metadata.artifact_id);
    errdefer alloc.free(artifact_id);
    const checksum = try alloc.dupe(u8, metadata.checksum);
    return .{
        .kind = .graph_metric_segment,
        .name = name,
        .artifact_id = artifact_id,
        .byte_len = metadata.byte_len,
        .checksum = checksum,
        .metadata_version = built.artifact.metadata_version,
        .published_generation = built.artifact.published_generation,
        .edge_generation = built.artifact.edge_generation,
        .computed_at_ms = built.artifact.computed_at_ms,
        .materializer_fingerprint = built.artifact.materializer_fingerprint,
        .graph_metric_control_len = built.artifact.graph_metric_control_len,
        .graph_metric_routing_footer_len = built.artifact.graph_metric_routing_footer_len,
        .graph_metric_control_checksum = built.artifact.graph_metric_control_checksum,
        .graph_metric_routing_checksum = built.artifact.graph_metric_routing_checksum,
        .graph_metric_config_fingerprint = built.artifact.graph_metric_config_fingerprint,
        .graph_metric_source_checksum = built.artifact.graph_metric_source_checksum,
        .graph_metric_materialization_state = built.artifact.graph_metric_materialization_state,
        .graph_metric_rejection_reason = built.artifact.graph_metric_rejection_reason,
    };
}

/// Builds a metric only from bytes verified against the immutable graph
/// artifact reference. Production callers should prefer this entry point so a
/// caller cannot accidentally publish a metric whose provenance names bytes
/// other than those that were actually evaluated.
pub fn publishFromGraphArtifactAlloc(
    alloc: Allocator,
    artifacts: *artifact_store.ArtifactStore,
    options: BuildOptions,
) !artifact_ref.ArtifactRef {
    if (options.source_graph.byte_len > options.limits.max_graph_payload_bytes) return error.GraphMetricBuildBudgetExceeded;
    const graph_payload = try artifacts.getVerifiedAllocWithCancellationUsingAllocator(
        alloc,
        options.source_graph.artifact_id,
        options.source_graph.byte_len,
        options.source_graph.checksum,
        options.cancellation,
    );
    defer alloc.free(graph_payload);
    return try publishFromGraphPayloadAlloc(alloc, artifacts, graph_payload, options);
}

/// Publishes several metric configurations from one verified fetch and one
/// graph decode. This is the normal lifecycle entry point for a graph index;
/// its peak graph memory is constant and its graph verification/decoding cost
/// does not grow with the number of configured metrics.
pub fn publishManyFromGraphArtifactAlloc(
    alloc: Allocator,
    artifacts: *artifact_store.ArtifactStore,
    graph_index_name: []const u8,
    source_graph: artifact_ref.ArtifactRef,
    configs: []const graph_mod.GraphMetricConfig,
    cancellation: CancellationToken,
    limits: Limits,
    provenance: Provenance,
) ![]artifact_ref.ArtifactRef {
    var batch_budget = graph_metric_policy.Budget{ .limits = limits };
    return publishManyFromGraphArtifactWithBudgetAlloc(
        alloc,
        artifacts,
        graph_index_name,
        source_graph,
        configs,
        cancellation,
        limits,
        &batch_budget,
        provenance,
    );
}

/// Variant used by table publication so work and output admission span every
/// graph index rebuilt in the same publication, rather than resetting for each
/// index.
pub fn publishManyFromGraphArtifactWithBudgetAlloc(
    alloc: Allocator,
    artifacts: *artifact_store.ArtifactStore,
    graph_index_name: []const u8,
    source_graph: artifact_ref.ArtifactRef,
    configs: []const graph_mod.GraphMetricConfig,
    cancellation: CancellationToken,
    limits: Limits,
    batch_budget: *graph_metric_policy.Budget,
    provenance: Provenance,
) ![]artifact_ref.ArtifactRef {
    if (configs.len == 0) return try alloc.alloc(artifact_ref.ArtifactRef, 0);
    try validatePublicationOptions(graph_index_name, source_graph, configs, cancellation, limits, batch_budget);
    try batch_budget.chargeGraphPayload(source_graph.artifact_id, source_graph.checksum, source_graph.byte_len);
    var prepared = try prepareGraphArtifactAlloc(alloc, artifacts, source_graph, cancellation, limits);
    defer prepared.deinit(alloc);
    return try publishManyFromPreparedGraphWithBudgetAlloc(
        alloc,
        artifacts,
        graph_index_name,
        source_graph,
        configs,
        cancellation,
        limits,
        batch_budget,
        &prepared,
        provenance,
    );
}

pub fn prepareGraphArtifactAlloc(
    alloc: Allocator,
    artifacts: *artifact_store.ArtifactStore,
    source_graph: artifact_ref.ArtifactRef,
    cancellation: CancellationToken,
    limits: Limits,
) !PreparedGraphArtifact {
    try cancellation.check();
    try graph_metric_policy.validateLimits(limits);
    if (source_graph.kind != .graph_segment or source_graph.byte_len == 0) return error.InvalidGraphMetricBuildOptions;
    if (source_graph.byte_len > limits.max_graph_payload_bytes) return error.GraphMetricBuildBudgetExceeded;
    const graph_payload = try artifacts.getVerifiedAllocWithCancellationUsingAllocator(
        alloc,
        source_graph.artifact_id,
        source_graph.byte_len,
        source_graph.checksum,
        cancellation,
    );
    defer alloc.free(graph_payload);
    var graph = graph_segment.decodeAllocWithCancellation(alloc, graph_payload, cancellation) catch |err| switch (err) {
        error.DecodedArtifactTooLarge => return error.GraphMetricBuildBudgetExceeded,
        else => return err,
    };
    errdefer graph.deinit(alloc);
    const source_artifact_id = try alloc.dupe(u8, source_graph.artifact_id);
    errdefer alloc.free(source_artifact_id);
    const source_checksum = try alloc.dupe(u8, source_graph.checksum);
    return .{
        .source_artifact_id = source_artifact_id,
        .source_checksum = source_checksum,
        .source_byte_len = source_graph.byte_len,
        .graph = graph,
    };
}

pub fn publishManyFromPreparedGraphWithBudgetAlloc(
    alloc: Allocator,
    artifacts: *artifact_store.ArtifactStore,
    graph_index_name: []const u8,
    source_graph: artifact_ref.ArtifactRef,
    configs: []const graph_mod.GraphMetricConfig,
    cancellation: CancellationToken,
    limits: Limits,
    batch_budget: *graph_metric_policy.Budget,
    prepared: *const PreparedGraphArtifact,
    provenance: Provenance,
) ![]artifact_ref.ArtifactRef {
    if (configs.len == 0) return try alloc.alloc(artifact_ref.ArtifactRef, 0);
    try provenance.validate();
    try validatePublicationOptions(graph_index_name, source_graph, configs, cancellation, limits, batch_budget);
    if (!prepared.identifies(source_graph)) return error.ArtifactIntegrityMismatch;

    const graph = prepared.graph;

    const refs = try alloc.alloc(artifact_ref.ArtifactRef, configs.len);
    const initialized = try alloc.alloc(bool, configs.len);
    defer alloc.free(initialized);
    @memset(initialized, false);
    const processed = try alloc.alloc(bool, configs.len);
    defer alloc.free(processed);
    @memset(processed, false);
    errdefer {
        for (refs, initialized) |ref, ready| if (ready) freeArtifactRef(alloc, ref);
        alloc.free(refs);
    }
    for (configs, 0..) |config, i| {
        if (processed[i]) continue;
        try cancellation.check();
        if (findCompatibleHitsPairIndex(configs, i)) |pair_index| {
            var built_pair = buildHitsPairFromSegmentAlloc(alloc, graph, .{
                .graph_index_name = graph_index_name,
                .config = config,
                .source_graph = source_graph,
                .cancellation = cancellation,
                .limits = limits,
                .batch_budget = batch_budget,
                .provenance = provenance,
            }, configs[pair_index]) catch |err| switch (err) {
                error.GraphMetricBuildBudgetExceeded => {
                    refs[i] = try publishRejectedAlloc(alloc, artifacts, graph_index_name, source_graph, config, cancellation, .build_budget_exceeded, limits, provenance);
                    initialized[i] = true;
                    refs[pair_index] = try publishRejectedAlloc(alloc, artifacts, graph_index_name, source_graph, configs[pair_index], cancellation, .build_budget_exceeded, limits, provenance);
                    initialized[pair_index] = true;
                    processed[i] = true;
                    processed[pair_index] = true;
                    continue;
                },
                else => return err,
            };
            defer built_pair.deinit(alloc);
            refs[i] = try putBuildResultAlloc(alloc, artifacts, &built_pair.first, cancellation);
            initialized[i] = true;
            refs[pair_index] = try putBuildResultAlloc(alloc, artifacts, &built_pair.second, cancellation);
            initialized[pair_index] = true;
            processed[i] = true;
            processed[pair_index] = true;
            continue;
        }
        var built = buildFromSegmentAlloc(alloc, graph, .{
            .graph_index_name = graph_index_name,
            .config = config,
            .source_graph = source_graph,
            .cancellation = cancellation,
            .limits = limits,
            .batch_budget = batch_budget,
            .provenance = provenance,
        }) catch |err| switch (err) {
            error.GraphMetricBuildBudgetExceeded => {
                refs[i] = try publishRejectedAlloc(alloc, artifacts, graph_index_name, source_graph, config, cancellation, .build_budget_exceeded, limits, provenance);
                initialized[i] = true;
                processed[i] = true;
                continue;
            },
            else => return err,
        };
        defer built.deinit(alloc);
        refs[i] = try putBuildResultAlloc(alloc, artifacts, &built, cancellation);
        initialized[i] = true;
        processed[i] = true;
    }
    return refs;
}

fn validatePublicationOptions(
    graph_index_name: []const u8,
    source_graph: artifact_ref.ArtifactRef,
    configs: []const graph_mod.GraphMetricConfig,
    cancellation: CancellationToken,
    limits: Limits,
    batch_budget: *graph_metric_policy.Budget,
) !void {
    try cancellation.check();
    try graph_metric_policy.validateConfigs(configs, limits);
    if (!std.meta.eql(batch_budget.limits, limits)) return error.InvalidGraphMetricBuildOptions;
    if (graph_index_name.len == 0 or source_graph.kind != .graph_segment or source_graph.byte_len == 0) return error.InvalidGraphMetricBuildOptions;
    if (source_graph.byte_len > limits.max_graph_payload_bytes) return error.GraphMetricBuildBudgetExceeded;
    try graph_mod.validateGraphMetricEdgeFilters(&.{}, configs);
}

fn putBuildResultAlloc(alloc: Allocator, artifacts: *artifact_store.ArtifactStore, built: *const BuildResult, cancellation: CancellationToken) !artifact_ref.ArtifactRef {
    var metadata = try artifacts.putWithCancellation(built.payload, cancellation);
    defer metadata.deinit(alloc);
    const name = try alloc.dupe(u8, built.artifact.name);
    errdefer alloc.free(name);
    const artifact_id = try alloc.dupe(u8, metadata.artifact_id);
    errdefer alloc.free(artifact_id);
    const ref = artifact_ref.ArtifactRef{
        .kind = .graph_metric_segment,
        .name = name,
        .artifact_id = artifact_id,
        .byte_len = metadata.byte_len,
        .checksum = try alloc.dupe(u8, metadata.checksum),
        .metadata_version = built.artifact.metadata_version,
        .published_generation = built.artifact.published_generation,
        .edge_generation = built.artifact.edge_generation,
        .computed_at_ms = built.artifact.computed_at_ms,
        .materializer_fingerprint = built.artifact.materializer_fingerprint,
        .graph_metric_control_len = built.artifact.graph_metric_control_len,
        .graph_metric_routing_footer_len = built.artifact.graph_metric_routing_footer_len,
        .graph_metric_control_checksum = built.artifact.graph_metric_control_checksum,
        .graph_metric_routing_checksum = built.artifact.graph_metric_routing_checksum,
        .graph_metric_config_fingerprint = built.artifact.graph_metric_config_fingerprint,
        .graph_metric_source_checksum = built.artifact.graph_metric_source_checksum,
        .graph_metric_materialization_state = built.artifact.graph_metric_materialization_state,
        .graph_metric_rejection_reason = built.artifact.graph_metric_rejection_reason,
    };
    return ref;
}

fn findCompatibleHitsPairIndex(configs: []const graph_mod.GraphMetricConfig, index: usize) ?usize {
    if (graph_mod.graphMetricOppositeHitsKind(configs[index].kind) == null) return null;
    for (configs, 0..) |candidate, candidate_index| {
        if (candidate_index == index or candidate_index < index) continue;
        if (graph_mod.graphMetricHitsPairCompatible(configs[index], candidate)) return candidate_index;
    }
    return null;
}

/// Publishes durable terminal status sidecars when the source graph itself is
/// outside the materializer's admission budget. These sidecars preserve base
/// publication, prevent retry hot loops, and let queries return an actionable
/// rejection instead of pretending the configured metric does not exist.
pub fn publishRejectedManyAlloc(
    alloc: Allocator,
    artifacts: *artifact_store.ArtifactStore,
    graph_index_name: []const u8,
    source_graph: artifact_ref.ArtifactRef,
    configs: []const graph_mod.GraphMetricConfig,
    cancellation: CancellationToken,
    reason: metric_segment.RejectionReason,
    limits: Limits,
    provenance: Provenance,
) ![]artifact_ref.ArtifactRef {
    try provenance.validate();
    try graph_metric_policy.validateConfigs(configs, limits);
    const refs = try alloc.alloc(artifact_ref.ArtifactRef, configs.len);
    var initialized: usize = 0;
    errdefer {
        for (refs[0..initialized]) |ref| freeArtifactRef(alloc, ref);
        alloc.free(refs);
    }
    for (configs, 0..) |config, i| {
        try cancellation.check();
        refs[i] = try publishRejectedAlloc(alloc, artifacts, graph_index_name, source_graph, config, cancellation, reason, limits, provenance);
        initialized += 1;
    }
    return refs;
}

fn publishRejectedAlloc(
    alloc: Allocator,
    artifacts: *artifact_store.ArtifactStore,
    graph_index_name: []const u8,
    source_graph: artifact_ref.ArtifactRef,
    config: graph_mod.GraphMetricConfig,
    cancellation: CancellationToken,
    reason: metric_segment.RejectionReason,
    limits: Limits,
    provenance: Provenance,
) !artifact_ref.ArtifactRef {
    try cancellation.check();
    var segment = try rejectedSegmentAlloc(alloc, graph_index_name, source_graph, config, reason, limits, provenance);
    defer segment.deinit(alloc);
    const payload = try metric_segment.encodeAllocWithCancellation(alloc, segment, cancellation);
    defer alloc.free(payload);
    var metadata = try artifacts.putWithCancellation(payload, cancellation);
    defer metadata.deinit(alloc);
    const name = try metric_segment.artifactNameAlloc(alloc, graph_index_name, config.name);
    errdefer alloc.free(name);
    const artifact_id = try alloc.dupe(u8, metadata.artifact_id);
    errdefer alloc.free(artifact_id);
    const checksum = try alloc.dupe(u8, metadata.checksum);
    var ref = artifact_ref.ArtifactRef{
        .kind = .graph_metric_segment,
        .name = name,
        .artifact_id = artifact_id,
        .byte_len = metadata.byte_len,
        .checksum = checksum,
        .metadata_version = metric_segment.wire_version,
        .published_generation = provenance.published_generation,
        .edge_generation = provenance.edge_generation,
        .computed_at_ms = provenance.computed_at_ms,
        .materializer_fingerprint = segment.materializer_fingerprint,
    };
    try populateGraphMetricIntegrity(&ref, segment, payload);
    return ref;
}

fn rejectedSegmentAlloc(
    alloc: Allocator,
    graph_index_name: []const u8,
    source_graph: artifact_ref.ArtifactRef,
    config: graph_mod.GraphMetricConfig,
    reason: metric_segment.RejectionReason,
    limits: Limits,
    provenance: Provenance,
) !metric_segment.Segment {
    const owned_graph_index_name = try alloc.dupe(u8, graph_index_name);
    errdefer alloc.free(owned_graph_index_name);
    const owned_metric_name = try alloc.dupe(u8, config.name);
    errdefer alloc.free(owned_metric_name);
    const owned_source_graph_artifact_id = try alloc.dupe(u8, source_graph.artifact_id);
    errdefer alloc.free(owned_source_graph_artifact_id);
    const owned_source_graph_checksum = try alloc.dupe(u8, source_graph.checksum);
    errdefer alloc.free(owned_source_graph_checksum);
    var edge_filter = try cloneSortedEdgeFilterAlloc(alloc, config.edge_filter);
    errdefer edge_filter.deinit(alloc);
    const scores = try alloc.alloc(metric_segment.Score, 0);
    errdefer alloc.free(scores);
    const segment = metric_segment.Segment{
        .graph_index_name = owned_graph_index_name,
        .metric_name = owned_metric_name,
        .kind = config.kind,
        .source_graph_artifact_id = owned_source_graph_artifact_id,
        .source_graph_checksum = owned_source_graph_checksum,
        .config_fingerprint = configFingerprint(config),
        .materializer_fingerprint = graph_metric_policy.materializerFingerprint(limits),
        .published_generation = provenance.published_generation,
        .edge_generation = provenance.edge_generation,
        .computed_at_ms = provenance.computed_at_ms,
        .materialization_state = .rejected,
        .rejection_reason = reason,
        .edge_filter = edge_filter,
        .converged = false,
        .iterations_completed = 0,
        .delta = 0,
        .scores = scores,
    };
    return segment;
}

const Projection = struct {
    ordinals: std.StringHashMapUnmanaged(usize) = .empty,
    node_ids: std.ArrayListUnmanaged([]const u8) = .empty,
    edges: std.ArrayListUnmanaged(metrics.Edge) = .empty,
    source_node_count: usize = 0,
    source_edge_count: usize = 0,

    fn deinit(self: *Projection, alloc: Allocator) void {
        self.ordinals.deinit(alloc);
        self.node_ids.deinit(alloc);
        self.edges.deinit(alloc);
        self.* = .{};
    }
};

const EdgeFilterIndex = struct {
    all: bool,
    types: std.StringHashMapUnmanaged(void) = .empty,

    fn init(alloc: Allocator, filter: graph_mod.GraphMetricEdgeFilter) !EdgeFilterIndex {
        var self = EdgeFilterIndex{ .all = filter.mode == .all };
        errdefer self.deinit(alloc);
        if (!self.all) {
            const capacity = std.math.cast(std.StringHashMapUnmanaged(void).Size, filter.types.len) orelse
                return error.GraphMetricConfigurationLimitExceeded;
            try self.types.ensureTotalCapacity(alloc, capacity);
            for (filter.types) |edge_type| self.types.putAssumeCapacity(edge_type, {});
        }
        return self;
    }

    fn deinit(self: *EdgeFilterIndex, alloc: Allocator) void {
        self.types.deinit(alloc);
        self.* = undefined;
    }

    fn allows(self: EdgeFilterIndex, edge_type: []const u8) bool {
        return self.all or self.types.contains(edge_type);
    }
};

const BuildPairResult = struct {
    first: BuildResult,
    second: BuildResult,

    fn deinit(self: *BuildPairResult, alloc: Allocator) void {
        self.first.deinit(alloc);
        self.second.deinit(alloc);
        self.* = undefined;
    }
};

fn buildProjectionAlloc(alloc: Allocator, graph: graph_segment.Segment, options: BuildOptions) !Projection {
    try options.cancellation.check();
    if (graph.adjacencies.len > options.limits.max_nodes) return error.GraphMetricBuildBudgetExceeded;
    var projection = Projection{};
    errdefer projection.deinit(alloc);
    projection.source_node_count = graph.adjacencies.len;
    var filter = try EdgeFilterIndex.init(alloc, options.config.edge_filter);
    defer filter.deinit(alloc);

    for (graph.adjacencies, 0..) |adjacency, adjacency_index| {
        if (adjacency_index % 256 == 0) try options.cancellation.check();
        for (adjacency.out_edges) |edge| {
            projection.source_edge_count = std.math.add(usize, projection.source_edge_count, 1) catch
                return error.GraphMetricBuildBudgetExceeded;
            if (projection.source_edge_count > options.limits.max_edges) return error.GraphMetricBuildBudgetExceeded;
            if (projection.source_edge_count % 4096 == 0) try options.cancellation.check();
            if (!filter.allows(edge.edge_type)) continue;
            const source = try getOrPutNode(alloc, &projection.ordinals, &projection.node_ids, adjacency.node_id, options.limits.max_nodes);
            const target = try getOrPutNode(alloc, &projection.ordinals, &projection.node_ids, edge.neighbor_id, options.limits.max_nodes);
            if (projection.edges.items.len >= options.limits.max_edges) return error.GraphMetricBuildBudgetExceeded;
            try projection.edges.append(alloc, .{ .source = source, .target = target });
        }
    }
    return projection;
}

fn kernelOptions(options: BuildOptions) metrics.Options {
    return .{
        .damping = if (options.config.kind == .pagerank) options.config.damping else 0.85,
        .tolerance = if (options.config.kind == .degree) 0 else options.config.tolerance,
        .max_iterations = if (options.config.kind == .degree) 1 else options.config.max_iterations,
        .max_nodes = options.limits.max_nodes,
        .max_edges = options.limits.max_edges,
        .max_work_items = options.limits.max_work_items,
        .cancellation = options.cancellation,
    };
}

fn chargeWork(options: BuildOptions, projection: Projection) !void {
    if (options.batch_budget) |budget| {
        const amount = try graph_metric_policy.materializationWorkItems(
            options.config.kind,
            projection.source_node_count,
            projection.source_edge_count,
            projection.node_ids.items.len,
            projection.edges.items.len,
            options.config.max_iterations,
        );
        if (amount > options.limits.max_work_items) return error.GraphMetricBuildBudgetExceeded;
        try budget.chargeWork(amount);
    }
}

fn buildFromSegmentAlloc(alloc: Allocator, graph: graph_segment.Segment, options: BuildOptions) !BuildResult {
    var projection = try buildProjectionAlloc(alloc, graph, options);
    defer projection.deinit(alloc);
    try chargeWork(options, projection);

    const kernel_options = kernelOptions(options);
    var result = switch (options.config.kind) {
        .degree => try metrics.degreeAlloc(alloc, projection.node_ids.items.len, projection.edges.items, kernel_options),
        .pagerank => try metrics.pageRankAlloc(alloc, projection.node_ids.items.len, projection.edges.items, kernel_options),
        .eigenvector => try metrics.eigenvectorAlloc(alloc, projection.node_ids.items.len, projection.edges.items, kernel_options),
        .hits_authority, .hits_hub => blk: {
            const pair = try metrics.hitsAlloc(alloc, projection.node_ids.items.len, projection.edges.items, kernel_options);
            const selected = if (options.config.kind == .hits_authority) pair.authorities else pair.hubs;
            const unused = if (options.config.kind == .hits_authority) pair.hubs else pair.authorities;
            alloc.free(unused);
            break :blk metrics.Result{ .scores = selected, .iterations_completed = pair.iterations_completed, .converged = pair.converged, .delta = pair.delta };
        },
    };
    defer result.deinit(alloc);
    return try encodeMetricResultAlloc(alloc, projection.node_ids.items, options, result);
}

fn buildHitsPairFromSegmentAlloc(
    alloc: Allocator,
    graph: graph_segment.Segment,
    first_options: BuildOptions,
    second_config: graph_mod.GraphMetricConfig,
) !BuildPairResult {
    if (!graph_mod.graphMetricHitsPairCompatible(first_options.config, second_config)) return error.InvalidGraphMetricBuildOptions;
    var projection = try buildProjectionAlloc(alloc, graph, first_options);
    defer projection.deinit(alloc);
    try chargeWork(first_options, projection);

    var pair = try metrics.hitsAlloc(
        alloc,
        projection.node_ids.items.len,
        projection.edges.items,
        kernelOptions(first_options),
    );
    defer pair.deinit(alloc);
    const first_scores = if (first_options.config.kind == .hits_authority) pair.authorities else pair.hubs;
    const second_scores = if (second_config.kind == .hits_authority) pair.authorities else pair.hubs;
    const first_result = metrics.Result{ .scores = first_scores, .iterations_completed = pair.iterations_completed, .converged = pair.converged, .delta = pair.delta };
    const second_result = metrics.Result{ .scores = second_scores, .iterations_completed = pair.iterations_completed, .converged = pair.converged, .delta = pair.delta };
    var first = try encodeMetricResultAlloc(alloc, projection.node_ids.items, first_options, first_result);
    errdefer first.deinit(alloc);
    var second_options = first_options;
    second_options.config = second_config;
    const second = try encodeMetricResultAlloc(alloc, projection.node_ids.items, second_options, second_result);
    return .{ .first = first, .second = second };
}

fn encodeMetricResultAlloc(
    alloc: Allocator,
    node_ids: []const []const u8,
    options: BuildOptions,
    result: metrics.Result,
) !BuildResult {
    const scores = try makeScoresAlloc(alloc, node_ids, result.scores, options.cancellation);
    var scores_owned = true;
    defer if (scores_owned) {
        for (scores) |*score| score.deinit(alloc);
        alloc.free(scores);
    };

    var segment = try makeMetricSegmentAlloc(alloc, options, result, scores);
    scores_owned = false;
    defer segment.deinit(alloc);
    const payload = metric_segment.encodeAllocWithCancellationAndLimit(
        alloc,
        segment,
        options.cancellation,
        options.limits.max_metric_payload_bytes,
    ) catch |err| switch (err) {
        error.GraphMetricSegmentTooLarge => return error.GraphMetricBuildBudgetExceeded,
        else => return err,
    };
    errdefer alloc.free(payload);
    if (options.batch_budget) |budget| try budget.chargePayload(payload.len);
    const name = try artifactNameAlloc(alloc, options.graph_index_name, options.config.name);
    errdefer alloc.free(name);
    const artifact_id = try std.fmt.allocPrint(alloc, "lake-graph-metric:{d}:{s}:{d}", .{ name.len, name, payload.len });
    errdefer alloc.free(artifact_id);
    const checksum = try std.fmt.allocPrint(alloc, "len:{d}", .{payload.len});
    var artifact = artifact_ref.ArtifactRef{
        .kind = .graph_metric_segment,
        .name = name,
        .artifact_id = artifact_id,
        .byte_len = @intCast(payload.len),
        .checksum = checksum,
        .metadata_version = metric_segment.wire_version,
        .published_generation = options.provenance.published_generation,
        .edge_generation = options.provenance.edge_generation,
        .computed_at_ms = options.provenance.computed_at_ms,
        .materializer_fingerprint = segment.materializer_fingerprint,
    };
    try populateGraphMetricIntegrity(&artifact, segment, payload);
    return .{ .payload = payload, .artifact = artifact };
}

fn populateGraphMetricIntegrity(ref: *artifact_ref.ArtifactRef, segment: metric_segment.Segment, payload: []const u8) !void {
    const integrity = try metric_segment.artifactIntegrity(segment, payload);
    ref.graph_metric_control_len = integrity.control_len;
    ref.graph_metric_routing_footer_len = integrity.routing_footer_len;
    ref.graph_metric_control_checksum = integrity.control_checksum;
    ref.graph_metric_routing_checksum = integrity.routing_checksum;
    ref.graph_metric_config_fingerprint = segment.config_fingerprint;
    ref.graph_metric_source_checksum = artifact_store.sha256DigestFromChecksum(segment.source_graph_checksum) catch
        return error.ArtifactIntegrityMismatch;
    ref.graph_metric_materialization_state = @enumFromInt(@intFromEnum(segment.materialization_state));
    ref.graph_metric_rejection_reason = @enumFromInt(@intFromEnum(segment.rejection_reason));
}

fn validateOptions(graph_payload: []const u8, options: BuildOptions) !void {
    try graph_metric_policy.validateConfigs(&.{options.config}, options.limits);
    if (graph_payload.len > options.limits.max_graph_payload_bytes) return error.GraphMetricBuildBudgetExceeded;
    if (graph_payload.len == 0 or options.graph_index_name.len == 0 or options.config.name.len == 0 or options.source_graph.kind != .graph_segment or options.source_graph.artifact_id.len == 0 or options.source_graph.checksum.len == 0 or options.limits.max_metric_payload_bytes == 0) return error.InvalidGraphMetricBuildOptions;
    if (options.source_graph.byte_len != graph_payload.len) return error.ArtifactIntegrityMismatch;
    artifact_store.validateSha256ArtifactIdentity(options.source_graph.artifact_id, options.source_graph.checksum) catch return error.ArtifactIntegrityMismatch;
    try artifact_store.validatePayloadSha256WithCancellation(graph_payload, options.source_graph.checksum, options.cancellation);
    try graph_mod.validateGraphMetricEdgeFilters(&.{}, &.{options.config});
}

fn makeMetricSegmentAlloc(alloc: Allocator, options: BuildOptions, result: metrics.Result, scores: []metric_segment.Score) !metric_segment.Segment {
    const graph_index_name = try alloc.dupe(u8, options.graph_index_name);
    errdefer alloc.free(graph_index_name);
    const metric_name = try alloc.dupe(u8, options.config.name);
    errdefer alloc.free(metric_name);
    const source_artifact_id = try alloc.dupe(u8, options.source_graph.artifact_id);
    errdefer alloc.free(source_artifact_id);
    const source_checksum = try alloc.dupe(u8, options.source_graph.checksum);
    errdefer alloc.free(source_checksum);
    var edge_filter = try cloneSortedEdgeFilterAlloc(alloc, options.config.edge_filter);
    errdefer edge_filter.deinit(alloc);
    return .{
        .graph_index_name = graph_index_name,
        .metric_name = metric_name,
        .kind = options.config.kind,
        .source_graph_artifact_id = source_artifact_id,
        .source_graph_checksum = source_checksum,
        .config_fingerprint = configFingerprint(options.config),
        .materializer_fingerprint = graph_metric_policy.materializerFingerprint(options.limits),
        .published_generation = options.provenance.published_generation,
        .edge_generation = options.provenance.edge_generation,
        .computed_at_ms = options.provenance.computed_at_ms,
        .edge_filter = edge_filter,
        .converged = result.converged,
        .iterations_completed = result.iterations_completed,
        .delta = result.delta,
        .scores = scores,
    };
}

fn makeScoresAlloc(alloc: Allocator, node_ids: []const []const u8, values: []const f64, cancellation: CancellationToken) ![]metric_segment.Score {
    if (node_ids.len != values.len) return error.InvalidGraphMetricScore;
    try cancellation.check();
    const scores = try alloc.alloc(metric_segment.Score, node_ids.len);
    errdefer alloc.free(scores);
    var initialized: usize = 0;
    errdefer for (scores[0..initialized]) |*score| score.deinit(alloc);
    for (node_ids, values, 0..) |node_id, value, i| {
        if (i % 4096 == 0) try cancellation.check();
        if (!std.math.isFinite(value)) return error.InvalidGraphMetricScore;
        scores[i] = .{ .node_id = try alloc.dupe(u8, node_id), .value = value };
        initialized += 1;
    }
    try cancellation.check();
    std.mem.sort(metric_segment.Score, scores, {}, lessScore);
    try cancellation.check();
    return scores;
}

fn getOrPutNode(alloc: Allocator, ordinals: *std.StringHashMapUnmanaged(usize), node_ids: *std.ArrayListUnmanaged([]const u8), node_id: []const u8, max_nodes: usize) !usize {
    if (ordinals.get(node_id)) |ordinal| return ordinal;
    if (node_ids.items.len >= max_nodes) return error.GraphMetricBuildBudgetExceeded;
    const ordinal = node_ids.items.len;
    try node_ids.append(alloc, node_id);
    errdefer _ = node_ids.pop();
    try ordinals.put(alloc, node_id, ordinal);
    return ordinal;
}

fn cloneSortedEdgeFilterAlloc(alloc: Allocator, filter: graph_mod.GraphMetricEdgeFilter) !graph_mod.GraphMetricEdgeFilter {
    if (filter.types.len == 0) return .{ .mode = filter.mode };
    const edge_types = try alloc.alloc([]const u8, filter.types.len);
    errdefer alloc.free(edge_types);
    var initialized: usize = 0;
    errdefer for (edge_types[0..initialized]) |edge_type| alloc.free(edge_type);
    for (filter.types, 0..) |edge_type, i| {
        edge_types[i] = try alloc.dupe(u8, edge_type);
        initialized += 1;
    }
    std.mem.sort([]const u8, edge_types, {}, lessString);
    return .{ .mode = filter.mode, .types = edge_types };
}
fn lessString(_: void, a: []const u8, b: []const u8) bool {
    return std.mem.lessThan(u8, a, b);
}
fn lessScore(_: void, a: metric_segment.Score, b: metric_segment.Score) bool {
    return std.mem.lessThan(u8, a.node_id, b.node_id);
}

pub fn configFingerprint(config: graph_mod.GraphMetricConfig) u64 {
    var hasher = std.hash.Wyhash.init(0);
    hashU64(&hasher, @intFromEnum(config.kind));
    hashU64(&hasher, @bitCast(config.damping));
    hashU64(&hasher, @bitCast(config.tolerance));
    hashU64(&hasher, config.max_iterations);
    hashU64(&hasher, @intFromEnum(config.edge_filter.mode));
    hashU64(&hasher, config.edge_filter.types.len);
    const sorted = config.edge_filter.types;
    // The storage fingerprint is order-independent. Avoid allocating by
    // repeatedly selecting the next lexical value; config limits keep this tiny.
    var last: ?[]const u8 = null;
    for (0..sorted.len) |_| {
        var next: ?[]const u8 = null;
        for (sorted) |candidate| if ((last == null or std.mem.order(u8, candidate, last.?) == .gt) and (next == null or std.mem.lessThan(u8, candidate, next.?))) {
            next = candidate;
        };
        const value = next orelse break;
        hashU64(&hasher, value.len);
        hasher.update(value);
        last = value;
    }
    const value = hasher.final() & std.math.maxInt(i64);
    return if (value == 0) 1 else value;
}

pub fn materializerFingerprint(limits: Limits) u64 {
    return graph_metric_policy.materializerFingerprint(limits);
}
fn hashU64(hasher: *std.hash.Wyhash, value: u64) void {
    var raw = value;
    hasher.update(std.mem.asBytes(&raw));
}

pub fn freeArtifactRef(alloc: Allocator, artifact: artifact_ref.ArtifactRef) void {
    if (artifact.name.len > 0) alloc.free(artifact.name);
    alloc.free(artifact.artifact_id);
    alloc.free(artifact.checksum);
}

test "serverless lake graph metrics build immutable pagerank and degree vectors" {
    const alloc = std.testing.allocator;
    var graph = graph_segment.Segment{ .adjacencies = try alloc.alloc(graph_segment.Adjacency, 3) };
    defer graph.deinit(alloc);
    graph.adjacencies[0] = .{ .node_id = try alloc.dupe(u8, "a"), .out_edges = try alloc.alloc(graph_segment.Edge, 1), .in_edges = try alloc.alloc(graph_segment.Edge, 0) };
    graph.adjacencies[0].out_edges[0] = .{ .neighbor_id = try alloc.dupe(u8, "b"), .edge_type = try alloc.dupe(u8, "cites"), .weight = 1 };
    graph.adjacencies[1] = .{ .node_id = try alloc.dupe(u8, "b"), .out_edges = try alloc.alloc(graph_segment.Edge, 0), .in_edges = try alloc.alloc(graph_segment.Edge, 1) };
    graph.adjacencies[1].in_edges[0] = .{ .neighbor_id = try alloc.dupe(u8, "a"), .edge_type = try alloc.dupe(u8, "cites"), .weight = 1 };
    graph.adjacencies[2] = .{ .node_id = try alloc.dupe(u8, "isolated"), .out_edges = try alloc.alloc(graph_segment.Edge, 0), .in_edges = try alloc.alloc(graph_segment.Edge, 0) };
    const graph_payload = try graph_segment.encodeAlloc(alloc, graph);
    defer alloc.free(graph_payload);
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(graph_payload, &digest, .{});
    const checksum = std.fmt.bytesToHex(digest, .lower);
    var artifact_id_buf: [artifact_store.sha256_artifact_id_prefix.len + checksum.len]u8 = undefined;
    const artifact_id = try std.fmt.bufPrint(&artifact_id_buf, "{s}{s}", .{ artifact_store.sha256_artifact_id_prefix, checksum });
    const source = artifact_ref.ArtifactRef{ .kind = .graph_segment, .name = "graph", .artifact_id = artifact_id, .byte_len = graph_payload.len, .checksum = &checksum };
    var built = try buildFromGraphPayloadAlloc(alloc, graph_payload, .{ .graph_index_name = "graph", .config = .{ .name = "pagerank" }, .source_graph = source });
    defer built.deinit(alloc);
    var decoded = try metric_segment.decodeAlloc(alloc, built.payload);
    defer decoded.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), decoded.scores.len);
    try std.testing.expect(decoded.score("b").? > decoded.score("a").?);
    try std.testing.expect(decoded.score("isolated") == null);
    try std.testing.expectEqualStrings("5:graph8:pagerank", built.artifact.name);

    var tampered = try alloc.dupe(u8, graph_payload);
    defer alloc.free(tampered);
    tampered[tampered.len - 1] ^= 1;
    try std.testing.expectError(error.ArtifactIntegrityMismatch, buildFromGraphPayloadAlloc(alloc, tampered, .{
        .graph_index_name = "graph",
        .config = .{ .name = "pagerank" },
        .source_graph = source,
    }));
    var wrong_length = source;
    wrong_length.byte_len += 1;
    try std.testing.expectError(error.ArtifactIntegrityMismatch, buildFromGraphPayloadAlloc(alloc, graph_payload, .{
        .graph_index_name = "graph",
        .config = .{ .name = "pagerank" },
        .source_graph = wrong_length,
    }));
}

test "serverless lake graph metrics persist a budget rejection with exact provenance" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/graph-metric-artifacts", .{tmp.sub_path});
    defer alloc.free(root);
    var fs = try fs_artifact_store.FsStore.init(alloc, root);
    var artifacts = fs.artifactStore();
    defer artifacts.deinit();

    var graph = graph_segment.Segment{ .adjacencies = try alloc.alloc(graph_segment.Adjacency, 2) };
    defer graph.deinit(alloc);
    graph.adjacencies[0] = .{ .node_id = try alloc.dupe(u8, "a"), .out_edges = try alloc.alloc(graph_segment.Edge, 1), .in_edges = try alloc.alloc(graph_segment.Edge, 0) };
    graph.adjacencies[0].out_edges[0] = .{ .neighbor_id = try alloc.dupe(u8, "b"), .edge_type = try alloc.dupe(u8, "cites"), .weight = 1 };
    graph.adjacencies[1] = .{ .node_id = try alloc.dupe(u8, "b"), .out_edges = try alloc.alloc(graph_segment.Edge, 0), .in_edges = try alloc.alloc(graph_segment.Edge, 1) };
    graph.adjacencies[1].in_edges[0] = .{ .neighbor_id = try alloc.dupe(u8, "a"), .edge_type = try alloc.dupe(u8, "cites"), .weight = 1 };
    const graph_payload = try graph_segment.encodeAlloc(alloc, graph);
    defer alloc.free(graph_payload);
    var source_metadata = try artifacts.put(graph_payload);
    defer source_metadata.deinit(alloc);
    const source = artifact_ref.ArtifactRef{
        .kind = .graph_segment,
        .name = "graph",
        .artifact_id = source_metadata.artifact_id,
        .byte_len = source_metadata.byte_len,
        .checksum = source_metadata.checksum,
    };
    const configs = [_]graph_mod.GraphMetricConfig{.{ .name = "degree", .kind = .degree }};
    const published = try publishManyFromGraphArtifactAlloc(alloc, &artifacts, "graph", source, &configs, .none, .{ .max_nodes = 1 }, .{ .published_generation = 1, .edge_generation = 1, .computed_at_ms = 1 });
    defer {
        for (published) |ref| freeArtifactRef(alloc, ref);
        alloc.free(published);
    }
    try std.testing.expectEqual(@as(usize, 1), published.len);
    const payload = try artifacts.getVerifiedAllocWithCancellationUsingAllocator(
        alloc,
        published[0].artifact_id,
        published[0].byte_len,
        published[0].checksum,
        .none,
    );
    defer alloc.free(payload);
    var decoded = try metric_segment.decodeAlloc(alloc, payload);
    defer decoded.deinit(alloc);
    try std.testing.expectEqual(metric_segment.MaterializationState.rejected, decoded.materialization_state);
    try std.testing.expectEqual(metric_segment.RejectionReason.build_budget_exceeded, decoded.rejection_reason);
    try std.testing.expectEqualStrings(source.artifact_id, decoded.source_graph_artifact_id);
    try std.testing.expectEqualStrings(source.checksum, decoded.source_graph_checksum);
    try std.testing.expectEqual(configFingerprint(configs[0]), decoded.config_fingerprint);
    try std.testing.expectEqual(materializerFingerprint(.{ .max_nodes = 1 }), decoded.materializer_fingerprint);
}

test "serverless graph metric projection bounds filtered scans and inner-loop cancellation" {
    const alloc = std.testing.allocator;
    const edges = try alloc.alloc(graph_segment.Edge, 4096);
    defer alloc.free(edges);
    for (edges) |*edge| edge.* = .{
        .neighbor_id = @constCast("b"),
        .edge_type = @constCast("ignored"),
        .weight = 1,
    };
    var adjacencies = [_]graph_segment.Adjacency{.{
        .node_id = @constCast("a"),
        .out_edges = edges,
        .in_edges = @constCast(&.{}),
    }};
    const graph = graph_segment.Segment{ .adjacencies = &adjacencies };
    const source = artifact_ref.ArtifactRef{
        .kind = .graph_segment,
        .artifact_id = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        .byte_len = 1,
        .checksum = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    };
    const filtered = graph_mod.GraphMetricConfig{
        .name = "degree",
        .kind = .degree,
        .edge_filter = .{ .mode = .types, .types = &.{"wanted"} },
    };
    try std.testing.expectError(error.GraphMetricBuildBudgetExceeded, buildProjectionAlloc(alloc, graph, .{
        .graph_index_name = "graph",
        .config = filtered,
        .source_graph = source,
        .limits = .{ .max_edges = 4095 },
    }));

    const State = struct {
        calls: usize = 0,

        fn cancelled(ptr: *const anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(@constCast(ptr)));
            self.calls += 1;
            return self.calls >= 3;
        }
    };
    var state = State{};
    const cancellation = CancellationToken{ .ptr = &state, .is_cancelled_fn = State.cancelled };
    try std.testing.expectError(error.Canceled, buildProjectionAlloc(alloc, graph, .{
        .graph_index_name = "graph",
        .config = .{ .name = "degree", .kind = .degree },
        .source_graph = source,
        .cancellation = cancellation,
        .limits = .{ .max_edges = edges.len },
    }));
    try std.testing.expectEqual(@as(usize, 3), state.calls);
}

test "serverless lake graph metrics share one bounded HITS execution for a compatible pair" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/graph-metric-hits", .{tmp.sub_path});
    defer alloc.free(root);
    var fs = try fs_artifact_store.FsStore.init(alloc, root);
    var artifacts = fs.artifactStore();
    defer artifacts.deinit();

    var graph = graph_segment.Segment{ .adjacencies = try alloc.alloc(graph_segment.Adjacency, 2) };
    defer graph.deinit(alloc);
    graph.adjacencies[0] = .{ .node_id = try alloc.dupe(u8, "a"), .out_edges = try alloc.alloc(graph_segment.Edge, 1), .in_edges = try alloc.alloc(graph_segment.Edge, 0) };
    graph.adjacencies[0].out_edges[0] = .{ .neighbor_id = try alloc.dupe(u8, "b"), .edge_type = try alloc.dupe(u8, "cites"), .weight = 1 };
    graph.adjacencies[1] = .{ .node_id = try alloc.dupe(u8, "b"), .out_edges = try alloc.alloc(graph_segment.Edge, 0), .in_edges = try alloc.alloc(graph_segment.Edge, 1) };
    graph.adjacencies[1].in_edges[0] = .{ .neighbor_id = try alloc.dupe(u8, "a"), .edge_type = try alloc.dupe(u8, "cites"), .weight = 1 };
    const graph_payload = try graph_segment.encodeAlloc(alloc, graph);
    defer alloc.free(graph_payload);
    var source_metadata = try artifacts.put(graph_payload);
    defer source_metadata.deinit(alloc);
    const source = artifact_ref.ArtifactRef{ .kind = .graph_segment, .name = "graph", .artifact_id = source_metadata.artifact_id, .byte_len = source_metadata.byte_len, .checksum = source_metadata.checksum };
    const configs = [_]graph_mod.GraphMetricConfig{
        .{ .name = "authority", .kind = .hits_authority },
        .{ .name = "hub", .kind = .hits_hub },
    };
    // 300 kernel work items plus one source-node/edge projection pass.
    const limits = Limits{ .max_work_items = 303, .max_total_work_items = 303 };
    const published = try publishManyFromGraphArtifactAlloc(alloc, &artifacts, "graph", source, &configs, .none, limits, .{ .published_generation = 1, .edge_generation = 1, .computed_at_ms = 1 });
    defer {
        for (published) |ref| freeArtifactRef(alloc, ref);
        alloc.free(published);
    }
    try std.testing.expectEqual(@as(usize, 2), published.len);
    for (published) |ref| {
        const payload = try artifacts.getVerifiedAllocWithCancellationUsingAllocator(alloc, ref.artifact_id, ref.byte_len, ref.checksum, .none);
        defer alloc.free(payload);
        var decoded = try metric_segment.decodeAlloc(alloc, payload);
        defer decoded.deinit(alloc);
        try std.testing.expectEqual(metric_segment.MaterializationState.ready, decoded.materialization_state);
        try std.testing.expectEqual(materializerFingerprint(limits), decoded.materializer_fingerprint);
    }
}

test "serverless lake graph metrics reject work beyond the aggregate publication budget" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/graph-metric-aggregate", .{tmp.sub_path});
    defer alloc.free(root);
    var fs = try fs_artifact_store.FsStore.init(alloc, root);
    var artifacts = fs.artifactStore();
    defer artifacts.deinit();

    var graph = graph_segment.Segment{ .adjacencies = try alloc.alloc(graph_segment.Adjacency, 2) };
    defer graph.deinit(alloc);
    graph.adjacencies[0] = .{ .node_id = try alloc.dupe(u8, "a"), .out_edges = try alloc.alloc(graph_segment.Edge, 1), .in_edges = try alloc.alloc(graph_segment.Edge, 0) };
    graph.adjacencies[0].out_edges[0] = .{ .neighbor_id = try alloc.dupe(u8, "b"), .edge_type = try alloc.dupe(u8, "cites"), .weight = 1 };
    graph.adjacencies[1] = .{ .node_id = try alloc.dupe(u8, "b"), .out_edges = try alloc.alloc(graph_segment.Edge, 0), .in_edges = try alloc.alloc(graph_segment.Edge, 1) };
    graph.adjacencies[1].in_edges[0] = .{ .neighbor_id = try alloc.dupe(u8, "a"), .edge_type = try alloc.dupe(u8, "cites"), .weight = 1 };
    const graph_payload = try graph_segment.encodeAlloc(alloc, graph);
    defer alloc.free(graph_payload);
    var source_metadata = try artifacts.put(graph_payload);
    defer source_metadata.deinit(alloc);
    const source = artifact_ref.ArtifactRef{ .kind = .graph_segment, .name = "graph", .artifact_id = source_metadata.artifact_id, .byte_len = source_metadata.byte_len, .checksum = source_metadata.checksum };
    const configs = [_]graph_mod.GraphMetricConfig{
        .{ .name = "rank_a", .kind = .pagerank },
        .{ .name = "rank_b", .kind = .pagerank },
    };
    // One PageRank consumes 300 kernel work items plus three projection
    // items; the table-wide budget admits the first and rejects the second.
    const published = try publishManyFromGraphArtifactAlloc(alloc, &artifacts, "graph", source, &configs, .none, .{ .max_work_items = 303, .max_total_work_items = 303 }, .{ .published_generation = 1, .edge_generation = 1, .computed_at_ms = 1 });
    defer {
        for (published) |ref| freeArtifactRef(alloc, ref);
        alloc.free(published);
    }
    for (published, 0..) |ref, i| {
        const payload = try artifacts.getVerifiedAllocWithCancellationUsingAllocator(alloc, ref.artifact_id, ref.byte_len, ref.checksum, .none);
        defer alloc.free(payload);
        var decoded = try metric_segment.decodeAlloc(alloc, payload);
        defer decoded.deinit(alloc);
        try std.testing.expectEqual(if (i == 0) metric_segment.MaterializationState.ready else .rejected, decoded.materialization_state);
    }

    var shared_budget = graph_metric_policy.Budget{ .limits = .{ .max_work_items = 303, .max_total_work_items = 303 } };
    const one_config = [_]graph_mod.GraphMetricConfig{.{ .name = "shared_rank", .kind = .pagerank }};
    var prepared = try prepareGraphArtifactAlloc(alloc, &artifacts, source, .none, shared_budget.limits);
    defer prepared.deinit(alloc);
    try std.testing.expect(prepared.identifies(source));
    var wrong_source = source;
    wrong_source.byte_len += 1;
    try std.testing.expect(!prepared.identifies(wrong_source));
    const first_index = try publishManyFromPreparedGraphWithBudgetAlloc(
        alloc,
        &artifacts,
        "graph_a",
        source,
        &one_config,
        .none,
        shared_budget.limits,
        &shared_budget,
        &prepared,
        .{ .published_generation = 1, .edge_generation = 1, .computed_at_ms = 1 },
    );
    defer {
        for (first_index) |ref| freeArtifactRef(alloc, ref);
        alloc.free(first_index);
    }
    const second_index = try publishManyFromPreparedGraphWithBudgetAlloc(
        alloc,
        &artifacts,
        "graph_b",
        source,
        &one_config,
        .none,
        shared_budget.limits,
        &shared_budget,
        &prepared,
        .{ .published_generation = 1, .edge_generation = 1, .computed_at_ms = 1 },
    );
    defer {
        for (second_index) |ref| freeArtifactRef(alloc, ref);
        alloc.free(second_index);
    }
    const expected_states = [_]metric_segment.MaterializationState{ .ready, .rejected };
    for ([_][]const artifact_ref.ArtifactRef{ first_index, second_index }, expected_states) |refs, expected_state| {
        const payload = try artifacts.getVerifiedAllocWithCancellationUsingAllocator(alloc, refs[0].artifact_id, refs[0].byte_len, refs[0].checksum, .none);
        defer alloc.free(payload);
        var decoded = try metric_segment.decodeAlloc(alloc, payload);
        defer decoded.deinit(alloc);
        try std.testing.expectEqual(expected_state, decoded.materialization_state);
    }
}
