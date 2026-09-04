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
//! synchronous at its public boundary: callers schedule it through their
//! existing runtime, while large target-owned kernel ranges use that same
//! std.Io instance for bounded deterministic fan-out.

const std = @import("std");
const Allocator = std.mem.Allocator;
const CancellationToken = @import("../../common/cancellation.zig").CancellationToken;
const graph_mod = @import("../../graph/graph.zig");
const metrics = @import("../../graph/metrics.zig");
const artifact_ref = @import("../manifest/artifact_ref.zig");
const artifact_store = @import("../artifacts/store.zig");
const fs_artifact_store = @import("../artifacts/fs_store.zig");
const bounded_decode = @import("../bounded_decode.zig");
const graph_segment = @import("../graph_segment/mod.zig");
const metric_segment = @import("../graph_metric_segment/mod.zig");
const graph_metric_policy = @import("graph_metric_policy.zig");

pub const Limits = graph_metric_policy.Limits;
pub const default_compute_parallelism: usize = 4;
pub const max_compute_parallelism: usize = 16;

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
    io: ?std.Io = null,
    max_parallelism: usize = 1,
    topology_requirements: ?metrics.TopologyRequirements = null,
};

pub const ComputeRuntime = struct {
    io: ?std.Io = null,
    max_parallelism: usize = 1,

    fn validate(self: ComputeRuntime) !void {
        if (self.max_parallelism == 0 or self.max_parallelism > max_compute_parallelism) return error.InvalidGraphMetricBuildOptions;
        if (self.io == null and self.max_parallelism != 1) return error.InvalidGraphMetricBuildOptions;
    }
};

fn topologyRequirementsForKind(kind: graph_mod.GraphMetricKind) metrics.TopologyRequirements {
    return switch (kind) {
        .degree => .degree,
        .pagerank => .pagerank,
        .eigenvector => .eigenvector,
        .hits_authority, .hits_hub => .hits,
    };
}

pub const BuildResult = struct {
    payload: []u8,
    artifact: artifact_ref.ArtifactRef,

    pub fn deinit(self: *BuildResult, alloc: Allocator) void {
        alloc.free(self.payload);
        freeArtifactRef(alloc, self.artifact);
        self.* = undefined;
    }
};

const CompiledEdge = struct {
    source: u32,
    target: u32,
};

/// Filter-independent, ordinal topology compiled once per immutable graph
/// artifact. Node and edge-type strings are owned here so the much larger
/// decoded adjacency object can be released before projections and kernels run.
const CompiledTopology = struct {
    node_ids: []const []const u8,
    edge_types: []const []const u8,
    string_bytes: []u8,
    /// Counting-sort boundaries for `edges`, indexed by interned edge type.
    /// Narrow filters therefore visit only admitted edge ranges instead of
    /// rescanning every edge for every distinct metric configuration.
    edge_type_offsets: []const u32,
    edges: []const CompiledEdge,
    source_node_count: usize,
    source_edge_count: usize,
    retained_bytes: usize,

    fn deinit(self: *CompiledTopology, alloc: Allocator) void {
        alloc.free(self.node_ids);
        alloc.free(self.edge_types);
        alloc.free(self.string_bytes);
        alloc.free(self.edge_type_offsets);
        alloc.free(self.edges);
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
    topology: CompiledTopology,

    pub fn deinit(self: *PreparedGraphArtifact, alloc: Allocator) void {
        alloc.free(self.source_artifact_id);
        alloc.free(self.source_checksum);
        self.topology.deinit(alloc);
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

fn admitGraphDecodePeak(payload_bytes: usize, decoded_retained_bytes: usize, max_peak_memory_bytes: usize) !void {
    const decode_peak_bytes = std.math.add(usize, payload_bytes, decoded_retained_bytes) catch
        return error.GraphMetricBuildBudgetExceeded;
    if (decode_peak_bytes > max_peak_memory_bytes) return error.GraphMetricBuildBudgetExceeded;
}

/// Metric materialization is an outbound-edge computation. Drop the decoded
/// reverse adjacency immediately after artifact validation so it does not
/// coexist with compiled topology, dense vectors, sorting, and output bytes.
fn discardInboundEdges(alloc: Allocator, graph: *graph_segment.Segment) !usize {
    var released: usize = 0;
    for (graph.adjacencies) |*adjacency| {
        released = std.math.add(
            usize,
            released,
            std.math.mul(usize, adjacency.in_edges.len, @sizeOf(graph_segment.Edge)) catch
                return error.GraphMetricBuildBudgetExceeded,
        ) catch return error.GraphMetricBuildBudgetExceeded;
        for (adjacency.in_edges) |*edge| {
            released = std.math.add(usize, released, edge.neighbor_id.len) catch
                return error.GraphMetricBuildBudgetExceeded;
            released = std.math.add(usize, released, edge.edge_type.len) catch
                return error.GraphMetricBuildBudgetExceeded;
            edge.deinit(alloc);
        }
        alloc.free(adjacency.in_edges);
        adjacency.in_edges = &.{};
    }
    return released;
}

pub fn buildFromGraphPayloadAlloc(alloc: Allocator, graph_payload: []const u8, options: BuildOptions) !BuildResult {
    try validateOptions(graph_payload, options);
    const decoded_retained_bytes = try graph_segment.decodedRetainedBytes(graph_payload);
    try admitGraphDecodePeak(graph_payload.len, decoded_retained_bytes, options.limits.max_peak_memory_bytes);
    var graph = graph_segment.decodeAllocWithCancellation(alloc, graph_payload, options.cancellation) catch |err| switch (err) {
        error.DecodedArtifactTooLarge => return error.GraphMetricBuildBudgetExceeded,
        else => return err,
    };
    var graph_owned = true;
    defer if (graph_owned) graph.deinit(alloc);
    const released_inbound_bytes = try discardInboundEdges(alloc, &graph);
    const metric_retained_bytes = std.math.sub(usize, decoded_retained_bytes, released_inbound_bytes) catch
        return error.InvalidGraphMetricBuildOptions;
    var topology = try compileTopologyWithinBudgetAlloc(
        alloc,
        graph,
        metric_retained_bytes,
        options.limits.max_peak_memory_bytes,
        options.cancellation,
    );
    defer topology.deinit(alloc);
    graph.deinit(alloc);
    graph_owned = false;
    return try buildFromTopologyAlloc(alloc, topology, options);
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
        .{},
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
    runtime: ComputeRuntime,
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
        runtime,
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
    var graph_payload_owned = true;
    defer if (graph_payload_owned) alloc.free(graph_payload);
    const decoded_retained_bytes = try graph_segment.decodedRetainedBytes(graph_payload);
    // The verified object-store buffer remains live while the decoded graph is
    // allocated. Admit their overlap, not merely the eventual retained graph.
    try admitGraphDecodePeak(graph_payload.len, decoded_retained_bytes, limits.max_peak_memory_bytes);
    var graph = graph_segment.decodeAllocWithCancellation(alloc, graph_payload, cancellation) catch |err| switch (err) {
        error.DecodedArtifactTooLarge => return error.GraphMetricBuildBudgetExceeded,
        else => return err,
    };
    errdefer graph.deinit(alloc);
    alloc.free(graph_payload);
    graph_payload_owned = false;
    const released_inbound_bytes = try discardInboundEdges(alloc, &graph);
    const metric_retained_bytes = std.math.sub(usize, decoded_retained_bytes, released_inbound_bytes) catch
        return error.InvalidGraphMetricBuildOptions;
    var topology = try compileTopologyWithinBudgetAlloc(
        alloc,
        graph,
        metric_retained_bytes,
        limits.max_peak_memory_bytes,
        cancellation,
    );
    errdefer topology.deinit(alloc);
    const source_artifact_id = try alloc.dupe(u8, source_graph.artifact_id);
    errdefer alloc.free(source_artifact_id);
    const source_checksum = try alloc.dupe(u8, source_graph.checksum);
    graph.deinit(alloc);
    return .{
        .source_artifact_id = source_artifact_id,
        .source_checksum = source_checksum,
        .source_byte_len = source_graph.byte_len,
        .topology = topology,
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
    runtime: ComputeRuntime,
) ![]artifact_ref.ArtifactRef {
    if (configs.len == 0) return try alloc.alloc(artifact_ref.ArtifactRef, 0);
    try provenance.validate();
    try runtime.validate();
    try validatePublicationOptions(graph_index_name, source_graph, configs, cancellation, limits, batch_budget);
    if (!prepared.identifies(source_graph)) return error.ArtifactIntegrityMismatch;

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
        var group_requirements = metrics.TopologyRequirements{};
        for (configs, processed) |candidate, done| {
            if (done or !candidate.edge_filter.equivalent(config.edge_filter)) continue;
            group_requirements = group_requirements.merge(topologyRequirementsForKind(candidate.kind));
        }
        const group_options = BuildOptions{
            .graph_index_name = graph_index_name,
            .config = config,
            .source_graph = source_graph,
            .cancellation = cancellation,
            .limits = limits,
            .batch_budget = batch_budget,
            .provenance = provenance,
            .io = runtime.io,
            .max_parallelism = runtime.max_parallelism,
            .topology_requirements = group_requirements,
        };
        var projection = buildProjectionFromTopologyAlloc(alloc, prepared.topology, 0, group_options) catch |err| switch (err) {
            error.GraphMetricBuildBudgetExceeded => {
                for (configs, 0..) |candidate, candidate_index| {
                    if (processed[candidate_index] or !candidate.edge_filter.equivalent(config.edge_filter)) continue;
                    refs[candidate_index] = try publishRejectedAlloc(alloc, artifacts, graph_index_name, source_graph, candidate, cancellation, .build_budget_exceeded, limits, provenance);
                    initialized[candidate_index] = true;
                    processed[candidate_index] = true;
                }
                continue;
            },
            else => return err,
        };
        defer projection.deinit(alloc);
        projection.decoded_retained_bytes = prepared.topology.retained_bytes;
        chargeProjectionWork(group_options, projection) catch {
            for (configs, 0..) |candidate, candidate_index| {
                if (processed[candidate_index] or !candidate.edge_filter.equivalent(config.edge_filter)) continue;
                refs[candidate_index] = try publishRejectedAlloc(alloc, artifacts, graph_index_name, source_graph, candidate, cancellation, .build_budget_exceeded, limits, provenance);
                initialized[candidate_index] = true;
                processed[candidate_index] = true;
            }
            continue;
        };

        for (configs, 0..) |candidate, candidate_index| {
            if (processed[candidate_index] or !candidate.edge_filter.equivalent(config.edge_filter)) continue;
            try cancellation.check();
            var options = group_options;
            options.config = candidate;
            if (findCompatibleHitsPairIndex(configs, processed, candidate_index)) |pair_index| {
                const pair_refs = publishHitsPairFromProjectionAlloc(
                    alloc,
                    artifacts,
                    projection,
                    options,
                    configs[pair_index],
                    cancellation,
                ) catch |err| switch (err) {
                    error.GraphMetricBuildBudgetExceeded => {
                        refs[candidate_index] = try publishRejectedAlloc(alloc, artifacts, graph_index_name, source_graph, candidate, cancellation, .build_budget_exceeded, limits, provenance);
                        initialized[candidate_index] = true;
                        refs[pair_index] = try publishRejectedAlloc(alloc, artifacts, graph_index_name, source_graph, configs[pair_index], cancellation, .build_budget_exceeded, limits, provenance);
                        initialized[pair_index] = true;
                        processed[candidate_index] = true;
                        processed[pair_index] = true;
                        continue;
                    },
                    else => return err,
                };
                refs[candidate_index] = pair_refs[0];
                initialized[candidate_index] = true;
                refs[pair_index] = pair_refs[1];
                initialized[pair_index] = true;
                processed[candidate_index] = true;
                processed[pair_index] = true;
                continue;
            }
            var built = buildFromProjectionAlloc(alloc, projection, options) catch |err| switch (err) {
                error.GraphMetricBuildBudgetExceeded => {
                    refs[candidate_index] = try publishRejectedAlloc(alloc, artifacts, graph_index_name, source_graph, candidate, cancellation, .build_budget_exceeded, limits, provenance);
                    initialized[candidate_index] = true;
                    processed[candidate_index] = true;
                    continue;
                },
                else => return err,
            };
            defer built.deinit(alloc);
            refs[candidate_index] = try putBuildResultAlloc(alloc, artifacts, &built, cancellation);
            initialized[candidate_index] = true;
            processed[candidate_index] = true;
        }
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

fn findCompatibleHitsPairIndex(configs: []const graph_mod.GraphMetricConfig, processed: []const bool, index: usize) ?usize {
    if (configs.len != processed.len) return null;
    if (graph_mod.graphMetricOppositeHitsKind(configs[index].kind) == null) return null;
    for (configs, 0..) |candidate, candidate_index| {
        if (candidate_index == index or candidate_index < index or processed[candidate_index]) continue;
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
    topology: ?metrics.Topology = null,
    source_node_count: usize = 0,
    source_edge_count: usize = 0,
    node_id_bytes: usize = 0,
    decoded_retained_bytes: usize = 0,

    fn deinit(self: *Projection, alloc: Allocator) void {
        self.ordinals.deinit(alloc);
        self.node_ids.deinit(alloc);
        if (self.topology) |*topology| topology.deinit(alloc);
        self.* = .{};
    }

    fn edgeCount(self: Projection) usize {
        return if (self.topology) |topology| topology.edgeCount() else 0;
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

fn compileTopologyWithinBudgetAlloc(
    alloc: Allocator,
    graph: graph_segment.Segment,
    decoded_retained_bytes: usize,
    max_peak_memory_bytes: usize,
    cancellation: CancellationToken,
) !CompiledTopology {
    if (decoded_retained_bytes >= max_peak_memory_bytes)
        return error.GraphMetricBuildBudgetExceeded;
    // Enforce the configured ceiling against allocations actually made by
    // compilation. This admits graphs with many edges but few distinct edge
    // types without relying on std hash-map layout guesses or O(E) worst-case
    // string estimates. The decoded graph remains charged as retained memory.
    var limiter = bounded_decode.AllocationLimiter.init(
        alloc,
        max_peak_memory_bytes - decoded_retained_bytes,
    ) catch return error.InvalidGraphMetricBuildOptions;
    return compileTopologyAlloc(limiter.allocator(), graph, cancellation) catch |err| switch (err) {
        error.OutOfMemory => if (limiter.limit_exceeded)
            error.GraphMetricBuildBudgetExceeded
        else
            error.OutOfMemory,
        else => err,
    };
}

fn compileTopologyAlloc(
    alloc: Allocator,
    graph: graph_segment.Segment,
    cancellation: CancellationToken,
) !CompiledTopology {
    var ordinals = std.StringHashMapUnmanaged(u32).empty;
    defer ordinals.deinit(alloc);
    // Keys borrow the decoded graph only during compilation. The final slices
    // are rebound into one packed allocation below, avoiding O(V + T) heap
    // allocations in long-lived prepared artifacts.
    var node_ids = std.ArrayListUnmanaged([]const u8).empty;
    errdefer node_ids.deinit(alloc);
    var edge_type_ordinals = std.StringHashMapUnmanaged(u32).empty;
    defer edge_type_ordinals.deinit(alloc);
    var edge_types = std.ArrayListUnmanaged([]const u8).empty;
    errdefer edge_types.deinit(alloc);
    var edge_type_counts = std.ArrayListUnmanaged(u32).empty;
    defer edge_type_counts.deinit(alloc);

    for (graph.adjacencies, 0..) |adjacency, i| {
        if (i % 256 == 0) try cancellation.check();
        if (i > std.math.maxInt(u32)) return error.GraphMetricBuildBudgetExceeded;
        if (ordinals.contains(adjacency.node_id)) return error.InvalidGraphMetricBuildOptions;
        try node_ids.append(alloc, adjacency.node_id);
        try ordinals.put(alloc, adjacency.node_id, @intCast(i));
    }
    var source_edge_count: usize = 0;
    var local_edge_count: usize = 0;
    for (graph.adjacencies, 0..) |adjacency, adjacency_index| {
        if (adjacency_index % 256 == 0) try cancellation.check();
        for (adjacency.out_edges) |edge| {
            source_edge_count = std.math.add(usize, source_edge_count, 1) catch
                return error.GraphMetricBuildBudgetExceeded;
            if (source_edge_count % 4096 == 0) try cancellation.check();
            // A serverless graph artifact is table-scoped. Qualified endpoints
            // belong to another table's artifact and cannot be represented by
            // this metric segment's intentionally unqualified node-id key.
            if (edge.neighbor_table_id != null) continue;
            _ = ordinals.get(edge.neighbor_id) orelse return error.InvalidGraphMetricBuildOptions;
            const edge_type_id = if (edge_type_ordinals.get(edge.edge_type)) |existing|
                existing
            else blk: {
                if (edge_types.items.len > std.math.maxInt(u32)) return error.GraphMetricBuildBudgetExceeded;
                const id: u32 = @intCast(edge_types.items.len);
                try edge_types.append(alloc, edge.edge_type);
                try edge_type_counts.append(alloc, 0);
                try edge_type_ordinals.put(alloc, edge.edge_type, id);
                break :blk id;
            };
            if (edge_type_counts.items[edge_type_id] == std.math.maxInt(u32)) return error.GraphMetricBuildBudgetExceeded;
            edge_type_counts.items[edge_type_id] += 1;
            local_edge_count = std.math.add(usize, local_edge_count, 1) catch
                return error.GraphMetricBuildBudgetExceeded;
        }
    }
    if (local_edge_count > std.math.maxInt(u32)) return error.GraphMetricBuildBudgetExceeded;

    const edge_type_offsets = try alloc.alloc(u32, edge_types.items.len + 1);
    errdefer alloc.free(edge_type_offsets);
    edge_type_offsets[0] = 0;
    for (edge_type_counts.items, 0..) |count, i| {
        edge_type_offsets[i + 1] = std.math.add(u32, edge_type_offsets[i], count) catch
            return error.GraphMetricBuildBudgetExceeded;
    }
    const owned_edges = try alloc.alloc(CompiledEdge, local_edge_count);
    errdefer alloc.free(owned_edges);
    const edge_type_cursors = try alloc.dupe(u32, edge_type_offsets[0..edge_types.items.len]);
    defer alloc.free(edge_type_cursors);
    var visited_edges: usize = 0;
    for (graph.adjacencies) |adjacency| {
        const source = ordinals.get(adjacency.node_id) orelse return error.InvalidGraphMetricBuildOptions;
        for (adjacency.out_edges) |edge| {
            visited_edges += 1;
            if (visited_edges % 4096 == 0) try cancellation.check();
            if (edge.neighbor_table_id != null) continue;
            const target = ordinals.get(edge.neighbor_id) orelse return error.InvalidGraphMetricBuildOptions;
            const edge_type_id = edge_type_ordinals.get(edge.edge_type) orelse return error.InvalidGraphMetricBuildOptions;
            const destination = edge_type_cursors[edge_type_id];
            if (@as(usize, destination) >= owned_edges.len) return error.InvalidGraphMetricBuildOptions;
            owned_edges[destination] = .{ .source = source, .target = target };
            edge_type_cursors[edge_type_id] += 1;
        }
    }

    const owned_node_ids = try node_ids.toOwnedSlice(alloc);
    errdefer alloc.free(owned_node_ids);
    const owned_edge_types = try edge_types.toOwnedSlice(alloc);
    errdefer alloc.free(owned_edge_types);
    var string_bytes_len: usize = 0;
    for (owned_node_ids, 0..) |node_id, i| {
        if (i % 256 == 0) try cancellation.check();
        string_bytes_len = std.math.add(usize, string_bytes_len, node_id.len) catch
            return error.GraphMetricBuildBudgetExceeded;
    }
    for (owned_edge_types, 0..) |edge_type, i| {
        if (i % 256 == 0) try cancellation.check();
        string_bytes_len = std.math.add(usize, string_bytes_len, edge_type.len) catch
            return error.GraphMetricBuildBudgetExceeded;
    }
    const string_bytes = try alloc.alloc(u8, string_bytes_len);
    errdefer alloc.free(string_bytes);
    var string_offset: usize = 0;
    for (owned_node_ids, 0..) |*node_id, i| {
        if (i % 256 == 0) try cancellation.check();
        const len = node_id.*.len;
        @memcpy(string_bytes[string_offset .. string_offset + len], node_id.*);
        node_id.* = string_bytes[string_offset .. string_offset + len];
        string_offset += len;
    }
    for (owned_edge_types, 0..) |*edge_type, i| {
        if (i % 256 == 0) try cancellation.check();
        const len = edge_type.*.len;
        @memcpy(string_bytes[string_offset .. string_offset + len], edge_type.*);
        edge_type.* = string_bytes[string_offset .. string_offset + len];
        string_offset += len;
    }
    std.debug.assert(string_offset == string_bytes.len);
    var retained_bytes: usize = 0;
    try addPeakArrayBytes(&retained_bytes, owned_node_ids.len, []u8);
    try addPeakArrayBytes(&retained_bytes, owned_edge_types.len, []u8);
    try addPeakBytes(&retained_bytes, string_bytes.len);
    try addPeakArrayBytes(&retained_bytes, edge_type_offsets.len, u32);
    try addPeakArrayBytes(&retained_bytes, owned_edges.len, CompiledEdge);
    return .{
        .node_ids = owned_node_ids,
        .edge_types = owned_edge_types,
        .string_bytes = string_bytes,
        .edge_type_offsets = edge_type_offsets,
        .edges = owned_edges,
        .source_node_count = graph.adjacencies.len,
        .source_edge_count = source_edge_count,
        .retained_bytes = retained_bytes,
    };
}

fn buildProjectionFromTopologyAlloc(
    alloc: Allocator,
    topology: CompiledTopology,
    decoded_retained_bytes: usize,
    options: BuildOptions,
) !Projection {
    try options.cancellation.check();
    if (topology.edge_type_offsets.len != topology.edge_types.len + 1 or
        @as(usize, topology.edge_type_offsets[topology.edge_types.len]) != topology.edges.len)
    {
        return error.InvalidGraphMetricBuildOptions;
    }
    if (topology.source_node_count > options.limits.max_nodes or topology.source_edge_count > options.limits.max_edges)
        return error.GraphMetricBuildBudgetExceeded;
    const requirements = options.topology_requirements orelse topologyRequirementsForKind(options.config.kind);
    if (!requirements.satisfies(topologyRequirementsForKind(options.config.kind)))
        return error.InvalidGraphMetricBuildOptions;
    var projection = Projection{
        .source_node_count = topology.source_node_count,
        .source_edge_count = topology.source_edge_count,
    };
    errdefer projection.deinit(alloc);
    var filter = try EdgeFilterIndex.init(alloc, options.config.edge_filter);
    defer filter.deinit(alloc);

    const allowed_edge_types = try alloc.alloc(bool, topology.edge_types.len);
    defer alloc.free(allowed_edge_types);
    for (topology.edge_types, 0..) |edge_type, edge_type_id| {
        allowed_edge_types[edge_type_id] = filter.allows(edge_type);
    }

    const active_nodes = try alloc.alloc(bool, topology.node_ids.len);
    defer alloc.free(active_nodes);
    @memset(active_nodes, false);
    var projected_edge_count: usize = 0;
    var inspected_edges: usize = 0;
    for (allowed_edge_types, 0..) |allowed, edge_type_id| {
        if (!allowed) continue;
        const range_start: usize = @intCast(topology.edge_type_offsets[edge_type_id]);
        const range_end: usize = @intCast(topology.edge_type_offsets[edge_type_id + 1]);
        if (range_start > range_end or range_end > topology.edges.len) return error.InvalidGraphMetricBuildOptions;
        projected_edge_count = std.math.add(usize, projected_edge_count, range_end - range_start) catch
            return error.GraphMetricBuildBudgetExceeded;
        if (projected_edge_count > options.limits.max_edges) return error.GraphMetricBuildBudgetExceeded;
        for (topology.edges[range_start..range_end]) |edge| {
            inspected_edges += 1;
            if (inspected_edges % 4096 == 0) try options.cancellation.check();
            active_nodes[edge.source] = true;
            active_nodes[edge.target] = true;
        }
    }
    var projected_node_count: usize = 0;
    for (active_nodes) |active| {
        if (!active) continue;
        projected_node_count = std.math.add(usize, projected_node_count, 1) catch
            return error.GraphMetricBuildBudgetExceeded;
    }
    if (projected_node_count > options.limits.max_nodes) return error.GraphMetricBuildBudgetExceeded;

    var construction_peak = std.math.add(usize, decoded_retained_bytes, topology.retained_bytes) catch
        return error.GraphMetricBuildBudgetExceeded;
    try addPeakArrayBytes(&construction_peak, topology.edge_types.len, bool);
    try addPeakArrayBytes(&construction_peak, topology.node_ids.len, bool);
    try addPeakArrayBytes(&construction_peak, topology.node_ids.len, u32);
    try addPeakArrayBytes(&construction_peak, projected_node_count, []const u8);
    // Exact temporary edge pairs coexist only while building the required
    // compact adjacency lanes.
    try addPeakArrayBytes(&construction_peak, projected_edge_count, metrics.Edge);
    const projected_offset_count = std.math.add(usize, projected_node_count, 1) catch
        return error.GraphMetricBuildBudgetExceeded;
    if (requirements.incoming != .none) try addPeakArrayBytes(&construction_peak, projected_offset_count, u32);
    if (requirements.outgoing != .none) try addPeakArrayBytes(&construction_peak, projected_offset_count, u32);
    if (requirements.incoming == .neighbors) {
        try addPeakArrayBytes(&construction_peak, projected_edge_count, u32);
        try addPeakArrayBytes(&construction_peak, projected_node_count, u32);
    }
    if (requirements.outgoing == .neighbors) {
        try addPeakArrayBytes(&construction_peak, projected_edge_count, u32);
        try addPeakArrayBytes(&construction_peak, projected_node_count, u32);
    }
    if (construction_peak > options.limits.max_peak_memory_bytes)
        return error.GraphMetricBuildBudgetExceeded;

    const unassigned = std.math.maxInt(u32);
    const global_to_local = try alloc.alloc(u32, topology.node_ids.len);
    defer alloc.free(global_to_local);
    @memset(global_to_local, unassigned);
    try projection.node_ids.ensureTotalCapacityPrecise(alloc, projected_node_count);
    var projected_edges = std.ArrayListUnmanaged(metrics.Edge).empty;
    defer projected_edges.deinit(alloc);
    try projected_edges.ensureTotalCapacityPrecise(alloc, projected_edge_count);
    for (topology.node_ids, active_nodes, 0..) |node_id, active, global_ordinal| {
        if (!active) continue;
        if (projection.node_ids.items.len > std.math.maxInt(u32)) return error.GraphMetricBuildBudgetExceeded;
        global_to_local[global_ordinal] = @intCast(projection.node_ids.items.len);
        projection.node_ids.appendAssumeCapacity(node_id);
        projection.node_id_bytes = std.math.add(usize, projection.node_id_bytes, node_id.len) catch
            return error.GraphMetricBuildBudgetExceeded;
    }
    inspected_edges = 0;
    for (allowed_edge_types, 0..) |allowed, edge_type_id| {
        if (!allowed) continue;
        const range_start: usize = @intCast(topology.edge_type_offsets[edge_type_id]);
        const range_end: usize = @intCast(topology.edge_type_offsets[edge_type_id + 1]);
        for (topology.edges[range_start..range_end]) |edge| {
            inspected_edges += 1;
            if (inspected_edges % 4096 == 0) try options.cancellation.check();
            const source = global_to_local[edge.source];
            const target = global_to_local[edge.target];
            if (source == unassigned or target == unassigned) return error.InvalidGraphMetricBuildOptions;
            projected_edges.appendAssumeCapacity(.{ .source = source, .target = target });
        }
    }
    projection.topology = try metrics.Topology.initAllocFor(
        alloc,
        projection.node_ids.items.len,
        projected_edges.items,
        requirements,
        options.cancellation,
    );
    return projection;
}

fn buildProjectionAlloc(alloc: Allocator, graph: graph_segment.Segment, options: BuildOptions) !Projection {
    try options.cancellation.check();
    const requirements = options.topology_requirements orelse topologyRequirementsForKind(options.config.kind);
    if (!requirements.satisfies(topologyRequirementsForKind(options.config.kind)))
        return error.InvalidGraphMetricBuildOptions;
    if (graph.adjacencies.len > options.limits.max_nodes) return error.GraphMetricBuildBudgetExceeded;
    var projection = Projection{};
    errdefer projection.deinit(alloc);
    projection.source_node_count = graph.adjacencies.len;
    var filter = try EdgeFilterIndex.init(alloc, options.config.edge_filter);
    defer filter.deinit(alloc);
    var projected_edges = std.ArrayListUnmanaged(metrics.Edge).empty;
    defer projected_edges.deinit(alloc);

    for (graph.adjacencies, 0..) |adjacency, adjacency_index| {
        if (adjacency_index % 256 == 0) try options.cancellation.check();
        for (adjacency.out_edges) |edge| {
            projection.source_edge_count = std.math.add(usize, projection.source_edge_count, 1) catch
                return error.GraphMetricBuildBudgetExceeded;
            if (projection.source_edge_count > options.limits.max_edges) return error.GraphMetricBuildBudgetExceeded;
            if (projection.source_edge_count % 4096 == 0) try options.cancellation.check();
            if (edge.neighbor_table_id != null) continue;
            if (!filter.allows(edge.edge_type)) continue;
            const source = try getOrPutNode(alloc, &projection.ordinals, &projection.node_ids, adjacency.node_id, options.limits.max_nodes);
            const target = try getOrPutNode(alloc, &projection.ordinals, &projection.node_ids, edge.neighbor_id, options.limits.max_nodes);
            if (projected_edges.items.len >= options.limits.max_edges) return error.GraphMetricBuildBudgetExceeded;
            if (source > std.math.maxInt(u32) or target > std.math.maxInt(u32)) return error.GraphMetricBuildBudgetExceeded;
            try projected_edges.append(alloc, .{ .source = @intCast(source), .target = @intCast(target) });
        }
    }
    for (projection.node_ids.items) |node_id| {
        projection.node_id_bytes = std.math.add(usize, projection.node_id_bytes, node_id.len) catch
            return error.GraphMetricBuildBudgetExceeded;
    }
    projection.topology = try metrics.Topology.initAllocFor(
        alloc,
        projection.node_ids.items.len,
        projected_edges.items,
        requirements,
        options.cancellation,
    );
    return projection;
}

fn addPeakBytes(total: *usize, amount: usize) !void {
    total.* = std.math.add(usize, total.*, amount) catch return error.GraphMetricBuildBudgetExceeded;
}

fn addPeakArrayBytes(total: *usize, count: usize, comptime Element: type) !void {
    const bytes = std.math.mul(usize, count, @sizeOf(Element)) catch
        return error.GraphMetricBuildBudgetExceeded;
    try addPeakBytes(total, bytes);
}

fn estimatedPeakMemoryBytes(projection: Projection, options: BuildOptions, simultaneous_outputs: usize) !usize {
    if (simultaneous_outputs == 0) return error.InvalidGraphMetricBuildOptions;
    var total = projection.decoded_retained_bytes;
    const topology = projection.topology orelse return error.InvalidGraphMetricBuildOptions;
    try addPeakArrayBytes(&total, topology.incoming_offsets.len, u32);
    try addPeakArrayBytes(&total, topology.incoming_sources.len, u32);
    try addPeakArrayBytes(&total, topology.outgoing_offsets.len, u32);
    try addPeakArrayBytes(&total, topology.outgoing_targets.len, u32);
    try addPeakArrayBytes(&total, projection.node_ids.capacity, []const u8);
    // StringHashMap's exact control-byte layout is intentionally private. A
    // conservative per-node allowance covers keys, values, control bytes, and
    // normal load-factor slack without coupling admission to std internals.
    if (projection.ordinals.count() != 0) {
        try addPeakBytes(&total, std.math.mul(usize, projection.node_ids.items.len, 64) catch
            return error.GraphMetricBuildBudgetExceeded);
    }

    const kernel_vectors: usize = switch (options.config.kind) {
        .degree => 1,
        .pagerank => 3,
        .eigenvector => 2,
        .hits_authority, .hits_hub => 4,
    };
    try addPeakBytes(&total, std.math.mul(
        usize,
        projection.node_ids.items.len,
        kernel_vectors * @sizeOf(f64),
    ) catch return error.GraphMetricBuildBudgetExceeded);

    // Encoding borrows canonical node IDs from the projection. The output
    // buffer still repeats IDs in its primary stream and bounded top-score
    // routing tier, so both encoded copies remain part of peak admission.
    const per_output_fixed = std.math.mul(
        usize,
        projection.node_ids.items.len,
        @sizeOf(metric_segment.Score) + 32,
    ) catch return error.GraphMetricBuildBudgetExceeded;
    const per_output_node_ids = std.math.mul(usize, projection.node_id_bytes, 2) catch
        return error.GraphMetricBuildBudgetExceeded;
    const per_output = std.math.add(usize, per_output_fixed, per_output_node_ids) catch
        return error.GraphMetricBuildBudgetExceeded;
    try addPeakBytes(&total, std.math.mul(usize, per_output, simultaneous_outputs) catch
        return error.GraphMetricBuildBudgetExceeded);
    try addPeakBytes(&total, 1024 * 1024);
    return total;
}

fn admitPeakMemory(projection: Projection, options: BuildOptions, simultaneous_outputs: usize) !void {
    if ((try estimatedPeakMemoryBytes(projection, options, simultaneous_outputs)) > options.limits.max_peak_memory_bytes)
        return error.GraphMetricBuildBudgetExceeded;
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
        .io = options.io,
        .max_parallelism = if (options.io == null) 1 else options.max_parallelism,
    };
}

fn projectionWorkItems(projection: Projection, options: BuildOptions) !u64 {
    const requirements = options.topology_requirements orelse topologyRequirementsForKind(options.config.kind);
    const projected_passes: u64 = if (requirements.incoming == .neighbors or requirements.outgoing == .neighbors) 4 else 3;
    return try graph_metric_policy.projectionWorkItems(
        projection.source_node_count,
        projection.source_edge_count,
        projection.node_ids.items.len,
        projection.edgeCount(),
        projected_passes,
    );
}

fn chargeProjectionWork(options: BuildOptions, projection: Projection) !void {
    const amount = try projectionWorkItems(projection, options);
    if (amount > options.limits.max_work_items) return error.GraphMetricBuildBudgetExceeded;
    if (options.batch_budget) |budget| {
        try budget.chargeWork(amount);
    }
}

fn chargeKernelWork(options: BuildOptions, projection: Projection) !void {
    const kernel_amount = try graph_metric_policy.metricWorkItems(
        options.config.kind,
        projection.node_ids.items.len,
        projection.edgeCount(),
        options.config.max_iterations,
    );
    const materialization_amount = std.math.add(
        u64,
        try projectionWorkItems(projection, options),
        kernel_amount,
    ) catch return error.GraphMetricBuildBudgetExceeded;
    // The per-materialization limit still includes projection even when a
    // batch amortizes that projection across multiple compatible metrics.
    if (materialization_amount > options.limits.max_work_items)
        return error.GraphMetricBuildBudgetExceeded;
    if (options.batch_budget) |budget| {
        try budget.chargeWork(kernel_amount);
    }
}

fn buildFromProjectionAlloc(alloc: Allocator, projection: Projection, options: BuildOptions) !BuildResult {
    try admitPeakMemory(projection, options, 1);
    try chargeKernelWork(options, projection);

    const kernel_options = kernelOptions(options);
    const topology = projection.topology orelse return error.InvalidGraphMetricBuildOptions;
    var result = switch (options.config.kind) {
        .degree => try metrics.degreeTopologyAlloc(alloc, topology, kernel_options),
        .pagerank => try metrics.pageRankTopologyAlloc(alloc, topology, kernel_options),
        .eigenvector => try metrics.eigenvectorTopologyAlloc(alloc, topology, kernel_options),
        .hits_authority, .hits_hub => blk: {
            const pair = try metrics.hitsTopologyAlloc(alloc, topology, kernel_options);
            const selected = if (options.config.kind == .hits_authority) pair.authorities else pair.hubs;
            const unused = if (options.config.kind == .hits_authority) pair.hubs else pair.authorities;
            alloc.free(unused);
            break :blk metrics.Result{ .scores = selected, .iterations_completed = pair.iterations_completed, .converged = pair.converged, .delta = pair.delta };
        },
    };
    defer result.deinit(alloc);
    return try encodeMetricResultAlloc(alloc, projection.node_ids.items, options, result);
}

fn buildFromTopologyAlloc(
    alloc: Allocator,
    topology: CompiledTopology,
    options: BuildOptions,
) !BuildResult {
    var projection = try buildProjectionFromTopologyAlloc(alloc, topology, 0, options);
    defer projection.deinit(alloc);
    projection.decoded_retained_bytes = topology.retained_bytes;
    try chargeProjectionWork(options, projection);
    return try buildFromProjectionAlloc(alloc, projection, options);
}

fn publishHitsPairFromProjectionAlloc(
    alloc: Allocator,
    artifacts: *artifact_store.ArtifactStore,
    projection: Projection,
    first_options: BuildOptions,
    second_config: graph_mod.GraphMetricConfig,
    cancellation: CancellationToken,
) ![2]artifact_ref.ArtifactRef {
    if (!graph_mod.graphMetricHitsPairCompatible(first_options.config, second_config)) return error.InvalidGraphMetricBuildOptions;
    // Both vectors are computed once, while immutable artifacts are encoded
    // and published one at a time. This makes output memory independent of the
    // number of paired HITS lanes.
    try admitPeakMemory(projection, first_options, 1);
    try chargeKernelWork(first_options, projection);

    const topology = projection.topology orelse return error.InvalidGraphMetricBuildOptions;
    var pair = try metrics.hitsTopologyAlloc(alloc, topology, kernelOptions(first_options));
    defer pair.deinit(alloc);
    const first_scores = if (first_options.config.kind == .hits_authority) pair.authorities else pair.hubs;
    const second_scores = if (second_config.kind == .hits_authority) pair.authorities else pair.hubs;
    const first_result = metrics.Result{ .scores = first_scores, .iterations_completed = pair.iterations_completed, .converged = pair.converged, .delta = pair.delta };
    const second_result = metrics.Result{ .scores = second_scores, .iterations_completed = pair.iterations_completed, .converged = pair.converged, .delta = pair.delta };
    const first_ref = blk: {
        var first = try encodeMetricResultAlloc(alloc, projection.node_ids.items, first_options, first_result);
        defer first.deinit(alloc);
        break :blk try putBuildResultAlloc(alloc, artifacts, &first, cancellation);
    };
    errdefer freeArtifactRef(alloc, first_ref);
    if (first_options.config.kind == .hits_authority) {
        alloc.free(pair.authorities);
        pair.authorities = @constCast(&[_]f64{});
    } else {
        alloc.free(pair.hubs);
        pair.hubs = @constCast(&[_]f64{});
    }

    var second_options = first_options;
    second_options.config = second_config;
    const second_ref = blk: {
        var second = try encodeMetricResultAlloc(alloc, projection.node_ids.items, second_options, second_result);
        defer second.deinit(alloc);
        break :blk try putBuildResultAlloc(alloc, artifacts, &second, cancellation);
    };
    return .{ first_ref, second_ref };
}

fn encodeMetricResultAlloc(
    alloc: Allocator,
    node_ids: []const []const u8,
    options: BuildOptions,
    result: metrics.Result,
) !BuildResult {
    const scores = try makeScoresAlloc(alloc, node_ids, result.scores, options.cancellation);
    var scores_owned = true;
    defer if (scores_owned) alloc.free(scores);

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
    try (ComputeRuntime{ .io = options.io, .max_parallelism = options.max_parallelism }).validate();
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
        .owns_score_node_ids = false,
    };
}

fn makeScoresAlloc(alloc: Allocator, node_ids: []const []const u8, values: []const f64, cancellation: CancellationToken) ![]metric_segment.Score {
    if (node_ids.len != values.len) return error.InvalidGraphMetricScore;
    try cancellation.check();
    const scores = try alloc.alloc(metric_segment.Score, node_ids.len);
    errdefer alloc.free(scores);
    for (node_ids, values, 0..) |node_id, value, i| {
        if (i % 4096 == 0) try cancellation.check();
        if (!std.math.isFinite(value)) return error.InvalidGraphMetricScore;
        scores[i] = .{ .node_id = @constCast(node_id), .value = value };
    }
    try cancellation.check();
    // Canonical graph artifacts already carry node IDs in lexical order and
    // the compiled projection preserves that order. Keep the fallback for the
    // legacy direct-projection test seam, but avoid an O(V log V) sort in the
    // production path.
    if (!std.sort.isSorted(metric_segment.Score, scores, {}, lessScore)) {
        std.mem.sort(metric_segment.Score, scores, {}, lessScore);
    }
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

test "serverless graph metric decode admission includes the live source payload" {
    try admitGraphDecodePeak(10, 20, 30);
    try std.testing.expectError(error.GraphMetricBuildBudgetExceeded, admitGraphDecodePeak(10, 20, 29));
    try std.testing.expectError(
        error.GraphMetricBuildBudgetExceeded,
        admitGraphDecodePeak(std.math.maxInt(usize), 1, std.math.maxInt(usize)),
    );
}

test "serverless graph metric topology does not alias qualified endpoints with local nodes" {
    const alloc = std.testing.allocator;
    var graph = graph_segment.Segment{
        .neighbor_tables = try alloc.alloc([]u8, 1),
        .adjacencies = try alloc.alloc(graph_segment.Adjacency, 2),
    };
    defer graph.deinit(alloc);
    graph.neighbor_tables[0] = try alloc.dupe(u8, "entities");
    graph.adjacencies[0] = .{
        .node_id = try alloc.dupe(u8, "source"),
        .out_edges = try alloc.alloc(graph_segment.Edge, 2),
        .in_edges = try alloc.alloc(graph_segment.Edge, 0),
    };
    graph.adjacencies[0].out_edges[0] = .{
        .neighbor_id = try alloc.dupe(u8, "shared"),
        .edge_type = try alloc.dupe(u8, "external"),
        .weight = 1,
        .neighbor_table_id = 0,
    };
    graph.adjacencies[0].out_edges[1] = .{
        .neighbor_id = try alloc.dupe(u8, "shared"),
        .edge_type = try alloc.dupe(u8, "local"),
        .weight = 1,
    };
    graph.adjacencies[1] = .{
        .node_id = try alloc.dupe(u8, "shared"),
        .out_edges = try alloc.alloc(graph_segment.Edge, 0),
        .in_edges = try alloc.alloc(graph_segment.Edge, 1),
    };
    graph.adjacencies[1].in_edges[0] = .{
        .neighbor_id = try alloc.dupe(u8, "source"),
        .edge_type = try alloc.dupe(u8, "local"),
        .weight = 1,
    };

    var topology = try compileTopologyAlloc(alloc, graph, .none);
    defer topology.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), topology.edges.len);
    try std.testing.expectEqual(@as(usize, 1), topology.edge_types.len);
    try std.testing.expectEqualStrings("local", topology.edge_types[0]);

    var projection = try buildProjectionFromTopologyAlloc(alloc, topology, 0, .{
        .graph_index_name = "graph",
        .config = .{ .name = "degree", .kind = .degree },
        .source_graph = .{ .kind = .graph_segment, .name = "graph", .artifact_id = "sha256:placeholder", .byte_len = 1, .checksum = "placeholder" },
    });
    defer projection.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), projection.node_ids.items.len);
    try std.testing.expectEqual(@as(usize, 1), projection.edgeCount());
}

test "serverless graph metric topology admission tracks distinct edge types" {
    const alloc = std.testing.allocator;
    const edges = try alloc.alloc(graph_segment.Edge, 20_000);
    defer alloc.free(edges);
    for (edges) |*edge| edge.* = .{
        .neighbor_id = @constCast("b"),
        .edge_type = @constCast("repeated"),
        .weight = 1,
    };
    var adjacencies = [_]graph_segment.Adjacency{
        .{ .node_id = @constCast("a"), .out_edges = edges, .in_edges = @constCast(&.{}) },
        .{ .node_id = @constCast("b"), .out_edges = @constCast(&.{}), .in_edges = @constCast(&.{}) },
    };
    const graph = graph_segment.Segment{ .adjacencies = &adjacencies };
    var topology = try compileTopologyWithinBudgetAlloc(alloc, graph, 0, 1024 * 1024, .none);
    defer topology.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), topology.edge_types.len);
    try std.testing.expectEqual(edges.len, topology.edges.len);
    try std.testing.expectError(
        error.GraphMetricBuildBudgetExceeded,
        compileTopologyWithinBudgetAlloc(alloc, graph, 0, 1024, .none),
    );
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

    var admitted_projection = try buildProjectionAlloc(alloc, graph, .{
        .graph_index_name = "graph",
        .config = .{ .name = "degree", .kind = .degree },
        .source_graph = source,
        .limits = .{ .max_edges = edges.len },
    });
    defer admitted_projection.deinit(alloc);
    admitted_projection.decoded_retained_bytes = 1024;
    try std.testing.expectError(error.GraphMetricBuildBudgetExceeded, admitPeakMemory(admitted_projection, .{
        .graph_index_name = "graph",
        .config = .{ .name = "degree", .kind = .degree },
        .source_graph = source,
        .limits = .{ .max_edges = edges.len, .max_peak_memory_bytes = 1024 },
    }, 1));

    const one_output_peak = try estimatedPeakMemoryBytes(admitted_projection, .{
        .graph_index_name = "graph",
        .config = .{ .name = "degree", .kind = .degree },
        .source_graph = source,
        .limits = .{ .max_edges = edges.len },
    }, 1);
    const two_output_peak = try estimatedPeakMemoryBytes(admitted_projection, .{
        .graph_index_name = "graph",
        .config = .{ .name = "degree", .kind = .degree },
        .source_graph = source,
        .limits = .{ .max_edges = edges.len },
    }, 2);
    try std.testing.expect(two_output_peak > one_output_peak);
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
    // 604 HITS kernel work items plus the fifteen-item indexed projection.
    const limits = Limits{ .max_work_items = 619, .max_total_work_items = 619 };
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

    const degree_configs = [_]graph_mod.GraphMetricConfig{
        .{ .name = "degree_a", .kind = .degree },
        .{ .name = "degree_b", .kind = .degree },
    };
    // Fifteen projection/index work items are charged once for the shared edge
    // scope; each degree kernel then consumes two more. Rebuilding the
    // projection per metric would exceed this exact 19-item budget.
    const shared_projection_limits = Limits{ .max_work_items = 19, .max_total_work_items = 19 };
    const degree_published = try publishManyFromGraphArtifactAlloc(
        alloc,
        &artifacts,
        "graph",
        source,
        &degree_configs,
        .none,
        shared_projection_limits,
        .{ .published_generation = 1, .edge_generation = 1, .computed_at_ms = 1 },
    );
    defer {
        for (degree_published) |ref| freeArtifactRef(alloc, ref);
        alloc.free(degree_published);
    }
    for (degree_published) |ref| {
        const payload = try artifacts.getVerifiedAllocWithCancellationUsingAllocator(alloc, ref.artifact_id, ref.byte_len, ref.checksum, .none);
        defer alloc.free(payload);
        var decoded = try metric_segment.decodeAlloc(alloc, payload);
        defer decoded.deinit(alloc);
        try std.testing.expectEqual(metric_segment.MaterializationState.ready, decoded.materialization_state);
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
    // One PageRank consumes 254 kernel work items plus fifteen projection
    // items; the table-wide budget admits the first and rejects the second.
    const published = try publishManyFromGraphArtifactAlloc(alloc, &artifacts, "graph", source, &configs, .none, .{ .max_work_items = 269, .max_total_work_items = 269 }, .{ .published_generation = 1, .edge_generation = 1, .computed_at_ms = 1 });
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

    var shared_budget = graph_metric_policy.Budget{ .limits = .{ .max_work_items = 269, .max_total_work_items = 269 } };
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
        .{},
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
        .{},
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
