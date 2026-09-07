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
    /// Optional current-ordinal vectors recovered from the last compatible
    /// publication. They are borrowed for the duration of the build and are
    /// normalized/validated by the storage-independent kernel.
    initial_scores: ?[]const f64 = null,
    initial_authorities: ?[]const f64 = null,
    initial_hubs: ?[]const f64 = null,
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
    /// Bounded, request-scoped projection reuse. Publication visits graph
    /// aliases sequentially, so retaining only the most recent filter keeps
    /// peak memory O(one projection) while eliminating repeated O(V+E) work
    /// for aliases over the same immutable graph.
    cached_projection: ?Projection = null,
    cached_projection_filter: ?graph_mod.GraphMetricEdgeFilter = null,
    cached_projection_requirements: metrics.TopologyRequirements = .{},

    pub fn deinit(self: *PreparedGraphArtifact, alloc: Allocator) void {
        if (self.cached_projection) |*projection| projection.deinit(alloc);
        if (self.cached_projection_filter) |*filter| filter.deinit(alloc);
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
    return metric_segment.artifactNameAlloc(alloc, graph_index_name, metric_name) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.InvalidGraphMetricBuildOptions,
    };
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
    var topology = try prepareTopologyFromPackedAlloc(alloc, graph_payload, options.cancellation, options.limits);
    defer topology.deinit(alloc);
    // This entry point borrows the payload throughout kernel execution.
    topology.retained_bytes = try std.math.add(usize, topology.retained_bytes, graph_payload.len);
    return buildFromTopologyAlloc(alloc, topology, options);
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
        .graph_metric_point_index_checksum = built.artifact.graph_metric_point_index_checksum,
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
    var prepared = prepareGraphArtifactAlloc(alloc, artifacts, source_graph, cancellation, limits) catch |err| switch (err) {
        error.GraphMetricBuildBudgetExceeded => return publishRejectedManyAlloc(alloc, artifacts, graph_index_name, source_graph, configs, cancellation, .build_budget_exceeded, limits, provenance),
        else => return err,
    };
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
    defer alloc.free(graph_payload);
    var topology = try prepareTopologyFromPackedAlloc(alloc, graph_payload, cancellation, limits);
    errdefer topology.deinit(alloc);
    const source_artifact_id = try alloc.dupe(u8, source_graph.artifact_id);
    errdefer alloc.free(source_artifact_id);
    const source_checksum = try alloc.dupe(u8, source_graph.checksum);
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
    prepared: *PreparedGraphArtifact,
    provenance: Provenance,
    runtime: ComputeRuntime,
) ![]artifact_ref.ArtifactRef {
    return publishManyFromPreparedGraphWithWarmStartsAlloc(
        alloc,
        artifacts,
        graph_index_name,
        source_graph,
        configs,
        &.{},
        cancellation,
        limits,
        batch_budget,
        prepared,
        provenance,
        runtime,
    );
}

/// Rebuild variant that may seed iterative kernels from previous immutable
/// metric artifacts. `prior_artifacts`, when non-empty, is aligned with
/// `configs`; every candidate is independently authenticated and rejected as a
/// seed (not as a build) when its format or configuration is incompatible.
pub fn publishManyFromPreparedGraphWithWarmStartsAlloc(
    alloc: Allocator,
    artifacts: *artifact_store.ArtifactStore,
    graph_index_name: []const u8,
    source_graph: artifact_ref.ArtifactRef,
    configs: []const graph_mod.GraphMetricConfig,
    prior_artifacts: []const ?artifact_ref.ArtifactRef,
    cancellation: CancellationToken,
    limits: Limits,
    batch_budget: *graph_metric_policy.Budget,
    prepared: *PreparedGraphArtifact,
    provenance: Provenance,
    runtime: ComputeRuntime,
) ![]artifact_ref.ArtifactRef {
    try validatePublicationOptions(graph_index_name, source_graph, configs, cancellation, limits, batch_budget);
    return publishPreparedComputationsAlloc(alloc, artifacts, graph_index_name, source_graph, configs, prior_artifacts, cancellation, limits, batch_budget, prepared, provenance, runtime);
}

fn inProjectionGroup(candidate: graph_mod.GraphMetricConfig, representative: graph_mod.GraphMetricConfig, share: bool) bool {
    return candidate.edge_filter.equivalent(representative.edge_filter) and
        (share or std.meta.eql(topologyRequirementsForKind(candidate.kind), topologyRequirementsForKind(representative.kind)));
}

/// A no-allocation upper-bound admission pass. Share the union only when all
/// consumers fit; otherwise process cheapest exact requirement groups first.
/// Overestimation can forgo sharing, but cannot reject an affordable metric or
/// spend work building a union which is immediately discarded under pressure.
fn projectionGroupFits(topology: CompiledTopology, configs: []const graph_mod.GraphMetricConfig, processed: []const bool, filter: graph_mod.GraphMetricEdgeFilter, limits: Limits, budget: graph_metric_policy.Budget) bool {
    const n = topology.source_node_count;
    var e: usize = 0;
    for (topology.edge_types, 0..) |edge_type, i| {
        if (filter.mode != .all and !filter.includesType(edge_type)) continue;
        e += topology.edge_type_offsets[i + 1] - topology.edge_type_offsets[i];
    }
    // Covers packed source residency, active/mapping/CSR construction, every
    // kernel's vectors, borrowed score views and one encoded output. Paired
    // HITS outputs are encoded sequentially and share the same peak bound.
    var peak = std.math.add(usize, topology.retained_bytes, 1024 * 1024) catch return false;
    peak = std.math.add(usize, peak, std.math.mul(usize, topology.string_bytes.len, 2) catch return false) catch return false;
    peak = std.math.add(usize, peak, std.math.mul(usize, n, 192) catch return false) catch return false;
    peak = std.math.add(usize, peak, std.math.mul(usize, e, 24) catch return false) catch return false;
    if (peak > limits.max_peak_memory_bytes) return false;
    const projection = graph_metric_policy.projectionWorkItems(n, topology.source_edge_count, n, e, 4) catch return false;
    var work = projection;
    for (configs, processed, 0..) |config, done, i| {
        if (done or !config.edge_filter.equivalent(filter)) continue;
        const paired = for (configs[0..i], processed[0..i]) |prior, prior_done| {
            if (!prior_done and graph_mod.graphMetricHitsPairCompatible(prior, config)) break true;
        } else false;
        if (paired) continue;
        const kernel = graph_metric_policy.metricWorkItems(config.kind, n, e, config.max_iterations) catch return false;
        const individual = std.math.add(u64, projection, kernel) catch return false;
        if (individual > limits.max_work_items) return false;
        work = std.math.add(u64, work, kernel) catch return false;
    }
    return work <= limits.max_total_work_items -| budget.work_items;
}

/// Internal computation groups can span independently validated index aliases.
/// Their names and refresh policies are not a catalog configuration.
fn publishPreparedComputationsAlloc(
    alloc: Allocator,
    artifacts: *artifact_store.ArtifactStore,
    graph_index_name: []const u8,
    source_graph: artifact_ref.ArtifactRef,
    configs: []const graph_mod.GraphMetricConfig,
    prior_artifacts: []const ?artifact_ref.ArtifactRef,
    cancellation: CancellationToken,
    limits: Limits,
    batch_budget: *graph_metric_policy.Budget,
    prepared: *PreparedGraphArtifact,
    provenance: Provenance,
    runtime: ComputeRuntime,
) ![]artifact_ref.ArtifactRef {
    if (configs.len == 0) return try alloc.alloc(artifact_ref.ArtifactRef, 0);
    if (prior_artifacts.len != 0 and prior_artifacts.len != configs.len) return error.InvalidGraphMetricBuildOptions;
    try provenance.validate();
    try runtime.validate();
    if (!prepared.identifies(source_graph)) return error.ArtifactIntegrityMismatch;

    const refs = try alloc.alloc(artifact_ref.ArtifactRef, configs.len);
    errdefer alloc.free(refs);
    const initialized = try alloc.alloc(bool, configs.len);
    defer alloc.free(initialized);
    @memset(initialized, false);
    const processed = try alloc.alloc(bool, configs.len);
    defer alloc.free(processed);
    @memset(processed, false);
    errdefer {
        for (refs, initialized) |ref, ready| if (ready) freeArtifactRef(alloc, ref);
    }
    while (true) {
        const first = for (processed, 0..) |done, i| {
            if (!done) break i;
        } else break;
        try cancellation.check();
        var config = configs[first];
        const share = projectionGroupFits(prepared.topology, configs, processed, config.edge_filter, limits, batch_budget.*);
        if (!share) {
            var cheapest: u64 = std.math.maxInt(u64);
            for (configs, processed) |candidate, done| {
                if (done or !candidate.edge_filter.equivalent(config.edge_filter)) continue;
                const cost = graph_metric_policy.metricWorkItems(candidate.kind, prepared.topology.source_node_count, prepared.topology.edges.len, candidate.max_iterations) catch std.math.maxInt(u64);
                if (cost < cheapest) {
                    cheapest = cost;
                    config = candidate;
                }
            }
        }
        var group_requirements = topologyRequirementsForKind(config.kind);
        if (share) for (configs, processed) |candidate, done| {
            if (done or !candidate.edge_filter.equivalent(config.edge_filter)) continue;
            group_requirements = group_requirements.merge(topologyRequirementsForKind(candidate.kind));
        };
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
        const projection_result = preparedProjectionAlloc(alloc, prepared, group_options) catch |err| switch (err) {
            error.GraphMetricBuildBudgetExceeded => {
                for (configs, 0..) |candidate, candidate_index| {
                    if (processed[candidate_index] or !inProjectionGroup(candidate, config, share)) continue;
                    refs[candidate_index] = try publishRejectedAlloc(alloc, artifacts, graph_index_name, source_graph, candidate, cancellation, .build_budget_exceeded, limits, provenance);
                    initialized[candidate_index] = true;
                    processed[candidate_index] = true;
                }
                continue;
            },
            else => return err,
        };
        const projection = projection_result.projection;

        for (configs, 0..) |candidate, candidate_index| {
            if (processed[candidate_index] or !inProjectionGroup(candidate, config, share)) continue;
            try cancellation.check();
            const reused = for (configs, initialized, 0..) |prior, ready, prior_index| {
                if (ready and sameComputation(prior, candidate)) break prior_index;
            } else null;
            if (reused) |prior_index| {
                refs[candidate_index] = try aliasRefAlloc(alloc, refs[prior_index], graph_index_name, candidate.name, provenance);
                initialized[candidate_index] = true;
                processed[candidate_index] = true;
                continue;
            }
            var options = group_options;
            options.config = candidate;
            const projection_resident_bytes = try projectionResidentMemoryBytes(projection.*);
            const cold_peak_bytes = try estimatedPeakMemoryBytes(projection.*, options, 1);
            if (findCompatibleHitsPairIndex(configs, processed, candidate_index)) |pair_index| {
                // The pair still shares one topology and kernel execution,
                // but spectral vectors always use canonical cold seeds.
                const pair_refs = publishHitsPairFromProjectionAlloc(
                    alloc,
                    artifacts,
                    projection.*,
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
            admitProjectionKernel(projection.*, options) catch |err| switch (err) {
                error.GraphMetricBuildBudgetExceeded => {
                    refs[candidate_index] = try publishRejectedAlloc(alloc, artifacts, graph_index_name, source_graph, candidate, cancellation, .build_budget_exceeded, limits, provenance);
                    initialized[candidate_index] = true;
                    processed[candidate_index] = true;
                    continue;
                },
                else => return err,
            };
            const warm_seed: ?[]f64 = if (prior_artifacts.len == 0)
                null
            else
                try warmStartVectorAlloc(alloc, artifacts, prior_artifacts[candidate_index], projection.node_ids.items, candidate, cancellation, limits, projection_resident_bytes, cold_peak_bytes, options.batch_budget);
            defer if (warm_seed) |seed| alloc.free(seed);
            switch (candidate.kind) {
                .pagerank, .eigenvector => options.initial_scores = warm_seed,
                .hits_authority => options.initial_authorities = warm_seed,
                .hits_hub => options.initial_hubs = warm_seed,
                .degree => {},
            }
            var built = buildAdmittedProjectionAlloc(alloc, projection.*, options) catch |err| switch (err) {
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

pub const PublicationRequest = struct {
    graph_index_name: []const u8,
    source_graph: artifact_ref.ArtifactRef,
    config: graph_mod.GraphMetricConfig,
    prior_artifact: ?artifact_ref.ArtifactRef = null,
    provenance: Provenance,
};

fn sameSource(a: artifact_ref.ArtifactRef, b: artifact_ref.ArtifactRef) bool {
    return a.kind == b.kind and a.byte_len == b.byte_len and
        std.mem.eql(u8, a.artifact_id, b.artifact_id) and std.mem.eql(u8, a.checksum, b.checksum);
}

pub fn sameComputation(a: graph_mod.GraphMetricConfig, b: graph_mod.GraphMetricConfig) bool {
    // Exact equality, not a truncated storage fingerprint. Names and refresh
    // scheduling do not affect immutable metric computation.
    return a.kind == b.kind and @as(u64, @bitCast(a.damping)) == @as(u64, @bitCast(b.damping)) and
        @as(u64, @bitCast(a.tolerance)) == @as(u64, @bitCast(b.tolerance)) and
        a.max_iterations == b.max_iterations and a.edge_filter.equivalent(b.edge_filter);
}

fn aliasRefAlloc(alloc: Allocator, original: artifact_ref.ArtifactRef, index_name: []const u8, metric_name: []const u8, provenance: Provenance) !artifact_ref.ArtifactRef {
    var ref = original;
    ref.name = try metric_segment.artifactNameAlloc(alloc, index_name, metric_name);
    errdefer alloc.free(ref.name);
    ref.artifact_id = try alloc.dupe(u8, original.artifact_id);
    errdefer alloc.free(ref.artifact_id);
    ref.checksum = try alloc.dupe(u8, original.checksum);
    ref.published_generation = provenance.published_generation;
    ref.edge_generation = provenance.edge_generation;
    ref.computed_at_ms = provenance.computed_at_ms;
    return ref;
}

/// Plan a whole publication, not one named index at a time. Stable source and
/// filter groups keep exactly one decoded source and one projection resident.
/// Each distinct kernel is encoded/uploaded once; only lightweight, independently
/// named/provenanced references fan out. Results retain the caller's order.
pub fn publishRequestsAlloc(
    alloc: Allocator,
    artifacts: *artifact_store.ArtifactStore,
    requests: []const PublicationRequest,
    cancellation: CancellationToken,
    limits: Limits,
    budget: *graph_metric_policy.Budget,
    runtime: ComputeRuntime,
) ![]artifact_ref.ArtifactRef {
    return publishRequestsWithPriorAlloc(alloc, artifacts, requests, &.{}, cancellation, limits, budget, runtime);
}

/// The previous manifest is the durable admission-plan witness. Compare the
/// complete ordered plan, not just dirty metrics: removing/changing a sibling
/// can make a previously rejected computation affordable. Provenance alone is
/// not a plan change, and stable rejections must not turn into retry hot loops.
fn admissionPlanUnchanged(alloc: Allocator, requests: []const PublicationRequest, previous: []const artifact_ref.ArtifactRef, limits: Limits) !bool {
    var index: usize = 0;
    for (previous) |prior| {
        if (prior.kind != .graph_metric_segment) continue;
        if (index == requests.len) return false;
        const request = requests[index];
        const name = try artifactNameAlloc(alloc, request.graph_index_name, request.config.name);
        defer alloc.free(name);
        if (!std.mem.eql(u8, name, prior.name) or !priorIdentifiesComputation(prior, request, limits)) return false;
        index += 1;
    }
    return index == requests.len;
}

fn priorIdentifiesComputation(prior: artifact_ref.ArtifactRef, request: PublicationRequest, limits: Limits) bool {
    const source_checksum = artifact_store.sha256DigestFromChecksum(request.source_graph.checksum) catch return false;
    return prior.kind == .graph_metric_segment and prior.metadata_version == metric_segment.wire_version and
        prior.byte_len > 0 and prior.byte_len <= limits.max_metric_payload_bytes and
        prior.materializer_fingerprint == materializerFingerprint(limits) and
        prior.graph_metric_config_fingerprint == configFingerprint(request.config) and
        std.mem.eql(u8, &prior.graph_metric_source_checksum, &source_checksum);
}

/// One authenticated header per immutable prior payload, shared by aliases.
/// Failed verification is memoized too, so missing/corrupt aliases cannot
/// multiply origin requests before the canonical rebuild plan takes over.
const PriorInventory = struct {
    const Entry = struct { ref: artifact_ref.ArtifactRef, prefix: ?[]u8 };
    budget: *graph_metric_policy.Budget,
    entries: std.ArrayListUnmanaged(Entry) = .empty,

    fn deinit(self: *PriorInventory, alloc: Allocator) void {
        for (self.entries.items) |entry| if (entry.prefix) |prefix| alloc.free(prefix);
        self.entries.deinit(alloc);
    }

    fn header(self: *PriorInventory, alloc: Allocator, artifacts: *artifact_store.ArtifactStore, prior: artifact_ref.ArtifactRef, request: PublicationRequest, cancellation: CancellationToken) !?metric_segment.codec.Header {
        for (self.entries.items) |entry| if (sameSource(entry.ref, prior)) {
            return if (entry.prefix) |prefix| metric_segment.decodeHeader(prefix) catch null else null;
        };
        try self.entries.ensureUnusedCapacity(alloc, 1);
        var remaining = self.budget.limits.max_total_reuse_read_bytes -| self.budget.reuse_read_bytes;
        const before = remaining;
        defer self.budget.reuse_read_bytes += before - remaining;
        const prefix = read: {
            const len = try metric_segment.headerProbeLen(prior.byte_len, request.source_graph.artifact_id, request.source_graph.checksum);
            // The range verifier authenticates and pins the full object on a
            // cold miss. A separate verify call would duplicate provider HEADs.
            break :read artifacts.getVerifiedRangeAllocWithBudget(alloc, prior.artifact_id, prior.byte_len, prior.checksum, 0, len, cancellation, &remaining) catch |err| switch (err) {
                error.ArtifactReadBudgetExceeded, error.FileNotFound, error.InvalidArtifactId, error.InvalidRange, error.ArtifactIntegrityMismatch, error.ArtifactIdentityUnavailable => null,
                else => return err,
            };
        };
        self.entries.appendAssumeCapacity(.{ .ref = prior, .prefix = prefix });
        return if (prefix) |bytes| metric_segment.decodeHeader(bytes) catch null else null;
    }

    fn find(self: *PriorInventory, alloc: Allocator, artifacts: *artifact_store.ArtifactStore, previous: []const artifact_ref.ArtifactRef, request: PublicationRequest, reuse_rejections: bool, limits: Limits, cancellation: CancellationToken) !?artifact_ref.ArtifactRef {
        const name = try artifactNameAlloc(alloc, request.graph_index_name, request.config.name);
        defer alloc.free(name);
        // A valid ready computation always wins over a terminal rejection.
        for ([_]artifact_ref.GraphMetricMaterializationState{ .ready, .rejected }) |state| {
            if (state == .rejected and !reuse_rejections) continue;
            for ([_]bool{ true, false }) |same_name| {
                for (previous) |prior| {
                    try cancellation.check();
                    if (std.mem.eql(u8, name, prior.name) != same_name) continue;
                    if (prior.graph_metric_materialization_state != state or !priorIdentifiesComputation(prior, request, limits)) continue;
                    const decoded = (try self.header(alloc, artifacts, prior, request, cancellation)) orelse continue;
                    if (decoded.version != metric_segment.wire_version or decoded.kind != request.config.kind or
                        decoded.config_fingerprint != configFingerprint(request.config) or
                        decoded.materializer_fingerprint != materializerFingerprint(limits) or
                        @intFromEnum(decoded.materialization_state) != @intFromEnum(state) or
                        @intFromEnum(decoded.rejection_reason) != @intFromEnum(prior.graph_metric_rejection_reason) or
                        !std.mem.eql(u8, decoded.source_graph_artifact_id, request.source_graph.artifact_id) or
                        !std.mem.eql(u8, decoded.source_graph_checksum, request.source_graph.checksum)) continue;
                    return prior;
                }
            }
        }
        return null;
    }
};

pub fn publishRequestsWithPriorAlloc(
    alloc: Allocator,
    artifacts: *artifact_store.ArtifactStore,
    requests: []const PublicationRequest,
    previous: []const artifact_ref.ArtifactRef,
    cancellation: CancellationToken,
    limits: Limits,
    budget: *graph_metric_policy.Budget,
    runtime: ComputeRuntime,
) ![]artifact_ref.ArtifactRef {
    try graph_metric_policy.validateCatalogFanout(0, requests.len, limits);
    if (!std.meta.eql(budget.limits, limits)) return error.InvalidGraphMetricBuildOptions;
    try runtime.validate();
    for (requests) |request| {
        try request.provenance.validate();
        try graph_metric_policy.validateConfigs(&.{request.config}, limits);
        try graph_mod.validateGraphMetricEdgeFilters(&.{}, &.{request.config});
        if (request.graph_index_name.len == 0 or request.graph_index_name.len > limits.max_graph_index_name_bytes or
            request.source_graph.kind != .graph_segment or request.source_graph.byte_len == 0)
            return error.InvalidGraphMetricBuildOptions;
    }
    const refs = try alloc.alloc(artifact_ref.ArtifactRef, requests.len);
    errdefer alloc.free(refs);
    const ready = try alloc.alloc(bool, requests.len);
    defer alloc.free(ready);
    @memset(ready, false);
    errdefer for (refs, ready) |ref, initialized| if (initialized) freeArtifactRef(alloc, ref);
    const reuse_rejections = try admissionPlanUnchanged(alloc, requests, previous, limits);
    var inventory = PriorInventory{ .budget = budget };
    defer inventory.deinit(alloc);
    for (requests, refs, ready) |request, *ref, *initialized| {
        if (try inventory.find(alloc, artifacts, previous, request, reuse_rejections, limits, cancellation)) |prior| {
            ref.* = try aliasRefAlloc(alloc, prior, request.graph_index_name, request.config.name, request.provenance);
            // A new alias publishes now but did not recompute the existing
            // vector. Keep its real computation timestamp.
            if (prior.computed_at_ms != 0) ref.computed_at_ms = prior.computed_at_ms;
            if (std.mem.eql(u8, prior.name, ref.name) and prior.published_generation >= request.provenance.edge_generation)
                ref.published_generation = prior.published_generation;
            initialized.* = true;
        }
    }
    var configs = std.ArrayListUnmanaged(graph_mod.GraphMetricConfig).empty;
    defer configs.deinit(alloc);
    var priors = std.ArrayListUnmanaged(?artifact_ref.ArtifactRef).empty;
    defer priors.deinit(alloc);
    var mapping = std.ArrayListUnmanaged(usize).empty;
    defer mapping.deinit(alloc);
    for (requests, 0..) |first, first_index| {
        if (ready[first_index]) continue;
        try cancellation.check();
        configs.clearRetainingCapacity();
        priors.clearRetainingCapacity();
        mapping.clearRetainingCapacity();
        for (requests, ready) |request, initialized| {
            if (initialized or !sameSource(first.source_graph, request.source_graph)) continue;
            const existing = for (configs.items, 0..) |config, i| {
                if (sameComputation(config, request.config)) break i;
            } else null;
            const index = existing orelse configs.items.len;
            if (existing == null) {
                var config = request.config;
                config.refresh = .background;
                try configs.append(alloc, config);
                try priors.append(alloc, request.prior_artifact);
            } else if (priors.items[index] == null) {
                // The first available seed in stable publication order wins.
                // Seed authentication/compatibility is still mandatory.
                priors.items[index] = request.prior_artifact;
            }
            try mapping.append(alloc, index);
        }
        const built = build: {
            var prepared = prepare: {
                if (first.source_graph.byte_len > limits.max_graph_payload_bytes) break :prepare null;
                budget.chargeGraphPayload(first.source_graph.artifact_id, first.source_graph.checksum, first.source_graph.byte_len) catch break :prepare null;
                break :prepare prepareGraphArtifactAlloc(alloc, artifacts, first.source_graph, cancellation, limits) catch |err| switch (err) {
                    error.GraphMetricBuildBudgetExceeded => null,
                    else => return err,
                };
            };
            if (prepared) |*graph| {
                defer graph.deinit(alloc);
                break :build try publishPreparedComputationsAlloc(alloc, artifacts, first.graph_index_name, first.source_graph, configs.items, priors.items, cancellation, limits, budget, graph, first.provenance, runtime);
            }
            const rejected = try alloc.alloc(artifact_ref.ArtifactRef, configs.items.len);
            errdefer alloc.free(rejected);
            var count: usize = 0;
            errdefer for (rejected[0..count]) |ref| freeArtifactRef(alloc, ref);
            for (configs.items, rejected) |config, *ref| {
                ref.* = try publishRejectedAlloc(alloc, artifacts, first.graph_index_name, first.source_graph, config, cancellation, .build_budget_exceeded, limits, first.provenance);
                count += 1;
            }
            break :build rejected;
        };
        defer {
            for (built) |ref| freeArtifactRef(alloc, ref);
            alloc.free(built);
        }
        var mapped: usize = 0;
        for (requests, ready, refs) |request, *initialized, *ref| {
            if (initialized.* or !sameSource(first.source_graph, request.source_graph)) continue;
            ref.* = try aliasRefAlloc(alloc, built[mapping.items[mapped]], request.graph_index_name, request.config.name, request.provenance);
            initialized.* = true;
            mapped += 1;
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
        .graph_metric_point_index_checksum = built.artifact.graph_metric_point_index_checksum,
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
    var segment = try rejectedSegmentAlloc(alloc, source_graph, config, reason, limits, provenance);
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
    source_graph: artifact_ref.ArtifactRef,
    config: graph_mod.GraphMetricConfig,
    reason: metric_segment.RejectionReason,
    limits: Limits,
    provenance: Provenance,
) !metric_segment.Segment {
    const owned_source_graph_artifact_id = try alloc.dupe(u8, source_graph.artifact_id);
    errdefer alloc.free(owned_source_graph_artifact_id);
    const owned_source_graph_checksum = try alloc.dupe(u8, source_graph.checksum);
    errdefer alloc.free(owned_source_graph_checksum);
    var edge_filter = try cloneSortedEdgeFilterAlloc(alloc, config.edge_filter);
    errdefer edge_filter.deinit(alloc);
    const scores = try alloc.alloc(metric_segment.Score, 0);
    errdefer alloc.free(scores);
    const segment = metric_segment.Segment{
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

/// Directly consumes persisted ordinals. No edge-string allocations, node
/// hashing, or reverse-adjacency materialization occur in this path.
fn prepareTopologyFromPackedAlloc(alloc: Allocator, payload: []const u8, cancellation: CancellationToken, limits: Limits) !CompiledTopology {
    if (payload.len >= limits.max_peak_memory_bytes) return error.GraphMetricBuildBudgetExceeded;
    var limiter = try bounded_decode.AllocationLimiter.init(alloc, limits.max_peak_memory_bytes - payload.len);
    return compilePackedTopologyAlloc(limiter.allocator(), payload, cancellation, limits) catch |err| {
        if ((err == error.OutOfMemory and limiter.limit_exceeded) or err == error.DecodedArtifactTooLarge)
            return error.GraphMetricBuildBudgetExceeded;
        return err;
    };
}

fn compilePackedTopologyAlloc(alloc: Allocator, payload: []const u8, cancellation: CancellationToken, limits: Limits) !CompiledTopology {
    const compact = graph_segment.codec.compact;
    var graph = try compact.viewAlloc(alloc, payload, .{}, cancellation);
    defer graph.deinit(alloc);
    if (graph.adjacencies.len > limits.max_nodes) return error.GraphMetricBuildBudgetExceeded;
    const missing = std.math.maxInt(u32);
    const ordinals = try alloc.alloc(u32, graph.nodes.len);
    defer alloc.free(ordinals);
    @memset(ordinals, missing);
    var node_ids = std.ArrayListUnmanaged([]const u8).empty;
    errdefer node_ids.deinit(alloc);
    try node_ids.ensureTotalCapacity(alloc, graph.adjacencies.len);
    for (graph.adjacencies, 0..) |adjacency, i| {
        if (i % 256 == 0) try cancellation.check();
        if (ordinals[adjacency.node] != missing) return error.InvalidGraphMetricBuildOptions;
        ordinals[adjacency.node] = @intCast(i);
        node_ids.appendAssumeCapacity(graph.nodes[adjacency.node]);
    }
    var edge_types = std.ArrayListUnmanaged([]const u8).empty;
    errdefer edge_types.deinit(alloc);
    try edge_types.appendSlice(alloc, graph.edge_types);
    const edge_type_offsets = try alloc.alloc(u32, graph.edge_types.len + 1);
    errdefer alloc.free(edge_type_offsets);
    @memset(edge_type_offsets, 0);
    var source_edge_count: usize = 0;
    var local_edge_count: usize = 0;
    for (graph.adjacencies) |adjacency| {
        source_edge_count = std.math.add(usize, source_edge_count, adjacency.out.len / compact.edge_len) catch return error.GraphMetricBuildBudgetExceeded;
        if (source_edge_count > limits.max_edges) return error.GraphMetricBuildBudgetExceeded;
        for (0..adjacency.out.len / compact.edge_len) |i| {
            if (i % 4096 == 0) try cancellation.check();
            const edge = compact.readEdge(adjacency.out, i);
            if (edge.table != null) continue;
            if (ordinals[edge.node] == missing) return error.InvalidGraphMetricBuildOptions;
            edge_type_offsets[edge.edge_type + 1] = std.math.add(u32, edge_type_offsets[edge.edge_type + 1], 1) catch return error.GraphMetricBuildBudgetExceeded;
            local_edge_count += 1;
        }
    }
    for (1..edge_type_offsets.len) |i| edge_type_offsets[i] = std.math.add(u32, edge_type_offsets[i - 1], edge_type_offsets[i]) catch return error.GraphMetricBuildBudgetExceeded;
    const owned_edges = try alloc.alloc(CompiledEdge, local_edge_count);
    errdefer alloc.free(owned_edges);
    const cursors = try alloc.dupe(u32, edge_type_offsets[0..graph.edge_types.len]);
    defer alloc.free(cursors);
    for (graph.adjacencies, 0..) |adjacency, source| {
        for (0..adjacency.out.len / compact.edge_len) |i| {
            if (i % 4096 == 0) try cancellation.check();
            const edge = compact.readEdge(adjacency.out, i);
            if (edge.table != null) continue;
            owned_edges[cursors[edge.edge_type]] = .{ .source = @intCast(source), .target = ordinals[edge.node] };
            cursors[edge.edge_type] += 1;
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

/// Benchmark/reference oracle only. Both paths read the same current wire;
/// the reference intentionally recreates the former unpack/hash preparation.
pub fn benchmarkPreparation(alloc: Allocator, payload: []const u8, reference: bool) !usize {
    if (!reference) {
        var topology = try prepareTopologyFromPackedAlloc(alloc, payload, .none, .{});
        defer topology.deinit(alloc);
        return topology.edges.len;
    }
    var graph = try graph_segment.decodeAlloc(alloc, payload);
    defer graph.deinit(alloc);
    _ = try discardInboundEdges(alloc, &graph);
    var topology = try compileTopologyWithinBudgetAlloc(alloc, graph, 0, 1024 * 1024 * 1024, .none);
    defer topology.deinit(alloc);
    return topology.edges.len;
}

/// Source preparation plus sixteen exhausted projection groups. The oracle
/// models the former build-then-charge ordering; neither path runs a kernel.
pub fn benchmarkRejectedPreparation(alloc: Allocator, payload: []const u8, reference: bool) !usize {
    var topology = try prepareTopologyFromPackedAlloc(alloc, payload, .none, .{});
    defer topology.deinit(alloc);
    var budget = graph_metric_policy.Budget{ .limits = .{ .max_total_work_items = 0 } };
    const options = BuildOptions{
        .graph_index_name = "bench",
        .config = .{ .name = "degree", .kind = .degree },
        .source_graph = .{ .kind = .graph_segment, .artifact_id = "fixture", .checksum = "fixture", .byte_len = payload.len },
    };
    for (0..16) |_| {
        if (reference) {
            var projection = try buildAdmittedProjectionFromTopologyAlloc(alloc, topology, 0, options);
            projection.deinit(alloc);
        } else {
            var bounded = options;
            bounded.batch_budget = &budget;
            var projection = buildProjectionFromTopologyAlloc(alloc, topology, 0, bounded) catch |err| switch (err) {
                error.GraphMetricBuildBudgetExceeded => continue,
                else => return err,
            };
            projection.deinit(alloc);
            return error.InvalidBenchmarkResult;
        }
    }
    return topology.edges.len;
}

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
    const retained = std.math.add(usize, decoded_retained_bytes, topology.retained_bytes) catch
        return error.GraphMetricBuildBudgetExceeded;
    if (retained >= options.limits.max_peak_memory_bytes) return error.GraphMetricBuildBudgetExceeded;
    // Charge the census before touching edges, even if the later exact-sized
    // projection cannot fit. Rejected work must never disappear from accounting.
    const census_work = try graph_metric_policy.workItems(topology.source_node_count, topology.source_edge_count, 1, 1);
    if (census_work > options.limits.max_work_items) return error.GraphMetricBuildBudgetExceeded;
    if (options.batch_budget) |budget| try budget.chargeWork(census_work);
    // Unlike an estimate made after the census, this also bounds scratch
    // allocations on every failure path. Result buffers use the backing allocator.
    var limiter = try bounded_decode.AllocationLimiter.init(alloc, options.limits.max_peak_memory_bytes - retained);
    return buildAdmittedProjectionFromTopologyAlloc(limiter.allocator(), topology, decoded_retained_bytes, options) catch |err| switch (err) {
        error.OutOfMemory => if (limiter.limit_exceeded) error.GraphMetricBuildBudgetExceeded else error.OutOfMemory,
        else => err,
    };
}

fn buildAdmittedProjectionFromTopologyAlloc(
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
    if (requirements.incoming == .degrees and requirements.outgoing == .degrees) {
        return try buildDegreeProjectionFromTopologyAlloc(alloc, topology, decoded_retained_bytes, options);
    }
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

    var active_nodes = try std.DynamicBitSetUnmanaged.initEmpty(alloc, topology.node_ids.len);
    defer active_nodes.deinit(alloc);
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
            active_nodes.set(edge.source);
            active_nodes.set(edge.target);
        }
    }
    var projected_node_count: usize = 0;
    for (0..topology.node_ids.len) |node_ordinal| {
        if (!active_nodes.isSet(node_ordinal)) continue;
        projected_node_count = std.math.add(usize, projected_node_count, 1) catch
            return error.GraphMetricBuildBudgetExceeded;
    }
    if (projected_node_count > options.limits.max_nodes) return error.GraphMetricBuildBudgetExceeded;

    try chargeProjectionConstruction(options, topology, projected_node_count, projected_edge_count);

    var construction_peak = std.math.add(usize, decoded_retained_bytes, topology.retained_bytes) catch
        return error.GraphMetricBuildBudgetExceeded;
    try addPeakArrayBytes(&construction_peak, topology.edge_types.len, bool);
    const active_node_word_count = std.math.divCeil(usize, topology.node_ids.len, @bitSizeOf(usize)) catch
        return error.GraphMetricBuildBudgetExceeded;
    try addPeakArrayBytes(&construction_peak, active_node_word_count, usize);
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
    for (topology.node_ids, 0..) |node_id, global_ordinal| {
        if (!active_nodes.isSet(global_ordinal)) continue;
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

/// Degree-only materializations do not need edge ordinals after counting.
/// Count directly from the compiled edge-type runs in one O(E) pass instead
/// of copying projected edges and scanning them twice more to build CSR.
fn buildDegreeProjectionFromTopologyAlloc(
    alloc: Allocator,
    topology: CompiledTopology,
    decoded_retained_bytes: usize,
    options: BuildOptions,
) !Projection {
    if (topology.source_node_count > options.limits.max_nodes or topology.source_edge_count > options.limits.max_edges)
        return error.GraphMetricBuildBudgetExceeded;
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

    var active_nodes = try std.DynamicBitSetUnmanaged.initEmpty(alloc, topology.node_ids.len);
    defer active_nodes.deinit(alloc);
    const incoming_counts = try alloc.alloc(u32, topology.node_ids.len);
    defer alloc.free(incoming_counts);
    const outgoing_counts = try alloc.alloc(u32, topology.node_ids.len);
    defer alloc.free(outgoing_counts);
    @memset(incoming_counts, 0);
    @memset(outgoing_counts, 0);
    var projected_edge_count: usize = 0;
    for (allowed_edge_types, 0..) |allowed, edge_type_id| {
        if (!allowed) continue;
        const range_start: usize = @intCast(topology.edge_type_offsets[edge_type_id]);
        const range_end: usize = @intCast(topology.edge_type_offsets[edge_type_id + 1]);
        if (range_start > range_end or range_end > topology.edges.len) return error.InvalidGraphMetricBuildOptions;
        for (topology.edges[range_start..range_end]) |edge| {
            projected_edge_count = std.math.add(usize, projected_edge_count, 1) catch
                return error.GraphMetricBuildBudgetExceeded;
            if (projected_edge_count > options.limits.max_edges) return error.GraphMetricBuildBudgetExceeded;
            if (projected_edge_count % 4096 == 0) try options.cancellation.check();
            incoming_counts[edge.target] = std.math.add(u32, incoming_counts[edge.target], 1) catch
                return error.GraphMetricBuildBudgetExceeded;
            outgoing_counts[edge.source] = std.math.add(u32, outgoing_counts[edge.source], 1) catch
                return error.GraphMetricBuildBudgetExceeded;
            active_nodes.set(edge.source);
            active_nodes.set(edge.target);
        }
    }
    const projected_node_count = active_nodes.count();
    if (projected_node_count > options.limits.max_nodes) return error.GraphMetricBuildBudgetExceeded;
    try chargeProjectionConstruction(options, topology, projected_node_count, projected_edge_count);
    var construction_peak = std.math.add(usize, decoded_retained_bytes, topology.retained_bytes) catch
        return error.GraphMetricBuildBudgetExceeded;
    try addPeakArrayBytes(&construction_peak, topology.edge_types.len, bool);
    try addPeakArrayBytes(&construction_peak, std.math.divCeil(usize, topology.node_ids.len, @bitSizeOf(usize)) catch
        return error.GraphMetricBuildBudgetExceeded, usize);
    try addPeakArrayBytes(&construction_peak, topology.node_ids.len, u32);
    try addPeakArrayBytes(&construction_peak, topology.node_ids.len, u32);
    try addPeakArrayBytes(&construction_peak, projected_node_count, []const u8);
    try addPeakArrayBytes(&construction_peak, projected_node_count + 1, u32);
    try addPeakArrayBytes(&construction_peak, projected_node_count + 1, u32);
    if (construction_peak > options.limits.max_peak_memory_bytes) return error.GraphMetricBuildBudgetExceeded;

    try projection.node_ids.ensureTotalCapacityPrecise(alloc, projected_node_count);
    const incoming_offsets = try alloc.alloc(u32, projected_node_count + 1);
    errdefer alloc.free(incoming_offsets);
    const outgoing_offsets = try alloc.alloc(u32, projected_node_count + 1);
    errdefer alloc.free(outgoing_offsets);
    incoming_offsets[0] = 0;
    outgoing_offsets[0] = 0;
    var local_index: usize = 0;
    for (topology.node_ids, 0..) |node_id, global_ordinal| {
        if (!active_nodes.isSet(global_ordinal)) continue;
        projection.node_ids.appendAssumeCapacity(node_id);
        projection.node_id_bytes = std.math.add(usize, projection.node_id_bytes, node_id.len) catch
            return error.GraphMetricBuildBudgetExceeded;
        incoming_offsets[local_index + 1] = std.math.add(u32, incoming_offsets[local_index], incoming_counts[global_ordinal]) catch
            return error.GraphMetricBuildBudgetExceeded;
        outgoing_offsets[local_index + 1] = std.math.add(u32, outgoing_offsets[local_index], outgoing_counts[global_ordinal]) catch
            return error.GraphMetricBuildBudgetExceeded;
        local_index += 1;
    }
    std.debug.assert(local_index == projected_node_count);
    projection.topology = .{
        .node_count = projected_node_count,
        .edge_count = projected_edge_count,
        .requirements = .degree,
        .incoming_offsets = incoming_offsets,
        .incoming_sources = @constCast(&[_]u32{}),
        .outgoing_offsets = outgoing_offsets,
        .outgoing_targets = @constCast(&[_]u32{}),
    };
    return projection;
}

const PreparedProjection = struct {
    projection: *Projection,
    built: bool,
};

fn preparedProjectionAlloc(
    alloc: Allocator,
    prepared: *PreparedGraphArtifact,
    options: BuildOptions,
) !PreparedProjection {
    // Prepared artifacts may outlive a single publication request. Re-admit
    // the immutable source topology against every caller's limits before a
    // cache hit can bypass projection construction.
    if (prepared.topology.source_node_count > options.limits.max_nodes or
        prepared.topology.source_edge_count > options.limits.max_edges)
    {
        return error.GraphMetricBuildBudgetExceeded;
    }
    const requirements = options.topology_requirements orelse topologyRequirementsForKind(options.config.kind);
    if (prepared.cached_projection != null and
        prepared.cached_projection_filter.?.equivalent(options.config.edge_filter) and
        prepared.cached_projection_requirements.satisfies(requirements))
    {
        return .{ .projection = &prepared.cached_projection.?, .built = false };
    }

    // Clone the tiny filter identity before releasing the previous projection,
    // then replace the cache in place. A failed rebuild leaves it empty rather
    // than retaining a projection whose requirements do not satisfy the call.
    var filter = try options.config.edge_filter.cloneAlloc(alloc);
    errdefer filter.deinit(alloc);
    if (prepared.cached_projection) |*projection| projection.deinit(alloc);
    prepared.cached_projection = null;
    if (prepared.cached_projection_filter) |*previous_filter| previous_filter.deinit(alloc);
    prepared.cached_projection_filter = null;

    var projection = try buildProjectionFromTopologyAlloc(alloc, prepared.topology, 0, options);
    projection.decoded_retained_bytes = prepared.topology.retained_bytes;
    prepared.cached_projection = projection;
    prepared.cached_projection_filter = filter;
    prepared.cached_projection_requirements = requirements;
    return .{ .projection = &prepared.cached_projection.?, .built = true };
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

fn projectionResidentMemoryBytes(projection: Projection) !usize {
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
    return total;
}

fn estimatedPeakMemoryBytes(projection: Projection, options: BuildOptions, simultaneous_outputs: usize) !usize {
    if (simultaneous_outputs == 0) return error.InvalidGraphMetricBuildOptions;
    var total = try projectionResidentMemoryBytes(projection);

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
    const seed_vectors: usize = @intFromBool(options.initial_scores != null) +
        @intFromBool(options.initial_authorities != null) +
        @intFromBool(options.initial_hubs != null);
    try addPeakBytes(&total, std.math.mul(
        usize,
        projection.node_ids.items.len,
        seed_vectors * @sizeOf(f64),
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
        .initial_scores = options.initial_scores,
        .initial_authorities = options.initial_authorities,
        .initial_hubs = options.initial_hubs,
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

fn chargeProjectionConstruction(options: BuildOptions, topology: CompiledTopology, nodes: usize, edges: usize) !void {
    const requirements = options.topology_requirements orelse topologyRequirementsForKind(options.config.kind);
    const passes: u64 = if (requirements.incoming == .neighbors or requirements.outgoing == .neighbors) 4 else 3;
    const total = try graph_metric_policy.projectionWorkItems(topology.source_node_count, topology.source_edge_count, nodes, edges, passes);
    if (total > options.limits.max_work_items) return error.GraphMetricBuildBudgetExceeded;
    if (options.batch_budget) |budget| try budget.chargeWork(try graph_metric_policy.workItems(nodes, edges, 1, passes));
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

fn admitProjectionKernel(projection: Projection, options: BuildOptions) !void {
    try admitPeakMemory(projection, options, 1);
    try chargeKernelWork(options, projection);
}

fn buildFromProjectionAlloc(alloc: Allocator, projection: Projection, options: BuildOptions) !BuildResult {
    try admitProjectionKernel(projection, options);
    return buildAdmittedProjectionAlloc(alloc, projection, options);
}

fn buildAdmittedProjectionAlloc(alloc: Allocator, projection: Projection, options: BuildOptions) !BuildResult {
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
    ref.graph_metric_point_index_checksum = integrity.point_index_checksum;
    ref.graph_metric_config_fingerprint = segment.config_fingerprint;
    ref.graph_metric_source_checksum = artifact_store.sha256DigestFromChecksum(segment.source_graph_checksum) catch
        return error.ArtifactIntegrityMismatch;
    ref.graph_metric_materialization_state = @enumFromInt(@intFromEnum(segment.materialization_state));
    ref.graph_metric_rejection_reason = @enumFromInt(@intFromEnum(segment.rejection_reason));
}

/// Maps the last published node-sorted vector onto the current canonical node
/// dictionary without materializing the old segment's node IDs. The verified
/// payload is walked block-by-block and a linear merge fills the ordinal seed;
/// added nodes remain zero and deleted nodes are skipped. This keeps warm-start
/// preparation O(V_old + V_new) with one dense output vector.
fn warmStartVectorAlloc(
    alloc: Allocator,
    artifacts: *artifact_store.ArtifactStore,
    prior_artifact: ?artifact_ref.ArtifactRef,
    current_node_ids: []const []const u8,
    config: graph_mod.GraphMetricConfig,
    cancellation: CancellationToken,
    limits: Limits,
    existing_resident_bytes: usize,
    execution_peak_bytes: usize,
    batch_budget: ?*graph_metric_policy.Budget,
) !?[]f64 {
    if (!metrics.warm_start.supported(config.kind) or prior_artifact == null or current_node_ids.len == 0) return null;
    const prior = prior_artifact.?;
    if (prior.kind != .graph_metric_segment or prior.metadata_version != metric_segment.wire_version or
        prior.byte_len == 0 or prior.byte_len > limits.max_metric_payload_bytes or
        prior.graph_metric_materialization_state != .ready or
        prior.graph_metric_config_fingerprint != configFingerprint(config)) return null;
    const seed_bytes = std.math.mul(usize, current_node_ids.len, @sizeOf(f64)) catch return error.GraphMetricBuildBudgetExceeded;
    // Admit both lifetimes before doing optional I/O. A seed that fits during
    // preparation may not fit alongside the kernel and encoded output.
    if (execution_peak_bytes > limits.max_peak_memory_bytes or
        seed_bytes > limits.max_peak_memory_bytes - execution_peak_bytes) return null;
    var retained = std.math.add(u64, prior.byte_len, seed_bytes) catch return error.GraphMetricBuildBudgetExceeded;
    retained = std.math.add(u64, retained, existing_resident_bytes) catch return error.GraphMetricBuildBudgetExceeded;
    // Decoding borrows IDs but allocates primary/ranked routing structs. Their
    // layout is bounded by twice the encoded footer size.
    retained = std.math.add(u64, retained, @as(u64, prior.graph_metric_routing_footer_len) * 2) catch return null;
    if (retained > limits.max_peak_memory_bytes) return null;
    var local_budget = graph_metric_policy.Budget{ .limits = limits };
    const seed_budget = batch_budget orelse &local_budget;
    if (!seed_budget.admitSeed(prior.byte_len, current_node_ids.len)) return null;

    const payload = artifacts.getVerifiedAllocWithCancellationUsingAllocator(
        alloc,
        prior.artifact_id,
        prior.byte_len,
        prior.checksum,
        cancellation,
    ) catch |err| switch (err) {
        error.Canceled, error.OutOfMemory => return err,
        else => {
            // The old vector is an optional accelerator, never an input to
            // the authoritative cold build. Authentication failures must not
            // use the seed, but must not poison fresh topology publication.
            try cancellation.check();
            std.log.warn("graph metric warm start unavailable; using cold seed metric={s} artifact={s} err={s}", .{ config.name, prior.artifact_id, @errorName(err) });
            return null;
        },
    };
    defer alloc.free(payload);
    const header = metric_segment.decodeHeader(payload) catch return null;
    if (header.kind != config.kind or header.materialization_state != .ready or
        header.config_fingerprint != configFingerprint(config)) return null;
    const control = metric_segment.decodeControl(payload, config.edge_filter) catch return null;
    if (control.score_count == 0 or payload.len < metric_segment.routing_trailer_len) return null;
    const footer_len = metric_segment.routingFooterLenFromTrailer(
        payload.len,
        payload[payload.len - metric_segment.routing_trailer_len ..],
    ) catch return null;
    if (footer_len != prior.graph_metric_routing_footer_len) return null;
    var routing = metric_segment.decodeRoutingIndexForVersionWithCancellationAlloc(
        alloc,
        payload[payload.len - footer_len ..],
        payload.len,
        header.version,
        cancellation,
    ) catch |err| switch (err) {
        error.Canceled, error.OutOfMemory => return err,
        else => return null,
    };
    defer routing.deinit(alloc);

    const expected_blocks = control.score_count / metric_segment.score_block_entries +
        @intFromBool(control.score_count % metric_segment.score_block_entries != 0);
    if (routing.entries.len != expected_blocks) return null;
    const seed = try alloc.alloc(f64, current_node_ids.len);
    var seed_owned = true;
    defer if (seed_owned) alloc.free(seed);
    @memset(seed, 0);
    var current_index: usize = 0;
    var matched: usize = 0;
    var positive_sum: f64 = 0;
    for (routing.entries, 0..) |entry, block_index| {
        if (block_index % 32 == 0) try cancellation.check();
        const offset = std.math.cast(usize, entry.offset) orelse return null;
        if (offset > payload.len or entry.len > payload.len - offset) return null;
        const decoded = metric_segment.decodeScoreBlockWithCancellation(payload[offset..][0..entry.len], cancellation) catch |err| switch (err) {
            error.Canceled => return err,
            else => return null,
        };
        var old_index: usize = 0;
        var comparisons: usize = 0;
        while (current_index < current_node_ids.len and old_index < decoded.len) {
            if (comparisons % 4096 == 0) try cancellation.check();
            comparisons += 1;
            switch (decoded.scores[old_index].orderNode(decoded.node_prefix, current_node_ids[current_index])) {
                .lt => old_index += 1,
                .gt => current_index += 1,
                .eq => {
                    const value = decoded.scores[old_index].value;
                    seed[current_index] = value;
                    positive_sum += value;
                    matched += 1;
                    old_index += 1;
                    current_index += 1;
                },
            }
        }
        if (current_index == current_node_ids.len) break;
    }
    if (matched == 0 or !std.math.isFinite(positive_sum) or positive_sum <= 0) {
        return null;
    }
    seed_owned = false;
    return seed;
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
    const source_artifact_id = try alloc.dupe(u8, options.source_graph.artifact_id);
    errdefer alloc.free(source_artifact_id);
    const source_checksum = try alloc.dupe(u8, options.source_graph.checksum);
    errdefer alloc.free(source_checksum);
    var edge_filter = try cloneSortedEdgeFilterAlloc(alloc, options.config.edge_filter);
    errdefer edge_filter.deinit(alloc);
    return .{
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
    try std.testing.expectEqual(@as(usize, 0), projection.ordinals.count());
    const compact = projection.topology.?;
    try std.testing.expectEqualSlices(u32, &.{ 0, 0, 1 }, compact.incoming_offsets);
    try std.testing.expectEqualSlices(u32, &.{ 0, 1, 1 }, compact.outgoing_offsets);
    try std.testing.expectEqual(@as(usize, 0), compact.incoming_sources.len);
    try std.testing.expectEqual(@as(usize, 0), compact.outgoing_targets.len);
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

test "serverless packed topology matches reference kernels across filters and qualified edges" {
    const alloc = std.testing.allocator;
    var out_z = [_]graph_segment.Edge{
        .{ .neighbor_id = @constCast("a"), .edge_type = @constCast("alpha"), .weight = 1 },
        .{ .neighbor_id = @constCast("a"), .edge_type = @constCast("external"), .weight = 1, .neighbor_table_id = 0 },
        .{ .neighbor_id = @constCast("m"), .edge_type = @constCast("zeta"), .weight = 2 },
    };
    var out_a = [_]graph_segment.Edge{
        .{ .neighbor_id = @constCast("m"), .edge_type = @constCast("alpha"), .weight = 1 },
    };
    var out_m = [_]graph_segment.Edge{
        .{ .neighbor_id = @constCast("z"), .edge_type = @constCast("zeta"), .weight = 1 },
    };
    var adjacencies = [_]graph_segment.Adjacency{
        .{ .node_id = @constCast("z"), .out_edges = &out_z, .in_edges = @constCast(&.{}) },
        .{ .node_id = @constCast("a"), .out_edges = &out_a, .in_edges = @constCast(&.{}) },
        .{ .node_id = @constCast("m"), .out_edges = &out_m, .in_edges = @constCast(&.{}) },
        .{ .node_id = @constCast("isolated"), .out_edges = @constCast(&.{}), .in_edges = @constCast(&.{}) },
    };
    var tables = [_][]u8{@constCast("entities")};
    const graph = graph_segment.Segment{ .adjacencies = &adjacencies, .neighbor_tables = &tables };
    const payload = try graph_segment.encodeAlloc(alloc, graph);
    defer alloc.free(payload);
    var packed_topology = try prepareTopologyFromPackedAlloc(alloc, payload, .none, .{});
    defer packed_topology.deinit(alloc);
    var reference = try compileTopologyAlloc(alloc, graph, .none);
    defer reference.deinit(alloc);
    const Runner = struct {
        fn prepare(failing_alloc: Allocator, input: []const u8) !void {
            var topology = try prepareTopologyFromPackedAlloc(failing_alloc, input, .none, .{});
            defer topology.deinit(failing_alloc);
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, Runner.prepare, .{payload});
    var filter_types = [_][]u8{@constCast("alpha")};
    for ([_]graph_mod.GraphMetricEdgeFilter{ .{}, .{ .mode = .types, .types = &filter_types } }) |filter| {
        for ([_]graph_mod.GraphMetricKind{ .degree, .pagerank, .eigenvector, .hits_authority, .hits_hub }) |kind| {
            const options = BuildOptions{
                .graph_index_name = "graph",
                .config = .{ .name = "metric", .kind = kind, .edge_filter = filter },
                .source_graph = .{ .kind = .graph_segment, .name = "graph", .artifact_id = "sha256:placeholder", .byte_len = payload.len, .checksum = "placeholder" },
            };
            var a = try buildProjectionFromTopologyAlloc(alloc, reference, 0, options);
            defer a.deinit(alloc);
            var b = try buildProjectionFromTopologyAlloc(alloc, packed_topology, 0, options);
            defer b.deinit(alloc);
            try std.testing.expectEqual(a.edgeCount(), b.edgeCount());
            try std.testing.expectEqual(a.node_ids.items.len, b.node_ids.items.len);
            for (a.node_ids.items, b.node_ids.items) |left, right| try std.testing.expectEqualStrings(left, right);
            const ta = a.topology.?;
            const tb = b.topology.?;
            try std.testing.expectEqualSlices(u32, ta.incoming_offsets, tb.incoming_offsets);
            try std.testing.expectEqualSlices(u32, ta.incoming_sources, tb.incoming_sources);
            try std.testing.expectEqualSlices(u32, ta.outgoing_offsets, tb.outgoing_offsets);
            try std.testing.expectEqualSlices(u32, ta.outgoing_targets, tb.outgoing_targets);
        }
    }
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

test "serverless graph metric warm start maps an authenticated prior vector onto new ordinals" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/graph-metric-warm-start", .{tmp.sub_path});
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
    const config = graph_mod.GraphMetricConfig{ .name = "rank", .kind = .pagerank };
    const prior = try publishFromGraphArtifactAlloc(alloc, &artifacts, .{
        .graph_index_name = "graph",
        .config = config,
        .source_graph = source,
        .provenance = .{ .published_generation = 1, .edge_generation = 1, .computed_at_ms = 1 },
    });
    defer freeArtifactRef(alloc, prior);

    const current_nodes = [_][]const u8{ "a", "b", "c" };
    const seed = (try warmStartVectorAlloc(alloc, &artifacts, prior, &current_nodes, config, .none, .{}, 0, 0, null)).?;
    defer alloc.free(seed);
    try std.testing.expect(seed[0] > 0);
    try std.testing.expect(seed[1] > 0);
    try std.testing.expectEqual(@as(f64, 0), seed[2]);

    // Optional preparation must fall back before fetching an artifact if its
    // retained vector would crowd out otherwise admissible cold execution.
    var unavailable = prior;
    unavailable.artifact_id = "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
    unavailable.checksum = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
    try std.testing.expect((try warmStartVectorAlloc(alloc, &artifacts, unavailable, &current_nodes, config, .none, .{}, 0, 0, null)) == null);
    var mismatched = prior;
    mismatched.byte_len += 1;
    try std.testing.expect((try warmStartVectorAlloc(alloc, &artifacts, mismatched, &current_nodes, config, .none, .{}, 0, 0, null)) == null);
    var canceled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(error.Canceled, warmStartVectorAlloc(alloc, &artifacts, prior, &current_nodes, config, CancellationToken.fromAtomic(&canceled), .{}, 0, 0, null));
    var failing = std.testing.FailingAllocator.init(alloc, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, warmStartVectorAlloc(failing.allocator(), &artifacts, prior, &current_nodes, config, .none, .{}, 0, 0, null));
    try std.testing.expect((try warmStartVectorAlloc(alloc, &artifacts, unavailable, &current_nodes, config, .none, .{}, 0, (Limits{}).max_peak_memory_bytes, null)) == null);
    var exhausted = graph_metric_policy.Budget{ .limits = .{ .max_total_seed_payload_bytes = 0 } };
    try std.testing.expect((try warmStartVectorAlloc(alloc, &artifacts, unavailable, &current_nodes, config, .none, exhausted.limits, 0, 0, &exhausted)) == null);
    var spectral = config;
    spectral.kind = .eigenvector;
    try std.testing.expect((try warmStartVectorAlloc(alloc, &artifacts, unavailable, &current_nodes, spectral, .none, .{}, 0, 0, null)) == null);

    // A content-addressed but semantically malformed score block is not a
    // usable seed. The nullable fallback must also release the dense vector it
    // allocated before block decoding failed.
    const prior_payload = try artifacts.getVerifiedAllocWithCancellationUsingAllocator(
        alloc,
        prior.artifact_id,
        prior.byte_len,
        prior.checksum,
        .none,
    );
    defer alloc.free(prior_payload);
    const prior_control = try metric_segment.decodeControl(prior_payload, config.edge_filter);
    const score_offset = std.math.cast(usize, prior_control.score_data_offset) orelse return error.TestUnexpectedResult;
    if (prior_payload.len - score_offset < 2) return error.TestUnexpectedResult;
    const malformed_payload = try alloc.dupe(u8, prior_payload);
    defer alloc.free(malformed_payload);
    @memset(malformed_payload[score_offset..][0..2], 0xff);
    var malformed_metadata = try artifacts.put(malformed_payload);
    defer malformed_metadata.deinit(alloc);
    var malformed_prior = prior;
    malformed_prior.artifact_id = malformed_metadata.artifact_id;
    malformed_prior.byte_len = malformed_metadata.byte_len;
    malformed_prior.checksum = malformed_metadata.checksum;
    try std.testing.expect((try warmStartVectorAlloc(
        alloc,
        &artifacts,
        malformed_prior,
        &current_nodes,
        config,
        .none,
        .{},
        0,
        0,
        null,
    )) == null);
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

test "serverless graph metric projection admits census before allocations and bounds rejected scratch" {
    const alloc = std.testing.allocator;
    const topology = CompiledTopology{
        .node_ids = &.{ "a", "b" },
        .edge_types = &.{"cites"},
        .string_bytes = &.{},
        .edge_type_offsets = &.{ 0, 1 },
        .edges = &.{.{ .source = 0, .target = 1 }},
        .source_node_count = 2,
        .source_edge_count = 1,
        .retained_bytes = 128,
    };
    const source = artifact_ref.ArtifactRef{ .kind = .graph_segment, .artifact_id = "fixture", .checksum = "fixture", .byte_len = 1 };
    var exhausted = graph_metric_policy.Budget{ .limits = .{ .max_total_work_items = 0 } };
    var failing = std.testing.FailingAllocator.init(alloc, .{ .fail_index = 0 });
    try std.testing.expectError(error.GraphMetricBuildBudgetExceeded, buildProjectionFromTopologyAlloc(failing.allocator(), topology, 0, .{
        .graph_index_name = "graph",
        .config = .{ .name = "rank" },
        .source_graph = source,
        .batch_budget = &exhausted,
    }));
    try std.testing.expect(!failing.has_induced_failure);
    for ([_]graph_mod.GraphMetricKind{ .degree, .pagerank }) |kind| {
        var budget = graph_metric_policy.Budget{ .limits = .{} };
        try std.testing.expectError(error.GraphMetricBuildBudgetExceeded, buildProjectionFromTopologyAlloc(alloc, topology, 0, .{
            .graph_index_name = "graph",
            .config = .{ .name = "metric", .kind = kind },
            .source_graph = source,
            .batch_budget = &budget,
            .limits = .{ .max_peak_memory_bytes = topology.retained_bytes + 1 },
        }));
        // A failed preparation still consumes its reserved census allowance.
        try std.testing.expectEqual(@as(u64, 3), budget.work_items);
    }
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
        .{ .name = "rank_b", .kind = .pagerank, .damping = 0.75 },
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

    // An unaffordable PageRank must not force its larger CSR projection on a
    // degree metric. Degree fits exactly: source 3 + projection 9 + kernel 2.
    const pressure = try publishManyFromGraphArtifactAlloc(alloc, &artifacts, "pressure", source, &.{
        .{ .name = "rank", .kind = .pagerank },
        .{ .name = "degree", .kind = .degree },
    }, .none, .{ .max_work_items = 14, .max_total_work_items = 14 }, .{ .published_generation = 1, .edge_generation = 1, .computed_at_ms = 1 });
    defer {
        for (pressure) |ref| freeArtifactRef(alloc, ref);
        alloc.free(pressure);
    }
    try std.testing.expectEqual(artifact_ref.GraphMetricMaterializationState.rejected, pressure[0].graph_metric_materialization_state);
    try std.testing.expectEqual(artifact_ref.GraphMetricMaterializationState.ready, pressure[1].graph_metric_materialization_state);

    var shared_budget = graph_metric_policy.Budget{ .limits = .{ .max_work_items = 269, .max_total_work_items = 269 } };
    const one_config = [_]graph_mod.GraphMetricConfig{.{ .name = "shared_rank", .kind = .pagerank }};
    var prepared = try prepareGraphArtifactAlloc(alloc, &artifacts, source, .none, shared_budget.limits);
    defer prepared.deinit(alloc);
    try std.testing.expect(prepared.identifies(source));
    // Exercise the real publication entry point: failures in either scratch
    // allocation must release the already allocated result array.
    for ([_]usize{ 1, 2 }) |fail_index| {
        var failing = std.testing.FailingAllocator.init(alloc, .{ .fail_index = fail_index });
        var failure_budget = graph_metric_policy.Budget{ .limits = .{} };
        try std.testing.expectError(error.OutOfMemory, publishManyFromPreparedGraphWithBudgetAlloc(
            failing.allocator(),
            &artifacts,
            "graph",
            source,
            &one_config,
            .none,
            failure_budget.limits,
            &failure_budget,
            &prepared,
            .{ .published_generation = 1, .edge_generation = 1, .computed_at_ms = 1 },
            .{},
        ));
        try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
    }
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

    // Logical aliases must not perturb immutable metric content. With enough
    // aggregate work budget both publications reuse the cached projection and
    // converge on the same object-store identity; only their manifest names
    // differ.
    // Two 254-item kernels plus one 15-item projection fit exactly. A second
    // projection would force the alias publication into terminal rejection.
    const alias_limits = Limits{ .max_work_items = 523, .max_total_work_items = 523 };
    var alias_budget = graph_metric_policy.Budget{ .limits = alias_limits };
    var alias_prepared = try prepareGraphArtifactAlloc(alloc, &artifacts, source, .none, alias_limits);
    defer alias_prepared.deinit(alloc);
    const alias_a = try publishManyFromPreparedGraphWithBudgetAlloc(
        alloc,
        &artifacts,
        "alias_a",
        source,
        &one_config,
        .none,
        alias_limits,
        &alias_budget,
        &alias_prepared,
        .{ .published_generation = 2, .edge_generation = 1, .computed_at_ms = 2 },
        .{},
    );
    defer {
        for (alias_a) |ref| freeArtifactRef(alloc, ref);
        alloc.free(alias_a);
    }
    const alias_b = try publishManyFromPreparedGraphWithBudgetAlloc(
        alloc,
        &artifacts,
        "alias_b",
        source,
        &one_config,
        .none,
        alias_limits,
        &alias_budget,
        &alias_prepared,
        .{ .published_generation = 2, .edge_generation = 1, .computed_at_ms = 2 },
        .{},
    );
    defer {
        for (alias_b) |ref| freeArtifactRef(alloc, ref);
        alloc.free(alias_b);
    }
    try std.testing.expectEqual(@as(usize, 1), alias_a.len);
    try std.testing.expectEqual(@as(usize, 1), alias_b.len);
    try std.testing.expect(!std.mem.eql(u8, alias_a[0].name, alias_b[0].name));
    try std.testing.expectEqualStrings(alias_a[0].artifact_id, alias_b[0].artifact_id);
    try std.testing.expectEqualStrings(alias_a[0].checksum, alias_b[0].checksum);

    // Whole-publication planning must survive A -> B -> A and all -> typed
    // -> all ordering without another source read, projection, kernel or PUT.
    graph.adjacencies[0].out_edges[0].weight = 2;
    const other_payload = try graph_segment.encodeAlloc(alloc, graph);
    defer alloc.free(other_payload);
    var other_metadata = try artifacts.put(other_payload);
    defer other_metadata.deinit(alloc);
    const other_source = artifact_ref.ArtifactRef{ .kind = .graph_segment, .name = "other", .artifact_id = other_metadata.artifact_id, .byte_len = other_metadata.byte_len, .checksum = other_metadata.checksum };
    const CountingStore = struct {
        inner: *artifact_store.ArtifactStore,
        reads: usize = 0,
        writes: usize = 0,
        verifications: usize = 0,
        header_reads: usize = 0,
        fn get(ptr: *anyopaque, allocator: Allocator, id: []const u8) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.reads += 1;
            return self.inner.getAllocWithCancellationUsingAllocator(allocator, id, .none);
        }
        fn put(ptr: *anyopaque, allocator: Allocator, bytes: []const u8) !artifact_store.ArtifactMetadata {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.writes += 1;
            return self.inner.vtable.put(self.inner.ptr, allocator, bytes);
        }
        fn deinit(_: Allocator, _: *anyopaque) void {}
        fn range(ptr: *anyopaque, allocator: Allocator, id: []const u8, offset: u64, len: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.reads += 1;
            return self.inner.getRangeAllocWithCancellationUsingAllocator(allocator, id, offset, len, .none);
        }
        fn stat(ptr: *anyopaque, allocator: Allocator, id: []const u8) !artifact_store.ArtifactMetadata {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.inner.statWithCancellationUsingAllocator(allocator, id, .none);
        }
        fn verify(ptr: *anyopaque, allocator: Allocator, id: []const u8, len: u64, checksum: []const u8, cancellation: CancellationToken) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.verifications += 1;
            return self.inner.verifyContentWithCancellationUsingAllocator(allocator, id, len, checksum, cancellation);
        }
        fn verifiedRange(ptr: *anyopaque, allocator: Allocator, id: []const u8, byte_len: u64, checksum: []const u8, offset: u64, len: usize, cancellation: CancellationToken) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.header_reads += 1;
            return self.inner.getVerifiedRangeAllocWithCancellationUsingAllocator(allocator, id, byte_len, checksum, offset, len, cancellation);
        }
        fn delete(_: *anyopaque, _: []const u8) !void {
            return error.UnexpectedDelete;
        }
    };
    var counting = CountingStore{ .inner = &artifacts };
    var counted = artifact_store.ArtifactStore{ .allocator = alloc, .ptr = &counting, .vtable = &.{ .deinit = CountingStore.deinit, .put = CountingStore.put, .get_alloc = CountingStore.get, .get_range_alloc = CountingStore.range, .stat = CountingStore.stat, .delete = CountingStore.delete, .verify_content = CountingStore.verify, .get_verified_range_alloc_with_cancellation = CountingStore.verifiedRange } };
    const request_a = PublicationRequest{ .graph_index_name = "a", .source_graph = source, .config = one_config[0], .provenance = .{ .published_generation = 2, .edge_generation = 1, .computed_at_ms = 2 } };
    // Even numerically equal signed zeroes have distinct persisted config
    // fingerprints. Deduplication must preserve the exact storage identity.
    try std.testing.expect(!sameComputation(.{ .name = "zero", .tolerance = 0.0 }, .{ .name = "negative_zero", .tolerance = -0.0 }));
    var request_b = request_a;
    request_b.graph_index_name = "b";
    request_b.source_graph = other_source;
    var request_typed = request_a;
    request_typed.config.name = "typed_rank";
    request_typed.config.edge_filter = .{ .mode = .types, .types = &.{"cites"} };
    var request_alias = request_a;
    request_alias.graph_index_name = "alias";
    request_alias.provenance = .{ .published_generation = 4, .edge_generation = 3, .computed_at_ms = 4 };
    var request_authority = request_a;
    request_authority.config = .{ .name = "authority", .kind = .hits_authority };
    var request_hub = request_alias;
    request_hub.config = .{ .name = "hub", .kind = .hits_hub };
    var baseline_budget = graph_metric_policy.Budget{ .limits = .{} };
    const baseline = try publishRequestsAlloc(alloc, &counted, &.{ request_a, request_typed, request_b, request_authority, request_hub }, .none, baseline_budget.limits, &baseline_budget, .{});
    defer {
        for (baseline) |ref| freeArtifactRef(alloc, ref);
        alloc.free(baseline);
    }
    try std.testing.expectEqual(@as(usize, 2), counting.reads);
    try std.testing.expectEqual(@as(usize, 5), counting.writes);
    counting.reads = 0;
    counting.writes = 0;
    var planned_budget = graph_metric_policy.Budget{ .limits = .{
        .max_total_work_items = baseline_budget.work_items,
        .max_total_graph_payload_bytes = baseline_budget.graph_payload_bytes,
        .max_total_metric_payload_bytes = baseline_budget.metric_payload_bytes,
    } };
    const planned = try publishRequestsAlloc(alloc, &counted, &.{ request_a, request_b, request_typed, request_alias, request_hub, request_typed, request_authority, request_authority }, .none, planned_budget.limits, &planned_budget, .{});
    defer {
        for (planned) |ref| freeArtifactRef(alloc, ref);
        alloc.free(planned);
    }
    try std.testing.expectEqual(@as(usize, 2), counting.reads);
    try std.testing.expectEqual(@as(usize, 5), counting.writes);
    try std.testing.expectEqual(baseline_budget.work_items, planned_budget.work_items);
    try std.testing.expectEqualStrings(planned[0].artifact_id, planned[3].artifact_id);
    try std.testing.expectEqualStrings(planned[2].artifact_id, planned[5].artifact_id);
    try std.testing.expectEqualStrings(planned[6].artifact_id, planned[7].artifact_id);
    try std.testing.expectEqualStrings("1:a11:shared_rank", planned[0].name);
    try std.testing.expectEqualStrings("5:alias11:shared_rank", planned[3].name);
    try std.testing.expectEqual(@as(u64, 3), planned[3].edge_generation);
    for (planned) |ref| try std.testing.expectEqual(artifact_ref.GraphMetricMaterializationState.ready, ref.graph_metric_materialization_state);

    // Adding/renaming aliases uses one authenticated prior computation without
    // reading the graph, running a kernel, encoding, uploading, or using a seed.
    counting = .{ .inner = &artifacts };
    var reuse_budget = graph_metric_policy.Budget{ .limits = baseline_budget.limits };
    var renamed = request_alias;
    renamed.config.name = "renamed_rank";
    const reused = try publishRequestsWithPriorAlloc(alloc, &counted, &.{ request_alias, request_a, renamed }, baseline, .none, reuse_budget.limits, &reuse_budget, .{});
    defer {
        for (reused) |ref| freeArtifactRef(alloc, ref);
        alloc.free(reused);
    }
    try std.testing.expectEqual(@as(usize, 0), counting.reads);
    try std.testing.expectEqual(@as(usize, 0), counting.writes);
    try std.testing.expectEqual(@as(usize, 0), counting.verifications);
    try std.testing.expectEqual(@as(usize, 1), counting.header_reads);
    try std.testing.expect(reuse_budget.reuse_read_bytes > 0);
    try std.testing.expectEqual(@as(u64, 0), reuse_budget.work_items);
    try std.testing.expectEqual(@as(usize, 0), reuse_budget.graph_payload_bytes);
    for (reused) |ref| try std.testing.expectEqualStrings(baseline[0].artifact_id, ref.artifact_id);
    try std.testing.expectEqual(@as(u64, 4), reused[0].published_generation);
    try std.testing.expectEqual(@as(u64, 3), reused[0].edge_generation);
    try std.testing.expectEqual(baseline[0].computed_at_ms, reused[0].computed_at_ms);

    // A publication exhausted by the first computation rejects the second.
    // Its unchanged plan stays terminal, but removing the expensive sibling
    // must make the remaining metric eligible again, with fresh provenance.
    var rejected_budget = graph_metric_policy.Budget{ .limits = .{ .max_work_items = 269, .max_total_work_items = 269 } };
    const rejected = try publishRequestsAlloc(alloc, &artifacts, &.{ request_a, request_b }, .none, rejected_budget.limits, &rejected_budget, .{});
    defer {
        for (rejected) |ref| freeArtifactRef(alloc, ref);
        alloc.free(rejected);
    }
    try std.testing.expectEqual(artifact_ref.GraphMetricMaterializationState.rejected, rejected[1].graph_metric_materialization_state);
    counting = .{ .inner = &artifacts };
    rejected_budget = .{ .limits = rejected_budget.limits };
    const stable = try publishRequestsWithPriorAlloc(alloc, &counted, &.{ request_a, request_b }, rejected, .none, rejected_budget.limits, &rejected_budget, .{});
    defer {
        for (stable) |ref| freeArtifactRef(alloc, ref);
        alloc.free(stable);
    }
    try std.testing.expectEqual(artifact_ref.GraphMetricMaterializationState.rejected, stable[1].graph_metric_materialization_state);
    try std.testing.expectEqual(@as(usize, 0), counting.reads);
    try std.testing.expectEqual(@as(usize, 0), counting.writes);
    var retried_request = request_b;
    retried_request.provenance = .{ .published_generation = 5, .edge_generation = 1, .computed_at_ms = 5 };
    rejected_budget = .{ .limits = rejected_budget.limits };
    const retried = try publishRequestsWithPriorAlloc(alloc, &counted, &.{retried_request}, stable, .none, rejected_budget.limits, &rejected_budget, .{});
    defer {
        for (retried) |ref| freeArtifactRef(alloc, ref);
        alloc.free(retried);
    }
    try std.testing.expectEqual(artifact_ref.GraphMetricMaterializationState.ready, retried[0].graph_metric_materialization_state);
    try std.testing.expectEqual(@as(usize, 1), counting.reads);
    try std.testing.expectEqual(@as(usize, 1), counting.writes);
    try std.testing.expectEqual(@as(u64, 5), retried[0].published_generation);
    try std.testing.expectEqual(@as(u64, 5), retried[0].computed_at_ms);

    const AllocationRunner = struct {
        fn run(failing: Allocator, store: *artifact_store.ArtifactStore, prior: []const artifact_ref.ArtifactRef, requests: []const PublicationRequest) !void {
            var budget = graph_metric_policy.Budget{ .limits = .{} };
            const result = try publishRequestsWithPriorAlloc(failing, store, requests, prior, .none, budget.limits, &budget, .{});
            defer {
                for (result) |ref| freeArtifactRef(failing, ref);
                failing.free(result);
            }
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, AllocationRunner.run, .{ &artifacts, @as([]const artifact_ref.ArtifactRef, baseline), @as([]const PublicationRequest, &.{ request_alias, request_a, renamed }) });

    // Cache reuse must not inherit admission from the request that populated
    // the cache. A later caller's stricter source-topology limit still wins.
    const strict_limits = Limits{ .max_nodes = 1 };
    var strict_budget = graph_metric_policy.Budget{ .limits = strict_limits };
    const strict = try publishManyFromPreparedGraphWithBudgetAlloc(
        alloc,
        &artifacts,
        "strict_alias",
        source,
        &one_config,
        .none,
        strict_limits,
        &strict_budget,
        &alias_prepared,
        .{ .published_generation = 2, .edge_generation = 1, .computed_at_ms = 2 },
        .{},
    );
    defer {
        for (strict) |ref| freeArtifactRef(alloc, ref);
        alloc.free(strict);
    }
    const strict_payload = try artifacts.getVerifiedAllocWithCancellationUsingAllocator(alloc, strict[0].artifact_id, strict[0].byte_len, strict[0].checksum, .none);
    defer alloc.free(strict_payload);
    var strict_decoded = try metric_segment.decodeAlloc(alloc, strict_payload);
    defer strict_decoded.deinit(alloc);
    try std.testing.expectEqual(metric_segment.MaterializationState.rejected, strict_decoded.materialization_state);
}
