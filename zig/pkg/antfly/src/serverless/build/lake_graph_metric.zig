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
const graph_segment = @import("../graph_segment/mod.zig");
const metric_segment = @import("../graph_metric_segment/mod.zig");

pub const Limits = struct {
    max_graph_payload_bytes: usize = 512 * 1024 * 1024,
    max_metric_payload_bytes: usize = 256 * 1024 * 1024,
    max_nodes: usize = 1_000_000,
    max_edges: usize = 10_000_000,
    max_work_items: u64 = 500_000_000,
};

pub const BuildOptions = struct {
    graph_index_name: []const u8,
    config: graph_mod.GraphMetricConfig,
    source_graph: artifact_ref.ArtifactRef,
    cancellation: CancellationToken = .none,
    limits: Limits = .{},
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

pub fn artifactNameAlloc(alloc: Allocator, graph_index_name: []const u8, metric_name: []const u8) ![]u8 {
    return metric_segment.artifactNameAlloc(alloc, graph_index_name, metric_name) catch return error.InvalidGraphMetricBuildOptions;
}

pub fn buildFromGraphPayloadAlloc(alloc: Allocator, graph_payload: []const u8, options: BuildOptions) !BuildResult {
    try validateOptions(graph_payload, options);
    var graph = try graph_segment.decodeAlloc(alloc, graph_payload);
    defer graph.deinit(alloc);
    return try buildFromSegmentAlloc(alloc, graph, options);
}

pub fn publishFromGraphPayloadAlloc(alloc: Allocator, artifacts: *artifact_store.ArtifactStore, graph_payload: []const u8, options: BuildOptions) !artifact_ref.ArtifactRef {
    var built = try buildFromGraphPayloadAlloc(alloc, graph_payload, options);
    defer built.deinit(alloc);
    var metadata = try artifacts.put(built.payload);
    defer metadata.deinit(alloc);
    const name = try alloc.dupe(u8, built.artifact.name);
    errdefer alloc.free(name);
    const artifact_id = try alloc.dupe(u8, metadata.artifact_id);
    errdefer alloc.free(artifact_id);
    const checksum = try alloc.dupe(u8, metadata.checksum);
    return .{ .kind = .graph_metric_segment, .name = name, .artifact_id = artifact_id, .byte_len = metadata.byte_len, .checksum = checksum };
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
) ![]artifact_ref.ArtifactRef {
    if (configs.len == 0) return try alloc.alloc(artifact_ref.ArtifactRef, 0);
    if (graph_index_name.len == 0 or source_graph.kind != .graph_segment or source_graph.byte_len == 0) return error.InvalidGraphMetricBuildOptions;
    if (source_graph.byte_len > limits.max_graph_payload_bytes) return error.GraphMetricBuildBudgetExceeded;
    try graph_mod.validateGraphMetricEdgeFilters(&.{}, configs);
    const graph_payload = try artifacts.getVerifiedAllocWithCancellationUsingAllocator(
        alloc,
        source_graph.artifact_id,
        source_graph.byte_len,
        source_graph.checksum,
        cancellation,
    );
    defer alloc.free(graph_payload);
    var graph = try graph_segment.decodeAlloc(alloc, graph_payload);
    defer graph.deinit(alloc);

    const refs = try alloc.alloc(artifact_ref.ArtifactRef, configs.len);
    var initialized: usize = 0;
    errdefer {
        for (refs[0..initialized]) |ref| freeArtifactRef(alloc, ref);
        alloc.free(refs);
    }
    for (configs, 0..) |config, i| {
        try cancellation.check();
        var built = try buildFromSegmentAlloc(alloc, graph, .{
            .graph_index_name = graph_index_name,
            .config = config,
            .source_graph = source_graph,
            .cancellation = cancellation,
            .limits = limits,
        });
        defer built.deinit(alloc);
        var metadata = try artifacts.put(built.payload);
        defer metadata.deinit(alloc);
        const name = try alloc.dupe(u8, built.artifact.name);
        errdefer alloc.free(name);
        const artifact_id = try alloc.dupe(u8, metadata.artifact_id);
        errdefer alloc.free(artifact_id);
        const checksum = try alloc.dupe(u8, metadata.checksum);
        refs[i] = .{
            .kind = .graph_metric_segment,
            .name = name,
            .artifact_id = artifact_id,
            .byte_len = metadata.byte_len,
            .checksum = checksum,
        };
        initialized += 1;
    }
    return refs;
}

fn buildFromSegmentAlloc(alloc: Allocator, graph: graph_segment.Segment, options: BuildOptions) !BuildResult {
    try options.cancellation.check();
    var ordinals = std.StringHashMapUnmanaged(usize).empty;
    defer ordinals.deinit(alloc);
    var node_ids = std.ArrayListUnmanaged([]const u8).empty;
    defer node_ids.deinit(alloc);
    var edges = std.ArrayListUnmanaged(metrics.Edge).empty;
    defer edges.deinit(alloc);

    for (graph.adjacencies, 0..) |adjacency, adjacency_index| {
        if (adjacency_index % 256 == 0) try options.cancellation.check();
        for (adjacency.out_edges) |edge| {
            if (!edgeAllowed(options.config.edge_filter, edge.edge_type)) continue;
            const source = try getOrPutNode(alloc, &ordinals, &node_ids, adjacency.node_id, options.limits.max_nodes);
            const target = try getOrPutNode(alloc, &ordinals, &node_ids, edge.neighbor_id, options.limits.max_nodes);
            if (edges.items.len >= options.limits.max_edges) return error.GraphMetricBuildBudgetExceeded;
            try edges.append(alloc, .{ .source = source, .target = target });
        }
    }

    const kernel_options = metrics.Options{
        .damping = if (options.config.kind == .pagerank) options.config.damping else 0.85,
        .tolerance = if (options.config.kind == .degree) 0 else options.config.tolerance,
        .max_iterations = if (options.config.kind == .degree) 1 else options.config.max_iterations,
        .max_nodes = options.limits.max_nodes,
        .max_edges = options.limits.max_edges,
        .max_work_items = options.limits.max_work_items,
        .cancellation = options.cancellation,
    };
    var result = switch (options.config.kind) {
        .degree => try metrics.degreeAlloc(alloc, node_ids.items.len, edges.items, kernel_options),
        .pagerank => try metrics.pageRankAlloc(alloc, node_ids.items.len, edges.items, kernel_options),
        .eigenvector => try metrics.eigenvectorAlloc(alloc, node_ids.items.len, edges.items, kernel_options),
        .hits_authority, .hits_hub => blk: {
            const pair = try metrics.hitsAlloc(alloc, node_ids.items.len, edges.items, kernel_options);
            const selected = if (options.config.kind == .hits_authority) pair.authorities else pair.hubs;
            const unused = if (options.config.kind == .hits_authority) pair.hubs else pair.authorities;
            alloc.free(unused);
            break :blk metrics.Result{ .scores = selected, .iterations_completed = pair.iterations_completed, .converged = pair.converged, .delta = pair.delta };
        },
    };
    defer result.deinit(alloc);

    const scores = try makeScoresAlloc(alloc, node_ids.items, result.scores);
    var scores_owned = true;
    defer if (scores_owned) {
        for (scores) |*score| score.deinit(alloc);
        alloc.free(scores);
    };

    var segment = try makeMetricSegmentAlloc(alloc, options, result, scores);
    scores_owned = false;
    defer segment.deinit(alloc);
    const encoded_size = try metric_segment.encodedSize(segment);
    if (encoded_size > options.limits.max_metric_payload_bytes) return error.GraphMetricBuildBudgetExceeded;
    const payload = try metric_segment.encodeAlloc(alloc, segment);
    errdefer alloc.free(payload);
    const name = try artifactNameAlloc(alloc, options.graph_index_name, options.config.name);
    errdefer alloc.free(name);
    const artifact_id = try std.fmt.allocPrint(alloc, "lake-graph-metric:{d}:{s}:{d}", .{ name.len, name, payload.len });
    errdefer alloc.free(artifact_id);
    const checksum = try std.fmt.allocPrint(alloc, "len:{d}", .{payload.len});
    return .{ .payload = payload, .artifact = .{ .kind = .graph_metric_segment, .name = name, .artifact_id = artifact_id, .byte_len = @intCast(payload.len), .checksum = checksum } };
}

fn validateOptions(graph_payload: []const u8, options: BuildOptions) !void {
    if (graph_payload.len == 0 or graph_payload.len > options.limits.max_graph_payload_bytes or options.graph_index_name.len == 0 or options.config.name.len == 0 or options.source_graph.kind != .graph_segment or options.source_graph.artifact_id.len == 0 or options.source_graph.checksum.len == 0 or options.limits.max_metric_payload_bytes == 0) return error.InvalidGraphMetricBuildOptions;
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
        .edge_filter = edge_filter,
        .converged = result.converged,
        .iterations_completed = result.iterations_completed,
        .delta = result.delta,
        .scores = scores,
    };
}

fn makeScoresAlloc(alloc: Allocator, node_ids: []const []const u8, values: []const f64) ![]metric_segment.Score {
    if (node_ids.len != values.len) return error.InvalidGraphMetricScore;
    const scores = try alloc.alloc(metric_segment.Score, node_ids.len);
    errdefer alloc.free(scores);
    var initialized: usize = 0;
    errdefer for (scores[0..initialized]) |*score| score.deinit(alloc);
    for (node_ids, values, 0..) |node_id, value, i| {
        if (!std.math.isFinite(value)) return error.InvalidGraphMetricScore;
        scores[i] = .{ .node_id = try alloc.dupe(u8, node_id), .value = value };
        initialized += 1;
    }
    std.mem.sort(metric_segment.Score, scores, {}, lessScore);
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

fn edgeAllowed(filter: graph_mod.GraphMetricEdgeFilter, edge_type: []const u8) bool {
    return filter.mode == .all or filter.includesType(edge_type);
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
