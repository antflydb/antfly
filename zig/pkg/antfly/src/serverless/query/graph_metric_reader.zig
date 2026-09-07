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

//! Query-time access to immutable graph metric vectors. Every read verifies
//! that the metric was derived from the graph artifact pinned by this session,
//! preventing silent cross-generation score mixing.

const std = @import("std");
const Allocator = std.mem.Allocator;
const metric_segment = @import("../graph_metric_segment/mod.zig");
const graph_mod = @import("../../graph/graph.zig");
const graph_metric_config = @import("../build/graph_metric_config.zig");
const lake_graph_metric = @import("../build/lake_graph_metric.zig");
const graph_metric_policy = @import("../build/graph_metric_policy.zig");
const runtime_mod = @import("runtime.zig");
const operation = @import("../../api/operation.zig");
const artifacts_mod = @import("../artifacts/mod.zig");
const manifest_mod = @import("../manifest/mod.zig");
const routing_cache = @import("graph_metric_routing_cache.zig");

pub const Limits = struct {
    // Keep this aligned with the public query contract. The current wire stores this entire
    // bounded prefix in independently authenticated ranked blocks.
    max_top_k: usize = 10_000,
    max_point_scores: usize = 100_000,
    max_result_bytes: usize = 64 * 1024 * 1024,
};

fn recordRejectionDiagnostic(
    session: *runtime_mod.QuerySession,
    graph_index_name: []const u8,
    metric_name: []const u8,
    materializer_fingerprint: u64,
) void {
    session.recordGraphMetricRejection(
        graph_index_name,
        metric_name,
        materializer_fingerprint,
    );
}

pub const Score = struct {
    node_id: []u8,
    value: f64,

    pub fn deinit(self: *Score, alloc: Allocator) void {
        alloc.free(self.node_id);
        self.* = undefined;
    }
};

pub const Result = struct {
    scores: []Score,
    config_fingerprint: u64,
    converged: bool,
    iterations_completed: u32,
    delta: f64,
    edge_filter: graph_mod.GraphMetricEdgeFilter,
    metadata_version: u16,
    published_generation: u64,
    edge_generation: u64,
    computed_at_ms: u64,

    pub fn deinit(self: *Result, alloc: Allocator) void {
        for (self.scores) |*score| score.deinit(alloc);
        alloc.free(self.scores);
        self.edge_filter.deinit(alloc);
        self.* = undefined;
    }
};

pub const PointScoresResult = struct {
    scores: []?f64,
    owns_scores: bool = true,
    config_fingerprint: u64,
    converged: bool,
    iterations_completed: u32,
    delta: f64,
    edge_filter: graph_mod.GraphMetricEdgeFilter,
    metadata_version: u16,
    published_generation: u64,
    edge_generation: u64,
    computed_at_ms: u64,

    pub fn deinit(self: *PointScoresResult, alloc: Allocator) void {
        if (self.owns_scores) alloc.free(self.scores);
        self.edge_filter.deinit(alloc);
        self.* = undefined;
    }

    /// Transfers the column without encoding allocator ownership in a
    /// sentinel slice. This remains correct for zero-row queries, where a
    /// successful zero-length allocation still belongs to the caller.
    pub fn takeScores(self: *PointScoresResult) ![]?f64 {
        if (!self.owns_scores) return error.GraphMetricScoresAlreadyTaken;
        self.owns_scores = false;
        return self.scores;
    }
};

const PointScoresMetadata = struct {
    config_fingerprint: u64,
    converged: bool,
    iterations_completed: u32,
    delta: f64,
    metadata_version: u16,
    published_generation: u64,
    edge_generation: u64,
    computed_at_ms: u64,
};

pub const PointScoreColumnsResult = struct {
    columns: []PointScoresResult,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        for (self.columns) |*column| column.deinit(alloc);
        alloc.free(self.columns);
        self.* = undefined;
    }
};

const RankedScoreBoundaryValidator = struct {
    previous_node_id: [metric_segment.codec.max_score_node_id_bytes]u8 = undefined,
    previous_node_id_len: usize = 0,
    previous_value: f64 = 0,
    has_previous: bool = false,

    fn observeBlock(self: *@This(), decoded: metric_segment.codec.DecodedScoreBlock) !void {
        if (decoded.len == 0) return error.InvalidGraphMetricSegment;
        const first = decoded.scores[0];
        if (self.has_previous) {
            const node_order = first.orderNode(decoded.node_prefix, self.previous_node_id[0..self.previous_node_id_len]);
            if (self.previous_value < first.value or
                (self.previous_value == first.value and node_order != .gt))
            {
                return error.InvalidGraphMetricSegment;
            }
        }

        const last = decoded.scores[decoded.len - 1];
        if (last.nodeIdLen(decoded.node_prefix) > self.previous_node_id.len) return error.InvalidGraphMetricSegment;
        _ = try last.copyNode(decoded.node_prefix, self.previous_node_id[0..]);
        self.previous_node_id_len = last.nodeIdLen(decoded.node_prefix);
        self.previous_value = last.value;
        self.has_previous = true;
    }
};
const ScoreFetchRange = struct { first_block: usize, last_block: usize, offset: u64, len: usize };
const ScoreFetchWindow = struct {
    first_block: usize,
    last_block: usize,
    offset: u64,
    len: usize,
    touched_blocks: usize,
    touched_bytes: usize,
    selected: bool = false,
};
// Default used by isolated planner tests. Production derives this from the
// whole query's remaining budget after preparing every column's routing.
const max_score_range_requests: usize = 60;
const max_top_score_range_requests: usize = 64;
const max_parallel_metric_range_requests: usize = 8;
const max_point_score_columns: usize = 16;
// A column may itself hold a 16 MiB routing footer and a 32 MiB range batch.
// Two columns retain useful overlap without allowing nested fan-out to turn a
// valid request into an avoidable serverless memory spike.
const max_parallel_point_score_columns: usize = 2;
/// Bound aggregate live payload memory as well as request count. Coalescing
/// deliberately makes ranges variable-sized, so a count-only fanout can turn
/// eight harmless-looking reads into a large transient allocation spike.
const max_parallel_metric_range_bytes: usize = 32 * 1024 * 1024;
const coalesced_score_window_bytes: u64 = 8 * 1024 * 1024;
const ranked_fetch_window_bytes: usize = 8 * 1024 * 1024;

fn metricRangeBatchEnd(ranges: []const ScoreFetchRange, start: usize) usize {
    var end = start;
    var bytes: usize = 0;
    while (end < ranges.len and end - start < max_parallel_metric_range_requests) : (end += 1) {
        const next_bytes = std.math.add(usize, bytes, ranges[end].len) catch break;
        if (end > start and next_bytes > max_parallel_metric_range_bytes) break;
        bytes = next_bytes;
    }
    return end;
}

pub fn scoreAlloc(alloc: Allocator, session: *runtime_mod.QuerySession, graph_index_name: []const u8, metric_name: []const u8, node_id: []const u8) !?Score {
    const node_ids = [_][]const u8{node_id};
    var loaded = try scoresAlloc(alloc, session, graph_index_name, metric_name, &node_ids);
    defer loaded.deinit(alloc);
    const value = loaded.scores[0] orelse return null;
    return .{ .node_id = try alloc.dupe(u8, node_id), .value = value };
}

fn pointScoresResultAlloc(
    alloc: Allocator,
    session: *runtime_mod.QuerySession,
    graph_index_name: []const u8,
    metric_name: []const u8,
    scores: []?f64,
    metadata: PointScoresMetadata,
) !PointScoresResult {
    const specs = try session.graphMetricSpecs();
    const config = findConfig(specs, graph_index_name, metric_name) orelse return error.MetricNotConfigured;
    var edge_filter = try config.edge_filter.cloneAlloc(alloc);
    errdefer edge_filter.deinit(alloc);
    return .{
        .scores = scores,
        .config_fingerprint = metadata.config_fingerprint,
        .converged = metadata.converged,
        .iterations_completed = metadata.iterations_completed,
        .delta = metadata.delta,
        .edge_filter = edge_filter,
        .metadata_version = metadata.metadata_version,
        .published_generation = metadata.published_generation,
        .edge_generation = metadata.edge_generation,
        .computed_at_ms = metadata.computed_at_ms,
    };
}

const ColumnPass = enum { prepare, execute };

fn scoreColumnWorker(
    child: *runtime_mod.QuerySession,
    graph_index_name: []const u8,
    metric_name: []const u8,
    node_ids: []const []const u8,
    scores: []?f64,
    plan: *?PointScorePlan,
    pass: ColumnPass,
    failure: *?anyerror,
    cancel_siblings: *std.atomic.Value(bool),
) void {
    if (pass == .prepare) {
        plan.* = preparePointScoresAlloc(std.heap.smp_allocator, child, graph_index_name, metric_name, node_ids, scores) catch |err| {
            failure.* = err;
            cancel_siblings.store(true, .release);
            return;
        };
    } else {
        executePointScores(child, &plan.*.?, node_ids, scores) catch |err| {
            failure.* = err;
            cancel_siblings.store(true, .release);
        };
    }
}

const CombinedColumnCancellation = struct {
    parent: operation.CancellationToken,
    sibling_failure: *const std.atomic.Value(bool),

    fn token(self: *const @This()) operation.CancellationToken {
        return .{
            .ptr = self,
            .is_cancelled_fn = isCancelled,
        };
    }

    fn isCancelled(ptr: *const anyopaque) bool {
        const self: *const @This() = @ptrCast(@alignCast(ptr));
        return self.parent.isCancelled() or self.sibling_failure.load(.acquire);
    }
};

/// Resolves an entire graph-query metric shape with bounded cross-column
/// parallelism. Each worker owns its transient decode state while all workers
/// share the pinned manifest, cancellation token, cache, and atomic read
/// budget. Returned columns preserve `metric_names` order.
pub fn scoreColumnsAlloc(
    alloc: Allocator,
    session: *runtime_mod.QuerySession,
    graph_index_name: []const u8,
    metric_names: []const []const u8,
    node_ids: []const []const u8,
) !PointScoreColumnsResult {
    if (metric_names.len > max_point_score_columns or node_ids.len > (Limits{}).max_point_scores) return error.GraphMetricQueryBudgetExceeded;
    if (metric_names.len == 0) return .{ .columns = try alloc.alloc(PointScoresResult, 0) };
    _ = try session.graphMetricSpecs();
    const columns = try alloc.alloc(PointScoresResult, metric_names.len);
    var initialized_columns: usize = 0;
    errdefer {
        for (columns[0..initialized_columns]) |*column| column.deinit(alloc);
        alloc.free(columns);
    }
    var plans: [max_point_score_columns]?PointScorePlan = @splat(null);
    defer for (plans[0..metric_names.len]) |*plan| if (plan.*) |*value| value.deinit();
    var buffers: [max_point_score_columns]?[]?f64 = @splat(null);
    defer for (buffers[0..metric_names.len]) |buffer| if (buffer) |value| alloc.free(value);
    for (buffers[0..metric_names.len]) |*buffer| buffer.* = try alloc.alloc(?f64, node_ids.len);

    for ([_]ColumnPass{ .prepare, .execute }) |pass| {
        if (pass == .execute) try admitPointPlans(alloc, session, plans[0..metric_names.len]);
        var start: usize = 0;
        while (start < metric_names.len) {
            const end = @min(start + max_parallel_point_score_columns, metric_names.len);
            const count = end - start;
            var children: [max_parallel_point_score_columns]runtime_mod.QuerySession = undefined;
            var diagnostics: [max_parallel_point_score_columns]operation.RequestDiagnostics = @splat(.{});
            var failures: [max_parallel_point_score_columns]?anyerror = @splat(null);
            var sibling_failure = std.atomic.Value(bool).init(false);
            var cancellations: [max_parallel_point_score_columns]CombinedColumnCancellation = undefined;
            var group: std.Io.Group = .init;
            // Every fallible allocation happened before launching these workers.
            for (metric_names[start..end], 0..) |metric_name, i| {
                children[i] = session.forkGraphMetricRead(std.heap.smp_allocator);
                cancellations[i] = .{ .parent = session.cancellation, .sibling_failure = &sibling_failure };
                children[i].cancellation = cancellations[i].token();
                if (session.diagnostics != null) children[i].setDiagnostics(&diagnostics[i]);
                const args = .{ &children[i], graph_index_name, metric_name, node_ids, buffers[start + i].?, &plans[start + i], pass, &failures[i], &sibling_failure };
                if (session.io) |io| group.async(io, scoreColumnWorker, args) else @call(.auto, scoreColumnWorker, args);
            }
            const joined = if (session.io) |io| group.await(io) else {};
            for (children[0..count]) |*child| child.deinit();
            try joined;
            for (diagnostics[0..count]) |diagnostic| {
                const rejection = diagnostic.graph_metric_rejection orelse continue;
                session.recordGraphMetricRejection(rejection.graphIndexName(), rejection.metricName(), rejection.materializer_fingerprint);
                break;
            }
            for (failures[0..count]) |failure| if (failure) |err| {
                if (err != error.Canceled) return err;
            };
            for (failures[0..count]) |failure| if (failure) |err| return err;
            start = end;
        }
    }
    for (metric_names, 0..) |name, i| {
        columns[i] = try pointScoresResultAlloc(alloc, session, graph_index_name, name, buffers[i].?, plans[i].?.metadata);
        initialized_columns += 1;
        buffers[i] = null;
    }
    return .{ .columns = columns };
}

/// Resolves exact node scores using authenticated routing and cached blocks,
/// with at most one range fetch per missed block. Positions match `node_ids`.
pub fn scoresAlloc(
    alloc: Allocator,
    session: *runtime_mod.QuerySession,
    graph_index_name: []const u8,
    metric_name: []const u8,
    node_ids: []const []const u8,
) !PointScoresResult {
    const values = try alloc.alloc(?f64, node_ids.len);
    errdefer alloc.free(values);
    const metadata = try scoresInto(alloc, session, graph_index_name, metric_name, node_ids, values);
    return try pointScoresResultAlloc(alloc, session, graph_index_name, metric_name, values, metadata);
}

const PendingNode = struct {
    block_index: usize,
    node_index: usize,

    fn lessThan(_: void, left: @This(), right: @This()) bool {
        return left.block_index < right.block_index or
            (left.block_index == right.block_index and left.node_index < right.node_index);
    }
};
const TouchedBlock = struct {
    block_index: usize,
    first_pending: usize,
    pending_count: usize,
};

const PointScorePlan = struct {
    alloc: Allocator,
    range_alloc: Allocator,
    metadata: PointScoresMetadata,
    metric_index: usize,
    score_count: usize,
    score_data_offset: u64,
    point_routing: ?PointRouting = null,
    pending_nodes: []PendingNode = &.{},
    touched_blocks: []TouchedBlock = &.{},
    ranges: []ScoreFetchRange = &.{},

    fn deinit(self: *@This()) void {
        if (self.point_routing) |*routing| routing.deinit();
        self.alloc.free(self.pending_nodes);
        self.alloc.free(self.touched_blocks);
        self.range_alloc.free(self.ranges);
    }
};

/// Resolves scores directly into caller-owned storage using the same admission
/// and execution phases as a multi-column query.
fn scoresInto(alloc: Allocator, session: *runtime_mod.QuerySession, graph_index_name: []const u8, metric_name: []const u8, node_ids: []const []const u8, values: []?f64) !PointScoresMetadata {
    var plans = [_]?PointScorePlan{try preparePointScoresAlloc(alloc, session, graph_index_name, metric_name, node_ids, values)};
    defer plans[0].?.deinit();
    try admitPointPlans(alloc, session, &plans);
    try executePointScores(session, &plans[0].?, node_ids, values);
    return plans[0].?.metadata;
}

fn preparePointScoresAlloc(
    alloc: Allocator,
    session: *runtime_mod.QuerySession,
    graph_index_name: []const u8,
    metric_name: []const u8,
    node_ids: []const []const u8,
    values: []?f64,
) !PointScorePlan {
    try session.checkCancellation();
    if (values.len != node_ids.len) return error.InvalidGraphMetricSegment;
    if (node_ids.len > (Limits{}).max_point_scores) return error.GraphMetricQueryBudgetExceeded;
    const retained_score_bytes = std.math.mul(usize, node_ids.len, @sizeOf(?f64)) catch return error.GraphMetricQueryBudgetExceeded;
    try session.chargeGraphMetricRetained(retained_score_bytes);
    try session.chargeGraphMetricDecode(0, node_ids.len);
    for (node_ids) |node_id| {
        if (node_id.len == 0 or node_id.len > metric_segment.codec.max_score_node_id_bytes) return error.InvalidGraphMetricNodeId;
    }

    const specs = try session.graphMetricSpecs();
    const config = findConfig(specs, graph_index_name, metric_name) orelse return error.MetricNotConfigured;
    const graph_index = session.findNamedArtifactIndex(.graph_segment, graph_index_name) orelse return error.GraphSegmentNotFound;
    const graph_artifact = session.artifactRef(graph_index).?;
    const artifact_name = try metric_segment.artifactNameAlloc(alloc, graph_index_name, metric_name);
    defer alloc.free(artifact_name);
    const metric_index = session.findNamedArtifactIndex(.graph_metric_segment, artifact_name) orelse return error.MetricNotReady;
    const metric_artifact = session.artifactRef(metric_index) orelse return error.InvalidGraphMetricSegment;

    const control_len = try metric_segment.controlProbeLen(
        metric_artifact.byte_len,
        graph_artifact.artifact_id,
        graph_artifact.checksum,
        config.edge_filter,
    );
    const control_bytes = try fetchControlAlloc(session, metric_index, metric_artifact, control_len);
    defer session.alloc.free(control_bytes);
    const control = try metric_segment.decodeControl(control_bytes, config.edge_filter);
    try validateControl(control.header, graph_artifact, config);
    if (control.header.materialization_state == .rejected) {
        recordRejectionDiagnostic(session, graph_index_name, metric_name, control.header.materializer_fingerprint);
        return error.GraphMetricMaterializationRejected;
    }
    const metadata = PointScoresMetadata{
        .config_fingerprint = control.header.config_fingerprint,
        .converged = control.header.converged,
        .iterations_completed = control.header.iterations_completed,
        .delta = control.header.delta,
        .metadata_version = control.header.version,
        .published_generation = if (metric_artifact.published_generation != 0) metric_artifact.published_generation else session.manifest.version,
        .edge_generation = if (metric_artifact.edge_generation != 0) metric_artifact.edge_generation else session.manifest.version,
        .computed_at_ms = if (metric_artifact.computed_at_ms != 0) metric_artifact.computed_at_ms else @divTrunc(session.manifest.built_at_ns, std.time.ns_per_ms),
    };
    if (node_ids.len == 0) return .{ .alloc = alloc, .range_alloc = alloc, .metadata = metadata, .metric_index = metric_index, .score_count = control.score_count, .score_data_offset = control.score_data_offset };

    const footer_len = try routingFooterLen(metric_artifact, control.header.version);
    const footer_offset = metric_artifact.byte_len - footer_len;
    const expected_blocks = @as(usize, control.score_count) / metric_segment.score_block_entries +
        @intFromBool(@as(usize, control.score_count) % metric_segment.score_block_entries != 0);
    var point_routing = try loadPointRouting(alloc, session, metric_index, metric_artifact, control, footer_offset, expected_blocks, node_ids);
    errdefer point_routing.deinit();
    const routing = point_routing.routing;

    @memset(values, null);
    // Candidate sets are normally much smaller than the persisted vector.
    // Keep request planning proportional to requested nodes/touched blocks,
    // rather than allocating and clearing three arrays sized to every block in
    // the artifact for a one-node lookup.
    var pending_nodes = std.ArrayListUnmanaged(PendingNode).empty;
    errdefer pending_nodes.deinit(alloc);
    try pending_nodes.ensureTotalCapacity(alloc, node_ids.len);
    for (node_ids, 0..) |node_id, node_index| {
        const block_index = routing.findIndex(node_id) orelse continue;
        pending_nodes.appendAssumeCapacity(.{ .block_index = block_index, .node_index = node_index });
    }
    std.mem.sort(PendingNode, pending_nodes.items, {}, PendingNode.lessThan);
    var touched_blocks = std.ArrayListUnmanaged(TouchedBlock).empty;
    errdefer touched_blocks.deinit(alloc);
    var pending_start: usize = 0;
    while (pending_start < pending_nodes.items.len) {
        const block_index = pending_nodes.items[pending_start].block_index;
        var pending_end = pending_start + 1;
        while (pending_end < pending_nodes.items.len and pending_nodes.items[pending_end].block_index == block_index) : (pending_end += 1) {}
        try touched_blocks.append(alloc, .{
            .block_index = block_index,
            .first_pending = pending_start,
            .pending_count = pending_end - pending_start,
        });
        pending_start = pending_end;
    }

    // Consume authenticated cache hits before network admission. Transport may
    // bridge a cached gap only when the request/byte plan calls for overfetch.
    if (session.cache != null) {
        var missing_count: usize = 0;
        for (touched_blocks.items) |touched| {
            const entry = routing.entries[touched.block_index];
            var id_buf: [64]u8 = undefined;
            const id = try metricBlockId(&id_buf, .score, entry.block_index);
            if (try session.readCachedAuthenticatedBlockAlloc(std.heap.smp_allocator, metric_index, id, entry.offset, entry.len, &entry.checksum)) |bytes| {
                defer std.heap.smp_allocator.free(bytes);
                try decodePointScoreBlock(session, entry, control.score_count, bytes, pending_nodes.items[touched.first_pending..][0..touched.pending_count], node_ids, values);
            } else {
                touched_blocks.items[missing_count] = touched;
                missing_count += 1;
            }
        }
        touched_blocks.items = touched_blocks.items[0..missing_count];
    }
    const pending = try pending_nodes.toOwnedSlice(alloc);
    errdefer alloc.free(pending);
    const touched = try touched_blocks.toOwnedSlice(alloc);
    return .{ .alloc = alloc, .range_alloc = alloc, .metadata = metadata, .metric_index = metric_index, .score_count = control.score_count, .score_data_offset = control.score_data_offset, .point_routing = point_routing, .pending_nodes = pending, .touched_blocks = touched };
}

fn decodePointScoreBlock(session: *runtime_mod.QuerySession, entry: metric_segment.codec.RoutingEntry, score_count: usize, payload: []const u8, pending_nodes: []const PendingNode, node_ids: []const []const u8, values: []?f64) !void {
    const first_score = std.math.mul(usize, entry.block_index, metric_segment.score_block_entries) catch return error.InvalidGraphMetricSegment;
    if (first_score >= score_count) return error.InvalidGraphMetricSegment;
    const expected = @min(metric_segment.score_block_entries, score_count - first_score);
    try session.chargeGraphMetricDecode(1, expected);
    const decoded = try metric_segment.decodeScoreBlockWithCancellation(payload, session.cancellation);
    if (decoded.len != expected or !decoded.scores[0].eqlNode(decoded.node_prefix, entry.first_node_id)) return error.InvalidGraphMetricSegment;
    for (pending_nodes) |pending| values[pending.node_index] = decoded.score(node_ids[pending.node_index]);
}

fn executePointScores(session: *runtime_mod.QuerySession, plan: *const PointScorePlan, node_ids: []const []const u8, values: []?f64) !void {
    const point_routing = plan.point_routing orelse return;
    const routing = point_routing.routing;
    const alloc = plan.alloc;
    const metric_index = plan.metric_index;
    const fetch_ranges = plan.ranges;
    var fetch_start: usize = 0;
    while (fetch_start < fetch_ranges.len) {
        const fetch_end = metricRangeBatchEnd(fetch_ranges, fetch_start);
        const range_batch = fetch_ranges[fetch_start..fetch_end];
        var fetched_ranges = try fetchMetricRangeBatchAlloc(
            alloc,
            session,
            metric_index,
            plan.metadata.metadata_version,
            routing.entries,
            range_batch,
            .reserved_score,
        );
        defer fetched_ranges.deinit(alloc);

        for (range_batch, fetched_ranges.payloads) |range, payload| {
            try session.checkCancellation();
            var touched_index = std.sort.lowerBound(TouchedBlock, plan.touched_blocks, range.first_block, struct {
                fn order(block_index: usize, touched: TouchedBlock) std.math.Order {
                    return std.math.order(block_index, touched.block_index);
                }
            }.order);
            while (touched_index < plan.touched_blocks.len and plan.touched_blocks[touched_index].block_index <= range.last_block) : (touched_index += 1) {
                const touched = plan.touched_blocks[touched_index];
                const block_index = touched.block_index;
                const entry = routing.entries[block_index];
                if (entry.offset < range.offset) return error.InvalidGraphMetricSegment;
                const relative_offset = std.math.cast(usize, entry.offset - range.offset) orelse return error.InvalidGraphMetricSegment;
                const relative_end = std.math.add(usize, relative_offset, entry.len) catch return error.InvalidGraphMetricSegment;
                if (relative_end > payload.len) return error.InvalidGraphMetricSegment;
                try decodePointScoreBlock(session, entry, plan.score_count, payload[relative_offset..relative_end], plan.pending_nodes[touched.first_pending..][0..touched.pending_count], node_ids, values);
            }
        }
        fetch_start = fetch_end;
    }
}

fn admitPointPlans(alloc: Allocator, session: *runtime_mod.QuerySession, plans: []?PointScorePlan) !void {
    const remaining = session.graphMetricRangeBudget();
    const allowance = std.math.cast(usize, remaining.requests) orelse std.math.maxInt(usize);
    var requests: usize = 0;
    for (plans) |*maybe_plan| {
        const plan = &maybe_plan.*.?;
        plan.range_alloc = alloc;
        const routing = plan.point_routing orelse continue;
        plan.ranges = try planSparseScoreFetchRangesAlloc(alloc, routing.routing.entries, plan.touched_blocks, plan.score_data_offset, std.math.maxInt(usize));
        requests = std.math.add(usize, requests, plan.ranges.len) catch return error.GraphMetricQueryBudgetExceeded;
    }
    if (requests > allowance) {
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const temp = arena.allocator();
        const inputs = try temp.alloc(RangePlanningColumn, plans.len);
        for (plans, inputs) |*maybe_plan, *input| {
            const plan = &maybe_plan.*.?;
            const entries = if (plan.point_routing) |routing| routing.routing.entries else &.{};
            const counts = try temp.alloc(usize, entries.len);
            @memset(counts, 0);
            for (plan.touched_blocks) |block| counts[block.block_index] = block.pending_count;
            input.* = .{ .entries = entries, .counts = counts, .score_data_offset = plan.score_data_offset };
        }
        const ranges = try planScoreColumnsWithinBudgetAlloc(alloc, inputs, allowance, remaining.bytes, session.cancellation);
        defer alloc.free(ranges);
        for (plans, ranges) |*maybe_plan, planned| {
            const plan = &maybe_plan.*.?;
            alloc.free(plan.ranges);
            plan.ranges = planned;
        }
    }
    requests = 0;
    var bytes: usize = 0;
    for (plans) |maybe_plan| for (maybe_plan.?.ranges) |range| {
        requests = std.math.add(usize, requests, 1) catch return error.GraphMetricQueryBudgetExceeded;
        bytes = std.math.add(usize, bytes, range.len) catch return error.GraphMetricQueryBudgetExceeded;
    };
    // Reserve the complete score plan atomically before any score I/O starts.
    try session.reserveGraphMetricRanges(requests, bytes);
}

fn planSparseScoreFetchRangesAlloc(
    alloc: Allocator,
    entries: []const metric_segment.codec.RoutingEntry,
    touched_blocks: anytype,
    score_data_offset: u64,
    request_limit: usize,
) ![]ScoreFetchRange {
    if (touched_blocks.len <= request_limit) {
        const ranges = try alloc.alloc(ScoreFetchRange, touched_blocks.len);
        errdefer alloc.free(ranges);
        for (touched_blocks, ranges) |touched, *range| {
            if (touched.block_index >= entries.len) return error.InvalidGraphMetricSegment;
            const entry = entries[touched.block_index];
            range.* = .{
                .first_block = touched.block_index,
                .last_block = touched.block_index,
                .offset = entry.offset,
                .len = entry.len,
            };
        }
        return ranges;
    }

    // Dense/coalesced requests are already bounded by max_point_scores. Only
    // this less common path pays O(total blocks), where evaluating fixed
    // windows is necessary to enforce the request-count budget.
    const block_counts = try alloc.alloc(usize, entries.len);
    defer alloc.free(block_counts);
    @memset(block_counts, 0);
    for (touched_blocks) |touched| {
        if (touched.block_index >= entries.len) return error.InvalidGraphMetricSegment;
        block_counts[touched.block_index] = touched.pending_count;
    }
    return try planScoreFetchRangesAlloc(alloc, entries, block_counts, score_data_offset, request_limit);
}

const RangePlanningColumn = struct {
    entries: []const metric_segment.codec.RoutingEntry,
    counts: []const usize,
    score_data_offset: u64,
};

fn planScoreFetchRangesAlloc(alloc: Allocator, entries: []const metric_segment.codec.RoutingEntry, counts: []const usize, score_data_offset: u64, request_limit: usize) ![]ScoreFetchRange {
    const columns = try planScoreColumnsRangesAlloc(alloc, &.{.{ .entries = entries, .counts = counts, .score_data_offset = score_data_offset }}, request_limit);
    defer alloc.free(columns);
    return columns[0];
}

fn scoreFetchWindowsAlloc(alloc: Allocator, entries: []const metric_segment.codec.RoutingEntry, block_counts: []const usize, score_data_offset: u64) ![]ScoreFetchWindow {
    var windows = std.ArrayListUnmanaged(ScoreFetchWindow).empty;
    errdefer windows.deinit(alloc);
    var first_block: usize = 0;
    while (first_block < entries.len) {
        const first = entries[first_block];
        if (first.offset < score_data_offset) return error.InvalidGraphMetricSegment;
        if (first.len == 0 or first.len > coalesced_score_window_bytes) return error.GraphMetricQueryBudgetExceeded;
        var last_block = first_block;
        var window_touched: usize = @intFromBool(block_counts[first_block] != 0);
        var touched_bytes: usize = if (block_counts[first_block] != 0) first.len else 0;
        while (last_block + 1 < entries.len) {
            const prior_end = std.math.add(u64, entries[last_block].offset, entries[last_block].len) catch
                return error.InvalidGraphMetricSegment;
            const next = entries[last_block + 1];
            // A sparse routing set can omit entire pages. Never coalesce
            // across an unauthenticated gap.
            if (next.offset != prior_end) break;
            const next_end = std.math.add(u64, next.offset, next.len) catch
                return error.InvalidGraphMetricSegment;
            if (next_end - first.offset > coalesced_score_window_bytes) break;
            last_block += 1;
            if (block_counts[last_block] != 0) {
                window_touched += 1;
                touched_bytes = std.math.add(usize, touched_bytes, entries[last_block].len) catch
                    return error.GraphMetricQueryBudgetExceeded;
            }
        }
        if (window_touched != 0) {
            // Cache units no longer depend on transport extents. Do not read
            // unused leading/trailing blocks merely to stabilize a range key.
            var first_touched = first_block;
            while (block_counts[first_touched] == 0) : (first_touched += 1) {}
            var last_touched = last_block;
            while (block_counts[last_touched] == 0) : (last_touched -= 1) {}
            const last_end = std.math.add(u64, entries[last_touched].offset, entries[last_touched].len) catch
                return error.InvalidGraphMetricSegment;
            try windows.append(alloc, .{
                .first_block = first_touched,
                .last_block = last_touched,
                .offset = entries[first_touched].offset,
                .len = std.math.cast(usize, last_end - entries[first_touched].offset) orelse return error.GraphMetricQueryBudgetExceeded,
                .touched_blocks = window_touched,
                .touched_bytes = touched_bytes,
            });
        }
        first_block = last_block + 1;
    }

    return windows.toOwnedSlice(alloc);
}

/// Joint admission minimizes bytes subject to the shared request limit.
/// Transport windows are query-local; authenticated blocks are the cache units.
fn planScoreColumnsRangesAlloc(alloc: Allocator, columns: []const RangePlanningColumn, request_limit: usize) ![][]ScoreFetchRange {
    return planScoreColumnsWithinBudgetAlloc(alloc, columns, request_limit, std.math.maxInt(u64), .{});
}

fn planScoreColumnsWithinBudgetAlloc(alloc: Allocator, columns: []const RangePlanningColumn, request_limit: usize, byte_limit: u64, cancellation: operation.CancellationToken) ![][]ScoreFetchRange {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const temp = arena.allocator();
    var requests: usize = 0;
    var exact_bytes: u64 = 0;
    for (columns) |column| {
        try cancellation.check();
        if (column.entries.len != column.counts.len) return error.InvalidGraphMetricSegment;
        for (column.entries, column.counts, 0..) |entry, count, i| {
            if (i % 1024 == 0) try cancellation.check();
            if (count == 0) continue;
            requests = std.math.add(usize, requests, 1) catch return error.GraphMetricQueryBudgetExceeded;
            exact_bytes = std.math.add(u64, exact_bytes, entry.len) catch return error.GraphMetricQueryBudgetExceeded;
        }
    }
    // Coalescing cannot improve on the exact miss-byte lower bound.
    if (exact_bytes > byte_limit) return error.GraphMetricQueryBudgetExceeded;
    const windows = try temp.alloc([]ScoreFetchWindow, columns.len);
    const coalesce = requests > request_limit;
    if (coalesce) {
        // The public request budget is 128. Bound optimization workspace even
        // when a test/internal caller supplies a larger allowance.
        const limit = @min(request_limit, (runtime_mod.GraphMetricReadLimits{}).max_range_requests);
        var window_count: usize = 0;
        for (columns, windows) |column, *column_windows| {
            try cancellation.check();
            column_windows.* = try scoreFetchWindowsAlloc(temp, column.entries, column.counts, column.score_data_offset);
            window_count = std.math.add(usize, window_count, column_windows.len) catch return error.GraphMetricQueryBudgetExceeded;
            // Every nonempty window requires at least one read.
            if (window_count > limit) return error.GraphMetricQueryBudgetExceeded;
        }
        const width = limit + 1;
        const choices = try temp.alloc(bool, window_count * width);
        @memset(choices, false);
        var costs = try temp.alloc(u64, width);
        var next = try temp.alloc(u64, width);
        const unreachable_cost = std.math.maxInt(u64);
        @memset(costs, unreachable_cost);
        costs[0] = 0;
        var step: usize = 0;
        for (windows) |column_windows| for (column_windows) |window| {
            try cancellation.check();
            @memset(next, unreachable_cost);
            for (costs, 0..) |cost, used| {
                if (cost == unreachable_cost) continue;
                const alternatives = [_]struct { reads: usize, bytes: usize, coalesced: bool }{
                    .{ .reads = window.touched_blocks, .bytes = window.touched_bytes, .coalesced = false },
                    .{ .reads = 1, .bytes = window.len, .coalesced = true },
                };
                for (alternatives) |alternative| {
                    if (alternative.reads > limit - used) continue;
                    const total = std.math.add(u64, cost, alternative.bytes) catch continue;
                    const destination = used + alternative.reads;
                    if (total <= byte_limit and total < next[destination]) {
                        next[destination] = total;
                        choices[step * width + destination] = alternative.coalesced;
                    }
                }
            }
            std.mem.swap([]u64, &costs, &next);
            step += 1;
        };
        var best: ?usize = null;
        for (costs, 0..) |cost, used| {
            if (cost != unreachable_cost and (best == null or cost < costs[best.?])) best = used;
        }
        var used = best orelse return error.GraphMetricQueryBudgetExceeded;
        var column_index = windows.len;
        while (column_index != 0) {
            column_index -= 1;
            var window_index = windows[column_index].len;
            while (window_index != 0) {
                window_index -= 1;
                step -= 1;
                const window = &windows[column_index][window_index];
                window.selected = choices[step * width + used];
                used -= if (window.selected) @as(usize, 1) else window.touched_blocks;
            }
        }
        std.debug.assert(used == 0);
    }
    const results = try alloc.alloc([]ScoreFetchRange, columns.len);
    var initialized: usize = 0;
    errdefer {
        for (results[0..initialized]) |ranges| alloc.free(ranges);
        alloc.free(results);
    }
    for (columns, 0..) |column, i| {
        var ranges = std.ArrayListUnmanaged(ScoreFetchRange).empty;
        errdefer ranges.deinit(alloc);
        if (coalesce) {
            for (windows[i]) |window| {
                if (window.selected) {
                    try ranges.append(alloc, .{ .first_block = window.first_block, .last_block = window.last_block, .offset = window.offset, .len = window.len });
                } else {
                    for (window.first_block..window.last_block + 1) |j| {
                        if (column.counts[j] == 0) continue;
                        const entry = column.entries[j];
                        try ranges.append(alloc, .{ .first_block = j, .last_block = j, .offset = entry.offset, .len = entry.len });
                    }
                }
            }
        } else {
            for (column.entries, column.counts, 0..) |entry, count, j| {
                if (count != 0) try ranges.append(alloc, .{ .first_block = j, .last_block = j, .offset = entry.offset, .len = entry.len });
            }
        }
        results[i] = try ranges.toOwnedSlice(alloc);
        initialized += 1;
    }
    return results;
}

fn fetchControlAlloc(
    session: *runtime_mod.QuerySession,
    metric_index: usize,
    artifact: manifest_mod.ArtifactRef,
    expected_len: usize,
) ![]u8 {
    try session.chargeGraphMetricRange(expected_len);
    if (artifact.metadata_version != metric_segment.wire_version or
        artifact.graph_metric_control_len != expected_len) return error.InvalidGraphMetricSegment;
    const subranges = [_]runtime_mod.AuthenticatedSubrange{.{
        .relative_offset = 0,
        .len = expected_len,
        .checksum = artifact.graph_metric_control_checksum,
    }};
    return session.fetchArtifactAuthenticatedRangeAlloc(metric_index, 0, expected_len, &subranges) catch |err| switch (err) {
        error.ArtifactIntegrityMismatch => error.InvalidGraphMetricSegment,
        else => |other| other,
    };
}

fn routingFooterLen(
    artifact: manifest_mod.ArtifactRef,
    segment_version: u16,
) !usize {
    if (segment_version != metric_segment.wire_version or artifact.metadata_version != segment_version or
        artifact.graph_metric_routing_footer_len == 0 or
        artifact.graph_metric_routing_footer_len > metric_segment.codec.max_routing_bytes or
        artifact.graph_metric_routing_footer_len > artifact.byte_len)
    {
        return error.InvalidGraphMetricSegment;
    }
    return artifact.graph_metric_routing_footer_len;
}

fn fetchRoutingFooterAlloc(
    session: *runtime_mod.QuerySession,
    metric_index: usize,
    artifact: manifest_mod.ArtifactRef,
    segment_version: u16,
    footer_offset: u64,
    footer_len: usize,
    root_len: usize,
) ![]u8 {
    try session.chargeGraphMetricRange(footer_len);
    if (segment_version != metric_segment.wire_version) return error.InvalidGraphMetricSegment;
    if (root_len > footer_len) return error.InvalidGraphMetricSegment;
    const subranges = [_]runtime_mod.AuthenticatedSubrange{.{
        .relative_offset = footer_len - root_len,
        .len = root_len,
        .checksum = artifact.graph_metric_routing_checksum,
    }};
    const both = [_]runtime_mod.AuthenticatedSubrange{ .{
        .relative_offset = 0,
        .len = footer_len - root_len,
        .checksum = artifact.graph_metric_point_index_checksum,
    }, subranges[0] };
    return session.fetchArtifactAuthenticatedRangeAlloc(metric_index, footer_offset, footer_len, if (footer_len == root_len) &subranges else &both) catch |err| switch (err) {
        error.ArtifactIntegrityMismatch => error.InvalidGraphMetricSegment,
        else => |other| other,
    };
}

const DirectoryRead = struct {
    footer_offset: u64,
    checksum: [32]u8,
    block_count: usize,
};

const PointRouting = struct {
    alloc: Allocator,
    payload_alloc: Allocator,
    payloads: std.ArrayListUnmanaged([]u8) = .empty,
    routing: metric_segment.codec.RoutingIndex,
    lease: ?routing_cache.Lease = null,

    fn deinit(self: *@This()) void {
        if (self.lease) |*lease| lease.deinit() else self.routing.deinit(self.alloc);
        for (self.payloads.items) |payload| self.payload_alloc.free(payload);
        self.payloads.deinit(self.alloc);
    }
};

fn loadPointRouting(alloc: Allocator, session: *runtime_mod.QuerySession, metric_index: usize, artifact: manifest_mod.ArtifactRef, control: metric_segment.codec.Control, footer_offset: u64, block_count: usize, node_ids: []const []const u8) !PointRouting {
    const codec = metric_segment.codec;
    const root_len = codec.routingRootLen(control.score_count);
    if (root_len > artifact.graph_metric_routing_footer_len) return error.InvalidGraphMetricSegment;
    // A one-page index is bounded even with maximum-length identifiers.
    // Small byte extents also keep the one-round-trip decoded cache fast path.
    if (block_count <= codec.routing_page_entries or artifact.graph_metric_routing_footer_len <= 64 * 1024) {
        var lease = try acquireRouting(session, metric_index, artifact, control.header.version, footer_offset, artifact.graph_metric_routing_footer_len, block_count, root_len, false, null);
        errdefer lease.deinit();
        const routing = lease.entry.routing;
        if (routing.entries.len != block_count or routing.footer_offset != footer_offset or routing.primary_data_offset != control.score_data_offset) return error.InvalidGraphMetricSegment;
        return .{ .alloc = alloc, .payload_alloc = session.alloc, .routing = routing, .lease = lease };
    }
    var root_lease = try acquireRouting(session, metric_index, artifact, control.header.version, artifact.byte_len - root_len, root_len, 40, root_len, true, null);
    defer root_lease.deinit();
    const root = root_lease.entry.routing;
    if (root.footer_offset != footer_offset or root.primary_data_offset != control.score_data_offset) return error.InvalidGraphMetricSegment;
    const directory_offset = artifact.byte_len - root_len - root.directory_len;
    const page_count = std.math.divCeil(usize, block_count, codec.routing_page_entries) catch return error.InvalidGraphMetricSegment;
    var directory_lease = try acquireRouting(session, metric_index, artifact, control.header.version, directory_offset, root.directory_len, page_count, root_len, false, .{
        .footer_offset = footer_offset,
        .checksum = root.directory_checksum,
        .block_count = block_count,
    });
    defer directory_lease.deinit();
    const directory = directory_lease.entry.routing;
    var selected = std.ArrayListUnmanaged(usize).empty;
    defer selected.deinit(alloc);
    try session.chargeGraphMetricRetained(std.math.mul(usize, node_ids.len, @sizeOf(usize)) catch return error.GraphMetricQueryBudgetExceeded);
    try selected.ensureTotalCapacityPrecise(alloc, node_ids.len);
    for (node_ids) |node_id| {
        try session.checkCancellation();
        if (directory.findIndex(node_id)) |i| try selected.append(alloc, i);
    }
    std.mem.sort(usize, selected.items, {}, std.sort.asc(usize));
    var selected_count: usize = 0;
    for (selected.items) |i| {
        if (selected_count != 0 and selected.items[selected_count - 1] == i) continue;
        selected.items[selected_count] = i;
        selected_count += 1;
    }
    selected.items = selected.items[0..selected_count];
    var entries = std.ArrayListUnmanaged(codec.RoutingEntry).empty;
    errdefer entries.deinit(alloc);
    const payload_alloc = std.heap.smp_allocator;
    var payloads = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (payloads.items) |payload| payload_alloc.free(payload);
        payloads.deinit(alloc);
    }
    // Cache units are authenticated pages, independent of transport grouping.
    // Keep scratch state proportional to selected pages, not the full index.
    try session.chargeGraphMetricRetained(std.math.mul(usize, selected.items.len, @sizeOf(?[]const u8)) catch return error.GraphMetricQueryBudgetExceeded);
    const page_views = try alloc.alloc(?[]const u8, selected.items.len);
    defer alloc.free(page_views);
    @memset(page_views, null);
    var misses = std.ArrayListUnmanaged(usize).empty;
    defer misses.deinit(alloc);
    for (selected.items, 0..) |i, view_index| {
        const page = directory.entries[i];
        const count = @min(codec.routing_page_entries, block_count - page.block_index);
        try session.chargeGraphMetricDecode(1, count);
        try session.chargeGraphMetricRetained(page.len + count * @sizeOf(codec.RoutingEntry) * 3 + 2 * @sizeOf([]u8));
        var id_buf: [64]u8 = undefined;
        const id = try metricBlockId(&id_buf, .routing, page.block_index);
        if (try session.readCachedAuthenticatedBlockAlloc(payload_alloc, metric_index, id, page.offset, page.len, &page.checksum)) |bytes| {
            payloads.append(alloc, bytes) catch |err| {
                payload_alloc.free(bytes);
                return err;
            };
            page_views[view_index] = bytes;
        } else try misses.append(alloc, i);
    }
    const ranges = try planRoutingPageRangesAlloc(alloc, directory.entries, misses.items);
    defer alloc.free(ranges);
    var start: usize = 0;
    while (start < ranges.len) {
        const end = metricRangeBatchEnd(ranges, start);
        var fetched = try fetchMetricRangeBatchAlloc(alloc, session, metric_index, control.header.version, directory.entries, ranges[start..end], .routing);
        defer fetched.deinit(alloc);
        for (ranges[start..end], fetched.payloads) |range, *payload| {
            try payloads.append(alloc, payload.*);
            const bytes = payload.*;
            payload.* = @constCast((&[_]u8{})[0..]);
            const first_view = std.sort.lowerBound(usize, selected.items, range.first_block, struct {
                fn order(needle: usize, item: usize) std.math.Order {
                    return std.math.order(needle, item);
                }
            }.order);
            for (range.first_block..range.last_block + 1) |i| {
                const page = directory.entries[i];
                const offset: usize = @intCast(page.offset - range.offset);
                const view_index = first_view + i - range.first_block;
                std.debug.assert(selected.items[view_index] == i);
                page_views[view_index] = bytes[offset..][0..page.len];
            }
        }
        start = end;
    }
    for (selected.items, 0..) |i, view_index| {
        const page = directory.entries[i];
        const bytes = page_views[view_index] orelse return error.InvalidGraphMetricSegment;
        const decoded = try codec.decodePointPageAlloc(alloc, bytes, page, block_count, root.primary_data_offset, root.primary_data_end, session.cancellation);
        defer alloc.free(decoded);
        if (i + 1 < directory.entries.len and std.mem.order(u8, decoded[decoded.len - 1].first_node_id, directory.entries[i + 1].first_node_id) != .lt) return error.InvalidGraphMetricSegment;
        try entries.appendSlice(alloc, decoded);
    }
    const owned_entries = try entries.toOwnedSlice(alloc);
    errdefer alloc.free(owned_entries);
    return .{
        .alloc = alloc,
        .payload_alloc = payload_alloc,
        .payloads = payloads,
        .routing = .{
            .entries = owned_entries,
            .ranked_entries = try alloc.alloc(codec.RankedRoutingEntry, 0),
            .top_score_count = 0,
            .footer_offset = footer_offset,
            .primary_data_offset = root.primary_data_offset,
            .primary_data_end = root.primary_data_end,
        },
    };
}

/// Contiguous selected routing pages share one authenticated range. Independent
/// runs use the same bounded byte/fanout executor as primary score reads.
fn planRoutingPageRangesAlloc(alloc: Allocator, entries: []const metric_segment.codec.RoutingEntry, selected: []const usize) ![]ScoreFetchRange {
    var ranges = std.ArrayListUnmanaged(ScoreFetchRange).empty;
    errdefer ranges.deinit(alloc);
    var previous: ?usize = null;
    for (selected) |index| {
        if (previous == index) continue;
        if (index >= entries.len or (previous != null and index < previous.?)) return error.InvalidGraphMetricSegment;
        previous = index;
        const entry = entries[index];
        if (entry.len == 0 or entry.len > coalesced_score_window_bytes) return error.InvalidGraphMetricSegment;
        _ = std.math.add(u64, entry.offset, entry.len) catch return error.InvalidGraphMetricSegment;
        if (ranges.items.len > 0) {
            const last = &ranges.items[ranges.items.len - 1];
            if (last.last_block + 1 == index and last.offset + last.len == entry.offset and entry.len <= coalesced_score_window_bytes - last.len) {
                last.last_block = index;
                last.len += entry.len;
                continue;
            }
        }
        try ranges.append(alloc, .{ .first_block = index, .last_block = index, .offset = entry.offset, .len = entry.len });
    }
    return ranges.toOwnedSlice(alloc);
}

fn acquireRouting(
    session: *runtime_mod.QuerySession,
    metric_index: usize,
    artifact: manifest_mod.ArtifactRef,
    version: u16,
    offset: u64,
    len: usize,
    decode_work: usize,
    root_len: usize,
    ranked_only: bool,
    directory: ?DirectoryRead,
) !routing_cache.Lease {
    try session.cancellation.check();
    // Include all authentication and interpretation inputs, not merely a
    // logical metric name (which can point at a new immutable publication).
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update(artifact.artifact_id);
    hash.update(&.{0});
    hash.update(artifact.checksum);
    hash.update(&artifact.graph_metric_routing_checksum);
    hash.update(&artifact.graph_metric_point_index_checksum);
    hash.update(&.{ @intFromBool(ranked_only), @intFromBool(directory != null) });
    if (directory) |info| hash.update(&info.checksum);
    var dimensions: [42]u8 = undefined;
    std.mem.writeInt(u64, dimensions[0..8], artifact.byte_len, .little);
    std.mem.writeInt(u64, dimensions[8..16], offset, .little);
    std.mem.writeInt(u64, dimensions[16..24], len, .little);
    std.mem.writeInt(u16, dimensions[24..26], version, .little);
    std.mem.writeInt(u64, dimensions[26..34], if (directory) |info| info.block_count else 0, .little);
    std.mem.writeInt(u64, dimensions[34..42], if (directory) |info| info.footer_offset else 0, .little);
    hash.update(&dimensions);
    var key: [32]u8 = undefined;
    hash.final(&key);
    var fill: ?usize = null;
    defer if (fill) |index| session.cache.?.graph_metric_routing.finish(index, session.io);
    if (session.cache) |cache| {
        while (true) {
            try session.cancellation.check();
            switch (cache.graph_metric_routing.begin(key)) {
                .hit => |cached| {
                    var lease = cached;
                    errdefer lease.deinit();
                    try session.chargeGraphMetricRetained(lease.entry.bytes());
                    try session.chargeGraphMetricDecode(0, 1);
                    return lease;
                },
                .fill => |index| {
                    fill = index;
                    break;
                },
                .wait => |registered| {
                    var waiter = registered;
                    defer waiter.deinit();
                    if (try waiter.awaitResult(session.io, session.cancellation)) |shared| {
                        var lease = shared;
                        errdefer lease.deinit();
                        try session.chargeGraphMetricRetained(lease.entry.bytes());
                        try session.chargeGraphMetricDecode(0, 1);
                        return lease;
                    }
                },
                .saturated => |epoch| {
                    try cache.graph_metric_routing.awaitFill(session.io, session.cancellation, epoch);
                },
            }
        }
    }
    // Reserve transient decode memory before fetching/duplicating the footer.
    // The bounded top tier contributes at most 40 ranked routing entries.
    const routing_bytes = std.math.mul(usize, decode_work, @sizeOf(metric_segment.codec.RoutingEntry)) catch return error.GraphMetricQueryBudgetExceeded;
    const footer_bytes = std.math.mul(usize, len, 2) catch return error.GraphMetricQueryBudgetExceeded;
    const overhead = @sizeOf(routing_cache.Entry) +
        (metric_segment.codec.max_persisted_top_entries / metric_segment.codec.ranked_score_block_entries + 1) * @sizeOf(metric_segment.codec.RankedRoutingEntry);
    const reservation = std.math.add(usize, footer_bytes, routing_bytes) catch return error.GraphMetricQueryBudgetExceeded;
    try session.chargeGraphMetricRetained(std.math.add(usize, reservation, overhead) catch return error.GraphMetricQueryBudgetExceeded);
    try session.chargeGraphMetricDecode(1, decode_work);
    const footer = if (directory) |info| blk: {
        try session.chargeGraphMetricRange(len);
        break :blk session.fetchArtifactAuthenticatedBlockAlloc(metric_index, "graph-metric-directory", offset, len, &info.checksum) catch |err| switch (err) {
            error.ArtifactIntegrityMismatch => return error.InvalidGraphMetricSegment,
            else => return err,
        };
    } else try fetchRoutingFooterAlloc(session, metric_index, artifact, version, offset, len, root_len);
    var owns_footer = true;
    defer if (owns_footer) session.alloc.free(footer);
    const owner = if (session.cache) |cache| cache.alloc else session.alloc;
    const owned = if (owner.ptr == session.alloc.ptr and owner.vtable == session.alloc.vtable) blk: {
        owns_footer = false;
        break :blk footer;
    } else try owner.dupe(u8, footer);
    errdefer owner.free(owned);
    // The control's count is the admitted decode/memory shape. Check the
    // authenticated footer count before its decoder allocates routing entries.
    if (!ranked_only and directory == null and
        (owned.len < 8 or std.mem.readInt(u32, owned[4..8], .little) != decode_work)) return error.InvalidGraphMetricSegment;
    var routing = if (directory) |info| blk: {
        const entries = try metric_segment.codec.decodePointDirectoryAlloc(owner, owned, offset, info.footer_offset, info.block_count, session.cancellation);
        errdefer owner.free(entries);
        break :blk metric_segment.codec.RoutingIndex{
            .entries = entries,
            .ranked_entries = try owner.alloc(metric_segment.codec.RankedRoutingEntry, 0),
            .top_score_count = 0,
            .footer_offset = info.footer_offset,
            .point_index_checksum = artifact.graph_metric_point_index_checksum,
        };
    } else if (ranked_only)
        try metric_segment.codec.decodeRoutingRootAlloc(owner, owned, artifact.byte_len, version, session.cancellation)
    else
        try metric_segment.decodeRoutingIndexForVersionWithCancellationAlloc(owner, owned, artifact.byte_len, version, session.cancellation);
    errdefer routing.deinit(owner);
    if (!std.mem.eql(u8, &routing.point_index_checksum, &artifact.graph_metric_point_index_checksum)) return error.InvalidGraphMetricSegment;
    const entry = try owner.create(routing_cache.Entry);
    errdefer owner.destroy(entry);
    entry.* = .{ .key = key, .alloc = owner, .footer = owned, .routing = routing };
    try session.cancellation.check();
    if (session.cache) |cache| return cache.graph_metric_routing.publish(fill.?, entry, cache.cfg.max_graph_metric_routing_bytes);
    return .{ .entry = entry };
}

const MetricRangeKind = enum { score, reserved_score, routing };

fn metricBlockId(buf: []u8, kind: MetricRangeKind, block_index: usize) ![]const u8 {
    return std.fmt.bufPrint(buf, "graph-metric-{s}-{d}-exact", .{ if (kind == .routing) "routing" else "score", block_index });
}

fn fetchMetricRangeAlloc(
    alloc: Allocator,
    session: *runtime_mod.QuerySession,
    metric_index: usize,
    segment_version: u16,
    entries: []const metric_segment.codec.RoutingEntry,
    range: ScoreFetchRange,
    kind: MetricRangeKind,
) ![]u8 {
    if (kind != .reserved_score) try session.chargeGraphMetricRange(range.len);
    if (segment_version != metric_segment.wire_version) return error.InvalidGraphMetricSegment;
    if (range.first_block > range.last_block or range.last_block >= entries.len) return error.InvalidGraphMetricSegment;
    if (range.first_block == range.last_block) {
        const entry = entries[range.first_block];
        if (entry.offset != range.offset or entry.len != range.len) return error.InvalidGraphMetricSegment;
        var block_id_buf: [64]u8 = undefined;
        const block_id = metricBlockId(&block_id_buf, kind, entry.block_index) catch
            return error.InvalidGraphMetricSegment;
        return session.fetchArtifactAuthenticatedBlockAlloc(
            metric_index,
            block_id,
            range.offset,
            range.len,
            &entry.checksum,
        ) catch |err| switch (err) {
            error.ArtifactIntegrityMismatch => error.InvalidGraphMetricSegment,
            else => |other| other,
        };
    }
    const subranges = try alloc.alloc(runtime_mod.AuthenticatedSubrange, range.last_block - range.first_block + 1);
    defer alloc.free(subranges);
    var covered: usize = 0;
    for (entries[range.first_block .. range.last_block + 1], 0..) |entry, index| {
        if (entry.offset < range.offset) return error.InvalidGraphMetricSegment;
        const relative_offset = std.math.cast(usize, entry.offset - range.offset) orelse return error.InvalidGraphMetricSegment;
        if (relative_offset != covered) return error.InvalidGraphMetricSegment;
        subranges[index] = .{
            .relative_offset = relative_offset,
            .len = entry.len,
            .checksum = entry.checksum,
        };
        covered = std.math.add(usize, covered, entry.len) catch return error.InvalidGraphMetricSegment;
    }
    if (covered != range.len) return error.InvalidGraphMetricSegment;
    {
        const bytes = session.fetchArtifactAuthenticatedRangeUncachedAlloc(metric_index, range.offset, range.len, subranges) catch |err| switch (err) {
            error.ArtifactIntegrityMismatch => return error.InvalidGraphMetricSegment,
            else => return err,
        };
        errdefer session.alloc.free(bytes);
        // Authenticate the entire transport response before publishing any
        // canonical block. Never retain candidate-specific combined ranges.
        for (entries[range.first_block .. range.last_block + 1], subranges) |entry, subrange| {
            var id_buf: [64]u8 = undefined;
            const id = try metricBlockId(&id_buf, kind, entry.block_index);
            try session.cacheAuthenticatedBlock(metric_index, id, entry.offset, bytes[subrange.relative_offset..][0..entry.len], &entry.checksum);
        }
        return bytes;
    }
}

const FetchedMetricRanges = struct {
    payloads: [][]u8,

    fn deinit(self: *@This(), alloc: Allocator) void {
        for (self.payloads) |payload| if (payload.len > 0) std.heap.smp_allocator.free(payload);
        alloc.free(self.payloads);
    }
};

fn fetchMetricRangeWorker(
    child: *runtime_mod.QuerySession,
    metric_index: usize,
    segment_version: u16,
    entries: []const metric_segment.codec.RoutingEntry,
    range: ScoreFetchRange,
    output: *[]u8,
    failure: *?anyerror,
    kind: MetricRangeKind,
) void {
    output.* = fetchMetricRangeAlloc(
        std.heap.smp_allocator,
        child,
        metric_index,
        segment_version,
        entries,
        range,
        kind,
    ) catch |err| {
        failure.* = err;
        return;
    };
}

/// Fetch independent immutable routing or score ranges with bounded fanout.
/// Every child borrows the pinned manifest and charges the same synchronized
/// request budget, so parallelism changes latency without weakening admission.
fn fetchMetricRangeBatchAlloc(
    alloc: Allocator,
    session: *runtime_mod.QuerySession,
    metric_index: usize,
    segment_version: u16,
    entries: []const metric_segment.codec.RoutingEntry,
    ranges: []const ScoreFetchRange,
    kind: MetricRangeKind,
) !FetchedMetricRanges {
    if (ranges.len > max_parallel_metric_range_requests)
        return error.GraphMetricQueryBudgetExceeded;
    var requested_bytes: usize = 0;
    for (ranges) |range| {
        requested_bytes = std.math.add(usize, requested_bytes, range.len) catch
            return error.GraphMetricQueryBudgetExceeded;
    }
    if (requested_bytes > max_parallel_metric_range_bytes)
        return error.GraphMetricQueryBudgetExceeded;
    const payloads = try alloc.alloc([]u8, ranges.len);
    @memset(payloads, @constCast((&[_]u8{})[0..]));
    errdefer {
        for (payloads) |payload| if (payload.len > 0) std.heap.smp_allocator.free(payload);
        alloc.free(payloads);
    }

    var children: [max_parallel_metric_range_requests]runtime_mod.QuerySession = undefined;
    var failures: [max_parallel_metric_range_requests]?anyerror = @splat(null);
    if (session.io) |io| {
        var group: std.Io.Group = .init;
        for (ranges, 0..) |range, index| {
            children[index] = session.forkGraphMetricRead(std.heap.smp_allocator);
            group.async(io, fetchMetricRangeWorker, .{
                &children[index], metric_index, segment_version, entries, range, &payloads[index], &failures[index], kind,
            });
        }
        const await_result = group.await(io);
        for (children[0..ranges.len]) |*child| child.deinit();
        try await_result;
    } else {
        for (ranges, 0..) |range, index| {
            children[index] = session.forkGraphMetricRead(std.heap.smp_allocator);
            fetchMetricRangeWorker(
                &children[index],
                metric_index,
                segment_version,
                entries,
                range,
                &payloads[index],
                &failures[index],
                kind,
            );
        }
        for (children[0..ranges.len]) |*child| child.deinit();
    }
    for (failures[0..ranges.len]) |failure| if (failure) |err| return err;
    return .{ .payloads = payloads };
}

fn fetchRankedScoreBlocksAlloc(
    alloc: Allocator,
    session: *runtime_mod.QuerySession,
    metric_index: usize,
    entries: []const metric_segment.codec.RankedRoutingEntry,
) ![]u8 {
    if (entries.len == 0) return try alloc.alloc(u8, 0);
    const range_offset = entries[0].offset;
    var range_len: usize = 0;
    const subranges = try alloc.alloc(runtime_mod.AuthenticatedSubrange, entries.len);
    defer alloc.free(subranges);
    for (entries, 0..) |entry, index| {
        if (entry.len == 0 or entry.len > metric_segment.codec.max_ranked_score_block_bytes) return error.InvalidGraphMetricSegment;
        const relative_offset = std.math.cast(usize, entry.offset -| range_offset) orelse return error.InvalidGraphMetricSegment;
        if (relative_offset != range_len) return error.InvalidGraphMetricSegment;
        subranges[index] = .{
            .relative_offset = relative_offset,
            .len = entry.len,
            .checksum = entry.checksum,
        };
        range_len = std.math.add(usize, range_len, entry.len) catch return error.GraphMetricQueryBudgetExceeded;
    }
    try session.chargeGraphMetricRange(range_len);
    return session.fetchArtifactAuthenticatedRangeAlloc(metric_index, range_offset, range_len, subranges) catch |err| switch (err) {
        error.ArtifactIntegrityMismatch => error.InvalidGraphMetricSegment,
        else => |other| other,
    };
}

fn planAllScoreFetchRangesAlloc(
    alloc: Allocator,
    entries: []const metric_segment.codec.RoutingEntry,
) ![]ScoreFetchRange {
    var ranges = std.ArrayListUnmanaged(ScoreFetchRange).empty;
    errdefer ranges.deinit(alloc);
    for (entries, 0..) |entry, block_index| {
        const entry_end = std.math.add(u64, entry.offset, entry.len) catch return error.InvalidGraphMetricSegment;
        if (ranges.items.len == 0) {
            try ranges.append(alloc, .{
                .first_block = block_index,
                .last_block = block_index,
                .offset = entry.offset,
                .len = entry.len,
            });
            continue;
        }
        const range = &ranges.items[ranges.items.len - 1];
        const range_end = std.math.add(u64, range.offset, range.len) catch return error.InvalidGraphMetricSegment;
        const combined_len = entry_end - range.offset;
        if (entry.offset != range_end or combined_len > coalesced_score_window_bytes) {
            try ranges.append(alloc, .{
                .first_block = block_index,
                .last_block = block_index,
                .offset = entry.offset,
                .len = entry.len,
            });
        } else {
            range.last_block = block_index;
            range.len = std.math.cast(usize, combined_len) orelse return error.GraphMetricQueryBudgetExceeded;
        }
    }
    if (ranges.items.len > max_top_score_range_requests) return error.GraphMetricQueryBudgetExceeded;
    return try ranges.toOwnedSlice(alloc);
}

pub fn openAlloc(alloc: Allocator, session: *runtime_mod.QuerySession, graph_index_name: []const u8, metric_name: []const u8) !metric_segment.Segment {
    return try loadVerifiedAlloc(alloc, session, graph_index_name, metric_name);
}

pub fn topAlloc(alloc: Allocator, session: *runtime_mod.QuerySession, graph_index_name: []const u8, metric_name: []const u8, top_k: usize) !Result {
    return try topWithLimitsAlloc(alloc, session, graph_index_name, metric_name, top_k, .{});
}

pub fn topWithLimitsAlloc(alloc: Allocator, session: *runtime_mod.QuerySession, graph_index_name: []const u8, metric_name: []const u8, top_k: usize, limits: Limits) !Result {
    if (top_k > limits.max_top_k or top_k > metric_segment.codec.max_persisted_top_entries or
        limits.max_top_k == 0 or limits.max_result_bytes == 0)
    {
        return error.GraphMetricQueryBudgetExceeded;
    }

    try session.checkCancellation();
    const specs = try session.graphMetricSpecs();
    const config = findConfig(specs, graph_index_name, metric_name) orelse return error.MetricNotConfigured;
    const graph_index = session.findNamedArtifactIndex(.graph_segment, graph_index_name) orelse return error.GraphSegmentNotFound;
    const graph_artifact = session.artifactRef(graph_index).?;
    const artifact_name = try metric_segment.artifactNameAlloc(alloc, graph_index_name, metric_name);
    defer alloc.free(artifact_name);
    const metric_index = session.findNamedArtifactIndex(.graph_metric_segment, artifact_name) orelse return error.MetricNotReady;
    const metric_artifact = session.artifactRef(metric_index) orelse return error.InvalidGraphMetricSegment;
    const control_len = try metric_segment.controlProbeLen(
        metric_artifact.byte_len,
        graph_artifact.artifact_id,
        graph_artifact.checksum,
        config.edge_filter,
    );
    const control_bytes = try fetchControlAlloc(session, metric_index, metric_artifact, control_len);
    defer session.alloc.free(control_bytes);
    const control = try metric_segment.decodeControl(control_bytes, config.edge_filter);
    try validateControl(control.header, graph_artifact, config);
    if (control.header.materialization_state == .rejected) {
        recordRejectionDiagnostic(session, graph_index_name, metric_name, control.header.materializer_fingerprint);
        return error.GraphMetricMaterializationRejected;
    }
    // A zero-cardinality request still authenticates control/provenance but
    // never pays for a potentially large routing footer.
    if (top_k == 0 or control.score_count == 0) {
        const scores = try alloc.alloc(Score, 0);
        errdefer alloc.free(scores);
        var edge_filter = try config.edge_filter.cloneAlloc(alloc);
        errdefer edge_filter.deinit(alloc);
        return .{
            .scores = scores,
            .config_fingerprint = control.header.config_fingerprint,
            .converged = control.header.converged,
            .iterations_completed = control.header.iterations_completed,
            .delta = control.header.delta,
            .edge_filter = edge_filter,
            .metadata_version = control.header.version,
            .published_generation = if (metric_artifact.published_generation != 0) metric_artifact.published_generation else session.manifest.version,
            .edge_generation = if (metric_artifact.edge_generation != 0) metric_artifact.edge_generation else session.manifest.version,
            .computed_at_ms = if (metric_artifact.computed_at_ms != 0) metric_artifact.computed_at_ms else @divTrunc(session.manifest.built_at_ns, std.time.ns_per_ms),
        };
    }
    // Wire-v8 has an independently authenticated routing root. Cold top-K
    // never fetches, decodes or retains the primary point-lookup index.
    const footer_len = try routingFooterLen(metric_artifact, control.header.version);
    const footer_offset = metric_artifact.byte_len - footer_len;
    const expected_top_count = @min(@as(usize, control.score_count), metric_segment.codec.max_persisted_top_entries);
    const expected_ranked_blocks = expected_top_count / metric_segment.codec.ranked_score_block_entries +
        @intFromBool(expected_top_count % metric_segment.codec.ranked_score_block_entries != 0);
    const root_len = metric_segment.codec.routingRootLen(control.score_count);
    if (root_len > footer_len) return error.InvalidGraphMetricSegment;
    var routing_lease = try acquireRouting(session, metric_index, metric_artifact, control.header.version, metric_artifact.byte_len - root_len, root_len, expected_ranked_blocks, root_len, true, null);
    defer routing_lease.deinit();
    const routing = routing_lease.entry.routing;
    if (routing.entries.len != 0 or routing.footer_offset != footer_offset or routing.primary_data_offset != control.score_data_offset) {
        return error.InvalidGraphMetricSegment;
    }

    if (routing.top_score_count != expected_top_count or routing.ranked_entries.len != expected_ranked_blocks) {
        return error.InvalidGraphMetricSegment;
    }
    const result_count = @min(top_k, @as(usize, control.score_count));
    if (result_count > expected_top_count) return error.InvalidGraphMetricSegment;
    {
        const required_blocks = result_count / metric_segment.codec.ranked_score_block_entries +
            @intFromBool(result_count % metric_segment.codec.ranked_score_block_entries != 0);
        const scores = try alloc.alloc(Score, result_count);
        var initialized: usize = 0;
        errdefer {
            for (scores[0..initialized]) |*score| score.deinit(alloc);
            alloc.free(scores);
        }
        var result_bytes = std.math.mul(usize, result_count, @sizeOf(Score)) catch return error.GraphMetricQueryBudgetExceeded;
        var block_cursor: usize = 0;
        var boundary_validator = RankedScoreBoundaryValidator{};
        while (block_cursor < required_blocks) {
            var range_end = block_cursor;
            var range_bytes: usize = 0;
            while (range_end < required_blocks) : (range_end += 1) {
                const entry_len = routing.ranked_entries[range_end].len;
                if (range_bytes > 0 and (range_bytes >= ranked_fetch_window_bytes or entry_len > ranked_fetch_window_bytes - range_bytes)) break;
                range_bytes = std.math.add(usize, range_bytes, entry_len) catch return error.GraphMetricQueryBudgetExceeded;
            }
            const range_entries = routing.ranked_entries[block_cursor..range_end];
            const ranked_payload = try fetchRankedScoreBlocksAlloc(alloc, session, metric_index, range_entries);
            defer session.alloc.free(ranked_payload);
            for (range_entries) |entry| {
                try session.checkCancellation();
                const relative_offset = std.math.cast(usize, entry.offset -| range_entries[0].offset) orelse return error.InvalidGraphMetricSegment;
                if (relative_offset > ranked_payload.len or entry.len > ranked_payload.len - relative_offset) return error.InvalidGraphMetricSegment;
                const decoded = try metric_segment.codec.decodeRankedScoreBlockWithCancellation(
                    ranked_payload[relative_offset..][0..entry.len],
                    session.cancellation,
                );
                const expected_block_count = @min(
                    metric_segment.codec.ranked_score_block_entries,
                    expected_top_count - initialized,
                );
                try session.chargeGraphMetricDecode(1, expected_block_count);
                if (decoded.len != expected_block_count) return error.InvalidGraphMetricSegment;
                try boundary_validator.observeBlock(decoded);
                for (decoded.scores[0..@min(decoded.len, result_count - initialized)]) |score| {
                    result_bytes = std.math.add(usize, result_bytes, score.nodeIdLen(decoded.node_prefix)) catch return error.GraphMetricQueryBudgetExceeded;
                    if (result_bytes > limits.max_result_bytes) return error.GraphMetricQueryBudgetExceeded;
                    scores[initialized] = .{ .node_id = try score.dupeNodeAlloc(alloc, decoded.node_prefix), .value = score.value };
                    initialized += 1;
                }
            }
            block_cursor = range_end;
        }
        if (initialized != result_count) return error.InvalidGraphMetricSegment;
        try session.chargeGraphMetricRetained(result_bytes);
        var edge_filter = try config.edge_filter.cloneAlloc(alloc);
        errdefer edge_filter.deinit(alloc);
        return .{
            .scores = scores,
            .config_fingerprint = control.header.config_fingerprint,
            .converged = control.header.converged,
            .iterations_completed = control.header.iterations_completed,
            .delta = control.header.delta,
            .edge_filter = edge_filter,
            .metadata_version = control.header.version,
            .published_generation = if (metric_artifact.published_generation != 0) metric_artifact.published_generation else session.manifest.version,
            .edge_generation = if (metric_artifact.edge_generation != 0) metric_artifact.edge_generation else session.manifest.version,
            .computed_at_ms = if (metric_artifact.computed_at_ms != 0) metric_artifact.computed_at_ms else @divTrunc(session.manifest.built_at_ns, std.time.ns_per_ms),
        };
    }
}

fn findConfig(
    specs: []const graph_metric_config.IndexSpec,
    graph_index_name: []const u8,
    metric_name: []const u8,
) ?graph_mod.GraphMetricConfig {
    for (specs) |spec| {
        if (!std.mem.eql(u8, spec.index_name, graph_index_name)) continue;
        for (spec.configs) |config| if (std.mem.eql(u8, config.name, metric_name)) return config;
        return null;
    }
    return null;
}

fn validateControl(
    header: metric_segment.codec.Header,
    graph_artifact: anytype,
    config: graph_mod.GraphMetricConfig,
) !void {
    if (header.kind != config.kind or header.config_fingerprint != lake_graph_metric.configFingerprint(config)) {
        return error.MetricStale;
    }
    if (!std.mem.eql(u8, header.source_graph_artifact_id, graph_artifact.artifact_id) or
        !std.mem.eql(u8, header.source_graph_checksum, graph_artifact.checksum)) return error.MetricStale;
    if (header.materializer_fingerprint != graph_metric_policy.materializerFingerprint(.{})) {
        return error.GraphMetricPolicyStale;
    }
}

fn loadVerifiedAlloc(alloc: Allocator, session: *runtime_mod.QuerySession, graph_index_name: []const u8, metric_name: []const u8) !metric_segment.Segment {
    try session.checkCancellation();
    const specs = try session.graphMetricSpecs();
    const config = findConfig(specs, graph_index_name, metric_name) orelse return error.MetricNotConfigured;
    const graph_index = session.findNamedArtifactIndex(.graph_segment, graph_index_name) orelse return error.GraphSegmentNotFound;
    const graph_artifact = session.artifactRef(graph_index).?;
    const name = try metric_segment.artifactNameAlloc(alloc, graph_index_name, metric_name);
    defer alloc.free(name);
    const metric_index = session.findNamedArtifactIndex(.graph_metric_segment, name) orelse return error.MetricNotReady;
    const metric_artifact = session.artifactRef(metric_index) orelse return error.InvalidGraphMetricSegment;
    const artifact_len = std.math.cast(usize, metric_artifact.byte_len) orelse return error.GraphMetricQueryBudgetExceeded;
    try session.chargeGraphMetricRange(artifact_len);
    try session.chargeGraphMetricDecode(1, std.math.divCeil(usize, artifact_len, 12) catch return error.GraphMetricQueryBudgetExceeded);
    const retained_bytes = std.math.mul(usize, artifact_len, 2) catch return error.GraphMetricQueryBudgetExceeded;
    try session.chargeGraphMetricRetained(retained_bytes);
    const payload = try session.fetchArtifactAlloc(metric_index);
    defer session.alloc.free(payload);
    var segment = try metric_segment.decodeAllocWithCancellation(alloc, payload, session.cancellation);
    errdefer segment.deinit(alloc);
    // The authenticated payload owns its schema version. Manifest provenance
    // is optional and must not override the decoded wire contract.
    segment.published_generation = if (metric_artifact.published_generation != 0) metric_artifact.published_generation else session.manifest.version;
    segment.edge_generation = if (metric_artifact.edge_generation != 0) metric_artifact.edge_generation else session.manifest.version;
    segment.computed_at_ms = if (metric_artifact.computed_at_ms != 0) metric_artifact.computed_at_ms else @divTrunc(session.manifest.built_at_ns, std.time.ns_per_ms);
    if (segment.kind != config.kind or segment.config_fingerprint != lake_graph_metric.configFingerprint(config) or
        !segment.edge_filter.equivalent(config.edge_filter)) return error.MetricStale;
    if (!std.mem.eql(u8, segment.source_graph_artifact_id, graph_artifact.artifact_id) or !std.mem.eql(u8, segment.source_graph_checksum, graph_artifact.checksum)) return error.MetricStale;
    if (segment.materializer_fingerprint != graph_metric_policy.materializerFingerprint(.{})) return error.GraphMetricPolicyStale;
    if (segment.materialization_state == .rejected) return switch (segment.rejection_reason) {
        .build_budget_exceeded => {
            recordRejectionDiagnostic(session, graph_index_name, metric_name, segment.materializer_fingerprint);
            return error.GraphMetricMaterializationRejected;
        },
        .none => error.InvalidGraphMetricSegment,
    };
    return segment;
}

test "serverless graph metric column reads bound shape and accept empty dependencies" {
    const alloc = std.testing.allocator;
    var unused_session: runtime_mod.QuerySession = undefined;
    var empty = try scoreColumnsAlloc(alloc, &unused_session, "graph_idx", &.{}, &.{});
    defer empty.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), empty.columns.len);

    const too_many: [max_point_score_columns + 1][]const u8 = @splat("rank");
    try std.testing.expectError(
        error.GraphMetricQueryBudgetExceeded,
        scoreColumnsAlloc(alloc, &unused_session, "graph_idx", &too_many, &.{}),
    );
}

test "serverless graph metric top limit cannot exceed the persisted ranked tier" {
    const alloc = std.testing.allocator;
    var unused_session: runtime_mod.QuerySession = undefined;
    const oversized = metric_segment.codec.max_persisted_top_entries + 1;
    try std.testing.expectError(
        error.GraphMetricQueryBudgetExceeded,
        topWithLimitsAlloc(alloc, &unused_session, "graph_idx", "rank", oversized, .{ .max_top_k = oversized }),
    );
}

test "serverless graph metric routing batches coalesce selected pages within byte bounds" {
    const alloc = std.testing.allocator;
    var entries: [5]metric_segment.codec.RoutingEntry = undefined;
    for (&entries, 0..) |*entry, i| entry.* = .{ .block_index = i * metric_segment.codec.routing_page_entries, .first_node_id = "", .offset = i * 1024, .len = 1024 };
    const ranges = try planRoutingPageRangesAlloc(alloc, &entries, &.{ 0, 0, 1, 3, 4 });
    defer alloc.free(ranges);
    try std.testing.expectEqual(@as(usize, 2), ranges.len);
    try std.testing.expectEqual(@as(usize, 2048), ranges[0].len);
    try std.testing.expectEqual(@as(usize, 3), ranges[1].first_block);
    try std.testing.expectError(error.InvalidGraphMetricSegment, planRoutingPageRangesAlloc(alloc, &entries, &.{ 2, 1 }));
    try std.testing.expectError(error.InvalidGraphMetricSegment, planRoutingPageRangesAlloc(alloc, &entries, &.{5}));
    entries[0].len = coalesced_score_window_bytes;
    entries[1].offset = entries[0].len;
    const bounded = try planRoutingPageRangesAlloc(alloc, &entries, &.{ 0, 1 });
    defer alloc.free(bounded);
    try std.testing.expectEqual(@as(usize, 2), bounded.len);
    entries[0].offset = std.math.maxInt(u64);
    try std.testing.expectError(error.InvalidGraphMetricSegment, planRoutingPageRangesAlloc(alloc, &entries, &.{0}));
}

test "serverless graph metric joint admission gives uneven columns their actual request costs" {
    const alloc = std.testing.allocator;
    var heavy: [8]metric_segment.codec.RoutingEntry = undefined;
    for (&heavy, 0..) |*entry, i| entry.* = .{ .first_node_id = "node", .block_index = i * 128, .offset = i * 16 * 1024 * 1024, .len = 1024 };
    const counts: [8]usize = @splat(1);
    var inputs: [8]RangePlanningColumn = undefined;
    inputs[0] = .{ .entries = &heavy, .counts = &counts, .score_data_offset = 0 };
    for (inputs[1..]) |*input| input.* = .{ .entries = heavy[0..1], .counts = counts[0..1], .score_data_offset = 0 };
    // Metadata/routing cost 25, scores cost 15: forty requests total.
    // No column quota may reject the eight disconnected exact score ranges.
    const ranges = try planScoreColumnsRangesAlloc(alloc, &inputs, 15);
    defer {
        for (ranges) |column| alloc.free(column);
        alloc.free(ranges);
    }
    try std.testing.expectEqual(@as(usize, 8), ranges[0].len);
    for (ranges[1..]) |column| try std.testing.expectEqual(@as(usize, 1), column.len);
    try std.testing.expectError(error.GraphMetricQueryBudgetExceeded, planScoreColumnsRangesAlloc(alloc, &inputs, 14));
    const Runner = struct {
        fn run(a: Allocator, columns: []const RangePlanningColumn) !void {
            const result = try planScoreColumnsRangesAlloc(a, columns, 15);
            defer a.free(result);
            for (result) |column| a.free(column);
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, Runner.run, .{@as([]const RangePlanningColumn, &inputs)});
}

test "serverless graph metric joint coalescing spends overfetch only where it saves most" {
    const alloc = std.testing.allocator;
    var expensive: [4]metric_segment.codec.RoutingEntry = undefined;
    var cheap: [4]metric_segment.codec.RoutingEntry = undefined;
    for (&expensive, &cheap, 0..) |*large, *small, i| {
        large.* = .{ .first_node_id = "node", .offset = i * 1024 * 1024, .len = 1024 * 1024 };
        small.* = .{ .first_node_id = "node", .offset = i * 1024, .len = 1024 };
    }
    const inputs = [_]RangePlanningColumn{
        .{ .entries = &expensive, .counts = &.{ 1, 0, 0, 1 }, .score_data_offset = 0 },
        .{ .entries = &cheap, .counts = &.{ 1, 1, 1, 1 }, .score_data_offset = 0 },
    };
    const ranges = try planScoreColumnsRangesAlloc(alloc, &inputs, 3);
    defer {
        for (ranges) |column| alloc.free(column);
        alloc.free(ranges);
    }
    try std.testing.expectEqual(@as(usize, 2), ranges[0].len);
    try std.testing.expectEqual(@as(usize, 1), ranges[1].len);
    try std.testing.expectEqual(@as(usize, 4096), ranges[1][0].len);
    const Runner = struct {
        fn run(a: Allocator, columns: []const RangePlanningColumn) !void {
            const result = try planScoreColumnsRangesAlloc(a, columns, 3);
            defer a.free(result);
            for (result) |column| a.free(column);
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, Runner.run, .{@as([]const RangePlanningColumn, &inputs)});
}

test "serverless graph metric joint admission finds the byte-feasible alternative to greedy coalescing" {
    const alloc = std.testing.allocator;
    const mib = 1024 * 1024;
    var a: [8]metric_segment.codec.RoutingEntry = undefined;
    var b: [4]metric_segment.codec.RoutingEntry = undefined;
    for (&a, 0..) |*entry, i| entry.* = .{ .first_node_id = "node", .offset = i * mib, .len = mib };
    for (&b, 0..) |*entry, i| entry.* = .{ .first_node_id = "node", .offset = i * mib, .len = mib };
    const inputs = [_]RangePlanningColumn{
        .{ .entries = &a, .counts = &.{ 1, 1, 1, 0, 0, 0, 0, 1 }, .score_data_offset = 0 },
        .{ .entries = &b, .counts = &.{ 1, 0, 0, 1 }, .score_data_offset = 0 },
    };
    // Greedy selected A (3 total reads / 10 MiB), although B coalesced
    // alongside exact A fits both limits (5 total reads / 8 MiB).
    const result = try planScoreColumnsWithinBudgetAlloc(alloc, &inputs, 5, 9 * mib, .{});
    defer {
        for (result) |ranges| alloc.free(ranges);
        alloc.free(result);
    }
    try std.testing.expectEqual(@as(usize, 4), result[0].len);
    try std.testing.expectEqual(@as(usize, 1), result[1].len);
    try std.testing.expectEqual(@as(usize, 4 * mib), result[1][0].len);
    try std.testing.expectError(error.GraphMetricQueryBudgetExceeded, planScoreColumnsWithinBudgetAlloc(alloc, &inputs, 5, 7 * mib, .{}));
    const Runner = struct {
        fn run(failing: Allocator, columns: []const RangePlanningColumn) !void {
            const ranges = try planScoreColumnsWithinBudgetAlloc(failing, columns, 5, 9 * 1024 * 1024, .{});
            defer failing.free(ranges);
            for (ranges) |column| failing.free(column);
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, Runner.run, .{@as([]const RangePlanningColumn, &inputs)});
}

test "serverless graph metric bounded admission matches exhaustive window choices" {
    const alloc = std.testing.allocator;
    for (0..32) |seed| {
        var entries: [3][4]metric_segment.codec.RoutingEntry = undefined;
        var counts: [3][4]usize = undefined;
        var inputs: [3]RangePlanningColumn = undefined;
        var exact_counts: [3]usize = @splat(0);
        var exact_bytes: [3]usize = @splat(0);
        var window_bytes: [3]usize = undefined;
        for (&entries, &counts, &inputs, 0..) |*column, *hits, *input, i| {
            const mask = (seed + i * 5) % 15 + 1;
            const block_bytes = (i + 1) * 1024;
            for (column, hits, 0..) |*entry, *count, j| {
                entry.* = .{ .first_node_id = "node", .offset = j * block_bytes, .len = block_bytes };
                count.* = @intFromBool(mask & (@as(usize, 1) << @intCast(j)) != 0);
                exact_counts[i] += count.*;
                exact_bytes[i] += count.* * block_bytes;
            }
            input.* = .{ .entries = column, .counts = hits, .score_data_offset = 0 };
            var first: usize = 0;
            while (hits[first] == 0) : (first += 1) {}
            var last: usize = 3;
            while (hits[last] == 0) : (last -= 1) {}
            window_bytes[i] = (last - first + 1) * block_bytes;
        }
        for (1..13) |limit| {
            var minimum: usize = std.math.maxInt(usize);
            for (0..8) |mask| {
                var reads: usize = 0;
                var bytes: usize = 0;
                for (0..3) |i| {
                    const coalesced = mask & (@as(usize, 1) << @intCast(i)) != 0;
                    reads += if (coalesced) 1 else exact_counts[i];
                    bytes += if (coalesced) window_bytes[i] else exact_bytes[i];
                }
                if (reads <= limit) minimum = @min(minimum, bytes);
            }
            if (minimum == std.math.maxInt(usize)) {
                try std.testing.expectError(error.GraphMetricQueryBudgetExceeded, planScoreColumnsRangesAlloc(alloc, &inputs, limit));
                continue;
            }
            const result = try planScoreColumnsWithinBudgetAlloc(alloc, &inputs, limit, minimum, .{});
            defer {
                for (result) |ranges| alloc.free(ranges);
                alloc.free(result);
            }
            var bytes: usize = 0;
            var reads: usize = 0;
            for (result) |ranges| for (ranges) |range| {
                bytes += range.len;
                reads += 1;
            };
            try std.testing.expectEqual(minimum, bytes);
            try std.testing.expect(reads <= limit);
            try std.testing.expectError(error.GraphMetricQueryBudgetExceeded, planScoreColumnsWithinBudgetAlloc(alloc, &inputs, limit, minimum - 1, .{}));
        }
    }
}

test "serverless graph metric cached gaps only cost overfetch when admission requires it" {
    const alloc = std.testing.allocator;
    var entries: [4]metric_segment.codec.RoutingEntry = undefined;
    for (&entries, 0..) |*entry, i| entry.* = .{ .first_node_id = "node", .offset = i * 1024, .len = 1024 };
    const inputs = [_]RangePlanningColumn{.{ .entries = &entries, .counts = &.{ 1, 0, 1, 0 }, .score_data_offset = 0 }};
    const bridged = try planScoreColumnsWithinBudgetAlloc(alloc, &inputs, 1, 3072, .{});
    defer {
        for (bridged) |ranges| alloc.free(ranges);
        alloc.free(bridged);
    }
    try std.testing.expectEqual(@as(usize, 1), bridged[0].len);
    try std.testing.expectEqual(@as(usize, 3072), bridged[0][0].len);
    const result = try planScoreColumnsRangesAlloc(alloc, &inputs, 2);
    defer {
        for (result) |ranges| alloc.free(ranges);
        alloc.free(result);
    }
    try std.testing.expectEqual(@as(usize, 2), result[0].len);
    for (result[0]) |range| try std.testing.expectEqual(@as(usize, 1024), range.len);
    const Cancel = struct {
        fn isCancelled(_: *const anyopaque) bool {
            return true;
        }
    };
    const canceled = true;
    try std.testing.expectError(error.Canceled, planScoreColumnsWithinBudgetAlloc(alloc, &inputs, 2, 4096, .{ .ptr = &canceled, .is_cancelled_fn = Cancel.isCancelled }));
}

test "serverless graph metric range planning caps broad point batches" {
    const alloc = std.testing.allocator;
    var entries: [128]metric_segment.codec.RoutingEntry = undefined;
    var counts: [128]usize = @splat(1);
    for (&entries, 0..) |*entry, index| entry.* = .{
        .first_node_id = "node",
        .offset = index * 64 * 1024,
        .len = 64 * 1024,
    };
    const broad = try planScoreFetchRangesAlloc(alloc, &entries, &counts, 0, max_score_range_requests);
    defer alloc.free(broad);
    try std.testing.expectEqual(@as(usize, 1), broad.len);
    try std.testing.expectEqual(@as(usize, 128), broad[0].last_block - broad[0].first_block + 1);
    try std.testing.expectEqual(coalesced_score_window_bytes, broad[0].len);

    // The old threshold was 32 and caused the 33rd touched block to jump from
    // exact reads to full windows. Moderate point batches now stay exact.
    @memset(&counts, 0);
    @memset(counts[0..33], 1);
    const moderate = try planScoreFetchRangesAlloc(alloc, &entries, &counts, 0, max_score_range_requests);
    defer alloc.free(moderate);
    try std.testing.expectEqual(@as(usize, 33), moderate.len);
    for (moderate) |range| try std.testing.expectEqual(range.first_block, range.last_block);

    @memset(&counts, 0);
    counts[0] = 1;
    counts[counts.len - 1] = 1;
    const sparse = try planScoreFetchRangesAlloc(alloc, &entries, &counts, 0, max_score_range_requests);
    defer alloc.free(sparse);
    try std.testing.expectEqual(@as(usize, 2), sparse.len);
    try std.testing.expectEqual(@as(usize, 64 * 1024), sparse[0].len);
    try std.testing.expectEqual(@as(usize, 64 * 1024), sparse[1].len);

    var unused_session: runtime_mod.QuerySession = undefined;
    const oversized_batch: [max_parallel_metric_range_requests + 1]ScoreFetchRange = @splat(.{
        .first_block = 0,
        .last_block = 0,
        .offset = 0,
        .len = 1,
    });
    try std.testing.expectError(
        error.GraphMetricQueryBudgetExceeded,
        fetchMetricRangeBatchAlloc(alloc, &unused_session, 0, 0, &.{}, &oversized_batch, .score),
    );
}

test "serverless graph metric sparse planning stays proportional to touched blocks" {
    const alloc = std.testing.allocator;
    var entries: [4096]metric_segment.codec.RoutingEntry = undefined;
    for (&entries, 0..) |*entry, index| entry.* = .{
        .first_node_id = "node",
        .offset = index * 64,
        .len = 64,
    };
    const touched = [_]struct { block_index: usize, first_pending: usize, pending_count: usize }{
        .{ .block_index = 3072, .first_pending = 0, .pending_count = 1 },
    };
    const ranges = try planSparseScoreFetchRangesAlloc(alloc, &entries, &touched, 0, max_score_range_requests);
    defer alloc.free(ranges);
    try std.testing.expectEqual(@as(usize, 1), ranges.len);
    try std.testing.expectEqual(@as(usize, 3072), ranges[0].first_block);
    try std.testing.expectEqual(@as(usize, 64), ranges[0].len);
}

test "serverless graph metric transport windows trim unused boundary blocks" {
    const alloc = std.testing.allocator;
    var entries: [128]metric_segment.codec.RoutingEntry = undefined;
    for (&entries, 0..) |*entry, index| entry.* = .{
        .first_node_id = "node",
        .offset = index * 64 * 1024,
        .len = 64 * 1024,
    };
    var first_counts: [128]usize = @splat(1);
    var second_counts: [128]usize = @splat(1);
    first_counts[0] = 0;
    second_counts[1] = 0;
    const first = try planScoreFetchRangesAlloc(alloc, &entries, &first_counts, 0, max_score_range_requests);
    defer alloc.free(first);
    const second = try planScoreFetchRangesAlloc(alloc, &entries, &second_counts, 0, max_score_range_requests);
    defer alloc.free(second);
    try std.testing.expectEqual(@as(usize, 1), first.len);
    try std.testing.expectEqual(@as(usize, 1), second.len);
    try std.testing.expectEqual(@as(u64, 64 * 1024), first[0].offset);
    try std.testing.expectEqual(@as(usize, 127 * 64 * 1024), first[0].len);
    try std.testing.expectEqual(@as(u64, 0), second[0].offset);
    try std.testing.expectEqual(@as(usize, 128 * 64 * 1024), second[0].len);
}

test "serverless graph metric top range planning coalesces contiguous blocks" {
    const alloc = std.testing.allocator;
    var entries: [977]metric_segment.codec.RoutingEntry = undefined;
    for (&entries, 0..) |*entry, index| entry.* = .{
        .first_node_id = "node",
        .offset = index * 256 * 1024,
        .len = 256 * 1024,
    };
    const ranges = try planAllScoreFetchRangesAlloc(alloc, &entries);
    defer alloc.free(ranges);
    try std.testing.expectEqual(@as(usize, 31), ranges.len);
    for (ranges) |range| try std.testing.expect(range.len <= coalesced_score_window_bytes);
}

test "serverless graph metric point reads authenticate before fetching ranges" {
    const alloc = std.testing.allocator;
    const State = struct {
        verify_calls: usize = 0,
        range_calls: usize = 0,

        fn deinit(_: Allocator, _: *anyopaque) void {}
        fn put(_: *anyopaque, _: Allocator, _: []const u8) !artifacts_mod.ArtifactMetadata {
            return error.UnexpectedPut;
        }
        fn getAlloc(_: *anyopaque, _: Allocator, _: []const u8) ![]u8 {
            return error.UnexpectedFullRead;
        }
        fn getRangeAlloc(ptr: *anyopaque, alloc_: Allocator, _: []const u8, _: u64, len: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.range_calls += 1;
            const payload = try alloc_.alloc(u8, len);
            @memset(payload, 0);
            return payload;
        }
        fn stat(_: *anyopaque, _: Allocator, _: []const u8) !artifacts_mod.ArtifactMetadata {
            return error.UnexpectedStat;
        }
        fn verifyContent(ptr: *anyopaque, _: Allocator, _: []const u8, _: u64, _: []const u8, _: @import("../../common/cancellation.zig").CancellationToken) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.verify_calls += 1;
            return error.ArtifactIntegrityMismatch;
        }
        fn delete(_: *anyopaque, _: []const u8) !void {
            return error.UnexpectedDelete;
        }

        const vtable = artifacts_mod.ArtifactStore.VTable{
            .deinit = deinit,
            .put = put,
            .get_alloc = getAlloc,
            .get_range_alloc = getRangeAlloc,
            .stat = stat,
            .verify_content = verifyContent,
            .delete = delete,
        };
    };
    const checksum = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const artifact_id = "sha256:" ++ checksum;
    var state = State{};
    var artifacts = artifacts_mod.ArtifactStore{ .allocator = alloc, .ptr = &state, .vtable = &State.vtable };
    defer artifacts.deinit();
    var refs = [_]manifest_mod.ArtifactRef{
        .{ .kind = .graph_segment, .name = "graph_idx", .artifact_id = artifact_id, .byte_len = 1, .checksum = checksum },
        .{ .kind = .graph_metric_segment, .name = "9:graph_idx4:rank", .artifact_id = artifact_id, .byte_len = 100, .checksum = checksum, .metadata_version = metric_segment.wire_version },
    };
    refs[1].graph_metric_control_len = @intCast(try metric_segment.controlProbeLen(
        refs[1].byte_len,
        refs[0].artifact_id,
        refs[0].checksum,
        .{},
    ));
    // Session fetch buffers and result buffers deliberately have different
    // owners. This exercises direct reads as well as forked column reads.
    var session_arena = std.heap.ArenaAllocator.init(alloc);
    defer session_arena.deinit();
    var session = runtime_mod.QuerySession{
        .alloc = session_arena.allocator(),
        .artifacts = &artifacts,
        .manifest = .{
            .namespace = "docs",
            .version = 1,
            .built_at_ns = 1,
            .wal_start_lsn = 1,
            .wal_end_lsn = 1,
            .stats = .{ .indexes_json = @constCast("{\"graph_idx\":{\"type\":\"graph\",\"metrics\":{\"rank\":{\"kind\":\"pagerank\"}}}}") },
            .artifacts = &refs,
        },
    };
    defer session.clearGraphMetricSpecs();
    const cached_specs = try session.graphMetricSpecs();
    const cached_specs_again = try session.graphMetricSpecs();
    try std.testing.expect(cached_specs.ptr == cached_specs_again.ptr);
    const node_ids = [_][]const u8{"node"};
    try std.testing.expectError(error.InvalidGraphMetricSegment, scoresAlloc(alloc, &session, "graph_idx", "rank", &node_ids));
    try std.testing.expectEqual(@as(usize, 0), state.verify_calls);
    try std.testing.expectEqual(@as(usize, 1), state.range_calls);
}

test "serverless graph metric point and top-1025 reads authenticate bounded ranges without full scans" {
    try testAuthenticatedMetricReads(metric_segment.score_block_entries + 1);
}

test "serverless graph metric point routing reads only selected pages of a large index" {
    try testAuthenticatedMetricReadsWithPrefix(2 * metric_segment.codec.routing_page_entries * metric_segment.score_block_entries + 1, "x" ** 256);
}

test "serverless graph metric small byte indexes keep a single routing read across pages" {
    try testAuthenticatedMetricReads(2 * metric_segment.codec.routing_page_entries * metric_segment.score_block_entries + 1);
}

fn testAuthenticatedMetricReads(score_count: usize) !void {
    try testAuthenticatedMetricReadsWithPrefix(score_count, "");
}

fn testAuthenticatedMetricReadsWithPrefix(score_count: usize, prefix: []const u8) !void {
    const alloc = std.testing.allocator;
    const source_checksum = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const source_artifact_id = "sha256:" ++ source_checksum;
    const config = graph_mod.GraphMetricConfig{ .name = "rank" };
    var metric = metric_segment.Segment{
        .kind = .pagerank,
        .source_graph_artifact_id = try alloc.dupe(u8, source_artifact_id),
        .source_graph_checksum = try alloc.dupe(u8, source_checksum),
        .config_fingerprint = lake_graph_metric.configFingerprint(config),
        .materializer_fingerprint = graph_metric_policy.materializerFingerprint(.{}),
        .edge_filter = .{},
        .converged = true,
        .iterations_completed = 2,
        .delta = 0.001,
        .scores = try alloc.alloc(metric_segment.Score, score_count),
    };
    for (metric.scores, 0..) |*score, index| {
        score.* = .{
            .node_id = try std.fmt.allocPrint(alloc, "node:{s}{d:0>8}", .{ prefix, index }),
            .value = @floatFromInt(index),
        };
    }
    defer metric.deinit(alloc);
    const payload = try metric_segment.encodeAlloc(alloc, metric);
    defer alloc.free(payload);
    const integrity = try metric_segment.artifactIntegrity(metric, payload);
    var metric_digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(payload, &metric_digest, .{});
    const metric_checksum = std.fmt.bytesToHex(metric_digest, .lower);
    var metric_artifact_id: [artifacts_mod.store.sha256_artifact_id_prefix.len + metric_checksum.len]u8 = undefined;
    @memcpy(metric_artifact_id[0..artifacts_mod.store.sha256_artifact_id_prefix.len], artifacts_mod.store.sha256_artifact_id_prefix);
    @memcpy(metric_artifact_id[artifacts_mod.store.sha256_artifact_id_prefix.len..], &metric_checksum);

    const State = struct {
        payload: []const u8,
        control_len: usize,
        footer_offset: usize,
        root_offset: usize,
        range_calls: std.atomic.Value(usize) = .init(0),
        control_calls: std.atomic.Value(usize) = .init(0),
        required_controls_before_scores: usize = 0,
        verify_calls: std.atomic.Value(usize) = .init(0),
        corrupt_score_reads: bool = false,
        reject_point_index_reads: bool = false,
        corrupt_point_index: bool = false,
        corrupt_routing_page: bool = false,
        corrupt_routing_root: bool = false,

        fn deinit(_: Allocator, _: *anyopaque) void {}
        fn put(_: *anyopaque, _: Allocator, _: []const u8) !artifacts_mod.ArtifactMetadata {
            return error.UnexpectedPut;
        }
        fn getAlloc(_: *anyopaque, _: Allocator, _: []const u8) ![]u8 {
            return error.UnexpectedFullRead;
        }
        fn getRangeAlloc(ptr: *anyopaque, result_alloc: Allocator, _: []const u8, offset: u64, len: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = self.range_calls.fetchAdd(1, .monotonic);
            const start = std.math.cast(usize, offset) orelse return error.InvalidRange;
            if (start > self.payload.len or len > self.payload.len - start) return error.InvalidRange;
            const touches_point_index = start < self.root_offset and start + len > self.footer_offset;
            if (start == 0) _ = self.control_calls.fetchAdd(1, .monotonic);
            if (start >= self.control_len and start < self.footer_offset and
                self.control_calls.load(.monotonic) < self.required_controls_before_scores) return error.ScoreReadBeforeWholeQueryPrepared;
            if (self.reject_point_index_reads and touches_point_index) return error.UnexpectedPointIndexRead;
            const out = try result_alloc.dupe(u8, self.payload[start..][0..len]);
            if (self.corrupt_point_index and touches_point_index) out[@max(start, self.footer_offset) - start] ^= 1;
            if (self.corrupt_routing_page and touches_point_index and !std.mem.startsWith(u8, self.payload[start..], "AFGD")) out[0] ^= 1;
            if (self.corrupt_routing_root and start <= self.root_offset and start + len > self.root_offset) out[self.root_offset - start] ^= 1;
            if (self.corrupt_score_reads and start >= self.control_len and start < self.footer_offset and out.len > @sizeOf(u32)) {
                out[@sizeOf(u32)] ^= 0x01;
            }
            return out;
        }
        fn stat(_: *anyopaque, _: Allocator, _: []const u8) !artifacts_mod.ArtifactMetadata {
            return error.UnexpectedStat;
        }
        fn verifyContent(ptr: *anyopaque, _: Allocator, _: []const u8, _: u64, _: []const u8, _: @import("../../common/cancellation.zig").CancellationToken) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = self.verify_calls.fetchAdd(1, .monotonic);
            return error.UnexpectedFullVerification;
        }
        fn delete(_: *anyopaque, _: []const u8) !void {
            return error.UnexpectedDelete;
        }

        const vtable = artifacts_mod.ArtifactStore.VTable{
            .deinit = deinit,
            .put = put,
            .get_alloc = getAlloc,
            .get_range_alloc = getRangeAlloc,
            .stat = stat,
            .verify_content = verifyContent,
            .delete = delete,
        };
    };
    var state = State{
        .payload = payload,
        .control_len = integrity.control_len,
        .footer_offset = payload.len - integrity.routing_footer_len,
        .root_offset = payload.len - metric_segment.codec.routingRootLen(metric.scores.len),
    };
    var artifacts = artifacts_mod.ArtifactStore{ .allocator = alloc, .ptr = &state, .vtable = &State.vtable };
    defer artifacts.deinit();
    var refs = [_]manifest_mod.ArtifactRef{
        .{ .kind = .graph_segment, .name = "graph_idx", .artifact_id = source_artifact_id, .byte_len = 1, .checksum = source_checksum },
        .{
            .kind = .graph_metric_segment,
            .name = "9:graph_idx4:rank",
            .artifact_id = &metric_artifact_id,
            .byte_len = payload.len,
            .checksum = &metric_checksum,
            .metadata_version = metric_segment.wire_version,
            .materializer_fingerprint = metric.materializer_fingerprint,
            .graph_metric_control_len = integrity.control_len,
            .graph_metric_routing_footer_len = integrity.routing_footer_len,
            .graph_metric_control_checksum = integrity.control_checksum,
            .graph_metric_routing_checksum = integrity.routing_checksum,
            .graph_metric_point_index_checksum = integrity.point_index_checksum,
            .graph_metric_config_fingerprint = metric.config_fingerprint,
            .graph_metric_source_checksum = @splat(0xaa),
        },
    };
    var session_arena = std.heap.ArenaAllocator.init(alloc);
    defer session_arena.deinit();
    var session = runtime_mod.QuerySession{
        .alloc = session_arena.allocator(),
        .artifacts = &artifacts,
        .manifest = .{
            .namespace = "docs",
            .version = 1,
            .built_at_ns = 1,
            .wal_start_lsn = 1,
            .wal_end_lsn = 1,
            .stats = .{ .indexes_json = @constCast("{\"graph_idx\":{\"type\":\"graph\",\"metrics\":{\"rank\":{\"kind\":\"pagerank\"}}}}") },
            .artifacts = &refs,
        },
    };
    defer session.clearGraphMetricSpecs();
    const cached_specs = try session.graphMetricSpecs();
    const cached_specs_again = try session.graphMetricSpecs();
    try std.testing.expect(cached_specs.ptr == cached_specs_again.ptr);
    const last_id = metric.scores[score_count - 1].node_id;
    const last_value: f64 = @floatFromInt(score_count - 1);
    const node_ids = [_][]const u8{last_id};

    var empty_points = try scoresAlloc(alloc, &session, "graph_idx", "rank", &.{});
    defer empty_points.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), empty_points.scores.len);
    // Only the authenticated control/status probe is required; routing and
    // score data remain untouched.
    try std.testing.expectEqual(@as(usize, 1), state.range_calls.load(.monotonic));

    state.range_calls.store(0, .monotonic);
    var result = try scoresAlloc(alloc, &session, "graph_idx", "rank", &node_ids);
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(?f64, last_value), result.scores[0]);
    try std.testing.expectEqual(@as(usize, 0), state.verify_calls.load(.monotonic));
    const paged = score_count > metric_segment.codec.routing_page_entries * metric_segment.score_block_entries and integrity.routing_footer_len > 64 * 1024;
    try std.testing.expectEqual(@as(usize, if (paged) 5 else 3), state.range_calls.load(.monotonic));

    // Exercise disconnected selected pages, duplicate candidates, and misses.
    session.graph_metric_read_budget = .{};
    const mixed_ids = [_][]const u8{ last_id, metric.scores[0].node_id, last_id, "before", "zzzz" };
    var mixed = try scoresAlloc(alloc, &session, "graph_idx", "rank", &mixed_ids);
    defer mixed.deinit(alloc);
    try std.testing.expectEqualSlices(?f64, &.{ last_value, 0, last_value, null, null }, mixed.scores);
    session.graph_metric_read_budget = .{};

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    session.setIo(io_impl.io());
    if (paged) {
        // Sixty distinct score blocks across three routing pages, twice.
        // Metadata and routing must fit alongside the scores in the shared
        // 128-request budget, independent of column scheduling order.
        var broad_ids: [60][]const u8 = undefined;
        for (&broad_ids, 0..) |*id, i| id.* = metric.scores[(i * 128 / 59) * metric_segment.score_block_entries].node_id;
        session.graph_metric_read_budget = .{};
        state.range_calls.store(0, .monotonic);
        var broad = try scoreColumnsAlloc(alloc, &session, "graph_idx", &.{ "rank", "rank" }, &broad_ids);
        defer broad.deinit(alloc);
        for (broad.columns) |column| for (column.scores, 0..) |score, i| {
            try std.testing.expectEqual(@as(?f64, @floatFromInt((i * 128 / 59) * metric_segment.score_block_entries)), score);
        };
        try std.testing.expectEqual(@as(u64, 128), session.graph_metric_read_budget.range_requests);
        try std.testing.expectEqual(@as(usize, 128), state.range_calls.load(.monotonic));
        session.graph_metric_read_budget = .{};
    }
    const column_names = [_][]const u8{ "rank", "rank", "rank" };
    state.control_calls.store(0, .monotonic);
    state.required_controls_before_scores = column_names.len;
    var columns = try scoreColumnsAlloc(alloc, &session, "graph_idx", &column_names, &node_ids);
    state.required_controls_before_scores = 0;
    defer columns.deinit(alloc);
    try std.testing.expectEqual(@as(usize, column_names.len), columns.columns.len);
    for (columns.columns) |column| try std.testing.expectEqual(@as(?f64, last_value), column.scores[0]);

    // All routing fits, but the complete score plan does not. Admission must
    // reject before any column starts score I/O, including earlier cohorts.
    const metadata_requests: u64 = column_names.len * @as(u64, if (paged) 4 else 2);
    session.graph_metric_read_budget = .{ .limits = .{ .max_range_requests = metadata_requests } };
    state.range_calls.store(0, .monotonic);
    state.control_calls.store(0, .monotonic);
    state.required_controls_before_scores = column_names.len;
    try std.testing.expectError(error.GraphMetricQueryBudgetExceeded, scoreColumnsAlloc(alloc, &session, "graph_idx", &column_names, &node_ids));
    state.required_controls_before_scores = 0;
    try std.testing.expectEqual(column_names.len, state.control_calls.load(.monotonic));
    try std.testing.expectEqual(metadata_requests, state.range_calls.load(.monotonic));
    session.graph_metric_read_budget = .{};

    const AllocationRunner = struct {
        fn run(failing_alloc: Allocator, active_session: *runtime_mod.QuerySession, lookup_id: []const u8) !void {
            // Each injected run is one logical request. Workers share this
            // budget, but never the intentionally non-thread-safe allocator.
            active_session.graph_metric_read_budget = .{};
            const names = [_][]const u8{ "rank", "rank", "rank" };
            const ids = [_][]const u8{lookup_id};
            var direct = try scoresAlloc(failing_alloc, active_session, "graph_idx", "rank", &ids);
            defer direct.deinit(failing_alloc);
            var loaded = try scoreColumnsAlloc(failing_alloc, active_session, "graph_idx", &names, &ids);
            defer loaded.deinit(failing_alloc);
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, AllocationRunner.run, .{ &session, last_id });

    state.range_calls.store(0, .monotonic);
    state.reject_point_index_reads = true;
    var top_one = try topWithLimitsAlloc(alloc, &session, "graph_idx", "rank", 1, .{});
    defer top_one.deinit(alloc);
    try std.testing.expectEqualStrings(last_id, top_one.scores[0].node_id);
    try std.testing.expectEqual(@as(usize, 3), state.range_calls.load(.monotonic));
    state.range_calls.store(0, .monotonic);
    var top = try topWithLimitsAlloc(alloc, &session, "graph_idx", "rank", metric_segment.score_block_entries + 1, .{});
    defer top.deinit(alloc);
    try std.testing.expectEqual(@as(usize, metric_segment.score_block_entries + 1), top.scores.len);
    try std.testing.expectEqualStrings(last_id, top.scores[0].node_id);
    try std.testing.expectEqual(last_value, top.scores[0].value);
    try std.testing.expectEqualStrings(metric.scores[score_count - top.scores.len].node_id, top.scores[top.scores.len - 1].node_id);
    try std.testing.expectEqual(@as(usize, 3), state.range_calls.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 0), state.verify_calls.load(.monotonic));

    state.reject_point_index_reads = false;
    state.corrupt_point_index = true;
    try std.testing.expectError(error.InvalidGraphMetricSegment, scoresAlloc(alloc, &session, "graph_idx", "rank", &node_ids));
    state.corrupt_point_index = false;
    state.corrupt_routing_page = true;
    try std.testing.expectError(error.InvalidGraphMetricSegment, scoresAlloc(alloc, &session, "graph_idx", "rank", &node_ids));
    state.corrupt_routing_page = false;
    state.corrupt_routing_root = true;
    try std.testing.expectError(error.InvalidGraphMetricSegment, scoresAlloc(alloc, &session, "graph_idx", "rank", &node_ids));
    try std.testing.expectError(error.InvalidGraphMetricSegment, topWithLimitsAlloc(alloc, &session, "graph_idx", "rank", 1, .{}));
    state.corrupt_routing_root = false;
    state.corrupt_score_reads = true;
    try std.testing.expectError(error.InvalidGraphMetricSegment, scoresAlloc(alloc, &session, "graph_idx", "rank", &node_ids));
    try std.testing.expectError(error.InvalidGraphMetricSegment, topWithLimitsAlloc(
        alloc,
        &session,
        "graph_idx",
        "rank",
        metric_segment.score_block_entries + 1,
        .{},
    ));
    try std.testing.expectEqual(@as(usize, 0), state.verify_calls.load(.monotonic));

    state.corrupt_score_reads = false;
    refs[1].graph_metric_routing_footer_len = @intCast(metric_segment.codec.routingRootLen(score_count) - 1);
    try std.testing.expectError(error.InvalidGraphMetricSegment, scoresAlloc(alloc, &session, "graph_idx", "rank", &node_ids));
    refs[1].graph_metric_routing_footer_len = integrity.routing_footer_len;
    state.range_calls.store(0, .monotonic);
    session.graph_metric_read_budget = .{ .limits = .{
        .max_range_requests = 2,
        .max_range_bytes = std.math.maxInt(u64),
        .max_decoded_blocks = std.math.maxInt(u64),
        .max_work_items = std.math.maxInt(u64),
        .max_retained_bytes = std.math.maxInt(u64),
    } };
    try std.testing.expectError(
        error.GraphMetricQueryBudgetExceeded,
        scoresAlloc(alloc, &session, "graph_idx", "rank", &node_ids),
    );
    // Control and routing are fetched; the score range is rejected before it
    // can reach the backend. The counter is shared by every later metric read
    // performed through this pinned request session.
    try std.testing.expectEqual(@as(usize, 2), state.range_calls.load(.monotonic));

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const cache_root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/routing", .{tmp.sub_path});
    defer alloc.free(cache_root);
    var cache = try @import("cache.zig").QueryCache.init(alloc, cache_root);
    defer cache.deinit();
    session.cache = &cache;
    defer session.cache = null;
    session.graph_metric_read_budget = .{};
    var first_cached = try scoresAlloc(alloc, &session, "graph_idx", "rank", &node_ids);
    defer first_cached.deinit(alloc);
    // A warm lookup decodes one bounded routing page and one score block.
    // Root and directory are leased from the decoded routing cache.
    session.graph_metric_read_budget = .{ .limits = .{ .max_decoded_blocks = if (paged) 2 else 1 } };
    var second_cached = try scoresAlloc(alloc, &session, "graph_idx", "rank", &node_ids);
    defer second_cached.deinit(alloc);
    try std.testing.expectEqual(@as(?f64, last_value), second_cached.scores[0]);
    try std.testing.expectEqual(@as(u64, if (paged) 2 else 1), cache.graph_metric_routing.hits);
    try std.testing.expectEqual(@as(u64, if (paged) 2 else 1), session.graph_metric_read_budget.decoded_blocks);
    if (paged) {
        session.graph_metric_read_budget = .{};
        state.range_calls.store(0, .monotonic);
        const adjacent = [_][]const u8{ metric.scores[0].node_id, metric.scores[64 * metric_segment.score_block_entries].node_id };
        var pair = try scoresAlloc(alloc, &session, "graph_idx", "rank", &adjacent);
        defer pair.deinit(alloc);
        // One coalesced routing miss plus two exact score misses.
        try std.testing.expectEqual(@as(usize, 3), state.range_calls.load(.monotonic));
        session.graph_metric_read_budget = .{};
        state.range_calls.store(0, .monotonic);
        const overlap = [_][]const u8{ adjacent[1], last_id };
        var overlapping = try scoresAlloc(alloc, &session, "graph_idx", "rank", &overlap);
        defer overlapping.deinit(alloc);
        try std.testing.expectEqualSlices(?f64, &.{ @floatFromInt(64 * metric_segment.score_block_entries), last_value }, overlapping.scores);
        var single = try scoresAlloc(alloc, &session, "graph_idx", "rank", adjacent[1..]);
        defer single.deinit(alloc);
        // Coalesced [0,1], overlapping [1,2], and exact [1] share pages.
        try std.testing.expectEqual(@as(usize, 0), state.range_calls.load(.monotonic));

        // Score blocks are also canonical cache units. A broad miss set fits
        // one bounded transport despite interspersed cache hits.
        var broad_ids: [129][]const u8 = undefined;
        for (&broad_ids, 0..) |*id, i| id.* = metric.scores[i * metric_segment.score_block_entries].node_id;
        session.graph_metric_read_budget = .{ .limits = .{ .max_range_requests = 3 } };
        state.corrupt_score_reads = true;
        try std.testing.expectError(error.InvalidGraphMetricSegment, scoresAlloc(alloc, &session, "graph_idx", "rank", &broad_ids));
        state.corrupt_score_reads = false;
        session.graph_metric_read_budget = .{ .limits = .{ .max_range_requests = 3 } };
        state.range_calls.store(0, .monotonic);
        var broad_cached = try scoresAlloc(alloc, &session, "graph_idx", "rank", &broad_ids);
        defer broad_cached.deinit(alloc);
        // The corrupt response did not publish any of its score blocks.
        try std.testing.expectEqual(@as(usize, 1), state.range_calls.load(.monotonic));
        for (broad_cached.scores, 0..) |score, i| try std.testing.expectEqual(@as(?f64, @floatFromInt(i * metric_segment.score_block_entries)), score);
        state.range_calls.store(0, .monotonic);
        // Only the control read is charged. Cached scores need no network
        // admission even when a new candidate set changes the routing pages.
        session.graph_metric_read_budget = .{ .limits = .{ .max_range_requests = 1, .max_range_bytes = integrity.control_len } };
        var crossed = try scoresAlloc(alloc, &session, "graph_idx", "rank", broad_ids[63..66]);
        defer crossed.deinit(alloc);
        for (crossed.scores, 63..) |score, i| try std.testing.expectEqual(@as(?f64, @floatFromInt(i * metric_segment.score_block_entries)), score);
        session.graph_metric_read_budget = .{ .limits = .{ .max_range_requests = 1, .max_range_bytes = integrity.control_len } };
        var one_page = try scoresAlloc(alloc, &session, "graph_idx", "rank", broad_ids[96..97]);
        defer one_page.deinit(alloc);
        try std.testing.expectEqual(@as(?f64, @floatFromInt(96 * metric_segment.score_block_entries)), one_page.scores[0]);
        try std.testing.expectEqual(@as(usize, 0), state.range_calls.load(.monotonic));
    }
    try std.testing.checkAllAllocationFailures(alloc, AllocationRunner.run, .{ &session, last_id });
}

test "serverless graph metric ranked blocks reject cross-boundary inversions and duplicates" {
    var validator = RankedScoreBoundaryValidator{};
    var first = metric_segment.codec.DecodedScoreBlock{};
    first.scores[0] = .{ .node_suffix = "a", .value = 10 };
    first.scores[1] = .{ .node_suffix = "b", .value = 9 };
    first.len = 2;
    try validator.observeBlock(first);

    var valid = metric_segment.codec.DecodedScoreBlock{};
    valid.scores[0] = .{ .node_suffix = "c", .value = 9 };
    valid.scores[1] = .{ .node_suffix = "d", .value = 8 };
    valid.len = 2;
    try validator.observeBlock(valid);

    var inverted_validator = RankedScoreBoundaryValidator{};
    try inverted_validator.observeBlock(first);
    var inverted = metric_segment.codec.DecodedScoreBlock{};
    inverted.scores[0] = .{ .node_suffix = "c", .value = 11 };
    inverted.len = 1;
    try std.testing.expectError(error.InvalidGraphMetricSegment, inverted_validator.observeBlock(inverted));

    var duplicate_validator = RankedScoreBoundaryValidator{};
    try duplicate_validator.observeBlock(first);
    var duplicate = metric_segment.codec.DecodedScoreBlock{};
    duplicate.scores[0] = .{ .node_suffix = "b", .value = 9 };
    duplicate.len = 1;
    try std.testing.expectError(error.InvalidGraphMetricSegment, duplicate_validator.observeBlock(duplicate));
}
