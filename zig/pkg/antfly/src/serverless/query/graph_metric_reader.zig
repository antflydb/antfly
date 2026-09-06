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

    fn savedRequests(self: @This()) usize {
        return self.touched_blocks -| 1;
    }

    fn overfetchBytes(self: @This()) usize {
        return self.len -| self.touched_bytes;
    }
};
// Two point-score columns may run concurrently and share one 128-range request
// budget. Reserve control/footer reads for both while allowing moderate sparse
// candidate sets to retain exact block identities beyond the old 32-block
// coalescing cliff: 2 * (2 + 60) == 124.
const max_score_range_requests: usize = 60;
const max_top_score_range_requests: usize = 64;
const max_parallel_score_range_requests: usize = 8;
const max_point_score_columns: usize = 16;
// A column may itself hold a 16 MiB routing footer and a 32 MiB range batch.
// Two columns retain useful overlap without allowing nested fan-out to turn a
// valid request into an avoidable serverless memory spike.
const max_parallel_point_score_columns: usize = 2;
/// Bound aggregate live payload memory as well as request count. Coalescing
/// deliberately makes ranges variable-sized, so a count-only fanout can turn
/// eight harmless-looking reads into a large transient allocation spike.
const max_parallel_score_range_bytes: usize = 32 * 1024 * 1024;
const coalesced_score_window_bytes: u64 = 8 * 1024 * 1024;
const ranked_fetch_window_bytes: usize = 8 * 1024 * 1024;

fn scoreRangeBatchEnd(ranges: []const ScoreFetchRange, start: usize) usize {
    var end = start;
    var bytes: usize = 0;
    while (end < ranges.len and end - start < max_parallel_score_range_requests) : (end += 1) {
        const next_bytes = std.math.add(usize, bytes, ranges[end].len) catch break;
        if (end > start and next_bytes > max_parallel_score_range_bytes) break;
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

fn scoreColumnWorker(
    child: *runtime_mod.QuerySession,
    graph_index_name: []const u8,
    metric_name: []const u8,
    node_ids: []const []const u8,
    scores: []?f64,
    output: *?PointScoresMetadata,
    failure: *?anyerror,
    cancel_siblings: *std.atomic.Value(bool),
) void {
    output.* = scoresInto(
        std.heap.smp_allocator,
        child,
        graph_index_name,
        metric_name,
        node_ids,
        scores,
    ) catch |err| {
        failure.* = err;
        cancel_siblings.store(true, .release);
        return;
    };
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
    if (metric_names.len > max_point_score_columns) return error.GraphMetricQueryBudgetExceeded;
    if (metric_names.len == 0) return .{ .columns = try alloc.alloc(PointScoresResult, 0) };
    // Parse request-owned configuration before forking: QuerySession's lazy
    // cache is deliberately not synchronized, while the parsed slice is
    // immutable and safe for child sessions to borrow.
    _ = try session.graphMetricSpecs();

    const columns = try alloc.alloc(PointScoresResult, metric_names.len);
    var initialized_columns: usize = 0;
    errdefer {
        for (columns[0..initialized_columns]) |*column| column.deinit(alloc);
        alloc.free(columns);
    }
    if (session.io == null) {
        for (metric_names, 0..) |metric_name, index| {
            columns[index] = try scoresAlloc(alloc, session, graph_index_name, metric_name, node_ids);
            initialized_columns += 1;
        }
        return .{ .columns = columns };
    }

    var start: usize = 0;
    while (start < metric_names.len) {
        const end = @min(start + max_parallel_point_score_columns, metric_names.len);
        const count = end - start;
        var children: [max_parallel_point_score_columns]runtime_mod.QuerySession = undefined;
        var child_diagnostics: [max_parallel_point_score_columns]operation.RequestDiagnostics = @splat(.{});
        var score_buffers: [max_parallel_point_score_columns]?[]?f64 = @splat(null);
        var temporary: [max_parallel_point_score_columns]?PointScoresMetadata = @splat(null);
        var failures: [max_parallel_point_score_columns]?anyerror = @splat(null);
        var sibling_failure = std.atomic.Value(bool).init(false);
        var combined_cancellations: [max_parallel_point_score_columns]CombinedColumnCancellation = undefined;
        defer for (score_buffers[0..count]) |maybe_scores| if (maybe_scores) |scores| alloc.free(scores);

        // Complete every fallible preparation step before the first worker is
        // scheduled. Once group.async has observed the stack-backed child
        // state, returning before group.await would let error cleanup release
        // result buffers while a worker can still write through them.
        for (0..count) |relative_index| {
            score_buffers[relative_index] = try alloc.alloc(?f64, node_ids.len);
        }

        const io = session.io.?;
        var group: std.Io.Group = .init;
        for (metric_names[start..end], 0..) |metric_name, relative_index| {
            children[relative_index] = session.forkGraphMetricRead(std.heap.smp_allocator);
            combined_cancellations[relative_index] = .{
                .parent = session.cancellation,
                .sibling_failure = &sibling_failure,
            };
            children[relative_index].cancellation = combined_cancellations[relative_index].token();
            if (session.diagnostics != null) children[relative_index].setDiagnostics(&child_diagnostics[relative_index]);
            group.async(io, scoreColumnWorker, .{
                &children[relative_index],
                graph_index_name,
                metric_name,
                node_ids,
                score_buffers[relative_index].?,
                &temporary[relative_index],
                &failures[relative_index],
                &sibling_failure,
            });
        }
        const await_result = group.await(io);
        for (children[0..count]) |*child| child.deinit();
        try await_result;
        for (child_diagnostics[0..count]) |diagnostics| {
            const rejection = diagnostics.graph_metric_rejection orelse continue;
            session.recordGraphMetricRejection(
                rejection.graphIndexName(),
                rejection.metricName(),
                rejection.materializer_fingerprint,
            );
            break;
        }
        // A sibling may observe the local cancellation before the originating
        // failure is joined. Preserve the actionable root cause instead of
        // returning that derived cancellation based on array order.
        for (failures[0..count]) |failure| if (failure) |err| {
            if (err != error.Canceled) return err;
        };
        for (failures[0..count]) |failure| if (failure) |err| return err;

        for (temporary[0..count], 0..) |maybe_metadata, relative_index| {
            const metadata = maybe_metadata orelse return error.InvalidGraphMetricSegment;
            columns[start + relative_index] = try pointScoresResultAlloc(
                alloc,
                session,
                graph_index_name,
                metric_names[start + relative_index],
                score_buffers[relative_index].?,
                metadata,
            );
            initialized_columns += 1;
            score_buffers[relative_index] = null;
        }
        start = end;
    }
    return .{ .columns = columns };
}

/// Resolves many exact node scores with one control/footer read and at most one
/// range fetch per touched score block. Output positions match `node_ids`.
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

/// Resolves scores directly into caller-owned storage. Parallel column reads
/// allocate their final buffers once on the request allocator while workers
/// use a thread-safe allocator only for transient I/O and decode state.
fn scoresInto(
    alloc: Allocator,
    session: *runtime_mod.QuerySession,
    graph_index_name: []const u8,
    metric_name: []const u8,
    node_ids: []const []const u8,
    values: []?f64,
) !PointScoresMetadata {
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
    defer alloc.free(control_bytes);
    const control = try metric_segment.decodeControl(control_bytes, config.edge_filter);
    try validateControl(control.header, graph_artifact, config);
    if (control.header.materialization_state == .rejected) {
        recordRejectionDiagnostic(session, graph_index_name, metric_name, control.header.materializer_fingerprint);
        return error.GraphMetricMaterializationRejected;
    }
    // Status/control is still authenticated for an empty result set, but no
    // score can be observed. Avoid fetching the routing footer (up to 16 MiB)
    // merely to return an empty column.
    if (node_ids.len == 0) {
        return .{
            .config_fingerprint = control.header.config_fingerprint,
            .converged = control.header.converged,
            .iterations_completed = control.header.iterations_completed,
            .delta = control.header.delta,
            .metadata_version = control.header.version,
            .published_generation = if (metric_artifact.published_generation != 0) metric_artifact.published_generation else session.manifest.version,
            .edge_generation = if (metric_artifact.edge_generation != 0) metric_artifact.edge_generation else session.manifest.version,
            .computed_at_ms = if (metric_artifact.computed_at_ms != 0) metric_artifact.computed_at_ms else @divTrunc(session.manifest.built_at_ns, std.time.ns_per_ms),
        };
    }

    const footer_len = try routingFooterLen(metric_artifact, control.header.version);
    const footer_offset = metric_artifact.byte_len - footer_len;
    const expected_blocks = @as(usize, control.score_count) / metric_segment.score_block_entries +
        @intFromBool(@as(usize, control.score_count) % metric_segment.score_block_entries != 0);
    var routing_lease = try acquireRouting(session, metric_index, metric_artifact, control.header.version, footer_offset, footer_len, expected_blocks, metric_segment.codec.routingRootLen(control.score_count), false);
    defer routing_lease.deinit();
    const routing = routing_lease.entry.routing;
    if (routing.entries.len != expected_blocks or routing.footer_offset != footer_offset or
        (routing.entries.len == 0 and routing.footer_offset != control.score_data_offset) or
        (routing.entries.len > 0 and routing.entries[0].offset != control.score_data_offset))
    {
        return error.InvalidGraphMetricSegment;
    }

    @memset(values, null);
    // Candidate sets are normally much smaller than the persisted vector.
    // Keep request planning proportional to requested nodes/touched blocks,
    // rather than allocating and clearing three arrays sized to every block in
    // the artifact for a one-node lookup.
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
    var pending_nodes = std.ArrayListUnmanaged(PendingNode).empty;
    defer pending_nodes.deinit(alloc);
    try pending_nodes.ensureTotalCapacity(alloc, node_ids.len);
    for (node_ids, 0..) |node_id, node_index| {
        const block_index = routing.findIndex(node_id) orelse continue;
        pending_nodes.appendAssumeCapacity(.{ .block_index = block_index, .node_index = node_index });
    }
    std.mem.sort(PendingNode, pending_nodes.items, {}, PendingNode.lessThan);
    var touched_blocks = std.ArrayListUnmanaged(TouchedBlock).empty;
    defer touched_blocks.deinit(alloc);
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

    const fetch_ranges = try planSparseScoreFetchRangesAlloc(alloc, routing.entries, touched_blocks.items, control.score_data_offset);
    defer alloc.free(fetch_ranges);
    var fetch_start: usize = 0;
    while (fetch_start < fetch_ranges.len) {
        const fetch_end = scoreRangeBatchEnd(fetch_ranges, fetch_start);
        const range_batch = fetch_ranges[fetch_start..fetch_end];
        var fetched_ranges = try fetchScoreRangeBatchAlloc(
            alloc,
            session,
            metric_index,
            control.header.version,
            routing.entries,
            range_batch,
        );
        defer fetched_ranges.deinit(alloc);

        for (range_batch, fetched_ranges.payloads) |range, payload| {
            try session.checkCancellation();
            var touched_index = std.sort.lowerBound(TouchedBlock, touched_blocks.items, range.first_block, struct {
                fn order(block_index: usize, touched: TouchedBlock) std.math.Order {
                    return std.math.order(block_index, touched.block_index);
                }
            }.order);
            while (touched_index < touched_blocks.items.len and touched_blocks.items[touched_index].block_index <= range.last_block) : (touched_index += 1) {
                const touched = touched_blocks.items[touched_index];
                const block_index = touched.block_index;
                const entry = routing.entries[block_index];
                if (entry.offset < range.offset) return error.InvalidGraphMetricSegment;
                const relative_offset = std.math.cast(usize, entry.offset - range.offset) orelse return error.InvalidGraphMetricSegment;
                const relative_end = std.math.add(usize, relative_offset, entry.len) catch return error.InvalidGraphMetricSegment;
                if (relative_end > payload.len) return error.InvalidGraphMetricSegment;
                const decoded_block = try metric_segment.decodeScoreBlockWithCancellation(payload[relative_offset..relative_end], session.cancellation);
                const expected_score_count = if (block_index + 1 < routing.entries.len)
                    metric_segment.score_block_entries
                else
                    @as(usize, control.score_count) - block_index * metric_segment.score_block_entries;
                try session.chargeGraphMetricDecode(1, expected_score_count);
                if (decoded_block.len != expected_score_count or
                    !decoded_block.scores[0].eqlNode(decoded_block.node_prefix, entry.first_node_id))
                {
                    return error.InvalidGraphMetricSegment;
                }
                for (pending_nodes.items[touched.first_pending..][0..touched.pending_count]) |pending| {
                    const node_index = pending.node_index;
                    values[node_index] = decoded_block.score(node_ids[node_index]);
                }
            }
        }
        fetch_start = fetch_end;
    }
    return .{
        .config_fingerprint = control.header.config_fingerprint,
        .converged = control.header.converged,
        .iterations_completed = control.header.iterations_completed,
        .delta = control.header.delta,
        .metadata_version = control.header.version,
        .published_generation = if (metric_artifact.published_generation != 0) metric_artifact.published_generation else session.manifest.version,
        .edge_generation = if (metric_artifact.edge_generation != 0) metric_artifact.edge_generation else session.manifest.version,
        .computed_at_ms = if (metric_artifact.computed_at_ms != 0) metric_artifact.computed_at_ms else @divTrunc(session.manifest.built_at_ns, std.time.ns_per_ms),
    };
}

fn planSparseScoreFetchRangesAlloc(
    alloc: Allocator,
    entries: []const metric_segment.codec.RoutingEntry,
    touched_blocks: anytype,
    score_data_offset: u64,
) ![]ScoreFetchRange {
    if (touched_blocks.len <= max_score_range_requests) {
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
    return try planScoreFetchRangesAlloc(alloc, entries, block_counts, score_data_offset);
}

fn planScoreFetchRangesAlloc(
    alloc: Allocator,
    entries: []const metric_segment.codec.RoutingEntry,
    block_counts: []const usize,
    score_data_offset: u64,
) ![]ScoreFetchRange {
    if (entries.len != block_counts.len) return error.InvalidGraphMetricSegment;
    var ranges = std.ArrayListUnmanaged(ScoreFetchRange).empty;
    errdefer ranges.deinit(alloc);
    var touched_blocks: usize = 0;
    for (block_counts) |count| touched_blocks += @intFromBool(count != 0);
    if (touched_blocks <= max_score_range_requests) {
        for (entries, block_counts, 0..) |entry, count, block_index| {
            if (count == 0) continue;
            try ranges.append(alloc, .{ .first_block = block_index, .last_block = block_index, .offset = entry.offset, .len = entry.len });
        }
        return try ranges.toOwnedSlice(alloc);
    }

    // Exact blocks are the preferred cache and transfer unit. When the request
    // count would exceed its per-column share, select only the fixed immutable
    // windows that buy the most requests for the least overfetch. This avoids a
    // count-triggered all-or-nothing jump to 8 MiB reads while preserving cache
    // identity across candidate sets.
    var windows = std.ArrayListUnmanaged(ScoreFetchWindow).empty;
    defer windows.deinit(alloc);
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
            if (next.offset != prior_end) return error.InvalidGraphMetricSegment;
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
            const last_end = std.math.add(u64, entries[last_block].offset, entries[last_block].len) catch
                return error.InvalidGraphMetricSegment;
            try windows.append(alloc, .{
                .first_block = first_block,
                .last_block = last_block,
                .offset = first.offset,
                .len = std.math.cast(usize, last_end - first.offset) orelse return error.GraphMetricQueryBudgetExceeded,
                .touched_blocks = window_touched,
                .touched_bytes = touched_bytes,
            });
        }
        first_block = last_block + 1;
    }

    var planned_requests = touched_blocks;
    while (planned_requests > max_score_range_requests) {
        var best: ?usize = null;
        for (windows.items, 0..) |window, index| {
            if (window.selected or window.savedRequests() == 0) continue;
            if (best) |best_index| {
                const current = windows.items[best_index];
                const left = @as(u128, window.overfetchBytes()) * current.savedRequests();
                const right = @as(u128, current.overfetchBytes()) * window.savedRequests();
                if (left > right or (left == right and window.offset >= current.offset)) continue;
            }
            best = index;
        }
        const selected = best orelse return error.GraphMetricQueryBudgetExceeded;
        windows.items[selected].selected = true;
        planned_requests -= windows.items[selected].savedRequests();
    }

    for (windows.items) |window| {
        if (window.selected) {
            try ranges.append(alloc, .{
                .first_block = window.first_block,
                .last_block = window.last_block,
                .offset = window.offset,
                .len = window.len,
            });
            continue;
        }
        for (entries[window.first_block .. window.last_block + 1], block_counts[window.first_block .. window.last_block + 1], window.first_block..) |entry, count, block_index| {
            if (count == 0) continue;
            try ranges.append(alloc, .{
                .first_block = block_index,
                .last_block = block_index,
                .offset = entry.offset,
                .len = entry.len,
            });
        }
    }
    if (ranges.items.len > max_score_range_requests) return error.GraphMetricQueryBudgetExceeded;
    return try ranges.toOwnedSlice(alloc);
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
    var dimensions: [26]u8 = undefined;
    std.mem.writeInt(u64, dimensions[0..8], artifact.byte_len, .little);
    std.mem.writeInt(u64, dimensions[8..16], offset, .little);
    std.mem.writeInt(u64, dimensions[16..24], len, .little);
    std.mem.writeInt(u16, dimensions[24..26], version, .little);
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
    const footer = try fetchRoutingFooterAlloc(session, metric_index, artifact, version, offset, len, root_len);
    var owns_footer = true;
    defer if (owns_footer) session.alloc.free(footer);
    const owner = if (session.cache) |cache| cache.alloc else session.alloc;
    const owned = if (owner.ptr == session.alloc.ptr and owner.vtable == session.alloc.vtable) blk: {
        owns_footer = false;
        break :blk footer;
    } else try owner.dupe(u8, footer);
    errdefer owner.free(owned);
    var routing = if (ranked_only)
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

fn fetchScoreRangeAlloc(
    alloc: Allocator,
    session: *runtime_mod.QuerySession,
    metric_index: usize,
    segment_version: u16,
    entries: []const metric_segment.codec.RoutingEntry,
    range: ScoreFetchRange,
) ![]u8 {
    try session.chargeGraphMetricRange(range.len);
    if (segment_version != metric_segment.wire_version) return error.InvalidGraphMetricSegment;
    if (range.first_block > range.last_block or range.last_block >= entries.len) return error.InvalidGraphMetricSegment;
    if (range.first_block == range.last_block) {
        const entry = entries[range.first_block];
        if (entry.offset != range.offset or entry.len != range.len) return error.InvalidGraphMetricSegment;
        var block_id_buf: [64]u8 = undefined;
        const block_id = std.fmt.bufPrint(&block_id_buf, "graph-metric-score-{d}-exact", .{range.first_block}) catch
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
    return session.fetchArtifactAuthenticatedRangeAlloc(metric_index, range.offset, range.len, subranges) catch |err| switch (err) {
        error.ArtifactIntegrityMismatch => error.InvalidGraphMetricSegment,
        else => |other| other,
    };
}

const FetchedScoreRanges = struct {
    payloads: [][]u8,

    fn deinit(self: *@This(), alloc: Allocator) void {
        for (self.payloads) |payload| if (payload.len > 0) std.heap.smp_allocator.free(payload);
        alloc.free(self.payloads);
    }
};

fn fetchScoreRangeWorker(
    child: *runtime_mod.QuerySession,
    metric_index: usize,
    segment_version: u16,
    entries: []const metric_segment.codec.RoutingEntry,
    range: ScoreFetchRange,
    output: *[]u8,
    failure: *?anyerror,
) void {
    output.* = fetchScoreRangeAlloc(
        std.heap.smp_allocator,
        child,
        metric_index,
        segment_version,
        entries,
        range,
    ) catch |err| {
        failure.* = err;
        return;
    };
}

/// Fetch independent immutable score ranges concurrently with bounded fanout.
/// Every child borrows the pinned manifest and charges the same synchronized
/// request budget, so parallelism changes latency without weakening admission.
fn fetchScoreRangeBatchAlloc(
    alloc: Allocator,
    session: *runtime_mod.QuerySession,
    metric_index: usize,
    segment_version: u16,
    entries: []const metric_segment.codec.RoutingEntry,
    ranges: []const ScoreFetchRange,
) !FetchedScoreRanges {
    if (ranges.len > max_parallel_score_range_requests)
        return error.GraphMetricQueryBudgetExceeded;
    var requested_bytes: usize = 0;
    for (ranges) |range| {
        requested_bytes = std.math.add(usize, requested_bytes, range.len) catch
            return error.GraphMetricQueryBudgetExceeded;
    }
    if (requested_bytes > max_parallel_score_range_bytes)
        return error.GraphMetricQueryBudgetExceeded;
    const payloads = try alloc.alloc([]u8, ranges.len);
    @memset(payloads, @constCast((&[_]u8{})[0..]));
    errdefer {
        for (payloads) |payload| if (payload.len > 0) std.heap.smp_allocator.free(payload);
        alloc.free(payloads);
    }

    var children: [max_parallel_score_range_requests]runtime_mod.QuerySession = undefined;
    var failures: [max_parallel_score_range_requests]?anyerror = @splat(null);
    if (session.io) |io| {
        var group: std.Io.Group = .init;
        for (ranges, 0..) |range, index| {
            children[index] = session.forkGraphMetricRead(std.heap.smp_allocator);
            group.async(io, fetchScoreRangeWorker, .{
                &children[index], metric_index, segment_version, entries, range, &payloads[index], &failures[index],
            });
        }
        const await_result = group.await(io);
        for (children[0..ranges.len]) |*child| child.deinit();
        try await_result;
    } else {
        for (ranges, 0..) |range, index| {
            children[index] = session.forkGraphMetricRead(std.heap.smp_allocator);
            fetchScoreRangeWorker(
                &children[index],
                metric_index,
                segment_version,
                entries,
                range,
                &payloads[index],
                &failures[index],
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
    defer alloc.free(control_bytes);
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
    var routing_lease = try acquireRouting(session, metric_index, metric_artifact, control.header.version, metric_artifact.byte_len - root_len, root_len, expected_ranked_blocks, root_len, true);
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
            defer alloc.free(ranked_payload);
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
    defer alloc.free(payload);
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

test "serverless graph metric range planning caps broad point batches" {
    const alloc = std.testing.allocator;
    var entries: [128]metric_segment.codec.RoutingEntry = undefined;
    var counts: [128]usize = @splat(1);
    for (&entries, 0..) |*entry, index| entry.* = .{
        .first_node_id = "node",
        .offset = index * 64 * 1024,
        .len = 64 * 1024,
    };
    const broad = try planScoreFetchRangesAlloc(alloc, &entries, &counts, 0);
    defer alloc.free(broad);
    try std.testing.expectEqual(@as(usize, 1), broad.len);
    try std.testing.expectEqual(@as(usize, 128), broad[0].last_block - broad[0].first_block + 1);
    try std.testing.expectEqual(coalesced_score_window_bytes, broad[0].len);

    // The old threshold was 32 and caused the 33rd touched block to jump from
    // exact reads to full windows. Moderate point batches now stay exact.
    @memset(&counts, 0);
    @memset(counts[0..33], 1);
    const moderate = try planScoreFetchRangesAlloc(alloc, &entries, &counts, 0);
    defer alloc.free(moderate);
    try std.testing.expectEqual(@as(usize, 33), moderate.len);
    for (moderate) |range| try std.testing.expectEqual(range.first_block, range.last_block);

    @memset(&counts, 0);
    counts[0] = 1;
    counts[counts.len - 1] = 1;
    const sparse = try planScoreFetchRangesAlloc(alloc, &entries, &counts, 0);
    defer alloc.free(sparse);
    try std.testing.expectEqual(@as(usize, 2), sparse.len);
    try std.testing.expectEqual(@as(usize, 64 * 1024), sparse[0].len);
    try std.testing.expectEqual(@as(usize, 64 * 1024), sparse[1].len);

    var unused_session: runtime_mod.QuerySession = undefined;
    const oversized_batch: [max_parallel_score_range_requests + 1]ScoreFetchRange = @splat(.{
        .first_block = 0,
        .last_block = 0,
        .offset = 0,
        .len = 1,
    });
    try std.testing.expectError(
        error.GraphMetricQueryBudgetExceeded,
        fetchScoreRangeBatchAlloc(alloc, &unused_session, 0, 0, &.{}, &oversized_batch),
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
    const ranges = try planSparseScoreFetchRangesAlloc(alloc, &entries, &touched, 0);
    defer alloc.free(ranges);
    try std.testing.expectEqual(@as(usize, 1), ranges.len);
    try std.testing.expectEqual(@as(usize, 3072), ranges[0].first_block);
    try std.testing.expectEqual(@as(usize, 64), ranges[0].len);
}

test "serverless graph metric broad point ranges have candidate-independent cache identity" {
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
    const first = try planScoreFetchRangesAlloc(alloc, &entries, &first_counts, 0);
    defer alloc.free(first);
    const second = try planScoreFetchRangesAlloc(alloc, &entries, &second_counts, 0);
    defer alloc.free(second);
    try std.testing.expectEqual(first.len, second.len);
    for (first, second) |left, right| {
        try std.testing.expectEqual(left.first_block, right.first_block);
        try std.testing.expectEqual(left.last_block, right.last_block);
        try std.testing.expectEqual(left.offset, right.offset);
        try std.testing.expectEqual(left.len, right.len);
    }
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
    var session = runtime_mod.QuerySession{
        .alloc = alloc,
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
        .scores = try alloc.alloc(metric_segment.Score, metric_segment.score_block_entries + 1),
    };
    for (metric.scores, 0..) |*score, index| {
        score.* = .{
            .node_id = try std.fmt.allocPrint(alloc, "node:{d:0>4}", .{index}),
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
        verify_calls: std.atomic.Value(usize) = .init(0),
        corrupt_score_reads: bool = false,
        reject_point_index_reads: bool = false,
        corrupt_point_index: bool = false,
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
            if (self.reject_point_index_reads and touches_point_index) return error.UnexpectedPointIndexRead;
            const out = try result_alloc.dupe(u8, self.payload[start..][0..len]);
            if (self.corrupt_point_index and touches_point_index) out[self.footer_offset - start] ^= 1;
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
    var session = runtime_mod.QuerySession{
        .alloc = alloc,
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
    const node_ids = [_][]const u8{"node:1024"};

    var empty_points = try scoresAlloc(alloc, &session, "graph_idx", "rank", &.{});
    defer empty_points.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), empty_points.scores.len);
    // Only the authenticated control/status probe is required; routing and
    // score data remain untouched.
    try std.testing.expectEqual(@as(usize, 1), state.range_calls.load(.monotonic));

    state.range_calls.store(0, .monotonic);
    var result = try scoresAlloc(alloc, &session, "graph_idx", "rank", &node_ids);
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(?f64, 1024), result.scores[0]);
    try std.testing.expectEqual(@as(usize, 0), state.verify_calls.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 3), state.range_calls.load(.monotonic));

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    session.setIo(io_impl.io());
    const column_names = [_][]const u8{ "rank", "rank", "rank" };
    var columns = try scoreColumnsAlloc(alloc, &session, "graph_idx", &column_names, &node_ids);
    defer columns.deinit(alloc);
    try std.testing.expectEqual(@as(usize, column_names.len), columns.columns.len);
    for (columns.columns) |column| try std.testing.expectEqual(@as(?f64, 1024), column.scores[0]);

    const AllocationRunner = struct {
        fn run(failing_alloc: Allocator, active_session: *runtime_mod.QuerySession) !void {
            // Each injected run is one logical request. Workers share this
            // budget, but never the intentionally non-thread-safe allocator.
            active_session.graph_metric_read_budget = .{};
            const names = [_][]const u8{ "rank", "rank", "rank" };
            const ids = [_][]const u8{"node:1024"};
            var loaded = try scoreColumnsAlloc(failing_alloc, active_session, "graph_idx", &names, &ids);
            defer loaded.deinit(failing_alloc);
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, AllocationRunner.run, .{&session});

    state.range_calls.store(0, .monotonic);
    state.reject_point_index_reads = true;
    var top_one = try topWithLimitsAlloc(alloc, &session, "graph_idx", "rank", 1, .{});
    defer top_one.deinit(alloc);
    try std.testing.expectEqualStrings("node:1024", top_one.scores[0].node_id);
    try std.testing.expectEqual(@as(usize, 3), state.range_calls.load(.monotonic));
    state.range_calls.store(0, .monotonic);
    var top = try topWithLimitsAlloc(alloc, &session, "graph_idx", "rank", metric_segment.score_block_entries + 1, .{});
    defer top.deinit(alloc);
    try std.testing.expectEqual(@as(usize, metric_segment.score_block_entries + 1), top.scores.len);
    try std.testing.expectEqualStrings("node:1024", top.scores[0].node_id);
    try std.testing.expectEqual(@as(f64, 1024), top.scores[0].value);
    try std.testing.expectEqualStrings("node:0000", top.scores[top.scores.len - 1].node_id);
    try std.testing.expectEqual(@as(usize, 3), state.range_calls.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 0), state.verify_calls.load(.monotonic));

    state.reject_point_index_reads = false;
    state.corrupt_point_index = true;
    try std.testing.expectError(error.InvalidGraphMetricSegment, scoresAlloc(alloc, &session, "graph_idx", "rank", &node_ids));
    state.corrupt_point_index = false;
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
    // A warm lookup needs one score-block decode, not a routing-index decode.
    session.graph_metric_read_budget = .{ .limits = .{ .max_decoded_blocks = 1 } };
    var second_cached = try scoresAlloc(alloc, &session, "graph_idx", "rank", &node_ids);
    defer second_cached.deinit(alloc);
    try std.testing.expectEqual(@as(?f64, 1024), second_cached.scores[0]);
    try std.testing.expectEqual(@as(u64, 1), cache.graph_metric_routing.hits);
    try std.testing.expectEqual(@as(u64, 1), session.graph_metric_read_budget.decoded_blocks);
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
