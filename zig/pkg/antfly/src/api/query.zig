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
const db_mod = @import("../storage/db/mod.zig");
const graph_mod = @import("../graph/graph.zig");
const graph_paths = @import("../graph/paths.zig");
const graph_query_mod = @import("../graph/query.zig");
const query_contract = @import("query_contract.zig");

pub const QueryResponse = query_contract.QueryResponse;
pub const QueryResponseMeta = query_contract.QueryResponseMeta;
pub const OwnedQueryRequest = query_contract.OwnedQueryRequest;

pub const parseQueryRequest = query_contract.parseQueryRequest;
pub const parsePublicQueryRequest = query_contract.parsePublicQueryRequest;
pub const parseAggregationRequestsJson = query_contract.parseAggregationRequestsJson;
pub const freeAggregationRequests = query_contract.freeAggregationRequests;
pub const encodeQueryResponses = query_contract.encodeQueryResponses;

const FakeSemanticResolver = struct {
    fn iface() query_contract.SemanticResolver {
        return .{
            .ptr = undefined,
            .vtable = &.{
                .resolve_dense_query = resolveDenseQuery,
            },
        };
    }

    fn resolveDenseQuery(
        _: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
        semantic_search: []const u8,
        embedding_template: ?[]const u8,
        limit: u32,
    ) !db_mod.types.DenseKnnQuery {
        try std.testing.expectEqualStrings("docs", table_name);
        try std.testing.expectEqualStrings("semantic_idx", index_name);
        try std.testing.expectEqualStrings("alpha concept", semantic_search);
        try std.testing.expect(embedding_template == null or std.mem.eql(u8, embedding_template.?, "{{remotePDF url=this}}"));
        const vector = try alloc.alloc(f32, 3);
        vector[0] = 0.25;
        vector[1] = 0.5;
        vector[2] = 0.75;
        return .{
            .vector = vector,
            .k = limit,
        };
    }
};

fn jsonStringifyAlloc(alloc: std.mem.Allocator, value: anytype) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(alloc);
    defer out.deinit();
    try std.json.Stringify.value(value, .{}, &out.writer);
    return try alloc.dupe(u8, out.written());
}

pub fn mergeSearchResults(
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
    results: []const db_mod.types.SearchResult,
    offset: u32,
    limit: u32,
) !db_mod.types.SearchResult {
    var total_hits: u32 = 0;
    var has_graph_results = false;
    var has_graph_metric_results = false;
    var has_graph_metric_rerank_status = false;
    for (results) |result| {
        if (result.graph_results.len > 0) has_graph_results = true;
        if (result.graph_metric_results.len > 0) has_graph_metric_results = true;
        if (result.graph_metric_rerank_status != null) has_graph_metric_rerank_status = true;
        total_hits +|= result.total_hits;
    }

    var graph_metric_rerank_status = if (req.graph_metric_rerank != null)
        try mergeGraphMetricRerankStatus(alloc, req, results)
    else if (has_graph_metric_rerank_status)
        try mergeGraphMetricRerankStatus(alloc, req, results)
    else
        null;
    errdefer if (graph_metric_rerank_status) |*status| status.deinit(alloc);

    var merged_hits = std.ArrayListUnmanaged(db_mod.types.SearchHit).empty;
    defer {
        for (merged_hits.items) |*hit| hit.deinit(alloc);
        merged_hits.deinit(alloc);
    }

    for (results) |result| {
        for (result.hits) |hit| {
            try merged_hits.append(alloc, try hit.clone(alloc));
        }
    }

    const lower_scores_are_better = isPureDenseRequest(req);
    std.sort.pdq(db_mod.types.SearchHit, merged_hits.items, lower_scores_are_better, struct {
        fn lessThan(ascending_scores: bool, a: db_mod.types.SearchHit, b: db_mod.types.SearchHit) bool {
            const a_score = a.score orelse 0;
            const b_score = b.score orelse 0;
            if (a_score != b_score) {
                return if (ascending_scores) a_score < b_score else a_score > b_score;
            }
            return std.mem.order(u8, a.id, b.id) == .lt;
        }
    }.lessThan);

    const start: usize = @min(offset, merged_hits.items.len);
    const max_count: usize = if (limit == 0) merged_hits.items.len - start else @min(limit, merged_hits.items.len - start);
    const end = start + max_count;

    var final_hits = try alloc.alloc(db_mod.types.SearchHit, max_count);
    var moved: usize = 0;
    errdefer {
        for (final_hits[0..moved]) |*hit| hit.deinit(alloc);
        alloc.free(final_hits);
    }

    for (merged_hits.items[start..end], 0..) |hit, i| {
        final_hits[i] = try hit.clone(alloc);
        moved += 1;
    }

    const graph_results = if (has_graph_results)
        try mergeGraphSearchResults(alloc, results)
    else
        @constCast((&[_]db_mod.types.GraphSearchResult{})[0..]);
    errdefer {
        for (graph_results) |*graph_result| graph_result.deinit(alloc);
        if (graph_results.len > 0) alloc.free(graph_results);
    }
    const graph_metric_results = if (has_graph_metric_results)
        try mergeGraphMetricResults(alloc, req, results)
    else
        @constCast((&[_]db_mod.types.GraphMetricResult{})[0..]);
    errdefer {
        for (graph_metric_results) |*metric_result| metric_result.deinit(alloc);
        if (graph_metric_results.len > 0) alloc.free(graph_metric_results);
    }

    if (results.len > 1) {
        clearMergedDocOrdinals(final_hits);
        for (graph_results) |*graph_result| clearMergedDocOrdinals(graph_result.hits);
    }

    return .{
        .alloc = alloc,
        .hits = final_hits,
        .total_hits = total_hits,
        .identity_read_generation = mergedSearchResultIdentityReadGeneration(req, results),
        .graph_results = graph_results,
        .graph_metric_results = graph_metric_results,
        .graph_metric_rerank_status = graph_metric_rerank_status,
    };
}

fn mergedSearchResultIdentityReadGeneration(
    req: db_mod.types.SearchRequest,
    results: []const db_mod.types.SearchResult,
) ?u64 {
    if (req.identity_read_generation) |generation| return generation;
    var common: ?u64 = null;
    for (results) |result| {
        const generation = result.identity_read_generation orelse return null;
        if (common) |existing| {
            if (existing != generation) return null;
        } else {
            common = generation;
        }
    }
    return common;
}

fn clearMergedDocOrdinals(hits: []db_mod.types.SearchHit) void {
    for (hits) |*hit| hit.doc_ordinal = null;
}

fn isPureDenseRequest(req: db_mod.types.SearchRequest) bool {
    return req.dense_queries.len > 0 and
        req.full_text_queries.len == 0 and
        req.full_text == null and
        req.sparse_queries.len == 0 and
        req.graph_queries.len == 0 and
        req.graph_metric_queries.len == 0 and
        req.graph_metric_rerank == null and
        req.sparse == null and
        req.merge_config == null;
}

const GraphSearchResultBuilder = struct {
    name: []u8,
    nodes: std.ArrayListUnmanaged(graph_query_mod.GraphResultNode) = .empty,
    paths: std.ArrayListUnmanaged(db_mod.types.GraphPath) = .empty,
    matches: std.ArrayListUnmanaged(db_mod.types.GraphPatternMatch) = .empty,
    hits: std.ArrayListUnmanaged(db_mod.types.SearchHit) = .empty,
    metric_status: std.ArrayListUnmanaged(db_mod.types.GraphMetricStatus) = .empty,
    total_hits: u32 = 0,

    fn deinit(self: *GraphSearchResultBuilder, alloc: std.mem.Allocator) void {
        if (self.name.len > 0) alloc.free(self.name);
        for (self.nodes.items) |*node| node.deinit(alloc);
        self.nodes.deinit(alloc);
        for (self.paths.items) |path| graph_paths.freePath(alloc, path);
        self.paths.deinit(alloc);
        for (self.matches.items) |*match| match.deinit(alloc);
        self.matches.deinit(alloc);
        for (self.hits.items) |*hit| hit.deinit(alloc);
        self.hits.deinit(alloc);
        for (self.metric_status.items) |*status| status.deinit(alloc);
        self.metric_status.deinit(alloc);
        self.* = undefined;
    }

    fn toOwned(self: *GraphSearchResultBuilder, alloc: std.mem.Allocator) !db_mod.types.GraphSearchResult {
        return .{
            .name = self.name,
            .nodes = try self.nodes.toOwnedSlice(alloc),
            .paths = try self.paths.toOwnedSlice(alloc),
            .matches = try self.matches.toOwnedSlice(alloc),
            .hits = try self.hits.toOwnedSlice(alloc),
            .total_hits = self.total_hits,
            .metric_status = try self.metric_status.toOwnedSlice(alloc),
        };
    }
};

const GraphMetricResultBuilder = struct {
    name: []u8,
    index_name: []u8,
    metric_name: []u8,
    scores: std.ArrayListUnmanaged(db_mod.types.GraphMetricScore) = .empty,
    status: ?db_mod.types.GraphMetricStatus = null,

    fn deinit(self: *GraphMetricResultBuilder, alloc: std.mem.Allocator) void {
        if (self.name.len > 0) alloc.free(self.name);
        if (self.index_name.len > 0) alloc.free(self.index_name);
        if (self.metric_name.len > 0) alloc.free(self.metric_name);
        for (self.scores.items) |*score| score.deinit(alloc);
        self.scores.deinit(alloc);
        if (self.status) |*status| status.deinit(alloc);
        self.* = undefined;
    }

    fn toOwned(self: *GraphMetricResultBuilder, alloc: std.mem.Allocator, top_k: u32) !db_mod.types.GraphMetricResult {
        std.sort.pdq(db_mod.types.GraphMetricScore, self.scores.items, {}, struct {
            fn lessThan(_: void, a: db_mod.types.GraphMetricScore, b: db_mod.types.GraphMetricScore) bool {
                if (a.score != b.score) return a.score > b.score;
                return std.mem.order(u8, a.node, b.node) == .lt;
            }
        }.lessThan);

        const count = if (top_k == 0)
            self.scores.items.len
        else
            @min(self.scores.items.len, @as(usize, @intCast(top_k)));
        const scores = try alloc.alloc(db_mod.types.GraphMetricScore, count);
        var initialized: usize = 0;
        errdefer {
            for (scores[0..initialized]) |*score| score.deinit(alloc);
            alloc.free(scores);
        }
        for (self.scores.items[0..count], 0..) |score, i| {
            scores[i] = .{
                .node = try alloc.dupe(u8, score.node),
                .score = score.score,
            };
            initialized += 1;
        }

        return .{
            .name = self.name,
            .index_name = self.index_name,
            .metric_name = self.metric_name,
            .scores = scores,
            .status = self.status orelse return error.InvalidQueryResponse,
        };
    }
};

fn mergeGraphMetricResults(
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
    results: []const db_mod.types.SearchResult,
) ![]db_mod.types.GraphMetricResult {
    var builders = std.ArrayListUnmanaged(GraphMetricResultBuilder).empty;
    defer {
        for (builders.items) |*builder| builder.deinit(alloc);
        builders.deinit(alloc);
    }

    for (results) |result| {
        for (result.graph_metric_results) |metric_result| {
            const idx = blk: {
                for (builders.items, 0..) |builder, i| {
                    if (std.mem.eql(u8, builder.name, metric_result.name)) break :blk i;
                }
                try builders.append(alloc, .{
                    .name = try alloc.dupe(u8, metric_result.name),
                    .index_name = try alloc.dupe(u8, metric_result.index_name),
                    .metric_name = try alloc.dupe(u8, metric_result.metric_name),
                });
                break :blk builders.items.len - 1;
            };
            var builder = &builders.items[idx];
            try ensureGraphMetricResultComparable(builder, metric_result);
            for (metric_result.scores) |score| {
                try builder.scores.append(alloc, .{
                    .node = try alloc.dupe(u8, score.node),
                    .score = score.score,
                });
            }
            if (builder.status) |*status| {
                try mergeGraphMetricStatusInto(alloc, status, metric_result.status);
            } else {
                builder.status = try cloneGraphMetricStatus(alloc, metric_result.status);
            }
        }
    }

    const merged = try alloc.alloc(db_mod.types.GraphMetricResult, builders.items.len);
    var initialized: usize = 0;
    errdefer {
        for (merged[0..initialized]) |*metric_result| metric_result.deinit(alloc);
        alloc.free(merged);
    }
    for (builders.items, 0..) |*builder, i| {
        merged[i] = try builder.toOwned(alloc, graphMetricQueryTopK(req, builder.name));
        initialized += 1;
        builder.name = &.{};
        builder.index_name = &.{};
        builder.metric_name = &.{};
        builder.status = null;
    }
    return merged;
}

fn ensureGraphMetricResultComparable(
    builder: *const GraphMetricResultBuilder,
    metric_result: db_mod.types.GraphMetricResult,
) !void {
    if (!std.mem.eql(u8, builder.index_name, metric_result.index_name)) return error.UnsupportedQueryRequest;
    if (!std.mem.eql(u8, builder.metric_name, metric_result.metric_name)) return error.UnsupportedQueryRequest;
    const existing = builder.status orelse return;
    if (existing.published_generation != 0 and
        metric_result.status.published_generation != 0 and
        existing.published_generation != metric_result.status.published_generation)
    {
        return error.UnsupportedQueryRequest;
    }
    if ((existing.published_generation == 0 and builder.scores.items.len > 0) or
        (metric_result.status.published_generation == 0 and metric_result.scores.len > 0))
    {
        return error.UnsupportedQueryRequest;
    }
}

fn graphMetricQueryTopK(req: db_mod.types.SearchRequest, query_name: []const u8) u32 {
    for (req.graph_metric_queries) |query| {
        if (std.mem.eql(u8, query.name, query_name)) return query.query.top_k;
    }
    return 0;
}

fn mergeGraphMetricRerankStatus(
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
    results: []const db_mod.types.SearchResult,
) !?db_mod.types.GraphMetricStatus {
    const rerank = req.graph_metric_rerank orelse {
        for (results) |result| {
            if (result.graph_metric_rerank_status != null) return error.UnsupportedQueryRequest;
        }
        return null;
    };

    var merged: ?db_mod.types.GraphMetricStatus = null;
    errdefer if (merged) |*status| status.deinit(alloc);

    for (results) |result| {
        const status = result.graph_metric_rerank_status orelse return error.UnsupportedQueryRequest;
        if (!std.mem.eql(u8, status.name, rerank.metric_name)) return error.UnsupportedQueryRequest;
        if (status.published_generation == 0) return error.UnsupportedQueryRequest;
        if (merged) |*existing| {
            if (existing.published_generation != status.published_generation) return error.UnsupportedQueryRequest;
            try mergeGraphMetricStatusInto(alloc, existing, status);
        } else {
            merged = try cloneGraphMetricStatus(alloc, status);
        }
    }

    return merged;
}

fn mergeGraphMetricStatusInto(alloc: std.mem.Allocator, target: *db_mod.types.GraphMetricStatus, source: db_mod.types.GraphMetricStatus) !void {
    target.state = mergeGraphMetricState(target.state, source.state);
    target.phase = mergeGraphMetricPhase(target.phase, source.phase);
    target.metadata_version = mergeGraphMetricMetadataVersion(target.metadata_version, source.metadata_version);
    target.maintenance_paused = target.maintenance_paused or source.maintenance_paused;
    target.build_queued = target.build_queued or source.build_queued;
    target.published_generation = mergeComparableGeneration(target.published_generation, source.published_generation);
    target.edge_generation = @max(target.edge_generation, source.edge_generation);
    target.target_edge_generation = @max(target.target_edge_generation, source.target_edge_generation);
    target.queued_generation = @max(target.queued_generation, source.queued_generation);
    target.building_generation = @max(target.building_generation, source.building_generation);
    target.build_job_id = if (target.build_job_id == 0) source.build_job_id else if (source.build_job_id == 0 or source.build_job_id == target.build_job_id) target.build_job_id else 0;
    target.build_started_at_ms = if (target.build_started_at_ms == 0) source.build_started_at_ms else if (source.build_started_at_ms == 0 or source.build_started_at_ms == target.build_started_at_ms) target.build_started_at_ms else @min(target.build_started_at_ms, source.build_started_at_ms);
    target.build_iteration = @max(target.build_iteration, source.build_iteration);
    target.build_lease_expires_at_ms = @max(target.build_lease_expires_at_ms, source.build_lease_expires_at_ms);
    if (target.build_worker_id.len == 0) {
        target.build_worker_id = if (source.build_worker_id.len > 0) try alloc.dupe(u8, source.build_worker_id) else "";
    } else if (source.build_worker_id.len > 0 and !std.mem.eql(u8, target.build_worker_id, source.build_worker_id)) {
        alloc.free(target.build_worker_id);
        target.build_worker_id = try alloc.dupe(u8, "multiple");
    }
    target.retry_count = @max(target.retry_count, source.retry_count);
    if (target.last_error.len == 0 and source.last_error.len > 0) {
        target.last_error = try alloc.dupe(u8, source.last_error);
    }
    target.progress = @min(target.progress, source.progress);
    target.converged = target.converged and source.converged;
    target.iterations_completed = @max(target.iterations_completed, source.iterations_completed);
    target.delta = @max(target.delta, source.delta);
    target.computed_at_ms = @max(target.computed_at_ms, source.computed_at_ms);
}

fn mergeGraphMetricMetadataVersion(left: u32, right: u32) u32 {
    if (left == 0) return right;
    if (right == 0) return left;
    if (left == right) return left;
    return 0;
}

fn mergeComparableGeneration(left: u64, right: u64) u64 {
    if (left == 0) return right;
    if (right == 0) return left;
    if (left == right) return left;
    return @min(left, right);
}

fn mergeGraphMetricState(left: graph_mod.GraphIndex.GraphMetricState, right: graph_mod.GraphIndex.GraphMetricState) graph_mod.GraphIndex.GraphMetricState {
    return if (graphMetricStateSeverity(right) > graphMetricStateSeverity(left)) right else left;
}

fn graphMetricStateSeverity(state: graph_mod.GraphIndex.GraphMetricState) u8 {
    return switch (state) {
        .disabled => 5,
        .failed => 4,
        .building => 3,
        .not_ready => 2,
        .stale => 1,
        .fresh => 0,
    };
}

fn mergeGraphMetricPhase(left: graph_mod.GraphIndex.GraphMetricBuildPhase, right: graph_mod.GraphIndex.GraphMetricBuildPhase) graph_mod.GraphIndex.GraphMetricBuildPhase {
    return if (graphMetricPhaseSeverity(right) > graphMetricPhaseSeverity(left)) right else left;
}

fn graphMetricPhaseSeverity(phase: graph_mod.GraphIndex.GraphMetricBuildPhase) u8 {
    return switch (phase) {
        .cleanup_old_generations => 10,
        .publish_generation, .publishing => 9,
        .check_convergence => 8,
        .reduce_ranks => 7,
        .iterate_contributions, .computing => 6,
        .initialize_ranks => 5,
        .scan_edges_and_out_degree => 4,
        .prepare_generation => 3,
        .idle => 1,
        .complete => 0,
    };
}

fn mergeGraphSearchResults(
    alloc: std.mem.Allocator,
    results: []const db_mod.types.SearchResult,
) ![]db_mod.types.GraphSearchResult {
    var builders = std.ArrayListUnmanaged(GraphSearchResultBuilder).empty;
    defer {
        for (builders.items) |*builder| builder.deinit(alloc);
        builders.deinit(alloc);
    }

    for (results) |result| {
        for (result.graph_results) |graph_result| {
            const idx = blk: {
                for (builders.items, 0..) |builder, i| {
                    if (std.mem.eql(u8, builder.name, graph_result.name)) break :blk i;
                }
                try builders.append(alloc, .{
                    .name = try alloc.dupe(u8, graph_result.name),
                });
                break :blk builders.items.len - 1;
            };
            var builder = &builders.items[idx];
            builder.total_hits +|= graph_result.total_hits;

            for (graph_result.nodes) |node| {
                try builder.nodes.append(alloc, try cloneGraphResultNode(alloc, node));
            }
            for (graph_result.paths) |path| {
                try builder.paths.append(alloc, try cloneGraphPath(alloc, path));
            }
            for (graph_result.matches) |match| {
                try builder.matches.append(alloc, try cloneGraphPatternMatch(alloc, match));
            }
            for (graph_result.hits) |hit| {
                try builder.hits.append(alloc, try hit.clone(alloc));
            }
            for (graph_result.metric_status) |status| {
                try builder.metric_status.append(alloc, try cloneGraphMetricStatus(alloc, status));
            }
        }
    }

    const merged = try alloc.alloc(db_mod.types.GraphSearchResult, builders.items.len);
    var initialized: usize = 0;
    errdefer {
        for (merged[0..initialized]) |*graph_result| graph_result.deinit(alloc);
        alloc.free(merged);
    }
    for (builders.items, 0..) |*builder, i| {
        merged[i] = try builder.toOwned(alloc);
        initialized += 1;
        builder.name = &.{};
        builder.nodes = .empty;
        builder.paths = .empty;
        builder.matches = .empty;
        builder.hits = .empty;
        builder.metric_status = .empty;
    }
    return merged;
}

fn cloneGraphSearchResult(
    alloc: std.mem.Allocator,
    source: db_mod.types.GraphSearchResult,
) !db_mod.types.GraphSearchResult {
    const GraphNode = std.meta.Child(@TypeOf(source.nodes));
    const nodes = try alloc.alloc(GraphNode, source.nodes.len);
    var initialized_nodes: usize = 0;
    errdefer {
        for (nodes[0..initialized_nodes]) |*node| node.deinit(alloc);
        if (source.nodes.len > 0) alloc.free(nodes);
    }
    for (source.nodes, 0..) |node, i| {
        nodes[i] = try cloneGraphResultNode(alloc, node);
        initialized_nodes += 1;
    }

    const GraphPath = std.meta.Child(@TypeOf(source.paths));
    const paths = try alloc.alloc(GraphPath, source.paths.len);
    var initialized_paths: usize = 0;
    errdefer {
        for (paths[0..initialized_paths]) |path| graph_paths.freePath(alloc, path);
        if (source.paths.len > 0) alloc.free(paths);
    }
    for (source.paths, 0..) |path, i| {
        paths[i] = try cloneGraphPath(alloc, path);
        initialized_paths += 1;
    }

    const hits = try alloc.alloc(db_mod.types.SearchHit, source.hits.len);
    var initialized_hits: usize = 0;
    errdefer {
        for (hits[0..initialized_hits]) |*hit| hit.deinit(alloc);
        if (source.hits.len > 0) alloc.free(hits);
    }
    for (source.hits, 0..) |hit, i| {
        hits[i] = try hit.clone(alloc);
        initialized_hits += 1;
    }

    const matches = try alloc.alloc(db_mod.types.GraphPatternMatch, source.matches.len);
    var initialized_matches: usize = 0;
    errdefer {
        for (matches[0..initialized_matches]) |*match| match.deinit(alloc);
        if (source.matches.len > 0) alloc.free(matches);
    }
    for (source.matches, 0..) |match, i| {
        matches[i] = try cloneGraphPatternMatch(alloc, match);
        initialized_matches += 1;
    }

    const metric_status = try alloc.alloc(db_mod.types.GraphMetricStatus, source.metric_status.len);
    var initialized_metric_status: usize = 0;
    errdefer {
        for (metric_status[0..initialized_metric_status]) |*status| status.deinit(alloc);
        if (source.metric_status.len > 0) alloc.free(metric_status);
    }
    for (source.metric_status, 0..) |status, i| {
        metric_status[i] = try cloneGraphMetricStatus(alloc, status);
        initialized_metric_status += 1;
    }

    return .{
        .name = try alloc.dupe(u8, source.name),
        .nodes = nodes,
        .paths = paths,
        .matches = matches,
        .hits = hits,
        .total_hits = source.total_hits,
        .metric_status = metric_status,
    };
}

fn cloneGraphMetricStatus(
    alloc: std.mem.Allocator,
    source: db_mod.types.GraphMetricStatus,
) !db_mod.types.GraphMetricStatus {
    const name = try alloc.dupe(u8, source.name);
    errdefer alloc.free(name);
    var edge_filter = try source.edge_filter.cloneAlloc(alloc);
    errdefer edge_filter.deinit(alloc);
    const recent_events = if (source.recent_events.len > 0)
        try alloc.dupe(graph_mod.GraphIndex.GraphMetricEvent, source.recent_events)
    else
        @constCast((&[_]graph_mod.GraphIndex.GraphMetricEvent{})[0..]);
    errdefer if (recent_events.len > 0) alloc.free(recent_events);
    const last_error = if (source.last_error.len > 0) try alloc.dupe(u8, source.last_error) else "";
    errdefer if (last_error.len > 0) alloc.free(last_error);
    const build_worker_id = if (source.build_worker_id.len > 0) try alloc.dupe(u8, source.build_worker_id) else "";
    errdefer if (build_worker_id.len > 0) alloc.free(build_worker_id);
    return .{
        .name = name,
        .state = source.state,
        .phase = source.phase,
        .edge_filter = edge_filter,
        .metadata_version = source.metadata_version,
        .maintenance_paused = source.maintenance_paused,
        .build_queued = source.build_queued,
        .published_generation = source.published_generation,
        .edge_generation = source.edge_generation,
        .target_edge_generation = source.target_edge_generation,
        .queued_generation = source.queued_generation,
        .building_generation = source.building_generation,
        .build_job_id = source.build_job_id,
        .build_started_at_ms = source.build_started_at_ms,
        .build_iteration = source.build_iteration,
        .build_lease_expires_at_ms = source.build_lease_expires_at_ms,
        .build_worker_id = build_worker_id,
        .retry_count = source.retry_count,
        .last_error = last_error,
        .progress = source.progress,
        .converged = source.converged,
        .iterations_completed = source.iterations_completed,
        .delta = source.delta,
        .computed_at_ms = source.computed_at_ms,
        .last_event = source.last_event,
        .recent_events = recent_events,
    };
}

fn cloneGraphPatternMatch(
    alloc: std.mem.Allocator,
    source: db_mod.types.GraphPatternMatch,
) !db_mod.types.GraphPatternMatch {
    const bindings = try alloc.alloc(db_mod.types.GraphPatternBinding, source.bindings.len);
    var initialized_bindings: usize = 0;
    errdefer {
        for (bindings[0..initialized_bindings]) |*binding| binding.deinit(alloc);
        if (source.bindings.len > 0) alloc.free(bindings);
    }
    for (source.bindings, 0..) |binding, i| {
        bindings[i] = .{
            .alias = try alloc.dupe(u8, binding.alias),
            .node = try cloneGraphResultNode(alloc, binding.node),
        };
        initialized_bindings += 1;
    }

    const path = try alloc.alloc(graph_query_mod.PathEdgeInfo, source.path.len);
    var initialized_path: usize = 0;
    errdefer {
        for (path[0..initialized_path]) |edge| {
            alloc.free(edge.source);
            alloc.free(edge.target);
            alloc.free(edge.edge_type);
        }
        if (source.path.len > 0) alloc.free(path);
    }
    for (source.path, 0..) |edge, i| {
        path[i] = .{
            .source = try alloc.dupe(u8, edge.source),
            .target = try alloc.dupe(u8, edge.target),
            .edge_type = try alloc.dupe(u8, edge.edge_type),
            .weight = edge.weight,
        };
        initialized_path += 1;
    }

    return .{
        .bindings = bindings,
        .path = path,
    };
}

fn cloneGraphResultNode(
    alloc: std.mem.Allocator,
    source: graph_query_mod.GraphResultNode,
) !graph_query_mod.GraphResultNode {
    const path = if (source.path) |items| blk: {
        const out = try alloc.alloc([]const u8, items.len);
        errdefer alloc.free(out);
        for (items, 0..) |item, i| out[i] = try alloc.dupe(u8, item);
        break :blk out;
    } else null;

    const path_edges = if (source.path_edges) |items| blk: {
        const PathEdge = std.meta.Child(@TypeOf(items));
        const out = try alloc.alloc(PathEdge, items.len);
        errdefer alloc.free(out);
        for (items, 0..) |item, i| {
            out[i] = .{
                .source = try alloc.dupe(u8, item.source),
                .target = try alloc.dupe(u8, item.target),
                .edge_type = try alloc.dupe(u8, item.edge_type),
                .weight = item.weight,
            };
        }
        break :blk out;
    } else null;

    const provenance = if (source.provenance) |items| blk: {
        const out = try alloc.alloc([]const u8, items.len);
        errdefer alloc.free(out);
        for (items, 0..) |item, i| out[i] = try alloc.dupe(u8, item);
        break :blk out;
    } else null;

    const metrics = try alloc.alloc(graph_query_mod.GraphMetricValue, source.metrics.len);
    var initialized_metrics: usize = 0;
    errdefer {
        for (metrics[0..initialized_metrics]) |*metric| metric.deinit(alloc);
        if (source.metrics.len > 0) alloc.free(metrics);
    }
    for (source.metrics, 0..) |metric, i| {
        metrics[i] = .{
            .name = try alloc.dupe(u8, metric.name),
            .score = metric.score,
        };
        initialized_metrics += 1;
    }

    return .{
        .key = try alloc.dupe(u8, source.key),
        .depth = source.depth,
        .distance = source.distance,
        .path = path,
        .path_edges = path_edges,
        .provenance = provenance,
        .table = if (source.table) |table| try alloc.dupe(u8, table) else null,
        .metrics = metrics,
    };
}

fn cloneGraphPath(
    alloc: std.mem.Allocator,
    source: db_mod.types.GraphPath,
) !db_mod.types.GraphPath {
    const nodes = try alloc.alloc([]const u8, source.nodes.len);
    errdefer alloc.free(nodes);
    for (source.nodes, 0..) |node, i| nodes[i] = try alloc.dupe(u8, node);

    const PathEdge = std.meta.Child(@TypeOf(source.edges));
    const edges = try alloc.alloc(PathEdge, source.edges.len);
    errdefer alloc.free(edges);
    for (source.edges, 0..) |edge, i| {
        edges[i] = .{
            .source = try alloc.dupe(u8, edge.source),
            .target = try alloc.dupe(u8, edge.target),
            .edge_type = try alloc.dupe(u8, edge.edge_type),
            .weight = edge.weight,
        };
    }

    return .{
        .nodes = nodes,
        .edges = edges,
        .total_weight = source.total_weight,
        .length = source.length,
    };
}

test "query parser accepts full text request subset" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"full_text_search":{"match":{"field":"body","text":"alpha"}},"fields":["title"],"limit":5}
    );
    defer owned.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u32, 5), owned.req.limit);
    try std.testing.expectEqual(@as(usize, 1), owned.req.fields.len);
    try std.testing.expectEqual(false, owned.req.include_all_fields);
}

test "query parser accepts generated query request shape" {
    const metadata_openapi = @import("antfly_metadata_openapi");

    var full_text = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"match":{"field":"body","text":"alpha"}}
    , .{});
    defer full_text.deinit();

    const body = try jsonStringifyAlloc(std.testing.allocator, metadata_openapi.QueryRequest{
        .full_text_search = full_text.value,
        .fields = &.{"title"},
        .limit = 5,
        .profile = false,
    });
    defer std.testing.allocator.free(body);

    var owned = try parseQueryRequest(std.testing.allocator, null, "docs", body);
    defer owned.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u32, 5), owned.req.limit);
    try std.testing.expectEqual(@as(usize, 1), owned.req.fields.len);
    try std.testing.expectEqualStrings("title", owned.req.fields[0]);
}

test "query parser defers ordinary stored projection to response encoding" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"fields":["id","title"],"limit":5}
    );
    defer owned.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 2), owned.req.fields.len);
    try std.testing.expect(owned.req.defer_stored_projection);
}

test "query parser defaults to key-only when fields are omitted" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"limit":5}
    );
    defer owned.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), owned.req.fields.len);
    try std.testing.expectEqual(false, owned.req.include_all_fields);
    try std.testing.expectEqual(false, owned.req.include_stored);
    try std.testing.expectEqual(false, owned.req.defer_stored_projection);
}

test "query parser keeps special stored projection in db layer" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"fields":["title","_chunks.*"],"limit":5}
    );
    defer owned.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 2), owned.req.fields.len);
    try std.testing.expect(!owned.req.defer_stored_projection);
}

test "query parser accepts generated count and profile flags" {
    const metadata_openapi = @import("antfly_metadata_openapi");

    var full_text = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"match":{"field":"body","text":"alpha"}}
    , .{});
    defer full_text.deinit();

    const body = try jsonStringifyAlloc(std.testing.allocator, metadata_openapi.QueryRequest{
        .full_text_search = full_text.value,
        .count = true,
        .profile = true,
    });
    defer std.testing.allocator.free(body);

    var owned = try parseQueryRequest(std.testing.allocator, null, "docs", body);
    defer owned.deinit(std.testing.allocator);
    try std.testing.expect(owned.req.count_only);
    try std.testing.expect(owned.req.profile);
}

test "query parser accepts aggregations" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"full_text_search":{"match":{"field":"body","text":"alpha"}},"aggregations":{"price_stats":{"type":"stats","field":"price"},"categories":{"type":"terms","field":"category","size":5}}}
    );
    defer owned.deinit(std.testing.allocator);

    try std.testing.expect(owned.req.aggregations_json.len > 0);
    try std.testing.expect(std.mem.indexOf(u8, owned.req.aggregations_json, "\"price_stats\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, owned.req.aggregations_json, "\"categories\"") != null);
}

test "query parser accepts bleve match query shape" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"full_text_search":{"match":"alpha","field":"body"},"limit":5}
    );
    defer owned.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u32, 5), owned.req.limit);
    try std.testing.expect(owned.req.full_text != null);
    try std.testing.expect(owned.req.full_text.? == .match);
    try std.testing.expectEqualStrings("body", owned.req.full_text.?.match.field);
    try std.testing.expectEqualStrings("alpha", owned.req.full_text.?.match.text);
}

test "query parser accepts bleve match_all query shape" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"full_text_search":{"match_all":{}},"limit":5}
    );
    defer owned.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u32, 5), owned.req.limit);
    try std.testing.expect(owned.req.full_text != null);
    try std.testing.expect(owned.req.full_text.? == .match_all);
}

test "query parser accepts bleve boolean filter shape" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"full_text_search":{"filter":{"match_all":{}}}}
    );
    defer owned.deinit(std.testing.allocator);
    try std.testing.expect(owned.req.full_text != null);
    try std.testing.expect(owned.req.full_text.? == .bool_query);
}

test "query parser preserves filter and exclusion request JSON" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"full_text_search":{"match":"alpha","field":"body"},"filter_query":{"term":"published","field":"status"},"exclusion_query":{"term":"draft","field":"status"}}
    );
    defer owned.deinit(std.testing.allocator);
    try std.testing.expect(owned.req.full_text != null);
    try std.testing.expect(owned.req.full_text.? == .match);
    try std.testing.expect(std.mem.indexOf(u8, owned.req.filter_query_json, "\"term\":{\"status\":\"published\"}") != null);
    try std.testing.expect(std.mem.indexOf(u8, owned.req.exclusion_query_json, "\"term\":{\"status\":\"draft\"}") != null);
}

test "query parser does not use dense fast path when public filters are present" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"embeddings":{"dense_idx":[0.1,0.2]},"indexes":["dense_idx"],"filter_query":{"term":{"status":"published"}},"exclusion_query":{"term":{"status":"draft"}},"limit":5}
    );
    defer owned.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), owned.req.dense_queries.len);
    try std.testing.expect(std.mem.indexOf(u8, owned.req.filter_query_json, "\"status\":\"published\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, owned.req.exclusion_query_json, "\"status\":\"draft\"") != null);
}

test "query parser accepts typed bleve leaf queries through db full_text" {
    var fuzzy = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"full_text_search":{"term":"alph","field":"body","fuzziness":1}}
    );
    defer fuzzy.deinit(std.testing.allocator);
    try std.testing.expect(fuzzy.req.full_text != null);
    try std.testing.expect(fuzzy.req.full_text.? == .fuzzy or fuzzy.req.full_text.? == .term);
    switch (fuzzy.req.full_text.?) {
        .fuzzy => |q| try std.testing.expectEqualStrings("alph", q.term),
        .term => |q| try std.testing.expectEqualStrings("alph", q.term),
        else => return error.TestUnexpectedResult,
    }

    var numeric = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"full_text_search":{"field":"score","min":10,"max":20,"inclusive_max":true}}
    );
    defer numeric.deinit(std.testing.allocator);
    try std.testing.expect(numeric.req.full_text != null);
    try std.testing.expect(numeric.req.full_text.? == .numeric_range);
    try std.testing.expectEqual(@as(f64, 10), numeric.req.full_text.?.numeric_range.min.?);
    try std.testing.expectEqual(@as(f64, 20), numeric.req.full_text.?.numeric_range.max.?);
    try std.testing.expectEqual(true, numeric.req.full_text.?.numeric_range.inclusive_max);

    var date_range = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"full_text_search":{"field":"created_at","start":"2026-03-01T00:00:00Z","end":"2026-03-31","inclusive_end":true}}
    );
    defer date_range.deinit(std.testing.allocator);
    try std.testing.expect(date_range.req.full_text != null);
    try std.testing.expect(date_range.req.full_text.? == .date_range);
    try std.testing.expect(date_range.req.full_text.?.date_range.start_ns != null);
    try std.testing.expect(date_range.req.full_text.?.date_range.end_ns != null);
    try std.testing.expectEqual(true, date_range.req.full_text.?.date_range.inclusive_end);
}

test "query parser accepts bleve query string queries" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"full_text_search":{"query":"body:alpha AND title:\"beta gamma\""},"limit":5}
    );
    defer owned.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u32, 5), owned.req.limit);
    try std.testing.expect(owned.req.full_text != null);
    try std.testing.expect(owned.req.full_text.? == .bool_query);
    const root = owned.req.full_text.?.bool_query;
    try std.testing.expectEqual(@as(usize, 2), root.must.len);
    try std.testing.expect(root.must[0] == .match);
    try std.testing.expectEqualStrings("body", root.must[0].match.field);
    try std.testing.expectEqualStrings("alpha", root.must[0].match.text);
    try std.testing.expect(root.must[1] == .match_phrase);
    try std.testing.expectEqualStrings("title", root.must[1].match_phrase.field);
    try std.testing.expectEqualStrings("beta gamma", root.must[1].match_phrase.text);
}

test "query parser accepts bleve query string boosts" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"full_text_search":{"query":"body:alpha^2 AND title:\"beta gamma\"~3^4"}}
    );
    defer owned.deinit(std.testing.allocator);

    try std.testing.expect(owned.req.full_text != null);
    try std.testing.expect(owned.req.full_text.? == .bool_query);
    const root = owned.req.full_text.?.bool_query;
    try std.testing.expectEqual(@as(usize, 2), root.must.len);
    try std.testing.expect(root.must[0] == .match);
    try std.testing.expectApproxEqAbs(@as(f32, 2.0), root.must[0].match.boost, 0.0001);
    try std.testing.expect(root.must[1] == .match_phrase);
    try std.testing.expectApproxEqAbs(@as(f32, 4.0), root.must[1].match_phrase.boost, 0.0001);
}

test "query parser accepts bleve query string field groups" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"full_text_search":{"query":"title:(alpha beta)"}}
    );
    defer owned.deinit(std.testing.allocator);

    try std.testing.expect(owned.req.full_text != null);
    try std.testing.expect(owned.req.full_text.? == .bool_query);
    const root = owned.req.full_text.?.bool_query;
    try std.testing.expectEqual(@as(usize, 2), root.must.len);
    try std.testing.expect(root.must[0] == .match);
    try std.testing.expect(root.must[1] == .match);
    try std.testing.expectEqualStrings("title", root.must[0].match.field);
    try std.testing.expectEqualStrings("alpha", root.must[0].match.text);
    try std.testing.expectEqualStrings("title", root.must[1].match.field);
    try std.testing.expectEqualStrings("beta", root.must[1].match.text);
}

test "query parser accepts bleve query string inline ranges" {
    var numeric = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"full_text_search":{"query":"score:[10 TO 20}"}}
    );
    defer numeric.deinit(std.testing.allocator);

    try std.testing.expect(numeric.req.full_text != null);
    try std.testing.expect(numeric.req.full_text.? == .numeric_range);
    try std.testing.expectEqual(@as(f64, 10), numeric.req.full_text.?.numeric_range.min.?);
    try std.testing.expectEqual(@as(f64, 20), numeric.req.full_text.?.numeric_range.max.?);
    try std.testing.expect(numeric.req.full_text.?.numeric_range.inclusive_min);
    try std.testing.expect(!numeric.req.full_text.?.numeric_range.inclusive_max);

    var date = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"full_text_search":{"query":"created:[2024-01-01T00:00:00Z TO 2024-12-31T00:00:00Z]"}}
    );
    defer date.deinit(std.testing.allocator);

    try std.testing.expect(date.req.full_text != null);
    try std.testing.expect(date.req.full_text.? == .date_range);
    try std.testing.expect(date.req.full_text.?.date_range.start_ns != null);
    try std.testing.expect(date.req.full_text.?.date_range.end_ns != null);

    var term = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"full_text_search":{"query":"title:[alpha TO omega]"}}
    );
    defer term.deinit(std.testing.allocator);

    try std.testing.expect(term.req.full_text != null);
    try std.testing.expect(term.req.full_text.? == .term_range);
    try std.testing.expectEqualStrings("alpha", term.req.full_text.?.term_range.min.?);
    try std.testing.expectEqualStrings("omega", term.req.full_text.?.term_range.max.?);
}

test "query parser accepts bleve query string filters" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"full_text_search":{"query":"alpha"},"filter_query":{"query":"status:published OR status:review"}}
    );
    defer owned.deinit(std.testing.allocator);

    try std.testing.expect(owned.req.full_text != null);
    try std.testing.expect(owned.req.full_text.? == .match);
    try std.testing.expect(std.mem.indexOf(u8, owned.req.filter_query_json, "\"bool\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, owned.req.filter_query_json, "\"status\":\"published\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, owned.req.filter_query_json, "\"status\":\"review\"") != null);
}

test "query parser rejects invalid bleve date ranges" {
    try std.testing.expectError(error.UnsupportedQueryRequest, parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"full_text_search":{"field":"created_at","start":"not-a-date"}}
    ));
}

test "query parser resolves semantic search into dense query" {
    var owned = try parseQueryRequest(std.testing.allocator, FakeSemanticResolver.iface(), "docs",
        \\{"semantic_search":"alpha concept","indexes":["semantic_idx"],"limit":4}
    );
    defer owned.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), owned.req.dense_queries.len);
    try std.testing.expectEqualStrings("semantic_idx", owned.req.dense_queries[0].index_name);
    try std.testing.expectEqual(@as(u32, 4), owned.req.dense_queries[0].query.k);
    try std.testing.expectEqual(@as(usize, 3), owned.req.dense_queries[0].query.vector.len);
}

test "query parser preserves search effort for semantic search" {
    var owned = try parseQueryRequest(std.testing.allocator, FakeSemanticResolver.iface(), "docs",
        \\{"semantic_search":"alpha concept","indexes":["semantic_idx"],"limit":4,"search_effort":0.3}
    );
    defer owned.deinit(std.testing.allocator);

    try std.testing.expect(owned.req.search_effort != null);
    try std.testing.expectApproxEqAbs(@as(f32, 0.3), owned.req.search_effort.?, 0.0001);
}

test "query parser accepts semantic embedding template" {
    var owned = try parseQueryRequest(std.testing.allocator, FakeSemanticResolver.iface(), "docs",
        \\{"semantic_search":"alpha concept","embedding_template":"{{remotePDF url=this}}","indexes":["semantic_idx"],"limit":4}
    );
    defer owned.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), owned.req.dense_queries.len);
    try std.testing.expectEqualStrings("semantic_idx", owned.req.dense_queries[0].index_name);
}

test "query parser accepts precomputed embedding payload" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"embeddings":{"semantic_idx":[0.5,1.5,2.5]},"limit":6}
    );
    defer owned.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), owned.req.dense_queries.len);
    try std.testing.expectEqualStrings("semantic_idx", owned.req.dense_queries[0].index_name);
    try std.testing.expectEqual(@as(u32, 6), owned.req.dense_queries[0].query.k);
    try std.testing.expectEqual(@as(usize, 3), owned.req.dense_queries[0].query.vector.len);
    try std.testing.expectEqual(@as(f32, 1.5), owned.req.dense_queries[0].query.vector[1]);
}

test "query parser accepts packed dense embedding payload" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"embeddings":{"semantic_idx":"AAAAPwAAwD8AACBA"},"indexes":["semantic_idx"],"fields":["title"],"search_effort":0.3,"limit":6}
    );
    defer owned.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), owned.req.dense_queries.len);
    try std.testing.expectEqualStrings("semantic_idx", owned.req.dense_queries[0].index_name);
    try std.testing.expectEqual(@as(u32, 6), owned.req.dense_queries[0].query.k);
    try std.testing.expectEqual(@as(usize, 3), owned.req.dense_queries[0].query.vector.len);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), owned.req.dense_queries[0].query.vector[0], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 1.5), owned.req.dense_queries[0].query.vector[1], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 2.5), owned.req.dense_queries[0].query.vector[2], 0.0001);
    try std.testing.expectEqual(@as(?f32, 0.3), owned.req.search_effort);
    try std.testing.expect(owned.req.defer_stored_projection);
}

test "query parser rejects packed dense indexes that reference a missing embedding" {
    try std.testing.expectError(error.UnsupportedQueryRequest, parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"embeddings":{"semantic_idx":"AAAAPwAAwD8AACBA"},"indexes":["missing_idx"],"limit":6}
    ));
}

test "query parser rejects invalid packed dense embedding payload" {
    try std.testing.expectError(error.InvalidQueryRequest, parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"embeddings":{"semantic_idx":"not-base64"},"indexes":["semantic_idx"],"limit":6}
    ));
}

test "query parser accepts sparse embedding payload" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"embeddings":{"sparse_idx":{"indices":[1,7],"values":[0.4,0.9]}},"limit":6}
    );
    defer owned.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), owned.req.sparse_queries.len);
    try std.testing.expectEqualStrings("sparse_idx", owned.req.sparse_queries[0].index_name);
    try std.testing.expectEqual(@as(u32, 6), owned.req.sparse_queries[0].query.k);
    try std.testing.expectEqual(@as(usize, 2), owned.req.sparse_queries[0].query.indices.len);
    try std.testing.expectEqual(@as(u32, 7), owned.req.sparse_queries[0].query.indices[1]);
}

test "query parser accepts merge config reranker and pruner" {
    var owned = try parseQueryRequest(std.testing.allocator, FakeSemanticResolver.iface(), "docs",
        \\{"semantic_search":"alpha concept","indexes":["semantic_idx"],"full_text_search":{"match":{"field":"body","text":"alpha concept"}},"merge_config":{"strategy":"rsf","window_size":25,"rank_constant":42.0,"weights":{"full_text":0.5,"semantic_idx":1.5}},"reranker":{"provider":"antfly","model":"cross-encoder/ms-marco-MiniLM-L-6-v2","field":"body","top_n":3},"pruner":{"min_score_ratio":0.5,"require_multi_index":true},"limit":6}
    );
    defer owned.deinit(std.testing.allocator);

    try std.testing.expect(owned.req.merge_config != null);
    try std.testing.expectEqual(.rsf, owned.req.merge_config.?.strategy);
    try std.testing.expectEqual(@as(u32, 25), owned.req.merge_config.?.window_size);
    try std.testing.expectEqual(@as(usize, 2), owned.req.merge_config.?.weights.len);
    try std.testing.expect(owned.req.reranker != null);
    try std.testing.expectEqual(.antfly, owned.req.reranker.?.provider);
    try std.testing.expectEqualStrings("body", owned.req.reranker.?.field);
    try std.testing.expectEqual(@as(?u32, 3), owned.req.reranker.?.top_n);
    try std.testing.expectEqualStrings("alpha concept", owned.req.reranker_query_text);
    try std.testing.expect(owned.req.include_stored);
    try std.testing.expect(owned.req.pruner != null);
    try std.testing.expectEqual(@as(f64, 0.5), owned.req.pruner.?.min_score_ratio);
    try std.testing.expect(owned.req.pruner.?.require_multi_index);
}

test "query parser keeps stored documents for dense reranking without fields" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"embeddings":{"dense_idx":[1.0,0.0,0.0]},"indexes":["dense_idx"],"reranker":{"provider":"antfly","model":"cross-encoder/ms-marco-MiniLM-L-6-v2","field":"body","top_n":2},"limit":6}
    );
    defer owned.deinit(std.testing.allocator);

    try std.testing.expect(owned.req.reranker != null);
    try std.testing.expect(owned.req.include_stored);
}

test "query parser accepts graph searches" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"graph_searches":{"neighbors":{"type":"neighbors","index_name":"graph_idx","start_nodes":{"keys":["doc:a"]},"params":{"edge_types":["links"]},"metrics":["pagerank"],"order_by":[{"metric":"pagerank","direction":"desc","nulls":"last"}],"where_metric":[{"metric":"pagerank","op":">=","value":0.01}],"metric_freshness":"fresh","include_metric_status":true}},"limit":10}
    );
    defer owned.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), owned.req.graph_queries.len);
    try std.testing.expectEqualStrings("neighbors", owned.req.graph_queries[0].name);
    try std.testing.expectEqualStrings("graph_idx", owned.req.graph_queries[0].query.index_name);
    try std.testing.expect(owned.req.graph_queries[0].query.query_type == .neighbors);
    switch (owned.req.graph_queries[0].query.start_nodes) {
        .keys => |keys| try std.testing.expectEqualStrings("doc:a", keys[0]),
        else => return error.TestUnexpectedResult,
    }
    try std.testing.expectEqual(@as(usize, 1), owned.req.graph_queries[0].query.metrics.len);
    try std.testing.expectEqualStrings("pagerank", owned.req.graph_queries[0].query.metrics[0].name);
    try std.testing.expectEqual(graph_query_mod.GraphMetricFreshness.fresh, owned.req.graph_queries[0].query.metrics[0].freshness);
    try std.testing.expectEqual(@as(usize, 1), owned.req.graph_queries[0].query.order_by.len);
    try std.testing.expectEqualStrings("pagerank", owned.req.graph_queries[0].query.order_by[0].name);
    try std.testing.expectEqual(graph_query_mod.GraphMetricOrderDirection.desc, owned.req.graph_queries[0].query.order_by[0].direction);
    try std.testing.expectEqual(graph_query_mod.GraphMetricNullOrder.last, owned.req.graph_queries[0].query.order_by[0].nulls);
    try std.testing.expectEqual(graph_query_mod.GraphMetricFreshness.fresh, owned.req.graph_queries[0].query.order_by[0].freshness);
    try std.testing.expectEqual(@as(usize, 1), owned.req.graph_queries[0].query.where_metric.len);
    try std.testing.expectEqualStrings("pagerank", owned.req.graph_queries[0].query.where_metric[0].name);
    try std.testing.expectEqual(graph_query_mod.GraphMetricFilterOp.gte, owned.req.graph_queries[0].query.where_metric[0].op);
    try std.testing.expectApproxEqAbs(@as(f64, 0.01), owned.req.graph_queries[0].query.where_metric[0].value, 0.000001);
    try std.testing.expectEqual(graph_query_mod.GraphMetricFreshness.fresh, owned.req.graph_queries[0].query.where_metric[0].freshness);
    try std.testing.expect(owned.req.graph_queries[0].query.include_metric_status);
}

test "query parser accepts direct graph metric reads" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"graph_metric":{"index":"graph_idx","metric":"pagerank","top_k":25,"metric_freshness":"fresh"}}
    );
    defer owned.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), owned.req.graph_metric_queries.len);
    try std.testing.expectEqualStrings("pagerank", owned.req.graph_metric_queries[0].name);
    try std.testing.expectEqualStrings("graph_idx", owned.req.graph_metric_queries[0].query.index_name);
    try std.testing.expectEqualStrings("pagerank", owned.req.graph_metric_queries[0].query.metric_name);
    try std.testing.expectEqual(@as(u32, 25), owned.req.graph_metric_queries[0].query.top_k);
    try std.testing.expectEqual(db_mod.types.GraphMetricFreshness.fresh, owned.req.graph_metric_queries[0].query.freshness);
}

test "query parser accepts graph metric rerank" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"full_text_search":{"match_all":{}},"graph_metric_rerank":{"index":"graph_idx","metric":"pagerank","base_weight":0.5,"weight":2.5,"missing_score":-0.25,"metric_freshness":"fresh"},"limit":10}
    );
    defer owned.deinit(std.testing.allocator);

    try std.testing.expect(owned.req.graph_metric_rerank != null);
    try std.testing.expectEqualStrings("graph_idx", owned.req.graph_metric_rerank.?.index_name);
    try std.testing.expectEqualStrings("pagerank", owned.req.graph_metric_rerank.?.metric_name);
    try std.testing.expectEqual(db_mod.types.GraphMetricFreshness.fresh, owned.req.graph_metric_rerank.?.freshness);
    try std.testing.expectApproxEqAbs(@as(f64, 0.5), owned.req.graph_metric_rerank.?.base_weight, 0.000001);
    try std.testing.expectApproxEqAbs(@as(f64, 2.5), owned.req.graph_metric_rerank.?.weight, 0.000001);
    try std.testing.expectApproxEqAbs(@as(f64, -0.25), owned.req.graph_metric_rerank.?.missing_score, 0.000001);
}

test "query parser accepts graph pattern searches" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"graph_searches":{"pattern_walk":{"type":"pattern","index_name":"graph_idx","start_nodes":{"keys":["doc:a"]},"pattern":[{"alias":"a"},{"alias":"b","edge":{"types":["links"],"direction":"out","min_hops":1,"max_hops":2}}],"return_aliases":["b"],"include_documents":true}},"limit":10}
    );
    defer owned.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), owned.req.graph_queries.len);
    try std.testing.expect(owned.req.graph_queries[0].query.query_type == .pattern);
    try std.testing.expectEqual(@as(usize, 2), owned.req.graph_queries[0].query.pattern.len);
    try std.testing.expectEqual(@as(usize, 1), owned.req.graph_queries[0].query.return_aliases.len);
    try std.testing.expect(owned.req.graph_queries[0].query.include_documents);
}

test "query parser rejects semantic search offsets" {
    try std.testing.expectError(error.UnsupportedQueryRequest, parseQueryRequest(std.testing.allocator, FakeSemanticResolver.iface(), "docs",
        \\{"semantic_search":"alpha concept","indexes":["semantic_idx"],"limit":4,"offset":1}
    ));
}

test "query encoder emits antfly-style response envelope" {
    const alloc = std.testing.allocator;
    var hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .score = 1.25,
        .stored_data = try alloc.dupe(u8, "{\"title\":\"alpha\"}"),
    };
    var result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 1,
    };
    defer result.deinit();

    var encoded = try encodeQueryResponses(alloc, "docs", .{}, .{}, result);
    defer encoded.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"responses\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"_id\":\"doc:a\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"table\":\"docs\"") != null);
}

test "query encoder emits graph metric results" {
    const alloc = std.testing.allocator;
    var scores = try alloc.alloc(db_mod.types.GraphMetricScore, 1);
    scores[0] = .{
        .node = try alloc.dupe(u8, "doc:b"),
        .score = 0.75,
    };
    var metric_results = try alloc.alloc(db_mod.types.GraphMetricResult, 1);
    metric_results[0] = .{
        .name = try alloc.dupe(u8, "pagerank"),
        .index_name = try alloc.dupe(u8, "graph_idx"),
        .metric_name = try alloc.dupe(u8, "pagerank"),
        .scores = scores,
        .status = .{
            .name = try alloc.dupe(u8, "pagerank"),
            .state = .fresh,
            .published_generation = 3,
            .edge_generation = 3,
            .converged = true,
            .iterations_completed = 12,
            .delta = 0.00001,
            .computed_at_ms = 1780000000000,
        },
    };
    var result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_metric_results = metric_results,
    };
    defer result.deinit();

    var encoded = try encodeQueryResponses(alloc, "docs", .{}, .{}, result);
    defer encoded.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"graph_metric_results\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"pagerank\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"state\":\"fresh\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"node\":\"doc:b\"") != null);
}

test "query encoder does not expose internal doc ordinals" {
    const alloc = std.testing.allocator;
    var hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .doc_ordinal = 42,
        .score = 1.25,
        .stored_data = try alloc.dupe(u8, "{\"title\":\"alpha\"}"),
    };
    var result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 1,
    };
    defer result.deinit();

    var encoded = try encodeQueryResponses(alloc, "docs", .{}, .{}, result);
    defer encoded.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"_id\":\"doc:a\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "doc_ordinal") == null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "ordinal") == null);
}

test "query encoder emits aggregations" {
    const alloc = std.testing.allocator;
    var hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .score = 1.25,
        .stored_data = try alloc.dupe(u8, "{\"title\":\"alpha\",\"price\":10,\"category\":\"books\"}"),
    };
    var result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 1,
    };
    defer result.deinit();

    const aggregation_results = try alloc.alloc(db_mod.aggregations.SearchAggregationResult, 2);
    aggregation_results[0] = .{
        .name = "price_stats",
        .field = "price",
        .type = "stats",
        .value_json = try alloc.dupe(u8, "{\"count\":1,\"sum\":10,\"avg\":10,\"min\":10,\"max\":10,\"sum_squares\":100,\"variance\":0,\"std_dev\":0}"),
    };
    const buckets = try alloc.alloc(db_mod.aggregations.SearchAggregationBucket, 1);
    buckets[0] = .{
        .key_json = try alloc.dupe(u8, "\"books\""),
        .count = 1,
    };
    aggregation_results[1] = .{
        .name = "categories",
        .field = "category",
        .type = "terms",
        .buckets = buckets,
    };

    var meta: QueryResponseMeta = .{
        .aggregation_results = aggregation_results,
    };
    defer meta.deinit(alloc);

    var encoded = try encodeQueryResponses(alloc, "docs", .{
        .aggregations_json =
        \\{"price_stats":{"type":"stats","field":"price"},"categories":{"type":"terms","field":"category","size":5}}
        ,
    }, meta, result);
    defer encoded.deinit(alloc);

    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"aggregations\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"price_stats\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"sum\":10") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"categories\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"key\":\"books\"") != null);
}

test "query encoder supports count-only and profile responses" {
    const alloc = std.testing.allocator;
    var hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .score = 1.25,
        .stored_data = try alloc.dupe(u8, "{\"title\":\"alpha\"}"),
    };
    var result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 1,
    };
    const graph_metric_results = try alloc.alloc(db_mod.types.GraphMetricResult, 1);
    graph_metric_results[0] = .{
        .name = try alloc.dupe(u8, "central"),
        .index_name = try alloc.dupe(u8, "graph_idx"),
        .metric_name = try alloc.dupe(u8, "pagerank"),
        .scores = &.{},
        .status = .{
            .name = try alloc.dupe(u8, "pagerank"),
            .state = .fresh,
            .published_generation = 7,
            .edge_generation = 7,
            .target_edge_generation = 7,
            .progress = 1.0,
            .converged = true,
            .iterations_completed = 12,
            .computed_at_ms = 1780000000000,
        },
    };
    result.graph_metric_results = graph_metric_results;
    result.graph_metric_rerank_status = .{
        .name = try alloc.dupe(u8, "pagerank"),
        .state = .fresh,
        .published_generation = 7,
        .edge_generation = 7,
        .target_edge_generation = 7,
        .progress = 1.0,
        .converged = true,
        .iterations_completed = 12,
        .computed_at_ms = 1780000000000,
    };
    defer result.deinit();

    const graph_metric_queries = [_]db_mod.types.NamedGraphMetricQuery{.{
        .name = "central",
        .query = .{
            .index_name = "graph_idx",
            .metric_name = "pagerank",
            .freshness = .fresh,
        },
    }};

    var encoded = try encodeQueryResponses(alloc, "docs", .{
        .count_only = true,
        .profile = true,
        .graph_metric_queries = &graph_metric_queries,
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "pagerank",
            .freshness = .fresh,
            .weight = 1.0,
        },
    }, .{
        .took_ms = 7,
        .shard_count = 3,
        .merged = true,
        .dense_search = .{
            .resolved_search_width = 128,
            .resolved_epsilon = 0.15,
            .hbc_reranked_vectors = 42,
            .hbc_search_ns = 123456,
        },
    }, result);
    defer encoded.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"total\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"hits\":[]") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"profile\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"took\":7") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"shards\":{\"total\":3,\"successful\":3,\"failed\":0}") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"merge\":{\"strategy\":\"rrf\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"dense_search\":{\"total_ns\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"resolved_search_width\":128") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"resolved_epsilon\":0.15") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"hbc_reranked_vectors\":42") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"graph_metrics\":[{\"query_name\":\"central\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"source\":\"graph_metric\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"query_name\":\"graph_metric_rerank\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"source\":\"graph_metric_rerank\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"freshness\":\"fresh\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"published_generation\":7") != null);
}

test "query encoder emits graph metric rerank score details" {
    const alloc = std.testing.allocator;
    var hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .score = 4.0,
        .score_details = .{
            .index_name = try alloc.dupe(u8, "graph_idx"),
            .metric_name = try alloc.dupe(u8, "pagerank"),
            .base_score = 1.0,
            .base_weight = 0.5,
            .metric_score = 0.7,
            .metric_score_used = 0.7,
            .metric_weight = 5.0,
            .missing_score_used = false,
            .final_score = 4.0,
            .published_generation = 11,
        },
    };
    var result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 1,
    };
    defer result.deinit();

    var encoded = try encodeQueryResponses(alloc, "docs", .{}, .{ .took_ms = 2 }, result);
    defer encoded.deinit(alloc);

    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"_score_details\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"graph_metric_rerank\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"index_name\":\"graph_idx\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"metric_name\":\"pagerank\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"base_score\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"base_weight\":0.5") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"metric_score\":0.7") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"metric_score_used\":0.7") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"metric_weight\":5") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"missing_score_used\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"final_score\":4") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"published_generation\":11") != null);
}

test "query encoder projects deferred stored fields without round-tripping bytes" {
    const alloc = std.testing.allocator;
    var hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .score = 1.25,
        .stored_data = try alloc.dupe(u8, "{\"title\":\"alpha\",\"id\":\"stored-id\",\"body\":\"hello\"}"),
    };
    var result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 1,
    };
    defer result.deinit();

    var encoded = try encodeQueryResponses(alloc, "docs", .{
        .fields = &.{ "id", "title" },
        .include_all_fields = false,
        .defer_stored_projection = true,
    }, .{}, result);
    defer encoded.deinit(alloc);

    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"_id\":\"doc:a\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"_source\":{\"id\":\"stored-id\",\"title\":\"alpha\"}") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"body\"") == null);
}

test "query encoder omits _source for key-only hits" {
    const alloc = std.testing.allocator;
    var hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    hits[0] = .{
        .id = try alloc.dupe(u8, "doc:key-only"),
        .score = 0.75,
        .stored_data = null,
    };
    var result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 1,
    };
    defer result.deinit();

    var encoded = try encodeQueryResponses(alloc, "docs", .{
        .include_all_fields = false,
    }, .{}, result);
    defer encoded.deinit(alloc);

    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"_id\":\"doc:key-only\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"_source\"") == null);
}

test "query encoder emits graph results" {
    const alloc = std.testing.allocator;
    const graph_nodes = try alloc.alloc(graph_query_mod.GraphResultNode, 1);
    graph_nodes[0] = .{
        .key = try alloc.dupe(u8, "doc:b"),
        .depth = 1,
        .distance = 1,
        .path = null,
        .path_edges = null,
        .metrics = try alloc.dupe(graph_query_mod.GraphMetricValue, &.{
            .{ .name = try alloc.dupe(u8, "pagerank"), .score = 0.75 },
        }),
    };
    const graph_hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    graph_hits[0] = .{
        .id = try alloc.dupe(u8, "doc:b"),
        .score = 1,
        .stored_data = try alloc.dupe(u8, "{\"title\":\"beta\"}"),
    };
    const graph_results = try alloc.alloc(db_mod.types.GraphSearchResult, 1);
    graph_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .nodes = graph_nodes,
        .paths = &.{},
        .hits = graph_hits,
        .total_hits = 1,
        .metric_status = try alloc.dupe(db_mod.types.GraphMetricStatus, &.{
            .{
                .name = try alloc.dupe(u8, "pagerank"),
                .state = .fresh,
                .published_generation = 3,
                .edge_generation = 3,
                .converged = true,
                .iterations_completed = 12,
                .delta = 0.00001,
                .computed_at_ms = 1780000000000,
            },
        }),
    };
    var result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = graph_results,
    };
    defer result.deinit();

    var encoded = try encodeQueryResponses(alloc, "docs", .{
        .graph_queries = &.{
            .{
                .name = "neighbors",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "graph_idx",
                    .start_nodes = .{ .keys = &.{"doc:a"} },
                },
            },
        },
    }, .{ .took_ms = 4 }, result);
    defer encoded.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"graph_results\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"neighbors\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"type\":\"neighbors\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"document\":{\"title\":\"beta\"}") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"metrics\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"metric_status\":{\"pagerank\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"edge_filter\":{\"mode\":\"all\"}") != null);
}

test "query merge applies global score ordering and offset" {
    const alloc = std.testing.allocator;

    var left_hits = try alloc.alloc(db_mod.types.SearchHit, 2);
    left_hits[0] = .{
        .id = try alloc.dupe(u8, "doc:b"),
        .doc_ordinal = 2,
        .score = 2.0,
        .stored_data = try alloc.dupe(u8, "{\"title\":\"beta\"}"),
    };
    left_hits[1] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .doc_ordinal = 1,
        .score = 3.0,
        .stored_data = try alloc.dupe(u8, "{\"title\":\"alpha\"}"),
    };
    var right_hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    right_hits[0] = .{
        .id = try alloc.dupe(u8, "doc:c"),
        .score = 1.0,
        .stored_data = try alloc.dupe(u8, "{\"title\":\"gamma\"}"),
    };

    var left = db_mod.types.SearchResult{ .alloc = alloc, .hits = left_hits, .total_hits = 2 };
    defer left.deinit();
    var right = db_mod.types.SearchResult{ .alloc = alloc, .hits = right_hits, .total_hits = 1 };
    defer right.deinit();

    var merged = try mergeSearchResults(alloc, .{}, &.{ left, right }, 1, 1);
    defer merged.deinit();

    try std.testing.expectEqual(@as(u32, 3), merged.total_hits);
    try std.testing.expectEqual(@as(usize, 1), merged.hits.len);
    try std.testing.expectEqualStrings("doc:b", merged.hits[0].id);
    try std.testing.expectEqual(@as(?u32, null), merged.hits[0].doc_ordinal);
}

test "query merge applies deterministic graph metric top-k across shards" {
    const alloc = std.testing.allocator;

    const left_scores = try alloc.alloc(db_mod.types.GraphMetricScore, 2);
    left_scores[0] = .{ .node = try alloc.dupe(u8, "doc:b"), .score = 0.8 };
    left_scores[1] = .{ .node = try alloc.dupe(u8, "doc:d"), .score = 0.6 };
    const left_metrics = try alloc.alloc(db_mod.types.GraphMetricResult, 1);
    left_metrics[0] = .{
        .name = try alloc.dupe(u8, "central"),
        .index_name = try alloc.dupe(u8, "graph_idx"),
        .metric_name = try alloc.dupe(u8, "pagerank"),
        .scores = left_scores,
        .status = .{
            .name = try alloc.dupe(u8, "pagerank"),
            .state = .fresh,
            .published_generation = 5,
            .edge_generation = 5,
            .target_edge_generation = 5,
            .building_generation = 6,
            .build_job_id = 12345,
            .build_started_at_ms = 1780000000100,
            .build_iteration = 2,
            .progress = 1.0,
            .converged = true,
        },
    };
    var left = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_metric_results = left_metrics,
    };
    defer left.deinit();

    const right_scores = try alloc.alloc(db_mod.types.GraphMetricScore, 3);
    right_scores[0] = .{ .node = try alloc.dupe(u8, "doc:c"), .score = 0.9 };
    right_scores[1] = .{ .node = try alloc.dupe(u8, "doc:a"), .score = 0.8 };
    right_scores[2] = .{ .node = try alloc.dupe(u8, "doc:e"), .score = 0.1 };
    const right_metrics = try alloc.alloc(db_mod.types.GraphMetricResult, 1);
    right_metrics[0] = .{
        .name = try alloc.dupe(u8, "central"),
        .index_name = try alloc.dupe(u8, "graph_idx"),
        .metric_name = try alloc.dupe(u8, "pagerank"),
        .scores = right_scores,
        .status = .{
            .name = try alloc.dupe(u8, "pagerank"),
            .state = .stale,
            .build_queued = true,
            .published_generation = 5,
            .edge_generation = 6,
            .target_edge_generation = 6,
            .queued_generation = 6,
            .building_generation = 6,
            .build_job_id = 12345,
            .build_started_at_ms = 1780000000100,
            .build_iteration = 3,
            .progress = 0.0,
            .converged = true,
        },
    };
    var right = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_metric_results = right_metrics,
    };
    defer right.deinit();

    const graph_metric_queries = [_]db_mod.types.NamedGraphMetricQuery{.{
        .name = "central",
        .query = .{
            .index_name = "graph_idx",
            .metric_name = "pagerank",
            .top_k = 3,
        },
    }};
    var merged = try mergeSearchResults(alloc, .{ .graph_metric_queries = &graph_metric_queries }, &.{ left, right }, 0, 10);
    defer merged.deinit();

    try std.testing.expectEqual(@as(usize, 1), merged.graph_metric_results.len);
    const central = merged.graph_metric_results[0];
    try std.testing.expectEqualStrings("central", central.name);
    try std.testing.expectEqual(@as(usize, 3), central.scores.len);
    try std.testing.expectEqualStrings("doc:c", central.scores[0].node);
    try std.testing.expectEqualStrings("doc:a", central.scores[1].node);
    try std.testing.expectEqualStrings("doc:b", central.scores[2].node);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.stale, central.status.state);
    try std.testing.expect(central.status.build_queued);
    try std.testing.expectEqual(@as(u64, 5), central.status.published_generation);
    try std.testing.expectEqual(@as(u64, 6), central.status.edge_generation);
    try std.testing.expectEqual(@as(u64, 6), central.status.queued_generation);
    try std.testing.expectEqual(@as(u64, 6), central.status.building_generation);
    try std.testing.expectEqual(@as(u64, 12345), central.status.build_job_id);
    try std.testing.expectEqual(@as(u64, 1780000000100), central.status.build_started_at_ms);
    try std.testing.expectEqual(@as(u32, 3), central.status.build_iteration);

    right_metrics[0].status.published_generation = 4;
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_metric_queries = &graph_metric_queries }, &.{ left, right }, 0, 10));
}

test "query merge requires comparable graph metric rerank generations across shards" {
    const alloc = std.testing.allocator;

    var left_hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    left_hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .score = 3.0,
    };
    var left = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = left_hits,
        .total_hits = 1,
        .graph_metric_rerank_status = .{
            .name = try alloc.dupe(u8, "pagerank"),
            .state = .fresh,
            .published_generation = 8,
            .edge_generation = 8,
            .target_edge_generation = 8,
            .progress = 1.0,
            .converged = true,
        },
    };
    defer left.deinit();

    var right_hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    right_hits[0] = .{
        .id = try alloc.dupe(u8, "doc:b"),
        .score = 2.0,
    };
    var right = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = right_hits,
        .total_hits = 1,
        .graph_metric_rerank_status = .{
            .name = try alloc.dupe(u8, "pagerank"),
            .state = .stale,
            .published_generation = 8,
            .edge_generation = 9,
            .target_edge_generation = 9,
            .progress = 1.0,
            .converged = true,
        },
    };
    defer right.deinit();

    const req = db_mod.types.SearchRequest{
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "pagerank",
            .freshness = .published,
            .weight = 1.0,
        },
    };

    var merged = try mergeSearchResults(alloc, req, &.{ left, right }, 0, 10);
    defer merged.deinit();
    try std.testing.expect(merged.graph_metric_rerank_status != null);
    try std.testing.expectEqual(@as(u64, 8), merged.graph_metric_rerank_status.?.published_generation);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.stale, merged.graph_metric_rerank_status.?.state);
    try std.testing.expectEqualStrings("doc:a", merged.hits[0].id);

    right.graph_metric_rerank_status.?.published_generation = 7;
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, req, &.{ left, right }, 0, 10));
}

test "query merge preserves single-result doc ordinals" {
    const alloc = std.testing.allocator;

    var hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .doc_ordinal = 9,
        .score = 1.0,
    };

    var single = db_mod.types.SearchResult{ .alloc = alloc, .hits = hits, .total_hits = 1 };
    defer single.deinit();

    var merged = try mergeSearchResults(alloc, .{}, &.{single}, 0, 1);
    defer merged.deinit();

    try std.testing.expectEqual(@as(usize, 1), merged.hits.len);
    try std.testing.expectEqualStrings("doc:a", merged.hits[0].id);
    try std.testing.expectEqual(@as(?u32, 9), merged.hits[0].doc_ordinal);
}

test "query merge preserves common identity read generation" {
    const alloc = std.testing.allocator;

    var left_hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    left_hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .score = 1.0,
    };
    var right_hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    right_hits[0] = .{
        .id = try alloc.dupe(u8, "doc:b"),
        .score = 0.5,
    };

    var left = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = left_hits,
        .total_hits = 1,
        .identity_read_generation = 17,
    };
    defer left.deinit();
    var right = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = right_hits,
        .total_hits = 1,
        .identity_read_generation = 17,
    };
    defer right.deinit();

    var merged = try mergeSearchResults(alloc, .{}, &.{ left, right }, 0, 10);
    defer merged.deinit();
    try std.testing.expectEqual(@as(?u64, 17), merged.identity_read_generation);

    var stamped = try mergeSearchResults(alloc, .{ .identity_read_generation = 19 }, &.{ left, right }, 0, 10);
    defer stamped.deinit();
    try std.testing.expectEqual(@as(?u64, 19), stamped.identity_read_generation);

    right.identity_read_generation = 18;
    var mixed = try mergeSearchResults(alloc, .{}, &.{ left, right }, 0, 10);
    defer mixed.deinit();
    try std.testing.expectEqual(@as(?u64, null), mixed.identity_read_generation);
}

test "query merge orders pure dense results by ascending distance" {
    const alloc = std.testing.allocator;

    var left_hits = try alloc.alloc(db_mod.types.SearchHit, 2);
    left_hits[0] = .{
        .id = try alloc.dupe(u8, "doc:b"),
        .score = 1.0,
        .stored_data = null,
    };
    left_hits[1] = .{
        .id = try alloc.dupe(u8, "doc:c"),
        .score = 0.2,
        .stored_data = null,
    };
    var right_hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    right_hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .score = 0.0,
        .stored_data = null,
    };

    var left = db_mod.types.SearchResult{ .alloc = alloc, .hits = left_hits, .total_hits = 2 };
    defer left.deinit();
    var right = db_mod.types.SearchResult{ .alloc = alloc, .hits = right_hits, .total_hits = 1 };
    defer right.deinit();

    var req: db_mod.types.SearchRequest = .{};
    const dense_vec = try alloc.alloc(f32, 1);
    defer alloc.free(dense_vec);
    dense_vec[0] = 1.0;
    const dense_queries = try alloc.alloc(db_mod.types.NamedDenseQuery, 1);
    defer {
        alloc.free(dense_queries[0].index_name);
        alloc.free(dense_queries[0].query.vector);
        alloc.free(dense_queries);
    }
    dense_queries[0] = .{
        .name = "",
        .index_name = try alloc.dupe(u8, "dense_idx"),
        .query = .{ .vector = try alloc.dupe(f32, dense_vec), .k = 3 },
    };
    req.dense_queries = dense_queries;

    var merged = try mergeSearchResults(alloc, req, &.{ left, right }, 0, 3);
    defer merged.deinit();

    try std.testing.expectEqual(@as(usize, 3), merged.hits.len);
    try std.testing.expectEqualStrings("doc:a", merged.hits[0].id);
    try std.testing.expectEqualStrings("doc:c", merged.hits[1].id);
    try std.testing.expectEqualStrings("doc:b", merged.hits[2].id);
}
