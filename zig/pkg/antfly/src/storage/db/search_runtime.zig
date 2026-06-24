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
const platform = @import("antfly_platform");
const builtin = @import("builtin");

const aggregations_mod = @import("aggregations.zig");
const algebraic_mod = @import("algebraic/mod.zig");
const doc_set = @import("doc_set.zig");
const docstore_mod = @import("../docstore.zig");
const index_manager_mod = @import("catalog/index_manager.zig");
const planning_adapter_mod = @import("planning_adapter.zig");
const planning_bindings_mod = @import("planning_bindings.zig");
const planning_stats_mod = @import("planning_stats.zig");
const schema_mod = @import("../schema.zig");
const graph_mod = @import("../../graph/graph.zig");
const graph_query_mod = @import("../../graph/query.zig");
const graph_pattern_mod = @import("../../graph/pattern.zig");
const search_mod = @import("../../search/search.zig");
const types = @import("types.zig");
const db_query_graph = @import("query/graph_exec.zig");
const db_query_metrics = @import("query_metrics.zig");
const db_query_result_shape = @import("query/result_shape.zig");
const db_query_search = @import("query/search_exec.zig");
const distributed_stats_mod = @import("../../search/distributed_stats.zig");
const platform_time = @import("../../platform/time.zig");
const vectorindex_mod = @import("antfly_vectorindex");

const Allocator = std.mem.Allocator;
const NamedResultSet = db_query_graph.NamedResultSet;
const AlgebraicIndex = @import("algebraic/index.zig").Index;

pub const AlgebraicDocFilterRequest = struct {
    req: types.SearchRequest,
    index: ?*AlgebraicIndex = null,
    filter_doc_ids: [][]u8 = &.{},
    exclude_doc_ids: [][]u8 = &.{},
    resolved_doc_filter: ?*doc_set.ResolvedDocFilter = null,
    resolved_doc_filter_alloc: ?Allocator = null,

    pub fn deinit(self: *@This()) void {
        if (self.index) |index| {
            index.freeDocIds(self.filter_doc_ids);
            index.freeDocIds(self.exclude_doc_ids);
        }
        if (self.resolved_doc_filter) |filter| {
            const alloc = self.resolved_doc_filter_alloc.?;
            filter.deinit(alloc);
            alloc.destroy(filter);
        }
        self.* = undefined;
    }
};

fn benchQueryProfileEnabled() bool {
    return platform.env.getenv("ANTFLY_BENCH_QUERY_PROFILE") != null;
}

fn cloneGraphMetricStatusFromGraph(
    alloc: Allocator,
    source: graph_mod.GraphIndex.GraphMetricStatus,
) !types.GraphMetricStatus {
    const name = try alloc.dupe(u8, source.name);
    var name_moved = false;
    errdefer if (!name_moved) alloc.free(name);
    var edge_filter = try source.edge_filter.cloneAlloc(alloc);
    var edge_filter_moved = false;
    errdefer if (!edge_filter_moved) edge_filter.deinit(alloc);
    const recent_events = if (source.recent_events.len > 0)
        try alloc.dupe(graph_mod.GraphIndex.GraphMetricEvent, source.recent_events)
    else
        @constCast((&[_]graph_mod.GraphIndex.GraphMetricEvent{})[0..]);
    var recent_events_moved = false;
    errdefer if (!recent_events_moved and recent_events.len > 0) alloc.free(recent_events);
    const last_error = if (source.last_error.len > 0) try alloc.dupe(u8, source.last_error) else "";
    var last_error_moved = false;
    errdefer if (!last_error_moved and last_error.len > 0) alloc.free(last_error);
    const build_worker_id = if (source.build_worker_id.len > 0) try alloc.dupe(u8, source.build_worker_id) else "";
    var build_worker_id_moved = false;
    errdefer if (!build_worker_id_moved and build_worker_id.len > 0) alloc.free(build_worker_id);
    const build_cursor = if (source.build_cursor.len > 0) try alloc.dupe(u8, source.build_cursor) else "";
    var build_cursor_moved = false;
    errdefer if (!build_cursor_moved and build_cursor.len > 0) alloc.free(build_cursor);
    const build_pages = try cloneGraphMetricBuildPageStatusesFromGraph(alloc, source.build_pages);
    var build_pages_moved = false;
    errdefer if (!build_pages_moved) {
        for (build_pages) |*page| page.deinit(alloc);
        if (build_pages.len > 0) alloc.free(build_pages);
    };
    const out = types.GraphMetricStatus{
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
        .build_cursor = build_cursor,
        .build_completed_units = source.build_completed_units,
        .build_total_units = source.build_total_units,
        .build_pages = build_pages,
        .build_pages_truncated = source.build_pages_truncated,
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
    name_moved = true;
    edge_filter_moved = true;
    recent_events_moved = true;
    last_error_moved = true;
    build_worker_id_moved = true;
    build_cursor_moved = true;
    build_pages_moved = true;
    return out;
}

fn cloneGraphMetricBuildPageStatusesFromGraph(
    alloc: Allocator,
    source: []const graph_mod.GraphIndex.GraphMetricBuildPageStatus,
) ![]types.GraphMetricBuildPageStatus {
    if (source.len == 0) return @constCast((&[_]types.GraphMetricBuildPageStatus{})[0..]);
    const out = try alloc.alloc(types.GraphMetricBuildPageStatus, source.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*page| page.deinit(alloc);
        alloc.free(out);
    }
    for (source, 0..) |page, i| {
        const worker_id = if (page.worker_id.len > 0) try alloc.dupe(u8, page.worker_id) else "";
        var worker_id_moved = false;
        errdefer if (!worker_id_moved and worker_id.len > 0) alloc.free(worker_id);
        const cursor = if (page.cursor.len > 0) try alloc.dupe(u8, page.cursor) else "";
        var cursor_moved = false;
        errdefer if (!cursor_moved and cursor.len > 0) alloc.free(cursor);
        const last_error = if (page.last_error.len > 0) try alloc.dupe(u8, page.last_error) else "";
        var last_error_moved = false;
        errdefer if (!last_error_moved and last_error.len > 0) alloc.free(last_error);
        out[i] = .{
            .phase = page.phase,
            .iteration = page.iteration,
            .page_id = page.page_id,
            .state = page.state,
            .range_kind = page.range_kind,
            .worker_id = worker_id,
            .lease_expires_at_ms = page.lease_expires_at_ms,
            .attempt = page.attempt,
            .cursor = cursor,
            .completed_units = page.completed_units,
            .total_units = page.total_units,
            .last_error = last_error,
        };
        worker_id_moved = true;
        cursor_moved = true;
        last_error_moved = true;
        initialized += 1;
    }
    return out;
}

pub fn Impl(comptime DB: type) type {
    return struct {
        const Self = @This();

        pub fn search(self: *DB, alloc: Allocator, req: types.SearchRequest) !types.SearchResult {
            return try Self.searchWithExecutionContext(self, alloc, req, .{});
        }

        pub fn searchWithExecutionContext(
            self: *DB,
            alloc: Allocator,
            req: types.SearchRequest,
            exec_ctx: types.ExecutionContext,
        ) !types.SearchResult {
            if (req.row_claim != null) {
                return try Self.searchWithRowClaim(self, alloc, req, exec_ctx);
            }
            const bench_profile = benchQueryProfileEnabled();
            const total_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
            var generation_ns: u64 = 0;
            var lock_wait_ns: u64 = 0;
            var locked_search_ns: u64 = 0;
            if (self.searchRuntimeCanUsePublishedDenseSearch(req)) {
                const generation_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
                const snapshot_req = try Self.searchRequestAtCurrentIdentityGeneration(self, req);
                if (bench_profile) generation_ns = platform_time.monotonicNs() - generation_start_ns;
                const search_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
                const result = try Self.searchLockedWithExecutionContext(self, alloc, snapshot_req, exec_ctx);
                if (bench_profile) {
                    locked_search_ns = platform_time.monotonicNs() - search_start_ns;
                    std.log.info(
                        "antfly_bench_db_search_wrapper total_us={d} generation_us={d} lock_wait_us={d} locked_search_us={d} published_dense={}",
                        .{ (platform_time.monotonicNs() - total_start_ns) / 1000, generation_ns / 1000, lock_wait_ns / 1000, locked_search_ns / 1000, true },
                    );
                }
                return result;
            }
            const lock_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
            self.core.lockApplyShared();
            if (bench_profile) lock_wait_ns = platform_time.monotonicNs() - lock_start_ns;
            defer self.core.unlockApplyShared();
            const generation_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
            const snapshot_req = try Self.searchRequestAtCurrentIdentityGeneration(self, req);
            if (bench_profile) generation_ns = platform_time.monotonicNs() - generation_start_ns;
            const search_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
            const result = try Self.searchLockedWithExecutionContext(self, alloc, snapshot_req, exec_ctx);
            if (bench_profile) {
                locked_search_ns = platform_time.monotonicNs() - search_start_ns;
                std.log.info(
                    "antfly_bench_db_search_wrapper total_us={d} generation_us={d} lock_wait_us={d} locked_search_us={d} published_dense={}",
                    .{ (platform_time.monotonicNs() - total_start_ns) / 1000, generation_ns / 1000, lock_wait_ns / 1000, locked_search_ns / 1000, false },
                );
            }
            return result;
        }

        fn searchLocked(self: *DB, alloc: Allocator, req: types.SearchRequest) !types.SearchResult {
            return try Self.searchLockedWithExecutionContext(self, alloc, try Self.searchRequestAtCurrentIdentityGeneration(self, req), .{});
        }

        fn searchWithRowClaim(
            self: *DB,
            alloc: Allocator,
            req: types.SearchRequest,
            exec_ctx: types.ExecutionContext,
        ) anyerror!types.SearchResult {
            const claim = req.row_claim orelse return error.InvalidQueryRequest;
            const txn_id = claim.txn_id orelse return error.InvalidQueryRequest;
            if (!claim.mode.usesDurableIntent()) return error.InvalidQueryRequest;
            if (req.count_only) return error.UnsupportedQueryRequest;
            if (req.return_mode != .parent) return error.UnsupportedQueryRequest;
            if (req.graph_queries.len > 0) return error.UnsupportedQueryRequest;
            if (req.graph_metric_queries.len > 0) return error.UnsupportedQueryRequest;

            var search_req = req;
            search_req.row_claim = null;
            var result = try Self.searchWithExecutionContext(self, alloc, search_req, exec_ctx);
            errdefer result.deinit();
            try self.searchRuntimeApplyRowClaimToSearchResult(&result, txn_id, claim);
            return result;
        }

        fn searchLockedWithExecutionContext(
            self: *DB,
            alloc: Allocator,
            req: types.SearchRequest,
            exec_ctx: types.ExecutionContext,
        ) !types.SearchResult {
            const execution_req = directSingleVectorRequest(req) orelse req;
            if (execution_req.full_text_queries.len > 0 or execution_req.dense_queries.len > 0 or execution_req.sparse_queries.len > 0 or execution_req.merge_config != null) {
                var composed = try Self.searchComposed(self, alloc, execution_req, exec_ctx);
                errdefer composed.deinit();
                try Self.applyGraphMetricRerank(self, &composed, execution_req);
                try db_query_result_shape.externalizeSearchResultArtifactIds(alloc, &composed);
                return composed;
            }

            const has_primary = execution_req.full_text != null or execution_req.dense != null or execution_req.sparse != null or !db_query_search.isDefaultMatchAll(execution_req.query) or (execution_req.graph_queries.len == 0 and execution_req.graph_metric_queries.len == 0);

            var base = if (!has_primary and (execution_req.graph_queries.len > 0 or execution_req.graph_metric_queries.len > 0))
                try db_query_search.emptySearchResult(alloc)
            else if (execution_req.full_text) |text|
                try Self.searchTextQuery(self, alloc, execution_req, text)
            else if (execution_req.dense) |dense|
                try Self.searchDense(self, alloc, execution_req, dense)
            else if (execution_req.sparse) |sparse|
                try Self.searchSparse(self, alloc, execution_req, sparse)
            else switch (execution_req.query) {
                .match_none,
                .match_all,
                .phrase,
                .multi_phrase,
                .term,
                .fuzzy,
                .numeric_range,
                .date_range,
                .doc_id,
                .bool_field,
                .geo_distance,
                .geo_bbox,
                .term_range,
                .ip_range,
                .geo_shape,
                .match,
                .match_phrase,
                .prefix,
                .wildcard,
                .regexp,
                => try Self.searchText(self, alloc, execution_req),
                .dense_knn => |dense| try Self.searchDense(self, alloc, execution_req, dense),
                .sparse_knn => |sparse| try Self.searchSparse(self, alloc, execution_req, sparse),
                .graph => |graph| try Self.searchGraph(self, alloc, execution_req, graph, null),
            };
            errdefer base.deinit();

            if (execution_req.graph_metric_queries.len > 0) {
                base.graph_metric_results = try Self.executeGraphMetricQueries(self, alloc, execution_req.graph_metric_queries);
            }

            try Self.applyGraphMetricRerank(self, &base, execution_req);

            if (execution_req.graph_queries.len == 0) {
                try db_query_result_shape.externalizeSearchResultArtifactIds(alloc, &base);
                return base;
            }

            base.graph_results = try Self.executeGraphQueries(self, alloc, execution_req, execution_req.graph_queries, base.hits, base.total_hits);
            try self.searchRuntimeApplyGraphExpandStrategy(alloc, &base, execution_req.expand_strategy);
            try db_query_result_shape.externalizeSearchResultArtifactIds(alloc, &base);
            return base;
        }

        fn directSingleVectorRequest(req: types.SearchRequest) ?types.SearchRequest {
            if (req.merge_config != null or req.reranker != null or req.pruner != null) return null;
            if (req.full_text_queries.len != 0) return null;
            if (req.full_text) |text| switch (text) {
                .match_all => {},
                else => return null,
            };
            if (!db_query_search.isDefaultMatchAll(req.query)) return null;
            if (req.graph_queries.len != 0 or req.graph_metric_queries.len != 0 or req.expand_strategy != null) return null;
            if (req.dense != null or req.sparse != null) return null;
            if (req.dense_queries.len == 1 and req.sparse_queries.len == 0) {
                var next = req;
                next.index_name = req.dense_queries[0].index_name;
                next.full_text = null;
                next.dense = req.dense_queries[0].query;
                next.dense_queries = &.{};
                return next;
            }
            if (req.sparse_queries.len == 1 and req.dense_queries.len == 0) {
                var next = req;
                next.index_name = req.sparse_queries[0].index_name;
                next.full_text = null;
                next.sparse = req.sparse_queries[0].query;
                next.sparse_queries = &.{};
                return next;
            }
            return null;
        }

        fn executeGraphMetricQueries(
            self: *DB,
            alloc: Allocator,
            queries: []const types.NamedGraphMetricQuery,
        ) ![]types.GraphMetricResult {
            if (queries.len == 0) return &.{};
            const results = try alloc.alloc(types.GraphMetricResult, queries.len);
            var initialized: usize = 0;
            errdefer {
                for (results[0..initialized]) |*result| result.deinit(alloc);
                alloc.free(results);
            }

            for (queries, 0..) |named, i| {
                results[i] = try executeGraphMetricQuery(self, alloc, named);
                initialized += 1;
            }
            return results;
        }

        fn applyGraphMetricRerank(
            self: *DB,
            result: *types.SearchResult,
            req: types.SearchRequest,
        ) !void {
            const rerank = req.graph_metric_rerank orelse return;
            if (req.count_only) return error.UnsupportedQueryRequest;

            const entry = self.core.graphIndex(rerank.index_name) orelse return error.IndexNotFound;
            var status = try entry.index.graphMetricStatus(rerank.metric_name);
            defer status.deinit(entry.index.alloc);

            if (status.published_generation == 0) return error.MetricNotReady;
            if (rerank.freshness == .fresh and status.state != .fresh) return error.MetricStale;

            var result_status = try cloneGraphMetricStatusFromGraph(result.alloc, status);
            var result_status_moved = false;
            errdefer if (!result_status_moved) result_status.deinit(result.alloc);

            for (result.hits) |*hit| {
                const metric_score_opt = try entry.index.graphMetricScore(rerank.metric_name, hit.id);
                const metric_score = metric_score_opt orelse rerank.missing_score;
                const base_score: f64 = if (hit.score) |score| @floatCast(score) else 0.0;
                const final_score = rerank.base_weight * base_score + rerank.weight * metric_score;
                const clamped_final_score = clampF64ToF32(final_score);
                const detail_index_name = try result.alloc.dupe(u8, rerank.index_name);
                errdefer result.alloc.free(detail_index_name);
                const detail_metric_name = try result.alloc.dupe(u8, rerank.metric_name);
                errdefer result.alloc.free(detail_metric_name);
                var score_details = types.GraphMetricRerankScoreDetails{
                    .index_name = detail_index_name,
                    .metric_name = detail_metric_name,
                    .base_score = base_score,
                    .base_weight = rerank.base_weight,
                    .metric_score = metric_score_opt,
                    .metric_score_used = metric_score,
                    .metric_weight = rerank.weight,
                    .missing_score_used = metric_score_opt == null,
                    .final_score = clamped_final_score,
                    .published_generation = status.published_generation,
                };
                var score_details_moved = false;
                errdefer if (!score_details_moved) score_details.deinit(result.alloc);

                if (hit.score_details) |*old_details| old_details.deinit(result.alloc);
                hit.score_details = score_details;
                score_details_moved = true;
                hit.score = clamped_final_score;
            }

            std.mem.sort(types.SearchHit, result.hits, {}, struct {
                fn lessThan(_: void, a: types.SearchHit, b: types.SearchHit) bool {
                    const a_score = a.score orelse 0.0;
                    const b_score = b.score orelse 0.0;
                    if (a_score == b_score) return std.mem.lessThan(u8, a.id, b.id);
                    return a_score > b_score;
                }
            }.lessThan);

            if (result.graph_metric_rerank_status) |*old_status| old_status.deinit(result.alloc);
            result.graph_metric_rerank_status = result_status;
            result_status_moved = true;
        }

        fn clampF64ToF32(value: f64) f32 {
            const max = std.math.floatMax(f32);
            if (value > max) return max;
            if (value < -max) return -max;
            return @floatCast(value);
        }

        fn executeGraphMetricQuery(
            self: *DB,
            alloc: Allocator,
            named: types.NamedGraphMetricQuery,
        ) !types.GraphMetricResult {
            const entry = self.core.graphIndex(named.query.index_name) orelse return error.IndexNotFound;
            var status = try entry.index.graphMetricStatus(named.query.metric_name);
            defer status.deinit(entry.index.alloc);

            if (status.published_generation == 0) return error.MetricNotReady;
            if (named.query.freshness == .fresh and status.state != .fresh) return error.MetricStale;

            const raw_scores = try entry.index.graphMetricTopK(named.query.metric_name, named.query.top_k);
            defer {
                for (raw_scores) |*score| score.deinit(entry.index.alloc);
                if (raw_scores.len > 0) entry.index.alloc.free(raw_scores);
            }

            const scores = try alloc.alloc(types.GraphMetricScore, raw_scores.len);
            var initialized_scores: usize = 0;
            errdefer {
                for (scores[0..initialized_scores]) |*score| score.deinit(alloc);
                alloc.free(scores);
            }
            for (raw_scores, 0..) |score, i| {
                scores[i] = .{
                    .node = try alloc.dupe(u8, score.node),
                    .score = score.score,
                };
                initialized_scores += 1;
            }

            return .{
                .name = try alloc.dupe(u8, named.name),
                .index_name = try alloc.dupe(u8, named.query.index_name),
                .metric_name = try alloc.dupe(u8, named.query.metric_name),
                .scores = scores,
                .status = try cloneGraphMetricStatusFromGraph(alloc, status),
            };
        }

        pub fn searchRequestWithTextAlgebraicDocFilterAlloc(self: *DB, req: types.SearchRequest) !AlgebraicDocFilterRequest {
            const needs_algebraic_doc_filter = req.doc_filter_bindings.len > 0 or req.require_algebraic_filter_resolution;
            return if (needs_algebraic_doc_filter)
                try Self.searchRequestWithAlgebraicDocFilterAlloc(self, req)
            else
                AlgebraicDocFilterRequest{ .req = req };
        }

        pub fn searchRequestWithAlgebraicDocFilterAlloc(self: *DB, req: types.SearchRequest) !AlgebraicDocFilterRequest {
            if (req.filter_query_json.len == 0 and req.exclusion_query_json.len == 0) return .{ .req = req };
            if (!req.require_algebraic_filter_resolution and req.doc_filter_bindings.len == 0) {
                if (try Self.searchRequestWithDynamicStructuredDocFilterAlloc(self, req)) |direct| return direct;
            }
            const entry = self.core.index_manager.algebraicIndex(null) orelse {
                self.searchRuntimeRecordUnsupportedDocSetFilterShape();
                if (req.require_algebraic_filter_resolution) return error.UnsupportedQueryRequest;
                return .{ .req = req };
            };
            entry.index.recordVectorFilterAttempt();
            if (entry.index.hasErrors() or !entry.index.plannerLifecycleReady()) {
                entry.index.recordVectorFilterUnsupported(req.require_algebraic_filter_resolution);
                self.searchRuntimeRecordUnsupportedDocSetFilterShape();
                if (req.require_algebraic_filter_resolution) return error.UnsupportedQueryRequest;
                return .{ .req = req };
            }
            if (try Self.searchRequestWithDirectAlgebraicDocFilterAlloc(self, req, &entry.index)) |direct| return direct;
            var filter_doc_ids: [][]u8 = &.{};
            errdefer entry.index.freeDocIds(filter_doc_ids);
            var exclude_doc_ids: [][]u8 = &.{};
            errdefer entry.index.freeDocIds(exclude_doc_ids);
            var changed = false;
            var filter_json_resolved = false;
            var exclusion_json_resolved = false;
            var filter_supported = req.filter_doc_ids_positive or req.filter_doc_ids.len > 0;
            var filter_bindings = std.ArrayListUnmanaged(AlgebraicIndex.FilterBinding).empty;
            defer {
                for (filter_bindings.items) |*binding| binding.set.deinit(&entry.index);
                filter_bindings.deinit(entry.index.alloc);
            }

            if (filter_supported) filter_doc_ids = try dupeAlgebraicDocIds(entry.index.alloc, req.filter_doc_ids);
            if (req.exclude_doc_ids.len > 0) exclude_doc_ids = try dupeAlgebraicDocIds(entry.index.alloc, req.exclude_doc_ids);

            for (req.doc_filter_bindings) |binding| {
                if (binding.name.len == 0 or binding.filter_query_json.len == 0) return error.InvalidArgument;
                for (filter_bindings.items) |existing| {
                    if (std.mem.eql(u8, existing.name, binding.name)) return error.InvalidArgument;
                }
                var set = (try entry.index.docIdSetForFilterJsonWithBindingsAlloc(
                    self.core.store,
                    binding.filter_query_json,
                    filter_bindings.items,
                )) orelse {
                    entry.index.recordVectorFilterUnsupported(req.require_algebraic_filter_resolution);
                    self.searchRuntimeRecordUnsupportedDocSetFilterShape();
                    if (req.require_algebraic_filter_resolution) return error.UnsupportedQueryRequest;
                    return .{ .req = req };
                };
                errdefer set.deinit(&entry.index);
                try filter_bindings.append(entry.index.alloc, .{
                    .name = binding.name,
                    .set = set,
                });
                set = .{};
            }

            if (req.filter_query_json.len > 0) {
                if (try entry.index.docIdSetForFilterJsonWithBindingsAlloc(self.core.store, req.filter_query_json, filter_bindings.items)) |set| {
                    var owned_set = set;
                    defer owned_set.deinit(&entry.index);
                    if (owned_set.include) |ids| {
                        if (filter_supported) {
                            const intersected = try intersectAlgebraicDocIds(entry.index.alloc, filter_doc_ids, ids);
                            entry.index.freeDocIds(filter_doc_ids);
                            filter_doc_ids = intersected;
                        } else {
                            filter_doc_ids = try dupeAlgebraicDocIds(entry.index.alloc, ids);
                        }
                        filter_supported = true;
                        changed = true;
                    }
                    if (owned_set.exclude.len > 0) {
                        const merged = try unionAlgebraicDocIds(entry.index.alloc, exclude_doc_ids, owned_set.exclude);
                        entry.index.freeDocIds(exclude_doc_ids);
                        exclude_doc_ids = merged;
                        changed = true;
                    }
                    filter_json_resolved = true;
                }
            }
            if (req.exclusion_query_json.len > 0) {
                if (try entry.index.docIdSetForFilterJsonWithBindingsAlloc(self.core.store, req.exclusion_query_json, filter_bindings.items)) |set| {
                    var owned_set = set;
                    defer owned_set.deinit(&entry.index);
                    if (owned_set.include) |ids| {
                        const merged = try unionAlgebraicDocIds(entry.index.alloc, exclude_doc_ids, ids);
                        entry.index.freeDocIds(exclude_doc_ids);
                        exclude_doc_ids = merged;
                        changed = true;
                    }
                    if (owned_set.exclude.len > 0) {
                        const merged = try unionAlgebraicDocIds(entry.index.alloc, exclude_doc_ids, owned_set.exclude);
                        entry.index.freeDocIds(exclude_doc_ids);
                        exclude_doc_ids = merged;
                        changed = true;
                    }
                    exclusion_json_resolved = true;
                }
            }
            if (!changed) {
                entry.index.recordVectorFilterUnsupported(req.require_algebraic_filter_resolution);
                self.searchRuntimeRecordUnsupportedDocSetFilterShape();
                if (req.require_algebraic_filter_resolution) return error.UnsupportedQueryRequest;
                return .{ .req = req };
            }

            var next = req;
            if (filter_supported) {
                next.filter_doc_ids = filter_doc_ids;
                next.filter_doc_ids_positive = true;
            }
            if (exclude_doc_ids.len > 0) next.exclude_doc_ids = exclude_doc_ids;
            if (filter_json_resolved) next.filter_query_json = "";
            if (exclusion_json_resolved) next.exclusion_query_json = "";
            if (filter_bindings.items.len > 0) next.doc_filter_bindings = &.{};
            if (next.require_algebraic_filter_resolution and (next.filter_query_json.len > 0 or next.exclusion_query_json.len > 0)) {
                entry.index.recordVectorFilterUnsupported(true);
                self.searchRuntimeRecordUnsupportedDocSetFilterShape();
                return error.UnsupportedQueryRequest;
            }
            const resolved_filter = try self.alloc.create(doc_set.ResolvedDocFilter);
            errdefer self.alloc.destroy(resolved_filter);
            resolved_filter.* = try self.searchRuntimeResolvedDocFilterForIdsAlloc(filter_supported, filter_doc_ids, exclude_doc_ids, req.identity_read_generation);
            errdefer resolved_filter.deinit(self.alloc);
            next.resolved_doc_filter = resolved_filter;
            entry.index.recordVectorFilterResolved(filter_doc_ids.len, exclude_doc_ids.len);
            return .{
                .req = next,
                .index = &entry.index,
                .filter_doc_ids = filter_doc_ids,
                .exclude_doc_ids = exclude_doc_ids,
                .resolved_doc_filter = resolved_filter,
                .resolved_doc_filter_alloc = self.alloc,
            };
        }

        fn searchRequestWithDynamicStructuredDocFilterAlloc(self: *DB, req: types.SearchRequest) !?AlgebraicDocFilterRequest {
            var resolved = (try db_query_search.resolveStructuredDocFilterForComposedAlloc(self.alloc, req, .{
                .ctx = self,
                .text_index_entry = Self.textIndexEntryCallback,
                .resolve_doc_set_doc_ids = Self.resolveDocSetDocIdsCallback,
                .resolve_doc_ids_to_doc_set = Self.resolveDocIdsToDocSetCallback,
                .resolve_relational_filter_doc_set = Self.resolveRelationalFilterDocSetCallback,
                .live_filter_doc_set = Self.liveFilterDocSetCallback,
                .project_ordinals_to_doc_ids = false,
                .identity_read_generation = req.identity_read_generation,
            })) orelse return null;
            errdefer resolved.deinit(self.alloc);

            const resolved_filter = try self.alloc.create(doc_set.ResolvedDocFilter);
            errdefer self.alloc.destroy(resolved_filter);
            resolved_filter.* = resolved;
            resolved = .{};

            var next = req;
            next.resolved_doc_filter = resolved_filter;
            next.filter_query_json = "";
            next.exclusion_query_json = "";
            return .{
                .req = next,
                .resolved_doc_filter = resolved_filter,
                .resolved_doc_filter_alloc = self.alloc,
            };
        }

        fn searchRequestWithDirectAlgebraicDocFilterAlloc(self: *DB, req: types.SearchRequest, index: *AlgebraicIndex) !?AlgebraicDocFilterRequest {
            const algebraic_filter_rows_are_visible = true;
            var resolved_bindings = std.ArrayListUnmanaged(AlgebraicIndex.ResolvedFilterBinding).empty;
            defer {
                for (resolved_bindings.items) |*binding| binding.filter.deinit(index.alloc);
                resolved_bindings.deinit(index.alloc);
            }
            for (req.doc_filter_bindings) |binding| {
                if (binding.name.len == 0 or binding.filter_query_json.len == 0) return error.InvalidArgument;
                for (resolved_bindings.items) |existing| {
                    if (std.mem.eql(u8, existing.name, binding.name)) return error.InvalidArgument;
                }
                var binding_filter = (if (algebraic_filter_rows_are_visible)
                    try index.resolvedDocFilterForFilterJsonUncheckedAlloc(
                        self.core.store,
                        binding.filter_query_json,
                        resolved_bindings.items,
                    )
                else
                    try index.resolvedDocFilterForFilterJsonWithBindingsAtGenerationAlloc(
                        self.core.store,
                        binding.filter_query_json,
                        req.identity_read_generation,
                        resolved_bindings.items,
                    )) orelse return null;
                errdefer binding_filter.deinit(index.alloc);
                try resolved_bindings.append(index.alloc, .{
                    .name = binding.name,
                    .filter = binding_filter,
                });
                binding_filter = .{};
            }

            var filter = doc_set.ResolvedDocFilter{};
            errdefer filter.deinit(index.alloc);
            var changed = false;
            var request_constraints_resolved = false;
            var filter_json_resolved = false;
            var exclusion_json_resolved = false;

            if (try Self.resolvedDocFilterForRequestNativeConstraintsAlloc(self, index.alloc, req)) |initial| {
                filter = initial;
                changed = true;
                request_constraints_resolved = true;
            }

            if (req.filter_query_json.len > 0) {
                var query_filter = (if (algebraic_filter_rows_are_visible)
                    try index.resolvedDocFilterForFilterJsonUncheckedAlloc(
                        self.core.store,
                        req.filter_query_json,
                        resolved_bindings.items,
                    )
                else if (resolved_bindings.items.len > 0)
                    try index.resolvedDocFilterForFilterJsonWithBindingsAtGenerationAlloc(
                        self.core.store,
                        req.filter_query_json,
                        req.identity_read_generation,
                        resolved_bindings.items,
                    )
                else
                    try index.resolvedDocFilterForFilterJsonAtGenerationAlloc(self.core.store, req.filter_query_json, req.identity_read_generation)) orelse {
                    filter.deinit(index.alloc);
                    return null;
                };
                defer query_filter.deinit(index.alloc);
                if (!(try applyResolvedFilterIncludeAlloc(index.alloc, &filter, &query_filter))) {
                    filter.deinit(index.alloc);
                    return null;
                }
                changed = true;
                filter_json_resolved = true;
            }
            if (req.exclusion_query_json.len > 0) {
                var exclusion = (if (algebraic_filter_rows_are_visible)
                    try index.resolvedDocFilterForFilterJsonUncheckedAlloc(
                        self.core.store,
                        req.exclusion_query_json,
                        resolved_bindings.items,
                    )
                else if (resolved_bindings.items.len > 0)
                    try index.resolvedDocFilterForFilterJsonWithBindingsAtGenerationAlloc(
                        self.core.store,
                        req.exclusion_query_json,
                        req.identity_read_generation,
                        resolved_bindings.items,
                    )
                else
                    try index.resolvedDocFilterForFilterJsonAtGenerationAlloc(self.core.store, req.exclusion_query_json, req.identity_read_generation)) orelse {
                    filter.deinit(index.alloc);
                    return null;
                };
                defer exclusion.deinit(index.alloc);

                if (!(try unionResolvedFilterExcludeAlloc(index.alloc, &filter, &exclusion.include))) {
                    filter.deinit(index.alloc);
                    return null;
                }
                if (!(try unionResolvedFilterExcludeAlloc(index.alloc, &filter, &exclusion.exclude))) {
                    filter.deinit(index.alloc);
                    return null;
                }
                changed = true;
                exclusion_json_resolved = true;
            }
            if (!changed) return null;

            const resolved_filter = try index.alloc.create(doc_set.ResolvedDocFilter);
            errdefer index.alloc.destroy(resolved_filter);
            resolved_filter.* = filter;
            filter = .{};

            var next = req;
            if (request_constraints_resolved) {
                next.filter_doc_ids = &.{};
                next.filter_doc_ids_positive = false;
                next.exclude_doc_ids = &.{};
            }
            if (filter_json_resolved) next.filter_query_json = "";
            if (exclusion_json_resolved) next.exclusion_query_json = "";
            if (resolved_bindings.items.len > 0) next.doc_filter_bindings = &.{};
            next.resolved_doc_filter = resolved_filter;

            index.recordVectorFilterResolved(
                resolvedDocSetStatCount(&resolved_filter.include),
                resolvedDocSetStatCount(&resolved_filter.exclude),
            );
            return .{
                .req = next,
                .resolved_doc_filter = resolved_filter,
                .resolved_doc_filter_alloc = index.alloc,
            };
        }

        pub fn resolvedDocFilterForRequestNativeConstraintsAlloc(
            self: *DB,
            alloc: Allocator,
            req: types.SearchRequest,
        ) !?doc_set.ResolvedDocFilter {
            var filter = doc_set.ResolvedDocFilter{};
            errdefer filter.deinit(alloc);
            var changed = false;

            if (req.resolved_doc_filter) |ptr| {
                const existing: *const doc_set.ResolvedDocFilter = @ptrCast(@alignCast(ptr));
                filter = try Self.normalizeResolvedDocFilterDocKeysNoLockAtGenerationAlloc(self, alloc, existing, req.identity_read_generation);
                changed = true;
            }

            if (req.filter_doc_ids_positive or req.filter_doc_ids.len != 0) {
                var include = if (req.filter_doc_ids.len == 0)
                    doc_set.ResolvedDocSet.none
                else
                    try self.searchRuntimeResolveDocIdsToDocSet(alloc, req.filter_doc_ids, req.identity_read_generation);
                defer include.deinit(alloc);
                if (!(try intersectResolvedFilterIncludeAlloc(alloc, &filter, &include))) {
                    filter.deinit(alloc);
                    return null;
                }
                changed = true;
            }

            if (req.exclude_doc_ids.len != 0) {
                var exclude = try self.searchRuntimeResolveDocIdsToDocSet(alloc, req.exclude_doc_ids, req.identity_read_generation);
                defer exclude.deinit(alloc);
                if (!(try unionResolvedFilterExcludeAlloc(alloc, &filter, &exclude))) {
                    filter.deinit(alloc);
                    return null;
                }
                changed = true;
            }

            if (!changed) return null;
            return filter;
        }

        fn normalizeResolvedDocFilterDocKeysNoLockAtGenerationAlloc(
            self: *DB,
            alloc: Allocator,
            filter: *const doc_set.ResolvedDocFilter,
            generation: ?u64,
        ) !doc_set.ResolvedDocFilter {
            var out = doc_set.ResolvedDocFilter{
                .include = try Self.normalizeResolvedDocSetDocKeysNoLockAtGenerationAlloc(self, alloc, &filter.include, generation),
            };
            errdefer out.deinit(alloc);
            out.exclude = try Self.normalizeResolvedDocSetDocKeysNoLockAtGenerationAlloc(self, alloc, &filter.exclude, generation);
            return out;
        }

        fn normalizeResolvedDocSetDocKeysNoLockAtGenerationAlloc(
            self: *DB,
            alloc: Allocator,
            set: *const doc_set.ResolvedDocSet,
            generation: ?u64,
        ) !doc_set.ResolvedDocSet {
            return switch (set.*) {
                .doc_keys => |keys| try self.searchRuntimeResolveDocIdsToDocSet(alloc, keys, generation),
                else => try doc_set.cloneAlloc(alloc, set),
            };
        }

        fn applyResolvedFilterIncludeAlloc(
            alloc: Allocator,
            target: *doc_set.ResolvedDocFilter,
            include_filter: *const doc_set.ResolvedDocFilter,
        ) !bool {
            var merged_include = (try doc_set.intersectAlloc(alloc, &target.include, &include_filter.include)) orelse return false;
            errdefer merged_include.deinit(alloc);
            var merged_exclude = (try doc_set.unionAlloc(alloc, &target.exclude, &include_filter.exclude)) orelse return false;
            errdefer merged_exclude.deinit(alloc);

            target.include.deinit(alloc);
            target.exclude.deinit(alloc);
            target.include = merged_include;
            target.exclude = merged_exclude;
            return true;
        }

        fn intersectResolvedFilterIncludeAlloc(
            alloc: Allocator,
            target: *doc_set.ResolvedDocFilter,
            include: *const doc_set.ResolvedDocSet,
        ) !bool {
            var merged = (try doc_set.intersectAlloc(alloc, &target.include, include)) orelse return false;
            errdefer merged.deinit(alloc);
            target.include.deinit(alloc);
            target.include = merged;
            return true;
        }

        fn unionResolvedFilterExcludeAlloc(
            alloc: Allocator,
            target: *doc_set.ResolvedDocFilter,
            exclude: *const doc_set.ResolvedDocSet,
        ) !bool {
            var merged = (try doc_set.unionAlloc(alloc, &target.exclude, exclude)) orelse return false;
            errdefer merged.deinit(alloc);
            target.exclude.deinit(alloc);
            target.exclude = merged;
            return true;
        }

        fn resolvedDocSetStatCount(set: *const doc_set.ResolvedDocSet) usize {
            return set.estimatedCardinality() orelse 0;
        }

        fn unionResolvedDocSetsAlloc(
            alloc: Allocator,
            left: *const doc_set.ResolvedDocSet,
            right: *const doc_set.ResolvedDocSet,
        ) !?doc_set.ResolvedDocSet {
            return switch (left.*) {
                .all => .all,
                .none => try doc_set.cloneAlloc(alloc, right),
                .doc_keys => |left_keys| switch (right.*) {
                    .all => .all,
                    .none => try doc_set.cloneAlloc(alloc, left),
                    .doc_keys => |right_keys| .{ .doc_keys = try unionAlgebraicDocIds(alloc, left_keys, right_keys) },
                    .ordinals, .ordinal_bitmap => null,
                },
                .ordinals, .ordinal_bitmap => switch (right.*) {
                    .all => .all,
                    .none => try doc_set.cloneAlloc(alloc, left),
                    .doc_keys => null,
                    .ordinals, .ordinal_bitmap => try unionOrdinalDocSetsAlloc(alloc, left, right),
                },
            };
        }

        fn unionOrdinalDocSetsAlloc(
            alloc: Allocator,
            left: *const doc_set.ResolvedDocSet,
            right: *const doc_set.ResolvedDocSet,
        ) !doc_set.ResolvedDocSet {
            var ordinals = std.ArrayListUnmanaged(doc_set.DocOrdinal).empty;
            defer ordinals.deinit(alloc);
            try appendResolvedDocSetOrdinalsAlloc(alloc, &ordinals, left);
            try appendResolvedDocSetOrdinalsAlloc(alloc, &ordinals, right);
            return try doc_set.fromOrdinalsAlloc(alloc, ordinals.items);
        }

        fn appendResolvedDocSetOrdinalsAlloc(
            alloc: Allocator,
            out: *std.ArrayListUnmanaged(doc_set.DocOrdinal),
            set: *const doc_set.ResolvedDocSet,
        ) !void {
            switch (set.*) {
                .ordinals => |ordinals| try out.appendSlice(alloc, ordinals),
                .ordinal_bitmap => |*bitmap| {
                    var iter = bitmap.iterator();
                    while (iter.next()) |ordinal| try out.append(alloc, ordinal);
                },
                .all, .none, .doc_keys => {},
            }
        }

        fn dupeAlgebraicDocIds(alloc: Allocator, doc_ids: []const []const u8) ![][]u8 {
            var out = try alloc.alloc([]u8, doc_ids.len);
            var initialized: usize = 0;
            errdefer {
                for (out[0..initialized]) |item| alloc.free(item);
                if (out.len > 0) alloc.free(out);
            }
            for (doc_ids, 0..) |doc_id, i| {
                out[i] = try alloc.dupe(u8, doc_id);
                initialized += 1;
            }
            return out;
        }

        fn containsAlgebraicDocId(doc_ids: []const []const u8, candidate: []const u8) bool {
            for (doc_ids) |doc_id| {
                if (std.mem.eql(u8, doc_id, candidate)) return true;
            }
            return false;
        }

        fn intersectAlgebraicDocIds(alloc: Allocator, left: []const []const u8, right: []const []const u8) ![][]u8 {
            var out = std.ArrayListUnmanaged([]u8).empty;
            errdefer {
                for (out.items) |item| alloc.free(item);
                out.deinit(alloc);
            }
            for (left) |doc_id| {
                if (!containsAlgebraicDocId(right, doc_id)) continue;
                try out.append(alloc, try alloc.dupe(u8, doc_id));
            }
            return try out.toOwnedSlice(alloc);
        }

        fn unionAlgebraicDocIds(alloc: Allocator, left: []const []const u8, right: []const []const u8) ![][]u8 {
            var out = std.ArrayListUnmanaged([]u8).empty;
            errdefer {
                for (out.items) |item| alloc.free(item);
                out.deinit(alloc);
            }
            for (left) |doc_id| try out.append(alloc, try alloc.dupe(u8, doc_id));
            for (right) |doc_id| {
                if (containsAlgebraicDocId(out.items, doc_id)) continue;
                try out.append(alloc, try alloc.dupe(u8, doc_id));
            }
            return try out.toOwnedSlice(alloc);
        }

        fn searchComposed(
            self: *DB,
            alloc: Allocator,
            req: types.SearchRequest,
            exec_ctx: types.ExecutionContext,
        ) !types.SearchResult {
            _ = exec_ctx;
            return try db_query_search.searchComposed(alloc, req, .{
                .ctx = self,
                .resolve_structured_doc_filter = Self.resolveStructuredDocFilterForComposedCallback,
                .resolve_structured_text_doc_filter = Self.resolveStructuredTextDocFilterForComposedCallback,
                .search_text_query = Self.searchTextQueryCallback,
                .search_text = Self.searchTextComposedCallback,
                .search_dense = Self.searchDenseComposedCallback,
                .search_sparse = Self.searchSparseComposedCallback,
                .clone_named_set = Self.cloneNamedSetCallback,
                .fuse_named_sets = Self.fuseNamedSetsCallback,
                .resolve_hits_to_doc_set = Self.resolveSearchHitsToDocSetCallback,
                .attach_graph_results = Self.attachGraphResultsCallback,
            });
        }

        fn searchText(self: *DB, alloc: Allocator, req: types.SearchRequest) !types.SearchResult {
            return try db_query_search.searchText(alloc, req, .{
                .ctx = self,
                .func = Self.searchTextQueryCallback,
            });
        }

        fn searchMatchAll(self: *DB, alloc: Allocator, req: types.SearchRequest) !types.SearchResult {
            return try db_query_search.searchMatchAll(alloc, req, .{
                .ctx = self,
                .collect_candidates = Self.collectSearchMatchAllCandidatesCallback,
                .text_index_entry = Self.textIndexEntryCallback,
                .resolve_doc_set_doc_ids = Self.resolveDocSetDocIdsCallback,
                .resolve_doc_ids_to_doc_set = Self.resolveDocIdsToDocSetCallback,
                .resolve_relational_filter_doc_set = Self.resolveRelationalFilterDocSetCallback,
                .live_filter_doc_set = Self.liveFilterDocSetCallback,
                .load_projected_document = Self.loadRequiredProjectedSearchDocumentCallback,
                .load_stored = Self.loadStoredSearchDocumentCallback,
                .load_many_stored = Self.loadStoredSearchDocumentManyCallback,
            });
        }

        fn searchGraph(self: *DB, alloc: Allocator, req: types.SearchRequest, graph_query: graph_query_mod.GraphQuery, base_hits: ?[]const types.SearchHit) !types.SearchResult {
            _ = req.index_name;
            var raw = try db_query_graph.executeSearchGraph(alloc, req, graph_query, base_hits, .{
                .ctx = self,
                .execute_graph_query = Self.executeSearchGraphQueryCallback,
                .load_projected_document = Self.loadProjectedSearchDocumentCallback,
                .lookup_doc_ordinal = Self.lookupLiveDocOrdinalNoLockCallback,
            });
            errdefer raw.deinit();
            try self.searchRuntimeAnnotateSearchHitOrdinalsNoLock(alloc, req, raw.hits);
            return try self.searchRuntimeFilterExpiredSearchResult(alloc, raw);
        }

        fn fuseNamedSets(self: *DB, alloc: Allocator, req: types.SearchRequest, named_sets: []const NamedResultSet) !types.SearchResult {
            const raw = try db_query_graph.fuseNamedSets(alloc, req, named_sets, .{
                .ctx = self,
                .load_projected_document = Self.loadProjectedSearchDocumentCallback,
            });
            return try self.searchRuntimeFilterExpiredSearchResult(alloc, raw);
        }

        fn executeGraphQueries(
            self: *DB,
            alloc: Allocator,
            req: types.SearchRequest,
            graph_queries: []const types.NamedGraphQuery,
            base_hits: []const types.SearchHit,
            base_total_hits: u32,
        ) ![]types.GraphSearchResult {
            return try db_query_graph.executeGraphQueries(alloc, req, graph_queries, base_hits, base_total_hits, .{
                .ctx = self,
                .func = Self.executeSingleGraphQueryWithSetsCallback,
                .resolve_hits_to_doc_set = Self.resolveSearchHitsToDocSetCallback,
                .resolve_nodes_to_doc_set = Self.resolveGraphNodesToDocSetCallback,
            });
        }

        pub fn executeGraphQueriesWithSets(
            self: *DB,
            alloc: Allocator,
            req: types.SearchRequest,
            graph_queries: []const types.NamedGraphQuery,
            named_sets: []const NamedResultSet,
        ) ![]types.GraphSearchResult {
            return try db_query_graph.executeGraphQueriesWithSets(alloc, req, graph_queries, named_sets, .{
                .ctx = self,
                .func = Self.executeSingleGraphQueryWithSetsCallback,
                .resolve_hits_to_doc_set = Self.resolveSearchHitsToDocSetCallback,
                .resolve_nodes_to_doc_set = Self.resolveGraphNodesToDocSetCallback,
            });
        }

        fn executeSingleGraphQueryWithSets(
            self: *DB,
            alloc: Allocator,
            req: types.SearchRequest,
            named: *const types.NamedGraphQuery,
            named_sets: []const NamedResultSet,
        ) !types.GraphSearchResult {
            var result = switch (named.query.query_type) {
                .pattern => try Self.executeSinglePatternQueryWithSets(self, alloc, req, named, named_sets),
                else => try db_query_graph.executeSingleNonPatternQueryWithSets(alloc, req, named, named_sets, .{
                    .ctx = self,
                    .find_shortest_path = Self.executeShortestPathCallback,
                    .find_k_shortest_paths = Self.executeKShortestPathsCallback,
                    .execute_graph_query = Self.executeGraphQueryCallback,
                    .load_projected_document = Self.loadProjectedSearchDocumentCallback,
                    .resolve_doc_set_doc_ids = Self.resolveDocSetDocIdsForGraphCallback,
                    .lookup_doc_ordinal = Self.lookupLiveDocOrdinalNoLockCallback,
                }),
            };
            errdefer result.deinit(alloc);
            try self.searchRuntimeAnnotateSearchHitOrdinalsNoLock(alloc, req, result.hits);
            return result;
        }

        fn executeSinglePatternQueryWithSets(
            self: *DB,
            alloc: Allocator,
            req: types.SearchRequest,
            named: *const types.NamedGraphQuery,
            named_sets: []const NamedResultSet,
        ) !types.GraphSearchResult {
            return try db_query_graph.executeSinglePatternQueryWithSets(alloc, req, named, named_sets, .{
                .ctx = self,
                .match_pattern = Self.executePatternMatchCallback,
                .load_projected_document = Self.loadPatternProjectedDocumentCallback,
                .resolve_doc_set_doc_ids = Self.resolveDocSetDocIdsForGraphCallback,
                .lookup_doc_ordinal = Self.lookupLiveDocOrdinalNoLockCallback,
            });
        }

        fn searchTextQuery(self: *DB, alloc: Allocator, req: types.SearchRequest, text_query: types.TextQuery) !types.SearchResult {
            var algebraic_filter = try self.searchRuntimeSearchRequestWithTextAlgebraicDocFilterAlloc(req);
            defer algebraic_filter.deinit();
            try Self.proveTextQueryAccessPaths(self, algebraic_filter.req.index_name, text_query);
            const metric_name = Self.textQueryMetricIndexName(self, algebraic_filter.req);
            const start_ns = platform_time.monotonicNs();
            defer db_query_metrics.observe(metric_name, .search, platform_time.monotonicNs() -| start_ns);
            return try db_query_search.searchTextQuery(alloc, algebraic_filter.req, text_query, .{
                .ctx = self,
                .text_index_entry = Self.textIndexEntryCallback,
                .text_index_is_chunk_backed = Self.textIndexIsChunkBackedCallback,
                .search_match_all = Self.searchMatchAllCallback,
                .project_stored_search = Self.projectStoredBytesForSearchCallback,
                .load_projected_document = Self.loadProjectedSearchDocumentCallback,
                .resolve_doc_set_doc_ids = Self.resolveDocSetDocIdsCallback,
                .resolve_doc_ids_to_doc_set = Self.resolveDocIdsToDocSetCallback,
                .resolve_relational_filter_doc_set = Self.resolveRelationalFilterDocSetCallback,
                .live_filter_doc_set = Self.liveFilterDocSetCallback,
                .postprocess = Self.postprocessTextSearchResultCallback,
            });
        }

        fn proveTextQueryAccessPaths(self: *DB, index_name: ?[]const u8, text_query: types.TextQuery) !void {
            switch (text_query) {
                .term => |term| try Self.proveTextFieldAccessPath(self, index_name, term.field, null, .slice),
                .match => |match| try Self.proveTextFieldAccessPath(self, index_name, match.field, match.analyzer, .slice),
                .multi_match_bool_prefix => |multi_match| {
                    for (multi_match.fields) |field| try Self.proveMultiMatchBoolPrefixAccessPaths(self, index_name, field.field);
                },
                .prefix => |prefix| try Self.proveTextFieldAccessPath(self, index_name, prefix.field, null, .automaton_select),
                .wildcard => |wildcard| try Self.proveTextFieldAccessPath(self, index_name, wildcard.field, null, .automaton_select),
                .regexp => |regexp| try Self.proveTextFieldAccessPath(self, index_name, regexp.field, null, .automaton_select),
                .fuzzy => |fuzzy| try Self.proveTextFieldAccessPath(self, index_name, fuzzy.field, null, .automaton_select),
                .bool_query => |bool_query| {
                    for (bool_query.must) |child| try Self.proveTextQueryAccessPaths(self, index_name, child);
                    for (bool_query.should) |child| try Self.proveTextQueryAccessPaths(self, index_name, child);
                    for (bool_query.must_not) |child| try Self.proveTextQueryAccessPaths(self, index_name, child);
                },
                else => {},
            }
        }

        fn proveTextFieldAccessPath(self: *DB, index_name: ?[]const u8, field: []const u8, analyzer: ?[]const u8, fragment: algebraic_mod.ir.TensorFragment) !void {
            _ = try self.core.index_manager.planFullTextLexicalAccessPathAlloc(self.alloc, index_name, field, analyzer, fragment) orelse return error.IndexNotFound;
        }

        fn proveOptionalTextFieldAccessPath(self: *DB, index_name: ?[]const u8, field: []const u8, fragment: algebraic_mod.ir.TensorFragment) !bool {
            const plan = try self.core.index_manager.planFullTextLexicalAccessPathAlloc(self.alloc, index_name, field, null, fragment);
            return plan != null;
        }

        fn proveMultiMatchBoolPrefixAccessPaths(self: *DB, index_name: ?[]const u8, field: []const u8) !void {
            if (std.mem.endsWith(u8, field, "._index_prefix")) {
                try Self.proveTextFieldAccessPath(self, index_name, field, null, .slice);
                return;
            }

            try Self.proveTextFieldAccessPath(self, index_name, field, null, .slice);
            try Self.proveTextFieldAccessPath(self, index_name, field, null, .automaton_select);
            if (isSearchAsYouTypeGeneratedFieldName(field)) return;

            const two_gram = try std.fmt.allocPrint(self.alloc, "{s}._2gram", .{field});
            defer self.alloc.free(two_gram);
            const has_two_gram = try Self.proveOptionalTextFieldAccessPath(self, index_name, two_gram, .slice);
            if (has_two_gram) _ = try Self.proveOptionalTextFieldAccessPath(self, index_name, two_gram, .automaton_select);

            const three_gram = try std.fmt.allocPrint(self.alloc, "{s}._3gram", .{field});
            defer self.alloc.free(three_gram);
            const has_three_gram = try Self.proveOptionalTextFieldAccessPath(self, index_name, three_gram, .slice);
            if (has_three_gram) _ = try Self.proveOptionalTextFieldAccessPath(self, index_name, three_gram, .automaton_select);

            const index_prefix = try std.fmt.allocPrint(self.alloc, "{s}._index_prefix", .{field});
            defer self.alloc.free(index_prefix);
            _ = try Self.proveOptionalTextFieldAccessPath(self, index_name, index_prefix, .slice);
        }

        fn isSearchAsYouTypeGeneratedFieldName(field: []const u8) bool {
            return std.mem.endsWith(u8, field, "._2gram") or
                std.mem.endsWith(u8, field, "._3gram") or
                std.mem.endsWith(u8, field, "._index_prefix");
        }

        fn textQueryMetricIndexName(self: *DB, req: types.SearchRequest) ?[]const u8 {
            if (req.index_name) |name| return name;
            const entry = self.core.textIndexEntry(null) orelse return null;
            return entry.config.name;
        }

        fn searchDense(self: *DB, alloc: Allocator, req: types.SearchRequest, dense: types.DenseKnnQuery) !types.SearchResult {
            if (builtin.os.tag == .freestanding) return error.UnsupportedPlatform;
            const metric_name = Self.denseQueryMetricIndexName(self, req);
            const start_ns = platform_time.monotonicNs();
            defer db_query_metrics.observe(metric_name, .vector, platform_time.monotonicNs() -| start_ns);
            const bench_profile = benchQueryProfileEnabled();
            const total_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
            var algebraic_ns: u64 = 0;
            var prove_ns: u64 = 0;
            var inner_ns: u64 = 0;
            const algebraic_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
            var algebraic_filter = try self.searchRuntimeSearchRequestWithAlgebraicDocFilterAlloc(req);
            defer algebraic_filter.deinit();
            if (bench_profile) algebraic_ns = platform_time.monotonicNs() - algebraic_start_ns;
            const prove_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
            try Self.proveVectorSearchAccessPath(self, algebraic_filter.req.index_name, .dense_vector, hasNativeDocIdConstraints(algebraic_filter.req));
            if (bench_profile) prove_ns = platform_time.monotonicNs() - prove_start_ns;
            const inner_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
            const result = try db_query_search.searchDense(alloc, algebraic_filter.req, dense, Self.denseSearchExecutor(self));
            if (bench_profile) {
                inner_ns = platform_time.monotonicNs() - inner_start_ns;
                std.log.info(
                    "antfly_bench_db_dense_wrapper total_us={d} algebraic_us={d} prove_us={d} inner_us={d}",
                    .{ (platform_time.monotonicNs() - total_start_ns) / 1000, algebraic_ns / 1000, prove_ns / 1000, inner_ns / 1000 },
                );
            }
            return result;
        }

        fn searchSparse(self: *DB, alloc: Allocator, req: types.SearchRequest, sparse: types.SparseKnnQuery) !types.SearchResult {
            if (builtin.os.tag == .freestanding) return error.UnsupportedPlatform;
            const metric_name = Self.sparseQueryMetricIndexName(self, req);
            const start_ns = platform_time.monotonicNs();
            defer db_query_metrics.observe(metric_name, .vector, platform_time.monotonicNs() -| start_ns);
            const bench_profile = benchQueryProfileEnabled();
            const total_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
            var algebraic_ns: u64 = 0;
            var prove_ns: u64 = 0;
            var inner_ns: u64 = 0;
            const algebraic_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
            var algebraic_filter = try self.searchRuntimeSearchRequestWithAlgebraicDocFilterAlloc(req);
            defer algebraic_filter.deinit();
            if (bench_profile) algebraic_ns = platform_time.monotonicNs() - algebraic_start_ns;
            const prove_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
            try Self.proveVectorSearchAccessPath(self, algebraic_filter.req.index_name, .sparse_vector, hasNativeDocIdConstraints(algebraic_filter.req));
            if (bench_profile) prove_ns = platform_time.monotonicNs() - prove_start_ns;
            const inner_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
            const result = try db_query_search.searchSparse(alloc, algebraic_filter.req, sparse, Self.sparseSearchExecutor(self));
            if (bench_profile) {
                inner_ns = platform_time.monotonicNs() - inner_start_ns;
                std.log.info(
                    "antfly_bench_db_sparse_wrapper total_us={d} algebraic_us={d} prove_us={d} inner_us={d}",
                    .{ (platform_time.monotonicNs() - total_start_ns) / 1000, algebraic_ns / 1000, prove_ns / 1000, inner_ns / 1000 },
                );
            }
            return result;
        }

        fn searchDenseProfiledAtSnapshot(self: *DB, alloc: Allocator, req: types.SearchRequest, dense: types.DenseKnnQuery) !db_query_search.ProfiledDenseSearchResult {
            if (builtin.os.tag == .freestanding) return error.UnsupportedPlatform;
            var algebraic_filter = try self.searchRuntimeSearchRequestWithAlgebraicDocFilterAlloc(req);
            defer algebraic_filter.deinit();
            try Self.proveVectorSearchAccessPath(self, algebraic_filter.req.index_name, .dense_vector, hasNativeDocIdConstraints(algebraic_filter.req));
            const profiled = db_query_search.searchDenseProfiled(alloc, algebraic_filter.req, dense, Self.denseSearchExecutor(self));
            return profiled catch |err| {
                if (err == error.IndexNotFound) {
                    const index_configs = self.listIndexes(alloc) catch |list_err| {
                        std.log.err("dense profiled search missing index requested={s} list_err={s}", .{
                            req.index_name orelse "<null>",
                            @errorName(list_err),
                        });
                        return err;
                    };
                    defer types.freeIndexConfigs(alloc, index_configs);
                    std.log.err("dense profiled search missing index requested={s} configured_index_count={d}", .{
                        req.index_name orelse "<null>",
                        index_configs.len,
                    });
                    for (index_configs) |cfg| {
                        std.log.err("dense profiled search visible index name={s} kind={s}", .{
                            cfg.name,
                            @tagName(cfg.kind),
                        });
                    }
                }
                return err;
            };
        }

        fn proveVectorSearchAccessPath(self: *DB, index_name: ?[]const u8, layout: algebraic_mod.ir.PhysicalLayout, constrained: bool) !void {
            const selected_path = switch (layout) {
                .dense_vector => self.core.index_manager.denseVectorAccessPath(index_name),
                .sparse_vector => self.core.index_manager.sparseVectorAccessPath(index_name),
                else => null,
            };
            const access_path = selected_path orelse return error.IndexNotFound;
            var planned = (try algebraic_mod.planner.planVectorSearchTensorProgramAlloc(self.alloc, access_path.owner, layout, constrained)) orelse return error.InvalidIndexConfig;
            defer planned.deinit(self.alloc);
            if (planned.access_paths.len != 1 or
                planned.access_paths[0].layout != layout or
                !std.mem.eql(u8, planned.access_paths[0].owner, access_path.owner) or
                !std.mem.eql(algebraic_mod.ir.Dimension, planned.access_paths[0].output_dims, access_path.output_dims))
            {
                return error.InvalidIndexConfig;
            }
            if (!algebraic_mod.ir.vectorSearchProgramMatchesTarget(planned.asProgram(), access_path.owner, layout, constrained)) return error.InvalidIndexConfig;
        }

        fn hasNativeDocIdConstraints(req: types.SearchRequest) bool {
            return req.filter_doc_ids_positive or
                req.filter_doc_ids.len > 0 or
                req.exclude_doc_ids.len > 0 or
                req.resolved_doc_filter != null or
                req.doc_filter_bindings.len > 0;
        }

        fn denseQueryMetricIndexName(self: *DB, req: types.SearchRequest) ?[]const u8 {
            if (req.index_name) |name| return name;
            const entry = self.core.denseIndex(null) orelse return null;
            return entry.config.name;
        }

        fn sparseQueryMetricIndexName(self: *DB, req: types.SearchRequest) ?[]const u8 {
            if (req.index_name) |name| return name;
            const entry = self.core.sparseIndex(null) orelse return null;
            return entry.config.name;
        }

        fn denseSearchExecutor(self: *DB) db_query_search.DenseSearchExecutor {
            return .{
                .ctx = self,
                .text_index_entry = Self.textIndexEntryCallback,
                .dense_index = Self.denseIndexCallback,
                .lookup_doc_key = Self.denseDocKeyCallback,
                .lookup_vector_id = Self.denseVectorIdCallback,
                .lookup_vector_ids_for_ordinals = Self.denseVectorIdsForOrdinalsCallback,
                .all_docs_visible_fast = Self.allDocsVisibleFastCallback,
                .lookup_doc_ordinal = Self.lookupLiveDocOrdinalNoLockCallback,
                .lookup_doc_ordinals = Self.lookupLiveDocOrdinalsNoLockCallback,
                .lookup_doc_ordinals_for_vector_ids = Self.denseOrdinalsForVectorIdsCallback,
                .resolve_doc_set_doc_ids = Self.resolveDocSetDocIdsCallback,
                .resolve_doc_ids_to_doc_set = Self.resolveDocIdsToDocSetCallback,
                .resolve_relational_filter_doc_set = Self.resolveRelationalFilterDocSetCallback,
                .live_filter_doc_set = Self.liveFilterDocSetCallback,
                .load_projected_document = Self.loadRequiredProjectedSearchDocumentCallback,
                .hbc_search = Self.hbcSearchCallback,
                .hbc_search_profiled = Self.hbcSearchProfiledCallback,
                .postprocess = Self.postprocessVectorSearchResultCallback,
            };
        }

        fn sparseSearchExecutor(self: *DB) db_query_search.SparseSearchExecutor {
            return .{
                .ctx = self,
                .text_index_entry = Self.textIndexEntryCallback,
                .sparse_index = Self.sparseIndexCallback,
                .resolve_doc_set_doc_ids = Self.resolveDocSetDocIdsCallback,
                .resolve_doc_ids_to_doc_set = Self.resolveDocIdsToDocSetCallback,
                .resolve_relational_filter_doc_set = Self.resolveRelationalFilterDocSetCallback,
                .live_filter_doc_set = Self.liveFilterDocSetCallback,
                .lookup_doc_nums_for_ordinals = Self.sparseDocNumsForOrdinalsCallback,
                .lookup_doc_ordinal = Self.lookupLiveDocOrdinalNoLockCallback,
                .lookup_doc_ordinals = Self.lookupLiveDocOrdinalsNoLockCallback,
                .load_projected_document = Self.loadRequiredProjectedSearchDocumentCallback,
                .load_projected_documents = Self.loadProjectedSearchDocumentManyCallback,
                .postprocess = Self.postprocessVectorSearchResultCallback,
            };
        }

        pub fn searchRequestAtCurrentIdentityGeneration(self: *DB, req: types.SearchRequest) !types.SearchRequest {
            var snapshot_req = req;
            snapshot_req.identity_read_generation = try self.currentIdentityReadGenerationForRequest(snapshot_req.identity_read_generation);
            try Self.validateResolvedDocFilterWireContext(self, snapshot_req);
            return snapshot_req;
        }

        fn validateResolvedDocFilterWireContext(self: *DB, req: types.SearchRequest) !void {
            const ctx = req.resolved_doc_filter_wire_context orelse return;
            if (req.resolved_doc_filter == null) return error.InvalidQueryRequest;
            if (!ctx.namespace.eql(self.core.identity_namespace)) return error.DocIdentityNamespaceMismatch;
            if (req.identity_read_generation == null or req.identity_read_generation.? != ctx.identity_read_generation) {
                self.doc_set_planning_stats.recordStaleIdentityGenerationRejection();
                return error.UnsupportedQueryRequest;
            }
        }

        pub fn collectSearchRequestTextStats(self: *DB, alloc: Allocator, req: types.SearchRequest) ![]const distributed_stats_mod.TextFieldStats {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();
            return try db_query_search.collectSearchRequestTextStats(alloc, try Self.searchRequestAtCurrentIdentityGeneration(self, req), .{
                .ctx = self,
                .text_index_entry = Self.textIndexEntryCallback,
            });
        }

        pub fn preflightSearchRequest(self: *DB, alloc: Allocator, req: types.SearchRequest, max_work: u32) !db_query_search.RuntimePreflightSummary {
            return try Self.preflightSearchRequestWithExecutionContext(self, alloc, req, max_work, .{});
        }

        pub fn preflightSearchRequestWithExecutionContext(
            self: *DB,
            alloc: Allocator,
            req: types.SearchRequest,
            max_work: u32,
            exec_ctx: types.ExecutionContext,
        ) !db_query_search.RuntimePreflightSummary {
            return try Self.collectPlanningStatsWithExecutionContext(self, alloc, req, max_work, exec_ctx);
        }

        pub fn collectPlanningStats(
            self: *DB,
            alloc: Allocator,
            req: types.SearchRequest,
            max_work: u32,
        ) !planning_stats_mod.PlanningStatsSummary {
            return try Self.collectPlanningStatsWithExecutionContext(self, alloc, req, max_work, .{});
        }

        pub fn collectPlanningStatsWithExecutionContext(
            self: *DB,
            alloc: Allocator,
            req: types.SearchRequest,
            max_work: u32,
            exec_ctx: types.ExecutionContext,
        ) !planning_stats_mod.PlanningStatsSummary {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();
            return try Self.collectPlanningStatsLocked(self, alloc, try Self.searchRequestAtCurrentIdentityGeneration(self, req), max_work, exec_ctx);
        }

        fn collectPlanningStatsLocked(
            self: *DB,
            alloc: Allocator,
            req: types.SearchRequest,
            max_work: u32,
            exec_ctx: types.ExecutionContext,
        ) !planning_stats_mod.PlanningStatsSummary {
            _ = exec_ctx;
            try planning_bindings_mod.validateSearchRequestBindings(&self.core, self.alloc, req);
            return try planning_adapter_mod.collectSearchRequestStatsAlloc(
                alloc,
                &self.core,
                self,
                planningStatsSearchRequestCallback,
                req,
                max_work,
            );
        }

        pub fn planningStatsProvider(self: *DB) planning_stats_mod.PlanningStatsProvider {
            return planning_stats_mod.PlanningStatsProvider.init(self, planningStatsProviderCollectSearchRequestStats);
        }

        fn planningStatsProviderCollectSearchRequestStats(
            ptr: *anyopaque,
            alloc: Allocator,
            req: types.SearchRequest,
            max_work: u32,
        ) !planning_stats_mod.PlanningStatsSummary {
            const self: *DB = @ptrCast(@alignCast(ptr));
            return try Self.collectPlanningStats(self, alloc, req, max_work);
        }

        fn planningStatsSearchRequestCallback(
            ptr: *anyopaque,
            alloc: Allocator,
            req: types.SearchRequest,
        ) !types.SearchResult {
            const self: *DB = @ptrCast(@alignCast(ptr));
            return try Self.searchLocked(self, alloc, req);
        }

        pub fn collectExplicitTextStats(self: *DB, alloc: Allocator, requests: []const db_query_search.ExplicitTextStatRequest) ![]const distributed_stats_mod.TextFieldStats {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();
            return try db_query_search.collectExplicitTextStats(alloc, requests, .{
                .ctx = self,
                .text_index_entry = Self.textIndexEntryCallback,
            });
        }

        pub fn collectExplicitBackgroundTextStats(
            self: *DB,
            alloc: Allocator,
            requests: []const db_query_search.ExplicitBackgroundTextStatRequest,
        ) ![]const aggregations_mod.DistributedBackgroundTextStats {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();
            return try db_query_search.collectExplicitBackgroundTextStats(alloc, requests, .{
                .ctx = self,
                .text_index_entry = Self.textIndexEntryCallback,
            });
        }

        pub fn searchDenseProfiled(self: *DB, alloc: Allocator, req: types.SearchRequest, dense: types.DenseKnnQuery) !db_query_search.ProfiledDenseSearchResult {
            if (self.searchRuntimeCanUsePublishedDenseSearch(req)) {
                return try Self.searchDenseProfiledAtSnapshot(self, alloc, try Self.searchRequestAtCurrentIdentityGeneration(self, req), dense);
            }
            {
                self.core.lockApplyShared();
                defer self.core.unlockApplyShared();
                return try Self.searchDenseProfiledAtSnapshot(self, alloc, try Self.searchRequestAtCurrentIdentityGeneration(self, req), dense);
            }
        }

        fn textIndexEntryCallback(
            ctx: ?*anyopaque,
            index_name: ?[]const u8,
        ) anyerror!?*index_manager_mod.IndexManager.TextIndex {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return self.core.textIndexEntry(index_name);
        }

        fn resolveStructuredDocFilterForComposedCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            req: types.SearchRequest,
        ) anyerror!?doc_set.ResolvedDocFilter {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try db_query_search.resolveStructuredDocFilterForComposedAlloc(alloc, req, .{
                .ctx = self,
                .text_index_entry = Self.textIndexEntryCallback,
                .resolve_doc_set_doc_ids = Self.resolveDocSetDocIdsCallback,
                .resolve_doc_ids_to_doc_set = Self.resolveDocIdsToDocSetCallback,
                .resolve_relational_filter_doc_set = Self.resolveRelationalFilterDocSetCallback,
                .live_filter_doc_set = Self.liveFilterDocSetCallback,
                .project_ordinals_to_doc_ids = false,
                .identity_read_generation = req.identity_read_generation,
            });
        }

        fn resolveStructuredTextDocFilterForComposedCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            req: types.SearchRequest,
        ) anyerror!?db_query_search.ResolvedTextDocNumFilter {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try db_query_search.resolveStructuredTextDocNumFilterForComposedAlloc(alloc, req, .{
                .ctx = self,
                .text_index_entry = Self.textIndexEntryCallback,
                .resolve_doc_set_doc_ids = Self.resolveDocSetDocIdsCallback,
                .resolve_doc_ids_to_doc_set = Self.resolveDocIdsToDocSetCallback,
                .resolve_relational_filter_doc_set = Self.resolveRelationalFilterDocSetCallback,
                .live_filter_doc_set = Self.liveFilterDocSetCallback,
                .all_docs_visible = Self.allDocsVisibleCallback,
                .project_ordinals_to_doc_ids = false,
                .identity_read_generation = req.identity_read_generation,
            });
        }

        fn searchTextQueryCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            req: types.SearchRequest,
            text_query: types.TextQuery,
        ) anyerror!types.SearchResult {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try Self.searchTextQuery(self, alloc, req, text_query);
        }

        fn searchTextComposedCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            req: types.SearchRequest,
        ) anyerror!types.SearchResult {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try Self.searchText(self, alloc, req);
        }

        fn searchDenseComposedCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            req: types.SearchRequest,
            dense: types.DenseKnnQuery,
        ) anyerror!types.SearchResult {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try Self.searchDense(self, alloc, req, dense);
        }

        fn searchSparseComposedCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            req: types.SearchRequest,
            sparse: types.SparseKnnQuery,
        ) anyerror!types.SearchResult {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try Self.searchSparse(self, alloc, req, sparse);
        }

        fn cloneNamedSetCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            set: NamedResultSet,
            include_stored: bool,
        ) anyerror!types.SearchResult {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try self.searchRuntimeCloneNamedSetAsResult(alloc, set, include_stored);
        }

        fn fuseNamedSetsCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            req: types.SearchRequest,
            named_sets: []const NamedResultSet,
        ) anyerror!types.SearchResult {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try Self.fuseNamedSets(self, alloc, req, named_sets);
        }

        fn resolveSearchHitsToDocSetCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            req: types.SearchRequest,
            hits: []const types.SearchHit,
        ) anyerror!doc_set.ResolvedDocSet {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try self.searchRuntimeResolveSearchHitsToDocSet(alloc, req, hits);
        }

        fn attachGraphResultsCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            req: types.SearchRequest,
            base: *types.SearchResult,
            named_sets: []const NamedResultSet,
        ) anyerror!void {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            base.graph_results = try Self.executeGraphQueriesWithSets(self, alloc, req, req.graph_queries, named_sets);
            try self.searchRuntimeApplyGraphExpandStrategy(alloc, base, req.expand_strategy);
        }

        fn executeSingleGraphQueryWithSetsCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            req: types.SearchRequest,
            named: *const types.NamedGraphQuery,
            named_sets: []const NamedResultSet,
        ) anyerror!types.GraphSearchResult {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try Self.executeSingleGraphQueryWithSets(self, alloc, req, named, named_sets);
        }

        fn resolveDocSetDocIdsCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            set: *const doc_set.ResolvedDocSet,
            generation: ?u64,
        ) anyerror!?[]const []const u8 {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try self.searchRuntimeResolveDocSetDocIds(alloc, set, generation);
        }

        fn resolveDocSetDocIdsForGraphCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            set: *const doc_set.ResolvedDocSet,
            generation: ?u64,
        ) anyerror!?[][]u8 {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            const ids = (try self.searchRuntimeResolveDocSetDocIds(alloc, set, generation)) orelse return null;
            defer alloc.free(@constCast(ids));
            errdefer for (ids) |id| alloc.free(@constCast(id));

            const out = try alloc.alloc([]u8, ids.len);
            for (ids, 0..) |id, i| out[i] = @constCast(id);
            return out;
        }

        fn resolveGraphNodesToDocSetCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            req: types.SearchRequest,
            nodes: []const graph_query_mod.GraphResultNode,
        ) anyerror!doc_set.ResolvedDocSet {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try self.searchRuntimeResolveGraphNodesToDocSet(alloc, req, nodes);
        }

        fn resolveDocIdsToDocSetCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            doc_ids: []const []const u8,
            generation: ?u64,
        ) anyerror!doc_set.ResolvedDocSet {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try self.searchRuntimeResolveDocIdsToDocSet(alloc, doc_ids, generation);
        }

        fn resolveRelationalFilterDocSetCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            runtime_schema: schema_mod.TableSchema,
            query: search_mod.SearchQuery,
            generation: ?u64,
        ) anyerror!?doc_set.ResolvedDocSet {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try self.searchRuntimeResolveRelationalFilterDocSet(alloc, runtime_schema, query, generation);
        }

        fn liveFilterDocSetCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            set: *const doc_set.ResolvedDocSet,
            generation: ?u64,
        ) anyerror!doc_set.ResolvedDocSet {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try self.searchRuntimeLiveFilterDocSet(alloc, set, generation);
        }

        fn allDocsVisibleCallback(
            ctx: ?*anyopaque,
            generation: ?u64,
        ) anyerror!bool {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try self.searchRuntimeAllDocsVisible(generation);
        }

        fn textIndexIsChunkBackedCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            index_name: ?[]const u8,
        ) anyerror!bool {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try self.searchRuntimeTextIndexIsChunkBacked(alloc, index_name);
        }

        fn searchMatchAllCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            req: types.SearchRequest,
        ) anyerror!types.SearchResult {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try Self.searchMatchAll(self, alloc, req);
        }

        fn projectStoredBytesForSearchCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            req: types.SearchRequest,
            doc_key: []const u8,
            raw: []const u8,
        ) anyerror![]u8 {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try self.searchRuntimeProjectStoredBytesForSearch(alloc, req, doc_key, raw);
        }

        fn loadProjectedSearchDocumentCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            req: types.SearchRequest,
            key: []const u8,
        ) anyerror!?[]u8 {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try self.searchRuntimeLoadProjectedSearchDocument(alloc, req, key);
        }

        fn postprocessTextSearchResultCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            req: types.SearchRequest,
            raw: types.SearchResult,
            chunk_backed: bool,
        ) anyerror!types.SearchResult {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try self.searchRuntimePostprocessTextSearchResult(alloc, req, raw, chunk_backed);
        }

        fn denseIndexCallback(
            ctx: ?*anyopaque,
            index_name: ?[]const u8,
        ) anyerror!?*index_manager_mod.IndexManager.DenseIndex {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return self.searchRuntimeDenseIndex(index_name);
        }

        fn sparseIndexCallback(
            ctx: ?*anyopaque,
            index_name: ?[]const u8,
        ) anyerror!?*index_manager_mod.IndexManager.SparseIndex {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return self.searchRuntimeSparseIndex(index_name);
        }

        fn denseDocKeyCallback(
            ctx: ?*anyopaque,
            index_name: []const u8,
            vector_id: u64,
        ) anyerror!?[]u8 {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try self.searchRuntimeDenseDocKey(index_name, vector_id);
        }

        fn denseVectorIdCallback(
            ctx: ?*anyopaque,
            index_name: []const u8,
            doc_key: []const u8,
        ) anyerror!?u64 {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try self.searchRuntimeDenseVectorId(index_name, doc_key);
        }

        fn denseVectorIdsForOrdinalsCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            index_name: []const u8,
            ordinals: []const u32,
        ) anyerror![]u64 {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try self.searchRuntimeDenseVectorIdsForOrdinals(alloc, index_name, ordinals);
        }

        fn allDocsVisibleFastCallback(
            ctx: ?*anyopaque,
            generation: ?u64,
        ) anyerror!bool {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try self.searchRuntimeAllDocsVisibleFast(generation);
        }

        fn lookupLiveDocOrdinalNoLockCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            doc_id: []const u8,
            generation: ?u64,
        ) anyerror!?doc_set.DocOrdinal {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try self.searchRuntimeLookupLiveDocOrdinalNoLock(alloc, doc_id, generation);
        }

        fn lookupLiveDocOrdinalsNoLockCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            doc_ids: []const []const u8,
            generation: ?u64,
        ) anyerror![]?doc_set.DocOrdinal {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try self.searchRuntimeLookupLiveDocOrdinalsNoLock(alloc, doc_ids, generation);
        }

        fn denseOrdinalsForVectorIdsCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            index_name: []const u8,
            vector_ids: []const u64,
            generation: ?u64,
        ) anyerror![]?doc_set.DocOrdinal {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try self.searchRuntimeDenseOrdinalsForVectorIds(alloc, index_name, vector_ids, generation);
        }

        fn sparseDocNumsForOrdinalsCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            index_name: []const u8,
            ordinals: []const u32,
        ) anyerror![]const u32 {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try self.searchRuntimeSparseDocNumsForOrdinals(alloc, index_name, ordinals);
        }

        fn loadRequiredProjectedSearchDocumentCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            req: types.SearchRequest,
            key: []const u8,
        ) anyerror![]u8 {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try self.searchRuntimeLoadRequiredProjectedSearchDocument(alloc, req, key);
        }

        fn loadProjectedSearchDocumentManyCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            req: types.SearchRequest,
            keys: []const []const u8,
        ) anyerror![]?[]u8 {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try self.searchRuntimeLoadProjectedSearchDocumentMany(alloc, req, keys);
        }

        fn postprocessVectorSearchResultCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            req: types.SearchRequest,
            raw: types.SearchResult,
            chunk_backed: bool,
        ) anyerror!types.SearchResult {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try self.searchRuntimePostprocessVectorSearchResult(alloc, req, raw, chunk_backed);
        }

        fn hbcSearchCallback(
            ctx: ?*anyopaque,
            entry: *index_manager_mod.IndexManager.DenseIndex,
            req: vectorindex_mod.SearchRequest,
        ) anyerror!vectorindex_mod.SearchResults {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try self.searchRuntimeHbcSearch(entry, req);
        }

        fn hbcSearchProfiledCallback(
            ctx: ?*anyopaque,
            entry: *index_manager_mod.IndexManager.DenseIndex,
            req: vectorindex_mod.SearchRequest,
        ) anyerror!vectorindex_mod.ProfiledSearchResults {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try self.searchRuntimeHbcSearchProfiled(entry, req);
        }

        fn collectSearchMatchAllCandidatesCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            req: types.SearchRequest,
        ) anyerror!db_query_search.MatchAllCandidates {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try db_query_search.collectMatchAllCandidates(alloc, req, .{
                .ctx = self,
                .relational_base_rows = self.searchRuntimeHasRelationalBaseRows(),
                .scan_store_range = Self.scanStoreRangeCallback,
                .is_expired_key = Self.isExpiredDocumentKeyCallback,
                .lookup_doc_ordinal = Self.lookupLiveDocOrdinalCallback,
            });
        }

        fn scanStoreRangeCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            lower: []const u8,
            upper: []const u8,
        ) anyerror![]docstore_mod.OwnedKVPair {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try self.searchRuntimeScanStoreRange(alloc, lower, upper);
        }

        fn isExpiredDocumentKeyCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            key: []const u8,
        ) anyerror!bool {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try self.searchRuntimeIsExpiredDocumentKey(alloc, key);
        }

        fn lookupLiveDocOrdinalCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            doc_id: []const u8,
            generation: ?u64,
        ) anyerror!?doc_set.DocOrdinal {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try self.searchRuntimeLookupLiveDocOrdinal(alloc, doc_id, generation);
        }

        fn loadStoredSearchDocumentCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            key: []const u8,
        ) anyerror!?[]u8 {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try self.searchRuntimeLoadStoredSearchDocument(alloc, key);
        }

        fn loadStoredSearchDocumentManyCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            keys: []const []const u8,
        ) anyerror![]?[]u8 {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try self.searchRuntimeLoadStoredSearchDocumentMany(alloc, keys);
        }

        fn executePatternMatchCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            named: *const types.NamedGraphQuery,
            start_key_refs: []const []const u8,
        ) anyerror![]graph_pattern_mod.PatternMatch {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try self.searchRuntimeMatchPattern(alloc, named, start_key_refs);
        }

        fn loadPatternProjectedDocumentCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            query: graph_query_mod.GraphQuery,
            key: []const u8,
        ) anyerror!?[]u8 {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try self.searchRuntimeLoadPatternProjectedDocument(alloc, query, key);
        }

        fn executeShortestPathCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            named: *const types.NamedGraphQuery,
            source: []const u8,
            target: []const u8,
        ) anyerror!?types.GraphPath {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try self.searchRuntimeFindShortestPath(alloc, named, source, target);
        }

        fn executeKShortestPathsCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            named: *const types.NamedGraphQuery,
            source: []const u8,
            target: []const u8,
        ) anyerror![]types.GraphPath {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try self.searchRuntimeFindKShortestPaths(alloc, named, source, target);
        }

        fn executeGraphQueryCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            named: *const types.NamedGraphQuery,
            start_key_refs: []const []const u8,
            target_keys: [][]u8,
        ) anyerror!graph_query_mod.GraphQueryResult {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try self.searchRuntimeExecuteSearchGraphQuery(alloc, named.query, start_key_refs, target_keys);
        }

        fn executeSearchGraphQueryCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            graph_query: graph_query_mod.GraphQuery,
            start_key_refs: []const []const u8,
            target_keys: [][]u8,
        ) anyerror!graph_query_mod.GraphQueryResult {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try self.searchRuntimeExecuteSearchGraphQuery(alloc, graph_query, start_key_refs, target_keys);
        }
    };
}
