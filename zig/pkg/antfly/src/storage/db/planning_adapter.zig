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
const db_core = @import("core.zig");
const planning_collectors = @import("planning_collectors.zig");
const planning_stats = @import("planning_stats.zig");
const types = @import("types.zig");

pub const SearchRequestFn = *const fn (
    ctx: *anyopaque,
    alloc: Allocator,
    req: types.SearchRequest,
) anyerror!types.SearchResult;

const CollectorContext = struct {
    core: *db_core.DBCore,
    search_ctx: *anyopaque,
    search_request_fn: SearchRequestFn,

    fn collector(self: *@This()) planning_stats.PlanningStatsCollector {
        return .{
            .ptr = self,
            .vtable = &.{
                .resolve_text_index_estimate = resolveTextIndexEstimate,
                .resolve_dense_index_estimate = resolveDenseIndexEstimate,
                .resolve_sparse_index_estimate = resolveSparseIndexEstimate,
                .resolve_graph_index_estimate = resolveGraphIndexEstimate,
                .collect_text_query_stats = collectTextQueryStats,
                .append_dense_query_cost = appendDenseQueryCost,
                .estimate_structured_filter_sample = estimateStructuredFilterSample,
                .search_request = searchRequest,
            },
        };
    }
};

pub fn collectSearchRequestStatsAlloc(
    alloc: Allocator,
    core: *db_core.DBCore,
    search_ctx: *anyopaque,
    search_request_fn: SearchRequestFn,
    req: types.SearchRequest,
    max_work: u32,
) !planning_stats.PlanningStatsSummary {
    var ctx = CollectorContext{
        .core = core,
        .search_ctx = search_ctx,
        .search_request_fn = search_request_fn,
    };
    return try planning_stats.collectSearchRequestStatsAlloc(alloc, ctx.collector(), req, max_work);
}

fn resolveTextIndexEstimate(
    ptr: *anyopaque,
    alloc: Allocator,
    index_name: ?[]const u8,
    req: types.SearchRequest,
) !?@import("query/search_exec.zig").TextIndexEstimate {
    const ctx: *CollectorContext = @ptrCast(@alignCast(ptr));
    return try planning_collectors.resolveTextIndexEstimate(ctx.core, alloc, index_name, req);
}

fn resolveDenseIndexEstimate(
    ptr: *anyopaque,
    alloc: Allocator,
    index_name: ?[]const u8,
) !?@import("query/search_exec.zig").EmbeddingIndexEstimate {
    const ctx: *CollectorContext = @ptrCast(@alignCast(ptr));
    return try planning_collectors.resolveDenseIndexEstimate(ctx.core, alloc, index_name);
}

fn resolveSparseIndexEstimate(
    ptr: *anyopaque,
    alloc: Allocator,
    index_name: ?[]const u8,
) !?@import("query/search_exec.zig").EmbeddingIndexEstimate {
    const ctx: *CollectorContext = @ptrCast(@alignCast(ptr));
    return try planning_collectors.resolveSparseIndexEstimate(ctx.core, alloc, index_name);
}

fn resolveGraphIndexEstimate(
    ptr: *anyopaque,
    alloc: Allocator,
    index_name: []const u8,
) !?@import("query/search_exec.zig").GraphIndexEstimate {
    const ctx: *CollectorContext = @ptrCast(@alignCast(ptr));
    return try planning_collectors.resolveGraphIndexEstimate(ctx.core, alloc, index_name);
}

fn collectTextQueryStats(
    ptr: *anyopaque,
    alloc: Allocator,
    req: types.SearchRequest,
) ![]const @import("../../search/distributed_stats.zig").TextFieldStats {
    const ctx: *CollectorContext = @ptrCast(@alignCast(ptr));
    return try planning_collectors.collectTextQueryStats(ctx.core, alloc, req);
}

fn appendDenseQueryCost(
    ptr: *anyopaque,
    summary: *planning_stats.PlanningStatsSummary,
    req: types.SearchRequest,
    index_name: ?[]const u8,
    dense: types.DenseKnnQuery,
) !void {
    const ctx: *CollectorContext = @ptrCast(@alignCast(ptr));
    try planning_collectors.appendDenseQueryCost(ctx.core, summary, req, index_name, dense);
}

fn estimateStructuredFilterSample(
    ptr: *anyopaque,
    alloc: Allocator,
    req: types.SearchRequest,
    sample_size: u32,
    corpus_doc_count: u64,
) !?planning_stats.PlanningStatsCollector.StructuredFilterSampleEstimate {
    const ctx: *CollectorContext = @ptrCast(@alignCast(ptr));
    return try planning_collectors.estimateStructuredFilterSample(ctx.core, alloc, req, sample_size, corpus_doc_count);
}

fn searchRequest(
    ptr: *anyopaque,
    alloc: Allocator,
    req: types.SearchRequest,
) !types.SearchResult {
    const ctx: *CollectorContext = @ptrCast(@alignCast(ptr));
    return try ctx.search_request_fn(ctx.search_ctx, alloc, req);
}
