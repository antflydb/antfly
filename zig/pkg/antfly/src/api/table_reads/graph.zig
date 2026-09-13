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

const graph_query_mod = @import("../../graph/query.zig");
const db_mod = @import("../../storage/db/control_root.zig");

pub const GraphMetricFanInShardRequest = struct {
    req: db_mod.types.SearchRequest,
    graph_queries: []db_mod.types.NamedGraphQuery = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.graph_queries.len > 0) alloc.free(self.graph_queries);
        self.* = undefined;
    }
};

pub fn graphSearchQueryNeedsInternalMetricStatus(query: graph_query_mod.GraphQuery) bool {
    return query.metrics.len > 0 or query.order_by.len > 0 or query.where_metric.len > 0;
}

pub fn rejectNonGlobalGraphMetricFanout(group_count: usize, req: db_mod.types.SearchRequest) !void {
    if (group_count <= 1) return;
    if (req.graph_metric_queries.len > 0 or req.graph_metric_rerank != null) {
        return error.GraphMetricGlobalMaterializationRequired;
    }
    for (req.graph_queries) |named| {
        if (graphSearchQueryNeedsInternalMetricStatus(named.query)) {
            // Shard-local PageRank/eigenvector/HITS scores are normalized over
            // different graphs and their numeric generations are not a global
            // snapshot identity. Merging them would return plausible but
            // mathematically invalid results. Fail closed until a coordinator
            // supplies one globally materialized metric snapshot.
            return error.GraphMetricGlobalMaterializationRequired;
        }
    }
}

pub fn prepareGraphMetricFanInShardRequest(
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
) !GraphMetricFanInShardRequest {
    var needs_graph_query_status = false;
    for (req.graph_queries) |query| {
        if (!query.query.include_metric_status and graphSearchQueryNeedsInternalMetricStatus(query.query)) {
            needs_graph_query_status = true;
            break;
        }
    }
    const needs_rerank_status = req.graph_metric_rerank != null and !req.profile;
    if (!needs_graph_query_status and !needs_rerank_status) return .{ .req = req };

    const graph_queries = if (needs_graph_query_status)
        try alloc.alloc(db_mod.types.NamedGraphQuery, req.graph_queries.len)
    else
        @constCast((&[_]db_mod.types.NamedGraphQuery{})[0..]);
    if (needs_graph_query_status) {
        @memcpy(graph_queries, req.graph_queries);
        for (graph_queries) |*query| {
            if (graphSearchQueryNeedsInternalMetricStatus(query.query)) query.query.include_metric_status = true;
        }
    }

    var out = req;
    if (needs_graph_query_status) out.graph_queries = graph_queries;
    // The public response keeps metric maintenance state in the optional
    // profile. Shards must return it so the coordinator can validate a rerank
    // generation before merging, while the caller's public request remains
    // unchanged.
    if (needs_rerank_status) out.profile = true;
    return .{ .req = out, .graph_queries = graph_queries };
}

pub const consumer_tests = consumerTests();
fn consumerTests() type {
    if (!@import("builtin").is_test) return struct {};
    const test_owner_root = @import("antfly_source_root");
    if (@hasDecl(test_owner_root, "implementation_tests_only") and test_owner_root.implementation_tests_only) return struct {};
    const Suite = struct {
        test "multi-shard reads fail closed for shard-local graph metric scores" {
            const metric_req = db_mod.types.SearchRequest{
                .graph_metric_queries = &.{.{
                    .name = "central",
                    .query = .{ .index_name = "graph_idx", .metric_name = "pagerank" },
                }},
            };
            try rejectNonGlobalGraphMetricFanout(1, metric_req);
            try std.testing.expectError(error.GraphMetricGlobalMaterializationRequired, rejectNonGlobalGraphMetricFanout(2, metric_req));

            const traversal_only = db_mod.types.SearchRequest{ .graph_queries = &.{.{
                .name = "neighbors",
                .query = .{ .query_type = .neighbors, .index_name = "graph_idx", .start_nodes = .{ .keys = &.{"doc-a"} } },
            }} };
            try rejectNonGlobalGraphMetricFanout(2, traversal_only);
        }
    };
    return Suite;
}
comptime {
    if (@import("builtin").is_test) _ = consumer_tests;
}
