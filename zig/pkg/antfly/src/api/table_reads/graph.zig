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
const metadata_mod = @import("../../metadata/domain.zig");
const raft_mod = @import("../../raft/mod.zig");
const db_mod = @import("../../storage/db/mod.zig");
const distributed_graph = @import("../distributed_graph.zig");
const table_catalog = @import("../table_catalog.zig");

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

pub fn graphHydrateRequestHasResolvedDocFilter(req: distributed_graph.GraphHydrateRequest) bool {
    return req.resolved_doc_filter != null;
}

pub fn requiresDistributedGraphCoordinator(
    group_count: usize,
    req: db_mod.types.SearchRequest,
) bool {
    return distributed_graph.supportsCrossRange(req) and
        (group_count > 1 or req.graph_table_read_authorizer != null);
}

pub fn validateGraphHydrateResolvedDocFilterForDb(req: distributed_graph.GraphHydrateRequest, db: *db_mod.DB) !void {
    if (!graphHydrateRequestHasResolvedDocFilter(req)) return;
    const ctx = req.resolved_doc_filter_wire_context orelse return error.UnsupportedQueryRequest;
    if (!ctx.namespace.eql(db.core.identity_namespace)) return error.DocIdentityNamespaceMismatch;
    const generation = try db.currentIdentityReadGenerationForRequest(req.identity_read_generation);
    if (generation != ctx.identity_read_generation) return error.IdentityReadGenerationChanged;
}

pub fn graphHydrateSearchRequest(req: distributed_graph.GraphHydrateRequest) db_mod.types.SearchRequest {
    return .{
        .query = .{ .match_all = {} },
        .filter_query_json = req.filter_query_json,
        .exclusion_query_json = req.exclusion_query_json,
        .include_stored = req.include_stored,
        .resolved_doc_filter = req.resolved_doc_filter,
        .resolved_doc_filter_wire_context = req.resolved_doc_filter_wire_context,
        .identity_read_generation = req.identity_read_generation,
        .execution_deadline_ns = distributed_graph.executionDeadlineFromTimeoutMs(req.timeout_ms),
        .cancellation = req.cancellation,
    };
}

fn prepareGraphSearchConsistency(
    reads: raft_mod.FeatureDBReads,
    req: db_mod.types.SearchRequest,
    consistency: raft_mod.ReadConsistency,
    fallback_to_stale_on_not_leader: bool,
) !void {
    reads.reads.prepareSearchWithConsistency(reads.group_id, req, consistency) catch |err| switch (err) {
        error.NotLeader => {
            if (!fallback_to_stale_on_not_leader or consistency == .stale) return err;
            try reads.reads.prepareSearchWithConsistency(reads.group_id, req, .stale);
        },
        else => return err,
    };
}

pub fn graphHydrateOnPreparedDb(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    req: distributed_graph.GraphHydrateRequest,
    search_req: db_mod.types.SearchRequest,
) !distributed_graph.GraphHydrateResponse {
    if (req.incoming_index_name.len > 0) {
        if (!req.incoming_index_identity.valid()) return error.IndexGenerationMismatch;
        const actual = db.core.index_manager.coverageIdentityForIndex(req.incoming_index_name) orelse
            return error.IndexGenerationMismatch;
        if (actual.generation != req.incoming_index_identity.incarnation or
            actual.config_fingerprint == null or
            actual.config_fingerprint.? != req.incoming_index_identity.config_hash)
        {
            return error.IndexGenerationMismatch;
        }
    }
    const hits = if (req.include_hits)
        try db.graphHydrateKeysForInternalRead(alloc, search_req, req.keys)
    else
        @constCast((&[_]db_mod.types.SearchHit{})[0..]);
    errdefer {
        for (hits) |*hit| hit.deinit(alloc);
        if (hits.len > 0) alloc.free(hits);
    }
    return .{
        .hits = hits,
        .has_incoming = if (req.incoming_index_name.len > 0)
            try db.graphHasIncomingEdgesForInternalRead(alloc, req.incoming_index_name, req.keys)
        else
            @constCast((&[_]bool{})[0..]),
        .incoming_index_identity = req.incoming_index_identity,
    };
}

pub fn graphHydrateOnOpenDb(
    alloc: std.mem.Allocator,
    reads: raft_mod.FeatureDBReads,
    db: *db_mod.DB,
    req: distributed_graph.GraphHydrateRequest,
    consistency: raft_mod.ReadConsistency,
    fallback_to_stale_on_not_leader: bool,
) !distributed_graph.GraphHydrateResponse {
    const search_req = graphHydrateSearchRequest(req);
    try prepareGraphSearchConsistency(reads, search_req, consistency, fallback_to_stale_on_not_leader);
    return try graphHydrateOnPreparedDb(alloc, db, req, search_req);
}

pub const OpenProvisionedQueryDbFn = fn (
    alloc: std.mem.Allocator,
    path: []const u8,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    group_id: u64,
    lsm_root_generation: u64,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
) anyerror!db_mod.DB;

pub fn graphGetEdgesLocal(
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    catalog: table_catalog.CatalogSource,
    requester: raft_mod.ReadableLeaseRequester,
    group_id: u64,
    lsm_root_generation: u64,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    table_name: []const u8,
    req: distributed_graph.GraphEdgesRequest,
    consistency: raft_mod.ReadConsistency,
    comptime open_db: OpenProvisionedQueryDbFn,
) anyerror!distributed_graph.GraphEdgesResponse {
    try table_catalog.validateTopologyEpoch(alloc, catalog, table_name, req.topology_epoch);
    try distributed_graph.validateGraphEdgesTensorAccessPath(alloc, req);

    const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
    defer alloc.free(path);
    var db = try open_db(alloc, path, catalog, table_name, group_id, lsm_root_generation, backend_runtime);
    defer db.close();
    _ = try db.currentIdentityReadGenerationForRequest(req.identity_read_generation);

    const reads = raft_mod.FeatureDBReads.init(group_id, requester);
    try reads.reads.prepareLookupWithConsistency(group_id, req.key, .{}, consistency);

    const graph_entry = db.core.graphIndex(req.index_name) orelse return error.IndexNotFound;
    return .{ .edges = try graph_entry.index.getEdgesByTypes(alloc, req.key, req.edge_types, req.direction) };
}

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
