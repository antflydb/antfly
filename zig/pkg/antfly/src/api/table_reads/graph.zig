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
const metadata_api = @import("../../metadata/api.zig");
const metadata_mod = @import("../../metadata/mod.zig");
const metadata_table_manager = @import("../../metadata/table_manager.zig");
const metadata_transition_state = @import("../../metadata/transition_state.zig");
const raft_mod = @import("../../raft/mod.zig");
const raft_reconciler = @import("../../raft/reconciler.zig");
const db_mod = @import("../../storage/db/mod.zig");
const platform_time = @import("antfly_platform").time;
const distributed_graph = @import("../distributed_graph.zig");
const table_catalog = @import("../../metadata/catalog/routing.zig");
const tables_api = @import("../../metadata/catalog/table_ddl.zig");
const table_read_cache = @import("cache.zig");

const algebraic_ir = db_mod.algebraic.ir;
const algebraic_law = db_mod.algebraic.law;

fn uniqueTestTmpPathAlloc(alloc: std.mem.Allocator, prefix: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "/tmp/{s}-{d}", .{ prefix, platform_time.monotonicNs() });
}

pub const GraphMetricFanInShardRequest = struct {
    req: db_mod.types.SearchRequest,
    graph_queries: []db_mod.types.NamedGraphQuery = &.{},

    pub fn deinit(self: *GraphMetricFanInShardRequest, alloc: std.mem.Allocator) void {
        if (self.graph_queries.len > 0) alloc.free(self.graph_queries);
        self.* = undefined;
    }
};

pub fn graphSearchQueryNeedsInternalMetricStatus(query: graph_query_mod.GraphQuery) bool {
    return query.metrics.len > 0 or query.order_by.len > 0 or query.where_metric.len > 0;
}

pub fn searchRequestNeedsInternalGraphMetricStatus(req: db_mod.types.SearchRequest) bool {
    for (req.graph_queries) |query| {
        if (!query.query.include_metric_status and graphSearchQueryNeedsInternalMetricStatus(query.query)) return true;
    }
    return false;
}

pub fn prepareGraphMetricFanInShardRequest(
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
) !GraphMetricFanInShardRequest {
    if (!searchRequestNeedsInternalGraphMetricStatus(req)) {
        return .{ .req = req };
    }

    const graph_queries = try alloc.alloc(db_mod.types.NamedGraphQuery, req.graph_queries.len);
    @memcpy(graph_queries, req.graph_queries);
    for (graph_queries) |*query| {
        if (graphSearchQueryNeedsInternalMetricStatus(query.query)) {
            query.query.include_metric_status = true;
        }
    }

    var out = req;
    out.graph_queries = graph_queries;
    return .{
        .req = out,
        .graph_queries = graph_queries,
    };
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

pub fn graphHydrateOnOpenDb(
    alloc: std.mem.Allocator,
    reads: raft_mod.FeatureDBReads,
    db: *db_mod.DB,
    req: distributed_graph.GraphHydrateRequest,
    consistency: raft_mod.ReadConsistency,
    fallback_to_stale_on_not_leader: bool,
) !distributed_graph.GraphHydrateResponse {
    const search_req = graphHydrateSearchRequest(req);
    reads.reads.prepareSearchWithConsistency(reads.group_id, search_req, consistency) catch |err| switch (err) {
        error.NotLeader => {
            if (!fallback_to_stale_on_not_leader or consistency == .stale) return err;
            try reads.reads.prepareSearchWithConsistency(reads.group_id, search_req, .stale);
        },
        else => return err,
    };
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
            try db.graphHasIncomingEdgesForInternalRead(
                alloc,
                req.incoming_index_name,
                req.keys,
            )
        else
            @constCast((&[_]bool{})[0..]),
    };
}

pub fn graphExpandWithSearch(
    comptime Context: type,
    alloc: std.mem.Allocator,
    table_name: []const u8,
    req: distributed_graph.GraphExpandRequest,
    context: Context,
    comptime search: fn (Context, std.mem.Allocator, db_mod.types.SearchRequest) anyerror!db_mod.types.SearchResult,
) !distributed_graph.GraphExpandResponse {
    const expansions = try alloc.alloc(distributed_graph.GraphExpansion, req.frontier.len);
    var initialized: usize = 0;
    errdefer {
        for (expansions[0..initialized]) |*expansion| expansion.deinit(alloc);
        alloc.free(expansions);
    }
    for (req.frontier, 0..) |item, i| {
        const search_req = try distributed_graph.frontierItemToSearchRequest(alloc, req, item);
        defer distributed_graph.freeExpandSearchRequest(alloc, search_req);

        expansions[i] = .{
            .frontier_id = item.id,
            .frontier_key = try alloc.dupe(u8, item.key),
            .graph_result = graph_result_blk: {
                var result = try search(context, alloc, search_req);
                defer result.deinit();
                var graph_result = if (result.graph_results.len > 0)
                    try distributed_graph.filterGraphSearchResult(alloc, table_name, result.graph_results[0], req.exclude_nodes, req.exclude_edges)
                else
                    try distributed_graph.emptyGraphSearchResult(alloc, req.name);
                for (graph_result.hits) |*hit| hit.deinit(alloc);
                if (graph_result.hits.len > 0) alloc.free(graph_result.hits);
                graph_result.hits = @constCast((&[_]db_mod.types.SearchHit{})[0..]);
                break :graph_result_blk graph_result;
            },
        };
        initialized += 1;
    }

    return .{ .expansions = expansions };
}

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
) anyerror!distributed_graph.GraphEdgesResponse {
    try table_catalog.validateTopologyEpoch(alloc, catalog, table_name, req.topology_epoch);
    try distributed_graph.validateGraphEdgesTensorAccessPath(alloc, req);

    const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
    defer alloc.free(path);
    var db = try table_read_cache.openProvisionedQueryDbForTableWithRuntime(alloc, path, catalog, table_name, group_id, lsm_root_generation, backend_runtime);
    defer db.close();
    _ = try db.currentIdentityReadGenerationForRequest(req.identity_read_generation);

    const reads = raft_mod.FeatureDBReads.init(group_id, requester);
    try reads.reads.prepareLookupWithConsistency(group_id, req.key, .{}, consistency);

    const graph_entry = db.core.graphIndex(req.index_name) orelse return error.IndexNotFound;
    const edges = try graph_entry.index.getEdges(alloc, req.key, "", req.direction);
    return .{ .edges = edges };
}

test "graph metric fan-in shard request carries internal status without mutating public request" {
    const alloc = std.testing.allocator;
    const graph_queries = [_]db_mod.types.NamedGraphQuery{
        .{
            .name = "ranked",
            .query = .{
                .query_type = .neighbors,
                .index_name = "graph_idx",
                .start_nodes = .{ .keys = &.{"doc:a"} },
                .order_by = &.{.{
                    .name = "pagerank",
                    .direction = .desc,
                    .freshness = .published,
                }},
                .include_metric_status = false,
            },
        },
        .{
            .name = "plain",
            .query = .{
                .query_type = .neighbors,
                .index_name = "graph_idx",
                .start_nodes = .{ .keys = &.{"doc:a"} },
                .include_metric_status = false,
            },
        },
    };
    const req = db_mod.types.SearchRequest{
        .query = .{ .match_all = {} },
        .graph_queries = &graph_queries,
    };

    var shard_req = try prepareGraphMetricFanInShardRequest(alloc, req);
    defer shard_req.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), shard_req.req.graph_queries.len);
    try std.testing.expect(shard_req.req.graph_queries.ptr != req.graph_queries.ptr);
    try std.testing.expect(shard_req.req.graph_queries[0].query.include_metric_status);
    try std.testing.expect(!shard_req.req.graph_queries[1].query.include_metric_status);
    try std.testing.expect(!req.graph_queries[0].query.include_metric_status);
    try std.testing.expect(!req.graph_queries[1].query.include_metric_status);
}

test "graph edge local read rejects stale identity generation" {
    const alloc = std.testing.allocator;
    const root = try uniqueTestTmpPathAlloc(alloc, "antfly-api-graph-edge-stale-identity-generation");
    defer alloc.free(root);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), root) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), root) catch {};

    const CatalogState = struct {
        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .description = "docs table",
                    .schema_json = "",
                    .read_schema_json = "",
                    .indexes_json = tables_api.default_indexes_json,
                    .replication_sources_json = "[]",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .range_id = 7107,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const path = algebraic_ir.graphEdgeAccessPath("graph_v1");
    var req = distributed_graph.GraphEdgesRequest{
        .index_name = try alloc.dupe(u8, "graph_v1"),
        .key = try alloc.dupe(u8, "doc:a"),
        .direction = .out,
        .identity_read_generation = 999,
        .tensor_access_path = .{
            .owner = try alloc.dupe(u8, path.owner),
            .layout = path.layout,
            .fragments = try alloc.dupe(algebraic_ir.TensorFragment, path.fragments),
            .output_dims = try alloc.dupe(algebraic_ir.Dimension, path.output_dims),
            .law_ids = try alloc.dupe(algebraic_law.Id, path.law_ids),
        },
        .tensor_program = try distributed_graph.graphEdgesTensorProgramEnvelopeAlloc(alloc, "graph_v1"),
    };
    defer req.deinit(alloc);

    var catalog_state = CatalogState{};
    try std.testing.expectError(error.IdentityReadGenerationChanged, graphGetEdgesLocal(
        alloc,
        root,
        catalog_state.iface(),
        raft_mod.read_gate.noopReadableLeaseRequester(),
        7001,
        0,
        null,
        "docs",
        req,
        .stale,
    ));
}

test "graph edge local read rejects stale identity namespace" {
    const alloc = std.testing.allocator;
    const root = try uniqueTestTmpPathAlloc(alloc, "antfly-api-graph-edge-stale-identity-namespace");
    defer alloc.free(root);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), root) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), root) catch {};

    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, root, 7001);
    defer alloc.free(db_path);
    {
        var db = try db_mod.DB.open(alloc, db_path, .{
            .start_index_workers = false,
            .identity_namespace = .{
                .table_id = 7,
                .shard_id = 7001,
                .range_id = 7196,
            },
        });
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"name\":\"alpha\"}" }},
        });
        db.close();
    }

    const CatalogState = struct {
        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .description = "docs table",
                    .schema_json = "",
                    .read_schema_json = "",
                    .indexes_json = tables_api.default_indexes_json,
                    .replication_sources_json = "[]",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .range_id = 7107,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const path = algebraic_ir.graphEdgeAccessPath("graph_v1");
    var req = distributed_graph.GraphEdgesRequest{
        .index_name = try alloc.dupe(u8, "graph_v1"),
        .key = try alloc.dupe(u8, "doc:a"),
        .direction = .out,
        .identity_read_generation = null,
        .tensor_access_path = .{
            .owner = try alloc.dupe(u8, path.owner),
            .layout = path.layout,
            .fragments = try alloc.dupe(algebraic_ir.TensorFragment, path.fragments),
            .output_dims = try alloc.dupe(algebraic_ir.Dimension, path.output_dims),
            .law_ids = try alloc.dupe(algebraic_law.Id, path.law_ids),
        },
        .tensor_program = try distributed_graph.graphEdgesTensorProgramEnvelopeAlloc(alloc, "graph_v1"),
    };
    defer req.deinit(alloc);

    var catalog_state = CatalogState{};
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, graphGetEdgesLocal(
        alloc,
        root,
        catalog_state.iface(),
        raft_mod.read_gate.noopReadableLeaseRequester(),
        7001,
        0,
        null,
        "docs",
        req,
        .stale,
    ));
}
