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
const metadata_api = @import("../metadata/api.zig");
const managed_embedder = @import("../inference/managed_embedder.zig");
const db_mod = @import("../storage/db/mod.zig");
const metadata_openapi = @import("antfly_metadata_openapi");
const distributed_graph = @import("distributed_graph.zig");
const http_route_helpers = @import("http_route_helpers.zig");
const query_api = @import("query.zig");
const query_contract = @import("query_contract.zig");
const table_reads = @import("table_reads.zig");
const tables_api = @import("tables.zig");
const http_common = @import("../raft/transport/http_common.zig");
const routes = @import("http_routes.zig");

const QueryPreflightRequestWire = struct {
    query_request: std.json.Value,
    max_work: u32 = 0,
};

fn parseQueryPreflightRequest(
    alloc: std.mem.Allocator,
    body: []const u8,
) !struct { query_request_body: []u8, max_work: u32 } {
    var parsed = std.json.parseFromSlice(QueryPreflightRequestWire, alloc, body, .{ .allocate = .alloc_always }) catch {
        return .{
            .query_request_body = try alloc.dupe(u8, body),
            .max_work = 0,
        };
    };
    defer parsed.deinit();
    return .{
        .query_request_body = try std.json.Stringify.valueAlloc(alloc, parsed.value.query_request, .{}),
        .max_work = parsed.value.max_work,
    };
}

pub const CatalogSource = struct {
    ptr: *anyopaque,
    admin_snapshot: ?*const fn (ptr: *anyopaque) anyerror!metadata_api.AdminSnapshot = null,
    free_admin_snapshot: ?*const fn (ptr: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void = null,

    fn adminSnapshot(self: CatalogSource) !?metadata_api.AdminSnapshot {
        const fn_ptr = self.admin_snapshot orelse return null;
        return try fn_ptr(self.ptr);
    }

    fn freeAdminSnapshot(self: CatalogSource, snapshot: *metadata_api.AdminSnapshot) void {
        const fn_ptr = self.free_admin_snapshot orelse return;
        fn_ptr(self.ptr, snapshot);
    }
};

pub const QueryRouter = struct {
    ptr: *anyopaque,
    route_query_to_read_schema: *const fn (ptr: *anyopaque, table_name: []const u8, req: *db_mod.types.SearchRequest) anyerror!void,

    fn route(self: QueryRouter, table_name: []const u8, req: *db_mod.types.SearchRequest) !void {
        return try self.route_query_to_read_schema(self.ptr, table_name, req);
    }
};

pub const Context = struct {
    alloc: std.mem.Allocator,
    reads: ?table_reads.TableReadSource,
    catalog: CatalogSource,
    query_router: QueryRouter,
};

pub const QueryPlanningContext = struct {
    ptr: *anyopaque,
    admin_snapshot: *const fn (ptr: *anyopaque) anyerror!metadata_api.AdminSnapshot,
    free_admin_snapshot: *const fn (ptr: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void,
    local_termite_provider: ?managed_embedder.LocalTermiteProvider = null,

    fn adminSnapshot(self: QueryPlanningContext) !metadata_api.AdminSnapshot {
        return try self.admin_snapshot(self.ptr);
    }

    fn freeAdminSnapshot(self: QueryPlanningContext, snapshot: *metadata_api.AdminSnapshot) void {
        self.free_admin_snapshot(self.ptr, snapshot);
    }
};

pub fn planSemanticQuery(
    planning: QueryPlanningContext,
    alloc: std.mem.Allocator,
    table_name: []const u8,
    index_name: []const u8,
    semantic_search: []const u8,
    embedding_template: ?[]const u8,
    limit: u32,
) !db_mod.types.DenseKnnQuery {
    var snapshot = try planning.adminSnapshot();
    defer planning.freeAdminSnapshot(&snapshot);

    const table = tables_api.findTableByName(&snapshot, table_name) orelse return error.TableNotFound;
    var runtime = try managed_embedder.ManagedEmbedder.initFromIndexesJsonWithLocalTermite(alloc, table.indexes_json, planning.local_termite_provider);
    defer runtime.deinit();

    return .{
        .vector = if (embedding_template) |value|
            try runtime.embedQueryWithTemplate(alloc, index_name, semantic_search, value)
        else
            try runtime.embedQuery(alloc, index_name, semantic_search),
        .k = limit,
    };
}

pub fn resolveDenseQuery(
    planning: QueryPlanningContext,
    alloc: std.mem.Allocator,
    table_name: []const u8,
    index_name: []const u8,
    semantic_search: []const u8,
    embedding_template: ?[]const u8,
    limit: u32,
) !db_mod.types.DenseKnnQuery {
    return try planSemanticQuery(
        planning,
        alloc,
        table_name,
        index_name,
        semantic_search,
        embedding_template,
        limit,
    );
}

const SemanticStatusResolver = struct {
    catalog: CatalogSource,

    fn iface(self: *SemanticStatusResolver) query_contract.SemanticResolver {
        return .{
            .ptr = self,
            .vtable = &.{
                .resolve_dense_query = resolveDenseQueryImpl,
            },
        };
    }

    fn resolveDenseQueryImpl(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
        semantic_search: []const u8,
        embedding_template: ?[]const u8,
        limit: u32,
    ) !db_mod.types.DenseKnnQuery {
        const self: *SemanticStatusResolver = @ptrCast(@alignCast(ptr));
        return try planSemanticQuery(.{
            .ptr = self.catalog.ptr,
            .admin_snapshot = self.catalog.admin_snapshot orelse return error.UnsupportedQueryRequest,
            .free_admin_snapshot = self.catalog.free_admin_snapshot orelse return error.UnsupportedQueryRequest,
            .local_termite_provider = null,
        }, alloc, table_name, index_name, semantic_search, embedding_template, limit);
    }
};

pub fn handle(ctx: Context, req: http_common.HttpRequest, path: []const u8, query: []const u8) !?http_common.HttpResponse {
    const alloc = ctx.alloc;
    const source = ctx.reads;
    if (req.method == .GET) {
        if (routes.Routes.matchGroupLookup(path)) |lookup| {
            const reads = source orelse return try http_route_helpers.textResponse(alloc, 404, "not found");
            const decoded_key = try http_route_helpers.decodePercentEncodedPathComponentAlloc(alloc, lookup.key);
            defer alloc.free(decoded_key);
            var lookup_opts = try http_route_helpers.parseLookupOptions(alloc, query);
            defer lookup_opts.deinit(alloc);

            var result = (try reads.lookupGroupLocal(
                alloc,
                lookup.group_id,
                lookup.table_name,
                decoded_key,
                lookup_opts.opts,
                .read_index,
            )) orelse return try http_route_helpers.textResponse(alloc, 404, "not found");
            defer result.deinit(alloc);
            return try http_route_helpers.jsonWithHeadersResponse(alloc, 200, result.json, &.{
                .{
                    .name = "X-Antfly-Version",
                    .value = try std.fmt.allocPrint(alloc, "{d}", .{result.version}),
                },
            });
        }
    }

    if (req.method == .POST) {
        if (routes.Routes.matchGroupScan(path)) |scan| {
            const reads = source orelse return try http_route_helpers.textResponse(alloc, 404, "not found");
            var scan_req = try http_route_helpers.parseScanKeysRequest(alloc, req.body);
            defer scan_req.deinit(alloc);

            var result = (try reads.scanGroupLocal(
                alloc,
                scan.group_id,
                scan.table_name,
                scan_req.from,
                scan_req.to,
                scan_req.opts,
                .read_index,
            )) orelse return try http_route_helpers.textResponse(alloc, 404, "not found");
            defer result.deinit(alloc);
            return try http_route_helpers.ndjsonResponse(alloc, 200, result.ndjson);
        }
        if (routes.Routes.matchGroupGraphExpand(path)) |graph_expand_route| {
            const reads = source orelse return try http_route_helpers.textResponse(alloc, 404, "not found");
            var expand_req = distributed_graph.parseGraphExpandRequest(alloc, req.body) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return try http_route_helpers.textResponse(alloc, 400, "invalid graph expand request"),
            };
            defer expand_req.deinit(alloc);

            var result = (reads.graphExpandGroupLocal(
                alloc,
                graph_expand_route.group_id,
                graph_expand_route.table_name,
                expand_req,
                .read_index,
            ) catch |err| switch (err) {
                error.TopologyChanged => return try http_route_helpers.textResponse(alloc, 409, "topology changed"),
                error.UnknownGroup, error.TableNotFound => return try http_route_helpers.textResponse(alloc, 404, "not found"),
                else => return err,
            }) orelse return try http_route_helpers.textResponse(alloc, 404, "not found");
            defer result.deinit(alloc);
            return try http_route_helpers.jsonResponse(alloc, result);
        }
        if (routes.Routes.matchGroupQuery(path)) |query_route| {
            const reads = source orelse return try http_route_helpers.textResponse(alloc, 404, "not found");
            var semantic_resolver = SemanticStatusResolver{ .catalog = ctx.catalog };
            var query_req = query_api.parseQueryRequest(alloc, semantic_resolver.iface(), query_route.table_name, req.body) catch |err| switch (err) {
                error.InvalidQueryRequest, error.UnsupportedQueryRequest => return try http_route_helpers.textResponse(alloc, 400, @errorName(err)),
                else => return err,
            };
            defer query_req.deinit(alloc);
            ctx.query_router.route(query_route.table_name, &query_req.req) catch |err| switch (err) {
                error.TableNotFound => return try http_route_helpers.textResponse(alloc, 404, @errorName(err)),
                error.InvalidSchemaUpdateRequest, error.InvalidTableIndexMetadata => return try http_route_helpers.textResponse(alloc, 500, "invalid table metadata"),
                else => return err,
            };

            var result = (reads.queryGroupLocal(
                alloc,
                query_route.group_id,
                query_route.table_name,
                query_req.req,
                .read_index,
            ) catch |err| switch (err) {
                error.InvalidQueryRequest, error.UnsupportedQueryRequest, error.InvalidArgument, error.IndexNotFound => return try http_route_helpers.textResponse(alloc, 400, @errorName(err)),
                error.UnknownGroup, error.TableNotFound => return try http_route_helpers.textResponse(alloc, 404, @errorName(err)),
                else => return err,
            }) orelse return try http_route_helpers.textResponse(alloc, 404, "not found");
            defer result.deinit(alloc);

            var arena_impl = std.heap.ArenaAllocator.init(alloc);
            defer arena_impl.deinit();
            const response = try std.json.parseFromSliceLeaky(metadata_openapi.QueryResponses, arena_impl.allocator(), result.json, .{
                .allocate = .alloc_always,
            });
            return try http_route_helpers.jsonResponse(alloc, response);
        }
        if (routes.Routes.matchGroupVectorWorker(path)) |vector_route| {
            const reads = source orelse return try http_route_helpers.textResponse(alloc, 404, "not found");
            var envelope = query_contract.parseAlgebraicVectorWorkerRequestEnvelopeAlloc(alloc, req.body) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return try http_route_helpers.textResponse(alloc, 400, "invalid vector worker request"),
            };
            defer envelope.deinit(alloc);
            const vector_req = table_reads.searchRequestFromVectorWorkerEnvelope(&envelope);

            var result = (reads.queryGroupLocal(
                alloc,
                vector_route.group_id,
                vector_route.table_name,
                vector_req,
                .read_index,
            ) catch |err| switch (err) {
                error.InvalidQueryRequest, error.UnsupportedQueryRequest, error.InvalidArgument, error.IndexNotFound => return try http_route_helpers.textResponse(alloc, 400, @errorName(err)),
                error.UnknownGroup, error.TableNotFound => return try http_route_helpers.textResponse(alloc, 404, @errorName(err)),
                else => return err,
            }) orelse return try http_route_helpers.textResponse(alloc, 404, "not found");
            defer result.deinit(alloc);

            var arena_impl = std.heap.ArenaAllocator.init(alloc);
            defer arena_impl.deinit();
            const response = try std.json.parseFromSliceLeaky(metadata_openapi.QueryResponses, arena_impl.allocator(), result.json, .{
                .allocate = .alloc_always,
            });
            return try http_route_helpers.jsonResponse(alloc, response);
        }
        if (routes.Routes.matchGroupQueryPreflight(path)) |query_route| {
            const reads = source orelse return try http_route_helpers.textResponse(alloc, 404, "not found");
            const preflight_req = try parseQueryPreflightRequest(alloc, req.body);
            defer alloc.free(preflight_req.query_request_body);
            var semantic_resolver = SemanticStatusResolver{ .catalog = ctx.catalog };
            var query_req = query_api.parseQueryRequest(alloc, semantic_resolver.iface(), query_route.table_name, preflight_req.query_request_body) catch |err| switch (err) {
                error.InvalidQueryRequest, error.UnsupportedQueryRequest => return try http_route_helpers.textResponse(alloc, 400, @errorName(err)),
                else => return err,
            };
            defer query_req.deinit(alloc);
            ctx.query_router.route(query_route.table_name, &query_req.req) catch |err| switch (err) {
                error.TableNotFound => return try http_route_helpers.textResponse(alloc, 404, @errorName(err)),
                error.InvalidSchemaUpdateRequest, error.InvalidTableIndexMetadata => return try http_route_helpers.textResponse(alloc, 500, "invalid table metadata"),
                else => return err,
            };

            var summary = (reads.preflightQueryGroupLocal(
                alloc,
                query_route.group_id,
                query_route.table_name,
                query_req.req,
                .read_index,
                preflight_req.max_work,
            ) catch |err| switch (err) {
                error.InvalidQueryRequest, error.UnsupportedQueryRequest, error.InvalidArgument, error.IndexNotFound => return try http_route_helpers.textResponse(alloc, 400, @errorName(err)),
                error.UnknownGroup, error.TableNotFound => return try http_route_helpers.textResponse(alloc, 404, @errorName(err)),
                error.TopologyChanged => return try http_route_helpers.textResponse(alloc, 409, @errorName(err)),
                else => return err,
            }) orelse return try http_route_helpers.textResponse(alloc, 404, "not found");
            defer summary.deinit(alloc);
            return try http_route_helpers.jsonResponse(alloc, summary);
        }
        if (routes.Routes.matchGroupGraphHydrate(path)) |graph_hydrate_route| {
            const reads = source orelse return try http_route_helpers.textResponse(alloc, 404, "not found");
            var hydrate_req = distributed_graph.parseGraphHydrateRequest(alloc, req.body) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return try http_route_helpers.textResponse(alloc, 400, "invalid graph hydrate request"),
            };
            defer hydrate_req.deinit(alloc);

            var result = (reads.graphHydrateGroupLocal(
                alloc,
                graph_hydrate_route.group_id,
                graph_hydrate_route.table_name,
                hydrate_req,
                .read_index,
            ) catch |err| switch (err) {
                error.TopologyChanged => return try http_route_helpers.textResponse(alloc, 409, "topology changed"),
                error.UnknownGroup, error.TableNotFound => return try http_route_helpers.textResponse(alloc, 404, "not found"),
                else => return err,
            }) orelse return try http_route_helpers.textResponse(alloc, 404, "not found");
            defer result.deinit(alloc);
            return try http_route_helpers.jsonResponse(alloc, result);
        }
        if (routes.Routes.matchGroupGraphEdges(path)) |graph_edges_route| {
            const reads = source orelse return try http_route_helpers.textResponse(alloc, 404, "not found");
            var edges_req = distributed_graph.parseGraphEdgesRequest(alloc, req.body) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return try http_route_helpers.textResponse(alloc, 400, "invalid graph edges request"),
            };
            defer edges_req.deinit(alloc);

            var result = (reads.graphEdgesGroupLocal(
                alloc,
                graph_edges_route.group_id,
                graph_edges_route.table_name,
                edges_req,
                .read_index,
            ) catch |err| switch (err) {
                error.TopologyChanged => return try http_route_helpers.textResponse(alloc, 409, "topology changed"),
                error.UnknownGroup, error.TableNotFound => return try http_route_helpers.textResponse(alloc, 404, "not found"),
                error.InvalidQueryRequest, error.IndexNotFound => return try http_route_helpers.textResponse(alloc, 400, "invalid graph edges request"),
                else => return err,
            }) orelse return try http_route_helpers.textResponse(alloc, 404, "not found");
            defer result.deinit(alloc);
            return try http_route_helpers.jsonResponse(alloc, result);
        }
        if (routes.Routes.matchGroupTextStats(path)) |text_stats_route| {
            const reads = source orelse return try http_route_helpers.textResponse(alloc, 404, "not found");
            var text_stats_result = reads.textStatsGroupLocal(alloc, text_stats_route.group_id, text_stats_route.table_name, req.body) catch |err| switch (err) {
                error.InvalidQueryRequest, error.UnsupportedQueryRequest => return try http_route_helpers.textResponse(alloc, 400, "invalid text stats request"),
                error.TableNotFound, error.UnknownGroup => return try http_route_helpers.textResponse(alloc, 404, "not found"),
                else => return err,
            } orelse return try http_route_helpers.textResponse(alloc, 404, "not found");
            defer text_stats_result.deinit(alloc);
            const body = try alloc.dupe(u8, text_stats_result.json);
            defer alloc.free(body);
            var arena_impl = std.heap.ArenaAllocator.init(alloc);
            defer arena_impl.deinit();
            var response = try table_reads.parseTextStatsHttpResponse(arena_impl.allocator(), req.body, body);
            defer response.deinit(arena_impl.allocator());
            return switch (response) {
                .fields => |value| try http_route_helpers.jsonResponse(alloc, value),
                .background_fields => |value| try http_route_helpers.jsonResponse(alloc, value),
            };
        }
        if (routes.Routes.matchGroupAlgebraicPartials(path)) |partials_route| {
            const reads = source orelse return try http_route_helpers.textResponse(alloc, 404, "not found");
            var partials_result = reads.algebraicPartialsGroupLocal(alloc, partials_route.group_id, partials_route.table_name, req.body) catch |err| switch (err) {
                error.InvalidQueryRequest, error.UnsupportedQueryRequest => return try http_route_helpers.textResponse(alloc, 400, "invalid algebraic partials request"),
                error.TableNotFound, error.UnknownGroup => return try http_route_helpers.textResponse(alloc, 404, "not found"),
                else => return err,
            } orelse return try http_route_helpers.textResponse(alloc, 404, "not found");
            defer partials_result.deinit(alloc);
            return try http_route_helpers.jsonResponse(alloc, partials_result);
        }
    }

    return null;
}

test "internal group read routes handle text stats errors" {
    const alloc = std.testing.allocator;

    const FakeReads = struct {
        fn source() table_reads.TableReadSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .text_stats_group_local = textStatsGroupLocal,
                },
            };
        }

        fn lookup(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: @import("../storage/db/mod.zig").types.LookupOptions,
            _: @import("../raft/mod.zig").ReadConsistency,
        ) !?table_reads.LookupResponse {
            return null;
        }

        fn scan(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: []const u8,
            _: @import("../storage/db/mod.zig").types.ScanOptions,
            _: @import("../raft/mod.zig").ReadConsistency,
        ) !?table_reads.ScanResponse {
            return null;
        }

        fn query(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: @import("../storage/db/mod.zig").types.SearchRequest,
            _: @import("../raft/mod.zig").ReadConsistency,
        ) !?@import("query.zig").QueryResponse {
            return null;
        }

        fn textStatsGroupLocal(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: u64,
            _: []const u8,
            _: []const u8,
        ) !?@import("query.zig").QueryResponse {
            return error.InvalidQueryRequest;
        }
    };

    var resp = (try handle(.{
        .alloc = alloc,
        .reads = FakeReads.source(),
        .catalog = .{
            .ptr = undefined,
        },
        .query_router = .{
            .ptr = undefined,
            .route_query_to_read_schema = struct {
                fn route(_: *anyopaque, _: []const u8, _: *@import("../storage/db/mod.zig").types.SearchRequest) !void {}
            }.route,
        },
    }, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/tables/docs/text-stats",
        .body = "{}",
    }, "/internal/v1/groups/7/tables/docs/text-stats", "")).?;
    defer resp.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 400), resp.status);
    try std.testing.expectEqualStrings("invalid text stats request", resp.body);
}

test "internal group read routes handle query preflight" {
    const alloc = std.testing.allocator;

    const FakeReads = struct {
        fn source() table_reads.TableReadSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .preflight_query_group_local = preflightQueryGroupLocal,
                },
            };
        }

        fn lookup(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: @import("../storage/db/mod.zig").types.LookupOptions,
            _: @import("../raft/mod.zig").ReadConsistency,
        ) !?table_reads.LookupResponse {
            return null;
        }

        fn scan(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: []const u8,
            _: @import("../storage/db/mod.zig").types.ScanOptions,
            _: @import("../raft/mod.zig").ReadConsistency,
        ) !?table_reads.ScanResponse {
            return null;
        }

        fn query(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: @import("../storage/db/mod.zig").types.SearchRequest,
            _: @import("../raft/mod.zig").ReadConsistency,
        ) !?@import("query.zig").QueryResponse {
            return null;
        }

        fn preflightQueryGroupLocal(
            _: *anyopaque,
            alloc_inner: std.mem.Allocator,
            _: u64,
            _: []const u8,
            _: @import("../storage/db/mod.zig").types.SearchRequest,
            _: @import("../raft/mod.zig").ReadConsistency,
            _: u32,
        ) !?@import("../storage/db/mod.zig").RuntimePreflightSummary {
            const result_refs = try alloc_inner.alloc([]const u8, 1);
            errdefer alloc_inner.free(result_refs);
            result_refs[0] = try alloc_inner.dupe(u8, "$embeddings_results");
            errdefer alloc_inner.free(result_refs[0]);
            const text_indexes = try alloc_inner.dupe(@import("../storage/db/mod.zig").TextIndexEstimate, &.{.{
                .name = try alloc_inner.dupe(u8, "search_idx"),
                .doc_count = 42,
                .chunk_backed = false,
                .group_chunk_parents = false,
            }});
            errdefer {
                alloc_inner.free(text_indexes[0].name);
                alloc_inner.free(text_indexes);
            }
            var summary: @import("../storage/db/mod.zig").RuntimePreflightSummary = .{
                .result_refs = result_refs,
                .graph_query_order = &.{},
                .text_indexes = text_indexes,
                .structured_filter_doc_count_sample_estimate = 5,
                .structured_filter_count_sample_size = 3,
                .stored_projection_doc_upper_bound_total = 8,
                .rerank_doc_upper_bound = 4,
                .aggregation_may_scan_full_results = true,
                .shard_count = 1,
            };
            @import("../storage/db/mod.zig").deriveRuntimePreflightEstimates(&summary);
            return summary;
        }
    };

    var resp = (try handle(.{
        .alloc = alloc,
        .reads = FakeReads.source(),
        .catalog = .{
            .ptr = undefined,
        },
        .query_router = .{
            .ptr = undefined,
            .route_query_to_read_schema = struct {
                fn route(_: *anyopaque, _: []const u8, _: *@import("../storage/db/mod.zig").types.SearchRequest) !void {}
            }.route,
        },
    }, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/tables/docs/query-preflight",
        .body = "{\"embeddings\":{\"dense_idx\":[1.0,0.0,0.0]},\"indexes\":[\"dense_idx\"],\"limit\":3}",
    }, "/internal/v1/groups/7/tables/docs/query-preflight", "")).?;
    defer resp.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 200), resp.status);
    var parsed = try std.json.parseFromSlice(@import("../storage/db/mod.zig").RuntimePreflightSummary, alloc, resp.body, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed.value.result_refs.len);
    try std.testing.expectEqualStrings("$embeddings_results", parsed.value.result_refs[0]);
    try std.testing.expectEqual(@as(?u64, 42), parsed.value.corpus_doc_count_estimate);
    try std.testing.expectEqual(@as(?u32, 5), parsed.value.result_doc_estimate);
    try std.testing.expectEqual(@as(?u64, 5), parsed.value.effective_stored_projection_doc_estimate_total);
    try std.testing.expectEqual(@as(?u32, 4), parsed.value.effective_rerank_doc_upper_bound);
    try std.testing.expectEqual(@as(?u32, 5), parsed.value.aggregation_second_pass_doc_estimate);
}
