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
const distributed_join = @import("distributed_join.zig");
const foreign_mod = @import("../foreign/mod.zig");
const db_mod = @import("../storage/db/mod.zig");
const http_common = @import("../raft/transport/http_common.zig");
const http_route_helpers = @import("http_route_helpers.zig");
const metadata_api = @import("../metadata/api.zig");
const query_api = @import("query.zig");
const raft_mod = @import("../raft/mod.zig");
const routes = @import("http_routes.zig");
const table_reads = @import("table_reads.zig");

pub const Context = struct {
    alloc: std.mem.Allocator,
    reads: ?table_reads.TableReadSource,
    join_ctx: distributed_join.JoinContext,
    join_job_store: *distributed_join.JoinJobStore,
};

pub fn handle(ctx: Context, req: http_common.HttpRequest, path: []const u8) !?http_common.HttpResponse {
    if (req.method != .POST) return null;

    if (routes.Routes.matchGroupJoinJobState(path)) |join_job_state_route| {
        _ = join_job_state_route;
        const body = distributed_join.executeGroupJoinJobStateRequest(ctx.join_job_store, ctx.alloc, req.body) catch |err| switch (err) {
            else => return try joinRouteErrorResponse(ctx.alloc, err, "invalid join job state request"),
        };
        defer ctx.alloc.free(body);
        var arena_impl = std.heap.ArenaAllocator.init(ctx.alloc);
        defer arena_impl.deinit();
        const response = try std.json.parseFromSliceLeaky(distributed_join.EncodedJoinJobState, arena_impl.allocator(), body, .{
            .allocate = .alloc_always,
        });
        return try http_route_helpers.jsonResponse(ctx.alloc, response);
    }
    if (routes.Routes.matchGroupJoinFinalize(path)) |join_finalize_route| {
        const reads = ctx.reads orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        const body = distributed_join.executeGroupJoinFinalizeRequest(ctx.join_ctx, ctx.join_job_store, ctx.alloc, reads, join_finalize_route.group_id, join_finalize_route.table_name, req.body) catch |err| switch (err) {
            else => return try joinRouteErrorResponse(ctx.alloc, err, "invalid join finalize request"),
        };
        defer ctx.alloc.free(body);
        var arena_impl = std.heap.ArenaAllocator.init(ctx.alloc);
        defer arena_impl.deinit();
        const response = try std.json.parseFromSliceLeaky(distributed_join.EncodedJoinPartitionResponse, arena_impl.allocator(), body, .{
            .allocate = .alloc_always,
        });
        return try http_route_helpers.jsonResponse(ctx.alloc, response);
    }
    if (routes.Routes.matchGroupJoinRows(path)) |join_rows_route| {
        const reads = ctx.reads orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        const body = distributed_join.executeGroupJoinRowsRequest(ctx.join_ctx, ctx.alloc, reads, join_rows_route.group_id, join_rows_route.table_name, req.body) catch |err| switch (err) {
            else => return try joinRouteErrorResponse(ctx.alloc, err, "invalid join rows request"),
        };
        defer ctx.alloc.free(body);
        var arena_impl = std.heap.ArenaAllocator.init(ctx.alloc);
        defer arena_impl.deinit();
        const response = try std.json.parseFromSliceLeaky(distributed_join.EncodedJoinRowsResponse, arena_impl.allocator(), body, .{
            .allocate = .alloc_always,
        });
        return try http_route_helpers.jsonResponse(ctx.alloc, response);
    }
    if (routes.Routes.matchGroupJoinUnmatched(path)) |join_unmatched_route| {
        const reads = ctx.reads orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        const body = distributed_join.executeGroupJoinUnmatchedRequest(ctx.join_ctx, ctx.alloc, reads, join_unmatched_route.group_id, join_unmatched_route.table_name, req.body) catch |err| switch (err) {
            else => return try joinRouteErrorResponse(ctx.alloc, err, "invalid join unmatched request"),
        };
        defer ctx.alloc.free(body);
        var arena_impl = std.heap.ArenaAllocator.init(ctx.alloc);
        defer arena_impl.deinit();
        const response = try std.json.parseFromSliceLeaky(distributed_join.EncodedJoinUnmatchedResponse, arena_impl.allocator(), body, .{
            .allocate = .alloc_always,
        });
        return try http_route_helpers.jsonResponse(ctx.alloc, response);
    }
    if (routes.Routes.matchGroupJoinPartition(path)) |join_route| {
        const reads = ctx.reads orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        const body = distributed_join.executeGroupJoinPartitionRequest(ctx.join_ctx, ctx.join_job_store, ctx.alloc, reads, join_route.group_id, join_route.table_name, req.body) catch |err| switch (err) {
            else => return try joinRouteErrorResponse(ctx.alloc, err, "invalid join partition request"),
        };
        defer ctx.alloc.free(body);
        var arena_impl = std.heap.ArenaAllocator.init(ctx.alloc);
        defer arena_impl.deinit();
        const response = try std.json.parseFromSliceLeaky(distributed_join.EncodedJoinPartitionResponse, arena_impl.allocator(), body, .{
            .allocate = .alloc_always,
        });
        return try http_route_helpers.jsonResponse(ctx.alloc, response);
    }

    return null;
}

fn joinRouteErrorResponse(alloc: std.mem.Allocator, err: anyerror, invalid_message: []const u8) !http_common.HttpResponse {
    return switch (err) {
        error.InvalidQueryRequest, error.UnsupportedQueryRequest => try http_route_helpers.textResponse(alloc, 400, invalid_message),
        error.TableNotFound, error.UnknownGroup => try http_route_helpers.textResponse(alloc, 404, "not found"),
        error.TopologyChanged => try http_route_helpers.textResponse(alloc, 409, "topology changed"),
        error.DocIdentityNamespaceMismatch => try http_route_helpers.textResponse(alloc, 409, "doc identity namespace mismatch"),
        else => err,
    };
}

test "internal group join routes require reads for join rows" {
    const alloc = std.testing.allocator;
    var join_job_store = distributed_join.JoinJobStore.init(alloc, .{});
    defer join_job_store.deinit();

    var resp = (try handle(.{
        .alloc = alloc,
        .reads = null,
        .join_ctx = TestJoinContext.context(),
        .join_job_store = &join_job_store,
    }, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/tables/docs/join-rows",
        .body = "{}",
    }, "/internal/v1/groups/7/tables/docs/join-rows")).?;
    defer resp.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 404), resp.status);
    try std.testing.expectEqualStrings("not found", resp.body);
}

test "internal group join routes validate join job state requests" {
    const alloc = std.testing.allocator;
    var join_job_store = distributed_join.JoinJobStore.init(alloc, .{});
    defer join_job_store.deinit();

    var resp = (try handle(.{
        .alloc = alloc,
        .reads = null,
        .join_ctx = TestJoinContext.context(),
        .join_job_store = &join_job_store,
    }, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/tables/docs/join-job-state",
        .body = "{}",
    }, "/internal/v1/groups/7/tables/docs/join-job-state")).?;
    defer resp.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 400), resp.status);
    try std.testing.expectEqualStrings("invalid join job state request", resp.body);
}

test "internal group join routes map doc identity mismatch to conflict" {
    const alloc = std.testing.allocator;

    const FailingJoinContext = struct {
        fn context() distributed_join.JoinContext {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .execute_plain_query = executePlainQuery,
                    .execute_query_dispatch = executeQueryDispatch,
                    .build_owned_search_request = buildOwnedSearchRequest,
                    .ensure_foreign_registry = ensureForeignRegistry,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !?metadata_api.AdminSnapshot {
            return error.DocIdentityNamespaceMismatch;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn executePlainQuery(_: *anyopaque, _: std.mem.Allocator, _: table_reads.TableReadSource, _: []const u8, _: []const u8, _: ?[]const u8) !query_api.QueryResponse {
            return error.UnsupportedOperation;
        }

        fn executeQueryDispatch(_: *anyopaque, _: std.mem.Allocator, _: table_reads.TableReadSource, _: []const u8, _: []const u8, _: ?[]const u8) ![]u8 {
            return error.UnsupportedOperation;
        }

        fn buildOwnedSearchRequest(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: std.json.Value) !query_api.OwnedQueryRequest {
            return error.UnsupportedOperation;
        }

        fn ensureForeignRegistry(_: *anyopaque) !*const foreign_mod.Registry {
            return error.UnsupportedOperation;
        }
    };

    const NoopReads = struct {
        fn source() table_reads.TableReadSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                },
            };
        }

        fn lookup(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: db_mod.types.LookupOptions,
            _: raft_mod.ReadConsistency,
        ) !?table_reads.LookupResponse {
            return null;
        }

        fn scan(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: []const u8,
            _: db_mod.types.ScanOptions,
            _: raft_mod.ReadConsistency,
        ) !?table_reads.ScanResponse {
            return null;
        }

        fn query(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: db_mod.types.SearchRequest,
            _: raft_mod.ReadConsistency,
        ) !?query_api.QueryResponse {
            return null;
        }
    };

    var join_job_store = distributed_join.JoinJobStore.init(alloc, .{});
    defer join_job_store.deinit();

    var resp = (try handle(.{
        .alloc = alloc,
        .reads = NoopReads.source(),
        .join_ctx = FailingJoinContext.context(),
        .join_job_store = &join_job_store,
    }, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/tables/docs/join-finalize",
        .body =
        \\{
        \\  "job_id":1,
        \\  "join":{
        \\    "right_table":"docs",
        \\    "join_type":"right",
        \\    "on":{"left_field":"customer_id","right_field":"customer_id","operator":"eq"}
        \\  },
        \\  "left_hits":[],
        \\  "left_fields":[],
        \\  "shuffle_partitions":2
        \\}
        ,
    }, "/internal/v1/groups/7/tables/docs/join-finalize")).?;
    defer resp.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 409), resp.status);
    try std.testing.expectEqualStrings("doc identity namespace mismatch", resp.body);
}

const TestJoinContext = struct {
    fn context() distributed_join.JoinContext {
        return .{
            .ptr = undefined,
            .vtable = &.{
                .admin_snapshot = adminSnapshot,
                .free_admin_snapshot = freeAdminSnapshot,
                .execute_plain_query = executePlainQuery,
                .execute_query_dispatch = executeQueryDispatch,
                .build_owned_search_request = buildOwnedSearchRequest,
                .ensure_foreign_registry = ensureForeignRegistry,
            },
        };
    }

    fn adminSnapshot(_: *anyopaque) !?metadata_api.AdminSnapshot {
        return null;
    }

    fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

    fn executePlainQuery(_: *anyopaque, _: std.mem.Allocator, _: table_reads.TableReadSource, _: []const u8, _: []const u8, _: ?[]const u8) !query_api.QueryResponse {
        return error.UnsupportedOperation;
    }

    fn executeQueryDispatch(_: *anyopaque, _: std.mem.Allocator, _: table_reads.TableReadSource, _: []const u8, _: []const u8, _: ?[]const u8) ![]u8 {
        return error.UnsupportedOperation;
    }

    fn buildOwnedSearchRequest(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: std.json.Value) !query_api.OwnedQueryRequest {
        return error.UnsupportedOperation;
    }

    fn ensureForeignRegistry(_: *anyopaque) !*const foreign_mod.Registry {
        return error.UnsupportedOperation;
    }
};
