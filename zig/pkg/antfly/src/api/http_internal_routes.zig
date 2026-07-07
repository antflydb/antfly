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
const http_common = @import("../raft/transport/http_common.zig");
const http_route_helpers = @import("http_route_helpers.zig");
const http_internal_group_join_routes = @import("http_internal_group_join_routes.zig");
const http_internal_group_read_routes = @import("http_internal_group_read_routes.zig");
const http_internal_group_write_routes = @import("http_internal_group_write_routes.zig");
const repair_jobs = @import("repair_jobs.zig");
const routes = @import("http_routes.zig");

pub const RetrievalExecutor = struct {
    ptr: *anyopaque,
    execute: *const fn (ptr: *anyopaque, req: http_common.HttpRequest, path: []const u8) anyerror!?http_common.HttpResponse,

    fn run(self: RetrievalExecutor, req: http_common.HttpRequest, path: []const u8) !?http_common.HttpResponse {
        return try self.execute(self.ptr, req, path);
    }
};

pub const Context = struct {
    alloc: std.mem.Allocator,
    path: []const u8,
    query: []const u8,
    read_ctx: http_internal_group_read_routes.Context,
    join_ctx: distributed_join.JoinContext,
    join_job_store: *distributed_join.JoinJobStore,
    write_ctx: http_internal_group_write_routes.Context,
    retrieval_executor: RetrievalExecutor,
};

fn handleRepairCancelState(ctx: Context, route: routes.Routes.InternalTableRepairCancelState) !http_common.HttpResponse {
    const store = ctx.write_ctx.repair_job_store orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
    const job_id = std.fmt.parseUnsigned(u64, route.job_id, 10) catch return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid repair job id");
    const attempt_id = std.fmt.parseUnsigned(u64, route.attempt_id, 10) catch return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid repair attempt id");
    const table_name = http_route_helpers.decodePercentEncodedPathComponentAlloc(ctx.alloc, route.table_name) catch {
        return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid path parameter");
    };
    defer ctx.alloc.free(table_name);
    const encoded = (try store.loadJobAlloc(ctx.alloc, job_id)) orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
    defer ctx.alloc.free(encoded);
    var parsed = std.json.parseFromSlice(repair_jobs.JobState, ctx.alloc, encoded, .{ .ignore_unknown_fields = true }) catch {
        return try http_route_helpers.textResponse(ctx.alloc, 500, "invalid repair job state");
    };
    defer parsed.deinit();
    if (!std.mem.eql(u8, parsed.value.table_name, table_name)) return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
    const cancel_requested = parsed.value.cancel_requested or
        repair_jobs.isTerminalPhase(parsed.value.phase) or
        parsed.value.attempt_id != attempt_id;
    return try http_route_helpers.jsonResponseWithStatus(ctx.alloc, 200, .{ .cancel_requested = cancel_requested });
}

pub fn handle(ctx: Context, req: http_common.HttpRequest) !?http_common.HttpResponse {
    if (routes.Routes.matchInternalTableRepairCancelState(ctx.path)) |route| return try handleRepairCancelState(ctx, route);
    if (try ctx.retrieval_executor.run(req, ctx.path)) |resp| return resp;
    if (try http_internal_group_read_routes.handle(ctx.read_ctx, req, ctx.path, ctx.query)) |resp| return resp;
    if (try http_internal_group_join_routes.handle(.{
        .alloc = ctx.alloc,
        .reads = ctx.read_ctx.reads,
        .join_ctx = ctx.join_ctx,
        .join_job_store = ctx.join_job_store,
    }, req, ctx.path)) |resp| return resp;
    if (try http_internal_group_write_routes.handle(ctx.write_ctx, req, ctx.path)) |resp| return resp;
    return null;
}
