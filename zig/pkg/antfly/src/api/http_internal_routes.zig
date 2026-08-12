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
const http_internal_group_join_routes = @import("http_internal_group_join_routes.zig");
const http_internal_group_read_routes = @import("http_internal_group_read_routes.zig");
const http_internal_group_write_routes = @import("http_internal_group_write_routes.zig");

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

pub fn handle(ctx: Context, req: http_common.HttpRequest) !?http_common.HttpResponse {
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
