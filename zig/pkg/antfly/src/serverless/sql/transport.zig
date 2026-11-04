// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Native task dispatch over the existing signed node-to-node HTTP transport.
//! Worker URLs are supplied by trusted deployment discovery, never by a plan.
const std = @import("std");
const local = @import("antfly_local_sources");
const http = local.common_http_http_common;
const auth = @import("../../api/internal_service_auth.zig");
const runtime = @import("runtime.zig");
const plan = @import("plan.zig");
const A = std.mem.Allocator;
const Context = local.serverless_query_lake_read_context.Context;
pub const task_path = "/internal/v1/sql/tasks/execute";
pub const submit_path = "/internal/v1/sql/queries/submit";
pub const turn_path = "/internal/v1/sql/queries/turn";
pub const result_path = "/internal/v1/sql/queries/result";
pub const QueryRequest = struct { tenant: plan.Digest, query: plan.Digest };
pub const Client = struct {
    a: A,
    executor: http.RequestExecutor,
    authentication: auth.Config,
    workers: []const []const u8,
    io: ?std.Io = null,
    pub fn dispatcher(self: *Client) runtime.Dispatcher {
        return .{ .ptr = self, .dispatch = dispatch };
    }
    fn dispatch(raw: *anyopaque, tickets: []const plan.Ticket, context: Context) !void {
        const self: *Client = @ptrCast(@alignCast(raw));
        if (tickets.len > 256 or self.workers.len == 0) return error.SqlWorkerUnavailable;
        var failures: [256]?anyerror = @splat(null);
        if (self.io != null and self.executor.supportsConcurrentRequests()) {
            var group: std.Io.Group = .init;
            defer group.cancel(self.io.?);
            for (tickets, 0..) |ticket, index| group.async(self.io.?, send, .{ self, ticket, context, &failures[index] });
            try group.await(self.io.?);
        } else for (tickets, 0..) |ticket, index| send(self, ticket, context, &failures[index]);
        for (failures[0..tickets.len]) |failure| if (failure) |err| return err;
    }
    fn queryCall(self: *Client, path: []const u8, body: []const u8, context: Context) !http.HttpResponse {
        var last_error: anyerror = error.SqlWorkerUnavailable;
        // All control operations are replayable against shared durable state.
        // Bound discovery failover; a dead first coordinator must not pin the
        // gateway to that process while another query pod can make progress.
        for (0..@min(self.workers.len, 8)) |index| {
            try context.ensureActive();
            return self.queryCallAt(index, path, body, context) catch |err| switch (err) {
                error.SqlExecutionUnauthorized, error.SqlJoinBudgetExceeded, error.SqlQueryExpired, error.DeadlineExceeded, error.Canceled => return err,
                else => {
                    last_error = err;
                    continue;
                },
            };
        }
        return last_error;
    }
    fn queryCallAt(self: *Client, index: usize, path: []const u8, body: []const u8, context: Context) !http.HttpResponse {
        const uri = try std.fmt.allocPrint(self.a, "{s}{s}", .{ std.mem.trimEnd(u8, self.workers[index], "/"), path });
        defer self.a.free(uri);
        const Guard = struct {
            value: Context,
            fn canceled(raw: *const anyopaque) bool {
                const guard: *const @This() = @ptrCast(@alignCast(raw));
                guard.value.ensureActive() catch return true;
                return false;
            }
        };
        const guard: Guard = .{ .value = context };
        const cancellation: http.RequestCancellation = .{ .borrowed_context = &guard, .borrowed_is_cancelled = Guard.canceled };
        var response_ = auth.executeRequest(self.a, self.executor, .{ .method = .POST, .uri = uri, .content_type = "application/json", .body = body, .timeout_ms = 60_000, .max_response_bytes = 4 * 1024 * 1024, .cancellation = &cancellation }, self.authentication) catch |err| {
            try context.ensureActive();
            return err;
        };
        errdefer response_.deinit(self.a);
        try context.ensureActive();
        if (response_.status == 401 or response_.status == 403) return error.SqlExecutionUnauthorized;
        if (response_.status == 422) return error.SqlJoinBudgetExceeded;
        if (response_.status == 410) return error.SqlQueryExpired;
        if (response_.status != 200) return error.SqlWorkerUnavailable;
        return response_;
    }
    pub fn submit(self: *Client, specification: plan.Plan, context: Context) !plan.Digest {
        const bytes = try specification.encode(self.a);
        defer self.a.free(bytes);
        var response_ = try self.queryCall(submit_path, bytes, context);
        defer response_.deinit(self.a);
        var parsed = try std.json.parseFromSlice(struct { query: plan.Digest }, self.a, response_.body, .{});
        defer parsed.deinit();
        const expected = try specification.digest(self.a);
        if (!std.mem.eql(u8, &expected, &parsed.value.query)) return error.InvalidSqlDistributedPlan;
        return parsed.value.query;
    }
    pub fn turn(self: *Client, query: QueryRequest, context: Context) !bool {
        const bytes = try std.json.Stringify.valueAlloc(self.a, query, .{});
        defer self.a.free(bytes);
        var response_ = try self.queryCall(turn_path, bytes, context);
        defer response_.deinit(self.a);
        var parsed = try std.json.parseFromSlice(struct { complete: bool }, self.a, response_.body, .{});
        defer parsed.deinit();
        return parsed.value.complete;
    }
    pub fn result(self: *Client, a: A, query: QueryRequest, context: Context) !?runtime.Result {
        const bytes = try std.json.Stringify.valueAlloc(self.a, query, .{});
        defer self.a.free(bytes);
        var response_ = try self.queryCall(result_path, bytes, context);
        defer response_.deinit(self.a);
        return std.json.parseFromSliceLeaky(?runtime.Result, a, response_.body, .{ .allocate = .alloc_always });
    }
    fn send(self: *Client, ticket: plan.Ticket, context: Context, failure: *?anyerror) void {
        self.execute(ticket, context) catch |err| {
            failure.* = err;
        };
    }
    pub fn execute(self: *Client, ticket: plan.Ticket, context: Context) !void {
        try context.ensureActive();
        if (self.workers.len == 0) return error.SqlWorkerUnavailable;
        // Rendezvous ownership is unnecessary for correctness. A stable task
        // hash distributes requests and lets service discovery change on retry.
        const index = (std.hash.Wyhash.hash(ticket.stage, &ticket.query) +% ticket.task) % self.workers.len;
        const uri = try std.fmt.allocPrint(self.a, "{s}{s}", .{ std.mem.trimEnd(u8, self.workers[index], "/"), task_path });
        defer self.a.free(uri);
        const body = try std.json.Stringify.valueAlloc(self.a, ticket, .{});
        defer self.a.free(body);
        const Guard = struct {
            value: Context,
            fn canceled(raw: *const anyopaque) bool {
                const self_: *const @This() = @ptrCast(@alignCast(raw));
                self_.value.ensureActive() catch return true;
                return false;
            }
        };
        const guard: Guard = .{ .value = context };
        const cancellation: http.RequestCancellation = .{ .borrowed_context = &guard, .borrowed_is_cancelled = Guard.canceled };
        var response = auth.executeRequest(self.a, self.executor, .{ .method = .POST, .uri = uri, .content_type = "application/json", .body = body, .timeout_ms = 60_000, .max_response_bytes = 1024, .cancellation = &cancellation }, self.authentication) catch |err| {
            try context.ensureActive();
            return err;
        };
        defer response.deinit(self.a);
        try context.ensureActive();
        if (response.status == 401 or response.status == 403) return error.SqlExecutionUnauthorized;
        if (response.status != 200 and response.status != 204) return error.SqlWorkerUnavailable;
    }
};

/// Installed on query/combined listeners. No migration path accepts unsigned
/// execution requests, and tasks share the listener's foreground admission.
pub const Server = struct {
    factory: runtime.Factory,
    authentication: auth.Config,
    admission: *@import("antfly_local_sources").common_request_admission.RequestAdmission,
    pub fn handles(path: []const u8) bool {
        return std.mem.eql(u8, path, task_path) or std.mem.eql(u8, path, submit_path) or std.mem.eql(u8, path, turn_path) or std.mem.eql(u8, path, result_path);
    }
    pub fn handle(self: Server, a: A, path: []const u8, credential: ?[]const u8, body: []const u8, context: Context) !http.HttpResponse {
        const now: i64 = @intCast(@divFloor(@import("antfly_platform").time.realtimeNs(), std.time.ns_per_s));
        auth.verifyToken(a, credential orelse return response(a, 401, "{}"), self.authentication, now) catch return response(a, 403, "{}");
        if (body.len > (if (std.mem.eql(u8, path, submit_path)) @as(usize, 1024 * 1024) else 1024)) return response(a, 413, "{}");
        // Coordinator control traffic must not occupy a worker slot while
        // waiting for that same pod's task RPC. HTTP connection admission
        // separately bounds control requests.
        var lease = if (std.mem.eql(u8, path, task_path)) self.admission.tryAcquireLease() orelse return response(a, 503, "{}") else null;
        defer if (lease) |*held| held.release();
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        return self.execute(a, arena.allocator(), path, body, context) catch |err| response(a, switch (err) {
            error.SqlExecutionUnauthorized => 403,
            error.SqlQueryRoleRequired, error.SqlExecutionFenced, error.PreconditionFailed, error.SqlWorkerUnavailable => 503,
            error.SqlQueryExpired => 410,
            error.SqlExecutionNotFound => 404,
            error.DeadlineExceeded => 504,
            error.Canceled => 408,
            error.SqlJoinBudgetExceeded => 422,
            else => 500,
        }, "{}");
    }
    fn execute(self: Server, a: A, scratch: A, path: []const u8, body: []const u8, context: Context) !http.HttpResponse {
        try context.ensureActive();
        if (std.mem.eql(u8, path, task_path)) {
            const input = std.json.parseFromSliceLeaky(plan.Ticket, scratch, body, .{}) catch return response(a, 400, "{}");
            const service = try self.factory.open(self.factory.ptr, scratch, input.tenant, context);
            try service.execute(input);
            return response(a, 200, "{}");
        }
        if (std.mem.eql(u8, path, submit_path)) {
            const input = std.json.parseFromSliceLeaky(plan.Plan, scratch, body, .{}) catch return response(a, 400, "{}");
            const service = try self.factory.open(self.factory.ptr, scratch, input.tenant, context);
            const query = try service.submit(input);
            return json(a, .{ .query = query });
        }
        const input = std.json.parseFromSliceLeaky(QueryRequest, scratch, body, .{}) catch return response(a, 400, "{}");
        const service = try self.factory.open(self.factory.ptr, scratch, input.tenant, context);
        if (std.mem.eql(u8, path, turn_path)) return json(a, .{ .complete = try service.turn(input.query, self.factory.dispatcher) });
        if (std.mem.eql(u8, path, result_path)) return json(a, try service.pollResult(scratch, input.query));
        return response(a, 404, "{}");
    }
    fn json(a: A, value: anytype) !http.HttpResponse {
        const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
        defer a.free(bytes);
        return response(a, 200, bytes);
    }
    fn response(a: A, status: u16, body: []const u8) !http.HttpResponse {
        return .{ .status = status, .owner_allocator = a, .body = try a.dupe(u8, body) };
    }
};
