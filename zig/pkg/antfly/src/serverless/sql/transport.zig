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
pub const cancel_path = "/internal/v1/sql/queries/cancel";
pub const result_path = "/internal/v1/sql/queries/result";
pub const QueryRequest = struct { tenant: plan.Digest, query: @import("state.zig").Query };
/// Resolve a bounded snapshot per call; concurrent requests never mutate the
/// shared client's routing slice. Headless Services expose ready query pods.
pub const Discovery = struct {
    name: []const u8,
    port: u16,
    io: std.Io,
    fn resolve(self: Discovery, a: A, context: Context) ![]const []const u8 {
        try context.ensureActive();
        var storage: [256]std.Io.net.HostName.LookupResult = undefined;
        var queue: std.Io.Queue(std.Io.net.HostName.LookupResult) = .init(&storage);
        try (try std.Io.net.HostName.init(self.name)).lookup(self.io, &queue, .{ .port = self.port });
        var urls: std.ArrayList([]const u8) = .empty;
        while (queue.getOne(self.io)) |result| {
            if (result == .address) try urls.append(a, try std.fmt.allocPrint(a, "http://{f}", .{result.address}));
        } else |err| {
            switch (err) {
                error.Closed => {},
                else => return err,
            }
        }
        try context.ensureActive();
        if (urls.items.len == 0) return error.SqlWorkerUnavailable;
        std.mem.sort([]const u8, urls.items, {}, struct {
            fn less(_: void, left: []const u8, right: []const u8) bool {
                return std.mem.lessThan(u8, left, right);
            }
        }.less);
        return urls.items;
    }
};
pub const Client = struct {
    a: A,
    executor: http.RequestExecutor,
    authentication: auth.Config,
    workers: []const []const u8,
    io: ?std.Io = null,
    discovery: ?Discovery = null,
    control_attempt_ms: u32 = 5_000,
    pub fn dispatcher(self: *Client) runtime.Dispatcher {
        return .{ .ptr = self, .dispatch = dispatch };
    }
    fn dispatch(raw: *anyopaque, tickets: []const plan.Ticket, context: Context) !void {
        const self: *Client = @ptrCast(@alignCast(raw));
        if (self.discovery) |discovery| {
            var arena = std.heap.ArenaAllocator.init(self.a);
            defer arena.deinit();
            var snapshot = self.*;
            snapshot.discovery = null;
            snapshot.workers = try discovery.resolve(arena.allocator(), context);
            return dispatch(&snapshot, tickets, context);
        }
        if (tickets.len > 256 or self.workers.len == 0) return error.SqlWorkerUnavailable;
        var turn_context = context;
        const deadline = @import("antfly_platform").time.monotonicNs() +| 55_000 * std.time.ns_per_ms;
        turn_context.deadline_ns = if (turn_context.deadline_ns) |parent| @min(parent, deadline) else deadline;
        var failures: [256]?anyerror = @splat(null);
        if (self.io != null and self.executor.supportsConcurrentRequests()) {
            var group: std.Io.Group = .init;
            defer group.cancel(self.io.?);
            for (tickets, 0..) |ticket, index| group.async(self.io.?, send, .{ self, ticket, turn_context, &failures[index] });
            try group.await(self.io.?);
        } else for (tickets, 0..) |ticket, index| send(self, ticket, turn_context, &failures[index]);
        for (failures[0..tickets.len]) |failure| if (failure) |err| return err;
    }
    fn queryCall(self: *Client, path: []const u8, body: []const u8, context: Context) !http.HttpResponse {
        if (self.discovery) |discovery| {
            var arena = std.heap.ArenaAllocator.init(self.a);
            defer arena.deinit();
            var snapshot = self.*;
            snapshot.discovery = null;
            snapshot.workers = try discovery.resolve(arena.allocator(), context);
            return snapshot.queryCall(path, body, context);
        }
        var last_error: anyerror = error.SqlWorkerUnavailable;
        // All control operations are replayable against shared durable state.
        // Bound discovery failover; a dead first coordinator must not pin the
        // gateway to that process while another query pod can make progress.
        if (self.workers.len == 0) return error.SqlWorkerUnavailable;
        const first = try firstControlWorker(self.a, self.workers.len, path, body);
        const attempts = @min(@max(self.workers.len, 2), 8);
        for (0..attempts) |attempt| {
            const scoped = try self.controlContext(context, attempts - attempt);
            return self.queryCallAt((first + attempt) % self.workers.len, path, body, scoped) catch |err| {
                // A worker's timeout/cancellation is not the caller's deadline.
                // Check the parent first, including on the last failed attempt.
                try context.ensureActive();
                switch (err) {
                    error.SqlExecutionCancelled => return error.Canceled,
                    error.SqlExecutionUnauthorized, error.SqlExecutionFailed, error.SqlJoinBudgetExceeded, error.SqlQueryExpired => return err,
                    else => {
                        last_error = err;
                        continue;
                    },
                }
            };
        }
        try context.ensureActive();
        return switch (last_error) {
            error.DeadlineExceeded, error.Timeout, error.Canceled, error.Cancelled => error.SqlWorkerUnavailable,
            else => last_error,
        };
    }
    fn controlContext(self: Client, parent: Context, attempts: usize) !Context {
        try parent.ensureActive();
        var scoped = parent;
        const now = @import("antfly_platform").time.monotonicNs();
        var budget = @as(u64, @max(@as(u32, 1), @min(self.control_attempt_ms, 60_000))) * std.time.ns_per_ms;
        if (parent.deadline_ns) |deadline| budget = @min(budget, @max(@as(u64, 1), (deadline -| now) / @max(@as(usize, 1), attempts)));
        scoped.deadline_ns = now +| budget;
        if (parent.deadline_ns) |deadline| scoped.deadline_ns = @min(deadline, scoped.deadline_ns.?);
        return scoped;
    }
    fn firstControlWorker(a: A, count: usize, path: []const u8, body: []const u8) !usize {
        const identity = if (std.mem.eql(u8, path, submit_path))
            local.serverless_external_source_mod.lake_catalog.types.digestHex(body)
        else blk: {
            const parsed = try std.json.parseFromSlice(QueryRequest, a, body, .{});
            defer parsed.deinit();
            break :blk parsed.value.query.id;
        };
        return std.hash.Wyhash.hash(0, &identity) % count;
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
        const remaining_ns = context.deadline_ns.? -| @import("antfly_platform").time.monotonicNs();
        const timeout_ms: u32 = @intCast(@max(@as(u64, 1), @min(@as(u64, 60_000), (remaining_ns +| (std.time.ns_per_ms - 1)) / std.time.ns_per_ms)));
        var response_ = auth.executeRequest(self.a, self.executor, .{ .method = .POST, .uri = uri, .content_type = "application/json", .body = body, .timeout_ms = timeout_ms, .max_response_bytes = 4 * 1024 * 1024, .cancellation = &cancellation }, self.authentication) catch |err| {
            try context.ensureActive();
            return err;
        };
        errdefer response_.deinit(self.a);
        try context.ensureActive();
        if (response_.status == 401 or response_.status == 403) return error.SqlExecutionUnauthorized;
        if (response_.status == 408) return error.SqlExecutionCancelled;
        if (response_.status == 504) return error.DeadlineExceeded;
        if (response_.status == 409) return error.SqlExecutionFailed;
        if (response_.status == 422) return error.SqlJoinBudgetExceeded;
        if (response_.status == 410) return error.SqlQueryExpired;
        if (response_.status != 200) return error.SqlWorkerUnavailable;
        return response_;
    }
    pub fn submit(self: *Client, specification: plan.Plan, context: Context) !@import("state.zig").Query {
        const bytes = try specification.encode(self.a);
        defer self.a.free(bytes);
        var response_ = try self.queryCall(submit_path, bytes, context);
        defer response_.deinit(self.a);
        var parsed = try std.json.parseFromSlice(struct { query: @import("state.zig").Query }, self.a, response_.body, .{});
        defer parsed.deinit();
        const expected = try specification.digest(self.a);
        if (!std.mem.eql(u8, &expected, &parsed.value.query.id) or parsed.value.query.expires_ms != specification.expires_ms) return error.InvalidSqlDistributedPlan;
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
    pub fn cancel(self: *Client, query: QueryRequest, context: Context) !void {
        const bytes = try std.json.Stringify.valueAlloc(self.a, query, .{});
        defer self.a.free(bytes);
        var response_ = try self.queryCall(cancel_path, bytes, context);
        response_.deinit(self.a);
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
        const first = (std.hash.Wyhash.hash(ticket.stage, &ticket.query.id) +% ticket.task) % self.workers.len;
        var last_error: anyerror = error.SqlWorkerUnavailable;
        for (0..@min(self.workers.len, 8)) |offset| {
            self.executeAt((first + offset) % self.workers.len, ticket, context) catch |err| {
                try context.ensureActive();
                if (err == error.SqlExecutionUnauthorized) return err;
                last_error = err;
                continue;
            };
            return;
        }
        return last_error;
    }
    fn executeAt(self: *Client, index: usize, ticket: plan.Ticket, context: Context) !void {
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
        return std.mem.eql(u8, path, task_path) or std.mem.eql(u8, path, submit_path) or std.mem.eql(u8, path, turn_path) or std.mem.eql(u8, path, result_path) or std.mem.eql(u8, path, cancel_path);
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
        return self.execute(a, arena.allocator(), path, body, context) catch |err| {
            if (err == error.Canceled or err == error.Cancelled) context.ensureActive() catch return response(a, 504, "{}");
            return response(a, switch (err) {
                error.SqlExecutionUnauthorized => 403,
                error.SqlQueryRoleRequired, error.SqlExecutionFenced, error.PreconditionFailed, error.SqlWorkerUnavailable => 503,
                error.SqlQueryExpired => 410,
                error.SqlExecutionFailed => 409,
                error.SqlExecutionNotFound => 404,
                error.DeadlineExceeded => 504,
                error.Canceled => 408,
                error.SqlJoinBudgetExceeded => 422,
                else => 500,
            }, "{}");
        };
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
        if (std.mem.eql(u8, path, cancel_path)) {
            try service.requestCancel(input.query);
            return response(a, 200, "{}");
        }
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

test "distributed SQL control routing is stable and spreads healthy queries" {
    var seen: [8]bool = @splat(false);
    for (0..128) |id| {
        const a = std.testing.allocator;
        const label = try std.fmt.allocPrint(a, "query-{d}", .{id});
        defer a.free(label);
        const digest_ = local.serverless_external_source_mod.lake_catalog.types.digestHex(label);
        const request: QueryRequest = .{ .tenant = @splat('a'), .query = .{ .id = digest_, .expires_ms = 1000000 } };
        const body = try std.json.Stringify.valueAlloc(a, request, .{});
        defer a.free(body);
        const chosen = try Client.firstControlWorker(a, seen.len, result_path, body);
        try std.testing.expectEqual(chosen, try Client.firstControlWorker(a, seen.len, turn_path, body));
        try std.testing.expectEqual(chosen, try Client.firstControlWorker(a, seen.len, cancel_path, body));
        try std.testing.expectEqual(@as(usize, std.hash.Wyhash.hash(0, &digest_) % seen.len), chosen);
        seen[chosen] = true;
    }
    for (seen) |used| try std.testing.expect(used);
}
