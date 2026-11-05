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
pub const Discovery = @import("discovery.zig").Discovery;
pub const Routing = @import("routing.zig").Routing;
pub const Client = struct {
    a: A,
    routing: *Routing,
    executor: http.RequestExecutor,
    authentication: auth.Config,
    workers: []const []const u8,
    io: ?std.Io = null,
    discovery: ?*Discovery = null,
    control_attempt_ms: u32 = 5_000,
    submit_attempt_ms: u32 = 20_000,
    recovery_attempt_ms: u32 = 1000,
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
        if (self.workers.len == 0) return error.SqlWorkerUnavailable;
        const first = try firstControlWorker(self.a, self.workers.len, path, body);
        var attempts: usize = 0;
        const maximum = @min(@max(self.workers.len, 2), 8);
        var round: u8 = 0;
        while (round < 2 and attempts < maximum) : (round += 1) {
            for (0..self.workers.len) |offset| {
                try context.ensureActive();
                const index = (first + offset) % self.workers.len;
                const url = self.workers[index];
                const admission = self.routing.admit(url, @import("antfly_platform").time.monotonicNs());
                if (admission.kind == .blocked) continue;
                const response_ = self.controlAdmitted(index, path, body, context, admission, attempts == 0 and self.workers.len > 1) catch |err| {
                    switch (err) {
                        // An authoritative application response proves reachability;
                        // missing queries keep the existing replay/failover behavior.
                        error.SqlExecutionCancelled, error.SqlExecutionFailed, error.SqlJoinBudgetExceeded, error.SqlQueryExpired, error.SqlExecutionNotFound, error.SqlControlRequestRejected => {
                            self.routing.success(url);
                            try context.ensureActive();
                            if (err == error.SqlExecutionCancelled) return error.Canceled;
                            if (err != error.SqlExecutionNotFound and err != error.SqlControlRequestRejected) return err;
                        },
                        else => {
                            const cancelled = if (context.cancellation) |token| token.isCancelled() else false;
                            if (!cancelled and err != error.SqlExecutionUnauthorized and !localFailure(err)) self.routing.failure(url, @import("antfly_platform").time.monotonicNs()) else self.routing.abandon(url, admission);
                            try context.ensureActive();
                            if (err == error.SqlExecutionUnauthorized or localFailure(err)) return err;
                        },
                    }
                    last_error = err;
                    attempts += 1;
                    if (attempts == maximum) break;
                    continue;
                };
                self.routing.success(url);
                return response_;
            }
            // Preserve replay after an indeterminate ACK with one bounded
            // cooldown/recovery round when no healthy peer remains.
            if (round != 0 or attempts == maximum or !try self.waitForControlRetry(context)) break;
        }
        try context.ensureActive();
        return switch (last_error) {
            error.DeadlineExceeded, error.Timeout, error.Canceled, error.Cancelled, error.SqlExecutionNotFound, error.SqlControlRequestRejected => error.SqlWorkerUnavailable,
            else => last_error,
        };
    }
    fn waitForControlRetry(self: *Client, context: Context) !bool {
        const io = self.io orelse context.io orelse return false;
        const now = @import("antfly_platform").time.monotonicNs();
        var delay: u64 = std.math.maxInt(u64);
        for (self.workers) |url| delay = @min(delay, self.routing.readyIn(url, now));
        // Never wait out a long quarantine or another caller's active probe.
        if (delay > std.time.ns_per_s) return false;
        if (context.deadline_ns) |deadline| if (delay > (deadline -| now) / 2) return false;
        const until = now +| delay;
        while (true) {
            try context.ensureActive();
            const time = @import("antfly_platform").time.monotonicNs();
            if (time >= until) return true;
            try std.Io.sleep(io, .fromNanoseconds(@intCast(@min(until - time, 10 * std.time.ns_per_ms))), .awake);
        }
    }
    fn controlAdmitted(self: *Client, index: usize, path: []const u8, body: []const u8, context: Context, admission: Routing.Admission, reserve_peer: bool) !http.HttpResponse {
        if (admission.kind == .probe) {
            const probe_body = if (std.mem.eql(u8, path, submit_path)) blk: {
                const parsed = try std.json.parseFromSlice(struct { tenant: plan.Digest, expires_ms: u64 }, self.a, body, .{ .ignore_unknown_fields = true });
                defer parsed.deinit();
                break :blk try std.json.Stringify.valueAlloc(self.a, QueryRequest{ .tenant = parsed.value.tenant, .query = .{ .id = local.serverless_external_source_mod.lake_catalog.types.digestHex(body), .expires_ms = parsed.value.expires_ms } }, .{});
            } else try self.a.dupe(u8, body);
            defer self.a.free(probe_body);
            try self.probe(index, probe_body, context);
        }
        return self.queryCallAt(index, path, body, try self.controlContext(context, reserve_peer, std.mem.eql(u8, path, submit_path)));
    }
    fn probe(self: *Client, index: usize, body: []const u8, context: Context) !void {
        var scoped = context;
        const now = @import("antfly_platform").time.monotonicNs();
        var budget: u64 = @as(u64, @max(@as(u32, 1), @min(self.recovery_attempt_ms, 1000))) * std.time.ns_per_ms;
        if (context.deadline_ns) |deadline| budget = @min(budget, (deadline -| now) / 2);
        scoped.deadline_ns = now +| budget;
        var response_ = self.queryCallAt(index, result_path, body, scoped) catch |err| switch (err) {
            // A submission may not yet have created its query. All these signed
            // metadata responses establish endpoint health without task work.
            error.SqlExecutionCancelled, error.SqlExecutionFailed, error.SqlJoinBudgetExceeded, error.SqlQueryExpired, error.SqlExecutionNotFound => return,
            else => return err,
        };
        response_.deinit(self.a);
    }
    fn controlContext(self: Client, parent: Context, reserve_peer: bool, submission: bool) !Context {
        try parent.ensureActive();
        var scoped = parent;
        const now = @import("antfly_platform").time.monotonicNs();
        const cap = if (submission) self.submit_attempt_ms else self.control_attempt_ms;
        var budget = @as(u64, @max(@as(u32, 1), @min(cap, 60_000))) * std.time.ns_per_ms;
        // Reserve a backup attempt without shrinking every healthy request's
        // budget in proportion to the cluster's pod count. Submit also retains
        // two source cuts and legitimately needs more time than a compact poll.
        if (parent.deadline_ns) |deadline| budget = @min(budget, @max(@as(u64, 1), (deadline -| now) / @as(u64, if (reserve_peer) 2 else 1)));
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
        if (response_.status == 404) return error.SqlExecutionNotFound;
        if (response_.status == 409) return error.SqlExecutionFailed;
        if (response_.status == 422) return error.SqlJoinBudgetExceeded;
        if (response_.status == 410) return error.SqlQueryExpired;
        if (response_.status >= 400 and response_.status < 500 and response_.status != 429) return error.SqlControlRequestRejected;
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
    fn localFailure(err: anyerror) bool {
        return switch (err) {
            error.OutOfMemory, error.ConcurrencyUnavailable, error.SystemResources, error.ProcessFdQuotaExceeded, error.SystemFdQuotaExceeded, error.LockedMemoryLimitExceeded => true,
            else => false,
        };
    }
    pub fn execute(self: *Client, ticket: plan.Ticket, context: Context) !void {
        try context.ensureActive();
        if (self.workers.len == 0) return error.SqlWorkerUnavailable;
        const first = (std.hash.Wyhash.hash(ticket.stage, &ticket.query.id) +% ticket.task +% ticket.attempt) % self.workers.len;
        var last_error: anyerror = error.SqlWorkerUnavailable;
        var attempts: usize = 0;
        for (0..self.workers.len) |offset| {
            const index = (first + offset) % self.workers.len;
            const url = self.workers[index];
            const admission = self.routing.admit(url, @import("antfly_platform").time.monotonicNs());
            if (admission.kind == .blocked) continue;
            self.executeAdmitted(index, ticket, context, admission) catch |err| {
                // Record an unresponsive endpoint even when it consumed the
                // delivery quantum. The next durable replay must try a peer.
                // Explicit query/caller cancellation says nothing about health.
                const cancelled = if (context.cancellation) |token| token.isCancelled() else false;
                if (err == error.SqlTaskAdmissionUnavailable)
                    self.routing.success(url)
                else if (!cancelled and err != error.SqlExecutionUnauthorized and !localFailure(err))
                    self.routing.failure(url, @import("antfly_platform").time.monotonicNs())
                else
                    self.routing.abandon(url, admission);
                try context.ensureActive();
                if (err == error.SqlExecutionUnauthorized or localFailure(err)) return err;
                last_error = err;
                attempts += 1;
                if (attempts == 8) break;
                continue;
            };
            self.routing.success(url);
            return;
        }
        return if (last_error == error.SqlTaskAdmissionUnavailable) error.SqlWorkerUnavailable else last_error;
    }
    fn executeAdmitted(self: *Client, index: usize, ticket: plan.Ticket, context: Context, admission: Routing.Admission) !void {
        if (admission.kind == .probe) {
            const body = try std.json.Stringify.valueAlloc(self.a, QueryRequest{ .tenant = ticket.tenant, .query = ticket.query }, .{});
            defer self.a.free(body);
            try self.probe(index, body, context);
        }
        try self.executeAt(index, ticket, context);
    }
    fn executeAt(self: *Client, index: usize, ticket: plan.Ticket, context: Context) !void {
        const uri = try std.fmt.allocPrint(self.a, "{s}{s}", .{ std.mem.trimEnd(u8, self.workers[index], "/"), task_path });
        defer self.a.free(uri);
        // Routing hints stay process-local; the worker RPC remains compatible
        // with existing ticket readers and accepts only execution identity.
        const body = try std.json.Stringify.valueAlloc(self.a, .{ .tenant = ticket.tenant, .query = ticket.query, .stage = ticket.stage, .task = ticket.task }, .{});
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
        if (response.status == 429) return error.SqlTaskAdmissionUnavailable;
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
        var lease = if (std.mem.eql(u8, path, task_path)) self.admission.tryAcquireLease() orelse return response(a, 429, "{}") else null;
        defer if (lease) |*held| held.release();
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        return self.execute(a, arena.allocator(), path, body, context) catch |err| {
            if (err == error.Canceled or err == error.Cancelled) context.ensureActive() catch return response(a, 504, "{}");
            return response(a, switch (err) {
                error.SqlExecutionUnauthorized => 403,
                error.SqlTaskAdmissionUnavailable => 429,
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
            service.execute(input) catch |err| {
                if (runtime.retiredTaskDelivery(err)) return response(a, 204, "");
                return err;
            };
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

test "distributed SQL control budgets reserve a backup and allow source retention work" {
    const client: Client = .{ .routing = undefined, .a = std.testing.allocator, .executor = undefined, .authentication = undefined, .workers = &.{} };
    const now = @import("antfly_platform").time.monotonicNs();
    const parent: Context = .{ .deadline_ns = now + 60 * std.time.ns_per_s };
    const poll = try client.controlContext(parent, true, false);
    const submit_ = try client.controlContext(parent, true, true);
    try std.testing.expect(submit_.deadline_ns.? > poll.deadline_ns.?);
    try std.testing.expect(submit_.deadline_ns.? <= parent.deadline_ns.?);
    const short: Context = .{ .deadline_ns = @import("antfly_platform").time.monotonicNs() + 2 * std.time.ns_per_s };
    const first = try client.controlContext(short, true, false);
    try std.testing.expect(first.deadline_ns.? < short.deadline_ns.?);
    const backup = try client.controlContext(short, false, false);
    try std.testing.expectEqual(short.deadline_ns, backup.deadline_ns);
}

test "distributed SQL task replay avoids a stalled endpoint across routing snapshots" {
    const Fake = struct {
        stalled: usize = 0,
        probes: usize = 0,
        healthy: usize = 0,
        fn execute(raw: *anyopaque, allocator: A, request_: http.HttpRequest) !http.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const wire = try std.json.parseFromSlice(std.json.Value, allocator, request_.body, .{});
            defer wire.deinit();
            try std.testing.expect(wire.value.object.get("attempt") == null);
            if (std.mem.startsWith(u8, request_.uri, "http://stalled/")) {
                if (std.mem.endsWith(u8, request_.uri, result_path)) self.probes += 1 else self.stalled += 1;
                while (!request_.cancellation.?.isCancelled()) try std.Io.sleep(std.testing.io, .fromMilliseconds(1), .awake);
                return error.Canceled;
            }
            self.healthy += 1;
            return .{ .status = 200, .owner_allocator = allocator, .body = try allocator.dupe(u8, "{}") };
        }
    };
    var fake: Fake = .{};
    var routing: Routing = .{ .cooldown_ms = 5000 };
    var client: Client = .{ .routing = &routing, .a = std.testing.allocator, .executor = .{ .ptr = &fake, .vtable = &.{ .execute = Fake.execute } }, .authentication = .{ .secret = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", .issuer = "test" }, .workers = &.{ "http://stalled", "http://healthy" }, .recovery_attempt_ms = 20 };
    var ticket: plan.Ticket = .{ .tenant = @splat('a'), .query = .{ .id = @splat('b'), .expires_ms = 1000000 }, .stage = 0, .task = 0 };
    ticket.task = @intCast(std.hash.Wyhash.hash(ticket.stage, &ticket.query.id) % 2);
    const expired: Context = .{ .deadline_ns = @import("antfly_platform").time.monotonicNs() + 20 * std.time.ns_per_ms };
    try std.testing.expectError(error.DeadlineExceeded, client.execute(ticket, expired));
    var snapshot = client;
    const live: Context = .{ .deadline_ns = @import("antfly_platform").time.monotonicNs() + std.time.ns_per_s };
    try snapshot.execute(ticket, live);
    try client.execute(ticket, live);
    try std.testing.expectEqual(@as(usize, 1), fake.stalled);
    try std.testing.expectEqual(@as(usize, 2), fake.healthy);
    // Lease-expired replay moves peers even after quarantine expiry/restart.
    routing = .{};
    var replay = ticket;
    replay.attempt = 1;
    try client.execute(replay, live);
    try std.testing.expectEqual(@as(usize, 1), fake.stalled);
    try std.testing.expectEqual(@as(usize, 3), fake.healthy);
    // An expired quarantine gets a short metadata probe, not another whole
    // delivery timeout. A healthy peer remains reachable in this same call.
    routing.failure("http://stalled", 0);
    try client.execute(ticket, live);
    try live.ensureActive();
    try std.testing.expectEqual(@as(usize, 1), fake.stalled);
    try std.testing.expectEqual(@as(usize, 1), fake.probes);
    try std.testing.expectEqual(@as(usize, 4), fake.healthy);
}

test "distributed SQL caller cancellation never quarantines a healthy endpoint" {
    const Fake = struct {
        cancelled: bool = false,
        fn check(raw: *const anyopaque) bool {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            return self.cancelled;
        }
        fn execute(raw: *anyopaque, _: A, _: http.HttpRequest) !http.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.cancelled = true;
            return error.Canceled;
        }
    };
    var fake: Fake = .{};
    var routing: Routing = .{};
    var client: Client = .{ .a = std.testing.allocator, .routing = &routing, .executor = .{ .ptr = &fake, .vtable = &.{ .execute = Fake.execute } }, .authentication = .{ .secret = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", .issuer = "test" }, .workers = &.{"http://healthy"} };
    const ticket: plan.Ticket = .{ .tenant = @splat('a'), .query = .{ .id = @splat('b'), .expires_ms = 1000000 }, .stage = 0, .task = 0 };
    try std.testing.expectError(error.Canceled, client.execute(ticket, .{ .cancellation = .{ .ptr = &fake, .is_cancelled_fn = Fake.check } }));
    try std.testing.expectEqual(.healthy, routing.admit("http://healthy", @import("antfly_platform").time.monotonicNs()).kind);
}

test "distributed SQL local allocation and executor capacity failures preserve endpoint health" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var routing: Routing = .{};
    var client: Client = .{ .a = failing.allocator(), .routing = &routing, .executor = undefined, .authentication = undefined, .workers = &.{"http://healthy"} };
    const ticket: plan.Ticket = .{ .tenant = @splat('a'), .query = .{ .id = @splat('b'), .expires_ms = 1000000 }, .stage = 0, .task = 0 };
    try std.testing.expectError(error.OutOfMemory, client.execute(ticket, .{}));
    try std.testing.expectEqual(.healthy, routing.admit("http://healthy", @import("antfly_platform").time.monotonicNs()).kind);
    const Fake = struct {
        failure: anyerror,
        fn execute(raw: *anyopaque, _: A, _: http.HttpRequest) !http.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.failure;
        }
    };
    var fake: Fake = .{ .failure = error.ConcurrencyUnavailable };
    client.a = std.testing.allocator;
    client.authentication = .{ .secret = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", .issuer = "test" };
    client.executor = .{ .ptr = &fake, .vtable = &.{ .execute = Fake.execute } };
    for ([_]anyerror{ error.ConcurrencyUnavailable, error.SystemResources, error.ProcessFdQuotaExceeded }) |failure| {
        fake.failure = failure;
        try std.testing.expectError(failure, client.execute(ticket, .{}));
        try std.testing.expectEqual(.healthy, routing.admit("http://healthy", @import("antfly_platform").time.monotonicNs()).kind);
    }
}

test "distributed SQL control calls share task quarantine and skip repeated failed peers" {
    const a = std.testing.allocator;
    const Fake = struct {
        failed: usize = 0,
        healthy: usize = 0,
        fn execute(raw: *anyopaque, allocator: A, request_: http.HttpRequest) !http.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (std.mem.startsWith(u8, request_.uri, "http://stalled/")) {
                self.failed += 1;
                return error.Timeout;
            }
            self.healthy += 1;
            return .{ .status = 200, .owner_allocator = allocator, .body = try allocator.dupe(u8, if (std.mem.endsWith(u8, request_.uri, result_path)) "null" else "{}") };
        }
    };
    const query: QueryRequest = .{ .tenant = @splat('a'), .query = .{ .id = @splat('b'), .expires_ms = 1000000 } };
    const body = try std.json.Stringify.valueAlloc(a, query, .{});
    defer a.free(body);
    const first = try Client.firstControlWorker(a, 2, result_path, body);
    const workers: [2][]const u8 = if (first == 0) .{ "http://stalled", "http://healthy" } else .{ "http://healthy", "http://stalled" };
    var fake: Fake = .{};
    var routing: Routing = .{ .cooldown_ms = 30000 };
    var client: Client = .{ .a = a, .routing = &routing, .executor = .{ .ptr = &fake, .vtable = &.{ .execute = Fake.execute } }, .authentication = .{ .secret = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", .issuer = "test" }, .workers = &workers };
    const context: Context = .{ .deadline_ns = @import("antfly_platform").time.monotonicNs() + 30 * std.time.ns_per_s };
    for (0..3) |_| try std.testing.expect((try client.result(a, query, context)) == null);
    try std.testing.expectEqual(@as(usize, 1), fake.failed);
    try std.testing.expectEqual(.blocked, routing.admit("http://stalled", @import("antfly_platform").time.monotonicNs()).kind);
    // The next task also sees the control request's failure through the same
    // process-owned cache, even when its deterministic first choice is stalled.
    var ticket: plan.Ticket = .{ .tenant = query.tenant, .query = query.query, .stage = 0, .task = 0 };
    if (std.hash.Wyhash.hash(ticket.stage, &ticket.query.id) % 2 != first) ticket.task = 1;
    try client.execute(ticket, context);
    try std.testing.expectEqual(@as(usize, 1), fake.failed);
    try std.testing.expectEqual(@as(usize, 4), fake.healthy);
}

test "distributed SQL submission recovery uses a short absent-query probe and retains its work budget" {
    const a = std.testing.allocator;
    const body = try std.json.Stringify.valueAlloc(a, .{ .tenant = @as(plan.Digest, @splat('a')), .expires_ms = @as(u64, 1000000) }, .{});
    defer a.free(body);
    const Fake = struct {
        expected: plan.Digest,
        probes: usize = 0,
        submissions: usize = 0,
        fn execute(raw: *anyopaque, allocator: A, request_: http.HttpRequest) !http.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (std.mem.endsWith(u8, request_.uri, result_path)) {
                self.probes += 1;
                const parsed = try std.json.parseFromSlice(QueryRequest, allocator, request_.body, .{});
                defer parsed.deinit();
                try std.testing.expectEqual(self.expected, parsed.value.query.id);
                try std.testing.expectEqual(@as(u64, 1000000), parsed.value.query.expires_ms);
                try std.testing.expect(request_.timeout_ms.? <= 1000);
                return .{ .status = 404, .owner_allocator = allocator, .body = try allocator.dupe(u8, "{}") };
            }
            self.submissions += 1;
            try std.testing.expect(request_.timeout_ms.? > 1000);
            return .{ .status = 200, .owner_allocator = allocator, .body = try allocator.dupe(u8, "{}") };
        }
    };
    var fake: Fake = .{ .expected = local.serverless_external_source_mod.lake_catalog.types.digestHex(body) };
    var routing: Routing = .{ .cooldown_ms = 1 };
    routing.failure("http://recovered", @import("antfly_platform").time.monotonicNs() -| 2 * std.time.ns_per_ms);
    var client: Client = .{ .a = a, .routing = &routing, .executor = .{ .ptr = &fake, .vtable = &.{ .execute = Fake.execute } }, .authentication = .{ .secret = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", .issuer = "test" }, .workers = &.{"http://recovered"}, .recovery_attempt_ms = 1000 };
    var response_ = try client.queryCall(submit_path, body, .{ .deadline_ns = @import("antfly_platform").time.monotonicNs() + 30 * std.time.ns_per_s });
    response_.deinit(a);
    try std.testing.expectEqual(@as(usize, 1), fake.probes);
    try std.testing.expectEqual(@as(usize, 1), fake.submissions);
    try std.testing.expectEqual(.healthy, routing.admit("http://recovered", @import("antfly_platform").time.monotonicNs()).kind);
}

test "distributed SQL control semantic responses and local pressure do not quarantine workers" {
    const a = std.testing.allocator;
    const Fake = struct {
        status: u16 = 200,
        failure: ?anyerror = null,
        fn execute(raw: *anyopaque, allocator: A, _: http.HttpRequest) !http.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.failure) |err| return err;
            return .{ .status = self.status, .owner_allocator = allocator, .body = try allocator.dupe(u8, "{}") };
        }
    };
    const Case = struct { status: u16 = 200, failure: ?anyerror = null, expected: anyerror };
    const query: QueryRequest = .{ .tenant = @splat('a'), .query = .{ .id = @splat('b'), .expires_ms = 1000000 } };
    for ([_]Case{
        .{ .status = 400, .expected = error.SqlWorkerUnavailable },
        .{ .status = 413, .expected = error.SqlWorkerUnavailable },
        .{ .status = 403, .expected = error.SqlExecutionUnauthorized },
        .{ .status = 408, .expected = error.Canceled },
        .{ .status = 409, .expected = error.SqlExecutionFailed },
        .{ .status = 410, .expected = error.SqlQueryExpired },
        .{ .status = 422, .expected = error.SqlJoinBudgetExceeded },
        .{ .status = 404, .expected = error.SqlWorkerUnavailable },
        .{ .failure = error.OutOfMemory, .expected = error.OutOfMemory },
        .{ .failure = error.ConcurrencyUnavailable, .expected = error.ConcurrencyUnavailable },
    }) |case| {
        var fake: Fake = .{ .status = case.status, .failure = case.failure };
        var routing: Routing = .{};
        var client: Client = .{ .a = a, .routing = &routing, .executor = .{ .ptr = &fake, .vtable = &.{ .execute = Fake.execute } }, .authentication = .{ .secret = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", .issuer = "test" }, .workers = &.{"http://worker"} };
        try std.testing.expectError(case.expected, client.result(a, query, .{}));
        try std.testing.expectEqual(.healthy, routing.admit("http://worker", @import("antfly_platform").time.monotonicNs()).kind);
    }
}

test "distributed SQL cancelled control probes release reservations without poisoning endpoint health" {
    const a = std.testing.allocator;
    const Fake = struct {
        cancelled: bool = false,
        calls: usize = 0,
        fn check(raw: *const anyopaque) bool {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            return self.cancelled;
        }
        fn execute(raw: *anyopaque, _: A, _: http.HttpRequest) !http.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            self.cancelled = true;
            return error.Canceled;
        }
    };
    var fake: Fake = .{};
    var routing: Routing = .{ .cooldown_ms = 1 };
    routing.failure("http://recovered", @import("antfly_platform").time.monotonicNs() -| 2 * std.time.ns_per_ms);
    var client: Client = .{ .a = a, .routing = &routing, .executor = .{ .ptr = &fake, .vtable = &.{ .execute = Fake.execute } }, .authentication = .{ .secret = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", .issuer = "test" }, .workers = &.{"http://recovered"} };
    const query: QueryRequest = .{ .tenant = @splat('a'), .query = .{ .id = @splat('b'), .expires_ms = 1000000 } };
    try std.testing.expectError(error.Canceled, client.result(a, query, .{ .cancellation = .{ .ptr = &fake, .is_cancelled_fn = Fake.check } }));
    try std.testing.expectEqual(@as(usize, 1), fake.calls);
    try std.testing.expectEqual(.probe, routing.admit("http://recovered", @import("antfly_platform").time.monotonicNs()).kind);
}

test "distributed SQL spill pressure fails over without quarantining responsive workers" {
    const a = std.testing.allocator;
    const Fake = struct {
        busy: usize = 0,
        accepted: usize = 0,
        all_busy: bool = false,
        fn execute(raw: *anyopaque, allocator: A, request_: http.HttpRequest) !http.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const busy = self.all_busy or std.mem.startsWith(u8, request_.uri, "http://busy/");
            if (busy) self.busy += 1 else self.accepted += 1;
            return .{ .status = if (busy) 429 else 200, .owner_allocator = allocator, .body = try allocator.dupe(u8, "{}") };
        }
    };
    var fake: Fake = .{};
    var routing: Routing = .{};
    var ticket: plan.Ticket = .{ .tenant = @splat('a'), .query = .{ .id = @splat('b'), .expires_ms = 1000000 }, .stage = 0, .task = 0 };
    if (std.hash.Wyhash.hash(ticket.stage, &ticket.query.id) % 2 != 0) ticket.task = 1;
    var client: Client = .{ .a = a, .routing = &routing, .executor = .{ .ptr = &fake, .vtable = &.{ .execute = Fake.execute } }, .authentication = .{ .secret = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", .issuer = "test" }, .workers = &.{ "http://busy", "http://ready" } };
    for (0..3) |_| try client.execute(ticket, .{});
    try std.testing.expectEqual(@as(usize, 3), fake.busy);
    try std.testing.expectEqual(@as(usize, 3), fake.accepted);
    try std.testing.expectEqual(.healthy, routing.admit("http://busy", @import("antfly_platform").time.monotonicNs()).kind);
    fake.all_busy = true;
    try std.testing.expectError(error.SqlWorkerUnavailable, client.execute(ticket, .{}));
    try std.testing.expectEqual(.healthy, routing.admit("http://busy", @import("antfly_platform").time.monotonicNs()).kind);
    try std.testing.expectEqual(.healthy, routing.admit("http://ready", @import("antfly_platform").time.monotonicNs()).kind);
}
