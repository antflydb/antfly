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

//! Independent source calls use private contexts and response allocation. The
//! coordinator alone touches the caller's allocator or combines profiles.
const std = @import("std");
const local = @import("antfly_local_sources");
const scheduling = local.sql_parallel_scheduler;
const Response = @import("contextual_operations.zig").OwnedResponse;
const A = std.mem.Allocator;
pub const Request = struct { table: []const u8, body: []const u8 };
pub const Batch = struct {
    responses: []?Response,
    failed: ?usize = null,
    pub fn take(self: *Batch, index: usize) !Response {
        const response = self.responses[index] orelse return error.InvalidQueryRequest;
        self.responses[index] = null;
        return response;
    }
    pub fn deinit(self: *Batch, a: A) void {
        for (self.responses) |*response| if (response.*) |*owned| owned.deinit(a);
        a.free(self.responses);
    }
};
pub const Callbacks = struct {
    ptr: *anyopaque,
    prepare: *const fn (*anyopaque, []const u8) anyerror!*anyopaque,
    execute: *const fn (*anyopaque, A, []const u8, []const u8) anyerror!Response,
    cancel: *const fn (*anyopaque) void,
    finish: *const fn (*anyopaque) void,
};
const Wave = struct {
    callbacks: Callbacks,
    work: []Work,
    failed: std.atomic.Value(usize) = .init(std.math.maxInt(usize)),
    fn fail(self: *Wave, index: usize) void {
        if (self.failed.cmpxchgStrong(std.math.maxInt(usize), index, .acq_rel, .acquire) != null) return;
        // Cancel semantic request contexts before joining an earlier parked
        // provider. Preserve the initiating error instead of its siblings' 503s.
        for (self.work) |*work| self.callbacks.cancel(work.context);
    }
};
const Work = struct {
    wave: *Wave,
    context: *anyopaque,
    request: Request,
    allocator: A,
    index: usize,
    task: ?scheduling.Task(void) = null,
    response: ?Response = null,
    failure: ?anyerror = null,
    fn run(self: *Work) void {
        self.response = self.wave.callbacks.execute(self.context, self.allocator, self.request.table, self.request.body) catch |err| {
            self.failure = err;
            self.wave.fail(self.index);
            return;
        };
        if (self.response.?.status != 200) self.wave.fail(self.index);
    }
};
fn cloneResponse(a: A, response: Response) !Response {
    var result = response;
    result.body = try a.dupe(u8, response.body);
    errdefer a.free(result.body);
    result.headers = try a.alloc(@import("contextual_operations.zig").Header, response.headers.len);
    var copied: usize = 0;
    errdefer {
        for (result.headers[0..copied]) |*header| header.deinit(a);
        a.free(result.headers);
    }
    for (response.headers, result.headers) |header, *out| {
        out.name = try a.dupe(u8, header.name);
        errdefer a.free(out.name);
        out.value = try a.dupe(u8, header.value);
        copied += 1;
    }
    return result;
}
pub fn run(a: A, work_allocator: A, io: std.Io, requests: []const Request, callbacks: Callbacks, scheduler: *scheduling.Scheduler) !Batch {
    if (requests.len > 16) return error.InvalidQueryRequest;
    for (requests, 0..) |request, i| {
        if (request.table.len == 0) return error.InvalidQueryRequest;
        for (requests[0..i]) |prior| if (std.mem.eql(u8, prior.table, request.table)) return error.InvalidQueryRequest;
    }
    var result: Batch = .{ .responses = try a.alloc(?Response, requests.len) };
    @memset(result.responses, null);
    errdefer result.deinit(a);
    var start: usize = 0;
    var response_bytes: usize = 0;
    while (start < requests.len) : (start += 4) {
        var work: [4]Work = undefined;
        const count = @min(work.len, requests.len - start);
        var wave: Wave = .{ .callbacks = callbacks, .work = work[0..count] };
        var prepared: usize = 0;
        defer {
            for (work[0..prepared]) |*item| if (item.task) |*task| {
                callbacks.cancel(item.context);
                task.cancel(io);
                item.task = null;
            };
            for (work[0..prepared]) |*item| {
                if (item.response) |*response| response.deinit(work_allocator);
                callbacks.finish(item.context);
            }
        }
        for (work[0..count], requests[start..][0..count], 0..) |*item, request, i| {
            item.* = .{ .wave = &wave, .context = try callbacks.prepare(callbacks.ptr, request.table), .request = request, .allocator = work_allocator, .index = i };
            prepared += 1;
        }
        // Pull coordinators retain bounded workspace, not an I/O/CPU slot
        // needed by their children. Submit every lane before inline fallback.
        for (work[0..count]) |*item| item.task = scheduler.submitPullTransient(io, 32 * 1024 * 1024, Work.run, .{item});
        for (work[0..count]) |*item| if (item.task == null) item.run();
        for (work[0..count]) |*item| if (item.task) |*task| {
            task.await(io);
            item.task = null;
        };
        const failed = wave.failed.load(.acquire);
        if (failed == std.math.maxInt(usize)) for (work[0..count]) |*item| {
            // A canceled-before-start void future has no callback result.
            // Report cancellation rather than passing an incomplete page array.
            if (item.response == null) return error.Canceled;
        };
        if (failed < count) {
            if (work[failed].failure) |err| return err;
            result.failed = start + failed;
        }
        for (work[0..count], 0..) |*item, i| if (item.response) |response| {
            if (result.failed != null and i != failed) continue;
            if (response.headers.len > 256) return error.QueryCandidateBudgetExceeded;
            response_bytes = try std.math.add(usize, response_bytes, response.body.len);
            response_bytes = try std.math.add(usize, response_bytes, response.headers.len * @sizeOf(@import("contextual_operations.zig").Header));
            for (response.headers) |header| {
                response_bytes = try std.math.add(usize, response_bytes, header.name.len);
                response_bytes = try std.math.add(usize, response_bytes, header.value.len);
            }
            if (response_bytes > 32 * 1024 * 1024) return error.QueryCandidateBudgetExceeded;
            if (a.ptr == work_allocator.ptr and a.vtable == work_allocator.vtable) {
                result.responses[start + i] = response;
                item.response = null;
            } else result.responses[start + i] = try cloneResponse(a, response);
        };
        if (result.failed != null) break;
    }
    return result;
}

const Fixture = struct {
    sources: [2]Source = undefined,
    starts: std.atomic.Value(usize) = .init(0),
    finishes: usize = 0,
    barrier: bool = false,
    fail_second: bool = false,
    status_second: bool = false,
    headers: bool = false,
    const Source = struct { fixture: *Fixture, index: usize, canceled: std.atomic.Value(bool) = .init(false) };
    fn prepare(raw: *anyopaque, table: []const u8) !*anyopaque {
        const self: *Fixture = @ptrCast(@alignCast(raw));
        const index: usize = if (std.mem.eql(u8, table, "a")) 0 else 1;
        self.sources[index] = .{ .fixture = self, .index = index };
        return &self.sources[index];
    }
    fn execute(raw: *anyopaque, a: A, _: []const u8, _: []const u8) !Response {
        const source: *Source = @ptrCast(@alignCast(raw));
        const self = source.fixture;
        _ = self.starts.fetchAdd(1, .release);
        const deadline = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s;
        while (self.barrier and (self.starts.load(.acquire) != 2 or ((self.fail_second or self.status_second) and source.index == 0))) {
            if (source.canceled.load(.acquire)) return error.Canceled;
            if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.TestSourceBarrierTimeout;
            try std.testing.io.sleep(.fromMilliseconds(1), .awake);
        }
        if (self.fail_second and source.index == 1) return error.InjectedSourceFailure;
        var response = @import("contextual_operations.zig").jsonWithStatus(if (self.status_second and source.index == 1) 403 else 200, try a.dupe(u8, "result"), false);
        errdefer response.deinit(a);
        if (self.headers) {
            const headers = try a.alloc(@import("contextual_operations.zig").Header, 1);
            errdefer a.free(headers);
            const name = try a.dupe(u8, "x-source");
            errdefer a.free(name);
            headers[0] = .{ .name = name, .value = try a.dupe(u8, "retained") };
            response.headers = headers;
        }
        return response;
    }
    fn cancel(raw: *anyopaque) void {
        const source: *Source = @ptrCast(@alignCast(raw));
        source.canceled.store(true, .release);
    }
    fn finish(raw: *anyopaque) void {
        const source: *Source = @ptrCast(@alignCast(raw));
        source.fixture.finishes += 1;
    }
    fn callbacks(self: *Fixture) Callbacks {
        return .{ .ptr = self, .prepare = prepare, .execute = execute, .cancel = cancel, .finish = finish };
    }
};
const fixture_requests = [_]Request{ .{ .table = "a", .body = "{}" }, .{ .table = "b", .body = "{}" } };
test "external lake independent source waves overlap without touching a caller arena" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var fixture: Fixture = .{ .barrier = true };
    var scheduler: scheduling.Scheduler = .{};
    var batch = try run(arena.allocator(), std.heap.page_allocator, std.testing.io, &fixture_requests, fixture.callbacks(), &scheduler);
    defer batch.deinit(arena.allocator());
    try std.testing.expect(batch.failed == null);
    for (batch.responses) |response| try std.testing.expectEqualStrings("result", response.?.body);
    try std.testing.expectEqual(@as(usize, 2), fixture.finishes);
    try std.testing.expectEqual(@as(usize, 0), scheduler.pull_workers);
    try std.testing.expectEqual(@as(usize, 0), scheduler.pull_bytes);
}
test "external lake independent source waves cancel parked siblings and preserve the initiating failure" {
    for ([_]bool{ false, true }) |status| {
        var fixture: Fixture = .{ .barrier = true, .fail_second = !status, .status_second = status };
        var scheduler: scheduling.Scheduler = .{};
        if (status) {
            var batch = try run(std.testing.allocator, std.heap.page_allocator, std.testing.io, &fixture_requests, fixture.callbacks(), &scheduler);
            defer batch.deinit(std.testing.allocator);
            try std.testing.expectEqual(@as(?usize, 1), batch.failed);
            try std.testing.expectEqual(@as(u16, 403), batch.responses[1].?.status);
        } else try std.testing.expectError(error.InjectedSourceFailure, run(std.testing.allocator, std.heap.page_allocator, std.testing.io, &fixture_requests, fixture.callbacks(), &scheduler));
        try std.testing.expectEqual(@as(usize, 2), fixture.finishes);
        try std.testing.expectEqual(@as(usize, 0), scheduler.pull_workers);
        try std.testing.expectEqual(@as(usize, 0), scheduler.pull_bytes);
    }
}
test "external lake independent source waves decline to serial work and reject duplicate contexts" {
    var fixture: Fixture = .{};
    var scheduler: scheduling.Scheduler = .{ .max_pull_workers = 0 };
    var batch = try run(std.testing.allocator, std.heap.page_allocator, std.testing.io, &fixture_requests, fixture.callbacks(), &scheduler);
    defer batch.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 2), fixture.finishes);
    try std.testing.expectError(error.InvalidQueryRequest, run(std.testing.allocator, std.heap.page_allocator, std.testing.io, &.{ fixture_requests[0], fixture_requests[0] }, fixture.callbacks(), &scheduler));
}

test "external lake independent source waves unwind caller allocation failures after joining every worker" {
    const Case = struct {
        fn runCase(a: A) !void {
            var fixture: Fixture = .{ .headers = true };
            var scheduler: scheduling.Scheduler = .{ .max_pull_workers = 0 };
            var worker_allocator: scheduling.LockedAllocator = .{ .backing = std.testing.allocator };
            var batch = try run(a, worker_allocator.allocator(), std.testing.io, &fixture_requests, fixture.callbacks(), &scheduler);
            defer batch.deinit(a);
            for (batch.responses) |response| {
                try std.testing.expectEqualStrings("x-source", response.?.headers[0].name);
                try std.testing.expectEqualStrings("retained", response.?.headers[0].value);
            }
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Case.runCase, .{});
}
