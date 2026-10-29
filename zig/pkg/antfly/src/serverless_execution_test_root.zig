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

pub const antfly_sources = @import("source_owner_embedded_db.zig");
pub const consumer_tests_only = true;
const std = @import("std");
const Runnable = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
const handler_mod = @import("serverless/api/http_handler.zig");

test "serverless execution default queues preserve cancellation deadlines and protected health" {
    const Mode = enum { coarse_cancel, coarse_deadline, runnable_cancel };
    const Job = struct {
        handler: *handler_mod.HttpHandler,
        request: @import("serverless/api/http_types.zig").HttpRequest,
        done: std.atomic.Value(bool) = .init(false),
        failure: ?anyerror = null,
        fn run(self: *@This()) void {
            defer self.done.store(true, .release);
            var response = self.handler.handle(self.request) catch |err| {
                self.failure = err;
                return;
            };
            response.deinit(std.testing.allocator);
        }
        fn check(raw: *const anyopaque) !void {
            if (@as(*const std.atomic.Value(bool), @ptrCast(@alignCast(raw))).load(.acquire)) return error.Canceled;
        }
    };
    const io = std.testing.io;
    const config = @import("antfly_local_sources").common_config;
    for (std.enums.values(Mode)) |mode| {
        var scheduler = try Runnable.Scheduler.init(.{ .slots = 2, .max_wait_ms = 5000 });
        defer scheduler.close();
        var handler: handler_mod.HttpHandler = .{
            .alloc = std.testing.allocator,
            .api = undefined,
            .catalog = undefined,
            .manifests = undefined,
            .progress = undefined,
            .query = undefined,
            .runtime_status = undefined,
            .io = io,
            .runnable_scheduler = &scheduler,
            // POST reads must use one query slot, not this impossible write team.
            .write_runnable_slots = 2,
        };
        var waiting = config.default_query_waiting;
        if (mode == .coarse_deadline) waiting.max_wait_ms = 5000;
        try handler.configureAdmissionWithWaiting(1, 1, waiting);
        var held: ?@import("antfly_local_sources").common_request_admission.RequestAdmission.Lease = if (mode != .runnable_cancel) handler.query_admission.tryAcquireLease().? else null;
        defer if (held) |*lease| lease.release();
        var ordinary: Runnable.Execution = .{};
        defer ordinary.finish();
        if (mode == .runnable_cancel) try ordinary.start(&scheduler, .{ .io = io, .allow_wait = false }, .query);
        var canceled: std.atomic.Value(bool) = .init(false);
        var job: Job = .{ .handler = &handler, .request = .{
            .method = .post,
            .path = "/tables/waiting/query",
            .body = "{}",
            .cancellation = .{ .ptr = &canceled, .check_fn = Job.check },
            .deadline_ns = if (mode == .coarse_deadline) @import("antfly_platform").time.monotonicNs() + std.time.ns_per_s else null,
        } };
        var future = try io.concurrent(Job.run, .{&job});
        defer future.cancel(io);
        const until = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(2));
        while (if (mode == .runnable_cancel) scheduler.stats().queued == 0 else handler.query_admission.stats().queued == 0) {
            if (job.done.load(.acquire)) return job.failure orelse error.RequestDidNotQueue;
            if (std.Io.Clock.awake.now(io).nanoseconds >= until.nanoseconds) return error.RequestDidNotQueue;
            try io.sleep(.fromMilliseconds(1), .awake);
        }
        var health = try handler.handle(.{ .method = .get, .path = "/healthz" });
        try std.testing.expectEqual(@as(u16, 200), health.status);
        health.deinit(std.testing.allocator);
        if (mode != .coarse_deadline) canceled.store(true, .release);
        future.await(io);
        try std.testing.expectEqual(if (mode == .coarse_deadline) error.DeadlineExceeded else error.Canceled, job.failure.?);
        if (held) |*lease| lease.release();
        ordinary.finish();
        try std.testing.expectEqual(@as(usize, 0), handler.query_admission.stats().queued);
        try std.testing.expectEqual(@as(usize, 0), handler.query_admission.stats().in_flight);
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
    }
}
const bootstrap = @import("serverless/runtime/bootstrap.zig");

test "serverless execution policy validates shared managed reserves" {
    var cfg: bootstrap.BootstrapConfig = .{
        .artifacts_uri = "file:///tmp/artifacts",
        .manifests_uri = "file:///tmp/manifests",
        .wal_uri = "file:///tmp/wal",
        .progress_uri = "file:///tmp/progress",
        .catalog_uri = "file:///tmp/catalog",
        .runnable_policy = .{ .slots = 3, .cleanup_slots = 1 },
    };
    try bootstrap.validateConfig(std.testing.allocator, cfg);
    cfg.runnable_policy = .{ .slots = 2 };
    try std.testing.expectError(error.ManagerCleanupCapacityUnavailable, bootstrap.validateConfig(std.testing.allocator, cfg));
}

test "serverless execution handler protects bounded health under ordinary saturation" {
    var scheduler = try Runnable.Scheduler.init(.{ .slots = 3, .cleanup_slots = 1 });
    defer scheduler.close();
    var ordinary: Runnable.Execution = .{};
    try ordinary.start(&scheduler, .{ .io = std.testing.io, .allow_wait = false }, .query);
    defer ordinary.finish();
    var handler: handler_mod.HttpHandler = .{
        .alloc = std.testing.allocator,
        .api = undefined,
        .catalog = undefined,
        .manifests = undefined,
        .progress = undefined,
        .query = undefined,
        .runtime_status = undefined,
        .io = std.testing.io,
        .runnable_scheduler = &scheduler,
    };
    var response = try handler.handle(.{ .method = .get, .path = "/healthz" });
    defer response.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), response.status);
    try std.testing.expectEqual(@as(usize, 2), scheduler.stats().operations);
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(u64, handler_mod.HttpHandler.requestRetainedBytes()), scheduler.stats().retained_bytes);
    try std.testing.expectError(error.DeadlineExceeded, handler.handle(.{ .method = .get, .path = "/healthz", .deadline_ns = 0 }));
    const Probe = struct {
        calls: usize = 0,
        fn check(raw: *const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.calls += 1;
            if (self.calls == 4) return error.CatalogGenerationChanged;
        }
    };
    var probe: Probe = .{};
    try std.testing.expectError(error.CatalogGenerationChanged, handler.handle(.{ .method = .get, .path = "/healthz", .cancellation = .{ .ptr = &probe, .check_fn = Probe.check } }));
    try std.testing.expectEqual(@as(usize, 4), probe.calls);
    try std.testing.expectEqual(@as(usize, 2), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, handler_mod.HttpHandler.requestRetainedBytes()), scheduler.stats().retained_bytes);
    scheduler.close();
    try std.testing.expectError(error.SchedulerClosed, handler.handle(.{ .method = .get, .path = "/healthz" }));
    ordinary.finish();
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
}

test "serverless execution background ownership retires before canceled work reaches runtime mutex" {
    const Runtime = @import("serverless/runtime/manager.zig").ManagedRuntime;
    var scheduler = try Runnable.Scheduler.init(.{ .slots = 3, .cleanup_slots = 1 });
    defer scheduler.close();
    const Probe = struct {
        scheduler: *Runnable.Scheduler,
        saw_grant: bool = false,
        fn check(raw: *const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            if (!self.scheduler.mutex.tryLock()) return;
            self.scheduler.mutex.unlock();
            if (self.scheduler.stats().runnable_slots != 0) {
                self.saw_grant = true;
                return error.Canceled;
            }
        }
    };
    var probe: Probe = .{ .scheduler = &scheduler };
    var runtime: Runtime = .{ .alloc = std.testing.allocator, .io = std.testing.io, .cfg = .{}, .catalog = undefined, .publisher = undefined, .pruner = undefined, .runnable_scheduler = &scheduler };
    try std.testing.expectError(error.Canceled, runtime.runOnceWithCancellation(.{ .ptr = &probe, .check_fn = Probe.check }));
    try std.testing.expect(probe.saw_grant);
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
    try std.testing.expect(runtime.run_mu.tryLock());
    runtime.run_mu.unlock();
}

test "serverless execution accepted ingest yields to a queued query and preserves protected control" {
    const Service = @import("serverless/api/service.zig").Service;
    const Wal = @import("serverless/wal/store.zig").WalStore;
    var scheduler = try Runnable.Scheduler.init(.{ .slots = 3, .cleanup_slots = 1, .checkpoint_budget = 1 });
    defer scheduler.close();
    const State = struct {
        scheduler: *Runnable.Scheduler,
        execution: *Runnable.Execution,
        appended: std.atomic.Value(u64) = .init(0),
        query_done: std.atomic.Value(bool) = .init(false),
        query_go: std.Io.Event = .unset,
        fn query(self: *@This()) !void {
            try self.query_go.wait(std.testing.io);
            var query_execution: Runnable.Execution = .{};
            defer query_execution.finish();
            try query_execution.start(self.scheduler, .{ .io = std.testing.io, .native_now_ns = @import("antfly_platform").time.monotonicNs, .deadline_ns = @import("antfly_platform").time.monotonicNs() + std.time.ns_per_s }, .query);
            try std.testing.expectEqual(@as(u64, 1), self.appended.load(.acquire));
            self.query_done.store(true, .release);
        }
        fn canceled(raw: *const anyopaque) bool {
            return @as(*const @This(), @ptrCast(@alignCast(raw))).appended.load(.acquire) != 0;
        }
        fn append(raw: *anyopaque, _: []const u8, _: u64, _: []const u8) !u64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(self.execution.entry.?.completion_active);
            try std.testing.expectEqual(Runnable.Class.write, try self.execution.retainedClass());
            try std.testing.expect(self.execution.entry.?.options.cancellation == null);
            const lsn = self.appended.fetchAdd(1, .acq_rel) + 1;
            if (lsn == 1) {
                // A bounded health root runs while the accepted write owns
                // the sole ordinary slot. It does not retire the write root.
                var control: Runnable.Execution = .{};
                defer control.finish();
                try control.start(self.scheduler, .{ .io = std.testing.io, .allow_wait = false }, .control);
                try std.testing.expectEqual(@as(usize, 2), self.scheduler.stats().runnable_slots);
                self.query_go.set(std.testing.io);
                const deadline = @import("antfly_platform").time.monotonicNs() + std.time.ns_per_s;
                while (self.scheduler.stats().queued == 0) {
                    if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.TestUnexpectedResult;
                    try std.testing.io.sleep(.fromMilliseconds(1), .awake);
                }
            } else try std.testing.expect(self.query_done.load(.acquire));
            return lsn;
        }
    };
    var execution: Runnable.Execution = .{};
    defer execution.finish();
    var state: State = .{ .scheduler = &scheduler, .execution = &execution };
    try execution.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = Service.completionRetainedBytes(), .cancellation = .{ .ptr = &state, .requested = State.canceled } }, .write);
    var query_task = try std.testing.io.concurrent(State.query, .{&state});
    var query_joined = false;
    defer if (!query_joined) {
        state.query_go.set(std.testing.io);
        _ = query_task.cancel(std.testing.io) catch {};
    };
    var wal: Wal = .{ .allocator = std.testing.allocator, .ptr = &state, .vtable = &.{ .deinit = undefined, .append = State.append, .read_from_alloc = undefined, .latest_lsn = undefined, .truncate_prefix = undefined } };
    var service: Service = .{ .alloc = std.testing.allocator, .wal = &wal, .builder = undefined };
    var result = try service.ingestBatchWithExecution(.{ .namespace = "docs", .timestamp_ns = 1, .mutations = &.{ .{ .kind = .upsert, .doc_id = "a", .body = "alpha" }, .{ .kind = .upsert, .doc_id = "b", .body = "beta" } } }, .none, &execution, std.testing.io);
    defer result.deinit(std.testing.allocator);
    try query_task.await(std.testing.io);
    query_joined = true;
    try std.testing.expectEqual(@as(u64, 2), result.end_lsn);
    try std.testing.expect(state.query_done.load(.acquire));
    try std.testing.expect(scheduler.stats().yields > 0);
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
    execution.finish();
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
}

test "serverless execution completion refuses missing retained metadata before WAL publication" {
    const Service = @import("serverless/api/service.zig").Service;
    var scheduler = try Runnable.Scheduler.init(.{ .slots = 3, .cleanup_slots = 1 });
    defer scheduler.close();
    var execution: Runnable.Execution = .{};
    defer execution.finish();
    try execution.start(&scheduler, .{ .io = std.testing.io }, .write);
    var service: Service = .{ .alloc = std.testing.allocator, .wal = undefined, .builder = undefined };
    try std.testing.expectError(error.InvalidRunnableDemand, service.ingestBatchWithExecution(.{ .namespace = "docs", .timestamp_ns = 1, .mutations = &.{} }, .none, &execution, std.testing.io));
    try std.testing.expect(!execution.entry.?.completion_active);
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
    execution.finish();
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
}

test "serverless execution filesystem WAL contention loans CPU and rejects before publication" {
    const Fs = @import("serverless/wal/fs_store.zig").FsStore;
    const Wal = @import("serverless/wal/store.zig").WalStore;
    const Case = enum { success, canceled, deadline, closed };
    const State = struct {
        store: *Fs,
        wal: *Wal,
        scheduler: *Runnable.Scheduler,
        execution: *Runnable.Execution,
        case: Case,
        cancellation: std.atomic.Value(bool) = .init(false),
        released: std.atomic.Value(bool) = .init(false),
        query_ran: std.atomic.Value(bool) = .init(false),
        var clock: std.atomic.Value(u64) = .init(0);
        fn now() u64 {
            return clock.load(.acquire);
        }
        fn canceled(raw: *const anyopaque) bool {
            return @as(*const @This(), @ptrCast(@alignCast(raw))).cancellation.load(.acquire);
        }
        fn append(self: *@This()) !u64 {
            return self.wal.appendWithExecution("docs", 1, "payload", self.execution);
        }
        fn query(self: *@This()) !void {
            const deadline = @import("antfly_platform").time.monotonicNs() + std.time.ns_per_s;
            while (self.scheduler.stats().resource_blocked == 0) {
                if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.TestUnexpectedResult;
                try std.testing.io.sleep(.fromMilliseconds(1), .awake);
            }
            var query_execution: Runnable.Execution = .{};
            defer query_execution.finish();
            try query_execution.start(self.scheduler, .{ .io = std.testing.io, .native_now_ns = @import("antfly_platform").time.monotonicNs, .deadline_ns = deadline }, .query);
            // The writer owns no CPU or mutex lease while reentering. A
            // legacy holder can release serialization using ordinary CPU.
            if (self.store.mutex.tryLock()) {
                self.store.mutex.unlock();
                return error.TestUnexpectedResult;
            }
            try std.testing.expectEqual(@as(usize, 1), self.scheduler.stats().runnable_slots);
            self.query_ran.store(true, .release);
            switch (self.case) {
                .success => {},
                .canceled => self.cancellation.store(true, .release),
                .deadline => clock.store(100, .release),
                .closed => self.scheduler.close(),
            }
            if (!self.released.swap(true, .acq_rel)) self.store.mutex.unlock();
        }
    };
    for (std.enums.values(Case)) |case| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/wal-contention", .{tmp.sub_path});
        defer std.testing.allocator.free(path);
        var store = try Fs.init(std.testing.allocator, path);
        defer store.deinit();
        var wal = store.walStore();
        var scheduler = try Runnable.Scheduler.init(.{ .slots = 3, .cleanup_slots = 1, .checkpoint_budget = 1 });
        defer scheduler.close();
        var execution: Runnable.Execution = .{};
        defer execution.finish();
        State.clock.store(0, .release);
        var state: State = .{ .store = &store, .wal = &wal, .scheduler = &scheduler, .execution = &execution, .case = case };
        try execution.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = State.now, .deadline_ns = 100, .retained_bytes = 17, .cancellation = .{ .ptr = &state, .requested = State.canceled } }, .write);
        try std.testing.expect(store.mutex.tryLock());
        defer if (!state.released.load(.acquire)) store.mutex.unlock();
        var writer = try std.testing.io.concurrent(State.append, .{&state});
        var writer_joined = false;
        defer if (!writer_joined) {
            if (!state.released.swap(true, .acq_rel)) store.mutex.unlock();
            _ = writer.cancel(std.testing.io) catch {};
        };
        var query = try std.testing.io.concurrent(State.query, .{&state});
        var query_joined = false;
        defer if (!query_joined) {
            if (!state.released.swap(true, .acq_rel)) store.mutex.unlock();
            _ = query.cancel(std.testing.io) catch {};
        };
        try query.await(std.testing.io);
        query_joined = true;
        const result = writer.await(std.testing.io);
        writer_joined = true;
        switch (case) {
            .success => try std.testing.expectEqual(@as(u64, 1), try result),
            .canceled => try std.testing.expectError(error.Canceled, result),
            .deadline => try std.testing.expectError(error.DeadlineExceeded, result),
            .closed => try std.testing.expectError(error.SchedulerClosed, result),
        }
        try std.testing.expect(state.query_ran.load(.acquire));
        try std.testing.expectEqual(@as(u64, if (case == .success) 1 else 0), try store.latestLsn("docs"));
        try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 17), scheduler.stats().retained_bytes);
        try std.testing.expect(store.mutex.tryLock());
        store.mutex.unlock();
        execution.finish();
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().runnable_slots);
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().resource_blocked);
        try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
    }
}

test "serverless execution listener serves health over H2 while ordinary handlers are saturated" {
    const httpx = @import("httpx");
    const listener = @import("serverless/server.zig");
    const Types = @import("serverless/api/types.zig");
    const State = struct {
        entered: std.atomic.Value(usize) = .init(0),
        release: std.Io.Event = .unset,
        handler: *handler_mod.HttpHandler,
        fn blocked(self: *@This(), ctx: *httpx.Context) !httpx.Response {
            _ = self.entered.fetchAdd(1, .acq_rel);
            try self.release.wait(ctx.io);
            return ctx.text("done");
        }
        fn health(self: *@This(), ctx: *httpx.Context) !httpx.Response {
            var result = try self.handler.handle(.{ .method = .get, .path = ctx.request.uri.path });
            defer result.deinit(std.testing.allocator);
            return httpx.Response.fromText(ctx.allocator, result.status, result.body);
        }
        fn request(peer: *httpx.H2Connection, socket: *httpx.Socket, path: []const u8) !*httpx.Stream {
            const stream = try peer.stream_manager.createStream();
            const headers = try httpx.H2Connection.buildRequestHeaders("GET", path, "http", "localhost", &.{}, std.testing.allocator);
            defer std.testing.allocator.free(headers);
            try peer.sendHeaders(socket, stream.id, headers, true);
            return stream;
        }
    };
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    const startup = try bootstrap.resolveStartupConfig(.{ .artifacts_uri = "file:///tmp/a", .manifests_uri = "file:///tmp/m", .wal_uri = "file:///tmp/w", .progress_uri = "file:///tmp/p", .catalog_uri = "file:///tmp/c", .runnable_policy = .{ .capacity_mode = .automatic, .slots = 4 }, .process_memory_limit_bytes = 64 << 20 });
    var scheduler = try Runnable.Scheduler.init(startup.runnable_policy.?);
    defer scheduler.close();
    var ordinary: Runnable.Execution = .{};
    defer ordinary.finish();
    try ordinary.start(&scheduler, .{ .io = io, .allow_wait = false }, .query);
    var status: Types.RuntimeStatusResult = .{ .role = .combined, .tick_interval_ms = 1, .validated = true, .targets = &.{} };
    var handler: handler_mod.HttpHandler = .{ .alloc = std.testing.allocator, .api = undefined, .catalog = undefined, .manifests = undefined, .progress = undefined, .query = undefined, .runtime_status = &status, .io = io, .runnable_scheduler = &scheduler };
    var state: State = .{ .handler = &handler };
    const cfg = listener.listenerHttpxConfigWithMemory(.{ .max_connections = 4, .max_request_tasks = 3 }, startup, null);
    try std.testing.expectEqual(@as(u32, 2), cfg.protected_request_tasks);
    var server = httpx.Server.initWithConfig(std.testing.allocator, io, cfg);
    defer server.deinit();
    try server.get("/blocked", httpx.Handler.bind(&state, State.blocked));
    try server.get("/healthz", httpx.Handler.bind(&state, State.health));
    try server.get("/readyz", httpx.Handler.bind(&state, State.health));
    var task = httpx.Server.ListenerTask.init(&server);
    try task.start();
    defer {
        server.stop();
        task.join() catch {};
    }
    defer state.release.set(io);
    var socket = try httpx.Socket.connect(server.boundAddress().?, io);
    defer socket.close();
    try socket.setRecvTimeout(5000);
    var peer = httpx.H2Connection.initClient(std.testing.allocator, io);
    defer peer.deinit();
    try peer.sendClientPreface(&socket);
    const blocked = try State.request(&peer, &socket, "/blocked");
    const deadline = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s;
    while (state.entered.load(.acquire) == 0) {
        if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.TestUnexpectedResult;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    const pressure = server.application_memory.capacity - server.application_memory.in_use.load(.acquire);
    try std.testing.expect(server.application_memory.tryReserve(pressure));
    var pressure_held = true;
    defer if (pressure_held) server.application_memory.release(pressure);
    try std.testing.expectEqual(server.application_memory.capacity, server.application_memory.in_use.load(.acquire));
    const denied = try State.request(&peer, &socket, "/blocked");
    while (denied.stream_error == null) _ = try peer.processOneFrame(&socket, &socket);
    peer.stream_manager.removeStream(denied.id);
    for ([_][]const u8{ "/healthz", "/readyz" }) |path| {
        const control = try State.request(&peer, &socket, path);
        while (!control.completed) _ = try peer.processOneFrame(&socket, &socket);
        try std.testing.expect(control.stream_error == null);
        try std.testing.expect(std.mem.indexOf(u8, control.data_buf.items, if (std.mem.eql(u8, path, "/healthz")) "ok" else "ready") != null);
        peer.stream_manager.removeStream(control.id);
    }
    try std.testing.expectEqual(@as(usize, 1), state.entered.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
    server.application_memory.release(pressure);
    pressure_held = false;
    state.release.set(io);
    while (!blocked.completed) _ = try peer.processOneFrame(&socket, &socket);
    try std.testing.expectEqualStrings("done", blocked.data_buf.items);
    ordinary.finish();
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
}

test "serverless execution H2 slow readers preserve owners and allow queries and controls" {
    const Httpx = @import("httpx");
    const Listener = @import("serverless/server.zig");
    const Adapter = @import("serverless_http_server.zig").ServerlessHttpServer;
    const Types = @import("serverless/api/types.zig");
    const Memory = @import("common/allocation_budget.zig").Partitioned;
    const Case = enum { resumed, reset, disconnect, unacknowledged_disconnect, silent };
    const Helper = struct {
        fn request(peer: *Httpx.H2Connection, socket: *Httpx.Socket, path: []const u8, credit: bool) !*Httpx.Stream {
            const stream = try peer.stream_manager.createStream();
            const headers = try Httpx.H2Connection.buildRequestHeaders("GET", path, "http", "localhost", &.{}, std.testing.allocator);
            defer std.testing.allocator.free(headers);
            try peer.sendHeaders(socket, stream.id, headers, true);
            if (credit) try peer.sendWindowUpdate(socket, stream.id, 8192);
            return stream;
        }
        fn waitRetired(io: std.Io, scheduler: *Runnable.Scheduler, memory: *Memory, expected: usize, disconnected: ?*const Httpx.Server) !void {
            const until = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s;
            while (scheduler.stats().operations != expected or
                (expected == 0 and (memory.foreground.stats().used_bytes != 0 or memory.control.stats().used_bytes != 0)) or
                (if (disconnected) |server| server.runtimeStats().active_connections != 0 else false))
            {
                if (@import("antfly_platform").time.monotonicNs() >= until) {
                    std.debug.print("slow-output retirement timeout: operations {d}, output {d}, foreground {d}, control {d}\n", .{ scheduler.stats().operations, scheduler.stats().output_blocked, memory.foreground.stats().used_bytes, memory.control.stats().used_bytes });
                    return error.TestUnexpectedResult;
                }
                try io.sleep(.fromMilliseconds(1), .awake);
            }
        }
    };
    const a = std.testing.allocator;
    for (std.meta.tags(Case)) |case| {
        std.debug.print("slow-output case {s}: start\n", .{@tagName(case)});
        var io_impl = std.Io.Threaded.init(a, .{});
        defer io_impl.deinit();
        const io = io_impl.io();
        var scheduler = try Runnable.Scheduler.init(.{ .slots = 3, .cleanup_slots = 1 });
        defer scheduler.close();
        var memory: Memory = .{
            .foreground = .{ .child = a, .limit = 4 << 20 },
            .control = .{ .child = a, .limit = 64 << 10 },
        };
        var status: Types.RuntimeStatusResult = .{ .role = .combined, .tick_interval_ms = 1, .validated = true, .targets = &.{} };
        var handler: handler_mod.HttpHandler = .{
            .alloc = memory.allocator(false),
            .control_allocator = memory.allocator(true),
            .api = undefined,
            .catalog = undefined,
            .manifests = undefined,
            .progress = undefined,
            .query = undefined,
            .runtime_status = &status,
            .io = io,
            .runnable_scheduler = &scheduler,
        };
        var adapter = Adapter.init(memory.allocator(false), .{}, &handler);
        var cfg = Listener.listenerHttpxConfig(.{ .max_connections = 4, .max_request_tasks = 4 }, null);
        cfg.protected_request_tasks = 2;
        if (case == .unacknowledged_disconnect or case == .silent) {
            // No terminal packet need reach a server after abandonment. Keep
            // the original finite request/idle ceilings authoritative when
            // abort is unobserved, including a peer that never closes at all.
            cfg.request_timeout_ms = 1500;
            cfg.h2_idle_timeout_ms = 250;
        }
        cfg.application_allocator = memory.allocator(false);
        cfg.protected_application_allocator = memory.allocator(true);
        cfg.application_memory_budget_bytes = 3 << 20;
        cfg.protected_application_memory_bytes = 32 << 10;
        var server = Httpx.Server.initWithConfig(a, io, cfg.normalized());
        defer server.deinit();
        server.global(Httpx.Handler.bind(&adapter, Adapter.handleHttpx));
        var task = Httpx.Server.ListenerTask.init(&server);
        try task.start();
        defer {
            server.stop();
            task.join() catch {};
        }
        var socket = try Httpx.Socket.connect(server.boundAddress().?, io);
        var socket_live = true;
        defer if (socket_live) socket.close();
        try socket.setRecvTimeout(5000);
        var peer = Httpx.H2Connection.initClient(a, io);
        defer peer.deinit();
        peer.local_settings.initial_window_size = 0;
        try peer.sendClientPreface(&socket);
        const slow = try Helper.request(&peer, &socket, "/status", false);
        while (!slow.got_headers) _ = try peer.processOneFrame(&socket, &socket);
        std.debug.print("slow-output case {s}: stalled headers, content length {any}\n", .{ @tagName(case), slow.content_length });
        try Helper.waitRetired(io, &scheduler, &memory, 1, null);
        try std.testing.expect(!slow.completed);
        try std.testing.expectEqual(@as(usize, 0), slow.data_buf.items.len);
        try std.testing.expectEqual(@as(usize, 1), scheduler.stats().output_blocked);
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().runnable_slots);
        try std.testing.expect(memory.foreground.stats().used_bytes > 0);

        const query = try Helper.request(&peer, &socket, "/status", true);
        while (!query.got_headers and !query.completed) _ = try peer.processOneFrame(&socket, &socket);
        std.debug.print("slow-output case {s}: query headers, content length {any}, operations {d}\n", .{ @tagName(case), query.content_length, scheduler.stats().operations });
        while (!query.completed) _ = try peer.processOneFrame(&socket, &socket);
        std.debug.print("slow-output case {s}: query complete\n", .{@tagName(case)});
        try std.testing.expect(query.stream_error == null);
        const expected = try a.dupe(u8, query.data_buf.items);
        defer a.free(expected);
        var parsed = try std.json.parseFromSlice(struct { validated: bool, role: []const u8 }, a, expected, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        try std.testing.expect(parsed.value.validated);
        try std.testing.expectEqualStrings("combined", parsed.value.role);
        peer.stream_manager.removeStream(query.id);
        for ([_][]const u8{ "/healthz", "/readyz" }) |path| {
            const control = try Helper.request(&peer, &socket, path, true);
            while (!control.completed) _ = try peer.processOneFrame(&socket, &socket);
            std.debug.print("slow-output case {s}: control {s} complete\n", .{ @tagName(case), path });
            try std.testing.expect(control.stream_error == null);
            try std.testing.expect(std.mem.indexOf(u8, control.data_buf.items, if (std.mem.eql(u8, path, "/healthz")) "ok" else "ready") != null);
            peer.stream_manager.removeStream(control.id);
        }
        try Helper.waitRetired(io, &scheduler, &memory, 1, null);
        try std.testing.expect(!slow.completed);
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().runnable_slots);
        switch (case) {
            .resumed => {
                try peer.sendWindowUpdate(&socket, slow.id, 8192);
                while (!slow.completed) _ = try peer.processOneFrame(&socket, &socket);
                try std.testing.expect(slow.stream_error == null);
                try std.testing.expectEqualStrings(expected, slow.data_buf.items);
            },
            .reset => try peer.sendRstStream(&socket, slow.id, .cancel),
            .disconnect, .unacknowledged_disconnect => {
                // Abort at an acknowledged frame boundary. Closing the local
                // descriptor with queued control frames alone does not prove
                // the peer observed disconnect (the socket can stay live).
                if (case == .disconnect) {
                    try peer.sendPing(&socket, "closing!".*);
                    while (!peer.ping_ack_event.isSet()) _ = try peer.processOneFrame(&socket, &socket);
                    try std.testing.expectEqual(@as(usize, 0), slow.data_buf.items.len);
                    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().output_blocked);
                }
                socket.abortRequest();
                socket.close();
                socket_live = false;
            },
            .silent => {}, // Leave the transport open with no response credit.
        }
        const disconnected = case == .disconnect or case == .unacknowledged_disconnect or case == .silent;
        try Helper.waitRetired(io, &scheduler, &memory, 0, if (disconnected) &server else null);
        if (disconnected) try std.testing.expectEqual(@as(usize, 0), server.runtimeStats().active_requests);
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().output_blocked);
        try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
    }
}

test "serverless execution startup freezes one process-clamped scheduling policy" {
    var cfg: bootstrap.BootstrapConfig = .{ .artifacts_uri = "file:///tmp/a", .manifests_uri = "file:///tmp/m", .wal_uri = "file:///tmp/w", .progress_uri = "file:///tmp/p", .catalog_uri = "file:///tmp/c", .runnable_policy = .{ .capacity_mode = .automatic, .slots = 16 }, .process_memory_limit_bytes = 64 << 20 };
    const effective = (try bootstrap.resolveRunnablePolicy(cfg)).?;
    try std.testing.expectEqual(@as(@TypeOf(effective.capacity_mode), .fixed), effective.capacity_mode);
    try std.testing.expect(effective.max_bytes <= 2 << 20);
    try std.testing.expect(effective.slots >= 3 and effective.slots <= 16);
    try std.testing.expectEqual(@as(usize, 1), effective.cleanup_slots);
    cfg.runnable_policy = effective;
    cfg.process_memory_limit_bytes = 1;
    try std.testing.expectEqualDeep(effective, (try bootstrap.resolveRunnablePolicy(cfg)).?);
    cfg.runnable_policy = .{ .capacity_mode = .automatic };
    cfg.process_memory_limit_bytes = 1 << 20;
    try std.testing.expectError(error.InvalidRunnablePolicy, bootstrap.resolveRunnablePolicy(cfg));
    cfg.runnable_policy = null;
    try std.testing.expect((try bootstrap.resolveRunnablePolicy(cfg)) == null);
}

test {
    _ = @import("serverless_main.zig");
}

test "serverless execution frontend quotas share the frozen startup envelope" {
    const listener = @import("serverless/server.zig");
    const requested: bootstrap.BootstrapConfig = .{ .artifacts_uri = "file:///tmp/a", .manifests_uri = "file:///tmp/m", .wal_uri = "file:///tmp/w", .progress_uri = "file:///tmp/p", .catalog_uri = "file:///tmp/c", .runnable_policy = .{ .capacity_mode = .automatic }, .process_memory_limit_bytes = 64 << 20 };
    const effective = try bootstrap.resolveStartupConfig(requested);
    try std.testing.expect(effective.max_request_memory_bytes <= 8 << 20);
    try std.testing.expect(effective.max_retained_memory_bytes <= 8 << 20);
    var frozen = effective;
    frozen.process_memory_limit_bytes = 1;
    try std.testing.expectEqualDeep(frozen, try bootstrap.resolveStartupConfig(frozen));
    const cfg = listener.listenerHttpxConfigWithMemory(.{}, effective, null);
    try std.testing.expect(cfg.max_body_size <= 4 << 20);
    try std.testing.expect(cfg.max_body_size * 2 <= cfg.request_body_buffer_budget_bytes);
    try std.testing.expect(cfg.request_body_buffer_budget_bytes <= effective.max_request_memory_bytes);
    try std.testing.expect(cfg.application_memory_budget_bytes <= effective.max_request_memory_bytes);
    try std.testing.expect(cfg.protected_application_memory_bytes > 0);
    try std.testing.expect(cfg.protected_application_memory_bytes <= cfg.application_memory_budget_bytes / 8);
    // An advertised maximum body must fit an idle ordinary root, including
    // the largest request-local completion metadata charged by ingress.
    const metadata = handler_mod.HttpHandler.requestRetainedBytes() + @import("serverless/api/service.zig").Service.completionRetainedBytes();
    var scheduler = try Runnable.Scheduler.init(effective.runnable_policy.?);
    defer scheduler.close();
    var execution: Runnable.Execution = .{};
    defer execution.finish();
    try execution.start(&scheduler, .{ .io = std.testing.io, .allow_wait = false, .retained_bytes = cfg.max_body_size + metadata }, .write);
    execution.finish();
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
    var smaller = requested;
    smaller.max_request_memory_bytes = 2 << 20;
    const capped = listener.listenerHttpxConfigWithMemory(.{}, try bootstrap.resolveStartupConfig(smaller), null);
    const read_reserved = if (effective.runnable_policy.?.readIsolation()) @import("common/read_memory.zig").startupReservedBytes((2 << 20) - (2 << 20) / 8) else 0;
    try std.testing.expectEqual(@as(usize, (2 << 20) - read_reserved) / 2, capped.max_body_size);
    try std.testing.expectEqual(@as(usize, (2 << 20) - read_reserved), capped.application_memory_budget_bytes);
    var tiny = requested;
    tiny.process_memory_limit_bytes = 4 << 20;
    try std.testing.expectError(error.InvalidConfig, bootstrap.resolveStartupConfig(tiny));
    var disabled = requested;
    disabled.runnable_policy = null;
    disabled.process_memory_limit_bytes = 1;
    try std.testing.expectEqualDeep(disabled, try bootstrap.resolveStartupConfig(disabled));
}

test "serverless execution memory owner preserves retained provenance and protected acknowledgements" {
    const Memory = @import("common/allocation_budget.zig").Partitioned;
    var memory: Memory = .{ .foreground = .{ .child = std.testing.allocator, .limit = 4096 }, .control = .{ .child = std.testing.allocator, .limit = 4096 }, .retained = .{ .child = std.testing.allocator, .limit = 4096 } };
    const foreground = memory.allocator(false);
    const control = memory.allocator(true);
    const retained = memory.retainedAllocator();
    const stored = try retained.alloc(u8, 512);
    const grown = try foreground.realloc(stored, 1024);
    control.free(grown);
    try std.testing.expectEqual(@as(usize, 0), memory.retained.?.stats().used_bytes);
    const pressure = try foreground.alloc(u8, 4096 - Memory.overhead(.of(u8)));
    defer foreground.free(pressure);
    try std.testing.expectError(error.OutOfMemory, foreground.alloc(u8, 1));
    var handler: handler_mod.HttpHandler = .{ .alloc = foreground, .control_allocator = control, .api = undefined, .catalog = undefined, .manifests = undefined, .progress = undefined, .query = undefined, .runtime_status = undefined };
    var response = try handler.handle(.{ .method = .get, .path = "/healthz" });
    try std.testing.expectEqual(@as(u16, 200), response.status);
    try std.testing.expect(memory.control.stats().used_bytes > 0);
    response.deinit(foreground);
    try std.testing.expectEqual(@as(usize, 0), memory.control.stats().used_bytes);
}

test "serverless execution response owner retains output metadata without retaining CPU" {
    const Server = @import("serverless_http_server.zig").ServerlessHttpServer;
    const Httpx = @import("httpx");
    const a = std.testing.allocator;
    var scheduler = try Runnable.Scheduler.init(.{ .slots = 3, .cleanup_slots = 1 });
    defer scheduler.close();
    var handler: handler_mod.HttpHandler = .{
        .alloc = a,
        .api = undefined,
        .catalog = undefined,
        .manifests = undefined,
        .progress = undefined,
        .query = undefined,
        .runtime_status = undefined,
        .io = std.testing.io,
        .runnable_scheduler = &scheduler,
    };
    var server = Server.init(a, .{}, &handler);
    var reply = try server.handle(.{ .method = .GET, .uri = "/healthz" });
    var reply_live = true;
    defer if (reply_live) reply.deinit(a);
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().output_blocked);
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(u64, handler_mod.HttpHandler.requestRetainedBytes()), scheduler.stats().retained_bytes);
    var query: Runnable.Execution = .{};
    try query.start(&scheduler, .{ .io = std.testing.io, .allow_wait = false }, .query);
    query.finish();
    reply.deinit(a);
    reply_live = false;
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);

    var request = try Httpx.Request.init(a, .GET, "/healthz");
    defer request.deinit();
    var ctx = Httpx.Context.init(a, std.testing.io, &request);
    defer ctx.deinit();
    var transported = try server.handleHttpx(&ctx);
    var transported_live = true;
    defer if (transported_live) transported.deinit();
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().output_blocked);
    transported.releaseBody();
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
    scheduler.close();
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
    transported.deinit();
    transported_live = false;
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
}

test "serverless execution response transfer preserves the body owner with allocations denied" {
    const Server = @import("serverless_http_server.zig").ServerlessHttpServer;
    const Types = @import("serverless/api/http_types.zig");
    const Httpx = @import("httpx");
    const Budget = @import("common/allocation_budget.zig").Shared;
    const Handler = struct {
        budget: *Budget,
        body_ptr: ?[*]u8 = null,
        pub fn handle(self: *@This(), _: Types.HttpRequest) !Types.HttpResponse {
            const alloc = self.budget.allocator();
            const content_type = try alloc.dupe(u8, "application/json");
            errdefer alloc.free(content_type);
            const body = try alloc.dupe(u8, "{\"accepted\":true}");
            self.body_ptr = body.ptr;
            self.budget.limit = 0;
            return .{ .status = 202, .content_type = content_type, .body = body };
        }
    };
    var budget: Budget = .{ .child = std.testing.allocator, .limit = 4096 };
    var handler: Handler = .{ .budget = &budget };
    var server = Server.init(budget.allocator(), .{}, &handler);
    var response = try server.handle(.{ .method = .POST, .uri = "/namespaces/docs/ingest" });
    try std.testing.expectEqual(handler.body_ptr.?, response.body.ptr);
    try std.testing.expectEqual(@as(u16, 202), response.status);
    response.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 0), budget.stats().used_bytes);

    budget.limit = 8192;
    var request = try Httpx.Request.init(std.testing.allocator, .POST, "/namespaces/docs/ingest");
    defer request.deinit();
    var ctx = Httpx.Context.init(budget.allocator(), std.testing.io, &request);
    defer ctx.deinit();
    var transported = try server.handleHttpx(&ctx);
    try std.testing.expectEqual(handler.body_ptr.?, transported.body.?.ptr);
    try std.testing.expectEqualStrings("{\"accepted\":true}", transported.body.?);
    try std.testing.expectEqual(@as(u16, 202), transported.status.code);
    try std.testing.expect(budget.stats().used_bytes > 0);
    transported.deinit();
    try std.testing.expectEqual(@as(usize, 0), budget.stats().used_bytes);
}

test "serverless execution canceled requests preserve their cause before header reservation" {
    const Server = @import("serverless_http_server.zig").ServerlessHttpServer;
    const Types = @import("serverless/api/http_types.zig");
    const Httpx = @import("httpx");
    const Handler = struct {
        called: bool = false,
        pub fn handle(self: *@This(), _: Types.HttpRequest) !Types.HttpResponse {
            self.called = true;
            return error.TestUnexpectedResult;
        }
    };
    var handler: Handler = .{};
    var budget: @import("common/allocation_budget.zig").Shared = .{ .child = std.testing.allocator, .limit = 0 };
    var server = Server.init(budget.allocator(), .{}, &handler);
    var request = try Httpx.Request.init(std.testing.allocator, .POST, "/namespaces/docs/ingest");
    defer request.deinit();
    var ctx = Httpx.Context.init(budget.allocator(), std.testing.io, &request);
    defer ctx.deinit();
    var canceled = std.atomic.Value(bool).init(true);
    ctx.cancellation = &canceled;
    try std.testing.expectError(error.Canceled, server.handleHttpx(&ctx));
    canceled.store(false, .release);
    ctx.application_deadline_ns = 0;
    try std.testing.expectError(error.DeadlineExceeded, server.handleHttpx(&ctx));
    try std.testing.expect(!handler.called);
    try std.testing.expectEqual(@as(u64, 0), budget.stats().rejected_total);
}

test "serverless execution httpx transferred body retires on replacement and failed build" {
    const Httpx = @import("httpx");
    const Budget = @import("common/allocation_budget.zig").Shared;
    var owner: Budget = .{ .child = std.testing.allocator, .limit = 4096 };
    var builder = Httpx.ResponseBuilder.init(std.testing.allocator);
    defer builder.deinit();
    _ = builder.ownedBody(try owner.allocator().dupe(u8, "owned"), owner.allocator());
    _ = builder.body("replacement");
    try std.testing.expectEqual(@as(usize, 0), owner.stats().used_bytes);

    var headers: Budget = .{ .child = std.testing.allocator, .limit = 0 };
    var refused = Httpx.ResponseBuilder.init(headers.allocator());
    defer refused.deinit();
    _ = refused.ownedBody(try owner.allocator().dupe(u8, "owned"), owner.allocator());
    try std.testing.expectError(error.OutOfMemory, refused.build());
    try std.testing.expectEqual(@as(usize, 0), owner.stats().used_bytes);
    try std.testing.expectEqual(@as(usize, 0), headers.stats().used_bytes);

    _ = builder.ownedBody(try owner.allocator().dupe(u8, "HEAD body"), owner.allocator());
    var response = try builder.build();
    defer response.deinit();
    response.releaseBody();
    try std.testing.expectEqual(@as(usize, 0), owner.stats().used_bytes);
    try std.testing.expect(response.body == null);
}

test "serverless execution response conversion retires partial allocations" {
    const Server = @import("serverless_http_server.zig").ServerlessHttpServer;
    const Types = @import("serverless/api/http_types.zig");
    const Fixture = struct {
        alloc: std.mem.Allocator,
        pub fn handle(self: *@This(), _: Types.HttpRequest) !Types.HttpResponse {
            const content_type = try self.alloc.dupe(u8, "application/json");
            errdefer self.alloc.free(content_type);
            return .{ .status = 200, .content_type = content_type, .body = try self.alloc.dupe(u8, "acknowledged") };
        }
        fn run(alloc: std.mem.Allocator) !void {
            var fixture: @This() = .{ .alloc = alloc };
            var server = Server.init(alloc, .{}, &fixture);
            var response = try server.handle(.{ .method = .GET, .uri = "/healthz" });
            defer response.deinit(alloc);
            try std.testing.expectEqualStrings("acknowledged", response.body);
        }
    };
    try Fixture.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "serverless execution maintenance mutex wait lends CPU and preserves original rejection" {
    const manager = @import("serverless/runtime/manager.zig");
    const Job = struct {
        runtime: *manager.ManagedRuntime,
        request_driven: bool,
        owner: Runnable.Execution = .{},
        canceled: std.atomic.Value(bool) = .init(false),
        done: std.atomic.Value(bool) = .init(false),
        failure: ?anyerror = null,
        fn check(raw: *const anyopaque) !void {
            if (requested(raw)) return error.CatalogGenerationChanged;
        }
        fn requested(raw: *const anyopaque) bool {
            return @as(*const @This(), @ptrCast(@alignCast(raw))).canceled.load(.acquire);
        }
        fn perform(self: *@This()) !void {
            const cancellation: @import("antfly_cancellation").CancellationToken = .{ .ptr = self, .check_fn = check };
            if (self.request_driven) {
                var completion = try @import("api/runnable_write_team.zig").Completion.init(&self.owner, std.testing.allocator);
                const team = try completion.team();
                const parent = try team.borrow.producerParent();
                var bridge = try @import("api/runnable_native_borrow.zig").Bridge.init(&parent, std.testing.allocator);
                var execution: Runnable.Execution = .{};
                defer execution.finish();
                try bridge.bind(&execution);
                _ = try self.runtime.runNamespaceMaterializationWithExecution("docs", cancellation, &execution);
            } else _ = try self.runtime.runOnceWithCancellation(cancellation);
        }
        fn run(self: *@This()) void {
            defer self.done.store(true, .release);
            self.perform() catch |err| {
                self.failure = err;
            };
        }
    };
    const io = std.testing.io;
    for ([_]bool{ false, true }) |request_driven| {
        var scheduler = try Runnable.Scheduler.init(.{ .slots = 4, .cleanup_slots = 1, .max_wait_ms = 2000 });
        defer scheduler.close();
        var runtime: manager.ManagedRuntime = .{
            .alloc = std.testing.allocator,
            .io = io,
            .cfg = .{},
            // No provider may be touched before this held mutex is rejected.
            .catalog = undefined,
            .publisher = undefined,
            .pruner = undefined,
            .enricher = @as(@import("serverless/enrichment/mod.zig").SparseEnricher, undefined),
            .runnable_scheduler = &scheduler,
            .runnable_slots = 2,
        };
        try std.testing.expect(runtime.run_mu.tryLock());
        defer runtime.run_mu.unlock();
        var job: Job = .{ .runtime = &runtime, .request_driven = request_driven };
        defer job.owner.finish();
        if (request_driven) try job.owner.startWithSlots(&scheduler, .{ .io = io, .cancellation = .{ .ptr = &job, .requested = Job.requested } }, .write, 2);
        var future = try io.concurrent(Job.run, .{&job});
        defer future.cancel(io);
        const until = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(2));
        while (scheduler.stats().resource_blocked == 0) {
            if (job.done.load(.acquire)) return job.failure orelse error.MaintenanceDidNotWait;
            if (std.Io.Clock.awake.now(io).nanoseconds >= until.nanoseconds) return error.MaintenanceDidNotWait;
            try io.sleep(.fromMilliseconds(1), .awake);
        }
        var foreground: Runnable.Execution = .{};
        defer foreground.finish();
        try foreground.startWithSlots(&scheduler, .{ .io = io }, .query, 2);
        job.canceled.store(true, .release);
        future.await(io);
        try std.testing.expectEqual(error.CatalogGenerationChanged, job.failure.?);
        // A borrowed scope never retires its request parent's reservation.
        if (request_driven) {
            try std.testing.expectEqual(@as(usize, 2), scheduler.stats().operations);
            job.owner.finish();
        }
        try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
        foreground.finish();
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
    }
}

test "serverless execution managed metadata borrows provider waits and preserves committed data after rejection" {
    const local = @import("antfly_local_sources");
    const maintenance = @import("serverless/maintenance_cancellation.zig");
    const Catalog = @import("serverless/catalog/service.zig").CatalogService;
    const Probe = struct {
        execution: *Runnable.Execution,
        scheduler: *Runnable.Scheduler,
        waits: std.atomic.Value(usize) = .init(0),
        lost: std.atomic.Value(bool) = .init(false),
        saw_renewal: std.atomic.Value(bool) = .init(false),
        reject_return: bool = false,
        fn observe(self: *@This()) !void {
            @import("antfly_platform").sync.lockYielding(&self.scheduler.mutex);
            const blocked = self.execution.entry.?.state == .resource_blocked;
            self.scheduler.mutex.unlock();
            if (self.scheduler.stats().control_children != 0) self.saw_renewal.store(true, .release);
            if (!blocked) return;
            var unrelated: Runnable.Execution = .{};
            defer unrelated.finish();
            unrelated.start(self.scheduler, .{ .io = std.testing.io, .allow_wait = false }, .query) catch |err| switch (err) {
                // A concurrently resuming parent can consume the observed
                // ordinary slot; this observer never rejects its authority.
                error.ResourceTemporarilyUnavailable, error.SchedulerFull => return,
                else => return err,
            };
            _ = self.waits.fetchAdd(1, .acq_rel);
        }
        fn authority(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.lost.load(.acquire)) return error.WorkLeaseLost;
            try self.observe();
        }
        fn original(raw: *const anyopaque) !void {
            try authority(@constCast(raw));
        }
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try authority(raw);
            try self.execution.checkpoint();
            try self.execution.park(.resource_blocked);
            try self.observe();
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.reject_return) self.lost.store(true, .release);
            try self.execution.enter(.background);
            try authority(raw);
        }
    };
    for ([_]bool{ false, true }) |reserved_return| {
        var directory = try local.common_test_directory.TestDirectory.init("managed-metadata");
        defer directory.cleanup();
        const alloc = std.testing.allocator;
        var uris: [5][]u8 = undefined;
        var initialized: usize = 0;
        defer for (uris[0..initialized]) |uri| alloc.free(uri);
        for ([_][]const u8{ "artifacts", "manifests", "wal", "progress", "catalog" }, 0..) |name, index| {
            uris[index] = try std.fmt.allocPrint(alloc, "file://{s}/{s}", .{ directory.path(), name });
            initialized += 1;
        }
        var stack: bootstrap.OwnedStack = undefined;
        try stack.init(alloc, .{
            .artifacts_uri = uris[0],
            .manifests_uri = uris[1],
            .wal_uri = uris[2],
            .progress_uri = uris[3],
            .catalog_uri = uris[4],
            .query_cache_dir = null,
            .enrichment_enabled = false,
            .compaction_enabled = false,
            .prune_enabled = false,
            .graph_metric_max_parallelism = 1,
            .process_memory_limit_bytes = 64 << 20,
            .runnable_policy = .{ .slots = if (reserved_return) 4 else 3, .cleanup_slots = 1, .publication_slots = if (reserved_return) 1 else 0 },
        }, std.testing.io);
        defer stack.deinit();
        try std.testing.expect(try stack.catalog.ensureTable("docs", 1));
        var accepted = try stack.api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 1, .mutations = &.{.{ .kind = .upsert, .doc_id = "a", .body = "{\"body\":\"alpha\"}" }} });
        defer accepted.deinit(stack.api.alloc);
        var execution: Runnable.Execution = .{};
        defer execution.finish();
        const scheduler = &stack.runnable_scheduler.?;
        try execution.start(scheduler, .{ .io = std.testing.io, .retained_bytes = @sizeOf(Runnable.Execution) + Catalog.Scope.retained_bytes }, .background);
        var probe: Probe = .{ .execution = &execution, .scheduler = scheduler };
        var context = (local.serverless_query_lake_read_context.Context{ .io = std.testing.io, .authority = .{ .ptr = &probe, .check = Probe.authority } }).withExecution(&execution);
        context.wait = .{ .ptr = &probe, .begin = Probe.begin, .end = Probe.end };
        var scope: Catalog.Scope = .{};
        const catalog = scope.bind(&stack.catalog, context);
        try std.testing.expect(catalog.lease_owner.? == &stack.catalog);
        _ = try catalog.getPolicy("docs");
        // Catalog read sequences preserve their mutex; a disabled return
        // reserve keeps CPU rather than queueing while holding that mutex.
        try std.testing.expectEqual(reserved_return, probe.waits.load(.acquire) != 0);
        const before = probe.waits.load(.acquire);
        try std.testing.expectEqual(@as(u64, 1), try catalog.wal.latestLsn("docs"));
        const records = try catalog.wal.readFromAlloc("docs", 1);
        defer @import("serverless/wal/mod.zig").freeRecords(catalog.wal.allocator, records);
        try std.testing.expectEqual(@as(usize, 1), records.len);
        try std.testing.expect(probe.waits.load(.acquire) > before);
        const token: maintenance.Token = .{ .io = std.testing.io, .execution = &execution, .cooperative = .{ .ptr = &probe, .check_fn = Probe.original } };
        var publication = try catalog.buildNamespace("docs");
        try std.testing.expect(probe.saw_renewal.load(.acquire));
        defer publication.deinit(catalog.alloc);
        try std.testing.expect(publication.published);
        var second = try stack.api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 2, .mutations = &.{.{ .kind = .upsert, .doc_id = "b", .body = "{\"body\":\"beta\"}" }} });
        defer second.deinit(stack.api.alloc);
        var updated = try catalog.buildNamespaceGuardedUntil("docs", null, token);
        defer updated.deinit(catalog.alloc);
        try std.testing.expect(updated.published);
        const builds = @import("serverless/build/mod.zig");
        var compactor = builds.Compactor.init(stack.alloc, &stack.artifacts, &stack.manifests, &stack.progress);
        var compacted = try compactor.compactHeadGuardedUntil("docs", null, token);
        defer compacted.deinit(stack.alloc);
        try std.testing.expect(compacted.published);
        var pruner = builds.Pruner.init(stack.alloc, &stack.artifacts, &stack.manifests, &stack.progress, &stack.wal);
        var pruned = try pruner.pruneNamespaceUntil("docs", 2, token);
        defer pruned.deinit(stack.alloc);
        try std.testing.expect(!pruned.work_lease_conflict and !pruned.gc_watermark_conflict);
        const head = try catalog.progress.getHead("docs");
        var manifest = try catalog.manifests.getAlloc("docs", head);
        defer manifest.deinit(catalog.manifests.allocator);
        try std.testing.expectEqual(@as(u64, 2), manifest.stats.document_count);
        // The durable HEAD ETag and lease record fence this write. Keeping a
        // legacy caller's local mutex locked cannot obstruct CPU return here.
        try std.testing.expect(stack.progress_impl.mutex.tryLock());
        const republished = catalog.progress.compareAndSwapHead("docs", head, head);
        stack.progress_impl.mutex.unlock();
        try std.testing.expect(try republished);
        probe.reject_return = true;
        try std.testing.expectError(error.WorkLeaseLost, catalog.wal.readFromAlloc("docs", 1));
        try std.testing.expect(stack.wal.context == null and stack.manifests.context == null and stack.progress.context == null and stack.catalog_store.context == null);
        execution.finish();
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
        // The original public read path still sees the exact committed postimage.
        var response = try stack.handler.handle(.{ .method = .get, .path = "/tables/docs/query/published" });
        defer response.deinit(stack.handler.alloc);
        try std.testing.expectEqual(@as(u16, 200), response.status);
        var decoded = try std.json.parseFromSlice(std.json.Value, alloc, response.body, .{});
        defer decoded.deinit();
        const docs = decoded.value.object.get("documents").?.array.items;
        try std.testing.expectEqual(@as(usize, 2), docs.len);
        try std.testing.expectEqualStrings("a", docs[0].object.get("doc_id").?.string);
        try std.testing.expectEqualStrings("{\"body\":\"alpha\"}", docs[0].object.get("body").?.string);
        try std.testing.expectEqualStrings("b", docs[1].object.get("doc_id").?.string);
        try std.testing.expectEqualStrings("{\"body\":\"beta\"}", docs[1].object.get("body").?.string);
    }
}

test "serverless execution HEAD refusal after provider publication preserves the durable fenced result" {
    const objectstore = @import("objectstore");
    const progress_mod = @import("serverless/catalog/object_progress_store.zig");
    const Fixture = struct {
        var original_put: @FieldType(objectstore.Client.VTable, "put_object") = undefined;
        var lost: bool = false;
        var armed: bool = false;
        fn put(raw: *anyopaque, alloc: std.mem.Allocator, bucket: []const u8, key: []const u8, bytes: []const u8, options: objectstore.PutOptions) !objectstore.PutResult {
            const result = try original_put(raw, alloc, bucket, key, bytes, options);
            if (armed) lost = true;
            return result;
        }
        fn check(_: *anyopaque) !void {
            if (lost) return error.WorkLeaseLost;
        }
    };
    Fixture.lost = false;
    Fixture.armed = false;
    defer Fixture.armed = false;
    var directory = try @import("antfly_local_sources").common_test_directory.TestDirectory.init("head-publication-return");
    defer directory.cleanup();
    var filesystem = try objectstore.FilesystemClient.initWithIo(std.testing.allocator, directory.path(), std.testing.io);
    defer filesystem.deinit();
    var client = filesystem.client();
    var vtable = client.vtable.*;
    Fixture.original_put = vtable.put_object;
    vtable.put_object = Fixture.put;
    client.vtable = &vtable;
    var backend = try progress_mod.ObjectProgressStore.initWithClient(std.testing.allocator, client, "progress", "");
    var original = backend.progressStore();
    defer original.deinit();
    try std.testing.expect(try original.compareAndSwapHead("docs", null, 1));
    var scheduler = try Runnable.Scheduler.init(.{ .slots = 2 });
    defer scheduler.close();
    var execution: Runnable.Execution = .{};
    defer execution.finish();
    try execution.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = @sizeOf(Runnable.Execution) }, .background);
    var marker: u8 = 0;
    var scoped = original;
    scoped.context = (@import("antfly_local_sources").serverless_query_lake_read_context.Context{
        .io = std.testing.io,
        .authority = .{ .ptr = &marker, .check = Fixture.check },
    }).withExecution(&execution);
    try std.testing.expectError(error.WorkLeaseLost, scoped.compareAndSwapHeadFenced("docs", 1, 2, .{ .owner_id = "stale", .fencing_token = 1 }));
    try std.testing.expectEqual(@as(u64, 1), try original.getHead("docs"));
    Fixture.armed = true;
    try std.testing.expectError(error.WorkLeaseLost, scoped.compareAndSwapHead("docs", 1, 2));
    // Refusal discards the provider response. It cannot roll back or report
    // nonacceptance for the atomic durable mutation that already completed.
    try std.testing.expectEqual(@as(u64, 2), try original.getHead("docs"));
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
    execution.finish();
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
}

test "serverless execution manifest rejection during decoding retires the owned provider result" {
    const objectstore = @import("objectstore");
    const manifests = @import("serverless/manifest/mod.zig");
    const Allocations = struct {
        live: usize = 0,
        lost: bool = false,
        fn allocator(self: *@This()) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{ .alloc = allocate, .resize = resize, .remap = remap, .free = free } };
        }
        fn allocate(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const bytes = std.testing.allocator.rawAlloc(len, alignment, ret_addr) orelse return null;
            self.live += len;
            // This fixture's decoded namespace is four bytes. Provider keys,
            // bucket, checksums, content type and encoded body are longer.
            if (len == 4) self.lost = true;
            return bytes;
        }
        fn resize(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) bool {
            return false;
        }
        fn remap(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
            return null;
        }
        fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.testing.allocator.rawFree(bytes, alignment, ret_addr);
            self.live -= bytes.len;
        }
        fn authority(raw: *anyopaque) !void {
            if (@as(*@This(), @ptrCast(@alignCast(raw))).lost) return error.WorkLeaseLost;
        }
    };
    var directory = try @import("antfly_local_sources").common_test_directory.TestDirectory.init("manifest-decode-return");
    defer directory.cleanup();
    var filesystem = try objectstore.FilesystemClient.initWithIo(std.testing.allocator, directory.path(), std.testing.io);
    defer filesystem.deinit();
    var backend = try @import("serverless/manifest/object_store.zig").ObjectStore.initWithClient(std.testing.allocator, filesystem.client(), "manifests", "");
    var original = backend.manifestStore();
    defer original.deinit();
    try original.put(.{ .namespace = @constCast("docs"), .version = 1, .built_at_ns = 1, .wal_start_lsn = 1, .wal_end_lsn = 1, .stats = .{}, .artifacts = @constCast(&.{}) });
    var scheduler = try Runnable.Scheduler.init(.{ .slots = 2 });
    defer scheduler.close();
    var execution: Runnable.Execution = .{};
    defer execution.finish();
    try execution.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = @sizeOf(Runnable.Execution) }, .background);
    var allocations: Allocations = .{};
    var scoped = original;
    scoped.allocator = allocations.allocator();
    scoped.context = (@import("antfly_local_sources").serverless_query_lake_read_context.Context{
        .io = std.testing.io,
        .authority = .{ .ptr = &allocations, .check = Allocations.authority },
    }).withExecution(&execution);
    const result = scoped.getAllocWithCancellation("docs", 1, .none);
    if (result) |value| {
        var unexpected = value;
        unexpected.deinit(scoped.allocator);
        return error.TestUnexpectedResult;
    } else |err| try std.testing.expectEqual(@as(anyerror, error.WorkLeaseLost), err);
    try std.testing.expect(allocations.lost);
    try std.testing.expectEqual(@as(usize, 0), allocations.live);
    var healthy: manifests.Manifest = try original.getAlloc("docs", 1);
    defer healthy.deinit(original.allocator);
    try std.testing.expectEqualStrings("docs", healthy.namespace);
    execution.finish();
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
}

const ModelHttpTest = struct {
    const sources = @import("antfly_local_sources");
    const managed_mod = sources.inference_managed_embedder;
    const http = sources.common_http_http_common;
    const listener_mod = sources.common_http_std_http_listener;
    const Context = @import("antfly_inference_execution_context").RequestContext;
    const App = struct {
        received: std.atomic.Value(bool) = .init(false),
        catalogs: std.atomic.Value(usize) = .init(0),
        block: std.atomic.Value(bool) = .init(false),
        release: std.Io.Event = .unset,
        fn execute(raw: *anyopaque, alloc: std.mem.Allocator, req: http.HttpRequest) !http.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (req.method == .GET) _ = self.catalogs.fetchAdd(1, .monotonic);
            if (self.block.load(.acquire)) {
                self.received.store(true, .release);
                try self.release.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
            }
            const body = if (req.method == .GET)
                "{\"embedders\":{\"text-embedding-3-small\":{\"inputs\":[\"text\"],\"inference_capabilities\":{\"task\":\"embed\",\"batch\":{\"mode\":\"native\",\"preferred_items\":1,\"max_items\":8,\"max_encoded_bytes\":1048576,\"max_decoded_pixels\":0,\"max_media_parts_per_item\":1,\"per_item_failures\":false}}}}}"
            else
                "{\"data\":[{\"embedding\":[0.25,0.75]}]}";
            return .{ .status = 200, .content_type = try alloc.dupe(u8, "application/json"), .body = try alloc.dupe(u8, body) };
        }
    };
    const Job = struct {
        dense: sources.storage_db_enrichment_embedder.DenseEmbedder,
        context: Context,
        revoked: std.atomic.Value(bool) = .init(false),
        done: std.atomic.Value(bool) = .init(false),
        failure: ?anyerror = null,
        fn check(raw: *const anyopaque) !void {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            if (self.revoked.load(.acquire)) return error.ModelTransportRevoked;
        }
        fn canceled(raw: *const anyopaque) bool {
            check(raw) catch return true;
            return false;
        }
        fn run(self: *@This()) void {
            defer self.done.store(true, .release);
            self.context.cancellation = .{ .ptr = self, .check_fn = check, .is_cancelled_fn = canceled };
            const result = self.dense.embedDenseWithContext(std.testing.allocator, "model", "text", 2, self.context) catch |err| {
                self.failure = err;
                return;
            };
            defer std.testing.allocator.free(result);
            std.testing.expectEqualSlices(f32, &.{ 0.25, 0.75 }, result) catch |err| {
                self.failure = err;
            };
        }
    };
};

test "serverless execution remote model HTTP lends capacity and fences response reentry" {
    const sources = @import("antfly_local_sources");
    const managed_mod = sources.inference_managed_embedder;
    const listener_mod = sources.common_http_std_http_listener;
    const App = ModelHttpTest.App;
    const Job = ModelHttpTest.Job;
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    const Cache = @typeInfo(@typeInfo(@FieldType(managed_mod.InitOptions, "remote_capability_cache")).optional.child).pointer.child;
    for ([_][]const u8{ "openai", "antfly" }) |provider_kind| {
        for ([_]bool{ false, true }) |reject| {
            var app = App{};
            var listener = listener_mod.StdHttpListener.init(alloc, .{}, .{ .ptr = &app, .vtable = &.{ .execute = App.execute } });
            defer listener.deinit();
            try listener.start();
            const url = try listener.baseUri(alloc);
            defer alloc.free(url);
            const indexes = try std.fmt.allocPrint(alloc, "{{\"model\":{{\"type\":\"embeddings\",\"field\":\"text\",\"dimension\":2,\"embedder\":{{\"provider\":\"{s}\",\"model\":\"text-embedding-3-small\",\"url\":\"{s}\"}}}}}}", .{ provider_kind, url });
            defer alloc.free(indexes);
            var cache = Cache.init(alloc, io);
            defer cache.deinit();
            var managed = try managed_mod.ManagedEmbedder.initFromIndexesJsonWithOptions(alloc, indexes, .{ .io = io, .remote_capability_cache = &cache });
            defer managed.deinit();
            // Construction may probe the provider. Exercise cold discovery with
            // the invocation scope rather than reusing that constructor lease.
            cache.deinit();
            cache = Cache.init(alloc, io);
            var scheduler = try Runnable.Scheduler.init(.{ .slots = 3, .max_wait_ms = 5000 });
            defer scheduler.close();
            var execution: Runnable.Execution = .{};
            defer execution.finish();
            try execution.startWithSlots(&scheduler, .{ .io = io, .retained_bytes = @import("serverless/catalog/service.zig").CatalogService.Scope.retained_bytes }, .background, 2);
            var read: @import("api/runnable_local_read.zig").Read = .{ .execution = &execution, .allocator = alloc };
            const team = try read.team();
            var job: Job = .{ .dense = managed.denseInterface(), .context = .{ .io = io, .deadline_ns = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s, .runnable = team.borrow } };
            app.block.store(true, .release);
            var future = try io.concurrent(Job.run, .{&job});
            defer future.cancel(io);
            defer app.release.set(io);
            const until = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(3));
            while (!app.received.load(.acquire) or scheduler.stats().runnable_slots != 0) {
                if (job.done.load(.acquire)) return job.failure orelse error.ModelDidNotLendHttpWait;
                if (std.Io.Clock.awake.now(io).nanoseconds >= until.nanoseconds) return error.ModelDidNotLendHttpWait;
                try io.sleep(.fromMilliseconds(1), .awake);
            }
            try std.testing.expect(scheduler.stats().retained_bytes > 0);
            var query: Runnable.Execution = .{};
            defer query.finish();
            try query.start(&scheduler, .{ .io = io, .allow_wait = false }, .query);
            var health: Runnable.Execution = .{};
            defer health.finish();
            try health.start(&scheduler, .{ .io = io, .allow_wait = false }, .control);
            health.finish();
            app.release.set(io);
            if (reject) {
                while (scheduler.stats().queued == 0) {
                    if (job.done.load(.acquire)) return job.failure orelse error.ModelDidNotQueueForResponse;
                    if (std.Io.Clock.awake.now(io).nanoseconds >= until.nanoseconds) return error.ModelDidNotQueueForResponse;
                    try io.sleep(.fromMilliseconds(1), .awake);
                }
                job.revoked.store(true, .release);
                while (!job.done.load(.acquire)) {
                    if (std.Io.Clock.awake.now(io).nanoseconds >= until.nanoseconds) return error.ModelDidNotRejectResponse;
                    try io.sleep(.fromMilliseconds(1), .awake);
                }
            } else query.finish();
            future.await(io);
            if (reject) try std.testing.expectEqual(error.ModelTransportRevoked, job.failure.?) else if (job.failure) |err| return err;
            try std.testing.expect(managed.entries[0].http_control == null);
            query.finish();
            execution.finish();
            try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
            try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
        }
    }
}

test "serverless execution remote model discovery waiter lends its own retained capacity" {
    const sources = @import("antfly_local_sources");
    const managed_mod = sources.inference_managed_embedder;
    const Cache = @typeInfo(@typeInfo(@FieldType(managed_mod.InitOptions, "remote_capability_cache")).optional.child).pointer.child;
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    var app: ModelHttpTest.App = .{};
    var listener = sources.common_http_std_http_listener.StdHttpListener.init(alloc, .{}, .{ .ptr = &app, .vtable = &.{ .execute = ModelHttpTest.App.execute } });
    defer listener.deinit();
    try listener.start();
    const url = try listener.baseUri(alloc);
    defer alloc.free(url);
    const indexes = try std.fmt.allocPrint(alloc, "{{\"model\":{{\"type\":\"embeddings\",\"field\":\"text\",\"dimension\":2,\"embedder\":{{\"provider\":\"antfly\",\"model\":\"text-embedding-3-small\",\"url\":\"{s}\"}}}}}}", .{url});
    defer alloc.free(indexes);
    var cache = Cache.init(alloc, io);
    defer cache.deinit();
    var managed = try managed_mod.ManagedEmbedder.initFromIndexesJsonWithOptions(alloc, indexes, .{ .io = io, .remote_capability_cache = &cache });
    defer managed.deinit();
    cache.deinit();
    cache = Cache.init(alloc, io);
    app.catalogs.store(0, .release);
    app.block.store(true, .release);
    var scheduler = try Runnable.Scheduler.init(.{ .slots = 3, .max_wait_ms = 5000 });
    defer scheduler.close();
    var executions: [2]Runnable.Execution = @splat(.{});
    defer for (&executions) |*execution| execution.finish();
    var reads: [2]@import("api/runnable_local_read.zig").Read = undefined;
    var jobs: [2]ModelHttpTest.Job = undefined;
    var futures: [2]std.Io.Future(void) = undefined;
    var active: usize = 0;
    defer for (futures[0..active]) |*future| future.cancel(io);
    defer app.release.set(io);
    const until = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(3));
    for (0..2) |index| {
        try executions[index].start(&scheduler, .{ .io = io, .retained_bytes = @import("serverless/catalog/service.zig").CatalogService.Scope.retained_bytes }, .background);
        reads[index] = .{ .execution = &executions[index], .allocator = alloc };
        jobs[index] = .{ .dense = managed.denseInterface(), .context = .{ .io = io, .deadline_ns = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s, .runnable = (try reads[index].team()).borrow } };
        futures[index] = try io.concurrent(ModelHttpTest.Job.run, .{&jobs[index]});
        active += 1;
        while (scheduler.stats().resource_blocked != index + 1 or !app.received.load(.acquire)) {
            if (jobs[index].done.load(.acquire)) return jobs[index].failure orelse error.DiscoveryDidNotWait;
            if (std.Io.Clock.awake.now(io).nanoseconds >= until.nanoseconds) return error.DiscoveryWaiterKeptCpu;
            try io.sleep(.fromMilliseconds(1), .awake);
        }
    }
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(usize, 2), scheduler.stats().operations);
    var query: Runnable.Execution = .{};
    defer query.finish();
    try query.start(&scheduler, .{ .io = io, .allow_wait = false }, .query);
    var control: Runnable.Execution = .{};
    defer control.finish();
    try control.start(&scheduler, .{ .io = io, .allow_wait = false }, .control);
    control.finish();
    query.finish();
    app.release.set(io);
    for (&futures, &jobs) |*future, *job| {
        future.await(io);
        if (job.failure) |err| return err;
    }
    active = 0;
    try std.testing.expectEqual(@as(usize, 1), app.catalogs.load(.acquire));
    try std.testing.expect(managed.entries[0].http_control == null);
    for (&executions) |*execution| execution.finish();
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
}

test "serverless execution model quota wait lends capacity and never releases an unowned permit" {
    const sources = @import("antfly_local_sources");
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    const App = struct {
        calls: std.atomic.Value(usize) = .init(0),
        fn execute(raw: *anyopaque, a: std.mem.Allocator, _: sources.common_http_http_common.HttpRequest) !sources.common_http_http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            _ = self.calls.fetchAdd(1, .release);
            return .{ .status = 200, .content_type = try a.dupe(u8, "application/json"), .body = try a.dupe(u8, "{\"data\":[{\"embedding\":[0.25,0.75]}]}") };
        }
        fn never(_: *const anyopaque) bool {
            return false;
        }
    };
    for ([_]bool{ false, true }) |reject| {
        var app = App{};
        var listener = sources.common_http_std_http_listener.StdHttpListener.init(alloc, .{}, .{ .ptr = &app, .vtable = &.{ .execute = App.execute } });
        defer listener.deinit();
        try listener.start();
        const url = try listener.baseUri(alloc);
        defer alloc.free(url);
        const raw = try std.fmt.allocPrint(alloc, "{{\"model\":{{\"type\":\"embeddings\",\"field\":\"text\",\"dimension\":2,\"embedder\":{{\"provider\":\"openai\",\"model\":\"text-embedding-3-small\",\"url\":\"{s}\",\"rate_limit\":{{\"max_concurrency\":1}}}}}}}}", .{url});
        defer alloc.free(raw);
        var managed = try sources.inference_managed_embedder.ManagedEmbedder.initFromIndexesJsonWithOptions(alloc, raw, .{ .io = io });
        defer managed.deinit();
        app.calls.store(0, .release);
        const limiter = managed.entries[0].quota.?.limiter();
        const observer = limiter.observer(0);
        try observer.before(observer.ptr, .{ .io = io, .deadline_ms = null, .cancellation_ptr = &app, .is_cancelled = App.never, .body_bytes = 0, .output_tokens = 0 });
        var held = true;
        defer if (held) observer.after(observer.ptr, null);
        var scheduler = try Runnable.Scheduler.init(.{ .slots = 3, .max_wait_ms = 5000 });
        defer scheduler.close();
        var execution: Runnable.Execution = .{};
        defer execution.finish();
        try execution.startWithSlots(&scheduler, .{ .io = io, .retained_bytes = @import("serverless/catalog/service.zig").CatalogService.Scope.retained_bytes }, .background, 2);
        var read: @import("api/runnable_local_read.zig").Read = .{ .execution = &execution, .allocator = alloc };
        var job: ModelHttpTest.Job = .{ .dense = managed.denseInterface(), .context = .{ .io = io, .deadline_ns = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s, .runnable = (try read.team()).borrow } };
        var future = try io.concurrent(ModelHttpTest.Job.run, .{&job});
        defer future.cancel(io);
        const until = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(3));
        while (scheduler.stats().runnable_slots != 0) {
            if (job.done.load(.acquire)) return job.failure orelse error.QuotaWaitDidNotBlock;
            if (std.Io.Clock.awake.now(io).nanoseconds >= until.nanoseconds) return error.QuotaWaitKeptCpu;
            try io.sleep(.fromMilliseconds(1), .awake);
        }
        var query: Runnable.Execution = .{};
        defer query.finish();
        try query.start(&scheduler, .{ .io = io, .allow_wait = false }, .query);
        var control: Runnable.Execution = .{};
        defer control.finish();
        try control.start(&scheduler, .{ .io = io, .allow_wait = false }, .control);
        control.finish();
        try std.testing.expectEqual(@as(usize, 0), app.calls.load(.acquire));
        if (reject) {
            job.revoked.store(true, .release);
            while (!job.done.load(.acquire)) {
                if (std.Io.Clock.awake.now(io).nanoseconds >= until.nanoseconds) return error.QuotaScopeDidNotReject;
                try io.sleep(.fromMilliseconds(1), .awake);
            }
        } else {
            observer.after(observer.ptr, null);
            held = false;
            query.finish();
        }
        future.await(io);
        if (reject) {
            try std.testing.expectEqual(error.ModelTransportRevoked, job.failure.?);
            try std.testing.expectEqual(@as(u32, 1), limiter.in_flight);
            try std.testing.expectEqual(@as(usize, 0), app.calls.load(.acquire));
            observer.after(observer.ptr, null);
            held = false;
        } else {
            if (job.failure) |err| return err;
            try std.testing.expectEqual(@as(usize, 1), app.calls.load(.acquire));
        }
        try std.testing.expectEqual(@as(u32, 0), limiter.in_flight);
        query.finish();
        execution.finish();
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
    }
}

test "serverless execution credential refresh owners and waiters lend capacity without retaining cache locks" {
    const google = @import("antfly_google");
    const aws = @import("antfly_credentials").aws;
    const sources = @import("antfly_local_sources");
    const httpx = @import("httpx");
    const Context = @import("antfly_inference_execution_context").RequestContext;
    const Family = enum { google, aws };
    const App = struct {
        family: Family,
        received: std.atomic.Value(bool) = .init(false),
        calls: std.atomic.Value(usize) = .init(0),
        release: std.Io.Event = .unset,
        fn execute(raw: *anyopaque, a: std.mem.Allocator, _: sources.common_http_http_common.HttpRequest) !sources.common_http_http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            _ = self.calls.fetchAdd(1, .release);
            self.received.store(true, .release);
            try self.release.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
            const body = if (self.family == .google) "{\"access_token\":\"new-token\",\"expires_in\":3600,\"token_type\":\"Bearer\"}" else "<AssumeRoleWithWebIdentityResponse><AssumeRoleWithWebIdentityResult><Credentials><AccessKeyId>AKID</AccessKeyId><SecretAccessKey>secret</SecretAccessKey><SessionToken>session</SessionToken><Expiration>2099-01-01T00:00:00Z</Expiration></Credentials></AssumeRoleWithWebIdentityResult></AssumeRoleWithWebIdentityResponse>";
            return .{ .status = 200, .content_type = try a.dupe(u8, "application/json"), .body = try a.dupe(u8, body) };
        }
    };
    const Job = struct {
        family: Family,
        google_source: *google.CachedTokenSource,
        aws_cache: *aws.CredentialCache,
        client: *httpx.Client,
        aws_source: aws.CredentialSource,
        context: Context,
        revoked: std.atomic.Value(bool) = .init(false),
        done: std.atomic.Value(bool) = .init(false),
        failure: ?anyerror = null,
        fn check(raw: *const anyopaque) !void {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            if (self.revoked.load(.acquire)) return error.CredentialScopeRevoked;
        }
        fn cancelled(raw: *const anyopaque) bool {
            check(raw) catch return true;
            return false;
        }
        fn perform(self: *@This(), wait: *@import("antfly_inference_execution_context").HttpWait) !void {
            const a = std.testing.allocator;
            switch (self.family) {
                .google => {
                    const token = try self.google_source.accessTokenAllocWithControl(a, .{ .deadline_ns = self.context.deadline_ns, .execution_wait = wait.control() });
                    defer a.free(token);
                    try wait.ready();
                    try std.testing.expectEqualStrings("new-token", token);
                },
                .aws => {
                    var credentials = try self.aws_cache.getForSourceWithContextAndIo(a, std.heap.smp_allocator, self.client, self.context.io, "us-east-1", self.aws_source, aws.RequestContext.init(self.context.io, 5000, null).withExecutionWait(wait.control()));
                    defer credentials.deinit(a);
                    try wait.ready();
                    try std.testing.expectEqualStrings("AKID", credentials.access_key_id);
                },
            }
        }
        fn run(self: *@This()) void {
            defer self.done.store(true, .release);
            self.context.cancellation = .{ .ptr = self, .check_fn = check, .is_cancelled_fn = cancelled };
            var wait: @import("antfly_inference_execution_context").HttpWait = .{ .context = self.context };
            self.perform(&wait) catch |err| {
                wait.ready() catch |original| {
                    self.failure = original;
                    return;
                };
                self.failure = err;
            };
        }
    };
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    for (std.enums.values(Family)) |family| for ([_]bool{ false, true }) |reject| {
        var app: App = .{ .family = family };
        var listener = sources.common_http_std_http_listener.StdHttpListener.init(alloc, .{}, .{ .ptr = &app, .vtable = &.{ .execute = App.execute } });
        defer listener.deinit();
        try listener.start();
        const url = try listener.baseUri(alloc);
        defer alloc.free(url);
        const cfg: google.Config = .{ .credentials = .{ .authorized_user = .{ .client_id = try alloc.dupe(u8, "client"), .client_secret = try alloc.dupe(u8, "secret"), .refresh_token = try alloc.dupe(u8, "refresh"), .token_uri = try alloc.dupe(u8, url) } }, .scope = try alloc.dupe(u8, google.default_scope) };
        var google_source = try google.CachedTokenSource.initWithIo(alloc, cfg, io);
        defer google_source.deinit();
        var aws_cache = aws.CredentialCache{};
        defer aws_cache.deinit(std.heap.smp_allocator);
        var client = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false });
        defer client.deinit();
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        try tmp.dir.writeFile(io, .{ .sub_path = "token", .data = "test-web-identity" });
        const token_path = try tmp.dir.realPathFileAlloc(io, "token", alloc);
        defer alloc.free(token_path);
        const aws_source: aws.CredentialSource = .{ .web_identity = .{ .role_arn = "test-role", .token_file = token_path, .sts_endpoint = url } };
        var scheduler = try Runnable.Scheduler.init(.{ .slots = 3, .max_wait_ms = 5000 });
        defer scheduler.close();
        var executions: [2]Runnable.Execution = @splat(.{});
        defer for (&executions) |*execution| execution.finish();
        var reads: [2]@import("api/runnable_local_read.zig").Read = undefined;
        var jobs: [2]Job = undefined;
        var futures: [2]std.Io.Future(void) = undefined;
        var active: usize = 0;
        defer for (futures[0..active]) |*future| future.cancel(io);
        defer app.release.set(io);
        const until = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(3));
        const count: usize = if (reject) 1 else 2;
        for (0..count) |index| {
            try executions[index].startWithSlots(&scheduler, .{ .io = io, .retained_bytes = @import("serverless/catalog/service.zig").CatalogService.Scope.retained_bytes }, .background, if (reject) 2 else 1);
            reads[index] = .{ .execution = &executions[index], .allocator = alloc };
            jobs[index] = .{ .family = family, .google_source = &google_source, .aws_cache = &aws_cache, .client = &client, .aws_source = aws_source, .context = .{ .io = io, .deadline_ns = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s, .runnable = (try reads[index].team()).borrow } };
            futures[index] = try io.concurrent(Job.run, .{&jobs[index]});
            active += 1;
            while (scheduler.stats().resource_blocked != index + 1 or !app.received.load(.acquire)) {
                if (jobs[index].done.load(.acquire)) return jobs[index].failure orelse error.CredentialRefreshDidNotWait;
                if (std.Io.Clock.awake.now(io).nanoseconds >= until.nanoseconds) return error.CredentialWaiterKeptCpu;
                try io.sleep(.fromMilliseconds(1), .awake);
            }
        }
        if (family == .google) {
            try std.testing.expect(google_source.mutex.tryLock());
            google_source.mutex.unlock();
        } else {
            try std.testing.expect(aws_cache.mutex.tryLock());
            aws_cache.mutex.unlock(io);
        }
        var query: Runnable.Execution = .{};
        defer query.finish();
        try query.start(&scheduler, .{ .io = io, .allow_wait = false }, .query);
        var control: Runnable.Execution = .{};
        defer control.finish();
        try control.start(&scheduler, .{ .io = io, .allow_wait = false }, .control);
        control.finish();
        app.release.set(io);
        if (reject) {
            while (scheduler.stats().queued == 0) {
                if (jobs[0].done.load(.acquire)) return jobs[0].failure orelse error.CredentialReturnDidNotQueue;
                if (std.Io.Clock.awake.now(io).nanoseconds >= until.nanoseconds) return error.CredentialReturnDidNotQueue;
                try io.sleep(.fromMilliseconds(1), .awake);
            }
            jobs[0].revoked.store(true, .release);
            while (!jobs[0].done.load(.acquire)) {
                if (std.Io.Clock.awake.now(io).nanoseconds >= until.nanoseconds) return error.CredentialReturnDidNotReject;
                try io.sleep(.fromMilliseconds(1), .awake);
            }
        } else query.finish();
        for (0..count) |index| {
            futures[index].await(io);
            if (reject) try std.testing.expectEqual(error.CredentialScopeRevoked, jobs[index].failure.?) else if (jobs[index].failure) |err| return err;
        }
        active = 0;
        try std.testing.expectEqual(@as(usize, 1), app.calls.load(.acquire));
        if (family == .google) {
            try std.testing.expect(!google_source.refreshing);
            if (reject) try std.testing.expect(google_source.cached_token == null);
        } else {
            try std.testing.expect(!aws_cache.refreshing);
            if (reject) try std.testing.expect(aws_cache.cached == null);
        }
        query.finish();
        for (&executions) |*execution| execution.finish();
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
    };
}

test "serverless execution credential file admission preserves cache ownership and caller authority" {
    const google = @import("antfly_google");
    const aws = @import("antfly_credentials").aws;
    const httpx = @import("httpx");
    const inference = @import("antfly_inference_execution_context");
    const Family = enum { google, aws };
    const Job = struct {
        family: Family,
        manager: *google.CredentialManager,
        cache: *aws.CredentialCache,
        client: *httpx.Client,
        path: []const u8,
        wait: inference.HttpWait,
        gate_at: usize,
        ends: usize = 0,
        ready: std.atomic.Value(bool) = .init(false),
        release: std.Io.Event = .unset,
        revoked: std.atomic.Value(bool) = .init(false),
        done: std.atomic.Value(bool) = .init(false),
        result: ?*google.CachedTokenSource = null,
        failure: ?anyerror = null,
        fn check(raw: *const anyopaque) !void {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            if (self.revoked.load(.acquire)) return error.CredentialFileScopeRevoked;
        }
        fn cancelled(raw: *const anyopaque) bool {
            check(raw) catch return true;
            return false;
        }
        fn originalCheck(raw: ?*anyopaque) httpx.socket.WriteControl.Error!void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            try self.wait.control().check();
        }
        fn begin(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            const control = self.wait.control();
            try control.wait_begin_fn.?(control.ptr);
        }
        fn end(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.ends += 1;
            if (self.ends == self.gate_at) {
                // Real filesystem I/O has completed. Hold only its CPU return,
                // deterministically exercising reentry rather than OS latency.
                self.ready.store(true, .release);
                try self.release.waitTimeout(self.wait.context.io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
            }
            const control = self.wait.control();
            try control.wait_end_fn.?(control.ptr);
        }
        fn perform(self: *@This()) !void {
            const a = std.testing.allocator;
            const control: httpx.socket.WriteControl = .{ .ptr = self, .check_fn = originalCheck, .wait_begin_fn = begin, .wait_end_fn = end };
            switch (self.family) {
                .google => self.result = try self.manager.tokenSourceWithControl(self.path, google.default_scope, .{ .deadline_ns = self.wait.context.deadline_ns, .execution_wait = control }),
                .aws => {
                    var credentials = try self.cache.getForSourceWithContextAndIo(a, std.heap.smp_allocator, self.client, self.wait.context.io, "us-east-1", .{ .profile = .{ .name = "admission-fixture", .shared_credentials_file = self.path } }, aws.RequestContext.init(self.wait.context.io, 5000, null).withExecutionWait(control));
                    defer credentials.deinit(a);
                    try std.testing.expectEqualStrings("file-access", credentials.access_key_id);
                },
            }
            try self.wait.ready();
        }
        fn run(self: *@This()) void {
            defer self.done.store(true, .release);
            self.wait.context.cancellation = .{ .ptr = self, .check_fn = check, .is_cancelled_fn = cancelled };
            self.perform() catch |err| {
                self.wait.ready() catch |original| {
                    self.failure = original;
                    return;
                };
                self.failure = err;
            };
        }
    };
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    for (std.enums.values(Family)) |family| for (0..3) |mode| {
        const reject = mode != 0;
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        try tmp.dir.writeFile(io, .{ .sub_path = "credentials", .data = if (family == .google)
            "{\"type\":\"authorized_user\",\"client_id\":\"client\",\"client_secret\":\"secret\",\"refresh_token\":\"refresh\"}"
        else
            "[admission-fixture]\naws_access_key_id=file-access\naws_secret_access_key=file-secret\n" });
        const path = try tmp.dir.realPathFileAlloc(io, "credentials", alloc);
        defer alloc.free(path);
        var manager = google.CredentialManager.init(alloc, io);
        defer manager.deinit();
        var cache = aws.CredentialCache{};
        defer cache.deinit(std.heap.smp_allocator);
        var client = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false });
        defer client.deinit();
        var scheduler = try Runnable.Scheduler.init(.{ .slots = 3, .max_wait_ms = 5000 });
        defer scheduler.close();
        var executions: [2]Runnable.Execution = @splat(.{});
        defer for (&executions) |*execution| execution.finish();
        var reads: [2]@import("api/runnable_local_read.zig").Read = undefined;
        var jobs: [2]Job = undefined;
        var futures: [2]std.Io.Future(void) = undefined;
        var active: usize = 0;
        defer for (futures[0..active]) |*future| future.cancel(io);
        defer for (jobs[0..active]) |*job| job.release.set(io);
        const count: usize = if (!reject and family == .google) 2 else 1;
        const until = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(4));
        for (0..count) |index| {
            try executions[index].startWithSlots(&scheduler, .{ .io = io, .retained_bytes = @import("serverless/catalog/service.zig").CatalogService.Scope.retained_bytes }, .background, if (reject) 2 else 1);
            reads[index] = .{ .execution = &executions[index], .allocator = alloc };
            jobs[index] = .{ .family = family, .manager = &manager, .cache = &cache, .client = &client, .path = path, .gate_at = if (mode == 2) 2 else 1, .wait = .{ .context = .{ .io = io, .deadline_ns = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s, .runnable = (try reads[index].team()).borrow } } };
            futures[index] = try io.concurrent(Job.run, .{&jobs[index]});
            active += 1;
            while (!jobs[index].ready.load(.acquire)) {
                if (jobs[index].done.load(.acquire)) return jobs[index].failure orelse error.CredentialFileDidNotWait;
                if (std.Io.Clock.awake.now(io).nanoseconds >= until.nanoseconds) return error.CredentialFileDidNotWait;
                try io.sleep(.fromMilliseconds(1), .awake);
            }
        }
        try std.testing.expectEqual(count, scheduler.stats().resource_blocked);
        try std.testing.expect(manager.mutex.tryLock());
        manager.mutex.unlock(io);
        try std.testing.expect(cache.mutex.tryLock());
        cache.mutex.unlock(io);
        var query: Runnable.Execution = .{};
        defer query.finish();
        try query.start(&scheduler, .{ .io = io, .allow_wait = false }, .query);
        var control: Runnable.Execution = .{};
        defer control.finish();
        try control.start(&scheduler, .{ .io = io, .allow_wait = false }, .control);
        control.finish();
        for (jobs[0..count]) |*job| job.release.set(io);
        if (reject) {
            while (scheduler.stats().queued == 0) {
                if (jobs[0].done.load(.acquire)) return jobs[0].failure orelse error.CredentialFileReturnDidNotQueue;
                if (std.Io.Clock.awake.now(io).nanoseconds >= until.nanoseconds) return error.CredentialFileReturnDidNotQueue;
                try io.sleep(.fromMilliseconds(1), .awake);
            }
            jobs[0].revoked.store(true, .release);
            while (!jobs[0].done.load(.acquire)) {
                if (std.Io.Clock.awake.now(io).nanoseconds >= until.nanoseconds) return error.CredentialFileReturnDidNotReject;
                try io.sleep(.fromMilliseconds(1), .awake);
            }
        } else query.finish();
        for (0..count) |index| {
            futures[index].await(io);
            if (reject) try std.testing.expectEqual(error.CredentialFileScopeRevoked, jobs[index].failure.?) else if (jobs[index].failure) |err| return err;
        }
        active = 0;
        if (family == .google) {
            try std.testing.expectEqual(@as(usize, if (reject) 0 else 1), manager.sources.count());
            if (!reject) try std.testing.expect(jobs[0].result.? == jobs[1].result.?);
        } else {
            try std.testing.expect(!cache.refreshing);
            if (reject) try std.testing.expect(cache.cached == null);
        }
        query.finish();
        for (&executions) |*execution| execution.finish();
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
    };
}

test "serverless execution query template downloads lend capacity before provider dispatch" {
    const sources = @import("antfly_local_sources");
    const managed_mod = sources.inference_managed_embedder;
    const App = struct {
        received: std.atomic.Value(bool) = .init(false),
        embeddings: std.atomic.Value(usize) = .init(0),
        release: std.Io.Event = .unset,
        fn execute(raw: *anyopaque, alloc: std.mem.Allocator, req: sources.common_http_http_common.HttpRequest) !sources.common_http_http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (req.method == .GET) {
                self.received.store(true, .release);
                try self.release.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
            } else _ = self.embeddings.fetchAdd(1, .monotonic);
            return .{ .status = 200, .content_type = try alloc.dupe(u8, "text/plain"), .body = try alloc.dupe(u8, if (req.method == .GET) "downloaded template text" else "{\"data\":[{\"embedding\":[0.25,0.75]}]}") };
        }
    };
    const Job = struct {
        managed: *managed_mod.ManagedEmbedder,
        url: []const u8,
        revoked: std.atomic.Value(bool) = .init(false),
        done: std.atomic.Value(bool) = .init(false),
        failure: ?anyerror = null,
        fn check(raw: *const anyopaque) !void {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            if (self.revoked.load(.acquire)) return error.TemplateSourceRevoked;
        }
        fn run(self: *@This()) void {
            defer self.done.store(true, .release);
            const result = self.managed.embedQueryWithTemplateAndCancellation(std.testing.allocator, "model", self.url, "{{remoteText url=this}}", .{ .ptr = self, .check_fn = check }) catch |err| {
                self.failure = err;
                return;
            };
            defer std.testing.allocator.free(result);
            std.testing.expectEqualSlices(f32, &.{ 0.25, 0.75 }, result) catch |err| {
                self.failure = err;
            };
        }
    };
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    for ([_]bool{ false, true }) |reject| {
        var app: App = .{};
        var listener = sources.common_http_std_http_listener.StdHttpListener.init(alloc, .{}, .{ .ptr = &app, .vtable = &.{ .execute = App.execute } });
        defer listener.deinit();
        try listener.start();
        const url = try listener.baseUri(alloc);
        defer alloc.free(url);
        const indexes = try std.fmt.allocPrint(alloc, "{{\"model\":{{\"type\":\"embeddings\",\"field\":\"text\",\"dimension\":2,\"embedder\":{{\"provider\":\"openai\",\"model\":\"text-embedding-3-small\",\"url\":\"{s}\"}}}}}}", .{url});
        defer alloc.free(indexes);
        var scheduler = try Runnable.Scheduler.init(.{ .slots = 3, .max_wait_ms = 5000 });
        defer scheduler.close();
        var execution: Runnable.Execution = .{};
        defer execution.finish();
        try execution.startWithSlots(&scheduler, .{ .io = io, .retained_bytes = 4096 }, .background, 2);
        var read: @import("api/runnable_local_read.zig").Read = .{ .execution = &execution, .allocator = alloc };
        var remote_content: @import("antfly_scraping").RemoteContentConfig = .{ .security = .{ .block_private_ips = false } };
        var managed = try managed_mod.ManagedEmbedder.initFromIndexesJsonWithOptions(alloc, indexes, .{ .io = io, .remote_content = &remote_content, .runnable = (try read.team()).borrow });
        defer managed.deinit();
        var job: Job = .{ .managed = &managed, .url = url };
        var future = try io.concurrent(Job.run, .{&job});
        defer future.cancel(io);
        defer app.release.set(io);
        const until = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(3));
        while (!app.received.load(.acquire) or scheduler.stats().runnable_slots != 0) {
            if (job.done.load(.acquire)) return job.failure orelse error.TemplateDidNotLendCapacity;
            if (std.Io.Clock.awake.now(io).nanoseconds >= until.nanoseconds) return error.TemplateDidNotLendCapacity;
            try io.sleep(.fromMilliseconds(1), .awake);
        }
        try std.testing.expectEqual(@as(u64, 4096), scheduler.stats().retained_bytes);
        var query: Runnable.Execution = .{};
        defer query.finish();
        try query.start(&scheduler, .{ .io = io, .allow_wait = false }, .query);
        var health: Runnable.Execution = .{};
        defer health.finish();
        try health.start(&scheduler, .{ .io = io, .allow_wait = false }, .control);
        health.finish();
        app.release.set(io);
        if (reject) {
            while (scheduler.stats().queued == 0) {
                if (job.done.load(.acquire)) return job.failure orelse error.TemplateDidNotQueueForReentry;
                if (std.Io.Clock.awake.now(io).nanoseconds >= until.nanoseconds) return error.TemplateDidNotQueueForReentry;
                try io.sleep(.fromMilliseconds(1), .awake);
            }
            job.revoked.store(true, .release);
            while (!job.done.load(.acquire)) {
                if (std.Io.Clock.awake.now(io).nanoseconds >= until.nanoseconds) return error.TemplateDidNotRejectReentry;
                try io.sleep(.fromMilliseconds(1), .awake);
            }
        } else query.finish();
        future.await(io);
        if (reject) try std.testing.expectEqual(error.TemplateSourceRevoked, job.failure.?) else if (job.failure) |err| return err;
        try std.testing.expectEqual(@as(usize, if (reject) 0 else 1), app.embeddings.load(.acquire));
        try std.testing.expect(managed.entries[0].http_control == null);
        try std.testing.expect(managed.entries[0].cancellation == null);
        query.finish();
        try std.testing.expectEqual(@as(usize, if (reject) 0 else 2), scheduler.stats().runnable_slots);
        try std.testing.expectEqual(@as(u64, 4096), scheduler.stats().retained_bytes);
        execution.finish();
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
    }
}

test "serverless execution asset providers own HTTP waits capability discovery and compatibility children" {
    const sources = @import("antfly_local_sources");
    const Runtime = sources.asset_producer_runtime.Runtime;
    const Producer = @typeInfo(@TypeOf(Runtime.producer)).@"fn".return_type.?;
    const Request = @typeInfo(@TypeOf(Producer.produceWithContext)).@"fn".param_types[2].?;
    const Context = @import("antfly_inference_execution_context").RequestContext;
    const Mode = enum { reader, transcriber, generator, extractor, capabilities, parallel };
    const App = struct {
        mode: Mode,
        received: std.atomic.Value(usize) = .init(0),
        release: std.Io.Event = .unset,
        fn execute(raw: *anyopaque, a: std.mem.Allocator, _: sources.common_http_http_common.HttpRequest) !sources.common_http_http_common.HttpResponse {
            const app: *@This() = @ptrCast(@alignCast(raw));
            _ = app.received.fetchAdd(1, .acq_rel);
            try app.release.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
            const body = switch (app.mode) {
                .reader, .generator => "{\"choices\":[{\"finish_reason\":\"stop\",\"message\":{\"role\":\"assistant\",\"content\":\"asset text\"}}]}",
                .transcriber, .parallel => "{\"text\":\"asset text\"}",
                .extractor => "{\"object\":\"extraction\",\"model\":\"g\",\"data\":[{\"entities\":[{\"label\":\"x\",\"text\":\"asset text\"}]}]}",
                .capabilities => "{\"readers\":{\"g\":{\"inputs\":[\"text\",\"image\"],\"inference_capabilities\":{\"task\":\"read\",\"batch\":{\"mode\":\"native\",\"preferred_items\":8,\"max_items\":12,\"max_encoded_bytes\":33554432,\"max_decoded_pixels\":0,\"max_media_parts_per_item\":1,\"per_item_failures\":false}}}}}",
            };
            return .{ .status = 200, .content_type = try a.dupe(u8, "application/json"), .body = try a.dupe(u8, body) };
        }
    };
    const Job = struct {
        producer: Producer,
        request: Request,
        context: Context,
        mode: Mode,
        revoked: std.atomic.Value(bool) = .init(false),
        done: std.atomic.Value(bool) = .init(false),
        failure: ?anyerror = null,
        fn check(raw: *const anyopaque) !void {
            const job: *const @This() = @ptrCast(@alignCast(raw));
            if (job.revoked.load(.acquire)) return error.AssetInvocationRevoked;
        }
        fn invoke(job: *@This()) !void {
            job.context.cancellation = .{ .ptr = job, .check_fn = check };
            if (job.mode == .capabilities) {
                const scoped = job.producer.withInvocationContext(.{ .io = job.context.io, .deadline_ns = job.context.deadline_ns, .cancellation = job.context.cancellation.?, .runnable = job.context.runnable });
                const caps = try scoped.capabilitiesForRequests(std.testing.allocator, &.{job.request});
                try std.testing.expect(caps != null);
                return;
            }
            if (job.mode == .parallel) {
                const outputs = try job.producer.produceBatchWithContext(std.testing.allocator, &.{ job.request, job.request }, job.context);
                defer {
                    for (outputs) |output| std.testing.allocator.free(output);
                    std.testing.allocator.free(outputs);
                }
                try std.testing.expectEqual(@as(usize, 2), outputs.len);
                for (outputs) |output| try std.testing.expect(std.mem.indexOf(u8, output, "asset text") != null);
                return;
            }
            const output = try job.producer.produceWithContext(std.testing.allocator, job.request, job.context);
            defer std.testing.allocator.free(output);
            try std.testing.expect(std.mem.indexOf(u8, output, "asset text") != null);
        }
        fn run(job: *@This()) void {
            defer job.done.store(true, .release);
            job.invoke() catch |err| {
                job.failure = err;
            };
        }
    };
    const a = std.testing.allocator;
    const io = std.testing.io;
    for (std.enums.values(Mode)) |mode| for ([_]bool{ false, true }) |reject| {
        var app: App = .{ .mode = mode };
        var listener = sources.common_http_std_http_listener.StdHttpListener.init(a, .{ .serve_in_connection_threads = true, .max_connection_threads = 8 }, .{ .ptr = &app, .vtable = &.{ .execute = App.execute } });
        defer listener.deinit();
        try listener.start();
        const url = try listener.baseUri(a);
        defer a.free(url);
        var client = @import("httpx").Client.initWithConfig(a, io, .{ .keep_alive = false });
        defer client.deinit();
        var runtime = Runtime.init(a, &client);
        defer runtime.deinit();
        const config = try std.fmt.allocPrint(a, "{{\"provider\":\"{s}\",\"model\":\"g\",\"{s}\":\"{s}\"{s}}}", .{ if (mode == .capabilities) "antfly" else "openai", switch (mode) {
            .reader, .transcriber, .parallel => "base_url",
            else => "url",
        }, url, if (mode == .extractor) ",\"schema\":{\"entities\":[\"x\"]}" else "" });
        defer a.free(config);
        var scheduler = try Runnable.Scheduler.init(.{ .slots = 3, .max_wait_ms = 5000 });
        defer scheduler.close();
        var execution: Runnable.Execution = .{};
        defer execution.finish();
        try execution.startWithSlots(&scheduler, .{ .io = io, .retained_bytes = 4096 }, .background, 2);
        var read: @import("api/runnable_local_read.zig").Read = .{ .execution = &execution, .allocator = a };
        var job: Job = .{ .producer = runtime.producer(), .mode = mode, .request = .{
            .producer_type = switch (mode) {
                .reader, .capabilities => .reader,
                .transcriber, .parallel => .transcriber,
                .generator => .generator,
                .extractor => .extractor,
            },
            .config_json = config,
            .source_text = switch (mode) {
                .reader, .capabilities => "data:image/png;base64,iVBORw0KGgoAAAAAAAAAAAAAAAIAAAAD",
                .transcriber, .parallel => "data:audio/wav;base64,UklGRg==",
                else => "asset text",
            },
            .content_type = "text/plain",
        }, .context = .{ .io = io, .deadline_ns = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s, .runnable = (try read.team()).borrow } };
        var future = try io.concurrent(Job.run, .{&job});
        defer future.cancel(io);
        defer app.release.set(io);
        const until = @import("antfly_platform").time.monotonicNs() + 3 * std.time.ns_per_s;
        while (app.received.load(.acquire) < @as(usize, if (mode == .parallel) 2 else 1) or scheduler.stats().runnable_slots != 0) {
            if (job.done.load(.acquire)) {
                std.debug.print("asset mode {s}: {?}\n", .{ @tagName(mode), job.failure });
                return job.failure orelse error.AssetDidNotWait;
            }
            if (@import("antfly_platform").time.monotonicNs() >= until) {
                std.debug.print("asset wait mode={s} reject={} received={d} slots={d} queued={d} operations={d} children={d}\n", .{ @tagName(mode), reject, app.received.load(.acquire), scheduler.stats().runnable_slots, scheduler.stats().queued, scheduler.stats().operations, execution.entry.?.children });
                return error.AssetKeptCapacity;
            }
            try io.sleep(.fromMilliseconds(1), .awake);
        }
        var query: Runnable.Execution = .{};
        defer query.finish();
        try query.startWithSlots(&scheduler, .{ .io = io, .allow_wait = false }, .query, 2);
        var health: Runnable.Execution = .{};
        defer health.finish();
        try health.start(&scheduler, .{ .io = io, .allow_wait = false }, .control);
        health.finish();
        app.release.set(io);
        if (reject) {
            while (scheduler.stats().queued == 0) {
                if (job.done.load(.acquire)) return job.failure orelse error.AssetDidNotQueue;
                if (@import("antfly_platform").time.monotonicNs() >= until) return error.AssetDidNotQueue;
                try io.sleep(.fromMilliseconds(1), .awake);
            }
            job.revoked.store(true, .release);
            while (!job.done.load(.acquire)) {
                if (@import("antfly_platform").time.monotonicNs() >= until) return error.AssetDidNotReject;
                try io.sleep(.fromMilliseconds(1), .awake);
            }
        } else query.finish();
        future.await(io);
        if (reject) try std.testing.expectEqual(error.AssetInvocationRevoked, job.failure.?) else if (job.failure) |err| {
            std.debug.print("asset mode {s}: {s}\n", .{ @tagName(mode), @errorName(err) });
            return err;
        }
        try std.testing.expect(runtime.http_wait == null and runtime.request_runnable == null);
        query.finish();
        execution.finish();
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
    };
}

test "serverless execution credential vendor process waits preserve publication and physical retirement" {
    if (@import("builtin").os.tag != .linux and @import("builtin").os.tag != .macos) return error.SkipZigTest;
    const aws = @import("antfly_credentials").aws;
    const httpx = @import("httpx");
    const Context = @import("antfly_inference_execution_context");
    const Terminal = enum { success, cancel_wait, cancel_return, cancel_once_wait, cancel_once_return, deadline, process_timeout };
    const Fixture = struct {
        threaded: std.Io.Threaded,
        config_path: []const u8,
        close_pipes: bool,
        started: std.atomic.Value(bool) = .init(false),
        waiting_exit: std.atomic.Value(bool) = .init(false),
        live: std.atomic.Value(usize) = .init(0),
        retired: std.atomic.Value(usize) = .init(0),
        fn from(raw: ?*anyopaque) *@This() {
            const threaded: *std.Io.Threaded = @ptrCast(@alignCast(raw.?));
            return @fieldParentPtr("threaded", threaded);
        }
        fn access(_: ?*anyopaque, _: std.Io.Dir, path: []const u8, _: std.Io.Dir.AccessOptions) std.Io.Dir.AccessError!void {
            // Test authority resolves only the vendor command, without reading
            // the user's AWS files or executing an installed credential tool.
            if (!std.mem.endsWith(u8, path, "/aws")) return error.FileNotFound;
        }
        fn open(raw: ?*anyopaque, dir: std.Io.Dir, _: []const u8, options: std.Io.Dir.OpenFileOptions) std.Io.File.OpenError!std.Io.File {
            const fixture = from(raw);
            const base = fixture.threaded.io();
            return base.vtable.dirOpenFile(raw, dir, fixture.config_path, options);
        }
        fn spawn(raw: ?*anyopaque, options: std.process.SpawnOptions) std.process.SpawnError!std.process.Child {
            const fixture = from(raw);
            if (options.argv.len != 9 or !std.mem.eql(u8, options.argv[1], "configure") or !std.mem.eql(u8, options.argv[2], "export-credentials") or !std.mem.eql(u8, options.argv[4], "fixture-process")) return error.FileNotFound;
            const body = "printf '%s' '{\"Version\":1,\"AccessKeyId\":\"vendor-access\",\"SecretAccessKey\":\"vendor-secret\",\"SessionToken\":\"vendor-session\",\"Expiration\":\"2030-01-02T03:04:05Z\"}'; ";
            const argv_open = [_][]const u8{ "/bin/sh", "-c", body ++ "exec /bin/sleep 0.3" };
            const argv_closed = [_][]const u8{ "/bin/sh", "-c", body ++ "exec 1>&- 2>&-; exec /bin/sleep 0.3" };
            var adapted = options;
            adapted.argv = if (fixture.close_pipes) &argv_closed else &argv_open;
            const base = fixture.threaded.io();
            const child = try base.vtable.processSpawn(raw, adapted);
            _ = fixture.live.fetchAdd(1, .acq_rel);
            fixture.started.store(true, .release);
            return child;
        }
        fn wait(raw: ?*anyopaque, child: *std.process.Child) std.process.Child.WaitError!std.process.Child.Term {
            const fixture = from(raw);
            fixture.waiting_exit.store(true, .release);
            const base = fixture.threaded.io();
            const term = try base.vtable.childWait(raw, child);
            std.debug.assert(child.id == null);
            _ = fixture.live.fetchSub(1, .acq_rel);
            _ = fixture.retired.fetchAdd(1, .acq_rel);
            return term;
        }
        fn kill(raw: ?*anyopaque, child: *std.process.Child) void {
            const fixture = from(raw);
            const active = child.id != null;
            const base = fixture.threaded.io();
            base.vtable.childKill(raw, child);
            std.debug.assert(child.id == null);
            if (active) {
                _ = fixture.live.fetchSub(1, .acq_rel);
                _ = fixture.retired.fetchAdd(1, .acq_rel);
            }
        }
    };
    const Job = struct {
        cache: *aws.CredentialCache,
        client: *httpx.Client,
        scope: Context.HttpWait,
        process_timeout_ms: u64 = 5000,
        revoked: std.atomic.Value(bool) = .init(false),
        once: bool = false,
        consumed: std.atomic.Value(bool) = .init(false),
        done: std.atomic.Value(bool) = .init(false),
        failure: ?anyerror = null,
        fn check(raw: *const anyopaque) !void {
            const job: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            if (job.revoked.load(.acquire) and (!job.once or !job.consumed.swap(true, .acq_rel))) return error.CredentialProcessLeaseLost;
        }
        fn perform(job: *@This()) !void {
            var credentials = try job.cache.getForSourceWithContextAndIo(std.testing.allocator, std.heap.smp_allocator, job.client, job.scope.context.io, "us-east-1", .{ .profile = .{ .name = "fixture-process" } }, aws.RequestContext.init(job.scope.context.io, job.process_timeout_ms, job.scope.context.cancellation).withExecutionWait(job.scope.control()));
            defer credentials.deinit(std.testing.allocator);
            try job.scope.ready();
            try std.testing.expectEqualStrings("vendor-access", credentials.access_key_id);
        }
        fn run(job: *@This()) void {
            defer job.done.store(true, .release);
            job.scope.context.cancellation = .{ .ptr = job, .check_fn = check };
            job.perform() catch |err| {
                job.scope.ready() catch |original| {
                    job.failure = original;
                    return;
                };
                job.failure = err;
            };
        }
    };
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |close_pipes| for (std.enums.values(Terminal)) |terminal| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "config", .data = "[profile fixture-process]\nlogin_session = fixture-session\n" });
        const path = try tmp.dir.realPathFileAlloc(std.testing.io, "config", a);
        defer a.free(path);
        var fixture: Fixture = .{ .threaded = .init(a, .{}), .config_path = path, .close_pipes = close_pipes };
        defer fixture.threaded.deinit();
        var io = fixture.threaded.io();
        var vtable = io.vtable.*;
        vtable.dirAccess = Fixture.access;
        vtable.dirOpenFile = Fixture.open;
        vtable.processSpawn = Fixture.spawn;
        vtable.childWait = Fixture.wait;
        vtable.childKill = Fixture.kill;
        io.vtable = &vtable;
        var client = httpx.Client.init(a, io);
        defer client.deinit();
        var cache: aws.CredentialCache = .{};
        defer cache.deinit(std.heap.smp_allocator);
        var scheduler = try Runnable.Scheduler.init(.{ .slots = 3, .max_wait_ms = 5000 });
        defer scheduler.close();
        var execution: Runnable.Execution = .{};
        defer execution.finish();
        try execution.startWithSlots(&scheduler, .{ .io = io, .retained_bytes = 4096 }, .background, 2);
        var read: @import("api/runnable_local_read.zig").Read = .{ .execution = &execution, .allocator = a };
        var job: Job = .{ .once = terminal == .cancel_once_wait or terminal == .cancel_once_return, .process_timeout_ms = if (terminal == .process_timeout) 150 else 5000, .cache = &cache, .client = &client, .scope = .{ .context = .{ .io = io, .deadline_ns = @import("antfly_platform").time.monotonicNs() + if (terminal == .deadline) @as(u64, 150 * std.time.ns_per_ms) else 5 * std.time.ns_per_s, .runnable = (try read.team()).borrow } } };
        var future = try io.concurrent(Job.run, .{&job});
        defer future.cancel(io);
        const until = @import("antfly_platform").time.monotonicNs() + 3 * std.time.ns_per_s;
        while (!fixture.started.load(.acquire) or scheduler.stats().runnable_slots != 0 or (close_pipes and !fixture.waiting_exit.load(.acquire))) {
            if (job.done.load(.acquire)) return job.failure orelse error.CredentialProcessDidNotWait;
            if (@import("antfly_platform").time.monotonicNs() >= until) return error.CredentialProcessKeptCapacity;
            try io.sleep(.fromMilliseconds(1), .awake);
        }
        {
            try std.testing.expect(cache.mutex.tryLock());
            defer cache.mutex.unlock(io);
            try std.testing.expect(cache.refreshing);
        }
        try std.testing.expectEqual(@as(usize, 1), fixture.live.load(.acquire));
        var query: Runnable.Execution = .{};
        defer query.finish();
        try query.startWithSlots(&scheduler, .{ .io = io, .allow_wait = false }, .query, 2);
        var health: Runnable.Execution = .{};
        defer health.finish();
        try health.start(&scheduler, .{ .io = io, .allow_wait = false }, .control);
        health.finish();
        if (terminal == .cancel_wait or terminal == .cancel_once_wait) job.revoked.store(true, .release);
        if (terminal == .success or terminal == .cancel_return or terminal == .cancel_once_return) {
            while (scheduler.stats().queued == 0) {
                if (job.done.load(.acquire)) return job.failure orelse error.CredentialProcessDidNotQueue;
                if (@import("antfly_platform").time.monotonicNs() >= until) return error.CredentialProcessDidNotQueue;
                try io.sleep(.fromMilliseconds(1), .awake);
            }
            if (terminal == .success) query.finish() else job.revoked.store(true, .release);
        }
        while (!job.done.load(.acquire)) {
            if (@import("antfly_platform").time.monotonicNs() >= until) return error.CredentialProcessDidNotRetire;
            try io.sleep(.fromMilliseconds(1), .awake);
        }
        future.await(io);
        if (terminal == .success) {
            if (job.failure) |err| return err;
        } else try std.testing.expectEqual(if (terminal == .deadline or terminal == .process_timeout) error.Timeout else error.CredentialProcessLeaseLost, job.failure.?);
        try std.testing.expectEqual(@as(usize, 0), fixture.live.load(.acquire));
        try std.testing.expectEqual(@as(usize, 1), fixture.retired.load(.acquire));
        try std.testing.expectEqual(terminal == .success, cache.cached != null);
        try std.testing.expect(!cache.refreshing);
        query.finish();
        execution.finish();
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
    };
}

test "serverless execution held lease renewals own control work and keep watchdogs passive" {
    const leases = @import("serverless/build/work_lease.zig");
    const maintenance = @import("serverless/maintenance_cancellation.zig");
    const objectstore = @import("objectstore");
    const FsProgress = @import("serverless/catalog/fs_progress_store.zig").FsProgressStore;
    const ObjectLease = @import("serverless/build/object_work_lease_store.zig").ObjectWorkLeaseStore;
    const Mode = enum { success, canceled, deadline, takeover };
    const Fixture = struct {
        threaded: std.Io.Threaded,
        armed: std.atomic.Value(bool) = .init(false),
        entered: std.atomic.Value(bool) = .init(false),
        release: std.Io.Event = .unset,
        calls: std.atomic.Value(usize) = .init(0),
        fn shiftedNow(raw: ?*anyopaque, clock_id: std.Io.Clock) std.Io.Timestamp {
            const threaded: *std.Io.Threaded = @ptrCast(@alignCast(raw.?));
            const base = threaded.io();
            const timestamp = base.vtable.now(raw, clock_id);
            // Acquisition and renewal must use the supplied clock authority;
            // consulting the host wall clock would immediately expire this lease.
            return if (clock_id == .real) .fromNanoseconds(timestamp.toNanoseconds() - std.time.ns_per_day) else timestamp;
        }
        fn sync(raw: ?*anyopaque, file: std.Io.File) std.Io.File.SyncError!void {
            const threaded: *std.Io.Threaded = @ptrCast(@alignCast(raw.?));
            const self: *@This() = @fieldParentPtr("threaded", threaded);
            _ = self.calls.fetchAdd(1, .acq_rel);
            const io = threaded.io();
            if (self.armed.swap(false, .acq_rel)) {
                self.entered.store(true, .release);
                try self.release.wait(io);
            }
            return io.vtable.fileSync(raw, file);
        }
    };
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |objects| for ([_]bool{ false, true }) |is_bootstrap| for (std.enums.values(Mode)) |mode| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", a);
        defer a.free(root);
        var fixture: Fixture = .{ .threaded = .init(a, .{}) };
        defer fixture.threaded.deinit();
        var io = fixture.threaded.io();
        var vtable = io.vtable.*;
        vtable.fileSync = Fixture.sync;
        vtable.now = Fixture.shiftedNow;
        io.vtable = &vtable;
        const cwd = try std.Io.Dir.cwd().realPathFileAlloc(io, ".", a);
        defer a.free(cwd);
        const relative_root = try std.fs.path.relativeAlloc(a, cwd, null, cwd, root);
        defer a.free(relative_root);
        var native = try FsProgress.init(a, if (is_bootstrap) relative_root else root);
        defer native.deinit();
        var filesystem = try objectstore.FilesystemClient.initWithIo(a, root, io);
        defer filesystem.deinit();
        var store = try ObjectLease.initWithClient(a, filesystem.client(), "leases", "");
        defer store.deinit();
        var progress = native.progressStore();
        const provider = if (objects) store.provider() else try progress.workLeaseProvider();
        const ttl = 800 * std.time.ns_per_ms;
        var held = (try leases.acquireHeld(provider, io, "docs", "one", ttl)).?;
        var initial = (try leases.acquireBootstrapHeld(provider, io, "bootstrap", "one", ttl)).?;
        defer _ = held.release() catch false;
        defer _ = initial.release() catch false;
        var scheduler = try Runnable.Scheduler.init(.{ .slots = 3, .max_wait_ms = 5000 });
        defer scheduler.close();
        var execution: Runnable.Execution = .{};
        defer execution.finish();
        try execution.startWithSlots(&scheduler, .{ .io = io, .retained_bytes = 4096 }, .background, 2);
        var canceled = std.atomic.Value(bool).init(false);
        const token: maintenance.Token = .{ .io = io, .execution = &execution, .requested = &canceled, .deadline_ns = @import("antfly_platform").time.monotonicNs() + if (mode == .deadline) @as(u64, 400 * std.time.ns_per_ms) else 5 * std.time.ns_per_s };
        const control = if (is_bootstrap) try initial.cancellation(token) else try held.cancellation(token);
        const renewal = if (is_bootstrap) &initial.renewal else &held.renewal;
        const original_expiry = renewal.unix_deadline.load(.acquire);
        defer renewal.stop();
        fixture.armed.store(true, .release);
        try execution.park(.resource_blocked);
        const until = @import("antfly_platform").time.monotonicNs() + 3 * std.time.ns_per_s;
        while (!fixture.entered.load(.acquire)) {
            try control.check();
            if (@import("antfly_platform").time.monotonicNs() >= until) return error.LeaseRenewalDidNotEnterProvider;
            try io.sleep(.fromMilliseconds(1), .awake);
        }
        defer fixture.release.set(io);
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().runnable_slots);
        // Watchdog checks must neither wait for the provider mutex nor perform
        // another provider request while the renewal sync is physically blocked.
        const calls = fixture.calls.load(.acquire);
        for (0..8) |_| try control.check();
        try std.testing.expectEqual(calls, fixture.calls.load(.acquire));
        var query: Runnable.Execution = .{};
        defer query.finish();
        try query.startWithSlots(&scheduler, .{ .io = io, .allow_wait = false }, .query, 2);
        var health: Runnable.Execution = .{};
        defer health.finish();
        try health.start(&scheduler, .{ .io = io, .allow_wait = false }, .control);
        health.finish();
        if (mode == .canceled) canceled.store(true, .release);
        if (mode == .deadline) {
            while (@import("antfly_platform").time.monotonicNs() < token.deadline_ns.?) try io.sleep(.fromMilliseconds(1), .awake);
        }
        if (mode == .canceled) try std.testing.expectError(error.Canceled, control.check());
        if (mode == .deadline) try std.testing.expectError(error.DeadlineExceeded, control.check());
        fixture.release.set(io);
        if (mode == .success or mode == .takeover) {
            while (renewal.unix_deadline.load(.acquire) <= original_expiry) {
                try control.check();
                if (@import("antfly_platform").time.monotonicNs() >= until) return error.LeaseRenewalDidNotPublish;
                try io.sleep(.fromMilliseconds(1), .awake);
            }
            if (mode == .takeover) {
                const now = @as(u64, @intCast(std.Io.Timestamp.now(io, .real).toNanoseconds())) + ttl * 2;
                const namespace = if (is_bootstrap) "bootstrap" else "docs";
                const other = (try provider.acquire(namespace, "two", now, ttl)).?;
                while (true) {
                    control.check() catch |err| {
                        try std.testing.expectEqual(error.WorkLeaseLost, err);
                        break;
                    };
                    if (@import("antfly_platform").time.monotonicNs() >= until) return error.LeaseTakeoverWasNotObserved;
                    try io.sleep(.fromMilliseconds(1), .awake);
                }
                try provider.validate(namespace, "two", other.fencing_token, now);
            }
        }
        query.finish();
        if (mode == .success) {
            try execution.enter(.background);
            const guard = if (is_bootstrap) initial.guard() else held.guard();
            try std.testing.expect((try guard.preparePublication(if (is_bootstrap) "bootstrap" else "docs")) != null);
            try std.testing.expectEqual(@as(usize, 2), scheduler.stats().runnable_slots);
        }
        if (is_bootstrap) _ = try initial.release() else _ = try held.release();
        try std.testing.expect(!renewal.active());
        execution.finish();
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
    };
}
