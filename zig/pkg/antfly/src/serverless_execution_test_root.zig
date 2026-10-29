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
        waits: usize = 0,
        lost: bool = false,
        reject_return: bool = false,
        fn observe(self: *@This()) !void {
            if (self.execution.entry.?.state != .resource_blocked) return;
            var unrelated: Runnable.Execution = .{};
            defer unrelated.finish();
            try unrelated.start(self.scheduler, .{ .io = std.testing.io, .allow_wait = false }, .query);
            self.waits += 1;
        }
        fn authority(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.lost) return error.WorkLeaseLost;
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
            if (self.reject_return) self.lost = true;
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
        try std.testing.expectEqual(reserved_return, probe.waits != 0);
        const before = probe.waits;
        try std.testing.expectEqual(@as(u64, 1), try catalog.wal.latestLsn("docs"));
        const records = try catalog.wal.readFromAlloc("docs", 1);
        defer @import("serverless/wal/mod.zig").freeRecords(catalog.wal.allocator, records);
        try std.testing.expectEqual(@as(usize, 1), records.len);
        try std.testing.expect(probe.waits > before);
        const token: maintenance.Token = .{ .io = std.testing.io, .execution = &execution, .cooperative = .{ .ptr = &probe, .check_fn = Probe.original } };
        var publication = try catalog.buildNamespaceGuardedUntil("docs", null, token);
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
