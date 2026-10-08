// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Actual listener/TCP output and checked shared cleanup execution ownership.
const std = @import("std");
const runtime = @import("antfly_runtime_abi");
const httpx = @import("httpx");
const owner_module = @import("runnable_owner_execution.zig");
const runnable = @import("antfly_local_sources").common_request_admission.runnable_scheduler;

const Observer = struct {
    state: *owner_module.State,
    adapter: *runtime.http_cleanup.Owner,
    io: std.Io,
    connection: ?*httpx.h2_connection.H2Connection = null,
    socket: ?*httpx.socket.Socket = null,
    starts: std.atomic.Value(usize) = .init(0),
    parks: std.atomic.Value(usize) = .init(0),
    finishes: std.atomic.Value(usize) = .init(0),
    retained: u64 = 0,
    invalid: std.atomic.Value(bool) = .init(false),
    retired_before_finish: std.atomic.Value(bool) = .init(false),
    fn view(self: *Observer) httpx.server_mod.CleanupExecution {
        return .{ .context = self, .start = start, .park = park, .enter = enter, .finish = finish };
    }
    fn start(raw: *anyopaque, io: std.Io, deadline: u64, bytes: u64) !*anyopaque {
        const self: *Observer = @ptrCast(@alignCast(raw));
        if (self.connection) |connection| {
            if (!connection.write_mutex.tryLock()) return error.AdmissionHeldConnectionMutex;
            connection.write_mutex.unlock(io);
        }
        const view_ = self.adapter.view();
        const handle = try view_.start(view_.context, io, deadline, bytes);
        const stats = self.state.owner.scheduler.stats();
        try std.testing.expectEqual(@as(usize, 3), stats.runnable_slots);
        try std.testing.expectEqual(@as(usize, 1), stats.control_slots);
        self.retained = stats.retained_bytes;
        _ = self.starts.fetchAdd(1, .release);
        return handle;
    }
    fn park(raw: *anyopaque, handle: *anyopaque) !void {
        const self: *Observer = @ptrCast(@alignCast(raw));
        const view_ = self.adapter.view();
        try view_.park(view_.context, handle);
        const parked = self.state.owner.scheduler.stats();
        try std.testing.expectEqual(@as(usize, 2), parked.runnable_slots);
        try std.testing.expectEqual(@as(usize, 0), parked.control_slots);
        try std.testing.expectEqual(self.retained, parked.retained_bytes);
        try std.testing.expect(self.state.cleanup_in_use.load(.acquire));
        // Real protected work can acquire capacity while transport IO owns the
        // parked cleanup. A second cleanup cannot spend health headroom.
        const deadline: u64 = @intCast(@max(0, std.Io.Clock.now(.awake, self.io).nanoseconds) + 249 * std.time.ns_per_ms);
        try std.testing.expectError(error.ResourceTemporarilyUnavailable, view_.start(view_.context, self.io, deadline, 4096));
        const port = self.state.port();
        const borrow = runtime.native_abi.IoBorrow.init(&self.io);
        var health: ?*anyopaque = null;
        const status = port.start(port.context, &.{ .io = &borrow, .class = @backingInt(runtime.runnable_abi.Class.control), .allow_wait = 0, .out_handle = &health });
        if (!status.isOk()) return runtime.error_abi.errorFromStatus(status);
        port.finish(health.?);
        try std.testing.expectEqual(self.retained, self.state.owner.scheduler.stats().retained_bytes);
        _ = self.parks.fetchAdd(1, .release);
    }
    fn enter(raw: *anyopaque, handle: *anyopaque) !void {
        const self: *Observer = @ptrCast(@alignCast(raw));
        if (self.connection) |connection| {
            if (!connection.write_mutex.tryLock()) return error.ResumeHeldConnectionMutex;
            connection.write_mutex.unlock(self.io);
        }
        const view_ = self.adapter.view();
        try view_.enter(view_.context, handle);
        try std.testing.expectEqual(@as(usize, 3), self.state.owner.scheduler.stats().runnable_slots);
    }
    fn finish(raw: *anyopaque, handle: *anyopaque) void {
        const self: *Observer = @ptrCast(@alignCast(raw));
        const stats = self.state.owner.scheduler.stats();
        if (stats.operations != 2 or stats.retained_bytes != self.retained or !self.state.cleanup_in_use.load(.acquire)) self.invalid.store(true, .release);
        if (self.connection != null) {
            _ = self.socket.?.send("unowned") catch blk: {
                self.retired_before_finish.store(true, .release);
                break :blk @as(usize, 0);
            };
        }
        const view_ = self.adapter.view();
        view_.finish(view_.context, handle);
        _ = self.finishes.fetchAdd(1, .release);
    }
    fn handler(context: *httpx.Context) anyerror!httpx.Response {
        const self: *Observer = @ptrCast(@alignCast(context.route_data.?));
        self.connection = context.h2;
        self.socket = context.h2_sock orelse context.h1_sock;
        return error.SchedulerFull;
    }
};

fn run(comptime h2_credit_stall: bool) !void {
    const allocator = std.testing.allocator;
    var lane = std.Io.Threaded.init(allocator, .{ .async_limit = .nothing, .concurrent_limit = .limited(8) });
    defer lane.deinit();
    const io = lane.io();
    var state: owner_module.State = .{ .owner_alloc = allocator, .owner = try runnable.NodeOwner.init(.{ .slots = 3, .max_operations = 8 }), .native_now_ns = struct {
        fn now() u64 {
            return 0;
        }
    }.now };
    defer state.owner.tryDeinit() catch @panic("owner leaked");
    var adapter = try runtime.http_cleanup.Owner.init(state.port());
    defer adapter.deinit();
    const port = state.port();
    const borrow = runtime.native_abi.IoBorrow.init(&io);
    var ordinary: ?*anyopaque = null;
    const status = port.start(port.context, &.{ .io = &borrow, .class = @backingInt(runtime.runnable_abi.Class.query), .slots = 2, .retained_bytes = 17, .out_handle = &ordinary });
    if (!status.isOk()) return runtime.error_abi.errorFromStatus(status);
    defer port.finish(ordinary.?);
    var observer = Observer{ .state = &state, .adapter = &adapter, .io = io };
    var server = httpx.Server.initWithConfig(allocator, io, .{ .host = "127.0.0.1", .port = 0, .max_connections = 4, .max_request_tasks = 4, .h1_disconnect_cancellation = .disabled, .borrow_http_runtime_io = true, .response_write_timeout_ms = 1000, .cleanup_execution = observer.view() });
    defer server.deinit();
    try server.routeWithData(.GET, "/deny", Observer.handler, &observer);
    var task = httpx.Server.ListenerTask.init(&server);
    try task.start();
    defer {
        task.requestStop();
        task.join() catch {};
    }
    const client_io = std.Io.Threaded.global_single_threaded.io();
    var client = try httpx.socket.Socket.connect(server.boundAddress().?, client_io);
    defer client.close();
    client.enableNativeTimeouts();
    try client.setRecvTimeout(2000);
    if (h2_credit_stall) {
        var h2 = httpx.h2_connection.H2Connection.initClient(allocator, client_io);
        defer h2.deinit();
        h2.local_settings.initial_window_size = 0;
        try client.sendAll(httpx.http.HTTP2_PREFACE);
        try h2.sendSettings(&client);
        try h2.sendHeaders(&client, 1, &.{ .{ .name = ":method", .value = "GET" }, .{ .name = ":scheme", .value = "http" }, .{ .name = ":authority", .value = "localhost" }, .{ .name = ":path", .value = "/deny" } }, true);
        var saw_status = false;
        var saw_retry = false;
        var saw_ping_ack = false;
        while (true) {
            var frame = h2.readFrame(&client) catch |err| {
                if (err == error.ConnectionClosed) break;
                return err;
            };
            defer frame.deinit(allocator);
            switch (frame.header.frame_type) {
                .settings => if (frame.header.flags & httpx.h2_connection.H2Connection.FLAG_ACK == 0) {
                    try h2.sendSettingsAck(&client);
                },
                .headers => {
                    const decoded = try h2.decodeFrameHeaders(frame.payload, frame.header.flags);
                    defer httpx.stream.freeDecodedHeaders(allocator, decoded.headers);
                    for (decoded.headers) |header| {
                        if (std.mem.eql(u8, header.name, ":status") and std.mem.eql(u8, header.value, "503")) saw_status = true;
                        if (std.mem.eql(u8, header.name, "retry-after") and std.mem.eql(u8, header.value, "1")) saw_retry = true;
                    }
                    try h2.sendPing(&client, @splat(7));
                },
                .ping => {
                    if (frame.header.flags & httpx.h2_connection.H2Connection.FLAG_ACK != 0 and std.mem.eql(u8, frame.payload, &@as([8]u8, @splat(7)))) saw_ping_ack = true;
                },
                .data => return error.ErrorBodyIgnoredZeroCredit,
                else => {},
            }
        }
        try std.testing.expect(saw_status and saw_retry and saw_ping_ack);
    } else {
        try client.sendAll("GET /deny HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n");
        var bytes: [2048]u8 = undefined;
        var used: usize = 0;
        while (used < bytes.len) {
            const count = try client.recv(bytes[used..]);
            if (count == 0) break;
            used += count;
        }
        try std.testing.expect(used > 0 and used < bytes.len);
        try std.testing.expect(std.mem.indexOf(u8, bytes[0..used], " 503 ") != null);
        try std.testing.expect(std.ascii.findIgnoreCase(bytes[0..used], "retry-after: 1\r\n") != null);
    }
    const until = std.Io.Clock.now(.awake, io).nanoseconds + std.time.ns_per_s;
    while (observer.finishes.load(.acquire) == 0) {
        if (std.Io.Clock.now(.awake, io).nanoseconds >= until) return error.CleanupDidNotFinish;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expectEqual(@as(usize, 1), observer.starts.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), observer.finishes.load(.acquire));
    try std.testing.expect(observer.parks.load(.acquire) > 0);
    try std.testing.expect(!observer.invalid.load(.acquire));
    if (h2_credit_stall) try std.testing.expect(observer.retired_before_finish.load(.acquire));
    try std.testing.expect(!state.cleanup_in_use.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), state.owner.scheduler.stats().operations);
}

test "runnable HTTP cleanup real shared owner returns retryable H1 error with ordinary CPU full" {
    try run(false);
}
test "runnable HTTP cleanup real shared owner parks on H2 credit permits ping and health and retires before finish" {
    try run(true);
}
