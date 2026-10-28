// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Real HTTP contexts and listener output retain the original SQL input owner.
const std = @import("std");
const httpx = @import("httpx");
const input = @import("sql_input_capsule.zig");
const scheduler = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
pub const antfly_sources = struct {};
pub const consumer_tests_only = true;
const Token = struct {
    context: *const httpx.Context,
    fn requested(raw: *const anyopaque) bool {
        return @as(*const @This(), @ptrCast(@alignCast(raw))).context.isCancellationRequested();
    }
};
const Input = input.Capsule(input.Request);
const Frame = input.OwnedFrame(Token);
const Clock = struct {
    var value: u64 = 10;
    fn now() u64 {
        return value;
    }
};

// The frame creation reference and capsule route reference both retire here;
// only the transport context retains the returned, non-owning observation.
fn openContextOwned(ctx: *httpx.Context, node: *scheduler.NodeOwner, native_clock: bool) !*Input {
    const a = std.testing.allocator;
    const frame = try Frame.create(a, node, .{ .context = ctx });
    defer frame.release();
    const owned = try Input.create(a, a, frame.parents(), &node.scheduler, .{ .io = ctx.io, .native_now_ns = if (native_clock) Clock.now else null, .deadline_ns = if (native_clock) 30 else null, .cancellation = .{ .ptr = &frame.cancellation, .requested = Token.requested } }, .query, "{\"statement\":\"SELECT $1\",\"parameters\":[\"\\u03bb\"]}", 8 << 20);
    defer owned.release();
    try owned.retainUntilContextRetires(ctx);
    try owned.start();
    try owned.decode(.{ .parse_numbers = false });
    input.parkForHandoff(&owned.execution);
    return owned;
}

test "SQL HTTP lifetime survives route stack retirement and response destruction until physical context retirement" {
    const a = std.testing.allocator;
    for ([_]enum { success, cancel, expire, shutdown }{ .success, .cancel, .expire, .shutdown }) |mode| {
        var node = try scheduler.NodeOwner.init(.{ .slots = 2 });
        defer node.deinit();
        var request = try httpx.Request.init(a, .POST, "http://localhost/db/v1/sql");
        defer request.deinit();
        var ctx = httpx.Context.init(a, std.testing.io, &request);
        var live = true;
        defer if (live) ctx.deinit();
        var canceled = std.atomic.Value(bool).init(false);
        ctx.cancellation = &canceled;
        Clock.value = 10;
        const owned = try openContextOwned(&ctx, &node, true);
        const entry = &owned.execution.entry.?;
        const charge = entry.bytes;
        try std.testing.expectEqualStrings("λ", owned.parsed.?.value.parameters.?[0].string);
        try std.testing.expectEqual(@as(usize, 1), node.borrowers);
        try std.testing.expectEqual(@as(usize, 1), node.scheduler.stats().operations);
        try std.testing.expectEqual(@as(usize, 0), node.scheduler.stats().runnable_slots);
        try std.testing.expectError(error.InvalidRunnableTransition, owned.retainUntilContextRetires(&ctx));
        var response = try ctx.text("encoded response");
        response.deinit();
        try std.testing.expectEqual(@as(usize, 1), node.scheduler.stats().operations);
        var control: scheduler.Execution = .{};
        try control.start(&node.scheduler, .{ .io = std.testing.io, .allow_wait = false }, .control);
        control.finish();
        switch (mode) {
            .success => try owned.execution.checkLifetime(),
            .cancel => {
                canceled.store(true, .release);
                try std.testing.expectError(error.Canceled, owned.execution.checkLifetime());
            },
            .expire => {
                Clock.value = 31;
                try std.testing.expectError(error.DeadlineExceeded, owned.execution.checkLifetime());
            },
            .shutdown => {
                node.close();
                try std.testing.expectError(error.SchedulerClosed, owned.execution.checkLifetime());
            },
        }
        try std.testing.expect(entry == &owned.execution.entry.?);
        try std.testing.expectEqual(charge, node.scheduler.stats().retained_bytes);
        try std.testing.expectError(error.InvalidRunnableTransition, node.tryDeinit());
        ctx.deinit();
        live = false;
        try std.testing.expectEqual(@as(usize, 0), node.borrowers);
        try std.testing.expectEqual(@as(usize, 0), node.scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), node.scheduler.stats().retained_bytes);
    }
}

test "SQL HTTP lifetime context registration allocation failure returns all temporary pins" {
    const a = std.testing.allocator;
    var node = try scheduler.NodeOwner.init(.{ .slots = 2 });
    defer node.deinit();
    var request = try httpx.Request.init(a, .POST, "http://localhost/db/v1/sql");
    defer request.deinit();
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    var ctx = httpx.Context.init(failing.allocator(), std.testing.io, &request);
    defer ctx.deinit();
    const frame = try Frame.create(a, &node, .{ .context = &ctx });
    var frame_live = true;
    defer if (frame_live) frame.release();
    const owned = try Input.create(a, a, frame.parents(), &node.scheduler, .{ .io = std.testing.io }, .query, "{}", 8 << 20);
    var owned_live = true;
    defer if (owned_live) owned.release();
    try std.testing.expectError(error.OutOfMemory, owned.retainUntilContextRetires(&ctx));
    try std.testing.expectEqual(@as(usize, 1), owned.references.load(.acquire));
    try std.testing.expectEqual(@as(usize, 2), frame.references.load(.acquire));
    owned.release();
    owned_live = false;
    frame.release();
    frame_live = false;
    try std.testing.expectEqual(@as(usize, 0), node.borrowers);
    try std.testing.expectEqual(@as(usize, 0), node.scheduler.stats().operations);
}

test "SQL HTTP lifetime rejects context escape for a borrowed handler frame" {
    const a = std.testing.allocator;
    var node = try scheduler.NodeOwner.init(.{ .slots = 2 });
    defer node.deinit();
    var request = try httpx.Request.init(a, .POST, "http://localhost/db/v1/sql");
    defer request.deinit();
    var ctx = httpx.Context.init(a, std.testing.io, &request);
    defer ctx.deinit();
    var scoped: input.ScopedFrame = .{ .node = &node };
    const owned = try Input.create(a, a, .{ .context = &scoped, .retain = input.ScopedFrame.retain, .release = input.ScopedFrame.release }, &node.scheduler, .{ .io = std.testing.io }, .query, "{}", 8 << 20);
    defer owned.release();
    try std.testing.expectError(error.InvalidRunnableTransition, owned.retainUntilContextRetires(&ctx));
    try std.testing.expectEqual(@as(usize, 1), owned.references.load(.acquire));
}

const ListenerOwner = struct {
    node: *scheduler.NodeOwner,
    fn handler(ctx: *httpx.Context) anyerror!httpx.Response {
        const self: *@This() = @ptrCast(@alignCast(ctx.route_data.?));
        // The route stack drops both references inside this helper. The
        // response writer can only keep the root through Context's pin.
        _ = try openContextOwned(ctx, self.node, false);
        return ctx.text("SQL response retained through physical delivery");
    }
};

fn listenerLifetime(comptime mode: enum { h1, h2_credit, h2_reset }) !void {
    const a = std.testing.allocator;
    var lane = std.Io.Threaded.init(a, .{ .async_limit = .nothing, .concurrent_limit = .limited(8) });
    defer lane.deinit();
    const io = lane.io();
    var node = try scheduler.NodeOwner.init(.{ .slots = 2 });
    defer node.deinit();
    var observer = ListenerOwner{ .node = &node };
    var server = httpx.Server.initWithConfig(a, io, .{ .host = "127.0.0.1", .port = 0, .max_connections = 4, .max_request_tasks = 4, .h1_disconnect_cancellation = .disabled, .borrow_http_runtime_io = true, .response_write_timeout_ms = 3000 });
    defer server.deinit();
    try server.routeWithData(.GET, "/hold", ListenerOwner.handler, &observer);
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
    if (mode == .h1) {
        try client.sendAll("GET /hold HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
        var bytes: [2048]u8 = undefined;
        var used: usize = 0;
        while (used < bytes.len) {
            const count = try client.recv(bytes[used..]);
            if (count == 0) break;
            used += count;
        }
        try std.testing.expect(std.mem.indexOf(u8, bytes[0..used], " 200 ") != null);
        try std.testing.expect(std.mem.indexOf(u8, bytes[0..used], "SQL response retained through physical delivery") != null);
    } else {
        var h2 = httpx.h2_connection.H2Connection.initClient(a, client_io);
        defer h2.deinit();
        h2.local_settings.initial_window_size = 0;
        try client.sendAll(httpx.http.HTTP2_PREFACE);
        try h2.sendSettings(&client);
        try h2.sendHeaders(&client, 1, &.{ .{ .name = ":method", .value = "GET" }, .{ .name = ":scheme", .value = "http" }, .{ .name = ":authority", .value = "localhost" }, .{ .name = ":path", .value = "/hold" } }, true);
        var headers_seen = false;
        var ping_seen = false;
        while (!ping_seen) {
            var frame = try h2.readFrame(&client);
            defer frame.deinit(a);
            switch (frame.header.frame_type) {
                .settings => if (frame.header.flags & httpx.h2_connection.H2Connection.FLAG_ACK == 0) {
                    try h2.sendSettingsAck(&client);
                },
                .headers => {
                    const decoded = try h2.decodeFrameHeaders(frame.payload, frame.header.flags);
                    defer httpx.stream.freeDecodedHeaders(a, decoded.headers);
                    for (decoded.headers) |header| {
                        if (std.mem.eql(u8, header.name, ":status")) try std.testing.expectEqualStrings("200", header.value);
                    }
                    headers_seen = true;
                    try h2.sendPing(&client, @splat(7));
                },
                .ping => {
                    ping_seen = frame.header.flags & httpx.h2_connection.H2Connection.FLAG_ACK != 0 and std.mem.eql(u8, frame.payload, &@as([8]u8, @splat(7)));
                },
                .data => return error.BodyIgnoredZeroCredit,
                else => {},
            }
        }
        try std.testing.expect(headers_seen);
        const retained = node.scheduler.stats();
        try std.testing.expectEqual(@as(usize, 1), retained.operations);
        try std.testing.expectEqual(@as(usize, 0), retained.runnable_slots);
        try std.testing.expect(retained.retained_bytes > 0);
        try std.testing.expectError(error.InvalidRunnableTransition, node.tryDeinit());
        var control: scheduler.Execution = .{};
        try control.start(&node.scheduler, .{ .io = io, .allow_wait = false }, .control);
        control.finish();
        try std.testing.expectEqual(retained.retained_bytes, node.scheduler.stats().retained_bytes);
        if (mode == .h2_reset) {
            try h2.sendRstStream(&client, 1, .cancel);
        } else {
            try h2.sendWindowUpdate(&client, 1, 1024);
            var delivered: [1024]u8 = undefined;
            var used: usize = 0;
            while (true) {
                var frame = try h2.readFrame(&client);
                defer frame.deinit(a);
                if (frame.header.frame_type == .data) {
                    if (frame.payload.len > delivered.len - used) return error.ResponseTooLarge;
                    @memcpy(delivered[used..][0..frame.payload.len], frame.payload);
                    used += frame.payload.len;
                    if (frame.header.flags & httpx.h2_connection.H2Connection.FLAG_END_STREAM != 0) break;
                }
            }
            try std.testing.expectEqualStrings("SQL response retained through physical delivery", delivered[0..used]);
        }
    }
    const until = std.Io.Clock.now(.awake, io).nanoseconds + 2 * std.time.ns_per_s;
    while (node.scheduler.stats().operations != 0) {
        if (std.Io.Clock.now(.awake, io).nanoseconds >= until) return error.SQLRootDidNotRetire;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    // Join transport tasks before checking non-atomic parent pins and tearing
    // down the scheduler. A finished root can precede the final parent free.
    client.shutdown();
    task.requestStop();
    try task.join();
    try std.testing.expectEqual(@as(usize, 0), node.borrowers);
    try std.testing.expectEqual(@as(u64, 0), node.scheduler.stats().retained_bytes);
}

test "SQL HTTP lifetime real H1 retires after complete response" {
    try listenerLifetime(.h1);
}
test "SQL HTTP lifetime real H2 zero credit retains original root permits control and retires after delivery" {
    try listenerLifetime(.h2_credit);
}
test "SQL HTTP lifetime real H2 reset retires stalled response and original root" {
    try listenerLifetime(.h2_reset);
}
