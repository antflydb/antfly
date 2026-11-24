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

//! Socket-idle delivery and acknowledgement-after-flush contracts.
const std = @import("std");
const backend = @import("backend.zig");
const protocol = @import("protocol.zig");
const frames = @import("cursor_reference_test_support.zig");
const types = @import("antfly_local_sources").sql_ast.notifications;
const Mock = struct {
    listening: std.atomic.Value(bool) = .init(false),
    pending: std.atomic.Value(bool) = .init(false),
    acknowledgements: std.atomic.Value(u32) = .init(0),
    acknowledged: std.Io.Event = .unset,
    on_execute: bool = false,
    payload: []const u8 = "idle-event",
    fn source(self: *Mock) backend.Backend {
        return .{ .context = self, .vtable = &.{ .authenticate = authenticate, .describe = describe, .execute = execute, .disconnect = disconnect, .notifications = notifications, .has_notifications = hasNotifications } };
    }
    fn authenticate(raw: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8) !backend.Identity {
        return .{ .context = raw, .release = release };
    }
    fn release(_: *anyopaque, _: std.mem.Allocator) void {}
    fn describe(_: *anyopaque, _: std.mem.Allocator, _: backend.Identity, _: backend.Request) !backend.Description {
        return .{ .columns = &.{} };
    }
    fn execute(raw: *anyopaque, _: std.mem.Allocator, _: backend.Identity, request: backend.Request) !backend.Result {
        const self: *Mock = @ptrCast(@alignCast(raw));
        try std.testing.expectEqualStrings("LISTEN events", request.statement);
        self.listening.store(true, .release);
        if (self.on_execute) self.pending.store(true, .release);
        return .{ .columns = &.{}, .rows = &.{}, .command_tag = "LISTEN" };
    }
    fn disconnect(_: *anyopaque, _: backend.Identity, _: ?[]const u8) void {}
    fn hasNotifications(raw: *anyopaque, _: backend.Identity) bool {
        const self: *Mock = @ptrCast(@alignCast(raw));
        return self.listening.load(.acquire);
    }
    fn notifications(raw: *anyopaque, a: std.mem.Allocator, _: backend.Identity, request: backend.Request, control: types.DeliveryControl) !types.Page {
        const self: *Mock = @ptrCast(@alignCast(raw));
        switch (control) {
            .poll => {
                if (!self.pending.load(.acquire)) return .{ .subscribed = true };
                const events = try a.dupe(types.Event, &.{.{ .sequence = 1, .ordinal = 0, .sender_pid = 42, .channel = "events", .payload = self.payload }});
                return .{ .events = events, .subscribed = true, .cursor = .{ .sequence = 2 } };
            },
            .acknowledge => |cursor| {
                try std.testing.expectEqual(@as(u64, 2), cursor.sequence);
                self.pending.store(false, .release);
                _ = self.acknowledgements.fetchAdd(1, .acq_rel);
                self.acknowledged.set(request.io);
                return .{};
            },
        }
    }
};
fn readUntilReady(reader: *std.Io.Reader) !void {
    while (true) {
        const tag = try reader.takeByte();
        const count = try reader.takeInt(u32, .big);
        if (count < 4 or count > 16384) return error.ProtocolViolation;
        try reader.discardAll(count - 4);
        if (tag == 'E') return error.UnexpectedWireError;
        if (tag == 'Z') return;
    }
}
fn readNotification(reader: *std.Io.Reader) !void {
    try std.testing.expectEqual(@as(u8, 'A'), try reader.takeByte());
    const count = try reader.takeInt(u32, .big);
    const body = try reader.readAlloc(std.testing.allocator, count - 4);
    defer std.testing.allocator.free(body);
    var cursor: protocol.Cursor = .{ .bytes = body };
    try std.testing.expectEqual(@as(i32, 42), try cursor.int(i32));
    try std.testing.expectEqualStrings("events", try cursor.string());
    try std.testing.expectEqualStrings("idle-event", try cursor.string());
    try std.testing.expectEqual(body.len, cursor.offset);
}
fn timeout(io: std.Io) std.Io.Cancelable!void {
    try io.sleep(.fromSeconds(3), .awake);
}

test "pgwire notifications arrive on an idle socket without another SQL command and acknowledge only flushed frames" {
    var mock: Mock = .{};
    var listener = try @import("server.zig").start(std.testing.allocator, .{ .io = std.testing.io, .backend = mock.source(), .bind_port = 0, .max_connections = 1 });
    defer listener.deinit();
    const stream = try listener.address().connect(std.testing.io, .{ .mode = .stream });
    defer stream.close(std.testing.io);
    var buffer: [1024]u8 = undefined;
    var reader = stream.reader(std.testing.io, &buffer);
    var writer = stream.writer(std.testing.io, &.{});
    try frames.startup(std.testing.allocator, &writer.interface, "listener", "password", "db");
    try writer.interface.flush();
    try readUntilReady(&reader.interface);
    try frames.frame(&writer.interface, 'Q', "LISTEN events\x00");
    try writer.interface.flush();
    try readUntilReady(&reader.interface);
    mock.pending.store(true, .release);
    const Outcome = union(enum) { delivery: anyerror!void, timeout: std.Io.Cancelable!void };
    var outcomes: [2]Outcome = undefined;
    var select = std.Io.Select(Outcome).init(std.testing.io, &outcomes);
    defer select.cancelDiscard();
    try select.concurrent(.delivery, readNotification, .{&reader.interface});
    try select.concurrent(.timeout, timeout, .{std.testing.io});
    switch (try select.await()) {
        .delivery => |result| try result,
        .timeout => return error.IdleNotificationNotDelivered,
    }
    try mock.acknowledged.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromSeconds(3), .clock = .awake } });
    try std.testing.expectEqual(@as(u32, 1), mock.acknowledgements.load(.acquire));
    try frames.frame(&writer.interface, 'X', "");
    try writer.interface.flush();
}

test "pgwire notifications retain an unacknowledged prefix when response flush fails" {
    const a = std.testing.allocator;
    const payload = try a.alloc(u8, 7999);
    defer a.free(payload);
    @memset(payload, 'x');
    var mock: Mock = .{ .on_execute = true, .payload = payload };
    var input: std.Io.Writer.Allocating = .init(a);
    defer input.deinit();
    try frames.startup(a, &input.writer, "listener", "password", "db");
    try frames.frame(&input.writer, 'Q', "LISTEN events\x00");
    try frames.frame(&input.writer, 'X', "");
    var reader = std.Io.Reader.fixed(input.written());
    var storage: [4096]u8 = undefined;
    var writer = std.Io.Writer.fixed(&storage);
    var session: protocol.Session = .{ .alloc = a, .io = std.testing.io, .source = mock.source(), .reader = &reader, .writer = &writer };
    defer session.deinit();
    try std.testing.expectError(error.WriteFailed, session.run());
    try std.testing.expectEqual(@as(u32, 0), mock.acknowledgements.load(.acquire));
    try std.testing.expect(mock.pending.load(.acquire));
}
