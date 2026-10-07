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

const std = @import("std");
const builtin = @import("builtin");
const platform = @import("antfly_platform");

fn readClock(io: std.Io) i96 {
    return std.Io.Clock.awake.now(io).nanoseconds;
}

test "platform Io context interoperates with standard APIs and owned tasks" {
    try std.testing.expect(platform.Io.Context == std.Io);
    const dependency = @import("platform_dependency");
    try std.testing.expect(dependency.Io.Context == platform.Io.Context);
    try std.testing.expect(dependency.Io.Threaded == platform.Io.Threaded);
    if (builtin.os.tag == .windows) {
        try std.testing.expect(platform.Io.Threaded != std.Io.Threaded);
    } else {
        try std.testing.expect(platform.Io.Threaded == std.Io.Threaded);
    }
    var executor = platform.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(2) });
    defer executor.deinit();
    const io: platform.Io.Context = executor.io();
    const before = readClock(io);
    var task = try io.concurrent(readClock, .{io});
    try std.testing.expect(task.await(io) >= before);
}

test "platform Io keeps the qualified Evented backend" {
    if (!std.Io.fiber.supported) {
        try std.testing.expect(platform.Io.Evented == void);
    } else switch (builtin.os.tag) {
        .linux, .macos => try std.testing.expect(platform.Io.Evented != std.Io.Evented),
        else => try std.testing.expect(platform.Io.Evented == std.Io.Evented),
    }
}

// Hostname and datagram paths are deliberately separate from numeric-IP TCP.
test "platform Io resolves localhost without private PEB data" {
    var executor = platform.Io.Threaded.init(std.testing.allocator, .{});
    defer executor.deinit();
    const io = executor.io();
    const host = try std.Io.net.HostName.init("localhost");
    var buffer: [16]std.Io.net.HostName.LookupResult = undefined;
    var queue: std.Io.Queue(std.Io.net.HostName.LookupResult) = .init(&buffer);
    try host.lookup(io, &queue, .{ .port = 80 });
    var count: usize = 0;
    while (queue.getOneUncancelable(io)) |result| {
        switch (result) {
            .address => |address| {
                try std.testing.expectEqual(@as(u16, 80), address.getPort());
                count += 1;
            },
            .canonical_name => {},
        }
    } else |err| try std.testing.expectEqual(error.Closed, err);
    try std.testing.expect(count != 0);
}

fn receiveForCancellation(io: std.Io, socket: std.Io.net.Socket, started: *std.atomic.Value(bool)) std.Io.net.Socket.ReceiveError!void {
    var buffer: [64]u8 = undefined;
    started.store(true, .release);
    _ = try socket.receive(io, &buffer);
}

test "platform Io datagrams preserve payload and source across canceled receives" {
    var executor = platform.Io.Threaded.init(std.testing.allocator, .{});
    defer executor.deinit();
    const io = executor.io();
    const address: std.Io.net.IpAddress = .{ .ip4 = .loopback(0) };
    const sender = try address.bind(io, .{ .mode = .dgram });
    defer sender.close(io);
    const receiver = try address.bind(io, .{ .mode = .dgram });
    defer receiver.close(io);
    var buffer: [64]u8 = undefined;
    for (0..8) |_| {
        var started: std.atomic.Value(bool) = .init(false);
        var pending = try io.concurrent(receiveForCancellation, .{ io, receiver, &started });
        defer _ = pending.cancel(io) catch {};
        while (!started.load(.acquire)) try io.sleep(.fromMilliseconds(1), .awake);
        try io.sleep(.fromMilliseconds(10), .awake);
        try std.testing.expectError(error.Canceled, pending.cancel(io));
        // Canceling must drain only its request and leave the socket usable.
        for ([_][]const u8{ "hello", "" }) |payload| {
            try sender.send(io, &receiver.address, payload);
            const message = try receiver.receive(io, &buffer);
            try std.testing.expectEqualStrings(payload, message.data);
            try std.testing.expectEqual(sender.address.getPort(), message.from.getPort());
        }
    }
    var oversized: [65536]u8 = @splat(0);
    try std.testing.expectError(error.MessageOversize, sender.send(io, &receiver.address, &oversized));
    if (builtin.os.tag == .windows) {
        try sender.send(io, &receiver.address, "too large");
        try std.testing.expectError(error.MessageOversize, receiver.receive(io, buffer[0..1]));
    }
    try sender.send(io, &receiver.address, "after truncation");
    const message = try receiver.receive(io, &buffer);
    try std.testing.expectEqualStrings("after truncation", message.data);
}

const BlockedDnsProvider = struct {
    var started: std.atomic.Value(bool) = .init(false);
    var allow_completion: std.atomic.Value(bool) = .init(false);
    fn query() i32 {
        started.store(true, .release);
        while (!allow_completion.load(.acquire)) platform.time.sleepNs(std.time.ns_per_ms);
        return 11001; // WSAHOST_NOT_FOUND
    }
};

fn lookupForCancellation(io: std.Io) !void {
    var buffer: [16]std.Io.net.HostName.LookupResult = undefined;
    var queue: std.Io.Queue(std.Io.net.HostName.LookupResult) = .init(&buffer);
    try (try std.Io.net.HostName.init("request.invalid")).lookup(io, &queue, .{ .port = 80 });
}

test "platform Io Wine DNS cancellation owns provider storage and bounds detached work" {
    if (builtin.os.tag != .windows) return error.SkipZigTest;
    const dns = platform.c.dns_testing;
    if (!dns.enabled()) return error.SkipZigTest;
    try std.testing.expectEqual(@as(usize, 0), dns.pending());
    BlockedDnsProvider.started.store(false, .release);
    BlockedDnsProvider.allow_completion.store(false, .release);
    dns.provider = BlockedDnsProvider.query;
    defer {
        BlockedDnsProvider.allow_completion.store(true, .release);
        while (dns.pending() != 0) platform.time.sleepNs(std.time.ns_per_ms);
        dns.provider = null;
    }
    {
        var executor = platform.Io.Threaded.init(std.testing.allocator, .{});
        defer executor.deinit();
        const io = executor.io();
        var task = try io.concurrent(lookupForCancellation, .{io});
        defer _ = task.cancel(io) catch {};
        while (!BlockedDnsProvider.started.load(.acquire)) try io.sleep(.fromMilliseconds(1), .awake);
        try std.testing.expectError(error.Canceled, task.cancel(io));
    } // The executor has gone away while the native provider still owns its request.
    try std.testing.expectEqual(@as(usize, 1), dns.pending());
    for (0..31) |_| try std.testing.expectError(error.Canceled, dns.cancelAtSubmission());
    try std.testing.expectEqual(@as(usize, 32), dns.pending());
    try std.testing.expectError(error.SystemResources, dns.cancelAtSubmission());
    BlockedDnsProvider.allow_completion.store(true, .release);
    while (dns.pending() != 0) platform.time.sleepNs(std.time.ns_per_ms);
    try std.testing.expectEqual(@as(usize, 0), dns.pending());
}
