// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
const std = @import("std");
const response = @import("core/response.zig");

test "HTTP response module contracts" {
    std.testing.refAllDecls(response);
}

test "response operation retirement follows final teardown and failed construction" {
    const Owner = struct {
        releases: usize = 0,
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.releases += 1;
        }
    };
    var owner: Owner = .{};
    var builder = response.ResponseBuilder.init(std.testing.allocator);
    defer builder.deinit();
    _ = builder.ownedBody(try std.testing.allocator.dupe(u8, "owned"), std.testing.allocator);
    builder.retirement = .{ .ptr = &owner, .release = Owner.release };
    var reply = try builder.build();
    reply.releaseBody();
    try std.testing.expectEqual(@as(usize, 0), owner.releases);
    reply.deinit();
    try std.testing.expectEqual(@as(usize, 1), owner.releases);

    try builder.reserveHeaderMemory(1);
    _ = builder.ownedBody(try std.testing.allocator.dupe(u8, "owned"), std.testing.allocator);
    builder.retirement = .{ .ptr = &owner, .release = Owner.release };
    try std.testing.expectError(error.OutOfMemory, builder.build());
    try std.testing.expectEqual(@as(usize, 2), owner.releases);

    _ = builder.ownedBody(try std.testing.allocator.dupe(u8, "replaced"), std.testing.allocator);
    builder.retirement = .{ .ptr = &owner, .release = Owner.release };
    _ = builder.body("borrowed replacement");
    try std.testing.expectEqual(@as(usize, 3), owner.releases);
}

test "reserved response headers survive backing exhaustion and retire with the body" {
    var storage: [8192]u8 = undefined;
    var backing = std.heap.FixedBufferAllocator.init(&storage);
    var builder = response.ResponseBuilder.init(backing.allocator());
    defer builder.deinit();
    _ = try builder.header("X-Existing", "preserved");
    const body = try std.testing.allocator.dupe(u8, "accepted");
    _ = builder.ownedBody(body, std.testing.allocator);
    try builder.reserveHeaderMemory(4096);
    backing.end_index = storage.len;
    _ = try builder.header("Content-Type", "application/json");
    var reply = try builder.build();
    defer reply.deinit();
    try reply.headers.set("Date", "Tue, 13 Oct 2026 00:00:00 GMT");
    try std.testing.expectEqualStrings("preserved", reply.header("X-Existing").?);
    try std.testing.expectEqual(@as(u64, body.len), reply.contentLength().?);
    try std.testing.expectEqual(body.ptr, reply.body.?.ptr);
    try std.testing.expect(builder.header_arena == null);
    try std.testing.expect(reply.header_arena != null);
}

test "response header reservation allocation failures preserve the original builder" {
    const Fixture = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var builder = response.ResponseBuilder.init(alloc);
            defer builder.deinit();
            _ = try builder.header("X-Existing", "preserved");
            builder.reserveHeaderMemory(4096) catch |err| {
                try std.testing.expectEqualStrings("preserved", builder.headers.get("X-Existing").?);
                return err;
            };
            var reply = try builder.build();
            defer reply.deinit();
            try std.testing.expectEqualStrings("preserved", reply.header("X-Existing").?);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "exhausted reserved headers release a transferred body and permit builder reuse" {
    var builder = response.ResponseBuilder.init(std.testing.allocator);
    defer builder.deinit();
    try builder.reserveHeaderMemory(1);
    _ = builder.ownedBody(try std.testing.allocator.dupe(u8, "accepted"), std.testing.allocator);
    try std.testing.expectError(error.OutOfMemory, builder.build());
    try std.testing.expect(builder.header_arena == null);
    _ = builder.body("reused");
    var reply = try builder.build();
    defer reply.deinit();
    try std.testing.expectEqualStrings("reused", reply.body.?);
}
