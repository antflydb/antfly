// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: ELv2

test "evented enrichment executor initializes when supported" {
    try @import("enrichment_executor.zig").testEventedExecutor(true);
}

const std = @import("std");
const builtin = @import("builtin");
const Evented = @import("antfly_platform").Evented;

test "Dispatch stderr mutex hands ownership past canceled waiters" {
    if (builtin.os.tag != .macos or !std.Io.fiber.supported) return error.SkipZigTest;
    var ev: Evented = undefined;
    try ev.init(std.testing.allocator, .{});
    defer ev.deinit();
    const io = ev.io();
    const Work = struct {
        fn acquire(task_io: std.Io, ready: *std.Io.Event) std.Io.Cancelable!void {
            var buffer: [128]u8 = undefined;
            ready.set(task_io);
            _ = try task_io.lockStderr(&buffer, null);
            defer task_io.unlockStderr();
        }
    };
    for ([_]u3{ 0, 1, 2, 7 }) |canceled_mask| {
        var buffer: [128]u8 = undefined;
        _ = try io.lockStderr(&buffer, null);
        var held = true;
        defer if (held) io.unlockStderr();
        var ready: [3]std.Io.Event = @splat(.unset);
        var futures: [3]std.Io.Future(std.Io.Cancelable!void) = undefined;
        var launched: usize = 0;
        defer for (futures[0..launched]) |*future| {
            future.cancel(io) catch {};
        };
        for (&futures, &ready) |*future, *event| {
            future.* = try io.concurrent(Work.acquire, .{ io, event });
            launched += 1;
            event.waitUncancelable(io);
        }
        // Give each task an opportunity to register its mutex waiter.
        try io.sleep(.fromMilliseconds(20), .awake);
        for (&futures, 0..) |*future, index| {
            if (canceled_mask & (@as(u3, 1) << @intCast(index)) != 0)
                try std.testing.expectError(error.Canceled, future.cancel(io));
        }
        io.unlockStderr();
        held = false;
        for (&futures, 0..) |*future, index| {
            if (canceled_mask & (@as(u3, 1) << @intCast(index)) == 0)
                try future.await(io);
        }
    }
}

test "Dispatch teardown drains group child cleanup after join" {
    if (builtin.os.tag != .macos or !std.Io.fiber.supported) return error.SkipZigTest;
    const Allocator = struct {
        freeing: std.atomic.Value(bool) = .init(false),
        outstanding: std.atomic.Value(usize) = .init(0),
        fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            const memory = std.heap.c_allocator.rawAlloc(len, alignment, ra) orelse return null;
            _ = self.outstanding.fetchAdd(1, .monotonic);
            return memory;
        }
        fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ra: usize) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            // Delay fiber frees to expose the completion/teardown race without
            // touching the large stack reservation or relying on scheduling luck.
            if (memory.len > 60 * 1024 * 1024) {
                self.freeing.store(true, .release);
                _ = std.c.nanosleep(&.{ .sec = 0, .nsec = 50 * std.time.ns_per_ms }, null);
            }
            std.heap.c_allocator.rawFree(memory, alignment, ra);
            _ = self.outstanding.fetchSub(1, .release);
        }
        fn allocator(self: *@This()) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{
                .alloc = alloc,
                .resize = std.mem.Allocator.noResize,
                .remap = std.mem.Allocator.noRemap,
                .free = free,
            } };
        }
    };
    const Work = struct {
        fn run() void {}
    };
    for ([_]bool{ false, true }) |join_after_completion| {
        var allocator: Allocator = .{};
        var ev: Evented = undefined;
        try ev.init(allocator.allocator(), .{});
        const io = ev.io();
        var group: std.Io.Group = .init;
        group.async(io, Work.run, .{});
        if (join_after_completion) {
            // The child has emptied the token and is still freeing its stack.
            while (!allocator.freeing.load(.acquire)) std.atomic.spinLoopHint();
        }
        try group.await(io);
        ev.deinit();
        try std.testing.expectEqual(@as(usize, 0), allocator.outstanding.load(.acquire));
    }
}
