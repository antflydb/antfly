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

test "Dispatch completed groups stop accessing caller-owned storage" {
    if (builtin.os.tag != .macos or !std.Io.fiber.supported) return error.SkipZigTest;
    const Work = struct {
        fn run() void {}
    };
    // Debug poisons each 60 MiB fiber allocation; keep that run bounded.
    const iterations: usize = if (builtin.mode == .debug) 128 else 20_000;
    const groups = try std.heap.c_allocator.alloc(std.Io.Group, iterations);
    defer std.heap.c_allocator.free(groups);
    const marker = std.math.maxInt(usize);
    {
        var ev: Evented = undefined;
        try ev.init(std.heap.c_allocator, .{ .backing_allocator_needs_mutex = false });
        defer ev.deinit();
        const io = ev.io();
        for (groups) |*group| {
            group.* = .init;
            group.async(io, Work.run, .{});
            while (group.token.load(.acquire) != null) std.atomic.spinLoopHint();
            try group.await(io);
            // Model reuse immediately after the empty-token fast path returns.
            // Retain the allocation until cleanup drains so a late write is
            // reported as a failed assertion rather than corrupting freed memory.
            @atomicStore(usize, &group.state, marker, .seq_cst);
        }
    }
    for (groups) |*group| {
        try std.testing.expectEqual(marker, @atomicLoad(usize, &group.state, .seq_cst));
    }
}

test "Evented groups can be reused after await and cancellation" {
    if ((builtin.os.tag != .macos and builtin.os.tag != .linux) or !std.Io.fiber.supported)
        return error.SkipZigTest;
    var ev: Evented = undefined;
    try ev.init(std.testing.allocator, .{});
    defer ev.deinit();
    const io = ev.io();
    const Work = struct {
        fn complete(task_io: std.Io) std.Io.Cancelable!void {
            try task_io.sleep(.fromMilliseconds(10), .awake);
        }
        fn wait(task_io: std.Io) std.Io.Cancelable!void {
            try task_io.sleep(.fromSeconds(3600), .awake);
        }
        fn canceledAwait(task_io: std.Io, group: *std.Io.Group, ready: *std.Io.Event) std.Io.Cancelable!void {
            group.async(task_io, wait, .{task_io});
            ready.set(task_io);
            try group.await(task_io);
        }
    };
    var group: std.Io.Group = .init;
    defer group.cancel(io);
    for (0..4) |_| {
        group.async(io, Work.complete, .{io});
        try group.await(io);
        // A completed group remains idempotent and can accept another batch.
        try group.await(io);
        group.async(io, Work.wait, .{io});
        group.cancel(io);
        group.cancel(io);
    }
    // An await which returns error.Canceled must reset the group too.
    var ready: std.Io.Event = .unset;
    var parent = try io.concurrent(Work.canceledAwait, .{ io, &group, &ready });
    defer parent.cancel(io) catch {};
    ready.waitUncancelable(io);
    try io.sleep(.fromMilliseconds(1), .awake);
    try std.testing.expectError(error.Canceled, parent.cancel(io));
    group.async(io, Work.complete, .{io});
    try group.await(io);
}

test "Evented parent cancellation preserves an in-progress group cancel join" {
    if ((builtin.os.tag != .macos and builtin.os.tag != .linux) or !std.Io.fiber.supported)
        return error.SkipZigTest;
    var ev: Evented = undefined;
    try ev.init(std.testing.allocator, .{});
    defer ev.deinit();
    const io = ev.io();
    const Work = struct {
        fn child(task_io: std.Io, ready: *std.Io.Event, finished: *std.atomic.Value(bool)) void {
            const old = task_io.swapCancelProtection(.blocked);
            defer _ = task_io.swapCancelProtection(old);
            ready.set(task_io);
            task_io.sleep(.fromMilliseconds(200), .awake) catch unreachable;
            finished.store(true, .release);
        }
        fn parent(task_io: std.Io, ready: *std.Io.Event, finished: *std.atomic.Value(bool)) std.Io.Cancelable!void {
            var child_ready: std.Io.Event = .unset;
            var group: std.Io.Group = .init;
            group.async(task_io, child, .{ task_io, &child_ready, finished });
            child_ready.waitUncancelable(task_io);
            ready.set(task_io);
            // This join must finish even if the parent receives cancellation.
            group.cancel(task_io);
            // The parent cancellation must remain pending after the join.
            try task_io.checkCancel();
        }
    };
    for (0..2) |_| {
        var ready: std.Io.Event = .unset;
        var finished: std.atomic.Value(bool) = .init(false);
        var parent = try io.concurrent(Work.parent, .{ io, &ready, &finished });
        defer parent.cancel(io) catch {};
        ready.waitUncancelable(io);
        try io.sleep(.fromMilliseconds(10), .awake);
        try std.testing.expectError(error.Canceled, parent.cancel(io));
        try std.testing.expect(finished.load(.acquire));
    }
}

test "Evented protected group await finishes children and retains cancellation" {
    if ((builtin.os.tag != .macos and builtin.os.tag != .linux) or !std.Io.fiber.supported)
        return error.SkipZigTest;
    var ev: Evented = undefined;
    try ev.init(std.testing.allocator, .{});
    defer ev.deinit();
    const io = ev.io();
    const Work = struct {
        fn child(task_io: std.Io, completed: *std.atomic.Value(bool)) void {
            task_io.sleep(.fromMilliseconds(200), .awake) catch return;
            completed.store(true, .release);
        }
        fn parent(task_io: std.Io, ready: *std.Io.Event, completed: *std.atomic.Value(bool), before_await: bool) std.Io.Cancelable!void {
            const old = task_io.swapCancelProtection(.blocked);
            var group: std.Io.Group = .init;
            group.async(task_io, child, .{ task_io, completed });
            ready.set(task_io);
            // Exercise cancellation both before and after registering the join.
            if (before_await) task_io.sleep(.fromMilliseconds(30), .awake) catch unreachable;
            group.await(task_io) catch unreachable;
            _ = task_io.swapCancelProtection(old);
            // Blocking notification must preserve the request for later.
            try task_io.checkCancel();
        }
    };
    for ([_]bool{ false, true }) |before_await| {
        var ready: std.Io.Event = .unset;
        var completed: std.atomic.Value(bool) = .init(false);
        var parent = try io.concurrent(Work.parent, .{ io, &ready, &completed, before_await });
        defer parent.cancel(io) catch {};
        ready.waitUncancelable(io);
        try io.sleep(.fromMilliseconds(10), .awake);
        try std.testing.expectError(error.Canceled, parent.cancel(io));
        try std.testing.expect(completed.load(.acquire));
    }
}
