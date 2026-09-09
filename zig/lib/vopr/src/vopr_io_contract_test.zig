// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

const std = @import("std");
const vopr = struct {
    const vopr_io = @import("vopr_io.zig");
    const transition = @import("transition.zig");
    const event = @import("event.zig");
};

test "VoprIo contract queued group cancellation preserves callback cleanup" {
    const Worker = struct {
        fn run(cleaned: *bool) void {
            defer cleaned.* = true;
        }
    };
    var sim = try vopr.vopr_io.VoprIo.init(.{});
    defer sim.deinit();
    var cleaned = false;
    var group: std.Io.Group = .init;
    group.async(sim.io(), Worker.run, .{&cleaned});
    group.cancel(sim.io());
    try std.testing.expect(cleaned);
}

test "VoprIo contract group context honors advertised alignment or rejects submission" {
    const Context = struct {
        observed: *usize,
        padding: u8 align(64) = 0,
        fn run(raw: *const anyopaque) void {
            // Read the pointer with alignment one so the diagnostic itself
            // does not trap before reporting the implementation's address.
            const context: *align(1) const @This() = @ptrCast(raw);
            context.observed.* = @intFromPtr(raw) % @alignOf(@This());
        }
    };
    var sim = try vopr.vopr_io.VoprIo.init(.{});
    defer sim.deinit();
    var observed: usize = 999;
    var context = Context{ .observed = &observed };
    var group: std.Io.Group = .init;
    const io = sim.io();
    io.vtable.groupConcurrent(io.userdata, &group, std.mem.asBytes(&context), .of(Context), Context.run) catch |err| switch (err) {
        error.ConcurrencyUnavailable => return,
    };
    var enabled: vopr.transition.List = .{};
    defer enabled.deinit(std.testing.allocator);
    var sink: vopr.event.Sink = .{};
    defer sink.deinit(std.testing.allocator);
    try sim.scheduler().enumerateReady(&enabled, std.testing.allocator);
    try sim.scheduler().executeReady(enabled.items.items[0].id, &sink, std.testing.allocator);
    group.cancel(io);
    try std.testing.expectEqual(@as(usize, 0), observed);
}

test "VoprIo contract cancellation cannot finish a protected sleep before its deadline" {
    const Worker = struct {
        fn run(io: std.Io, woke: *i96, canceled: *bool) void {
            {
                const old = io.swapCancelProtection(.blocked);
                defer _ = io.swapCancelProtection(old);
                io.sleep(.fromNanoseconds(100), .awake) catch unreachable;
            }
            woke.* = std.Io.Timestamp.now(io, .awake).nanoseconds;
            io.checkCancel() catch {
                canceled.* = true;
            };
        }
    };
    var sim = try vopr.vopr_io.VoprIo.init(.{});
    defer sim.deinit();
    var woke: i96 = -1;
    var canceled = false;
    var future = sim.io().async(Worker.run, .{ sim.io(), &woke, &canceled });
    var enabled: vopr.transition.List = .{};
    defer enabled.deinit(std.testing.allocator);
    var sink: vopr.event.Sink = .{};
    defer sink.deinit(std.testing.allocator);
    try sim.scheduler().enumerateReady(&enabled, std.testing.allocator);
    try sim.scheduler().executeReady(enabled.items.items[0].id, &sink, std.testing.allocator);
    _ = try sim.cancelAndDrainTasksForTeardown(std.testing.allocator, 100);
    future.await(sim.io());
    try std.testing.expect(woke >= 100);
    try std.testing.expect(canceled);
}

test "VoprIo contract group argument padding and eager fallback preserve alignment" {
    inline for (.{ 16, 64 }) |alignment| {
        const Context = struct {
            observed: *usize,
            padding: u8 align(alignment) = 0,
            fn run(raw: *const anyopaque) void {
                const context: *const @This() = @ptrCast(@alignCast(raw));
                context.observed.* = @intFromPtr(raw) % @alignOf(@This());
            }
        };
        var sim = try vopr.vopr_io.VoprIo.init(.{});
        defer sim.deinit();
        var observed: usize = 999;
        var context = Context{ .observed = &observed };
        var group: std.Io.Group = .init;
        const io = sim.io();
        io.vtable.groupAsync(io.userdata, &group, std.mem.asBytes(&context), .of(Context), Context.run);
        group.cancel(io);
        try std.testing.expectEqual(@as(usize, 0), observed);
        try std.testing.expectEqual(@as(usize, 0), sim.tasks.totalTaskCount());
    }
}

test "VoprIo contract cancellation cannot wake an uncancelable futex" {
    const Worker = struct {
        fn run(io: std.Io, word: *u32, finished: *bool) void {
            io.vtable.futexWaitUncancelable(io.userdata, word, 0);
            finished.* = true;
        }
    };
    var sim = try vopr.vopr_io.VoprIo.init(.{});
    defer sim.deinit();
    var word: u32 = 0;
    var finished = false;
    var future = sim.io().async(Worker.run, .{ sim.io(), &word, &finished });
    var enabled: vopr.transition.List = .{};
    defer enabled.deinit(std.testing.allocator);
    var sink: vopr.event.Sink = .{};
    defer sink.deinit(std.testing.allocator);
    try sim.scheduler().enumerateReady(&enabled, std.testing.allocator);
    try sim.scheduler().executeReady(enabled.items.items[0].id, &sink, std.testing.allocator);
    sim.tasks.requestCancelAll();
    enabled.items.clearRetainingCapacity();
    try sim.scheduler().enumerateReady(&enabled, std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 0), enabled.items.items.len);
    word = 1;
    try sim.tasks.futexWake(&word, 1);
    _ = try sim.cancelAndDrainTasksForTeardown(std.testing.allocator, 100);
    future.await(sim.io());
    try std.testing.expect(finished);
}
