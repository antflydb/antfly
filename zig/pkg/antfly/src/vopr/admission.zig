// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Production resource admission composed with VoprIo scheduling. The
//! resource manager remains independent of VOPR; this adapter makes contention,
//! denial, release, and recovery part of a replayable schedule.

const std = @import("std");
const vopr = @import("vopr");
const resource_manager = @import("../storage/resource_manager.zig");

test "resource admission VOPR denies contention and recovers after release" {
    var options: resource_manager.Options = .{
        .identity_allocator = std.testing.allocator,
        .memory_budget = .{ .soft_limit_bytes = 3, .hard_limit_bytes = 4 },
    };
    options.budgets[@intFromEnum(resource_manager.Slice.derived_backlog)] =
        .{ .soft_limit_bytes = 3, .hard_limit_bytes = 4 };
    var manager = resource_manager.ResourceManager.init(options);
    defer manager.deinit(std.testing.allocator);

    var vopr_io = try vopr.vopr_io.VoprIo.init(.{
        .required = .of(&.{ .task_scheduling, .sleep, .clock_read }),
        .tasks = .{ .max_tasks = 2 },
        .instrumentation = .{ .enabled = false, .map_digest = 0x41444d49 },
    });
    defer vopr_io.deinit();
    const io = vopr_io.io();

    const Shared = struct {
        io: std.Io,
        manager: *resource_manager.ResourceManager,
        holder_parked: bool = false,
        holder_released: bool = false,
        contender_denied: bool = false,

        fn holder(self: *@This()) void {
            var reservation = self.manager.reserve(.derived_backlog, 4) catch unreachable;
            self.holder_parked = true;
            std.Io.sleep(self.io, .fromNanoseconds(10), .awake) catch unreachable;
            reservation.release();
            // Releasing twice is deliberately harmless: shutdown/error paths
            // can converge on the same cleanup edge.
            reservation.release();
            self.holder_released = true;
        }

        fn contender(self: *@This()) void {
            var reservation = self.manager.reserve(.derived_backlog, 1) catch |err| {
                std.debug.assert(err == error.ResourceBudgetExceeded);
                self.contender_denied = true;
                return;
            };
            reservation.release();
        }
    };

    var shared = Shared{ .io = io, .manager = &manager };
    _ = io.async(Shared.holder, .{&shared});

    const scheduler = vopr_io.scheduler();
    var enabled: vopr.transition.List = .{};
    defer enabled.deinit(std.testing.allocator);
    var events: vopr.event.Sink = .{};
    defer events.deinit(std.testing.allocator);

    // Start the holder alone so its reservation is live when the competing
    // admission becomes runnable.
    try scheduler.enumerateReady(&enabled, std.testing.allocator);
    try enabled.canonicalize();
    try std.testing.expectEqual(@as(usize, 1), enabled.items.items.len);
    try scheduler.executeReady(enabled.items.items[0].id, &events, std.testing.allocator);
    try std.testing.expect(shared.holder_parked);
    try std.testing.expectEqual(@as(u64, 4), manager.sliceStats(.derived_backlog).used_bytes);

    _ = io.async(Shared.contender, .{&shared});
    enabled.items.clearRetainingCapacity();
    try scheduler.enumerateReady(&enabled, std.testing.allocator);
    try enabled.canonicalize();
    const contender_transition = for (enabled.items.items) |candidate| {
        if (!std.mem.eql(u8, candidate.name, "sim-io.time_advance")) break candidate;
    } else return error.MissingContenderTransition;
    try scheduler.executeReady(contender_transition.id, &events, std.testing.allocator);
    try std.testing.expect(shared.contender_denied);
    try std.testing.expect(!shared.holder_released);
    try std.testing.expectEqual(@as(u64, 1), manager.sliceStats(.derived_backlog).hard_limit_rejections);

    while (!scheduler.quiescent()) {
        enabled.items.clearRetainingCapacity();
        try scheduler.enumerateReady(&enabled, std.testing.allocator);
        try enabled.canonicalize();
        try std.testing.expect(enabled.items.items.len != 0);
        try scheduler.executeReady(enabled.items.items[0].id, &events, std.testing.allocator);
    }
    try std.testing.expect(shared.holder_released);

    const after_release = manager.sliceStats(.derived_backlog);
    try std.testing.expectEqual(@as(u64, 0), after_release.used_bytes);
    try std.testing.expectEqual(@as(u64, 4), after_release.peak_bytes);
    try std.testing.expectEqual(@as(u64, 1), after_release.soft_limit_events);

    var recovered = try manager.reserve(.derived_backlog, 4);
    recovered.release();
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.derived_backlog).used_bytes);
    try vopr_io.ensureNoCapabilityViolation();
}
