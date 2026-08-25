// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Production resource admission composed with VoprIo scheduling. The
//! resource manager remains independent of VOPR; this adapter makes contention,
//! denial, release, and recovery part of a replayable schedule.

const std = @import("std");
const vopr = @import("vopr");
const resource_manager = @import("../storage/resource_manager.zig");
const request_admission = @import("../common/request_admission.zig");

fn drive(
    sim: *vopr.vopr_io.VoprIo,
    alloc: std.mem.Allocator,
    prefer_work: bool,
    transition_budget: usize,
) !void {
    var enabled: vopr.transition.List = .{};
    defer enabled.deinit(alloc);
    var events: vopr.event.Sink = .{};
    defer events.deinit(alloc);
    const scheduler = sim.scheduler();
    var transitions: usize = 0;
    while (!scheduler.quiescent()) {
        enabled.items.clearRetainingCapacity();
        try scheduler.enumerateReady(&enabled, alloc);
        try enabled.canonicalize();
        if (enabled.items.items.len == 0) return error.ResourceAdmissionVoprDeadlock;
        var selected = enabled.items.items[0];
        if (prefer_work) {
            for (enabled.items.items) |candidate| {
                if (!std.mem.eql(u8, candidate.name, "sim-io.time_advance")) {
                    selected = candidate;
                    break;
                }
            }
        }
        try scheduler.executeReady(selected.id, &events, alloc);
        transitions += 1;
        if (transitions > transition_budget) return error.ResourceAdmissionVoprTransitionBudgetExceeded;
    }
}

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

test "resource admission VOPR composes request and multi-slice cancellation edges" {
    const alloc = std.testing.allocator;
    var options: resource_manager.Options = .{
        .identity_allocator = alloc,
        .memory_budget = .{ .soft_limit_bytes = 12, .hard_limit_bytes = 16 },
    };
    options.budgets[@intFromEnum(resource_manager.Slice.dense_search_working_set)] =
        .{ .soft_limit_bytes = 6, .hard_limit_bytes = 8 };
    options.budgets[@intFromEnum(resource_manager.Slice.derived_replay_window)] =
        .{ .soft_limit_bytes = 6, .hard_limit_bytes = 8 };
    options.budgets[@intFromEnum(resource_manager.Slice.inference_scratch_working_set)] =
        .{ .soft_limit_bytes = 2, .hard_limit_bytes = 4 };
    var manager = resource_manager.ResourceManager.init(options);
    defer manager.deinit(alloc);
    var foreground = request_admission.RequestAdmission.init(4);

    var sim = try vopr.vopr_io.VoprIo.init(.{
        .required = .of(&.{ .clock_read, .task_scheduling, .sleep }),
        .tasks = .{ .max_tasks = 4 },
    });
    defer sim.deinit();
    const io = sim.io();

    const CancelEdge = enum { request, batch, scratch, complete };
    const Shared = struct {
        io: std.Io,
        manager: *resource_manager.ResourceManager,
        foreground: *request_admission.RequestAdmission,
        completed: usize = 0,
        canceled: usize = 0,
        failure: ?anyerror = null,

        fn request(self: *@This(), edge: CancelEdge) void {
            if (!self.foreground.tryAcquire()) {
                self.failure = error.ForegroundAdmissionUnexpectedlyDenied;
                return;
            }
            defer self.foreground.release();
            self.io.sleep(.zero, .awake) catch {};
            if (edge == .request) {
                self.canceled += 1;
                return;
            }

            var batch = self.manager.reserveBatch(&.{
                .{ .slice = .dense_search_working_set, .bytes = 3 },
                .{ .slice = .derived_replay_window, .bytes = 2 },
            }) catch |err| {
                self.failure = err;
                return;
            };
            defer batch.release();
            self.io.sleep(.zero, .awake) catch {};
            if (edge == .batch) {
                self.canceled += 1;
                return;
            }

            var scratch = self.manager.reserve(.inference_scratch_working_set, 1) catch |err| {
                self.failure = err;
                return;
            };
            defer scratch.release();
            self.io.sleep(.zero, .awake) catch {};
            if (edge == .scratch) {
                self.canceled += 1;
                return;
            }
            self.completed += 1;
        }
    };

    var shared = Shared{ .io = io, .manager = &manager, .foreground = &foreground };
    inline for (std.meta.tags(CancelEdge)) |edge| _ = io.async(Shared.request, .{ &shared, edge });
    try drive(&sim, alloc, true, 1_000);

    if (shared.failure) |err| return err;
    try std.testing.expectEqual(@as(usize, 3), shared.canceled);
    try std.testing.expectEqual(@as(usize, 1), shared.completed);
    try std.testing.expectEqual(@as(usize, 0), foreground.stats().in_flight);
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.dense_search_working_set).used_bytes);
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.derived_replay_window).used_bytes);
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.inference_scratch_working_set).used_bytes);
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.accounting_errors);
    try sim.ensureNoCapabilityViolation();
}

test "resource admission VOPR preserves foreground priority and bounded minimum progress" {
    const alloc = std.testing.allocator;
    var options: resource_manager.Options = .{
        .identity_allocator = alloc,
        .memory_budget = .{ .soft_limit_bytes = 20, .hard_limit_bytes = 30 },
    };
    options.budgets[@intFromEnum(resource_manager.Slice.lsm_compaction_work)] =
        .{ .soft_limit_bytes = 2, .hard_limit_bytes = 4 };
    options.policies[@intFromEnum(resource_manager.Slice.lsm_compaction_work)] =
        .{ .soft_action = .defer_background_work, .hard_action = .reject_work };
    options.budgets[@intFromEnum(resource_manager.Slice.text_merge_buffers)] =
        .{ .soft_limit_bytes = 8, .hard_limit_bytes = 10 };
    options.budgets[@intFromEnum(resource_manager.Slice.dense_search_working_set)] =
        .{ .soft_limit_bytes = 4, .hard_limit_bytes = 8 };
    var manager = resource_manager.ResourceManager.init(options);
    defer manager.deinit(alloc);

    var sim = try vopr.vopr_io.VoprIo.init(.{
        .required = .of(&.{ .clock_read, .task_scheduling, .sleep }),
        .tasks = .{ .max_tasks = 4 },
    });
    defer sim.deinit();
    const io = sim.io();

    const Shared = struct {
        io: std.Io,
        manager: *resource_manager.ResourceManager,
        background_parked: bool = false,
        minimum_parked: bool = false,
        background_released: bool = false,
        minimum_released: bool = false,
        foreground_progressed: bool = false,
        contender_denied: bool = false,
        failure: ?anyerror = null,

        fn background(self: *@This()) void {
            var reservation = self.manager.reserve(.lsm_compaction_work, 3) catch |err| {
                self.failure = err;
                return;
            };
            defer reservation.release();
            self.background_parked = true;
            self.io.sleep(.fromNanoseconds(10), .awake) catch {};
            self.background_released = true;
        }

        fn minimum(self: *@This()) void {
            if (self.manager.reserve(.text_merge_buffers, 18)) |unexpected| {
                var reservation = unexpected;
                reservation.release();
                self.failure = error.OversizedWorkUnexpectedlyAdmittedNormally;
                return;
            } else |err| {
                if (err != error.ResourceBudgetExceeded) {
                    self.failure = err;
                    return;
                }
            }
            var reservation = self.manager.reserveBoundedOversizedSingle(.text_merge_buffers, 18, 2) catch |err| {
                self.failure = err;
                return;
            };
            defer reservation.release();
            self.minimum_parked = true;
            self.io.sleep(.fromNanoseconds(10), .awake) catch {};
            self.minimum_released = true;
        }

        fn foreground(self: *@This()) void {
            if (!self.manager.shouldDeferBackgroundWork(.lsm_compaction_work)) {
                self.failure = error.BackgroundPriorityPolicyNotActive;
                return;
            }
            var reservation = self.manager.reserve(.dense_search_working_set, 4) catch |err| {
                self.failure = err;
                return;
            };
            defer reservation.release();
            self.foreground_progressed = true;
        }

        fn minimumContender(self: *@This()) void {
            if (self.manager.reserveBoundedOversizedSingle(.text_merge_buffers, 1, 2)) |unexpected| {
                var reservation = unexpected;
                reservation.release();
                self.failure = error.ConcurrentMinimumProgressUnexpectedlyAdmitted;
                return;
            } else |err| {
                if (err != error.ResourceBudgetExceeded) self.failure = err;
                self.contender_denied = true;
            }
        }
    };

    var shared = Shared{ .io = io, .manager = &manager };
    _ = io.async(Shared.background, .{&shared});
    _ = io.async(Shared.minimum, .{&shared});

    // Drive both holders to their sleeps without advancing virtual time.
    var enabled: vopr.transition.List = .{};
    defer enabled.deinit(alloc);
    var events: vopr.event.Sink = .{};
    defer events.deinit(alloc);
    const scheduler = sim.scheduler();
    while (!shared.background_parked or !shared.minimum_parked) {
        enabled.items.clearRetainingCapacity();
        try scheduler.enumerateReady(&enabled, alloc);
        try enabled.canonicalize();
        const selected = for (enabled.items.items) |candidate| {
            if (!std.mem.eql(u8, candidate.name, "sim-io.time_advance")) break candidate;
        } else return error.ResourceAdmissionVoprHolderDeadlock;
        try scheduler.executeReady(selected.id, &events, alloc);
    }

    _ = io.async(Shared.foreground, .{&shared});
    _ = io.async(Shared.minimumContender, .{&shared});
    try drive(&sim, alloc, true, 1_000);

    if (shared.failure) |err| return err;
    try std.testing.expect(shared.foreground_progressed);
    try std.testing.expect(shared.contender_denied);
    try std.testing.expect(shared.background_released);
    try std.testing.expect(shared.minimum_released);
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_compaction_work).used_bytes);
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.text_merge_buffers).used_bytes);
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.dense_search_working_set).used_bytes);
    try std.testing.expectEqual(@as(u64, 1), manager.sliceStats(.text_merge_buffers).oversized_single_grants);
    try std.testing.expect(manager.sliceStats(.text_merge_buffers).hard_limit_rejections >= 2);
    try sim.ensureNoCapabilityViolation();
}
