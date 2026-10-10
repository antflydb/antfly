// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
const std = @import("std");
const abi = @import("antfly_runtime_abi").runnable_abi;
const fanout = @import("runnable_fanout_owner.zig");

/// A bounded participant window over stable caller-owned result slots.
/// Reserve every child before publishing any task. Each worker activates its
/// own Owner and parks it after destroying its arena. Join all physical tasks
/// before resuming the original coordinator class or retiring any child.
pub const Window = struct {
    team: *abi.Team,
    owners: []fanout.Owner,
    phase: ?abi.Class = null,
    retire_children: bool = true,
    state: enum { reserved, parked, joined, retired } = .reserved,

    pub fn reserve(team: *abi.Team, owners: []fanout.Owner, io: std.Io, retained_bytes_per_child: u64) !Window {
        if (team.state != .active or team.borrow.cpu_slots != 1 or owners.len == 0)
            return error.InvalidRunnableTransition;
        try team.borrow.checkLifetime();
        if (try team.borrow.currentClass() == .control) return error.InvalidRunnableDemand;
        for (owners) |owner| if (owner.child != null or owner.running) return error.InvalidRunnableTransition;
        var reserved: usize = 0;
        errdefer for (owners[0..reserved]) |*owner| owner.retire();
        for (owners) |*owner| {
            try owner.reserve(team.borrow, io, retained_bytes_per_child);
            reserved += 1;
        }
        return .{ .team = team, .owners = owners };
    }

    /// Attach a phase to pre-reserved invocation-owned packets. No registry
    /// admission or coordination allocation follows the durable decision.
    pub fn reuse(team: *abi.Team, owners: []fanout.Owner) !Window {
        if (team.state != .active or team.borrow.cpu_slots != 1 or owners.len == 0) return error.InvalidRunnableTransition;
        try team.borrow.checkLifetime();
        if (try team.borrow.currentClass() == .control) return error.InvalidRunnableDemand;
        for (owners) |*owner| {
            if (owner.child == null or owner.running or !owner.can_activate) return error.InvalidRunnableTransition;
            try owner.child.?.borrow.checkLifetime();
        }
        return .{ .team = team, .owners = owners, .retire_children = false };
    }

    pub fn park(self: *Window) !void {
        if (self.state != .reserved) return error.InvalidRunnableTransition;
        self.phase = try fanout.parkCoordinator(self.team.borrow);
        self.state = .parked;
    }

    pub fn join(self: *Window, group: *std.Io.Group, io: std.Io) !void {
        if (self.state != .parked) return error.InvalidRunnableTransition;
        // Group.await reports cancellation only after every task has finished.
        // Accepted transaction work handles its original pre-decision fences
        // separately; joining cannot abandon a mandatory completion consumer.
        group.await(io) catch {};
        self.state = .joined;
        for (self.owners) |*owner| owner.joined();
        try fanout.resumeCoordinator(self.team.borrow, self.phase);
    }

    /// A reserved window may retire only when no task was published. A parked
    /// window must physically join even after cancellation or failed resumption.
    pub fn retire(self: *Window) void {
        if (self.state == .retired) return;
        std.debug.assert(self.state == .reserved or self.state == .joined);
        if (self.retire_children) for (self.owners) |*owner| owner.retire();
        self.state = .retired;
    }
};

test "SQL write team participant window physically joins independent completion children at one CPU slot" {
    const runtime = @import("antfly_runtime_abi");
    const native = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
    const allocation = @import("antfly_request_allocation_budget");
    const writes = @import("runnable_write_team.zig");
    const Clock = struct {
        var current: std.atomic.Value(u64) = .init(0);
        fn now() u64 {
            return current.load(.acquire);
        }
        fn canceled(raw: *const anyopaque) bool {
            return @as(*const std.atomic.Value(bool), @ptrCast(@alignCast(raw))).load(.acquire);
        }
    };
    const Worker = struct {
        owner: *fanout.Owner,
        io: std.Io,
        allocator: std.mem.Allocator,
        ready: std.Io.Event = .unset,
        go: *std.Io.Event,
        finished: *std.atomic.Value(usize),
        expire_after_join: bool,
        err: ?anyerror = null,
        fn run(self: *@This()) void {
            self.execute() catch |err| {
                self.err = err;
                self.ready.set(self.io);
            };
            if (self.finished.fetchAdd(1, .acq_rel) == 1 and self.expire_after_join)
                Clock.current.store(20 * std.time.ns_per_s, .release);
        }
        fn execute(self: *@This()) !void {
            const borrow = (try self.owner.activate()).?;
            defer self.owner.park();
            var arena = std.heap.ArenaAllocator.init(self.allocator);
            defer arena.deinit();
            try std.testing.expectEqual(@as(u64, 14 * std.time.ns_per_s), try borrow.completionRemainingNs());
            var wait: fanout.RemoteWait = .{ .borrow = borrow };
            const boundary = wait.boundary().?;
            try boundary.beginWait();
            self.ready.set(self.io);
            self.go.waitUncancelable(self.io);
            try boundary.endWait();
            try borrow.checkLifetime();
            const result = try arena.allocator().dupe(u8, "participant finished");
            try std.testing.expectEqualStrings("participant finished", result);
        }
    };
    const Case = enum { open, closed, expire_after_join };
    for (std.enums.values(Case)) |case| {
        Clock.current.store(0, .release);
        defer Clock.current.store(0, .release);
        const io = std.testing.io;
        const io_borrow = runtime.native_abi.IoBorrow.init(&io);
        var scheduler = try native.Scheduler.init(.{ .slots = 2, .control_slots = 1 });
        defer scheduler.close();
        var memory: allocation.Shared = .{ .child = std.testing.allocator, .limit = 64 * 1024 };
        var canceled = std.atomic.Value(bool).init(false);
        var root: native.Execution = .{};
        defer root.finish();
        try root.start(&scheduler, .{ .io = io, .native_now_ns = Clock.now, .deadline_ns = 5 * std.time.ns_per_s, .retained_bytes = 256, .cancellation = .{ .ptr = &canceled, .requested = Clock.canceled } }, .write);
        var bridge = try writes.Completion.init(&root, memory.allocator());
        var team = try bridge.team();
        try team.borrow.reserveCompletion(&.{ .io = &io_borrow, .native_deadline = 1, .deadline_ns = 20 * std.time.ns_per_s });
        try team.borrow.activateCompletion();
        canceled.store(true, .release);
        Clock.current.store(6 * std.time.ns_per_s, .release);
        if (case == .closed) scheduler.close();
        try team.reserveIndependent();
        defer team.finish();
        try std.testing.expectEqual(@as(usize, 1), team.borrow.cpu_slots);
        var owners: [2]fanout.Owner = @splat(.{});
        var window = try Window.reserve(&team, &owners, io, 31);
        defer window.retire();
        const retained = scheduler.stats().retained_bytes;
        const receivers = memory.used.load(.acquire);
        try std.testing.expect(receivers > 0);
        try std.testing.expectEqual(@as(usize, 3), scheduler.stats().operations);
        try std.testing.expect(owners[0].child.?.borrow.context != owners[1].child.?.borrow.context);
        try std.testing.expect(owners[0].child.?.borrow.context != team.borrow.context);
        try window.park();
        var go: std.Io.Event = .unset;
        var finished: std.atomic.Value(usize) = .init(0);
        var workers: [2]Worker = undefined;
        var group: std.Io.Group = .init;
        // The failure path must still release and physically join every worker.
        defer {
            go.set(io);
            if (window.state == .parked) window.join(&group, io) catch {};
        }
        for (&workers, &owners) |*worker, *owner| {
            worker.* = .{ .owner = owner, .io = io, .allocator = team.allocator, .go = &go, .finished = &finished, .expire_after_join = case == .expire_after_join };
            group.async(io, Worker.run, .{worker});
        }
        for (&workers) |*worker| try worker.ready.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
        for (workers) |worker| try std.testing.expect(worker.err == null);
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().runnable_slots);
        try std.testing.expectEqual(@as(usize, 3), scheduler.stats().resource_blocked);
        try std.testing.expectEqual(retained, scheduler.stats().retained_bytes);
        try std.testing.expectEqual(receivers, memory.used.load(.acquire));
        if (case != .closed) {
            var health: native.Execution = .{};
            defer health.finish();
            try health.start(&scheduler, .{ .io = io, .allow_wait = false }, .control);
            try health.checkpoint();
            health.finish();
        }
        go.set(io);
        if (case == .expire_after_join) {
            try std.testing.expectError(error.DeadlineExceeded, window.join(&group, io));
        } else {
            try window.join(&group, io);
            try std.testing.expectEqual(native.Class.write, try root.retainedClass());
        }
        try std.testing.expectEqual(@as(usize, 2), finished.load(.acquire));
        for (workers) |worker| try std.testing.expect(worker.err == null);
        try std.testing.expectEqual(@as(usize, 3), scheduler.stats().operations);
        try std.testing.expectEqual(retained, scheduler.stats().retained_bytes);
        // All arenas have been destroyed; receiver ownership remains until the
        // physical join and coordinator resumption attempt are both complete.
        try std.testing.expectEqual(receivers, memory.used.load(.acquire));
        window.retire();
        try std.testing.expectEqual(@as(usize, 0), memory.used.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), root.entry.?.children);
        try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 256), scheduler.stats().retained_bytes);
        root.finish();
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().runnable_slots);
    }
}

test "SQL write team participant window reservation rolls back operation byte and allocator denials" {
    const runtime = @import("antfly_runtime_abi");
    const native = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
    const allocation = @import("antfly_request_allocation_budget");
    const writes = @import("runnable_write_team.zig");
    const io = std.testing.io;
    const io_borrow = runtime.native_abi.IoBorrow.init(&io);
    for (0..3) |case| {
        var scheduler = try native.Scheduler.init(.{ .slots = 2, .control_slots = 1, .max_operations = if (case == 0) 4 else 16, .max_bytes = if (case == 1) 4096 else 1024 * 1024, .control_bytes = if (case == 1) 2048 else 65536 });
        defer scheduler.close();
        var memory: allocation.Shared = .{ .child = std.testing.allocator, .limit = if (case == 2) 0 else 64 * 1024 };
        var root: native.Execution = .{};
        defer root.finish();
        const bytes: u64 = if (case == 1) 2032 else 256;
        try root.start(&scheduler, .{ .io = io, .retained_bytes = bytes }, .write);
        var bridge = try writes.Completion.init(&root, memory.allocator());
        var team = try bridge.team();
        try team.borrow.reserveCompletion(&.{ .io = &io_borrow, .deadline_ns = @as(u64, @intCast(std.Io.Clock.awake.now(io).nanoseconds)) + 5 * std.time.ns_per_s });
        try team.borrow.activateCompletion();
        try team.reserveIndependent();
        defer team.finish();
        var owners: [2]fanout.Owner = @splat(.{});
        const cause = if (case == 2) error.OutOfMemory else error.SchedulerFull;
        try std.testing.expectError(cause, Window.reserve(&team, &owners, io, 0));
        for (owners) |owner| try std.testing.expect(owner.child == null and !owner.running);
        try std.testing.expectEqual(@as(usize, 0), memory.used.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), root.entry.?.children);
        try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
        try std.testing.expectEqual(@as(usize, 1), scheduler.stats().runnable_slots);
        try std.testing.expectEqual(bytes, scheduler.stats().retained_bytes);
    }
}

test "SQL write team independent reservation shrinks quiescent grants and rejects unsupported publication" {
    const runtime = @import("antfly_runtime_abi");
    const native = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
    const writes = @import("runnable_write_team.zig");
    const io = std.testing.io;
    const io_borrow = runtime.native_abi.IoBorrow.init(&io);
    var scheduler = try native.Scheduler.init(.{ .slots = 4, .control_slots = 1 });
    defer scheduler.close();
    var root: native.Execution = .{};
    defer root.finish();
    try root.startWithSlots(&scheduler, .{ .io = io, .retained_bytes = 256 }, .write, 3);
    var unsupported = try writes.init(&root, std.testing.allocator);
    try std.testing.expectError(error.UnsupportedOperation, unsupported.reserveIndependent());
    try std.testing.expect(unsupported.state == .fresh);
    try std.testing.expectEqual(@as(usize, 3), scheduler.stats().runnable_slots);
    var bridge = try writes.Completion.init(&root, std.testing.allocator);
    var team = try bridge.team();
    try team.borrow.reserveCompletion(&.{ .io = &io_borrow, .deadline_ns = @as(u64, @intCast(std.Io.Clock.awake.now(io).nanoseconds)) + 5 * std.time.ns_per_s });
    try team.borrow.activateCompletion();
    try team.reserveIndependent();
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(usize, 1), team.borrow.cpu_slots);
    try std.testing.expectError(error.InvalidRunnableTransition, team.reserveIndependent());
    var owners: [1]fanout.Owner = @splat(.{});
    var window = try Window.reserve(&team, &owners, io, 0);
    // Unpublished packets can be abandoned without parking or creating tasks.
    window.retire();
    team.finish();
    try team.reserveIndependent();
    team.finish();
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 256), scheduler.stats().retained_bytes);
}

test "SQL protected commit team independent participant children cannot consume health capacity" {
    const runtime = @import("antfly_runtime_abi");
    const native = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
    const writes = @import("runnable_write_team.zig");
    const io = std.testing.io;
    const io_borrow = runtime.native_abi.IoBorrow.init(&io);
    var scheduler = try native.Scheduler.init(.{ .slots = 4, .control_slots = 2 });
    defer scheduler.close();
    var ordinary: native.Execution = .{};
    defer ordinary.finish();
    try ordinary.startWithSlots(&scheduler, .{ .io = io }, .write, 2);
    var root: native.Execution = .{};
    defer root.finish();
    try root.start(&scheduler, .{ .io = io, .retained_bytes = 256 }, .control);
    var bridge = try writes.Completion.init(&root, std.testing.allocator);
    var team = try bridge.team();
    try team.borrow.reserveCompletion(&.{ .io = &io_borrow, .deadline_ns = @as(u64, @intCast(std.Io.Clock.awake.now(io).nanoseconds)) + 5 * std.time.ns_per_s });
    try team.borrow.activateCompletion();
    try std.testing.expectError(error.InvalidRunnableDemand, team.reserveIndependent());
    try std.testing.expect(team.state == .fresh);
    try std.testing.expectEqual(@as(usize, 0), root.entry.?.children);
    // A legacy single-slot team reservation cannot bypass the independent
    // window's prohibition on widening protected control work.
    try team.reserve(0);
    defer team.finish();
    var owners: [2]fanout.Owner = @splat(.{});
    try std.testing.expectError(error.InvalidRunnableDemand, Window.reserve(&team, &owners, io, 0));
    try std.testing.expectEqual(@as(usize, 0), root.entry.?.children);
    try std.testing.expectEqual(@as(usize, 3), scheduler.stats().runnable_slots);
    var health: native.Execution = .{};
    defer health.finish();
    try health.start(&scheduler, .{ .io = io, .allow_wait = false }, .control);
    try health.checkpoint();
    try std.testing.expectEqual(@as(usize, 4), scheduler.stats().runnable_slots);
}

test "SQL write team reusable participant packets reject activation before join and retain original charges" {
    const runtime = @import("antfly_runtime_abi");
    const native = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
    const allocation = @import("antfly_request_allocation_budget");
    const writes = @import("runnable_write_team.zig");
    const Worker = struct {
        owner: *fanout.Owner,
        err: ?anyerror = null,
        fn run(self: *@This()) void {
            _ = self.owner.activate() catch |err| {
                self.err = err;
                return;
            };
            self.owner.park();
        }
    };
    const io = std.testing.io;
    const io_borrow = runtime.native_abi.IoBorrow.init(&io);
    var scheduler = try native.Scheduler.init(.{ .slots = 2, .control_slots = 1 });
    defer scheduler.close();
    var memory: allocation.Shared = .{ .child = std.testing.allocator, .limit = 64 * 1024 };
    var root: native.Execution = .{};
    defer root.finish();
    try root.start(&scheduler, .{ .io = io, .retained_bytes = 256 }, .write);
    var bridge = try writes.Completion.init(&root, memory.allocator());
    var team = try bridge.team();
    try team.borrow.reserveCompletion(&.{ .io = &io_borrow, .deadline_ns = @as(u64, @intCast(std.Io.Clock.awake.now(io).nanoseconds)) + 5 * std.time.ns_per_s });
    try team.borrow.activateCompletion();
    try team.reserveIndependent();
    defer team.finish();
    var owners: [1]fanout.Owner = @splat(.{});
    try owners[0].reserve(team.borrow, io, 31);
    defer owners[0].retire();
    const original_context = owners[0].child.?.borrow.context;
    const original_bytes = scheduler.stats().retained_bytes;
    const original_allocated = memory.used.load(.acquire);
    for (0..3) |_| {
        var window = try Window.reuse(&team, &owners);
        defer window.retire();
        try window.park();
        var worker: Worker = .{ .owner = &owners[0] };
        var group: std.Io.Group = .init;
        group.async(io, Worker.run, .{&worker});
        group.await(io) catch {};
        // Physical completion alone does not grant another worker the packet;
        // the owning window must explicitly join and finish its handoff.
        try std.testing.expect(worker.err == null);
        try std.testing.expectError(error.InvalidRunnableTransition, owners[0].activate());
        try std.testing.expectError(error.InvalidRunnableTransition, Window.reuse(&team, &owners));
        try window.join(&group, io);
        window.retire();
        try std.testing.expectEqual(original_context, owners[0].child.?.borrow.context);
        try std.testing.expectEqual(original_bytes, scheduler.stats().retained_bytes);
        try std.testing.expectEqual(original_allocated, memory.used.load(.acquire));
        try std.testing.expectEqual(@as(usize, 2), scheduler.stats().operations);
        try std.testing.expectEqual(@as(usize, 1), scheduler.stats().runnable_slots);
    }
    owners[0].retire();
    try std.testing.expectEqual(@as(usize, 0), root.entry.?.children);
    try std.testing.expectEqual(@as(usize, 0), memory.used.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 256), scheduler.stats().retained_bytes);
}
