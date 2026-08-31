// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Antfly adapter from the production durable-job contract to a deterministic
//! `std.Io`. The reusable scheduler remains in `lib/vopr`; this file owns only
//! Antfly job classes and owner lifetimes.
//!
//! Jobs are ordinary `std.Io` group tasks rather than atomic VOPR callbacks.
//! Production work may therefore sleep, wait on futexes, perform I/O, and
//! observe cancellation without escaping the simulated runtime.

const std = @import("std");
const vopr = @import("vopr");
const background_runtime = @import("background_runtime.zig");
const lsm_background = @import("lsm_backend/background.zig");

pub const Lane = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    entries: std.ArrayListUnmanaged(*Entry) = .empty,
    closed_owners: std.AutoHashMapUnmanaged(u64, void) = .empty,
    paused_owners: std.AutoHashMapUnmanaged(u64, void) = .empty,
    completed_jobs: u64 = 0,
    failed_jobs: u64 = 0,
    last_error_name: ?[]const u8 = null,

    const Entry = struct {
        lane: *Lane,
        group: std.Io.Group = .init,
        job: background_runtime.Job,
        finished: bool = false,

        fn run(self: *Entry) std.Io.Cancelable!void {
            defer {
                self.job.deinit(self.job.ptr);
                self.finished = true;
            }
            self.job.run(self.job.ptr) catch |err| {
                if (err == error.Canceled or err == error.Cancelled)
                    return error.Canceled;
                self.lane.failed_jobs +|= 1;
                self.lane.last_error_name = @errorName(err);
                return;
            };
            self.lane.completed_jobs +|= 1;
        }
    };

    pub const Stats = struct {
        pending_jobs: usize,
        completed_jobs: u64,
        failed_jobs: u64,
        last_error_name: ?[]const u8,
    };

    pub fn init(allocator: std.mem.Allocator, io: std.Io) Lane {
        return .{ .allocator = allocator, .io = io };
    }

    pub fn deinit(self: *Lane) void {
        while (self.entries.items.len != 0) {
            const entry = self.entries.items[self.entries.items.len - 1];
            entry.group.cancel(self.io);
            self.removeAndDestroyEntry(entry);
        }
        self.entries.deinit(self.allocator);
        self.closed_owners.deinit(self.allocator);
        self.paused_owners.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn lane(self: *Lane) background_runtime.DurableJobLane {
        return .{ .ptr = self, .vtable = &vtable };
    }

    /// Reopens a logical owner after a prior close. Owner IDs are supplied by
    /// the scenario so their identity is part of its replayable state.
    pub fn registerOwner(self: *Lane, owner_id: u64) !void {
        if (owner_id == 0) return error.InvalidBackgroundOwner;
        _ = self.closed_owners.remove(owner_id);
        _ = self.paused_owners.remove(owner_id);
    }

    pub fn stats(self: *const Lane) Stats {
        return .{
            .pending_jobs = self.entries.items.len,
            .completed_jobs = self.completed_jobs,
            .failed_jobs = self.failed_jobs,
            .last_error_name = self.last_error_name,
        };
    }

    fn submit(ptr: *anyopaque, job: background_runtime.Job) !void {
        const self: *Lane = @ptrCast(@alignCast(ptr));
        if (job.owner_id == 0) return error.InvalidBackgroundOwner;
        if (self.closed_owners.contains(job.owner_id)) return error.BackgroundOwnerClosed;
        if (self.paused_owners.contains(job.owner_id)) return error.BackgroundOwnerPaused;

        const entry = try self.allocator.create(Entry);
        errdefer self.allocator.destroy(entry);
        entry.* = .{
            .lane = self,
            .job = job,
        };
        try self.entries.append(self.allocator, entry);
        errdefer _ = self.entries.pop();
        entry.group.async(self.io, Entry.run, .{entry});
    }

    fn drainOwner(ptr: *anyopaque, owner_id: u64) void {
        const self: *Lane = @ptrCast(@alignCast(ptr));
        while (self.findOwnerEntry(owner_id)) |entry| {
            entry.group.await(self.io) catch |err| switch (err) {
                error.Canceled => {},
            };
            self.removeAndDestroyEntry(entry);
        }
    }

    fn closeOwner(ptr: *anyopaque, owner_id: u64) void {
        const self: *Lane = @ptrCast(@alignCast(ptr));
        _ = self.paused_owners.remove(owner_id);
        self.closed_owners.put(self.allocator, owner_id, {}) catch |err| {
            std.debug.panic("failed to close VOPR durable-job owner: {s}", .{@errorName(err)});
        };
        while (self.findOwnerEntry(owner_id)) |entry| {
            entry.group.cancel(self.io);
            self.removeAndDestroyEntry(entry);
        }
    }

    fn pauseOwner(ptr: *anyopaque, owner_id: u64) !void {
        const self: *Lane = @ptrCast(@alignCast(ptr));
        if (owner_id == 0) return error.InvalidBackgroundOwner;
        if (self.closed_owners.contains(owner_id)) return error.BackgroundOwnerClosed;
        try self.paused_owners.put(self.allocator, owner_id, {});
    }

    fn resumeOwner(ptr: *anyopaque, owner_id: u64) !void {
        const self: *Lane = @ptrCast(@alignCast(ptr));
        if (owner_id == 0) return error.InvalidBackgroundOwner;
        if (self.closed_owners.contains(owner_id)) return error.BackgroundOwnerClosed;
        _ = self.paused_owners.remove(owner_id);
    }

    fn reopenOwner(ptr: *anyopaque, owner_id: u64) !void {
        const self: *Lane = @ptrCast(@alignCast(ptr));
        try self.registerOwner(owner_id);
    }

    fn poll(_: *anyopaque, _: usize) !usize {
        // Execution is exclusively controlled by SchedulerPort choices.
        return 0;
    }

    fn findOwnerEntry(self: *Lane, owner_id: u64) ?*Entry {
        for (self.entries.items) |entry|
            if (entry.job.owner_id == owner_id) return entry;
        return null;
    }

    fn removeAndDestroyEntry(self: *Lane, target: *Entry) void {
        for (self.entries.items, 0..) |entry, index| {
            if (entry == target) {
                _ = self.entries.swapRemove(index);
                // A group canceled before its function starts never enters
                // Entry.run, so the production job still owns its deinit hook.
                if (!target.finished) {
                    target.job.deinit(target.job.ptr);
                    target.finished = true;
                }
                self.allocator.destroy(target);
                return;
            }
        }
        @panic("VOPR durable job entry was finalized more than once");
    }

    const vtable = background_runtime.DurableJobLane.VTable{
        .submit = submit,
        .drain_owner = drainOwner,
        .close_owner = closeOwner,
        .pause_owner = pauseOwner,
        .resume_owner = resumeOwner,
        .reopen_owner = reopenOwner,
        .poll = poll,
        .executes_inline = false,
    };
};

test "VOPR durable job lane runs the production LSM background executor as a scheduler transition" {
    const Context = struct {
        runs: usize = 0,
        deinits: usize = 0,

        fn run(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.runs += 1;
        }

        fn deinitJob(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.deinits += 1;
        }
    };

    var runtime = try vopr.vopr_io.VoprIo.init(.{ .task_allocator = std.testing.allocator });
    defer runtime.deinit();
    var adapter = Lane.init(std.testing.allocator, runtime.io());
    defer adapter.deinit();
    var context = Context{};
    const production_executor = lsm_background.Executor.initLane(adapter.lane(), 41);
    try production_executor.submit(.commit_durable, &context, Context.run, Context.deinitJob);

    try std.testing.expectEqual(@as(usize, 1), adapter.stats().pending_jobs);
    try std.testing.expectEqual(@as(usize, 0), context.runs);
    var transitions = vopr.transition.List{};
    defer transitions.deinit(std.testing.allocator);
    try runtime.scheduler().enumerateReady(&transitions, std.testing.allocator);
    try transitions.canonicalize();
    try std.testing.expectEqual(@as(usize, 1), transitions.items.items.len);
    var sink = vopr.event.Sink{};
    defer sink.deinit(std.testing.allocator);
    try runtime.scheduler().executeReady(transitions.items.items[0].id, &sink, std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), context.runs);
    try std.testing.expectEqual(@as(usize, 1), context.deinits);
    try std.testing.expectEqual(@as(u64, 1), adapter.stats().completed_jobs);
    adapter.lane().drainOwner(41);
    try std.testing.expectEqual(@as(usize, 0), adapter.stats().pending_jobs);
}

test "VOPR durable job owner close cancels queued work exactly once" {
    const Context = struct {
        runs: usize = 0,
        deinits: usize = 0,

        fn run(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.runs += 1;
        }

        fn deinitJob(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.deinits += 1;
        }
    };

    var runtime = try vopr.vopr_io.VoprIo.init(.{ .task_allocator = std.testing.allocator });
    defer runtime.deinit();
    var adapter = Lane.init(std.testing.allocator, runtime.io());
    defer adapter.deinit();
    var context = Context{};
    try adapter.lane().submit(.{
        .owner_id = 7,
        .class = .cleanup,
        .ptr = &context,
        .run = Context.run,
        .deinit = Context.deinitJob,
    });
    adapter.lane().closeOwner(7);

    try std.testing.expectEqual(@as(usize, 0), context.runs);
    try std.testing.expectEqual(@as(usize, 1), context.deinits);
    try std.testing.expect(runtime.scheduler().quiescent());
    try std.testing.expectError(error.BackgroundOwnerClosed, adapter.lane().submit(.{
        .owner_id = 7,
        .class = .cleanup,
        .ptr = &context,
        .run = Context.run,
        .deinit = Context.deinitJob,
    }));
    // A failed submission leaves ownership with the caller.
    try std.testing.expectEqual(@as(usize, 1), context.deinits);
}

test "VOPR durable job owner uses production pause close and reopen protocol" {
    const Context = struct {
        runs: usize = 0,
        deinits: usize = 0,

        fn run(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.runs += 1;
        }

        fn deinitJob(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.deinits += 1;
        }
    };

    var runtime = try vopr.vopr_io.VoprIo.init(.{ .task_allocator = std.testing.allocator });
    defer runtime.deinit();
    var adapter = Lane.init(std.testing.allocator, runtime.io());
    defer adapter.deinit();
    const lane = adapter.lane();
    var context = Context{};

    try lane.pauseOwner(17);
    try std.testing.expectError(error.BackgroundOwnerPaused, lane.submit(.{
        .owner_id = 17,
        .class = .maintenance,
        .ptr = &context,
        .run = Context.run,
        .deinit = Context.deinitJob,
    }));
    try lane.resumeOwner(17);
    try lane.submit(.{
        .owner_id = 17,
        .class = .maintenance,
        .ptr = &context,
        .run = Context.run,
        .deinit = Context.deinitJob,
    });
    lane.closeOwner(17);
    try std.testing.expectEqual(@as(usize, 0), context.runs);
    try std.testing.expectEqual(@as(usize, 1), context.deinits);

    try lane.reopenOwner(17);
    try lane.submit(.{
        .owner_id = 17,
        .class = .cleanup,
        .ptr = &context,
        .run = Context.run,
        .deinit = Context.deinitJob,
    });
    var transitions: vopr.transition.List = .{};
    defer transitions.deinit(std.testing.allocator);
    try runtime.scheduler().enumerateReady(&transitions, std.testing.allocator);
    try transitions.canonicalize();
    var sink: vopr.event.Sink = .{};
    defer sink.deinit(std.testing.allocator);
    try runtime.scheduler().executeReady(transitions.items.items[0].id, &sink, std.testing.allocator);
    lane.drainOwner(17);
    lane.closeOwner(17);
    try std.testing.expectEqual(@as(usize, 1), context.runs);
    try std.testing.expectEqual(@as(usize, 2), context.deinits);
}
