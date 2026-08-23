// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Antfly adapter from the production durable-job contract to VOPR's narrow
//! deterministic executor. The reusable scheduler remains in `lib/vopr`; the
//! knowledge of Antfly job classes and owner lifetimes stays here.
//!
//! A submission becomes one atomic VOPR scheduler transition. `closeOwner`
//! cancels queued work because a deterministic runtime cannot synchronously
//! wait for its own scheduler. `drainOwner` therefore requires the caller to
//! have driven the runtime to quiescence and fails loudly if work is pending.

const std = @import("std");
const vopr = @import("vopr");
const background_runtime = @import("background_runtime.zig");
const lsm_background = @import("lsm_backend/background.zig");

pub const Lane = struct {
    allocator: std.mem.Allocator,
    executor: vopr.runtime.Executor,
    next_sequence: u64 = 1,
    entries: std.ArrayListUnmanaged(*Entry) = .empty,
    closed_owners: std.AutoHashMapUnmanaged(u64, void) = .empty,
    completed_jobs: u64 = 0,
    failed_jobs: u64 = 0,
    last_error_name: ?[]const u8 = null,

    const Entry = struct {
        lane: *Lane,
        task_id: vopr.runtime.TaskId,
        job: background_runtime.Job,

        fn run(ptr: *anyopaque) !void {
            const self: *Entry = @ptrCast(@alignCast(ptr));
            self.job.run(self.job.ptr) catch |err| {
                self.lane.failed_jobs +|= 1;
                self.lane.last_error_name = @errorName(err);
                return;
            };
            self.lane.completed_jobs +|= 1;
        }

        fn deinit(ptr: *anyopaque) void {
            const self: *Entry = @ptrCast(@alignCast(ptr));
            const owner = self.lane;
            owner.removeEntry(self);
            self.job.deinit(self.job.ptr);
            owner.allocator.destroy(self);
        }
    };

    pub const Stats = struct {
        pending_jobs: usize,
        completed_jobs: u64,
        failed_jobs: u64,
        last_error_name: ?[]const u8,
    };

    pub fn init(allocator: std.mem.Allocator, executor: vopr.runtime.Executor) Lane {
        return .{ .allocator = allocator, .executor = executor };
    }

    pub fn deinit(self: *Lane) void {
        while (self.entries.items.len != 0) {
            const task_id = self.entries.items[self.entries.items.len - 1].task_id;
            const canceled = self.executor.cancel(task_id) catch |err| {
                std.debug.panic("failed to cancel VOPR durable job during teardown: {s}", .{@errorName(err)});
            };
            if (!canceled) @panic("VOPR durable job disappeared during serialized teardown");
        }
        self.entries.deinit(self.allocator);
        self.closed_owners.deinit(self.allocator);
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
    }

    pub fn stats(self: *const Lane) Stats {
        return .{
            .pending_jobs = self.entries.items.len,
            .completed_jobs = self.completed_jobs,
            .failed_jobs = self.failed_jobs,
            .last_error_name = self.last_error_name,
        };
    }

    fn className(class: background_runtime.Job.Class) []const u8 {
        return switch (class) {
            .commit_durable => "antfly.commit_durable",
            .maintenance => "antfly.maintenance",
            .cleanup => "antfly.cleanup",
        };
    }

    fn allocateTaskId(self: *Lane, owner_id: u64) !vopr.runtime.TaskId {
        if (self.next_sequence == std.math.maxInt(u64)) return error.VoprDurableJobSequenceExhausted;
        const sequence = self.next_sequence;
        self.next_sequence += 1;
        return vopr.id.derive("antfly.durable-job", owner_id, sequence);
    }

    fn submit(ptr: *anyopaque, job: background_runtime.Job) !void {
        const self: *Lane = @ptrCast(@alignCast(ptr));
        if (job.owner_id == 0) return error.InvalidBackgroundOwner;
        if (self.closed_owners.contains(job.owner_id)) return error.BackgroundOwnerClosed;

        const entry = try self.allocator.create(Entry);
        errdefer self.allocator.destroy(entry);
        entry.* = .{
            .lane = self,
            .task_id = try self.allocateTaskId(job.owner_id),
            .job = job,
        };
        try self.entries.append(self.allocator, entry);
        errdefer _ = self.entries.pop();
        try self.executor.submit(.{
            .id = entry.task_id,
            .name = className(job.class),
            .context = entry,
            .run_fn = Entry.run,
            .deinit_fn = Entry.deinit,
        });
    }

    fn drainOwner(ptr: *anyopaque, owner_id: u64) void {
        const self: *Lane = @ptrCast(@alignCast(ptr));
        for (self.entries.items) |entry| {
            if (entry.job.owner_id == owner_id) {
                @panic("drive the VOPR scheduler to quiescence before draining a durable-job owner");
            }
        }
    }

    fn closeOwner(ptr: *anyopaque, owner_id: u64) void {
        const self: *Lane = @ptrCast(@alignCast(ptr));
        self.closed_owners.put(self.allocator, owner_id, {}) catch |err| {
            std.debug.panic("failed to close VOPR durable-job owner: {s}", .{@errorName(err)});
        };
        while (true) {
            const task_id = for (self.entries.items) |entry| {
                if (entry.job.owner_id == owner_id) break entry.task_id;
            } else break;
            const canceled = self.executor.cancel(task_id) catch |err| {
                std.debug.panic("failed to cancel VOPR durable job: {s}", .{@errorName(err)});
            };
            if (!canceled) @panic("VOPR durable job disappeared during serialized owner close");
        }
    }

    fn poll(_: *anyopaque, _: usize) !usize {
        // Execution is exclusively controlled by SchedulerPort choices.
        return 0;
    }

    fn removeEntry(self: *Lane, target: *Entry) void {
        for (self.entries.items, 0..) |entry, index| {
            if (entry == target) {
                _ = self.entries.swapRemove(index);
                return;
            }
        }
        @panic("VOPR durable job entry was finalized more than once");
    }

    const vtable = background_runtime.DurableJobLane.VTable{
        .submit = submit,
        .drain_owner = drainOwner,
        .close_owner = closeOwner,
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

    var runtime = vopr.sim_runtime.SimRuntime.init(std.testing.allocator, 0);
    defer runtime.deinit();
    var adapter = Lane.init(std.testing.allocator, runtime.runtime().executor);
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
    try std.testing.expectEqual(@as(usize, 0), adapter.stats().pending_jobs);
    try std.testing.expectEqual(@as(u64, 1), adapter.stats().completed_jobs);
    adapter.lane().drainOwner(41);
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

    var runtime = vopr.sim_runtime.SimRuntime.init(std.testing.allocator, 0);
    defer runtime.deinit();
    var adapter = Lane.init(std.testing.allocator, runtime.runtime().executor);
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
