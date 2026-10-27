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

//! Antfly adapter from the production durable-job contract to a deterministic
//! `std.Io`. The reusable scheduler remains in `lib/vopr`; this file owns only
//! Antfly job classes and owner lifetimes.
//!
//! Jobs are ordinary `std.Io` group tasks rather than atomic VOPR callbacks.
//! Production work may therefore sleep, wait on futexes, perform I/O, and
//! observe cancellation without escaping the simulated runtime.

const std = @import("std");
const vopr = @import("vopr");
const background_runtime = @import("antfly_local_sources").storage_background_runtime;
const lsm_background = @import("antfly_local_sources").storage_lsm_backend_background;

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
        // A canceller/awaiter owns the group until it has returned from the
        // VoprIo yield. A different fiber must not reap its completed child
        // and free the group while that owner is still parked.
        finalizing: bool = false,

        fn run(self: *Entry) std.Io.Cancelable!void {
            defer {
                self.job.deinit(self.job.ptr);
                self.finished = true;
            }
            // Group cancellation still enters this ownership wrapper. Reject
            // cancelled queued work only after installing its cleanup defer.
            try self.lane.io.checkCancel();
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
            std.debug.assert(!entry.finalizing);
            entry.finalizing = true;
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
        var pending: usize = 0;
        for (self.entries.items) |entry| if (!entry.finished) {
            pending += 1;
        };
        return .{
            .pending_jobs = pending,
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
        _ = try poll(self, self.entries.items.len);

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
            entry.finalizing = true;
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
            entry.finalizing = true;
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

    fn poll(ptr: *anyopaque, max_jobs: usize) !usize {
        const self: *Lane = @ptrCast(@alignCast(ptr));
        var reaped: usize = 0;
        var index: usize = 0;
        while (index < self.entries.items.len and reaped < max_jobs) {
            const entry = self.entries.items[index];
            if (!entry.finished or entry.finalizing) {
                index += 1;
                continue;
            }
            // Only reclaim completed groups. Job execution remains entirely
            // controlled by SchedulerPort choices, including cancellation.
            entry.group.await(self.io) catch {};
            self.removeAndDestroyEntry(entry);
            reaped += 1;
        }
        return reaped;
    }

    fn findOwnerEntry(self: *Lane, owner_id: u64) ?*Entry {
        for (self.entries.items) |entry|
            if (entry.job.owner_id == owner_id and !entry.finalizing) return entry;
        return null;
    }

    fn removeAndDestroyEntry(self: *Lane, target: *Entry) void {
        for (self.entries.items, 0..) |entry, index| {
            if (entry == target) {
                _ = self.entries.swapRemove(index);
                // Await/cancel must drain the wrapper and its cleanup defer.
                std.debug.assert(target.finished);
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
    try std.testing.expectEqual(@as(usize, 0), adapter.stats().pending_jobs);
    try std.testing.expectEqual(@as(usize, 1), try adapter.lane().poll(1));
    try std.testing.expectEqual(@as(usize, 0), try adapter.lane().poll(1));
    adapter.lane().drainOwner(41);
    try std.testing.expectEqual(@as(usize, 1), context.deinits);
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

test "VOPR durable job poll cannot reap a group while its canceller is parked" {
    const Context = struct {
        io: std.Io,
        lane: *Lane,
        entered: bool = false,
        cleaned: bool = false,
        closing: bool = false,
        closed: bool = false,

        fn run(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.entered = true;
            const forever: std.Io.Timeout = .none;
            try forever.sleep(self.io);
        }

        fn deinitJob(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.cleaned = true;
        }

        fn close(self: *@This()) void {
            self.closing = true;
            self.lane.lane().closeOwner(7);
            self.closed = true;
        }
    };

    var runtime = try vopr.vopr_io.VoprIo.init(.{ .task_allocator = std.testing.allocator });
    defer runtime.deinit();
    var adapter = Lane.init(std.testing.allocator, runtime.io());
    defer adapter.deinit();
    var context = Context{ .io = runtime.io(), .lane = &adapter };
    try adapter.lane().submit(.{
        .owner_id = 7,
        .class = .cleanup,
        .ptr = &context,
        .run = Context.run,
        .deinit = Context.deinitJob,
    });

    var transitions: vopr.transition.List = .{};
    defer transitions.deinit(std.testing.allocator);
    var sink: vopr.event.Sink = .{};
    defer sink.deinit(std.testing.allocator);
    const scheduler = runtime.scheduler();
    try scheduler.enumerateReady(&transitions, std.testing.allocator);
    try transitions.canonicalize();
    try std.testing.expectEqual(@as(usize, 1), transitions.items.items.len);
    try scheduler.executeReady(transitions.items.items[0].id, &sink, std.testing.allocator);
    try std.testing.expect(context.entered);
    try std.testing.expect(!context.cleaned);

    var closer: std.Io.Group = .init;
    closer.async(runtime.io(), Context.close, .{&context});
    for (0..32) |_| {
        if (context.closing and context.cleaned) break;
        transitions.items.clearRetainingCapacity();
        try scheduler.enumerateReady(&transitions, std.testing.allocator);
        try transitions.canonicalize();
        try std.testing.expect(transitions.items.items.len != 0);
        try scheduler.executeReady(transitions.items.items[0].id, &sink, std.testing.allocator);
    } else return error.VoprIoCancellationDidNotDrain;

    try std.testing.expect(!context.closed);
    try std.testing.expectEqual(@as(usize, 0), try adapter.lane().poll(1));
    for (0..32) |_| {
        if (context.closed) break;
        transitions.items.clearRetainingCapacity();
        try scheduler.enumerateReady(&transitions, std.testing.allocator);
        try transitions.canonicalize();
        try std.testing.expect(transitions.items.items.len != 0);
        try scheduler.executeReady(transitions.items.items[0].id, &sink, std.testing.allocator);
    } else return error.VoprIoCancellationDidNotDrain;

    try std.testing.expect(context.cleaned);
    try std.testing.expectEqual(@as(usize, 0), adapter.stats().pending_jobs);
    closer.cancel(runtime.io());
    try runtime.ensureNoCapabilityViolation();
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

test "VOPR durable document owner takeover replays accepted stages and rejects delayed completion" {
    const readers = @import("antfly_readers");
    const runs = readers.document_runs;
    const Fixture = struct {
        io: std.Io,
        store: runs.Store,
        old: runs.Lease,
        identity: runs.Identity,
        physical_calls: u64 = 1,
        replayed: bool = false,
        deinits: usize = 0,
        old_delay: u64,
        fn now(self: *@This()) u64 {
            return @intCast(@divFloor(std.Io.Clock.real.now(self.io).toNanoseconds(), std.time.ns_per_ms));
        }
        fn oldRun(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.io.sleep(.fromMilliseconds(@intCast(self.old_delay)), .awake);
            var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer arena.deinit();
            _ = self.store.complete(arena.allocator(), self.old, @splat(1), .{ .calls = 1, .tokens = 3, .output_bytes = 3 }, "old", self.now()) catch |err| switch (err) {
                error.DocumentRunLeaseLost => return,
                else => return err,
            };
        }
        fn newRun(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.io.sleep(.fromMilliseconds(15), .awake);
            var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer arena.deinit();
            const a = arena.allocator();
            const lease = try self.store.claim(a, "simulated", "alice", self.identity, self.now(), 100);
            const record = try self.store.load(a, "simulated", "alice");
            var index = try runs.Index.init(std.testing.allocator, self.store, record, 100, 16384);
            defer index.deinit();
            const result = if (index.get(@splat(1))) |accepted| result: {
                self.replayed = true;
                break :result accepted;
            } else result: {
                if (!try self.store.reserve(a, lease, @splat(1), .{ .calls = 1, .tokens = 7, .output_bytes = 32 }, self.now())) return error.TestUnexpectedResult;
                self.physical_calls += 1;
                break :result try self.store.complete(a, lease, @splat(1), .{ .calls = 1, .tokens = 3, .output_bytes = 3 }, "new", self.now());
            };
            try self.store.finish(a, lease, result, self.now());
        }
        fn deinitJob(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.deinits += 1;
        }
        fn simulate(seed: u64) ![32]u8 {
            const a = std.testing.allocator;
            var runtime = try vopr.vopr_io.VoprIo.init(.{ .seed = seed, .task_allocator = a });
            defer runtime.deinit();
            var adapter = Lane.init(a, runtime.io());
            defer adapter.deinit();
            var memory = readers.testing.Store{ .a = a };
            defer memory.deinit();
            const store = memory.interface();
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const work = arena.allocator();
            const identity: runs.Identity = .{ .request = @splat(1), .sources = @splat(2), .definition = @splat(3), .models = @splat(4), .runtime = @splat(5) };
            try store.create(.{ .run_id = "simulated", .owner = "alice", .identity = identity, .inputs = &.{}, .limits = .{ .calls = 4, .tokens = 100, .output_bytes = 1000 } });
            const initial = try store.claim(work, "simulated", "alice", identity, 0, 10);
            try std.testing.expect(try store.reserve(work, initial, @splat(1), .{ .calls = 1, .tokens = 7, .output_bytes = 32 }, 0));
            var fixture = @This(){ .io = runtime.io(), .store = store, .old = initial, .identity = identity, .old_delay = if (seed % 2 == 0) 5 else 20 };
            try adapter.lane().submit(.{ .owner_id = 1, .class = .maintenance, .ptr = &fixture, .run = oldRun, .deinit = deinitJob });
            try adapter.lane().submit(.{ .owner_id = 2, .class = .maintenance, .ptr = &fixture, .run = newRun, .deinit = deinitJob });
            var choices = std.Random.DefaultPrng.init(seed);
            var trace = std.crypto.hash.sha2.Sha256.init(.{});
            var sink: vopr.event.Sink = .{};
            defer sink.deinit(a);
            for (0..128) |_| {
                var transitions: vopr.transition.List = .{};
                defer transitions.deinit(a);
                try runtime.scheduler().enumerateReady(&transitions, a);
                try transitions.canonicalize();
                if (transitions.items.items.len == 0) break;
                const selected = transitions.items.items[choices.random().uintLessThan(usize, transitions.items.items.len)];
                trace.update(std.mem.asBytes(&selected.id));
                try runtime.scheduler().executeReady(selected.id, &sink, a);
            }
            _ = try adapter.lane().poll(16);
            try std.testing.expectEqual(@as(usize, 0), adapter.stats().pending_jobs);
            try std.testing.expectEqual(@as(usize, 2), fixture.deinits);
            try std.testing.expectEqual(@as(u64, 0), adapter.stats().failed_jobs);
            const final = try store.load(work, "simulated", "alice");
            try std.testing.expectEqual(.succeeded, final.state);
            try std.testing.expectEqual(@as(u64, if (fixture.replayed) 1 else 2), fixture.physical_calls);
            try std.testing.expectEqual(@as(u64, if (fixture.replayed) 3 else 10), final.used.tokens);
            try std.testing.expectEqual(@as(u64, if (fixture.replayed) 0 else 7), final.uncertain.tokens);
            return trace.finalResult();
        }
    };
    for (0..16) |seed| {
        const first = try Fixture.simulate(seed);
        const replay = try Fixture.simulate(seed);
        try std.testing.expectEqualSlices(u8, &first, &replay);
    }
}
