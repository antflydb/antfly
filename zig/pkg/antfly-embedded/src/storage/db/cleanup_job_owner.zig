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

const std = @import("std");
const admission = @import("coalesced_job_admission.zig");
const builtin = @import("builtin");
const runtime = @import("../background_runtime.zig");

pub const Advance = enum { idle, progressed, busy };
/// Bounded cleanup supervision over borrowed storage operations. The admission
/// atomic is owned by IndexManager; its durable lane must drain before either
/// the manager or operation context is destroyed. Storage cursors and markers
/// remain authoritative after a job yields or admission fails.
pub const Port = struct {
    ptr: *anyopaque,
    state: *std.atomic.Value(u8),
    lane: runtime.DurableJobLane,
    owner_id: u64,
    io: ?std.Io,
    closing: ?*const std.atomic.Value(bool) = null,
    advance: *const fn (*anyopaque) anyerror!Advance,
    name: []const u8,
    /// Classifies one of `advance`'s own errors as persistent (`true`)
    /// rather than transient (`false`, the default for every error when
    /// this is unset, preserving prior behavior for a caller that never
    /// opts in). A persistent error gives up immediately -- see
    /// `drain`'s own doc comment -- instead of spending this run's full
    /// retry budget backing off on something that backing off cannot
    /// fix.
    persistent: ?*const fn (anyerror) bool = null,
    /// Called at most once per `drain` run, the moment it gives up on a
    /// run of failures from `advance` (persistent, by `persistent`'s own
    /// classification, or merely exhausted its retry budget) instead of
    /// resubmitting. `drain` itself knows nothing about what `advance`
    /// is backed by, so recording this anywhere a status/maintenance
    /// view can see it -- a counter, a log line beyond the ones `drain`
    /// already emits, a "last error" field -- is entirely the owner's
    /// own call. Always invoked after `port.state` has already been
    /// released (see `drain`), so there is nothing here to coordinate
    /// with a concurrent `schedule` about.
    on_give_up: ?*const fn (*anyopaque, anyerror) void = null,
};
const Work = struct {
    port: Port,
    fn run(ptr: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try drain(self.port);
    }
    pub fn deinit(ptr: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        std.heap.page_allocator.destroy(self);
    }
};
pub fn schedule(port: Port) void {
    if (port.closing) |closing| if (closing.load(.acquire)) return;
    if (!admission.request(port.state)) return;
    const work = std.heap.page_allocator.create(Work) catch {
        port.state.store(0, .release);
        return;
    };
    work.* = .{ .port = port };
    port.lane.submit(.{ .owner_id = port.owner_id, .class = .cleanup, .ptr = work, .run = Work.run, .deinit = Work.deinit }) catch |err| {
        std.heap.page_allocator.destroy(work);
        port.state.store(0, .release);
        std.log.warn("{s} cleanup was not scheduled err={s}", .{ port.name, @errorName(err) });
    };
}
fn pause(io: std.Io, attempt: usize) void {
    const shift: u6 = @intCast(@min(attempt - 1, 5));
    const delay_ms: i64 = if (builtin.is_test) 1 else @min(@as(i64, 25) << shift, 1000);
    io.sleep(.fromMilliseconds(delay_ms), .awake) catch {};
}
fn yield(port: Port) void {
    port.state.store(0, .release);
    // Inline lanes have no queue to yield to: explicit polling or reopening
    // rediscover the durable marker. Never submit recursively on this stack.
    if (!port.lane.executesInline()) schedule(port);
}
/// `advance`'s own errors never loop forever here, no matter how
/// persistent one turns out to be. Giving up (clearing `port.state` and
/// returning the error, never calling `yield`/`schedule` again from
/// this run) happens in exactly two cases: `port.persistent` classifies
/// the error as persistent -- immediately, without spending the rest of
/// this run's backoff budget on something backing off cannot fix -- or
/// this run's retry budget (8 attempts) is exhausted regardless of
/// classification. Before this, an unclassified-but-actually-persistent
/// error (e.g. a storage invariant violation) still hit the 8-attempt
/// cap, but `yield` resubmitted a *fresh* `drain` with its own retry
/// counter back at zero -- forever, since nothing about the failure
/// ever changes between resubmissions. `.busy`/`.idle` contention below
/// is a different matter: that contention is expected to clear through
/// someone else's progress, so it keeps its own existing yield-and-
/// resubmit backoff unchanged.
pub fn drain(port: Port) !void {
    var pages: usize = 0;
    var retries: usize = 0;
    var contention_retries: usize = 0;
    while (true) {
        if (port.closing) |closing| if (closing.load(.acquire)) {
            port.state.store(0, .release);
            return;
        };
        const advance = port.advance(port.ptr) catch |err| {
            retries += 1;
            if (retries == 1 or std.math.isPowerOfTwo(retries)) std.log.warn("{s} cleanup retry attempt={} err={s}", .{ port.name, retries, @errorName(err) });
            const give_up = retries >= 8 or (if (port.persistent) |classify| classify(err) else false);
            if (give_up) {
                port.state.store(0, .release);
                if (port.on_give_up) |cb| cb(port.ptr, err);
                return err;
            }
            if (port.io) |io| pause(io, retries) else {
                port.state.store(0, .release);
                return err;
            }
            continue;
        };
        retries = 0;
        if (advance == .busy) {
            const io = port.io orelse {
                port.state.store(0, .release);
                return;
            };
            contention_retries += 1;
            if (contention_retries < 8) {
                pause(io, contention_retries);
                continue;
            }
            yield(port);
            return;
        }
        contention_retries = 0;
        if (advance == .idle) {
            if (admission.settled(port.state)) return;
            pages = 0;
            continue;
        }
        pages += 1;
        if (pages < 8) continue;
        yield(port);
        return;
    }
}

test "cleanup job owner bounds inline error contention and progress slices" {
    const F = struct {
        state: std.atomic.Value(u8) = .init(0),
        calls: usize = 0,
        depth: usize = 0,
        max_depth: usize = 0,
        outcome: ?Advance = null,
        fn submit(ptr: *anyopaque, job: runtime.Job) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.depth += 1;
            defer self.depth -= 1;
            self.max_depth = @max(self.max_depth, self.depth);
            try job.run(job.ptr);
            job.deinit(job.ptr);
        }
        fn noop(_: *anyopaque, _: u64) void {}
        fn poll(_: *anyopaque, _: usize) !usize {
            return 0;
        }
        fn advance(ptr: *anyopaque) !Advance {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            return self.outcome orelse error.InjectedCleanupFailure;
        }
        fn port(self: *@This()) Port {
            return .{ .ptr = self, .state = &self.state, .lane = .{ .ptr = self, .vtable = &.{ .submit = submit, .drain_owner = noop, .close_owner = noop, .poll = poll, .executes_inline = true } }, .owner_id = 1, .io = std.testing.io, .advance = advance, .name = "inline-test" };
        }
    };
    var f: F = .{};
    for ([_]?Advance{ null, .busy, .progressed }) |outcome| {
        f.outcome = outcome;
        const before = f.calls;
        schedule(f.port());
        try std.testing.expectEqual(@as(usize, 8), f.calls - before);
        try std.testing.expectEqual(@as(u8, 0), f.state.load(.acquire));
    }
    f.outcome = .idle;
    schedule(f.port());
    try std.testing.expectEqual(@as(usize, 25), f.calls);
    try std.testing.expectEqual(@as(usize, 1), f.max_depth);
}

test "cleanup job owner coalesces a raced idle notification and respects closing" {
    const F = struct {
        state: std.atomic.Value(u8) = .init(1),
        calls: usize = 0,
        fn advance(ptr: *anyopaque) !Advance {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            if (self.calls == 1) self.state.store(2, .release);
            return .idle;
        }
    };
    var f: F = .{};
    var closing: std.atomic.Value(bool) = .init(false);
    const port: Port = .{ .ptr = &f, .state = &f.state, .lane = undefined, .owner_id = 1, .io = null, .closing = &closing, .advance = F.advance, .name = "test" };
    try drain(port);
    try std.testing.expectEqual(@as(usize, 2), f.calls);
    closing.store(true, .release);
    schedule(port);
    try std.testing.expectEqual(@as(u8, 0), f.state.load(.acquire));
    f.state.store(1, .release);
    try drain(port);
    try std.testing.expectEqual(@as(usize, 2), f.calls);
    try std.testing.expectEqual(@as(u8, 0), f.state.load(.acquire));
}

test "cleanup job owner yields queued slices and releases rejected admission" {
    const F = struct {
        state: std.atomic.Value(u8) = .init(0),
        queued: ?runtime.Job = null,
        calls: usize = 0,
        rejecting: bool = false,
        fn submit(ptr: *anyopaque, job: runtime.Job) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.rejecting) return error.BackgroundOwnerClosed;
            std.debug.assert(self.queued == null);
            self.queued = job;
        }
        fn noop(_: *anyopaque, _: u64) void {}
        fn poll(_: *anyopaque, _: usize) !usize {
            return 0;
        }
        fn advance(ptr: *anyopaque) !Advance {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            return if (self.calls <= 8) .progressed else .idle;
        }
        fn port(self: *@This()) Port {
            return .{ .ptr = self, .state = &self.state, .lane = .{ .ptr = self, .vtable = &.{ .submit = submit, .drain_owner = noop, .close_owner = noop, .poll = poll } }, .owner_id = 1, .io = null, .advance = advance, .name = "queued-test" };
        }
        fn next(self: *@This()) !void {
            const job = self.queued.?;
            self.queued = null;
            defer job.deinit(job.ptr);
            try job.run(job.ptr);
        }
    };
    var f: F = .{};
    schedule(f.port());
    try f.next();
    try std.testing.expectEqual(@as(usize, 8), f.calls);
    try std.testing.expect(f.queued != null);
    try f.next();
    try std.testing.expect(f.queued == null);
    try std.testing.expectEqual(@as(u8, 0), f.state.load(.acquire));
    f.rejecting = true;
    schedule(f.port());
    try std.testing.expect(f.queued == null);
    try std.testing.expectEqual(@as(u8, 0), f.state.load(.acquire));
}

// The liveness bug this guards against: before this design, exhausting
// the 8-attempt retry budget on a non-inline lane unconditionally
// called `yield`, which resubmitted a *fresh* `drain` run -- its own
// retry counter back at zero -- regardless of whether the error was
// ever going to clear. A persistent `advance` failure (a storage
// invariant violation, say) then looped forever: 8 failed attempts,
// resubmit, 8 more, resubmit, ... A non-inline lane (this fixture's
// `submit` queues rather than runs) is the case that actually exhibits
// it -- an inline lane already returned the error instead of calling
// `schedule` again (see `yield`'s own doc comment), so it never showed
// this bug.
test "cleanup job owner stops resubmitting after a persistent advance failure" {
    const F = struct {
        state: std.atomic.Value(u8) = .init(0),
        queued: ?runtime.Job = null,
        calls: usize = 0,
        gave_up_err: ?anyerror = null,
        gave_up_calls: usize = 0,
        fn submit(ptr: *anyopaque, job: runtime.Job) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            std.debug.assert(self.queued == null);
            self.queued = job;
        }
        fn noop(_: *anyopaque, _: u64) void {}
        fn poll(_: *anyopaque, _: usize) !usize {
            return 0;
        }
        fn advance(ptr: *anyopaque) !Advance {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            return error.InjectedPersistentCleanupFailure;
        }
        fn onGiveUp(ptr: *anyopaque, err: anyerror) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.gave_up_err = err;
            self.gave_up_calls += 1;
        }
        fn port(self: *@This()) Port {
            return .{
                .ptr = self,
                .state = &self.state,
                .lane = .{ .ptr = self, .vtable = &.{ .submit = submit, .drain_owner = noop, .close_owner = noop, .poll = poll } },
                .owner_id = 1,
                .io = std.testing.io,
                .advance = advance,
                .name = "persistent-test",
                .on_give_up = onGiveUp,
            };
        }
        fn next(self: *@This()) void {
            const job = self.queued.?;
            self.queued = null;
            // Mirrors the real `ThreadedDurableJobLane.runEntry`: a job's own
            // returned error is logged and discarded there too, never
            // propagated into the lane's worker loop.
            defer job.deinit(job.ptr);
            job.run(job.ptr) catch {};
        }
    };
    var f: F = .{};
    schedule(f.port());
    try std.testing.expect(f.queued != null);
    f.next(); // runs one `drain`: 8 failed `advance` attempts, then gives up
    try std.testing.expectEqual(@as(usize, 8), f.calls);
    try std.testing.expectEqual(@as(usize, 1), f.gave_up_calls);
    try std.testing.expectEqual(error.InjectedPersistentCleanupFailure, f.gave_up_err.?);
    try std.testing.expectEqual(@as(u8, 0), f.state.load(.acquire));
    // Nothing resubmitted: this is the actual termination property --
    // before the fix, `f.queued` would already hold a freshly requeued
    // `Work` here, and repeating `f.next()` would never stop calling
    // `advance` in groups of 8 forever.
    try std.testing.expect(f.queued == null);

    // A classified-persistent error gives up immediately, without
    // spending the retry budget on backoff that cannot fix it.
    f.calls = 0;
    f.gave_up_calls = 0;
    var classified = f.port();
    classified.persistent = struct {
        fn classify(err: anyerror) bool {
            return err == error.InjectedPersistentCleanupFailure;
        }
    }.classify;
    drain(classified) catch |err| try std.testing.expectEqual(error.InjectedPersistentCleanupFailure, err);
    try std.testing.expectEqual(@as(usize, 1), f.calls);
    try std.testing.expectEqual(@as(usize, 1), f.gave_up_calls);
    try std.testing.expectEqual(@as(u8, 0), f.state.load(.acquire));
    try std.testing.expect(f.queued == null);
}
