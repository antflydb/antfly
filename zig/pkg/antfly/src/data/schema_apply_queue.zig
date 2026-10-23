// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Fixed-key bridge from committed apply to the existing owner control lane.
//! Tickets and retained entry buffers are created/destroyed only by control.
//! No command bytes, allocator, cold open or metadata lookup enters admission.
const std = @import("std");
const sync = @import("antfly_platform").sync;

pub const Key = struct {
    group: u64,
    term: u64,
    index: u64,

    fn validate(self: Key) !void {
        if (self.group == 0 or self.term == 0 or self.index == 0) return error.InvalidOrderedApplyReceipt;
    }
};
pub const capacity = 4;
pub const Step = enum { worked, pending, idle };
const Phase = enum(u8) { empty, queued, preparing, ready, applying, releasing };

fn retryable(err: anyerror) bool {
    return switch (err) {
        error.RaftApplyWriterUnavailable,
        error.StorageBusy,
        error.StorageReadTemporarilyUnavailable,
        error.StorageKernelOwnerTransitionRequired,
        error.SplitSourceProjectionNotReady,
        error.ResourceBudgetExceeded,
        error.OutOfMemory,
        error.RowPolicyReadersActive,
        error.RetainedEffectsFull,
        error.RestoreProjectionCatchUpPending,
        => true,
        else => false,
    };
}

pub fn Queue(comptime Ticket: type) type {
    return struct {
        const Self = @This();
        const Slot = struct {
            phase: std.atomic.Value(Phase) = .init(.empty),
            group: std.atomic.Value(u64) = .init(0),
            canceled: std.atomic.Value(bool) = .init(false),
            retired: std.atomic.Value(bool) = .init(false),
            key: Key = .{ .group = 0, .term = 0, .index = 0 },
            ticket: ?Ticket = null,
            failure: ?anyerror = null,
            retry_at_ns: u64 = 0,
            retry_delay_ns: u64 = 2 * std.time.ns_per_ms,
        };
        mutex: std.atomic.Mutex = .unlocked,
        slots: [capacity]Slot = @splat(.{}),
        cursor: usize = 0,

        /// Single progress lane. A ready ticket is borrowed for one atomic
        /// apply; releasing it is signaled, never performed on this lane.
        pub fn applyOrEnqueue(self: *Self, key: Key) anyerror!void {
            try key.validate();
            if (!self.mutex.tryLock()) return error.RaftApplyWriterUnavailable;
            var held = true;
            defer if (held) self.mutex.unlock();
            var available: ?*Slot = null;
            for (&self.slots) |*slot| {
                const phase = slot.phase.load(.acquire);
                if (phase == .empty) {
                    if (available == null) available = slot;
                    continue;
                }
                if (slot.key.group != key.group) continue;
                if (slot.key.index == key.index and slot.key.term != key.term) return error.ConflictingDataApplyBatch;
                if (!std.meta.eql(slot.key, key)) return error.RaftApplyWriterUnavailable;
                if (slot.canceled.load(.acquire)) return if (slot.retired.load(.acquire)) error.ApplyStoreGroupRetired else error.RaftApplyWriterUnavailable;
                if (phase != .ready) return error.RaftApplyWriterUnavailable;
                if (slot.failure) |err| {
                    slot.phase.store(.releasing, .release);
                    return err;
                }
                slot.phase.store(.applying, .release);
                self.mutex.unlock();
                held = false;
                slot.ticket.?.apply(.{}) catch |err| {
                    if (err == error.SchemaValidationPreparationChanged) {
                        // The former rejection candidate is no longer valid.
                        // Drop it on control, then admit fresh preparation;
                        // retaining it ready would retry forever without data.
                        slot.phase.store(.releasing, .release);
                        return error.RaftApplyWriterUnavailable;
                    }
                    slot.phase.store(if (retryable(err)) .ready else .releasing, .release);
                    return if (retryable(err)) error.RaftApplyWriterUnavailable else err;
                };
                slot.phase.store(.releasing, .release);
                return;
            }
            const slot = available orelse return error.RaftApplyWriterUnavailable;
            slot.key = key;
            slot.failure = null;
            slot.retry_at_ns = 0;
            slot.retry_delay_ns = 2 * std.time.ns_per_ms;
            slot.canceled.store(false, .release);
            slot.retired.store(false, .release);
            slot.group.store(key.group, .release);
            slot.phase.store(.queued, .release);
            return error.RaftApplyWriterUnavailable;
        }

        /// Retirement is a bounded signal, not a wait for an applying ticket.
        /// Called by the same progress lane that admits this group's entries.
        pub fn cancelGroup(self: *Self, group: u64) void {
            for (&self.slots) |*slot| if (slot.group.load(.acquire) == group) {
                slot.retired.store(true, .release);
                slot.canceled.store(true, .release);
            };
        }

        /// Every preparation pins a physical owner, even if its entry is
        /// newer than the incoming snapshot. Release all such pins before
        /// root publication; uncovered entries must prepare on the new root.
        /// This is neither a durable apply watermark nor group retirement.
        pub fn discardForRootReplacement(self: *Self, group: u64) void {
            for (&self.slots) |*slot| if (slot.group.load(.acquire) == group)
                slot.canceled.store(true, .release);
        }

        /// One control quantum, shared with ordinary descriptor opens. Host
        /// preparation owns the exact retained entry and physical-root lease.
        pub fn step(self: *Self, host: anytype, now_ns: u64) Step {
            sync.lockYielding(&self.mutex);
            var pending = false;
            for (0..capacity) |offset| {
                const index = (self.cursor + offset) % capacity;
                const slot = &self.slots[index];
                const phase = slot.phase.load(.acquire);
                if (phase == .empty or phase == .preparing or phase == .applying) continue;
                if (phase == .releasing or slot.canceled.load(.acquire)) {
                    slot.phase.store(.preparing, .release);
                    var ticket = slot.ticket;
                    slot.ticket = null;
                    self.mutex.unlock();
                    if (ticket) |*value| value.deinit();
                    self.reset(slot);
                    self.cursor = (index + 1) % capacity;
                    host.notify();
                    return .worked;
                }
                if (phase != .queued) continue;
                pending = true;
                if (now_ns < slot.retry_at_ns) continue;
                const key = slot.key;
                slot.phase.store(.preparing, .release);
                self.mutex.unlock();
                var prepared: ?Ticket = host.prepare(key, &slot.canceled) catch |err| {
                    sync.lockYielding(&self.mutex);
                    if (retryable(err)) {
                        slot.retry_at_ns = now_ns +| slot.retry_delay_ns;
                        slot.retry_delay_ns = @min(slot.retry_delay_ns *| 2, std.time.ns_per_s);
                        slot.phase.store(.queued, .release);
                    } else {
                        slot.failure = err;
                        slot.phase.store(.ready, .release);
                    }
                    self.mutex.unlock();
                    self.cursor = (index + 1) % capacity;
                    host.notify();
                    return .worked;
                };
                sync.lockYielding(&self.mutex);
                if (slot.canceled.load(.acquire)) {
                    self.mutex.unlock();
                    if (prepared) |*ticket| ticket.deinit();
                    self.reset(slot);
                } else {
                    slot.ticket = prepared;
                    slot.phase.store(.ready, .release);
                    self.mutex.unlock();
                }
                self.cursor = (index + 1) % capacity;
                host.notify();
                return .worked;
            }
            self.mutex.unlock();
            return if (pending) .pending else .idle;
        }

        fn reset(self: *Self, slot: *Slot) void {
            sync.lockYielding(&self.mutex);
            defer self.mutex.unlock();
            slot.failure = null;
            slot.group.store(0, .release);
            slot.phase.store(.empty, .release);
        }

        /// Control-lane timed wait; admission/retirement can wake it early.
        /// Resource pressure must not create a 500 Hz polling loop per job.
        pub fn retryDelayNs(self: *Self, now_ns: u64) u64 {
            sync.lockYielding(&self.mutex);
            defer self.mutex.unlock();
            var delay: u64 = std.time.ns_per_s;
            for (&self.slots) |*slot| {
                if (slot.phase.load(.acquire) == .queued) delay = @min(delay, slot.retry_at_ns -| now_ns);
            }
            return delay;
        }

        /// Control lane after its worker and attached apply calls have drained.
        pub fn deinit(self: *Self) void {
            for (&self.slots) |*slot| {
                const phase = slot.phase.load(.acquire);
                std.debug.assert(phase != .preparing and phase != .applying);
                if (slot.ticket) |*ticket| ticket.deinit();
            }
            self.* = .{};
        }
    };
}

test "schema apply queue bounds keys preserves prepared retries and destroys only on control" {
    const Ticket = struct {
        applied: *usize,
        destroyed: *usize,
        busy: *bool,
        stale: *bool,
        fn apply(self: *@This(), _: anytype) !void {
            if (self.stale.*) return error.SchemaValidationPreparationChanged;
            if (self.busy.*) return error.StorageBusy;
            self.applied.* += 1;
        }
        fn deinit(self: *@This()) void {
            self.destroyed.* += 1;
        }
    };
    const Host = struct {
        applied: usize = 0,
        destroyed: usize = 0,
        prepared: usize = 0,
        busy: bool = false,
        stale: bool = false,
        failure: ?anyerror = null,
        cancel_during_prepare: bool = false,
        queue: ?*Queue(Ticket) = null,
        fn prepare(self: *@This(), key: Key, _: *const std.atomic.Value(bool)) anyerror!Ticket {
            if (self.failure) |err| return err;
            if (self.cancel_during_prepare) self.queue.?.cancelGroup(key.group);
            self.prepared += 1;
            return .{ .applied = &self.applied, .destroyed = &self.destroyed, .busy = &self.busy, .stale = &self.stale };
        }
        fn notify(_: *@This()) void {}
    };
    var queue: Queue(Ticket) = .{};
    defer queue.deinit();
    var host = Host{ .queue = &queue };
    const key: Key = .{ .group = 7, .term = 2, .index = 10 };
    try std.testing.expect(queue.mutex.tryLock());
    try std.testing.expectError(error.RaftApplyWriterUnavailable, queue.applyOrEnqueue(key));
    queue.mutex.unlock();
    try std.testing.expectError(error.InvalidOrderedApplyReceipt, queue.applyOrEnqueue(.{ .group = 0, .term = 2, .index = 10 }));
    try std.testing.expectError(error.RaftApplyWriterUnavailable, queue.applyOrEnqueue(key));
    try std.testing.expectError(error.ConflictingDataApplyBatch, queue.applyOrEnqueue(.{ .group = 7, .term = 3, .index = 10 }));
    try std.testing.expectEqual(Step.worked, queue.step(&host, 0));
    host.busy = true;
    try std.testing.expectError(error.RaftApplyWriterUnavailable, queue.applyOrEnqueue(key));
    try std.testing.expectEqual(@as(usize, 1), host.prepared);
    try std.testing.expectEqual(@as(usize, 0), host.destroyed);
    host.busy = false;
    try queue.applyOrEnqueue(key);
    try std.testing.expectEqual(@as(usize, 1), host.applied);
    try std.testing.expectEqual(@as(usize, 0), host.destroyed);
    try std.testing.expectEqual(Step.worked, queue.step(&host, 0));
    try std.testing.expectEqual(@as(usize, 1), host.destroyed);
    for (0..capacity) |i| try std.testing.expectError(error.RaftApplyWriterUnavailable, queue.applyOrEnqueue(.{ .group = i + 1, .term = 1, .index = 1 }));
    try std.testing.expectError(error.RaftApplyWriterUnavailable, queue.applyOrEnqueue(key));
    for (0..capacity) |i| queue.cancelGroup(i + 1);
    for (0..capacity) |_| try std.testing.expectEqual(Step.worked, queue.step(&host, 0));
    host.failure = error.OutOfMemory;
    try std.testing.expectError(error.RaftApplyWriterUnavailable, queue.applyOrEnqueue(key));
    try std.testing.expectEqual(Step.worked, queue.step(&host, 0));
    try std.testing.expectEqual(Step.pending, queue.step(&host, 0));
    try std.testing.expectEqual(@as(u64, 2 * std.time.ns_per_ms), queue.retryDelayNs(0));
    host.failure = error.NotFound;
    try std.testing.expectEqual(Step.worked, queue.step(&host, 2 * std.time.ns_per_ms));
    try std.testing.expectError(error.NotFound, queue.applyOrEnqueue(key));
    try std.testing.expectEqual(Step.worked, queue.step(&host, 0));
    try std.testing.expectEqual(Step.idle, queue.step(&host, 0));
    host.failure = null;
    try std.testing.expectError(error.RaftApplyWriterUnavailable, queue.applyOrEnqueue(key));
    try std.testing.expectEqual(Step.worked, queue.step(&host, 0));
    queue.cancelGroup(key.group);
    try std.testing.expectError(error.ApplyStoreGroupRetired, queue.applyOrEnqueue(key));
    try std.testing.expectEqual(Step.worked, queue.step(&host, 0));
    try std.testing.expectEqual(@as(usize, 2), host.destroyed);
    host.cancel_during_prepare = true;
    try std.testing.expectError(error.RaftApplyWriterUnavailable, queue.applyOrEnqueue(key));
    try std.testing.expectEqual(Step.worked, queue.step(&host, 0));
    try std.testing.expectEqual(@as(usize, 3), host.destroyed);
    try std.testing.expectEqual(Step.idle, queue.step(&host, 0));
    host.cancel_during_prepare = false;
    try std.testing.expectError(error.RaftApplyWriterUnavailable, queue.applyOrEnqueue(key));
    try std.testing.expectEqual(Step.worked, queue.step(&host, 0));
    queue.deinit(); // control shutdown drains an unapplied ready ticket
    try std.testing.expectEqual(@as(usize, 4), host.destroyed);
    try std.testing.expectEqual(@as(usize, 1), host.applied);
    try std.testing.expectError(error.RaftApplyWriterUnavailable, queue.applyOrEnqueue(key));
    try std.testing.expectEqual(Step.worked, queue.step(&host, 0));
    queue.discardForRootReplacement(key.group + 1);
    try queue.applyOrEnqueue(key); // another group's replacement cannot discard this entry
    try std.testing.expectEqual(Step.worked, queue.step(&host, 0));
    try std.testing.expectError(error.RaftApplyWriterUnavailable, queue.applyOrEnqueue(key));
    try std.testing.expectEqual(Step.worked, queue.step(&host, 0));
    queue.discardForRootReplacement(key.group);
    try std.testing.expectError(error.RaftApplyWriterUnavailable, queue.applyOrEnqueue(key));
    try std.testing.expectEqual(Step.worked, queue.step(&host, 0));
    try std.testing.expectEqual(@as(usize, 6), host.destroyed);
    try std.testing.expectEqual(@as(usize, 2), host.applied);
    try std.testing.expectError(error.RaftApplyWriterUnavailable, queue.applyOrEnqueue(key));
    try std.testing.expectEqual(Step.worked, queue.step(&host, 0));
    queue.discardForRootReplacement(key.group);
    try std.testing.expectError(error.RaftApplyWriterUnavailable, queue.applyOrEnqueue(key));
    try std.testing.expectEqual(Step.worked, queue.step(&host, 0));
    try std.testing.expectEqual(@as(usize, 7), host.destroyed);
    try std.testing.expectEqual(@as(usize, 2), host.applied);
    // Discarding the old root did not retire membership or acknowledge the
    // uncovered entry. The same key must obtain an independent preparation.
    try std.testing.expectError(error.RaftApplyWriterUnavailable, queue.applyOrEnqueue(key));
    try std.testing.expectEqual(Step.worked, queue.step(&host, 0));
    try queue.applyOrEnqueue(key);
    try std.testing.expectEqual(Step.worked, queue.step(&host, 0));
    try std.testing.expectEqual(@as(usize, 8), host.destroyed);
    try std.testing.expectEqual(@as(usize, 3), host.applied);
    host.stale = true;
    try std.testing.expectError(error.RaftApplyWriterUnavailable, queue.applyOrEnqueue(key));
    try std.testing.expectEqual(Step.worked, queue.step(&host, 0));
    try std.testing.expectError(error.RaftApplyWriterUnavailable, queue.applyOrEnqueue(key));
    try std.testing.expectEqual(Step.worked, queue.step(&host, 0));
    try std.testing.expectEqual(@as(usize, 9), host.destroyed);
    host.stale = false;
    try std.testing.expectError(error.RaftApplyWriterUnavailable, queue.applyOrEnqueue(key));
    try std.testing.expectEqual(Step.worked, queue.step(&host, 0));
    try queue.applyOrEnqueue(key);
    try std.testing.expectEqual(Step.worked, queue.step(&host, 0));
    try std.testing.expectEqual(@as(usize, 10), host.destroyed);
    try std.testing.expectEqual(@as(usize, 4), host.applied);
}

test "schema apply queue preparation leaves admission live and snapshot discard drains its ticket" {
    const Ticket = struct {
        applied: *std.atomic.Value(usize),
        destroyed: *std.atomic.Value(usize),
        fn apply(self: *@This(), _: anytype) !void {
            _ = self.applied.fetchAdd(1, .acq_rel);
        }
        fn deinit(self: *@This()) void {
            _ = self.destroyed.fetchAdd(1, .acq_rel);
        }
    };
    const Host = struct {
        queue: *Queue(Ticket),
        io: std.Io,
        entered: std.Io.Event = .unset,
        release: std.Io.Event = .unset,
        finished: std.Io.Event = .unset,
        applied: std.atomic.Value(usize) = .init(0),
        destroyed: std.atomic.Value(usize) = .init(0),
        aborted: std.atomic.Value(bool) = .init(false),
        fn prepare(self: *@This(), _: Key, canceled: *const std.atomic.Value(bool)) !Ticket {
            self.entered.set(self.io);
            try self.release.wait(self.io);
            self.aborted.store(canceled.load(.acquire), .release);
            return .{ .applied = &self.applied, .destroyed = &self.destroyed };
        }
        fn notify(_: *@This()) void {}
        fn run(self: *@This()) void {
            _ = self.queue.step(self, 0);
            self.finished.set(self.io);
        }
    };
    var queue: Queue(Ticket) = .{};
    defer queue.deinit();
    var host: Host = .{ .queue = &queue, .io = std.testing.io };
    const first: Key = .{ .group = 7, .term = 2, .index = 10 };
    const second: Key = .{ .group = 8, .term = 3, .index = 11 };
    try std.testing.expectError(error.RaftApplyWriterUnavailable, queue.applyOrEnqueue(first));
    var task = try host.io.concurrent(Host.run, .{&host});
    defer {
        host.release.set(host.io);
        task.await(host.io);
    }
    try host.entered.waitTimeout(host.io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    try std.testing.expectError(error.RaftApplyWriterUnavailable, queue.applyOrEnqueue(first));
    try std.testing.expectError(error.RaftApplyWriterUnavailable, queue.applyOrEnqueue(second));
    // The snapshot can precede first.index and still replaces the physical
    // root pinned by this preparation. It must not wait for that pin forever.
    queue.discardForRootReplacement(first.group);
    try std.testing.expectError(error.RaftApplyWriterUnavailable, queue.applyOrEnqueue(first));
    host.release.set(host.io);
    try host.finished.waitTimeout(host.io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    try std.testing.expectEqual(@as(usize, 1), host.destroyed.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), host.applied.load(.acquire));
    try std.testing.expect(host.aborted.load(.acquire));
    try std.testing.expectEqual(Step.worked, queue.step(&host, 0));
    try queue.applyOrEnqueue(second);
    try std.testing.expectEqual(Step.worked, queue.step(&host, 0));
    try std.testing.expectEqual(@as(usize, 2), host.destroyed.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), host.applied.load(.acquire));
}
