// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Elastic-2.0
//! External publication scheduling. Storage provides bounded durable facts;
//! this owner chooses retry cadence and fairness and retains admission state.
const std = @import("std");
const publication = @import("db/artifact_publication.zig");
const transport = @import("db/artifact_publication_transport.zig");

pub const Scheduler = struct {
    pub const poll_interval_ns: u64 = 5 * std.time.ns_per_s;
    cursor: std.atomic.Value(u64) = .init(0),
    mutex: std.Io.Mutex = .init,
    tracker: RecoveryTracker = .{},
    retry_after_ns: std.atomic.Value(u64) = .init(0),

    /// Check cadence before storage opens a snapshot or scans upload metadata.
    pub fn shouldPoll(self: *Scheduler, tick: publication.UploadRecoveryTick) bool {
        if (tick.trigger == .explicit) return true;
        const previous = self.retry_after_ns.load(.acquire);
        if (tick.now_ns < previous) return false;
        return self.retry_after_ns.cmpxchgStrong(previous, tick.now_ns +| poll_interval_ns, .acq_rel, .acquire) == null;
    }

    pub fn advance(self: *Scheduler, dispatcher: publication.Dispatcher, invocation: publication.UploadRecoveryInvocation) !bool {
        const hint = blk: {
            self.mutex.lockUncancelable(invocation.io);
            defer self.mutex.unlock(invocation.io);
            break :blk self.tracker.observe(invocation.inventory, invocation.now_ns, self.cursor.load(.acquire)) orelse return false;
        };
        // Never hold the scheduler mutex across a possibly blocking admission.
        const encoded = hint.encode();
        try dispatcher.enqueue(dispatcher.ptr, hint.namespace, &encoded);
        self.cursor.store(hint.created_index, .release);
        return true;
    }
};

test "artifact upload recovery owner retains refused cursor and separates explicit retry from cadence" {
    const Capture = struct {
        scheduler: Scheduler = .{},
        refused: bool = true,
        calls: usize = 0,
        fn enqueue(ptr: *anyopaque, _: publication.Namespace, _: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            // An admission callback must be able to acquire the scheduler lock.
            try std.testing.expect(self.scheduler.mutex.tryLock());
            self.scheduler.mutex.unlock(std.testing.io);
            self.calls += 1;
            if (self.refused) return error.ResourceLimitExceeded;
        }
    };
    var capture: Capture = .{};
    const dispatcher: publication.Dispatcher = .{ .ptr = &capture, .enqueue = Capture.enqueue };
    var invocation: publication.UploadRecoveryInvocation = .{
        .io = std.testing.io,
        .now_ns = 10,
        .trigger = .maintenance,
        .inventory = .{},
    };
    invocation.inventory.entries[0] = .{
        .hint = .{ .namespace = @splat(1), .publication_digest = @splat(2), .root = @splat(3), .created_index = 4 },
        .complete = true,
        .progress = @splat(0),
    };
    invocation.inventory.count = 1;
    try std.testing.expect(capture.scheduler.shouldPoll(.{ .now_ns = invocation.now_ns, .trigger = .maintenance }));
    try std.testing.expectError(error.ResourceLimitExceeded, capture.scheduler.advance(dispatcher, invocation));
    try std.testing.expectEqual(@as(u64, 0), capture.scheduler.cursor.load(.acquire));
    capture.refused = false;
    try std.testing.expect(!capture.scheduler.shouldPoll(.{ .now_ns = invocation.now_ns, .trigger = .maintenance }));
    try std.testing.expectEqual(@as(usize, 1), capture.calls);
    invocation.trigger = .explicit;
    try std.testing.expect(capture.scheduler.shouldPoll(.{ .now_ns = invocation.now_ns, .trigger = .explicit }));
    try std.testing.expect(try capture.scheduler.advance(dispatcher, invocation));
    try std.testing.expectEqual(@as(u64, 4), capture.scheduler.cursor.load(.acquire));
    invocation.trigger = .maintenance;
    invocation.now_ns += Scheduler.poll_interval_ns;
    try std.testing.expect(capture.scheduler.shouldPoll(.{ .now_ns = invocation.now_ns, .trigger = .maintenance }));
    try std.testing.expect(try capture.scheduler.advance(dispatcher, invocation));
    try std.testing.expectEqual(@as(usize, 3), capture.calls);
}

const RecoveryInventory = transport.RecoveryInventory;
const RecoveryHint = transport.RecoveryHint;
const Digest = publication.Digest;
const max_active_uploads = transport.max_active_uploads;

/// Bounded local failure detector. Clocks select proposals only: replicated
/// retirement checks the exact incarnation/progress in its writer snapshot.
/// Restart, clock regression, or newly observed progress starts a fresh grace
/// period. A pending ready upload cannot starve retirement of an idle one.
pub const RecoveryTracker = struct {
    pub const idle_ns: u64 = 5 * std.time.ns_per_min;
    const Slot = struct { identity: RecoveryHint, progress: Digest, since: u64 };
    slots: [max_active_uploads]?Slot = @splat(null),
    prefer_abandon: bool = false,

    pub fn observe(self: *RecoveryTracker, inventory: RecoveryInventory, now: u64, after: u64) ?RecoveryHint {
        var current: [max_active_uploads]?Slot = @splat(null);
        var first: ?RecoveryHint = null;
        var next: ?RecoveryHint = null;
        for (inventory.entries[0..inventory.count], 0..) |entry, i| {
            if (entry.complete) continue;
            var since = now;
            for (self.slots) |maybe| if (maybe) |previous| {
                if (std.meta.eql(previous.identity, entry.hint) and std.mem.eql(u8, &previous.progress, &entry.progress) and now >= previous.since) {
                    since = previous.since;
                    break;
                }
            };
            current[i] = .{ .identity = entry.hint, .progress = entry.progress, .since = since };
            if (now - since < idle_ns) continue;
            var hint = entry.hint;
            hint.action = .abandon;
            hint.observed_progress = entry.progress;
            if (first == null or hint.created_index < first.?.created_index) first = hint;
            if (hint.created_index > after and (next == null or hint.created_index < next.?.created_index)) next = hint;
        }
        self.slots = current;
        const abandon = next orelse first;
        const ready = inventory.nextReady(after);
        const result = if (self.prefer_abandon) abandon orelse ready else ready orelse abandon;
        if (result) |hint| self.prefer_abandon = hint.action == .finalize;
        return result;
    }
};

test "artifact publication upload idle detector resets on progress and inventory changes" {
    var inventory: RecoveryInventory = .{};
    inventory.count = 1;
    inventory.entries[0] = .{ .hint = .{ .namespace = @splat(1), .publication_digest = @splat(2), .root = @splat(3), .created_index = 1 }, .complete = false, .progress = @splat(4) };
    var tracker: RecoveryTracker = .{};
    try std.testing.expect(tracker.observe(inventory, 0, 0) == null);
    try std.testing.expect(tracker.observe(inventory, RecoveryTracker.idle_ns - 1, 0) == null);
    inventory.entries[0].progress = @splat(5);
    try std.testing.expect(tracker.observe(inventory, RecoveryTracker.idle_ns, 0) == null);
    try std.testing.expect(tracker.observe(inventory, RecoveryTracker.idle_ns * 2 - 1, 0) == null);
    const abandoned = tracker.observe(inventory, RecoveryTracker.idle_ns * 2, 0).?;
    try std.testing.expectEqual(.abandon, abandoned.action);
    try std.testing.expectEqualDeep(inventory.entries[0].progress, abandoned.observed_progress);
    try std.testing.expect(tracker.observe(.{}, RecoveryTracker.idle_ns * 2, 0) == null);
    try std.testing.expect(tracker.observe(inventory, RecoveryTracker.idle_ns * 3, 0) == null);
    inventory.entries[0].complete = true;
    try std.testing.expectEqual(.finalize, tracker.observe(inventory, RecoveryTracker.idle_ns * 3, 0).?.action);
}

test "artifact upload recovery alternates ready and idle entries and resets on clock regression and reincarnation" {
    var inventory: RecoveryInventory = .{};
    inventory.count = 3;
    for (0..3) |i| inventory.entries[i] = .{
        .hint = .{ .namespace = @splat(1), .publication_digest = @splat(@intCast(i + 2)), .root = @splat(3), .created_index = 20_000 + i },
        .complete = i != 1,
        .progress = @splat(4),
    };
    var tracker: RecoveryTracker = .{};
    try std.testing.expectEqual(@as(u64, 20_000), tracker.observe(inventory, 0, 0).?.created_index);
    try std.testing.expectEqual(.finalize, tracker.observe(inventory, RecoveryTracker.idle_ns - 1, 0).?.action);
    const abandoned = tracker.observe(inventory, RecoveryTracker.idle_ns, 0).?;
    try std.testing.expectEqual(.abandon, abandoned.action);
    try std.testing.expectEqual(@as(u64, 20_001), abandoned.created_index);
    try std.testing.expectEqual(@as(u64, 20_002), tracker.observe(inventory, RecoveryTracker.idle_ns, abandoned.created_index).?.created_index);
    try std.testing.expectEqual(.abandon, tracker.observe(inventory, RecoveryTracker.idle_ns, 20_002).?.action);
    // Reusing the digest at a new durable incarnation starts a fresh grace.
    inventory.entries[1].hint.created_index = 21_000;
    try std.testing.expectEqual(.finalize, tracker.observe(inventory, RecoveryTracker.idle_ns * 2, 0).?.action);
    try std.testing.expectEqual(.finalize, tracker.observe(inventory, 0, 0).?.action);
    const replacement = tracker.observe(inventory, RecoveryTracker.idle_ns, 0).?;
    try std.testing.expectEqual(.abandon, replacement.action);
    try std.testing.expectEqual(@as(u64, 21_000), replacement.created_index);
}
