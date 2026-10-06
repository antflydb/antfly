// Copyright 2026 Antfly, Inc.
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

const std = @import("std");
const sessions = @import("dense_catch_up_session_owner.zig");
const manager = @import("catalog/index_manager.zig").IndexManager;
const AtomicU64 = @import("antfly_platform").atomic.Value(u64);

/// Local replay/publication admission. Locked transitions require mutex;
/// optimistic generation construction and durable checkpointing stay outside.
/// The coordinator drains callbacks before deinit while the manager is alive.
pub const Owner = struct {
    mutex: std.atomic.Mutex = .unlocked,
    sessions: sessions.Owner = .{},
    external_sessions: std.atomic.Value(u32) = .init(0),
    waiters: std.atomic.Value(u32) = .init(0),
    finalizing: std.atomic.Value(bool) = .init(false),
    committing: std.atomic.Value(bool) = .init(false),
    finalization_requested: bool = false,
    pending: std.StringHashMapUnmanaged(u64) = .empty,
    deferred_sequence: AtomicU64 = .init(0),
    pub fn beginReplayLocked(self: *Owner) !void {
        if (self.external_sessions.load(.acquire) != 0 or self.waiters.load(.acquire) != 0 or self.committing.load(.acquire)) return error.ReplayDocumentNotVisible;
        self.sessions.beginTracking();
    }
    pub fn finishReplayLocked(self: *Owner) ?u32 {
        return self.sessions.finishTracking();
    }
    pub fn beginExternalLocked(self: *Owner) !void {
        if (self.sessions.active.load(.acquire) != 0 or self.committing.load(.acquire)) return error.ReplayDocumentNotVisible;
        _ = self.external_sessions.fetchAdd(1, .release);
    }
    pub fn beginWaitLocked(self: *Owner) void {
        _ = self.waiters.fetchAdd(1, .release);
    }
    pub fn cancelWaitLocked(self: *Owner) void {
        std.debug.assert(self.waiters.load(.acquire) != 0);
        _ = self.waiters.fetchSub(1, .release);
    }
    pub fn admitWaiterLocked(self: *Owner) bool {
        self.beginExternalLocked() catch return false;
        self.cancelWaitLocked();
        return true;
    }
    pub fn finishExternalLocked(self: *Owner) bool {
        const active = self.external_sessions.load(.acquire);
        if (active == 0) return false;
        self.external_sessions.store(active - 1, .release);
        return true;
    }
    pub fn deferSequence(self: *Owner, sequence: u64) void {
        var previous = self.deferred_sequence.load(.acquire);
        while (previous < sequence) {
            previous = self.deferred_sequence.cmpxchgWeak(previous, sequence, .acq_rel, .acquire) orelse return;
        }
    }
    pub fn takeDeferredSequence(self: *Owner) u64 {
        return self.deferred_sequence.swap(0, .acq_rel);
    }
    pub fn deinit(self: *Owner, alloc: std.mem.Allocator, index_manager: *manager) void {
        self.sessions.deinit(alloc, index_manager);
        var keys = self.pending.keyIterator();
        while (keys.next()) |key| alloc.free(@constCast(key.*));
        self.pending.deinit(alloc);
    }
    pub fn hasSessionsOrWaiters(self: *const Owner) bool {
        return self.sessions.active.load(.acquire) != 0 or
            self.external_sessions.load(.acquire) != 0 or
            self.waiters.load(.acquire) != 0;
    }
    pub fn claimLocked(self: *Owner) bool {
        if (self.hasSessionsOrWaiters() or
            self.finalizing.load(.acquire)) return false;
        self.finalization_requested = false;
        self.finalizing.store(true, .release);
        return true;
    }
    pub fn claimOrRequestLocked(self: *Owner) bool {
        if (self.claimLocked()) return true;
        if (self.finalizing.load(.acquire)) {
            self.finalization_requested = true;
        }
        return false;
    }
    pub fn finishFinalizationLocked(self: *Owner) bool {
        const requested = self.finalization_requested;
        self.finalization_requested = false;
        self.finalizing.store(false, .release);
        return requested;
    }
    /// Retain the current claim when a source completion requested another pass.
    pub fn completeFinalizationPassLocked(self: *Owner) bool {
        if (self.finalization_requested) {
            self.finalization_requested = false;
            return true;
        }
        self.finalizing.store(false, .release);
        return false;
    }
    pub fn requestFinalizationLocked(self: *Owner) void {
        self.finalization_requested = true;
    }
    pub fn pendingCheckpointSequenceLocked(self: *const Owner, name: []const u8) u64 {
        return self.pending.get(name) orelse 0;
    }
    /// The coordinator supplies catalog eligibility while this owner retires
    /// obsolete hints and their names. The predicate must not mutate this map.
    pub fn pruneCheckpointsLocked(self: *Owner, alloc: std.mem.Allocator, context: anytype, comptime keep: anytype) void {
        var entries = self.pending.iterator();
        while (entries.next()) |entry| {
            if (keep(context, entry.key_ptr.*)) continue;
            const name = entry.key_ptr.*;
            self.pending.removeByPtr(entry.key_ptr);
            alloc.free(@constCast(name));
        }
    }
    pub fn beginCommitLocked(self: *Owner) bool {
        if (self.hasSessionsOrWaiters() or self.committing.load(.acquire)) return false;
        self.committing.store(true, .release);
        return true;
    }
    pub fn finishCommitLocked(self: *Owner) void {
        self.committing.store(false, .release);
    }
    pub fn deferCheckpointLocked(self: *Owner, alloc: std.mem.Allocator, name: []const u8, sequence: u64) !void {
        if (self.pending.getPtr(name)) |pending_sequence| {
            pending_sequence.* = @max(pending_sequence.*, sequence);
            return;
        }
        const owned = try alloc.dupe(u8, name);
        errdefer alloc.free(owned);
        try self.pending.putNoClobber(alloc, owned, sequence);
    }
    pub fn clearCheckpointLocked(self: *Owner, alloc: std.mem.Allocator, name: []const u8) void {
        if (self.pending.fetchRemove(name)) |removed| alloc.free(@constCast(removed.key));
    }
};

test "dense publication admission fences waiters and retains source completion handoff" {
    var owner: Owner = .{};
    defer owner.deinit(std.testing.allocator, undefined);
    owner.waiters.store(1, .release);
    try std.testing.expect(!owner.claimLocked());
    try std.testing.expect(!owner.beginCommitLocked());
    owner.waiters.store(0, .release);
    try std.testing.expect(owner.claimLocked());
    try std.testing.expect(!owner.claimOrRequestLocked());
    try std.testing.expect(owner.finishFinalizationLocked());
    try std.testing.expect(owner.claimLocked());
    try std.testing.expect(!owner.finishFinalizationLocked());
    try std.testing.expect(owner.beginCommitLocked());
    try std.testing.expect(!owner.beginCommitLocked());
    owner.finishCommitLocked();
    try std.testing.expect(owner.beginCommitLocked());
    owner.finishCommitLocked();
}

test "dense publication admission owns checkpoint names and monotonic deferred sequences" {
    const Check = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var owner: Owner = .{};
            defer owner.deinit(alloc, undefined);
            var name = [_]u8{ 'i', 'd', 'x' };
            try owner.deferCheckpointLocked(alloc, &name, 7);
            name[0] = 'x';
            try owner.deferCheckpointLocked(alloc, "idx", 3);
            try std.testing.expectEqual(@as(u64, 7), owner.pending.get("idx").?);
            owner.clearCheckpointLocked(alloc, "idx");
            try std.testing.expectEqual(@as(u32, 0), owner.pending.count());
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}

test "dense publication admission releases cancelled waiters and fences replay against commit" {
    var owner: Owner = .{};
    defer owner.deinit(std.testing.allocator, undefined);
    try owner.beginReplayLocked();
    owner.beginWaitLocked();
    try std.testing.expect(!owner.admitWaiterLocked());
    try std.testing.expectEqual(@as(?u32, 0), owner.finishReplayLocked());
    try std.testing.expect(owner.admitWaiterLocked());
    try std.testing.expectError(error.ReplayDocumentNotVisible, owner.beginReplayLocked());
    try std.testing.expect(owner.finishExternalLocked());
    try std.testing.expect(!owner.finishExternalLocked());
    owner.beginWaitLocked();
    owner.cancelWaitLocked();
    try std.testing.expect(owner.beginCommitLocked());
    try std.testing.expectError(error.ReplayDocumentNotVisible, owner.beginReplayLocked());
    try std.testing.expectError(error.ReplayDocumentNotVisible, owner.beginExternalLocked());
    owner.finishCommitLocked();
    try owner.beginReplayLocked();
    try std.testing.expectEqual(@as(?u32, 0), owner.finishReplayLocked());
    owner.deferSequence(7);
    owner.deferSequence(3);
    try std.testing.expectEqual(@as(u64, 7), owner.takeDeferredSequence());
    try std.testing.expectEqual(@as(u64, 0), owner.takeDeferredSequence());
}

test "dense publication admission retains finalization claims and retires only obsolete checkpoint hints" {
    const Check = struct {
        fn keep(_: void, name: []const u8) bool {
            return std.mem.eql(u8, name, "live");
        }
        fn run(alloc: std.mem.Allocator) !void {
            var owner: Owner = .{};
            defer owner.deinit(alloc, undefined);
            try owner.deferCheckpointLocked(alloc, "removed", 3);
            try owner.deferCheckpointLocked(alloc, "live", 7);
            try owner.deferCheckpointLocked(alloc, "completed", 9);
            owner.pruneCheckpointsLocked(alloc, {}, keep);
            try std.testing.expectEqual(@as(u32, 1), owner.pending.count());
            try std.testing.expectEqual(@as(u64, 7), owner.pendingCheckpointSequenceLocked("live"));
            try std.testing.expectEqual(@as(u64, 0), owner.pendingCheckpointSequenceLocked("removed"));
            try std.testing.expect(owner.claimLocked());
            owner.requestFinalizationLocked();
            try std.testing.expect(owner.completeFinalizationPassLocked());
            try std.testing.expect(owner.finalizing.load(.acquire));
            try std.testing.expect(!owner.completeFinalizationPassLocked());
            try std.testing.expect(!owner.finalizing.load(.acquire));
            // A later source completion can acquire a fresh claim.
            try std.testing.expect(owner.claimLocked());
            try std.testing.expect(!owner.finishFinalizationLocked());
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}
