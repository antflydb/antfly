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
const index_manager_mod = @import("catalog/index_manager.zig");
const snapshot_admission_mod = @import("snapshot_admission.zig");
const derived_executor_mod = @import("derived/derived_executor.zig");
const AtomicU64 = @import("antfly_platform").atomic.Value(u64);

pub const Session = struct {
    index_name: []u8,
    index_incarnation: u64,
    lease: ?index_manager_mod.IndexManager.DensePostingCaptureLease,
    /// Covers the complete derived transaction, including native WAL commit,
    /// immutable-generation publication, and lifecycle checkpointing. A
    /// snapshot capture must never observe the replay apply as drained while
    /// its durable query generation is still being published.
    snapshot_replay: ?snapshot_admission_mod.SnapshotAdmission.MutationLease,
};

/// Owns token identity, capture/replay admission and their transfer to finishing
/// callbacks. Catalog validation and durable generation publication stay with DB.
/// Tracking transitions require the caller's dense-finish admission fence.
/// All callbacks must drain before deinit; an independently retained replay
/// lease continues protecting its transaction after its token is removed.
pub const Owner = struct {
    mutex: std.atomic.Mutex = .unlocked,
    nonce: AtomicU64 = .init(0),
    sessions: std.AutoHashMapUnmanaged(u64, Session) = .empty,
    active: std.atomic.Value(u32) = .init(0),
    fn lock(self: *Owner) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    pub fn beginTracking(self: *Owner) void {
        _ = self.active.fetchAdd(1, .release);
    }
    pub fn finishTracking(self: *Owner) ?u32 {
        const active = self.active.load(.acquire);
        if (active == 0) return null;
        self.active.store(active - 1, .release);
        return active - 1;
    }
    pub fn deinit(self: *Owner, alloc: std.mem.Allocator, manager: *index_manager_mod.IndexManager) void {
        var it = self.sessions.valueIterator();
        while (it.next()) |session| {
            if (session.lease) |lease| if (lease.ownsLifecycle()) {
                manager.cancelDensePostingSidecarCaptureLeaseByName(session.index_name, lease) catch {};
            };
            if (session.snapshot_replay) |*lease| lease.release();
            alloc.free(session.index_name);
        }
        self.sessions.deinit(alloc);
        self.* = .{};
    }
    pub fn install(
        self: *Owner,
        alloc: std.mem.Allocator,
        index_name: []const u8,
        index_incarnation: u64,
        lease: ?index_manager_mod.IndexManager.DensePostingCaptureLease,
        snapshot_replay: *?snapshot_admission_mod.SnapshotAdmission.MutationLease,
    ) !derived_executor_mod.CatchUpSessionToken {
        if (lease) |value| if (!value.ownsLifecycle()) return error.PostingWalCaptureOwnershipConflict;
        const owned_name = try alloc.dupe(u8, index_name);
        errdefer alloc.free(owned_name);
        var observed = self.nonce.load(.monotonic);
        const session_id = while (true) {
            if (observed == std.math.maxInt(u64)) return error.DenseCatchUpSessionTokenExhausted;
            const candidate = observed + 1;
            if (self.nonce.cmpxchgWeak(observed, candidate, .monotonic, .monotonic)) |raced| {
                observed = raced;
                continue;
            }
            break candidate;
        };
        self.lock();
        defer self.mutex.unlock();
        const entry = try self.sessions.getOrPut(alloc, session_id);
        if (entry.found_existing) return error.DenseCatchUpSessionTokenExhausted;
        entry.value_ptr.* = .{
            .index_name = owned_name,
            .index_incarnation = index_incarnation,
            .lease = lease,
            .snapshot_replay = snapshot_replay.*,
        };
        snapshot_replay.* = null;
        return .{ .value = session_id };
    }

    // The map mutex protects the transfer from the session's lease to an
    // independently retained batch lease. Session close cannot retire admission
    // underneath an in-flight callback, and stale tokens cannot borrow a new one.
    pub fn retainAdmission(
        self: *Owner,
        index_name: []const u8,
        token: derived_executor_mod.CatchUpSessionToken,
    ) !?snapshot_admission_mod.SnapshotAdmission.MutationLease {
        if (token.isNone()) return error.DenseCatchUpSessionSuperseded;
        self.lock();
        defer self.mutex.unlock();
        const session = self.sessions.getPtr(token.value) orelse return error.DenseCatchUpSessionSuperseded;
        if (!std.mem.eql(u8, session.index_name, index_name)) return error.DenseCatchUpSessionSuperseded;
        return if (session.snapshot_replay) |*lease| lease.retain() else null;
    }

    pub fn take(
        self: *Owner,
        index_name: []const u8,
        token: derived_executor_mod.CatchUpSessionToken,
    ) !Session {
        if (token.isNone()) return error.DenseCatchUpSessionSuperseded;
        self.lock();
        defer self.mutex.unlock();
        const current = self.sessions.get(token.value) orelse return error.DenseCatchUpSessionSuperseded;
        if (!std.mem.eql(u8, current.index_name, index_name)) return error.DenseCatchUpSessionSuperseded;
        const removed = self.sessions.fetchRemove(token.value) orelse unreachable;
        return removed.value;
    }
};

test "dense session owner transfers admission only after every allocation succeeds" {
    const Check = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var owner: Owner = .{};
            defer owner.deinit(alloc, undefined);
            var admission: snapshot_admission_mod.SnapshotAdmission = .{};
            var mutation: ?snapshot_admission_mod.SnapshotAdmission.MutationLease = admission.acquireMutation();
            defer if (mutation) |*lease| lease.release();
            const token = try owner.install(alloc, "idx", 7, null, &mutation);
            try std.testing.expect(mutation == null);
            var retained = (try owner.retainAdmission("idx", token)).?;
            defer retained.release();
            var session = try owner.take("idx", token);
            defer alloc.free(session.index_name);
            defer if (session.snapshot_replay) |*lease| lease.release();
            try std.testing.expectEqual(@as(u64, 7), session.index_incarnation);
            try std.testing.expectError(error.DenseCatchUpSessionSuperseded, owner.take("idx", token));
            try std.testing.expect(!admission.lock.tryLockExclusive());
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}

test "dense session owner fences exhausted and mismatched tokens without losing leases" {
    const alloc = std.testing.allocator;
    var owner: Owner = .{};
    defer owner.deinit(alloc, undefined);
    var admission: snapshot_admission_mod.SnapshotAdmission = .{};
    var mutation: ?snapshot_admission_mod.SnapshotAdmission.MutationLease = admission.acquireMutation();
    defer if (mutation) |*lease| lease.release();
    owner.nonce.store(std.math.maxInt(u64), .monotonic);
    try std.testing.expectError(error.DenseCatchUpSessionTokenExhausted, owner.install(alloc, "idx", 7, null, &mutation));
    try std.testing.expect(mutation != null);
    try std.testing.expectEqual(@as(u32, 0), owner.sessions.count());
    owner.nonce.store(0, .monotonic);
    const token = try owner.install(alloc, "idx", 7, null, &mutation);
    try std.testing.expectError(error.DenseCatchUpSessionSuperseded, owner.take("other", token));
    try std.testing.expectError(error.DenseCatchUpSessionSuperseded, owner.retainAdmission("other", token));
    try std.testing.expectEqual(@as(u32, 1), owner.sessions.count());
    owner.deinit(alloc, undefined);
    try std.testing.expect(admission.lock.tryLockExclusive());
    admission.lock.unlockExclusive();
}

test "dense session owner retires tracking exactly once under the caller admission fence" {
    var owner: Owner = .{};
    try std.testing.expect(owner.finishTracking() == null);
    owner.beginTracking();
    owner.beginTracking();
    try std.testing.expectEqual(@as(?u32, 1), owner.finishTracking());
    try std.testing.expectEqual(@as(?u32, 0), owner.finishTracking());
    try std.testing.expect(owner.finishTracking() == null);
}
