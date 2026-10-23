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

const ReservedCleanup = @import("antfly_runtime_abi").runnable_abi.ReservedCleanup;

pub const Session = struct {
    cleanup: ?ReservedCleanup = null,
    index_name: []u8,
    index_incarnation: u64,
    lease: ?index_manager_mod.IndexManager.DensePostingCaptureLease,
    /// Covers the complete derived transaction, including native WAL commit,
    /// immutable-generation publication, and lifecycle checkpointing. A
    /// snapshot capture must never observe the replay apply as drained while
    /// its durable query generation is still being published.
    snapshot_replay: ?snapshot_admission_mod.SnapshotAdmission.MutationLease,
};

// Stable metadata lets a rejected synchronous claim release without waiting
// for the registry mutex. The map may rehash while a claim lends CPU.
const StoredSession = struct {
    session: Session,
    claimed: std.atomic.Value(bool) = .init(false),
    allocator: std.mem.Allocator,
};
pub const session_metadata_bytes = @sizeOf(StoredSession);

/// Owns token identity, capture/replay admission and their transfer to finishing
/// callbacks. Catalog validation and durable generation publication stay with DB.
/// Tracking transitions require the caller's dense-finish admission fence.
/// All callbacks must drain before deinit; an independently retained replay
/// lease continues protecting its transaction after its token is removed.
pub const Owner = struct {
    mutex: std.atomic.Mutex = .unlocked,
    nonce: AtomicU64 = .init(0),
    sessions: std.AutoHashMapUnmanaged(u64, *StoredSession) = .empty,
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
        while (it.next()) |slot| {
            const stored = slot.*;
            std.debug.assert(!stored.claimed.load(.acquire));
            const session = &stored.session;
            if (session.lease) |lease| if (lease.ownsLifecycle()) {
                manager.cancelDensePostingSidecarCaptureLeaseByName(session.index_name, lease) catch {};
            };
            if (session.snapshot_replay) |*lease| lease.release();
            alloc.free(session.index_name);
            if (session.cleanup) |*cleanup| cleanup.deinit();
            stored.allocator.destroy(stored);
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
        var cleanup: ?ReservedCleanup = null;
        return self.installWithCleanup(alloc, index_name, index_incarnation, lease, snapshot_replay, &cleanup);
    }

    pub fn installWithCleanup(
        self: *Owner,
        alloc: std.mem.Allocator,
        index_name: []const u8,
        index_incarnation: u64,
        lease: ?index_manager_mod.IndexManager.DensePostingCaptureLease,
        snapshot_replay: *?snapshot_admission_mod.SnapshotAdmission.MutationLease,
        cleanup: *?ReservedCleanup,
    ) !derived_executor_mod.CatchUpSessionToken {
        if (lease) |value| if (!value.ownsLifecycle()) return error.PostingWalCaptureOwnershipConflict;
        const owned_name = try alloc.dupe(u8, index_name);
        errdefer alloc.free(owned_name);
        const stored = try alloc.create(StoredSession);
        errdefer alloc.destroy(stored);
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
        stored.* = .{ .allocator = alloc, .session = .{
            .index_name = owned_name,
            .index_incarnation = index_incarnation,
            .lease = lease,
            .snapshot_replay = snapshot_replay.*,
            .cleanup = cleanup.*,
        } };
        entry.value_ptr.* = stored;
        snapshot_replay.* = null;
        cleanup.* = null;
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
        const stored = self.sessions.get(token.value) orelse return error.DenseCatchUpSessionSuperseded;
        if (!std.mem.eql(u8, stored.session.index_name, index_name)) return error.DenseCatchUpSessionSuperseded;
        if (stored.claimed.load(.acquire)) return error.ResourceTemporarilyUnavailable;
        return if (stored.session.snapshot_replay) |*lease| lease.retain() else null;
    }

    pub fn take(
        self: *Owner,
        index_name: []const u8,
        token: derived_executor_mod.CatchUpSessionToken,
    ) !Session {
        if (token.isNone()) return error.DenseCatchUpSessionSuperseded;
        self.lock();
        defer self.mutex.unlock();
        return self.takeLocked(index_name, token);
    }

    /// No wait while an index guard is held. A busy registry leaves both the
    /// session and its leases owned here until the caller drops all guards.
    pub fn tryTake(self: *Owner, index_name: []const u8, token: derived_executor_mod.CatchUpSessionToken) !?Session {
        if (token.isNone()) return error.DenseCatchUpSessionSuperseded;
        if (!self.mutex.tryLock()) return null;
        defer self.mutex.unlock();
        const current = self.sessions.get(token.value) orelse return error.DenseCatchUpSessionSuperseded;
        if (!std.mem.eql(u8, current.session.index_name, index_name)) return error.DenseCatchUpSessionSuperseded;
        if (current.claimed.load(.acquire)) return null;
        return try self.takeLocked(index_name, token);
    }

    /// Acquire before any catalog/apply guard. A claim owns no map-value
    /// pointer: concurrent installs may rehash while the caller lends CPU.
    /// Its exact session retains capture, snapshot and runnable ownership.
    pub fn tryClaimFinish(self: *Owner, index_name: []const u8, token: derived_executor_mod.CatchUpSessionToken) !?FinishClaim {
        if (token.isNone()) return error.DenseCatchUpSessionSuperseded;
        if (!self.mutex.tryLock()) return null;
        defer self.mutex.unlock();
        const current = self.sessions.get(token.value) orelse return error.DenseCatchUpSessionSuperseded;
        if (!std.mem.eql(u8, current.session.index_name, index_name)) return error.DenseCatchUpSessionSuperseded;
        if (current.claimed.load(.acquire)) return null;
        current.claimed.store(true, .release);
        return .{ .owner = self, .token = token, .stored = current };
    }

    fn takeLocked(self: *Owner, index_name: []const u8, token: derived_executor_mod.CatchUpSessionToken) !Session {
        const current = self.sessions.get(token.value) orelse return error.DenseCatchUpSessionSuperseded;
        if (!std.mem.eql(u8, current.session.index_name, index_name)) return error.DenseCatchUpSessionSuperseded;
        if (current.claimed.load(.acquire)) return error.ResourceTemporarilyUnavailable;
        const removed = self.sessions.fetchRemove(token.value) orelse unreachable;
        const value = removed.value.session;
        removed.value.allocator.destroy(removed.value);
        return value;
    }
};

/// Move-only synchronous preparation ownership. Release before draining Owner.
/// Rejection restores the same session without allocation or a renewed budget;
/// only take transfers physical ownership and consumes the token.
pub const FinishClaim = struct {
    owner: ?*Owner,
    token: derived_executor_mod.CatchUpSessionToken,
    stored: *StoredSession,

    /// Synchronous borrow protected by this exclusive claim. Never retain the
    /// pointer across tryTake: that operation frees the registry metadata.
    pub fn reservation(self: *FinishClaim) !?*ReservedCleanup {
        if (self.owner == null) return error.DenseCatchUpSessionSuperseded;
        return if (self.stored.session.cleanup) |*cleanup| cleanup else null;
    }
    pub fn deinit(self: *FinishClaim) void {
        if (self.owner == null) return;
        self.owner = null;
        // Last access to stored: another caller may remove and free it as
        // soon as this release is observed under the registry mutex.
        self.stored.claimed.store(false, .release);
    }

    /// Called with the prepared apply guard held. Never wait for the map here.
    pub fn tryTake(self: *FinishClaim) !?Session {
        const owner = self.owner orelse return error.DenseCatchUpSessionSuperseded;
        if (!owner.mutex.tryLock()) return null;
        defer owner.mutex.unlock();
        const current = owner.sessions.get(self.token.value) orelse return error.DenseCatchUpSessionSuperseded;
        std.debug.assert(current == self.stored and current.claimed.load(.acquire));
        const removed = owner.sessions.fetchRemove(self.token.value) orelse unreachable;
        self.owner = null;
        const value = removed.value.session;
        removed.value.allocator.destroy(removed.value);
        return value;
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

test "controlled dense finalization session claim excludes competing transfers across map growth" {
    const alloc = std.testing.allocator;
    var owner: Owner = .{};
    defer owner.deinit(alloc, undefined);
    var snapshot: snapshot_admission_mod.SnapshotAdmission = .{};
    var mutation: ?snapshot_admission_mod.SnapshotAdmission.MutationLease = snapshot.acquireMutation();
    defer if (mutation) |*lease| lease.release();
    const token = try owner.install(alloc, "idx", 7, null, &mutation);
    var retained = (try owner.retainAdmission("idx", token)).?;
    defer retained.release();
    var claim = (try owner.tryClaimFinish("idx", token)).?;
    defer claim.deinit();
    try std.testing.expectError(error.DenseCatchUpSessionSuperseded, owner.tryClaimFinish("other", token));
    try std.testing.expectError(error.DenseCatchUpSessionSuperseded, owner.tryClaimFinish("idx", .{}));
    try std.testing.expect(try owner.tryClaimFinish("idx", token) == null);
    try std.testing.expect(try owner.tryTake("idx", token) == null);
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, owner.take("idx", token));
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, owner.retainAdmission("idx", token));
    for (0..128) |_| {
        var no_snapshot: ?snapshot_admission_mod.SnapshotAdmission.MutationLease = null;
        _ = try owner.install(alloc, "other", 8, null, &no_snapshot);
    }
    var session = (try claim.tryTake()).?;
    defer alloc.free(session.index_name);
    defer if (session.snapshot_replay) |*lease| lease.release();
    try std.testing.expectEqual(@as(u64, 7), session.index_incarnation);
    try std.testing.expectError(error.DenseCatchUpSessionSuperseded, claim.tryTake());
    claim.deinit();
    try std.testing.expectError(error.DenseCatchUpSessionSuperseded, owner.tryClaimFinish("idx", token));
    try std.testing.expect(!snapshot.lock.tryLockExclusive());
    session.snapshot_replay.?.release();
    session.snapshot_replay = null;
    try std.testing.expect(!snapshot.lock.tryLockExclusive());
    retained.release();
    try std.testing.expect(snapshot.lock.tryLockExclusive());
    snapshot.lock.unlockExclusive();
}

test "controlled dense finalization session claim releases rejection without retiring the token" {
    const Check = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var owner: Owner = .{};
            defer owner.deinit(alloc, undefined);
            var snapshot: snapshot_admission_mod.SnapshotAdmission = .{};
            var mutation: ?snapshot_admission_mod.SnapshotAdmission.MutationLease = snapshot.acquireMutation();
            defer if (mutation) |*lease| lease.release();
            const token = try owner.install(alloc, "idx", 7, null, &mutation);
            var claim = (try owner.tryClaimFinish("idx", token)).?;
            try std.testing.expect(owner.mutex.tryLock());
            claim.deinit();
            owner.mutex.unlock();
            claim.deinit();
            try std.testing.expectEqual(@as(u32, 1), owner.sessions.count());
            try std.testing.expect(!snapshot.lock.tryLockExclusive());
            var retry = (try owner.tryClaimFinish("idx", token)).?;
            defer retry.deinit();
            var session = (try retry.tryTake()).?;
            defer alloc.free(session.index_name);
            defer if (session.snapshot_replay) |*lease| lease.release();
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}

test "controlled dense finalization session claim never waits for a busy registry during transfer" {
    var owner: Owner = .{};
    defer owner.deinit(std.testing.allocator, undefined);
    var no_snapshot: ?snapshot_admission_mod.SnapshotAdmission.MutationLease = null;
    const token = try owner.install(std.testing.allocator, "idx", 7, null, &no_snapshot);
    try std.testing.expect(owner.mutex.tryLock());
    const unavailable = owner.tryClaimFinish("idx", token);
    owner.mutex.unlock();
    try std.testing.expect(try unavailable == null);
    var claim = (try owner.tryClaimFinish("idx", token)).?;
    defer claim.deinit();
    try std.testing.expect(owner.mutex.tryLock());
    const blocked = claim.tryTake();
    owner.mutex.unlock();
    try std.testing.expect(try blocked == null);
    const session = (try claim.tryTake()).?;
    defer std.testing.allocator.free(session.index_name);
    try std.testing.expectEqual(@as(u64, 7), session.index_incarnation);
}
