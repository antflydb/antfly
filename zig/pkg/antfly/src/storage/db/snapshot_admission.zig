// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Per-DB admission barrier for revision-consistent native snapshots.
//!
//! Persistent mutation paths hold a shared lease before reserving a replay
//! sequence or changing the index catalog. Native capture holds the exclusive
//! lease while it drains maintenance and stages a generation. The underlying
//! writer-preferring lock prevents a continuous write stream from starving a
//! queued snapshot. Thread-local lease tracking makes nested mutation helpers
//! safe even after an exclusive snapshot waiter has queued.

const std = @import("std");
const apply_rw_lock_mod = @import("apply_rw_lock.zig");

const max_nested_admissions = 16;

const SharedEntry = struct {
    admission: *SnapshotAdmission,
    depth: usize,
};

threadlocal var shared_entries: [max_nested_admissions]SharedEntry = undefined;
threadlocal var shared_entry_count: usize = 0;
threadlocal var exclusive_admissions: [max_nested_admissions]*SnapshotAdmission = undefined;
threadlocal var exclusive_admission_count: usize = 0;

fn ownsExclusive(admission: *SnapshotAdmission) bool {
    for (exclusive_admissions[0..exclusive_admission_count]) |entry| {
        if (entry == admission) return true;
    }
    return false;
}

fn registerExclusive(admission: *SnapshotAdmission) void {
    std.debug.assert(!ownsExclusive(admission));
    std.debug.assert(exclusive_admission_count < exclusive_admissions.len);
    exclusive_admissions[exclusive_admission_count] = admission;
    exclusive_admission_count += 1;
}

fn unregisterExclusive(admission: *SnapshotAdmission) void {
    for (exclusive_admissions[0..exclusive_admission_count], 0..) |entry, i| {
        if (entry != admission) continue;
        exclusive_admission_count -= 1;
        if (i != exclusive_admission_count)
            exclusive_admissions[i] = exclusive_admissions[exclusive_admission_count];
        return;
    }
    unreachable;
}

pub const SnapshotAdmission = struct {
    lock: apply_rw_lock_mod.ApplyRwLock = .{},

    pub const MutationLease = struct {
        admission: *SnapshotAdmission,
        bypassed: bool = false,
        active: bool = true,

        pub fn release(self: *@This()) void {
            if (!self.active) return;
            if (!self.bypassed) self.admission.releaseMutation();
            self.active = false;
        }

        pub fn deinit(self: *@This()) void {
            self.release();
        }
    };

    pub const CaptureLease = struct {
        admission: *SnapshotAdmission,
        active: bool = true,

        pub fn release(self: *@This()) void {
            if (!self.active) return;
            unregisterExclusive(self.admission);
            self.admission.lock.unlockExclusive();
            self.active = false;
        }

        pub fn deinit(self: *@This()) void {
            self.release();
        }
    };

    pub fn acquireMutation(self: *@This()) MutationLease {
        // Maintenance invoked synchronously by the capture owner is allowed to
        // use ordinary mutation helpers while every other thread remains
        // fenced. Capture chooses its target only after that maintenance drain.
        if (ownsExclusive(self)) {
            return .{ .admission = self, .bypassed = true };
        }
        for (shared_entries[0..shared_entry_count]) |*entry| {
            if (entry.admission == self) {
                entry.depth += 1;
                return .{ .admission = self };
            }
        }
        std.debug.assert(shared_entry_count < shared_entries.len);
        self.lock.lockShared();
        shared_entries[shared_entry_count] = .{ .admission = self, .depth = 1 };
        shared_entry_count += 1;
        return .{ .admission = self };
    }

    pub fn acquireCapture(self: *@This()) CaptureLease {
        std.debug.assert(!ownsExclusive(self));
        for (shared_entries[0..shared_entry_count]) |entry| {
            std.debug.assert(entry.admission != self);
        }
        self.lock.lockExclusive();
        registerExclusive(self);
        return .{ .admission = self };
    }

    pub fn acquireCaptureIo(self: *@This(), io: std.Io, cancellation: anytype) !CaptureLease {
        std.debug.assert(!ownsExclusive(self));
        for (shared_entries[0..shared_entry_count]) |entry| {
            std.debug.assert(entry.admission != self);
        }
        try self.lock.lockExclusiveIo(io, cancellation);
        registerExclusive(self);
        return .{ .admission = self };
    }

    fn releaseMutation(self: *@This()) void {
        for (shared_entries[0..shared_entry_count], 0..) |*entry, i| {
            if (entry.admission != self) continue;
            std.debug.assert(entry.depth > 0);
            entry.depth -= 1;
            if (entry.depth != 0) return;
            shared_entry_count -= 1;
            if (i != shared_entry_count) shared_entries[i] = shared_entries[shared_entry_count];
            self.lock.unlockShared();
            return;
        }
        unreachable;
    }
};

test "storage.db snapshot admission permits nested mutation leases" {
    var admission: SnapshotAdmission = .{};
    var outer = admission.acquireMutation();
    var inner = admission.acquireMutation();
    try std.testing.expect(!admission.lock.tryLockExclusive());
    inner.release();
    try std.testing.expect(!admission.lock.tryLockExclusive());
    outer.release();
    try std.testing.expect(admission.lock.tryLockExclusive());
    admission.lock.unlockExclusive();
}

test "storage.db snapshot admission lets capture-owned maintenance bypass the fence" {
    var admission: SnapshotAdmission = .{};
    var capture = admission.acquireCapture();
    var maintenance = admission.acquireMutation();
    try std.testing.expect(maintenance.bypassed);
    maintenance.release();
    capture.release();
}

test "storage.db snapshot admission supports ordered nested capture barriers" {
    var primary: SnapshotAdmission = .{};
    var replay: SnapshotAdmission = .{};
    var primary_capture = primary.acquireCapture();
    var replay_capture = replay.acquireCapture();
    var primary_maintenance = primary.acquireMutation();
    var replay_maintenance = replay.acquireMutation();
    try std.testing.expect(primary_maintenance.bypassed);
    try std.testing.expect(replay_maintenance.bypassed);
    replay_maintenance.release();
    primary_maintenance.release();
    replay_capture.release();
    primary_capture.release();
}
