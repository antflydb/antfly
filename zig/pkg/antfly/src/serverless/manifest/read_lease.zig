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

//! Read protection shared by all readers of an immutable manifest version.
//! Pin publication precedes the retirement-floor check. GC advances the floor
//! before reading pins, so a successful acquisition is visible to every sweep
//! that could reclaim the version. A retired version cannot acquire new rights.
//! Deadlines require bounded inter-host clock skew, like publication leases.
//! GC allows 30 seconds of skew; readers additionally use a suspend-inclusive
//! local authority deadline so a wall-clock rollback cannot extend authority.
const std = @import("std");
const platform = @import("antfly_platform");
const ProgressStore = @import("../catalog/progress_store.zig").ProgressStore;

pub const duration_ns = 10 * std.time.ns_per_min;
pub const reuse_min_ns = 5 * std.time.ns_per_min;
pub const gc_grace_ns = 30 * std.time.ns_per_s;

pub const Clock = struct {
    ptr: ?*const anyopaque = null,
    unix_fn: ?*const fn (*const anyopaque) u64 = null,

    pub fn unixNs(self: Clock) u64 {
        if (self.unix_fn) |read| return read(self.ptr.?);
        return platform.time.realtimeNs();
    }
};

pub const Lease = struct {
    unix_deadline: u64,
    authority_deadline: u64,

    pub fn check(self: Lease) !void {
        const unix = platform.time.realtimeNs();
        const authority = platform.time.authorityNs();
        if (unix == 0 or authority == 0 or unix >= self.unix_deadline or authority >= self.authority_deadline)
            return error.ManifestReadLeaseExpired;
    }

    fn reusable(self: Lease, unix: u64, authority: u64) bool {
        return self.unix_deadline -| unix >= reuse_min_ns and self.authority_deadline -| authority >= reuse_min_ns;
    }
};

pub fn protects(deadline: u64, unix: u64) bool {
    // A failed wall-clock read must retain, never prematurely reclaim.
    return unix == 0 or deadline +| gc_grace_ns >= unix;
}

/// Small allocation-free process-local cache. No remote I/O under its mutex;
/// concurrent cache misses converge through the durable monotonic CAS. Cache
/// eviction only costs another acquisition, never releases active readers.
pub const Cache = struct {
    const Entry = struct { namespace: [32]u8, version: u64, lease: Lease };
    mutex: std.atomic.Mutex = .unlocked,
    entries: [64]?Entry = @splat(null),
    next: usize = 0,

    pub fn acquire(self: *Cache, progress: *ProgressStore, namespace: []const u8, version: u64) !Lease {
        const unix = platform.time.realtimeNs();
        const authority = platform.time.authorityNs();
        if (unix == 0 or authority == 0 or authority == std.math.maxInt(u64)) return error.ManifestReadLeaseExpired;
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(namespace, &digest, .{});
        platform.sync.lockYielding(&self.mutex);
        for (self.entries) |slot| if (slot) |entry| {
            if (entry.version == version and std.mem.eql(u8, &entry.namespace, &digest) and entry.lease.reusable(unix, authority)) {
                self.mutex.unlock();
                return entry.lease;
            }
        };
        self.mutex.unlock();
        const lease = try acquireAt(progress, namespace, version, unix, authority);
        try lease.check();
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        for (&self.entries) |*slot| if (slot.*) |entry| {
            if (entry.version == version and std.mem.eql(u8, &entry.namespace, &digest)) {
                if (lease.authority_deadline > entry.lease.authority_deadline) slot.*.?.lease = lease;
                return lease;
            }
        };
        self.entries[self.next] = .{ .namespace = digest, .version = version, .lease = lease };
        self.next = (self.next + 1) % self.entries.len;
        return lease;
    }
};

/// Request-local protection from an already authenticated bounded cut. Never
/// publish or renew a pin, and never populate the long-lived lease cache. The
/// progress transport supplies either a durable pin or a GC-protected cut with
/// its original deadline. A missing, expired or retired cut fails closed.
pub fn acquireBoundedAt(progress: *ProgressStore, namespace: []const u8, version: u64, unix: u64, authority: u64) !Lease {
    if (unix == 0 or authority == 0 or authority == std.math.maxInt(u64)) return error.ManifestReadLeaseExpired;
    const deadline = (try progress.getManifestReadDeadline(namespace, version)) orelse return error.ManifestReadLeaseExpired;
    if (deadline <= unix) return error.ManifestReadLeaseExpired;
    if (try progress.getManifestGcFloor(namespace)) |floor| if (version < floor) return error.ManifestVersionRetired;
    const remaining = @min(deadline - unix, 30 * std.time.ns_per_s);
    return .{ .unix_deadline = std.math.add(u64, unix, remaining) catch return error.ManifestReadLeaseExpired, .authority_deadline = std.math.add(u64, authority, remaining) catch return error.ManifestReadLeaseExpired };
}

pub fn acquireAt(progress: *ProgressStore, namespace: []const u8, version: u64, unix: u64, authority: u64) !Lease {
    if (try progress.getManifestGcFloor(namespace)) |floor| if (version < floor) return error.ManifestVersionRetired;
    const desired = std.math.add(u64, unix, duration_ns) catch return error.ManifestReadLeaseExpired;
    var deadline: u64 = undefined;
    for (0..16) |_| {
        const prior = try progress.getManifestReadDeadline(namespace, version);
        if (prior) |value| if (value -| unix >= reuse_min_ns) {
            deadline = value;
            break;
        };
        if (try progress.compareAndSwapManifestReadDeadline(namespace, version, prior, desired)) {
            deadline = desired;
            break;
        }
    } else return error.ManifestReadLeaseContended;
    // This check MUST follow the pin write (or observation of another reader's
    // write). Reversing these operations admits readers after a GC snapshot.
    if (try progress.getManifestGcFloor(namespace)) |floor| if (version < floor) return error.ManifestVersionRetired;
    return .{
        .unix_deadline = @min(deadline, desired),
        .authority_deadline = std.math.add(u64, authority, @min(deadline -| unix, duration_ns)) catch return error.ManifestReadLeaseExpired,
    };
}

test "serverless manifest read lease grace is saturating and clock failure retains" {
    try std.testing.expect(protects(100, 0));
    try std.testing.expect(protects(100, 100 + gc_grace_ns));
    try std.testing.expect(!protects(100, 101 + gc_grace_ns));
    try std.testing.expect(protects(std.math.maxInt(u64), std.math.maxInt(u64)));
}

test "serverless native read plan bounded manifest cuts never renew or retain expired protection" {
    const objects = @import("objectstore");
    const a = std.testing.allocator;
    var memory = objects.MemoryClient.init(a);
    defer memory.deinit();
    var backing = try @import("../catalog/object_progress_store.zig").ObjectProgressStore.initWithClient(a, memory.client(), "db", "progress");
    defer backing.deinit();
    var progress = backing.progressStore();
    const now = 100 * std.time.ns_per_s;
    try std.testing.expectError(error.ManifestReadLeaseExpired, acquireBoundedAt(&progress, "ns", 3, now, 1));
    const deadline = now + 20 * std.time.ns_per_s;
    try std.testing.expect(try progress.compareAndSwapManifestReadDeadline("ns", 3, null, deadline));
    const cut = try acquireBoundedAt(&progress, "ns", 3, now, 1);
    try std.testing.expectEqual(deadline, cut.unix_deadline);
    try std.testing.expectEqual(20 * std.time.ns_per_s + 1, cut.authority_deadline);
    try std.testing.expectEqual(deadline, (try progress.getManifestReadDeadline("ns", 3)).?);
    try std.testing.expectError(error.ManifestReadLeaseExpired, acquireBoundedAt(&progress, "ns", 3, deadline, 1));
    try std.testing.expect(try progress.compareAndSwapManifestGcFloor("ns", null, 4));
    try std.testing.expectError(error.ManifestVersionRetired, acquireBoundedAt(&progress, "ns", 3, now, 1));
    try std.testing.expect(try progress.compareAndSwapManifestReadDeadline("ns", 4, null, now + duration_ns));
    const long = try acquireBoundedAt(&progress, "ns", 4, now, 1);
    try std.testing.expectEqual(now + 30 * std.time.ns_per_s, long.unix_deadline);
    try std.testing.expectError(error.ManifestReadLeaseExpired, acquireBoundedAt(&progress, "ns", 4, 0, 1));
}
