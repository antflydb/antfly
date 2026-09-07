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
const Allocator = std.mem.Allocator;
const lsm_backend = @import("../lsm_backend.zig");
const platform_time = @import("antfly_platform").time;
const docstore_mod = @import("../docstore.zig");
const mem_backend = @import("../mem_backend.zig");
const lease_mod = @import("lease.zig");

pub const Config = struct {
    lease_owned: bool = false,
    owner_id: []const u8 = "local",
    lease_ttl_ms: u64 = 30_000,
};

pub const Stats = struct {
    lease_owned: bool = false,
    has_lease: bool = false,
    acquisition_count: u64 = 0,
    takeover_count: u64 = 0,
    lease_acquire_failures: u64 = 0,
    lost_leases: u64 = 0,
    last_acquired_ms: u64 = 0,
    lease_expires_at_ms: u64 = 0,
    lease_renew_after_ms: u64 = 0,
    renewal_count: u64 = 0,
};

pub const State = struct {
    lease: lease_mod.Lease,
    owner_id: []u8,
    lease_owned: bool,
    lease_ttl_ms: u64,
    has_lease: bool,
    acquisition_count: u64,
    takeover_count: u64,
    lease_acquire_failures: u64,
    lost_leases: u64,
    last_acquired_ms: u64,
    lease_expires_at_ms: u64,
    lease_renew_after_ms: u64,
    renewal_count: u64,

    pub fn init(alloc: Allocator, store: anytype, key: []const u8, config: Config) !State {
        return .{
            .lease = try lease_mod.Lease.init(alloc, store, key),
            .owner_id = try alloc.dupe(u8, config.owner_id),
            .lease_owned = config.lease_owned,
            .lease_ttl_ms = config.lease_ttl_ms,
            .has_lease = !config.lease_owned,
            .acquisition_count = 0,
            .takeover_count = 0,
            .lease_acquire_failures = 0,
            .lost_leases = 0,
            .last_acquired_ms = 0,
            .lease_expires_at_ms = 0,
            .lease_renew_after_ms = 0,
            .renewal_count = 0,
        };
    }

    pub fn deinit(self: *State, alloc: Allocator) void {
        self.release();
        self.lease.deinit();
        alloc.free(self.owner_id);
        self.* = undefined;
    }

    pub fn deinitPreserveLease(self: *State, alloc: Allocator) void {
        self.lease.deinit();
        alloc.free(self.owner_id);
        self.* = undefined;
    }

    pub fn ensureLease(self: *State, now_ms: u64) !bool {
        if (!self.lease_owned) {
            self.has_lease = true;
            return true;
        }

        const had_lease = self.has_lease;
        // A lease cannot be taken by a conforming peer before its durable
        // expiry. Keep the overwhelmingly common runtime tick on this
        // in-memory path and renew early enough to tolerate scheduler stalls.
        if (had_lease and now_ms < self.lease_renew_after_ms) return true;
        const acquired = try self.lease.tryAcquireDetailed(self.owner_id, now_ms, self.lease_ttl_ms);
        if (acquired.acquiredLease()) {
            self.has_lease = true;
            if (!had_lease) {
                self.acquisition_count += 1;
                self.last_acquired_ms = now_ms;
            }
            if (had_lease and acquired == .renewed) self.renewal_count += 1;
            if (acquired == .takeover) self.takeover_count += 1;
            self.lease_expires_at_ms = now_ms +| self.lease_ttl_ms;
            // Renew with one third of the TTL remaining. A deterministic
            // per-owner jitter spreads writers sharing the same TTL without
            // making tests or restart behavior nondeterministic.
            const renewal_slack = @max(@as(u64, 1), self.lease_ttl_ms / 3);
            const jitter_window = self.lease_ttl_ms / 10;
            const jitter = if (jitter_window == 0)
                0
            else
                std.hash.Wyhash.hash(0, self.owner_id) % (jitter_window + 1);
            const base_renew_after = self.lease_expires_at_ms -| renewal_slack;
            self.lease_renew_after_ms = base_renew_after -| jitter;
            return true;
        }

        self.noteAcquireFailure();
        return false;
    }

    pub fn noteAcquireFailure(self: *State) void {
        self.lease_acquire_failures += 1;
        if (self.has_lease and self.lease_owned) {
            self.has_lease = false;
            self.lost_leases += 1;
        }
        self.lease_expires_at_ms = 0;
        self.lease_renew_after_ms = 0;
    }

    pub fn release(self: *State) void {
        if (self.lease_owned and self.has_lease) {
            _ = self.lease.release(self.owner_id) catch false;
        }
        self.has_lease = !self.lease_owned;
        self.lease_expires_at_ms = 0;
        self.lease_renew_after_ms = 0;
    }

    pub fn releaseHeldLease(self: *State) !bool {
        const released = if (self.lease_owned)
            try self.lease.release(self.owner_id)
        else
            false;
        self.has_lease = !self.lease_owned;
        self.lease_expires_at_ms = 0;
        self.lease_renew_after_ms = 0;
        return released;
    }

    pub fn loadLease(self: *State, alloc: Allocator) !?lease_mod.LeaseRecord {
        return try self.lease.load(alloc);
    }

    pub fn stats(self: *const State) Stats {
        return .{
            .lease_owned = self.lease_owned,
            .has_lease = self.has_lease,
            .acquisition_count = self.acquisition_count,
            .takeover_count = self.takeover_count,
            .lease_acquire_failures = self.lease_acquire_failures,
            .lost_leases = self.lost_leases,
            .last_acquired_ms = self.last_acquired_ms,
            .lease_expires_at_ms = self.lease_expires_at_ms,
            .lease_renew_after_ms = self.lease_renew_after_ms,
            .renewal_count = self.renewal_count,
        };
    }
};

var temp_path_nonce: u64 = 0;

fn tempPath(buf: []u8) [*:0]const u8 {
    const base = "/tmp/antfly-db-ownership-test-";
    const ts = platform_time.monotonicNs();
    const nonce = @atomicRmw(u64, &temp_path_nonce, .Add, 1, .monotonic);
    const path = std.fmt.bufPrint(buf, "{s}{d}-{d}\x00", .{ base, ts, nonce }) catch unreachable;
    return @ptrCast(path.ptr);
}

fn cleanupTempDir(path: [*:0]const u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}

test "ownership state tracks lease takeover and loss" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var store = try docstore_mod.DocStore.open(alloc, path, .{});
    defer store.close();

    var owner_a = try State.init(alloc, &store, "\x00\x00__metadata__:ownership_test", .{
        .lease_owned = true,
        .owner_id = "worker-a",
        .lease_ttl_ms = 250,
    });
    defer owner_a.deinit(alloc);
    var owner_b = try State.init(alloc, &store, "\x00\x00__metadata__:ownership_test", .{
        .lease_owned = true,
        .owner_id = "worker-b",
        .lease_ttl_ms = 250,
    });
    defer owner_b.deinit(alloc);

    try std.testing.expect(try owner_a.ensureLease(1_000));
    try std.testing.expectEqual(@as(u64, 1), owner_a.acquisition_count);
    try std.testing.expect(!(try owner_b.ensureLease(1_100)));
    try std.testing.expectEqual(@as(u64, 1), owner_b.lease_acquire_failures);
    try std.testing.expect(try owner_b.ensureLease(1_300));
    try std.testing.expectEqual(@as(u64, 1), owner_b.acquisition_count);
    try std.testing.expect(!(try owner_a.ensureLease(1_320)));
    try std.testing.expectEqual(@as(u64, 1), owner_a.lost_leases);
    try std.testing.expect(!owner_a.has_lease);
    try std.testing.expect(owner_b.has_lease);
}

test "ownership state renews only at the cached renewal deadline" {
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var runtime = try backend.runtimeStore(alloc, .{ .name = "lease-renewal" });
    defer runtime.deinit();
    var owner = try State.init(alloc, runtime, "\x00\x00__metadata__:ownership_renewal_test", .{
        .lease_owned = true,
        .owner_id = "worker-renewal",
        .lease_ttl_ms = 30_000,
    });
    defer owner.deinit(alloc);

    try std.testing.expect(try owner.ensureLease(1_000));
    const first_deadline = owner.lease_renew_after_ms;
    try std.testing.expect(first_deadline > 1_000);
    var first = (try owner.loadLease(alloc)) orelse return error.TestExpectedLease;
    defer lease_mod.deinitRecord(alloc, &first);
    try std.testing.expectEqual(@as(u64, 31_000), first.expires_at_ms);

    try std.testing.expect(try owner.ensureLease(first_deadline - 1));
    try std.testing.expectEqual(@as(u64, 0), owner.renewal_count);
    var unchanged = (try owner.loadLease(alloc)) orelse return error.TestExpectedLease;
    defer lease_mod.deinitRecord(alloc, &unchanged);
    try std.testing.expectEqual(first.expires_at_ms, unchanged.expires_at_ms);

    try std.testing.expect(try owner.ensureLease(first_deadline));
    try std.testing.expectEqual(@as(u64, 1), owner.renewal_count);
    var renewed = (try owner.loadLease(alloc)) orelse return error.TestExpectedLease;
    defer lease_mod.deinitRecord(alloc, &renewed);
    try std.testing.expectEqual(first_deadline + 30_000, renewed.expires_at_ms);
}

test "ownership state works with memory backend store" {
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();

    var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer runtime.deinit();

    var owner_a = try State.init(alloc, runtime, "\x00\x00__metadata__:ownership_test", .{
        .lease_owned = true,
        .owner_id = "worker-a",
        .lease_ttl_ms = 250,
    });
    defer owner_a.deinit(alloc);
    var owner_b = try State.init(alloc, runtime, "\x00\x00__metadata__:ownership_test", .{
        .lease_owned = true,
        .owner_id = "worker-b",
        .lease_ttl_ms = 250,
    });
    defer owner_b.deinit(alloc);

    try std.testing.expect(try owner_a.ensureLease(1_000));
    try std.testing.expect(!(try owner_b.ensureLease(1_100)));
    try std.testing.expect(try owner_b.ensureLease(1_300));
    try std.testing.expect(!(try owner_a.ensureLease(1_320)));
    try std.testing.expectEqual(@as(u64, 1), owner_a.lost_leases);
}

test "ownership state works with lsm backend store" {
    const alloc = std.testing.allocator;
    var backend = lsm_backend.Backend.init(alloc, .{ .flush_threshold = 2 });
    defer backend.close();

    var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer runtime.deinit();

    var owner_a = try State.init(alloc, runtime, "\x00\x00__metadata__:ownership_test", .{
        .lease_owned = true,
        .owner_id = "worker-a",
        .lease_ttl_ms = 250,
    });
    defer owner_a.deinit(alloc);
    var owner_b = try State.init(alloc, runtime, "\x00\x00__metadata__:ownership_test", .{
        .lease_owned = true,
        .owner_id = "worker-b",
        .lease_ttl_ms = 250,
    });
    defer owner_b.deinit(alloc);

    try std.testing.expect(try owner_a.ensureLease(1_000));
    try std.testing.expect(!(try owner_b.ensureLease(1_100)));
    try std.testing.expect(try owner_b.ensureLease(1_300));
    try std.testing.expect(!(try owner_a.ensureLease(1_320)));
    try std.testing.expectEqual(@as(u64, 1), owner_a.lost_leases);
}
