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

//! Fixed node metadata for publishing verified family support. Endpoint identity
//! is immutable here; replacing a peer requires a separately owned descriptor.
const std = @import("std");
const caps = @import("../common/remote_read_capabilities.zig");
const replies = @import("../common/remote_attempt_reply.zig");
const scheduling = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
const time = @import("antfly_platform").time;
fn bits(value: caps.Families) u8 {
    return @as(u8, @intFromBool(value.group_query)) | (@as(u8, @intFromBool(value.retained_read)) << 1);
}
fn families(value: u8) caps.Families {
    return .{ .group_query = value & 1 != 0, .retained_read = value & 2 != 0 };
}
pub const Cache = struct {
    /// Both the observation tag and families are published in one atomic byte.
    /// A fresh signed negative is different from absent or invalidated proof.
    pub const ProfileTag = enum(u8) { unverified, verified };
    pub const Profile = union(ProfileTag) { unverified, verified: caps.Families };
    const verified_profile_mask: u8 = 1 << 7;
    allocator: std.mem.Allocator,
    entries: []Entry,
    baseline: scheduling.Scheduler.ControlBaseline,
    now_ns: *const fn () u64,
    mutex: std.atomic.Mutex = .unlocked,
    pending: usize = 0,
    closed: bool = false,
    enabled: std.atomic.Value(u8) = .init(0),
    shared_activation: ?*std.atomic.Value(u8) = null,
    const Entry = struct {
        authority: replies.Authority,
        sequence: u64 = 0,
        active_sequence: u64 = 0,
        pending: bool = false,
        invalidated: bool = false,
        published: std.atomic.Value(u8) = .init(0),
    };
    pub fn reservationBytes(count: usize) !u64 {
        if (count == 0 or count > 256) return error.InvalidArgument;
        return std.math.add(u64, @sizeOf(Cache), try std.math.mul(u64, count, @sizeOf(Entry)));
    }
    pub fn create(allocator: std.mem.Allocator, scheduler: *scheduling.Scheduler, authorities: []const replies.Authority, now_ns: *const fn () u64) !*Cache {
        var baseline = scheduler.reserveControlBaseline(try reservationBytes(authorities.len)) catch |err| return if (err == error.SchedulerClosed) err else error.CleanupCapacityUnavailable;
        errdefer baseline.release();
        const self = try allocator.create(Cache);
        errdefer allocator.destroy(self);
        const entries = try allocator.alloc(Entry, authorities.len);
        for (entries, authorities) |*entry, authority| entry.* = .{ .authority = authority };
        self.* = .{ .allocator = allocator, .entries = entries, .baseline = baseline, .now_ns = now_ns };
        return self;
    }
    pub fn reservedBytes(self: *const Cache) u64 {
        return self.baseline.bytes;
    }
    fn lock(self: *Cache) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    fn checkedEntry(self: *Cache, index: usize, authority: replies.Authority) !*Entry {
        if (index >= self.entries.len or !std.meta.eql(self.entries[index].authority, authority)) return error.InvalidRemoteReply;
        return &self.entries[index];
    }
    pub fn profile(self: *Cache, index: usize, authority: replies.Authority) !Profile {
        const value = try self.checkedEntry(index, authority);
        const published = value.published.load(.acquire);
        if (published & verified_profile_mask == 0) return .unverified;
        return .{ .verified = families(published) };
    }
    pub fn snapshot(self: *Cache, index: usize, authority: replies.Authority) !caps.Families {
        return switch (try self.profile(index, authority)) {
            .unverified => .{},
            .verified => |value| value,
        };
    }
    /// Once this process opts into an owned family, a failed refresh cannot
    /// silently send new work through legacy unowned dispatch.
    pub fn everEnabled(self: *Cache, retained: bool) bool {
        return (if (self.shared_activation) |shared| shared.load(.acquire) else self.enabled.load(.acquire)) & @as(u8, if (retained) 2 else 1) != 0;
    }
    /// Durable journals predate per-family activation bits. Any prior
    /// publication conservatively closes both legacy families after restart.
    /// This only restores the monotonic fallback latch: published capability
    /// bits remain empty until a fresh destination-bound observation arrives.
    pub fn restorePublishedHistory(self: *Cache, journal: @import("../common/remote_fence_store.zig").Store.InitializationState) !void {
        self.lock();
        defer self.mutex.unlock();
        if (self.closed) return error.CleanupCapacityUnavailable;
        if (std.mem.allEqual(u8, &journal.id, 0)) return error.InvalidRemoteFenceStore;
        if (!journal.publication_started) return;
        const activated = bits(.{ .group_query = true, .retained_read = true });
        _ = self.enabled.fetchOr(activated, .acq_rel);
        if (self.shared_activation) |shared| _ = shared.fetchOr(activated, .acq_rel);
    }

    /// Move-only, one in-flight probe per enrolled destination. Destruction
    /// requires all tickets to have physically returned and been consumed.
    pub const Probe = struct {
        owner: ?*Cache,
        index: usize,
        sequence: u64,
        pub fn publish(self: *Probe, observation: caps.Verified) !void {
            const owner = self.owner orelse return error.InvalidRunnableTransition;
            self.owner = null;
            try owner.finish(self.index, self.sequence, observation);
        }
        pub fn abort(self: *Probe) !void {
            const owner = self.owner orelse return;
            self.owner = null;
            try owner.finish(self.index, self.sequence, null);
        }
    };
    pub fn begin(self: *Cache, index: usize, authority: replies.Authority) !Probe {
        self.lock();
        defer self.mutex.unlock();
        if (self.closed) return error.CleanupCapacityUnavailable;
        const value = try self.checkedEntry(index, authority);
        if (value.pending) return error.ResourceTemporarilyUnavailable;
        if (value.sequence == std.math.maxInt(u64)) return error.RemoteAttemptSequenceExhausted;
        value.sequence += 1;
        value.active_sequence = value.sequence;
        value.pending = true;
        value.invalidated = false;
        self.pending += 1;
        return .{ .owner = self, .index = index, .sequence = value.sequence };
    }
    fn finish(self: *Cache, index: usize, sequence: u64, observation: ?caps.Verified) !void {
        self.lock();
        defer self.mutex.unlock();
        if (index >= self.entries.len) return error.InvalidRunnableTransition;
        const value = &self.entries[index];
        if (!value.pending or value.active_sequence != sequence) return error.InvalidRunnableTransition;
        value.pending = false;
        self.pending -= 1;
        const proof = observation orelse return;
        if (self.closed) return error.CleanupCapacityUnavailable;
        if (value.invalidated or value.sequence != sequence) return error.InvalidRunnableTransition;
        const accepted = try proof.forAuthority(value.authority, self.now_ns());
        const mask = bits(accepted);
        // Close legacy fallback before making the first owned family visible.
        if (self.shared_activation) |shared| _ = shared.fetchOr(mask, .acq_rel);
        _ = self.enabled.fetchOr(mask, .acq_rel);
        value.published.store(mask | verified_profile_mask, .release);
    }
    pub fn invalidate(self: *Cache, index: usize, authority: replies.Authority) !void {
        self.lock();
        defer self.mutex.unlock();
        const value = try self.checkedEntry(index, authority);
        value.published.store(0, .release);
        value.invalidated = true;
        if (value.sequence != std.math.maxInt(u64)) value.sequence += 1;
    }
    pub fn close(self: *Cache) void {
        self.lock();
        defer self.mutex.unlock();
        self.closed = true;
        for (self.entries) |*value| value.published.store(0, .release);
    }
    /// The supervisor first fences/joins every client reader and producer.
    /// A pending probe leaves the charged owner intact for a later join.
    pub fn deinit(self: *Cache) !void {
        self.lock();
        const ready = self.closed and self.pending == 0;
        self.mutex.unlock();
        if (!ready) return error.CleanupPending;
        const allocator = self.allocator;
        var baseline = self.baseline;
        allocator.free(self.entries);
        allocator.destroy(self);
        baseline.release();
    }
};

test "remote attempt client capability publication preserves authority scope baseline and pending shutdown" {
    const Clock = struct {
        var tick: u64 = 100;
        fn now() u64 {
            return tick;
        }
    };
    const pair = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(@splat(7));
    const authority: replies.Authority = .{ .local = .{ .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .node_id = 9, .store_id = 13, .root_incarnation = 11, .reporter_incarnation = 17 }, .public_key = pair.public_key.toBytes() };
    var node = try scheduling.NodeOwner.init(.{ .slots = 2, .read_isolation = false, .control_bytes = 1 << 20 });
    defer node.deinit();
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, Cache.create(failing.allocator(), &node.scheduler, &.{authority}, Clock.now));
    try std.testing.expectEqual(@as(u64, 0), node.scheduler.stats().retained_bytes);
    const cache = try Cache.create(std.testing.allocator, &node.scheduler, &.{authority}, Clock.now);
    var live = true;
    defer if (live) {
        cache.close();
        cache.deinit() catch unreachable;
    };
    const baseline = cache.reservedBytes();
    try std.testing.expectEqual(baseline, node.scheduler.stats().retained_bytes);
    try std.testing.expectEqual(@as(usize, 0), node.scheduler.stats().operations);
    try std.testing.expectEqual(@as(usize, 0), node.scheduler.stats().runnable_slots);
    try std.testing.expectError(error.InvalidRunnableTransition, node.tryDeinit());
    const proof: caps.Verified = .{ .authority = authority, .families = .{ .retained_read = true }, .deadline_ns = 200 };
    var probe = try cache.begin(0, authority);
    defer if (live) probe.abort() catch unreachable;
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, cache.begin(0, authority));
    Clock.tick = 200;
    try std.testing.expectError(error.DeadlineExceeded, probe.publish(proof));
    try std.testing.expect(!(try cache.snapshot(0, authority)).retained_read);
    Clock.tick = 100;
    probe = try cache.begin(0, authority);
    var forged = proof;
    forged.authority.local.reporter_incarnation += 1;
    try std.testing.expectError(error.InvalidRemoteReply, probe.publish(forged));
    probe = try cache.begin(0, authority);
    try probe.publish(proof);
    try std.testing.expect((try cache.snapshot(0, authority)).retained_read);
    try std.testing.expect(cache.everEnabled(true));
    probe = try cache.begin(0, authority);
    try cache.invalidate(0, authority);
    try std.testing.expectError(error.InvalidRunnableTransition, probe.publish(proof));
    try std.testing.expect(!(try cache.snapshot(0, authority)).retained_read);
    try std.testing.expect(cache.everEnabled(true));
    cache.entries[0].sequence = std.math.maxInt(u64) - 1;
    probe = try cache.begin(0, authority);
    try cache.invalidate(0, authority);
    try std.testing.expectError(error.InvalidRunnableTransition, probe.publish(proof));
    try std.testing.expectError(error.RemoteAttemptSequenceExhausted, cache.begin(0, authority));
    // A fresh fixture sequence permits the independent pending-join check.
    cache.entries[0].sequence = 1000;
    probe = try cache.begin(0, authority);
    cache.close();
    try std.testing.expectError(error.CleanupPending, cache.deinit());
    try std.testing.expectEqual(baseline, node.scheduler.stats().retained_bytes);
    try std.testing.expectError(error.CleanupCapacityUnavailable, probe.publish(proof));
    try cache.deinit();
    live = false;
    try std.testing.expectEqual(@as(u64, 0), node.scheduler.stats().retained_bytes);
}

test "remote attempt client capability publication allocation failures release every provisional byte" {
    const Fixture = struct {
        fn run(allocator: std.mem.Allocator) !void {
            var scheduler = try scheduling.Scheduler.init(.{ .slots = 2, .read_isolation = false, .control_bytes = 1 << 20 });
            defer scheduler.close();
            const authority: replies.Authority = .{ .local = .{ .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .node_id = 9, .store_id = 13, .root_incarnation = 11, .reporter_incarnation = 17 }, .public_key = @splat(7) };
            const cache = Cache.create(allocator, &scheduler, &.{authority}, time.monotonicNs) catch |err| {
                try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
                return err;
            };
            cache.close();
            try cache.deinit();
            try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "remote attempt client restores durable publication activation without granting capabilities" {
    const durable = @import("../common/remote_fence_store.zig");
    const Authority = struct {
        phase: @FieldType(durable.InitializationGrant, "phase") = .prepared,
        fn resolve(raw: *anyopaque) !durable.InitializationGrant {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return .{ .id = @splat(7), .phase = self.phase };
        }
    };
    const Clock = struct {
        fn now() u64 {
            return 100;
        }
    };
    const authority: replies.Authority = .{ .local = .{ .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .node_id = 9, .store_id = 13, .root_incarnation = 11, .reporter_incarnation = 17 }, .public_key = @splat(7) };
    for ([_]bool{ false, true }) |published| {
        var directory = std.testing.tmpDir(.{});
        defer directory.cleanup();
        var enrollment: Authority = .{};
        const enrollment_port: durable.InitializationAuthority = .{ .context = &enrollment, .resolve = Authority.resolve };
        {
            var store = try durable.Store.openAuthorized(std.testing.allocator, std.testing.io, directory.dir, 11, 2, enrollment_port);
            defer store.deinit();
            if (published) try store.withWait(null).markPublicationStarted();
        }
        enrollment.phase = .active;
        var reopened = try durable.Store.openAuthorized(std.testing.allocator, std.testing.io, directory.dir, 11, 2, enrollment_port);
        defer reopened.deinit();
        const journal = try reopened.initializationState();
        try std.testing.expectEqual(published, journal.publication_started);
        var node = try scheduling.NodeOwner.init(.{ .slots = 2, .read_isolation = false, .control_bytes = 1 << 20 });
        defer node.deinit();
        var shared_activation: std.atomic.Value(u8) = .init(0);
        {
            const cache = try Cache.create(std.testing.allocator, &node.scheduler, &.{authority}, Clock.now);
            defer {
                cache.close();
                cache.deinit() catch unreachable;
            }
            cache.shared_activation = &shared_activation;
            try std.testing.expectError(error.InvalidRemoteFenceStore, cache.restorePublishedHistory(.{ .id = @splat(0), .publication_started = true }));
            try std.testing.expect(!cache.everEnabled(false) and !cache.everEnabled(true));
            try cache.restorePublishedHistory(journal);
            try std.testing.expectEqual(published, cache.everEnabled(false));
            try std.testing.expectEqual(published, cache.everEnabled(true));
            try std.testing.expectEqualDeep(caps.Families{}, try cache.snapshot(0, authority));
            // Even a fresh verified response advertising no owned support
            // must not erase the durable fallback prohibition.
            var probe = try cache.begin(0, authority);
            defer probe.abort() catch unreachable;
            try probe.publish(.{ .authority = authority, .families = .{}, .deadline_ns = 200 });
            try cache.invalidate(0, authority);
            try std.testing.expectEqual(published, cache.everEnabled(false));
            try std.testing.expectEqual(published, cache.everEnabled(true));
            try std.testing.expectEqualDeep(caps.Families{}, try cache.snapshot(0, authority));
        }
        // Endpoint refresh creates a new cache sharing this node latch. It
        // must retain the prohibition without inventing capability grants.
        const replacement = try Cache.create(std.testing.allocator, &node.scheduler, &.{authority}, Clock.now);
        defer {
            replacement.close();
            replacement.deinit() catch unreachable;
        }
        replacement.shared_activation = &shared_activation;
        try std.testing.expectEqual(published, replacement.everEnabled(false));
        try std.testing.expectEqual(published, replacement.everEnabled(true));
        try std.testing.expectEqualDeep(caps.Families{}, try replacement.snapshot(0, authority));
    }
}

test "remote attempt client capability publication distinguishes unverified from verified negative profiles atomically" {
    const Clock = struct {
        var tick: u64 = 100;
        fn now() u64 {
            return tick;
        }
    };
    Clock.tick = 100;
    const pair = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(@splat(7));
    const authority: replies.Authority = .{ .local = .{ .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .node_id = 9, .store_id = 13, .root_incarnation = 11, .reporter_incarnation = 17 }, .public_key = pair.public_key.toBytes() };
    var node = try scheduling.NodeOwner.init(.{ .slots = 2, .read_isolation = false, .control_bytes = 1 << 20 });
    defer node.tryDeinit() catch unreachable;
    var shared_activation = std.atomic.Value(u8).init(0);
    const cache = try Cache.create(std.testing.allocator, &node.scheduler, &.{authority}, Clock.now);
    cache.shared_activation = &shared_activation;
    defer {
        cache.close();
        cache.deinit() catch unreachable;
    }
    try std.testing.expect((try cache.profile(0, authority)) == .unverified);
    var negative = try cache.begin(0, authority);
    try negative.publish(.{ .authority = authority, .families = .{}, .deadline_ns = 200 });
    switch (try cache.profile(0, authority)) {
        .unverified => return error.TestNegativeProfileLost,
        .verified => |profile| try std.testing.expectEqualDeep(caps.Families{}, profile),
    }
    try std.testing.expectEqualDeep(caps.Families{}, try cache.snapshot(0, authority));
    try std.testing.expect(!cache.everEnabled(false) and !cache.everEnabled(true));
    // A deferred local probe preserves even a verified negative observation.
    var aborted = try cache.begin(0, authority);
    try aborted.abort();
    try std.testing.expect((try cache.profile(0, authority)) == .verified);
    var positive = try cache.begin(0, authority);
    const supported: caps.Families = .{ .group_query = true, .retained_read = true };
    try positive.publish(.{ .authority = authority, .families = supported, .deadline_ns = 200 });
    switch (try cache.profile(0, authority)) {
        .unverified => return error.TestPositiveProfileLost,
        .verified => |profile| try std.testing.expectEqualDeep(supported, profile),
    }
    try std.testing.expectEqualDeep(supported, try cache.snapshot(0, authority));
    try cache.invalidate(0, authority);
    try std.testing.expect((try cache.profile(0, authority)) == .unverified);
    try std.testing.expectEqualDeep(caps.Families{}, try cache.snapshot(0, authority));
    try std.testing.expect(cache.everEnabled(false) and cache.everEnabled(true));
    var expired = try cache.begin(0, authority);
    Clock.tick = 200;
    try std.testing.expectError(error.DeadlineExceeded, expired.publish(.{ .authority = authority, .families = supported, .deadline_ns = 200 }));
    try std.testing.expect((try cache.profile(0, authority)) == .unverified);
    Clock.tick = 100;
    var different = authority;
    different.local.reporter_incarnation += 1;
    try std.testing.expectError(error.InvalidRemoteReply, cache.profile(0, different));
    negative = try cache.begin(0, authority);
    try negative.publish(.{ .authority = authority, .families = .{}, .deadline_ns = 200 });
    try std.testing.expect((try cache.profile(0, authority)) == .verified);
    try std.testing.expect(cache.everEnabled(false) and cache.everEnabled(true));
    // A replacement endpoint cache inherits the no-fallback latch, but cannot
    // inherit any previous cache's profile observation or capability grants.
    const replacement = try Cache.create(std.testing.allocator, &node.scheduler, &.{authority}, Clock.now);
    replacement.shared_activation = &shared_activation;
    defer {
        replacement.close();
        replacement.deinit() catch unreachable;
    }
    try std.testing.expect((try replacement.profile(0, authority)) == .unverified);
    try std.testing.expectEqualDeep(caps.Families{}, try replacement.snapshot(0, authority));
    try std.testing.expect(replacement.everEnabled(false) and replacement.everEnabled(true));
}
