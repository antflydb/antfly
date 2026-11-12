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

//! Reader fence lifetimes, now backed by `shared_lock_region.zig`'s reader
//! table instead of `reader_epochs.zig`'s namespace-directory markers (that
//! file is deleted; this is the migration §8.7 names for it). A reader with
//! a live `*shared_lock_region.Region` claims and pins one of its fixed
//! slots -- cheap enough (a CAS, no filesystem I/O) that there is no longer
//! a reason to batch/share pins the way the old marker-file registration
//! amortized `open`+`flock`+`write` across many readers; `RegistrationBatch`
//! survives only as a stable no-op wrapper for external callers
//! (`PathWriterLock.reader_registration_batch`) that still begin/end one
//! around a path-lease hold.
//!
//! A reader with no region available (the caller passes `region = null`,
//! e.g. because `inode_lock.isRegionUnusable` says so, or because the
//! region's own slot table is exhausted) falls back to an inode-only fence:
//! no slot, no cross-process frontier contribution of its own, protected
//! only by the kernel shared lock it already holds on `self.file` --
//! exactly `writer_queue.zig`/`inode_lock.zig`'s pre-region posture. Such a
//! fence can still be temporarily suspended (its kernel lock briefly
//! released) while a writer with `reuse = true` and no other live inode-
//! only reader on the inode proves it safe to promote reuse; a region-
//! backed fence never needs this dance, since its pin already answers that
//! question directly, from any process, without touching a kernel lock at
//! all.
const std = @import("std");
const shared_lock_region = @import("shared_lock_region.zig");

pub const Coordinator = struct {
    allocator: std.mem.Allocator,
    /// Must outlive the last reader, not merely the writer. C ABI owners use
    /// the process-wide handle executor, which survives individual handles.
    io: std.Io,
    mutex: std.Io.Mutex = .init,
    references: std.atomic.Value(usize) = .init(1),
    heap_owned: bool = false,
    readers: std.ArrayList(*Fence) = .empty,
    windows: usize = 0,
    registration_batches: usize = 0,

    known: ?struct { inode: u64, tip: [32]u8 } = null,

    /// A stable compatibility shim: external callers (`capi/lite_connection
    /// .zig`'s `withReadRegistrationBatch`, and the `PathWriterLock
    /// .reader_registration_batch` field it populates) still begin and end
    /// one of these around a path-lease hold. It no longer amortizes
    /// anything -- each reader registered while a batch is open still
    /// claims and pins its own region slot directly -- so `end()` only
    /// balances the refcount `beginRegistrationBatch` bumped. Kept as a
    /// real (if now-trivial) type, not deleted, because its field name and
    /// shape are depended on outside this file; see this file's own
    /// `beginRegistrationBatch` doc for why no sharing is needed anymore.
    pub const RegistrationBatch = struct {
        coordinator: *Coordinator,
        pub fn end(self: RegistrationBatch) void {
            self.coordinator.endRegistrationBatch();
        }
    };

    /// Previously opened a shared marker lease amortized across every
    /// reader registered before the matching `end()`, because a marker
    /// file's `open`+`flock`+`write` was too expensive to pay per reader.
    /// A region reader-slot claim+pin is a CAS and two atomic stores, no
    /// filesystem I/O at all -- cheap enough per reader that sharing buys
    /// nothing anymore. This now exists only so callers that still
    /// begin/end a batch around a path-lease hold (`PathWriterLock
    /// .reader_registration_batch`'s only producer and consumer) keep
    /// working unchanged.
    pub fn beginRegistrationBatch(self: *Coordinator) RegistrationBatch {
        self.mutex.lockUncancelable(self.io);
        self.registration_batches += 1;
        _ = self.references.fetchAdd(1, .monotonic);
        self.mutex.unlock(self.io);
        return .{ .coordinator = self };
    }
    fn endRegistrationBatch(self: *Coordinator) void {
        self.mutex.lockUncancelable(self.io);
        std.debug.assert(self.registration_batches != 0);
        self.registration_batches -= 1;
        self.mutex.unlock(self.io);
        self.release();
    }

    pub const Fence = struct {
        file: std.Io.File,
        inode: u64,
        sequence: u64,
        captured_at_ns: i96 = 0,
        /// Full graph scans (snapshots, allocator verification) also
        /// retain allocator/recovery metadata pages, regardless of
        /// whether the underlying file has indexed reclamation enabled --
        /// the region's metadata pin doesn't care either way.
        metadata: bool = false,
        poisoned: bool = false,
        /// True exactly when this fence has no region slot (`slot ==
        /// null`): either no region was available at registration time,
        /// or the region's slot table was exhausted. Kept as its own
        /// field (not just `!hasEpoch()`) for readability at call sites
        /// that only care about the fallback case, not the slot itself.
        inode_only: bool = false,
        suspended: bool = false,
        region: ?*shared_lock_region.Region = null,
        slot: ?u32 = null,
        fn hasEpoch(self: Fence) bool {
            return self.slot != null;
        }
        fn retentionSequence(self: Fence) u64 {
            return self.sequence;
        }
    };
    pub fn init(allocator: std.mem.Allocator, io: std.Io) Coordinator {
        return .{ .allocator = allocator, .io = io };
    }
    pub fn create(allocator: std.mem.Allocator, io: std.Io) !*Coordinator {
        const self = try allocator.create(Coordinator);
        self.* = init(allocator, io);
        self.heap_owned = true;
        return self;
    }
    pub fn release(self: *Coordinator) void {
        if (self.references.fetchSub(1, .acq_rel) != 1) return;
        std.debug.assert(self.heap_owned);
        const allocator = self.allocator;
        self.deinit();
        allocator.destroy(self);
    }
    pub fn deinit(self: *Coordinator) void {
        std.debug.assert(self.readers.items.len == 0);
        std.debug.assert(self.registration_batches == 0);
        self.readers.deinit(self.allocator);
    }
    /// The descriptor already holds its kernel shared lock. Registration may
    /// wait for a writer, which then treats this unregistered lock as external.
    pub fn register(self: *Coordinator, file: std.Io.File, region: ?*shared_lock_region.Region, inode: u64, sequence: u64, tip: [32]u8) !*Fence {
        return self.registerEpoch(file, region, inode, sequence, tip, false);
    }
    pub fn registerFull(self: *Coordinator, file: std.Io.File, region: ?*shared_lock_region.Region, inode: u64, sequence: u64, tip: [32]u8) !*Fence {
        return self.registerEpoch(file, region, inode, sequence, tip, true);
    }
    pub fn registerInodeOnly(self: *Coordinator, file: std.Io.File, inode: u64, sequence: u64, tip: [32]u8) !*Fence {
        return self.registerEpoch(file, null, inode, sequence, tip, false);
    }
    /// Claims a region reader slot and pins it at `sequence` (and, if
    /// `metadata`, pins the slot's metadata companion too), or registers an
    /// inode-only fence when `region` is null or its slot table is
    /// exhausted. The caller (`NativeFile.acquireReaderFence`) decides
    /// `region`: typically the current region for this inode, or `null`
    /// when it is unusable for this open (`inode_lock.isRegionUnusable`) or
    /// simply doesn't apply (no registry entry at all).
    pub fn registerEpoch(self: *Coordinator, file: std.Io.File, region: ?*shared_lock_region.Region, inode: u64, sequence: u64, tip: [32]u8, metadata: bool) !*Fence {
        var slot: ?u32 = null;
        if (region) |r| {
            const pid: u32 = @bitCast(std.c.getpid());
            slot = r.claimReaderSlot(pid) catch null;
            if (slot) |s| {
                // The region's own convention (shared_lock_region.zig's
                // `ReaderSlot.pinned_sequence` doc) is that 0 means "not
                // pinned" and real sequence numbers start above 0. A
                // caller can legitimately compute `sequence == 0` here
                // (e.g. `metadataReuseFrontier()` mixing in a pristine,
                // never-written checkpoint slot before this database's
                // first real commit) meaning "protect everything, there
                // is no floor yet" -- the most conservative frontier, not
                // "nothing to protect." Floor it at 1 so that intent is
                // never silently indistinguishable from an absent pin.
                const pinned_sequence = @max(sequence, 1);
                r.pinReader(s, pinned_sequence);
                if (metadata) r.pinMetadataReader(s, pinned_sequence);
            }
        }
        errdefer if (slot) |s| if (region) |r| {
            if (metadata) r.unpinMetadataReader(s);
            r.unpinReader(s);
            r.releaseReaderSlot(s);
        };
        return self.registerWithScope(file, inode, sequence, tip, metadata, if (slot != null) region else null, slot);
    }
    fn registerWithScope(self: *Coordinator, file: std.Io.File, inode: u64, sequence: u64, tip: [32]u8, metadata: bool, region: ?*shared_lock_region.Region, slot: ?u32) !*Fence {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        const fence = try self.allocator.create(Fence);
        errdefer self.allocator.destroy(fence);
        // The shared lock may have blocked physical reuse before registration,
        // while an external writer promoted retirement. A delayed old reader
        // must never become eligible merely by arriving after that detection.
        const stale = slot == null and if (self.known) |known| known.inode != inode or !std.mem.eql(u8, &known.tip, &tip) else false;
        fence.* = .{ .file = file, .inode = inode, .sequence = sequence, .captured_at_ns = std.Io.Clock.awake.now(self.io).nanoseconds, .metadata = metadata, .poisoned = stale, .inode_only = slot == null, .region = region, .slot = slot };
        try self.readers.append(self.allocator, fence);
        _ = self.references.fetchAdd(1, .monotonic);
        if (self.known == null) self.known = .{ .inode = inode, .tip = tip };
        return fence;
    }
    pub fn unregister(self: *Coordinator, fence: *Fence) void {
        self.mutex.lockUncancelable(self.io);
        for (self.readers.items, 0..) |item, i| if (item == fence) {
            _ = self.readers.swapRemove(i);
            const region = fence.region;
            const slot = fence.slot;
            const metadata = fence.metadata;
            self.allocator.destroy(fence);
            self.mutex.unlock(self.io);
            if (region) |r| if (slot) |s| {
                r.unpinReader(s);
                if (metadata) r.unpinMetadataReader(s);
                r.releaseReaderSlot(s);
            };
            self.release();
            return;
        };
        unreachable;
    }
    /// The path-writer lease must enclose this entire window, including
    /// restoration after exclusive reuse. Do not retain mutex across caller
    /// allocations: reclaiming an idle artifact may unregister its descriptor.
    pub fn begin(self: *Coordinator, inode: u64, tip: [32]u8, reuse: bool, region: ?*shared_lock_region.Region) ?u64 {
        return self.beginWithMetadata(inode, tip, reuse, region).data;
    }
    pub const Frontier = struct { data: ?u64 = null, metadata: ?u64 = null };
    pub const ReaderStats = struct {
        frontier: Frontier = .{},
        count: u64 = 0,
        poisoned: u64 = 0,
        oldest_age_ms: u64 = 0,
    };
    /// Observation only: never suspend locks or advance the known tip.
    /// `region` is optional purely so a caller with no live region handy
    /// (e.g. a status probe that hasn't opened one) still gets the
    /// in-process (inode-only) contribution rather than erroring.
    pub fn readerStats(self: *Coordinator, inode: u64, region: ?*shared_lock_region.Region) ReaderStats {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        var result: ReaderStats = .{};
        const now = std.Io.Clock.awake.now(self.io).nanoseconds;
        if (region) |r| {
            result.frontier.data = r.minimumPinnedSequence();
            result.frontier.metadata = r.minimumMetadataPinnedSequence();
        }
        for (self.readers.items) |fence| if (fence.inode == inode) {
            result.count += 1;
            result.poisoned += @intFromBool(fence.poisoned);
            // A region-backed fence's contribution is already counted by
            // the scan above, on every process mapping the region, not
            // just this one; only the inode-only fallback needs this
            // process's own in-memory list.
            if (!fence.hasEpoch()) {
                result.frontier.data = @min(result.frontier.data orelse fence.retentionSequence(), fence.retentionSequence());
                if (fence.metadata) result.frontier.metadata = @min(result.frontier.metadata orelse fence.retentionSequence(), fence.retentionSequence());
            }
            result.oldest_age_ms = @max(result.oldest_age_ms, @as(u64, @intCast(@divTrunc(@max(0, now - fence.captured_at_ns), std.time.ns_per_ms))));
        };
        return result;
    }

    /// `region` is the current region for `inode` (or `null` if
    /// unavailable); passed in rather than stored on the `Coordinator`
    /// because one coordinator's lifetime can outlive a vacuum generation
    /// swap, which can hand a later call a different region than an
    /// earlier one saw for what is, by inode, a different file underneath
    /// the same logical database.
    pub fn beginWithMetadata(self: *Coordinator, inode: u64, tip: [32]u8, reuse: bool, region: ?*shared_lock_region.Region) Frontier {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        self.windows += 1;
        if (self.known) |known| {
            if (known.inode != inode or !std.mem.eql(u8, &known.tip, &tip)) {
                for (self.readers.items) |fence| if (fence.inode == inode and !fence.hasEpoch()) {
                    fence.poisoned = true;
                };
            }
        }
        self.known = .{ .inode = inode, .tip = tip };
        var frontier: Frontier = .{};
        if (region) |r| {
            frontier.data = r.minimumPinnedSequence();
            frontier.metadata = r.minimumMetadataPinnedSequence();
        }
        var can_suspend = reuse;
        for (self.readers.items) |fence| if (fence.inode == inode and !fence.hasEpoch()) {
            // Only an inode-only fence needs an in-process contribution
            // here -- a region-backed one was already counted above, by
            // any process, not just this one.
            frontier.data = @min(frontier.data orelse fence.retentionSequence(), fence.retentionSequence());
            if (fence.metadata) frontier.metadata = @min(frontier.metadata orelse fence.retentionSequence(), fence.retentionSequence());
            can_suspend = can_suspend and !fence.poisoned;
        };
        if (can_suspend and self.windows == 1) for (self.readers.items) |fence| if (fence.inode == inode and !fence.hasEpoch()) {
            fence.file.unlock(self.io);
            fence.suspended = true;
        };
        return frontier;
    }
    pub fn end(self: *Coordinator) void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        std.debug.assert(self.windows != 0);
        self.windows -= 1;
        if (self.windows != 0) return;
        // Losing a restored lock would expose live readers after the path
        // lease is released. Fail closed on an unexpected kernel-lock error.
        const protection = self.io.swapCancelProtection(.blocked);
        defer _ = self.io.swapCancelProtection(protection);
        for (self.readers.items) |fence| if (fence.suspended) {
            fence.file.lock(self.io, .shared) catch @panic("could not restore Lite reader fence");
            fence.suspended = false;
        };
    }
    /// Caller holds the path-writer lease and has read the current header.
    /// Fence readers captured before an unrecognized publication, then admit
    /// newly captured readers against this authoritative tip. Waiting until a
    /// writer window begins would poison those fresh readers too.
    pub fn observeCurrent(self: *Coordinator, inode: u64, tip: [32]u8) void {
        self.published(inode, tip, tip, false);
    }

    pub fn published(self: *Coordinator, inode: u64, previous_tip: [32]u8, tip: [32]u8, uncertain: bool) void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        const changed = if (self.known) |known| known.inode != inode or !std.mem.eql(u8, &known.tip, &previous_tip) else false;
        if (uncertain or changed) for (self.readers.items) |fence| if (fence.inode == inode and !fence.hasEpoch()) {
            fence.poisoned = true;
        };
        self.known = .{ .inode = inode, .tip = tip };
    }
};

const RegistrationFixture = struct {
    tmp: std.testing.TmpDir,
    path: []u8,
    data: std.Io.File,
    inode: u64,
    region: shared_lock_region.Region,
    coordinator: Coordinator,
    const Reader = struct {
        owner: *Coordinator,
        fence: ?*Coordinator.Fence,
        fn close(self: *Reader) void {
            if (self.fence) |fence| {
                const file = fence.file;
                self.owner.unregister(fence);
                file.close(std.testing.io);
                self.fence = null;
            }
        }
    };
    fn init() !RegistrationFixture {
        var tmp = std.testing.tmpDir(.{});
        errdefer tmp.cleanup();
        const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/registration.aflite", .{tmp.sub_path});
        errdefer std.testing.allocator.free(path);
        const data = try std.Io.Dir.cwd().createFile(std.testing.io, path, .{ .read = true });
        errdefer data.close(std.testing.io);
        const region = try shared_lock_region.Region.open(std.testing.allocator, std.testing.io, path);
        return .{ .tmp = tmp, .path = path, .data = data, .inode = (try data.stat(std.testing.io)).inode, .region = region, .coordinator = Coordinator.init(std.testing.allocator, std.testing.io) };
    }
    fn deinit(self: *RegistrationFixture) void {
        self.coordinator.deinit();
        self.region.close();
        self.data.close(std.testing.io);
        std.testing.allocator.free(self.path);
        self.tmp.cleanup();
    }
    fn reader(self: *RegistrationFixture, sequence: u64, metadata: bool) !Reader {
        const file = try std.Io.Dir.cwd().openFile(std.testing.io, self.path, .{});
        errdefer file.close(std.testing.io);
        try file.lock(std.testing.io, .shared);
        const fence = self.coordinator.registerEpoch(file, &self.region, self.inode, sequence, @splat(0), metadata) catch |err| {
            file.unlock(std.testing.io);
            return err;
        };
        return .{ .owner = &self.coordinator, .fence = fence };
    }
    fn readerWithoutRegion(self: *RegistrationFixture, sequence: u64) !Reader {
        const file = try std.Io.Dir.cwd().openFile(std.testing.io, self.path, .{});
        errdefer file.close(std.testing.io);
        try file.lock(std.testing.io, .shared);
        const fence = self.coordinator.registerInodeOnly(file, self.inode, sequence, @splat(0)) catch |err| {
            file.unlock(std.testing.io);
            return err;
        };
        return .{ .owner = &self.coordinator, .fence = fence };
    }
};

test "lite reader registration claims a region slot per reader and the frontier tracks the minimum pin" {
    var fixture = try RegistrationFixture.init();
    defer fixture.deinit();
    try std.testing.expectEqual(@as(?u64, null), fixture.region.minimumPinnedSequence());

    var first = try fixture.reader(10, false);
    defer first.close();
    var second = try fixture.reader(30, false);
    defer second.close();
    try std.testing.expect(first.fence.?.slot != null);
    try std.testing.expect(second.fence.?.slot != null);
    try std.testing.expect(first.fence.?.slot != second.fence.?.slot);
    try std.testing.expectEqual(@as(?u64, 10), fixture.region.minimumPinnedSequence());
    try std.testing.expectEqual(@as(?u64, 10), fixture.coordinator.readerStats(fixture.inode, &fixture.region).frontier.data);

    first.close();
    try std.testing.expectEqual(@as(?u64, 30), fixture.region.minimumPinnedSequence());
    second.close();
    try std.testing.expectEqual(@as(?u64, null), fixture.region.minimumPinnedSequence());
}

test "lite reader registration metadata pin is independent of the data frontier" {
    var fixture = try RegistrationFixture.init();
    defer fixture.deinit();

    var plain = try fixture.reader(20, false);
    defer plain.close();
    var full = try fixture.reader(10, true);
    defer full.close();
    try std.testing.expectEqual(@as(?u64, 10), fixture.region.minimumPinnedSequence());
    try std.testing.expectEqual(@as(?u64, 10), fixture.region.minimumMetadataPinnedSequence());
    try std.testing.expectEqual(@as(?u64, 10), fixture.coordinator.readerStats(fixture.inode, &fixture.region).frontier.metadata);

    full.close();
    try std.testing.expectEqual(@as(?u64, 20), fixture.region.minimumPinnedSequence());
    try std.testing.expectEqual(@as(?u64, null), fixture.region.minimumMetadataPinnedSequence());
}

test "lite reader registration falls back to inode-only fencing when no region is available" {
    var fixture = try RegistrationFixture.init();
    defer fixture.deinit();
    var fallback = try fixture.readerWithoutRegion(20);
    defer fallback.close();
    try std.testing.expect(fallback.fence.?.inode_only);
    try std.testing.expect(fallback.fence.?.slot == null);
    // No region slot was claimed, so the region's own frontier sees
    // nothing -- the inode-only fence's protection is the kernel lock
    // alone, exactly as `writer_queue.zig`'s pre-region posture.
    try std.testing.expectEqual(@as(?u64, null), fixture.region.minimumPinnedSequence());
    try std.testing.expect(!try fixture.data.tryLock(std.testing.io, .exclusive));
    try std.testing.expectEqual(@as(?u64, 20), fixture.coordinator.readerStats(fixture.inode, &fixture.region).frontier.data);
}

test "lite reader registration begin/end windows combine region and inode-only frontiers and poison stale inode-only fences" {
    var fixture = try RegistrationFixture.init();
    defer fixture.deinit();
    var region_backed = try fixture.reader(15, false);
    defer region_backed.close();
    var inode_only = try fixture.readerWithoutRegion(25);
    defer inode_only.close();

    const frontier = fixture.coordinator.beginWithMetadata(fixture.inode, @splat(0), false, &fixture.region);
    defer fixture.coordinator.end();
    try std.testing.expectEqual(@as(?u64, 15), frontier.data);
    try std.testing.expect(!inode_only.fence.?.poisoned);

    // An uncertain publication poisons every inode-only fence on this
    // inode (region-backed ones are unaffected -- their protection
    // doesn't depend on the coordinator's own notion of "known tip" at
    // all).
    fixture.coordinator.published(fixture.inode, @splat(0), @splat(0), true);
    try std.testing.expect(inode_only.fence.?.poisoned);
    try std.testing.expect(!region_backed.fence.?.poisoned);
}

test "lite reader registration allocation failures release every fence and claimed slot" {
    const Exercise = struct {
        fn run(allocator: std.mem.Allocator) !void {
            var fixture = try RegistrationFixture.init();
            defer fixture.deinit();
            fixture.coordinator.allocator = allocator;
            var data = try fixture.reader(10, false);
            defer data.close();
            var shared = try fixture.reader(10, false);
            defer shared.close();
            var metadata = try fixture.reader(10, true);
            defer metadata.close();
            var next = try fixture.reader(20, false);
            defer next.close();
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Exercise.run, .{});
}

test "lite reader registration batch is a stable no-op that still balances its own refcount" {
    var fixture = try RegistrationFixture.init();
    defer fixture.deinit();
    const batch = fixture.coordinator.beginRegistrationBatch();
    var reader = try fixture.reader(10, false);
    defer reader.close();
    // Registering while a batch is open still claims this reader's own
    // slot directly -- the batch shares nothing anymore.
    try std.testing.expectEqual(@as(?u64, 10), fixture.region.minimumPinnedSequence());
    batch.end();
    reader.close();
    try std.testing.expectEqual(@as(?u64, null), fixture.region.minimumPinnedSequence());
}
