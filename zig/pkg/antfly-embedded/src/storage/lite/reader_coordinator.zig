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

//! Reader fence lifetimes. Revision-5 epochs protect snapshots across processes;
//! legacy inode fences can be suspended only by their own trusted coordinator.
//! A writer's exclusive data lock fences epoch registration and page reuse.
const std = @import("std");
const epochs = @import("reader_epochs.zig");
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
    registration_batch_id: u64 = 0,
    batch_data_epoch: ?*SharedEpoch = null,
    batch_metadata_epoch: ?*SharedEpoch = null,

    known: ?struct { inode: u64, tip: [32]u8 } = null,

    const SharedEpoch = struct {
        lease: epochs.Lease,
        references: usize,
        path: []u8,
        binding: [32]u8,
        inode: u64,
        sequence: u64,
        captured_at_ns: i96,
    };

    /// Amortize marker publication while an admission lease is held.
    /// Data and metadata epochs remain separate. A surviving reader retains
    /// its marker after the batch ends. Ending the batch drops idle anchors.
    pub const RegistrationBatch = struct {
        coordinator: *Coordinator,
        pub fn end(self: RegistrationBatch) void {
            const owner = self.coordinator;
            owner.mutex.lockUncancelable(owner.io);
            std.debug.assert(owner.registration_batches != 0);
            owner.registration_batches -= 1;
            var data: ?*SharedEpoch = null;
            var metadata: ?*SharedEpoch = null;
            if (owner.registration_batches == 0) {
                data = owner.batch_data_epoch;
                metadata = owner.batch_metadata_epoch;
                owner.batch_data_epoch = null;
                owner.batch_metadata_epoch = null;
            }
            owner.mutex.unlock(owner.io);
            if (data) |epoch| owner.releaseSharedEpoch(epoch);
            if (metadata) |epoch| owner.releaseSharedEpoch(epoch);
            owner.release();
        }
    };

    pub fn beginRegistrationBatch(self: *Coordinator) RegistrationBatch {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        if (self.registration_batches == 0) self.registration_batch_id +%= 1;
        self.registration_batches += 1;
        _ = self.references.fetchAdd(1, .monotonic);
        return .{ .coordinator = self };
    }

    fn releaseSharedEpoch(self: *Coordinator, epoch: *SharedEpoch) void {
        self.mutex.lockUncancelable(self.io);
        std.debug.assert(epoch.references != 0);
        epoch.references -= 1;
        const last = epoch.references == 0;
        self.mutex.unlock(self.io);
        if (last) {
            epoch.lease.close(self.allocator, self.io);
            self.allocator.free(epoch.path);
            self.allocator.destroy(epoch);
        }
    }

    fn sharedEpoch(self: *Coordinator, path: []const u8, binding: [32]u8, inode: u64, sequence: u64, metadata: bool) ?*SharedEpoch {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        if (self.registration_batches == 0) return null;
        const epoch = (if (metadata) self.batch_metadata_epoch else self.batch_data_epoch) orelse return null;
        // A batch cannot borrow protection from a different inode, namespace,
        // scope, or checkpoint. Exact paths avoid alias assumptions here.
        if (epoch.inode != inode or epoch.sequence != sequence or
            !std.mem.eql(u8, &epoch.binding, &binding) or !std.mem.eql(u8, epoch.path, path)) return null;
        epoch.references += 1;
        return epoch;
    }

    fn shareNewEpoch(self: *Coordinator, lease: epochs.Lease, path: []const u8, binding: [32]u8, inode: u64, sequence: u64, metadata: bool) !?*SharedEpoch {
        self.mutex.lockUncancelable(self.io);
        const batch_id = self.registration_batch_id;
        const wanted = self.registration_batches != 0;
        self.mutex.unlock(self.io);
        if (!wanted) return null;
        const epoch = try self.allocator.create(SharedEpoch);
        errdefer self.allocator.destroy(epoch);
        const owned_path = try self.allocator.dupe(u8, path);
        self.mutex.lockUncancelable(self.io);
        const slot = if (metadata) &self.batch_metadata_epoch else &self.batch_data_epoch;
        const previous = slot.*;
        const keep_previous = if (previous) |old| old.inode == inode and old.sequence >= sequence and
            std.mem.eql(u8, &old.binding, &binding) and std.mem.eql(u8, old.path, path) else false;
        if (self.registration_batches == 0 or self.registration_batch_id != batch_id or keep_previous) {
            self.mutex.unlock(self.io);
            self.allocator.free(owned_path);
            self.allocator.destroy(epoch);
            return null;
        }
        epoch.* = .{ .lease = lease, .references = 2, .path = owned_path, .binding = binding, .inode = inode, .sequence = sequence, .captured_at_ns = std.Io.Clock.awake.now(self.io).nanoseconds };
        slot.* = epoch;
        self.mutex.unlock(self.io);
        if (previous) |old| self.releaseSharedEpoch(old);
        return epoch;
    }

    pub const Fence = struct {
        file: std.Io.File,
        inode: u64,
        sequence: u64,
        captured_at_ns: i96 = 0,
        /// Full graph scans also retain allocator/recovery metadata pages.
        metadata: bool = false,
        poisoned: bool = false,
        inode_only: bool = false,
        suspended: bool = false,
        epoch: ?epochs.Lease = null,
        shared_epoch: ?*SharedEpoch = null,
        fn hasEpoch(self: Fence) bool {
            return self.epoch != null or self.shared_epoch != null;
        }
        fn retentionSequence(self: Fence) u64 {
            return if (self.shared_epoch) |epoch| epoch.sequence else self.sequence;
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
        std.debug.assert(self.registration_batches == 0 and self.batch_data_epoch == null and self.batch_metadata_epoch == null);
        self.readers.deinit(self.allocator);
    }
    /// The descriptor already holds its kernel shared lock. Registration may
    /// wait for a writer, which then treats this unregistered lock as external.
    pub fn register(self: *Coordinator, file: std.Io.File, inode: u64, sequence: u64, tip: [32]u8) !*Fence {
        return self.registerWithScope(file, inode, sequence, tip, false, null, false, null);
    }
    pub fn registerFull(self: *Coordinator, file: std.Io.File, inode: u64, sequence: u64, tip: [32]u8) !*Fence {
        return self.registerWithScope(file, inode, sequence, tip, true, null, false, null);
    }
    pub fn registerInodeOnly(self: *Coordinator, file: std.Io.File, inode: u64, sequence: u64, tip: [32]u8) !*Fence {
        return self.registerWithScope(file, inode, sequence, tip, false, null, true, null);
    }
    /// The caller holds the data inode shared lock through registration.
    pub fn registerEpoch(self: *Coordinator, file: std.Io.File, path: []const u8, binding: [32]u8, inode: u64, sequence: u64, tip: [32]u8, metadata: bool) !*Fence {
        if (self.sharedEpoch(path, binding, inode, sequence, metadata)) |shared| {
            errdefer self.releaseSharedEpoch(shared);
            const fence = try self.registerWithScope(file, inode, sequence, tip, metadata, null, false, shared);
            file.unlock(self.io);
            return fence;
        }
        const epoch = epochs.register(self.allocator, self.io, path, binding, inode, sequence, metadata) catch |err| switch (err) {
            error.AccessDenied, error.ReadOnlyFileSystem, error.UnsafeNativeReaderAlias => return self.registerWithScope(file, inode, sequence, tip, metadata, null, true, null),
            else => return err,
        };
        const shared = self.shareNewEpoch(epoch, path, binding, inode, sequence, metadata) catch |err| {
            epoch.close(self.allocator, self.io);
            return err;
        };
        errdefer if (shared) |retained| self.releaseSharedEpoch(retained) else epoch.close(self.allocator, self.io);
        const fence = try self.registerWithScope(file, inode, sequence, tip, metadata, if (shared == null) epoch else null, false, shared);
        file.unlock(self.io);
        return fence;
    }
    fn registerWithScope(self: *Coordinator, file: std.Io.File, inode: u64, sequence: u64, tip: [32]u8, metadata: bool, epoch: ?epochs.Lease, inode_only: bool, shared: ?*SharedEpoch) !*Fence {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        const fence = try self.allocator.create(Fence);
        errdefer self.allocator.destroy(fence);
        // The shared lock may have blocked physical reuse before registration,
        // while an external writer promoted retirement. A delayed old reader
        // must never become eligible merely by arriving after that detection.
        const stale = epoch == null and shared == null and if (self.known) |known| known.inode != inode or !std.mem.eql(u8, &known.tip, &tip) else false;
        fence.* = .{ .file = file, .inode = inode, .sequence = sequence, .captured_at_ns = std.Io.Clock.awake.now(self.io).nanoseconds, .metadata = metadata, .poisoned = stale, .epoch = epoch, .inode_only = inode_only, .shared_epoch = shared };
        try self.readers.append(self.allocator, fence);
        _ = self.references.fetchAdd(1, .monotonic);
        if (self.known == null) self.known = .{ .inode = inode, .tip = tip };
        return fence;
    }
    pub fn unregister(self: *Coordinator, fence: *Fence) void {
        self.mutex.lockUncancelable(self.io);
        for (self.readers.items, 0..) |item, i| if (item == fence) {
            _ = self.readers.swapRemove(i);
            const epoch = fence.epoch;
            const shared = fence.shared_epoch;
            self.allocator.destroy(fence);
            self.mutex.unlock(self.io);
            if (epoch) |owned| owned.close(self.allocator, self.io);
            if (shared) |retained| self.releaseSharedEpoch(retained);
            self.release();
            return;
        };
        unreachable;
    }
    /// The path-writer lease must enclose this entire window, including
    /// restoration after exclusive reuse. Do not retain mutex across caller
    /// allocations: reclaiming an idle artifact may unregister its descriptor.
    pub fn begin(self: *Coordinator, inode: u64, tip: [32]u8, reuse: bool) ?u64 {
        return self.beginWithMetadata(inode, tip, reuse).data;
    }
    pub const Frontier = struct { data: ?u64 = null, metadata: ?u64 = null };
    pub const ReaderStats = struct {
        frontier: Frontier = .{},
        count: u64 = 0,
        poisoned: u64 = 0,
        oldest_age_ms: u64 = 0,
    };
    /// Observation only: never suspend locks or advance the known tip.
    pub fn readerStats(self: *Coordinator, inode: u64) ReaderStats {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        var result: ReaderStats = .{};
        const now = std.Io.Clock.awake.now(self.io).nanoseconds;
        self.includeBatchFrontiers(inode, &result.frontier);
        for (self.readers.items) |fence| if (fence.inode == inode) {
            result.count += 1;
            result.poisoned += @intFromBool(fence.poisoned);
            result.frontier.data = @min(result.frontier.data orelse fence.retentionSequence(), fence.retentionSequence());
            if (fence.metadata) result.frontier.metadata = @min(result.frontier.metadata orelse fence.retentionSequence(), fence.retentionSequence());
            result.oldest_age_ms = @max(result.oldest_age_ms, @as(u64, @intCast(@divTrunc(@max(0, now - if (fence.shared_epoch) |shared| shared.captured_at_ns else fence.captured_at_ns), std.time.ns_per_ms))));
        };
        return result;
    }
    fn includeBatchFrontiers(self: *Coordinator, inode: u64, frontier: *Frontier) void {
        for ([_]?*SharedEpoch{ self.batch_data_epoch, self.batch_metadata_epoch }, 0..) |maybe, kind| {
            const epoch = maybe orelse continue;
            if (epoch.inode != inode) continue;
            frontier.data = @min(frontier.data orelse epoch.sequence, epoch.sequence);
            if (kind == 1) frontier.metadata = @min(frontier.metadata orelse epoch.sequence, epoch.sequence);
        }
    }

    pub fn beginWithMetadata(self: *Coordinator, inode: u64, tip: [32]u8, reuse: bool) Frontier {
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
        self.includeBatchFrontiers(inode, &frontier);
        var can_suspend = reuse;
        for (self.readers.items) |fence| if (fence.inode == inode) {
            frontier.data = @min(frontier.data orelse fence.retentionSequence(), fence.retentionSequence());
            if (fence.metadata) frontier.metadata = @min(frontier.metadata orelse fence.retentionSequence(), fence.retentionSequence());
            if (!fence.hasEpoch()) can_suspend = can_suspend and !fence.poisoned and !fence.inode_only;
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
    binding: [32]u8,
    inode: u64,
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
        return .{ .tmp = tmp, .path = path, .data = data, .binding = try epochs.namespaceForPublication(std.testing.allocator, std.testing.io, path), .inode = (try data.stat(std.testing.io)).inode, .coordinator = Coordinator.init(std.testing.allocator, std.testing.io) };
    }
    fn deinit(self: *RegistrationFixture) void {
        self.coordinator.deinit();
        self.data.close(std.testing.io);
        std.testing.allocator.free(self.path);
        self.tmp.cleanup();
    }
    fn reader(self: *RegistrationFixture, sequence: u64, metadata: bool, binding: [32]u8) !Reader {
        const file = try std.Io.Dir.cwd().openFile(std.testing.io, self.path, .{});
        errdefer file.close(std.testing.io);
        try file.lock(std.testing.io, .shared);
        return .{ .owner = &self.coordinator, .fence = try self.coordinator.registerEpoch(file, self.path, binding, self.inode, sequence, @splat(0), metadata) };
    }
    fn scan(self: *RegistrationFixture) !epochs.Frontier {
        return epochs.scan(std.testing.allocator, std.testing.io, self.path, self.binding, self.inode);
    }
};

test "lite reader registration batch shares markers and preserves escaped reader scopes" {
    var fixture = try RegistrationFixture.init();
    defer fixture.deinit();
    const batch = fixture.coordinator.beginRegistrationBatch();
    var active = true;
    defer if (active) batch.end();
    var first = try fixture.reader(10, false, fixture.binding);
    defer first.close();
    var second = try fixture.reader(10, false, fixture.binding);
    defer second.close();
    var full = try fixture.reader(30, true, fixture.binding);
    defer full.close();
    try std.testing.expect(first.fence.?.shared_epoch != null);
    try std.testing.expectEqual(first.fence.?.shared_epoch, second.fence.?.shared_epoch);
    try std.testing.expect(first.fence.?.shared_epoch != full.fence.?.shared_epoch);
    try std.testing.expectEqual(epochs.Frontier{ .data = 10, .metadata = 30 }, try fixture.scan());
    full.close();
    batch.end();
    active = false;
    try std.testing.expectEqual(epochs.Frontier{ .data = 10 }, try fixture.scan());
    first.close();
    // The remaining reader keeps the original immutable marker alive.
    try std.testing.expectEqual(epochs.Frontier{ .data = 10 }, try fixture.scan());
    try std.testing.expectEqual(@as(?u64, 10), fixture.coordinator.readerStats(fixture.inode).frontier.data);
    second.close();
    try std.testing.expectEqual(epochs.Frontier{}, try fixture.scan());
}

test "lite reader registration batches never reuse newer or previous-batch protection" {
    var fixture = try RegistrationFixture.init();
    defer fixture.deinit();
    const batch = fixture.coordinator.beginRegistrationBatch();
    var active = true;
    defer if (active) batch.end();
    var first = try fixture.reader(20, false, fixture.binding);
    defer first.close();
    var older = try fixture.reader(10, false, fixture.binding);
    defer older.close();
    try std.testing.expect(older.fence.?.epoch != null);
    try std.testing.expect(older.fence.?.shared_epoch == null);
    try std.testing.expectEqual(@as(?u64, 10), (try fixture.scan()).data);
    older.close();
    batch.end();
    active = false;
    const next = fixture.coordinator.beginRegistrationBatch();
    defer next.end();
    var newer = try fixture.reader(50, false, fixture.binding);
    defer newer.close();
    try std.testing.expect(first.fence.?.shared_epoch != newer.fence.?.shared_epoch);
    first.close();
    try std.testing.expectEqual(@as(?u64, 50), (try fixture.scan()).data);
}

test "lite reader registration batch namespace mismatch retains inode fencing" {
    var fixture = try RegistrationFixture.init();
    defer fixture.deinit();
    const batch = fixture.coordinator.beginRegistrationBatch();
    defer batch.end();
    var first = try fixture.reader(10, false, fixture.binding);
    defer first.close();
    var wrong = fixture.binding;
    wrong[0] ^= 1;
    var fallback = try fixture.reader(20, false, wrong);
    defer fallback.close();
    try std.testing.expect(fallback.fence.?.inode_only);
    try std.testing.expect(fallback.fence.?.epoch == null and fallback.fence.?.shared_epoch == null);
    try std.testing.expect(!try fixture.data.tryLock(std.testing.io, .exclusive));
}

test "lite reader registration batch without live transactions retains its advertised frontier only until scope end" {
    var fixture = try RegistrationFixture.init();
    defer fixture.deinit();
    const batch = fixture.coordinator.beginRegistrationBatch();
    var active = true;
    defer if (active) batch.end();
    var reader = try fixture.reader(10, false, fixture.binding);
    defer reader.close();
    reader.close();
    try std.testing.expectEqual(@as(?u64, 10), fixture.coordinator.readerStats(fixture.inode).frontier.data);
    try std.testing.expectEqual(@as(?u64, 10), fixture.coordinator.beginWithMetadata(fixture.inode, @splat(0), true).data);
    fixture.coordinator.end();
    batch.end();
    active = false;
    try std.testing.expectEqual(@as(?u64, null), fixture.coordinator.readerStats(fixture.inode).frontier.data);
    try std.testing.expectEqual(epochs.Frontier{}, try fixture.scan());
}

test "lite reader registration batch rotates idle anchors without extending older reader lifetimes" {
    var fixture = try RegistrationFixture.init();
    defer fixture.deinit();
    const batch = fixture.coordinator.beginRegistrationBatch();
    defer batch.end();
    var old = try fixture.reader(10, false, fixture.binding);
    defer old.close();
    var current = try fixture.reader(20, false, fixture.binding);
    defer current.close();
    try std.testing.expect(old.fence.?.shared_epoch != current.fence.?.shared_epoch);
    try std.testing.expectEqual(@as(?u64, 10), (try fixture.scan()).data);
    old.close();
    try std.testing.expectEqual(@as(?u64, 20), (try fixture.scan()).data);
    current.close();
    var next = try fixture.reader(30, false, fixture.binding);
    defer next.close();
    try std.testing.expectEqual(@as(?u64, 30), (try fixture.scan()).data);
}

test "lite reader registration batch allocation failures release every fence and marker" {
    const Exercise = struct {
        fn run(allocator: std.mem.Allocator) !void {
            var fixture = try RegistrationFixture.init();
            defer fixture.deinit();
            fixture.coordinator.allocator = allocator;
            const batch = fixture.coordinator.beginRegistrationBatch();
            defer batch.end();
            var data = try fixture.reader(10, false, fixture.binding);
            defer data.close();
            var shared = try fixture.reader(10, false, fixture.binding);
            defer shared.close();
            var metadata = try fixture.reader(10, true, fixture.binding);
            defer metadata.close();
            var next = try fixture.reader(20, false, fixture.binding);
            defer next.close();
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Exercise.run, .{});
}
