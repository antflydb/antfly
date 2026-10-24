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

//! Backend-owned prepared roots through durable descriptor installation.
const std = @import("std");
const admission = @import("../../common/read_admission.zig");
const repository = @import("repository.zig");
const manifest = @import("../lsm/manifest.zig");
const Snapshot = @import("ledger_reclamation.zig").Snapshot;
const Directory = @import("run_directory.zig").Directory;
const storage_io = @import("storage_io.zig");
const bounded = @import("../../common/bounded_mutex.zig");

/// Caller permanently fences unrelated serving, mutation and accounting for
/// this continuation. Prepared directory/snapshot are already registered in
/// the backend and funded by its existing observer; no heap allocation or new
/// application budget occurs here. The caller also prepays memoryBound() and
/// retains provider funding through resource retirement or grant transfer.
/// The caller retains cleanup tickets for every possibly published final path
/// reported by physical.attempted/paths, including after abort/retirement.
///
/// Preparation must include every retired manifest path in obsolete_paths.
/// Successful initialization consumes the prepared directory, snapshot and
/// grant. Failed initialization leaves them with the caller. Keep this owner's
/// address, backend, metadata and allocator authorities stable until retirement.
pub fn Publication(comptime BackendType: type) type {
    return struct {
        const Source = struct {
            cursor: Directory.Cursor,
            ledger: repository.ObsoleteLedger.Cursor,
            count: usize,
            root: []const u8,
            pub fn runCount(self: *@This()) usize {
                return self.cursor.directory.count();
            }
            pub fn obsoleteCount(self: *@This()) usize {
                return self.count;
            }
            pub fn nextRun(self: *@This()) ?manifest.RunMeta {
                var meta = repository.runMeta((self.cursor.next() orelse return null).run.*);
                meta.path = repository.manifestRelativePath(self.root, meta.path);
                return meta;
            }
            pub fn nextObsolete(self: *@This()) ?manifest.ObsoletePathMeta {
                const path = self.ledger.next() orelse return null;
                return .{ .path = repository.manifestRelativePath(self.root, path.path), .delete_after_ns = path.delete_after_ns };
            }
        };
        pub const Physical = @import("owned_manifest_set.zig").Publication(*Source);
        backend: *BackendType,
        source: Source,
        physical: Physical = undefined,
        fence: repository.ManifestJournal.CheckpointFence,
        directory: ?*Directory,
        captured_directory: *Directory,
        snapshot: ?*Snapshot,
        turn: ?BackendType.ManifestTurn = null,
        id: u64,
        next_run_id: u64,
        started: u64,
        covered_logical_bytes: u64,
        checked_paths: usize = 0,
        descriptor_permitted: bool = false,
        installed: bool = false,
        detached: bool = false,
        aborting: bool = false,
        retired: bool = false,
        failure: ?anyerror = null,
        path: [std.fs.max_path_bytes]u8 = undefined,

        const Self = @This();
        pub fn memoryBound(storage: storage_io.Storage) !u64 {
            return std.math.add(u64, @sizeOf(Self), try storage.appendResourceBytes(0)) catch error.ResourceBudgetExceeded;
        }
        /// Caller holds backend.mu. A busy publication lane returns refusal;
        /// release that mutex before waiting under the original execution.
        pub fn initInPlace(self: *Self, backend: *BackendType, snapshot: *Snapshot, resources: storage_io.DurabilityResources, original: admission.Port) !void {
            if (backend.manifest_recovery_required) return error.RecoveryRequired;
            if (backend.active_readers != 0 or backend.manifest_publish_in_flight or backend.manifest_checkpoint_build_in_flight or backend.ledger_reclaim_in_flight) return error.ResourceBudgetExceeded;
            const root = backend.root_dir orelse return error.ExecutionWaitUnsupported;
            const directory = backend.manifest_checkpoint_directory orelse return error.InvalidArgument;
            const current = backend.run_directory orelse return error.InvalidArgument;
            if (backend.run_directory_dirty or directory.tree.root != current.tree.root or directory.memory_run_count != 0 or snapshot.retired or snapshot.value.tree.root != backend.obsolete_paths.tree.root) return error.InvalidArgument;
            if (snapshot.active_previous) |previous| {
                if (previous.active_next != snapshot) return error.InvalidArgument;
            } else if (backend.ledger_snapshots != snapshot) return error.InvalidArgument;
            const id = backend.next_run_id;
            const next_run_id = std.math.add(u64, id, 1) catch return error.ResourceBudgetExceeded;
            var turn = (try backend.tryBeginManifestTurnWithAdmission(original)) orelse return error.ResourceBudgetExceeded;
            errdefer turn.deinit();
            self.* = .{ .backend = backend, .source = .{ .cursor = directory.readCursor(), .ledger = snapshot.value.iterator(), .count = snapshot.value.count(), .root = root }, .fence = .capture(backend), .directory = directory, .captured_directory = directory, .snapshot = snapshot, .id = id, .next_run_id = next_run_id, .started = backend.writeStatsNowNs(), .covered_logical_bytes = backend.unpublished_wal_logical_bytes };
            try self.physical.initInPlace(resources, root, id, 0, next_run_id, &self.source, repository.maxManifestReadBytes());
            self.turn = turn;
            backend.next_run_id = next_run_id;
            backend.manifest_checkpoint_build_in_flight = true;
            backend.manifest_checkpoint_build_id = id;
        }
        fn inputsMatch(self: *const Self) bool {
            return self.turn != null and self.fence.matches(self.backend) and self.backend.manifest_checkpoint_directory == self.directory and self.backend.manifest_checkpoint_build_id == self.id and self.backend.manifest_checkpoint_build_in_flight and self.snapshot != null and !self.snapshot.?.retired;
        }
        fn fenceRecovery(self: *Self) void {
            self.backend.manifest_recovery_required = true;
            self.backend.manifest_journal.sequence = null;
            self.backend.markManifestDirty();
        }
        fn notePhysicalFailure(self: *Self, cause: anyerror, original: admission.Port) !void {
            if (self.physical.file.phase == .failed or self.physical.encoder.failure != null) {
                self.failure = cause;
                if (self.physical.attempted[2]) {
                    try bounded.lockAtomic(&self.backend.mu, original);
                    defer self.backend.mu.unlock();
                    self.fenceRecovery();
                }
            }
            return cause;
        }
        pub fn resumeWithAdmission(self: *Self, original: admission.Port) !void {
            if (self.aborting or self.retired) return error.InvalidArgument;
            if (self.installed) return original.checkpoint();
            if (self.failure) |failure| {
                if (self.physical.attempted[2]) {
                    try bounded.lockAtomic(&self.backend.mu, original);
                    defer self.backend.mu.unlock();
                    self.fenceRecovery();
                }
                return failure;
            }
            while (self.checked_paths < self.fence.retiredPathCount()) {
                try original.checkpoint();
                const path = try self.fence.retiredPathAt(self.source.root, self.checked_paths, &self.path);
                if (!self.snapshot.?.value.contains(path)) {
                    self.failure = error.InvalidManifest;
                    return error.InvalidManifest;
                }
                self.checked_paths += 1;
            }
            if (!self.descriptor_permitted) {
                self.physical.prepareDescriptorWithAdmission(original) catch |cause| return self.notePhysicalFailure(cause, original);
                try bounded.lockAtomic(&self.backend.mu, original);
                defer self.backend.mu.unlock();
                if (!self.inputsMatch()) {
                    self.failure = error.RecoveryRequired;
                    self.fenceRecovery();
                    return error.RecoveryRequired;
                }
                self.descriptor_permitted = true;
            }
            self.physical.resumeWithAdmission(original) catch |cause| return self.notePhysicalFailure(cause, original);
            try bounded.lockAtomic(&self.backend.mu, original);
            defer self.backend.mu.unlock();
            if (!self.inputsMatch()) {
                self.failure = error.RecoveryRequired;
                self.fenceRecovery();
                return error.RecoveryRequired;
            }
            const descriptor: repository.manifest_set.Descriptor = .{ .checkpoint = self.id, .first_segment = self.id, .sequence = 0 };
            try self.backend.installPreparedManifestCheckpoint(&self.fence, descriptor, self.next_run_id, self.physical.encoder.total, self.directory.?, &self.snapshot.?.value, self.covered_logical_bytes, self.started);
            // Fixed installation receipt before any subsequent fallible work.
            self.installed = true;
            self.directory = null;
            self.detach(false);
        }
        /// Under backend.mu. Input roots move to existing bounded retirement
        /// queues; accounting remains charged until those queues release them.
        fn detach(self: *Self, uncertain_descriptor: bool) void {
            if (self.detached) return;
            if (uncertain_descriptor and !self.installed) self.fenceRecovery();
            if (self.directory) |directory| self.backend.retireCheckpointDirectory(directory);
            self.directory = null;
            if (self.backend.manifest_checkpoint_directory == self.captured_directory) self.backend.manifest_checkpoint_directory = null;
            if (self.snapshot) |snapshot| snapshot.retire(self.backend);
            self.snapshot = null;
            if (self.backend.manifest_checkpoint_build_id == self.id) {
                self.backend.manifest_checkpoint_build_id = null;
                self.backend.manifest_checkpoint_build_in_flight = false;
            }
            if (!self.installed) self.backend.markManifestDirty();
            self.turn.?.deinit();
            self.turn = null;
            self.detached = true;
        }
        pub fn takeResources(self: *Self) !storage_io.DurabilityResources {
            if (!self.installed or self.aborting or self.retired) return error.InvalidArgument;
            return self.physical.takeResources();
        }
        pub fn retireWithAdmission(self: *Self, original: admission.Port) !void {
            if (self.retired) return;
            if (!self.installed or self.aborting) return error.InvalidArgument;
            try self.physical.retireWithAdmission(original);
            self.retired = true;
        }
        pub fn abortWithAdmission(self: *Self, original: admission.Port) !void {
            if (self.retired) return;
            self.aborting = true;
            try self.physical.abortWithAdmission(original);
            if (!self.detached) {
                try bounded.lockAtomic(&self.backend.mu, original);
                defer self.backend.mu.unlock();
                self.detach(self.physical.attempted[2]);
            }
            self.retired = true;
        }
        /// Explicit quiesced fallback; caller must not hold backend.mu. Join
        /// physical borrows before transferring metadata retirement ownership.
        pub fn deinit(self: *Self) void {
            const uncertain = self.physical.attempted[2];
            self.physical.deinit();
            if (!self.detached) {
                @import("antfly_platform").sync.lockYielding(&self.backend.mu);
                defer self.backend.mu.unlock();
                self.detach(uncertain);
            }
        }
    };
}

const Backend = @import("../lsm_backend.zig").Backend;
const Job = Publication(Backend);
const cpu = @import("../../common/runnable_scheduler.zig");
const Mode = enum { retry, stale_before, stale_after, ambiguous, missing_path, busy };
const Control = struct {
    owner: *Job,
    backend: *Backend,
    mode: Mode,
    failure: anyerror = error.Canceled,
    enabled: bool = false,
    seen: [4]bool = @splat(false),
    changed: bool = false,
    allow_held_backend: bool = false,
    reject_progress: bool = false,
    reject_abort: bool = false,
    execution: *cpu.Execution,
    scheduler: *cpu.Scheduler,
    waits: usize = 0,
    fn check(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try self.execution.checkLifetime();
        try std.testing.expectEqual(cpu.State.running, self.scheduler.state(&self.execution.entry.?));
        if (!self.allow_held_backend) {
            try std.testing.expect(self.backend.mu.tryLock());
            self.backend.mu.unlock();
        }
        if (!self.enabled) return;
        const owner = self.owner;
        if (self.reject_abort and owner.aborting and owner.physical.retired and !owner.detached) {
            self.reject_abort = false;
            return self.failure;
        }
        if (self.mode == .retry) {
            const index: ?usize = if (owner.physical.file.phase == .writing) @as(usize, @backingInt(owner.physical.phase)) else if (owner.physical.phase == .complete and !owner.installed) 3 else null;
            if (index) |at| if (!self.seen[at]) {
                self.seen[at] = true;
                return self.failure;
            };
        }
        if (!self.changed and ((self.mode == .stale_before and owner.physical.phase == .descriptor and !owner.descriptor_permitted) or (self.mode == .stale_after and owner.physical.phase == .complete and !owner.installed))) {
            try std.testing.expect(self.backend.mu.tryLock());
            self.backend.manifest_journal.bytes += 1;
            self.backend.mu.unlock();
            self.changed = true;
        }
        if (!self.changed and self.mode == .ambiguous and owner.physical.phase == .descriptor and owner.physical.fixed_written and owner.physical.file.phase == .ready) {
            storage_io.NativeSyncTestControl.control_failure_after_rename.store(true, .release);
            self.changed = true;
        }
    }
    fn lifetime(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try self.execution.checkLifetime();
    }
    fn progress(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.reject_progress) return self.failure;
        return error.UnexpectedWait;
    }
    fn deadline(raw: *anyopaque) !?u64 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return self.execution.entry.?.options.deadline_ns;
    }
    fn begin(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try std.testing.expect(self.backend.mu.tryLock());
        self.backend.mu.unlock();
        try self.execution.park(.resource_blocked);
        var control: cpu.Execution = .{};
        defer control.finish();
        try control.start(self.scheduler, .{ .io = std.testing.io, .native_now_ns = @import("antfly_platform").time.monotonicNs }, .control);
        self.waits += 1;
        storage_io.NativeSyncTestControl.release.store(true, .release);
    }
    fn end(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        storage_io.NativeSyncTestControl.release.store(false, .release);
        try self.execution.enter(.write);
    }
    fn payload(self: *@This()) admission.Payload {
        return .{ .ptr = self, .vtable = &.{ .checkpoint = check, .check_lifetime = lifetime, .deadline_ns = deadline, .wait_for_progress = progress }, .execution_wait = .{ .ptr = self, .begin = begin, .end = end } };
    }
};

fn prepare(backend: *Backend, include_retired: bool) !*Snapshot {
    const fence = repository.ManifestJournal.CheckpointFence.capture(backend);
    if (include_retired) {
        var buffer: [std.fs.max_path_bytes]u8 = undefined;
        for (0..fence.retiredPathCount()) |i| {
            const path = try fence.retiredPathAt(backend.root_dir.?, i, &buffer);
            if (!backend.obsolete_paths.contains(path)) {
                const owned = try backend.allocator.dupe(u8, path);
                errdefer backend.allocator.free(owned);
                try backend.obsolete_paths.append(backend.allocator, .{ .path = owned, .delete_after_ns = std.math.maxInt(u64) });
            }
        }
    }
    backend.manifest_checkpoint_directory = try (try backend.planningDirectory()).fork(backend.allocator);
    const snapshot = try Snapshot.capture(backend, &backend.obsolete_paths);
    backend.syncTrackedInMemoryStateUsageCurrentLocked();
    return snapshot;
}

test "controlled physical sync prepared backend checkpoint installs once across rejection and fences stale or uncertain lineage" {
    if (comptime @import("builtin").os.tag == .freestanding or @import("builtin").os.tag == .windows or @import("builtin").os.tag == .wasi) return error.SkipZigTest;
    const resources = @import("../resource_manager.zig");
    const a = std.testing.allocator;
    for ([_]Mode{ .retry, .retry, .retry, .stale_before, .stale_after, .ambiguous, .missing_path, .busy }, 0..) |mode, case| {
        var manager = resources.ResourceManager.init(.{ .identity_allocator = a, .memory_budget = .{ .hard_limit_bytes = 64 * 1024 * 1024 } });
        defer manager.deinit(a);
        var allocations = std.testing.FailingAllocator.init(a, .{});
        var pool = storage_io.NativeStoragePool.initWithCapacityForTest(a, 32);
        defer pool.deinit();
        var native = try storage_io.NativeStorage.initWithPool(a, .threaded, &pool);
        defer native.deinit();
        var root_buffer: [256]u8 = undefined;
        const root_z = repository.tmpPath(&root_buffer, "prepared-checkpoint-install");
        defer repository.cleanupTmp(root_z);
        const root = std.mem.span(root_z);
        var backend: Backend = undefined;
        try backend.openInto(allocations.allocator(), root, .{ .storage = native.storage(), .native_storage_pool = &pool, .resource_manager = &manager, .flush_threshold = 1 });
        var backend_live = true;
        defer if (backend_live) backend.abandonAfterCrash();
        for ([_][]const u8{ "a", "b", "z" }) |key| {
            var txn = try backend.beginWrite();
            errdefer txn.abort();
            try txn.put(.{ .name = "docs" }, key, "durable value");
            try txn.commit();
        }
        // This test explicitly prepares using the existing legacy admission
        // path. The operation begins only after both roots are registered/funded.
        const snapshot = try prepare(&backend, mode != .missing_path);
        const candidate = backend.manifest_checkpoint_directory.?;
        const old_directory = backend.manifest_directory;
        const old_descriptor = backend.manifest_journal.descriptor;
        const baseline = backend.tracked_in_memory_state_bytes;
        const writes = backend.write_stats.manifest_writes;
        const checkpoints = backend.write_stats.manifest_checkpoints;
        backend.unpublished_wal_logical_bytes = 777;
        var fixed = try manager.reserve(.lsm_table_builder_working_set, try Job.memoryBound(native.storage()));
        defer fixed.release();
        var scheduler = try cpu.Scheduler.init(.{ .slots = 3, .control_slots = 1, .cleanup_slots = 1 });
        defer scheduler.close();
        var execution: cpu.Execution = .{};
        defer execution.finish();
        try execution.reserveCleanupRoot(&scheduler, .{ .io = std.testing.io, .native_now_ns = @import("antfly_platform").time.monotonicNs, .allow_wait = true }, 30 * std.time.ns_per_s);
        try execution.activateCleanup();
        var query: cpu.Execution = .{};
        defer query.finish();
        try query.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = @import("antfly_platform").time.monotonicNs }, .query);
        var owner: Job = undefined;
        var control: Control = .{ .owner = &owner, .backend = &backend, .mode = mode, .execution = &execution, .scheduler = &scheduler, .failure = ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.FileNotFound })[@min(case, 2)] };
        const payload = control.payload();
        const original = admission.Port.init(&payload);
        var reserved: usize = 0;
        while (pool.tryReserveDescriptorsForTest(std.testing.io, 1)) reserved += 1;
        defer pool.releaseDescriptorsForTest(std.testing.io, reserved);
        try std.testing.expect(reserved > 0);
        pool.releaseDescriptorsForTest(std.testing.io, 1);
        reserved -= 1;
        const grant = (try native.storage().tryPrepareDurabilityWithAdmission(original)).?;
        if (mode == .busy) {
            const io = backend.manifestCoordinationIo().?;
            try std.testing.expect(backend.manifest_publish_mu.tryLock());
            const next_id = backend.next_run_id;
            try std.testing.expect(backend.mu.tryLock());
            const refused = owner.initInPlace(&backend, snapshot, grant, original);
            backend.mu.unlock();
            backend.manifest_publish_mu.unlock(io);
            try std.testing.expectError(error.ResourceBudgetExceeded, refused);
            try std.testing.expectEqual(next_id, backend.next_run_id);
            try std.testing.expectEqual(@as(usize, 0), backend.active_readers);
            try std.testing.expect(!snapshot.retired and backend.manifest_checkpoint_directory == candidate);
        }
        const allocator_calls = allocations.alloc_index;
        allocations.fail_index = allocator_calls;
        allocations.resize_fail_index = allocations.resize_index;
        defer allocations.fail_index = std.math.maxInt(usize);
        defer allocations.resize_fail_index = std.math.maxInt(usize);
        try std.testing.expect(backend.mu.tryLock());
        const initialized = owner.initInPlace(&backend, snapshot, grant, original);
        backend.mu.unlock();
        try initialized;
        var owner_live = true;
        defer if (owner_live) owner.deinit();
        control.enabled = true;
        storage_io.NativeSyncTestControl.pause.store(true, .release);
        storage_io.NativeSyncTestControl.release.store(false, .release);
        defer storage_io.NativeSyncTestControl.pause.store(false, .release);
        defer storage_io.NativeSyncTestControl.release.store(true, .release);
        defer storage_io.NativeSyncTestControl.control_failure_after_rename.store(false, .release);
        const held = pool.snapshotStats().fd_admitted_descriptors;
        if (mode == .retry or mode == .busy) {
            var after_descriptor_calls: ?usize = null;
            while (true) {
                owner.resumeWithAdmission(original) catch |cause| {
                    try std.testing.expectEqual(control.failure, cause);
                    try std.testing.expect(backend.manifest_directory == old_directory and !owner.installed);
                    try std.testing.expect(backend.manifest_publish_in_flight and backend.manifest_checkpoint_build_in_flight and !snapshot.retired);
                    if (owner.physical.phase == .complete) {
                        after_descriptor_calls = storage_io.NativeSyncTestControl.metadata.load(.acquire);
                        try std.testing.expect(backend.mu.tryLock());
                        control.allow_held_backend = true;
                        control.reject_progress = true;
                        const refused = owner.resumeWithAdmission(original);
                        backend.mu.unlock();
                        control.allow_held_backend = false;
                        control.reject_progress = false;
                        try std.testing.expectError(control.failure, refused);
                        try std.testing.expect(!owner.installed and !snapshot.retired and owner.turn != null);
                        try std.testing.expectEqual(after_descriptor_calls.?, storage_io.NativeSyncTestControl.metadata.load(.acquire));
                    }
                    continue;
                };
                break;
            }
            if (after_descriptor_calls) |calls| try std.testing.expectEqual(calls, storage_io.NativeSyncTestControl.metadata.load(.acquire));
            if (mode == .retry) for (control.seen) |seen| try std.testing.expect(seen);
            try std.testing.expect(owner.installed and owner.detached and snapshot.retired);
            try std.testing.expect(backend.manifest_directory == candidate and backend.manifest_checkpoint_directory == null);
            try std.testing.expect(!backend.manifest_publish_in_flight and !backend.manifest_checkpoint_build_in_flight);
            try std.testing.expectEqual(@as(usize, 0), backend.active_readers);
            try std.testing.expectEqual(writes + 1, backend.write_stats.manifest_writes);
            try std.testing.expectEqual(checkpoints + 1, backend.write_stats.manifest_checkpoints);
            try std.testing.expectEqual(@as(u64, 0), backend.unpublished_wal_logical_bytes);
            try std.testing.expect(!backend.manifest_dirty and !backend.manifest_recovery_required);
            try owner.resumeWithAdmission(original);
            try std.testing.expectEqual(writes + 1, backend.write_stats.manifest_writes);
            try std.testing.expectEqual(held, pool.snapshotStats().fd_admitted_descriptors);
            try owner.retireWithAdmission(original);
            try std.testing.expect(control.waits > 5);
        } else {
            const failure: anyerror = switch (mode) {
                .stale_before, .stale_after => error.RecoveryRequired,
                .ambiguous => error.InputOutput,
                .missing_path => error.InvalidManifest,
                else => unreachable,
            };
            try std.testing.expectError(failure, owner.resumeWithAdmission(original));
            try std.testing.expect(!owner.installed and backend.manifest_directory == old_directory);
            try std.testing.expect(std.meta.eql(old_descriptor, backend.manifest_journal.descriptor));
            if (mode == .stale_before or mode == .missing_path) try std.testing.expect(!owner.physical.attempted[2]);
            const calls = storage_io.NativeSyncTestControl.metadata.load(.acquire);
            try std.testing.expectError(failure, owner.resumeWithAdmission(original));
            try std.testing.expectEqual(calls, storage_io.NativeSyncTestControl.metadata.load(.acquire));
            storage_io.NativeSyncTestControl.control_failure_after_rename.store(false, .release);
            control.reject_abort = true;
            try std.testing.expectError(control.failure, owner.abortWithAdmission(original));
            try std.testing.expect(owner.physical.retired and !owner.detached and !snapshot.retired);
            try std.testing.expect(owner.turn != null and backend.manifest_checkpoint_directory == candidate);
            try std.testing.expectEqual(baseline, backend.tracked_in_memory_state_bytes);
            try owner.abortWithAdmission(original);
            try std.testing.expect(snapshot.retired and backend.manifest_checkpoint_directory == null);
            try std.testing.expectEqual(mode != .missing_path, backend.manifest_recovery_required);
            try std.testing.expectEqual(writes, backend.write_stats.manifest_writes);
            try std.testing.expectEqual(@as(u64, 777), backend.unpublished_wal_logical_bytes);
        }
        try std.testing.expectEqual(baseline, backend.tracked_in_memory_state_bytes);
        try std.testing.expectEqual(allocator_calls, allocations.alloc_index);
        try std.testing.expect(!allocations.has_induced_failure);
        owner.deinit();
        owner_live = false;
        storage_io.NativeSyncTestControl.pause.store(false, .release);
        allocations.fail_index = std.math.maxInt(usize);
        allocations.resize_fail_index = std.math.maxInt(usize);
        pool.releaseDescriptorsForTest(std.testing.io, reserved);
        reserved = 0;
        fixed.release();
        // No legacy close publication may mask what this operation persisted.
        backend.abandonAfterCrash();
        backend_live = false;
        var reopened = try Backend.open(a, root, .{ .backend = .{ .read_only = true }, .storage = native.storage() });
        defer reopened.close();
        var txn = try reopened.beginRead();
        defer txn.abort();
        for ([_][]const u8{ "a", "b", "z" }) |key| try std.testing.expectEqualStrings("durable value", try txn.get(.{ .name = "docs" }, key));
        try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.accounting_errors);
    }
}
