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
const Crc32 = @import("antfly_hash").Crc32;
const platform_sync = @import("antfly_platform").sync;
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const backend_erased = @import("../../backend_erased.zig");
const docstore_mod = @import("../../docstore.zig");
const lsm_backend = @import("../../lsm_backend.zig");
const mem_backend = @import("../../mem_backend.zig");
const fs_paths = @import("antfly_runtime_fs").fs_paths;
const platform_time = @import("antfly_platform").time;

const metadata_prefix = "\x00\x00__metadata__:derived_apply:";
const checkpoint_file_name = "derived_apply.checkpoint";
const checkpoint_magic = "AFPRJCP1";
const projection_checkpoint_format_version: u32 = 3;
const projection_checkpoint_legacy_format_version: u32 = 2;
const checkpoint_checksum_len: usize = @sizeOf(u32);
const checkpoint_max_bytes: usize = 16 * 1024 * 1024;

const checkpoint_lock_alloc = std.heap.page_allocator;
var checkpoint_lock_registry_mutex: std.atomic.Mutex = .unlocked;
var checkpoint_locks: std.StringHashMapUnmanaged(*CheckpointFileLock) = .empty;

fn lockAtomicMutex(mutex: *std.atomic.Mutex) void {
    platform_sync.lockYielding(mutex);
}

const CheckpointFileLock = struct {
    path: []u8,
    mutex: std.atomic.Mutex = .unlocked,
    refs: usize = 0,
};

const CheckpointWriteGuard = struct {
    lock: *CheckpointFileLock,

    fn release(self: *@This()) void {
        self.lock.mutex.unlock();
        releaseCheckpointFileLock(self.lock);
    }
};

fn acquireCheckpointFileLock(path: []const u8) !CheckpointWriteGuard {
    const lock = try retainCheckpointFileLock(path);
    lockAtomicMutex(&lock.mutex);
    return .{ .lock = lock };
}

fn retainCheckpointFileLock(path: []const u8) !*CheckpointFileLock {
    lockAtomicMutex(&checkpoint_lock_registry_mutex);
    defer checkpoint_lock_registry_mutex.unlock();

    const gop = try checkpoint_locks.getOrPut(checkpoint_lock_alloc, path);
    if (!gop.found_existing) {
        errdefer _ = checkpoint_locks.remove(path);

        const owned_path = try checkpoint_lock_alloc.dupe(u8, path);
        errdefer checkpoint_lock_alloc.free(owned_path);

        const lock = try checkpoint_lock_alloc.create(CheckpointFileLock);
        lock.* = .{
            .path = owned_path,
        };
        gop.key_ptr.* = owned_path;
        gop.value_ptr.* = lock;
    }

    const lock = gop.value_ptr.*;
    lock.refs += 1;
    return lock;
}

fn releaseCheckpointFileLock(lock: *CheckpointFileLock) void {
    lockAtomicMutex(&checkpoint_lock_registry_mutex);
    defer checkpoint_lock_registry_mutex.unlock();

    std.debug.assert(lock.refs > 0);
    lock.refs -= 1;
    if (lock.refs != 0) return;

    const removed = checkpoint_locks.fetchRemove(lock.path) orelse {
        std.debug.panic("missing derived apply checkpoint lock for {s}", .{lock.path});
    };
    std.debug.assert(removed.value == lock);
    checkpoint_lock_alloc.free(lock.path);
    checkpoint_lock_alloc.destroy(lock);
}

pub const ProjectionStatus = enum(u8) {
    clean = 1,
    rebuilding = 2,
    degraded = 3,
    repair_required = 4,
};

pub const ProjectionCheckpoint = struct {
    applied_sequence: u64 = 0,
    status: ProjectionStatus = .clean,
    generation: u64 = 0,
    config_hash: u64 = 0,
    /// Physical result cardinality certified by the same durable publication
    /// boundary as `applied_sequence`. This is deliberately distinct from the
    /// live artifact target: generated artifacts may advance while this last
    /// atomically published snapshot remains safe to query.
    published_count: ?u64 = null,
    format_version: u32 = projection_checkpoint_format_version,
};

/// A coherent receiver-local projection checkpoint cut. The file lock remains
/// held until deinit, including while a caller publishes evidence into primary
/// metadata. Acquiring an epoch before/after an unguarded file read is NOT
/// equivalent: a writer revokes first and then replaces the file.
///
/// Lock order is checkpoint file -> primary transaction, matching publishers.
/// Acquire before opening a primary write transaction. This lease proves a
/// checkpoint cut only; the caller must independently validate the physical
/// index generation before treating a clean watermark as completion evidence.
pub const ProjectionSnapshot = struct {
    alloc: Allocator,
    guard: CheckpointWriteGuard,
    checkpoint: CheckpointMap,
    authority: @import("../artifact_publication.zig").Authority,
    epoch: u64,

    pub fn deinit(self: *@This()) void {
        self.checkpoint.deinit(self.alloc);
        self.guard.release();
        self.* = undefined;
    }

    /// Absence is not a clean zero-watermark certificate.
    pub fn get(self: *const @This(), index_name: []const u8) ?ProjectionCheckpoint {
        return self.checkpoint.map.get(index_name);
    }

    /// Recheck in the evidence publication transaction. Raw physical resets,
    /// authority switches and reopen can revoke independently of this lock.
    pub fn requireCurrent(self: *const @This(), txn: anytype) !void {
        const current = (try @import("../artifact_publication.zig").authority(txn)) orelse return error.ArtifactCatalogDrift;
        if (!std.meta.eql(self.authority, current)) return error.ArtifactCatalogDrift;
        try @import("../artifact_projection_epoch.zig").requireCurrent(txn, self.epoch);
    }
};

/// Read and decode the sidecar once for a batch of index evidence publications,
/// not once per document/requirement. A busy publisher yields no snapshot rather
/// than blocking the completion driver behind index I/O. Missing or scalar-only
/// checkpoints likewise cannot certify physical generations.
pub fn tryAcquireProjectionSnapshot(
    alloc: Allocator,
    io: std.Io,
    store: anytype,
    checkpoint_path: ?[]const u8,
) !?ProjectionSnapshot {
    if (comptime builtin.os.tag == .freestanding) return null;
    const path = checkpoint_path orelse return null;
    const lock = try retainCheckpointFileLock(path);
    if (!lock.mutex.tryLock()) {
        releaseCheckpointFileLock(lock);
        return null;
    }
    var guard: CheckpointWriteGuard = .{ .lock = lock };
    var owned = true;
    defer if (owned) guard.release();
    var runtime = try initRuntimeStore(alloc, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginProbe();
    defer txn.abort();
    const authority = (try @import("../artifact_publication.zig").authority(&txn)) orelse return null;
    const epoch = try @import("../artifact_projection_epoch.zig").load(&txn);
    var checkpoint = loadCheckpoint(alloc, io, path) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    errdefer checkpoint.deinit(alloc);
    owned = false;
    return .{ .alloc = alloc, .guard = guard, .checkpoint = checkpoint, .authority = authority, .epoch = epoch };
}

pub const AppliedSequenceUpdate = struct {
    index_name: []const u8,
    sequence: u64,
    status: ProjectionStatus = .clean,
    generation: u64 = 0,
    config_hash: u64 = 0,
    published_count: ?u64 = null,

    fn checkpoint(self: @This()) ProjectionCheckpoint {
        return .{
            .applied_sequence = self.sequence,
            .status = self.status,
            .generation = self.generation,
            .config_hash = self.config_hash,
            .published_count = self.published_count,
        };
    }
};

pub fn checkpointPathAlloc(alloc: Allocator, db_path: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}/{s}", .{ db_path, checkpoint_file_name });
}

pub fn loadAppliedSequence(alloc: Allocator, store: anytype, index_name: []const u8) !u64 {
    const key = try std.fmt.allocPrint(alloc, "{s}{s}", .{ metadata_prefix, index_name });
    defer alloc.free(key);

    var runtime = try initRuntimeStore(alloc, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginProbe();
    defer txn.abort();
    const borrowed = txn.get(key) catch |err| switch (err) {
        error.NotFound => return 0,
        else => return err,
    };
    const raw = try alloc.dupe(u8, borrowed);
    defer alloc.free(raw);

    if (raw.len != 8) return error.InvalidDerivedApplyState;
    return std.mem.readInt(u64, raw[0..8], .little);
}

pub fn loadAppliedSequenceWithCheckpoint(
    alloc: Allocator,
    io: std.Io,
    store: anytype,
    checkpoint_path: ?[]const u8,
    index_name: []const u8,
) !u64 {
    if (comptime builtin.os.tag == .freestanding or builtin.os.tag == .wasi) return try loadAppliedSequence(alloc, store, index_name);
    const path = checkpoint_path orelse return try loadAppliedSequence(alloc, store, index_name);
    const checkpoint = loadProjectionCheckpoint(alloc, io, path, index_name) catch |err| switch (err) {
        error.FileNotFound => return 0,
        else => return err,
    };
    return if (checkpoint) |value| value.applied_sequence else 0;
}

pub fn loadProjectionCheckpointWithSidecar(
    alloc: Allocator,
    io: std.Io,
    store: anytype,
    checkpoint_path: ?[]const u8,
    index_name: []const u8,
) !ProjectionCheckpoint {
    if (comptime builtin.os.tag == .freestanding or builtin.os.tag == .wasi) {
        return .{ .applied_sequence = try loadAppliedSequence(alloc, store, index_name) };
    }
    const path = checkpoint_path orelse return .{ .applied_sequence = try loadAppliedSequence(alloc, store, index_name) };
    const checkpoint = loadProjectionCheckpoint(alloc, io, path, index_name) catch |err| switch (err) {
        error.FileNotFound => return .{},
        else => return err,
    };
    return checkpoint orelse .{};
}

pub fn saveAppliedSequence(store: anytype, index_name: []const u8, sequence: u64) !void {
    var runtime = try initRuntimeStore(std.heap.page_allocator, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginWrite();
    errdefer txn.abort();
    try saveAppliedSequenceTxn(&txn, index_name, sequence);
    try txn.commit();
}

pub fn saveAppliedSequenceTxn(txn: anytype, index_name: []const u8, sequence: u64) !void {
    var mutable_txn = txn;
    var buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &buf, sequence, .little);

    var key_buf: [256]u8 = undefined;
    const key = try std.fmt.bufPrint(&key_buf, "{s}{s}", .{ metadata_prefix, index_name });
    const previous = mutable_txn.get(key) catch |err| if (err == error.NotFound) null else return err;
    if (previous) |raw| {
        if (raw.len != 8) return error.InvalidDerivedApplyState;
        if (sequence < std.mem.readInt(u64, raw[0..8], .little)) try @import("../artifact_projection_epoch.zig").revoke(mutable_txn);
    } else try @import("../artifact_projection_epoch.zig").revoke(mutable_txn);
    try mutable_txn.put(key, &buf);
}

pub fn saveAppliedSequences(store: anytype, updates: []const AppliedSequenceUpdate) !void {
    if (updates.len == 0) return;

    var runtime = try initRuntimeStore(std.heap.page_allocator, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginWrite();
    errdefer txn.abort();
    for (updates) |update| {
        try saveAppliedSequenceTxn(&txn, update.index_name, update.sequence);
    }
    try txn.commit();
}

pub fn saveAppliedSequenceWithCheckpoint(
    alloc: Allocator,
    io: std.Io,
    store: anytype,
    checkpoint_path: ?[]const u8,
    index_name: []const u8,
    sequence: u64,
) !void {
    if (comptime builtin.os.tag == .freestanding or builtin.os.tag == .wasi) return try saveAppliedSequence(store, index_name, sequence);
    if (checkpoint_path) |path| {
        try setAppliedSequencesCheckpoint(alloc, io, path, store, &[_]AppliedSequenceUpdate{.{
            .index_name = index_name,
            .sequence = sequence,
        }});
        return;
    }
    try saveAppliedSequence(store, index_name, sequence);
}

pub fn saveAppliedSequenceUpdateWithCheckpoint(
    alloc: Allocator,
    io: std.Io,
    store: anytype,
    checkpoint_path: ?[]const u8,
    update: AppliedSequenceUpdate,
) !void {
    if (comptime builtin.os.tag == .freestanding) {
        if (update.status != .clean or update.generation != 0) try revokeCompletion(alloc, store);
        return try saveAppliedSequence(store, update.index_name, update.sequence);
    }
    if (checkpoint_path) |path| {
        try setAppliedSequencesCheckpoint(alloc, io, path, store, &[_]AppliedSequenceUpdate{update});
        return;
    }
    if (update.status != .clean or update.generation != 0) try revokeCompletion(alloc, store);
    try saveAppliedSequence(store, update.index_name, update.sequence);
}

pub fn saveProjectionCheckpointWithSidecar(
    alloc: Allocator,
    io: std.Io,
    store: anytype,
    checkpoint_path: ?[]const u8,
    index_name: []const u8,
    checkpoint: ProjectionCheckpoint,
) !void {
    if (comptime builtin.os.tag == .freestanding) {
        try revokeCompletion(alloc, store);
        return try saveAppliedSequence(store, index_name, checkpoint.applied_sequence);
    }
    if (checkpoint_path) |path| {
        try setProjectionCheckpoints(alloc, io, path, store, &[_]AppliedSequenceUpdate{.{
            .index_name = index_name,
            .sequence = checkpoint.applied_sequence,
            .status = checkpoint.status,
            .generation = checkpoint.generation,
            .config_hash = checkpoint.config_hash,
            .published_count = checkpoint.published_count,
        }});
        return;
    }
    // Metadata-only stores cannot compare lifecycle identities in the legacy
    // scalar watermark. Explicit checkpoint replacement must revoke; ordinary
    // forward sequence updates retain their cheaper monotonic path.
    try revokeCompletion(alloc, store);
    try saveAppliedSequence(store, index_name, checkpoint.applied_sequence);
}

pub fn saveAppliedSequencesWithCheckpoint(
    alloc: Allocator,
    io: std.Io,
    store: anytype,
    checkpoint_path: ?[]const u8,
    updates: []const AppliedSequenceUpdate,
) !void {
    if (updates.len == 0) return;
    if (comptime builtin.os.tag == .freestanding or builtin.os.tag == .wasi) return try saveAppliedSequences(store, updates);
    if (checkpoint_path) |path| {
        try saveAppliedSequencesCheckpoint(alloc, io, path, store, updates);
        return;
    }
    try saveAppliedSequences(store, updates);
}

pub fn clearAppliedSequence(store: anytype, index_name: []const u8) !void {
    var runtime = try initRuntimeStore(std.heap.page_allocator, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginWrite();
    errdefer txn.abort();
    try clearAppliedSequenceTxn(&txn, index_name);
    try txn.commit();
}

pub fn clearAppliedSequenceWithCheckpoint(
    alloc: Allocator,
    io: std.Io,
    store: anytype,
    checkpoint_path: ?[]const u8,
    index_name: []const u8,
) !void {
    if (comptime builtin.os.tag == .freestanding or builtin.os.tag == .wasi) return try clearAppliedSequence(store, index_name);
    if (checkpoint_path) |path| {
        try clearAppliedSequenceCheckpoint(alloc, io, path, store, index_name);
        return;
    }
    try clearAppliedSequence(store, index_name);
}

pub fn clearAppliedSequenceTxn(txn: anytype, index_name: []const u8) !void {
    var mutable_txn = txn;
    try @import("../artifact_projection_epoch.zig").revoke(mutable_txn);
    var key_buf: [256]u8 = undefined;
    const key = try std.fmt.bufPrint(&key_buf, "{s}{s}", .{ metadata_prefix, index_name });
    mutable_txn.delete(key) catch |err| switch (err) {
        error.NotFound => {},
        else => return err,
    };
}

const RuntimeStoreHandle = struct {
    store: backend_erased.Store,
    owned: bool,

    pub fn deinit(self: *@This()) void {
        if (self.owned) self.store.deinit();
    }
};

fn initRuntimeStore(alloc: Allocator, store: anytype) !RuntimeStoreHandle {
    const T = @TypeOf(store);
    if (T == backend_erased.Store) return .{ .store = store, .owned = false };
    if (T == *backend_erased.Store) return .{ .store = store.*, .owned = false };

    switch (@typeInfo(T)) {
        .pointer => |ptr| {
            if (@hasDecl(ptr.child, "backendStore")) {
                return .{
                    .store = try backend_erased.storeFrom(alloc, store.backendStore()),
                    .owned = true,
                };
            }
        },
        else => {
            if (@hasDecl(T, "backendStore")) {
                return .{
                    .store = try backend_erased.storeFrom(alloc, store.backendStore()),
                    .owned = true,
                };
            }
        },
    }

    return .{
        .store = try backend_erased.storeFrom(alloc, store),
        .owned = true,
    };
}

const CheckpointMap = struct {
    map: std.StringHashMapUnmanaged(ProjectionCheckpoint) = .empty,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        var it = self.map.iterator();
        while (it.next()) |entry| alloc.free(@constCast(entry.key_ptr.*));
        self.map.deinit(alloc);
        self.* = .{};
    }

    fn putMax(self: *@This(), alloc: Allocator, name: []const u8, checkpoint: ProjectionCheckpoint) !void {
        const gop = try self.map.getOrPut(alloc, name);
        if (gop.found_existing) {
            if (checkpoint.applied_sequence > gop.value_ptr.*.applied_sequence) {
                gop.value_ptr.* = checkpoint;
            }
            return;
        }
        errdefer _ = self.map.remove(name);
        gop.key_ptr.* = try alloc.dupe(u8, name);
        gop.value_ptr.* = checkpoint;
    }

    fn put(self: *@This(), alloc: Allocator, name: []const u8, checkpoint: ProjectionCheckpoint) !void {
        const gop = try self.map.getOrPut(alloc, name);
        if (gop.found_existing) {
            gop.value_ptr.* = checkpoint;
            return;
        }
        errdefer _ = self.map.remove(name);
        gop.key_ptr.* = try alloc.dupe(u8, name);
        gop.value_ptr.* = checkpoint;
    }

    fn putMaxSequence(self: *@This(), alloc: Allocator, name: []const u8, sequence: u64) !void {
        const gop = try self.map.getOrPut(alloc, name);
        if (gop.found_existing) {
            gop.value_ptr.*.applied_sequence = @max(gop.value_ptr.*.applied_sequence, sequence);
            return;
        }
        errdefer _ = self.map.remove(name);
        gop.key_ptr.* = try alloc.dupe(u8, name);
        gop.value_ptr.* = .{ .applied_sequence = sequence };
    }

    fn putMaxSequenceUpdate(self: *@This(), alloc: Allocator, update: AppliedSequenceUpdate) !void {
        const gop = try self.map.getOrPut(alloc, update.index_name);
        if (gop.found_existing) {
            const previous_sequence = gop.value_ptr.*.applied_sequence;
            gop.value_ptr.*.applied_sequence = @max(gop.value_ptr.*.applied_sequence, update.sequence);
            mergeSequenceMetadata(gop.value_ptr, update, previous_sequence, false);
            return;
        }
        errdefer _ = self.map.remove(update.index_name);
        gop.key_ptr.* = try alloc.dupe(u8, update.index_name);
        gop.value_ptr.* = .{
            .applied_sequence = update.sequence,
            .generation = update.generation,
            .config_hash = update.config_hash,
            .published_count = update.published_count,
        };
    }

    fn putSequence(self: *@This(), alloc: Allocator, name: []const u8, sequence: u64) !void {
        const gop = try self.map.getOrPut(alloc, name);
        if (gop.found_existing) {
            gop.value_ptr.*.applied_sequence = sequence;
            return;
        }
        errdefer _ = self.map.remove(name);
        gop.key_ptr.* = try alloc.dupe(u8, name);
        gop.value_ptr.* = .{ .applied_sequence = sequence };
    }

    fn putSequenceUpdate(self: *@This(), alloc: Allocator, update: AppliedSequenceUpdate) !void {
        const gop = try self.map.getOrPut(alloc, update.index_name);
        if (gop.found_existing) {
            const previous_sequence = gop.value_ptr.*.applied_sequence;
            gop.value_ptr.*.applied_sequence = update.sequence;
            mergeSequenceMetadata(gop.value_ptr, update, previous_sequence, true);
            return;
        }
        errdefer _ = self.map.remove(update.index_name);
        gop.key_ptr.* = try alloc.dupe(u8, update.index_name);
        gop.value_ptr.* = .{
            .applied_sequence = update.sequence,
            .generation = update.generation,
            .config_hash = update.config_hash,
            .published_count = update.published_count,
        };
    }

    fn remove(self: *@This(), alloc: Allocator, name: []const u8) void {
        const removed = self.map.fetchRemove(name) orelse return;
        alloc.free(@constCast(removed.key));
    }
};

fn mergeSequenceMetadata(
    checkpoint: *ProjectionCheckpoint,
    update: AppliedSequenceUpdate,
    previous_sequence: u64,
    exact_sequence: bool,
) void {
    const previous_generation = checkpoint.generation;
    const previous_config_hash = checkpoint.config_hash;
    if (update.generation != 0) checkpoint.generation = update.generation;
    if (update.config_hash != 0) checkpoint.config_hash = update.config_hash;
    const identity_changed = checkpoint.generation != previous_generation or
        checkpoint.config_hash != previous_config_hash;
    const boundary_changed = checkpoint.applied_sequence != previous_sequence;

    // A physical publication certificate is meaningful only for the exact
    // sequence and incarnation that produced it. Advancing/replacing either
    // without a new certificate must invalidate the old one; a stale/lower
    // max update must never rebind its certificate to a newer cursor.
    if (exact_sequence or boundary_changed or identity_changed) {
        checkpoint.published_count = if (update.sequence == checkpoint.applied_sequence)
            update.published_count
        else
            null;
    } else if (update.published_count) |count| {
        checkpoint.published_count = count;
    }
}

fn loadProjectionCheckpoint(alloc: Allocator, io: std.Io, path: []const u8, index_name: []const u8) !?ProjectionCheckpoint {
    var checkpoint = try loadCheckpoint(alloc, io, path);
    defer checkpoint.deinit(alloc);
    return checkpoint.map.get(index_name);
}

test "ordered artifact inventory projection snapshot owns a coherent checkpoint and publication fence" {
    const alloc = std.testing.allocator;
    const db_mod = @import("antfly_source_root").antfly_sources.physical_db;
    const publication = @import("../artifact_publication.zig");
    const epoch = @import("../artifact_projection_epoch.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/projection-snapshot", .{tmp.sub_path});
    defer alloc.free(path);
    var db = try db_mod.DB.open(alloc, path, .{ .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false });
    defer db.close();
    const sidecar = try checkpointPathAlloc(alloc, path);
    defer alloc.free(sidecar);
    try std.testing.expect(try tryAcquireProjectionSnapshot(alloc, std.testing.io, db.core.store, sidecar) == null);
    var activation: publication.Command = .{ .mode = .activate, .namespace = @splat(1), .authority_epoch = 1, .catalog_digest = @splat(2), .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) };
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try publication.stageAuthority(&txn, activation);
        try txn.commit();
    }
    try std.testing.expect(try tryAcquireProjectionSnapshot(alloc, std.testing.io, db.core.store, sidecar) == null);
    try std.testing.expect(try tryAcquireProjectionSnapshot(alloc, std.testing.io, db.core.store, null) == null);
    try saveProjectionCheckpointWithSidecar(alloc, std.testing.io, db.core.store, sidecar, "text", .{ .applied_sequence = 11, .generation = 2, .config_hash = 7 });
    const Check = struct {
        fn run(a: Allocator, store: *docstore_mod.DocStore, location: []const u8) !void {
            var snapshot = (try tryAcquireProjectionSnapshot(a, std.testing.io, store, location)).?;
            defer snapshot.deinit();
            try std.testing.expectEqual(@as(u64, 11), snapshot.get("text").?.applied_sequence);
            try std.testing.expect(snapshot.get("missing") == null);
            // This is the same lock used by every sidecar publication. It is
            // still held after file decoding, not just during readFileAlloc.
            const unexpectedly_unlocked = snapshot.guard.lock.mutex.tryLock();
            if (unexpectedly_unlocked) snapshot.guard.lock.mutex.unlock();
            try std.testing.expect(!unexpectedly_unlocked);
            var txn = try store.beginReadTxn();
            defer txn.abort();
            try snapshot.requireCurrent(&txn);
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, Check.run, .{ db.core.store, sidecar });
    var snapshot = (try tryAcquireProjectionSnapshot(alloc, std.testing.io, db.core.store, sidecar)).?;
    defer snapshot.deinit();
    try std.testing.expect(try tryAcquireProjectionSnapshot(alloc, std.testing.io, db.core.store, sidecar) == null);
    // Physical reset/open revocation need not replace a sidecar. A held file
    // lock therefore cannot replace the primary transaction's final CAS.
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try epoch.revoke(&txn);
        try std.testing.expectError(error.EnrichmentSourceChanged, snapshot.requireCurrent(&txn));
        try txn.commit();
    }
    {
        var txn = try db.core.store.beginReadTxn();
        defer txn.abort();
        try std.testing.expectError(error.EnrichmentSourceChanged, snapshot.requireCurrent(&txn));
    }
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        activation.authority_epoch += 1;
        try publication.stageAuthority(&txn, activation);
        try std.testing.expectError(error.ArtifactCatalogDrift, snapshot.requireCurrent(&txn));
        try txn.commit();
    }
}

test "ordered artifact inventory projection sidecar transitions revoke completion before publication" {
    const alloc = std.testing.allocator;
    const db_mod = @import("antfly_source_root").antfly_sources.physical_db;
    const publication = @import("../artifact_publication.zig");
    const epoch = @import("../artifact_projection_epoch.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/projection-epoch", .{tmp.sub_path});
    defer alloc.free(path);
    var db = try db_mod.DB.open(alloc, path, .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 3 }, .online_source_authority = .raft, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false });
    defer db.close();
    try db.setSchemaJson(alloc, "{}");
    var catalog = try db.artifactInventoryCommand(alloc);
    defer catalog.catalogs.deinit(alloc);
    catalog.binding.effect_protocol = 15;
    try @import("antfly_server_test_sources").local_test_sources.storage_server_db_adapter.applyOrdered(&db, .{ .artifact_catalog = catalog }, .{ .term = 1, .index = 1 });
    var activation: publication.Command = .{ .mode = .activate, .namespace = catalog.namespace, .authority_epoch = catalog.binding.epoch, .catalog_digest = catalog.binding.digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) };
    activation.publication_digest = activation.digest();
    try @import("antfly_server_test_sources").local_test_sources.storage_server_db_adapter.applyOrdered(&db, .{ .artifact_publication = activation }, .{ .term = 1, .index = 2 });
    const sidecar = try checkpointPathAlloc(alloc, path);
    defer alloc.free(sidecar);
    const Probe = struct {
        fn current(store: *docstore_mod.DocStore) !u64 {
            var read = try store.beginReadTxn();
            defer read.abort();
            return epoch.load(&read);
        }
    };
    try std.testing.expectEqual(@as(u64, 0), try Probe.current(db.core.store));
    try saveProjectionCheckpointWithSidecar(alloc, std.testing.io, db.core.store, sidecar, "text", .{ .applied_sequence = 10, .generation = 1, .config_hash = 7 });
    const first = try Probe.current(db.core.store);
    try std.testing.expectEqual(@as(u64, 1), first);
    try saveAppliedSequencesWithCheckpoint(alloc, std.testing.io, db.core.store, sidecar, &.{.{ .index_name = "text", .sequence = 11, .config_hash = 7 }});
    try std.testing.expectEqual(first, try Probe.current(db.core.store));
    try saveAppliedSequenceUpdateWithCheckpoint(alloc, std.testing.io, db.core.store, sidecar, .{ .index_name = "text", .sequence = 12, .config_hash = 7 });
    try std.testing.expectEqual(first, try Probe.current(db.core.store));
    // Rebuilding, even at the same source cut, invalidates prior completion.
    try saveProjectionCheckpointWithSidecar(alloc, std.testing.io, db.core.store, sidecar, "text", .{ .applied_sequence = 12, .status = .rebuilding, .generation = 2, .config_hash = 7 });
    try std.testing.expectEqual(first + 1, try Probe.current(db.core.store));
    // Recovery may deliberately lower a watermark; it cannot retain a prefix
    // captured from the old physical projection.
    try saveAppliedSequenceWithCheckpoint(alloc, std.testing.io, db.core.store, sidecar, "text", 3);
    try std.testing.expectEqual(first + 2, try Probe.current(db.core.store));
    try clearAppliedSequenceWithCheckpoint(alloc, std.testing.io, db.core.store, sidecar, "text");
    try std.testing.expectEqual(first + 3, try Probe.current(db.core.store));
    // A failed sidecar replacement is permitted to leave an extra revocation.
    // The reverse ordering would leave stale evidence after a crash.
    var failed_after_revoke = false;
    for (0..256) |fail_index| {
        var failing = std.testing.FailingAllocator.init(alloc, .{ .fail_index = fail_index });
        saveProjectionCheckpointWithSidecar(failing.allocator(), std.testing.io, db.core.store, sidecar, "text", .{ .applied_sequence = 20, .generation = 3, .config_hash = 7 }) catch |err| {
            if (err != error.OutOfMemory) return err;
            if (try Probe.current(db.core.store) > first + 3) {
                failed_after_revoke = true;
                try std.testing.expectEqual(@as(u64, 0), try loadAppliedSequenceWithCheckpoint(alloc, std.testing.io, db.core.store, sidecar, "text"));
                break;
            }
            continue;
        };
        return error.TestUnexpectedResult;
    }
    try std.testing.expect(failed_after_revoke);
    try std.testing.expectEqual(first + 4, try Probe.current(db.core.store));
    // The metadata-only backend path revokes in the same transaction as reset.
    try saveAppliedSequence(db.core.store, "text", 20);
    const metadata_epoch = try Probe.current(db.core.store);
    try saveAppliedSequence(db.core.store, "text", 21);
    try std.testing.expectEqual(metadata_epoch, try Probe.current(db.core.store));
    {
        var txn = try db.core.store.beginWriteTxn();
        defer txn.abort();
        try clearAppliedSequenceTxn(&txn, "text");
        try std.testing.expectEqual(metadata_epoch + 1, try epoch.load(&txn));
    }
    try std.testing.expectEqual(metadata_epoch, try Probe.current(db.core.store));
    try saveAppliedSequence(db.core.store, "text", 2);
    try std.testing.expectEqual(metadata_epoch + 1, try Probe.current(db.core.store));
}

fn invalidatesCompletion(previous: ?ProjectionCheckpoint, next: ProjectionCheckpoint) bool {
    const old = previous orelse return true;
    return old.status != next.status or old.generation != next.generation or
        old.config_hash != next.config_hash or next.applied_sequence < old.applied_sequence or
        (next.applied_sequence == old.applied_sequence and old.published_count != null and !std.meta.eql(old.published_count, next.published_count));
}

fn revokeCompletion(alloc: Allocator, store: anytype) !void {
    // Pure sidecar codec/concurrency tests have no physical owner. Production
    // wrappers always supply the store whose projections are being changed.
    if (comptime @TypeOf(store) == @TypeOf(null)) return;
    var runtime = try initRuntimeStore(alloc, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginWrite();
    errdefer txn.abort();
    try @import("../artifact_projection_epoch.zig").revoke(&txn);
    try txn.commit();
}

/// Live physical resets must revoke completion before touching their files,
/// even when they do not otherwise replace an applied-sequence sidecar.
pub fn invalidateProjectionCompletion(alloc: Allocator, store: anytype) !void {
    try revokeCompletion(alloc, store);
}

fn saveAppliedSequencesCheckpoint(alloc: Allocator, io: std.Io, path: []const u8, store: anytype, updates: []const AppliedSequenceUpdate) !void {
    var guard = try acquireCheckpointFileLock(path);
    defer guard.release();

    var checkpoint = loadCheckpoint(alloc, io, path) catch |err| switch (err) {
        error.FileNotFound => CheckpointMap{},
        else => return err,
    };
    defer checkpoint.deinit(alloc);

    var revoke = false;
    for (updates) |update| {
        const previous = checkpoint.map.get(update.index_name);
        try checkpoint.putMaxSequenceUpdate(alloc, update);
        revoke = revoke or invalidatesCompletion(previous, checkpoint.map.get(update.index_name).?);
    }
    if (revoke) try revokeCompletion(alloc, store);
    try writeCheckpointAtomically(alloc, io, path, &checkpoint);
}

fn saveProjectionCheckpoints(alloc: Allocator, io: std.Io, path: []const u8, updates: []const AppliedSequenceUpdate) !void {
    var guard = try acquireCheckpointFileLock(path);
    defer guard.release();

    var checkpoint = loadCheckpoint(alloc, io, path) catch |err| switch (err) {
        error.FileNotFound => CheckpointMap{},
        else => return err,
    };
    defer checkpoint.deinit(alloc);

    for (updates) |update| {
        try checkpoint.putMax(alloc, update.index_name, update.checkpoint());
    }
    try writeCheckpointAtomically(alloc, io, path, &checkpoint);
}

fn setAppliedSequencesCheckpoint(alloc: Allocator, io: std.Io, path: []const u8, store: anytype, updates: []const AppliedSequenceUpdate) !void {
    var guard = try acquireCheckpointFileLock(path);
    defer guard.release();

    var checkpoint = loadCheckpoint(alloc, io, path) catch |err| switch (err) {
        error.FileNotFound => CheckpointMap{},
        else => return err,
    };
    defer checkpoint.deinit(alloc);

    var revoke = false;
    for (updates) |update| {
        const previous = checkpoint.map.get(update.index_name);
        try checkpoint.putSequenceUpdate(alloc, update);
        revoke = revoke or invalidatesCompletion(previous, checkpoint.map.get(update.index_name).?);
    }
    if (revoke) try revokeCompletion(alloc, store);
    try writeCheckpointAtomically(alloc, io, path, &checkpoint);
}

fn setProjectionCheckpoints(alloc: Allocator, io: std.Io, path: []const u8, store: anytype, updates: []const AppliedSequenceUpdate) !void {
    var guard = try acquireCheckpointFileLock(path);
    defer guard.release();

    var checkpoint = loadCheckpoint(alloc, io, path) catch |err| switch (err) {
        error.FileNotFound => CheckpointMap{},
        else => return err,
    };
    defer checkpoint.deinit(alloc);

    var revoke = false;
    for (updates) |update| {
        const previous = checkpoint.map.get(update.index_name);
        try checkpoint.put(alloc, update.index_name, update.checkpoint());
        revoke = revoke or invalidatesCompletion(previous, checkpoint.map.get(update.index_name).?);
    }
    if (revoke) try revokeCompletion(alloc, store);
    try writeCheckpointAtomically(alloc, io, path, &checkpoint);
}

fn clearAppliedSequenceCheckpoint(alloc: Allocator, io: std.Io, path: []const u8, store: anytype, index_name: []const u8) !void {
    var guard = try acquireCheckpointFileLock(path);
    defer guard.release();

    // Revoke even when the sidecar is already missing. Absence cannot retain
    // a completion prefix captured before a crash or interrupted replacement.
    try revokeCompletion(alloc, store);
    var checkpoint = loadCheckpoint(alloc, io, path) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };
    defer checkpoint.deinit(alloc);

    checkpoint.remove(alloc, index_name);
    try writeCheckpointAtomically(alloc, io, path, &checkpoint);
}

fn loadCheckpoint(alloc: Allocator, io: std.Io, path: []const u8) !CheckpointMap {
    const raw = try std.Io.Dir.cwd().readFileAlloc(io, path, alloc, .limited(checkpoint_max_bytes));
    defer alloc.free(raw);
    return try decodeCheckpoint(alloc, raw);
}

fn decodeCheckpoint(alloc: Allocator, raw: []const u8) !CheckpointMap {
    if (raw.len < checkpoint_magic.len + 8 + checkpoint_checksum_len) return error.InvalidDerivedApplyState;
    const body = raw[0 .. raw.len - checkpoint_checksum_len];
    const stored_checksum = std.mem.readInt(
        u32,
        raw[raw.len - checkpoint_checksum_len ..][0..checkpoint_checksum_len],
        .little,
    );
    if (Crc32.hash(body) != stored_checksum) return error.InvalidDerivedApplyState;
    if (!std.mem.eql(u8, body[0..checkpoint_magic.len], checkpoint_magic)) return error.InvalidDerivedApplyState;
    var pos: usize = checkpoint_magic.len;
    const format_version = try readCheckpointInt(body, &pos, u32);
    if (format_version != projection_checkpoint_legacy_format_version and
        format_version != projection_checkpoint_format_version)
    {
        return error.InvalidDerivedApplyState;
    }
    const count = try readCheckpointInt(body, &pos, u32);
    const certificate_bytes: usize = if (format_version >= 3) @sizeOf(u8) + @sizeOf(u64) else 0;
    const minimum_entry_bytes = @sizeOf(u32) + @sizeOf(u64) + @sizeOf(u8) + @sizeOf(u64) + @sizeOf(u64) +
        certificate_bytes + 1;
    if (count > (body.len - pos) / minimum_entry_bytes) return error.InvalidDerivedApplyState;

    var checkpoint = CheckpointMap{};
    errdefer checkpoint.deinit(alloc);
    var i: u32 = 0;
    while (i < count) : (i += 1) {
        const name_len = try readCheckpointInt(body, &pos, u32);
        const applied_sequence = try readCheckpointInt(body, &pos, u64);
        const status_raw = try readCheckpointInt(body, &pos, u8);
        const generation = try readCheckpointInt(body, &pos, u64);
        const config_hash = try readCheckpointInt(body, &pos, u64);
        const published_count: ?u64 = if (format_version >= 3) blk: {
            const present = try readCheckpointInt(body, &pos, u8);
            if (present > 1) return error.InvalidDerivedApplyState;
            const value = try readCheckpointInt(body, &pos, u64);
            break :blk if (present == 1) value else null;
        } else null;
        if (name_len == 0 or name_len > std.math.maxInt(u16)) return error.InvalidDerivedApplyState;
        if (name_len > body.len - pos) return error.InvalidDerivedApplyState;
        const name = body[pos .. pos + name_len];
        pos += name_len;
        if (checkpoint.map.contains(name)) return error.InvalidDerivedApplyState;
        const status: ProjectionStatus = switch (status_raw) {
            @backingInt(ProjectionStatus.clean) => .clean,
            @backingInt(ProjectionStatus.rebuilding) => .rebuilding,
            @backingInt(ProjectionStatus.degraded) => .degraded,
            @backingInt(ProjectionStatus.repair_required) => .repair_required,
            else => return error.InvalidDerivedApplyState,
        };
        try checkpoint.putMax(alloc, name, .{
            .applied_sequence = applied_sequence,
            .status = status,
            .generation = generation,
            .config_hash = config_hash,
            .published_count = published_count,
            .format_version = format_version,
        });
    }
    if (pos != body.len) return error.InvalidDerivedApplyState;
    return checkpoint;
}

fn readCheckpointInt(raw: []const u8, pos: *usize, comptime T: type) !T {
    const size = @sizeOf(T);
    if (pos.* + size > raw.len) return error.InvalidDerivedApplyState;
    const out = std.mem.readInt(T, raw[pos.* .. pos.* + size][0..size], .little);
    pos.* += size;
    return out;
}

fn encodeCheckpoint(alloc: Allocator, checkpoint: *const CheckpointMap) ![]u8 {
    if (checkpoint.map.count() > std.math.maxInt(u32)) return error.InvalidDerivedApplyState;
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, checkpoint_magic);
    try appendCheckpointInt(alloc, &out, u32, projection_checkpoint_format_version);
    try appendCheckpointInt(alloc, &out, u32, @intCast(checkpoint.map.count()));

    const names = try alloc.alloc([]const u8, checkpoint.map.count());
    defer alloc.free(names);
    var it = checkpoint.map.iterator();
    var name_idx: usize = 0;
    while (it.next()) |entry| : (name_idx += 1) names[name_idx] = entry.key_ptr.*;
    std.mem.sort([]const u8, names, {}, struct {
        fn lessThan(_: void, lhs: []const u8, rhs: []const u8) bool {
            return std.mem.order(u8, lhs, rhs) == .lt;
        }
    }.lessThan);

    for (names) |name| {
        if (name.len == 0 or name.len > std.math.maxInt(u16)) return error.InvalidDerivedApplyState;
        const value = checkpoint.map.get(name) orelse return error.InvalidDerivedApplyState;
        try appendCheckpointInt(alloc, &out, u32, @intCast(name.len));
        try appendCheckpointInt(alloc, &out, u64, value.applied_sequence);
        try appendCheckpointInt(alloc, &out, u8, @backingInt(value.status));
        try appendCheckpointInt(alloc, &out, u64, value.generation);
        try appendCheckpointInt(alloc, &out, u64, value.config_hash);
        try appendCheckpointInt(alloc, &out, u8, @intFromBool(value.published_count != null));
        try appendCheckpointInt(alloc, &out, u64, value.published_count orelse 0);
        try out.appendSlice(alloc, name);
    }
    try appendCheckpointInt(alloc, &out, u32, Crc32.hash(out.items));
    if (out.items.len > checkpoint_max_bytes) return error.InvalidDerivedApplyState;
    return try out.toOwnedSlice(alloc);
}

fn appendCheckpointInt(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    comptime T: type,
    value: T,
) !void {
    var buf: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &buf, value, .little);
    try out.appendSlice(alloc, &buf);
}

fn writeCheckpointAtomically(alloc: Allocator, io: std.Io, path: []const u8, checkpoint: *const CheckpointMap) !void {
    const encoded = try encodeCheckpoint(alloc, checkpoint);
    defer alloc.free(encoded);

    if (std.fs.path.dirname(path)) |parent| {
        try fs_paths.createDirPathPortable(io, parent);
    }

    const tmp_path = try std.fmt.allocPrint(alloc, "{s}.tmp-{d}", .{ path, platform_time.monotonicNs() });
    defer alloc.free(tmp_path);

    {
        var file = try fs_paths.createFilePortable(io, tmp_path, .{ .truncate = true });
        defer file.close(io);
        var buf: [4096]u8 = undefined;
        var writer = file.writer(io, &buf);
        try writer.interface.writeAll(encoded);
        try writer.end();
        try file.sync(io);
    }
    std.Io.Dir.rename(std.Io.Dir.cwd(), tmp_path, std.Io.Dir.cwd(), path, io) catch |err| {
        std.Io.Dir.cwd().deleteFile(io, tmp_path) catch {};
        return err;
    };
    try fs_paths.syncDirPortable(io, std.fs.path.dirname(path) orelse ".");
}

test "derived apply state works with memory backend store" {
    var backend = mem_backend.Backend.init(std.testing.allocator, .{});
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    try std.testing.expectEqual(@as(u64, 0), try loadAppliedSequence(std.testing.allocator, runtime, "idx"));
    try saveAppliedSequence(runtime, "idx", 27);
    try std.testing.expectEqual(@as(u64, 27), try loadAppliedSequence(std.testing.allocator, runtime, "idx"));
}

test "derived apply state works with lsm backend store" {
    var backend = lsm_backend.Backend.init(std.testing.allocator, .{ .flush_threshold = 2 });
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    try std.testing.expectEqual(@as(u64, 0), try loadAppliedSequence(std.testing.allocator, runtime, "idx"));
    try saveAppliedSequence(runtime, "idx", 31);
    try std.testing.expectEqual(@as(u64, 31), try loadAppliedSequence(std.testing.allocator, runtime, "idx"));
}

test "derived apply state lsm point load does not clone mutable snapshot" {
    var backend = lsm_backend.Backend.init(std.testing.allocator, .{ .flush_threshold = 1024 });
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    try saveAppliedSequence(runtime, "idx", 41);
    const before = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(@as(u64, 41), try loadAppliedSequence(std.testing.allocator, runtime, "idx"));
    const after = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(before.mutable_snapshot_clone_calls, after.mutable_snapshot_clone_calls);
}

test "derived apply state keeps latest lsm value across many flushed overwrites" {
    // Preserve leak checks; allocation backtraces are opt-in for diagnostics.
    var allocator_state: std.heap.SafeAllocator = .init(std.heap.page_allocator, .{ .stack_trace_frames = 0 });
    defer std.debug.assert(allocator_state.deinit() == 0);
    const alloc = if (@import("antfly_platform").env.getenvBool("ANTFLY_TEST_ALLOCATOR_TRACES")) std.testing.allocator else allocator_state.allocator();
    var tmp = @import("../../../common/test_directory.zig").fastTmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path_len = try tmp.dir.realPath(std.testing.io, &path_buf);
    const path = path_buf[0..path_len];

    {
        var backend = try lsm_backend.Backend.open(alloc, path, .{
            .flush_threshold = 1,
            .compact_threshold_runs = 4,
            .level_target_runs_base = 2,
            .level_target_runs_multiplier = 2,
            .level_target_bytes_base = 0,
        });
        defer backend.close();

        var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer runtime.deinit();

        try saveAppliedSequence(runtime, "idx", 0);
        var sequence: u64 = 1;
        while (sequence <= 1024) : (sequence += 1) {
            try saveAppliedSequence(runtime, "idx", sequence);
        }
        try std.testing.expectEqual(@as(u64, 1024), try loadAppliedSequence(alloc, runtime, "idx"));
        const work = backend.snapshotWriteStats();
        try std.testing.expectEqual(@as(u64, 1025), work.flushes);
        try std.testing.expect(work.manifest_writes <= 2 * work.flushes);
        if (@import("antfly_platform").env.getenvBool("ANTFLY_TEST_WORK_PROFILE"))
            std.debug.print("\nWORK flushed-overwrites flushes={d} compactions={d} manifests={d}\n", .{ work.flushes, work.compactions, work.manifest_writes });
    }

    {
        var backend = try lsm_backend.Backend.open(alloc, path, .{
            .flush_threshold = 1,
            .compact_threshold_runs = 4,
            .level_target_runs_base = 2,
            .level_target_runs_multiplier = 2,
            .level_target_bytes_base = 0,
        });
        defer backend.close();

        var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer runtime.deinit();

        try std.testing.expectEqual(@as(u64, 1024), try loadAppliedSequence(alloc, runtime, "idx"));
    }
}

test "derived apply state clear removes persisted sequence" {
    var backend = mem_backend.Backend.init(std.testing.allocator, .{});
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    try saveAppliedSequence(runtime, "idx", 41);
    try clearAppliedSequence(runtime, "idx");
    try std.testing.expectEqual(@as(u64, 0), try loadAppliedSequence(std.testing.allocator, runtime, "idx"));
}

test "derived apply checkpoint is authoritative when configured" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const db_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const checkpoint_path = try checkpointPathAlloc(alloc, db_path);
    defer alloc.free(checkpoint_path);

    var backend = lsm_backend.Backend.init(alloc, .{ .flush_threshold = 2 });
    defer backend.close();
    var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer runtime.deinit();

    try saveAppliedSequence(runtime, "legacy_idx", 7);
    try std.testing.expectEqual(
        @as(u64, 0),
        try loadAppliedSequenceWithCheckpoint(alloc, std.testing.io, runtime, checkpoint_path, "legacy_idx"),
    );
    try std.testing.expectEqual(
        @as(u64, 7),
        try loadAppliedSequenceWithCheckpoint(alloc, std.testing.io, runtime, null, "legacy_idx"),
    );

    try saveAppliedSequencesWithCheckpoint(alloc, std.testing.io, runtime, checkpoint_path, &[_]AppliedSequenceUpdate{
        .{ .index_name = "dense_idx", .sequence = 10 },
        .{ .index_name = "sparse_idx", .sequence = 3 },
    });
    try saveAppliedSequencesWithCheckpoint(alloc, std.testing.io, runtime, checkpoint_path, &[_]AppliedSequenceUpdate{
        .{ .index_name = "dense_idx", .sequence = 12 },
        .{ .index_name = "sparse_idx", .sequence = 2 },
    });

    try std.testing.expectEqual(
        @as(u64, 12),
        try loadAppliedSequenceWithCheckpoint(alloc, std.testing.io, runtime, checkpoint_path, "dense_idx"),
    );
    try std.testing.expectEqual(
        @as(u64, 3),
        try loadAppliedSequenceWithCheckpoint(alloc, std.testing.io, runtime, checkpoint_path, "sparse_idx"),
    );
    try std.testing.expectEqual(
        @as(u64, 0),
        try loadAppliedSequenceWithCheckpoint(alloc, std.testing.io, runtime, checkpoint_path, "legacy_idx"),
    );
}

test "derived apply checkpoint clear removes sidecar entry" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const db_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const checkpoint_path = try checkpointPathAlloc(alloc, db_path);
    defer alloc.free(checkpoint_path);

    var backend = lsm_backend.Backend.init(alloc, .{ .flush_threshold = 2 });
    defer backend.close();
    var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer runtime.deinit();

    try saveAppliedSequenceWithCheckpoint(alloc, std.testing.io, runtime, checkpoint_path, "dense_idx", 22);
    try std.testing.expectEqual(
        @as(u64, 22),
        try loadAppliedSequenceWithCheckpoint(alloc, std.testing.io, runtime, checkpoint_path, "dense_idx"),
    );
    try clearAppliedSequenceWithCheckpoint(alloc, std.testing.io, runtime, checkpoint_path, "dense_idx");
    try std.testing.expectEqual(
        @as(u64, 0),
        try loadAppliedSequenceWithCheckpoint(alloc, std.testing.io, runtime, checkpoint_path, "dense_idx"),
    );
}

test "projection checkpoint sidecar persists status and identity fields" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const db_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const checkpoint_path = try checkpointPathAlloc(alloc, db_path);
    defer alloc.free(checkpoint_path);

    var backend = lsm_backend.Backend.init(alloc, .{ .flush_threshold = 2 });
    defer backend.close();
    var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer runtime.deinit();

    try saveProjectionCheckpointWithSidecar(alloc, std.testing.io, runtime, checkpoint_path, "dense_idx", .{
        .applied_sequence = 44,
        .status = .degraded,
        .generation = 9,
        .config_hash = 0x1234,
        .published_count = 37,
    });

    const checkpoint = try loadProjectionCheckpointWithSidecar(alloc, std.testing.io, runtime, checkpoint_path, "dense_idx");
    try std.testing.expectEqual(@as(u64, 44), checkpoint.applied_sequence);
    try std.testing.expectEqual(ProjectionStatus.degraded, checkpoint.status);
    try std.testing.expectEqual(@as(u64, 9), checkpoint.generation);
    try std.testing.expectEqual(@as(u64, 0x1234), checkpoint.config_hash);
    try std.testing.expectEqual(@as(?u64, 37), checkpoint.published_count);
    try std.testing.expectEqual(
        @as(u64, 44),
        try loadAppliedSequenceWithCheckpoint(alloc, std.testing.io, runtime, checkpoint_path, "dense_idx"),
    );
}

test "projection checkpoint reads v2 without inventing a publication certificate" {
    const alloc = std.testing.allocator;
    const name = "dense_idx";
    var encoded = std.ArrayListUnmanaged(u8).empty;
    defer encoded.deinit(alloc);
    try encoded.appendSlice(alloc, checkpoint_magic);
    try appendCheckpointInt(alloc, &encoded, u32, projection_checkpoint_legacy_format_version);
    try appendCheckpointInt(alloc, &encoded, u32, 1);
    try appendCheckpointInt(alloc, &encoded, u32, name.len);
    try appendCheckpointInt(alloc, &encoded, u64, 44);
    try appendCheckpointInt(alloc, &encoded, u8, @backingInt(ProjectionStatus.clean));
    try appendCheckpointInt(alloc, &encoded, u64, 9);
    try appendCheckpointInt(alloc, &encoded, u64, 0x1234);
    try encoded.appendSlice(alloc, name);
    try appendCheckpointInt(alloc, &encoded, u32, Crc32.hash(encoded.items));

    var checkpoint = try decodeCheckpoint(alloc, encoded.items);
    defer checkpoint.deinit(alloc);
    const value = checkpoint.map.get(name) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(u64, 44), value.applied_sequence);
    try std.testing.expectEqual(ProjectionStatus.clean, value.status);
    try std.testing.expectEqual(@as(u64, 9), value.generation);
    try std.testing.expectEqual(@as(u64, 0x1234), value.config_hash);
    try std.testing.expectEqual(@as(?u64, null), value.published_count);
    try std.testing.expectEqual(projection_checkpoint_legacy_format_version, value.format_version);
}

test "projection checkpoint rejects plausible cursor corruption" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const db_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const checkpoint_path = try checkpointPathAlloc(alloc, db_path);
    defer alloc.free(checkpoint_path);

    var backend = lsm_backend.Backend.init(alloc, .{ .flush_threshold = 2 });
    defer backend.close();
    var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer runtime.deinit();

    try saveProjectionCheckpointWithSidecar(alloc, std.testing.io, runtime, checkpoint_path, "dense_idx", .{
        .applied_sequence = 44,
        .status = .clean,
        .generation = 9,
        .config_hash = 0x1234,
    });

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const raw = try std.Io.Dir.cwd().readFileAlloc(
        io_impl.io(),
        checkpoint_path,
        alloc,
        .limited(checkpoint_max_bytes),
    );
    defer alloc.free(raw);
    const applied_sequence_offset = checkpoint_magic.len + @sizeOf(u32) + @sizeOf(u32) + @sizeOf(u32);
    raw[applied_sequence_offset] ^= 0x40;
    try writeRawCheckpointTestFile(std.testing.io, checkpoint_path, raw);

    try std.testing.expectError(
        error.InvalidDerivedApplyState,
        loadProjectionCheckpointWithSidecar(alloc, std.testing.io, runtime, checkpoint_path, "dense_idx"),
    );
    try std.testing.expectError(
        error.InvalidDerivedApplyState,
        loadAppliedSequenceWithCheckpoint(alloc, std.testing.io, runtime, checkpoint_path, "dense_idx"),
    );
    try std.testing.expectError(
        error.InvalidDerivedApplyState,
        setProjectionCheckpoints(alloc, std.testing.io, checkpoint_path, null, &.{.{
            .index_name = "other_idx",
            .sequence = 99,
        }}),
    );
    const after_failed_update = try std.Io.Dir.cwd().readFileAlloc(
        io_impl.io(),
        checkpoint_path,
        alloc,
        .limited(checkpoint_max_bytes),
    );
    defer alloc.free(after_failed_update);
    try std.testing.expectEqualSlices(u8, raw, after_failed_update);
}

test "projection checkpoint encoding is deterministic by index name" {
    const alloc = std.testing.allocator;
    var left = CheckpointMap{};
    defer left.deinit(alloc);
    try left.put(alloc, "zeta", .{ .applied_sequence = 3 });
    try left.put(alloc, "alpha", .{ .applied_sequence = 7 });

    var right = CheckpointMap{};
    defer right.deinit(alloc);
    try right.put(alloc, "alpha", .{ .applied_sequence = 7 });
    try right.put(alloc, "zeta", .{ .applied_sequence = 3 });

    const left_encoded = try encodeCheckpoint(alloc, &left);
    defer alloc.free(left_encoded);
    const right_encoded = try encodeCheckpoint(alloc, &right);
    defer alloc.free(right_encoded);
    try std.testing.expectEqualSlices(u8, left_encoded, right_encoded);
}

test "applied sequence checkpoint preserves projection metadata" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const db_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const checkpoint_path = try checkpointPathAlloc(alloc, db_path);
    defer alloc.free(checkpoint_path);

    var backend = lsm_backend.Backend.init(alloc, .{ .flush_threshold = 2 });
    defer backend.close();
    var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer runtime.deinit();

    try saveProjectionCheckpointWithSidecar(alloc, std.testing.io, runtime, checkpoint_path, "dense_idx", .{
        .applied_sequence = 44,
        .status = .repair_required,
        .generation = 9,
        .config_hash = 0x1234,
        .published_count = 37,
    });
    try saveAppliedSequenceWithCheckpoint(alloc, std.testing.io, runtime, checkpoint_path, "dense_idx", 45);
    try saveAppliedSequencesWithCheckpoint(alloc, std.testing.io, runtime, checkpoint_path, &[_]AppliedSequenceUpdate{.{
        .index_name = "dense_idx",
        .sequence = 40,
        .generation = 10,
        .config_hash = 0x5678,
    }});

    const checkpoint = try loadProjectionCheckpointWithSidecar(alloc, std.testing.io, runtime, checkpoint_path, "dense_idx");
    try std.testing.expectEqual(@as(u64, 45), checkpoint.applied_sequence);
    try std.testing.expectEqual(ProjectionStatus.repair_required, checkpoint.status);
    try std.testing.expectEqual(@as(u64, 10), checkpoint.generation);
    try std.testing.expectEqual(@as(u64, 0x5678), checkpoint.config_hash);
    try std.testing.expectEqual(@as(?u64, null), checkpoint.published_count);
}

fn writeRawCheckpointTestFile(io: std.Io, path: []const u8, raw: []const u8) !void {
    if (std.fs.path.dirname(path)) |parent| {
        try fs_paths.createDirPathPortable(io, parent);
    }
    var file = try fs_paths.createFilePortable(io, path, .{ .truncate = true });
    defer file.close(io);
    var buf: [4096]u8 = undefined;
    var writer = file.writer(io, &buf);
    try writer.interface.writeAll(raw);
    try writer.end();
}

test "projection checkpoint rejects unknown checkpoint format" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const db_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const checkpoint_path = try checkpointPathAlloc(alloc, db_path);
    defer alloc.free(checkpoint_path);

    var raw = std.ArrayListUnmanaged(u8).empty;
    defer raw.deinit(alloc);
    try raw.appendSlice(alloc, "AFBAD001");
    try appendCheckpointInt(alloc, &raw, u32, 1);
    try appendCheckpointInt(alloc, &raw, u32, @intCast("dense_idx".len));
    try appendCheckpointInt(alloc, &raw, u64, 77);
    try raw.appendSlice(alloc, "dense_idx");
    try writeRawCheckpointTestFile(std.testing.io, checkpoint_path, raw.items);

    var backend = lsm_backend.Backend.init(alloc, .{ .flush_threshold = 2 });
    defer backend.close();
    var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer runtime.deinit();

    try std.testing.expectError(
        error.InvalidDerivedApplyState,
        loadProjectionCheckpointWithSidecar(alloc, std.testing.io, runtime, checkpoint_path, "dense_idx"),
    );
    try std.testing.expectError(
        error.InvalidDerivedApplyState,
        loadAppliedSequenceWithCheckpoint(alloc, std.testing.io, runtime, checkpoint_path, "dense_idx"),
    );
}

test "derived apply checkpoint write locks are scoped per checkpoint path" {
    var guard_a = try acquireCheckpointFileLock(".zig-cache/tmp/derived-apply-a.checkpoint");
    defer guard_a.release();

    var guard_b = try acquireCheckpointFileLock(".zig-cache/tmp/derived-apply-b.checkpoint");
    defer guard_b.release();

    try std.testing.expect(guard_a.lock != guard_b.lock);
}

test "derived apply checkpoint serializes concurrent sidecar writers" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const db_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const checkpoint_path = try checkpointPathAlloc(alloc, db_path);
    defer alloc.free(checkpoint_path);

    const worker_count = 6;
    const Barrier = struct {
        mutex: std.atomic.Mutex = .unlocked,
        waiting: usize = 0,
        open: bool = false,

        fn wait(self: *@This(), total: usize) void {
            var registered = false;
            while (true) {
                lockAtomicMutex(&self.mutex);
                if (!registered) {
                    self.waiting += 1;
                    registered = true;
                    if (self.waiting == total) self.open = true;
                }
                const ready = self.open;
                self.mutex.unlock();
                if (ready) return;
                std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
            }
        }
    };

    const Worker = struct {
        alloc: Allocator,
        io: std.Io,
        path: []const u8,
        name: []const u8,
        sequence: u64,
        barrier: *Barrier,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            self.barrier.wait(worker_count);
            saveAppliedSequencesCheckpoint(self.alloc, self.io, self.path, null, &[_]AppliedSequenceUpdate{.{
                .index_name = self.name,
                .sequence = self.sequence,
            }}) catch |err| {
                self.err = err;
            };
        }
    };

    var barrier = Barrier{};
    var workers: [worker_count]Worker = undefined;
    var threads: [worker_count]std.Io.Future(void) = undefined;
    const names = [_][]const u8{ "idx0", "idx1", "idx2", "idx3", "idx4", "idx5" };
    var started_tasks: usize = 0;
    defer {
        lockAtomicMutex(&barrier.mutex);
        barrier.open = true;
        barrier.mutex.unlock();
        for (threads[0..started_tasks]) |*task| task.await(std.testing.io);
    }
    for (&workers, 0..) |*worker, i| {
        worker.* = .{
            .alloc = alloc,
            .io = std.testing.io,
            .path = checkpoint_path,
            .name = names[i],
            .sequence = @intCast(i + 1),
            .barrier = &barrier,
        };
        threads[i] = try std.testing.io.concurrent(Worker.run, .{worker});
        started_tasks += 1;
    }
    for (&threads) |*thread| thread.await(std.testing.io);
    for (&workers) |*worker| {
        if (worker.err) |err| return err;
    }

    for (names, 0..) |name, i| {
        const checkpoint = (try loadProjectionCheckpoint(alloc, std.testing.io, checkpoint_path, name)) orelse return error.TestUnexpectedResult;
        try std.testing.expectEqual(@as(u64, @intCast(i + 1)), checkpoint.applied_sequence);
        try std.testing.expectEqual(ProjectionStatus.clean, checkpoint.status);
    }
}
