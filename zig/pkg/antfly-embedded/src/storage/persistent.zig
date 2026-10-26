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

//! Persistent full-text index catalog with WAL for crash safety.
//!
//! Wraps IndexWriter (in-memory) with durable metadata. Full-text segment bytes
//! are immutable AFSM segment files when a host file store is available; the
//! metadata backend tracks active segment ids, ranges, deletions, and committed
//! WAL state. The WAL ensures that in-flight batches survive crashes. On
//! recovery, the WAL is replayed to reconstruct any batches that were written
//! but not yet published.
//!
//! Architecture:
//!   PersistentIndex
//!   ├── IndexWriter (existing, in-memory snapshot management)
//!   ├── Segment files (<path>/segments/*.seg) — immutable full-text segments
//!   ├── Main backend (<path>/index/) — metadata/catalog only
//!   └── WAL backend (<path>/wal/) — pending batches for crash recovery
//!
//! Write flow:
//!   1. WAL.append(serialized_batch) → returns LSN (durable)
//!   2. Publish immutable segment file atomically
//!   3. Metadata txn: update active segments/ranges + committed_lsn = LSN
//!   4. IndexWriter.addSegmentWithId(segment) → new in-memory snapshot
//!   4. WAL.truncate(LSN)

const std = @import("std");
const platform_sync = @import("antfly_platform").sync;
const projection_seal = @import("projection_seal.zig");
pub const source_publication = @import("index_publication.zig");
const family_graph = @import("immutable_graph.zig");
const source_family = @import("index_source_family.zig");
const field_generation = @import("index_field_generation.zig");
const field_build = @import("index_field_build.zig");
const field_source = @import("index_field_source.zig");
const source_epoch = @import("index_source_epoch.zig");
pub const exact_source_map = @import("index_source_map.zig");
const source_claims = @import("index_source_claim.zig");
const source_recovery = @import("index_source_recovery.zig");
const builtin = @import("builtin");
// Fixture-only controls compare reader policies without a public runtime flag.
pub var test_force_source_snapshot_reads = false;
pub var test_source_probe_reads: std.atomic.Value(u64) = .init(0);
pub var test_source_point_eligible: std.atomic.Value(bool) = .init(false);

const build_options = @import("build_options");
const Allocator = std.mem.Allocator;
const backend_adapter = @import("backend_adapter.zig");
const backend_erased = @import("backend_erased.zig");
const backend_types = @import("backend_types.zig");
const native_artifact_sink = @import("native_artifact_sink.zig");
const mem_backend = @import("mem_backend.zig");
const lsm_backend = @import("lsm_backend/mod.zig");
const storage_io = lsm_backend.storage_io;
const fs_paths = @import("antfly_runtime_fs").fs_paths;
const CancellationToken = @import("antfly_cancellation").CancellationToken;
const platform_time = @import("antfly_platform").time;
const wal_mod = if (builtin.os.tag == .freestanding) @import("portable_wal.zig") else @import("wal.zig");
const CommitBackend = wal_mod.CommitBackend;
const CommitStats = wal_mod.CommitStats;
const storage_sim = @import("sim_runtime.zig");
const sim_fixture = @import("sim_fixture.zig");
const persistent_sim_fixture = @import("persistent_sim_fixture.zig");
const zig_lmdb = if (builtin.is_test) @import("lmdb_engine") else struct {
    pub const storage_sim_soak = false;
    pub const is_zig_backend = false;
    pub const sim = struct {};
};
const index_mod = @import("../index.zig");
const resource_manager_mod = @import("resource_manager.zig");
const segment_mod = @import("../segment.zig");
const inverted_mod = @import("../section/inverted.zig");
const introducer_mod = @import("../introducer.zig");
const roaring = @import("../encoding/roaring.zig");
const storage_sim_soak = zig_lmdb.storage_sim_soak;

var bench_persistent_publish_cache: std.atomic.Value(u8) = .init(0);

fn getenv(name: [*:0]const u8) ?[*:0]u8 {
    if (!builtin.link_libc) return null;
    return std.c.getenv(name);
}

fn envEnabled(name: [*:0]const u8) bool {
    const raw_z = getenv(name) orelse return false;
    const raw = std.mem.span(raw_z);
    return !(std.mem.eql(u8, raw, "0") or
        std.ascii.eqlIgnoreCase(raw, "false") or
        std.ascii.eqlIgnoreCase(raw, "no"));
}

fn benchPersistentPublishEnabled() bool {
    switch (bench_persistent_publish_cache.load(.monotonic)) {
        1 => return false,
        2 => return true,
        else => {},
    }
    const enabled = envEnabled("ANTFLY_BENCH_PERSISTENT_PUBLISH") or
        envEnabled("ANTFLY_BENCH_TEXT_METRICS") or
        envEnabled("ANTFLY_BENCH_TEXT_PROFILE");
    bench_persistent_publish_cache.store(if (enabled) 2 else 1, .monotonic);
    return enabled;
}

fn nsToMs(ns: u64) u64 {
    return ns / std.time.ns_per_ms;
}

pub const SourceReaderBroker = @import("index_source_broker.zig").Broker;
pub const PersistentIndexOptions = struct {
    source_reader_broker: SourceReaderBroker = .local(),
    /// Monotone optimization hint only; actual snapshot leases remain authority.
    source_owner_hint: ?*std.atomic.Value(bool) = null,
    path: [*:0]const u8,
    io: ?std.Io = null,
    main_backend: MainBackend = .lsm,
    wal_backend: ?wal_mod.StorageBackend = null,
    main_lsm_storage: ?lsm_backend.Storage = null,
    wal_storage: ?lsm_backend.Storage = null,
    main_lsm_options: lsm_backend.Options = .{ .flush_threshold = 1 },
    wal_lsm_options: lsm_backend.Options = .{},
    lsm_cache: ?*lsm_backend.Cache = null,
    lsm_root_generation: u64 = 0,
    read_only: bool = false,
    main_map_size: usize = 256 * 1024 * 1024, // 256MB default
    wal_map_size: usize = 64 * 1024 * 1024, // 64MB default
    main_no_sync: bool = false,
    main_no_meta_sync: bool = false,
    wal_no_sync: bool = false,
    main_commit_backend: CommitBackend = .sync,
    wal_commit_backend: CommitBackend = .adaptive,
    wal_group_commit_window_ns: u64 = 0,
    wal_group_commit_max_requests: usize = 64,
    wal_clock: storage_sim.Clock = storage_sim.real_clock,
    wal_commit_scheduler: storage_sim.CompletionScheduler = storage_sim.real_completion_scheduler,
    model_wal_commit_backend_completions: bool = false,

    pub fn resolvedWalBackend(self: PersistentIndexOptions) wal_mod.StorageBackend {
        return self.wal_backend orelse switch (self.main_backend) {
            .lsm_memory, .mem => .lsm_memory,
            .lsm => .lsm,
        };
    }
};

fn resolvedMainLsmOptions(opts: PersistentIndexOptions, memory_only: bool) lsm_backend.Options {
    var lsm_options = opts.main_lsm_options;
    lsm_options.backend.read_only = opts.read_only;
    if (opts.read_only) lsm_options.backend.create_if_missing = false;
    lsm_options.backend.durability = if (memory_only or opts.main_no_sync) .none else lsm_options.backend.durability;
    if (!memory_only) lsm_options.storage = opts.main_lsm_storage orelse lsm_options.storage;
    lsm_options.cache = opts.lsm_cache orelse lsm_options.cache;
    if (opts.lsm_root_generation != 0 and lsm_options.root_generation == 0) {
        lsm_options.root_generation = opts.lsm_root_generation;
    }
    return lsm_options;
}

test "persistent index defaults to lsm main backend" {
    const opts = PersistentIndexOptions{ .path = "" };
    try std.testing.expectEqual(MainBackend.lsm, opts.main_backend);
}

test "persistent index defaults to lsm wal backend" {
    const opts = PersistentIndexOptions{ .path = "" };
    try std.testing.expectEqual(wal_mod.StorageBackend.lsm, opts.resolvedWalBackend());
}

test "persistent index uses in-memory lsm wal backend for in-memory main stores" {
    try std.testing.expectEqual(wal_mod.StorageBackend.lsm_memory, (PersistentIndexOptions{
        .path = "",
        .main_backend = .mem,
    }).resolvedWalBackend());
    try std.testing.expectEqual(wal_mod.StorageBackend.lsm_memory, (PersistentIndexOptions{
        .path = "",
        .main_backend = .lsm_memory,
    }).resolvedWalBackend());
}

test "persistent index routes main lsm profile options" {
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    defer cleanupPersistDir(path);

    var idx = try PersistentIndex.open(std.testing.allocator, .{
        .path = path,
        .main_backend = .lsm_memory,
        .main_lsm_options = .{ .flush_threshold = 77 },
    });
    defer idx.close();

    switch (idx.main_store_owner) {
        .lsm => |handle| try std.testing.expectEqual(@as(usize, 77), handle.backend.options.flush_threshold),
        else => return error.TestUnexpectedResult,
    }
}

test "persistent index routes wal lsm profile options" {
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    defer cleanupPersistDir(path);

    var idx = try PersistentIndex.open(std.testing.allocator, .{
        .path = path,
        .main_backend = .lsm_memory,
        .wal_lsm_options = .{ .flush_threshold = 88 },
    });
    defer idx.close();

    switch (idx.wal.store_owner) {
        .lsm => |handle| try std.testing.expectEqual(@as(usize, 88), handle.backend.options.flush_threshold),
    }
}

pub const MainBackend = enum {
    mem,
    lsm_memory,
    lsm,
};

pub const PersistentIndexStats = struct {
    wal: wal_mod.WalStats,
    main_commit: ?CommitStats,
};

pub const PersistentIndexMemoryStats = struct {
    segment_virtual_mapped_bytes: u64 = 0,
    segment_estimated_resident_bytes: u64 = 0,
    segment_recently_touched_bytes: u64 = 0,
    segment_cold_mapped_bytes: u64 = 0,
    segment_residency_evictions: u64 = 0,
};

const SegmentFileStore = struct {
    allocator: Allocator,
    root_dir: []u8,
    storage_owner: ?*storage_io.NativeStorage = null,
    storage: storage_io.Storage,

    fn open(
        allocator: Allocator,
        root_dir: []const u8,
        storage: ?storage_io.Storage,
        create_if_missing: bool,
        io_runtime: storage_io.RuntimeKind,
        native_storage_pool: ?*storage_io.NativeStoragePool,
    ) !SegmentFileStore {
        const owned_root = try allocator.dupe(u8, root_dir);
        errdefer allocator.free(owned_root);

        if (storage) |provided| {
            if (create_if_missing) {
                try provided.createDirPath(owned_root);
                // Segment publication fsyncs each immutable file and its
                // containing directory. Publish the directory entry itself
                // first so those acknowledged files remain reachable after a
                // crash that follows initial index creation.
                try provided.syncParentAbsolute(owned_root);
            }
            return .{
                .allocator = allocator,
                .root_dir = owned_root,
                .storage = provided,
            };
        }

        const owner = try allocator.create(storage_io.NativeStorage);
        errdefer allocator.destroy(owner);
        owner.* = try storage_io.NativeStorage.initWithPool(allocator, io_runtime, native_storage_pool);
        errdefer owner.deinit();
        if (create_if_missing) {
            try owner.storage().createDirPath(owned_root);
            try owner.storage().syncParentAbsolute(owned_root);
        }

        return .{
            .allocator = allocator,
            .root_dir = owned_root,
            .storage_owner = owner,
            .storage = owner.storage(),
        };
    }

    fn close(self: *SegmentFileStore) void {
        if (self.storage_owner) |owner| {
            owner.deinit();
            self.allocator.destroy(owner);
        }
        self.allocator.free(self.root_dir);
        self.* = undefined;
    }

    fn pathAlloc(self: *const SegmentFileStore, seg_id: u64) ![]u8 {
        return try std.fmt.allocPrint(self.allocator, "{s}/{d}.seg", .{ self.root_dir, seg_id });
    }

    fn mapFile(self: *SegmentFileStore, path: []const u8) ![]align(std.heap.page_size_min) u8 {
        if (comptime builtin.os.tag == .freestanding) return error.Unsupported;
        const owner = self.storage_owner orelse return error.Unsupported;
        var permit = try owner.acquireFdPermit();
        defer permit.release();
        return try mapSegmentFile(path);
    }

    fn publish(self: *SegmentFileStore, seg_id: u64, bytes: []const u8) !index_mod.SegmentData {
        return try self.publishImpl(seg_id, bytes, true);
    }

    /// Re-materializes an already committed segment. A failed replacement
    /// must retain whichever final path is present so the committed inline
    /// bytes can be retried on the next open.
    fn publishExisting(self: *SegmentFileStore, seg_id: u64, bytes: []const u8) !index_mod.SegmentData {
        return try self.publishImpl(seg_id, bytes, false);
    }

    fn publishImpl(self: *SegmentFileStore, seg_id: u64, bytes: []const u8, delete_final_on_error: bool) !index_mod.SegmentData {
        const path = try self.pathAlloc(seg_id);
        defer self.allocator.free(path);
        errdefer if (delete_final_on_error) self.delete(seg_id);

        if (comptime builtin.os.tag != .freestanding) {
            if (self.storage_owner) |owner| {
                var admission = try NativeSegmentPublicationAdmission.init(owner);
                defer admission.deinit();
                var writer = try admission.beginAtomicWrite(self.allocator, path);
                var active = true;
                defer if (active) writer.abort();
                try writer.appendSlice(bytes);
                active = false;
                writer.finish() catch |err| {
                    std.log.warn("text segment atomic finish failed: {s}", .{@errorName(err)});
                    return err;
                };
                return .fromMapped(try admission.mapFile(path));
            }
        }

        var writer = try self.storage.beginAtomicWrite(self.allocator, path);
        var active = true;
        defer if (active) writer.abort();
        if (writer.vtable.finish_source != null or writer.vtable.finish_mapped != null) writer.setCacheIntent(.cold_sequential);
        try writer.appendSlice(bytes);
        active = false;
        if (writer.vtable.finish_source != null) return .fromNative(try writer.finishSource());
        if (writer.vtable.finish_mapped != null) return .fromMappedArtifact(try writer.finishMapped());
        writer.finish() catch |err| {
            // The atomic writer preserves the previously published segment on
            // failure and the caller receives the error for retry/rollback.
            // This is an operational write failure, not an invariant breach.
            if (builtin.os.tag != .freestanding) std.log.warn("text segment atomic finish failed: {s}", .{@errorName(err)});
            return err;
        };

        if (self.storage.vtable.open_leased_immutable_source != null or self.storage.vtable.map_immutable_artifact != null)
            return self.openPublishedSource(path);
        return .fromOwnedHeap(try self.allocator.dupe(u8, bytes));
    }

    /// Bounded-memory immutable transfer. Contiguous mappings retain their
    /// existing fast path; range-backed stores stream through one fixed buffer.
    fn publishSource(self: *SegmentFileStore, seg_id: u64, source: @import("../segment_source.zig").Source) !index_mod.SegmentData {
        try source.checkReadContext();
        if (source == .contiguous) return self.publish(seg_id, source.contiguous);
        const path = try self.pathAlloc(seg_id);
        defer self.allocator.free(path);
        errdefer self.delete(seg_id);
        if (comptime builtin.os.tag != .freestanding) {
            if (self.storage_owner) |owner| {
                var admission = try NativeSegmentPublicationAdmission.init(owner);
                defer admission.deinit();
                var writer = try admission.beginAtomicWrite(self.allocator, path);
                var active = true;
                defer if (active) writer.abort();
                try streamSource(&writer, source);
                active = false;
                try writer.finish();
                return .fromMapped(try admission.mapFile(path));
            }
        }
        var writer = try self.storage.beginAtomicWrite(self.allocator, path);
        var active = true;
        defer if (active) writer.abort();
        if (writer.vtable.finish_source != null or writer.vtable.finish_mapped != null) writer.setCacheIntent(.cold_sequential);
        if (writer.vtable.finish_source == null and writer.vtable.finish_mapped == null and
            self.storage.vtable.open_leased_immutable_source == null and self.storage.vtable.map_immutable_artifact == null) return error.UnsupportedSourcePublication;
        try streamSource(&writer, source);
        active = false;
        if (writer.vtable.finish_source != null) return .fromNative(try writer.finishSource());
        if (writer.vtable.finish_mapped != null) return .fromMappedArtifact(try writer.finishMapped());
        try writer.finish();
        return self.openPublishedSource(path);
    }

    fn streamSource(writer: *storage_io.AtomicWriteSink, source: @import("../segment_source.zig").Source) !void {
        var buffer: [64 * 1024]u8 = undefined;
        var offset: u64 = 0;
        while (offset < source.len()) {
            const take: usize = @intCast(@min(buffer.len, source.len() - offset));
            try source.readInto(offset, buffer[0..take]);
            try writer.appendSlice(buffer[0..take]);
            offset += take;
        }
    }

    fn openPublishedSource(self: *SegmentFileStore, path: []const u8) !index_mod.SegmentData {
        if (self.storage.vtable.open_leased_immutable_source) |open_source|
            return .fromNative(try open_source(self.storage.ptr, self.allocator, path));
        if (self.storage.vtable.map_immutable_artifact) |map|
            return .fromMappedArtifact(try map(self.storage.ptr, self.allocator, path));
        return error.Unsupported;
    }

    fn mapPublished(self: *SegmentFileStore, seg_id: u64) !index_mod.SegmentData {
        if (self.storage_owner == null) return error.Unsupported;
        const path = try self.pathAlloc(seg_id);
        defer self.allocator.free(path);
        return .fromMapped(try self.mapFile(path));
    }

    fn delete(self: *SegmentFileStore, seg_id: u64) void {
        self.deleteChecked(seg_id) catch |err| {
            if (err == error.EnrichmentWaitCanceled or err == error.Canceled) return;
            if (builtin.os.tag != .freestanding) {
                std.log.warn("text segment cleanup failed segment={d}: {s}", .{ seg_id, @errorName(err) });
            }
        };
    }

    fn deleteChecked(self: *SegmentFileStore, seg_id: u64) !void {
        const path = try self.pathAlloc(seg_id);
        defer self.allocator.free(path);
        self.storage.deleteFileAbsolute(path) catch |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        };
    }

    fn syncDeletes(self: *SegmentFileStore) !void {
        // All segment files share one parent. Passing any child path asks the
        // storage implementation to persist that directory's namespace state.
        const sentinel_path = try self.pathAlloc(0);
        defer self.allocator.free(sentinel_path);
        try self.storage.syncParentAbsolute(sentinel_path);
    }
};

/// Reserves the writer and mmap descriptor slots as one publication admission
/// before any expensive segment construction begins. The writer consumes one
/// permit; the second remains held across rename and the final mmap open.
const NativeSegmentPublicationAdmission = struct {
    owner: *storage_io.NativeStorage,
    writer_permit: storage_io.NativeFdPermit,
    map_permit: storage_io.NativeFdPermit,

    fn init(owner: *storage_io.NativeStorage) !NativeSegmentPublicationAdmission {
        var map_permit = try owner.acquireFdPermits(2);
        errdefer map_permit.release();
        const writer_permit = try map_permit.splitOne();
        return .{
            .owner = owner,
            .writer_permit = writer_permit,
            .map_permit = map_permit,
        };
    }

    fn deinit(self: *NativeSegmentPublicationAdmission) void {
        self.writer_permit.release();
        self.map_permit.release();
        self.* = undefined;
    }

    fn beginAtomicWrite(self: *NativeSegmentPublicationAdmission, allocator: Allocator, path: []const u8) !storage_io.AtomicWriteSink {
        return try self.owner.beginAtomicWriteWithPermit(allocator, path, &self.writer_permit);
    }

    fn mapFile(self: *NativeSegmentPublicationAdmission, path: []const u8) ![]align(std.heap.page_size_min) u8 {
        try self.owner.validateFdPermit(&self.map_permit);
        return try mapSegmentFile(path);
    }
};

const RetiredSegmentFileDeleter = struct {
    const RetryEntry = struct {
        seg_id: u64,
        failure_count: u8 = 0,
        next_attempt_ns: u64 = 0,
    };

    // Keep retirement maintenance below a small, deterministic foreground
    // budget. Inspection is memory-only; attempts may issue filesystem calls.
    const retry_attempts_per_boundary: usize = 8;
    const retry_inspections_per_boundary: usize = 64;
    const completed_markers_per_boundary: usize = 64;
    const retry_base_delay_ns: u64 = 10 * std.time.ns_per_ms;
    const retry_max_delay_ns: u64 = 5 * std.time.ns_per_s;

    allocator: Allocator,
    root_dir: []u8,
    storage: storage_io.Storage,
    native_storage_lease: ?storage_io.NativeStorage.Lease = null,
    refs: std.atomic.Value(usize) = .init(1),
    delete_mu: std.atomic.Mutex = .unlocked,
    delete_enabled: bool = true,
    completed_ids: std.ArrayListUnmanaged(u64) = .empty,
    completed_positions: std.AutoHashMapUnmanaged(u64, usize) = .empty,
    retry_entries: std.ArrayListUnmanaged(RetryEntry) = .empty,
    retry_positions: std.AutoHashMapUnmanaged(u64, usize) = .empty,
    retry_cursor: usize = 0,
    retry_attempts: u64 = 0,
    retry_failures: u64 = 0,

    fn init(allocator: Allocator, store: *const SegmentFileStore) !*RetiredSegmentFileDeleter {
        const deleter = try allocator.create(RetiredSegmentFileDeleter);
        errdefer allocator.destroy(deleter);
        var native_storage_lease: ?storage_io.NativeStorage.Lease = null;
        errdefer if (native_storage_lease) |*lease| lease.deinit();
        if (store.storage_owner) |owner| native_storage_lease = try owner.acquireLease();
        deleter.* = .{
            .allocator = allocator,
            .root_dir = try allocator.dupe(u8, store.root_dir),
            .storage = if (native_storage_lease) |*lease| lease.storage() else store.storage,
            .native_storage_lease = native_storage_lease,
        };
        return deleter;
    }

    fn retain(ptr: *anyopaque) void {
        const self: *RetiredSegmentFileDeleter = @ptrCast(@alignCast(ptr));
        const previous = self.refs.fetchAdd(1, .monotonic);
        std.debug.assert(previous > 0);
    }

    fn release(self: *RetiredSegmentFileDeleter) void {
        if (self.refs.fetchSub(1, .acq_rel) != 1) return;
        const allocator = self.allocator;
        self.completed_ids.deinit(allocator);
        self.completed_positions.deinit(allocator);
        self.retry_entries.deinit(allocator);
        self.retry_positions.deinit(allocator);
        if (self.native_storage_lease) |*lease| lease.deinit();
        allocator.free(self.root_dir);
        allocator.destroy(self);
    }

    fn releaseContext(ptr: *anyopaque) void {
        const self: *RetiredSegmentFileDeleter = @ptrCast(@alignCast(ptr));
        self.release();
    }

    /// A modeled crash rolls storage back before the old in-memory generation
    /// is released. Cleanup callbacks retained by that generation must never
    /// unlink files from the restored generation, even if a snapshot is
    /// released concurrently with teardown.
    fn disarm(self: *RetiredSegmentFileDeleter) void {
        platform_sync.lockYielding(&self.delete_mu);
        defer self.delete_mu.unlock();
        self.delete_enabled = false;
    }

    fn isDisarmed(self: *RetiredSegmentFileDeleter) bool {
        platform_sync.lockYielding(&self.delete_mu);
        defer self.delete_mu.unlock();
        return !self.delete_enabled;
    }

    fn snapshotCompletedIds(self: *RetiredSegmentFileDeleter, allocator: Allocator, max_count: usize) ![]u64 {
        platform_sync.lockYielding(&self.delete_mu);
        defer self.delete_mu.unlock();
        const count = @min(max_count, self.completed_ids.items.len);
        return try allocator.dupe(u64, self.completed_ids.items[0..count]);
    }

    fn acknowledgeCompletedId(self: *RetiredSegmentFileDeleter, seg_id: u64) void {
        platform_sync.lockYielding(&self.delete_mu);
        defer self.delete_mu.unlock();
        self.removeCompletedLocked(seg_id);
    }

    fn appendCompletedLocked(self: *RetiredSegmentFileDeleter, seg_id: u64) !void {
        const position = try self.completed_positions.getOrPut(self.allocator, seg_id);
        if (position.found_existing) return;
        errdefer _ = self.completed_positions.remove(seg_id);
        position.value_ptr.* = self.completed_ids.items.len;
        try self.completed_ids.append(self.allocator, seg_id);
    }

    fn removeCompletedLocked(self: *RetiredSegmentFileDeleter, seg_id: u64) void {
        const removed = self.completed_positions.fetchRemove(seg_id) orelse return;
        const position = removed.value;
        const last_position = self.completed_ids.items.len - 1;
        const moved_id = self.completed_ids.items[last_position];
        _ = self.completed_ids.swapRemove(position);
        if (position != last_position) self.completed_positions.getPtr(moved_id).?.* = position;
    }

    fn enqueueRetryLocked(self: *RetiredSegmentFileDeleter, seg_id: u64) !void {
        const position = try self.retry_positions.getOrPut(self.allocator, seg_id);
        if (position.found_existing) return;
        errdefer _ = self.retry_positions.remove(seg_id);
        position.value_ptr.* = self.retry_entries.items.len;
        try self.retry_entries.append(self.allocator, .{ .seg_id = seg_id });
    }

    fn enqueueRetry(self: *RetiredSegmentFileDeleter, seg_id: u64) !void {
        platform_sync.lockYielding(&self.delete_mu);
        defer self.delete_mu.unlock();
        if (!self.delete_enabled) return;
        try self.enqueueRetryLocked(seg_id);
    }

    fn removeRetryLocked(self: *RetiredSegmentFileDeleter, seg_id: u64) void {
        const removed = self.retry_positions.fetchRemove(seg_id) orelse return;
        const position = removed.value;
        const last_position = self.retry_entries.items.len - 1;
        const moved_entry = self.retry_entries.items[last_position];
        _ = self.retry_entries.swapRemove(position);
        if (position != last_position) self.retry_positions.getPtr(moved_entry.seg_id).?.* = position;
        if (self.retry_entries.items.len == 0) {
            self.retry_cursor = 0;
        } else {
            self.retry_cursor %= self.retry_entries.items.len;
        }
    }

    fn retryDelayNs(failure_count: u8) u64 {
        const exponent: u6 = @intCast(@min(@as(u8, 9), failure_count -| 1));
        return @min(retry_base_delay_ns << exponent, retry_max_delay_ns);
    }

    /// Record a stale retirement marker for a segment which is still active.
    /// Any queued unlink must be cancelled before the marker is cleared.
    fn completeWithoutDelete(self: *RetiredSegmentFileDeleter, seg_id: u64) !void {
        platform_sync.lockYielding(&self.delete_mu);
        defer self.delete_mu.unlock();
        if (!self.delete_enabled) return;
        self.removeRetryLocked(seg_id);
        try self.appendCompletedLocked(seg_id);
    }

    /// Attempt one unlink and retain failures in the live owner. The durable
    /// retirement marker remains authoritative if even queue allocation fails.
    fn deleteOrQueue(self: *RetiredSegmentFileDeleter, seg_id: u64) !void {
        platform_sync.lockYielding(&self.delete_mu);
        defer self.delete_mu.unlock();
        if (!self.delete_enabled) return;

        const path = self.pathAlloc(seg_id) catch {
            try self.enqueueRetryLocked(seg_id);
            return;
        };
        defer self.allocator.free(path);
        self.storage.deleteFileAbsolute(path) catch |err| switch (err) {
            error.FileNotFound => {},
            else => {
                try self.enqueueRetryLocked(seg_id);
                if (builtin.os.tag != .freestanding) {
                    std.log.warn("retired text segment cleanup deferred segment={d}: {s}", .{ seg_id, @errorName(err) });
                }
                return;
            },
        };
        try self.appendCompletedLocked(seg_id);
        self.removeRetryLocked(seg_id);
    }

    fn retryFailedDeletes(self: *RetiredSegmentFileDeleter) void {
        platform_sync.lockYielding(&self.delete_mu);
        defer self.delete_mu.unlock();
        if (!self.delete_enabled) return;

        const now_ns = platform_time.monotonicNs();
        const inspection_limit = @min(retry_inspections_per_boundary, self.retry_entries.items.len);
        var inspections: usize = 0;
        var attempts: usize = 0;
        while (inspections < inspection_limit and
            attempts < retry_attempts_per_boundary and
            self.retry_entries.items.len > 0)
        {
            const position = self.retry_cursor % self.retry_entries.items.len;
            const entry = &self.retry_entries.items[position];
            inspections += 1;
            if (entry.next_attempt_ns > now_ns) {
                self.retry_cursor = (position + 1) % self.retry_entries.items.len;
                continue;
            }

            const seg_id = entry.seg_id;
            attempts += 1;
            self.retry_attempts +|= 1;
            const path = self.pathAlloc(seg_id) catch {
                entry.failure_count +|= 1;
                entry.next_attempt_ns = now_ns +| retryDelayNs(entry.failure_count);
                self.retry_failures +|= 1;
                self.retry_cursor = (position + 1) % self.retry_entries.items.len;
                continue;
            };
            const delete_result = self.storage.deleteFileAbsolute(path);
            self.allocator.free(path);
            delete_result catch |err| switch (err) {
                error.FileNotFound => {},
                else => {
                    entry.failure_count +|= 1;
                    entry.next_attempt_ns = now_ns +| retryDelayNs(entry.failure_count);
                    self.retry_failures +|= 1;
                    self.retry_cursor = (position + 1) % self.retry_entries.items.len;
                    continue;
                },
            };
            self.appendCompletedLocked(seg_id) catch {
                entry.failure_count +|= 1;
                entry.next_attempt_ns = now_ns +| retryDelayNs(entry.failure_count);
                self.retry_failures +|= 1;
                self.retry_cursor = (position + 1) % self.retry_entries.items.len;
                continue;
            };
            self.removeRetryLocked(seg_id);
            if (self.retry_entries.items.len > 0) self.retry_cursor = position % self.retry_entries.items.len;
        }
    }

    fn hasRetirementWork(self: *RetiredSegmentFileDeleter) bool {
        platform_sync.lockYielding(&self.delete_mu);
        defer self.delete_mu.unlock();
        return self.completed_ids.items.len != 0 or self.retry_entries.items.len != 0;
    }

    fn cleanup(self: *RetiredSegmentFileDeleter) index_mod.RetiredSegmentCleanup {
        return .{
            .ptr = self,
            .delete = delete,
            .retain_context = retain,
            .release_context = releaseContext,
        };
    }

    fn pathAlloc(self: *const RetiredSegmentFileDeleter, seg_id: u64) ![]u8 {
        return try std.fmt.allocPrint(self.allocator, "{s}/{d}.seg", .{ self.root_dir, seg_id });
    }

    fn delete(ptr: *anyopaque, seg_id: u64) void {
        const self: *RetiredSegmentFileDeleter = @ptrCast(@alignCast(ptr));
        self.deleteOrQueue(seg_id) catch |err| {
            if (builtin.os.tag != .freestanding) {
                std.log.warn("retired text segment cleanup could not be queued segment={d}: {s}", .{ seg_id, @errorName(err) });
            }
        };
    }
};

const AtomicSegmentSink = struct {
    allocator: Allocator,
    writer: *storage_io.AtomicWriteSink,
    append_buffer: std.ArrayListUnmanaged(u8) = .empty,

    const append_buffer_bytes: usize = 1024 * 1024;

    fn init(allocator: Allocator, writer: *storage_io.AtomicWriteSink) AtomicSegmentSink {
        return .{ .allocator = allocator, .writer = writer };
    }

    fn deinit(self: *AtomicSegmentSink) void {
        self.append_buffer.deinit(self.allocator);
    }

    pub fn flush(self: *AtomicSegmentSink) !void {
        if (self.append_buffer.items.len == 0) return;
        try self.writer.appendSlice(self.append_buffer.items);
        self.append_buffer.clearRetainingCapacity();
    }

    fn sink(self: *AtomicSegmentSink) segment_mod.SegmentSink {
        return .{
            .ptr = self,
            .vtable = &atomic_segment_sink_vtable,
        };
    }

    fn len(ptr: *anyopaque) usize {
        const self: *AtomicSegmentSink = @ptrCast(@alignCast(ptr));
        return self.writer.len() + self.append_buffer.items.len;
    }

    fn appendSlice(ptr: *anyopaque, bytes: []const u8) !void {
        const self: *AtomicSegmentSink = @ptrCast(@alignCast(ptr));
        var remaining = bytes;
        while (remaining.len > 0) {
            if (self.append_buffer.items.len == 0 and remaining.len >= append_buffer_bytes) {
                const direct_len = remaining.len - (remaining.len % append_buffer_bytes);
                try self.writer.appendSlice(remaining[0..direct_len]);
                remaining = remaining[direct_len..];
                continue;
            }

            try self.append_buffer.ensureTotalCapacity(self.allocator, append_buffer_bytes);
            const available = append_buffer_bytes - self.append_buffer.items.len;
            const take = @min(available, remaining.len);
            try self.append_buffer.appendSlice(self.allocator, remaining[0..take]);
            remaining = remaining[take..];
            if (self.append_buffer.items.len == append_buffer_bytes) try self.flush();
        }
    }

    fn appendByte(ptr: *anyopaque, byte: u8) !void {
        const self: *AtomicSegmentSink = @ptrCast(@alignCast(ptr));
        const one = [1]u8{byte};
        try appendSlice(self, &one);
    }

    fn appendNTimes(ptr: *anyopaque, byte: u8, count: usize) !void {
        const self: *AtomicSegmentSink = @ptrCast(@alignCast(ptr));
        var buf: [4096]u8 = undefined;
        @memset(&buf, byte);
        var remaining = count;
        while (remaining > 0) {
            const n = @min(remaining, buf.len);
            try appendSlice(self, buf[0..n]);
            remaining -= n;
        }
    }

    fn writeAt(ptr: *anyopaque, offset: usize, bytes: []const u8) !void {
        const self: *AtomicSegmentSink = @ptrCast(@alignCast(ptr));
        const buffer_start = self.writer.len();
        if (offset >= buffer_start and bytes.len <= self.append_buffer.items.len and offset - buffer_start <= self.append_buffer.items.len - bytes.len) {
            const relative = offset - buffer_start;
            @memcpy(self.append_buffer.items[relative..][0..bytes.len], bytes);
            return;
        }
        try self.flush();
        try self.writer.writeAt(offset, bytes);
    }

    fn residentBytes(ptr: *anyopaque) usize {
        const self: *AtomicSegmentSink = @ptrCast(@alignCast(ptr));
        return self.append_buffer.capacity;
    }

    fn crc32Prefix(ptr: *anyopaque, len_prefix: usize) !u32 {
        const self: *AtomicSegmentSink = @ptrCast(@alignCast(ptr));
        try self.flush();
        return try self.writer.crc32Prefix(len_prefix);
    }

    fn crc32Range(ptr: *anyopaque, offset: usize, range_len: usize) !u32 {
        const self: *AtomicSegmentSink = @ptrCast(@alignCast(ptr));
        try self.flush();
        return try self.writer.crc32Range(offset, range_len);
    }
};

const atomic_segment_sink_vtable = segment_mod.SegmentSink.VTable{
    .len = AtomicSegmentSink.len,
    .append_slice = AtomicSegmentSink.appendSlice,
    .append_byte = AtomicSegmentSink.appendByte,
    .append_ntimes = AtomicSegmentSink.appendNTimes,
    .write_at = AtomicSegmentSink.writeAt,
    .crc32_prefix = AtomicSegmentSink.crc32Prefix,
    .crc32_range = AtomicSegmentSink.crc32Range,
    .resident_bytes = AtomicSegmentSink.residentBytes,
};

fn walCommitBackendForOptions(backend: CommitBackend) wal_mod.CommitBackend {
    return switch (backend) {
        .sync => .sync,
        .worker_thread => .worker_thread,
        .async_io => .async_io,
        .adaptive => .adaptive,
    };
}

pub const SegmentKeyRange = struct {
    seg_id: u64,
    min_doc_key: []u8,
    max_doc_key: []u8,

    pub fn deinit(self: *SegmentKeyRange, alloc: Allocator) void {
        alloc.free(self.min_doc_key);
        alloc.free(self.max_doc_key);
        self.* = undefined;
    }
};

pub const SegmentSplitClass = enum {
    left_only,
    right_only,
    mixed,
};

pub const SegmentSplitPlanEntry = struct {
    seg_id: u64,
    min_doc_key: []u8,
    max_doc_key: []u8,
    class: SegmentSplitClass,

    pub fn deinit(self: *SegmentSplitPlanEntry, alloc: Allocator) void {
        alloc.free(self.min_doc_key);
        alloc.free(self.max_doc_key);
        self.* = undefined;
    }
};

pub const SegmentHandoffResult = struct {
    transferred_segments: usize,
    doc_keys: [][]u8,

    pub fn deinit(self: *SegmentHandoffResult, alloc: Allocator) void {
        for (self.doc_keys) |key| alloc.free(key);
        alloc.free(self.doc_keys);
        self.* = undefined;
    }
};

pub const StoredSegment = struct {
    seg_id: u64,
    segment_bytes: []u8,
    deletion_bitmap_bytes: ?[]u8,

    pub fn deinit(self: *StoredSegment, alloc: Allocator) void {
        alloc.free(self.segment_bytes);
        if (self.deletion_bitmap_bytes) |bytes| alloc.free(bytes);
        self.* = undefined;
    }
};

pub const PackedMergeResource = struct {
    allocator: Allocator,
    resource: field_generation.Resource,
    logical_id: u64,
    fields: ?PersistentIndex.NewSourcePublication,
    transferred: bool = false,
};

pub const PreparedMergeSegment = struct {
    id: u64,
    data: index_mod.SegmentData,
    key_range: SegmentKeyRange,
    /// Owns the prepared list and key-range metadata. It must remain alive
    /// until the prepared merge is either published or discarded.
    staging_alloc: Allocator,
    native_pack: ?*PackedMergeResource = null,

    pub fn deinit(self: *PreparedMergeSegment, data_alloc: Allocator) void {
        self.data.deinit(data_alloc);
        self.key_range.deinit(self.staging_alloc);
        if (self.native_pack) |pack_output| {
            if (pack_output.fields) |*fields| fields.publisher.deinit();
            pack_output.allocator.destroy(pack_output);
        }
        self.* = undefined;
    }
};

/// Meta keys in persistent storage.
const meta_committed_lsn = "committed_lsn";
const meta_rebuild_cursor = "rebuild_snapshot_cursor";
const meta_next_seg_id = "next_seg_id";
const meta_active_segments = "active_segments";
const meta_active_segment_prefix = "active_segment:";
const meta_retired_segment_prefix = "retired_segment:";
const meta_segment_range_prefix = "segment_range:";
pub const text_projection_provenance_meta_key = "text_projection_provenance";
const segments_db_name = "segments";
const meta_db_name = "meta";
const deletions_db_name = "deletions";

fn persistentStorageIo(runtime_io: ?std.Io) std.Io {
    return runtime_io orelse if (comptime @import("builtin").os.tag == .freestanding)
        .failing
    else
        std.Io.Threaded.global_single_threaded.io();
}

test "persistent storage contention yields through borrowed IO during cancellation cleanup" {
    const Probe = struct {
        index: *PersistentIndex,
        waits: usize = 0,
        wakes: usize = 0,
        io: std.Io = undefined,

        fn wait(ptr: ?*anyopaque, _: *const u32, _: u32) void {
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            self.waits += 1;
            self.index.unlockStorage();
        }

        fn wake(ptr: ?*anyopaque, _: *const u32, _: u32) void {
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            self.wakes += 1;
        }
    };
    var index: PersistentIndex = undefined;
    var mutex: std.Io.Mutex = .init;
    index.storage_mu = &mutex;
    var probe = Probe{ .index = &index };
    var vtable: std.Io.VTable = undefined;
    vtable.futexWaitUncancelable = Probe.wait;
    vtable.futexWake = Probe.wake;
    probe.io = .{ .userdata = &probe, .vtable = &vtable };
    index.io = probe.io;
    try std.testing.expect(index.storage_mu.tryLock());
    index.lockStorage();
    index.unlockStorage();
    try std.testing.expectEqual(@as(usize, 1), probe.waits);
    try std.testing.expectEqual(@as(usize, 2), probe.wakes);
}

const MainKeyspace = enum {
    segments,
    meta,
    deletions,
};

fn mapRuntimeNamespace(namespace: backend_types.Namespace) !MainKeyspace {
    if (namespace.name == null) return .meta;
    if (std.mem.eql(u8, namespace.name.?, "segments")) return .segments;
    if (std.mem.eql(u8, namespace.name.?, "meta")) return .meta;
    if (std.mem.eql(u8, namespace.name.?, "deletions")) return .deletions;
    return error.InvalidNamespace;
}

fn runtimeNamespace(keyspace: MainKeyspace) backend_types.Namespace {
    return switch (keyspace) {
        .segments => .{ .name = "segments" },
        .meta => .{ .name = "meta" },
        .deletions => .{ .name = "deletions" },
    };
}

const MainStoreOwner = union(enum) {
    mem: *mem_backend.Backend,
    lsm: lsm_backend.BackendHandle,

    fn close(self: *MainStoreOwner, alloc: Allocator) void {
        switch (self.*) {
            .mem => |backend| {
                backend.close();
                alloc.destroy(backend);
            },
            .lsm => |*handle| handle.close(),
        }
        self.* = undefined;
    }

    fn abandonAfterCrash(self: *MainStoreOwner, alloc: Allocator) void {
        switch (self.*) {
            .mem => |backend| {
                backend.close();
                alloc.destroy(backend);
            },
            .lsm => |*handle| handle.abandonAfterCrash(),
        }
        self.* = undefined;
    }

    fn sync(self: *MainStoreOwner, force: bool) !void {
        switch (self.*) {
            .mem => {},
            .lsm => |*handle| try handle.backend.sync(force),
        }
    }

    fn lsmMaintenanceScore(self: *const MainStoreOwner) u64 {
        return switch (self.*) {
            .mem => 0,
            .lsm => |handle| handle.backend.maintenanceScore(),
        };
    }

    fn snapshotLsmMaintenanceStats(self: *const MainStoreOwner) ?lsm_backend.Backend.MaintenanceStats {
        return switch (self.*) {
            .mem => null,
            .lsm => |handle| handle.backend.snapshotMaintenanceStats(),
        };
    }

    fn snapshotLsmWriteStats(self: *const MainStoreOwner) ?lsm_backend.Backend.WriteStats {
        return switch (self.*) {
            .mem => null,
            .lsm => |handle| handle.backend.snapshotWriteStats(),
        };
    }

    fn snapshotLsmOpenStats(self: *const MainStoreOwner) ?lsm_backend.Backend.OpenStats {
        return switch (self.*) {
            .mem => null,
            .lsm => |handle| handle.backend.snapshotOpenStats(),
        };
    }

    fn checkpointLsmWalAfterDurableBoundary(self: *MainStoreOwner) !void {
        switch (self.*) {
            .mem => {},
            .lsm => |handle| try handle.backend.checkpointWalAfterDurableBoundary(),
        }
    }

    fn pinNativeCheckpoint(self: *MainStoreOwner) !lsm_backend.Backend.NativeCheckpoint {
        return switch (self.*) {
            .mem => error.Unsupported,
            .lsm => |*handle| try handle.backend.pinNativeCheckpoint(),
        };
    }

    fn snapshotLsmNativeStorageStats(self: *const MainStoreOwner) ?lsm_backend.NativeStorageStats {
        return switch (self.*) {
            .mem => null,
            .lsm => |handle| handle.backend.snapshotNativeStorageStats(),
        };
    }

    fn runLsmMaintenanceStep(self: *MainStoreOwner) !bool {
        return switch (self.*) {
            .mem => false,
            .lsm => |*handle| try handle.backend.runMaintenanceStep(),
        };
    }

    fn runLsmMaintenanceStepBestEffort(self: *MainStoreOwner) !bool {
        return switch (self.*) {
            .mem => false,
            .lsm => |*handle| try handle.backend.runMaintenanceStepBestEffort(),
        };
    }

    fn commitStatsSnapshot(self: *MainStoreOwner) ?CommitStats {
        return switch (self.*) {
            .mem, .lsm => null,
        };
    }
};

const OpenedMainStore = struct {
    store: backend_erased.NamespaceStore,
    owner: MainStoreOwner,
};

pub const PersistentIndex = struct {
    source_reader_broker: SourceReaderBroker = .local(),
    /// Monotone optimization hint only; actual snapshot leases remain authority.
    source_owner_hint: ?*std.atomic.Value(bool) = null,
    alloc: Allocator,
    // Each live generation owns its writer, WAL, and stores. Serialize its
    // mutations without fencing unrelated indexes or their IO runtimes.
    // Keep the wait address stable when the catalog moves a quiescent owner.
    storage_mu: *std.Io.Mutex,
    io: ?std.Io = null,
    writer: index_mod.IndexWriter,
    main_store: backend_erased.NamespaceStore,
    main_store_owner: MainStoreOwner,
    segment_files: ?SegmentFileStore,
    retired_segment_file_deleter: ?*RetiredSegmentFileDeleter = null,
    retirement_reconcile_pending: bool = false,
    active_rebuild_page: ?*RebuildPage = null,
    source_roots_present: bool = false,
    source_maintenance: std.atomic.Value(?*SourceMaintenance) = .init(null),
    wal: wal_mod.WAL,
    committed_lsn: u64,
    main_backend: MainBackend,
    wal_backend: wal_mod.StorageBackend,
    main_map_size: usize,
    wal_map_size: usize,
    read_only: bool = false,

    // Only source-owning writers allocate this state. Wake contexts retain
    // it across index close while old snapshots/queries finish; callbacks only
    // set an atomic bit and cannot touch a closed index or start I/O.
    const SourceMaintenance = struct {
        allocator: Allocator,
        owner: source_publication.Digest,
        registry: source_publication.SharedReaders.OwnerRegistry,
        durable: bool = false, // accessed under storage_mu
        closed: bool = false, // monotonic, accessed under storage_mu
        durable_epoch_floor: u64 = 0, // accessed under storage_mu
        refs: std.atomic.Value(usize) = .init(1),
        mutex: std.atomic.Mutex = .unlocked,
        pending: std.atomic.Value(bool) = .init(true),
        scanning: std.atomic.Value(bool) = .init(false),
        sweep: source_recovery.Sweep = .{},
        epoch_enabled: std.atomic.Value(bool) = .init(false),
        families_durable: std.atomic.Value(bool) = .init(false),
        fields_durable: std.atomic.Value(bool) = .init(false),
        family_after: ?source_publication.Digest = null, // maintenance mutex
        field_preparation_after: ?u64 = null, // maintenance mutex
        family_scan_worked: bool = false, // one rescan after a productive pass
        gc_active: bool = false, // owned by maintenance mutex
        gc_after: ?source_publication.Key = null,
        gc_epoch: u64 = 0,
        gc_debt: u64 = 0,
        fn create(a: Allocator, owner: source_publication.Digest, broker: SourceReaderBroker) !*@This() {
            const value = try a.create(@This());
            errdefer a.destroy(value);
            value.* = .{ .allocator = a, .owner = owner, .registry = try broker.open(a, owner) };
            return value;
        }
        fn retain(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const before = self.refs.fetchAdd(1, .monotonic);
            std.debug.assert(before != 0 and before != std.math.maxInt(usize));
        }
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const before = self.refs.fetchSub(1, .acq_rel);
            std.debug.assert(before != 0);
            if (before == 1) {
                self.registry.deinit();
                self.allocator.destroy(self);
            }
        }
        fn notify(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.pending.store(true, .release);
        }
        fn port(self: *@This()) source_publication.SharedReaders.WakePort {
            return .{ .context = self, .retain = retain, .release = release, .notify = notify };
        }
    };

    pub fn sourceOwnershipMaintenancePending(self: *const PersistentIndex) bool {
        const state = self.source_maintenance.load(.acquire) orelse return false;
        return state.pending.load(.acquire) or state.scanning.load(.acquire);
    }

    pub fn noteSourceRecoveryWork(self: *PersistentIndex) void {
        if (self.source_maintenance.load(.acquire)) |state| state.pending.store(true, .release);
    }

    pub fn runSourceOwnershipMaintenanceStep(self: *PersistentIndex, primary: anytype, limit: u32) !?source_recovery.Sweep.Result {
        if (self.read_only) return null;
        const state = self.source_maintenance.load(.acquire) orelse return null;
        @import("antfly_platform").sync.lockYielding(&state.mutex);
        defer state.mutex.unlock();
        if (!state.scanning.load(.acquire) and !state.pending.swap(false, .acq_rel)) return null;
        state.scanning.store(true, .release);
        errdefer state.pending.store(true, .release);
        if (state.families_durable.load(.acquire)) {
            const family_work = try self.collectSourceFamilyGarbageLocked(primary, @min(limit, 64));
            if (family_work.more) return .{ .progress = .{ .worked = family_work.worked }, .scan_more = true };
        }
        if (state.gc_active) {
            const gc = try self.reclaimSourceReceiptWindow(primary, state.gc_after, @min(limit, 64));
            const changed = state.gc_epoch != 0 and state.gc_epoch != gc.current_epoch;
            if (changed) {
                state.gc_after = null;
                state.gc_debt = 0;
                state.gc_epoch = gc.current_epoch;
                return .{ .progress = .{ .worked = gc.purged != 0 }, .scan_more = true };
            }
            state.gc_epoch = gc.current_epoch;
            state.gc_debt +|= gc.current_retired;
            if (gc.more) {
                state.gc_after = gc.after;
                return .{ .progress = .{ .worked = gc.purged != 0 }, .scan_more = true };
            }
            if (!gc.waiting_publishers and state.gc_debt >= source_receipt_epoch_batch) {
                const epoch = try self.advanceSourcePublicationEpoch();
                state.gc_after = null;
                state.gc_debt = 0;
                state.gc_epoch = epoch.current;
                return .{ .progress = .{ .worked = true }, .scan_more = true };
            }
            state.gc_active = false;
            state.gc_after = null;
            state.gc_debt = 0;
            state.scanning.store(false, .release);
            return .{ .progress = .{ .worked = gc.purged != 0, .waiting_publisher = gc.waiting_publishers }, .scan_more = false };
        }
        const result = try state.sweep.step(self.alloc, primary, self, state.owner, limit);
        if (!result.scan_more and state.epoch_enabled.load(.acquire)) {
            state.gc_active = true;
            state.gc_after = null;
            state.gc_epoch = 0;
            state.gc_debt = 0;
            return .{ .progress = result.progress, .scan_more = true };
        }
        state.scanning.store(result.scan_more, .release);
        return result;
    }

    // Coalesce cold epoch fences rather than forcing two durability barriers
    // for every retired segment. Old-reader debt may survive until its pins end.
    pub const source_receipt_epoch_batch: u32 = 256;

    /// A private candidate publishes all segments produced by one source page
    /// together with its cursor. Until commit they are unreachable orphan files
    /// (or owned buffers), so a crash cannot expose a partial or duplicate page.
    /// The candidate's single repair writer owns this synchronous scope.
    pub const RebuildPage = struct {
        index: *PersistentIndex,
        cursor: []const u8,
        segments: std.ArrayListUnmanaged(index_mod.ReplacementSegmentData) = .empty,
        active: bool = false,

        pub fn begin(self: *RebuildPage) !void {
            if (self.index.read_only) return error.ReadOnly;
            if (self.index.active_rebuild_page != null) return error.InvalidArgument;
            self.index.active_rebuild_page = self;
            self.active = true;
        }

        pub fn deinit(self: *RebuildPage) void {
            if (self.active) self.index.active_rebuild_page = null;
            for (self.segments.items) |*segment| {
                segment.data.deinit(self.index.alloc);
                self.index.deleteSegmentFile(segment.id);
            }
            self.segments.deinit(self.index.alloc);
            self.* = undefined;
        }

        pub fn commit(self: *RebuildPage) !void {
            std.debug.assert(self.active and self.index.active_rebuild_page == self);
            const index = self.index;
            index.lockStorage();
            defer index.unlockStorage();
            var publication = if (self.segments.items.len != 0)
                try index.writer.prepareSegmentsManyData(&.{}, self.segments.items)
            else
                null;
            defer if (publication) |*prepared| prepared.abort();
            var txn = try index.beginWriteMainTxn();
            errdefer txn.abort();
            for (self.segments.items) |segment| {
                var range = try extractSegmentDataKeyRange(index.alloc, segment.data);
                defer range.deinit(index.alloc);
                const key = std.mem.toBytes(std.mem.nativeToBig(u64, segment.id));
                if (index.segment_files == null) try txn.put(.segments, &key, segment.data.bytes());
                try index.saveSegmentRange(&txn, segment.id, range);
                try index.updateActiveSegments(&txn, segment.id, .add);
            }
            try saveNextSegmentId(&txn, index.writer.next_segment_id);
            try txn.put(.meta, meta_rebuild_cursor, self.cursor);
            try txn.commit();
            if (publication) |*prepared| prepared.publish();
            self.segments.clearRetainingCapacity(); // ownership moved to writer
            index.active_rebuild_page = null;
            self.active = false;
            // The external repair intent may now advance independently. Its
            // cursor can lag this transaction, but cannot outrun durable pages.
            try index.main_store_owner.sync(true);
        }
    };

    pub fn rebuildCursorAlloc(self: *PersistentIndex, alloc: Allocator) !?[]u8 {
        self.lockStorage();
        defer self.unlockStorage();
        var txn = try self.beginReadMainTxn();
        defer txn.abort();
        const cursor = txn.get(.meta, meta_rebuild_cursor) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        return try alloc.dupe(u8, cursor);
    }

    pub const BackendStore = backend_adapter.Store(PersistentIndex, MainTxn, MainTxn, MainTxn, .{
        .capabilities = backendCapabilities,
        .begin_read = beginReadMainTxn,
        .begin_write = beginWriteMainTxn,
        .begin_batch = beginWriteMainTxn,
    });

    pub const SegmentSinkBuildFn = *const fn (*anyopaque, *segment_mod.SegmentSink) anyerror!void;

    pub fn privateScratchDirectory(self: *const PersistentIndex, fallback: []const u8) ?[]const u8 {
        if (self.segment_files) |store| return store.storage.privateScratchDirectory(fallback);
        return fallback;
    }

    pub fn supportsFileBackedSegmentArtifacts(self: *const PersistentIndex) bool {
        const store = self.segment_files orelse return false;
        return store.storage_owner != null or store.storage.vtable.open_leased_immutable_source != null or store.storage.vtable.map_immutable_artifact != null;
    }

    pub fn attachResourceManager(self: *PersistentIndex, manager: *resource_manager_mod.ResourceManager) void {
        self.writer.attachResourceManager(manager);
    }

    /// Publication records share the active-segment metadata transaction.
    /// This adapter intentionally exposes no independent commit operation.
    const source_owner_key = source_epoch.owner_key;
    const source_owner_closed_key = "index_source_owner_closed_v1";

    fn sourceOwnerClosedInTxn(txn: *MainTxn) !bool {
        const raw = txn.get(.meta, source_owner_closed_key) catch |err| switch (err) {
            error.NotFound => return false,
            else => return err,
        };
        if (raw.len != 68 or !std.mem.eql(u8, raw[0..4], "IOC1")) return error.InvalidIndexPublication;
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(raw[0..36], &digest, .{});
        if (!std.mem.eql(u8, raw[36..68], &digest)) return error.InvalidIndexPublication;
        const expected = raw[4..36].*;
        const owner = (try sourceOwnerInTxn(txn)) orelse return error.InvalidIndexPublication;
        if (!std.mem.eql(u8, &owner, &expected)) return error.IndexSourceOwnerMismatch;
        return true;
    }

    fn requireSourceOwnerOpen(txn: *MainTxn) !void {
        if (try sourceOwnerClosedInTxn(txn)) return error.IndexSourceOwnerClosed;
    }

    /// Permanently fences this physical owner, including preparations already
    /// between their primary and index commits. Existing readers and receipts
    /// remain valid. Catalog removal must durably retain the recovery route
    /// before calling this; this operation alone does not delete source claims.
    pub fn closeSourceOwner(self: *PersistentIndex, owner: source_publication.Digest) !void {
        if (self.read_only) return error.ReadOnly;
        self.lockStorage();
        defer self.unlockStorage();
        var txn = try self.beginWriteMainTxn();
        errdefer txn.abort();
        const actual = (try sourceOwnerInTxn(&txn)) orelse return error.IndexSourceOwnerMismatch;
        if (!std.mem.eql(u8, &actual, &owner)) return error.IndexSourceOwnerMismatch;
        if (!try sourceOwnerClosedInTxn(&txn)) {
            var raw: [68]u8 = undefined;
            @memcpy(raw[0..4], "IOC1");
            @memcpy(raw[4..36], &owner);
            std.crypto.hash.sha2.Sha256.hash(raw[0..36], raw[36..68], .{});
            try txn.put(.meta, source_owner_closed_key, &raw);
        }
        try txn.commit();
        if (self.source_maintenance.load(.acquire)) |state| {
            state.closed = true;
            state.pending.store(true, .release);
        }
        try self.main_store_owner.sync(true);
    }

    fn sourceOwnerRecordInTxn(txn: *MainTxn) !?source_epoch.Owner {
        const raw = txn.get(.meta, source_owner_key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        return try source_epoch.Owner.decode(raw);
    }
    fn sourceOwnerInTxn(txn: *MainTxn) !?source_publication.Digest {
        return if (try sourceOwnerRecordInTxn(txn)) |record| record.identity else null;
    }
    fn sourcePublicationEpoch(txn: *MainTxn, binding: source_publication.Binding) !u64 {
        try requireSourceOwnerOpen(txn);
        const owner = (try sourceOwnerRecordInTxn(txn)) orelse return error.IndexSourceOwnerMismatch;
        return owner.admitPublication(binding);
    }

    /// Native factory; callers retain its binding through the complete
    /// preparation/publication retry. Caller entropy never supplies an epoch.
    pub fn newSourcePublicationBinding(self: *PersistentIndex, io: std.Io) !source_publication.Binding {
        self.lockStorage();
        defer self.unlockStorage();
        var read = try self.beginReadMainTxn();
        defer read.abort();
        try requireSourceOwnerOpen(&read);
        const owner = (try sourceOwnerRecordInTxn(&read)) orelse return error.IndexSourceOwnerMismatch;
        var entropy: [16]u8 = undefined;
        io.random(&entropy);
        return owner.newBinding(entropy);
    }

    pub const NewSourcePublication = struct {
        binding: source_publication.Binding,
        publisher: source_publication.SharedReaders.PublisherRegistration,
    };
    /// Owns staged files, source claims and publisher registrations across the
    /// unlocked producer phase. Cleanup retains its storage owner independently
    /// of movable catalog entries and index removal. No primary bytes are kept.
    pub const PreparedSourceFamily = struct {
        alloc: Allocator,
        data_alloc: Allocator,
        physical: ?PreparedMergeSegment,
        layout: source_family.Layout,
        stamp: ?@import("../segment_source.zig").ContentStamp = null,
        field_family: ?*PreparedFieldSourceFamily = null,
        leaves: []source_recovery.Prepared,
        deletions: ?roaring.RoaringBitmap = null,
        parent: NewSourcePublication,
        cleanup: *RetiredSegmentFileDeleter,

        pub fn jsonStringify(_: PreparedSourceFamily, _: anytype) !void {
            return error.NativeIndexSourceCannotSerialize;
        }
        pub fn jsonParse(_: Allocator, _: anytype, _: std.json.ParseOptions) !PreparedSourceFamily {
            return error.NativeIndexSourceCannotSerialize;
        }

        pub fn deinit(self: *PreparedSourceFamily) void {
            if (self.field_family) |field| {
                field.deinit();
                self.alloc.destroy(field);
            }
            if (self.deletions) |*deleted| deleted.deinit();
            self.parent.publisher.deinit();
            for (self.leaves) |*leaf| leaf.deinit(self.alloc);
            self.alloc.free(self.leaves);
            if (self.physical) |*physical| {
                const id = physical.id;
                physical.deinit(self.data_alloc);
                RetiredSegmentFileDeleter.delete(self.cleanup, id);
            }
            self.cleanup.release();
            self.* = undefined;
        }
        pub fn publish(self: *PreparedSourceFamily, index: *PersistentIndex, primary: anytype) !source_publication.Decision {
            if (self.field_family) |field| {
                field.layout.transfer = self.layout.transfer;
                const result = try field.publish(index, primary);
                self.layout = field.layout;
                return result;
            }
            const physical = self.physical orelse return error.InvalidIndexSourceFamily;
            // append consumes the mapping on every path, including ambiguous
            // commit errors. Its catalog proof controls physical-file deletion.
            self.physical = null;
            return index.appendPreparedSourceFamilyImpl(primary, physical, self.layout, self.leaves, &self.parent, self.stamp, if (self.deletions) |*deleted| deleted else null);
        }
    };

    /// Materialize and force the immutable artifact and primary claims before
    /// entering the managed-index apply guard. Only catalog publication remains
    /// in that serialized section. Input observations remain borrowed here only.
    pub fn prepareSourceFamily(self: *PersistentIndex, a: Allocator, primary: anytype, inputs: []const source_recovery.Input, bytes: []u8, configuration: source_publication.Digest, io: std.Io) !PreparedSourceFamily {
        if (inputs.len == 0 or inputs.len > std.math.maxInt(u32)) return error.InvalidIndexSourceFamily;
        const cleanup = self.retired_segment_file_deleter orelse return error.UnsupportedSourcePublication;
        RetiredSegmentFileDeleter.retain(cleanup);
        errdefer cleanup.release();
        const owner = try self.ensureSourceOwner(io);
        const artifacts = try self.prepareSourceSegmentCopies(a, &.{bytes});
        defer a.free(artifacts);
        errdefer {
            const id = artifacts[0].id;
            artifacts[0].deinit(self.alloc);
            RetiredSegmentFileDeleter.delete(cleanup, id);
        }
        // Discard can outlive this index handle. Persist the reserved ID range
        // before handing ownership out, so reopening the same physical root
        // cannot reuse a path that a delayed discard still owns.
        {
            self.lockStorage();
            defer self.unlockStorage();
            var txn = try self.beginWriteMainTxn();
            errdefer txn.abort();
            try saveNextSegmentId(&txn, self.writer.next_segment_id);
            try txn.commit();
            try self.main_store_owner.sync(true);
        }
        // Hash the retained immutable mapping once, outside the managed apply
        // guard. The native stamp binds this capability through publication.
        const stamp = try @import("../segment_source.zig").ContentStamp.capture(artifacts[0].data.source());
        const layout: source_family.Layout = .{ .segment = artifacts[0].id, .documents = @intCast(inputs.len), .physical = stamp.digest, .configuration = configuration };
        const cohort_count = std.math.divCeil(usize, inputs.len, source_family.cohort_capacity) catch unreachable;
        const cohorts = try a.alloc([]const source_recovery.Input, cohort_count);
        defer a.free(cohorts);
        const identities = try a.alloc(source_publication.Digest, cohort_count);
        defer a.free(identities);
        for (cohorts, identities, 0..) |*rows, *identity, ordinal| {
            const first = ordinal * source_family.cohort_capacity;
            rows.* = inputs[first..@min(inputs.len, first + source_family.cohort_capacity)];
            identity.* = try layout.identity(@intCast(first));
        }
        const leaves = try source_recovery.prepareSourceLeaves(a, primary, self, owner, cohorts, identities, .{}, io);
        errdefer {
            for (leaves) |*leaf| leaf.deinit(a);
            a.free(leaves);
        }
        const parent = try self.beginNewSourcePublication(io);
        return .{ .alloc = a, .data_alloc = self.alloc, .physical = artifacts[0], .layout = layout, .stamp = stamp, .leaves = leaves, .parent = parent, .cleanup = cleanup };
    }

    fn fieldOrdinalInventory(self: *PersistentIndex, a: Allocator, reader: *const segment_mod.SegmentReader) !source_publication.Digest {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly/field-ordinal-inventory/v1\x00");
        const state = self.source_maintenance.load(.acquire) orelse return error.IndexSourceOwnerMismatch;
        hash.update(&state.owner);
        var raw: [8]u8 = undefined;
        std.mem.writeInt(u64, &raw, reader.doc_count, .little);
        hash.update(&raw);
        for (0..reader.doc_count) |ordinal| {
            const id = (try reader.storedIdAlloc(a, @intCast(ordinal))) orelse return error.InvalidIndexSourceMap;
            defer a.free(id);
            std.mem.writeInt(u64, &raw, id.len, .little);
            hash.update(&raw);
            hash.update(id);
            const canonical = try reader.docOrdinal(@intCast(ordinal));
            hash.update(&.{@intFromBool(canonical != null)});
            if (canonical) |value| {
                std.mem.writeInt(u64, &raw, value, .little);
                hash.update(&raw);
            }
        }
        return hash.finalResult();
    }
    fn admitFieldReuse(a: Allocator, field_names: anytype, documents: u32, ordinal: source_publication.Digest, configuration: source_publication.Digest, inputs: anytype, metadata: anytype, scope: *const source_publication.SharedReaders.Lease) !bool {
        const held = try family_graph.openHeldFields(metadata, scope);
        const directory = try field_generation.describeFields(&held);
        if (directory.documents != documents or !std.mem.eql(u8, &directory.ordinal_inventory, &ordinal) or !std.mem.eql(u8, &directory.configuration, &configuration)) return false;
        var positions: std.AutoHashMapUnmanaged(source_publication.Digest, u8) = .empty;
        defer positions.deinit(a);
        var resources: [63]field_generation.Resource = undefined;
        var retained: [63]u64 = @splat(0);
        var count: usize = 0;
        for (field_names) |field| {
            if (field.name.len != 0 and field.name[0] == 0) continue;
            var low: usize = 0;
            var high: usize = directory.fields;
            while (low < high) {
                const middle = low + (high - low) / 2;
                const selected = try field_generation.selectFields(metadata, &held, @intCast(middle));
                const prior = try selected.field.descriptor();
                switch (std.mem.order(u8, prior.name(), field.name)) {
                    .lt => low = middle + 1,
                    .gt => high = middle,
                    .eq => {
                        const input = try inputs.inputForField(field.name);
                        if (!std.mem.eql(u8, &prior.inputs(), &input)) break;
                        for (0..prior.sectionCount()) |section_index| {
                            const section = try prior.section(section_index);
                            const member = try selected.field.resource(metadata, section);
                            const resource = try member.descriptor();
                            const slot = positions.get(resource.identity) orelse added: {
                                if (count == resources.len) return false;
                                try positions.put(a, resource.identity, @intCast(count));
                                resources[count] = resource;
                                count += 1;
                                break :added count - 1;
                            };
                            retained[slot] = @min(resource.payload_bytes, retained[slot] +| section.length);
                        }
                        break;
                    },
                }
            }
        }
        if (count == 0) return false;
        // A tiny surviving section must not retain an obsolete large payload.
        // Native packs append distinct section ranges. Repack on low utility;
        // the same bounded rule handles update history and mixed shapes. This
        // metadata pass acquires no physical sources or pack body pages.
        for (resources[0..count], retained[0..count]) |resource, live| {
            if (live < resource.payload_bytes / 2 + resource.payload_bytes % 2) return false;
        }
        return true;
    }
    fn prepareNativeFieldPlan(self: *PersistentIndex, a: Allocator, reader: *const segment_mod.SegmentReader, encoder: *@import("../field_pack.zig").Encoder, file: u64, ordinal: source_publication.Digest, configuration: source_publication.Digest, inputs: anytype) !field_build.Plan {
        // Weak inventory pointers carry no ownership. Select and pin under
        // the same lock as graph retirement; release before encoding sections.
        self.lockStorage();
        var locked = true;
        defer if (locked) self.unlockStorage();
        var read = try self.beginReadMainTxn();
        defer read.abort();
        const previous: ?source_publication.Binding = candidate: {
            const raw = read.get(.meta, &fieldInventoryKey(ordinal)) catch |err| switch (err) {
                error.NotFound => break :candidate null,
                else => return err,
            };
            break :candidate try decodeFieldBinding(raw);
        };
        if (previous) |binding| reuse: {
            var metadata: PublicationTxn = .{ .txn = &read };
            const decision = (try source_publication.decision(&metadata, binding)) orelse break :reuse;
            if (decision.state != .published) break :reuse;
            var readers = try self.openSourceReaders(binding);
            defer readers.deinit();
            var scope = readers.lease() catch |err| switch (err) {
                error.IndexSourceLeaseClosed => break :reuse,
                else => return err,
            };
            defer scope.deinit();
            // Admission bounds fanout and dead bytes retained by sharing. It
            // acquires no pack files merely to decide whether sharing pays.
            if (!try admitFieldReuse(a, reader.fields, reader.doc_count, ordinal, configuration, inputs, &metadata, &scope)) break :reuse;
            self.unlockStorage();
            locked = false;
            return field_build.prepareIncremental(a, reader, encoder, file, ordinal, configuration, inputs, &metadata, &scope);
        }
        self.unlockStorage();
        locked = false;
        return field_build.prepare(a, reader, encoder, file, ordinal, configuration, inputs);
    }

    pub fn copyFieldInputLedger(self: *PersistentIndex, a: Allocator, inventory: field_source.Inventory, name: []const u8) !?[]u8 {
        const ledger = @import("index_field_ledger.zig");
        var scope = try inventory.lease();
        defer scope.deinit();
        var read = try self.beginReadMainTxn();
        defer read.abort();
        var metadata: PublicationTxn = .{ .txn = &read };
        const root = try family_graph.openHeldFields(&metadata, &scope);
        const directory = try field_generation.describeFields(&root);
        var low: u32 = 0;
        var high = directory.fields;
        while (low < high) {
            const middle = low + (high - low) / 2;
            const selected = try field_generation.selectFields(&metadata, &root, middle);
            const field = try selected.field.descriptor();
            switch (std.mem.order(u8, field.name(), name)) {
                .lt => low = middle + 1,
                .gt => high = middle,
                .eq => {
                    const key = ledger.key(root.binding.owner, field.inputs());
                    const raw = metadata.get(&key) catch |err| switch (err) {
                        error.NotFound => return null,
                        else => return err,
                    };
                    const view = ledger.open(raw, field.inputs()) catch return null;
                    if (view.documents() != directory.documents) return null;
                    return try a.dupe(u8, raw);
                },
            }
        }
        return null;
    }

    pub const FieldCompilation = struct {
        allocator: Allocator,
        lease: source_publication.SharedReaders.Lease,
        names: [][]u8,
        ordinal: source_publication.Digest,
        configuration: source_publication.Digest,
        copy_inventory: ?field_source.Inventory = null,
        remap_rows: ?[]u32 = null,
        remap_evidence: ?[][]u8 = null,
        pub fn deinit(self: *@This()) void {
            if (self.copy_inventory) |*inventory| inventory.deinit();
            if (self.remap_rows) |rows| self.allocator.free(rows);
            if (self.remap_evidence) |evidence| {
                for (evidence) |raw| self.allocator.free(raw);
                self.allocator.free(evidence);
            }
            for (self.names) |name| self.allocator.free(name);
            self.allocator.free(self.names);
            self.lease.deinit();
            self.* = undefined;
        }
    };

    /// Establish raw-input equality before tokenization. The actual native
    /// field pin remains owned across compilation; publication rechecks it.
    pub fn prepareFieldCompilation(self: *PersistentIndex, a: Allocator, docs: []const @import("../introducer.zig").TextDocument, inputs: *const @import("index_field_inputs.zig").Inputs, configuration: source_publication.Digest) !?FieldCompilation {
        if (docs.len == 0 or docs.len > std.math.maxInt(u32) or !self.sourceFieldsEnabled()) return null;
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly/field-ordinal-inventory/v1\x00");
        const state = self.source_maintenance.load(.acquire) orelse return null;
        hash.update(&state.owner);
        var raw: [8]u8 = undefined;
        std.mem.writeInt(u64, &raw, docs.len, .little);
        hash.update(&raw);
        for (docs) |doc| {
            std.mem.writeInt(u64, &raw, doc.id.len, .little);
            hash.update(&raw);
            hash.update(doc.id);
            // The compiler encodes zero as an absent canonical coordinate.
            const canonical = if (doc.doc_ordinal) |value| if (value != 0) @as(?u32, value) else null else null;
            hash.update(&.{@intFromBool(canonical != null)});
            if (canonical) |value| {
                std.mem.writeInt(u64, &raw, value, .little);
                hash.update(&raw);
            }
        }
        const ordinal = hash.finalResult();
        self.lockStorage();
        var locked = true;
        defer if (locked) self.unlockStorage();
        var read = try self.beginReadMainTxn();
        defer read.abort();
        const encoded = read.get(.meta, &fieldInventoryKey(ordinal)) catch |err| switch (err) {
            error.NotFound => {
                self.unlockStorage();
                locked = false;
                return self.prepareRemappedFieldCompilation(a, docs, inputs, configuration, ordinal);
            },
            else => return err,
        };
        const binding = try decodeFieldBinding(encoded);
        var metadata: PublicationTxn = .{ .txn = &read };
        const decision = try source_publication.decision(&metadata, binding);
        if (decision == null or decision.?.state != .published) {
            self.unlockStorage();
            locked = false;
            return self.prepareRemappedFieldCompilation(a, docs, inputs, configuration, ordinal);
        }
        var readers = try self.openSourceReaders(binding);
        defer readers.deinit();
        var scope = readers.lease() catch |err| switch (err) {
            error.IndexSourceLeaseClosed => return null,
            else => return err,
        };
        var owns_scope = true;
        defer if (owns_scope) scope.deinit();
        const names = try inputs.fieldNames(a);
        defer a.free(names);
        const retain_packs = try admitFieldReuse(a, names, @intCast(docs.len), ordinal, configuration, inputs, &metadata, &scope);
        const held = try family_graph.openHeldFields(&metadata, &scope);
        const directory = try field_generation.describeFields(&held);
        if (directory.documents != docs.len or !std.mem.eql(u8, &directory.ordinal_inventory, &ordinal) or !std.mem.eql(u8, &directory.configuration, &configuration)) return null;
        var skipped: std.ArrayListUnmanaged([]u8) = .empty;
        errdefer {
            for (skipped.items) |name| a.free(name);
            skipped.deinit(a);
        }
        for (0..directory.fields) |index| {
            const selected = try field_generation.selectFields(&metadata, &held, @intCast(index));
            const field = try selected.field.descriptor();
            if (!inputs.hasField(field.name()) or !std.mem.eql(u8, &field.inputs(), &try inputs.inputForField(field.name()))) continue;
            const name = try a.dupe(u8, field.name());
            errdefer a.free(name);
            try skipped.append(a, name);
        }
        if (skipped.items.len == 0) {
            skipped.deinit(a);
            return null;
        }
        const owned_names = try skipped.toOwnedSlice(a);
        errdefer {
            for (owned_names) |name| a.free(name);
            a.free(owned_names);
        }
        // The actual field pin now protects the graph. Capture exact physical
        // versions outside the publication lock only when sections need copying.
        self.unlockStorage();
        locked = false;
        const store = self.segment_files orelse return error.UnsupportedSourcePublication;
        const inventory: ?field_source.Inventory = if (retain_packs) null else try field_source.open(a, &metadata, &scope, store.storage, store.root_dir, .{});
        owns_scope = false;
        return .{ .allocator = a, .lease = scope, .names = owned_names, .ordinal = ordinal, .configuration = configuration, .copy_inventory = inventory };
    }

    fn prepareRemappedFieldCompilation(self: *PersistentIndex, a: Allocator, docs: []const @import("../introducer.zig").TextDocument, inputs: *const @import("index_field_inputs.zig").Inputs, configuration: source_publication.Digest, ordinal: source_publication.Digest) !?FieldCompilation {
        if (inputs.documents.len == 0 or docs.len > 4096) return null;
        const corpus = self.writer.acquireSnapshot();
        defer corpus.release();
        var candidates: usize = 0;
        var identity_bytes: usize = 0;
        var segment_index = corpus.segments.len;
        while (segment_index != 0 and candidates < 8) {
            segment_index -= 1;
            candidates += 1;
            const entry = &corpus.segments[segment_index];
            if (entry.data != .authenticated or entry.reader.doc_count < docs.len or entry.reader.doc_count > 4096) continue;
            var inventory = try entry.data.authenticated.inventory();
            var inventory_owned = true;
            defer if (inventory_owned) inventory.deinit();
            const directory = try inventory.directory();
            if (directory.fields > 256 or !std.mem.eql(u8, &directory.configuration, &configuration)) continue;
            // A remap scans encoded terms. Reject expensive old dictionaries
            // before reading identities or evidence, rather than trading a
            // small compile for an unbounded historical scan.
            var useful = false;
            for (0..directory.fields) |index| {
                const field = try inventory.field(index);
                const budget = inputs.fieldInputBytes(field.name()) / 4;
                if (budget == 0) continue;
                var bytes: u64 = 0;
                for (0..field.sectionCount()) |part| bytes +|= (try field.section(part)).length;
                if (bytes <= @min(budget, 8 * 1024 * 1024)) {
                    useful = true;
                    break;
                }
            }
            if (!useful) continue;
            const rows = try a.alloc(u32, docs.len);
            var rows_owned = true;
            defer if (rows_owned) a.free(rows);
            @memset(rows, std.math.maxInt(u32));
            var wanted: std.StringHashMapUnmanaged(usize) = .empty;
            defer wanted.deinit(a);
            for (docs, 0..) |doc, index| {
                const inserted = try wanted.getOrPut(a, doc.id);
                if (inserted.found_existing) return error.InvalidIndexSourceMap;
                inserted.value_ptr.* = index;
            }
            var found: usize = 0;
            for (0..entry.reader.doc_count) |old| {
                const id = (try entry.reader.storedIdAlloc(a, @intCast(old))) orelse continue;
                defer a.free(id);
                identity_bytes +|= id.len;
                if (identity_bytes > 64 * 1024) return null;
                if (wanted.get(id)) |target| {
                    if (rows[target] != std.math.maxInt(u32)) return error.InvalidIndexSourceMap;
                    rows[target] = @intCast(old);
                    found += 1;
                }
            }
            if (found != docs.len) continue;
            var skipped: std.ArrayListUnmanaged([]u8) = .empty;
            defer {
                for (skipped.items) |name| a.free(name);
                skipped.deinit(a);
            }
            var evidence: std.ArrayListUnmanaged([]u8) = .empty;
            defer {
                for (evidence.items) |raw| a.free(raw);
                evidence.deinit(a);
            }
            for (0..directory.fields) |index| {
                const field = try inventory.field(index);
                if (!inputs.hasLedger(field.name())) continue;
                var bytes: u64 = 0;
                for (0..field.sectionCount()) |part| bytes +|= (try field.section(part)).length;
                if (bytes > @min(inputs.fieldInputBytes(field.name()) / 4, 8 * 1024 * 1024)) continue;
                const raw = (try self.copyFieldInputLedger(a, inventory, field.name())) orelse continue;
                var raw_owned = true;
                defer if (raw_owned) a.free(raw);
                const view = try @import("index_field_ledger.zig").open(raw, field.inputs());
                var equal = true;
                for (rows, 0..) |old, current| {
                    const before = (try view.row(old)) orelse {
                        equal = false;
                        break;
                    };
                    const after = (try inputs.ledgerRow(field.name(), current)) orelse {
                        equal = false;
                        break;
                    };
                    if (!std.mem.eql(u8, &before, &after)) {
                        equal = false;
                        break;
                    }
                }
                if (equal) {
                    const name = try a.dupe(u8, field.name());
                    errdefer a.free(name);
                    try evidence.append(a, raw);
                    raw_owned = false;
                    try skipped.append(a, name);
                }
            }
            if (skipped.items.len == 0) continue;
            const scope = try inventory.lease();
            errdefer {
                var held = scope;
                held.deinit();
            }
            const owned_evidence = try evidence.toOwnedSlice(a);
            errdefer {
                for (owned_evidence) |raw| a.free(raw);
                a.free(owned_evidence);
            }
            const owned_names = try skipped.toOwnedSlice(a);
            inventory_owned = false;
            rows_owned = false;
            return .{ .allocator = a, .lease = scope, .names = owned_names, .ordinal = ordinal, .configuration = configuration, .copy_inventory = inventory, .remap_rows = rows, .remap_evidence = owned_evidence };
        }
        return null;
    }

    const PreparedFieldArtifact = struct {
        allocator: Allocator,
        plan: field_build.Plan,
        layout: source_family.Layout,
        key_range: SegmentKeyRange,
        fields: NewSourcePublication,
        fn deinit(self: *@This()) void {
            self.plan.deinit();
            self.key_range.deinit(self.allocator);
            self.fields.publisher.deinit();
        }
    };
    fn prepareFieldArtifact(self: *PersistentIndex, a: Allocator, reader: *const segment_mod.SegmentReader, configuration: source_publication.Digest, ordinal: source_publication.Digest, compiled: anytype, io: std.Io) !PreparedFieldArtifact {
        return self.prepareFieldArtifactWithCompilation(a, reader, configuration, ordinal, compiled, null, io);
    }
    fn prepareFieldArtifactWithCompilation(self: *PersistentIndex, a: Allocator, reader: *const segment_mod.SegmentReader, configuration: source_publication.Digest, ordinal: source_publication.Digest, compiled: anytype, compilation: ?*const FieldCompilation, io: std.Io) !PreparedFieldArtifact {
        const store = self.segment_files orelse return error.UnsupportedSourcePublication;
        try self.admitSourceFields();
        var fields = try self.beginNewSourcePublication(io);
        errdefer fields.publisher.deinit();
        const ids = try self.reserveSegmentIds(a, 2);
        defer a.free(ids);
        {
            self.lockStorage();
            defer self.unlockStorage();
            var txn = try self.beginWriteMainTxn();
            errdefer txn.abort();
            try requireSourceOwnerOpen(&txn);
            try saveNextSegmentId(&txn, self.writer.next_segment_id);
            try txn.put(.meta, &fieldPreparationKey(ids[1]), &try encodeFieldBinding(fields.binding));
            try txn.commit();
            try self.main_store_owner.sync(true);
        }
        self.noteSourceRecoveryWork();
        const path = try store.pathAlloc(ids[1]);
        defer store.allocator.free(path);
        var output = try store.storage.beginPreparedImmutableWrite(a, path);
        var output_live = true;
        defer if (output_live) output.abort();
        var buffered = AtomicSegmentSink.init(a, &output);
        defer buffered.deinit();
        var sink = buffered.sink();
        var encoder = try @import("../field_pack.zig").Encoder.init(a, &sink, .{});
        defer encoder.deinit();
        var plan = if (compilation) |proof| projected: {
            if (!std.mem.eql(u8, &proof.ordinal, &ordinal) or !std.mem.eql(u8, &proof.configuration, &configuration)) return error.IndexPublicationFenceMismatch;
            var read = try self.beginReadMainTxn();
            defer read.abort();
            var metadata: PublicationTxn = .{ .txn = &read };
            break :projected try field_build.prepareProjected(a, reader, &encoder, ids[1], ordinal, configuration, compiled, &metadata, &proof.lease, proof.names, if (proof.copy_inventory) |*inventory| inventory else null, proof.remap_rows, proof.remap_evidence);
        } else try self.prepareNativeFieldPlan(a, reader, &encoder, ids[1], ordinal, configuration, compiled);
        errdefer plan.deinit();
        try buffered.flush();
        output_live = false;
        try output.finish();
        var key_range = try extractReaderKeyRange(a, reader);
        key_range.seg_id = ids[0];
        return .{ .allocator = a, .plan = plan, .key_range = key_range, .fields = fields, .layout = .{ .segment = ids[0], .documents = reader.doc_count, .physical = plan.resource.identity, .configuration = configuration, .field_inventory = .{ .ordinal = ordinal, .count = @intCast(plan.records.len) } } };
    }

    fn prepareDirectPackedMerge(self: *PersistentIndex, work: Allocator, staging: Allocator, inputs: []const segment_mod.MergeInput, index_sort: []const segment_mod.SegmentIndexSortField, map: *segment_mod.MergeSourceMap, io: std.Io) ![]PreparedMergeSegment {
        const store = self.segment_files orelse return error.DirectPackUnavailable;
        var fields = try self.beginNewSourcePublication(io);
        var fields_owned = true;
        defer if (fields_owned) fields.publisher.deinit();
        const ids = try self.reserveSegmentIds(work, 2);
        defer work.free(ids);
        {
            self.lockStorage();
            defer self.unlockStorage();
            var txn = try self.beginWriteMainTxn();
            errdefer txn.abort();
            try requireSourceOwnerOpen(&txn);
            try saveNextSegmentId(&txn, self.writer.next_segment_id);
            try txn.put(.meta, &fieldPreparationKey(ids[1]), &try encodeFieldBinding(fields.binding));
            try txn.commit();
            try self.main_store_owner.sync(true);
        }
        self.noteSourceRecoveryWork();
        const path = try store.pathAlloc(ids[1]);
        defer store.allocator.free(path);
        var writer = store.storage.beginPreparedImmutableWrite(work, path) catch |err| switch (err) {
            error.Unsupported => return error.DirectPackUnavailable,
            else => return err,
        };
        var writing = true;
        defer if (writing) writer.abort();
        if (writer.vtable.read_at == null) return error.DirectPackUnavailable;
        writer.setCacheIntent(.cold_sequential);
        var buffered = AtomicSegmentSink.init(work, &writer);
        defer buffered.deinit();
        var physical_sink = buffered.sink();
        const pack = @import("../field_pack.zig");
        var encoder = try pack.Encoder.init(work, &physical_sink, .{});
        defer encoder.deinit();
        var direct: @import("index_field_merge_sink.zig").Sink(AtomicSegmentSink) = .{ .allocator = work, .encoder = &encoder, .buffered = &buffered };
        defer direct.deinit();
        var sink = direct.sink();
        try segment_mod.writeMergedSegmentToSinkWithOptions(work, &sink, inputs, .{ .index_sort = index_sort, .source_map = map, .scratch = if (self.privateScratchDirectory(store.root_dir)) |directory| .{ .io = io, .directory = directory, .resource_manager = self.writer.resource_manager } else null });
        var live: u32 = 0;
        for (inputs) |input| live += input.reader.doc_count - if (input.deleted) |deleted| @as(u32, @intCast(deleted.cardinality())) else @as(u32, 0);
        try map.finishOutput(live);
        const metadata = try encoder.finishPatched(&direct);
        try buffered.flush();
        const physical_bytes = physical_sink.len();
        writing = false;
        try writer.finish();
        const resource: field_generation.Resource = .{ .file = ids[1], .payload_bytes = metadata.payload_bytes, .physical_bytes = physical_bytes, .identity = metadata.identity };
        var raw = try store.storage.openLeasedImmutableSource(staging, path);
        var raw_owned = true;
        defer if (raw_owned) raw.close();
        var body = try pack.open(staging, raw, resource.identity, .{});
        raw_owned = false;
        var body_owned = true;
        defer if (body_owned) body.close();
        const preview = try direct.preview(staging, body);
        body_owned = false;
        var data: ?index_mod.SegmentData = .fromNative(preview);
        errdefer if (data) |*owned| owned.deinit(staging);
        var range = try extractSegmentDataKeyRange(staging, data.?);
        errdefer range.deinit(staging);
        range.seg_id = ids[1];
        const pack_output = try staging.create(PackedMergeResource);
        errdefer staging.destroy(pack_output);
        const outputs = try staging.alloc(PreparedMergeSegment, 1);
        pack_output.* = .{ .allocator = staging, .resource = resource, .logical_id = ids[0], .fields = fields };
        fields_owned = false;
        outputs[0] = .{ .id = ids[1], .data = data.?, .key_range = range, .staging_alloc = staging, .native_pack = pack_output };
        data = null;
        return outputs;
    }

    pub fn prepareFieldSourceFamilyFromPackedMerge(self: *PersistentIndex, a: Allocator, primary: anytype, output: *PreparedMergeSegment, reader: *const segment_mod.SegmentReader, rows: []const source_recovery.Input, configuration: source_publication.Digest, compiled: anytype, io: std.Io) !PreparedFieldSourceFamily {
        const pack_output = output.native_pack orelse return error.InvalidIndexFieldGeneration;
        if (pack_output.transferred or pack_output.fields == null or rows.len != reader.doc_count) return error.IndexPublicationConflict;
        const owner = try self.ensureSourceOwner(io);
        var parent = try self.beginNewSourcePublication(io);
        errdefer parent.publisher.deinit();
        const ordinal = try self.fieldOrdinalInventory(a, reader);
        var plan = try field_build.adopt(a, reader, pack_output.resource, ordinal, configuration, compiled);
        errdefer plan.deinit();
        var range = try extractReaderKeyRange(a, reader);
        errdefer range.deinit(a);
        range.seg_id = pack_output.logical_id;
        const layout: source_family.Layout = .{ .segment = pack_output.logical_id, .documents = reader.doc_count, .physical = plan.resource.identity, .configuration = configuration, .field_inventory = .{ .ordinal = ordinal, .count = @intCast(plan.records.len) } };
        const count = (rows.len - 1) / source_family.cohort_capacity + 1;
        const cohorts = try a.alloc([]const source_recovery.Input, count);
        defer a.free(cohorts);
        const identities = try a.alloc(source_publication.Digest, count);
        defer a.free(identities);
        for (cohorts, identities, 0..) |*cohort, *identity, n| {
            const first = n * source_family.cohort_capacity;
            cohort.* = rows[first..@min(rows.len, first + source_family.cohort_capacity)];
            identity.* = try layout.identity(@intCast(first));
        }
        const leaves = try source_recovery.prepareFieldSourceLeaves(a, primary, self, owner, cohorts, identities, .{}, io);
        const fields = pack_output.fields.?;
        pack_output.fields = null;
        pack_output.transferred = true;
        return .{ .allocator = a, .plan = plan, .layout = layout, .key_range = range, .leaves = leaves, .parent = parent, .fields = fields };
    }

    pub fn sourceFieldsEnabled(self: *const PersistentIndex) bool {
        const state = self.source_maintenance.load(.acquire) orelse return false;
        return state.fields_durable.load(.acquire);
    }
    pub fn prepareFieldSourceFamilyAsSource(self: *PersistentIndex, a: Allocator, primary: anytype, inputs: []const source_recovery.Input, bytes: []const u8, configuration: source_publication.Digest, compiled: anytype, io: std.Io) !PreparedSourceFamily {
        var prepared = try self.prepareFieldSourceFamily(a, primary, inputs, bytes, configuration, compiled, io);
        errdefer prepared.deinit();
        const cleanup = self.retired_segment_file_deleter orelse return error.UnsupportedSourcePublication;
        const field = try a.create(PreparedFieldSourceFamily);
        field.* = prepared;
        RetiredSegmentFileDeleter.retain(cleanup);
        return .{ .alloc = a, .data_alloc = self.alloc, .physical = null, .field_family = field, .layout = field.layout, .leaves = &.{}, .parent = .{ .binding = field.parent.binding, .publisher = .{} }, .cleanup = cleanup };
    }

    pub fn prepareProjectedFieldSourceFamilyAsSource(self: *PersistentIndex, a: Allocator, primary: anytype, inputs: []const source_recovery.Input, bytes: []const u8, configuration: source_publication.Digest, compiled: anytype, proof: *const FieldCompilation, io: std.Io) !PreparedSourceFamily {
        var reader = try segment_mod.SegmentReader.init(a, bytes);
        defer reader.deinit();
        var prepared = try self.prepareFieldSourceFamilyFromReaderImpl(a, primary, inputs, &reader, configuration, compiled, proof, io);
        errdefer prepared.deinit();
        const cleanup = self.retired_segment_file_deleter orelse return error.UnsupportedSourcePublication;
        const field = try a.create(PreparedFieldSourceFamily);
        field.* = prepared;
        RetiredSegmentFileDeleter.retain(cleanup);
        return .{ .alloc = a, .data_alloc = self.alloc, .physical = null, .field_family = field, .layout = field.layout, .leaves = &.{}, .parent = .{ .binding = field.parent.binding, .publisher = .{} }, .cleanup = cleanup };
    }

    pub const PreparedFieldSourceFamily = struct {
        allocator: Allocator,
        plan: field_build.Plan,
        layout: source_family.Layout,
        key_range: SegmentKeyRange,
        leaves: []source_recovery.Prepared,
        parent: NewSourcePublication,
        fields: NewSourcePublication,
        deletions: ?roaring.RoaringBitmap = null,
        published: bool = false,
        pub fn deinit(self: *PreparedFieldSourceFamily) void {
            if (self.deletions) |*deleted| deleted.deinit();
            self.plan.deinit();
            self.key_range.deinit(self.allocator);
            for (self.leaves) |*leaf| leaf.deinit(self.allocator);
            self.allocator.free(self.leaves);
            self.parent.publisher.deinit();
            self.fields.publisher.deinit();
            // The durable preparation marker owns abandoned packs. It is
            // reconciled after publisher drainage, including ambiguous commits.
            self.* = undefined;
        }
        pub fn publish(self: *PreparedFieldSourceFamily, index: *PersistentIndex, primary: anytype) !source_publication.Decision {
            return index.appendPreparedFieldSourceFamily(primary, self);
        }
    };

    /// Prepare the complete field/source closure outside the managed apply
    /// guard. Durable reservation precedes creation of the final immutable file;
    /// a crash never leaves an unrecorded staging pathname or reusable file ID.
    pub fn prepareFieldSourceFamily(self: *PersistentIndex, a: Allocator, primary: anytype, inputs: []const source_recovery.Input, bytes: []const u8, configuration: source_publication.Digest, compiled_inputs: anytype, io: std.Io) !PreparedFieldSourceFamily {
        var reader = try segment_mod.SegmentReader.init(a, bytes);
        defer reader.deinit();
        return self.prepareFieldSourceFamilyFromReader(a, primary, inputs, &reader, configuration, compiled_inputs, io);
    }

    /// Consume a retained decoder directly. Compaction can preserve packed
    /// fields without materializing source JSON or retokenizing old documents.
    pub fn prepareFieldSourceFamilyFromReader(self: *PersistentIndex, a: Allocator, primary: anytype, inputs: []const source_recovery.Input, reader: *const segment_mod.SegmentReader, configuration: source_publication.Digest, compiled_inputs: anytype, io: std.Io) !PreparedFieldSourceFamily {
        return self.prepareFieldSourceFamilyFromReaderImpl(a, primary, inputs, reader, configuration, compiled_inputs, null, io);
    }
    fn prepareFieldSourceFamilyFromReaderImpl(self: *PersistentIndex, a: Allocator, primary: anytype, inputs: []const source_recovery.Input, reader: *const segment_mod.SegmentReader, configuration: source_publication.Digest, compiled_inputs: anytype, compilation: ?*const FieldCompilation, io: std.Io) !PreparedFieldSourceFamily {
        if (inputs.len == 0 or inputs.len > std.math.maxInt(u32) or reader.doc_count != inputs.len) return error.InvalidIndexSourceFamily;
        const store = self.segment_files orelse return error.UnsupportedSourcePublication;
        if (store.storage.vtable.begin_prepared_immutable_write == null or store.storage.vtable.open_leased_immutable_source == null) return error.UnsupportedSourcePublication;
        const owner = try self.ensureSourceOwner(io);
        try self.admitSourceFields();
        var parent = try self.beginNewSourcePublication(io);
        errdefer parent.publisher.deinit();
        const ordinal = try self.fieldOrdinalInventory(a, reader);
        var artifact = try self.prepareFieldArtifactWithCompilation(a, reader, configuration, ordinal, compiled_inputs, compilation, io);
        errdefer artifact.deinit();
        const count = (inputs.len - 1) / source_family.cohort_capacity + 1;
        const cohorts = try a.alloc([]const source_recovery.Input, count);
        defer a.free(cohorts);
        const identities = try a.alloc(source_publication.Digest, count);
        defer a.free(identities);
        for (cohorts, identities, 0..) |*rows, *identity, n| {
            const first = n * source_family.cohort_capacity;
            rows.* = inputs[first..@min(inputs.len, first + source_family.cohort_capacity)];
            identity.* = try artifact.layout.identity(@intCast(first));
        }
        const leaves = try source_recovery.prepareFieldSourceLeaves(a, primary, self, owner, cohorts, identities, .{}, io);
        return .{ .allocator = a, .plan = artifact.plan, .layout = artifact.layout, .key_range = artifact.key_range, .leaves = leaves, .parent = parent, .fields = artifact.fields };
    }

    /// One atomic catalog/snapshot replacement for all packed merge outputs.
    /// Source binding tokens are CAS expectations from the actual retained task
    /// snapshot; they grant no reads and detect source-only refreshes as well
    /// as physical replacements. Caller reconciles concurrent deletion deltas.
    pub fn replacePreparedFieldFamilies(self: *PersistentIndex, a: Allocator, primary: anytype, expected: *const index_mod.IndexSnapshot, indices: []const usize, outputs: []PreparedFieldSourceFamily, deleted: []const ?roaring.RoaringBitmap) !bool {
        if (self.read_only) return error.ReadOnly;
        if (outputs.len == 0 or indices.len == 0 or deleted.len != outputs.len) return error.InvalidIndexSourceFamily;
        {
            var read = try primary.beginReadTxn();
            defer read.abort();
            for (outputs) |*output| {
                if (output.published) return error.IndexPublicationConflict;
                try output.parent.publisher.require(output.parent.binding);
                try output.fields.publisher.require(output.fields.binding);
                for (output.leaves) |*leaf| {
                    try leaf.publisher.require(leaf.job.binding);
                    try ((try source_recovery.load(&read, leaf.job.binding)) orelse return error.IndexSourcePublicationPending).requirePreparation(leaf.job);
                }
            }
        }
        try primary.sync(true);
        const old_ids = try a.alloc(u64, indices.len);
        defer a.free(old_ids);
        for (indices, old_ids) |ordinal, *id| {
            if (ordinal >= expected.segments.len) return error.InvalidSegment;
            id.* = expected.segments[ordinal].id;
        }
        const replacements = try a.alloc(index_mod.ReplacementSegmentData, outputs.len);
        defer a.free(replacements);
        const layouts = try a.alloc(source_family.Layout, outputs.len);
        defer a.free(layouts);
        var initialized: usize = 0;
        var published = false;
        defer if (!published) for (replacements[0..initialized]) |*replacement| replacement.data.deinit(self.alloc);
        self.lockStorage();
        defer self.unlockStorage();
        var txn = try self.beginWriteMainTxn();
        errdefer txn.abort();
        try requireSourceOwnerOpen(&txn);
        const owner = (try sourceOwnerRecordInTxn(&txn)) orelse return error.IndexSourceOwnerMismatch;
        if (!owner.fields) return error.IndexPublicationFenceMismatch;
        for (indices, old_ids) |ordinal, id| {
            _ = txn.get(.meta, &activeSegmentMetaKey(id)) catch |err| switch (err) {
                error.NotFound => {
                    txn.abort();
                    return false;
                },
                else => return err,
            };
            var lease = (try expected.segments[ordinal].sourceLease()) orelse return error.InvalidIndexSourceFamily;
            defer lease.deinit();
            const binding = try lease.binding();
            if (!std.meta.eql(try sourceSegmentBinding(&txn, id), @as(?source_publication.Binding, binding))) {
                txn.abort();
                return false;
            }
        }
        var metadata: PublicationTxn = .{ .txn = &txn };
        try family_graph.initialize(&metadata, owner.identity);
        const store = self.segment_files orelse return error.UnsupportedSourcePublication;
        for (outputs, replacements, layouts, deleted) |*output, *replacement, *layout, *mask| {
            if (!std.mem.eql(u8, &owner.identity, &output.parent.binding.owner) or !std.mem.eql(u8, &owner.identity, &output.fields.binding.owner)) return error.IndexSourceOwnerMismatch;
            const reservation = try decodeFieldBinding(try txn.get(.meta, &fieldPreparationKey(output.plan.resource.file)));
            if (!std.meta.eql(reservation, output.fields.binding)) return error.IndexPublicationFenceMismatch;
            if (try sourceSegmentBinding(&txn, output.layout.segment) != null) return error.IndexPublicationConflict;
            const field_root = try output.plan.stage(&metadata, output.fields.binding, .{});
            layout.* = output.layout;
            layout.physical = field_root;
            var readers = try self.openSourceReaders(output.fields.binding);
            defer readers.deinit();
            var scope = try readers.lease();
            defer scope.deinit();
            var inventory = try field_source.open(self.alloc, &metadata, &scope, store.storage, store.root_dir, .{});
            defer inventory.deinit();
            var authenticated = try field_source.authenticated(inventory, self.alloc);
            const data = index_mod.SegmentData.fromAuthenticated(authenticated) catch |err| {
                authenticated.deinit();
                return err;
            };
            replacement.* = .{ .id = layout.segment, .data = data, .deleted = if (mask.*) |*bitmap| bitmap else null };
            initialized += 1;
        }
        var staged = try self.writer.prepareSegmentsManyDataWithScratch(a, old_ids, replacements);
        defer staged.abort();
        for (outputs, layouts, deleted, 0..) |*output, layout, *mask, index| {
            const entry = &staged.snapshot.segments[staged.carried_count + index];
            try self.attachSourceReferences(entry, output.parent.binding, null);
            var builder = try source_family.Builder.initWithFields(owner.identity, layout, layout.physical);
            for (output.leaves, 0..) |*leaf, n| {
                try self.validateSourceCohortReferences(entry, leaf, @intCast(n * 64));
                if (leaf.job.role != .source_leaf or leaf.job.documents != try layout.count(@intCast(n * 64)) or !std.mem.eql(u8, &leaf.job.segment, &try layout.identity(@intCast(n * 64)))) return error.InvalidIndexSourceFamily;
                const plan = try exact_source_map.Plan.prepare(a, leaf.job.binding, leaf.job.segment, leaf.references);
                defer plan.deinit(a);
                if (!std.mem.eql(u8, &plan.root(), &leaf.job.root)) return error.IndexPublicationConflict;
                try plan.stage(&metadata);
                _ = try source_publication.stagePublishRoleInEpoch(a, &metadata, leaf.job.binding, plan.root(), &.{}, try owner.admitPublication(leaf.job.binding), .source_leaf);
                try builder.append(&metadata, (try source_publication.decision(&metadata, leaf.job.binding)).?, .{});
            }
            const root = try builder.finish(&metadata, .{});
            _ = try source_publication.stagePublishRoleInEpoch(a, &metadata, output.parent.binding, root, &.{layout.segment}, try owner.admitPublication(output.parent.binding), .family);
            if (!try family_graph.compareAndSwap(&metadata, owner.identity, source_family.slot(layout.segment), null, output.parent.binding.publication, root, .{})) return error.IndexPublicationConflict;
            if (mask.*) |*bitmap| if (!bitmap.isEmpty()) {
                if (bitmap.maximum().? >= layout.documents) return error.InvalidIndexSourceFamily;
                const bytes = try bitmap.toBytes(a);
                defer a.free(bytes);
                try txn.put(.deletions, &std.mem.toBytes(std.mem.nativeToBig(u64, layout.segment)), bytes);
            };
            try txn.put(.meta, &fieldInventoryKey(output.plan.ordinal_inventory), &try encodeFieldBinding(output.fields.binding));
            try txn.put(.meta, &fieldSegmentKey(layout.segment), &try encodeFieldBinding(output.fields.binding));
            try txn.delete(.meta, &fieldPreparationKey(output.plan.resource.file));
            try self.saveSegmentRangeWithAllocator(a, &txn, layout.segment, output.key_range);
            try self.updateActiveSegments(&txn, layout.segment, .add);
            try stageSourceSegment(&txn, layout.segment, output.parent.binding);
        }
        for (old_ids) |id| {
            try self.updateActiveSegments(&txn, id, .remove);
            const key = std.mem.toBytes(std.mem.nativeToBig(u64, id));
            txn.delete(.segments, &key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            txn.delete(.deletions, &key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            txn.delete(.meta, &fieldSegmentKey(id)) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            try self.deleteSegmentRange(&txn, id);
        }
        try self.retireRemovedSourceRoots(&txn, old_ids);
        try saveNextSegmentId(&txn, self.writer.next_segment_id);
        try txn.commit();
        staged.publish();
        published = true;
        for (outputs, layouts) |*output, layout| {
            output.layout = layout;
            output.published = true;
        }
        self.source_roots_present = true;
        self.noteSourceRecoveryWork();
        try self.main_store_owner.sync(true);
        return true;
    }

    fn appendPreparedFieldSourceFamily(self: *PersistentIndex, primary: anytype, prepared: *PreparedFieldSourceFamily) !source_publication.Decision {
        if (self.read_only) return error.ReadOnly;
        try prepared.parent.publisher.require(prepared.parent.binding);
        try prepared.fields.publisher.require(prepared.fields.binding);
        {
            var read = try primary.beginReadTxn();
            defer read.abort();
            for (prepared.leaves) |*leaf| {
                try leaf.publisher.require(leaf.job.binding);
                try ((try source_recovery.load(&read, leaf.job.binding)) orelse return error.IndexSourcePublicationPending).requirePreparation(leaf.job);
            }
        }
        try primary.sync(true);
        self.lockStorage();
        defer self.unlockStorage();
        var txn = try self.beginWriteMainTxn();
        errdefer txn.abort();
        try requireSourceOwnerOpen(&txn);
        const owner = (try sourceOwnerRecordInTxn(&txn)) orelse return error.IndexSourceOwnerMismatch;
        if (!owner.fields or !std.mem.eql(u8, &owner.identity, &prepared.parent.binding.owner) or !std.mem.eql(u8, &owner.identity, &prepared.fields.binding.owner)) return error.IndexSourceOwnerMismatch;
        var metadata: PublicationTxn = .{ .txn = &txn };
        if (try source_publication.decision(&metadata, prepared.parent.binding)) |existing| {
            try existing.requireRole(.family);
            try existing.require(prepared.parent.binding, .published);
            const record = (try source_publication.load(&metadata, prepared.parent.binding)).?;
            if (record.count != 1 or record.segments[0] != prepared.layout.segment) return error.IndexPublicationConflict;
            const expected_fields = try prepared.plan.stage(&metadata, prepared.fields.binding, .{});
            var expected_layout = prepared.layout;
            expected_layout.physical = expected_fields;
            var readers = try self.openSourceReaders(prepared.parent.binding);
            defer readers.deinit();
            var scope = try readers.lease();
            defer scope.deinit();
            const held = try family_graph.openHeld(&metadata, &scope);
            for (prepared.leaves, 0..) |leaf, n| {
                const selected = try source_family.select(&metadata, &held, @intCast(n * 64));
                if (!std.meta.eql(selected.layout, expected_layout) or !std.mem.eql(u8, &selected.leaf.publication_id, &leaf.job.binding.publication) or !std.mem.eql(u8, &selected.leaf.root, &leaf.job.root)) return error.IndexPublicationConflict;
            }
            const corpus = self.writer.acquireSnapshot();
            defer corpus.release();
            const present = for (corpus.segments) |*entry| {
                if (entry.id == prepared.layout.segment) break entry;
            } else return error.IndexPublicationRequiresRecovery;
            var deleted = try present.cloneDeleted(self.alloc);
            defer if (deleted) |*bitmap| bitmap.deinit();
            if (prepared.deletions) |*expected| {
                if (deleted) |*actual| {
                    if (!expected.eql(actual)) return error.IndexPublicationConflict;
                } else if (!expected.isEmpty()) return error.IndexPublicationConflict;
            } else if (deleted) |*actual| if (!actual.isEmpty()) return error.IndexPublicationConflict;
            txn.abort();
            prepared.published = true;
            return (try self.durableSourceDecisionLocked(prepared.parent.binding)).?;
        }
        const reservation = try decodeFieldBinding(try txn.get(.meta, &fieldPreparationKey(prepared.plan.resource.file)));
        if (!std.meta.eql(reservation, prepared.fields.binding)) return error.IndexPublicationFenceMismatch;
        if (try sourceSegmentBinding(&txn, prepared.layout.segment) != null) return error.IndexPublicationConflict;
        try family_graph.initialize(&metadata, owner.identity);
        const field_root = try prepared.plan.stage(&metadata, prepared.fields.binding, .{});
        var layout = prepared.layout;
        layout.physical = field_root;
        var field_readers = try self.openSourceReaders(prepared.fields.binding);
        defer field_readers.deinit();
        var field_lease = try field_readers.lease();
        defer field_lease.deinit();
        const store = self.segment_files orelse return error.UnsupportedSourcePublication;
        var inventory = try field_source.open(self.alloc, &metadata, &field_lease, store.storage, store.root_dir, .{});
        defer inventory.deinit();
        var authenticated = try field_source.authenticated(inventory, self.alloc);
        var data: ?index_mod.SegmentData = index_mod.SegmentData.fromAuthenticated(authenticated) catch |err| {
            authenticated.deinit();
            return err;
        };
        defer if (data) |*owned| owned.deinit(self.alloc);
        var replacements = [_]index_mod.ReplacementSegmentData{.{ .id = layout.segment, .data = data.?, .deleted = if (prepared.deletions) |*deleted| deleted else null }};
        var staged = try self.writer.prepareSegmentsManyData(&.{}, &replacements);
        defer staged.abort();
        try self.attachSourceReferences(&staged.snapshot.segments[staged.carried_count], prepared.parent.binding, null);
        var builder = try source_family.Builder.initWithFields(owner.identity, layout, field_root);
        for (prepared.leaves, 0..) |*leaf, n| {
            try self.validateSourceCohortReferences(&staged.snapshot.segments[staged.carried_count], leaf, @intCast(n * 64));
            if (leaf.job.role != .source_leaf or leaf.job.documents != try layout.count(@intCast(n * 64)) or !std.mem.eql(u8, &leaf.job.segment, &try layout.identity(@intCast(n * 64)))) return error.InvalidIndexSourceFamily;
            const source_plan = try exact_source_map.Plan.prepare(self.alloc, leaf.job.binding, leaf.job.segment, leaf.references);
            defer source_plan.deinit(self.alloc);
            if (!std.mem.eql(u8, &source_plan.root(), &leaf.job.root)) return error.IndexPublicationConflict;
            try source_plan.stage(&metadata);
            _ = try source_publication.stagePublishRoleInEpoch(self.alloc, &metadata, leaf.job.binding, source_plan.root(), &.{}, try owner.admitPublication(leaf.job.binding), .source_leaf);
            try builder.append(&metadata, (try source_publication.decision(&metadata, leaf.job.binding)).?, .{});
        }
        const root = try builder.finish(&metadata, .{});
        _ = try source_publication.stagePublishRoleInEpoch(self.alloc, &metadata, prepared.parent.binding, root, &.{layout.segment}, try owner.admitPublication(prepared.parent.binding), .family);
        if (!try family_graph.compareAndSwap(&metadata, owner.identity, source_family.slot(layout.segment), null, prepared.parent.binding.publication, root, .{})) return error.IndexPublicationConflict;
        if (prepared.deletions) |*deleted| if (!deleted.isEmpty()) {
            if (deleted.maximum().? >= layout.documents) return error.InvalidIndexSourceFamily;
            const raw = try deleted.toBytes(self.alloc);
            defer self.alloc.free(raw);
            try txn.put(.deletions, &std.mem.toBytes(std.mem.nativeToBig(u64, layout.segment)), raw);
        };
        try txn.put(.meta, &fieldInventoryKey(prepared.plan.ordinal_inventory), &try encodeFieldBinding(prepared.fields.binding));
        try txn.put(.meta, &fieldSegmentKey(layout.segment), &try encodeFieldBinding(prepared.fields.binding));
        try txn.delete(.meta, &fieldPreparationKey(prepared.plan.resource.file));
        try self.saveSegmentRange(&txn, layout.segment, prepared.key_range);
        try self.updateActiveSegments(&txn, layout.segment, .add);
        try stageSourceSegment(&txn, layout.segment, prepared.parent.binding);
        try saveNextSegmentId(&txn, self.writer.next_segment_id);
        try txn.commit();
        staged.publish();
        data = null;
        prepared.layout = layout;
        prepared.published = true;
        self.source_roots_present = true;
        self.noteSourceRecoveryWork();
        return (try self.durableSourceDecisionLocked(prepared.parent.binding)).?;
    }

    /// Rebase an immutable donor segment into this index/primary ownership
    /// domain. The donor snapshot and source cut stay held through bounded
    /// claim import; no current receiver row supplies an indexed afterimage.
    /// The returned preparation owns the new file and every receiver claim.
    pub fn prepareImportedSourceFamily(self: *PersistentIndex, a: Allocator, primary: *@import("docstore.zig").DocStore, donor: *PersistentIndex, donor_primary: *@import("docstore.zig").DocStore, segment_id: u64, configuration: source_publication.Digest, io: std.Io) !PreparedSourceFamily {
        return self.prepareImportedSourceFamilyWithMap(a, primary, donor, donor_primary, segment_id, configuration, null, null, null, null, io);
    }

    /// A native pruning builder supplies the paired output and ordinal map.
    /// Only single-input, monotone, one-output maps are admitted here; arbitrary
    /// reorder/merge operations use their own authenticated merge preparation.
    pub fn prepareImportedPrunedSourceFamily(self: *PersistentIndex, a: Allocator, primary: *@import("docstore.zig").DocStore, donor: *PersistentIndex, donor_primary: *@import("docstore.zig").DocStore, segment_id: u64, configuration: source_publication.Digest, bytes: []const u8, map: *const segment_mod.MergeSourceMap, io: std.Io) !PreparedSourceFamily {
        return self.prepareImportedSourceFamilyWithMap(a, primary, donor, donor_primary, segment_id, configuration, .{ .contiguous = bytes }, map, null, null, io);
    }

    /// Consumes the pruning builder's artifact on every path. Its native map
    /// remains borrowed only until receiver claims are durably prepared.
    pub fn prepareImportedPreparedSourceFamily(self: *PersistentIndex, a: Allocator, primary: *@import("docstore.zig").DocStore, donor: *PersistentIndex, donor_primary: *@import("docstore.zig").DocStore, segment_id: u64, configuration: source_publication.Digest, artifact: PreparedMergeSegment, map: *const segment_mod.MergeSourceMap, io: std.Io) !PreparedSourceFamily {
        return self.prepareImportedSourceFamilyWithMap(a, primary, donor, donor_primary, segment_id, configuration, null, map, artifact, null, io);
    }

    pub fn prepareImportedSourceFamilyAt(self: *PersistentIndex, a: Allocator, primary: *@import("docstore.zig").DocStore, donor: *PersistentIndex, donor_primary: *@import("docstore.zig").DocStore, corpus: *const index_mod.IndexSnapshot, ordinal: usize, configuration: source_publication.Digest, io: std.Io) !PreparedSourceFamily {
        if (ordinal >= corpus.segments.len) return error.InvalidSegment;
        return self.prepareImportedSourceFamilyWithMap(a, primary, donor, donor_primary, corpus.segments[ordinal].id, configuration, null, null, null, corpus, io);
    }

    pub fn prepareImportedPreparedSourceFamilyAt(self: *PersistentIndex, a: Allocator, primary: *@import("docstore.zig").DocStore, donor: *PersistentIndex, donor_primary: *@import("docstore.zig").DocStore, corpus: *const index_mod.IndexSnapshot, ordinal: usize, configuration: source_publication.Digest, artifact: PreparedMergeSegment, map: *const segment_mod.MergeSourceMap, io: std.Io) !PreparedSourceFamily {
        // The internal consumer closes offered ownership even for an invalid
        // ordinal. ID zero cannot identify an actual donor segment.
        const id = if (ordinal < corpus.segments.len) corpus.segments[ordinal].id else 0;
        return self.prepareImportedSourceFamilyWithMap(a, primary, donor, donor_primary, id, configuration, null, map, artifact, corpus, io);
    }

    pub fn sourceFamilyConfiguration(self: *PersistentIndex, segment_id: u64) !source_publication.Digest {
        const corpus = self.writer.acquireSnapshot();
        defer corpus.release();
        for (corpus.segments, 0..) |entry, ordinal| {
            if (entry.id == segment_id) return self.sourceFamilyConfigurationAt(corpus, ordinal);
        }
        return error.InvalidSegment;
    }

    pub fn sourceFamilyConfigurationAt(self: *PersistentIndex, corpus: *const index_mod.IndexSnapshot, ordinal: usize) !source_publication.Digest {
        return (try self.sourceFamilyLayoutAt(corpus, ordinal)).configuration;
    }

    pub fn sourceFamilyLayoutAt(self: *PersistentIndex, corpus: *const index_mod.IndexSnapshot, ordinal: usize) !source_family.Layout {
        try corpus.checkScoringReadContext();
        if (ordinal >= corpus.segments.len) return error.InvalidSegment;
        const entry = &corpus.segments[ordinal];
        var lease = (try entry.sourceLease()) orelse return error.InvalidIndexSourceFamily;
        defer lease.deinit();
        var read = try self.beginReadMainTxn();
        defer read.abort();
        var metadata: PublicationTxn = .{ .txn = &read };
        const decision = try source_publication.heldReaderDecision(&metadata, try lease.binding(), &lease.inner);
        if (decision.role == .segment) return error.IndexSourceConfigurationUnavailable;
        const held = try family_graph.openHeld(&metadata, &lease);
        return (try source_family.select(&metadata, &held, 0)).layout;
    }

    /// Generation-local retry identity. Only the actual donor lease/cut and
    /// deletion mask enter this fingerprint; it cannot authorize any read.
    pub fn sourceTransferIdentityAt(self: *PersistentIndex, corpus: *const index_mod.IndexSnapshot, ordinal: usize, split_key: []const u8, configuration: source_publication.Digest) !source_publication.Digest {
        try corpus.checkScoringReadContext();
        if (ordinal >= corpus.segments.len) return error.InvalidSegment;
        const entry = &corpus.segments[ordinal];
        var lease = (try entry.sourceLease()) orelse return error.InvalidIndexSourceFamily;
        defer lease.deinit();
        var read = try self.beginReadMainTxn();
        defer read.abort();
        var metadata: PublicationTxn = .{ .txn = &read };
        const decision = try source_publication.heldReaderDecision(&metadata, try lease.binding(), &lease.inner);
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly/source-transfer-selection/v1\x00");
        hash.update(&decision.binding.owner);
        hash.update(&decision.binding.publication);
        hash.update(&decision.root);
        hash.update(&configuration);
        var bytes: [8]u8 = undefined;
        std.mem.writeInt(u64, &bytes, split_key.len, .little);
        hash.update(&bytes);
        hash.update(split_key);
        if (try entry.cloneDeleted(self.alloc)) |value| {
            var deleted = value;
            defer deleted.deinit();
            var iter = deleted.iterator();
            while (iter.next()) |doc| {
                std.mem.writeInt(u64, &bytes, doc, .little);
                hash.update(&bytes);
            }
        }
        return hash.finalResult();
    }

    fn prepareImportedSourceFamilyWithMap(self: *PersistentIndex, a: Allocator, primary: *@import("docstore.zig").DocStore, donor: *PersistentIndex, donor_primary: *@import("docstore.zig").DocStore, segment_id: u64, configuration: source_publication.Digest, rebuilt: ?@import("../segment_source.zig").Source, mapping: ?*const segment_mod.MergeSourceMap, supplied: ?PreparedMergeSegment, captured_corpus: ?*const index_mod.IndexSnapshot, io: std.Io) !PreparedSourceFamily {
        var offered = supplied;
        defer if (offered) |*output| {
            const id = output.id;
            output.deinit(self.alloc);
            self.deleteSegmentFile(id);
        };
        const acquired = if (captured_corpus == null) donor.writer.acquireSnapshot() else null;
        defer if (acquired) |corpus| corpus.release();
        const donor_snapshot = captured_corpus orelse acquired.?;
        try donor_snapshot.checkScoringReadContext();
        const entry = for (donor_snapshot.segments) |*candidate| {
            if (candidate.id == segment_id) break candidate;
        } else return error.InvalidSegment;
        if (!entry.hasSourceReferences()) return error.InvalidIndexSourceFamily;
        var family_lease = (try entry.sourceLease()) orelse return error.InvalidIndexSourceFamily;
        defer family_lease.deinit();
        const donor_layout = blk: {
            var read = try donor.beginReadMainTxn();
            defer read.abort();
            var metadata: PublicationTxn = .{ .txn = &read };
            const decision = try source_publication.heldReaderDecision(&metadata, try family_lease.binding(), &family_lease.inner);
            if (decision.role == .family) {
                const held = try family_graph.openHeld(&metadata, &family_lease);
                break :blk (try source_family.select(&metadata, &held, 0)).layout;
            }
            try decision.requireRole(.segment);
            const summary = try exact_source_map.summary(&metadata, decision.binding, decision.root);
            if (summary.documents != entry.reader.doc_count) return error.InvalidIndexSourceMap;
            // Older flat maps authenticate physical bytes and source membership,
            // but have no configuration field. Their native managed caller has
            // already checked source/receiver compiled-schema compatibility.
            break :blk source_family.Layout{ .segment = entry.id, .documents = summary.documents, .physical = summary.identity, .configuration = configuration };
        };
        if (donor_layout.segment != entry.id or donor_layout.documents != entry.reader.doc_count or
            !std.mem.eql(u8, &donor_layout.configuration, &configuration)) return error.IndexPublicationConflict;
        if (mapping) |map| {
            if (map.rows.len != 1 or map.rows[0].count != entry.reader.doc_count or map.output_ends.items.len != 1) return error.InvalidIndexSourceMap;
        }
        var carried_deletions = if (mapping == null) try entry.cloneDeleted(a) else null;
        errdefer if (carried_deletions) |*bitmap| bitmap.deinit();
        const cleanup = self.retired_segment_file_deleter orelse return error.UnsupportedSourcePublication;
        RetiredSegmentFileDeleter.retain(cleanup);
        errdefer cleanup.release();
        const owner = try self.ensureSourceOwner(io);
        var field_artifact: ?PreparedFieldArtifact = null;
        errdefer if (field_artifact) |*value| value.deinit();
        if (donor_layout.field_inventory) |inventory| {
            if (entry.data != .authenticated) return error.IndexPublicationFenceMismatch;
            var captured = try entry.data.authenticated.inventory();
            defer captured.deinit();
            const Compiled = struct {
                inventory: field_source.Inventory,
                changed_ordinal: ?source_publication.Digest,
                pub fn inputForField(value: *@This(), name: []const u8) !source_publication.Digest {
                    var low: usize = 0;
                    var high: usize = (try value.inventory.directory()).fields;
                    while (low < high) {
                        const middle = low + (high - low) / 2;
                        const field = try value.inventory.field(middle);
                        switch (std.mem.order(u8, field.name(), name)) {
                            .lt => low = middle + 1,
                            .gt => high = middle,
                            .eq => {
                                if (value.changed_ordinal) |ordinal| {
                                    var hash = std.crypto.hash.sha2.Sha256.init(.{});
                                    hash.update("antfly/pruned-field-input/v1\x00");
                                    hash.update(&ordinal);
                                    hash.update(&field.inputs());
                                    return hash.finalResult();
                                }
                                return field.inputs();
                            },
                        }
                    }
                    return error.InvalidIndexFieldGeneration;
                }
            };
            var ordinal = inventory.ordinal;
            if (mapping != null) io.random(&ordinal);
            var compiled: Compiled = .{ .inventory = captured, .changed_ordinal = if (mapping != null) ordinal else null };
            var reader = if (offered) |output| try output.data.initReader(a) else if (rebuilt) |source| try segment_mod.SegmentReader.initSource(a, source) else try entry.data.initReader(a);
            defer reader.deinit();
            if (mapping) |map| {
                if (reader.doc_count != map.output_ends.items[0]) return error.InvalidIndexSourceMap;
            } else if (reader.doc_count != entry.reader.doc_count) return error.InvalidIndexSourceMap;
            field_artifact = try self.prepareFieldArtifact(a, &reader, configuration, ordinal, &compiled, io);
        }
        var artifact: ?PreparedMergeSegment = if (field_artifact != null) null else if (offered) |output| accepted: {
            offered = null;
            break :accepted output;
        } else try self.prepareSourceSegmentCopy(a, rebuilt orelse entry.data.source());
        errdefer if (artifact) |*value| {
            const id = value.id;
            value.deinit(self.alloc);
            RetiredSegmentFileDeleter.delete(cleanup, id);
        };
        if (artifact != null) {
            self.lockStorage();
            defer self.unlockStorage();
            var txn = try self.beginWriteMainTxn();
            errdefer txn.abort();
            try saveNextSegmentId(&txn, self.writer.next_segment_id);
            try txn.commit();
            try self.main_store_owner.sync(true);
        }
        const stamp = if (artifact) |value| try @import("../segment_source.zig").ContentStamp.capture(value.data.source()) else null;
        if (mapping == null and stamp != null and !std.mem.eql(u8, &stamp.?.digest, &donor_layout.physical)) return error.InvalidIndexSourceFamily;
        const documents = if (field_artifact) |value| value.layout.documents else blk: {
            var reader = try artifact.?.data.initReader(a);
            defer reader.deinit();
            if (mapping) |map| {
                if (map.output_ends.items[0] != reader.doc_count) return error.InvalidIndexSourceMap;
            } else if (reader.doc_count != entry.reader.doc_count) return error.InvalidIndexSourceMap;
            break :blk reader.doc_count;
        };
        if (documents == 0) return error.InvalidIndexSourceMap;
        const layout: source_family.Layout = if (field_artifact) |value| value.layout else .{ .segment = artifact.?.id, .documents = documents, .physical = stamp.?.digest, .configuration = configuration };
        const identities = try a.alloc(source_publication.Digest, (layout.documents - 1) / source_family.cohort_capacity + 1);
        defer a.free(identities);
        for (identities, 0..) |*identity, n| identity.* = try layout.identity(@intCast(n * source_family.cohort_capacity));
        const DocTxn = @import("docstore.zig").DocStore.Txn;
        const Producer = struct {
            donor: *PersistentIndex,
            segment: *const index_mod.SegmentEntry,
            read: *DocTxn,
            mapping: ?*const segment_mod.MergeSourceMap,
            source_cursor: u32 = 0,
            output_cursor: u32 = 0,
            const Batch = struct {
                references: []source_claims.Reference,
                frames: []CapturedSourceReferences,
                frame_count: usize = 0,
                frame_for_row: [source_family.cohort_capacity]u8 = undefined,
                read: *DocTxn,
                pub fn deinit(batch: *@This(), alloc: Allocator) void {
                    for (batch.frames[0..batch.frame_count]) |*frame| frame.deinit();
                    alloc.free(batch.frames);
                    alloc.free(batch.references);
                }
                pub fn open(batch: *@This(), alloc: Allocator, row: usize) !source_claims.ReadSession(*DocTxn) {
                    if (row >= batch.references.len) return error.InvalidIndexSourceMap;
                    const frame = &batch.frames[batch.frame_for_row[row]];
                    return source_claims.openRead(alloc, batch.read, batch.references[row], frame.decision, &frame.lease.inner);
                }
            };
            pub fn capture(producer: *@This(), alloc: Allocator, first: u32, count: u32) !Batch {
                if (count == 0 or count > source_family.cohort_capacity or first != producer.output_cursor) return error.InvalidIndexSourceMap;
                const references = try alloc.alloc(source_claims.Reference, count);
                errdefer alloc.free(references);
                const frames = try alloc.alloc(CapturedSourceReferences, count);
                errdefer alloc.free(frames);
                var batch: Batch = .{ .references = references, .frames = frames, .read = producer.read };
                errdefer for (frames[0..batch.frame_count]) |*frame| frame.deinit();
                var window: [source_family.cohort_capacity]source_claims.Reference = undefined;
                var window_first: u32 = 0;
                var window_count: u32 = 0;
                for (references, 0..) |*reference, n| {
                    const ordinal = if (producer.mapping) |map| selected: {
                        while (producer.source_cursor < producer.segment.reader.doc_count) {
                            const source_ordinal = producer.source_cursor;
                            producer.source_cursor += 1;
                            if (try map.lookupRead(0, source_ordinal)) |target| {
                                if (target.segment != 0 or target.doc != producer.output_cursor) return error.InvalidIndexSourceMap;
                                break :selected source_ordinal;
                            }
                        }
                        return error.InvalidIndexSourceMap;
                    } else first + @as(u32, @intCast(n));
                    producer.output_cursor += 1;
                    if (window_count == 0 or ordinal < window_first or ordinal - window_first >= window_count) {
                        frames[batch.frame_count] = (try producer.donor.captureSourceReferencesAt(alloc, producer.segment, ordinal)) orelse return error.InvalidIndexSourceMap;
                        batch.frame_count += 1;
                        const frame = &frames[batch.frame_count - 1];
                        const local_first = (ordinal - frame.first_document) / source_family.cohort_capacity * source_family.cohort_capacity;
                        if (local_first >= frame.view.documents) return error.InvalidIndexSourceMap;
                        window_first = frame.first_document + local_first;
                        window_count = @min(source_family.cohort_capacity, frame.view.documents - local_first);
                        try frame.readWindow(producer.donor, local_first, window[0..window_count]);
                    }
                    reference.* = window[ordinal - window_first];
                    batch.frame_for_row[n] = @intCast(batch.frame_count - 1);
                }
                return batch;
            }
        };
        var donor_cut = try donor_primary.beginReadTxn();
        defer donor_cut.abort();
        var producer: Producer = .{ .donor = donor, .segment = entry, .read = &donor_cut, .mapping = mapping };
        const leaves = if (field_artifact != null) try source_recovery.prepareImportedFieldSourceLeaves(a, primary, self, owner, &producer, layout.documents, identities, .{}, io) else try source_recovery.prepareImportedSourceLeaves(a, primary, self, owner, &producer, layout.documents, identities, .{}, io);
        errdefer {
            for (leaves) |*leaf| leaf.deinit(a);
            a.free(leaves);
        }
        var parent = try self.beginNewSourcePublication(io);
        errdefer parent.publisher.deinit();
        if (field_artifact) |value| {
            const result = try a.create(PreparedFieldSourceFamily);
            result.* = .{ .allocator = a, .plan = value.plan, .layout = value.layout, .key_range = value.key_range, .fields = value.fields, .parent = parent, .leaves = leaves, .deletions = carried_deletions };
            field_artifact = null;
            carried_deletions = null;
            return .{ .alloc = a, .data_alloc = self.alloc, .physical = null, .field_family = result, .layout = layout, .leaves = &.{}, .parent = .{ .binding = parent.binding, .publisher = .{} }, .cleanup = cleanup };
        }
        return .{ .alloc = a, .data_alloc = self.alloc, .physical = artifact, .layout = layout, .stamp = stamp, .leaves = leaves, .deletions = carried_deletions, .parent = parent, .cleanup = cleanup };
    }

    /// Select the birth epoch and pin its preparation under the same lock.
    /// An epoch transition cannot invalidate this already-started build.
    pub fn beginNewSourcePublication(self: *PersistentIndex, io: std.Io) !NewSourcePublication {
        if (self.read_only) return error.ReadOnly;
        self.lockStorage();
        defer self.unlockStorage();
        var txn = try self.beginReadMainTxn();
        defer txn.abort();
        try requireSourceOwnerOpen(&txn);
        const owner = (try sourceOwnerRecordInTxn(&txn)) orelse return error.IndexSourceOwnerMismatch;
        var entropy: [16]u8 = undefined;
        io.random(&entropy);
        const binding = owner.newBinding(entropy);
        var readers = try self.openSourceReaders(binding);
        defer readers.deinit();
        if (self.source_maintenance.load(.acquire)) |state| readers.watch(state.port());
        return .{ .binding = binding, .publisher = try readers.prepare(owner.current) };
    }

    pub const SourcePublicationEpoch = struct { current: u64, floor: u64, waiting_publishers: bool };
    /// Family-aware peers must be admitted before this irreversible local
    /// reader fence. The public writer does not call this until cohort rollout.
    pub fn admitSourceFamilies(self: *PersistentIndex) !void {
        if (@import("schema.zig").maximum_reader_format_version < 28) return error.UnsupportedVersion;
        if (self.read_only) return error.ReadOnly;
        self.lockStorage();
        defer self.unlockStorage();
        var txn = try self.beginWriteMainTxn();
        errdefer txn.abort();
        try requireSourceOwnerOpen(&txn);
        var owner = (try sourceOwnerRecordInTxn(&txn)) orelse return error.IndexSourceOwnerMismatch;
        if (owner.families) {
            txn.abort();
            if (self.source_maintenance.load(.acquire)) |state| if (state.families_durable.load(.acquire)) return;
            // Retry the durability barrier after a prior commit/sync failure.
            try self.main_store_owner.sync(true);
            if (self.source_maintenance.load(.acquire)) |state| state.families_durable.store(true, .release);
            return;
        }
        if (owner.current == 0) owner.current = 1;
        owner.families = true;
        try txn.put(.meta, source_owner_key, &try owner.encode());
        try txn.commit();
        if (self.source_maintenance.load(.acquire)) |state| state.epoch_enabled.store(true, .release);
        self.noteSourceRecoveryWork();
        try self.main_store_owner.sync(true);
        if (self.source_maintenance.load(.acquire)) |state| state.families_durable.store(true, .release);
    }

    /// Independent field-reader scopes require the ISO4 index fence. The
    /// production coordinator admits this only after peer/reader rollout and
    /// an eligible-write plan; default ASCH25 writers never call it.
    pub fn admitSourceFields(self: *PersistentIndex) !void {
        if (@import("schema.zig").maximum_reader_format_version < 29) return error.UnsupportedVersion;
        if (self.read_only) return error.ReadOnly;
        self.lockStorage();
        defer self.unlockStorage();
        var txn = try self.beginWriteMainTxn();
        var txn_open = true;
        errdefer if (txn_open) txn.abort();
        try requireSourceOwnerOpen(&txn);
        var owner = (try sourceOwnerRecordInTxn(&txn)) orelse return error.IndexSourceOwnerMismatch;
        if (owner.fields) {
            txn.abort();
            txn_open = false;
            if (self.source_maintenance.load(.acquire)) |state| if (state.fields_durable.load(.acquire)) return;
        } else {
            if (owner.current == 0) owner.current = 1;
            owner.families = true;
            owner.fields = true;
            try txn.put(.meta, source_owner_key, &try owner.encode());
            try txn.commit();
            txn_open = false;
        }
        try self.main_store_owner.sync(true);
        if (self.source_maintenance.load(.acquire)) |state| {
            state.epoch_enabled.store(true, .release);
            state.families_durable.store(true, .release);
            state.fields_durable.store(true, .release);
        }
        self.noteSourceRecoveryWork();
    }
    /// Enter the next birth epoch without aborting existing preparations.
    /// ISO2 durably fences older index implementations before receipts may GC.
    pub fn advanceSourcePublicationEpoch(self: *PersistentIndex) !SourcePublicationEpoch {
        if (self.read_only) return error.ReadOnly;
        self.lockStorage();
        defer self.unlockStorage();
        var writer = try self.beginWriteMainTxn();
        errdefer writer.abort();
        try requireSourceOwnerOpen(&writer);
        var owner = (try sourceOwnerRecordInTxn(&writer)) orelse return error.IndexSourceOwnerMismatch;
        const state = self.source_maintenance.load(.acquire) orelse return error.IndexSourceOwnerMismatch;
        if (owner.current == owner.floor) owner.current = try std.math.add(u64, owner.current, 1);
        const waiting = state.registry.hasPreparationsBefore(owner.current);
        if (!waiting) owner.floor = owner.current;
        try writer.put(.meta, source_owner_key, &try owner.encode());
        try writer.commit();
        // Even a failed post-commit sync leaves a visible ISO2 header. Keep
        // maintenance armed; it must retry the floor barrier before GC.
        state.epoch_enabled.store(true, .release);
        self.noteSourceRecoveryWork();
        try self.main_store_owner.sync(true);
        state.durable_epoch_floor = owner.floor;
        return .{ .current = owner.current, .floor = owner.floor, .waiting_publishers = waiting };
    }

    pub const SourceReceiptGc = struct {
        after: ?source_publication.Key = null,
        more: bool = false,
        purged: u32 = 0,
        waiting_publishers: bool = false,
        current_epoch: u64 = 0,
        current_retired: u32 = 0,
    };
    /// Reclaim at most `limit` receipt records. All metadata/claim decisions
    /// use fixed headers; force primary cleanup durability outside the index
    /// lock before forgetting any terminal receipt. The epoch floor is the
    /// permanent late-publication fence after an individual receipt is gone.
    pub fn reclaimSourceReceiptWindow(self: *PersistentIndex, primary: anytype, after: ?source_publication.Key, limit: u32) !SourceReceiptGc {
        if (self.read_only) return error.ReadOnly;
        if (limit == 0 or limit > 64) return error.InvalidArgument;
        const Candidate = struct {
            binding: source_publication.Binding,
            record: source_publication.Record,
            readers: source_publication.SharedReaders.Handle,
            eligible: bool = false,
        };
        var candidates: [64]Candidate = undefined;
        var initialized: usize = 0;
        defer for (candidates[0..initialized]) |*item| item.readers.deinit();
        var result: SourceReceiptGc = .{};
        var owner_identity: source_publication.Digest = undefined;
        {
            self.lockStorage();
            defer self.unlockStorage();
            var txn = try self.beginWriteMainTxn();
            errdefer txn.abort();
            try requireSourceOwnerOpen(&txn);
            var owner = (try sourceOwnerRecordInTxn(&txn)) orelse return error.IndexSourceOwnerMismatch;
            if (owner.current == 0) return error.SourcePublicationEpochRequired;
            owner_identity = owner.identity;
            result.current_epoch = owner.current;
            const state = self.source_maintenance.load(.acquire) orelse return error.IndexSourceOwnerMismatch;
            if (owner.floor < owner.current) {
                if (state.registry.hasPreparationsBefore(owner.current)) {
                    // An in-flight newer birth delays only the next floor.
                    // Receipts already below the durable floor remain eligible.
                    result.waiting_publishers = true;
                } else {
                    owner.floor = owner.current;
                    try txn.put(.meta, source_owner_key, &try owner.encode());
                    try txn.commit();
                    // This fence must survive a crash before receipt deletion.
                    try self.main_store_owner.sync(true);
                    state.durable_epoch_floor = owner.floor;
                    txn = try self.beginWriteMainTxn();
                }
            }
            defer txn.abort();
            // A previous post-commit sync may have failed. A visible floor
            // alone is not authority to forget its per-publication fences.
            if (state.durable_epoch_floor < owner.floor) {
                try self.main_store_owner.sync(true);
                state.durable_epoch_floor = owner.floor;
            }
            const start = source_publication.ownerPrefix(owner.identity);
            if (after) |cursor| if (!std.mem.startsWith(u8, &cursor, &start)) return error.InvalidArgument;
            var cursor = try txn.openCursor(.meta);
            defer cursor.close();
            var row = try cursor.seekAtOrAfter(if (after) |*key| key else &start);
            if (row != null and after != null and std.mem.eql(u8, row.?.key, &after.?)) row = try cursor.next();
            var visited: u32 = 0;
            var receipt_metadata: PublicationTxn = .{ .txn = &txn };
            while (row) |entry| {
                if (!std.mem.startsWith(u8, entry.key, &start)) break;
                if (visited == limit) {
                    result.more = true;
                    break;
                }
                const binding = try source_publication.bindingFromKey(entry.key);
                const record = try source_publication.Record.decode(binding, entry.value);
                if (record.epoch > owner.current or (record.epoch != 0 and source_epoch.tokenEpoch(binding) != record.epoch)) return error.InvalidIndexPublication;
                result.after = entry.key[0..@sizeOf(source_publication.Key)].*;
                visited += 1;
                if (record.state != .published) {
                    if (record.epoch == owner.current) result.current_retired += 1;
                    if (record.epoch < owner.floor and !try exact_source_map.hasRoot(&receipt_metadata, binding) and
                        (record.role != .family or !try family_graph.ownsPublication(&receipt_metadata, binding.owner, binding.publication)) and
                        ((record.role != .source_leaf and record.role != .fields) or try family_graph.resourceNode(&receipt_metadata, binding.owner, binding.publication) == null))
                    {
                        var readers = try self.openSourceReaders(binding);
                        readers.retire();
                        if (readers.drained() != null) {
                            candidates[initialized] = .{ .binding = binding, .record = record, .readers = readers };
                            initialized += 1;
                        } else {
                            readers.watch(state.port());
                            readers.deinit();
                        }
                    }
                }
                row = try cursor.next();
            }
        }
        if (initialized == 0) return result;
        var eligible_count: usize = 0;
        {
            var read = try primary.beginReadTxn();
            defer read.abort();
            for (candidates[0..initialized]) |*candidate| {
                if (try source_recovery.load(&read, candidate.binding) != null or try source_claims.bindingHasClaims(&read, candidate.binding)) continue;
                candidate.eligible = true;
                eligible_count += 1;
            }
        }
        if (eligible_count == 0) return result;
        try primary.sync(true);
        self.lockStorage();
        defer self.unlockStorage();
        var txn = try self.beginWriteMainTxn();
        errdefer txn.abort();
        try requireSourceOwnerOpen(&txn);
        const owner = (try sourceOwnerRecordInTxn(&txn)) orelse return error.IndexSourceOwnerMismatch;
        if (!std.mem.eql(u8, &owner.identity, &owner_identity)) return error.IndexSourceOwnerMismatch;
        var metadata: PublicationTxn = .{ .txn = &txn };
        for (candidates[0..initialized]) |candidate| {
            if (!candidate.eligible or candidate.readers.drained() == null or candidate.record.epoch >= owner.floor) continue;
            const current = (try source_publication.load(&metadata, candidate.binding)) orelse continue;
            if (!current.eql(candidate.record)) return error.IndexPublicationConflict;
            // A missing journal alone is not evidence that a manually
            // published map has finished its independent cleanup protocol.
            if (try exact_source_map.hasRoot(&metadata, candidate.binding)) continue;
            if (current.role == .family and try family_graph.ownsPublication(&metadata, candidate.binding.owner, candidate.binding.publication)) continue;
            if ((current.role == .source_leaf or current.role == .fields) and try family_graph.resourceNode(&metadata, candidate.binding.owner, candidate.binding.publication) != null) continue;
            for (current.segments[0..current.count]) |id| {
                if (try sourceSegmentReboundToFamily(&txn, id, candidate.binding)) continue;
                if (current.role == .family) {
                    const bound = (try sourceSegmentBindingForReader(&txn, id, true)) orelse continue;
                    if (!std.meta.eql(bound, candidate.binding)) continue;
                }
                _ = txn.get(.meta, &activeSegmentMetaKey(id)) catch |err| switch (err) {
                    error.NotFound => continue,
                    else => return err,
                };
                return error.IndexPublicationStillActive;
            }
            for (current.segments[0..current.count]) |id| {
                if (try sourceSegmentBindingForReader(&txn, id, true)) |binding| {
                    if (!std.meta.eql(binding, candidate.binding)) {
                        if (current.role == .family or try sourceSegmentReboundToFamily(&txn, id, candidate.binding)) continue;
                        return error.IndexPublicationConflict;
                    }
                    try txn.delete(.meta, &sourceSegmentKey(id));
                }
            }
            try txn.delete(.meta, &source_publication.key(candidate.binding));
            result.purged += 1;
        }
        try txn.commit();
        return result;
    }

    fn bindSourceOwner(txn: *MainTxn, owner: source_publication.Digest) !void {
        if (std.mem.allEqual(u8, &owner, 0)) return error.InvalidIndexPublication;
        if (try sourceOwnerInTxn(txn)) |existing| {
            if (!std.mem.eql(u8, &existing, &owner)) return error.IndexSourceOwnerMismatch;
            return;
        }
        var raw: [68]u8 = undefined;
        @memcpy(raw[0..4], "ISO1");
        @memcpy(raw[4..36], &owner);
        std.crypto.hash.sha2.Sha256.hash(raw[0..36], raw[36..68], .{});
        try txn.put(.meta, source_owner_key, &raw);
    }

    /// Reserve the durable physical index identity before preparing primary
    /// claims, so recovery can route even the first unpublished claim safely.
    /// Owner identities are globally unique and preserved by physical backup.
    pub fn reserveSourceOwner(self: *PersistentIndex, owner: source_publication.Digest) !void {
        if (self.read_only) return error.ReadOnly;
        if (self.source_owner_hint) |hint| hint.store(true, .release);
        self.lockStorage();
        defer self.unlockStorage();
        return self.reserveSourceOwnerLocked(owner);
    }

    /// Concurrent producers converge on one durable owner. Checking and
    /// reserving in separate critical sections can race on the first batch.
    pub fn ensureSourceOwner(self: *PersistentIndex, io: std.Io) !source_publication.Digest {
        if (self.read_only) return error.ReadOnly;
        if (self.source_owner_hint) |hint| hint.store(true, .release);
        self.lockStorage();
        defer self.unlockStorage();
        const previous = previous: {
            var txn = try self.beginReadMainTxn();
            defer txn.abort();
            break :previous try sourceOwnerInTxn(&txn);
        };
        var identity: source_publication.Digest = previous orelse @splat(0);
        if (previous == null) io.random(&identity);
        try self.reserveSourceOwnerLocked(identity);
        return identity;
    }

    fn reserveSourceOwnerLocked(self: *PersistentIndex, owner: source_publication.Digest) !void {
        if (std.mem.allEqual(u8, &owner, 0)) return error.InvalidIndexPublication;
        const existing = self.source_maintenance.load(.acquire);
        if (existing) |state| {
            if (!std.mem.eql(u8, &state.owner, &owner)) return error.IndexSourceOwnerMismatch;
            if (state.closed) return error.IndexSourceOwnerClosed;
            if (state.durable) return;
        }
        const state = existing orelse try SourceMaintenance.create(self.alloc, owner, self.source_reader_broker);
        errdefer if (existing == null) SourceMaintenance.release(state);
        var txn = try self.beginWriteMainTxn();
        errdefer txn.abort();
        try bindSourceOwner(&txn, owner);
        try requireSourceOwnerOpen(&txn);
        try txn.commit();
        try self.main_store_owner.sync(true);
        state.durable = true;
        if (existing == null) self.source_maintenance.store(state, .release);
    }

    pub fn sourceOwner(self: *PersistentIndex) !?source_publication.Digest {
        self.lockStorage();
        defer self.unlockStorage();
        var txn = try self.beginReadMainTxn();
        defer txn.abort();
        return sourceOwnerInTxn(&txn);
    }

    fn sourceSegmentKey(id: u64) ["index_source_segment_v1:".len + 8]u8 {
        var out: ["index_source_segment_v1:".len + 8]u8 = undefined;
        @memcpy(out[0.."index_source_segment_v1:".len], "index_source_segment_v1:");
        std.mem.writeInt(u64, out["index_source_segment_v1:".len..], id, .big);
        return out;
    }

    fn sourceSegmentBinding(txn: *MainTxn, id: u64) !?source_publication.Binding {
        return sourceSegmentBindingForReader(txn, id, false);
    }

    fn sourceSegmentReboundToFamily(txn: *MainTxn, id: u64, previous: source_publication.Binding) !bool {
        const bound = (try sourceSegmentBindingForReader(txn, id, true)) orelse return false;
        if (std.meta.eql(bound, previous)) return false;
        var metadata: PublicationTxn = .{ .txn = txn };
        const record = (try source_publication.load(&metadata, bound)) orelse return error.InvalidIndexPublication;
        return record.role == .family and record.state != .cancelled and record.count == 1 and record.segments[0] == id;
    }

    fn sourceSegmentBindingForReader(txn: *MainTxn, id: u64, allow_retired: bool) !?source_publication.Binding {
        const raw = txn.get(.meta, &sourceSegmentKey(id)) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        if (raw.len != 100 or !std.mem.eql(u8, raw[0..4], "ISL1")) return error.InvalidIndexPublication;
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update(&sourceSegmentKey(id));
        hash.update(raw[0..68]);
        if (!std.mem.eql(u8, raw[68..], &hash.finalResult())) return error.InvalidIndexPublication;
        const binding: source_publication.Binding = .{ .owner = raw[4..36].*, .publication = raw[36..68].* };
        const owner_record = (try sourceOwnerRecordInTxn(txn)) orelse return error.InvalidIndexPublication;
        if (!std.mem.eql(u8, &owner_record.identity, &binding.owner)) return error.IndexSourceOwnerMismatch;
        var metadata: PublicationTxn = .{ .txn = txn };
        const record = (try source_publication.load(&metadata, binding)) orelse return error.InvalidIndexPublication;
        if (record.epoch > owner_record.current or (record.epoch != 0 and source_epoch.tokenEpoch(binding) != record.epoch)) return error.InvalidIndexPublication;
        if ((record.state != .published and !(allow_retired and record.state == .retired)) or std.mem.indexOfScalar(u64, record.segments[0..record.count], id) == null) return error.InvalidIndexPublication;
        return binding;
    }

    fn stageSourceSegment(txn: *MainTxn, id: u64, binding: source_publication.Binding) !void {
        var raw: [100]u8 = undefined;
        @memcpy(raw[0..4], "ISL1");
        @memcpy(raw[4..36], &binding.owner);
        @memcpy(raw[36..68], &binding.publication);
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update(&sourceSegmentKey(id));
        hash.update(raw[0..68]);
        @memcpy(raw[68..], &hash.finalResult());
        try txn.put(.meta, &sourceSegmentKey(id), &raw);
    }

    const PublicationTxn = struct {
        txn: *MainTxn,
        /// A failed compound source operation cannot be caught and committed.
        /// Abort is idempotent and keeps normal metadata commits unchanged.
        pub fn poison(self: *@This()) void {
            self.txn.abort();
        }
        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            return self.txn.get(.meta, key);
        }
        pub fn put(self: *@This(), key: []const u8, value: []const u8) !void {
            return self.txn.put(.meta, key, value);
        }
        pub fn delete(self: *@This(), key: []const u8) !void {
            return self.txn.delete(.meta, key);
        }
        pub fn openCursor(self: *@This()) !backend_erased.Cursor {
            return self.txn.openCursor(.meta);
        }
        pub fn hasPrefix(self: *@This(), prefix: []const u8) !bool {
            var cursor = try self.openCursor();
            defer cursor.close();
            cursor.setKeysOnly();
            const item = (try cursor.seekAtOrAfter(prefix)) orelse return false;
            return std.mem.startsWith(u8, item.key, prefix);
        }
    };

    const MainTxn = struct {
        read: ?backend_erased.NamespaceReadTxn = null,
        write: ?backend_erased.NamespaceWriteTxn = null,
        retirement_cleanup_started: bool = false,

        const ReadAdapter = backend_adapter.NamespaceReadTxn(MainTxn, MainKeyspace, .{
            .abort = abort,
            .get = get,
        });
        const WriteAdapter = backend_adapter.NamespaceWriteTxn(MainTxn, MainKeyspace, .{
            .abort = abort,
            .commit = commit,
            .get = get,
            .put = put,
            .delete = delete,
        });

        fn open(store: *backend_erased.NamespaceStore, read_only: bool) !MainTxn {
            if (read_only) {
                return .{ .read = try store.beginRead() };
            }
            return .{ .write = try store.beginWrite() };
        }

        pub fn abort(self: *MainTxn) void {
            if (self.read) |*txn| {
                txn.abort();
                self.read = null;
            }
            if (self.write) |*txn| {
                txn.abort();
                self.write = null;
            }
        }

        pub fn commit(self: *MainTxn) !void {
            if (self.write) |*txn| {
                try txn.commit();
                self.write = null;
                return;
            }
            return if (self.read != null) error.ReadOnly else error.TransactionClosed;
        }

        pub fn get(self: *MainTxn, keyspace: MainKeyspace, key: []const u8) ![]const u8 {
            const namespace = runtimeNamespace(keyspace);
            if (self.read) |*txn| return try txn.get(namespace, key);
            if (self.write) |*txn| return try txn.get(namespace, key);
            return error.TransactionClosed;
        }

        pub fn put(self: *MainTxn, keyspace: MainKeyspace, key: []const u8, value: []const u8) !void {
            if (self.write) |*txn| {
                try txn.put(runtimeNamespace(keyspace), key, value);
                return;
            }
            return error.ReadOnly;
        }

        pub fn delete(self: *MainTxn, keyspace: MainKeyspace, key: []const u8) !void {
            if (self.write) |*txn| {
                try txn.delete(runtimeNamespace(keyspace), key);
                return;
            }
            return error.ReadOnly;
        }

        pub fn openCursor(self: *MainTxn, keyspace: MainKeyspace) !backend_erased.Cursor {
            const namespace = runtimeNamespace(keyspace);
            if (self.read) |*txn| return try txn.openCursor(namespace);
            if (self.write) |*txn| return try txn.openCursor(namespace);
            return error.TransactionClosed;
        }

        fn readAdapter(self: *MainTxn) ReadAdapter {
            return ReadAdapter.init(self);
        }

        fn writeAdapter(self: *MainTxn) WriteAdapter {
            return WriteAdapter.init(self);
        }
    };

    fn beginReadMainTxn(self: *PersistentIndex) !MainTxn {
        return try MainTxn.open(&self.main_store, true);
    }

    fn beginWriteMainTxn(self: *PersistentIndex) !MainTxn {
        return try MainTxn.open(&self.main_store, false);
    }

    fn backendCapabilities(_: *PersistentIndex) backend_types.Capabilities {
        return .{
            .ordered_ranges = true,
            .reverse_ranges = true,
            .cursors = true,
            .native_namespaces = true,
            .write_batches = .atomic,
            .single_writer = true,
            .read_snapshots = .snapshot,
        };
    }

    pub fn backendStore(self: *PersistentIndex) BackendStore {
        return BackendStore.init(self);
    }

    /// Read generation-owned metadata from the same durable namespace as the
    /// active segment catalog. The returned bytes are owned by `alloc`.
    pub fn readGenerationMetadataAlloc(
        self: *PersistentIndex,
        alloc: Allocator,
        key: []const u8,
    ) !?[]u8 {
        self.lockStorage();
        defer self.unlockStorage();

        var txn = try self.beginReadMainTxn();
        defer txn.abort();
        const value = txn.get(.meta, key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        return try alloc.dupe(u8, value);
    }

    /// Persist generation-owned metadata before any segment whose semantics
    /// depend on it is admitted to the generation.
    pub fn writeGenerationMetadata(
        self: *PersistentIndex,
        key: []const u8,
        value: []const u8,
    ) !void {
        if (self.read_only) return error.ReadOnly;
        self.lockStorage();
        defer self.unlockStorage();

        var txn = try self.beginWriteMainTxn();
        errdefer txn.abort();
        try txn.put(.meta, key, value);
        try txn.commit();
    }

    pub fn deleteGenerationMetadata(self: *PersistentIndex, key: []const u8) !void {
        if (self.read_only) return error.ReadOnly;
        self.lockStorage();
        defer self.unlockStorage();

        var txn = try self.beginWriteMainTxn();
        errdefer txn.abort();
        txn.delete(.meta, key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
        try txn.commit();
    }

    pub fn loadProjectionSeal(self: *PersistentIndex, alloc: Allocator) !?projection_seal.Seal {
        const raw = (try self.readGenerationMetadataAlloc(alloc, projection_seal.metadata_key)) orelse return null;
        defer alloc.free(raw);
        return try projection_seal.Seal.decode(raw);
    }

    /// Called only after ordered replay has applied every effect through the
    /// supplied cut. Force physical durability BEFORE publishing its coverage;
    /// a crash may leave data ahead of the seal, never a seal ahead of data.
    /// The owner must serialize this with its per-index replay/apply lane.
    pub fn publishProjectionSeal(self: *PersistentIndex, seal: projection_seal.Seal) !void {
        if (self.read_only) return error.ReadOnly;
        _ = try seal.encode();
        var next = seal;
        self.lockStorage();
        defer self.unlockStorage();
        var already_covered = false;
        {
            var read = try self.beginReadMainTxn();
            defer read.abort();
            const previous = read.get(.meta, projection_seal.metadata_key) catch |err| if (err == error.NotFound) null else return err;
            if (previous) |raw| {
                const old = try projection_seal.Seal.decode(raw);
                if (!old.sameIdentity(seal)) return error.ProjectionSealIdentityChanged;
                // Incremental replay preserves the completed baseline of this
                // physical generation. Reset/removal deletes the entire seal.
                if (next.baseline == null) next.baseline = old.baseline;
                next.applied_sequence = @max(next.applied_sequence, old.applied_sequence);
                already_covered = std.meta.eql(old, next);
            }
        }
        // Even an idempotent retry must finish a previously failed seal sync.
        try self.main_store_owner.sync(true);
        if (already_covered) return;
        try self.wal.sync(true);
        var txn = try self.beginWriteMainTxn();
        errdefer txn.abort();
        try txn.put(.meta, projection_seal.metadata_key, &try next.encode());
        try txn.commit();
        try self.main_store_owner.sync(true);
    }

    /// Physical segments, including fully-deleted segments, are projection
    /// history. They prevent changing a generation's analyzer semantics.
    pub fn hasPhysicalSegments(self: *PersistentIndex) bool {
        const snap = self.writer.acquireSnapshot();
        defer snap.release();
        return snap.segments.len != 0;
    }

    fn openMainStore(alloc: Allocator, index_path: []const u8, opts: PersistentIndexOptions) !OpenedMainStore {
        switch (opts.main_backend) {
            .mem => {
                const backend = try alloc.create(mem_backend.Backend);
                errdefer alloc.destroy(backend);
                backend.* = mem_backend.Backend.init(alloc, .{});
                errdefer backend.close();

                var store = try backend.runtimeNamespaceStore(alloc);
                errdefer store.deinit();
                return .{
                    .store = store,
                    .owner = .{ .mem = backend },
                };
            },
            .lsm_memory => {
                var handle = try lsm_backend.BackendHandle.init(alloc, resolvedMainLsmOptions(opts, true));
                errdefer handle.close();

                var store = try handle.backend.runtimeNamespaceStore(alloc);
                errdefer store.deinit();
                return .{
                    .store = store,
                    .owner = .{ .lsm = handle },
                };
            },
            .lsm => {
                var handle = try lsm_backend.BackendHandle.open(alloc, index_path, resolvedMainLsmOptions(opts, false));
                errdefer handle.close();

                var store = try handle.backend.runtimeNamespaceStore(alloc);
                errdefer store.deinit();
                return .{
                    .store = store,
                    .owner = .{ .lsm = handle },
                };
            },
        }
    }

    /// Open or create a persistent index. Recovers existing state + replays WAL.
    pub fn open(alloc: Allocator, opts: PersistentIndexOptions) !PersistentIndex {
        return openWithPurpose(alloc, opts, .query);
    }

    /// A closed physical owner's metadata-only recovery handle. It never
    /// opens segment readers, maps artifacts, or replays payload-bearing WAL.
    pub const SourceOwnerCleanup = struct {
        index: PersistentIndex,
        owner: source_publication.Digest,
        sweep: source_recovery.Sweep = .{},
        active: bool = true,
        finished: bool = false,
        pub const Progress = struct { worked: bool = false, more: bool = false, finished: bool = false, waiting: bool = false };
        pub fn open(a: Allocator, options: PersistentIndexOptions, owner: source_publication.Digest) !@This() {
            var opts = options;
            if (opts.read_only) return error.ReadOnly;
            opts.main_lsm_options.backend.create_if_missing = false;
            var index = try openWithPurpose(a, opts, .source_reclamation);
            errdefer index.close();
            const actual = (try index.sourceOwner()) orelse return error.IndexSourceOwnerMismatch;
            if (!std.mem.eql(u8, &actual, &owner)) return error.IndexSourceOwnerMismatch;
            index.source_roots_present = true;
            try index.closeSourceOwner(owner);
            if (index.source_maintenance.load(.acquire)) |state| try state.registry.watch(state.port());
            return .{ .index = index, .owner = owner };
        }
        pub fn deinit(self: *@This()) void {
            self.index.close();
            self.* = undefined;
        }
        pub fn pending(self: *const @This()) bool {
            return !self.finished and (self.active or self.index.sourceOwnershipMaintenancePending());
        }
        pub fn step(self: *@This(), primary: anytype, limit: u32) !Progress {
            if (limit == 0 or limit > 64) return error.InvalidArgument;
            if (self.finished) return .{ .finished = true };
            if (!self.pending()) return .{ .waiting = true };
            if (self.index.source_maintenance.load(.acquire)) |state| _ = state.pending.swap(false, .acq_rel);
            self.active = true;
            if (try self.index.retireClosedSourceRootsWindow(limit) != 0) return .{ .worked = true, .more = true };
            var family_nodes: u64 = 0;
            if (self.index.source_maintenance.load(.acquire)) |state| if (state.families_durable.load(.acquire)) {
                const graph = try self.index.collectSourceFamilyGarbage(primary, limit);
                family_nodes = graph.remaining_nodes;
                if (graph.more) return .{ .worked = graph.worked, .more = true };
            };
            const result = try self.sweep.step(self.index.alloc, primary, &self.index, self.owner, limit);
            if (result.scan_more) return .{ .worked = result.progress.worked, .more = true };
            // No preparation can create a new journal after its last pin is
            // released. Check pins before the final primary seek, then force
            // primary cleanup durability before allowing physical deletion.
            if (family_nodes == 0 and !self.index.source_maintenance.load(.acquire).?.registry.hasPins() and !try source_recovery.ownerHasJobs(primary, self.owner)) {
                try primary.sync(true);
                try self.index.sync(true);
                self.finished = true;
                return .{ .finished = true };
            }
            self.active = false;
            return .{ .waiting = true };
        }
    };

    fn retireClosedSourceRootsWindow(self: *PersistentIndex, limit: u32) !usize {
        self.lockStorage();
        defer self.unlockStorage();
        var txn = try self.beginWriteMainTxn();
        errdefer txn.abort();
        if (!try sourceOwnerClosedInTxn(&txn)) return error.IndexSourceOwnerNotClosed;
        var ids: [64]u64 = undefined;
        var count: usize = 0;
        {
            var cursor = try txn.openCursor(.meta);
            defer cursor.close();
            var row = try cursor.seekAtOrAfter(meta_active_segment_prefix);
            while (row) |entry| {
                if (!std.mem.startsWith(u8, entry.key, meta_active_segment_prefix)) break;
                if (entry.key.len != meta_active_segment_prefix.len + 8) return error.InvalidIndexPublication;
                ids[count] = std.mem.readInt(u64, entry.key[meta_active_segment_prefix.len..][0..8], .big);
                count += 1;
                if (count == limit) break;
                row = try cursor.next();
            }
        }
        if (count == 0) {
            txn.abort();
            return 0;
        }
        for (ids[0..count]) |id| {
            const key = std.mem.toBytes(std.mem.nativeToBig(u64, id));
            try self.updateActiveSegments(&txn, id, .remove);
            txn.delete(.segments, &key) catch |err| if (err != error.NotFound) return err;
            txn.delete(.deletions, &key) catch |err| if (err != error.NotFound) return err;
            try self.deleteSegmentRange(&txn, id);
        }
        try self.retireRemovedSourceRoots(&txn, ids[0..count]);
        try txn.commit();
        return count;
    }

    const OpenPurpose = enum { query, source_reclamation };
    fn openWithPurpose(alloc: Allocator, opts: PersistentIndexOptions, purpose: OpenPurpose) !PersistentIndex {
        // The new owner is unpublished until recovery completes.
        const storage_mu = try alloc.create(std.Io.Mutex);
        errdefer alloc.destroy(storage_mu);
        storage_mu.* = .init;
        const path_span = std.mem.span(opts.path);
        const wal_storage = opts.wal_storage orelse opts.main_lsm_storage;
        const needs_host_dirs =
            (opts.main_backend == .lsm and opts.main_lsm_storage == null) or
            (opts.resolvedWalBackend() == .lsm and wal_storage == null);

        // Create subdirectories
        const index_path_raw = try std.fmt.allocPrint(alloc, "{s}/index", .{path_span});
        defer alloc.free(index_path_raw);
        const index_path = try alloc.dupeSentinel(u8, index_path_raw, 0);
        defer alloc.free(index_path);
        const index_path_span = index_path[0..index_path.len];
        if (purpose == .query and builtin.os.tag != .freestanding and needs_host_dirs) {
            var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
            defer io_impl.deinit();
            try storage_io.createDirPathPortable(io_impl.io(), path_span);
            try storage_io.createDirPathPortable(io_impl.io(), index_path_span);
        }

        const wal_path_raw = try std.fmt.allocPrint(alloc, "{s}/wal", .{path_span});
        defer alloc.free(wal_path_raw);
        const wal_path = try alloc.dupeSentinel(u8, wal_path_raw, 0);
        defer alloc.free(wal_path);

        var segment_files: ?SegmentFileStore = null;
        if (purpose == .query and (builtin.os.tag != .freestanding or opts.main_lsm_storage != null)) {
            var segments_buf: [512]u8 = undefined;
            const segments_path = std.fmt.bufPrint(&segments_buf, "{s}/segments", .{path_span}) catch return error.PathTooLong;
            segment_files = try SegmentFileStore.open(
                alloc,
                segments_path,
                opts.main_lsm_storage,
                !opts.read_only,
                opts.main_lsm_options.io_runtime,
                opts.main_lsm_options.native_storage_pool,
            );
        }
        errdefer if (segment_files) |*store| store.close();

        // Open WAL
        var wal = try wal_mod.WAL.open(wal_path.ptr, .{
            .map_size = opts.wal_map_size,
            .no_sync = opts.wal_no_sync,
            .commit_backend = walCommitBackendForOptions(opts.wal_commit_backend),
            .group_commit_window_ns = opts.wal_group_commit_window_ns,
            .group_commit_max_requests = opts.wal_group_commit_max_requests,
            .backend = if (purpose == .source_reclamation) .lsm_memory else opts.resolvedWalBackend(),
            .read_only = opts.read_only,
            .storage = if (purpose == .source_reclamation) null else wal_storage,
            .lsm_options = opts.wal_lsm_options,
            .clock = opts.wal_clock,
            .commit_scheduler = opts.wal_commit_scheduler,
            .model_commit_backend_completions = opts.model_wal_commit_backend_completions,
        });
        errdefer wal.close();

        // Open main backing store
        var opened_main = try openMainStore(alloc, index_path_span, opts);
        errdefer {
            opened_main.store.deinit();
            opened_main.owner.close(alloc);
        }

        // Open namespaces eagerly on writable backends
        if (!opts.read_only) {
            var setup_txn = try MainTxn.open(&opened_main.store, false);
            errdefer setup_txn.abort();
            try setup_txn.commit();
        }

        var writer = try index_mod.IndexWriter.init(alloc);
        errdefer writer.deinit();
        var stale_active_ids = std.ArrayListUnmanaged(u64).empty;
        defer stale_active_ids.deinit(alloc);

        // Recovery: load existing segments from metadata plus immutable files.
        var committed_lsn: u64 = 0;
        var source_roots_present = false;
        var source_maintenance: ?*SourceMaintenance = null;
        errdefer if (source_maintenance) |state| SourceMaintenance.release(state);
        {
            var read_txn = try MainTxn.open(&opened_main.store, true);
            defer read_txn.abort();

            // Read committed_lsn
            const lsn_bytes = read_txn.get(.meta, meta_committed_lsn) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
            if (lsn_bytes) |lb| {
                if (lb.len >= 8) committed_lsn = std.mem.readInt(u64, lb[0..8], .little);
            }

            // One owner probe per index avoids an absent per-segment lookup
            // on ordinary legacy roots, independently of segment count.
            const source_owner_record = try sourceOwnerRecordInTxn(&read_txn);
            const source_owner = if (source_owner_record) |record| record.identity else null;
            if (purpose == .query and source_owner != null) {
                if (opts.source_owner_hint) |hint| hint.store(true, .release);
            }
            if (source_owner) |owner| if (!opts.read_only) {
                source_maintenance = try SourceMaintenance.create(alloc, owner, opts.source_reader_broker);
                source_maintenance.?.closed = try sourceOwnerClosedInTxn(&read_txn);
                source_maintenance.?.epoch_enabled.store(source_owner_record.?.current != 0, .release);
                source_maintenance.?.families_durable.store(source_owner_record.?.families, .release);
                source_maintenance.?.fields_durable.store(source_owner_record.?.fields, .release);
            };
            if (purpose == .query and source_owner != null and try sourceOwnerClosedInTxn(&read_txn)) return error.IndexSourceOwnerClosed;
            const active_ids = if (purpose == .query) try loadActiveSegmentIdsFromTxn(&read_txn, alloc) else try alloc.alloc(u64, 0);
            defer alloc.free(active_ids);
            for (active_ids) |seg_id| {
                const seg_key = std.mem.toBytes(std.mem.nativeToBig(u64, seg_id));
                const seg_bytes = read_txn.get(.segments, &seg_key) catch |err| switch (err) {
                    error.NotFound => null,
                    else => return err,
                };
                const field_binding: ?source_publication.Binding = if (source_owner_record != null and source_owner_record.?.fields) blk: {
                    const raw = read_txn.get(.meta, &fieldSegmentKey(seg_id)) catch |err| switch (err) {
                        error.NotFound => break :blk null,
                        else => return err,
                    };
                    break :blk try decodeFieldBinding(raw);
                } else null;
                var segment_data: ?index_mod.SegmentData = if (field_binding) |binding| blk: {
                    const store = segment_files orelse return error.UnsupportedSourcePublication;
                    if (!std.mem.eql(u8, &binding.owner, &source_owner.?)) return error.IndexSourceOwnerMismatch;
                    var readers = if (source_maintenance) |state| try state.registry.openPublication(alloc, binding) else try openReadersWithBroker(opts.source_reader_broker, alloc, binding);
                    defer readers.deinit();
                    if (source_maintenance) |state| readers.watch(state.port());
                    var scope = try readers.lease();
                    defer scope.deinit();
                    var metadata: PublicationTxn = .{ .txn = &read_txn };
                    var inventory = try field_source.open(alloc, &metadata, &scope, store.storage, store.root_dir, .{});
                    defer inventory.deinit();
                    var authenticated = try field_source.authenticated(inventory, alloc);
                    break :blk index_mod.SegmentData.fromAuthenticated(authenticated) catch |err| {
                        authenticated.deinit();
                        return err;
                    };
                } else if (seg_bytes) |bytes| blk: {
                    break :blk if (segment_files) |*store|
                        try store.publishExisting(seg_id, bytes)
                    else
                        index_mod.SegmentData.fromOwnedHeap(try alloc.dupe(u8, bytes));
                } else if (segment_files) |*store| blk: {
                    const segment_path = try store.pathAlloc(seg_id);
                    defer store.allocator.free(segment_path);
                    if (comptime builtin.os.tag != .freestanding) {
                        if (store.storage_owner != null) {
                            break :blk index_mod.SegmentData.fromMapped(store.mapFile(segment_path) catch |err| switch (err) {
                                error.FileNotFound => {
                                    try stale_active_ids.append(alloc, seg_id);
                                    continue;
                                },
                                else => return err,
                            });
                        }
                    }
                    if (store.storage.vtable.open_leased_immutable_source != null) {
                        const source = store.storage.openLeasedImmutableSource(alloc, segment_path) catch |err| switch (err) {
                            error.FileNotFound => {
                                try stale_active_ids.append(alloc, seg_id);
                                continue;
                            },
                            else => return err,
                        };
                        break :blk index_mod.SegmentData.fromNative(source);
                    }
                    if (store.storage.vtable.map_immutable_artifact != null) {
                        const artifact = store.storage.mapImmutableArtifact(alloc, segment_path) catch |err| switch (err) {
                            error.FileNotFound => {
                                try stale_active_ids.append(alloc, seg_id);
                                continue;
                            },
                            // Read-only filesystems still support the existing
                            // heap-backed reader when no private view is possible.
                            error.MappedArtifactUnavailable => null,
                            else => return err,
                        };
                        if (artifact) |view| break :blk index_mod.SegmentData.fromMappedArtifact(view);
                    }
                    const loaded = store.storage.readFileAlloc(alloc, segment_path, std.math.maxInt(usize)) catch |err| switch (err) {
                        error.FileNotFound => {
                            try stale_active_ids.append(alloc, seg_id);
                            continue;
                        },
                        else => return err,
                    };
                    break :blk index_mod.SegmentData.fromOwnedHeap(loaded);
                } else {
                    try stale_active_ids.append(alloc, seg_id);
                    continue;
                };
                segment_data.?.madviseAccessPattern();
                errdefer if (segment_data) |*data| data.deinit(alloc);
                var source_lease: source_publication.SharedReaders.Registration = .{};
                defer source_lease.deinit();
                if (source_owner != null) {
                    if (try sourceSegmentBinding(&read_txn, seg_id)) |binding| {
                        var readers = if (source_maintenance) |state| try state.registry.openPublication(alloc, binding) else try openReadersWithBroker(opts.source_reader_broker, alloc, binding);
                        defer readers.deinit();
                        if (source_maintenance) |state| readers.watch(state.port());
                        source_lease = try readers.register();
                    }
                }
                writer.addSegmentWithIdData(seg_id, segment_data.?) catch |err| {
                    if (isStaleActiveSegmentDataError(err)) {
                        segment_data.?.deinit(alloc);
                        segment_data = null;
                        try stale_active_ids.append(alloc, seg_id);
                        continue;
                    }
                    return err;
                };
                segment_data = null;
                if (source_lease.cell != null) {
                    const recovered = writer.current;
                    const registration = source_lease;
                    source_lease = .{};
                    try recovered.segments[recovered.segments.len - 1].attachSourceGeneration(alloc, registration);
                    source_roots_present = true;
                }

                const deletion_bytes = read_txn.get(.deletions, &seg_key) catch |err| switch (err) {
                    error.NotFound => null,
                    else => return err,
                };
                if (deletion_bytes) |bytes| {
                    var bitmap = try roaring.RoaringBitmap.fromBytes(alloc, bytes);
                    if (!bitmap.isEmpty()) {
                        writer.setDeletionBitmap(seg_id, bitmap);
                    } else {
                        bitmap.deinit();
                    }
                }
            }

            const next_seg_id_bytes = read_txn.get(.meta, meta_next_seg_id) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
            if (next_seg_id_bytes) |bytes| {
                if (bytes.len >= @sizeOf(u64)) {
                    writer.next_segment_id = @max(
                        writer.next_segment_id,
                        std.mem.readInt(u64, bytes[0..@sizeOf(u64)], .little),
                    );
                }
            }
        }

        // Replay WAL entries after committed_lsn
        const wal_entries = if (purpose == .query) try wal.iterateFrom(alloc, committed_lsn + 1) else &.{};
        defer {
            for (wal_entries) |e| alloc.free(@constCast(e.data));
            alloc.free(wal_entries);
        }

        var pi = PersistentIndex{
            .source_reader_broker = opts.source_reader_broker,
            .source_owner_hint = opts.source_owner_hint,
            .source_roots_present = source_roots_present,
            .source_maintenance = .init(source_maintenance),
            .storage_mu = storage_mu,
            .io = opts.io,
            .alloc = alloc,
            .writer = writer,
            .main_store = opened_main.store,
            .main_store_owner = opened_main.owner,
            .segment_files = segment_files,
            .wal = wal,
            .committed_lsn = committed_lsn,
            .main_backend = opts.main_backend,
            .wal_backend = opts.resolvedWalBackend(),
            .main_map_size = opts.main_map_size,
            .wal_map_size = opts.wal_map_size,
            .read_only = opts.read_only,
        };

        // Replay each WAL entry
        if (!opts.read_only) {
            for (wal_entries) |entry| {
                try pi.replayWalEntry(entry.lsn, entry.data);
            }
        }

        if (!opts.read_only and stale_active_ids.items.len > 0) {
            std.log.warn("persistent index pruning {d} stale active segment references path={s}", .{
                stale_active_ids.items.len,
                path_span,
            });
            try pi.pruneMissingActiveSegmentsLocked(stale_active_ids.items);
        }
        if (!opts.read_only and purpose == .query) {
            try pi.pruneActiveSegmentsMissingRangesOrDataLocked(path_span);
        }

        if (pi.segment_files) |*store| {
            pi.retired_segment_file_deleter = try RetiredSegmentFileDeleter.init(alloc, store);
            pi.writer.setRetiredSegmentCleanup(pi.retired_segment_file_deleter.?.cleanup());
            if (!opts.read_only) pi.reconcileRetiredSegmentFilesLocked();
        }

        return pi;
    }

    fn lockStorage(self: *PersistentIndex) void {
        self.storage_mu.lockUncancelable(persistentStorageIo(self.io));
    }

    fn unlockStorage(self: *PersistentIndex) void {
        self.storage_mu.unlock(persistentStorageIo(self.io));
    }

    pub fn close(self: *PersistentIndex) void {
        self.lockStorage();
        self.writer.deinit();
        self.flushCompletedRetirementMarkersLocked();
        if (self.retired_segment_file_deleter) |deleter| {
            if (deleter.native_storage_lease == null) deleter.disarm();
        }
        self.wal.close();
        self.main_store.deinit();
        self.main_store_owner.close(self.alloc);
        if (self.retired_segment_file_deleter) |deleter| deleter.release();
        if (self.segment_files) |*store| store.close();
        if (self.source_maintenance.swap(null, .acq_rel)) |state| SourceMaintenance.release(state);
        self.unlockStorage();
        self.alloc.destroy(self.storage_mu);
        self.* = undefined;
    }

    /// Tears down a pre-crash owner without allowing it to flush, compact, or
    /// reclaim files after storage has been rolled back to a durable snapshot.
    pub fn abandonAfterCrash(self: *PersistentIndex) void {
        self.lockStorage();
        if (self.retired_segment_file_deleter) |deleter| {
            // Disarming here is too late to order against the storage rollback.
            // Keep the call defensive in optimized builds, but make misuse fail
            // loudly in Debug so every modeled-crash owner establishes the fence.
            std.debug.assert(deleter.isDisarmed());
            deleter.disarm();
        }
        self.writer.deinit();
        self.wal.abandonAfterCrash();
        self.main_store.deinit();
        self.main_store_owner.abandonAfterCrash(self.alloc);
        if (self.retired_segment_file_deleter) |deleter| deleter.release();
        if (self.segment_files) |*store| store.close();
        if (self.source_maintenance.swap(null, .acq_rel)) |state| SourceMaintenance.release(state);
        self.unlockStorage();
        self.alloc.destroy(self.storage_mu);
        self.* = undefined;
    }

    /// Fence delayed cleanup before a modeled device rolls its namespace back.
    /// On return, no retired-segment callback is executing and future callbacks
    /// owned by this generation are inert. Callers must invoke this before the
    /// rollback, then use abandonAfterCrash() to tear down the stale generation.
    pub fn prepareForCrashRollback(self: *PersistentIndex) void {
        self.lockStorage();
        defer self.unlockStorage();
        if (self.retired_segment_file_deleter) |deleter| deleter.disarm();
    }

    fn clearCompletedRetirementMarkersInTxn(self: *PersistentIndex, txn: *MainTxn) !void {
        if (txn.retirement_cleanup_started) return;
        txn.retirement_cleanup_started = true;
        const deleter = self.retired_segment_file_deleter orelse return;
        const store = &(self.segment_files orelse return);

        // Retry work discovered by an earlier transaction once per subsequent
        // publication boundary. Newly discovered failures wait for the next
        // boundary, avoiding a tight retry loop on a persistent I/O failure.
        deleter.retryFailedDeletes();
        if (self.retirement_reconcile_pending) {
            if (self.discoverRetiredSegmentFilesInTxn(txn)) {
                self.retirement_reconcile_pending = false;
            } else |err| {
                // Reconciliation is maintenance, not a publication
                // prerequisite. Preserve the pending bit and let the write
                // proceed; a later transaction retries the scan.
                if (builtin.os.tag != .freestanding) {
                    std.log.warn("retired text segment reconciliation deferred: {s}", .{@errorName(err)});
                }
            }
        }
        const completed_ids = deleter.snapshotCompletedIds(self.alloc, RetiredSegmentFileDeleter.completed_markers_per_boundary) catch return;
        defer self.alloc.free(completed_ids);
        if (completed_ids.len == 0) return;

        const pending_ids = self.alloc.alloc(u64, completed_ids.len) catch return;
        defer self.alloc.free(pending_ids);
        var pending_count: usize = 0;
        for (completed_ids) |seg_id| {
            const retired_key = retiredSegmentMetaKey(seg_id);
            if (txn.get(.meta, &retired_key)) |_| {
                pending_ids[pending_count] = seg_id;
                pending_count += 1;
            } else |err| switch (err) {
                error.NotFound => deleter.acknowledgeCompletedId(seg_id),
                else => return err,
            }
        }
        if (pending_count == 0) return;

        store.syncDeletes() catch |err| {
            if (builtin.os.tag != .freestanding) {
                std.log.warn("retired text segment directory sync deferred: {s}", .{@errorName(err)});
            }
            return;
        };
        for (pending_ids[0..pending_count]) |seg_id| {
            const retired_key = retiredSegmentMetaKey(seg_id);
            txn.delete(.meta, &retired_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
        }
    }

    fn flushCompletedRetirementMarkersLocked(self: *PersistentIndex) void {
        if (self.read_only) return;
        const deleter = self.retired_segment_file_deleter orelse return;
        if (!self.retirement_reconcile_pending and !deleter.hasRetirementWork()) return;
        var txn = self.beginWriteMainTxn() catch |err| {
            if (builtin.os.tag != .freestanding) {
                std.log.warn("retired text segment marker flush deferred: {s}", .{@errorName(err)});
            }
            return;
        };
        var txn_open = true;
        defer if (txn_open) txn.abort();
        self.clearCompletedRetirementMarkersInTxn(&txn) catch |err| {
            if (builtin.os.tag != .freestanding) {
                std.log.warn("retired text segment marker flush deferred: {s}", .{@errorName(err)});
            }
            return;
        };
        txn.commit() catch |err| {
            if (builtin.os.tag != .freestanding) {
                std.log.warn("retired text segment marker flush commit deferred: {s}", .{@errorName(err)});
            }
            return;
        };
        txn_open = false;
    }

    /// Discover durable retirement work on open. Failed scans stay pending;
    /// failed unlinks are retained by the live owner and retried at the next
    /// segment publication boundary (or close), without rescanning the catalog.
    fn reconcileRetiredSegmentFilesLocked(self: *PersistentIndex) void {
        if (self.read_only) return;
        if (self.segment_files == null or self.retired_segment_file_deleter == null) return;
        self.retirement_reconcile_pending = true;
        self.flushCompletedRetirementMarkersLocked();
    }

    fn discoverRetiredSegmentFilesInTxn(self: *PersistentIndex, txn: *MainTxn) !void {
        const deleter = self.retired_segment_file_deleter orelse return;
        const retired_ids = try loadRetiredSegmentIdsFromTxn(txn, self.alloc);
        defer self.alloc.free(retired_ids);
        for (retired_ids) |seg_id| {
            const active_key = activeSegmentMetaKey(seg_id);
            const is_active = if (txn.get(.meta, &active_key)) |_| true else |err| switch (err) {
                error.NotFound => false,
                else => return err,
            };
            if (is_active) {
                try deleter.completeWithoutDelete(seg_id);
            } else {
                try deleter.deleteOrQueue(seg_id);
            }
        }
    }

    pub fn resetAllForRebuild(self: *PersistentIndex) !void {
        if (self.read_only) return error.ReadOnly;

        const retired_cleanup = self.writer.retired_segment_cleanup;
        const resource_manager = self.writer.resource_manager;
        var replacement_writer = try index_mod.IndexWriter.init(self.alloc);
        var replacement_writer_moved = false;
        defer if (!replacement_writer_moved) replacement_writer.deinit();
        replacement_writer.setRetiredSegmentCleanup(retired_cleanup);
        if (resource_manager) |manager| replacement_writer.attachResourceManager(manager);

        self.lockStorage();
        defer self.unlockStorage();

        // Segment filenames are keyed by ID. Preserve the high-water mark so
        // cleanup retained by an older snapshot can never target a newly
        // rebuilt segment, including after closing and reopening an empty
        // rebuilt index.
        replacement_writer.next_segment_id = self.writer.next_segment_id;

        const active_ids = blk: {
            var read_txn = try self.beginReadMainTxn();
            defer read_txn.abort();
            break :blk try self.loadActiveSegmentIds(&read_txn, self.alloc);
        };
        defer self.alloc.free(active_ids);

        var txn = try self.beginWriteMainTxn();
        errdefer txn.abort();
        for (active_ids) |seg_id| {
            const seg_key = std.mem.toBytes(std.mem.nativeToBig(u64, seg_id));
            txn.delete(.segments, &seg_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            txn.delete(.deletions, &seg_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            try self.deleteSegmentRange(&txn, seg_id);
            try self.updateActiveSegments(&txn, seg_id, .remove);
        }
        try saveNextSegmentId(&txn, replacement_writer.next_segment_id);
        txn.delete(.meta, meta_rebuild_cursor) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
        txn.delete(.meta, projection_seal.metadata_key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
        try txn.commit();

        // The catalog transaction is the logical reset commit. Retaining the
        // committed WAL watermark makes every pre-reset record ineligible for
        // replay, even if the process crashes immediately after this point.
        // Normal post-reset indexing advances and truncates the WAL later.
        self.writer.deinit();
        self.writer = replacement_writer;
        replacement_writer_moved = true;

        // Physical reclamation follows publication. Failures are recoverable
        // through the retirement markers committed above.
        if (self.retired_segment_file_deleter) |deleter| {
            for (active_ids) |seg_id| RetiredSegmentFileDeleter.delete(deleter, seg_id);
        }
    }

    pub fn sync(self: *PersistentIndex, force: bool) !void {
        self.lockStorage();
        defer self.unlockStorage();
        try self.main_store_owner.sync(force);
        try self.wal.sync(force);
    }

    pub fn syncMain(self: *PersistentIndex, force: bool) !void {
        self.lockStorage();
        defer self.unlockStorage();
        try self.main_store_owner.sync(force);
    }

    pub fn lsmMaintenanceScore(self: *const PersistentIndex) u64 {
        return self.main_store_owner.lsmMaintenanceScore();
    }

    pub fn lsmMaintenanceDebtHint(self: *const PersistentIndex) u64 {
        return switch (self.main_store_owner) {
            .mem => 0,
            .lsm => |handle| handle.backend.maintenanceDebtHint(),
        };
    }

    pub fn nextLsmMaintenanceWakeDelayNsBestEffort(self: *const PersistentIndex) ?u64 {
        return switch (self.main_store_owner) {
            .mem => null,
            .lsm => |handle| handle.backend.nextMaintenanceWakeDelayNsBestEffort(),
        };
    }

    pub fn refreshLsmMaintenanceDebtHint(self: *PersistentIndex) void {
        switch (self.main_store_owner) {
            .mem => {},
            .lsm => |handle| handle.backend.refreshMaintenanceDebtHint(),
        }
    }

    pub fn snapshotLsmMaintenanceStats(self: *const PersistentIndex) ?lsm_backend.Backend.MaintenanceStats {
        return self.main_store_owner.snapshotLsmMaintenanceStats();
    }

    pub fn snapshotLsmWriteStats(self: *const PersistentIndex) ?lsm_backend.Backend.WriteStats {
        return self.main_store_owner.snapshotLsmWriteStats();
    }

    pub fn snapshotLsmOpenStats(self: *const PersistentIndex) ?lsm_backend.Backend.OpenStats {
        return self.main_store_owner.snapshotLsmOpenStats();
    }

    pub fn checkpointLsmWalAfterDurableBoundary(self: *PersistentIndex) !void {
        try self.main_store_owner.checkpointLsmWalAfterDurableBoundary();
        try self.wal.checkpointLsmWalAfterDurableBoundary();
    }

    pub const NativeCheckpoints = struct {
        main: lsm_backend.Backend.NativeCheckpoint,
        wal: lsm_backend.Backend.NativeCheckpoint,
        segments: ?NativeSegmentCheckpoint,

        pub fn deinit(self: *NativeCheckpoints) void {
            if (self.segments) |*segments| segments.deinit();
            self.wal.deinit();
            self.main.deinit();
            self.* = undefined;
        }
    };

    /// A retained writer snapshot pins every active immutable segment and its
    /// mapped bytes. Retired cleanup cannot unlink the selected generation
    /// until this owner is released after off-fence materialization.
    pub const NativeSegmentCheckpoint = struct {
        snapshot: *index_mod.IndexSnapshot,
        source_root: []const u8,

        pub fn seal(self: *const NativeSegmentCheckpoint, alloc: Allocator, io: std.Io, destination_root: []const u8, cancellation: CancellationToken) !u64 {
            try fs_paths.createDirPathPortable(io, destination_root);
            var total: u64 = 0;
            var seen: std.AutoHashMapUnmanaged(u64, source_publication.Digest) = .empty;
            defer seen.deinit(alloc);
            for (self.snapshot.segments) |segment| {
                try cancellation.check();
                if (segment.data == .authenticated) {
                    total = try std.math.add(u64, total, try materializeFields(segment.data.authenticated.*, alloc, io, destination_root, cancellation, null, &seen));
                    continue;
                }
                const source = try std.fmt.allocPrint(alloc, "{s}/{d}.seg", .{ self.source_root, segment.id });
                defer alloc.free(source);
                const target = try std.fmt.allocPrint(alloc, "{s}/{d}.seg", .{ destination_root, segment.id });
                defer alloc.free(target);
                try std.Io.Dir.hardLink(.cwd(), source, .cwd(), target, io, .{});
                total = std.math.add(u64, total, @intCast(segment.data.len())) catch return error.FileTooBig;
            }
            try fs_paths.syncDirPortable(io, destination_root);
            return total;
        }

        pub fn deinit(self: *NativeSegmentCheckpoint) void {
            self.snapshot.release();
            self.* = undefined;
        }

        pub fn materialize(
            self: *const NativeSegmentCheckpoint,
            alloc: Allocator,
            io: std.Io,
            destination_root: []const u8,
            cancellation: CancellationToken,
        ) !u64 {
            return try self.materializeWithSink(alloc, io, destination_root, cancellation, null);
        }

        pub fn materializeWithSink(
            self: *const NativeSegmentCheckpoint,
            alloc: Allocator,
            io: std.Io,
            destination_root: []const u8,
            cancellation: CancellationToken,
            sink: ?native_artifact_sink.Sink,
        ) !u64 {
            try fs_paths.createDirPathPortable(io, destination_root);
            var total: u64 = 0;
            var seen: std.AutoHashMapUnmanaged(u64, source_publication.Digest) = .empty;
            defer seen.deinit(alloc);
            for (self.snapshot.segments) |*segment| {
                try cancellation.check();
                if (segment.data == .authenticated) {
                    total = try std.math.add(u64, total, try materializeFields(segment.data.authenticated.*, alloc, io, destination_root, cancellation, sink, &seen));
                    continue;
                }
                const destination = try std.fmt.allocPrint(alloc, "{s}/{d}.seg", .{ destination_root, segment.id });
                defer alloc.free(destination);
                var file = try fs_paths.createFilePortable(io, destination, .{ .truncate = true });
                defer file.close(io);
                var writer_buffer: [16 * 1024]u8 = undefined;
                var writer = file.writer(io, &writer_buffer);
                var hash = native_artifact_sink.Sha256.init(.{});
                var read_buffer: [64 * 1024]u8 = undefined;
                var offset: u64 = 0;
                const source = segment.data.source();
                while (offset < source.len()) {
                    try cancellation.check();
                    const bytes = read_buffer[0..@intCast(@min(read_buffer.len, source.len() - offset))];
                    try source.readInto(offset, bytes);
                    try writer.interface.writeAll(bytes);
                    if (sink != null) hash.update(bytes);
                    offset += bytes.len;
                }
                try writer.end();
                try file.sync(io);
                if (sink) |active| {
                    var digest: [native_artifact_sink.Sha256.digest_length]u8 = undefined;
                    hash.final(&digest);
                    try active.record(destination, @intCast(segment.data.len()), digest);
                }
                total = std.math.add(u64, total, @intCast(segment.data.len())) catch
                    return error.FileTooBig;
            }
            try fs_paths.syncDirPortable(io, destination_root);
            return total;
        }
        fn materializeFields(authenticated: field_source.Authenticated, alloc: Allocator, io: std.Io, destination_root: []const u8, cancellation: CancellationToken, sink: ?native_artifact_sink.Sink, seen: *std.AutoHashMapUnmanaged(u64, source_publication.Digest)) !u64 {
            var total: u64 = 0;
            const count = try authenticated.resourceCount();
            for (0..count) |ordinal| {
                try cancellation.check();
                var resource = try authenticated.resource(alloc, ordinal);
                defer resource.source.close();
                const entry = try seen.getOrPut(alloc, resource.descriptor.file);
                if (entry.found_existing) {
                    if (!std.mem.eql(u8, &entry.value_ptr.*, &resource.descriptor.identity)) return error.InvalidIndexFieldResource;
                    continue;
                }
                entry.value_ptr.* = resource.descriptor.identity;
                const destination = try std.fmt.allocPrint(alloc, "{s}/{d}.seg", .{ destination_root, resource.descriptor.file });
                defer alloc.free(destination);
                var file = try fs_paths.createFilePortable(io, destination, .{ .truncate = true });
                defer file.close(io);
                var writer_buffer: [16 * 1024]u8 = undefined;
                var writer = file.writer(io, &writer_buffer);
                var hash = native_artifact_sink.Sha256.init(.{});
                var read_buffer: [64 * 1024]u8 = undefined;
                var offset: u64 = 0;
                while (offset < resource.source.len()) {
                    try cancellation.check();
                    const bytes = read_buffer[0..@intCast(@min(read_buffer.len, resource.source.len() - offset))];
                    try resource.source.readInto(offset, bytes);
                    try writer.interface.writeAll(bytes);
                    if (sink != null) hash.update(bytes);
                    offset += bytes.len;
                }
                try writer.end();
                try file.sync(io);
                if (sink) |active| try active.record(destination, resource.source.len(), hash.finalResult());
                total = try std.math.add(u64, total, resource.source.len());
            }
            return total;
        }
    };

    /// Pins both stores which make up a persistent text-index generation.
    /// The caller keeps mutation admission fenced only for this bounded
    /// manifest/run-reference operation; materialization happens later.
    pub fn pinNativeCheckpoints(self: *PersistentIndex) !NativeCheckpoints {
        self.lockStorage();
        defer self.unlockStorage();
        const segments = if (self.segment_files != null)
            NativeSegmentCheckpoint{ .snapshot = self.writer.acquireSnapshot(), .source_root = self.segment_files.?.root_dir }
        else
            null;
        errdefer if (segments) |checkpoint| checkpoint.snapshot.release();
        var main = try self.main_store_owner.pinNativeCheckpoint();
        errdefer main.deinit();
        const wal = try self.wal.pinNativeCheckpoint();
        return .{ .main = main, .wal = wal, .segments = segments };
    }

    pub fn snapshotLsmNativeStorageStats(self: *const PersistentIndex) ?lsm_backend.NativeStorageStats {
        return self.main_store_owner.snapshotLsmNativeStorageStats();
    }

    pub fn runLsmMaintenanceStep(self: *PersistentIndex) !bool {
        self.lockStorage();
        defer self.unlockStorage();
        return try self.main_store_owner.runLsmMaintenanceStep();
    }

    pub fn runLsmMaintenanceStepBestEffort(self: *PersistentIndex) !bool {
        self.lockStorage();
        defer self.unlockStorage();
        return try self.main_store_owner.runLsmMaintenanceStepBestEffort();
    }

    /// Index a pre-built segment (WAL-protected, durable after return).
    pub fn indexSegment(self: *PersistentIndex, segment_bytes: []const u8) !void {
        const owned = try self.alloc.dupe(u8, segment_bytes);
        try self.indexSegmentOwned(owned);
    }

    /// Like indexSegment(), but takes ownership of segment_bytes.
    pub fn indexSegmentOwned(self: *PersistentIndex, segment_bytes: []u8) !void {
        _ = try self.indexSegmentOwnedPublication(segment_bytes, null, null);
    }

    /// Takes ownership on every path. The caller first pins exact source
    /// afterimages in the primary store. Root and publication receipt commit
    /// atomically here; acknowledgment in the primary store may follow later.
    pub fn indexSegmentOwnedWithSources(self: *PersistentIndex, segment_bytes: []u8, binding: source_publication.Binding) !source_publication.Decision {
        return (try self.indexSegmentOwnedPublication(segment_bytes, binding, null)).?;
    }

    /// Immutable page map is committed with the segment and receipt. Entries
    /// follow segment-local document order and bind exact primary afterimages.
    pub fn indexSegmentOwnedWithSourceMap(self: *PersistentIndex, segment_bytes: []u8, binding: source_publication.Binding, refs: []const source_claims.Reference) !source_publication.Decision {
        return (try self.indexSegmentOwnedPublication(segment_bytes, binding, refs)).?;
    }

    /// Accept independently budgeted preparation buffers. Compatible ownership
    /// transfers directly; other allocators cross into the index owner once.
    pub fn indexSegmentOwnedWithSourceMapFromAllocator(self: *PersistentIndex, owner: Allocator, bytes: []u8, binding: source_publication.Binding, refs: []const source_claims.Reference) !source_publication.Decision {
        if (owner.ptr == self.alloc.ptr and owner.vtable == self.alloc.vtable) return self.indexSegmentOwnedWithSourceMap(bytes, binding, refs);
        defer owner.free(bytes);
        return self.indexSegmentOwnedWithSourceMap(try self.alloc.dupe(u8, bytes), binding, refs);
    }

    fn indexSegmentOwnedPublication(self: *PersistentIndex, segment_bytes: []u8, binding: ?source_publication.Binding, refs: ?[]const source_claims.Reference) !?source_publication.Decision {
        self.lockStorage();
        defer self.unlockStorage();

        var owned: ?[]u8 = segment_bytes;
        defer if (owned) |buf| self.alloc.free(buf);

        var source_plan: ?exact_source_map.Plan = null;
        defer if (source_plan) |plan| plan.deinit(self.alloc);
        var source_root: source_publication.Digest = undefined;
        if (binding) |identity| {
            if (self.read_only) return error.ReadOnly;
            // Legacy inline WAL records cannot bind a receipt during replay.
            // Reject before appending anything; those callers retain embedded
            // source bytes until a receipt-aware WAL envelope is available.
            if (self.segment_files == null or self.active_rebuild_page != null) return error.UnsupportedSourcePublication;
            std.crypto.hash.sha2.Sha256.hash(segment_bytes, &source_root, .{});
            if (refs) |inputs| {
                source_plan = try exact_source_map.Plan.prepare(self.alloc, identity, source_root, inputs);
                source_root = source_plan.?.root();
            }
            var txn = try self.beginReadMainTxn();
            defer txn.abort();
            var metadata: PublicationTxn = .{ .txn = &txn };
            if (try sourceOwnerInTxn(&txn)) |owner| {
                if (!std.mem.eql(u8, &owner, &identity.owner)) return error.IndexSourceOwnerMismatch;
            }
            if (try source_publication.load(&metadata, identity)) |existing| {
                if (existing.state == .cancelled) return error.IndexPublicationCancelled;
                if (existing.state == .retired) return error.IndexPublicationRetired;
                if (!std.mem.eql(u8, &existing.root, &source_root)) return error.IndexPublicationConflict;
                // Finish the physical durability barrier even on a retry.
                txn.abort();
                return self.durableSourceDecisionLocked(identity);
            }
        }

        if (self.active_rebuild_page) |page| {
            const id = self.reserveSegmentId();
            var data = if (self.segment_files != null)
                try self.materializeSegmentData(id, owned.?)
            else blk: {
                const value = index_mod.SegmentData.fromOwnedHeap(owned.?);
                owned = null;
                break :blk value;
            };
            errdefer {
                data.deinit(self.alloc);
                self.deleteSegmentFile(id);
            }
            try page.segments.append(self.alloc, .{ .id = id, .data = data });
            return null;
        }

        const profile_enabled = benchPersistentPublishEnabled();
        const total_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
        var wal_append_ns: u64 = 0;
        var reserve_id_ns: u64 = 0;
        var materialize_ns: u64 = 0;
        var persist_ns: u64 = 0;
        var writer_publish_ns: u64 = 0;
        const begin_write_ns: u64 = 0;
        const build_sink_ns: u64 = 0;
        const finish_write_ns: u64 = 0;
        const map_segment_ns: u64 = 0;
        const key_range_ns: u64 = 0;
        var wal_truncate_ns: u64 = 0;
        const uses_segment_wal = self.segment_files == null;

        // 1. Write inline segment bytes to WAL. File-backed segments are
        // atomically published before the active metadata commit, so replay can
        // recover from metadata alone and orphan pre-commit files are harmless.
        const lsn = if (uses_segment_wal) blk: {
            const wal_append_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
            const appended_lsn = try self.wal.append(owned.?);
            if (profile_enabled) wal_append_ns = platform_time.monotonicNs() - wal_append_start_ns;
            break :blk appended_lsn;
        } else self.committed_lsn;

        // 2. Allocate segment ID under writer lock to prevent races
        const reserve_id_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
        self.writer.lockMutex();
        const seg_id = self.writer.next_segment_id;
        self.writer.next_segment_id += 1;
        self.writer.mu.unlock();
        if (profile_enabled) reserve_id_ns = platform_time.monotonicNs() - reserve_id_start_ns;

        // 3. Publish immutable file-backed segment bytes before metadata becomes visible.
        var segment_data: ?index_mod.SegmentData = null;
        if (self.segment_files != null) {
            const materialize_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
            segment_data = try self.materializeSegmentData(seg_id, owned.?);
            segment_data.?.madviseAccessPattern();
            if (profile_enabled) materialize_ns = platform_time.monotonicNs() - materialize_start_ns;
        } else {
            segment_data = index_mod.SegmentData.fromOwnedHeap(owned.?);
            owned = null;
        }
        var rollback_segment = true;
        errdefer {
            if (rollback_segment) {
                if (segment_data) |*data| data.deinit(self.alloc);
                if (self.segment_files != null) self.deleteSegmentFile(seg_id);
            }
        }

        var replacement = [_]index_mod.ReplacementSegmentData{.{
            .id = seg_id,
            .data = segment_data.?,
        }};
        var writer_publication = try self.writer.prepareSegmentsManyData(&.{}, &replacement);
        defer writer_publication.abort();

        if (binding) |identity| {
            std.debug.assert(writer_publication.snapshot.segments.len > writer_publication.carried_count);
            try self.attachSourceReferences(&writer_publication.snapshot.segments[writer_publication.carried_count], identity, refs);
        }

        // 4. Persist metadata.
        const persist_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
        try self.persistSegmentWithPublication(seg_id, owned orelse segment_data.?.bytes(), lsn, if (binding) |identity| .{ .binding = identity, .root = source_root, .source_map_plan = if (source_plan) |*plan| plan else null } else null);
        if (profile_enabled) persist_ns = platform_time.monotonicNs() - persist_start_ns;

        // 5. Every fallible snapshot allocation was staged before catalog
        // commit. Publication is now infallible and transfers segment_data.
        const writer_publish_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
        if (binding != null) self.source_roots_present = true;
        writer_publication.publish();
        if (binding != null) self.noteSourceRecoveryWork();
        segment_data = null;
        rollback_segment = false;
        if (profile_enabled) writer_publish_ns = platform_time.monotonicNs() - writer_publish_start_ns;

        // 6. Truncate WAL
        if (uses_segment_wal) {
            const wal_truncate_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
            try self.wal.truncate(lsn);
            if (profile_enabled) wal_truncate_ns = platform_time.monotonicNs() - wal_truncate_start_ns;
        }

        if (profile_enabled) {
            std.log.info(
                "antfly_bench_text_publish seg_id={d} bytes={d} total_ms={d} wal_append_ms={d} reserve_id_ms={d} materialize_ms={d} begin_write_ms={d} build_sink_ms={d} finish_write_ms={d} map_segment_ms={d} key_range_ms={d} persist_ms={d} writer_publish_ms={d} wal_truncate_ms={d} file_backed={} uses_segment_wal={}",
                .{
                    seg_id,
                    segment_bytes.len,
                    nsToMs(platform_time.monotonicNs() - total_start_ns),
                    nsToMs(wal_append_ns),
                    nsToMs(reserve_id_ns),
                    nsToMs(materialize_ns),
                    nsToMs(begin_write_ns),
                    nsToMs(build_sink_ns),
                    nsToMs(finish_write_ns),
                    nsToMs(map_segment_ns),
                    nsToMs(key_range_ns),
                    nsToMs(persist_ns),
                    nsToMs(writer_publish_ns),
                    nsToMs(wal_truncate_ns),
                    self.segment_files != null,
                    uses_segment_wal,
                },
            );
        }
        return if (binding) |identity| try self.durableSourceDecisionLocked(identity) else null;
    }

    fn attachSourceReferences(self: *PersistentIndex, segment: *index_mod.SegmentEntry, binding: source_publication.Binding, refs: ?[]const source_claims.Reference) !void {
        if (self.source_owner_hint) |hint| hint.store(true, .release);
        var readers = try self.openSourceReaders(binding);
        defer readers.deinit();
        if (self.source_maintenance.load(.acquire)) |state| readers.watch(state.port());
        try segment.attachSourceGeneration(self.alloc, try readers.register());
        if (refs) |inputs| {
            if (inputs.len != segment.reader.doc_count) return error.InvalidIndexSourceMap;
            for (inputs, 0..) |ref, ordinal| {
                const id = (try segment.reader.storedIdAlloc(self.alloc, @intCast(ordinal))) orelse return error.InvalidIndexSourceMap;
                defer self.alloc.free(id);
                const keys = @import("internal_keys.zig");
                const physical = try keys.documentKeyAlloc(self.alloc, id);
                defer self.alloc.free(physical);
                const relational = try keys.relationalRowKeyAlloc(self.alloc, id);
                defer self.alloc.free(relational);
                var primary_digest: [32]u8 = undefined;
                var row_digest: [32]u8 = undefined;
                std.crypto.hash.sha2.Sha256.hash(physical, &primary_digest, .{});
                std.crypto.hash.sha2.Sha256.hash(relational, &row_digest, .{});
                if (!std.mem.eql(u8, &ref.document, &primary_digest) and !std.mem.eql(u8, &ref.document, &row_digest)) return error.InvalidIndexSourceMap;
            }
        }
    }

    fn openReadersWithBroker(broker: SourceReaderBroker, a: Allocator, binding: source_publication.Binding) !source_publication.SharedReaders.Handle {
        var registry = try broker.open(a, binding.owner);
        defer registry.deinit();
        return registry.openPublication(a, binding);
    }
    fn openSourceReaders(self: *PersistentIndex, binding: source_publication.Binding) !source_publication.SharedReaders.Handle {
        if (self.source_maintenance.load(.acquire)) |state| return state.registry.openPublication(self.alloc, binding);
        return openReadersWithBroker(self.source_reader_broker, self.alloc, binding);
    }

    /// An in-process preparation owns a pointer-sized pin until it publishes
    /// or is abandoned. Acquire under the same index lock as cancellation so
    /// recovery cannot cancel a live preparation between its two stores.
    pub fn beginSourcePublication(self: *PersistentIndex, binding: source_publication.Binding) !source_publication.SharedReaders.PublisherRegistration {
        if (self.read_only) return error.ReadOnly;
        self.lockStorage();
        defer self.unlockStorage();
        var txn = try self.beginReadMainTxn();
        defer txn.abort();
        const owner = (try sourceOwnerInTxn(&txn)) orelse return error.IndexSourceOwnerMismatch;
        if (!std.mem.eql(u8, &owner, &binding.owner)) return error.IndexSourceOwnerMismatch;
        try requireSourceOwnerOpen(&txn);
        var metadata: PublicationTxn = .{ .txn = &txn };
        if (try source_publication.load(&metadata, binding)) |record| return switch (record.state) {
            .published => error.IndexPublicationAlreadyCommitted,
            .cancelled => error.IndexPublicationCancelled,
            .retired => error.IndexPublicationRetired,
        };
        var readers = try self.openSourceReaders(binding);
        defer readers.deinit();
        if (self.source_maintenance.load(.acquire)) |state| readers.watch(state.port());
        const owner_record = (try sourceOwnerRecordInTxn(&txn)).?;
        return readers.prepare(try owner_record.admitBirth(binding));
    }

    /// Cancellation is durable before the primary store may release its
    /// pending source claims. The storage lock excludes a late publisher.
    pub fn cancelSourcePublication(self: *PersistentIndex, binding: source_publication.Binding) !source_publication.Decision {
        return self.cancelSourcePublicationMode(binding, false);
    }

    pub fn cancelSourcePublicationIfAbandoned(self: *PersistentIndex, binding: source_publication.Binding) !source_publication.Decision {
        return self.cancelSourcePublicationMode(binding, true);
    }

    fn cancelSourcePublicationMode(self: *PersistentIndex, binding: source_publication.Binding, only_abandoned: bool) !source_publication.Decision {
        if (self.read_only) return error.ReadOnly;
        self.lockStorage();
        defer self.unlockStorage();
        var preparations: ?source_publication.SharedReaders.Handle = if (only_abandoned) try self.openSourceReaders(binding) else null;
        defer if (preparations) |*value| value.deinit();
        if (preparations) |value| {
            if (self.source_maintenance.load(.acquire)) |state| value.watch(state.port());
            if (value.hasPins()) return error.IndexPublicationInFlight;
        }
        var txn = try self.beginWriteMainTxn();
        errdefer txn.abort();
        var metadata: PublicationTxn = .{ .txn = &txn };
        try bindSourceOwner(&txn, binding.owner);
        const owner_record = (try sourceOwnerRecordInTxn(&txn)).?;
        const cancellation_epoch = if (owner_record.current == 0) 0 else source_epoch.tokenEpoch(binding) orelse 0;
        if (cancellation_epoch > owner_record.current) return error.IndexPublicationEpochExpired;
        // The durable floor already rejects this old publication forever.
        // Do not let stale cancellation retries recreate collected receipts.
        if (cancellation_epoch < owner_record.floor and try source_publication.load(&metadata, binding) == null) {
            txn.abort();
            return (try self.durableSourceDecisionLocked(binding)) orelse error.InvalidIndexPublication;
        }
        try source_publication.stageCancelInEpoch(self.alloc, &metadata, binding, cancellation_epoch);
        try txn.commit();
        self.noteSourceRecoveryWork();
        return (try self.durableSourceDecisionLocked(binding)).?;
    }

    /// Admission hint only. An observed publication may still need its
    /// physical durability barrier; this state grants no ownership authority.
    pub fn sourcePublicationState(self: *PersistentIndex, binding: source_publication.Binding) !?source_publication.State {
        self.lockStorage();
        defer self.unlockStorage();
        var txn = try self.beginReadMainTxn();
        defer txn.abort();
        var metadata: PublicationTxn = .{ .txn = &txn };
        const record = (try source_publication.load(&metadata, binding)) orelse return null;
        return record.state;
    }

    /// Also completes the durability barrier after a failed post-commit sync.
    pub fn loadDurableSourcePublication(self: *PersistentIndex, binding: source_publication.Binding) !?source_publication.Decision {
        if (self.read_only) return error.ReadOnly;
        self.lockStorage();
        defer self.unlockStorage();
        return self.durableSourceDecisionLocked(binding);
    }

    fn durableSourceDecisionLocked(self: *PersistentIndex, binding: source_publication.Binding) !?source_publication.Decision {
        try self.main_store_owner.sync(true);
        var txn = try self.beginReadMainTxn();
        defer txn.abort();
        var metadata: PublicationTxn = .{ .txn = &txn };
        const result = (try source_publication.decision(&metadata, binding)) orelse return source_publication.cancelledByEpoch(&metadata, binding);
        const owner = (try sourceOwnerRecordInTxn(&txn)) orelse return error.IndexSourceOwnerMismatch;
        if (!std.mem.eql(u8, &owner.identity, &binding.owner)) return error.IndexSourceOwnerMismatch;
        if (result.epoch > owner.current or (result.epoch != 0 and source_epoch.tokenEpoch(binding) != result.epoch)) return error.InvalidIndexPublication;
        return result;
    }

    /// Marks only roots whose original segments are absent from the durable
    /// catalog. This is NOT permission to reclaim primary sources: the caller
    /// must additionally drain the original readers, including held snapshots.
    pub fn retireSourcePublication(self: *PersistentIndex, binding: source_publication.Binding) !source_publication.Decision {
        if (self.read_only) return error.ReadOnly;
        self.lockStorage();
        defer self.unlockStorage();
        var txn = try self.beginWriteMainTxn();
        errdefer txn.abort();
        var metadata: PublicationTxn = .{ .txn = &txn };
        const record = (try source_publication.load(&metadata, binding)) orelse return error.IndexPublicationMissing;
        if (record.role == .source_leaf or record.role == .fields) {
            if (try family_graph.resourceNode(&metadata, binding.owner, binding.publication)) |node| {
                if (try family_graph.referenceCount(&metadata, binding.owner, node) != 0) return error.IndexPublicationStillActive;
            }
        }
        for (record.segments[0..record.count]) |id| {
            if (try sourceSegmentReboundToFamily(&txn, id, binding)) continue;
            _ = txn.get(.meta, &activeSegmentMetaKey(id)) catch |err| switch (err) {
                error.NotFound => continue,
                else => return err,
            };
            return error.IndexPublicationStillActive;
        }
        try source_publication.stageRetire(self.alloc, &metadata, binding, &.{});
        try txn.commit();
        if (record.state != .retired) self.noteSourceRecoveryWork();
        return (try self.durableSourceDecisionLocked(binding)).?;
    }

    pub const CapturedSource = source_claims.CapturedSource;

    /// Initial family publication for ordinary producers and replay. Consumes
    /// the prepared physical segment on every path (its enclosing list remains
    /// caller-owned). Posting visibility, all leaf maps/receipts and the family
    /// root commit together; no intermediate unowned segment becomes visible.
    pub fn appendPreparedSourceFamily(self: *PersistentIndex, primary: anytype, physical: PreparedMergeSegment, layout: source_family.Layout, leaves: []source_recovery.Prepared, parent: *NewSourcePublication) !source_publication.Decision {
        return self.appendPreparedSourceFamilyImpl(primary, physical, layout, leaves, parent, null, null);
    }

    fn appendPreparedSourceFamilyImpl(self: *PersistentIndex, primary: anytype, physical: PreparedMergeSegment, layout: source_family.Layout, leaves: []source_recovery.Prepared, parent: *NewSourcePublication, captured: ?@import("../segment_source.zig").ContentStamp, deleted: ?*const roaring.RoaringBitmap) !source_publication.Decision {
        var incoming = physical;
        var transferred = false;
        defer {
            if (transferred) incoming.key_range.deinit(incoming.staging_alloc) else incoming.deinit(self.alloc);
        }
        if (@import("schema.zig").maximum_reader_format_version < 28) return error.UnsupportedVersion;
        if (self.read_only) return error.ReadOnly;
        if (layout.field_inventory != null) return error.SourcePublicationRequiresHandoff;
        if (layout.segment != incoming.id or layout.documents == 0 or leaves.len != (layout.documents - 1) / 64 + 1) return error.InvalidIndexSourceFamily;
        if (deleted) |bitmap| if (bitmap.maximum()) |maximum| if (maximum >= layout.documents) return error.InvalidIndexSourceFamily;
        try parent.publisher.require(parent.binding);
        const stamp = captured orelse try @import("../segment_source.zig").ContentStamp.capture(incoming.data.source());
        try stamp.require(incoming.data.source());
        if (!std.mem.eql(u8, &stamp.digest, &layout.physical)) return error.InvalidIndexSourceFamily;
        {
            var read = try primary.beginReadTxn();
            defer read.abort();
            for (leaves, 0..) |*leaf, n| {
                const first: u32 = @intCast(n * 64);
                try leaf.publisher.require(leaf.job.binding);
                if (leaf.job.role != .source_leaf or !std.mem.eql(u8, &leaf.job.binding.owner, &parent.binding.owner) or leaf.job.documents != try layout.count(first) or
                    !std.mem.eql(u8, &leaf.job.segment, &try layout.identity(first))) return error.InvalidIndexSourceFamily;
                try ((try source_recovery.load(&read, leaf.job.binding)) orelse return error.IndexSourcePublicationPending).requirePreparation(leaf.job);
            }
        }
        try primary.sync(true);
        self.lockStorage();
        defer self.unlockStorage();
        // Only delete a proven unpublished file. A commit with an ambiguous
        // outcome may leave durable catalog ownership that reopen must adopt.
        defer if (!transferred and self.sourcePreparedFileUnpublished(incoming.id)) self.deleteSegmentFile(incoming.id);
        if (self.segment_files == null or self.active_rebuild_page != null) return error.UnsupportedSourcePublication;
        var txn = try self.beginWriteMainTxn();
        errdefer txn.abort();
        try requireSourceOwnerOpen(&txn);
        const owner = (try sourceOwnerRecordInTxn(&txn)) orelse return error.IndexSourceOwnerMismatch;
        if (!owner.families or !std.mem.eql(u8, &owner.identity, &parent.binding.owner)) return error.IndexSourceOwnerMismatch;
        var metadata: PublicationTxn = .{ .txn = &txn };
        if (try source_publication.load(&metadata, parent.binding)) |existing| {
            if (existing.state != .published or existing.role != .family or existing.count != 1 or existing.segments[0] != incoming.id) return error.IndexPublicationConflict;
            const corpus = self.writer.acquireSnapshot();
            defer corpus.release();
            const present = for (corpus.segments) |*candidate| {
                if (candidate.id == incoming.id) break candidate;
            } else return error.IndexPublicationRequiresRecovery;
            var current_deleted = try present.cloneDeleted(self.alloc);
            defer if (current_deleted) |*bitmap| bitmap.deinit();
            if (deleted) |bitmap| {
                if (current_deleted) |*current| {
                    if (!bitmap.eql(current)) return error.IndexPublicationConflict;
                } else if (!bitmap.isEmpty()) return error.IndexPublicationConflict;
            } else if (current_deleted) |*current| if (!current.isEmpty()) return error.IndexPublicationConflict;
            var readers = try self.openSourceReaders(parent.binding);
            defer readers.deinit();
            var lease = try readers.lease();
            defer lease.deinit();
            const held = try family_graph.openHeld(&metadata, &lease);
            for (leaves, 0..) |leaf, n| {
                const selected = try source_family.select(&metadata, &held, @intCast(n * 64));
                if (!std.meta.eql(selected.layout, layout) or !std.mem.eql(u8, &selected.leaf.publication_id, &leaf.job.binding.publication) or !std.mem.eql(u8, &selected.leaf.root, &leaf.job.root)) return error.IndexPublicationConflict;
            }
            txn.abort();
            return (try self.durableSourceDecisionLocked(parent.binding)).?;
        }
        if (try sourceSegmentBinding(&txn, incoming.id) != null or try family_graph.loadRoot(&metadata, owner.identity, source_family.slot(incoming.id)) != null) return error.IndexPublicationConflict;
        var replacement = [_]index_mod.ReplacementSegmentData{.{ .id = incoming.id, .data = incoming.data, .deleted = deleted }};
        var staged = try self.writer.prepareSegmentsManyData(&.{}, &replacement);
        defer staged.abort();
        const entry = &staged.snapshot.segments[staged.carried_count];
        if (entry.reader.doc_count != layout.documents) return error.InvalidIndexSourceFamily;
        try self.attachSourceReferences(entry, parent.binding, null);
        try family_graph.initialize(&metadata, owner.identity);
        var builder = try source_family.Builder.init(owner.identity, layout);
        for (leaves, 0..) |*leaf, n| {
            try self.validateSourceCohortReferences(entry, leaf, @intCast(n * 64));
            const plan = try exact_source_map.Plan.prepare(self.alloc, leaf.job.binding, leaf.job.segment, leaf.references);
            defer plan.deinit(self.alloc);
            if (!std.mem.eql(u8, &plan.root(), &leaf.job.root)) return error.IndexPublicationConflict;
            _ = try source_publication.stagePublishRoleInEpoch(self.alloc, &metadata, leaf.job.binding, leaf.job.root, &.{}, try owner.admitPublication(leaf.job.binding), .source_leaf);
            try plan.stage(&metadata);
            try builder.append(&metadata, (try source_publication.decision(&metadata, leaf.job.binding)).?, .{});
        }
        const root = try builder.finish(&metadata, .{});
        _ = try source_publication.stagePublishRoleInEpoch(self.alloc, &metadata, parent.binding, root, &.{incoming.id}, try owner.admitPublication(parent.binding), .family);
        if (!try family_graph.compareAndSwap(&metadata, owner.identity, source_family.slot(incoming.id), null, parent.binding.publication, root, .{})) return error.IndexPublicationConflict;
        if (deleted) |bitmap| if (!bitmap.isEmpty()) {
            const encoded = try bitmap.toBytes(self.alloc);
            defer self.alloc.free(encoded);
            const key = std.mem.toBytes(std.mem.nativeToBig(u64, incoming.id));
            try txn.put(.deletions, &key, encoded);
        };
        try self.saveSegmentRange(&txn, incoming.id, incoming.key_range);
        try self.updateActiveSegments(&txn, incoming.id, .add);
        try stageSourceSegment(&txn, incoming.id, parent.binding);
        try saveNextSegmentId(&txn, self.writer.next_segment_id);
        try txn.commit();
        staged.publish();
        transferred = true;
        self.source_roots_present = true;
        self.noteSourceRecoveryWork();
        return (try self.durableSourceDecisionLocked(parent.binding)).?;
    }

    /// Drain exact-name field preparations whose actual native publisher has
    /// gone away. Discovery is bounded and rotates past live preparations.
    /// Successful publications remove the marker in their catalog transaction;
    /// any remaining file is owned by this recovery debt until durable unlink.
    fn recoverFieldPreparationsLocked(self: *PersistentIndex, state: *SourceMaintenance, limit: u32) !bool {
        if (!state.fields_durable.load(.acquire)) return false;
        self.lockStorage();
        defer self.unlockStorage();
        var txn = try self.beginWriteMainTxn();
        errdefer txn.abort();
        const Item = struct { file: u64, binding: source_publication.Binding };
        var items: [128]Item = undefined;
        var count: usize = 0;
        {
            var cursor = try txn.openCursor(.meta);
            defer cursor.close();
            const after_key = fieldPreparationKey(state.field_preparation_after orelse 0);
            var entry = try cursor.seekAtOrAfter(if (state.field_preparation_after != null) &after_key else field_preparation_prefix);
            if (state.field_preparation_after != null and entry != null and std.mem.eql(u8, entry.?.key, &after_key)) entry = try cursor.next();
            while (entry) |item| : (entry = try cursor.next()) {
                if (!std.mem.startsWith(u8, item.key, field_preparation_prefix)) break;
                if (item.key.len != field_preparation_prefix.len + 8) return error.InvalidIndexFieldGeneration;
                items[count] = .{ .file = std.mem.readInt(u64, item.key[field_preparation_prefix.len..][0..8], .big), .binding = try decodeFieldBinding(item.value) };
                count += 1;
                if (count == limit) break;
            }
        }
        var changed = false;
        var metadata: PublicationTxn = .{ .txn = &txn };
        for (items[0..count]) |item| {
            if (!std.mem.eql(u8, &item.binding.owner, &state.owner)) return error.IndexSourceOwnerMismatch;
            var readers = try self.openSourceReaders(item.binding);
            defer readers.deinit();
            readers.watch(state.port());
            if (readers.hasPreparations()) continue;
            // A field receipt and its preparation marker cannot be committed
            // together. Fail closed if storage violates that atomic contract.
            if (try source_publication.decision(&metadata, item.binding) != null) return error.IndexPublicationRequiresRecovery;
            readers.retire();
            if (readers.drained() == null) continue;
            try txn.put(.meta, &retiredSegmentMetaKey(item.file), &.{});
            try txn.delete(.meta, &fieldPreparationKey(item.file));
            changed = true;
        }
        try txn.commit();
        state.field_preparation_after = if (count == limit) items[count - 1].file else null;
        if (changed) {
            try self.main_store_owner.sync(true);
            self.retirement_reconcile_pending = true;
            self.flushCompletedRetirementMarkersLocked();
        }
        return count == limit or changed;
    }

    pub const SourceFamilyGc = struct { worked: bool = false, more: bool = false, roots: usize = 0, nodes: usize = 0, leaves: usize = 0, remaining_nodes: u64 = 0 };
    fn finishSourceFamilyGcScan(state: *SourceMaintenance, result: *SourceFamilyGc, reached_end: bool) void {
        // Work can make a previously visited entry eligible behind the durable
        // bookmark. Rescan once after a productive pass. A complete blocked
        // pass then sleeps until its registered native reader wakeup arrives.
        if (reached_end) {
            result.more = result.more or state.family_scan_worked;
            state.family_scan_worked = false;
        } else state.family_scan_worked = state.family_scan_worked or result.worked;
        result.more = result.more or result.worked;
    }
    /// Bounded cross-store reclamation. Discover/retire under the index writer,
    /// finish native primary jobs outside it, force primary completion, then
    /// remove graph resource markers. No primary mutation holds an index txn.
    pub fn collectSourceFamilyGarbage(self: *PersistentIndex, primary: anytype, limit: u32) !SourceFamilyGc {
        const state = self.source_maintenance.load(.acquire) orelse return error.IndexSourceOwnerMismatch;
        @import("antfly_platform").sync.lockYielding(&state.mutex);
        defer state.mutex.unlock();
        return self.collectSourceFamilyGarbageLocked(primary, limit);
    }
    fn collectSourceFamilyGarbageLocked(self: *PersistentIndex, primary: anytype, limit: u32) !SourceFamilyGc {
        if (self.read_only) return error.ReadOnly;
        if (limit == 0 or limit > 128) return error.InvalidArgument;
        const state = self.source_maintenance.load(.acquire) orelse return error.IndexSourceOwnerMismatch;
        const Candidate = struct { node: source_publication.Digest, leaf: source_family.Leaf, ready: bool = false };
        var candidates: [128]Candidate = undefined;
        var count: usize = 0;
        var result: SourceFamilyGc = .{ .more = try self.recoverFieldPreparationsLocked(state, limit) };
        var reached_end = false;
        {
            self.lockStorage();
            defer self.unlockStorage();
            var txn = try self.beginWriteMainTxn();
            errdefer txn.abort();
            const owner = (try sourceOwnerRecordInTxn(&txn)) orelse return error.IndexSourceOwnerMismatch;
            if (!owner.families) {
                txn.abort();
                return .{};
            }
            var metadata: PublicationTxn = .{ .txn = &txn };
            // An admitted owner may crash before its first family is staged.
            // Initialize the empty graph under its durable format fence.
            try family_graph.initialize(&metadata, owner.identity);
            var archives: [128]family_graph.Root = undefined;
            const found = try family_graph.retiredWindow(&metadata, owner.identity, state.family_after, archives[0..limit]);
            for (archives[0..found]) |root| {
                const binding: source_publication.Binding = .{ .owner = owner.identity, .publication = root.publication };
                const decision = (try source_publication.decision(&metadata, binding)) orelse return error.IndexPublicationMissing;
                if (decision.state != .retired) continue;
                var readers = try self.openSourceReaders(binding);
                defer readers.deinit();
                readers.watch(state.port());
                readers.retire();
                if (readers.drained()) |drained| {
                    if (try family_graph.releaseRetired(&metadata, owner.identity, decision, drained)) result.roots += 1;
                }
            }
            const Probe = struct {
                writer: *PublicationTxn,
                index: *PersistentIndex,
                state: *SourceMaintenance,
                allocator: Allocator,
                candidates: []Candidate,
                count: *usize,
                pub fn retire(probe: *@This(), owner_id: source_publication.Digest, node: source_publication.Digest, bytes: []const u8) !bool {
                    if (std.mem.startsWith(u8, bytes, "IFD")) {
                        const directory = try field_generation.Directory.decode(bytes);
                        const binding: source_publication.Binding = .{ .owner = owner_id, .publication = directory.publication_id orelse return error.IndexPublicationFenceMismatch };
                        var readers = try probe.index.openSourceReaders(binding);
                        defer readers.deinit();
                        readers.watch(probe.state.port());
                        // A zero-edge root may still be in an off-lock native
                        // preparation. Never seal its publisher out of the CAS.
                        if (readers.hasPreparations()) return false;
                        var decision = (try source_publication.decision(probe.writer, binding)) orelse return error.IndexPublicationMissing;
                        try decision.requireRole(.fields);
                        if (!std.mem.eql(u8, &decision.root, &node)) return error.IndexPublicationFenceMismatch;
                        if (decision.state == .published) {
                            try source_publication.stageRetire(probe.allocator, probe.writer, binding, &.{});
                            decision = (try source_publication.decision(probe.writer, binding)).?;
                        }
                        readers.retire();
                        const drained = readers.drained() orelse return false;
                        try field_generation.completeRetirement(probe.writer, node, decision, drained);
                        const inventory_key = fieldInventoryKey(directory.ordinal_inventory);
                        if (probe.writer.txn.get(.meta, &inventory_key)) |raw| {
                            if (std.meta.eql(try decodeFieldBinding(raw), binding)) try probe.writer.txn.delete(.meta, &inventory_key);
                        } else |err| switch (err) {
                            error.NotFound => {},
                            else => return err,
                        }
                        return true;
                    }
                    if (std.mem.startsWith(u8, bytes, "IFG1")) {
                        return true;
                    }
                    if (std.mem.startsWith(u8, bytes, "IFR1")) {
                        const resource = try field_generation.Resource.decode(bytes);
                        // Resource IDs use the same durable high-water mark as
                        // legacy segment IDs, but are never active logical IDs.
                        // Commit retirement debt with graph removal; a crash or
                        // failed unlink cannot lose ownership of physical work.
                        const active = activeSegmentMetaKey(resource.file);
                        if (probe.writer.txn.get(.meta, &active)) |_| return error.ImmutableGraphCorrupt else |err| switch (err) {
                            error.NotFound => {},
                            else => return err,
                        }
                        try probe.writer.txn.put(.meta, &retiredSegmentMetaKey(resource.file), &.{});
                        probe.index.retirement_reconcile_pending = true;
                        return true;
                    }
                    if (!std.mem.startsWith(u8, bytes, "ISL1")) return false;
                    const leaf = try source_family.Leaf.decode(bytes);
                    const binding: source_publication.Binding = .{ .owner = owner_id, .publication = leaf.publication_id };
                    const actual = (try family_graph.resourceNode(probe.writer, owner_id, leaf.publication_id)) orelse return error.ImmutableGraphCorrupt;
                    if (!std.mem.eql(u8, &actual, &node)) return error.ImmutableGraphCorrupt;
                    const decision = (try source_publication.decision(probe.writer, binding)) orelse return error.IndexPublicationMissing;
                    try decision.requireRole(.source_leaf);
                    if (!std.mem.eql(u8, &decision.root, &leaf.root)) return error.IndexPublicationFenceMismatch;
                    if (decision.state == .published) try source_publication.stageRetire(probe.allocator, probe.writer, binding, &.{});
                    if (decision.state == .cancelled) return error.IndexPublicationFenceMismatch;
                    probe.candidates[probe.count.*] = .{ .node = node, .leaf = leaf };
                    probe.count.* += 1;
                    return false;
                }
            };
            var probe: Probe = .{ .writer = &metadata, .index = self, .state = state, .allocator = self.alloc, .candidates = &candidates, .count = &count };
            const graph = try family_graph.collect(&metadata, owner.identity, limit, &probe);
            result.nodes = graph.reclaimed;
            result.worked = result.roots != 0 or result.nodes != 0;
            reached_end = graph.scanned == 0;
            result.more = found == limit or graph.scanned != 0;
            result.remaining_nodes = (try family_graph.Budget.load(&metadata, owner.identity)).nodes;
            try txn.commit();
            if (found != 0 or graph.scanned != 0) try self.main_store_owner.sync(true);
            if (self.retirement_reconcile_pending) self.flushCompletedRetirementMarkersLocked();
            state.family_after = if (found == limit) archives[found - 1].publication else null;
        }
        for (candidates[0..count]) |candidate| {
            const binding: source_publication.Binding = .{ .owner = state.owner, .publication = candidate.leaf.publication_id };
            const progress = try source_recovery.recoverBinding(self.alloc, primary, self, binding, 64);
            result.worked = result.worked or progress.worked;
            result.more = result.more or progress.worked;
        }
        var ready: usize = 0;
        {
            var read = try primary.beginReadTxn();
            defer read.abort();
            for (candidates[0..count]) |*candidate| {
                const binding: source_publication.Binding = .{ .owner = state.owner, .publication = candidate.leaf.publication_id };
                if (try source_recovery.load(&read, binding) != null or try source_claims.bindingHasClaims(&read, binding)) continue;
                candidate.ready = true;
                ready += 1;
            }
        }
        if (ready == 0) {
            finishSourceFamilyGcScan(state, &result, reached_end);
            return result;
        }
        try primary.sync(true);
        self.lockStorage();
        defer self.unlockStorage();
        var txn = try self.beginWriteMainTxn();
        errdefer txn.abort();
        var metadata: PublicationTxn = .{ .txn = &txn };
        const Completed = struct {
            writer: *PublicationTxn,
            pub fn retire(probe: *@This(), owner: source_publication.Digest, node: source_publication.Digest, bytes: []const u8) !bool {
                const leaf = try source_family.Leaf.decode(bytes);
                try family_graph.unbindResource(probe.writer, owner, leaf.publication_id, node);
                return true;
            }
        };
        var completed: Completed = .{ .writer = &metadata };
        for (candidates[0..count]) |candidate| {
            if (!candidate.ready) continue;
            const binding: source_publication.Binding = .{ .owner = state.owner, .publication = candidate.leaf.publication_id };
            const decision = (try source_publication.decision(&metadata, binding)) orelse return error.IndexPublicationMissing;
            try decision.requireRole(.source_leaf);
            try decision.require(binding, .retired);
            if (try exact_source_map.hasRoot(&metadata, binding)) continue;
            var readers = try self.openSourceReaders(binding);
            defer readers.deinit();
            readers.retire();
            if (readers.drained() == null) continue;
            if (try family_graph.collectLeaf(&metadata, state.owner, candidate.node, &completed)) result.leaves += 1;
        }
        result.remaining_nodes = (try family_graph.Budget.load(&metadata, state.owner)).nodes;
        try txn.commit();
        result.worked = result.worked or result.leaves != 0;
        finishSourceFamilyGcScan(state, &result, reached_end);
        return result;
    }

    /// Replace one fixed source cohort under the current native family. The
    /// expected root is only a CAS token; identity and coordinates come from
    /// the actual held generation. Unchanged postings/leaves are not rehashed,
    /// copied, or re-prepared, and old readers retain their prior family cut.
    pub fn replaceSourceCohort(self: *PersistentIndex, primary: anytype, segment_id: u64, first: u32, expected: source_publication.Digest, prepared: *source_recovery.Prepared, io: std.Io) !source_publication.Decision {
        return self.replaceSourceCohorts(primary, segment_id, expected, &.{.{ .first = first, .prepared = prepared }}, io);
    }

    pub const SourceCohortReplacement = struct { first: u32, prepared: *source_recovery.Prepared };

    /// One durability barrier, writer snapshot and root CAS for a bounded set
    /// of dirty cohorts. Postings remain shared; each changed directory is copied
    /// once. Callers still need a consumer dependency proof before source reuse.
    pub fn replaceSourceCohorts(self: *PersistentIndex, primary: anytype, segment_id: u64, expected: source_publication.Digest, replacements: []const SourceCohortReplacement, io: std.Io) !source_publication.Decision {
        if (@import("schema.zig").maximum_reader_format_version < 28) return error.UnsupportedVersion;
        if (self.read_only) return error.ReadOnly;
        if (replacements.len == 0 or replacements.len > family_graph.maximum_path_replacements) return error.InvalidIndexSourceFamily;
        {
            var read = try primary.beginReadTxn();
            defer read.abort();
            for (replacements, 0..) |item, n| {
                if (item.first % 64 != 0 or item.prepared.job.role != .source_leaf or
                    (n != 0 and replacements[n - 1].first >= item.first)) return error.InvalidIndexSourceFamily;
                try item.prepared.publisher.require(item.prepared.job.binding);
                const observed = (try source_recovery.load(&read, item.prepared.job.binding)) orelse return error.IndexSourcePublicationPending;
                try observed.requirePreparation(item.prepared.job);
            }
        }
        try primary.sync(true);
        var birth = try self.beginNewSourcePublication(io);
        defer birth.publisher.deinit();
        self.lockStorage();
        defer self.unlockStorage();
        const corpus = self.writer.acquireSnapshot();
        defer corpus.release();
        const segment = for (corpus.segments) |*entry| {
            if (entry.id == segment_id) break entry;
        } else return error.InvalidSegment;
        var held = (try segment.sourceLease()) orelse return error.InvalidIndexSourceFamily;
        defer held.deinit();
        const previous = try held.binding();
        var txn = try self.beginWriteMainTxn();
        var txn_open = true;
        errdefer if (txn_open) txn.abort();
        try requireSourceOwnerOpen(&txn);
        const owner = (try sourceOwnerRecordInTxn(&txn)) orelse return error.IndexSourceOwnerMismatch;
        if (!owner.families) return error.IndexSourceOwnerMismatch;
        var metadata: PublicationTxn = .{ .txn = &txn };
        const root = try family_graph.openHeld(&metadata, &held);
        var selections: [family_graph.maximum_path_replacements]source_family.Selection = undefined;
        var already_published = true;
        for (replacements, selections[0..replacements.len]) |item, *selected| {
            const prepared = item.prepared;
            if (!std.mem.eql(u8, &owner.identity, &prepared.job.binding.owner)) return error.IndexSourceOwnerMismatch;
            selected.* = try source_family.select(&metadata, &root, item.first);
            already_published = already_published and std.mem.eql(u8, &selected.leaf.publication_id, &prepared.job.binding.publication) and std.mem.eql(u8, &selected.leaf.root, &prepared.job.root);
            if (selected.layout.segment != segment_id or selected.layout.documents != segment.reader.doc_count or prepared.job.documents != selected.leaf.count or
                !std.mem.eql(u8, &prepared.job.segment, &try selected.layout.identity(item.first))) return error.InvalidIndexSourceFamily;
        }
        if (!std.mem.eql(u8, &root.id, &expected)) {
            if (!already_published) return error.IndexPublicationConflict;
            txn.abort();
            txn_open = false;
            return (try self.durableSourceDecisionLocked(previous)).?;
        }
        var readers = try self.openSourceReaders(birth.binding);
        defer readers.deinit();
        var staged = try self.writer.prepareSourceGeneration(segment_id, previous, try readers.register());
        defer staged.abort();
        var paths: [family_graph.maximum_path_replacements]family_graph.PathReplacement = undefined;
        for (replacements, selections[0..replacements.len], paths[0..replacements.len]) |item, *selected, *path| {
            const prepared = item.prepared;
            try self.validateSourceCohortReferences(segment, prepared, item.first);
            const plan = try exact_source_map.Plan.prepare(self.alloc, prepared.job.binding, prepared.job.segment, prepared.references);
            defer plan.deinit(self.alloc);
            if (!std.mem.eql(u8, &plan.root(), &prepared.job.root)) return error.IndexPublicationConflict;
            _ = try source_publication.stagePublishRoleInEpoch(self.alloc, &metadata, prepared.job.binding, prepared.job.root, &.{}, try owner.admitPublication(prepared.job.binding), .source_leaf);
            try plan.stage(&metadata);
            const leaf = try source_family.stageLeaf(&metadata, selected.layout, (try source_publication.decision(&metadata, prepared.job.binding)).?, item.first, .{});
            path.* = .{ .path = selected.path[0..selected.path_length], .replacement = leaf };
        }
        // A changed source cut invalidates transfer retry provenance. The
        // cleared marker shares all unchanged children and has no history key.
        var next_layout = selections[0].layout;
        next_layout.transfer = null;
        var next_metadata: [source_family.maximum_layout_bytes]u8 = undefined;
        const next = try family_graph.replacePathsWithMetadata(&metadata, &root, paths[0..replacements.len], try next_layout.metadata(&next_metadata), .{});
        _ = try source_publication.stagePublishRoleInEpoch(self.alloc, &metadata, birth.binding, next, &.{segment_id}, try owner.admitPublication(birth.binding), .family);
        const slot = source_family.slot(segment_id);
        const before = (try family_graph.loadRoot(&metadata, owner.identity, slot)) orelse return error.InvalidIndexSourceFamily;
        if (!std.mem.eql(u8, &before.node, &expected) or !std.mem.eql(u8, &before.publication, &previous.publication)) return error.IndexPublicationConflict;
        if (!try family_graph.compareAndSwap(&metadata, owner.identity, slot, before, birth.binding.publication, next, .{})) return error.IndexPublicationConflict;
        if (!std.meta.eql(try sourceSegmentBinding(&txn, segment_id), previous)) return error.IndexPublicationConflict;
        try stageSourceSegment(&txn, segment_id, birth.binding);
        try source_publication.stageRetire(self.alloc, &metadata, previous, &.{});
        try txn.commit();
        txn_open = false;
        staged.publish();
        self.noteSourceRecoveryWork();
        return (try self.durableSourceDecisionLocked(birth.binding)).?;
    }

    fn validateSourceCohortReferences(self: *PersistentIndex, segment: *const index_mod.SegmentEntry, item: *const source_recovery.Prepared, first: u32) !void {
        if (item.references.len != item.job.documents) return error.InvalidIndexSourceFamily;
        for (item.references, 0..) |ref, offset| {
            const id = (try segment.reader.storedIdAlloc(self.alloc, first + @as(u32, @intCast(offset)))) orelse return error.InvalidIndexSourceMap;
            defer self.alloc.free(id);
            const physical = try @import("internal_keys.zig").documentKeyAlloc(self.alloc, id);
            defer self.alloc.free(physical);
            const relational = try @import("internal_keys.zig").relationalRowKeyAlloc(self.alloc, id);
            defer self.alloc.free(relational);
            var document_digest: [32]u8 = undefined;
            var row_digest: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(physical, &document_digest, .{});
            std.crypto.hash.sha2.Sha256.hash(relational, &row_digest, .{});
            if (!std.mem.eql(u8, &ref.document, &document_digest) and !std.mem.eql(u8, &ref.document, &row_digest)) return error.InvalidIndexSourceMap;
        }
    }

    /// Admit a first source-family generation over an existing immutable
    /// physical segment. Primary preparation is durable before the index CAS;
    /// every fallible snapshot allocation precedes the catalog commit.
    /// This native qualification path remains fenced from production writers.
    pub fn publishSourceFamily(self: *PersistentIndex, primary: anytype, layout: source_family.Layout, prepared: []source_recovery.Prepared, io: std.Io) !source_publication.Decision {
        if (@import("schema.zig").maximum_reader_format_version < 28) return error.UnsupportedVersion;
        if (self.read_only) return error.ReadOnly;
        if (prepared.len != (layout.documents -| 1) / 64 + 1 or prepared.len == 0) return error.InvalidIndexSourceFamily;
        const owner = prepared[0].job.binding.owner;
        {
            var read = try primary.beginReadTxn();
            defer read.abort();
            for (prepared, 0..) |*item, n| {
                const first: u32 = @intCast(n * 64);
                try item.publisher.require(item.job.binding);
                if (item.job.role != .source_leaf or item.job.documents != try layout.count(first) or
                    !std.mem.eql(u8, &owner, &item.job.binding.owner) or !std.mem.eql(u8, &item.job.segment, &try layout.identity(first))) return error.InvalidIndexSourceFamily;
                try ((try source_recovery.load(&read, item.job.binding)) orelse return error.IndexSourcePublicationPending).requirePreparation(item.job);
            }
        }
        try primary.sync(true);
        var birth = try self.beginNewSourcePublication(io);
        defer birth.publisher.deinit();
        self.lockStorage();
        defer self.unlockStorage();
        const corpus = self.writer.acquireSnapshot();
        defer corpus.release();
        const segment = for (corpus.segments) |*entry| {
            if (entry.id == layout.segment) break entry;
        } else return error.InvalidSegment;
        if (segment.reader.doc_count != layout.documents) return error.InvalidIndexSourceFamily;
        var previous_lease = try segment.sourceLease();
        defer if (previous_lease) |*lease| lease.deinit();
        const previous = if (previous_lease) |*lease| try lease.binding() else null;
        {
            var read = try self.beginReadMainTxn();
            defer read.abort();
            var metadata: PublicationTxn = .{ .txn = &read };
            if (try family_graph.loadRoot(&metadata, owner, source_family.slot(layout.segment))) |current| {
                const lease = if (previous_lease) |*held| held else return error.IndexPublicationConflict;
                const held = try family_graph.openHeld(&metadata, lease);
                if (!std.mem.eql(u8, &current.node, &held.id) or !std.mem.eql(u8, &current.publication, &previous.?.publication)) return error.IndexPublicationConflict;
                for (prepared, 0..) |*item, n| {
                    const selected = try source_family.select(&metadata, &held, @intCast(n * 64));
                    if (!std.meta.eql(selected.layout, layout) or !std.mem.eql(u8, &selected.leaf.publication_id, &item.job.binding.publication) or
                        !std.mem.eql(u8, &selected.leaf.root, &item.job.root)) return error.IndexPublicationConflict;
                }
                read.abort();
                return (try self.durableSourceDecisionLocked(previous.?)).?;
            }
        }
        // Hash the retained artifact once for initial migration. Cohort updates
        // and retries inherit its identity from the actual held family.
        const stamp = try @import("../segment_source.zig").ContentStamp.capture(segment.data.source());
        if (!std.mem.eql(u8, &stamp.digest, &layout.physical)) return error.InvalidIndexSourceFamily;
        var readers = try self.openSourceReaders(birth.binding);
        defer readers.deinit();
        var staged = try self.writer.prepareSourceGeneration(layout.segment, previous, try readers.register());
        defer staged.abort();
        var txn = try self.beginWriteMainTxn();
        errdefer txn.abort();
        try requireSourceOwnerOpen(&txn);
        const owner_record = (try sourceOwnerRecordInTxn(&txn)) orelse return error.IndexSourceOwnerMismatch;
        if (!owner_record.families or !std.mem.eql(u8, &owner_record.identity, &owner)) return error.IndexSourceOwnerMismatch;
        var metadata: PublicationTxn = .{ .txn = &txn };
        const slot = source_family.slot(layout.segment);
        if (try family_graph.loadRoot(&metadata, owner, slot) != null) return error.IndexPublicationConflict;
        try family_graph.initialize(&metadata, owner);
        var builder = try source_family.Builder.init(owner, layout);
        for (prepared, 0..) |*item, n| {
            const first: u32 = @intCast(n * 64);
            try self.validateSourceCohortReferences(segment, item, first);
            const plan = try exact_source_map.Plan.prepare(self.alloc, item.job.binding, item.job.segment, item.references);
            defer plan.deinit(self.alloc);
            if (!std.mem.eql(u8, &plan.root(), &item.job.root)) return error.IndexPublicationConflict;
            _ = try source_publication.stagePublishRoleInEpoch(self.alloc, &metadata, item.job.binding, item.job.root, &.{}, try owner_record.admitPublication(item.job.binding), .source_leaf);
            try plan.stage(&metadata);
            try builder.append(&metadata, (try source_publication.decision(&metadata, item.job.binding)).?, .{});
        }
        const root = try builder.finish(&metadata, .{});
        _ = try source_publication.stagePublishRoleInEpoch(self.alloc, &metadata, birth.binding, root, &.{layout.segment}, try owner_record.admitPublication(birth.binding), .family);
        if (!try family_graph.compareAndSwap(&metadata, owner, slot, null, birth.binding.publication, root, .{})) return error.IndexPublicationConflict;
        if (!std.meta.eql(try sourceSegmentBinding(&txn, layout.segment), previous)) return error.IndexPublicationConflict;
        try stageSourceSegment(&txn, layout.segment, birth.binding);
        if (previous) |old| {
            const old_record = (try source_publication.load(&metadata, old)) orelse return error.InvalidIndexPublication;
            if (old_record.role != .segment) return error.IndexPublicationFenceMismatch;
            // A legacy root can cover several physical segments. Retire only
            // after none of its original catalog bindings remains current.
            const still_current = for (old_record.segments[0..old_record.count]) |id| {
                if (try sourceSegmentBinding(&txn, id)) |bound| if (std.meta.eql(bound, old)) break true;
            } else false;
            if (!still_current) try source_publication.stageRetire(self.alloc, &metadata, old, &.{});
        }
        try txn.commit();
        staged.publish();
        self.source_roots_present = true;
        self.noteSourceRecoveryWork();
        return (try self.durableSourceDecisionLocked(birth.binding)).?;
    }

    /// Capture from a held index segment BEFORE opening the primary read view.
    /// Lookup reads one authenticated source-map page, never the latest row.
    /// Retired roots remain readable through their original snapshot leases.
    pub fn captureSource(self: *PersistentIndex, segment: *const index_mod.SegmentEntry, document: u32) !?CapturedSource {
        var lease = (try segment.sourceLease()) orelse return null;
        errdefer lease.deinit();
        self.lockStorage();
        defer self.unlockStorage();
        var txn = try self.beginReadMainTxn();
        defer txn.abort();
        const binding = try lease.binding();
        var metadata: PublicationTxn = .{ .txn = &txn };
        const decision = try source_publication.heldReaderDecision(&metadata, binding, &lease.inner);
        if (decision.role == .family) {
            const held = try family_graph.openHeld(&metadata, &lease);
            const selected = try source_family.select(&metadata, &held, document);
            if (selected.layout.segment != segment.id or selected.layout.documents != segment.reader.doc_count) return error.InvalidIndexSourceFamily;
            const child_binding: source_publication.Binding = .{ .owner = binding.owner, .publication = selected.leaf.publication_id };
            var readers = try self.openSourceReaders(child_binding);
            defer readers.deinit();
            var child = try readers.lease();
            errdefer child.deinit();
            const child_decision = try source_publication.heldReaderDecision(&metadata, child_binding, &child.inner);
            try child_decision.requireRole(.source_leaf);
            try child_decision.require(child_binding, .published);
            if (!std.mem.eql(u8, &child_decision.root, &selected.leaf.root)) return error.IndexPublicationFenceMismatch;
            const ref = try exact_source_map.lookup(&metadata, child_binding, child_decision.root, document - selected.leaf.first);
            lease.deinit();
            return .{ .reference = ref, .decision = child_decision, .lease = child };
        }
        try decision.requireRole(.segment);
        const ref = try exact_source_map.lookup(&metadata, binding, decision.root, document);
        return .{ .reference = ref, .decision = decision, .lease = lease };
    }

    fn sourcePublicationRoot(self: *PersistentIndex, segment: *const index_mod.SegmentEntry) !?source_publication.Digest {
        var lease = (try segment.sourceLease()) orelse return null;
        defer lease.deinit();
        self.lockStorage();
        defer self.unlockStorage();
        var txn = try self.beginReadMainTxn();
        defer txn.abort();
        var metadata: PublicationTxn = .{ .txn = &txn };
        const binding = try lease.binding();
        const decision = try source_publication.heldReaderDecision(&metadata, binding, &lease.inner);
        if (decision.role != .segment and decision.role != .family) return error.IndexPublicationFenceMismatch;
        return decision.root;
    }

    pub const CapturedSourceReferences = struct {
        allocator: Allocator,
        decision: source_publication.Decision,
        lease: source_publication.SharedReaders.Lease,
        view: exact_source_map.ReadView,
        first_document: u32 = 0,
        family_root: ?source_publication.Digest = null,
        pub fn deinit(self: *CapturedSourceReferences) void {
            self.view.deinit(self.allocator);
            self.lease.deinit();
            self.* = undefined;
        }
        pub fn readWindow(self: *const CapturedSourceReferences, owner: *PersistentIndex, first: u32, refs: []source_claims.Reference) !void {
            return self.readWindowInto(owner, first, refs);
        }
        pub fn readCompactWindow(self: *const CapturedSourceReferences, owner: *PersistentIndex, first: u32, refs: []source_claims.SnapshotEntry) !void {
            return self.readWindowInto(owner, first, refs);
        }
        fn readWindowInto(self: *const CapturedSourceReferences, owner: *PersistentIndex, first: u32, refs: anytype) !void {
            try self.lease.inner.require(self.decision.binding);
            owner.lockStorage();
            defer owner.unlockStorage();
            var read = try owner.beginReadMainTxn();
            defer read.abort();
            var metadata: PublicationTxn = .{ .txn = &read };
            if (comptime @typeInfo(@TypeOf(refs)).pointer.child == source_claims.Reference)
                try self.view.readWindow(&metadata, first, refs)
            else
                try self.view.readCompactWindow(&metadata, first, refs);
        }
    };

    pub const CapturedSourceCohorts = struct {
        allocator: Allocator,
        items: []CapturedSourceReferences,
        pub fn deinit(self: *@This()) void {
            for (self.items) |*item| item.deinit();
            self.allocator.free(self.items);
            self.* = undefined;
        }
    };
    /// Structural handoff retains stable leaf lease addresses until claim
    /// preparation finishes. Legacy whole-map roots still require one slot.
    pub fn captureSourceCohorts(self: *PersistentIndex, a: Allocator, segment: *const index_mod.SegmentEntry) !?CapturedSourceCohorts {
        var first = (try self.captureSourceReferencesAt(a, segment, 0)) orelse return null;
        var first_owned = true;
        errdefer if (first_owned) first.deinit();
        const count: usize = if (first.family_root == null) 1 else (segment.reader.doc_count - 1) / 64 + 1;
        const items = try a.alloc(CapturedSourceReferences, count);
        var initialized: usize = 1;
        items[0] = first;
        first_owned = false;
        errdefer {
            for (items[0..initialized]) |*item| item.deinit();
            a.free(items);
        }
        for (items[1..], 1..) |*item, n| {
            item.* = (try self.captureSourceReferencesAt(a, segment, @intCast(n * 64))) orelse return error.InvalidIndexSourceMap;
            initialized += 1;
        }
        return .{ .allocator = a, .items = items };
    }

    pub fn captureSourceReferences(self: *PersistentIndex, a: Allocator, segment: *const index_mod.SegmentEntry) !?CapturedSourceReferences {
        var result = (try self.captureSourceReferencesAt(a, segment, 0)) orelse return null;
        errdefer result.deinit();
        if (result.view.documents != segment.reader.doc_count) return error.SourcePublicationRequiresHandoff;
        return result;
    }

    pub fn captureSourceReferencesAt(self: *PersistentIndex, a: Allocator, segment: *const index_mod.SegmentEntry, document: u32) !?CapturedSourceReferences {
        if (document >= segment.reader.doc_count) return error.InvalidArgument;
        var lease = (try segment.sourceLease()) orelse return null;
        errdefer lease.deinit();
        self.lockStorage();
        defer self.unlockStorage();
        var read = try self.beginReadMainTxn();
        defer read.abort();
        const binding = try lease.binding();
        var metadata: PublicationTxn = .{ .txn = &read };
        const decision = try source_publication.heldReaderDecision(&metadata, binding, &lease.inner);
        if (decision.role == .family) {
            const held = try family_graph.openHeld(&metadata, &lease);
            const selected = try source_family.select(&metadata, &held, document);
            if (selected.layout.segment != segment.id or selected.layout.documents != segment.reader.doc_count) return error.InvalidIndexSourceFamily;
            const child_binding: source_publication.Binding = .{ .owner = binding.owner, .publication = selected.leaf.publication_id };
            var readers = try self.openSourceReaders(child_binding);
            defer readers.deinit();
            var child = try readers.lease();
            errdefer child.deinit();
            const child_decision = try source_publication.heldReaderDecision(&metadata, child_binding, &child.inner);
            try child_decision.requireRole(.source_leaf);
            try child_decision.require(child_binding, .published);
            if (!std.mem.eql(u8, &child_decision.root, &selected.leaf.root)) return error.IndexPublicationFenceMismatch;
            const summary = try exact_source_map.summary(&metadata, child_binding, child_decision.root);
            if (summary.documents != selected.leaf.count or !std.mem.eql(u8, &summary.identity, &try selected.layout.identity(selected.leaf.first))) return error.InvalidIndexSourceFamily;
            const view = try exact_source_map.ReadView.open(a, &metadata, child_binding, child_decision.root);
            lease.deinit();
            return .{ .allocator = a, .decision = child_decision, .lease = child, .view = view, .first_document = selected.leaf.first, .family_root = decision.root };
        }
        try decision.requireRole(.segment);
        const view = try exact_source_map.ReadView.open(a, &metadata, binding, decision.root);
        errdefer view.deinit(a);
        if (view.documents != segment.reader.doc_count) return error.InvalidIndexSourceMap;
        return .{ .allocator = a, .decision = decision, .lease = lease, .view = view };
    }

    /// Query-local source view. The held corpus pins its source roots before
    /// primary access opens. Immutable claim descendants use native checked
    /// value leases; legacy reads retain a stable primary snapshot. Directories
    /// load lazily into two slots and selected rows reuse authenticated pages.
    /// Both database and index owners outlive this internal frame.
    pub fn SourceReadFrame(comptime Primary: type) type {
        const PrimaryRead = @typeInfo(@TypeOf(@as(Primary, undefined).beginReadTxn())).error_union.payload;
        const PrimaryOwner = switch (@typeInfo(Primary)) {
            .pointer => |pointer| pointer.child,
            else => Primary,
        };
        const can_probe = if (@hasDecl(PrimaryOwner, "beginProbeTxn"))
            @typeInfo(@TypeOf(@as(Primary, undefined).beginProbeTxn())).error_union.payload == PrimaryRead
        else
            false;
        const ImmutableRead = PrimaryRead;
        return struct {
            allocator: Allocator,
            owner: *PersistentIndex,
            snapshot: ?*index_mod.IndexSnapshot,
            primary_owner: Primary,
            primary: ?PrimaryRead = null,
            immutable_primary: ?ImmutableRead = null,
            selected_rows: usize = 0,
            capture_complete: bool = false,
            slots: [2]Slot = .{ .{}, .{} },
            next_slot: usize = 0,
            const Slot = struct {
                segment: ?usize = null,
                source: ?CapturedSourceReferences = null,
                first: u32 = 0,
                count: usize = 0,
                references: [exact_source_map.page_capacity]source_claims.SnapshotEntry = undefined,
                fn clear(self: *Slot) void {
                    if (self.source) |*source| source.deinit();
                    self.* = .{};
                }
            };
            pub fn deinit(self: *@This()) void {
                self.clearInventory();
                if (self.primary) |*read| read.abort();
                if (self.immutable_primary) |*read| read.abort();
                self.* = undefined;
            }
            fn clearInventory(self: *@This()) void {
                for (&self.slots) |*slot| slot.clear();
                if (self.snapshot) |corpus| corpus.release();
                self.snapshot = null;
            }
            /// Exported selected roots retain their own pins. Drop the complete
            /// physical corpus after ranking, keeping only its primary view.
            /// Revocable provider facades retain their guard through deinit.
            pub fn finishCapture(self: *@This()) void {
                self.capture_complete = true;
                const corpus = self.snapshot orelse return;
                if (corpus.read_context == null and corpus.read_context_check == null and corpus.text_statistics == null) self.clearInventory();
            }
            fn checkContext(self: *const @This()) !void {
                // Physical local snapshots have no revocable query context.
                // A facade must recheck its native authority before even a
                // directory/cache identity can be consumed.
                const corpus = self.snapshot orelse return;
                if (corpus.read_context != null or corpus.read_context_check != null or corpus.text_statistics != null) try corpus.checkScoringReadContext();
            }
            /// Source membership may be answered entirely by immutable index
            /// metadata/memos. Pin the primary only when a source read is owed.
            pub fn readView(self: *@This()) !*PrimaryRead {
                try self.checkContext();
                if (self.primary == null) self.primary = try self.primary_owner.beginReadTxn();
                return &self.primary.?;
            }
            /// Only use through CapturedSource.immutableReader, which checks
            /// the actual source lease on every read and rejects primary rows.
            /// Legacy delivery retains readView's stable primary snapshot.
            pub fn immutableReadView(self: *@This()) !*ImmutableRead {
                try self.checkContext();
                if (self.immutable_primary == null) {
                    // One authenticated reference page bounds retained point
                    // loans. Bulk delivery amortizes the stable snapshot and
                    // avoids accumulating one borrowed value per lookup.
                    const bounded = self.capture_complete and self.selected_rows <= exact_source_map.page_capacity;
                    const backend_prefers = if (comptime @hasDecl(PrimaryOwner, "prefersImmutablePointReads"))
                        bounded and self.primary_owner.prefersImmutablePointReads(self.selected_rows * 2)
                    else
                        false;
                    if (builtin.is_test) test_source_point_eligible.store(backend_prefers, .monotonic);
                    const point_loans = backend_prefers and !(builtin.is_test and test_force_source_snapshot_reads);
                    self.immutable_primary = if (comptime can_probe) choose: {
                        if (point_loans) {
                            if (builtin.is_test) _ = test_source_probe_reads.fetchAdd(1, .monotonic);
                            break :choose try self.primary_owner.beginProbeTxn();
                        }
                        break :choose try self.primary_owner.beginReadTxn();
                    } else try self.primary_owner.beginReadTxn();
                }
                return &self.immutable_primary.?;
            }

            /// Native coordinates belong to this frame's exact corpus. A
            /// legacy segment returns null so its complete-source fallback can
            /// use this same primary cut; absence never selects a current row
            /// for a segment that promises indexed sources.
            fn slotFor(self: *@This(), segment: usize, document: u32) !?*Slot {
                try self.checkContext();
                const corpus = self.snapshot orelse return error.Canceled;
                if (segment >= corpus.segments.len) return error.InvalidArgument;
                const entry = &corpus.segments[segment];
                if (!entry.hasSourceReferences()) return null;
                return for (&self.slots) |*slot| {
                    if (slot.segment == segment and document >= slot.source.?.first_document and document - slot.source.?.first_document < slot.source.?.view.documents) break slot;
                } else fresh: {
                    const slot = &self.slots[self.next_slot];
                    self.next_slot = (self.next_slot + 1) % self.slots.len;
                    slot.clear();
                    slot.source = (try self.owner.captureSourceReferencesAt(self.allocator, entry, document)) orelse return error.InvalidIndexSourceMap;
                    slot.segment = segment;
                    break :fresh slot;
                };
            }
            pub fn publicationRootAt(self: *@This(), segment: usize) !?source_publication.Digest {
                try self.checkContext();
                const corpus = self.snapshot orelse return error.Canceled;
                if (segment >= corpus.segments.len) return error.InvalidArgument;
                for (&self.slots) |*slot| if (slot.segment == segment) return slot.source.?.family_root orelse slot.source.?.decision.root;
                // A root identity needs no source map allocation, leaf lease,
                // or cache eviction. Pin that source only if a row is selected.
                return self.owner.sourcePublicationRoot(&corpus.segments[segment]);
            }
            pub fn sourceCohortAt(self: *@This(), segment: usize, document: u32) !?struct { first: u32, count: u32 } {
                const slot = (try self.slotFor(segment, document)) orelse return null;
                return .{ .first = slot.source.?.first_document, .count = slot.source.?.view.documents };
            }
            fn referenceAt(self: *@This(), segment: usize, document: u32) !?struct { slot: *Slot, reference: source_claims.Reference } {
                const corpus = self.snapshot orelse return error.Canceled;
                if (segment >= corpus.segments.len or document >= corpus.segments[segment].reader.doc_count) return error.InvalidArgument;
                const cached = (try self.slotFor(segment, document)) orelse return null;
                const source = &cached.source.?;
                if (cached.count == 0 or document < cached.first or document - cached.first >= cached.count) {
                    const first = document / exact_source_map.page_capacity * exact_source_map.page_capacity;
                    const count = @min(exact_source_map.page_capacity, source.view.documents - (first - source.first_document));
                    // A failed page read must not expose partially replaced
                    // coordinates as a previously authenticated cache window.
                    cached.count = 0;
                    try source.readCompactWindow(self.owner, first - source.first_document, cached.references[0..count]);
                    cached.first = first;
                    cached.count = count;
                }
                return .{ .slot = cached, .reference = cached.references[document - cached.first].reference(source.decision.binding) };
            }
            /// One native root pin covers a bounded selected-row cohort. No
            /// per-document ownership transaction or index-body read is needed.
            pub fn captureReferencesAt(self: *@This(), segment: usize, documents: []const u32, references: []source_claims.Reference) !?source_claims.CapturedRoot {
                if (documents.len == 0 or documents.len > 64 or documents.len != references.len) return error.InvalidArgument;
                const slot = (try self.slotFor(segment, documents[0])) orelse return null;
                const source = &slot.source.?;
                for (documents) |document| if (document < source.first_document or document - source.first_document >= source.view.documents) return error.InvalidIndexSourceMap;
                var lease = try source.lease.clone();
                errdefer lease.deinit();
                const decision = source.decision;
                for (documents, references) |document, *reference| reference.* = (try self.referenceAt(segment, document)).?.reference;
                self.selected_rows +|= documents.len;
                return .{ .decision = decision, .lease = lease };
            }
            pub fn identityAt(self: *@This(), segment: usize, document: u32) !?source_claims.Digest {
                const located = (try self.referenceAt(segment, document)) orelse return null;
                return located.reference.snapshot;
            }
            pub fn captureAt(self: *@This(), segment: usize, document: u32) !?CapturedSource {
                const located = (try self.referenceAt(segment, document)) orelse return null;
                const source = &located.slot.source.?;
                return .{ .reference = located.reference, .decision = source.decision, .lease = try source.lease.clone() };
            }
        };
    }

    /// Borrowed native database/index owners must remain open through deinit.
    /// This internal frame never escapes into public SearchResult ownership.
    pub fn openSourceReadFrame(self: *PersistentIndex, a: Allocator, primary: anytype, snapshot_ref: *index_mod.IndexSnapshot) !?SourceReadFrame(@TypeOf(primary)) {
        if (!snapshot_ref.hasSourceReferences()) return null;
        const held = snapshot_ref.retain();
        errdefer held.release();
        return .{ .allocator = a, .owner = self, .snapshot = held, .primary_owner = primary };
    }

    pub const SourceRetirement = struct {
        decision: source_publication.Decision,
        readers: source_publication.SharedReaders.Handle,
        pub fn deinit(self: *SourceRetirement) void {
            self.readers.deinit();
        }
        pub fn drained(self: SourceRetirement) ?source_publication.ReaderPins.Drained {
            return self.readers.drained();
        }
    };

    pub fn retireSourcePublicationWithReaders(self: *PersistentIndex, binding: source_publication.Binding) !SourceRetirement {
        // Keep this cell live across the durable fence. Reopeners and old
        // snapshots join it, so sealing cannot overlook older generations.
        var readers = try self.openSourceReaders(binding);
        errdefer readers.deinit();
        const decision = try self.retireSourcePublication(binding);
        readers.retire();
        if (self.source_maintenance.load(.acquire)) |state| readers.watch(state.port());
        return .{ .decision = decision, .readers = readers };
    }

    pub fn reclaimSourceMapWindow(self: *PersistentIndex, retired: *const SourceRetirement, limit: u32) !exact_source_map.ReclaimProgress {
        if (self.read_only) return error.ReadOnly;
        const proof = retired.drained() orelse return error.IndexSourceReadersActive;
        try source_publication.ReaderPins.requireDrained(proof, retired.decision.binding);
        try retired.decision.require(retired.decision.binding, .retired);
        self.lockStorage();
        defer self.unlockStorage();
        var txn = try self.beginWriteMainTxn();
        errdefer txn.abort();
        var metadata: PublicationTxn = .{ .txn = &txn };
        const record = (try source_publication.load(&metadata, retired.decision.binding)) orelse return error.IndexPublicationMissing;
        if (record.state != .retired or !std.mem.eql(u8, &record.root, &retired.decision.root)) return error.IndexPublicationFenceMismatch;
        const progress = try exact_source_map.reclaimPages(&metadata, retired.decision.binding, retired.decision.root, limit);
        try txn.commit();
        return progress;
    }

    /// Legacy structural writers cannot silently drop an exact source map.
    /// Their coordinator must first fork source claims and publish a new map,
    /// or materialize a verified complete-source replacement. Ordinary roots
    /// take one false branch and issue no additional metadata probes.
    fn requireSourceHandoff(self: *PersistentIndex, txn: *MainTxn, ids: []const u64) !void {
        if (!self.source_roots_present) return;
        for (ids) |id| {
            _ = txn.get(.meta, &sourceSegmentKey(id)) catch |err| switch (err) {
                error.NotFound => continue,
                else => return err,
            };
            return error.SourcePublicationRequiresHandoff;
        }
    }

    /// Build and publish one segment directly into the persistent segment
    /// artifact when native segment files are available.
    ///
    /// Backends with owned native files map the final artifact. Lite transfers
    /// an independent staging-inode lease. Providers without either capability
    /// fall back to heap-backed `indexSegmentOwned`.
    pub fn indexSegmentFromSinkBuilder(
        self: *PersistentIndex,
        ctx: *anyopaque,
        build_fn: SegmentSinkBuildFn,
    ) !usize {
        if (builtin.os.tag == .freestanding or !self.supportsFileBackedSegmentArtifacts()) {
            var sink_impl = segment_mod.MemorySegmentSink.init(self.alloc);
            errdefer sink_impl.deinit();
            var sink = sink_impl.sink();
            try build_fn(ctx, &sink);
            const segment_len = sink.len();
            const owned = try sink_impl.finishOwned();
            try self.indexSegmentOwned(owned);
            return segment_len;
        }

        const profile_enabled = benchPersistentPublishEnabled();
        const total_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
        var reserve_id_ns: u64 = 0;
        var materialize_ns: u64 = 0;
        var begin_write_ns: u64 = 0;
        var build_sink_ns: u64 = 0;
        var finish_write_ns: u64 = 0;
        var map_segment_ns: u64 = 0;
        var key_range_ns: u64 = 0;
        var persist_ns: u64 = 0;
        var writer_publish_ns: u64 = 0;

        const reserve_id_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
        const seg_id = self.reserveSegmentId();
        if (profile_enabled) reserve_id_ns = platform_time.monotonicNs() - reserve_id_start_ns;
        var rollback_segment = true;
        errdefer if (rollback_segment) self.deleteSegmentFile(seg_id);

        const store = &self.segment_files.?;
        var publication_admission: ?NativeSegmentPublicationAdmission = if (store.storage_owner) |owner|
            try NativeSegmentPublicationAdmission.init(owner)
        else
            null;
        defer if (publication_admission) |*admission| admission.deinit();
        const path = try store.pathAlloc(seg_id);
        defer store.allocator.free(path);

        const materialize_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
        const begin_write_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
        var writer = if (publication_admission) |*admission|
            try admission.beginAtomicWrite(self.alloc, path)
        else
            try store.storage.beginAtomicWrite(self.alloc, path);
        if (profile_enabled) begin_write_ns = platform_time.monotonicNs() - begin_write_start_ns;
        var writer_active = true;
        errdefer if (writer_active) writer.abort();

        if (writer.vtable.finish_source != null or writer.vtable.finish_mapped != null) writer.setCacheIntent(.cold_sequential);
        var sink_adapter = AtomicSegmentSink.init(self.alloc, &writer);
        defer sink_adapter.deinit();
        var sink = sink_adapter.sink();
        const build_sink_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
        try build_fn(ctx, &sink);
        const segment_len = sink.len();
        try sink_adapter.flush();
        if (profile_enabled) build_sink_ns = platform_time.monotonicNs() - build_sink_start_ns;

        writer_active = false;
        const finish_write_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
        const finished_source = if (writer.vtable.finish_source != null) try writer.finishSource() else null;
        const finished_mapping = if (finished_source != null) null else if (writer.vtable.finish_mapped != null) try writer.finishMapped() else blk: {
            try writer.finish();
            break :blk null;
        };
        if (profile_enabled) finish_write_ns = platform_time.monotonicNs() - finish_write_start_ns;
        if (profile_enabled) materialize_ns = platform_time.monotonicNs() - materialize_start_ns;

        const map_segment_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
        var segment_data: ?index_mod.SegmentData = if (finished_source) |source|
            .fromNative(source)
        else if (finished_mapping) |artifact|
            .fromMappedArtifact(artifact)
        else if (publication_admission) |*admission|
            .fromMapped(try admission.mapFile(path))
        else
            try store.openPublishedSource(path);
        segment_data.?.madviseAccessPattern();
        if (profile_enabled) map_segment_ns = platform_time.monotonicNs() - map_segment_start_ns;
        errdefer {
            if (segment_data) |*data| data.deinit(self.alloc);
        }

        const key_range_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
        var key_range = extractSegmentDataKeyRange(self.alloc, segment_data.?) catch |err| {
            if (builtin.os.tag != .freestanding) std.log.err("text segment key-range scan failed: {s}", .{@errorName(err)});
            return err;
        };
        if (profile_enabled) key_range_ns = platform_time.monotonicNs() - key_range_start_ns;
        defer key_range.deinit(self.alloc);

        if (self.active_rebuild_page) |page| {
            try page.segments.append(self.alloc, .{ .id = seg_id, .data = segment_data.? });
            segment_data = null;
            rollback_segment = false;
            return segment_len;
        }

        var replacement = [_]index_mod.ReplacementSegmentData{.{
            .id = seg_id,
            .data = segment_data.?,
        }};
        var writer_publication = try self.writer.prepareSegmentsManyData(&.{}, &replacement);
        defer writer_publication.abort();

        const persist_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
        self.lockStorage();
        defer self.unlockStorage();
        var txn = self.beginWriteMainTxn() catch |err| {
            if (builtin.os.tag != .freestanding) std.log.err("text segment catalog transaction failed: {s}", .{@errorName(err)});
            return err;
        };
        errdefer txn.abort();
        self.saveSegmentRange(&txn, seg_id, key_range) catch |err| {
            if (builtin.os.tag != .freestanding) std.log.err("text segment key-range publish failed: {s}", .{@errorName(err)});
            return err;
        };
        self.updateActiveSegments(&txn, seg_id, .add) catch |err| {
            if (builtin.os.tag != .freestanding) std.log.err("text segment active-set publish failed: {s}", .{@errorName(err)});
            return err;
        };
        const lsn_bytes = std.mem.toBytes(std.mem.nativeToLittle(u64, self.committed_lsn));
        txn.put(.meta, meta_committed_lsn, &lsn_bytes) catch |err| {
            if (builtin.os.tag != .freestanding) std.log.err("text segment checkpoint publish failed: {s}", .{@errorName(err)});
            return err;
        };
        txn.commit() catch |err| {
            if (builtin.os.tag != .freestanding) std.log.err("text segment catalog commit failed: {s}", .{@errorName(err)});
            return err;
        };
        if (profile_enabled) persist_ns = platform_time.monotonicNs() - persist_start_ns;

        const writer_publish_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
        writer_publication.publish();
        segment_data = null;
        rollback_segment = false;
        if (profile_enabled) writer_publish_ns = platform_time.monotonicNs() - writer_publish_start_ns;

        if (profile_enabled) {
            std.log.info(
                "antfly_bench_text_publish seg_id={d} bytes={d} total_ms={d} wal_append_ms={d} reserve_id_ms={d} materialize_ms={d} begin_write_ms={d} build_sink_ms={d} finish_write_ms={d} map_segment_ms={d} key_range_ms={d} persist_ms={d} writer_publish_ms={d} wal_truncate_ms={d} file_backed={} uses_segment_wal={}",
                .{
                    seg_id,
                    segment_len,
                    nsToMs(platform_time.monotonicNs() - total_start_ns),
                    0,
                    nsToMs(reserve_id_ns),
                    nsToMs(materialize_ns),
                    nsToMs(begin_write_ns),
                    nsToMs(build_sink_ns),
                    nsToMs(finish_write_ns),
                    nsToMs(map_segment_ns),
                    nsToMs(key_range_ns),
                    nsToMs(persist_ns),
                    nsToMs(writer_publish_ns),
                    0,
                    true,
                    false,
                },
            );
        }
        return segment_len;
    }

    /// Get current snapshot (lock-free, delegates to IndexWriter).
    pub fn snapshot(self: *PersistentIndex) *index_mod.IndexSnapshot {
        return self.writer.snapshot();
    }

    pub fn acquireSnapshot(self: *PersistentIndex) *index_mod.IndexSnapshot {
        return self.writer.acquireSnapshot();
    }

    pub fn walStatsSnapshot(self: *PersistentIndex) wal_mod.WalStats {
        return self.wal.statsSnapshot();
    }

    pub fn commitStatsSnapshot(self: *PersistentIndex) ?CommitStats {
        return self.main_store_owner.commitStatsSnapshot();
    }

    pub fn statsSnapshot(self: *PersistentIndex) PersistentIndexStats {
        return .{
            .wal = self.wal.statsSnapshot(),
            .main_commit = self.main_store_owner.commitStatsSnapshot(),
        };
    }

    pub fn memoryStatsSnapshot(self: *PersistentIndex) PersistentIndexMemoryStats {
        const residency = self.writer.mappedResidencyStats();
        return .{
            .segment_virtual_mapped_bytes = residency.virtual_mapped_bytes,
            .segment_estimated_resident_bytes = residency.estimated_resident_bytes,
            .segment_recently_touched_bytes = residency.recently_touched_bytes,
            .segment_cold_mapped_bytes = residency.cold_mapped_bytes,
            .segment_residency_evictions = residency.eviction_count,
        };
    }

    pub fn activeSegmentRanges(self: *PersistentIndex, alloc: Allocator) ![]SegmentKeyRange {
        return self.activeSegmentRangesRepairing(alloc, true, false);
    }

    fn activeSegmentRangesRepairing(
        self: *PersistentIndex,
        alloc: Allocator,
        repair_stale: bool,
        storage_locked: bool,
    ) ![]SegmentKeyRange {
        var did_repair = false;
        while (true) {
            const ranges = self.loadActiveSegmentRangesSkippingMissing(alloc, repair_stale, storage_locked) catch |err| switch (err) {
                error.StaleActiveSegments => {
                    if (did_repair) return err;
                    did_repair = true;
                    continue;
                },
                else => return err,
            };
            return ranges;
        }
    }

    fn loadActiveSegmentRangesSkippingMissing(
        self: *PersistentIndex,
        alloc: Allocator,
        repair_stale: bool,
        storage_locked: bool,
    ) ![]SegmentKeyRange {
        var txn = try self.beginReadMainTxn();
        var txn_open = true;
        defer if (txn_open) txn.abort();

        const active_ids = try self.loadActiveSegmentIds(&txn, alloc);
        defer alloc.free(active_ids);

        var ranges = std.ArrayListUnmanaged(SegmentKeyRange).empty;
        errdefer {
            for (ranges.items) |*range| range.deinit(alloc);
            ranges.deinit(alloc);
        }
        var stale_ids = std.ArrayListUnmanaged(u64).empty;
        defer stale_ids.deinit(alloc);

        for (active_ids) |seg_id| {
            const range = self.loadSegmentRange(&txn, alloc, seg_id) catch |err| switch (err) {
                error.NotFound => {
                    if (!repair_stale) return err;
                    try stale_ids.append(alloc, seg_id);
                    continue;
                },
                else => return err,
            };
            try ranges.append(alloc, range);
        }
        if (stale_ids.items.len > 0) {
            txn.abort();
            txn_open = false;
            std.log.warn("persistent index pruning {d} active segment references with missing range/data", .{
                stale_ids.items.len,
            });
            if (storage_locked) {
                try self.pruneMissingActiveSegmentsLocked(stale_ids.items);
            } else {
                try self.pruneMissingActiveSegments(stale_ids.items);
            }
            return error.StaleActiveSegments;
        }
        return try ranges.toOwnedSlice(alloc);
    }

    pub fn classifyActiveSegmentsForSplit(
        self: *PersistentIndex,
        alloc: Allocator,
        split_key: []const u8,
    ) ![]SegmentSplitPlanEntry {
        const ranges = try self.activeSegmentRanges(alloc);
        defer {
            for (ranges) |*range| range.deinit(alloc);
            alloc.free(ranges);
        }

        var plan = try alloc.alloc(SegmentSplitPlanEntry, ranges.len);
        errdefer {
            for (plan) |*entry| {
                if (entry.min_doc_key.len > 0) alloc.free(entry.min_doc_key);
                if (entry.max_doc_key.len > 0) alloc.free(entry.max_doc_key);
            }
            alloc.free(plan);
        }
        for (plan) |*entry| {
            entry.* = .{
                .seg_id = 0,
                .min_doc_key = &.{},
                .max_doc_key = &.{},
                .class = .mixed,
            };
        }

        for (ranges, 0..) |range, i| {
            plan[i] = .{
                .seg_id = range.seg_id,
                .min_doc_key = try alloc.dupe(u8, range.min_doc_key),
                .max_doc_key = try alloc.dupe(u8, range.max_doc_key),
                .class = classifySegmentRange(range.min_doc_key, range.max_doc_key, split_key),
            };
        }
        return plan;
    }

    pub fn handoffRightOnlySegmentsToChild(self: *PersistentIndex, dest: *PersistentIndex, split_key: []const u8) !usize {
        var result = try self.handoffRightOnlySegmentsToChildDetailed(dest, split_key, self.alloc, true);
        defer result.deinit(self.alloc);
        return result.transferred_segments;
    }

    pub fn handoffRightOnlySegmentsToChildDetailed(
        self: *PersistentIndex,
        dest: *PersistentIndex,
        split_key: []const u8,
        alloc: Allocator,
        collect_doc_keys: bool,
    ) !SegmentHandoffResult {
        return self.handoffRightOnlySegmentsToChildImpl(dest, split_key, alloc, collect_doc_keys, false);
    }

    pub fn handoffRightOnlyLegacySegmentsToChildDetailed(self: *PersistentIndex, dest: *PersistentIndex, split_key: []const u8, alloc: Allocator, collect_doc_keys: bool) !SegmentHandoffResult {
        return self.handoffRightOnlySegmentsToChildImpl(dest, split_key, alloc, collect_doc_keys, true);
    }

    fn handoffRightOnlySegmentsToChildImpl(self: *PersistentIndex, dest: *PersistentIndex, split_key: []const u8, alloc: Allocator, collect_doc_keys: bool, skip_owned_sources: bool) !SegmentHandoffResult {
        const plan = try self.classifyActiveSegmentsForSplit(alloc, split_key);
        defer {
            for (plan) |*entry| entry.deinit(alloc);
            alloc.free(plan);
        }

        var source_txn = try self.beginReadMainTxn();
        defer source_txn.abort();

        var dest_txn = try dest.beginWriteMainTxn();
        errdefer dest_txn.abort();

        const existing_dest_ids = try dest.loadActiveSegmentIds(&dest_txn, alloc);
        defer alloc.free(existing_dest_ids);
        var existing_dest = std.AutoHashMapUnmanaged(u64, void).empty;
        defer existing_dest.deinit(alloc);
        try existing_dest.ensureTotalCapacity(alloc, @intCast(existing_dest_ids.len + plan.len));
        for (existing_dest_ids) |seg_id| {
            existing_dest.putAssumeCapacity(seg_id, {});
        }

        var replacements = try alloc.alloc(index_mod.ReplacementSegmentData, plan.len);
        defer alloc.free(replacements);
        var replacement_deleted = try alloc.alloc(?roaring.RoaringBitmap, plan.len);
        defer {
            for (replacement_deleted) |*deleted| {
                if (deleted.*) |*bitmap| bitmap.deinit();
            }
            alloc.free(replacement_deleted);
        }
        @memset(replacement_deleted, null);
        var replacements_initialized: usize = 0;
        var writer_published = false;
        defer if (!writer_published) {
            for (replacements[0..replacements_initialized]) |*replacement| {
                replacement.data.deinit(dest.alloc);
                dest.deleteSegmentFile(replacement.id);
            }
        };

        var copied: usize = 0;
        var doc_keys = std.ArrayListUnmanaged([]u8).empty;
        errdefer {
            for (doc_keys.items) |key| alloc.free(key);
            doc_keys.deinit(alloc);
        }
        for (plan) |entry| {
            if (entry.class != .right_only) continue;
            if (skip_owned_sources and self.source_roots_present and try sourceSegmentBinding(&source_txn, entry.seg_id) != null) continue;
            try self.requireSourceHandoff(&source_txn, &.{entry.seg_id});

            const seg_bytes = try self.readSegmentBytesAlloc(&source_txn, alloc, entry.seg_id);
            defer alloc.free(seg_bytes);

            const seg_key = std.mem.toBytes(std.mem.nativeToBig(u64, entry.seg_id));
            const deletion_bytes = source_txn.get(.deletions, &seg_key) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };

            if (collect_doc_keys) {
                var reader = try segment_mod.SegmentReader.init(alloc, seg_bytes);
                defer reader.deinit();
                for (0..reader.doc_count) |doc_idx| {
                    const stored = (try reader.storedDoc(@intCast(doc_idx))) orelse continue;
                    try doc_keys.append(alloc, try alloc.dupe(u8, stored.id));
                }
            }

            if (existing_dest.contains(entry.seg_id)) continue;

            if (deletion_bytes) |bytes| {
                var bitmap = try roaring.RoaringBitmap.fromBytes(alloc, bytes);
                if (!bitmap.isEmpty()) {
                    replacement_deleted[replacements_initialized] = bitmap;
                } else {
                    bitmap.deinit();
                }
                try dest_txn.put(.deletions, &seg_key, bytes);
            }

            replacements[replacements_initialized] = .{
                .id = entry.seg_id,
                .data = try dest.materializeSegmentData(entry.seg_id, seg_bytes),
                .deleted = if (replacement_deleted[replacements_initialized]) |*bitmap| bitmap else null,
            };
            replacements_initialized += 1;

            if (dest.segment_files == null) try dest_txn.put(.segments, &seg_key, seg_bytes);
            try dest.saveSegmentRange(&dest_txn, entry.seg_id, .{
                .seg_id = entry.seg_id,
                .min_doc_key = entry.min_doc_key,
                .max_doc_key = entry.max_doc_key,
            });
            try dest.updateActiveSegments(&dest_txn, entry.seg_id, .add);
            existing_dest.putAssumeCapacity(entry.seg_id, {});
            copied += 1;
        }

        var writer_publication: ?index_mod.IndexWriter.PreparedSegmentReplacement = null;
        defer if (writer_publication) |*publication| publication.abort();
        if (replacements_initialized > 0) {
            writer_publication = try dest.writer.prepareSegmentsManyData(&.{}, replacements[0..replacements_initialized]);
        }
        const owned_doc_keys = try doc_keys.toOwnedSlice(alloc);
        errdefer {
            for (owned_doc_keys) |key| alloc.free(key);
            alloc.free(owned_doc_keys);
        }
        try dest_txn.commit();
        if (writer_publication) |*publication| publication.publish();
        writer_published = true;

        return .{
            .transferred_segments = copied,
            .doc_keys = owned_doc_keys,
        };
    }

    pub fn readStoredSegment(self: *PersistentIndex, alloc: Allocator, seg_id: u64) !StoredSegment {
        var txn = try self.beginReadMainTxn();
        defer txn.abort();

        const seg_key = std.mem.toBytes(std.mem.nativeToBig(u64, seg_id));
        const segment_bytes = try self.readSegmentBytesAlloc(&txn, alloc, seg_id);
        errdefer alloc.free(segment_bytes);

        const deletion_bitmap_bytes = txn.get(.deletions, &seg_key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };

        return .{
            .seg_id = seg_id,
            .segment_bytes = segment_bytes,
            .deletion_bitmap_bytes = if (deletion_bitmap_bytes) |bytes| try alloc.dupe(u8, bytes) else null,
        };
    }

    /// Tombstone a document by external ID and persist the segment deletion bitmap.
    pub fn deleteById(self: *PersistentIndex, doc_id: []const u8) !bool {
        return try self.deleteByIds(&.{doc_id});
    }

    /// Tombstone a batch of external IDs in one segment traversal and one
    /// storage transaction. This preserves all-or-nothing bitmap persistence
    /// without multiplying the scan cost by the number of IDs.
    pub fn deleteByIds(self: *PersistentIndex, doc_ids: []const []const u8) !bool {
        const delete_infos = try self.deleteByIdsTracked(doc_ids);
        defer self.freeDeleteInfos(delete_infos);
        return delete_infos.len != 0;
    }

    pub fn freeDeleteInfos(self: *PersistentIndex, delete_infos: []index_mod.IndexWriter.DeleteInfo) void {
        index_mod.IndexWriter.freeDeleteInfos(self.alloc, delete_infos);
    }

    /// Tombstone a batch and return the exact newly-deleted local document IDs
    /// after their bitmap transaction commits. Merge publication uses this
    /// committed delta directly, avoiding a scan of historical tombstones.
    /// The caller owns the returned delete infos and must release them with
    /// `freeDeleteInfos` on this index.
    pub fn deleteByIdsTracked(self: *PersistentIndex, doc_ids: []const []const u8) ![]index_mod.IndexWriter.DeleteInfo {
        if (doc_ids.len == 0) return try self.alloc.alloc(index_mod.IndexWriter.DeleteInfo, 0);
        self.lockStorage();
        defer self.unlockStorage();

        const delete_infos = try self.writer.deleteAllByIdsTracked(self.alloc, doc_ids);
        errdefer index_mod.IndexWriter.freeDeleteInfos(self.alloc, delete_infos);
        if (delete_infos.len == 0) return delete_infos;
        var persisted = false;
        defer if (!persisted) self.writer.rollbackDeleteInfos(delete_infos);

        var txn = try self.beginWriteMainTxn();
        errdefer txn.abort();

        for (delete_infos) |delete_info| {
            const seg_key = std.mem.toBytes(std.mem.nativeToBig(u64, delete_info.seg_id));
            try txn.put(.deletions, &seg_key, delete_info.bitmap_bytes);
        }
        try txn.commit();
        persisted = true;
        return delete_infos;
    }

    pub const SourceRoot = struct {
        binding: source_publication.Binding,
        references: []const source_claims.Reference,
        stamp: @import("../segment_source.zig").ContentStamp,
    };

    const SourceReplacement = struct { binding: source_publication.Binding, references: []const source_claims.Reference };

    /// Physically durable root handoff, with a prepared primary claim for each
    /// target document. Receipt, source map, active roots and old retirement
    /// commit together. Takes segment ownership on every success/error/CAS path.
    pub fn replaceSegmentsIfActiveOwnedWithSourceMap(self: *PersistentIndex, previous: []const u64, segment: []u8, binding: source_publication.Binding, refs: []const source_claims.Reference) !?source_publication.Decision {
        return self.replaceSegmentsIfActiveOwnedWithSourceMapFromAllocator(self.alloc, previous, segment, binding, refs);
    }

    pub fn replaceSegmentsIfActiveOwnedWithSourceMapFromAllocator(self: *PersistentIndex, owner: Allocator, previous: []const u64, segment: []u8, binding: source_publication.Binding, refs: []const source_claims.Reference) !?source_publication.Decision {
        if (previous.len == 0) {
            owner.free(segment);
            return null;
        }
        const segments = owner.alloc([]u8, 1) catch |err| {
            owner.free(segment);
            return err;
        };
        segments[0] = segment;
        self.lockStorage();
        defer self.unlockStorage();
        const ids = self.reserveSegmentIds(owner, 1) catch |err| {
            freeOwnedSegmentList(owner, segments);
            return err;
        };
        defer owner.free(ids);
        if (!try self.replaceSegmentsWithReservedIdsOwned(owner, previous, ids, segments, null, true, .{ .binding = binding, .references = refs })) return null;
        return try self.durableSourceDecisionLocked(binding);
    }

    /// Atomically replace old segments with a newly merged segment.
    pub fn replaceSegments(self: *PersistentIndex, old_seg_ids: []const u64, segment_bytes: []const u8) !void {
        self.lockStorage();
        defer self.unlockStorage();

        const new_seg_id = self.reserveSegmentId();
        _ = try self.replaceSegmentsWithReservedId(old_seg_ids, new_seg_id, segment_bytes, false);
    }

    /// Like replaceSegments(), but takes ownership of segment_bytes.
    pub fn replaceSegmentsOwned(self: *PersistentIndex, old_seg_ids: []const u64, segment_bytes: []u8) !void {
        self.lockStorage();
        defer self.unlockStorage();

        const new_seg_id = self.reserveSegmentId();
        _ = try self.replaceSegmentsWithReservedIdOwned(old_seg_ids, new_seg_id, segment_bytes, false);
    }

    /// Like replaceSegments, but skips the merge if the planned source segment
    /// IDs are no longer all active. Merge schedulers use this after building
    /// against a retained snapshot while concurrent writes may have advanced.
    pub fn replaceSegmentsIfActive(self: *PersistentIndex, old_seg_ids: []const u64, segment_bytes: []const u8) !bool {
        if (old_seg_ids.len == 0) return false;
        self.lockStorage();
        defer self.unlockStorage();

        const new_seg_id = self.reserveSegmentId();
        return try self.replaceSegmentsWithReservedId(old_seg_ids, new_seg_id, segment_bytes, true);
    }

    /// Like replaceSegmentsIfActive(), but consumes segment_bytes regardless of
    /// whether the merge is applied.
    pub fn replaceSegmentsIfActiveOwned(self: *PersistentIndex, old_seg_ids: []const u64, segment_bytes: []u8) !bool {
        if (old_seg_ids.len == 0) {
            self.alloc.free(segment_bytes);
            return false;
        }
        self.lockStorage();
        defer self.unlockStorage();

        const new_seg_id = self.reserveSegmentId();
        return try self.replaceSegmentsWithReservedIdOwned(old_seg_ids, new_seg_id, segment_bytes, true);
    }

    /// Atomically replace old segments with zero or more newly merged segments.
    /// Consumes segment_bytes_list and every segment buffer regardless of
    /// whether the replacement is applied.
    pub fn replaceSegmentsIfActiveManyOwned(self: *PersistentIndex, old_seg_ids: []const u64, segment_bytes_list: [][]u8) !bool {
        return try self.replaceSegmentsIfActiveManyOwnedWithDeletes(old_seg_ids, segment_bytes_list, null);
    }

    /// Publish merged heap-backed segments while atomically carrying forward
    /// tombstones that arrived after the merge task froze its source view.
    pub fn replaceSegmentsIfActiveManyOwnedWithDeletes(
        self: *PersistentIndex,
        old_seg_ids: []const u64,
        segment_bytes_list: [][]u8,
        replacement_deleted: ?[]const ?roaring.RoaringBitmap,
    ) !bool {
        return try self.replaceSegmentsIfActiveManyOwnedWithDeletesAndScratch(
            self.alloc,
            old_seg_ids,
            segment_bytes_list,
            replacement_deleted,
        );
    }

    /// Heap-backed merge publication with caller-budgeted transient staging.
    /// `scratch_alloc` owns the consumed input list and its segment buffers as
    /// well as arrays and serialized tombstones that die before return. The
    /// durable segment copy and published snapshot use the persistent allocator.
    pub fn replaceSegmentsIfActiveManyOwnedWithDeletesAndScratch(
        self: *PersistentIndex,
        scratch_alloc: Allocator,
        old_seg_ids: []const u64,
        segment_bytes_list: [][]u8,
        replacement_deleted: ?[]const ?roaring.RoaringBitmap,
    ) !bool {
        if (old_seg_ids.len == 0) {
            freeOwnedSegmentList(scratch_alloc, segment_bytes_list);
            return false;
        }
        if (segment_bytes_list.len == 0) {
            return try self.removeSegmentsIfActive(old_seg_ids);
        }
        if (replacement_deleted) |deleted| {
            if (deleted.len != segment_bytes_list.len) {
                freeOwnedSegmentList(scratch_alloc, segment_bytes_list);
                return error.InvalidDeletionSnapshot;
            }
        }
        self.lockStorage();
        defer self.unlockStorage();

        const new_seg_ids = self.reserveSegmentIds(scratch_alloc, segment_bytes_list.len) catch |err| {
            freeOwnedSegmentList(scratch_alloc, segment_bytes_list);
            return err;
        };
        defer scratch_alloc.free(new_seg_ids);
        return try self.replaceSegmentsWithReservedIdsOwned(
            scratch_alloc,
            old_seg_ids,
            new_seg_ids,
            segment_bytes_list,
            replacement_deleted,
            true,
            null,
        );
    }

    pub fn prepareMergedSegmentToFile(self: *PersistentIndex, snap: *const index_mod.IndexSnapshot, segment_indices: []const usize) ![]PreparedMergeSegment {
        return try self.prepareMergedSegmentToFileWithAllocator(self.alloc, snap, segment_indices);
    }

    /// File-backed merge with an explicit allocator for task-local working
    /// state. The returned prepared list retains `work_alloc` for its key-range
    /// metadata and must be published or discarded before that allocator dies.
    pub fn prepareMergedSegmentToFileWithAllocator(
        self: *PersistentIndex,
        work_alloc: Allocator,
        snap: *const index_mod.IndexSnapshot,
        segment_indices: []const usize,
    ) ![]PreparedMergeSegment {
        return try self.prepareMergedSegmentToFileWithAllocatorAndDeletes(work_alloc, snap, segment_indices, null);
    }

    /// Prepare a file-backed merge against an immutable, task-owned deletion
    /// view. The optional slice is aligned with `segment_indices`; callers may
    /// therefore release their mutation lock while segment bytes are merged.
    pub fn prepareMergedSegmentToFileWithAllocatorAndDeletes(
        self: *PersistentIndex,
        work_alloc: Allocator,
        snap: *const index_mod.IndexSnapshot,
        segment_indices: []const usize,
        deleted_docs: ?[]const ?roaring.RoaringBitmap,
    ) ![]PreparedMergeSegment {
        return try self.prepareMergedSegmentToFileWithAllocatorsAndDeletes(
            work_alloc,
            work_alloc,
            snap,
            segment_indices,
            deleted_docs,
        );
    }

    /// Variant for callers whose transient accounting allocator is scoped to
    /// the build call. Retained result metadata uses `staging_alloc`, which
    /// must remain alive until publication or discard.
    pub fn prepareMergedSegmentToFileWithAllocatorsAndDeletes(
        self: *PersistentIndex,
        work_alloc: Allocator,
        staging_alloc: Allocator,
        snap: *const index_mod.IndexSnapshot,
        segment_indices: []const usize,
        deleted_docs: ?[]const ?roaring.RoaringBitmap,
    ) ![]PreparedMergeSegment {
        return self.prepareMergedSegmentToFileWithSourceMap(work_alloc, staging_alloc, snap, segment_indices, deleted_docs, null);
    }

    pub fn prepareMergedSegmentToFileWithSourceMap(
        self: *PersistentIndex,
        work_alloc: Allocator,
        staging_alloc: Allocator,
        snap: *const index_mod.IndexSnapshot,
        segment_indices: []const usize,
        deleted_docs: ?[]const ?roaring.RoaringBitmap,
        source_map: ?*segment_mod.MergeSourceMap,
    ) ![]PreparedMergeSegment {
        return self.prepareMergedSegmentToFileWithSourceMapAndIo(work_alloc, staging_alloc, snap, segment_indices, deleted_docs, source_map, self.io);
    }

    pub fn prepareMergedSegmentToFileWithSourceMapAndIo(
        self: *PersistentIndex,
        work_alloc: Allocator,
        staging_alloc: Allocator,
        snap: *const index_mod.IndexSnapshot,
        segment_indices: []const usize,
        deleted_docs: ?[]const ?roaring.RoaringBitmap,
        source_map: ?*segment_mod.MergeSourceMap,
        native_io: ?std.Io,
    ) ![]PreparedMergeSegment {
        if (segment_indices.len == 0) return error.NoSegments;
        if (deleted_docs) |frozen| {
            if (frozen.len != segment_indices.len) return error.InvalidDeletionSnapshot;
        }
        const store = &(self.segment_files orelse return error.Unsupported);
        if (!self.supportsFileBackedSegmentArtifacts()) return error.Unsupported;
        var publication_admission: ?NativeSegmentPublicationAdmission = if (store.storage_owner) |owner|
            try NativeSegmentPublicationAdmission.init(owner)
        else
            null;
        defer if (publication_admission) |*admission| admission.deinit();

        var inputs = try work_alloc.alloc(segment_mod.MergeInput, segment_indices.len);
        defer work_alloc.free(inputs);
        const owned_deleted_docs = try work_alloc.alloc(
            ?roaring.RoaringBitmap,
            if (deleted_docs == null) segment_indices.len else 0,
        );
        defer {
            for (owned_deleted_docs) |*maybe_deleted| {
                if (maybe_deleted.*) |*deleted| deleted.deinit();
            }
            work_alloc.free(owned_deleted_docs);
        }
        if (owned_deleted_docs.len > 0) @memset(owned_deleted_docs, null);
        for (segment_indices, 0..) |seg_idx, i| {
            const seg = &snap.segments[seg_idx];
            if (deleted_docs == null) owned_deleted_docs[i] = try seg.cloneDeleted(work_alloc);
            inputs[i] = .{
                .reader = &seg.reader,
                .deleted = if (deleted_docs) |frozen| frozen[i] else owned_deleted_docs[i],
            };
        }
        const index_sort = try segment_mod.commonIndexSortForMergeInputsAlloc(work_alloc, inputs);
        defer segment_mod.freeIndexSortFields(work_alloc, index_sort);

        if (self.sourceFieldsEnabled() and source_map != null and native_io != null) {
            const all_sources = for (segment_indices) |index| {
                if (!snap.segments[index].hasSourceReferences()) break false;
            } else true;
            if (all_sources) {
                if (self.prepareDirectPackedMerge(work_alloc, staging_alloc, inputs, index_sort, source_map.?, native_io.?)) |outputs| return outputs else |err| {
                    // Capability misses are reported before the source map or any
                    // output body is written. Optional backend capability
                    // misses retain the original flat-then-pack path.
                    if (err != error.DirectPackUnavailable) return err;
                }
            }
        }
        const new_seg_id = self.reserveSegmentId();
        errdefer self.deleteSegmentFile(new_seg_id);
        const path = try store.pathAlloc(new_seg_id);
        defer store.allocator.free(path);

        var writer = if (publication_admission) |*admission|
            try admission.beginAtomicWrite(work_alloc, path)
        else
            try store.storage.beginAtomicWrite(work_alloc, path);
        var writer_active = true;
        errdefer if (writer_active) writer.abort();

        if (writer.vtable.finish_source != null or writer.vtable.finish_mapped != null) writer.setCacheIntent(.cold_sequential);
        var sink_adapter = AtomicSegmentSink.init(work_alloc, &writer);
        defer sink_adapter.deinit();
        var sink = sink_adapter.sink();
        try segment_mod.writeMergedSegmentToSinkWithOptions(work_alloc, &sink, inputs, .{
            .index_sort = index_sort,
            .source_map = source_map,
            .scratch = if (self.io) |runtime| if (self.privateScratchDirectory(store.root_dir)) |directory| .{ .io = runtime, .directory = directory, .resource_manager = self.writer.resource_manager } else null else null,
        });
        if (source_map) |map| try map.finishOutput(blk: {
            var live: u32 = 0;
            for (inputs) |input| {
                live += input.reader.doc_count - if (input.deleted) |deleted| @as(u32, @intCast(deleted.cardinality())) else @as(u32, 0);
            }
            break :blk live;
        });
        try sink_adapter.flush();

        writer_active = false;
        var data: ?index_mod.SegmentData = if (writer.vtable.finish_source != null)
            .fromNative(try writer.finishSource())
        else if (writer.vtable.finish_mapped != null)
            .fromMappedArtifact(try writer.finishMapped())
        else blk: {
            try writer.finish();
            break :blk if (publication_admission) |*admission|
                .fromMapped(try admission.mapFile(path))
            else
                try store.openPublishedSource(path);
        };
        errdefer if (data) |*segment_data| segment_data.deinit(self.alloc);

        var key_range = try extractSegmentDataKeyRange(staging_alloc, data.?);
        errdefer key_range.deinit(staging_alloc);
        key_range.seg_id = new_seg_id;

        const prepared = try staging_alloc.alloc(PreparedMergeSegment, 1);
        errdefer staging_alloc.free(prepared);
        prepared[0] = .{
            .id = new_seg_id,
            .data = data.?,
            .key_range = key_range,
            .staging_alloc = staging_alloc,
        };
        data = null;
        return prepared;
    }

    pub fn replaceSegmentsIfActiveManyPrepared(self: *PersistentIndex, old_seg_ids: []const u64, prepared_segments: []PreparedMergeSegment) !bool {
        return try self.replaceSegmentsIfActiveManyPreparedWithDeletes(old_seg_ids, prepared_segments, null);
    }

    /// Publish prepared file-backed segments and their deletion bitmaps in the
    /// same durable transaction and volatile snapshot replacement.
    pub fn replaceSegmentsIfActiveManyPreparedWithDeletes(
        self: *PersistentIndex,
        old_seg_ids: []const u64,
        prepared_segments: []PreparedMergeSegment,
        replacement_deleted: ?[]const ?roaring.RoaringBitmap,
    ) !bool {
        return try self.replaceSegmentsIfActiveManyPreparedWithDeletesAndScratch(
            self.alloc,
            old_seg_ids,
            prepared_segments,
            replacement_deleted,
        );
    }

    /// File-backed merge publication with caller-budgeted transient staging.
    pub fn replaceSegmentsIfActiveManyPreparedWithDeletesAndScratch(
        self: *PersistentIndex,
        scratch_alloc: Allocator,
        old_seg_ids: []const u64,
        prepared_segments: []PreparedMergeSegment,
        replacement_deleted: ?[]const ?roaring.RoaringBitmap,
    ) !bool {
        return self.replaceSegmentsManyPreparedImpl(scratch_alloc, old_seg_ids, prepared_segments, replacement_deleted, null);
    }

    pub fn replaceSegmentsIfActiveManyPreparedWithSourceRoots(self: *PersistentIndex, scratch_alloc: Allocator, old_seg_ids: []const u64, prepared_segments: []PreparedMergeSegment, replacement_deleted: ?[]const ?roaring.RoaringBitmap, roots: []const SourceRoot) !bool {
        return self.replaceSegmentsManyPreparedImpl(scratch_alloc, old_seg_ids, prepared_segments, replacement_deleted, roots);
    }

    // Called with storage_mu held. On uncertain ownership keep the artifact;
    // deleting an unpublished orphan can wait for inventory reconciliation.
    fn sourcePreparedFileUnpublished(self: *PersistentIndex, id: u64) bool {
        var read = self.beginReadMainTxn() catch return false;
        defer read.abort();
        const segment_key = std.mem.toBytes(std.mem.nativeToBig(u64, id));
        const probes = [_]struct { space: MainKeyspace, key: []const u8 }{
            .{ .space = .meta, .key = &activeSegmentMetaKey(id) },
            .{ .space = .meta, .key = &sourceSegmentKey(id) },
            .{ .space = .segments, .key = &segment_key },
        };
        for (probes) |probe| {
            _ = read.get(probe.space, probe.key) catch |err| switch (err) {
                error.NotFound => continue,
                else => return false,
            };
            return false;
        }
        return true;
    }

    fn replaceSegmentsManyPreparedImpl(
        self: *PersistentIndex,
        scratch_alloc: Allocator,
        old_seg_ids: []const u64,
        prepared_segments: []PreparedMergeSegment,
        replacement_deleted: ?[]const ?roaring.RoaringBitmap,
        roots: ?[]const SourceRoot,
    ) !bool {
        if (prepared_segments.len == 0) {
            if (roots) |sources| if (sources.len != 0) return error.InvalidIndexSourceMap;
            return try self.removeSegmentsIfActive(old_seg_ids);
        }

        self.lockStorage();
        defer self.unlockStorage();

        const staging_alloc = prepared_segments[0].staging_alloc;
        var disposition: enum { discard, published, already_published } = .discard;
        defer {
            if (disposition != .published) {
                for (prepared_segments) |*segment| {
                    const id = segment.id;
                    segment.deinit(self.alloc);
                    if (disposition == .discard and (roots == null or self.sourcePreparedFileUnpublished(id))) self.deleteSegmentFile(id);
                }
            } else {
                for (prepared_segments) |*segment| {
                    segment.key_range.deinit(segment.staging_alloc);
                }
            }
            staging_alloc.free(prepared_segments);
        }

        // Each output has its own single-segment receipt. The receipt codec's
        // segment limit is not a bound on this atomic replacement's outputs;
        // its working state uses the caller's admitted preparation allocator.
        if (roots) |sources| if (sources.len != prepared_segments.len) return error.InvalidIndexSourceMap;
        if (old_seg_ids.len == 0) return false;
        if (replacement_deleted) |deleted| if (deleted.len != prepared_segments.len) return error.InvalidDeletionSnapshot;

        const plans = try scratch_alloc.alloc(exact_source_map.Plan, if (roots) |sources| sources.len else 0);
        var plans_initialized: usize = 0;
        defer {
            for (plans[0..plans_initialized]) |plan| plan.deinit(scratch_alloc);
            scratch_alloc.free(plans);
        }
        if (roots) |sources| {
            if (self.read_only) return error.ReadOnly;
            if (self.segment_files == null or self.active_rebuild_page != null) return error.UnsupportedSourcePublication;
            var read = try self.beginReadMainTxn();
            defer read.abort();
            const owner = (try sourceOwnerInTxn(&read)) orelse return error.IndexSourceOwnerMismatch;
            var metadata: PublicationTxn = .{ .txn = &read };
            for (sources) |source| if (!std.mem.eql(u8, &owner, &source.binding.owner)) return error.IndexSourceOwnerMismatch;
            if (sources.len > 1) {
                const identities = try scratch_alloc.alloc(source_publication.Digest, sources.len);
                defer scratch_alloc.free(identities);
                for (sources, identities) |source, *identity| identity.* = source.binding.publication;
                std.mem.sort(source_publication.Digest, identities, {}, struct {
                    fn less(_: void, left: source_publication.Digest, right: source_publication.Digest) bool {
                        return std.mem.order(u8, &left, &right) == .lt;
                    }
                }.less);
                for (identities[1..], identities[0 .. identities.len - 1]) |next, previous| {
                    if (std.mem.eql(u8, &next, &previous)) return error.IndexPublicationConflict;
                }
            }
            var committed: usize = 0;
            for (sources, prepared_segments, 0..) |source, segment, i| {
                try source.stamp.require(segment.data.source());
                plans[i] = try exact_source_map.Plan.prepare(scratch_alloc, source.binding, source.stamp.digest, source.references);
                plans_initialized += 1;
                if (try source_publication.load(&metadata, source.binding)) |existing| {
                    if (existing.state == .cancelled) return error.IndexPublicationCancelled;
                    if (existing.state == .retired) return error.IndexPublicationRetired;
                    if (!std.mem.eql(u8, &existing.root, &plans[i].root()) or existing.count != 1 or existing.segments[0] != segment.id) return error.IndexPublicationConflict;
                    committed += 1;
                }
            }
            if (committed != 0) {
                if (committed != sources.len) return error.IndexPublicationConflict;
                disposition = .already_published;
                read.abort();
                try self.main_store_owner.sync(true);
                return true;
            }
        }

        const replacements = try scratch_alloc.alloc(index_mod.ReplacementSegmentData, prepared_segments.len);
        defer scratch_alloc.free(replacements);
        for (prepared_segments, 0..) |*segment, i| {
            replacements[i] = .{
                .id = segment.id,
                .data = segment.data,
                .deleted = if (replacement_deleted) |deleted|
                    if (deleted[i]) |*bitmap| bitmap else null
                else
                    null,
            };
        }

        var txn = try self.beginWriteMainTxn();
        errdefer txn.abort();
        if (roots == null) try self.requireSourceHandoff(&txn, old_seg_ids) else try requireSourceOwnerOpen(&txn);

        const active_ids = try self.loadActiveSegmentIds(&txn, scratch_alloc);
        defer scratch_alloc.free(active_ids);
        for (old_seg_ids) |old_id| {
            if (!containsSegmentId(active_ids, old_id)) {
                txn.abort();
                return false;
            }
        }

        var writer_publication = try self.writer.prepareSegmentsManyDataWithScratch(scratch_alloc, old_seg_ids, replacements);
        defer writer_publication.abort();
        if (roots) |sources| for (sources, 0..) |source, i| {
            try self.attachSourceReferences(&writer_publication.snapshot.segments[writer_publication.carried_count + i], source.binding, source.references);
        };

        for (old_seg_ids) |old_id| {
            const old_seg_key = std.mem.toBytes(std.mem.nativeToBig(u64, old_id));
            try self.updateActiveSegments(&txn, old_id, .remove);
            txn.delete(.segments, &old_seg_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            txn.delete(.deletions, &old_seg_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            txn.delete(.meta, &fieldSegmentKey(old_id)) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            try self.deleteSegmentRange(&txn, old_id);
        }

        for (prepared_segments, 0..) |*segment, i| {
            try self.saveSegmentRangeWithAllocator(scratch_alloc, &txn, segment.id, segment.key_range);
            try self.updateActiveSegments(&txn, segment.id, .add);
            if (replacement_deleted) |deleted| {
                if (deleted[i]) |*bitmap| {
                    if (!bitmap.isEmpty()) {
                        const bitmap_bytes = try bitmap.toBytes(scratch_alloc);
                        defer scratch_alloc.free(bitmap_bytes);
                        const segment_key = std.mem.toBytes(std.mem.nativeToBig(u64, segment.id));
                        try txn.put(.deletions, &segment_key, bitmap_bytes);
                    }
                }
            }
        }
        if (roots) |sources| {
            var metadata: PublicationTxn = .{ .txn = &txn };
            for (sources, prepared_segments, plans) |source, segment, plan| {
                if (!try source_publication.stagePublishInEpoch(scratch_alloc, &metadata, source.binding, plan.root(), &.{segment.id}, try sourcePublicationEpoch(&txn, source.binding))) return error.IndexPublicationConflict;
                try stageSourceSegment(&txn, segment.id, source.binding);
                try plan.stage(&metadata);
            }
            try self.retireRemovedSourceRoots(&txn, old_seg_ids);
        }
        try saveNextSegmentId(&txn, self.writer.next_segment_id);
        try txn.commit();

        if (roots != null) self.source_roots_present = true;
        writer_publication.publish();
        disposition = .published;
        if (roots != null) {
            self.noteSourceRecoveryWork();
            try self.main_store_owner.sync(true);
        }
        return true;
    }

    /// Compatibility for a merge backend that returned heap output. Materialize
    /// each immutable artifact once, preserving the caller's staging budget.
    /// Input buffers remain borrowed; output owns independent physical sources.
    pub fn prepareSourceSegmentCopies(self: *PersistentIndex, a: Allocator, bytes: []const []u8) ![]PreparedMergeSegment {
        if (self.segment_files == null) return error.UnsupportedSourcePublication;
        const prepared = try a.alloc(PreparedMergeSegment, bytes.len);
        var initialized: usize = 0;
        errdefer {
            for (prepared[0..initialized]) |*item| {
                const id = item.id;
                item.deinit(self.alloc);
                self.deleteSegmentFile(id);
            }
            a.free(prepared);
        }
        for (bytes, prepared) |body, *item| {
            const id = self.reserveSegmentId();
            errdefer self.deleteSegmentFile(id);
            var data = try self.materializeSegmentData(id, body);
            errdefer data.deinit(self.alloc);
            var range = try extractSegmentKeyRange(a, body);
            range.seg_id = id;
            item.* = .{ .id = id, .data = data, .key_range = range, .staging_alloc = a };
            initialized += 1;
        }
        return prepared;
    }

    fn prepareSourceSegmentCopy(self: *PersistentIndex, a: Allocator, source: @import("../segment_source.zig").Source) !PreparedMergeSegment {
        const files = if (self.segment_files) |*store| store else return error.UnsupportedSourcePublication;
        const id = self.reserveSegmentId();
        errdefer self.deleteSegmentFile(id);
        var data = try files.publishSource(id, source);
        errdefer data.deinit(self.alloc);
        var range = try extractSegmentDataKeyRange(a, data);
        range.seg_id = id;
        return .{ .id = id, .data = data, .key_range = range, .staging_alloc = a };
    }

    pub fn discardPreparedMergeSegments(self: *PersistentIndex, prepared_segments: []PreparedMergeSegment) void {
        if (prepared_segments.len == 0) return;
        const staging_alloc = prepared_segments[0].staging_alloc;
        for (prepared_segments) |*segment| {
            const id = segment.id;
            const transferred = if (segment.native_pack) |pack_output| pack_output.transferred else false;
            segment.deinit(self.alloc);
            if (!transferred) self.deleteSegmentFile(id);
        }
        staging_alloc.free(prepared_segments);
    }

    pub fn removeSegmentsIfActive(self: *PersistentIndex, old_seg_ids: []const u64) !bool {
        if (old_seg_ids.len == 0) return false;
        self.lockStorage();
        defer self.unlockStorage();

        if (!try self.allSegmentsActive(old_seg_ids)) return false;
        try self.removeSegmentsLocked(old_seg_ids);
        return true;
    }

    fn allSegmentsActive(self: *PersistentIndex, old_seg_ids: []const u64) !bool {
        var txn = try self.beginReadMainTxn();
        defer txn.abort();
        const active_ids = try self.loadActiveSegmentIds(&txn, self.alloc);
        defer self.alloc.free(active_ids);
        for (old_seg_ids) |old_id| {
            if (!containsSegmentId(active_ids, old_id)) return false;
        }
        return true;
    }

    fn reserveSegmentId(self: *PersistentIndex) u64 {
        self.writer.lockMutex();
        const seg_id = self.writer.next_segment_id;
        self.writer.next_segment_id += 1;
        self.writer.mu.unlock();
        return seg_id;
    }

    fn reserveSegmentIds(self: *PersistentIndex, alloc: Allocator, count: usize) ![]u64 {
        const seg_ids = try alloc.alloc(u64, count);
        self.writer.lockMutex();
        defer self.writer.mu.unlock();
        for (seg_ids, 0..) |*seg_id, i| {
            seg_id.* = self.writer.next_segment_id + i;
        }
        self.writer.next_segment_id += count;
        return seg_ids;
    }

    fn materializeSegmentData(self: *PersistentIndex, seg_id: u64, segment_bytes: []const u8) !index_mod.SegmentData {
        if (comptime @import("builtin").os.tag != .freestanding) {
            if (self.segment_files) |*store| {
                return try store.publish(seg_id, segment_bytes);
            }
        }
        return index_mod.SegmentData.fromOwnedHeap(try self.alloc.dupe(u8, segment_bytes));
    }

    fn readSegmentBytesAlloc(self: *PersistentIndex, txn: *MainTxn, alloc: Allocator, seg_id: u64) ![]u8 {
        const seg_key = std.mem.toBytes(std.mem.nativeToBig(u64, seg_id));
        if (txn.get(.segments, &seg_key)) |segment_bytes| {
            return try alloc.dupe(u8, segment_bytes);
        } else |err| switch (err) {
            error.NotFound => {},
            else => return err,
        }

        const store = self.segment_files orelse return error.NotFound;
        if (comptime builtin.os.tag == .freestanding) return error.NotFound;

        const segment_path = try store.pathAlloc(seg_id);
        defer store.allocator.free(segment_path);

        return store.storage.readFileAlloc(
            alloc,
            segment_path,
            std.math.maxInt(usize),
        ) catch |file_err| switch (file_err) {
            error.FileNotFound => error.NotFound,
            else => file_err,
        };
    }

    fn deleteSegmentFile(self: *PersistentIndex, seg_id: u64) void {
        if (self.segment_files) |*store| store.delete(seg_id);
    }

    fn pruneActiveSegmentsMissingRangesOrDataLocked(self: *PersistentIndex, log_path: []const u8) !void {
        const ranges = self.activeSegmentRangesRepairing(self.alloc, true, true) catch |err| switch (err) {
            error.StaleActiveSegments => return error.NotFound,
            else => return err,
        };
        defer {
            for (ranges) |*range| range.deinit(self.alloc);
            self.alloc.free(ranges);
        }
        _ = log_path;
    }

    fn pruneMissingActiveSegments(self: *PersistentIndex, stale_seg_ids: []const u64) !void {
        self.lockStorage();
        defer self.unlockStorage();
        try self.pruneMissingActiveSegmentsLocked(stale_seg_ids);
    }

    fn pruneMissingActiveSegmentsLocked(self: *PersistentIndex, stale_seg_ids: []const u64) !void {
        if (stale_seg_ids.len == 0) return;

        var txn = try self.beginWriteMainTxn();
        errdefer txn.abort();

        for (stale_seg_ids) |seg_id| {
            const seg_key = std.mem.toBytes(std.mem.nativeToBig(u64, seg_id));
            try self.updateActiveSegments(&txn, seg_id, .remove);
            txn.delete(.segments, &seg_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            txn.delete(.deletions, &seg_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            try self.deleteSegmentRange(&txn, seg_id);
        }

        try txn.commit();
    }

    fn replaceSegmentsWithReservedId(self: *PersistentIndex, old_seg_ids: []const u64, new_seg_id: u64, segment_bytes: []const u8, require_active: bool) !bool {
        const owned = try self.alloc.dupe(u8, segment_bytes);
        return try self.replaceSegmentsWithReservedIdOwned(old_seg_ids, new_seg_id, owned, require_active);
    }

    fn replaceSegmentsWithReservedIdOwned(self: *PersistentIndex, old_seg_ids: []const u64, new_seg_id: u64, segment_bytes: []u8, require_active: bool) !bool {
        const owned = segment_bytes;
        defer self.alloc.free(owned);

        var key_range = try extractSegmentKeyRange(self.alloc, owned);
        defer key_range.deinit(self.alloc);

        var segment_data: ?index_mod.SegmentData = try self.materializeSegmentData(new_seg_id, owned);
        errdefer {
            if (segment_data) |*data| data.deinit(self.alloc);
            self.deleteSegmentFile(new_seg_id);
        }

        var txn = try self.beginWriteMainTxn();
        errdefer txn.abort();
        try self.requireSourceHandoff(&txn, old_seg_ids);

        if (require_active) {
            const active_ids = try self.loadActiveSegmentIds(&txn, self.alloc);
            defer self.alloc.free(active_ids);
            for (old_seg_ids) |old_id| {
                if (!containsSegmentId(active_ids, old_id)) {
                    txn.abort();
                    if (segment_data) |*data| data.deinit(self.alloc);
                    self.deleteSegmentFile(new_seg_id);
                    return false;
                }
            }
        }

        var replacement = [_]index_mod.ReplacementSegmentData{.{
            .id = new_seg_id,
            .data = segment_data.?,
        }};
        var writer_publication = try self.writer.prepareSegmentsManyData(old_seg_ids, &replacement);
        defer writer_publication.abort();

        const new_seg_key = std.mem.toBytes(std.mem.nativeToBig(u64, new_seg_id));
        if (self.segment_files == null) try txn.put(.segments, &new_seg_key, owned);

        for (old_seg_ids) |old_id| {
            const old_seg_key = std.mem.toBytes(std.mem.nativeToBig(u64, old_id));
            try self.updateActiveSegments(&txn, old_id, .remove);
            txn.delete(.segments, &old_seg_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            txn.delete(.deletions, &old_seg_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            txn.delete(.meta, &fieldSegmentKey(old_id)) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            try self.deleteSegmentRange(&txn, old_id);
        }

        try self.saveSegmentRange(&txn, new_seg_id, key_range);
        try self.updateActiveSegments(&txn, new_seg_id, .add);
        try txn.commit();

        writer_publication.publish();
        segment_data = null;
        return true;
    }

    fn replaceSegmentsCatalogOnlyForTest(self: *PersistentIndex, old_seg_ids: []const u64, segment_bytes: []const u8) !void {
        if (!builtin.is_test) return error.Unsupported;

        const new_seg_id = self.reserveSegmentId();
        var key_range = try extractSegmentKeyRange(self.alloc, segment_bytes);
        defer key_range.deinit(self.alloc);

        var segment_data: ?index_mod.SegmentData = try self.materializeSegmentData(new_seg_id, segment_bytes);
        defer if (segment_data) |*data| data.deinit(self.alloc);
        errdefer self.deleteSegmentFile(new_seg_id);

        var txn = try self.beginWriteMainTxn();
        errdefer txn.abort();
        try self.requireSourceHandoff(&txn, old_seg_ids);

        const new_seg_key = std.mem.toBytes(std.mem.nativeToBig(u64, new_seg_id));
        if (self.segment_files == null) try txn.put(.segments, &new_seg_key, segment_bytes);

        for (old_seg_ids) |old_id| {
            const old_seg_key = std.mem.toBytes(std.mem.nativeToBig(u64, old_id));
            try self.updateActiveSegments(&txn, old_id, .remove);
            txn.delete(.segments, &old_seg_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            txn.delete(.deletions, &old_seg_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            txn.delete(.meta, &fieldSegmentKey(old_id)) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            try self.deleteSegmentRange(&txn, old_id);
        }

        try self.saveSegmentRange(&txn, new_seg_id, key_range);
        try self.updateActiveSegments(&txn, new_seg_id, .add);
        try txn.commit();

        // Simulates a process crash after durable catalog publish but before
        // the volatile IndexWriter snapshot observes the replacement.
        segment_data.?.deinit(self.alloc);
        segment_data = null;
    }

    fn replaceSegmentsWithReservedIdsOwned(
        self: *PersistentIndex,
        scratch_alloc: Allocator,
        old_seg_ids: []const u64,
        new_seg_ids: []const u64,
        segment_bytes_list: [][]u8,
        replacement_deleted: ?[]const ?roaring.RoaringBitmap,
        require_active: bool,
        source: ?SourceReplacement,
    ) !bool {
        std.debug.assert(new_seg_ids.len == segment_bytes_list.len);
        defer freeOwnedSegmentList(scratch_alloc, segment_bytes_list);
        if (replacement_deleted) |deleted| {
            if (deleted.len != segment_bytes_list.len) return error.InvalidDeletionSnapshot;
        }

        var source_plan: ?exact_source_map.Plan = null;
        defer if (source_plan) |plan| plan.deinit(scratch_alloc);
        if (source) |handoff| {
            if (self.read_only) return error.ReadOnly;
            if (self.segment_files == null or self.active_rebuild_page != null or segment_bytes_list.len != 1) return error.UnsupportedSourcePublication;
            var digest: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(segment_bytes_list[0], &digest, .{});
            source_plan = try exact_source_map.Plan.prepare(scratch_alloc, handoff.binding, digest, handoff.references);
            var read = try self.beginReadMainTxn();
            defer read.abort();
            const owner = (try sourceOwnerInTxn(&read)) orelse return error.IndexSourceOwnerMismatch;
            if (!std.mem.eql(u8, &owner, &handoff.binding.owner)) return error.IndexSourceOwnerMismatch;
            var metadata: PublicationTxn = .{ .txn = &read };
            if (try source_publication.load(&metadata, handoff.binding)) |existing| {
                if (existing.state == .cancelled) return error.IndexPublicationCancelled;
                if (existing.state == .retired) return error.IndexPublicationRetired;
                if (!std.mem.eql(u8, &existing.root, &source_plan.?.root())) return error.IndexPublicationConflict;
                return true;
            }
        }

        var key_ranges = try scratch_alloc.alloc(SegmentKeyRange, segment_bytes_list.len);
        var key_ranges_initialized: usize = 0;
        defer {
            for (key_ranges[0..key_ranges_initialized]) |*range| range.deinit(scratch_alloc);
            scratch_alloc.free(key_ranges);
        }

        var replacements = try scratch_alloc.alloc(index_mod.ReplacementSegmentData, segment_bytes_list.len);
        var replacements_initialized: usize = 0;
        var published_to_writer = false;
        defer {
            if (!published_to_writer) {
                for (replacements[0..replacements_initialized]) |*replacement| {
                    replacement.data.deinit(self.alloc);
                    self.deleteSegmentFile(replacement.id);
                }
            }
            scratch_alloc.free(replacements);
        }

        for (segment_bytes_list, 0..) |segment_bytes, i| {
            key_ranges[i] = try extractSegmentKeyRange(scratch_alloc, segment_bytes);
            key_ranges[i].seg_id = new_seg_ids[i];
            key_ranges_initialized += 1;

            replacements[i] = .{
                .id = new_seg_ids[i],
                .data = try self.materializeSegmentData(new_seg_ids[i], segment_bytes),
                .deleted = if (replacement_deleted) |deleted|
                    if (deleted[i]) |*bitmap| bitmap else null
                else
                    null,
            };
            replacements_initialized += 1;
        }

        var txn = try self.beginWriteMainTxn();
        errdefer txn.abort();
        if (source == null) try self.requireSourceHandoff(&txn, old_seg_ids) else try requireSourceOwnerOpen(&txn);

        if (require_active) {
            const active_ids = try self.loadActiveSegmentIds(&txn, scratch_alloc);
            defer scratch_alloc.free(active_ids);
            for (old_seg_ids) |old_id| {
                if (!containsSegmentId(active_ids, old_id)) {
                    txn.abort();
                    return false;
                }
            }
        }

        var writer_publication = try self.writer.prepareSegmentsManyDataWithScratch(scratch_alloc, old_seg_ids, replacements);
        defer writer_publication.abort();
        if (source) |handoff| try self.attachSourceReferences(&writer_publication.snapshot.segments[writer_publication.carried_count], handoff.binding, handoff.references);

        for (segment_bytes_list, 0..) |segment_bytes, i| {
            const new_seg_key = std.mem.toBytes(std.mem.nativeToBig(u64, new_seg_ids[i]));
            if (self.segment_files == null) try txn.put(.segments, &new_seg_key, segment_bytes);
            if (replacement_deleted) |deleted| {
                if (deleted[i]) |*bitmap| {
                    if (!bitmap.isEmpty()) {
                        const bitmap_bytes = try bitmap.toBytes(scratch_alloc);
                        defer scratch_alloc.free(bitmap_bytes);
                        try txn.put(.deletions, &new_seg_key, bitmap_bytes);
                    }
                }
            }
        }

        for (old_seg_ids) |old_id| {
            const old_seg_key = std.mem.toBytes(std.mem.nativeToBig(u64, old_id));
            try self.updateActiveSegments(&txn, old_id, .remove);
            txn.delete(.segments, &old_seg_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            txn.delete(.deletions, &old_seg_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            txn.delete(.meta, &fieldSegmentKey(old_id)) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            try self.deleteSegmentRange(&txn, old_id);
        }

        for (key_ranges[0..key_ranges_initialized]) |key_range| {
            try self.saveSegmentRangeWithAllocator(scratch_alloc, &txn, key_range.seg_id, key_range);
            try self.updateActiveSegments(&txn, key_range.seg_id, .add);
        }
        if (source) |handoff| {
            var metadata: PublicationTxn = .{ .txn = &txn };
            if (!try source_publication.stagePublishInEpoch(scratch_alloc, &metadata, handoff.binding, source_plan.?.root(), new_seg_ids, try sourcePublicationEpoch(&txn, handoff.binding))) return error.IndexPublicationConflict;
            try stageSourceSegment(&txn, new_seg_ids[0], handoff.binding);
            try source_plan.?.stage(&metadata);
            try self.retireRemovedSourceRoots(&txn, old_seg_ids);
        }
        try saveNextSegmentId(&txn, self.writer.next_segment_id);
        try txn.commit();

        if (source != null) self.source_roots_present = true;
        writer_publication.publish();
        published_to_writer = true;
        if (source != null) self.noteSourceRecoveryWork();
        return true;
    }

    pub fn removeSegments(self: *PersistentIndex, old_seg_ids: []const u64) !void {
        if (old_seg_ids.len == 0) return;
        self.lockStorage();
        defer self.unlockStorage();
        try self.removeSegmentsLocked(old_seg_ids);
    }

    fn removeSegmentsLocked(self: *PersistentIndex, old_seg_ids: []const u64) !void {
        if (old_seg_ids.len == 0) return;

        var txn = try self.beginWriteMainTxn();
        errdefer txn.abort();
        var writer_publication = try self.writer.prepareSegmentsManyData(old_seg_ids, &.{});
        defer writer_publication.abort();
        // Unlike compaction replacement, structural removal is not a proof
        // that the old source cut is still represented by this generation.
        txn.delete(.meta, projection_seal.metadata_key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };

        for (old_seg_ids) |old_id| {
            const old_seg_key = std.mem.toBytes(std.mem.nativeToBig(u64, old_id));
            try self.updateActiveSegments(&txn, old_id, .remove);
            txn.delete(.segments, &old_seg_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            txn.delete(.deletions, &old_seg_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            txn.delete(.meta, &fieldSegmentKey(old_id)) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            try self.deleteSegmentRange(&txn, old_id);
        }

        try self.retireRemovedSourceRoots(&txn, old_seg_ids);
        try saveNextSegmentId(&txn, self.writer.next_segment_id);
        try txn.commit();
        writer_publication.publish();
        if (self.source_roots_present) self.noteSourceRecoveryWork();
    }

    fn retireRemovedSourceRoots(self: *PersistentIndex, txn: *MainTxn, old_seg_ids: []const u64) !void {
        if (!self.source_roots_present) return;
        for (old_seg_ids) |id| {
            const binding = (try sourceSegmentBindingForReader(txn, id, true)) orelse continue;
            var metadata: PublicationTxn = .{ .txn = txn };
            const record = (try source_publication.load(&metadata, binding)) orelse return error.InvalidIndexPublication;
            if (record.state == .retired) continue;
            const live = for (record.segments[0..record.count]) |segment_id| {
                _ = txn.get(.meta, &activeSegmentMetaKey(segment_id)) catch |err| switch (err) {
                    error.NotFound => continue,
                    else => return err,
                };
                break true;
            } else false;
            if (!live) {
                if (record.role == .family) {
                    const slot = source_family.slot(id);
                    const root = (try family_graph.loadRoot(&metadata, binding.owner, slot)) orelse return error.ImmutableGraphCorrupt;
                    if (!std.mem.eql(u8, &root.publication, &binding.publication) or !try family_graph.removeRoot(&metadata, binding.owner, slot, root)) return error.IndexPublicationConflict;
                }
                try source_publication.stageRetire(self.alloc, &metadata, binding, &.{});
            }
        }
    }

    const SourcePublication = struct { binding: source_publication.Binding, root: source_publication.Digest, source_map_plan: ?*const exact_source_map.Plan = null };

    fn persistSegment(self: *PersistentIndex, seg_id: u64, segment_bytes: []const u8, lsn: u64) !void {
        return self.persistSegmentWithPublication(seg_id, segment_bytes, lsn, null);
    }

    fn persistSegmentWithPublication(self: *PersistentIndex, seg_id: u64, segment_bytes: []const u8, lsn: u64, source: ?SourcePublication) !void {
        var key_range = try extractSegmentKeyRange(self.alloc, segment_bytes);
        defer key_range.deinit(self.alloc);

        var txn = try self.beginWriteMainTxn();
        errdefer txn.abort();

        const seg_key = std.mem.toBytes(std.mem.nativeToBig(u64, seg_id));
        if (self.segment_files == null) try txn.put(.segments, &seg_key, segment_bytes);

        try self.saveSegmentRange(&txn, seg_id, key_range);

        // Update active segments list
        try self.updateActiveSegments(&txn, seg_id, .add);

        // Update committed_lsn
        const lsn_bytes = std.mem.toBytes(std.mem.nativeToLittle(u64, lsn));
        try txn.put(.meta, meta_committed_lsn, &lsn_bytes);

        if (source) |publication| {
            try bindSourceOwner(&txn, publication.binding.owner);
            try requireSourceOwnerOpen(&txn);
            var metadata: PublicationTxn = .{ .txn = &txn };
            if (!try source_publication.stagePublishInEpoch(self.alloc, &metadata, publication.binding, publication.root, &.{seg_id}, try sourcePublicationEpoch(&txn, publication.binding))) return error.IndexPublicationConflict;
            try stageSourceSegment(&txn, seg_id, publication.binding);
            if (publication.source_map_plan) |plan| try plan.stage(&metadata);
        }
        try txn.commit();
        self.committed_lsn = lsn;
    }

    fn updateActiveSegments(self: *PersistentIndex, txn: *MainTxn, seg_id: u64, op: enum { add, remove }) !void {
        try self.clearCompletedRetirementMarkersInTxn(txn);
        const marker_key = activeSegmentMetaKey(seg_id);
        switch (op) {
            .add => try txn.put(.meta, &marker_key, &.{1}),
            .remove => {
                txn.delete(.meta, &marker_key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                if (self.segment_files != null) {
                    const retired_key = retiredSegmentMetaKey(seg_id);
                    try txn.put(.meta, &retired_key, &.{1});
                }
            },
        }
        txn.delete(.meta, meta_active_segments) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
    }

    fn saveNextSegmentId(txn: *MainTxn, next_segment_id: u64) !void {
        const next_segment_id_bytes = std.mem.toBytes(std.mem.nativeToLittle(u64, next_segment_id));
        try txn.put(.meta, meta_next_seg_id, &next_segment_id_bytes);
    }

    fn replayWalEntry(self: *PersistentIndex, lsn: u64, segment_bytes: []const u8) !void {
        const seg_id = self.writer.next_segment_id;
        var segment_data: ?index_mod.SegmentData = try self.materializeSegmentData(seg_id, segment_bytes);
        var rollback_segment = true;
        errdefer {
            if (rollback_segment) {
                if (segment_data) |*data| data.deinit(self.alloc);
                self.deleteSegmentFile(seg_id);
            }
        }
        var replacement = [_]index_mod.ReplacementSegmentData{.{
            .id = seg_id,
            .data = segment_data.?,
        }};
        var writer_publication = try self.writer.prepareSegmentsManyData(&.{}, &replacement);
        defer writer_publication.abort();
        try self.persistSegment(seg_id, segment_bytes, lsn);
        writer_publication.publish();
        segment_data = null;
        rollback_segment = false;
        try self.wal.truncate(lsn);
    }

    fn saveSegmentRange(
        self: *PersistentIndex,
        txn: *MainTxn,
        seg_id: u64,
        key_range: SegmentKeyRange,
    ) !void {
        return try self.saveSegmentRangeWithAllocator(self.alloc, txn, seg_id, key_range);
    }

    fn saveSegmentRangeWithAllocator(
        self: *PersistentIndex,
        alloc: Allocator,
        txn: *MainTxn,
        seg_id: u64,
        key_range: SegmentKeyRange,
    ) !void {
        _ = self;
        const range_key = segmentRangeMetaKey(seg_id);
        var buf = try alloc.alloc(u8, 8 + key_range.min_doc_key.len + key_range.max_doc_key.len);
        defer alloc.free(buf);
        buf[0..4].* = @bitCast(@as(u32, @as(u32, @intCast(key_range.min_doc_key.len))));
        @memcpy(buf[4..][0..key_range.min_doc_key.len], key_range.min_doc_key);
        const max_off = 4 + key_range.min_doc_key.len;
        buf[max_off..][0..4].* = @bitCast(@as(u32, @as(u32, @intCast(key_range.max_doc_key.len))));
        @memcpy(buf[max_off + 4 ..][0..key_range.max_doc_key.len], key_range.max_doc_key);
        try txn.put(.meta, &range_key, buf);
    }

    fn deleteSegmentRange(self: *PersistentIndex, txn: *MainTxn, seg_id: u64) !void {
        _ = self;
        const range_key = segmentRangeMetaKey(seg_id);
        txn.delete(.meta, &range_key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
    }

    fn loadActiveSegmentIds(
        self: *PersistentIndex,
        txn: *MainTxn,
        alloc: Allocator,
    ) ![]u64 {
        _ = self;
        return try loadActiveSegmentIdsFromTxn(txn, alloc);
    }

    fn loadSegmentRange(
        self: *PersistentIndex,
        txn: *MainTxn,
        alloc: Allocator,
        seg_id: u64,
    ) !SegmentKeyRange {
        const range_key = segmentRangeMetaKey(seg_id);
        const encoded = txn.get(.meta, &range_key) catch |err| switch (err) {
            error.NotFound => {
                const seg_key = std.mem.toBytes(std.mem.nativeToBig(u64, seg_id));
                if (txn.get(.segments, &seg_key)) |segment_bytes| {
                    var rebuilt = try extractSegmentKeyRange(alloc, segment_bytes);
                    rebuilt.seg_id = seg_id;
                    return rebuilt;
                } else |segment_err| switch (segment_err) {
                    error.NotFound => {
                        if (self.segment_files) |*store| {
                            if (comptime builtin.os.tag == .freestanding) {
                                return error.NotFound;
                            }

                            const segment_path = try store.pathAlloc(seg_id);
                            defer self.alloc.free(segment_path);

                            if (store.storage.vtable.open_immutable_source != null) {
                                var source = store.storage.openImmutableSource(alloc, segment_path) catch |source_err| switch (source_err) {
                                    error.FileNotFound => return error.NotFound,
                                    else => return source_err,
                                };
                                var owns_source = true;
                                defer if (owns_source) source.close();
                                if (try segment_mod.RangeSegmentReader.supports(source)) {
                                    var reader = try segment_mod.RangeSegmentReader.init(alloc, source, .{ .key_range_only = true });
                                    owns_source = false;
                                    defer reader.deinit();
                                    if (try reader.docKeyRangeOwned(alloc, std.math.maxInt(usize))) |range| {
                                        return .{ .seg_id = seg_id, .min_doc_key = range.min_key, .max_doc_key = range.max_key };
                                    }
                                    var min_key: ?[]u8 = null;
                                    errdefer if (min_key) |key| alloc.free(key);
                                    var max_key: ?[]u8 = null;
                                    errdefer if (max_key) |key| alloc.free(key);
                                    var scope = try segment_mod.RangeSegmentReader.ReadScope.init(alloc, &reader, 256 * 1024);
                                    defer scope.deinit();
                                    for (0..reader.doc_count) |doc| {
                                        const id = (try scope.storedDocIdOwned(alloc, @intCast(doc), std.math.maxInt(usize))) orelse continue;
                                        defer alloc.free(id);
                                        if (min_key == null or std.mem.order(u8, id, min_key.?) == .lt) {
                                            const next = try alloc.dupe(u8, id);
                                            if (min_key) |key| alloc.free(key);
                                            min_key = next;
                                        }
                                        if (max_key == null or std.mem.order(u8, id, max_key.?) == .gt) {
                                            const next = try alloc.dupe(u8, id);
                                            if (max_key) |key| alloc.free(key);
                                            max_key = next;
                                        }
                                    }
                                    if (min_key == null or max_key == null) return error.EmptySegment;
                                    return .{ .seg_id = seg_id, .min_doc_key = min_key.?, .max_doc_key = max_key.? };
                                }
                            }

                            const file_bytes = store.storage.readFileAlloc(
                                alloc,
                                segment_path,
                                std.math.maxInt(usize),
                            ) catch |file_err| switch (file_err) {
                                error.FileNotFound => return error.NotFound,
                                else => return file_err,
                            };
                            defer alloc.free(file_bytes);

                            var rebuilt = try extractSegmentKeyRange(alloc, file_bytes);
                            rebuilt.seg_id = seg_id;
                            return rebuilt;
                        }
                        return error.NotFound;
                    },
                    else => return segment_err,
                }
            },
            else => return err,
        };
        return try decodeSegmentRange(alloc, seg_id, encoded);
    }
};

fn loadActiveSegmentIdsFromTxn(txn: *PersistentIndex.MainTxn, alloc: Allocator) ![]u64 {
    return try loadSegmentIdsForMetaPrefix(txn, alloc, meta_active_segment_prefix);
}

fn loadRetiredSegmentIdsFromTxn(txn: *PersistentIndex.MainTxn, alloc: Allocator) ![]u64 {
    return try loadSegmentIdsForMetaPrefix(txn, alloc, meta_retired_segment_prefix);
}

fn loadSegmentIdsForMetaPrefix(txn: *PersistentIndex.MainTxn, alloc: Allocator, prefix: []const u8) ![]u64 {
    var cursor = txn.openCursor(.meta) catch |err| switch (err) {
        error.NotFound => return try alloc.alloc(u64, 0),
        else => return err,
    };
    defer cursor.close();

    var seg_ids = std.ArrayListUnmanaged(u64).empty;
    errdefer seg_ids.deinit(alloc);

    var entry = try cursor.seekAtOrAfter(prefix);
    while (entry) |item| : (entry = try cursor.next()) {
        if (!std.mem.startsWith(u8, item.key, prefix)) break;
        if (item.key.len != prefix.len + 8) continue;
        try seg_ids.append(alloc, std.mem.readInt(u64, item.key[prefix.len..][0..8], .big));
    }
    return try seg_ids.toOwnedSlice(alloc);
}

const field_inventory_prefix = "field_inventory:";
fn fieldInventoryKey(ordinal: source_publication.Digest) [field_inventory_prefix.len + 32]u8 {
    return field_inventory_prefix.* ++ ordinal;
}
const field_preparation_prefix = "field_preparation:";
const field_segment_prefix = "field_segment:";
fn fieldPreparationKey(id: u64) [field_preparation_prefix.len + 8]u8 {
    return field_preparation_prefix.* ++ std.mem.toBytes(std.mem.nativeToBig(u64, id));
}
fn fieldSegmentKey(id: u64) [field_segment_prefix.len + 8]u8 {
    return field_segment_prefix.* ++ std.mem.toBytes(std.mem.nativeToBig(u64, id));
}
fn encodeFieldBinding(binding: source_publication.Binding) ![100]u8 {
    if (!binding.valid()) return error.InvalidIndexPublication;
    var raw: [100]u8 = undefined;
    @memcpy(raw[0..4], "IFC1");
    @memcpy(raw[4..36], &binding.owner);
    @memcpy(raw[36..68], &binding.publication);
    std.crypto.hash.sha2.Sha256.hash(raw[0..68], raw[68..100], .{});
    return raw;
}
fn decodeFieldBinding(raw: []const u8) !source_publication.Binding {
    if (raw.len != 100 or !std.mem.eql(u8, raw[0..4], "IFC1")) return error.InvalidIndexFieldGeneration;
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(raw[0..68], &digest, .{});
    if (!std.mem.eql(u8, &digest, raw[68..100])) return error.InvalidIndexFieldGeneration;
    const binding: source_publication.Binding = .{ .owner = raw[4..36].*, .publication = raw[36..68].* };
    if (!binding.valid()) return error.InvalidIndexPublication;
    return binding;
}

fn segmentRangeMetaKey(seg_id: u64) [meta_segment_range_prefix.len + 8]u8 {
    var key: [meta_segment_range_prefix.len + 8]u8 = undefined;
    @memcpy(key[0..meta_segment_range_prefix.len], meta_segment_range_prefix);
    key[meta_segment_range_prefix.len..][0..8].* = std.mem.toBytes(std.mem.nativeToBig(u64, seg_id));
    return key;
}

fn activeSegmentMetaKey(seg_id: u64) [meta_active_segment_prefix.len + 8]u8 {
    var key: [meta_active_segment_prefix.len + 8]u8 = undefined;
    @memcpy(key[0..meta_active_segment_prefix.len], meta_active_segment_prefix);
    key[meta_active_segment_prefix.len..][0..8].* = std.mem.toBytes(std.mem.nativeToBig(u64, seg_id));
    return key;
}

fn retiredSegmentMetaKey(seg_id: u64) [meta_retired_segment_prefix.len + 8]u8 {
    var key: [meta_retired_segment_prefix.len + 8]u8 = undefined;
    @memcpy(key[0..meta_retired_segment_prefix.len], meta_retired_segment_prefix);
    key[meta_retired_segment_prefix.len..][0..8].* = std.mem.toBytes(std.mem.nativeToBig(u64, seg_id));
    return key;
}

fn isStaleActiveSegmentDataError(err: anyerror) bool {
    return switch (err) {
        error.CorruptInput,
        error.CrcMismatch,
        error.InvalidMagic,
        error.InvalidSegment,
        error.UnsupportedVersion,
        => true,
        else => false,
    };
}

fn extractSegmentKeyRange(alloc: Allocator, segment_bytes: []const u8) !SegmentKeyRange {
    return extractSegmentDataKeyRange(alloc, .{ .heap = @constCast(segment_bytes) });
}

fn extractSegmentDataKeyRange(alloc: Allocator, data: index_mod.SegmentData) !SegmentKeyRange {
    var reader = try data.initReader(alloc);
    defer reader.deinit();
    return extractReaderKeyRange(alloc, &reader);
}
fn extractReaderKeyRange(alloc: Allocator, reader: *const segment_mod.SegmentReader) !SegmentKeyRange {
    if (reader.doc_count == 0) return error.EmptySegment;

    if (try reader.storedFieldsOmitted()) {
        const range = (try reader.docKeyRange()) orelse return error.InvalidSegment;
        const min_doc_key = try alloc.dupe(u8, range.min_key);
        errdefer alloc.free(min_doc_key);
        const max_doc_key = try alloc.dupe(u8, range.max_key);
        return .{
            .seg_id = 0,
            .min_doc_key = min_doc_key,
            .max_doc_key = max_doc_key,
        };
    }

    var min_key: ?[]u8 = null;
    errdefer if (min_key) |key| alloc.free(key);
    var max_key: ?[]u8 = null;
    errdefer if (max_key) |key| alloc.free(key);

    var identity_scratch = segment_mod.SegmentReadScratch.init(alloc, 64 * 1024);
    defer identity_scratch.deinit();
    for (0..reader.doc_count) |doc_idx| {
        identity_scratch.reset();
        const stored = .{ .id = (try reader.storedIdScoped(identity_scratch.allocator(), @intCast(doc_idx))) orelse continue };
        if (min_key == null or std.mem.order(u8, stored.id, min_key.?) == .lt) {
            if (min_key) |key| alloc.free(key);
            min_key = try alloc.dupe(u8, stored.id);
        }
        if (max_key == null or std.mem.order(u8, stored.id, max_key.?) == .gt) {
            if (max_key) |key| alloc.free(key);
            max_key = try alloc.dupe(u8, stored.id);
        }
    }

    if (min_key == null or max_key == null) return error.EmptySegment;
    return .{
        .seg_id = 0,
        .min_doc_key = min_key.?,
        .max_doc_key = max_key.?,
    };
}

fn decodeSegmentRange(alloc: Allocator, seg_id: u64, encoded: []const u8) !SegmentKeyRange {
    if (encoded.len < 8) return error.CorruptEnvironment;
    const min_len = std.mem.readInt(u32, encoded[0..4], .little);
    const min_end = 4 + min_len;
    if (min_end + 4 > encoded.len) return error.CorruptEnvironment;
    const max_len = std.mem.readInt(u32, encoded[min_end..][0..4], .little);
    const max_start = min_end + 4;
    const max_end = max_start + max_len;
    if (max_end > encoded.len) return error.CorruptEnvironment;

    return .{
        .seg_id = seg_id,
        .min_doc_key = try alloc.dupe(u8, encoded[4..min_end]),
        .max_doc_key = try alloc.dupe(u8, encoded[max_start..max_end]),
    };
}

fn classifySegmentRange(min_doc_key: []const u8, max_doc_key: []const u8, split_key: []const u8) SegmentSplitClass {
    if (std.mem.order(u8, max_doc_key, split_key) == .lt) return .left_only;
    if (std.mem.order(u8, min_doc_key, split_key) != .lt) return .right_only;
    return .mixed;
}

fn containsSegmentId(segment_ids: []const u64, target: u64) bool {
    for (segment_ids) |seg_id| {
        if (seg_id == target) return true;
    }
    return false;
}

fn freeOwnedSegmentList(alloc: Allocator, segments: [][]u8) void {
    for (segments) |segment| alloc.free(segment);
    if (segments.len > 0) alloc.free(segments);
}

fn mapSegmentFile(path: []const u8) anyerror![]align(std.heap.page_size_min) u8 {
    if (builtin.os.tag == .freestanding or builtin.os.tag == .windows or builtin.os.tag == .wasi) {
        return error.UnsupportedPlatform;
    }

    const fd = try std.posix.openat(std.posix.AT.FDCWD, path, .{
        .ACCMODE = .RDONLY,
        .CLOEXEC = true,
    }, 0);
    defer _ = std.posix.system.close(fd);

    const len = try fileSizeFromFd(fd);
    if (len == 0) return error.EmptySegment;

    return try std.posix.mmap(
        null,
        len,
        .{ .READ = true },
        .{ .TYPE = .SHARED },
        fd,
        0,
    );
}

fn fileSizeFromFd(fd: std.posix.fd_t) !usize {
    if (builtin.os.tag == .linux) {
        const linux = std.os.linux;
        const empty_path: [*:0]const u8 = "";
        while (true) {
            var statx = std.mem.zeroes(linux.Statx);
            switch (linux.errno(linux.statx(fd, empty_path, linux.AT.EMPTY_PATH, .{ .SIZE = true }, &statx))) {
                .SUCCESS => {
                    if (!statx.mask.SIZE) return error.Unexpected;
                    return @intCast(statx.size);
                },
                .INTR => continue,
                else => return error.Unexpected,
            }
        }
    } else {
        var stat: std.posix.Stat = undefined;
        while (true) {
            const rc = std.posix.system.fstat(fd, &stat);
            switch (std.posix.errno(rc)) {
                .SUCCESS => return @intCast(stat.size),
                .INTR => continue,
                else => return error.Unexpected,
            }
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

const scorer_mod = @import("../search/scorer.zig");

fn buildSimpleSegment(alloc: Allocator, doc_id: []const u8, term: []const u8) ![]u8 {
    var inv_builder = inverted_mod.InvertedIndexBuilder.init(alloc, .{});
    defer inv_builder.deinit();
    try inv_builder.addDocument(0, &.{.{ .term = term, .freq = 1, .norm = 10 }});
    const inv_data = try inv_builder.build();
    defer alloc.free(inv_data);

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();
    const field_idx = try seg_writer.addField("body");
    try seg_writer.addSection(field_idx, .inverted_text, inv_data);
    try seg_writer.addStoredDoc(doc_id, "{}");

    return seg_writer.build();
}

test "tracked empty deletion returns before touching index storage" {
    const alloc = std.testing.allocator;
    var index: PersistentIndex = undefined;
    index.alloc = alloc;

    const delete_infos = try index.deleteByIdsTracked(&.{});
    defer index.freeDeleteInfos(delete_infos);
    try std.testing.expectEqual(@as(usize, 0), delete_infos.len);
}

fn buildMultiDocSegment(alloc: Allocator, docs: []const struct { doc_id: []const u8, term: []const u8 }) ![]u8 {
    var inv_builder = inverted_mod.InvertedIndexBuilder.init(alloc, .{});
    defer inv_builder.deinit();
    for (docs, 0..) |doc, i| {
        try inv_builder.addDocument(@intCast(i), &.{.{ .term = doc.term, .freq = 1, .norm = 10 }});
    }
    const inv_data = try inv_builder.build();
    defer alloc.free(inv_data);

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();
    const field_idx = try seg_writer.addField("body");
    try seg_writer.addSection(field_idx, .inverted_text, inv_data);
    for (docs) |doc| {
        try seg_writer.addStoredDoc(doc.doc_id, "{}");
    }
    return seg_writer.build();
}

fn buildHighFrequencyKeywordSegmentRange(alloc: Allocator, start: usize, count: usize) ![]u8 {
    const published_count = 4_237;
    var body_builder = inverted_mod.InvertedIndexBuilder.init(alloc, .{});
    defer body_builder.deinit();
    var state_builder = inverted_mod.InvertedIndexBuilder.init(alloc, .{});
    defer state_builder.deinit();

    for (0..count) |local_doc| {
        const doc_num: u32 = @intCast(local_doc);
        const i = start + local_doc;
        try body_builder.addDocument(doc_num, &.{.{ .term = "document", .freq = 1, .norm = 10 }});
        try state_builder.addDocument(doc_num, &.{.{
            .term = if (i < published_count) "published" else "draft",
            .freq = 1,
            .norm = 10,
        }});
    }
    const body_data = try body_builder.build();
    defer alloc.free(body_data);
    const state_data = try state_builder.build();
    defer alloc.free(state_data);

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();
    const body_field = try seg_writer.addField("body");
    try seg_writer.addSection(body_field, .inverted_text, body_data);
    const state_field = try seg_writer.addField("state");
    try seg_writer.addSection(state_field, .inverted_text, state_data);
    var doc_id_buf: [32]u8 = undefined;
    for (0..count) |local_doc| {
        const i = start + local_doc;
        const doc_id = try std.fmt.bufPrint(&doc_id_buf, "doc:{d}", .{i});
        try seg_writer.addStoredDoc(doc_id, "{}");
    }
    return seg_writer.build();
}

fn buildHighFrequencyKeywordSegment(alloc: Allocator) ![]u8 {
    return buildHighFrequencyKeywordSegmentRange(alloc, 0, 5_000);
}

var persist_tmp_nonce: u64 = 0;

fn persistTmpPath(buf: []u8) [*:0]const u8 {
    const base = "/tmp/antfly-persist-test-";
    var random: [16]u8 = undefined;
    std.testing.io.random(&random);
    const nonce = @atomicRmw(u64, &persist_tmp_nonce, .Add, 1, .monotonic);
    const slice = std.fmt.bufPrint(buf, "{s}{x}-{d}\x00", .{ base, random, nonce }) catch unreachable;
    return @ptrCast(slice.ptr);
}

fn cleanupPersistDir(path: [*:0]const u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}

test "ordered artifact inventory physical projection seal survives reopen and retires with its generation" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    defer cleanupPersistDir(path);
    const options: PersistentIndexOptions = .{ .path = path, .main_backend = .lsm, .wal_backend = .lsm };
    var seal: projection_seal.Seal = .{ .root = 1, .namespace = @splat(1), .generation = 2, .config_hash = 3, .applied_sequence = 4 };
    {
        var idx = try PersistentIndex.open(alloc, options);
        defer idx.close();
        const segment = try buildSimpleSegment(alloc, "doc", "durable");
        defer alloc.free(segment);
        try idx.indexSegment(segment);
        try idx.publishProjectionSeal(seal);
        try std.testing.expectEqualDeep(seal, (try idx.loadProjectionSeal(alloc)).?);
        var old = seal;
        old.applied_sequence -= 1;
        try idx.publishProjectionSeal(old);
        try std.testing.expectEqualDeep(seal, (try idx.loadProjectionSeal(alloc)).?);
        var foreign = seal;
        foreign.root += 1;
        try std.testing.expectError(error.ProjectionSealIdentityChanged, idx.publishProjectionSeal(foreign));
        // A completed snapshot can strengthen an already durable replay cut.
        seal.baseline = .{ .boundary = .{ .authority = .{ .namespace = seal.namespace, .epoch = 1, .catalog_digest = @splat(2) }, .replay_sequence = 2 }, .gap_epoch = 0 };
        try idx.publishProjectionSeal(seal);
        try std.testing.expectEqualDeep(seal, (try idx.loadProjectionSeal(alloc)).?);
        seal.applied_sequence += 1;
        var incremental = seal;
        incremental.baseline = null;
        try idx.publishProjectionSeal(incremental);
    }
    {
        var idx = try PersistentIndex.open(alloc, options);
        defer idx.close();
        try std.testing.expectEqualDeep(seal, (try idx.loadProjectionSeal(alloc)).?);
        try std.testing.expectEqual(@as(u32, 1), idx.snapshot().liveDocCount());
        const pinned = idx.acquireSnapshot();
        defer pinned.release();
        try idx.resetAllForRebuild();
        try std.testing.expect(try idx.loadProjectionSeal(alloc) == null);
        try std.testing.expectEqual(@as(u32, 1), pinned.liveDocCount());
        try std.testing.expectEqual(@as(u32, 0), idx.snapshot().liveDocCount());
        try idx.sync(true);
    }
    {
        var idx = try PersistentIndex.open(alloc, options);
        defer idx.close();
        try std.testing.expect(try idx.loadProjectionSeal(alloc) == null);
        const segment = try buildSimpleSegment(alloc, "new", "rebuilt");
        defer alloc.free(segment);
        try idx.indexSegment(segment);
        seal.generation += 1;
        try idx.publishProjectionSeal(seal);
        const id = idx.snapshot().segments[0].id;
        try idx.removeSegments(&.{id});
        try std.testing.expect(try idx.loadProjectionSeal(alloc) == null);
    }
}

test "ordered artifact inventory physical projection seal and data remain coherent across modeled crash" {
    const alloc = std.testing.allocator;
    var runtime = storage_sim.Runtime.init(alloc);
    defer runtime.deinit();
    var device_model = storage_sim.ModeledDevice.init(alloc);
    defer device_model.deinit();
    const path: [*:0]const u8 = "/physical-projection-seal-crash";
    const options = persistentModeledOptionsToIndexOptions(path, .{}, &device_model, &runtime);
    var idx = try PersistentIndex.open(alloc, options);
    defer idx.close();
    const segment = try buildSimpleSegment(alloc, "doc", "durable");
    defer alloc.free(segment);
    try idx.indexSegment(segment);
    const seal: projection_seal.Seal = .{ .root = 1, .namespace = @splat(1), .generation = 2, .config_hash = 3, .applied_sequence = 4 };
    try idx.publishProjectionSeal(seal);
    try crashReopenModeledPersistentIndex(alloc, &idx, &device_model, options);
    try std.testing.expectEqualDeep(seal, (try idx.loadProjectionSeal(alloc)).?);
    try std.testing.expectEqual(@as(usize, 1), try persistentSearchHitCount(alloc, idx.snapshot(), "durable"));
    // Failed reset commit must preserve BOTH the physical rows and their
    // certificate. The deletion is not a separate best-effort metadata write.
    try device_model.injectSyncFailureForPathContains("/index/");
    try expectModeledPersistentReplaceError(idx.resetAllForRebuild());
    try crashReopenModeledPersistentIndex(alloc, &idx, &device_model, options);
    try std.testing.expectEqualDeep(seal, (try idx.loadProjectionSeal(alloc)).?);
    try std.testing.expectEqual(@as(u32, 1), idx.snapshot().liveDocCount());
    try idx.resetAllForRebuild();
    try crashReopenModeledPersistentIndex(alloc, &idx, &device_model, options);
    try std.testing.expect(try idx.loadProjectionSeal(alloc) == null);
    try std.testing.expectEqual(@as(u32, 0), idx.snapshot().liveDocCount());
}

test "persistent independent indexes publish while another owner is locked" {
    const alloc = std.testing.allocator;
    var first_path_buf: [256]u8 = undefined;
    const first_path = persistTmpPath(&first_path_buf);
    defer cleanupPersistDir(first_path);
    var second_path_buf: [256]u8 = undefined;
    const second_path = persistTmpPath(&second_path_buf);
    defer cleanupPersistDir(second_path);
    var first = try PersistentIndex.open(alloc, .{ .path = first_path });
    defer first.close();

    // A contended second owner releases the first so a regression fails an
    // assertion instead of hanging. Neither open nor publication may wait.
    const Probe = struct {
        first: *PersistentIndex,
        first_locked: bool = true,
        waits: usize = 0,

        fn wait(ptr: ?*anyopaque, _: *const u32, _: u32) void {
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            self.waits += 1;
            if (self.first_locked) {
                self.first_locked = false;
                self.first.unlockStorage();
            }
        }

        fn wake(_: ?*anyopaque, _: *const u32, _: u32) void {}
    };
    var probe = Probe{ .first = &first };
    var vtable = std.testing.io.vtable.*;
    vtable.futexWaitUncancelable = Probe.wait;
    vtable.futexWake = Probe.wake;
    const io: std.Io = .{ .userdata = &probe, .vtable = &vtable };
    first.lockStorage();
    defer if (probe.first_locked) first.unlockStorage();
    var second = try PersistentIndex.open(alloc, .{ .path = second_path, .io = io });
    defer second.close();
    const segment = try buildSimpleSegment(alloc, "doc1", "hello");
    defer alloc.free(segment);
    try second.indexSegment(segment);
    try second.writeGenerationMetadata("independent", "published");
    const value = (try second.readGenerationMetadataAlloc(alloc, "independent")).?;
    defer alloc.free(value);
    try std.testing.expectEqualStrings("published", value);
    try std.testing.expectEqual(@as(usize, 0), probe.waits);
    try std.testing.expect(probe.first_locked);
}

test "persistent source publication owner closure fences live preparations across reopen" {
    const a = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    defer cleanupPersistDir(path);
    const binding: source_publication.Binding = .{ .owner = @splat(41), .publication = @splat(42) };
    const pending: source_publication.Binding = .{ .owner = binding.owner, .publication = @splat(43) };
    const fresh: source_publication.Binding = .{ .owner = binding.owner, .publication = @splat(44) };
    const bytes = try buildSimpleSegment(a, "doc1", "exact source");
    defer a.free(bytes);
    {
        var index = try PersistentIndex.open(a, .{ .path = path });
        defer index.close();
        try index.reserveSourceOwner(binding.owner);
        var preparation = try index.beginSourcePublication(pending);
        var preparation_open = true;
        defer if (preparation_open) preparation.deinit();
        _ = try index.indexSegmentOwnedWithSources(try a.dupe(u8, bytes), binding);
        const held = index.writer.acquireSnapshot();
        defer held.release();
        try std.testing.expectError(error.IndexSourceOwnerMismatch, index.closeSourceOwner(@splat(99)));
        try index.closeSourceOwner(binding.owner);
        try index.closeSourceOwner(binding.owner);
        try std.testing.expectError(error.IndexSourceOwnerClosed, index.reserveSourceOwner(binding.owner));
        try std.testing.expectError(error.IndexSourceOwnerClosed, index.beginSourcePublication(fresh));
        // The publisher already held a preparation before closure. It still
        // cannot install a new root, while an existing receipt stays retryable.
        try std.testing.expectError(error.IndexSourceOwnerClosed, index.indexSegmentOwnedWithSources(try a.dupe(u8, bytes), pending));
        _ = try index.indexSegmentOwnedWithSources(try a.dupe(u8, bytes), binding);
        try std.testing.expectEqual(@as(usize, 1), index.snapshot().segments.len);
        try std.testing.expectError(error.IndexPublicationInFlight, index.cancelSourcePublicationIfAbandoned(pending));
        preparation.deinit();
        preparation_open = false;
        try (try index.cancelSourcePublicationIfAbandoned(pending)).require(pending, .cancelled);
        try index.removeSegments(&.{held.segments[0].id});
        var retired = try index.retireSourcePublicationWithReaders(binding);
        defer retired.deinit();
        try std.testing.expect(retired.drained() == null);
        try std.testing.expectEqual(@as(u32, 1), held.liveDocCount());
    }
    {
        try std.testing.expectError(error.IndexSourceOwnerClosed, PersistentIndex.open(a, .{ .path = path }));
        var cleanup = try PersistentIndex.SourceOwnerCleanup.open(a, .{ .path = path }, binding.owner);
        defer cleanup.deinit();
        const index = &cleanup.index;
        try std.testing.expectEqual(@as(usize, 0), index.snapshot().segments.len);
        try std.testing.expect(index.segment_files == null);
        try index.closeSourceOwner(binding.owner);
        try std.testing.expectError(error.IndexSourceOwnerClosed, index.reserveSourceOwner(binding.owner));
        try std.testing.expectError(error.IndexSourceOwnerClosed, index.beginSourcePublication(fresh));
        try std.testing.expectError(error.UnsupportedSourcePublication, index.indexSegmentOwnedWithSources(try a.dupe(u8, bytes), fresh));
        try (try index.loadDurableSourcePublication(binding)).?.require(binding, .retired);
        try (try index.loadDurableSourcePublication(pending)).?.require(pending, .cancelled);
        var retired = try index.retireSourcePublicationWithReaders(binding);
        defer retired.deinit();
        try std.testing.expect(retired.drained() != null);
    }
}

test "persistent source publication fences retries cancellation and retirement across reopen" {
    const a = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    defer cleanupPersistDir(path);
    const binding: source_publication.Binding = .{ .owner = @splat(1), .publication = @splat(2) };
    const cancelled_binding: source_publication.Binding = .{ .owner = @splat(1), .publication = @splat(3) };
    const segment = try buildSimpleSegment(a, "doc1", "hello");
    defer a.free(segment);
    var root: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(segment, &root, .{});
    {
        var index = try PersistentIndex.open(a, .{ .path = path });
        defer index.close();
        try index.reserveSourceOwner(binding.owner);
        try std.testing.expectEqualSlices(u8, &binding.owner, &(try index.sourceOwner()).?);
        try std.testing.expectError(error.IndexSourceOwnerMismatch, index.reserveSourceOwner(@splat(99)));
        const foreign: source_publication.Binding = .{ .owner = @splat(99), .publication = @splat(100) };
        try std.testing.expectError(error.IndexSourceOwnerMismatch, index.cancelSourcePublication(foreign));
        try std.testing.expectError(error.IndexSourceOwnerMismatch, index.indexSegmentOwnedWithSources(try a.dupe(u8, segment), foreign));
        const published = try index.indexSegmentOwnedWithSources(try a.dupe(u8, segment), binding);
        try published.require(binding, .published);
        try std.testing.expectEqualSlices(u8, &root, &published.root);
        _ = try index.indexSegmentOwnedWithSources(try a.dupe(u8, segment), binding);
        try std.testing.expectEqual(@as(usize, 1), index.snapshot().segments.len);
        try std.testing.expectError(error.IndexPublicationAlreadyCommitted, index.cancelSourcePublication(binding));
        try std.testing.expectError(error.IndexPublicationStillActive, index.retireSourcePublication(binding));
        const other = try buildSimpleSegment(a, "other", "different");
        try std.testing.expectError(error.IndexPublicationConflict, index.indexSegmentOwnedWithSources(other, binding));
        const cancelled = try index.cancelSourcePublication(cancelled_binding);
        try cancelled.require(cancelled_binding, .cancelled);
        try std.testing.expectError(error.IndexPublicationCancelled, index.indexSegmentOwnedWithSources(try a.dupe(u8, segment), cancelled_binding));
    }
    {
        var index = try PersistentIndex.open(a, .{ .path = path });
        defer index.close();
        try std.testing.expectEqualSlices(u8, &binding.owner, &(try index.sourceOwner()).?);
        const published = (try index.loadDurableSourcePublication(binding)).?;
        try published.require(binding, .published);
        try std.testing.expectEqualSlices(u8, &root, &published.root);
        _ = try index.indexSegmentOwnedWithSources(try a.dupe(u8, segment), binding);
        try std.testing.expectEqual(@as(usize, 1), index.snapshot().segments.len);
        try std.testing.expectError(error.IndexPublicationCancelled, index.indexSegmentOwnedWithSources(try a.dupe(u8, segment), cancelled_binding));
        // A held snapshot still owns the old source after catalog retirement.
        // The decision alone deliberately cannot prove reader drainage.
        const held = index.writer.acquireSnapshot();
        defer held.release();
        const id = held.segments[0].id;
        try index.removeSegments(&.{id});
        const retired = try index.retireSourcePublication(binding);
        try retired.require(binding, .retired);
        try std.testing.expectEqual(@as(u32, 1), held.liveDocCount());
        try std.testing.expectError(error.IndexPublicationRetired, index.indexSegmentOwnedWithSources(try a.dupe(u8, segment), binding));
    }
    {
        var index = try PersistentIndex.open(a, .{ .path = path });
        defer index.close();
        try (try index.loadDurableSourcePublication(binding)).?.require(binding, .retired);
        try (try index.loadDurableSourcePublication(cancelled_binding)).?.require(cancelled_binding, .cancelled);
        try std.testing.expectEqual(@as(usize, 0), index.snapshot().segments.len);
    }
}

test "persistent source publication readers survive close reopen and retired query clones" {
    const a = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    defer cleanupPersistDir(path);
    const binding: source_publication.Binding = .{ .owner = @splat(11), .publication = @splat(12) };
    const keys = @import("internal_keys.zig");
    const physical = try keys.documentKeyAlloc(a, "doc");
    defer a.free(physical);
    var document_digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(physical, &document_digest, .{});
    const ref: source_claims.Reference = .{ .binding = binding, .document = document_digest, .revision = .{ .domain = .mutation, .value = 1 }, .root = @splat(14), .snapshot = @splat(15) };
    var old: ?*index_mod.IndexSnapshot = null;
    defer if (old) |snapshot| snapshot.release();
    {
        var index = try PersistentIndex.open(a, .{ .path = path });
        defer index.close();
        _ = try index.indexSegmentOwnedWithSourceMap(try buildSimpleSegment(a, "doc", "hello"), binding, &.{ref});
        old = index.writer.acquireSnapshot();
    }
    {
        var index = try PersistentIndex.open(a, .{ .path = path });
        defer index.close();
        var current: ?*index_mod.IndexSnapshot = index.writer.acquireSnapshot();
        defer if (current) |snapshot| snapshot.release();
        var query = (try old.?.segments[0].sourceLease()).?;
        defer query.deinit();
        // A reopened physical segment joins the original reader cell.
        var newer_query = (try current.?.segments[0].sourceLease()).?;
        defer newer_query.deinit();
        try index.removeSegments(&.{current.?.segments[0].id});
        var retirement = try index.retireSourcePublicationWithReaders(binding);
        defer retirement.deinit();
        try std.testing.expect(retirement.drained() == null);
        {
            // Even if a caller catches a bad borrowed map, no earlier index
            // metadata in that compound transaction can escape rollback.
            var borrowed = [_]source_claims.Reference{ref};
            const plan = try exact_source_map.Plan.prepare(a, binding, @splat(91), &borrowed);
            defer plan.deinit(a);
            borrowed[0].revision.value += 1;
            var txn = try index.beginWriteMainTxn();
            defer txn.abort();
            try txn.put(.meta, "source-map-rollback-sentinel", "pending");
            var metadata: PersistentIndex.PublicationTxn = .{ .txn = &txn };
            try std.testing.expectError(error.IndexPublicationConflict, plan.stage(&metadata));
            try std.testing.expectError(error.TransactionClosed, txn.commit());
            var read = try index.beginReadMainTxn();
            defer read.abort();
            try std.testing.expectError(error.NotFound, read.get(.meta, "source-map-rollback-sentinel"));
        }
        // Logical directory coordinates may be reclaimed/rebound independently
        // of held physical readers. Capture follows the lease's original root,
        // not today's segment-to-publication metadata.
        {
            var metadata = try index.beginWriteMainTxn();
            errdefer metadata.abort();
            try metadata.delete(.meta, &PersistentIndex.sourceSegmentKey(old.?.segments[0].id));
            try metadata.commit();
        }
        // The durable source map survives reopen and remains available for
        // the original held snapshot even after the catalog root retires.
        var captured = (try index.captureSource(&old.?.segments[0], 0)).?;
        defer captured.deinit();
        try std.testing.expectEqualDeep(ref, captured.reference);
        captured.deinit();
        current.?.release();
        current = null;
        old.?.release();
        old = null;
        try std.testing.expect(retirement.drained() == null);
        // Work belonging to an already-held query may fork after retirement.
        var child = try query.clone();
        query.deinit();
        newer_query.deinit();
        try std.testing.expect(retirement.drained() == null);
        child.deinit();
        try source_publication.ReaderPins.requireDrained(retirement.drained().?, binding);
    }
    // After all old process readers are gone, recovery needs no stale runtime
    // registry entry. Durable retirement remains terminal across reopen.
    {
        var index = try PersistentIndex.open(a, .{ .path = path });
        defer index.close();
        var retirement = try index.retireSourcePublicationWithReaders(binding);
        defer retirement.deinit();
        try std.testing.expect(retirement.drained() != null);
    }
}

test "persistent source publication pins the primary afterimage through acknowledgment and reader drainage" {
    const a = std.testing.allocator;
    const claims = @import("index_source_claim.zig");
    const docstore = @import("docstore.zig");
    const schema = @import("schema.zig");
    const mutation = @import("document_mutation_revision.zig");
    const keys = @import("internal_keys.zig");
    var backend = lsm_backend.Backend.init(a, .{});
    defer backend.close();
    var primary = try docstore.DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer primary.close();
    const physical = try keys.documentKeyAlloc(a, "doc");
    defer a.free(physical);
    const encoded = try schema.serializeSchema(a, .{});
    defer a.free(encoded);
    try primary.put(schema.schema_key, encoded);
    const source = "{\"body\":\"hello\",\"metadata\":1}";
    try primary.put(physical, source);
    var root: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(source, &root, .{});
    const binding: source_publication.Binding = .{ .owner = @splat(21), .publication = @splat(22) };
    var ref: claims.Reference = undefined;
    {
        var txn = try primary.beginWriteTxn();
        errdefer txn.abort();
        const revision = try mutation.load(&txn, mutation.keyForPhysical(physical).?);
        ref = try claims.prepareCurrent(a, &txn, binding, physical, revision, root, .{});
        try txn.commit();
    }
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    defer cleanupPersistDir(path);
    var index = try PersistentIndex.open(a, .{ .path = path });
    defer index.close();
    const decision = try index.indexSegmentOwnedWithSourceMap(try buildSimpleSegment(a, "doc", "hello"), binding, &.{ref});
    var held: ?*index_mod.IndexSnapshot = index.writer.acquireSnapshot();
    defer if (held) |snapshot| snapshot.release();
    var query = (try held.?.segments[0].sourceLease()).?;
    defer query.deinit();
    var captured = (try index.captureSource(&held.?.segments[0], 0)).?;
    defer captured.deinit();
    try std.testing.expectEqualDeep(ref, captured.reference);
    // The index transaction committed, but the primary acknowledgment has not.
    // Source reads must work at this crash cut, even after a primary overwrite.
    try primary.put(physical, "{\"body\":\"different\",\"metadata\":2}");
    {
        var read = try primary.beginReadTxn();
        defer read.abort();
        const exact = try captured.read(a, &read);
        defer exact.deinit(a);
        try std.testing.expectEqualStrings(source, exact.bytes);
    }
    {
        var txn = try primary.beginWriteTxn();
        errdefer txn.abort();
        try claims.acknowledge(a, &txn, ref, decision);
        try txn.commit();
    }
    const replacement = try buildSimpleSegment(a, "doc", "hello");
    defer a.free(replacement);
    try std.testing.expectError(error.SourcePublicationRequiresHandoff, index.replaceSegments(&.{held.?.segments[0].id}, replacement));
    try index.removeSegments(&.{held.?.segments[0].id});
    var retired = try index.retireSourcePublicationWithReaders(binding);
    defer retired.deinit();
    try std.testing.expect(retired.drained() == null);
    try std.testing.expectError(error.IndexSourceReadersActive, index.reclaimSourceMapWindow(&retired, 128));
    held.?.release();
    held = null;
    try std.testing.expect(retired.drained() == null);
    {
        var read = try primary.beginReadTxn();
        defer read.abort();
        const exact = try captured.read(a, &read);
        defer exact.deinit(a);
        try std.testing.expectEqualStrings(source, exact.bytes);
    }
    query.deinit();
    captured.deinit();
    {
        var txn = try primary.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expect(try claims.retire(a, &txn, ref, retired.decision, retired.drained().?));
        try std.testing.expectEqualDeep(claims.Budget{}, try claims.Budget.load(&txn));
        try txn.commit();
    }
    try std.testing.expect((try index.reclaimSourceMapWindow(&retired, 128)).done);
    try std.testing.expect((try index.reclaimSourceMapWindow(&retired, 128)).done);
    try (try index.loadDurableSourcePublication(binding)).?.require(binding, .retired);
}

test "persistent index write and read" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    defer cleanupPersistDir(path);

    {
        var pi = try PersistentIndex.open(alloc, .{ .path = path });
        defer pi.close();

        const seg = try buildSimpleSegment(alloc, "doc1", "hello");
        defer alloc.free(seg);
        try pi.indexSegment(seg);

        const snap = pi.snapshot();
        try std.testing.expectEqual(@as(u32, 1), snap.liveDocCount());
    }
}

test "persistent index reopen recovery" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    defer cleanupPersistDir(path);

    // Write data
    {
        var pi = try PersistentIndex.open(alloc, .{ .path = path });
        defer pi.close();

        const seg1 = try buildSimpleSegment(alloc, "doc1", "hello");
        defer alloc.free(seg1);
        try pi.indexSegment(seg1);

        const seg2 = try buildSimpleSegment(alloc, "doc2", "world");
        defer alloc.free(seg2);
        try pi.indexSegment(seg2);

        try std.testing.expectEqual(@as(u32, 2), pi.snapshot().liveDocCount());
    }

    // Reopen and verify data survived
    {
        var pi = try PersistentIndex.open(alloc, .{ .path = path });
        defer pi.close();

        try std.testing.expectEqual(@as(u32, 2), pi.snapshot().liveDocCount());

        // Verify search works
        const snap = pi.snapshot();
        const results = try snap.search(alloc, "body", &.{"hello"}, 10);
        defer alloc.free(results.hits);
        try std.testing.expect(results.hits.len >= 1);
    }
}

test "persistent index keeps high-frequency keyword postings across two read-only restarts" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    defer cleanupPersistDir(path);

    const segment = try buildHighFrequencyKeywordSegment(alloc);
    defer alloc.free(segment);
    {
        var index = try PersistentIndex.open(alloc, .{ .path = path, .main_backend = .lsm });
        defer index.close();
        try index.indexSegment(segment);
        try index.syncMain(true);
        const snapshot = index.snapshot();
        const published = try snapshot.search(alloc, "state", &.{"published"}, 5_000);
        defer alloc.free(published.hits);
        try std.testing.expectEqual(@as(u32, 4_237), published.total_count);
    }

    // Neither recovery opens writes nor re-indexes the documents. Each cycle
    // must rebuild the reader directly from the durable posting representation.
    for (0..2) |_| {
        var reopened = try PersistentIndex.open(alloc, .{
            .path = path,
            .main_backend = .lsm,
            .read_only = true,
        });
        defer reopened.close();
        const snapshot = reopened.snapshot();
        const published = try snapshot.search(alloc, "state", &.{"published"}, 5_000);
        defer alloc.free(published.hits);
        try std.testing.expectEqual(@as(u32, 4_237), published.total_count);
        const draft = try snapshot.search(alloc, "state", &.{"draft"}, 5_000);
        defer alloc.free(draft.hits);
        try std.testing.expectEqual(@as(u32, 763), draft.total_count);
        const unfiltered = try snapshot.search(alloc, "body", &.{"document"}, 5_000);
        defer alloc.free(unfiltered.hits);
        try std.testing.expectEqual(@as(u32, 5_000), unfiltered.total_count);
        try std.testing.expectEqual(@as(u32, 5_000), snapshot.liveDocCount());
    }
}

test "persistent index keeps high-frequency keyword postings across many segments on restart" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    defer cleanupPersistDir(path);

    {
        var index = try PersistentIndex.open(alloc, .{ .path = path, .main_backend = .lsm });
        defer index.close();
        for (0..10) |batch| {
            const segment = try buildHighFrequencyKeywordSegmentRange(alloc, batch * 500, 500);
            defer alloc.free(segment);
            try index.indexSegment(segment);
        }
        try index.syncMain(true);
    }

    var reopened = try PersistentIndex.open(alloc, .{
        .path = path,
        .main_backend = .lsm,
        .read_only = true,
    });
    defer reopened.close();
    const snapshot = reopened.snapshot();
    const published = try snapshot.search(alloc, "state", &.{"published"}, 5_000);
    defer alloc.free(published.hits);
    try std.testing.expectEqual(@as(u32, 4_237), published.total_count);
    const draft = try snapshot.search(alloc, "state", &.{"draft"}, 5_000);
    defer alloc.free(draft.hits);
    try std.testing.expectEqual(@as(u32, 763), draft.total_count);
}

test "persistent segment-file publication does not append to WAL" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    defer cleanupPersistDir(path);

    var pi = try PersistentIndex.open(alloc, .{
        .path = path,
        .wal_group_commit_window_ns = 5 * std.time.ns_per_ms,
        .wal_group_commit_max_requests = 4,
    });
    defer pi.close();

    const seg = try buildSimpleSegment(alloc, "doc1", "hello");
    defer alloc.free(seg);
    try pi.indexSegment(seg);

    const stats = pi.statsSnapshot();
    try std.testing.expectEqual(@as(u64, 0), stats.wal.logical_entries);
    try std.testing.expectEqual(@as(u64, 0), stats.wal.physical_commits);
    try std.testing.expect(stats.main_commit == null);
}

test "persistent index persists segment key ranges across reopen" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    defer cleanupPersistDir(path);

    {
        var pi = try PersistentIndex.open(alloc, .{ .path = path });
        defer pi.close();

        const seg_a = try buildSimpleSegment(alloc, "doc:c", "alpha");
        defer alloc.free(seg_a);
        try pi.indexSegment(seg_a);

        const seg_b = try buildMultiDocSegment(alloc, &.{
            .{ .doc_id = "doc:m", .term = "middle" },
            .{ .doc_id = "doc:z", .term = "omega" },
        });
        defer alloc.free(seg_b);
        try pi.indexSegment(seg_b);

        const ranges = try pi.activeSegmentRanges(alloc);
        defer {
            for (ranges) |*range| range.deinit(alloc);
            alloc.free(ranges);
        }
        try std.testing.expectEqual(@as(usize, 2), ranges.len);
        try std.testing.expectEqualStrings("doc:c", ranges[0].min_doc_key);
        try std.testing.expectEqualStrings("doc:c", ranges[0].max_doc_key);
        try std.testing.expectEqualStrings("doc:m", ranges[1].min_doc_key);
        try std.testing.expectEqualStrings("doc:z", ranges[1].max_doc_key);
    }

    {
        var pi = try PersistentIndex.open(alloc, .{ .path = path });
        defer pi.close();

        const ranges = try pi.activeSegmentRanges(alloc);
        defer {
            for (ranges) |*range| range.deinit(alloc);
            alloc.free(ranges);
        }
        try std.testing.expectEqual(@as(usize, 2), ranges.len);
        try std.testing.expectEqualStrings("doc:c", ranges[0].min_doc_key);
        try std.testing.expectEqualStrings("doc:c", ranges[0].max_doc_key);
        try std.testing.expectEqualStrings("doc:m", ranges[1].min_doc_key);
        try std.testing.expectEqualStrings("doc:z", ranges[1].max_doc_key);
    }
}

test "native segment publication admits writer and mmap before creating output" {
    if (builtin.os.tag == .freestanding or builtin.os.tag == .windows or builtin.os.tag == .wasi) return error.SkipZigTest;

    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    const path_span = std.mem.span(path);
    defer cleanupPersistDir(path);

    var pool = storage_io.NativeStoragePool.initWithCapacityForTest(alloc, 4);
    defer pool.deinit();
    var store = try SegmentFileStore.open(alloc, path_span, null, true, .threaded, &pool);
    defer store.close();

    const lock_path_a = try std.fmt.allocPrint(alloc, "{s}/publication-a.lock", .{path_span});
    defer alloc.free(lock_path_a);
    const lock_path_b = try std.fmt.allocPrint(alloc, "{s}/publication-b.lock", .{path_span});
    defer alloc.free(lock_path_b);
    var lock_a = try storage_io.openNativePathLockFileWithPool(alloc, lock_path_a, .{ .create_if_missing = true }, &pool);
    var lock_a_active = true;
    defer if (lock_a_active) lock_a.close();
    var lock_b = try storage_io.openNativePathLockFileWithPool(alloc, lock_path_b, .{ .create_if_missing = true }, &pool);
    var lock_b_active = true;
    defer if (lock_b_active) lock_b.close();

    // Two lifetime descriptors leave no safe two-slot publication window.
    // Admission must fail before the atomic writer creates the segment file.
    try std.testing.expectError(
        error.PersistentDescriptorAdmissionExhausted,
        store.publish(1, "not published"),
    );

    lock_b.close();
    lock_b_active = false;
    lock_a.close();
    lock_a_active = false;

    const segment_path = try store.pathAlloc(1);
    defer alloc.free(segment_path);
    try std.testing.expectError(
        error.FileNotFound,
        store.storage.readFileAlloc(alloc, segment_path, 64),
    );

    // With the two-slot window available, the writer consumes one reservation
    // and mmap uses the other without any intervening admission request.
    var published = try store.publish(2, "published");
    try std.testing.expectEqualStrings("published", published.bytes());
    published.deinit(alloc);
    store.delete(2);

    // A failure after atomic rename is still owned by this unpublished
    // operation. Empty files deterministically fail mmap validation and must
    // not remain as orphan segment artifacts.
    try std.testing.expectError(error.EmptySegment, store.publish(3, ""));
    const empty_segment_path = try store.pathAlloc(3);
    defer alloc.free(empty_segment_path);
    try std.testing.expectError(
        error.FileNotFound,
        store.storage.readFileAlloc(alloc, empty_segment_path, 64),
    );
}

test "native sink publication admits writer and mmap before invoking builder" {
    if (builtin.os.tag == .freestanding or builtin.os.tag == .windows or builtin.os.tag == .wasi) return error.SkipZigTest;

    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    const path_span = std.mem.span(path);
    defer cleanupPersistDir(path);

    var pool = storage_io.NativeStoragePool.initWithCapacityForTest(alloc, 16);
    defer pool.deinit();
    var pi = try PersistentIndex.open(alloc, .{
        .path = path,
        .main_lsm_options = .{ .flush_threshold = 1, .native_storage_pool = &pool },
        .wal_lsm_options = .{ .native_storage_pool = &pool },
    });
    defer pi.close();

    var locks = std.ArrayListUnmanaged(storage_io.NativePathLockFile).empty;
    defer {
        for (locks.items) |*lock| lock.close();
        locks.deinit(alloc);
    }
    var exhausted = false;
    for (0..32) |i| {
        const lock_path = try std.fmt.allocPrint(alloc, "{s}/sink-admission-{d}.lock", .{ path_span, i });
        defer alloc.free(lock_path);
        var lock = storage_io.openNativePathLockFileWithPool(alloc, lock_path, .{ .create_if_missing = true }, &pool) catch |err| switch (err) {
            error.PersistentDescriptorAdmissionExhausted => {
                exhausted = true;
                break;
            },
            else => return err,
        };
        locks.append(alloc, lock) catch |err| {
            lock.close();
            return err;
        };
    }
    try std.testing.expect(exhausted);

    const segment = try buildSimpleSegment(alloc, "doc:a", "alpha");
    defer alloc.free(segment);
    const CopyBuilder = struct {
        bytes: []const u8,
        calls: *usize,

        fn build(ptr: *anyopaque, sink: *segment_mod.SegmentSink) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls.* += 1;
            try sink.appendSlice(self.bytes);
        }
    };
    var calls: usize = 0;
    var builder = CopyBuilder{ .bytes = segment, .calls = &calls };
    try std.testing.expectError(
        error.PersistentDescriptorAdmissionExhausted,
        pi.indexSegmentFromSinkBuilder(&builder, CopyBuilder.build),
    );
    try std.testing.expectEqual(@as(usize, 0), calls);

    for (locks.items) |*lock| lock.close();
    locks.clearRetainingCapacity();

    try std.testing.expectEqual(segment.len, try pi.indexSegmentFromSinkBuilder(&builder, CopyBuilder.build));
    try std.testing.expectEqual(@as(usize, 1), calls);
    try std.testing.expectEqual(@as(u32, 1), pi.snapshot().liveDocCount());
}

test "retired segment cleanup outlives persistent index close" {
    if (builtin.os.tag == .freestanding or builtin.os.tag == .windows or builtin.os.tag == .wasi) return error.SkipZigTest;

    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    defer cleanupPersistDir(path);

    var pi = try PersistentIndex.open(alloc, .{ .path = path });
    var pi_active = true;
    defer if (pi_active) pi.close();

    const seg_a = try buildSimpleSegment(alloc, "doc:a", "alpha");
    defer alloc.free(seg_a);
    try pi.indexSegment(seg_a);
    const seg_b = try buildSimpleSegment(alloc, "doc:b", "beta");
    defer alloc.free(seg_b);
    try pi.indexSegment(seg_b);

    const held = pi.acquireSnapshot();
    var held_active = true;
    defer if (held_active) held.release();

    const merged = try buildMultiDocSegment(alloc, &.{
        .{ .doc_id = "doc:a", .term = "alpha" },
        .{ .doc_id = "doc:b", .term = "beta" },
    });
    defer alloc.free(merged);
    try pi.replaceSegments(&.{ 1, 2 }, merged);

    const retired_path_a = try pi.segment_files.?.pathAlloc(1);
    defer alloc.free(retired_path_a);
    const retired_path_b = try pi.segment_files.?.pathAlloc(2);
    defer alloc.free(retired_path_b);

    pi.close();
    pi_active = false;

    var verifier = try storage_io.NativeStorage.init(alloc, .threaded);
    defer verifier.deinit();
    const pinned = try verifier.storage().readFileAlloc(alloc, retired_path_a, std.math.maxInt(usize));
    alloc.free(pinned);

    held.release();
    held_active = false;
    try std.testing.expectError(
        error.FileNotFound,
        verifier.storage().readFileAlloc(alloc, retired_path_a, 64),
    );
    try std.testing.expectError(
        error.FileNotFound,
        verifier.storage().readFileAlloc(alloc, retired_path_b, 64),
    );
}

test "retired segment cleanup bounds maintenance work per publication boundary" {
    if (builtin.os.tag == .freestanding or builtin.os.tag == .windows or builtin.os.tag == .wasi) return error.SkipZigTest;

    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    defer cleanupPersistDir(path);

    var pi = try PersistentIndex.open(alloc, .{ .path = path });
    defer pi.close();
    const deleter = pi.retired_segment_file_deleter orelse return error.TestUnexpectedResult;

    const queued = RetiredSegmentFileDeleter.retry_attempts_per_boundary * 3;
    for (0..queued) |offset| try deleter.enqueueRetry(10_000 + @as(u64, @intCast(offset)));

    deleter.retryFailedDeletes();
    {
        platform_sync.lockYielding(&deleter.delete_mu);
        defer deleter.delete_mu.unlock();
        try std.testing.expectEqual(RetiredSegmentFileDeleter.retry_attempts_per_boundary, deleter.completed_ids.items.len);
        try std.testing.expectEqual(queued - RetiredSegmentFileDeleter.retry_attempts_per_boundary, deleter.retry_entries.items.len);
        try std.testing.expectEqual(@as(u64, RetiredSegmentFileDeleter.retry_attempts_per_boundary), deleter.retry_attempts);
        try std.testing.expectEqual(deleter.completed_ids.items.len, deleter.completed_positions.count());
        try std.testing.expectEqual(deleter.retry_entries.items.len, deleter.retry_positions.count());
        for (deleter.retry_entries.items) |*entry| entry.next_attempt_ns = std.math.maxInt(u64);
    }
    deleter.retryFailedDeletes();
    try std.testing.expectEqual(@as(u64, RetiredSegmentFileDeleter.retry_attempts_per_boundary), deleter.retry_attempts);

    for (0..RetiredSegmentFileDeleter.completed_markers_per_boundary + 3) |offset| {
        try deleter.completeWithoutDelete(20_000 + @as(u64, @intCast(offset)));
    }
    const completed_snapshot = try deleter.snapshotCompletedIds(alloc, RetiredSegmentFileDeleter.completed_markers_per_boundary);
    defer alloc.free(completed_snapshot);
    try std.testing.expectEqual(RetiredSegmentFileDeleter.completed_markers_per_boundary, completed_snapshot.len);
}

test "persistent index replaceSegments updates segment key range metadata" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    defer cleanupPersistDir(path);

    var pi = try PersistentIndex.open(alloc, .{ .path = path });
    defer pi.close();

    const seg_a = try buildSimpleSegment(alloc, "doc:a", "alpha");
    defer alloc.free(seg_a);
    try pi.indexSegment(seg_a);

    const seg_b = try buildSimpleSegment(alloc, "doc:z", "omega");
    defer alloc.free(seg_b);
    try pi.indexSegment(seg_b);

    const merged = try buildMultiDocSegment(alloc, &.{
        .{ .doc_id = "doc:a", .term = "alpha" },
        .{ .doc_id = "doc:z", .term = "omega" },
    });
    defer alloc.free(merged);
    try pi.replaceSegments(&.{ 1, 2 }, merged);

    const ranges = try pi.activeSegmentRanges(alloc);
    defer {
        for (ranges) |*range| range.deinit(alloc);
        alloc.free(ranges);
    }
    try std.testing.expectEqual(@as(usize, 1), ranges.len);
    try std.testing.expectEqual(@as(u64, 3), ranges[0].seg_id);
    try std.testing.expectEqualStrings("doc:a", ranges[0].min_doc_key);
    try std.testing.expectEqualStrings("doc:z", ranges[0].max_doc_key);
}

test "persistent index replaceSegmentsIfActiveManyOwned publishes bounded merge outputs atomically" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    defer cleanupPersistDir(path);

    var pi = try PersistentIndex.open(alloc, .{ .path = path });
    defer pi.close();

    const seg_a = try buildSimpleSegment(alloc, "doc:a", "alpha");
    defer alloc.free(seg_a);
    try pi.indexSegment(seg_a);

    const seg_b = try buildSimpleSegment(alloc, "doc:z", "omega");
    defer alloc.free(seg_b);
    try pi.indexSegment(seg_b);

    const replacements = try alloc.alloc([]u8, 2);
    replacements[0] = try buildSimpleSegment(alloc, "doc:a", "alpha");
    replacements[1] = try buildSimpleSegment(alloc, "doc:z", "omega");
    const applied = try pi.replaceSegmentsIfActiveManyOwned(&.{ 1, 2 }, replacements);
    try std.testing.expect(applied);

    const snap = pi.snapshot();
    try std.testing.expectEqual(@as(usize, 2), snap.segments.len);
    try std.testing.expectEqual(@as(u64, 3), snap.segments[0].id);
    try std.testing.expectEqual(@as(u64, 4), snap.segments[1].id);

    const ranges = try pi.activeSegmentRanges(alloc);
    defer {
        for (ranges) |*range| range.deinit(alloc);
        alloc.free(ranges);
    }
    try std.testing.expectEqual(@as(usize, 2), ranges.len);
    try std.testing.expectEqualStrings("doc:a", ranges[0].min_doc_key);
    try std.testing.expectEqualStrings("doc:z", ranges[1].min_doc_key);
}

test "persistent budgeted merge publication consumes input when id staging is denied" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    defer cleanupPersistDir(path);

    var pi = try PersistentIndex.open(alloc, .{ .path = path });
    defer pi.close();

    const source = try buildSimpleSegment(alloc, "doc:a", "alpha");
    defer alloc.free(source);
    try pi.indexSegment(source);

    var failing = std.testing.FailingAllocator.init(alloc, .{});
    const scratch_alloc = failing.allocator();
    const replacements = try scratch_alloc.alloc([]u8, 1);
    replacements[0] = try scratch_alloc.dupe(u8, source);
    failing.fail_index = failing.alloc_index;

    try std.testing.expectError(
        error.OutOfMemory,
        pi.replaceSegmentsIfActiveManyOwnedWithDeletesAndScratch(
            scratch_alloc,
            &.{1},
            replacements,
            null,
        ),
    );
    try std.testing.expect(failing.has_induced_failure);
}

test "persistent merge replacement carries deletion bitmap across reopen" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    defer cleanupPersistDir(path);

    {
        var pi = try PersistentIndex.open(alloc, .{ .path = path });
        defer pi.close();

        const seg_a = try buildSimpleSegment(alloc, "doc:a", "alpha");
        defer alloc.free(seg_a);
        try pi.indexSegment(seg_a);

        const seg_b = try buildSimpleSegment(alloc, "doc:b", "beta");
        defer alloc.free(seg_b);
        try pi.indexSegment(seg_b);

        const replacements = try alloc.alloc([]u8, 1);
        replacements[0] = try buildMultiDocSegment(alloc, &.{
            .{ .doc_id = "doc:a", .term = "alpha" },
            .{ .doc_id = "doc:b", .term = "beta" },
        });
        var deleted = roaring.RoaringBitmap.init(alloc);
        defer deleted.deinit();
        try deleted.add(1);
        const replacement_deleted = [_]?roaring.RoaringBitmap{deleted};

        try std.testing.expect(try pi.replaceSegmentsIfActiveManyOwnedWithDeletes(
            &.{ 1, 2 },
            replacements,
            &replacement_deleted,
        ));
        try std.testing.expectEqual(@as(u64, 1), pi.snapshot().liveDocCount());
    }

    {
        var pi = try PersistentIndex.open(alloc, .{ .path = path });
        defer pi.close();

        const snap = pi.snapshot();
        try std.testing.expectEqual(@as(usize, 1), snap.segments.len);
        try std.testing.expectEqual(@as(u64, 1), snap.liveDocCount());
        try std.testing.expect(snap.segments[0].shared.deleted.?.contains(1));
    }
}

test "persistent index open prunes stale active segment references" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    defer cleanupPersistDir(path);

    {
        var pi = try PersistentIndex.open(alloc, .{ .path = path });
        defer pi.close();

        const seg = try buildSimpleSegment(alloc, "doc:a", "alpha");
        defer alloc.free(seg);
        try pi.indexSegment(seg);

        var txn = try pi.beginWriteMainTxn();
        errdefer txn.abort();

        const seg_key = std.mem.toBytes(std.mem.nativeToBig(u64, @as(u64, 1)));
        txn.delete(.segments, &seg_key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
        try pi.deleteSegmentRange(&txn, 1);
        try txn.commit();
        pi.deleteSegmentFile(1);
    }

    {
        var pi = try PersistentIndex.open(alloc, .{ .path = path });
        defer pi.close();

        const ranges = try pi.activeSegmentRanges(alloc);
        defer {
            for (ranges) |*range| range.deinit(alloc);
            alloc.free(ranges);
        }
        try std.testing.expectEqual(@as(usize, 0), ranges.len);

        var txn = try pi.beginReadMainTxn();
        defer txn.abort();
        const active_ids = try pi.loadActiveSegmentIds(&txn, alloc);
        defer alloc.free(active_ids);
        try std.testing.expectEqual(@as(usize, 0), active_ids.len);
    }
}

test "persistent index open keeps recovered segment ownership on later catalog error" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    defer cleanupPersistDir(path);

    {
        var pi = try PersistentIndex.open(alloc, .{ .path = path });
        defer pi.close();

        const seg = try buildSimpleSegment(alloc, "doc:a", "alpha");
        defer alloc.free(seg);
        try pi.indexSegment(seg);

        var txn = try pi.beginWriteMainTxn();
        errdefer txn.abort();
        const seg_key = std.mem.toBytes(std.mem.nativeToBig(u64, @as(u64, 1)));
        try txn.put(.deletions, &seg_key, "\x01\x00");
        try txn.commit();
    }

    try std.testing.expectError(error.InvalidRoaringBitmap, PersistentIndex.open(alloc, .{ .path = path }));
}

test "persistent index classifies active segments for split" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    defer cleanupPersistDir(path);

    var pi = try PersistentIndex.open(alloc, .{ .path = path });
    defer pi.close();

    const left_seg = try buildMultiDocSegment(alloc, &.{
        .{ .doc_id = "doc:a", .term = "alpha" },
        .{ .doc_id = "doc:c", .term = "charlie" },
    });
    defer alloc.free(left_seg);
    try pi.indexSegment(left_seg);

    const right_seg = try buildMultiDocSegment(alloc, &.{
        .{ .doc_id = "doc:m", .term = "middle" },
        .{ .doc_id = "doc:z", .term = "omega" },
    });
    defer alloc.free(right_seg);
    try pi.indexSegment(right_seg);

    const mixed_seg = try buildMultiDocSegment(alloc, &.{
        .{ .doc_id = "doc:f", .term = "foxtrot" },
        .{ .doc_id = "doc:t", .term = "tango" },
    });
    defer alloc.free(mixed_seg);
    try pi.indexSegment(mixed_seg);

    const plan = try pi.classifyActiveSegmentsForSplit(alloc, "doc:m");
    defer {
        for (plan) |*entry| entry.deinit(alloc);
        alloc.free(plan);
    }

    try std.testing.expectEqual(@as(usize, 3), plan.len);
    try std.testing.expectEqual(SegmentSplitClass.left_only, plan[0].class);
    try std.testing.expectEqual(SegmentSplitClass.right_only, plan[1].class);
    try std.testing.expectEqual(SegmentSplitClass.mixed, plan[2].class);
}

test "persistent index preserves deletion of repeated document versions across reopen" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    defer cleanupPersistDir(path);

    {
        var index = try PersistentIndex.open(alloc, .{ .path = path });
        defer index.close();

        const first = try buildSimpleSegment(alloc, "doc:a", "gamma");
        defer alloc.free(first);
        try index.indexSegment(first);
        try std.testing.expect(try index.deleteById("doc:a"));

        const second = try buildSimpleSegment(alloc, "doc:a", "delta");
        defer alloc.free(second);
        try index.indexSegment(second);
        try std.testing.expect(try index.deleteById("doc:a"));

        const final = try buildSimpleSegment(alloc, "doc:a", "epsilon");
        defer alloc.free(final);
        try index.indexSegment(final);
    }

    var reopened = try PersistentIndex.open(alloc, .{ .path = path });
    defer reopened.close();
    const stale = try reopened.snapshot().search(alloc, "body", &.{"gamma"}, 10);
    defer alloc.free(stale.hits);
    try std.testing.expectEqual(@as(u32, 0), stale.total_count);
    const superseded = try reopened.snapshot().search(alloc, "body", &.{"delta"}, 10);
    defer alloc.free(superseded.hits);
    try std.testing.expectEqual(@as(u32, 0), superseded.total_count);
    const current = try reopened.snapshot().search(alloc, "body", &.{"epsilon"}, 10);
    defer alloc.free(current.hits);
    try std.testing.expectEqual(@as(u32, 1), current.total_count);
}

test "persistent index hands off right-only segments to child index" {
    const alloc = std.testing.allocator;
    var src_path_buf: [256]u8 = undefined;
    const src_path = persistTmpPath(&src_path_buf);
    defer cleanupPersistDir(src_path);

    var dest_path_buf: [256]u8 = undefined;
    const dest_path = persistTmpPath(&dest_path_buf);
    defer cleanupPersistDir(dest_path);

    var src = try PersistentIndex.open(alloc, .{ .path = src_path });
    defer src.close();

    const left_seg = try buildMultiDocSegment(alloc, &.{
        .{ .doc_id = "doc:a", .term = "alpha" },
        .{ .doc_id = "doc:c", .term = "charlie" },
    });
    defer alloc.free(left_seg);
    try src.indexSegment(left_seg);

    const right_seg = try buildMultiDocSegment(alloc, &.{
        .{ .doc_id = "doc:m", .term = "middle" },
        .{ .doc_id = "doc:z", .term = "omega" },
    });
    defer alloc.free(right_seg);
    try src.indexSegment(right_seg);
    try std.testing.expect(try src.deleteById("doc:z"));

    const mixed_seg = try buildMultiDocSegment(alloc, &.{
        .{ .doc_id = "doc:f", .term = "foxtrot" },
        .{ .doc_id = "doc:t", .term = "tango" },
    });
    defer alloc.free(mixed_seg);
    try src.indexSegment(mixed_seg);

    var dest = try PersistentIndex.open(alloc, .{ .path = dest_path });
    defer dest.close();

    // The handoff scratch/result allocator is intentionally distinct from the
    // persistent indexes' allocator. Every temporary and returned allocation
    // must be released through the allocator supplied to the API.
    var handoff_arena = std.heap.ArenaAllocator.init(alloc);
    defer handoff_arena.deinit();
    const handoff_alloc = handoff_arena.allocator();
    var handoff = try src.handoffRightOnlySegmentsToChildDetailed(&dest, "doc:m", handoff_alloc, true);
    defer handoff.deinit(handoff_alloc);
    try std.testing.expectEqual(@as(usize, 1), handoff.transferred_segments);

    const ranges = try dest.activeSegmentRanges(alloc);
    defer {
        for (ranges) |*range| range.deinit(alloc);
        alloc.free(ranges);
    }
    try std.testing.expectEqual(@as(usize, 1), ranges.len);
    try std.testing.expectEqualStrings("doc:m", ranges[0].min_doc_key);
    try std.testing.expectEqualStrings("doc:z", ranges[0].max_doc_key);

    const snap = dest.snapshot();
    try std.testing.expectEqual(@as(u32, 1), snap.liveDocCount());

    const middle_results = try snap.search(alloc, "body", &.{"middle"}, 10);
    defer alloc.free(middle_results.hits);
    try std.testing.expectEqual(@as(u32, 1), middle_results.total_count);

    const omega_results = try snap.search(alloc, "body", &.{"omega"}, 10);
    defer alloc.free(omega_results.hits);
    try std.testing.expectEqual(@as(u32, 0), omega_results.total_count);

    const alpha_results = try snap.search(alloc, "body", &.{"alpha"}, 10);
    defer alloc.free(alpha_results.hits);
    try std.testing.expectEqual(@as(u32, 0), alpha_results.total_count);
}

test "persistent namespace adapters expose multi-partition txn operations" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    defer cleanupPersistDir(path);

    var idx = try PersistentIndex.open(alloc, .{ .path = path });
    defer idx.close();

    const seg_key = std.mem.toBytes(std.mem.nativeToBig(u64, 7));

    {
        var txn = try idx.beginWriteMainTxn();
        errdefer txn.abort();
        var write = txn.writeAdapter();
        try write.put(.meta, meta_committed_lsn, &std.mem.toBytes(@as(u64, 11)));
        try write.put(.segments, &seg_key, "seg7");
        try write.put(.deletions, &seg_key, "del7");
        try write.commit();
    }

    {
        var txn = try idx.beginReadMainTxn();
        defer txn.abort();
        var read = txn.readAdapter();
        const lsn_bytes = try read.get(.meta, meta_committed_lsn);
        try std.testing.expectEqual(@as(u64, 11), std.mem.readInt(u64, lsn_bytes[0..8], .little));
        try std.testing.expectEqualStrings("seg7", try read.get(.segments, &seg_key));
        try std.testing.expectEqualStrings("del7", try read.get(.deletions, &seg_key));
    }
}

test "persistent backend store opens concrete txn handles" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    defer cleanupPersistDir(path);

    var idx = try PersistentIndex.open(alloc, .{ .path = path });
    defer idx.close();

    const seg_key = std.mem.toBytes(std.mem.nativeToBig(u64, 8));
    var backend = idx.backendStore();
    try std.testing.expect(backend.capabilities().native_namespaces);

    {
        var txn = try backend.beginWrite();
        errdefer txn.abort();
        var write = txn.writeAdapter();
        try write.put(.segments, &seg_key, "seg8");
        try write.commit();
    }

    {
        var txn = try backend.beginRead();
        defer txn.abort();
        var read = txn.readAdapter();
        try std.testing.expectEqualStrings("seg8", try read.get(.segments, &seg_key));
    }

    {
        var batch = try backend.beginBatch();
        errdefer batch.abort();
        var write = batch.writeAdapter();
        try write.put(.meta, meta_committed_lsn, &std.mem.toBytes(@as(u64, 13)));
        try write.commit();
    }
}

test "persistent backend runtime erases namespace store handles" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    defer cleanupPersistDir(path);

    var idx = try PersistentIndex.open(alloc, .{ .path = path });
    defer idx.close();

    var runtime = try backend_erased.namespaceStoreFrom(
        std.testing.allocator,
        idx.backendStore(),
        MainKeyspace,
        mapRuntimeNamespace,
    );
    defer runtime.deinit();
    try std.testing.expect(runtime.capabilities().native_namespaces);

    const seg_key = std.mem.toBytes(std.mem.nativeToBig(u64, 12));

    {
        var txn = try runtime.beginWrite();
        try txn.put(.{ .name = "segments" }, &seg_key, "seg12");
        try txn.put(.{}, meta_committed_lsn, &std.mem.toBytes(@as(u64, 21)));
        try txn.commit();
    }

    {
        var txn = try runtime.beginRead();
        defer txn.abort();
        try std.testing.expectEqualStrings("seg12", try txn.get(.{ .name = "segments" }, &seg_key));
        const lsn = try txn.get(.{}, meta_committed_lsn);
        try std.testing.expectEqual(@as(u64, 21), std.mem.readInt(u64, lsn[0..8], .little));
    }
}

test "persistent index reopens with durable lsm main backend" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    defer cleanupPersistDir(path);

    const seg = try buildSimpleSegment(alloc, "doc:a", "alpha");
    defer alloc.free(seg);

    {
        var idx = try PersistentIndex.open(alloc, .{
            .path = path,
            .main_backend = .lsm,
        });
        defer idx.close();
        try idx.indexSegment(seg);
        try idx.syncMain(true);
        try std.testing.expect(idx.commitStatsSnapshot() == null);
    }

    {
        var idx = try PersistentIndex.open(alloc, .{
            .path = path,
            .main_backend = .lsm,
        });
        defer idx.close();

        const snap = idx.snapshot();
        try std.testing.expectEqual(@as(u32, 1), snap.liveDocCount());
        const results = try snap.search(alloc, "body", &.{"alpha"}, 10);
        defer alloc.free(results.hits);
        try std.testing.expectEqual(@as(u32, 1), results.total_count);
    }
}

test "persistent index lsm maintenance debt hint tracks segment store writes" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    defer cleanupPersistDir(path);

    const seg = try buildSimpleSegment(alloc, "doc:a", "alpha");
    defer alloc.free(seg);

    var idx = try PersistentIndex.open(alloc, .{
        .path = path,
        .main_backend = .lsm,
        .main_lsm_options = .{ .flush_threshold = 1024 },
    });
    defer idx.close();

    try std.testing.expectEqual(@as(u64, 0), idx.lsmMaintenanceDebtHint());

    try idx.indexSegment(seg);
    try std.testing.expect(idx.lsmMaintenanceDebtHint() > 0);

    while (try idx.runLsmMaintenanceStep()) {}
    try std.testing.expectEqual(@as(u64, 0), idx.lsmMaintenanceDebtHint());
}

test "persistent index snapshots use mapped segment files when native storage is available" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    defer cleanupPersistDir(path);

    const seg = try buildSimpleSegment(alloc, "doc:a", "alpha");
    defer alloc.free(seg);

    var idx = try PersistentIndex.open(alloc, .{
        .path = path,
        .main_backend = .lsm,
    });
    defer idx.close();

    try idx.indexSegment(seg);

    const snap = idx.snapshot();
    try std.testing.expectEqual(@as(usize, 1), snap.segments.len);
    try std.testing.expect(snap.segments[0].data.isFileBacked());
    try std.testing.expectEqualStrings("doc:a", (try snap.storedDoc(0)).?.id);
}

test "persistent index deletes replaced segment files only after retained snapshot release" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    defer cleanupPersistDir(path);

    var idx = try PersistentIndex.open(alloc, .{
        .path = path,
        .main_backend = .lsm,
    });
    defer idx.close();

    const seg_a = try buildSimpleSegment(alloc, "doc:a", "alpha");
    defer alloc.free(seg_a);
    try idx.indexSegment(seg_a);

    const seg_b = try buildSimpleSegment(alloc, "doc:z", "omega");
    defer alloc.free(seg_b);
    try idx.indexSegment(seg_b);

    const retained = idx.acquireSnapshot();
    try std.testing.expectEqual(@as(usize, 2), retained.segments.len);

    const store = &idx.segment_files.?;
    const path_1 = try store.pathAlloc(1);
    defer alloc.free(path_1);
    const path_2 = try store.pathAlloc(2);
    defer alloc.free(path_2);

    _ = try store.storage.fileSize(path_1);
    _ = try store.storage.fileSize(path_2);

    const merged = try buildMultiDocSegment(alloc, &.{
        .{ .doc_id = "doc:a", .term = "alpha" },
        .{ .doc_id = "doc:z", .term = "omega" },
    });
    defer alloc.free(merged);
    try idx.replaceSegments(&.{ 1, 2 }, merged);

    _ = try store.storage.fileSize(path_1);
    _ = try store.storage.fileSize(path_2);

    retained.release();
    try std.testing.expectError(error.FileNotFound, store.storage.fileSize(path_1));
    try std.testing.expectError(error.FileNotFound, store.storage.fileSize(path_2));
}

test "retired cleanup cannot reuse segment ids across reset" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    defer cleanupPersistDir(path);

    var idx = try PersistentIndex.open(alloc, .{ .path = path });
    var idx_open = true;
    defer if (idx_open) idx.close();

    const old_segment = try buildSimpleSegment(alloc, "doc:old", "old");
    defer alloc.free(old_segment);
    try idx.indexSegment(old_segment);

    const retained = idx.acquireSnapshot();
    var retained_open = true;
    defer if (retained_open) retained.release();
    try idx.removeSegments(&.{1});
    try idx.resetAllForRebuild();

    const rebuilt_segment = try buildSimpleSegment(alloc, "doc:new", "new");
    defer alloc.free(rebuilt_segment);
    try idx.indexSegment(rebuilt_segment);
    try std.testing.expectEqual(@as(u64, 2), idx.snapshot().segments[0].id);

    retained.release();
    retained_open = false;
    idx.close();
    idx_open = false;

    var reopened = try PersistentIndex.open(alloc, .{ .path = path });
    defer reopened.close();
    try std.testing.expectEqual(@as(u32, 1), reopened.snapshot().liveDocCount());
    try std.testing.expectEqual(@as(u64, 2), reopened.snapshot().segments[0].id);
}

test "removed segment high-water survives close with a retained snapshot" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    defer cleanupPersistDir(path);

    var idx = try PersistentIndex.open(alloc, .{ .path = path });
    const old_segment = try buildSimpleSegment(alloc, "doc:old", "old");
    defer alloc.free(old_segment);
    try idx.indexSegment(old_segment);
    const retained = idx.acquireSnapshot();
    var retained_open = true;
    defer if (retained_open) retained.release();
    try idx.removeSegments(&.{1});
    idx.close();

    var reopened = try PersistentIndex.open(alloc, .{ .path = path });
    var reopened_open = true;
    defer if (reopened_open) reopened.close();
    const replacement = try buildSimpleSegment(alloc, "doc:new", "new");
    defer alloc.free(replacement);
    try reopened.indexSegment(replacement);
    try std.testing.expectEqual(@as(u64, 2), reopened.snapshot().segments[0].id);

    retained.release();
    retained_open = false;
    reopened.close();
    reopened_open = false;

    var verified = try PersistentIndex.open(alloc, .{ .path = path });
    defer verified.close();
    try std.testing.expectEqual(@as(u32, 1), verified.snapshot().liveDocCount());
    try std.testing.expectEqual(@as(u64, 2), verified.snapshot().segments[0].id);
}

test "crash rollback fence disarms retained segment cleanup" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    defer cleanupPersistDir(path);

    var idx = try PersistentIndex.open(alloc, .{ .path = path });
    const segment = try buildSimpleSegment(alloc, "doc:old", "old");
    defer alloc.free(segment);
    try idx.indexSegment(segment);
    const retained = idx.acquireSnapshot();
    var retained_open = true;
    defer if (retained_open) retained.release();
    try idx.removeSegments(&.{1});

    const retired_path = try idx.segment_files.?.pathAlloc(1);
    defer alloc.free(retired_path);
    idx.prepareForCrashRollback();
    idx.abandonAfterCrash();
    retained.release();
    retained_open = false;

    var verifier = try storage_io.NativeStorage.init(alloc, .threaded);
    defer verifier.deinit();
    _ = try verifier.storage().fileSize(retired_path);
}

const PersistentSimAction = persistent_sim_fixture.Action;
pub const VoprAction = PersistentSimAction;
const PersistentCrashOutcome = persistent_sim_fixture.CrashOutcome;
const PersistentSimSegmentSpec = persistent_sim_fixture.SegmentSpec;

pub const VoprSummary = struct {
    doc_count: u32,
    segment_count: usize,
    alpha_hits: usize,
    beta_hits: usize,
    gamma_hits: usize,
};
const PersistentSimSummary = VoprSummary;

fn persistentSimOptionsToIndexOptions(
    path: [*:0]const u8,
    opts: persistent_sim_fixture.Options,
) PersistentIndexOptions {
    return .{
        .path = path,
        .main_no_sync = opts.main_no_sync,
        .main_no_meta_sync = opts.main_no_meta_sync,
        .wal_no_sync = opts.wal_no_sync,
        .main_commit_backend = switch (opts.main_commit_backend) {
            .sync => .sync,
            .worker_thread => .worker_thread,
            .async_io => .async_io,
            .adaptive => .adaptive,
        },
        .wal_commit_backend = switch (opts.wal_commit_backend) {
            .sync => .sync,
            .worker_thread => .worker_thread,
            .async_io => .async_io,
            .adaptive => .adaptive,
        },
    };
}

fn persistentModeledOptionsToIndexOptions(
    path: [*:0]const u8,
    opts: persistent_sim_fixture.Options,
    device_model: *storage_sim.ModeledDevice,
    runtime: *storage_sim.Runtime,
) PersistentIndexOptions {
    var index_opts = persistentSimOptionsToIndexOptions(path, opts);
    index_opts.main_backend = .lsm;
    index_opts.wal_backend = .lsm;
    index_opts.main_lsm_storage = device_model.storage();
    index_opts.wal_storage = device_model.storage();
    index_opts.wal_clock = runtime.clock();
    index_opts.wal_commit_scheduler = runtime.completionScheduler();
    index_opts.model_wal_commit_backend_completions = true;
    return index_opts;
}

fn fixtureOptionsFromPersistentOptions(opts: PersistentIndexOptions) persistent_sim_fixture.Options {
    return .{
        .main_no_sync = opts.main_no_sync,
        .main_no_meta_sync = opts.main_no_meta_sync,
        .wal_no_sync = opts.wal_no_sync,
        .main_commit_backend = switch (opts.main_commit_backend) {
            .sync => .sync,
            .worker_thread => .worker_thread,
            .async_io => .async_io,
            .adaptive => .adaptive,
        },
        .wal_commit_backend = switch (opts.wal_commit_backend) {
            .sync => .sync,
            .worker_thread => .worker_thread,
            .async_io => .async_io,
            .adaptive => .adaptive,
        },
    };
}

fn persistentSimSummaryFromIndex(alloc: Allocator, pi: *PersistentIndex) !PersistentSimSummary {
    const ranges = try pi.activeSegmentRanges(alloc);
    defer {
        for (ranges) |*range| range.deinit(alloc);
        alloc.free(ranges);
    }

    const snap = pi.snapshot();
    return .{
        .doc_count = snap.liveDocCount(),
        .segment_count = ranges.len,
        .alpha_hits = try persistentSearchHitCount(alloc, snap, "alpha"),
        .beta_hits = try persistentSearchHitCount(alloc, snap, "beta"),
        .gamma_hits = try persistentSearchHitCount(alloc, snap, "gamma"),
    };
}

fn persistentSearchHitCount(alloc: Allocator, snap: *index_mod.IndexSnapshot, term: []const u8) !usize {
    const results = try snap.search(alloc, "body", &.{term}, 32);
    defer alloc.free(results.hits);
    return @intCast(results.total_count);
}

fn expectedPersistentSummary(actions: []const PersistentSimAction) PersistentSimSummary {
    var summary: PersistentSimSummary = .{
        .doc_count = 0,
        .segment_count = 0,
        .alpha_hits = 0,
        .beta_hits = 0,
        .gamma_hits = 0,
    };
    for (actions) |action| {
        switch (action) {
            .reopen => {},
            .index_segment => |spec| switch (spec) {
                .alpha => {
                    summary.doc_count += 1;
                    summary.segment_count += 1;
                    summary.alpha_hits += 1;
                },
                .beta => {
                    summary.doc_count += 1;
                    summary.segment_count += 1;
                    summary.beta_hits += 1;
                },
                .gamma_pair => {
                    summary.doc_count += 2;
                    summary.segment_count += 1;
                    summary.gamma_hits += 2;
                },
                .alpha_beta_pair => {
                    summary.doc_count += 2;
                    summary.segment_count += 1;
                    summary.alpha_hits += 1;
                    summary.beta_hits += 1;
                },
            },
        }
    }
    return summary;
}

fn expectPersistentSummaryFields(
    fixture_name: []const u8,
    opts: persistent_sim_fixture.Options,
    summary: PersistentSimSummary,
) !void {
    if (opts.expected_doc_count) |expected| {
        try sim_fixture.expectFieldEqual(fixture_name, "expected_doc_count", expected, summary.doc_count);
    }
    if (opts.expected_segment_count) |expected| {
        try sim_fixture.expectFieldEqual(fixture_name, "expected_segment_count", expected, summary.segment_count);
    }
    if (opts.expected_alpha_hits) |expected| {
        try sim_fixture.expectFieldEqual(fixture_name, "expected_alpha_hits", expected, summary.alpha_hits);
    }
    if (opts.expected_beta_hits) |expected| {
        try sim_fixture.expectFieldEqual(fixture_name, "expected_beta_hits", expected, summary.beta_hits);
    }
    if (opts.expected_gamma_hits) |expected| {
        try sim_fixture.expectFieldEqual(fixture_name, "expected_gamma_hits", expected, summary.gamma_hits);
    }
}

fn expectPersistentSummariesEqual(
    fixture_name: []const u8,
    expected: PersistentSimSummary,
    actual: PersistentSimSummary,
) !void {
    try sim_fixture.expectFieldEqual(fixture_name, "expected_doc_count", expected.doc_count, actual.doc_count);
    try sim_fixture.expectFieldEqual(fixture_name, "expected_segment_count", expected.segment_count, actual.segment_count);
    try sim_fixture.expectFieldEqual(fixture_name, "expected_alpha_hits", expected.alpha_hits, actual.alpha_hits);
    try sim_fixture.expectFieldEqual(fixture_name, "expected_beta_hits", expected.beta_hits, actual.beta_hits);
    try sim_fixture.expectFieldEqual(fixture_name, "expected_gamma_hits", expected.gamma_hits, actual.gamma_hits);
}

fn expectPersistentCrashOutcome(
    fixture_name: []const u8,
    opts: persistent_sim_fixture.Options,
    outcome: PersistentCrashOutcome,
) !void {
    if (opts.expected_outcome) |expected| {
        try sim_fixture.expectFieldEqual(fixture_name, "expected_outcome", expected, outcome);
    }
}

fn persistentSimSegment(alloc: Allocator, spec: PersistentSimSegmentSpec, step: usize) ![]u8 {
    return switch (spec) {
        .alpha => blk: {
            const doc_id = try std.fmt.allocPrint(alloc, "doc:alpha:{d}", .{step});
            defer alloc.free(doc_id);
            break :blk try buildSimpleSegment(alloc, doc_id, "alpha");
        },
        .beta => blk: {
            const doc_id = try std.fmt.allocPrint(alloc, "doc:beta:{d}", .{step});
            defer alloc.free(doc_id);
            break :blk try buildSimpleSegment(alloc, doc_id, "beta");
        },
        .gamma_pair => blk: {
            const doc_id_a = try std.fmt.allocPrint(alloc, "doc:gamma:{d}:0", .{step});
            defer alloc.free(doc_id_a);
            const doc_id_b = try std.fmt.allocPrint(alloc, "doc:gamma:{d}:1", .{step});
            defer alloc.free(doc_id_b);
            break :blk try buildMultiDocSegment(alloc, &.{
                .{ .doc_id = doc_id_a, .term = "gamma" },
                .{ .doc_id = doc_id_b, .term = "gamma" },
            });
        },
        .alpha_beta_pair => blk: {
            const doc_id_a = try std.fmt.allocPrint(alloc, "doc:mix:{d}:a", .{step});
            defer alloc.free(doc_id_a);
            const doc_id_b = try std.fmt.allocPrint(alloc, "doc:mix:{d}:b", .{step});
            defer alloc.free(doc_id_b);
            break :blk try buildMultiDocSegment(alloc, &.{
                .{ .doc_id = doc_id_a, .term = "alpha" },
                .{ .doc_id = doc_id_b, .term = "beta" },
            });
        },
    };
}

fn applyPersistentReplayAction(pi: *PersistentIndex, alloc: Allocator, action: PersistentSimAction, step: usize) !void {
    switch (action) {
        .reopen => return,
        .index_segment => |spec| {
            const segment = try persistentSimSegment(alloc, spec, step);
            defer alloc.free(segment);
            try pi.indexSegment(segment);
        },
    }
}

fn replayPersistentSimActionsAtPath(
    alloc: Allocator,
    path: [*:0]const u8,
    opts: PersistentIndexOptions,
    actions: []const PersistentSimAction,
) !PersistentSimSummary {
    _ = path;
    var pi = try PersistentIndex.open(alloc, opts);
    var pi_open = true;
    defer if (pi_open) pi.close();

    for (actions, 0..) |action, step| {
        switch (action) {
            .reopen => {
                pi.close();
                pi_open = false;
                pi = try PersistentIndex.open(alloc, opts);
                pi_open = true;
            },
            else => try applyPersistentReplayAction(&pi, alloc, action, step),
        }
    }

    pi.close();
    pi_open = false;
    pi = try PersistentIndex.open(alloc, opts);
    pi_open = true;
    return try persistentSimSummaryFromIndex(alloc, &pi);
}

fn applyCommittedPersistentActionAtPath(
    alloc: Allocator,
    path: [*:0]const u8,
    opts: PersistentIndexOptions,
    step: usize,
    action: PersistentSimAction,
) !void {
    _ = path;
    var pi = try PersistentIndex.open(alloc, opts);
    defer pi.close();
    try applyPersistentReplayAction(&pi, alloc, action, step);
}

fn persistentSimOptionsAtPath(path: [*:0]const u8, opts: PersistentIndexOptions) PersistentIndexOptions {
    var copy = opts;
    copy.path = path;
    return copy;
}

fn persistTmpPathWithSuffix(buf: []u8, suffix: []const u8) [*:0]const u8 {
    const base = "/tmp/antfly-persist-test-";
    var random: [16]u8 = undefined;
    std.testing.io.random(&random);
    const nonce = @atomicRmw(u64, &persist_tmp_nonce, .Add, 1, .monotonic);
    const slice = std.fmt.bufPrint(buf, "{s}{x}-{d}-{s}\x00", .{ base, random, nonce, suffix }) catch unreachable;
    return @ptrCast(slice.ptr);
}

fn persistentReplayArtifactPath(buf: []u8, suffix: []const u8) []const u8 {
    const base = "/tmp/antfly-persistent-replay-";
    var random: [16]u8 = undefined;
    std.testing.io.random(&random);
    const nonce = @atomicRmw(u64, &persist_tmp_nonce, .Add, 1, .monotonic);
    return std.fmt.bufPrint(buf, "{s}{x}-{d}-{s}.fixture", .{ base, random, nonce, suffix }) catch unreachable;
}

fn writePersistentReplayArtifactFile(path: []const u8, contents: []const u8) !void {
    var file = try std.Io.Dir.createFileAbsolute(std.testing.io, path, .{});
    defer file.close(std.testing.io);

    var file_buf: [4096]u8 = undefined;
    var writer = file.writer(std.testing.io, &file_buf);
    try writer.interface.writeAll(contents);
    try writer.end();
}

fn writePersistentReplayFixtureArtifact(
    alloc: Allocator,
    opts: PersistentIndexOptions,
    case_label: []const u8,
    seed: u64,
    expectation_note: []const u8,
    summary: PersistentSimSummary,
    actions: []const PersistentSimAction,
) !?[]u8 {
    var path_buf: [256]u8 = undefined;
    const artifact_path = persistentReplayArtifactPath(&path_buf, case_label);
    const path = try alloc.dupe(u8, artifact_path);
    errdefer alloc.free(path);

    const normalized = try persistent_sim_fixture.renderReplayArtifact(
        alloc,
        blk: {
            var fixture_opts = fixtureOptionsFromPersistentOptions(opts);
            fixture_opts.expected_doc_count = summary.doc_count;
            fixture_opts.expected_segment_count = summary.segment_count;
            fixture_opts.expected_alpha_hits = summary.alpha_hits;
            fixture_opts.expected_beta_hits = summary.beta_hits;
            fixture_opts.expected_gamma_hits = summary.gamma_hits;
            break :blk fixture_opts;
        },
        case_label,
        seed,
        expectation_note,
        actions,
    );
    defer alloc.free(normalized);

    try writePersistentReplayArtifactFile(path, normalized);
    return path;
}

fn printPersistentAction(action: PersistentSimAction) !void {
    const line = try persistent_sim_fixture.renderAction(std.testing.allocator, action);
    defer std.testing.allocator.free(line);
    std.debug.print("    {s}\n", .{line});
}

fn reportReducedPersistentSchedule(
    alloc: Allocator,
    opts: PersistentIndexOptions,
    case_label: []const u8,
    seed: u64,
    actions: []const PersistentSimAction,
) !void {
    const Replayer = struct {
        alloc: Allocator,
        opts: PersistentIndexOptions,
        case_label: []const u8,

        pub fn replay(self: @This(), candidate: []const PersistentSimAction) !void {
            const expected = expectedPersistentSummary(candidate);
            var path_buf: [256]u8 = undefined;
            const tmp_path = persistTmpPathWithSuffix(&path_buf, "sim-reduce");
            defer cleanupPersistDir(tmp_path);
            const actual = try replayPersistentSimActionsAtPath(self.alloc, tmp_path, persistentSimOptionsAtPath(tmp_path, self.opts), candidate);
            try expectPersistentSummariesEqual(self.case_label, expected, actual);
        }
    };

    const reduced = try zig_lmdb.sim.reduceFailingSequence(
        PersistentSimAction,
        alloc,
        actions,
        Replayer{ .alloc = alloc, .opts = opts, .case_label = case_label },
    );
    defer alloc.free(reduced);

    const summary = expectedPersistentSummary(reduced);
    const artifact_path = writePersistentReplayFixtureArtifact(
        alloc,
        opts,
        case_label,
        seed,
        "expected persistent index replay to preserve WAL-backed segments across reopen cycles",
        summary,
        reduced,
    ) catch |err| blk: {
        std.debug.print("failed to write persistent replay artifact for {s}: {s}\n", .{ case_label, @errorName(err) });
        break :blk null;
    };
    defer if (artifact_path) |path| alloc.free(path);

    std.debug.print("reduced failing persistent schedule ({d} actions):\n", .{reduced.len});
    if (artifact_path) |path| std.debug.print("replay fixture: {s}\n", .{path});
    for (reduced) |action| try printPersistentAction(action);
}

fn randomPersistentAction(random: std.Random) PersistentSimAction {
    if (random.uintLessThan(u8, 5) == 0) return .reopen;
    const spec_index = random.uintLessThan(u8, 4);
    return .{ .index_segment = @fromBackingInt(@intCast(spec_index)) };
}

fn runPersistentReplayCase(
    alloc: Allocator,
    opts: PersistentIndexOptions,
    case_label: []const u8,
    seed: u64,
    steps: usize,
) !void {
    var prng = std.Random.DefaultPrng.init(seed);
    const random = prng.random();

    var actions = std.ArrayListUnmanaged(PersistentSimAction).empty;
    defer actions.deinit(alloc);

    for (0..steps) |_| {
        try actions.append(alloc, randomPersistentAction(random));
    }

    var path_buf: [256]u8 = undefined;
    const path = persistTmpPathWithSuffix(&path_buf, "sim-replay");
    defer cleanupPersistDir(path);

    const actual = replayPersistentSimActionsAtPath(alloc, path, persistentSimOptionsAtPath(path, opts), actions.items) catch |err| {
        reportReducedPersistentSchedule(alloc, opts, case_label, seed, actions.items) catch {};
        return err;
    };
    try expectPersistentSummariesEqual(case_label, expectedPersistentSummary(actions.items), actual);
}

fn replayPersistentFixtureFile(alloc: Allocator, name: []const u8) !void {
    const path = try std.fmt.allocPrint(alloc, "pkg/antfly-embedded/src/storage/persistent_sim_fixtures/{s}", .{name});
    defer alloc.free(path);

    const contents = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, path, alloc, .limited(64 * 1024));
    defer alloc.free(contents);

    var fixture = try persistent_sim_fixture.parseFixture(alloc, contents);
    defer fixture.deinit(alloc);

    var path_buf: [256]u8 = undefined;
    const tmp_path = persistTmpPathWithSuffix(&path_buf, "fixture");
    defer cleanupPersistDir(tmp_path);

    const opts = persistentSimOptionsToIndexOptions(tmp_path, fixture.opts);
    switch (fixture.mode) {
        .replay => {
            const summary = try replayPersistentSimActionsAtPath(alloc, tmp_path, opts, fixture.actions);
            try expectPersistentSummaryFields(fixture.case_label orelse fixture.label orelse name, fixture.opts, summary);
        },
        .crash => try replayModeledPersistentFixtureFile(alloc, name),
    }
}

fn replayModeledPersistentFixtureFile(alloc: Allocator, name: []const u8) !void {
    const path = try std.fmt.allocPrint(alloc, "pkg/antfly-embedded/src/storage/persistent_sim_fixtures/{s}", .{name});
    defer alloc.free(path);

    const contents = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, path, alloc, .limited(64 * 1024));
    defer alloc.free(contents);

    var fixture = try persistent_sim_fixture.parseFixture(alloc, contents);
    defer fixture.deinit(alloc);

    var runtime = storage_sim.Runtime.init(alloc);
    defer runtime.deinit();
    var device_model = storage_sim.ModeledDevice.init(alloc);
    defer device_model.deinit();

    const modeled_path: [*:0]const u8 = "/persistent-modeled-fixture";
    const opts = persistentModeledOptionsToIndexOptions(modeled_path, fixture.opts, &device_model, &runtime);
    switch (fixture.mode) {
        .replay => {
            const summary = try replayPersistentSimActionsAtPath(alloc, modeled_path, opts, fixture.actions);
            try expectPersistentSummaryFields(fixture.case_label orelse fixture.label orelse name, fixture.opts, summary);
        },
        .crash => {
            const outcome = try replayModeledPersistentCrashFixture(
                alloc,
                modeled_path,
                opts,
                fixture.case_label orelse fixture.label orelse name,
                fixture.prelude_actions,
                fixture.crash_action orelse return error.InvalidFixture,
                &device_model,
            );
            try expectPersistentCrashOutcome(fixture.case_label orelse fixture.label orelse name, fixture.opts, outcome);
        },
    }
}

fn replayModeledPersistentCrashFixture(
    alloc: Allocator,
    path: [*:0]const u8,
    opts: PersistentIndexOptions,
    case_label: []const u8,
    prelude_actions: []const PersistentSimAction,
    crash_action: PersistentSimAction,
    device_model: *storage_sim.ModeledDevice,
) !PersistentCrashOutcome {
    _ = try replayPersistentSimActionsAtPath(alloc, path, opts, prelude_actions);
    try applyCommittedPersistentActionAtPath(alloc, path, opts, prelude_actions.len, crash_action);
    try device_model.device().crash();

    const actual = try replayPersistentSimActionsAtPath(alloc, path, opts, &.{});
    const full_actions = try alloc.alloc(PersistentSimAction, prelude_actions.len + 1);
    defer alloc.free(full_actions);
    @memcpy(full_actions[0..prelude_actions.len], prelude_actions);
    full_actions[prelude_actions.len] = crash_action;
    try expectPersistentSummariesEqual(case_label, expectedPersistentSummary(full_actions), actual);
    return .committed;
}

fn runPersistentReplayFixtures(alloc: Allocator) !void {
    var fixtures_dir = std.Io.Dir.cwd().openDir(std.testing.io, "pkg/antfly-embedded/src/storage/persistent_sim_fixtures", .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };
    defer fixtures_dir.close(std.testing.io);

    var fixture_names: std.ArrayListUnmanaged([]u8) = .empty;
    defer {
        for (fixture_names.items) |name| alloc.free(name);
        fixture_names.deinit(alloc);
    }

    var walker = try fixtures_dir.walk(alloc);
    defer walker.deinit();

    while (try walker.next(std.testing.io)) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.path, ".fixture")) continue;
        try fixture_names.append(alloc, try alloc.dupe(u8, entry.path));
    }

    std.mem.sort([]u8, fixture_names.items, {}, struct {
        fn lessThan(_: void, lhs: []u8, rhs: []u8) bool {
            return std.mem.lessThan(u8, lhs, rhs);
        }
    }.lessThan);

    for (fixture_names.items) |name| try replayPersistentFixtureFile(alloc, name);
}

fn runModeledPersistentFixtures(alloc: Allocator) !void {
    var fixtures_dir = std.Io.Dir.cwd().openDir(std.testing.io, "pkg/antfly-embedded/src/storage/persistent_sim_fixtures", .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };
    defer fixtures_dir.close(std.testing.io);

    var fixture_names: std.ArrayListUnmanaged([]u8) = .empty;
    defer {
        for (fixture_names.items) |name| alloc.free(name);
        fixture_names.deinit(alloc);
    }

    var walker = try fixtures_dir.walk(alloc);
    defer walker.deinit();

    while (try walker.next(std.testing.io)) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.path, ".fixture")) continue;
        try fixture_names.append(alloc, try alloc.dupe(u8, entry.path));
    }

    std.mem.sort([]u8, fixture_names.items, {}, struct {
        fn lessThan(_: void, lhs: []u8, rhs: []u8) bool {
            return std.mem.lessThan(u8, lhs, rhs);
        }
    }.lessThan);

    for (fixture_names.items) |name| try replayModeledPersistentFixtureFile(alloc, name);
}

fn runModeledPersistentReplayCase(
    alloc: Allocator,
    case_label: []const u8,
    seed: u64,
    steps: usize,
) !void {
    var runtime = storage_sim.Runtime.init(alloc);
    defer runtime.deinit();
    var device_model = storage_sim.ModeledDevice.init(alloc);
    defer device_model.deinit();

    var prng = std.Random.DefaultPrng.init(seed);
    const random = prng.random();

    var actions = std.ArrayListUnmanaged(PersistentSimAction).empty;
    defer actions.deinit(alloc);
    for (0..steps) |_| {
        try actions.append(alloc, randomPersistentAction(random));
    }

    const path: [*:0]const u8 = "/persistent-modeled-sim";
    const opts = persistentModeledOptionsToIndexOptions(path, .{
        .main_commit_backend = .async_io,
        .wal_commit_backend = .worker_thread,
    }, &device_model, &runtime);
    const actual = try replayPersistentSimActionsAtPath(alloc, path, opts, actions.items);
    try device_model.device().crash();
    const reopened = try replayPersistentSimActionsAtPath(alloc, path, opts, &.{});
    try expectPersistentSummariesEqual(case_label, actual, reopened);
    try expectPersistentSummariesEqual(case_label, expectedPersistentSummary(actions.items), reopened);
}

fn crashReopenModeledPersistentIndex(
    alloc: Allocator,
    pi: *PersistentIndex,
    device_model: *storage_sim.ModeledDevice,
    opts: PersistentIndexOptions,
) !void {
    pi.close();
    try device_model.device().crash();
    pi.* = try PersistentIndex.open(alloc, opts);
}

/// Narrow live-world seam for the common VOPR adapter. It deliberately owns
/// the same modeled device/runtime and real PersistentIndex used by the
/// focused fixture suite; exploration policy remains outside this domain.
pub const VoprHarness = struct {
    alloc: Allocator,
    runtime: storage_sim.Runtime,
    device_model: storage_sim.ModeledDevice,
    opts: PersistentIndexOptions,
    index: PersistentIndex,
    index_open: bool,
    actions: std.ArrayListUnmanaged(VoprAction) = .empty,
    recovered: bool = false,

    pub fn init(alloc: Allocator) !*VoprHarness {
        const self = try alloc.create(VoprHarness);
        errdefer alloc.destroy(self);
        self.alloc = alloc;
        self.runtime = storage_sim.Runtime.init(alloc);
        errdefer self.runtime.deinit();
        self.device_model = storage_sim.ModeledDevice.init(alloc);
        errdefer self.device_model.deinit();
        self.opts = persistentModeledOptionsToIndexOptions("/persistent-vopr", .{
            .main_commit_backend = .async_io,
            .wal_commit_backend = .worker_thread,
        }, &self.device_model, &self.runtime);
        self.index = try PersistentIndex.open(alloc, self.opts);
        self.index_open = true;
        self.actions = .empty;
        self.recovered = false;
        return self;
    }

    pub fn deinit(self: *VoprHarness) void {
        if (self.index_open) self.index.close();
        self.actions.deinit(self.alloc);
        self.device_model.deinit();
        self.runtime.deinit();
        const alloc = self.alloc;
        self.* = undefined;
        alloc.destroy(self);
    }

    pub fn apply(self: *VoprHarness, action: VoprAction) !void {
        const step = self.actions.items.len;
        switch (action) {
            .reopen => try self.reopen(),
            else => try applyPersistentReplayAction(&self.index, self.alloc, action, step),
        }
        try self.actions.append(self.alloc, action);
    }

    pub fn crashAndRecover(self: *VoprHarness) !void {
        self.index.close();
        self.index_open = false;
        try self.device_model.device().crash();
        self.index = try PersistentIndex.open(self.alloc, self.opts);
        self.index_open = true;
        self.recovered = true;
    }

    pub fn summary(self: *VoprHarness) !VoprSummary {
        return persistentSimSummaryFromIndex(self.alloc, &self.index);
    }

    pub fn expected(self: *const VoprHarness) VoprSummary {
        return expectedPersistentSummary(self.actions.items);
    }

    fn reopen(self: *VoprHarness) !void {
        self.index.close();
        self.index_open = false;
        self.index = try PersistentIndex.open(self.alloc, self.opts);
        self.index_open = true;
    }
};

fn buildCompactedTextSegment(alloc: Allocator) ![]u8 {
    return try buildMultiDocSegment(alloc, &.{
        .{ .doc_id = "doc:a", .term = "fresh" },
        .{ .doc_id = "doc:b", .term = "beta" },
    });
}

fn seedTextCompactionInputs(pi: *PersistentIndex, alloc: Allocator) !void {
    const stale = try buildSimpleSegment(alloc, "doc:a", "stale");
    defer alloc.free(stale);
    try pi.indexSegment(stale);

    const beta = try buildSimpleSegment(alloc, "doc:b", "beta");
    defer alloc.free(beta);
    try pi.indexSegment(beta);
}

fn expectPersistentTextCompactionView(
    alloc: Allocator,
    pi: *PersistentIndex,
    expected_segments: usize,
    stale_hits: usize,
    fresh_hits: usize,
    beta_hits: usize,
) !void {
    const ranges = try pi.activeSegmentRanges(alloc);
    defer {
        for (ranges) |*range| range.deinit(alloc);
        alloc.free(ranges);
    }

    const snap = pi.snapshot();
    try std.testing.expectEqual(@as(u32, 2), snap.liveDocCount());
    try std.testing.expectEqual(expected_segments, ranges.len);
    try std.testing.expectEqual(stale_hits, try persistentSearchHitCount(alloc, snap, "stale"));
    try std.testing.expectEqual(fresh_hits, try persistentSearchHitCount(alloc, snap, "fresh"));
    try std.testing.expectEqual(beta_hits, try persistentSearchHitCount(alloc, snap, "beta"));
}

fn expectModeledPersistentReplaceError(result: anyerror!void) !void {
    result catch |err| switch (err) {
        error.InjectedWriteFault, error.InjectedSyncFault => return,
        else => return err,
    };
    return error.TestExpectedError;
}

test "persistent modeled full-text compaction publish faults stay green" {
    const alloc = std.testing.allocator;

    {
        var runtime = storage_sim.Runtime.init(alloc);
        defer runtime.deinit();
        var device_model = storage_sim.ModeledDevice.init(alloc);
        defer device_model.deinit();

        const path: [*:0]const u8 = "/persistent-text-compaction-segment-write-fault";
        const opts = persistentModeledOptionsToIndexOptions(path, .{}, &device_model, &runtime);
        var pi = try PersistentIndex.open(alloc, opts);
        defer pi.close();

        try seedTextCompactionInputs(&pi, alloc);
        try crashReopenModeledPersistentIndex(alloc, &pi, &device_model, opts);
        try expectPersistentTextCompactionView(alloc, &pi, 2, 1, 0, 1);

        const compacted = try buildCompactedTextSegment(alloc);
        defer alloc.free(compacted);
        try device_model.injectWriteFailureForPathContains(".seg");
        try expectModeledPersistentReplaceError(pi.replaceSegments(&.{ 1, 2 }, compacted));
        try crashReopenModeledPersistentIndex(alloc, &pi, &device_model, opts);
        try expectPersistentTextCompactionView(alloc, &pi, 2, 1, 0, 1);
    }

    {
        var runtime = storage_sim.Runtime.init(alloc);
        defer runtime.deinit();
        var device_model = storage_sim.ModeledDevice.init(alloc);
        defer device_model.deinit();

        const path: [*:0]const u8 = "/persistent-text-compaction-catalog-sync-fault";
        const opts = persistentModeledOptionsToIndexOptions(path, .{}, &device_model, &runtime);
        var pi = try PersistentIndex.open(alloc, opts);
        defer pi.close();

        try seedTextCompactionInputs(&pi, alloc);
        try crashReopenModeledPersistentIndex(alloc, &pi, &device_model, opts);

        const compacted = try buildCompactedTextSegment(alloc);
        defer alloc.free(compacted);
        try device_model.injectSyncFailureForPathContains("/index/");
        try expectModeledPersistentReplaceError(pi.replaceSegments(&.{ 1, 2 }, compacted));
        try crashReopenModeledPersistentIndex(alloc, &pi, &device_model, opts);
        try expectPersistentTextCompactionView(alloc, &pi, 2, 1, 0, 1);
    }

    {
        var runtime = storage_sim.Runtime.init(alloc);
        defer runtime.deinit();
        var device_model = storage_sim.ModeledDevice.init(alloc);
        defer device_model.deinit();

        const path: [*:0]const u8 = "/persistent-text-compaction-after-catalog-publish";
        const opts = persistentModeledOptionsToIndexOptions(path, .{}, &device_model, &runtime);
        var pi = try PersistentIndex.open(alloc, opts);
        defer pi.close();

        try seedTextCompactionInputs(&pi, alloc);
        try crashReopenModeledPersistentIndex(alloc, &pi, &device_model, opts);

        const compacted = try buildCompactedTextSegment(alloc);
        defer alloc.free(compacted);
        const retired_path = try pi.segment_files.?.pathAlloc(1);
        defer alloc.free(retired_path);
        try pi.replaceSegmentsCatalogOnlyForTest(&.{ 1, 2 }, compacted);
        pi.prepareForCrashRollback();
        try device_model.device().crash();
        pi.abandonAfterCrash();

        // The first writable owner sees the durable retirement marker, but its
        // initial unlink fails. The next ordinary segment publication must
        // retry from live-owner state rather than requiring another restart.
        try device_model.injectDeleteFailureForPathContains("1.seg");
        pi = try PersistentIndex.open(alloc, opts);
        try expectPersistentTextCompactionView(alloc, &pi, 1, 0, 1, 1);
        _ = try device_model.fileSize(retired_path);
        {
            var txn = try pi.beginReadMainTxn();
            defer txn.abort();
            const retired_key = retiredSegmentMetaKey(1);
            _ = try txn.get(.meta, &retired_key);
        }

        const followup = try buildSimpleSegment(alloc, "doc:followup", "followup");
        defer alloc.free(followup);
        try pi.indexSegment(followup);
        try std.testing.expectError(error.FileNotFound, device_model.fileSize(retired_path));
        {
            var txn = try pi.beginReadMainTxn();
            defer txn.abort();
            const retired_key = retiredSegmentMetaKey(1);
            try std.testing.expectError(error.NotFound, txn.get(.meta, &retired_key));
        }
    }

    {
        var runtime = storage_sim.Runtime.init(alloc);
        defer runtime.deinit();
        var device_model = storage_sim.ModeledDevice.init(alloc);
        defer device_model.deinit();

        const path: [*:0]const u8 = "/persistent-text-compaction-cleanup-fault";
        const opts = persistentModeledOptionsToIndexOptions(path, .{}, &device_model, &runtime);
        var pi = try PersistentIndex.open(alloc, opts);
        defer pi.close();

        try seedTextCompactionInputs(&pi, alloc);
        try crashReopenModeledPersistentIndex(alloc, &pi, &device_model, opts);

        const compacted = try buildCompactedTextSegment(alloc);
        defer alloc.free(compacted);
        const retired_path = try pi.segment_files.?.pathAlloc(1);
        defer alloc.free(retired_path);
        try device_model.injectDeleteFailureForPathContains("1.seg");
        try pi.replaceSegments(&.{ 1, 2 }, compacted);
        _ = try device_model.fileSize(retired_path);
        {
            var txn = try pi.beginReadMainTxn();
            defer txn.abort();
            const retired_key = retiredSegmentMetaKey(1);
            _ = try txn.get(.meta, &retired_key);
        }
        pi.prepareForCrashRollback();
        try device_model.device().crash();
        pi.abandonAfterCrash();
        pi = try PersistentIndex.open(alloc, opts);
        try expectPersistentTextCompactionView(alloc, &pi, 1, 0, 1, 1);
        try std.testing.expectError(error.FileNotFound, device_model.fileSize(retired_path));
        {
            var txn = try pi.beginReadMainTxn();
            defer txn.abort();
            const retired_key = retiredSegmentMetaKey(1);
            try std.testing.expectError(error.NotFound, txn.get(.meta, &retired_key));
        }
    }
}

test "persistent modeled reset publishes before reclamation and fences crash cleanup" {
    const alloc = std.testing.allocator;
    var runtime = storage_sim.Runtime.init(alloc);
    defer runtime.deinit();
    var device_model = storage_sim.ModeledDevice.init(alloc);
    defer device_model.deinit();

    const path: [*:0]const u8 = "/persistent-reset-crash-fence";
    const opts = persistentModeledOptionsToIndexOptions(path, .{}, &device_model, &runtime);
    var pi = try PersistentIndex.open(alloc, opts);
    defer pi.close();

    const segment = try buildSimpleSegment(alloc, "doc:old", "old");
    defer alloc.free(segment);
    try pi.indexSegment(segment);
    try crashReopenModeledPersistentIndex(alloc, &pi, &device_model, opts);

    const retained = pi.acquireSnapshot();
    var retained_open = true;
    defer if (retained_open) retained.release();
    const retired_path = try pi.segment_files.?.pathAlloc(1);
    defer alloc.free(retired_path);

    try pi.resetAllForRebuild();
    try std.testing.expectEqual(@as(u32, 0), pi.snapshot().liveDocCount());

    // Fence callbacks before rollback. The modeled device restores the old
    // durable directory entry, and releasing the pre-reset snapshot afterward
    // must not delete that restored generation's file.
    pi.prepareForCrashRollback();
    try device_model.device().crash();
    _ = try device_model.fileSize(retired_path);
    pi.abandonAfterCrash();
    retained.release();
    retained_open = false;
    _ = try device_model.fileSize(retired_path);

    pi = try PersistentIndex.open(alloc, opts);
    try std.testing.expectEqual(@as(u32, 0), pi.snapshot().liveDocCount());
    try std.testing.expectError(error.FileNotFound, device_model.fileSize(retired_path));
    var txn = try pi.beginReadMainTxn();
    defer txn.abort();
    const retired_key = retiredSegmentMetaKey(1);
    try std.testing.expectError(error.NotFound, txn.get(.meta, &retired_key));
}

test "persistent modeled reset commit failure preserves the published generation" {
    const alloc = std.testing.allocator;
    var runtime = storage_sim.Runtime.init(alloc);
    defer runtime.deinit();
    var device_model = storage_sim.ModeledDevice.init(alloc);
    defer device_model.deinit();

    const path: [*:0]const u8 = "/persistent-reset-catalog-fault";
    const opts = persistentModeledOptionsToIndexOptions(path, .{}, &device_model, &runtime);
    var pi = try PersistentIndex.open(alloc, opts);
    defer pi.close();

    const segment = try buildSimpleSegment(alloc, "doc:old", "old");
    defer alloc.free(segment);
    try pi.indexSegment(segment);
    try crashReopenModeledPersistentIndex(alloc, &pi, &device_model, opts);

    const old_path = try pi.segment_files.?.pathAlloc(1);
    defer alloc.free(old_path);
    try device_model.injectSyncFailureForPathContains("/index/");
    try expectModeledPersistentReplaceError(pi.resetAllForRebuild());
    try std.testing.expectEqual(@as(u32, 1), pi.snapshot().liveDocCount());
    _ = try device_model.fileSize(old_path);

    try crashReopenModeledPersistentIndex(alloc, &pi, &device_model, opts);
    try std.testing.expectEqual(@as(u32, 1), pi.snapshot().liveDocCount());
    try std.testing.expectEqual(@as(usize, 1), try persistentSearchHitCount(alloc, pi.snapshot(), "old"));
    _ = try device_model.fileSize(old_path);
}

fn runPersistentSoak(alloc: Allocator) !void {
    try runPersistentReplayCase(alloc, .{ .path = "" }, "persistent-soak-default-a", 0xA17F_B201, 72);
    try runPersistentReplayCase(alloc, .{ .path = "" }, "persistent-soak-default-b", 0xA17F_B202, 72);
    try runPersistentReplayCase(
        alloc,
        .{ .path = "", .main_commit_backend = .async_io, .wal_commit_backend = .worker_thread },
        "persistent-soak-async-main",
        0xA17F_B203,
        60,
    );
}

test "persistent sim workloads stay green" {
    const alloc = std.testing.allocator;
    try runPersistentReplayCase(alloc, .{ .path = "" }, "persistent-default", 0xA17F_B001, 16);
    try runPersistentReplayCase(alloc, .{ .path = "", .main_commit_backend = .async_io, .wal_commit_backend = .worker_thread }, "persistent-async-main", 0xA17F_B002, 14);
}

test "persistent replay fixtures stay green" {
    try runPersistentReplayFixtures(std.testing.allocator);
}

test "persistent modeled replay fixtures stay green" {
    try runModeledPersistentFixtures(std.testing.allocator);
}

test "persistent modeled sim workload stays green" {
    try runModeledPersistentReplayCase(std.testing.allocator, "persistent-modeled-sim", 0xA17F_B501, 18);
}

test "persistent sim soak stays green" {
    if (!storage_sim_soak) return;
    try runPersistentSoak(std.testing.allocator);
}

test "persistent rebuild page publishes segments and cursor atomically across reopen" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    defer cleanupPersistDir(path);
    {
        var pi = try PersistentIndex.open(alloc, .{ .path = path });
        defer pi.close();
        var page: PersistentIndex.RebuildPage = .{ .index = &pi, .cursor = "doc2" };
        defer page.deinit();
        try page.begin();
        const first = try buildSimpleSegment(alloc, "doc1", "hello");
        defer alloc.free(first);
        const second = try buildSimpleSegment(alloc, "doc2", "world");
        defer alloc.free(second);
        try pi.indexSegment(first);
        try pi.indexSegment(second);
        try std.testing.expectEqual(@as(u32, 0), pi.snapshot().liveDocCount());
        try std.testing.expect((try pi.rebuildCursorAlloc(alloc)) == null);
        try page.commit();
        try std.testing.expectEqual(@as(u32, 2), pi.snapshot().liveDocCount());
        try std.testing.expectEqual(@as(u64, 2), pi.snapshot().global_total_field_len.get("body").?);
        const cursor = (try pi.rebuildCursorAlloc(alloc)).?;
        defer alloc.free(cursor);
        try std.testing.expectEqualStrings("doc2", cursor);
        // Aborting a later partial page must not publish any of its segments.
        var aborted: PersistentIndex.RebuildPage = .{ .index = &pi, .cursor = "doc3" };
        defer aborted.deinit();
        try aborted.begin();
        const third = try buildSimpleSegment(alloc, "doc3", "aborted");
        defer alloc.free(third);
        try pi.indexSegment(third);
    }
    {
        var pi = try PersistentIndex.open(alloc, .{ .path = path });
        defer pi.close();
        try std.testing.expectEqual(@as(u32, 2), pi.snapshot().liveDocCount());
        try std.testing.expectEqual(@as(u64, 2), pi.snapshot().global_total_field_len.get("body").?);
        const cursor = (try pi.rebuildCursorAlloc(alloc)).?;
        defer alloc.free(cursor);
        try std.testing.expectEqualStrings("doc2", cursor);
        var appended: PersistentIndex.RebuildPage = .{ .index = &pi, .cursor = "doc3" };
        defer appended.deinit();
        try appended.begin();
        const third = try buildSimpleSegment(alloc, "doc3", "committed");
        defer alloc.free(third);
        try pi.indexSegment(third);
        try appended.commit();
        try std.testing.expectEqual(@as(u32, 3), pi.snapshot().liveDocCount());
        try std.testing.expectEqual(@as(u64, 3), pi.snapshot().global_total_field_len.get("body").?);
        // Filtered pages can advance the cursor without an output segment.
        var empty: PersistentIndex.RebuildPage = .{ .index = &pi, .cursor = "doc4" };
        defer empty.deinit();
        try empty.begin();
        try empty.commit();
        const advanced = (try pi.rebuildCursorAlloc(alloc)).?;
        defer alloc.free(advanced);
        try std.testing.expectEqualStrings("doc4", advanced);
        try std.testing.expectEqual(@as(u32, 3), pi.snapshot().liveDocCount());
        try std.testing.expectEqual(@as(u64, 3), pi.snapshot().global_total_field_len.get("body").?);
    }
}

test "persistent rebuild page publication scaling benchmark" {
    if (!envEnabled("ANTFLY_BENCH_REBUILD_PAGE")) return error.SkipZigTest;
    const alloc = std.testing.allocator;
    for ([_]usize{ 16_384, 65_536, 131_072 }) |count| {
        for ([_]bool{ false, true }) |atomic_page| {
            var path_buf: [256]u8 = undefined;
            const path = persistTmpPath(&path_buf);
            defer cleanupPersistDir(path);
            var pi = try PersistentIndex.open(alloc, .{ .path = path });
            defer pi.close();
            var elapsed_ns: u64 = 0;
            var start: usize = 0;
            while (start < count) : (start += 256) {
                const segment = try buildHighFrequencyKeywordSegmentRange(alloc, start, 256);
                defer alloc.free(segment);
                var keys: [256][32]u8 = undefined;
                var ids: [256][]const u8 = undefined;
                for (&ids, 0..) |*id, i| id.* = try std.fmt.bufPrint(&keys[i], "doc:{d}", .{start + i});
                const began = platform_time.monotonicNs();
                if (atomic_page) {
                    var page: PersistentIndex.RebuildPage = .{ .index = &pi, .cursor = ids[255] };
                    defer page.deinit();
                    try page.begin();
                    try pi.indexSegment(segment);
                    try page.commit();
                } else {
                    const deleted = try pi.deleteByIdsTracked(&ids);
                    defer pi.freeDeleteInfos(deleted);
                    try pi.indexSegment(segment);
                }
                elapsed_ns += platform_time.monotonicNs() - began;
            }
            try std.testing.expectEqual(@as(u32, @intCast(count)), pi.snapshot().liveDocCount());
            std.debug.print("rebuild_page_bench docs={} atomic={} publication_ns={} segments={}\n", .{ count, atomic_page, elapsed_ns, count / 256 });
        }
    }
}

test "lite persistent owned publication merge recovery and retained snapshot ownership without mapping" {
    if (builtin.os.tag != .linux and builtin.os.tag != .macos) return error.SkipZigTest;
    const a = std.testing.allocator;
    const lite_docs = @import("lite/docstore.zig");
    const lite_indexes = @import("lite/index_storage.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/persistent-mapped.aflite", .{tmp.sub_path});
    defer a.free(path);
    var docs = try lite_docs.Store.createWithOptions(a, path, .{ .no_sync = true, .io = std.testing.io });
    var docs_closed = false;
    defer if (!docs_closed) docs.close();
    var storage = lite_indexes.Store.init(a, &docs);
    // Model a leased backend whose atomic writer only provides finish().
    // Neither writer publication nor storage reopening has a mapping callback.
    const PlainWriter = struct {
        allocator: Allocator,
        inner: storage_io.AtomicWriteSink,
        const vtable: storage_io.AtomicWriteSink.VTable = .{
            .len = len,
            .append_slice = append,
            .write_at = writeAt,
            .crc32_prefix = prefix,
            .crc32_range = range,
            .finish = finish,
            .abort = abort,
        };
        fn begin(ptr: *anyopaque, alloc: Allocator, output: []const u8) !storage_io.AtomicWriteSink {
            const self = try alloc.create(@This());
            errdefer alloc.destroy(self);
            const original: *lite_indexes.Store = @ptrCast(@alignCast(ptr));
            self.* = .{ .allocator = alloc, .inner = try original.storage().beginAtomicWrite(alloc, output) };
            return .{ .ptr = self, .vtable = &vtable };
        }
        fn get(ptr: *anyopaque) *@This() {
            return @ptrCast(@alignCast(ptr));
        }
        fn len(ptr: *anyopaque) usize {
            return get(ptr).inner.len();
        }
        fn append(ptr: *anyopaque, bytes: []const u8) !void {
            try get(ptr).inner.appendSlice(bytes);
        }
        fn writeAt(ptr: *anyopaque, offset: usize, bytes: []const u8) !void {
            try get(ptr).inner.writeAt(offset, bytes);
        }
        fn prefix(ptr: *anyopaque, length: usize) !u32 {
            return get(ptr).inner.crc32Prefix(length);
        }
        fn range(ptr: *anyopaque, offset: usize, length: usize) !u32 {
            return get(ptr).inner.crc32Range(offset, length);
        }
        fn finish(ptr: *anyopaque) !void {
            const self = get(ptr);
            defer self.allocator.destroy(self);
            try self.inner.finish();
        }
        fn abort(ptr: *anyopaque) void {
            const self = get(ptr);
            defer self.allocator.destroy(self);
            self.inner.abort();
        }
    };
    var source_storage = storage.storage();
    var source_vtable = source_storage.vtable.*;
    source_vtable.map_immutable_artifact = null;
    source_vtable.begin_atomic_write = PlainWriter.begin;
    source_storage.vtable = &source_vtable;
    const opts = PersistentIndexOptions{
        .path = "/lite-text",
        .io = std.testing.io,
        .main_lsm_storage = source_storage,
        .wal_storage = source_storage,
        .main_no_sync = true,
        .wal_no_sync = true,
    };
    var index = try PersistentIndex.open(a, opts);
    var index_closed = false;
    defer if (!index_closed) index.close();
    const first = try buildSimpleSegment(a, "doc:a", "alpha");
    defer a.free(first);
    const second = try buildSimpleSegment(a, "doc:b", "beta");
    defer a.free(second);
    const Builder = struct {
        bytes: []const u8,
        fn build(ptr: *anyopaque, sink: *segment_mod.SegmentSink) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try sink.appendSlice(self.bytes);
        }
    };
    var builder = Builder{ .bytes = first };
    _ = try index.indexSegmentFromSinkBuilder(&builder, Builder.build);
    try index.indexSegment(second);
    try std.testing.expect(index.supportsFileBackedSegmentArtifacts());
    const pinned = index.acquireSnapshot();
    defer pinned.release();
    try std.testing.expectEqual(@as(usize, 2), pinned.segments.len);
    for (pinned.segments) |segment| try std.testing.expect(segment.data == .native);
    const old_ids = [_]u64{ pinned.segments[0].id, pinned.segments[1].id };
    const prepared = try index.prepareMergedSegmentToFile(pinned, &.{ 0, 1 });
    try std.testing.expect(prepared[0].data == .native);
    try std.testing.expect(try index.replaceSegmentsIfActiveManyPrepared(&old_ids, prepared));
    try std.testing.expectEqual(@as(usize, 1), index.snapshot().segments.len);
    try std.testing.expectEqual(@as(usize, 1), try persistentSearchHitCount(a, index.snapshot(), "alpha"));
    try std.testing.expectEqual(@as(usize, 1), try persistentSearchHitCount(a, index.snapshot(), "beta"));
    try index.syncMain(true);
    index.close();
    index_closed = true;
    // Reopen retains native ranges without a private file or full payload copy.
    index = try PersistentIndex.open(a, opts);
    index_closed = false;
    for (index.snapshot().segments) |segment| try std.testing.expect(segment.data == .native);
    try std.testing.expectEqual(@as(usize, 1), try persistentSearchHitCount(a, index.snapshot(), "alpha"));
    try std.testing.expectEqual(@as(usize, 1), try persistentSearchHitCount(a, index.snapshot(), "beta"));
    index.close();
    index_closed = true;
    docs.close();
    docs_closed = true;
    // Export also uses the pinned native roots after both owners close.
    const export_root = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/native-export", .{tmp.sub_path});
    defer a.free(export_root);
    const checkpoint = PersistentIndex.NativeSegmentCheckpoint{ .snapshot = pinned, .source_root = "/unused-native-path" };
    const exported_bytes = try checkpoint.materialize(a, std.testing.io, export_root, CancellationToken.none);
    try std.testing.expectEqual(@as(u64, first.len + second.len), exported_bytes);
    for (pinned.segments, 0..) |segment, i| {
        const exported_path = try std.fmt.allocPrint(a, "{s}/{d}.seg", .{ export_root, segment.id });
        defer a.free(exported_path);
        const contents = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, exported_path, a, .limited(1024 * 1024));
        defer a.free(contents);
        try std.testing.expectEqualSlices(u8, if (i == 0) first else second, contents);
    }
    // The old snapshot's exact artifact leases survive both owner closures.
    try std.testing.expectEqual(@as(usize, 1), try persistentSearchHitCount(a, pinned, "alpha"));
    try std.testing.expectEqual(@as(usize, 1), try persistentSearchHitCount(a, pinned, "beta"));
}

test "persistent reopen falls back to heap reads when foreign mapping is unavailable" {
    const a = std.testing.allocator;
    const lite_docs = @import("lite/docstore.zig");
    const lite_indexes = @import("lite/index_storage.zig");
    const Foreign = struct {
        var map_calls: usize = 0;
        fn map(_: *anyopaque, _: Allocator, _: []const u8) anyerror!@import("../segment_source.zig").MappedArtifact {
            map_calls += 1;
            return error.MappedArtifactUnavailable;
        }
        fn dispatch(contract: *const @import("antfly_runtime_abi").native_abi.CallContract, callback: *const anyopaque, args: *const anyopaque, output: ?*anyopaque) callconv(.c) @import("antfly_runtime_abi").error_abi.Status {
            return @import("../runtime_callback_abi.zig").Boundary(storage_io.Storage.VTable).local_dispatch(contract, callback, args, output);
        }
    };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/mapping-fallback.aflite", .{tmp.sub_path});
    defer a.free(path);
    var docs = try lite_docs.Store.createWithOptions(a, path, .{ .no_sync = true, .io = std.testing.io });
    defer docs.close();
    var storage = lite_indexes.Store.init(a, &docs);
    var opts = PersistentIndexOptions{ .path = "/text", .io = std.testing.io, .main_lsm_storage = storage.storage(), .wal_storage = storage.storage(), .main_no_sync = true, .wal_no_sync = true };
    {
        var index = try PersistentIndex.open(a, opts);
        defer index.close();
        const bytes = try buildSimpleSegment(a, "doc:a", "alpha");
        defer a.free(bytes);
        try index.indexSegment(bytes);
        try index.syncMain(true);
    }
    var foreign_storage = storage.storage();
    var vtable = foreign_storage.vtable.*;
    // Exercise the existing heap fallback on a provider without native leases.
    vtable.open_leased_immutable_source = null;
    vtable.map_immutable_artifact = Foreign.map;
    foreign_storage.vtable = &vtable;
    foreign_storage.boundary_dispatch = Foreign.dispatch;
    opts.main_lsm_storage = foreign_storage;
    opts.wal_storage = foreign_storage;
    opts.read_only = true;
    Foreign.map_calls = 0;
    var index = try PersistentIndex.open(a, opts);
    defer index.close();
    try std.testing.expectEqual(@as(usize, 1), Foreign.map_calls);
    try std.testing.expect(index.snapshot().segments[0].data == .heap);
    try std.testing.expectEqual(@as(usize, 1), try persistentSearchHitCount(a, index.snapshot(), "alpha"));
}

test "lite persistent mapped read only fallback preserves unwritable directory queries" {
    if (builtin.os.tag != .linux and builtin.os.tag != .macos) return error.SkipZigTest;
    const a = std.testing.allocator;
    const lite_docs = @import("lite/docstore.zig");
    const lite_indexes = @import("lite/index_storage.zig");
    // chmod requires a real directory fd; Linux path-only (O_PATH) handles
    // cannot be passed to fchmod.
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/readonly-persistent.aflite", .{tmp.sub_path});
    defer a.free(path);
    {
        var docs = try lite_docs.Store.createWithOptions(a, path, .{ .no_sync = true, .io = std.testing.io });
        defer docs.close();
        var storage = lite_indexes.Store.init(a, &docs);
        var index = try PersistentIndex.open(a, .{ .path = "/text", .io = std.testing.io, .main_lsm_storage = storage.storage(), .wal_storage = storage.storage(), .main_no_sync = true, .wal_no_sync = true });
        defer index.close();
        const bytes = try buildSimpleSegment(a, "doc:a", "alpha");
        defer a.free(bytes);
        try index.indexSegment(bytes);
        try index.syncMain(true);
    }
    var docs = try lite_docs.Store.openWithOptions(a, path, .{ .read_only = true, .io = std.testing.io });
    defer docs.close();
    var storage = lite_indexes.Store.init(a, &docs);
    try tmp.dir.setPermissions(std.testing.io, .fromMode(0o555));
    defer tmp.dir.setPermissions(std.testing.io, .fromMode(0o700)) catch {};
    var index = try PersistentIndex.open(a, .{ .path = "/text", .io = std.testing.io, .read_only = true, .main_lsm_storage = storage.storage(), .wal_storage = storage.storage(), .main_no_sync = true, .wal_no_sync = true });
    defer index.close();
    try std.testing.expect(index.snapshot().segments[0].data == .native);
    try std.testing.expectEqual(@as(usize, 1), try persistentSearchHitCount(a, index.snapshot(), "alpha"));
    try std.testing.expectEqual(@as(u64, 0), (try docs.reclamationStatus()).artifact_file_bytes);
}

test "lite persistent mapped range repair streams oversized navigation and keys" {
    if (builtin.os.tag != .linux and builtin.os.tag != .macos) return error.SkipZigTest;
    const a = std.testing.allocator;
    const lite_docs = @import("lite/docstore.zig");
    const lite_indexes = @import("lite/index_storage.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/repair-mapped.aflite", .{tmp.sub_path});
    defer a.free(path);
    var docs = try lite_docs.Store.createWithOptions(a, path, .{ .no_sync = true, .io = std.testing.io, .reclamation = .{ .enabled = false } });
    defer docs.close();
    var storage = lite_indexes.Store.init(a, &docs);
    var index = try PersistentIndex.open(a, .{ .path = "/text", .io = std.testing.io, .main_lsm_storage = storage.storage(), .wal_storage = storage.storage(), .main_no_sync = true, .wal_no_sync = true });
    defer index.close();
    var writer = segment_mod.SegmentWriter.init(a);
    defer writer.deinit();
    try writer.addStoredDoc("id", "body");
    const name = try a.alloc(u8, 60000);
    defer a.free(name);
    @memset(name, 'f');
    for (0..18) |i| {
        name[0] = @intCast(i + 1);
        _ = try writer.addField(name);
    }
    const min_key = try a.alloc(u8, 600000);
    defer a.free(min_key);
    const max_key = try a.alloc(u8, 600000);
    defer a.free(max_key);
    @memset(min_key, 'a');
    @memset(max_key, 'z');
    try writer.addDocKeyRange(min_key, max_key);
    const bytes = try writer.build();
    defer a.free(bytes);
    try index.indexSegment(bytes);
    const id = index.snapshot().segments[0].id;
    {
        var txn = try index.beginWriteMainTxn();
        errdefer txn.abort();
        const range_key = segmentRangeMetaKey(id);
        try txn.delete(.meta, &range_key);
        try txn.commit();
    }
    var txn = try index.beginReadMainTxn();
    defer txn.abort();
    var budget = @import("lite/test_allocator.zig").BudgetAllocator{ .backing = a, .limit = min_key.len + max_key.len + 1024 + 64 * 1024 };
    var range = try index.loadSegmentRange(&txn, budget.allocator(), id);
    defer range.deinit(budget.allocator());
    try std.testing.expectEqualSlices(u8, min_key, range.min_doc_key);
    try std.testing.expectEqualSlices(u8, max_key, range.max_doc_key);
    try std.testing.expectEqual(min_key.len + max_key.len, budget.live);
    try std.testing.expect(budget.peak <= budget.limit);
}

test "lite persistent mapped fifty thousand sorted documents stream postings and reopen" {
    if (builtin.os.tag != .linux and builtin.os.tag != .macos) return error.SkipZigTest;
    const a = std.testing.allocator;
    const lite_docs = @import("lite/docstore.zig");
    const lite_indexes = @import("lite/index_storage.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/large-merge.aflite", .{tmp.sub_path});
    defer a.free(path);
    var docs = try lite_docs.Store.createWithOptions(a, path, .{ .no_sync = true, .io = std.testing.io });
    defer docs.close();
    var storage = lite_indexes.Store.init(a, &docs);
    const options = PersistentIndexOptions{ .path = "/text", .io = std.testing.io, .main_lsm_storage = storage.storage(), .wal_storage = storage.storage(), .main_no_sync = true, .wal_no_sync = true };
    var index = try PersistentIndex.open(a, options);
    var open = true;
    defer if (open) index.close();
    const Harness = struct {
        fn build(alloc: Allocator, parity: u32) ![]u8 {
            var segment = segment_mod.SegmentWriter.init(alloc);
            defer segment.deinit();
            var inverted = inverted_mod.InvertedIndexBuilder.init(alloc, .{});
            defer inverted.deinit();
            const typed = @import("../section/typed_doc_values.zig");
            var values = typed.TypedDocValuesWriter.init(alloc, .u64_val, 1024);
            defer values.deinit();
            for (0..25_000) |doc| {
                const rank = doc * 2 + parity;
                var key: [32]u8 = undefined;
                try segment.addStoredDoc(try std.fmt.bufPrint(&key, "doc-{d:0>5}", .{rank}), "");
                try inverted.addDocument(@intCast(doc), &.{.{ .term = "common", .freq = 3, .positions = &.{ 0, 2, 5 } }});
                try values.add(@intCast(doc), .{ .u64_val = rank });
            }
            const text = try inverted.build();
            defer alloc.free(text);
            const column = try values.build();
            defer alloc.free(column);
            try segment.addSection(try segment.addField("body"), .inverted_text, text);
            try segment.addSection(try segment.addField("rank"), .typed_doc_values, column);
            try segment.addIndexSortMetadata(&.{.{ .field = "rank", .desc = false }});
            return segment.build();
        }
        fn verify(alloc: Allocator, snapshot: *index_mod.IndexSnapshot) !void {
            try std.testing.expectEqual(@as(usize, 1), snapshot.segments.len);
            const segment = &snapshot.segments[0];
            try std.testing.expect(segment.data == .native);
            try std.testing.expectEqual(@as(u32, 50_000), segment.reader.doc_count);
            try std.testing.expectEqual(@as(usize, 50_000), try persistentSearchHitCount(alloc, snapshot, "common"));
            var reads = segment_mod.TypedReadScope.init(alloc);
            defer reads.deinit();
            const values = (try reads.get(&segment.reader, "rank")).?;
            for ([_]u32{ 0, 1, 127, 128, 2048, 49_999 }) |doc| {
                try std.testing.expectEqual(@as(u64, doc), (try values.getU64(doc)).?);
                var key: [32]u8 = undefined;
                try std.testing.expectEqualStrings(try std.fmt.bufPrint(&key, "doc-{d:0>5}", .{doc}), (try segment.reader.storedDoc(doc)).?.id);
            }
            var text = (try segment.reader.invertedIndexScoped(alloc, "body")).?;
            defer text.deinit();
            var result = (try text.lookup("common")).?;
            var postings = try result.iterator(alloc);
            defer postings.deinit();
            for ([_]u32{ 0, 128, 2048, 49_999 }) |doc| {
                const hit = (try postings.advanceToWithPositions(doc)).?;
                try std.testing.expectEqual(doc, hit.doc_id);
                try std.testing.expectEqualSlices(u32, &.{ 0, 2, 5 }, hit.positions);
            }
        }
    };
    for (0..2) |parity| {
        const bytes = try Harness.build(a, @intCast(parity));
        defer a.free(bytes);
        try index.indexSegment(bytes);
    }
    const pinned = index.acquireSnapshot();
    defer pinned.release();
    const ids = [_]u64{ pinned.segments[0].id, pinned.segments[1].id };
    const clock = @import("antfly_platform").time;
    const started = clock.monotonicNs();
    const prepared = try index.prepareMergedSegmentToFile(pinned, &.{ 0, 1 });
    const elapsed = clock.monotonicNs() - started;
    try std.testing.expect(try index.replaceSegmentsIfActiveManyPrepared(&ids, prepared));
    try Harness.verify(a, index.snapshot());
    try index.syncMain(true);
    index.close();
    open = false;
    index = try PersistentIndex.open(a, options);
    open = true;
    try Harness.verify(a, index.snapshot());
    try std.testing.expectEqual(@as(usize, 50_000), try persistentSearchHitCount(a, pinned, "common"));
    std.debug.print("LITE_NATIVE_SORTED_MERGE documents=50000 elapsed_ns={d} reopen=ok pinned_snapshot=ok\n", .{elapsed});
}

test "lite persistent mapped initial streamed typed flush aborts and reopens atomically" {
    if (builtin.os.tag != .linux and builtin.os.tag != .macos) return error.SkipZigTest;
    const a = std.testing.allocator;
    const introducer = @import("../introducer.zig");
    const analysis = @import("../search/analysis.zig");
    const lite_docs = @import("lite/docstore.zig");
    const lite_indexes = @import("lite/index_storage.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/initial-stream.aflite", .{tmp.sub_path});
    defer a.free(path);
    var docs = try lite_docs.Store.createWithOptions(a, path, .{ .no_sync = true, .io = std.testing.io });
    defer docs.close();
    var storage = lite_indexes.Store.init(a, &docs);
    const opts = PersistentIndexOptions{ .path = "/lite-text", .io = std.testing.io, .main_lsm_storage = storage.storage(), .wal_storage = storage.storage(), .main_no_sync = true, .wal_no_sync = true };
    var index = try PersistentIndex.open(a, opts);
    var open = true;
    defer if (open) index.close();
    const Builder = struct {
        allocator: Allocator,
        fail: bool = false,
        fn build(ptr: *anyopaque, output: *segment_mod.SegmentSink) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const FailureSink = struct {
                inner: *segment_mod.SegmentSink,
                fn owner(p: *anyopaque) *@This() {
                    return @ptrCast(@alignCast(p));
                }
                fn len(p: *anyopaque) usize {
                    return owner(p).inner.len();
                }
                fn append(p: *anyopaque, bytes: []const u8) !void {
                    if (owner(p).inner.len() > 100) return error.InjectedStreamWriteFailure;
                    try owner(p).inner.appendSlice(bytes);
                }
                fn byte(p: *anyopaque, value: u8) !void {
                    try append(p, &.{value});
                }
                fn repeat(p: *anyopaque, value: u8, count: usize) !void {
                    try owner(p).inner.appendNTimes(value, count);
                }
                fn patch(p: *anyopaque, offset: usize, bytes: []const u8) !void {
                    try owner(p).inner.writeAt(offset, bytes);
                }
                fn prefix(p: *anyopaque, count: usize) !u32 {
                    return owner(p).inner.crc32Prefix(count);
                }
                fn range(p: *anyopaque, offset: usize, count: usize) !u32 {
                    return owner(p).inner.crc32Range(offset, count);
                }
                const vtable = segment_mod.SegmentSink.VTable{ .len = len, .append_slice = append, .append_byte = byte, .append_ntimes = repeat, .write_at = patch, .crc32_prefix = prefix, .crc32_range = range };
            };
            var failing = FailureSink{ .inner = output };
            var failing_sink = segment_mod.SegmentSink{ .ptr = &failing, .vtable = &FailureSink.vtable };
            try introducer.writeSegmentFromTextWithAnalysisOptions(self.allocator, &.{
                .{ .id = "b", .stored_data = "body b", .text_fields = &.{}, .typed_fields = &.{
                    .{ .field_name = "rank", .value_type = .u64_val, .value = .{ .u64_val = 2 } },
                    .{ .field_name = "label", .value_type = .bytes_val, .value = .{ .bytes_val = "second" } },
                } },
                .{ .id = "a", .stored_data = "body a", .text_fields = &.{}, .typed_fields = &.{
                    .{ .field_name = "rank", .value_type = .u64_val, .value = .{ .u64_val = 1 } },
                } },
            }, &analysis.default_analyzer, .{}, .{ .index_sort = &.{.{ .field = "rank", .desc = false }} }, if (self.fail) &failing_sink else output);
        }
    };
    var builder = Builder{ .allocator = a, .fail = true };
    try std.testing.expectError(error.InjectedStreamWriteFailure, index.indexSegmentFromSinkBuilder(&builder, Builder.build));
    try std.testing.expectEqual(@as(u32, 0), index.snapshot().liveDocCount());
    builder.fail = false;
    _ = try index.indexSegmentFromSinkBuilder(&builder, Builder.build);
    try std.testing.expectEqual(@as(u32, 2), index.snapshot().liveDocCount());
    try std.testing.expect(index.snapshot().segments[0].data == .native);
    try index.syncMain(true);
    index.close();
    open = false;
    index = try PersistentIndex.open(a, opts);
    open = true;
    const reader = &index.snapshot().segments[0].reader;
    try std.testing.expectEqual(@as(u32, 2), reader.doc_count);
    var cursor = segment_mod.SegmentReader.StoredDocCursor.init(a);
    defer cursor.deinit();
    const first = (try cursor.get(reader, 0)).?;
    try std.testing.expectEqualStrings("a", first.id);
    try std.testing.expectEqualStrings("body a", first.data);
    var rank = (try reader.typedDocValuesScoped(a, "rank")).?;
    defer rank.deinit();
    try std.testing.expectEqual(@as(?u64, 1), try rank.getU64(0));
    try std.testing.expectEqual(@as(?u64, 2), try rank.getU64(1));
    var labels = (try reader.typedDocValuesScoped(a, "label")).?;
    defer labels.deinit();
    try std.testing.expect((try labels.getBytesAlloc(0)) == null);
    const label = (try labels.getBytesAlloc(1)).?;
    defer a.free(label);
    try std.testing.expectEqualStrings("second", label);
}

test "persistent source publication family receipt collection waits for its archived graph root" {
    const a = std.testing.allocator;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/family-receipts", .{temporary.sub_path}, 0);
    defer a.free(path);
    var primary_backend = @import("lsm_backend.zig").Backend.init(a, .{});
    defer primary_backend.close();
    var primary = try @import("docstore.zig").DocStore.openRuntime(a, try primary_backend.runtimeStore(a, .{}));
    defer primary.close();
    var index = try PersistentIndex.open(a, .{ .path = path });
    defer index.close();
    const owner: source_publication.Digest = @splat(244);
    const slot: source_publication.Digest = @splat(245);
    try index.reserveSourceOwner(owner);
    _ = try index.advanceSourcePublicationEpoch();
    var birth = try index.beginNewSourcePublication(std.testing.io);
    defer birth.publisher.deinit();
    var readers = try index.openSourceReaders(birth.binding);
    defer readers.deinit();
    var held = try readers.lease();
    defer held.deinit();
    {
        var txn = try index.beginWriteMainTxn();
        errdefer txn.abort();
        var metadata: PersistentIndex.PublicationTxn = .{ .txn = &txn };
        try family_graph.initialize(&metadata, owner);
        const leaf = try family_graph.stage(&metadata, owner, .{ .kind = .leaf, .payload = "metadata-only test leaf" }, .{});
        const root = try family_graph.stage(&metadata, owner, .{ .kind = .family, .children = &.{leaf} }, .{});
        const epoch = source_epoch.tokenEpoch(birth.binding).?;
        try std.testing.expect(try source_publication.stagePublishRoleInEpoch(a, &metadata, birth.binding, root, &.{999}, epoch, .family));
        try std.testing.expectError(error.IndexPublicationConflict, source_publication.stagePublishInEpoch(a, &metadata, birth.binding, root, &.{999}, epoch));
        try std.testing.expect(try family_graph.compareAndSwap(&metadata, owner, slot, null, birth.binding.publication, root, .{}));
        const current = (try family_graph.loadRoot(&metadata, owner, slot)).?;
        try std.testing.expect(try family_graph.removeRoot(&metadata, owner, slot, current));
        try source_publication.stageRetire(a, &metadata, birth.binding, &.{});
        try txn.commit();
    }
    try index.main_store_owner.sync(true);
    birth.publisher.deinit();
    readers.retire();
    _ = try index.advanceSourcePublicationEpoch();
    try std.testing.expectEqual(@as(u32, 0), (try index.reclaimSourceReceiptWindow(&primary, null, 64)).purged);
    held.deinit();
    // Native readers drained, but the archived graph edge still needs its
    // durable retirement decision; receipt collection must keep it available.
    try std.testing.expect(readers.drained() != null);
    try std.testing.expectEqual(@as(u32, 0), (try index.reclaimSourceReceiptWindow(&primary, null, 64)).purged);
    {
        var txn = try index.beginWriteMainTxn();
        errdefer txn.abort();
        var metadata: PersistentIndex.PublicationTxn = .{ .txn = &txn };
        const decision = (try source_publication.decision(&metadata, birth.binding)).?;
        try decision.requireRole(.family);
        try std.testing.expect(try family_graph.releaseRetired(&metadata, owner, decision, readers.drained().?));
        try txn.commit();
    }
    try index.main_store_owner.sync(true);
    try std.testing.expectEqual(@as(u32, 1), (try index.reclaimSourceReceiptWindow(&primary, null, 64)).purged);
    try (try index.loadDurableSourcePublication(birth.binding)).?.require(birth.binding, .cancelled);
}

test {
    _ = @import("index_source_recovery_test.zig");
    _ = @import("immutable_graph_test.zig");
    _ = @import("index_field_inputs.zig");
}

test "persistent source publication copies range artifacts with bounded transfer scratch" {
    const a = std.testing.allocator;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/range-import", .{temporary.sub_path}, 0);
    defer a.free(path);
    var index = try PersistentIndex.open(a, .{ .path = path });
    defer index.close();
    const bytes = try buildMultiDocSegment(a, &.{ .{ .doc_id = "a", .term = "hello" }, .{ .doc_id = "z", .term = "world" } });
    defer a.free(bytes);
    const Range = struct {
        bytes: []const u8,
        largest: usize = 0,
        fn read(raw: *anyopaque, offset: u64, out: []u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.largest = @max(self.largest, out.len);
            if (offset > self.bytes.len or out.len > self.bytes.len - offset) return error.InvalidData;
            @memcpy(out, self.bytes[@intCast(offset)..][0..out.len]);
        }
        fn close(_: *anyopaque) void {}
    };
    var range: Range = .{ .bytes = bytes };
    const source: @import("../segment_source.zig").Source = .{ .ranges = .{ .ptr = &range, .length = bytes.len, .read_into = Range.read, .close = Range.close } };
    var artifact = try index.prepareSourceSegmentCopy(a, source);
    defer {
        const id = artifact.id;
        artifact.deinit(a);
        index.deleteSegmentFile(id);
    }
    try std.testing.expect(range.largest <= 64 * 1024);
    try std.testing.expectEqualStrings("a", artifact.key_range.min_doc_key);
    try std.testing.expectEqualStrings("z", artifact.key_range.max_doc_key);
    try std.testing.expectEqualDeep((try @import("../segment_source.zig").ContentStamp.capture(.{ .contiguous = bytes })).digest, (try @import("../segment_source.zig").ContentStamp.capture(artifact.data.source())).digest);
}

test "persistent source publication composite extents reuse resources and preserve read authority" {
    const composite = @import("../segment_composite.zig");
    const a = std.testing.allocator;
    const Provider = struct {
        references: usize = 0,
        resolutions: usize = 0,
        allowed: bool = true,
        fn retain(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.references += 1;
        }
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.references -= 1;
        }
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!self.allowed) return error.PermissionDenied;
        }
        fn resolve(raw: *anyopaque, _: Allocator, identity: [32]u8) !composite.Source {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try check(raw);
            if (!std.mem.eql(u8, &identity, &@as([32]u8, @splat(1)))) return error.NotFound;
            self.resolutions += 1;
            return .{ .contiguous = "abcdef" };
        }
        fn provider(self: *@This()) composite.Provider {
            return .{ .ptr = self, .resolve = resolve, .retain = retain, .release = release, .check = check };
        }
    };
    var owner: Provider = .{};
    const descriptor = try composite.encode(a, &.{
        .{ .inline_bytes = "prefix/" },
        .{ .resource = .{ .identity = @splat(1), .source_bytes = 6, .offset = 1, .length = 3 } },
        .{ .resource = .{ .identity = @splat(1), .source_bytes = 6, .offset = 4, .length = 2 } },
        .{ .inline_bytes = "/end" },
    }, .{});
    var source = composite.open(a, descriptor, owner.provider(), .{}) catch |err| {
        a.free(descriptor);
        return err;
    };
    defer source.close();
    a.free(descriptor);
    try std.testing.expectEqual(@as(usize, 1), owner.references);
    try std.testing.expectEqual(@as(usize, 1), owner.resolutions);
    var whole: [16]u8 = undefined;
    try source.readInto(0, &whole);
    try std.testing.expectEqualStrings("prefix/bcdef/end", &whole);
    var crossing: [8]u8 = undefined;
    try source.readInto(5, &crossing);
    try std.testing.expectEqualStrings("x/bcdef/", &crossing);
    try std.testing.expectError(error.EndOfStream, source.readInto(15, &crossing));
    owner.allowed = false;
    try std.testing.expectError(error.PermissionDenied, source.readInto(0, &whole));
    owner.allowed = true;
    const extents = try composite.slice(a, source, 5, 8);
    defer a.free(extents);
    try std.testing.expectEqual(@as(usize, 1), owner.resolutions);
    const next_descriptor = try composite.encode(a, extents, .{});
    defer a.free(next_descriptor);
    var next = try composite.open(a, next_descriptor, owner.provider(), .{});
    defer next.close();
    source.close();
    try std.testing.expectEqual(@as(usize, 1), owner.references);
    // The successor holds direct resources and its own copied metadata. It
    // continues after the parent descriptor/read owner has been released.
    try next.readInto(0, &crossing);
    try std.testing.expectEqualStrings("x/bcdef/", &crossing);
    next.close();
    try std.testing.expectEqual(@as(usize, 0), owner.references);
}

test "persistent source publication composite opens unwind every allocation failure" {
    const composite = @import("../segment_composite.zig");
    const Fixture = struct {
        references: usize = 0,
        fn retain(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.references += 1;
        }
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.references -= 1;
        }
        fn resolve(_: *anyopaque, _: Allocator, _: [32]u8) !composite.Source {
            return .{ .contiguous = "value" };
        }
        fn run(a: Allocator) !void {
            var owner: @This() = .{};
            defer std.debug.assert(owner.references == 0);
            const raw = try composite.encode(a, &.{.{ .resource = .{ .identity = @splat(2), .source_bytes = 5, .offset = 0, .length = 5 } }}, .{});
            defer a.free(raw);
            const provider: composite.Provider = .{ .ptr = &owner, .resolve = resolve, .retain = retain, .release = release };
            var source = try composite.open(a, raw, provider, .{});
            defer source.close();
            var value: [5]u8 = undefined;
            try source.readInto(0, &value);
            try std.testing.expectEqualStrings("value", &value);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "persistent source publication field packs authenticate sparse pages without warming the body" {
    const pack = @import("../field_pack.zig");
    const a = std.testing.allocator;
    const data = try a.alloc(u8, pack.page_bytes * 2 + 17);
    defer a.free(data);
    for (data, 0..) |*byte, n| byte.* = @intCast(n % 251);
    var memory = segment_mod.MemorySegmentSink.init(a);
    defer memory.deinit();
    var sink = memory.sink();
    var encoder = try pack.Encoder.init(a, &sink, .{});
    defer encoder.deinit();
    try encoder.append(data[0..31]);
    try encoder.append(data[31..70000]);
    try encoder.append(data[70000..]);
    const metadata = try encoder.finish();
    try std.testing.expectEqual(@as(u64, 3), metadata.pages);
    const Physical = struct {
        bytes: []u8,
        payload_bytes: usize,
        reads: usize = 0,
        closes: usize = 0,
        fn read(raw: *anyopaque, offset: u64, output: []u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (offset > self.bytes.len or output.len > self.bytes.len - offset) return error.EndOfStream;
            if (offset >= 8 and offset < 8 + self.payload_bytes) self.reads += output.len;
            @memcpy(output, self.bytes[@intCast(offset)..][0..output.len]);
        }
        fn close(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.closes += 1;
        }
        fn source(self: *@This()) @import("../segment_source.zig").Source {
            return .{ .ranges = .{ .ptr = self, .length = self.bytes.len, .read_into = read, .close = close } };
        }
    };
    var physical: Physical = .{ .bytes = memory.out.items, .payload_bytes = data.len };
    var source = try pack.open(a, physical.source(), metadata.identity, .{});
    defer source.close();
    try std.testing.expectEqual(@as(usize, 0), physical.reads);
    var crossing: [7]u8 = undefined;
    try source.readInto(pack.page_bytes - 3, &crossing);
    try std.testing.expectEqualSlices(u8, data[pack.page_bytes - 3 ..][0..7], &crossing);
    try std.testing.expectEqual(@as(usize, pack.page_bytes * 2), physical.reads);
    const before = physical.reads;
    try source.readInto(pack.page_bytes - 3, &crossing);
    try std.testing.expectEqual(@as(usize, 7), physical.reads - before);
    // The untouched last page remains cold and is rejected when corrupted.
    physical.bytes[8 + pack.page_bytes * 2] ^= 1;
    try std.testing.expectError(error.FieldPackIntegrityMismatch, source.readInto(pack.page_bytes * 2, &crossing));
    const after_corruption = physical.reads;
    try std.testing.expectError(error.FieldPackIntegrityMismatch, source.readInto(pack.page_bytes * 2, &crossing));
    try std.testing.expectEqual(after_corruption, physical.reads);
    source.close();
    try std.testing.expectEqual(@as(usize, 1), physical.closes);
}

test "persistent source publication field pack allocation failures preserve physical ownership" {
    const pack = @import("../field_pack.zig");
    const Fixture = struct {
        fn run(a: Allocator) !void {
            var memory = segment_mod.MemorySegmentSink.init(a);
            defer memory.deinit();
            var sink = memory.sink();
            var encoder = try pack.Encoder.init(a, &sink, .{});
            defer encoder.deinit();
            try encoder.append("immutable field bytes");
            const metadata = try encoder.finish();
            try std.testing.expectError(error.FieldPackClosed, encoder.append("late"));
            var source = try pack.open(a, .{ .contiguous = memory.out.items }, metadata.identity, .{});
            defer source.close();
            var bytes: [21]u8 = undefined;
            try source.readInto(0, &bytes);
            try std.testing.expectEqualStrings("immutable field bytes", &bytes);
            var wrong = metadata.identity;
            wrong[0] ^= 1;
            // This separate negative assertion must not turn an injected OOM
            // into TestUnexpectedError in the successful ownership exercise.
            try std.testing.expectError(error.FieldPackIntegrityMismatch, pack.open(std.testing.allocator, .{ .contiguous = memory.out.items }, wrong, .{}));
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}
