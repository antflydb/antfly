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

//! RowSource batch assembly for the first supported Parquet scan path.

const std = @import("std");
const Allocator = std.mem.Allocator;
const external_binding = @import("../external_source/catalog_binding.zig");
const external_source = @import("../external_source/types.zig");
const algebraic_segment = @import("../algebraic_segment/mod.zig");
const rowsource_bridge = @import("../external_source/rowsource_bridge.zig");
const parquet_footer = @import("lake_parquet_footer.zig");
const parquet_metadata = @import("lake_parquet_metadata.zig");
const parquet_page = @import("lake_parquet_page.zig");
const lake_rows = @import("lake_rows.zig");
const lake_scan_plan = @import("lake_scan_plan.zig");
const lake_sidecar_selection = @import("lake_sidecar_selection.zig");
const range_io = @import("lake_range_io.zig");
const sidecar_manifest = @import("../segment/sidecar_manifest.zig");
const rowsource = @import("../../storage/rowsource/types.zig");
const rowsource_external = @import("../../storage/rowsource/external.zig");
const resource_manager_mod = @import("../../storage/resource_manager.zig");
const fs_paths = @import("../../common/fs_paths.zig");
const snappy = @import("../../encoding/snappy.zig");
pub const ObjectRangeCacheDigest = [std.crypto.hash.sha2.Sha256.digest_length]u8;

pub const MaterializationLimits = struct {
    max_rows: usize = 1_000_000,
    max_projected_cells: usize = 8_000_000,
    max_struct_allocation_bytes: usize = 256 * 1024 * 1024,
    max_input_bytes: usize = 256 * 1024 * 1024,
    max_decoded_bytes: usize = 256 * 1024 * 1024,
    max_physical_reads: usize = 4096,

    pub fn validate(self: MaterializationLimits) !void {
        if (self.max_rows == 0 or
            self.max_projected_cells == 0 or
            self.max_struct_allocation_bytes == 0 or
            self.max_input_bytes == 0 or
            self.max_decoded_bytes == 0 or
            self.max_physical_reads == 0)
        {
            return error.InvalidParquetMaterializationLimits;
        }
    }
};

pub const ColumnChunkInput = struct {
    column_id: []const u8,
    /// Raw column chunk bytes for one supported i64 column path.
    bytes: []const u8,

    pub fn validate(self: ColumnChunkInput) !void {
        if (self.column_id.len == 0) return error.InvalidParquetRowGroupBatch;
        if (self.bytes.len == 0) return error.InvalidParquetRowGroupBatch;
    }
};

pub const RowGroupInput = struct {
    file_id: []const u8,
    row_group_ordinal: u32,
    chunks: []const ColumnChunkInput,

    pub fn validate(self: RowGroupInput) !void {
        if (self.file_id.len == 0) return error.InvalidParquetRowGroupBatch;
        if (self.chunks.len == 0) return error.InvalidParquetRowGroupBatch;
        for (self.chunks) |chunk| try chunk.validate();
    }
};

pub const ObjectRangeReader = struct {
    ctx: *anyopaque,
    read_range_alloc: *const fn (
        ctx: *anyopaque,
        alloc: Allocator,
        bucket: []const u8,
        key: []const u8,
        offset: u64,
        len: usize,
    ) anyerror![]u8,
    read_planned_range_alloc: ?*const fn (
        ctx: *anyopaque,
        alloc: Allocator,
        read: range_io.RangeRead,
    ) anyerror![]u8 = null,

    pub fn readAlloc(
        self: ObjectRangeReader,
        alloc: Allocator,
        bucket: []const u8,
        key: []const u8,
        offset: u64,
        len: usize,
    ) ![]u8 {
        if (bucket.len == 0 or key.len == 0 or len == 0) return error.InvalidLakeRangeRead;
        if (len > range_io.max_physical_range_read_bytes) return error.LakeRangeReadTooLarge;
        const bytes = try self.read_range_alloc(self.ctx, alloc, bucket, key, offset, len);
        errdefer alloc.free(bytes);
        if (bytes.len != len) return error.InvalidLakeRangeRead;
        return bytes;
    }

    pub fn readPlannedAlloc(
        self: ObjectRangeReader,
        alloc: Allocator,
        read: range_io.RangeRead,
    ) ![]u8 {
        try read.validate();
        const len: usize = std.math.cast(usize, read.range.len) orelse return error.InvalidLakeRangeRead;
        const bytes = if (self.read_planned_range_alloc) |read_planned|
            try read_planned(self.ctx, alloc, read)
        else
            try self.readAlloc(alloc, read.object.bucket, read.object.key, read.range.offset, len);
        errdefer alloc.free(bytes);
        if (bytes.len != len) return error.InvalidLakeRangeRead;
        return bytes;
    }
};

const object_range_cache_lane_count = std.meta.fields(range_io.CacheLane).len;

pub const ObjectRangeCacheLaneStats = struct {
    hits: usize = 0,
    misses: usize = 0,
    stored_bytes: usize = 0,
    evicted_bytes: usize = 0,
    rejected_bytes: usize = 0,
};

pub const ObjectRangeCacheStats = struct {
    hits: usize = 0,
    misses: usize = 0,
    stored_bytes: usize = 0,
    evicted_bytes: usize = 0,
    rejected_bytes: usize = 0,
    lanes: [object_range_cache_lane_count]ObjectRangeCacheLaneStats = [_]ObjectRangeCacheLaneStats{.{}} ** object_range_cache_lane_count,

    pub fn lane(self: ObjectRangeCacheStats, cache_lane: range_io.CacheLane) ObjectRangeCacheLaneStats {
        return self.lanes[@intFromEnum(cache_lane)];
    }
};

pub const ObjectRangeCachePolicy = struct {
    max_total_bytes: ?usize = null,
    max_bytes_by_lane: [object_range_cache_lane_count]?usize = [_]?usize{null} ** object_range_cache_lane_count,
    protected_lanes: [object_range_cache_lane_count]bool = [_]bool{false} ** object_range_cache_lane_count,
    max_fetch_bytes: usize = range_io.max_physical_range_read_bytes,

    pub fn lakeServingDefaults(max_total_bytes: usize) ObjectRangeCachePolicy {
        var policy = ObjectRangeCachePolicy{};
        policy.setTotalLimit(max_total_bytes);
        policy.protectLane(.metadata);
        policy.protectLane(.serving_sidecar);
        policy.setLaneLimit(.broad_scan_scratch, max_total_bytes / 8);
        return policy;
    }

    pub fn withLaneLimit(cache_lane: range_io.CacheLane, max_bytes: usize) ObjectRangeCachePolicy {
        var policy = ObjectRangeCachePolicy{};
        policy.max_bytes_by_lane[@intFromEnum(cache_lane)] = max_bytes;
        return policy;
    }

    pub fn setLaneLimit(self: *ObjectRangeCachePolicy, cache_lane: range_io.CacheLane, max_bytes: usize) void {
        self.max_bytes_by_lane[@intFromEnum(cache_lane)] = max_bytes;
    }

    pub fn setTotalLimit(self: *ObjectRangeCachePolicy, max_bytes: usize) void {
        self.max_total_bytes = max_bytes;
    }

    pub fn setFetchLimit(self: *ObjectRangeCachePolicy, max_bytes: usize) void {
        self.max_fetch_bytes = @min(max_bytes, range_io.max_physical_range_read_bytes);
    }

    pub fn protectLane(self: *ObjectRangeCachePolicy, cache_lane: range_io.CacheLane) void {
        self.protected_lanes[@intFromEnum(cache_lane)] = true;
    }

    pub fn laneLimit(self: ObjectRangeCachePolicy, cache_lane: range_io.CacheLane) ?usize {
        return self.max_bytes_by_lane[@intFromEnum(cache_lane)];
    }

    pub fn isProtected(self: ObjectRangeCachePolicy, cache_lane: range_io.CacheLane) bool {
        return self.protected_lanes[@intFromEnum(cache_lane)];
    }
};

pub const ObjectRangeCacheEntry = struct {
    bytes: []u8,
    checksum: ObjectRangeCacheDigest,
};

pub const PersistentObjectRangeCacheDurability = enum {
    /// Cache entries are checksum-protected and disposable. Atomic rename is
    /// sufficient by default, avoiding a storage durability barrier on the
    /// query miss path.
    cache_only,
    /// Opt-in for deployments that prefer surviving a host crash over miss
    /// latency, even though every entry can be fetched again from its source.
    durable,
};

pub const PersistentObjectRangeCachePolicy = struct {
    /// Includes the provenance header as well as payload bytes.
    max_total_bytes: usize = 10 * 1024 * 1024 * 1024,
    max_entries: usize = 16_384,
    /// Pending and in-flight write memory is independently bounded so a slow
    /// local disk cannot turn object-store misses into an unbounded RAM queue.
    max_write_queue_bytes: usize = 512 * 1024 * 1024,
    max_write_queue_entries: usize = 64,
    max_cache_key_bytes: usize = 64 * 1024,
    durability: PersistentObjectRangeCacheDurability = .cache_only,

    pub fn validate(self: PersistentObjectRangeCachePolicy) !void {
        if (self.max_total_bytes == 0 or self.max_entries == 0 or
            self.max_write_queue_bytes == 0 or self.max_write_queue_entries == 0 or self.max_cache_key_bytes == 0 or
            self.max_cache_key_bytes > std.math.maxInt(u32))
        {
            return error.InvalidPersistentObjectRangeCachePolicy;
        }
    }
};

pub const PersistentObjectRangeCacheStats = struct {
    read_hits: usize = 0,
    read_misses: usize = 0,
    read_errors: usize = 0,
    stored_bytes: usize = 0,
    entries: usize = 0,
    evicted_bytes: usize = 0,
    evicted_entries: usize = 0,
    corrupt_entries_removed: usize = 0,
    temporary_files_removed: usize = 0,
    writes_completed: usize = 0,
    write_errors: usize = 0,
    writes_coalesced: usize = 0,
    writes_dropped: usize = 0,
    dropped_bytes: usize = 0,
    queued_bytes: usize = 0,
    queued_entries: usize = 0,
};

pub const PersistentObjectRangeCacheEnqueueResult = enum {
    enqueued,
    coalesced,
    dropped,
};

pub const PersistentObjectRangeCacheResources = struct {
    /// Optional node-wide memory and storage envelope. The manager must
    /// outlive the cache. Queue memory is charged to
    /// `lake_range_cache_queue`; if the manager has a CapacitySource, each
    /// physical cache write also reserves future disk growth before I/O.
    resource_manager: ?*resource_manager_mod.ResourceManager = null,
};

pub const PersistentObjectRangeCache = struct {
    state: *PersistentObjectRangeCacheState,

    /// The returned cache is the sole process owner for `root_dir`. Quiesce
    /// callers, then call `deinit` to drain accepted writes and release the
    /// worker.
    pub fn init(io: std.Io, root_dir: []const u8) !PersistentObjectRangeCache {
        return try initWithPolicy(io, root_dir, .{});
    }

    pub fn initWithDurability(
        io: std.Io,
        root_dir: []const u8,
        durability: PersistentObjectRangeCacheDurability,
    ) !PersistentObjectRangeCache {
        var policy = PersistentObjectRangeCachePolicy{};
        policy.durability = durability;
        return try initWithPolicy(io, root_dir, policy);
    }

    pub fn initWithPolicy(
        io: std.Io,
        root_dir: []const u8,
        policy: PersistentObjectRangeCachePolicy,
    ) !PersistentObjectRangeCache {
        return try initWithPolicyAndResources(io, root_dir, policy, .{});
    }

    pub fn initWithPolicyAndResources(
        io: std.Io,
        root_dir: []const u8,
        policy: PersistentObjectRangeCachePolicy,
        resources: PersistentObjectRangeCacheResources,
    ) !PersistentObjectRangeCache {
        try policy.validate();
        if (root_dir.len == 0) return error.InvalidPersistentObjectRangeCachePolicy;

        const internal_alloc = std.heap.smp_allocator;
        const state = try internal_alloc.create(PersistentObjectRangeCacheState);
        errdefer internal_alloc.destroy(state);
        const owned_root = try internal_alloc.dupe(u8, root_dir);
        errdefer internal_alloc.free(owned_root);
        state.* = .{
            .alloc = internal_alloc,
            .root_dir = owned_root,
            .policy = policy,
            .io = io,
            .resource_manager = resources.resource_manager,
        };
        try state.initializeInventory();
        errdefer state.deinitInventory();
        state.worker = try io.concurrent(persistentObjectRangeWorkerMain, .{state});
        return .{ .state = state };
    }

    pub fn deinit(self: *PersistentObjectRangeCache) void {
        const state = self.state;
        const io = state.io;
        state.mutex.lockUncancelable(io);
        state.closing = true;
        state.condition.broadcast(io);
        state.mutex.unlock(io);
        if (state.worker) |*worker| _ = worker.await(io);
        state.deinitInventory();
        state.pending.deinit(state.alloc);
        state.queue.deinit(state.alloc);
        state.alloc.free(state.root_dir);
        const internal_alloc = state.alloc;
        internal_alloc.destroy(state);
        self.* = undefined;
    }

    pub fn configuredPolicy(self: *const PersistentObjectRangeCache) PersistentObjectRangeCachePolicy {
        return self.state.policy;
    }

    pub fn statsSnapshot(self: *const PersistentObjectRangeCache) PersistentObjectRangeCacheStats {
        const state = self.state;
        const io = state.io;
        state.mutex.lockUncancelable(io);
        defer state.mutex.unlock(io);
        var stats = state.stats;
        stats.queued_bytes = state.pending_bytes;
        stats.queued_entries = state.pending_count;
        return stats;
    }

    pub fn flush(self: *PersistentObjectRangeCache) void {
        const state = self.state;
        const io = state.io;
        state.mutex.lockUncancelable(io);
        defer state.mutex.unlock(io);
        while (state.pending_count != 0) state.condition.waitUncancelable(io, &state.mutex);
    }

    pub fn readAlloc(
        self: *PersistentObjectRangeCache,
        alloc: Allocator,
        cache_key: []const u8,
        expected_len: usize,
    ) !?[]u8 {
        if (expected_len > range_io.max_physical_range_read_bytes) return error.InvalidLakeRangeRead;
        const state = self.state;
        if (cache_key.len > state.policy.max_cache_key_bytes) {
            state.recordReadMiss();
            return null;
        }
        const path = try self.cachePathAlloc(alloc, cache_key);
        defer alloc.free(path);
        const filename = std.fs.path.basename(path);
        const entry = state.pinEntry(filename) orelse {
            state.recordReadMiss();
            return null;
        };
        var valid = true;
        defer state.releaseEntry(entry, valid);
        const bytes = persistentObjectRangeReadEntryWithIoAlloc(
            state.io,
            alloc,
            path,
            cache_key,
            expected_len,
        ) catch |err| switch (err) {
            error.OutOfMemory => return err,
            error.FileNotFound, error.InvalidPersistentObjectRangeCache => {
                valid = false;
                state.recordReadMiss();
                return null;
            },
            else => {
                state.recordReadError();
                return null;
            },
        };
        state.recordReadHit();
        return bytes;
    }

    pub fn enqueueWrite(
        self: *PersistentObjectRangeCache,
        cache_key: []const u8,
        bytes: []const u8,
    ) PersistentObjectRangeCacheEnqueueResult {
        return self.state.enqueueWrite(cache_key, bytes);
    }

    fn cachePathAlloc(self: *const PersistentObjectRangeCache, alloc: Allocator, cache_key: []const u8) ![]u8 {
        const filename = try objectRangeCacheKeyDigestHexAlloc(alloc, cache_key);
        defer alloc.free(filename);
        return try std.fs.path.join(alloc, &.{ self.state.root_dir, filename });
    }
};

const PersistentObjectRangeCacheEntry = struct {
    filename: []u8,
    disk_bytes: usize,
    modified_ns: i128,
    pin_count: usize = 0,
    removing: bool = false,
    lru_node: std.DoublyLinkedList.Node = .{},
};

const PersistentObjectRangeCacheWrite = struct {
    cache_key: []u8,
    bytes: []u8,
    memory_bytes: usize,
    disk_bytes: usize,
    memory_reservation: ?resource_manager_mod.Reservation,
};

const PersistentObjectRangeCacheWriteOutcome = enum {
    written,
    already_present,
    capacity_unavailable,
};

fn persistentObjectRangeEntryAgeOrder(
    _: void,
    lhs: *PersistentObjectRangeCacheEntry,
    rhs: *PersistentObjectRangeCacheEntry,
) std.math.Order {
    const modified_order = std.math.order(lhs.modified_ns, rhs.modified_ns);
    if (modified_order != .eq) return modified_order;
    if (std.mem.eql(u8, lhs.filename, rhs.filename)) return .eq;
    return if (std.mem.lessThan(u8, lhs.filename, rhs.filename)) .lt else .gt;
}

const PersistentObjectRangeCacheState = struct {
    alloc: Allocator,
    root_dir: []u8,
    policy: PersistentObjectRangeCachePolicy,
    io: std.Io,
    resource_manager: ?*resource_manager_mod.ResourceManager = null,
    mutex: std.Io.Mutex = .init,
    condition: std.Io.Condition = .init,
    worker: ?std.Io.Future(void) = null,
    closing: bool = false,
    queue: std.ArrayListUnmanaged(PersistentObjectRangeCacheWrite) = .empty,
    queue_head: usize = 0,
    pending: std.StringHashMapUnmanaged(void) = .empty,
    pending_bytes: usize = 0,
    pending_count: usize = 0,
    entries: std.StringHashMapUnmanaged(*PersistentObjectRangeCacheEntry) = .empty,
    lru: std.DoublyLinkedList = .{},
    stats: PersistentObjectRangeCacheStats = .{},

    fn initializeInventory(self: *PersistentObjectRangeCacheState) !void {
        const io = self.io;
        try fs_paths.createDirPathPortable(io, self.root_dir);
        var dir = if (std.fs.path.isAbsolute(self.root_dir))
            try std.Io.Dir.openDirAbsolute(io, self.root_dir, .{ .iterate = true })
        else
            try std.Io.Dir.cwd().openDir(io, self.root_dir, .{ .iterate = true });
        defer dir.close(io);

        var discovered = std.PriorityQueue(
            *PersistentObjectRangeCacheEntry,
            void,
            persistentObjectRangeEntryAgeOrder,
        ).initContext({});
        defer discovered.deinit(self.alloc);
        var registered: usize = 0;
        errdefer {
            for (discovered.items[registered..]) |entry| self.freeEntry(entry);
            self.deinitInventory();
        }

        var iter = dir.iterateAssumeFirstIteration();
        while (try iter.next(io)) |entry| {
            if (entry.kind != .file) continue;
            if (isPersistentObjectRangeTemporaryFilename(entry.name)) {
                dir.deleteFile(io, entry.name) catch |err| switch (err) {
                    error.FileNotFound => {},
                    else => return err,
                };
                self.stats.temporary_files_removed += 1;
                continue;
            }
            if (!isPersistentObjectRangeCacheFilename(entry.name)) continue;
            const stat = dir.statFile(io, entry.name, .{}) catch |err| switch (err) {
                // Directory iteration and stat are not atomic. A concurrent
                // cleanup may legitimately win this race.
                error.FileNotFound => continue,
                // Do not enable the cache with a managed file that could not
                // be inventoried: it would sit outside the configured disk
                // ceiling and make capacity accounting untrustworthy.
                else => return err,
            };
            const disk_bytes = std.math.cast(usize, stat.size) orelse {
                dir.deleteFile(io, entry.name) catch |err| switch (err) {
                    error.FileNotFound => {},
                    else => return err,
                };
                self.stats.corrupt_entries_removed += 1;
                continue;
            };
            if (disk_bytes == 0 or disk_bytes > self.policy.max_total_bytes) {
                dir.deleteFile(io, entry.name) catch |err| switch (err) {
                    error.FileNotFound => {},
                    else => return err,
                };
                if (disk_bytes == 0) {
                    self.stats.corrupt_entries_removed += 1;
                } else {
                    self.stats.evicted_bytes +|= disk_bytes;
                    self.stats.evicted_entries += 1;
                }
                continue;
            }
            const owned = try self.alloc.create(PersistentObjectRangeCacheEntry);
            const filename = self.alloc.dupe(u8, entry.name) catch |err| {
                self.alloc.destroy(owned);
                return err;
            };
            owned.* = .{
                .filename = filename,
                .disk_bytes = disk_bytes,
                .modified_ns = stat.mtime.toNanoseconds(),
            };
            discovered.push(self.alloc, owned) catch |err| {
                self.freeEntry(owned);
                return err;
            };
            if (discovered.count() > self.policy.max_entries) {
                const victim = discovered.pop().?;
                dir.deleteFile(io, victim.filename) catch |err| switch (err) {
                    error.FileNotFound => {},
                    else => {
                        self.freeEntry(victim);
                        return err;
                    },
                };
                self.stats.evicted_bytes +|= victim.disk_bytes;
                self.stats.evicted_entries += 1;
                self.freeEntry(victim);
            }
        }

        std.sort.pdq(*PersistentObjectRangeCacheEntry, discovered.items, {}, struct {
            fn lessThan(_: void, lhs: *PersistentObjectRangeCacheEntry, rhs: *PersistentObjectRangeCacheEntry) bool {
                if (lhs.modified_ns != rhs.modified_ns) return lhs.modified_ns < rhs.modified_ns;
                return std.mem.lessThan(u8, lhs.filename, rhs.filename);
            }
        }.lessThan);
        try self.entries.ensureTotalCapacity(self.alloc, @intCast(discovered.items.len));
        for (discovered.items) |entry| {
            try self.entries.put(self.alloc, entry.filename, entry);
            self.lru.append(&entry.lru_node);
            self.stats.stored_bytes +|= entry.disk_bytes;
            self.stats.entries +|= 1;
            registered += 1;
        }
        while (self.stats.stored_bytes > self.policy.max_total_bytes or self.stats.entries > self.policy.max_entries) {
            const victim_node = self.lru.first orelse break;
            const victim: *PersistentObjectRangeCacheEntry = @alignCast(@fieldParentPtr("lru_node", victim_node));
            dir.deleteFile(io, victim.filename) catch |err| switch (err) {
                error.FileNotFound => {},
                else => return err,
            };
            _ = self.entries.remove(victim.filename);
            self.lru.remove(victim_node);
            decrementSaturating(&self.stats.stored_bytes, victim.disk_bytes);
            decrementSaturating(&self.stats.entries, 1);
            self.stats.evicted_bytes +|= victim.disk_bytes;
            self.stats.evicted_entries += 1;
            self.freeEntry(victim);
        }
    }

    fn deinitInventory(self: *PersistentObjectRangeCacheState) void {
        var iter = self.entries.valueIterator();
        while (iter.next()) |entry| self.freeEntry(entry.*);
        self.entries.deinit(self.alloc);
        self.entries = .empty;
        self.lru = .{};
    }

    fn freeEntry(self: *PersistentObjectRangeCacheState, entry: *PersistentObjectRangeCacheEntry) void {
        self.alloc.free(entry.filename);
        self.alloc.destroy(entry);
    }

    fn enqueueWrite(
        self: *PersistentObjectRangeCacheState,
        cache_key: []const u8,
        bytes: []const u8,
    ) PersistentObjectRangeCacheEnqueueResult {
        const disk_bytes = persistentObjectRangeEncodedLen(cache_key.len, bytes.len) orelse
            return self.recordDropped(bytes.len);
        if (cache_key.len > self.policy.max_cache_key_bytes or disk_bytes > self.policy.max_total_bytes) {
            return self.recordDropped(bytes.len);
        }
        const memory_bytes = std.math.add(usize, cache_key.len, bytes.len) catch
            return self.recordDropped(bytes.len);
        var memory_reservation: ?resource_manager_mod.Reservation = if (self.resource_manager) |manager|
            manager.reserve(.lake_range_cache_queue, @intCast(memory_bytes)) catch
                return self.recordDropped(bytes.len)
        else
            null;

        const owned_key = self.alloc.dupe(u8, cache_key) catch {
            releaseMemoryReservation(&memory_reservation);
            return self.recordDropped(bytes.len);
        };

        const io = self.io;
        self.mutex.lockUncancelable(io);
        if (self.closing) {
            _ = self.recordDroppedLocked(bytes.len);
            self.mutex.unlock(io);
            self.alloc.free(owned_key);
            releaseMemoryReservation(&memory_reservation);
            return .dropped;
        }
        if (self.pending.contains(cache_key)) {
            self.stats.writes_coalesced += 1;
            self.mutex.unlock(io);
            self.alloc.free(owned_key);
            releaseMemoryReservation(&memory_reservation);
            return .coalesced;
        }
        if (self.pending_count >= self.policy.max_write_queue_entries or
            memory_bytes > self.policy.max_write_queue_bytes or
            self.pending_bytes > self.policy.max_write_queue_bytes - memory_bytes)
        {
            _ = self.recordDroppedLocked(bytes.len);
            self.mutex.unlock(io);
            self.alloc.free(owned_key);
            releaseMemoryReservation(&memory_reservation);
            return .dropped;
        }

        self.pending.put(self.alloc, owned_key, {}) catch {
            _ = self.recordDroppedLocked(bytes.len);
            self.mutex.unlock(io);
            self.alloc.free(owned_key);
            releaseMemoryReservation(&memory_reservation);
            return .dropped;
        };
        self.pending_bytes += memory_bytes;
        self.pending_count += 1;
        self.mutex.unlock(io);

        const owned_bytes = self.alloc.dupe(u8, bytes) catch {
            self.rollbackWriteReservation(owned_key, memory_bytes, bytes.len, &memory_reservation);
            return .dropped;
        };

        self.mutex.lockUncancelable(io);
        self.queue.ensureUnusedCapacity(self.alloc, 1) catch {
            _ = self.pending.remove(owned_key);
            decrementSaturating(&self.pending_bytes, memory_bytes);
            decrementSaturating(&self.pending_count, 1);
            _ = self.recordDroppedLocked(bytes.len);
            self.condition.broadcast(io);
            self.mutex.unlock(io);
            self.alloc.free(owned_key);
            self.alloc.free(owned_bytes);
            releaseMemoryReservation(&memory_reservation);
            return .dropped;
        };
        self.queue.appendAssumeCapacity(.{
            .cache_key = owned_key,
            .bytes = owned_bytes,
            .memory_bytes = memory_bytes,
            .disk_bytes = disk_bytes,
            .memory_reservation = memory_reservation,
        });
        memory_reservation = null;
        self.condition.signal(io);
        self.mutex.unlock(io);
        return .enqueued;
    }

    fn rollbackWriteReservation(
        self: *PersistentObjectRangeCacheState,
        owned_key: []u8,
        memory_bytes: usize,
        payload_bytes: usize,
        memory_reservation: *?resource_manager_mod.Reservation,
    ) void {
        const io = self.io;
        self.mutex.lockUncancelable(io);
        _ = self.pending.remove(owned_key);
        decrementSaturating(&self.pending_bytes, memory_bytes);
        decrementSaturating(&self.pending_count, 1);
        _ = self.recordDroppedLocked(payload_bytes);
        self.condition.broadcast(io);
        self.mutex.unlock(io);
        self.alloc.free(owned_key);
        releaseMemoryReservation(memory_reservation);
    }

    fn recordDropped(self: *PersistentObjectRangeCacheState, byte_len: usize) PersistentObjectRangeCacheEnqueueResult {
        const io = self.io;
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        return self.recordDroppedLocked(byte_len);
    }

    fn recordDroppedLocked(self: *PersistentObjectRangeCacheState, byte_len: usize) PersistentObjectRangeCacheEnqueueResult {
        self.stats.writes_dropped += 1;
        self.stats.dropped_bytes +|= byte_len;
        return .dropped;
    }

    fn recordReadHit(self: *PersistentObjectRangeCacheState) void {
        const io = self.io;
        self.mutex.lockUncancelable(io);
        self.stats.read_hits += 1;
        self.mutex.unlock(io);
    }

    fn recordReadMiss(self: *PersistentObjectRangeCacheState) void {
        const io = self.io;
        self.mutex.lockUncancelable(io);
        self.stats.read_misses += 1;
        self.mutex.unlock(io);
    }

    fn recordReadError(self: *PersistentObjectRangeCacheState) void {
        const io = self.io;
        self.mutex.lockUncancelable(io);
        self.stats.read_misses += 1;
        self.stats.read_errors += 1;
        self.mutex.unlock(io);
    }

    fn pinEntry(self: *PersistentObjectRangeCacheState, filename: []const u8) ?*PersistentObjectRangeCacheEntry {
        const io = self.io;
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        const entry = self.entries.get(filename) orelse return null;
        if (entry.removing) return null;
        entry.pin_count += 1;
        self.touchEntryLocked(entry);
        return entry;
    }

    fn releaseEntry(self: *PersistentObjectRangeCacheState, entry: *PersistentObjectRangeCacheEntry, valid: bool) void {
        const io = self.io;
        self.mutex.lockUncancelable(io);
        std.debug.assert(entry.pin_count > 0);
        entry.pin_count -= 1;
        if (!valid) entry.removing = true;
        const should_remove = entry.removing and entry.pin_count == 0 and self.entries.get(entry.filename) == entry;
        self.mutex.unlock(io);
        if (should_remove) self.finishRemovingEntry(entry, true);
    }

    fn touchEntryLocked(self: *PersistentObjectRangeCacheState, entry: *PersistentObjectRangeCacheEntry) void {
        if (self.lru.last == &entry.lru_node) return;
        self.lru.remove(&entry.lru_node);
        self.lru.append(&entry.lru_node);
    }

    fn finishRemovingEntry(
        self: *PersistentObjectRangeCacheState,
        entry: *PersistentObjectRangeCacheEntry,
        corrupt: bool,
    ) void {
        const deleted = self.deleteCacheFile(entry.filename) catch false;
        const io = self.io;
        self.mutex.lockUncancelable(io);
        if (!deleted) {
            entry.removing = false;
            self.condition.broadcast(io);
            self.mutex.unlock(io);
            return;
        }
        if (self.entries.get(entry.filename) != entry or entry.pin_count != 0) {
            self.condition.broadcast(io);
            self.mutex.unlock(io);
            return;
        }
        _ = self.entries.remove(entry.filename);
        self.lru.remove(&entry.lru_node);
        decrementSaturating(&self.stats.stored_bytes, entry.disk_bytes);
        decrementSaturating(&self.stats.entries, 1);
        if (corrupt) self.stats.corrupt_entries_removed += 1;
        self.condition.broadcast(io);
        self.mutex.unlock(io);
        self.freeEntry(entry);
    }

    fn evictOne(self: *PersistentObjectRangeCacheState) bool {
        const io = self.io;
        self.mutex.lockUncancelable(io);
        var node = self.lru.first;
        const victim = while (node) |candidate_node| : (node = candidate_node.next) {
            const candidate: *PersistentObjectRangeCacheEntry = @alignCast(@fieldParentPtr("lru_node", candidate_node));
            if (candidate.pin_count == 0 and !candidate.removing) {
                candidate.removing = true;
                break candidate;
            }
        } else {
            self.mutex.unlock(io);
            return false;
        };
        self.mutex.unlock(io);

        const deleted = self.deleteCacheFile(victim.filename) catch false;
        self.mutex.lockUncancelable(io);
        if (!deleted) {
            victim.removing = false;
            self.mutex.unlock(io);
            return false;
        }
        _ = self.entries.remove(victim.filename);
        self.lru.remove(&victim.lru_node);
        decrementSaturating(&self.stats.stored_bytes, victim.disk_bytes);
        decrementSaturating(&self.stats.entries, 1);
        self.stats.evicted_bytes += victim.disk_bytes;
        self.stats.evicted_entries += 1;
        self.mutex.unlock(io);
        self.freeEntry(victim);
        return true;
    }

    fn hasCapacityFor(self: *PersistentObjectRangeCacheState, disk_bytes: usize) bool {
        const io = self.io;
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        return self.stats.entries < self.policy.max_entries and
            self.stats.stored_bytes <= self.policy.max_total_bytes and
            disk_bytes <= self.policy.max_total_bytes - self.stats.stored_bytes;
    }

    fn makeCapacityFor(self: *PersistentObjectRangeCacheState, disk_bytes: usize) bool {
        if (disk_bytes > self.policy.max_total_bytes) return false;
        while (!self.hasCapacityFor(disk_bytes)) {
            if (!self.evictOne()) return false;
        }
        return true;
    }

    fn reserveWriteCapacity(
        self: *PersistentObjectRangeCacheState,
        disk_bytes: usize,
    ) !?resource_manager_mod.CapacityReservation {
        const manager = self.resource_manager orelse return null;
        const source = manager.capacitySource() orelse return null;
        const observation = try source.current();
        const timestamp_ns = std.Io.Timestamp.now(self.io, .awake).toNanoseconds();
        const now_ns: u64 = std.math.cast(u64, timestamp_ns) orelse
            if (timestamp_ns < 0) 0 else std.math.maxInt(u64);
        return manager.reserveCapacity(
            self.alloc,
            source.domain_id,
            @intCast(disk_bytes),
            observation,
            now_ns,
        ) catch |err| switch (err) {
            error.CapacityUnavailable, error.CapacityObservationStale => return error.PersistentObjectRangeCacheCapacityUnavailable,
            else => return err,
        };
    }

    fn writeQueued(self: *PersistentObjectRangeCacheState, task: PersistentObjectRangeCacheWrite) !PersistentObjectRangeCacheWriteOutcome {
        const filename = try objectRangeCacheKeyDigestHexAlloc(self.alloc, task.cache_key);
        defer self.alloc.free(filename);
        const io = self.io;
        self.mutex.lockUncancelable(io);
        while (self.entries.get(filename)) |entry| {
            if (entry.removing) {
                self.condition.waitUncancelable(io, &self.mutex);
                continue;
            }
            self.touchEntryLocked(entry);
            self.mutex.unlock(io);
            return .already_present;
        }
        self.mutex.unlock(io);
        if (!self.makeCapacityFor(task.disk_bytes)) return .capacity_unavailable;
        var capacity_reservation = self.reserveWriteCapacity(task.disk_bytes) catch |err| switch (err) {
            error.PersistentObjectRangeCacheCapacityUnavailable => return .capacity_unavailable,
            else => return err,
        };
        defer if (capacity_reservation) |*reservation| reservation.release();

        const path = try std.fs.path.join(self.alloc, &.{ self.root_dir, filename });
        defer self.alloc.free(path);
        const entry = try self.alloc.create(PersistentObjectRangeCacheEntry);
        errdefer self.alloc.destroy(entry);
        const owned_filename = try self.alloc.dupe(u8, filename);
        errdefer self.alloc.free(owned_filename);
        entry.* = .{
            .filename = owned_filename,
            .disk_bytes = task.disk_bytes,
            .modified_ns = 0,
        };
        self.mutex.lockUncancelable(io);
        self.entries.ensureUnusedCapacity(self.alloc, 1) catch |err| {
            self.mutex.unlock(io);
            return err;
        };
        self.mutex.unlock(io);
        try persistentObjectRangeWriteEntryAtomicallyWithIo(
            io,
            path,
            task.cache_key,
            task.bytes,
            self.policy.durability,
        );

        self.mutex.lockUncancelable(io);
        std.debug.assert(!self.entries.contains(entry.filename));
        self.entries.putAssumeCapacityNoClobber(entry.filename, entry);
        self.lru.append(&entry.lru_node);
        self.stats.stored_bytes +|= entry.disk_bytes;
        self.stats.entries +|= 1;
        self.mutex.unlock(io);
        return .written;
    }

    fn deleteCacheFile(self: *PersistentObjectRangeCacheState, filename: []const u8) !bool {
        const io = self.io;
        var dir = if (std.fs.path.isAbsolute(self.root_dir))
            try std.Io.Dir.openDirAbsolute(io, self.root_dir, .{})
        else
            try std.Io.Dir.cwd().openDir(io, self.root_dir, .{});
        defer dir.close(io);
        dir.deleteFile(io, filename) catch |err| switch (err) {
            error.FileNotFound => return true,
            else => return err,
        };
        return true;
    }
};

fn persistentObjectRangeWorkerMain(state: *PersistentObjectRangeCacheState) void {
    const io = state.io;
    while (true) {
        state.mutex.lockUncancelable(io);
        while (state.queue_head == state.queue.items.len and !state.closing) {
            state.condition.waitUncancelable(io, &state.mutex);
        }
        if (state.queue_head == state.queue.items.len and state.closing) {
            state.mutex.unlock(io);
            return;
        }
        var task = state.queue.items[state.queue_head];
        state.queue_head += 1;
        if (state.queue_head == state.queue.items.len) {
            state.queue.clearRetainingCapacity();
            state.queue_head = 0;
        }
        state.mutex.unlock(io);

        const outcome = state.writeQueued(task) catch null;
        state.mutex.lockUncancelable(io);
        _ = state.pending.remove(task.cache_key);
        decrementSaturating(&state.pending_bytes, task.memory_bytes);
        decrementSaturating(&state.pending_count, 1);
        if (outcome) |result| switch (result) {
            .written, .already_present => state.stats.writes_completed += 1,
            .capacity_unavailable => {
                state.stats.writes_dropped += 1;
                state.stats.dropped_bytes +|= task.bytes.len;
            },
        } else {
            state.stats.write_errors += 1;
        }
        state.condition.broadcast(io);
        state.mutex.unlock(io);
        state.alloc.free(task.cache_key);
        state.alloc.free(task.bytes);
        releaseMemoryReservation(&task.memory_reservation);
    }
}

fn releaseMemoryReservation(reservation: *?resource_manager_mod.Reservation) void {
    if (reservation.*) |*held| held.release();
    reservation.* = null;
}

fn persistentObjectRangeEncodedLen(cache_key_len: usize, payload_len: usize) ?usize {
    const fixed_len = persistent_object_range_cache_magic.len + 4 + std.crypto.hash.sha2.Sha256.digest_length;
    const header_len = std.math.add(usize, fixed_len, cache_key_len) catch return null;
    return std.math.add(usize, header_len, payload_len) catch null;
}

fn isPersistentObjectRangeCacheFilename(name: []const u8) bool {
    if (name.len != std.crypto.hash.sha2.Sha256.digest_length * 2) return false;
    for (name) |byte| if (!std.ascii.isDigit(byte) and !(byte >= 'a' and byte <= 'f')) return false;
    return true;
}

fn isPersistentObjectRangeTemporaryFilename(name: []const u8) bool {
    const digest_len = std.crypto.hash.sha2.Sha256.digest_length * 2;
    return name.len > digest_len and isPersistentObjectRangeCacheFilename(name[0..digest_len]) and
        std.mem.startsWith(u8, name[digest_len..], ".tmp-");
}

pub const ObjectRangeCache = struct {
    entries: std.StringHashMapUnmanaged(ObjectRangeCacheEntry) = .empty,
    stats: ObjectRangeCacheStats = .{},
    policy: ObjectRangeCachePolicy = .{},
    persistent: ?*PersistentObjectRangeCache = null,

    pub fn initWithLakeServingDefaults(max_total_bytes: usize) ObjectRangeCache {
        return .{ .policy = ObjectRangeCachePolicy.lakeServingDefaults(max_total_bytes) };
    }

    pub fn deinit(self: *ObjectRangeCache, alloc: Allocator) void {
        var iter = self.entries.iterator();
        while (iter.next()) |entry| {
            alloc.free(entry.key_ptr.*);
            alloc.free(entry.value_ptr.bytes);
        }
        self.entries.deinit(alloc);
        self.* = undefined;
    }

    pub fn statsSnapshot(self: ObjectRangeCache) ObjectRangeCacheStats {
        return self.stats;
    }

    fn currentBytesForLane(self: ObjectRangeCache, cache_lane: range_io.CacheLane) usize {
        const lane_stats = self.stats.lane(cache_lane);
        return lane_stats.stored_bytes;
    }

    fn ensureLaneCapacity(self: *ObjectRangeCache, alloc: Allocator, cache_lane: range_io.CacheLane, new_bytes: usize) void {
        const limit = self.policy.laneLimit(cache_lane) orelse return;
        if (new_bytes > limit) return;
        while (self.currentBytesForLane(cache_lane) > limit - new_bytes) {
            if (!self.evictOneEntryForLane(alloc, cache_lane)) return;
        }
    }

    fn ensureTotalCapacity(self: *ObjectRangeCache, alloc: Allocator, incoming_lane: range_io.CacheLane, new_bytes: usize) void {
        const limit = self.policy.max_total_bytes orelse return;
        if (new_bytes > limit) return;
        while (self.stats.stored_bytes > limit - new_bytes) {
            if (!self.evictOneEntryForTotal(alloc, incoming_lane)) return;
        }
    }

    fn admitsLaneBytes(self: ObjectRangeCache, cache_lane: range_io.CacheLane, new_bytes: usize) bool {
        const limit = self.policy.laneLimit(cache_lane) orelse return true;
        return self.currentBytesForLane(cache_lane) <= limit and new_bytes <= limit - self.currentBytesForLane(cache_lane);
    }

    fn admitsTotalBytes(self: ObjectRangeCache, new_bytes: usize) bool {
        const limit = self.policy.max_total_bytes orelse return true;
        return self.stats.stored_bytes <= limit and new_bytes <= limit - self.stats.stored_bytes;
    }

    fn evictOneEntryForLane(self: *ObjectRangeCache, alloc: Allocator, cache_lane: range_io.CacheLane) bool {
        var iter = self.entries.iterator();
        while (iter.next()) |entry| {
            const entry_lane = cacheLaneFromObjectRangeCacheKey(entry.key_ptr.*) orelse continue;
            if (entry_lane != cache_lane) continue;
            const key = entry.key_ptr.*;
            const value = entry.value_ptr.*;
            const byte_len = value.bytes.len;
            if (!self.entries.remove(key)) return false;
            alloc.free(key);
            alloc.free(value.bytes);
            decrementSaturating(&self.stats.stored_bytes, byte_len);
            decrementSaturating(&self.stats.lanes[@intFromEnum(cache_lane)].stored_bytes, byte_len);
            self.stats.evicted_bytes += byte_len;
            self.stats.lanes[@intFromEnum(cache_lane)].evicted_bytes += byte_len;
            return true;
        }
        return false;
    }

    fn evictOneEntryForTotal(self: *ObjectRangeCache, alloc: Allocator, incoming_lane: range_io.CacheLane) bool {
        var iter = self.entries.iterator();
        while (iter.next()) |entry| {
            const entry_lane = cacheLaneFromObjectRangeCacheKey(entry.key_ptr.*) orelse continue;
            if (!self.canEvictLaneForIncoming(entry_lane, incoming_lane)) continue;
            return self.evictEntry(alloc, entry.key_ptr.*, entry.value_ptr.*, entry_lane);
        }
        return false;
    }

    fn canEvictLaneForIncoming(
        self: ObjectRangeCache,
        entry_lane: range_io.CacheLane,
        incoming_lane: range_io.CacheLane,
    ) bool {
        if (entry_lane == incoming_lane) return true;
        if (self.policy.isProtected(entry_lane) and !self.policy.isProtected(incoming_lane)) return false;
        return true;
    }

    fn evictEntry(
        self: *ObjectRangeCache,
        alloc: Allocator,
        key: []const u8,
        value: ObjectRangeCacheEntry,
        cache_lane: range_io.CacheLane,
    ) bool {
        const byte_len = value.bytes.len;
        if (!self.entries.remove(key)) return false;
        alloc.free(key);
        alloc.free(value.bytes);
        decrementSaturating(&self.stats.stored_bytes, byte_len);
        decrementSaturating(&self.stats.lanes[@intFromEnum(cache_lane)].stored_bytes, byte_len);
        self.stats.evicted_bytes += byte_len;
        self.stats.lanes[@intFromEnum(cache_lane)].evicted_bytes += byte_len;
        return true;
    }

    pub fn readAlloc(
        self: *ObjectRangeCache,
        alloc: Allocator,
        reader: ObjectRangeReader,
        read: range_io.RangeRead,
    ) ![]u8 {
        const cache_lane = read.cacheLane();
        const read_len: usize = std.math.cast(usize, read.range.len) orelse {
            return error.InvalidLakeRangeRead;
        };
        if (read_len > self.policy.max_fetch_bytes) return error.LakeRangeReadTooLarge;
        const cache_key = try read.cacheKeyAlloc(alloc);
        defer alloc.free(cache_key);
        if (self.entries.get(cache_key)) |cached| {
            if (cached.bytes.len != read_len) {
                return error.InvalidLakeRangeRead;
            }
            const checksum = objectRangeCacheDigest(cached.bytes);
            if (!std.mem.eql(u8, &cached.checksum, &checksum)) {
                return error.InvalidLakeRangeRead;
            }
            self.stats.hits += 1;
            self.stats.lanes[@intFromEnum(cache_lane)].hits += 1;
            return try alloc.dupe(u8, cached.bytes);
        }

        if (self.persistent) |persistent| {
            if (try persistent.readAlloc(alloc, cache_key, read_len)) |bytes| {
                errdefer alloc.free(bytes);
                self.stats.hits += 1;
                self.stats.lanes[@intFromEnum(cache_lane)].hits += 1;
                try self.storeFetchedBytes(alloc, cache_key, cache_lane, bytes, false);
                return bytes;
            }
        }

        self.stats.misses += 1;
        self.stats.lanes[@intFromEnum(cache_lane)].misses += 1;
        const bytes = try readObjectRangeAlloc(alloc, reader, read);
        errdefer alloc.free(bytes);
        try self.storeFetchedBytes(alloc, cache_key, cache_lane, bytes, true);
        return bytes;
    }

    fn storeFetchedBytes(
        self: *ObjectRangeCache,
        alloc: Allocator,
        cache_key: []const u8,
        cache_lane: range_io.CacheLane,
        bytes: []const u8,
        write_persistent: bool,
    ) !void {
        if (write_persistent) {
            if (self.persistent) |persistent| _ = persistent.enqueueWrite(cache_key, bytes);
        }
        self.ensureLaneCapacity(alloc, cache_lane, bytes.len);
        self.ensureTotalCapacity(alloc, cache_lane, bytes.len);
        if (!self.admitsLaneBytes(cache_lane, bytes.len) or !self.admitsTotalBytes(bytes.len)) {
            self.stats.rejected_bytes += bytes.len;
            self.stats.lanes[@intFromEnum(cache_lane)].rejected_bytes += bytes.len;
            return;
        }
        const stored = try alloc.dupe(u8, bytes);
        errdefer alloc.free(stored);
        const entry = ObjectRangeCacheEntry{
            .bytes = stored,
            .checksum = objectRangeCacheDigest(stored),
        };
        const owned_key = try alloc.dupe(u8, cache_key);
        errdefer alloc.free(owned_key);
        try self.entries.put(alloc, owned_key, entry);
        self.stats.stored_bytes += stored.len;
        self.stats.lanes[@intFromEnum(cache_lane)].stored_bytes += stored.len;
    }
};

const persistent_object_range_cache_magic = "AFORC02\n";
var persistent_object_range_cache_nonce: std.atomic.Value(u64) = .init(0);

fn objectRangeCacheDigest(bytes: []const u8) ObjectRangeCacheDigest {
    var digest: ObjectRangeCacheDigest = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    return digest;
}

fn objectRangeCacheKeyDigestHexAlloc(alloc: Allocator, cache_key: []const u8) ![]u8 {
    const digest = objectRangeCacheDigest(cache_key);
    const out = try alloc.alloc(u8, std.crypto.hash.sha2.Sha256.digest_length * 2);
    for (digest, 0..) |byte, idx| {
        out[idx * 2] = hexNibble(byte >> 4);
        out[idx * 2 + 1] = hexNibble(byte & 0x0f);
    }
    return out;
}

fn persistentObjectRangeEncodeAlloc(alloc: Allocator, cache_key: []const u8, bytes: []const u8) ![]u8 {
    const cache_key_len: u32 = std.math.cast(u32, cache_key.len) orelse return error.InvalidLakeRangeRead;
    const digest = objectRangeCacheDigest(bytes);
    const digest_len = std.crypto.hash.sha2.Sha256.digest_length;
    const key_len_field = 4;
    const header_len = std.math.add(
        usize,
        persistent_object_range_cache_magic.len + key_len_field + digest_len,
        cache_key.len,
    ) catch return error.InvalidLakeRangeRead;
    const encoded_len = std.math.add(usize, header_len, bytes.len) catch return error.InvalidLakeRangeRead;
    const encoded = try alloc.alloc(u8, encoded_len);
    var offset: usize = 0;
    @memcpy(encoded[offset..][0..persistent_object_range_cache_magic.len], persistent_object_range_cache_magic);
    offset += persistent_object_range_cache_magic.len;
    std.mem.writeInt(u32, encoded[offset..][0..key_len_field], cache_key_len, .little);
    offset += key_len_field;
    @memcpy(encoded[offset..][0..cache_key.len], cache_key);
    offset += cache_key.len;
    @memcpy(encoded[offset..][0..digest_len], &digest);
    offset += digest_len;
    @memcpy(encoded[offset..], bytes);
    return encoded;
}

fn persistentObjectRangeReadEntryWithIoAlloc(
    io: std.Io,
    alloc: Allocator,
    path: []const u8,
    cache_key: []const u8,
    expected_len: usize,
) ![]u8 {
    const digest_len = std.crypto.hash.sha2.Sha256.digest_length;
    const key_len_field = 4;
    const fixed_header_len = persistent_object_range_cache_magic.len + key_len_field + digest_len;
    const header_len = std.math.add(usize, fixed_header_len, cache_key.len) catch
        return error.InvalidPersistentObjectRangeCache;
    const exact_len = std.math.add(usize, header_len, expected_len) catch
        return error.InvalidPersistentObjectRangeCache;
    const exact_len_u64 = std.math.cast(u64, exact_len) orelse
        return error.InvalidPersistentObjectRangeCache;

    var file = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);
    if (try file.length(io) != exact_len_u64) return error.InvalidPersistentObjectRangeCache;

    var prefix: [persistent_object_range_cache_magic.len + key_len_field]u8 = undefined;
    if (try file.readPositionalAll(io, &prefix, 0) != prefix.len)
        return error.InvalidPersistentObjectRangeCache;
    if (!std.mem.eql(u8, prefix[0..persistent_object_range_cache_magic.len], persistent_object_range_cache_magic))
        return error.InvalidPersistentObjectRangeCache;
    const stored_cache_key_len = std.mem.readInt(
        u32,
        prefix[persistent_object_range_cache_magic.len..][0..key_len_field],
        .little,
    );
    const expected_cache_key_len = std.math.cast(u32, cache_key.len) orelse
        return error.InvalidPersistentObjectRangeCache;
    if (stored_cache_key_len != expected_cache_key_len)
        return error.InvalidPersistentObjectRangeCache;

    var offset: u64 = prefix.len;
    var key_offset: usize = 0;
    var compare_buffer: [4096]u8 = undefined;
    while (key_offset < cache_key.len) {
        const chunk_len = @min(compare_buffer.len, cache_key.len - key_offset);
        if (try file.readPositionalAll(io, compare_buffer[0..chunk_len], offset) != chunk_len)
            return error.InvalidPersistentObjectRangeCache;
        if (!std.mem.eql(u8, compare_buffer[0..chunk_len], cache_key[key_offset..][0..chunk_len]))
            return error.InvalidPersistentObjectRangeCache;
        key_offset += chunk_len;
        offset += @intCast(chunk_len);
    }

    var expected_checksum: [digest_len]u8 = undefined;
    if (try file.readPositionalAll(io, &expected_checksum, offset) != digest_len)
        return error.InvalidPersistentObjectRangeCache;
    offset += digest_len;

    const payload = try alloc.alloc(u8, expected_len);
    errdefer alloc.free(payload);
    if (try file.readPositionalAll(io, payload, offset) != expected_len)
        return error.InvalidPersistentObjectRangeCache;
    var extra: [1]u8 = undefined;
    if (try file.readPositionalAll(io, &extra, exact_len_u64) != 0)
        return error.InvalidPersistentObjectRangeCache;
    if (try file.length(io) != exact_len_u64) return error.InvalidPersistentObjectRangeCache;
    const actual_checksum = objectRangeCacheDigest(payload);
    if (!std.crypto.timing_safe.eql([digest_len]u8, expected_checksum, actual_checksum))
        return error.InvalidPersistentObjectRangeCache;
    return payload;
}

fn persistentObjectRangeEnsureParentDir(io: std.Io, path: []const u8) !void {
    const parent = std.fs.path.dirname(path) orelse return;
    try fs_paths.createDirPathPortable(io, parent);
}

fn persistentObjectRangeWriteFileAtomically(
    io: std.Io,
    path: []const u8,
    contents: []const u8,
    durability: PersistentObjectRangeCacheDurability,
) !void {
    return persistentObjectRangeWriteFilePartsAtomicallyWithIo(io, path, &.{contents}, durability);
}

fn persistentObjectRangeWriteEntryAtomically(
    io: std.Io,
    path: []const u8,
    cache_key: []const u8,
    bytes: []const u8,
    durability: PersistentObjectRangeCacheDurability,
) !void {
    return try persistentObjectRangeWriteEntryAtomicallyWithIo(io, path, cache_key, bytes, durability);
}

fn persistentObjectRangeWriteEntryAtomicallyWithIo(
    io: std.Io,
    path: []const u8,
    cache_key: []const u8,
    bytes: []const u8,
    durability: PersistentObjectRangeCacheDurability,
) !void {
    const cache_key_len: u32 = std.math.cast(u32, cache_key.len) orelse return error.InvalidLakeRangeRead;
    var cache_key_len_bytes: [4]u8 = undefined;
    std.mem.writeInt(u32, &cache_key_len_bytes, cache_key_len, .little);
    const digest = objectRangeCacheDigest(bytes);
    try persistentObjectRangeWriteFilePartsAtomicallyWithIo(io, path, &.{
        persistent_object_range_cache_magic,
        &cache_key_len_bytes,
        cache_key,
        &digest,
        bytes,
    }, durability);
}

fn persistentObjectRangeWriteFilePartsAtomicallyWithIo(
    io: std.Io,
    path: []const u8,
    parts: []const []const u8,
    durability: PersistentObjectRangeCacheDurability,
) !void {
    var entropy: [8]u8 = undefined;
    try io.randomSecure(&entropy);
    const random_suffix = std.fmt.bytesToHex(entropy, .lower);
    const tmp_path = try std.fmt.allocPrint(std.heap.page_allocator, "{s}.tmp-{d}-{s}", .{
        path,
        persistent_object_range_cache_nonce.fetchAdd(1, .monotonic),
        &random_suffix,
    });
    defer std.heap.page_allocator.free(tmp_path);
    errdefer if (std.fs.path.isAbsolute(tmp_path))
        std.Io.Dir.deleteFileAbsolute(io, tmp_path) catch {}
    else
        std.Io.Dir.cwd().deleteFile(io, tmp_path) catch {};
    {
        var file = if (std.fs.path.isAbsolute(tmp_path))
            try std.Io.Dir.createFileAbsolute(io, tmp_path, .{ .truncate = true })
        else
            try std.Io.Dir.cwd().createFile(io, tmp_path, .{ .truncate = true });
        defer file.close(io);
        var buf: [4096]u8 = undefined;
        var writer = file.writer(io, &buf);
        for (parts) |part| try writer.interface.writeAll(part);
        try writer.end();
        if (durability == .durable) try file.sync(io);
    }
    if (std.fs.path.isAbsolute(path)) {
        try std.Io.Dir.renameAbsolute(tmp_path, path, io);
    } else {
        try std.Io.Dir.rename(std.Io.Dir.cwd(), tmp_path, std.Io.Dir.cwd(), path, io);
    }
    if (durability == .durable) {
        if (std.fs.path.dirname(path)) |parent| try fs_paths.syncDirPortable(io, parent);
    }
}

fn hexNibble(value: u8) u8 {
    return if (value < 10) '0' + value else 'a' + (value - 10);
}

fn decrementSaturating(value: *usize, amount: usize) void {
    value.* = if (value.* >= amount) value.* - amount else 0;
}

fn cacheLaneFromObjectRangeCacheKey(cache_key: []const u8) ?range_io.CacheLane {
    const marker = ":purpose=";
    const marker_index = std.mem.indexOf(u8, cache_key, marker) orelse return null;
    const purpose_start = marker_index + marker.len;
    const purpose_end = std.mem.indexOfScalarPos(u8, cache_key, purpose_start, ':') orelse cache_key.len;
    const purpose_name = cache_key[purpose_start..purpose_end];
    const purpose = std.meta.stringToEnum(range_io.RangePurpose, purpose_name) orelse return null;
    return purpose.cacheLane();
}

pub const ObjectRangeRowGroupInput = struct {
    file_id: []const u8,
    row_group_ordinal: u32,
    projected_columns: []const []const u8,

    pub fn validate(self: ObjectRangeRowGroupInput) !void {
        if (self.file_id.len == 0) return error.InvalidParquetRowGroupBatch;
        if (self.projected_columns.len == 0) return error.InvalidParquetRowGroupBatch;
        for (self.projected_columns) |column| {
            if (column.len == 0) return error.InvalidParquetRowGroupBatch;
        }
    }
};

pub const ObjectRangeRowGroupPlan = struct {
    row_groups: []ObjectRangeRowGroupInput,

    pub fn deinit(self: *ObjectRangeRowGroupPlan, alloc: Allocator) void {
        alloc.free(self.row_groups);
        self.* = undefined;
    }
};

pub const DiscoveredObjectRangeRowGroupPlan = struct {
    inventory: external_source.Inventory,
    row_group_plan: ObjectRangeRowGroupPlan,

    pub fn deinit(self: *DiscoveredObjectRangeRowGroupPlan, alloc: Allocator) void {
        self.row_group_plan.deinit(alloc);
        self.inventory.deinit(alloc);
        self.* = undefined;
    }
};

pub const ObjectRangeRowsQueryRequest = struct {
    binding: ?external_binding.Binding = null,
    reader: ObjectRangeReader,
    cache: ?*ObjectRangeCache = null,
    inventory: external_source.Inventory,
    projected_columns: []const []const u8,
    predicate: ?lake_rows.Predicate = null,
    limit: ?usize = null,
    deleted_row_refs: []const rowsource.RowRef = &.{},
    scan_limits: lake_rows.ScanLimits = .{},
    materialization_limits: MaterializationLimits = .{},
    coalesce_options: range_io.CoalesceOptions = .{},
    sidecars: []const sidecar_manifest.DeclaredArtifact = &.{},
    desired_sidecars: []const lake_sidecar_selection.DesiredSidecar = &.{},
    sidecar_policy: lake_sidecar_selection.Policy = .{},
    candidates: []const lake_rows.SidecarCandidateSet = &.{},
};

pub const ObjectRangeExpressionAggregateRequest = struct {
    binding: ?external_binding.Binding = null,
    reader: ObjectRangeReader,
    cache: ?*ObjectRangeCache = null,
    inventory: external_source.Inventory,
    aggregate: lake_rows.ExpressionAggregateRequest,
    materialization_limits: MaterializationLimits = .{},
    coalesce_options: range_io.CoalesceOptions = .{},
};

pub const OwnedBatch = struct {
    batch: rowsource.ColumnBatch,
    row_refs: []rowsource.RowRef,
    columns: []rowsource.ColumnVector,
    column_names: [][]u8,
    decoded_columns: []DecodedColumn,
    null_bitmaps: [][]u8,

    pub fn deinit(self: *OwnedBatch, alloc: Allocator) void {
        for (self.column_names) |name| alloc.free(name);
        alloc.free(self.column_names);
        for (self.decoded_columns) |*column| column.deinit(alloc);
        alloc.free(self.decoded_columns);
        for (self.null_bitmaps) |nulls| {
            if (nulls.len > 0) alloc.free(nulls);
        }
        alloc.free(self.null_bitmaps);
        alloc.free(self.columns);
        alloc.free(self.row_refs);
        self.* = undefined;
    }
};

const DecodedColumn = union(enum) {
    i64: []i64,
    f64: []f64,
    bool: []bool,
    bytes: [][]u8,

    fn deinit(self: *DecodedColumn, alloc: Allocator) void {
        switch (self.*) {
            .i64 => |values| alloc.free(values),
            .f64 => |values| alloc.free(values),
            .bool => |values| alloc.free(values),
            .bytes => |values| parquet_page.freePlainByteArrays(alloc, values),
        }
        self.* = undefined;
    }
};

pub const RowGroupSource = struct {
    inventory: external_source.Inventory,
    row_groups: []const RowGroupInput,
    materialization_limits: MaterializationLimits = .{},
    next_index: usize = 0,
    current: ?OwnedBatch = null,

    pub fn init(inventory: external_source.Inventory, row_groups: []const RowGroupInput) !RowGroupSource {
        try inventory.validate();
        if (inventory.format != .parquet and inventory.format != .iceberg) return error.InvalidParquetRowGroupBatch;
        if (row_groups.len == 0) return error.InvalidParquetRowGroupBatch;
        for (row_groups) |row_group| try row_group.validate();
        return .{
            .inventory = inventory,
            .row_groups = row_groups,
        };
    }

    pub fn deinit(self: *RowGroupSource, alloc: Allocator) void {
        self.clearCurrent(alloc);
        self.* = undefined;
    }

    pub fn rowSource(self: *RowGroupSource) rowsource.Source {
        return .{
            .kind = .external_parquet,
            .ctx = self,
            .next_batch = nextBatch,
            .deinit_fn = deinitSource,
        };
    }

    fn nextBatch(ctx: *anyopaque, alloc: Allocator) !?rowsource.ColumnBatch {
        const self: *RowGroupSource = @ptrCast(@alignCast(ctx));
        self.clearCurrent(alloc);
        if (self.next_index >= self.row_groups.len) return null;

        const input = self.row_groups[self.next_index];
        self.next_index += 1;
        self.current = try buildSupportedI64RowGroupBatchAllocWithLimits(
            alloc,
            self.inventory,
            input.file_id,
            input.row_group_ordinal,
            input.chunks,
            self.materialization_limits,
        );
        return self.current.?.batch;
    }

    fn deinitSource(ctx: *anyopaque, alloc: Allocator) void {
        const self: *RowGroupSource = @ptrCast(@alignCast(ctx));
        self.clearCurrent(alloc);
    }

    fn clearCurrent(self: *RowGroupSource, alloc: Allocator) void {
        if (self.current) |*current| {
            current.deinit(alloc);
            self.current = null;
        }
    }
};

pub const ObjectRangeRowGroupSource = struct {
    reader: ObjectRangeReader,
    cache: ?*ObjectRangeCache = null,
    inventory: external_source.Inventory,
    row_groups: []const ObjectRangeRowGroupInput,
    coalesce_options: range_io.CoalesceOptions = .{},
    materialization_limits: MaterializationLimits = .{},
    next_index: usize = 0,
    current: ?OwnedBatch = null,

    pub fn init(
        reader: ObjectRangeReader,
        inventory: external_source.Inventory,
        row_groups: []const ObjectRangeRowGroupInput,
    ) !ObjectRangeRowGroupSource {
        try inventory.validate();
        if (inventory.format != .parquet and inventory.format != .iceberg) return error.InvalidParquetRowGroupBatch;
        if (row_groups.len == 0) return error.InvalidParquetRowGroupBatch;
        for (row_groups) |row_group| try row_group.validate();
        return .{
            .reader = reader,
            .inventory = inventory,
            .row_groups = row_groups,
        };
    }

    pub fn initWithCoalesceOptions(
        reader: ObjectRangeReader,
        inventory: external_source.Inventory,
        row_groups: []const ObjectRangeRowGroupInput,
        coalesce_options: range_io.CoalesceOptions,
    ) !ObjectRangeRowGroupSource {
        var source = try init(reader, inventory, row_groups);
        source.coalesce_options = coalesce_options;
        return source;
    }

    pub fn initWithCacheAndCoalesceOptions(
        reader: ObjectRangeReader,
        cache: *ObjectRangeCache,
        inventory: external_source.Inventory,
        row_groups: []const ObjectRangeRowGroupInput,
        coalesce_options: range_io.CoalesceOptions,
    ) !ObjectRangeRowGroupSource {
        var source = try initWithCoalesceOptions(reader, inventory, row_groups, coalesce_options);
        source.cache = cache;
        return source;
    }

    pub fn deinit(self: *ObjectRangeRowGroupSource, alloc: Allocator) void {
        self.clearCurrent(alloc);
        self.* = undefined;
    }

    pub fn rowSource(self: *ObjectRangeRowGroupSource) rowsource.Source {
        return .{
            .kind = sourceKindForInventoryFormat(self.inventory.format),
            .ctx = self,
            .next_batch = nextBatch,
            .deinit_fn = deinitSource,
        };
    }

    fn nextBatch(ctx: *anyopaque, alloc: Allocator) !?rowsource.ColumnBatch {
        const self: *ObjectRangeRowGroupSource = @ptrCast(@alignCast(ctx));
        self.clearCurrent(alloc);
        if (self.next_index >= self.row_groups.len) return null;

        const input = self.row_groups[self.next_index];
        self.next_index += 1;
        self.current = try buildSupportedI64RowGroupBatchFromMaybeCachedCoalescedObjectRangeReaderAlloc(
            alloc,
            self.reader,
            self.cache,
            self.inventory,
            input.file_id,
            input.row_group_ordinal,
            input.projected_columns,
            self.coalesce_options,
            self.materialization_limits,
        );
        return self.current.?.batch;
    }

    fn deinitSource(ctx: *anyopaque, alloc: Allocator) void {
        const self: *ObjectRangeRowGroupSource = @ptrCast(@alignCast(ctx));
        self.clearCurrent(alloc);
    }

    fn clearCurrent(self: *ObjectRangeRowGroupSource, alloc: Allocator) void {
        if (self.current) |*current| {
            current.deinit(alloc);
            self.current = null;
        }
    }
};

pub fn buildRequiredPlainI64RowGroupBatchAlloc(
    alloc: Allocator,
    inventory: external_source.Inventory,
    file_id: []const u8,
    row_group_ordinal: u32,
    projected_chunks: []const ColumnChunkInput,
) !OwnedBatch {
    return try buildPlainI64RowGroupBatchAlloc(alloc, inventory, file_id, row_group_ordinal, projected_chunks, .{ .fixed = .required }, .{});
}

pub fn buildOptionalPlainI64RowGroupBatchAlloc(
    alloc: Allocator,
    inventory: external_source.Inventory,
    file_id: []const u8,
    row_group_ordinal: u32,
    projected_chunks: []const ColumnChunkInput,
) !OwnedBatch {
    return try buildPlainI64RowGroupBatchAlloc(alloc, inventory, file_id, row_group_ordinal, projected_chunks, .{ .fixed = .optional }, .{});
}

pub fn buildDictionaryPlainI64RowGroupBatchAlloc(
    alloc: Allocator,
    inventory: external_source.Inventory,
    file_id: []const u8,
    row_group_ordinal: u32,
    projected_chunks: []const ColumnChunkInput,
) !OwnedBatch {
    return try buildPlainI64RowGroupBatchAlloc(alloc, inventory, file_id, row_group_ordinal, projected_chunks, .{ .fixed = .dictionary_required }, .{});
}

pub fn buildSupportedI64RowGroupBatchAlloc(
    alloc: Allocator,
    inventory: external_source.Inventory,
    file_id: []const u8,
    row_group_ordinal: u32,
    projected_chunks: []const ColumnChunkInput,
) !OwnedBatch {
    return try buildSupportedI64RowGroupBatchAllocWithLimits(alloc, inventory, file_id, row_group_ordinal, projected_chunks, .{});
}

pub fn buildSupportedI64RowGroupBatchAllocWithLimits(
    alloc: Allocator,
    inventory: external_source.Inventory,
    file_id: []const u8,
    row_group_ordinal: u32,
    projected_chunks: []const ColumnChunkInput,
    limits: MaterializationLimits,
) !OwnedBatch {
    return try buildPlainI64RowGroupBatchAlloc(alloc, inventory, file_id, row_group_ordinal, projected_chunks, .from_inventory, limits);
}

const PlainI64Mode = enum {
    required,
    optional,
    dictionary_required,
    dictionary_optional,
    int96_timestamp_required,
    int96_timestamp_optional,
    timestamp_millis_required,
    timestamp_millis_optional,
    timestamp_millis_dictionary_required,
    timestamp_millis_dictionary_optional,
    timestamp_micros_required,
    timestamp_micros_optional,
    timestamp_micros_dictionary_required,
    timestamp_micros_dictionary_optional,
    timestamp_nanos_required,
    timestamp_nanos_optional,
    timestamp_nanos_dictionary_required,
    timestamp_nanos_dictionary_optional,
};

const SupportedColumnMode = union(enum) {
    i64: PlainI64Mode,
    i32: PlainI32Mode,
    f64: PlainF64Mode,
    decimal: DecimalMode,
    bool: PlainBoolMode,
    bytes: ByteArrayMode,
};

const PlainI32Mode = enum {
    required,
    optional,
    dictionary_required,
    dictionary_optional,
};

const ByteArrayMode = enum {
    required,
    optional,
    dictionary_required,
    dictionary_optional,
};

const PlainF64Mode = enum {
    required,
    optional,
    dictionary_required,
    dictionary_optional,
    float_required,
    float_optional,
    float_dictionary_required,
    float_dictionary_optional,
};

const DecimalPhysical = enum {
    int32,
    int64,
    byte_array,
    fixed_len_byte_array,
};

const DecimalMode = struct {
    physical: DecimalPhysical,
    nullable: bool = false,
    dictionary: bool = false,
    scale: i32 = 0,
    type_length: usize = 0,
};

const PlainBoolMode = enum {
    required,
    optional,
};

const PlainI64ModeRequest = union(enum) {
    fixed: PlainI64Mode,
    from_inventory,
};

fn admitMaterialization(
    raw_row_count: u64,
    projected_column_count: usize,
    limits: MaterializationLimits,
) !usize {
    try limits.validate();
    if (projected_column_count == 0) return error.InvalidParquetRowGroupBatch;
    const row_count = std.math.cast(usize, raw_row_count) orelse return error.ParquetRowGroupTooLarge;
    if (row_count > limits.max_rows) return error.ParquetRowGroupTooLarge;
    const projected_cells = std.math.mul(usize, row_count, projected_column_count) catch
        return error.ParquetRowGroupTooLarge;
    if (projected_cells > limits.max_projected_cells) return error.ParquetRowGroupTooLarge;

    var estimated_bytes = std.math.mul(usize, row_count, @sizeOf(rowsource.RowRef)) catch
        return error.ParquetRowGroupTooLarge;
    const column_metadata_bytes = std.math.mul(
        usize,
        projected_column_count,
        @sizeOf(rowsource.ColumnVector) + @sizeOf([]u8) + @sizeOf(DecodedColumn) + @sizeOf([]u8),
    ) catch return error.ParquetRowGroupTooLarge;
    estimated_bytes = std.math.add(usize, estimated_bytes, column_metadata_bytes) catch
        return error.ParquetRowGroupTooLarge;
    const projected_value_bytes = std.math.mul(
        usize,
        projected_cells,
        @max(@sizeOf(i64), @sizeOf([]u8)) + @sizeOf(u8),
    ) catch return error.ParquetRowGroupTooLarge;
    estimated_bytes = std.math.add(usize, estimated_bytes, projected_value_bytes) catch
        return error.ParquetRowGroupTooLarge;
    if (estimated_bytes > limits.max_struct_allocation_bytes) return error.ParquetRowGroupTooLarge;
    return row_count;
}

fn preflightColumnChunks(
    projected_chunks: []const ColumnChunkInput,
    expected_row_count: usize,
    limits: MaterializationLimits,
) !void {
    var input_bytes: usize = 0;
    var decoded_bytes: usize = 0;
    for (projected_chunks) |input| {
        try input.validate();
        input_bytes = std.math.add(usize, input_bytes, input.bytes.len) catch
            return error.ParquetRowGroupTooLarge;
        if (input_bytes > limits.max_input_bytes) return error.ParquetRowGroupTooLarge;
        const usage = try parquet_page.inspectColumnChunkResourceUsage(input.bytes);
        if (usage.data_value_count != expected_row_count) return error.ParquetRowGroupRowCountMismatch;
        decoded_bytes = std.math.add(usize, decoded_bytes, usage.uncompressed_bytes) catch
            return error.ParquetRowGroupTooLarge;
        if (decoded_bytes > limits.max_decoded_bytes) return error.ParquetRowGroupTooLarge;
    }
}

fn admitPhysicalReads(
    physical_reads: []const range_io.RangeRead,
    limits: MaterializationLimits,
) !void {
    if (physical_reads.len > limits.max_physical_reads) return error.ParquetRowGroupTooLarge;
    var total_bytes: usize = 0;
    for (physical_reads) |read| {
        const len = std.math.cast(usize, read.range.len) orelse return error.ParquetRowGroupTooLarge;
        total_bytes = std.math.add(usize, total_bytes, len) catch return error.ParquetRowGroupTooLarge;
        if (total_bytes > limits.max_input_bytes) return error.ParquetRowGroupTooLarge;
    }
}

fn buildPlainI64RowGroupBatchAlloc(
    alloc: Allocator,
    inventory: external_source.Inventory,
    file_id: []const u8,
    row_group_ordinal: u32,
    projected_chunks: []const ColumnChunkInput,
    mode_request: PlainI64ModeRequest,
    limits: MaterializationLimits,
) !OwnedBatch {
    try inventory.validate();
    if (projected_chunks.len == 0) return error.InvalidParquetRowGroupBatch;
    const file = inventory.fileById(file_id) orelse return error.ExternalSourceFileNotFound;
    if (row_group_ordinal >= file.row_groups.len) return error.ExternalSourceRowOutOfBounds;
    const row_group = file.row_groups[row_group_ordinal];
    const row_count = try admitMaterialization(row_group.row_count, projected_chunks.len, limits);
    try preflightColumnChunks(projected_chunks, row_count, limits);
    const binding = rowsource_bridge.bindingFromValidatedInventory(inventory);

    const row_refs = try alloc.alloc(rowsource.RowRef, row_count);
    errdefer alloc.free(row_refs);
    for (row_refs, 0..) |*row_ref, idx| {
        row_ref.* = try rowsource_external.makeRowRef(binding, file_id, row_group_ordinal, idx);
    }

    const columns = try alloc.alloc(rowsource.ColumnVector, projected_chunks.len);
    errdefer alloc.free(columns);
    const column_names = try alloc.alloc([]u8, projected_chunks.len);
    errdefer alloc.free(column_names);
    const decoded_columns = try alloc.alloc(DecodedColumn, projected_chunks.len);
    errdefer alloc.free(decoded_columns);
    const null_bitmaps = try alloc.alloc([]u8, projected_chunks.len);
    errdefer alloc.free(null_bitmaps);

    var initialized_names: usize = 0;
    errdefer {
        for (column_names[0..initialized_names]) |name| alloc.free(name);
    }
    var initialized_decoded: usize = 0;
    errdefer {
        for (decoded_columns[0..initialized_decoded]) |*column| column.deinit(alloc);
    }
    var initialized_nulls: usize = 0;
    errdefer {
        for (null_bitmaps[0..initialized_nulls]) |nulls| {
            if (nulls.len > 0) alloc.free(nulls);
        }
    }

    for (projected_chunks, 0..) |input, idx| {
        try input.validate();
        const chunk = findColumnChunk(row_group, input.column_id) orelse return error.ParquetColumnNotFound;
        const mode = switch (mode_request) {
            .fixed => |fixed| SupportedColumnMode{ .i64 = fixed },
            .from_inventory => try supportedColumnModeForColumnChunk(chunk),
        };
        const compression = try compressionCodecForColumnChunk(chunk);
        column_names[idx] = try alloc.dupe(u8, input.column_id);
        initialized_names += 1;
        switch (mode) {
            .i64 => |i64_mode| switch (i64_mode) {
                .required => {
                    decoded_columns[idx] = .{ .i64 = try parquet_page.scanPlainI64ColumnChunkAlloc(alloc, input.bytes, compression) };
                    null_bitmaps[idx] = &.{};
                },
                .optional => {
                    var decoded = try parquet_page.scanOptionalPlainI64ColumnChunkAlloc(alloc, input.bytes, compression);
                    decoded_columns[idx] = .{ .i64 = decoded.values };
                    null_bitmaps[idx] = decoded.nulls;
                    decoded = undefined;
                },
                .dictionary_required => {
                    decoded_columns[idx] = .{ .i64 = try parquet_page.scanDictionaryI64ColumnChunkAlloc(alloc, input.bytes, compression) };
                    null_bitmaps[idx] = &.{};
                },
                .dictionary_optional => {
                    var decoded = try parquet_page.scanOptionalDictionaryI64ColumnChunkAlloc(alloc, input.bytes, compression);
                    decoded_columns[idx] = .{ .i64 = decoded.values };
                    null_bitmaps[idx] = decoded.nulls;
                    decoded = undefined;
                },
                .int96_timestamp_required => {
                    decoded_columns[idx] = .{ .i64 = try parquet_page.scanPlainInt96TimestampNsColumnChunkAlloc(alloc, input.bytes, compression) };
                    null_bitmaps[idx] = &.{};
                },
                .int96_timestamp_optional => {
                    var decoded = try parquet_page.scanOptionalPlainInt96TimestampNsColumnChunkAlloc(alloc, input.bytes, compression);
                    decoded_columns[idx] = .{ .i64 = decoded.values };
                    null_bitmaps[idx] = decoded.nulls;
                    decoded = undefined;
                },
                .timestamp_millis_required, .timestamp_micros_required, .timestamp_nanos_required => {
                    const values = try parquet_page.scanPlainI64ColumnChunkAlloc(alloc, input.bytes, compression);
                    errdefer alloc.free(values);
                    try scaleTimestampNsValues(values, timestampScaleFactorForMode(i64_mode));
                    decoded_columns[idx] = .{ .i64 = values };
                    null_bitmaps[idx] = &.{};
                },
                .timestamp_millis_optional, .timestamp_micros_optional, .timestamp_nanos_optional => {
                    var decoded = try parquet_page.scanOptionalPlainI64ColumnChunkAlloc(alloc, input.bytes, compression);
                    errdefer decoded.deinit(alloc);
                    try scaleTimestampNsValues(decoded.values, timestampScaleFactorForMode(i64_mode));
                    decoded_columns[idx] = .{ .i64 = decoded.values };
                    null_bitmaps[idx] = decoded.nulls;
                    decoded = undefined;
                },
                .timestamp_millis_dictionary_required, .timestamp_micros_dictionary_required, .timestamp_nanos_dictionary_required => {
                    const values = try parquet_page.scanDictionaryI64ColumnChunkAlloc(alloc, input.bytes, compression);
                    errdefer alloc.free(values);
                    try scaleTimestampNsValues(values, timestampScaleFactorForMode(i64_mode));
                    decoded_columns[idx] = .{ .i64 = values };
                    null_bitmaps[idx] = &.{};
                },
                .timestamp_millis_dictionary_optional, .timestamp_micros_dictionary_optional, .timestamp_nanos_dictionary_optional => {
                    var decoded = try parquet_page.scanOptionalDictionaryI64ColumnChunkAlloc(alloc, input.bytes, compression);
                    errdefer decoded.deinit(alloc);
                    try scaleTimestampNsValues(decoded.values, timestampScaleFactorForMode(i64_mode));
                    decoded_columns[idx] = .{ .i64 = decoded.values };
                    null_bitmaps[idx] = decoded.nulls;
                    decoded = undefined;
                },
            },
            .i32 => |i32_mode| switch (i32_mode) {
                .required => {
                    decoded_columns[idx] = .{ .i64 = try parquet_page.scanPlainI32AsI64ColumnChunkAlloc(alloc, input.bytes, compression) };
                    null_bitmaps[idx] = &.{};
                },
                .optional => {
                    var decoded = try parquet_page.scanOptionalPlainI32AsI64ColumnChunkAlloc(alloc, input.bytes, compression);
                    decoded_columns[idx] = .{ .i64 = decoded.values };
                    null_bitmaps[idx] = decoded.nulls;
                    decoded = undefined;
                },
                .dictionary_required => {
                    decoded_columns[idx] = .{ .i64 = try parquet_page.scanDictionaryI32AsI64ColumnChunkAlloc(alloc, input.bytes, compression) };
                    null_bitmaps[idx] = &.{};
                },
                .dictionary_optional => {
                    var decoded = try parquet_page.scanOptionalDictionaryI32AsI64ColumnChunkAlloc(alloc, input.bytes, compression);
                    decoded_columns[idx] = .{ .i64 = decoded.values };
                    null_bitmaps[idx] = decoded.nulls;
                    decoded = undefined;
                },
            },
            .f64 => |f64_mode| switch (f64_mode) {
                .required => {
                    decoded_columns[idx] = .{ .f64 = try parquet_page.scanPlainF64ColumnChunkAlloc(alloc, input.bytes, compression) };
                    null_bitmaps[idx] = &.{};
                },
                .optional => {
                    var decoded = try parquet_page.scanOptionalPlainF64ColumnChunkAlloc(alloc, input.bytes, compression);
                    decoded_columns[idx] = .{ .f64 = decoded.values };
                    null_bitmaps[idx] = decoded.nulls;
                    decoded = undefined;
                },
                .dictionary_required => {
                    decoded_columns[idx] = .{ .f64 = try parquet_page.scanDictionaryF64ColumnChunkAlloc(alloc, input.bytes, compression) };
                    null_bitmaps[idx] = &.{};
                },
                .dictionary_optional => {
                    var decoded = try parquet_page.scanOptionalDictionaryF64ColumnChunkAlloc(alloc, input.bytes, compression);
                    decoded_columns[idx] = .{ .f64 = decoded.values };
                    null_bitmaps[idx] = decoded.nulls;
                    decoded = undefined;
                },
                .float_required => {
                    decoded_columns[idx] = .{ .f64 = try parquet_page.scanPlainF32AsF64ColumnChunkAlloc(alloc, input.bytes, compression) };
                    null_bitmaps[idx] = &.{};
                },
                .float_optional => {
                    var decoded = try parquet_page.scanOptionalPlainF32AsF64ColumnChunkAlloc(alloc, input.bytes, compression);
                    decoded_columns[idx] = .{ .f64 = decoded.values };
                    null_bitmaps[idx] = decoded.nulls;
                    decoded = undefined;
                },
                .float_dictionary_required => {
                    decoded_columns[idx] = .{ .f64 = try parquet_page.scanDictionaryF32AsF64ColumnChunkAlloc(alloc, input.bytes, compression) };
                    null_bitmaps[idx] = &.{};
                },
                .float_dictionary_optional => {
                    var decoded = try parquet_page.scanOptionalDictionaryF32AsF64ColumnChunkAlloc(alloc, input.bytes, compression);
                    decoded_columns[idx] = .{ .f64 = decoded.values };
                    null_bitmaps[idx] = decoded.nulls;
                    decoded = undefined;
                },
            },
            .decimal => |decimal_mode| {
                var decoded = try scanDecimalAsF64ColumnChunkAlloc(alloc, input.bytes, compression, decimal_mode);
                decoded_columns[idx] = .{ .f64 = decoded.values };
                null_bitmaps[idx] = decoded.nulls;
                decoded = undefined;
            },
            .bool => |bool_mode| switch (bool_mode) {
                .required => {
                    decoded_columns[idx] = .{ .bool = try parquet_page.scanPlainBoolColumnChunkAlloc(alloc, input.bytes, compression) };
                    null_bitmaps[idx] = &.{};
                },
                .optional => {
                    var decoded = try parquet_page.scanOptionalPlainBoolColumnChunkAlloc(alloc, input.bytes, compression);
                    decoded_columns[idx] = .{ .bool = decoded.values };
                    null_bitmaps[idx] = decoded.nulls;
                    decoded = undefined;
                },
            },
            .bytes => |byte_mode| switch (byte_mode) {
                .required => {
                    decoded_columns[idx] = .{ .bytes = try parquet_page.scanPlainByteArrayColumnChunkAlloc(alloc, input.bytes, compression) };
                    null_bitmaps[idx] = &.{};
                },
                .optional => {
                    var decoded = try parquet_page.scanOptionalPlainByteArrayColumnChunkAlloc(alloc, input.bytes, compression);
                    decoded_columns[idx] = .{ .bytes = decoded.values };
                    null_bitmaps[idx] = decoded.nulls;
                    decoded = undefined;
                },
                .dictionary_required => {
                    decoded_columns[idx] = .{ .bytes = try parquet_page.scanDictionaryByteArrayColumnChunkAlloc(alloc, input.bytes, compression) };
                    null_bitmaps[idx] = &.{};
                },
                .dictionary_optional => {
                    var decoded = try parquet_page.scanOptionalDictionaryByteArrayColumnChunkAlloc(alloc, input.bytes, compression);
                    decoded_columns[idx] = .{ .bytes = decoded.values };
                    null_bitmaps[idx] = decoded.nulls;
                    decoded = undefined;
                },
            },
        }
        initialized_decoded += 1;
        initialized_nulls += 1;
        columns[idx] = switch (decoded_columns[idx]) {
            .i64 => |values| blk: {
                if (values.len != row_count) return error.ParquetRowGroupRowCountMismatch;
                break :blk .{
                    .name = column_names[idx],
                    .values = .{ .i64 = values },
                    .nulls = .{ .bytes = null_bitmaps[idx] },
                };
            },
            .f64 => |values| blk: {
                if (values.len != row_count) return error.ParquetRowGroupRowCountMismatch;
                break :blk .{
                    .name = column_names[idx],
                    .values = .{ .f64 = values },
                    .nulls = .{ .bytes = null_bitmaps[idx] },
                };
            },
            .bool => |values| blk: {
                if (values.len != row_count) return error.ParquetRowGroupRowCountMismatch;
                break :blk .{
                    .name = column_names[idx],
                    .values = .{ .bool = values },
                    .nulls = .{ .bytes = null_bitmaps[idx] },
                };
            },
            .bytes => |values| blk: {
                if (values.len != row_count) return error.ParquetRowGroupRowCountMismatch;
                break :blk .{
                    .name = column_names[idx],
                    .values = .{ .bytes = values },
                    .nulls = .{ .bytes = null_bitmaps[idx] },
                };
            },
        };
    }

    const batch = rowsource.ColumnBatch{
        .snapshot = binding.snapshot(),
        .row_refs = row_refs,
        .columns = columns,
    };
    try rowsource_bridge.validateBatchAgainstInventoryAlloc(alloc, inventory, batch);

    return .{
        .batch = batch,
        .row_refs = row_refs,
        .columns = columns,
        .column_names = column_names,
        .decoded_columns = decoded_columns,
        .null_bitmaps = null_bitmaps,
    };
}

pub fn buildRequiredPlainI64RowGroupBatchFromObjectRangeReaderAlloc(
    alloc: Allocator,
    reader: ObjectRangeReader,
    inventory: external_source.Inventory,
    file_id: []const u8,
    row_group_ordinal: u32,
    projected_columns: []const []const u8,
) !OwnedBatch {
    return try buildPlainI64RowGroupBatchFromObjectRangeReaderAlloc(
        alloc,
        reader,
        null,
        inventory,
        file_id,
        row_group_ordinal,
        projected_columns,
        .{ .fixed = .required },
        .{},
        .{},
    );
}

pub fn buildSupportedI64RowGroupBatchFromObjectRangeReaderAlloc(
    alloc: Allocator,
    reader: ObjectRangeReader,
    inventory: external_source.Inventory,
    file_id: []const u8,
    row_group_ordinal: u32,
    projected_columns: []const []const u8,
) !OwnedBatch {
    return try buildSupportedI64RowGroupBatchFromObjectRangeReaderAllocWithLimits(
        alloc,
        reader,
        inventory,
        file_id,
        row_group_ordinal,
        projected_columns,
        .{},
    );
}

pub fn buildSupportedI64RowGroupBatchFromObjectRangeReaderAllocWithLimits(
    alloc: Allocator,
    reader: ObjectRangeReader,
    inventory: external_source.Inventory,
    file_id: []const u8,
    row_group_ordinal: u32,
    projected_columns: []const []const u8,
    limits: MaterializationLimits,
) !OwnedBatch {
    return try buildPlainI64RowGroupBatchFromObjectRangeReaderAlloc(
        alloc,
        reader,
        null,
        inventory,
        file_id,
        row_group_ordinal,
        projected_columns,
        .from_inventory,
        .{},
        limits,
    );
}

pub fn buildSupportedI64RowGroupBatchFromCachedObjectRangeReaderAlloc(
    alloc: Allocator,
    reader: ObjectRangeReader,
    cache: *ObjectRangeCache,
    inventory: external_source.Inventory,
    file_id: []const u8,
    row_group_ordinal: u32,
    projected_columns: []const []const u8,
) !OwnedBatch {
    return try buildSupportedI64RowGroupBatchFromCachedCoalescedObjectRangeReaderAlloc(
        alloc,
        reader,
        cache,
        inventory,
        file_id,
        row_group_ordinal,
        projected_columns,
        .{},
    );
}

pub fn buildSupportedI64RowGroupBatchFromCoalescedObjectRangeReaderAlloc(
    alloc: Allocator,
    reader: ObjectRangeReader,
    inventory: external_source.Inventory,
    file_id: []const u8,
    row_group_ordinal: u32,
    projected_columns: []const []const u8,
    coalesce_options: range_io.CoalesceOptions,
) !OwnedBatch {
    return try buildSupportedI64RowGroupBatchFromMaybeCachedCoalescedObjectRangeReaderAlloc(
        alloc,
        reader,
        null,
        inventory,
        file_id,
        row_group_ordinal,
        projected_columns,
        coalesce_options,
        .{},
    );
}

pub fn buildSupportedI64RowGroupBatchFromCachedCoalescedObjectRangeReaderAlloc(
    alloc: Allocator,
    reader: ObjectRangeReader,
    cache: *ObjectRangeCache,
    inventory: external_source.Inventory,
    file_id: []const u8,
    row_group_ordinal: u32,
    projected_columns: []const []const u8,
    coalesce_options: range_io.CoalesceOptions,
) !OwnedBatch {
    return try buildSupportedI64RowGroupBatchFromMaybeCachedCoalescedObjectRangeReaderAlloc(
        alloc,
        reader,
        cache,
        inventory,
        file_id,
        row_group_ordinal,
        projected_columns,
        coalesce_options,
        .{},
    );
}

fn buildSupportedI64RowGroupBatchFromMaybeCachedCoalescedObjectRangeReaderAlloc(
    alloc: Allocator,
    reader: ObjectRangeReader,
    cache: ?*ObjectRangeCache,
    inventory: external_source.Inventory,
    file_id: []const u8,
    row_group_ordinal: u32,
    projected_columns: []const []const u8,
    coalesce_options: range_io.CoalesceOptions,
    limits: MaterializationLimits,
) !OwnedBatch {
    return try buildPlainI64RowGroupBatchFromObjectRangeReaderAlloc(
        alloc,
        reader,
        cache,
        inventory,
        file_id,
        row_group_ordinal,
        projected_columns,
        .from_inventory,
        coalesce_options,
        limits,
    );
}

fn buildPlainI64RowGroupBatchFromObjectRangeReaderAlloc(
    alloc: Allocator,
    reader: ObjectRangeReader,
    cache: ?*ObjectRangeCache,
    inventory: external_source.Inventory,
    file_id: []const u8,
    row_group_ordinal: u32,
    projected_columns: []const []const u8,
    mode_request: PlainI64ModeRequest,
    coalesce_options: range_io.CoalesceOptions,
    limits: MaterializationLimits,
) !OwnedBatch {
    try inventory.validate();
    if (projected_columns.len == 0) return error.InvalidParquetRowGroupBatch;
    const file = inventory.fileById(file_id) orelse return error.ExternalSourceFileNotFound;
    if (row_group_ordinal >= file.row_groups.len) return error.ExternalSourceRowOutOfBounds;
    const row_group = file.row_groups[row_group_ordinal];
    _ = try admitMaterialization(row_group.row_count, projected_columns.len, limits);
    const object = try range_io.objectRefForExternalFileUri(file);

    const chunk_inputs = try alloc.alloc(ColumnChunkInput, projected_columns.len);
    errdefer alloc.free(chunk_inputs);
    const logical_reads = try alloc.alloc(range_io.RangeRead, projected_columns.len);
    defer alloc.free(logical_reads);

    for (projected_columns, 0..) |column_id, idx| {
        if (column_id.len == 0) return error.InvalidParquetRowGroupBatch;
        const chunk = findColumnChunk(row_group, column_id) orelse return error.ParquetColumnNotFound;
        logical_reads[idx] = try range_io.planColumnChunkRead(object, chunk);
        chunk_inputs[idx] = .{
            .column_id = column_id,
            .bytes = &.{},
        };
    }

    const physical_reads = try range_io.coalescePhysicalReadsAlloc(alloc, logical_reads, coalesce_options);
    defer alloc.free(physical_reads);
    try admitPhysicalReads(physical_reads, limits);
    const physical_bytes = try alloc.alloc([]u8, physical_reads.len);
    defer alloc.free(physical_bytes);
    var initialized_physical: usize = 0;
    errdefer {
        for (physical_bytes[0..initialized_physical]) |bytes| alloc.free(bytes);
    }
    for (physical_reads, 0..) |read, idx| {
        physical_bytes[idx] = try readMaybeCachedObjectRangeAlloc(alloc, reader, cache, read);
        initialized_physical += 1;
    }
    for (logical_reads, 0..) |logical, idx| {
        chunk_inputs[idx].bytes = try logicalReadSlice(logical, physical_reads, physical_bytes);
    }

    var owned = try buildPlainI64RowGroupBatchAlloc(
        alloc,
        inventory,
        file_id,
        row_group_ordinal,
        chunk_inputs,
        mode_request,
        limits,
    );
    errdefer owned.deinit(alloc);

    for (physical_bytes[0..initialized_physical]) |bytes| alloc.free(bytes);
    alloc.free(chunk_inputs);
    return owned;
}

fn logicalReadSlice(logical: range_io.RangeRead, physical_reads: []const range_io.RangeRead, physical_bytes: []const []u8) ![]const u8 {
    for (physical_reads, physical_bytes) |physical, bytes| {
        if (!readContains(physical, logical)) continue;
        const start: usize = std.math.cast(usize, logical.range.offset - physical.range.offset) orelse return error.InvalidLakeRangeRead;
        const len: usize = std.math.cast(usize, logical.range.len) orelse return error.InvalidLakeRangeRead;
        if (start > bytes.len or len > bytes.len - start) return error.InvalidLakeRangeRead;
        return bytes[start..][0..len];
    }
    return error.InvalidLakeRangeRead;
}

fn readContains(physical: range_io.RangeRead, logical: range_io.RangeRead) bool {
    if (physical.purpose != logical.purpose) return false;
    if (!std.mem.eql(u8, physical.object.bucket, logical.object.bucket)) return false;
    if (!std.mem.eql(u8, physical.object.key, logical.object.key)) return false;
    if (physical.object.byte_len != logical.object.byte_len) return false;
    if (!std.mem.eql(u8, physical.object.version.etag, logical.object.version.etag)) return false;
    if (!std.mem.eql(u8, physical.object.version.version_id, logical.object.version.version_id)) return false;
    return logical.range.offset >= physical.range.offset and logical.range.end() <= physical.range.end();
}

pub fn planRequiredPlainI64ObjectRangeRowGroupsAlloc(
    alloc: Allocator,
    inventory: external_source.Inventory,
    projected_columns: []const []const u8,
) !ObjectRangeRowGroupPlan {
    return try planObjectRangeRowGroupsAlloc(alloc, inventory, projected_columns, .none);
}

pub fn planSupportedI64ObjectRangeRowGroupsAlloc(
    alloc: Allocator,
    inventory: external_source.Inventory,
    projected_columns: []const []const u8,
) !ObjectRangeRowGroupPlan {
    return try planObjectRangeRowGroupsAlloc(alloc, inventory, projected_columns, .supported_i64);
}

fn planSupportedI64ObjectRangeRowGroupsForQueryAlloc(
    alloc: Allocator,
    inventory: external_source.Inventory,
    projected_columns: []const []const u8,
    predicate: ?lake_rows.Predicate,
) !ObjectRangeRowGroupPlan {
    return try planObjectRangeRowGroupsForPredicateAlloc(alloc, inventory, projected_columns, .supported_i64, predicate);
}

pub fn querySupportedI64ObjectRangeRowsAlloc(
    alloc: Allocator,
    request: ObjectRangeRowsQueryRequest,
) !lake_rows.ScanResult {
    const maybe_binding = request.binding;
    if (maybe_binding) |binding| {
        try lake_scan_plan.validateBindingInventory(binding, request.inventory);
    } else {
        try request.inventory.validate();
        if (request.inventory.format != .parquet) return error.InvalidParquetRowGroupBatch;
        if (request.sidecars.len != 0 or request.desired_sidecars.len != 0 or request.candidates.len != 0) {
            return error.InvalidLakeSidecarSelection;
        }
    }

    const scan_columns = try rowsQueryScanColumnsAlloc(alloc, request.projected_columns, request.predicate);
    defer alloc.free(scan_columns);

    var row_group_plan = try planSupportedI64ObjectRangeRowGroupsForQueryAlloc(alloc, request.inventory, scan_columns, request.predicate);
    defer row_group_plan.deinit(alloc);

    if (row_group_plan.row_groups.len == 0) {
        return .{
            .rows = try alloc.alloc(lake_rows.ProjectedRow, 0),
            .total = 0,
        };
    }

    var source = if (request.cache) |cache|
        try ObjectRangeRowGroupSource.initWithCacheAndCoalesceOptions(
            request.reader,
            cache,
            request.inventory,
            row_group_plan.row_groups,
            request.coalesce_options,
        )
    else
        try ObjectRangeRowGroupSource.initWithCoalesceOptions(
            request.reader,
            request.inventory,
            row_group_plan.row_groups,
            request.coalesce_options,
        );
    defer source.deinit(alloc);
    source.materialization_limits = request.materialization_limits;

    const scan_request: lake_rows.ScanRequest = .{
        .projected_columns = request.projected_columns,
        .predicate = request.predicate,
        .limit = request.limit,
        .deleted_row_refs = request.deleted_row_refs,
        .deleted_row_filter = inventoryDeletedRowFilter(&request.inventory),
        .limits = request.scan_limits,
    };
    if (maybe_binding) |binding| {
        const base_source = try binding.toManifestBaseSource(request.inventory.snapshot_id, null);
        var sidecar_result = try lake_rows.scanRowsWithAutomaticSidecarsAlloc(alloc, source.rowSource(), .{
            .scan = scan_request,
            .base_source = base_source,
            .sidecars = request.sidecars,
            .desired_sidecars = request.desired_sidecars,
            .sidecar_policy = request.sidecar_policy,
            .candidates = request.candidates,
        });
        errdefer sidecar_result.deinit(alloc);
        return .{
            .rows = sidecar_result.rows,
            .total = sidecar_result.total,
        };
    }

    return try lake_rows.scanRowsAlloc(alloc, source.rowSource(), scan_request);
}

pub fn executeSupportedI64ObjectRangeExpressionAggregatesAlloc(
    alloc: Allocator,
    request: ObjectRangeExpressionAggregateRequest,
) !lake_rows.ExpressionAggregateResult {
    const maybe_binding = request.binding;
    if (maybe_binding) |binding| {
        try lake_scan_plan.validateBindingInventory(binding, request.inventory);
    } else {
        try request.inventory.validate();
        if (request.inventory.format != .parquet) return error.InvalidParquetRowGroupBatch;
    }

    const scan_columns = try expressionAggregateScanColumnsAlloc(alloc, request.aggregate.expressions);
    defer alloc.free(scan_columns);
    if (scan_columns.len == 0) return error.UnsupportedLakeRowsExpressionAggregate;

    var row_group_plan = try planSupportedI64ObjectRangeRowGroupsAlloc(alloc, request.inventory, scan_columns);
    defer row_group_plan.deinit(alloc);

    if (row_group_plan.row_groups.len == 0) {
        const EmptySource = struct {
            fn next(_: *anyopaque, _: Allocator) !?rowsource.ColumnBatch {
                return null;
            }
        };
        var dummy: u8 = 0;
        const source = rowsource.Source{
            .kind = sourceKindForInventoryFormat(request.inventory.format),
            .ctx = &dummy,
            .next_batch = EmptySource.next,
        };
        var aggregate = request.aggregate;
        aggregate.deleted_row_filter = inventoryDeletedRowFilter(&request.inventory);
        return try lake_rows.executeExpressionAggregatesAlloc(alloc, source, aggregate, null);
    }

    var source = if (request.cache) |cache|
        try ObjectRangeRowGroupSource.initWithCacheAndCoalesceOptions(
            request.reader,
            cache,
            request.inventory,
            row_group_plan.row_groups,
            request.coalesce_options,
        )
    else
        try ObjectRangeRowGroupSource.initWithCoalesceOptions(
            request.reader,
            request.inventory,
            row_group_plan.row_groups,
            request.coalesce_options,
        );
    defer source.deinit(alloc);

    source.materialization_limits = request.materialization_limits;
    var aggregate = request.aggregate;
    aggregate.deleted_row_filter = inventoryDeletedRowFilter(&request.inventory);
    return try lake_rows.executeExpressionAggregatesAlloc(alloc, source.rowSource(), aggregate, null);
}

fn inventoryDeletedRowFilter(inventory: *const external_source.Inventory) ?lake_rows.DeletedRowFilter {
    if (inventory.deleted_row_groups.len == 0) return null;
    return .{ .ctx = inventory, .contains_fn = inventoryContainsDeletedRow };
}

fn inventoryContainsDeletedRow(ctx: *const anyopaque, row_ref: rowsource.RowRef) bool {
    const inventory: *const external_source.Inventory = @ptrCast(@alignCast(ctx));
    const external = switch (row_ref) {
        .external => |value| value,
        else => return false,
    };
    if (!std.mem.eql(u8, external.source_id, inventory.source_id) or
        !std.mem.eql(u8, external.snapshot_id, inventory.snapshot_id)) return false;
    return inventory.isDeleted(external.file_id, external.row_group_ordinal, external.row_ordinal);
}

fn rowsQueryScanColumnsAlloc(
    alloc: Allocator,
    projected_columns: []const []const u8,
    predicate: ?lake_rows.Predicate,
) ![][]const u8 {
    if (projected_columns.len == 0) return error.InvalidLakeRowsQuery;
    var needs_predicate_column = false;
    if (predicate) |pred| {
        if (pred.column.len == 0) return error.InvalidLakeRowsQuery;
        needs_predicate_column = !containsColumn(projected_columns, pred.column);
    }

    const len = projected_columns.len + @intFromBool(needs_predicate_column);
    const scan_columns = try alloc.alloc([]const u8, len);
    errdefer alloc.free(scan_columns);
    for (projected_columns, 0..) |column, idx| {
        if (column.len == 0) return error.InvalidLakeRowsQuery;
        scan_columns[idx] = column;
    }
    if (needs_predicate_column) {
        scan_columns[projected_columns.len] = predicate.?.column;
    }
    return scan_columns;
}

fn expressionAggregateScanColumnsAlloc(
    alloc: Allocator,
    expressions: []const algebraic_segment.ExpressionSpec,
) ![][]const u8 {
    var columns = std.ArrayListUnmanaged([]const u8).empty;
    errdefer columns.deinit(alloc);
    for (expressions) |expression| {
        if (expression.op == .count) continue;
        if (expression.value_column.len == 0) return error.InvalidLakeRowsQuery;
        if (!containsColumn(columns.items, expression.value_column)) {
            try columns.append(alloc, expression.value_column);
        }
    }
    return try columns.toOwnedSlice(alloc);
}

fn sourceKindForInventoryFormat(format: external_source.Format) rowsource.SourceKind {
    return switch (format) {
        .parquet => .external_parquet,
        .iceberg => .external_iceberg,
        .lance => .external_lance,
    };
}

fn containsColumn(columns: []const []const u8, column: []const u8) bool {
    for (columns) |candidate| {
        if (std.mem.eql(u8, candidate, column)) return true;
    }
    return false;
}

pub fn discoverSupportedI64ObjectRangeRowGroupsFromFootersAlloc(
    alloc: Allocator,
    reader: ObjectRangeReader,
    raw_inventory: external_source.Inventory,
    projected_columns: []const []const u8,
    footer_probe_bytes: u64,
) !DiscoveredObjectRangeRowGroupPlan {
    return try discoverSupportedI64ObjectRangeRowGroupsFromMaybeCachedFootersAlloc(
        alloc,
        reader,
        null,
        raw_inventory,
        projected_columns,
        footer_probe_bytes,
    );
}

pub fn discoverSupportedI64ObjectRangeRowGroupsFromCachedFootersAlloc(
    alloc: Allocator,
    reader: ObjectRangeReader,
    cache: *ObjectRangeCache,
    raw_inventory: external_source.Inventory,
    projected_columns: []const []const u8,
    footer_probe_bytes: u64,
) !DiscoveredObjectRangeRowGroupPlan {
    return try discoverSupportedI64ObjectRangeRowGroupsFromMaybeCachedFootersAlloc(
        alloc,
        reader,
        cache,
        raw_inventory,
        projected_columns,
        footer_probe_bytes,
    );
}

fn discoverSupportedI64ObjectRangeRowGroupsFromMaybeCachedFootersAlloc(
    alloc: Allocator,
    reader: ObjectRangeReader,
    cache: ?*ObjectRangeCache,
    raw_inventory: external_source.Inventory,
    projected_columns: []const []const u8,
    footer_probe_bytes: u64,
) !DiscoveredObjectRangeRowGroupPlan {
    try raw_inventory.validate();
    if (raw_inventory.format != .parquet and raw_inventory.format != .iceberg) return error.InvalidParquetRowGroupBatch;
    if (raw_inventory.files.len == 0) return error.InvalidParquetRowGroupBatch;
    if (footer_probe_bytes == 0) return error.InvalidLakeRangeRead;

    const footers = try alloc.alloc(parquet_metadata.FileFooter, raw_inventory.files.len);
    errdefer alloc.free(footers);
    var initialized_footers: usize = 0;
    errdefer {
        for (footers[0..initialized_footers]) |*entry| entry.footer.deinit(alloc);
    }

    for (raw_inventory.files, 0..) |file, idx| {
        const object = try range_io.objectRefForExternalFileUri(file);
        const tail_read = try range_io.planParquetFooterRead(object, footer_probe_bytes);
        const tail = try readMaybeCachedObjectRangeAlloc(alloc, reader, cache, tail_read);
        defer alloc.free(tail);

        const preflight = try parquet_footer.parseFooterPreflight(object.byte_len, tail_read.range.offset, tail);
        const metadata_bytes = if (preflight.metadataSlice(tail)) |slice|
            try alloc.dupe(u8, slice)
        else blk: {
            const read = try parquet_footer.planFooterMetadataRead(object, tail_read.range.offset, tail);
            break :blk try readMaybeCachedObjectRangeAlloc(alloc, reader, cache, read);
        };
        defer alloc.free(metadata_bytes);

        footers[idx] = .{
            .file_id = file.file_id,
            .footer = try parquet_metadata.parseFooterMetadataAlloc(alloc, metadata_bytes, file.byte_len),
        };
        initialized_footers += 1;
    }

    var enriched = try parquet_metadata.enrichInventoryFilesWithFootersAlloc(alloc, raw_inventory, footers);
    errdefer enriched.deinit(alloc);
    var row_group_plan = try planSupportedI64ObjectRangeRowGroupsAlloc(alloc, enriched, projected_columns);
    errdefer row_group_plan.deinit(alloc);

    for (footers[0..initialized_footers]) |*entry| entry.footer.deinit(alloc);
    alloc.free(footers);

    return .{
        .inventory = enriched,
        .row_group_plan = row_group_plan,
    };
}

fn readMaybeCachedObjectRangeAlloc(
    alloc: Allocator,
    reader: ObjectRangeReader,
    cache: ?*ObjectRangeCache,
    read: range_io.RangeRead,
) ![]u8 {
    if (cache) |range_cache| {
        return try range_cache.readAlloc(alloc, reader, read);
    }
    return try readObjectRangeAlloc(alloc, reader, read);
}

fn readObjectRangeAlloc(alloc: Allocator, reader: ObjectRangeReader, read: range_io.RangeRead) ![]u8 {
    return try reader.readPlannedAlloc(alloc, read);
}

const RowGroupPlanValidation = enum {
    none,
    supported_i64,
};

fn planObjectRangeRowGroupsAlloc(
    alloc: Allocator,
    inventory: external_source.Inventory,
    projected_columns: []const []const u8,
    validation: RowGroupPlanValidation,
) !ObjectRangeRowGroupPlan {
    return try planObjectRangeRowGroupsForPredicateAlloc(alloc, inventory, projected_columns, validation, null);
}

fn planObjectRangeRowGroupsForPredicateAlloc(
    alloc: Allocator,
    inventory: external_source.Inventory,
    projected_columns: []const []const u8,
    validation: RowGroupPlanValidation,
    predicate: ?lake_rows.Predicate,
) !ObjectRangeRowGroupPlan {
    try inventory.validate();
    if (inventory.format != .parquet and inventory.format != .iceberg) return error.InvalidParquetRowGroupBatch;
    if (projected_columns.len == 0) return error.InvalidParquetRowGroupBatch;
    for (projected_columns) |column| {
        if (column.len == 0) return error.InvalidParquetRowGroupBatch;
    }

    var total_row_groups: usize = 0;
    for (inventory.files) |file| {
        if (!fileMayMatchPredicate(file, predicate)) continue;
        for (file.row_groups) |row_group| {
            for (projected_columns) |column| {
                const chunk = findColumnChunk(row_group, column) orelse return error.ParquetColumnNotFound;
                try validatePlannedChunk(inventory.format, chunk, validation);
            }
            if (!rowGroupMayMatchPredicate(row_group, predicate)) continue;
            total_row_groups += 1;
        }
    }
    if (total_row_groups == 0 and predicate == null) return error.InvalidParquetRowGroupBatch;

    const row_groups = try alloc.alloc(ObjectRangeRowGroupInput, total_row_groups);
    errdefer alloc.free(row_groups);
    var out_idx: usize = 0;
    for (inventory.files) |file| {
        if (!fileMayMatchPredicate(file, predicate)) continue;
        for (file.row_groups) |row_group| {
            if (!rowGroupMayMatchPredicate(row_group, predicate)) continue;
            row_groups[out_idx] = .{
                .file_id = file.file_id,
                .row_group_ordinal = row_group.ordinal,
                .projected_columns = projected_columns,
            };
            out_idx += 1;
        }
    }

    return .{ .row_groups = row_groups };
}

fn fileMayMatchPredicate(file: external_source.FileEntry, predicate: ?lake_rows.Predicate) bool {
    const pred = predicate orelse return true;
    const partition = findPartitionValue(file, pred.column) orelse return true;
    return partitionMayMatchPredicate(partition, pred);
}

fn partitionMayMatchPredicate(partition: external_source.PartitionValue, predicate: lake_rows.Predicate) bool {
    return switch (predicate.op) {
        .eq_bytes => std.mem.eql(u8, partition.string_value, predicate.bytes_value),
        .eq_i64 => {
            const value = std.fmt.parseInt(i64, partition.string_value, 10) catch return true;
            return value == predicate.i64_value;
        },
        .eq_f64 => {
            const value = std.fmt.parseFloat(f64, partition.string_value) catch return true;
            return value == predicate.f64_value;
        },
        .eq_bool => {
            if (std.mem.eql(u8, partition.string_value, "true")) return predicate.bool_value;
            if (std.mem.eql(u8, partition.string_value, "false")) return !predicate.bool_value;
            return true;
        },
    };
}

fn rowGroupMayMatchPredicate(row_group: external_source.RowGroup, predicate: ?lake_rows.Predicate) bool {
    const pred = predicate orelse return true;
    const chunk = findColumnChunk(row_group, pred.column) orelse return true;
    return switch (pred.op) {
        .eq_i64 => blk: {
            if (chunk.stats_min_i64) |min| {
                const max = chunk.stats_max_i64 orelse return true;
                break :blk pred.i64_value >= min and pred.i64_value <= max;
            }
            const min = chunk.stats_min_f64 orelse return true;
            const max = chunk.stats_max_f64 orelse return true;
            const value: f64 = @floatFromInt(pred.i64_value);
            break :blk value >= min and value <= max;
        },
        .eq_f64 => blk: {
            if (chunk.stats_min_f64) |min| {
                const max = chunk.stats_max_f64 orelse return true;
                break :blk pred.f64_value >= min and pred.f64_value <= max;
            }
            const value = exactI64FromF64(pred.f64_value) orelse break :blk false;
            const min = chunk.stats_min_i64 orelse return true;
            const max = chunk.stats_max_i64 orelse return true;
            break :blk value >= min and value <= max;
        },
        .eq_bytes => blk: {
            const min = chunk.stats_min_bytes orelse return true;
            const max = chunk.stats_max_bytes orelse return true;
            break :blk std.mem.order(u8, pred.bytes_value, min) != .lt and std.mem.order(u8, pred.bytes_value, max) != .gt;
        },
        .eq_bool => blk: {
            const min = chunk.stats_min_bool orelse return true;
            const max = chunk.stats_max_bool orelse return true;
            if (min == max) break :blk pred.bool_value == min;
            break :blk true;
        },
    };
}

fn exactI64FromF64(value: f64) ?i64 {
    if (!std.math.isFinite(value)) return null;
    if (@trunc(value) != value) return null;
    if (value < @as(f64, @floatFromInt(std.math.minInt(i64)))) return null;
    if (value > @as(f64, @floatFromInt(std.math.maxInt(i64)))) return null;
    const as_i64: i64 = @intFromFloat(value);
    if (@as(f64, @floatFromInt(as_i64)) != value) return null;
    return as_i64;
}

fn findPartitionValue(file: external_source.FileEntry, column_id: []const u8) ?external_source.PartitionValue {
    for (file.partition_values) |partition| {
        if (std.mem.eql(u8, partition.column_id, column_id)) return partition;
    }
    return null;
}

fn validatePlannedChunk(format: external_source.Format, chunk: external_source.ColumnChunk, validation: RowGroupPlanValidation) !void {
    if (format == .iceberg and chunk.field_id == null) return error.UnsupportedIcebergSchemaEvolution;
    switch (validation) {
        .none => {},
        .supported_i64 => {
            _ = try supportedColumnModeForColumnChunk(chunk);
            _ = try compressionCodecForColumnChunk(chunk);
        },
    }
}

fn findColumnChunk(row_group: external_source.RowGroup, column_id: []const u8) ?external_source.ColumnChunk {
    for (row_group.column_chunks) |chunk| {
        if (std.mem.eql(u8, chunk.column_id, column_id)) return chunk;
    }
    return null;
}

fn plainI64ModeForColumnChunk(chunk: external_source.ColumnChunk) !PlainI64Mode {
    if (chunk.physical_type.len != 0 and !std.ascii.eqlIgnoreCase(chunk.physical_type, "int64")) return error.UnsupportedParquetPage;
    if (chunk.encoding.len == 0 or std.ascii.eqlIgnoreCase(chunk.encoding, "plain")) return if (chunk.nullable) .optional else .required;
    if (std.ascii.eqlIgnoreCase(chunk.encoding, "rle_dictionary") or
        std.ascii.eqlIgnoreCase(chunk.encoding, "plain_dictionary"))
    {
        return if (chunk.nullable) .dictionary_optional else .dictionary_required;
    }
    return error.UnsupportedParquetPage;
}

fn timestampI64ModeForColumnChunk(chunk: external_source.ColumnChunk, unit: TimestampUnit) !PlainI64Mode {
    if (chunk.physical_type.len != 0 and !std.ascii.eqlIgnoreCase(chunk.physical_type, "int64")) return error.UnsupportedParquetPage;
    if (chunk.encoding.len == 0 or std.ascii.eqlIgnoreCase(chunk.encoding, "plain")) {
        return switch (unit) {
            .millis => if (chunk.nullable) .timestamp_millis_optional else .timestamp_millis_required,
            .micros => if (chunk.nullable) .timestamp_micros_optional else .timestamp_micros_required,
            .nanos => if (chunk.nullable) .timestamp_nanos_optional else .timestamp_nanos_required,
        };
    }
    if (std.ascii.eqlIgnoreCase(chunk.encoding, "rle_dictionary") or
        std.ascii.eqlIgnoreCase(chunk.encoding, "plain_dictionary"))
    {
        return switch (unit) {
            .millis => if (chunk.nullable) .timestamp_millis_dictionary_optional else .timestamp_millis_dictionary_required,
            .micros => if (chunk.nullable) .timestamp_micros_dictionary_optional else .timestamp_micros_dictionary_required,
            .nanos => if (chunk.nullable) .timestamp_nanos_dictionary_optional else .timestamp_nanos_dictionary_required,
        };
    }
    return error.UnsupportedParquetPage;
}

fn supportedColumnModeForColumnChunk(chunk: external_source.ColumnChunk) !SupportedColumnMode {
    if (std.ascii.eqlIgnoreCase(chunk.logical_type, "decimal")) {
        return .{ .decimal = try decimalModeForColumnChunk(chunk) };
    }
    if (chunk.physical_type.len == 0 or std.ascii.eqlIgnoreCase(chunk.physical_type, "int64")) {
        if (timestampUnitForLogicalType(chunk.logical_type)) |unit| {
            return .{ .i64 = try timestampI64ModeForColumnChunk(chunk, unit) };
        }
        return .{ .i64 = try plainI64ModeForColumnChunk(chunk) };
    }
    if (std.ascii.eqlIgnoreCase(chunk.physical_type, "int32")) {
        if (chunk.encoding.len == 0 or std.ascii.eqlIgnoreCase(chunk.encoding, "plain")) return .{ .i32 = if (chunk.nullable) .optional else .required };
        if (std.ascii.eqlIgnoreCase(chunk.encoding, "rle_dictionary") or
            std.ascii.eqlIgnoreCase(chunk.encoding, "plain_dictionary"))
        {
            return .{ .i32 = if (chunk.nullable) .dictionary_optional else .dictionary_required };
        }
    }
    if (std.ascii.eqlIgnoreCase(chunk.physical_type, "int96")) {
        if (chunk.encoding.len == 0 or std.ascii.eqlIgnoreCase(chunk.encoding, "plain")) return .{ .i64 = if (chunk.nullable) .int96_timestamp_optional else .int96_timestamp_required };
    }
    if (std.ascii.eqlIgnoreCase(chunk.physical_type, "double")) {
        if (chunk.encoding.len == 0 or std.ascii.eqlIgnoreCase(chunk.encoding, "plain")) return .{ .f64 = if (chunk.nullable) .optional else .required };
        if (std.ascii.eqlIgnoreCase(chunk.encoding, "rle_dictionary") or
            std.ascii.eqlIgnoreCase(chunk.encoding, "plain_dictionary"))
        {
            return .{ .f64 = if (chunk.nullable) .dictionary_optional else .dictionary_required };
        }
    }
    if (std.ascii.eqlIgnoreCase(chunk.physical_type, "float")) {
        if (chunk.encoding.len == 0 or std.ascii.eqlIgnoreCase(chunk.encoding, "plain")) return .{ .f64 = if (chunk.nullable) .float_optional else .float_required };
        if (std.ascii.eqlIgnoreCase(chunk.encoding, "rle_dictionary") or
            std.ascii.eqlIgnoreCase(chunk.encoding, "plain_dictionary"))
        {
            return .{ .f64 = if (chunk.nullable) .float_dictionary_optional else .float_dictionary_required };
        }
    }
    if (std.ascii.eqlIgnoreCase(chunk.physical_type, "boolean")) {
        if (chunk.encoding.len == 0 or std.ascii.eqlIgnoreCase(chunk.encoding, "plain")) return .{ .bool = if (chunk.nullable) .optional else .required };
    }
    if (std.ascii.eqlIgnoreCase(chunk.physical_type, "byte_array")) {
        if (chunk.encoding.len == 0 or std.ascii.eqlIgnoreCase(chunk.encoding, "plain")) return .{ .bytes = if (chunk.nullable) .optional else .required };
        if (std.ascii.eqlIgnoreCase(chunk.encoding, "rle_dictionary") or
            std.ascii.eqlIgnoreCase(chunk.encoding, "plain_dictionary"))
        {
            return .{ .bytes = if (chunk.nullable) .dictionary_optional else .dictionary_required };
        }
    }
    return error.UnsupportedParquetPage;
}

fn decimalModeForColumnChunk(chunk: external_source.ColumnChunk) !DecimalMode {
    if (chunk.decimal_precision <= 0) return error.UnsupportedParquetPage;
    if (chunk.decimal_scale < 0 or chunk.decimal_scale > chunk.decimal_precision) return error.UnsupportedParquetPage;
    const physical: DecimalPhysical = if (std.ascii.eqlIgnoreCase(chunk.physical_type, "int32"))
        .int32
    else if (chunk.physical_type.len == 0 or std.ascii.eqlIgnoreCase(chunk.physical_type, "int64"))
        .int64
    else if (std.ascii.eqlIgnoreCase(chunk.physical_type, "byte_array"))
        .byte_array
    else if (std.ascii.eqlIgnoreCase(chunk.physical_type, "fixed_len_byte_array"))
        .fixed_len_byte_array
    else
        return error.UnsupportedParquetPage;
    const type_length: usize = if (physical == .fixed_len_byte_array) blk: {
        if (chunk.type_length <= 0 or chunk.type_length > 8) return error.UnsupportedParquetPage;
        break :blk @intCast(chunk.type_length);
    } else 0;
    if (chunk.encoding.len == 0 or std.ascii.eqlIgnoreCase(chunk.encoding, "plain")) {
        return .{
            .physical = physical,
            .nullable = chunk.nullable,
            .dictionary = false,
            .scale = chunk.decimal_scale,
            .type_length = type_length,
        };
    }
    if (std.ascii.eqlIgnoreCase(chunk.encoding, "rle_dictionary") or
        std.ascii.eqlIgnoreCase(chunk.encoding, "plain_dictionary"))
    {
        return .{
            .physical = physical,
            .nullable = chunk.nullable,
            .dictionary = true,
            .scale = chunk.decimal_scale,
            .type_length = type_length,
        };
    }
    return error.UnsupportedParquetPage;
}

const TimestampUnit = enum {
    millis,
    micros,
    nanos,
};

fn timestampUnitForLogicalType(logical_type: []const u8) ?TimestampUnit {
    if (std.ascii.eqlIgnoreCase(logical_type, "timestamp_millis")) return .millis;
    if (std.ascii.eqlIgnoreCase(logical_type, "timestamp_micros")) return .micros;
    if (std.ascii.eqlIgnoreCase(logical_type, "timestamp_nanos")) return .nanos;
    return null;
}

fn timestampScaleFactorForMode(mode: PlainI64Mode) i64 {
    return switch (mode) {
        .timestamp_millis_required,
        .timestamp_millis_optional,
        .timestamp_millis_dictionary_required,
        .timestamp_millis_dictionary_optional,
        => 1_000_000,
        .timestamp_micros_required,
        .timestamp_micros_optional,
        .timestamp_micros_dictionary_required,
        .timestamp_micros_dictionary_optional,
        => 1_000,
        .timestamp_nanos_required,
        .timestamp_nanos_optional,
        .timestamp_nanos_dictionary_required,
        .timestamp_nanos_dictionary_optional,
        => 1,
        else => unreachable,
    };
}

fn scaleTimestampNsValues(values: []i64, scale_factor: i64) !void {
    if (scale_factor == 1) return;
    for (values) |*value| {
        value.* = std.math.mul(i64, value.*, scale_factor) catch return error.InvalidParquetPage;
    }
}

fn scanDecimalAsF64ColumnChunkAlloc(
    alloc: Allocator,
    bytes: []const u8,
    compression: parquet_page.CompressionCodec,
    mode: DecimalMode,
) !parquet_page.NullableF64Values {
    const unscaled = switch (mode.physical) {
        .int32 => try scanDecimalInt32UnscaledAlloc(alloc, bytes, compression, mode),
        .int64 => try scanDecimalInt64UnscaledAlloc(alloc, bytes, compression, mode),
        .byte_array => try scanDecimalByteArrayUnscaledAlloc(alloc, bytes, compression, mode),
        .fixed_len_byte_array => try scanDecimalFixedLenByteArrayUnscaledAlloc(alloc, bytes, compression, mode),
    };
    errdefer {
        alloc.free(unscaled.values);
        if (unscaled.nulls.len > 0) alloc.free(unscaled.nulls);
    }

    const values = try alloc.alloc(f64, unscaled.values.len);
    errdefer alloc.free(values);
    const divisor = try decimalScaleDivisor(mode.scale);
    for (unscaled.values, values) |value, *out| {
        out.* = @as(f64, @floatFromInt(value)) / divisor;
    }

    alloc.free(unscaled.values);
    return .{
        .values = values,
        .nulls = unscaled.nulls,
    };
}

fn scanDecimalInt32UnscaledAlloc(
    alloc: Allocator,
    bytes: []const u8,
    compression: parquet_page.CompressionCodec,
    mode: DecimalMode,
) !parquet_page.NullableI64Values {
    if (mode.dictionary) {
        if (mode.nullable) return try parquet_page.scanOptionalDictionaryI32AsI64ColumnChunkAlloc(alloc, bytes, compression);
        return .{
            .values = try parquet_page.scanDictionaryI32AsI64ColumnChunkAlloc(alloc, bytes, compression),
            .nulls = &.{},
        };
    }
    if (mode.nullable) return try parquet_page.scanOptionalPlainI32AsI64ColumnChunkAlloc(alloc, bytes, compression);
    return .{
        .values = try parquet_page.scanPlainI32AsI64ColumnChunkAlloc(alloc, bytes, compression),
        .nulls = &.{},
    };
}

fn scanDecimalInt64UnscaledAlloc(
    alloc: Allocator,
    bytes: []const u8,
    compression: parquet_page.CompressionCodec,
    mode: DecimalMode,
) !parquet_page.NullableI64Values {
    if (mode.dictionary) {
        if (mode.nullable) return try parquet_page.scanOptionalDictionaryI64ColumnChunkAlloc(alloc, bytes, compression);
        return .{
            .values = try parquet_page.scanDictionaryI64ColumnChunkAlloc(alloc, bytes, compression),
            .nulls = &.{},
        };
    }
    if (mode.nullable) return try parquet_page.scanOptionalPlainI64ColumnChunkAlloc(alloc, bytes, compression);
    return .{
        .values = try parquet_page.scanPlainI64ColumnChunkAlloc(alloc, bytes, compression),
        .nulls = &.{},
    };
}

fn scanDecimalByteArrayUnscaledAlloc(
    alloc: Allocator,
    bytes: []const u8,
    compression: parquet_page.CompressionCodec,
    mode: DecimalMode,
) !parquet_page.NullableI64Values {
    const decoded = if (mode.dictionary) blk: {
        if (mode.nullable) break :blk try parquet_page.scanOptionalDictionaryByteArrayColumnChunkAlloc(alloc, bytes, compression);
        break :blk parquet_page.NullableByteArrayValues{
            .values = try parquet_page.scanDictionaryByteArrayColumnChunkAlloc(alloc, bytes, compression),
            .nulls = &.{},
        };
    } else blk: {
        if (mode.nullable) break :blk try parquet_page.scanOptionalPlainByteArrayColumnChunkAlloc(alloc, bytes, compression);
        break :blk parquet_page.NullableByteArrayValues{
            .values = try parquet_page.scanPlainByteArrayColumnChunkAlloc(alloc, bytes, compression),
            .nulls = &.{},
        };
    };
    return try decimalBytesToUnscaledI64Alloc(alloc, decoded);
}

fn scanDecimalFixedLenByteArrayUnscaledAlloc(
    alloc: Allocator,
    bytes: []const u8,
    compression: parquet_page.CompressionCodec,
    mode: DecimalMode,
) !parquet_page.NullableI64Values {
    if (mode.type_length == 0 or mode.type_length > 8) return error.UnsupportedParquetPage;
    const decoded = if (mode.dictionary) blk: {
        if (mode.nullable) break :blk try parquet_page.scanOptionalDictionaryFixedLenByteArrayColumnChunkAlloc(alloc, bytes, compression, mode.type_length);
        break :blk parquet_page.NullableByteArrayValues{
            .values = try parquet_page.scanDictionaryFixedLenByteArrayColumnChunkAlloc(alloc, bytes, compression, mode.type_length),
            .nulls = &.{},
        };
    } else blk: {
        if (mode.nullable) break :blk try parquet_page.scanOptionalPlainFixedLenByteArrayColumnChunkAlloc(alloc, bytes, compression, mode.type_length);
        break :blk parquet_page.NullableByteArrayValues{
            .values = try parquet_page.scanPlainFixedLenByteArrayColumnChunkAlloc(alloc, bytes, compression, mode.type_length),
            .nulls = &.{},
        };
    };
    return try decimalBytesToUnscaledI64Alloc(alloc, decoded);
}

fn decimalBytesToUnscaledI64Alloc(
    alloc: Allocator,
    decoded: parquet_page.NullableByteArrayValues,
) !parquet_page.NullableI64Values {
    var input = decoded;
    defer {
        parquet_page.freePlainByteArrays(alloc, input.values);
        if (input.nulls.len > 0) alloc.free(input.nulls);
    }

    const values = try alloc.alloc(i64, input.values.len);
    errdefer alloc.free(values);
    for (input.values, 0..) |bytes, idx| {
        if (input.nulls.len > 0 and input.nulls[idx] != 0) {
            values[idx] = 0;
            continue;
        }
        values[idx] = try signedBigEndianDecimalBytesToI64(bytes);
    }

    const nulls = input.nulls;
    input.nulls = &.{};
    return .{
        .values = values,
        .nulls = nulls,
    };
}

fn signedBigEndianDecimalBytesToI64(bytes: []const u8) !i64 {
    if (bytes.len == 0) return error.InvalidParquetPage;
    if (bytes.len > 8) return error.UnsupportedParquetPage;
    var raw: u64 = 0;
    for (bytes) |byte| raw = (raw << 8) | byte;
    if ((bytes[0] & 0x80) != 0 and bytes.len < 8) {
        const bit_count: u6 = @intCast(bytes.len * 8);
        raw |= (~@as(u64, 0)) << bit_count;
    }
    return @as(i64, @bitCast(raw));
}

fn decimalScaleDivisor(scale: i32) !f64 {
    if (scale < 0) return error.UnsupportedParquetPage;
    var divisor: f64 = 1;
    const scale_count: usize = @intCast(scale);
    for (0..scale_count) |_| divisor *= 10;
    return divisor;
}

fn compressionCodecForColumnChunk(chunk: external_source.ColumnChunk) !parquet_page.CompressionCodec {
    if (chunk.compression_codec.len == 0 or
        std.ascii.eqlIgnoreCase(chunk.compression_codec, "uncompressed") or
        std.ascii.eqlIgnoreCase(chunk.compression_codec, "none"))
    {
        return .uncompressed;
    }
    if (std.ascii.eqlIgnoreCase(chunk.compression_codec, "snappy")) return .snappy;
    if (std.ascii.eqlIgnoreCase(chunk.compression_codec, "gzip")) return .gzip;
    if (std.ascii.eqlIgnoreCase(chunk.compression_codec, "zstd")) return .zstd;
    return error.UnsupportedParquetPage;
}

fn appendField(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, previous: *i16, id: i16, field_type: enum(u4) {
    stop = 0,
    boolean_true = 1,
    boolean_false = 2,
    byte = 3,
    i16 = 4,
    i32 = 5,
    i64 = 6,
    double = 7,
    binary = 8,
    list = 9,
    set = 10,
    map = 11,
    struct_ = 12,
}) !void {
    const delta = id - previous.*;
    if (delta > 0 and delta <= 15) {
        try out.append(alloc, (@as(u8, @intCast(delta)) << 4) | @as(u8, @intFromEnum(field_type)));
    } else {
        try out.append(alloc, @intFromEnum(field_type));
        try appendI16(out, alloc, id);
    }
    previous.* = id;
}

fn appendStop(out: *std.ArrayListUnmanaged(u8), alloc: Allocator) !void {
    try out.append(alloc, 0);
}

fn appendI16(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, value: i16) !void {
    try appendZigzag(out, alloc, value);
}

fn appendI32(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, value: i32) !void {
    try appendZigzag(out, alloc, value);
}

fn appendI64(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, value: i64) !void {
    try appendZigzag(out, alloc, value);
}

fn appendZigzag(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, value: anytype) !void {
    const Int = @TypeOf(value);
    const Unsigned = std.meta.Int(.unsigned, @bitSizeOf(Int));
    const encoded: Unsigned = @bitCast((value << 1) ^ (value >> (@bitSizeOf(Int) - 1)));
    try appendVarint(out, alloc, encoded);
}

fn appendVarint(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, value: anytype) !void {
    var remaining: u64 = @intCast(value);
    while (remaining >= 0x80) {
        try out.append(alloc, @as(u8, @intCast(remaining & 0x7f)) | 0x80);
        remaining >>= 7;
    }
    try out.append(alloc, @intCast(remaining));
}

fn appendListHeader(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, elem_type: enum(u4) {
    stop = 0,
    boolean_true = 1,
    boolean_false = 2,
    byte = 3,
    i16 = 4,
    i32 = 5,
    i64 = 6,
    double = 7,
    binary = 8,
    list = 9,
    set = 10,
    map = 11,
    struct_ = 12,
}, len: usize) !void {
    if (len < 15) {
        try out.append(alloc, (@as(u8, @intCast(len)) << 4) | @as(u8, @intFromEnum(elem_type)));
    } else {
        try out.append(alloc, 0xf0 | @as(u8, @intFromEnum(elem_type)));
        try appendVarint(out, alloc, len);
    }
}

fn appendBinary(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, bytes: []const u8) !void {
    try appendVarint(out, alloc, bytes.len);
    try out.appendSlice(alloc, bytes);
}

fn appendPlainI64DataPage(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, values: []const i64) !void {
    const byte_len: usize = values.len * 8;
    try appendPlainI64DataPageHeader(out, alloc, values.len, byte_len, byte_len);

    for (values) |value| {
        var buf: [8]u8 = undefined;
        std.mem.writeInt(i64, &buf, value, .little);
        try out.appendSlice(alloc, &buf);
    }
}

const TestInt96Timestamp = struct {
    nanos_of_day: u64,
    julian_day: u32,
};

fn appendPlainInt96TimestampDataPage(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, values: []const TestInt96Timestamp) !void {
    const byte_len: usize = values.len * 12;
    try appendPlainI64DataPageHeader(out, alloc, values.len, byte_len, byte_len);

    for (values) |value| {
        var buf: [12]u8 = undefined;
        std.mem.writeInt(u64, buf[0..8], value.nanos_of_day, .little);
        std.mem.writeInt(u32, buf[8..12], value.julian_day, .little);
        try out.appendSlice(alloc, &buf);
    }
}

fn appendPlainF64DataPage(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, values: []const f64) !void {
    const byte_len: usize = values.len * 8;
    try appendPlainI64DataPageHeader(out, alloc, values.len, byte_len, byte_len);

    for (values) |value| {
        var buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &buf, @bitCast(value), .little);
        try out.appendSlice(alloc, &buf);
    }
}

fn appendPlainF32DataPage(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, values: []const f32) !void {
    const byte_len: usize = values.len * 4;
    try appendPlainI64DataPageHeader(out, alloc, values.len, byte_len, byte_len);

    for (values) |value| {
        var buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &buf, @bitCast(value), .little);
        try out.appendSlice(alloc, &buf);
    }
}

fn appendPlainI32DataPage(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, values: []const i32) !void {
    const byte_len: usize = values.len * 4;
    try appendPlainI64DataPageHeader(out, alloc, values.len, byte_len, byte_len);

    for (values) |value| {
        var buf: [4]u8 = undefined;
        std.mem.writeInt(i32, &buf, value, .little);
        try out.appendSlice(alloc, &buf);
    }
}

fn appendPlainBoolDataPage(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, values: []const bool) !void {
    const byte_len: usize = (values.len + 7) / 8;
    try appendPlainI64DataPageHeader(out, alloc, values.len, byte_len, byte_len);

    var packed_bool_bytes = try alloc.alloc(u8, byte_len);
    defer alloc.free(packed_bool_bytes);
    @memset(packed_bool_bytes, 0);
    for (values, 0..) |value, idx| {
        if (value) packed_bool_bytes[idx / 8] |= @as(u8, 1) << @intCast(idx % 8);
    }
    try out.appendSlice(alloc, packed_bool_bytes);
}

fn appendSnappyPlainI64DataPage(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, values: []const i64) !void {
    var payload = std.ArrayListUnmanaged(u8).empty;
    defer payload.deinit(alloc);
    for (values) |value| {
        var buf: [8]u8 = undefined;
        std.mem.writeInt(i64, &buf, value, .little);
        try payload.appendSlice(alloc, &buf);
    }

    const compressed = try snappy.encode(alloc, payload.items);
    defer alloc.free(compressed);
    try appendPlainI64DataPageHeader(out, alloc, values.len, payload.items.len, compressed.len);
    try out.appendSlice(alloc, compressed);
}

fn appendGzipPlainI64DataPage(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, values: []const i64) !void {
    var payload = std.ArrayListUnmanaged(u8).empty;
    defer payload.deinit(alloc);
    for (values) |value| {
        var buf: [8]u8 = undefined;
        std.mem.writeInt(i64, &buf, value, .little);
        try payload.appendSlice(alloc, &buf);
    }

    var out_buf: [256]u8 = undefined;
    var writer: std.Io.Writer = .fixed(&out_buf);
    var hist: [std.compress.flate.max_window_len]u8 = undefined;
    var compressor = try std.compress.flate.Compress.init(&writer, hist[0..], .gzip, .default);
    try compressor.writer.writeAll(payload.items);
    try compressor.finish();
    const compressed = writer.buffered();
    try appendPlainI64DataPageHeader(out, alloc, values.len, payload.items.len, compressed.len);
    try out.appendSlice(alloc, compressed);
}

fn appendZstdPlainI64FixtureDataPage(out: *std.ArrayListUnmanaged(u8), alloc: Allocator) !void {
    const uncompressed_len = 24;
    const compressed = [_]u8{
        0x28, 0xb5, 0x2f, 0xfd, 0x04, 0x58, 0xa5, 0x00, 0x00, 0x60, 0x01, 0x00,
        0x02, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00,
        0x60, 0xe0, 0x01, 0x60, 0x01, 0xfa, 0xcd, 0xc0, 0xe5,
    };
    try appendPlainI64DataPageHeader(out, alloc, 3, uncompressed_len, compressed.len);
    try out.appendSlice(alloc, &compressed);
}

fn appendPlainByteArrayDataPage(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, values: []const []const u8) !void {
    var payload = std.ArrayListUnmanaged(u8).empty;
    defer payload.deinit(alloc);
    for (values) |value| {
        var len_buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &len_buf, @intCast(value.len), .little);
        try payload.appendSlice(alloc, &len_buf);
        try payload.appendSlice(alloc, value);
    }
    try appendPlainI64DataPageHeader(out, alloc, values.len, payload.items.len, payload.items.len);
    try out.appendSlice(alloc, payload.items);
}

fn appendPlainFixedLenByteArrayDataPage(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, values: []const []const u8) !void {
    var payload = std.ArrayListUnmanaged(u8).empty;
    defer payload.deinit(alloc);
    const type_length = if (values.len == 0) 0 else values[0].len;
    if (type_length == 0) return error.InvalidParquetRowGroupBatch;
    for (values) |value| {
        if (value.len != type_length) return error.InvalidParquetRowGroupBatch;
        try payload.appendSlice(alloc, value);
    }
    try appendPlainI64DataPageHeader(out, alloc, values.len, payload.items.len, payload.items.len);
    try out.appendSlice(alloc, payload.items);
}

fn appendPlainI64DataPageHeader(
    out: *std.ArrayListUnmanaged(u8),
    alloc: Allocator,
    value_count: usize,
    uncompressed_byte_len: usize,
    compressed_byte_len: usize,
) !void {
    var page_prev: i16 = 0;
    try appendField(out, alloc, &page_prev, 1, .i32);
    try appendI32(out, alloc, 0);
    try appendField(out, alloc, &page_prev, 2, .i32);
    try appendI32(out, alloc, @intCast(uncompressed_byte_len));
    try appendField(out, alloc, &page_prev, 3, .i32);
    try appendI32(out, alloc, @intCast(compressed_byte_len));
    try appendField(out, alloc, &page_prev, 5, .struct_);

    var data_prev: i16 = 0;
    try appendField(out, alloc, &data_prev, 1, .i32);
    try appendI32(out, alloc, @intCast(value_count));
    try appendField(out, alloc, &data_prev, 2, .i32);
    try appendI32(out, alloc, 0);
    try appendField(out, alloc, &data_prev, 3, .i32);
    try appendI32(out, alloc, 2);
    try appendField(out, alloc, &data_prev, 4, .i32);
    try appendI32(out, alloc, 2);
    try appendStop(out, alloc);
    try appendStop(out, alloc);
}

fn appendPlainI64DictionaryPage(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, values: []const i64) !void {
    const byte_len: i32 = @intCast(values.len * 8);
    var page_prev: i16 = 0;
    try appendField(out, alloc, &page_prev, 1, .i32);
    try appendI32(out, alloc, 2);
    try appendField(out, alloc, &page_prev, 2, .i32);
    try appendI32(out, alloc, byte_len);
    try appendField(out, alloc, &page_prev, 3, .i32);
    try appendI32(out, alloc, byte_len);
    try appendField(out, alloc, &page_prev, 7, .struct_);

    var dictionary_prev: i16 = 0;
    try appendField(out, alloc, &dictionary_prev, 1, .i32);
    try appendI32(out, alloc, @intCast(values.len));
    try appendField(out, alloc, &dictionary_prev, 2, .i32);
    try appendI32(out, alloc, 0);
    try appendStop(out, alloc);
    try appendStop(out, alloc);

    for (values) |value| {
        var buf: [8]u8 = undefined;
        std.mem.writeInt(i64, &buf, value, .little);
        try out.appendSlice(alloc, &buf);
    }
}

fn appendPlainF64DictionaryPage(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, values: []const f64) !void {
    const byte_len: i32 = @intCast(values.len * 8);
    var page_prev: i16 = 0;
    try appendField(out, alloc, &page_prev, 1, .i32);
    try appendI32(out, alloc, 2);
    try appendField(out, alloc, &page_prev, 2, .i32);
    try appendI32(out, alloc, byte_len);
    try appendField(out, alloc, &page_prev, 3, .i32);
    try appendI32(out, alloc, byte_len);
    try appendField(out, alloc, &page_prev, 7, .struct_);

    var dictionary_prev: i16 = 0;
    try appendField(out, alloc, &dictionary_prev, 1, .i32);
    try appendI32(out, alloc, @intCast(values.len));
    try appendField(out, alloc, &dictionary_prev, 2, .i32);
    try appendI32(out, alloc, 0);
    try appendStop(out, alloc);
    try appendStop(out, alloc);

    for (values) |value| {
        var buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &buf, @bitCast(value), .little);
        try out.appendSlice(alloc, &buf);
    }
}

fn appendPlainF32DictionaryPage(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, values: []const f32) !void {
    const byte_len: i32 = @intCast(values.len * 4);
    var page_prev: i16 = 0;
    try appendField(out, alloc, &page_prev, 1, .i32);
    try appendI32(out, alloc, 2);
    try appendField(out, alloc, &page_prev, 2, .i32);
    try appendI32(out, alloc, byte_len);
    try appendField(out, alloc, &page_prev, 3, .i32);
    try appendI32(out, alloc, byte_len);
    try appendField(out, alloc, &page_prev, 7, .struct_);

    var dictionary_prev: i16 = 0;
    try appendField(out, alloc, &dictionary_prev, 1, .i32);
    try appendI32(out, alloc, @intCast(values.len));
    try appendField(out, alloc, &dictionary_prev, 2, .i32);
    try appendI32(out, alloc, 0);
    try appendStop(out, alloc);
    try appendStop(out, alloc);

    for (values) |value| {
        var buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &buf, @bitCast(value), .little);
        try out.appendSlice(alloc, &buf);
    }
}

fn appendPlainI32DictionaryPage(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, values: []const i32) !void {
    const byte_len: i32 = @intCast(values.len * 4);
    var page_prev: i16 = 0;
    try appendField(out, alloc, &page_prev, 1, .i32);
    try appendI32(out, alloc, 2);
    try appendField(out, alloc, &page_prev, 2, .i32);
    try appendI32(out, alloc, byte_len);
    try appendField(out, alloc, &page_prev, 3, .i32);
    try appendI32(out, alloc, byte_len);
    try appendField(out, alloc, &page_prev, 7, .struct_);

    var dictionary_prev: i16 = 0;
    try appendField(out, alloc, &dictionary_prev, 1, .i32);
    try appendI32(out, alloc, @intCast(values.len));
    try appendField(out, alloc, &dictionary_prev, 2, .i32);
    try appendI32(out, alloc, 0);
    try appendStop(out, alloc);
    try appendStop(out, alloc);

    for (values) |value| {
        var buf: [4]u8 = undefined;
        std.mem.writeInt(i32, &buf, value, .little);
        try out.appendSlice(alloc, &buf);
    }
}

fn appendPlainByteArrayDictionaryPage(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, values: []const []const u8) !void {
    var payload = std.ArrayListUnmanaged(u8).empty;
    defer payload.deinit(alloc);
    for (values) |value| {
        var len_buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &len_buf, @intCast(value.len), .little);
        try payload.appendSlice(alloc, &len_buf);
        try payload.appendSlice(alloc, value);
    }

    var page_prev: i16 = 0;
    try appendField(out, alloc, &page_prev, 1, .i32);
    try appendI32(out, alloc, 2);
    try appendField(out, alloc, &page_prev, 2, .i32);
    try appendI32(out, alloc, @intCast(payload.items.len));
    try appendField(out, alloc, &page_prev, 3, .i32);
    try appendI32(out, alloc, @intCast(payload.items.len));
    try appendField(out, alloc, &page_prev, 7, .struct_);

    var dictionary_prev: i16 = 0;
    try appendField(out, alloc, &dictionary_prev, 1, .i32);
    try appendI32(out, alloc, @intCast(values.len));
    try appendField(out, alloc, &dictionary_prev, 2, .i32);
    try appendI32(out, alloc, 0);
    try appendStop(out, alloc);
    try appendStop(out, alloc);

    try out.appendSlice(alloc, payload.items);
}

fn appendPlainFixedLenByteArrayDictionaryPage(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, values: []const []const u8) !void {
    var payload = std.ArrayListUnmanaged(u8).empty;
    defer payload.deinit(alloc);
    const type_length = if (values.len == 0) 0 else values[0].len;
    if (type_length == 0) return error.InvalidParquetRowGroupBatch;
    for (values) |value| {
        if (value.len != type_length) return error.InvalidParquetRowGroupBatch;
        try payload.appendSlice(alloc, value);
    }

    var page_prev: i16 = 0;
    try appendField(out, alloc, &page_prev, 1, .i32);
    try appendI32(out, alloc, 2);
    try appendField(out, alloc, &page_prev, 2, .i32);
    try appendI32(out, alloc, @intCast(payload.items.len));
    try appendField(out, alloc, &page_prev, 3, .i32);
    try appendI32(out, alloc, @intCast(payload.items.len));
    try appendField(out, alloc, &page_prev, 7, .struct_);

    var dictionary_prev: i16 = 0;
    try appendField(out, alloc, &dictionary_prev, 1, .i32);
    try appendI32(out, alloc, @intCast(values.len));
    try appendField(out, alloc, &dictionary_prev, 2, .i32);
    try appendI32(out, alloc, 0);
    try appendStop(out, alloc);
    try appendStop(out, alloc);

    try out.appendSlice(alloc, payload.items);
}

fn appendDictionaryI64DataPage(
    out: *std.ArrayListUnmanaged(u8),
    alloc: Allocator,
    value_count: usize,
    bit_width: u8,
    encoded_indexes: []const u8,
) !void {
    const byte_len: i32 = @intCast(1 + encoded_indexes.len);
    var page_prev: i16 = 0;
    try appendField(out, alloc, &page_prev, 1, .i32);
    try appendI32(out, alloc, 0);
    try appendField(out, alloc, &page_prev, 2, .i32);
    try appendI32(out, alloc, byte_len);
    try appendField(out, alloc, &page_prev, 3, .i32);
    try appendI32(out, alloc, byte_len);
    try appendField(out, alloc, &page_prev, 5, .struct_);

    var data_prev: i16 = 0;
    try appendField(out, alloc, &data_prev, 1, .i32);
    try appendI32(out, alloc, @intCast(value_count));
    try appendField(out, alloc, &data_prev, 2, .i32);
    try appendI32(out, alloc, 7);
    try appendField(out, alloc, &data_prev, 3, .i32);
    try appendI32(out, alloc, 2);
    try appendField(out, alloc, &data_prev, 4, .i32);
    try appendI32(out, alloc, 2);
    try appendStop(out, alloc);
    try appendStop(out, alloc);

    try out.append(alloc, bit_width);
    try out.appendSlice(alloc, encoded_indexes);
}

fn appendOptionalDictionaryI64DataPageV2(
    out: *std.ArrayListUnmanaged(u8),
    alloc: Allocator,
    values: []const ?u8,
    bit_width: u8,
    encoded_indexes: []const u8,
) !void {
    var present_count: usize = 0;
    for (values) |maybe| {
        if (maybe != null) present_count += 1;
    }
    const level_group_count = (values.len + 7) / 8;
    const definition_level_bytes: i32 = @intCast(1 + level_group_count);
    const byte_len: i32 = @intCast(@as(usize, @intCast(definition_level_bytes)) + 1 + encoded_indexes.len);
    const null_count: i32 = @intCast(values.len - present_count);

    var page_prev: i16 = 0;
    try appendField(out, alloc, &page_prev, 1, .i32);
    try appendI32(out, alloc, 3);
    try appendField(out, alloc, &page_prev, 2, .i32);
    try appendI32(out, alloc, byte_len);
    try appendField(out, alloc, &page_prev, 3, .i32);
    try appendI32(out, alloc, byte_len);
    try appendField(out, alloc, &page_prev, 8, .struct_);

    var data_prev: i16 = 0;
    try appendField(out, alloc, &data_prev, 1, .i32);
    try appendI32(out, alloc, @intCast(values.len));
    try appendField(out, alloc, &data_prev, 2, .i32);
    try appendI32(out, alloc, null_count);
    try appendField(out, alloc, &data_prev, 3, .i32);
    try appendI32(out, alloc, @intCast(values.len));
    try appendField(out, alloc, &data_prev, 4, .i32);
    try appendI32(out, alloc, 7);
    try appendField(out, alloc, &data_prev, 5, .i32);
    try appendI32(out, alloc, definition_level_bytes);
    try appendField(out, alloc, &data_prev, 6, .i32);
    try appendI32(out, alloc, 0);
    try appendStop(out, alloc);
    try appendStop(out, alloc);

    try appendVarint(out, alloc, (@as(u64, @intCast(level_group_count)) << 1) | 1);
    var packed_levels = try alloc.alloc(u8, level_group_count);
    defer alloc.free(packed_levels);
    @memset(packed_levels, 0);
    for (values, 0..) |maybe, idx| {
        if (maybe != null) packed_levels[idx / 8] |= @as(u8, 1) << @intCast(idx % 8);
    }
    try out.appendSlice(alloc, packed_levels);
    try out.append(alloc, bit_width);
    try out.appendSlice(alloc, encoded_indexes);
}

pub const TestPlainI64Column = struct {
    column_id: []const u8,
    values: []const i64,
    field_id: ?i32 = null,
};

pub const TestPlainByteArrayColumn = struct {
    column_id: []const u8,
    values: []const []const u8,
};

const TestColumnFooter = struct {
    column_id: []const u8,
    column_offset: usize,
    compressed_len: usize,
    uncompressed_len: usize,
    physical_type: i32 = 2,
    encoding: i32 = 0,
    compression_codec: i32 = 0,
    field_id: ?i32 = null,
};

fn appendSingleColumnFooterMetadata(
    out: *std.ArrayListUnmanaged(u8),
    alloc: Allocator,
    column_id: []const u8,
    row_count: usize,
    column_offset: usize,
    compressed_len: usize,
    uncompressed_len: usize,
    encoding: i32,
    compression_codec: i32,
) !void {
    const columns = [_]TestColumnFooter{.{
        .column_id = column_id,
        .column_offset = column_offset,
        .compressed_len = compressed_len,
        .uncompressed_len = uncompressed_len,
        .encoding = encoding,
        .compression_codec = compression_codec,
    }};
    return try appendPlainI64FooterMetadata(out, alloc, row_count, &columns);
}

fn appendSingleColumnFooterMetadataWithFieldId(
    out: *std.ArrayListUnmanaged(u8),
    alloc: Allocator,
    column_id: []const u8,
    row_count: usize,
    column_offset: usize,
    compressed_len: usize,
    uncompressed_len: usize,
    encoding: i32,
    compression_codec: i32,
    field_id: i32,
) !void {
    const columns = [_]TestColumnFooter{.{
        .column_id = column_id,
        .column_offset = column_offset,
        .compressed_len = compressed_len,
        .uncompressed_len = uncompressed_len,
        .encoding = encoding,
        .compression_codec = compression_codec,
        .field_id = field_id,
    }};
    return try appendPlainI64FooterMetadata(out, alloc, row_count, &columns);
}

fn appendPlainI64FooterMetadata(
    out: *std.ArrayListUnmanaged(u8),
    alloc: Allocator,
    row_count: usize,
    columns: []const TestColumnFooter,
) !void {
    if (row_count == 0 or columns.len == 0) return error.InvalidParquetRowGroupBatch;
    var total_byte_len: usize = 0;
    for (columns) |column| {
        if (column.column_id.len == 0 or column.compressed_len == 0 or column.uncompressed_len == 0) return error.InvalidParquetRowGroupBatch;
        total_byte_len += column.uncompressed_len;
    }

    var file_prev: i16 = 0;
    try appendField(out, alloc, &file_prev, 1, .i32);
    try appendI32(out, alloc, 1);
    if (footerSchemaHasFieldIds(columns)) {
        try appendField(out, alloc, &file_prev, 2, .list);
        try appendListHeader(out, alloc, .struct_, columns.len + 1);

        var root_prev: i16 = 0;
        try appendField(out, alloc, &root_prev, 4, .binary);
        try appendBinary(out, alloc, "schema");
        try appendField(out, alloc, &root_prev, 5, .i32);
        try appendI32(out, alloc, @intCast(columns.len));
        try appendStop(out, alloc);

        for (columns) |column| {
            var leaf_prev: i16 = 0;
            try appendField(out, alloc, &leaf_prev, 1, .i32);
            try appendI32(out, alloc, column.physical_type);
            try appendField(out, alloc, &leaf_prev, 3, .i32);
            try appendI32(out, alloc, 0);
            try appendField(out, alloc, &leaf_prev, 4, .binary);
            try appendBinary(out, alloc, column.column_id);
            if (column.field_id) |field_id| {
                try appendField(out, alloc, &leaf_prev, 9, .i32);
                try appendI32(out, alloc, field_id);
            }
            try appendStop(out, alloc);
        }
    }
    try appendField(out, alloc, &file_prev, 3, .i64);
    try appendI64(out, alloc, @intCast(row_count));
    try appendField(out, alloc, &file_prev, 4, .list);
    try appendListHeader(out, alloc, .struct_, 1);

    var rg_prev: i16 = 0;
    try appendField(out, alloc, &rg_prev, 1, .list);
    try appendListHeader(out, alloc, .struct_, columns.len);
    for (columns) |column| {
        try appendColumnFooterMetadata(out, alloc, column);
    }
    try appendField(out, alloc, &rg_prev, 2, .i64);
    try appendI64(out, alloc, @intCast(total_byte_len));
    try appendField(out, alloc, &rg_prev, 3, .i64);
    try appendI64(out, alloc, @intCast(row_count));
    try appendField(out, alloc, &rg_prev, 5, .i64);
    try appendI64(out, alloc, @intCast(columns[0].column_offset));
    try appendStop(out, alloc);

    try appendStop(out, alloc);
}

fn footerSchemaHasFieldIds(columns: []const TestColumnFooter) bool {
    for (columns) |column| {
        if (column.field_id != null) return true;
    }
    return false;
}

fn appendColumnFooterMetadata(
    out: *std.ArrayListUnmanaged(u8),
    alloc: Allocator,
    column: TestColumnFooter,
) !void {
    var chunk_prev: i16 = 0;
    try appendField(out, alloc, &chunk_prev, 2, .i64);
    try appendI64(out, alloc, @intCast(column.column_offset));
    try appendField(out, alloc, &chunk_prev, 3, .struct_);

    var meta_prev: i16 = 0;
    try appendField(out, alloc, &meta_prev, 1, .i32);
    try appendI32(out, alloc, column.physical_type);
    try appendField(out, alloc, &meta_prev, 2, .list);
    try appendListHeader(out, alloc, .i32, 1);
    try appendI32(out, alloc, column.encoding);
    try appendField(out, alloc, &meta_prev, 3, .list);
    try appendListHeader(out, alloc, .binary, 1);
    try appendBinary(out, alloc, column.column_id);
    try appendField(out, alloc, &meta_prev, 4, .i32);
    try appendI32(out, alloc, column.compression_codec);
    try appendField(out, alloc, &meta_prev, 6, .i64);
    try appendI64(out, alloc, @intCast(column.uncompressed_len));
    try appendField(out, alloc, &meta_prev, 7, .i64);
    try appendI64(out, alloc, @intCast(column.compressed_len));
    try appendField(out, alloc, &meta_prev, 9, .i64);
    try appendI64(out, alloc, @intCast(column.column_offset));
    try appendStop(out, alloc);

    try appendStop(out, alloc);
}

fn appendParquetTrailer(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, metadata_len: usize) !void {
    var len_buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &len_buf, @intCast(metadata_len), .little);
    try out.appendSlice(alloc, &len_buf);
    try out.appendSlice(alloc, "PAR1");
}

pub fn buildTestSingleColumnPlainI64ParquetObjectAlloc(alloc: Allocator, column_id: []const u8, values: []const i64) ![]u8 {
    const columns = [_]TestPlainI64Column{.{ .column_id = column_id, .values = values }};
    return try buildTestPlainI64ParquetObjectAlloc(alloc, &columns);
}

pub fn buildTestPlainI64ParquetObjectAlloc(alloc: Allocator, columns: []const TestPlainI64Column) ![]u8 {
    if (columns.len == 0) return error.InvalidParquetRowGroupBatch;
    const row_count = columns[0].values.len;
    if (row_count == 0) return error.InvalidParquetRowGroupBatch;
    for (columns) |column| {
        if (column.column_id.len == 0 or column.values.len != row_count) return error.InvalidParquetRowGroupBatch;
    }

    const chunks = try alloc.alloc(std.ArrayListUnmanaged(u8), columns.len);
    defer alloc.free(chunks);
    @memset(chunks, .empty);
    var initialized_chunks: usize = 0;
    defer {
        for (chunks[0..initialized_chunks]) |*chunk| chunk.deinit(alloc);
    }
    for (columns, 0..) |column, idx| {
        try appendPlainI64DataPage(&chunks[idx], alloc, column.values);
        initialized_chunks += 1;
    }

    const footers = try alloc.alloc(TestColumnFooter, columns.len);
    defer alloc.free(footers);

    var object = std.ArrayListUnmanaged(u8).empty;
    errdefer object.deinit(alloc);
    try object.appendNTimes(alloc, 0, 100);
    for (columns, chunks, 0..) |column, chunk, idx| {
        footers[idx] = .{
            .column_id = column.column_id,
            .column_offset = object.items.len,
            .compressed_len = chunk.items.len,
            .uncompressed_len = chunk.items.len,
            .field_id = column.field_id,
        };
        try object.appendSlice(alloc, chunk.items);
    }
    const metadata_start = object.items.len;
    try appendPlainI64FooterMetadata(&object, alloc, row_count, footers);
    const metadata_len = object.items.len - metadata_start;
    try appendParquetTrailer(&object, alloc, metadata_len);
    return try object.toOwnedSlice(alloc);
}

pub fn buildTestPlainI64AndByteArrayParquetObjectAlloc(
    alloc: Allocator,
    i64_columns: []const TestPlainI64Column,
    byte_array_columns: []const TestPlainByteArrayColumn,
) ![]u8 {
    if (i64_columns.len == 0 and byte_array_columns.len == 0) return error.InvalidParquetRowGroupBatch;
    const row_count = if (i64_columns.len != 0) i64_columns[0].values.len else byte_array_columns[0].values.len;
    if (row_count == 0) return error.InvalidParquetRowGroupBatch;
    for (i64_columns) |column| {
        if (column.column_id.len == 0 or column.values.len != row_count) return error.InvalidParquetRowGroupBatch;
    }
    for (byte_array_columns) |column| {
        if (column.column_id.len == 0 or column.values.len != row_count) return error.InvalidParquetRowGroupBatch;
    }

    const column_count = i64_columns.len + byte_array_columns.len;
    const chunks = try alloc.alloc(std.ArrayListUnmanaged(u8), column_count);
    defer alloc.free(chunks);
    @memset(chunks, .empty);
    var initialized_chunks: usize = 0;
    defer {
        for (chunks[0..initialized_chunks]) |*chunk| chunk.deinit(alloc);
    }
    for (i64_columns, 0..) |column, idx| {
        try appendPlainI64DataPage(&chunks[idx], alloc, column.values);
        initialized_chunks += 1;
    }
    for (byte_array_columns, 0..) |column, idx| {
        try appendPlainByteArrayDataPage(&chunks[i64_columns.len + idx], alloc, column.values);
        initialized_chunks += 1;
    }

    const footers = try alloc.alloc(TestColumnFooter, column_count);
    defer alloc.free(footers);

    var object = std.ArrayListUnmanaged(u8).empty;
    errdefer object.deinit(alloc);
    try object.appendNTimes(alloc, 0, 100);
    for (i64_columns, chunks[0..i64_columns.len], 0..) |column, chunk, idx| {
        footers[idx] = .{
            .column_id = column.column_id,
            .column_offset = object.items.len,
            .compressed_len = chunk.items.len,
            .uncompressed_len = chunk.items.len,
            .physical_type = 2,
            .field_id = column.field_id,
        };
        try object.appendSlice(alloc, chunk.items);
    }
    for (byte_array_columns, chunks[i64_columns.len..], 0..) |column, chunk, idx| {
        footers[i64_columns.len + idx] = .{
            .column_id = column.column_id,
            .column_offset = object.items.len,
            .compressed_len = chunk.items.len,
            .uncompressed_len = chunk.items.len,
            .physical_type = 6,
        };
        try object.appendSlice(alloc, chunk.items);
    }
    const metadata_start = object.items.len;
    try appendPlainI64FooterMetadata(&object, alloc, row_count, footers);
    const metadata_len = object.items.len - metadata_start;
    try appendParquetTrailer(&object, alloc, metadata_len);
    return try object.toOwnedSlice(alloc);
}

fn appendOptionalPlainI64DataPageV2(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, values: []const ?i64) !void {
    var present_count: usize = 0;
    for (values) |maybe| {
        if (maybe != null) present_count += 1;
    }
    const level_group_count = (values.len + 7) / 8;
    const definition_level_bytes: i32 = @intCast(1 + level_group_count);
    const byte_len: i32 = @intCast(@as(usize, @intCast(definition_level_bytes)) + present_count * 8);
    const null_count: i32 = @intCast(values.len - present_count);
    var page_prev: i16 = 0;
    try appendField(out, alloc, &page_prev, 1, .i32);
    try appendI32(out, alloc, 3);
    try appendField(out, alloc, &page_prev, 2, .i32);
    try appendI32(out, alloc, byte_len);
    try appendField(out, alloc, &page_prev, 3, .i32);
    try appendI32(out, alloc, byte_len);
    try appendField(out, alloc, &page_prev, 8, .struct_);

    var data_prev: i16 = 0;
    try appendField(out, alloc, &data_prev, 1, .i32);
    try appendI32(out, alloc, @intCast(values.len));
    try appendField(out, alloc, &data_prev, 2, .i32);
    try appendI32(out, alloc, null_count);
    try appendField(out, alloc, &data_prev, 3, .i32);
    try appendI32(out, alloc, @intCast(values.len));
    try appendField(out, alloc, &data_prev, 4, .i32);
    try appendI32(out, alloc, 0);
    try appendField(out, alloc, &data_prev, 5, .i32);
    try appendI32(out, alloc, definition_level_bytes);
    try appendField(out, alloc, &data_prev, 6, .i32);
    try appendI32(out, alloc, 0);
    try appendStop(out, alloc);
    try appendStop(out, alloc);

    try appendVarint(out, alloc, (@as(u64, @intCast(level_group_count)) << 1) | 1);
    var packed_levels = try alloc.alloc(u8, level_group_count);
    defer alloc.free(packed_levels);
    @memset(packed_levels, 0);
    for (values, 0..) |maybe, idx| {
        if (maybe != null) packed_levels[idx / 8] |= @as(u8, 1) << @intCast(idx % 8);
    }
    try out.appendSlice(alloc, packed_levels);
    for (values) |maybe| {
        const value = maybe orelse continue;
        var buf: [8]u8 = undefined;
        std.mem.writeInt(i64, &buf, value, .little);
        try out.appendSlice(alloc, &buf);
    }
}

fn appendOptionalPlainInt96TimestampDataPageV2(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, values: []const ?TestInt96Timestamp) !void {
    var present_count: usize = 0;
    for (values) |maybe| {
        if (maybe != null) present_count += 1;
    }
    const level_group_count = (values.len + 7) / 8;
    const definition_level_bytes: i32 = @intCast(1 + level_group_count);
    const byte_len: i32 = @intCast(@as(usize, @intCast(definition_level_bytes)) + present_count * 12);
    const null_count: i32 = @intCast(values.len - present_count);
    var page_prev: i16 = 0;
    try appendField(out, alloc, &page_prev, 1, .i32);
    try appendI32(out, alloc, 3);
    try appendField(out, alloc, &page_prev, 2, .i32);
    try appendI32(out, alloc, byte_len);
    try appendField(out, alloc, &page_prev, 3, .i32);
    try appendI32(out, alloc, byte_len);
    try appendField(out, alloc, &page_prev, 8, .struct_);

    var data_prev: i16 = 0;
    try appendField(out, alloc, &data_prev, 1, .i32);
    try appendI32(out, alloc, @intCast(values.len));
    try appendField(out, alloc, &data_prev, 2, .i32);
    try appendI32(out, alloc, null_count);
    try appendField(out, alloc, &data_prev, 3, .i32);
    try appendI32(out, alloc, @intCast(values.len));
    try appendField(out, alloc, &data_prev, 4, .i32);
    try appendI32(out, alloc, 0);
    try appendField(out, alloc, &data_prev, 5, .i32);
    try appendI32(out, alloc, definition_level_bytes);
    try appendField(out, alloc, &data_prev, 6, .i32);
    try appendI32(out, alloc, 0);
    try appendStop(out, alloc);
    try appendStop(out, alloc);

    try appendVarint(out, alloc, (@as(u64, @intCast(level_group_count)) << 1) | 1);
    var packed_levels = try alloc.alloc(u8, level_group_count);
    defer alloc.free(packed_levels);
    @memset(packed_levels, 0);
    for (values, 0..) |maybe, idx| {
        if (maybe != null) packed_levels[idx / 8] |= @as(u8, 1) << @intCast(idx % 8);
    }
    try out.appendSlice(alloc, packed_levels);
    for (values) |maybe| {
        const value = maybe orelse continue;
        var buf: [12]u8 = undefined;
        std.mem.writeInt(u64, buf[0..8], value.nanos_of_day, .little);
        std.mem.writeInt(u32, buf[8..12], value.julian_day, .little);
        try out.appendSlice(alloc, &buf);
    }
}

fn appendOptionalPlainF64DataPageV2(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, values: []const ?f64) !void {
    var present_count: usize = 0;
    for (values) |maybe| {
        if (maybe != null) present_count += 1;
    }
    const level_group_count = (values.len + 7) / 8;
    const definition_level_bytes: i32 = @intCast(1 + level_group_count);
    const byte_len: i32 = @intCast(@as(usize, @intCast(definition_level_bytes)) + present_count * 8);
    const null_count: i32 = @intCast(values.len - present_count);
    var page_prev: i16 = 0;
    try appendField(out, alloc, &page_prev, 1, .i32);
    try appendI32(out, alloc, 3);
    try appendField(out, alloc, &page_prev, 2, .i32);
    try appendI32(out, alloc, byte_len);
    try appendField(out, alloc, &page_prev, 3, .i32);
    try appendI32(out, alloc, byte_len);
    try appendField(out, alloc, &page_prev, 8, .struct_);

    var data_prev: i16 = 0;
    try appendField(out, alloc, &data_prev, 1, .i32);
    try appendI32(out, alloc, @intCast(values.len));
    try appendField(out, alloc, &data_prev, 2, .i32);
    try appendI32(out, alloc, null_count);
    try appendField(out, alloc, &data_prev, 3, .i32);
    try appendI32(out, alloc, @intCast(values.len));
    try appendField(out, alloc, &data_prev, 4, .i32);
    try appendI32(out, alloc, 0);
    try appendField(out, alloc, &data_prev, 5, .i32);
    try appendI32(out, alloc, definition_level_bytes);
    try appendField(out, alloc, &data_prev, 6, .i32);
    try appendI32(out, alloc, 0);
    try appendStop(out, alloc);
    try appendStop(out, alloc);

    try appendVarint(out, alloc, (@as(u64, @intCast(level_group_count)) << 1) | 1);
    var packed_levels = try alloc.alloc(u8, level_group_count);
    defer alloc.free(packed_levels);
    @memset(packed_levels, 0);
    for (values, 0..) |maybe, idx| {
        if (maybe != null) packed_levels[idx / 8] |= @as(u8, 1) << @intCast(idx % 8);
    }
    try out.appendSlice(alloc, packed_levels);
    for (values) |maybe| {
        const value = maybe orelse continue;
        var buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &buf, @bitCast(value), .little);
        try out.appendSlice(alloc, &buf);
    }
}

fn appendOptionalPlainF32DataPageV2(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, values: []const ?f32) !void {
    var present_count: usize = 0;
    for (values) |maybe| {
        if (maybe != null) present_count += 1;
    }
    const level_group_count = (values.len + 7) / 8;
    const definition_level_bytes: i32 = @intCast(1 + level_group_count);
    const byte_len: i32 = @intCast(@as(usize, @intCast(definition_level_bytes)) + present_count * 4);
    const null_count: i32 = @intCast(values.len - present_count);
    var page_prev: i16 = 0;
    try appendField(out, alloc, &page_prev, 1, .i32);
    try appendI32(out, alloc, 3);
    try appendField(out, alloc, &page_prev, 2, .i32);
    try appendI32(out, alloc, byte_len);
    try appendField(out, alloc, &page_prev, 3, .i32);
    try appendI32(out, alloc, byte_len);
    try appendField(out, alloc, &page_prev, 8, .struct_);

    var data_prev: i16 = 0;
    try appendField(out, alloc, &data_prev, 1, .i32);
    try appendI32(out, alloc, @intCast(values.len));
    try appendField(out, alloc, &data_prev, 2, .i32);
    try appendI32(out, alloc, null_count);
    try appendField(out, alloc, &data_prev, 3, .i32);
    try appendI32(out, alloc, @intCast(values.len));
    try appendField(out, alloc, &data_prev, 4, .i32);
    try appendI32(out, alloc, 0);
    try appendField(out, alloc, &data_prev, 5, .i32);
    try appendI32(out, alloc, definition_level_bytes);
    try appendField(out, alloc, &data_prev, 6, .i32);
    try appendI32(out, alloc, 0);
    try appendStop(out, alloc);
    try appendStop(out, alloc);

    try appendVarint(out, alloc, (@as(u64, @intCast(level_group_count)) << 1) | 1);
    var packed_levels = try alloc.alloc(u8, level_group_count);
    defer alloc.free(packed_levels);
    @memset(packed_levels, 0);
    for (values, 0..) |maybe, idx| {
        if (maybe != null) packed_levels[idx / 8] |= @as(u8, 1) << @intCast(idx % 8);
    }
    try out.appendSlice(alloc, packed_levels);
    for (values) |maybe| {
        const value = maybe orelse continue;
        var buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &buf, @bitCast(value), .little);
        try out.appendSlice(alloc, &buf);
    }
}

fn appendOptionalPlainI32DataPageV2(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, values: []const ?i32) !void {
    var present_count: usize = 0;
    for (values) |maybe| {
        if (maybe != null) present_count += 1;
    }
    const level_group_count = (values.len + 7) / 8;
    const definition_level_bytes: i32 = @intCast(1 + level_group_count);
    const byte_len: i32 = @intCast(@as(usize, @intCast(definition_level_bytes)) + present_count * 4);
    const null_count: i32 = @intCast(values.len - present_count);
    var page_prev: i16 = 0;
    try appendField(out, alloc, &page_prev, 1, .i32);
    try appendI32(out, alloc, 3);
    try appendField(out, alloc, &page_prev, 2, .i32);
    try appendI32(out, alloc, byte_len);
    try appendField(out, alloc, &page_prev, 3, .i32);
    try appendI32(out, alloc, byte_len);
    try appendField(out, alloc, &page_prev, 8, .struct_);

    var data_prev: i16 = 0;
    try appendField(out, alloc, &data_prev, 1, .i32);
    try appendI32(out, alloc, @intCast(values.len));
    try appendField(out, alloc, &data_prev, 2, .i32);
    try appendI32(out, alloc, null_count);
    try appendField(out, alloc, &data_prev, 3, .i32);
    try appendI32(out, alloc, @intCast(values.len));
    try appendField(out, alloc, &data_prev, 4, .i32);
    try appendI32(out, alloc, 0);
    try appendField(out, alloc, &data_prev, 5, .i32);
    try appendI32(out, alloc, definition_level_bytes);
    try appendField(out, alloc, &data_prev, 6, .i32);
    try appendI32(out, alloc, 0);
    try appendStop(out, alloc);
    try appendStop(out, alloc);

    try appendVarint(out, alloc, (@as(u64, @intCast(level_group_count)) << 1) | 1);
    var packed_levels = try alloc.alloc(u8, level_group_count);
    defer alloc.free(packed_levels);
    @memset(packed_levels, 0);
    for (values, 0..) |maybe, idx| {
        if (maybe != null) packed_levels[idx / 8] |= @as(u8, 1) << @intCast(idx % 8);
    }
    try out.appendSlice(alloc, packed_levels);
    for (values) |maybe| {
        const value = maybe orelse continue;
        var buf: [4]u8 = undefined;
        std.mem.writeInt(i32, &buf, value, .little);
        try out.appendSlice(alloc, &buf);
    }
}

fn appendOptionalPlainBoolDataPageV2(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, values: []const ?bool) !void {
    var present_count: usize = 0;
    for (values) |maybe| {
        if (maybe != null) present_count += 1;
    }
    const level_group_count = (values.len + 7) / 8;
    const definition_level_bytes: i32 = @intCast(1 + level_group_count);
    const value_bytes = (present_count + 7) / 8;
    const byte_len: i32 = @intCast(@as(usize, @intCast(definition_level_bytes)) + value_bytes);
    const null_count: i32 = @intCast(values.len - present_count);
    var page_prev: i16 = 0;
    try appendField(out, alloc, &page_prev, 1, .i32);
    try appendI32(out, alloc, 3);
    try appendField(out, alloc, &page_prev, 2, .i32);
    try appendI32(out, alloc, byte_len);
    try appendField(out, alloc, &page_prev, 3, .i32);
    try appendI32(out, alloc, byte_len);
    try appendField(out, alloc, &page_prev, 8, .struct_);

    var data_prev: i16 = 0;
    try appendField(out, alloc, &data_prev, 1, .i32);
    try appendI32(out, alloc, @intCast(values.len));
    try appendField(out, alloc, &data_prev, 2, .i32);
    try appendI32(out, alloc, null_count);
    try appendField(out, alloc, &data_prev, 3, .i32);
    try appendI32(out, alloc, @intCast(values.len));
    try appendField(out, alloc, &data_prev, 4, .i32);
    try appendI32(out, alloc, 0);
    try appendField(out, alloc, &data_prev, 5, .i32);
    try appendI32(out, alloc, definition_level_bytes);
    try appendField(out, alloc, &data_prev, 6, .i32);
    try appendI32(out, alloc, 0);
    try appendStop(out, alloc);
    try appendStop(out, alloc);

    try appendVarint(out, alloc, (@as(u64, @intCast(level_group_count)) << 1) | 1);
    var packed_levels = try alloc.alloc(u8, level_group_count);
    defer alloc.free(packed_levels);
    @memset(packed_levels, 0);
    for (values, 0..) |maybe, idx| {
        if (maybe != null) packed_levels[idx / 8] |= @as(u8, 1) << @intCast(idx % 8);
    }
    try out.appendSlice(alloc, packed_levels);

    var packed_values = try alloc.alloc(u8, value_bytes);
    defer alloc.free(packed_values);
    @memset(packed_values, 0);
    var present_idx: usize = 0;
    for (values) |maybe| {
        const value = maybe orelse continue;
        if (value) packed_values[present_idx / 8] |= @as(u8, 1) << @intCast(present_idx % 8);
        present_idx += 1;
    }
    try out.appendSlice(alloc, packed_values);
}

fn appendOptionalPlainByteArrayDataPageV2(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, values: []const ?[]const u8) !void {
    var present_byte_len: usize = 0;
    var present_count: usize = 0;
    for (values) |maybe| {
        if (maybe) |value| {
            present_count += 1;
            present_byte_len += 4 + value.len;
        }
    }
    const level_group_count = (values.len + 7) / 8;
    const definition_level_bytes: i32 = @intCast(1 + level_group_count);
    const byte_len: i32 = @intCast(@as(usize, @intCast(definition_level_bytes)) + present_byte_len);
    const null_count: i32 = @intCast(values.len - present_count);
    var page_prev: i16 = 0;
    try appendField(out, alloc, &page_prev, 1, .i32);
    try appendI32(out, alloc, 3);
    try appendField(out, alloc, &page_prev, 2, .i32);
    try appendI32(out, alloc, byte_len);
    try appendField(out, alloc, &page_prev, 3, .i32);
    try appendI32(out, alloc, byte_len);
    try appendField(out, alloc, &page_prev, 8, .struct_);

    var data_prev: i16 = 0;
    try appendField(out, alloc, &data_prev, 1, .i32);
    try appendI32(out, alloc, @intCast(values.len));
    try appendField(out, alloc, &data_prev, 2, .i32);
    try appendI32(out, alloc, null_count);
    try appendField(out, alloc, &data_prev, 3, .i32);
    try appendI32(out, alloc, @intCast(values.len));
    try appendField(out, alloc, &data_prev, 4, .i32);
    try appendI32(out, alloc, 0);
    try appendField(out, alloc, &data_prev, 5, .i32);
    try appendI32(out, alloc, definition_level_bytes);
    try appendField(out, alloc, &data_prev, 6, .i32);
    try appendI32(out, alloc, 0);
    try appendStop(out, alloc);
    try appendStop(out, alloc);

    try appendVarint(out, alloc, (@as(u64, @intCast(level_group_count)) << 1) | 1);
    var packed_levels = try alloc.alloc(u8, level_group_count);
    defer alloc.free(packed_levels);
    @memset(packed_levels, 0);
    for (values, 0..) |maybe, idx| {
        if (maybe != null) packed_levels[idx / 8] |= @as(u8, 1) << @intCast(idx % 8);
    }
    try out.appendSlice(alloc, packed_levels);
    for (values) |maybe| {
        const value = maybe orelse continue;
        var len_buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &len_buf, @intCast(value.len), .little);
        try out.appendSlice(alloc, &len_buf);
        try out.appendSlice(alloc, value);
    }
}

fn appendOptionalPlainFixedLenByteArrayDataPageV2(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, values: []const ?[]const u8) !void {
    var type_length: usize = 0;
    var present_count: usize = 0;
    for (values) |maybe| {
        if (maybe) |value| {
            if (type_length == 0) type_length = value.len;
            if (value.len == 0 or value.len != type_length) return error.InvalidParquetRowGroupBatch;
            present_count += 1;
        }
    }
    if (type_length == 0) return error.InvalidParquetRowGroupBatch;
    const level_group_count = (values.len + 7) / 8;
    const definition_level_bytes: i32 = @intCast(1 + level_group_count);
    const present_byte_len = present_count * type_length;
    const byte_len: i32 = @intCast(@as(usize, @intCast(definition_level_bytes)) + present_byte_len);
    const null_count: i32 = @intCast(values.len - present_count);
    var page_prev: i16 = 0;
    try appendField(out, alloc, &page_prev, 1, .i32);
    try appendI32(out, alloc, 3);
    try appendField(out, alloc, &page_prev, 2, .i32);
    try appendI32(out, alloc, byte_len);
    try appendField(out, alloc, &page_prev, 3, .i32);
    try appendI32(out, alloc, byte_len);
    try appendField(out, alloc, &page_prev, 8, .struct_);

    var data_prev: i16 = 0;
    try appendField(out, alloc, &data_prev, 1, .i32);
    try appendI32(out, alloc, @intCast(values.len));
    try appendField(out, alloc, &data_prev, 2, .i32);
    try appendI32(out, alloc, null_count);
    try appendField(out, alloc, &data_prev, 3, .i32);
    try appendI32(out, alloc, @intCast(values.len));
    try appendField(out, alloc, &data_prev, 4, .i32);
    try appendI32(out, alloc, 0);
    try appendField(out, alloc, &data_prev, 5, .i32);
    try appendI32(out, alloc, definition_level_bytes);
    try appendField(out, alloc, &data_prev, 6, .i32);
    try appendI32(out, alloc, 0);
    try appendStop(out, alloc);
    try appendStop(out, alloc);

    try appendVarint(out, alloc, (@as(u64, @intCast(level_group_count)) << 1) | 1);
    var packed_levels = try alloc.alloc(u8, level_group_count);
    defer alloc.free(packed_levels);
    @memset(packed_levels, 0);
    for (values, 0..) |maybe, idx| {
        if (maybe != null) packed_levels[idx / 8] |= @as(u8, 1) << @intCast(idx % 8);
    }
    try out.appendSlice(alloc, packed_levels);
    for (values) |maybe| {
        const value = maybe orelse continue;
        try out.appendSlice(alloc, value);
    }
}

test "parquet row group batch assembles decoded i64 columns with external row refs" {
    const alloc = std.testing.allocator;
    var inventory = external_source.Inventory{
        .format = .parquet,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/events"),
        .snapshot_id = try alloc.dupe(u8, "sha256:objects"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "part-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/events/part-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-a"),
        .byte_len = 1024,
        .row_count = 3,
        .row_groups = try alloc.dupe(external_source.RowGroup, &[_]external_source.RowGroup{.{
            .ordinal = 0,
            .row_count = 3,
            .file_offset = 100,
            .total_byte_len = 96,
            .column_chunks = try alloc.dupe(external_source.ColumnChunk, &[_]external_source.ColumnChunk{.{
                .column_id = try alloc.dupe(u8, "amount"),
                .file_offset = 100,
                .compressed_len = 96,
                .uncompressed_len = 96,
                .encoding = try alloc.dupe(u8, "plain"),
            }}),
        }}),
    };
    try inventory.validate();

    var chunk = std.ArrayListUnmanaged(u8).empty;
    defer chunk.deinit(alloc);
    try appendPlainI64DataPage(&chunk, alloc, &[_]i64{ 10, 20 });
    try appendPlainI64DataPage(&chunk, alloc, &[_]i64{30});

    var owned = try buildRequiredPlainI64RowGroupBatchAlloc(alloc, inventory, "part-a.parquet", 0, &[_]ColumnChunkInput{.{
        .column_id = "amount",
        .bytes = chunk.items,
    }});
    defer owned.deinit(alloc);

    try owned.batch.validate();
    try std.testing.expectEqual(@as(usize, 3), owned.batch.rowCount());
    try std.testing.expectEqualStrings("amount", owned.batch.columns[0].name);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 10, 20, 30 }, owned.batch.columns[0].values.i64);
    const first_ref = owned.batch.row_refs[0].external;
    try std.testing.expectEqualStrings("events", first_ref.source_id);
    try std.testing.expectEqualStrings("sha256:objects", first_ref.snapshot_id);
    try std.testing.expectEqualStrings("part-a.parquet", first_ref.file_id);
    try std.testing.expectEqual(@as(u32, 0), first_ref.row_group_ordinal);
    try std.testing.expectEqual(@as(u64, 0), first_ref.row_ordinal);
}

test "parquet row group batch assembles optional i64 columns with null bitmap" {
    const alloc = std.testing.allocator;
    var inventory = external_source.Inventory{
        .format = .parquet,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/events"),
        .snapshot_id = try alloc.dupe(u8, "sha256:objects"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "part-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/events/part-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-a"),
        .byte_len = 1024,
        .row_count = 3,
        .row_groups = try alloc.dupe(external_source.RowGroup, &[_]external_source.RowGroup{.{
            .ordinal = 0,
            .row_count = 3,
            .file_offset = 100,
            .total_byte_len = 96,
            .column_chunks = try alloc.dupe(external_source.ColumnChunk, &[_]external_source.ColumnChunk{.{
                .column_id = try alloc.dupe(u8, "amount"),
                .file_offset = 100,
                .compressed_len = 96,
                .uncompressed_len = 96,
                .encoding = try alloc.dupe(u8, "plain"),
            }}),
        }}),
    };
    try inventory.validate();

    var chunk = std.ArrayListUnmanaged(u8).empty;
    defer chunk.deinit(alloc);
    try appendOptionalPlainI64DataPageV2(&chunk, alloc, &[_]?i64{ 10, null, 30 });

    var owned = try buildOptionalPlainI64RowGroupBatchAlloc(alloc, inventory, "part-a.parquet", 0, &[_]ColumnChunkInput{.{
        .column_id = "amount",
        .bytes = chunk.items,
    }});
    defer owned.deinit(alloc);

    try owned.batch.validate();
    try std.testing.expectEqual(@as(usize, 3), owned.batch.rowCount());
    try std.testing.expectEqualSlices(i64, &[_]i64{ 10, 0, 30 }, owned.batch.columns[0].values.i64);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0, 1, 0 }, owned.batch.columns[0].nulls.bytes);
    try std.testing.expect(!owned.batch.columns[0].nulls.isNull(0));
    try std.testing.expect(owned.batch.columns[0].nulls.isNull(1));
    try std.testing.expect(!owned.batch.columns[0].nulls.isNull(2));
    const null_ref = owned.batch.row_refs[1].external;
    try std.testing.expectEqualStrings("part-a.parquet", null_ref.file_id);
    try std.testing.expectEqual(@as(u64, 1), null_ref.row_ordinal);
}

test "parquet row group batch assembles dictionary i64 columns" {
    const alloc = std.testing.allocator;
    var inventory = external_source.Inventory{
        .format = .parquet,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/events"),
        .snapshot_id = try alloc.dupe(u8, "sha256:objects"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "part-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/events/part-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-a"),
        .byte_len = 1024,
        .row_count = 4,
        .row_groups = try alloc.dupe(external_source.RowGroup, &[_]external_source.RowGroup{.{
            .ordinal = 0,
            .row_count = 4,
            .file_offset = 100,
            .total_byte_len = 96,
            .column_chunks = try alloc.dupe(external_source.ColumnChunk, &[_]external_source.ColumnChunk{.{
                .column_id = try alloc.dupe(u8, "amount"),
                .file_offset = 100,
                .compressed_len = 96,
                .uncompressed_len = 96,
                .encoding = try alloc.dupe(u8, "rle_dictionary"),
            }}),
        }}),
    };
    try inventory.validate();

    var chunk = std.ArrayListUnmanaged(u8).empty;
    defer chunk.deinit(alloc);
    try appendPlainI64DictionaryPage(&chunk, alloc, &[_]i64{ 11, 22, 33 });
    try appendDictionaryI64DataPage(&chunk, alloc, 4, 2, &[_]u8{ 3, 0b10000100, 0 });

    var owned = try buildDictionaryPlainI64RowGroupBatchAlloc(alloc, inventory, "part-a.parquet", 0, &[_]ColumnChunkInput{.{
        .column_id = "amount",
        .bytes = chunk.items,
    }});
    defer owned.deinit(alloc);

    try owned.batch.validate();
    try std.testing.expectEqual(@as(usize, 4), owned.batch.rowCount());
    try std.testing.expectEqualSlices(i64, &[_]i64{ 11, 22, 11, 33 }, owned.batch.columns[0].values.i64);
    try std.testing.expectEqual(@as(usize, 0), owned.batch.columns[0].nulls.bytes.len);
    const row_ref = owned.batch.row_refs[3].external;
    try std.testing.expectEqualStrings("part-a.parquet", row_ref.file_id);
    try std.testing.expectEqual(@as(u64, 3), row_ref.row_ordinal);
}

test "parquet row group batch assembles optional dictionary i64 columns from inventory" {
    const alloc = std.testing.allocator;
    var inventory = external_source.Inventory{
        .format = .parquet,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/events"),
        .snapshot_id = try alloc.dupe(u8, "sha256:objects"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "part-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/events/part-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-a"),
        .byte_len = 1024,
        .row_count = 4,
        .row_groups = try alloc.dupe(external_source.RowGroup, &[_]external_source.RowGroup{.{
            .ordinal = 0,
            .row_count = 4,
            .file_offset = 100,
            .total_byte_len = 96,
            .column_chunks = try alloc.dupe(external_source.ColumnChunk, &[_]external_source.ColumnChunk{.{
                .column_id = try alloc.dupe(u8, "amount"),
                .file_offset = 100,
                .compressed_len = 96,
                .uncompressed_len = 96,
                .encoding = try alloc.dupe(u8, "rle_dictionary"),
                .physical_type = try alloc.dupe(u8, "int64"),
                .nullable = true,
            }}),
        }}),
    };
    try inventory.validate();

    var chunk = std.ArrayListUnmanaged(u8).empty;
    defer chunk.deinit(alloc);
    try appendPlainI64DictionaryPage(&chunk, alloc, &[_]i64{ 11, 22, 33 });
    try appendOptionalDictionaryI64DataPageV2(&chunk, alloc, &[_]?u8{ 0, null, 2, 1 }, 2, &[_]u8{ 3, 0b00011000, 0 });

    var owned = try buildSupportedI64RowGroupBatchAlloc(alloc, inventory, "part-a.parquet", 0, &[_]ColumnChunkInput{.{
        .column_id = "amount",
        .bytes = chunk.items,
    }});
    defer owned.deinit(alloc);

    try owned.batch.validate();
    try std.testing.expectEqual(@as(usize, 4), owned.batch.rowCount());
    try std.testing.expectEqualSlices(i64, &[_]i64{ 11, 0, 33, 22 }, owned.batch.columns[0].values.i64);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0, 1, 0, 0 }, owned.batch.columns[0].nulls.bytes);
    try std.testing.expect(!owned.batch.columns[0].nulls.isNull(0));
    try std.testing.expect(owned.batch.columns[0].nulls.isNull(1));
    try std.testing.expect(!owned.batch.columns[0].nulls.isNull(2));
    try std.testing.expect(!owned.batch.columns[0].nulls.isNull(3));
}

test "parquet row group batch dispatches supported i64 encodings from inventory" {
    const alloc = std.testing.allocator;
    var inventory = external_source.Inventory{
        .format = .parquet,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/events"),
        .snapshot_id = try alloc.dupe(u8, "sha256:objects"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "part-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/events/part-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-a"),
        .byte_len = 2048,
        .row_count = 4,
        .row_groups = try alloc.dupe(external_source.RowGroup, &[_]external_source.RowGroup{.{
            .ordinal = 0,
            .row_count = 4,
            .file_offset = 100,
            .total_byte_len = 256,
            .column_chunks = try alloc.dupe(external_source.ColumnChunk, &[_]external_source.ColumnChunk{
                .{
                    .column_id = try alloc.dupe(u8, "amount"),
                    .file_offset = 100,
                    .compressed_len = 96,
                    .uncompressed_len = 96,
                    .encoding = try alloc.dupe(u8, "plain"),
                },
                .{
                    .column_id = try alloc.dupe(u8, "tenant"),
                    .file_offset = 196,
                    .compressed_len = 96,
                    .uncompressed_len = 96,
                    .encoding = try alloc.dupe(u8, "rle_dictionary"),
                },
            }),
        }}),
    };
    try inventory.validate();

    var amount_chunk = std.ArrayListUnmanaged(u8).empty;
    defer amount_chunk.deinit(alloc);
    try appendPlainI64DataPage(&amount_chunk, alloc, &[_]i64{ 10, 20, 30, 40 });
    var tenant_chunk = std.ArrayListUnmanaged(u8).empty;
    defer tenant_chunk.deinit(alloc);
    try appendPlainI64DictionaryPage(&tenant_chunk, alloc, &[_]i64{ 7, 8 });
    try appendDictionaryI64DataPage(&tenant_chunk, alloc, 4, 1, &[_]u8{ 3, 0b00001010 });

    var owned = try buildSupportedI64RowGroupBatchAlloc(alloc, inventory, "part-a.parquet", 0, &[_]ColumnChunkInput{
        .{ .column_id = "amount", .bytes = amount_chunk.items },
        .{ .column_id = "tenant", .bytes = tenant_chunk.items },
    });
    defer owned.deinit(alloc);

    try owned.batch.validate();
    try std.testing.expectEqual(@as(usize, 4), owned.batch.rowCount());
    try std.testing.expectEqualSlices(i64, &[_]i64{ 10, 20, 30, 40 }, owned.batch.columns[0].values.i64);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 7, 8, 7, 8 }, owned.batch.columns[1].values.i64);

    var plan = try planSupportedI64ObjectRangeRowGroupsAlloc(alloc, inventory, &[_][]const u8{ "amount", "tenant" });
    defer plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), plan.row_groups.len);

    inventory.files[0].row_groups[0].column_chunks[1].encoding[0] = 'd';
    try std.testing.expectError(error.UnsupportedParquetPage, planSupportedI64ObjectRangeRowGroupsAlloc(alloc, inventory, &[_][]const u8{"tenant"}));
    try std.testing.expectError(error.UnsupportedParquetPage, buildSupportedI64RowGroupBatchAlloc(alloc, inventory, "part-a.parquet", 0, &[_]ColumnChunkInput{
        .{ .column_id = "tenant", .bytes = tenant_chunk.items },
    }));
}

test "parquet row group batch dispatches required byte array columns from inventory" {
    const alloc = std.testing.allocator;
    var inventory = external_source.Inventory{
        .format = .parquet,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/events"),
        .snapshot_id = try alloc.dupe(u8, "sha256:objects"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "part-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/events/part-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-a"),
        .byte_len = 2048,
        .row_count = 3,
        .row_groups = try alloc.dupe(external_source.RowGroup, &[_]external_source.RowGroup{.{
            .ordinal = 0,
            .row_count = 3,
            .file_offset = 100,
            .total_byte_len = 256,
            .column_chunks = try alloc.dupe(external_source.ColumnChunk, &[_]external_source.ColumnChunk{
                .{
                    .column_id = try alloc.dupe(u8, "amount"),
                    .file_offset = 100,
                    .compressed_len = 96,
                    .uncompressed_len = 96,
                    .encoding = try alloc.dupe(u8, "plain"),
                    .physical_type = try alloc.dupe(u8, "int64"),
                },
                .{
                    .column_id = try alloc.dupe(u8, "tenant"),
                    .file_offset = 196,
                    .compressed_len = 96,
                    .uncompressed_len = 96,
                    .encoding = try alloc.dupe(u8, "plain"),
                    .physical_type = try alloc.dupe(u8, "byte_array"),
                },
            }),
        }}),
    };
    try inventory.validate();

    var amount_chunk = std.ArrayListUnmanaged(u8).empty;
    defer amount_chunk.deinit(alloc);
    try appendPlainI64DataPage(&amount_chunk, alloc, &[_]i64{ 10, 20, 30 });
    var tenant_chunk = std.ArrayListUnmanaged(u8).empty;
    defer tenant_chunk.deinit(alloc);
    try appendPlainByteArrayDataPage(&tenant_chunk, alloc, &[_][]const u8{ "t1", "t2", "t2" });

    var owned = try buildSupportedI64RowGroupBatchAlloc(alloc, inventory, "part-a.parquet", 0, &[_]ColumnChunkInput{
        .{ .column_id = "amount", .bytes = amount_chunk.items },
        .{ .column_id = "tenant", .bytes = tenant_chunk.items },
    });
    defer owned.deinit(alloc);

    try owned.batch.validate();
    try std.testing.expectEqual(@as(usize, 3), owned.batch.rowCount());
    try std.testing.expectEqualSlices(i64, &[_]i64{ 10, 20, 30 }, owned.batch.columns[0].values.i64);
    try std.testing.expectEqualStrings("t1", owned.batch.columns[1].values.bytes[0]);
    try std.testing.expectEqualStrings("t2", owned.batch.columns[1].values.bytes[1]);

    const projection = [_][]const u8{"amount"};
    const chunks = [_]ColumnChunkInput{
        .{ .column_id = "amount", .bytes = amount_chunk.items },
        .{ .column_id = "tenant", .bytes = tenant_chunk.items },
    };
    const row_groups = [_]RowGroupInput{.{
        .file_id = "part-a.parquet",
        .row_group_ordinal = 0,
        .chunks = &chunks,
    }};
    var source = try RowGroupSource.init(inventory, &row_groups);
    defer source.deinit(alloc);

    var result = try lake_rows.scanRowsAlloc(alloc, source.rowSource(), .{
        .projected_columns = &projection,
        .predicate = .{
            .column = "tenant",
            .op = .eq_bytes,
            .bytes_value = "t2",
        },
    });
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), result.rows.len);
    try std.testing.expectEqual(@as(u32, 0), result.rows[0].row_ref.external.row_group_ordinal);
    try std.testing.expectEqual(@as(u64, 1), result.rows[0].row_ref.external.row_ordinal);
    try std.testing.expectEqual(@as(i64, 20), result.rows[0].find("amount").?.value.?.i64);

    inventory.files[0].row_groups[0].column_chunks[1].encoding[0] = 'd';
    try std.testing.expectError(error.UnsupportedParquetPage, planSupportedI64ObjectRangeRowGroupsAlloc(alloc, inventory, &[_][]const u8{"tenant"}));
}

test "parquet row group batch dispatches int32 columns from inventory" {
    const alloc = std.testing.allocator;
    var inventory = external_source.Inventory{
        .format = .parquet,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/events"),
        .snapshot_id = try alloc.dupe(u8, "sha256:objects"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "part-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/events/part-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-a"),
        .byte_len = 2048,
        .row_count = 3,
        .row_groups = try alloc.dupe(external_source.RowGroup, &[_]external_source.RowGroup{.{
            .ordinal = 0,
            .row_count = 3,
            .file_offset = 100,
            .total_byte_len = 256,
            .column_chunks = try alloc.dupe(external_source.ColumnChunk, &[_]external_source.ColumnChunk{
                .{
                    .column_id = try alloc.dupe(u8, "amount"),
                    .file_offset = 100,
                    .compressed_len = 64,
                    .uncompressed_len = 64,
                    .encoding = try alloc.dupe(u8, "plain"),
                    .physical_type = try alloc.dupe(u8, "int32"),
                },
                .{
                    .column_id = try alloc.dupe(u8, "rank"),
                    .file_offset = 164,
                    .compressed_len = 64,
                    .uncompressed_len = 64,
                    .encoding = try alloc.dupe(u8, "rle_dictionary"),
                    .physical_type = try alloc.dupe(u8, "int32"),
                },
            }),
        }}),
    };
    try inventory.validate();

    var amount_chunk = std.ArrayListUnmanaged(u8).empty;
    defer amount_chunk.deinit(alloc);
    try appendPlainI32DataPage(&amount_chunk, alloc, &[_]i32{ 10, 20, 30 });
    var rank_chunk = std.ArrayListUnmanaged(u8).empty;
    defer rank_chunk.deinit(alloc);
    try appendPlainI32DictionaryPage(&rank_chunk, alloc, &[_]i32{ 1, 2 });
    try appendDictionaryI64DataPage(&rank_chunk, alloc, 3, 1, &[_]u8{ 3, 0b00000110 });

    var owned = try buildSupportedI64RowGroupBatchAlloc(alloc, inventory, "part-a.parquet", 0, &[_]ColumnChunkInput{
        .{ .column_id = "amount", .bytes = amount_chunk.items },
        .{ .column_id = "rank", .bytes = rank_chunk.items },
    });
    defer owned.deinit(alloc);

    try owned.batch.validate();
    try std.testing.expectEqualSlices(i64, &[_]i64{ 10, 20, 30 }, owned.batch.columns[0].values.i64);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 2 }, owned.batch.columns[1].values.i64);

    const projection = [_][]const u8{"amount"};
    const chunks = [_]ColumnChunkInput{
        .{ .column_id = "amount", .bytes = amount_chunk.items },
        .{ .column_id = "rank", .bytes = rank_chunk.items },
    };
    const row_groups = [_]RowGroupInput{.{
        .file_id = "part-a.parquet",
        .row_group_ordinal = 0,
        .chunks = &chunks,
    }};
    var source = try RowGroupSource.init(inventory, &row_groups);
    defer source.deinit(alloc);

    var result = try lake_rows.scanRowsAlloc(alloc, source.rowSource(), .{
        .projected_columns = &projection,
        .predicate = .{
            .column = "rank",
            .op = .eq_i64,
            .i64_value = 2,
        },
    });
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), result.rows.len);
    try std.testing.expectEqual(@as(i64, 20), result.rows[0].find("amount").?.value.?.i64);
    try std.testing.expectEqual(@as(i64, 30), result.rows[1].find("amount").?.value.?.i64);
}

test "parquet row group batch dispatches nullable int32 and byte array columns from inventory" {
    const alloc = std.testing.allocator;
    var inventory = external_source.Inventory{
        .format = .parquet,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/events"),
        .snapshot_id = try alloc.dupe(u8, "sha256:objects"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "part-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/events/part-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-a"),
        .byte_len = 2048,
        .row_count = 3,
        .row_groups = try alloc.dupe(external_source.RowGroup, &[_]external_source.RowGroup{.{
            .ordinal = 0,
            .row_count = 3,
            .file_offset = 100,
            .total_byte_len = 512,
            .column_chunks = try alloc.dupe(external_source.ColumnChunk, &[_]external_source.ColumnChunk{
                .{
                    .column_id = try alloc.dupe(u8, "age"),
                    .file_offset = 100,
                    .compressed_len = 64,
                    .uncompressed_len = 64,
                    .encoding = try alloc.dupe(u8, "plain"),
                    .physical_type = try alloc.dupe(u8, "int32"),
                    .nullable = true,
                },
                .{
                    .column_id = try alloc.dupe(u8, "rank"),
                    .file_offset = 164,
                    .compressed_len = 64,
                    .uncompressed_len = 64,
                    .encoding = try alloc.dupe(u8, "rle_dictionary"),
                    .physical_type = try alloc.dupe(u8, "int32"),
                    .nullable = true,
                },
                .{
                    .column_id = try alloc.dupe(u8, "tag"),
                    .file_offset = 228,
                    .compressed_len = 96,
                    .uncompressed_len = 96,
                    .encoding = try alloc.dupe(u8, "plain"),
                    .physical_type = try alloc.dupe(u8, "byte_array"),
                    .nullable = true,
                },
                .{
                    .column_id = try alloc.dupe(u8, "tenant"),
                    .file_offset = 324,
                    .compressed_len = 96,
                    .uncompressed_len = 96,
                    .encoding = try alloc.dupe(u8, "rle_dictionary"),
                    .physical_type = try alloc.dupe(u8, "byte_array"),
                    .nullable = true,
                },
            }),
        }}),
    };
    try inventory.validate();

    var age_chunk = std.ArrayListUnmanaged(u8).empty;
    defer age_chunk.deinit(alloc);
    try appendOptionalPlainI32DataPageV2(&age_chunk, alloc, &[_]?i32{ 10, null, 30 });

    var rank_chunk = std.ArrayListUnmanaged(u8).empty;
    defer rank_chunk.deinit(alloc);
    try appendPlainI32DictionaryPage(&rank_chunk, alloc, &[_]i32{ 1, 2 });
    try appendOptionalDictionaryI64DataPageV2(&rank_chunk, alloc, &[_]?u8{ 0, null, 1 }, 1, &[_]u8{ 3, 0b00000010 });

    var tag_chunk = std.ArrayListUnmanaged(u8).empty;
    defer tag_chunk.deinit(alloc);
    try appendOptionalPlainByteArrayDataPageV2(&tag_chunk, alloc, &[_]?[]const u8{ "alpha", null, "omega" });

    var tenant_chunk = std.ArrayListUnmanaged(u8).empty;
    defer tenant_chunk.deinit(alloc);
    try appendPlainByteArrayDictionaryPage(&tenant_chunk, alloc, &[_][]const u8{ "t1", "t2" });
    try appendOptionalDictionaryI64DataPageV2(&tenant_chunk, alloc, &[_]?u8{ 0, null, 1 }, 1, &[_]u8{ 3, 0b00000010 });

    var owned = try buildSupportedI64RowGroupBatchAlloc(alloc, inventory, "part-a.parquet", 0, &[_]ColumnChunkInput{
        .{ .column_id = "age", .bytes = age_chunk.items },
        .{ .column_id = "rank", .bytes = rank_chunk.items },
        .{ .column_id = "tag", .bytes = tag_chunk.items },
        .{ .column_id = "tenant", .bytes = tenant_chunk.items },
    });
    defer owned.deinit(alloc);

    try owned.batch.validate();
    try std.testing.expectEqualSlices(i64, &[_]i64{ 10, 0, 30 }, owned.batch.columns[0].values.i64);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0, 1, 0 }, owned.batch.columns[0].nulls.bytes);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 0, 2 }, owned.batch.columns[1].values.i64);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0, 1, 0 }, owned.batch.columns[1].nulls.bytes);
    try std.testing.expectEqualStrings("alpha", owned.batch.columns[2].values.bytes[0]);
    try std.testing.expectEqualStrings("", owned.batch.columns[2].values.bytes[1]);
    try std.testing.expectEqualStrings("omega", owned.batch.columns[2].values.bytes[2]);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0, 1, 0 }, owned.batch.columns[2].nulls.bytes);
    try std.testing.expectEqualStrings("t1", owned.batch.columns[3].values.bytes[0]);
    try std.testing.expectEqualStrings("", owned.batch.columns[3].values.bytes[1]);
    try std.testing.expectEqualStrings("t2", owned.batch.columns[3].values.bytes[2]);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0, 1, 0 }, owned.batch.columns[3].nulls.bytes);

    var plan = try planSupportedI64ObjectRangeRowGroupsAlloc(alloc, inventory, &[_][]const u8{ "age", "rank", "tag", "tenant" });
    defer plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), plan.row_groups.len);
}

test "parquet row group batch dispatches double columns from inventory" {
    const alloc = std.testing.allocator;
    var inventory = external_source.Inventory{
        .format = .parquet,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/events"),
        .snapshot_id = try alloc.dupe(u8, "sha256:objects"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "part-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/events/part-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-a"),
        .byte_len = 2048,
        .row_count = 3,
        .row_groups = try alloc.dupe(external_source.RowGroup, &[_]external_source.RowGroup{.{
            .ordinal = 0,
            .row_count = 3,
            .file_offset = 100,
            .total_byte_len = 384,
            .column_chunks = try alloc.dupe(external_source.ColumnChunk, &[_]external_source.ColumnChunk{
                .{
                    .column_id = try alloc.dupe(u8, "score"),
                    .file_offset = 100,
                    .compressed_len = 96,
                    .uncompressed_len = 96,
                    .encoding = try alloc.dupe(u8, "plain"),
                    .physical_type = try alloc.dupe(u8, "double"),
                },
                .{
                    .column_id = try alloc.dupe(u8, "ratio"),
                    .file_offset = 196,
                    .compressed_len = 96,
                    .uncompressed_len = 96,
                    .encoding = try alloc.dupe(u8, "plain"),
                    .physical_type = try alloc.dupe(u8, "double"),
                    .nullable = true,
                },
                .{
                    .column_id = try alloc.dupe(u8, "weight"),
                    .file_offset = 292,
                    .compressed_len = 96,
                    .uncompressed_len = 96,
                    .encoding = try alloc.dupe(u8, "rle_dictionary"),
                    .physical_type = try alloc.dupe(u8, "double"),
                },
                .{
                    .column_id = try alloc.dupe(u8, "confidence"),
                    .file_offset = 388,
                    .compressed_len = 96,
                    .uncompressed_len = 96,
                    .encoding = try alloc.dupe(u8, "rle_dictionary"),
                    .physical_type = try alloc.dupe(u8, "double"),
                    .nullable = true,
                },
            }),
        }}),
    };
    try inventory.validate();

    var score_chunk = std.ArrayListUnmanaged(u8).empty;
    defer score_chunk.deinit(alloc);
    try appendPlainF64DataPage(&score_chunk, alloc, &[_]f64{ 1.5, 2.25, 3.75 });

    var ratio_chunk = std.ArrayListUnmanaged(u8).empty;
    defer ratio_chunk.deinit(alloc);
    try appendOptionalPlainF64DataPageV2(&ratio_chunk, alloc, &[_]?f64{ 0.5, null, 0.75 });

    var weight_chunk = std.ArrayListUnmanaged(u8).empty;
    defer weight_chunk.deinit(alloc);
    try appendPlainF64DictionaryPage(&weight_chunk, alloc, &[_]f64{ 1.25, 2.5 });
    try appendDictionaryI64DataPage(&weight_chunk, alloc, 3, 1, &[_]u8{ 3, 0b00000110 });

    var confidence_chunk = std.ArrayListUnmanaged(u8).empty;
    defer confidence_chunk.deinit(alloc);
    try appendPlainF64DictionaryPage(&confidence_chunk, alloc, &[_]f64{ 0.125, 0.5 });
    try appendOptionalDictionaryI64DataPageV2(&confidence_chunk, alloc, &[_]?u8{ 0, null, 1 }, 1, &[_]u8{ 3, 0b00000010 });

    var owned = try buildSupportedI64RowGroupBatchAlloc(alloc, inventory, "part-a.parquet", 0, &[_]ColumnChunkInput{
        .{ .column_id = "score", .bytes = score_chunk.items },
        .{ .column_id = "ratio", .bytes = ratio_chunk.items },
        .{ .column_id = "weight", .bytes = weight_chunk.items },
        .{ .column_id = "confidence", .bytes = confidence_chunk.items },
    });
    defer owned.deinit(alloc);

    try owned.batch.validate();
    try std.testing.expectEqualSlices(f64, &[_]f64{ 1.5, 2.25, 3.75 }, owned.batch.columns[0].values.f64);
    try std.testing.expectEqualSlices(f64, &[_]f64{ 0.5, 0, 0.75 }, owned.batch.columns[1].values.f64);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0, 1, 0 }, owned.batch.columns[1].nulls.bytes);
    try std.testing.expectEqualSlices(f64, &[_]f64{ 1.25, 2.5, 2.5 }, owned.batch.columns[2].values.f64);
    try std.testing.expectEqualSlices(f64, &[_]f64{ 0.125, 0, 0.5 }, owned.batch.columns[3].values.f64);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0, 1, 0 }, owned.batch.columns[3].nulls.bytes);

    var plan = try planSupportedI64ObjectRangeRowGroupsAlloc(alloc, inventory, &[_][]const u8{ "score", "ratio", "weight", "confidence" });
    defer plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), plan.row_groups.len);
}

test "parquet row group batch dispatches float columns from inventory" {
    const alloc = std.testing.allocator;
    var inventory = external_source.Inventory{
        .format = .parquet,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/events"),
        .snapshot_id = try alloc.dupe(u8, "sha256:objects"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "part-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/events/part-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-a"),
        .byte_len = 2048,
        .row_count = 3,
        .row_groups = try alloc.dupe(external_source.RowGroup, &[_]external_source.RowGroup{.{
            .ordinal = 0,
            .row_count = 3,
            .file_offset = 100,
            .total_byte_len = 384,
            .column_chunks = try alloc.dupe(external_source.ColumnChunk, &[_]external_source.ColumnChunk{
                .{
                    .column_id = try alloc.dupe(u8, "score"),
                    .file_offset = 100,
                    .compressed_len = 96,
                    .uncompressed_len = 96,
                    .encoding = try alloc.dupe(u8, "plain"),
                    .physical_type = try alloc.dupe(u8, "float"),
                },
                .{
                    .column_id = try alloc.dupe(u8, "ratio"),
                    .file_offset = 196,
                    .compressed_len = 96,
                    .uncompressed_len = 96,
                    .encoding = try alloc.dupe(u8, "plain"),
                    .physical_type = try alloc.dupe(u8, "float"),
                    .nullable = true,
                },
                .{
                    .column_id = try alloc.dupe(u8, "weight"),
                    .file_offset = 292,
                    .compressed_len = 96,
                    .uncompressed_len = 96,
                    .encoding = try alloc.dupe(u8, "rle_dictionary"),
                    .physical_type = try alloc.dupe(u8, "float"),
                },
                .{
                    .column_id = try alloc.dupe(u8, "confidence"),
                    .file_offset = 388,
                    .compressed_len = 96,
                    .uncompressed_len = 96,
                    .encoding = try alloc.dupe(u8, "rle_dictionary"),
                    .physical_type = try alloc.dupe(u8, "float"),
                    .nullable = true,
                },
            }),
        }}),
    };
    try inventory.validate();

    var score_chunk = std.ArrayListUnmanaged(u8).empty;
    defer score_chunk.deinit(alloc);
    try appendPlainF32DataPage(&score_chunk, alloc, &[_]f32{ 1.25, 2.5, 3.75 });

    var ratio_chunk = std.ArrayListUnmanaged(u8).empty;
    defer ratio_chunk.deinit(alloc);
    try appendOptionalPlainF32DataPageV2(&ratio_chunk, alloc, &[_]?f32{ 0.5, null, 0.75 });

    var weight_chunk = std.ArrayListUnmanaged(u8).empty;
    defer weight_chunk.deinit(alloc);
    try appendPlainF32DictionaryPage(&weight_chunk, alloc, &[_]f32{ 1.25, 2.5 });
    try appendDictionaryI64DataPage(&weight_chunk, alloc, 3, 1, &[_]u8{ 3, 0b00000110 });

    var confidence_chunk = std.ArrayListUnmanaged(u8).empty;
    defer confidence_chunk.deinit(alloc);
    try appendPlainF32DictionaryPage(&confidence_chunk, alloc, &[_]f32{ 0.125, 0.5 });
    try appendOptionalDictionaryI64DataPageV2(&confidence_chunk, alloc, &[_]?u8{ 0, null, 1 }, 1, &[_]u8{ 3, 0b00000010 });

    var owned = try buildSupportedI64RowGroupBatchAlloc(alloc, inventory, "part-a.parquet", 0, &[_]ColumnChunkInput{
        .{ .column_id = "score", .bytes = score_chunk.items },
        .{ .column_id = "ratio", .bytes = ratio_chunk.items },
        .{ .column_id = "weight", .bytes = weight_chunk.items },
        .{ .column_id = "confidence", .bytes = confidence_chunk.items },
    });
    defer owned.deinit(alloc);

    try owned.batch.validate();
    try std.testing.expectEqualSlices(f64, &[_]f64{ 1.25, 2.5, 3.75 }, owned.batch.columns[0].values.f64);
    try std.testing.expectEqualSlices(f64, &[_]f64{ 0.5, 0, 0.75 }, owned.batch.columns[1].values.f64);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0, 1, 0 }, owned.batch.columns[1].nulls.bytes);
    try std.testing.expectEqualSlices(f64, &[_]f64{ 1.25, 2.5, 2.5 }, owned.batch.columns[2].values.f64);
    try std.testing.expectEqualSlices(f64, &[_]f64{ 0.125, 0, 0.5 }, owned.batch.columns[3].values.f64);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0, 1, 0 }, owned.batch.columns[3].nulls.bytes);

    var plan = try planSupportedI64ObjectRangeRowGroupsAlloc(alloc, inventory, &[_][]const u8{ "score", "ratio", "weight", "confidence" });
    defer plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), plan.row_groups.len);
}

test "parquet row group batch dispatches boolean columns from inventory" {
    const alloc = std.testing.allocator;
    var inventory = external_source.Inventory{
        .format = .parquet,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/events"),
        .snapshot_id = try alloc.dupe(u8, "sha256:objects"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "part-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/events/part-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-a"),
        .byte_len = 2048,
        .row_count = 3,
        .row_groups = try alloc.dupe(external_source.RowGroup, &[_]external_source.RowGroup{.{
            .ordinal = 0,
            .row_count = 3,
            .file_offset = 100,
            .total_byte_len = 192,
            .column_chunks = try alloc.dupe(external_source.ColumnChunk, &[_]external_source.ColumnChunk{
                .{
                    .column_id = try alloc.dupe(u8, "active"),
                    .file_offset = 100,
                    .compressed_len = 96,
                    .uncompressed_len = 96,
                    .encoding = try alloc.dupe(u8, "plain"),
                    .physical_type = try alloc.dupe(u8, "boolean"),
                },
                .{
                    .column_id = try alloc.dupe(u8, "flagged"),
                    .file_offset = 196,
                    .compressed_len = 96,
                    .uncompressed_len = 96,
                    .encoding = try alloc.dupe(u8, "plain"),
                    .physical_type = try alloc.dupe(u8, "boolean"),
                    .nullable = true,
                },
            }),
        }}),
    };
    try inventory.validate();

    var active_chunk = std.ArrayListUnmanaged(u8).empty;
    defer active_chunk.deinit(alloc);
    try appendPlainBoolDataPage(&active_chunk, alloc, &[_]bool{ true, false, true });

    var flagged_chunk = std.ArrayListUnmanaged(u8).empty;
    defer flagged_chunk.deinit(alloc);
    try appendOptionalPlainBoolDataPageV2(&flagged_chunk, alloc, &[_]?bool{ false, null, true });

    var owned = try buildSupportedI64RowGroupBatchAlloc(alloc, inventory, "part-a.parquet", 0, &[_]ColumnChunkInput{
        .{ .column_id = "active", .bytes = active_chunk.items },
        .{ .column_id = "flagged", .bytes = flagged_chunk.items },
    });
    defer owned.deinit(alloc);

    try owned.batch.validate();
    try std.testing.expectEqualSlices(bool, &[_]bool{ true, false, true }, owned.batch.columns[0].values.bool);
    try std.testing.expectEqualSlices(bool, &[_]bool{ false, false, true }, owned.batch.columns[1].values.bool);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0, 1, 0 }, owned.batch.columns[1].nulls.bytes);

    var plan = try planSupportedI64ObjectRangeRowGroupsAlloc(alloc, inventory, &[_][]const u8{ "active", "flagged" });
    defer plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), plan.row_groups.len);
}

test "parquet row group batch dispatches int96 timestamp columns from inventory" {
    const alloc = std.testing.allocator;
    var inventory = external_source.Inventory{
        .format = .parquet,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/events"),
        .snapshot_id = try alloc.dupe(u8, "sha256:objects"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "part-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/events/part-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-a"),
        .byte_len = 2048,
        .row_count = 3,
        .row_groups = try alloc.dupe(external_source.RowGroup, &[_]external_source.RowGroup{.{
            .ordinal = 0,
            .row_count = 3,
            .file_offset = 100,
            .total_byte_len = 192,
            .column_chunks = try alloc.dupe(external_source.ColumnChunk, &[_]external_source.ColumnChunk{
                .{
                    .column_id = try alloc.dupe(u8, "created_at"),
                    .file_offset = 100,
                    .compressed_len = 96,
                    .uncompressed_len = 96,
                    .encoding = try alloc.dupe(u8, "plain"),
                    .physical_type = try alloc.dupe(u8, "int96"),
                },
                .{
                    .column_id = try alloc.dupe(u8, "processed_at"),
                    .file_offset = 196,
                    .compressed_len = 96,
                    .uncompressed_len = 96,
                    .encoding = try alloc.dupe(u8, "plain"),
                    .physical_type = try alloc.dupe(u8, "int96"),
                    .nullable = true,
                },
            }),
        }}),
    };
    try inventory.validate();

    var created_chunk = std.ArrayListUnmanaged(u8).empty;
    defer created_chunk.deinit(alloc);
    try appendPlainInt96TimestampDataPage(&created_chunk, alloc, &[_]TestInt96Timestamp{
        .{ .nanos_of_day = 1_000_000_000, .julian_day = 2_440_588 },
        .{ .nanos_of_day = 2_000_000_000, .julian_day = 2_440_589 },
        .{ .nanos_of_day = 3_000_000_000, .julian_day = 2_440_590 },
    });

    var processed_chunk = std.ArrayListUnmanaged(u8).empty;
    defer processed_chunk.deinit(alloc);
    try appendOptionalPlainInt96TimestampDataPageV2(&processed_chunk, alloc, &[_]?TestInt96Timestamp{
        .{ .nanos_of_day = 4_000_000_000, .julian_day = 2_440_588 },
        null,
        .{ .nanos_of_day = 5_000_000_000, .julian_day = 2_440_589 },
    });

    var owned = try buildSupportedI64RowGroupBatchAlloc(alloc, inventory, "part-a.parquet", 0, &[_]ColumnChunkInput{
        .{ .column_id = "created_at", .bytes = created_chunk.items },
        .{ .column_id = "processed_at", .bytes = processed_chunk.items },
    });
    defer owned.deinit(alloc);

    try owned.batch.validate();
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1_000_000_000, 86_402_000_000_000, 172_803_000_000_000 }, owned.batch.columns[0].values.i64);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 4_000_000_000, 0, 86_405_000_000_000 }, owned.batch.columns[1].values.i64);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0, 1, 0 }, owned.batch.columns[1].nulls.bytes);

    var plan = try planSupportedI64ObjectRangeRowGroupsAlloc(alloc, inventory, &[_][]const u8{ "created_at", "processed_at" });
    defer plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), plan.row_groups.len);
}

test "parquet row group batch dispatches logical int64 timestamp columns from inventory" {
    const alloc = std.testing.allocator;
    var inventory = external_source.Inventory{
        .format = .parquet,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/events"),
        .snapshot_id = try alloc.dupe(u8, "sha256:objects"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "part-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/events/part-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-a"),
        .byte_len = 2048,
        .row_count = 3,
        .row_groups = try alloc.dupe(external_source.RowGroup, &[_]external_source.RowGroup{.{
            .ordinal = 0,
            .row_count = 3,
            .file_offset = 100,
            .total_byte_len = 288,
            .column_chunks = try alloc.dupe(external_source.ColumnChunk, &[_]external_source.ColumnChunk{
                .{
                    .column_id = try alloc.dupe(u8, "created_ms"),
                    .file_offset = 100,
                    .compressed_len = 96,
                    .uncompressed_len = 96,
                    .encoding = try alloc.dupe(u8, "plain"),
                    .physical_type = try alloc.dupe(u8, "int64"),
                    .logical_type = try alloc.dupe(u8, "timestamp_millis"),
                },
                .{
                    .column_id = try alloc.dupe(u8, "processed_us"),
                    .file_offset = 196,
                    .compressed_len = 96,
                    .uncompressed_len = 96,
                    .encoding = try alloc.dupe(u8, "plain"),
                    .physical_type = try alloc.dupe(u8, "int64"),
                    .logical_type = try alloc.dupe(u8, "timestamp_micros"),
                    .nullable = true,
                },
                .{
                    .column_id = try alloc.dupe(u8, "event_ns"),
                    .file_offset = 292,
                    .compressed_len = 96,
                    .uncompressed_len = 96,
                    .encoding = try alloc.dupe(u8, "rle_dictionary"),
                    .physical_type = try alloc.dupe(u8, "int64"),
                    .logical_type = try alloc.dupe(u8, "timestamp_nanos"),
                },
            }),
        }}),
    };
    try inventory.validate();

    var created_chunk = std.ArrayListUnmanaged(u8).empty;
    defer created_chunk.deinit(alloc);
    try appendPlainI64DataPage(&created_chunk, alloc, &[_]i64{ 1, 2, 3 });

    var processed_chunk = std.ArrayListUnmanaged(u8).empty;
    defer processed_chunk.deinit(alloc);
    try appendOptionalPlainI64DataPageV2(&processed_chunk, alloc, &[_]?i64{ 4, null, 5 });

    var event_chunk = std.ArrayListUnmanaged(u8).empty;
    defer event_chunk.deinit(alloc);
    try appendPlainI64DictionaryPage(&event_chunk, alloc, &[_]i64{ 6, 7 });
    try appendDictionaryI64DataPage(&event_chunk, alloc, 3, 1, &[_]u8{ 3, 0b00000110 });

    var owned = try buildSupportedI64RowGroupBatchAlloc(alloc, inventory, "part-a.parquet", 0, &[_]ColumnChunkInput{
        .{ .column_id = "created_ms", .bytes = created_chunk.items },
        .{ .column_id = "processed_us", .bytes = processed_chunk.items },
        .{ .column_id = "event_ns", .bytes = event_chunk.items },
    });
    defer owned.deinit(alloc);

    try owned.batch.validate();
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1_000_000, 2_000_000, 3_000_000 }, owned.batch.columns[0].values.i64);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 4_000, 0, 5_000 }, owned.batch.columns[1].values.i64);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0, 1, 0 }, owned.batch.columns[1].nulls.bytes);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 6, 7, 7 }, owned.batch.columns[2].values.i64);

    var plan = try planSupportedI64ObjectRangeRowGroupsAlloc(alloc, inventory, &[_][]const u8{ "created_ms", "processed_us", "event_ns" });
    defer plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), plan.row_groups.len);
}

test "parquet row group batch dispatches decimal columns from inventory" {
    const alloc = std.testing.allocator;
    var inventory = external_source.Inventory{
        .format = .parquet,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/events"),
        .snapshot_id = try alloc.dupe(u8, "sha256:objects"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "part-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/events/part-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-a"),
        .byte_len = 4096,
        .row_count = 3,
        .row_groups = try alloc.dupe(external_source.RowGroup, &[_]external_source.RowGroup{.{
            .ordinal = 0,
            .row_count = 3,
            .file_offset = 100,
            .total_byte_len = 480,
            .column_chunks = try alloc.dupe(external_source.ColumnChunk, &[_]external_source.ColumnChunk{
                .{
                    .column_id = try alloc.dupe(u8, "price"),
                    .file_offset = 100,
                    .compressed_len = 96,
                    .uncompressed_len = 96,
                    .encoding = try alloc.dupe(u8, "plain"),
                    .physical_type = try alloc.dupe(u8, "int32"),
                    .logical_type = try alloc.dupe(u8, "decimal"),
                    .decimal_precision = 9,
                    .decimal_scale = 2,
                },
                .{
                    .column_id = try alloc.dupe(u8, "discount"),
                    .file_offset = 196,
                    .compressed_len = 96,
                    .uncompressed_len = 96,
                    .encoding = try alloc.dupe(u8, "plain"),
                    .physical_type = try alloc.dupe(u8, "int64"),
                    .logical_type = try alloc.dupe(u8, "decimal"),
                    .decimal_precision = 12,
                    .decimal_scale = 3,
                    .nullable = true,
                },
                .{
                    .column_id = try alloc.dupe(u8, "tax"),
                    .file_offset = 292,
                    .compressed_len = 96,
                    .uncompressed_len = 96,
                    .encoding = try alloc.dupe(u8, "rle_dictionary"),
                    .physical_type = try alloc.dupe(u8, "int64"),
                    .logical_type = try alloc.dupe(u8, "decimal"),
                    .decimal_precision = 9,
                    .decimal_scale = 2,
                },
                .{
                    .column_id = try alloc.dupe(u8, "rebate"),
                    .file_offset = 388,
                    .compressed_len = 96,
                    .uncompressed_len = 96,
                    .encoding = try alloc.dupe(u8, "plain"),
                    .physical_type = try alloc.dupe(u8, "byte_array"),
                    .logical_type = try alloc.dupe(u8, "decimal"),
                    .decimal_precision = 9,
                    .decimal_scale = 2,
                },
                .{
                    .column_id = try alloc.dupe(u8, "fee"),
                    .file_offset = 484,
                    .compressed_len = 96,
                    .uncompressed_len = 96,
                    .encoding = try alloc.dupe(u8, "plain"),
                    .physical_type = try alloc.dupe(u8, "fixed_len_byte_array"),
                    .type_length = 4,
                    .logical_type = try alloc.dupe(u8, "decimal"),
                    .decimal_precision = 9,
                    .decimal_scale = 2,
                    .nullable = true,
                },
            }),
        }}),
    };
    try inventory.validate();

    var price_chunk = std.ArrayListUnmanaged(u8).empty;
    defer price_chunk.deinit(alloc);
    try appendPlainI32DataPage(&price_chunk, alloc, &[_]i32{ 1234, -250, 0 });

    var discount_chunk = std.ArrayListUnmanaged(u8).empty;
    defer discount_chunk.deinit(alloc);
    try appendOptionalPlainI64DataPageV2(&discount_chunk, alloc, &[_]?i64{ 1250, null, -500 });

    var tax_chunk = std.ArrayListUnmanaged(u8).empty;
    defer tax_chunk.deinit(alloc);
    try appendPlainI64DictionaryPage(&tax_chunk, alloc, &[_]i64{ 75, 125 });
    try appendDictionaryI64DataPage(&tax_chunk, alloc, 3, 1, &[_]u8{ 3, 0b00000110 });

    var rebate_chunk = std.ArrayListUnmanaged(u8).empty;
    defer rebate_chunk.deinit(alloc);
    try appendPlainByteArrayDataPage(&rebate_chunk, alloc, &[_][]const u8{
        &[_]u8{ 0x04, 0xd2 },
        &[_]u8{ 0xff, 0x06 },
        &[_]u8{0x00},
    });

    var fee_chunk = std.ArrayListUnmanaged(u8).empty;
    defer fee_chunk.deinit(alloc);
    try appendOptionalPlainFixedLenByteArrayDataPageV2(&fee_chunk, alloc, &[_]?[]const u8{
        &[_]u8{ 0x00, 0x00, 0x04, 0xd2 },
        null,
        &[_]u8{ 0xff, 0xff, 0xff, 0x06 },
    });

    var owned = try buildSupportedI64RowGroupBatchAlloc(alloc, inventory, "part-a.parquet", 0, &[_]ColumnChunkInput{
        .{ .column_id = "price", .bytes = price_chunk.items },
        .{ .column_id = "discount", .bytes = discount_chunk.items },
        .{ .column_id = "tax", .bytes = tax_chunk.items },
        .{ .column_id = "rebate", .bytes = rebate_chunk.items },
        .{ .column_id = "fee", .bytes = fee_chunk.items },
    });
    defer owned.deinit(alloc);

    try owned.batch.validate();
    try std.testing.expectEqualSlices(f64, &[_]f64{ 12.34, -2.5, 0 }, owned.batch.columns[0].values.f64);
    try std.testing.expectEqualSlices(f64, &[_]f64{ 1.25, 0, -0.5 }, owned.batch.columns[1].values.f64);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0, 1, 0 }, owned.batch.columns[1].nulls.bytes);
    try std.testing.expectEqualSlices(f64, &[_]f64{ 0.75, 1.25, 1.25 }, owned.batch.columns[2].values.f64);
    try std.testing.expectEqualSlices(f64, &[_]f64{ 12.34, -2.5, 0 }, owned.batch.columns[3].values.f64);
    try std.testing.expectEqualSlices(f64, &[_]f64{ 12.34, 0, -2.5 }, owned.batch.columns[4].values.f64);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0, 1, 0 }, owned.batch.columns[4].nulls.bytes);

    var plan = try planSupportedI64ObjectRangeRowGroupsAlloc(alloc, inventory, &[_][]const u8{ "price", "discount", "tax", "rebate", "fee" });
    defer plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), plan.row_groups.len);
}

test "parquet row group batch dispatches dictionary byte array columns from inventory" {
    const alloc = std.testing.allocator;
    var inventory = external_source.Inventory{
        .format = .parquet,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/events"),
        .snapshot_id = try alloc.dupe(u8, "sha256:objects"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "part-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/events/part-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-a"),
        .byte_len = 2048,
        .row_count = 3,
        .row_groups = try alloc.dupe(external_source.RowGroup, &[_]external_source.RowGroup{.{
            .ordinal = 0,
            .row_count = 3,
            .file_offset = 100,
            .total_byte_len = 256,
            .column_chunks = try alloc.dupe(external_source.ColumnChunk, &[_]external_source.ColumnChunk{
                .{
                    .column_id = try alloc.dupe(u8, "amount"),
                    .file_offset = 100,
                    .compressed_len = 96,
                    .uncompressed_len = 96,
                    .encoding = try alloc.dupe(u8, "plain"),
                    .physical_type = try alloc.dupe(u8, "int64"),
                },
                .{
                    .column_id = try alloc.dupe(u8, "tenant"),
                    .file_offset = 196,
                    .compressed_len = 96,
                    .uncompressed_len = 96,
                    .encoding = try alloc.dupe(u8, "rle_dictionary"),
                    .physical_type = try alloc.dupe(u8, "byte_array"),
                },
            }),
        }}),
    };
    try inventory.validate();

    var amount_chunk = std.ArrayListUnmanaged(u8).empty;
    defer amount_chunk.deinit(alloc);
    try appendPlainI64DataPage(&amount_chunk, alloc, &[_]i64{ 10, 20, 30 });
    var tenant_chunk = std.ArrayListUnmanaged(u8).empty;
    defer tenant_chunk.deinit(alloc);
    try appendPlainByteArrayDictionaryPage(&tenant_chunk, alloc, &[_][]const u8{ "t1", "t2" });
    try appendDictionaryI64DataPage(&tenant_chunk, alloc, 3, 1, &[_]u8{ 3, 0b00000110 });

    var owned = try buildSupportedI64RowGroupBatchAlloc(alloc, inventory, "part-a.parquet", 0, &[_]ColumnChunkInput{
        .{ .column_id = "amount", .bytes = amount_chunk.items },
        .{ .column_id = "tenant", .bytes = tenant_chunk.items },
    });
    defer owned.deinit(alloc);

    try owned.batch.validate();
    try std.testing.expectEqual(@as(usize, 3), owned.batch.rowCount());
    try std.testing.expectEqualStrings("t1", owned.batch.columns[1].values.bytes[0]);
    try std.testing.expectEqualStrings("t2", owned.batch.columns[1].values.bytes[1]);
    try std.testing.expectEqualStrings("t2", owned.batch.columns[1].values.bytes[2]);

    const projection = [_][]const u8{"amount"};
    const chunks = [_]ColumnChunkInput{
        .{ .column_id = "amount", .bytes = amount_chunk.items },
        .{ .column_id = "tenant", .bytes = tenant_chunk.items },
    };
    const row_groups = [_]RowGroupInput{.{
        .file_id = "part-a.parquet",
        .row_group_ordinal = 0,
        .chunks = &chunks,
    }};
    var source = try RowGroupSource.init(inventory, &row_groups);
    defer source.deinit(alloc);

    var result = try lake_rows.scanRowsAlloc(alloc, source.rowSource(), .{
        .projected_columns = &projection,
        .predicate = .{
            .column = "tenant",
            .op = .eq_bytes,
            .bytes_value = "t2",
        },
    });
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), result.rows.len);
    try std.testing.expectEqual(@as(i64, 20), result.rows[0].find("amount").?.value.?.i64);
    try std.testing.expectEqual(@as(i64, 30), result.rows[1].find("amount").?.value.?.i64);
    try std.testing.expectEqual(@as(u64, 1), result.rows[0].row_ref.external.row_ordinal);
    try std.testing.expectEqual(@as(u64, 2), result.rows[1].row_ref.external.row_ordinal);
}

test "parquet row group batch dispatches supported compression from inventory" {
    const alloc = std.testing.allocator;
    var inventory = external_source.Inventory{
        .format = .parquet,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/events"),
        .snapshot_id = try alloc.dupe(u8, "sha256:objects"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "part-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/events/part-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-a"),
        .byte_len = 1024,
        .row_count = 3,
        .row_groups = try alloc.dupe(external_source.RowGroup, &[_]external_source.RowGroup{.{
            .ordinal = 0,
            .row_count = 3,
            .file_offset = 100,
            .total_byte_len = 96,
            .column_chunks = try alloc.dupe(external_source.ColumnChunk, &[_]external_source.ColumnChunk{
                .{
                    .column_id = try alloc.dupe(u8, "amount"),
                    .file_offset = 100,
                    .compressed_len = 96,
                    .uncompressed_len = 96,
                    .compression_codec = try alloc.dupe(u8, "snappy"),
                    .encoding = try alloc.dupe(u8, "plain"),
                },
                .{
                    .column_id = try alloc.dupe(u8, "count"),
                    .file_offset = 196,
                    .compressed_len = 96,
                    .uncompressed_len = 96,
                    .compression_codec = try alloc.dupe(u8, "gzip"),
                    .encoding = try alloc.dupe(u8, "plain"),
                },
                .{
                    .column_id = try alloc.dupe(u8, "zcount"),
                    .file_offset = 292,
                    .compressed_len = 96,
                    .uncompressed_len = 96,
                    .compression_codec = try alloc.dupe(u8, "zstd"),
                    .encoding = try alloc.dupe(u8, "plain"),
                },
            }),
        }}),
    };
    try inventory.validate();

    var amount_chunk = std.ArrayListUnmanaged(u8).empty;
    defer amount_chunk.deinit(alloc);
    try appendSnappyPlainI64DataPage(&amount_chunk, alloc, &[_]i64{ 10, 20, 30 });
    var count_chunk = std.ArrayListUnmanaged(u8).empty;
    defer count_chunk.deinit(alloc);
    try appendGzipPlainI64DataPage(&count_chunk, alloc, &[_]i64{ 1, 2, 3 });
    var zcount_chunk = std.ArrayListUnmanaged(u8).empty;
    defer zcount_chunk.deinit(alloc);
    try appendZstdPlainI64FixtureDataPage(&zcount_chunk, alloc);

    var owned = try buildSupportedI64RowGroupBatchAlloc(alloc, inventory, "part-a.parquet", 0, &[_]ColumnChunkInput{
        .{
            .column_id = "amount",
            .bytes = amount_chunk.items,
        },
        .{
            .column_id = "count",
            .bytes = count_chunk.items,
        },
        .{
            .column_id = "zcount",
            .bytes = zcount_chunk.items,
        },
    });
    defer owned.deinit(alloc);

    try owned.batch.validate();
    try std.testing.expectEqualSlices(i64, &[_]i64{ 10, 20, 30 }, owned.batch.columns[0].values.i64);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 3 }, owned.batch.columns[1].values.i64);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 3 }, owned.batch.columns[2].values.i64);

    var plan = try planSupportedI64ObjectRangeRowGroupsAlloc(alloc, inventory, &[_][]const u8{ "amount", "count", "zcount" });
    defer plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), plan.row_groups.len);

    @memcpy(inventory.files[0].row_groups[0].column_chunks[0].compression_codec[0..6], "brotli");
    try std.testing.expectError(error.UnsupportedParquetPage, planSupportedI64ObjectRangeRowGroupsAlloc(alloc, inventory, &[_][]const u8{"amount"}));
    try std.testing.expectError(error.UnsupportedParquetPage, buildSupportedI64RowGroupBatchAlloc(alloc, inventory, "part-a.parquet", 0, &[_]ColumnChunkInput{.{
        .column_id = "amount",
        .bytes = amount_chunk.items,
    }}));
}

test "parquet row group source scans through lake rows" {
    const alloc = std.testing.allocator;

    var inventory = external_source.Inventory{
        .format = .parquet,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/events"),
        .snapshot_id = try alloc.dupe(u8, "sha256:objects"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "part-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/events/part-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-a"),
        .byte_len = 2048,
        .row_count = 4,
        .row_groups = try alloc.dupe(external_source.RowGroup, &[_]external_source.RowGroup{
            .{
                .ordinal = 0,
                .row_count = 2,
                .file_offset = 100,
                .total_byte_len = 64,
                .column_chunks = try alloc.dupe(external_source.ColumnChunk, &[_]external_source.ColumnChunk{.{
                    .column_id = try alloc.dupe(u8, "amount"),
                    .file_offset = 100,
                    .compressed_len = 64,
                    .uncompressed_len = 64,
                    .encoding = try alloc.dupe(u8, "plain"),
                }}),
            },
            .{
                .ordinal = 1,
                .row_count = 2,
                .file_offset = 200,
                .total_byte_len = 64,
                .column_chunks = try alloc.dupe(external_source.ColumnChunk, &[_]external_source.ColumnChunk{.{
                    .column_id = try alloc.dupe(u8, "amount"),
                    .file_offset = 200,
                    .compressed_len = 64,
                    .uncompressed_len = 64,
                    .encoding = try alloc.dupe(u8, "rle_dictionary"),
                }}),
            },
        }),
    };
    try inventory.validate();

    var first_chunk = std.ArrayListUnmanaged(u8).empty;
    defer first_chunk.deinit(alloc);
    try appendPlainI64DataPage(&first_chunk, alloc, &[_]i64{ 10, 20 });
    var second_chunk = std.ArrayListUnmanaged(u8).empty;
    defer second_chunk.deinit(alloc);
    try appendPlainI64DictionaryPage(&second_chunk, alloc, &[_]i64{ 30, 40 });
    try appendDictionaryI64DataPage(&second_chunk, alloc, 2, 1, &[_]u8{ 3, 0b00000010 });

    const first_chunks = [_]ColumnChunkInput{.{ .column_id = "amount", .bytes = first_chunk.items }};
    const second_chunks = [_]ColumnChunkInput{.{ .column_id = "amount", .bytes = second_chunk.items }};
    const row_groups = [_]RowGroupInput{
        .{ .file_id = "part-a.parquet", .row_group_ordinal = 0, .chunks = &first_chunks },
        .{ .file_id = "part-a.parquet", .row_group_ordinal = 1, .chunks = &second_chunks },
    };
    var source = try RowGroupSource.init(inventory, &row_groups);
    defer source.deinit(alloc);

    const projection = [_][]const u8{"amount"};
    var result = try lake_rows.scanRowsAlloc(alloc, source.rowSource(), .{
        .projected_columns = &projection,
        .predicate = .{
            .column = "amount",
            .op = .eq_i64,
            .i64_value = 30,
        },
    });
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expectEqual(@as(i64, 30), result.rows[0].find("amount").?.value.?.i64);
    const row_ref = result.rows[0].row_ref.external;
    try std.testing.expectEqualStrings("part-a.parquet", row_ref.file_id);
    try std.testing.expectEqual(@as(u32, 1), row_ref.row_group_ordinal);
    try std.testing.expectEqual(@as(u64, 0), row_ref.row_ordinal);
}

test "parquet object range row group source reads chunks into lake rows" {
    const alloc = std.testing.allocator;

    var first_chunk = std.ArrayListUnmanaged(u8).empty;
    defer first_chunk.deinit(alloc);
    try appendPlainI64DataPage(&first_chunk, alloc, &[_]i64{ 10, 20 });
    var second_chunk = std.ArrayListUnmanaged(u8).empty;
    defer second_chunk.deinit(alloc);
    try appendPlainI64DictionaryPage(&second_chunk, alloc, &[_]i64{ 30, 40 });
    try appendDictionaryI64DataPage(&second_chunk, alloc, 2, 1, &[_]u8{ 3, 0b00000010 });

    const first_offset: usize = 100;
    const second_offset: usize = 200;
    const object_len = second_offset + second_chunk.items.len;
    const object_bytes = try alloc.alloc(u8, object_len);
    defer alloc.free(object_bytes);
    @memset(object_bytes, 0);
    @memcpy(object_bytes[first_offset..][0..first_chunk.items.len], first_chunk.items);
    @memcpy(object_bytes[second_offset..][0..second_chunk.items.len], second_chunk.items);

    const MemoryRangeReader = struct {
        bucket: []const u8,
        key: []const u8,
        body: []const u8,

        fn reader(self: *@This()) ObjectRangeReader {
            return .{
                .ctx = self,
                .read_range_alloc = readRangeAlloc,
            };
        }

        fn readRangeAlloc(
            ctx: *anyopaque,
            a: Allocator,
            bucket: []const u8,
            key: []const u8,
            offset: u64,
            len: usize,
        ) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (!std.mem.eql(u8, self.bucket, bucket)) return error.ObjectNotFound;
            if (!std.mem.eql(u8, self.key, key)) return error.ObjectNotFound;
            const start: usize = std.math.cast(usize, offset) orelse return error.InvalidLakeRangeRead;
            if (start > self.body.len or len > self.body.len - start) return error.InvalidLakeRangeRead;
            return try a.dupe(u8, self.body[start..][0..len]);
        }
    };
    var range_reader = MemoryRangeReader{
        .bucket = "bucket",
        .key = "events/part-a.parquet",
        .body = object_bytes,
    };

    var inventory = external_source.Inventory{
        .format = .parquet,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/events"),
        .snapshot_id = try alloc.dupe(u8, "sha256:objects"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "part-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/events/part-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-a"),
        .byte_len = object_len,
        .row_count = 4,
        .row_groups = try alloc.dupe(external_source.RowGroup, &[_]external_source.RowGroup{
            .{
                .ordinal = 0,
                .row_count = 2,
                .file_offset = first_offset,
                .total_byte_len = first_chunk.items.len,
                .column_chunks = try alloc.dupe(external_source.ColumnChunk, &[_]external_source.ColumnChunk{.{
                    .column_id = try alloc.dupe(u8, "amount"),
                    .file_offset = first_offset,
                    .compressed_len = first_chunk.items.len,
                    .uncompressed_len = first_chunk.items.len,
                    .encoding = try alloc.dupe(u8, "plain"),
                }}),
            },
            .{
                .ordinal = 1,
                .row_count = 2,
                .file_offset = second_offset,
                .total_byte_len = second_chunk.items.len,
                .column_chunks = try alloc.dupe(external_source.ColumnChunk, &[_]external_source.ColumnChunk{.{
                    .column_id = try alloc.dupe(u8, "amount"),
                    .file_offset = second_offset,
                    .compressed_len = second_chunk.items.len,
                    .uncompressed_len = second_chunk.items.len,
                    .encoding = try alloc.dupe(u8, "rle_dictionary"),
                }}),
            },
        }),
    };
    try inventory.validate();

    const projection = [_][]const u8{"amount"};
    var plan = try planSupportedI64ObjectRangeRowGroupsAlloc(alloc, inventory, &projection);
    defer plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), plan.row_groups.len);
    try std.testing.expectEqualStrings("part-a.parquet", plan.row_groups[0].file_id);
    try std.testing.expectEqual(@as(u32, 0), plan.row_groups[0].row_group_ordinal);
    try std.testing.expectEqual(@as(u32, 1), plan.row_groups[1].row_group_ordinal);
    try std.testing.expectError(
        error.ParquetColumnNotFound,
        planSupportedI64ObjectRangeRowGroupsAlloc(alloc, inventory, &[_][]const u8{"missing"}),
    );

    var source = try ObjectRangeRowGroupSource.init(range_reader.reader(), inventory, plan.row_groups);
    defer source.deinit(alloc);

    var result = try lake_rows.scanRowsAlloc(alloc, source.rowSource(), .{
        .projected_columns = &projection,
        .predicate = .{
            .column = "amount",
            .op = .eq_i64,
            .i64_value = 40,
        },
    });
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expectEqual(@as(i64, 40), result.rows[0].find("amount").?.value.?.i64);
    const row_ref = result.rows[0].row_ref.external;
    try std.testing.expectEqualStrings("part-a.parquet", row_ref.file_id);
    try std.testing.expectEqual(@as(u32, 1), row_ref.row_group_ordinal);
    try std.testing.expectEqual(@as(u64, 1), row_ref.row_ordinal);

    const deleted_row_refs = [_]rowsource.RowRef{.{ .external = .{
        .source_id = "events",
        .snapshot_id = "sha256:objects",
        .file_id = "part-a.parquet",
        .row_group_ordinal = 1,
        .row_ordinal = 1,
    } }};
    var filtered = try querySupportedI64ObjectRangeRowsAlloc(alloc, .{
        .reader = range_reader.reader(),
        .inventory = inventory,
        .projected_columns = &projection,
        .deleted_row_refs = &deleted_row_refs,
    });
    defer filtered.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 3), filtered.total);
    try std.testing.expectEqual(@as(usize, 3), filtered.rows.len);
    try std.testing.expectEqual(@as(i64, 10), filtered.rows[0].find("amount").?.value.?.i64);
    try std.testing.expectEqual(@as(i64, 20), filtered.rows[1].find("amount").?.value.?.i64);
    try std.testing.expectEqual(@as(i64, 30), filtered.rows[2].find("amount").?.value.?.i64);
}

test "parquet object range row group source coalesces adjacent projected chunks" {
    const alloc = std.testing.allocator;

    var amount_chunk = std.ArrayListUnmanaged(u8).empty;
    defer amount_chunk.deinit(alloc);
    try appendPlainI64DataPage(&amount_chunk, alloc, &[_]i64{ 10, 20 });
    var tenant_chunk = std.ArrayListUnmanaged(u8).empty;
    defer tenant_chunk.deinit(alloc);
    try appendPlainI64DataPage(&tenant_chunk, alloc, &[_]i64{ 7, 8 });

    const amount_offset: usize = 100;
    const tenant_offset = amount_offset + amount_chunk.items.len;
    const object_len = tenant_offset + tenant_chunk.items.len;
    const object_bytes = try alloc.alloc(u8, object_len);
    defer alloc.free(object_bytes);
    @memset(object_bytes, 0);
    @memcpy(object_bytes[amount_offset..][0..amount_chunk.items.len], amount_chunk.items);
    @memcpy(object_bytes[tenant_offset..][0..tenant_chunk.items.len], tenant_chunk.items);

    const CountingRangeReader = struct {
        bucket: []const u8,
        key: []const u8,
        body: []const u8,
        read_count: usize = 0,

        fn reader(self: *@This()) ObjectRangeReader {
            return .{
                .ctx = self,
                .read_range_alloc = readRangeAlloc,
            };
        }

        fn readRangeAlloc(
            ctx: *anyopaque,
            a: Allocator,
            bucket: []const u8,
            key: []const u8,
            offset: u64,
            len: usize,
        ) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (!std.mem.eql(u8, self.bucket, bucket)) return error.ObjectNotFound;
            if (!std.mem.eql(u8, self.key, key)) return error.ObjectNotFound;
            const start: usize = std.math.cast(usize, offset) orelse return error.InvalidLakeRangeRead;
            if (start > self.body.len or len > self.body.len - start) return error.InvalidLakeRangeRead;
            self.read_count += 1;
            return try a.dupe(u8, self.body[start..][0..len]);
        }
    };
    var range_reader = CountingRangeReader{
        .bucket = "bucket",
        .key = "events/part-a.parquet",
        .body = object_bytes,
    };

    var inventory = external_source.Inventory{
        .format = .parquet,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/events"),
        .snapshot_id = try alloc.dupe(u8, "sha256:objects"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "part-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/events/part-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-a"),
        .byte_len = object_len,
        .row_count = 2,
        .row_groups = try alloc.dupe(external_source.RowGroup, &[_]external_source.RowGroup{.{
            .ordinal = 0,
            .row_count = 2,
            .file_offset = amount_offset,
            .total_byte_len = amount_chunk.items.len + tenant_chunk.items.len,
            .column_chunks = try alloc.dupe(external_source.ColumnChunk, &[_]external_source.ColumnChunk{
                .{
                    .column_id = try alloc.dupe(u8, "amount"),
                    .file_offset = amount_offset,
                    .compressed_len = amount_chunk.items.len,
                    .uncompressed_len = amount_chunk.items.len,
                    .encoding = try alloc.dupe(u8, "plain"),
                },
                .{
                    .column_id = try alloc.dupe(u8, "tenant"),
                    .file_offset = tenant_offset,
                    .compressed_len = tenant_chunk.items.len,
                    .uncompressed_len = tenant_chunk.items.len,
                    .encoding = try alloc.dupe(u8, "plain"),
                },
            }),
        }}),
    };
    try inventory.validate();

    const projection = [_][]const u8{ "amount", "tenant" };
    const row_groups = [_]ObjectRangeRowGroupInput{.{
        .file_id = "part-a.parquet",
        .row_group_ordinal = 0,
        .projected_columns = &projection,
    }};
    var source = try ObjectRangeRowGroupSource.init(range_reader.reader(), inventory, &row_groups);
    defer source.deinit(alloc);
    var row_source = source.rowSource();
    const batch = (try row_source.next(alloc)).?;

    try std.testing.expectEqual(@as(usize, 1), range_reader.read_count);
    try std.testing.expectEqual(@as(usize, 2), batch.rowCount());
    try std.testing.expectEqualSlices(i64, &[_]i64{ 10, 20 }, batch.columns[0].values.i64);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 7, 8 }, batch.columns[1].values.i64);
    try std.testing.expect((try row_source.next(alloc)) == null);
}

test "parquet object range cache reuses coalesced projected chunks" {
    const alloc = std.testing.allocator;

    var amount_chunk = std.ArrayListUnmanaged(u8).empty;
    defer amount_chunk.deinit(alloc);
    try appendPlainI64DataPage(&amount_chunk, alloc, &[_]i64{ 10, 20 });
    var tenant_chunk = std.ArrayListUnmanaged(u8).empty;
    defer tenant_chunk.deinit(alloc);
    try appendPlainI64DataPage(&tenant_chunk, alloc, &[_]i64{ 7, 8 });

    const amount_offset: usize = 100;
    const tenant_offset = amount_offset + amount_chunk.items.len;
    const object_len = tenant_offset + tenant_chunk.items.len;
    const object_bytes = try alloc.alloc(u8, object_len);
    defer alloc.free(object_bytes);
    @memset(object_bytes, 0);
    @memcpy(object_bytes[amount_offset..][0..amount_chunk.items.len], amount_chunk.items);
    @memcpy(object_bytes[tenant_offset..][0..tenant_chunk.items.len], tenant_chunk.items);

    const CountingRangeReader = struct {
        bucket: []const u8,
        key: []const u8,
        body: []const u8,
        read_count: usize = 0,

        fn reader(self: *@This()) ObjectRangeReader {
            return .{
                .ctx = self,
                .read_range_alloc = readRangeAlloc,
            };
        }

        fn readRangeAlloc(
            ctx: *anyopaque,
            a: Allocator,
            bucket: []const u8,
            key: []const u8,
            offset: u64,
            len: usize,
        ) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (!std.mem.eql(u8, self.bucket, bucket)) return error.ObjectNotFound;
            if (!std.mem.eql(u8, self.key, key)) return error.ObjectNotFound;
            const start: usize = std.math.cast(usize, offset) orelse return error.InvalidLakeRangeRead;
            if (start > self.body.len or len > self.body.len - start) return error.InvalidLakeRangeRead;
            self.read_count += 1;
            return try a.dupe(u8, self.body[start..][0..len]);
        }
    };
    var range_reader = CountingRangeReader{
        .bucket = "bucket",
        .key = "events/part-a.parquet",
        .body = object_bytes,
    };

    var cache = ObjectRangeCache{};
    defer cache.deinit(alloc);

    var inventory = external_source.Inventory{
        .format = .parquet,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/events"),
        .snapshot_id = try alloc.dupe(u8, "sha256:objects"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "part-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/events/part-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-a"),
        .byte_len = object_len,
        .row_count = 2,
        .row_groups = try alloc.dupe(external_source.RowGroup, &[_]external_source.RowGroup{.{
            .ordinal = 0,
            .row_count = 2,
            .file_offset = amount_offset,
            .total_byte_len = amount_chunk.items.len + tenant_chunk.items.len,
            .column_chunks = try alloc.dupe(external_source.ColumnChunk, &[_]external_source.ColumnChunk{
                .{
                    .column_id = try alloc.dupe(u8, "amount"),
                    .file_offset = amount_offset,
                    .compressed_len = amount_chunk.items.len,
                    .uncompressed_len = amount_chunk.items.len,
                    .encoding = try alloc.dupe(u8, "plain"),
                },
                .{
                    .column_id = try alloc.dupe(u8, "tenant"),
                    .file_offset = tenant_offset,
                    .compressed_len = tenant_chunk.items.len,
                    .uncompressed_len = tenant_chunk.items.len,
                    .encoding = try alloc.dupe(u8, "plain"),
                },
            }),
        }}),
    };
    try inventory.validate();

    const projection = [_][]const u8{ "amount", "tenant" };
    var first = try buildSupportedI64RowGroupBatchFromCachedCoalescedObjectRangeReaderAlloc(
        alloc,
        range_reader.reader(),
        &cache,
        inventory,
        "part-a.parquet",
        0,
        &projection,
        .{},
    );
    defer first.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), range_reader.read_count);

    var second = try buildSupportedI64RowGroupBatchFromCachedCoalescedObjectRangeReaderAlloc(
        alloc,
        range_reader.reader(),
        &cache,
        inventory,
        "part-a.parquet",
        0,
        &projection,
        .{},
    );
    defer second.deinit(alloc);

    const stats = cache.statsSnapshot();
    try std.testing.expectEqual(@as(usize, 1), range_reader.read_count);
    try std.testing.expectEqual(@as(usize, 1), stats.misses);
    try std.testing.expectEqual(@as(usize, 1), stats.hits);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 10, 20 }, second.batch.columns[0].values.i64);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 7, 8 }, second.batch.columns[1].values.i64);
}

test "lake persistent object range cache durability is cache-only by default and explicit when durable" {
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const alloc = std.testing.allocator;
    const default_root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/default-cache", .{tmp.sub_path});
    defer alloc.free(default_root);
    const durable_root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/durable-cache", .{tmp.sub_path});
    defer alloc.free(durable_root);
    var default_cache = try PersistentObjectRangeCache.init(io, default_root);
    defer default_cache.deinit();
    try std.testing.expectEqual(PersistentObjectRangeCacheDurability.cache_only, default_cache.configuredPolicy().durability);
    var durable_cache = try PersistentObjectRangeCache.initWithDurability(io, durable_root, .durable);
    defer durable_cache.deinit();
    try std.testing.expectEqual(PersistentObjectRangeCacheDurability.durable, durable_cache.configuredPolicy().durability);
}

test "lake persistent object range cache bounds write-behind admission" {
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const cache_root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/bounded-cache", .{tmp.sub_path});
    defer alloc.free(cache_root);
    var persistent = try PersistentObjectRangeCache.initWithPolicy(io_impl.io(), cache_root, .{
        .max_total_bytes = 1024,
        .max_entries = 4,
        .max_write_queue_bytes = 4,
        .max_write_queue_entries = 1,
        .max_cache_key_bytes = 3,
    });
    defer persistent.deinit();

    try std.testing.expectEqual(
        PersistentObjectRangeCacheEnqueueResult.dropped,
        persistent.enqueueWrite("key", "payload"),
    );
    const stats = persistent.statsSnapshot();
    try std.testing.expectEqual(@as(usize, 1), stats.writes_dropped);
    try std.testing.expectEqual(@as(usize, 7), stats.dropped_bytes);
    try std.testing.expectEqual(@as(usize, 0), stats.queued_entries);
    try std.testing.expect((try persistent.readAlloc(alloc, "long", 1)) == null);
    try std.testing.expectEqual(@as(usize, 1), persistent.statsSnapshot().read_misses);
}

test "lake persistent object range cache evicts least recently used entries within disk ceilings" {
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const cache_root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/lru-cache", .{tmp.sub_path});
    defer alloc.free(cache_root);
    const one_entry_bytes = persistentObjectRangeEncodedLen("key-a".len, "data".len).?;
    var persistent = try PersistentObjectRangeCache.initWithPolicy(io_impl.io(), cache_root, .{
        .max_total_bytes = one_entry_bytes * 2,
        .max_entries = 2,
        .max_write_queue_bytes = 1024,
        .max_write_queue_entries = 4,
    });
    defer persistent.deinit();

    try std.testing.expectEqual(.enqueued, persistent.enqueueWrite("key-a", "data"));
    persistent.flush();
    try std.testing.expectEqual(.enqueued, persistent.enqueueWrite("key-b", "data"));
    persistent.flush();
    const touched = (try persistent.readAlloc(alloc, "key-a", 4)).?;
    defer alloc.free(touched);
    try std.testing.expectEqualStrings("data", touched);
    try std.testing.expectEqual(.enqueued, persistent.enqueueWrite("key-c", "data"));
    persistent.flush();

    try std.testing.expect((try persistent.readAlloc(alloc, "key-b", 4)) == null);
    const retained_a = (try persistent.readAlloc(alloc, "key-a", 4)).?;
    defer alloc.free(retained_a);
    const retained_c = (try persistent.readAlloc(alloc, "key-c", 4)).?;
    defer alloc.free(retained_c);
    const stats = persistent.statsSnapshot();
    try std.testing.expectEqual(@as(usize, 2), stats.entries);
    try std.testing.expectEqual(one_entry_bytes * 2, stats.stored_bytes);
    try std.testing.expectEqual(@as(usize, 1), stats.evicted_entries);
    try std.testing.expectEqual(one_entry_bytes, stats.evicted_bytes);
    try std.testing.expectEqual(@as(usize, 3), stats.read_hits);
    try std.testing.expectEqual(@as(usize, 1), stats.read_misses);
    try std.testing.expectEqual(@as(usize, 3), stats.writes_completed);
}

test "lake persistent object range cache removes interrupted and corrupted entries" {
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const cache_root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/cleanup-cache", .{tmp.sub_path});
    defer alloc.free(cache_root);
    const temporary_path = try std.fmt.allocPrint(
        alloc,
        "{s}/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.tmp-orphan",
        .{cache_root},
    );
    defer alloc.free(temporary_path);
    try persistentObjectRangeEnsureParentDir(io, temporary_path);
    try persistentObjectRangeWriteFileAtomically(io, temporary_path, "partial", .cache_only);

    var persistent = try PersistentObjectRangeCache.init(io, cache_root);
    defer persistent.deinit();
    try std.testing.expectEqual(@as(usize, 1), persistent.statsSnapshot().temporary_files_removed);

    try std.testing.expectEqual(.enqueued, persistent.enqueueWrite("corrupt-key", "data"));
    persistent.flush();
    const corrupt_path = try persistent.cachePathAlloc(alloc, "corrupt-key");
    defer alloc.free(corrupt_path);
    try persistentObjectRangeWriteFileAtomically(io, corrupt_path, "corrupt", .cache_only);
    try std.testing.expect((try persistent.readAlloc(alloc, "corrupt-key", 4)) == null);
    const stats = persistent.statsSnapshot();
    try std.testing.expectEqual(@as(usize, 1), stats.corrupt_entries_removed);
    try std.testing.expectEqual(@as(usize, 0), stats.entries);
    try std.testing.expectEqual(@as(usize, 0), stats.stored_bytes);
    try std.testing.expectEqual(@as(usize, 1), stats.read_misses);
}

test "lake persistent object range cache bounds startup inventory and drains accepted writes" {
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const cache_root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/startup-cache", .{tmp.sub_path});
    defer alloc.free(cache_root);
    const keys = [_][]const u8{ "startup-a", "startup-b", "startup-c" };
    for (keys) |key| {
        const filename = try objectRangeCacheKeyDigestHexAlloc(alloc, key);
        defer alloc.free(filename);
        const path = try std.fs.path.join(alloc, &.{ cache_root, filename });
        defer alloc.free(path);
        try persistentObjectRangeEnsureParentDir(io, path);
        try persistentObjectRangeWriteEntryAtomically(io, path, key, "data", .cache_only);
    }

    {
        var persistent = try PersistentObjectRangeCache.initWithPolicy(io, cache_root, .{
            .max_total_bytes = 1024,
            .max_entries = 1,
            .max_write_queue_bytes = 1024,
            .max_write_queue_entries = 2,
        });
        defer persistent.deinit();
        const stats = persistent.statsSnapshot();
        try std.testing.expectEqual(@as(usize, 1), stats.entries);
        try std.testing.expectEqual(@as(usize, 2), stats.evicted_entries);
        try std.testing.expectEqual(.enqueued, persistent.enqueueWrite("shutdown-write", "next"));
    }

    var reopened = try PersistentObjectRangeCache.initWithPolicy(io, cache_root, .{
        .max_total_bytes = 1024,
        .max_entries = 2,
        .max_write_queue_bytes = 1024,
        .max_write_queue_entries = 2,
    });
    defer reopened.deinit();
    const drained = (try reopened.readAlloc(alloc, "shutdown-write", 4)).?;
    defer alloc.free(drained);
    try std.testing.expectEqualStrings("next", drained);
}

test "parquet object range cache reuses persistent validated ranges" {
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const cache_root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/range-cache", .{tmp.sub_path});
    defer alloc.free(cache_root);
    var persistent = try PersistentObjectRangeCache.init(io_impl.io(), cache_root);
    defer persistent.deinit();

    const CountingRangeReader = struct {
        body: []const u8,
        read_count: usize = 0,

        fn reader(self: *@This()) ObjectRangeReader {
            return .{ .ctx = self, .read_range_alloc = readRangeAlloc };
        }

        fn readRangeAlloc(
            ctx: *anyopaque,
            a: Allocator,
            bucket: []const u8,
            key: []const u8,
            offset: u64,
            len: usize,
        ) ![]u8 {
            _ = bucket;
            _ = key;
            const self: *@This() = @ptrCast(@alignCast(ctx));
            const start: usize = std.math.cast(usize, offset) orelse return error.InvalidLakeRangeRead;
            if (start > self.body.len or len > self.body.len - start) return error.InvalidLakeRangeRead;
            self.read_count += 1;
            return try a.dupe(u8, self.body[start..][0..len]);
        }
    };
    var reader = CountingRangeReader{ .body = "0123456789" };
    const read = range_io.RangeRead{
        .object = .{
            .bucket = "bucket",
            .key = "events/part-a.parquet",
            .byte_len = 10,
            .version = .{ .etag = "etag-a" },
        },
        .range = .{ .offset = 2, .len = 4 },
        .purpose = .parquet_footer,
    };

    {
        var cache = ObjectRangeCache{ .persistent = &persistent };
        defer cache.deinit(alloc);
        const bytes = try cache.readAlloc(alloc, reader.reader(), read);
        defer alloc.free(bytes);
        try std.testing.expectEqualStrings("2345", bytes);
        try std.testing.expectEqual(@as(usize, 1), reader.read_count);
        const stats = cache.statsSnapshot();
        try std.testing.expectEqual(@as(usize, 1), stats.misses);
        try std.testing.expectEqual(@as(usize, 0), stats.hits);
    }

    persistent.flush();

    {
        var cache = ObjectRangeCache{ .persistent = &persistent };
        defer cache.deinit(alloc);
        const bytes = try cache.readAlloc(alloc, reader.reader(), read);
        defer alloc.free(bytes);
        try std.testing.expectEqualStrings("2345", bytes);
        try std.testing.expectEqual(@as(usize, 1), reader.read_count);
        const stats = cache.statsSnapshot();
        try std.testing.expectEqual(@as(usize, 0), stats.misses);
        try std.testing.expectEqual(@as(usize, 1), stats.hits);
        try std.testing.expectEqual(@as(usize, 4), stats.stored_bytes);
    }
}

test "parquet object range cache ignores corrupted persistent ranges" {
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const cache_root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/range-cache", .{tmp.sub_path});
    defer alloc.free(cache_root);
    var persistent = try PersistentObjectRangeCache.init(io, cache_root);
    defer persistent.deinit();

    const CountingRangeReader = struct {
        body: []const u8,
        read_count: usize = 0,

        fn reader(self: *@This()) ObjectRangeReader {
            return .{ .ctx = self, .read_range_alloc = readRangeAlloc };
        }

        fn readRangeAlloc(
            ctx: *anyopaque,
            a: Allocator,
            bucket: []const u8,
            key: []const u8,
            offset: u64,
            len: usize,
        ) ![]u8 {
            _ = bucket;
            _ = key;
            const self: *@This() = @ptrCast(@alignCast(ctx));
            const start: usize = std.math.cast(usize, offset) orelse return error.InvalidLakeRangeRead;
            if (start > self.body.len or len > self.body.len - start) return error.InvalidLakeRangeRead;
            self.read_count += 1;
            return try a.dupe(u8, self.body[start..][0..len]);
        }
    };
    var reader = CountingRangeReader{ .body = "0123456789" };
    const read = range_io.RangeRead{
        .object = .{
            .bucket = "bucket",
            .key = "events/part-a.parquet",
            .byte_len = 10,
            .version = .{ .etag = "etag-a" },
        },
        .range = .{ .offset = 2, .len = 4 },
        .purpose = .parquet_footer,
    };
    const cache_key = try read.cacheKeyAlloc(alloc);
    defer alloc.free(cache_key);
    const corrupted_path = try persistent.cachePathAlloc(alloc, cache_key);
    defer alloc.free(corrupted_path);
    try persistentObjectRangeEnsureParentDir(io, corrupted_path);
    try persistentObjectRangeWriteFileAtomically(io, corrupted_path, "not-a-valid-cache-entry", .cache_only);

    var cache = ObjectRangeCache{ .persistent = &persistent };
    defer cache.deinit(alloc);
    const bytes = try cache.readAlloc(alloc, reader.reader(), read);
    defer alloc.free(bytes);
    try std.testing.expectEqualStrings("2345", bytes);
    try std.testing.expectEqual(@as(usize, 1), reader.read_count);
    const stats = cache.statsSnapshot();
    try std.testing.expectEqual(@as(usize, 1), stats.misses);
    try std.testing.expectEqual(@as(usize, 0), stats.hits);
}

test "lake persistent object range cache rejects an otherwise valid overlong entry" {
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const cache_root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/range-cache", .{tmp.sub_path});
    defer alloc.free(cache_root);
    var persistent = try PersistentObjectRangeCache.init(io, cache_root);
    defer persistent.deinit();
    const cache_key = "bucket=test:key=part.parquet:offset=2:len=4";
    const path = try persistent.cachePathAlloc(alloc, cache_key);
    defer alloc.free(path);
    const encoded = try persistentObjectRangeEncodeAlloc(alloc, cache_key, "2345");
    defer alloc.free(encoded);
    const overlong = try alloc.alloc(u8, encoded.len + 1);
    defer alloc.free(overlong);
    @memcpy(overlong[0..encoded.len], encoded);
    overlong[encoded.len] = 0xaa;
    try persistentObjectRangeEnsureParentDir(io, path);
    try persistentObjectRangeWriteFileAtomically(io, path, overlong, .cache_only);

    try std.testing.expect((try persistent.readAlloc(alloc, cache_key, 4)) == null);
    try std.testing.expectError(
        error.InvalidLakeRangeRead,
        persistent.readAlloc(alloc, cache_key, range_io.max_physical_range_read_bytes + 1),
    );
}

test "parquet object range cache rejects mismatched persistent provenance" {
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const cache_root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/range-cache", .{tmp.sub_path});
    defer alloc.free(cache_root);
    var persistent = try PersistentObjectRangeCache.init(io, cache_root);
    defer persistent.deinit();

    const CountingRangeReader = struct {
        body: []const u8,
        read_count: usize = 0,

        fn reader(self: *@This()) ObjectRangeReader {
            return .{ .ctx = self, .read_range_alloc = readRangeAlloc };
        }

        fn readRangeAlloc(
            ctx: *anyopaque,
            a: Allocator,
            bucket: []const u8,
            key: []const u8,
            offset: u64,
            len: usize,
        ) ![]u8 {
            _ = bucket;
            _ = key;
            const self: *@This() = @ptrCast(@alignCast(ctx));
            const start: usize = std.math.cast(usize, offset) orelse return error.InvalidLakeRangeRead;
            if (start > self.body.len or len > self.body.len - start) return error.InvalidLakeRangeRead;
            self.read_count += 1;
            return try a.dupe(u8, self.body[start..][0..len]);
        }
    };
    var reader = CountingRangeReader{ .body = "0123456789" };
    const read = range_io.RangeRead{
        .object = .{
            .bucket = "bucket",
            .key = "events/part-a.parquet",
            .byte_len = 10,
            .version = .{ .etag = "etag-a" },
        },
        .range = .{ .offset = 2, .len = 4 },
        .purpose = .parquet_footer,
    };
    const cache_key = try read.cacheKeyAlloc(alloc);
    defer alloc.free(cache_key);
    const mismatched_path = try persistent.cachePathAlloc(alloc, cache_key);
    defer alloc.free(mismatched_path);
    const encoded = try persistentObjectRangeEncodeAlloc(alloc, "bucket=other:key=events/part-a.parquet", "2345");
    defer alloc.free(encoded);
    try persistentObjectRangeEnsureParentDir(io, mismatched_path);
    try persistentObjectRangeWriteFileAtomically(io, mismatched_path, encoded, .cache_only);

    var cache = ObjectRangeCache{ .persistent = &persistent };
    defer cache.deinit(alloc);
    const bytes = try cache.readAlloc(alloc, reader.reader(), read);
    defer alloc.free(bytes);
    try std.testing.expectEqualStrings("2345", bytes);
    try std.testing.expectEqual(@as(usize, 1), reader.read_count);
    const stats = cache.statsSnapshot();
    try std.testing.expectEqual(@as(usize, 1), stats.misses);
    try std.testing.expectEqual(@as(usize, 0), stats.hits);
}

test "parquet object range reader rejects malformed range lengths" {
    const alloc = std.testing.allocator;
    const MalformedRangeReader = struct {
        body: []const u8,

        fn reader(self: *@This()) ObjectRangeReader {
            return .{
                .ctx = self,
                .read_range_alloc = readRangeAlloc,
            };
        }

        fn readRangeAlloc(
            ctx: *anyopaque,
            a: Allocator,
            bucket: []const u8,
            key: []const u8,
            offset: u64,
            len: usize,
        ) ![]u8 {
            _ = bucket;
            _ = key;
            _ = offset;
            const self: *@This() = @ptrCast(@alignCast(ctx));
            return try a.dupe(u8, self.body[0..@min(self.body.len, len + 1)]);
        }
    };

    var short_reader = MalformedRangeReader{ .body = "abc" };
    try std.testing.expectError(
        error.InvalidLakeRangeRead,
        short_reader.reader().readAlloc(alloc, "bucket", "object", 0, 4),
    );

    var long_reader = MalformedRangeReader{ .body = "abcde" };
    try std.testing.expectError(
        error.InvalidLakeRangeRead,
        long_reader.reader().readAlloc(alloc, "bucket", "object", 0, 4),
    );
}

test "parquet object range cache rejects corrupted cached range lengths" {
    const alloc = std.testing.allocator;
    var cache = ObjectRangeCache{};
    defer cache.deinit(alloc);

    const file = external_source.FileEntry{
        .file_id = @constCast("part-a.parquet"),
        .object_uri = @constCast("s3://bucket/events/part-a.parquet"),
        .etag = @constCast("etag-a"),
        .byte_len = 16,
        .row_count = 1,
        .row_groups = &.{},
    };
    const object = try range_io.objectRefForExternalFileUri(file);
    const read = range_io.RangeRead{
        .object = object,
        .range = .{ .offset = 4, .len = 6 },
        .purpose = .parquet_column_chunk,
        .decoded_column_id = "amount",
    };
    const cache_key = try read.cacheKeyAlloc(alloc);
    const stored = try alloc.dupe(u8, "short");
    try cache.entries.put(alloc, cache_key, .{
        .bytes = stored,
        .checksum = objectRangeCacheDigest(stored),
    });

    const UnusedReader = struct {
        fn reader(self: *@This()) ObjectRangeReader {
            return .{
                .ctx = self,
                .read_range_alloc = readRangeAlloc,
            };
        }

        fn readRangeAlloc(
            ctx: *anyopaque,
            a: Allocator,
            bucket: []const u8,
            key: []const u8,
            offset: u64,
            len: usize,
        ) ![]u8 {
            _ = ctx;
            _ = bucket;
            _ = key;
            _ = offset;
            return try a.alloc(u8, len);
        }
    };
    var reader = UnusedReader{};

    try std.testing.expectError(
        error.InvalidLakeRangeRead,
        cache.readAlloc(alloc, reader.reader(), read),
    );
    const stats = cache.statsSnapshot();
    try std.testing.expectEqual(@as(usize, 0), stats.hits);
    try std.testing.expectEqual(@as(usize, 0), stats.misses);
}

test "parquet object range cache enforces fetch admission before reader invocation" {
    const alloc = std.testing.allocator;
    const CountingReader = struct {
        calls: usize = 0,

        fn readRangeAlloc(
            ctx: *anyopaque,
            a: Allocator,
            bucket: []const u8,
            key: []const u8,
            offset: u64,
            len: usize,
        ) ![]u8 {
            _ = bucket;
            _ = key;
            _ = offset;
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.calls += 1;
            return try a.alloc(u8, len);
        }
    };
    var reader_impl = CountingReader{};
    const reader = ObjectRangeReader{
        .ctx = &reader_impl,
        .read_range_alloc = CountingReader.readRangeAlloc,
    };
    var policy = ObjectRangeCachePolicy{};
    policy.setFetchLimit(1);
    var cache = ObjectRangeCache{ .policy = policy };
    defer cache.deinit(alloc);
    const read = range_io.RangeRead{
        .object = .{
            .bucket = "bucket",
            .key = "object",
            .byte_len = 2,
            .version = .{ .etag = "etag" },
        },
        .range = .{ .offset = 0, .len = 2 },
        .purpose = .parquet_column_chunk,
    };
    try std.testing.expectError(error.LakeRangeReadTooLarge, cache.readAlloc(alloc, reader, read));
    try std.testing.expectEqual(@as(usize, 0), reader_impl.calls);
}

test "parquet object range cache releases its cache key when a source read fails" {
    const alloc = std.testing.allocator;
    const FailingReader = struct {
        fn readRangeAlloc(
            ctx: *anyopaque,
            a: Allocator,
            bucket: []const u8,
            key: []const u8,
            offset: u64,
            len: usize,
        ) ![]u8 {
            _ = ctx;
            _ = a;
            _ = bucket;
            _ = key;
            _ = offset;
            _ = len;
            return error.TestRangeReadFailure;
        }
    };
    var unused: u8 = 0;
    const reader = ObjectRangeReader{ .ctx = &unused, .read_range_alloc = FailingReader.readRangeAlloc };
    var cache = ObjectRangeCache{};
    defer cache.deinit(alloc);
    const read = range_io.RangeRead{
        .object = .{
            .bucket = "bucket",
            .key = "object",
            .byte_len = 2,
            .version = .{ .etag = "etag" },
        },
        .range = .{ .offset = 0, .len = 2 },
        .purpose = .parquet_column_chunk,
    };

    try std.testing.expectError(error.TestRangeReadFailure, cache.readAlloc(alloc, reader, read));
}

test "parquet serving cache capacity does not reject larger safe reads" {
    const alloc = std.testing.allocator;
    const Reader = struct {
        fn readRangeAlloc(
            ctx: *anyopaque,
            a: Allocator,
            bucket: []const u8,
            key: []const u8,
            offset: u64,
            len: usize,
        ) ![]u8 {
            _ = ctx;
            _ = bucket;
            _ = key;
            _ = offset;
            return try a.alloc(u8, len);
        }
    };
    var unused: u8 = 0;
    const reader = ObjectRangeReader{ .ctx = &unused, .read_range_alloc = Reader.readRangeAlloc };
    var cache = ObjectRangeCache.initWithLakeServingDefaults(1);
    defer cache.deinit(alloc);
    const read = range_io.RangeRead{
        .object = .{
            .bucket = "bucket",
            .key = "object",
            .byte_len = 2,
            .version = .{ .etag = "etag" },
        },
        .range = .{ .offset = 0, .len = 2 },
        .purpose = .parquet_column_chunk,
    };
    const bytes = try cache.readAlloc(alloc, reader, read);
    defer alloc.free(bytes);
    try std.testing.expectEqual(@as(usize, 2), bytes.len);
    try std.testing.expectEqual(@as(usize, 0), cache.statsSnapshot().stored_bytes);
    try std.testing.expectEqual(@as(usize, 2), cache.statsSnapshot().rejected_bytes);
}

test "parquet object range cache rejects corrupted cached range checksums" {
    const alloc = std.testing.allocator;
    var cache = ObjectRangeCache{};
    defer cache.deinit(alloc);

    const file = external_source.FileEntry{
        .file_id = @constCast("part-a.parquet"),
        .object_uri = @constCast("s3://bucket/events/part-a.parquet"),
        .etag = @constCast("etag-a"),
        .byte_len = 16,
        .row_count = 1,
        .row_groups = &.{},
    };
    const object = try range_io.objectRefForExternalFileUri(file);
    const read = range_io.RangeRead{
        .object = object,
        .range = .{ .offset = 4, .len = 6 },
        .purpose = .parquet_column_chunk,
        .decoded_column_id = "amount",
    };
    const cache_key = try read.cacheKeyAlloc(alloc);
    const stored = try alloc.dupe(u8, "abcdef");
    try cache.entries.put(alloc, cache_key, .{
        .bytes = stored,
        .checksum = objectRangeCacheDigest("ghijkl"),
    });

    const UnusedReader = struct {
        fn reader(self: *@This()) ObjectRangeReader {
            return .{
                .ctx = self,
                .read_range_alloc = readRangeAlloc,
            };
        }

        fn readRangeAlloc(
            ctx: *anyopaque,
            a: Allocator,
            bucket: []const u8,
            key: []const u8,
            offset: u64,
            len: usize,
        ) ![]u8 {
            _ = ctx;
            _ = bucket;
            _ = key;
            _ = offset;
            return try a.alloc(u8, len);
        }
    };
    var reader = UnusedReader{};

    try std.testing.expectError(
        error.InvalidLakeRangeRead,
        cache.readAlloc(alloc, reader.reader(), read),
    );
    const stats = cache.statsSnapshot();
    try std.testing.expectEqual(@as(usize, 0), stats.hits);
    try std.testing.expectEqual(@as(usize, 0), stats.misses);
}

test "parquet object range cache accounts bytes by cache lane" {
    const alloc = std.testing.allocator;
    var cache = ObjectRangeCache{};
    defer cache.deinit(alloc);

    const CountingReader = struct {
        body: []const u8,
        read_count: usize = 0,

        fn reader(self: *@This()) ObjectRangeReader {
            return .{
                .ctx = self,
                .read_range_alloc = readRangeAlloc,
            };
        }

        fn readRangeAlloc(
            ctx: *anyopaque,
            a: Allocator,
            bucket: []const u8,
            key: []const u8,
            offset: u64,
            len: usize,
        ) ![]u8 {
            _ = bucket;
            _ = key;
            const self: *@This() = @ptrCast(@alignCast(ctx));
            const start: usize = std.math.cast(usize, offset) orelse return error.InvalidLakeRangeRead;
            if (start > self.body.len or len > self.body.len - start) return error.InvalidLakeRangeRead;
            self.read_count += 1;
            return try a.dupe(u8, self.body[start..][0..len]);
        }
    };
    var reader = CountingReader{ .body = "abcdefghijklmnopqrstuvwxyz" };
    const object = range_io.ObjectRef{
        .bucket = "bucket",
        .key = "events/part-a.parquet",
        .byte_len = 26,
        .version = .{ .etag = "etag-a" },
    };
    const metadata_read = range_io.RangeRead{
        .object = object,
        .range = .{ .offset = 0, .len = 4 },
        .purpose = .parquet_footer,
    };
    const chunk_read = range_io.RangeRead{
        .object = object,
        .range = .{ .offset = 8, .len = 6 },
        .purpose = .parquet_column_chunk,
        .decoded_column_id = "amount",
    };

    const metadata_first = try cache.readAlloc(alloc, reader.reader(), metadata_read);
    defer alloc.free(metadata_first);
    const chunk_first = try cache.readAlloc(alloc, reader.reader(), chunk_read);
    defer alloc.free(chunk_first);
    const metadata_second = try cache.readAlloc(alloc, reader.reader(), metadata_read);
    defer alloc.free(metadata_second);
    const chunk_second = try cache.readAlloc(alloc, reader.reader(), chunk_read);
    defer alloc.free(chunk_second);

    const stats = cache.statsSnapshot();
    try std.testing.expectEqual(@as(usize, 2), reader.read_count);
    try std.testing.expectEqual(@as(usize, 2), stats.misses);
    try std.testing.expectEqual(@as(usize, 2), stats.hits);
    try std.testing.expectEqual(@as(usize, 10), stats.stored_bytes);
    try std.testing.expectEqual(@as(usize, 4), stats.lane(.metadata).stored_bytes);
    try std.testing.expectEqual(@as(usize, 1), stats.lane(.metadata).misses);
    try std.testing.expectEqual(@as(usize, 1), stats.lane(.metadata).hits);
    try std.testing.expectEqual(@as(usize, 6), stats.lane(.compressed_range).stored_bytes);
    try std.testing.expectEqual(@as(usize, 1), stats.lane(.compressed_range).misses);
    try std.testing.expectEqual(@as(usize, 1), stats.lane(.compressed_range).hits);
}

test "parquet object range cache applies lane admission limits independently" {
    const alloc = std.testing.allocator;
    var cache = ObjectRangeCache{
        .policy = ObjectRangeCachePolicy.withLaneLimit(.broad_scan_scratch, 0),
    };
    defer cache.deinit(alloc);

    const CountingReader = struct {
        read_count: usize = 0,

        fn reader(self: *@This()) ObjectRangeReader {
            return .{
                .ctx = self,
                .read_range_alloc = readRangeAlloc,
            };
        }

        fn readRangeAlloc(
            ctx: *anyopaque,
            a: Allocator,
            bucket: []const u8,
            key: []const u8,
            offset: u64,
            len: usize,
        ) ![]u8 {
            _ = bucket;
            _ = key;
            _ = offset;
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.read_count += 1;
            const bytes = try a.alloc(u8, len);
            @memset(bytes, 'x');
            return bytes;
        }
    };
    var reader = CountingReader{};
    const object = range_io.ObjectRef{
        .bucket = "bucket",
        .key = "events/scan.tmp",
        .byte_len = 64,
        .version = .{ .etag = "etag-a" },
    };
    const scratch_read = range_io.RangeRead{
        .object = object,
        .range = .{ .offset = 0, .len = 8 },
        .purpose = .broad_scan_scratch,
    };
    const metadata_read = range_io.RangeRead{
        .object = object,
        .range = .{ .offset = 8, .len = 4 },
        .purpose = .parquet_footer,
    };

    const scratch_first = try cache.readAlloc(alloc, reader.reader(), scratch_read);
    defer alloc.free(scratch_first);
    const scratch_second = try cache.readAlloc(alloc, reader.reader(), scratch_read);
    defer alloc.free(scratch_second);
    const metadata_first = try cache.readAlloc(alloc, reader.reader(), metadata_read);
    defer alloc.free(metadata_first);
    const metadata_second = try cache.readAlloc(alloc, reader.reader(), metadata_read);
    defer alloc.free(metadata_second);

    const stats = cache.statsSnapshot();
    try std.testing.expectEqual(@as(usize, 3), reader.read_count);
    try std.testing.expectEqual(@as(usize, 3), stats.misses);
    try std.testing.expectEqual(@as(usize, 1), stats.hits);
    try std.testing.expectEqual(@as(usize, 4), stats.stored_bytes);
    try std.testing.expectEqual(@as(usize, 16), stats.rejected_bytes);
    try std.testing.expectEqual(@as(usize, 0), stats.lane(.broad_scan_scratch).stored_bytes);
    try std.testing.expectEqual(@as(usize, 16), stats.lane(.broad_scan_scratch).rejected_bytes);
    try std.testing.expectEqual(@as(usize, 2), stats.lane(.broad_scan_scratch).misses);
    try std.testing.expectEqual(@as(usize, 4), stats.lane(.metadata).stored_bytes);
    try std.testing.expectEqual(@as(usize, 1), stats.lane(.metadata).hits);
}

test "parquet object range cache evicts within one lane without evicting metadata" {
    const alloc = std.testing.allocator;
    var policy = ObjectRangeCachePolicy{};
    policy.setLaneLimit(.compressed_range, 6);
    var cache = ObjectRangeCache{ .policy = policy };
    defer cache.deinit(alloc);

    const CountingReader = struct {
        body: []const u8,
        read_count: usize = 0,

        fn reader(self: *@This()) ObjectRangeReader {
            return .{
                .ctx = self,
                .read_range_alloc = readRangeAlloc,
            };
        }

        fn readRangeAlloc(
            ctx: *anyopaque,
            a: Allocator,
            bucket: []const u8,
            key: []const u8,
            offset: u64,
            len: usize,
        ) ![]u8 {
            _ = bucket;
            _ = key;
            const self: *@This() = @ptrCast(@alignCast(ctx));
            const start: usize = std.math.cast(usize, offset) orelse return error.InvalidLakeRangeRead;
            if (start > self.body.len or len > self.body.len - start) return error.InvalidLakeRangeRead;
            self.read_count += 1;
            return try a.dupe(u8, self.body[start..][0..len]);
        }
    };
    var reader = CountingReader{ .body = "abcdefghijklmnopqrstuvwxyz" };
    const object = range_io.ObjectRef{
        .bucket = "bucket",
        .key = "events/part-a.parquet",
        .byte_len = 26,
        .version = .{ .etag = "etag-a" },
    };
    const metadata_read = range_io.RangeRead{
        .object = object,
        .range = .{ .offset = 0, .len = 4 },
        .purpose = .parquet_footer,
    };
    const chunk_a = range_io.RangeRead{
        .object = object,
        .range = .{ .offset = 4, .len = 6 },
        .purpose = .parquet_column_chunk,
        .decoded_column_id = "amount",
    };
    const chunk_b = range_io.RangeRead{
        .object = object,
        .range = .{ .offset = 10, .len = 6 },
        .purpose = .parquet_column_chunk,
        .decoded_column_id = "tenant",
    };

    const metadata_first = try cache.readAlloc(alloc, reader.reader(), metadata_read);
    defer alloc.free(metadata_first);
    const chunk_a_first = try cache.readAlloc(alloc, reader.reader(), chunk_a);
    defer alloc.free(chunk_a_first);
    const chunk_b_first = try cache.readAlloc(alloc, reader.reader(), chunk_b);
    defer alloc.free(chunk_b_first);
    const metadata_second = try cache.readAlloc(alloc, reader.reader(), metadata_read);
    defer alloc.free(metadata_second);
    const chunk_a_second = try cache.readAlloc(alloc, reader.reader(), chunk_a);
    defer alloc.free(chunk_a_second);

    const stats = cache.statsSnapshot();
    try std.testing.expectEqual(@as(usize, 4), reader.read_count);
    try std.testing.expectEqual(@as(usize, 4), stats.misses);
    try std.testing.expectEqual(@as(usize, 1), stats.hits);
    try std.testing.expectEqual(@as(usize, 10), stats.stored_bytes);
    try std.testing.expectEqual(@as(usize, 12), stats.evicted_bytes);
    try std.testing.expectEqual(@as(usize, 4), stats.lane(.metadata).stored_bytes);
    try std.testing.expectEqual(@as(usize, 0), stats.lane(.metadata).evicted_bytes);
    try std.testing.expectEqual(@as(usize, 1), stats.lane(.metadata).hits);
    try std.testing.expectEqual(@as(usize, 6), stats.lane(.compressed_range).stored_bytes);
    try std.testing.expectEqual(@as(usize, 12), stats.lane(.compressed_range).evicted_bytes);
}

test "parquet object range cache protects serving sidecars from broad scans" {
    const alloc = std.testing.allocator;
    var policy = ObjectRangeCachePolicy{};
    policy.setTotalLimit(10);
    policy.protectLane(.serving_sidecar);
    var cache = ObjectRangeCache{ .policy = policy };
    defer cache.deinit(alloc);

    const CountingReader = struct {
        body: []const u8,
        read_count: usize = 0,

        fn reader(self: *@This()) ObjectRangeReader {
            return .{
                .ctx = self,
                .read_range_alloc = readRangeAlloc,
            };
        }

        fn readRangeAlloc(
            ctx: *anyopaque,
            a: Allocator,
            bucket: []const u8,
            key: []const u8,
            offset: u64,
            len: usize,
        ) ![]u8 {
            _ = bucket;
            _ = key;
            const self: *@This() = @ptrCast(@alignCast(ctx));
            const start: usize = std.math.cast(usize, offset) orelse return error.InvalidLakeRangeRead;
            if (start > self.body.len or len > self.body.len - start) return error.InvalidLakeRangeRead;
            self.read_count += 1;
            return try a.dupe(u8, self.body[start..][0..len]);
        }
    };
    var reader = CountingReader{ .body = "abcdefghijklmnop" };
    const object = range_io.ObjectRef{
        .bucket = "bucket",
        .key = "events/sidecar-and-scratch.bin",
        .byte_len = 16,
        .version = .{ .etag = "etag-a" },
    };
    const sidecar_read = range_io.RangeRead{
        .object = object,
        .range = .{ .offset = 0, .len = 8 },
        .purpose = .sidecar_payload,
    };
    const scratch_read = range_io.RangeRead{
        .object = object,
        .range = .{ .offset = 8, .len = 4 },
        .purpose = .broad_scan_scratch,
    };

    const sidecar_first = try cache.readAlloc(alloc, reader.reader(), sidecar_read);
    defer alloc.free(sidecar_first);
    const scratch_first = try cache.readAlloc(alloc, reader.reader(), scratch_read);
    defer alloc.free(scratch_first);
    const sidecar_second = try cache.readAlloc(alloc, reader.reader(), sidecar_read);
    defer alloc.free(sidecar_second);

    const stats = cache.statsSnapshot();
    try std.testing.expectEqual(@as(usize, 2), reader.read_count);
    try std.testing.expectEqual(@as(usize, 1), stats.hits);
    try std.testing.expectEqual(@as(usize, 8), stats.stored_bytes);
    try std.testing.expectEqual(@as(usize, 4), stats.rejected_bytes);
    try std.testing.expectEqual(@as(usize, 8), stats.lane(.serving_sidecar).stored_bytes);
    try std.testing.expectEqual(@as(usize, 1), stats.lane(.serving_sidecar).hits);
    try std.testing.expectEqual(@as(usize, 0), stats.lane(.broad_scan_scratch).stored_bytes);
    try std.testing.expectEqual(@as(usize, 4), stats.lane(.broad_scan_scratch).rejected_bytes);
}

test "parquet object range cache lets protected sidecars displace scratch" {
    const alloc = std.testing.allocator;
    var policy = ObjectRangeCachePolicy{};
    policy.setTotalLimit(10);
    policy.protectLane(.serving_sidecar);
    var cache = ObjectRangeCache{ .policy = policy };
    defer cache.deinit(alloc);

    const CountingReader = struct {
        body: []const u8,
        read_count: usize = 0,

        fn reader(self: *@This()) ObjectRangeReader {
            return .{
                .ctx = self,
                .read_range_alloc = readRangeAlloc,
            };
        }

        fn readRangeAlloc(
            ctx: *anyopaque,
            a: Allocator,
            bucket: []const u8,
            key: []const u8,
            offset: u64,
            len: usize,
        ) ![]u8 {
            _ = bucket;
            _ = key;
            const self: *@This() = @ptrCast(@alignCast(ctx));
            const start: usize = std.math.cast(usize, offset) orelse return error.InvalidLakeRangeRead;
            if (start > self.body.len or len > self.body.len - start) return error.InvalidLakeRangeRead;
            self.read_count += 1;
            return try a.dupe(u8, self.body[start..][0..len]);
        }
    };
    var reader = CountingReader{ .body = "abcdefghijklmnop" };
    const object = range_io.ObjectRef{
        .bucket = "bucket",
        .key = "events/sidecar-and-scratch.bin",
        .byte_len = 16,
        .version = .{ .etag = "etag-a" },
    };
    const scratch_read = range_io.RangeRead{
        .object = object,
        .range = .{ .offset = 8, .len = 4 },
        .purpose = .broad_scan_scratch,
    };
    const sidecar_read = range_io.RangeRead{
        .object = object,
        .range = .{ .offset = 0, .len = 8 },
        .purpose = .sidecar_payload,
    };

    const scratch_first = try cache.readAlloc(alloc, reader.reader(), scratch_read);
    defer alloc.free(scratch_first);
    const sidecar_first = try cache.readAlloc(alloc, reader.reader(), sidecar_read);
    defer alloc.free(sidecar_first);
    const sidecar_second = try cache.readAlloc(alloc, reader.reader(), sidecar_read);
    defer alloc.free(sidecar_second);

    const stats = cache.statsSnapshot();
    try std.testing.expectEqual(@as(usize, 2), reader.read_count);
    try std.testing.expectEqual(@as(usize, 1), stats.hits);
    try std.testing.expectEqual(@as(usize, 8), stats.stored_bytes);
    try std.testing.expectEqual(@as(usize, 4), stats.evicted_bytes);
    try std.testing.expectEqual(@as(usize, 8), stats.lane(.serving_sidecar).stored_bytes);
    try std.testing.expectEqual(@as(usize, 1), stats.lane(.serving_sidecar).hits);
    try std.testing.expectEqual(@as(usize, 0), stats.lane(.broad_scan_scratch).stored_bytes);
    try std.testing.expectEqual(@as(usize, 4), stats.lane(.broad_scan_scratch).evicted_bytes);
}

test "parquet object range cache serving defaults protect metadata and sidecars" {
    const alloc = std.testing.allocator;
    var cache = ObjectRangeCache.initWithLakeServingDefaults(16);
    defer cache.deinit(alloc);

    const CountingReader = struct {
        body: []const u8,
        read_count: usize = 0,

        fn reader(self: *@This()) ObjectRangeReader {
            return .{
                .ctx = self,
                .read_range_alloc = readRangeAlloc,
            };
        }

        fn readRangeAlloc(
            ctx: *anyopaque,
            a: Allocator,
            bucket: []const u8,
            key: []const u8,
            offset: u64,
            len: usize,
        ) ![]u8 {
            _ = bucket;
            _ = key;
            const self: *@This() = @ptrCast(@alignCast(ctx));
            const start: usize = std.math.cast(usize, offset) orelse return error.InvalidLakeRangeRead;
            if (start > self.body.len or len > self.body.len - start) return error.InvalidLakeRangeRead;
            self.read_count += 1;
            return try a.dupe(u8, self.body[start..][0..len]);
        }
    };
    var reader = CountingReader{ .body = "abcdefghijklmnopqrstuvwxyz" };
    const object = range_io.ObjectRef{
        .bucket = "bucket",
        .key = "events/default-policy.bin",
        .byte_len = 26,
        .version = .{ .etag = "etag-a" },
    };
    const metadata_read = range_io.RangeRead{
        .object = object,
        .range = .{ .offset = 0, .len = 8 },
        .purpose = .parquet_footer,
    };
    const sidecar_read = range_io.RangeRead{
        .object = object,
        .range = .{ .offset = 8, .len = 8 },
        .purpose = .sidecar_payload,
    };
    const scratch_read = range_io.RangeRead{
        .object = object,
        .range = .{ .offset = 16, .len = 2 },
        .purpose = .broad_scan_scratch,
    };

    try std.testing.expect(cache.policy.isProtected(.metadata));
    try std.testing.expect(cache.policy.isProtected(.serving_sidecar));
    try std.testing.expectEqual(@as(?usize, 2), cache.policy.laneLimit(.broad_scan_scratch));

    const metadata_first = try cache.readAlloc(alloc, reader.reader(), metadata_read);
    defer alloc.free(metadata_first);
    const sidecar_first = try cache.readAlloc(alloc, reader.reader(), sidecar_read);
    defer alloc.free(sidecar_first);
    const scratch_first = try cache.readAlloc(alloc, reader.reader(), scratch_read);
    defer alloc.free(scratch_first);
    const metadata_second = try cache.readAlloc(alloc, reader.reader(), metadata_read);
    defer alloc.free(metadata_second);
    const sidecar_second = try cache.readAlloc(alloc, reader.reader(), sidecar_read);
    defer alloc.free(sidecar_second);

    const stats = cache.statsSnapshot();
    try std.testing.expectEqual(@as(usize, 3), reader.read_count);
    try std.testing.expectEqual(@as(usize, 2), stats.hits);
    try std.testing.expectEqual(@as(usize, 16), stats.stored_bytes);
    try std.testing.expectEqual(@as(usize, 2), stats.rejected_bytes);
    try std.testing.expectEqual(@as(usize, 8), stats.lane(.metadata).stored_bytes);
    try std.testing.expectEqual(@as(usize, 1), stats.lane(.metadata).hits);
    try std.testing.expectEqual(@as(usize, 8), stats.lane(.serving_sidecar).stored_bytes);
    try std.testing.expectEqual(@as(usize, 1), stats.lane(.serving_sidecar).hits);
    try std.testing.expectEqual(@as(usize, 0), stats.lane(.broad_scan_scratch).stored_bytes);
    try std.testing.expectEqual(@as(usize, 2), stats.lane(.broad_scan_scratch).rejected_bytes);
}

test "parquet object range rows query filters on unprojected column" {
    const alloc = std.testing.allocator;

    var amount_chunk = std.ArrayListUnmanaged(u8).empty;
    defer amount_chunk.deinit(alloc);
    try appendPlainI64DataPage(&amount_chunk, alloc, &[_]i64{ 10, 20, 30 });
    var tenant_chunk = std.ArrayListUnmanaged(u8).empty;
    defer tenant_chunk.deinit(alloc);
    try appendPlainI64DataPage(&tenant_chunk, alloc, &[_]i64{ 7, 8, 9 });

    const amount_offset: usize = 100;
    const tenant_offset = amount_offset + amount_chunk.items.len;
    const object_len = tenant_offset + tenant_chunk.items.len;
    const object_bytes = try alloc.alloc(u8, object_len);
    defer alloc.free(object_bytes);
    @memset(object_bytes, 0);
    @memcpy(object_bytes[amount_offset..][0..amount_chunk.items.len], amount_chunk.items);
    @memcpy(object_bytes[tenant_offset..][0..tenant_chunk.items.len], tenant_chunk.items);

    const CountingRangeReader = struct {
        bucket: []const u8,
        key: []const u8,
        body: []const u8,
        read_count: usize = 0,

        fn reader(self: *@This()) ObjectRangeReader {
            return .{
                .ctx = self,
                .read_range_alloc = readRangeAlloc,
            };
        }

        fn readRangeAlloc(
            ctx: *anyopaque,
            a: Allocator,
            bucket: []const u8,
            key: []const u8,
            offset: u64,
            len: usize,
        ) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (!std.mem.eql(u8, self.bucket, bucket)) return error.ObjectNotFound;
            if (!std.mem.eql(u8, self.key, key)) return error.ObjectNotFound;
            const start: usize = std.math.cast(usize, offset) orelse return error.InvalidLakeRangeRead;
            if (start > self.body.len or len > self.body.len - start) return error.InvalidLakeRangeRead;
            self.read_count += 1;
            return try a.dupe(u8, self.body[start..][0..len]);
        }
    };
    var range_reader = CountingRangeReader{
        .bucket = "bucket",
        .key = "events/part-a.parquet",
        .body = object_bytes,
    };

    var inventory = external_source.Inventory{
        .format = .parquet,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/events"),
        .snapshot_id = try alloc.dupe(u8, "sha256:objects"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "part-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/events/part-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-a"),
        .byte_len = object_len,
        .row_count = 3,
        .row_groups = try alloc.dupe(external_source.RowGroup, &[_]external_source.RowGroup{.{
            .ordinal = 0,
            .row_count = 3,
            .file_offset = amount_offset,
            .total_byte_len = amount_chunk.items.len + tenant_chunk.items.len,
            .column_chunks = try alloc.dupe(external_source.ColumnChunk, &[_]external_source.ColumnChunk{
                .{
                    .column_id = try alloc.dupe(u8, "amount"),
                    .file_offset = amount_offset,
                    .compressed_len = amount_chunk.items.len,
                    .uncompressed_len = amount_chunk.items.len,
                    .encoding = try alloc.dupe(u8, "plain"),
                },
                .{
                    .column_id = try alloc.dupe(u8, "tenant"),
                    .file_offset = tenant_offset,
                    .compressed_len = tenant_chunk.items.len,
                    .uncompressed_len = tenant_chunk.items.len,
                    .encoding = try alloc.dupe(u8, "plain"),
                },
            }),
        }}),
    };
    try inventory.validate();

    const projection = [_][]const u8{"amount"};
    var result = try querySupportedI64ObjectRangeRowsAlloc(alloc, .{
        .binding = .{
            .table_id = "events",
            .format = .parquet,
            .source_uri = "s3://bucket/events",
            .snapshot_mode = .{ .object_version_digest = "sha256:objects" },
            .schema_fingerprint = "schema-v1",
        },
        .reader = range_reader.reader(),
        .inventory = inventory,
        .projected_columns = &projection,
        .predicate = .{
            .column = "tenant",
            .op = .eq_i64,
            .i64_value = 8,
        },
        .coalesce_options = .{},
    });
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), range_reader.read_count);
    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expectEqual(@as(usize, 1), result.rows[0].cells.len);
    try std.testing.expectEqualStrings("amount", result.rows[0].cells[0].name);
    try std.testing.expectEqual(@as(i64, 20), result.rows[0].cells[0].value.?.i64);
    try std.testing.expectEqual(@as(u64, 1), result.rows[0].row_ref.external.row_ordinal);
}

test "parquet object range rows query rejects sidecars without binding" {
    const alloc = std.testing.allocator;
    const DummyRangeReader = struct {
        fn reader(self: *@This()) ObjectRangeReader {
            return .{
                .ctx = self,
                .read_range_alloc = readRangeAlloc,
            };
        }

        fn readRangeAlloc(
            _: *anyopaque,
            _: Allocator,
            _: []const u8,
            _: []const u8,
            _: u64,
            _: usize,
        ) ![]u8 {
            return error.TestUnexpectedResult;
        }
    };
    var reader = DummyRangeReader{};

    var inventory = external_source.Inventory{
        .format = .parquet,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/events"),
        .snapshot_id = try alloc.dupe(u8, "sha256:objects"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 0),
    };
    defer inventory.deinit(alloc);

    const projection = [_][]const u8{"amount"};
    const desired = [_]lake_sidecar_selection.DesiredSidecar{.{ .kind = .vector }};
    try std.testing.expectError(error.InvalidLakeSidecarSelection, querySupportedI64ObjectRangeRowsAlloc(alloc, .{
        .reader = reader.reader(),
        .inventory = inventory,
        .projected_columns = &projection,
        .desired_sidecars = &desired,
    }));
}

test "parquet object range rows query prunes row groups with i64 statistics" {
    const alloc = std.testing.allocator;

    var first_chunk = std.ArrayListUnmanaged(u8).empty;
    defer first_chunk.deinit(alloc);
    try appendPlainI64DataPage(&first_chunk, alloc, &[_]i64{ 10, 20 });
    var second_chunk = std.ArrayListUnmanaged(u8).empty;
    defer second_chunk.deinit(alloc);
    try appendPlainI64DataPage(&second_chunk, alloc, &[_]i64{ 30, 40 });

    const first_offset: usize = 100;
    const second_offset = first_offset + first_chunk.items.len + 32;
    const object_len = second_offset + second_chunk.items.len;
    const object_bytes = try alloc.alloc(u8, object_len);
    defer alloc.free(object_bytes);
    @memset(object_bytes, 0);
    @memcpy(object_bytes[first_offset..][0..first_chunk.items.len], first_chunk.items);
    @memcpy(object_bytes[second_offset..][0..second_chunk.items.len], second_chunk.items);

    const CountingRangeReader = struct {
        bucket: []const u8,
        key: []const u8,
        body: []const u8,
        read_count: usize = 0,

        fn reader(self: *@This()) ObjectRangeReader {
            return .{
                .ctx = self,
                .read_range_alloc = readRangeAlloc,
            };
        }

        fn readRangeAlloc(
            ctx: *anyopaque,
            a: Allocator,
            bucket: []const u8,
            key: []const u8,
            offset: u64,
            len: usize,
        ) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (!std.mem.eql(u8, self.bucket, bucket)) return error.ObjectNotFound;
            if (!std.mem.eql(u8, self.key, key)) return error.ObjectNotFound;
            const start: usize = std.math.cast(usize, offset) orelse return error.InvalidLakeRangeRead;
            if (start > self.body.len or len > self.body.len - start) return error.InvalidLakeRangeRead;
            self.read_count += 1;
            return try a.dupe(u8, self.body[start..][0..len]);
        }
    };
    var range_reader = CountingRangeReader{
        .bucket = "bucket",
        .key = "events/part-a.parquet",
        .body = object_bytes,
    };

    var inventory = external_source.Inventory{
        .format = .parquet,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/events"),
        .snapshot_id = try alloc.dupe(u8, "sha256:objects"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "part-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/events/part-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-a"),
        .byte_len = object_len,
        .row_count = 4,
        .row_groups = try alloc.dupe(external_source.RowGroup, &[_]external_source.RowGroup{
            .{
                .ordinal = 0,
                .row_count = 2,
                .file_offset = first_offset,
                .total_byte_len = first_chunk.items.len,
                .column_chunks = try alloc.dupe(external_source.ColumnChunk, &[_]external_source.ColumnChunk{.{
                    .column_id = try alloc.dupe(u8, "amount"),
                    .file_offset = first_offset,
                    .compressed_len = first_chunk.items.len,
                    .uncompressed_len = first_chunk.items.len,
                    .encoding = try alloc.dupe(u8, "plain"),
                    .physical_type = try alloc.dupe(u8, "int64"),
                    .stats_min_i64 = 10,
                    .stats_max_i64 = 20,
                }}),
            },
            .{
                .ordinal = 1,
                .row_count = 2,
                .file_offset = second_offset,
                .total_byte_len = second_chunk.items.len,
                .column_chunks = try alloc.dupe(external_source.ColumnChunk, &[_]external_source.ColumnChunk{.{
                    .column_id = try alloc.dupe(u8, "amount"),
                    .file_offset = second_offset,
                    .compressed_len = second_chunk.items.len,
                    .uncompressed_len = second_chunk.items.len,
                    .encoding = try alloc.dupe(u8, "plain"),
                    .physical_type = try alloc.dupe(u8, "int64"),
                    .stats_min_i64 = 30,
                    .stats_max_i64 = 40,
                }}),
            },
        }),
    };
    try inventory.validate();

    const projection = [_][]const u8{"amount"};
    var result = try querySupportedI64ObjectRangeRowsAlloc(alloc, .{
        .binding = .{
            .table_id = "events",
            .format = .parquet,
            .source_uri = "s3://bucket/events",
            .snapshot_mode = .{ .object_version_digest = "sha256:objects" },
            .schema_fingerprint = "schema-v1",
        },
        .reader = range_reader.reader(),
        .inventory = inventory,
        .projected_columns = &projection,
        .predicate = .{
            .column = "amount",
            .op = .eq_i64,
            .i64_value = 20,
        },
        .coalesce_options = .{},
    });
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), range_reader.read_count);
    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expectEqual(@as(i64, 20), result.rows[0].cells[0].value.?.i64);
    try std.testing.expectEqual(@as(u32, 0), result.rows[0].row_ref.external.row_group_ordinal);

    var empty = try querySupportedI64ObjectRangeRowsAlloc(alloc, .{
        .binding = .{
            .table_id = "events",
            .format = .parquet,
            .source_uri = "s3://bucket/events",
            .snapshot_mode = .{ .object_version_digest = "sha256:objects" },
            .schema_fingerprint = "schema-v1",
        },
        .reader = range_reader.reader(),
        .inventory = inventory,
        .projected_columns = &projection,
        .predicate = .{
            .column = "amount",
            .op = .eq_i64,
            .i64_value = 99,
        },
        .coalesce_options = .{},
    });
    defer empty.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), range_reader.read_count);
    try std.testing.expectEqual(@as(usize, 0), empty.rows.len);
    try std.testing.expectEqual(@as(u32, 0), empty.total);
}

test "parquet object range rows query prunes row groups with byte statistics" {
    const alloc = std.testing.allocator;

    var first_chunk = std.ArrayListUnmanaged(u8).empty;
    defer first_chunk.deinit(alloc);
    try appendPlainByteArrayDataPage(&first_chunk, alloc, &[_][]const u8{ "alpha", "beta" });
    var second_chunk = std.ArrayListUnmanaged(u8).empty;
    defer second_chunk.deinit(alloc);
    try appendPlainByteArrayDataPage(&second_chunk, alloc, &[_][]const u8{ "delta", "echo" });

    const first_offset: usize = 128;
    const second_offset = first_offset + first_chunk.items.len + 64;
    const object_len = second_offset + second_chunk.items.len;
    const object_bytes = try alloc.alloc(u8, object_len);
    defer alloc.free(object_bytes);
    @memset(object_bytes, 0);
    @memcpy(object_bytes[first_offset..][0..first_chunk.items.len], first_chunk.items);
    @memcpy(object_bytes[second_offset..][0..second_chunk.items.len], second_chunk.items);

    const CountingRangeReader = struct {
        bucket: []const u8,
        key: []const u8,
        body: []const u8,
        read_count: usize = 0,

        fn reader(self: *@This()) ObjectRangeReader {
            return .{
                .ctx = self,
                .read_range_alloc = readRangeAlloc,
            };
        }

        fn readRangeAlloc(
            ctx: *anyopaque,
            a: Allocator,
            bucket: []const u8,
            key: []const u8,
            offset: u64,
            len: usize,
        ) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (!std.mem.eql(u8, self.bucket, bucket)) return error.ObjectNotFound;
            if (!std.mem.eql(u8, self.key, key)) return error.ObjectNotFound;
            const start: usize = std.math.cast(usize, offset) orelse return error.InvalidLakeRangeRead;
            if (start > self.body.len or len > self.body.len - start) return error.InvalidLakeRangeRead;
            self.read_count += 1;
            return try a.dupe(u8, self.body[start..][0..len]);
        }
    };
    var range_reader = CountingRangeReader{
        .bucket = "bucket",
        .key = "events/part-a.parquet",
        .body = object_bytes,
    };

    var inventory = external_source.Inventory{
        .format = .parquet,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/events"),
        .snapshot_id = try alloc.dupe(u8, "sha256:objects"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "part-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/events/part-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-a"),
        .byte_len = object_len,
        .row_count = 4,
        .row_groups = try alloc.dupe(external_source.RowGroup, &[_]external_source.RowGroup{
            .{
                .ordinal = 0,
                .row_count = 2,
                .file_offset = first_offset,
                .total_byte_len = first_chunk.items.len,
                .column_chunks = try alloc.dupe(external_source.ColumnChunk, &[_]external_source.ColumnChunk{.{
                    .column_id = try alloc.dupe(u8, "tag"),
                    .file_offset = first_offset,
                    .compressed_len = first_chunk.items.len,
                    .uncompressed_len = first_chunk.items.len,
                    .encoding = try alloc.dupe(u8, "plain"),
                    .physical_type = try alloc.dupe(u8, "byte_array"),
                    .stats_min_bytes = try alloc.dupe(u8, "alpha"),
                    .stats_max_bytes = try alloc.dupe(u8, "beta"),
                }}),
            },
            .{
                .ordinal = 1,
                .row_count = 2,
                .file_offset = second_offset,
                .total_byte_len = second_chunk.items.len,
                .column_chunks = try alloc.dupe(external_source.ColumnChunk, &[_]external_source.ColumnChunk{.{
                    .column_id = try alloc.dupe(u8, "tag"),
                    .file_offset = second_offset,
                    .compressed_len = second_chunk.items.len,
                    .uncompressed_len = second_chunk.items.len,
                    .encoding = try alloc.dupe(u8, "plain"),
                    .physical_type = try alloc.dupe(u8, "byte_array"),
                    .stats_min_bytes = try alloc.dupe(u8, "delta"),
                    .stats_max_bytes = try alloc.dupe(u8, "echo"),
                }}),
            },
        }),
    };
    try inventory.validate();

    const projection = [_][]const u8{"tag"};
    var result = try querySupportedI64ObjectRangeRowsAlloc(alloc, .{
        .binding = .{
            .table_id = "events",
            .format = .parquet,
            .source_uri = "s3://bucket/events",
            .snapshot_mode = .{ .object_version_digest = "sha256:objects" },
            .schema_fingerprint = "schema-v1",
        },
        .reader = range_reader.reader(),
        .inventory = inventory,
        .projected_columns = &projection,
        .predicate = .{
            .column = "tag",
            .op = .eq_bytes,
            .bytes_value = "delta",
        },
        .coalesce_options = .{},
    });
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), range_reader.read_count);
    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expectEqualStrings("delta", result.rows[0].cells[0].value.?.bytes);
    try std.testing.expectEqual(@as(u32, 1), result.rows[0].row_ref.external.row_group_ordinal);
}

test "parquet object range rows query prunes row groups with boolean statistics" {
    const alloc = std.testing.allocator;

    var first_chunk = std.ArrayListUnmanaged(u8).empty;
    defer first_chunk.deinit(alloc);
    try appendPlainBoolDataPage(&first_chunk, alloc, &[_]bool{ false, false });
    var second_chunk = std.ArrayListUnmanaged(u8).empty;
    defer second_chunk.deinit(alloc);
    try appendPlainBoolDataPage(&second_chunk, alloc, &[_]bool{ true, true });

    const first_offset: usize = 160;
    const second_offset = first_offset + first_chunk.items.len + 64;
    const object_len = second_offset + second_chunk.items.len;
    const object_bytes = try alloc.alloc(u8, object_len);
    defer alloc.free(object_bytes);
    @memset(object_bytes, 0);
    @memcpy(object_bytes[first_offset..][0..first_chunk.items.len], first_chunk.items);
    @memcpy(object_bytes[second_offset..][0..second_chunk.items.len], second_chunk.items);

    const CountingRangeReader = struct {
        bucket: []const u8,
        key: []const u8,
        body: []const u8,
        read_count: usize = 0,

        fn reader(self: *@This()) ObjectRangeReader {
            return .{
                .ctx = self,
                .read_range_alloc = readRangeAlloc,
            };
        }

        fn readRangeAlloc(
            ctx: *anyopaque,
            a: Allocator,
            bucket: []const u8,
            key: []const u8,
            offset: u64,
            len: usize,
        ) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (!std.mem.eql(u8, self.bucket, bucket)) return error.ObjectNotFound;
            if (!std.mem.eql(u8, self.key, key)) return error.ObjectNotFound;
            const start: usize = std.math.cast(usize, offset) orelse return error.InvalidLakeRangeRead;
            if (start > self.body.len or len > self.body.len - start) return error.InvalidLakeRangeRead;
            self.read_count += 1;
            return try a.dupe(u8, self.body[start..][0..len]);
        }
    };
    var range_reader = CountingRangeReader{
        .bucket = "bucket",
        .key = "events/part-a.parquet",
        .body = object_bytes,
    };

    var inventory = external_source.Inventory{
        .format = .parquet,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/events"),
        .snapshot_id = try alloc.dupe(u8, "sha256:objects"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "part-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/events/part-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-a"),
        .byte_len = object_len,
        .row_count = 4,
        .row_groups = try alloc.dupe(external_source.RowGroup, &[_]external_source.RowGroup{
            .{
                .ordinal = 0,
                .row_count = 2,
                .file_offset = first_offset,
                .total_byte_len = first_chunk.items.len,
                .column_chunks = try alloc.dupe(external_source.ColumnChunk, &[_]external_source.ColumnChunk{.{
                    .column_id = try alloc.dupe(u8, "active"),
                    .file_offset = first_offset,
                    .compressed_len = first_chunk.items.len,
                    .uncompressed_len = first_chunk.items.len,
                    .encoding = try alloc.dupe(u8, "plain"),
                    .physical_type = try alloc.dupe(u8, "boolean"),
                    .stats_min_bool = false,
                    .stats_max_bool = false,
                }}),
            },
            .{
                .ordinal = 1,
                .row_count = 2,
                .file_offset = second_offset,
                .total_byte_len = second_chunk.items.len,
                .column_chunks = try alloc.dupe(external_source.ColumnChunk, &[_]external_source.ColumnChunk{.{
                    .column_id = try alloc.dupe(u8, "active"),
                    .file_offset = second_offset,
                    .compressed_len = second_chunk.items.len,
                    .uncompressed_len = second_chunk.items.len,
                    .encoding = try alloc.dupe(u8, "plain"),
                    .physical_type = try alloc.dupe(u8, "boolean"),
                    .stats_min_bool = true,
                    .stats_max_bool = true,
                }}),
            },
        }),
    };
    try inventory.validate();

    const projection = [_][]const u8{"active"};
    var result = try querySupportedI64ObjectRangeRowsAlloc(alloc, .{
        .binding = .{
            .table_id = "events",
            .format = .parquet,
            .source_uri = "s3://bucket/events",
            .snapshot_mode = .{ .object_version_digest = "sha256:objects" },
            .schema_fingerprint = "schema-v1",
        },
        .reader = range_reader.reader(),
        .inventory = inventory,
        .projected_columns = &projection,
        .predicate = .{
            .column = "active",
            .op = .eq_bool,
            .bool_value = true,
        },
        .coalesce_options = .{},
    });
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), range_reader.read_count);
    try std.testing.expectEqual(@as(usize, 2), result.rows.len);
    try std.testing.expectEqual(true, result.rows[0].cells[0].value.?.bool);
    try std.testing.expectEqual(@as(u32, 1), result.rows[0].row_ref.external.row_group_ordinal);
}

test "parquet object range rows query prunes row groups with f64 statistics" {
    const alloc = std.testing.allocator;

    var first_chunk = std.ArrayListUnmanaged(u8).empty;
    defer first_chunk.deinit(alloc);
    try appendPlainF64DataPage(&first_chunk, alloc, &[_]f64{ 1.25, 1.5 });
    var second_chunk = std.ArrayListUnmanaged(u8).empty;
    defer second_chunk.deinit(alloc);
    try appendPlainF64DataPage(&second_chunk, alloc, &[_]f64{ 2.5, 3.5 });

    const first_offset: usize = 192;
    const second_offset = first_offset + first_chunk.items.len + 64;
    const object_len = second_offset + second_chunk.items.len;
    const object_bytes = try alloc.alloc(u8, object_len);
    defer alloc.free(object_bytes);
    @memset(object_bytes, 0);
    @memcpy(object_bytes[first_offset..][0..first_chunk.items.len], first_chunk.items);
    @memcpy(object_bytes[second_offset..][0..second_chunk.items.len], second_chunk.items);

    const CountingRangeReader = struct {
        bucket: []const u8,
        key: []const u8,
        body: []const u8,
        read_count: usize = 0,

        fn reader(self: *@This()) ObjectRangeReader {
            return .{
                .ctx = self,
                .read_range_alloc = readRangeAlloc,
            };
        }

        fn readRangeAlloc(
            ctx: *anyopaque,
            a: Allocator,
            bucket: []const u8,
            key: []const u8,
            offset: u64,
            len: usize,
        ) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (!std.mem.eql(u8, self.bucket, bucket)) return error.ObjectNotFound;
            if (!std.mem.eql(u8, self.key, key)) return error.ObjectNotFound;
            const start: usize = std.math.cast(usize, offset) orelse return error.InvalidLakeRangeRead;
            if (start > self.body.len or len > self.body.len - start) return error.InvalidLakeRangeRead;
            self.read_count += 1;
            return try a.dupe(u8, self.body[start..][0..len]);
        }
    };
    var range_reader = CountingRangeReader{
        .bucket = "bucket",
        .key = "events/part-a.parquet",
        .body = object_bytes,
    };

    var inventory = external_source.Inventory{
        .format = .parquet,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/events"),
        .snapshot_id = try alloc.dupe(u8, "sha256:objects"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "part-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/events/part-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-a"),
        .byte_len = object_len,
        .row_count = 4,
        .row_groups = try alloc.dupe(external_source.RowGroup, &[_]external_source.RowGroup{
            .{
                .ordinal = 0,
                .row_count = 2,
                .file_offset = first_offset,
                .total_byte_len = first_chunk.items.len,
                .column_chunks = try alloc.dupe(external_source.ColumnChunk, &[_]external_source.ColumnChunk{.{
                    .column_id = try alloc.dupe(u8, "score"),
                    .file_offset = first_offset,
                    .compressed_len = first_chunk.items.len,
                    .uncompressed_len = first_chunk.items.len,
                    .encoding = try alloc.dupe(u8, "plain"),
                    .physical_type = try alloc.dupe(u8, "double"),
                    .stats_min_f64 = 1.25,
                    .stats_max_f64 = 1.5,
                }}),
            },
            .{
                .ordinal = 1,
                .row_count = 2,
                .file_offset = second_offset,
                .total_byte_len = second_chunk.items.len,
                .column_chunks = try alloc.dupe(external_source.ColumnChunk, &[_]external_source.ColumnChunk{.{
                    .column_id = try alloc.dupe(u8, "score"),
                    .file_offset = second_offset,
                    .compressed_len = second_chunk.items.len,
                    .uncompressed_len = second_chunk.items.len,
                    .encoding = try alloc.dupe(u8, "plain"),
                    .physical_type = try alloc.dupe(u8, "double"),
                    .stats_min_f64 = 2.5,
                    .stats_max_f64 = 3.5,
                }}),
            },
        }),
    };
    try inventory.validate();

    const projection = [_][]const u8{"score"};
    var result = try querySupportedI64ObjectRangeRowsAlloc(alloc, .{
        .binding = .{
            .table_id = "events",
            .format = .parquet,
            .source_uri = "s3://bucket/events",
            .snapshot_mode = .{ .object_version_digest = "sha256:objects" },
            .schema_fingerprint = "schema-v1",
        },
        .reader = range_reader.reader(),
        .inventory = inventory,
        .projected_columns = &projection,
        .predicate = .{
            .column = "score",
            .op = .eq_f64,
            .f64_value = 2.5,
        },
        .coalesce_options = .{},
    });
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), range_reader.read_count);
    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expectEqual(@as(f64, 2.5), result.rows[0].cells[0].value.?.f64);
    try std.testing.expectEqual(@as(u32, 1), result.rows[0].row_ref.external.row_group_ordinal);
}

test "parquet row-group predicate pruning bridges exact i64 and f64 statistics" {
    const i64_chunks = [_]external_source.ColumnChunk{.{
        .column_id = @constCast("amount"),
        .file_offset = 0,
        .compressed_len = 1,
        .stats_min_i64 = 10,
        .stats_max_i64 = 20,
    }};
    const i64_group = external_source.RowGroup{
        .ordinal = 0,
        .row_count = 1,
        .column_chunks = @constCast(i64_chunks[0..]),
    };
    try std.testing.expect(rowGroupMayMatchPredicate(i64_group, .{
        .column = "amount",
        .op = .eq_f64,
        .f64_value = 20.0,
    }));
    try std.testing.expect(!rowGroupMayMatchPredicate(i64_group, .{
        .column = "amount",
        .op = .eq_f64,
        .f64_value = 20.5,
    }));

    const f64_chunks = [_]external_source.ColumnChunk{.{
        .column_id = @constCast("score"),
        .file_offset = 0,
        .compressed_len = 1,
        .stats_min_f64 = 1.5,
        .stats_max_f64 = 2.5,
    }};
    const f64_group = external_source.RowGroup{
        .ordinal = 0,
        .row_count = 1,
        .column_chunks = @constCast(f64_chunks[0..]),
    };
    try std.testing.expect(rowGroupMayMatchPredicate(f64_group, .{
        .column = "score",
        .op = .eq_i64,
        .i64_value = 2,
    }));
    try std.testing.expect(!rowGroupMayMatchPredicate(f64_group, .{
        .column = "score",
        .op = .eq_i64,
        .i64_value = 3,
    }));
}

test "iceberg object range planner prunes files with partition equality metadata" {
    const alloc = std.testing.allocator;
    var inventory = external_source.Inventory{
        .format = .iceberg,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/events"),
        .snapshot_id = try alloc.dupe(u8, "12"),
        .schema_fingerprint = try alloc.dupe(u8, "iceberg-schema:7"),
        .files = try alloc.alloc(external_source.FileEntry, 2),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "part-east.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/events/part-east.parquet"),
        .version_id = try alloc.dupe(u8, "iceberg:v1:east"),
        .byte_len = 1024,
        .row_count = 2,
        .partition_values = try alloc.dupe(external_source.PartitionValue, &[_]external_source.PartitionValue{.{
            .column_id = try alloc.dupe(u8, "region"),
            .string_value = try alloc.dupe(u8, "us-east"),
        }}),
        .row_groups = try alloc.dupe(external_source.RowGroup, &[_]external_source.RowGroup{.{
            .ordinal = 0,
            .row_count = 2,
            .file_offset = 100,
            .total_byte_len = 16,
            .column_chunks = try alloc.dupe(external_source.ColumnChunk, &[_]external_source.ColumnChunk{.{
                .column_id = try alloc.dupe(u8, "amount"),
                .file_offset = 100,
                .compressed_len = 16,
                .uncompressed_len = 16,
                .encoding = try alloc.dupe(u8, "plain"),
                .physical_type = try alloc.dupe(u8, "int64"),
                .field_id = 1,
            }}),
        }}),
    };
    inventory.files[1] = .{
        .file_id = try alloc.dupe(u8, "part-west.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/events/part-west.parquet"),
        .version_id = try alloc.dupe(u8, "iceberg:v1:west"),
        .byte_len = 1024,
        .row_count = 2,
        .partition_values = try alloc.dupe(external_source.PartitionValue, &[_]external_source.PartitionValue{.{
            .column_id = try alloc.dupe(u8, "region"),
            .string_value = try alloc.dupe(u8, "us-west"),
        }}),
        .row_groups = try alloc.dupe(external_source.RowGroup, &[_]external_source.RowGroup{.{
            .ordinal = 0,
            .row_count = 2,
            .file_offset = 100,
            .total_byte_len = 32,
            .column_chunks = try alloc.dupe(external_source.ColumnChunk, &[_]external_source.ColumnChunk{
                .{
                    .column_id = try alloc.dupe(u8, "amount"),
                    .file_offset = 100,
                    .compressed_len = 16,
                    .uncompressed_len = 16,
                    .encoding = try alloc.dupe(u8, "plain"),
                    .physical_type = try alloc.dupe(u8, "int64"),
                    .field_id = 1,
                },
                .{
                    .column_id = try alloc.dupe(u8, "region"),
                    .file_offset = 116,
                    .compressed_len = 16,
                    .uncompressed_len = 16,
                    .encoding = try alloc.dupe(u8, "plain"),
                    .physical_type = try alloc.dupe(u8, "byte_array"),
                    .field_id = 2,
                },
            }),
        }}),
    };
    try inventory.validate();

    var plan = try planSupportedI64ObjectRangeRowGroupsForQueryAlloc(
        alloc,
        inventory,
        &[_][]const u8{ "amount", "region" },
        .{
            .column = "region",
            .op = .eq_bytes,
            .bytes_value = "us-west",
        },
    );
    defer plan.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), plan.row_groups.len);
    try std.testing.expectEqualStrings("part-west.parquet", plan.row_groups[0].file_id);
}

test "iceberg object range planner requires projected field ids" {
    const alloc = std.testing.allocator;

    var chunks = [_]external_source.ColumnChunk{.{
        .column_id = @constCast("amount"),
        .file_offset = 100,
        .compressed_len = 16,
        .uncompressed_len = 16,
        .encoding = @constCast("plain"),
        .physical_type = @constCast("int64"),
    }};
    var row_groups = [_]external_source.RowGroup{.{
        .ordinal = 0,
        .row_count = 2,
        .file_offset = 100,
        .total_byte_len = 16,
        .column_chunks = chunks[0..],
    }};
    var files = [_]external_source.FileEntry{.{
        .file_id = @constCast("part-a.parquet"),
        .object_uri = @constCast("s3://bucket/events/part-a.parquet"),
        .version_id = @constCast("iceberg:v1:a"),
        .byte_len = 1024,
        .row_count = 2,
        .row_groups = row_groups[0..],
    }};
    const inventory = external_source.Inventory{
        .format = .iceberg,
        .source_id = @constCast("events"),
        .source_uri = @constCast("s3://bucket/events"),
        .snapshot_id = @constCast("12"),
        .schema_fingerprint = @constCast("iceberg-schema:7"),
        .files = files[0..],
    };
    const projection = [_][]const u8{"amount"};

    try std.testing.expectError(
        error.UnsupportedIcebergSchemaEvolution,
        planSupportedI64ObjectRangeRowGroupsAlloc(alloc, inventory, &projection),
    );

    chunks[0].field_id = 1;
    var plan = try planSupportedI64ObjectRangeRowGroupsAlloc(alloc, inventory, &projection);
    defer plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), plan.row_groups.len);
    try std.testing.expectEqualStrings("part-a.parquet", plan.row_groups[0].file_id);
}

test "iceberg partition predicate pruning supports typed equality metadata" {
    const int_partition = external_source.PartitionValue{
        .column_id = @constCast("bucket_id"),
        .string_value = @constCast("42"),
    };
    try std.testing.expect(partitionMayMatchPredicate(int_partition, .{
        .column = "bucket_id",
        .op = .eq_i64,
        .i64_value = 42,
    }));
    try std.testing.expect(!partitionMayMatchPredicate(int_partition, .{
        .column = "bucket_id",
        .op = .eq_i64,
        .i64_value = 7,
    }));

    const bool_partition = external_source.PartitionValue{
        .column_id = @constCast("is_active"),
        .string_value = @constCast("true"),
    };
    try std.testing.expect(partitionMayMatchPredicate(bool_partition, .{
        .column = "is_active",
        .op = .eq_bool,
        .bool_value = true,
    }));
    try std.testing.expect(!partitionMayMatchPredicate(bool_partition, .{
        .column = "is_active",
        .op = .eq_bool,
        .bool_value = false,
    }));

    const transformed_partition = external_source.PartitionValue{
        .column_id = @constCast("bucket_id"),
        .string_value = @constCast("bucket-42"),
    };
    try std.testing.expect(partitionMayMatchPredicate(transformed_partition, .{
        .column = "bucket_id",
        .op = .eq_i64,
        .i64_value = 42,
    }));
}

test "parquet object range discovery reads footers and builds row group source" {
    const alloc = std.testing.allocator;

    const column_offset: usize = 100;
    var chunk = std.ArrayListUnmanaged(u8).empty;
    defer chunk.deinit(alloc);
    try appendPlainI64DataPage(&chunk, alloc, &[_]i64{ 10, 20, 30 });

    var object = std.ArrayListUnmanaged(u8).empty;
    defer object.deinit(alloc);
    try object.appendNTimes(alloc, 0, column_offset);
    try object.appendSlice(alloc, chunk.items);
    const metadata_start = object.items.len;
    try appendSingleColumnFooterMetadata(
        &object,
        alloc,
        "amount",
        3,
        column_offset,
        chunk.items.len,
        chunk.items.len,
        0,
        0,
    );
    const metadata_len = object.items.len - metadata_start;
    try appendParquetTrailer(&object, alloc, metadata_len);

    const MemoryRangeReader = struct {
        bucket: []const u8,
        key: []const u8,
        body: []const u8,

        fn reader(self: *@This()) ObjectRangeReader {
            return .{
                .ctx = self,
                .read_range_alloc = readRangeAlloc,
            };
        }

        fn readRangeAlloc(
            ctx: *anyopaque,
            a: Allocator,
            bucket: []const u8,
            key: []const u8,
            offset: u64,
            len: usize,
        ) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (!std.mem.eql(u8, self.bucket, bucket)) return error.ObjectNotFound;
            if (!std.mem.eql(u8, self.key, key)) return error.ObjectNotFound;
            const start: usize = std.math.cast(usize, offset) orelse return error.InvalidLakeRangeRead;
            if (start > self.body.len or len > self.body.len - start) return error.InvalidLakeRangeRead;
            return try a.dupe(u8, self.body[start..][0..len]);
        }
    };
    var range_reader = MemoryRangeReader{
        .bucket = "bucket",
        .key = "events/part-a.parquet",
        .body = object.items,
    };

    var raw = external_source.Inventory{
        .format = .parquet,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/events"),
        .snapshot_id = try alloc.dupe(u8, "sha256:objects"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer raw.deinit(alloc);
    raw.files[0] = .{
        .file_id = try alloc.dupe(u8, "part-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/events/part-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-a"),
        .byte_len = object.items.len,
        .row_count = 0,
        .row_groups = &.{},
    };
    try raw.validate();

    const projection = [_][]const u8{"amount"};
    var discovered = try discoverSupportedI64ObjectRangeRowGroupsFromFootersAlloc(
        alloc,
        range_reader.reader(),
        raw,
        &projection,
        16,
    );
    defer discovered.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 3), discovered.inventory.files[0].row_count);
    try std.testing.expectEqual(@as(usize, 1), discovered.row_group_plan.row_groups.len);
    try std.testing.expectEqualStrings("amount", discovered.row_group_plan.row_groups[0].projected_columns[0]);

    var source = try ObjectRangeRowGroupSource.init(range_reader.reader(), discovered.inventory, discovered.row_group_plan.row_groups);
    defer source.deinit(alloc);

    var result = try lake_rows.scanRowsAlloc(alloc, source.rowSource(), .{
        .projected_columns = &projection,
        .predicate = .{
            .column = "amount",
            .op = .eq_i64,
            .i64_value = 20,
        },
    });
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expectEqual(@as(i64, 20), result.rows[0].find("amount").?.value.?.i64);
    const row_ref = result.rows[0].row_ref.external;
    try std.testing.expectEqualStrings("part-a.parquet", row_ref.file_id);
    try std.testing.expectEqual(@as(u64, 1), row_ref.row_ordinal);
}

test "iceberg object range discovery enriches footer field ids" {
    const alloc = std.testing.allocator;

    const column_offset: usize = 100;
    var chunk = std.ArrayListUnmanaged(u8).empty;
    defer chunk.deinit(alloc);
    try appendPlainI64DataPage(&chunk, alloc, &[_]i64{ 10, 20, 30 });

    var object = std.ArrayListUnmanaged(u8).empty;
    defer object.deinit(alloc);
    try object.appendNTimes(alloc, 0, column_offset);
    try object.appendSlice(alloc, chunk.items);
    const metadata_start = object.items.len;
    try appendSingleColumnFooterMetadataWithFieldId(
        &object,
        alloc,
        "amount",
        3,
        column_offset,
        chunk.items.len,
        chunk.items.len,
        0,
        0,
        7,
    );
    const metadata_len = object.items.len - metadata_start;
    try appendParquetTrailer(&object, alloc, metadata_len);

    const MemoryRangeReader = struct {
        bucket: []const u8,
        key: []const u8,
        body: []const u8,

        fn reader(self: *@This()) ObjectRangeReader {
            return .{
                .ctx = self,
                .read_range_alloc = readRangeAlloc,
            };
        }

        fn readRangeAlloc(
            ctx: *anyopaque,
            a: Allocator,
            bucket: []const u8,
            key: []const u8,
            offset: u64,
            len: usize,
        ) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (!std.mem.eql(u8, self.bucket, bucket)) return error.ObjectNotFound;
            if (!std.mem.eql(u8, self.key, key)) return error.ObjectNotFound;
            const start: usize = std.math.cast(usize, offset) orelse return error.InvalidLakeRangeRead;
            if (start > self.body.len or len > self.body.len - start) return error.InvalidLakeRangeRead;
            return try a.dupe(u8, self.body[start..][0..len]);
        }
    };
    var range_reader = MemoryRangeReader{
        .bucket = "bucket",
        .key = "events/part-a.parquet",
        .body = object.items,
    };

    var raw = external_source.Inventory{
        .format = .iceberg,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/events"),
        .snapshot_id = try alloc.dupe(u8, "iceberg-snapshot:12"),
        .schema_fingerprint = try alloc.dupe(u8, "iceberg-schema:7"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer raw.deinit(alloc);
    raw.files[0] = .{
        .file_id = try alloc.dupe(u8, "part-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/events/part-a.parquet"),
        .version_id = try alloc.dupe(u8, "iceberg:v1:a"),
        .byte_len = object.items.len,
        .row_count = 0,
        .row_groups = &.{},
    };
    try raw.validate();

    const projection = [_][]const u8{"amount"};
    var discovered = try discoverSupportedI64ObjectRangeRowGroupsFromFootersAlloc(
        alloc,
        range_reader.reader(),
        raw,
        &projection,
        16,
    );
    defer discovered.deinit(alloc);

    try std.testing.expectEqual(external_source.Format.iceberg, discovered.inventory.format);
    try std.testing.expectEqual(@as(u64, 3), discovered.inventory.files[0].row_count);
    try std.testing.expectEqual(@as(?i32, 7), discovered.inventory.files[0].row_groups[0].column_chunks[0].field_id);
    try std.testing.expectEqual(@as(usize, 1), discovered.row_group_plan.row_groups.len);

    var source = try ObjectRangeRowGroupSource.init(range_reader.reader(), discovered.inventory, discovered.row_group_plan.row_groups);
    defer source.deinit(alloc);
    try std.testing.expectEqual(rowsource.SourceKind.external_iceberg, source.rowSource().kind);
}

test "parquet test object builder supports multi-column footer discovery" {
    const alloc = std.testing.allocator;

    const amount_values = [_]i64{ 10, 20, 30 };
    const tenant_values = [_]i64{ 7, 8, 7 };
    const columns = [_]TestPlainI64Column{
        .{ .column_id = "amount", .values = &amount_values },
        .{ .column_id = "tenant", .values = &tenant_values },
    };
    const object = try buildTestPlainI64ParquetObjectAlloc(alloc, &columns);
    defer alloc.free(object);

    const MemoryRangeReader = struct {
        body: []const u8,

        fn reader(self: *@This()) ObjectRangeReader {
            return .{
                .ctx = self,
                .read_range_alloc = readRangeAlloc,
            };
        }

        fn readRangeAlloc(
            ctx: *anyopaque,
            a: Allocator,
            bucket: []const u8,
            key: []const u8,
            offset: u64,
            len: usize,
        ) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            try std.testing.expectEqualStrings("bucket", bucket);
            try std.testing.expectEqualStrings("events/part-a.parquet", key);
            const start: usize = std.math.cast(usize, offset) orelse return error.InvalidLakeRangeRead;
            if (start > self.body.len or len > self.body.len - start) return error.InvalidLakeRangeRead;
            return try a.dupe(u8, self.body[start..][0..len]);
        }
    };
    var range_reader = MemoryRangeReader{ .body = object };

    var raw = external_source.Inventory{
        .format = .parquet,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/events"),
        .snapshot_id = try alloc.dupe(u8, "sha256:objects"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer raw.deinit(alloc);
    raw.files[0] = .{
        .file_id = try alloc.dupe(u8, "part-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/events/part-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-a"),
        .byte_len = object.len,
        .row_count = 0,
        .row_groups = &.{},
    };
    try raw.validate();

    const projected = [_][]const u8{"amount"};
    const scan_columns = [_][]const u8{ "amount", "tenant" };
    var discovered = try discoverSupportedI64ObjectRangeRowGroupsFromFootersAlloc(
        alloc,
        range_reader.reader(),
        raw,
        &scan_columns,
        16,
    );
    defer discovered.deinit(alloc);

    var result = try querySupportedI64ObjectRangeRowsAlloc(alloc, .{
        .binding = .{
            .table_id = "events",
            .format = .parquet,
            .source_uri = "s3://bucket/events",
            .snapshot_mode = .{ .object_version_digest = "sha256:objects" },
            .schema_fingerprint = "schema-v1",
        },
        .reader = range_reader.reader(),
        .inventory = discovered.inventory,
        .projected_columns = &projected,
        .predicate = .{
            .column = "tenant",
            .op = .eq_i64,
            .i64_value = 8,
        },
    });
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expectEqual(@as(usize, 1), result.rows[0].cells.len);
    try std.testing.expectEqualStrings("amount", result.rows[0].cells[0].name);
    try std.testing.expectEqual(@as(i64, 20), result.rows[0].cells[0].value.?.i64);
    try std.testing.expectEqual(@as(u64, 1), result.rows[0].row_ref.external.row_ordinal);
}

test "parquet object range discovery scans dictionary byte array predicates" {
    const alloc = std.testing.allocator;

    var amount_chunk = std.ArrayListUnmanaged(u8).empty;
    defer amount_chunk.deinit(alloc);
    try appendPlainI64DataPage(&amount_chunk, alloc, &[_]i64{ 10, 20, 30 });
    var tenant_chunk = std.ArrayListUnmanaged(u8).empty;
    defer tenant_chunk.deinit(alloc);
    try appendPlainByteArrayDictionaryPage(&tenant_chunk, alloc, &[_][]const u8{ "t1", "t2" });
    try appendDictionaryI64DataPage(&tenant_chunk, alloc, 3, 1, &[_]u8{ 3, 0b00000110 });

    var object = std.ArrayListUnmanaged(u8).empty;
    defer object.deinit(alloc);
    try object.appendNTimes(alloc, 0, 100);
    const amount_offset = object.items.len;
    try object.appendSlice(alloc, amount_chunk.items);
    const tenant_offset = object.items.len;
    try object.appendSlice(alloc, tenant_chunk.items);

    const footers = [_]TestColumnFooter{
        .{
            .column_id = "amount",
            .column_offset = amount_offset,
            .compressed_len = amount_chunk.items.len,
            .uncompressed_len = amount_chunk.items.len,
            .physical_type = 2,
        },
        .{
            .column_id = "tenant",
            .column_offset = tenant_offset,
            .compressed_len = tenant_chunk.items.len,
            .uncompressed_len = tenant_chunk.items.len,
            .physical_type = 6,
            .encoding = 7,
        },
    };
    const metadata_start = object.items.len;
    try appendPlainI64FooterMetadata(&object, alloc, 3, &footers);
    const metadata_len = object.items.len - metadata_start;
    try appendParquetTrailer(&object, alloc, metadata_len);

    const MemoryRangeReader = struct {
        body: []const u8,

        fn reader(self: *@This()) ObjectRangeReader {
            return .{
                .ctx = self,
                .read_range_alloc = readRangeAlloc,
            };
        }

        fn readRangeAlloc(
            ctx: *anyopaque,
            a: Allocator,
            bucket: []const u8,
            key: []const u8,
            offset: u64,
            len: usize,
        ) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            try std.testing.expectEqualStrings("bucket", bucket);
            try std.testing.expectEqualStrings("events/part-a.parquet", key);
            const start: usize = std.math.cast(usize, offset) orelse return error.InvalidLakeRangeRead;
            if (start > self.body.len or len > self.body.len - start) return error.InvalidLakeRangeRead;
            return try a.dupe(u8, self.body[start..][0..len]);
        }
    };
    var range_reader = MemoryRangeReader{ .body = object.items };

    var raw = external_source.Inventory{
        .format = .parquet,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/events"),
        .snapshot_id = try alloc.dupe(u8, "sha256:objects"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer raw.deinit(alloc);
    raw.files[0] = .{
        .file_id = try alloc.dupe(u8, "part-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/events/part-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-a"),
        .byte_len = object.items.len,
        .row_count = 0,
        .row_groups = &.{},
    };
    try raw.validate();

    const projected = [_][]const u8{"amount"};
    const scan_columns = [_][]const u8{ "amount", "tenant" };
    var discovered = try discoverSupportedI64ObjectRangeRowGroupsFromFootersAlloc(
        alloc,
        range_reader.reader(),
        raw,
        &scan_columns,
        16,
    );
    defer discovered.deinit(alloc);
    try std.testing.expectEqualStrings("int64", discovered.inventory.files[0].row_groups[0].column_chunks[0].physical_type);
    try std.testing.expectEqualStrings("byte_array", discovered.inventory.files[0].row_groups[0].column_chunks[1].physical_type);

    var result = try querySupportedI64ObjectRangeRowsAlloc(alloc, .{
        .binding = .{
            .table_id = "events",
            .format = .parquet,
            .source_uri = "s3://bucket/events",
            .snapshot_mode = .{ .object_version_digest = "sha256:objects" },
            .schema_fingerprint = "schema-v1",
        },
        .reader = range_reader.reader(),
        .inventory = discovered.inventory,
        .projected_columns = &projected,
        .predicate = .{
            .column = "tenant",
            .op = .eq_bytes,
            .bytes_value = "t2",
        },
    });
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), result.rows.len);
    try std.testing.expectEqual(@as(i64, 20), result.rows[0].cells[0].value.?.i64);
    try std.testing.expectEqual(@as(i64, 30), result.rows[1].cells[0].value.?.i64);
    try std.testing.expectEqual(@as(u64, 1), result.rows[0].row_ref.external.row_ordinal);
    try std.testing.expectEqual(@as(u64, 2), result.rows[1].row_ref.external.row_ordinal);
}

test "parquet object range discovery scans int32 predicates" {
    const alloc = std.testing.allocator;

    var amount_chunk = std.ArrayListUnmanaged(u8).empty;
    defer amount_chunk.deinit(alloc);
    try appendPlainI32DataPage(&amount_chunk, alloc, &[_]i32{ 10, 20, 30 });
    var rank_chunk = std.ArrayListUnmanaged(u8).empty;
    defer rank_chunk.deinit(alloc);
    try appendPlainI32DictionaryPage(&rank_chunk, alloc, &[_]i32{ 1, 2 });
    try appendDictionaryI64DataPage(&rank_chunk, alloc, 3, 1, &[_]u8{ 3, 0b00000110 });

    var object = std.ArrayListUnmanaged(u8).empty;
    defer object.deinit(alloc);
    try object.appendNTimes(alloc, 0, 100);
    const amount_offset = object.items.len;
    try object.appendSlice(alloc, amount_chunk.items);
    const rank_offset = object.items.len;
    try object.appendSlice(alloc, rank_chunk.items);

    const footers = [_]TestColumnFooter{
        .{
            .column_id = "amount",
            .column_offset = amount_offset,
            .compressed_len = amount_chunk.items.len,
            .uncompressed_len = amount_chunk.items.len,
            .physical_type = 1,
        },
        .{
            .column_id = "rank",
            .column_offset = rank_offset,
            .compressed_len = rank_chunk.items.len,
            .uncompressed_len = rank_chunk.items.len,
            .physical_type = 1,
            .encoding = 7,
        },
    };
    const metadata_start = object.items.len;
    try appendPlainI64FooterMetadata(&object, alloc, 3, &footers);
    const metadata_len = object.items.len - metadata_start;
    try appendParquetTrailer(&object, alloc, metadata_len);

    const MemoryRangeReader = struct {
        body: []const u8,

        fn reader(self: *@This()) ObjectRangeReader {
            return .{
                .ctx = self,
                .read_range_alloc = readRangeAlloc,
            };
        }

        fn readRangeAlloc(
            ctx: *anyopaque,
            a: Allocator,
            bucket: []const u8,
            key: []const u8,
            offset: u64,
            len: usize,
        ) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            try std.testing.expectEqualStrings("bucket", bucket);
            try std.testing.expectEqualStrings("events/part-a.parquet", key);
            const start: usize = std.math.cast(usize, offset) orelse return error.InvalidLakeRangeRead;
            if (start > self.body.len or len > self.body.len - start) return error.InvalidLakeRangeRead;
            return try a.dupe(u8, self.body[start..][0..len]);
        }
    };
    var range_reader = MemoryRangeReader{ .body = object.items };

    var raw = external_source.Inventory{
        .format = .parquet,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/events"),
        .snapshot_id = try alloc.dupe(u8, "sha256:objects"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer raw.deinit(alloc);
    raw.files[0] = .{
        .file_id = try alloc.dupe(u8, "part-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/events/part-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-a"),
        .byte_len = object.items.len,
        .row_count = 0,
        .row_groups = &.{},
    };
    try raw.validate();

    const projected = [_][]const u8{"amount"};
    const scan_columns = [_][]const u8{ "amount", "rank" };
    var discovered = try discoverSupportedI64ObjectRangeRowGroupsFromFootersAlloc(
        alloc,
        range_reader.reader(),
        raw,
        &scan_columns,
        16,
    );
    defer discovered.deinit(alloc);
    try std.testing.expectEqualStrings("int32", discovered.inventory.files[0].row_groups[0].column_chunks[0].physical_type);
    try std.testing.expectEqualStrings("int32", discovered.inventory.files[0].row_groups[0].column_chunks[1].physical_type);

    var result = try querySupportedI64ObjectRangeRowsAlloc(alloc, .{
        .binding = .{
            .table_id = "events",
            .format = .parquet,
            .source_uri = "s3://bucket/events",
            .snapshot_mode = .{ .object_version_digest = "sha256:objects" },
            .schema_fingerprint = "schema-v1",
        },
        .reader = range_reader.reader(),
        .inventory = discovered.inventory,
        .projected_columns = &projected,
        .predicate = .{
            .column = "rank",
            .op = .eq_i64,
            .i64_value = 2,
        },
    });
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), result.rows.len);
    try std.testing.expectEqual(@as(i64, 20), result.rows[0].cells[0].value.?.i64);
    try std.testing.expectEqual(@as(i64, 30), result.rows[1].cells[0].value.?.i64);
    try std.testing.expectEqual(@as(u64, 1), result.rows[0].row_ref.external.row_ordinal);
    try std.testing.expectEqual(@as(u64, 2), result.rows[1].row_ref.external.row_ordinal);
}

test "parquet object range cache reuses footer discovery reads" {
    const alloc = std.testing.allocator;

    const column_offset: usize = 100;
    var chunk = std.ArrayListUnmanaged(u8).empty;
    defer chunk.deinit(alloc);
    try appendPlainI64DataPage(&chunk, alloc, &[_]i64{ 10, 20, 30 });

    var object = std.ArrayListUnmanaged(u8).empty;
    defer object.deinit(alloc);
    try object.appendNTimes(alloc, 0, column_offset);
    try object.appendSlice(alloc, chunk.items);
    const metadata_start = object.items.len;
    try appendSingleColumnFooterMetadata(
        &object,
        alloc,
        "amount",
        3,
        column_offset,
        chunk.items.len,
        chunk.items.len,
        0,
        0,
    );
    const metadata_len = object.items.len - metadata_start;
    try appendParquetTrailer(&object, alloc, metadata_len);

    const CountingRangeReader = struct {
        bucket: []const u8,
        key: []const u8,
        body: []const u8,
        read_count: usize = 0,

        fn reader(self: *@This()) ObjectRangeReader {
            return .{
                .ctx = self,
                .read_range_alloc = readRangeAlloc,
            };
        }

        fn readRangeAlloc(
            ctx: *anyopaque,
            a: Allocator,
            bucket: []const u8,
            key: []const u8,
            offset: u64,
            len: usize,
        ) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (!std.mem.eql(u8, self.bucket, bucket)) return error.ObjectNotFound;
            if (!std.mem.eql(u8, self.key, key)) return error.ObjectNotFound;
            const start: usize = std.math.cast(usize, offset) orelse return error.InvalidLakeRangeRead;
            if (start > self.body.len or len > self.body.len - start) return error.InvalidLakeRangeRead;
            self.read_count += 1;
            return try a.dupe(u8, self.body[start..][0..len]);
        }
    };
    var range_reader = CountingRangeReader{
        .bucket = "bucket",
        .key = "events/part-a.parquet",
        .body = object.items,
    };

    var cache = ObjectRangeCache{};
    defer cache.deinit(alloc);

    var raw = external_source.Inventory{
        .format = .parquet,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/events"),
        .snapshot_id = try alloc.dupe(u8, "sha256:objects"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer raw.deinit(alloc);
    raw.files[0] = .{
        .file_id = try alloc.dupe(u8, "part-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/events/part-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-a"),
        .byte_len = object.items.len,
        .row_count = 0,
        .row_groups = &.{},
    };
    try raw.validate();

    const projection = [_][]const u8{"amount"};
    var first = try discoverSupportedI64ObjectRangeRowGroupsFromCachedFootersAlloc(
        alloc,
        range_reader.reader(),
        &cache,
        raw,
        &projection,
        16,
    );
    defer first.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), range_reader.read_count);

    var second = try discoverSupportedI64ObjectRangeRowGroupsFromCachedFootersAlloc(
        alloc,
        range_reader.reader(),
        &cache,
        raw,
        &projection,
        16,
    );
    defer second.deinit(alloc);

    const stats = cache.statsSnapshot();
    try std.testing.expectEqual(@as(usize, 2), range_reader.read_count);
    try std.testing.expectEqual(@as(usize, 2), stats.misses);
    try std.testing.expectEqual(@as(usize, 2), stats.hits);
    try std.testing.expectEqual(@as(u64, 3), second.inventory.files[0].row_count);
    try std.testing.expectEqual(@as(usize, 1), second.row_group_plan.row_groups.len);
}

test "parquet row group batch rejects mismatched decoded row counts" {
    const alloc = std.testing.allocator;
    var inventory = external_source.Inventory{
        .format = .parquet,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/events"),
        .snapshot_id = try alloc.dupe(u8, "sha256:objects"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "part-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/events/part-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-a"),
        .byte_len = 1024,
        .row_count = 2,
        .row_groups = try alloc.dupe(external_source.RowGroup, &[_]external_source.RowGroup{.{
            .ordinal = 0,
            .row_count = 2,
            .column_chunks = try alloc.dupe(external_source.ColumnChunk, &[_]external_source.ColumnChunk{.{
                .column_id = try alloc.dupe(u8, "amount"),
                .file_offset = 100,
                .compressed_len = 24,
            }}),
        }}),
    };

    var chunk = std.ArrayListUnmanaged(u8).empty;
    defer chunk.deinit(alloc);
    try appendPlainI64DataPage(&chunk, alloc, &[_]i64{ 10, 20, 30 });

    try std.testing.expectError(error.ParquetRowGroupRowCountMismatch, buildRequiredPlainI64RowGroupBatchAlloc(
        alloc,
        inventory,
        "part-a.parquet",
        0,
        &[_]ColumnChunkInput{.{ .column_id = "amount", .bytes = chunk.items }},
    ));
}

test "parquet row group materialization rejects oversized batches before decoding" {
    const alloc = std.testing.allocator;
    var inventory = external_source.Inventory{
        .format = .parquet,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/events"),
        .snapshot_id = try alloc.dupe(u8, "sha256:objects"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "part-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/events/part-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-a"),
        .byte_len = 1,
        .row_count = 11,
        .row_groups = try alloc.alloc(external_source.RowGroup, 1),
    };
    inventory.files[0].row_groups[0] = .{
        .ordinal = 0,
        .row_count = 11,
        .file_offset = 0,
        .total_byte_len = 1,
        .column_chunks = try alloc.alloc(external_source.ColumnChunk, 1),
    };
    inventory.files[0].row_groups[0].column_chunks[0] = .{
        .column_id = try alloc.dupe(u8, "amount"),
        .file_offset = 0,
        .compressed_len = 1,
    };

    try std.testing.expectError(error.ParquetRowGroupTooLarge, buildSupportedI64RowGroupBatchAllocWithLimits(
        alloc,
        inventory,
        "part-a.parquet",
        0,
        &.{.{ .column_id = "amount", .bytes = "x" }},
        .{ .max_rows = 10 },
    ));
}

test "parquet row group preflight bounds cumulative decoded page bytes" {
    const alloc = std.testing.allocator;
    var chunk = std.ArrayListUnmanaged(u8).empty;
    defer chunk.deinit(alloc);
    try appendPlainI64DataPage(&chunk, alloc, &.{1});
    try appendPlainI64DataPage(&chunk, alloc, &.{2});

    var inventory = external_source.Inventory{
        .format = .parquet,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/events"),
        .snapshot_id = try alloc.dupe(u8, "sha256:objects"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "part-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/events/part-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-a"),
        .byte_len = chunk.items.len,
        .row_count = 2,
        .row_groups = try alloc.dupe(external_source.RowGroup, &.{.{
            .ordinal = 0,
            .row_count = 2,
            .file_offset = 0,
            .total_byte_len = chunk.items.len,
            .column_chunks = try alloc.dupe(external_source.ColumnChunk, &.{.{
                .column_id = try alloc.dupe(u8, "amount"),
                .file_offset = 0,
                .compressed_len = chunk.items.len,
            }}),
        }}),
    };

    try std.testing.expectError(error.ParquetRowGroupTooLarge, buildSupportedI64RowGroupBatchAllocWithLimits(
        alloc,
        inventory,
        "part-a.parquet",
        0,
        &.{.{ .column_id = "amount", .bytes = chunk.items }},
        .{ .max_decoded_bytes = 8 },
    ));
}

test "parquet row group rejects cumulative object input before reads" {
    const alloc = std.testing.allocator;
    const CountingReader = struct {
        calls: usize = 0,
        fn readRangeAlloc(
            ctx: *anyopaque,
            a: Allocator,
            bucket: []const u8,
            key: []const u8,
            offset: u64,
            len: usize,
        ) ![]u8 {
            _ = bucket;
            _ = key;
            _ = offset;
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.calls += 1;
            return try a.alloc(u8, len);
        }
    };
    var reader_impl = CountingReader{};
    const reader = ObjectRangeReader{ .ctx = &reader_impl, .read_range_alloc = CountingReader.readRangeAlloc };
    var inventory = external_source.Inventory{
        .format = .parquet,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/events"),
        .snapshot_id = try alloc.dupe(u8, "sha256:objects"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "part-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/events/part-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-a"),
        .byte_len = 2,
        .row_count = 1,
        .row_groups = try alloc.dupe(external_source.RowGroup, &.{.{
            .ordinal = 0,
            .row_count = 1,
            .file_offset = 0,
            .total_byte_len = 2,
            .column_chunks = try alloc.dupe(external_source.ColumnChunk, &.{
                .{ .column_id = try alloc.dupe(u8, "a"), .file_offset = 0, .compressed_len = 1 },
                .{ .column_id = try alloc.dupe(u8, "b"), .file_offset = 1, .compressed_len = 1 },
            }),
        }}),
    };

    try std.testing.expectError(error.ParquetRowGroupTooLarge, buildSupportedI64RowGroupBatchFromObjectRangeReaderAllocWithLimits(
        alloc,
        reader,
        inventory,
        "part-a.parquet",
        0,
        &.{ "a", "b" },
        .{ .max_input_bytes = 1 },
    ));
    try std.testing.expectEqual(@as(usize, 0), reader_impl.calls);
}
