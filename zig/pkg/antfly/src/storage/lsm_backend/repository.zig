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
const Allocator = std.mem.Allocator;
const bloom = @import("bloom");
const byte_copy = @import("../../common/byte_copy.zig");
const lsm_manifest = @import("../lsm/manifest.zig");
const lsm_table_file = @import("../lsm/table_file.zig");
const resource_manager_mod = @import("../resource_manager.zig");
const state_mod = @import("state.zig");
const storage_io = @import("storage_io.zig");
pub const manifest_set = @import("manifest_set.zig");
pub const ObsoleteLedger = @import("obsolete_ledger.zig").Ledger;

const max_run_file_read_bytes = 512 * 1024 * 1024;
const max_manifest_read_bytes = 128 * 1024 * 1024;
const table_write_buffer_size = 256 * 1024;
const table_builder_accounting_step_bytes: u64 = 64 * 1024;

pub fn maxRunFileReadBytes() usize {
    return max_run_file_read_bytes;
}

pub fn maxManifestReadBytes() usize {
    return max_manifest_read_bytes;
}

pub const ObsoletePath = struct {
    path: []u8,
    delete_after_ns: u64,
    owns_path: bool = true,

    pub fn deinit(self: *ObsoletePath, allocator: Allocator) void {
        if (self.owns_path) allocator.free(self.path);
        self.* = undefined;
    }
};

/// Shared physical metadata ownership is independent of a run's level/GC
/// revision. Writer candidate roots and retirement queues retain this header,
/// so moving a run cannot leave a pinned writer epoch borrowing freed bytes.
pub const RunOwner = struct {
    refs: std.atomic.Value(usize) = .init(1),
    raw: *Run,
    destroy: *const fn (*RunOwner, Allocator) void,
    pub fn retain(self: *RunOwner) *RunOwner {
        _ = self.refs.fetchAdd(1, .monotonic);
        return self;
    }
    pub fn release(self: *RunOwner, allocator: Allocator) void {
        if (self.refs.fetchSub(1, .acq_rel) == 1) self.destroy(self, allocator);
    }
};

pub const Run = struct {
    owner: ?*RunOwner = null,
    output_ticket: ?*@import("output_cleanup.zig").Ticket = null,
    id: u64,
    /// Logical newest-write precedence for L0. Physical rewrites allocate a
    /// fresh id but retain the newest input sequence so tiered merges cannot
    /// make older values shadow newer unmerged runs. Zero means `id` for
    /// in-memory/legacy callers.
    level: u32,
    size_bytes: u64,
    compression_stats: lsm_table_file.CompressionStats = .{},
    path: ?[]u8,
    smallest_namespace_name: ?[]u8,
    smallest_key: []u8,
    largest_namespace_name: ?[]u8,
    largest_key: []u8,
    entry_count: u32,
    tombstone_count: ?u32 = null,
    oldest_tombstone_unix_ns: u64 = 0,
    visibility_id: u64 = 0,
    gc_requested: bool = false,
    bloom_filter: ?bloom.OwnedFilter,
    owns_metadata: bool = true,
    owns_path: bool = false,
    owns_bloom_filter: bool = true,
    cached_state_index: ?usize = null,
    cached_index_index: ?usize = null,
    cached_table_index: ?usize = null,
    table_index: ?lsm_table_file.TableIndex = null,
    version_ref_pinned: bool = false,
    shared_read_version: bool = false,
    state: ?state_mod.State,

    pub fn deinit(self: *Run, allocator: Allocator) void {
        if (self.owner) |owner| {
            self.* = .{ .id = self.id, .level = self.level, .size_bytes = 0, .path = null, .smallest_namespace_name = null, .smallest_key = &.{}, .largest_namespace_name = null, .largest_key = &.{}, .entry_count = 0, .bloom_filter = null, .state = null, .owns_metadata = false, .owns_bloom_filter = false };
            owner.release(allocator);
            return;
        }
        if (self.output_ticket) |ticket| ticket.abandon();
        self.output_ticket = null;
        if (self.owns_path) {
            if (self.path) |path| allocator.free(path);
        }
        if (self.owns_metadata) {
            if (!self.owns_path) {
                if (self.path) |path| allocator.free(path);
            }
            if (self.smallest_namespace_name) |name| allocator.free(name);
            allocator.free(self.smallest_key);
            if (self.largest_namespace_name) |name| allocator.free(name);
            allocator.free(self.largest_key);
        }
        if (self.owns_bloom_filter) {
            if (self.bloom_filter) |*filter| filter.deinit(allocator);
        }
        if (self.table_index) |*index| index.deinit(allocator);
        if (self.state) |*state| state.deinit(allocator);
        self.* = .{
            .id = self.id,
            .level = self.level,
            .size_bytes = 0,
            .compression_stats = .{},
            .path = null,
            .smallest_namespace_name = null,
            .smallest_key = &.{},
            .largest_namespace_name = null,
            .largest_key = &.{},
            .entry_count = 0,
            .bloom_filter = null,
            .owns_metadata = false,
            .owns_path = false,
            .owns_bloom_filter = false,
            .cached_state_index = null,
            .cached_index_index = null,
            .cached_table_index = null,
            .table_index = null,
            .version_ref_pinned = false,
            .state = null,
        };
    }

    pub fn retainOwned(self: Run) Run {
        std.debug.assert(self.owner != null);
        _ = self.owner.?.retain();
        return self;
    }

    pub fn versionOwner(self: *Run) *Run {
        return if (self.owner) |owner| owner.raw else self;
    }

    /// Only after publication is irrevocable; caller retains an owner until
    /// this completes, so a concurrent retirement cannot abandon this ticket.
    pub fn commitOutput(self: *Run) void {
        const raw = self.versionOwner();
        if (raw.output_ticket) |ticket| ticket.destroy();
        raw.output_ticket = null;
    }

    pub fn abandonOutput(self: *Run) bool {
        const raw = self.versionOwner();
        const ticket = raw.output_ticket orelse return false;
        raw.output_ticket = null;
        ticket.abandon();
        return true;
    }

    pub fn ensureState(self: *Run, allocator: Allocator) !*state_mod.State {
        if (self.state == null) {
            const path = self.path orelse return error.RunStateUnavailable;
            self.state = try loadRunStateAlloc(allocator, path);
        }
        return &self.state.?;
    }

    pub fn ensureStateWithStorage(self: *Run, allocator: Allocator, storage: storage_io.Storage) !*state_mod.State {
        if (self.state == null) {
            const path = self.path orelse return error.RunStateUnavailable;
            self.state = try loadRunStateAllocWithStorage(storage, allocator, path);
        }
        return &self.state.?;
    }

    pub fn ensureBloomFilter(self: *Run, allocator: Allocator) !bloom.OwnedFilter {
        if (self.bloom_filter) |filter| return filter;
        var native = try storage_io.NativeStorage.init(std.heap.page_allocator, .threaded);
        defer native.deinit();
        return try self.ensureBloomFilterWithStorage(allocator, native.storage());
    }

    pub fn ensureBloomFilterWithOptionalStorage(self: *Run, allocator: Allocator, storage: ?storage_io.Storage) !bloom.OwnedFilter {
        if (storage) |concrete| return try self.ensureBloomFilterWithStorage(allocator, concrete);
        return try self.ensureBloomFilter(allocator);
    }

    pub fn ensureBloomFilterWithStorage(self: *Run, allocator: Allocator, storage: storage_io.Storage) !bloom.OwnedFilter {
        if (self.bloom_filter) |filter| return filter;
        if (self.table_index) |index| return index.filter;
        const path = self.path orelse return error.RunBloomFilterUnavailable;
        self.table_index = try loadRunTableIndexAllocWithStorage(storage, allocator, path);
        return self.table_index.?.filter;
    }
};

pub fn cloneRunSnapshot(allocator: Allocator, source: Run) !Run {
    var metadata_owned = true;
    const smallest_namespace_name = if (source.smallest_namespace_name) |name| try allocator.dupe(u8, name) else null;
    errdefer if (metadata_owned) if (smallest_namespace_name) |name| allocator.free(name);
    const smallest_key = try allocator.dupe(u8, source.smallest_key);
    errdefer if (metadata_owned) allocator.free(smallest_key);
    const largest_namespace_name = if (source.largest_namespace_name) |name| try allocator.dupe(u8, name) else null;
    errdefer if (metadata_owned) if (largest_namespace_name) |name| allocator.free(name);
    const largest_key = try allocator.dupe(u8, source.largest_key);
    errdefer if (metadata_owned) allocator.free(largest_key);
    const path = if (source.path) |path| try allocator.dupe(u8, path) else null;
    errdefer if (metadata_owned) if (path) |owned| allocator.free(owned);

    var out = Run{
        .id = source.id,
        .level = source.level,
        .size_bytes = source.size_bytes,
        .compression_stats = source.compression_stats,
        .path = path,
        .smallest_namespace_name = smallest_namespace_name,
        .smallest_key = smallest_key,
        .largest_namespace_name = largest_namespace_name,
        .largest_key = largest_key,
        .entry_count = source.entry_count,
        .tombstone_count = source.tombstone_count,
        .oldest_tombstone_unix_ns = source.oldest_tombstone_unix_ns,
        .visibility_id = source.visibility_id,
        .gc_requested = source.gc_requested,
        .bloom_filter = if (source.bloom_filter) |filter| try filter.clone(allocator) else null,
        .cached_state_index = null,
        .cached_index_index = null,
        .cached_table_index = null,
        .table_index = null,
        .state = null,
    };
    metadata_owned = false;
    errdefer out.deinit(allocator);

    if (source.path == null) {
        const state = source.state orelse return error.RunStateUnavailable;
        out.state = try state.clone(allocator);
    }
    return out;
}

pub fn cloneRunCompactionSnapshot(allocator: Allocator, source: Run) !Run {
    var metadata_owned = true;
    const smallest_namespace_name = if (source.smallest_namespace_name) |name| try allocator.dupe(u8, name) else null;
    errdefer if (metadata_owned) if (smallest_namespace_name) |name| allocator.free(name);
    const smallest_key = try allocator.dupe(u8, source.smallest_key);
    errdefer if (metadata_owned) allocator.free(smallest_key);
    const largest_namespace_name = if (source.largest_namespace_name) |name| try allocator.dupe(u8, name) else null;
    errdefer if (metadata_owned) if (largest_namespace_name) |name| allocator.free(name);
    const largest_key = try allocator.dupe(u8, source.largest_key);
    errdefer if (metadata_owned) allocator.free(largest_key);

    var out = Run{
        .id = source.id,
        .level = source.level,
        .size_bytes = source.size_bytes,
        .compression_stats = source.compression_stats,
        .path = if (source.path) |path| try allocator.dupe(u8, path) else null,
        .smallest_namespace_name = smallest_namespace_name,
        .smallest_key = smallest_key,
        .largest_namespace_name = largest_namespace_name,
        .largest_key = largest_key,
        .entry_count = source.entry_count,
        .tombstone_count = source.tombstone_count,
        .oldest_tombstone_unix_ns = source.oldest_tombstone_unix_ns,
        .visibility_id = source.visibility_id,
        .gc_requested = source.gc_requested,
        .bloom_filter = null,
        .owns_bloom_filter = false,
        .cached_state_index = null,
        .cached_index_index = null,
        .cached_table_index = null,
        .table_index = null,
        .state = null,
    };
    metadata_owned = false;
    errdefer out.deinit(allocator);

    if (source.path == null) {
        const state = source.state orelse return error.RunStateUnavailable;
        out.state = try state.clone(allocator);
    }
    return out;
}

pub fn ensureOpenDirs(root_dir: []const u8) !void {
    var native = try storage_io.NativeStorage.init(std.heap.page_allocator, .threaded);
    defer native.deinit();
    try ensureOpenDirsWithStorage(native.storage(), root_dir);
}

test "lsm snapshot clone has one metadata owner on state failure" {
    const allocator = std.testing.allocator;
    const source = Run{ .id = 1, .level = 0, .size_bytes = 1, .path = null, .smallest_namespace_name = @constCast("docs"), .smallest_key = @constCast("a"), .largest_namespace_name = @constCast("docs"), .largest_key = @constCast("z"), .entry_count = 1, .state = null, .bloom_filter = null, .owns_metadata = false };
    try std.testing.expectError(error.RunStateUnavailable, cloneRunSnapshot(allocator, source));
    try std.testing.expectError(error.RunStateUnavailable, cloneRunCompactionSnapshot(allocator, source));
    const Fixture = struct {
        fn check(alloc: Allocator, input: Run, compaction: bool) !void {
            var snapshot = if (compaction) try cloneRunCompactionSnapshot(alloc, input) else try cloneRunSnapshot(alloc, input);
            snapshot.deinit(alloc);
        }
    };
    var valid = source;
    valid.state = .{};
    for ([_]bool{ false, true }) |compaction| try std.testing.checkAllAllocationFailures(allocator, Fixture.check, .{ valid, compaction });
}

pub fn ensureOpenDirsWithStorage(storage: storage_io.Storage, root_dir: []const u8) !void {
    try storage.createDirPath(root_dir);
    const runs_dir = try std.fs.path.join(std.heap.page_allocator, &.{ root_dir, "runs" });
    defer std.heap.page_allocator.free(runs_dir);
    try storage.createDirPath(runs_dir);
}

pub fn loadManifestIfPresent(
    storage: storage_io.Storage,
    allocator: Allocator,
    root_dir: ?[]const u8,
    manifest_backing: *?[]u8,
    next_run_id: *u64,
    runs: *std.ArrayListUnmanaged(Run),
    obsolete_paths: anytype,
) !bool {
    const concrete_root = root_dir orelse return false;
    return try loadManifestIfPresentWithStorage(storage, allocator, concrete_root, manifest_backing, next_run_id, runs, obsolete_paths);
}

pub fn loadManifestIfPresentWithStorage(
    storage: storage_io.Storage,
    allocator: Allocator,
    root_dir: []const u8,
    manifest_backing: *?[]u8,
    next_run_id: *u64,
    runs: *std.ArrayListUnmanaged(Run),
    obsolete_paths: anytype,
) !bool {
    return loadManifestWithRecoveryState(storage, allocator, root_dir, manifest_backing, next_run_id, runs, obsolete_paths, null);
}

pub fn loadManifestWithRecoveryState(
    storage: storage_io.Storage,
    allocator: Allocator,
    root_dir: []const u8,
    manifest_backing: *?[]u8,
    next_run_id: *u64,
    runs: *std.ArrayListUnmanaged(Run),
    obsolete_paths: anytype,
    recovery: ?*?@import("manifest_replay.zig").RecoveryState,
) !bool {
    const manifest_path = try joinPath(allocator, root_dir, "manifest.bin");
    defer allocator.free(manifest_path);

    if (try readStreamingManifestIfPresent(storage, allocator, root_dir, manifest_path)) |loaded| {
        var catalog = loaded;
        defer catalog.deinit(allocator);
        next_run_id.* = catalog.next_run_id;
        try runs.ensureUnusedCapacity(allocator, catalog.runs.count());
        var entries = catalog.runs.valueIterator();
        while (entries.next()) |meta| {
            const path = try runPath(allocator, root_dir, meta.id);
            runs.appendAssumeCapacity(.{
                .id = meta.id,
                .level = meta.level,
                .size_bytes = meta.size_bytes,
                .compression_stats = meta.compression_stats,
                .path = path,
                .smallest_namespace_name = meta.smallest_namespace_name,
                .smallest_key = meta.smallest_key,
                .largest_namespace_name = meta.largest_namespace_name,
                .largest_key = meta.largest_key,
                .entry_count = meta.entry_count,
                .tombstone_count = meta.tombstone_count,
                .oldest_tombstone_unix_ns = meta.oldest_tombstone_unix_ns,
                .visibility_id = meta.visibility_id,
                .gc_requested = meta.gc_requested,
                .bloom_filter = null,
                .owns_metadata = true,
                .owns_path = true,
                .state = null,
            });
            // Transfer live metadata, not the checkpoint/journal byte buffers.
            meta.smallest_namespace_name = null;
            meta.smallest_key = &.{};
            meta.largest_namespace_name = null;
            meta.largest_key = &.{};
        }
        var paths = catalog.paths.valueIterator();
        while (paths.next()) |obsolete| {
            const path = try rebaseManifestPathAlloc(allocator, root_dir, obsolete.path);
            errdefer allocator.free(path);
            try obsolete_paths.append(allocator, .{ .path = path, .delete_after_ns = obsolete.delete_after_ns });
        }
        if (recovery) |out| out.* = catalog.recovery;
        return true;
    }

    var manifest_file_high_water: u64 = 0;
    const raw_manifest = readManifestWithHighWater(storage, allocator, root_dir, &manifest_file_high_water) catch |err| switch (err) {
        error.FileNotFound, error.NotDir => return false,
        else => {
            logReadFileFailure(storage, manifest_path, max_manifest_read_bytes, "loadManifestIfPresentWithStorage", err);
            return err;
        },
    };
    var raw_manifest_owned = true;
    errdefer if (raw_manifest_owned) allocator.free(raw_manifest);
    var decoded = try lsm_manifest.decodeBorrowedOwnedAlloc(allocator, raw_manifest);
    raw_manifest_owned = false;
    defer decoded.deinit(allocator);

    next_run_id.* = @max(decoded.next_run_id, manifest_file_high_water);
    try runs.ensureTotalCapacity(allocator, decoded.runs.len);
    for (decoded.runs) |*meta| {
        const owned_path = try runPath(allocator, root_dir, meta.id);
        var path_owned = true;
        errdefer if (path_owned) allocator.free(owned_path);
        try runs.append(allocator, .{
            .id = meta.id,
            .level = meta.level,
            .size_bytes = meta.size_bytes,
            .compression_stats = meta.compression_stats,
            .path = owned_path,
            .smallest_namespace_name = if (meta.smallest_namespace_name) |name| @constCast(name) else null,
            .smallest_key = @constCast(meta.smallest_key),
            .largest_namespace_name = if (meta.largest_namespace_name) |name| @constCast(name) else null,
            .largest_key = @constCast(meta.largest_key),
            .entry_count = meta.entry_count,
            .tombstone_count = meta.tombstone_count,
            .oldest_tombstone_unix_ns = meta.oldest_tombstone_unix_ns,
            .visibility_id = meta.visibility_id,
            .gc_requested = meta.gc_requested,
            .bloom_filter = null,
            .owns_metadata = false,
            .owns_path = true,
            .state = null,
        });
        path_owned = false;
    }
    for (decoded.obsolete_paths) |obsolete| {
        const owned_path = try rebaseManifestPathAlloc(allocator, root_dir, obsolete.path);
        errdefer allocator.free(owned_path);
        try obsolete_paths.append(allocator, .{
            .path = owned_path,
            .delete_after_ns = obsolete.delete_after_ns,
        });
    }
    manifest_backing.* = decoded.raw;
    decoded.raw = &.{};
    return true;
}

pub fn readManifestAllocWithStorage(storage: storage_io.Storage, allocator: Allocator, root_dir: []const u8) ![]u8 {
    var high_water: u64 = 0;
    return readManifestWithHighWater(storage, allocator, root_dir, &high_water);
}

fn readStreamingManifestIfPresent(storage: storage_io.Storage, allocator: Allocator, root: []const u8, path: []const u8) !?@import("manifest_replay.zig").Catalog {
    for (0..8) |_| {
        const raw = storage.readFileAlloc(allocator, path, max_manifest_read_bytes) catch |err| switch (err) {
            error.FileNotFound, error.NotDir => return null,
            else => return err,
        };
        defer allocator.free(raw);
        if (!std.mem.startsWith(u8, raw, manifest_set.magic)) return null;
        const descriptor = try manifest_set.Descriptor.decode(raw);
        var result = @import("manifest_replay.zig").load(allocator, storage, root, descriptor, max_manifest_read_bytes);
        var transferred = false;
        defer if (!transferred) {
            if (result) |*catalog| catalog.deinit(allocator) else |_| {}
        };
        const latest = storage.readFileAlloc(allocator, path, max_manifest_read_bytes) catch |err|
            return if (err == error.FileNotFound or err == error.NotDir) error.InvalidManifest else err;
        defer allocator.free(latest);
        if (!std.mem.eql(u8, raw, latest)) continue;
        const catalog = if (result) |*catalog| catalog else |err| return if (err == error.FileNotFound or err == error.NotDir) error.InvalidManifest else err;
        const high_water = std.math.add(u64, @max(descriptor.checkpoint, catalog.active_segment), 1) catch return error.InvalidManifest;
        catalog.next_run_id = @max(catalog.next_run_id, high_water);
        transferred = true;
        return catalog.*;
    }
    return error.ManifestChangedDuringOpen;
}

fn readManifestWithHighWater(storage: storage_io.Storage, allocator: Allocator, root_dir: []const u8, high_water: *u64) ![]u8 {
    const path = try manifestPath(allocator, root_dir);
    defer allocator.free(path);
    for (0..8) |_| {
        const raw = try storage.readFileAlloc(allocator, path, max_manifest_read_bytes);
        var owned = true;
        defer if (owned) allocator.free(raw);
        if (!std.mem.startsWith(u8, raw, manifest_set.magic)) {
            owned = false;
            return raw;
        }
        const descriptor = try manifest_set.Descriptor.decode(raw);
        const result = manifest_set.load(allocator, storage, root_dir, descriptor, max_manifest_read_bytes);
        // A concurrent checkpoint may retire the descriptor's old files while
        // a read-only opener is loading them. Retry a changed descriptor; a
        // missing child of an unchanged descriptor is corruption, not an empty
        // database. In particular, never propagate FileNotFound to the caller
        // that interprets it as permission to initialize a new store.
        const latest = storage.readFileAlloc(allocator, path, max_manifest_read_bytes) catch |err| {
            if (result) |loaded| allocator.free(loaded.bytes) else |_| {}
            return if (err == error.FileNotFound) error.InvalidManifest else err;
        };
        defer allocator.free(latest);
        if (!std.mem.eql(u8, raw, latest)) {
            if (result) |loaded| allocator.free(loaded.bytes) else |_| {}
            continue;
        }
        const loaded = result catch |err| return if (err == error.FileNotFound or err == error.NotDir) error.InvalidManifest else err;
        errdefer allocator.free(loaded.bytes);
        // Rotation reserves a file ID before the next edit records next_run_id.
        high_water.* = try std.math.add(u64, @max(descriptor.checkpoint, loaded.active_segment), 1);
        return loaded.bytes;
    }
    return error.ManifestChangedDuringOpen;
}

fn rebaseManifestPathAlloc(allocator: Allocator, root_dir: []const u8, path: []const u8) ![]u8 {
    if (!std.fs.path.isAbsolute(path)) return try std.fs.path.join(allocator, &.{ root_dir, path });
    const parent = std.fs.path.dirname(path) orelse return try allocator.dupe(u8, path);
    if (std.mem.startsWith(u8, std.fs.path.basename(path), "manifest-")) return try std.fs.path.join(allocator, &.{ root_dir, std.fs.path.basename(path) });
    if (!std.mem.eql(u8, std.fs.path.basename(parent), "runs")) return try allocator.dupe(u8, path);
    return try std.fs.path.join(allocator, &.{ root_dir, "runs", std.fs.path.basename(path) });
}

pub fn persistRunFile(allocator: Allocator, root_dir: []const u8, run: *Run) ![]u8 {
    var native = try storage_io.NativeStorage.init(std.heap.page_allocator, .threaded);
    defer native.deinit();
    return try persistRunFileWithStorage(native.storage(), allocator, root_dir, run, .snappy_adaptive, lsm_table_file.default_prefix_extractor);
}

pub fn persistRunFileWithStorage(
    storage: storage_io.Storage,
    allocator: Allocator,
    root_dir: []const u8,
    run: *Run,
    compression_policy: lsm_table_file.CompressionPolicy,
    prefix_extractor: lsm_table_file.PrefixExtractor,
) ![]u8 {
    return try persistRunFileWithStorageAccounted(storage, allocator, root_dir, run, compression_policy, prefix_extractor, null);
}

pub fn persistRunFileWithStorageAccounted(
    storage: storage_io.Storage,
    allocator: Allocator,
    root_dir: []const u8,
    run: *Run,
    compression_policy: lsm_table_file.CompressionPolicy,
    prefix_extractor: lsm_table_file.PrefixExtractor,
    resource_manager: ?*resource_manager_mod.ResourceManager,
) ![]u8 {
    return try persistRunFileWithStorageAccountedOptions(
        storage,
        allocator,
        root_dir,
        run,
        lsm_table_file.default_filter_config,
        compression_policy,
        prefix_extractor,
        resource_manager,
        // Flush and direct-ingest runs are immutable one-pass outputs just
        // like compaction runs. Keeping every new L0 run resident makes the
        // later L0->L1 compaction temporarily own both the full input corpus
        // and its output in RSS. Foreground point/range reads reopen through
        // the normal descriptor cache when they actually need a page.
        .cold_sequential,
        max_run_file_read_bytes,
    );
}

pub fn persistRunFileWithStorageAccountedOptions(
    storage: storage_io.Storage,
    allocator: Allocator,
    root_dir: []const u8,
    run: *Run,
    bloom_config: bloom.Config,
    compression_policy: lsm_table_file.CompressionPolicy,
    prefix_extractor: lsm_table_file.PrefixExtractor,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    cache_intent: storage_io.AtomicWriteCacheIntent,
    max_file_bytes: usize,
) ![]u8 {
    const state = run.state orelse return error.RunStateUnavailable;
    var writer: StreamingRunFileWriter = undefined;
    try writer.initInPlace(
        storage,
        allocator,
        root_dir,
        run.id,
        state.entryCount(),
        @min(max_file_bytes, max_run_file_read_bytes),
        bloom_config,
        compression_policy,
        prefix_extractor,
        resource_manager,
        cache_intent,
    );
    var writer_active = true;
    errdefer if (writer_active) writer.deinit();

    var cursor: state_mod.State.EntryCursor = .{};
    for (0..state.entryCount()) |i| {
        const entry = cursor.at(&state, i);
        try writer.appendEntry(.{
            .namespace_name = entry.namespace_name,
            .key = entry.key,
            .value = entry.value,
            .tombstone = entry.tombstone,
        });
    }

    var persisted = try writer.finish();
    writer_active = false;
    errdefer {
        allocator.free(persisted.path);
        persisted.filter.deinit(allocator);
    }

    if (run.owns_bloom_filter) {
        if (run.bloom_filter) |*filter| filter.deinit(allocator);
    }
    run.size_bytes = persisted.size_bytes;
    run.compression_stats = persisted.compression_stats;
    run.bloom_filter = persisted.filter;
    run.owns_bloom_filter = true;
    return persisted.path;
}

pub fn persistTableEntriesAsRunFile(
    storage: storage_io.Storage,
    allocator: Allocator,
    root_dir: []const u8,
    run_id: u64,
    entries: []const lsm_table_file.Entry,
    filter: bloom.OwnedFilter,
    compression_policy: lsm_table_file.CompressionPolicy,
) !PersistedRunFile {
    try ensureOpenDirsWithStorage(storage, root_dir);
    const run_path = try runPath(allocator, root_dir, run_id);
    errdefer allocator.free(run_path);

    const written = try writeTableFileAtomically(storage, allocator, run_path, entries, filter, compression_policy);
    return .{
        .path = run_path,
        .size_bytes = written.size_bytes,
        .compression_stats = written.compression_stats,
    };
}

pub fn buildFilterForState(allocator: Allocator, state: *const state_mod.State) !bloom.OwnedFilter {
    return buildFilterForStateWithConfig(allocator, state, lsm_table_file.default_filter_config);
}

pub fn buildFilterForStateWithConfig(
    allocator: Allocator,
    state: *const state_mod.State,
    config: bloom.Config,
) !bloom.OwnedFilter {
    var table_entries = try allocator.alloc(lsm_table_file.Entry, state.entryCount());
    defer allocator.free(table_entries);
    var cursor: state_mod.State.EntryCursor = .{};
    for (0..state.entryCount()) |i| {
        const entry = cursor.at(state, i);
        table_entries[i] = .{
            .namespace_name = entry.namespace_name,
            .key = entry.key,
            .value = entry.value,
            .tombstone = entry.tombstone,
        };
    }
    return try lsm_table_file.buildFilterAlloc(allocator, table_entries, config);
}

pub fn persistManifest(
    allocator: Allocator,
    root_dir: []const u8,
    next_run_id: u64,
    runs: []const Run,
    obsolete_paths: []const ObsoletePath,
) !void {
    var native = try storage_io.NativeStorage.init(std.heap.page_allocator, .threaded);
    defer native.deinit();
    try persistManifestWithStorage(native.storage(), allocator, root_dir, next_run_id, runs, obsolete_paths);
}

pub fn persistManifestWithStorage(
    storage: storage_io.Storage,
    allocator: Allocator,
    root_dir: []const u8,
    next_run_id: u64,
    runs: []const Run,
    obsolete_paths: []const ObsoletePath,
) !void {
    _ = try persistManifestWithStorageCount(storage, allocator, root_dir, next_run_id, runs, obsolete_paths);
}

pub fn persistManifestWithStorageCount(
    storage: storage_io.Storage,
    allocator: Allocator,
    root_dir: []const u8,
    next_run_id: u64,
    runs: []const Run,
    obsolete_paths: []const ObsoletePath,
) !u64 {
    try ensureOpenDirsWithStorage(storage, root_dir);
    const manifest_path = try joinPath(allocator, root_dir, "manifest.bin");
    defer allocator.free(manifest_path);

    const encoded = try encodeManifestAlloc(allocator, next_run_id, runs, obsolete_paths);
    defer allocator.free(encoded);
    try replaceFileAtomicallyAbsolute(storage, manifest_path, encoded);
    return @intCast(encoded.len);
}

pub fn runMeta(run: Run) lsm_manifest.RunMeta {
    return .{
        .id = run.id,
        .level = run.level,
        .size_bytes = run.size_bytes,
        .compression_stats = run.compression_stats,
        .path = run.path.?,
        .smallest_namespace_name = run.smallest_namespace_name,
        .smallest_key = run.smallest_key,
        .largest_namespace_name = run.largest_namespace_name,
        .largest_key = run.largest_key,
        .entry_count = run.entry_count,
        .tombstone_count = run.tombstone_count,
        .oldest_tombstone_unix_ns = run.oldest_tombstone_unix_ns,
        .visibility_id = run.visibility_id,
        .gc_requested = run.gc_requested,
    };
}

// Journal identities must survive staged restore's directory rename. Runtime
// paths remain absolute for I/O; wire paths are relative to the owning root.
fn manifestRelativePath(root: []const u8, path: []const u8) []const u8 {
    const trimmed = std.mem.trimEnd(u8, root, "/\\");
    if (path.len > trimmed.len and std.mem.startsWith(u8, path, trimmed) and (path[trimmed.len] == '/' or path[trimmed.len] == '\\')) return std.mem.trimStart(u8, path[trimmed.len..], "/\\");
    return path;
}

test "manifest path identities strip only the owning directory boundary" {
    try std.testing.expectEqualStrings("runs/1.tbl", manifestRelativePath("/stage/", "/stage/runs/1.tbl"));
    try std.testing.expectEqualStrings("runs/1.tbl", manifestRelativePath("/stage/", "/stage//runs/1.tbl"));
    try std.testing.expectEqualStrings("/staged/runs/1.tbl", manifestRelativePath("/stage", "/staged/runs/1.tbl"));
    try std.testing.expectEqualStrings("runs\\1.tbl", manifestRelativePath("C:\\stage\\", "C:\\stage\\runs\\1.tbl"));
}

pub fn encodeManifestAlloc(allocator: Allocator, next_run_id: u64, runs: []const Run, obsolete_paths: []const ObsoletePath) ![]u8 {
    var metas = try allocator.alloc(lsm_manifest.RunMeta, runs.len);
    defer allocator.free(metas);
    for (runs, 0..) |run, i| {
        metas[i] = runMeta(run);
    }

    var obsolete_metas = try allocator.alloc(lsm_manifest.ObsoletePathMeta, obsolete_paths.len);
    defer allocator.free(obsolete_metas);
    for (obsolete_paths, 0..) |obsolete, i| {
        obsolete_metas[i] = .{
            .path = obsolete.path,
            .delete_after_ns = obsolete.delete_after_ns,
        };
    }

    return try lsm_manifest.encodeAlloc(allocator, .{
        .next_run_id = next_run_id,
        .runs = metas,
        .obsolete_paths = obsolete_metas,
    });
}

/// Deterministic interleaving point; never consulted in production builds.
pub var checkpoint_test_hook: ?struct { context: *anyopaque, run: *const fn (*anyopaque) anyerror!void } = null;
pub var publication_test_hook: ?struct { context: *anyopaque, run: *const fn (*anyopaque) anyerror!void } = null;
/// Benchmark control: identical durable format and planner, serialized fsync.
pub var publication_test_keep_backend_locked = false;

const LedgerSnapshot = @import("ledger_reclamation.zig").Snapshot;

pub const ManifestJournal = struct {
    sequence: ?u64 = null,
    bytes: u64 = 0,
    edit_bytes: u64 = 0,
    next_run_id: u64 = 0,
    descriptor: ?manifest_set.Descriptor = null,
    active_segment: u64 = 0,
    checkpoint_sequence: u64 = 0,
    segments: [manifest_set.max_segments]u64 = @splat(0),
    segment_count: usize = 0,
    obsolete: ObsoleteLedger = .empty,
    const Measure = struct {
        wire_bytes: u64 = 128,
        rows: u64 = 0,
        pub fn put(measure: *@This(), run: Run) !void {
            measure.rows += 1;
            measure.wire_bytes += 112 + run.path.?.len + run.smallest_key.len + run.largest_key.len;
            if (run.smallest_namespace_name) |name| measure.wire_bytes += name.len;
            if (run.largest_namespace_name) |name| measure.wire_bytes += name.len;
        }
        pub fn remove(measure: *@This(), _: Run) !void {
            measure.wire_bytes += 8;
        }
    };
    pub const Result = struct { written: u64, total: u64, checkpoint: bool };

    pub fn deinit(self: *@This(), allocator: Allocator) void {
        self.obsolete.deinit(allocator);
        self.* = .{};
    }

    pub fn memoryBytes(self: *const @This()) u64 {
        return self.obsolete.memoryBytes(state_mod.memory_account.nextPass());
    }

    pub fn recoverState(self: *@This(), backend: anytype) !void {
        if (comptime @hasField(@TypeOf(backend.*), "recovered_manifest")) if (backend.recovered_manifest) |recovered| {
            if (self.descriptor != null or backend.options.backend.read_only) return;
            const allocator = backend.allocator;
            const storage = backend.storage.?;
            const id = recovered.segment_ids[recovered.segment_count - 1];
            if (recovered.torn_tail_bytes != 0) {
                const path = try manifest_set.pathAlloc(allocator, backend.root_dir.?, id, .journal);
                defer allocator.free(path);
                var sink = try storage.beginAtomicWrite(allocator, path);
                var owned = true;
                errdefer if (owned) sink.abort();
                const buffer = try allocator.alloc(u8, 64 * 1024);
                defer allocator.free(buffer);
                var offset: u64 = 0;
                while (offset < recovered.valid_active_size) {
                    const len: usize = @intCast(@min(buffer.len, recovered.valid_active_size - offset));
                    try storage.readFileRangeInto(allocator, path, offset, buffer[0..len]);
                    try sink.appendSlice(buffer[0..len]);
                    offset += len;
                }
                owned = false;
                try sink.finish();
            }
            const durable = try backend.run_directory.?.fork(allocator);
            self.obsolete = backend.obsolete_paths.fork();
            self.descriptor = recovered.descriptor;
            self.active_segment = id;
            self.segments = recovered.segment_ids;
            self.segment_count = recovered.segment_count;
            self.sequence = recovered.last_sequence;
            self.checkpoint_sequence = recovered.descriptor.sequence;
            self.bytes = @intCast(recovered.valid_bytes);
            self.edit_bytes = @intCast(recovered.valid_bytes - recovered.checkpoint_bytes);
            self.next_run_id = backend.next_run_id;
            backend.publishManifestDirectory(durable);
            return;
        };
        if (self.descriptor != null or backend.manifest_backing == null or backend.options.backend.read_only) return;
        const allocator = backend.allocator;
        const storage = backend.storage.?;
        const root = backend.root_dir.?;
        const pointer_path = try manifestPath(allocator, root);
        defer allocator.free(pointer_path);
        const pointer = try storage.readFileRangeAlloc(allocator, pointer_path, 0, 8);
        defer allocator.free(pointer);
        if (!std.mem.eql(u8, pointer, manifest_set.magic)) return;
        const raw_descriptor = try storage.readFileAlloc(allocator, pointer_path, 37);
        defer allocator.free(raw_descriptor);
        const descriptor = try manifest_set.Descriptor.decode(raw_descriptor);
        const backing = backend.manifest_backing.?;
        // The codec already validated the complete prefix. Recover sequencing
        // directly from it rather than decoding/copying all metadata again.
        if (backing.len < 36 or !std.mem.startsWith(u8, backing, lsm_manifest.journal_magic) or std.mem.readInt(u64, backing[16..24], .little) != descriptor.sequence) return error.InvalidManifest;
        var offset: usize = 8;
        var last_sequence = descriptor.sequence;
        while (backing.len - offset >= 24) {
            const len = std.mem.readInt(u64, backing[offset..][0..8], .little);
            if (len > backing.len - offset - 24 or backing.len - offset - 24 - @as(usize, @intCast(len)) < 4) break;
            last_sequence = std.mem.readInt(u64, backing[offset + 8 ..][0..8], .little);
            offset += @as(usize, @intCast(len)) + 28;
        }
        var id = descriptor.first_segment;
        var count: usize = 0;
        while (true) {
            if (count == self.segments.len) return error.ManifestJournalBacklogExceeded;
            self.segments[count] = id;
            count += 1;
            id = try manifest_set.nextSegment(allocator, storage, root, id) orelse break;
        }
        const torn_bytes = backing.len - offset;
        if (torn_bytes != 0) {
            const path = try manifest_set.pathAlloc(allocator, root, id, .journal);
            defer allocator.free(path);
            const size = try storage.fileSize(path);
            if (size < torn_bytes + manifest_set.header_len) return error.InvalidManifest;
            const valid = try storage.readFileRangeAlloc(allocator, path, 0, @intCast(size - torn_bytes));
            defer allocator.free(valid);
            try manifest_set.replace(allocator, storage, path, valid);
        }
        errdefer self.deinit(allocator);
        self.obsolete = backend.obsolete_paths.fork();
        self.descriptor = descriptor;
        self.active_segment = id;
        self.segment_count = count;
        self.sequence = last_sequence;
        self.checkpoint_sequence = descriptor.sequence;
        self.bytes = offset;
        self.edit_bytes = offset - @as(usize, @intCast(std.mem.readInt(u64, backing[8..16], .little))) - 36;
        self.next_run_id = backend.next_run_id;
        backend.publishManifestDirectory(try backend.run_directory.?.fork(allocator));
    }

    pub fn checkpointDue(self: *const @This()) bool {
        const edit_budget = @min(8 * 1024 * 1024, (max_manifest_read_bytes -| (self.bytes -| self.edit_bytes)) / 2);
        return self.sequence == null or self.sequence.? -| self.checkpoint_sequence >= 256 or self.edit_bytes >= edit_budget;
    }

    /// The builder captured an older prefix while publications advanced the
    /// active segment. The replacement must fit together with that suffix,
    /// not merely as a standalone checkpoint file. Subtraction-based checks
    /// also reject overflow and a fenced/replaced lineage's byte counters.
    pub fn checkpointFits(self: *const @This(), checkpoint_bytes: u64, captured_bytes: u64) bool {
        if (self.bytes < captured_bytes or checkpoint_bytes > max_manifest_read_bytes) return false;
        return self.bytes - captured_bytes <= max_manifest_read_bytes - checkpoint_bytes;
    }

    pub fn protectsPath(self: *const @This(), path: []const u8) bool {
        const file = manifest_set.identify(path) orelse return false;
        // An ambiguous namespace write may have committed either lineage.
        // Reclamation must wait until a replacement descriptor is durable.
        if (self.sequence == null) return true;
        if (self.descriptor) |descriptor| if (file.kind == .checkpoint and file.id == descriptor.checkpoint) return true;
        if (file.kind != .checkpoint) for (self.segments[0..self.segment_count]) |id| {
            if (file.id == id) return true;
        };
        return false;
    }

    fn retiredFileCount(self: *const @This()) usize {
        const descriptor = self.descriptor orelse return 0;
        var count: usize = 1;
        for (self.segments[0..self.segment_count]) |id| count += if (id == descriptor.checkpoint) @as(usize, 2) else 3;
        return count;
    }

    fn retiredFiles(self: *const @This(), allocator: Allocator, root: []const u8, deadline: u64, out: []lsm_manifest.ObsoletePathMeta, initialized: *usize) !void {
        const descriptor = self.descriptor orelse return;
        out[initialized.*] = .{ .path = try manifest_set.pathAlloc(allocator, root, descriptor.checkpoint, .checkpoint), .delete_after_ns = deadline };
        initialized.* += 1;
        for (self.segments[0..self.segment_count]) |id| {
            for ([_]manifest_set.Kind{ .journal, .next, .checkpoint }) |kind| {
                if (kind == .checkpoint and id == descriptor.checkpoint) continue;
                out[initialized.*] = .{ .path = try manifest_set.pathAlloc(allocator, root, id, kind), .delete_after_ns = deadline };
                initialized.* += 1;
            }
        }
    }

    /// Single-flight maintenance operation. Only the namespace handoff and
    /// descriptor installation are serialized; walking/encoding/fsyncing the
    /// immutable checkpoint runs without the backend mutex.
    pub fn runCheckpoint(self: *@This(), backend: anytype) !bool {
        if (backend.manifest_publish_in_flight or backend.manifest_checkpoint_build_in_flight or self.sequence == null or self.descriptor == null or backend.manifest_directory == null) return false;
        if (self.segment_count == manifest_set.max_segments) {
            // Repeated failed builders can consume the linked-segment bound.
            // Once storage recovers, allow a fenced replacement to settle the
            // debt instead of rejecting every future checkpoint forever.
            self.sequence = null;
            backend.markManifestDirty();
            try backend.persistManifestLocked();
            return true;
        }
        const allocator = backend.allocator;
        const storage = backend.storage.?;
        const root = backend.root_dir.?;
        var handoff = try backend.beginManifestTurn();
        var handoff_held = true;
        defer if (handoff_held) handoff.deinit();
        const old_descriptor = self.descriptor.?;
        const sequence = self.sequence.?;
        const old_edit_bytes = self.edit_bytes;
        const old_bytes = self.bytes;
        const retired_count = self.retiredFileCount();
        var reservation: ?resource_manager_mod.Reservation = null;
        if (backend.options.resource_manager) |manager| {
            // Native sinks stream, but the generic host fallback buffers the
            // full atomic file. Account its geometric buffer as well.
            const sink_bytes = if (storage.vtable.begin_atomic_write == null) self.bytes * 2 else 0;
            reservation = try manager.reserve(.lsm_table_builder_working_set, 64 * 1024 + sink_bytes + retired_count * (root.len + 128) * 3);
        }
        defer {
            backend.syncTrackedInMemoryStateUsageCurrentLocked();
            if (reservation) |*lease| lease.release();
        }
        const directory = try backend.manifest_directory.?.fork(allocator);
        defer backend.retireCheckpointDirectory(directory);
        const ledger_owner = try LedgerSnapshot.capture(backend, &self.obsolete);
        defer ledger_owner.retire(backend);
        const ledger = &ledger_owner.value;
        const paths = try allocator.alloc(lsm_manifest.ObsoletePathMeta, retired_count);
        defer allocator.free(paths);
        var initialized: usize = 0;
        var transferred = false;
        defer if (!transferred) for (paths[0..initialized]) |path| allocator.free(path.path);
        const deadline = backend.nowNs() +| backend.options.obsolete_retention_ns;
        try self.retiredFiles(allocator, root, deadline, paths, &initialized);
        const tracked_keys = try allocator.alloc([]u8, retired_count);
        defer allocator.free(tracked_keys);
        var keys_initialized: usize = 0;
        defer if (!transferred) for (tracked_keys[0..keys_initialized]) |key| allocator.free(key);
        for (paths, tracked_keys) |path, *key| {
            key.* = try allocator.dupe(u8, path.path);
            keys_initialized += 1;
        }
        const id = backend.next_run_id;
        backend.next_run_id = try std.math.add(u64, id, 1);
        const next_run_id = backend.next_run_id;
        const runtime = @import("runtime.zig");
        const BackendType = @TypeOf(backend.*);
        const unlock_io = @import("builtin").os.tag != .freestanding and !(@import("builtin").is_test and publication_test_keep_backend_locked);
        const rotate_started = backend.writeStatsNowNs();
        if (unlock_io) runtime.unlockBackend(BackendType, backend, true);
        const rotated = manifest_set.rotate(allocator, storage, root, self.active_segment, id, sequence + 1);
        if (unlock_io) {
            _ = runtime.lockBackend(BackendType, backend);
            backend.write_stats.manifest_io_unlocked_ns +|= backend.writeStatsElapsedNs(rotate_started);
        }
        rotated catch |err| {
            self.sequence = null;
            return err;
        };
        self.active_segment = id;
        self.segments[self.segment_count] = id;
        self.segment_count += 1;
        handoff.deinit();
        handoff_held = false;
        backend.write_stats.manifest_bytes += 2 * manifest_set.header_len;
        backend.manifest_checkpoint_build_in_flight = true;
        defer backend.manifest_checkpoint_build_in_flight = false;
        backend.manifest_checkpoint_build_id = id;
        defer backend.manifest_checkpoint_build_id = null;
        backend.manifest_checkpoint_directory = directory;
        defer backend.manifest_checkpoint_directory = null;
        backend.retainReaderKind(.other);
        defer backend.releaseReaderKind(.other);
        const Source = struct {
            cursor: @import("run_directory.zig").Directory.Cursor,
            ledger: ObsoleteLedger.Cursor,
            ledger_count: usize,
            paths: []const lsm_manifest.ObsoletePathMeta,
            root: []const u8,
            path_index: usize = 0,
            pub fn runCount(source: *@This()) usize {
                return source.cursor.directory.count();
            }
            pub fn obsoleteCount(source: *@This()) usize {
                return source.ledger_count + source.paths.len;
            }
            pub fn nextRun(source: *@This()) ?lsm_manifest.RunMeta {
                var meta = runMeta((source.cursor.next() orelse return null).run.*);
                meta.path = manifestRelativePath(source.root, meta.path);
                return meta;
            }
            pub fn nextObsolete(source: *@This()) ?lsm_manifest.ObsoletePathMeta {
                if (source.ledger.next()) |path| return .{ .path = manifestRelativePath(source.root, path.path), .delete_after_ns = path.delete_after_ns };
                if (source.path_index == source.paths.len) return null;
                defer source.path_index += 1;
                var path = source.paths[source.path_index];
                path.path = manifestRelativePath(source.root, path.path);
                return path;
            }
        };
        var source = Source{ .cursor = directory.readCursor(), .ledger = ledger.iterator(), .ledger_count = ledger.count(), .paths = paths, .root = root };
        runtime.unlockBackend(BackendType, backend, true);
        const built = blk: {
            if (@import("builtin").is_test) if (checkpoint_test_hook) |hook| hook.run(hook.context) catch |err| break :blk @as(anyerror!usize, err);
            break :blk manifest_set.writeCheckpoint(allocator, storage, root, id, sequence, next_run_id, &source, max_manifest_read_bytes);
        };
        _ = runtime.lockBackend(BackendType, backend);
        const checkpoint_bytes = try built;
        var publication = try backend.beginManifestTurn();
        defer publication.deinit();
        // An append fault may have fenced/replaced this set while the builder
        // was outside the mutex. Never install across that lineage change.
        if (self.sequence == null or self.descriptor == null or self.descriptor.?.checkpoint != old_descriptor.checkpoint or self.active_segment != id) return false;
        // Keep the old descriptor authoritative if concurrent suffix growth
        // exhausted recovery headroom. Publishing an individually valid base
        // plus an oversized suffix would make the next reopen unrecoverable.
        if (!self.checkpointFits(checkpoint_bytes, old_bytes)) return error.ManifestJournalBacklogExceeded;
        const next_ledger_owner = try LedgerSnapshot.capture(backend, &self.obsolete);
        defer next_ledger_owner.retire(backend);
        const next_ledger = &next_ledger_owner.value;
        // Writers may have grown the ledger while the checkpoint streamed.
        // Admit path-copy scratch from the actual publication-time heights,
        // not a fixed bytes-per-path approximation made at capture time.
        var ledger_reservation: ?resource_manager_mod.Reservation = null;
        defer {
            backend.syncTrackedInMemoryStateUsageCurrentLocked();
            if (ledger_reservation) |*lease| lease.release();
        }
        if (backend.options.resource_manager) |manager| {
            const ledger_bound = try std.math.add(u64, try next_ledger.prepareMemoryBound(retired_count), try backend.obsolete_paths.prepareMemoryBound(retired_count));
            ledger_reservation = try manager.reserve(.lsm_table_builder_working_set, ledger_bound);
        }
        try next_ledger.ensureUnusedCapacity(allocator, retired_count);
        try backend.obsolete_paths.ensureUnusedCapacity(allocator, retired_count);
        const descriptor = manifest_set.Descriptor{ .checkpoint = id, .first_segment = id, .sequence = sequence };
        // Install conservative retirement intent before dropping the mutex.
        // The old live/ambiguous lineage protects these files until the
        // descriptor succeeds; concurrent writers cannot consume reserved
        // path-copy slots needed by our post-fsync ownership transition.
        for (paths, tracked_keys) |path, key| {
            next_ledger.appendAssumeCapacity(.{ .path = key, .delete_after_ns = path.delete_after_ns });
            backend.obsolete_paths.appendAssumeCapacity(.{ .path = @constCast(path.path), .delete_after_ns = path.delete_after_ns });
        }
        transferred = true;
        backend.obsolete_manifest_dirty = true;
        const descriptor_started = backend.writeStatsNowNs();
        if (unlock_io) runtime.unlockBackend(BackendType, backend, true);
        const published = manifest_set.publish(allocator, storage, root, descriptor);
        if (unlock_io) {
            _ = runtime.lockBackend(BackendType, backend);
            backend.write_stats.manifest_io_unlocked_ns +|= backend.writeStatsElapsedNs(descriptor_started);
        }
        published catch |err| {
            self.sequence = null;
            return err;
        };
        std.mem.swap(ObsoleteLedger, &self.obsolete, next_ledger);
        self.descriptor = descriptor;
        self.checkpoint_sequence = sequence;
        self.next_run_id = @max(self.next_run_id, next_run_id);
        self.edit_bytes -= old_edit_bytes;
        self.bytes = checkpoint_bytes + (self.bytes - old_bytes);
        self.segments[0] = id;
        self.segment_count = 1;
        backend.current_manifest_bytes = self.bytes;
        backend.write_stats.manifest_checkpoints += 1;
        backend.write_stats.manifest_writes += 1;
        backend.write_stats.manifest_bytes += checkpoint_bytes + descriptor.encode().len;
        return true;
    }

    /// Caller holds the publication lock. The last durable directory owns SST
    /// pins until its successor is durable, independently of reader lifetimes.
    pub fn persist(self: *@This(), backend: anytype, root_dir: []const u8, runs: []const Run) !Result {
        return self.persistAttempt(backend, root_dir, runs) catch |err| {
            if (err != error.ManifestEditTooLarge) return err;
            // The failed preparation already reset the journal state and
            // released its scratch. Replace it with a full checkpoint rather
            // than rejecting an otherwise representable large publication.
            return self.persistAttempt(backend, root_dir, runs);
        };
    }

    /// Cold/fenced replacement has the same immutable capture boundary as an
    /// edit. Never encode all live runs into a temporary frame under the mutex.
    fn persistCheckpointPinned(self: *@This(), backend: anytype, root: []const u8, current: *const @import("run_directory.zig").Directory) !Result {
        const allocator = backend.allocator;
        const storage = backend.storage.?;
        errdefer self.sequence = null;
        var reservation: ?resource_manager_mod.Reservation = null;
        defer {
            backend.syncTrackedInMemoryStateUsageCurrentLocked();
            if (reservation) |*lease| lease.release();
        }
        const retired_count = self.retiredFileCount();
        const prepared_paths = try std.math.add(usize, backend.obsolete_paths.spare.items.len, retired_count);
        if (backend.options.resource_manager) |manager| {
            const bound = try std.math.add(u64, 64 * 1024 + retired_count * (root.len + 128) * 2, try backend.obsolete_paths.prepareMemoryBound(prepared_paths));
            reservation = try manager.reserve(.lsm_table_builder_working_set, bound);
        }
        const directory = try current.fork(allocator);
        errdefer directory.destroy(allocator);
        const retired = try allocator.alloc(lsm_manifest.ObsoletePathMeta, retired_count);
        defer allocator.free(retired);
        var initialized: usize = 0;
        var transferred = false;
        defer if (!transferred) for (retired[0..initialized]) |path| allocator.free(path.path);
        try self.retiredFiles(allocator, root, backend.nowNs() +| backend.options.obsolete_retention_ns, retired, &initialized);
        try backend.obsolete_paths.ensureUnusedCapacity(allocator, prepared_paths);
        for (retired) |path| {
            if (backend.obsolete_paths.contains(path.path)) {
                allocator.free(path.path);
            } else backend.obsolete_paths.appendAssumeCapacity(.{ .path = @constCast(path.path), .delete_after_ns = path.delete_after_ns });
        }
        transferred = true;
        const ledger_owner = try LedgerSnapshot.capture(backend, &backend.obsolete_paths);
        defer ledger_owner.retire(backend);
        const ledger = &ledger_owner.value;
        const id = backend.next_run_id;
        backend.next_run_id = try std.math.add(u64, id, 1);
        const next_run_id = backend.next_run_id;
        const Source = struct {
            cursor: @import("run_directory.zig").Directory.Cursor,
            ledger: ObsoleteLedger.Cursor,
            count: usize,
            root: []const u8,
            pub fn runCount(source: *@This()) usize {
                return source.cursor.directory.count();
            }
            pub fn obsoleteCount(source: *@This()) usize {
                return source.count;
            }
            pub fn nextRun(source: *@This()) ?lsm_manifest.RunMeta {
                var meta = runMeta((source.cursor.next() orelse return null).run.*);
                meta.path = manifestRelativePath(source.root, meta.path);
                return meta;
            }
            pub fn nextObsolete(source: *@This()) ?lsm_manifest.ObsoletePathMeta {
                const path = source.ledger.next() orelse return null;
                return .{ .path = manifestRelativePath(source.root, path.path), .delete_after_ns = path.delete_after_ns };
            }
        };
        var source = Source{ .cursor = directory.readCursor(), .ledger = ledger.iterator(), .count = ledger.count(), .root = root };
        const runtime = @import("runtime.zig");
        const BackendType = @TypeOf(backend.*);
        const unlocked = @import("builtin").os.tag != .freestanding and !(@import("builtin").is_test and publication_test_keep_backend_locked);
        const started = backend.writeStatsNowNs();
        if (unlocked) runtime.unlockBackend(BackendType, backend, true);
        const descriptor = manifest_set.Descriptor{ .checkpoint = id, .first_segment = id, .sequence = 0 };
        const built = blk: {
            var sink_reservation: ?resource_manager_mod.Reservation = null;
            defer if (sink_reservation) |*lease| lease.release();
            if (storage.vtable.begin_atomic_write == null) {
                // Host adapters without streaming sinks buffer the atomic
                // object. Measure/admit that buffer off-lock before allocating.
                if (backend.options.resource_manager) |manager| {
                    var measure: Measure = .{};
                    var runs = directory.readCursor();
                    while (runs.next()) |handle| measure.put(handle.run.*) catch |err| break :blk @as(anyerror!usize, err);
                    var paths = ledger.iterator();
                    while (paths.next()) |path| measure.wire_bytes += 12 + path.path.len;
                    sink_reservation = manager.reserve(.lsm_table_builder_working_set, measure.wire_bytes * 2) catch |err| break :blk @as(anyerror!usize, err);
                }
            }
            if (@import("builtin").is_test and unlocked) if (publication_test_hook) |hook| hook.run(hook.context) catch |err| break :blk @as(anyerror!usize, err);
            ensureOpenDirsWithStorage(storage, root) catch |err| break :blk @as(anyerror!usize, err);
            const bytes = manifest_set.writeCheckpoint(allocator, storage, root, id, 0, next_run_id, &source, max_manifest_read_bytes) catch |err| break :blk @as(anyerror!usize, err);
            manifest_set.createSegment(allocator, storage, root, id, 1) catch |err| break :blk @as(anyerror!usize, err);
            manifest_set.publish(allocator, storage, root, descriptor) catch |err| break :blk @as(anyerror!usize, err);
            break :blk @as(anyerror!usize, bytes);
        };
        if (unlocked) {
            _ = runtime.lockBackend(BackendType, backend);
            backend.write_stats.manifest_io_unlocked_ns +|= backend.writeStatsElapsedNs(started);
        }
        const bytes = try built;
        std.mem.swap(ObsoleteLedger, &self.obsolete, ledger);
        self.descriptor = descriptor;
        self.active_segment = id;
        self.segments[0] = id;
        self.segment_count = 1;
        self.sequence = 0;
        self.checkpoint_sequence = 0;
        self.bytes = bytes;
        self.edit_bytes = 0;
        self.next_run_id = next_run_id;
        backend.publishManifestDirectory(directory);
        return .{ .written = bytes + 36 + manifest_set.header_len, .total = bytes, .checkpoint = true };
    }

    fn persistAttempt(self: *@This(), backend: anytype, root_dir: []const u8, runs: []const Run) !Result {
        const allocator = backend.allocator;
        const current = if (backend.manifest_publish_allow_concurrency and !backend.run_directory_dirty) backend.run_directory else null;
        // Count suffix bytes, not the checkpoint itself: large stores must
        // not checkpoint on every edit merely because their base exceeds 8 MiB.
        const checkpoint = self.sequence == null or self.descriptor == null or current == null or backend.manifest_directory == null;
        if (checkpoint) if (current) |directory| return self.persistCheckpointPinned(backend, root_dir, directory);
        if (!checkpoint and current.?.tree.root == backend.manifest_directory.?.tree.root and self.next_run_id == backend.next_run_id and self.obsolete.tree.root == backend.obsolete_paths.tree.root)
            return .{ .written = 0, .total = self.bytes, .checkpoint = false };
        var reservation: ?resource_manager_mod.Reservation = null;
        defer {
            backend.syncTrackedInMemoryStateUsageCurrentLocked();
            if (reservation) |*lease| lease.release();
        }
        if (backend.options.resource_manager) |manager| {
            var measure: Measure = .{};
            if (checkpoint) {
                for (runs) |run| try measure.put(run);
                measure.wire_bytes += self.retiredFileCount() * (root_dir.len + 128);
            } else try current.?.changesSince(backend.manifest_directory.?, &measure);
            var path_measure = struct {
                bytes: *u64,
                pub fn put(measured: *@This(), path: ObsoletePath) !void {
                    measured.bytes.* += 12 + path.path.len;
                }
                pub fn remove(measured: *@This(), path: ObsoletePath) !void {
                    measured.bytes.* += 4 + path.path.len;
                }
            }{ .bytes = &measure.wire_bytes };
            if (checkpoint) {
                var paths = backend.obsolete_paths.iterator();
                while (paths.next()) |path| try path_measure.put(path);
            } else try backend.obsolete_paths.changesSince(&self.obsolete, &path_measure);
            // Three framed encoding buffers, geometric growth, descriptor
            // scratch, and path-map rehashing are admitted before allocation.
            const bound = measure.wire_bytes * 8 + measure.rows * @sizeOf(lsm_manifest.RunMeta) * 2;
            // Encoding is transient builder work, not retained memtable/run
            // metadata. Charging it to retained state can prevent a pressured
            // memtable from flushing precisely when publication frees memory.
            // The persistent directory/path ledger is observed separately in
            // the defer above, before this scratch reservation is released.
            reservation = try manager.reserve(.lsm_table_builder_working_set, bound);
        }
        const next_directory = if (current) |directory| try directory.fork(allocator) else null;
        errdefer if (next_directory) |directory| directory.destroy(allocator);
        // Any error, including an ambiguous append/sync result, forces the
        // next attempt to atomically replace the journal with a checkpoint.
        errdefer self.sequence = null;
        const Delta = struct {
            allocator: Allocator,
            root: []const u8,
            added: std.ArrayListUnmanaged(lsm_manifest.RunMeta) = .empty,
            removed: std.ArrayListUnmanaged(u64) = .empty,
            pub fn put(delta: *@This(), run: Run) !void {
                var meta = runMeta(run);
                meta.path = manifestRelativePath(delta.root, meta.path);
                try delta.added.append(delta.allocator, meta);
            }
            pub fn remove(delta: *@This(), run: Run) !void {
                try delta.removed.append(delta.allocator, run.id);
            }
        };
        var delta = Delta{ .allocator = allocator, .root = root_dir };
        defer delta.added.deinit(allocator);
        defer delta.removed.deinit(allocator);
        if (checkpoint) {
            for (runs) |run| try delta.put(run);
            // Record the entire old lineage in the replacement checkpoint,
            // including checkpoints left by interrupted builders. Keep the
            // old descriptor in memory until publication succeeds so retries
            // and reclamation retain a conservative live-file fence.
            const retired = try allocator.alloc(lsm_manifest.ObsoletePathMeta, self.retiredFileCount());
            defer allocator.free(retired);
            var initialized: usize = 0;
            defer for (retired[0..initialized]) |path| allocator.free(path.path);
            try self.retiredFiles(allocator, root_dir, backend.nowNs() +| backend.options.obsolete_retention_ns, retired, &initialized);
            for (retired) |path| {
                if (backend.obsolete_paths.contains(path.path)) continue;
                const owned = try allocator.dupe(u8, path.path);
                errdefer allocator.free(owned);
                try backend.obsolete_paths.append(allocator, .{ .path = owned, .delete_after_ns = path.delete_after_ns });
            }
        } else try current.?.changesSince(backend.manifest_directory.?, &delta);

        var added_paths: std.ArrayListUnmanaged(lsm_manifest.ObsoletePathMeta) = .empty;
        defer added_paths.deinit(allocator);
        var removed_paths: std.ArrayListUnmanaged([]const u8) = .empty;
        defer removed_paths.deinit(allocator);
        var path_delta = struct {
            allocator: Allocator,
            root: []const u8,
            added: *std.ArrayListUnmanaged(lsm_manifest.ObsoletePathMeta),
            removed: *std.ArrayListUnmanaged([]const u8),
            pub fn put(change: *@This(), path: ObsoletePath) !void {
                try change.added.append(change.allocator, .{ .path = manifestRelativePath(change.root, path.path), .delete_after_ns = path.delete_after_ns });
            }
            pub fn remove(change: *@This(), path: ObsoletePath) !void {
                try change.removed.append(change.allocator, path.path);
            }
        }{ .allocator = allocator, .root = root_dir, .added = &added_paths, .removed = &removed_paths };
        if (checkpoint) {
            var paths = backend.obsolete_paths.iterator();
            while (paths.next()) |path| try path_delta.put(path);
        } else try backend.obsolete_paths.changesSince(&self.obsolete, &path_delta);
        if (!checkpoint and delta.added.items.len == 0 and delta.removed.items.len == 0 and added_paths.items.len == 0 and removed_paths.items.len == 0 and self.next_run_id == backend.next_run_id) {
            // Semantically unchanged edits may have a different COW root.
            // Settle that identity as well, or cleanup debt stays dirty and
            // every maintenance turn repeats the same empty ledger diff.
            const settled = try LedgerSnapshot.capture(backend, &backend.obsolete_paths);
            defer settled.retire(backend);
            std.mem.swap(ObsoleteLedger, &self.obsolete, &settled.value);
            backend.publishManifestDirectory(next_directory);
            return .{ .written = 0, .total = self.bytes, .checkpoint = false };
        }
        const file_id = if (checkpoint) blk: {
            const id = backend.next_run_id;
            backend.next_run_id = try std.math.add(u64, id, 1);
            break :blk id;
        } else self.active_segment;
        const next_run_id = backend.next_run_id;
        const next_obsolete_owner = try LedgerSnapshot.capture(backend, &backend.obsolete_paths);
        defer next_obsolete_owner.retire(backend);
        const next_obsolete = &next_obsolete_owner.value;
        const sequence = if (checkpoint) 0 else self.sequence.? + 1;
        const wire_removed_paths = try allocator.alloc([]const u8, removed_paths.items.len);
        defer allocator.free(wire_removed_paths);
        for (removed_paths.items, wire_removed_paths) |path, *wire| wire.* = manifestRelativePath(root_dir, path);
        const encoded = try lsm_manifest.encodeJournalFrameAlloc(allocator, sequence, checkpoint, delta.removed.items, wire_removed_paths, .{
            .next_run_id = next_run_id,
            .runs = delta.added.items,
            .obsolete_paths = added_paths.items,
        });
        defer allocator.free(encoded);
        const total = if (checkpoint) encoded.len else self.bytes + encoded.len;
        if (total > max_manifest_read_bytes) return if (checkpoint) error.FileTooBig else error.ManifestEditTooLarge;
        const unlocked = current != null and @import("builtin").os.tag != .freestanding and !(@import("builtin").is_test and publication_test_keep_backend_locked);
        const io_started = backend.writeStatsNowNs();
        if (unlocked) @import("runtime.zig").unlockBackend(@TypeOf(backend.*), backend, true);
        const written = blk: {
            if (@import("builtin").is_test and unlocked) if (publication_test_hook) |hook| hook.run(hook.context) catch |err| break :blk @as(anyerror!?manifest_set.Descriptor, err);
            break :blk writePublicationFiles(allocator, backend.storage.?, root_dir, file_id, checkpoint, encoded);
        };
        if (unlocked) {
            _ = @import("runtime.zig").lockBackend(@TypeOf(backend.*), backend);
            backend.write_stats.manifest_io_unlocked_ns +|= backend.writeStatsElapsedNs(io_started);
        }
        if (try written) |descriptor| {
            self.descriptor = descriptor;
            self.active_segment = file_id;
            self.segments[0] = file_id;
            self.segment_count = 1;
            self.checkpoint_sequence = 0;
        }
        self.sequence = sequence;
        std.mem.swap(ObsoleteLedger, &self.obsolete, next_obsolete);
        self.bytes = total;
        self.edit_bytes = if (checkpoint) 0 else self.edit_bytes + encoded.len;
        self.next_run_id = next_run_id;
        backend.publishManifestDirectory(next_directory);
        return .{ .written = encoded.len + (if (checkpoint) @as(usize, 36 + manifest_set.header_len) else 0), .total = total, .checkpoint = checkpoint };
    }
};

fn writePublicationFiles(allocator: Allocator, storage: storage_io.Storage, root: []const u8, file_id: u64, checkpoint: bool, encoded: []const u8) !?manifest_set.Descriptor {
    try ensureOpenDirsWithStorage(storage, root);
    const path = try manifest_set.pathAlloc(allocator, root, file_id, if (checkpoint) .checkpoint else .journal);
    defer allocator.free(path);
    if (!checkpoint) {
        try storage.appendFileAbsolute(allocator, path, encoded, true);
        return null;
    }
    try replaceFileAtomicallyAbsolute(storage, path, encoded);
    try manifest_set.createSegment(allocator, storage, root, file_id, 1);
    const descriptor = manifest_set.Descriptor{ .checkpoint = file_id, .first_segment = file_id, .sequence = 0 };
    try manifest_set.publish(allocator, storage, root, descriptor);
    return descriptor;
}

test "manifest checkpoint headroom includes the concurrently advanced suffix" {
    var journal = ManifestJournal{ .bytes = 1000 };
    try std.testing.expect(journal.checkpointFits(max_manifest_read_bytes, 1000));
    journal.bytes += 1;
    try std.testing.expect(!journal.checkpointFits(max_manifest_read_bytes, 1000));
    try std.testing.expect(journal.checkpointFits(max_manifest_read_bytes - 1, 1000));
    try std.testing.expect(!journal.checkpointFits(max_manifest_read_bytes + 1, 1000));
    try std.testing.expect(!journal.checkpointFits(1, 1002));
    journal.bytes = std.math.maxInt(u64);
    try std.testing.expect(!journal.checkpointFits(1, 0));
}

test "manifest checkpoint cadence measures suffix rather than large base bytes" {
    var journal = ManifestJournal{ .sequence = 1, .bytes = 32 * 1024 * 1024 };
    try std.testing.expect(!journal.checkpointDue());
    journal.edit_bytes = 8 * 1024 * 1024;
    try std.testing.expect(journal.checkpointDue());
    journal.edit_bytes = 0;
    journal.sequence = 256;
    try std.testing.expect(journal.checkpointDue());
    journal.sequence = 1;
    journal.bytes = 127 * 1024 * 1024 + 512 * 1024;
    journal.edit_bytes = 512 * 1024;
    try std.testing.expect(journal.checkpointDue());
}

fn estimateRunBytes(entry_count: u32, bloom_len: usize) u64 {
    return @as(u64, entry_count) * 64 + @as(u64, @intCast(bloom_len));
}

pub fn loadRunStateAlloc(allocator: Allocator, path: []const u8) !state_mod.State {
    var native = try storage_io.NativeStorage.init(std.heap.page_allocator, .threaded);
    defer native.deinit();
    return try loadRunStateAllocWithStorage(native.storage(), allocator, path);
}

pub fn loadRunStateAllocWithStorage(storage: storage_io.Storage, allocator: Allocator, path: []const u8) !state_mod.State {
    var index = try loadRunTableIndexAllocWithStorage(storage, allocator, path);
    defer index.deinit(allocator);

    var state: state_mod.State = .{};
    errdefer state.deinit(allocator);
    try state.entries.ensureTotalCapacity(allocator, index.entryCount());
    if (index.entryCount() > 0 and index.blockCount() == 0) return error.InvalidTableFile;

    for (index.blocks, 0..) |block, block_index| {
        const window = index.blockWindow(block_index);
        const payload = try storage.readFileRangeAlloc(
            allocator,
            path,
            @as(u64, @intCast(index.entry_data_start)) + window.physicalRelativeOffset(),
            window.physicalLen(),
        );
        defer allocator.free(payload);
        const bytes = try lsm_table_file.decodeBlockPayloadAlloc(
            allocator,
            window.compression,
            payload,
            window.len,
            window.checksum,
        );
        defer allocator.free(bytes);

        const end = block.first_entry_index + block.entry_count;
        for (block.first_entry_index..end) |entry_index| {
            const relative_offset: usize = @intCast(index.entryStartInBlock(entry_index, block_index) - window.relative_offset);
            const entry = try lsm_table_file.parseEntryAt(bytes, relative_offset);
            try appendStateEntryClone(allocator, &state, entry);
        }
    }
    return state;
}

fn appendStateEntryClone(allocator: Allocator, state: *state_mod.State, entry: lsm_table_file.Entry) !void {
    const namespace_name = if (entry.namespace_name) |name| try allocator.dupe(u8, name) else null;
    errdefer if (namespace_name) |name| allocator.free(name);
    const key = try allocator.dupe(u8, entry.key);
    errdefer allocator.free(key);
    const value = try allocator.dupe(u8, entry.value);
    errdefer allocator.free(value);
    state.entries.appendAssumeCapacity(.{
        .namespace_name = namespace_name,
        .key = key,
        .value = value,
        .tombstone = entry.tombstone,
    });
}

pub fn loadRunTableBorrowedAlloc(allocator: Allocator, path: []const u8) !lsm_table_file.BorrowedDecoded {
    var native = try storage_io.NativeStorage.init(std.heap.page_allocator, .threaded);
    defer native.deinit();
    return try loadRunTableBorrowedAllocWithStorage(native.storage(), allocator, path);
}

pub fn loadRunTableBorrowedAllocWithStorage(
    storage: storage_io.Storage,
    allocator: Allocator,
    path: []const u8,
) !lsm_table_file.BorrowedDecoded {
    const raw_table = storage.readFileAlloc(allocator, path, max_run_file_read_bytes) catch |err| {
        logReadFileFailure(storage, path, max_run_file_read_bytes, "loadRunTableBorrowedAllocWithStorage", err);
        return err;
    };
    errdefer allocator.free(raw_table);
    return try lsm_table_file.decodeBorrowedOwnedAlloc(allocator, raw_table);
}

pub fn loadRunTableIndexAllocWithStorage(
    storage: storage_io.Storage,
    allocator: Allocator,
    path: []const u8,
) !lsm_table_file.TableIndex {
    // A missing or truncated run file usually means a concurrent writer
    // obsoleted and reclaimed it after this reader loaded the manifest;
    // propagate the transient error so callers can retry against a fresh
    // manifest instead of misreporting a format mismatch.
    const footer = try loadRunFooterWithStorage(storage, allocator, path);
    const metadata_bytes = try storage.readFileRangeAlloc(allocator, path, footer.metadata_offset, footer.metadata_len);
    defer allocator.free(metadata_bytes);
    return try lsm_table_file.decodeIndexFromFooterAlloc(allocator, footer, metadata_bytes);
}

pub fn loadRunSequentialTableIndexAllocWithStorage(
    storage: storage_io.Storage,
    allocator: Allocator,
    path: []const u8,
) !lsm_table_file.SequentialTableIndex {
    const footer = try loadRunFooterWithStorage(storage, allocator, path);
    const metadata_bytes = try storage.readFileRangeAlloc(allocator, path, footer.metadata_offset, footer.metadata_len);
    defer allocator.free(metadata_bytes);
    return try lsm_table_file.decodeSequentialIndexFromFooterAlloc(allocator, footer, metadata_bytes);
}

pub fn loadRunFooterWithStorage(
    storage: storage_io.Storage,
    allocator: Allocator,
    path: []const u8,
) !lsm_table_file.Footer {
    var trailer = try storage.readFileTrailerAlloc(
        allocator,
        path,
        lsm_table_file.footer_len,
    );
    defer trailer.deinit(allocator);

    const file_size = trailer.file_size;
    if (file_size > max_run_file_read_bytes) return error.FileTooBig;
    if (file_size < lsm_table_file.header_len + lsm_table_file.footer_len)
        return error.InvalidTableFile;

    const footer_offset_u64 = file_size - lsm_table_file.footer_len;
    if (!lsm_table_file.hasFooterMagic(trailer.bytes)) return error.UnsupportedVersion;

    const footer = try lsm_table_file.decodeFooterBytes(trailer.bytes);
    const footer_offset: usize = @intCast(footer_offset_u64);
    if (footer.metadata_offset > footer_offset or
        footer.metadata_len != footer_offset - footer.metadata_offset)
    {
        return error.InvalidTableFile;
    }
    return footer;
}

/// Validate the checksummed manifest's exact physical run size during mount.
/// This preserves lazy table I/O while ensuring an oversized run is rejected
/// before the backend can report ready.
pub fn validateManifestRunSize(manifest_size: u64) !void {
    if (manifest_size > max_run_file_read_bytes) return error.FileTooBig;
}

/// Validate the immutable file against the checksummed manifest without
/// decoding or allocating the table. Run files are atomically published and
/// never modified in place, so a size mismatch means the manifest and file do
/// not describe the same durable object. Reject it during mount instead of
/// letting the backend report ready and fail on the first query.
pub fn validateManifestRunPhysicalSize(manifest_size: u64, physical_size: u64) !void {
    try validateManifestRunSize(manifest_size);
    if (physical_size > max_run_file_read_bytes) return error.FileTooBig;
    if (physical_size != manifest_size) return error.InvalidTableFile;
}

pub fn deleteFileAbsolute(path: []const u8) !void {
    var native = try storage_io.NativeStorage.init(std.heap.page_allocator, .threaded);
    defer native.deinit();
    try deleteFileAbsoluteWithStorage(native.storage(), path);
}

pub fn deleteFileAbsoluteWithStorage(storage: storage_io.Storage, path: []const u8) !void {
    try storage.deleteFileAbsolute(path);
}

pub fn manifestPath(allocator: Allocator, root_dir: []const u8) ![]u8 {
    return try joinPath(allocator, root_dir, "manifest.bin");
}

pub fn runPath(allocator: Allocator, root_dir: []const u8, run_id: u64) ![]u8 {
    const suffix = try std.fmt.allocPrint(allocator, "runs/{d}.tbl", .{run_id});
    defer allocator.free(suffix);
    return try joinPath(allocator, root_dir, suffix);
}

pub fn tmpPath(buf: []u8, label: []const u8) [*:0]const u8 {
    const nonce = test_nonce.fetchAdd(1, .monotonic);
    const slice = std.fmt.bufPrint(buf, "/tmp/antfly-lsm-{s}-{d}-{d}\x00", .{
        label,
        nowNs(),
        nonce,
    }) catch unreachable;
    return @ptrCast(slice.ptr);
}

pub fn cleanupTmp(path: [*:0]const u8) void {
    var native = storage_io.NativeStorage.init(std.heap.page_allocator, .threaded) catch return;
    defer native.deinit();
    native.storage().deleteTree(std.mem.span(path)) catch {};
}

pub fn writeFileAbsolute(path: []const u8, contents: []const u8) !void {
    var native = try storage_io.NativeStorage.init(std.heap.page_allocator, .threaded);
    defer native.deinit();
    try writeFileAbsoluteWithStorage(native.storage(), path, contents);
}

pub fn writeFileAbsoluteWithStorage(storage: storage_io.Storage, path: []const u8, contents: []const u8) !void {
    try storage.writeFileAbsolute(path, contents);
}

pub fn copyFileAbsolute(allocator: Allocator, src_path: []const u8, dst_path: []const u8) !u64 {
    var native = try storage_io.NativeStorage.init(std.heap.page_allocator, .threaded);
    defer native.deinit();
    return try copyFileAbsoluteWithStorage(native.storage(), allocator, src_path, dst_path);
}

pub fn copyFileAbsoluteWithStorage(
    storage: storage_io.Storage,
    allocator: Allocator,
    src_path: []const u8,
    dst_path: []const u8,
) !u64 {
    const max_copy_bytes = 256 * 1024 * 1024;
    const contents = storage.readFileAlloc(allocator, src_path, max_copy_bytes) catch |err| {
        logReadFileFailure(storage, src_path, max_copy_bytes, "copyFileAbsoluteWithStorage", err);
        return err;
    };
    defer allocator.free(contents);
    try writeFileAbsoluteWithStorage(storage, dst_path, contents);
    return contents.len;
}

fn logReadFileFailure(storage: storage_io.Storage, path: []const u8, max_bytes: usize, site: []const u8, err: anyerror) void {
    if (err != error.StreamTooLong) return;
    const size = storage.fileSize(path) catch |size_err| {
        std.log.err("lsm readFileAlloc StreamTooLong site={s} path={s} max_bytes={d} file_size_err={}", .{ site, path, max_bytes, size_err });
        return;
    };
    std.log.err("lsm readFileAlloc StreamTooLong site={s} path={s} max_bytes={d} file_size={d}", .{ site, path, max_bytes, size });
}

pub fn tempSiblingPath(allocator: Allocator, path: []const u8) ![]u8 {
    const nonce = test_nonce.fetchAdd(1, .monotonic);
    return try std.fmt.allocPrint(allocator, "{s}.tmp-{d}", .{ path, nonce });
}

fn writeTableFileAtomically(
    storage: storage_io.Storage,
    allocator: Allocator,
    path: []const u8,
    entries: []const lsm_table_file.Entry,
    filter: bloom.OwnedFilter,
    compression_policy: lsm_table_file.CompressionPolicy,
) !WrittenTableFile {
    var writer = try storage.beginAtomicWrite(allocator, path);
    writer.setCacheIntent(.cold_sequential);
    var active = true;
    defer if (active) writer.abort();

    var adapter = try BufferedAtomicTableSink.init(allocator, &writer);
    defer adapter.deinit();
    var sink = adapter.sink();
    var compression_stats: lsm_table_file.CompressionStats = .{};
    const size_bytes = try lsm_table_file.encodeWithFilterToSinkOptions(allocator, &sink, entries, filter, .{
        .block_compression = compression_policy,
        .compression_stats = &compression_stats,
    });
    // The legacy whole-table path is not used by normal flush or compaction,
    // but it must obey the same reader contract: never publish a run that the
    // repository's own bounded reader will reject.
    if (size_bytes > max_run_file_read_bytes) return error.TableFileTooLarge;
    try adapter.flush();

    active = false;
    try writer.finish();
    return .{
        .size_bytes = @intCast(size_bytes),
        .compression_stats = compression_stats,
    };
}

const BufferedAtomicTableSink = struct {
    allocator: Allocator,
    writer: *storage_io.AtomicWriteSink,
    buffer: []u8,
    len_buffered: usize = 0,
    len_flushed: usize = 0,

    fn init(allocator: Allocator, writer: *storage_io.AtomicWriteSink) !BufferedAtomicTableSink {
        return .{
            .allocator = allocator,
            .writer = writer,
            .buffer = try allocator.alloc(u8, table_write_buffer_size),
        };
    }

    fn deinit(self: *BufferedAtomicTableSink) void {
        self.allocator.free(self.buffer);
        self.* = undefined;
    }

    fn sink(self: *BufferedAtomicTableSink) lsm_table_file.TableSink {
        return .{
            .ptr = self,
            .vtable = &buffered_atomic_table_sink_vtable,
        };
    }

    fn flush(self: *BufferedAtomicTableSink) !void {
        if (self.len_buffered == 0) return;
        try self.writer.appendSlice(self.buffer[0..self.len_buffered]);
        self.len_flushed += self.len_buffered;
        self.len_buffered = 0;
    }

    fn len(ptr: *anyopaque) usize {
        const self: *BufferedAtomicTableSink = @ptrCast(@alignCast(ptr));
        return self.len_flushed + self.len_buffered;
    }

    fn appendSlice(ptr: *anyopaque, bytes: []const u8) !void {
        const self: *BufferedAtomicTableSink = @ptrCast(@alignCast(ptr));
        var remaining = bytes;
        while (remaining.len > 0) {
            if (self.len_buffered == self.buffer.len) try self.flush();
            if (remaining.len >= self.buffer.len and self.len_buffered == 0) {
                const direct_len = remaining.len - (remaining.len % self.buffer.len);
                try self.writer.appendSlice(remaining[0..direct_len]);
                self.len_flushed += direct_len;
                remaining = remaining[direct_len..];
                continue;
            }
            const n = @min(self.buffer.len - self.len_buffered, remaining.len);
            byte_copy.copyPossiblyAliased(self.buffer[self.len_buffered..][0..n], remaining[0..n]);
            self.len_buffered += n;
            remaining = remaining[n..];
        }
    }

    fn appendByte(ptr: *anyopaque, byte: u8) !void {
        const self: *BufferedAtomicTableSink = @ptrCast(@alignCast(ptr));
        if (self.len_buffered == self.buffer.len) try self.flush();
        self.buffer[self.len_buffered] = byte;
        self.len_buffered += 1;
    }

    fn writeAt(ptr: *anyopaque, offset: usize, bytes: []const u8) !void {
        const self: *BufferedAtomicTableSink = @ptrCast(@alignCast(ptr));
        const logical_len = self.len_flushed + self.len_buffered;
        if (offset > logical_len or bytes.len > logical_len - offset) return error.InvalidAtomicWriteOffset;

        var src_offset: usize = 0;
        if (offset < self.len_flushed) {
            const flushed_len = @min(bytes.len, self.len_flushed - offset);
            try self.writer.writeAt(offset, bytes[0..flushed_len]);
            src_offset = flushed_len;
        }

        if (src_offset < bytes.len) {
            const buffer_offset = offset + src_offset - self.len_flushed;
            byte_copy.copyPossiblyAliased(self.buffer[buffer_offset..][0 .. bytes.len - src_offset], bytes[src_offset..]);
        }
    }
};

const buffered_atomic_table_sink_vtable: lsm_table_file.TableSink.VTable = .{
    .len = BufferedAtomicTableSink.len,
    .append_slice = BufferedAtomicTableSink.appendSlice,
    .append_byte = BufferedAtomicTableSink.appendByte,
    .write_at = BufferedAtomicTableSink.writeAt,
};

fn joinPath(allocator: Allocator, root_dir: []const u8, suffix: []const u8) ![]u8 {
    return try std.fs.path.join(allocator, &.{ root_dir, suffix });
}

fn replaceFileAtomicallyAbsolute(storage: storage_io.Storage, path: []const u8, contents: []const u8) !void {
    var writer = storage.beginAtomicWrite(std.heap.page_allocator, path) catch |err| {
        if (!isInjectedStorageFault(err)) {
            std.log.err("lsm atomic replace begin failed path={s} bytes={} err={s}", .{ path, contents.len, @errorName(err) });
        }
        return err;
    };
    var writer_open = true;
    defer if (writer_open) writer.abort();
    writer.appendSlice(contents) catch |err| {
        if (!isInjectedStorageFault(err)) {
            std.log.err("lsm atomic replace write failed path={s} bytes={} err={s}", .{ path, contents.len, @errorName(err) });
        }
        return err;
    };
    writer_open = false;
    writer.finish() catch |err| {
        if (!isInjectedStorageFault(err)) {
            std.log.err("lsm atomic replace publish failed path={s} bytes={} err={s}", .{ path, contents.len, @errorName(err) });
        }
        return err;
    };
}

fn isInjectedStorageFault(err: anyerror) bool {
    return err == error.InjectedWriteFault or
        err == error.InjectedSyncFault or
        err == error.InjectedDeleteFault;
}

fn nowNs() u64 {
    var native = storage_io.NativeStorage.init(std.heap.page_allocator, .threaded) catch return 0;
    defer native.deinit();
    return native.storage().nowNs();
}

var test_nonce: std.atomic.Value(u32) = .init(0);
pub const PersistedRunFile = struct {
    path: []u8,
    size_bytes: u64,
    compression_stats: lsm_table_file.CompressionStats = .{},
};

pub const PersistedStreamingRunFile = struct {
    path: []u8,
    size_bytes: u64,
    entry_count: usize,
    compression_stats: lsm_table_file.CompressionStats = .{},
    filter: bloom.OwnedFilter,
};

pub const StreamingRunFileWriter = struct {
    allocator: Allocator,
    path: []u8 = &.{},
    writer: storage_io.AtomicWriteSink = undefined,
    writer_active: bool = false,
    adapter: BufferedAtomicTableSink = undefined,
    adapter_active: bool = false,
    sink: lsm_table_file.TableSink = undefined,
    encoder: lsm_table_file.StreamingEncoder = undefined,
    encoder_active: bool = false,
    resource_manager: ?*resource_manager_mod.ResourceManager = null,
    tracked_builder_bytes: u64 = 0,
    last_reported_builder_bytes: u64 = 0,
    max_file_bytes: usize = max_run_file_read_bytes,

    pub fn initInPlace(
        self: *StreamingRunFileWriter,
        storage: storage_io.Storage,
        allocator: Allocator,
        root_dir: []const u8,
        run_id: u64,
        expected_entries: usize,
        max_file_bytes: usize,
        bloom_config: bloom.Config,
        compression_policy: lsm_table_file.CompressionPolicy,
        prefix_extractor: lsm_table_file.PrefixExtractor,
        resource_manager: ?*resource_manager_mod.ResourceManager,
        cache_intent: storage_io.AtomicWriteCacheIntent,
    ) !void {
        self.* = .{
            .allocator = allocator,
            .resource_manager = resource_manager,
            .max_file_bytes = max_file_bytes,
        };
        try ensureOpenDirsWithStorage(storage, root_dir);
        self.path = try runPath(allocator, root_dir, run_id);
        errdefer {
            allocator.free(self.path);
            self.path = &.{};
        }

        self.writer = try storage.beginAtomicWrite(allocator, self.path);
        self.writer.setCacheIntent(cache_intent);
        self.writer_active = true;
        errdefer {
            self.writer.abort();
            self.writer_active = false;
        }

        self.adapter = try BufferedAtomicTableSink.init(allocator, &self.writer);
        self.adapter_active = true;
        errdefer {
            self.adapter.deinit();
            self.adapter_active = false;
        }

        self.sink = self.adapter.sink();
        self.encoder = try lsm_table_file.StreamingEncoder.init(allocator, &self.sink, expected_entries, .{
            .block_compression = compression_policy,
            .prefix_extractor = prefix_extractor,
            .bloom_config = bloom_config,
        });
        self.encoder_active = true;
        self.observeBuilderWorkingSet(true);
    }

    pub fn deinit(self: *StreamingRunFileWriter) void {
        self.releaseBuilderWorkingSet();
        if (self.encoder_active) {
            self.encoder.deinit();
            self.encoder_active = false;
        }
        if (self.adapter_active) {
            self.adapter.deinit();
            self.adapter_active = false;
        }
        if (self.writer_active) {
            self.writer.abort();
            self.writer_active = false;
        }
        if (self.path.len > 0) {
            self.allocator.free(self.path);
            self.path = &.{};
        }
        self.* = undefined;
    }

    pub fn appendEntry(self: *StreamingRunFileWriter, entry: lsm_table_file.Entry) !void {
        try self.encoder.appendEntry(entry);
        self.observeBuilderWorkingSet(false);
    }

    pub fn canAppendEntry(self: *const StreamingRunFileWriter, entry: lsm_table_file.Entry) bool {
        return (self.encoder.encodedSizeUpperBoundAfterEntry(entry) catch return false) <= self.max_file_bytes;
    }

    pub fn finish(self: *StreamingRunFileWriter) !PersistedStreamingRunFile {
        self.observeBuilderWorkingSet(true);
        var encoded = try self.encoder.finish();
        errdefer encoded.filter.deinit(self.allocator);
        if (encoded.size_bytes > self.max_file_bytes) return error.TableFileTooLarge;
        self.observeBuilderWorkingSet(true);
        self.encoder_active = false;
        self.encoder.deinit();
        self.observeBuilderWorkingSet(true);
        try self.adapter.flush();
        self.writer_active = false;
        try self.writer.finish();
        self.adapter.deinit();
        self.adapter_active = false;
        self.releaseBuilderWorkingSet();

        const path = self.path;
        self.path = &.{};
        return .{
            .path = path,
            .size_bytes = @intCast(encoded.size_bytes),
            .entry_count = encoded.entry_count,
            .compression_stats = encoded.compression_stats,
            .filter = encoded.filter,
        };
    }

    fn observeBuilderWorkingSet(self: *StreamingRunFileWriter, force: bool) void {
        if (self.resource_manager == null) return;
        const next = self.builderWorkingSetBytes();
        const grew_enough = next >= self.last_reported_builder_bytes +| table_builder_accounting_step_bytes;
        const shrank_enough = self.last_reported_builder_bytes >= next +| table_builder_accounting_step_bytes;
        if (!force and !grew_enough and !shrank_enough) return;
        self.observeTrackedBuilderBytes(next);
        self.last_reported_builder_bytes = next;
    }

    fn builderWorkingSetBytes(self: *const StreamingRunFileWriter) u64 {
        var bytes: u64 = 0;
        if (self.adapter_active) bytes +|= self.adapter.buffer.len;
        if (self.encoder_active) bytes +|= self.encoder.workingSetBytes();
        return bytes;
    }

    fn releaseBuilderWorkingSet(self: *StreamingRunFileWriter) void {
        self.observeTrackedBuilderBytes(0);
        self.last_reported_builder_bytes = 0;
    }

    fn observeTrackedBuilderBytes(self: *StreamingRunFileWriter, next: u64) void {
        const manager = self.resource_manager orelse return;
        manager.observeUsage(.lsm_table_builder_working_set, &self.tracked_builder_bytes, next);
    }
};

const WrittenTableFile = struct {
    size_bytes: u64,
    compression_stats: lsm_table_file.CompressionStats,
};

test "repository manifest persist omits run bloom filters" {
    const allocator = std.testing.allocator;
    var storage = storage_io.MemoryStorage.init(allocator);
    defer storage.deinit();

    const entries = [_]lsm_table_file.Entry{
        .{ .namespace_name = "docs", .key = "doc:a", .value = "A", .tombstone = false },
    };
    const filter = try lsm_table_file.buildFilterAlloc(allocator, &entries, .{});

    var run = Run{
        .id = 1,
        .level = 0,
        .size_bytes = 4096,
        .path = try allocator.dupe(u8, "/repository-manifest-small/runs/1.tbl"),
        .smallest_namespace_name = try allocator.dupe(u8, "docs"),
        .smallest_key = try allocator.dupe(u8, "doc:a"),
        .largest_namespace_name = try allocator.dupe(u8, "docs"),
        .largest_key = try allocator.dupe(u8, "doc:z"),
        .entry_count = 128,
        .bloom_filter = filter,
        .state = null,
    };
    defer run.deinit(allocator);

    var runs = [_]Run{run};
    const written = try persistManifestWithStorageCount(storage.storage(), allocator, "/repository-manifest-small", 2, &runs, &.{});
    try std.testing.expect(written < 512);

    const manifest_path = try joinPath(allocator, "/repository-manifest-small", "manifest.bin");
    defer allocator.free(manifest_path);
    const raw = try storage.storage().readFileAlloc(allocator, manifest_path, 1024);
    defer allocator.free(raw);
    try std.testing.expect(raw.len < 512);
}

test "repository run table index load surfaces missing run file" {
    const allocator = std.testing.allocator;
    var storage = storage_io.MemoryStorage.init(allocator);
    defer storage.deinit();

    const missing_path = try runPath(allocator, "/repository-missing-run", 7);
    defer allocator.free(missing_path);

    try std.testing.expectError(
        error.FileNotFound,
        loadRunTableIndexAllocWithStorage(storage.storage(), allocator, missing_path),
    );
}

test "repository refuses to publish a streaming run above the reader cap" {
    const allocator = std.testing.allocator;
    var storage = storage_io.MemoryStorage.init(allocator);
    defer storage.deinit();

    const root_dir = "/repository-streaming-run-reader-cap";
    var writer: StreamingRunFileWriter = undefined;
    try writer.initInPlace(
        storage.storage(),
        allocator,
        root_dir,
        1,
        1,
        32,
        .{},
        .none,
        .none,
        null,
        .cold_sequential,
    );
    var writer_active = true;
    defer if (writer_active) writer.deinit();

    const entry = lsm_table_file.Entry{
        .namespace_name = "docs",
        .key = "doc:a",
        .value = "alpha",
    };
    try std.testing.expect(!writer.canAppendEntry(entry));
    try writer.appendEntry(entry);
    try std.testing.expectError(error.TableFileTooLarge, writer.finish());
    writer.deinit();
    writer_active = false;

    const path = try runPath(allocator, root_dir, 1);
    defer allocator.free(path);
    try std.testing.expectError(error.FileNotFound, storage.storage().readFileAlloc(allocator, path, 1024));
}

test "repository streaming size admission bounds metadata-heavy runs" {
    const allocator = std.testing.allocator;
    var storage = storage_io.MemoryStorage.init(allocator);
    defer storage.deinit();

    const max_file_bytes = 1024;
    var writer: StreamingRunFileWriter = undefined;
    try writer.initInPlace(
        storage.storage(),
        allocator,
        "/repository-streaming-run-size-admission",
        1,
        128,
        max_file_bytes,
        .{},
        .none,
        .first_separator,
        null,
        .normal,
    );
    var writer_active = true;
    defer if (writer_active) writer.deinit();

    var admitted: usize = 0;
    var key_buf: [32]u8 = undefined;
    for (0..128) |i| {
        const key = try std.fmt.bufPrint(&key_buf, "tenant:{d:0>4}", .{i});
        const entry = lsm_table_file.Entry{ .namespace_name = "docs", .key = key, .value = "v" };
        if (!writer.canAppendEntry(entry)) break;
        try writer.appendEntry(entry);
        admitted += 1;
    }
    try std.testing.expect(admitted > 0);
    try std.testing.expect(admitted < 128);

    var persisted = try writer.finish();
    writer_active = false;
    defer {
        allocator.free(persisted.path);
        persisted.filter.deinit(allocator);
    }
    try std.testing.expect(persisted.size_bytes <= max_file_bytes);
    var index = try loadRunTableIndexAllocWithStorage(storage.storage(), allocator, persisted.path);
    defer index.deinit(allocator);
    try std.testing.expectEqual(admitted, index.entryCount());
}

test "repository streaming size admission remains exact across completed blocks" {
    const allocator = std.testing.allocator;
    var storage = storage_io.MemoryStorage.init(allocator);
    defer storage.deinit();

    const max_file_bytes = 96 * 1024;
    const entry_count = 20;
    var writer: StreamingRunFileWriter = undefined;
    try writer.initInPlace(
        storage.storage(),
        allocator,
        "/repository-streaming-run-multi-block-admission",
        1,
        entry_count,
        max_file_bytes,
        .{},
        .none,
        .none,
        null,
        .normal,
    );
    var writer_active = true;
    defer if (writer_active) writer.deinit();

    var value: [4096]u8 = undefined;
    @memset(&value, 'v');
    var key_buf: [32]u8 = undefined;
    for (0..entry_count) |i| {
        const key = try std.fmt.bufPrint(&key_buf, "doc:{d:0>4}", .{i});
        const entry = lsm_table_file.Entry{ .namespace_name = "docs", .key = key, .value = &value };
        try std.testing.expect(writer.canAppendEntry(entry));
        try writer.appendEntry(entry);
    }

    var persisted = try writer.finish();
    writer_active = false;
    defer {
        allocator.free(persisted.path);
        persisted.filter.deinit(allocator);
    }
    try std.testing.expect(persisted.size_bytes <= max_file_bytes);
    var index = try loadRunTableIndexAllocWithStorage(storage.storage(), allocator, persisted.path);
    defer index.deinit(allocator);
    try std.testing.expect(index.blocks.len > 1);
    try std.testing.expectEqual(@as(usize, entry_count), index.entryCount());
}

test "repository rejects forged run metadata length before allocating" {
    const allocator = std.testing.allocator;
    var storage = storage_io.MemoryStorage.init(allocator);
    defer storage.deinit();

    const entries = [_]lsm_table_file.Entry{
        .{ .namespace_name = "docs", .key = "doc:a", .value = "A" },
    };
    const encoded = try lsm_table_file.encodeAlloc(allocator, &entries);
    defer allocator.free(encoded);

    const footer_offset = encoded.len - lsm_table_file.footer_len;
    const footer = encoded[footer_offset..];
    std.mem.writeInt(u64, footer[24..32], max_run_file_read_bytes, .little);
    std.mem.writeInt(u32, footer[44..48], Crc32.hash(footer[0..44]), .little);

    const path = "/repository-forged-run-metadata/run.tbl";
    try storage.storage().writeFileAbsolute(path, encoded);
    try std.testing.expectError(
        error.InvalidTableFile,
        loadRunTableIndexAllocWithStorage(storage.storage(), allocator, path),
    );
}

test "repository streams state run publication through table builder accounting" {
    const allocator = std.testing.allocator;
    var storage = storage_io.MemoryStorage.init(allocator);
    defer storage.deinit();
    var manager = resource_manager_mod.ResourceManager.init(.{});

    var value: [512]u8 = undefined;
    @memset(&value, 'v');

    var state: state_mod.State = .{};
    errdefer state.deinit(allocator);
    try state.entries.ensureTotalCapacity(allocator, 64);
    for (0..64) |i| {
        var key_buf: [32]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "doc:{d:0>4}", .{i});
        state.entries.appendAssumeCapacity(try state_mod.initEntry(
            allocator,
            .{ .name = "docs" },
            key,
            &value,
            false,
        ));
    }

    var run = Run{
        .id = 1,
        .level = 0,
        .size_bytes = 0,
        .path = null,
        .smallest_namespace_name = @constCast("docs"),
        .smallest_key = @constCast("doc:0000"),
        .largest_namespace_name = @constCast("docs"),
        .largest_key = @constCast("doc:0063"),
        .entry_count = 64,
        .bloom_filter = null,
        .owns_metadata = false,
        .owns_bloom_filter = true,
        .state = state,
    };
    state = .{};
    defer {
        if (run.path) |path| {
            allocator.free(path);
            run.path = null;
        }
        run.deinit(allocator);
    }

    run.path = try persistRunFileWithStorageAccounted(
        storage.storage(),
        allocator,
        "/repository-stream-state-run",
        &run,
        .snappy_adaptive,
        .none,
        &manager,
    );

    const builder_stats = manager.sliceStats(.lsm_table_builder_working_set);
    const compaction_work_stats = manager.sliceStats(.lsm_compaction_work);
    try std.testing.expectEqual(@as(u64, 0), builder_stats.used_bytes);
    try std.testing.expect(builder_stats.peak_bytes >= table_write_buffer_size);
    try std.testing.expectEqual(@as(u64, 0), compaction_work_stats.peak_bytes);
    try std.testing.expect(run.size_bytes > 0);
    try std.testing.expect(run.bloom_filter != null);

    var index = try loadRunTableIndexAllocWithStorage(storage.storage(), allocator, run.path.?);
    defer index.deinit(allocator);
    try std.testing.expectEqual(lsm_table_file.PrefixExtractor.none, index.prefix_extractor);

    var loaded = try loadRunStateAllocWithStorage(storage.storage(), allocator, run.path.?);
    defer loaded.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 64), loaded.entries.items.len);
}

test "repository table builder peak stays below whole-run logical payload" {
    const allocator = std.testing.allocator;
    var storage = storage_io.MemoryStorage.init(allocator);
    defer storage.deinit();
    var manager = resource_manager_mod.ResourceManager.init(.{});

    var value: [2048]u8 = undefined;
    @memset(&value, 'v');

    var state: state_mod.State = .{};
    errdefer state.deinit(allocator);
    const entry_count: usize = 1024;
    try state.entries.ensureTotalCapacity(allocator, entry_count);
    var logical_payload_bytes: u64 = 0;
    for (0..entry_count) |i| {
        var key_buf: [32]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "doc:{d:0>6}", .{i});
        logical_payload_bytes +|= key.len + value.len;
        state.entries.appendAssumeCapacity(try state_mod.initEntry(
            allocator,
            .{ .name = "docs" },
            key,
            &value,
            false,
        ));
    }

    var run = Run{
        .id = 2,
        .level = 0,
        .size_bytes = 0,
        .path = null,
        .smallest_namespace_name = @constCast("docs"),
        .smallest_key = @constCast("doc:000000"),
        .largest_namespace_name = @constCast("docs"),
        .largest_key = @constCast("doc:001023"),
        .entry_count = @intCast(entry_count),
        .bloom_filter = null,
        .owns_metadata = false,
        .owns_bloom_filter = true,
        .state = state,
    };
    state = .{};
    defer {
        if (run.path) |path| {
            allocator.free(path);
            run.path = null;
        }
        run.deinit(allocator);
    }

    run.path = try persistRunFileWithStorageAccounted(
        storage.storage(),
        allocator,
        "/repository-stream-large-state-run",
        &run,
        .none,
        .none,
        &manager,
    );

    const builder_stats = manager.sliceStats(.lsm_table_builder_working_set);
    try std.testing.expectEqual(@as(u64, 0), builder_stats.used_bytes);
    try std.testing.expect(builder_stats.peak_bytes >= table_write_buffer_size);
    try std.testing.expect(builder_stats.peak_bytes < logical_payload_bytes / 2);
    try std.testing.expect(run.size_bytes > logical_payload_bytes);
}
