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
const bloom = @import("bloom");
const byte_copy = @import("../../common/byte_copy.zig");
const lsm_manifest = @import("../lsm/manifest.zig");
const lsm_table_file = @import("../lsm/table_file.zig");
const resource_manager_mod = @import("../resource_manager.zig");
const state_mod = @import("state.zig");
const storage_io = @import("storage_io.zig");

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

pub const Run = struct {
    id: u64,
    /// Logical newest-write precedence for L0. Physical rewrites allocate a
    /// fresh id but retain the newest input sequence so tiered merges cannot
    /// make older values shadow newer unmerged runs. Zero means `id` for
    /// in-memory/legacy callers.
    l0_sequence: u64 = 0,
    level: u32,
    size_bytes: u64,
    compression_stats: lsm_table_file.CompressionStats = .{},
    path: ?[]u8,
    smallest_namespace_name: ?[]u8,
    smallest_key: []u8,
    largest_namespace_name: ?[]u8,
    largest_key: []u8,
    entry_count: u32,
    bloom_filter: ?bloom.OwnedFilter,
    owns_metadata: bool = true,
    owns_path: bool = false,
    owns_bloom_filter: bool = true,
    cached_state_index: ?usize = null,
    cached_index_index: ?usize = null,
    cached_table_index: ?usize = null,
    table_index: ?lsm_table_file.TableIndex = null,
    version_ref_pinned: bool = false,
    state: ?state_mod.State,

    pub fn deinit(self: *Run, allocator: Allocator) void {
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
    const smallest_namespace_name = if (source.smallest_namespace_name) |name| try allocator.dupe(u8, name) else null;
    errdefer if (smallest_namespace_name) |name| allocator.free(name);
    const smallest_key = try allocator.dupe(u8, source.smallest_key);
    errdefer allocator.free(smallest_key);
    const largest_namespace_name = if (source.largest_namespace_name) |name| try allocator.dupe(u8, name) else null;
    errdefer if (largest_namespace_name) |name| allocator.free(name);
    const largest_key = try allocator.dupe(u8, source.largest_key);
    errdefer allocator.free(largest_key);

    var out = Run{
        .id = source.id,
        .l0_sequence = source.l0_sequence,
        .level = source.level,
        .size_bytes = source.size_bytes,
        .compression_stats = source.compression_stats,
        .path = if (source.path) |path| try allocator.dupe(u8, path) else null,
        .smallest_namespace_name = smallest_namespace_name,
        .smallest_key = smallest_key,
        .largest_namespace_name = largest_namespace_name,
        .largest_key = largest_key,
        .entry_count = source.entry_count,
        .bloom_filter = if (source.bloom_filter) |filter| try filter.clone(allocator) else null,
        .cached_state_index = null,
        .cached_index_index = null,
        .cached_table_index = null,
        .table_index = null,
        .state = null,
    };
    errdefer out.deinit(allocator);

    if (source.path == null) {
        const state = source.state orelse return error.RunStateUnavailable;
        out.state = try state.clone(allocator);
    }
    return out;
}

pub fn cloneRunCompactionSnapshot(allocator: Allocator, source: Run) !Run {
    const smallest_namespace_name = if (source.smallest_namespace_name) |name| try allocator.dupe(u8, name) else null;
    errdefer if (smallest_namespace_name) |name| allocator.free(name);
    const smallest_key = try allocator.dupe(u8, source.smallest_key);
    errdefer allocator.free(smallest_key);
    const largest_namespace_name = if (source.largest_namespace_name) |name| try allocator.dupe(u8, name) else null;
    errdefer if (largest_namespace_name) |name| allocator.free(name);
    const largest_key = try allocator.dupe(u8, source.largest_key);
    errdefer allocator.free(largest_key);

    var out = Run{
        .id = source.id,
        .l0_sequence = source.l0_sequence,
        .level = source.level,
        .size_bytes = source.size_bytes,
        .compression_stats = source.compression_stats,
        .path = if (source.path) |path| try allocator.dupe(u8, path) else null,
        .smallest_namespace_name = smallest_namespace_name,
        .smallest_key = smallest_key,
        .largest_namespace_name = largest_namespace_name,
        .largest_key = largest_key,
        .entry_count = source.entry_count,
        .bloom_filter = null,
        .owns_bloom_filter = false,
        .cached_state_index = null,
        .cached_index_index = null,
        .cached_table_index = null,
        .table_index = null,
        .state = null,
    };
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
    obsolete_paths: *std.ArrayListUnmanaged(ObsoletePath),
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
    obsolete_paths: *std.ArrayListUnmanaged(ObsoletePath),
) !bool {
    const manifest_path = try joinPath(allocator, root_dir, "manifest.bin");
    defer allocator.free(manifest_path);

    const raw_manifest = storage.readFileAlloc(allocator, manifest_path, max_manifest_read_bytes) catch |err| switch (err) {
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

    next_run_id.* = decoded.next_run_id;
    try runs.ensureTotalCapacity(allocator, decoded.runs.len);
    for (decoded.runs) |*meta| {
        const owned_path = try runPath(allocator, root_dir, meta.id);
        var path_owned = true;
        errdefer if (path_owned) allocator.free(owned_path);
        try runs.append(allocator, .{
            .id = meta.id,
            .l0_sequence = meta.l0_sequence,
            .level = meta.level,
            .size_bytes = meta.size_bytes,
            .compression_stats = meta.compression_stats,
            .path = owned_path,
            .smallest_namespace_name = if (meta.smallest_namespace_name) |name| @constCast(name) else null,
            .smallest_key = @constCast(meta.smallest_key),
            .largest_namespace_name = if (meta.largest_namespace_name) |name| @constCast(name) else null,
            .largest_key = @constCast(meta.largest_key),
            .entry_count = meta.entry_count,
            .bloom_filter = null,
            .owns_metadata = false,
            .owns_path = true,
            .state = null,
        });
        path_owned = false;
    }
    try obsolete_paths.ensureTotalCapacity(allocator, decoded.obsolete_paths.len);
    for (decoded.obsolete_paths) |obsolete| {
        const owned_path = try rebaseManifestPathAlloc(allocator, root_dir, obsolete.path);
        obsolete_paths.appendAssumeCapacity(.{
            .path = owned_path,
            .delete_after_ns = obsolete.delete_after_ns,
        });
    }
    manifest_backing.* = decoded.raw;
    decoded.raw = &.{};
    return true;
}

fn rebaseManifestPathAlloc(allocator: Allocator, root_dir: []const u8, path: []const u8) ![]u8 {
    if (!std.fs.path.isAbsolute(path)) return try std.fs.path.join(allocator, &.{ root_dir, path });
    const parent = std.fs.path.dirname(path) orelse return try allocator.dupe(u8, path);
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
        state.entries.items.len,
        @min(max_file_bytes, max_run_file_read_bytes),
        bloom_config,
        compression_policy,
        prefix_extractor,
        resource_manager,
        cache_intent,
    );
    var writer_active = true;
    errdefer if (writer_active) writer.deinit();

    for (state.entries.items) |entry| {
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
    var table_entries = try allocator.alloc(lsm_table_file.Entry, state.entries.items.len);
    defer allocator.free(table_entries);
    for (state.entries.items, 0..) |entry, i| {
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

    var metas = try allocator.alloc(lsm_manifest.RunMeta, runs.len);
    defer allocator.free(metas);
    for (runs, 0..) |run, i| {
        metas[i] = .{
            .id = run.id,
            .l0_sequence = if (run.l0_sequence != 0) run.l0_sequence else run.id,
            .level = run.level,
            .size_bytes = run.size_bytes,
            .compression_stats = run.compression_stats,
            .path = run.path.?,
            .smallest_namespace_name = run.smallest_namespace_name,
            .smallest_key = run.smallest_key,
            .largest_namespace_name = run.largest_namespace_name,
            .largest_key = run.largest_key,
            .entry_count = run.entry_count,
        };
    }

    var obsolete_metas = try allocator.alloc(lsm_manifest.ObsoletePathMeta, obsolete_paths.len);
    defer allocator.free(obsolete_metas);
    for (obsolete_paths, 0..) |obsolete, i| {
        obsolete_metas[i] = .{
            .path = obsolete.path,
            .delete_after_ns = obsolete.delete_after_ns,
        };
    }

    const encoded = try lsm_manifest.encodeAlloc(allocator, .{
        .next_run_id = next_run_id,
        .runs = metas,
        .obsolete_paths = obsolete_metas,
    });
    defer allocator.free(encoded);
    try replaceFileAtomicallyAbsolute(storage, manifest_path, encoded);
    return @intCast(encoded.len);
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

fn loadRunFooterWithStorage(
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
    std.mem.writeInt(u32, footer[44..48], std.hash.Crc32.hash(footer[0..44]), .little);

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
