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
const lsm_manifest = @import("../lsm/manifest.zig");
const lsm_table_file = @import("../lsm/table_file.zig");
const state_mod = @import("state.zig");
const storage_io = @import("storage_io.zig");

const max_run_file_read_bytes = 512 * 1024 * 1024;
const max_manifest_read_bytes = 128 * 1024 * 1024;
const table_write_buffer_size = 256 * 1024;

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
    encoded_bloom_filter: ?[]u8,
    owns_metadata: bool = true,
    owns_bloom_filter: bool = true,
    cached_state_index: ?usize = null,
    cached_index_index: ?usize = null,
    cached_table_index: ?usize = null,
    table_index: ?lsm_table_file.TableIndex = null,
    state: ?state_mod.State,

    pub fn deinit(self: *Run, allocator: Allocator) void {
        if (self.owns_metadata) {
            if (self.path) |path| allocator.free(path);
            if (self.smallest_namespace_name) |name| allocator.free(name);
            allocator.free(self.smallest_key);
            if (self.largest_namespace_name) |name| allocator.free(name);
            allocator.free(self.largest_key);
            if (self.encoded_bloom_filter) |raw| allocator.free(raw);
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
            .encoded_bloom_filter = null,
            .owns_metadata = false,
            .owns_bloom_filter = false,
            .cached_state_index = null,
            .cached_index_index = null,
            .cached_table_index = null,
            .table_index = null,
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
        const encoded = self.encoded_bloom_filter orelse return error.RunBloomFilterUnavailable;
        self.bloom_filter = try bloom.OwnedFilter.decodeAlloc(allocator, encoded);
        self.owns_bloom_filter = true;
        return self.bloom_filter.?;
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
        .encoded_bloom_filter = if (source.encoded_bloom_filter) |encoded| try allocator.dupe(u8, encoded) else null,
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
    var decoded = try lsm_manifest.decodeBorrowedOwnedAlloc(allocator, raw_manifest);
    defer decoded.deinit(allocator);

    next_run_id.* = decoded.next_run_id;
    try runs.ensureTotalCapacity(allocator, decoded.runs.len);
    for (decoded.runs) |*meta| {
        if (meta.bloom_filter.len == 0) return error.MissingRunBloomFilter;

        try runs.append(allocator, .{
            .id = meta.id,
            .level = meta.level,
            .size_bytes = meta.size_bytes,
            .compression_stats = meta.compression_stats,
            .path = @constCast(meta.path),
            .smallest_namespace_name = if (meta.smallest_namespace_name) |name| @constCast(name) else null,
            .smallest_key = @constCast(meta.smallest_key),
            .largest_namespace_name = if (meta.largest_namespace_name) |name| @constCast(name) else null,
            .largest_key = @constCast(meta.largest_key),
            .entry_count = meta.entry_count,
            .bloom_filter = null,
            .encoded_bloom_filter = @constCast(meta.bloom_filter),
            .owns_metadata = false,
            .state = null,
        });
    }
    try obsolete_paths.ensureTotalCapacity(allocator, decoded.obsolete_paths.len);
    for (decoded.obsolete_paths) |obsolete| {
        const owned_path = try allocator.dupe(u8, obsolete.path);
        obsolete_paths.appendAssumeCapacity(.{
            .path = owned_path,
            .delete_after_ns = obsolete.delete_after_ns,
        });
    }
    manifest_backing.* = decoded.raw;
    decoded.raw = &.{};
    return true;
}

pub fn persistRunFile(allocator: Allocator, root_dir: []const u8, run: *Run) ![]u8 {
    var native = try storage_io.NativeStorage.init(std.heap.page_allocator, .threaded);
    defer native.deinit();
    return try persistRunFileWithStorage(native.storage(), allocator, root_dir, run, .snappy_adaptive);
}

pub fn persistRunFileWithStorage(storage: storage_io.Storage, allocator: Allocator, root_dir: []const u8, run: *Run, compression_policy: lsm_table_file.CompressionPolicy) ![]u8 {
    try ensureOpenDirsWithStorage(storage, root_dir);
    const run_path = try runPath(allocator, root_dir, run.id);
    errdefer allocator.free(run_path);

    const state = run.state orelse return error.RunStateUnavailable;
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

    const written = try writeTableFileAtomically(storage, allocator, run_path, table_entries, try run.ensureBloomFilter(allocator), compression_policy);
    run.size_bytes = written.size_bytes;
    run.compression_stats = written.compression_stats;
    return run_path;
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
        const encoded_filter = if (run.encoded_bloom_filter) |raw|
            try allocator.dupe(u8, raw)
        else
            try run.bloom_filter.?.encodeAlloc(allocator);
        errdefer allocator.free(encoded_filter);
        metas[i] = .{
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
            .bloom_filter = encoded_filter,
        };
    }
    defer for (metas) |meta| allocator.free(meta.bloom_filter);

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

    if (index.blockCount() > 0) {
        for (index.blocks, 0..) |block, block_index| {
            const window = index.blockWindow(block_index);
            const payload = try storage.readFileRangeAlloc(
                allocator,
                path,
                @as(u64, @intCast(index.entry_data_start)) + window.physicalRelativeOffset(),
                window.physicalLen(),
            );
            defer allocator.free(payload);
            const bytes = try lsm_table_file.decodeBlockPayloadAlloc(allocator, window.compression, payload, window.len);
            defer allocator.free(bytes);

            const end = block.first_entry_index + block.entry_count;
            for (block.first_entry_index..end) |entry_index| {
                const relative_offset: usize = @intCast(index.entryStart(entry_index) - window.relative_offset);
                const entry = try lsm_table_file.parseEntryAt(bytes, relative_offset);
                try appendStateEntryClone(allocator, &state, entry);
            }
        }
        return state;
    }

    var loaded_window: ?lsm_table_file.EntryDataWindow = null;
    var loaded_bytes: ?[]u8 = null;
    defer if (loaded_bytes) |bytes| allocator.free(bytes);

    for (0..index.entryCount()) |entry_index| {
        const window = index.entryDataWindow(entry_index, lsm_table_file.default_block_size);
        if (loaded_window == null or
            loaded_window.?.relative_offset != window.relative_offset or
            loaded_window.?.len != window.len)
        {
            if (loaded_bytes) |bytes| allocator.free(bytes);
            const payload = try storage.readFileRangeAlloc(
                allocator,
                path,
                @as(u64, @intCast(index.entry_data_start)) + window.physicalRelativeOffset(),
                window.physicalLen(),
            );
            defer allocator.free(payload);
            loaded_bytes = try lsm_table_file.decodeBlockPayloadAlloc(allocator, window.compression, payload, window.len);
            loaded_window = window;
        }
        const relative_offset: usize = @intCast(index.entryStart(entry_index) - window.relative_offset);
        const entry = try lsm_table_file.parseEntryAt(loaded_bytes.?, relative_offset);
        try appendStateEntryClone(allocator, &state, entry);
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
    const footer_bytes = storage.readFileTrailerAlloc(allocator, path, lsm_table_file.footer_len) catch |err| switch (err) {
        error.EndOfStream, error.FileNotFound => null,
        else => return err,
    };
    defer if (footer_bytes) |bytes| allocator.free(bytes);

    if (footer_bytes) |bytes| {
        if (lsm_table_file.hasFooterMagic(bytes)) {
            const footer = try lsm_table_file.decodeFooterBytes(bytes);
            const metadata_bytes = try storage.readFileRangeAlloc(allocator, path, footer.metadata_offset, footer.metadata_len);
            defer allocator.free(metadata_bytes);
            return try lsm_table_file.decodeIndexFromFooterAlloc(allocator, footer, metadata_bytes);
        }
    }

    const header_bytes = try storage.readFileRangeAlloc(allocator, path, 0, lsm_table_file.magic.len + 12);
    defer allocator.free(header_bytes);

    var cursor: usize = 0;
    const header = try lsm_table_file.decodeHeader(header_bytes, &cursor);
    if (header.version != 3) {
        const raw_table = storage.readFileAlloc(allocator, path, max_run_file_read_bytes) catch |err| {
            logReadFileFailure(storage, path, max_run_file_read_bytes, "loadRunTableIndexAllocWithStorage.legacy", err);
            return err;
        };
        defer allocator.free(raw_table);
        return try lsm_table_file.decodeIndexAlloc(allocator, raw_table);
    }

    const offsets_offset: u64 = @intCast(header.entry_offsets_start);
    const offsets_len = header.entry_count * @sizeOf(u32);
    const offsets_bytes = try storage.readFileRangeAlloc(allocator, path, offsets_offset, offsets_len);
    defer allocator.free(offsets_bytes);

    const offsets = try allocator.alloc(u32, header.entry_count);
    errdefer allocator.free(offsets);
    for (offsets, 0..) |*offset, i| {
        const start = i * @sizeOf(u32);
        offset.* = std.mem.readInt(u32, std.mem.bytesAsValue([4]u8, offsets_bytes[start .. start + @sizeOf(u32)]), .little);
    }

    const bloom_len_offset = header.entry_data_start + header.entry_data_len;
    const file_size = try storage.fileSize(path);
    const bloom_bytes_offset = bloom_len_offset + @sizeOf(u32);
    if (file_size < bloom_bytes_offset) return error.InvalidTableFile;
    const bloom_len: usize = @intCast(file_size - bloom_bytes_offset);
    const encoded_filter = try storage.readFileRangeAlloc(allocator, path, bloom_bytes_offset, bloom_len);
    defer allocator.free(encoded_filter);

    var filter = try bloom.OwnedFilter.decodeAlloc(allocator, encoded_filter);
    errdefer filter.deinit(allocator);

    return .{
        .entry_offsets = offsets,
        .entry_data_start = header.entry_data_start,
        .entry_data_len = header.entry_data_len,
        .filter = filter,
    };
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
            @memcpy(self.buffer[self.len_buffered..][0..n], remaining[0..n]);
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
            @memcpy(self.buffer[buffer_offset..][0 .. bytes.len - src_offset], bytes[src_offset..]);
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
    const tmp_path = try tempSiblingPath(std.heap.page_allocator, path);
    defer std.heap.page_allocator.free(tmp_path);
    writeFileAbsoluteWithStorage(storage, tmp_path, contents) catch |err| {
        if (!isInjectedStorageFault(err)) {
            std.log.err("lsm replace write failed path={s} tmp_path={s} bytes={} err={s}", .{ path, tmp_path, contents.len, @errorName(err) });
        }
        deleteFileAbsoluteWithStorage(storage, tmp_path) catch {};
        return err;
    };

    storage.renameAbsolute(tmp_path, path) catch |err| {
        if (err == error.FileNotFound) {
            writeFileAbsoluteWithStorage(storage, tmp_path, contents) catch |rewrite_err| {
                if (!isInjectedStorageFault(rewrite_err)) {
                    std.log.err("lsm replace rewrite failed path={s} tmp_path={s} bytes={} err={s}", .{ path, tmp_path, contents.len, @errorName(rewrite_err) });
                }
                deleteFileAbsoluteWithStorage(storage, tmp_path) catch {};
                return rewrite_err;
            };
            storage.renameAbsolute(tmp_path, path) catch |retry_err| {
                if (!isInjectedStorageFault(retry_err)) {
                    std.log.err("lsm replace rename retry failed path={s} tmp_path={s} bytes={} err={s}", .{ path, tmp_path, contents.len, @errorName(retry_err) });
                }
                deleteFileAbsoluteWithStorage(storage, tmp_path) catch {};
                return retry_err;
            };
            return;
        }
        if (!isInjectedStorageFault(err)) {
            std.log.err("lsm replace rename failed path={s} tmp_path={s} bytes={} err={s}", .{ path, tmp_path, contents.len, @errorName(err) });
        }
        deleteFileAbsoluteWithStorage(storage, tmp_path) catch {};
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

    pub fn initInPlace(
        self: *StreamingRunFileWriter,
        storage: storage_io.Storage,
        allocator: Allocator,
        root_dir: []const u8,
        run_id: u64,
        expected_entries: usize,
        bloom_config: bloom.Config,
        compression_policy: lsm_table_file.CompressionPolicy,
    ) !void {
        self.* = .{ .allocator = allocator };
        try ensureOpenDirsWithStorage(storage, root_dir);
        self.path = try runPath(allocator, root_dir, run_id);
        errdefer {
            allocator.free(self.path);
            self.path = &.{};
        }

        self.writer = try storage.beginAtomicWrite(allocator, self.path);
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
            .bloom_config = bloom_config,
        });
        self.encoder_active = true;
    }

    pub fn deinit(self: *StreamingRunFileWriter) void {
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
    }

    pub fn finish(self: *StreamingRunFileWriter) !PersistedStreamingRunFile {
        var encoded = try self.encoder.finish();
        errdefer encoded.filter.deinit(self.allocator);
        self.encoder_active = false;
        self.encoder.deinit();
        try self.adapter.flush();
        self.writer_active = false;
        try self.writer.finish();
        self.adapter.deinit();
        self.adapter_active = false;

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
};

const WrittenTableFile = struct {
    size_bytes: u64,
    compression_stats: lsm_table_file.CompressionStats,
};
