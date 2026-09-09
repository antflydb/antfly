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
const lsm_table_file = @import("table_file.zig");

pub const magic = "ALSMMAN1";
pub const version: u32 = 10;
pub const journal_magic = "ALSMJNL1";
const journal_header_len = 24;
const checksum_len: usize = @sizeOf(u32);

pub const RunMeta = struct {
    id: u64,
    level: u32,
    size_bytes: u64,
    compression_stats: lsm_table_file.CompressionStats = .{},
    path: []const u8,
    smallest_namespace_name: ?[]const u8,
    smallest_key: []const u8,
    largest_namespace_name: ?[]const u8,
    largest_key: []const u8,
    entry_count: u32,
    tombstone_count: ?u32 = null,
    oldest_tombstone_unix_ns: u64 = 0,
    visibility_id: u64 = 0,
    gc_requested: bool = false,
};

pub const ObsoletePathMeta = struct {
    path: []const u8,
    delete_after_ns: u64,
};

pub const OwnedRunMeta = struct {
    id: u64,
    level: u32,
    size_bytes: u64,
    compression_stats: lsm_table_file.CompressionStats = .{},
    path: []u8,
    smallest_namespace_name: ?[]u8,
    smallest_key: []u8,
    largest_namespace_name: ?[]u8,
    largest_key: []u8,
    entry_count: u32,
    tombstone_count: ?u32 = null,
    oldest_tombstone_unix_ns: u64 = 0,
    visibility_id: u64 = 0,
    gc_requested: bool = false,

    pub fn deinit(self: *OwnedRunMeta, allocator: std.mem.Allocator) void {
        allocator.free(self.path);
        if (self.smallest_namespace_name) |name| allocator.free(name);
        allocator.free(self.smallest_key);
        if (self.largest_namespace_name) |name| allocator.free(name);
        allocator.free(self.largest_key);
        self.* = undefined;
    }
};

pub const OwnedObsoletePathMeta = struct {
    path: []u8,
    delete_after_ns: u64,

    pub fn deinit(self: *OwnedObsoletePathMeta, allocator: std.mem.Allocator) void {
        allocator.free(self.path);
        self.* = undefined;
    }
};

pub const Manifest = struct {
    next_run_id: u64,
    runs: []const RunMeta,
    obsolete_paths: []const ObsoletePathMeta = &.{},
};

pub const OwnedManifest = struct {
    next_run_id: u64,
    runs: []OwnedRunMeta,
    obsolete_paths: []OwnedObsoletePathMeta,

    pub fn deinit(self: *OwnedManifest, allocator: std.mem.Allocator) void {
        for (self.runs) |*run| run.deinit(allocator);
        allocator.free(self.runs);
        for (self.obsolete_paths) |*obsolete| obsolete.deinit(allocator);
        allocator.free(self.obsolete_paths);
        self.* = undefined;
    }
};

pub const BorrowedRunMeta = struct {
    id: u64,
    level: u32,
    size_bytes: u64,
    compression_stats: lsm_table_file.CompressionStats = .{},
    path: []const u8,
    smallest_namespace_name: ?[]const u8,
    smallest_key: []const u8,
    largest_namespace_name: ?[]const u8,
    largest_key: []const u8,
    entry_count: u32,
    tombstone_count: ?u32 = null,
    oldest_tombstone_unix_ns: u64 = 0,
    visibility_id: u64 = 0,
    gc_requested: bool = false,
};

pub const BorrowedObsoletePathMeta = struct {
    path: []const u8,
    delete_after_ns: u64,
};

pub const BorrowedManifest = struct {
    raw: []u8,
    next_run_id: u64,
    runs: []BorrowedRunMeta,
    obsolete_paths: []BorrowedObsoletePathMeta,

    pub fn deinit(self: *BorrowedManifest, allocator: std.mem.Allocator) void {
        allocator.free(self.runs);
        allocator.free(self.obsolete_paths);
        if (self.raw.len > 0) allocator.free(self.raw);
        self.* = undefined;
    }
};

pub fn encodeAlloc(allocator: std.mem.Allocator, manifest: Manifest) ![]u8 {
    var bytes = std.ArrayListUnmanaged(u8).empty;
    errdefer bytes.deinit(allocator);

    try bytes.appendSlice(allocator, magic);
    try appendU32(allocator, &bytes, version);
    try appendU64(allocator, &bytes, manifest.next_run_id);
    try appendU32(allocator, &bytes, @intCast(manifest.runs.len));
    try appendU32(allocator, &bytes, @intCast(manifest.obsolete_paths.len));
    for (manifest.runs) |run| {
        try bytes.appendSlice(allocator, &runHeader(run));
        try bytes.appendSlice(allocator, run.path);
        if (run.smallest_namespace_name) |name| try bytes.appendSlice(allocator, name);
        try bytes.appendSlice(allocator, run.smallest_key);
        if (run.largest_namespace_name) |name| try bytes.appendSlice(allocator, name);
        try bytes.appendSlice(allocator, run.largest_key);
    }
    for (manifest.obsolete_paths) |obsolete| {
        try appendU64(allocator, &bytes, obsolete.delete_after_ns);
        try appendU32(allocator, &bytes, @intCast(obsolete.path.len));
        try bytes.appendSlice(allocator, obsolete.path);
    }
    try appendU32(allocator, &bytes, Crc32.hash(bytes.items));

    return try bytes.toOwnedSlice(allocator);
}

fn putInt(comptime T: type, bytes: []u8, offset: *usize, value: T) void {
    std.mem.writeInt(T, bytes[offset.*..][0..@sizeOf(T)], value, .little);
    offset.* += @sizeOf(T);
}

pub fn runHeader(run: RunMeta) [112]u8 {
    var bytes: [112]u8 = undefined;
    var offset: usize = 0;
    putInt(u64, &bytes, &offset, run.id);
    putInt(u32, &bytes, &offset, run.level);
    putInt(u64, &bytes, &offset, run.size_bytes);
    inline for (.{ "logical_entry_bytes", "physical_entry_bytes", "raw_blocks", "compressed_blocks", "compression_codec_mask" }) |field|
        putInt(u64, &bytes, &offset, @field(run.compression_stats, field));
    putInt(u32, &bytes, &offset, @intCast(run.path.len));
    putInt(u32, &bytes, &offset, if (run.smallest_namespace_name) |name| @intCast(name.len) else 0);
    putInt(u32, &bytes, &offset, @intCast(run.smallest_key.len));
    putInt(u32, &bytes, &offset, if (run.largest_namespace_name) |name| @intCast(name.len) else 0);
    putInt(u32, &bytes, &offset, @intCast(run.largest_key.len));
    putInt(u32, &bytes, &offset, run.entry_count);
    putInt(u64, &bytes, &offset, if (run.tombstone_count) |count| count else std.math.maxInt(u64));
    putInt(u64, &bytes, &offset, run.oldest_tombstone_unix_ns);
    putInt(u64, &bytes, &offset, run.visibility_id);
    putInt(u32, &bytes, &offset, @intFromBool(run.gc_requested));
    std.debug.assert(offset == bytes.len);
    return bytes;
}

pub fn decodeAlloc(allocator: std.mem.Allocator, raw: []const u8) anyerror!OwnedManifest {
    if (std.mem.startsWith(u8, raw, journal_magic)) {
        const owned_raw = try allocator.dupe(u8, raw);
        var decoded = decodeBorrowedOwnedAlloc(allocator, owned_raw) catch |err| {
            allocator.free(owned_raw);
            return err;
        };
        defer decoded.deinit(allocator);
        const runs = try allocator.alloc(RunMeta, decoded.runs.len);
        defer allocator.free(runs);
        for (runs, decoded.runs) |*out, run| out.* = borrowedRunMeta(run);
        const obsolete = try allocator.alloc(ObsoletePathMeta, decoded.obsolete_paths.len);
        defer allocator.free(obsolete);
        for (obsolete, decoded.obsolete_paths) |*out, path| out.* = .{ .path = path.path, .delete_after_ns = path.delete_after_ns };
        const canonical = try encodeAlloc(allocator, .{ .next_run_id = decoded.next_run_id, .runs = runs, .obsolete_paths = obsolete });
        defer allocator.free(canonical);
        return decodeAlloc(allocator, canonical);
    }
    const body = try verifiedBody(raw);
    var cursor: usize = 0;
    if (body.len < magic.len + 20) return error.InvalidManifest;
    if (!std.mem.eql(u8, body[0..magic.len], magic)) return error.InvalidManifest;
    cursor += magic.len;

    const found_version = try readU32(body, &cursor);
    if (found_version != version and found_version != 9) return error.UnsupportedVersion;

    const next_run_id = try readU64(body, &cursor);
    const run_count: usize = @intCast(try readU32(body, &cursor));
    const obsolete_count: usize = @intCast(try readU32(body, &cursor));
    if (run_count > (body.len - cursor) / @as(usize, if (found_version >= 10) 112 else 84)) return error.InvalidManifest;
    if (obsolete_count > (body.len - cursor) / 12) return error.InvalidManifest;
    const run_metas = try allocator.alloc(OwnedRunMeta, run_count);
    errdefer allocator.free(run_metas);
    const obsolete_metas = try allocator.alloc(OwnedObsoletePathMeta, obsolete_count);
    errdefer allocator.free(obsolete_metas);
    var out: OwnedManifest = .{
        .next_run_id = next_run_id,
        .runs = run_metas,
        .obsolete_paths = obsolete_metas,
    };

    var initialized: usize = 0;
    errdefer {
        for (out.runs[0..initialized]) |*run| run.deinit(allocator);
    }
    var obsolete_initialized: usize = 0;
    errdefer {
        for (out.obsolete_paths[0..obsolete_initialized]) |*obsolete| obsolete.deinit(allocator);
    }

    for (out.runs) |*run| {
        const id = try readU64(body, &cursor);
        const level = try readU32(body, &cursor);
        const size_bytes = try readU64(body, &cursor);
        const compression_stats = try readCompressionStats(body, &cursor);
        const path_len: usize = @intCast(try readU32(body, &cursor));
        const smallest_namespace_len: usize = @intCast(try readU32(body, &cursor));
        const smallest_len: usize = @intCast(try readU32(body, &cursor));
        const largest_namespace_len: usize = @intCast(try readU32(body, &cursor));
        const largest_len: usize = @intCast(try readU32(body, &cursor));
        const entry_count = try readU32(body, &cursor);
        const tombstone_count = if (found_version >= 10) try readTombstoneCount(body, &cursor, entry_count) else null;
        const oldest_tombstone_unix_ns = if (found_version >= 10) try readU64(body, &cursor) else 0;
        const visibility_id = if (found_version >= 10) try readU64(body, &cursor) else 0;
        if (visibility_id > id) return error.InvalidManifest;
        const gc_requested = if (found_version >= 10) try readU32(body, &cursor) else 0;
        if (gc_requested > 1) return error.InvalidManifest;
        if (id == 0 or path_len == 0) return error.InvalidManifest;

        const path = try allocator.dupe(u8, try readSlice(body, &cursor, path_len));
        errdefer allocator.free(path);
        const smallest_namespace = if (smallest_namespace_len > 0) try allocator.dupe(u8, try readSlice(body, &cursor, smallest_namespace_len)) else null;
        errdefer if (smallest_namespace) |name| allocator.free(name);
        const smallest_key = try allocator.dupe(u8, try readSlice(body, &cursor, smallest_len));
        errdefer allocator.free(smallest_key);
        const largest_namespace = if (largest_namespace_len > 0) try allocator.dupe(u8, try readSlice(body, &cursor, largest_namespace_len)) else null;
        errdefer if (largest_namespace) |name| allocator.free(name);
        const largest_key = try allocator.dupe(u8, try readSlice(body, &cursor, largest_len));
        errdefer allocator.free(largest_key);
        run.* = .{
            .id = id,
            .level = level,
            .size_bytes = size_bytes,
            .compression_stats = compression_stats,
            .path = path,
            .smallest_namespace_name = smallest_namespace,
            .smallest_key = smallest_key,
            .largest_namespace_name = largest_namespace,
            .largest_key = largest_key,
            .entry_count = entry_count,
            .tombstone_count = tombstone_count,
            .oldest_tombstone_unix_ns = oldest_tombstone_unix_ns,
            .visibility_id = visibility_id,
            .gc_requested = gc_requested != 0,
        };
        initialized += 1;
    }

    for (out.obsolete_paths) |*obsolete| {
        const delete_after_ns = try readU64(body, &cursor);
        const path_len: usize = @intCast(try readU32(body, &cursor));
        if (path_len == 0) return error.InvalidManifest;
        obsolete.* = .{
            .delete_after_ns = delete_after_ns,
            .path = try allocator.dupe(u8, try readSlice(body, &cursor, path_len)),
        };
        obsolete_initialized += 1;
    }

    if (cursor != body.len) return error.InvalidManifest;
    return out;
}

pub fn decodeBorrowedOwnedAlloc(allocator: std.mem.Allocator, raw: []u8) anyerror!BorrowedManifest {
    if (std.mem.startsWith(u8, raw, journal_magic)) return decodeJournalBorrowed(allocator, raw);
    const body = try verifiedBody(raw);
    var cursor: usize = 0;
    if (body.len < magic.len + 20) return error.InvalidManifest;
    if (!std.mem.eql(u8, body[0..magic.len], magic)) return error.InvalidManifest;
    cursor += magic.len;

    const found_version = try readU32(body, &cursor);
    if (found_version != version and found_version != 9) return error.UnsupportedVersion;

    const next_run_id = try readU64(body, &cursor);
    const run_count: usize = @intCast(try readU32(body, &cursor));
    const obsolete_count: usize = @intCast(try readU32(body, &cursor));
    if (run_count > (body.len - cursor) / @as(usize, if (found_version >= 10) 112 else 84)) return error.InvalidManifest;
    if (obsolete_count > (body.len - cursor) / 12) return error.InvalidManifest;
    const run_metas = try allocator.alloc(BorrowedRunMeta, run_count);
    errdefer allocator.free(run_metas);
    const obsolete_metas = try allocator.alloc(BorrowedObsoletePathMeta, obsolete_count);
    errdefer allocator.free(obsolete_metas);
    const out: BorrowedManifest = .{
        .raw = raw,
        .next_run_id = next_run_id,
        .runs = run_metas,
        .obsolete_paths = obsolete_metas,
    };

    for (out.runs) |*run| {
        const id = try readU64(body, &cursor);
        const level = try readU32(body, &cursor);
        const size_bytes = try readU64(body, &cursor);
        const compression_stats = try readCompressionStats(body, &cursor);
        const path_len: usize = @intCast(try readU32(body, &cursor));
        const smallest_namespace_len: usize = @intCast(try readU32(body, &cursor));
        const smallest_len: usize = @intCast(try readU32(body, &cursor));
        const largest_namespace_len: usize = @intCast(try readU32(body, &cursor));
        const largest_len: usize = @intCast(try readU32(body, &cursor));
        const entry_count = try readU32(body, &cursor);
        const tombstone_count = if (found_version >= 10) try readTombstoneCount(body, &cursor, entry_count) else null;
        const oldest_tombstone_unix_ns = if (found_version >= 10) try readU64(body, &cursor) else 0;
        const visibility_id = if (found_version >= 10) try readU64(body, &cursor) else 0;
        if (visibility_id > id) return error.InvalidManifest;
        const gc_requested = if (found_version >= 10) try readU32(body, &cursor) else 0;
        if (gc_requested > 1) return error.InvalidManifest;
        if (id == 0 or path_len == 0) return error.InvalidManifest;

        run.* = .{
            .id = id,
            .level = level,
            .size_bytes = size_bytes,
            .compression_stats = compression_stats,
            .path = try readSlice(body, &cursor, path_len),
            .smallest_namespace_name = if (smallest_namespace_len > 0) try readSlice(body, &cursor, smallest_namespace_len) else null,
            .smallest_key = try readSlice(body, &cursor, smallest_len),
            .largest_namespace_name = if (largest_namespace_len > 0) try readSlice(body, &cursor, largest_namespace_len) else null,
            .largest_key = try readSlice(body, &cursor, largest_len),
            .entry_count = entry_count,
            .tombstone_count = tombstone_count,
            .oldest_tombstone_unix_ns = oldest_tombstone_unix_ns,
            .visibility_id = visibility_id,
            .gc_requested = gc_requested != 0,
        };
    }

    for (out.obsolete_paths) |*obsolete| {
        const delete_after_ns = try readU64(body, &cursor);
        const path_len: usize = @intCast(try readU32(body, &cursor));
        if (path_len == 0) return error.InvalidManifest;
        obsolete.* = .{
            .delete_after_ns = delete_after_ns,
            .path = try readSlice(body, &cursor, path_len),
        };
    }

    if (cursor != body.len) return error.InvalidManifest;
    return out;
}

pub fn borrowedRunMeta(run: BorrowedRunMeta) RunMeta {
    var out: RunMeta = undefined;
    inline for (@typeInfo(RunMeta).@"struct".fields) |field| @field(out, field.name) = @field(run, field.name);
    return out;
}

/// A checkpoint starts a new journal file. Each frame independently protects
/// its length/sequence header and complete payload, so recovery distinguishes
/// an incomplete final append from corruption of a complete record.
pub fn encodeJournalFrameAlloc(allocator: std.mem.Allocator, sequence: u64, checkpoint: bool, removed_runs: []const u64, removed_paths: []const []const u8, manifest: Manifest) ![]u8 {
    if (checkpoint and (removed_runs.len != 0 or removed_paths.len != 0)) return error.InvalidManifest;
    const encoded = try encodeAlloc(allocator, manifest);
    defer allocator.free(encoded);
    var body: std.ArrayListUnmanaged(u8) = .empty;
    defer body.deinit(allocator);
    try appendU32(allocator, &body, @intCast(removed_runs.len));
    for (removed_runs) |id| try appendU64(allocator, &body, id);
    try appendU32(allocator, &body, @intCast(removed_paths.len));
    for (removed_paths) |path| {
        try appendU32(allocator, &body, @intCast(path.len));
        try body.appendSlice(allocator, path);
    }
    try body.appendSlice(allocator, encoded);
    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(allocator);
    if (checkpoint) try out.appendSlice(allocator, journal_magic);
    const header_start = out.items.len;
    try appendU64(allocator, &out, body.items.len);
    try appendU64(allocator, &out, sequence);
    try appendU32(allocator, &out, @intFromBool(checkpoint));
    try appendU32(allocator, &out, Crc32.hash(out.items[header_start..]));
    try out.appendSlice(allocator, body.items);
    try appendU32(allocator, &out, Crc32.hash(body.items));
    return try out.toOwnedSlice(allocator);
}

fn decodeJournalBorrowed(allocator: std.mem.Allocator, raw: []u8) !BorrowedManifest {
    var runs: std.AutoHashMapUnmanaged(u64, BorrowedRunMeta) = .empty;
    defer runs.deinit(allocator);
    var paths: std.StringHashMapUnmanaged(BorrowedObsoletePathMeta) = .empty;
    defer paths.deinit(allocator);
    var cursor: usize = journal_magic.len;
    var sequence: u64 = 0;
    var first = true;
    var next_run_id: u64 = 0;
    while (cursor < raw.len) {
        if (raw.len - cursor < journal_header_len) break;
        const header = raw[cursor..][0..journal_header_len];
        if (Crc32.hash(header[0..20]) != std.mem.readInt(u32, header[20..24], .little)) return error.InvalidManifest;
        const length = std.mem.readInt(u64, header[0..8], .little);
        if (length > std.math.maxInt(u32)) return error.InvalidManifest;
        const found_sequence = std.mem.readInt(u64, header[8..16], .little);
        const kind = std.mem.readInt(u32, header[16..20], .little);
        if (first) sequence = found_sequence;
        if (found_sequence != sequence or kind != @intFromBool(first)) return error.InvalidManifest;
        cursor += journal_header_len;
        if (length > raw.len - cursor or raw.len - cursor - @as(usize, @intCast(length)) < 4) break;
        const body = raw[cursor..][0..@intCast(length)];
        if (Crc32.hash(body) != std.mem.readInt(u32, raw[cursor + body.len ..][0..4], .little)) return error.InvalidManifest;
        var offset: usize = 0;
        const removed_count = try readU32(body, &offset);
        if (removed_count > (body.len - offset) / 8 or (first and removed_count != 0)) return error.InvalidManifest;
        for (0..removed_count) |_| {
            if (!runs.remove(try readU64(body, &offset))) return error.InvalidManifest;
        }
        const removed_paths = try readU32(body, &offset);
        if (removed_paths > (body.len - offset) / 4 or (first and removed_paths != 0)) return error.InvalidManifest;
        for (0..removed_paths) |_| {
            const len = try readU32(body, &offset);
            if (!paths.remove(try readSlice(body, &offset, len))) return error.InvalidManifest;
        }
        // Embedded payloads are ordinary standalone manifests, never journals.
        if (!std.mem.startsWith(u8, body[offset..], magic)) return error.InvalidManifest;
        var edit = try decodeBorrowedOwnedAlloc(allocator, body[offset..]);
        edit.raw = &.{};
        defer edit.deinit(allocator);
        if (!first and edit.next_run_id < next_run_id) return error.InvalidManifest;
        next_run_id = edit.next_run_id;
        for (edit.runs) |run| try runs.put(allocator, run.id, run);
        for (edit.obsolete_paths) |path| try paths.put(allocator, path.path, path);
        sequence = std.math.add(u64, sequence, 1) catch return error.InvalidManifest;
        first = false;
        cursor += body.len + 4;
    }
    if (first) return error.InvalidManifest;
    const result_runs = try allocator.alloc(BorrowedRunMeta, runs.count());
    errdefer allocator.free(result_runs);
    var run_iter = runs.valueIterator();
    for (result_runs) |*run| run.* = run_iter.next().?.*;
    const result_paths = try allocator.alloc(BorrowedObsoletePathMeta, paths.count());
    var path_iter = paths.valueIterator();
    for (result_paths) |*path| path.* = path_iter.next().?.*;
    return .{ .raw = raw, .next_run_id = next_run_id, .runs = result_runs, .obsolete_paths = result_paths };
}

fn readTombstoneCount(raw: []const u8, cursor: *usize, entry_count: u32) !?u32 {
    const count = try readU64(raw, cursor);
    if (count == std.math.maxInt(u64)) return null;
    if (count > entry_count) return error.InvalidManifest;
    return @intCast(count);
}

fn verifiedBody(raw: []const u8) ![]const u8 {
    if (raw.len < checksum_len) return error.InvalidManifest;
    const body = raw[0 .. raw.len - checksum_len];
    const expected = std.mem.readInt(u32, raw[raw.len - checksum_len ..][0..checksum_len], .little);
    if (Crc32.hash(body) != expected) return error.InvalidManifest;
    return body;
}

fn appendU32(allocator: std.mem.Allocator, bytes: *std.ArrayListUnmanaged(u8), value: u32) !void {
    var buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &buf, value, .little);
    try bytes.appendSlice(allocator, &buf);
}

fn appendU64(allocator: std.mem.Allocator, bytes: *std.ArrayListUnmanaged(u8), value: u64) !void {
    var buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &buf, value, .little);
    try bytes.appendSlice(allocator, &buf);
}

fn appendCompressionStats(allocator: std.mem.Allocator, bytes: *std.ArrayListUnmanaged(u8), stats: lsm_table_file.CompressionStats) !void {
    try appendU64(allocator, bytes, stats.logical_entry_bytes);
    try appendU64(allocator, bytes, stats.physical_entry_bytes);
    try appendU64(allocator, bytes, stats.raw_blocks);
    try appendU64(allocator, bytes, stats.compressed_blocks);
    try appendU64(allocator, bytes, stats.compression_codec_mask);
}

fn readU32(raw: []const u8, cursor: *usize) !u32 {
    const bytes = try readSlice(raw, cursor, 4);
    return std.mem.readInt(u32, bytes[0..4], .little);
}

fn readU64(raw: []const u8, cursor: *usize) !u64 {
    const bytes = try readSlice(raw, cursor, 8);
    return std.mem.readInt(u64, bytes[0..8], .little);
}

fn readCompressionStats(raw: []const u8, cursor: *usize) !lsm_table_file.CompressionStats {
    return .{
        .logical_entry_bytes = try readU64(raw, cursor),
        .physical_entry_bytes = try readU64(raw, cursor),
        .raw_blocks = try readU64(raw, cursor),
        .compressed_blocks = try readU64(raw, cursor),
        .compression_codec_mask = try readU64(raw, cursor),
    };
}

fn readSlice(raw: []const u8, cursor: *usize, len: usize) ![]const u8 {
    if (cursor.* + len > raw.len) return error.InvalidManifest;
    const out = raw[cursor.* .. cursor.* + len];
    cursor.* += len;
    return out;
}

test "manifest tombstone counts round trip and reject impossible values" {
    const allocator = std.testing.allocator;
    var runs = [_]RunMeta{.{ .id = 1, .level = 0, .size_bytes = 1, .path = "runs/1.tbl", .smallest_namespace_name = null, .smallest_key = "a", .largest_namespace_name = null, .largest_key = "z", .entry_count = 4, .tombstone_count = 3, .oldest_tombstone_unix_ns = 123, .gc_requested = true }};
    const encoded = try encodeAlloc(allocator, .{ .next_run_id = 2, .runs = &runs });
    defer allocator.free(encoded);
    var owned = try decodeAlloc(allocator, encoded);
    defer owned.deinit(allocator);
    try std.testing.expectEqual(@as(?u32, 3), owned.runs[0].tombstone_count);
    try std.testing.expectEqual(@as(u64, 123), owned.runs[0].oldest_tombstone_unix_ns);
    try std.testing.expect(owned.runs[0].gc_requested);
    var borrowed = try decodeBorrowedOwnedAlloc(allocator, try allocator.dupe(u8, encoded));
    defer borrowed.deinit(allocator);
    try std.testing.expectEqual(@as(?u32, 3), borrowed.runs[0].tombstone_count);
    try std.testing.expectEqual(@as(u64, 123), borrowed.runs[0].oldest_tombstone_unix_ns);
    try std.testing.expect(borrowed.runs[0].gc_requested);
    runs[0].tombstone_count = 5;
    const invalid = try encodeAlloc(allocator, .{ .next_run_id = 2, .runs = &runs });
    defer allocator.free(invalid);
    try std.testing.expectError(error.InvalidManifest, decodeAlloc(allocator, invalid));
    runs[0].tombstone_count = 3;
    runs[0].id = 7;
    runs[0].visibility_id = 1;
    const split_encoded = try encodeAlloc(allocator, .{ .next_run_id = 8, .runs = &runs });
    defer allocator.free(split_encoded);
    var split = try decodeAlloc(allocator, split_encoded);
    defer split.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 1), split.runs[0].visibility_id);
    var split_borrowed = try decodeBorrowedOwnedAlloc(allocator, try allocator.dupe(u8, split_encoded));
    defer split_borrowed.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 1), split_borrowed.runs[0].visibility_id);
    runs[0].visibility_id = 8;
    const invalid_visibility = try encodeAlloc(allocator, .{ .next_run_id = 8, .runs = &runs });
    defer allocator.free(invalid_visibility);
    try std.testing.expectError(error.InvalidManifest, decodeAlloc(allocator, invalid_visibility));
    // Ownership transfers only on successful decoding.
    try std.testing.expectError(error.InvalidManifest, decodeBorrowedOwnedAlloc(allocator, invalid_visibility));
}

test "manifest journal replays edits and accepts only incomplete final frames" {
    const allocator = std.testing.allocator;
    const run = RunMeta{ .id = 1, .level = 0, .size_bytes = 1, .path = "runs/1.tbl", .smallest_namespace_name = null, .smallest_key = "a", .largest_namespace_name = null, .largest_key = "z", .entry_count = 4, .tombstone_count = 3 };
    const base = try encodeJournalFrameAlloc(allocator, 0, true, &.{}, &.{}, .{ .next_run_id = 2, .runs = &.{run}, .obsolete_paths = &.{.{ .path = "old.tbl", .delete_after_ns = 9 }} });
    defer allocator.free(base);
    var moved = run;
    moved.level = 1;
    moved.gc_requested = true;
    const edit = try encodeJournalFrameAlloc(allocator, 1, false, &.{1}, &.{"old.tbl"}, .{ .next_run_id = 2, .runs = &.{moved} });
    defer allocator.free(edit);
    const combined = try std.mem.concat(allocator, u8, &.{ base, edit });
    defer allocator.free(combined);
    for (0..edit.len + 1) |tail| {
        var decoded = try decodeAlloc(allocator, combined[0 .. base.len + tail]);
        defer decoded.deinit(allocator);
        try std.testing.expectEqual(@as(usize, 1), decoded.runs.len);
        try std.testing.expectEqual(@as(u32, if (tail == edit.len) 1 else 0), decoded.runs[0].level);
        try std.testing.expectEqual(tail == edit.len, decoded.runs[0].gc_requested);
        try std.testing.expectEqual(@as(usize, if (tail == edit.len) 0 else 1), decoded.obsolete_paths.len);
    }
    for ([_]usize{ base.len, base.len + 8, base.len + 16, base.len + journal_header_len, combined.len - 1 }) |offset| {
        combined[offset] ^= 1;
        try std.testing.expectError(error.InvalidManifest, decodeAlloc(allocator, combined));
        combined[offset] ^= 1;
    }
    var borrowed = try decodeBorrowedOwnedAlloc(allocator, try allocator.dupe(u8, combined));
    defer borrowed.deinit(allocator);
    try std.testing.expectEqualStrings("runs/1.tbl", borrowed.runs[0].path);
    const duplicate = try std.mem.concat(allocator, u8, &.{ combined, edit });
    defer allocator.free(duplicate);
    try std.testing.expectError(error.InvalidManifest, decodeAlloc(allocator, duplicate));
    try std.testing.checkAllAllocationFailures(allocator, struct {
        fn decode(alloc: std.mem.Allocator, bytes: []const u8) !void {
            var decoded = try decodeAlloc(alloc, bytes);
            defer decoded.deinit(alloc);
            try std.testing.expectEqual(@as(usize, 1), decoded.runs.len);
        }
    }.decode, .{combined});
}

test "manifest codec round trips run metadata" {
    const runs = [_]RunMeta{
        .{
            .id = 7,
            .level = 0,
            .size_bytes = 700,
            .compression_stats = .{
                .logical_entry_bytes = 900,
                .physical_entry_bytes = 450,
                .raw_blocks = 1,
                .compressed_blocks = 2,
                .compression_codec_mask = lsm_table_file.blockCompressionCodecMask(.snappy),
            },
            .path = "runs/000007.tbl",
            .smallest_namespace_name = null,
            .smallest_key = "doc:a",
            .largest_namespace_name = null,
            .largest_key = "doc:z",
            .entry_count = 24,
        },
        .{
            .id = 8,
            .level = 2,
            .size_bytes = 800,
            .compression_stats = .{
                .logical_entry_bytes = 1200,
                .physical_entry_bytes = 1000,
                .raw_blocks = 3,
                .compressed_blocks = 4,
                .compression_codec_mask = lsm_table_file.blockCompressionCodecMask(.snappy),
            },
            .path = "runs/000008.tbl",
            .smallest_namespace_name = "meta",
            .smallest_key = "meta:a",
            .largest_namespace_name = "meta",
            .largest_key = "meta:z",
            .entry_count = 3,
        },
    };

    const encoded = try encodeAlloc(std.testing.allocator, .{
        .next_run_id = 9,
        .runs = &runs,
        .obsolete_paths = &.{
            .{
                .path = "runs/000001.tbl",
                .delete_after_ns = 1234,
            },
        },
    });
    defer std.testing.allocator.free(encoded);

    var decoded = try decodeAlloc(std.testing.allocator, encoded);
    defer decoded.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u64, 9), decoded.next_run_id);
    try std.testing.expectEqual(@as(usize, 2), decoded.runs.len);
    try std.testing.expectEqual(@as(u64, 7), decoded.runs[0].id);
    try std.testing.expectEqual(@as(u32, 0), decoded.runs[0].level);
    try std.testing.expectEqual(@as(u64, 700), decoded.runs[0].size_bytes);
    try std.testing.expectEqual(@as(u64, 900), decoded.runs[0].compression_stats.logical_entry_bytes);
    try std.testing.expectEqual(@as(u64, 450), decoded.runs[0].compression_stats.physical_entry_bytes);
    try std.testing.expectEqual(lsm_table_file.blockCompressionCodecMask(.snappy), decoded.runs[0].compression_stats.compression_codec_mask);
    try std.testing.expectEqualStrings("runs/000007.tbl", decoded.runs[0].path);
    try std.testing.expectEqual(@as(?[]const u8, null), decoded.runs[0].smallest_namespace_name);
    try std.testing.expectEqualStrings("doc:z", decoded.runs[0].largest_key);
    try std.testing.expectEqualStrings("meta", decoded.runs[1].largest_namespace_name.?);
    try std.testing.expectEqual(@as(u32, 2), decoded.runs[1].level);
    try std.testing.expectEqual(@as(u64, 800), decoded.runs[1].size_bytes);
    try std.testing.expectEqual(@as(u64, 1200), decoded.runs[1].compression_stats.logical_entry_bytes);
    try std.testing.expectEqual(@as(u64, 4), decoded.runs[1].compression_stats.compressed_blocks);
    try std.testing.expectEqual(@as(u32, 3), decoded.runs[1].entry_count);
    try std.testing.expectEqual(@as(usize, 1), decoded.obsolete_paths.len);
    try std.testing.expectEqual(@as(u64, 1234), decoded.obsolete_paths[0].delete_after_ns);
    try std.testing.expectEqualStrings("runs/000001.tbl", decoded.obsolete_paths[0].path);
}

test "manifest borrowed codec round trips run metadata" {
    const runs = [_]RunMeta{
        .{
            .id = 7,
            .level = 0,
            .size_bytes = 700,
            .compression_stats = .{
                .logical_entry_bytes = 900,
                .physical_entry_bytes = 450,
                .raw_blocks = 1,
                .compressed_blocks = 2,
                .compression_codec_mask = lsm_table_file.blockCompressionCodecMask(.snappy),
            },
            .path = "runs/000007.tbl",
            .smallest_namespace_name = null,
            .smallest_key = "doc:a",
            .largest_namespace_name = null,
            .largest_key = "doc:z",
            .entry_count = 24,
        },
        .{
            .id = 8,
            .level = 2,
            .size_bytes = 800,
            .compression_stats = .{
                .logical_entry_bytes = 1200,
                .physical_entry_bytes = 1000,
                .raw_blocks = 3,
                .compressed_blocks = 4,
                .compression_codec_mask = lsm_table_file.blockCompressionCodecMask(.snappy),
            },
            .path = "runs/000008.tbl",
            .smallest_namespace_name = "meta",
            .smallest_key = "meta:a",
            .largest_namespace_name = "meta",
            .largest_key = "meta:z",
            .entry_count = 3,
        },
    };

    const encoded = try encodeAlloc(std.testing.allocator, .{
        .next_run_id = 9,
        .runs = &runs,
        .obsolete_paths = &.{
            .{
                .path = "runs/000001.tbl",
                .delete_after_ns = 1234,
            },
        },
    });

    var decoded = try decodeBorrowedOwnedAlloc(std.testing.allocator, encoded);
    defer decoded.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u64, 9), decoded.next_run_id);
    try std.testing.expectEqual(@as(usize, 2), decoded.runs.len);
    try std.testing.expectEqual(@as(u64, 7), decoded.runs[0].id);
    try std.testing.expectEqual(@as(u32, 0), decoded.runs[0].level);
    try std.testing.expectEqual(@as(u64, 700), decoded.runs[0].size_bytes);
    try std.testing.expectEqual(@as(u64, 900), decoded.runs[0].compression_stats.logical_entry_bytes);
    try std.testing.expectEqual(@as(u64, 450), decoded.runs[0].compression_stats.physical_entry_bytes);
    try std.testing.expectEqual(lsm_table_file.blockCompressionCodecMask(.snappy), decoded.runs[0].compression_stats.compression_codec_mask);
    try std.testing.expectEqualStrings("runs/000007.tbl", decoded.runs[0].path);
    try std.testing.expectEqual(@as(?[]const u8, null), decoded.runs[0].smallest_namespace_name);
    try std.testing.expectEqualStrings("doc:z", decoded.runs[0].largest_key);
    try std.testing.expectEqualStrings("meta", decoded.runs[1].largest_namespace_name.?);
    try std.testing.expectEqual(@as(u32, 2), decoded.runs[1].level);
    try std.testing.expectEqual(@as(u64, 800), decoded.runs[1].size_bytes);
    try std.testing.expectEqual(@as(u64, 1200), decoded.runs[1].compression_stats.logical_entry_bytes);
    try std.testing.expectEqual(@as(u64, 4), decoded.runs[1].compression_stats.compressed_blocks);
    try std.testing.expectEqual(@as(u32, 3), decoded.runs[1].entry_count);
    try std.testing.expectEqual(@as(usize, 1), decoded.obsolete_paths.len);
    try std.testing.expectEqual(@as(u64, 1234), decoded.obsolete_paths[0].delete_after_ns);
    try std.testing.expectEqualStrings("runs/000001.tbl", decoded.obsolete_paths[0].path);
}

test "manifest codec rejects invalid header" {
    try std.testing.expectError(error.InvalidManifest, decodeAlloc(std.testing.allocator, "bad"));
}

test "manifest codec rejects plausible checksummed run metadata corruption" {
    const encoded = try encodeAlloc(std.testing.allocator, .{
        .next_run_id = 8,
        .runs = &.{.{
            .id = 7,
            .level = 0,
            .size_bytes = 700,
            .path = "runs/000007.tbl",
            .smallest_namespace_name = null,
            .smallest_key = "doc:a",
            .largest_namespace_name = null,
            .largest_key = "doc:z",
            .entry_count = 24,
        }},
    });
    defer std.testing.allocator.free(encoded);

    // Preserve a structurally plausible level while invalidating the checksum.
    const level_offset = magic.len + @sizeOf(u32) + @sizeOf(u64) + @sizeOf(u32) * 2 + @sizeOf(u64);
    encoded[level_offset] ^= 0x01;
    try std.testing.expectError(
        error.InvalidManifest,
        decodeAlloc(std.testing.allocator, encoded),
    );
}
