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
const lsm_table_file = @import("table_file.zig");

pub const magic = "ALSMMAN1";
pub const version: u32 = 7;

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
    bloom_filter: []const u8,
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
    bloom_filter: []u8,

    pub fn deinit(self: *OwnedRunMeta, allocator: std.mem.Allocator) void {
        allocator.free(self.path);
        if (self.smallest_namespace_name) |name| allocator.free(name);
        allocator.free(self.smallest_key);
        if (self.largest_namespace_name) |name| allocator.free(name);
        allocator.free(self.largest_key);
        allocator.free(self.bloom_filter);
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
    bloom_filter: []const u8,
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
        try appendU64(allocator, &bytes, run.id);
        try appendU32(allocator, &bytes, run.level);
        try appendU64(allocator, &bytes, run.size_bytes);
        try appendCompressionStats(allocator, &bytes, run.compression_stats);
        try appendU32(allocator, &bytes, @intCast(run.path.len));
        try appendU32(allocator, &bytes, if (run.smallest_namespace_name) |name| @intCast(name.len) else 0);
        try appendU32(allocator, &bytes, @intCast(run.smallest_key.len));
        try appendU32(allocator, &bytes, if (run.largest_namespace_name) |name| @intCast(name.len) else 0);
        try appendU32(allocator, &bytes, @intCast(run.largest_key.len));
        try appendU32(allocator, &bytes, run.entry_count);
        try appendU32(allocator, &bytes, @intCast(run.bloom_filter.len));
        try bytes.appendSlice(allocator, run.path);
        if (run.smallest_namespace_name) |name| try bytes.appendSlice(allocator, name);
        try bytes.appendSlice(allocator, run.smallest_key);
        if (run.largest_namespace_name) |name| try bytes.appendSlice(allocator, name);
        try bytes.appendSlice(allocator, run.largest_key);
        try bytes.appendSlice(allocator, run.bloom_filter);
    }
    for (manifest.obsolete_paths) |obsolete| {
        try appendU64(allocator, &bytes, obsolete.delete_after_ns);
        try appendU32(allocator, &bytes, @intCast(obsolete.path.len));
        try bytes.appendSlice(allocator, obsolete.path);
    }

    return try bytes.toOwnedSlice(allocator);
}

pub fn decodeAlloc(allocator: std.mem.Allocator, raw: []const u8) !OwnedManifest {
    var cursor: usize = 0;
    if (raw.len < magic.len + 12) return error.InvalidManifest;
    if (!std.mem.eql(u8, raw[0..magic.len], magic)) return error.InvalidManifest;
    cursor += magic.len;

    const found_version = try readU32(raw, &cursor);
    if (found_version != version) return error.UnsupportedVersion;

    var out: OwnedManifest = .{
        .next_run_id = try readU64(raw, &cursor),
        .runs = try allocator.alloc(OwnedRunMeta, @intCast(try readU32(raw, &cursor))),
        .obsolete_paths = try allocator.alloc(OwnedObsoletePathMeta, @intCast(try readU32(raw, &cursor))),
    };
    errdefer {
        allocator.free(out.runs);
        allocator.free(out.obsolete_paths);
    }

    var initialized: usize = 0;
    errdefer {
        for (out.runs[0..initialized]) |*run| run.deinit(allocator);
    }
    var obsolete_initialized: usize = 0;
    errdefer {
        for (out.obsolete_paths[0..obsolete_initialized]) |*obsolete| obsolete.deinit(allocator);
    }

    for (out.runs) |*run| {
        const id = try readU64(raw, &cursor);
        const level = try readU32(raw, &cursor);
        const size_bytes = try readU64(raw, &cursor);
        const compression_stats = try readCompressionStats(raw, &cursor);
        const path_len: usize = @intCast(try readU32(raw, &cursor));
        const smallest_namespace_len: usize = @intCast(try readU32(raw, &cursor));
        const smallest_len: usize = @intCast(try readU32(raw, &cursor));
        const largest_namespace_len: usize = @intCast(try readU32(raw, &cursor));
        const largest_len: usize = @intCast(try readU32(raw, &cursor));
        const entry_count = try readU32(raw, &cursor);
        const bloom_len: usize = @intCast(try readU32(raw, &cursor));

        run.* = .{
            .id = id,
            .level = level,
            .size_bytes = size_bytes,
            .compression_stats = compression_stats,
            .path = try allocator.dupe(u8, try readSlice(raw, &cursor, path_len)),
            .smallest_namespace_name = if (smallest_namespace_len > 0) try allocator.dupe(u8, try readSlice(raw, &cursor, smallest_namespace_len)) else null,
            .smallest_key = try allocator.dupe(u8, try readSlice(raw, &cursor, smallest_len)),
            .largest_namespace_name = if (largest_namespace_len > 0) try allocator.dupe(u8, try readSlice(raw, &cursor, largest_namespace_len)) else null,
            .largest_key = try allocator.dupe(u8, try readSlice(raw, &cursor, largest_len)),
            .entry_count = entry_count,
            .bloom_filter = try allocator.dupe(u8, try readSlice(raw, &cursor, bloom_len)),
        };
        initialized += 1;
    }

    for (out.obsolete_paths) |*obsolete| {
        const delete_after_ns = try readU64(raw, &cursor);
        const path_len: usize = @intCast(try readU32(raw, &cursor));
        obsolete.* = .{
            .delete_after_ns = delete_after_ns,
            .path = try allocator.dupe(u8, try readSlice(raw, &cursor, path_len)),
        };
        obsolete_initialized += 1;
    }

    if (cursor != raw.len) return error.InvalidManifest;
    return out;
}

pub fn decodeBorrowedOwnedAlloc(allocator: std.mem.Allocator, raw: []u8) !BorrowedManifest {
    var cursor: usize = 0;
    if (raw.len < magic.len + 12) return error.InvalidManifest;
    if (!std.mem.eql(u8, raw[0..magic.len], magic)) return error.InvalidManifest;
    cursor += magic.len;

    const found_version = try readU32(raw, &cursor);
    if (found_version != version) return error.UnsupportedVersion;

    const out: BorrowedManifest = .{
        .raw = raw,
        .next_run_id = try readU64(raw, &cursor),
        .runs = try allocator.alloc(BorrowedRunMeta, @intCast(try readU32(raw, &cursor))),
        .obsolete_paths = try allocator.alloc(BorrowedObsoletePathMeta, @intCast(try readU32(raw, &cursor))),
    };
    errdefer {
        allocator.free(out.runs);
        allocator.free(out.obsolete_paths);
    }

    for (out.runs) |*run| {
        const id = try readU64(raw, &cursor);
        const level = try readU32(raw, &cursor);
        const size_bytes = try readU64(raw, &cursor);
        const compression_stats = try readCompressionStats(raw, &cursor);
        const path_len: usize = @intCast(try readU32(raw, &cursor));
        const smallest_namespace_len: usize = @intCast(try readU32(raw, &cursor));
        const smallest_len: usize = @intCast(try readU32(raw, &cursor));
        const largest_namespace_len: usize = @intCast(try readU32(raw, &cursor));
        const largest_len: usize = @intCast(try readU32(raw, &cursor));
        const entry_count = try readU32(raw, &cursor);
        const bloom_len: usize = @intCast(try readU32(raw, &cursor));

        run.* = .{
            .id = id,
            .level = level,
            .size_bytes = size_bytes,
            .compression_stats = compression_stats,
            .path = try readSlice(raw, &cursor, path_len),
            .smallest_namespace_name = if (smallest_namespace_len > 0) try readSlice(raw, &cursor, smallest_namespace_len) else null,
            .smallest_key = try readSlice(raw, &cursor, smallest_len),
            .largest_namespace_name = if (largest_namespace_len > 0) try readSlice(raw, &cursor, largest_namespace_len) else null,
            .largest_key = try readSlice(raw, &cursor, largest_len),
            .entry_count = entry_count,
            .bloom_filter = try readSlice(raw, &cursor, bloom_len),
        };
    }

    for (out.obsolete_paths) |*obsolete| {
        const delete_after_ns = try readU64(raw, &cursor);
        const path_len: usize = @intCast(try readU32(raw, &cursor));
        obsolete.* = .{
            .delete_after_ns = delete_after_ns,
            .path = try readSlice(raw, &cursor, path_len),
        };
    }

    if (cursor != raw.len) return error.InvalidManifest;
    return out;
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
            .bloom_filter = "bloom-a",
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
            .bloom_filter = "bloom-b",
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
    try std.testing.expectEqualStrings("bloom-b", decoded.runs[1].bloom_filter);
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
            .bloom_filter = "bloom-a",
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
            .bloom_filter = "bloom-b",
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
    try std.testing.expectEqualStrings("bloom-b", decoded.runs[1].bloom_filter);
    try std.testing.expectEqual(@as(usize, 1), decoded.obsolete_paths.len);
    try std.testing.expectEqual(@as(u64, 1234), decoded.obsolete_paths[0].delete_after_ns);
    try std.testing.expectEqualStrings("runs/000001.tbl", decoded.obsolete_paths[0].path);
}

test "manifest codec rejects invalid header" {
    try std.testing.expectError(error.InvalidManifest, decodeAlloc(std.testing.allocator, "bad"));
}
