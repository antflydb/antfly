// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the Elastic License 2.0 is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
// the Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! HA base-backup manifest format.
//!
//! A base backup pins immutable files plus the WAL boundary needed to catch a
//! standby up after copying. This module keeps that handoff explicit and
//! checksum-verifiable before a real filesystem/object-store copy flow exists.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Crc32 = std.hash.Crc32;
const standby_mod = @import("standby.zig");

pub const magic = [8]u8{ 'A', 'F', 'H', 'A', 'B', 'K', 'P', '\n' };
pub const format_version: u16 = 1;
pub const header_size: usize = 96;

const version_offset: usize = 8;
const header_len_offset: usize = 10;
const flags_offset: usize = 12;
const cluster_id_offset: usize = 16;
const shard_id_offset: usize = 24;
const table_id_offset: usize = 32;
const timeline_id_offset: usize = 40;
const epoch_offset: usize = 48;
const backup_lsn_offset: usize = 56;
const checkpoint_lsn_offset: usize = 64;
const file_count_offset: usize = 72;
const manifest_id_len_offset: usize = 76;
const body_len_offset: usize = 80;
const body_crc_offset: usize = 88;
const header_crc_offset: usize = 92;

const entry_header_size: usize = 28;

comptime {
    std.debug.assert(header_crc_offset + 4 == header_size);
}

pub const Identity = standby_mod.Identity;

pub const FileKind = enum(u16) {
    sstable = 1,
    manifest = 2,
    metadata = 3,
    wal_tail = 4,
    artifact = 5,
    other = 255,
    _,
};

pub const FileEntry = struct {
    path: []const u8,
    kind: FileKind,
    size_bytes: u64,
    crc32: u32,
    flags: u32 = 0,
};

pub const Manifest = struct {
    identity: Identity,
    manifest_id: []const u8,
    backup_lsn: u64,
    checkpoint_lsn: u64,
    files: []const FileEntry,
    flags: u32 = 0,
};

pub const FileEntryView = FileEntry;

pub const ManifestView = struct {
    identity: Identity,
    manifest_id: []const u8,
    backup_lsn: u64,
    checkpoint_lsn: u64,
    files: []const FileEntryView,
    flags: u32,

    pub fn totalBytes(self: ManifestView) u64 {
        var total: u64 = 0;
        for (self.files) |file| total +|= file.size_bytes;
        return total;
    }

    pub fn fileIndex(self: ManifestView, path: []const u8) ?usize {
        for (self.files, 0..) |file, idx| {
            if (std.mem.eql(u8, file.path, path)) return idx;
        }
        return null;
    }
};

pub const FileContent = struct {
    path: []const u8,
    bytes: []const u8,
};

pub fn crc32(bytes: []const u8) u32 {
    return Crc32.hash(bytes);
}

pub fn encodeAlloc(alloc: Allocator, manifest: Manifest) ![]u8 {
    try validateManifestInput(manifest);

    var body_len = manifest.manifest_id.len;
    for (manifest.files) |file| {
        body_len = try std.math.add(usize, body_len, entry_header_size);
        body_len = try std.math.add(usize, body_len, file.path.len);
    }

    const total_len = try std.math.add(usize, header_size, body_len);
    const out = try alloc.alloc(u8, total_len);
    errdefer alloc.free(out);

    @memset(out[0..header_size], 0);
    @memcpy(out[0..8], &magic);
    std.mem.writeInt(u16, out[version_offset..][0..2], format_version, .little);
    std.mem.writeInt(u16, out[header_len_offset..][0..2], header_size, .little);
    std.mem.writeInt(u32, out[flags_offset..][0..4], manifest.flags, .little);
    std.mem.writeInt(u64, out[cluster_id_offset..][0..8], manifest.identity.cluster_id, .little);
    std.mem.writeInt(u64, out[shard_id_offset..][0..8], manifest.identity.shard_id, .little);
    std.mem.writeInt(u64, out[table_id_offset..][0..8], manifest.identity.table_id, .little);
    std.mem.writeInt(u64, out[timeline_id_offset..][0..8], manifest.identity.timeline_id, .little);
    std.mem.writeInt(u64, out[epoch_offset..][0..8], manifest.identity.epoch, .little);
    std.mem.writeInt(u64, out[backup_lsn_offset..][0..8], manifest.backup_lsn, .little);
    std.mem.writeInt(u64, out[checkpoint_lsn_offset..][0..8], manifest.checkpoint_lsn, .little);
    std.mem.writeInt(u32, out[file_count_offset..][0..4], @intCast(manifest.files.len), .little);
    std.mem.writeInt(u32, out[manifest_id_len_offset..][0..4], @intCast(manifest.manifest_id.len), .little);
    std.mem.writeInt(u64, out[body_len_offset..][0..8], @intCast(body_len), .little);

    var cursor: usize = header_size;
    @memcpy(out[cursor..][0..manifest.manifest_id.len], manifest.manifest_id);
    cursor += manifest.manifest_id.len;
    for (manifest.files) |file| {
        std.mem.writeInt(u16, out[cursor..][0..2], @intFromEnum(file.kind), .little);
        std.mem.writeInt(u16, out[cursor + 2 ..][0..2], 0, .little);
        std.mem.writeInt(u32, out[cursor + 4 ..][0..4], file.flags, .little);
        std.mem.writeInt(u64, out[cursor + 8 ..][0..8], file.size_bytes, .little);
        std.mem.writeInt(u32, out[cursor + 16 ..][0..4], file.crc32, .little);
        std.mem.writeInt(u32, out[cursor + 20 ..][0..4], @intCast(file.path.len), .little);
        std.mem.writeInt(u32, out[cursor + 24 ..][0..4], 0, .little);
        cursor += entry_header_size;
        @memcpy(out[cursor..][0..file.path.len], file.path);
        cursor += file.path.len;
    }
    std.debug.assert(cursor == out.len);

    std.mem.writeInt(u32, out[body_crc_offset..][0..4], Crc32.hash(out[header_size..]), .little);
    std.mem.writeInt(u32, out[header_crc_offset..][0..4], Crc32.hash(out[0..header_crc_offset]), .little);
    return out;
}

pub fn decodeAlloc(alloc: Allocator, bytes: []const u8) !ManifestView {
    if (bytes.len < header_size) return error.EndOfStream;
    if (!std.mem.eql(u8, bytes[0..8], &magic)) return error.InvalidMagic;
    const decoded_version = std.mem.readInt(u16, bytes[version_offset..][0..2], .little);
    if (decoded_version == 0 or decoded_version > format_version) return error.UnsupportedVersion;
    const decoded_header_len = std.mem.readInt(u16, bytes[header_len_offset..][0..2], .little);
    if (decoded_header_len != header_size) return error.UnsupportedHeaderLength;
    const stored_header_crc = std.mem.readInt(u32, bytes[header_crc_offset..][0..4], .little);
    if (stored_header_crc != Crc32.hash(bytes[0..header_crc_offset])) return error.HeaderCrcMismatch;

    const body_len_u64 = std.mem.readInt(u64, bytes[body_len_offset..][0..8], .little);
    if (body_len_u64 > std.math.maxInt(usize)) return error.BodyTooLarge;
    const body_len: usize = @intCast(body_len_u64);
    const total_len = try std.math.add(usize, header_size, body_len);
    if (bytes.len < total_len) return error.EndOfStream;
    if (bytes.len != total_len) return error.TrailingBytes;
    const stored_body_crc = std.mem.readInt(u32, bytes[body_crc_offset..][0..4], .little);
    if (stored_body_crc != Crc32.hash(bytes[header_size..])) return error.BodyCrcMismatch;

    const manifest_id_len_u32 = std.mem.readInt(u32, bytes[manifest_id_len_offset..][0..4], .little);
    const manifest_id_len: usize = @intCast(manifest_id_len_u32);
    if (manifest_id_len == 0) return error.InvalidManifestId;
    if (manifest_id_len > body_len) return error.EndOfStream;

    const file_count_u32 = std.mem.readInt(u32, bytes[file_count_offset..][0..4], .little);
    const file_count: usize = @intCast(file_count_u32);
    if (file_count == 0) return error.EmptyManifest;
    const files = try alloc.alloc(FileEntryView, file_count);
    errdefer alloc.free(files);

    var cursor: usize = header_size;
    const manifest_id = bytes[cursor..][0..manifest_id_len];
    cursor += manifest_id_len;
    for (files) |*file| {
        if (bytes.len - cursor < entry_header_size) return error.EndOfStream;
        const path_len_u32 = std.mem.readInt(u32, bytes[cursor + 20 ..][0..4], .little);
        const path_len: usize = @intCast(path_len_u32);
        const entry_end = try std.math.add(usize, cursor + entry_header_size, path_len);
        if (entry_end > bytes.len) return error.EndOfStream;
        if (path_len == 0) return error.InvalidManifestPath;
        file.* = .{
            .path = bytes[cursor + entry_header_size .. entry_end],
            .kind = @enumFromInt(std.mem.readInt(u16, bytes[cursor..][0..2], .little)),
            .flags = std.mem.readInt(u32, bytes[cursor + 4 ..][0..4], .little),
            .size_bytes = std.mem.readInt(u64, bytes[cursor + 8 ..][0..8], .little),
            .crc32 = std.mem.readInt(u32, bytes[cursor + 16 ..][0..4], .little),
        };
        cursor = entry_end;
    }
    if (cursor != bytes.len) return error.TrailingBytes;

    const view = ManifestView{
        .identity = .{
            .cluster_id = std.mem.readInt(u64, bytes[cluster_id_offset..][0..8], .little),
            .shard_id = std.mem.readInt(u64, bytes[shard_id_offset..][0..8], .little),
            .table_id = std.mem.readInt(u64, bytes[table_id_offset..][0..8], .little),
            .timeline_id = std.mem.readInt(u64, bytes[timeline_id_offset..][0..8], .little),
            .epoch = std.mem.readInt(u64, bytes[epoch_offset..][0..8], .little),
        },
        .manifest_id = manifest_id,
        .backup_lsn = std.mem.readInt(u64, bytes[backup_lsn_offset..][0..8], .little),
        .checkpoint_lsn = std.mem.readInt(u64, bytes[checkpoint_lsn_offset..][0..8], .little),
        .files = files,
        .flags = std.mem.readInt(u32, bytes[flags_offset..][0..4], .little),
    };
    try validateManifestView(view);
    return view;
}

pub fn freeDecoded(alloc: Allocator, view: ManifestView) void {
    alloc.free(view.files);
}

pub fn validateManifestInput(manifest: Manifest) !void {
    try validateManifestIdentity(manifest.identity);
    if (manifest.manifest_id.len == 0) return error.InvalidManifestId;
    if (manifest.manifest_id.len > std.math.maxInt(u32)) return error.ManifestIdTooLong;
    if (manifest.files.len == 0) return error.EmptyManifest;
    if (manifest.files.len > std.math.maxInt(u32)) return error.TooManyManifestFiles;
    if (manifest.backup_lsn == 0) return error.InvalidBackupLsn;
    if (manifest.checkpoint_lsn < manifest.backup_lsn) return error.InvalidCheckpointLsn;
    try validateFiles(manifest.files);
}

pub fn validateManifestView(view: ManifestView) !void {
    try validateManifestIdentity(view.identity);
    if (view.manifest_id.len == 0) return error.InvalidManifestId;
    if (view.files.len == 0) return error.EmptyManifest;
    if (view.backup_lsn == 0) return error.InvalidBackupLsn;
    if (view.checkpoint_lsn < view.backup_lsn) return error.InvalidCheckpointLsn;
    try validateFiles(view.files);
}

fn validateManifestIdentity(identity: Identity) !void {
    if (identity.cluster_id == 0) return error.InvalidClusterId;
    if (identity.timeline_id == 0) return error.InvalidTimelineId;
    if (identity.epoch == 0) return error.InvalidEpoch;
}

pub fn verifyFileContents(view: ManifestView, contents: []const FileContent) !void {
    if (contents.len != view.files.len) return error.ManifestFileSetMismatch;
    for (view.files) |file| {
        const content = findContent(contents, file.path) orelse return error.ManifestFileMissing;
        if (content.bytes.len != file.size_bytes) return error.ManifestFileSizeMismatch;
        if (Crc32.hash(content.bytes) != file.crc32) return error.ManifestFileCrcMismatch;
    }
}

fn validateFiles(files: []const FileEntry) !void {
    for (files, 0..) |file, idx| {
        try validatePath(file.path);
        if (file.path.len > std.math.maxInt(u32)) return error.ManifestPathTooLong;
        for (files[0..idx]) |prior| {
            if (std.mem.eql(u8, prior.path, file.path)) return error.DuplicateManifestPath;
        }
    }
}

/// Validate a manifest-relative path before joining it to a local staging root
/// or an object-store generation prefix.
pub fn validatePath(path: []const u8) !void {
    if (path.len == 0) return error.InvalidManifestPath;
    if (std.mem.indexOfScalar(u8, path, 0) != null) return error.InvalidManifestPath;
    if (std.fs.path.isAbsolute(path)) return error.InvalidManifestPath;
    if (std.mem.indexOf(u8, path, "..") != null) return error.InvalidManifestPath;
}

fn findContent(contents: []const FileContent, path: []const u8) ?FileContent {
    for (contents) |content| {
        if (std.mem.eql(u8, content.path, path)) return content;
    }
    return null;
}

fn testIdentity() Identity {
    return .{
        .cluster_id = 100,
        .shard_id = 10,
        .table_id = 20,
        .timeline_id = 1,
        .epoch = 1,
    };
}

test "storage.ha backup manifest round trips backup bounds and files" {
    const alloc = std.testing.allocator;
    const files = [_]FileEntry{
        .{ .path = "store/1/manifest", .kind = .manifest, .size_bytes = 9, .crc32 = crc32("manifest!") },
        .{ .path = "store/1/sst/0001.sst", .kind = .sstable, .size_bytes = 7, .crc32 = crc32("sstable") },
        .{ .path = "wal/tail-0002", .kind = .wal_tail, .size_bytes = 8, .crc32 = crc32("wal-tail") },
    };
    const encoded = try encodeAlloc(alloc, .{
        .identity = testIdentity(),
        .manifest_id = "backup-0001",
        .backup_lsn = 10,
        .checkpoint_lsn = 12,
        .files = &files,
    });
    defer alloc.free(encoded);

    const decoded = try decodeAlloc(alloc, encoded);
    defer freeDecoded(alloc, decoded);
    try std.testing.expectEqual(@as(u64, 100), decoded.identity.cluster_id);
    try std.testing.expectEqualStrings("backup-0001", decoded.manifest_id);
    try std.testing.expectEqual(@as(u64, 10), decoded.backup_lsn);
    try std.testing.expectEqual(@as(u64, 12), decoded.checkpoint_lsn);
    try std.testing.expectEqual(@as(usize, 3), decoded.files.len);
    try std.testing.expectEqual(@as(u64, 24), decoded.totalBytes());
    try std.testing.expectEqual(@as(usize, 1), decoded.fileIndex("store/1/sst/0001.sst").?);
}

test "storage.ha backup manifest validates copied file contents" {
    const alloc = std.testing.allocator;
    const files = [_]FileEntry{
        .{ .path = "manifest", .kind = .manifest, .size_bytes = 5, .crc32 = crc32("hello") },
        .{ .path = "sst/1", .kind = .sstable, .size_bytes = 5, .crc32 = crc32("world") },
    };
    const encoded = try encodeAlloc(alloc, .{
        .identity = testIdentity(),
        .manifest_id = "copy",
        .backup_lsn = 1,
        .checkpoint_lsn = 2,
        .files = &files,
    });
    defer alloc.free(encoded);
    const decoded = try decodeAlloc(alloc, encoded);
    defer freeDecoded(alloc, decoded);

    const good = [_]FileContent{
        .{ .path = "manifest", .bytes = "hello" },
        .{ .path = "sst/1", .bytes = "world" },
    };
    try verifyFileContents(decoded, &good);

    const bad_size = [_]FileContent{
        .{ .path = "manifest", .bytes = "hello!" },
        .{ .path = "sst/1", .bytes = "world" },
    };
    try std.testing.expectError(error.ManifestFileSizeMismatch, verifyFileContents(decoded, &bad_size));

    const bad_crc = [_]FileContent{
        .{ .path = "manifest", .bytes = "HELLO" },
        .{ .path = "sst/1", .bytes = "world" },
    };
    try std.testing.expectError(error.ManifestFileCrcMismatch, verifyFileContents(decoded, &bad_crc));
}

test "storage.ha backup manifest rejects unsafe paths and invalid wal bounds" {
    const files = [_]FileEntry{
        .{ .path = "../escape", .kind = .metadata, .size_bytes = 1, .crc32 = 1 },
    };
    try std.testing.expectError(error.InvalidCheckpointLsn, validateManifestInput(.{
        .identity = testIdentity(),
        .manifest_id = "bad-lsn",
        .backup_lsn = 10,
        .checkpoint_lsn = 9,
        .files = &files,
    }));
    try std.testing.expectError(error.InvalidManifestPath, validateManifestInput(.{
        .identity = testIdentity(),
        .manifest_id = "bad-path",
        .backup_lsn = 1,
        .checkpoint_lsn = 1,
        .files = &files,
    }));

    const dupes = [_]FileEntry{
        .{ .path = "a", .kind = .metadata, .size_bytes = 1, .crc32 = 1 },
        .{ .path = "a", .kind = .metadata, .size_bytes = 1, .crc32 = 1 },
    };
    try std.testing.expectError(error.DuplicateManifestPath, validateManifestInput(.{
        .identity = testIdentity(),
        .manifest_id = "dupes",
        .backup_lsn = 1,
        .checkpoint_lsn = 1,
        .files = &dupes,
    }));

    var missing_identity = testIdentity();
    missing_identity.cluster_id = 0;
    try std.testing.expectError(error.InvalidClusterId, validateManifestInput(.{
        .identity = missing_identity,
        .manifest_id = "missing-cluster",
        .backup_lsn = 1,
        .checkpoint_lsn = 1,
        .files = &dupes,
    }));
    missing_identity = testIdentity();
    missing_identity.timeline_id = 0;
    try std.testing.expectError(error.InvalidTimelineId, validateManifestInput(.{
        .identity = missing_identity,
        .manifest_id = "missing-timeline",
        .backup_lsn = 1,
        .checkpoint_lsn = 1,
        .files = &dupes,
    }));
    missing_identity = testIdentity();
    missing_identity.epoch = 0;
    try std.testing.expectError(error.InvalidEpoch, validateManifestInput(.{
        .identity = missing_identity,
        .manifest_id = "missing-epoch",
        .backup_lsn = 1,
        .checkpoint_lsn = 1,
        .files = &dupes,
    }));
}

test "storage.ha backup manifest accepts only canonical portable relative paths" {
    for ([_][]const u8{
        "catalog/manifest",
        "tables/1/sst-0001",
        "catalog/file..name",
    }) |path| try validatePath(path);

    for ([_][]const u8{
        "",
        ".",
        "..",
        "/absolute",
        "./catalog",
        "catalog/./manifest",
        "catalog/../escape",
        "catalog//manifest",
        "catalog/",
        "catalog\\..\\escape",
        "catalog\\manifest",
    }) |path| {
        try std.testing.expectError(error.InvalidManifestPath, validatePath(path));
    }
}

test "storage.ha backup manifest detects body and header corruption" {
    const alloc = std.testing.allocator;
    const files = [_]FileEntry{
        .{ .path = "manifest", .kind = .manifest, .size_bytes = 5, .crc32 = crc32("hello") },
    };
    var encoded = try encodeAlloc(alloc, .{
        .identity = testIdentity(),
        .manifest_id = "corrupt",
        .backup_lsn = 1,
        .checkpoint_lsn = 1,
        .files = &files,
    });
    defer alloc.free(encoded);

    encoded[flags_offset] ^= 0x01;
    try std.testing.expectError(error.HeaderCrcMismatch, decodeAlloc(alloc, encoded));

    encoded[flags_offset] ^= 0x01;
    encoded[encoded.len - 1] ^= 0x01;
    try std.testing.expectError(error.BodyCrcMismatch, decodeAlloc(alloc, encoded));
}
