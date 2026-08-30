// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

//! AFB2's representation-neutral manifest and native-file block codecs.
//!
//! AFB is a transport envelope, not the backup catalog. Native and portable
//! are explicit representations inside the same envelope; full and delta are
//! explicit snapshot modes. Repository retention, refs, and deduplication live
//! in backup_repository.zig and address complete manifests by SHA-256.

const std = @import("std");

pub const manifest_schema_version: u32 = 1;
pub const afb_reader_version: u32 = 2;
pub const max_manifest_bytes: usize = 16 * 1024 * 1024;
pub const max_objects: usize = 1_000_000;
pub const max_path_bytes: usize = 4096;
pub const native_chunk_target_bytes: usize = 4 * 1024 * 1024;

pub const Representation = enum { portable, native };
pub const SnapshotMode = enum { full, delta };
pub const Compression = enum { none, zstd };

pub const Encryption = struct {
    algorithm: []const u8,
    key_id: []const u8,
};

pub const Compatibility = struct {
    min_afb_reader: u32 = afb_reader_version,
    storage_engine: []const u8 = "",
    min_antfly_version: []const u8 = "",
};

pub const ObjectDescriptor = struct {
    logical_path: []const u8,
    role: []const u8,
    size_bytes: u64,
    sha256: []const u8,
    /// Full bundles include every object. Delta manifests still describe the
    /// complete materialized snapshot, but set this false for objects supplied
    /// by the declared base manifest.
    included: bool = true,
};

pub const Manifest = struct {
    schema_version: u32 = manifest_schema_version,
    /// Logical identities are carried in the root manifest rather than
    /// inferred from a filename. Empty values are reserved for embedded/Lite
    /// exports that are intentionally rebound by the importer.
    backup_id: []const u8 = "",
    table_name: []const u8 = "",
    representation: Representation,
    mode: SnapshotMode = .full,
    /// Required for deltas and forbidden for full snapshots. This identifies
    /// the complete parent manifest, never a mutable ref name.
    parent_manifest_sha256: ?[]const u8 = null,
    created_at_unix_ns: i128 = 0,
    compatibility: Compatibility = .{},
    compression: Compression = .none,
    encryption: ?Encryption = null,
    /// Complete materialized inventory. In delta mode only descriptors marked
    /// `included` have blob records in this container; restore resolves the
    /// others from the exact parent manifest before atomic publication.
    objects: []const ObjectDescriptor = &.{},
};

pub fn encodeManifestAlloc(alloc: std.mem.Allocator, manifest: Manifest) ![]u8 {
    try validateManifest(manifest);
    const encoded = try std.json.Stringify.valueAlloc(alloc, manifest, .{});
    errdefer alloc.free(encoded);
    if (encoded.len > max_manifest_bytes) return error.BackupManifestTooLarge;
    return encoded;
}

pub const ParsedManifest = std.json.Parsed(Manifest);

pub fn parseManifest(alloc: std.mem.Allocator, encoded: []const u8) !ParsedManifest {
    if (encoded.len == 0 or encoded.len > max_manifest_bytes) return error.BackupManifestTooLarge;
    var parsed = std.json.parseFromSlice(Manifest, alloc, encoded, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = false,
    }) catch return error.InvalidBackupManifest;
    errdefer parsed.deinit();
    try validateManifest(parsed.value);
    return parsed;
}

pub fn validateManifest(manifest: Manifest) !void {
    if (manifest.schema_version != manifest_schema_version) return error.UnsupportedBackupManifestVersion;
    if (manifest.compatibility.min_afb_reader > afb_reader_version) return error.UnsupportedBackupManifestVersion;
    if ((manifest.backup_id.len == 0) != (manifest.table_name.len == 0) or
        manifest.backup_id.len > 128 or manifest.table_name.len > 4096)
        return error.InvalidBackupManifest;
    if (manifest.backup_id.len > 0) {
        if (std.mem.eql(u8, manifest.backup_id, ".") or std.mem.eql(u8, manifest.backup_id, ".."))
            return error.InvalidBackupManifest;
        for (manifest.backup_id) |char| {
            if (!std.ascii.isAlphanumeric(char) and char != '-' and char != '_' and char != '.')
                return error.InvalidBackupManifest;
        }
    }
    switch (manifest.mode) {
        .full => if (manifest.parent_manifest_sha256 != null) return error.InvalidBackupManifest,
        .delta => {
            const parent = manifest.parent_manifest_sha256 orelse return error.InvalidBackupManifest;
            try validateSha256(parent);
        },
    }
    if (manifest.objects.len > max_objects) return error.BackupManifestTooLarge;
    var previous_path: ?[]const u8 = null;
    for (manifest.objects) |object| {
        try validateRelativePath(object.logical_path);
        if (object.role.len == 0 or object.role.len > 128)
            return error.InvalidBackupManifest;
        try validateSha256(object.sha256);
        if (previous_path) |previous| {
            if (std.mem.order(u8, previous, object.logical_path) != .lt)
                return error.NonCanonicalBackupManifest;
        }
        previous_path = object.logical_path;
        if (manifest.mode == .full and !object.included)
            return error.IncompleteBackupInventory;
    }
    if (manifest.encryption) |encryption| {
        if (encryption.algorithm.len == 0 or encryption.algorithm.len > 64 or
            encryption.key_id.len == 0 or encryption.key_id.len > 1024)
            return error.InvalidBackupManifest;
    }
}

pub fn validateSha256(value: []const u8) !void {
    if (value.len != std.crypto.hash.sha2.Sha256.digest_length * 2)
        return error.InvalidBackupDigest;
    for (value) |char| {
        if (!std.ascii.isDigit(char) and !(char >= 'a' and char <= 'f'))
            return error.InvalidBackupDigest;
    }
}

pub fn validateRelativePath(path: []const u8) !void {
    if (path.len == 0 or path.len > max_path_bytes or std.fs.path.isAbsolute(path))
        return error.InvalidBackupPath;
    var components = std.mem.splitAny(u8, path, "/\\");
    while (components.next()) |component| {
        if (component.len == 0 or std.mem.eql(u8, component, ".") or
            std.mem.eql(u8, component, "..")) return error.InvalidBackupPath;
    }
}

pub const BlobHeader = struct {
    ordinal: u32,
    logical_path: []const u8,
    role: []const u8,
    logical_size_bytes: u64,
    stored_size_bytes: u64,
    compression: Compression = .none,
    sha256: [std.crypto.hash.sha2.Sha256.digest_length]u8,
};

pub fn encodeBlobHeaderAlloc(alloc: std.mem.Allocator, header: BlobHeader) ![]u8 {
    try validateRelativePath(header.logical_path);
    if (header.role.len == 0 or header.role.len > 128)
        return error.InvalidBackupManifest;
    const fixed = 4 + 4 + 4 + 8 + 8 + 1 + header.sha256.len;
    const variable = std.math.add(usize, header.logical_path.len, header.role.len) catch return error.BackupBlockTooLarge;
    const total = std.math.add(usize, fixed, variable) catch return error.BackupBlockTooLarge;
    var out = try alloc.alloc(u8, total);
    var pos: usize = 0;
    std.mem.writeInt(u32, out[pos..][0..4], header.ordinal, .little);
    pos += 4;
    std.mem.writeInt(u32, out[pos..][0..4], @intCast(header.logical_path.len), .little);
    pos += 4;
    @memcpy(out[pos..][0..header.logical_path.len], header.logical_path);
    pos += header.logical_path.len;
    std.mem.writeInt(u32, out[pos..][0..4], @intCast(header.role.len), .little);
    pos += 4;
    @memcpy(out[pos..][0..header.role.len], header.role);
    pos += header.role.len;
    std.mem.writeInt(u64, out[pos..][0..8], header.logical_size_bytes, .little);
    pos += 8;
    std.mem.writeInt(u64, out[pos..][0..8], header.stored_size_bytes, .little);
    pos += 8;
    out[pos] = @intFromEnum(header.compression);
    pos += 1;
    @memcpy(out[pos..][0..header.sha256.len], &header.sha256);
    return out;
}

pub const DecodedBlobHeader = struct {
    ordinal: u32,
    logical_path: []u8,
    role: []u8,
    logical_size_bytes: u64,
    stored_size_bytes: u64,
    compression: Compression,
    sha256: [std.crypto.hash.sha2.Sha256.digest_length]u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.logical_path);
        alloc.free(self.role);
        self.* = undefined;
    }
};

pub fn decodeBlobHeader(alloc: std.mem.Allocator, encoded: []const u8) !DecodedBlobHeader {
    const fixed_without_strings = 4 + 4 + 4 + 8 + 8 + 1 + std.crypto.hash.sha2.Sha256.digest_length;
    if (encoded.len < fixed_without_strings) return error.InvalidNativeFileHeader;
    const path_len = std.mem.readInt(u32, encoded[4..8], .little);
    if (path_len == 0 or path_len > max_path_bytes or encoded.len < 8 + path_len + 4)
        return error.InvalidNativeFileHeader;
    const path = try alloc.dupe(u8, encoded[8..][0..path_len]);
    errdefer alloc.free(path);
    try validateRelativePath(path);
    var pos: usize = 8 + path_len;
    const role_len = std.mem.readInt(u32, encoded[pos..][0..4], .little);
    pos += 4;
    if (role_len == 0 or role_len > 128 or encoded.len != fixed_without_strings + path_len + role_len)
        return error.InvalidNativeFileHeader;
    const role = try alloc.dupe(u8, encoded[pos..][0..role_len]);
    errdefer alloc.free(role);
    pos += role_len;
    const logical_size_bytes = std.mem.readInt(u64, encoded[pos..][0..8], .little);
    pos += 8;
    const stored_size_bytes = std.mem.readInt(u64, encoded[pos..][0..8], .little);
    pos += 8;
    const compression = std.enums.fromInt(Compression, encoded[pos]) orelse return error.InvalidNativeFileHeader;
    pos += 1;
    return .{
        .ordinal = std.mem.readInt(u32, encoded[0..4], .little),
        .logical_path = path,
        .role = role,
        .logical_size_bytes = logical_size_bytes,
        .stored_size_bytes = stored_size_bytes,
        .compression = compression,
        .sha256 = encoded[pos..][0..std.crypto.hash.sha2.Sha256.digest_length].*,
    };
}

pub const BlobChunk = struct {
    ordinal: u32,
    offset: u64,
    bytes: []const u8,
};

pub fn encodeBlobChunkAlloc(alloc: std.mem.Allocator, chunk: BlobChunk) ![]u8 {
    if (chunk.bytes.len == 0 or chunk.bytes.len > native_chunk_target_bytes)
        return error.BackupBlockTooLarge;
    const out = try alloc.alloc(u8, 12 + chunk.bytes.len);
    std.mem.writeInt(u32, out[0..4], chunk.ordinal, .little);
    std.mem.writeInt(u64, out[4..12], chunk.offset, .little);
    @memcpy(out[12..], chunk.bytes);
    return out;
}

pub fn decodeBlobChunk(encoded: []const u8) !BlobChunk {
    if (encoded.len <= 12 or encoded.len > 12 + native_chunk_target_bytes)
        return error.InvalidNativeFileChunk;
    return .{
        .ordinal = std.mem.readInt(u32, encoded[0..4], .little),
        .offset = std.mem.readInt(u64, encoded[4..12], .little),
        .bytes = encoded[12..],
    };
}

pub const FooterIndexEntry = struct {
    sha256: [std.crypto.hash.sha2.Sha256.digest_length]u8,
    header_offset: u64,
    stored_size_bytes: u64,
};

pub fn encodeFooterIndexAlloc(alloc: std.mem.Allocator, entries: []const FooterIndexEntry) ![]u8 {
    if (entries.len > max_objects) return error.BackupManifestTooLarge;
    const entry_size = std.crypto.hash.sha2.Sha256.digest_length + 8 + 8;
    const payload_size = std.math.mul(usize, entries.len, entry_size) catch return error.BackupBlockTooLarge;
    const out = try alloc.alloc(u8, 4 + payload_size);
    std.mem.writeInt(u32, out[0..4], @intCast(entries.len), .little);
    var pos: usize = 4;
    for (entries) |entry| {
        @memcpy(out[pos..][0..entry.sha256.len], &entry.sha256);
        pos += entry.sha256.len;
        std.mem.writeInt(u64, out[pos..][0..8], entry.header_offset, .little);
        pos += 8;
        std.mem.writeInt(u64, out[pos..][0..8], entry.stored_size_bytes, .little);
        pos += 8;
    }
    return out;
}

pub fn decodeFooterIndexAlloc(alloc: std.mem.Allocator, encoded: []const u8) ![]FooterIndexEntry {
    if (encoded.len < 4) return error.InvalidBundleFooter;
    const count = std.mem.readInt(u32, encoded[0..4], .little);
    if (count > max_objects) return error.InvalidBundleFooter;
    const entry_size = std.crypto.hash.sha2.Sha256.digest_length + 8 + 8;
    if (encoded.len != 4 + @as(usize, count) * entry_size) return error.InvalidBundleFooter;
    const entries = try alloc.alloc(FooterIndexEntry, count);
    var pos: usize = 4;
    for (entries) |*entry| {
        entry.* = .{
            .sha256 = encoded[pos..][0..std.crypto.hash.sha2.Sha256.digest_length].*,
            .header_offset = std.mem.readInt(u64, encoded[pos + std.crypto.hash.sha2.Sha256.digest_length ..][0..8], .little),
            .stored_size_bytes = std.mem.readInt(u64, encoded[pos + std.crypto.hash.sha2.Sha256.digest_length + 8 ..][0..8], .little),
        };
        pos += entry_size;
    }
    return entries;
}

test "AFB2 manifest separates representation from snapshot mode" {
    const alloc = std.testing.allocator;
    const encoded = try encodeManifestAlloc(alloc, .{
        .representation = .native,
        .mode = .delta,
        .parent_manifest_sha256 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        .objects = &.{.{
            .logical_path = "indexes/dense/segment-1",
            .role = "dense_projection",
            .size_bytes = 42,
            .sha256 = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
        }},
    });
    defer alloc.free(encoded);
    var parsed = try parseManifest(alloc, encoded);
    defer parsed.deinit();
    try std.testing.expectEqual(Representation.native, parsed.value.representation);
    try std.testing.expectEqual(SnapshotMode.delta, parsed.value.mode);
    try std.testing.expectEqual(@as(usize, 1), parsed.value.objects.len);
}

test "AFB2 manifest rejects ambiguous delta and traversal paths" {
    try std.testing.expectError(error.InvalidBackupManifest, validateManifest(.{
        .representation = .portable,
        .mode = .delta,
    }));
    try std.testing.expectError(error.InvalidBackupPath, validateRelativePath("indexes/../store"));
    try std.testing.expectError(error.InvalidBackupPath, validateRelativePath("/absolute"));
}
