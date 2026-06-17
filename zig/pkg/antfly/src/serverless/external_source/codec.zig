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
const external_source = @import("types.zig");

const magic = "AFXS";
const version: u32 = 1;

pub fn encodeAlloc(alloc: Allocator, inventory: external_source.Inventory) ![]u8 {
    try inventory.validate();

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);

    try out.appendSlice(alloc, magic);
    try appendU32(alloc, &out, version);
    try out.append(alloc, @intFromEnum(inventory.format));
    try appendBytes(alloc, &out, inventory.source_id);
    try appendBytes(alloc, &out, inventory.source_uri);
    try appendBytes(alloc, &out, inventory.snapshot_id);
    try appendBytes(alloc, &out, inventory.schema_fingerprint);
    try appendU32(alloc, &out, @intCast(inventory.files.len));

    for (inventory.files) |file| {
        try appendBytes(alloc, &out, file.file_id);
        try appendBytes(alloc, &out, file.object_uri);
        try appendU64(alloc, &out, file.byte_len);
        try appendU64(alloc, &out, file.row_count);
        try appendU32(alloc, &out, @intCast(file.row_groups.len));
        for (file.row_groups) |row_group| {
            try appendU32(alloc, &out, row_group.ordinal);
            try appendU64(alloc, &out, row_group.row_count);
        }
    }

    return try out.toOwnedSlice(alloc);
}

pub fn decodeAlloc(alloc: Allocator, bytes: []const u8) !external_source.Inventory {
    var cursor: usize = 0;
    if (bytes.len < magic.len + 4) return error.InvalidExternalSourceInventory;
    if (!std.mem.eql(u8, bytes[0..magic.len], magic)) return error.InvalidExternalSourceInventoryMagic;
    cursor += magic.len;
    const got_version = try readU32(bytes, &cursor);
    if (got_version != version) return error.UnsupportedExternalSourceInventoryVersion;
    if (cursor >= bytes.len) return error.InvalidExternalSourceInventory;
    const format = try decodeFormat(bytes[cursor]);
    cursor += 1;

    const source_id = try readBytesAlloc(alloc, bytes, &cursor);
    errdefer alloc.free(source_id);
    const source_uri = try readBytesAlloc(alloc, bytes, &cursor);
    errdefer alloc.free(source_uri);
    const snapshot_id = try readBytesAlloc(alloc, bytes, &cursor);
    errdefer alloc.free(snapshot_id);
    const schema_fingerprint = try readBytesAlloc(alloc, bytes, &cursor);
    errdefer alloc.free(schema_fingerprint);
    const file_count = try readU32(bytes, &cursor);

    const files = try alloc.alloc(external_source.FileEntry, file_count);
    errdefer alloc.free(files);
    var initialized_files: usize = 0;
    errdefer {
        for (files[0..initialized_files]) |*file| file.deinit(alloc);
    }

    for (files) |*file| {
        var keep_file = false;
        const file_id = try readBytesAlloc(alloc, bytes, &cursor);
        errdefer if (!keep_file) alloc.free(file_id);
        const object_uri = try readBytesAlloc(alloc, bytes, &cursor);
        errdefer if (!keep_file) alloc.free(object_uri);
        const byte_len = try readU64(bytes, &cursor);
        const row_count = try readU64(bytes, &cursor);
        const row_group_count = try readU32(bytes, &cursor);
        const row_groups = try alloc.alloc(external_source.RowGroup, row_group_count);
        errdefer if (!keep_file) alloc.free(row_groups);
        for (row_groups) |*row_group| {
            row_group.* = .{
                .ordinal = try readU32(bytes, &cursor),
                .row_count = try readU64(bytes, &cursor),
            };
        }
        file.* = .{
            .file_id = file_id,
            .object_uri = object_uri,
            .byte_len = byte_len,
            .row_count = row_count,
            .row_groups = row_groups,
        };
        keep_file = true;
        initialized_files += 1;
    }

    if (cursor != bytes.len) return error.InvalidExternalSourceInventory;

    var inventory = external_source.Inventory{
        .format = format,
        .source_id = source_id,
        .source_uri = source_uri,
        .snapshot_id = snapshot_id,
        .schema_fingerprint = schema_fingerprint,
        .files = files,
    };
    errdefer inventory.deinit(alloc);
    try inventory.validate();
    return inventory;
}

fn decodeFormat(raw: u8) !external_source.Format {
    return switch (raw) {
        1 => .parquet,
        2 => .iceberg,
        3 => .lance,
        else => error.InvalidExternalSourceInventory,
    };
}

fn appendBytes(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), bytes: []const u8) !void {
    try appendU32(alloc, out, @intCast(bytes.len));
    try out.appendSlice(alloc, bytes);
}

fn readBytesAlloc(alloc: Allocator, bytes: []const u8, cursor: *usize) ![]u8 {
    const len = try readU32(bytes, cursor);
    if (cursor.* + len > bytes.len) return error.InvalidExternalSourceInventory;
    const out = try alloc.dupe(u8, bytes[cursor.* .. cursor.* + len]);
    cursor.* += len;
    return out;
}

fn appendU32(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: u32) !void {
    var buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &buf, value, .little);
    try out.appendSlice(alloc, &buf);
}

fn appendU64(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: u64) !void {
    var buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &buf, value, .little);
    try out.appendSlice(alloc, &buf);
}

fn readU32(bytes: []const u8, cursor: *usize) !u32 {
    if (cursor.* + 4 > bytes.len) return error.InvalidExternalSourceInventory;
    const value = std.mem.readInt(u32, bytes[cursor.*..][0..4], .little);
    cursor.* += 4;
    return value;
}

fn readU64(bytes: []const u8, cursor: *usize) !u64 {
    if (cursor.* + 8 > bytes.len) return error.InvalidExternalSourceInventory;
    const value = std.mem.readInt(u64, bytes[cursor.*..][0..8], .little);
    cursor.* += 8;
    return value;
}

test "external source inventory codec round-trips file inventory" {
    const alloc = std.testing.allocator;
    var inventory = external_source.Inventory{
        .format = .iceberg,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/warehouse/events"),
        .snapshot_id = try alloc.dupe(u8, "iceberg-123"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "file-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/warehouse/events/file-a.parquet"),
        .byte_len = 1024,
        .row_count = 2,
        .row_groups = try alloc.dupe(external_source.RowGroup, &[_]external_source.RowGroup{
            .{ .ordinal = 0, .row_count = 2 },
        }),
    };

    const encoded = try encodeAlloc(alloc, inventory);
    defer alloc.free(encoded);

    var decoded = try decodeAlloc(alloc, encoded);
    defer decoded.deinit(alloc);

    try std.testing.expectEqual(external_source.Format.iceberg, decoded.format);
    try std.testing.expectEqualStrings("iceberg-123", decoded.snapshot_id);
    try std.testing.expectEqual(@as(usize, 1), decoded.files.len);
    try std.testing.expectEqualStrings("file-a.parquet", decoded.files[0].file_id);
    try std.testing.expectEqual(@as(u64, 2), decoded.files[0].row_groups[0].row_count);
}
