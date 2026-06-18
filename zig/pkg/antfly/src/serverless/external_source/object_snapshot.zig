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

//! Deterministic snapshots for raw Parquet prefixes.
//!
//! Iceberg and Lance provide table snapshots. A raw Parquet prefix does not, so
//! Antfly pins one by hashing object keys, sizes, ETags/version IDs, and schema
//! identity from an object listing. Footer scanning can later enrich the
//! returned inventory with row-group and column-chunk metadata.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Sha256 = std.crypto.hash.sha2.Sha256;
const external_source = @import("types.zig");

pub const ListedObject = struct {
    key: []const u8,
    byte_len: u64,
    etag: []const u8 = &.{},
    version_id: []const u8 = &.{},

    pub fn validate(self: ListedObject) !void {
        if (self.key.len == 0) return error.InvalidExternalSourceSnapshot;
        if (self.byte_len == 0) return error.InvalidExternalSourceSnapshot;
        if (self.etag.len == 0 and self.version_id.len == 0) return error.InvalidExternalSourceSnapshot;
    }
};

pub fn isParquetDataObject(key: []const u8) bool {
    return std.mem.endsWith(u8, key, ".parquet");
}

pub fn planParquetPrefixInventoryAlloc(
    alloc: Allocator,
    source_id: []const u8,
    source_uri: []const u8,
    schema_fingerprint: []const u8,
    objects: []const ListedObject,
) !external_source.Inventory {
    if (source_id.len == 0) return error.InvalidExternalSourceSnapshot;
    if (source_uri.len == 0) return error.InvalidExternalSourceSnapshot;
    if (schema_fingerprint.len == 0) return error.InvalidExternalSourceSnapshot;

    var parquet_count: usize = 0;
    for (objects) |object| {
        if (!isParquetDataObject(object.key)) continue;
        try object.validate();
        parquet_count += 1;
    }
    if (parquet_count == 0) return error.EmptyExternalSourceSnapshot;

    const sorted_indexes = try alloc.alloc(usize, parquet_count);
    defer alloc.free(sorted_indexes);
    var next_index: usize = 0;
    for (objects, 0..) |object, idx| {
        if (!isParquetDataObject(object.key)) continue;
        sorted_indexes[next_index] = idx;
        next_index += 1;
    }
    std.mem.sort(usize, sorted_indexes, objects, struct {
        fn lessThan(ctx: []const ListedObject, a: usize, b: usize) bool {
            const left = ctx[a];
            const right = ctx[b];
            const key_order = std.mem.order(u8, left.key, right.key);
            if (key_order != .eq) return key_order == .lt;
            const version_order = std.mem.order(u8, left.version_id, right.version_id);
            if (version_order != .eq) return version_order == .lt;
            const etag_order = std.mem.order(u8, left.etag, right.etag);
            if (etag_order != .eq) return etag_order == .lt;
            return left.byte_len < right.byte_len;
        }
    }.lessThan);

    const snapshot_id = try snapshotDigestAlloc(alloc, source_id, source_uri, schema_fingerprint, objects, sorted_indexes);
    errdefer alloc.free(snapshot_id);
    const files = try alloc.alloc(external_source.FileEntry, parquet_count);
    errdefer alloc.free(files);
    var initialized_files: usize = 0;
    errdefer {
        for (files[0..initialized_files]) |*file| file.deinit(alloc);
    }

    for (sorted_indexes, 0..) |object_idx, out_idx| {
        const object = objects[object_idx];
        const file_id = try alloc.dupe(u8, object.key);
        errdefer alloc.free(file_id);
        const object_uri = try objectUriAlloc(alloc, source_uri, object.key);
        errdefer alloc.free(object_uri);
        const etag: []u8 = if (object.etag.len == 0) &.{} else try alloc.dupe(u8, object.etag);
        errdefer if (etag.len != 0) alloc.free(etag);
        const version_id: []u8 = if (object.version_id.len == 0) &.{} else try alloc.dupe(u8, object.version_id);
        errdefer if (version_id.len != 0) alloc.free(version_id);

        files[out_idx] = .{
            .file_id = file_id,
            .object_uri = object_uri,
            .etag = etag,
            .version_id = version_id,
            .byte_len = object.byte_len,
            .row_count = 0,
            .row_groups = &.{},
        };
        initialized_files += 1;
    }

    const source_id_copy = try alloc.dupe(u8, source_id);
    errdefer alloc.free(source_id_copy);
    const source_uri_copy = try alloc.dupe(u8, source_uri);
    errdefer alloc.free(source_uri_copy);
    const schema_fingerprint_copy = try alloc.dupe(u8, schema_fingerprint);
    errdefer alloc.free(schema_fingerprint_copy);

    var inventory = external_source.Inventory{
        .format = .parquet,
        .source_id = source_id_copy,
        .source_uri = source_uri_copy,
        .snapshot_id = snapshot_id,
        .schema_fingerprint = schema_fingerprint_copy,
        .files = files,
    };
    errdefer inventory.deinit(alloc);
    try inventory.validate();
    return inventory;
}

fn snapshotDigestAlloc(
    alloc: Allocator,
    source_id: []const u8,
    source_uri: []const u8,
    schema_fingerprint: []const u8,
    objects: []const ListedObject,
    sorted_indexes: []const usize,
) ![]u8 {
    var hasher = Sha256.init(.{});
    hashBytes(&hasher, "antfly:raw-parquet-prefix:v1");
    hashBytes(&hasher, source_id);
    hashBytes(&hasher, source_uri);
    hashBytes(&hasher, schema_fingerprint);
    for (sorted_indexes) |idx| {
        const object = objects[idx];
        hashBytes(&hasher, object.key);
        hashU64(&hasher, object.byte_len);
        hashBytes(&hasher, object.etag);
        hashBytes(&hasher, object.version_id);
    }

    var digest: [Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    return sha256IdAlloc(alloc, &digest);
}

fn hashBytes(hasher: *Sha256, bytes: []const u8) void {
    var len_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &len_buf, bytes.len, .little);
    hasher.update(&len_buf);
    hasher.update(bytes);
}

fn hashU64(hasher: *Sha256, value: u64) void {
    var buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &buf, value, .little);
    hasher.update(&buf);
}

fn sha256IdAlloc(alloc: Allocator, digest: *const [Sha256.digest_length]u8) ![]u8 {
    const prefix = "sha256:";
    const out = try alloc.alloc(u8, prefix.len + Sha256.digest_length * 2);
    @memcpy(out[0..prefix.len], prefix);
    for (digest, 0..) |byte, idx| {
        out[prefix.len + idx * 2] = hexNibble(byte >> 4);
        out[prefix.len + idx * 2 + 1] = hexNibble(byte & 0x0f);
    }
    return out;
}

fn hexNibble(value: u8) u8 {
    return if (value < 10) '0' + value else 'a' + (value - 10);
}

fn objectUriAlloc(alloc: Allocator, source_uri: []const u8, key: []const u8) ![]u8 {
    if (std.mem.endsWith(u8, source_uri, "/")) {
        return try std.fmt.allocPrint(alloc, "{s}{s}", .{ source_uri, key });
    }
    return try std.fmt.allocPrint(alloc, "{s}/{s}", .{ source_uri, key });
}

test "raw parquet prefix snapshot is deterministic across listing order" {
    const alloc = std.testing.allocator;
    const listing_a = [_]ListedObject{
        .{ .key = "part-b.parquet", .byte_len = 200, .etag = "etag-b" },
        .{ .key = "_SUCCESS", .byte_len = 1, .etag = "etag-success" },
        .{ .key = "part-a.parquet", .byte_len = 100, .version_id = "v-a" },
    };
    const listing_b = [_]ListedObject{
        .{ .key = "part-a.parquet", .byte_len = 100, .version_id = "v-a" },
        .{ .key = "part-b.parquet", .byte_len = 200, .etag = "etag-b" },
    };

    var inv_a = try planParquetPrefixInventoryAlloc(alloc, "logs", "s3://bucket/logs", "schema-v1", &listing_a);
    defer inv_a.deinit(alloc);
    var inv_b = try planParquetPrefixInventoryAlloc(alloc, "logs", "s3://bucket/logs", "schema-v1", &listing_b);
    defer inv_b.deinit(alloc);

    try std.testing.expectEqualStrings(inv_a.snapshot_id, inv_b.snapshot_id);
    try std.testing.expectEqual(@as(usize, 2), inv_a.files.len);
    try std.testing.expectEqualStrings("part-a.parquet", inv_a.files[0].file_id);
    try std.testing.expectEqualStrings("s3://bucket/logs/part-a.parquet", inv_a.files[0].object_uri);
    try std.testing.expectEqual(@as(usize, 0), inv_a.files[0].row_groups.len);
    try std.testing.expect(std.mem.startsWith(u8, inv_a.snapshot_id, "sha256:"));
}

test "raw parquet prefix snapshot changes with object version identity" {
    const alloc = std.testing.allocator;
    const listing_a = [_]ListedObject{
        .{ .key = "part-a.parquet", .byte_len = 100, .etag = "etag-a" },
    };
    const listing_b = [_]ListedObject{
        .{ .key = "part-a.parquet", .byte_len = 100, .etag = "etag-b" },
    };

    var inv_a = try planParquetPrefixInventoryAlloc(alloc, "logs", "s3://bucket/logs/", "schema-v1", &listing_a);
    defer inv_a.deinit(alloc);
    var inv_b = try planParquetPrefixInventoryAlloc(alloc, "logs", "s3://bucket/logs/", "schema-v1", &listing_b);
    defer inv_b.deinit(alloc);

    try std.testing.expect(!std.mem.eql(u8, inv_a.snapshot_id, inv_b.snapshot_id));
    try std.testing.expectEqualStrings("s3://bucket/logs/part-a.parquet", inv_a.files[0].object_uri);
}

test "raw parquet prefix snapshot rejects empty and unversioned listings" {
    const alloc = std.testing.allocator;
    const ignored = [_]ListedObject{
        .{ .key = "_SUCCESS", .byte_len = 1, .etag = "etag-success" },
    };
    try std.testing.expectError(
        error.EmptyExternalSourceSnapshot,
        planParquetPrefixInventoryAlloc(alloc, "logs", "s3://bucket/logs", "schema-v1", &ignored),
    );

    const unversioned = [_]ListedObject{
        .{ .key = "part-a.parquet", .byte_len = 100 },
    };
    try std.testing.expectError(
        error.InvalidExternalSourceSnapshot,
        planParquetPrefixInventoryAlloc(alloc, "logs", "s3://bucket/logs", "schema-v1", &unversioned),
    );
}
