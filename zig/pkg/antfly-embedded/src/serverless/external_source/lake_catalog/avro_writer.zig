// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Native Avro OCF encoder for Iceberg v2 manifests. Field IDs and record
//! layouts match the public Iceberg specification; no Python runtime required.
const std = @import("std");
const A = std.mem.Allocator;
const V = std.json.Value;
pub const Metadata = struct { key: []const u8, value: []const u8 };
const Buffer = struct {
    a: A,
    bytes: std.ArrayList(u8) = .empty,
    fn raw(b: *Buffer, value: []const u8) !void {
        try b.bytes.appendSlice(b.a, value);
    }
    fn long(b: *Buffer, value: i64) !void {
        var n: u64 = @as(u64, @bitCast(value << 1)) ^ @as(u64, @bitCast(value >> 63));
        while (n >= 128) : (n >>= 7) try b.bytes.append(b.a, @as(u8, @truncate(n)) | 128);
        try b.bytes.append(b.a, @intCast(n));
    }
    fn string(b: *Buffer, value: []const u8) !void {
        try b.long(@intCast(value.len));
        try b.raw(value);
    }
};
fn encode(b: *Buffer, schema: V, value: V) anyerror!void {
    if (schema == .array) { // All optional fields use null first.
        const index: usize = if (value == .null) 0 else 1;
        if (index >= schema.array.items.len) return error.InvalidLakeManifest;
        try b.long(@intCast(index));
        return encode(b, schema.array.items[index], value);
    }
    if (schema == .object) {
        const kind = schema.object.get("type") orelse return error.InvalidLakeManifest;
        if (kind != .string) return encode(b, kind, value);
        if (std.mem.eql(u8, kind.string, "record")) {
            if (value != .object) return error.InvalidLakeManifest;
            const fields = schema.object.get("fields").?.array.items;
            for (fields) |field| {
                const name = field.object.get("name").?.string;
                try encode(b, field.object.get("type").?, value.object.get(name) orelse .null);
            }
            return;
        }
        if (std.mem.eql(u8, kind.string, "array")) {
            if (value != .array) return error.InvalidLakeManifest;
            if (value.array.items.len != 0) {
                try b.long(@intCast(value.array.items.len));
                for (value.array.items) |item| try encode(b, schema.object.get("items").?, item);
            }
            return b.long(0);
        }
        return encode(b, kind, value);
    }
    if (schema != .string) return error.InvalidLakeManifest;
    if (std.mem.eql(u8, schema.string, "null")) {
        if (value != .null) return error.InvalidLakeManifest;
    } else if (std.mem.eql(u8, schema.string, "int") or std.mem.eql(u8, schema.string, "long")) {
        if (value != .integer) return error.InvalidLakeManifest;
        try b.long(value.integer);
    } else if (std.mem.eql(u8, schema.string, "string") or std.mem.eql(u8, schema.string, "bytes")) {
        if (value != .string) return error.InvalidLakeManifest;
        try b.string(value.string);
    } else if (std.mem.eql(u8, schema.string, "boolean")) {
        if (value != .bool) return error.InvalidLakeManifest;
        try b.bytes.append(b.a, @intFromBool(value.bool));
    } else return error.InvalidLakeManifest;
}
pub fn ocf(a: A, schema_json: []const u8, rows: []const V, metadata: []const Metadata) ![]u8 {
    var parsed = try std.json.parseFromSlice(V, a, schema_json, .{});
    defer parsed.deinit();
    var block: Buffer = .{ .a = a };
    defer block.bytes.deinit(a);
    for (rows) |row| try encode(&block, parsed.value, row);
    if (block.bytes.items.len > 32 * 1024 * 1024) return error.LakeWriteTooLarge;
    // Content-derived sync marker makes retried immutable uploads identical.
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(block.bytes.items, &digest, .{});
    var out: Buffer = .{ .a = a };
    errdefer out.bytes.deinit(a);
    try out.raw("Obj\x01");
    try out.long(@intCast(metadata.len + 2));
    try out.string("avro.schema");
    try out.string(schema_json);
    try out.string("avro.codec");
    try out.string("null");
    for (metadata) |item| {
        try out.string(item.key);
        try out.string(item.value);
    }
    try out.long(0);
    try out.raw(digest[0..16]);
    if (rows.len != 0) {
        try out.long(@intCast(rows.len));
        try out.long(@intCast(block.bytes.items.len));
        try out.raw(block.bytes.items);
        try out.raw(digest[0..16]);
    }
    return out.bytes.toOwnedSlice(a);
}
pub const entry_schema =
    \\{"type":"record","fields":[{"name":"status","field-id":0,"type":"int"},{"name":"snapshot_id","field-id":1,"type":["null","long"],"default":null},{"name":"sequence_number","field-id":3,"type":["null","long"],"default":null},{"name":"file_sequence_number","field-id":4,"type":["null","long"],"default":null},{"name":"data_file","field-id":2,"type":{"type":"record","fields":[{"name":"content","field-id":134,"type":"int","doc":"File format name: avro, orc, or parquet"},{"name":"file_path","field-id":100,"type":"string","doc":"Location URI with FS scheme"},{"name":"file_format","field-id":101,"type":"string","doc":"File format name: avro, orc, or parquet"},{"name":"partition","field-id":102,"type":{"type":"record","fields":[],"name":"r102"},"doc":"Partition data tuple, schema based on the partition spec"},{"name":"record_count","field-id":103,"type":"long","doc":"Number of records in the file"},{"name":"file_size_in_bytes","field-id":104,"type":"long","doc":"Total file size in bytes"},{"name":"column_sizes","field-id":108,"type":["null",{"type":"array","items":{"type":"record","name":"k117_v118","fields":[{"name":"key","type":"int","field-id":117},{"name":"value","type":"long","field-id":118}]},"logicalType":"map"}],"default":null,"doc":"Map of column id to total size on disk"},{"name":"value_counts","field-id":109,"type":["null",{"type":"array","items":{"type":"record","name":"k119_v120","fields":[{"name":"key","type":"int","field-id":119},{"name":"value","type":"long","field-id":120}]},"logicalType":"map"}],"default":null,"doc":"Map of column id to total count, including null and NaN"},{"name":"null_value_counts","field-id":110,"type":["null",{"type":"array","items":{"type":"record","name":"k121_v122","fields":[{"name":"key","type":"int","field-id":121},{"name":"value","type":"long","field-id":122}]},"logicalType":"map"}],"default":null,"doc":"Map of column id to null value count"},{"name":"nan_value_counts","field-id":137,"type":["null",{"type":"array","items":{"type":"record","name":"k138_v139","fields":[{"name":"key","type":"int","field-id":138},{"name":"value","type":"long","field-id":139}]},"logicalType":"map"}],"default":null,"doc":"Map of column id to number of NaN values in the column"},{"name":"lower_bounds","field-id":125,"type":["null",{"type":"array","items":{"type":"record","name":"k126_v127","fields":[{"name":"key","type":"int","field-id":126},{"name":"value","type":"bytes","field-id":127}]},"logicalType":"map"}],"default":null,"doc":"Map of column id to lower bound"},{"name":"upper_bounds","field-id":128,"type":["null",{"type":"array","items":{"type":"record","name":"k129_v130","fields":[{"name":"key","type":"int","field-id":129},{"name":"value","type":"bytes","field-id":130}]},"logicalType":"map"}],"default":null,"doc":"Map of column id to upper bound"},{"name":"key_metadata","field-id":131,"type":["null","bytes"],"default":null,"doc":"Encryption key metadata blob"},{"name":"split_offsets","field-id":132,"type":["null",{"type":"array","element-id":133,"items":"long"}],"default":null,"doc":"Splittable offsets"},{"name":"equality_ids","field-id":135,"type":["null",{"type":"array","element-id":136,"items":"long"}],"default":null,"doc":"Field ids used to determine row equality in equality delete files."},{"name":"sort_order_id","field-id":140,"type":["null","int"],"default":null,"doc":"ID representing sort order for this file"}],"name":"r2"}}],"name":"manifest_entry"}
;
pub const list_schema =
    \\{"type":"record","fields":[{"name":"manifest_path","field-id":500,"type":"string","doc":"Location URI with FS scheme"},{"name":"manifest_length","field-id":501,"type":"long"},{"name":"partition_spec_id","field-id":502,"type":"int"},{"name":"content","field-id":517,"type":"int"},{"name":"sequence_number","field-id":515,"type":"long"},{"name":"min_sequence_number","field-id":516,"type":"long"},{"name":"added_snapshot_id","field-id":503,"type":"long"},{"name":"added_files_count","field-id":504,"type":"int"},{"name":"existing_files_count","field-id":505,"type":"int"},{"name":"deleted_files_count","field-id":506,"type":"int"},{"name":"added_rows_count","field-id":512,"type":"long"},{"name":"existing_rows_count","field-id":513,"type":"long"},{"name":"deleted_rows_count","field-id":514,"type":"long"},{"name":"partitions","field-id":507,"type":["null",{"type":"array","element-id":508,"items":{"type":"record","fields":[{"name":"contains_null","field-id":509,"type":"boolean"},{"name":"contains_nan","field-id":518,"type":["null","boolean"],"default":null},{"name":"lower_bound","field-id":510,"type":["null","bytes"],"default":null},{"name":"upper_bound","field-id":511,"type":["null","bytes"],"default":null}],"name":"r508"}}],"default":null},{"name":"key_metadata","field-id":519,"type":["null","bytes"],"default":null}],"name":"manifest_file"}
;
