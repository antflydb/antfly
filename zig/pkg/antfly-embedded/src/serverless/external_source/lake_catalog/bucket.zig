// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Shared Iceberg bucket layout for native writers, compaction and scans.
const std = @import("std");
const m = @import("metadata.zig");
const avro = @import("avro_writer.zig");
const A = std.mem.Allocator;
const V = std.json.Value;
pub const Kind = enum { integer, string };
pub const Spec = struct {
    id: i32,
    source_id: i32,
    field_id: i32,
    source: []const u8,
    name: []const u8,
    kind: Kind,
    count: u32,
    fields: V,

    pub fn value(self: Spec, row: V) !?u32 {
        if (row != .object) return error.InvalidLakeRow;
        const cell = row.object.get(self.source) orelse return null;
        if (cell == .null) return null;
        const hash = switch (self.kind) {
            .integer => if (cell == .integer) hashInteger(cell.integer) else return error.InvalidLakeRow,
            .string => if (cell == .string) hashBytes(cell.string) else return error.InvalidLakeRow,
        };
        return (hash & 0x7fffffff) % self.count;
    }
    pub fn partition(self: Spec, a: A, value_: ?u32) !V {
        var result: V = .{ .object = .empty };
        try result.object.put(a, self.name, if (value_) |v| .{ .integer = v } else .null);
        return result;
    }
    pub fn entrySchema(self: Spec, a: A) ![]const u8 {
        var schema = try std.json.parseFromSliceLeaky(V, a, avro.entry_schema, .{});
        const fields = schema.object.getPtr("fields").?.array.items;
        for (fields) |*field| {
            if (!std.mem.eql(u8, field.object.get("name").?.string, "data_file")) continue;
            const file_fields = field.object.getPtr("type").?.object.getPtr("fields").?.array.items;
            for (file_fields) |*file_field| {
                if (!std.mem.eql(u8, file_field.object.get("name").?.string, "partition")) continue;
                const encoded = try std.json.Stringify.valueAlloc(a, .{.{ .name = self.name, .@"field-id" = self.field_id, .type = .{ "null", "int" }, .default = @as(?u8, null) }}, .{});
                try file_field.object.getPtr("type").?.object.put(a, "fields", try std.json.parseFromSliceLeaky(V, a, encoded, .{}));
            }
        }
        return std.json.Stringify.valueAlloc(a, schema, .{});
    }
};
pub fn hashInteger(value: i64) u32 {
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(i64, &bytes, value, .little);
    return hashBytes(&bytes);
}
pub fn hashBytes(bytes: []const u8) u32 {
    return std.hash.Murmur3_32.hashWithSeed(bytes, 0);
}
/// Only a single supported transform proves a bucket layout. Other layouts
/// remain readable and use the existing general scan/shuffle path.
pub fn parse(spec: V, schema: V) !?Spec {
    const fields = try m.get(spec, "fields");
    if (fields != .array or fields.array.items.len != 1) return null;
    const field = fields.array.items[0];
    const transform = try m.str(try m.get(field, "transform"));
    if (!std.mem.startsWith(u8, transform, "bucket[") or !std.mem.endsWith(u8, transform, "]")) return null;
    const count = std.fmt.parseInt(u32, transform[7 .. transform.len - 1], 10) catch return null;
    if (count == 0 or count > std.math.maxInt(i32)) return null;
    const source_id = try m.int(try m.get(field, "source-id"));
    for ((try m.get(schema, "fields")).array.items) |column| {
        if (try m.int(try m.get(column, "id")) != source_id) continue;
        const type_ = try m.get(column, "type");
        if (type_ != .string) return null;
        const kind: Kind = if (std.mem.eql(u8, type_.string, "int") or std.mem.eql(u8, type_.string, "long")) .integer else if (std.mem.eql(u8, type_.string, "string")) .string else return null;
        return .{ .id = std.math.cast(i32, try m.int(try m.get(spec, "spec-id"))) orelse return error.InvalidLakeMetadata, .source_id = std.math.cast(i32, source_id) orelse return error.InvalidLakeMetadata, .field_id = std.math.cast(i32, try m.int(try m.get(field, "field-id"))) orelse return error.InvalidLakeMetadata, .source = try m.str(try m.get(column, "name")), .name = try m.str(try m.get(field, "name")), .kind = kind, .count = count, .fields = fields };
    }
    return null;
}
pub fn defaultSpec(root: V, schema: V) !?Spec {
    const id = try m.int(try m.get(root, "default-spec-id"));
    for ((try m.get(root, "partition-specs")).array.items) |spec| if (try m.int(try m.get(spec, "spec-id")) == id) return parse(spec, schema);
    return error.InvalidLakeMetadata;
}
pub const Group = struct { bucket: ?u32, rows: []const V };
/// Allocate only populated buckets, even for an Iceberg spec with billions of
/// possible buckets. Sort once and preserve stable row order within each bucket.
pub fn group(a: A, spec: Spec, rows: []const V) ![]const Group {
    const Item = struct { bucket: ?u32, index: usize };
    const items = try a.alloc(Item, rows.len);
    defer a.free(items);
    for (rows, items, 0..) |row, *item, i| item.* = .{ .bucket = try spec.value(row), .index = i };
    std.mem.sort(Item, items, {}, struct {
        fn less(_: void, l: Item, r: Item) bool {
            const lb = l.bucket orelse std.math.maxInt(u32);
            const rb = r.bucket orelse std.math.maxInt(u32);
            return if (lb == rb) l.index < r.index else lb < rb;
        }
    }.less);
    var groups: std.ArrayList(Group) = .empty;
    var start: usize = 0;
    while (start < items.len) {
        var end = start + 1;
        while (end < items.len and items[end].bucket == items[start].bucket) : (end += 1) {}
        const values = try a.alloc(V, end - start);
        for (items[start..end], values) |item, *value| value.* = rows[item.index];
        try groups.append(a, .{ .bucket = items[start].bucket, .rows = values });
        start = end;
    }
    return groups.toOwnedSlice(a);
}
test "Iceberg bucket hashes preserve promotion and published vectors" {
    try std.testing.expectEqual(@as(u32, 2017239379), hashInteger(34));
    try std.testing.expectEqual(@as(u32, 1210000089), hashBytes("iceberg"));
}

/// Establish proof from every active data file in the pinned inventory, rather
/// than assuming the latest default spec describes older snapshots/files.
pub fn prove(root: V, schema: V, files: []const @import("../types.zig").FileEntry) !?Spec {
    const expected = (try defaultSpec(root, schema)) orelse return null;
    for (files) |file| {
        const id = file.partition_spec_id orelse return null;
        const actual = for ((try m.get(root, "partition-specs")).array.items) |spec| {
            if (try m.int(try m.get(spec, "spec-id")) == id) break (try parse(spec, schema)) orelse return null;
        } else return null;
        if (actual.source_id != expected.source_id or actual.kind != expected.kind or actual.count != expected.count or file.partition_field_count != 1 or file.partition_values.len > 1) return null;
        for (file.partition_values) |stored| {
            if (!std.mem.eql(u8, stored.column_id, actual.name)) return null;
            const value = std.fmt.parseInt(u32, stored.string_value, 10) catch return null;
            if (value >= actual.count) return null;
        }
    }
    return expected;
}

test "lake catalog bucket proof rejects mixed evolved specs and malformed partitions" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const root = try std.json.parseFromSliceLeaky(V, a,
        \\{"default-spec-id":1,"partition-specs":[{"spec-id":0,"fields":[]},{"spec-id":1,"fields":[{"source-id":1,"field-id":1000,"name":"b","transform":"bucket[12]"}]},{"spec-id":2,"fields":[{"source-id":1,"field-id":1001,"name":"old_b","transform":"bucket[12]"}]},{"spec-id":3,"fields":[{"source-id":1,"field-id":1002,"name":"old_b","transform":"bucket[8]"}]}]}
    , .{});
    const schema = try std.json.parseFromSliceLeaky(V, a,
        \\{"fields":[{"id":1,"name":"id","type":"long"}]}
    , .{});
    const File = @import("../types.zig").FileEntry;
    var file: File = .{ .file_id = @constCast("f"), .object_uri = @constCast("s3://b/f"), .byte_len = 1, .row_count = 1, .row_groups = &.{}, .partition_spec_id = 1, .partition_field_count = 1 };
    try std.testing.expectEqual(@as(u32, 12), (try prove(root, schema, &.{file})).?.count);
    file.partition_spec_id = 2;
    try std.testing.expect(try prove(root, schema, &.{file}) != null);
    file.partition_spec_id = 3;
    try std.testing.expect(try prove(root, schema, &.{file}) == null);
    file.partition_spec_id = 0;
    try std.testing.expect(try prove(root, schema, &.{file}) == null);
    file.partition_spec_id = 1;
    file.partition_field_count = 0;
    try std.testing.expect(try prove(root, schema, &.{file}) == null);
    file.partition_field_count = 1;
    var values = [_]@import("../types.zig").PartitionValue{.{ .column_id = @constCast("b"), .string_value = @constCast("12") }};
    file.partition_values = &values;
    try std.testing.expect(try prove(root, schema, &.{file}) == null);
    values[0].string_value = @constCast("11");
    try std.testing.expect(try prove(root, schema, &.{file}) != null);
    values[0].column_id = @constCast("other");
    try std.testing.expect(try prove(root, schema, &.{file}) == null);
}
