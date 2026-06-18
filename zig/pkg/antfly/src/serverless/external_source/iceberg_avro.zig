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

//! Iceberg Avro manifest-list planning.
//!
//! This is a deliberately small Avro Object Container File reader for the
//! Iceberg manifest-list boundary. It supports uncompressed OCF blocks and a
//! schema-driven subset of primitive/nullable Avro fields so the next planner
//! step can expand manifest-list rows into manifest files without inventing a
//! JSON-only test format.

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const ManifestContent = enum(u8) {
    data = 0,
    deletes = 1,
};

pub const ManifestListEntry = struct {
    manifest_path: []u8,
    manifest_length: u64,
    partition_spec_id: i32 = 0,
    content: ManifestContent = .data,
    sequence_number: i64 = 0,
    min_sequence_number: i64 = 0,
    added_snapshot_id: i64 = 0,
    added_files_count: u32 = 0,
    existing_files_count: u32 = 0,
    deleted_files_count: u32 = 0,
    added_rows_count: u64 = 0,
    existing_rows_count: u64 = 0,
    deleted_rows_count: u64 = 0,

    pub fn deinit(self: *ManifestListEntry, alloc: Allocator) void {
        alloc.free(self.manifest_path);
        self.* = undefined;
    }

    pub fn validate(self: ManifestListEntry) !void {
        if (self.manifest_path.len == 0) return error.InvalidIcebergManifestList;
        if (self.manifest_length == 0) return error.InvalidIcebergManifestList;
    }
};

pub const ManifestList = struct {
    entries: []ManifestListEntry,

    pub fn deinit(self: *ManifestList, alloc: Allocator) void {
        for (self.entries) |*entry| entry.deinit(alloc);
        alloc.free(self.entries);
        self.* = undefined;
    }

    pub fn validate(self: ManifestList) !void {
        for (self.entries) |entry| try entry.validate();
    }
};

pub fn parseManifestListAlloc(alloc: Allocator, avro_ocf: []const u8) !ManifestList {
    var reader = Reader.init(avro_ocf);
    try reader.expectBytes("Obj\x01");

    const schema_json = try readMetadataMapAlloc(alloc, &reader);
    defer alloc.free(schema_json);
    const sync = try reader.readSlice(16);

    const fields = try parseSchemaFieldPlansAlloc(alloc, schema_json);
    defer alloc.free(fields);

    var entries = std.ArrayListUnmanaged(ManifestListEntry).empty;
    errdefer {
        for (entries.items) |*entry| entry.deinit(alloc);
        entries.deinit(alloc);
    }

    while (!reader.eof()) {
        const block_count = try reader.readLong();
        if (block_count <= 0) return error.InvalidIcebergManifestList;
        const block_size_i64 = try reader.readLong();
        if (block_size_i64 < 0) return error.InvalidIcebergManifestList;
        const block_size = std.math.cast(usize, block_size_i64) orelse return error.InvalidIcebergManifestList;
        const block = try reader.readSlice(block_size);
        var block_reader = Reader.init(block);
        const count = std.math.cast(usize, block_count) orelse return error.InvalidIcebergManifestList;
        for (0..count) |_| {
            const entry = try readManifestListEntryAlloc(alloc, &block_reader, fields);
            try entries.append(alloc, entry);
        }
        if (!block_reader.eof()) return error.InvalidIcebergManifestList;
        const got_sync = try reader.readSlice(16);
        if (!std.mem.eql(u8, sync, got_sync)) return error.InvalidIcebergManifestList;
    }

    var list = ManifestList{ .entries = try entries.toOwnedSlice(alloc) };
    errdefer list.deinit(alloc);
    try list.validate();
    return list;
}

const Reader = struct {
    bytes: []const u8,
    offset: usize = 0,

    fn init(bytes: []const u8) Reader {
        return .{ .bytes = bytes };
    }

    fn eof(self: Reader) bool {
        return self.offset == self.bytes.len;
    }

    fn readByte(self: *Reader) !u8 {
        if (self.offset >= self.bytes.len) return error.InvalidAvroContainer;
        const byte = self.bytes[self.offset];
        self.offset += 1;
        return byte;
    }

    fn readSlice(self: *Reader, len: usize) ![]const u8 {
        if (len > self.bytes.len - self.offset) return error.InvalidAvroContainer;
        const out = self.bytes[self.offset .. self.offset + len];
        self.offset += len;
        return out;
    }

    fn expectBytes(self: *Reader, expected: []const u8) !void {
        const got = try self.readSlice(expected.len);
        if (!std.mem.eql(u8, got, expected)) return error.InvalidAvroContainer;
    }

    fn readLong(self: *Reader) !i64 {
        var raw: u64 = 0;
        var shift: u6 = 0;
        var count: usize = 0;
        while (true) {
            const byte = try self.readByte();
            count += 1;
            if (count > 10) return error.InvalidAvroContainer;
            const payload: u64 = @intCast(byte & 0x7f);
            if (shift == 63 and payload > 1) return error.InvalidAvroContainer;
            raw |= payload << shift;
            if ((byte & 0x80) == 0) break;
            if (shift > 56) return error.InvalidAvroContainer;
            shift += 7;
        }
        return decodeZigzag(raw) orelse error.InvalidAvroContainer;
    }

    fn readStringAlloc(self: *Reader, alloc: Allocator) ![]u8 {
        const len_i64 = try self.readLong();
        if (len_i64 < 0) return error.InvalidAvroContainer;
        const len = std.math.cast(usize, len_i64) orelse return error.InvalidAvroContainer;
        const bytes = try self.readSlice(len);
        return try alloc.dupe(u8, bytes);
    }

    fn skipString(self: *Reader) !void {
        const len_i64 = try self.readLong();
        if (len_i64 < 0) return error.InvalidAvroContainer;
        const len = std.math.cast(usize, len_i64) orelse return error.InvalidAvroContainer;
        _ = try self.readSlice(len);
    }

    fn readBytes(self: *Reader) ![]const u8 {
        const len_i64 = try self.readLong();
        if (len_i64 < 0) return error.InvalidAvroContainer;
        const len = std.math.cast(usize, len_i64) orelse return error.InvalidAvroContainer;
        return try self.readSlice(len);
    }
};

fn decodeZigzag(raw: u64) ?i64 {
    const half = raw >> 1;
    if (half > @as(u64, @intCast(std.math.maxInt(i64)))) return null;
    var value: i64 = @intCast(half);
    if ((raw & 1) != 0) value = ~value;
    return value;
}

fn readMetadataMapAlloc(alloc: Allocator, reader: *Reader) ![]u8 {
    var schema_json: ?[]u8 = null;
    errdefer if (schema_json) |schema| alloc.free(schema);

    while (true) {
        var block_count = try reader.readLong();
        if (block_count == 0) break;
        if (block_count < 0) {
            block_count = -block_count;
            const block_size = try reader.readLong();
            if (block_size < 0) return error.InvalidAvroContainer;
        }
        const count = std.math.cast(usize, block_count) orelse return error.InvalidAvroContainer;
        for (0..count) |_| {
            const key = try reader.readStringAlloc(alloc);
            defer alloc.free(key);
            const value = try reader.readBytes();
            if (std.mem.eql(u8, key, "avro.schema")) {
                if (schema_json != null) return error.InvalidAvroContainer;
                schema_json = try alloc.dupe(u8, value);
            } else if (std.mem.eql(u8, key, "avro.codec")) {
                if (!std.mem.eql(u8, value, "null")) return error.UnsupportedAvroCodec;
            }
        }
    }

    return schema_json orelse error.InvalidIcebergManifestList;
}

const AvroPrimitive = enum {
    string,
    int,
    long,
};

const KnownField = enum {
    unknown,
    manifest_path,
    manifest_length,
    partition_spec_id,
    content,
    sequence_number,
    min_sequence_number,
    added_snapshot_id,
    added_files_count,
    existing_files_count,
    deleted_files_count,
    added_rows_count,
    existing_rows_count,
    deleted_rows_count,
};

const FieldPlan = struct {
    known: KnownField,
    primitive: AvroPrimitive,
    nullable: bool = false,
    null_union_index: i64 = -1,
    value_union_index: i64 = -1,
};

fn parseSchemaFieldPlansAlloc(alloc: Allocator, schema_json: []const u8) ![]FieldPlan {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, schema_json, .{});
    defer parsed.deinit();
    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidIcebergManifestList,
    };
    const fields_value = root.get("fields") orelse return error.InvalidIcebergManifestList;
    const fields_array = switch (fields_value) {
        .array => |array| array,
        else => return error.InvalidIcebergManifestList,
    };

    var plans = std.ArrayListUnmanaged(FieldPlan).empty;
    errdefer plans.deinit(alloc);
    var saw_manifest_path = false;
    for (fields_array.items) |field_value| {
        const field_object = switch (field_value) {
            .object => |object| object,
            else => return error.InvalidIcebergManifestList,
        };
        const name_value = field_object.get("name") orelse return error.InvalidIcebergManifestList;
        const name = switch (name_value) {
            .string => |text| text,
            else => return error.InvalidIcebergManifestList,
        };
        const type_value = field_object.get("type") orelse return error.InvalidIcebergManifestList;
        var plan = try parseAvroType(type_value);
        plan.known = knownFieldForName(name);
        if (plan.known == .manifest_path) saw_manifest_path = true;
        try plans.append(alloc, plan);
    }
    if (!saw_manifest_path) return error.InvalidIcebergManifestList;
    return try plans.toOwnedSlice(alloc);
}

fn parseAvroType(value: std.json.Value) !FieldPlan {
    return switch (value) {
        .string => |text| .{
            .known = .unknown,
            .primitive = try primitiveForName(text),
        },
        .object => |object| blk: {
            const type_value = object.get("type") orelse return error.InvalidIcebergManifestList;
            const type_name = switch (type_value) {
                .string => |text| text,
                else => return error.InvalidIcebergManifestList,
            };
            break :blk .{
                .known = .unknown,
                .primitive = try primitiveForName(type_name),
            };
        },
        .array => |array| blk: {
            if (array.items.len == 0) return error.InvalidIcebergManifestList;
            var null_index: ?i64 = null;
            var value_index: ?i64 = null;
            var primitive: ?AvroPrimitive = null;
            for (array.items, 0..) |item, idx| {
                const item_index: i64 = @intCast(idx);
                const type_name = switch (item) {
                    .string => |text| text,
                    .object => |object| text: {
                        const type_value = object.get("type") orelse return error.InvalidIcebergManifestList;
                        break :text switch (type_value) {
                            .string => |object_type| object_type,
                            else => return error.InvalidIcebergManifestList,
                        };
                    },
                    else => return error.InvalidIcebergManifestList,
                };
                if (std.mem.eql(u8, type_name, "null")) {
                    if (null_index != null) return error.InvalidIcebergManifestList;
                    null_index = item_index;
                } else {
                    if (value_index != null) return error.InvalidIcebergManifestList;
                    value_index = item_index;
                    primitive = try primitiveForName(type_name);
                }
            }
            break :blk .{
                .known = .unknown,
                .primitive = primitive orelse return error.InvalidIcebergManifestList,
                .nullable = null_index != null,
                .null_union_index = null_index orelse -1,
                .value_union_index = value_index orelse return error.InvalidIcebergManifestList,
            };
        },
        else => error.InvalidIcebergManifestList,
    };
}

fn primitiveForName(name: []const u8) !AvroPrimitive {
    if (std.mem.eql(u8, name, "string")) return .string;
    if (std.mem.eql(u8, name, "int")) return .int;
    if (std.mem.eql(u8, name, "long")) return .long;
    return error.UnsupportedAvroManifestField;
}

fn knownFieldForName(name: []const u8) KnownField {
    inline for (@typeInfo(KnownField).@"enum".fields) |field| {
        if (std.mem.eql(u8, name, field.name)) return @enumFromInt(field.value);
    }
    return .unknown;
}

const EntryScratch = struct {
    manifest_path: ?[]u8 = null,
    manifest_length: u64 = 0,
    partition_spec_id: i32 = 0,
    content: ManifestContent = .data,
    sequence_number: i64 = 0,
    min_sequence_number: i64 = 0,
    added_snapshot_id: i64 = 0,
    added_files_count: u32 = 0,
    existing_files_count: u32 = 0,
    deleted_files_count: u32 = 0,
    added_rows_count: u64 = 0,
    existing_rows_count: u64 = 0,
    deleted_rows_count: u64 = 0,

    fn deinit(self: *EntryScratch, alloc: Allocator) void {
        if (self.manifest_path) |path| alloc.free(path);
        self.* = undefined;
    }
};

fn readManifestListEntryAlloc(alloc: Allocator, reader: *Reader, fields: []const FieldPlan) !ManifestListEntry {
    var scratch = EntryScratch{};
    errdefer scratch.deinit(alloc);

    for (fields) |field| {
        const is_null = try readUnionTagIfNeeded(reader, field);
        if (is_null) continue;
        switch (field.primitive) {
            .string => {
                if (field.known == .manifest_path) {
                    if (scratch.manifest_path != null) return error.InvalidIcebergManifestList;
                    scratch.manifest_path = try reader.readStringAlloc(alloc);
                } else {
                    try reader.skipString();
                }
            },
            .int => {
                const value = try readAvroInt(reader);
                try assignIntField(&scratch, field.known, value);
            },
            .long => {
                const value = try reader.readLong();
                try assignLongField(&scratch, field.known, value);
            },
        }
    }

    const path = scratch.manifest_path orelse return error.InvalidIcebergManifestList;
    scratch.manifest_path = null;
    var entry = ManifestListEntry{
        .manifest_path = path,
        .manifest_length = scratch.manifest_length,
        .partition_spec_id = scratch.partition_spec_id,
        .content = scratch.content,
        .sequence_number = scratch.sequence_number,
        .min_sequence_number = scratch.min_sequence_number,
        .added_snapshot_id = scratch.added_snapshot_id,
        .added_files_count = scratch.added_files_count,
        .existing_files_count = scratch.existing_files_count,
        .deleted_files_count = scratch.deleted_files_count,
        .added_rows_count = scratch.added_rows_count,
        .existing_rows_count = scratch.existing_rows_count,
        .deleted_rows_count = scratch.deleted_rows_count,
    };
    errdefer entry.deinit(alloc);
    try entry.validate();
    return entry;
}

fn readUnionTagIfNeeded(reader: *Reader, field: FieldPlan) !bool {
    if (!field.nullable) return false;
    const tag = try reader.readLong();
    if (tag == field.null_union_index) return true;
    if (tag != field.value_union_index) return error.InvalidIcebergManifestList;
    return false;
}

fn readAvroInt(reader: *Reader) !i32 {
    const value = try reader.readLong();
    return std.math.cast(i32, value) orelse error.InvalidIcebergManifestList;
}

fn assignIntField(scratch: *EntryScratch, known: KnownField, value: i32) !void {
    switch (known) {
        .partition_spec_id => scratch.partition_spec_id = value,
        .content => scratch.content = switch (value) {
            0 => .data,
            1 => .deletes,
            else => return error.InvalidIcebergManifestList,
        },
        .added_files_count => scratch.added_files_count = try nonNegativeU32(value),
        .existing_files_count => scratch.existing_files_count = try nonNegativeU32(value),
        .deleted_files_count => scratch.deleted_files_count = try nonNegativeU32(value),
        else => {},
    }
}

fn assignLongField(scratch: *EntryScratch, known: KnownField, value: i64) !void {
    switch (known) {
        .manifest_length => scratch.manifest_length = try nonNegativeU64(value),
        .sequence_number => scratch.sequence_number = value,
        .min_sequence_number => scratch.min_sequence_number = value,
        .added_snapshot_id => scratch.added_snapshot_id = value,
        .added_rows_count => scratch.added_rows_count = try nonNegativeU64(value),
        .existing_rows_count => scratch.existing_rows_count = try nonNegativeU64(value),
        .deleted_rows_count => scratch.deleted_rows_count = try nonNegativeU64(value),
        else => {},
    }
}

fn nonNegativeU32(value: i32) !u32 {
    if (value < 0) return error.InvalidIcebergManifestList;
    return @intCast(value);
}

fn nonNegativeU64(value: i64) !u64 {
    if (value < 0) return error.InvalidIcebergManifestList;
    return @intCast(value);
}

test "iceberg avro manifest-list decoder reads data and delete manifests" {
    const alloc = std.testing.allocator;
    var fixture = try buildManifestListFixture(alloc, "null", true);
    defer fixture.deinit(alloc);

    var list = try parseManifestListAlloc(alloc, fixture.items);
    defer list.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), list.entries.len);
    try std.testing.expectEqualStrings("s3://bucket/t/metadata/m0.avro", list.entries[0].manifest_path);
    try std.testing.expectEqual(@as(u64, 111), list.entries[0].manifest_length);
    try std.testing.expectEqual(@as(i32, 3), list.entries[0].partition_spec_id);
    try std.testing.expectEqual(ManifestContent.data, list.entries[0].content);
    try std.testing.expectEqual(@as(i64, 42), list.entries[0].sequence_number);
    try std.testing.expectEqual(@as(u32, 2), list.entries[0].added_files_count);
    try std.testing.expectEqual(@as(u64, 1000), list.entries[0].added_rows_count);

    try std.testing.expectEqualStrings("s3://bucket/t/metadata/d0.avro", list.entries[1].manifest_path);
    try std.testing.expectEqual(ManifestContent.deletes, list.entries[1].content);
    try std.testing.expectEqual(@as(i64, 43), list.entries[1].sequence_number);
    try std.testing.expectEqual(@as(u64, 3), list.entries[1].deleted_rows_count);
}

test "iceberg avro manifest-list decoder rejects unsupported codec" {
    const alloc = std.testing.allocator;
    var fixture = try buildManifestListFixture(alloc, "deflate", true);
    defer fixture.deinit(alloc);

    try std.testing.expectError(error.UnsupportedAvroCodec, parseManifestListAlloc(alloc, fixture.items));
}

test "iceberg avro manifest-list decoder requires manifest path field" {
    const alloc = std.testing.allocator;
    var fixture = try buildManifestListFixture(alloc, "null", false);
    defer fixture.deinit(alloc);

    try std.testing.expectError(error.InvalidIcebergManifestList, parseManifestListAlloc(alloc, fixture.items));
}

fn buildManifestListFixture(
    alloc: Allocator,
    codec: []const u8,
    include_manifest_path: bool,
) !std.ArrayListUnmanaged(u8) {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    try out.appendSlice(alloc, "Obj\x01");
    try appendLong(alloc, &out, 2);
    try appendString(alloc, &out, "avro.schema");
    try appendBytes(alloc, &out, manifestListSchema(include_manifest_path));
    try appendString(alloc, &out, "avro.codec");
    try appendBytes(alloc, &out, codec);
    try appendLong(alloc, &out, 0);
    const sync = "0123456789abcdef";
    try out.appendSlice(alloc, sync);

    var block = std.ArrayListUnmanaged(u8).empty;
    defer block.deinit(alloc);
    try appendManifestRecord(alloc, &block, "s3://bucket/t/metadata/m0.avro", .data, 42, 2, 1000, 0);
    try appendManifestRecord(alloc, &block, "s3://bucket/t/metadata/d0.avro", .deletes, 43, 0, 0, 3);

    try appendLong(alloc, &out, 2);
    try appendLong(alloc, &out, @intCast(block.items.len));
    try out.appendSlice(alloc, block.items);
    try out.appendSlice(alloc, sync);
    return out;
}

fn manifestListSchema(include_manifest_path: bool) []const u8 {
    if (include_manifest_path) {
        return
        \\{"type":"record","name":"manifest_file","fields":[
        \\{"name":"manifest_path","type":"string"},
        \\{"name":"manifest_length","type":"long"},
        \\{"name":"partition_spec_id","type":"int"},
        \\{"name":"content","type":"int"},
        \\{"name":"sequence_number","type":["null","long"]},
        \\{"name":"min_sequence_number","type":["null","long"]},
        \\{"name":"added_snapshot_id","type":"long"},
        \\{"name":"added_files_count","type":"int"},
        \\{"name":"existing_files_count","type":"int"},
        \\{"name":"deleted_files_count","type":"int"},
        \\{"name":"added_rows_count","type":"long"},
        \\{"name":"existing_rows_count","type":"long"},
        \\{"name":"deleted_rows_count","type":"long"}]}
        ;
    }
    return
    \\{"type":"record","name":"manifest_file","fields":[
    \\{"name":"manifest_length","type":"long"}]}
    ;
}

fn appendManifestRecord(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    path: []const u8,
    content: ManifestContent,
    sequence_number: i64,
    added_files_count: i32,
    added_rows_count: i64,
    deleted_rows_count: i64,
) !void {
    try appendString(alloc, out, path);
    try appendLong(alloc, out, 111);
    try appendLong(alloc, out, 3);
    try appendLong(alloc, out, @intFromEnum(content));
    try appendLong(alloc, out, 1);
    try appendLong(alloc, out, sequence_number);
    try appendLong(alloc, out, 1);
    try appendLong(alloc, out, sequence_number);
    try appendLong(alloc, out, 123);
    try appendLong(alloc, out, added_files_count);
    try appendLong(alloc, out, 1);
    try appendLong(alloc, out, if (content == .deletes) 1 else 0);
    try appendLong(alloc, out, added_rows_count);
    try appendLong(alloc, out, 200);
    try appendLong(alloc, out, deleted_rows_count);
}

fn appendBytes(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), bytes: []const u8) !void {
    try appendLong(alloc, out, @intCast(bytes.len));
    try out.appendSlice(alloc, bytes);
}

fn appendString(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), text: []const u8) !void {
    try appendBytes(alloc, out, text);
}

fn appendLong(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: i64) !void {
    const raw = encodeZigzag(value);
    var remaining = raw;
    while (remaining >= 0x80) {
        try out.append(alloc, @as(u8, @intCast(remaining & 0x7f)) | 0x80);
        remaining >>= 7;
    }
    try out.append(alloc, @intCast(remaining));
}

fn encodeZigzag(value: i64) u64 {
    if (value >= 0) return @as(u64, @intCast(value)) << 1;
    const magnitude: u64 = @intCast(-(value + 1));
    return (magnitude << 1) | 1;
}
