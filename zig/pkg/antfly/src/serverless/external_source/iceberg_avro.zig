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
//! Iceberg manifest-list boundary. It supports uncompressed, deflate, snappy,
//! and zstandard OCF blocks plus a schema-driven subset of primitive/nullable
//! Avro fields so the next planner step can expand manifest-list rows into
//! manifest files without inventing a JSON-only test format.

const std = @import("std");
const Allocator = std.mem.Allocator;
const snappy = @import("../../encoding/snappy.zig");

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

pub const ManifestEntryStatus = enum(u8) {
    existing = 0,
    added = 1,
    deleted = 2,
};

pub const DataFileContent = enum(u8) {
    data = 0,
    position_deletes = 1,
    equality_deletes = 2,
};

pub const DataFileEntry = struct {
    status: ManifestEntryStatus,
    snapshot_id: i64 = 0,
    data_sequence_number: i64 = 0,
    file_sequence_number: i64 = 0,
    content: DataFileContent = .data,
    file_path: []u8,
    file_format: []u8,
    partition_values: []PartitionValue = &.{},
    equality_ids: []i32 = &.{},
    record_count: u64,
    file_size_in_bytes: u64,

    pub fn deinit(self: *DataFileEntry, alloc: Allocator) void {
        alloc.free(self.file_path);
        alloc.free(self.file_format);
        for (self.partition_values) |*partition| partition.deinit(alloc);
        if (self.partition_values.len > 0) alloc.free(self.partition_values);
        if (self.equality_ids.len > 0) alloc.free(self.equality_ids);
        self.* = undefined;
    }

    pub fn validate(self: DataFileEntry) !void {
        if (self.file_path.len == 0) return error.InvalidIcebergDataManifest;
        if (self.file_format.len == 0) return error.InvalidIcebergDataManifest;
        if (!std.ascii.eqlIgnoreCase(self.file_format, "PARQUET")) return error.UnsupportedIcebergDataFileFormat;
        if (self.record_count == 0) return error.InvalidIcebergDataManifest;
        if (self.file_size_in_bytes == 0) return error.InvalidIcebergDataManifest;
        if (self.content == .equality_deletes and self.equality_ids.len == 0) return error.InvalidIcebergDataManifest;
        for (self.equality_ids, 0..) |field_id, idx| {
            if (field_id < 0) return error.InvalidIcebergDataManifest;
            for (self.equality_ids[0..idx]) |previous| {
                if (previous == field_id) return error.InvalidIcebergDataManifest;
            }
        }
        for (self.partition_values, 0..) |partition, idx| {
            try partition.validate();
            for (self.partition_values[0..idx]) |previous| {
                if (std.mem.eql(u8, previous.column_id, partition.column_id)) return error.InvalidIcebergDataManifest;
            }
        }
    }
};

pub const PartitionValue = struct {
    column_id: []u8,
    string_value: []u8,

    pub fn deinit(self: *PartitionValue, alloc: Allocator) void {
        alloc.free(self.column_id);
        alloc.free(self.string_value);
        self.* = undefined;
    }

    pub fn validate(self: PartitionValue) !void {
        if (self.column_id.len == 0) return error.InvalidIcebergDataManifest;
    }
};

pub const DataManifest = struct {
    entries: []DataFileEntry,

    pub fn deinit(self: *DataManifest, alloc: Allocator) void {
        for (self.entries) |*entry| entry.deinit(alloc);
        alloc.free(self.entries);
        self.* = undefined;
    }

    pub fn validate(self: DataManifest) !void {
        for (self.entries) |entry| try entry.validate();
    }
};

pub fn parseManifestListAlloc(alloc: Allocator, avro_ocf: []const u8) !ManifestList {
    var reader = Reader.init(avro_ocf);
    try reader.expectBytes("Obj\x01");

    var metadata = try readMetadataMapAlloc(alloc, &reader);
    defer metadata.deinit(alloc);
    const sync = try reader.readSlice(16);

    const fields = try parseSchemaFieldPlansAlloc(alloc, metadata.schema_json);
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
        const encoded_block = try reader.readSlice(block_size);
        {
            const decoded_block = try decodeBlockAlloc(alloc, metadata.codec, encoded_block);
            defer decoded_block.deinit(alloc);
            var block_reader = Reader.init(decoded_block.bytes);
            const count = std.math.cast(usize, block_count) orelse return error.InvalidIcebergManifestList;
            for (0..count) |_| {
                const entry = try readManifestListEntryAlloc(alloc, &block_reader, fields);
                try entries.append(alloc, entry);
            }
            if (!block_reader.eof()) return error.InvalidIcebergManifestList;
        }
        const got_sync = try reader.readSlice(16);
        if (!std.mem.eql(u8, sync, got_sync)) return error.InvalidIcebergManifestList;
    }

    var list = ManifestList{ .entries = try entries.toOwnedSlice(alloc) };
    errdefer list.deinit(alloc);
    try list.validate();
    return list;
}

pub fn parseDataManifestAlloc(alloc: Allocator, avro_ocf: []const u8) !DataManifest {
    var reader = Reader.init(avro_ocf);
    try reader.expectBytes("Obj\x01");

    var metadata = try readMetadataMapAlloc(alloc, &reader);
    defer metadata.deinit(alloc);
    const sync = try reader.readSlice(16);

    var parsed_schema = try std.json.parseFromSlice(std.json.Value, alloc, metadata.schema_json, .{});
    defer parsed_schema.deinit();
    try validateRecordSchema(parsed_schema.value);

    var entries = std.ArrayListUnmanaged(DataFileEntry).empty;
    errdefer {
        for (entries.items) |*entry| entry.deinit(alloc);
        entries.deinit(alloc);
    }

    while (!reader.eof()) {
        const block_count = try reader.readLong();
        if (block_count <= 0) return error.InvalidIcebergDataManifest;
        const block_size_i64 = try reader.readLong();
        if (block_size_i64 < 0) return error.InvalidIcebergDataManifest;
        const block_size = std.math.cast(usize, block_size_i64) orelse return error.InvalidIcebergDataManifest;
        const encoded_block = try reader.readSlice(block_size);
        {
            const decoded_block = try decodeBlockAlloc(alloc, metadata.codec, encoded_block);
            defer decoded_block.deinit(alloc);
            var block_reader = Reader.init(decoded_block.bytes);
            const count = std.math.cast(usize, block_count) orelse return error.InvalidIcebergDataManifest;
            for (0..count) |_| {
                const entry = try readDataManifestEntryAlloc(alloc, &block_reader, parsed_schema.value);
                try entries.append(alloc, entry);
            }
            if (!block_reader.eof()) return error.InvalidIcebergDataManifest;
        }
        const got_sync = try reader.readSlice(16);
        if (!std.mem.eql(u8, sync, got_sync)) return error.InvalidIcebergDataManifest;
    }

    var manifest = DataManifest{ .entries = try entries.toOwnedSlice(alloc) };
    errdefer manifest.deinit(alloc);
    try manifest.validate();
    return manifest;
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

    fn skipBytes(self: *Reader) !void {
        try self.skipString();
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

const AvroCodec = enum {
    null,
    deflate,
    snappy,
    zstandard,
};

const OcfMetadata = struct {
    schema_json: []u8,
    codec: AvroCodec = .null,

    fn deinit(self: *OcfMetadata, alloc: Allocator) void {
        alloc.free(self.schema_json);
        self.* = undefined;
    }
};

const DecodedBlock = struct {
    bytes: []const u8,
    owned: ?[]u8 = null,

    fn deinit(self: DecodedBlock, alloc: Allocator) void {
        if (self.owned) |owned| alloc.free(owned);
    }
};

fn readMetadataMapAlloc(alloc: Allocator, reader: *Reader) !OcfMetadata {
    var schema_json: ?[]u8 = null;
    errdefer if (schema_json) |schema| alloc.free(schema);
    var codec: AvroCodec = .null;

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
                codec = try parseAvroCodec(value);
            }
        }
    }

    return .{
        .schema_json = schema_json orelse return error.InvalidIcebergManifestList,
        .codec = codec,
    };
}

fn parseAvroCodec(value: []const u8) !AvroCodec {
    if (std.mem.eql(u8, value, "null")) return .null;
    if (std.mem.eql(u8, value, "deflate")) return .deflate;
    if (std.mem.eql(u8, value, "snappy")) return .snappy;
    if (std.mem.eql(u8, value, "zstandard")) return .zstandard;
    return error.UnsupportedAvroCodec;
}

fn decodeBlockAlloc(
    alloc: Allocator,
    codec: AvroCodec,
    encoded_block: []const u8,
) !DecodedBlock {
    return switch (codec) {
        .null => .{ .bytes = encoded_block },
        .deflate => blk: {
            var in: std.Io.Reader = .fixed(encoded_block);
            var out: std.Io.Writer.Allocating = .init(alloc);
            defer out.deinit();
            var window: [std.compress.flate.max_window_len]u8 = undefined;
            var decompress: std.compress.flate.Decompress = .init(&in, .raw, &window);
            _ = try decompress.reader.streamRemaining(&out.writer);
            const decoded = try out.toOwnedSlice();
            break :blk .{ .bytes = decoded, .owned = decoded };
        },
        .snappy => blk: {
            if (encoded_block.len < 4) return error.InvalidAvroContainer;
            const checksum_start = encoded_block.len - 4;
            const compressed = encoded_block[0..checksum_start];
            const expected_crc = std.mem.readInt(u32, encoded_block[checksum_start..][0..4], .big);
            const decoded = try snappy.decode(alloc, compressed);
            errdefer alloc.free(decoded);
            if (std.hash.Crc32.hash(decoded) != expected_crc) return error.AvroBlockChecksumMismatch;
            break :blk .{ .bytes = decoded, .owned = decoded };
        },
        .zstandard => blk: {
            var in: std.Io.Reader = .fixed(encoded_block);
            var window: [std.compress.zstd.default_window_len + std.compress.zstd.block_size_max]u8 = undefined;
            var decompress: std.compress.zstd.Decompress = .init(&in, &window, .{});
            const decoded = try decompress.reader.allocRemaining(alloc, .unlimited);
            break :blk .{ .bytes = decoded, .owned = decoded };
        },
    };
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

const DataManifestScratch = struct {
    status: ?ManifestEntryStatus = null,
    snapshot_id: i64 = 0,
    data_sequence_number: i64 = 0,
    file_sequence_number: i64 = 0,
    data_file: ?DataFileScratch = null,

    fn deinit(self: *DataManifestScratch, alloc: Allocator) void {
        if (self.data_file) |*file| file.deinit(alloc);
        self.* = undefined;
    }
};

const DataFileScratch = struct {
    content: ?DataFileContent = null,
    file_path: ?[]u8 = null,
    file_format: ?[]u8 = null,
    partition_values: []PartitionValue = &.{},
    equality_ids: []i32 = &.{},
    record_count: u64 = 0,
    file_size_in_bytes: u64 = 0,

    fn deinit(self: *DataFileScratch, alloc: Allocator) void {
        if (self.file_path) |path| alloc.free(path);
        if (self.file_format) |format| alloc.free(format);
        for (self.partition_values) |*partition| partition.deinit(alloc);
        if (self.partition_values.len > 0) alloc.free(self.partition_values);
        if (self.equality_ids.len > 0) alloc.free(self.equality_ids);
        self.* = undefined;
    }
};

fn readDataManifestEntryAlloc(alloc: Allocator, reader: *Reader, schema: std.json.Value) !DataFileEntry {
    const fields = try recordFields(schema);
    var scratch = DataManifestScratch{};
    errdefer scratch.deinit(alloc);

    for (fields.items) |field| {
        const field_object = try jsonObject(field);
        const name = try jsonRequiredString(field_object, "name");
        const field_type = field_object.get("type") orelse return error.InvalidIcebergDataManifest;
        if (std.mem.eql(u8, name, "status")) {
            scratch.status = try dataManifestStatus(try readJsonAvroInt(reader, field_type));
        } else if (std.mem.eql(u8, name, "snapshot_id")) {
            if (try readJsonAvroLongNullable(reader, field_type)) |value| scratch.snapshot_id = value;
        } else if (std.mem.eql(u8, name, "data_sequence_number")) {
            if (try readJsonAvroLongNullable(reader, field_type)) |value| scratch.data_sequence_number = value;
        } else if (std.mem.eql(u8, name, "file_sequence_number")) {
            if (try readJsonAvroLongNullable(reader, field_type)) |value| scratch.file_sequence_number = value;
        } else if (std.mem.eql(u8, name, "data_file")) {
            if (scratch.data_file != null) return error.InvalidIcebergDataManifest;
            scratch.data_file = try readDataFileRecordAlloc(alloc, reader, field_type);
        } else {
            try skipJsonAvroValue(reader, field_type);
        }
    }

    const status = scratch.status orelse return error.InvalidIcebergDataManifest;
    var data_file = scratch.data_file orelse return error.InvalidIcebergDataManifest;
    scratch.data_file = null;
    errdefer data_file.deinit(alloc);

    const file_path = data_file.file_path orelse return error.InvalidIcebergDataManifest;
    const file_format = data_file.file_format orelse return error.InvalidIcebergDataManifest;
    const content = data_file.content orelse return error.InvalidIcebergDataManifest;
    data_file.file_path = null;
    data_file.file_format = null;
    const partition_values = data_file.partition_values;
    data_file.partition_values = &.{};
    const equality_ids = data_file.equality_ids;
    data_file.equality_ids = &.{};
    var entry = DataFileEntry{
        .status = status,
        .snapshot_id = scratch.snapshot_id,
        .data_sequence_number = scratch.data_sequence_number,
        .file_sequence_number = scratch.file_sequence_number,
        .content = content,
        .file_path = file_path,
        .file_format = file_format,
        .partition_values = partition_values,
        .equality_ids = equality_ids,
        .record_count = data_file.record_count,
        .file_size_in_bytes = data_file.file_size_in_bytes,
    };
    errdefer entry.deinit(alloc);
    try entry.validate();
    return entry;
}

fn readDataFileRecordAlloc(alloc: Allocator, reader: *Reader, schema: std.json.Value) !DataFileScratch {
    const value_schema = try readJsonUnionTagForValue(reader, schema) orelse return error.InvalidIcebergDataManifest;
    const fields = try recordFields(value_schema);
    var scratch = DataFileScratch{};
    errdefer scratch.deinit(alloc);

    for (fields.items) |field| {
        const field_object = try jsonObject(field);
        const name = try jsonRequiredString(field_object, "name");
        const field_type = field_object.get("type") orelse return error.InvalidIcebergDataManifest;
        if (std.mem.eql(u8, name, "content")) {
            scratch.content = try dataFileContent(try readJsonAvroInt(reader, field_type));
        } else if (std.mem.eql(u8, name, "file_path")) {
            if (scratch.file_path != null) return error.InvalidIcebergDataManifest;
            scratch.file_path = try readJsonAvroStringAlloc(alloc, reader, field_type);
        } else if (std.mem.eql(u8, name, "file_format")) {
            if (scratch.file_format != null) return error.InvalidIcebergDataManifest;
            scratch.file_format = try readJsonAvroStringAlloc(alloc, reader, field_type);
        } else if (std.mem.eql(u8, name, "partition")) {
            if (scratch.partition_values.len != 0) return error.InvalidIcebergDataManifest;
            scratch.partition_values = try readPartitionRecordAlloc(alloc, reader, field_type);
        } else if (std.mem.eql(u8, name, "equality_ids")) {
            if (scratch.equality_ids.len != 0) return error.InvalidIcebergDataManifest;
            scratch.equality_ids = try readJsonAvroIntArrayAlloc(alloc, reader, field_type);
        } else if (std.mem.eql(u8, name, "record_count")) {
            scratch.record_count = try nonNegativeDataU64(try readJsonAvroLong(reader, field_type));
        } else if (std.mem.eql(u8, name, "file_size_in_bytes")) {
            scratch.file_size_in_bytes = try nonNegativeDataU64(try readJsonAvroLong(reader, field_type));
        } else {
            try skipJsonAvroValue(reader, field_type);
        }
    }

    return scratch;
}

fn readPartitionRecordAlloc(alloc: Allocator, reader: *Reader, schema: std.json.Value) ![]PartitionValue {
    const value_schema = try readJsonUnionTagForValue(reader, schema) orelse return &.{};
    const fields = try recordFields(value_schema);
    var partitions = std.ArrayListUnmanaged(PartitionValue).empty;
    errdefer {
        for (partitions.items) |*partition| partition.deinit(alloc);
        partitions.deinit(alloc);
    }

    for (fields.items) |field| {
        const field_object = try jsonObject(field);
        const name = try jsonRequiredString(field_object, "name");
        const field_type = field_object.get("type") orelse return error.InvalidIcebergDataManifest;
        if (try readJsonAvroPartitionStringAlloc(alloc, reader, field_type)) |value| {
            var keep = false;
            errdefer if (!keep) alloc.free(value);
            const column_id = try alloc.dupe(u8, name);
            errdefer if (!keep) alloc.free(column_id);
            try partitions.append(alloc, .{
                .column_id = column_id,
                .string_value = value,
            });
            keep = true;
        }
    }

    return try partitions.toOwnedSlice(alloc);
}

fn readJsonAvroIntArrayAlloc(alloc: Allocator, reader: *Reader, schema: std.json.Value) ![]i32 {
    const value_schema = try readJsonUnionTagForValue(reader, schema) orelse return &.{};
    const object = try jsonObject(value_schema);
    if (!jsonTypeNameEql(value_schema, "array")) return error.InvalidIcebergDataManifest;
    const item_schema = object.get("items") orelse return error.InvalidIcebergDataManifest;
    if (!jsonTypeNameEql(item_schema, "int")) return error.InvalidIcebergDataManifest;

    var values = std.ArrayListUnmanaged(i32).empty;
    errdefer values.deinit(alloc);
    while (true) {
        var block_count = try reader.readLong();
        if (block_count == 0) break;
        if (block_count < 0) {
            block_count = -block_count;
            const block_size = try reader.readLong();
            if (block_size < 0) return error.InvalidIcebergDataManifest;
        }
        const count = std.math.cast(usize, block_count) orelse return error.InvalidIcebergDataManifest;
        for (0..count) |_| {
            const value = std.math.cast(i32, try reader.readLong()) orelse return error.InvalidIcebergDataManifest;
            if (value < 0) return error.InvalidIcebergDataManifest;
            try values.append(alloc, value);
        }
    }
    return try values.toOwnedSlice(alloc);
}

fn readJsonAvroPartitionStringAlloc(alloc: Allocator, reader: *Reader, schema: std.json.Value) !?[]u8 {
    switch (schema) {
        .array => |branches| {
            const tag = try reader.readLong();
            if (tag < 0) return error.InvalidIcebergDataManifest;
            const tag_index = std.math.cast(usize, tag) orelse return error.InvalidIcebergDataManifest;
            if (tag_index >= branches.items.len) return error.InvalidIcebergDataManifest;
            const branch = branches.items[tag_index];
            if (jsonTypeNameEql(branch, "null")) return null;
            if (jsonTypeNameEql(branch, "string")) return try reader.readStringAlloc(alloc);
            try skipJsonAvroValueAfterUnionTag(reader, branch);
            return null;
        },
        else => {
            if (jsonTypeNameEql(schema, "string")) return try reader.readStringAlloc(alloc);
            try skipJsonAvroValue(reader, schema);
            return null;
        },
    }
}

fn readJsonAvroStringAlloc(alloc: Allocator, reader: *Reader, schema: std.json.Value) ![]u8 {
    const value_schema = try readJsonUnionTagForValue(reader, schema) orelse return error.InvalidIcebergDataManifest;
    if (!jsonTypeNameEql(value_schema, "string")) return error.InvalidIcebergDataManifest;
    return try reader.readStringAlloc(alloc);
}

fn readJsonAvroInt(reader: *Reader, schema: std.json.Value) !i32 {
    const value_schema = try readJsonUnionTagForValue(reader, schema) orelse return error.InvalidIcebergDataManifest;
    if (!jsonTypeNameEql(value_schema, "int")) return error.InvalidIcebergDataManifest;
    return std.math.cast(i32, try reader.readLong()) orelse error.InvalidIcebergDataManifest;
}

fn readJsonAvroLong(reader: *Reader, schema: std.json.Value) !i64 {
    const value_schema = try readJsonUnionTagForValue(reader, schema) orelse return error.InvalidIcebergDataManifest;
    if (!jsonTypeNameEql(value_schema, "long")) return error.InvalidIcebergDataManifest;
    return try reader.readLong();
}

fn readJsonAvroLongNullable(reader: *Reader, schema: std.json.Value) !?i64 {
    const value_schema = try readJsonUnionTagForValue(reader, schema) orelse return null;
    if (!jsonTypeNameEql(value_schema, "long")) return error.InvalidIcebergDataManifest;
    return try reader.readLong();
}

fn readJsonUnionTagForValue(reader: *Reader, schema: std.json.Value) !?std.json.Value {
    return switch (schema) {
        .array => |branches| {
            const tag = try reader.readLong();
            if (tag < 0) return error.InvalidIcebergDataManifest;
            const tag_index = std.math.cast(usize, tag) orelse return error.InvalidIcebergDataManifest;
            if (tag_index >= branches.items.len) return error.InvalidIcebergDataManifest;
            const branch = branches.items[tag_index];
            if (jsonTypeNameEql(branch, "null")) return null;
            return branch;
        },
        else => schema,
    };
}

const JsonAvroSkipError = error{
    InvalidAvroContainer,
    InvalidIcebergDataManifest,
    UnsupportedAvroManifestField,
};

fn skipJsonAvroValue(reader: *Reader, schema: std.json.Value) JsonAvroSkipError!void {
    const value_schema = try readJsonUnionTagForValue(reader, schema) orelse return;
    return try skipJsonAvroValueAfterUnionTag(reader, value_schema);
}

fn skipJsonAvroValueAfterUnionTag(reader: *Reader, value_schema: std.json.Value) JsonAvroSkipError!void {
    const type_name = try jsonTypeName(value_schema);
    if (std.mem.eql(u8, type_name, "null")) {
        return;
    } else if (std.mem.eql(u8, type_name, "boolean")) {
        _ = try reader.readByte();
    } else if (std.mem.eql(u8, type_name, "int") or std.mem.eql(u8, type_name, "long") or std.mem.eql(u8, type_name, "enum")) {
        _ = try reader.readLong();
    } else if (std.mem.eql(u8, type_name, "float")) {
        _ = try reader.readSlice(4);
    } else if (std.mem.eql(u8, type_name, "double")) {
        _ = try reader.readSlice(8);
    } else if (std.mem.eql(u8, type_name, "string")) {
        try reader.skipString();
    } else if (std.mem.eql(u8, type_name, "bytes")) {
        try reader.skipBytes();
    } else if (std.mem.eql(u8, type_name, "fixed")) {
        const object = try jsonObject(value_schema);
        const size = try jsonRequiredUsize(object, "size");
        _ = try reader.readSlice(size);
    } else if (std.mem.eql(u8, type_name, "record")) {
        const fields = try recordFields(value_schema);
        for (fields.items) |field| {
            const field_object = try jsonObject(field);
            const field_type = field_object.get("type") orelse return error.InvalidIcebergDataManifest;
            try skipJsonAvroValue(reader, field_type);
        }
    } else if (std.mem.eql(u8, type_name, "array")) {
        const object = try jsonObject(value_schema);
        const item_schema = object.get("items") orelse return error.InvalidIcebergDataManifest;
        while (true) {
            var block_count = try reader.readLong();
            if (block_count == 0) break;
            if (block_count < 0) {
                block_count = -block_count;
                const block_size = try reader.readLong();
                if (block_size < 0) return error.InvalidIcebergDataManifest;
            }
            const count = std.math.cast(usize, block_count) orelse return error.InvalidIcebergDataManifest;
            for (0..count) |_| try skipJsonAvroValue(reader, item_schema);
        }
    } else if (std.mem.eql(u8, type_name, "map")) {
        const object = try jsonObject(value_schema);
        const value_type = object.get("values") orelse return error.InvalidIcebergDataManifest;
        while (true) {
            var block_count = try reader.readLong();
            if (block_count == 0) break;
            if (block_count < 0) {
                block_count = -block_count;
                const block_size = try reader.readLong();
                if (block_size < 0) return error.InvalidIcebergDataManifest;
            }
            const count = std.math.cast(usize, block_count) orelse return error.InvalidIcebergDataManifest;
            for (0..count) |_| {
                try reader.skipString();
                try skipJsonAvroValue(reader, value_type);
            }
        }
    } else {
        return error.UnsupportedAvroManifestField;
    }
}

fn validateRecordSchema(schema: std.json.Value) !void {
    _ = try recordFields(schema);
}

fn recordFields(schema: std.json.Value) !std.json.Array {
    const object = try jsonObject(schema);
    if (!jsonTypeNameEql(schema, "record")) return error.InvalidIcebergDataManifest;
    const fields_value = object.get("fields") orelse return error.InvalidIcebergDataManifest;
    return switch (fields_value) {
        .array => |array| array,
        else => error.InvalidIcebergDataManifest,
    };
}

fn jsonObject(value: std.json.Value) !std.json.ObjectMap {
    return switch (value) {
        .object => |object| object,
        else => error.InvalidIcebergDataManifest,
    };
}

fn jsonRequiredString(object: std.json.ObjectMap, name: []const u8) ![]const u8 {
    const value = object.get(name) orelse return error.InvalidIcebergDataManifest;
    return switch (value) {
        .string => |text| if (text.len == 0) error.InvalidIcebergDataManifest else text,
        else => error.InvalidIcebergDataManifest,
    };
}

fn jsonRequiredUsize(object: std.json.ObjectMap, name: []const u8) !usize {
    const value = object.get(name) orelse return error.InvalidIcebergDataManifest;
    return switch (value) {
        .integer => |int_value| if (int_value >= 0) std.math.cast(usize, int_value) orelse error.InvalidIcebergDataManifest else error.InvalidIcebergDataManifest,
        else => error.InvalidIcebergDataManifest,
    };
}

fn jsonTypeName(value: std.json.Value) ![]const u8 {
    return switch (value) {
        .string => |text| text,
        .object => |object| try jsonRequiredString(object, "type"),
        else => error.InvalidIcebergDataManifest,
    };
}

fn jsonTypeNameEql(value: std.json.Value, expected: []const u8) bool {
    const got = jsonTypeName(value) catch return false;
    return std.mem.eql(u8, got, expected);
}

fn dataManifestStatus(value: i32) !ManifestEntryStatus {
    return switch (value) {
        0 => .existing,
        1 => .added,
        2 => .deleted,
        else => error.InvalidIcebergDataManifest,
    };
}

fn dataFileContent(value: i32) !DataFileContent {
    return switch (value) {
        0 => .data,
        1 => .position_deletes,
        2 => .equality_deletes,
        else => error.InvalidIcebergDataManifest,
    };
}

fn nonNegativeDataU64(value: i64) !u64 {
    if (value < 0) return error.InvalidIcebergDataManifest;
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

test "iceberg avro manifest-list decoder reads deflate blocks" {
    const alloc = std.testing.allocator;
    var fixture = try buildManifestListFixture(alloc, "deflate", true);
    defer fixture.deinit(alloc);

    var list = try parseManifestListAlloc(alloc, fixture.items);
    defer list.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), list.entries.len);
    try std.testing.expectEqualStrings("s3://bucket/t/metadata/m0.avro", list.entries[0].manifest_path);
    try std.testing.expectEqual(ManifestContent.deletes, list.entries[1].content);
}

test "iceberg avro manifest-list decoder reads snappy blocks" {
    const alloc = std.testing.allocator;
    var fixture = try buildManifestListFixture(alloc, "snappy", true);
    defer fixture.deinit(alloc);

    var list = try parseManifestListAlloc(alloc, fixture.items);
    defer list.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), list.entries.len);
    try std.testing.expectEqualStrings("s3://bucket/t/metadata/m0.avro", list.entries[0].manifest_path);
    try std.testing.expectEqual(ManifestContent.deletes, list.entries[1].content);
}

test "iceberg avro manifest-list decoder reads zstandard blocks" {
    const alloc = std.testing.allocator;
    var fixture = try buildManifestListFixture(alloc, "zstandard", true);
    defer fixture.deinit(alloc);

    var list = try parseManifestListAlloc(alloc, fixture.items);
    defer list.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), list.entries.len);
    try std.testing.expectEqualStrings("s3://bucket/t/metadata/m0.avro", list.entries[0].manifest_path);
    try std.testing.expectEqual(ManifestContent.deletes, list.entries[1].content);
}

test "iceberg avro manifest-list decoder rejects unsupported codec" {
    const alloc = std.testing.allocator;
    var fixture = try buildManifestListFixture(alloc, "bzip2", true);
    defer fixture.deinit(alloc);

    try std.testing.expectError(error.UnsupportedAvroCodec, parseManifestListAlloc(alloc, fixture.items));
}

test "iceberg avro manifest-list decoder requires manifest path field" {
    const alloc = std.testing.allocator;
    var fixture = try buildManifestListFixture(alloc, "null", false);
    defer fixture.deinit(alloc);

    try std.testing.expectError(error.InvalidIcebergManifestList, parseManifestListAlloc(alloc, fixture.items));
}

test "iceberg avro data-manifest decoder reads parquet data files" {
    const alloc = std.testing.allocator;
    var fixture = try buildDataManifestFixture(alloc, "null", "PARQUET", 7);
    defer fixture.deinit(alloc);

    var manifest = try parseDataManifestAlloc(alloc, fixture.items);
    defer manifest.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), manifest.entries.len);
    try std.testing.expectEqual(ManifestEntryStatus.added, manifest.entries[0].status);
    try std.testing.expectEqual(@as(i64, 123), manifest.entries[0].snapshot_id);
    try std.testing.expectEqual(@as(i64, 44), manifest.entries[0].data_sequence_number);
    try std.testing.expectEqual(@as(i64, 45), manifest.entries[0].file_sequence_number);
    try std.testing.expectEqual(DataFileContent.data, manifest.entries[0].content);
    try std.testing.expectEqualStrings("s3://bucket/t/data/a.parquet", manifest.entries[0].file_path);
    try std.testing.expectEqualStrings("PARQUET", manifest.entries[0].file_format);
    try std.testing.expectEqual(@as(usize, 1), manifest.entries[0].partition_values.len);
    try std.testing.expectEqualStrings("region", manifest.entries[0].partition_values[0].column_id);
    try std.testing.expectEqualStrings("us-west", manifest.entries[0].partition_values[0].string_value);
    try std.testing.expectEqual(@as(u64, 3), manifest.entries[0].record_count);
    try std.testing.expectEqual(@as(u64, 4096), manifest.entries[0].file_size_in_bytes);

    try std.testing.expectEqual(ManifestEntryStatus.deleted, manifest.entries[1].status);
    try std.testing.expectEqualStrings("s3://bucket/t/data/b.parquet", manifest.entries[1].file_path);
}

fn buildEqualityDeleteManifestFixture(
    alloc: Allocator,
    codec: []const u8,
    equality_ids: []const i32,
) !std.ArrayListUnmanaged(u8) {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, "Obj\x01");
    try appendLong(alloc, &out, 2);
    try appendString(alloc, &out, "avro.schema");
    try appendBytes(alloc, &out, equalityDeleteManifestSchema());
    try appendString(alloc, &out, "avro.codec");
    try appendBytes(alloc, &out, codec);
    try appendLong(alloc, &out, 0);
    const sync = "fedcba9876543210";
    try out.appendSlice(alloc, sync);

    var block = std.ArrayListUnmanaged(u8).empty;
    defer block.deinit(alloc);
    try appendDataManifestRecordWithContentAndEqualityIds(
        alloc,
        &block,
        .added,
        .equality_deletes,
        "s3://bucket/t/delete/eq-a.parquet",
        "PARQUET",
        1,
        1024,
        equality_ids,
    );

    const encoded_block = try encodeFixtureBlockAlloc(alloc, codec, block.items);
    defer if (encoded_block.owned) |owned| alloc.free(owned);
    try appendLong(alloc, &out, 1);
    try appendLong(alloc, &out, @intCast(encoded_block.bytes.len));
    try out.appendSlice(alloc, encoded_block.bytes);
    try out.appendSlice(alloc, sync);
    return out;
}

test "iceberg avro data-manifest decoder preserves equality delete ids" {
    const alloc = std.testing.allocator;
    var fixture = try buildEqualityDeleteManifestFixture(alloc, "null", &[_]i32{ 1, 2 });
    defer fixture.deinit(alloc);

    var manifest = try parseDataManifestAlloc(alloc, fixture.items);
    defer manifest.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), manifest.entries.len);
    try std.testing.expectEqual(ManifestEntryStatus.added, manifest.entries[0].status);
    try std.testing.expectEqual(DataFileContent.equality_deletes, manifest.entries[0].content);
    try std.testing.expectEqualStrings("s3://bucket/t/delete/eq-a.parquet", manifest.entries[0].file_path);
    try std.testing.expectEqualSlices(i32, &[_]i32{ 1, 2 }, manifest.entries[0].equality_ids);

    var empty_ids = try buildEqualityDeleteManifestFixture(alloc, "null", &.{});
    defer empty_ids.deinit(alloc);
    try std.testing.expectError(error.InvalidIcebergDataManifest, parseDataManifestAlloc(alloc, empty_ids.items));

    var duplicate_ids = try buildEqualityDeleteManifestFixture(alloc, "null", &[_]i32{ 1, 1 });
    defer duplicate_ids.deinit(alloc);
    try std.testing.expectError(error.InvalidIcebergDataManifest, parseDataManifestAlloc(alloc, duplicate_ids.items));
}

test "iceberg avro data-manifest decoder rejects unsupported data file format" {
    const alloc = std.testing.allocator;
    var fixture = try buildDataManifestFixture(alloc, "null", "ORC", 7);
    defer fixture.deinit(alloc);

    try std.testing.expectError(error.UnsupportedIcebergDataFileFormat, parseDataManifestAlloc(alloc, fixture.items));
}

test "iceberg avro data-manifest decoder reads deflate blocks" {
    const alloc = std.testing.allocator;
    var fixture = try buildDataManifestFixture(alloc, "deflate", "PARQUET", 7);
    defer fixture.deinit(alloc);

    var manifest = try parseDataManifestAlloc(alloc, fixture.items);
    defer manifest.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), manifest.entries.len);
    try std.testing.expectEqualStrings("s3://bucket/t/data/a.parquet", manifest.entries[0].file_path);
    try std.testing.expectEqualStrings("s3://bucket/t/data/b.parquet", manifest.entries[1].file_path);
}

test "iceberg avro data-manifest decoder reads snappy blocks" {
    const alloc = std.testing.allocator;
    var fixture = try buildDataManifestFixture(alloc, "snappy", "PARQUET", 7);
    defer fixture.deinit(alloc);

    var manifest = try parseDataManifestAlloc(alloc, fixture.items);
    defer manifest.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), manifest.entries.len);
    try std.testing.expectEqualStrings("s3://bucket/t/data/a.parquet", manifest.entries[0].file_path);
    try std.testing.expectEqualStrings("s3://bucket/t/data/b.parquet", manifest.entries[1].file_path);
}

test "iceberg avro data-manifest decoder rejects snappy checksum mismatch" {
    const alloc = std.testing.allocator;
    var fixture = try buildDataManifestFixture(alloc, "snappy", "PARQUET", 7);
    defer fixture.deinit(alloc);

    fixture.items[fixture.items.len - 17] ^= 0x01;
    try std.testing.expectError(error.AvroBlockChecksumMismatch, parseDataManifestAlloc(alloc, fixture.items));
}

test "iceberg avro data-manifest decoder reads zstandard blocks" {
    const alloc = std.testing.allocator;
    var fixture = try buildDataManifestFixture(alloc, "zstandard", "PARQUET", 7);
    defer fixture.deinit(alloc);

    var manifest = try parseDataManifestAlloc(alloc, fixture.items);
    defer manifest.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), manifest.entries.len);
    try std.testing.expectEqualStrings("s3://bucket/t/data/a.parquet", manifest.entries[0].file_path);
    try std.testing.expectEqualStrings("s3://bucket/t/data/b.parquet", manifest.entries[1].file_path);
}

test "iceberg avro data-manifest decoder rejects unsupported codec" {
    const alloc = std.testing.allocator;
    var fixture = try buildDataManifestFixture(alloc, "bzip2", "PARQUET", 7);
    defer fixture.deinit(alloc);

    try std.testing.expectError(error.UnsupportedAvroCodec, parseDataManifestAlloc(alloc, fixture.items));
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

    const encoded_block = try encodeFixtureBlockAlloc(alloc, codec, block.items);
    defer if (encoded_block.owned) |owned| alloc.free(owned);
    try appendLong(alloc, &out, 2);
    try appendLong(alloc, &out, @intCast(encoded_block.bytes.len));
    try out.appendSlice(alloc, encoded_block.bytes);
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

const EncodedFixtureBlock = struct {
    bytes: []const u8,
    owned: ?[]u8 = null,
};

fn encodeFixtureBlockAlloc(
    alloc: Allocator,
    codec: []const u8,
    block: []const u8,
) !EncodedFixtureBlock {
    if (std.mem.eql(u8, codec, "deflate")) {
        var out_buf: [4096]u8 = undefined;
        var out: std.Io.Writer = .fixed(&out_buf);
        var hist: [std.compress.flate.max_window_len]u8 = undefined;
        var compressor = try std.compress.flate.Compress.init(&out, hist[0..], .raw, .default);
        try compressor.writer.writeAll(block);
        try compressor.finish();
        const compressed = try alloc.dupe(u8, out.buffered());
        return .{ .bytes = compressed, .owned = compressed };
    }
    if (std.mem.eql(u8, codec, "snappy")) {
        const compressed = try snappy.encode(alloc, block);
        defer alloc.free(compressed);
        const encoded = try alloc.alloc(u8, compressed.len + 4);
        errdefer alloc.free(encoded);
        @memcpy(encoded[0..compressed.len], compressed);
        std.mem.writeInt(u32, encoded[compressed.len..][0..4], std.hash.Crc32.hash(block), .big);
        return .{ .bytes = encoded, .owned = encoded };
    }
    if (std.mem.eql(u8, codec, "zstandard")) {
        const frame = try buildRawZstdFrameAlloc(alloc, block);
        return .{ .bytes = frame, .owned = frame };
    }
    return .{ .bytes = block };
}

fn buildRawZstdFrameAlloc(alloc: Allocator, block: []const u8) ![]u8 {
    if (block.len > std.compress.zstd.block_size_max) return error.InvalidAvroContainer;
    if (block.len > std.math.maxInt(u8)) return error.InvalidAvroContainer;
    const frame = try alloc.alloc(u8, 4 + 1 + 1 + 3 + block.len);
    errdefer alloc.free(frame);

    frame[0] = 0x28;
    frame[1] = 0xb5;
    frame[2] = 0x2f;
    frame[3] = 0xfd;
    frame[4] = 0x20;
    frame[5] = @intCast(block.len);
    const block_header: u24 = 1 | (@as(u24, @intCast(block.len)) << 3);
    std.mem.writeInt(u24, frame[6..9], block_header, .little);
    @memcpy(frame[9..], block);
    return frame;
}

fn encodeZigzag(value: i64) u64 {
    if (value >= 0) return @as(u64, @intCast(value)) << 1;
    const magnitude: u64 = @intCast(-(value + 1));
    return (magnitude << 1) | 1;
}

fn buildDataManifestFixture(
    alloc: Allocator,
    codec: []const u8,
    file_format: []const u8,
    schema_variant: u8,
) !std.ArrayListUnmanaged(u8) {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    try out.appendSlice(alloc, "Obj\x01");
    try appendLong(alloc, &out, 2);
    try appendString(alloc, &out, "avro.schema");
    try appendBytes(alloc, &out, dataManifestSchema(schema_variant));
    try appendString(alloc, &out, "avro.codec");
    try appendBytes(alloc, &out, codec);
    try appendLong(alloc, &out, 0);
    const sync = "fedcba9876543210";
    try out.appendSlice(alloc, sync);

    var block = std.ArrayListUnmanaged(u8).empty;
    defer block.deinit(alloc);
    try appendDataManifestRecord(alloc, &block, .added, "s3://bucket/t/data/a.parquet", file_format, 3, 4096);
    try appendDataManifestRecord(alloc, &block, .deleted, "s3://bucket/t/data/b.parquet", file_format, 2, 2048);

    const encoded_block = try encodeFixtureBlockAlloc(alloc, codec, block.items);
    defer if (encoded_block.owned) |owned| alloc.free(owned);
    try appendLong(alloc, &out, 2);
    try appendLong(alloc, &out, @intCast(encoded_block.bytes.len));
    try out.appendSlice(alloc, encoded_block.bytes);
    try out.appendSlice(alloc, sync);
    return out;
}

fn dataManifestSchema(_: u8) []const u8 {
    return
    \\{"type":"record","name":"manifest_entry","fields":[
    \\{"name":"status","type":"int"},
    \\{"name":"snapshot_id","type":["null","long"]},
    \\{"name":"data_sequence_number","type":["null","long"]},
    \\{"name":"file_sequence_number","type":["null","long"]},
    \\{"name":"data_file","type":{"type":"record","name":"data_file","fields":[
    \\{"name":"content","type":"int"},
    \\{"name":"file_path","type":"string"},
    \\{"name":"file_format","type":"string"},
    \\{"name":"partition","type":{"type":"record","name":"partition","fields":[
    \\{"name":"region","type":"string"}]}},
    \\{"name":"record_count","type":"long"},
    \\{"name":"file_size_in_bytes","type":"long"},
    \\{"name":"column_sizes","type":{"type":"map","values":"long"}},
    \\{"name":"key_metadata","type":["null","bytes"]},
    \\{"name":"split_offsets","type":{"type":"array","items":"long"}}]}}]}
    ;
}

fn equalityDeleteManifestSchema() []const u8 {
    return
    \\{"type":"record","name":"manifest_entry","fields":[
    \\{"name":"status","type":"int"},
    \\{"name":"snapshot_id","type":["null","long"]},
    \\{"name":"data_sequence_number","type":["null","long"]},
    \\{"name":"file_sequence_number","type":["null","long"]},
    \\{"name":"data_file","type":{"type":"record","name":"data_file","fields":[
    \\{"name":"content","type":"int"},
    \\{"name":"file_path","type":"string"},
    \\{"name":"file_format","type":"string"},
    \\{"name":"partition","type":{"type":"record","name":"partition","fields":[
    \\{"name":"region","type":"string"}]}},
    \\{"name":"record_count","type":"long"},
    \\{"name":"file_size_in_bytes","type":"long"},
    \\{"name":"equality_ids","type":{"type":"array","items":"int"}},
    \\{"name":"column_sizes","type":{"type":"map","values":"long"}},
    \\{"name":"key_metadata","type":["null","bytes"]},
    \\{"name":"split_offsets","type":{"type":"array","items":"long"}}]}}]}
    ;
}

fn appendDataManifestRecord(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    status: ManifestEntryStatus,
    file_path: []const u8,
    file_format: []const u8,
    record_count: i64,
    file_size_in_bytes: i64,
) !void {
    try appendLong(alloc, out, @intFromEnum(status));
    try appendLong(alloc, out, 1);
    try appendLong(alloc, out, 123);
    try appendLong(alloc, out, 1);
    try appendLong(alloc, out, 44);
    try appendLong(alloc, out, 1);
    try appendLong(alloc, out, 45);
    try appendLong(alloc, out, @intFromEnum(DataFileContent.data));
    try appendString(alloc, out, file_path);
    try appendString(alloc, out, file_format);
    try appendString(alloc, out, "us-west");
    try appendLong(alloc, out, record_count);
    try appendLong(alloc, out, file_size_in_bytes);
    try appendMapLongs(alloc, out);
    try appendLong(alloc, out, 0);
    try appendArrayLongs(alloc, out);
}

fn appendDataManifestRecordWithContentAndEqualityIds(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    status: ManifestEntryStatus,
    content: DataFileContent,
    file_path: []const u8,
    file_format: []const u8,
    record_count: i64,
    file_size_in_bytes: i64,
    equality_ids: []const i32,
) !void {
    try appendLong(alloc, out, @intFromEnum(status));
    try appendLong(alloc, out, 1);
    try appendLong(alloc, out, 123);
    try appendLong(alloc, out, 1);
    try appendLong(alloc, out, 44);
    try appendLong(alloc, out, 1);
    try appendLong(alloc, out, 45);
    try appendLong(alloc, out, @intFromEnum(content));
    try appendString(alloc, out, file_path);
    try appendString(alloc, out, file_format);
    try appendString(alloc, out, "us-west");
    try appendLong(alloc, out, record_count);
    try appendLong(alloc, out, file_size_in_bytes);
    try appendArrayInts(alloc, out, equality_ids);
    try appendMapLongs(alloc, out);
    try appendLong(alloc, out, 0);
    try appendArrayLongs(alloc, out);
}

fn appendMapLongs(alloc: Allocator, out: *std.ArrayListUnmanaged(u8)) !void {
    try appendLong(alloc, out, 1);
    try appendString(alloc, out, "id");
    try appendLong(alloc, out, 64);
    try appendLong(alloc, out, 0);
}

fn appendArrayInts(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), values: []const i32) !void {
    if (values.len > 0) {
        try appendLong(alloc, out, @intCast(values.len));
        for (values) |value| try appendLong(alloc, out, value);
    }
    try appendLong(alloc, out, 0);
}

fn appendArrayLongs(alloc: Allocator, out: *std.ArrayListUnmanaged(u8)) !void {
    try appendLong(alloc, out, 2);
    try appendLong(alloc, out, 4);
    try appendLong(alloc, out, 2048);
    try appendLong(alloc, out, 0);
}
