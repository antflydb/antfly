// Copyright 2026 Antfly, Inc.
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

const std = @import("std");
const tensor_types = @import("tensor_types.zig");
const a4b_qualification = @import("../runtime/moe/a4b_qualification.zig");

pub const magic = "GGUF";
pub const default_alignment: u64 = 32;

pub const Header = struct {
    version: u32,
    tensor_count: u64,
    metadata_count: u64,
};

pub const MetadataValueType = enum(u32) {
    u8 = 0,
    i8 = 1,
    u16 = 2,
    i16 = 3,
    u32 = 4,
    i32 = 5,
    f32 = 6,
    bool_ = 7,
    string = 8,
    array = 9,
    u64 = 10,
    i64 = 11,
    f64 = 12,
};

pub const MetadataArray = struct {
    element_type: MetadataValueType,
    values: []MetadataValue,
};

pub const MetadataValue = union(enum) {
    u8: u8,
    i8: i8,
    u16: u16,
    i16: i16,
    u32: u32,
    i32: i32,
    f32: f32,
    bool_: bool,
    string: []const u8,
    array: MetadataArray,
    u64: u64,
    i64: i64,
    f64: f64,

    pub fn deinit(self: *MetadataValue, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .string => |value| allocator.free(value),
            .array => |*arr| {
                for (arr.values) |*value| value.deinit(allocator);
                allocator.free(arr.values);
            },
            else => {},
        }
    }
};

pub const MetadataEntry = struct {
    key: []const u8,
    value: MetadataValue,

    pub fn deinit(self: *MetadataEntry, allocator: std.mem.Allocator) void {
        self.value.deinit(allocator);
        allocator.free(self.key);
    }
};

pub const TensorInfo = struct {
    name: []const u8,
    dimensions: []u64,
    tensor_type: tensor_types.TensorType,
    offset: u64,
    data_offset: u64,

    pub fn deinit(self: *TensorInfo, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        allocator.free(self.dimensions);
    }
};

pub const File = struct {
    header: Header,
    metadata: []MetadataEntry,
    tensors: []TensorInfo,
    alignment: u64,
    data_region_offset: u64,

    pub fn deinit(self: *File, allocator: std.mem.Allocator) void {
        for (self.metadata) |*entry| entry.deinit(allocator);
        allocator.free(self.metadata);
        for (self.tensors) |*tensor| tensor.deinit(allocator);
        allocator.free(self.tensors);
    }
};

pub fn parse(allocator: std.mem.Allocator, bytes: []const u8) !File {
    return parseWithOptions(allocator, bytes, .{});
}

/// Parse compatibility-relevant metadata and tensor headers without materializing
/// tokenizer vocabulary arrays. The returned file is sufficient for architecture,
/// tensor-type, shape, and required-weight inspection.
pub fn parseStructure(allocator: std.mem.Allocator, bytes: []const u8) !File {
    return parseWithOptions(allocator, bytes, .{ .skip_tokenizer_metadata = true });
}

/// Validate every tensor's encoded byte range without reading tensor payloads.
///
/// Header parsing alone cannot prove that a GGUF is safe to materialize: a
/// truncated file can contain a valid tensor table whose offsets extend beyond
/// the mapping. Keep this check separate from parsing so metadata-only callers
/// can operate on header fixtures, while artifact-opening paths can fail before
/// retaining an unsafe mapping.
pub fn validateTensorDataRanges(file: *const File, file_len: usize) !void {
    const file_len_u64: u64 = @intCast(file_len);
    // Metadata-only GGUFs have no data region to validate, and need not carry
    // alignment padding after their header.
    if (file.tensors.len > 0 and file.data_region_offset > file_len_u64)
        return error.TruncatedTensorData;

    for (file.tensors) |tensor| {
        if (tensor.offset % file.alignment != 0) return error.InvalidTensorOffset;
        // Unknown encodings are reported by backend compatibility checks. Their
        // byte span is unknowable here, so do not misclassify them as truncation.
        const byte_len = tensor_types.byteLen(tensor.tensor_type, tensor.dimensions) orelse continue;
        const data_end = std.math.add(u64, tensor.data_offset, byte_len) catch
            return error.InvalidTensorOffset;
        if (data_end > file_len_u64) return error.TruncatedTensorData;
    }
}

/// Return the encoded byte span of metadata entries whose keys share `prefix`
/// without allocating or materializing arrays. Resource admission uses this to
/// size embedded tokenizer state without treating the whole GGUF weight file as
/// tokenizer input.
pub fn encodedMetadataBytesWithPrefix(
    bytes: []const u8,
    prefix: []const u8,
) !usize {
    var cursor = Cursor{ .bytes = bytes };
    const header = try parseHeader(&cursor);
    var total: usize = 0;
    for (0..try countToUsize(header.metadata_count)) |_| {
        const entry_start = cursor.pos;
        const key = try cursor.readBorrowedString();
        const raw_type = try cursor.readInt(u32);
        const value_type = metadataValueTypeFromRaw(raw_type) orelse
            return error.UnsupportedMetadataType;
        try cursor.skipMetadataValue(value_type);
        if (std.mem.startsWith(u8, key, prefix)) {
            total = std.math.add(
                usize,
                total,
                cursor.pos - entry_start,
            ) catch return error.InvalidMetadataCount;
        }
    }
    return total;
}

/// Return the encoded GGUF header length through the tensor-info table without
/// allocating names, dimensions, or metadata arrays.
pub fn encodedHeaderBytes(bytes: []const u8) !usize {
    var cursor = Cursor{ .bytes = bytes };
    const header = try parseHeader(&cursor);
    for (0..try countToUsize(header.metadata_count)) |_| {
        _ = try cursor.readBorrowedString();
        const raw_type = try cursor.readInt(u32);
        const value_type = metadataValueTypeFromRaw(raw_type) orelse
            return error.UnsupportedMetadataType;
        try cursor.skipMetadataValue(value_type);
    }

    const tensor_count = try countToUsize(header.tensor_count);
    if (tensor_count > cursor.remainingBytes() / 24)
        return error.InvalidTensorCount;
    for (0..tensor_count) |_| {
        _ = try cursor.readBorrowedString();
        const dimension_count = try cursor.readInt(u32);
        try cursor.skipBytes(try std.math.mul(
            u64,
            dimension_count,
            @sizeOf(u64),
        ));
        try cursor.skipBytes(@sizeOf(u32) + @sizeOf(u64));
    }
    return cursor.pos;
}

const ParseOptions = struct {
    skip_tokenizer_metadata: bool = false,
};

fn parseWithOptions(allocator: std.mem.Allocator, bytes: []const u8, options: ParseOptions) !File {
    var cursor = Cursor{ .bytes = bytes };
    const header = try parseHeader(&cursor);

    const metadata_count = try countToUsize(header.metadata_count);
    // Even the smallest metadata entry has an empty key, a type tag, and a
    // one-byte value. Reject impossible counts before reserving allocator space.
    if (metadata_count > cursor.remainingBytes() / 13) return error.InvalidMetadataCount;
    var metadata = std.ArrayListUnmanaged(MetadataEntry).empty;
    errdefer {
        for (metadata.items) |*entry| entry.deinit(allocator);
        metadata.deinit(allocator);
    }
    try metadata.ensureTotalCapacityPrecise(
        allocator,
        if (options.skip_tokenizer_metadata) @min(metadata_count, 32) else metadata_count,
    );

    for (0..metadata_count) |_| {
        if (options.skip_tokenizer_metadata) {
            const key = try cursor.readBorrowedString();
            const raw_type = try cursor.readInt(u32);
            const value_type = metadataValueTypeFromRaw(raw_type) orelse return error.UnsupportedMetadataType;
            if (std.mem.startsWith(u8, key, "tokenizer.")) {
                try cursor.skipMetadataValue(value_type);
                continue;
            }

            var value = try parseMetadataValue(allocator, &cursor, value_type);
            errdefer value.deinit(allocator);
            const owned_key = try allocator.dupe(u8, key);
            errdefer allocator.free(owned_key);
            try metadata.append(allocator, .{ .key = owned_key, .value = value });
        } else {
            var entry = try parseMetadataEntry(allocator, &cursor);
            errdefer entry.deinit(allocator);
            try metadata.append(allocator, entry);
        }
    }

    const parsed_metadata = try metadata.toOwnedSlice(allocator);
    errdefer {
        for (parsed_metadata) |*entry| entry.deinit(allocator);
        allocator.free(parsed_metadata);
    }
    const alignment = readAlignment(parsed_metadata) orelse default_alignment;
    if (alignment == 0 or !std.math.isPowerOfTwo(alignment)) return error.InvalidAlignment;
    const tensor_dialect = tensorDialectFromMetadata(parsed_metadata);

    const tensor_count = try countToUsize(header.tensor_count);
    // Empty tensor name + dimension count + type + offset is at least 24 bytes.
    if (tensor_count > cursor.remainingBytes() / 24) return error.InvalidTensorCount;
    const tensors = try allocator.alloc(TensorInfo, tensor_count);
    errdefer allocator.free(tensors);
    var tensor_len: usize = 0;
    errdefer {
        for (tensors[0..tensor_len]) |*tensor| tensor.deinit(allocator);
    }

    for (0..tensor_count) |_| {
        tensors[tensor_len] = try parseTensorInfo(allocator, &cursor, alignment, tensor_dialect);
        tensor_len += 1;
    }

    const data_region_offset = try alignForward(@intCast(cursor.pos), alignment);
    for (tensors[0..tensor_len]) |*tensor| {
        tensor.data_offset = try std.math.add(u64, data_region_offset, tensor.offset);
    }

    return .{
        .header = header,
        .metadata = parsed_metadata,
        .tensors = tensors[0..tensor_len],
        .alignment = alignment,
        .data_region_offset = data_region_offset,
    };
}

pub const SupportMetadata = struct {
    architecture: ?[]const u8 = null,
    expert_count: u32 = 0,
    block_count: u32 = 0,
    embedding_length: u32 = 0,
    expert_used_count: u32 = 0,
    expert_feed_forward_length: u32 = 0,
    file_type: u32 = 0,

    /// Lightweight serving qualification for the single Gemma 4 26B-A4B
    /// artifact geometry supported by the bounded Metal runtime. The backend
    /// still validates every packed expert tensor and rejects mixed/non-Q4_0
    /// layouts before publishing a session.
    pub fn isQualifiedGemma4A4b(self: SupportMetadata) bool {
        return self.architecture != null and
            std.mem.eql(u8, self.architecture.?, a4b_qualification.architecture) and
            a4b_qualification.matchesGeometry(
                self.block_count,
                self.expert_count,
                self.expert_used_count,
                self.embedding_length,
                self.expert_feed_forward_length,
            ) and
            self.file_type == a4b_qualification.gguf_file_type;
    }
};

/// Read the small set of GGUF metadata used to choose a safe serving path, skipping every
/// other metadata value without allocating it.
///
/// `parse` materializes the whole KV table, which for a modern tokenizer means allocating
/// hundreds of thousands of token strings. Model listing only needs the architecture, and
/// doing a full parse per listed model is what made `/ai/v1/models` take seconds per call.
///
/// Any returned architecture slice is borrowed from `bytes`.
pub fn readSupportMetadata(bytes: []const u8) !SupportMetadata {
    var cursor = Cursor{ .bytes = bytes };
    const header = try parseHeader(&cursor);
    var result = SupportMetadata{};

    for (0..try countToUsize(header.metadata_count)) |_| {
        const key = try cursor.readBorrowedString();
        const raw_type = try cursor.readInt(u32);
        const value_type = metadataValueTypeFromRaw(raw_type) orelse return error.UnsupportedMetadataType;

        if (value_type == .string and std.mem.eql(u8, key, "general.architecture")) {
            result.architecture = try cursor.readBorrowedString();
        } else if (value_type == .u32 and std.mem.eql(u8, key, "general.file_type")) {
            result.file_type = try cursor.readInt(u32);
        } else if (value_type == .u32 and std.mem.endsWith(u8, key, ".expert_count")) {
            result.expert_count = try cursor.readInt(u32);
        } else if (value_type == .u32 and std.mem.endsWith(u8, key, ".block_count")) {
            result.block_count = try cursor.readInt(u32);
        } else if (value_type == .u32 and std.mem.endsWith(u8, key, ".embedding_length")) {
            result.embedding_length = try cursor.readInt(u32);
        } else if (value_type == .u32 and std.mem.endsWith(u8, key, ".expert_used_count")) {
            result.expert_used_count = try cursor.readInt(u32);
        } else if (value_type == .u32 and std.mem.endsWith(u8, key, ".expert_feed_forward_length")) {
            result.expert_feed_forward_length = try cursor.readInt(u32);
        } else {
            try cursor.skipMetadataValue(value_type);
        }
    }

    return result;
}

pub fn readArchitecture(bytes: []const u8) !?[]const u8 {
    return (try readSupportMetadata(bytes)).architecture;
}

fn parseHeader(cursor: *Cursor) !Header {
    const got_magic = try cursor.readBytes(magic.len);
    if (!std.mem.eql(u8, got_magic, magic)) return error.InvalidGgufMagic;

    return .{
        .version = try cursor.readInt(u32),
        .tensor_count = try cursor.readInt(u64),
        .metadata_count = try cursor.readInt(u64),
    };
}

fn parseMetadataEntry(allocator: std.mem.Allocator, cursor: *Cursor) !MetadataEntry {
    const key = try cursor.readOwnedString(allocator);
    errdefer allocator.free(key);

    const raw_type = try cursor.readInt(u32);
    const value_type = metadataValueTypeFromRaw(raw_type) orelse return error.UnsupportedMetadataType;
    const value = try parseMetadataValue(allocator, cursor, value_type);

    return .{ .key = key, .value = value };
}

fn parseMetadataValue(allocator: std.mem.Allocator, cursor: *Cursor, value_type: MetadataValueType) !MetadataValue {
    return switch (value_type) {
        .u8 => .{ .u8 = try cursor.readInt(u8) },
        .i8 => .{ .i8 = try cursor.readInt(i8) },
        .u16 => .{ .u16 = try cursor.readInt(u16) },
        .i16 => .{ .i16 = try cursor.readInt(i16) },
        .u32 => .{ .u32 = try cursor.readInt(u32) },
        .i32 => .{ .i32 = try cursor.readInt(i32) },
        .f32 => .{ .f32 = @bitCast(try cursor.readInt(u32)) },
        .bool_ => .{ .bool_ = (try cursor.readInt(u8)) != 0 },
        .string => .{ .string = try cursor.readOwnedString(allocator) },
        .array => blk: {
            const raw_elem_type = try cursor.readInt(u32);
            const elem_type = metadataValueTypeFromRaw(raw_elem_type) orelse return error.UnsupportedMetadataType;
            if (elem_type == .array) return error.UnsupportedNestedMetadataArray;

            const count = try cursor.readInt(u64);
            const value_count = try countToUsize(count);
            if (value_count > cursor.remainingBytes()) return error.InvalidMetadataCount;
            const values = try allocator.alloc(MetadataValue, value_count);
            errdefer allocator.free(values);
            var len: usize = 0;
            errdefer {
                for (values[0..len]) |*value| value.deinit(allocator);
            }
            for (0..value_count) |_| {
                values[len] = try parseMetadataValue(allocator, cursor, elem_type);
                len += 1;
            }
            break :blk .{ .array = .{ .element_type = elem_type, .values = values[0..len] } };
        },
        .u64 => .{ .u64 = try cursor.readInt(u64) },
        .i64 => .{ .i64 = try cursor.readInt(i64) },
        .f64 => .{ .f64 = @bitCast(try cursor.readInt(u64)) },
    };
}

fn parseTensorInfo(allocator: std.mem.Allocator, cursor: *Cursor, alignment: u64, dialect: tensor_types.TensorType.Dialect) !TensorInfo {
    const name = try cursor.readOwnedString(allocator);
    errdefer allocator.free(name);

    const n_dimensions: usize = try countToUsize(try cursor.readInt(u32));
    if (n_dimensions > cursor.remainingBytes() / @sizeOf(u64)) return error.InvalidDimensionCount;
    const dimensions = try allocator.alloc(u64, n_dimensions);
    errdefer allocator.free(dimensions);
    for (dimensions) |*dim| dim.* = try cursor.readInt(u64);

    const tensor_type = tensor_types.TensorType.fromRawForDialect(try cursor.readInt(u32), dialect);
    const offset = try cursor.readInt(u64);

    return .{
        .name = name,
        .dimensions = dimensions,
        .tensor_type = tensor_type,
        .offset = offset,
        .data_offset = try std.math.add(u64, try alignForward(@intCast(cursor.pos), alignment), offset),
    };
}

fn tensorDialectFromMetadata(metadata: []const MetadataEntry) tensor_types.TensorType.Dialect {
    for (metadata) |entry| {
        if (!std.mem.eql(u8, entry.key, "general.architecture")) continue;
        if (entry.value != .string) return .ggml_org;
        if (std.mem.eql(u8, entry.value.string, "bitnet-b1.58")) return .bitnet;
        if (std.mem.eql(u8, entry.value.string, "bitnet")) return .bitnet;
        return .ggml_org;
    }
    return .ggml_org;
}

fn readAlignment(metadata: []const MetadataEntry) ?u64 {
    for (metadata) |entry| {
        if (!std.mem.eql(u8, entry.key, "general.alignment")) continue;
        return switch (entry.value) {
            .u32 => |value| value,
            .u64 => |value| value,
            else => null,
        };
    }
    return null;
}

fn metadataValueTypeFromRaw(raw: u32) ?MetadataValueType {
    return switch (raw) {
        @intFromEnum(MetadataValueType.u8) => .u8,
        @intFromEnum(MetadataValueType.i8) => .i8,
        @intFromEnum(MetadataValueType.u16) => .u16,
        @intFromEnum(MetadataValueType.i16) => .i16,
        @intFromEnum(MetadataValueType.u32) => .u32,
        @intFromEnum(MetadataValueType.i32) => .i32,
        @intFromEnum(MetadataValueType.f32) => .f32,
        @intFromEnum(MetadataValueType.bool_) => .bool_,
        @intFromEnum(MetadataValueType.string) => .string,
        @intFromEnum(MetadataValueType.array) => .array,
        @intFromEnum(MetadataValueType.u64) => .u64,
        @intFromEnum(MetadataValueType.i64) => .i64,
        @intFromEnum(MetadataValueType.f64) => .f64,
        else => null,
    };
}

fn alignForward(value: u64, alignment: u64) !u64 {
    if (alignment == 0 or alignment == 1) return value;
    const rem = value % alignment;
    if (rem == 0) return value;
    return std.math.add(u64, value, alignment - rem);
}

fn countToUsize(value: anytype) !usize {
    return std.math.cast(usize, value) orelse error.CountTooLarge;
}

const Cursor = struct {
    bytes: []const u8,
    pos: usize = 0,

    fn readBytes(self: *Cursor, count: usize) ![]const u8 {
        const start = self.pos;
        const end = std.math.add(usize, start, count) catch return error.UnexpectedEndOfFile;
        if (end > self.bytes.len) return error.UnexpectedEndOfFile;
        self.pos = end;
        return self.bytes[start..end];
    }

    fn readInt(self: *Cursor, comptime T: type) !T {
        const bytes = try self.readBytes(@sizeOf(T));
        return std.mem.readInt(T, bytes[0..@sizeOf(T)], .little);
    }

    fn readOwnedString(self: *Cursor, allocator: std.mem.Allocator) ![]u8 {
        const len = try self.readInt(u64);
        const bytes = try self.readBytes(try countToUsize(len));
        return allocator.dupe(u8, bytes);
    }

    fn readBorrowedString(self: *Cursor) ![]const u8 {
        const len = try self.readInt(u64);
        return self.readBytes(try countToUsize(len));
    }

    fn skipBytes(self: *Cursor, count: u64) !void {
        const count_usize = try countToUsize(count);
        const end = std.math.add(usize, self.pos, count_usize) catch return error.UnexpectedEndOfFile;
        if (end > self.bytes.len) return error.UnexpectedEndOfFile;
        self.pos = end;
    }

    fn remainingBytes(self: *const Cursor) usize {
        return self.bytes.len - self.pos;
    }

    /// Advance past one metadata value without allocating it.
    fn skipMetadataValue(self: *Cursor, value_type: MetadataValueType) !void {
        switch (value_type) {
            .u8, .i8, .bool_ => try self.skipBytes(1),
            .u16, .i16 => try self.skipBytes(2),
            .u32, .i32, .f32 => try self.skipBytes(4),
            .u64, .i64, .f64 => try self.skipBytes(8),
            .string => {
                const len = try self.readInt(u64);
                try self.skipBytes(len);
            },
            .array => {
                const raw_elem_type = try self.readInt(u32);
                const elem_type = metadataValueTypeFromRaw(raw_elem_type) orelse return error.UnsupportedMetadataType;
                if (elem_type == .array) return error.UnsupportedNestedMetadataArray;
                const count = try self.readInt(u64);
                switch (elem_type) {
                    // Fixed-width elements can be skipped in one jump.
                    .u8, .i8, .bool_ => try self.skipBytes(count),
                    .u16, .i16 => try self.skipBytes(try std.math.mul(u64, count, 2)),
                    .u32, .i32, .f32 => try self.skipBytes(try std.math.mul(u64, count, 4)),
                    .u64, .i64, .f64 => try self.skipBytes(try std.math.mul(u64, count, 8)),
                    // Strings are length-prefixed, so they must be walked individually.
                    .string => {
                        const string_count = try countToUsize(count);
                        if (string_count > self.remainingBytes() / @sizeOf(u64)) return error.InvalidMetadataCount;
                        for (0..string_count) |_| {
                            const len = try self.readInt(u64);
                            try self.skipBytes(len);
                        }
                    },
                    .array => unreachable,
                }
            },
        }
    }
};

test "readArchitecture skips past preceding metadata including token arrays" {
    const allocator = std.testing.allocator;
    var data = std.ArrayListUnmanaged(u8).empty;
    defer data.deinit(allocator);

    try data.appendSlice(allocator, magic);
    try appendLe(u32, allocator, &data, 3);
    try appendLe(u64, allocator, &data, 0);
    try appendLe(u64, allocator, &data, 9);

    // A string array, like tokenizer.ggml.tokens: the entry that makes a full parse
    // expensive, and the one readArchitecture must walk without allocating.
    try appendString(allocator, &data, "tokenizer.ggml.tokens");
    try appendLe(u32, allocator, &data, @intFromEnum(MetadataValueType.array));
    try appendLe(u32, allocator, &data, @intFromEnum(MetadataValueType.string));
    try appendLe(u64, allocator, &data, 1024);
    for (0..1024) |_| try appendString(allocator, &data, "representative-token");

    // A fixed-width array, skipped in one jump.
    try appendString(allocator, &data, "tokenizer.ggml.token_type");
    try appendLe(u32, allocator, &data, @intFromEnum(MetadataValueType.array));
    try appendLe(u32, allocator, &data, @intFromEnum(MetadataValueType.i32));
    try appendLe(u64, allocator, &data, 3);
    try appendLe(i32, allocator, &data, 1);
    try appendLe(i32, allocator, &data, 1);
    try appendLe(i32, allocator, &data, 1);

    try appendString(allocator, &data, "gemma4.block_count");
    try appendLe(u32, allocator, &data, @intFromEnum(MetadataValueType.u32));
    try appendLe(u32, allocator, &data, 30);

    try appendString(allocator, &data, "general.architecture");
    try appendLe(u32, allocator, &data, @intFromEnum(MetadataValueType.string));
    try appendString(allocator, &data, "gemma4");

    // Structural serving facts can appear after the architecture key, so the lightweight
    // reader must keep scanning rather than returning on the first match.
    try appendString(allocator, &data, "gemma4.expert_count");
    try appendLe(u32, allocator, &data, @intFromEnum(MetadataValueType.u32));
    try appendLe(u32, allocator, &data, 128);

    try appendString(allocator, &data, "gemma4.embedding_length");
    try appendLe(u32, allocator, &data, @intFromEnum(MetadataValueType.u32));
    try appendLe(u32, allocator, &data, 2816);

    try appendString(allocator, &data, "gemma4.expert_used_count");
    try appendLe(u32, allocator, &data, @intFromEnum(MetadataValueType.u32));
    try appendLe(u32, allocator, &data, 8);

    try appendString(allocator, &data, "gemma4.expert_feed_forward_length");
    try appendLe(u32, allocator, &data, @intFromEnum(MetadataValueType.u32));
    try appendLe(u32, allocator, &data, 704);

    try appendString(allocator, &data, "general.file_type");
    try appendLe(u32, allocator, &data, @intFromEnum(MetadataValueType.u32));
    try appendLe(u32, allocator, &data, 2);

    const metadata = try readSupportMetadata(data.items);
    try std.testing.expectEqualStrings("gemma4", metadata.architecture.?);
    try std.testing.expectEqual(@as(u32, 128), metadata.expert_count);
    try std.testing.expect(metadata.isQualifiedGemma4A4b());
    var unsupported = metadata;
    unsupported.expert_used_count = 4;
    try std.testing.expect(!unsupported.isQualifiedGemma4A4b());
}

test "encoded metadata prefix scan counts only matching entries" {
    const allocator = std.testing.allocator;
    var data = std.ArrayListUnmanaged(u8).empty;
    defer data.deinit(allocator);

    try data.appendSlice(allocator, magic);
    try appendLe(u32, allocator, &data, 3);
    try appendLe(u64, allocator, &data, 0);
    try appendLe(u64, allocator, &data, 3);

    const tokenizer_start = data.items.len;
    try appendString(allocator, &data, "tokenizer.ggml.tokens");
    try appendLe(u32, allocator, &data, @intFromEnum(MetadataValueType.array));
    try appendLe(u32, allocator, &data, @intFromEnum(MetadataValueType.string));
    try appendLe(u64, allocator, &data, 2);
    try appendString(allocator, &data, "hello");
    try appendString(allocator, &data, "world");
    const first_tokenizer_end = data.items.len;

    try appendString(allocator, &data, "general.architecture");
    try appendLe(u32, allocator, &data, @intFromEnum(MetadataValueType.string));
    try appendString(allocator, &data, "llama");

    const second_tokenizer_start = data.items.len;
    try appendString(allocator, &data, "tokenizer.ggml.eos_token_id");
    try appendLe(u32, allocator, &data, @intFromEnum(MetadataValueType.u32));
    try appendLe(u32, allocator, &data, 2);
    const second_tokenizer_end = data.items.len;

    try std.testing.expectEqual(
        (first_tokenizer_end - tokenizer_start) +
            (second_tokenizer_end - second_tokenizer_start),
        try encodedMetadataBytesWithPrefix(data.items, "tokenizer."),
    );
    try std.testing.expectEqual(data.items.len, try encodedHeaderBytes(data.items));
}

test "readArchitecture returns null when the key is absent" {
    const allocator = std.testing.allocator;
    var data = std.ArrayListUnmanaged(u8).empty;
    defer data.deinit(allocator);

    try data.appendSlice(allocator, magic);
    try appendLe(u32, allocator, &data, 3);
    try appendLe(u64, allocator, &data, 0);
    try appendLe(u64, allocator, &data, 1);

    try appendString(allocator, &data, "general.alignment");
    try appendLe(u32, allocator, &data, @intFromEnum(MetadataValueType.u32));
    try appendLe(u32, allocator, &data, 32);

    try std.testing.expect(try readArchitecture(data.items) == null);
}

test "parse minimal gguf file" {
    const allocator = std.testing.allocator;
    var data = std.ArrayListUnmanaged(u8).empty;
    defer data.deinit(allocator);

    try data.appendSlice(allocator, magic);
    try appendLe(u32, allocator, &data, 3);
    try appendLe(u64, allocator, &data, 1);
    try appendLe(u64, allocator, &data, 2);

    try appendString(allocator, &data, "general.architecture");
    try appendLe(u32, allocator, &data, @intFromEnum(MetadataValueType.string));
    try appendString(allocator, &data, "llama");

    try appendString(allocator, &data, "general.alignment");
    try appendLe(u32, allocator, &data, @intFromEnum(MetadataValueType.u32));
    try appendLe(u32, allocator, &data, 64);

    try appendString(allocator, &data, "tok_embeddings.weight");
    try appendLe(u32, allocator, &data, 2);
    try appendLe(u64, allocator, &data, 8);
    try appendLe(u64, allocator, &data, 4);
    try appendLe(u32, allocator, &data, 1);
    try appendLe(u64, allocator, &data, 0);

    while (data.items.len % 64 != 0) try data.append(allocator, 0);
    try data.appendNTimes(allocator, 0, 64);

    var parsed = try parse(allocator, data.items);
    defer parsed.deinit(allocator);

    try std.testing.expectEqual(@as(u32, 3), parsed.header.version);
    try std.testing.expectEqual(@as(u64, 1), parsed.header.tensor_count);
    try std.testing.expectEqual(@as(u64, 64), parsed.alignment);
    try std.testing.expectEqual(@as(usize, 2), parsed.metadata.len);
    try std.testing.expectEqualStrings("tok_embeddings.weight", parsed.tensors[0].name);
    try std.testing.expectEqual(@as(u64, 192), parsed.data_region_offset);
    try std.testing.expectEqual(@as(u64, 192), parsed.tensors[0].data_offset);
}

test "parseStructure skips tokenizer payloads and retains tensor headers" {
    const allocator = std.testing.allocator;
    var data = std.ArrayListUnmanaged(u8).empty;
    defer data.deinit(allocator);

    try data.appendSlice(allocator, magic);
    try appendLe(u32, allocator, &data, 3);
    try appendLe(u64, allocator, &data, 1);
    try appendLe(u64, allocator, &data, 3);

    try appendString(allocator, &data, "tokenizer.ggml.tokens");
    try appendLe(u32, allocator, &data, @intFromEnum(MetadataValueType.array));
    try appendLe(u32, allocator, &data, @intFromEnum(MetadataValueType.string));
    try appendLe(u64, allocator, &data, 3);
    try appendString(allocator, &data, "<bos>");
    try appendString(allocator, &data, "hello");
    try appendString(allocator, &data, "world");

    try appendString(allocator, &data, "general.architecture");
    try appendLe(u32, allocator, &data, @intFromEnum(MetadataValueType.string));
    try appendString(allocator, &data, "llama");
    try appendString(allocator, &data, "general.alignment");
    try appendLe(u32, allocator, &data, @intFromEnum(MetadataValueType.u32));
    try appendLe(u32, allocator, &data, 32);

    try appendString(allocator, &data, "token_embd.weight");
    try appendLe(u32, allocator, &data, 2);
    try appendLe(u64, allocator, &data, 8);
    try appendLe(u64, allocator, &data, 4);
    try appendLe(u32, allocator, &data, 1);
    try appendLe(u64, allocator, &data, 0);

    // The structural parser must not scale allocator use with vocabulary size.
    // A full parse needs well over this buffer just for 1,024 MetadataValue slots.
    var fixed_buffer: [4096]u8 = undefined;
    var fixed = std.heap.FixedBufferAllocator.init(&fixed_buffer);
    const structural_allocator = fixed.allocator();
    var parsed = try parseStructure(structural_allocator, data.items);
    defer parsed.deinit(structural_allocator);

    try std.testing.expectEqual(@as(u64, 3), parsed.header.metadata_count);
    try std.testing.expectEqual(@as(usize, 2), parsed.metadata.len);
    try std.testing.expectEqualStrings("general.architecture", parsed.metadata[0].key);
    try std.testing.expectEqualStrings("token_embd.weight", parsed.tensors[0].name);
}

test "metadata skipping rejects overflowing array lengths" {
    const allocator = std.testing.allocator;
    var data = std.ArrayListUnmanaged(u8).empty;
    defer data.deinit(allocator);

    try data.appendSlice(allocator, magic);
    try appendLe(u32, allocator, &data, 3);
    try appendLe(u64, allocator, &data, 0);
    try appendLe(u64, allocator, &data, 1);
    try appendString(allocator, &data, "tokenizer.ggml.token_type");
    try appendLe(u32, allocator, &data, @intFromEnum(MetadataValueType.array));
    try appendLe(u32, allocator, &data, @intFromEnum(MetadataValueType.u64));
    try appendLe(u64, allocator, &data, std.math.maxInt(u64));

    try std.testing.expectError(error.Overflow, readSupportMetadata(data.items));
}

test "parse tensor type 39 by gguf architecture dialect" {
    const allocator = std.testing.allocator;

    var llama = try buildSingleTensorGguf(allocator, "llama", 39);
    defer llama.deinit(allocator);
    var parsed_llama = try parse(allocator, llama.items);
    defer parsed_llama.deinit(allocator);
    try std.testing.expectEqualStrings("MXFP4", parsed_llama.tensors[0].tensor_type.name());

    var bitnet = try buildSingleTensorGguf(allocator, "bitnet-b1.58", 39);
    defer bitnet.deinit(allocator);
    var parsed_bitnet = try parse(allocator, bitnet.items);
    defer parsed_bitnet.deinit(allocator);
    try std.testing.expectEqualStrings("TL2", parsed_bitnet.tensors[0].tensor_type.name());
    try std.testing.expectEqual(@as(u32, 39), parsed_bitnet.tensors[0].tensor_type.raw());
}

test "tensor data range validation rejects truncated payloads" {
    const allocator = std.testing.allocator;

    // The fixture contains 128 payload bytes, while its 256x4 F32 tensor
    // contract requires 4,096 bytes.
    var truncated = try buildSingleTensorGguf(
        allocator,
        "llama",
        @intFromEnum(tensor_types.KnownTensorType.F32),
    );
    defer truncated.deinit(allocator);
    var parsed = try parseStructure(allocator, truncated.items);
    defer parsed.deinit(allocator);

    try std.testing.expectError(
        error.TruncatedTensorData,
        validateTensorDataRanges(&parsed, truncated.items.len),
    );
}

fn buildSingleTensorGguf(allocator: std.mem.Allocator, architecture: []const u8, raw_tensor_type: u32) !std.ArrayListUnmanaged(u8) {
    var data = std.ArrayListUnmanaged(u8).empty;
    errdefer data.deinit(allocator);

    try data.appendSlice(allocator, magic);
    try appendLe(u32, allocator, &data, 3);
    try appendLe(u64, allocator, &data, 1);
    try appendLe(u64, allocator, &data, 1);

    try appendString(allocator, &data, "general.architecture");
    try appendLe(u32, allocator, &data, @intFromEnum(MetadataValueType.string));
    try appendString(allocator, &data, architecture);

    try appendString(allocator, &data, "test.weight");
    try appendLe(u32, allocator, &data, 2);
    try appendLe(u64, allocator, &data, 256);
    try appendLe(u64, allocator, &data, 4);
    try appendLe(u32, allocator, &data, raw_tensor_type);
    try appendLe(u64, allocator, &data, 0);

    while (data.items.len % default_alignment != 0) try data.append(allocator, 0);
    try data.appendNTimes(allocator, 0, 128);
    return data;
}

fn appendLe(comptime T: type, allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), value: T) !void {
    var buf: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &buf, value, .little);
    try data.appendSlice(allocator, &buf);
}

fn appendString(allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    try appendLe(u64, allocator, data, value.len);
    try data.appendSlice(allocator, value);
}
