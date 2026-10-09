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

//! Typed columnar doc value storage.
//!
//! Unlike the untyped doc_values.zig, this stores a type tag per column,
//! enabling SIMD-friendly packed numeric access for aggregations and
//! range queries without per-value parsing.
//!
//! Supported types:
//!   - u64: unsigned 64-bit integers
//!   - i64: signed 64-bit integers
//!   - f64: 64-bit floating point
//!   - bytes: variable-length byte strings
//!   - geo_point: packed (lat, lon) as two f64s = 16 bytes
//!   - bool: single byte (0 or 1)
//!   - numeric: per-value tagged i64/u64/f64, preserving exact integer domains
//!
//! Wire format:
//!   [value_type: u8]
//!   [numChunks: u32 LE]
//!   [chunkOffset_0: u64 LE] ... [chunkOffset_N: u64 LE]
//!   [chunk_0_data: Snappy compressed] ...
//!
//! Per chunk (uncompressed):
//!   [numDocs: u32 LE]
//!   [docIDs: u32 LE × numDocs]
//!   For fixed-size types: [values: packed × numDocs]
//!   For bytes: per doc [valueLen: u32 LE][value: bytes]

const std = @import("std");
const Allocator = std.mem.Allocator;
const snappy = @import("../encoding/snappy.zig");

pub const ValueType = enum(u8) {
    u64_val = 0,
    f64_val = 1,
    bytes_val = 2,
    geo_point = 3,
    bool_val = 4,
    i64_val = 5,
    numeric_val = 6,
    /// Signed Unix nanoseconds, fixed 16-byte little endian (wire tag v1).
    datetime_ns = 7,
};

pub const NumericValue = union(enum(u8)) {
    u64_val: u64,
    i64_val: i64,
    f64_val: f64,
};

fn reverseOrder(order: std.math.Order) std.math.Order {
    return switch (order) {
        .lt => .gt,
        .eq => .eq,
        .gt => .lt,
    };
}

fn compareI64ToF64(a: i64, b: f64) std.math.Order {
    const min_i64_f = -9223372036854775808.0;
    const max_i64_plus_one_f = 9223372036854775808.0;
    if (b < min_i64_f) return .gt;
    if (b >= max_i64_plus_one_f) return .lt;
    if (b == min_i64_f) return std.math.order(a, std.math.minInt(i64));
    const truncated: i64 = @intFromFloat(b);
    const truncated_f: f64 = @floatFromInt(truncated);
    if (b == truncated_f) return std.math.order(a, truncated);
    if (b > 0) return if (a <= truncated) .lt else .gt;
    return if (a < truncated) .lt else .gt;
}

fn compareU64ToF64(a: u64, b: f64) std.math.Order {
    if (b < 0) return .gt;
    if (b >= 18446744073709551616.0) return .lt;
    const truncated: u64 = @intFromFloat(b);
    const truncated_f: f64 = @floatFromInt(truncated);
    if (b == truncated_f) return std.math.order(a, truncated);
    return if (a <= truncated) .lt else .gt;
}

pub fn compareNumericValues(a: NumericValue, b: NumericValue) std.math.Order {
    return switch (a) {
        .i64_val => |av| switch (b) {
            .i64_val => |bv| std.math.order(av, bv),
            .u64_val => |bv| if (av < 0) .lt else std.math.order(@as(u64, @intCast(av)), bv),
            .f64_val => |bv| compareI64ToF64(av, bv),
        },
        .u64_val => |av| switch (b) {
            .i64_val => |bv| if (bv < 0) .gt else std.math.order(av, @as(u64, @intCast(bv))),
            .u64_val => |bv| std.math.order(av, bv),
            .f64_val => |bv| compareU64ToF64(av, bv),
        },
        .f64_val => |av| switch (b) {
            .i64_val => |bv| reverseOrder(compareI64ToF64(bv, av)),
            .u64_val => |bv| reverseOrder(compareU64ToF64(bv, av)),
            .f64_val => |bv| std.math.order(av, bv),
        },
    };
}

pub fn numericValueAsF64(value: NumericValue) f64 {
    return switch (value) {
        .u64_val => |number| @floatFromInt(number),
        .i64_val => |number| @floatFromInt(number),
        .f64_val => |number| number,
    };
}

pub const GeoPoint = struct {
    lat: f64,
    lon: f64,
};

pub const TypedValue = union(enum) {
    u64_val: u64,
    i64_val: i64,
    f64_val: f64,
    bytes_val: []const u8,
    geo_point: GeoPoint,
    bool_val: bool,
    numeric_val: NumericValue,
    datetime_ns: i128,
};

fn typedValueMatchesValueType(value: TypedValue, value_type: ValueType) bool {
    return switch (value_type) {
        .u64_val => value == .u64_val,
        .i64_val => value == .i64_val,
        .f64_val => value == .f64_val,
        .bytes_val => value == .bytes_val,
        .geo_point => value == .geo_point,
        .bool_val => value == .bool_val,
        .numeric_val => value == .numeric_val,
        .datetime_ns => value == .datetime_ns,
    };
}

fn typedValueIsSerializable(value: TypedValue) bool {
    return switch (value) {
        .f64_val => |v| std.math.isFinite(v),
        .geo_point => |v| std.math.isFinite(v.lat) and std.math.isFinite(v.lon),
        .numeric_val => |v| switch (v) {
            .f64_val => |number| std.math.isFinite(number),
            else => true,
        },
        else => true,
    };
}

fn decodeSerializableF64(raw: [8]u8) !f64 {
    const value: f64 = @bitCast(raw);
    if (!std.math.isFinite(value)) return error.InvalidData;
    return value;
}

fn decodeSerializableBool(raw: u8) !bool {
    return switch (raw) {
        0 => false,
        1 => true,
        else => error.InvalidData,
    };
}

fn parseValueType(raw: u8) !ValueType {
    return switch (raw) {
        @backingInt(ValueType.u64_val) => .u64_val,
        @backingInt(ValueType.f64_val) => .f64_val,
        @backingInt(ValueType.bytes_val) => .bytes_val,
        @backingInt(ValueType.geo_point) => .geo_point,
        @backingInt(ValueType.bool_val) => .bool_val,
        @backingInt(ValueType.i64_val) => .i64_val,
        @backingInt(ValueType.numeric_val) => .numeric_val,
        @backingInt(ValueType.datetime_ns) => .datetime_ns,
        else => error.InvalidData,
    };
}

fn valuesStartForDocIds(chunk_data: []const u8, num_docs: u32) !usize {
    const num_docs_usize: usize = @intCast(num_docs);
    if (num_docs_usize > (std.math.maxInt(usize) - 4) / 4) return error.InvalidData;
    const values_start = 4 + num_docs_usize * 4;
    if (values_start > chunk_data.len) return error.InvalidData;
    return values_start;
}

fn fixedValueSpanStart(chunk_data: []const u8, num_docs: u32, value_width: usize) !usize {
    const values_start = try valuesStartForDocIds(chunk_data, num_docs);
    const num_docs_usize: usize = @intCast(num_docs);
    if (value_width != 0 and num_docs_usize > (std.math.maxInt(usize) - values_start) / value_width) return error.InvalidData;
    const values_bytes = num_docs_usize * value_width;
    if (values_bytes > chunk_data.len - values_start) return error.InvalidData;
    return values_start;
}

fn fixedValueOffset(chunk_data: []const u8, num_docs: u32, pos: u32, value_width: usize) !usize {
    if (pos >= num_docs) return error.InvalidData;
    const values_start = try fixedValueSpanStart(chunk_data, num_docs, value_width);
    return values_start + @as(usize, @intCast(pos)) * value_width;
}

/// Default number of documents per chunk.
pub const default_chunk_size: u32 = 1024;

// ============================================================================
// Writer
// ============================================================================

pub const TypedDocValuesWriter = struct {
    alloc: Allocator,
    value_type: ValueType,
    chunk_size: u32,
    entries: std.ArrayListUnmanaged(Entry),
    raw_value_bytes: usize = 0,
    last_doc_id: ?u32 = null,

    const Entry = struct {
        doc_id: u32,
        value: TypedValue,
        // For bytes_val, we own the data
        owned_bytes: ?[]u8 = null,
    };

    pub fn init(alloc: Allocator, value_type: ValueType, chunk_size: u32) TypedDocValuesWriter {
        return .{
            .alloc = alloc,
            .value_type = value_type,
            .chunk_size = chunk_size,
            .entries = .empty,
        };
    }

    pub fn deinit(self: *TypedDocValuesWriter) void {
        for (self.entries.items) |*e| {
            if (e.owned_bytes) |b| self.alloc.free(b);
        }
        self.entries.deinit(self.alloc);
    }

    pub fn estimatedMemoryBytes(self: *const TypedDocValuesWriter) u64 {
        var total: u64 = @as(u64, @intCast(self.entries.capacity)) * @sizeOf(Entry);
        for (self.entries.items) |entry| {
            if (entry.owned_bytes) |bytes| total +|= @intCast(bytes.len);
        }
        return total;
    }

    pub fn add(self: *TypedDocValuesWriter, doc_id: u32, value: TypedValue) !void {
        return self.addWithOwnership(doc_id, value, true);
    }

    /// Keep only descriptors for immutable batch values. Strings must remain
    /// alive until this writer is encoded or deinitialized.
    pub fn addBorrowed(self: *TypedDocValuesWriter, doc_id: u32, value: TypedValue) !void {
        return self.addWithOwnership(doc_id, value, false);
    }

    fn addWithOwnership(self: *TypedDocValuesWriter, doc_id: u32, value: TypedValue, own_bytes: bool) !void {
        if (!typedValueMatchesValueType(value, self.value_type)) return error.InvalidData;
        if (!typedValueIsSerializable(value)) return error.InvalidData;
        if (self.last_doc_id) |last_doc_id| {
            if (doc_id <= last_doc_id) return error.InvalidData;
        }

        const next_raw_bytes = try std.math.add(usize, self.raw_value_bytes, switch (value) {
            .bytes_val => |bytes| try std.math.add(usize, bytes.len, 8),
            .bool_val => 5,
            .geo_point, .datetime_ns => 20,
            .numeric_val => 13,
            else => 12,
        });
        var entry = Entry{ .doc_id = doc_id, .value = value };
        // For bytes, dupe the data so we own it
        if (own_bytes and value == .bytes_val) {
            const owned = try self.alloc.dupe(u8, value.bytes_val);
            entry.owned_bytes = owned;
            entry.value = .{ .bytes_val = owned };
        }
        errdefer if (entry.owned_bytes) |bytes| self.alloc.free(bytes);
        try self.entries.append(self.alloc, entry);
        self.raw_value_bytes = next_raw_bytes;
        self.last_doc_id = doc_id;
    }

    /// Build serialized typed doc values section. Caller owns returned bytes.
    pub fn build(self: *TypedDocValuesWriter) ![]u8 {
        var out = std.ArrayListUnmanaged(u8).empty;
        defer out.deinit(self.alloc);

        if (self.chunk_size == 0) return error.InvalidData;
        const num_entries = self.entries.items.len;
        var chunk_ends = std.ArrayListUnmanaged(usize).empty;
        defer chunk_ends.deinit(self.alloc);
        var next_entry: usize = 0;
        while (next_entry < num_entries) {
            const first = next_entry;
            var raw_bytes: usize = 4;
            while (next_entry < num_entries and next_entry - first < self.chunk_size) {
                const value_bytes: usize = switch (self.entries.items[next_entry].value) {
                    .u64_val, .i64_val, .f64_val => 8,
                    .numeric_val => 9,
                    .geo_point, .datetime_ns => 16,
                    .bool_val => 1,
                    .bytes_val => |bytes| std.math.add(usize, bytes.len, 4) catch return error.InvalidData,
                };
                const entry_bytes = std.math.add(usize, value_bytes, 4) catch return error.InvalidData;
                // A single large value remains representable; its output is
                // caller-owned and cannot be smaller than that value itself.
                if (next_entry != first and entry_bytes > 256 * 1024 -| raw_bytes) break;
                raw_bytes = std.math.add(usize, raw_bytes, entry_bytes) catch return error.InvalidData;
                next_entry += 1;
            }
            try chunk_ends.append(self.alloc, next_entry);
        }
        const num_chunks: u32 = std.math.cast(u32, chunk_ends.items.len) orelse return error.InvalidData;

        // Header: value_type + num_chunks
        try out.append(self.alloc, @backingInt(self.value_type));
        try out.appendSlice(self.alloc, &@as([4]u8, @bitCast(@as(u32, num_chunks))));

        // Reserve space for chunk offset table
        const offset_table_start = out.items.len;
        const offset_table_bytes = @as(usize, num_chunks) * 8;
        try out.appendNTimes(self.alloc, 0, offset_table_bytes);

        // Write chunks
        for (0..num_chunks) |chunk_idx| {
            const start = if (chunk_idx == 0) 0 else chunk_ends.items[chunk_idx - 1];
            const end = chunk_ends.items[chunk_idx];
            const chunk_entries = self.entries.items[start..end];

            // Build uncompressed chunk
            var chunk_data = std.ArrayListUnmanaged(u8).empty;
            defer chunk_data.deinit(self.alloc);

            const chunk_doc_count: u32 = @intCast(chunk_entries.len);
            try chunk_data.appendSlice(self.alloc, &@as([4]u8, @bitCast(@as(u32, chunk_doc_count))));

            // Doc IDs
            for (chunk_entries) |e| {
                try chunk_data.appendSlice(self.alloc, &@as([4]u8, @bitCast(@as(u32, e.doc_id))));
            }

            // Values (type-specific)
            for (chunk_entries) |e| {
                try self.writeValue(&chunk_data, e.value);
            }

            // Snappy compress
            const compressed = try snappy.encode(self.alloc, chunk_data.items);
            defer self.alloc.free(compressed);
            try out.appendSlice(self.alloc, compressed);

            // Write chunk end offset
            const chunk_end: u64 = @intCast(out.items.len);
            const off_pos = offset_table_start + chunk_idx * 8;
            out.items[off_pos..][0..8].* = @bitCast(@as(u64, chunk_end));
        }

        return try self.alloc.dupe(u8, out.items);
    }

    fn writeValue(self: *TypedDocValuesWriter, out: *std.ArrayListUnmanaged(u8), value: TypedValue) !void {
        switch (self.value_type) {
            .datetime_ns => {
                var bytes: [16]u8 = undefined;
                std.mem.writeInt(i128, &bytes, value.datetime_ns, .little);
                try out.appendSlice(self.alloc, &bytes);
            },
            .u64_val => {
                const v = value.u64_val;
                try out.appendSlice(self.alloc, &@as([8]u8, @bitCast(@as(u64, v))));
            },
            .i64_val => {
                const v = value.i64_val;
                try out.appendSlice(self.alloc, &@as([8]u8, @bitCast(@as(i64, v))));
            },
            .f64_val => {
                const v = value.f64_val;
                try out.appendSlice(self.alloc, &@as([8]u8, @bitCast(v)));
            },
            .geo_point => {
                const gp = value.geo_point;
                try out.appendSlice(self.alloc, &@as([8]u8, @bitCast(gp.lat)));
                try out.appendSlice(self.alloc, &@as([8]u8, @bitCast(gp.lon)));
            },
            .bool_val => {
                try out.append(self.alloc, if (value.bool_val) 1 else 0);
            },
            .bytes_val => {
                const bytes = value.bytes_val;
                const len: u32 = @intCast(bytes.len);
                try out.appendSlice(self.alloc, &@as([4]u8, @bitCast(@as(u32, len))));
                try out.appendSlice(self.alloc, bytes);
            },
            .numeric_val => switch (value.numeric_val) {
                .u64_val => |v| {
                    try out.append(self.alloc, 0);
                    try out.appendSlice(self.alloc, &@as([8]u8, @bitCast(@as(u64, v))));
                },
                .i64_val => |v| {
                    try out.append(self.alloc, 1);
                    try out.appendSlice(self.alloc, &@as([8]u8, @bitCast(@as(i64, v))));
                },
                .f64_val => |v| {
                    try out.append(self.alloc, 2);
                    try out.appendSlice(self.alloc, &@as([8]u8, @bitCast(v)));
                },
            },
        }
    }
};

/// Streaming layout: type | 0xc0, chunk count, compressed chunks, end-offset
/// directory, largest decoded chunk (u64 LE), and directory start (u64 LE).
/// Legacy front directories and 0x80 streams without summaries remain readable.
/// Only one bounded chunk and O(chunks) offsets are retained.
pub const StreamingWriter = struct {
    alloc: Allocator,
    sink: *@import("../segment.zig").SegmentSink,
    value_type: ValueType,
    scratch: @import("../segment_source.zig").Scratch,
    pending: ?TypedDocValuesWriter = null,
    raw: std.ArrayListUnmanaged(u8) = .empty,
    compressed: std.ArrayListUnmanaged(u8) = .empty,
    offsets: std.ArrayListUnmanaged(u8) = .empty,
    start: ?usize = null,
    raw_bytes: usize = 4,
    last_doc: ?u32 = null,
    largest_decoded_chunk: u64 = 0,

    pub fn init(alloc: Allocator, sink: *@import("../segment.zig").SegmentSink, value_type: ValueType) StreamingWriter {
        return .{ .alloc = alloc, .sink = sink, .value_type = value_type, .scratch = @import("../segment_source.zig").Scratch.init(alloc, 128 * 1024) };
    }
    pub fn deinit(self: *StreamingWriter) void {
        self.scratch.deinit();
        self.raw.deinit(self.alloc);
        self.compressed.deinit(self.alloc);
        self.offsets.deinit(self.alloc);
    }
    pub fn add(self: *StreamingWriter, doc: u32, value: TypedValue) !void {
        if (!typedValueMatchesValueType(value, self.value_type) or !typedValueIsSerializable(value)) return error.InvalidData;
        if (self.last_doc) |last| if (doc <= last) return error.InvalidData;
        const bytes: usize = switch (value) {
            .bytes_val => |v| try std.math.add(usize, 8, v.len),
            .bool_val => 5,
            .geo_point, .datetime_ns => 20,
            .numeric_val => 13,
            else => 12,
        };
        if (self.pending) |*pending| {
            if (pending.entries.items.len != 0 and (pending.entries.items.len == default_chunk_size or bytes > 64 * 1024 -| self.raw_bytes)) try self.flush();
        }
        if (self.pending == null) self.pending = TypedDocValuesWriter.init(self.scratch.allocator(), self.value_type, default_chunk_size);
        try self.pending.?.add(doc, value);
        self.raw_bytes = try std.math.add(usize, self.raw_bytes, bytes);
        self.last_doc = doc;
    }
    fn flush(self: *StreamingWriter) !void {
        const pending = if (self.pending) |*p| p else return;
        if (pending.entries.items.len == 0) return;
        if (self.start == null) {
            self.start = self.sink.len();
            try self.sink.appendSlice(&.{ @backingInt(self.value_type) | 0xc0, 0, 0, 0, 0 });
        }
        self.raw.clearRetainingCapacity();
        try self.raw.ensureTotalCapacity(self.alloc, self.raw_bytes);
        var count: [4]u8 = undefined;
        std.mem.writeInt(u32, &count, @intCast(pending.entries.items.len), .little);
        try self.raw.appendSlice(self.alloc, &count);
        for (pending.entries.items) |entry| {
            std.mem.writeInt(u32, &count, entry.doc_id, .little);
            try self.raw.appendSlice(self.alloc, &count);
        }
        // Serialization uses the reusable destination allocator, not scratch.
        var serializer = pending.*;
        serializer.alloc = self.alloc;
        for (pending.entries.items) |entry| try serializer.writeValue(&self.raw, entry.value);
        self.largest_decoded_chunk = @max(self.largest_decoded_chunk, self.raw.items.len);
        const encoded = try snappy.encodeInto(self.alloc, &self.compressed, self.raw.items);
        try self.sink.appendSlice(encoded);
        var end: [8]u8 = undefined;
        std.mem.writeInt(u64, &end, self.sink.len() - self.start.?, .little);
        try self.offsets.appendSlice(self.alloc, &end);
        self.pending = null;
        self.scratch.reset();
        self.raw_bytes = 4;
        if (self.raw.capacity > 512 * 1024) {
            self.raw.deinit(self.alloc);
            self.raw = .empty;
        }
        if (self.compressed.capacity > 512 * 1024) {
            self.compressed.deinit(self.alloc);
            self.compressed = .empty;
        }
    }
    pub fn finish(self: *StreamingWriter) !bool {
        try self.flush();
        const start = self.start orelse return false;
        var count: [4]u8 = undefined;
        std.mem.writeInt(u32, &count, std.math.cast(u32, self.offsets.items.len / 8) orelse return error.InvalidData, .little);
        try self.sink.writeAt(start + 1, &count);
        const directory = self.sink.len() - start;
        try self.sink.appendSlice(self.offsets.items);
        // Authenticated navigation summary: no payload reads for admission.
        var footer: [16]u8 = undefined;
        std.mem.writeInt(u64, footer[0..8], self.largest_decoded_chunk, .little);
        std.mem.writeInt(u64, footer[8..16], directory, .little);
        try self.sink.appendSlice(&footer);
        return true;
    }
};

/// Concatenate ordered private streams without decoding or recompressing values.
/// Callers validate monotonic IDs and type consistency during collection.
pub fn concatenateStreams(alloc: Allocator, sink: *@import("../segment.zig").SegmentSink, value_type: ValueType, views: []const @import("../segment_source.zig").View) !void {
    const start = sink.len();
    try sink.appendSlice(&.{ @backingInt(value_type) | 0xc0, 0, 0, 0, 0 });
    var offsets = std.ArrayListUnmanaged(u8).empty;
    defer offsets.deinit(alloc);
    var largest: u64 = 0;
    var scratch: [64 * 1024]u8 = undefined;
    for (views) |view| {
        if (view.length < 21) return error.InvalidData;
        var header: [5]u8 = undefined;
        try view.readInto(0, &header);
        if (header[0] != @backingInt(value_type) | 0xc0) return error.InvalidData;
        const count = std.mem.readInt(u32, header[1..5], .little);
        var footer: [16]u8 = undefined;
        try view.readInto(view.length - 16, &footer);
        largest = @max(largest, std.mem.readInt(u64, footer[0..8], .little));
        const directory = std.mem.readInt(u64, footer[8..16], .little);
        if (directory < 5 or directory > view.length - 16 or @as(u64, count) * 8 != view.length - 16 - directory) return error.InvalidData;
        var previous: u64 = 5;
        for (0..count) |i| {
            var end_bytes: [8]u8 = undefined;
            try view.readInto(directory + i * 8, &end_bytes);
            const end = std.mem.readInt(u64, &end_bytes, .little);
            if (end <= previous or end > directory) return error.InvalidData;
            while (previous < end) {
                const take: usize = @intCast(@min(scratch.len, end - previous));
                try view.readInto(previous, scratch[0..take]);
                try sink.appendSlice(scratch[0..take]);
                previous += take;
            }
            std.mem.writeInt(u64, &end_bytes, sink.len() - start, .little);
            try offsets.appendSlice(alloc, &end_bytes);
        }
        if (previous != directory) return error.InvalidData;
    }
    var count: [4]u8 = undefined;
    std.mem.writeInt(u32, &count, std.math.cast(u32, offsets.items.len / 8) orelse return error.InvalidData, .little);
    try sink.writeAt(start + 1, &count);
    const directory = sink.len() - start;
    try sink.appendSlice(offsets.items);
    var footer: [16]u8 = undefined;
    std.mem.writeInt(u64, footer[0..8], largest, .little);
    std.mem.writeInt(u64, footer[8..16], directory, .little);
    try sink.appendSlice(&footer);
}

// ============================================================================
// Reader
// ============================================================================

pub const TypedDocValuesReader = struct {
    alloc: Allocator,
    data: []const u8,
    value_type: ValueType,
    num_chunks: u32,
    owned_offsets: ?[]u8 = null,
    chunks_start: ?u64 = null,
    chunks_end: ?u64 = null,
    point_cache: ?*PointCache = null,
    owns_point_cache: bool = true,
    chunk_offsets: []const u8, // raw offset table bytes
    range: ?struct { view: @import("../segment_source.zig").View, max_chunk_bytes: usize } = null,

    pub fn init(alloc: Allocator, data: []const u8) !TypedDocValuesReader {
        if (data.len < 5) return error.InvalidData;
        const streaming = data[0] & 0x80 != 0;
        const value_type = try parseValueType(data[0] & 0x3f);
        const num_chunks = std.mem.readInt(u32, data[1..5], .little);
        const navigation = std.math.mul(usize, num_chunks, 8) catch return error.InvalidData;
        const table_start: usize = if (streaming) blk: {
            if (data.len < 13) return error.InvalidData;
            break :blk std.math.cast(usize, std.mem.readInt(u64, data[data.len - 8 ..][0..8], .little)) orelse return error.InvalidData;
        } else 5;
        const tail: usize = if (!streaming) 0 else if (data[0] & 0x40 != 0) 16 else 8;
        if (data[0] & 0x40 != 0 and !streaming) return error.InvalidData;
        if (data.len < tail + 5) return error.InvalidData;
        if (table_start < 5 or table_start > data.len - tail) return error.InvalidData;
        const remaining = data.len - tail - table_start;
        if (navigation > remaining or (streaming and navigation != remaining)) return error.InvalidData;
        const offset_table_end = table_start + navigation;
        const chunks_start: u64 = if (streaming) 5 else offset_table_end;
        const chunks_end: u64 = if (streaming) table_start else data.len;
        if (streaming) {
            var previous = chunks_start;
            for (0..num_chunks) |i| {
                const end = std.mem.readInt(u64, data[table_start + i * 8 ..][0..8], .little);
                if (end <= previous or end > chunks_end) return error.InvalidData;
                previous = end;
            }
            if (previous != chunks_end) return error.InvalidData;
        }

        return .{
            .alloc = alloc,
            .data = data,
            .value_type = value_type,
            .num_chunks = num_chunks,
            .chunk_offsets = data[table_start..offset_table_end],
            .chunks_start = chunks_start,
            .chunks_end = chunks_end,
        };
    }

    /// Close only navigation owned by a native field scope. Contiguous readers borrow it.
    pub fn deinit(self: *TypedDocValuesReader) void {
        if (self.owns_point_cache) if (self.point_cache) |cache| {
            cache.deinit();
            self.alloc.destroy(cache);
        };
        if (self.owned_offsets) |offsets| self.alloc.free(offsets);
        self.* = undefined;
    }

    pub fn enablePointCache(self: *TypedDocValuesReader, byte_budget: usize) !void {
        if (self.point_cache != null) return;
        const cache = try self.alloc.create(PointCache);
        cache.* = .{ .allocator = self.alloc, .byte_budget = byte_budget };
        self.point_cache = cache;
    }

    /// Owned results must be freed by the caller. Cached results borrow until
    /// the next read on any column sharing the cache, or until scope closure.
    pub const FoundDoc = struct { chunk_idx: u32, pos: u32, chunk_data: []u8, byte_offsets: []const u8 = &.{}, owned: bool = true };

    /// Worker-owned payload LRU plus bounded, independently retained range
    /// hints. Probed chunks are decoded once; subsequent searches can navigate
    /// by authenticated document bounds without decoding the same pivots.
    pub const PointCache = struct {
        const Bounds = struct { reader: *const TypedDocValuesReader, index: u32, first: u32, last: u32 };
        const Slot = struct { reader: *const TypedDocValuesReader, index: u32, chunk: DecodedChunk, buffer: []u8 };
        allocator: Allocator,
        byte_budget: usize,
        slots: []Slot = &.{},
        count: usize = 0,
        live_bytes: usize = 0,
        slab: []u8 = &.{},
        slab_end: usize = 0,
        bounds: [128]Bounds = undefined,
        bounds_count: usize = 0,
        next_bound: usize = 0,
        decode_count: usize = 0,

        fn evict(self: *PointCache) void {
            self.count -= 1;
            self.live_bytes -= self.slots[self.count].buffer.len;
            const buffer = self.slots[self.count].buffer;
            if (!self.inSlab(buffer)) self.allocator.free(buffer);
        }
        pub fn deinit(self: *PointCache) void {
            while (self.count != 0) self.evict();
            self.allocator.free(self.slab);
            self.allocator.free(self.slots);
        }
        fn inSlab(self: *const PointCache, buffer: []const u8) bool {
            return self.slab.len != 0 and @intFromPtr(buffer.ptr) >= @intFromPtr(self.slab.ptr) and @intFromPtr(buffer.ptr) - @intFromPtr(self.slab.ptr) < self.slab.len;
        }
        fn relocate(slot: *Slot, target: []u8) void {
            const data_len = slot.chunk.data.len;
            const offsets_len = slot.chunk.byte_offsets.len;
            std.mem.copyForwards(u8, target, slot.buffer);
            slot.buffer = target;
            slot.chunk.data = target[0..data_len];
            slot.chunk.byte_offsets = if (offsets_len == 0) &.{} else target[data_len..][0..offsets_len];
        }
        fn compact(self: *PointCache, target: []u8) void {
            // Physical order makes leftward in-place moves safe regardless of
            // the LRU order. Borrowed values expire at the next cache read.
            var order: [256]u16 = undefined;
            for (order[0..self.count], 0..) |*index, i| index.* = @intCast(i);
            std.mem.sort(u16, order[0..self.count], self.slots, struct {
                fn less(slots: []Slot, x: u16, y: u16) bool {
                    return @intFromPtr(slots[x].buffer.ptr) < @intFromPtr(slots[y].buffer.ptr);
                }
            }.less);
            var end: usize = 0;
            for (order[0..self.count]) |index| {
                const slot = &self.slots[index];
                const length = slot.buffer.len;
                relocate(slot, target[end..][0..length]);
                end += length;
            }
            self.slab_end = end;
        }
        fn workingBuffer(self: *PointCache, bytes: usize) ![]u8 {
            // Exceptional legacy chunks are sole working allocations. Ordinary
            // chunks share a geometrically grown slab: arena callers retain
            // less than twice the budget, even under variable-size eviction.
            if (bytes > self.byte_budget) return self.allocator.alloc(u8, bytes);
            const required = self.live_bytes + bytes;
            if (self.slab.len < required) {
                const capacity = @min(self.byte_budget, std.math.ceilPowerOfTwo(usize, @max(required, 1)) catch required);
                const slab = try self.allocator.alloc(u8, capacity);
                self.compact(slab);
                self.allocator.free(self.slab);
                self.slab = slab;
            } else if (self.slab_end > self.slab.len - bytes) {
                // Reuse an evicted range before moving resident chunks. A
                // rolling scan through equal-size chunks needs no compaction.
                var order: [256]u16 = undefined;
                for (order[0..self.count], 0..) |*index, i| index.* = @intCast(i);
                std.mem.sort(u16, order[0..self.count], self.slots, struct {
                    fn less(slots: []Slot, x: u16, y: u16) bool {
                        return @intFromPtr(slots[x].buffer.ptr) < @intFromPtr(slots[y].buffer.ptr);
                    }
                }.less);
                var start: usize = 0;
                for (order[0..self.count]) |index| {
                    const slot = self.slots[index];
                    const offset = @intFromPtr(slot.buffer.ptr) - @intFromPtr(self.slab.ptr);
                    if (offset - start >= bytes) return self.slab[start..][0..bytes];
                    start = offset + slot.buffer.len;
                }
                if (self.slab.len - start >= bytes) {
                    self.slab_end = @max(self.slab_end, start + bytes);
                    return self.slab[start..][0..bytes];
                }
                self.compact(self.slab);
            }
            const buffer = self.slab[self.slab_end..][0..bytes];
            self.slab_end += bytes;
            return buffer;
        }
        fn known(self: *const PointCache, reader: *const TypedDocValuesReader, index: u32) ?Bounds {
            for (self.bounds[0..self.bounds_count]) |bound| if (bound.reader == reader and bound.index == index) return bound;
            return null;
        }
        fn load(self: *PointCache, reader: *const TypedDocValuesReader, index: u32) !*const DecodedChunk {
            if (self.slots.len == 0) {
                // Bound directory overhead as well as payload bytes. Reserve
                // once so arena callers do not accumulate resized directories.
                const entries = @max(1, @min(256, self.byte_budget / 4096));
                self.slots = try self.allocator.alloc(Slot, entries);
            }
            for (self.slots[0..self.count], 0..) |slot, position| if (slot.reader == reader and slot.index == index) {
                std.mem.copyBackwards(Slot, self.slots[1 .. position + 1], self.slots[0..position]);
                self.slots[0] = slot;
                return &self.slots[0].chunk;
            };
            // Evict before allocating; oversized legacy chunks may be a sole
            // working chunk, but never permanently increase retained capacity.
            const extent = try reader.chunkRange(index);
            var header: [5]u8 = undefined;
            const take = @min(header.len, extent.end - extent.start);
            if (reader.range) |range| try range.view.readInto(extent.start, header[0..take]) else @memcpy(header[0..take], reader.data[extent.start..][0..take]);
            const needed = try snappy.decodedLen(header[0..take]);
            if (reader.range) |range| {
                if (extent.end - extent.start > range.max_chunk_bytes or needed > range.max_chunk_bytes) return error.SegmentReadBudgetExceeded;
            }
            // Decode just the four-byte row count for exact admission. A
            // worst-case directory based on decoded bytes wastes half a large
            // single-value chunk. The complete decoder still validates data.
            const directory_bytes: usize = if (reader.value_type == .bytes_val) blk: {
                const View = @import("../segment_source.zig").View;
                const view = if (reader.range) |range| try View.init(range.view.source, range.view.offset + extent.start, extent.end - extent.start) else try View.init(.{ .contiguous = reader.data }, extent.start, extent.end - extent.start);
                var prefix: [4]u8 = undefined;
                if (try snappy.decodePrefixFromView(view, &prefix) != needed) return error.InvalidData;
                const rows = std.mem.readInt(u32, &prefix, .little);
                if (rows > (needed -| 4) / 8) return error.InvalidData;
                break :blk @as(usize, rows) * 4;
            } else 0;
            const capacity = std.math.add(usize, needed, directory_bytes) catch return error.InvalidData;
            while (self.count != 0 and (self.count == self.slots.len or capacity > self.byte_budget or self.live_bytes > self.byte_budget -| capacity)) self.evict();
            const buffer = try self.workingBuffer(capacity);
            errdefer if (capacity > self.byte_budget) self.allocator.free(buffer) else {
                const end = @intFromPtr(buffer.ptr) - @intFromPtr(self.slab.ptr) + capacity;
                if (self.slab_end == end) self.slab_end -= capacity;
            };
            var chunk = try reader.decodeChunkInto(index, buffer[0..needed]);
            if (chunk.num_docs == 0) return error.InvalidData;
            var previous: ?u32 = null;
            for (0..chunk.num_docs) |pos| {
                const id = std.mem.readInt(u32, chunk.data[4 + pos * 4 ..][0..4], .little);
                if (previous) |last| if (id <= last) return error.InvalidData;
                previous = id;
            }
            if (reader.value_type == .bytes_val) {
                const offsets = buffer[needed..][0 .. @as(usize, chunk.num_docs) * 4];
                var cursor = chunk.values_start;
                for (0..chunk.num_docs) |pos| {
                    std.mem.writeInt(u32, offsets[pos * 4 ..][0..4], @intCast(cursor), .little);
                    const len = std.mem.readInt(u32, chunk.data[cursor..][0..4], .little);
                    cursor += 4 + @as(usize, len);
                }
                chunk.byte_offsets = offsets;
            }
            const bound = Bounds{ .reader = reader, .index = index, .first = std.mem.readInt(u32, chunk.data[4..8], .little), .last = previous.? };
            if (self.known(reader, index) == null) {
                self.bounds[self.next_bound] = bound;
                self.next_bound = (self.next_bound + 1) % self.bounds.len;
                self.bounds_count = @min(self.bounds.len, self.bounds_count + 1);
            }
            std.mem.copyBackwards(Slot, self.slots[1 .. self.count + 1], self.slots[0..self.count]);
            self.slots[0] = .{ .reader = reader, .index = index, .chunk = chunk, .buffer = buffer };
            self.count += 1;
            self.live_bytes += buffer.len;
            self.decode_count += 1;
            return &self.slots[0].chunk;
        }
        fn locate(index: u32, data: []u8, count: u32, byte_offsets: []const u8, doc_id: u32) ?FoundDoc {
            var low: u32 = 0;
            var high = count;
            while (low < high) {
                const mid = low + (high - low) / 2;
                const id = std.mem.readInt(u32, data[4 + @as(usize, mid) * 4 ..][0..4], .little);
                if (id < doc_id) low = mid + 1 else high = mid;
            }
            if (low == count or std.mem.readInt(u32, data[4 + @as(usize, low) * 4 ..][0..4], .little) != doc_id) return null;
            return .{ .chunk_idx = index, .pos = low, .chunk_data = data, .byte_offsets = byte_offsets, .owned = false };
        }
        fn find(self: *PointCache, reader: *const TypedDocValuesReader, doc_id: u32) !?FoundDoc {
            for (self.slots[0..self.count]) |slot| {
                if (slot.reader != reader) continue;
                const first = std.mem.readInt(u32, slot.chunk.data[4..8], .little);
                const last = std.mem.readInt(u32, slot.chunk.data[4 + @as(usize, slot.chunk.num_docs - 1) * 4 ..][0..4], .little);
                if (doc_id >= first and doc_id <= last) {
                    const chunk = try self.load(reader, slot.index);
                    return locate(slot.index, chunk.data, chunk.num_docs, chunk.byte_offsets, doc_id);
                }
            }
            var low: u32 = 0;
            var high = reader.num_chunks;
            while (low < high) {
                const mid = low + (high - low) / 2;
                const bound = self.known(reader, mid) orelse blk: {
                    const chunk = try self.load(reader, mid);
                    break :blk Bounds{ .reader = reader, .index = mid, .first = std.mem.readInt(u32, chunk.data[4..8], .little), .last = std.mem.readInt(u32, chunk.data[4 + @as(usize, chunk.num_docs - 1) * 4 ..][0..4], .little) };
                };
                if (doc_id < bound.first) high = mid else if (doc_id > bound.last) low = mid + 1 else {
                    const chunk = try self.load(reader, mid);
                    return locate(mid, chunk.data, chunk.num_docs, chunk.byte_offsets, doc_id);
                }
            }
            return null;
        }
    };

    /// Sequential field scans decode each chunk once. Values borrow the cursor
    /// until it advances to another chunk; exceptional scratch is not retained.
    pub const Cursor = struct {
        reader: *const TypedDocValuesReader,
        scratch: @import("../segment_source.zig").Scratch,
        chunk: ?DecodedChunk = null,
        entries: ?DecodedChunk.Iterator = null,
        next_chunk: u32 = 0,

        pub fn init(reader: *const TypedDocValuesReader) Cursor {
            return .{ .reader = reader, .scratch = @import("../segment_source.zig").Scratch.init(reader.alloc, 1024 * 1024) };
        }
        pub fn deinit(self: *Cursor) void {
            self.scratch.deinit();
            self.* = undefined;
        }
        pub fn next(self: *Cursor) !?DecodedChunk.Entry {
            while (true) {
                if (self.entries) |*entries| if (try entries.next()) |entry| return entry;
                self.entries = null;
                self.chunk = null;
                if (self.next_chunk >= self.reader.num_chunks) return null;
                self.scratch.reset();
                var scoped = self.reader.*;
                scoped.alloc = self.scratch.allocator();
                self.chunk = try scoped.decodeChunk(self.next_chunk);
                self.next_chunk += 1;
                self.entries = self.chunk.?.iterator();
            }
        }
    };

    fn chunkEndOffset(self: *const TypedDocValuesReader, chunk_idx: u32) u64 {
        const off = @as(usize, chunk_idx) * 8;
        return std.mem.readInt(u64, self.chunk_offsets[off..][0..8], .little);
    }

    fn chunkStartOffset(self: *const TypedDocValuesReader, chunk_idx: u32) u64 {
        if (chunk_idx == 0) return self.chunks_start orelse 5 + @as(u64, self.num_chunks) * 8;
        return self.chunkEndOffset(chunk_idx - 1);
    }

    fn chunkRange(self: *const TypedDocValuesReader, chunk_idx: u32) !struct { start: usize, end: usize } {
        if (chunk_idx >= self.num_chunks) return error.InvalidData;
        const start = self.chunkStartOffset(chunk_idx);
        const end = self.chunkEndOffset(chunk_idx);
        if (start > end) return error.InvalidData;
        if (end > (self.chunks_end orelse if (self.range) |range| range.view.length else self.data.len)) return error.InvalidData;
        return .{ .start = @intCast(start), .end = @intCast(end) };
    }

    /// Decompress a chunk and return its raw bytes. Caller owns result.
    fn decompressChunk(self: *const TypedDocValuesReader, chunk_idx: u32) ![]u8 {
        const range = try self.chunkRange(chunk_idx);
        if (self.range) |source| {
            const length = range.end - range.start;
            if (length > source.max_chunk_bytes) return error.SegmentReadBudgetExceeded;
            const view = try @import("../segment_source.zig").View.init(source.view.source, source.view.offset + range.start, length);
            return snappy.decodeFromView(self.alloc, view, source.max_chunk_bytes);
        }
        const compressed = self.data[range.start..range.end];
        return snappy.decode(self.alloc, compressed);
    }

    pub const DecodedChunk = struct {
        alloc: Allocator,
        data: []u8,
        value_type: ValueType,
        num_docs: u32,
        values_start: usize,
        byte_offsets: []const u8 = &.{},

        pub const Entry = struct {
            doc_id: u32,
            value: TypedValue,
        };

        pub const Iterator = struct {
            chunk: *const DecodedChunk,
            pos: u32 = 0,
            bytes_cursor: usize,

            pub fn next(self: *Iterator) !?Entry {
                if (self.pos >= self.chunk.num_docs) return null;
                const pos: usize = @intCast(self.pos);
                const doc_offset = 4 + pos * 4;
                const doc_id = std.mem.readInt(u32, self.chunk.data[doc_offset..][0..4], .little);
                const value: TypedValue = switch (self.chunk.value_type) {
                    .datetime_ns => .{ .datetime_ns = std.mem.readInt(i128, self.chunk.data[self.chunk.values_start + pos * 16 ..][0..16], .little) },
                    .u64_val => .{ .u64_val = std.mem.readInt(u64, self.chunk.data[self.chunk.values_start + pos * 8 ..][0..8], .little) },
                    .i64_val => .{ .i64_val = std.mem.readInt(i64, self.chunk.data[self.chunk.values_start + pos * 8 ..][0..8], .little) },
                    .f64_val => .{ .f64_val = try decodeSerializableF64(self.chunk.data[self.chunk.values_start + pos * 8 ..][0..8].*) },
                    .geo_point => blk: {
                        const value_offset = self.chunk.values_start + pos * 16;
                        break :blk .{ .geo_point = .{
                            .lat = try decodeSerializableF64(self.chunk.data[value_offset..][0..8].*),
                            .lon = try decodeSerializableF64(self.chunk.data[value_offset + 8 ..][0..8].*),
                        } };
                    },
                    .bool_val => .{ .bool_val = try decodeSerializableBool(self.chunk.data[self.chunk.values_start + pos]) },
                    .bytes_val => blk: {
                        if (self.bytes_cursor + 4 > self.chunk.data.len) return error.InvalidData;
                        const value_len = std.mem.readInt(u32, self.chunk.data[self.bytes_cursor..][0..4], .little);
                        self.bytes_cursor += 4;
                        if (value_len > self.chunk.data.len - self.bytes_cursor) return error.InvalidData;
                        const bytes = self.chunk.data[self.bytes_cursor..][0..value_len];
                        self.bytes_cursor += value_len;
                        break :blk .{ .bytes_val = bytes };
                    },
                    .numeric_val => .{ .numeric_val = try decodeNumericValue(self.chunk.data[self.chunk.values_start + pos * 9 ..][0..9].*) },
                };
                self.pos += 1;
                return .{ .doc_id = doc_id, .value = value };
            }
        };

        pub fn deinit(self: *DecodedChunk) void {
            self.alloc.free(self.data);
            self.* = undefined;
        }

        pub fn iterator(self: *const DecodedChunk) Iterator {
            return .{ .chunk = self, .bytes_cursor = self.values_start };
        }
    };

    /// Decode and validate one complete chunk for sequential consumers such as
    /// segment merge. This avoids the point-lookup API's repeated scan and
    /// decompression of every preceding chunk for each document.
    pub fn decodeChunk(self: *const TypedDocValuesReader, chunk_idx: u32) !DecodedChunk {
        const chunk_data = try self.decompressChunk(chunk_idx);
        errdefer self.alloc.free(chunk_data);
        return self.validatedChunk(chunk_data);
    }

    fn decodeChunkInto(self: *const TypedDocValuesReader, index: u32, out: []u8) !DecodedChunk {
        const extent = try self.chunkRange(index);
        const Source = @import("../segment_source.zig");
        const view = if (self.range) |range| blk: {
            if (extent.end - extent.start > range.max_chunk_bytes or out.len > range.max_chunk_bytes) return error.SegmentReadBudgetExceeded;
            break :blk try Source.View.init(range.view.source, range.view.offset + extent.start, extent.end - extent.start);
        } else try Source.View.init(.{ .contiguous = self.data }, extent.start, extent.end - extent.start);
        try snappy.decodeFromViewInto(view, out);
        return self.validatedChunk(out);
    }

    fn validatedChunk(self: *const TypedDocValuesReader, chunk_data: []u8) !DecodedChunk {
        if (chunk_data.len < 4) return error.InvalidData;
        const num_docs = std.mem.readInt(u32, chunk_data[0..4], .little);
        try self.validateChunkValuePayload(chunk_data, num_docs);
        return .{
            .alloc = self.alloc,
            .data = chunk_data,
            .value_type = self.value_type,
            .num_docs = num_docs,
            .values_start = try valuesStartForDocIds(chunk_data, num_docs),
        };
    }

    /// Find which chunk contains a given doc_id by scanning chunk doc IDs.
    /// Returns (chunk_idx, position_within_chunk).
    pub fn findDoc(self: *const TypedDocValuesReader, doc_id: u32) !?FoundDoc {
        if (self.point_cache) |cache| return cache.find(self, doc_id);
        if (self.range != null) {
            // Writers require strictly increasing IDs across the field. Use
            // that wire invariant for point access without a full-column copy.
            var low: u32 = 0;
            var high = self.num_chunks;
            while (low < high) {
                const mid = low + (high - low) / 2;
                const data = try self.decompressChunk(mid);
                var keep = false;
                defer if (!keep) self.alloc.free(data);
                if (data.len < 4) return error.InvalidData;
                const count = std.mem.readInt(u32, data[0..4], .little);
                _ = try valuesStartForDocIds(data, count);
                if (count == 0) return error.InvalidData;
                var previous: ?u32 = null;
                for (0..count) |position| {
                    const id = std.mem.readInt(u32, data[4 + position * 4 ..][0..4], .little);
                    if (previous) |last| if (id <= last) return error.InvalidData;
                    previous = id;
                }
                const first = std.mem.readInt(u32, data[4..8], .little);
                if (doc_id < first) {
                    high = mid;
                    continue;
                }
                if (doc_id > previous.?) {
                    low = mid + 1;
                    continue;
                }
                var begin: u32 = 0;
                var end = count;
                while (begin < end) {
                    const position = begin + (end - begin) / 2;
                    const id = std.mem.readInt(u32, data[4 + @as(usize, position) * 4 ..][0..4], .little);
                    if (id < doc_id) begin = position + 1 else end = position;
                }
                if (begin == count or std.mem.readInt(u32, data[4 + @as(usize, begin) * 4 ..][0..4], .little) != doc_id) return null;
                keep = true;
                return .{ .chunk_idx = mid, .pos = begin, .chunk_data = data };
            }
            return null;
        }
        for (0..self.num_chunks) |ci| {
            const chunk_data = try self.decompressChunk(@intCast(ci));
            if (chunk_data.len < 4) {
                self.alloc.free(chunk_data);
                return error.InvalidData;
            }
            const num_docs = std.mem.readInt(u32, chunk_data[0..4], .little);
            _ = valuesStartForDocIds(chunk_data, num_docs) catch |err| {
                self.alloc.free(chunk_data);
                return err;
            };
            for (0..num_docs) |i| {
                const off = 4 + i * 4;
                const did = std.mem.readInt(u32, chunk_data[off..][0..4], .little);
                if (did == doc_id) {
                    return .{ .chunk_idx = @intCast(ci), .pos = @intCast(i), .chunk_data = chunk_data };
                }
            }
            self.alloc.free(chunk_data);
        }
        return null;
    }

    /// Get a single u64 value for a doc.
    pub fn getU64(self: *const TypedDocValuesReader, doc_id: u32) !?u64 {
        if (self.value_type != .u64_val) return error.InvalidData;
        const found = try self.findDoc(doc_id) orelse return null;
        defer if (found.owned) self.alloc.free(found.chunk_data);
        const num_docs = std.mem.readInt(u32, found.chunk_data[0..4], .little);
        const val_off = try fixedValueOffset(found.chunk_data, num_docs, found.pos, 8);
        return std.mem.readInt(u64, found.chunk_data[val_off..][0..8], .little);
    }

    /// Legacy unsigned columns and signed v1 datetime columns share one reader contract.
    pub fn getDateTimeNs(self: *const TypedDocValuesReader, doc_id: u32) !?i128 {
        if (self.value_type == .u64_val) return if (try self.getU64(doc_id)) |v| @as(i128, v) else null;
        if (self.value_type != .datetime_ns) return error.InvalidData;
        const found = try self.findDoc(doc_id) orelse return null;
        defer if (found.owned) self.alloc.free(found.chunk_data);
        const num_docs = std.mem.readInt(u32, found.chunk_data[0..4], .little);
        const val_off = try fixedValueOffset(found.chunk_data, num_docs, found.pos, 16);
        return std.mem.readInt(i128, found.chunk_data[val_off..][0..16], .little);
    }

    /// Get a single i64 value for a doc.
    pub fn getI64(self: *const TypedDocValuesReader, doc_id: u32) !?i64 {
        if (self.value_type != .i64_val) return error.InvalidData;
        const found = try self.findDoc(doc_id) orelse return null;
        defer if (found.owned) self.alloc.free(found.chunk_data);
        const num_docs = std.mem.readInt(u32, found.chunk_data[0..4], .little);
        const val_off = try fixedValueOffset(found.chunk_data, num_docs, found.pos, 8);
        return std.mem.readInt(i64, found.chunk_data[val_off..][0..8], .little);
    }

    /// Get a single f64 value for a doc.
    pub fn getF64(self: *const TypedDocValuesReader, doc_id: u32) !?f64 {
        if (self.value_type != .f64_val) return error.InvalidData;
        const found = try self.findDoc(doc_id) orelse return null;
        defer if (found.owned) self.alloc.free(found.chunk_data);
        const num_docs = std.mem.readInt(u32, found.chunk_data[0..4], .little);
        const val_off = try fixedValueOffset(found.chunk_data, num_docs, found.pos, 8);
        return try decodeSerializableF64(found.chunk_data[val_off..][0..8].*);
    }

    pub fn getNumeric(self: *const TypedDocValuesReader, doc_id: u32) !?NumericValue {
        if (self.value_type != .numeric_val) return error.InvalidData;
        const found = try self.findDoc(doc_id) orelse return null;
        defer if (found.owned) self.alloc.free(found.chunk_data);
        const num_docs = std.mem.readInt(u32, found.chunk_data[0..4], .little);
        const val_off = try fixedValueOffset(found.chunk_data, num_docs, found.pos, 9);
        return try decodeNumericValue(found.chunk_data[val_off..][0..9].*);
    }

    /// Get a single GeoPoint value for a doc.
    pub fn getGeoPoint(self: *const TypedDocValuesReader, doc_id: u32) !?GeoPoint {
        if (self.value_type != .geo_point) return error.InvalidData;
        const found = try self.findDoc(doc_id) orelse return null;
        defer if (found.owned) self.alloc.free(found.chunk_data);
        const num_docs = std.mem.readInt(u32, found.chunk_data[0..4], .little);
        const val_off = try fixedValueOffset(found.chunk_data, num_docs, found.pos, 16);
        const lat = try decodeSerializableF64(found.chunk_data[val_off..][0..8].*);
        const lon = try decodeSerializableF64(found.chunk_data[val_off + 8 ..][0..8].*);
        return .{ .lat = lat, .lon = lon };
    }

    /// Get a single bool value for a doc.
    pub fn getBool(self: *const TypedDocValuesReader, doc_id: u32) !?bool {
        if (self.value_type != .bool_val) return error.InvalidData;
        const found = try self.findDoc(doc_id) orelse return null;
        defer if (found.owned) self.alloc.free(found.chunk_data);
        const num_docs = std.mem.readInt(u32, found.chunk_data[0..4], .little);
        const val_off = try fixedValueOffset(found.chunk_data, num_docs, found.pos, 1);
        return try decodeSerializableBool(found.chunk_data[val_off]);
    }

    /// Get a single bytes value for a doc. Caller owns the returned slice.
    pub fn getBytesAlloc(self: *const TypedDocValuesReader, doc_id: u32) !?[]u8 {
        return self.getBytesAllocWithAllocator(self.alloc, doc_id);
    }

    /// Result ownership is independent of the reader/cache allocator. This
    /// preserves loader contracts when output and query scratch use different
    /// accounting allocators or when the result outlives the field scope.
    pub fn getBytesAllocWithAllocator(self: *const TypedDocValuesReader, output_allocator: Allocator, doc_id: u32) !?[]u8 {
        if (self.value_type != .bytes_val) return error.InvalidData;
        const found = try self.findDoc(doc_id) orelse return null;
        defer if (found.owned) self.alloc.free(found.chunk_data);
        return try output_allocator.dupe(u8, try bytesForFound(found));
    }

    /// Requires a point cache. The value borrows it until the next read on ANY
    /// reader sharing that cache; callers needing longer ownership must copy.
    pub fn getBytesBorrowed(self: *const TypedDocValuesReader, doc_id: u32) !?[]const u8 {
        if (self.value_type != .bytes_val) return error.InvalidData;
        if (self.point_cache == null) return error.PointCacheRequired;
        const found = try self.findDoc(doc_id) orelse return null;
        return try bytesForFound(found);
    }

    fn bytesForFound(found: FoundDoc) ![]const u8 {
        const num_docs = std.mem.readInt(u32, found.chunk_data[0..4], .little);
        var cursor: usize = if (found.byte_offsets.len != 0) std.mem.readInt(u32, found.byte_offsets[@as(usize, found.pos) * 4 ..][0..4], .little) else 4 + @as(usize, num_docs) * 4;
        for (0..if (found.byte_offsets.len == 0) found.pos else 0) |_| {
            if (cursor + 4 > found.chunk_data.len) return error.InvalidData;
            const value_len = std.mem.readInt(u32, found.chunk_data[cursor..][0..4], .little);
            cursor += 4;
            if (value_len > found.chunk_data.len - cursor) return error.InvalidData;
            cursor += value_len;
        }
        if (cursor + 4 > found.chunk_data.len) return error.InvalidData;
        const value_len = std.mem.readInt(u32, found.chunk_data[cursor..][0..4], .little);
        cursor += 4;
        if (value_len > found.chunk_data.len - cursor) return error.InvalidData;
        return found.chunk_data[cursor..][0..value_len];
    }

    /// Read all u64 values in a chunk. Caller owns returned slice.
    pub fn readU64Chunk(self: *const TypedDocValuesReader, chunk_idx: u32) ![]u64 {
        if (self.value_type != .u64_val) return error.InvalidData;
        const chunk_data = try self.decompressChunk(chunk_idx);
        defer self.alloc.free(chunk_data);
        if (chunk_data.len < 4) return error.InvalidData;
        const num_docs = std.mem.readInt(u32, chunk_data[0..4], .little);
        const values_start = try fixedValueSpanStart(chunk_data, num_docs, 8);
        const result = try self.alloc.alloc(u64, num_docs);
        for (0..num_docs) |i| {
            const off = values_start + i * 8;
            result[i] = std.mem.readInt(u64, chunk_data[off..][0..8], .little);
        }
        return result;
    }

    /// Read all i64 values in a chunk. Caller owns returned slice.
    pub fn readI64Chunk(self: *const TypedDocValuesReader, chunk_idx: u32) ![]i64 {
        if (self.value_type != .i64_val) return error.InvalidData;
        const chunk_data = try self.decompressChunk(chunk_idx);
        defer self.alloc.free(chunk_data);
        if (chunk_data.len < 4) return error.InvalidData;
        const num_docs = std.mem.readInt(u32, chunk_data[0..4], .little);
        const values_start = try fixedValueSpanStart(chunk_data, num_docs, 8);
        const result = try self.alloc.alloc(i64, num_docs);
        for (0..num_docs) |i| {
            const off = values_start + i * 8;
            result[i] = std.mem.readInt(i64, chunk_data[off..][0..8], .little);
        }
        return result;
    }

    /// Read all f64 values in a chunk. Caller owns returned slice.
    pub fn readF64Chunk(self: *const TypedDocValuesReader, chunk_idx: u32) ![]f64 {
        if (self.value_type != .f64_val) return error.InvalidData;
        const chunk_data = try self.decompressChunk(chunk_idx);
        defer self.alloc.free(chunk_data);
        if (chunk_data.len < 4) return error.InvalidData;
        const num_docs = std.mem.readInt(u32, chunk_data[0..4], .little);
        const values_start = try fixedValueSpanStart(chunk_data, num_docs, 8);
        const result = try self.alloc.alloc(f64, num_docs);
        errdefer self.alloc.free(result);
        for (0..num_docs) |i| {
            const off = values_start + i * 8;
            result[i] = try decodeSerializableF64(chunk_data[off..][0..8].*);
        }
        return result;
    }

    pub fn readNumericChunk(self: *const TypedDocValuesReader, chunk_idx: u32) ![]NumericValue {
        if (self.value_type != .numeric_val) return error.InvalidData;
        const chunk_data = try self.decompressChunk(chunk_idx);
        defer self.alloc.free(chunk_data);
        if (chunk_data.len < 4) return error.InvalidData;
        const num_docs = std.mem.readInt(u32, chunk_data[0..4], .little);
        const values_start = try fixedValueSpanStart(chunk_data, num_docs, 9);
        const result = try self.alloc.alloc(NumericValue, num_docs);
        errdefer self.alloc.free(result);
        for (0..num_docs) |i| {
            result[i] = try decodeNumericValue(chunk_data[values_start + i * 9 ..][0..9].*);
        }
        return result;
    }

    /// Read all GeoPoint values in a chunk. Caller owns returned slice.
    pub fn readGeoPointChunk(self: *const TypedDocValuesReader, chunk_idx: u32) ![]GeoPoint {
        if (self.value_type != .geo_point) return error.InvalidData;
        const chunk_data = try self.decompressChunk(chunk_idx);
        defer self.alloc.free(chunk_data);
        if (chunk_data.len < 4) return error.InvalidData;
        const num_docs = std.mem.readInt(u32, chunk_data[0..4], .little);
        const values_start = try fixedValueSpanStart(chunk_data, num_docs, 16);
        const result = try self.alloc.alloc(GeoPoint, num_docs);
        errdefer self.alloc.free(result);
        for (0..num_docs) |i| {
            const off = values_start + i * 16;
            result[i] = .{
                .lat = try decodeSerializableF64(chunk_data[off..][0..8].*),
                .lon = try decodeSerializableF64(chunk_data[off + 8 ..][0..8].*),
            };
        }
        return result;
    }

    /// Read doc IDs in a chunk. Caller owns returned slice.
    pub fn readChunkDocIds(self: *const TypedDocValuesReader, chunk_idx: u32) ![]u32 {
        const chunk_data = try self.decompressChunk(chunk_idx);
        defer self.alloc.free(chunk_data);
        if (chunk_data.len < 4) return error.InvalidData;
        const num_docs = std.mem.readInt(u32, chunk_data[0..4], .little);
        _ = try valuesStartForDocIds(chunk_data, num_docs);
        const result = try self.alloc.alloc(u32, num_docs);
        for (0..num_docs) |i| {
            const off = 4 + i * 4;
            result[i] = std.mem.readInt(u32, chunk_data[off..][0..4], .little);
        }
        return result;
    }

    /// Read all doc IDs in a chunk after validating that the typed value
    /// payload for the same chunk is decodable. Caller owns returned slice.
    pub fn readValidatedChunkDocIds(self: *const TypedDocValuesReader, chunk_idx: u32) ![]u32 {
        const chunk_data = try self.decompressChunk(chunk_idx);
        defer self.alloc.free(chunk_data);
        if (chunk_data.len < 4) return error.InvalidData;
        const num_docs = std.mem.readInt(u32, chunk_data[0..4], .little);
        try self.validateChunkValuePayload(chunk_data, num_docs);

        const result = try self.alloc.alloc(u32, num_docs);
        for (0..num_docs) |i| {
            const off = 4 + i * 4;
            result[i] = std.mem.readInt(u32, chunk_data[off..][0..4], .little);
        }
        return result;
    }

    fn validateChunkValuePayload(
        self: *const TypedDocValuesReader,
        chunk_data: []const u8,
        num_docs: u32,
    ) !void {
        switch (self.value_type) {
            .datetime_ns => {
                _ = try fixedValueSpanStart(chunk_data, num_docs, 16);
            },
            .u64_val, .i64_val => {
                _ = try fixedValueSpanStart(chunk_data, num_docs, 8);
            },
            .f64_val => {
                const values_start = try fixedValueSpanStart(chunk_data, num_docs, 8);
                for (0..num_docs) |i| {
                    const off = values_start + i * 8;
                    _ = try decodeSerializableF64(chunk_data[off..][0..8].*);
                }
            },
            .geo_point => {
                const values_start = try fixedValueSpanStart(chunk_data, num_docs, 16);
                for (0..num_docs) |i| {
                    const off = values_start + i * 16;
                    _ = try decodeSerializableF64(chunk_data[off..][0..8].*);
                    _ = try decodeSerializableF64(chunk_data[off + 8 ..][0..8].*);
                }
            },
            .bool_val => {
                const values_start = try fixedValueSpanStart(chunk_data, num_docs, 1);
                for (0..num_docs) |i| {
                    _ = try decodeSerializableBool(chunk_data[values_start + i]);
                }
            },
            .bytes_val => {
                var cursor = try valuesStartForDocIds(chunk_data, num_docs);
                for (0..num_docs) |_| {
                    if (cursor + 4 > chunk_data.len) return error.InvalidData;
                    const value_len = std.mem.readInt(u32, chunk_data[cursor..][0..4], .little);
                    cursor += 4;
                    if (value_len > chunk_data.len - cursor) return error.InvalidData;
                    cursor += value_len;
                }
            },
            .numeric_val => {
                const values_start = try fixedValueSpanStart(chunk_data, num_docs, 9);
                for (0..num_docs) |i| {
                    _ = try decodeNumericValue(chunk_data[values_start + i * 9 ..][0..9].*);
                }
            },
        }
    }
};

fn decodeNumericValue(raw: [9]u8) !NumericValue {
    return switch (raw[0]) {
        0 => .{ .u64_val = std.mem.readInt(u64, raw[1..9], .little) },
        1 => .{ .i64_val = std.mem.readInt(i64, raw[1..9], .little) },
        2 => .{ .f64_val = try decodeSerializableF64(raw[1..9].*) },
        else => error.InvalidData,
    };
}

// ============================================================================
// Tests
// ============================================================================

fn buildSingleDocFixedSectionAlloc(alloc: Allocator, value_type: ValueType, doc_id: u32, value_bytes: []const u8) ![]u8 {
    var chunk = std.ArrayListUnmanaged(u8).empty;
    defer chunk.deinit(alloc);
    try chunk.appendSlice(alloc, &@as([4]u8, @bitCast(@as(u32, 1))));
    try chunk.appendSlice(alloc, &@as([4]u8, @bitCast(@as(u32, doc_id))));
    try chunk.appendSlice(alloc, value_bytes);

    const compressed = try snappy.encode(alloc, chunk.items);
    defer alloc.free(compressed);

    var data = std.ArrayListUnmanaged(u8).empty;
    defer data.deinit(alloc);
    try data.append(alloc, @backingInt(value_type));
    try data.appendSlice(alloc, &@as([4]u8, @bitCast(@as(u32, 1))));
    const chunk_end: u64 = @intCast(5 + 8 + compressed.len);
    try data.appendSlice(alloc, &@as([8]u8, @bitCast(@as(u64, chunk_end))));
    try data.appendSlice(alloc, compressed);
    return try data.toOwnedSlice(alloc);
}

test "typed doc values u64 round-trip" {
    const alloc = std.testing.allocator;

    var writer = TypedDocValuesWriter.init(alloc, .u64_val, 1024);
    defer writer.deinit();

    try writer.add(0, .{ .u64_val = 100 });
    try writer.add(1, .{ .u64_val = 200 });
    try writer.add(5, .{ .u64_val = 500 });

    const data = try writer.build();
    defer alloc.free(data);

    var reader = try TypedDocValuesReader.init(alloc, data);
    try std.testing.expectEqual(ValueType.u64_val, reader.value_type);

    try std.testing.expectEqual(@as(?u64, 100), try reader.getU64(0));
    try std.testing.expectEqual(@as(?u64, 200), try reader.getU64(1));
    try std.testing.expectEqual(@as(?u64, 500), try reader.getU64(5));
    try std.testing.expectEqual(@as(?u64, null), try reader.getU64(3));

    const doc_ids = try reader.readValidatedChunkDocIds(0);
    defer alloc.free(doc_ids);
    try std.testing.expectEqualSlices(u32, &.{ 0, 1, 5 }, doc_ids);
}

test "typed doc values exact numeric domain round-trip and comparison" {
    const alloc = std.testing.allocator;
    var writer = TypedDocValuesWriter.init(alloc, .numeric_val, 2);
    defer writer.deinit();
    try writer.add(0, .{ .numeric_val = .{ .i64_val = -9007199254740993 } });
    try writer.add(1, .{ .numeric_val = .{ .f64_val = 10.5 } });
    try writer.add(2, .{ .numeric_val = .{ .u64_val = std.math.maxInt(u64) } });

    const data = try writer.build();
    defer alloc.free(data);
    var reader = try TypedDocValuesReader.init(alloc, data);
    try std.testing.expectEqual(NumericValue{ .i64_val = -9007199254740993 }, (try reader.getNumeric(0)).?);
    try std.testing.expectEqual(NumericValue{ .f64_val = 10.5 }, (try reader.getNumeric(1)).?);
    try std.testing.expectEqual(NumericValue{ .u64_val = std.math.maxInt(u64) }, (try reader.getNumeric(2)).?);
    try std.testing.expectEqual(std.math.Order.lt, compareNumericValues(.{ .i64_val = 10 }, .{ .f64_val = 10.5 }));
    try std.testing.expectEqual(std.math.Order.gt, compareNumericValues(.{ .u64_val = 9007199254740993 }, .{ .f64_val = 9007199254740992.0 }));
}

test "typed doc values writer rejects mismatched value type" {
    const alloc = std.testing.allocator;

    var writer = TypedDocValuesWriter.init(alloc, .u64_val, 1024);
    defer writer.deinit();

    try std.testing.expectError(error.InvalidData, writer.add(0, .{ .i64_val = -1 }));
    try std.testing.expectError(error.InvalidData, writer.add(1, .{ .bytes_val = "not-u64" }));
    try std.testing.expectEqual(@as(usize, 0), writer.entries.items.len);
}

test "typed doc values writer rejects duplicate and out-of-order doc ids" {
    const alloc = std.testing.allocator;

    var duplicate_writer = TypedDocValuesWriter.init(alloc, .u64_val, 1024);
    defer duplicate_writer.deinit();
    try duplicate_writer.add(0, .{ .u64_val = 10 });
    try std.testing.expectError(error.InvalidData, duplicate_writer.add(0, .{ .u64_val = 20 }));

    var out_of_order_writer = TypedDocValuesWriter.init(alloc, .u64_val, 1024);
    defer out_of_order_writer.deinit();
    try out_of_order_writer.add(2, .{ .u64_val = 20 });
    try std.testing.expectError(error.InvalidData, out_of_order_writer.add(1, .{ .u64_val = 10 }));
}

test "typed doc values writer rejects non-finite floating values" {
    const alloc = std.testing.allocator;

    var f64_writer = TypedDocValuesWriter.init(alloc, .f64_val, 1024);
    defer f64_writer.deinit();
    try std.testing.expectError(error.InvalidData, f64_writer.add(0, .{ .f64_val = std.math.nan(f64) }));
    try std.testing.expectError(error.InvalidData, f64_writer.add(0, .{ .f64_val = std.math.inf(f64) }));
    try f64_writer.add(0, .{ .f64_val = 1.5 });

    var numeric_writer = TypedDocValuesWriter.init(alloc, .numeric_val, 1024);
    defer numeric_writer.deinit();
    try std.testing.expectError(error.InvalidData, numeric_writer.add(0, .{ .numeric_val = .{ .f64_val = std.math.nan(f64) } }));
    try std.testing.expectError(error.InvalidData, numeric_writer.add(0, .{ .numeric_val = .{ .f64_val = std.math.inf(f64) } }));
    try numeric_writer.add(0, .{ .numeric_val = .{ .f64_val = 1.5 } });

    var geo_writer = TypedDocValuesWriter.init(alloc, .geo_point, 1024);
    defer geo_writer.deinit();
    try std.testing.expectError(error.InvalidData, geo_writer.add(0, .{ .geo_point = .{ .lat = std.math.nan(f64), .lon = 10.0 } }));
    try std.testing.expectError(error.InvalidData, geo_writer.add(0, .{ .geo_point = .{ .lat = 10.0, .lon = std.math.inf(f64) } }));
    try geo_writer.add(0, .{ .geo_point = .{ .lat = 10.0, .lon = 20.0 } });
}

test "typed doc values reader rejects mismatched accessor type" {
    const alloc = std.testing.allocator;

    var writer = TypedDocValuesWriter.init(alloc, .u64_val, 1024);
    defer writer.deinit();

    try writer.add(0, .{ .u64_val = 42 });

    const data = try writer.build();
    defer alloc.free(data);

    var reader = try TypedDocValuesReader.init(alloc, data);
    try std.testing.expectEqual(@as(?u64, 42), try reader.getU64(0));

    try std.testing.expectError(error.InvalidData, reader.getI64(0));
    try std.testing.expectError(error.InvalidData, reader.getF64(0));
    try std.testing.expectError(error.InvalidData, reader.getBool(0));
    try std.testing.expectError(error.InvalidData, reader.getBytesAlloc(0));
    try std.testing.expectError(error.InvalidData, reader.readI64Chunk(0));
    try std.testing.expectError(error.InvalidData, reader.readF64Chunk(0));
    try std.testing.expectError(error.InvalidData, reader.readGeoPointChunk(0));
}

test "typed doc values reader rejects malformed headers and chunk bounds" {
    const alloc = std.testing.allocator;

    try std.testing.expectError(error.InvalidData, TypedDocValuesReader.init(alloc, &.{ 255, 0, 0, 0, 0 }));

    var writer = TypedDocValuesWriter.init(alloc, .u64_val, 1024);
    defer writer.deinit();

    try writer.add(0, .{ .u64_val = 42 });

    const data = try writer.build();
    defer alloc.free(data);

    var reader = try TypedDocValuesReader.init(alloc, data);
    try std.testing.expectError(error.InvalidData, reader.readU64Chunk(1));

    const corrupt_data = try alloc.dupe(u8, data);
    defer alloc.free(corrupt_data);
    std.mem.writeInt(u64, corrupt_data[5..][0..8], @as(u64, @intCast(corrupt_data.len + 1)), .little);

    var corrupt_reader = try TypedDocValuesReader.init(alloc, corrupt_data);
    try std.testing.expectError(error.InvalidData, corrupt_reader.readU64Chunk(0));
}

test "typed doc values reader rejects malformed bytes value lengths" {
    const alloc = std.testing.allocator;

    var chunk = std.ArrayListUnmanaged(u8).empty;
    defer chunk.deinit(alloc);
    try chunk.appendSlice(alloc, &@as([4]u8, @bitCast(@as(u32, 1))));
    try chunk.appendSlice(alloc, &@as([4]u8, @bitCast(@as(u32, 0))));
    try chunk.appendSlice(alloc, &@as([4]u8, @bitCast(@as(u32, 8))));
    try chunk.appendSlice(alloc, "abc");

    const compressed = try snappy.encode(alloc, chunk.items);
    defer alloc.free(compressed);

    var data = std.ArrayListUnmanaged(u8).empty;
    defer data.deinit(alloc);
    try data.append(alloc, @backingInt(ValueType.bytes_val));
    try data.appendSlice(alloc, &@as([4]u8, @bitCast(@as(u32, 1))));
    const chunk_end: u64 = @intCast(5 + 8 + compressed.len);
    try data.appendSlice(alloc, &@as([8]u8, @bitCast(@as(u64, chunk_end))));
    try data.appendSlice(alloc, compressed);

    var reader = try TypedDocValuesReader.init(alloc, data.items);
    try std.testing.expectError(error.InvalidData, reader.getBytesAlloc(0));
    try std.testing.expectError(error.InvalidData, reader.readValidatedChunkDocIds(0));
}

test "typed doc values reader rejects non-canonical bool values" {
    const alloc = std.testing.allocator;

    const data = try buildSingleDocFixedSectionAlloc(alloc, .bool_val, 0, &.{2});
    defer alloc.free(data);

    var reader = try TypedDocValuesReader.init(alloc, data);
    try std.testing.expectError(error.InvalidData, reader.getBool(0));
    try std.testing.expectError(error.InvalidData, reader.readValidatedChunkDocIds(0));
}

test "typed doc values reader rejects non-finite floating payloads" {
    const alloc = std.testing.allocator;

    const nan_bytes = @as([8]u8, @bitCast(std.math.nan(f64)));
    const f64_data = try buildSingleDocFixedSectionAlloc(alloc, .f64_val, 0, &nan_bytes);
    defer alloc.free(f64_data);

    var f64_reader = try TypedDocValuesReader.init(alloc, f64_data);
    try std.testing.expectError(error.InvalidData, f64_reader.getF64(0));
    try std.testing.expectError(error.InvalidData, f64_reader.readF64Chunk(0));
    try std.testing.expectError(error.InvalidData, f64_reader.readValidatedChunkDocIds(0));

    var geo_bytes = std.ArrayListUnmanaged(u8).empty;
    defer geo_bytes.deinit(alloc);
    try geo_bytes.appendSlice(alloc, &@as([8]u8, @bitCast(std.math.inf(f64))));
    try geo_bytes.appendSlice(alloc, &@as([8]u8, @bitCast(@as(f64, 10.0))));

    const geo_data = try buildSingleDocFixedSectionAlloc(alloc, .geo_point, 0, geo_bytes.items);
    defer alloc.free(geo_data);

    var geo_reader = try TypedDocValuesReader.init(alloc, geo_data);
    try std.testing.expectError(error.InvalidData, geo_reader.getGeoPoint(0));
    try std.testing.expectError(error.InvalidData, geo_reader.readGeoPointChunk(0));
    try std.testing.expectError(error.InvalidData, geo_reader.readValidatedChunkDocIds(0));
}

test "typed doc values i64 round-trip" {
    const alloc = std.testing.allocator;

    var writer = TypedDocValuesWriter.init(alloc, .i64_val, 1024);
    defer writer.deinit();

    try writer.add(0, .{ .i64_val = -100 });
    try writer.add(1, .{ .i64_val = 0 });
    try writer.add(5, .{ .i64_val = 500 });

    const data = try writer.build();
    defer alloc.free(data);

    var reader = try TypedDocValuesReader.init(alloc, data);
    try std.testing.expectEqual(ValueType.i64_val, reader.value_type);

    try std.testing.expectEqual(@as(?i64, -100), try reader.getI64(0));
    try std.testing.expectEqual(@as(?i64, 0), try reader.getI64(1));
    try std.testing.expectEqual(@as(?i64, 500), try reader.getI64(5));
    try std.testing.expectEqual(@as(?i64, null), try reader.getI64(3));
}

test "typed doc values f64 round-trip" {
    const alloc = std.testing.allocator;

    var writer = TypedDocValuesWriter.init(alloc, .f64_val, 1024);
    defer writer.deinit();

    try writer.add(0, .{ .f64_val = 3.14 });
    try writer.add(1, .{ .f64_val = 2.718 });

    const data = try writer.build();
    defer alloc.free(data);

    var reader = try TypedDocValuesReader.init(alloc, data);

    const v0 = (try reader.getF64(0)).?;
    try std.testing.expectApproxEqAbs(@as(f64, 3.14), v0, 0.001);

    const v1 = (try reader.getF64(1)).?;
    try std.testing.expectApproxEqAbs(@as(f64, 2.718), v1, 0.001);
}

test "typed doc values geo_point round-trip" {
    const alloc = std.testing.allocator;

    var writer = TypedDocValuesWriter.init(alloc, .geo_point, 1024);
    defer writer.deinit();

    try writer.add(0, .{ .geo_point = .{ .lat = 37.7749, .lon = -122.4194 } });
    try writer.add(1, .{ .geo_point = .{ .lat = 40.7128, .lon = -74.0060 } });

    const data = try writer.build();
    defer alloc.free(data);

    var reader = try TypedDocValuesReader.init(alloc, data);

    const gp0 = (try reader.getGeoPoint(0)).?;
    try std.testing.expectApproxEqAbs(@as(f64, 37.7749), gp0.lat, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, -122.4194), gp0.lon, 0.0001);

    const gp1 = (try reader.getGeoPoint(1)).?;
    try std.testing.expectApproxEqAbs(@as(f64, 40.7128), gp1.lat, 0.0001);
}

test "typed doc values bool round-trip" {
    const alloc = std.testing.allocator;

    var writer = TypedDocValuesWriter.init(alloc, .bool_val, 1024);
    defer writer.deinit();

    try writer.add(0, .{ .bool_val = true });
    try writer.add(1, .{ .bool_val = false });
    try writer.add(2, .{ .bool_val = true });

    const data = try writer.build();
    defer alloc.free(data);

    var reader = try TypedDocValuesReader.init(alloc, data);

    try std.testing.expectEqual(@as(?bool, true), try reader.getBool(0));
    try std.testing.expectEqual(@as(?bool, false), try reader.getBool(1));
    try std.testing.expectEqual(@as(?bool, true), try reader.getBool(2));
}

test "typed doc values bulk chunk read" {
    const alloc = std.testing.allocator;

    var writer = TypedDocValuesWriter.init(alloc, .u64_val, 1024);
    defer writer.deinit();

    for (0..5) |i| {
        try writer.add(@intCast(i), .{ .u64_val = @intCast(i * 10) });
    }

    const data = try writer.build();
    defer alloc.free(data);

    var reader = try TypedDocValuesReader.init(alloc, data);

    const values = try reader.readU64Chunk(0);
    defer alloc.free(values);
    try std.testing.expectEqual(@as(usize, 5), values.len);
    try std.testing.expectEqual(@as(u64, 0), values[0]);
    try std.testing.expectEqual(@as(u64, 10), values[1]);
    try std.testing.expectEqual(@as(u64, 40), values[4]);

    const doc_ids = try reader.readChunkDocIds(0);
    defer alloc.free(doc_ids);
    try std.testing.expectEqual(@as(usize, 5), doc_ids.len);
    try std.testing.expectEqual(@as(u32, 0), doc_ids[0]);
    try std.testing.expectEqual(@as(u32, 4), doc_ids[4]);
}

test "typed doc values decoded chunk iterator preserves sparse values across chunks" {
    const alloc = std.testing.allocator;

    var writer = TypedDocValuesWriter.init(alloc, .u64_val, 2);
    defer writer.deinit();
    try writer.add(1, .{ .u64_val = 10 });
    try writer.add(4, .{ .u64_val = 40 });
    try writer.add(9, .{ .u64_val = 90 });
    try writer.add(15, .{ .u64_val = 150 });
    try writer.add(31, .{ .u64_val = 310 });

    const data = try writer.build();
    defer alloc.free(data);

    var reader = try TypedDocValuesReader.init(alloc, data);
    try std.testing.expectEqual(@as(u32, 3), reader.num_chunks);

    const expected_doc_ids = [_]u32{ 1, 4, 9, 15, 31 };
    const expected_values = [_]u64{ 10, 40, 90, 150, 310 };
    var value_index: usize = 0;
    for (0..reader.num_chunks) |chunk_index| {
        var chunk = try reader.decodeChunk(@intCast(chunk_index));
        defer chunk.deinit();
        var it = chunk.iterator();
        while (try it.next()) |entry| {
            try std.testing.expectEqual(expected_doc_ids[value_index], entry.doc_id);
            try std.testing.expectEqual(expected_values[value_index], entry.value.u64_val);
            value_index += 1;
        }
    }
    try std.testing.expectEqual(expected_doc_ids.len, value_index);
}

test "typed doc values decoded chunk iterator borrows byte values from its chunk" {
    const alloc = std.testing.allocator;

    var writer = TypedDocValuesWriter.init(alloc, .bytes_val, 2);
    defer writer.deinit();
    try writer.add(0, .{ .bytes_val = "alpha" });
    try writer.add(3, .{ .bytes_val = "beta" });
    try writer.add(8, .{ .bytes_val = "gamma" });

    const data = try writer.build();
    defer alloc.free(data);

    var reader = try TypedDocValuesReader.init(alloc, data);
    const expected_doc_ids = [_]u32{ 0, 3, 8 };
    const expected_values = [_][]const u8{ "alpha", "beta", "gamma" };
    var value_index: usize = 0;
    for (0..reader.num_chunks) |chunk_index| {
        var chunk = try reader.decodeChunk(@intCast(chunk_index));
        defer chunk.deinit();
        var it = chunk.iterator();
        while (try it.next()) |entry| {
            try std.testing.expectEqual(expected_doc_ids[value_index], entry.doc_id);
            try std.testing.expectEqualStrings(expected_values[value_index], entry.value.bytes_val);
            value_index += 1;
        }
    }
    try std.testing.expectEqual(expected_doc_ids.len, value_index);
}

test "typed doc values bytes round-trip" {
    const alloc = std.testing.allocator;

    var writer = TypedDocValuesWriter.init(alloc, .bytes_val, 1024);
    defer writer.deinit();

    try writer.add(0, .{ .bytes_val = "hello" });
    try writer.add(1, .{ .bytes_val = "world" });

    const data = try writer.build();
    defer alloc.free(data);

    var reader = try TypedDocValuesReader.init(alloc, data);
    try std.testing.expectEqual(ValueType.bytes_val, reader.value_type);
    try std.testing.expectEqual(@as(u32, 1), reader.num_chunks);

    const doc0 = (try reader.getBytesAlloc(0)).?;
    defer alloc.free(doc0);
    try std.testing.expectEqualStrings("hello", doc0);

    const doc1 = (try reader.getBytesAlloc(1)).?;
    defer alloc.free(doc1);
    try std.testing.expectEqualStrings("world", doc1);

    try std.testing.expect((try reader.getBytesAlloc(2)) == null);

    const doc_ids = try reader.readValidatedChunkDocIds(0);
    defer alloc.free(doc_ids);
    try std.testing.expectEqualSlices(u32, &.{ 0, 1 }, doc_ids);
}

/// Owns only chunk navigation. The exposed reader borrows this ownership and
/// fetches one compressed chunk at a time. Output chunks are caller-owned;
/// oversized compressed and decoded buffers are refused before allocation.
pub const RangeTypedDocValuesReader = struct {
    reader: TypedDocValuesReader,
    offsets: []u8,

    pub fn init(allocator: Allocator, view: @import("../segment_source.zig").View, max_metadata_bytes: usize, max_chunk_bytes: usize) !RangeTypedDocValuesReader {
        if (view.length < 5) return error.InvalidData;
        var header: [5]u8 = undefined;
        try view.readInto(0, &header);
        const streaming = header[0] & 0x80 != 0;
        const value_type = try parseValueType(header[0] & 0x3f);
        if (header[0] & 0x40 != 0 and !streaming) return error.InvalidData;
        const count = std.mem.readInt(u32, header[1..5], .little);
        const navigation = std.math.mul(usize, count, 8) catch return error.InvalidData;
        var table_start: u64 = 5;
        var chunks_end = view.length;
        if (streaming) {
            const tail: u64 = if (header[0] & 0x40 != 0) 16 else 8;
            if (view.length < 5 + tail) return error.InvalidData;
            var footer: [8]u8 = undefined;
            try view.readInto(view.length - 8, &footer);
            table_start = std.mem.readInt(u64, &footer, .little);
            if (table_start < 5 or table_start > view.length - tail or navigation != view.length - tail - table_start) return error.InvalidData;
            chunks_end = table_start;
        } else if (navigation > view.length - 5) return error.InvalidData;
        if (navigation > max_metadata_bytes) return error.SegmentMetadataTooLarge;
        const offsets = try allocator.alloc(u8, navigation);
        errdefer allocator.free(offsets);
        try view.readInto(table_start, offsets);
        const chunks_start: u64 = if (streaming) 5 else 5 + @as(u64, count) * 8;
        var previous = chunks_start;
        for (0..count) |chunk| {
            const end = std.mem.readInt(u64, offsets[chunk * 8 ..][0..8], .little);
            if (end <= previous or end > chunks_end) return error.InvalidData;
            previous = end;
        }
        if (previous != chunks_end) return error.InvalidData;
        return .{ .offsets = offsets, .reader = .{ .alloc = allocator, .data = &.{}, .value_type = value_type, .num_chunks = count, .chunk_offsets = offsets, .chunks_start = chunks_start, .chunks_end = chunks_end, .range = .{ .view = view, .max_chunk_bytes = max_chunk_bytes } } };
    }

    pub fn deinit(self: *RangeTypedDocValuesReader) void {
        self.reader.alloc.free(self.offsets);
        self.reader.deinit();
        self.* = undefined;
    }
};

test "typed point cache reduces repeated probes and bounds shared field payloads" {
    const a = std.testing.allocator;
    var writer = TypedDocValuesWriter.init(a, .u64_val, 128);
    defer writer.deinit();
    for (0..50_000) |doc| try writer.add(@intCast(doc * 2), .{ .u64_val = doc });
    const bytes = try writer.build();
    defer a.free(bytes);
    const Budget = @import("../storage/lite/test_allocator.zig").BudgetAllocator;
    var cached_budget = Budget{ .backing = a, .limit = 64 * 1024 };
    var reader = try TypedDocValuesReader.init(cached_budget.allocator(), bytes);
    defer reader.deinit();
    try reader.enablePointCache(16 * 1024);
    for (0..50_000) |doc| try std.testing.expectEqual(@as(u64, doc), (try reader.getU64(@intCast(doc * 2))).?);
    try std.testing.expect((try reader.getU64(99_999)) == null);
    const cache = reader.point_cache.?;
    try std.testing.expect(cache.live_bytes <= cache.byte_budget);
    try std.testing.expect(cache.count <= cache.slots.len);
    try std.testing.expect(cache.bounds_count <= cache.bounds.len);
    // A bounded navigation-hint table may re-probe pivots on very large
    // columns, but sequential point reads must not decompress per document.
    try std.testing.expect(cache.decode_count < 50_000 / 10);
    // A complete scan through an arena must retain only bounded slab growth,
    // rather than accumulating each evicted chunk until teardown.
    var arena_budget = Budget{ .backing = a, .limit = 64 * 1024 };
    var arena = std.heap.ArenaAllocator.init(arena_budget.allocator());
    defer arena.deinit();
    var arena_reader = try TypedDocValuesReader.init(arena.allocator(), bytes);
    defer arena_reader.deinit();
    try arena_reader.enablePointCache(16 * 1024);
    for (0..50_000) |doc| try std.testing.expectEqual(@as(u64, doc), (try arena_reader.getU64(@intCast(doc * 2))).?);
    try std.testing.expect(cached_budget.alloc_calls <= 6);
    var uncached_budget = Budget{ .backing = a };
    var uncached = try TypedDocValuesReader.init(uncached_budget.allocator(), bytes);
    defer uncached.deinit();
    for (0..1024) |doc| try std.testing.expectEqual(@as(u64, doc), (try uncached.getU64(@intCast(doc * 2))).?);
    _ = try reader.getU64(0);
    const warm_allocations = cached_budget.alloc_calls;
    for (0..1024) |doc| _ = try reader.getU64(@intCast((doc % 128) * 2));
    try std.testing.expectEqual(warm_allocations, cached_budget.alloc_calls);
    std.debug.print("LITE_TYPED_POINT_CACHE documents=50000 chunks={d} decodes={d} retained_bytes={d} cached_allocs={d} uncached_sample_documents=1024 uncached_allocs={d} warm_allocs=0\n", .{ reader.num_chunks, cache.decode_count, cache.live_bytes, cached_budget.alloc_calls, uncached_budget.alloc_calls });
}

test "typed legacy oversized byte chunk stays readable while new writes split by bytes" {
    const a = std.testing.allocator;
    const payload: [4096]u8 = @splat('x');
    var raw = std.ArrayListUnmanaged(u8).empty;
    defer raw.deinit(a);
    try raw.appendSlice(a, &std.mem.toBytes(@as(u32, 1024)));
    for (0..1024) |doc| try raw.appendSlice(a, &std.mem.toBytes(@as(u32, @intCast(doc))));
    for (0..1024) |_| {
        try raw.appendSlice(a, &std.mem.toBytes(@as(u32, payload.len)));
        try raw.appendSlice(a, &payload);
    }
    const compressed = try snappy.encode(a, raw.items);
    defer a.free(compressed);
    var encoded = std.ArrayListUnmanaged(u8).empty;
    defer encoded.deinit(a);
    try encoded.append(a, @backingInt(ValueType.bytes_val));
    try encoded.appendSlice(a, &std.mem.toBytes(@as(u32, 1)));
    try encoded.appendSlice(a, &std.mem.toBytes(@as(u64, 13 + compressed.len)));
    try encoded.appendSlice(a, compressed);
    var legacy = try TypedDocValuesReader.init(a, encoded.items);
    defer legacy.deinit();
    const segment_source = @import("../segment_source.zig");
    var native = try RangeTypedDocValuesReader.init(a, try segment_source.View.init(.{ .contiguous = encoded.items }, 0, encoded.items.len), std.math.maxInt(usize), std.math.maxInt(usize));
    defer native.deinit();
    try native.reader.enablePointCache(1024);
    for ([_]u32{ 0, 1023 }) |doc| {
        const old = (try legacy.getBytesAlloc(doc)).?;
        defer a.free(old);
        const fresh = (try native.reader.getBytesAlloc(doc)).?;
        defer a.free(fresh);
        try std.testing.expectEqualSlices(u8, old, fresh);
    }
    try std.testing.expectEqual(@as(usize, 1), native.reader.point_cache.?.decode_count);
    try std.testing.expectEqual(@as(usize, 1), native.reader.point_cache.?.count);
    var writer = TypedDocValuesWriter.init(a, .bytes_val, 1024);
    defer writer.deinit();
    for (0..1024) |doc| try writer.add(@intCast(doc), .{ .bytes_val = &payload });
    const bytes = try writer.build();
    defer a.free(bytes);
    var split = try TypedDocValuesReader.init(a, bytes);
    defer split.deinit();
    try std.testing.expect(split.num_chunks > 1);
    for (0..split.num_chunks) |chunk| {
        var decoded = try split.decodeChunk(@intCast(chunk));
        defer decoded.deinit();
        try std.testing.expect(decoded.data.len <= 256 * 1024);
    }
    // Loading an ordinary column into the shared cache releases the exceptional
    // working chunk instead of retaining its capacity for the next field.
    split.point_cache = native.reader.point_cache;
    split.owns_point_cache = false;
    const small = (try split.getBytesAlloc(0)).?;
    defer a.free(small);
    try std.testing.expect(native.reader.point_cache.?.live_bytes < raw.items.len);
}

test "typed streaming Snappy decoder matches buffered decoder across input windows" {
    const a = std.testing.allocator;
    const source = @import("../segment_source.zig");
    const State = struct {
        bytes: []const u8,
        largest: usize = 0,
        fail: bool = false,
        fn read(ptr: *anyopaque, off: u64, out: []u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.fail) return error.TestReadFailure;
            self.largest = @max(self.largest, out.len);
            @memcpy(out, self.bytes[@intCast(off)..][0..out.len]);
        }
        fn close(_: *anyopaque) void {}
    };
    const input = try a.alloc(u8, 128 * 1024);
    defer a.free(input);
    var prng = std.Random.DefaultPrng.init(4912);
    prng.random().bytes(input);
    // Repetition creates back-references whose offsets exceed the input window.
    @memcpy(input[64 * 1024 ..][0 .. 32 * 1024], input[32 * 1024 ..][0 .. 32 * 1024]);
    const encoded = try snappy.encode(a, input);
    defer a.free(encoded);
    var state = State{ .bytes = encoded };
    const view = try source.View.init(.{ .ranges = .{ .ptr = &state, .length = encoded.len, .read_into = State.read, .close = State.close } }, 0, encoded.len);
    const output = try snappy.decodeFromView(a, view, input.len);
    defer a.free(output);
    try std.testing.expectEqualSlices(u8, input, output);
    try std.testing.expect(state.largest <= 8192);
    try std.testing.expectError(error.SegmentReadBudgetExceeded, snappy.decodeFromView(a, view, input.len - 1));
    state.fail = true;
    try std.testing.expectError(error.TestReadFailure, snappy.decodeFromView(a, view, input.len));
    for ([_][]const u8{ &.{ 5, 0, 'a', 1, 1 }, &.{ 5, 0, 'a', 14, 1, 0 }, &.{ 5, 0, 'a', 15, 1, 0, 0, 0 } }) |copy| {
        const copy_view = try source.View.init(.{ .contiguous = copy }, 0, copy.len);
        const decoded = try snappy.decodeFromView(a, copy_view, 5);
        defer a.free(decoded);
        try std.testing.expectEqualStrings("aaaaa", decoded);
    }
    for ([_][]const u8{ &.{1}, &.{ 1, 2, 0, 0 }, &.{ 0xff, 0xff, 0xff, 0xff, 0xff } }) |invalid| {
        const bad = try source.View.init(.{ .contiguous = invalid }, 0, invalid.len);
        try std.testing.expectError(error.CorruptInput, snappy.decodeFromView(a, bad, std.math.maxInt(usize)));
    }
}

test "typed cached byte results use the output allocator and survive reader closure" {
    const a = std.testing.allocator;
    var writer = TypedDocValuesWriter.init(a, .bytes_val, 128);
    defer writer.deinit();
    try writer.add(0, .{ .bytes_val = "owned result" });
    const bytes = try writer.build();
    defer a.free(bytes);
    const Budget = @import("../storage/lite/test_allocator.zig").BudgetAllocator;
    var scratch = Budget{ .backing = a };
    var output = Budget{ .backing = a };
    var reader = try TypedDocValuesReader.init(scratch.allocator(), bytes);
    var open = true;
    defer if (open) reader.deinit();
    try reader.enablePointCache(1024);
    const value = (try reader.getBytesAllocWithAllocator(output.allocator(), 0)).?;
    defer output.allocator().free(value);
    try std.testing.expectEqual(value.len, output.live);
    reader.deinit();
    open = false;
    try std.testing.expectEqual(@as(usize, 0), scratch.live);
    try std.testing.expectEqualStrings("owned result", value);
}

test "shared point cache keeps eight columns warm within its byte budget" {
    const a = std.testing.allocator;
    var writer = TypedDocValuesWriter.init(a, .u64_val, 128);
    defer writer.deinit();
    for (0..4096) |doc| try writer.add(@intCast(doc), .{ .u64_val = doc });
    const bytes = try writer.build();
    defer a.free(bytes);
    var cache = TypedDocValuesReader.PointCache{ .allocator = a, .byte_budget = 1024 * 1024 };
    defer cache.deinit();
    var readers: [8]TypedDocValuesReader = undefined;
    for (&readers) |*reader| {
        reader.* = try TypedDocValuesReader.init(a, bytes);
        reader.point_cache = &cache;
    }
    for (0..4096) |doc| for (&readers) |*reader| {
        try std.testing.expectEqual(@as(u64, doc), (try reader.getU64(@intCast(doc))).?);
    };
    std.debug.print("LITE_TYPED_CACHE fanin=8 reads=32768 decodes={d} retained={d} slots={d}\n", .{ cache.decode_count, cache.live_bytes, cache.count });
    try std.testing.expect(cache.count > 4);
    try std.testing.expect(cache.live_bytes <= cache.byte_budget);
    try std.testing.expect(cache.decode_count < 1024);
    const before = cache.decode_count;
    for (0..100) |_| for (&readers) |*reader| {
        try std.testing.expectEqual(@as(u64, 4095), (try reader.getU64(4095)).?);
    };
    try std.testing.expectEqual(before, cache.decode_count);
}

test "cached variable bytes are indexed and borrowed while owned results survive eviction" {
    const a = std.testing.allocator;
    var writer = TypedDocValuesWriter.init(a, .bytes_val, 128);
    defer writer.deinit();
    for (0..256) |doc| {
        const value = try a.alloc(u8, doc % 31);
        defer a.free(value);
        @memset(value, @intCast(doc));
        try writer.add(@intCast(doc), .{ .bytes_val = value });
    }
    const bytes = try writer.build();
    defer a.free(bytes);
    var reader = try TypedDocValuesReader.init(a, bytes);
    defer reader.deinit();
    try reader.enablePointCache(4096);
    const owned = (try reader.getBytesAlloc(127)).?;
    defer a.free(owned);
    for (0..256) |doc| {
        const value = (try reader.getBytesBorrowed(@intCast(doc))).?;
        try std.testing.expectEqual(doc % 31, value.len);
        for (value) |byte| try std.testing.expectEqual(@as(u8, @intCast(doc)), byte);
        try std.testing.expect(reader.point_cache.?.slots[0].chunk.byte_offsets.len > 0);
    }
    for (owned) |byte| try std.testing.expectEqual(@as(u8, 127), byte);
}

test "variable-size point cache eviction stays bounded with an arena allocator" {
    const a = std.testing.allocator;
    var writer = TypedDocValuesWriter.init(a, .bytes_val, 8);
    defer writer.deinit();
    var payload: [512]u8 = @splat('v');
    for (0..4096) |doc| {
        const length = 31 + (doc / 8 % 7) * 43;
        try writer.add(@intCast(doc), .{ .bytes_val = payload[0..length] });
    }
    const bytes = try writer.build();
    defer a.free(bytes);
    const Budget = @import("../storage/lite/test_allocator.zig").BudgetAllocator;
    var budget = Budget{ .backing = a, .limit = 96 * 1024 };
    var arena = std.heap.ArenaAllocator.init(budget.allocator());
    defer arena.deinit();
    var reader = try TypedDocValuesReader.init(arena.allocator(), bytes);
    defer reader.deinit();
    try reader.enablePointCache(16 * 1024);
    for (0..4096) |doc| {
        const value = (try reader.getBytesBorrowed(@intCast(doc))).?;
        try std.testing.expectEqual(31 + (doc / 8 % 7) * 43, value.len);
        for (value) |byte| try std.testing.expectEqual(@as(u8, 'v'), byte);
        try std.testing.expect(reader.point_cache.?.live_bytes <= 16 * 1024);
    }
    try std.testing.expect(reader.point_cache.?.slab.len <= 16 * 1024);
    std.debug.print("LITE_TYPED_ARENA documents=4096 chunks={d} arena_peak={d} slab_bytes={d}\n", .{ reader.num_chunks, budget.peak, reader.point_cache.?.slab.len });
}

test "Snappy admission prefixes support copy tags and defer suffix validation" {
    const View = @import("../segment_source.zig").View;
    const copies = [_][]const u8{
        &.{ 5, 0, 'a', 1, 1 },
        &.{ 5, 0, 'a', 14, 1, 0 },
        &.{ 5, 0, 'a', 15, 1, 0, 0, 0 },
    };
    for (copies) |bytes| {
        var prefix: [4]u8 = undefined;
        const view = try View.init(.{ .contiguous = bytes }, 0, bytes.len);
        try std.testing.expectEqual(@as(usize, 5), try snappy.decodePrefixFromView(view, &prefix));
        try std.testing.expectEqualStrings("aaaa", &prefix);
        var full: [5]u8 = undefined;
        try snappy.decodeFromViewInto(view, &full);
        try std.testing.expectEqualStrings("aaaaa", &full);
    }
    const suffix_bad = [_]u8{ 8, 12, 'a', 'b', 'c', 'd', 255 };
    const view = try View.init(.{ .contiguous = &suffix_bad }, 0, suffix_bad.len);
    var prefix: [4]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 8), try snappy.decodePrefixFromView(view, &prefix));
    try std.testing.expectEqualStrings("abcd", &prefix);
    var full: [8]u8 = undefined;
    try std.testing.expectError(error.CorruptInput, snappy.decodeFromViewInto(view, &full));
    for ([_][]const u8{ &.{ 5, 1, 0 }, &.{ 5, 1, 1 }, &.{ 5, 12, 'a', 'b' } }) |bad| {
        try std.testing.expectError(error.CorruptInput, snappy.decodePrefixFromView(try View.init(.{ .contiguous = bad }, 0, bad.len), &prefix));
    }
}

test "large chunk eviction reuses freed slab ranges without relocation" {
    const a = std.testing.allocator;
    var writer = TypedDocValuesWriter.init(a, .bytes_val, 128);
    defer writer.deinit();
    const value: [1024]u8 = @splat('x');
    for (0..4096) |doc| try writer.add(@intCast(doc), .{ .bytes_val = &value });
    const bytes = try writer.build();
    defer a.free(bytes);
    var reader = try TypedDocValuesReader.init(a, bytes);
    defer reader.deinit();
    try reader.enablePointCache(1024 * 1024);
    const cache = reader.point_cache.?;
    const Before = struct { index: u32, pointer: usize, length: usize };
    var old: [256]Before = undefined;
    var relocated: usize = 0;
    var decoded: usize = 0;
    for (0..reader.num_chunks) |index| {
        const old_count = cache.count;
        const slab_pointer = @intFromPtr(cache.slab.ptr);
        for (cache.slots[0..old_count], old[0..old_count]) |slot, *snapshot| snapshot.* = .{ .index = slot.index, .pointer = @intFromPtr(slot.buffer.ptr), .length = slot.buffer.len };
        const chunk = try cache.load(&reader, @intCast(index));
        decoded += chunk.data.len;
        if (slab_pointer == @intFromPtr(cache.slab.ptr)) {
            for (old[0..old_count]) |snapshot| {
                for (cache.slots[0..cache.count]) |slot| {
                    if (slot.index == snapshot.index and @intFromPtr(slot.buffer.ptr) != snapshot.pointer) {
                        relocated += snapshot.length;
                        break;
                    }
                }
            }
        }
    }
    std.debug.print("LITE_CACHE_RELOCATION chunks={d} decoded_bytes={d} relocated_bytes={d} slab_bytes={d}\n", .{ reader.num_chunks, decoded, relocated, cache.slab.len });
    try std.testing.expect(relocated <= decoded);
}

test "streamed typed directories roundtrip through range readers and unwind allocation failures" {
    const a = std.testing.allocator;
    const Sweep = struct {
        fn run(allocator: Allocator) !void {
            var output = @import("../segment.zig").MemorySegmentSink.init(allocator);
            defer output.deinit();
            var sink = output.sink();
            var writer = StreamingWriter.init(allocator, &sink, .bytes_val);
            defer writer.deinit();
            for (0..300) |i| try writer.add(@intCast(i * 2), .{ .bytes_val = "owned bytes" });
            try std.testing.expect(try writer.finish());
            const view = try @import("../segment_source.zig").View.init(.{ .contiguous = output.out.items }, 0, output.out.items.len);
            var reader = try RangeTypedDocValuesReader.init(allocator, view, 1024, 64 * 1024);
            defer reader.deinit();
            const typed = &reader.reader;
            const value = (try typed.getBytesAlloc(598)).?;
            defer allocator.free(value);
            try std.testing.expectEqualStrings("owned bytes", value);
            const bad_footer: [8]u8 = @splat(0xff);
            try sink.writeAt(output.out.items.len - 8, &bad_footer);
            try std.testing.expectError(error.InvalidData, TypedDocValuesReader.init(allocator, output.out.items));
        }
    };
    try Sweep.run(a);
    try std.testing.checkAllAllocationFailures(a, Sweep.run, .{});
}

test "external lake signed datetime doc values round trip wide instants and legacy columns" {
    const a = std.testing.allocator;
    var writer = TypedDocValuesWriter.init(a, .datetime_ns, 2);
    defer writer.deinit();
    const values = [_]i128{ -1, -2208988800000000000, 0, 18446744073709551616, 253402300799999999999 };
    for (values, 0..) |value, i| try writer.add(@intCast(i), .{ .datetime_ns = value });
    const bytes = try writer.build();
    defer a.free(bytes);
    var reader = try TypedDocValuesReader.init(a, bytes);
    defer reader.deinit();
    for (values, 0..) |value, i| try std.testing.expectEqual(value, (try reader.getDateTimeNs(@intCast(i))).?);
    var cursor = TypedDocValuesReader.Cursor.init(&reader);
    defer cursor.deinit();
    for (values) |value| try std.testing.expectEqual(value, (try cursor.next()).?.value.datetime_ns);
    try std.testing.expectEqual(null, try cursor.next());
    var old = TypedDocValuesWriter.init(a, .u64_val, 2);
    defer old.deinit();
    try old.add(0, .{ .u64_val = std.math.maxInt(u64) });
    const legacy = try old.build();
    defer a.free(legacy);
    var old_reader = try TypedDocValuesReader.init(a, legacy);
    defer old_reader.deinit();
    try std.testing.expectEqual(@as(i128, std.math.maxInt(u64)), (try old_reader.getDateTimeNs(0)).?);
}
