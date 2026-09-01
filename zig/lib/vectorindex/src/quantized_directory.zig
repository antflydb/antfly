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

//! Immutable, mmap-friendly RaBitQ posting payload directory.
//!
//! The outer posting segment owns durability, checksum validation, and the
//! generation lease. This codec keeps the hot fixed-width arrays naturally
//! aligned and exposes borrowed slices, avoiding a second decoded heap copy
//! for every immutable posting visited by a query.

const std = @import("std");
const proto = @import("antfly_vector").proto;
const hbc_runtime = @import("hbc_runtime.zig");

const Allocator = std.mem.Allocator;
const magic: [4]u8 = "AFQD".*;
const version: u16 = 5;
const min_supported_version: u16 = 1;
const header_size: usize = 32;
const entry_header_size: usize = 32;
const index_entry_size: usize = 20;
const entry_alignment: usize = 64;
const entry_flag_member_ids: u32 = 1 << 0;
const entry_flag_projection_plane: u32 = 1 << 1;
const entry_flag_residual_locations: u32 = 1 << 2;
const known_entry_flags: u32 = entry_flag_member_ids | entry_flag_projection_plane | entry_flag_residual_locations;

fn appendZeros(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), count: usize) !void {
    try out.appendNTimes(alloc, 0, count);
}

fn alignOutput(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), alignment: usize) !void {
    const aligned = std.mem.alignForward(usize, out.items.len, alignment);
    try appendZeros(alloc, out, aligned - out.items.len);
}

fn appendU32(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: u32) !void {
    var bytes: [4]u8 = undefined;
    std.mem.writeInt(u32, &bytes, value, .little);
    try out.appendSlice(alloc, &bytes);
}

fn appendU64(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: u64) !void {
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, value, .little);
    try out.appendSlice(alloc, &bytes);
}

fn writeU16(bytes: []u8, offset: usize, value: u16) void {
    std.mem.writeInt(u16, bytes[offset..][0..2], value, .little);
}

fn writeU32(bytes: []u8, offset: usize, value: u32) void {
    std.mem.writeInt(u32, bytes[offset..][0..4], value, .little);
}

fn writeU64(bytes: []u8, offset: usize, value: u64) void {
    std.mem.writeInt(u64, bytes[offset..][0..8], value, .little);
}

fn writeF32(bytes: []u8, offset: usize, value: f32) void {
    writeU32(bytes, offset, @bitCast(value));
}

fn readU16(bytes: []const u8, offset: usize) u16 {
    return std.mem.readInt(u16, bytes[offset..][0..2], .little);
}

fn readU32(bytes: []const u8, offset: usize) u32 {
    return std.mem.readInt(u32, bytes[offset..][0..4], .little);
}

fn readU64(bytes: []const u8, offset: usize) u64 {
    return std.mem.readInt(u64, bytes[offset..][0..8], .little);
}

fn readF32(bytes: []const u8, offset: usize) f32 {
    return @bitCast(readU32(bytes, offset));
}

pub const Writer = struct {
    alloc: Allocator,
    dims: usize,
    metric: u8,
    out: std.ArrayListUnmanaged(u8) = .empty,
    posting_ids: std.ArrayListUnmanaged(u64) = .empty,
    offsets: std.ArrayListUnmanaged(u64) = .empty,
    finished: bool = false,

    pub fn init(alloc: Allocator, dims: usize, metric: u8) !Writer {
        if (dims == 0 or dims > std.math.maxInt(u32)) return error.InvalidQuantizedDirectoryConfig;
        var self: Writer = .{ .alloc = alloc, .dims = dims, .metric = metric };
        errdefer self.deinit();
        try appendZeros(alloc, &self.out, header_size);
        return self;
    }

    pub fn deinit(self: *Writer) void {
        self.out.deinit(self.alloc);
        self.posting_ids.deinit(self.alloc);
        self.offsets.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn append(self: *Writer, posting_id: u64, set: *const proto.RaBitQuantizedVectorSet) !void {
        return self.appendWithMemberBytes(posting_id, set, &.{});
    }

    /// Appends the leaf membership beside its fixed-width RaBitQ planes. The
    /// packed HBC node already encodes IDs as little-endian u64 values, so the
    /// checkpoint flattener can copy those bytes without allocating a second
    /// corpus-sized ID array. Internal postings pass an empty slice.
    pub fn appendWithMemberBytes(
        self: *Writer,
        posting_id: u64,
        set: *const proto.RaBitQuantizedVectorSet,
        member_id_bytes: []const u8,
    ) !void {
        return self.appendWithLeafPlanes(posting_id, set, member_id_bytes, &.{});
    }

    pub fn appendWithLeafPlanes(
        self: *Writer,
        posting_id: u64,
        set: *const proto.RaBitQuantizedVectorSet,
        member_id_bytes: []const u8,
        projections: []const hbc_runtime.NativeProjectionBuildValue,
    ) !void {
        if (self.finished) return error.QuantizedDirectoryWriterFinished;
        if (posting_id == 0 or set.centroid.len != self.dims) return error.InvalidQuantizedDirectoryEntry;
        const count = set.getCount();
        const width = std.math.cast(usize, set.codes.width) orelse return error.InvalidQuantizedDirectoryEntry;
        const encoded_count = std.math.cast(i64, count) orelse return error.InvalidQuantizedDirectoryEntry;
        const code_values = std.math.mul(usize, count, width) catch return error.InvalidQuantizedDirectoryEntry;
        const scalar_bytes = std.math.mul(usize, count, @sizeOf(f32)) catch return error.InvalidQuantizedDirectoryEntry;
        const omitted_l2_centroid_dots = self.metric == 0 and set.centroid_dot_products.len == 0;
        const member_bytes_expected = std.math.mul(usize, count, @sizeOf(u64)) catch
            return error.InvalidQuantizedDirectoryEntry;
        if (count == 0 or width == 0 or
            set.codes.count != encoded_count or
            set.codes.data.len != code_values or
            set.code_counts.len != count or
            set.centroid_distances.len != count or
            set.quantized_dot_products.len != count or
            (set.centroid_dot_products.len != count and !omitted_l2_centroid_dots) or
            (member_id_bytes.len != 0 and member_id_bytes.len != member_bytes_expected) or
            (projections.len != 0 and projections.len != count))
        {
            return error.InvalidQuantizedDirectoryEntry;
        }
        if (projections.len != 0) {
            const projection_bytes = std.math.mul(usize, self.dims, @sizeOf(f16)) catch
                return error.InvalidQuantizedDirectoryEntry;
            for (projections) |projection| {
                if (projection.bytes.len != projection_bytes or
                    !std.math.isFinite(projection.scale) or projection.scale <= 0 or
                    !std.math.isFinite(projection.error_norm) or projection.error_norm < 0 or
                    !std.math.isFinite(projection.decoded_norm_lower_bound) or projection.decoded_norm_lower_bound < 0)
                {
                    return error.InvalidQuantizedDirectoryEntry;
                }
            }
            const has_locations = projections[0].residual_location != null;
            for (projections) |projection| {
                if ((projection.residual_location != null) != has_locations)
                    return error.InvalidQuantizedDirectoryEntry;
                if (projection.residual_location) |location| {
                    if (location.reader_generation == 0 or location.residual_len == 0)
                        return error.InvalidQuantizedDirectoryEntry;
                }
            }
        }
        if (self.posting_ids.getLastOrNull()) |previous| {
            if (posting_id <= previous) return error.UnsortedQuantizedDirectory;
        }

        try alignOutput(self.alloc, &self.out, entry_alignment);
        const entry_offset = self.out.items.len;
        try self.posting_ids.append(self.alloc, posting_id);
        errdefer _ = self.posting_ids.pop();
        try self.offsets.append(self.alloc, @intCast(entry_offset));
        errdefer _ = self.offsets.pop();
        try appendZeros(self.alloc, &self.out, entry_header_size);
        writeU64(self.out.items, entry_offset, posting_id);
        writeU32(self.out.items, entry_offset + 8, @intCast(count));
        writeU32(self.out.items, entry_offset + 12, @intCast(width));
        writeF32(self.out.items, entry_offset + 16, set.centroid_norm);
        var flags: u32 = 0;
        if (member_id_bytes.len != 0) {
            flags |= entry_flag_member_ids;
            try self.out.appendSlice(self.alloc, member_id_bytes);
        }
        if (projections.len != 0) {
            flags |= entry_flag_projection_plane;
            if (projections[0].residual_location != null) flags |= entry_flag_residual_locations;
        }
        writeU32(self.out.items, entry_offset + 20, flags);
        try self.out.appendSlice(self.alloc, std.mem.sliceAsBytes(set.centroid));
        try alignOutput(self.alloc, &self.out, @alignOf(u64));
        try self.out.appendSlice(self.alloc, std.mem.sliceAsBytes(set.codes.data));
        try self.out.appendSlice(self.alloc, std.mem.sliceAsBytes(set.code_counts));
        try self.out.appendSlice(self.alloc, std.mem.sliceAsBytes(set.centroid_distances));
        try self.out.appendSlice(self.alloc, std.mem.sliceAsBytes(set.quantized_dot_products));
        if (omitted_l2_centroid_dots)
            try appendZeros(self.alloc, &self.out, scalar_bytes)
        else
            try self.out.appendSlice(self.alloc, std.mem.sliceAsBytes(set.centroid_dot_products));
        if (projections.len != 0) {
            // Keep each column contiguous: a leaf scan streams the f16 matrix,
            // while boundary math reads the much smaller metadata columns.
            try alignOutput(self.alloc, &self.out, @alignOf(f16));
            for (projections) |projection| try self.out.appendSlice(self.alloc, projection.bytes);
            try alignOutput(self.alloc, &self.out, @alignOf(f32));
            for (projections) |projection| try self.out.appendSlice(self.alloc, std.mem.asBytes(&projection.scale));
            for (projections) |projection| try self.out.appendSlice(self.alloc, std.mem.asBytes(&projection.error_norm));
            for (projections) |projection| try self.out.appendSlice(self.alloc, std.mem.asBytes(&projection.decoded_norm_lower_bound));
            for (projections) |projection| try self.out.appendSlice(self.alloc, std.mem.asBytes(&projection.checksum));
            if (projections[0].residual_location != null) {
                // Persist explicit columns so the record has no ABI padding
                // and every mmap view retains natural alignment.
                try alignOutput(self.alloc, &self.out, @alignOf(u64));
                for (projections) |projection| try appendU64(self.alloc, &self.out, projection.residual_location.?.reader_generation);
                try alignOutput(self.alloc, &self.out, @alignOf(u32));
                for (projections) |projection| try appendU32(self.alloc, &self.out, projection.residual_location.?.reader_shard_id);
                try alignOutput(self.alloc, &self.out, @alignOf(u64));
                for (projections) |projection| try appendU64(self.alloc, &self.out, projection.residual_location.?.revision);
                for (projections) |projection| try appendU64(self.alloc, &self.out, projection.residual_location.?.residual_offset);
                try alignOutput(self.alloc, &self.out, @alignOf(u32));
                for (projections) |projection| try appendU32(self.alloc, &self.out, projection.residual_location.?.residual_len);
                for (projections) |projection| try appendU32(self.alloc, &self.out, projection.residual_location.?.residual_checksum);
            }
        }
    }

    pub fn build(self: *Writer) ![]u8 {
        if (self.finished) return error.QuantizedDirectoryWriterFinished;
        self.finished = true;
        try alignOutput(self.alloc, &self.out, entry_alignment);
        const index_offset = self.out.items.len;
        for (self.posting_ids.items, self.offsets.items, 0..) |posting_id, offset, index| {
            const end = if (index + 1 < self.offsets.items.len) self.offsets.items[index + 1] else @as(u64, @intCast(index_offset));
            const start_usize: usize = @intCast(offset);
            const end_usize: usize = @intCast(end);
            const start = self.out.items.len;
            try appendZeros(self.alloc, &self.out, index_entry_size);
            writeU64(self.out.items, start, posting_id);
            writeU64(self.out.items, start + 8, offset);
            writeU32(self.out.items, start + 16, std.hash.Crc32.hash(self.out.items[start_usize..end_usize]));
        }
        @memcpy(self.out.items[0..4], &magic);
        writeU16(self.out.items, 4, version);
        self.out.items[6] = self.metric;
        writeU32(self.out.items, 8, @intCast(self.dims));
        writeU32(self.out.items, 12, std.hash.Crc32.hash(self.out.items[index_offset..]));
        writeU64(self.out.items, 16, @intCast(self.posting_ids.items.len));
        writeU64(self.out.items, 24, @intCast(index_offset));
        const owned = try self.out.toOwnedSlice(self.alloc);
        self.out = .empty;
        return owned;
    }
};

/// File-backed counterpart to `Writer`. Leaf payloads are emitted directly
/// into an enclosing atomic sink while only the compact posting index remains
/// resident. Offsets are relative to this nested container, allowing it to be
/// embedded in a posting segment without a corpus-sized intermediate slice.
pub const StreamingWriter = struct {
    alloc: Allocator,
    dims: usize,
    metric: u8,
    base_offset: usize,
    posting_ids: std.ArrayListUnmanaged(u64) = .empty,
    offsets: std.ArrayListUnmanaged(u64) = .empty,
    checksums: std.ArrayListUnmanaged(u32) = .empty,
    active_crc: ?std.hash.Crc32 = null,
    finished: bool = false,

    pub const Finish = struct {
        offset: usize,
        len: usize,
        /// Nested admission validates the header/index and lazily checks every
        /// leaf. The outer container therefore intentionally omits a second
        /// full-payload checksum that would reread the entire staged file.
        outer_checksum: u32 = 0,
    };

    pub fn init(alloc: Allocator, sink: anytype, dims: usize, metric: u8) !StreamingWriter {
        if (dims == 0 or dims > std.math.maxInt(u32)) return error.InvalidQuantizedDirectoryConfig;
        const base_offset = sink.len();
        var zeros: [header_size]u8 = @splat(0);
        try sink.appendSlice(&zeros);
        return .{
            .alloc = alloc,
            .dims = dims,
            .metric = metric,
            .base_offset = base_offset,
        };
    }

    pub fn deinit(self: *StreamingWriter) void {
        self.posting_ids.deinit(self.alloc);
        self.offsets.deinit(self.alloc);
        self.checksums.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn appendWithLeafPlanes(
        self: *StreamingWriter,
        sink: anytype,
        posting_id: u64,
        set: *const proto.RaBitQuantizedVectorSet,
        member_id_bytes: []const u8,
        projections: []const hbc_runtime.NativeProjectionBuildValue,
    ) !void {
        if (self.finished) return error.QuantizedDirectoryWriterFinished;
        if (posting_id == 0 or set.centroid.len != self.dims) return error.InvalidQuantizedDirectoryEntry;
        const count = set.getCount();
        const width = std.math.cast(usize, set.codes.width) orelse return error.InvalidQuantizedDirectoryEntry;
        const encoded_count = std.math.cast(i64, count) orelse return error.InvalidQuantizedDirectoryEntry;
        const code_values = std.math.mul(usize, count, width) catch return error.InvalidQuantizedDirectoryEntry;
        const scalar_bytes = std.math.mul(usize, count, @sizeOf(f32)) catch return error.InvalidQuantizedDirectoryEntry;
        const omitted_l2_centroid_dots = self.metric == 0 and set.centroid_dot_products.len == 0;
        const member_bytes_expected = std.math.mul(usize, count, @sizeOf(u64)) catch
            return error.InvalidQuantizedDirectoryEntry;
        if (count == 0 or width == 0 or
            set.codes.count != encoded_count or
            set.codes.data.len != code_values or
            set.code_counts.len != count or
            set.centroid_distances.len != count or
            set.quantized_dot_products.len != count or
            (set.centroid_dot_products.len != count and !omitted_l2_centroid_dots) or
            (member_id_bytes.len != 0 and member_id_bytes.len != member_bytes_expected) or
            (projections.len != 0 and projections.len != count))
        {
            return error.InvalidQuantizedDirectoryEntry;
        }
        if (projections.len != 0) {
            const projection_bytes = std.math.mul(usize, self.dims, @sizeOf(f16)) catch
                return error.InvalidQuantizedDirectoryEntry;
            for (projections) |projection| {
                if (projection.bytes.len != projection_bytes or
                    !std.math.isFinite(projection.scale) or projection.scale <= 0 or
                    !std.math.isFinite(projection.error_norm) or projection.error_norm < 0 or
                    !std.math.isFinite(projection.decoded_norm_lower_bound) or projection.decoded_norm_lower_bound < 0)
                {
                    return error.InvalidQuantizedDirectoryEntry;
                }
            }
            const has_locations = projections[0].residual_location != null;
            for (projections) |projection| {
                if ((projection.residual_location != null) != has_locations)
                    return error.InvalidQuantizedDirectoryEntry;
                if (projection.residual_location) |location| {
                    if (location.reader_generation == 0 or location.residual_len == 0)
                        return error.InvalidQuantizedDirectoryEntry;
                }
            }
        }
        if (self.posting_ids.getLastOrNull()) |previous| {
            if (posting_id <= previous) return error.UnsortedQuantizedDirectory;
        }

        try self.beginEntry(sink, posting_id);
        var header: [entry_header_size]u8 = @splat(0);
        writeU64(&header, 0, posting_id);
        writeU32(&header, 8, @intCast(count));
        writeU32(&header, 12, @intCast(width));
        writeF32(&header, 16, set.centroid_norm);
        var flags: u32 = 0;
        if (member_id_bytes.len != 0) flags |= entry_flag_member_ids;
        if (projections.len != 0) {
            flags |= entry_flag_projection_plane;
            if (projections[0].residual_location != null) flags |= entry_flag_residual_locations;
        }
        writeU32(&header, 20, flags);
        try self.appendEntryBytes(sink, &header);
        if (member_id_bytes.len != 0) try self.appendEntryBytes(sink, member_id_bytes);
        try self.appendEntryBytes(sink, std.mem.sliceAsBytes(set.centroid));
        try self.alignEntry(sink, @alignOf(u64));
        try self.appendEntryBytes(sink, std.mem.sliceAsBytes(set.codes.data));
        try self.appendEntryBytes(sink, std.mem.sliceAsBytes(set.code_counts));
        try self.appendEntryBytes(sink, std.mem.sliceAsBytes(set.centroid_distances));
        try self.appendEntryBytes(sink, std.mem.sliceAsBytes(set.quantized_dot_products));
        if (omitted_l2_centroid_dots)
            try self.appendEntryZeros(sink, scalar_bytes)
        else
            try self.appendEntryBytes(sink, std.mem.sliceAsBytes(set.centroid_dot_products));
        if (projections.len != 0) {
            try self.alignEntry(sink, @alignOf(f16));
            for (projections) |projection| try self.appendEntryBytes(sink, projection.bytes);
            try self.alignEntry(sink, @alignOf(f32));
            for (projections) |projection| try self.appendEntryBytes(sink, std.mem.asBytes(&projection.scale));
            for (projections) |projection| try self.appendEntryBytes(sink, std.mem.asBytes(&projection.error_norm));
            for (projections) |projection| try self.appendEntryBytes(sink, std.mem.asBytes(&projection.decoded_norm_lower_bound));
            for (projections) |projection| try self.appendEntryBytes(sink, std.mem.asBytes(&projection.checksum));
            if (projections[0].residual_location != null) {
                try self.alignEntry(sink, @alignOf(u64));
                for (projections) |projection| try self.appendEntryU64(sink, projection.residual_location.?.reader_generation);
                try self.alignEntry(sink, @alignOf(u32));
                for (projections) |projection| try self.appendEntryU32(sink, projection.residual_location.?.reader_shard_id);
                try self.alignEntry(sink, @alignOf(u64));
                for (projections) |projection| try self.appendEntryU64(sink, projection.residual_location.?.revision);
                for (projections) |projection| try self.appendEntryU64(sink, projection.residual_location.?.residual_offset);
                try self.alignEntry(sink, @alignOf(u32));
                for (projections) |projection| try self.appendEntryU32(sink, projection.residual_location.?.residual_len);
                for (projections) |projection| try self.appendEntryU32(sink, projection.residual_location.?.residual_checksum);
            }
        }
    }

    pub fn finish(self: *StreamingWriter, sink: anytype) !Finish {
        if (self.finished) return error.QuantizedDirectoryWriterFinished;
        self.finished = true;
        try self.finishActiveEntry(sink);
        const index_offset = sink.len() - self.base_offset;
        var index_crc = std.hash.Crc32.init();
        for (self.posting_ids.items, self.offsets.items, self.checksums.items) |posting_id, offset, checksum| {
            var encoded: [index_entry_size]u8 = undefined;
            writeU64(&encoded, 0, posting_id);
            writeU64(&encoded, 8, offset);
            writeU32(&encoded, 16, checksum);
            try sink.appendSlice(&encoded);
            index_crc.update(&encoded);
        }
        var header: [header_size]u8 = @splat(0);
        @memcpy(header[0..4], &magic);
        writeU16(&header, 4, version);
        header[6] = self.metric;
        writeU32(&header, 8, @intCast(self.dims));
        writeU32(&header, 12, index_crc.final());
        writeU64(&header, 16, @intCast(self.posting_ids.items.len));
        writeU64(&header, 24, @intCast(index_offset));
        try sink.writeAt(self.base_offset, &header);
        return .{
            .offset = self.base_offset,
            .len = sink.len() - self.base_offset,
        };
    }

    fn beginEntry(self: *StreamingWriter, sink: anytype, posting_id: u64) !void {
        if (self.active_crc != null) {
            try self.finishActiveEntry(sink);
        } else {
            const local_len = sink.len() - self.base_offset;
            const aligned = std.mem.alignForward(usize, local_len, entry_alignment);
            var zeros: [entry_alignment]u8 = @splat(0);
            try sink.appendSlice(zeros[0 .. aligned - local_len]);
        }
        try self.posting_ids.append(self.alloc, posting_id);
        errdefer _ = self.posting_ids.pop();
        try self.offsets.append(self.alloc, @intCast(sink.len() - self.base_offset));
        errdefer _ = self.offsets.pop();
        self.active_crc = std.hash.Crc32.init();
    }

    fn finishActiveEntry(self: *StreamingWriter, sink: anytype) !void {
        if (self.active_crc == null) return;
        try self.alignEntry(sink, entry_alignment);
        try self.checksums.append(self.alloc, self.active_crc.?.final());
        self.active_crc = null;
    }

    fn appendEntryBytes(self: *StreamingWriter, sink: anytype, bytes: []const u8) !void {
        try sink.appendSlice(bytes);
        self.active_crc.?.update(bytes);
    }

    fn appendEntryZeros(self: *StreamingWriter, sink: anytype, count: usize) !void {
        var zeros: [entry_alignment]u8 = @splat(0);
        var remaining = count;
        while (remaining != 0) {
            const n = @min(remaining, zeros.len);
            try self.appendEntryBytes(sink, zeros[0..n]);
            remaining -= n;
        }
    }

    fn alignEntry(self: *StreamingWriter, sink: anytype, alignment: usize) !void {
        const local_len = sink.len() - self.base_offset;
        const aligned = std.mem.alignForward(usize, local_len, alignment);
        try self.appendEntryZeros(sink, aligned - local_len);
    }

    fn appendEntryU32(self: *StreamingWriter, sink: anytype, value: u32) !void {
        var bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &bytes, value, .little);
        try self.appendEntryBytes(sink, &bytes);
    }

    fn appendEntryU64(self: *StreamingWriter, sink: anytype, value: u64) !void {
        var bytes: [8]u8 = undefined;
        std.mem.writeInt(u64, &bytes, value, .little);
        try self.appendEntryBytes(sink, &bytes);
    }
};

pub const View = struct {
    metric: u8,
    centroid: []const f32,
    codes: []const u64,
    code_counts: []const u32,
    centroid_distances: []const f32,
    quantized_dot_products: []const f32,
    centroid_dot_products: []const f32,
    centroid_norm: f32,
    /// Present only in V2 leaf entries. Internal postings and V1 generations
    /// retain an empty slice and continue through the packed-node path.
    member_ids: []const u64,
    projections: ?hbc_runtime.NativeProjectionPlane,
    count: usize,
    width: usize,

    pub fn asProto(self: View) proto.RaBitQuantizedVectorSet {
        return .{
            .metric = @enumFromInt(self.metric),
            .centroid = @constCast(self.centroid),
            .codes = .{
                .count = @intCast(self.count),
                .width = @intCast(self.width),
                .data = @constCast(self.codes),
            },
            .code_counts = @constCast(self.code_counts),
            .centroid_distances = @constCast(self.centroid_distances),
            .quantized_dot_products = @constCast(self.quantized_dot_products),
            .centroid_dot_products = @constCast(self.centroid_dot_products),
            .centroid_norm = self.centroid_norm,
        };
    }
};

pub const Reader = struct {
    data: []const u8,
    encoded_version: u16,
    dims: usize,
    metric: u8,
    posting_count: usize,
    index_offset: usize,

    pub fn init(data: []const u8) !Reader {
        if (data.len < header_size or !std.mem.eql(u8, data[0..4], &magic)) return error.InvalidQuantizedDirectory;
        if (@intFromPtr(data.ptr) % @alignOf(u64) != 0) return error.MisalignedQuantizedDirectory;
        const encoded_version = readU16(data, 4);
        if (encoded_version < min_supported_version or encoded_version > version)
            return error.UnsupportedQuantizedDirectoryVersion;
        const dims: usize = readU32(data, 8);
        const posting_count = std.math.cast(usize, readU64(data, 16)) orelse return error.QuantizedDirectoryTooLarge;
        const index_offset = std.math.cast(usize, readU64(data, 24)) orelse return error.QuantizedDirectoryTooLarge;
        const index_bytes = std.math.mul(usize, posting_count, index_entry_size) catch return error.InvalidQuantizedDirectory;
        if (dims == 0 or index_offset < header_size or index_offset > data.len or index_bytes != data.len - index_offset) {
            return error.InvalidQuantizedDirectory;
        }
        if (std.hash.Crc32.hash(data[index_offset..]) != readU32(data, 12)) {
            return error.QuantizedDirectoryIndexChecksumMismatch;
        }
        var previous_id: u64 = 0;
        var previous_offset: usize = 0;
        for (0..posting_count) |index| {
            const cursor = index_offset + index * index_entry_size;
            const posting_id = readU64(data, cursor);
            const offset = std.math.cast(usize, readU64(data, cursor + 8)) orelse return error.InvalidQuantizedDirectory;
            if (posting_id == 0 or (index != 0 and posting_id <= previous_id) or
                offset < header_size or offset % entry_alignment != 0 or
                offset > index_offset -| entry_header_size or (index != 0 and offset <= previous_offset))
            {
                return error.InvalidQuantizedDirectory;
            }
            previous_id = posting_id;
            previous_offset = offset;
        }
        return .{
            .data = data,
            .encoded_version = encoded_version,
            .dims = dims,
            .metric = data[6],
            .posting_count = posting_count,
            .index_offset = index_offset,
        };
    }

    fn indexId(self: Reader, index: usize) u64 {
        return readU64(self.data, self.index_offset + index * index_entry_size);
    }

    fn indexOffset(self: Reader, index: usize) usize {
        return @intCast(readU64(self.data, self.index_offset + index * index_entry_size + 8));
    }

    fn indexChecksum(self: Reader, index: usize) u32 {
        return readU32(self.data, self.index_offset + index * index_entry_size + 16);
    }

    fn findIndex(self: Reader, posting_id: u64) ?usize {
        var low: usize = 0;
        var high = self.posting_count;
        while (low < high) {
            const mid = low + (high - low) / 2;
            const id = self.indexId(mid);
            if (id < posting_id) low = mid + 1 else high = mid;
        }
        if (low == self.posting_count or self.indexId(low) != posting_id) return null;
        return low;
    }

    fn entryBytes(self: Reader, index: usize) ![]const u8 {
        if (index >= self.posting_count) return error.InvalidQuantizedDirectory;
        const start = self.indexOffset(index);
        const end = if (index + 1 < self.posting_count) self.indexOffset(index + 1) else self.index_offset;
        if (start > end or end > self.index_offset) return error.InvalidQuantizedDirectory;
        return self.data[start..end];
    }

    pub fn get(self: Reader, posting_id: u64) !?View {
        const index = self.findIndex(posting_id) orelse return null;
        return try self.viewAt(index);
    }

    fn viewAt(self: Reader, index: usize) !View {
        const entry = try self.entryBytes(index);
        const start = @intFromPtr(entry.ptr) - @intFromPtr(self.data.ptr);
        const end = start + entry.len;
        const posting_id = self.indexId(index);
        if (end - start < entry_header_size or readU64(self.data, start) != posting_id) {
            return error.InvalidQuantizedDirectory;
        }
        const count: usize = readU32(self.data, start + 8);
        const width: usize = readU32(self.data, start + 12);
        if (count == 0 or width == 0) return error.InvalidQuantizedDirectory;
        const flags = if (self.encoded_version >= 2) readU32(self.data, start + 20) else 0;
        if (flags & ~known_entry_flags != 0 or
            (flags & entry_flag_residual_locations != 0 and flags & entry_flag_projection_plane == 0) or
            (self.encoded_version < 2 and !std.mem.allEqual(u8, self.data[start + 20 .. start + entry_header_size], 0)))
        {
            return error.InvalidQuantizedDirectory;
        }
        var cursor = start + entry_header_size;
        const member_ids: []const u64 = if (flags & entry_flag_member_ids != 0) blk: {
            const member_bytes = std.math.mul(usize, count, @sizeOf(u64)) catch return error.InvalidQuantizedDirectory;
            const member_end = std.math.add(usize, cursor, member_bytes) catch return error.InvalidQuantizedDirectory;
            if (member_end > end) return error.InvalidQuantizedDirectory;
            const raw: []align(@alignOf(u64)) const u8 = @alignCast(self.data[cursor..member_end]);
            cursor = member_end;
            break :blk std.mem.bytesAsSlice(u64, raw);
        } else &.{};
        const centroid_bytes = std.math.mul(usize, self.dims, @sizeOf(f32)) catch return error.InvalidQuantizedDirectory;
        const code_values = std.math.mul(usize, count, width) catch return error.InvalidQuantizedDirectory;
        const code_bytes = std.math.mul(usize, code_values, @sizeOf(u64)) catch return error.InvalidQuantizedDirectory;
        const scalar_bytes = std.math.mul(usize, count, @sizeOf(f32)) catch return error.InvalidQuantizedDirectory;
        const centroid_end = std.math.add(usize, cursor, centroid_bytes) catch return error.InvalidQuantizedDirectory;
        if (centroid_end > end) return error.InvalidQuantizedDirectory;
        const centroid_raw: []align(@alignOf(f32)) const u8 = @alignCast(self.data[cursor..centroid_end]);
        const centroid = std.mem.bytesAsSlice(f32, centroid_raw);
        cursor = std.mem.alignForward(usize, centroid_end, @alignOf(u64));
        const codes_end = std.math.add(usize, cursor, code_bytes) catch return error.InvalidQuantizedDirectory;
        if (codes_end > end) return error.InvalidQuantizedDirectory;
        const codes_raw: []align(@alignOf(u64)) const u8 = @alignCast(self.data[cursor..codes_end]);
        const codes = std.mem.bytesAsSlice(u64, codes_raw);
        cursor = codes_end;
        const code_counts_end = std.math.add(usize, cursor, scalar_bytes) catch return error.InvalidQuantizedDirectory;
        const distances_end = std.math.add(usize, code_counts_end, scalar_bytes) catch return error.InvalidQuantizedDirectory;
        const dots_end = std.math.add(usize, distances_end, scalar_bytes) catch return error.InvalidQuantizedDirectory;
        const centroid_dots_end = std.math.add(usize, dots_end, scalar_bytes) catch return error.InvalidQuantizedDirectory;
        if (centroid_dots_end > end) return error.InvalidQuantizedDirectory;
        const code_counts_raw: []align(@alignOf(u32)) const u8 = @alignCast(self.data[cursor..code_counts_end]);
        const distances_raw: []align(@alignOf(f32)) const u8 = @alignCast(self.data[code_counts_end..distances_end]);
        const dots_raw: []align(@alignOf(f32)) const u8 = @alignCast(self.data[distances_end..dots_end]);
        const centroid_dots_raw: []align(@alignOf(f32)) const u8 = @alignCast(self.data[dots_end..centroid_dots_end]);
        cursor = centroid_dots_end;
        const projections: ?hbc_runtime.NativeProjectionPlane = if (flags & entry_flag_projection_plane != 0) blk: {
            if (self.encoded_version < 3 or member_ids.len != count) return error.InvalidQuantizedDirectory;
            cursor = std.mem.alignForward(usize, cursor, @alignOf(f16));
            const projection_values = std.math.mul(usize, count, self.dims) catch return error.InvalidQuantizedDirectory;
            const projection_bytes = std.math.mul(usize, projection_values, @sizeOf(f16)) catch return error.InvalidQuantizedDirectory;
            const projection_end = std.math.add(usize, cursor, projection_bytes) catch return error.InvalidQuantizedDirectory;
            if (projection_end > end) return error.InvalidQuantizedDirectory;
            const projection_raw: []align(@alignOf(f16)) const u8 = @alignCast(self.data[cursor..projection_end]);
            cursor = std.mem.alignForward(usize, projection_end, @alignOf(f32));
            const metadata_columns: usize = if (self.encoded_version >= 4) 4 else 3;
            const metadata_bytes = std.math.mul(usize, scalar_bytes, metadata_columns) catch return error.InvalidQuantizedDirectory;
            const metadata_end = std.math.add(usize, cursor, metadata_bytes) catch return error.InvalidQuantizedDirectory;
            if (metadata_end > end) return error.InvalidQuantizedDirectory;
            const scale_raw: []align(@alignOf(f32)) const u8 = @alignCast(self.data[cursor .. cursor + scalar_bytes]);
            cursor += scalar_bytes;
            const error_raw: []align(@alignOf(f32)) const u8 = @alignCast(self.data[cursor .. cursor + scalar_bytes]);
            cursor += scalar_bytes;
            const norm_raw: []align(@alignOf(f32)) const u8 = @alignCast(self.data[cursor .. cursor + scalar_bytes]);
            cursor += scalar_bytes;
            const checksums: []const u32 = if (self.encoded_version >= 4) blk_checksums: {
                const checksum_raw: []align(@alignOf(u32)) const u8 = @alignCast(self.data[cursor .. cursor + scalar_bytes]);
                cursor += scalar_bytes;
                break :blk_checksums std.mem.bytesAsSlice(u32, checksum_raw);
            } else &.{};
            const residual_locations: ?hbc_runtime.NativeResidualLocationPlane = if (flags & entry_flag_residual_locations != 0) blk_locations: {
                if (self.encoded_version < 5) return error.InvalidQuantizedDirectory;
                cursor = std.mem.alignForward(usize, cursor, @alignOf(u64));
                const u64_column_bytes = std.math.mul(usize, count, @sizeOf(u64)) catch return error.InvalidQuantizedDirectory;
                const u32_column_bytes = std.math.mul(usize, count, @sizeOf(u32)) catch return error.InvalidQuantizedDirectory;
                const generations_end = std.math.add(usize, cursor, u64_column_bytes) catch return error.InvalidQuantizedDirectory;
                if (generations_end > end) return error.InvalidQuantizedDirectory;
                const generations_raw: []align(@alignOf(u64)) const u8 = @alignCast(self.data[cursor..generations_end]);
                cursor = std.mem.alignForward(usize, generations_end, @alignOf(u32));
                const shards_end = std.math.add(usize, cursor, u32_column_bytes) catch return error.InvalidQuantizedDirectory;
                if (shards_end > end) return error.InvalidQuantizedDirectory;
                const shards_raw: []align(@alignOf(u32)) const u8 = @alignCast(self.data[cursor..shards_end]);
                cursor = std.mem.alignForward(usize, shards_end, @alignOf(u64));
                const revisions_end = std.math.add(usize, cursor, u64_column_bytes) catch return error.InvalidQuantizedDirectory;
                const offsets_end = std.math.add(usize, revisions_end, u64_column_bytes) catch return error.InvalidQuantizedDirectory;
                if (offsets_end > end) return error.InvalidQuantizedDirectory;
                const revisions_raw: []align(@alignOf(u64)) const u8 = @alignCast(self.data[cursor..revisions_end]);
                const offsets_raw: []align(@alignOf(u64)) const u8 = @alignCast(self.data[revisions_end..offsets_end]);
                cursor = std.mem.alignForward(usize, offsets_end, @alignOf(u32));
                const lengths_end = std.math.add(usize, cursor, u32_column_bytes) catch return error.InvalidQuantizedDirectory;
                const residual_checksums_end = std.math.add(usize, lengths_end, u32_column_bytes) catch return error.InvalidQuantizedDirectory;
                if (residual_checksums_end > end) return error.InvalidQuantizedDirectory;
                const lengths_raw: []align(@alignOf(u32)) const u8 = @alignCast(self.data[cursor..lengths_end]);
                const residual_checksums_raw: []align(@alignOf(u32)) const u8 = @alignCast(self.data[lengths_end..residual_checksums_end]);
                cursor = residual_checksums_end;
                break :blk_locations .{
                    .reader_generations = std.mem.bytesAsSlice(u64, generations_raw),
                    .reader_shard_ids = std.mem.bytesAsSlice(u32, shards_raw),
                    .revisions = std.mem.bytesAsSlice(u64, revisions_raw),
                    .residual_offsets = std.mem.bytesAsSlice(u64, offsets_raw),
                    .residual_lengths = std.mem.bytesAsSlice(u32, lengths_raw),
                    .residual_checksums = std.mem.bytesAsSlice(u32, residual_checksums_raw),
                };
            } else null;
            break :blk .{
                .dims = self.dims,
                .values = std.mem.bytesAsSlice(f16, projection_raw),
                .scales = std.mem.bytesAsSlice(f32, scale_raw),
                .error_norms = std.mem.bytesAsSlice(f32, error_raw),
                .decoded_norm_lower_bounds = std.mem.bytesAsSlice(f32, norm_raw),
                .checksums = checksums,
                .residual_locations = residual_locations,
            };
        } else null;
        if (!std.mem.allEqual(u8, self.data[cursor..end], 0)) return error.InvalidQuantizedDirectory;
        return View{
            .metric = self.metric,
            .centroid = centroid,
            .codes = codes,
            .code_counts = std.mem.bytesAsSlice(u32, code_counts_raw),
            .centroid_distances = std.mem.bytesAsSlice(f32, distances_raw),
            .quantized_dot_products = std.mem.bytesAsSlice(f32, dots_raw),
            .centroid_dot_products = std.mem.bytesAsSlice(f32, centroid_dots_raw),
            .centroid_norm = readF32(self.data, start + 16),
            .member_ids = member_ids,
            .projections = projections,
            .count = count,
            .width = width,
        };
    }
};

/// Lazily authenticates each independently indexed posting exactly once. This
/// preserves mmap-lazy restart while ensuring a corrupted payload can never be
/// consumed merely because the compact directory index itself was valid.
pub const VerifiedReader = struct {
    const unknown: u8 = 0;
    const valid: u8 = 1;
    const corrupt: u8 = 2;

    alloc: Allocator,
    reader: Reader,
    verification: []std.atomic.Value(u8),

    pub fn init(alloc: Allocator, data: []const u8) !VerifiedReader {
        const reader = try Reader.init(data);
        const verification = try alloc.alloc(std.atomic.Value(u8), reader.posting_count);
        for (verification) |*state| state.* = .init(unknown);
        return .{ .alloc = alloc, .reader = reader, .verification = verification };
    }

    pub fn deinit(self: *VerifiedReader) void {
        self.alloc.free(self.verification);
        self.* = undefined;
    }

    pub const EntryLocation = struct {
        offset: u64,
        len: usize,
    };

    /// Exposes the immutable container only for translating a nested offset
    /// into its enclosing generation file. Query callers should continue to
    /// use `get`, which retains the zero-copy mmap view.
    pub fn encodedBytes(self: *const VerifiedReader) []const u8 {
        return self.reader.data;
    }

    /// Returns the exact independently checksummed entry range. Maintenance
    /// can use this with a private cold reader instead of faulting a complete
    /// corpus-sized query mmap while rewriting a generation.
    pub fn entryLocation(self: *const VerifiedReader, posting_id: u64) ?EntryLocation {
        const index = self.reader.findIndex(posting_id) orelse return null;
        const start = self.reader.indexOffset(index);
        const end = if (index + 1 < self.reader.posting_count)
            self.reader.indexOffset(index + 1)
        else
            self.reader.index_offset;
        if (start > end or end > self.reader.index_offset) return null;
        return .{ .offset = @intCast(start), .len = end - start };
    }

    pub const OwnedView = struct {
        alloc: Allocator,
        encoded: []align(entry_alignment) u8,
        view: View,

        pub fn deinit(self: *OwnedView) void {
            self.alloc.free(self.encoded);
            self.* = undefined;
        }
    };

    /// Authenticates and decodes one entry read through a maintenance-private
    /// descriptor. A tiny synthetic one-entry directory lets the canonical
    /// parser remain the sole authority for layout/version validation. The
    /// returned allocation owns every slice in `view` and is bounded by one
    /// leaf rather than the corpus.
    pub fn decodeOwnedEntry(
        self: *VerifiedReader,
        alloc: Allocator,
        posting_id: u64,
        entry: []const u8,
    ) !OwnedView {
        const index = self.reader.findIndex(posting_id) orelse return error.InvalidQuantizedDirectory;
        const location = self.entryLocation(posting_id) orelse return error.InvalidQuantizedDirectory;
        if (entry.len != location.len) return error.InvalidQuantizedDirectory;

        const state = self.verification[index].load(.acquire);
        if (state == corrupt) return error.QuantizedDirectoryChecksumMismatch;
        if (state == unknown) {
            if (std.hash.Crc32.hash(entry) != self.reader.indexChecksum(index)) {
                self.verification[index].store(corrupt, .release);
                return error.QuantizedDirectoryChecksumMismatch;
            }
            self.verification[index].store(valid, .release);
        }

        const entry_offset = std.mem.alignForward(usize, header_size, entry_alignment);
        const index_offset = std.math.add(usize, entry_offset, entry.len) catch
            return error.QuantizedDirectoryTooLarge;
        const total_len = std.math.add(usize, index_offset, index_entry_size) catch
            return error.QuantizedDirectoryTooLarge;
        const encoded = try alloc.alignedAlloc(u8, .fromByteUnits(entry_alignment), total_len);
        errdefer alloc.free(encoded);
        @memset(encoded, 0);
        @memcpy(encoded[entry_offset..index_offset], entry);

        @memcpy(encoded[0..4], &magic);
        writeU16(encoded, 4, self.reader.encoded_version);
        encoded[6] = self.reader.metric;
        writeU32(encoded, 8, @intCast(self.reader.dims));
        writeU64(encoded, 16, 1);
        writeU64(encoded, 24, @intCast(index_offset));
        writeU64(encoded, index_offset, posting_id);
        writeU64(encoded, index_offset + 8, @intCast(entry_offset));
        writeU32(encoded, index_offset + 16, self.reader.indexChecksum(index));
        writeU32(encoded, 12, std.hash.Crc32.hash(encoded[index_offset..]));

        const standalone = try Reader.init(encoded);
        const view = (try standalone.get(posting_id)) orelse return error.InvalidQuantizedDirectory;
        return .{ .alloc = alloc, .encoded = encoded, .view = view };
    }

    pub fn get(self: *VerifiedReader, posting_id: u64) !?View {
        const index = self.reader.findIndex(posting_id) orelse return null;
        const state = self.verification[index].load(.acquire);
        if (state == corrupt) return error.QuantizedDirectoryChecksumMismatch;
        if (state == unknown) {
            const entry = try self.reader.entryBytes(index);
            if (std.hash.Crc32.hash(entry) != self.reader.indexChecksum(index)) {
                self.verification[index].store(corrupt, .release);
                return error.QuantizedDirectoryChecksumMismatch;
            }
            self.verification[index].store(valid, .release);
        }
        return try self.reader.viewAt(index);
    }
};

const TestingSink = struct {
    alloc: Allocator,
    out: std.ArrayListUnmanaged(u8) = .empty,

    fn deinit(self: *TestingSink) void {
        self.out.deinit(self.alloc);
    }

    fn len(self: *const TestingSink) usize {
        return self.out.items.len;
    }

    fn appendSlice(self: *TestingSink, bytes: []const u8) !void {
        try self.out.appendSlice(self.alloc, bytes);
    }

    fn writeAt(self: *TestingSink, offset: usize, bytes: []const u8) !void {
        if (offset > self.out.items.len or bytes.len > self.out.items.len - offset) return error.InvalidOffset;
        @memcpy(self.out.items[offset..][0..bytes.len], bytes);
    }
};

test "streaming quantized directory is byte-compatible with buffered writer" {
    const alloc = std.testing.allocator;
    const set = proto.RaBitQuantizedVectorSet{
        .metric = .cosine,
        .centroid = @constCast(&[_]f32{ 1, 2, 3 }),
        .codes = .{ .count = 2, .width = 1, .data = @constCast(&[_]u64{ 7, 9 }) },
        .code_counts = @constCast(&[_]u32{ 2, 3 }),
        .centroid_distances = @constCast(&[_]f32{ 0.25, 0.5 }),
        .quantized_dot_products = @constCast(&[_]f32{ 1.25, 1.5 }),
        .centroid_dot_products = @constCast(&[_]f32{ 2.25, 2.5 }),
        .centroid_norm = 3.5,
    };
    const member_ids = [_]u64{ 101, 202 };
    const first = [_]f16{ 1, 2, 3 };
    const second = [_]f16{ 4, 5, 6 };
    const first_location: hbc_runtime.NativeResidualLocation = .{
        .reader_generation = 11,
        .reader_shard_id = 2,
        .revision = 31,
        .residual_offset = 4096,
        .residual_len = 19,
        .residual_checksum = 41,
    };
    const second_location: hbc_runtime.NativeResidualLocation = .{
        .reader_generation = 11,
        .reader_shard_id = 3,
        .revision = 32,
        .residual_offset = 8192,
        .residual_len = 23,
        .residual_checksum = 43,
    };
    const projections = [_]hbc_runtime.NativeProjectionBuildValue{
        .{ .bytes = std.mem.sliceAsBytes(&first), .scale = 1, .error_norm = 0.01, .decoded_norm_lower_bound = 3.7, .checksum = 17, .residual_location = first_location },
        .{ .bytes = std.mem.sliceAsBytes(&second), .scale = 2, .error_norm = 0.02, .decoded_norm_lower_bound = 8.7, .checksum = 29, .residual_location = second_location },
    };

    var buffered = try Writer.init(alloc, 3, 2);
    defer buffered.deinit();
    try buffered.appendWithLeafPlanes(7, &set, std.mem.sliceAsBytes(&member_ids), &projections);
    try buffered.append(11, &set);
    const expected = try buffered.build();
    defer alloc.free(expected);

    var sink: TestingSink = .{ .alloc = alloc };
    defer sink.deinit();
    var streaming = try StreamingWriter.init(alloc, &sink, 3, 2);
    defer streaming.deinit();
    try streaming.appendWithLeafPlanes(&sink, 7, &set, std.mem.sliceAsBytes(&member_ids), &projections);
    try streaming.appendWithLeafPlanes(&sink, 11, &set, &.{}, &.{});
    const finish = try streaming.finish(&sink);
    try std.testing.expectEqual(@as(usize, 0), finish.offset);
    try std.testing.expectEqual(expected.len, finish.len);
    try std.testing.expectEqualSlices(u8, expected, sink.out.items);

    var verified = try VerifiedReader.init(alloc, sink.out.items);
    defer verified.deinit();
    const view = (try verified.get(7)).?;
    try std.testing.expectEqualSlices(u64, &member_ids, view.member_ids);
    const locations = view.projections.?.residual_locations.?;
    try std.testing.expectEqual(first_location, locations.at(0).?);
    try std.testing.expectEqual(second_location, locations.at(1).?);

    const location = verified.entryLocation(7).?;
    var owned = try verified.decodeOwnedEntry(
        alloc,
        7,
        sink.out.items[@intCast(location.offset)..][0..location.len],
    );
    defer owned.deinit();
    try std.testing.expectEqualSlices(u64, &member_ids, owned.view.member_ids);
    try std.testing.expectEqualSlices(f16, &first, owned.view.projections.?.values[0..first.len]);
    try std.testing.expectEqual(first_location, owned.view.projections.?.residual_locations.?.at(0).?);
}

test "quantized directory round trips borrowed aligned views" {
    const alloc = std.testing.allocator;
    var writer = try Writer.init(alloc, 3, 2);
    defer writer.deinit();
    const first = proto.RaBitQuantizedVectorSet{
        .metric = .cosine,
        .centroid = @constCast(&[_]f32{ 1, 2, 3 }),
        .codes = .{ .count = 2, .width = 1, .data = @constCast(&[_]u64{ 7, 9 }) },
        .code_counts = @constCast(&[_]u32{ 2, 3 }),
        .centroid_distances = @constCast(&[_]f32{ 0.25, 0.5 }),
        .quantized_dot_products = @constCast(&[_]f32{ 1.25, 1.5 }),
        .centroid_dot_products = @constCast(&[_]f32{ 2.25, 2.5 }),
        .centroid_norm = 3.5,
    };
    const first_ids = [_]u64{ 101, 202 };
    try writer.appendWithMemberBytes(7, &first, std.mem.sliceAsBytes(&first_ids));
    try writer.append(11, &first);
    const encoded = try writer.build();
    defer alloc.free(encoded);
    const reader = try Reader.init(encoded);
    try std.testing.expect((try reader.get(8)) == null);
    const ids_view = (try reader.get(7)).?;
    try std.testing.expectEqualSlices(u64, &first_ids, ids_view.member_ids);
    const view = (try reader.get(11)).?;
    try std.testing.expectEqual(@as(usize, 0), view.member_ids.len);
    try std.testing.expectEqualSlices(f32, first.centroid, view.centroid);
    try std.testing.expectEqualSlices(u64, first.codes.data, view.codes);
    try std.testing.expectEqualSlices(u32, first.code_counts, view.code_counts);
    try std.testing.expectEqual(first.centroid_norm, view.centroid_norm);
    const borrowed = view.asProto();
    try std.testing.expectEqualSlices(u64, first.codes.data, borrowed.codes.data);
}

test "quantized directory co-locates bounded float16 leaf projections" {
    const alloc = std.testing.allocator;
    var writer = try Writer.init(alloc, 3, 2);
    defer writer.deinit();
    const set = proto.RaBitQuantizedVectorSet{
        .metric = .cosine,
        .centroid = @constCast(&[_]f32{ 1, 2, 3 }),
        .codes = .{ .count = 2, .width = 1, .data = @constCast(&[_]u64{ 7, 9 }) },
        .code_counts = @constCast(&[_]u32{ 2, 3 }),
        .centroid_distances = @constCast(&[_]f32{ 0.25, 0.5 }),
        .quantized_dot_products = @constCast(&[_]f32{ 1.25, 1.5 }),
        .centroid_dot_products = @constCast(&[_]f32{ 2.25, 2.5 }),
        .centroid_norm = 3.5,
    };
    const member_ids = [_]u64{ 101, 202 };
    const first = [_]f16{ 1, 2, 3 };
    const second = [_]f16{ 4, 5, 6 };
    const first_location: hbc_runtime.NativeResidualLocation = .{
        .reader_generation = 11,
        .reader_shard_id = 2,
        .revision = 31,
        .residual_offset = 4096,
        .residual_len = 19,
        .residual_checksum = 41,
    };
    const second_location: hbc_runtime.NativeResidualLocation = .{
        .reader_generation = 12,
        .reader_shard_id = 3,
        .revision = 32,
        .residual_offset = 8192,
        .residual_len = 23,
        .residual_checksum = 43,
    };
    const projections = [_]hbc_runtime.NativeProjectionBuildValue{
        .{ .bytes = std.mem.sliceAsBytes(&first), .scale = 1, .error_norm = 0.01, .decoded_norm_lower_bound = 3.7, .checksum = 17, .residual_location = first_location },
        .{ .bytes = std.mem.sliceAsBytes(&second), .scale = 2, .error_norm = 0.02, .decoded_norm_lower_bound = 8.7, .checksum = 29, .residual_location = second_location },
    };
    try writer.appendWithLeafPlanes(7, &set, std.mem.sliceAsBytes(&member_ids), &projections);
    const encoded = try writer.build();
    defer alloc.free(encoded);

    var reader = try VerifiedReader.init(alloc, encoded);
    defer reader.deinit();
    const view = (try reader.get(7)).?;
    const plane = view.projections orelse return error.TestExpectedProjectionPlane;
    try std.testing.expect(plane.validFor(2, 3));
    try std.testing.expectEqualSlices(f16, &first, plane.values[0..3]);
    try std.testing.expectEqualSlices(f16, &second, plane.values[3..6]);
    try std.testing.expectEqualSlices(f32, &.{ 1, 2 }, plane.scales);
    try std.testing.expectEqualSlices(f32, &.{ 0.01, 0.02 }, plane.error_norms);
    try std.testing.expectEqualSlices(f32, &.{ 3.7, 8.7 }, plane.decoded_norm_lower_bounds);
    try std.testing.expectEqualSlices(u32, &.{ 17, 29 }, plane.checksums);
    const locations = plane.residual_locations orelse return error.TestExpectedResidualLocations;
    try std.testing.expectEqual(first_location, locations.at(0).?);
    try std.testing.expectEqual(second_location, locations.at(1).?);
}

test "quantized directory canonicalizes omitted l2 centroid dots" {
    const alloc = std.testing.allocator;
    var writer = try Writer.init(alloc, 2, 0);
    defer writer.deinit();
    const set = proto.RaBitQuantizedVectorSet{
        .metric = .l2_squared,
        .centroid = @constCast(&[_]f32{ 1, 2 }),
        .codes = .{ .count = 2, .width = 1, .data = @constCast(&[_]u64{ 7, 9 }) },
        .code_counts = @constCast(&[_]u32{ 2, 3 }),
        .centroid_distances = @constCast(&[_]f32{ 0.25, 0.5 }),
        .quantized_dot_products = @constCast(&[_]f32{ 1.25, 1.5 }),
        .centroid_dot_products = @constCast(&[_]f32{}),
        .centroid_norm = 3.5,
    };
    try writer.append(7, &set);
    const encoded = try writer.build();
    defer alloc.free(encoded);
    const reader = try Reader.init(encoded);
    const view = (try reader.get(7)).?;
    try std.testing.expectEqualSlices(f32, &.{ 0, 0 }, view.centroid_dot_products);
}

test "quantized directory rejects truncation" {
    const alloc = std.testing.allocator;
    var writer = try Writer.init(alloc, 1, 0);
    defer writer.deinit();
    const set = proto.RaBitQuantizedVectorSet{
        .centroid = @constCast(&[_]f32{1}),
        .codes = .{ .count = 1, .width = 1, .data = @constCast(&[_]u64{1}) },
        .code_counts = @constCast(&[_]u32{1}),
        .centroid_distances = @constCast(&[_]f32{1}),
        .quantized_dot_products = @constCast(&[_]f32{1}),
        .centroid_dot_products = @constCast(&[_]f32{1}),
    };
    try writer.append(1, &set);
    const encoded = try writer.build();
    defer alloc.free(encoded);
    try std.testing.expectError(error.InvalidQuantizedDirectory, Reader.init(encoded[0 .. encoded.len - 1]));
}

test "quantized directory authenticates its compact index eagerly" {
    const alloc = std.testing.allocator;
    var writer = try Writer.init(alloc, 1, 0);
    defer writer.deinit();
    const set = proto.RaBitQuantizedVectorSet{
        .centroid = @constCast(&[_]f32{1}),
        .codes = .{ .count = 1, .width = 1, .data = @constCast(&[_]u64{1}) },
        .code_counts = @constCast(&[_]u32{1}),
        .centroid_distances = @constCast(&[_]f32{1}),
        .quantized_dot_products = @constCast(&[_]f32{1}),
        .centroid_dot_products = @constCast(&[_]f32{1}),
    };
    try writer.append(1, &set);
    const encoded = try writer.build();
    defer alloc.free(encoded);
    encoded[encoded.len - index_entry_size] ^= 1;
    try std.testing.expectError(error.QuantizedDirectoryIndexChecksumMismatch, Reader.init(encoded));
}

test "quantized directory verifies posting payload lazily" {
    const alloc = std.testing.allocator;
    var writer = try Writer.init(alloc, 1, 0);
    defer writer.deinit();
    const set = proto.RaBitQuantizedVectorSet{
        .centroid = @constCast(&[_]f32{1}),
        .codes = .{ .count = 1, .width = 1, .data = @constCast(&[_]u64{1}) },
        .code_counts = @constCast(&[_]u32{1}),
        .centroid_distances = @constCast(&[_]f32{1}),
        .quantized_dot_products = @constCast(&[_]f32{1}),
        .centroid_dot_products = @constCast(&[_]f32{1}),
    };
    try writer.append(1, &set);
    const encoded = try writer.build();
    defer alloc.free(encoded);
    encoded[entry_alignment + entry_header_size] ^= 0x80;
    var reader = try VerifiedReader.init(alloc, encoded);
    defer reader.deinit();
    try std.testing.expectError(error.QuantizedDirectoryChecksumMismatch, reader.get(1));
    try std.testing.expectError(error.QuantizedDirectoryChecksumMismatch, reader.get(1));
}
