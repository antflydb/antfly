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

//! Chunked integer coder using StreamVByte encoding.
//!
//! Wire-compatible with zapx's streamVByteChunkedIntCoder/Decoder.
//! Supports all chunk formats: legacy varint, StreamVByte, StreamVByte+delta, and columnar.
//!
//! Data is divided into chunks (default 1024 docs) with an offset table for
//! random access. Each chunk is independently encoded.

const std = @import("std");
const Allocator = std.mem.Allocator;
const svb = @import("streamvbyte.zig");

/// Chunk format byte values (wire-compatible with zapx).
pub const ChunkFormat = enum(u8) {
    varint = 0x00, // Legacy varint encoding
    stream_vbyte = 0x01, // StreamVByte encoding
    stream_vbyte_delta = 0x02, // StreamVByte with naive delta encoding
    columnar = 0x03, // Columnar format with delta encoding for start/end
};

// ============================================================================
// Uvarint helpers (protobuf-style, matching Go's binary.Uvarint)
// ============================================================================

fn readUvarint(data: []const u8, pos: *usize) u64 {
    var result: u64 = 0;
    var shift: u6 = 0;
    while (pos.* < data.len) {
        const b = data[pos.*];
        pos.* += 1;
        result |= @as(u64, b & 0x7F) << shift;
        if (b & 0x80 == 0) break;
        shift +|= 7;
    }
    return result;
}

fn uvarintSize(value: u64) usize {
    if (value == 0) return 1;
    var v = value;
    var size: usize = 0;
    while (v > 0) {
        size += 1;
        v >>= 7;
    }
    return size;
}

fn writeUvarint(buf: []u8, value: u64) usize {
    var v = value;
    var i: usize = 0;
    while (v >= 0x80) {
        buf[i] = @as(u8, @truncate(v)) | 0x80;
        v >>= 7;
        i += 1;
    }
    buf[i] = @truncate(v);
    return i + 1;
}

// ============================================================================
// Chunked Int Encoder
// ============================================================================

/// Encodes integers using StreamVByte within chunks.
/// Wire-compatible with zapx's streamVByteChunkedIntCoder.
/// Controls which chunk format the encoder uses.
pub const EncoderMode = enum {
    /// Columnar format (0x03) for location data with delta-encoded starts/ends.
    columnar,
    /// Plain StreamVByte format (0x01) for flat value arrays (freq/norm).
    stream_vbyte,
};

pub const ChunkedIntEncoder = struct {
    alloc: Allocator,
    chunk_size: u64,
    chunk_lens: []u64,
    curr_chunk: u64,
    chunk_values: std.ArrayListUnmanaged(u32),
    final: std.ArrayListUnmanaged(u8),
    mode: EncoderMode,

    pub fn init(alloc: Allocator, chunk_size: u64, max_doc_num: u64) !ChunkedIntEncoder {
        return initWithMode(alloc, chunk_size, max_doc_num, .columnar);
    }

    pub fn initWithMode(alloc: Allocator, chunk_size: u64, max_doc_num: u64, mode: EncoderMode) !ChunkedIntEncoder {
        const total = max_doc_num / chunk_size + 1;
        const chunk_lens = try alloc.alloc(u64, @intCast(total));
        @memset(chunk_lens, 0);

        return .{
            .alloc = alloc,
            .chunk_size = chunk_size,
            .chunk_lens = chunk_lens,
            .curr_chunk = 0,
            .chunk_values = .empty,
            .final = .empty,
            .mode = mode,
        };
    }

    pub fn deinit(self: *ChunkedIntEncoder) void {
        self.alloc.free(self.chunk_lens);
        self.chunk_values.deinit(self.alloc);
        self.final.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn reset(self: *ChunkedIntEncoder) void {
        self.final.clearRetainingCapacity();
        self.chunk_values.clearRetainingCapacity();
        self.curr_chunk = 0;
        @memset(self.chunk_lens, 0);
    }

    /// Add values for a document.
    pub fn add(self: *ChunkedIntEncoder, doc_num: u64, values: []const u32) !void {
        const chunk = doc_num / self.chunk_size;
        if (chunk >= self.chunk_lens.len) return error.DocumentOutOfRange;
        if (chunk != self.curr_chunk) {
            try self.flushChunk();
            self.chunk_values.clearRetainingCapacity();
            self.curr_chunk = chunk;
        }
        try self.chunk_values.appendSlice(self.alloc, values);
    }

    /// Add a single value for a document.
    pub fn add1(self: *ChunkedIntEncoder, doc_num: u64, val: u32) !void {
        const chunk = doc_num / self.chunk_size;
        if (chunk >= self.chunk_lens.len) return error.DocumentOutOfRange;
        if (chunk != self.curr_chunk) {
            try self.flushChunk();
            self.chunk_values.clearRetainingCapacity();
            self.curr_chunk = chunk;
        }
        try self.chunk_values.append(self.alloc, val);
    }

    /// Flush the current chunk (called by add/add1 on chunk boundary and by close).
    fn flushChunk(self: *ChunkedIntEncoder) !void {
        if (self.chunk_values.items.len == 0) {
            self.chunk_lens[@as(usize, @intCast(self.curr_chunk))] = 0;
            return;
        }

        switch (self.mode) {
            .columnar => try self.flushChunkColumnar(),
            .stream_vbyte => try self.flushChunkStreamVByte(),
        }
    }

    /// Flush using columnar format (format 0x03) - matches zapx's closeColumnar.
    fn flushChunkColumnar(self: *ChunkedIntEncoder) !void {
        const vals = self.chunk_values.items;

        // Parse location data into columns
        var counts = std.ArrayListUnmanaged(u32).empty;
        defer counts.deinit(self.alloc);
        var field_ids = std.ArrayListUnmanaged(u32).empty;
        defer field_ids.deinit(self.alloc);
        var positions = std.ArrayListUnmanaged(u32).empty;
        defer positions.deinit(self.alloc);
        var starts = std.ArrayListUnmanaged(u32).empty;
        defer starts.deinit(self.alloc);
        var ends = std.ArrayListUnmanaged(u32).empty;
        defer ends.deinit(self.alloc);
        var num_aps = std.ArrayListUnmanaged(u32).empty;
        defer num_aps.deinit(self.alloc);
        var array_pos = std.ArrayListUnmanaged(u32).empty;
        defer array_pos.deinit(self.alloc);

        var idx: usize = 0;
        while (idx < vals.len) {
            const count = vals[idx];
            try counts.append(self.alloc, count);
            idx += 1;

            const end_idx = idx + count;
            while (idx < end_idx and idx + 4 < end_idx) {
                try field_ids.append(self.alloc, vals[idx]);
                try positions.append(self.alloc, vals[idx + 1]);
                try starts.append(self.alloc, vals[idx + 2]);
                try ends.append(self.alloc, vals[idx + 3]);
                const num_ap = vals[idx + 4];
                try num_aps.append(self.alloc, num_ap);
                idx += 5;

                for (0..num_ap) |_| {
                    if (idx < end_idx) {
                        try array_pos.append(self.alloc, vals[idx]);
                        idx += 1;
                    }
                }
            }
        }

        // Delta encode starts and ends
        const start_deltas = try self.alloc.dupe(u32, starts.items);
        defer self.alloc.free(start_deltas);
        svb.deltaEncode(start_deltas);

        const end_deltas = try self.alloc.dupe(u32, ends.items);
        defer self.alloc.free(end_deltas);
        svb.deltaEncode(end_deltas);

        // Build chunk bytes
        const start_len = self.final.items.len;

        // Format byte
        try self.final.append(self.alloc, @intFromEnum(ChunkFormat.columnar));

        // Header: numDocs, numLocs, numArrayPos
        var hdr_buf: [30]u8 = undefined;
        var n = writeUvarint(&hdr_buf, @intCast(counts.items.len));
        n += writeUvarint(hdr_buf[n..], @intCast(field_ids.items.len));
        n += writeUvarint(hdr_buf[n..], @intCast(array_pos.items.len));
        try self.final.appendSlice(self.alloc, hdr_buf[0..n]);

        // Write each column
        try self.writeColumn(counts.items);
        try self.writeColumn(field_ids.items);
        try self.writeColumn(positions.items);
        try self.writeColumn(start_deltas);
        try self.writeColumn(end_deltas);
        try self.writeColumn(num_aps.items);
        if (array_pos.items.len > 0) {
            try self.writeColumn(array_pos.items);
        }

        self.chunk_lens[@as(usize, @intCast(self.curr_chunk))] = self.final.items.len - start_len;
    }

    /// Encode and write a single StreamVByte column.
    fn writeColumn(self: *ChunkedIntEncoder, values: []const u32) !void {
        if (values.len == 0) {
            try self.final.append(self.alloc, 0); // 0 control length for empty column
            return;
        }

        const encoded = try svb.encode(self.alloc, values);
        defer self.alloc.free(encoded.control);
        defer self.alloc.free(encoded.data);

        // Write controlLen + control + data
        var len_buf: [10]u8 = undefined;
        const n = writeUvarint(&len_buf, @intCast(encoded.control.len));
        try self.final.appendSlice(self.alloc, len_buf[0..n]);
        try self.final.appendSlice(self.alloc, encoded.control);
        try self.final.appendSlice(self.alloc, encoded.data);
    }

    /// Flush using plain StreamVByte format (format 0x01).
    fn flushChunkStreamVByte(self: *ChunkedIntEncoder) !void {
        const vals = self.chunk_values.items;

        const encoded = try svb.encode(self.alloc, vals);
        defer self.alloc.free(encoded.control);
        defer self.alloc.free(encoded.data);

        const start_len = self.final.items.len;

        // Format byte
        try self.final.append(self.alloc, @intFromEnum(ChunkFormat.stream_vbyte));

        // numValues, controlLen
        var hdr_buf: [20]u8 = undefined;
        var n = writeUvarint(&hdr_buf, @intCast(vals.len));
        n += writeUvarint(hdr_buf[n..], @intCast(encoded.control.len));
        try self.final.appendSlice(self.alloc, hdr_buf[0..n]);

        // control + data
        try self.final.appendSlice(self.alloc, encoded.control);
        try self.final.appendSlice(self.alloc, encoded.data);

        self.chunk_lens[@as(usize, @intCast(self.curr_chunk))] = self.final.items.len - start_len;
    }

    /// Close the encoder, flushing the final chunk.
    pub fn close(self: *ChunkedIntEncoder) !void {
        try self.flushChunk();
    }

    /// Serialize to bytes: [numChunks] [offset0] [offset1] ... [chunk data]
    /// Returns owned slice. Caller must free with `alloc`.
    pub fn toBytes(self: *ChunkedIntEncoder) ![]u8 {
        // Convert chunk lengths to end offsets
        const offsets = try self.alloc.dupe(u64, self.chunk_lens);
        defer self.alloc.free(offsets);

        var running: u64 = 0;
        for (offsets) |*off| {
            running += off.*;
            off.* = running;
        }

        // Calculate header size
        var header_size: usize = uvarintSize(@intCast(offsets.len));
        for (offsets) |off| {
            header_size += uvarintSize(off);
        }

        // Build output
        const result = try self.alloc.alloc(u8, header_size + self.final.items.len);
        errdefer self.alloc.free(result);

        var pos: usize = 0;

        // Write numChunks
        pos += writeUvarint(result[pos..], @intCast(offsets.len));

        // Write each offset
        for (offsets) |off| {
            pos += writeUvarint(result[pos..], off);
        }

        // Write chunk data
        @memcpy(result[pos..][0..self.final.items.len], self.final.items);

        return result;
    }
};

// ============================================================================
// Chunked Int Decoder
// ============================================================================

/// Decodes StreamVByte-encoded chunks.
/// Wire-compatible with zapx's streamVByteChunkedIntDecoder.
pub const ChunkedIntDecoder = struct {
    data: []const u8,
    start_offset: u64,
    data_start_offset: u64,
    chunk_offsets: []u64,

    // Current chunk state
    values: std.ArrayListUnmanaged(u32),
    pos: usize,
    format: ChunkFormat,

    alloc: Allocator,

    pub fn init(alloc: Allocator, data: []const u8, offset: u64) !ChunkedIntDecoder {
        var self = ChunkedIntDecoder{
            .data = data,
            .start_offset = offset,
            .data_start_offset = 0,
            .chunk_offsets = &.{},
            .values = .empty,
            .pos = 0,
            .format = .varint,
            .alloc = alloc,
        };

        var read_pos = @as(usize, @intCast(offset));
        const num_chunks = readUvarint(data, &read_pos);

        if (num_chunks > 0) {
            self.chunk_offsets = try alloc.alloc(u64, @intCast(num_chunks));
            for (0..@as(usize, @intCast(num_chunks))) |i| {
                self.chunk_offsets[i] = readUvarint(data, &read_pos);
            }
        }

        self.data_start_offset = @intCast(read_pos);
        return self;
    }

    pub fn deinit(self: *ChunkedIntDecoder) void {
        if (self.chunk_offsets.len > 0) self.alloc.free(self.chunk_offsets);
        self.values.deinit(self.alloc);
        self.* = undefined;
    }

    /// Load and decode a specific chunk.
    pub fn loadChunk(self: *ChunkedIntDecoder, chunk: usize) !void {
        if (chunk >= self.chunk_offsets.len) return error.InvalidChunk;

        // Calculate chunk byte range
        const start_off = self.data_start_offset + if (chunk > 0) self.chunk_offsets[chunk - 1] else 0;
        const end_off = self.data_start_offset + self.chunk_offsets[chunk];
        const chunk_bytes = self.data[@intCast(start_off)..@intCast(end_off)];

        if (chunk_bytes.len == 0) {
            self.values.clearRetainingCapacity();
            self.pos = 0;
            return;
        }

        self.format = @enumFromInt(chunk_bytes[0]);

        switch (self.format) {
            .columnar => try self.loadChunkColumnar(chunk_bytes),
            .stream_vbyte, .stream_vbyte_delta => try self.loadChunkStreamVByte(chunk_bytes),
            .varint => try self.loadChunkVarint(chunk_bytes),
        }
    }

    fn loadChunkStreamVByte(self: *ChunkedIntDecoder, chunk_bytes: []const u8) !void {
        var offset: usize = 1;

        const num_values = readUvarint(chunk_bytes, &offset);
        const control_len = readUvarint(chunk_bytes, &offset);

        const control = chunk_bytes[offset..][0..@intCast(control_len)];
        const data_bytes = chunk_bytes[offset + @as(usize, @intCast(control_len)) ..];

        // Ensure capacity
        self.values.clearRetainingCapacity();
        try self.values.ensureTotalCapacity(self.alloc, @intCast(num_values));
        self.values.items.len = @intCast(num_values);

        _ = svb.decodeInto(control, data_bytes, self.values.items);

        // Delta decode if needed
        if (self.format == .stream_vbyte_delta) {
            svb.deltaDecode(self.values.items);
        }

        self.pos = 0;
    }

    fn loadChunkColumnar(self: *ChunkedIntDecoder, chunk_bytes: []const u8) !void {
        var offset: usize = 1;

        const num_docs = readUvarint(chunk_bytes, &offset);
        const num_locs = readUvarint(chunk_bytes, &offset);
        const num_array_pos = readUvarint(chunk_bytes, &offset);

        // Read columns
        const col_counts = try self.readColumn(chunk_bytes, &offset, @intCast(num_docs));
        defer self.alloc.free(col_counts);
        const col_field_ids = try self.readColumn(chunk_bytes, &offset, @intCast(num_locs));
        defer self.alloc.free(col_field_ids);
        const col_positions = try self.readColumn(chunk_bytes, &offset, @intCast(num_locs));
        defer self.alloc.free(col_positions);
        const start_deltas = try self.readColumn(chunk_bytes, &offset, @intCast(num_locs));
        defer self.alloc.free(start_deltas);
        const end_deltas = try self.readColumn(chunk_bytes, &offset, @intCast(num_locs));
        defer self.alloc.free(end_deltas);
        const col_num_aps = try self.readColumn(chunk_bytes, &offset, @intCast(num_locs));
        defer self.alloc.free(col_num_aps);

        var col_array_pos: []u32 = &.{};
        defer if (col_array_pos.len > 0) self.alloc.free(col_array_pos);
        if (num_array_pos > 0) {
            col_array_pos = try self.readColumn(chunk_bytes, &offset, @intCast(num_array_pos));
        }

        // Delta decode starts and ends
        svb.deltaDecode(start_deltas);
        svb.deltaDecode(end_deltas);

        // Reconstruct interleaved format
        const total_values = @as(usize, @intCast(num_docs)) + @as(usize, @intCast(num_locs)) * 5 + @as(usize, @intCast(num_array_pos));
        self.values.clearRetainingCapacity();
        try self.values.ensureTotalCapacity(self.alloc, total_values);

        var loc_idx: usize = 0;
        var ap_idx: usize = 0;
        for (0..@as(usize, @intCast(num_docs))) |doc_idx| {
            const count = col_counts[doc_idx];
            self.values.appendAssumeCapacity(count);

            var rem: i64 = @intCast(count);
            while (rem > 0 and loc_idx < @as(usize, @intCast(num_locs))) {
                self.values.appendAssumeCapacity(col_field_ids[loc_idx]);
                self.values.appendAssumeCapacity(col_positions[loc_idx]);
                self.values.appendAssumeCapacity(start_deltas[loc_idx]);
                self.values.appendAssumeCapacity(end_deltas[loc_idx]);
                self.values.appendAssumeCapacity(col_num_aps[loc_idx]);

                const num_ap = col_num_aps[loc_idx];
                for (0..num_ap) |_| {
                    if (ap_idx < @as(usize, @intCast(num_array_pos))) {
                        self.values.appendAssumeCapacity(col_array_pos[ap_idx]);
                        ap_idx += 1;
                    }
                }

                rem -= @as(i64, 5 + @as(i64, @intCast(num_ap)));
                loc_idx += 1;
            }
        }

        self.pos = 0;
    }

    fn readColumn(self: *ChunkedIntDecoder, chunk_bytes: []const u8, offset: *usize, num_values: usize) ![]u32 {
        const ctrl_len = readUvarint(chunk_bytes, offset);
        if (ctrl_len == 0) {
            return try self.alloc.alloc(u32, 0);
        }

        const control = chunk_bytes[offset.*..][0..@intCast(ctrl_len)];
        offset.* += @intCast(ctrl_len);

        // Calculate data length from control bytes
        const data_len = svb.dataLength(control);
        const data_bytes = chunk_bytes[offset.*..][0..data_len];
        offset.* += data_len;

        const result = try self.alloc.alloc(u32, num_values);
        _ = svb.decodeInto(control, data_bytes, result);
        return result;
    }

    fn loadChunkVarint(self: *ChunkedIntDecoder, chunk_bytes: []const u8) !void {
        // Legacy varint format - decode sequentially
        self.values.clearRetainingCapacity();
        var offset: usize = 0;
        while (offset < chunk_bytes.len) {
            const val = readUvarint(chunk_bytes, &offset);
            try self.values.append(self.alloc, @intCast(val));
        }
        self.pos = 0;
    }

    /// Read the next value.
    pub fn readValue(self: *ChunkedIntDecoder) ?u32 {
        if (self.pos >= self.values.items.len) return null;
        const val = self.values.items[self.pos];
        self.pos += 1;
        return val;
    }

    /// Read the next n values as a slice (borrows from internal buffer).
    pub fn readValues(self: *ChunkedIntDecoder, n: usize) ?[]const u32 {
        if (self.pos + n > self.values.items.len) return null;
        const result = self.values.items[self.pos..][0..n];
        self.pos += n;
        return result;
    }

    /// Skip n values.
    pub fn skip(self: *ChunkedIntDecoder, n: usize) void {
        self.pos = @min(self.pos + n, self.values.items.len);
    }

    /// Remaining values in current chunk.
    pub fn remaining(self: *const ChunkedIntDecoder) usize {
        if (self.pos >= self.values.items.len) return 0;
        return self.values.items.len - self.pos;
    }

    /// Number of chunks.
    pub fn numChunks(self: *const ChunkedIntDecoder) usize {
        return self.chunk_offsets.len;
    }
};

/// Rebuild the chunk header for a whole-chunk shift, preserving encoded chunk bytes.
/// Useful when document IDs move by an exact multiple of chunk_size.
pub fn prependEmptyChunks(alloc: Allocator, data: []const u8, chunk_delta: usize, total_chunks: usize) ![]u8 {
    var pos: usize = 0;
    const old_num_chunks = readUvarint(data, &pos);
    if (total_chunks < chunk_delta + old_num_chunks) return error.InvalidChunk;

    var old_total_len: u64 = 0;
    for (0..@as(usize, @intCast(old_num_chunks))) |_| {
        old_total_len = readUvarint(data, &pos);
    }

    const chunk_data = data[pos..];

    var header_size: usize = uvarintSize(total_chunks);
    var size_pos: usize = uvarintSize(old_num_chunks);
    var exact_shifted_idx: usize = 0;
    for (0..total_chunks) |chunk_idx| {
        const offset: u64 = if (chunk_idx < chunk_delta)
            0
        else if (exact_shifted_idx < old_num_chunks) blk: {
            const val = readUvarint(data, &size_pos);
            exact_shifted_idx += 1;
            break :blk val;
        } else old_total_len;
        header_size += uvarintSize(offset);
    }

    const result = try alloc.alloc(u8, header_size + chunk_data.len);
    errdefer alloc.free(result);

    pos = 0;
    pos += writeUvarint(result[pos..], total_chunks);
    var src_pos: usize = uvarintSize(old_num_chunks);
    var next_offset: u64 = 0;
    var shifted_idx: usize = 0;
    for (0..total_chunks) |chunk_idx| {
        const offset: u64 = if (chunk_idx < chunk_delta)
            0
        else if (shifted_idx < old_num_chunks) blk: {
            next_offset = readUvarint(data, &src_pos);
            shifted_idx += 1;
            break :blk next_offset;
        } else old_total_len;
        pos += writeUvarint(result[pos..], offset);
    }

    @memcpy(result[pos..][0..chunk_data.len], chunk_data);
    return result;
}

// ============================================================================
// Tests
// ============================================================================

test "ChunkedIntEncoder/Decoder columnar roundtrip" {
    const alloc = std.testing.allocator;

    // Simulate location data: [count, fieldID, pos, start, end, numAP]
    var enc = try ChunkedIntEncoder.init(alloc, 1024, 2);
    defer enc.deinit();

    // Doc 0: 1 location with 5 values (count=5)
    try enc.add(0, &[_]u32{ 5, 1, 10, 100, 110, 0 });
    // Doc 1: 1 location with 5 values (count=5)
    try enc.add(1, &[_]u32{ 5, 1, 20, 200, 220, 0 });

    try enc.close();

    const bytes = try enc.toBytes();
    defer alloc.free(bytes);

    // Decode
    var dec = try ChunkedIntDecoder.init(alloc, bytes, 0);
    defer dec.deinit();

    try dec.loadChunk(0);

    // Doc 0
    try std.testing.expectEqual(@as(?u32, 5), dec.readValue()); // count
    try std.testing.expectEqual(@as(?u32, 1), dec.readValue()); // fieldID
    try std.testing.expectEqual(@as(?u32, 10), dec.readValue()); // pos
    try std.testing.expectEqual(@as(?u32, 100), dec.readValue()); // start
    try std.testing.expectEqual(@as(?u32, 110), dec.readValue()); // end
    try std.testing.expectEqual(@as(?u32, 0), dec.readValue()); // numAP

    // Doc 1
    try std.testing.expectEqual(@as(?u32, 5), dec.readValue()); // count
    try std.testing.expectEqual(@as(?u32, 1), dec.readValue()); // fieldID
    try std.testing.expectEqual(@as(?u32, 20), dec.readValue()); // pos
    try std.testing.expectEqual(@as(?u32, 200), dec.readValue()); // start
    try std.testing.expectEqual(@as(?u32, 220), dec.readValue()); // end
    try std.testing.expectEqual(@as(?u32, 0), dec.readValue()); // numAP
}

test "ChunkedIntEncoder/Decoder with array positions" {
    const alloc = std.testing.allocator;

    var enc = try ChunkedIntEncoder.init(alloc, 1024, 1);
    defer enc.deinit();

    // 1 location: count=7 (5 base fields + 2 array positions)
    // fieldID=2, pos=5, start=50, end=60, numAP=2, ap0=0, ap1=1
    try enc.add(0, &[_]u32{ 7, 2, 5, 50, 60, 2, 0, 1 });

    try enc.close();

    const bytes = try enc.toBytes();
    defer alloc.free(bytes);

    var dec = try ChunkedIntDecoder.init(alloc, bytes, 0);
    defer dec.deinit();

    try dec.loadChunk(0);

    try std.testing.expectEqual(@as(?u32, 7), dec.readValue()); // count
    try std.testing.expectEqual(@as(?u32, 2), dec.readValue()); // fieldID
    try std.testing.expectEqual(@as(?u32, 5), dec.readValue()); // pos
    try std.testing.expectEqual(@as(?u32, 50), dec.readValue()); // start
    try std.testing.expectEqual(@as(?u32, 60), dec.readValue()); // end
    try std.testing.expectEqual(@as(?u32, 2), dec.readValue()); // numAP
    try std.testing.expectEqual(@as(?u32, 0), dec.readValue()); // arrayPos[0]
    try std.testing.expectEqual(@as(?u32, 1), dec.readValue()); // arrayPos[1]
}

test "ChunkedIntEncoder/Decoder multi-chunk" {
    const alloc = std.testing.allocator;

    // Small chunk size to force multiple chunks
    var enc = try ChunkedIntEncoder.init(alloc, 2, 5);
    defer enc.deinit();

    // Chunk 0: docs 0, 1
    try enc.add(0, &[_]u32{ 5, 0, 1, 10, 15, 0 });
    try enc.add(1, &[_]u32{ 5, 0, 2, 20, 25, 0 });

    // Chunk 1: docs 2, 3
    try enc.add(2, &[_]u32{ 5, 0, 3, 30, 35, 0 });
    try enc.add(3, &[_]u32{ 5, 0, 4, 40, 45, 0 });

    try enc.close();

    const bytes = try enc.toBytes();
    defer alloc.free(bytes);

    var dec = try ChunkedIntDecoder.init(alloc, bytes, 0);
    defer dec.deinit();

    // Load and verify chunk 0
    try dec.loadChunk(0);
    try std.testing.expectEqual(@as(?u32, 5), dec.readValue());
    try std.testing.expectEqual(@as(?u32, 0), dec.readValue());
    try std.testing.expectEqual(@as(?u32, 1), dec.readValue());
    try std.testing.expectEqual(@as(?u32, 10), dec.readValue());

    // Load and verify chunk 1
    try dec.loadChunk(1);
    try std.testing.expectEqual(@as(?u32, 5), dec.readValue());
    try std.testing.expectEqual(@as(?u32, 0), dec.readValue());
    try std.testing.expectEqual(@as(?u32, 3), dec.readValue());
    try std.testing.expectEqual(@as(?u32, 30), dec.readValue());
}

test "ChunkedIntDecoder readValues batch" {
    const alloc = std.testing.allocator;

    var enc = try ChunkedIntEncoder.init(alloc, 1024, 1);
    defer enc.deinit();

    try enc.add(0, &[_]u32{ 5, 1, 10, 100, 110, 0 });
    try enc.close();

    const bytes = try enc.toBytes();
    defer alloc.free(bytes);

    var dec = try ChunkedIntDecoder.init(alloc, bytes, 0);
    defer dec.deinit();

    try dec.loadChunk(0);

    // Read count
    try std.testing.expectEqual(@as(?u32, 5), dec.readValue());

    // Read remaining 5 values as batch
    const batch = dec.readValues(5);
    try std.testing.expect(batch != null);
    try std.testing.expectEqual(@as(u32, 1), batch.?[0]); // fieldID
    try std.testing.expectEqual(@as(u32, 10), batch.?[1]); // pos
    try std.testing.expectEqual(@as(u32, 100), batch.?[2]); // start
    try std.testing.expectEqual(@as(u32, 110), batch.?[3]); // end
    try std.testing.expectEqual(@as(u32, 0), batch.?[4]); // numAP
}

test "prependEmptyChunks shifts chunk table without reencoding payload" {
    const alloc = std.testing.allocator;

    var enc = try ChunkedIntEncoder.initWithMode(alloc, 4, 7, .stream_vbyte);
    defer enc.deinit();
    try enc.add(0, &.{ 10, 20 });
    try enc.add(1, &.{ 30, 40 });
    try enc.add(4, &.{ 50, 60 });
    try enc.close();

    const bytes = try enc.toBytes();
    defer alloc.free(bytes);

    const shifted = try prependEmptyChunks(alloc, bytes, 2, 4);
    defer alloc.free(shifted);

    var dec = try ChunkedIntDecoder.init(alloc, shifted, 0);
    defer dec.deinit();

    try std.testing.expectEqual(@as(usize, 4), dec.numChunks());

    try dec.loadChunk(0);
    try std.testing.expectEqual(@as(usize, 0), dec.remaining());
    try dec.loadChunk(1);
    try std.testing.expectEqual(@as(usize, 0), dec.remaining());
    try dec.loadChunk(2);
    try std.testing.expectEqual(@as(?u32, 10), dec.readValue());
    try std.testing.expectEqual(@as(?u32, 20), dec.readValue());
    try std.testing.expectEqual(@as(?u32, 30), dec.readValue());
    try std.testing.expectEqual(@as(?u32, 40), dec.readValue());
    try dec.loadChunk(3);
    try std.testing.expectEqual(@as(?u32, 50), dec.readValue());
    try std.testing.expectEqual(@as(?u32, 60), dec.readValue());
}
