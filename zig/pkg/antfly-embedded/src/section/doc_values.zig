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

//! Chunked columnar doc value storage.
//!
//! Doc values provide per-document field values for sorting, faceting, and
//! highlighting. Values are grouped into chunks (default 1024 docs) and
//! Snappy-compressed for efficient random access. Native source streams add
//! authenticated first/last document IDs to each trailing-directory entry.
//! A cursor builds record offsets once per decoded chunk, retaining one chunk.
//!
//! Wire format:
//!   [numChunks: u32 LE]
//!   [chunkOffset_0: u64 LE]   — end offset of chunk 0
//!   [chunkOffset_1: u64 LE]
//!   ...
//!   [chunk_0_data: Snappy-compressed]
//!   [chunk_1_data: Snappy-compressed]
//!   ...
//!
//! Each uncompressed chunk contains:
//!   [numDocs: u32 LE]
//!   For each doc:
//!     [docID: u32 LE]
//!     [valueLen: u32 LE]
//!     [value: bytes]

const std = @import("std");
const Allocator = std.mem.Allocator;
const snappy = @import("../encoding/snappy.zig");

/// Default number of documents per chunk.
pub const default_chunk_size: u32 = 1024;

// ============================================================================
// Doc Values Writer
// ============================================================================

/// Soft byte bound; a single larger value gets its own chunk.
pub const chunk_raw_target = 64 * 1024;
const streamed_flag: u32 = 0x80000000;
const indexed_flag: u32 = 0x40000000;
const count_mask: u32 = 0x3fffffff;

const ChunkBuffer = struct {
    raw: std.ArrayListUnmanaged(u8) = .empty,
    count: u32 = 0,
    last: ?u32 = null,
    max_docs: u32,

    fn needsFlush(self: *const ChunkBuffer, value_len: usize) bool {
        return self.count != 0 and (self.count >= self.max_docs or self.raw.items.len > chunk_raw_target - 8 or value_len > chunk_raw_target - (self.raw.items.len + 8));
    }
    fn add(self: *ChunkBuffer, a: Allocator, doc: u32, value: []const u8) !void {
        if (self.max_docs == 0 or value.len > std.math.maxInt(u32)) return error.InvalidData;
        if (self.last) |last| if (doc <= last) return error.InvalidData;
        const extra = try std.math.add(usize, value.len, if (self.count == 0) 12 else 8);
        try self.raw.ensureUnusedCapacity(a, extra);
        if (self.count == 0) self.raw.appendSliceAssumeCapacity(&@as([4]u8, @splat(0)));
        var entry: [8]u8 = undefined;
        std.mem.writeInt(u32, entry[0..4], doc, .little);
        std.mem.writeInt(u32, entry[4..8], @intCast(value.len), .little);
        self.raw.appendSliceAssumeCapacity(&entry);
        self.raw.appendSliceAssumeCapacity(value);
        self.count += 1;
        self.last = doc;
    }
    fn encode(self: *ChunkBuffer, a: Allocator, compressed: *std.ArrayListUnmanaged(u8)) ![]const u8 {
        std.mem.writeInt(u32, self.raw.items[0..4], self.count, .little);
        return snappy.encodeInto(a, compressed, self.raw.items);
    }
    fn reset(self: *ChunkBuffer, a: Allocator, compressed: *std.ArrayListUnmanaged(u8)) void {
        self.count = 0;
        if (self.raw.capacity > 2 * chunk_raw_target) {
            self.raw.deinit(a);
            self.raw = .empty;
        } else self.raw.clearRetainingCapacity();
        if (compressed.capacity > 2 * chunk_raw_target) {
            compressed.deinit(a);
            compressed.* = .empty;
        } else compressed.clearRetainingCapacity();
    }
};

/// Compatibility writer: keeps compressed output, never the complete raw column.
/// build emits the historical front-directory format with byte-bounded chunks.
pub const DocValuesWriter = struct {
    alloc: Allocator,
    pending: ChunkBuffer,
    compressed: std.ArrayListUnmanaged(u8) = .empty,
    payload: std.ArrayListUnmanaged(u8) = .empty,
    offsets: std.ArrayListUnmanaged(u64) = .empty,

    pub fn init(alloc: Allocator, chunk_size: u32) DocValuesWriter {
        return .{ .alloc = alloc, .pending = .{ .max_docs = chunk_size } };
    }
    pub fn deinit(self: *DocValuesWriter) void {
        self.pending.raw.deinit(self.alloc);
        self.compressed.deinit(self.alloc);
        self.payload.deinit(self.alloc);
        self.offsets.deinit(self.alloc);
    }
    pub fn add(self: *DocValuesWriter, doc: u32, value: []const u8) !void {
        if (self.pending.max_docs == 0 or value.len > std.math.maxInt(u32)) return error.InvalidData;
        if (self.pending.last) |last| if (doc <= last) return error.InvalidData;
        if (self.pending.needsFlush(value.len)) try self.flush();
        try self.pending.add(self.alloc, doc, value);
    }
    fn flush(self: *DocValuesWriter) !void {
        if (self.pending.count == 0) return;
        if (self.offsets.items.len >= count_mask) return error.InvalidData;
        try self.offsets.ensureUnusedCapacity(self.alloc, 1);
        const encoded = try self.pending.encode(self.alloc, &self.compressed);
        try self.payload.appendSlice(self.alloc, encoded);
        self.offsets.appendAssumeCapacity(self.payload.items.len);
        self.pending.reset(self.alloc, &self.compressed);
    }
    pub fn build(self: *DocValuesWriter) ![]u8 {
        try self.flush();
        const prefix = try std.math.add(usize, 4, try std.math.mul(usize, self.offsets.items.len, 8));
        const out = try self.alloc.alloc(u8, try std.math.add(usize, prefix, self.payload.items.len));
        std.mem.writeInt(u32, out[0..4], @intCast(self.offsets.items.len), .little);
        for (self.offsets.items, 0..) |offset, i| std.mem.writeInt(u64, out[4 + i * 8 ..][0..8], offset, .little);
        @memcpy(out[prefix..], self.payload.items);
        return out;
    }
};

/// Stream payload into a private segment sink. The high header bit selects a
/// trailing offset directory, followed by its payload-relative start (u64 LE).
/// Indexed source streams set bit 30 and append first/last doc IDs after every
/// u64 offset. Readers accept both directory variants and the historical format.
pub const StreamingWriter = struct {
    alloc: Allocator,
    sink: *@import("../segment.zig").SegmentSink,
    pending: ChunkBuffer,
    compressed: std.ArrayListUnmanaged(u8) = .empty,
    offsets: std.ArrayListUnmanaged(u64) = .empty,
    bounds: std.ArrayListUnmanaged(struct { first: u32, last: u32 }) = .empty,
    indexed: bool = false,
    start: ?usize = null,
    finished: bool = false,

    pub fn init(alloc: Allocator, sink: *@import("../segment.zig").SegmentSink, chunk_size: u32) StreamingWriter {
        return .{ .alloc = alloc, .sink = sink, .pending = .{ .max_docs = chunk_size } };
    }
    pub fn initIndexed(alloc: Allocator, sink: *@import("../segment.zig").SegmentSink, chunk_size: u32) StreamingWriter {
        var self = init(alloc, sink, chunk_size);
        self.indexed = true;
        return self;
    }
    pub fn deinit(self: *StreamingWriter) void {
        self.pending.raw.deinit(self.alloc);
        self.compressed.deinit(self.alloc);
        self.offsets.deinit(self.alloc);
        self.bounds.deinit(self.alloc);
    }
    pub fn add(self: *StreamingWriter, doc: u32, value: []const u8) !void {
        if (self.finished or self.pending.max_docs == 0 or value.len > std.math.maxInt(u32)) return error.InvalidData;
        if (self.pending.last) |last| if (doc <= last) return error.InvalidData;
        if (self.pending.needsFlush(value.len)) try self.flush();
        try self.pending.add(self.alloc, doc, value);
    }
    fn flush(self: *StreamingWriter) !void {
        if (self.pending.count == 0) return;
        if (self.offsets.items.len >= count_mask) return error.InvalidData;
        try self.offsets.ensureUnusedCapacity(self.alloc, 1);
        if (self.indexed) try self.bounds.ensureUnusedCapacity(self.alloc, 1);
        const encoded = try self.pending.encode(self.alloc, &self.compressed);
        if (self.start == null) {
            const start = self.sink.len();
            try self.sink.appendSlice(&@as([4]u8, @splat(0)));
            self.start = start;
        }
        try self.sink.appendSlice(encoded);
        self.offsets.appendAssumeCapacity(self.sink.len() - self.start.? - 4);
        if (self.indexed) self.bounds.appendAssumeCapacity(.{ .first = std.mem.readInt(u32, self.pending.raw.items[4..8], .little), .last = self.pending.last.? });
        self.pending.reset(self.alloc, &self.compressed);
    }
    pub fn finish(self: *StreamingWriter) !bool {
        if (self.finished) return error.InvalidData;
        try self.flush();
        self.finished = true;
        const start = self.start orelse return false;
        const directory_start = self.sink.len() - start - 4;
        var bytes: [8]u8 = undefined;
        for (self.offsets.items, 0..) |offset, i| {
            std.mem.writeInt(u64, &bytes, offset, .little);
            try self.sink.appendSlice(&bytes);
            if (self.indexed) {
                std.mem.writeInt(u32, bytes[0..4], self.bounds.items[i].first, .little);
                std.mem.writeInt(u32, bytes[4..8], self.bounds.items[i].last, .little);
                try self.sink.appendSlice(&bytes);
            }
        }
        std.mem.writeInt(u64, &bytes, directory_start, .little);
        try self.sink.appendSlice(&bytes);
        var header: [4]u8 = undefined;
        std.mem.writeInt(u32, &header, streamed_flag | (if (self.indexed) indexed_flag else @as(u32, 0)) | @as(u32, @intCast(self.offsets.items.len)), .little);
        try self.sink.writeAt(start, &header);
        return true;
    }
};

// ============================================================================
// Doc Values Reader
// ============================================================================

pub const DocValuesReader = struct {
    alloc: Allocator,
    data: []const u8,
    num_chunks: u32,
    chunk_offsets: []const u8, // raw offset table bytes
    data_start: usize,
    payload_length: u64,
    range: ?@import("../segment_source.zig").View = null,
    owned_offsets: ?[]u8 = null,
    indexed: bool = false,
    fn stride(self: *const DocValuesReader) usize {
        return if (self.indexed) 16 else 8;
    }
    fn endOffset(self: *const DocValuesReader, chunk: u32) u64 {
        return std.mem.readInt(u64, self.chunk_offsets[@as(usize, chunk) * self.stride() ..][0..8], .little);
    }
    fn bounds(self: *const DocValuesReader, chunk: u32) struct { first: u32, last: u32 } {
        const at = @as(usize, chunk) * self.stride() + 8;
        return .{ .first = std.mem.readInt(u32, self.chunk_offsets[at..][0..4], .little), .last = std.mem.readInt(u32, self.chunk_offsets[at + 4 ..][0..4], .little) };
    }
    fn chunkForDoc(self: *const DocValuesReader, doc: u32) ?u32 {
        var low: u32 = 0;
        var high = self.num_chunks;
        while (low < high) {
            const mid = low + (high - low) / 2;
            const range = self.bounds(mid);
            if (doc < range.first) high = mid else if (doc > range.last) low = mid + 1 else return mid;
        }
        return null;
    }

    pub fn init(alloc: Allocator, data: []const u8) !DocValuesReader {
        if (data.len < 4) return error.InvalidData;
        const raw_count = std.mem.readInt(u32, data[0..4], .little);
        const streamed = raw_count & streamed_flag != 0;
        const indexed = raw_count & indexed_flag != 0;
        if (indexed and !streamed) return error.InvalidData;
        const count = raw_count & count_mask;
        const stride_bytes: usize = if (indexed) 16 else 8;
        if (count > (data.len - 4) / stride_bytes) return error.InvalidData;
        const bytes = @as(usize, count) * stride_bytes;
        var directory: usize = 4;
        var start: usize = 4 + bytes;
        var length: usize = data.len - start;
        if (streamed) {
            if (data.len < 12 or bytes > data.len - 12) return error.InvalidData;
            const raw = std.mem.readInt(u64, data[data.len - 8 ..][0..8], .little);
            if (raw != data.len - 12 - bytes) return error.InvalidData;
            start = 4;
            length = @intCast(raw);
            directory = start + length;
        }
        const offsets = data[directory..][0..bytes];
        try validateOffsets(offsets, count, length, indexed);
        return .{ .alloc = alloc, .data = data, .num_chunks = count, .chunk_offsets = offsets, .data_start = start, .payload_length = length, .indexed = indexed };
    }

    fn validateOffsets(offsets: []const u8, count: u32, length: u64, indexed: bool) !void {
        const step: usize = if (indexed) 16 else 8;
        var prior_doc: ?u32 = null;
        var previous: u64 = 0;
        for (0..count) |i| {
            const end = std.mem.readInt(u64, offsets[i * step ..][0..8], .little);
            if (end <= previous or end > length) return error.InvalidData;
            if (indexed) {
                const first = std.mem.readInt(u32, offsets[i * step + 8 ..][0..4], .little);
                const last = std.mem.readInt(u32, offsets[i * step + 12 ..][0..4], .little);
                if (first > last or (prior_doc != null and first <= prior_doc.?)) return error.InvalidData;
                prior_doc = last;
            }
            previous = end;
        }
        if (previous != length) return error.InvalidData;
    }

    /// Borrow the immutable payload and own only its chunk directory.
    pub fn initRanges(alloc: Allocator, view: @import("../segment_source.zig").View) !DocValuesReader {
        if (view.length < 4) return error.InvalidData;
        var header: [4]u8 = undefined;
        try view.readInto(0, &header);
        const raw_count = std.mem.readInt(u32, &header, .little);
        const streamed = raw_count & streamed_flag != 0;
        const indexed = raw_count & indexed_flag != 0;
        if (indexed and !streamed) return error.InvalidData;
        const count = raw_count & count_mask;
        const stride_bytes: usize = if (indexed) 16 else 8;
        const bytes = std.math.mul(usize, count, stride_bytes) catch return error.InvalidData;
        if (bytes > view.length - 4) return error.InvalidData;
        var directory: u64 = 4;
        var start: usize = 4 + bytes;
        var length: u64 = view.length - start;
        if (streamed) {
            if (view.length < 12 or bytes > view.length - 12) return error.InvalidData;
            var trailer: [8]u8 = undefined;
            try view.readInto(view.length - 8, &trailer);
            const raw = std.mem.readInt(u64, &trailer, .little);
            if (raw != view.length - 12 - bytes) return error.InvalidData;
            start = 4;
            length = raw;
            directory = start + length;
        }
        const offsets = try alloc.alloc(u8, bytes);
        errdefer alloc.free(offsets);
        try view.readInto(directory, offsets);
        try validateOffsets(offsets, count, length, indexed);
        return .{ .alloc = alloc, .data = &.{}, .num_chunks = count, .chunk_offsets = offsets, .data_start = start, .payload_length = length, .range = view, .owned_offsets = offsets, .indexed = indexed };
    }
    pub fn deinit(self: *DocValuesReader) void {
        if (self.owned_offsets) |offsets| self.alloc.free(offsets);
        self.* = undefined;
    }

    /// Read a specific chunk, decompress, and return all doc values in it.
    /// Caller owns returned entries.
    pub fn readChunk(self: *const DocValuesReader, chunk_idx: u32) ![]DocValue {
        if (chunk_idx >= self.num_chunks) return error.InvalidChunk;

        const decompressed = try self.decodeChunk(chunk_idx);
        defer self.alloc.free(decompressed);

        // Parse chunk
        var pos: usize = 0;
        if (decompressed.len < 4) return error.InvalidData;
        const num_docs = std.mem.readInt(u32, decompressed[pos..][0..4], .little);
        if (num_docs > (decompressed.len - 4) / 8) return error.InvalidData;
        pos += 4;

        const entries = try self.alloc.alloc(DocValue, num_docs);
        var initialized: usize = 0;
        errdefer {
            for (entries[0..initialized]) |entry| self.alloc.free(entry.value);
            self.alloc.free(entries);
        }
        for (0..num_docs) |i| {
            if (pos > decompressed.len or decompressed.len - pos < 8) return error.InvalidData;
            const doc_id = std.mem.readInt(u32, decompressed[pos..][0..4], .little);
            pos += 4;
            const value_len = std.mem.readInt(u32, decompressed[pos..][0..4], .little);
            pos += 4;
            if (value_len > decompressed.len - pos) return error.InvalidData;
            if (i != 0 and doc_id <= entries[i - 1].doc_id) return error.InvalidData;
            const value = try self.alloc.dupe(u8, decompressed[pos..][0..value_len]);
            pos += value_len;
            entries[i] = .{ .doc_id = doc_id, .value = value };
            initialized += 1;
        }
        if (pos != decompressed.len) return error.InvalidData;
        return entries;
    }

    fn decodeChunk(self: *const DocValuesReader, chunk_idx: u32) ![]u8 {
        if (chunk_idx >= self.num_chunks) return error.InvalidChunk;
        // Get chunk byte range
        const chunk_start: u64 = if (chunk_idx > 0)
            self.endOffset(chunk_idx - 1)
        else
            0;
        const chunk_end = self.endOffset(chunk_idx);

        if (chunk_start > chunk_end or chunk_end > self.payload_length) return error.InvalidData;
        return if (self.range) |view| blk: {
            const part = try @import("../segment_source.zig").View.init(view.source, view.offset + self.data_start + chunk_start, chunk_end - chunk_start);
            break :blk try snappy.decodeFromView(self.alloc, part, std.math.maxInt(usize));
        } else blk: {
            if (self.data_start > self.data.len or chunk_end > self.data.len - self.data_start) return error.InvalidData;
            break :blk try snappy.decode(self.alloc, self.data[self.data_start + @as(usize, @intCast(chunk_start)) .. self.data_start + @as(usize, @intCast(chunk_end))]);
        };
    }

    /// Chunk sizes describe entry counts, not document ID ranges. Search the
    /// actual bounds, parsing borrowed values and copying only the selected one.
    pub fn get(self: *const DocValuesReader, doc_id: u32, chunk_size: u32) !?[]u8 {
        var low: u32 = 0;
        var high = self.num_chunks;
        // Preserve one-chunk access for dense historical columns. The hint is
        // accepted only after inspecting actual IDs, so sparse columns remain
        // correct and callers need not know the writer's chunk size.
        var hint: ?u32 = if (self.indexed) (self.chunkForDoc(doc_id) orelse return null) else if (chunk_size != 0 and doc_id / chunk_size < high) doc_id / chunk_size else null;
        while (low < high) {
            const mid = hint orelse (low + (high - low) / 2);
            hint = null;
            const bytes = try self.decodeChunk(mid);
            defer self.alloc.free(bytes);
            if (bytes.len < 4) return error.InvalidData;
            const count = std.mem.readInt(u32, bytes[0..4], .little);
            if (count == 0 or count > (bytes.len - 4) / 8) return error.InvalidData;
            var pos: usize = 4;
            var first: u32 = 0;
            var last: u32 = 0;
            var selected: ?[]const u8 = null;
            for (0..count) |i| {
                if (bytes.len - pos < 8) return error.InvalidData;
                const id = std.mem.readInt(u32, bytes[pos..][0..4], .little);
                const len = std.mem.readInt(u32, bytes[pos + 4 ..][0..4], .little);
                pos += 8;
                if (len > bytes.len - pos or (i != 0 and id <= last)) return error.InvalidData;
                if (i == 0) first = id;
                last = id;
                if (id == doc_id) selected = bytes[pos..][0..len];
                pos += len;
            }
            if (pos != bytes.len) return error.InvalidData;
            if (self.indexed) {
                const range = self.bounds(mid);
                if (first != range.first or last != range.last) return error.InvalidData;
            }
            if (selected) |value| return try self.alloc.dupe(u8, value);
            if (self.indexed) return null;
            if (doc_id < first) high = mid else if (doc_id > last) low = mid + 1 else return null;
        }
        return null;
    }
};

/// Caller-owned navigation plus one decoded chunk. Borrowed values expire on
/// the next get; no field payload is retained in the shared segment cache.
pub const Cursor = struct {
    const Record = struct { doc: u32, offset: usize, len: u32 };
    /// A cursor and result pages may share one immutable decoded chunk. Moving
    /// the cursor drops its reference; outstanding rows keep payloads valid.
    const Shared = struct {
        a: Allocator,
        refs: std.atomic.Value(usize) = .init(1),
        bytes: []u8,
        records: std.ArrayListUnmanaged(Record),
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.refs.fetchSub(1, .acq_rel) != 1) return;
            const a = self.a;
            a.free(self.bytes);
            self.records.deinit(a);
            a.destroy(self);
        }
    };
    pub fn retainChunk(self: *Cursor) !@import("../storage/rowsource/types.zig").ColumnOwner {
        if (self.chunk == null) return error.InvalidData;
        const shared = self.shared orelse create: {
            const value = try self.reader.alloc.create(Shared);
            value.* = .{ .a = self.reader.alloc, .bytes = self.bytes, .records = self.records };
            self.shared = value;
            break :create value;
        };
        _ = shared.refs.fetchAdd(1, .monotonic);
        return .{ .ptr = shared, .release_fn = Shared.release, .retained_bytes = @sizeOf(Shared) + shared.bytes.len + shared.records.capacity * @sizeOf(Record) };
    }
    fn retireChunk(self: *Cursor) void {
        if (self.shared) |shared| {
            Shared.release(shared);
            self.records = .empty;
            self.shared = null;
        } else self.reader.alloc.free(self.bytes);
        self.bytes = &.{};
        self.chunk = null;
        self.records.clearRetainingCapacity();
    }
    reader: DocValuesReader,
    chunk: ?u32 = null,
    bytes: []u8 = &.{},
    records: std.ArrayListUnmanaged(Record) = .empty,
    first: u32 = 0,
    last: u32 = 0,
    decode_count: usize = 0,
    shared: ?*Shared = null,
    pub fn init(reader: DocValuesReader) Cursor {
        return .{ .reader = reader };
    }
    pub fn deinit(self: *Cursor) void {
        self.retireChunk();
        self.records.deinit(self.reader.alloc);
        self.reader.deinit();
    }
    pub const Prefetch = struct { ranges: usize = 0, bytes: u64 = 0, examined: usize = 0 };
    /// Hints for the next selected chunks, never intervening archive rows.
    /// The enclosing segment authenticates this directory; required get still
    /// validates decoded IDs. The source owns admission, cancellation and joins.
    /// `docs` is sorted global ordinals within [base, end); no pointers escape.
    pub fn prefetchSelected(self: *const Cursor, docs: []const u32, base: u32, end: u32) Prefetch {
        var result: Prefetch = .{};
        if (!self.reader.indexed) return result;
        const view = self.reader.range orelse return result;
        if (view.source != .ranges or view.source.ranges.prefetch == null) return result;
        var position: usize = 0;
        while (position < docs.len and result.ranges < 4 and result.examined < 64) {
            result.examined += 1;
            const doc = docs[position];
            if (doc < base or doc >= end) break;
            const chunk = self.reader.chunkForDoc(doc - base) orelse {
                position += 1;
                continue;
            };
            const last = @as(u64, base) + self.reader.bounds(chunk).last;
            // Skip a dense selected chunk without scanning its rows or
            // issuing duplicate hints for each winner.
            var low = position + 1;
            var high = docs.len;
            while (low < high) {
                const mid = low + (high - low) / 2;
                if (docs[mid] <= last) low = mid + 1 else high = mid;
            }
            position = low;
            if (self.chunk == chunk) continue;
            const start = if (chunk == 0) 0 else self.reader.endOffset(chunk - 1);
            const length = self.reader.endOffset(chunk) - start;
            // Oversized required values remain demand reads. Speculation has
            // a byte cap as well as the existing four-task source queue.
            if (length > 1024 * 1024 - result.bytes) break;
            view.source.prefetch(view.offset + self.reader.data_start + start, length);
            result.ranges += 1;
            result.bytes += length;
        }
        return result;
    }
    pub fn get(self: *Cursor, doc: u32) !?[]const u8 {
        if (self.chunk != null and doc >= self.first and doc <= self.last) return self.find(doc);
        if (self.reader.indexed) {
            const selected = self.reader.chunkForDoc(doc) orelse return null;
            try self.load(selected);
            return self.find(doc);
        }
        // Legacy columns still advance sequentially before falling back to a
        // payload-bound search; new source columns seek in navigation alone.
        var low: u32 = 0;
        var high = self.reader.num_chunks;
        var adjacent: ?u32 = if (self.chunk) |at| (if (doc > self.last and at + 1 < high) at + 1 else if (doc < self.first and at != 0) at - 1 else null) else null;
        while (low < high) {
            const mid = adjacent orelse (low + (high - low) / 2);
            adjacent = null;
            try self.load(mid);
            if (doc < self.first) high = mid else if (doc > self.last) low = mid + 1 else return self.find(doc);
        }
        return null;
    }
    fn load(self: *Cursor, index: u32) !void {
        if (self.chunk == index) return;
        self.retireChunk();
        self.bytes = try self.reader.decodeChunk(index);
        self.decode_count += 1;
        if (self.bytes.len < 4) return error.InvalidData;
        const count = std.mem.readInt(u32, self.bytes[0..4], .little);
        if (count == 0 or count > (self.bytes.len - 4) / 8) return error.InvalidData;
        try self.records.ensureTotalCapacity(self.reader.alloc, count);
        var at: usize = 4;
        var last: ?u32 = null;
        for (0..count) |_| {
            if (self.bytes.len - at < 8) return error.InvalidData;
            const id = std.mem.readInt(u32, self.bytes[at..][0..4], .little);
            const len = std.mem.readInt(u32, self.bytes[at + 4 ..][0..4], .little);
            at += 8;
            if (len > self.bytes.len - at or (last != null and id <= last.?)) return error.InvalidData;
            self.records.appendAssumeCapacity(.{ .doc = id, .offset = at, .len = len });
            last = id;
            at += len;
        }
        if (at != self.bytes.len) return error.InvalidData;
        self.first = self.records.items[0].doc;
        self.last = last.?;
        if (self.reader.indexed) {
            const range = self.reader.bounds(index);
            if (range.first != self.first or range.last != self.last) return error.InvalidData;
        }
        self.chunk = index;
    }
    fn find(self: *const Cursor, doc: u32) ?[]const u8 {
        var low: usize = 0;
        var high = self.records.items.len;
        while (low < high) {
            const mid = low + (high - low) / 2;
            const record = self.records.items[mid];
            if (doc < record.doc) high = mid else if (doc > record.doc) low = mid + 1 else return self.bytes[record.offset..][0..record.len];
        }
        return null;
    }
};

pub const DocValue = struct {
    doc_id: u32,
    value: []u8,
};

// ============================================================================
// Tests
// ============================================================================

test "doc values round-trip" {
    const alloc = std.testing.allocator;
    var writer = DocValuesWriter.init(alloc, 2); // small chunk for testing
    defer writer.deinit();

    try writer.add(0, "hello");
    try writer.add(1, "world");
    try writer.add(2, "foo");

    const data = try writer.build();
    defer alloc.free(data);

    const reader = try DocValuesReader.init(alloc, data);

    // Chunk 0: docs 0,1
    const chunk0 = try reader.readChunk(0);
    defer {
        for (chunk0) |*e| alloc.free(e.value);
        alloc.free(chunk0);
    }
    try std.testing.expectEqual(@as(usize, 2), chunk0.len);
    try std.testing.expectEqual(@as(u32, 0), chunk0[0].doc_id);
    try std.testing.expectEqualStrings("hello", chunk0[0].value);
    try std.testing.expectEqual(@as(u32, 1), chunk0[1].doc_id);
    try std.testing.expectEqualStrings("world", chunk0[1].value);

    // Chunk 1: doc 2
    const chunk1 = try reader.readChunk(1);
    defer {
        for (chunk1) |*e| alloc.free(e.value);
        alloc.free(chunk1);
    }
    try std.testing.expectEqual(@as(usize, 1), chunk1.len);
    try std.testing.expectEqualStrings("foo", chunk1[0].value);
}

test "doc values get by id" {
    const alloc = std.testing.allocator;
    var writer = DocValuesWriter.init(alloc, 1024);
    defer writer.deinit();

    try writer.add(0, "alpha");
    try writer.add(5, "beta");
    try writer.add(10, "gamma");

    const data = try writer.build();
    defer alloc.free(data);

    const reader = try DocValuesReader.init(alloc, data);

    const v0 = (try reader.get(0, 1024)) orelse return error.TestExpectedEqual;
    defer alloc.free(v0);
    try std.testing.expectEqualStrings("alpha", v0);

    const v5 = (try reader.get(5, 1024)) orelse return error.TestExpectedEqual;
    defer alloc.free(v5);
    try std.testing.expectEqualStrings("beta", v5);

    // Non-existent doc
    try std.testing.expect(try reader.get(3, 1024) == null);
}

test "doc values empty" {
    const alloc = std.testing.allocator;
    var writer = DocValuesWriter.init(alloc, 1024);
    defer writer.deinit();

    const data = try writer.build();
    defer alloc.free(data);

    const reader = try DocValuesReader.init(alloc, data);
    try std.testing.expectEqual(@as(u32, 0), reader.num_chunks);
}

test "stored column range reader roundtrips and cleans up partial allocations" {
    const a = std.testing.allocator;
    var writer = DocValuesWriter.init(a, 2);
    defer writer.deinit();
    for (0..6) |i| try writer.add(@intCast(i), "stored value");
    const bytes = try writer.build();
    defer a.free(bytes);
    const view = try @import("../segment_source.zig").View.init(.{ .contiguous = bytes }, 0, bytes.len);
    const Sweep = struct {
        fn run(allocator: Allocator, source: @import("../segment_source.zig").View) !void {
            var reader = try DocValuesReader.initRanges(allocator, source);
            defer reader.deinit();
            const value = (try reader.get(5, 2)).?;
            defer allocator.free(value);
            try std.testing.expectEqualStrings("stored value", value);
        }
    };
    try Sweep.run(a, view);
    try std.testing.checkAllAllocationFailures(a, Sweep.run, .{view});
    const truncated = try @import("../segment_source.zig").View.init(view.source, 0, bytes.len - 1);
    try std.testing.expectError(error.InvalidData, DocValuesReader.initRanges(a, truncated));
}

test "sparse stored column lookup searches entry chunk bounds" {
    const a = std.testing.allocator;
    var writer = DocValuesWriter.init(a, 2);
    defer writer.deinit();
    for ([_]u32{ 100, 102, 900, 1000, 4000 }) |id| try writer.add(id, "value");
    try std.testing.expectError(error.InvalidData, writer.add(4000, "duplicate"));
    try std.testing.expectError(error.InvalidData, writer.add(0, "unordered"));
    const bytes = try writer.build();
    defer a.free(bytes);
    var reader = try DocValuesReader.init(a, bytes);
    defer reader.deinit();
    for ([_]u32{ 100, 102, 900, 1000, 4000 }) |id| {
        const value = (try reader.get(id, 2)).?;
        defer a.free(value);
        try std.testing.expectEqualStrings("value", value);
    }
    for ([_]u32{ 0, 99, 101, 103, 899, 999, 4001 }) |id| try std.testing.expectEqual(@as(?[]u8, null), try reader.get(id, 2));
    const Helper = struct {
        fn run(allocator: Allocator, data: []const u8) !void {
            var range = try DocValuesReader.initRanges(allocator, try @import("../segment_source.zig").View.init(.{ .contiguous = data }, 0, data.len));
            defer range.deinit();
            const value = (try range.get(1000, 2)).?;
            defer allocator.free(value);
            try std.testing.expectEqualStrings("value", value);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Helper.run, .{bytes});
}

test "stored columns byte bound chunks and release oversized writer scratch" {
    const a = std.testing.allocator;
    var writer = DocValuesWriter.init(a, 1024);
    defer writer.deinit();
    const value: [4096]u8 = @splat('v');
    for (0..1024) |doc| try writer.add(@intCast(doc * 2), &value);
    const bytes = try writer.build();
    defer a.free(bytes);
    var reader = try DocValuesReader.init(a, bytes);
    defer reader.deinit();
    try std.testing.expect(reader.num_chunks > 1);
    var largest: usize = 0;
    for (0..reader.num_chunks) |i| {
        const chunk = try reader.decodeChunk(@intCast(i));
        defer a.free(chunk);
        largest = @max(largest, chunk.len);
        try std.testing.expect(chunk.len <= chunk_raw_target);
    }
    const got = (try reader.get(2046, 1024)).?;
    defer a.free(got);
    try std.testing.expectEqualSlices(u8, &value, got);
    const large = try a.alloc(u8, 160 * 1024);
    defer a.free(large);
    @memset(large, 'z');
    try writer.add(2048, large);
    const rebuilt = try writer.build();
    defer a.free(rebuilt);
    try std.testing.expectEqual(@as(usize, 0), writer.pending.raw.capacity);
    var expanded = try DocValuesReader.init(a, rebuilt);
    defer expanded.deinit();
    const exception = (try expanded.get(2048, 1024)).?;
    defer a.free(exception);
    try std.testing.expectEqualSlices(u8, large, exception);
    std.debug.print("LITE_STORED_COLUMN rows=1024 raw_bytes={d} max_chunk={d} chunks={d}\n", .{ value.len * 1024, largest, reader.num_chunks });
}

test "streamed stored columns support sparse legacy and range reads and unwind OOM" {
    const a = std.testing.allocator;
    const Run = struct {
        fn run(allocator: Allocator) !void {
            var memory = @import("../segment.zig").MemorySegmentSink.init(allocator);
            defer memory.deinit();
            var sink = memory.sink();
            // Nonzero base verifies that every directory offset is section-relative.
            try sink.appendSlice("prefix");
            var empty = StreamingWriter.init(allocator, &sink, 1024);
            defer empty.deinit();
            try std.testing.expect(!try empty.finish());
            try std.testing.expectEqual(@as(usize, 6), sink.len());
            var writer = StreamingWriter.init(allocator, &sink, 2);
            defer writer.deinit();
            try writer.add(2, "two");
            try writer.add(100, "hundred");
            try writer.add(300, "last");
            try std.testing.expect(try writer.finish());
            try std.testing.expectError(error.InvalidData, writer.add(301, "late"));
            const bytes = memory.out.items[6..];
            var contiguous = try DocValuesReader.init(allocator, bytes);
            defer contiguous.deinit();
            const view = try @import("../segment_source.zig").View.init(.{ .contiguous = memory.out.items }, 6, bytes.len);
            var ranged = try DocValuesReader.initRanges(allocator, view);
            defer ranged.deinit();
            for ([_]*DocValuesReader{ &contiguous, &ranged }) |reader| {
                const got = (try reader.get(100, 1024)).?;
                defer allocator.free(got);
                try std.testing.expectEqualStrings("hundred", got);
                try std.testing.expect((try reader.get(101, 1024)) == null);
            }
            try sink.writeAt(memory.out.items.len - 8, &(@as([8]u8, @splat(0xff))));
            try std.testing.expectError(error.InvalidData, DocValuesReader.init(allocator, bytes));
            try std.testing.expectError(error.InvalidData, DocValuesReader.initRanges(allocator, view));
        }
    };
    try Run.run(a);
    try std.testing.checkAllAllocationFailures(a, Run.run, .{});
}

test "empty stored values respect the byte limit with large document chunk caps" {
    const a = std.testing.allocator;
    var writer = DocValuesWriter.init(a, 100000);
    defer writer.deinit();
    for (0..16384) |doc| try writer.add(@intCast(doc), "");
    const bytes = try writer.build();
    defer a.free(bytes);
    var reader = try DocValuesReader.init(a, bytes);
    defer reader.deinit();
    try std.testing.expect(reader.num_chunks > 1);
    for (0..reader.num_chunks) |i| {
        const chunk = try reader.decodeChunk(@intCast(i));
        defer a.free(chunk);
        try std.testing.expect(chunk.len <= chunk_raw_target);
    }
    const got = (try reader.get(16383, 100000)).?;
    defer a.free(got);
    try std.testing.expectEqual(@as(usize, 0), got.len);
}

test "external lake bounded window profiles indexed source navigation decodes only selected chunks and unwinds faults" {
    const Run = struct {
        fn run(a: Allocator) !void {
            var memory = @import("../segment.zig").MemorySegmentSink.init(a);
            defer memory.deinit();
            var sink = memory.sink();
            var writer = StreamingWriter.initIndexed(a, &sink, 1);
            defer writer.deinit();
            for (0..32) |i| try writer.add(@intCast(i * 2), "value");
            try std.testing.expect(try writer.finish());
            const view = try @import("../segment_source.zig").View.init(.{ .contiguous = memory.out.items }, 0, memory.out.items.len);
            var cursor = Cursor.init(try DocValuesReader.initRanges(a, view));
            defer cursor.deinit();
            try std.testing.expectEqualStrings("value", (try cursor.get(62)).?);
            try std.testing.expectEqual(@as(usize, 1), cursor.decode_count);
            try std.testing.expect((try cursor.get(61)) == null);
            try std.testing.expectEqual(@as(usize, 1), cursor.decode_count);
            for (0..32) |i| try std.testing.expectEqualStrings("value", (try cursor.get(@intCast(i * 2))).?);
            try std.testing.expectEqual(@as(usize, 33), cursor.decode_count);
            const directory = 4 + cursor.reader.payload_length;
            // Reject an overlapping row-range directory before opening payloads.
            try sink.writeAt(@intCast(directory + 16 + 8), &(@as([4]u8, @splat(0))));
            try std.testing.expectError(error.InvalidData, DocValuesReader.init(a, memory.out.items));
        }
    };
    try Run.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Run.run, .{});
}

test "external lake bounded window profiles source chunk prefetch selects exact ranges bounds bytes and preserves demand ownership" {
    const Run = struct {
        fn run(a: Allocator, large: bool) !void {
            const segment = @import("../segment.zig");
            var memory = segment.MemorySegmentSink.init(a);
            defer memory.deinit();
            var sink = memory.sink();
            try sink.appendSlice("padding");
            var writer = StreamingWriter.initIndexed(a, &sink, 1);
            defer writer.deinit();
            const payload = try a.alloc(u8, if (large) 700 * 1024 else 8);
            defer a.free(payload);
            var random = std.Random.DefaultPrng.init(17);
            random.random().bytes(payload);
            for (0..8) |i| try writer.add(@intCast(i * 2), payload);
            try std.testing.expect(try writer.finish());
            const State = struct {
                bytes: []const u8,
                reads: usize = 0,
                hints: [4]struct { offset: u64, length: u64 } = undefined,
                count: usize = 0,
                fn read(raw: *anyopaque, offset: u64, out: []u8) !void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    if (offset > self.bytes.len or out.len > self.bytes.len - offset) return error.EndOfStream;
                    self.reads += 1;
                    @memcpy(out, self.bytes[@intCast(offset)..][0..out.len]);
                }
                fn prefetch(raw: *anyopaque, offset: u64, length: u64) void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    std.debug.assert(self.count < self.hints.len);
                    self.hints[self.count] = .{ .offset = offset, .length = length };
                    self.count += 1;
                }
                fn close(_: *anyopaque) void {}
            };
            var state: State = .{ .bytes = memory.out.items };
            const source: @import("../segment_source.zig").Source = .{ .ranges = .{ .ptr = &state, .length = state.bytes.len, .read_into = State.read, .prefetch = State.prefetch, .close = State.close } };
            const view = try @import("../segment_source.zig").View.init(source, 7, state.bytes.len - 7);
            var cursor = Cursor.init(try DocValuesReader.initRanges(a, view));
            defer cursor.deinit();
            try std.testing.expectEqualSlices(u8, payload, (try cursor.get(0)).?);
            const reads = state.reads;
            const hint = cursor.prefetchSelected(&.{ 100, 100, 102, 102, 106, 108, 110, 114, 116 }, 100, 116);
            try std.testing.expectEqual(reads, state.reads);
            try std.testing.expectEqual(@as(usize, if (large) 1 else 4), hint.ranges);
            try std.testing.expectEqual(hint.ranges, state.count);
            try std.testing.expect(hint.bytes <= 1024 * 1024);
            const selected = [_]u32{ 1, 3, 4, 5 };
            for (state.hints[0..state.count], selected[0..state.count]) |range, chunk| {
                const start = cursor.reader.endOffset(chunk - 1);
                try std.testing.expectEqual(view.offset + cursor.reader.data_start + start, range.offset);
                try std.testing.expectEqual(cursor.reader.endOffset(chunk) - start, range.length);
            }
            // Prefetch neither changes the live decoded chunk nor transfers
            // value ownership; missing and out-of-segment IDs stay harmless.
            try std.testing.expectEqual(@as(?u32, 0), cursor.chunk);
            try std.testing.expectEqual(@as(usize, 1), cursor.decode_count);
            try std.testing.expectEqual(@as(usize, 0), cursor.prefetchSelected(&.{116}, 100, 116).ranges);
            // Sparse absent rows cannot make each chunk transition scan the
            // entire remaining selection. Bound metadata work independently.
            var missing: [256]u32 = undefined;
            for (&missing, 0..) |*doc, i| doc.* = @intCast(1000 + i);
            const sparse_hint = cursor.prefetchSelected(&missing, 0, 2000);
            try std.testing.expectEqual(@as(usize, 0), sparse_hint.ranges);
            try std.testing.expectEqual(@as(usize, 64), sparse_hint.examined);
            try std.testing.expectEqualSlices(u8, payload, (try cursor.get(14)).?);
            std.debug.print("SOURCE_PREFETCH large={} ranges={} bytes={} sparse_probes={} demand_reads_during_hint=0\n", .{ large, hint.ranges, hint.bytes, sparse_hint.examined });
        }
        fn faults(a: Allocator) !void {
            try run(a, false);
        }
    };
    try Run.run(std.testing.allocator, false);
    try Run.run(std.testing.allocator, true);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Run.faults, .{});
}
