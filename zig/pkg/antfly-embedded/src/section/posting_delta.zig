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

//! Bounded, normalized document contributions for an immutable text hot tail.
//! Analysis, field expansion and position assignment have already happened.
//! Appending a contribution builds no term dictionary, posting blocks or FST.
//! The same checked contributions serve queries and the eventual frozen segment.
const std = @import("std");
const A = std.mem.Allocator;
pub const magic = "AFTPD001";
pub const header_size = 32;
pub const maximum_documents = 256;
pub const maximum_bytes = 4 * 1024 * 1024;
pub const maximum_fragments = 32;
// Frozen posting controls reserve one u32 bit for position presence. Every
// durable delta must remain representable when its window is frozen.
const maximum_frequency = std.math.maxInt(u32) >> 1;

pub fn isDelta(bytes: []const u8) bool {
    return bytes.len >= magic.len and std.mem.eql(u8, bytes[0..magic.len], magic);
}

pub const Stats = struct {
    doc_count: u32,
    doc_space: u32,
    total_field_len: u64,
    body_bytes: u32,
    pub fn read(bytes: []const u8, length: u64) !Stats {
        if (bytes.len < header_size or !isDelta(bytes)) return error.InvalidPostingDelta;
        const result: Stats = .{
            .doc_count = std.mem.readInt(u32, bytes[8..12], .little),
            .doc_space = std.mem.readInt(u32, bytes[12..16], .little),
            .total_field_len = std.mem.readInt(u64, bytes[16..24], .little),
            .body_bytes = std.mem.readInt(u32, bytes[24..28], .little),
        };
        if (length < header_size or length > maximum_bytes or result.doc_space > maximum_documents or result.doc_count > result.doc_space or
            result.body_bytes != length -| header_size or @as(u64, result.doc_count) * 3 > result.body_bytes)
            return error.InvalidPostingDelta;
        return result;
    }
};

pub const Builder = struct {
    alloc: A,
    body: std.ArrayListUnmanaged(u8) = .empty,
    doc_count: u32 = 0,
    previous_doc: ?u32 = null,
    total_field_len: u64 = 0,

    pub fn init(a: A) Builder {
        return .{ .alloc = a };
    }
    pub fn deinit(self: *Builder) void {
        self.body.deinit(self.alloc);
        self.* = undefined;
    }
    fn integer(self: *Builder, value: u64) !void {
        var remaining = value;
        while (remaining >= 128) {
            try self.body.append(self.alloc, @as(u8, @truncate(remaining)) | 128);
            remaining >>= 7;
        }
        try self.body.append(self.alloc, @intCast(remaining));
    }
    pub fn add(self: *Builder, doc: u32, hits: anytype) !void {
        if (doc >= maximum_documents or self.doc_count == maximum_documents or
            (if (self.previous_doc) |previous| doc <= previous else false)) return error.InvalidPostingDelta;
        // Size the exact compact contribution before changing the builder.
        // Common term/frequency/position controls occupy one byte each; there
        // is no per-request FST or fixed-width posting envelope.
        var bytes = self.body.items.len;
        var norm: u32 = 0;
        var length: u32 = 0;
        for (hits) |hit| {
            if (hit.freq == 0 or hit.freq > maximum_frequency or (hit.positions.len != 0 and hit.positions.len != hit.freq)) return error.InvalidPostingDelta;
            const term_length = std.math.cast(u32, hit.term.len) orelse return error.PostingDeltaTooLarge;
            const control = @as(u64, hit.freq) * 2 + @intFromBool(hit.positions.len != 0);
            bytes = std.math.add(usize, bytes, varintBytes(term_length) + varintBytes(control) +| hit.term.len) catch return error.PostingDeltaTooLarge;
            var previous_input: u32 = 0;
            var normalized_position: u32 = 0;
            for (hit.positions) |position| {
                const delta = position -| previous_input;
                normalized_position = try std.math.add(u32, normalized_position, delta);
                bytes = std.math.add(usize, bytes, varintBytes(delta)) catch return error.PostingDeltaTooLarge;
                previous_input = position;
            }
            length = try std.math.add(u32, length, hit.freq);
            norm = @max(norm, hit.norm);
        }
        const count = std.math.cast(u32, hits.len) orelse return error.PostingDeltaTooLarge;
        const field_norm = if (norm == 0) length else norm;
        bytes = std.math.add(usize, bytes, varintBytes(doc) + varintBytes(field_norm) + varintBytes(count)) catch return error.PostingDeltaTooLarge;
        if (bytes > maximum_bytes - header_size) return error.PostingDeltaTooLarge;
        try self.body.ensureTotalCapacity(self.alloc, bytes);
        const start = self.body.items.len;
        errdefer self.body.items.len = start;
        try self.integer(doc);
        try self.integer(field_norm);
        try self.integer(count);
        for (hits) |hit| {
            try self.integer(hit.term.len);
            try self.integer(@as(u64, hit.freq) * 2 + @intFromBool(hit.positions.len != 0));
            try self.body.appendSlice(self.alloc, hit.term);
            var previous_input: u32 = 0;
            for (hit.positions) |position| {
                // Match the frozen codec's non-negative position deltas,
                // including duplicate/out-of-order custom token positions.
                try self.integer(position -| previous_input);
                previous_input = position;
            }
        }
        std.debug.assert(self.body.items.len == bytes);
        self.doc_count += 1;
        self.previous_doc = doc;
        self.total_field_len = try std.math.add(u64, self.total_field_len, length);
    }
    pub fn write(self: *const Builder, sink: anytype, doc_space: u32) !void {
        if (doc_space > maximum_documents or self.doc_count > doc_space or
            (if (self.previous_doc) |previous| previous >= doc_space else false)) return error.InvalidPostingDelta;
        var header: [header_size]u8 = undefined;
        @memcpy(header[0..8], magic);
        std.mem.writeInt(u32, header[8..12], self.doc_count, .little);
        std.mem.writeInt(u32, header[12..16], doc_space, .little);
        std.mem.writeInt(u64, header[16..24], self.total_field_len, .little);
        std.mem.writeInt(u32, header[24..28], @intCast(self.body.items.len), .little);
        var crc = std.hash.Crc32.init();
        crc.update(header[0..28]);
        crc.update(self.body.items);
        std.mem.writeInt(u32, header[28..32], crc.final(), .little);
        try sink.appendSlice(&header);
        try sink.appendSlice(self.body.items);
    }
};

fn varintBytes(value: u64) usize {
    return @max(1, (@as(usize, 64 - @clz(value)) + 6) / 7);
}

const Input = struct {
    bytes: []const u8,
    fn take(self: *Input, count: usize) ![]const u8 {
        if (count > self.bytes.len) return error.InvalidPostingDelta;
        const result = self.bytes[0..count];
        self.bytes = self.bytes[count..];
        return result;
    }
    fn wideInteger(self: *Input) !u64 {
        var value: u64 = 0;
        for (0..10) |index| {
            const byte = (try self.take(1))[0];
            if (index == 9 and byte > 1) return error.InvalidPostingDelta;
            value |= @as(u64, byte & 127) << @intCast(index * 7);
            if (byte & 128 == 0) return value;
        }
        return error.InvalidPostingDelta;
    }
    fn integer(self: *Input) !u32 {
        return std.math.cast(u32, try self.wideInteger()) orelse error.InvalidPostingDelta;
    }
};

/// Decode one bounded field into immutable posting arrays. No source documents,
/// analyzers, dictionary compiler or compressed posting blocks are involved.
/// The caller retains these arrays for every escaping posting iterator.
pub fn decode(a: A, bytes: []const u8) !@import("inverted.zig").InvertedIndexBuilder {
    const stats = try Stats.read(bytes, bytes.len);
    var crc = std.hash.Crc32.init();
    crc.update(bytes[0..28]);
    crc.update(bytes[header_size..]);
    if (crc.final() != std.mem.readInt(u32, bytes[28..32], .little)) return error.InvalidPostingDelta;
    const inverted = @import("inverted.zig");
    var builder = inverted.InvertedIndexBuilder.init(a, inverted.productionIndexConfig());
    errdefer builder.deinit();
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    var input: Input = .{ .bytes = bytes[header_size..] };
    var previous_doc: ?u32 = null;
    for (0..stats.doc_count) |_| {
        _ = scratch.reset(.{ .retain_with_limit = maximum_bytes });
        const temporary = scratch.allocator();
        const doc = try input.integer();
        const norm = try input.integer();
        const count = try input.integer();
        if (doc >= stats.doc_space or (if (previous_doc) |previous| doc <= previous else false) or count > input.bytes.len / 2)
            return error.InvalidPostingDelta;
        const hits = try temporary.alloc(inverted.InvertedIndexBuilder.TermHit, count);
        var field_length: u32 = 0;
        var terms: std.StringHashMapUnmanaged(void) = .empty;
        for (hits) |*hit| {
            const term_bytes = try input.integer();
            const control = try input.wideInteger();
            const freq = std.math.cast(u32, control >> 1) orelse return error.InvalidPostingDelta;
            const locations = if (control & 1 != 0) freq else 0;
            if (freq == 0 or freq > maximum_frequency) return error.InvalidPostingDelta;
            field_length = std.math.add(u32, field_length, freq) catch return error.InvalidPostingDelta;
            const term = try input.take(term_bytes);
            if ((try terms.getOrPut(temporary, term)).found_existing or locations > input.bytes.len) return error.InvalidPostingDelta;
            const positions = try temporary.alloc(u32, locations);
            var previous_position: u32 = 0;
            for (positions) |*position| {
                previous_position = std.math.add(u32, previous_position, try input.integer()) catch return error.InvalidPostingDelta;
                position.* = previous_position;
            }
            hit.* = .{ .term = term, .freq = freq, .norm = norm, .positions = positions };
        }
        try builder.addDocument(doc, hits);
        previous_doc = doc;
    }
    if (input.bytes.len != 0 or builder.total_field_len != stats.total_field_len) return error.InvalidPostingDelta;
    const old_norms = builder.doc_norms.items.len;
    try builder.doc_norms.resize(a, stats.doc_space);
    @memset(builder.doc_norms.items[old_norms..], 0);
    return builder;
}

pub fn materialize(a: A, bytes: []const u8) ![]u8 {
    var builder = try decode(a, bytes);
    defer builder.deinit();
    return builder.build();
}

test "normalized posting delta retains sparse identities unicode positions and exact field statistics" {
    const a = std.testing.allocator;
    const inverted = @import("inverted.zig");
    const segment = @import("../segment.zig");
    var delta = Builder.init(a);
    defer delta.deinit();
    const first = [_]inverted.InvertedIndexBuilder.TermHit{
        .{ .term = "résumé", .freq = 3, .norm = 9, .positions = &.{ 7, 2, 5 } },
        .{ .term = "日本語", .freq = 1, .norm = 9, .positions = &.{3} },
    };
    const second = [_]inverted.InvertedIndexBuilder.TermHit{
        .{ .term = "résumé", .freq = 1, .norm = 5, .positions = &.{0} },
        .{ .term = "κόσμος", .freq = 2, .norm = 5, .positions = &.{ 1, 4 } },
    };
    try delta.add(1, &first);
    try delta.add(4, &second);
    var memory = segment.MemorySegmentSink.init(a);
    defer memory.deinit();
    var sink = memory.sink();
    try delta.write(&sink, 6);
    const stats = try Stats.read(memory.out.items, memory.out.items.len);
    try std.testing.expectEqual(@as(u32, 2), stats.doc_count);
    try std.testing.expectEqual(@as(u32, 6), stats.doc_space);
    const encoded = try materialize(a, memory.out.items);
    defer a.free(encoded);
    var expected = inverted.InvertedIndexBuilder.init(a, inverted.productionIndexConfig());
    defer expected.deinit();
    try expected.addDocument(1, &first);
    try expected.addDocument(4, &second);
    const old_norms = expected.doc_norms.items.len;
    try expected.doc_norms.resize(a, 6);
    @memset(expected.doc_norms.items[old_norms..], 0);
    const control = try expected.build();
    defer a.free(control);
    // Dictionary order, quantized norms and every encoded posting match the
    // normal compiler, including custom non-monotone/duplicate positions.
    try std.testing.expectEqualSlices(u8, control, encoded);
    var reader = try inverted.InvertedIndexReader.init(a, encoded);
    const found = (try reader.lookupChecked("résumé")).?;
    var iterator = try found.iterator(a);
    defer iterator.deinit();
    const hit = (try iterator.next()).?;
    try std.testing.expectEqual(@as(u32, 1), hit.doc_id);
    try std.testing.expectEqualSlices(u32, &.{ 7, 7, 10 }, hit.positions);
    try std.testing.expectEqual(@as(u32, 4), (try iterator.next()).?.doc_id);
    try std.testing.expect((try iterator.next()) == null);
}

test "normalized posting delta bounds contributions before changing its private buffer" {
    const a = std.testing.allocator;
    const inverted = @import("inverted.zig");
    var delta = Builder.init(a);
    defer delta.deinit();
    try delta.add(0, &[_]inverted.InvertedIndexBuilder.TermHit{.{ .term = "hello", .freq = 1, .positions = &.{0} }});
    const before = delta.body.items.len;
    try std.testing.expectError(error.InvalidPostingDelta, delta.add(0, &[_]inverted.InvertedIndexBuilder.TermHit{}));
    try std.testing.expectError(error.InvalidPostingDelta, delta.add(maximum_documents, &[_]inverted.InvertedIndexBuilder.TermHit{}));
    const oversized = try a.alloc(u8, maximum_bytes);
    defer a.free(oversized);
    @memset(oversized, 'x');
    try std.testing.expectError(error.PostingDeltaTooLarge, delta.add(1, &[_]inverted.InvertedIndexBuilder.TermHit{.{ .term = oversized, .freq = 1 }}));
    try std.testing.expectEqual(before, delta.body.items.len);
    try std.testing.expectEqual(@as(u32, 1), delta.doc_count);
}

test "normalized posting delta rejects damaged and unchecked contributions" {
    const a = std.testing.allocator;
    const inverted = @import("inverted.zig");
    const segment = @import("../segment.zig");
    var delta = Builder.init(a);
    defer delta.deinit();
    // Producer violations must also be rejected by the durable decoder.
    const duplicate = [_]inverted.InvertedIndexBuilder.TermHit{ .{ .term = "same", .freq = 1 }, .{ .term = "same", .freq = 1 } };
    try delta.add(0, &duplicate);
    var memory = segment.MemorySegmentSink.init(a);
    defer memory.deinit();
    var sink = memory.sink();
    try delta.write(&sink, 1);
    try std.testing.expectError(error.InvalidPostingDelta, materialize(a, memory.out.items));
    memory.out.items.len = 0;
    var clean = Builder.init(a);
    defer clean.deinit();
    try clean.add(0, duplicate[0..1]);
    try clean.write(&sink, 1);
    try std.testing.expectError(error.InvalidPostingDelta, materialize(a, memory.out.items[0 .. memory.out.items.len - 1]));
    memory.out.items[header_size + 2] ^= 1;
    try std.testing.expectError(error.InvalidPostingDelta, materialize(a, memory.out.items));
}

test "normalized posting delta scopes retain independently escaping postings and dictionary iterators" {
    const a = std.testing.allocator;
    const inverted = @import("inverted.zig");
    const segment = @import("../segment.zig");
    var delta = Builder.init(a);
    defer delta.deinit();
    try delta.add(0, &[_]inverted.InvertedIndexBuilder.TermHit{
        .{ .term = "résumé", .freq = 2, .norm = 3, .positions = &.{ 0, 2 } },
        .{ .term = "日本語", .freq = 1, .norm = 3, .positions = &.{1} },
    });
    var memory = segment.MemorySegmentSink.init(a);
    defer memory.deinit();
    var sink = memory.sink();
    try delta.write(&sink, 1);
    var scope = try inverted.ScopedInvertedIndexReader.initContiguous(a, memory.out.items);
    try std.testing.expectEqual(@as(u64, 0), (try scope.layoutStats()).fst_bytes);
    const result = (try scope.lookup("résumé")).?;
    var postings = try result.iterator(a);
    defer postings.deinit();
    var dictionary = try scope.termIterator();
    defer dictionary.deinit();
    scope.deinit();
    const hit = (try postings.next()).?;
    try std.testing.expectEqual(@as(u32, 0), hit.doc_id);
    try std.testing.expectEqualSlices(u32, &.{ 0, 2 }, hit.positions);
    try std.testing.expect((try postings.next()) == null);
    try std.testing.expectEqualStrings("résumé", (try dictionary.next()).?.term);
    try std.testing.expectEqualStrings("日本語", (try dictionary.next()).?.term);
    try std.testing.expect((try dictionary.next()) == null);
    const view = try @import("../segment_source.zig").View.init(.{ .contiguous = memory.out.items }, 0, memory.out.items.len);
    var ranged = try inverted.ScopedInvertedIndexReader.initRanges(a, view, .{ .normalized_delta = true });
    defer ranged.deinit();
    try std.testing.expectEqual(@as(?u32, 1), try ranged.docFrequency("résumé"));
    try std.testing.expectError(error.SegmentReadBudgetExceeded, inverted.ScopedInvertedIndexReader.initRanges(a, view, .{ .normalized_delta = true, .navigation_bytes = 0 }));
}

test "normalized posting delta direct terms retain multi-block seeks and positions without an FST" {
    const a = std.testing.allocator;
    const inverted = @import("inverted.zig");
    const segment = @import("../segment.zig");
    var delta = Builder.init(a);
    defer delta.deinit();
    for (0..maximum_documents) |i| try delta.add(@intCast(i), &[_]inverted.InvertedIndexBuilder.TermHit{
        .{ .term = "shared", .freq = 2, .norm = 4, .positions = &.{ 1, 3 } },
        .{ .term = "other", .freq = 1, .norm = 4, .positions = &.{0} },
    });
    var memory = segment.MemorySegmentSink.init(a);
    defer memory.deinit();
    var sink = memory.sink();
    try delta.write(&sink, maximum_documents);
    var scope = try inverted.ScopedInvertedIndexReader.initContiguous(a, memory.out.items);
    defer scope.deinit();
    const result = (try scope.lookup("shared")).?;
    try std.testing.expectEqual(@as(u32, maximum_documents), result.docFreq());
    var iterator = try result.iterator(a);
    defer iterator.deinit();
    for (0..maximum_documents) |i| {
        const hit = (try iterator.next()).?;
        try std.testing.expectEqual(@as(u32, @intCast(i)), hit.doc_id);
        try std.testing.expectEqualSlices(u32, &.{ 1, 3 }, hit.positions);
    }
    try std.testing.expect((try iterator.next()) == null);
    var deferred = try result.iterator(a);
    defer deferred.deinit();
    try std.testing.expect(deferred.normalized_postings != null);
    try std.testing.expectEqual(@as(u32, 128), (try deferred.advanceToDeferredPositions(128)).?.doc_id);
    try std.testing.expectEqual(@as(u64, 0), deferred.decodedPositionRecords());
    const selected = try deferred.decodeDeferredPositions();
    try std.testing.expectEqualSlices(u32, &.{ 1, 3 }, selected.positions);
    try std.testing.expectEqual(@as(u64, 1), deferred.decodedPositionRecords());
    try std.testing.expectEqual(@as(usize, 0), deferred.payload_buffer.capacity);
    try std.testing.expectEqual(@as(usize, 0), deferred.freq_values.capacity);
    try std.testing.expectEqual(@as(usize, 0), deferred.positions_buf.capacity);
    try std.testing.expect(try deferred.containsDoc(3));
    try std.testing.expect(try deferred.containsDoc(255));
    try std.testing.expect(!try deferred.containsDoc(256));
    var range = try scope.rangeTermIterator("s", "t");
    defer range.deinit();
    try std.testing.expectEqualStrings("shared", (try range.next()).?.term);
    try std.testing.expect((try range.next()) == null);
    try std.testing.expectEqual(@as(u64, 0), (try scope.layoutStats()).fst_bytes);
}

test "normalized posting delta rejects overflowing frequencies and a foreign document space" {
    const a = std.testing.allocator;
    const inverted = @import("inverted.zig");
    const segment = @import("../segment.zig");
    var delta = Builder.init(a);
    defer delta.deinit();
    try delta.add(0, &[_]inverted.InvertedIndexBuilder.TermHit{
        .{ .term = "aaaa", .freq = 1 }, .{ .term = "bbbb", .freq = 1 },
    });
    var memory = segment.MemorySegmentSink.init(a);
    defer memory.deinit();
    var sink = memory.sink();
    try delta.write(&sink, 1);
    try std.testing.expectError(error.InvalidPostingDelta, inverted.ScopedInvertedIndexReader.initContiguousWithOptions(a, memory.out.items, .{ .normalized_doc_space = 2 }));
    // A valid envelope still cannot hide overflowing per-document counts.
    var malformed = Builder.init(a);
    defer malformed.deinit();
    try malformed.integer(0);
    try malformed.integer(1);
    try malformed.integer(2);
    for ([_][]const u8{ "aaaa", "bbbb" }) |term| {
        try malformed.integer(term.len);
        try malformed.integer(@as(u64, std.math.maxInt(u32)) * 2);
        try malformed.body.appendSlice(a, term);
    }
    malformed.doc_count = 1;
    malformed.previous_doc = 0;
    malformed.total_field_len = 2;
    memory.out.items.len = 0;
    try malformed.write(&sink, 1);
    try std.testing.expectError(error.InvalidPostingDelta, materialize(a, memory.out.items));
}

test "normalized posting delta releases every failed reader and iterator allocation" {
    const a = std.testing.allocator;
    const inverted = @import("inverted.zig");
    const segment = @import("../segment.zig");
    var delta = Builder.init(a);
    defer delta.deinit();
    try delta.add(0, &[_]inverted.InvertedIndexBuilder.TermHit{
        .{ .term = "résumé", .freq = 2, .norm = 3, .positions = &.{ 0, 2 } },
        .{ .term = "日本語", .freq = 1, .norm = 3, .positions = &.{1} },
    });
    var memory = segment.MemorySegmentSink.init(a);
    defer memory.deinit();
    var sink = memory.sink();
    try delta.write(&sink, 1);
    const Scenario = struct {
        fn run(alloc: A, bytes: []const u8) !void {
            var scope = try inverted.ScopedInvertedIndexReader.initContiguous(alloc, bytes);
            defer scope.deinit();
            const result = (try scope.lookup("résumé")).?;
            var postings = try result.iterator(alloc);
            defer postings.deinit();
            var terms = try scope.termIterator();
            defer terms.deinit();
            try std.testing.expectEqualSlices(u32, &.{ 0, 2 }, (try postings.next()).?.positions);
            while (try terms.next()) |_| {}
        }
    };
    try std.testing.checkAllAllocationFailures(a, Scenario.run, .{memory.out.items});
}
