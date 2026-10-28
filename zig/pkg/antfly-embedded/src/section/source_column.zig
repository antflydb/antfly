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

//! Authenticated native source columns. Large strings use independently
//! compressed 64-KiB pages and versioned original-offset analyzer vectors.
const std = @import("std");
const A = std.mem.Allocator;
const dv = @import("doc_values.zig");
const typed = @import("../storage/typed_json.zig");
const analysis = @import("../search/analysis.zig");
const snappy = @import("../encoding/snappy.zig");
const View = @import("../segment_source.zig").View;
const Sink = @import("../segment.zig").SegmentSink;
const Token = @import("antfly_cancellation").CancellationToken;
pub const page_size = 64 * 1024;
const magic = "AFSC0002";
const text_magic = "AFST2";
pub fn fingerprint(a: A, analyzer: *const analysis.Analyzer) ![32]u8 {
    const bytes = try std.json.Stringify.valueAlloc(a, .{ .version = @as(u8, 1), .char_filters = analyzer.char_filters, .tokenizer = analyzer.tokenizer, .filters = analyzer.filters }, .{});
    defer a.free(bytes);
    var digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(bytes, &digest, .{});
    return digest;
}
fn u32add(a: A, out: *std.ArrayList(u8), value: usize) !void {
    var b: [4]u8 = undefined;
    std.mem.writeInt(u32, &b, std.math.cast(u32, value) orelse return error.InvalidData, .little);
    try out.appendSlice(a, &b);
}
fn u64add(a: A, out: *std.ArrayList(u8), value: usize) !void {
    var b: [8]u8 = undefined;
    std.mem.writeInt(u64, &b, value, .little);
    try out.appendSlice(a, &b);
}
pub fn eligibleAnalyzer(analyzer: *const analysis.Analyzer) bool {
    switch (analyzer.tokenizer) {
        .unicode_words, .whitespace, .character => {},
        else => return false,
    }
    for (analyzer.filters) |filter| switch (filter) {
        .suffix, .ngram, .edge_ngram, .shingle, .camel_case => return false,
        else => {},
    };
    return true;
}
/// Writes pages now; the caller stores the small returned descriptor in its
/// ordinary indexed doc-value stream. All offsets are section-relative.
pub fn writeText(a: A, sink: *Sink, start: usize, text: []const u8, analyzers: []const *const analysis.Analyzer) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(a);
    try out.appendSlice(a, text_magic);
    try u32add(a, &out, text.len);
    try u32add(a, &out, (text.len + page_size - 1) / page_size);
    var begin: usize = 0;
    while (begin < text.len) {
        const end = @min(text.len, begin + page_size);
        const encoded = try snappy.encode(a, text[begin..end]);
        defer a.free(encoded);
        try u64add(a, &out, sink.len() - start);
        try u32add(a, &out, encoded.len);
        try u32add(a, &out, end - begin);
        try sink.appendSlice(encoded);
        begin = end;
    }
    var has_continuations = false;
    for (text) |byte| if (byte & 0xc0 == 0x80) {
        has_continuations = true;
        break;
    };
    const bitmap_len = if (has_continuations) (text.len + 7) / 8 else 0;
    try u32add(a, &out, bitmap_len);
    const bitmap_at = out.items.len;
    try out.appendNTimes(a, 0, bitmap_len);
    if (has_continuations) for (text, 0..) |byte, i| if (byte & 0xc0 == 0x80) {
        out.items[bitmap_at + i / 8] |= @as(u8, 1) << @as(u3, @intCast(i % 8));
    };
    try u32add(a, &out, analyzers.len);
    for (analyzers) |analyzer| {
        if (!eligibleAnalyzer(analyzer)) return error.InvalidData;
        try out.appendSlice(a, &try fingerprint(a, analyzer));
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        const sa = scratch.allocator();
        const tokens = try analyzer.analyzeWithSourceOffsets(sa, text);
        var vector: std.ArrayList(u8) = .empty;
        for (tokens) |token| {
            if (token.start_byte > token.end_byte or token.end_byte > text.len) return error.InvalidData;
            try u32add(sa, &vector, token.term.len);
            try vector.appendSlice(sa, token.term);
            try u32add(sa, &vector, token.start_byte);
            try u32add(sa, &vector, token.end_byte);
            try u32add(sa, &vector, token.position);
        }
        const encoded = try snappy.encode(sa, vector.items);
        try u64add(a, &out, sink.len() - start);
        try u32add(a, &out, encoded.len);
        try u32add(a, &out, vector.items.len);
        try u32add(a, &out, tokens.len);
        try sink.appendSlice(encoded);
    }
    return out.toOwnedSlice(a);
}
pub fn beginColumn(sink: *Sink) !usize {
    const start = sink.len();
    try sink.appendSlice(magic);
    return start;
}
pub fn finishColumn(sink: *Sink, start: usize, directory: usize) !void {
    var footer: [8]u8 = undefined;
    std.mem.writeInt(u64, &footer, directory - start, .little);
    try sink.appendSlice(&footer);
}
const Parser = struct {
    bytes: []const u8,
    at: usize = 0,
    fn take(self: *@This(), n: usize) ![]const u8 {
        if (n > self.bytes.len - self.at) return error.InvalidData;
        const value = self.bytes[self.at..][0..n];
        self.at += n;
        return value;
    }
    fn int(self: *@This(), comptime T: type) !T {
        return std.mem.readInt(T, (try self.take(@sizeOf(T)))[0..@sizeOf(T)], .little);
    }
};
pub const Text = struct {
    view: View,
    length: u32,
    pages: []const u8,
    continuations: []const u8,
    variants: []const u8,
    variant_count: u32,
    fn parse(view: View, bytes: []const u8) !Text {
        var p: Parser = .{ .bytes = bytes };
        if (!std.mem.eql(u8, try p.take(5), text_magic)) return error.InvalidData;
        const length = try p.int(u32);
        const count = try p.int(u32);
        if (count != (@as(u64, length) + page_size - 1) / page_size) return error.InvalidData;
        const pages = try p.take(try std.math.mul(usize, count, 16));
        var previous: u64 = 8;
        for (0..count) |i| {
            const part = pages[i * 16 ..][0..16];
            const offset = std.mem.readInt(u64, part[0..8], .little);
            const wire = std.mem.readInt(u32, part[8..12], .little);
            const raw = std.mem.readInt(u32, part[12..16], .little);
            if (offset < previous or offset > view.length or wire == 0 or wire > view.length - offset or raw != @min(page_size, @as(u64, length) - i * page_size)) return error.InvalidData;
            previous = offset + wire;
        }
        const n = try p.int(u32);
        if (n != 0 and n != (@as(u64, length) + 7) / 8) return error.InvalidData;
        const continuations = try p.take(n);
        if (n != 0 and (continuations[0] & 1 != 0 or (length % 8 != 0 and continuations[n - 1] >> @as(u3, @intCast(length % 8)) != 0))) return error.InvalidData;
        const variants = try p.int(u32);
        const entries = try p.take(try std.math.mul(usize, variants, 52));
        if (p.at != bytes.len) return error.InvalidData;
        for (0..variants) |i| {
            const entry = entries[i * 52 ..][0..52];
            const offset = std.mem.readInt(u64, entry[32..40], .little);
            const wire = std.mem.readInt(u32, entry[40..44], .little);
            const raw = std.mem.readInt(u32, entry[44..48], .little);
            if (offset < previous or offset > view.length or wire == 0 or wire > view.length - offset or raw > @as(u64, length) * 64 + 64 or std.mem.readInt(u32, entry[48..52], .little) > raw / 16) return error.InvalidData;
            previous = offset + wire;
        }
        return .{ .view = view, .length = length, .pages = pages, .continuations = continuations, .variant_count = variants, .variants = entries };
    }
    pub const Vector = struct {
        a: A,
        bytes: []u8,
        items: []analysis.Token,
        pub fn deinit(self: Vector) void {
            self.a.free(self.items);
            self.a.free(self.bytes);
        }
    };
    /// Only the requested analyzer vector is fetched. Projection needs just
    /// page navigation, never these independently compressed token payloads.
    pub fn tokens(self: Text, a: A, signature: [32]u8, control: Token) !?Vector {
        var p: Parser = .{ .bytes = self.variants };
        var selected: ?[]const u8 = null;
        var prior: u64 = if (self.pages.len == 0) 8 else last: {
            const page = self.pages[self.pages.len - 16 ..][0..16];
            break :last std.mem.readInt(u64, page[0..8], .little) + std.mem.readInt(u32, page[8..12], .little);
        };
        for (0..self.variant_count) |_| {
            try control.check();
            const entry = try p.take(52);
            const offset = std.mem.readInt(u64, entry[32..40], .little);
            const wire = std.mem.readInt(u32, entry[40..44], .little);
            const raw = std.mem.readInt(u32, entry[44..48], .little);
            const count = std.mem.readInt(u32, entry[48..52], .little);
            if (offset < prior or offset > self.view.length or wire == 0 or wire > self.view.length - offset or raw > @as(u64, self.length) * 64 + 64 or count > raw / 16) return error.InvalidData;
            prior = offset + wire;
            if (std.mem.eql(u8, entry[0..32], &signature)) {
                if (selected != null) return error.InvalidData;
                selected = entry;
            }
        }
        if (p.at != p.bytes.len) return error.InvalidData;
        const entry = selected orelse return null;
        const encoded = try a.alloc(u8, std.mem.readInt(u32, entry[40..44], .little));
        defer a.free(encoded);
        try control.check();
        try self.view.readInto(std.mem.readInt(u64, entry[32..40], .little), encoded);
        const bytes = try a.alloc(u8, std.mem.readInt(u32, entry[44..48], .little));
        errdefer a.free(bytes);
        try snappy.decodeInto(encoded, bytes);
        var vector: Parser = .{ .bytes = bytes };
        const items = try a.alloc(analysis.Token, std.mem.readInt(u32, entry[48..52], .little));
        errdefer a.free(items);
        for (items, 0..) |*token, i| {
            if (i % 256 == 0) try control.check();
            const term = try vector.take(try vector.int(u32));
            const start = try vector.int(u32);
            const end = try vector.int(u32);
            const position = try vector.int(u32);
            if (start > end or end > self.length) return error.InvalidData;
            token.* = .{ .term = term, .start_byte = start, .end_byte = end, .position = position };
        }
        if (vector.at != bytes.len) return error.InvalidData;
        return .{ .a = a, .bytes = bytes, .items = items };
    }
    fn continuation(self: Text, offset: u32) bool {
        return offset < self.length and self.continuations.len != 0 and self.continuations[offset / 8] & (@as(u8, 1) << @as(u3, @intCast(offset % 8))) != 0;
    }
    pub fn floor(self: Text, offset: u32) u32 {
        var at = offset;
        var n: u8 = 0;
        while (at > 0 and self.continuation(at) and n < 3) : (n += 1) at -= 1;
        return at;
    }
    pub fn ceil(self: Text, offset: u32) u32 {
        var at = offset;
        var n: u8 = 0;
        while (at < self.length and self.continuation(at) and n < 3) : (n += 1) at += 1;
        return at;
    }
    pub fn read(self: Text, a: A, first: u32, end: u32, control: Token) ![]u8 {
        if (first > end or end > self.length) return error.InvalidData;
        const out = try a.alloc(u8, end - first);
        errdefer a.free(out);
        var begin = first;
        var hinted: usize = first / page_size + 1;
        const stop_page = (@as(usize, end) + page_size - 1) / page_size;
        while (begin < end) {
            try control.check();
            const index = begin / page_size;
            // Required page plus at most four future pages, all inside the
            // requested string interval. The source owns task/byte admission,
            // cancellation, authentication and joining these advisory reads.
            const through = @min(stop_page, @as(usize, index) + 5);
            while (hinted < through) : (hinted += 1) {
                const future = self.pages[hinted * 16 ..][0..16];
                self.view.source.prefetch(self.view.offset + std.mem.readInt(u64, future[0..8], .little), std.mem.readInt(u32, future[8..12], .little));
            }
            const entry = self.pages[index * 16 ..][0..16];
            const offset = std.mem.readInt(u64, entry[0..8], .little);
            const n = std.mem.readInt(u32, entry[8..12], .little);
            const encoded = try a.alloc(u8, n);
            defer a.free(encoded);
            try self.view.readInto(offset, encoded);
            const raw = try a.alloc(u8, std.mem.readInt(u32, entry[12..16], .little));
            defer a.free(raw);
            try snappy.decodeInto(encoded, raw);
            const at = begin % page_size;
            const stop = @min(end - begin, raw.len - at);
            @memcpy(out[begin - first ..][0..stop], raw[at..][0..stop]);
            begin += @intCast(stop);
        }
        return out;
    }
};
pub const Reader = struct {
    directory: dv.DocValuesReader,
    source: ?View = null,
    pub fn init(a: A, view: View) !Reader {
        var header: [8]u8 = undefined;
        if (view.length >= 16) {
            try view.readInto(0, &header);
            if (std.mem.eql(u8, &header, magic)) {
                try view.readInto(view.length - 8, &header);
                const start = std.mem.readInt(u64, &header, .little);
                if (start < 8 or start > view.length - 8) return error.InvalidData;
                return .{ .directory = try dv.DocValuesReader.initRanges(a, try View.init(view.source, view.offset + start, view.length - 8 - start)), .source = try View.init(view.source, view.offset, start) };
            }
        }
        return .{ .directory = try dv.DocValuesReader.initRanges(a, view) };
    }
    pub fn deinit(self: *Reader) void {
        self.directory.deinit();
    }
    pub fn get(self: *Reader, doc: u32, chunk: u32) !?[]u8 {
        const bytes = (try self.directory.get(doc, chunk)) orelse return null;
        if (!std.mem.startsWith(u8, bytes, text_magic)) return bytes;
        defer self.directory.alloc.free(bytes);
        const text = try Text.parse(self.source orelse return error.InvalidData, bytes);
        const raw = try text.read(self.directory.alloc, 0, text.length, .none);
        defer self.directory.alloc.free(raw);
        return try typed.encodeBinary(self.directory.alloc, .{ .string = raw });
    }
};
pub const Cursor = struct {
    directory: dv.Cursor,
    source: ?View,
    full: []u8 = &.{},
    chunk: ?u32 = null,
    decode_count: usize = 0,
    control: Token = .none,
    pub fn init(reader: Reader) Cursor {
        return .{ .directory = .init(reader.directory), .source = reader.source };
    }
    pub fn deinit(self: *Cursor) void {
        self.directory.reader.alloc.free(self.full);
        self.directory.deinit();
    }
    pub fn text(self: *Cursor, doc: u32) !?Text {
        const bytes = (try self.directory.get(doc)) orelse return null;
        self.chunk = self.directory.chunk;
        self.decode_count = self.directory.decode_count;
        if (!std.mem.startsWith(u8, bytes, text_magic)) return null;
        return try Text.parse(self.source orelse return error.InvalidData, bytes);
    }
    pub fn get(self: *Cursor, doc: u32) !?[]const u8 {
        self.directory.reader.alloc.free(self.full);
        self.full = &.{};
        const bytes = (try self.directory.get(doc)) orelse return null;
        self.chunk = self.directory.chunk;
        self.decode_count = self.directory.decode_count;
        if (!std.mem.startsWith(u8, bytes, text_magic)) return bytes;
        const value = try Text.parse(self.source orelse return error.InvalidData, bytes);
        const a = self.directory.reader.alloc;
        const raw = try value.read(a, 0, value.length, self.control);
        defer a.free(raw);
        self.full = try typed.encodeBinary(a, .{ .string = raw });
        return self.full;
    }
    pub fn prefetchSelected(self: *const Cursor, docs: []const u32, base: u32, end: u32) dv.Cursor.Prefetch {
        return self.directory.prefetchSelected(docs, base, end);
    }
};

pub const Writer = struct {
    const Record = struct { doc: u32, bytes: []u8 };
    a: A,
    sink: *Sink,
    start: usize,
    records: std.ArrayList(Record) = .empty,
    retained: usize = 0,
    pub fn init(a: A, sink: *Sink) !Writer {
        return .{ .a = a, .sink = sink, .start = try beginColumn(sink) };
    }
    pub fn deinit(self: *Writer) void {
        for (self.records.items) |record| self.a.free(record.bytes);
        self.records.deinit(self.a);
    }
    fn append(self: *Writer, doc: u32, bytes: []u8) !void {
        errdefer self.a.free(bytes);
        if (self.records.items.len != 0 and self.records.items[self.records.items.len - 1].doc >= doc) return error.InvalidData;
        try self.records.append(self.a, .{ .doc = doc, .bytes = bytes });
        self.retained += bytes.len;
    }
    pub fn add(self: *Writer, doc: u32, value: std.json.Value, analyzers: []const *const analysis.Analyzer) !void {
        const bytes = if (value == .string and value.string.len > 4096 and std.unicode.utf8ValidateSlice(value.string))
            try writeText(self.a, self.sink, self.start, value.string, analyzers)
        else
            try typed.encodeBinary(self.a, value);
        try self.append(doc, bytes);
    }
    pub fn copy(self: *Writer, doc: u32, cursor: *Cursor, original: u32) !void {
        if (try cursor.text(original)) |text| {
            if (self.records.items.len != 0 and self.records.items[self.records.items.len - 1].doc >= doc) return error.InvalidData;
            const descriptor = (try cursor.directory.get(original)).?;
            const bytes = try self.a.dupe(u8, descriptor);
            errdefer self.a.free(bytes);
            for (0..text.pages.len / 16) |i| {
                const page = text.pages[i * 16 ..][0..16];
                const offset = std.mem.readInt(u64, page[0..8], .little);
                const wire = std.mem.readInt(u32, page[8..12], .little);
                const encoded = try self.a.alloc(u8, wire);
                defer self.a.free(encoded);
                try text.view.readInto(offset, encoded);
                std.mem.writeInt(u64, bytes[13 + i * 16 ..][0..8], self.sink.len() - self.start, .little);
                try self.sink.appendSlice(encoded);
            }
            const variants_at = descriptor.len - text.variants.len;
            for (0..text.variant_count) |i| {
                const entry = text.variants[i * 52 ..][0..52];
                const wire = std.mem.readInt(u32, entry[40..44], .little);
                const encoded = try self.a.alloc(u8, wire);
                defer self.a.free(encoded);
                try text.view.readInto(std.mem.readInt(u64, entry[32..40], .little), encoded);
                std.mem.writeInt(u64, bytes[variants_at + i * 52 + 32 ..][0..8], self.sink.len() - self.start, .little);
                try self.sink.appendSlice(encoded);
            }
            try self.records.append(self.a, .{ .doc = doc, .bytes = bytes });
            self.retained += bytes.len;
        } else if (try cursor.get(original)) |bytes| try self.append(doc, try self.a.dupe(u8, bytes));
    }
    pub fn finish(self: *Writer) !bool {
        if (self.records.items.len == 0) return false;
        const directory = self.sink.len();
        var writer = dv.StreamingWriter.initIndexed(self.a, self.sink, 1024);
        defer writer.deinit();
        for (self.records.items) |record| try writer.add(record.doc, record.bytes);
        if (!try writer.finish()) return error.InvalidData;
        try finishColumn(self.sink, self.start, directory);
        return true;
    }
};

test "highlight native source compaction preserves analyzer vectors and section-relative text pages" {
    const a = std.testing.allocator;
    const segment = @import("../segment.zig");
    const text = try a.alloc(u8, 8192);
    defer a.free(text);
    @memset(text, '.');
    @memcpy(text[4090..][0.."é needle".len], "é needle");
    var original = segment.MemorySegmentSink.init(a);
    defer original.deinit();
    var first = original.sink();
    var writer = try Writer.init(a, &first);
    defer writer.deinit();
    try writer.add(7, .{ .string = text }, &.{&analysis.simple_analyzer});
    try std.testing.expect(try writer.finish());
    const source = try View.init(.{ .contiguous = original.out.items }, 0, original.out.items.len);
    const Run = struct {
        fn run(alloc: A, input: View, expected: []const u8) !void {
            var cursor = Cursor.init(try Reader.init(alloc, input));
            defer cursor.deinit();
            var memory = segment.MemorySegmentSink.init(alloc);
            defer memory.deinit();
            var sink = memory.sink();
            try sink.appendSlice("prefix");
            var compacted = try Writer.init(alloc, &sink);
            defer compacted.deinit();
            try compacted.copy(0, &cursor, 7);
            try compacted.copy(2, &cursor, 7);
            try std.testing.expect(try compacted.finish());
            var copied = Cursor.init(try Reader.init(alloc, try View.init(.{ .contiguous = memory.out.items }, 6, memory.out.items.len - 6)));
            defer copied.deinit();
            for ([_]u32{ 2, 0 }) |doc| {
                const value = (try copied.text(doc)).?;
                const raw = try value.read(alloc, 0, value.length, .none);
                defer alloc.free(raw);
                try std.testing.expectEqualStrings(expected, raw);
                const tokens = (try value.tokens(alloc, try fingerprint(alloc, &analysis.simple_analyzer), .none)).?;
                defer tokens.deinit();
                try std.testing.expectEqual(@as(usize, 2), tokens.items.len);
                try std.testing.expectEqualStrings("needle", tokens.items[1].term);
                try std.testing.expectEqual(@as(u32, 4093), tokens.items[1].start_byte);
            }
        }
    };
    try Run.run(a, source, text);
    var stable = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(stable.allocator(), Run.run, .{ source, text });
}
