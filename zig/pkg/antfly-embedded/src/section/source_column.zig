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
const offsets = @import("source_offsets.zig");
pub const page_size = 64 * 1024;
const magic = "AFSC0005";
const text_magic = "AFST5";
const inline_magic = "AFSI1";
/// Below this size ordinary analysis is cheaper than persisting navigation.
pub const offset_min_bytes = 256;
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
pub const Variant = struct { signature: [32]u8, offset: usize, bytes: usize, root_raw: u32, count: u32 };
pub const Variants = struct { view: View, items: []const Variant };
/// The standalone writer shares the indexing producer's codec. Native lake
/// ingestion supplies already captured variants and does not analyze here.
pub fn writeText(a: A, sink: *Sink, start: usize, text: []const u8, analyzers: []const *const analysis.Analyzer) ![]u8 {
    var encoded = @import("../segment.zig").MemorySegmentSink.init(a);
    defer encoded.deinit();
    var output = encoded.sink();
    var variants: std.ArrayList(Variant) = .empty;
    defer variants.deinit(a);
    for (analyzers) |analyzer| {
        if (!eligibleAnalyzer(analyzer)) return error.InvalidData;
        const signature = try fingerprint(a, analyzer);
        if (for (variants.items) |value| {
            if (std.mem.eql(u8, &value.signature, &signature)) break true;
        } else false) continue;
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        const tokens = try analyzer.analyzeWithSourceOffsets(scratch.allocator(), text);
        const begin = output.len();
        const value = try offsets.write(scratch.allocator(), &output, tokens);
        try variants.append(a, .{ .signature = signature, .offset = begin, .bytes = value.bytes, .root_raw = value.root_raw, .count = value.count });
    }
    return writeTextPrepared(a, sink, start, text, .{ .view = try View.init(.{ .contiguous = encoded.out.items }, 0, encoded.out.items.len), .items = variants.items });
}
fn copyRange(sink: *Sink, view: View, offset: u64, length: usize) !void {
    var buffer: [page_size]u8 = undefined;
    var at: usize = 0;
    while (at < length) {
        const n = @min(buffer.len, length - at);
        try view.readInto(offset + at, buffer[0..n]);
        try sink.appendSlice(buffer[0..n]);
        at += n;
    }
}
/// Writes pages now; the caller stores the small returned descriptor in its
/// ordinary indexed doc-value stream. All offsets are section-relative.
pub fn writeTextPrepared(a: A, sink: *Sink, start: usize, text: []const u8, captured: Variants) ![]u8 {
    if (text.len > 4096 or captured.items.len == 0) return writePagedText(a, sink, start, text, captured);
    // Small fields share compression and repeated analyzer/dictionary metadata
    // with the enclosing indexed doc-value block. No separate per-row reads.
    var memory = @import("../segment.zig").MemorySegmentSink.init(a);
    defer memory.deinit();
    var output = memory.sink();
    try output.appendNTimes(0, 8);
    const descriptor = try writePagedText(a, &output, 0, text, captured);
    defer a.free(descriptor);
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(a);
    try out.appendSlice(a, inline_magic);
    try u32add(a, &out, memory.out.items.len);
    try out.appendSlice(a, memory.out.items);
    try out.appendSlice(a, descriptor);
    return out.toOwnedSlice(a);
}
fn writePagedText(a: A, sink: *Sink, start: usize, text: []const u8, captured: Variants) ![]u8 {
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
        var bitmap: [page_size / 8]u8 = @splat(0);
        var present = false;
        for (text[begin..end], 0..) |byte, i| if (byte & 0xc0 == 0x80) {
            bitmap[i / 8] |= @as(u8, 1) << @as(u3, @intCast(i % 8));
            present = true;
        };
        if (present) {
            const raw = bitmap[0 .. (end - begin + 7) / 8];
            const wire = try snappy.encode(a, raw);
            defer a.free(wire);
            try u64add(a, &out, sink.len() - start);
            try u32add(a, &out, wire.len);
            try u32add(a, &out, raw.len);
            try sink.appendSlice(wire);
        } else {
            try u64add(a, &out, 0);
            try u32add(a, &out, 0);
            try u32add(a, &out, 0);
        }
        begin = end;
    }
    try u32add(a, &out, captured.items.len);
    for (captured.items) |variant| {
        try out.appendSlice(a, &variant.signature);
        try u64add(a, &out, sink.len() - start);
        try u32add(a, &out, variant.bytes);
        try u32add(a, &out, variant.root_raw);
        try u32add(a, &out, variant.count);
        try copyRange(sink, captured.view, variant.offset, variant.bytes);
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
    variants: []const u8,
    variant_count: u32,
    fn descriptor(view: View, bytes: []const u8) !?Text {
        if (std.mem.startsWith(u8, bytes, inline_magic)) {
            if (bytes.len < 9) return error.InvalidData;
            const length = std.mem.readInt(u32, bytes[5..9], .little);
            if (length > bytes.len - 9) return error.InvalidData;
            const payload = bytes[9..][0..length];
            return try parse(try View.init(.{ .contiguous = payload }, 0, payload.len), bytes[9 + length ..]);
        }
        if (!std.mem.startsWith(u8, bytes, text_magic)) return null;
        return try parse(view, bytes);
    }
    fn parse(view: View, bytes: []const u8) !Text {
        var p: Parser = .{ .bytes = bytes };
        if (!std.mem.eql(u8, try p.take(5), text_magic)) return error.InvalidData;
        const length = try p.int(u32);
        const count = try p.int(u32);
        if (count != (@as(u64, length) + page_size - 1) / page_size) return error.InvalidData;
        const pages = try p.take(try std.math.mul(usize, count, 32));
        var previous: u64 = 8;
        for (0..count) |i| {
            const part = pages[i * 32 ..][0..32];
            const offset = std.mem.readInt(u64, part[0..8], .little);
            const wire = std.mem.readInt(u32, part[8..12], .little);
            const raw = std.mem.readInt(u32, part[12..16], .little);
            if (offset < previous or offset > view.length or wire == 0 or wire > view.length - offset or raw != @min(page_size, @as(u64, length) - i * page_size)) return error.InvalidData;
            previous = offset + wire;
            const boundary = std.mem.readInt(u64, part[16..24], .little);
            const boundary_wire = std.mem.readInt(u32, part[24..28], .little);
            const boundary_raw = std.mem.readInt(u32, part[28..32], .little);
            if (boundary_wire == 0) {
                if (boundary != 0 or boundary_raw != 0) return error.InvalidData;
            } else {
                if (boundary < previous or boundary > view.length or boundary_wire > view.length - boundary or boundary_raw != (raw + 7) / 8) return error.InvalidData;
                previous = boundary + boundary_wire;
            }
        }
        const variants = try p.int(u32);
        const entries = try p.take(try std.math.mul(usize, variants, 52));
        if (p.at != bytes.len) return error.InvalidData;
        for (0..variants) |i| {
            const entry = entries[i * 52 ..][0..52];
            const offset = std.mem.readInt(u64, entry[32..40], .little);
            const wire = std.mem.readInt(u32, entry[40..44], .little);
            const raw = std.mem.readInt(u32, entry[44..48], .little);
            if (offset < previous or offset > view.length or wire == 0 or wire > view.length - offset or raw > @as(u64, length) * 64 + 64 or std.mem.readInt(u32, entry[48..52], .little) > length) return error.InvalidData;
            previous = offset + wire;
        }
        return .{ .view = view, .length = length, .pages = pages, .variant_count = variants, .variants = entries };
    }
    /// ASCII pages cannot move window endpoints. Otherwise UTF-8 can extend
    /// a snapped window by at most three bytes at either end.
    pub fn boundaryExpansion(self: Text) u32 {
        for (0..self.pages.len / 32) |i| if (std.mem.readInt(u32, self.pages[i * 32 + 24 ..][0..4], .little) != 0) return 6;
        return 0;
    }
    pub const Vector = offsets.Vector;
    pub const Selection = offsets.Selection;
    /// All-token reads remain useful to codec/compaction oracles. Serving passes
    /// the compiled matcher selection and fetches only matching occurrence blocks.
    pub fn tokens(self: Text, a: A, signature: [32]u8, control: Token) !?Vector {
        return self.selectTokens(a, signature, .{}, control);
    }
    pub fn selectTokens(self: Text, a: A, signature: [32]u8, selection: Selection, control: Token) !?Vector {
        const plan = (try self.planTokens(a, signature, selection, control)) orelse return null;
        return try plan.materializeOwned(control);
    }
    pub fn planTokens(self: Text, a: A, signature: [32]u8, selection: Selection, control: Token) !?offsets.Plan {
        var found: ?[]const u8 = null;
        for (0..self.variant_count) |i| {
            try control.check();
            const entry = self.variants[i * 52 ..][0..52];
            if (!std.mem.eql(u8, entry[0..32], &signature)) continue;
            if (found != null) return error.InvalidData;
            found = entry;
        }
        const entry = found orelse return null;
        const start = std.mem.readInt(u64, entry[32..40], .little);
        const size = std.mem.readInt(u32, entry[40..44], .little);
        return try offsets.prepare(a, try View.init(self.view.source, self.view.offset + start, size), self.length, std.mem.readInt(u32, entry[44..48], .little), std.mem.readInt(u32, entry[48..52], .little), selection, control);
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
                const future = self.pages[hinted * 32 ..][0..16];
                self.view.source.prefetch(self.view.offset + std.mem.readInt(u64, future[0..8], .little), std.mem.readInt(u32, future[8..12], .little));
            }
            const entry = self.pages[index * 32 ..][0..16];
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
/// Two page-local bitmaps suffice for monotone window starts/ends. A tiny
/// snippet never admits a bitmap proportional to the complete source value.
pub const Boundaries = struct {
    const Slot = struct { page: u32, bytes: []u8, used: u64 };
    a: A,
    text: Text,
    control: Token,
    slots: [2]?Slot = @splat(null),
    tick: u64 = 0,
    reads: usize = 0,
    pub fn init(a: A, text: Text, control: Token) Boundaries {
        return .{ .a = @import("../sql/memory_budget.zig").reclaimingAllocator(a), .text = text, .control = control };
    }
    pub fn deinit(self: *Boundaries) void {
        for (self.slots) |slot| if (slot) |value| self.a.free(value.bytes);
    }
    fn continuation(self: *Boundaries, offset: u32) !bool {
        if (offset >= self.text.length) return false;
        const page = offset / page_size;
        const entry = self.text.pages[@as(usize, page) * 32 ..][0..32];
        const wire_len = std.mem.readInt(u32, entry[24..28], .little);
        if (wire_len == 0) return false;
        self.tick +|= 1;
        var oldest: usize = 0;
        const raw = for (&self.slots, 0..) |*slot, i| {
            if (slot.*) |*value| {
                if (value.page == page) {
                    value.used = self.tick;
                    break value.bytes;
                }
                if (self.slots[oldest] != null and value.used < self.slots[oldest].?.used) oldest = i;
            } else oldest = i;
        } else load: {
            try self.control.check();
            const encoded = try self.a.alloc(u8, wire_len);
            defer self.a.free(encoded);
            try self.text.view.readInto(std.mem.readInt(u64, entry[16..24], .little), encoded);
            const bytes = try self.a.alloc(u8, std.mem.readInt(u32, entry[28..32], .little));
            errdefer self.a.free(bytes);
            try snappy.decodeInto(encoded, bytes);
            if (page == 0 and bytes[0] & 1 != 0) return error.InvalidData;
            const length = std.mem.readInt(u32, entry[12..16], .little);
            if (length % 8 != 0 and bytes[bytes.len - 1] >> @as(u3, @intCast(length % 8)) != 0) return error.InvalidData;
            if (self.slots[oldest]) |value| self.a.free(value.bytes);
            self.slots[oldest] = .{ .page = page, .bytes = bytes, .used = self.tick };
            self.reads += 1;
            break :load bytes;
        };
        const at = offset % page_size;
        return raw[at / 8] & (@as(u8, 1) << @as(u3, @intCast(at % 8))) != 0;
    }
    pub fn floor(self: *Boundaries, offset: u32) !u32 {
        if (offset > self.text.length) return error.InvalidData;
        var at = offset;
        var n: u8 = 0;
        while (at > 0 and try self.continuation(at) and n < 3) : (n += 1) at -= 1;
        return at;
    }
    pub fn ceil(self: *Boundaries, offset: u32) !u32 {
        if (offset > self.text.length) return error.InvalidData;
        var at = offset;
        var n: u8 = 0;
        while (at < self.text.length and try self.continuation(at) and n < 3) : (n += 1) at += 1;
        return at;
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
        return try Text.descriptor(self.source orelse return error.InvalidData, bytes);
    }
    pub fn get(self: *Cursor, doc: u32) !?[]const u8 {
        self.directory.reader.alloc.free(self.full);
        self.full = &.{};
        const bytes = (try self.directory.get(doc)) orelse return null;
        self.chunk = self.directory.chunk;
        self.decode_count = self.directory.decode_count;
        const value = (try Text.descriptor(self.source orelse return error.InvalidData, bytes)) orelse return bytes;
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
        const bytes = if (value == .string and (value.string.len > 4096 or (value.string.len >= offset_min_bytes and analyzers.len != 0)) and std.unicode.utf8ValidateSlice(value.string))
            try writeText(self.a, self.sink, self.start, value.string, analyzers)
        else
            try typed.encodeBinary(self.a, value);
        try self.append(doc, bytes);
    }
    pub fn addPrepared(self: *Writer, doc: u32, value: std.json.Value, captured: Variants) !void {
        const bytes = if (value == .string and (value.string.len > 4096 or captured.items.len != 0) and std.unicode.utf8ValidateSlice(value.string))
            try writeTextPrepared(self.a, self.sink, self.start, value.string, captured)
        else
            try typed.encodeBinary(self.a, value);
        try self.append(doc, bytes);
    }
    pub fn copy(self: *Writer, doc: u32, cursor: *Cursor, original: u32) !void {
        if (try cursor.directory.get(original)) |descriptor| if (std.mem.startsWith(u8, descriptor, inline_magic)) {
            _ = try Text.descriptor(cursor.source orelse return error.InvalidData, descriptor);
            return self.append(doc, try self.a.dupe(u8, descriptor));
        };
        if (try cursor.text(original)) |text| {
            if (self.records.items.len != 0 and self.records.items[self.records.items.len - 1].doc >= doc) return error.InvalidData;
            const descriptor = (try cursor.directory.get(original)).?;
            const bytes = try self.a.dupe(u8, descriptor);
            errdefer self.a.free(bytes);
            for (0..text.pages.len / 32) |i| {
                const page = text.pages[i * 32 ..][0..32];
                for ([_]usize{ 0, 16 }) |part| {
                    const offset = std.mem.readInt(u64, page[part..][0..8], .little);
                    const wire = std.mem.readInt(u32, page[part + 8 ..][0..4], .little);
                    if (wire == 0) continue;
                    std.mem.writeInt(u64, bytes[13 + i * 32 + part ..][0..8], self.sink.len() - self.start, .little);
                    try copyRange(self.sink, text.view, offset, wire);
                }
            }
            const variants_at = descriptor.len - text.variants.len;
            for (0..text.variant_count) |i| {
                const entry = text.variants[i * 52 ..][0..52];
                const wire = std.mem.readInt(u32, entry[40..44], .little);
                std.mem.writeInt(u64, bytes[variants_at + i * 52 + 32 ..][0..8], self.sink.len() - self.start, .little);
                try copyRange(self.sink, text.view, std.mem.readInt(u64, entry[32..40], .little), wire);
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
    const alias = analysis.simple_analyzer;
    try writer.add(7, .{ .string = text }, &.{ &analysis.simple_analyzer, &alias });
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
                try std.testing.expectEqual(@as(u32, 1), value.variant_count);
                const raw = try value.read(alloc, 0, value.length, .none);
                defer alloc.free(raw);
                try std.testing.expectEqualStrings(expected, raw);
                const tokens = (try value.tokens(alloc, try fingerprint(alloc, &analysis.simple_analyzer), .none)).?;
                defer tokens.deinit();
                try std.testing.expectEqual(@as(usize, 2), tokens.items.len);
                const needle = for (tokens.items) |token| {
                    if (std.mem.eql(u8, token.term, "needle")) break token;
                } else return error.MissingNeedle;
                try std.testing.expectEqual(@as(u32, 4093), needle.start_byte);
            }
        }
    };
    try Run.run(a, source, text);
    var stable = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(stable.allocator(), Run.run, .{ source, text });
}

test "highlight paged Unicode boundaries keep large metadata outside tiny snippet admission" {
    const a = std.testing.allocator;
    var body: std.ArrayList(u8) = .empty;
    defer body.deinit(a);
    const unit = "αβ.🙂\n";
    try body.ensureTotalCapacity(a, 8 * 1024 * 1024);
    while (body.items.len + unit.len <= 8 * 1024 * 1024) try body.appendSlice(a, unit);
    var memory = @import("../segment.zig").MemorySegmentSink.init(a);
    defer memory.deinit();
    var sink = memory.sink();
    var writer = try Writer.init(a, &sink);
    defer writer.deinit();
    try writer.add(0, .{ .string = body.items }, &.{});
    try std.testing.expect(try writer.finish());
    var budget: @import("../sql/memory_budget.zig") = .{ .backing = a, .limit = 96 * 1024 };
    {
        var cursor = Cursor.init(try Reader.init(budget.allocator(), try View.init(.{ .contiguous = memory.out.items }, 0, memory.out.items.len)));
        defer cursor.deinit();
        const stored = (try cursor.text(0)).?;
        try std.testing.expect(cursor.directory.bytes.len < 8192);
        var boundaries = Boundaries.init(budget.allocator(), stored, .none);
        defer boundaries.deinit();
        for ([_]u32{ 1, page_size - 1, page_size, page_size + 1, 5 * page_size - 1, @intCast(body.items.len - 17) }) |at| {
            var lower = at;
            while (lower > 0 and body.items[lower] & 0xc0 == 0x80) lower -= 1;
            var upper = at;
            while (upper < body.items.len and body.items[upper] & 0xc0 == 0x80) upper += 1;
            try std.testing.expectEqual(lower, try boundaries.floor(at));
            try std.testing.expectEqual(upper, try boundaries.ceil(at));
        }
        const first = try boundaries.floor(page_size - 1);
        const end = try boundaries.ceil(page_size + 17);
        const snippet = try stored.read(budget.allocator(), first, end, .none);
        defer budget.allocator().free(snippet);
        try std.testing.expectEqualStrings(body.items[first..end], snippet);
        try std.testing.expect(std.unicode.utf8ValidateSlice(snippet));
        std.debug.print("SOURCE_BOUNDARIES source_bytes={d} descriptor_bytes={d} bitmap_reads={d} peak_bytes={d}\n", .{ body.items.len, cursor.directory.bytes.len, boundaries.reads, budget.peak });
    }
    try std.testing.expectEqual(@as(usize, 0), budget.live);
}

test "highlight inline small field offsets share indexed blocks and survive compaction under faults" {
    const a = std.testing.allocator;
    const phrase = "<p>running needle é</p> ";
    const body = try a.alloc(u8, phrase.len * 48);
    defer a.free(body);
    for (0..48) |i| @memcpy(body[i * phrase.len ..][0..phrase.len], phrase);
    const Run = struct {
        fn run(alloc: A, text: []const u8) !void {
            var original = @import("../segment.zig").MemorySegmentSink.init(alloc);
            defer original.deinit();
            var copied = @import("../segment.zig").MemorySegmentSink.init(alloc);
            defer copied.deinit();
            var output = copied.sink();
            var target = try Writer.init(alloc, &output);
            defer target.deinit();
            {
                var input = original.sink();
                var writer = try Writer.init(alloc, &input);
                defer writer.deinit();
                for (0..4) |i| try writer.add(@intCast(i), .{ .string = text }, &.{ &analysis.html_analyzer, &analysis.simple_analyzer });
                try std.testing.expect(try writer.finish());
                var cursor = Cursor.init(try Reader.init(alloc, try View.init(.{ .contiguous = original.out.items }, 0, original.out.items.len)));
                defer cursor.deinit();
                for (0..4) |i| {
                    const descriptor = (try cursor.directory.get(@intCast(i))).?;
                    try std.testing.expect(std.mem.startsWith(u8, descriptor, inline_magic));
                    const source = (try cursor.text(@intCast(i))).?;
                    try std.testing.expect(source.view.source == .contiguous);
                    const raw = try source.read(alloc, 0, source.length, .none);
                    defer alloc.free(raw);
                    try std.testing.expectEqualStrings(text, raw);
                    for ([_]*const analysis.Analyzer{ &analysis.html_analyzer, &analysis.simple_analyzer }) |analyzer| {
                        const vector = (try source.tokens(alloc, try fingerprint(alloc, analyzer), .none)).?;
                        defer vector.deinit();
                        try std.testing.expect(vector.items.len != 0);
                    }
                    try target.copy(@intCast(i), &cursor, @intCast(i));
                }
            }
            try std.testing.expect(try target.finish());
            // Originals and decoded directory chunks may be gone before reuse.
            original.deinit();
            original = @import("../segment.zig").MemorySegmentSink.init(alloc);
            var cursor = Cursor.init(try Reader.init(alloc, try View.init(.{ .contiguous = copied.out.items }, 0, copied.out.items.len)));
            defer cursor.deinit();
            for (0..4) |i| {
                const source = (try cursor.text(@intCast(i))).?;
                const vector = (try source.tokens(alloc, try fingerprint(alloc, &analysis.html_analyzer), .none)).?;
                defer vector.deinit();
                try std.testing.expectEqual(@as(usize, 144), vector.items.len);
            }
        }
    };
    try Run.run(a, body);
    var stable = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(stable.allocator(), Run.run, .{body});
}
