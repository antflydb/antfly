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

//! Build-owned original-offset capture. The first compatible indexing binding
//! emits its analyzed tokens into a private seekable run. Source-column writing
//! copies those compressed bytes without analyzing or retaining token arrays.
const std = @import("std");
const A = std.mem.Allocator;
const column = @import("source_column.zig");
const offsets = @import("source_offsets.zig");
const analysis = @import("../search/analysis.zig");
const segment = @import("../segment.zig");
const Run = @import("../postings_run.zig").Run;
pub const Capture = struct {
    const Key = struct { doc: u32, field: []const u8 };
    const Keys = struct {
        pub fn hash(_: @This(), key: Key) u64 {
            return std.hash.Wyhash.hash(key.doc, key.field);
        }
        pub fn eql(_: @This(), l: Key, r: Key) bool {
            return l.doc == r.doc and std.mem.eql(u8, l.field, r.field);
        }
    };
    pub const Request = struct { key: Key, signature: [32]u8, text_bytes: usize };
    a: A,
    active: bool,
    io: ?std.Io,
    directory: []const u8,
    resources: ?*@import("../storage/resource_manager.zig").ResourceManager,
    spool: ?*Run = null,
    memory: segment.MemorySegmentSink,
    entries: std.HashMapUnmanaged(Key, std.ArrayList(column.Variant), Keys, 80) = .empty,
    signatures: std.AutoHashMapUnmanaged(*const analysis.Analyzer, [32]u8) = .empty,
    captured: usize = 0,
    reused: usize = 0,
    pub fn init(a: A, active: bool, io: ?std.Io, directory: []const u8, resources: ?*@import("../storage/resource_manager.zig").ResourceManager) Capture {
        return .{ .a = a, .active = active, .io = io, .directory = directory, .resources = resources, .memory = .init(a) };
    }
    pub fn deinit(self: *Capture) void {
        var entries = self.entries.valueIterator();
        while (entries.next()) |entry| entry.deinit(self.a);
        self.entries.deinit(self.a);
        self.signatures.deinit(self.a);
        if (self.spool) |run| run.deinit();
        self.memory.deinit();
    }
    pub fn signatureFor(self: *Capture, analyzer: *const analysis.Analyzer) ![32]u8 {
        return self.signatures.get(analyzer) orelse add: {
            const bytes = try column.fingerprint(self.a, analyzer);
            try self.signatures.put(self.a, analyzer, bytes);
            break :add bytes;
        };
    }
    pub fn wants(self: *Capture, doc: u32, field: []const u8, text: []const u8, root: ?std.json.Value, analyzer: *const analysis.Analyzer) !?Request {
        if (!self.active or text.len < column.offset_min_bytes or !column.eligibleAnalyzer(analyzer)) return null;
        const value = root orelse return null;
        if (value != .object) return null;
        const original = value.object.get(field) orelse return null;
        if (original != .string or !std.mem.eql(u8, original.string, text) or !std.unicode.utf8ValidateSlice(text)) return null;
        const signature = try self.signatureFor(analyzer);
        const key: Key = .{ .doc = doc, .field = field };
        if (self.entries.get(key)) |held| for (held.items) |variant| if (std.mem.eql(u8, &variant.signature, &signature)) {
            self.reused += 1;
            return null;
        };
        return .{ .key = key, .signature = signature, .text_bytes = text.len };
    }
    pub fn record(self: *Capture, request: Request, tokens: []const analysis.Token) !void {
        var scratch = std.heap.ArenaAllocator.init(@import("../sql/memory_budget.zig").reclaimingAllocator(self.a));
        defer scratch.deinit();
        const sa = scratch.allocator();
        var staged = segment.MemorySegmentSink.init(sa);
        defer staged.deinit();
        var small: ?offsets.Encoded = null;
        if (request.text_bytes <= 4096) {
            var used: usize = 0;
            if (self.entries.get(request.key)) |held| for (held.items) |variant| {
                used +|= variant.bytes +| 52;
            };
            const allowance = request.text_bytes * 2 -| used;
            if (allowance <= 52 or !try column.smallOffsetMetadataFits(sa, tokens, allowance - 52)) return;
            var output = staged.sink();
            const encoded = try offsets.write(sa, &output, tokens);
            if (encoded.bytes > allowance - 52) return;
            small = encoded;
        }
        if (self.spool == null) {
            if (self.io) |io| {
                self.spool = try Run.createWithResources(self.a, io, self.directory, self.resources);
                self.spool.?.preferSequentialReads();
            }
        }
        var sink = if (self.spool) |run| run.sink() else self.memory.sink();
        const begin = sink.len();
        const encoded = if (small) |value| value else try offsets.write(sa, &sink, tokens);
        if (small != null) try sink.appendSlice(staged.out.items);
        if (self.spool) |run| try run.seal(begin);
        const slot = try self.entries.getOrPut(self.a, request.key);
        if (!slot.found_existing) slot.value_ptr.* = .empty;
        try slot.value_ptr.append(self.a, .{ .signature = request.signature, .offset = begin, .bytes = encoded.bytes, .root_raw = encoded.root_raw, .count = encoded.count });
        self.captured += 1;
    }
    pub fn variants(self: *Capture, doc: u32, field: []const u8) !column.Variants {
        const view = if (self.spool) |run| try run.view() else try @import("../segment_source.zig").View.init(.{ .contiguous = self.memory.out.items }, 0, self.memory.out.items.len);
        return .{ .view = view, .items = if (self.entries.get(.{ .doc = doc, .field = field })) |entry| entry.items else &.{} };
    }
};

test "highlight indexing capture shares analyzer identity spills and outlives private runs under allocation faults" {
    if (comptime @import("builtin").os.tag == .freestanding) return;
    const a = std.testing.allocator;
    const value = try a.alloc(u8, 8192);
    defer a.free(value);
    @memset(value, '.');
    @memcpy(value[4090..][0.."<p>needle needle</p>".len], "<p>needle needle</p>");
    const RunTest = struct {
        fn run(alloc: A, text: []const u8, spill: bool) !void {
            var root: std.json.ObjectMap = .empty;
            defer root.deinit(alloc);
            try root.put(alloc, "body", .{ .string = text });
            var memory = segment.MemorySegmentSink.init(alloc);
            defer memory.deinit();
            var sink = memory.sink();
            var writer = try column.Writer.init(alloc, &sink);
            defer writer.deinit();
            {
                var capture = Capture.init(alloc, true, if (spill) std.testing.io else null, ".", null);
                defer capture.deinit();
                const analyzer = &analysis.html_analyzer;
                const request = (try capture.wants(9, "body", text, .{ .object = root }, analyzer)).?;
                var scratch = std.heap.ArenaAllocator.init(alloc);
                defer scratch.deinit();
                const tokens = try analyzer.analyzeWithSourceOffsets(scratch.allocator(), text);
                try capture.record(request, tokens);
                const alias = analyzer.*;
                try std.testing.expect((try capture.wants(9, "body", text, .{ .object = root }, &alias)) == null);
                try std.testing.expectEqual(@as(usize, 1), capture.captured);
                try std.testing.expectEqual(@as(usize, 1), capture.reused);
                try std.testing.expectEqual(spill, capture.spool != null);
                try writer.addPrepared(0, .{ .string = text }, try capture.variants(9, "body"));
            }
            try std.testing.expect(try writer.finish());
            const View = @import("../segment_source.zig").View;
            var cursor = column.Cursor.init(try column.Reader.init(alloc, try View.init(.{ .contiguous = memory.out.items }, 0, memory.out.items.len)));
            defer cursor.deinit();
            const source = (try cursor.text(0)).?;
            const tokens = (try source.tokens(alloc, try column.fingerprint(alloc, &analysis.html_analyzer), .none)).?;
            defer tokens.deinit();
            try std.testing.expectEqual(@as(usize, 2), tokens.items.len);
            try std.testing.expectEqualStrings("needle", text[tokens.items[0].start_byte..tokens.items[0].end_byte]);
        }
    };
    for ([_]bool{ false, true }) |spill| {
        try RunTest.run(a, value, spill);
        var stable = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
        try std.testing.checkAllAllocationFailures(stable.allocator(), RunTest.run, .{ value, spill });
    }
}

test "highlight small offset admission rejects diverse metadata before spilling and preserves useful variants" {
    const a = std.testing.allocator;
    var body: std.ArrayList(u8) = .empty;
    defer body.deinit(a);
    for (0..100) |i| {
        var buffer: [24]u8 = undefined;
        try body.appendSlice(a, try std.fmt.bufPrint(&buffer, "word{d:0>4} ", .{i}));
    }
    var root: std.json.ObjectMap = .empty;
    defer root.deinit(a);
    try root.put(a, "body", .{ .string = body.items });
    var capture = Capture.init(a, true, std.testing.io, ".", null);
    defer capture.deinit();
    const request = (try capture.wants(0, "body", body.items, .{ .object = root }, &analysis.simple_analyzer)).?;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const tokens = try analysis.simple_analyzer.analyzeWithSourceOffsets(arena.allocator(), body.items);
    try capture.record(request, tokens);
    try std.testing.expect(capture.spool == null);
    try std.testing.expectEqual(@as(usize, 0), (try capture.variants(0, "body")).items.len);
    body.clearRetainingCapacity();
    for (0..128) |_| try body.appendSlice(a, "running ");
    root.getPtr("body").?.* = .{ .string = body.items };
    const useful = (try capture.wants(1, "body", body.items, .{ .object = root }, &analysis.simple_analyzer)).?;
    const repeated = try analysis.simple_analyzer.analyzeWithSourceOffsets(arena.allocator(), body.items);
    try capture.record(useful, repeated);
    try std.testing.expectEqual(@as(usize, 1), (try capture.variants(1, "body")).items.len);
}
