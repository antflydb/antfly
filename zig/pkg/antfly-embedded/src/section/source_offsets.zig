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

//! Relocatable source-offset vectors. A small ceiling directory routes exact
//! and prefix seeks to independently compressed dictionary pages. Each term
//! owns bounded delta-coded occurrence blocks; patterns run once per distinct
//! term, and unrelated occurrence payloads never enter query memory.
const std = @import("std");
const A = std.mem.Allocator;
const analysis = @import("../search/analysis.zig");
const snappy = @import("../encoding/snappy.zig");
const View = @import("../segment_source.zig").View;
const Sink = @import("../segment.zig").SegmentSink;
const Control = @import("antfly_cancellation").CancellationToken;
const magic = "AFOV0001";
const header_size = 32;
const block_tokens = 1024;
const page_terms = 64;
const page_bytes = 64 * 1024;
pub const Ref = struct { offset: u64, wire: u32, raw: u32, count: u32 };
pub const Encoded = struct { bytes: usize, root_raw: u32, count: u32 };
fn integer(a: A, out: *std.ArrayList(u8), comptime T: type, v: T) !void {
    var b: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &b, v, .little);
    try out.appendSlice(a, &b);
}
fn text(a: A, out: *std.ArrayList(u8), value: []const u8) !void {
    try integer(a, out, u32, std.math.cast(u32, value.len) orelse return error.InvalidData);
    try out.appendSlice(a, value);
}
fn reference(a: A, out: *std.ArrayList(u8), ref: Ref) !void {
    try integer(a, out, u64, ref.offset);
    try integer(a, out, u32, ref.wire);
    try integer(a, out, u32, ref.raw);
    try integer(a, out, u32, ref.count);
}
fn variable(a: A, out: *std.ArrayList(u8), value: u64) !void {
    var n = value;
    while (n >= 128) : (n >>= 7) try out.append(a, @as(u8, @intCast(n & 127)) | 128);
    try out.append(a, @intCast(n));
}
const Parser = struct {
    bytes: []const u8,
    at: usize = 0,
    fn take(self: *@This(), n: usize) ![]const u8 {
        if (n > self.bytes.len - self.at) return error.InvalidData;
        defer self.at += n;
        return self.bytes[self.at..][0..n];
    }
    fn int(self: *@This(), comptime T: type) !T {
        return std.mem.readInt(T, (try self.take(@sizeOf(T)))[0..@sizeOf(T)], .little);
    }
    fn string(self: *@This()) ![]const u8 {
        return self.take(try self.int(u32));
    }
    fn ref(self: *@This()) !Ref {
        return .{ .offset = try self.int(u64), .wire = try self.int(u32), .raw = try self.int(u32), .count = try self.int(u32) };
    }
    fn variable(self: *@This()) !u64 {
        var n: u64 = 0;
        var shift: u6 = 0;
        while (true) {
            const b = (try self.take(1))[0];
            if (shift == 63 and b > 1) return error.InvalidData;
            n |= @as(u64, b & 127) << shift;
            if (b < 128) return n;
            if (shift >= 63) return error.InvalidData;
            shift += 7;
        }
    }
};
fn chunk(a: A, sink: *Sink, base: usize, raw: []const u8, count: u32) !Ref {
    const start = sink.len();
    var at: usize = 0;
    while (at < raw.len) {
        const end = @min(raw.len, at + page_bytes);
        const encoded = try snappy.encode(a, raw[at..end]);
        defer a.free(encoded);
        var size: [4]u8 = undefined;
        std.mem.writeInt(u32, &size, @intCast(encoded.len), .little);
        try sink.appendSlice(&size);
        try sink.appendSlice(encoded);
        at = end;
    }
    return .{ .offset = start - base, .wire = std.math.cast(u32, sink.len() - start) orelse return error.InvalidData, .raw = std.math.cast(u32, raw.len) orelse return error.InvalidData, .count = count };
}
fn dictionary(a: A, sink: *Sink, base: usize, pending: *std.ArrayList(u8), count: u32, ceiling: []const u8, root: *std.ArrayList(u8)) !void {
    const ref = try chunk(a, sink, base, pending.items, count);
    try text(a, root, ceiling);
    try reference(a, root, ref);
    pending.clearRetainingCapacity();
}
/// Input tokens are borrowed from the indexing analysis. Only an index link
/// per occurrence and one group per distinct term are added; term bytes are
/// not copied. Compressed output goes straight into the private build run.
pub fn write(a: A, sink: *Sink, tokens: []const analysis.Token) !Encoded {
    const count = std.math.cast(u32, tokens.len) orelse return error.InvalidData;
    const base = sink.len();
    try sink.appendNTimes(0, header_size);
    const Group = struct { term: []const u8, first: u32, last: u32, count: u32 = 1 };
    var groups: std.ArrayList(Group) = .empty;
    defer groups.deinit(a);
    var by_term: std.StringHashMapUnmanaged(u32) = .empty;
    defer by_term.deinit(a);
    const next = try a.alloc(u32, tokens.len);
    defer a.free(next);
    @memset(next, std.math.maxInt(u32));
    for (tokens, 0..) |token, i| {
        if (token.start_byte > token.end_byte) return error.InvalidData;
        const entry = try by_term.getOrPut(a, token.term);
        if (!entry.found_existing) {
            entry.value_ptr.* = @intCast(groups.items.len);
            try groups.append(a, .{ .term = token.term, .first = @intCast(i), .last = @intCast(i) });
        } else {
            const group = &groups.items[entry.value_ptr.*];
            next[group.last] = @intCast(i);
            group.last = @intCast(i);
            group.count += 1;
        }
    }
    by_term.deinit(a);
    by_term = .empty;
    std.mem.sort(Group, groups.items, {}, struct {
        fn less(_: void, l: Group, r: Group) bool {
            return std.mem.lessThan(u8, l.term, r.term);
        }
    }.less);
    var pending: std.ArrayList(u8) = .empty;
    defer pending.deinit(a);
    var root: std.ArrayList(u8) = .empty;
    defer root.deinit(a);
    try integer(a, &root, u32, 0);
    var pages: u32 = 0;
    var terms: u32 = 0;
    var previous: []const u8 = "";
    var raw: std.ArrayList(u8) = .empty;
    defer raw.deinit(a);
    var refs: std.ArrayList(Ref) = .empty;
    defer refs.deinit(a);
    for (groups.items) |group| {
        if (terms != 0 and (terms == page_terms or pending.items.len >= page_bytes)) {
            try dictionary(a, sink, base, &pending, terms, previous, &root);
            pages += 1;
            terms = 0;
        }
        refs.clearRetainingCapacity();
        var at = group.first;
        var remaining = group.count;
        while (remaining != 0) {
            const n = @min(remaining, block_tokens);
            raw.clearRetainingCapacity();
            var start: i64 = 0;
            for (0..n) |_| {
                const token = tokens[at];
                const delta = @as(i64, token.start_byte) - start;
                try variable(a, &raw, @bitCast((delta << 1) ^ (delta >> 63)));
                try variable(a, &raw, token.end_byte - token.start_byte);
                try variable(a, &raw, token.position);
                start = token.start_byte;
                at = next[at];
            }
            try refs.append(a, try chunk(a, sink, base, raw.items, n));
            remaining -= n;
        }
        try text(a, &pending, group.term);
        try integer(a, &pending, u32, @intCast(refs.items.len));
        for (refs.items) |ref| try reference(a, &pending, ref);
        previous = group.term;
        terms += 1;
    }
    if (terms != 0) {
        try dictionary(a, sink, base, &pending, terms, previous, &root);
        pages += 1;
    }
    std.mem.writeInt(u32, root.items[0..4], pages, .little);
    const root_ref = try chunk(a, sink, base, root.items, pages);
    var header: [header_size]u8 = undefined;
    @memcpy(header[0..8], magic);
    std.mem.writeInt(u64, header[8..16], root_ref.offset, .little);
    std.mem.writeInt(u32, header[16..20], root_ref.wire, .little);
    std.mem.writeInt(u32, header[20..24], root_ref.raw, .little);
    std.mem.writeInt(u32, header[24..28], count, .little);
    std.mem.writeInt(u32, header[28..32], @intCast(groups.items.len), .little);
    try sink.writeAt(base, &header);
    return .{ .bytes = sink.len() - base, .root_raw = root_ref.raw, .count = count };
}
pub const Selection = struct {
    ptr: ?*anyopaque = null,
    matches: ?*const fn (*anyopaque, []const u8) anyerror!bool = null,
    /// When every matcher is exact/prefix, navigation can skip disjoint pages.
    /// Otherwise all dictionary pages are examined, never all occurrences.
    ranges: ?[]const Range = null,
    pub const Range = struct { lower: []const u8, prefix: bool = false };
    fn page(self: Selection, previous: ?[]const u8, ceiling: []const u8) bool {
        const ranges = self.ranges orelse return true;
        for (ranges) |range| {
            if (std.mem.order(u8, range.lower, ceiling) == .gt) continue;
            const lower = previous orelse return true;
            if (std.mem.order(u8, range.lower, lower) == .gt) return true;
            if (range.prefix) {
                // A prefix's lexicographic successor is exclusive. Comparing
                // only the prefix-width head handles all-0xff and empty prefixes.
                const n = @min(lower.len, range.lower.len);
                if (std.mem.order(u8, lower[0..n], range.lower[0..n]) != .gt) return true;
            }
        }
        return false;
    }
};
/// Binary seeks produce page intervals; merge them before marking the union.
/// Large OR/prefix sets therefore cost Q log P + Q log Q + P, rather than P*Q.
fn pageBound(pages: anytype, key: []const u8, comptime successor: bool) usize {
    var low: usize = 0;
    var high = pages.len;
    while (low < high) {
        const middle = low + (high - low) / 2;
        const ceiling = pages[middle].ceiling;
        const n = @min(ceiling.len, key.len);
        const before = if (successor)
            std.mem.order(u8, ceiling[0..n], key[0..n]) != .gt
        else
            std.mem.order(u8, ceiling, key) == .lt;
        if (before) low = middle + 1 else high = middle;
    }
    return low;
}
fn selectedPages(a: A, pages: anytype, selection: Selection) !?[]bool {
    const ranges = selection.ranges orelse return null;
    const selected = try a.alloc(bool, pages.len);
    errdefer a.free(selected);
    @memset(selected, false);
    const Interval = struct { first: usize, end: usize };
    const intervals = try a.alloc(Interval, ranges.len);
    defer a.free(intervals);
    var count: usize = 0;
    for (ranges) |range| {
        const first = pageBound(pages, range.lower, false);
        if (first == pages.len) continue;
        const last = if (range.prefix) pageBound(pages, range.lower, true) else first;
        intervals[count] = .{ .first = first, .end = @min(pages.len, last +| 1) };
        count += 1;
    }
    std.mem.sort(Interval, intervals[0..count], {}, struct {
        fn less(_: void, l: Interval, r: Interval) bool {
            return l.first < r.first;
        }
    }.less);
    var through: usize = 0;
    for (intervals[0..count]) |interval| {
        const first = @max(through, interval.first);
        if (first < interval.end) @memset(selected[first..interval.end], true);
        through = @max(through, interval.end);
    }
    return selected;
}
pub const Vector = struct {
    a: A,
    owner: *std.heap.ArenaAllocator,
    items: []analysis.Token,
    pub fn deinit(self: Vector) void {
        self.owner.deinit();
        self.a.destroy(self.owner);
    }
};
fn read(a: A, view: View, ref: Ref, bound: u64, control: Control) ![]u8 {
    try control.check();
    if (ref.offset < header_size or ref.offset > bound or ref.wire == 0 or ref.wire > bound - ref.offset) return error.InvalidData;
    const bytes = try a.alloc(u8, ref.wire);
    defer a.free(bytes);
    try view.readInto(ref.offset, bytes);
    const raw = try a.alloc(u8, ref.raw);
    errdefer a.free(raw);
    var frames: Parser = .{ .bytes = bytes };
    var at: usize = 0;
    while (at < raw.len) {
        try control.check();
        const end = @min(raw.len, at + page_bytes);
        try snappy.decodeInto(try frames.take(try frames.int(u32)), raw[at..end]);
        at = end;
    }
    if (frames.at != bytes.len) return error.InvalidData;
    try control.check();
    return raw;
}
pub fn select(a: A, view: View, source_length: u32, root_raw: u32, count: u32, selection: Selection, control: Control) !Vector {
    const work = @import("../sql/memory_budget.zig").reclaimingAllocator(a);
    var h: [header_size]u8 = undefined;
    try control.check();
    if (view.length < h.len) return error.InvalidData;
    try view.readInto(0, &h);
    if (!std.mem.eql(u8, h[0..8], magic) or std.mem.readInt(u32, h[20..24], .little) != root_raw or std.mem.readInt(u32, h[24..28], .little) != count or root_raw > @as(u64, source_length) * 64 + 64) return error.InvalidData;
    const root_ref: Ref = .{ .offset = std.mem.readInt(u64, h[8..16], .little), .wire = std.mem.readInt(u32, h[16..20], .little), .raw = root_raw, .count = 0 };
    if (root_ref.offset > view.length or root_ref.wire != view.length - root_ref.offset) return error.InvalidData;
    const root_bytes = try read(work, view, root_ref, view.length, control);
    defer work.free(root_bytes);
    var root: Parser = .{ .bytes = root_bytes };
    const n = try root.int(u32);
    if (n > (root_bytes.len - root.at) / 24) return error.InvalidData;
    const Page = struct { ceiling: []const u8, ref: Ref };
    const pages = try work.alloc(Page, n);
    defer work.free(pages);
    var previous: ?[]const u8 = null;
    var term_count: u64 = 0;
    var prior_end: u64 = header_size;
    for (pages) |*page| {
        try control.check();
        page.* = .{ .ceiling = try root.string(), .ref = try root.ref() };
        const ref = page.ref;
        if ((previous != null and std.mem.order(u8, previous.?, page.ceiling) != .lt) or ref.offset < prior_end or ref.offset > root_ref.offset or ref.wire == 0 or ref.wire > root_ref.offset - ref.offset or ref.count == 0 or ref.count > page_terms or ref.raw > @as(u64, source_length) * 64 + 64) return error.InvalidData;
        previous = page.ceiling;
        prior_end = ref.offset + ref.wire;
        term_count += ref.count;
    }
    if (root.at != root.bytes.len or term_count != std.mem.readInt(u32, h[28..32], .little) or term_count > count or (term_count == 0) != (count == 0)) return error.InvalidData;
    const needed = try selectedPages(work, pages, selection);
    defer if (needed) |bits| work.free(bits);
    try control.check();
    const owner = try a.create(std.heap.ArenaAllocator);
    owner.* = .init(a);
    errdefer {
        owner.deinit();
        a.destroy(owner);
    }
    const out = owner.allocator();
    var items: std.ArrayList(analysis.Token) = .empty;
    previous = null;
    var total: u64 = 0;
    for (pages, 0..) |page, page_index| {
        const lower = previous;
        previous = page.ceiling;
        if (needed) |bits| if (!bits[page_index]) continue;
        const dictionary_bytes = try read(work, view, page.ref, root_ref.offset, control);
        defer work.free(dictionary_bytes);
        var p: Parser = .{ .bytes = dictionary_bytes };
        var prior = lower;
        for (0..page.ref.count) |_| {
            try control.check();
            const term = try p.string();
            if ((prior != null and std.mem.order(u8, prior.?, term) != .lt) or std.mem.order(u8, term, page.ceiling) == .gt) return error.InvalidData;
            prior = term;
            const blocks = try p.int(u32);
            if (blocks == 0 or blocks > (p.bytes.len - p.at) / 20) return error.InvalidData;
            const matched = if (selection.matches) |check| try check(selection.ptr.?, term) else true;
            const retained = if (matched) try out.dupe(u8, term) else "";
            var block_end: u64 = header_size;
            for (0..blocks) |_| {
                const ref = try p.ref();
                if (ref.offset < block_end or ref.offset > page.ref.offset or ref.wire == 0 or ref.wire > page.ref.offset - ref.offset or ref.count == 0 or ref.count > block_tokens or ref.raw == 0 or ref.raw > page_bytes) return error.InvalidData;
                block_end = ref.offset + ref.wire;
                if (!matched) continue;
                total += ref.count;
                if (total > count) return error.InvalidData;
                const bytes = try read(work, view, ref, page.ref.offset, control);
                defer work.free(bytes);
                var block: Parser = .{ .bytes = bytes };
                var start: i64 = 0;
                for (0..ref.count) |i| {
                    if (i % 256 == 0) try control.check();
                    const delta = try block.variable();
                    if (delta > @as(u64, std.math.maxInt(u32)) * 2) return error.InvalidData;
                    start += @as(i64, @intCast(delta >> 1)) ^ -@as(i64, @intCast(delta & 1));
                    const length = try block.variable();
                    const position = try block.variable();
                    if (start < 0 or start > source_length or length > source_length - @as(u64, @intCast(start)) or position > std.math.maxInt(u32)) return error.InvalidData;
                    try items.append(out, .{ .term = retained, .start_byte = @intCast(start), .end_byte = @intCast(@as(u64, @intCast(start)) + length), .position = @intCast(position) });
                }
                if (block.at != bytes.len) return error.InvalidData;
            }
        }
        if (p.at != p.bytes.len or prior == null or !std.mem.eql(u8, prior.?, page.ceiling)) return error.InvalidData;
    }
    if (selection.matches == null and selection.ranges == null and total != count) return error.InvalidData;
    return .{ .a = a, .owner = owner, .items = try items.toOwnedSlice(out) };
}

test "highlight source offset dictionary skips unrelated occurrence blocks and bounds allocation failures" {
    const a = std.testing.allocator;
    const segment = @import("../segment.zig");
    var inputs = std.heap.ArenaAllocator.init(a);
    defer inputs.deinit();
    const ia = inputs.allocator();
    const vocabulary = 256;
    const repeats = 32;
    const tokens = try ia.alloc(analysis.Token, vocabulary * repeats);
    for (0..vocabulary) |term| {
        const name = try std.fmt.allocPrint(ia, "term-{d:0>6}", .{term});
        for (0..repeats) |repeat| {
            const i = repeat * vocabulary + term;
            tokens[i] = .{ .term = name, .start_byte = @intCast(i * 16), .end_byte = @intCast(i * 16 + name.len), .position = @intCast(i) };
        }
    }
    var memory = segment.MemorySegmentSink.init(a);
    defer memory.deinit();
    var sink = memory.sink();
    const encoded = try write(a, &sink, tokens);
    const Range = struct {
        bytes: []const u8,
        read_bytes: usize = 0,
        calls: usize = 0,
        fn close(_: *anyopaque) void {}
        fn read(raw: *anyopaque, at: u64, out: []u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (at > self.bytes.len or out.len > self.bytes.len - at) return error.EndOfStream;
            @memcpy(out, self.bytes[@intCast(at)..][0..out.len]);
            self.read_bytes += out.len;
            self.calls += 1;
        }
    };
    const Probe = struct {
        calls: usize = 0,
        fn matches(raw: *anyopaque, term: []const u8) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return std.mem.eql(u8, term, "term-000128");
        }
    };
    var reads: Range = .{ .bytes = memory.out.items };
    const view = try View.init(.{ .ranges = .{ .ptr = &reads, .length = memory.out.items.len, .read_into = Range.read, .close = Range.close } }, 0, memory.out.items.len);
    var probe: Probe = .{};
    const selected = try select(a, view, @intCast(tokens.len * 16), encoded.root_raw, encoded.count, .{ .ptr = &probe, .matches = Probe.matches, .ranges = &.{.{ .lower = "term-000128" }} }, .none);
    defer selected.deinit();
    try std.testing.expectEqual(@as(usize, repeats), selected.items.len);
    try std.testing.expect(probe.calls <= page_terms);
    try std.testing.expect(reads.read_bytes * 4 < encoded.bytes);
    for (selected.items, 0..) |token, i| {
        try std.testing.expectEqualStrings("term-000128", token.term);
        try std.testing.expectEqual(@as(u32, @intCast((i * vocabulary + 128) * 16)), token.start_byte);
        try std.testing.expectEqual(@as(u32, @intCast(i * vocabulary + 128)), token.position);
    }
    std.debug.print("SOURCE_OFFSET_SELECT tokens={d} term_tests={d} selected={d} blob_bytes={d} read_bytes={d}\n", .{ tokens.len, probe.calls, selected.items.len, encoded.bytes, reads.read_bytes });
    const RunTest = struct {
        fn run(alloc: A, source: View, metadata: Encoded, source_length: u32) !void {
            var check: Probe = .{};
            const value = try select(alloc, source, source_length, metadata.root_raw, metadata.count, .{ .ptr = &check, .matches = Probe.matches, .ranges = &.{.{ .lower = "term-000128" }} }, .none);
            defer value.deinit();
            try std.testing.expectEqual(@as(usize, repeats), value.items.len);
        }
    };
    const contiguous = try View.init(.{ .contiguous = memory.out.items }, 0, memory.out.items.len);
    var stable = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(stable.allocator(), RunTest.run, .{ contiguous, encoded, @as(u32, @intCast(tokens.len * 16)) });
    // Patterns evaluate the complete vocabulary once, independently of the
    // number of occurrences. Only the selected term's blocks are fetched.
    reads.read_bytes = 0;
    probe.calls = 0;
    const patterned = try select(a, view, @intCast(tokens.len * 16), encoded.root_raw, encoded.count, .{ .ptr = &probe, .matches = Probe.matches }, .none);
    defer patterned.deinit();
    try std.testing.expectEqual(@as(usize, vocabulary), probe.calls);
    try std.testing.expectEqual(@as(usize, repeats), patterned.items.len);
    // Every cancel point unwinds output accumulated before later block reads.
    const Stop = struct {
        calls: usize = 0,
        limit: usize,
        fn check(raw: *const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.calls += 1;
            if (self.calls == self.limit) return error.DeadlineExceeded;
        }
    };
    for ([_]usize{ 1, 4, 16, 64, 200 }) |limit| {
        var stop: Stop = .{ .limit = limit };
        try std.testing.expectError(error.DeadlineExceeded, select(a, contiguous, @intCast(tokens.len * 16), encoded.root_raw, encoded.count, .{}, .{ .ptr = &stop, .check_fn = Stop.check }));
    }
    // Corruption at the relocatable root is rejected before allocating output.
    const saved = memory.out.items[8..16].*;
    @memset(memory.out.items[8..16], 0xff);
    try std.testing.expectError(error.InvalidData, select(a, contiguous, @intCast(tokens.len * 16), encoded.root_raw, encoded.count, .{}, .none));
    @memcpy(memory.out.items[8..16], &saved);
}

test "highlight source offset prefix ranges preserve exact boundary semantics" {
    const ranges = [_]Selection.Range{.{ .lower = "abc", .prefix = true }};
    const selection: Selection = .{ .ranges = &ranges };
    try std.testing.expect(!selection.page(null, "abb"));
    try std.testing.expect(selection.page("abb", "abc0"));
    try std.testing.expect(selection.page("abc0", "abc9"));
    try std.testing.expect(selection.page("abc9", "abd"));
    try std.testing.expect(!selection.page("abd", "z"));
    try std.testing.expect((Selection{ .ranges = &.{.{ .lower = "", .prefix = true }} }).page("z", "zz"));
    try std.testing.expect((Selection{ .ranges = &.{.{ .lower = "\xff", .prefix = true }} }).page("\xffx", "\xffz"));
    try std.testing.expect(!(Selection{ .ranges = &.{.{ .lower = "abc" }} }).page("abc", "abcd"));
}

test "highlight source offset interval planning merges large exact and prefix unions" {
    const a = std.testing.allocator;
    const Page = struct { ceiling: []const u8 };
    const pages = [_]Page{ .{ .ceiling = "" }, .{ .ceiling = "aaa" }, .{ .ceiling = "abc0" }, .{ .ceiling = "abc3" }, .{ .ceiling = "abd" }, .{ .ceiling = "\xff" }, .{ .ceiling = "\xffz" } };
    const cases = [_][]const Selection.Range{
        &.{},
        &.{.{ .lower = "abc3" }},
        &.{.{ .lower = "abc", .prefix = true }},
        &.{ .{ .lower = "abc", .prefix = true }, .{ .lower = "abc3" }, .{ .lower = "aaa" } },
        &.{.{ .lower = "", .prefix = true }},
        &.{.{ .lower = "\xff", .prefix = true }},
    };
    for (cases) |ranges| {
        const selection: Selection = .{ .ranges = ranges };
        const bits = (try selectedPages(a, &pages, selection)).?;
        defer a.free(bits);
        for (pages, bits, 0..) |page, bit, i| try std.testing.expectEqual(selection.page(if (i == 0) null else pages[i - 1].ceiling, page.ceiling), bit);
    }
    const many = try a.alloc(Selection.Range, 2048);
    defer a.free(many);
    @memset(many, .{ .lower = "abc", .prefix = true });
    const bits = (try selectedPages(a, &pages, .{ .ranges = many })).?;
    defer a.free(bits);
    try std.testing.expectEqualSlices(bool, &.{ false, false, true, true, true, false, false }, bits);
}
