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
const magic = "AFOV0005";
const header_size = 32;
const block_tokens = 1024;
const page_terms = 64;
const page_bytes = 64 * 1024;
pub const Ref = struct {
    offset: u64,
    wire: u32,
    raw: u32,
    count: u32,
    through_end: u32 = 0,
    minimum_start: u32 = 0,
    minimum_gap: u32 = 0,
};
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
    try integer(a, out, u32, ref.through_end);
    try integer(a, out, u32, ref.minimum_start);
    try integer(a, out, u32, ref.minimum_gap);
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
        return .{ .offset = try self.int(u64), .wire = try self.int(u32), .raw = try self.int(u32), .count = try self.int(u32), .through_end = try self.int(u32), .minimum_start = try self.int(u32), .minimum_gap = try self.int(u32) };
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
    var ordered = true;
    var disjoint = true;
    for (tokens, 0..) |token, i| {
        if (i != 0 and (token.start_byte < tokens[i - 1].start_byte or (token.start_byte == tokens[i - 1].start_byte and token.end_byte > tokens[i - 1].end_byte))) ordered = false;
        if (i != 0 and token.start_byte <= tokens[i - 1].end_byte) disjoint = false;
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
        var prior: ?analysis.Token = null;
        var minimum_start: u32 = std.math.maxInt(u32);
        var minimum_gap: u32 = std.math.maxInt(u32);
        var through_end: u32 = 0;
        var regular = true;
        const first_token = tokens[group.first];
        var stride: ?u32 = null;
        while (remaining != 0) {
            const n = @min(remaining, block_tokens);
            raw.clearRetainingCapacity();
            var start: i64 = 0;
            var block_start: u32 = std.math.maxInt(u32);
            var block_gap: u32 = std.math.maxInt(u32);
            for (0..n) |_| {
                const token = tokens[at];
                through_end = @max(through_end, token.end_byte);
                if (prior) |previous_token| {
                    const step = token.start_byte -| previous_token.start_byte;
                    block_start = @min(block_start, step);
                    block_gap = @min(block_gap, token.start_byte -| previous_token.end_byte);
                    if (stride) |held| {
                        if (held != step) regular = false;
                    } else stride = step;
                    minimum_start = @min(minimum_start, step);
                    minimum_gap = @min(minimum_gap, token.start_byte -| previous_token.end_byte);
                }
                if (token.end_byte - token.start_byte != first_token.end_byte - first_token.start_byte) regular = false;
                prior = token;
                const delta = @as(i64, token.start_byte) - start;
                try variable(a, &raw, @bitCast((delta << 1) ^ (delta >> 63)));
                try variable(a, &raw, token.end_byte - token.start_byte);
                try variable(a, &raw, token.position);
                start = token.start_byte;
                at = next[at];
            }
            var ref = try chunk(a, sink, base, raw.items, n);
            ref.through_end = through_end;
            ref.minimum_start = if (block_start == std.math.maxInt(u32)) 0 else block_start;
            ref.minimum_gap = if (block_gap == std.math.maxInt(u32)) 0 else block_gap;
            try refs.append(a, ref);
            remaining -= n;
        }
        try text(a, &pending, group.term);
        try integer(a, &pending, u32, if (group.count < 2) 0 else minimum_start);
        try integer(a, &pending, u32, if (group.count < 2) 0 else minimum_gap);
        try integer(a, &pending, u32, first_token.start_byte);
        try integer(a, &pending, u32, if (regular and ordered and disjoint and group.count > 1) first_token.end_byte - first_token.start_byte else 0);
        try integer(a, &pending, u32, @intCast(refs.items.len));
        for (refs.items) |ref| try reference(a, &pending, ref);
        previous = group.term;
        terms += 1;
    }
    if (terms != 0) {
        try dictionary(a, sink, base, &pending, terms, previous, &root);
        pages += 1;
    }
    // Dense, diverse queries use a bounded source-order lane instead of one
    // decoded head block per term. Reuse the build links as compact term IDs.
    refs.clearRetainingCapacity();
    if (ordered and groups.items.len >= 64) {
        for (groups.items, 0..) |group, ordinal| {
            var at = group.first;
            for (0..group.count) |_| {
                const following = next[at];
                next[at] = @intCast(ordinal);
                at = following;
            }
        }
        var begin: usize = 0;
        var through: u32 = 0;
        while (begin < tokens.len) {
            const end = @min(tokens.len, begin + block_tokens);
            raw.clearRetainingCapacity();
            var start: i64 = 0;
            for (tokens[begin..end], next[begin..end]) |token, ordinal| {
                const delta = @as(i64, token.start_byte) - start;
                try variable(a, &raw, @bitCast((delta << 1) ^ (delta >> 63)));
                try variable(a, &raw, token.end_byte - token.start_byte);
                try variable(a, &raw, token.position);
                try variable(a, &raw, ordinal);
                start = token.start_byte;
                through = @max(through, token.end_byte);
            }
            var ref = try chunk(a, sink, base, raw.items, @intCast(end - begin));
            ref.through_end = through;
            try refs.append(a, ref);
            begin = end;
        }
    }
    try integer(a, &root, u32, @intCast(refs.items.len));
    for (refs.items) |ref| try reference(a, &root, ref);
    std.mem.writeInt(u32, root.items[0..4], pages, .little);
    const root_ref = try chunk(a, sink, base, root.items, pages);
    var header: [header_size]u8 = undefined;
    @memcpy(header[0..8], magic);
    std.mem.writeInt(u64, header[8..16], root_ref.offset, .little);
    std.mem.writeInt(u32, header[16..20], root_ref.wire, .little);
    std.mem.writeInt(u32, header[20..24], root_ref.raw, .little);
    std.mem.writeInt(u32, header[24..28], count, .little);
    if (groups.items.len > 0x3fffffff) return error.InvalidData;
    std.mem.writeInt(u32, header[28..32], @as(u32, @intCast(groups.items.len)) | (if (ordered) @as(u32, 0x80000000) else 0) | (if (ordered and disjoint) @as(u32, 0x40000000) else 0), .little);
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
pub fn prepare(a: A, view: View, source_length: u32, root_raw: u32, count: u32, selection: Selection, control: Control) !Plan {
    return prepareInternal(a, view, source_length, root_raw, count, selection, control, true, null);
}
fn maskContains(mask: []const u8, ordinal: usize) bool {
    return mask[ordinal / 8] & (@as(u8, 1) << @as(u3, @intCast(ordinal % 8))) != 0;
}
fn suffixBounds(blocks: anytype) void {
    var at = blocks.len;
    while (at > 1) {
        at -= 1;
        blocks[at - 1].minimum_start = @min(blocks[at - 1].minimum_start, blocks[at].minimum_start);
        blocks[at - 1].minimum_gap = @min(blocks[at - 1].minimum_gap, blocks[at].minimum_gap);
    }
}
fn prepareInternal(a: A, view: View, source_length: u32, root_raw: u32, count: u32, selection: Selection, control: Control, compact: bool, known_mask: ?[]const u8) anyerror!Plan {
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
    if (n > (root_bytes.len - root.at) / 36) return error.InvalidData;
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
    const lane_count = try root.int(u32);
    if (lane_count > (root.bytes.len - root.at) / 32) return error.InvalidData;
    const lane_refs = try work.alloc(Ref, lane_count);
    defer work.free(lane_refs);
    var lane_total: u64 = 0;
    var lane_end: u64 = prior_end;
    var lane_through: u32 = 0;
    for (lane_refs, 0..) |*ref, i| {
        try control.check();
        ref.* = try root.ref();
        if (ref.offset < lane_end or ref.offset > root_ref.offset or ref.wire == 0 or ref.wire > root_ref.offset - ref.offset or ref.count == 0 or ref.count > block_tokens or ref.raw == 0 or ref.raw > page_bytes or ref.through_end < lane_through or ref.through_end > source_length or (i + 1 < lane_refs.len and ref.count != block_tokens)) return error.InvalidData;
        lane_end = ref.offset + ref.wire;
        lane_through = ref.through_end;
        lane_total += ref.count;
    }
    if (lane_count != 0 and (lane_total != count or std.mem.readInt(u32, h[28..32], .little) & 0x80000000 == 0)) return error.InvalidData;
    if (root.at != root.bytes.len or term_count != (std.mem.readInt(u32, h[28..32], .little) & 0x3fffffff) or term_count > count or (term_count == 0) != (count == 0)) return error.InvalidData;
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
    var tentative = compact and lane_refs.len != 0;
    if (selection.ranges) |ranges| {
        const exact_only = for (ranges) |range| {
            if (range.prefix) break false;
        } else true;
        if (exact_only and ranges.len < 64) tentative = false;
    }
    var possible: usize = 0;
    for (pages, 0..) |page, index| if (needed == null or needed.?[index]) {
        possible += page.ref.count;
    };
    if (possible < 64) tentative = false;
    const mask = try out.alloc(u8, if (tentative) (@as(usize, @intCast(term_count)) + 7) / 8 else 0);
    @memset(mask, 0);
    var density: std.ArrayList(Plan.DensityTerm) = .empty;
    var singletons: u32 = 0;
    var selected_terms: usize = 0;
    var terms: std.ArrayList(Plan.Term) = .empty;
    previous = null;
    var total: u64 = 0;
    var ordinal: u32 = 0;
    for (pages, 0..) |page, page_index| {
        const lower = previous;
        previous = page.ceiling;
        if (needed) |bits| if (!bits[page_index]) {
            ordinal += page.ref.count;
            continue;
        };
        if (known_mask) |bits| {
            const any = for (0..page.ref.count) |i| {
                if (maskContains(bits, ordinal + i)) break true;
            } else false;
            if (!any) {
                ordinal += page.ref.count;
                continue;
            }
        }
        const dictionary_bytes = try read(work, view, page.ref, root_ref.offset, control);
        defer work.free(dictionary_bytes);
        var p: Parser = .{ .bytes = dictionary_bytes };
        var prior = lower;
        for (0..page.ref.count) |_| {
            defer ordinal += 1;
            try control.check();
            const term = try p.string();
            if ((prior != null and std.mem.order(u8, prior.?, term) != .lt) or std.mem.order(u8, term, page.ceiling) == .gt) return error.InvalidData;
            prior = term;
            const minimum_start = try p.int(u32);
            const minimum_gap = try p.int(u32);
            const first_start = try p.int(u32);
            const regular_length = try p.int(u32);
            const blocks = try p.int(u32);
            if (blocks == 0 or blocks > (p.bytes.len - p.at) / 32) return error.InvalidData;
            const matched = if (known_mask) |bits| maskContains(bits, ordinal) else if (selection.matches) |check| try check(selection.ptr.?, term) else true;
            const retained = if (matched and !tentative) try out.dupe(u8, term) else "";
            const refs = try out.alloc(Ref, if (matched and !tentative) blocks else 0);
            var bounds: []Plan.DensityBlock = &.{};
            var block_end: u64 = header_size;
            var prior_through: u32 = 0;
            var occurrences: u32 = 0;
            const ordered = std.mem.readInt(u32, h[28..32], .little) & 0x80000000 != 0;
            const disjoint = std.mem.readInt(u32, h[28..32], .little) & 0x40000000 != 0;
            if (disjoint and !ordered) return error.InvalidData;
            for (0..blocks) |i| {
                const ref = try p.ref();
                if (ref.offset < block_end or ref.offset > page.ref.offset or ref.wire == 0 or ref.wire > page.ref.offset - ref.offset or ref.count == 0 or ref.count > block_tokens or ref.raw == 0 or ref.raw > page_bytes) return error.InvalidData;
                if ((i + 1 < blocks and ref.count != block_tokens) or ref.through_end > source_length or ref.through_end < prior_through) return error.InvalidData;
                prior_through = ref.through_end;
                occurrences = std.math.add(u32, occurrences, ref.count) catch return error.InvalidData;
                block_end = ref.offset + ref.wire;
                if (ref.minimum_start > source_length or ref.minimum_gap > source_length or (disjoint and (i != 0 or ref.count > 1) and (ref.minimum_start == 0 or ref.minimum_gap == 0))) return error.InvalidData;
                if (!matched) continue;
                if (tentative and i == 0 and (blocks > 1 or ref.count > 1)) bounds = try out.alloc(Plan.DensityBlock, blocks);
                total += ref.count;
                if (total > count) return error.InvalidData;
                if (!tentative) refs[i] = ref else if (bounds.len != 0) bounds[i] = .{ .through_end = ref.through_end, .minimum_start = ref.minimum_start, .minimum_gap = ref.minimum_gap };
            }
            if (first_start > source_length or regular_length > source_length - first_start or (regular_length != 0 and (!disjoint or occurrences < 2 or minimum_start <= regular_length or @as(u64, first_start) + @as(u64, occurrences - 1) * minimum_start + regular_length > source_length))) return error.InvalidData;
            if (minimum_start > source_length or minimum_gap > source_length or (occurrences == 1 and (minimum_start != 0 or minimum_gap != 0)) or (disjoint and occurrences > 1 and (minimum_start == 0 or minimum_gap == 0))) return error.InvalidData;
            if (matched) {
                selected_terms += 1;
                if (tentative) {
                    mask[ordinal / 8] |= @as(u8, 1) << @as(u3, @intCast(ordinal % 8));
                    if (occurrences == 1) singletons += 1 else {
                        suffixBounds(bounds);
                        try density.append(out, .{ .count = occurrences, .blocks = bounds });
                    }
                } else {
                    suffixBounds(refs);
                    try terms.append(out, .{ .text = retained, .blocks = refs, .count = occurrences, .minimum_start = minimum_start, .minimum_gap = minimum_gap, .ordinal = ordinal, .first_start = first_start, .regular_length = regular_length });
                }
            }
        }
        if (p.at != p.bytes.len or prior == null or !std.mem.eql(u8, prior.?, page.ceiling)) return error.InvalidData;
    }
    if (known_mask == null and selection.matches == null and selection.ranges == null and total != count) return error.InvalidData;
    if (tentative and (selected_terms < 64 or total * 8 < count)) {
        const expanded = try prepareInternal(a, view, source_length, root_raw, count, .{}, control, false, mask);
        owner.deinit();
        a.destroy(owner);
        return expanded;
    }
    return .{ .a = a, .owner = owner, .view = view, .source_length = source_length, .terms = try terms.toOwnedSlice(out), .count = @intCast(total), .root_raw = root_raw, .source_mask = if (tentative) mask else null, .selected_terms = selected_terms, .density_terms = try density.toOwnedSlice(out), .source_singletons = singletons, .source_count = count, .term_count = @intCast(term_count), .source_blocks = try out.dupe(Ref, lane_refs), .ordered = std.mem.readInt(u32, h[28..32], .little) & 0x80000000 != 0, .disjoint = std.mem.readInt(u32, h[28..32], .little) & 0x40000000 != 0 };
}
pub fn select(a: A, view: View, source_length: u32, root_raw: u32, count: u32, selection: Selection, control: Control) !Vector {
    const plan = try prepare(a, view, source_length, root_raw, count, selection, control);
    return plan.materializeOwned(control);
}
/// Immutable term/block navigation, independent of occurrence materialization.
pub const Plan = struct {
    pub const Term = struct { text: []const u8, blocks: []const Ref, count: u32, minimum_start: u32, minimum_gap: u32, ordinal: u32 = 0, first_start: u32 = 0, regular_length: u32 = 0 };
    pub const DensityBlock = struct { through_end: u32, minimum_start: u32, minimum_gap: u32 };
    pub const DensityTerm = struct { count: u32, blocks: []const DensityBlock };
    root_raw: u32 = 0,
    source_mask: ?[]const u8 = null,
    selected_terms: usize = 0,
    density_terms: []const DensityTerm = &.{},
    source_singletons: u32 = 0,
    a: A,
    owner: *std.heap.ArenaAllocator,
    view: View,
    source_length: u32,
    terms: []const Term,
    count: u32,
    source_count: u32 = 0,
    term_count: u32 = 0,
    source_blocks: []const Ref = &.{},
    ordered: bool,
    disjoint: bool,
    pub fn selectedTermCount(self: *const Plan) usize {
        return self.selected_terms;
    }
    fn expand(self: *const Plan, a: A, control: Control) !Plan {
        return prepareInternal(a, self.view, self.source_length, self.root_raw, self.source_count, .{}, control, false, self.source_mask);
    }

    /// Spacing is a producer-attested lower bound; deleting terms only widens
    /// gaps. For future windows, expired blocks cannot contribute. The first
    /// overlapping span ends at least one byte into an integer window, and the
    /// next starts after its gap; remaining starts are minimum_start apart.
    pub fn densityUpper(self: *const Plan, width: u64, first: u32, ceiling: u32, exact_boundaries: bool) ?u32 {
        if (!self.disjoint) return null;
        if (exact_boundaries and self.terms.len == 1 and self.terms[0].regular_length != 0) {
            const term = self.terms[0];
            const stride: i64 = term.minimum_start;
            const length: i64 = term.regular_length;
            const w: i64 = @intCast(@min(width, self.source_length));
            const offset = @divFloor(length, 2) - @divFloor(w, 2);
            const interior = @divFloor(offset + w - 1, stride) - @divFloor(offset - length, stride);
            const edge = struct {
                fn count(t: Term, start: i64, end: i64) u32 {
                    const lo = @max(0, @divFloor(start - t.first_start - t.regular_length, t.minimum_start) + 1);
                    const hi = @min(@as(i64, t.count) - 1, @divFloor(end - 1 - t.first_start, t.minimum_start));
                    return @intCast(@max(0, hi - lo + 1));
                }
            }.count;
            const upper = @max(@min(term.count, @as(u32, @intCast(interior))), @max(edge(term, 0, w), edge(term, @as(i64, self.source_length) - w, self.source_length)));
            return if (upper <= ceiling) upper else null;
        }
        if (self.source_mask != null) return densityBound(self.density_terms, self.source_singletons, width, first, ceiling);
        return densityBound(self.terms, 0, width, first, ceiling);
    }
    fn densityBound(terms: anytype, singletons: u32, width: u64, first: u32, ceiling: u32) ?u32 {
        var total: u64 = singletons;
        if (total > ceiling) return null;
        for (terms) |term| {
            const block = seekBlock(term.blocks, first);
            if (block == term.blocks.len) continue;
            const remaining = @as(u64, term.count) - block * block_tokens;
            const summary = term.blocks[block];
            const bound = if (remaining <= 1) @as(u64, 1) else if (width <= @as(u64, summary.minimum_gap) + 1) @as(u64, 1) else 2 + (width - 2 - summary.minimum_gap) / summary.minimum_start;
            total += @min(remaining, bound);
            if (total > ceiling) return null;
        }
        return @intCast(total);
    }
    pub fn deinit(self: *Plan) void {
        self.owner.deinit();
        self.a.destroy(self.owner);
    }
    /// Consume navigation and reuse its region for small vector results.
    pub fn materializeOwned(self: Plan, control: Control) !Vector {
        if (self.source_mask != null) {
            var original = self;
            defer original.deinit();
            return (try self.expand(self.a, control)).materializeOwned(control);
        }
        errdefer {
            self.owner.deinit();
            self.a.destroy(self.owner);
        }
        const out = self.owner.allocator();
        const work = @import("../sql/memory_budget.zig").reclaimingAllocator(self.a);
        var items: std.ArrayList(analysis.Token) = .empty;
        for (self.terms, 0..) |term, term_index| {
            if (term_index % 4 == 0) prefetchTerms(self.view, self.terms[term_index + 1 ..], 0, false);
            var cursor: Occurrences = .{};
            defer cursor.deinit(work);
            while (try cursor.next(work, &self, term, control)) |token| try items.append(out, token);
        }
        if (items.items.len != self.count) return error.InvalidData;
        return .{ .a = self.a, .owner = self.owner, .items = try items.toOwnedSlice(out) };
    }
    pub fn materialize(self: *const Plan, a: A, control: Control) !Vector {
        if (self.source_mask != null) return (try self.expand(a, control)).materializeOwned(control);
        const owner = try a.create(std.heap.ArenaAllocator);
        owner.* = .init(a);
        errdefer {
            owner.deinit();
            a.destroy(owner);
        }
        const out = owner.allocator();
        var items: std.ArrayList(analysis.Token) = .empty;
        const work = @import("../sql/memory_budget.zig").reclaimingAllocator(a);
        for (self.terms, 0..) |term, term_index| {
            if (term_index % 4 == 0) prefetchTerms(self.view, self.terms[term_index + 1 ..], 0, false);
            var cursor: Occurrences = .{};
            defer cursor.deinit(work);
            const retained = try out.dupe(u8, term.text);
            while (try cursor.next(work, self, term, control)) |token| {
                var value = token;
                value.term = retained;
                try items.append(out, value);
            }
        }
        if (items.items.len != self.count) return error.InvalidData;
        return .{ .a = a, .owner = owner, .items = try items.toOwnedSlice(out) };
    }
};
fn seekBlock(blocks: anytype, first: u32) usize {
    var low: usize = 0;
    var high = blocks.len;
    while (low < high) {
        const mid = low + (high - low) / 2;
        if (blocks[mid].through_end <= first) low = mid + 1 else high = mid;
    }
    return low;
}
/// Advisory reads use the enclosing source's bounded queue and shared scheduler.
/// Only selected terms' blocks are hinted; no intervening occurrence payloads.
fn prefetch(view: View, refs: []const Ref) void {
    var bytes: u64 = 0;
    for (refs[0..@min(refs.len, 4)]) |ref| {
        if (ref.wire > 256 * 1024 - bytes) break;
        view.source.prefetch(view.offset + ref.offset, ref.wire);
        bytes += ref.wire;
    }
}
fn prefetchTerms(view: View, terms: []const Plan.Term, first: u32, skip_required: bool) void {
    var hints: usize = 0;
    var required = skip_required;
    for (terms) |term| {
        const block = seekBlock(term.blocks, first);
        if (block == term.blocks.len) continue;
        // The caller reads the first required head synchronously. Hint only
        // future work so a one-block selection incurs no scheduler overhead.
        if (required) {
            required = false;
            continue;
        }
        prefetch(view, term.blocks[block .. block + 1]);
        hints += 1;
        if (hints == 4) break;
    }
}
const Occurrences = struct {
    block: usize = 0,
    decoded: usize = 0,
    bytes: []u8 = &.{},
    parser: Parser = .{ .bytes = &.{} },
    remaining: u32 = 0,
    start: i64 = 0,
    prior: ?analysis.Token = null,
    fn deinit(self: *Occurrences, a: A) void {
        a.free(self.bytes);
        self.* = .{};
    }
    fn next(self: *Occurrences, a: A, plan: *const Plan, term: Plan.Term, control: Control) !?analysis.Token {
        if (self.remaining == 0) {
            if (self.parser.at != self.parser.bytes.len) return error.InvalidData;
            if (self.block == term.blocks.len) return null;
            a.free(self.bytes);
            self.bytes = &.{};
            const ref = term.blocks[self.block];
            prefetch(plan.view, term.blocks[self.block + 1 ..]);
            self.bytes = try read(a, plan.view, ref, plan.view.length, control);
            self.decoded += 1;
            self.parser = .{ .bytes = self.bytes };
            self.remaining = ref.count;
            self.start = 0;
            self.block += 1;
        }
        if (self.remaining % 256 == 0) try control.check();
        const delta = try self.parser.variable();
        if (delta > @as(u64, std.math.maxInt(u32)) * 2) return error.InvalidData;
        self.start += @as(i64, @intCast(delta >> 1)) ^ -@as(i64, @intCast(delta & 1));
        const length = try self.parser.variable();
        const position = try self.parser.variable();
        if (self.start < 0 or self.start > plan.source_length or length > plan.source_length - @as(u64, @intCast(self.start)) or position > std.math.maxInt(u32)) return error.InvalidData;
        self.remaining -= 1;
        const token: analysis.Token = .{ .term = term.text, .start_byte = @intCast(self.start), .end_byte = @intCast(@as(u64, @intCast(self.start)) + length), .position = @intCast(position) };
        if (plan.ordered) if (self.prior) |prior| if (token.start_byte < prior.start_byte or (token.start_byte == prior.start_byte and token.end_byte > prior.end_byte)) return error.InvalidData;
        self.prior = token;
        return token;
    }
};
/// Restartable source-order merge. Each term retains one compressed-block decode,
/// not a token array. General unordered analyzer outputs use materialization.
pub const Iterator = struct {
    const State = struct { cursor: Occurrences = .{}, current: ?analysis.Token = null };
    a: A,
    plan: *const Plan,
    control: Control,
    states: []State = &.{},
    heap: []usize = &.{},
    len: usize = 0,
    retired_reads: usize = 0,
    source_cursor: Occurrences = .{},
    use_source: bool = false,
    resolved: ?*Plan = null,
    pub fn blockReads(self: *const Iterator) usize {
        var count = self.retired_reads + self.source_cursor.decoded;
        for (self.states) |state| count += state.cursor.decoded;
        return count;
    }
    pub fn init(a: A, plan: *const Plan, control: Control) !Iterator {
        if (!plan.ordered) return error.InvalidData;
        var self: Iterator = .{ .a = @import("../sql/memory_budget.zig").reclaimingAllocator(a), .plan = plan, .control = control };
        errdefer self.deinit();
        self.use_source = plan.source_mask != null or (plan.source_blocks.len != 0 and plan.terms.len >= 64 and @as(u64, plan.count) * 8 >= plan.source_count);
        if (self.use_source) {
            try self.reset();
            return self;
        }
        self.states = try self.a.alloc(State, plan.terms.len);
        @memset(self.states, .{});
        self.heap = try self.a.alloc(usize, plan.terms.len);
        try self.reset();
        return self;
    }
    pub fn deinit(self: *Iterator) void {
        self.source_cursor.deinit(self.a);
        if (self.resolved) |plan| {
            plan.deinit();
            self.a.destroy(plan);
        }
        for (self.states) |*state| state.cursor.deinit(self.a);
        self.a.free(self.states);
        self.a.free(self.heap);
    }
    fn before(self: *const Iterator, l: usize, r: usize) bool {
        const a = self.states[l].current.?;
        const b = self.states[r].current.?;
        if (a.start_byte != b.start_byte) return a.start_byte < b.start_byte;
        if (a.end_byte != b.end_byte) return a.end_byte > b.end_byte;
        return l < r;
    }
    fn sift(self: *Iterator, first: usize) void {
        var at = first;
        while (at < self.len / 2) {
            var child = at * 2 + 1;
            if (child + 1 < self.len and self.before(self.heap[child + 1], self.heap[child])) child += 1;
            if (!self.before(self.heap[child], self.heap[at])) break;
            std.mem.swap(usize, &self.heap[at], &self.heap[child]);
            at = child;
        }
    }
    pub fn reset(self: *Iterator) !void {
        return self.seek(0);
    }
    /// Per-block prefix maxima also support overlapping ordered spans. Seeking
    /// keeps every span that might cross the requested fragment's start.
    pub fn seek(self: *Iterator, first: u32) !void {
        self.len = 0;
        try self.control.check();
        if (self.use_source) {
            self.retired_reads += self.source_cursor.decoded;
            self.source_cursor.deinit(self.a);
            self.source_cursor.block = seekBlock(self.plan.source_blocks, first);
            return;
        }
        prefetchTerms(self.plan.view, self.plan.terms, first, true);
        for (self.states, self.plan.terms, 0..) |*state, term, i| {
            try self.control.check();
            self.retired_reads += state.cursor.decoded;
            state.cursor.deinit(self.a);
            state.cursor.block = seekBlock(term.blocks, first);
            state.current = try state.cursor.next(self.a, self.plan, term, self.control);
            if (state.current != null) {
                self.heap[self.len] = i;
                self.len += 1;
            }
        }
        var parent = self.len / 2;
        while (parent != 0) {
            parent -= 1;
            self.sift(parent);
        }
    }
    pub const Span = struct { start_byte: u32, end_byte: u32 };
    pub fn nextSpan(self: *Iterator) !?Span {
        const token = (try self.nextValue(false)) orelse return null;
        return .{ .start_byte = token.start_byte, .end_byte = token.end_byte };
    }
    pub fn next(self: *Iterator) !?analysis.Token {
        if (self.plan.source_mask != null and self.resolved == null) {
            const expanded = try self.a.create(Plan);
            errdefer self.a.destroy(expanded);
            expanded.* = try self.plan.expand(self.a, self.control);
            self.resolved = expanded;
        }
        return self.nextValue(true);
    }
    fn nextValue(self: *Iterator, comptime want_term: bool) !?analysis.Token {
        if (self.use_source) {
            const term: Plan.Term = .{ .text = "", .blocks = self.plan.source_blocks, .count = self.plan.source_count, .minimum_start = 0, .minimum_gap = 0 };
            while (try self.source_cursor.next(self.a, self.plan, term, self.control)) |token| {
                const ordinal = try self.source_cursor.parser.variable();
                if (ordinal >= self.plan.term_count) return error.InvalidData;
                if (self.plan.source_mask) |mask| if (!maskContains(mask, @intCast(ordinal))) continue;
                if (!want_term and self.plan.source_mask != null) return token;
                const terms = if (self.resolved) |plan| plan.terms else self.plan.terms;
                var low: usize = 0;
                var high = terms.len;
                while (low < high) {
                    const mid = low + (high - low) / 2;
                    if (terms[mid].ordinal < ordinal) low = mid + 1 else high = mid;
                }
                if (low < terms.len and terms[low].ordinal == ordinal) {
                    var result = token;
                    result.term = terms[low].text;
                    return result;
                }
            }
            return null;
        }
        if (self.len == 0) return null;
        const i = self.heap[0];
        const result = self.states[i].current.?;
        self.states[i].current = try self.states[i].cursor.next(self.a, self.plan, self.plan.terms[i], self.control);
        if (self.states[i].current == null) {
            self.len -= 1;
            if (self.len != 0) self.heap[0] = self.heap[self.len];
        }
        self.sift(0);
        return result;
    }
};

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

test "highlight occurrence block seeks preserve crossing spans and prefetch only selected blocks" {
    const a = std.testing.allocator;
    const segment = @import("../segment.zig");
    const tokens = try a.alloc(analysis.Token, 4096);
    defer a.free(tokens);
    for (tokens, 0..) |*token, i| token.* = .{ .term = if (i % 2 == 0) "needle" else "other", .start_byte = @intCast(i * 7), .end_byte = @intCast(i * 7 + 6), .position = @intCast(i) };
    var memory = segment.MemorySegmentSink.init(a);
    defer memory.deinit();
    var sink = memory.sink();
    const encoded = try write(a, &sink, tokens);
    const Range = struct {
        bytes: []const u8,
        hints: [64]struct { start: u64, len: u64 } = undefined,
        hint_count: usize = 0,
        fn close(_: *anyopaque) void {}
        fn read(raw: *anyopaque, at: u64, out: []u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            @memcpy(out, self.bytes[@intCast(at)..][0..out.len]);
        }
        fn hint(raw: *anyopaque, at: u64, len: u64) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.hint_count == self.hints.len) return;
            self.hints[self.hint_count] = .{ .start = at, .len = len };
            self.hint_count += 1;
        }
    };
    const Match = struct {
        fn needle(_: *anyopaque, term: []const u8) !bool {
            return std.mem.eql(u8, term, "needle");
        }
    };
    var range: Range = .{ .bytes = memory.out.items };
    const source = try View.init(.{ .ranges = .{ .ptr = &range, .length = memory.out.items.len, .read_into = Range.read, .close = Range.close, .prefetch = Range.hint } }, 0, memory.out.items.len);
    var plan = try prepare(a, source, 4096 * 7, encoded.root_raw, encoded.count, .{ .ptr = &range, .matches = Match.needle, .ranges = &.{.{ .lower = "needle" }} }, .none);
    defer plan.deinit();
    try std.testing.expect(plan.disjoint);
    try std.testing.expectEqual(@as(usize, 1), plan.terms.len);
    const vector = try plan.materialize(a, .none);
    defer vector.deinit();
    for ([_]u32{ 1, 2, 7, 17, 100, 513 }) |width| for ([_]u32{ 0, 1, 7, 7000, 14000, 28000 }) |first| {
        var actual: u32 = 0;
        for (vector.items) |token| if (token.start_byte < first + width and token.end_byte > first) {
            actual += 1;
        };
        try std.testing.expect(plan.densityUpper(width, first, std.math.maxInt(u32), false).? >= actual);
    };
    var iterator = try Iterator.init(a, &plan, .none);
    defer iterator.deinit();
    try iterator.seek(28000);
    try std.testing.expectEqual(@as(usize, 2), iterator.blockReads());
    while (try iterator.next()) |token| try std.testing.expect(token.start_byte >= 2048 * 7);
    try std.testing.expect(range.hint_count != 0);
    for (range.hints[0..range.hint_count]) |hint| {
        const found = for (plan.terms[0].blocks) |block| {
            if (block.offset == hint.start and block.wire == hint.len) break true;
        } else false;
        try std.testing.expect(found);
    }
    var all = try prepare(a, source, 4096 * 7, encoded.root_raw, encoded.count, .{}, .none);
    defer all.deinit();
    range.hint_count = 0;
    const complete = try all.materialize(a, .none);
    defer complete.deinit();
    try std.testing.expectEqual(@as(usize, 4096), complete.items.len);
    try std.testing.expect(range.hint_count != 0);
    // The next term's first block overlaps the current term's synchronous
    // read. Never enqueue that current head just before demanding it.
    try std.testing.expectEqual(all.terms[1].blocks[0].offset, range.hints[0].start);
    for (range.hints[0..range.hint_count]) |hint| try std.testing.expect(hint.start != all.terms[0].blocks[0].offset);
    // A long earlier span must remain visible to a later fragment seek.
    tokens[0].end_byte = 25000;
    var overlapping = segment.MemorySegmentSink.init(a);
    defer overlapping.deinit();
    var output = overlapping.sink();
    const overlap = try write(a, &output, tokens);
    var other = try prepare(a, try View.init(.{ .contiguous = overlapping.out.items }, 0, overlapping.out.items.len), 4096 * 7, overlap.root_raw, overlap.count, .{}, .none);
    defer other.deinit();
    try std.testing.expect(!other.disjoint);
    try std.testing.expect(other.densityUpper(100, 20000, 10, false) == null);
    var crossing = try Iterator.init(a, &other, .none);
    defer crossing.deinit();
    try crossing.seek(20000);
    const first = (try crossing.next()).?;
    try std.testing.expectEqual(@as(u32, 0), first.start_byte);
    try std.testing.expectEqual(@as(u32, 25000), first.end_byte);
}

test "highlight source order lanes filter term IDs and seek crossing spans exactly" {
    const a = std.testing.allocator;
    var names: [128][]u8 = undefined;
    for (&names, 0..) |*name, i| name.* = try std.fmt.allocPrint(a, "term{d:0>3}", .{i});
    defer for (names) |name| a.free(name);
    const tokens = try a.alloc(analysis.Token, 4096);
    defer a.free(tokens);
    for (tokens, 0..) |*token, i| token.* = .{ .term = names[i % names.len], .start_byte = @intCast(i * 10), .end_byte = @intCast(i * 10 + if (i % 37 == 0) @as(usize, 500) else 3), .position = @intCast(i) };
    var memory = @import("../segment.zig").MemorySegmentSink.init(a);
    defer memory.deinit();
    var sink = memory.sink();
    const encoded = try write(a, &sink, tokens);
    const Match = struct {
        fn selected(_: *anyopaque, term: []const u8) !bool {
            return term.len == 7 and term[6] % 2 == 0;
        }
    };
    var dummy: u8 = 0;
    var plan = try prepare(a, try View.init(.{ .contiguous = memory.out.items }, 0, memory.out.items.len), 42000, encoded.root_raw, encoded.count, .{ .ptr = &dummy, .matches = Match.selected }, .none);
    defer plan.deinit();
    try std.testing.expectEqual(@as(usize, 64), plan.selectedTermCount());
    const vector = try plan.materialize(a, .none);
    defer vector.deinit();
    var iterator = try Iterator.init(a, &plan, .none);
    defer iterator.deinit();
    try std.testing.expect(iterator.use_source);
    for ([_]u32{ 0, 10240, 20321, 40500 }) |first| {
        try iterator.seek(first);
        var expected: usize = 0;
        while (expected < vector.items.len and vector.items[expected].end_byte <= first) : (expected += 1) {}
        // The materialized inverted vector is term-major; compare a source
        // ordering independently of the source-lane iterator implementation.
        const sorted = try a.dupe(analysis.Token, vector.items);
        defer a.free(sorted);
        std.mem.sort(analysis.Token, sorted, {}, struct {
            fn less(_: void, l: analysis.Token, r: analysis.Token) bool {
                return l.start_byte < r.start_byte;
            }
        }.less);
        expected = 0;
        while (try iterator.next()) |token| {
            if (token.end_byte <= first) continue;
            while (expected < sorted.len and sorted[expected].end_byte <= first) : (expected += 1) {}
            try std.testing.expect(expected < sorted.len);
            try std.testing.expectEqualDeep(sorted[expected], token);
            expected += 1;
        }
        while (expected < sorted.len and sorted[expected].end_byte <= first) : (expected += 1) {}
        try std.testing.expectEqual(sorted.len, expected);
    }
}

test "highlight compact navigation and lazy full tokens unwind every allocation failure" {
    const a = std.testing.allocator;
    var names: [64][7]u8 = undefined;
    var tokens: [128]analysis.Token = undefined;
    for (&names, 0..) |*name, i| _ = try std.fmt.bufPrint(name, "term{d:0>3}", .{i});
    for (&tokens, 0..) |*token, i| token.* = .{ .term = &names[i % names.len], .start_byte = @intCast(i * 10), .end_byte = @intCast(i * 10 + 7), .position = @intCast(i) };
    var memory = @import("../segment.zig").MemorySegmentSink.init(a);
    defer memory.deinit();
    var sink = memory.sink();
    const encoded = try write(a, &sink, &tokens);
    const view = try View.init(.{ .contiguous = memory.out.items }, 0, memory.out.items.len);
    const Scenario = struct {
        fn run(alloc: A, source: View, metadata: Encoded) !void {
            var plan = try prepare(alloc, source, 1280, metadata.root_raw, metadata.count, .{}, .none);
            defer plan.deinit();
            try std.testing.expect(plan.source_mask != null);
            try std.testing.expectEqual(@as(usize, 0), plan.terms.len);
            var iterator = try Iterator.init(alloc, &plan, .none);
            defer iterator.deinit();
            try std.testing.expectEqual(@as(u32, 0), (try iterator.nextSpan()).?.start_byte);
            try std.testing.expect(iterator.resolved == null);
            try iterator.reset();
            try std.testing.expectEqualStrings("term000", (try iterator.next()).?.term);
        }
    };
    try Scenario.run(a, view, encoded);
    var stable = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(stable.allocator(), Scenario.run, .{ view, encoded });
}
