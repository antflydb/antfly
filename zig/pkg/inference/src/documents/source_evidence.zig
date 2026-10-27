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

//! One bounded provenance index per extraction input. Field lookups use binary
//! search over disjoint source spans and visit only the evidence they emit.
const std = @import("std");
const provenance = @import("provenance.zig");
const placement = @import("placement.zig");
const c = @import("contract.zig");
const V = std.json.Value;
const A = std.mem.Allocator;
const Range = provenance.Range;
fn field(value: V, key: []const u8) ?V {
    return if (value == .object) value.object.get(key) else null;
}
fn optional(value: ?V) ?V {
    const item = value orelse return null;
    return if (item == .null) null else item;
}
fn offset(value: V) !usize {
    return switch (value) {
        .integer => std.math.cast(usize, value.integer) orelse error.InvalidDocumentEvidence,
        .number_string => std.fmt.parseUnsigned(usize, value.number_string, 10) catch error.InvalidDocumentEvidence,
        else => error.InvalidDocumentEvidence,
    };
}
fn origin(value: ?V, inherited: ?c.Origin) !?c.Origin {
    const item = optional(value) orelse return inherited;
    if (item != .string) return error.InvalidDocumentEvidence;
    return std.meta.stringToEnum(c.Origin, item.string) orelse error.InvalidDocumentEvidence;
}
fn literal(value: ?c.Origin) bool {
    const item = value orelse return true;
    return item == .native or item == .transcription;
}
const Counts = struct { nodes: usize = 0, spans: usize = 0 };
const Node = struct { id: usize, parent: ?usize };
const Segment = struct { base: usize, value: V };
const Span = struct { start: usize, end: usize, segment: usize, origin: ?c.Origin, node: ?usize };
pub const Index = struct {
    id: V = .null,
    spans: std.ArrayList(Span) = .empty,
    nodes: std.ArrayList(Node) = .empty,
    nonliteral: bool = false,
    segments: []Segment = &.{},
    page_region_limit: usize = 65536,

    /// Standalone callers accept the public absolute ceilings. The service
    /// supplies the active run limits after document/metadata admission.
    pub fn init(a: A, text: []const u8, metadata: ?V) !Index {
        return initWithLimits(a, text, metadata, .{ .max_regions = 65536, .max_pages = 10000, .max_output_bytes = 256 << 20 });
    }
    pub fn initWithLimits(a: A, text: []const u8, metadata: ?V, limits: c.Limits) !Index {
        try limits.validate();
        var self = Index{ .page_region_limit = limits.max_regions };
        const source = metadata orelse return self;
        const segments = field(source, "segments") orelse return self;
        self.id = field(source, "source_id") orelse return error.InvalidDocumentEvidence;
        // Bound aggregate index storage by the actual admitted metadata, rather
        // than imposing another document-wide region ceiling. Counting before
        // allocation also bounds ambiguous subtrees that emit no region spans.
        _ = try @import("antfly_documents").output.measure(source, limits.max_output_bytes);
        if (segments != .array) return error.InvalidDocumentEvidence;
        var pages: std.AutoHashMapUnmanaged(usize, usize) = .empty;
        defer pages.deinit(a);
        var total: usize = 0;
        var cursor: usize = 0;
        for (segments.array.items) |segment| {
            const start = try offset(field(segment, "start") orelse return error.InvalidDocumentEvidence);
            const end = try offset(field(segment, "end") orelse return error.InvalidDocumentEvidence);
            if (start < cursor or end < start or end > text.len) return error.InvalidDocumentEvidence;
            cursor = end;
            const number = try offset(field(segment, "number") orelse return error.InvalidDocumentEvidence);
            if (number == 0) return error.InvalidDocumentEvidence;
            if (pages.getPtr(number)) |remaining| {
                try countRegions(optional(field(segment, "regions")), 0, remaining, &total);
            } else {
                if (pages.count() >= limits.max_pages) return error.DocumentPageLimitExceeded;
                const remaining = try pages.getOrPut(a, number);
                remaining.value_ptr.* = limits.max_regions;
                try countRegions(optional(field(segment, "regions")), 0, remaining.value_ptr, &total);
            }
        }
        self.segments = try a.alloc(Segment, segments.array.items.len);
        for (segments.array.items, self.segments) |segment, *record| {
            record.* = .{ .base = try offset(field(segment, "start").?), .value = segment };
        }
        // Qualify once to size the materialized index exactly. Ambiguous or
        // homogeneous scopes can collapse without reserving storage for every
        // descendant. The second pass fills one allocation per compact array.
        var counts = Counts{};
        try self.build(text, &counts, false);
        std.debug.assert(counts.nodes <= total);
        try self.nodes.ensureTotalCapacityPrecise(a, counts.nodes);
        try self.spans.ensureTotalCapacityPrecise(a, counts.spans);
        counts = .{};
        try self.build(text, &counts, true);
        return self;
    }
    fn build(self: *Index, text: []const u8, counts: *Counts, comptime emit: bool) !void {
        for (self.segments, 0..) |record, index| {
            const segment = record.value;
            const end = try offset(field(segment, "end").?);
            try self.scope(text[record.base..end], index, .{ .start = 0, .end = end - record.base }, optional(field(segment, "regions")), try origin(field(segment, "origin"), null), null, 0, counts, emit);
        }
    }
    fn countRegions(regions: ?V, depth: usize, remaining: *usize, total: *usize) error{InvalidDocumentEvidence}!void {
        const list = regions orelse return;
        if (depth > 16 or list != .array or list.array.items.len > remaining.*) return error.InvalidDocumentEvidence;
        remaining.* -= list.array.items.len;
        total.* = std.math.add(usize, total.*, list.array.items.len) catch return error.InvalidDocumentEvidence;
        for (list.array.items) |region| {
            if (region != .object) return error.InvalidDocumentEvidence;
            try countRegions(optional(field(region, "children")), depth + 1, remaining, total);
        }
    }
    fn description(region: V) !placement.Description {
        const content = optional(field(region, "text"));
        if (content != null and content.? != .string) return error.InvalidDocumentEvidence;
        const first = optional(field(region, "text_start"));
        const last = optional(field(region, "text_end"));
        return .{ .text = if (content) |value| value.string else null, .first = if (first) |value| try offset(value) else null, .last = if (last) |value| try offset(value) else null };
    }
    fn locate(text: []const u8, range: Range, cursor: usize, region: V) !?Range {
        return placement.forward(text, range, cursor, try description(region));
    }
    fn qualified(text: []const u8, range: Range, items: []const V) !bool {
        var check = placement.Check.init(range);
        for (items) |item| if (!check.push(text, try description(item))) return false;
        var i = items.len;
        while (i > 0) {
            i -= 1;
            if (!check.pop(text, try description(items[i]))) return false;
        }
        return check.unique();
    }

    fn uniform(regions: []const V, inherited: ?c.Origin, depth: usize, remaining: *usize) error{InvalidDocumentEvidence}!bool {
        if (depth > 16 or regions.len > remaining.*) return error.InvalidDocumentEvidence;
        remaining.* -= regions.len;
        for (regions) |region| {
            const effective = try origin(field(region, "origin"), inherited);
            if (literal(effective) != literal(inherited)) return false;
            if (optional(field(region, "children"))) |children| {
                if (children != .array) return error.InvalidDocumentEvidence;
                if (!try uniform(children.array.items, effective, depth + 1, remaining)) return false;
            }
        }
        return true;
    }
    fn scope(self: *Index, text: []const u8, segment: usize, range: Range, regions: ?V, inherited: ?c.Origin, parent: ?usize, depth: usize, counts: *Counts, comptime emit: bool) error{InvalidDocumentEvidence}!void {
        if (depth > 16) return error.InvalidDocumentEvidence;
        const items = if (regions) |list| blk: {
            if (list != .array) return error.InvalidDocumentEvidence;
            break :blk list.array.items;
        } else &.{};
        // Qualify the complete sibling scope before publishing literal spans.
        // A later unlocatable/overlapping region cannot retract emitted proof.
        if (!try qualified(text, range, items)) {
            var budget = self.page_region_limit;
            const effective = if (try uniform(items, inherited, depth, &budget)) inherited else c.Origin.derived;
            try self.append(segment, range, effective, parent, counts, emit);
            return;
        }
        var cursor = range.start;
        for (items) |region| {
            const located = (try locate(text, range, cursor, region)).?;
            if (cursor < located.start) try self.append(segment, .{ .start = cursor, .end = located.start }, inherited, parent, counts, emit);
            const node = counts.nodes;
            const id = try offset(field(region, "id") orelse return error.InvalidDocumentEvidence);
            counts.nodes += 1;
            if (emit) self.nodes.appendAssumeCapacity(.{ .id = id, .parent = parent });
            try self.scope(text, segment, located, optional(field(region, "children")), try origin(field(region, "origin"), inherited), node, depth + 1, counts, emit);
            cursor = located.end;
        }
        if (cursor < range.end) try self.append(segment, .{ .start = cursor, .end = range.end }, inherited, parent, counts, emit);
    }
    fn append(self: *Index, segment: usize, range: Range, effective: ?c.Origin, node: ?usize, counts: *Counts, comptime emit: bool) !void {
        if (range.start == range.end) return;
        const base = self.segments[segment].base;
        counts.spans += 1;
        if (emit) self.spans.appendAssumeCapacity(.{ .start = base + range.start, .end = base + range.end, .segment = segment, .origin = effective, .node = node });
        self.nonliteral = self.nonliteral or !literal(effective);
    }
    pub fn map(self: *const Index, a: A, out: *std.array_list.Managed(V), range: [2]usize) !void {
        var low: usize = 0;
        var high = self.spans.items.len;
        while (low < high) {
            const middle = low + (high - low) / 2;
            if (self.spans.items[middle].end <= range[0]) low = middle + 1 else high = middle;
        }
        for (self.spans.items[low..]) |span| {
            const segment = self.segments[span.segment];
            if (span.start >= range[1]) break;
            var reference = V{ .object = .{} };
            try reference.object.put(a, "source_id", self.id);
            try reference.object.put(a, "offset_unit", .{ .string = "utf8_bytes" });
            try reference.object.put(a, "page_number", field(segment.value, "number") orelse return error.InvalidDocumentEvidence);
            try reference.object.put(a, "start", .{ .integer = @intCast(@max(span.start, range[0]) - segment.base) });
            try reference.object.put(a, "end", .{ .integer = @intCast(@min(span.end, range[1]) - segment.base) });
            if (span.origin) |value| try reference.object.put(a, "origin", .{ .string = @tagName(value) });
            if (field(segment.value, "role")) |role| try reference.object.put(a, "role", role);
            // Component-local offsets qualify synthetic table views without
            // claiming that appended bytes existed in the page's flat text.
            inline for (.{ "table_id", "table_scope", "table_index", "cell_index", "region_path", "row", "column", "row_span", "column_span", "coordinate_space", "bbox" }) |key| if (field(segment.value, key)) |value| try reference.object.put(a, key, value);

            if (span.node) |node| {
                var path: [17]usize = undefined;
                var length: usize = 0;
                var next: ?usize = node;
                while (next) |index| {
                    path[length] = self.nodes.items[index].id;
                    length += 1;
                    next = self.nodes.items[index].parent;
                }
                var array = std.json.Array.init(a);
                try array.ensureTotalCapacity(length);
                while (length > 0) {
                    length -= 1;
                    array.appendAssumeCapacity(try @import("representation.zig").unsignedValue(a, path[length]));
                }
                try reference.object.put(a, "region_path", .{ .array = array });
            }
            try out.append(reference);
        }
    }
};

test "document evidence indexing spans many independently admitted pages without aggregate structural ceilings" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const page_count = 50;
    const region_count = 4096;
    const regions = try a.alloc(c.Region, region_count);
    const page_text = try a.alloc(u8, region_count);
    @memset(page_text, 'x');
    for (regions, 0..) |*region, i| region.* = .{ .id = i, .text = "x", .origin = .native, .text_start = i, .text_end = i + 1 };
    // Borrow one immutable region tree across pages, just as retained input
    // values are borrowed by the service; no giant test-only JSON tree copy.
    const region_json = try std.json.Stringify.valueAlloc(a, regions, .{});
    const region_values = try std.json.parseFromSliceLeaky(V, a, region_json, .{});
    var segments = std.json.Array.init(a);
    const state = try a.alloc(u8, page_count * (region_count + 2));
    @memset(state, 'x');
    var run = c.Run{ .limits = .{ .max_output_bytes = 128 << 20 } };
    for (0..page_count) |i| {
        const page = c.Page{ .source_id = "report", .number = i + 1, .kind = .page, .text = page_text, .origin = .native, .status = .complete, .regions = regions };
        try @import("representation.zig").validatePage(page, run.limits.max_regions);
        _ = try run.outputJson(page);
        var segment = V{ .object = .{} };
        try segment.object.put(a, "number", .{ .integer = @intCast(i + 1) });
        try segment.object.put(a, "start", .{ .integer = @intCast(i * (region_count + 2)) });
        try segment.object.put(a, "end", .{ .integer = @intCast(i * (region_count + 2) + region_count) });
        try segment.object.put(a, "origin", .{ .string = "native" });
        try segment.object.put(a, "regions", region_values);
        try segments.append(segment);
    }
    var metadata = V{ .object = .{} };
    try metadata.object.put(a, "source_id", .{ .string = "report" });
    try metadata.object.put(a, "segments", .{ .array = segments });
    try run.output(try @import("antfly_documents").output.measure(metadata, run.remainingOutput()));
    const index = try Index.initWithLimits(a, state, metadata, run.limits);
    try std.testing.expectEqual(@as(usize, page_count * region_count), index.nodes.items.len);
    try std.testing.expectEqual(@as(usize, page_count * region_count), index.spans.items.len);
    var references: std.array_list.Managed(V) = .init(a);
    const last = (page_count - 1) * (region_count + 2) + region_count - 1;
    try index.map(a, &references, .{ last, last + 1 });
    try std.testing.expectEqual(@as(usize, 1), references.items.len);
    const reference = references.items[0];
    try std.testing.expectEqual(@as(i64, page_count), field(reference, "page_number").?.integer);
    try std.testing.expectEqual(@as(i64, region_count - 1), field(reference, "start").?.integer);
    try std.testing.expectEqual(@as(i64, region_count - 1), field(reference, "region_path").?.array.items[0].integer);
}

test "document evidence limits qualify page identities and combine page and note regions" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const metadata = try std.json.parseFromSliceLeaky(V, a,
        \\{"source_id":"report","segments":[{"number":3,"start":0,"end":1,"regions":[{"id":1,"text":"a"}]},{"number":3,"role":"speaker_notes","start":1,"end":2,"regions":[{"id":2,"text":"b"}]},{"number":9,"start":2,"end":4,"regions":[{"id":1,"text":"c"},{"id":2,"text":"d"}]}]}
    , .{});
    _ = try Index.initWithLimits(a, "abcd", metadata, .{ .max_regions = 2, .max_pages = 2 });
    try std.testing.expectError(error.InvalidDocumentEvidence, Index.initWithLimits(a, "abcd", metadata, .{ .max_regions = 1, .max_pages = 2 }));
    try std.testing.expectError(error.DocumentPageLimitExceeded, Index.initWithLimits(a, "abcd", metadata, .{ .max_regions = 2, .max_pages = 1 }));
    // Byte admission happens before any index or page-map allocations.
    try std.testing.expectError(error.DocumentOutputTooLarge, Index.initWithLimits(std.testing.failing_allocator, "abcd", metadata, .{ .max_output_bytes = 1 }));
    const nested = try std.json.parseFromSliceLeaky(V, a,
        \\{"source_id":"report","segments":[{"number":1,"start":0,"end":1,"regions":[{"id":1,"text":"missing","origin":"interpretation","children":[{"id":2},{"id":3}]}]}]}
    , .{});
    // Ambiguity must not hide an oversized subtree during preflight.
    try std.testing.expectError(error.InvalidDocumentEvidence, Index.initWithLimits(a, "a", nested, .{ .max_regions = 2 }));
}

test "document evidence storage sizes collapsed scopes from emitted spans" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const empty_regions = try a.alloc(V, 4096);
    @memset(empty_regions, .{ .object = .{} });
    var region_array = std.json.Array.init(a);
    try region_array.appendSlice(empty_regions);
    var segment = V{ .object = .{} };
    try segment.object.put(a, "number", .{ .integer = 1 });
    try segment.object.put(a, "start", .{ .integer = 0 });
    try segment.object.put(a, "end", .{ .integer = 1 });
    try segment.object.put(a, "regions", .{ .array = region_array });
    var segments = std.json.Array.init(a);
    try segments.append(segment);
    var metadata = V{ .object = .{} };
    try metadata.object.put(a, "source_id", .{ .string = "report" });
    try metadata.object.put(a, "segments", .{ .array = segments });
    var budget = std.testing.FailingAllocator.init(a, .{});
    const index = try Index.initWithLimits(budget.allocator(), "a", metadata, .{});
    try std.testing.expectEqual(@as(usize, 0), index.nodes.items.len);
    try std.testing.expectEqual(@as(usize, 1), index.spans.items.len);
    try std.testing.expect(budget.allocated_bytes < 8192);
}

test "document raw ambiguous repeated provenance cannot supply literal field evidence" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_][]const u8{ "", ",\"text_start\":0,\"text_end\":1", ",\"text_start\":3" }) |offsets| {
        const metadata = try std.json.parseFromSliceLeaky(V, a, try std.fmt.allocPrint(a, "{{\"source_id\":\"receipt\",\"segments\":[{{\"number\":1,\"start\":0,\"end\":5,\"origin\":\"transcription\",\"regions\":[{{\"id\":1,\"text\":\"10\",\"origin\":\"interpretation\"{s}}}]}}]}}", .{offsets}), .{});
        const index = try Index.init(a, "10 10", metadata);
        for ([_]usize{ 0, 3 }) |start| {
            var proof = std.json.Array.init(a);
            try index.map(a, &proof, .{ start, start + 2 });
            try std.testing.expectEqualStrings("derived", proof.items[0].object.get("origin").?.string);
        }
    }
}
