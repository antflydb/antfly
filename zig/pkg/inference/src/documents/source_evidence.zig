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
fn origin(value: ?V, inherited: ?V) !?V {
    const item = optional(value) orelse return inherited;
    if (item != .string or std.meta.stringToEnum(c.Origin, item.string) == null) return error.InvalidDocumentEvidence;
    return item;
}
fn literal(value: ?V) bool {
    const item = value orelse return true;
    return std.mem.eql(u8, item.string, "native") or std.mem.eql(u8, item.string, "transcription");
}
const Node = struct { id: usize, parent: ?usize };
const Span = struct { start: usize, end: usize, base: usize, segment: V, origin: ?V, node: ?usize };
pub const Index = struct {
    id: V = .null,
    spans: std.ArrayList(Span) = .empty,
    nodes: std.ArrayList(Node) = .empty,
    nonliteral: bool = false,
    remaining: usize = 65536,

    pub fn init(a: A, text: []const u8, metadata: ?V) !Index {
        var self = Index{};
        const source = metadata orelse return self;
        const segments = field(source, "segments") orelse return self;
        self.id = field(source, "source_id") orelse return error.InvalidDocumentEvidence;
        if (segments != .array or segments.array.items.len > 20000) return error.InvalidDocumentEvidence;
        var cursor: usize = 0;
        for (segments.array.items) |segment| {
            const start = try offset(field(segment, "start") orelse return error.InvalidDocumentEvidence);
            const end = try offset(field(segment, "end") orelse return error.InvalidDocumentEvidence);
            if (start < cursor or end < start or end > text.len) return error.InvalidDocumentEvidence;
            cursor = end;
            try self.scope(a, text[start..end], segment, start, .{ .start = 0, .end = end - start }, optional(field(segment, "regions")), try origin(field(segment, "origin"), null), null, 0);
        }
        return self;
    }
    fn locate(text: []const u8, range: Range, cursor: usize, region: V) !?Range {
        const content = optional(field(region, "text"));
        if (content != null and content.? != .string) return error.InvalidDocumentEvidence;
        const first = optional(field(region, "text_start"));
        const last = optional(field(region, "text_end"));
        return provenance.locateRange(text, range, cursor, if (content) |value| value.string else null, if (first) |value| try offset(value) else null, if (last) |value| try offset(value) else null);
    }
    fn uniform(regions: []const V, inherited: ?V, depth: usize, remaining: *usize) error{InvalidDocumentEvidence}!bool {
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
    fn scope(self: *Index, a: A, text: []const u8, segment: V, base: usize, range: Range, regions: ?V, inherited: ?V, parent: ?usize, depth: usize) error{ OutOfMemory, InvalidDocumentEvidence }!void {
        if (depth > 16) return error.InvalidDocumentEvidence;
        const items = if (regions) |list| blk: {
            if (list != .array) return error.InvalidDocumentEvidence;
            break :blk list.array.items;
        } else &.{};
        if (items.len > self.remaining) return error.InvalidDocumentEvidence;
        self.remaining -= items.len;
        // Qualify the complete sibling scope before publishing literal spans.
        // A later unlocatable/overlapping region cannot retract emitted proof.
        var cursor = range.start;
        for (items) |region| {
            const located = try locate(text, range, cursor, region) orelse {
                var budget: usize = 65536;
                const effective = if (try uniform(items, inherited, depth, &budget)) inherited else V{ .string = "derived" };
                try self.append(a, segment, base, range, effective, parent);
                return;
            };
            cursor = located.end;
        }
        cursor = range.start;
        for (items) |region| {
            const located = (try locate(text, range, cursor, region)).?;
            if (cursor < located.start) try self.append(a, segment, base, .{ .start = cursor, .end = located.start }, inherited, parent);
            const node = self.nodes.items.len;
            try self.nodes.append(a, .{ .id = try offset(field(region, "id") orelse return error.InvalidDocumentEvidence), .parent = parent });
            try self.scope(a, text, segment, base, located, optional(field(region, "children")), try origin(field(region, "origin"), inherited), node, depth + 1);
            cursor = located.end;
        }
        if (cursor < range.end) try self.append(a, segment, base, .{ .start = cursor, .end = range.end }, inherited, parent);
    }
    fn append(self: *Index, a: A, segment: V, base: usize, range: Range, effective: ?V, node: ?usize) !void {
        if (range.start == range.end) return;
        if (self.spans.items.len >= 196608) return error.InvalidDocumentEvidence;
        try self.spans.append(a, .{ .start = base + range.start, .end = base + range.end, .base = base, .segment = segment, .origin = effective, .node = node });
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
            if (span.start >= range[1]) break;
            var reference = V{ .object = .{} };
            try reference.object.put(a, "source_id", self.id);
            try reference.object.put(a, "offset_unit", .{ .string = "utf8_bytes" });
            try reference.object.put(a, "page_number", field(span.segment, "number") orelse return error.InvalidDocumentEvidence);
            try reference.object.put(a, "start", .{ .integer = @intCast(@max(span.start, range[0]) - span.base) });
            try reference.object.put(a, "end", .{ .integer = @intCast(@min(span.end, range[1]) - span.base) });
            if (span.origin) |value| try reference.object.put(a, "origin", value);
            if (field(span.segment, "role")) |role| try reference.object.put(a, "role", role);
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
                    array.appendAssumeCapacity(.{ .integer = @intCast(path[length]) });
                }
                try reference.object.put(a, "region_path", .{ .array = array });
            }
            try out.append(reference);
        }
    }
};
