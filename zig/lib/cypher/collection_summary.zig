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

//! Flat execution carriers replace one public value container rather than
//! wrapping it. This keeps private summaries out of public depth limits and
//! lets ordinary row ownership transfers preserve payload sharing.
const std = @import("std");
const v = @import("value.zig");
const A = std.mem.Allocator;
pub const Packed = struct { value: v.Value, code: i64 = 0 };
fn properties(a: A, cells: *std.ArrayList(v.Value), entries: []const v.Entry) !void {
    try cells.append(a, .{ .integer = @intCast(entries.len) });
    for (entries) |entry| {
        try cells.append(a, .{ .string = entry.name });
        try cells.append(a, entry.value);
    }
}
pub fn pack(a: A, cells: *std.ArrayList(v.Value), value: v.Value) !Packed {
    const offset = cells.items.len;
    switch (value) {
        .list => |items| {
            try cells.append(a, .{ .integer = @intCast(items.len) });
            try cells.appendSlice(a, items);
        },
        .map => |entries| try properties(a, cells, entries),
        .node => |node| {
            const labels = try a.alloc(v.Value, node.labels.len);
            for (node.labels, labels) |label, *out| out.* = .{ .string = label };
            const header = try a.dupe(v.Value, &.{ .{ .string = node.identity.table }, .{ .string = node.identity.key }, .{ .list = labels } });
            // Carrier metadata is scalar: entity projection must only visit
            // actual payload cells, never hydrate a synthetic identity header.
            try cells.append(a, .{ .list = header });
            try properties(a, cells, node.properties);
        },
        .relationship => |edge| {
            const header = try a.dupe(v.Value, &.{
                .{ .string = edge.identity.table },                                   .{ .string = edge.identity.key },
                .{ .string = edge.index },                                            .{ .string = edge.type },
                .{ .string = edge.source.table },                                     .{ .string = edge.source.key },
                .{ .string = edge.target.table },                                     .{ .string = edge.target.key },
                .{ .integer = @backingInt(edge.ownership) },                          if (edge.producer) |producer| .{ .string = producer.table } else .null,
                if (edge.producer) |producer| .{ .string = producer.key } else .null,
            });
            try cells.append(a, .{ .list = header });
            try properties(a, cells, edge.properties);
        },
        .path => |path| {
            try cells.append(a, .{ .integer = @intCast(path.nodes.len) });
            try cells.append(a, .{ .integer = @intCast(path.relationships.len) });
            for (path.nodes) |node| try cells.append(a, .{ .node = node });
            for (path.relationships) |edge| try cells.append(a, .{ .relationship = edge });
        },
        else => return .{ .value = value },
    }
    return .{ .value = .{ .integer = @intCast(offset) }, .code = -1 - @as(i64, @backingInt(std.meta.activeTag(value))) };
}
fn decodeEntries(a: A, cells: []const v.Value) ![]const v.Entry {
    const result = try a.alloc(v.Entry, @intCast(cells[0].integer));
    for (result, 0..) |*entry, i| entry.* = .{ .name = cells[1 + i * 2].string, .value = cells[2 + i * 2] };
    return result;
}
pub fn unpack(a: A, cells: []const v.Value, encoded: Packed) !v.Value {
    if (encoded.code > 0) return @errorFromInt(@as(@Int(.unsigned, @bitSizeOf(anyerror)), @intCast(encoded.code)));
    if (encoded.code == 0) return encoded.value;
    const payload = cells[@intCast(encoded.value.integer)..];
    const tag: std.meta.Tag(v.Value) = @fromBackingInt(@intCast(-encoded.code - 1));
    return switch (tag) {
        .list => .{ .list = payload[1..][0..@intCast(payload[0].integer)] },
        .map => .{ .map = try decodeEntries(a, payload) },
        .node => blk: {
            const node = try a.create(v.Node);
            const header = payload[0].list;
            const labels = try a.alloc([]const u8, header[2].list.len);
            for (header[2].list, labels) |label, *out| out.* = label.string;
            node.* = .{ .identity = .{ .table = header[0].string, .key = header[1].string }, .labels = labels, .properties = try decodeEntries(a, payload[1..]) };
            break :blk .{ .node = node };
        },
        .relationship => blk: {
            const edge = try a.create(v.Relationship);
            const header = payload[0].list;
            edge.* = .{
                .identity = .{ .table = header[0].string, .key = header[1].string },
                .index = header[2].string,
                .type = header[3].string,
                .source = .{ .table = header[4].string, .key = header[5].string },
                .target = .{ .table = header[6].string, .key = header[7].string },
                .ownership = @fromBackingInt(@intCast(header[8].integer)),
                .producer = if (header[9] == .null) null else .{ .table = header[9].string, .key = header[10].string },
                .properties = try decodeEntries(a, payload[1..]),
            };
            break :blk .{ .relationship = edge };
        },
        .path => blk: {
            const nodes = try a.alloc(*const v.Node, @intCast(payload[0].integer));
            const edges = try a.alloc(*const v.Relationship, @intCast(payload[1].integer));
            for (nodes, 0..) |*node, i| node.* = payload[2 + i].node;
            for (edges, 0..) |*edge, i| edge.* = payload[2 + nodes.len + i].relationship;
            break :blk .{ .path = .{ .nodes = nodes, .relationships = edges } };
        },
        else => unreachable,
    };
}

test "flat collection carriers preserve every root kind across ownership transfers" {
    const Campaign = struct {
        fn run(a: A) !void {
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const alloc = arena.allocator();
            const node: v.Node = .{ .identity = .{ .table = "docs", .key = "a" }, .labels = &.{"D"}, .properties = &.{.{ .name = "score", .value = .{ .integer = 3 } }} };
            const edge: v.Relationship = .{ .identity = .{ .table = "links", .key = "e" }, .index = "links", .type = "R", .source = node.identity, .target = node.identity, .properties = node.properties };
            const values = [_]v.Value{ .null, .{ .boolean = true }, .{ .integer = 1 }, .{ .number = -0.0 }, .{ .string = "wide" }, .{ .list = &.{.null} }, .{ .map = node.properties }, .{ .node = &node }, .{ .relationship = &edge }, .{ .path = .{ .nodes = &.{ &node, &node }, .relationships = &.{&edge} } } };
            for (values) |value| {
                var cells: std.ArrayList(v.Value) = .empty;
                const encoded = try pack(alloc, &cells, value);
                const owned = try (v.Value{ .list = cells.items }).clone(alloc);
                const decoded = try unpack(alloc, owned.list, encoded);
                try std.testing.expectEqualStrings(try std.json.Stringify.valueAlloc(alloc, value, .{}), try std.json.Stringify.valueAlloc(alloc, decoded, .{}));
            }
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Campaign.run, .{});
}
