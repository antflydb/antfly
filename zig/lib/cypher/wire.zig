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

//! Versioned graph transport values. User maps never share the discriminator
//! namespace; INTEGER uses decimal text so JavaScript preserves every bit.
const std = @import("std");
const v = @import("value.zig");
pub const version = 1;
pub const Value = struct {
    value: v.Value,
    pub fn jsonStringify(self: Value, writer: anytype) !void {
        try writer.beginObject();
        try writer.objectField("type");
        try writer.write(@tagName(self.value));
        try writer.objectField("value");
        switch (self.value) {
            .null => try writer.write(@as(?bool, null)),
            .boolean => |x| try writer.write(x),
            .integer => |x| {
                var buffer: [32]u8 = undefined;
                try writer.write(std.fmt.bufPrint(&buffer, "{d}", .{x}) catch unreachable);
            },
            .number => |x| try writer.write(x),
            .string => |x| try writer.write(x),
            .list => |items| {
                try writer.beginArray();
                for (items) |item| try writer.write(Value{ .value = item });
                try writer.endArray();
            },
            .map => |entries| {
                try writer.beginObject();
                for (entries) |entry| {
                    try writer.objectField(entry.name);
                    try writer.write(Value{ .value = entry.value });
                }
                try writer.endObject();
            },
            .node => |node| try writer.write(.{ .identity = node.identity, .labels = node.labels, .properties = Value{ .value = .{ .map = node.properties } } }),
            .relationship => |r| try writer.write(.{ .identity = r.identity, .index = r.index, .type = r.type, .source = r.source, .target = r.target, .ownership = r.ownership, .properties = Value{ .value = .{ .map = r.properties } } }),
            .path => |p| {
                try writer.beginObject();
                try writer.objectField("nodes");
                try writer.beginArray();
                for (p.nodes) |n| try writer.write(Value{ .value = .{ .node = n } });
                try writer.endArray();
                try writer.objectField("relationships");
                try writer.beginArray();
                for (p.relationships) |r| try writer.write(Value{ .value = .{ .relationship = r } });
                try writer.endArray();
                try writer.endObject();
            },
        }
        try writer.endObject();
    }
};

/// Parameter decoding deliberately accepts only data values. Entity handles
/// cannot be forged by a public caller; MATCH resolves authorized identities.
pub fn parameter(a: std.mem.Allocator, input: std.json.Value) !v.Value {
    return decode(a, input, 0, false);
}
pub fn transport(a: std.mem.Allocator, input: std.json.Value) !v.Value {
    return decode(a, input, 0, true);
}
fn decode(a: std.mem.Allocator, input: std.json.Value, depth: usize, entities: bool) anyerror!v.Value {
    if (depth > 64) return error.CypherValueDepthExceeded;
    if (input != .object or input.object.count() != 2) return error.InvalidCypherWireValue;
    const kind = input.object.get("type") orelse return error.InvalidCypherWireValue;
    const value = input.object.get("value") orelse return error.InvalidCypherWireValue;
    if (kind != .string) return error.InvalidCypherWireValue;
    if (std.mem.eql(u8, kind.string, "null") and value == .null) return .null;
    if (std.mem.eql(u8, kind.string, "boolean") and value == .bool) return .{ .boolean = value.bool };
    if (std.mem.eql(u8, kind.string, "string") and value == .string) return .{ .string = try a.dupe(u8, value.string) };
    if (std.mem.eql(u8, kind.string, "integer") and value == .string) return .{ .integer = std.fmt.parseInt(i64, value.string, 10) catch return error.CypherNumericOverflow };
    if (std.mem.eql(u8, kind.string, "number")) {
        const decoded = try v.Value.fromJson(a, value);
        if (decoded != .integer and decoded != .number) return error.InvalidCypherWireValue;
        return .{ .number = try decoded.numeric() };
    }
    if (std.mem.eql(u8, kind.string, "list") and value == .array) {
        const items = try a.alloc(v.Value, value.array.items.len);
        for (value.array.items, items) |item, *out| out.* = try decode(a, item, depth + 1, entities);
        return .{ .list = items };
    }
    if (std.mem.eql(u8, kind.string, "map") and value == .object) {
        const entries = try a.alloc(v.Entry, value.object.count());
        var iterator = value.object.iterator();
        var i: usize = 0;
        while (iterator.next()) |entry| : (i += 1) entries[i] = .{ .name = try a.dupe(u8, entry.key_ptr.*), .value = try decode(a, entry.value_ptr.*, depth + 1, entities) };
        return .{ .map = entries };
    }
    if (entities and std.mem.eql(u8, kind.string, "node")) {
        const Input = struct { identity: v.Identity, labels: []const []const u8, properties: std.json.Value };
        const node = try std.json.parseFromValueLeaky(Input, a, value, .{ .allocate = .alloc_always });
        const properties = try decode(a, node.properties, depth + 1, true);
        if (properties != .map or node.identity.table.len == 0 or node.identity.key.len == 0) return error.InvalidCypherWireValue;
        const result = try a.create(v.Node);
        result.* = .{ .identity = node.identity, .labels = node.labels, .properties = properties.map };
        return .{ .node = result };
    }
    if (entities and std.mem.eql(u8, kind.string, "relationship")) {
        const Input = struct { identity: v.Identity, index: []const u8, type: []const u8, source: v.Identity, target: v.Identity, ownership: @FieldType(v.Relationship, "ownership"), properties: std.json.Value };
        const relation = try std.json.parseFromValueLeaky(Input, a, value, .{ .allocate = .alloc_always });
        const properties = try decode(a, relation.properties, depth + 1, true);
        if (properties != .map or relation.identity.table.len == 0 or relation.identity.key.len == 0 or relation.index.len == 0 or relation.type.len == 0) return error.InvalidCypherWireValue;
        const result = try a.create(v.Relationship);
        result.* = .{ .identity = relation.identity, .index = relation.index, .type = relation.type, .source = relation.source, .target = relation.target, .ownership = relation.ownership, .properties = properties.map };
        return .{ .relationship = result };
    }
    if (entities and std.mem.eql(u8, kind.string, "path")) {
        const Input = struct { nodes: []const std.json.Value, relationships: []const std.json.Value };
        const path = try std.json.parseFromValueLeaky(Input, a, value, .{});
        if (path.nodes.len != path.relationships.len + 1) return error.InvalidCypherWireValue;
        const nodes = try a.alloc(*const v.Node, path.nodes.len);
        const relationships = try a.alloc(*const v.Relationship, path.relationships.len);
        for (path.nodes, nodes) |item, *out| {
            const node = try decode(a, item, depth + 1, true);
            if (node != .node) return error.InvalidCypherWireValue;
            out.* = node.node;
        }
        for (path.relationships, relationships, 0..) |item, *out, i| {
            const relationship = try decode(a, item, depth + 1, true);
            if (relationship != .relationship) return error.InvalidCypherWireValue;
            const r = relationship.relationship;
            if (!(r.source.eql(nodes[i].identity) and r.target.eql(nodes[i + 1].identity)) and !(r.target.eql(nodes[i].identity) and r.source.eql(nodes[i + 1].identity))) return error.InvalidCypherWireValue;
            out.* = r;
        }
        return .{ .path = .{ .nodes = nodes, .relationships = relationships } };
    }
    return error.InvalidCypherWireValue;
}

test "graph wire preserves signed integers and map discriminator keys" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const original: v.Value = .{ .map = &.{ .{ .name = "type", .value = .{ .string = "node" } }, .{ .name = "$type", .value = .{ .integer = std.math.minInt(i64) } }, .{ .name = "value", .value = .{ .list = &.{.{ .integer = std.math.maxInt(i64) }} } } } };
    const json = try std.json.Stringify.valueAlloc(a, Value{ .value = original }, .{});
    const parsed = try std.json.parseFromSliceLeaky(std.json.Value, a, json, .{});
    try std.testing.expect(v.Value.eql(original, try parameter(a, parsed)));
    const forged = try std.json.parseFromSliceLeaky(std.json.Value, a, "{\"type\":\"node\",\"value\":{}}", .{});
    try std.testing.expectError(error.InvalidCypherWireValue, parameter(a, forged));
}
