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

const std = @import("std");
pub const Identity = struct {
    pub const Context = struct {
        pub fn hash(_: @This(), identity: Identity) u64 {
            return std.hash.Wyhash.hash(std.hash.Wyhash.hash(0, identity.table), identity.key);
        }
        pub fn eql(_: @This(), left: Identity, right: Identity) bool {
            return left.eql(right);
        }
    };
    table: []const u8,
    key: []const u8,
    pub fn eql(a: Identity, b: Identity) bool {
        return std.mem.eql(u8, a.table, b.table) and std.mem.eql(u8, a.key, b.key);
    }
};
pub const Entry = struct { name: []const u8, value: Value };
pub const RelationshipIdentity = struct {
    identity: Identity,
    index: []const u8,
    pub const Context = struct {
        pub fn hash(_: @This(), identity: RelationshipIdentity) u64 {
            return std.hash.Wyhash.hash((Identity.Context{}).hash(identity.identity), identity.index);
        }
        pub fn eql(_: @This(), left: RelationshipIdentity, right: RelationshipIdentity) bool {
            return left.identity.eql(right.identity) and std.mem.eql(u8, left.index, right.index);
        }
    };
};
pub const EntityIdentity = union(enum) {
    node: Identity,
    relationship: RelationshipIdentity,
    pub fn eql(a: @This(), b: @This()) bool {
        return switch (a) {
            .node => |id| b == .node and id.eql(b.node),
            .relationship => |r| b == .relationship and r.identity.eql(b.relationship.identity) and std.mem.eql(u8, r.index, b.relationship.index),
        };
    }
    pub fn fromValue(value: Value) !@This() {
        return switch (value) {
            .node => |node| .{ .node = node.identity },
            .relationship => |r| .{ .relationship = .{ .identity = r.identity, .index = r.index } },
            else => error.CypherTypeMismatch,
        };
    }
};
pub const Node = struct { identity: Identity, labels: []const []const u8 = &.{}, properties: []const Entry = &.{} };
pub const Relationship = struct {
    // Identity includes index, physical source table, and durable edge key.
    identity: Identity,
    index: []const u8,
    type: []const u8,
    source: Identity,
    target: Identity,
    properties: []const Entry = &.{},
    ownership: enum { explicit, document, field, artifact } = .explicit,
    /// Producer authority and lifecycle are independent of traversal endpoints.
    producer: ?Identity = null,
    pub fn producingDocument(self: Relationship, a: std.mem.Allocator) !?Identity {
        if (self.producer) |identity| return identity;
        if (self.ownership == .explicit) return null;
        // Read pre-provenance wire values and retained fixture identities.
        const tuple = try std.json.parseFromSliceLeaky([]const []const u8, a, self.identity.key, .{});
        if (tuple.len != 5 or tuple[4].len == 0) return error.CypherInvalidRelationship;
        return .{ .table = self.identity.table, .key = tuple[4] };
    }
};
pub const Path = struct { nodes: []const *const Node, relationships: []const *const Relationship };
pub const Value = union(enum) {
    null,
    boolean: bool,
    integer: i64,
    number: f64,
    string: []const u8,
    node: *const Node,
    relationship: *const Relationship,
    path: Path,
    list: []const Value,
    map: []const Entry,

    pub fn property(self: Value, name: []const u8) Value {
        const entries = switch (self) {
            .node => |n| n.properties,
            .relationship => |r| r.properties,
            .map => |m| m,
            .null => return .null,
            else => return .null,
        };
        for (entries) |entry| if (std.mem.eql(u8, entry.name, name)) return entry.value;
        return .null;
    }
    pub const CloneError = std.mem.Allocator.Error || error{CypherValueDepthExceeded};
    pub fn clone(self: Value, a: std.mem.Allocator) CloneError!Value {
        return self.cloneDepth(a, 0);
    }
    fn cloneDepth(self: Value, a: std.mem.Allocator, depth: usize) CloneError!Value {
        if (depth > 64) return error.CypherValueDepthExceeded;
        return switch (self) {
            .null, .boolean, .integer, .number => self,
            .string => |s| .{ .string = try a.dupe(u8, s) },
            .list => |items| blk: {
                const output = try a.alloc(Value, items.len);
                for (items, output) |item, *out| out.* = try item.cloneDepth(a, depth + 1);
                break :blk .{ .list = output };
            },
            .map => |items| blk: {
                const output = try a.alloc(Entry, items.len);
                for (items, output) |item, *out| out.* = .{ .name = try a.dupe(u8, item.name), .value = try item.value.cloneDepth(a, depth + 1) };
                break :blk .{ .map = output };
            },
            .node => |n| blk: {
                const copy = try a.create(Node);
                const labels = try a.alloc([]const u8, n.labels.len);
                for (n.labels, labels) |label, *out| out.* = try a.dupe(u8, label);
                copy.* = .{ .identity = .{ .table = try a.dupe(u8, n.identity.table), .key = try a.dupe(u8, n.identity.key) }, .labels = labels, .properties = (try (Value{ .map = n.properties }).cloneDepth(a, depth + 1)).map };
                break :blk .{ .node = copy };
            },
            .relationship => |r| blk: {
                const copy = try a.create(Relationship);
                copy.* = r.*;
                copy.identity = .{ .table = try a.dupe(u8, r.identity.table), .key = try a.dupe(u8, r.identity.key) };
                copy.index = try a.dupe(u8, r.index);
                copy.type = try a.dupe(u8, r.type);
                copy.source = .{ .table = try a.dupe(u8, r.source.table), .key = try a.dupe(u8, r.source.key) };
                copy.target = .{ .table = try a.dupe(u8, r.target.table), .key = try a.dupe(u8, r.target.key) };
                if (r.producer) |producer| copy.producer = .{ .table = try a.dupe(u8, producer.table), .key = try a.dupe(u8, producer.key) };
                copy.properties = (try (Value{ .map = r.properties }).cloneDepth(a, depth + 1)).map;
                break :blk .{ .relationship = copy };
            },
            .path => |p| blk: {
                const nodes = try a.alloc(*const Node, p.nodes.len);
                const relationships = try a.alloc(*const Relationship, p.relationships.len);
                for (p.nodes, nodes) |node, *out| out.* = (try (Value{ .node = node }).cloneDepth(a, depth + 1)).node;
                for (p.relationships, relationships) |relationship, *out| out.* = (try (Value{ .relationship = relationship }).cloneDepth(a, depth + 1)).relationship;
                break :blk .{ .path = .{ .nodes = nodes, .relationships = relationships } };
            },
        };
    }
    pub fn truth(self: Value) !?bool {
        return switch (self) {
            .null => null,
            .boolean => |v| v,
            else => error.CypherTypeMismatch,
        };
    }
    pub fn numeric(self: Value) !f64 {
        return switch (self) {
            .integer => |v| @floatFromInt(v),
            .number => |v| v,
            else => error.CypherTypeMismatch,
        };
    }
    /// Expression equality keeps unknown values inside lists and maps. A
    /// decisive unequal member still makes the whole comparison false.
    pub fn equal(a: Value, b: Value) ?bool {
        if (a == .null or b == .null) return null;
        if (a == .list and b == .list) {
            if (a.list.len != b.list.len) return false;
            var unknown = false;
            for (a.list, b.list) |x, y| {
                if (equal(x, y)) |same| {
                    if (!same) return false;
                } else unknown = true;
            }
            return if (unknown) null else true;
        }
        if (a == .map and b == .map) {
            if (a.map.len != b.map.len) return false;
            var unknown = false;
            for (a.map) |entry| {
                var found = false;
                for (b.map) |other| if (std.mem.eql(u8, entry.name, other.name)) {
                    found = true;
                    if (equal(entry.value, other.value)) |same| {
                        if (!same) return false;
                    } else unknown = true;
                    break;
                };
                if (!found) return false;
            }
            return if (unknown) null else true;
        }
        return eql(a, b);
    }
    /// Grouping/DISTINCT equality, including NULL = NULL for grouping keys.
    pub fn eql(a: Value, b: Value) bool {
        if ((a == .integer or a == .number) and (b == .integer or b == .number)) return compareNumeric(a, b) == .eq;
        if (std.meta.activeTag(a) != std.meta.activeTag(b)) return false;
        return switch (a) {
            .null => true,
            .boolean => |v| v == b.boolean,
            .integer, .number => unreachable,
            .string => |s| std.mem.eql(u8, s, b.string),
            .node => |n| n.identity.eql(b.node.identity),
            .relationship => |r| r.identity.eql(b.relationship.identity) and std.mem.eql(u8, r.index, b.relationship.index),
            .list => |items| blk: {
                if (items.len != b.list.len) break :blk false;
                for (items, b.list) |x, y| if (!eql(x, y)) break :blk false;
                break :blk true;
            },
            .map => |items| blk: {
                if (items.len != b.map.len) break :blk false;
                for (items) |entry| {
                    var found = false;
                    for (b.map) |other| if (std.mem.eql(u8, entry.name, other.name)) {
                        found = eql(entry.value, other.value);
                        break;
                    };
                    if (!found) break :blk false;
                }
                break :blk true;
            },
            .path => |p| blk: {
                if (p.nodes.len != b.path.nodes.len or p.relationships.len != b.path.relationships.len) break :blk false;
                for (p.nodes, b.path.nodes) |x, y| if (!x.identity.eql(y.identity)) break :blk false;
                for (p.relationships, b.path.relationships) |x, y| if (!eql(.{ .relationship = x }, .{ .relationship = y })) break :blk false;
                break :blk true;
            },
        };
    }
    /// Predicate ordering is partial: NULL and incomparable types are unknown,
    /// including at the first undecidable member of a list or map.
    pub fn compare(a: Value, b: Value) ?std.math.Order {
        if (a == .null or b == .null or rank(a) != rank(b)) return null;
        if (a == .list) {
            for (0..@min(a.list.len, b.list.len)) |i| {
                const comparison = compare(a.list[i], b.list[i]) orelse return null;
                if (comparison != .eq) return comparison;
            }
            return std.math.order(a.list.len, b.list.len);
        }
        if (a == .map) {
            if (a.map.len != b.map.len) return std.math.order(a.map.len, b.map.len);
            var previous_a: ?[]const u8 = null;
            var previous_b: ?[]const u8 = null;
            for (0..a.map.len) |_| {
                const x = nextEntry(a.map, previous_a).?;
                const y = nextEntry(b.map, previous_b).?;
                const comparison = std.mem.order(u8, x.name, y.name);
                if (comparison != .eq) return comparison;
                previous_a = x.name;
                previous_b = y.name;
            }
            previous_a = null;
            previous_b = null;
            for (0..a.map.len) |_| {
                const x = nextEntry(a.map, previous_a).?;
                const y = nextEntry(b.map, previous_b).?;
                const comparison = compare(x.value, y.value) orelse return null;
                if (comparison != .eq) return comparison;
                previous_a = x.name;
                previous_b = y.name;
            }
            return .eq;
        }
        return order(a, b) catch unreachable;
    }
    /// ORDER BY uses Cypher's total type hierarchy, including NULL last.
    pub fn order(a: Value, b: Value) error{}!std.math.Order {
        const ar = rank(a);
        const br = rank(b);
        if (ar != br) return std.math.order(ar, br);
        return switch (a) {
            .null => .eq,
            .integer, .number => compareNumeric(a, b),
            .string => std.mem.order(u8, a.string, b.string),
            .boolean => std.math.order(@intFromBool(a.boolean), @intFromBool(b.boolean)),
            .node => identityOrder(a.node.identity, b.node.identity),
            .relationship => blk: {
                const first = identityOrder(a.relationship.identity, b.relationship.identity);
                if (first != .eq) break :blk first;
                break :blk std.mem.order(u8, a.relationship.index, b.relationship.index);
            },
            .list => blk: {
                for (0..@min(a.list.len, b.list.len)) |i| {
                    const comparison = try order(a.list[i], b.list[i]);
                    if (comparison != .eq) break :blk comparison;
                }
                break :blk std.math.order(a.list.len, b.list.len);
            },
            .path => blk: {
                for (0..@min(a.path.nodes.len, b.path.nodes.len)) |i| {
                    const node_order = identityOrder(a.path.nodes[i].identity, b.path.nodes[i].identity);
                    if (node_order != .eq) break :blk node_order;
                    if (i < @min(a.path.relationships.len, b.path.relationships.len)) {
                        const edge_order = try order(.{ .relationship = a.path.relationships[i] }, .{ .relationship = b.path.relationships[i] });
                        if (edge_order != .eq) break :blk edge_order;
                    }
                }
                break :blk std.math.order(a.path.nodes.len, b.path.nodes.len);
            },
            .map => blk: {
                if (a.map.len != b.map.len) break :blk std.math.order(a.map.len, b.map.len);
                var previous_a: ?[]const u8 = null;
                var previous_b: ?[]const u8 = null;
                // Compare all sorted keys before comparing their values.
                for (0..a.map.len) |_| {
                    const x = nextEntry(a.map, previous_a).?;
                    const y = nextEntry(b.map, previous_b).?;
                    const comparison = std.mem.order(u8, x.name, y.name);
                    if (comparison != .eq) break :blk comparison;
                    previous_a = x.name;
                    previous_b = y.name;
                }
                previous_a = null;
                previous_b = null;
                for (0..a.map.len) |_| {
                    const x = nextEntry(a.map, previous_a).?;
                    const y = nextEntry(b.map, previous_b).?;
                    const comparison = try order(x.value, y.value);
                    if (comparison != .eq) break :blk comparison;
                    previous_a = x.name;
                    previous_b = y.name;
                }
                break :blk .eq;
            },
        };
    }
    pub fn rank(value: Value) u8 {
        return switch (value) {
            .map => 0,
            .node => 1,
            .relationship => 2,
            .list => 3,
            .path => 4,
            .string => 5,
            .boolean => 6,
            .integer, .number => 7,
            .null => 8,
        };
    }
    fn identityOrder(a: Identity, b: Identity) std.math.Order {
        const table = std.mem.order(u8, a.table, b.table);
        return if (table == .eq) std.mem.order(u8, a.key, b.key) else table;
    }
    fn nextEntry(entries: []const Entry, previous: ?[]const u8) ?Entry {
        var selected: ?Entry = null;
        for (entries) |entry| {
            if (previous) |key| if (std.mem.order(u8, entry.name, key) != .gt) continue;
            if (selected == null or std.mem.order(u8, entry.name, selected.?.name) == .lt) selected = entry;
        }
        return selected;
    }
    pub const JsonError = std.mem.Allocator.Error || error{ CypherValueDepthExceeded, CypherNumericOverflow };
    pub fn fromJson(a: std.mem.Allocator, input: std.json.Value) JsonError!Value {
        return fromJsonDepth(a, input, 0);
    }
    fn fromJsonDepth(a: std.mem.Allocator, input: std.json.Value, depth: usize) JsonError!Value {
        if (depth > 64) return error.CypherValueDepthExceeded;
        return switch (input) {
            .null => .null,
            .bool => |v| .{ .boolean = v },
            .integer => |v| .{ .integer = v },
            .float => |v| if (std.math.isFinite(v)) .{ .number = v } else error.CypherNumericOverflow,
            .number_string => |s| blk: {
                if (std.mem.indexOfAny(u8, s, ".eE") == null) break :blk .{ .integer = std.fmt.parseInt(i64, s, 10) catch return error.CypherNumericOverflow };
                const number = std.fmt.parseFloat(f64, s) catch return error.CypherNumericOverflow;
                if (!std.math.isFinite(number)) return error.CypherNumericOverflow;
                break :blk .{ .number = number };
            },
            .string => |s| .{ .string = try a.dupe(u8, s) },
            .array => |v| blk: {
                const items = try a.alloc(Value, v.items.len);
                for (v.items, items) |x, *out| out.* = try fromJsonDepth(a, x, depth + 1);
                break :blk .{ .list = items };
            },
            .object => |v| blk: {
                const entries = try a.alloc(Entry, v.count());
                var it = v.iterator();
                var i: usize = 0;
                while (it.next()) |entry| : (i += 1) entries[i] = .{ .name = try a.dupe(u8, entry.key_ptr.*), .value = try fromJsonDepth(a, entry.value_ptr.*, depth + 1) };
                break :blk .{ .map = entries };
            },
        };
    }
    pub fn jsonStringify(self: Value, writer: anytype) !void {
        switch (self) {
            .null => try writer.write(@as(?bool, null)),
            .boolean => |v| try writer.write(v),
            .integer => |v| try writer.write(v),
            .number => |v| try writer.write(v),
            .string => |v| try writer.write(v),
            .list => |v| try writer.write(v),
            .map => |v| {
                try writer.beginObject();
                for (v) |entry| {
                    try writer.objectField(entry.name);
                    try writer.write(entry.value);
                }
                try writer.endObject();
            },
            .node => |n| try writer.write(.{ .@"$type" = "node", .identity = n.identity, .labels = n.labels, .properties = Value{ .map = n.properties } }),
            .relationship => |r| try writer.write(.{ .@"$type" = "relationship", .identity = r.identity, .index = r.index, .type = r.type, .source = r.source, .target = r.target, .ownership = r.ownership, .properties = Value{ .map = r.properties } }),
            .path => |p| {
                try writer.beginObject();
                try writer.objectField("$type");
                try writer.write("path");
                try writer.objectField("nodes");
                try writer.beginArray();
                for (p.nodes) |n| try writer.write(Value{ .node = n });
                try writer.endArray();
                try writer.objectField("relationships");
                try writer.beginArray();
                for (p.relationships) |r| try writer.write(Value{ .relationship = r });
                try writer.endArray();
                try writer.endObject();
            },
        }
    }
};
/// Entity properties accept finite data, including nested Antfly maps/lists,
/// but never graph handles. Validate before any mutation or JSON normalization:
/// assigning a handle to itself would otherwise introduce a serialization cycle.
pub fn validatePropertyValue(value: Value) error{ CypherInvalidPropertyValue, CypherValueDepthExceeded }!void {
    try validatePropertyDepth(value, 0);
}
fn validatePropertyDepth(value: Value, depth: usize) error{ CypherInvalidPropertyValue, CypherValueDepthExceeded }!void {
    if (depth > 64) return error.CypherValueDepthExceeded;
    switch (value) {
        .node, .relationship, .path => return error.CypherInvalidPropertyValue,
        .number => |number| if (!std.math.isFinite(number)) return error.CypherInvalidPropertyValue,
        .map => |entries| for (entries) |entry| try validatePropertyDepth(entry.value, depth + 1),
        .list => |items| for (items) |item| try validatePropertyDepth(item, depth + 1),
        else => {},
    }
}

/// An entity cannot have a null-valued property in Cypher. SQL NULL and
/// document JSON null both appear as absence at the entity boundary; nulls
/// inside a user map or list remain data and are preserved.
pub fn entityProperties(a: std.mem.Allocator, entries: []const Entry) ![]const Entry {
    var count: usize = 0;
    for (entries) |entry| if (entry.value != .null) {
        count += 1;
    };
    if (count == entries.len) return entries;
    const result = try a.alloc(Entry, count);
    var index: usize = 0;
    for (entries) |entry| if (entry.value != .null) {
        result[index] = entry;
        index += 1;
    };
    return result;
}

fn compareNumeric(a: Value, b: Value) std.math.Order {
    if (a == .integer and b == .integer) return std.math.order(a.integer, b.integer);
    if (a == .number and b == .number) return std.math.order(a.number, b.number);
    const integer = if (a == .integer) a.integer else b.integer;
    const number = if (a == .number) a.number else b.number;
    const order_value: std.math.Order = blk: {
        if (number >= 9223372036854775808.0) break :blk .lt;
        if (number < -9223372036854775808.0) break :blk .gt;
        const truncated: i64 = @intFromFloat(number);
        const integer_order = std.math.order(integer, truncated);
        if (integer_order != .eq) break :blk integer_order;
        break :blk std.math.order(@as(f64, @floatFromInt(integer)), number);
    };
    return if (a == .integer) order_value else order_value.invert();
}

test "value cloning bounds stack depth independently of query execution" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var nested: Value = .{ .integer = 1 };
    for (0..10000) |_| {
        const items = try a.alloc(Value, 1);
        items[0] = nested;
        nested = .{ .list = items };
    }
    try std.testing.expectError(error.CypherValueDepthExceeded, nested.clone(a));
}
