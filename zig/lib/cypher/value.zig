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
    pub const Context = struct {
        pub fn hash(_: @This(), identity: EntityIdentity) u64 {
            return switch (identity) {
                .node => |id| (Identity.Context{}).hash(id),
                .relationship => |id| (RelationshipIdentity.Context{}).hash(id) ^ 0x9e3779b97f4a7c15,
            };
        }
        pub fn eql(_: @This(), left: EntityIdentity, right: EntityIdentity) bool {
            return left.eql(right);
        }
    };
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
/// Physical attributes retained across property-only mutations. These belong
/// to the trusted storage cut and are omitted from the public graph wire value.
pub const NativeRelationshipAttributes = struct {
    weight: f64 = 1,
    created_at: u64 = 0,
    updated_at: u64 = 0,
    pub fn jsonStringify(self: @This(), writer: anytype) !void {
        var created: [20]u8 = undefined;
        var updated: [20]u8 = undefined;
        try writer.write(.{ .weight = self.weight, .created_at = (std.fmt.bufPrint(&created, "{d}", .{self.created_at}) catch unreachable), .updated_at = (std.fmt.bufPrint(&updated, "{d}", .{self.updated_at}) catch unreachable) });
    }
};
pub const Relationship = struct {
    native: NativeRelationshipAttributes = .{},
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
        return self.cloneOwned(a, depth, null);
    }
    fn cloneOwned(self: Value, a: std.mem.Allocator, depth: usize, context: ?*CloneContext) CloneError!Value {
        if (depth > 64) return error.CypherValueDepthExceeded;
        // Equality keys prune entities and do not need an allocation-identity
        // memo per input. Keep that path free of scratch allocation churn.
        const key = if (context != null and context.?.share_allocations) CloneContext.key(self) else null;
        if (context) |ctx| {
            if (key) |k| if (ctx.memo.get(k)) |owned| {
                if (depth + owned.height > 64) return error.CypherValueDepthExceeded;
                if (depth != 0) ctx.heights[depth - 1] = @max(ctx.heights[depth - 1], owned.height + 1);
                return owned.value;
            };
            ctx.heights[depth] = 0;
        }
        const result: Value = switch (self) {
            .null, .boolean, .integer, .number => self,
            .string => |s| .{ .string = if (context) |ctx| try ctx.cloneBytes(s) else try a.dupe(u8, s) },
            .list => |items| blk: {
                const output = try a.alloc(Value, items.len);
                for (items, output) |item, *out| out.* = try item.cloneOwned(a, depth + 1, context);
                break :blk .{ .list = output };
            },
            .map => |items| blk: {
                const output = try a.alloc(Entry, items.len);
                for (items, output) |item, *out| out.* = .{ .name = try a.dupe(u8, item.name), .value = try item.value.cloneOwned(a, depth + 1, context) };
                break :blk .{ .map = output };
            },
            .node => |n| blk: {
                const copy = try a.create(Node);
                const identity_only = if (context) |ctx| ctx.identity_only else false;
                const labels = try a.alloc([]const u8, if (identity_only) 0 else n.labels.len);
                for (n.labels[0..labels.len], labels) |label, *out| out.* = try a.dupe(u8, label);
                copy.* = .{ .identity = .{ .table = try a.dupe(u8, n.identity.table), .key = try a.dupe(u8, n.identity.key) }, .labels = labels, .properties = if (identity_only) &.{} else (try (Value{ .map = n.properties }).cloneOwned(a, depth + 1, context)).map };
                break :blk .{ .node = copy };
            },
            .relationship => |r| blk: {
                const copy = try a.create(Relationship);
                copy.* = r.*;
                copy.identity = .{ .table = try a.dupe(u8, r.identity.table), .key = try a.dupe(u8, r.identity.key) };
                copy.index = try a.dupe(u8, r.index);
                if (context) |ctx| if (ctx.identity_only) {
                    copy.type = "";
                    copy.source = .{ .table = "", .key = "" };
                    copy.target = copy.source;
                    copy.producer = null;
                    copy.properties = &.{};
                    break :blk .{ .relationship = copy };
                };
                copy.type = try a.dupe(u8, r.type);
                copy.source = .{ .table = try a.dupe(u8, r.source.table), .key = try a.dupe(u8, r.source.key) };
                copy.target = .{ .table = try a.dupe(u8, r.target.table), .key = try a.dupe(u8, r.target.key) };
                if (r.producer) |producer| copy.producer = .{ .table = try a.dupe(u8, producer.table), .key = try a.dupe(u8, producer.key) };
                copy.properties = (try (Value{ .map = r.properties }).cloneOwned(a, depth + 1, context)).map;
                break :blk .{ .relationship = copy };
            },
            .path => |p| blk: {
                const nodes = try a.alloc(*const Node, p.nodes.len);
                const relationships = try a.alloc(*const Relationship, p.relationships.len);
                for (p.nodes, nodes) |node, *out| out.* = (try (Value{ .node = node }).cloneOwned(a, depth + 1, context)).node;
                for (p.relationships, relationships) |relationship, *out| out.* = (try (Value{ .relationship = relationship }).cloneOwned(a, depth + 1, context)).relationship;
                break :blk .{ .path = .{ .nodes = nodes, .relationships = relationships } };
            },
        };
        if (context) |ctx| {
            const height = ctx.heights[depth];
            if (key) |k| try ctx.memo.put(ctx.scratch, k, .{ .value = result, .height = height });
            if (depth != 0) ctx.heights[depth - 1] = @max(ctx.heights[depth - 1], height + 1);
        }
        return result;
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

pub const IdentitySet = std.HashMapUnmanaged(Identity, void, Identity.Context, std.hash_map.default_max_load_percentage);
pub fn EntityMap(comptime T: type) type {
    return std.HashMapUnmanaged(EntityIdentity, T, EntityIdentity.Context, std.hash_map.default_max_load_percentage);
}
pub const EntitySet = EntityMap(void);

/// Allocation-identity sharing within one immutable ownership boundary. Memo
/// storage is scratch and must not outlive (or be reused across) source values.
/// Cached subtree heights preserve the recursion limit even when an allocation
/// is shared between different nesting depths.
pub const CloneContext = struct {
    const Key = struct { tag: std.meta.Tag(Value), pointer: usize, length: usize };
    a: std.mem.Allocator,
    scratch: std.mem.Allocator,
    identity_only: bool = false,
    share_allocations: bool = true,
    byte_pool: ?*?*BytePool = null,
    byte_allocator: ?std.mem.Allocator = null,
    memo: std.AutoHashMapUnmanaged(Key, struct { value: Value, height: usize }) = .empty,
    heights: [65]usize = @splat(0),
    fn key(value: Value) ?Key {
        return switch (value) {
            .string => |x| .{ .tag = .string, .pointer = @intFromPtr(x.ptr), .length = x.len },
            .list => |x| .{ .tag = .list, .pointer = @intFromPtr(x.ptr), .length = x.len },
            .map => |x| .{ .tag = .map, .pointer = @intFromPtr(x.ptr), .length = x.len },
            .node => |x| .{ .tag = .node, .pointer = @intFromPtr(x), .length = 0 },
            .relationship => |x| .{ .tag = .relationship, .pointer = @intFromPtr(x), .length = 0 },
            else => null,
        };
    }
    fn cloneBytes(self: *@This(), bytes: []const u8) std.mem.Allocator.Error![]const u8 {
        // Small values stay with their metadata, avoiding a pool per tiny group.
        if (bytes.len >= 256) if (self.byte_pool) |slot| {
            if (slot.* == null) {
                const pool = try self.a.create(BytePool);
                pool.* = .{ .backing = self.byte_allocator.? };
                slot.* = pool;
            }
            return slot.*.?.dupe(self.a, bytes);
        };
        return self.a.dupe(u8, bytes);
    }
    pub fn clone(self: *@This(), value: Value) Value.CloneError!Value {
        return value.cloneOwned(self.a, 0, self);
    }
};
/// Equality-only ownership. Entity properties and physical attributes do not
/// participate in grouping equality, including inside containers and paths.
pub fn cloneEquality(value: Value, a: std.mem.Allocator) Value.CloneError!Value {
    var context: CloneContext = .{ .a = a, .scratch = a, .identity_only = true, .share_allocations = false };
    return context.clone(value);
}

/// Byte slabs keep large membership payloads out of geometrically growing
/// metadata arenas. Slab growth stops at 64 KiB; oversized strings own an
/// exact allocation without disturbing the current slab.
pub const BytePool = struct {
    backing: std.mem.Allocator,
    blocks: std.ArrayList([]u8) = .empty,
    remaining: []u8 = &.{},
    next_size: usize = 8192,
    fn block(self: *@This(), metadata: std.mem.Allocator, size: usize) ![]u8 {
        const bytes = try self.backing.alloc(u8, size);
        errdefer self.backing.free(bytes);
        try self.blocks.append(metadata, bytes);
        return bytes;
    }
    fn dupe(self: *@This(), metadata: std.mem.Allocator, bytes: []const u8) ![]const u8 {
        if (bytes.len > 65536) {
            const output = try self.block(metadata, bytes.len);
            @memcpy(output, bytes);
            return output;
        }
        if (self.remaining.len < bytes.len) {
            self.remaining = try self.block(metadata, @max(bytes.len, self.next_size));
            self.next_size = @min(65536, self.remaining.len * 2);
        }
        const output = self.remaining[0..bytes.len];
        @memcpy(output, bytes);
        self.remaining = self.remaining[bytes.len..];
        return output;
    }
    pub fn deinit(self: *@This()) void {
        for (self.blocks.items) |bytes| self.backing.free(bytes);
    }
};
/// Membership metadata owns the pool descriptor; its owner must release the
/// pool's backing slabs before destroying that metadata arena.
pub fn cloneMembership(value: Value, a: std.mem.Allocator, backing: std.mem.Allocator, pool: *?*BytePool, retain_payload: bool) Value.CloneError!Value {
    // Containers can share large subtrees. Preserve that sharing in retained
    // keys without retaining source allocation addresses in the membership.
    var scratch = std.heap.ArenaAllocator.init(backing);
    defer scratch.deinit();
    const sharing = value == .list or value == .map or value == .path or (retain_payload and (value == .node or value == .relationship));
    var context: CloneContext = .{ .a = a, .scratch = scratch.allocator(), .identity_only = !retain_payload, .share_allocations = sharing, .byte_pool = pool, .byte_allocator = backing };
    return context.clone(value);
}

test "owned cloning shares allocation identity without merging representations or depth checks" {
    var owned_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer owned_arena.deinit();
    var scratch = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer scratch.deinit();
    const a = owned_arena.allocator();
    var context: CloneContext = .{ .a = a, .scratch = scratch.allocator() };
    const original: Value = .{ .list = &.{ .{ .string = "payload" }, .{ .number = 1.0 } } };
    const first = try context.clone(original);
    const second = try context.clone(original);
    try std.testing.expect(first.list.ptr == second.list.ptr);
    try std.testing.expect(first.list.ptr != original.list.ptr);
    try std.testing.expect(first.list[1] == .number);
    const string = try context.clone(original.list[0]);
    try std.testing.expect(string.string.ptr == first.list[0].string.ptr);
    const node: Node = .{ .identity = .{ .table = "a", .key = "key" }, .properties = &.{.{ .name = "x", .value = original }} };
    const changed: Node = .{ .identity = node.identity, .properties = &.{.{ .name = "x", .value = .{ .integer = 2 } }} };
    const old = try context.clone(.{ .node = &node });
    const new = try context.clone(.{ .node = &changed });
    try std.testing.expect(old.node != new.node);
    try std.testing.expect(old.property("x") == .list);
    try std.testing.expectEqual(@as(i64, 2), new.property("x").integer);
    var nested = original;
    for (0..64) |_| {
        const item = try a.create(Value);
        item.* = nested;
        nested = .{ .list = @as(*[1]Value, @ptrCast(item)) };
    }
    try std.testing.expectError(error.CypherValueDepthExceeded, context.clone(nested));
}

test "equality ownership strips entity payloads recursively and keeps full identities" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const node: Node = .{ .identity = .{ .table = "a", .key = "key" }, .labels = &.{"L"}, .properties = &.{.{ .name = "x", .value = .{ .string = "payload" } }} };
    const edge: Relationship = .{ .identity = node.identity, .index = "index", .type = "E", .source = node.identity, .target = node.identity, .properties = node.properties };
    const original: Value = .{ .map = &.{.{ .name = "nested", .value = .{ .list = &.{ .{ .node = &node }, .{ .relationship = &edge }, .{ .path = .{ .nodes = &.{&node}, .relationships = &.{&edge} } } } } }} };
    const key = try cloneEquality(original, a);
    try std.testing.expect(Value.eql(original, key));
    const cells = key.map[0].value.list;
    try std.testing.expectEqual(@as(usize, 0), cells[0].node.properties.len);
    try std.testing.expectEqual(@as(usize, 0), cells[1].relationship.properties.len);
    try std.testing.expectEqual(@as(usize, 0), cells[2].path.nodes[0].properties.len);
    const other: Node = .{ .identity = .{ .table = "b", .key = "key" } };
    try std.testing.expect(!Value.eql(cells[0], .{ .node = &other }));
    var other_edge = edge;
    other_edge.index = "other";
    try std.testing.expect(!Value.eql(cells[1], .{ .relationship = &other_edge }));
}

fn membershipBytesScenario(allocator: std.mem.Allocator) !void {
    // Arena remapping depends on adjacent allocations. Inject failures against
    // a deterministic allocation schedule, as the runtime campaigns do.
    var vtable = allocator.vtable.*;
    vtable.resize = std.mem.Allocator.noResize;
    vtable.remap = std.mem.Allocator.noRemap;
    const backing: std.mem.Allocator = .{ .ptr = allocator.ptr, .vtable = &vtable };
    var metadata = std.heap.ArenaAllocator.init(backing);
    defer metadata.deinit();
    const a = metadata.allocator();
    var pool: ?*BytePool = null;
    defer if (pool) |bytes| bytes.deinit();
    const payload: [4092]u8 = @splat('x');
    var first: Value = .null;
    for (0..32) |i| {
        const copy = try cloneMembership(.{ .list = &.{.{ .string = &payload }} }, a, backing, &pool, false);
        if (i == 0) first = copy;
        try std.testing.expectEqualStrings(&payload, copy.list[0].string);
    }
    const tail = pool.?.remaining;
    const oversized: [70000]u8 = @splat('y');
    const large = try cloneMembership(.{ .string = &oversized }, a, backing, &pool, true);
    try std.testing.expectEqualStrings(&oversized, large.string);
    try std.testing.expectEqual(tail.ptr, pool.?.remaining.ptr);
    try std.testing.expectEqual(tail.len, pool.?.remaining.len);
    try std.testing.expectEqualStrings(&payload, first.list[0].string);
    var retained: usize = 0;
    for (pool.?.blocks.items) |block_| retained += block_.len;
    try std.testing.expect(retained <= 32 * payload.len + oversized.len + 65536);
}
test "membership byte slabs bound retention and release every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, membershipBytesScenario, .{});
}
