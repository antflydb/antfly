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
const values = @import("value.zig");
pub const Label = struct {
    name: []const u8,
    table: []const u8,
    /// Authoritative physical incarnation; zero is reserved for host-defined
    /// graphs whose authority provider owns identity validation.
    table_id: u64 = 0,
    logical_table: []const u8 = "",
    database: []const u8 = "default",
    namespace: []const u8 = "public",
    /// Equality predicates defining a logical label over stored fields.
    predicate: []const values.Entry = &.{},
};
pub const IndexIdentity = struct { incarnation: u64, config_hash: u64 };
pub const Relationship = struct {
    name: []const u8,
    index: []const u8,
    source_label: []const u8,
    target_label: []const u8,
    stored_type: []const u8,
    identity: ?IndexIdentity = null,
};
pub const Definition = struct {
    labels: []const struct { name: []const u8, table: std.json.Value, where: ?std.json.Value = null },
    relationships: []const struct { name: []const u8, index: []const u8, source_label: []const u8, target_label: []const u8, stored_type: []const u8 } = &.{},
};

/// Caller-owned normalized definition. Parsing rejects unknown wire fields;
/// identity and revision come exclusively from the authoritative catalog.
pub fn parseDefinition(a: std.mem.Allocator, name: []const u8, incarnation: u64, revision: u64, json: []const u8) !Graph {
    const wire = try std.json.parseFromSliceLeaky(Definition, a, json, .{ .allocate = .alloc_always, .parse_numbers = false });
    const labels = try a.alloc(Label, wire.labels.len);
    for (wire.labels, labels) |label_value, *out| {
        const predicate = if (label_value.where) |where| try values.Value.fromJson(a, where) else values.Value{ .map = &.{} };
        if (predicate != .map) return error.InvalidGraphDefinition;
        const Target = struct { database: []const u8 = "default", namespace: []const u8 = "public", table: []const u8 };
        const target: Target = switch (label_value.table) {
            .string => |table| .{ .table = table },
            .object => try std.json.parseFromValueLeaky(Target, a, label_value.table, .{ .allocate = .alloc_always }),
            else => return error.InvalidGraphDefinition,
        };
        out.* = .{ .name = label_value.name, .table = target.table, .logical_table = target.table, .database = target.database, .namespace = target.namespace, .predicate = predicate.map };
    }
    const relationships = try a.alloc(Relationship, wire.relationships.len);
    for (wire.relationships, relationships) |input, *out| out.* = .{ .name = input.name, .index = input.index, .source_label = input.source_label, .target_label = input.target_label, .stored_type = input.stored_type };
    const graph: Graph = .{ .name = try a.dupe(u8, name), .incarnation = incarnation, .revision = revision, .labels = labels, .relationships = relationships };
    try graph.validate();
    return graph;
}
pub const Graph = struct {
    name: []const u8,
    incarnation: u64,
    revision: u64,
    labels: []const Label,
    relationships: []const Relationship,
    pub fn label(self: Graph, name: []const u8) ?Label {
        for (self.labels) |l| if (std.mem.eql(u8, l.name, name)) return l;
        return null;
    }
    pub fn relationship(self: Graph, name: []const u8) ?Relationship {
        for (self.relationships) |r| if (std.mem.eql(u8, r.name, name)) return r;
        return null;
    }
    pub fn validate(self: Graph) !void {
        if (self.name.len == 0 or self.incarnation == 0 or self.revision == 0 or self.labels.len == 0 or self.labels.len > 64 or self.relationships.len > 64) return error.InvalidGraphDefinition;
        for (self.labels, 0..) |l, i| {
            if (l.name.len == 0 or l.table.len == 0 or l.database.len == 0 or l.namespace.len == 0) return error.InvalidGraphDefinition;
            for (self.labels[0..i]) |other| if (std.mem.eql(u8, l.name, other.name)) return error.DuplicateGraphLabel;
        }
        for (self.relationships, 0..) |r, i| {
            if (r.name.len == 0 or r.index.len == 0 or r.stored_type.len == 0 or self.label(r.source_label) == null or self.label(r.target_label) == null) return error.InvalidGraphDefinition;
            for (self.relationships[0..i]) |other| if (std.mem.eql(u8, r.name, other.name)) return error.DuplicateGraphRelationship;
            // A Cypher relationship has one type. Reinterpreting one physical
            // edge under overlapping mappings would duplicate untyped MATCH
            // results and give equal relationship IDs different types.
            const source = self.label(r.source_label).?;
            for (self.relationships[0..i]) |other| {
                const previous = self.label(other.source_label).?;
                if (std.mem.eql(u8, r.index, other.index) and std.mem.eql(u8, r.stored_type, other.stored_type) and
                    std.mem.eql(u8, source.table, previous.table) and std.mem.eql(u8, source.database, previous.database) and std.mem.eql(u8, source.namespace, previous.namespace)) return error.AmbiguousGraphRelationship;
            }
            if (r.identity) |identity| if (identity.incarnation == 0 or identity.config_hash == 0) return error.InvalidGraphDefinition;
        }
    }
    pub fn admits(self: Graph, node: *const values.Node, label_name: []const u8) bool {
        const l = self.label(label_name) orelse return false;
        if (!std.mem.eql(u8, node.identity.table, l.table)) return false;
        for (l.predicate) |entry| if (!(values.Value.equal((values.Value{ .node = node }).property(entry.name), entry.value) orelse false)) return false;
        return true;
    }
};

/// Durable graph records are normalized data. Persistence adapters atomically
/// compare revisions and authorize graph resources and their underlying tables.
pub const Store = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    pub const VTable = struct {
        get: *const fn (*anyopaque, std.mem.Allocator, []const u8) anyerror!?Graph,
        list: *const fn (*anyopaque, std.mem.Allocator) anyerror![]const Graph,
        put: *const fn (*anyopaque, Graph, ?u64) anyerror!void,
        drop: *const fn (*anyopaque, []const u8, u64) anyerror!void,
    };
    pub fn put(self: Store, graph: Graph, expected_revision: ?u64) !void {
        try graph.validate();
        try self.vtable.put(self.ptr, graph, expected_revision);
    }
};
