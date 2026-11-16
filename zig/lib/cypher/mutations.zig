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

//! Shared immutable postimage planning for hosted and embedded coordinators.
const std = @import("std");
const root = @import("root.zig");
const A = std.mem.Allocator;
const v = root.value;
pub const Write = struct { key: []const u8, value: []const u8 };
pub const Edge = struct { weight: f64 = 1, created_at: u64 = 0, updated_at: u64 = 0, edge_id: []const u8, index_name: []const u8, source: []const u8, target: []const u8, edge_type: []const u8, metadata_json: []const u8 = "" };
pub const Plan = struct {
    table: []const u8,
    writes: std.ArrayList(Write) = .empty,
    deletes: std.ArrayList([]const u8) = .empty,
    edges: std.ArrayList(Edge) = .empty,
    removed_edges: std.ArrayList(Edge) = .empty,
};
fn plan(plans: []Plan, table: []const u8) !*Plan {
    for (plans) |*entry| if (std.mem.eql(u8, entry.table, table)) return entry;
    return error.UndefinedGraphTable;
}
pub fn document(a: A, node: *const v.Node) ![]const u8 {
    for (node.properties) |entry| try v.validatePropertyValue(entry.value);
    return std.json.Stringify.valueAlloc(a, Document{ .properties = node.properties }, .{});
}
const Document = struct {
    properties: []const v.Entry,
    pub fn jsonStringify(self: @This(), writer: anytype) !void {
        try writer.beginObject();
        for (self.properties) |entry| {
            if (entry.value == .null) continue;
            try writer.objectField(entry.name);
            try writer.write(entry.value);
        }
        try writer.endObject();
    }
};
fn edgeId(a: A, relationship: *const v.Relationship) ![]const u8 {
    if (std.mem.startsWith(u8, relationship.identity.key, "cypher:")) return relationship.identity.key;
    const tuple = try std.json.parseFromSliceLeaky([]const []const u8, a, relationship.identity.key, .{});
    if (tuple.len != 5 or !std.mem.eql(u8, tuple[0], relationship.source.key) or !std.mem.eql(u8, tuple[1], relationship.target.key) or tuple[4].len != 0) return error.CypherDerivedRelationshipMutation;
    return tuple[3];
}
pub fn build(a: A, graph: root.catalog.Graph, overlay: *root.transaction.Overlay) ![]Plan {
    var plans: std.ArrayList(Plan) = .empty;
    for (graph.labels) |label| {
        const exists = for (plans.items) |entry| {
            if (std.mem.eql(u8, entry.table, label.table)) break true;
        } else false;
        if (!exists) try plans.append(a, .{ .table = label.table });
    }
    for (overlay.nodes.items) |node| {
        try overlay.charge(1);
        if (!node.dirty) continue;
        const entry = try plan(plans.items, node.value.identity.table);
        if (node.deleted) {
            if (!node.created) try entry.deletes.append(a, node.value.identity.key);
        } else {
            var membership = overlay.valueWork(a);
            for (node.value.labels) |label| if (!try graph.admits(node.value, label, &membership)) return error.CypherLabelPredicateMismatch;
            try overlay.chargeValue(.{ .map = node.value.properties });
            try entry.writes.append(a, .{ .key = node.value.identity.key, .value = try document(a, node.value) });
        }
    }
    for (overlay.relationships.items) |item| {
        try overlay.charge(1);
        const relationship = item.value;
        if (relationship.ownership != .explicit or !item.dirty) continue;
        const entry = try plan(plans.items, relationship.source.table);
        const mapping = graph.relationship(relationship.type) orelse return error.UndefinedGraphRelationship;
        if (!std.mem.eql(u8, relationship.source.table, graph.label(mapping.source_label).?.table) or !std.mem.eql(u8, relationship.target.table, graph.label(mapping.target_label).?.table)) return error.CypherInvalidRelationship;
        const weight = (v.Value{ .relationship = relationship }).property("weight");
        const numeric_weight = if (weight == .integer or weight == .number) try weight.numeric() else relationship.native.weight;
        // Arbitrary Cypher property data must not narrow the native storage
        // domain. Only a valid native cost overrides the captured weight.
        var edge: Edge = .{ .weight = if (numeric_weight >= 0) numeric_weight else relationship.native.weight, .created_at = relationship.native.created_at, .updated_at = relationship.native.updated_at, .edge_id = try edgeId(a, relationship), .index_name = relationship.index, .source = relationship.source.key, .target = relationship.target.key, .edge_type = mapping.stored_type };
        if (item.deleted) {
            if (item.created) continue;
            try entry.removed_edges.append(a, edge);
        } else {
            // Never encode an active edge whose endpoint was removed in this
            // statement, even if a delete already supplies the source intent.
            for ([_]v.Identity{ relationship.source, relationship.target }) |identity| {
                const position = overlay.node_positions.get(identity) orelse return error.CypherDeletedEntity;
                if (overlay.nodes.items[position].deleted) return error.CypherDeletedEntity;
            }
            try overlay.chargeValue(.{ .map = relationship.properties });
            try v.validatePropertyValue(.{ .map = relationship.properties });
            edge.metadata_json = try std.json.Stringify.valueAlloc(a, .{ .source_table = relationship.source.table, .target_table = relationship.target.table, .antfly_cypher = .{ .version = @as(u8, 1), .properties = root.wire.Value{ .value = .{ .map = relationship.properties } } } }, .{});
            try entry.edges.append(a, edge);
        }
        // Graph effects have their own durable intents. Their source-row
        // authorization is checked against the native row under a read guard;
        // never reconstruct an unchanged SQL row from its Cypher projection.
    }
    return plans.toOwnedSlice(a);
}
