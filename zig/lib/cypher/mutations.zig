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
pub const Edge = struct { edge_id: []const u8, index_name: []const u8, source: []const u8, target: []const u8, edge_type: []const u8, metadata_json: []const u8 = "" };
pub const Plan = struct {
    table: []const u8,
    writes: std.ArrayList(Write) = .empty,
    deletes: std.ArrayList([]const u8) = .empty,
    edges: std.ArrayList(Edge) = .empty,
    removed_edges: std.ArrayList(Edge) = .empty,
    fn hasWrite(self: Plan, key: []const u8) bool {
        for (self.writes.items) |write| if (std.mem.eql(u8, write.key, key)) return true;
        for (self.deletes.items) |deleted| if (std.mem.eql(u8, deleted, key)) return true;
        return false;
    }
};
pub const Preimage = struct { ptr: *anyopaque, existed: *const fn (*anyopaque, []const u8, []const u8) bool };
fn plan(plans: []Plan, table: []const u8) !*Plan {
    for (plans) |*entry| if (std.mem.eql(u8, entry.table, table)) return entry;
    return error.UndefinedGraphTable;
}
fn data(value: v.Value) anyerror!void {
    switch (value) {
        .node, .relationship, .path => return error.CypherInvalidPropertyValue,
        .number => |number| if (!std.math.isFinite(number)) return error.CypherInvalidPropertyValue,
        .map => |entries| for (entries) |entry| try data(entry.value),
        .list => |items| for (items) |item| try data(item),
        else => {},
    }
}
pub fn document(a: A, node: *const v.Node) ![]const u8 {
    var properties: std.ArrayList(v.Entry) = .empty;
    for (node.properties) |entry| {
        try data(entry.value);
        if (entry.value != .null) try properties.append(a, entry);
    }
    return std.json.Stringify.valueAlloc(a, v.Value{ .map = properties.items }, .{});
}
fn nodeChanged(mutations: []const root.backend.Mutation, identity: v.Identity) bool {
    for (mutations) |mutation| switch (mutation) {
        .create_node => |node| if (node.identity.eql(identity)) return true,
        .delete_node => |node| if (node.identity.eql(identity)) return true,
        .set_properties => |update| if (update.target == .node and update.target.node.eql(identity)) return true,
        .remove_properties => |update| if (update.target == .node and update.target.node.eql(identity)) return true,
        else => {},
    };
    return false;
}
fn relationshipChanged(mutations: []const root.backend.Mutation, relationship: *const v.Relationship) bool {
    const target: v.EntityIdentity = .{ .relationship = .{ .identity = relationship.identity, .index = relationship.index } };
    for (mutations) |mutation| switch (mutation) {
        .create_relationship => |edge| if (edge.identity.eql(relationship.identity)) return true,
        .delete_relationship => |edge| if (target.relationship.identity.eql(edge.identity) and std.mem.eql(u8, edge.index, relationship.index)) return true,
        .set_properties => |update| if (target.eql(update.target)) return true,
        .remove_properties => |update| if (target.eql(update.target)) return true,
        .delete_node => |node| if (node.detach and (node.identity.eql(relationship.source) or node.identity.eql(relationship.target))) return true,
        else => {},
    };
    return false;
}
fn edgeId(a: A, relationship: *const v.Relationship) ![]const u8 {
    if (std.mem.startsWith(u8, relationship.identity.key, "cypher:")) return relationship.identity.key;
    const tuple = try std.json.parseFromSliceLeaky([]const []const u8, a, relationship.identity.key, .{});
    if (tuple.len != 5 or !std.mem.eql(u8, tuple[0], relationship.source.key) or !std.mem.eql(u8, tuple[1], relationship.target.key) or tuple[4].len != 0) return error.CypherDerivedRelationshipMutation;
    return tuple[3];
}
pub fn build(a: A, graph: root.catalog.Graph, overlay: *const root.transaction.Overlay, mutations: []const root.backend.Mutation, preimage: Preimage) ![]Plan {
    var plans: std.ArrayList(Plan) = .empty;
    for (graph.labels) |label| {
        const exists = for (plans.items) |entry| {
            if (std.mem.eql(u8, entry.table, label.table)) break true;
        } else false;
        if (!exists) try plans.append(a, .{ .table = label.table });
    }
    for (overlay.nodes.items) |node| {
        if (!nodeChanged(mutations, node.value.identity)) continue;
        const entry = try plan(plans.items, node.value.identity.table);
        if (node.deleted) {
            if (preimage.existed(preimage.ptr, entry.table, node.value.identity.key)) try entry.deletes.append(a, node.value.identity.key);
        } else {
            for (node.value.labels) |label| if (!graph.admits(node.value, label)) return error.CypherLabelPredicateMismatch;
            try entry.writes.append(a, .{ .key = node.value.identity.key, .value = try document(a, node.value) });
        }
    }
    for (overlay.relationships.items) |item| {
        const relationship = item.value;
        if (relationship.ownership != .explicit or !relationshipChanged(mutations, relationship)) continue;
        const entry = try plan(plans.items, relationship.source.table);
        const mapping = graph.relationship(relationship.type) orelse return error.UndefinedGraphRelationship;
        if (!std.mem.eql(u8, relationship.source.table, graph.label(mapping.source_label).?.table) or !std.mem.eql(u8, relationship.target.table, graph.label(mapping.target_label).?.table)) return error.CypherInvalidRelationship;
        var edge: Edge = .{ .edge_id = try edgeId(a, relationship), .index_name = relationship.index, .source = relationship.source.key, .target = relationship.target.key, .edge_type = mapping.stored_type };
        if (item.deleted) {
            const created = for (mutations) |mutation| {
                if (mutation == .create_relationship and mutation.create_relationship.identity.eql(relationship.identity)) break true;
            } else false;
            if (created) continue;
            try entry.removed_edges.append(a, edge);
        } else {
            // Never encode an active edge whose endpoint was removed in this
            // statement, even if a delete already supplies the source intent.
            for ([_]v.Identity{ relationship.source, relationship.target }) |identity| {
                const present = for (overlay.nodes.items) |node| {
                    if (!node.deleted and node.value.identity.eql(identity)) break true;
                } else false;
                if (!present) return error.CypherDeletedEntity;
            }
            try data(.{ .map = relationship.properties });
            edge.metadata_json = try std.json.Stringify.valueAlloc(a, .{ .source_table = relationship.source.table, .target_table = relationship.target.table, .antfly_cypher = .{ .version = @as(u8, 1), .properties = root.wire.Value{ .value = .{ .map = relationship.properties } } } }, .{});
            try entry.edges.append(a, edge);
        }
        if (!entry.hasWrite(relationship.source.key)) {
            const source = for (overlay.nodes.items) |node| {
                if (!node.deleted and node.value.identity.eql(relationship.source)) break node.value;
            } else return error.CypherDeletedEntity;
            try entry.writes.append(a, .{ .key = source.identity.key, .value = try document(a, source) });
        }
    }
    return plans.toOwnedSlice(a);
}
