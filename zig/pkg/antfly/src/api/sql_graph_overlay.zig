// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Read-only graph view over a retained cohort and staged SQL postimages.
//! Capture runs in the Cypher execution allocator, so all retained rows,
//! projections, and adjacency remain subject to the query memory/work limits.
const std = @import("std");
const cypher = @import("cypher");
const A = std.mem.Allocator;
const types = @import("antfly_local_sources").storage_db_types;
pub const Row = struct { identity: cypher.value.Identity, document_json: ?[]const u8, json_null_fields: []const []const u8 = &.{} };
pub const Effects = struct { table: []const u8, writes: []const types.GraphEdgeWrite, deletes: []const types.GraphEdgeDelete };
pub const View = struct {
    client: *cypher.rpc.Client,
    rows: []const Row,
    effects: []const Effects = &.{},
    limits: cypher.runtime.Limits,
    inner: cypher.transaction.Overlay = undefined,
    changed: bool = false,
    pub fn backend(self: *View) cypher.backend.Backend {
        return .{ .ptr = self, .vtable = &.{ .bind = bind, .checkpoint = checkpoint, .scan = scan, .get = get, .expand = expand, .search = search, .startup_work = startupWork } };
    }
    fn cast(ptr: *anyopaque) *View {
        return @ptrCast(@alignCast(ptr));
    }
    fn unavailableIdentity(_: *anyopaque, _: A, _: []const u8, _: bool) !cypher.value.Identity {
        return error.CypherMutationsUnavailable;
    }
    fn unavailableCommit(_: *anyopaque, _: A, _: []const cypher.backend.Mutation) !void {
        return error.CypherMutationsUnavailable;
    }
    fn capacity(self: *View) !void {
        if (self.inner.nodes.items.len + self.inner.relationships.items.len >= self.inner.max_entities) return error.CypherStateBudgetExceeded;
    }
    fn bind(ptr: *anyopaque, a: A, graph: cypher.catalog.Graph) !void {
        const self = cast(ptr);
        self.inner = .{ .base = self.client.backend(), .host = .{ .ptr = self, .new_identity = unavailableIdentity, .commit = unavailableCommit }, .max_entities = @min(65536, self.limits.intermediate_states), .max_anchors = self.limits.scanned_anchors, .max_edges = self.limits.explored_edges };
        const backend_inner = self.inner.backend();
        try backend_inner.vtable.bind(backend_inner.ptr, a, graph);
        try backend_inner.vtable.begin.?(backend_inner.ptr);
        for (self.rows) |row| {
            const member = for (graph.labels) |label| {
                if (std.mem.eql(u8, label.table, row.identity.table)) break true;
            } else false;
            if (!member) continue;
            try checkpoint(ptr);
            self.changed = true;
            self.inner.captured.scanned_anchors +|= 1;
            const projection: cypher.rpc.Projection = if (row.document_json) |json| try self.client.projectDocument(a, row.identity, try std.json.parseFromSliceLeaky(std.json.Value, a, json, .{ .parse_numbers = false }), row.json_null_fields, true) else .{ .node = null };
            const visible = projection.node != null and projection.node.?.labels.len != 0;
            var found = false;
            for (self.inner.nodes.items) |*node| {
                if (!node.value.identity.eql(row.identity)) continue;
                found = true;
                node.deleted = !visible;
                if (projection.node) |value| node.value = @constCast(value);
                break;
            }
            if (!found and visible) {
                try self.capacity();
                try self.inner.nodes.append(a, .{ .value = @constCast(projection.node.?) });
            }
            // A document owns its derived contributions even when an explicit
            // source override places their outgoing adjacency on another key.
            for (self.inner.relationships.items) |*relationship| {
                if (relationship.value.ownership != .document or !std.mem.eql(u8, relationship.value.identity.table, row.identity.table)) continue;
                const tuple = try std.json.parseFromSliceLeaky([]const []const u8, a, relationship.value.identity.key, .{});
                if (tuple.len != 5) return error.CypherInvalidRelationship;
                if (std.mem.eql(u8, tuple[4], row.identity.key)) relationship.deleted = true;
            }
            for (projection.relationships) |relationship| {
                try self.capacity();
                try self.inner.relationships.append(a, .{ .value = @constCast(relationship) });
                self.inner.captured.explored_edges +|= 1;
            }
        }
        for (self.effects) |effects| {
            for (effects.deletes) |edge| for (graph.relationships) |mapping| {
                if (!matches(graph, effects.table, mapping, edge)) continue;
                self.changed = true;
                for (self.inner.relationships.items) |*relationship| {
                    if (!std.mem.eql(u8, relationship.value.index, mapping.index) or !std.mem.eql(u8, relationship.value.type, mapping.name) or !std.mem.eql(u8, relationship.value.source.key, edge.source) or !std.mem.eql(u8, relationship.value.target.key, edge.target)) continue;
                    if (std.mem.eql(u8, relationship.value.identity.key, edge.edge_id) and edge.edge_id.len != 0) {
                        relationship.deleted = true;
                        continue;
                    }
                    if (!std.mem.startsWith(u8, relationship.value.identity.key, "[")) continue;
                    const tuple = try std.json.parseFromSliceLeaky([]const []const u8, a, relationship.value.identity.key, .{});
                    if (tuple.len != 5) return error.CypherInvalidRelationship;
                    if (std.mem.eql(u8, tuple[3], edge.edge_id) and std.mem.eql(u8, tuple[4], producer(edge))) relationship.deleted = true;
                }
            };
            for (effects.writes) |edge| for (graph.relationships) |mapping| {
                if (!matches(graph, effects.table, mapping, edge)) continue;
                self.changed = true;
                try checkpoint(ptr);
                const source = cypher.value.Identity{ .table = effects.table, .key = edge.source };
                const target = cypher.value.Identity{ .table = graph.label(mapping.target_label).?.table, .key = edge.target };
                const source_node = (try backend_inner.vtable.get(backend_inner.ptr, a, source)) orelse continue;
                const target_node = (try backend_inner.vtable.get(backend_inner.ptr, a, target)) orelse continue;
                if (!graph.admits(source_node, mapping.source_label) or !graph.admits(target_node, mapping.target_label)) continue;
                if (producer(edge).len != 0 and (try backend_inner.vtable.get(backend_inner.ptr, a, .{ .table = effects.table, .key = producer(edge) })) == null) continue;
                const raw = try std.json.parseFromSliceLeaky(std.json.Value, a, if (edge.metadata_json.len == 0) "{}" else edge.metadata_json, .{});
                const metadata = try cypher.value.Value.fromJson(a, raw);
                if (metadata != .map) return error.CypherInvalidRelationshipMetadata;
                const actual_source = metadata.property("source_table");
                const actual_target = metadata.property("target_table");
                if (actual_source != .null and (actual_source != .string or !std.mem.eql(u8, actual_source.string, source.table))) continue;
                if (actual_target != .null and (actual_target != .string or !std.mem.eql(u8, actual_target.string, target.table))) continue;
                const marker = raw.object.get("antfly_cypher");
                var properties: std.ArrayList(cypher.value.Entry) = .empty;
                if (marker) |payload| {
                    if (payload != .object) return error.CypherInvalidRelationshipMetadata;
                    const version = payload.object.get("version") orelse return error.CypherInvalidRelationshipMetadata;
                    if (version != .integer or version.integer != 1) return error.CypherInvalidRelationshipMetadata;
                    const decoded = try cypher.wire.parameter(a, payload.object.get("properties") orelse return error.CypherInvalidRelationshipMetadata);
                    if (decoded != .map) return error.CypherInvalidRelationshipMetadata;
                    try properties.appendSlice(a, decoded.map);
                } else {
                    for (metadata.map) |entry| if (!std.mem.eql(u8, entry.name, "weight") and !std.mem.eql(u8, entry.name, "source_table") and !std.mem.eql(u8, entry.name, "target_table")) try properties.append(a, entry);
                    try properties.append(a, .{ .name = "weight", .value = .{ .number = edge.weight } });
                }
                const key = if (marker != null and producer(edge).len == 0 and std.mem.startsWith(u8, edge.edge_id, "cypher:")) edge.edge_id else try std.json.Stringify.valueAlloc(a, .{ edge.source, edge.target, edge.edge_type, edge.edge_id, producer(edge) }, .{});
                var replaced = false;
                for (self.inner.relationships.items) |*relationship| {
                    if (!std.mem.eql(u8, relationship.value.index, mapping.index) or !relationship.value.identity.eql(.{ .table = effects.table, .key = key })) continue;
                    relationship.value.* = .{ .identity = .{ .table = effects.table, .key = key }, .index = mapping.index, .type = mapping.name, .source = source, .target = target, .properties = properties.items, .ownership = if (producer(edge).len == 0) .explicit else .document };
                    relationship.deleted = false;
                    replaced = true;
                    break;
                }
                if (!replaced) {
                    try self.capacity();
                    const relationship = try a.create(cypher.value.Relationship);
                    relationship.* = .{ .identity = .{ .table = effects.table, .key = key }, .index = mapping.index, .type = mapping.name, .source = source, .target = target, .properties = properties.items, .ownership = if (producer(edge).len == 0) .explicit else .document };
                    try self.inner.relationships.append(a, .{ .value = relationship });
                }
                self.inner.captured.explored_edges +|= 1;
            };
        }
    }
    fn matches(graph: cypher.catalog.Graph, table: []const u8, mapping: cypher.catalog.Relationship, edge: anytype) bool {
        return std.mem.eql(u8, graph.label(mapping.source_label).?.table, table) and std.mem.eql(u8, mapping.index, edge.index_name) and std.mem.eql(u8, mapping.stored_type, edge.edge_type);
    }
    fn producer(edge: anytype) []const u8 {
        return if (edge.owner_document.len != 0) edge.owner_document else edge.owner;
    }
    fn checkpoint(ptr: *anyopaque) !void {
        const self = cast(ptr);
        const base = self.client.backend();
        try base.vtable.checkpoint(base.ptr);
    }
    fn startupWork(ptr: *anyopaque) cypher.backend.StartupWork {
        return cast(ptr).inner.captured;
    }
    fn scan(ptr: *anyopaque, a: A, table: []const u8, after: ?[]const u8, limit: usize) !cypher.backend.Page(*const cypher.value.Node) {
        const backend_inner = cast(ptr).inner.backend();
        return backend_inner.vtable.scan(backend_inner.ptr, a, table, after, limit);
    }
    fn get(ptr: *anyopaque, a: A, identity: cypher.value.Identity) !?*const cypher.value.Node {
        const backend_inner = cast(ptr).inner.backend();
        return backend_inner.vtable.get(backend_inner.ptr, a, identity);
    }
    fn expand(ptr: *anyopaque, a: A, mapping: cypher.catalog.Relationship, identity: cypher.value.Identity, direction: cypher.ast.Direction, after: ?[]const u8, limit: usize) !cypher.backend.Page(*const cypher.value.Relationship) {
        const backend_inner = cast(ptr).inner.backend();
        return backend_inner.vtable.expand(backend_inner.ptr, a, mapping, identity, direction, after, limit);
    }
    fn search(ptr: *anyopaque, a: A, table: []const u8, input: cypher.backend.SearchInput) !cypher.backend.SearchPage {
        const self = cast(ptr);
        if (self.changed) return error.CypherSearchAfterWrite;
        const base = self.client.backend();
        return (base.vtable.search orelse return error.CypherSearchUnavailable)(base.ptr, a, table, input);
    }
};
