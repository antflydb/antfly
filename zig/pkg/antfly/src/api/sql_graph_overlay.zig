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
    staged_rows: cypher.value.IdentitySet = .empty,
    projected_fields: std.HashMapUnmanaged(FieldKey, void, FieldKey.Context, std.hash_map.default_max_load_percentage) = .empty,
    const FieldKey = struct {
        source: cypher.value.Identity,
        index: []const u8,
        kind: []const u8,
        const Context = struct {
            pub fn hash(_: @This(), key: FieldKey) u64 {
                return std.hash.Wyhash.hash(std.hash.Wyhash.hash((cypher.value.Identity.Context{}).hash(key.source), key.index), key.kind);
            }
            pub fn eql(_: @This(), left: FieldKey, right: FieldKey) bool {
                return left.source.eql(right.source) and std.mem.eql(u8, left.index, right.index) and std.mem.eql(u8, left.kind, right.kind);
            }
        };
    };
    pub fn backend(self: *View) cypher.backend.Backend {
        return .{ .ptr = self, .vtable = &.{ .bind = bind, .checkpoint = checkpoint, .scan = scan, .get = get, .expand = expand, .search = search, .startup_work = startupWork, .set_work = setWork } };
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
        self.inner = .{ .base = self.client.backend(), .host = .{ .ptr = self, .new_identity = unavailableIdentity, .commit = unavailableCommit }, .max_entities = self.limits.intermediate_states, .max_anchors = self.limits.scanned_anchors, .max_edges = self.limits.explored_edges };
        const backend_inner = self.inner.backend();
        try backend_inner.vtable.bind(backend_inner.ptr, a, graph);
        try backend_inner.vtable.begin.?(backend_inner.ptr);
        // Staged owner projection happens during bind, before the runtime can
        // attach its meter. Keep the same capture ceiling through this phase.
        const previous_work = self.client.work;
        self.client.work = .{ .ptr = self, .charge = captureCharge, .value = captureValue, .remaining = captureRemaining };
        defer self.client.work = previous_work;
        self.staged_rows = .empty;
        self.projected_fields = .empty;
        for (self.inner.relationships.items) |relationship| try self.indexField(a, relationship.value);
        for (self.rows) |row| {
            const member = for (graph.labels) |label| {
                if (std.mem.eql(u8, label.table, row.identity.table)) break true;
            } else false;
            if (!member) continue;
            try checkpoint(ptr);
            try self.inner.charge(1);
            try self.staged_rows.put(a, row.identity, {});
            self.changed = true;
            self.inner.captured.scanned_anchors +|= 1;
            const projection: cypher.rpc.Projection = if (row.document_json) |json| try self.client.projectDocument(a, row.identity, try std.json.parseFromSliceLeaky(std.json.Value, a, json, .{ .parse_numbers = false }), row.json_null_fields, true) else .{ .node = null };
            const authorized = projection.node != null;
            if (self.inner.node_positions.get(row.identity)) |position| {
                const node = &self.inner.nodes.items[position];
                node.deleted = !authorized;
                if (projection.node) |value| node.value = @constCast(value);
            } else {
                try self.capacity();
                const node = if (projection.node) |value| @constCast(value) else tombstone: {
                    const value = try a.create(cypher.value.Node);
                    value.* = .{ .identity = row.identity };
                    break :tombstone value;
                };
                // Authorized producer rows need not be admitted graph vertices.
                // Keep hidden/deleted rows as tombstones so a base-cut lookup
                // cannot restore their old producer authority.
                try self.inner.appendNode(a, .{ .value = node, .deleted = !authorized });
            }
            // A document owns its derived contributions even when an explicit
            // source override places their outgoing adjacency on another key.
            try self.inner.retireOwnedRelationships(a, row.identity, authorized);
            for (projection.relationships) |relationship| {
                try self.indexField(a, relationship);
                try self.capacity();
                try self.inner.appendRelationship(a, .{ .value = @constCast(relationship) });
                self.inner.captured.explored_edges +|= 1;
            }
        }
        for (self.effects) |effects| {
            for (effects.deletes) |edge| for (graph.relationships) |mapping| {
                if (!matches(graph, effects.table, mapping, edge) or self.projectedFieldEffect(effects.table, mapping, edge)) continue;
                self.changed = true;
                const tuple = try std.json.Stringify.valueAlloc(a, .{ edge.source, edge.target, edge.edge_type, edge.edge_id, producer(edge) }, .{});
                for ([_][]const u8{ edge.edge_id, tuple }) |key| {
                    if (key.len == 0) continue;
                    try self.inner.charge(1);
                    const position = self.inner.relationship_positions.get(.{ .identity = .{ .table = effects.table, .key = key }, .index = mapping.index }) orelse continue;
                    const relationship = &self.inner.relationships.items[position];
                    if (std.mem.eql(u8, relationship.value.type, mapping.name) and std.mem.eql(u8, relationship.value.source.key, edge.source) and std.mem.eql(u8, relationship.value.target.key, edge.target)) relationship.deleted = true;
                }
            };
            for (effects.writes) |edge| for (graph.relationships) |mapping| {
                if (!matches(graph, effects.table, mapping, edge) or self.projectedFieldEffect(effects.table, mapping, edge)) continue;
                self.changed = true;
                try checkpoint(ptr);
                const source = cypher.value.Identity{ .table = effects.table, .key = edge.source };
                const target = cypher.value.Identity{ .table = graph.label(mapping.target_label).?.table, .key = edge.target };
                const source_node = (try backend_inner.vtable.get(backend_inner.ptr, a, source)) orelse continue;
                const target_node = (try backend_inner.vtable.get(backend_inner.ptr, a, target)) orelse continue;
                var membership = self.inner.valueWork(a);
                if (!try graph.admits(source_node, mapping.source_label, &membership) or !try graph.admits(target_node, mapping.target_label, &membership)) continue;
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
                if (self.inner.relationship_positions.get(.{ .identity = .{ .table = effects.table, .key = key }, .index = mapping.index })) |position| {
                    const relationship = &self.inner.relationships.items[position];
                    const field_derived = relationship.value.ownership == .field;
                    relationship.value.* = .{ .identity = .{ .table = effects.table, .key = key }, .index = mapping.index, .type = mapping.name, .source = source, .target = target, .properties = try cypher.value.entityProperties(a, properties.items), .ownership = if (producer(edge).len == 0) .explicit else .document, .producer = if (producer(edge).len == 0) null else .{ .table = effects.table, .key = producer(edge) } };
                    if (field_derived) relationship.value.ownership = .field;
                    relationship.deleted = false;
                    try self.inner.indexRelationship(a, position);
                } else {
                    try self.capacity();
                    const relationship = try a.create(cypher.value.Relationship);
                    relationship.* = .{ .identity = .{ .table = effects.table, .key = key }, .index = mapping.index, .type = mapping.name, .source = source, .target = target, .properties = try cypher.value.entityProperties(a, properties.items), .ownership = if (producer(edge).len == 0) .explicit else .document, .producer = if (producer(edge).len == 0) null else .{ .table = effects.table, .key = producer(edge) } };
                    try self.inner.appendRelationship(a, .{ .value = relationship });
                }
                self.inner.captured.explored_edges +|= 1;
            };
        }
    }
    fn matches(graph: cypher.catalog.Graph, table: []const u8, mapping: cypher.catalog.Relationship, edge: anytype) bool {
        return std.mem.eql(u8, graph.label(mapping.source_label).?.table, table) and std.mem.eql(u8, mapping.index, edge.index_name) and std.mem.eql(u8, mapping.stored_type, edge.edge_type);
    }
    // Deterministic fields have already been rebuilt from the complete staged
    // document. Replaying their physical tuple deltas would also erase a
    // published artifact assertion sharing that tuple and producer.
    fn projectedFieldEffect(self: *View, table: []const u8, mapping: cypher.catalog.Relationship, edge: anytype) bool {
        if (edge.edge_id.len != 0 or edge.owner_document.len != 0 or (edge.owner.len != 0 and !std.mem.eql(u8, edge.owner, edge.source))) return false;
        const source = cypher.value.Identity{ .table = table, .key = edge.source };
        return self.staged_rows.contains(source) and self.projected_fields.contains(.{ .source = source, .index = mapping.index, .kind = mapping.name });
    }
    fn indexField(self: *View, a: A, relationship: *const cypher.value.Relationship) !void {
        try self.inner.charge(1);
        if (relationship.ownership == .field) try self.projected_fields.put(a, .{ .source = relationship.source, .index = relationship.index, .kind = relationship.type }, {});
    }
    fn captureCharge(ptr: *anyopaque, amount: usize) !void {
        try cast(ptr).inner.charge(amount);
    }
    fn captureRemaining(ptr: *anyopaque) usize {
        const self = cast(ptr);
        return self.inner.max_entities -| self.inner.captured.intermediate_states;
    }
    fn captureValue(ptr: *anyopaque, value: cypher.value.Value) !void {
        const self = cast(ptr);
        var work = self.inner.valueWork(self.inner.alloc.?);
        try work.output(value);
    }
    fn setWork(ptr: *anyopaque, work: ?cypher.backend.Work) void {
        cast(ptr).inner.setExecutionWork(work);
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
