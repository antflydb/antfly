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

//! Native document/adjacency adapter. The caller owns the retained DB cuts and
//! their catalog and authorization leases for the complete statement lifetime.
const std = @import("std");
const root = @import("root.zig");
const v = root.value;
const b = root.backend;
const A = std.mem.Allocator;

pub fn Adapter(comptime DB: type, comptime GraphIndex: type, comptime EdgeScanCursor: type) type {
    return struct {
        const Self = @This();
        pub const Table = struct {
            name: []const u8,
            /// Immutable namespace captured with this logical table binding.
            storage_name: ?[]const u8 = null,
            db: *DB,
            /// Only for bounded materialization while the exact owner's
            /// primary/replay capture fences are held. Never retain this
            /// capability in a query backend after releasing the fence.
            capture_fence: if (@hasDecl(DB, "StatementReadFence")) ?*const DB.StatementReadFence else ?*const anyopaque = null,
        };
        pub const ReadAuthorization = struct { proof: []const u8 = "", database: []const u8 = "", deadline_ns: ?u64 = null };
        /// Mandatory hooks hold graph/table authority, refresh revocation at
        /// checkpoints, and apply field projection before any value is bound.
        pub const Authority = struct {
            ptr: *anyopaque,
            bind: *const fn (*anyopaque, root.catalog.Graph) anyerror!void,
            checkpoint: *const fn (*anyopaque) anyerror!void,
            project: *const fn (*anyopaque, A, *const v.Node) anyerror!?*const v.Node,
            project_relationship: *const fn (*anyopaque, A, *const v.Relationship) anyerror!?*const v.Relationship,
            search: ?*const fn (*anyopaque, A, []const u8, b.SearchInput) anyerror!b.SearchPage = null,
            read_authorization: ?*const fn (*anyopaque, A, []const u8) anyerror!ReadAuthorization = null,
        };
        tables: []const Table,
        authority: Authority,
        ttl_now_ns: u64,
        graph: ?root.catalog.Graph = null,
        /// A routed owner serves only its catalog-certified table partition;
        /// the coordinator binds the complete graph before opening any owner.
        partitioned: bool = false,

        pub fn backend(self: *Self) b.Backend {
            return .{ .ptr = self, .vtable = &.{ .bind = bind, .checkpoint = checkpoint, .scan = scan, .get = get, .expand = expand, .search = search } };
        }
        fn search(ptr: *anyopaque, a: A, table: []const u8, input: b.SearchInput) !b.SearchPage {
            const self = cast(ptr);
            try checkpoint(ptr);
            _ = try self.selected(table);
            const callback = self.authority.search orelse return error.CypherSearchUnavailable;
            return callback(self.authority.ptr, a, table, input);
        }
        fn selected(self: *Self, name: []const u8) !*DB {
            for (self.tables) |table| if (std.mem.eql(u8, table.name, name)) return table.db;
            return error.UndefinedGraphTable;
        }
        fn matchesStorageTable(self: *Self, logical: []const u8, actual: []const u8) bool {
            if (std.mem.eql(u8, logical, actual)) return true;
            for (self.tables) |table| if (std.mem.eql(u8, table.name, logical)) {
                return if (table.storage_name) |physical| std.mem.eql(u8, physical, actual) else false;
            };
            return false;
        }
        fn cast(ptr: *anyopaque) *Self {
            return @ptrCast(@alignCast(ptr));
        }
        fn checkpoint(ptr: *anyopaque) !void {
            const self = cast(ptr);
            try self.authority.checkpoint(self.authority.ptr);
        }
        fn bind(ptr: *anyopaque, a: A, graph: root.catalog.Graph) !void {
            const self = cast(ptr);
            if (self.ttl_now_ns == 0) return error.CypherSnapshotRequired;
            try self.authority.bind(self.authority.ptr, graph);
            for (graph.labels) |label| {
                const db = self.selected(label.table) catch |err| {
                    if (self.partitioned and err == error.UndefinedGraphTable) continue;
                    return err;
                };
                if (label.table_id != 0 and db.core.identity_namespace.table_id != label.table_id) return error.CatalogGenerationChanged;
                // A shared apply lease on a mutable DB is not a snapshot of
                // independently published graph and document stores.
                if (!db.hasRetainedQueryCut()) {
                    if (comptime @hasDecl(DB, "StatementReadFence")) {
                        const table = for (self.tables) |entry| {
                            if (entry.db == db and std.mem.eql(u8, entry.name, label.table)) break entry;
                        } else return error.CypherSnapshotRequired;
                        const fence = table.capture_fence orelse return error.CypherSnapshotRequired;
                        if (fence.source != db) return error.CypherSnapshotRequired;
                    } else return error.CypherSnapshotRequired;
                }
            }
            for (graph.relationships) |relationship| {
                const table = graph.label(relationship.source_label).?.table;
                if (self.partitioned) {
                    _ = self.selected(table) catch |err| {
                        if (err == error.UndefinedGraphTable) continue;
                        return err;
                    };
                }
                if ((try self.selected(table)).core.graphIndex(relationship.index) == null) return error.IndexNotFound;
                if (relationship.identity) |expected| {
                    const actual = (try (try self.selected(table)).cypherGraphIndexIdentity(a, relationship.index)) orelse return error.IndexNotFound;
                    if (expected.incarnation != actual.incarnation or expected.config_hash != actual.config_hash) return error.CatalogGenerationChanged;
                }
            }
            self.graph = graph;
        }
        pub fn node(self: *Self, a: A, table: []const u8, key: []const u8, json: []const u8) !?*const v.Node {
            const value = try v.Value.fromJson(a, try std.json.parseFromSliceLeaky(std.json.Value, a, json, .{ .parse_numbers = false }));
            if (value != .map) return error.CypherInvalidDocument;
            const result = try a.create(v.Node);
            result.* = .{ .identity = .{ .table = try a.dupe(u8, table), .key = try a.dupe(u8, key) }, .properties = try v.entityProperties(a, value.map) };
            var labels: std.ArrayList([]const u8) = .empty;
            for (self.graph.?.labels) |label| if (self.graph.?.admits(result, label.name)) try labels.append(a, label.name);
            result.labels = try labels.toOwnedSlice(a);
            return self.authority.project(self.authority.ptr, a, result);
        }
        fn scan(ptr: *anyopaque, a: A, table: []const u8, cursor: ?[]const u8, limit: usize) !b.Page(*const v.Node) {
            const self = cast(ptr);
            try checkpoint(ptr);
            const db = try self.selected(table);
            const authorization = try self.readAuthorization(a, table);
            var found = if (comptime @hasDecl(DB, "cypherDocumentScanOptions"))
                try db.scan(a, cursor orelse "", "", try db.cypherDocumentScanOptions(a, .{ .execution_deadline_ns = authorization.deadline_ns, .row_policy_principal_proof = authorization.proof, .row_policy_database = authorization.database, .inclusive_from = cursor == null, .include_documents = true, .limit = std.math.cast(u32, limit) orelse return error.InvalidArgument }))
            else
                try db.scan(a, cursor orelse "", "", .{ .execution_deadline_ns = authorization.deadline_ns, .row_policy_principal_proof = authorization.proof, .row_policy_database = authorization.database, .inclusive_from = cursor == null, .include_documents = true, .limit = std.math.cast(u32, limit) orelse return error.InvalidArgument });
            defer found.deinit(a);
            var nodes: std.ArrayList(*const v.Node) = .empty;
            for (found.documents) |document| if (try self.node(a, table, document.id, document.json)) |authorized| if (authorized.labels.len != 0) try nodes.append(a, authorized);
            const next = if (found.documents.len == limit) try a.dupe(u8, found.documents[found.documents.len - 1].id) else null;
            try checkpoint(ptr);
            return .{ .items = try nodes.toOwnedSlice(a), .next = next, .examined = found.documents.len };
        }
        fn get(ptr: *anyopaque, a: A, identity: v.Identity) !?*const v.Node {
            const self = cast(ptr);
            if (comptime @hasDecl(DB, "cypherDocumentKeyAllowed")) {
                if (!DB.cypherDocumentKeyAllowed(identity.key)) return null;
            }
            try checkpoint(ptr);
            const db = try self.selected(identity.table);
            const authorization = try self.readAuthorization(a, identity.table);
            var document = (try db.getDocument(a, identity.key, .{ .row_policy_principal_proof = authorization.proof, .row_policy_database = authorization.database, .execution_deadline_ns = authorization.deadline_ns })) orelse return null;
            defer document.deinit(a);
            const result = try self.node(a, identity.table, identity.key, document.json);
            try checkpoint(ptr);
            return result;
        }
        const Resume = struct { incoming: bool = false, cursor: ?EdgeScanCursor = null };
        fn readAuthorization(self: *Self, a: A, table: []const u8) !ReadAuthorization {
            return if (self.authority.read_authorization) |resolve| try resolve(self.authority.ptr, a, table) else .{};
        }
        fn expand(ptr: *anyopaque, a: A, mapping: root.catalog.Relationship, identity: v.Identity, direction: root.ast.Direction, encoded: ?[]const u8, limit: usize) !b.Page(*const v.Relationship) {
            const self = cast(ptr);
            try checkpoint(ptr);
            const graph = self.graph.?;
            const source_table = graph.label(mapping.source_label).?.table;
            const target_table = graph.label(mapping.target_label).?.table;
            // Incoming adjacency physically belongs to the declared source
            // table, even when its target key belongs to another table.
            var position: Resume = if (encoded) |json| try std.json.parseFromSliceLeaky(Resume, a, json, .{}) else .{ .incoming = direction == .in or !std.mem.eql(u8, identity.table, source_table) };
            const incoming_possible = direction != .out and std.mem.eql(u8, identity.table, target_table);
            const outgoing_possible = direction != .in and std.mem.eql(u8, identity.table, source_table);
            if ((position.incoming and !incoming_possible) or (!position.incoming and !outgoing_possible)) return .{ .items = &.{} };
            const db = try self.selected(source_table);
            const authorization = try self.readAuthorization(a, source_table);
            var page = captured: {
                if (comptime @hasDecl(DB, "graphEdgesForCypherCapturePageAuthorizedAt")) {
                    for (self.tables) |table| if (table.db == db and std.mem.eql(u8, table.name, source_table)) {
                        if (table.capture_fence) |fence| break :captured try db.graphEdgesForCypherCapturePageAuthorizedAt(fence, a, mapping.index, identity.key, &.{mapping.stored_type}, if (position.incoming) .in else .out, position.cursor, .{ .max_edges = limit, .max_owned_bytes = 4 << 20, .max_scanned_rows = 4096 }, self.ttl_now_ns, authorization.proof, authorization.database);
                    };
                }
                break :captured if (comptime @hasDecl(DB, "graphEdgesForCypherPageAuthorizedAt"))
                    try db.graphEdgesForCypherPageAuthorizedAt(a, mapping.index, identity.key, &.{mapping.stored_type}, if (position.incoming) .in else .out, position.cursor, .{ .max_edges = limit, .max_owned_bytes = 4 << 20, .max_scanned_rows = 4096 }, self.ttl_now_ns, authorization.proof, authorization.database)
                else blk: {
                    if (authorization.proof.len != 0) return error.CypherRowPolicyUnsupported;
                    break :blk try db.graphEdgesForCypherPageAt(a, mapping.index, identity.key, &.{mapping.stored_type}, if (position.incoming) .in else .out, position.cursor, .{ .max_edges = limit, .max_owned_bytes = 4 << 20, .max_scanned_rows = 4096 }, self.ttl_now_ns);
                };
            };
            defer page.deinit(a);
            var result: std.ArrayList(*const v.Relationship) = .empty;
            for (page.edges) |edge| {
                const metadata = if (edge.metadata.len == 0) v.Value{ .map = &.{} } else try v.Value.fromJson(a, try std.json.parseFromSliceLeaky(std.json.Value, a, edge.metadata, .{}));
                if (metadata != .map) return error.CypherInvalidRelationshipMetadata;
                const actual_source = metadata.property("source_table");
                const actual_target = metadata.property("target_table");
                if (actual_source != .null and (actual_source != .string or !self.matchesStorageTable(source_table, actual_source.string))) continue;
                if (actual_target != .null and (actual_target != .string or !self.matchesStorageTable(target_table, actual_target.string))) continue;
                const source: v.Identity = .{ .table = source_table, .key = edge.source };
                const target: v.Identity = .{ .table = target_table, .key = edge.target };
                if (!(if (position.incoming) target else source).eql(identity)) continue;
                // Native in/out scans both contain a self-loop. Return it once
                // for an undirected pattern, preserving parallel IDs.
                if (position.incoming and outgoing_possible and source.eql(target)) continue;
                const relationship = try a.create(v.Relationship);
                var properties: std.ArrayList(v.Entry) = .empty;
                const raw_metadata = if (edge.metadata.len == 0) std.json.Value{ .null = {} } else try std.json.parseFromSliceLeaky(std.json.Value, a, edge.metadata, .{});
                const marker = if (raw_metadata == .object) raw_metadata.object.get("antfly_cypher") else null;
                if (marker) |payload| {
                    if (payload != .object) return error.CypherInvalidRelationshipMetadata;
                    const version = payload.object.get("version") orelse return error.CypherInvalidRelationshipMetadata;
                    if (version != .integer or version.integer != 1) return error.CypherInvalidRelationshipMetadata;
                    const decoded = try root.wire.parameter(a, payload.object.get("properties") orelse return error.CypherInvalidRelationshipMetadata);
                    if (decoded != .map) return error.CypherInvalidRelationshipMetadata;
                    try properties.appendSlice(a, decoded.map);
                } else {
                    for (metadata.map) |entry| if (!std.mem.eql(u8, entry.name, "weight") and !std.mem.eql(u8, entry.name, "source_table") and !std.mem.eql(u8, entry.name, "target_table")) try properties.append(a, entry);
                    try properties.append(a, .{ .name = "weight", .value = .{ .number = edge.weight } });
                }
                const field_derived = if (comptime @hasField(@TypeOf(edge), "field_derived")) edge.field_derived else edge.edge_id.len == 0 and if (comptime @hasDecl(DB, "cypherFieldDerivedEdgeType")) db.cypherFieldDerivedEdgeType(mapping.index, mapping.stored_type) else false;
                const artifact_derived = if (comptime @hasField(@TypeOf(edge), "artifact_derived")) edge.artifact_derived else false;
                const producer = if (edge.owner_document.len != 0) edge.owner_document else if (field_derived or artifact_derived) edge.source else "";
                const key = if (marker != null and producer.len == 0 and std.mem.startsWith(u8, edge.edge_id, "cypher:")) try a.dupe(u8, edge.edge_id) else if (field_derived) try std.json.Stringify.valueAlloc(a, .{ edge.source, edge.target, edge.edge_type, edge.edge_id, producer, "field" }, .{}) else try std.json.Stringify.valueAlloc(a, .{ edge.source, edge.target, edge.edge_type, edge.edge_id, producer }, .{});
                relationship.* = .{ .identity = .{ .table = try a.dupe(u8, source_table), .key = key }, .index = mapping.index, .type = mapping.name, .source = .{ .table = source_table, .key = try a.dupe(u8, source.key) }, .target = .{ .table = target_table, .key = try a.dupe(u8, target.key) }, .properties = try v.entityProperties(a, try properties.toOwnedSlice(a)), .ownership = if (artifact_derived) .artifact else if (field_derived) .field else if (producer.len == 0) .explicit else .document, .producer = if (producer.len == 0) null else .{ .table = source_table, .key = try a.dupe(u8, producer) } };
                if (try self.authority.project_relationship(self.authority.ptr, a, relationship)) |authorized| try result.append(a, authorized);
            }
            const next: ?[]const u8 = if (page.next_cursor) |cursor| try std.json.Stringify.valueAlloc(a, Resume{ .incoming = position.incoming, .cursor = cursor }, .{}) else if (!position.incoming and incoming_possible) blk: {
                position = .{ .incoming = true };
                break :blk try std.json.Stringify.valueAlloc(a, position, .{});
            } else null;
            try checkpoint(ptr);
            return .{ .items = try result.toOwnedSlice(a), .next = next, .examined = page.scanned_rows };
        }
        comptime {
            _ = GraphIndex;
        }
    };
}
