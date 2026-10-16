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

//! Portable single-source graph host used by the public Zig and WASM API.
//! It shares catalog, pinned storage cuts, mutation planning, schema normalization,
//! native intents, and durable receipts with the native deployment.
const std = @import("std");
const cypher = @import("cypher");
const graph_catalog = @import("embedded_graph_catalog.zig");
const types = @import("db/types.zig");
const tracking = @import("range_protection.zig");
const A = std.mem.Allocator;
pub const Scope = struct {
    transaction_id: ?types.TxnId = null,
    outcome: enum { not_started, aborted, committed, unknown } = .not_started,
};
pub fn Host(comptime DB: type) type {
    return struct {
        const Self = @This();
        root: *DB,
        table: []const u8,
        alloc: A,
        writable: bool,
        uncertain: bool = false,
        pub fn get(self: Self, _: A, name: []const u8) !*DB {
            if (!std.mem.eql(u8, name, self.table)) return error.UndefinedGraphTable;
            return self.root;
        }
        pub fn catalogJson(self: Self, result_allocator: A, body: []const u8) ![]u8 {
            var lease = try self.root.acquireCypherReadAuthority(self.alloc, "", "default");
            defer lease.release();
            return graph_catalog.execute(self, result_allocator, body);
        }
        const PinnedAdapter = cypher.native.Adapter(DB.CypherPinnedDatabase, void, DB.CypherEdgeScanCursor);
        const Statement = struct {
            host: Self,
            graph: cypher.catalog.Graph,
            lease: DB.CypherReadLease,
            schema_version: u32,
            scope: *Scope,
            pinned: DB.CypherPinnedDatabase = undefined,
            adapter: PinnedAdapter = undefined,
            tables: []PinnedAdapter.Table,
            overlay: ?*cypher.transaction.Overlay = null,
            proofs: []const tracking.Proof = &.{},
            next_entity: u64 = 0,
            pub fn pinnedBackend(self: *Statement) cypher.backend.Backend {
                return .{ .ptr = self, .vtable = &.{ .bind = pinnedBind, .checkpoint = checkpoint, .scan = pinnedScan, .get = pinnedGet, .expand = pinnedExpand, .set_work = pinnedWork, .normalize_node = normalize, .document_relationships = documentRelationships } };
            }
            fn pinnedBind(ptr: *anyopaque, a: A, graph: cypher.catalog.Graph) !void {
                const backend = cast(ptr).adapter.backend();
                try backend.vtable.bind(backend.ptr, a, graph);
            }
            fn pinnedWork(ptr: *anyopaque, work: ?cypher.backend.Work) void {
                const backend = cast(ptr).adapter.backend();
                backend.vtable.set_work.?(backend.ptr, work);
            }
            fn pinnedScan(ptr: *anyopaque, a: A, table: []const u8, after: ?[]const u8, limit: usize) !cypher.backend.Page(*const cypher.value.Node) {
                const backend = cast(ptr).adapter.backend();
                return backend.vtable.scan(backend.ptr, a, table, after, limit);
            }
            fn pinnedGet(ptr: *anyopaque, a: A, identity: cypher.value.Identity) !?*const cypher.value.Node {
                const backend = cast(ptr).adapter.backend();
                return backend.vtable.get(backend.ptr, a, identity);
            }
            fn pinnedExpand(ptr: *anyopaque, a: A, mapping: cypher.catalog.Relationship, identity: cypher.value.Identity, direction: cypher.ast.Direction, after: ?[]const u8, limit: usize) !cypher.backend.Page(*const cypher.value.Relationship) {
                const backend = cast(ptr).adapter.backend();
                return backend.vtable.expand(backend.ptr, a, mapping, identity, direction, after, limit);
            }
            fn cast(ptr: *anyopaque) *Statement {
                return @ptrCast(@alignCast(ptr));
            }
            fn checkpoint(ptr: *anyopaque) !void {
                const self = cast(ptr);
                try self.lease.checkAt(@intCast(@import("antfly_platform").time.realtimeNs() / std.time.ns_per_s));
                var view = self.host.root.core.acquireSchemaView();
                defer if (view) |*value| value.release();
                if ((if (view) |value| value.tableSchema().version else 0) != self.schema_version) return error.CatalogGenerationChanged;
            }
            fn bind(ptr: *anyopaque, graph: cypher.catalog.Graph) !void {
                const self = cast(ptr);
                if (graph.incarnation != self.graph.incarnation or graph.revision != self.graph.revision) return error.CatalogGenerationChanged;
                try graph_catalog.validateOwners(self.host.alloc, self.host, graph);
                try checkpoint(ptr);
            }
            fn project(_: *anyopaque, _: A, node: *const cypher.value.Node) !?*const cypher.value.Node {
                return node;
            }
            fn projectRelationship(_: *anyopaque, _: A, edge: *const cypher.value.Relationship) !?*const cypher.value.Relationship {
                return edge;
            }
            fn normalize(ptr: *anyopaque, a: A, node: *const cypher.value.Node, defaults: bool) !*const cypher.value.Node {
                const self = cast(ptr);
                try checkpoint(ptr);
                return self.host.root.normalizeCypherNode(a, self.graph, node, defaults);
            }
            fn documentRelationships(ptr: *anyopaque, a: A, node: *const cypher.value.Node) ![]const *const cypher.value.Relationship {
                const self = cast(ptr);
                try checkpoint(ptr);
                return self.host.root.cypherDocumentRelationships(a, self.graph, node);
            }
            fn newIdentity(ptr: *anyopaque, a: A, table: []const u8, _: bool) !cypher.value.Identity {
                const self = cast(ptr);
                self.next_entity = try std.math.add(u64, self.next_entity, 1);
                return .{ .table = table, .key = try std.fmt.allocPrint(a, "cypher:{s}:{d}", .{ std.fmt.bytesToHex(self.scope.transaction_id.?, .lower), self.next_entity }) };
            }
            fn commit(ptr: *anyopaque, a: A, _: []const cypher.backend.Mutation) !void {
                const self = cast(ptr);
                try bind(ptr, self.graph);
                const plans = try cypher.mutations.build(a, self.graph, self.overlay.?);
                if (plans.len != 1 or !std.mem.eql(u8, plans[0].table, self.host.table)) return error.UndefinedGraphTable;
                const plan = plans[0];
                const writes = try a.alloc(types.TransactionWrite, plan.writes.items.len);
                for (plan.writes.items, writes) |write, *out| out.* = .{ .key = write.key, .value = write.value };
                const edges = try a.alloc(types.GraphEdgeWrite, plan.edges.items.len);
                for (plan.edges.items, edges) |edge, *out| out.* = .{ .edge_id = edge.edge_id, .index_name = edge.index_name, .source = edge.source, .target = edge.target, .edge_type = edge.edge_type, .metadata_json = edge.metadata_json, .weight = edge.weight, .created_at = edge.created_at, .updated_at = edge.updated_at };
                const removed = try a.alloc(types.GraphEdgeDelete, plan.removed_edges.items.len);
                for (plan.removed_edges.items, removed) |edge, *out| out.* = .{ .edge_id = edge.edge_id, .index_name = edge.index_name, .source = edge.source, .target = edge.target, .edge_type = edge.edge_type };
                const id = self.scope.transaction_id.?;
                const existing: ?types.TxnStatus = self.host.root.getTransactionStatus(id) catch |err| switch (err) {
                    error.TxnNotFound => null,
                    else => return err,
                };
                if (existing != null) {
                    self.scope.outcome = .unknown;
                    return error.CypherWriteRequestAlreadyUsed;
                }
                const now = @import("antfly_platform").time.realtimeNs();
                _ = try self.host.root.beginTransactionScoped(id, now, now, &.{self.host.table}, true, true, null);
                self.host.root.writeTransaction(id, .{
                    .schema_version = if (self.schema_version == 0) null else self.schema_version,
                    .normalized_graph_rows = true,
                    .writes = writes,
                    .deletes = plan.deletes.items,
                    .graph_writes = edges,
                    .graph_deletes = removed,
                    .range_guards = self.proofs,
                }) catch |err| {
                    self.host.root.abortTransaction(id, now +| 1) catch {
                        self.scope.outcome = .unknown;
                        return error.SqlMutationOutcomeUnknown;
                    };
                    self.scope.outcome = .aborted;
                    return err;
                };
                self.host.root.commitTransaction(id, now +| 1) catch |err| {
                    const status = self.host.root.getTransactionStatus(id) catch {
                        self.scope.outcome = .unknown;
                        return error.SqlMutationOutcomeUnknown;
                    };
                    self.scope.outcome = switch (status) {
                        .committed => .committed,
                        .aborted => .aborted,
                        .pending => .unknown,
                    };
                    if (status != .committed) return err;
                };
                self.scope.outcome = .committed;
            }
        };
        pub fn queryJson(self: Self, a: A, name: []const u8, body: []const u8, diagnostic: *cypher.compiler.Diagnostic, scope: *Scope) ![]u8 {
            var temporary = std.heap.ArenaAllocator.init(a);
            defer temporary.deinit();
            const owned = temporary.allocator();
            var request = try cypher.request.Request.parse(owned, body, .{});
            defer request.deinit();
            var compiled = try cypher.compiler.compile(owned, request.query, .{}, diagnostic);
            defer compiled.deinit();
            const graph = try graph_catalog.graph(owned, try graph_catalog.get(owned, self, name));
            try graph_catalog.validateOwners(owned, self, graph);
            try cypher.binder.validate(owned, graph, compiled.query);
            const writes = cypher.runtime.queryHasWrites(compiled.query) and !compiled.query.explain;
            if (writes and !self.writable) return error.ReadOnly;
            if (writes) {
                const encoded = request.write_request_id orelse return error.InvalidCypherWriteRequestId;
                scope.transaction_id = try parseId(encoded);
                const existing: ?types.TxnStatus = self.root.getTransactionStatus(scope.transaction_id.?) catch |err| switch (err) {
                    error.TxnNotFound => null,
                    else => return err,
                };
                if (existing != null) {
                    scope.outcome = .unknown;
                    return error.CypherWriteRequestAlreadyUsed;
                }
                try self.root.batch(.{ .activate_range_tracking = true });
            }
            const tables = try owned.alloc(PinnedAdapter.Table, graph.labels.len);
            var statement: Statement = .{ .host = self, .graph = graph, .lease = try self.root.acquireCypherReadAuthority(owned, "", "default"), .schema_version = if (self.root.core.schema) |schema| schema.version else 0, .scope = scope, .tables = tables };
            defer statement.lease.release();
            if (compiled.query.explain) {
                const metadata: cypher.backend.Backend = .{ .ptr = &statement, .vtable = &.{ .bind = metadataBind, .checkpoint = Statement.checkpoint, .scan = noScan, .get = noGet, .expand = noExpand } };
                var result = try cypher.runtime.execute(a, metadata, graph, &compiled, request.parameters, request.limits);
                defer result.deinit();
                return cypher.bounded_json.encode(a, result, cypher.runtime.max_response_bytes, null);
            }
            if (self.root.local_execution.row_policy_table_name == null) self.root.local_execution.row_policy_table_name = self.table;
            if (self.writable) _ = try self.root.lookup(owned, "", .{ .cypher_prepare = true }) else try self.root.waitForCurrentSyncLevel(.full_index);
            var fence = (try self.root.tryStatementReadFence()) orelse return error.CypherSnapshotNotReady;
            var fenced = true;
            defer if (fenced) fence.release();
            statement.pinned = .{ .source = self.root, .core = self.root.core, .cut = try self.root.captureCypherPinnedSnapshot(owned, &fence, graph, self.table) };
            defer statement.pinned.cut.deinit();
            for (graph.labels, statement.tables) |label, *table| table.* = .{ .name = label.table, .db = &statement.pinned };
            statement.adapter = .{ .tables = statement.tables, .ttl_now_ns = statement.pinned.cut.now, .authority = .{ .ptr = &statement, .bind = Statement.bind, .checkpoint = Statement.checkpoint, .project = Statement.project, .project_relationship = Statement.projectRelationship } };
            if (writes) {
                var txn = try self.root.core.store.beginReadTxn();
                defer txn.abort();
                statement.proofs = try tracking.capture(owned, &txn, "", "");
            }
            fence.release();
            fenced = false;
            var overlay: cypher.transaction.Overlay = .{ .lazy = true, .retain_scan_preimages = true, .base = statement.pinnedBackend(), .host = .{ .ptr = &statement, .new_identity = Statement.newIdentity, .commit = Statement.commit }, .max_entities = request.limits.intermediate_states, .max_anchors = request.limits.scanned_anchors, .max_edges = request.limits.explored_edges };
            statement.overlay = &overlay;
            var result = try cypher.runtime.execute(a, if (writes) overlay.backend() else statement.pinnedBackend(), graph, &compiled, request.parameters, request.limits);
            defer result.deinit();
            if (writes) {
                var response = try std.json.parseFromSliceLeaky(std.json.Value, owned, try std.json.Stringify.valueAlloc(owned, result, .{}), .{});
                const receipt = try std.json.parseFromSliceLeaky(std.json.Value, owned, try std.json.Stringify.valueAlloc(owned, .{ .transaction_id = std.fmt.bytesToHex(scope.transaction_id.?, .lower), .status = "committed", .outcome = "committed" }, .{}), .{});
                try response.object.put(owned, "commit", receipt);
                return cypher.bounded_json.encode(a, response, cypher.runtime.max_response_bytes, null);
            }
            return cypher.bounded_json.encode(a, result, cypher.runtime.max_response_bytes, .{ .ptr = &statement, .call = Statement.checkpoint });
        }
        pub fn statusJson(self: Self, a: A, encoded: []const u8) ![]u8 {
            const id = try parseId(encoded);
            const status = try self.root.getTransactionStatus(id);
            // Hosted indexing is driven by the caller. Report readiness
            // conservatively against the owner's current publication target;
            // a durable receipt alone does not certify derived visibility.
            var pending = false;
            var repair = false;
            if (status == .committed) {
                const stats = try self.root.stats(a);
                defer types.freeDBStats(a, stats);
                pending = !stats.indexes_available or
                    stats.enrichment.applied_sequence < stats.enrichment.target_sequence or
                    stats.resolution.catch_up_required or stats.promotion.catch_up_required;
                repair = stats.repair_degraded;
                for (stats.indexes) |index| {
                    pending = pending or index.replay_catch_up_required or index.catch_up_active;
                    repair = repair or index.index_repair_action_required;
                }
            }
            return cypher.bounded_json.encode(a, .{ .transaction_id = std.fmt.bytesToHex(id, .lower), .disposition = switch (status) {
                .committed => "committed",
                .aborted => "aborted",
                .pending => "outcome_unknown",
            }, .durable = status != .pending, .visibility_pending = pending, .visibility_repair_required = repair }, 4096, null);
        }
        fn metadataBind(ptr: *anyopaque, _: A, graph: cypher.catalog.Graph) !void {
            try Statement.bind(ptr, graph);
        }
    };
}
fn parseId(encoded: []const u8) !types.TxnId {
    if (encoded.len != 32) return error.InvalidCypherWriteRequestId;
    var id: types.TxnId = undefined;
    for (&id, 0..) |*byte, index| byte.* = std.fmt.parseInt(u8, encoded[index * 2 ..][0..2], 16) catch return error.InvalidCypherWriteRequestId;
    return id;
}
fn noScan(_: *anyopaque, _: A, _: []const u8, _: ?[]const u8, _: usize) !cypher.backend.Page(*const cypher.value.Node) {
    return error.CypherSnapshotRequired;
}
fn noGet(_: *anyopaque, _: A, _: cypher.value.Identity) !?*const cypher.value.Node {
    return error.CypherSnapshotRequired;
}
fn noExpand(_: *anyopaque, _: A, _: cypher.catalog.Relationship, _: cypher.value.Identity, _: cypher.ast.Direction, _: ?[]const u8, _: usize) !cypher.backend.Page(*const cypher.value.Relationship) {
    return error.CypherSnapshotRequired;
}
