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

//! Embedded host for the shared graph overlay and native durable coordinator.
const std = @import("std");
const h = @import("handles.zig");
const cypher = @import("cypher");
const reads = @import("cypher.zig");
const commit_api = @import("sql_commit.zig");
const d = h.antfly.capi_dependencies;
const contract = d.api_distributed_txn_contract;
const types = h.db_mod.types;
const A = std.mem.Allocator;
pub const Scope = struct {
    transaction_id: ?types.TxnId = null,
    outcome: enum { not_started, aborted, committed, unknown } = .not_started,
};
pub const Writer = struct {
    read: *reads.Read,
    overlay: ?*cypher.transaction.Overlay = null,
    guards: []const commit_api.GraphGuard,
    scope: *Scope,
    request_id: ?[]const u8,
    outcome: d.sql_catalog.MutationOutcome = .committed,
    fn cast(ptr: *anyopaque) *Writer {
        return @ptrCast(@alignCast(ptr));
    }
    pub fn host(self: *Writer) cypher.transaction.Host {
        return .{ .ptr = self, .new_identity = newIdentity, .commit = commit };
    }
    fn newIdentity(ptr: *anyopaque, a: A, table: []const u8, _: bool) !cypher.value.Identity {
        const self = cast(ptr);
        var nonce: [16]u8 = undefined;
        try (self.read.handle.db.backend_runtime.io() orelse return error.BackendRuntimeIoUnavailable).randomSecure(&nonce);
        return .{ .table = table, .key = try std.fmt.allocPrint(a, "cypher:{s}", .{std.fmt.bytesToHex(nonce, .lower)}) };
    }
    fn existed(ptr: *anyopaque, table: []const u8, key: []const u8) bool {
        const self = cast(ptr);
        for (self.read.snapshot.?.overlay.nodes.items) |node| {
            if (std.mem.eql(u8, node.value.identity.table, table) and std.mem.eql(u8, node.value.identity.key, key)) return true;
        }
        return false;
    }
    fn commit(ptr: *anyopaque, a: A, mutations: []const cypher.backend.Mutation) !void {
        const self = cast(ptr);
        try self.read.validate();
        const plans = try cypher.mutations.build(a, self.read.graph, self.overlay.?, mutations, .{ .ptr = self, .existed = existed });
        const requests = try a.alloc(contract.TableCommitRequest, plans.len);
        for (plans, requests) |plan, *request| {
            const guard = for (self.guards) |entry| {
                if (std.mem.eql(u8, entry.table_name, plan.table)) break entry;
            } else return error.CatalogGenerationChanged;
            const writes = try a.alloc(types.TransactionWrite, plan.writes.items.len);
            for (plan.writes.items, writes) |write, *out| out.* = .{ .key = write.key, .value = write.value };
            const edges = try a.alloc(types.GraphEdgeWrite, plan.edges.items.len);
            for (plan.edges.items, edges) |edge, *out| out.* = .{ .edge_id = edge.edge_id, .index_name = edge.index_name, .source = edge.source, .target = edge.target, .edge_type = edge.edge_type, .metadata_json = edge.metadata_json };
            const removed = try a.alloc(types.GraphEdgeDelete, plan.removed_edges.items.len);
            for (plan.removed_edges.items, removed) |edge, *out| out.* = .{ .edge_id = edge.edge_id, .index_name = edge.index_name, .source = edge.source, .target = edge.target, .edge_type = edge.edge_type };
            request.* = .{ .normalized_graph_rows = true, .table_name = plan.table, .schema_version = if (guard.schema_version == 0) null else guard.schema_version, .writes = writes, .deletes = plan.deletes.items, .graph_writes = edges, .graph_deletes = removed };
        }
        var id: types.TxnId = undefined;
        if (self.request_id) |encoded| id = try parseId(encoded) else try (self.read.handle.db.backend_runtime.io() orelse return error.BackendRuntimeIoUnavailable).randomSecure(&id);
        self.scope.transaction_id = id;
        var out_id: ?types.TxnId = null;
        self.outcome = self.commitPrepared(a, requests, id, &out_id) catch |err| {
            const status: ?h.transactions_mod.TxnStatus = self.read.handle.db.getTransactionStatus(id) catch |status_error| switch (status_error) {
                error.TxnNotFound => null,
                else => {
                    self.scope.outcome = .unknown;
                    return error.SqlMutationOutcomeUnknown;
                },
            };
            self.scope.outcome = if (err == error.CypherWriteRequestAlreadyUsed) .unknown else if (status) |value| switch (value) {
                .committed => .committed,
                .aborted => .aborted,
                .pending => .unknown,
            } else if (err == error.SqlMutationOutcomeUnknown) .unknown else .not_started;
            return err;
        };
        self.scope.outcome = .committed;
    }
    fn commitPrepared(self: *Writer, a: A, requests: []const contract.TableCommitRequest, id: types.TxnId, out_id: *?types.TxnId) !d.sql_catalog.MutationOutcome {
        var coordinated = false;
        var first: ?*h.db_mod.DB = null;
        var first_name: []const u8 = "";
        for (requests) |request| {
            const db = try @import("tables.zig").get(self.read.handle, request.table_name);
            if (try db.getSchemaJson(a)) |schema| if (try d.api_relational_integrity_commit.requiresCoordination(a, schema)) {
                coordinated = true;
                first = db;
                first_name = request.table_name;
            };
        }
        if (!coordinated) return commit_api.commitWithGraphGuards(self.read.handle, requests, self.guards, id, out_id);
        var adapter = @import("sql.zig").Adapter(h.antfly){ .handle = self.read.handle, .db = first.?, .table_name = first_name };
        const metadata = try adapter.localCatalog(a, first.?.core.schema.?.version);
        var prepared = try d.api_relational_integrity_commit.prepareWithRepairAdmission(self.read.handle.alloc, adapter.localSource(), metadata.table, metadata.range, requests, adapter.repairAdmission(), .{});
        defer prepared.deinit();
        return commit_api.commitWithGraphGuards(self.read.handle, prepared.tables, self.guards, id, out_id);
    }
};
pub fn parseId(encoded: []const u8) !types.TxnId {
    if (encoded.len != 32) return error.InvalidCypherWriteRequestId;
    var id: types.TxnId = undefined;
    for (&id, 0..) |*byte, index| byte.* = std.fmt.parseInt(u8, encoded[index * 2 ..][0..2], 16) catch return error.InvalidCypherWriteRequestId;
    return id;
}
