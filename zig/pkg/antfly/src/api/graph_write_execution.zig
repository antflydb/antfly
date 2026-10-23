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

//! Compiles a graph overlay into the existing native distributed transaction.
const std = @import("std");
const cypher = @import("cypher");
const local = @import("antfly_local_sources");
const server_mod = @import("http_server.zig");
const types = local.storage_db_types;
const contract = local.api_local_transaction_contract;
const port = local.storage_statement_read_fence;
const A = std.mem.Allocator;
pub const Provider = struct {
    server: *server_mod.ApiHttpServer,
    context: local.api_operation.RequestContext,
    identity: *?server_mod.AuthenticatedIdentity,
    receipt: ?@import("graph_write_receipt.zig").Result = null,
    graph: cypher.catalog.Graph,
    tables: []const port.GraphTable,
    channel: cypher.rpc.Channel,
    release_read: ?*const fn (*anyopaque) void = null,
    overlay: ?*cypher.transaction.Overlay = null,
    transaction_id: ?types.TxnId = null,
    request_id: ?[]const u8 = null,
    outcome: ?contract.CommitOutcome = null,
    pub fn host(self: *Provider) cypher.transaction.Host {
        return .{ .ptr = self, .new_identity = newIdentity, .commit = commit };
    }
    fn cast(ptr: *anyopaque) *Provider {
        return @ptrCast(@alignCast(ptr));
    }
    fn newIdentity(ptr: *anyopaque, a: A, table: []const u8, _: bool) !cypher.value.Identity {
        const self = cast(ptr);
        try self.context.ensureActive();
        var nonce: [16]u8 = undefined;
        try self.server.embedding_provider_runtime.io.randomSecure(&nonce);
        return .{ .table = table, .key = try std.fmt.allocPrint(a, "cypher:{s}", .{std.fmt.bytesToHex(nonce, .lower)}) };
    }
    const Plan = struct {
        table: port.GraphTable,
        writes: std.ArrayList(types.TransactionWrite) = .empty,
        deletes: std.ArrayList([]const u8) = .empty,
        predicates: std.ArrayList(types.TransactionVersionPredicate) = .empty,
        edges: std.ArrayList(types.GraphEdgeWrite) = .empty,
        removed_edges: std.ArrayList(types.GraphEdgeDelete) = .empty,
        guards: std.ArrayList(local.api_range_read_guards.OwnerRangeProof) = .empty,
    };
    fn plan(plans: []Plan, table: []const u8) !*Plan {
        for (plans) |*entry| if (std.mem.eql(u8, entry.table.name, table)) return entry;
        return error.CypherSearchTableOutsideGraph;
    }
    fn commit(ptr: *anyopaque, a: A, _: []const cypher.backend.Mutation) !void {
        const self = cast(ptr);
        try self.context.ensureActive();
        const overlay = self.overlay orelse return error.CypherTransactionRequired;
        const plans = try a.alloc(Plan, self.tables.len);
        for (self.tables, plans) |table, *entry| entry.* = .{ .table = table };
        const body = try std.json.Stringify.valueAlloc(a, cypher.rpc.Request{ .operation = .observe_ranges, .graph_incarnation = self.graph.incarnation, .graph_revision = self.graph.revision }, .{});
        const bytes = try self.channel.call(self.channel.ptr, a, body);
        const owners = try std.json.parseFromSliceLeaky([]const local.storage_db_cypher_write_observation.Owner, a, bytes, .{});
        for (owners) |owner| {
            const entry = try plan(plans, owner.table);
            if (entry.table.table_id != owner.fence.table_id) return error.CatalogGenerationChanged;
            try entry.predicates.appendSlice(a, owner.predicates);
            try entry.guards.append(a, .{ .fence = owner.fence, .proofs = owner.proofs });
        }
        const postimages = try cypher.mutations.build(a, self.graph, overlay);
        for (postimages) |postimage| {
            const entry = try plan(plans, postimage.table);
            for (postimage.writes.items) |write| try entry.writes.append(a, .{ .key = write.key, .value = write.value });
            try entry.deletes.appendSlice(a, postimage.deletes.items);
            for (postimage.edges.items) |edge| try entry.edges.append(a, .{ .edge_id = edge.edge_id, .index_name = edge.index_name, .source = edge.source, .target = edge.target, .edge_type = edge.edge_type, .metadata_json = edge.metadata_json, .weight = edge.weight, .created_at = edge.created_at, .updated_at = edge.updated_at });
            for (postimage.removed_edges.items) |edge| try entry.removed_edges.append(a, .{ .edge_id = edge.edge_id, .index_name = edge.index_name, .source = edge.source, .target = edge.target, .edge_type = edge.edge_type });
        }
        const requests = try a.alloc(contract.TableCommitRequest, plans.len);
        for (plans, requests) |entry, *request| {
            if (entry.guards.items.len == 0) return error.SqlRangeTrackingRequired;
            const proof = (try self.server.rowPolicyWriteProof(a, self.context, entry.table.table_id, entry.table.name, entry.table.database, if (entry.table.schema_version == 0) null else entry.table.schema_version)) orelse "";
            request.* = .{ .normalized_graph_rows = true, .table_name = entry.table.name, .schema_version = if (entry.table.schema_version == 0) null else entry.table.schema_version, .writes = entry.writes.items, .deletes = entry.deletes.items, .predicates = entry.predicates.items, .graph_writes = entry.edges.items, .graph_deletes = entry.removed_edges.items, .range_guards = entry.guards.items, .row_policy_principal_proof = proof, .row_policy_database = if (proof.len == 0) "" else entry.table.database, .row_policy_admitted_at_seconds = if (proof.len == 0) 0 else @intCast(@import("antfly_platform").time.realtimeNs() / std.time.ns_per_s) };
        }
        // All result values, normalized postimages and conflict proofs are
        // owned now. Retaining resident read leases across Raft prepare can
        // deadlock a queued owner refresh against this transaction's apply.
        // Native predicates/range proofs validate the cut after its release.
        if (self.release_read) |release| release(self.channel.ptr);
        var id: types.TxnId = undefined;
        if (self.request_id) |encoded| id = try @import("distributed_txn.zig").parseTxnIdHex(encoded) else try self.server.embedding_provider_runtime.io.randomSecure(&id);
        self.transaction_id = id;
        const receipt = try @import("graph_write_receipt.zig").execute(self.server, self.identity.*, self.context, id, requests, a);
        self.receipt = receipt;
        self.outcome = receipt.outcome;
        if (receipt.status == .aborted) return receipt.failure orelse error.CypherTransactionConflict;
    }
};
