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

//! Shared authenticated Cypher entry point for transports and SQL hosts.
const std = @import("std");
const cypher = @import("cypher");
const server_mod = @import("http_server.zig");
const local = @import("antfly_local_sources");
const domain = local.system_catalog_domain;
const operation = local.api_operation;
const port = local.storage_statement_read_fence;

pub const WriteOutcome = enum { not_started, aborted, committed, unknown };

/// Shared operator ceilings for text queries and SQL GRAPH_TABLE execution.
pub fn limits(server: *const server_mod.ApiHttpServer) cypher.runtime.Limits {
    const ceilings = server.cfg.graph_execution_limits;
    return .{ .memory_bytes = ceilings.max_retained_state_bytes, .scanned_anchors = ceilings.max_scanned_anchors, .explored_edges = ceilings.max_explored_edges, .intermediate_states = ceilings.max_intermediate_states };
}

pub fn execute(a: std.mem.Allocator, server: *server_mod.ApiHttpServer, identity: *?server_mod.AuthenticatedIdentity, context: operation.RequestContext, name: []const u8, body: []const u8, diagnostic: *cypher.compiler.Diagnostic, write_outcome: *WriteOutcome) ![]u8 {
    try context.ensureActive();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var request = try cypher.request.Request.parse(scratch, body, limits(server));
    defer request.deinit();
    var compiled = try cypher.compiler.compile(scratch, request.query, .{}, diagnostic);
    defer compiled.deinit();
    const writes = cypher.runtime.queryHasWrites(compiled.query);
    const role_revision = if (identity.*) |principal| if (principal.live_user_manager) |manager| @as(?u64, manager.rowPolicyRoleRevision()) else null else null;
    if (identity.*) |principal| {
        if (!server_mod.permissionsAllow(principal.permissions, .graph, name, .read) or (writes and !compiled.query.explain and !server_mod.permissionsAllow(principal.permissions, .graph, name, .write))) return error.Forbidden;
    }
    var execution = try server.acquireSqlExecution(writes and !compiled.query.explain);
    defer execution.release();
    const bytes = try server.source.systemCatalog(scratch, context, .{ .read = .{ .kind = .graph, .name = name } });
    const state = try std.json.parseFromSliceLeaky(domain.State, scratch, bytes, .{ .allocate = .alloc_always });
    const record = state.find(.graph, 0, name) orelse return error.CatalogNotFound;
    const graph = try domain.graphForExecution(scratch, record);
    const logical = try cypher.catalog.parseDefinition(scratch, name, graph.incarnation, graph.revision, record.source_json);
    var sql: @import("sql_execution.zig").Adapter = .{ .server = server, .identity = identity, .context = context };
    const resolver = sql.backend();
    var tables: std.ArrayList(port.GraphTable) = .empty;
    for (logical.labels, @constCast(graph.labels), 0..) |label, *physical, label_index| {
        const target = try domain.graphLabelTarget(state, record.graph_tables[label_index], label);
        physical.logical_table = target.table;
        physical.database = target.database;
        physical.namespace = target.namespace;
        const table = try resolver.vtable.resolve(resolver.ptr, scratch, .{ .database = target.database, .namespace = target.namespace, .table = target.table }, if (writes and !compiled.query.explain) .read_write else .read);
        if (table.id != physical.table_id or !std.mem.eql(u8, table.physical_name, physical.table)) return error.CatalogGenerationChanged;
        var duplicate = false;
        for (tables.items) |existing| if (existing.table_id == table.id) {
            duplicate = true;
            break;
        };
        if (!duplicate) try tables.append(scratch, .{ .name = table.physical_name, .table_id = table.id, .schema_version = table.schema_version, .principal_proof = (try sql.rowPolicyProof(scratch, table)) orelse "", .database = target.database, .filter_query_json = (try server_mod.resolveEffectiveRowFilterJson(scratch, identity.*, table.physical_name)) orelse "" });
    }
    try cypher.binder.validate(scratch, graph, compiled.query);
    var metadata: Metadata = .{ .context = context };
    var provider = metadata.backend();
    var read: ?port.GraphRead = null;
    defer if (read) |value| value.deinit();
    var client: cypher.rpc.Client = undefined;
    var channel: AuthorityChannel = .{ .read_owner = &read, .server = server, .identity = identity, .context = context, .name = name, .record_id = record.id, .tables = tables.items, .writes = writes and !compiled.query.explain, .role_revision = role_revision };
    if (!compiled.query.explain) {
        const source = server.table_reads orelse return error.CypherSnapshotRequired;
        var nonce: [32]u8 = undefined;
        try server.embedding_provider_runtime.io.randomSecure(&nonce);
        const now = @import("antfly_platform").time.realtimeNs();
        const normalized = try context.platformDeadline();
        if (writes) {
            const write_source = server.table_writes orelse return error.CypherMutationsUnavailable;
            for (tables.items) |table| try write_source.activateRangeTracking(scratch, table.name, context);
        }
        read = try source.openCypherStatement(a, .{ .catalog_json = try std.json.Stringify.valueAlloc(scratch, record, .{}), .tables = tables.items, .cut_id = std.fmt.bytesToHex(nonce, .lower), .expires_ms = now / std.time.ns_per_ms + 30_000, .ttl_now_ns = now, .deadline_ns = normalized.deadline_ns, .cancellation = normalized.cancellation });
        channel.read = &read.?;
        client = .{ .channel = .{ .ptr = &channel, .call = AuthorityChannel.invoke, .checkpoint = AuthorityChannel.checkpoint }, .incarnation = graph.incarnation, .revision = graph.revision };
        provider = client.backend();
    }
    var writer: @import("graph_write_execution.zig").Provider = .{ .server = server, .context = context, .identity = identity, .request_id = request.write_request_id, .graph = graph, .tables = tables.items, .release_read = AuthorityChannel.releaseRead, .channel = .{ .ptr = &channel, .call = AuthorityChannel.invoke, .checkpoint = AuthorityChannel.checkpoint } };
    errdefer if (writer.receipt) |receipt| {
        write_outcome.* = switch (receipt.status) {
            .aborted => .aborted,
            .unknown => .unknown,
            .committed, .committed_pending, .committed_repair_required => .committed,
        };
    };
    var overlay: cypher.transaction.Overlay = .{ .base = provider, .host = writer.host(), .max_entities = request.limits.intermediate_states, .max_anchors = request.limits.scanned_anchors, .max_edges = request.limits.explored_edges };
    writer.overlay = &overlay;
    if (writes and !compiled.query.explain) provider = overlay.backend();
    var result = try cypher.runtime.execute(a, provider, graph, &compiled, request.parameters, request.limits);
    defer result.deinit();
    // After a durable write decision, revocation must not turn a committed
    // response into an apparent rejection that invites statement replay.
    if (!writes or compiled.query.explain) try channel.validateAuthority(true);
    if (writer.receipt) |write_receipt| {
        if (write_receipt.status == .unknown) result.complete = false;
        const encoded = try std.json.Stringify.valueAlloc(scratch, result, .{});
        var json = try std.json.parseFromSliceLeaky(std.json.Value, scratch, encoded, .{});
        const receipt = try std.json.parseFromSliceLeaky(std.json.Value, scratch, try std.json.Stringify.valueAlloc(scratch, .{ .transaction_id = std.fmt.bytesToHex(writer.transaction_id.?, .lower), .status = write_receipt.status, .outcome = write_receipt.outcome }, .{}), .{});
        try json.object.put(scratch, "commit", receipt);
        return cypher.bounded_json.encode(a, json, cypher.runtime.max_response_bytes, null);
    }
    const encoded = try cypher.bounded_json.encode(a, result, cypher.runtime.max_response_bytes, .{ .ptr = &channel, .call = AuthorityChannel.checkpoint });
    errdefer a.free(encoded);
    if (!writes or compiled.query.explain) try channel.validateAuthority(true);
    return encoded;
}
pub const AuthorityChannel = struct {
    server: *server_mod.ApiHttpServer,
    identity: *?server_mod.AuthenticatedIdentity,
    context: operation.RequestContext,
    name: []const u8,
    record_id: u64,
    tables: []const port.GraphTable,
    writes: bool,
    read: ?*port.GraphRead = null,
    read_owner: ?*?port.GraphRead = null,
    last_authority_check: ?u64 = null,
    role_revision: ?u64 = null,
    fn releaseRead(ptr: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.read = null;
        if (self.read_owner) |owner| {
            const retained = owner.*;
            owner.* = null;
            if (retained) |read| read.deinit();
        }
    }
    pub fn checkpoint(ptr: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try self.validateAuthority(false);
    }
    fn validateAuthority(self: *@This(), force: bool) !void {
        try self.context.ensureActive();
        const now = @import("antfly_platform").time.monotonicNs();
        if (!force) if (self.last_authority_check) |last| if (now -| last < 10 * std.time.ns_per_ms) return;
        if (self.role_revision) |revision| {
            const principal = self.identity.* orelse return error.Forbidden;
            const manager = principal.live_user_manager orelse return error.Forbidden;
            if (manager.rowPolicyRoleRevision() != revision) return error.Forbidden;
        }
        if (!try server_mod.resourcePermissionCurrentlyAllowed(self.identity.*, .graph, self.name, .read)) return error.Forbidden;
        if (self.writes and !try server_mod.resourcePermissionCurrentlyAllowed(self.identity.*, .graph, self.name, .write)) return error.Forbidden;
        for (self.tables) |table| {
            if (!try server_mod.tablePermissionCurrentlyAllowed(self.identity.*, table.name, .read)) return error.Forbidden;
            if (self.writes and !try server_mod.tablePermissionCurrentlyAllowed(self.identity.*, table.name, .write)) return error.Forbidden;
        }
        var arena = std.heap.ArenaAllocator.init(self.server.alloc);
        defer arena.deinit();
        const a = arena.allocator();
        const bytes = try self.server.source.systemCatalog(a, self.context, .{ .read = .{ .kind = .graph, .name = self.name } });
        const state = try std.json.parseFromSliceLeaky(domain.State, a, bytes, .{});
        const record = state.find(.graph, 0, self.name) orelse return error.CatalogGenerationChanged;
        if (record.id != self.record_id) return error.CatalogGenerationChanged;
        self.last_authority_check = now;
    }
    pub fn invoke(ptr: *anyopaque, a: std.mem.Allocator, body: []const u8) ![]u8 {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try self.validateAuthority(true);
        var request = try std.json.parseFromSlice(cypher.rpc.Request, a, body, .{});
        defer request.deinit();
        var prepared_body: ?[]u8 = null;
        defer if (prepared_body) |encoded| a.free(encoded);
        if (request.value.operation == .search) {
            var scratch = std.heap.ArenaAllocator.init(a);
            defer scratch.deinit();
            const owned = scratch.allocator();
            const parameters = try cypher.wire.parameter(owned, request.value.search orelse return error.InvalidQueryRequest);
            var json = try std.json.parseFromSliceLeaky(std.json.Value, owned, try std.json.Stringify.valueAlloc(owned, parameters, .{}), .{});
            if (json != .object) return error.InvalidQueryRequest;
            if (json.object.get("query")) |query| {
                _ = json.object.swapRemove("query");
                const mode = json.object.get("mode") orelse std.json.Value{ .string = "text" };
                _ = json.object.swapRemove("mode");
                if (mode != .string) return error.InvalidQueryRequest;
                if (std.mem.eql(u8, mode.string, "text") or std.mem.eql(u8, mode.string, "hybrid")) {
                    var text: std.json.ObjectMap = .empty;
                    try text.put(owned, "query", query);
                    try json.object.put(owned, "full_text_search", .{ .object = text });
                }
                if (std.mem.eql(u8, mode.string, "semantic") or std.mem.eql(u8, mode.string, "hybrid")) try json.object.put(owned, "semantic_search", query) else if (!std.mem.eql(u8, mode.string, "text")) return error.InvalidQueryRequest;
            }
            const scope = server_mod.ApiHttpServer.queryEmbeddingSecurityScope(self.identity.*);
            var resolver = self.server.semanticStatusResolver(scope.domain, scope.value);
            const normalized = try self.context.platformDeadline();
            resolver.query_embedding_deadline_ns = normalized.deadline_ns;
            resolver.query_cancellation = normalized.cancellation;
            var parsed = try local.api_query.parsePublicQueryRequestWithDeadline(owned, resolver.iface(), request.value.table, try std.json.Stringify.valueAlloc(owned, json, .{}), normalized.deadline_ns);
            defer parsed.deinit(owned);
            request.value.prepared_search_json = try local.api_local_query_contract.encodeQueryRequest(owned, parsed.req);
            prepared_body = try std.json.Stringify.valueAlloc(a, request.value, .{});
        }
        const result = try (self.read orelse return error.CypherSnapshotRequired).invoke(a, prepared_body orelse body);
        errdefer a.free(result);
        try self.validateAuthority(true);
        return result;
    }
};
const Metadata = struct {
    context: operation.RequestContext,
    fn backend(self: *@This()) cypher.backend.Backend {
        return .{ .ptr = self, .vtable = &.{ .bind = bind, .checkpoint = checkpoint, .scan = scan, .get = get, .expand = expand } };
    }
    fn bind(ptr: *anyopaque, _: std.mem.Allocator, _: cypher.catalog.Graph) !void {
        try checkpoint(ptr);
    }
    fn checkpoint(ptr: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try self.context.ensureActive();
    }
    fn scan(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: ?[]const u8, _: usize) !cypher.backend.Page(*const cypher.value.Node) {
        return error.CypherSnapshotRequired;
    }
    fn get(_: *anyopaque, _: std.mem.Allocator, _: cypher.value.Identity) !?*const cypher.value.Node {
        return error.CypherSnapshotRequired;
    }
    fn expand(_: *anyopaque, _: std.mem.Allocator, _: cypher.catalog.Relationship, _: cypher.value.Identity, _: cypher.ast.Direction, _: ?[]const u8, _: usize) !cypher.backend.Page(*const cypher.value.Relationship) {
        return error.CypherSnapshotRequired;
    }
};
pub fn httpStatus(err: anyerror) u16 {
    return switch (err) {
        error.Forbidden, error.RowPolicyDenied, error.RowPolicyAuthenticationRequired => 403,
        error.CatalogNotFound, error.TableNotFound, error.IndexNotFound => 404,
        error.CatalogGenerationChanged, error.SchemaVersionChanged, error.RowPolicyCatalogChanged, error.CypherTransactionConflict, error.CypherWriteRequestAlreadyUsed => 409,
        error.CypherSnapshotRequired, error.CypherSnapshotNotReady, error.ReadUnavailable, error.NotLeader, error.LeaderUnavailable, error.ReadIndexTimeout, error.TopologyChanged, error.StorageReadTemporarilyUnavailable, error.RetainedReadRestartRequired, error.GroupLeaderUnavailable, error.Backpressured, error.ConnectionResetByPeer, error.ConnectionRefused, error.ConnectionTimedOut, error.Timeout => 503,
        error.OutOfMemory, error.CypherResponseTooLarge, error.CypherMemoryBudgetExceeded, error.CypherAnchorBudgetExceeded, error.CypherEdgeBudgetExceeded, error.CypherStateBudgetExceeded, error.SqlWriteCapacityUnavailable => 429,
        error.DeadlineExceeded => 408,
        error.Canceled => 499,
        else => 400,
    };
}

/// Stable codes remain machine-readable; these messages explain the action
/// available to the query editor without exposing owner or policy internals.
pub fn errorMessage(err: anyerror, outcome: WriteOutcome) []const u8 {
    if (outcome == .unknown)
        return "The write outcome is unknown. Check its transaction status before submitting another write; do not replay this statement.";
    if (outcome == .committed)
        return "The write committed, but its response could not be delivered completely. Check its transaction status; do not replay this statement.";
    return switch (err) {
        error.CypherWriteRequestAlreadyUsed => "This write request ID has already been admitted. Check its transaction status instead of replaying the statement.",
        error.Forbidden, error.RowPolicyDenied, error.RowPolicyAuthenticationRequired => "The current principal cannot execute this query against the graph or its mapped tables.",
        error.CatalogNotFound => "This graph no longer exists. Refresh the graph catalog and select an available graph.",
        error.TableNotFound, error.IndexNotFound => "A mapped table or graph index is unavailable. Refresh the graph definition and check its mappings.",
        error.CatalogGenerationChanged, error.SchemaVersionChanged, error.RowPolicyCatalogChanged => "The graph catalog, table schema, or row policy changed during execution. Refresh the graph and run a new query.",
        error.CypherTransactionConflict => "The write was aborted because its read set changed. Run a new statement with a new write request ID.",
        error.CypherSnapshotRequired, error.CypherSnapshotNotReady, error.ReadUnavailable, error.NotLeader, error.LeaderUnavailable, error.ReadIndexTimeout, error.TopologyChanged, error.StorageReadTemporarilyUnavailable, error.RetainedReadRestartRequired, error.GroupLeaderUnavailable, error.Backpressured, error.ConnectionResetByPeer, error.ConnectionRefused, error.ConnectionTimedOut, error.Timeout => "A coherent graph read is temporarily unavailable. Check graph readiness and try again after the owners become available.",
        error.CypherValueDepthExceeded => "A computed or input value exceeds 64 nesting levels. Reduce nested list or map construction.",
        error.CypherResponseTooLarge => "The result exceeds the response limit. Reduce RETURN data or add a smaller LIMIT.",
        error.OutOfMemory, error.CypherMemoryBudgetExceeded, error.CypherAnchorBudgetExceeded, error.CypherEdgeBudgetExceeded, error.CypherStateBudgetExceeded => "The query exceeded its execution budget. Narrow the starting nodes, bound path lengths, or reduce intermediate results.",
        error.SqlWriteCapacityUnavailable => "Write capacity is temporarily unavailable. Wait for active transactions to finish before submitting a new write.",
        error.DeadlineExceeded => "The query exceeded its deadline. Narrow the query or increase the allowed deadline.",
        error.Canceled => "The query was canceled.",
        error.MissingCypherParameter, error.InvalidCypherParameters => "Provide every query parameter using the supported parameter object or typed parameter format.",
        else => @errorName(err),
    };
}

test "Cypher write releases retained owner reads before commit and cleanup is idempotent" {
    const Fixture = struct {
        releases: usize = 0,
        fn invoke(_: *anyopaque, _: std.mem.Allocator, _: []const u8) ![]u8 {
            return error.TestUnexpectedResult;
        }
        fn release(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.releases += 1;
        }
    };
    var fixture: Fixture = .{};
    var read: ?port.GraphRead = .{ .ptr = &fixture, .vtable = &.{ .invoke = Fixture.invoke, .release = Fixture.release } };
    var channel: AuthorityChannel = undefined;
    channel.read = &read.?;
    channel.read_owner = &read;
    AuthorityChannel.releaseRead(&channel);
    AuthorityChannel.releaseRead(&channel);
    try std.testing.expect(read == null);
    try std.testing.expect(channel.read == null);
    try std.testing.expectEqual(@as(usize, 1), fixture.releases);
}
