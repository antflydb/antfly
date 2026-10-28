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
    var sql: @import("sql_execution.zig").Adapter = .{ .server = server, .identity = identity, .context = context };
    var tables: std.ArrayList(port.GraphTable) = .empty;
    var targets: std.ArrayList(domain.Target) = .empty;
    const positions = try scratch.alloc(usize, graph.labels.len);
    for (graph.labels, positions, 0..) |label, *position, label_index| {
        var logical = label;
        logical.table = label.logical_table;
        const target = try domain.graphLabelTarget(state, record.graph_tables[label_index], logical);
        position.* = for (targets.items, 0..) |existing, index| {
            if (std.mem.eql(u8, existing.database, target.database) and std.mem.eql(u8, existing.namespace, target.namespace) and std.mem.eql(u8, existing.table, target.table)) break index;
        } else append: {
            const index = targets.items.len;
            try targets.append(scratch, target);
            break :append index;
        };
    }
    const resolved = try sql.resolveMany(scratch, targets.items, if (writes and !compiled.query.explain) .read_write else .read);
    for (@constCast(graph.labels), positions) |*physical, position| {
        const target = targets.items[position];
        const table = resolved[position];
        physical.logical_table = target.table;
        physical.database = target.database;
        physical.namespace = target.namespace;
        if (table.id != physical.table_id or !std.mem.eql(u8, table.physical_name, physical.table)) return error.CatalogGenerationChanged;
    }
    for (resolved, targets.items) |table, target| {
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
    const needed = try cypher.dependencies.required(scratch, compiled.query, graph);
    var read_tables: std.ArrayList(port.GraphTable) = .empty;
    for (tables.items) |table| if (cypher.dependencies.contains(needed, table.name)) try read_tables.append(scratch, table);
    if (!compiled.query.explain and read_tables.items.len != 0) {
        const source = server.table_reads orelse return error.CypherSnapshotRequired;
        var nonce: [32]u8 = undefined;
        try server.embedding_provider_runtime.io.randomSecure(&nonce);
        const now = @import("antfly_platform").time.realtimeNs();
        const normalized = try context.platformDeadline();
        if (writes) {
            const write_source = server.table_writes orelse return error.CypherMutationsUnavailable;
            for (read_tables.items) |table| try write_source.activateRangeTracking(scratch, table.name, context);
        }
        read = try source.openCypherStatement(a, .{ .catalog_json = try std.json.Stringify.valueAlloc(scratch, record, .{}), .tables = read_tables.items, .partial_graph = needed != null, .cut_id = std.fmt.bytesToHex(nonce, .lower), .expires_ms = now / std.time.ns_per_ms + 30_000, .ttl_now_ns = now, .deadline_ns = normalized.deadline_ns, .cancellation = normalized.cancellation });
        channel.read = &read.?;
        client = .{ .channel = .{ .ptr = &channel, .call = AuthorityChannel.invoke, .typed = AuthorityChannel.invokeTyped, .checkpoint = AuthorityChannel.checkpoint }, .incarnation = graph.incarnation, .revision = graph.revision };
        provider = client.backend();
    }
    var writer: @import("graph_write_execution.zig").Provider = .{ .server = server, .context = context, .identity = identity, .request_id = request.write_request_id, .graph = graph, .tables = read_tables.items, .release_read = AuthorityChannel.releaseRead, .channel = .{ .ptr = &channel, .call = AuthorityChannel.invoke, .checkpoint = AuthorityChannel.checkpoint } };
    errdefer if (writer.receipt) |receipt| {
        write_outcome.* = switch (receipt.status) {
            .aborted => .aborted,
            .unknown => .unknown,
            .committed, .committed_pending, .committed_repair_required => .committed,
        };
    };
    var overlay: cypher.transaction.Overlay = .{ .lazy = true, .retain_scan_preimages = true, .base = provider, .host = writer.host(), .max_entities = request.limits.intermediate_states, .max_anchors = request.limits.scanned_anchors, .max_edges = request.limits.explored_edges };
    writer.overlay = &overlay;
    if (writes and !compiled.query.explain) provider = overlay.backend();
    var result = execution: {
        if (!writes) try channel.validateAuthority(true);
        channel.guarded_operation = !writes;
        defer channel.guarded_operation = false;
        break :execution try cypher.runtime.execute(a, provider, graph, &compiled, request.parameters, request.limits);
    };
    defer result.deinit();
    // After a durable write decision, revocation must not turn a committed
    // response into an apparent rejection that invites statement replay.
    if (!writes or compiled.query.explain) try channel.validateAuthority(true);
    if (writer.receipt) |write_receipt| {
        if (write_receipt.status == .unknown) result.complete = false;
        return cypher.bounded_json.encode(a, result.withCommit(.{ .transaction_id = std.fmt.bytesToHex(writer.transaction_id.?, .lower), .status = write_receipt.status, .outcome = write_receipt.outcome }), cypher.runtime.max_response_bytes, null);
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
    catalog_guard: ?struct { ptr: *anyopaque, call: *const fn (*anyopaque, std.mem.Allocator) anyerror!void } = null,
    /// Read statements and SQL pages have authoritative guards at both ends.
    /// Internal RPCs share that boundary; long operations still refresh guards.
    guarded_operation: bool = false,
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
    pub fn validateAuthority(self: *@This(), force: bool) !void {
        try self.context.ensureActive();
        const now = @import("antfly_platform").time.monotonicNs();
        if (self.role_revision) |revision| {
            const principal = self.identity.* orelse return error.Forbidden;
            const manager = principal.live_user_manager orelse return error.Forbidden;
            if (manager.rowPolicyRoleRevision() != revision) return error.Forbidden;
        }
        if (!force or self.guarded_operation) if (self.last_authority_check) |last| if (now -| last < 10 * std.time.ns_per_ms) return;
        var arena = std.heap.ArenaAllocator.init(self.server.alloc);
        defer arena.deinit();
        const a = arena.allocator();
        if (self.catalog_guard) |guard| {
            try guard.call(guard.ptr, a);
            self.last_authority_check = @import("antfly_platform").time.monotonicNs();
            return;
        }
        var checks: std.ArrayList(server_mod.GraphPermissionCheck) = .empty;
        try checks.append(a, .{ .resource_type = .graph, .resource = self.name, .permission_type = .read });
        if (self.writes) try checks.append(a, .{ .resource_type = .graph, .resource = self.name, .permission_type = .write });
        for (self.tables) |table| {
            try checks.append(a, .{ .resource_type = .table, .resource = table.name, .permission_type = .read });
            if (self.writes) try checks.append(a, .{ .resource_type = .table, .resource = table.name, .permission_type = .write });
        }
        if (!try server_mod.resourcePermissionsCurrentlyAllowed(a, self.identity.*, checks.items)) return error.Forbidden;
        if (self.server.source.vtable.supports_graph_authority_probe) {
            const bytes = try self.server.source.systemCatalog(a, self.context, .{ .graph_authority = .{ .name = self.name, .graph_id = self.record_id } });
            const reply = try std.json.parseFromSliceLeaky(@import("../system_catalog/server_call.zig").GraphAuthorityResult, a, bytes, .{});
            if (!reply.present) return error.CatalogGenerationChanged;
            self.last_authority_check = @import("antfly_platform").time.monotonicNs();
            return;
        }
        const bytes = try self.server.source.systemCatalog(a, self.context, .{ .read = .{ .kind = .graph, .name = self.name } });
        const state = try std.json.parseFromSliceLeaky(domain.State, a, bytes, .{});
        const record = state.find(.graph, 0, self.name) orelse return error.CatalogGenerationChanged;
        if (record.id != self.record_id) return error.CatalogGenerationChanged;
        self.last_authority_check = @import("antfly_platform").time.monotonicNs();
    }
    pub fn invokeTyped(ptr: *anyopaque, a: std.mem.Allocator, request: cypher.rpc.Request) !cypher.rpc.Reply {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try self.validateAuthority(true);
        var arena = std.heap.ArenaAllocator.init(cypher.backend.reclaimingAllocator(a));
        defer arena.deinit();
        var prepared = request;
        if (prepared.operation == .search) try self.prepareSearch(arena.allocator(), &prepared);
        const result = try (self.read orelse return error.CypherSnapshotRequired).invokeTyped(a, prepared);
        try self.validateAuthority(true);
        return result;
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
            try self.prepareSearch(owned, &request.value);
            prepared_body = try std.json.Stringify.valueAlloc(a, request.value, .{});
        }
        const result = try (self.read orelse return error.CypherSnapshotRequired).invoke(a, prepared_body orelse body);
        errdefer a.free(result);
        try self.validateAuthority(true);
        return result;
    }
    fn prepareSearch(self: *@This(), owned: std.mem.Allocator, request: *cypher.rpc.Request) !void {
        const parameters = try cypher.wire.parameter(owned, request.search orelse return error.InvalidQueryRequest);
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
        var parsed = try local.api_query.parsePublicQueryRequestWithDeadline(owned, resolver.iface(), request.table, try std.json.Stringify.valueAlloc(owned, json, .{}), normalized.deadline_ns);
        defer parsed.deinit(owned);
        request.prepared_search_json = try local.api_local_query_contract.encodeQueryRequest(owned, parsed.req);
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
        error.EnrichmentRetryInProgress, error.CypherSnapshotRequired, error.CypherSnapshotNotReady, error.ReadUnavailable, error.NotLeader, error.LeaderUnavailable, error.ReadIndexTimeout, error.TopologyChanged, error.StorageReadTemporarilyUnavailable, error.RetainedReadRestartRequired, error.GroupLeaderUnavailable, error.Backpressured, error.ConnectionResetByPeer, error.ConnectionRefused, error.ConnectionTimedOut, error.Timeout => 503,
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
        error.EnrichmentRetryInProgress, error.CypherSnapshotRequired, error.CypherSnapshotNotReady, error.ReadUnavailable, error.NotLeader, error.LeaderUnavailable, error.ReadIndexTimeout, error.TopologyChanged, error.StorageReadTemporarilyUnavailable, error.RetainedReadRestartRequired, error.GroupLeaderUnavailable, error.Backpressured, error.ConnectionResetByPeer, error.ConnectionRefused, error.ConnectionTimedOut, error.Timeout => "A coherent graph read is temporarily unavailable. Check graph readiness and try again after the owners become available.",
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

test "graph admission batches distinct tables before resolving label aliases" {
    const Fake = struct {
        state: domain.State,
        resolutions: usize = 0,
        fn call(ptr: *anyopaque, a: std.mem.Allocator, _: operation.RequestContext, request: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return switch (request) {
                .read => std.json.Stringify.valueAlloc(a, self.state, .{}),
                .policy_publication_status => a.dupe(u8, "null"),
                .resolve_many => |input| resolve: {
                    self.resolutions += 1;
                    try std.testing.expectEqual(@as(usize, 2), input.targets.len);
                    try std.testing.expect(input.include_query_definitions);
                    const tables = try a.alloc(?domain.ResolvedTable, input.targets.len);
                    for (input.targets, tables, 0..) |target, *out, index| {
                        try std.testing.expectEqualStrings(if (index == 0) "docs" else "other", target.table);
                        out.* = .{ .table_id = index + 7, .name = target.table, .query_definition = .{ .table_id = index + 7, .schema_json = "{\"version\":1,\"storage_mode\":\"document\",\"default_type\":\"Document\",\"document_schemas\":{\"Document\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"}}}}}}", .read_schema_json = "", .indexes_json = "{}" } };
                    }
                    break :resolve std.json.Stringify.valueAlloc(a, domain.ResolvedMany{ .revision = 1, .tables = tables }, .{});
                },
                else => error.TestUnexpectedResult,
            };
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const Label = @typeInfo(@FieldType(cypher.catalog.Definition, "labels")).pointer.child;
    const labels = try a.alloc(Label, 64);
    const bindings = try a.alloc(domain.GraphTableBinding, labels.len);
    for (labels, bindings, 0..) |*label, *binding, index| {
        const table = if (index % 2 == 0) "docs" else "other";
        label.* = .{ .name = try std.fmt.allocPrint(a, "D{d}", .{index}), .table = .{ .string = table } };
        binding.* = .{ .label_index = @intCast(index), .table_id = index % 2 + 7, .storage_name = table };
    }
    const resources = try a.alloc(domain.Resource, 1);
    resources[0] = .{ .kind = .graph, .id = 1, .name = "g", .source_json = try std.json.Stringify.valueAlloc(a, cypher.catalog.Definition{ .labels = labels }, .{}), .graph_tables = bindings };
    var fake: Fake = .{ .state = .{ .revision = 1, .resources = resources } };
    var server = server_mod.ApiHttpServer.init(std.testing.allocator, .{}, .{ .ptr = &fake, .vtable = &.{ .status = undefined, .system_catalog = Fake.call, .supports_query_definitions = true } }, null, null);
    defer server.deinit();
    var identity: ?server_mod.AuthenticatedIdentity = null;
    var diagnostic: cypher.compiler.Diagnostic = .{};
    var outcome: WriteOutcome = .not_started;
    const result = try execute(std.testing.allocator, &server, &identity, .{}, "g", "{\"query\":\"RETURN 1\"}", &diagnostic, &outcome);
    defer std.testing.allocator.free(result);
    try std.testing.expectEqual(@as(usize, 1), fake.resolutions);
    // Every alias still checks its captured immutable table identity.
    bindings[63].table_id = 99;
    try std.testing.expectError(error.CatalogGenerationChanged, execute(std.testing.allocator, &server, &identity, .{}, "g", "{\"query\":\"RETURN 1\"}", &diagnostic, &outcome));
}

test "graph admission batches SQL operation guards from completed authority checks" {
    const Fixture = struct {
        completed: u64 = 0,
        calls: usize = 0,
        fn guard(ptr: *anyopaque, _: std.mem.Allocator) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.io.sleep(.fromMilliseconds(20), .awake);
            self.calls += 1;
            self.completed = @import("antfly_platform").time.monotonicNs();
        }
    };
    var fixture: Fixture = .{};
    var server = server_mod.ApiHttpServer.init(std.testing.allocator, .{}, .{ .ptr = &fixture, .vtable = &.{ .status = undefined } }, null, null);
    defer server.deinit();
    var identity: ?server_mod.AuthenticatedIdentity = null;
    var channel: AuthorityChannel = .{ .server = &server, .identity = &identity, .context = .{}, .name = "g", .record_id = 1, .tables = &.{}, .writes = false, .catalog_guard = .{ .ptr = &fixture, .call = Fixture.guard } };
    try channel.validateAuthority(true);
    // Metadata latency cannot immediately expire the completed page guard.
    try std.testing.expect(channel.last_authority_check.? >= fixture.completed);
    channel.guarded_operation = true;
    try channel.validateAuthority(true);
    channel.guarded_operation = false;
    try channel.validateAuthority(true);
    try std.testing.expect(fixture.calls >= 2);
    try std.testing.expect(channel.last_authority_check.? >= fixture.completed);
}
