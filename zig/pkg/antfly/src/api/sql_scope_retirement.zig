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

//! Public ancestor DROP retains one admission query. Recovery observes its
//! native outcome; it never resubmits a name-based DROP or starts a new job.
const std = @import("std");
const local = @import("antfly_local_sources");
const catalog = local.sql_catalog;
const contract = local.system_catalog_domain.scope_retirement;
const wire = @import("../metadata/scope_retirement_wire.zig");
const http = @import("http_server.zig");
const A = std.mem.Allocator;
const Context = local.api_operation.RequestContext;
const base64 = std.base64.url_safe_no_pad;
const Token = struct {
    version: u8 = 12,
    kind: catalog.CatalogOperationReceipt.Kind = .database,
    database: []const u8,
    name: []const u8,
    query: wire.Query,
    fn validate(self: @This()) !void {
        if (self.version != 12 or (self.kind != .database and self.kind != .namespace) or
            (self.kind == .database and (self.query.lookup.reference.kind != .database or !std.mem.eql(u8, self.database, self.name))) or
            (self.kind == .namespace and self.query.lookup.reference.kind != .namespace) or
            !std.mem.eql(u8, &self.query.nonce, &self.query.lookup.reference.id)) return error.InvalidSqlRecoveryToken;
        try local.system_catalog_domain.validateName(self.database);
        try local.system_catalog_domain.validateName(self.name);
        try self.query.validate();
    }
};
fn authorize(server: *http.ApiHttpServer, identity: ?http.AuthenticatedIdentity, a: A, kind: catalog.CatalogOperationReceipt.Kind, database: []const u8, name: []const u8) !void {
    return @import("sql_ordinary_catalog.zig").authorize(server, identity, a, .{ .kind = if (kind == .namespace) .namespace else .database, .action = .drop, .database = database, .namespace = "public", .name = name, .table_id = 0 });
}
fn encode(a: A, scratch: A, token: Token) ![]u8 {
    try token.validate();
    const bytes = try std.json.Stringify.valueAlloc(scratch, token, .{});
    defer scratch.free(bytes);
    const size = base64.Encoder.calcSize(bytes.len);
    if (size > @import("sql_catalog_recovery.zig").max_token_bytes) return error.InvalidSqlRecoveryToken;
    const result = try a.alloc(u8, size);
    _ = base64.Encoder.encode(result, bytes);
    return result;
}
fn receipt(a: A, token: Token, encoded: []const u8) !catalog.CatalogOperationReceipt {
    const database = try a.dupe(u8, token.database);
    errdefer a.free(database);
    const namespace = try a.dupe(u8, if (token.kind == .namespace) token.name else "public");
    errdefer a.free(namespace);
    const name = try a.dupe(u8, token.name);
    return .{ .kind = token.kind, .operation = .drop, .database = database, .namespace = namespace, .name = name, .object_id = token.query.lookup.reference.root, .recovery_token = encoded };
}
fn applyObservation(token: Token, observed: wire.Observation, result: *catalog.CatalogOperationReceipt) !void {
    try observed.requireFor(token.query);
    if (observed.current) |head| if (!std.mem.eql(u8, &head.target_digest, &contract.Scope.targetDigest(token.database, if (token.kind == .namespace) token.name else null))) return error.InvalidSqlRecoveryToken;
    const outcome = observed.outcome orelse return;
    // Rejections can exist before a Head is reserved. Their authorization
    // labels must be bound by the durable outcome, never by the caller token.
    const target_digest = outcome.target_digest orelse
        if (outcome.result == .accepted) outcome.result.accepted.target_digest else return error.InvalidSqlRecoveryToken;
    if (!std.mem.eql(u8, &target_digest, &contract.Scope.targetDigest(token.database, if (token.kind == .namespace) token.name else null))) return error.InvalidSqlRecoveryToken;
    if (outcome.result == .rejected) {
        const failure = local.sql_errors.describe(outcome.result.rejected.toError());
        result.state = .rejected;
        result.diagnostic = .{ .code = failure.code, .message = failure.message, .retryable = false };
        return;
    }
    const head = observed.current orelse return error.InvalidScopeRetirement;
    result.state = .pending;
    result.diagnostic = null;
    if (head.progress.phase == .published) {
        result.state = .committed;
        result.object_id = null;
        result.catalog_revision = head.progress.catalog_revision;
    } else if (head.progress.phase == .canceled) {
        result.state = .rejected;
        result.diagnostic = .{ .code = "57014", .message = "DROP was canceled before physical retirement", .retryable = false };
    }
}
fn observe(server: *http.ApiHttpServer, context: Context, a: A, token: Token, result: *catalog.CatalogOperationReceipt) !void {
    const response = try server.source.systemCatalog(a, context, .{ .scope_retirement_observation = token.query });
    try applyObservation(token, try std.json.parseFromSliceLeaky(wire.Observation, a, response, .{}), result);
}
pub fn recoverIfScope(server: *http.ApiHttpServer, identity: ?http.AuthenticatedIdentity, input: Context, a: A, encoded: []const u8) !?catalog.CatalogOperationReceipt {
    if (encoded.len == 0 or encoded.len > @import("sql_catalog_recovery.zig").max_token_bytes) return error.InvalidSqlRecoveryToken;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const size = base64.Decoder.calcSizeForSlice(encoded) catch return error.InvalidSqlRecoveryToken;
    const bytes = try scratch.alloc(u8, size);
    base64.Decoder.decode(bytes, encoded) catch return error.InvalidSqlRecoveryToken;
    const header = std.json.parseFromSliceLeaky(struct { version: u8 }, scratch, bytes, .{ .ignore_unknown_fields = true }) catch return error.InvalidSqlRecoveryToken;
    if (header.version != 12) return null;
    const token = try std.json.parseFromSliceLeaky(Token, scratch, bytes, .{});
    try token.validate();
    try authorize(server, identity, scratch, token.kind, token.database, token.name);
    const owned = try a.dupe(u8, encoded);
    var result = receipt(a, token, owned) catch |err| {
        a.free(owned);
        return err;
    };
    errdefer result.deinit(a);
    var context = try input.platformDeadline();
    context.setting_admin = true;
    context.schema_validation_authority = true;
    observe(server, context, scratch, token, &result) catch |err| {
        if (!@import("../metadata/schema_validation_coordinator.zig").isDeferred(err)) return err;
    };
    return result;
}
pub fn executeDatabase(server: *http.ApiHttpServer, identity: ?http.AuthenticatedIdentity, input: Context, current_database: []const u8, allocators: catalog.DdlAllocators, ddl: local.sql_ast.CatalogDdl) !catalog.DdlOutcome {
    if (ddl.kind != .database) return error.InvalidSqlSyntax;
    return executeScope(server, identity, input, current_database, allocators, ddl);
}

pub fn executeNamespace(server: *http.ApiHttpServer, identity: ?http.AuthenticatedIdentity, input: Context, current_database: []const u8, allocators: catalog.DdlAllocators, ddl: local.sql_ast.CatalogDdl) !catalog.DdlOutcome {
    if (ddl.kind != .namespace or ddl.name.namespace != null) return error.InvalidSqlSyntax;
    return executeScope(server, identity, input, current_database, allocators, ddl);
}

fn executeScope(server: *http.ApiHttpServer, identity: ?http.AuthenticatedIdentity, input: Context, current_database: []const u8, allocators: catalog.DdlAllocators, ddl: local.sql_ast.CatalogDdl) !catalog.DdlOutcome {
    if ((ddl.kind != .database and ddl.kind != .namespace) or ddl.action != .drop) return error.InvalidSqlSyntax;
    const namespace_drop = ddl.kind == .namespace;
    const scope_kind: @FieldType(contract.Scope, "kind") = if (namespace_drop) .namespace else .database;
    const target_database = if (namespace_drop) ddl.name.database orelse current_database else ddl.name.table;
    const receipt_kind: catalog.CatalogOperationReceipt.Kind = if (namespace_drop) .namespace else .database;
    try input.ensureActive();
    var arena = std.heap.ArenaAllocator.init(allocators.scratch);
    defer arena.deinit();
    const a = arena.allocator();
    try authorize(server, identity, a, receipt_kind, target_database, ddl.name.table);
    if (!namespace_drop and std.mem.eql(u8, ddl.name.table, current_database)) return error.SqlDropCurrentDatabase;
    var context = try input.platformDeadline();
    context.setting_admin = true;
    context.schema_validation_authority = true;
    context.fk_generation_publication_authority = true;
    const actor = try @import("sql_authorization_recovery.zig").actor(server, identity);
    const temporary_owner = if (namespace_drop) context.sql_relation_owner else null;
    const captured_owner = if (temporary_owner) |owner| owned: {
        var temporary_name: [72]u8 = undefined;
        break :owned if (std.mem.eql(u8, ddl.name.table, try owner.namespace(&temporary_name))) @as(?@TypeOf(owner), owner) else null;
    } else null;
    const body = server.source.systemCatalog(a, context, .{ .scope_retirement_prepare = .{ .kind = scope_kind, .database = target_database, .name = ddl.name.table, .creator = actor.session_user, .cascade = if (namespace_drop) ddl.cascade else true, .force = ddl.force, .sql_temporary_owner = captured_owner } }) catch |err| {
        if (ddl.conditional and err == (if (namespace_drop) error.NamespaceNotFound else error.DatabaseNotFound)) return .{};
        return err;
    };
    const manifest = try std.json.parseFromSliceLeaky(contract.Manifest, a, body, .{});
    if (manifest.scope.kind != scope_kind or
        !std.mem.eql(u8, manifest.scope.database.name, target_database) or
        (namespace_drop and (manifest.scope.namespace == null or !std.mem.eql(u8, manifest.scope.namespace.?.name, ddl.name.table) or !std.meta.eql(manifest.scope.namespace.?.sql_temporary_owner, captured_owner))) or
        manifest.cascade != (if (namespace_drop) ddl.cascade else true) or
        !manifest.creator.eql(actor.session_user) or manifest.force != ddl.force) return error.InvalidSqlBackendResponse;
    var selected = manifest.retiringTables();
    while (selected.next()) |table| {
        const record = try std.json.parseFromSliceLeaky(local.common_topology_records.TableRecord, a, table.preimage, .{});
        var schema = try local.schema_mod.parseValidatedTableSchema(a, if (record.schema_json.len == 0) "{}" else record.schema_json);
        defer schema.deinit(a);
        // Validate the complete physical capability footprint before admitting
        // an irreversible root. Unsupported owners never become stuck jobs.
        if (record.storage.engine != .local or schema.external_base_source != null) return error.UnsupportedSqlShape;
    }
    var survivors = try manifest.prepareSurvivors(allocators.scratch);
    defer survivors.deinit();
    if (survivors.changed != 0) return error.CatalogPublicationProofPending;
    const io = server.sharedApiIo() orelse return error.MissingBackendRuntimeIo;
    var nonce: [16]u8 = undefined;
    try io.randomSecure(&nonce);
    const request: wire.Request = .{ .admission = .{ .nonce = nonce, .head = manifest.catalog_revision, .command_fence = if (input.sql_command_fence) |fence| fence.* else null }, .operation = .{ .reserve = manifest } };
    const bytes = try request.encodeAlloc(a);
    var parsed = try wire.Input.parse(a, manifest.metadata_group_id, bytes);
    defer parsed.deinit();
    const token: Token = .{ .kind = receipt_kind, .database = manifest.scope.database.name, .name = if (manifest.scope.namespace) |ns| ns.name else manifest.scope.database.name, .query = parsed.query };
    const encoded = try encode(allocators.result, a, token);
    var result = receipt(allocators.result, token, encoded) catch |err| {
        allocators.result.free(encoded);
        return err;
    };
    errdefer result.deinit(allocators.result);
    try authorize(server, identity, a, receipt_kind, target_database, ddl.name.table);
    const response = server.source.systemCatalog(a, context, .{ .scope_retirement_submit = request }) catch |err| switch (err) {
        error.MetadataMutationOutcomeUnknown, error.OutOfMemory => return .{ .catalog_receipt = result },
        else => return err,
    };
    const admitted = std.json.parseFromSliceLeaky(wire.Observation, a, response, .{}) catch return .{ .catalog_receipt = result };
    if (admitted.outcome) |outcome| if (outcome.result == .rejected) return outcome.result.rejected.toError();
    try applyObservation(token, admitted, &result);
    const time = @import("antfly_platform").time;
    const deadline = if (allocators.completion == .bounded) time.monotonicNs() +| 2 * std.time.ns_per_s else context.deadline_ns orelse time.monotonicNs() +| 30 * std.time.ns_per_s;
    while (time.monotonicNs() < deadline) {
        input.ensureActive() catch break;
        var poll = std.heap.ArenaAllocator.init(allocators.scratch);
        defer poll.deinit();
        observe(server, context, poll.allocator(), token, &result) catch break;
        if (result.state == .committed) return .{ .mutation_outcome = .committed, .catalog_receipt = result };
        if (result.state == .rejected) return .{ .catalog_receipt = result };
        io.sleep(.fromMilliseconds(20), .awake) catch break;
    }
    return .{ .mutation_outcome = if (admitted.outcome != null) .committed_pending else null, .catalog_receipt = result };
}

test "SQL scope retirement schema receipts reject relabeled kinds names and replacement roots" {
    const a = std.testing.allocator;
    const token: Token = .{ .kind = .namespace, .database = "default", .name = "tenant_ops", .query = .{
        .lookup = .{ .group = 1, .reference = .{ .id = @splat(1), .digest = @splat(2), .database = 1, .root = 7, .kind = .namespace, .tables = 0, .resources = 0 } },
        .nonce = @splat(1),
        .digest = @splat(3),
    } };
    try token.validate();
    const head: @import("../metadata/scope_retirement_ledger.zig").Head = .{ .lookup = token.query.lookup, .metadata_incarnation = @splat(4), .creator = .bootstrap, .target_digest = contract.Scope.targetDigest("default", "tenant_ops"), .sequence = 1, .progress = .{ .reference = token.query.lookup.reference, .revision = 3, .phase = .published, .catalog_revision = 19 } };
    const observation: wire.Observation = .{ .current = head, .outcome = .{ .query = token.query, .result = .{ .accepted = head } } };
    const encoded = try encode(a, a, token);
    var result = receipt(a, token, encoded) catch |err| {
        a.free(encoded);
        return err;
    };
    defer result.deinit(a);
    try applyObservation(token, observation, &result);
    try std.testing.expectEqual(catalog.CatalogOperationReceipt.State.committed, result.state);
    try std.testing.expectEqual(@as(?u64, 19), result.catalog_revision);
    var forged = token;
    forged.kind = .database;
    try std.testing.expectError(error.InvalidSqlRecoveryToken, forged.validate());
    forged = token;
    forged.name = "another_schema";
    try std.testing.expectError(error.InvalidSqlRecoveryToken, applyObservation(forged, observation, &result));
    forged = token;
    forged.database = "another_database";
    try std.testing.expectError(error.InvalidSqlRecoveryToken, applyObservation(forged, observation, &result));
    // A denied reservation has no Head. Its outcome still binds the scope
    // labels so a token cannot relabel a foreign rejection as an owned one.
    const rejected: wire.Observation = .{ .current = null, .outcome = .{ .query = token.query, .target_digest = head.target_digest, .result = .{ .rejected = .Forbidden } } };
    try applyObservation(token, rejected, &result);
    try std.testing.expectEqual(catalog.CatalogOperationReceipt.State.rejected, result.state);
    forged = token;
    forged.name = "another_schema";
    try std.testing.expectError(error.InvalidSqlRecoveryToken, applyObservation(forged, rejected, &result));
    var unbound = rejected;
    unbound.outcome.?.target_digest = null;
    try std.testing.expectError(error.InvalidSqlRecoveryToken, applyObservation(token, unbound, &result));
    forged = token;
    forged.query.lookup.reference.root += 1;
    try std.testing.expectError(error.SequenceAdmissionConflict, applyObservation(forged, observation, &result));
}

test "SQL scope retirement schema recovery releases owned receipt on observation failure" {
    const Source = struct {
        fn status(_: *anyopaque) !@import("../metadata/api.zig").MetadataStatus {
            return error.UnexpectedStatusRead;
        }
        fn run(_: *anyopaque, _: A, _: Context, _: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            return error.InvalidSqlRecoveryToken;
        }
    };
    const a = std.testing.allocator;
    var source: Source = .{};
    var server = http.ApiHttpServer.init(a, .{ .imported_runtime_io = .{ .api = std.testing.io } }, .{ .ptr = &source, .vtable = &.{ .status = Source.status, .system_catalog = Source.run } }, null, null);
    defer server.deinit();
    const token: Token = .{ .kind = .namespace, .database = "default", .name = "tenant_ops", .query = .{
        .lookup = .{ .group = 1, .reference = .{ .id = @splat(1), .digest = @splat(2), .database = 1, .root = 7, .kind = .namespace, .tables = 0, .resources = 0 } },
        .nonce = @splat(1),
        .digest = @splat(3),
    } };
    const encoded = try encode(a, a, token);
    defer a.free(encoded);
    try std.testing.expectError(error.InvalidSqlRecoveryToken, recoverIfScope(&server, null, .{}, a, encoded));
}
