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

//! Authorized SQL catalog mutations use the same durable authority as REST.
const std = @import("std");
const server_mod = @import("http_server.zig");
const catalog = @import("antfly_local_sources").sql_catalog;
const domain = @import("antfly_local_sources").system_catalog_domain;
const operation = @import("antfly_local_sources").api_operation;
const tables = @import("tables.zig");
const restore_jobs = @import("restore_jobs.zig");

fn newReceipt(alloc: std.mem.Allocator, target: domain.Target, table_id: u64, version: u32) !catalog.DdlReceipt {
    const database = try alloc.dupe(u8, target.database);
    errdefer alloc.free(database);
    const namespace = try alloc.dupe(u8, target.namespace);
    errdefer alloc.free(namespace);
    const table = try alloc.dupe(u8, target.table);
    errdefer alloc.free(table);
    return .{ .database = database, .namespace = namespace, .table = table, .table_id = try std.fmt.allocPrint(alloc, "{d}", .{table_id}), .schema_version = version, .state = .pending };
}

pub const RelationAdmission = struct { table: domain.ResolvedTable, guard: domain.RelationMutationGuard };
const SearchPath = @import("../pgwire/search_path.zig").Path;

/// Prior read-layout retirement is deterministic maintenance, but it changes
/// the exact table-record CAS image. Wait before preparing/submitting the next
/// DDL, using only this already-authorized owner (never re-run search_path).
/// No mutation is replayed, and per-poll scratch cannot accumulate schemas.
fn waitPriorSchemaFinalization(server: *server_mod.ApiHttpServer, context: operation.RequestContext, result_alloc: std.mem.Allocator, scratch_backing: std.mem.Allocator, initial: RelationAdmission) !RelationAdmission {
    try context.ensureActive();
    const definition = initial.table.query_definition orelse return error.InvalidSqlBackendResponse;
    if (definition.storage_engine != .local or definition.read_schema_json.len == 0) return initial;
    const time = @import("antfly_platform").time;
    // Caller deadlines may use an Io clock with a different origin. Translate
    // once before combining them with the native-monotonic readiness budget.
    var bounded = try context.platformDeadline();
    const deadline = @min(bounded.deadline_ns orelse std.math.maxInt(u64), time.monotonicNs() +| 5 * std.time.ns_per_s);
    bounded.deadline_ns = deadline;
    const lookup: domain.ResolveMany = .{ .relations = &.{initial.guard.target}, .include_query_definitions = true };
    while (time.monotonicNs() < deadline) {
        try context.ensureActive();
        var scratch = std.heap.ArenaAllocator.init(scratch_backing);
        defer scratch.deinit();
        const a = scratch.allocator();
        const bytes = try server.source.systemCatalog(a, bounded, .{ .resolve_many = lookup });
        const observed = try std.json.parseFromSliceLeaky(domain.ResolvedMany, a, bytes, .{ .allocate = .alloc_always });
        try observed.validateRelations(lookup);
        const current = observed.relations[0] orelse return error.CatalogGenerationChanged;
        if (!std.mem.eql(u8, &observed.relation_epoch.?.incarnation, &initial.guard.incarnation) or
            !current.owner.eql(initial.guard.owner) or
            !std.mem.eql(u8, current.logical_table, initial.guard.logical_table) or
            !std.mem.eql(u8, current.table.name, initial.table.name))
            return error.CatalogGenerationChanged;
        const current_definition = current.table.query_definition orelse return error.InvalidSqlBackendResponse;
        if (current_definition.storage_engine != definition.storage_engine or
            !std.mem.eql(u8, current_definition.schema_json, definition.schema_json))
            return error.CatalogGenerationChanged;
        try context.ensureActive();
        if (current_definition.read_schema_json.len == 0)
            return .{ .table = try current.table.clone(result_alloc), .guard = initial.guard };
        const io = server.sharedApiIo() orelse return error.CatalogPublicationProofPending;
        const remaining = deadline -| time.monotonicNs();
        if (remaining == 0) break;
        try io.sleep(.fromNanoseconds(@min(remaining, 10 * std.time.ns_per_ms)), .awake);
    }
    return error.CatalogPublicationProofPending;
}

fn prepareGeneratedAddition(server: *server_mod.ApiHttpServer, context: operation.RequestContext, alloc: std.mem.Allocator, schema: std.json.Value, ddl: @import("antfly_local_sources").sql_ast.CatalogDdl, guard: *domain.RelationMutationGuard) !?std.json.Value {
    try context.ensureActive();
    var candidate = (try @import("antfly_local_sources").sql_schema_ddl.prepareGeneratedAddition(alloc, schema, ddl)) orelse return null;
    if (guard.constraint_name_admission != null) return error.InvalidSqlBackendResponse;
    var private = context;
    private.setting_admin = true;
    const bytes = try server.source.systemCatalog(alloc, private, .{ .constraint_name_prepare = .{
        .guard = guard.*,
        .fragment_json = candidate.fragment_json,
        .generated_name = candidate.generated_name,
    } });
    defer alloc.free(bytes);
    const prepared = std.json.parseFromSliceLeaky(domain.ConstraintNamePrepared, alloc, bytes, .{ .allocate = .alloc_always }) catch |err| {
        if (err == error.OutOfMemory) return err;
        return error.InvalidSqlBackendResponse;
    };
    // An additive response may choose one identity, never replace the schema
    // or change the kind which was already bound and locally validated.
    if (prepared.schema_json != null or prepared.admission.names.len != 1) return error.InvalidSqlBackendResponse;
    const expected: @TypeOf(prepared.admission.names[0].kind) = switch (ddl.schema_change.?) {
        .add_unique => |rule| if (rule.primary) .primary else .unique,
        .add_check => .check,
        .add_foreign_key => .foreign_key,
        .add_column => .foreign_key,
        else => unreachable,
    };
    const selected = prepared.admission.names[0];
    if (selected.kind != expected) return error.InvalidSqlBackendResponse;
    try candidate.selectName(alloc, selected.name);
    var guarded = guard.*;
    guarded.constraint_name_admission = prepared.admission;
    guarded.validate() catch return error.InvalidSqlBackendResponse;
    try context.ensureActive();
    guard.* = guarded;
    return candidate.schema;
}

test "SQL catalog anonymous addition preserves its pinned owner and validates preparation replies" {
    const Source = struct {
        calls: usize = 0,
        mode: enum { valid, wrong_kind, replacement, empty, wrong_scope } = .valid,
        fn status(_: *anyopaque) !@import("../metadata/api.zig").MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }
        fn run(raw: *anyopaque, a: std.mem.Allocator, context: operation.RequestContext, call: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(context.setting_admin);
            try std.testing.expect(call == .constraint_name_prepare);
            try std.testing.expect(!call.isMutation());
            self.calls += 1;
            const request = call.constraint_name_prepare;
            try std.testing.expectEqualStrings("items", request.guard.target.name);
            try std.testing.expectEqual(@as(u64, 17), request.guard.owner.table_id);
            try std.testing.expectEqual(@as(u32, 3), request.guard.owner.schema_version);
            try std.testing.expect(request.guard.constraint_name_admission == null);
            const fragment = try std.json.parseFromSliceLeaky(std.json.Value, a, request.fragment_json, .{});
            try std.testing.expectEqual(@as(usize, 1), fragment.object.count());
            const checks = fragment.object.get("checks").?.array.items;
            try std.testing.expectEqual(@as(usize, 1), checks.len);
            try std.testing.expectEqualStrings(request.generated_name, checks[0].object.get("name").?.string);
            var result: domain.ConstraintNamePrepared = .{
                .schema_json = if (self.mode == .replacement) "{}" else null,
                .admission = .{ .namespace_id = if (self.mode == .wrong_scope) 0 else 2, .names = &.{.{ .name = "items_id_check1", .kind = if (self.mode == .wrong_kind) .unique else .check }} },
            };
            if (self.mode == .empty) result.admission.names = &.{};
            return std.json.Stringify.valueAlloc(a, result, .{});
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var source: Source = .{};
    var server = server_mod.ApiHttpServer.init(a, .{}, .{ .ptr = &source, .vtable = &.{ .status = Source.status, .system_catalog = Source.run } }, null, null);
    defer server.deinit();
    const baseline = try std.json.parseFromSliceLeaky(std.json.Value, a,
        \\{"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}},"checks":[{"name":"items_id_check","expression":{"op":"literal","value":true}}]}
    , .{});
    const before = try std.json.Stringify.valueAlloc(a, baseline, .{});
    var compiled = try @import("antfly_local_sources").sql_compiler.compile(a, "ALTER TABLE items ADD CHECK (id > 0)", .{});
    defer compiled.deinit();
    const original: domain.RelationMutationGuard = .{
        .target = .{ .name = "items" },
        .logical_table = "items",
        .owner = .{ .table_id = 17, .schema_version = 3, .schema_digest = @splat(1), .kind = .table },
        .incarnation = @splat(2),
    };
    var guard = original;
    const result = (try prepareGeneratedAddition(&server, .{}, a, baseline, compiled.statement.catalog_ddl, &guard)).?;
    try std.testing.expectEqualStrings("items_id_check1", result.object.get("checks").?.array.items[1].object.get("name").?.string);
    try std.testing.expectEqualStrings("items_id_check1", guard.constraint_name_admission.?.names[0].name);
    try std.testing.expectEqual(@as(u64, 2), guard.constraint_name_admission.?.namespace_id);
    try std.testing.expectEqualDeep(original.owner, guard.owner);
    try std.testing.expectEqualDeep(original.incarnation, guard.incarnation);
    try std.testing.expectEqualStrings(before, try std.json.Stringify.valueAlloc(a, baseline, .{}));
    for ([_]@TypeOf(source.mode){ .wrong_kind, .replacement, .empty, .wrong_scope }) |mode| {
        source.mode = mode;
        guard = original;
        try std.testing.expectError(error.InvalidSqlBackendResponse, prepareGeneratedAddition(&server, .{}, a, baseline, compiled.statement.catalog_ddl, &guard));
        try std.testing.expect(guard.constraint_name_admission == null);
    }
    const calls = source.calls;
    try std.testing.expectError(error.DeadlineExceeded, prepareGeneratedAddition(&server, .{ .deadline_ns = 0 }, a, baseline, compiled.statement.catalog_ddl, &guard));
    try std.testing.expectEqual(calls, source.calls);
}

fn beginIndex(server: *server_mod.ApiHttpServer, context: operation.RequestContext, allocators: catalog.DdlAllocators, target: domain.Target, guard: domain.RelationMutationGuard, index_name: []const u8, schema_json: []const u8) !catalog.DdlOutcome {
    const reader = server.table_reads orelse return error.UnsupportedSqlExecution;
    const io = server.sharedApiIo() orelse return error.UnsupportedSqlExecution;
    // The caller already authorized this exact logical table. Preparation owns
    // its real predecessor catalog and never grants a key-routed substitute.
    var private = try context.platformDeadline();
    private.setting_admin = true;
    private.index_publication_authority = true;
    var owned = @import("relational_index_plan_builder.zig").prepare(server.source, reader, allocators.scratch, io, private, .{ .guard = guard, .index_name = index_name }, schema_json) catch |err| switch (err) {
        error.MetadataReplicationPending => return error.CatalogPublicationProofPending,
        else => return err,
    };
    var transferred = false;
    defer if (!transferred) owned.deinit();
    const a = allocators.result;
    var receipt = try newReceipt(a, target, owned.observation.table_id, try tables.schemaVersion(owned.request.begin.plan.after.schema_json));
    errdefer {
        a.free(receipt.database);
        a.free(receipt.namespace);
        a.free(receipt.table);
        a.free(receipt.table_id);
        if (receipt.index_publication_token) |token| a.free(token);
    }
    receipt.index_publication_token = try a.dupe(u8, &owned.recovery_token);
    transferred = true;
    return @import("relational_index_admission.zig").submitOwned(server.source, allocators.scratch, io, private, &owned, receipt, allocators.completion);
}

fn beginSchemaValidation(server: *server_mod.ApiHttpServer, context: operation.RequestContext, allocators: catalog.DdlAllocators, target: domain.Target, guard: domain.RelationMutationGuard, selection: @import("antfly_local_sources").storage_db_relational_schema_owner_contract.Selection) !catalog.DdlOutcome {
    const io = server.sharedApiIo() orelse return error.UnsupportedSqlExecution;
    var private = try context.platformDeadline();
    // captureTableAdmission already authorized this exact logical table. These
    // capabilities are never decoded from SQL, tokens, or public request JSON.
    private.setting_admin = true;
    private.schema_validation_authority = true;
    var transport: @import("schema_validation_owner_transport.zig").Transport = .{ .server = server, .alloc = allocators.scratch };
    defer transport.deinit();
    var owned = @import("schema_validation_plan_builder.zig").prepare(server.source, &transport, allocators.scratch, io, private, .{ .guard = guard, .selection = selection }) catch |err| switch (err) {
        error.MetadataReplicationPending => return error.CatalogPublicationProofPending,
        else => return err,
    };
    var transferred = false;
    defer if (!transferred) owned.deinit();
    const a = allocators.result;
    var receipt = try newReceipt(a, target, owned.lookup.table, std.math.add(u32, guard.owner.schema_version, 1) catch return error.SqlLimitExceeded);
    errdefer {
        a.free(receipt.database);
        a.free(receipt.namespace);
        a.free(receipt.table);
        a.free(receipt.table_id);
        if (receipt.schema_validation_token) |token| a.free(token);
    }
    receipt.schema_validation_token = try a.dupe(u8, &owned.recovery_token);
    transferred = true;
    return @import("schema_validation_admission.zig").submitOwned(server.source, allocators.scratch, io, private, &owned, receipt, allocators.completion);
}

fn validatesDeferredConstraint(schema: std.json.Value, ddl: @import("antfly_local_sources").sql_ast.CatalogDdl, collection: []const u8) bool {
    const change = ddl.schema_change orelse return false;
    if (change != .validate_constraint or schema != .object) return false;
    const constraints = schema.object.get(collection) orelse return false;
    if (constraints != .array) return false;
    for (constraints.array.items) |constraint| {
        if (constraint != .object) continue;
        const name = constraint.object.get("name") orelse continue;
        if (name != .string or !std.mem.eql(u8, name.string, change.validate_constraint)) continue;
        const not_valid = constraint.object.get("not_valid") orelse return false;
        return not_valid == .bool and not_valid.bool;
    }
    return false;
}

pub fn captureTableAdmission(server: *server_mod.ApiHttpServer, identity: ?server_mod.AuthenticatedIdentity, context: operation.RequestContext, a: std.mem.Allocator, requested_target: domain.Target, qualified: bool, path: ?*const SearchPath) !?RelationAdmission {
    const use_path = !qualified and path != null;
    const namespaces = if (use_path) path.?.len else 1;
    if (namespaces == 0) return null;
    if (namespaces > SearchPath.max_namespaces) return error.InvalidParameter;
    var candidates: [SearchPath.max_namespaces]domain.RelationTarget = undefined;
    for (0..namespaces) |i| candidates[i] = .{
        .database = requested_target.database,
        .namespace = if (use_path) path.?.entries[i].slice() else requested_target.namespace,
        .name = requested_target.table,
    };
    const lookup: domain.ResolveMany = .{ .relations = candidates[0..namespaces], .include_query_definitions = true };
    const bytes = try server.source.systemCatalog(a, context, .{ .resolve_many = lookup });
    const snapshot = try std.json.parseFromSliceLeaky(domain.ResolvedMany, a, bytes, .{ .allocate = .alloc_always });
    try snapshot.validateRelations(lookup);
    var selected: usize = 0;
    while (selected < snapshot.relations.len and snapshot.relations[selected] == null) : (selected += 1) {}
    const owner = if (selected < snapshot.relations.len) snapshot.relations[selected].? else return null;
    const resolved_target: domain.Target = .{ .database = lookup.relations[selected].database, .namespace = lookup.relations[selected].namespace, .table = owner.logical_table };
    // A wrong-kind or unauthorized first shadow must not expose/mutate a
    // later table. Capture authorization and definition in the same cut.
    try authorizeTable(identity, a, resolved_target);
    if (owner.owner.kind != .table) return error.SqlWrongObjectType;
    return .{ .table = owner.table, .guard = .{ .target = lookup.relations[selected], .logical_table = owner.logical_table, .owner = owner.owner, .incarnation = snapshot.relation_epoch.?.incarnation } };
}

/// Name-only DDL does not need to decode every candidate table schema. The
/// bounded path is resolved in one catalog transaction, authorization names the
/// first visible owner, and metadata admits the same exact owner before preparing topology.
fn captureTableNameMutation(server: *server_mod.ApiHttpServer, identity: ?server_mod.AuthenticatedIdentity, context: operation.RequestContext, a: std.mem.Allocator, requested: domain.Target, path: ?*const SearchPath) !?struct { target: domain.Target, guard: domain.RelationMutationGuard } {
    const namespaces = if (path) |value| value.len else 1;
    if (namespaces == 0) return null;
    if (namespaces > SearchPath.max_namespaces) return error.InvalidParameter;
    var candidates: [SearchPath.max_namespaces]domain.RelationTarget = undefined;
    var count: usize = 0;
    for (0..namespaces) |i| {
        const target: domain.Target = .{ .database = requested.database, .namespace = if (path) |value| value.entries[i].slice() else requested.namespace, .table = requested.table };
        if (path != null and !try @import("sql_catalog_auth.zig").namespaceUsage(identity, a, target)) continue;
        candidates[count] = .{ .database = target.database, .namespace = target.namespace, .name = target.table };
        count += 1;
    }
    if (count == 0) return null;
    const lookup: domain.ResolveMany = .{ .relations = candidates[0..count] };
    const bytes = try server.source.systemCatalog(a, context, .{ .resolve_many = lookup });
    const snapshot = try std.json.parseFromSliceLeaky(domain.ResolvedMany, a, bytes, .{ .allocate = .alloc_always });
    try snapshot.validateRelations(lookup);
    for (snapshot.relations, lookup.relations) |maybe, candidate| {
        const found = maybe orelse continue;
        const target: domain.Target = .{ .database = candidate.database, .namespace = candidate.namespace, .table = found.logical_table };
        try @import("sql_catalog_auth.zig").authorizeTarget(server, identity, a, target);
        if (found.owner.kind != .table) return error.SqlWrongObjectType;
        if (!std.mem.eql(u8, found.logical_table, candidate.name)) return error.InvalidSqlBackendResponse;
        return .{ .target = target, .guard = .{ .target = candidate, .logical_table = found.logical_table, .owner = found.owner, .incarnation = snapshot.relation_epoch.?.incarnation } };
    }
    return null;
}

fn commentResource(server: *server_mod.ApiHttpServer, identity: ?server_mod.AuthenticatedIdentity, context: operation.RequestContext, database: []const u8, allocators: catalog.DdlAllocators, ddl: @import("antfly_local_sources").sql_ast.CatalogDdl) !catalog.DdlOutcome {
    if ((ddl.kind != .database and ddl.kind != .namespace) or ddl.action != .comment or ddl.conditional or ddl.cascade or ddl.comment_column != null or
        ddl.name.database != null or ddl.name.namespace != null) return error.InvalidSqlSyntax;
    const kind: domain.Kind = if (ddl.kind == .database) .database else .namespace;
    const a = allocators.scratch;
    const route: @import("../system_catalog/routes.zig").Route = .{ .kind = kind, .database = database, .name = ddl.name.table };
    const resource = try @import("../system_catalog/routes.zig").resourceNameAlloc(a, route);
    defer a.free(resource);
    const permission_kind: @import("../usermgr/mod.zig").ResourceType = if (kind == .database) .database else .namespace;
    if (identity) |authenticated| if (!server_mod.permissionsAllow(authenticated.permissions, permission_kind, resource, .admin)) return error.Forbidden;
    var private = context;
    private.setting_admin = true;
    const response = server.source.systemCatalog(a, private, .{ .mutate = .{ .mutation = .{
        .action = .comment,
        .kind = kind,
        .name = ddl.name.table,
        .database = database,
        .description = ddl.comment_value,
    } } }) catch |err| {
        // PostgreSQL COMMENT ON DATABASE treats an absent database as a no-op.
        // Authorization still precedes lookup; no write was admitted here.
        if (kind == .database and err == error.DatabaseNotFound) return .{ .mutation_outcome = null };
        if (err == error.MetadataMutationOutcomeUnknown or err == error.OutOfMemory) return error.SqlMutationOutcomeUnknown;
        if (err == error.CatalogCommandTooLarge) return error.ProgramLimitExceeded;
        return err;
    };
    defer a.free(response);
    var result = std.json.parseFromSlice(domain.MutationResult, a, response, .{}) catch return error.SqlMutationOutcomeUnknown;
    defer result.deinit();
    const committed = result.value.resource orelse return error.SqlMutationOutcomeUnknown;
    if (result.value.revision == 0 or committed.kind != kind or !std.mem.eql(u8, committed.name, ddl.name.table)) return error.SqlMutationOutcomeUnknown;
    return .{ .mutation_outcome = .committed };
}

fn commentTable(server: *server_mod.ApiHttpServer, identity: ?server_mod.AuthenticatedIdentity, context: operation.RequestContext, database: []const u8, namespace: []const u8, allocators: catalog.DdlAllocators, ddl: @import("antfly_local_sources").sql_ast.CatalogDdl, path: ?*const SearchPath) !catalog.DdlOutcome {
    if (ddl.kind != .table or ddl.action != .comment or ddl.conditional or ddl.cascade) return error.InvalidSqlSyntax;
    if (!server.source.vtable.supports_query_definitions) return error.UnsupportedSqlExecution;
    var arena = std.heap.ArenaAllocator.init(allocators.scratch);
    defer arena.deinit();
    const a = arena.allocator();
    const target: domain.Target = .{ .database = ddl.name.database orelse database, .namespace = ddl.name.namespace orelse namespace, .table = ddl.name.table };
    try target.validate();
    if (ddl.name.database != null or ddl.name.namespace != null or path == null) try authorizeTable(identity, a, target);
    const captured = (try captureTableAdmission(server, identity, context, a, target, ddl.name.database != null or ddl.name.namespace != null, path)) orelse return error.TableNotFound;
    var private = context;
    private.setting_admin = true;
    const response = server.source.systemCatalog(a, private, .{ .relation_description_mutate = .{ .guard = captured.guard, .description = ddl.comment_value orelse "", .column = ddl.comment_column } }) catch |err| {
        if (err == error.MetadataMutationOutcomeUnknown) return error.SqlMutationOutcomeUnknown;
        if (err == error.CatalogCommandTooLarge) return error.ProgramLimitExceeded;
        return err;
    };
    // A successful reply is a durable metadata receipt, never a schema
    // publication. Comments cannot manufacture an epoch or rewrite rows.
    const receipt = std.json.parseFromSliceLeaky(@import("../system_catalog/server_call.zig").RelationReplacementResult, a, response, .{}) catch return error.SqlMutationOutcomeUnknown;
    if (receipt.schema_version != captured.guard.owner.schema_version) return error.SqlMutationOutcomeUnknown;
    return .{ .mutation_outcome = .committed };
}

fn alterSchema(server: *server_mod.ApiHttpServer, identity: ?server_mod.AuthenticatedIdentity, context: operation.RequestContext, allocators: catalog.DdlAllocators, requested_target: domain.Target, ddl: @import("antfly_local_sources").sql_ast.CatalogDdl, admission: ?RelationAdmission, path: ?*const SearchPath) !catalog.DdlOutcome {
    const alloc = allocators.result;
    // Compiler labels are not public identities. Do not publish an anonymous
    // addition until every publication path can atomically admit its name.
    if (ddl.schema_change) |change| if (change.generatedConstraintName() != null and
        @import("../metadata/topology_protocol.zig").current_version < @import("../metadata/topology_protocol.zig").constraint_occupancy_version)
        return error.TableTopologyProtocolUpgradeRequired;
    if (!server.source.vtable.supports_query_definitions) return error.UnsupportedSqlExecution;
    var arena = std.heap.ArenaAllocator.init(allocators.scratch);
    defer arena.deinit();
    const a = arena.allocator();
    const initially_captured = admission orelse
        ((try captureTableAdmission(server, identity, context, a, requested_target, ddl.name.namespace != null or ddl.name.database != null, path)) orelse return if (ddl.conditional) .{} else error.TableNotFound);
    var captured = initially_captured;
    const target: domain.Target = .{
        .database = captured.guard.target.database,
        .namespace = captured.guard.target.namespace,
        .table = captured.guard.logical_table,
    };
    var table = captured.table;
    var definition = table.query_definition orelse return error.InvalidSqlBackendResponse;
    var parsed = try @import("antfly_local_sources").schema_mod.parseValidatedTableSchema(a, definition.schema_json);
    defer parsed.deinit(a);
    const native = try @import("antfly_local_sources").schema_mod.deriveRuntimeTableSchema(a, parsed);
    if (native.storage_mode != .relational) return error.UnsupportedSqlShape;
    if (native.external_base_source != null) {
        if (ddl.concurrently) return error.UnsupportedSqlShape;
        const change = ddl.schema_change orelse return error.ExternalLakeReadOnly;
        switch (change) {
            .create_index => |index| if (index.unique) return error.ExternalLakeReadOnly,
            .drop_index => {},
            else => return error.ExternalLakeReadOnly,
        }
    }
    // Active schema/owner identity must remain identical while only the
    // previous read layout retires. Reuse the already validated active schema.
    captured = try waitPriorSchemaFinalization(server, context, a, allocators.scratch, captured);
    table = captured.table;
    definition = table.query_definition orelse return error.InvalidSqlBackendResponse;
    var schema = try std.json.parseFromSliceLeaky(std.json.Value, a, definition.schema_json, .{ .parse_numbers = false });
    const declared_check_validation = validatesDeferredConstraint(schema, ddl, "checks");
    const declared_foreign_validation = validatesDeferredConstraint(schema, ddl, "foreign_keys");
    var guard = captured.guard;
    // FK validation owns its successor schema and transaction coverage. The
    // generic schema mutator deliberately rejects validity-only publication.
    if (declared_foreign_validation)
        return beginSchemaValidation(server, context, allocators, target, guard, .{ .foreign_keys = &.{ddl.schema_change.?.validate_constraint} });
    var preparation_context = context;
    const Checkpoint = struct {
        fn check(raw: ?*anyopaque) anyerror!void {
            const request: *const operation.RequestContext = @ptrCast(@alignCast(raw.?));
            try request.ensureActive();
        }
    };
    var row_validation_required = false;
    const column_transform = try @import("antfly_local_sources").sql_schema_ddl.prepareColumnTransform(a, schema, ddl, .{ .checkpoint = Checkpoint.check, .checkpoint_context = &preparation_context });
    if (ddl.schema_change) |change| if (change == .rename_column)
        try @import("antfly_local_sources").sql_schema_ddl.requireIndependentRenameIndexes(a, definition.indexes_json, change.rename_column.name);
    if (ddl.schema_change != null and ddl.schema_change.? == .set_nullability) {
        const change = ddl.schema_change.?.set_nullability;
        const prepared = try @import("antfly_local_sources").schema_column_nullability.prepareBorrowed(&arena, schema, &.{.{
            .column = change.column,
            .nullable = change.nullable,
        }}, .{ .checkpoint = Checkpoint.check, .checkpoint_context = &preparation_context });
        if (prepared.summary.changed == 0) return .{};
        if (prepared.summary.changed != 1) return error.InvalidSqlBackendResponse;
        schema = prepared.schema;
        row_validation_required = prepared.summary.tightened != 0;
    } else if (try prepareGeneratedAddition(server, context, a, schema, ddl, &guard)) |candidate| {
        schema = candidate;
    } else {
        if (!try @import("antfly_local_sources").sql_schema_ddl.applyWithLimits(a, &schema, ddl, .{ .checkpoint = Checkpoint.check, .checkpoint_context = &preparation_context })) return .{};
    }
    _ = schema.object.swapRemove("version");
    const proposed = try std.json.Stringify.valueAlloc(a, schema, .{});
    const updated = try server.bindForeignKeySchema(a, target, table.name, proposed, definition.schema_json, identity, context);
    try context.ensureActive();
    if (column_transform) |transform|
        return rewriteSchemaWithPolicies(server, identity, context, alloc, target, table.name, table.table_id, native.version, updated, .{ .dropped_columns = if (ddl.schema_change.? == .rename_column) .allow else .reject, .expression_schema = definition.schema_json, .column_transforms = &.{transform} }, null);
    if (declared_check_validation)
        return rewriteSchema(server, identity, context, alloc, target, table.name, table.table_id, native.version, updated, &.{}, null);
    if (!try tables.foreignKeyDefinitionsUnchanged(a, definition.schema_json, updated)) {
        var current = (try server.source.adminSnapshot()) orelse return error.UnsupportedSqlExecution;
        defer server.source.freeAdminSnapshot(&current);
        const before = tables.findTableByName(&current, table.name) orelse return error.TableNotFound;
        if (before.table_id != table.table_id or try tables.schemaVersion(before.schema_json) != native.version or
            !std.mem.eql(u8, before.schema_json, definition.schema_json))
            return error.SchemaVersionChanged;
        // Own the public receipt before admission. Allocation failure after an
        // uncertain durable begin must never erase the caller's handle.
        var receipt = try newReceipt(alloc, target, table.table_id, std.math.add(u32, native.version, 1) catch return error.SqlLimitExceeded);
        errdefer {
            alloc.free(receipt.database);
            alloc.free(receipt.namespace);
            alloc.free(receipt.table);
            alloc.free(receipt.table_id);
        }
        const publication_id = try alloc.alloc(u8, 32);
        errdefer alloc.free(publication_id);
        const publication = server.beginFkGenerationPublicationWithConstraintNameGuard(alloc, context, identity, before.*, updated, if (guard.constraint_name_admission != null) guard else null) catch |err| switch (err) {
            error.MetadataMutationOutcomeUnknown => return error.SqlMutationOutcomeUnknown,
            else => return err,
        };
        const hex = std.fmt.bytesToHex(publication.plan_id, .lower);
        @memcpy(publication_id, &hex);
        receipt.fk_generation_publication_id = publication_id;
        receipt.schema_version = publication.schema_version;
        receipt.state = if (publication.state == .admission_unknown) .admission_unknown else .pending;
        receipt.diagnostic = if (publication.state == .admission_unknown)
            "FK generation admission is unresolved. Retain the publication ID, refresh table schema and constraint status, and do not replay the DDL."
        else
            "FK generation publication was admitted. Poll the table schema and constraint status; do not replay the DDL.";
        return .{ .mutation_outcome = if (publication.state == .admission_unknown) null else .committed_pending, .receipt = receipt };
    }
    if (ddl.schema_change) |change| switch (change) {
        .create_index => |index| if (index.unique) return beginIndex(server, context, allocators, target, guard, index.name, updated),
        .add_column => |column| if (!column.nullable or column.default_expression != null or column.generated_expression != null)
            return rewriteSchema(server, identity, context, alloc, target, table.name, table.table_id, native.version, updated, if (column.default_expression != null) &.{column.name} else &.{}, null),
        .set_nullability => |nullability| if (row_validation_required)
            return beginSchemaValidation(server, context, allocators, target, guard, .{ .not_null = &.{nullability.column} }),
        // A validated declaration must not become visible until every old
        // row passes. Reuse the unpublished generation's snapshot/tail barrier
        // instead of publishing first and turning invalid history into a
        // committed, repair-required table. The same job receipt survives
        // cancellation, lost replies, and metadata/owner restart.
        .add_unique => return rewriteSchema(server, identity, context, alloc, target, table.name, table.table_id, native.version, updated, &.{}, if (guard.constraint_name_admission != null) guard else null),
        .add_check => |check| if (!check.not_valid)
            return rewriteSchema(server, identity, context, alloc, target, table.name, table.table_id, native.version, updated, &.{}, if (guard.constraint_name_admission != null) guard else null),
        else => {},
    };
    const retirement = try requiresRetirement(a, definition.schema_json, schema);
    const validation = ddl.schema_change.? == .validate_constraint;
    const activation_required = if (ddl.schema_change) |change| switch (change) {
        .create_index => |index| index.unique,
        .add_unique, .add_check, .add_foreign_key, .validate_constraint, .drop_constraint, .rename_constraint => true,
        else => false,
    } else false;
    // Allocate all durable identity fields before submission. Later timeout,
    // cancellation, or status-read failure cannot erase this acknowledgement.
    const concurrent_index = if (ddl.concurrently and ddl.action == .alter_schema and ddl.schema_change.? == .create_index)
        ddl.schema_change.?.create_index.name
    else
        null;
    var receipt: ?catalog.DdlReceipt = if (activation_required or retirement or ddl.concurrently) try newReceipt(alloc, target, table.table_id, if (ddl.concurrently) std.math.add(u32, native.version, 1) catch return error.SqlLimitExceeded else native.version) else null;
    errdefer if (receipt) |value| {
        alloc.free(value.database);
        alloc.free(value.namespace);
        alloc.free(value.table);
        alloc.free(value.table_id);
    };
    // SQL index DDL has no local shard owner for an external table. Reserve
    // its publication wakeup before consensus, then activate only after the
    // exact mutation succeeds; the inventory sweep remains crash recovery.
    var lake_commit = if (native.external_base_source != null) try server.prepareLakeCommit(table.name) else null;
    defer if (lake_commit) |*prepared| prepared.deinit(server);
    // Physical identity is immutable and never reused; native schema CAS also
    // fences concurrent schema changes. Never replay an uncertain submission.
    const version = submitSchema(server, context, alloc, table.name, table.table_id, native.version, updated, retirement, validation, guard) catch |err| {
        if (err == error.ForeignKeyReferenced) return error.SqlDependentConstraint;
        if (ddl.concurrently and (err == error.MetadataMutationOutcomeUnknown or err == error.OutOfMemory)) {
            receipt.?.state = .admission_unknown;
            receipt.?.diagnostic = "Index DDL admission is unresolved. Inspect this table ID and schema version through /indexes before retrying; do not replay the DDL.";
            return .{ .mutation_outcome = null, .receipt = receipt };
        }
        if (err == error.MetadataMutationOutcomeUnknown or err == error.OutOfMemory) return error.SqlMutationOutcomeUnknown;
        return err;
    };
    if (lake_commit) |*prepared| server.activateLakeCommit(prepared);
    if (receipt) |*value| {
        value.schema_version = version;
        if (concurrent_index) |name| {
            awaitIndexActivation(server, context, allocators.scratch, table.name, name, value, allocators.completion) catch {
                value.state = .pending;
                value.diagnostic = "The index declaration committed; its online build is still pending. Inspect this table ID and schema version through /indexes; do not replay the DDL.";
            };
            return .{ .mutation_outcome = if (value.state == .ready) .committed else if (value.state == .invalid) .committed_repair_required else .committed_pending, .receipt = value.* };
        }
        awaitActivation(server, context, alloc, table.name, value, allocators.completion) catch {
            value.state = .pending;
            value.diagnostic = "The declaration committed; activation is still pending. Inspect the table constraint status before using the index; do not replay the DDL.";
        };
        return .{ .mutation_outcome = if (value.state == .ready) .committed else if (value.state == .invalid) .committed_repair_required else .committed_pending, .receipt = value.* };
    }
    return .{};
}

fn rewriteSchema(server: *server_mod.ApiHttpServer, identity: ?server_mod.AuthenticatedIdentity, context: operation.RequestContext, alloc: std.mem.Allocator, target: domain.Target, physical: []const u8, table_id: u64, version: u32, schema: []const u8, default_columns: []const []const u8, guard: ?domain.RelationMutationGuard) !catalog.DdlOutcome {
    return rewriteSchemaWithPolicies(server, identity, context, alloc, target, physical, table_id, version, schema, .{ .default_columns = default_columns }, guard);
}

fn rewriteSchemaWithPolicies(server: *server_mod.ApiHttpServer, identity: ?server_mod.AuthenticatedIdentity, context: operation.RequestContext, alloc: std.mem.Allocator, target: domain.Target, physical: []const u8, table_id: u64, version: u32, schema: []const u8, policies: @import("antfly_local_sources").storage_db_relational_row_transform.Policies, guard: ?domain.RelationMutationGuard) !catalog.DdlOutcome {
    // Rewrite jobs and their staging plan must be committed by the metadata
    // owner. A data node's local restore history cannot admit this operation.
    if (server.restore_job_store.replicated == null) return error.SqlSchemaRewriteRequiresMetadataOwner;
    var snapshot = (try server.source.adminSnapshot()) orelse return error.UnsupportedSqlExecution;
    defer server.source.freeAdminSnapshot(&snapshot);
    const table = tables.findTableByName(&snapshot, physical) orelse return error.TableNotFound;
    if (table.table_id != table_id or try tables.schemaVersion(table.schema_json) != version) return error.SchemaVersionChanged;
    const target_version = std.math.add(u32, version, 1) catch return error.SqlLimitExceeded;
    if (guard) |value| {
        // Send the selected schema, not the dependency cohort. Metadata loads
        // its own predecessor. This read cannot admit a restore job.
        var private = context;
        private.setting_admin = true;
        const checked = try server.source.systemCatalog(alloc, private, .{ .constraint_name_check = .{ .guard = value, .schema_json = schema } });
        defer alloc.free(checked);
        var ready = try std.json.parseFromSlice(bool, alloc, checked, .{});
        defer ready.deinit();
        if (!ready.value) return error.InvalidSqlBackendResponse;
    }
    var receipt = try newReceipt(alloc, target, table_id, target_version);
    errdefer {
        alloc.free(receipt.database);
        alloc.free(receipt.namespace);
        alloc.free(receipt.table);
        alloc.free(receipt.table_id);
        if (receipt.restore_job_id) |id| alloc.free(id);
        if (receipt.idempotency_key) |key| alloc.free(key);
    }
    // Own the recovery handle before calling an operation that may admit a
    // durable job and then fail while constructing its HTTP response.
    var random: [16]u8 = undefined;
    const io = server.restore_job_store.io orelse return error.AsyncRestoreUnavailable;
    try io.randomSecure(&random);
    receipt.idempotency_key = try std.fmt.allocPrint(alloc, "auto:{s}", .{std.fmt.bytesToHex(random, .lower)});
    const namespace = try std.fmt.allocPrint(alloc, "schema-rewrite:{s}:{s}", .{ server_mod.storedDestinationPrincipal(identity), physical });
    defer alloc.free(namespace);
    const job_id = try restore_jobs.jobIdForIdempotency(alloc, namespace, receipt.idempotency_key.?);
    receipt.restore_job_id = try std.fmt.allocPrint(alloc, "{d}", .{job_id});
    // Shared native restore machinery reserves the entire dependency cohort,
    // transforms into unpublished storage, validates, and publishes atomically.
    var response = server.handleSchemaRewriteWithPolicies(table.*, schema, receipt.idempotency_key, identity, context, policies, guard) catch |err| return rewriteCallFailure(receipt, err);
    defer response.deinit(server.alloc);
    return rewriteResponse(alloc, response.status, response.body, receipt);
}

fn rewriteResponse(alloc: std.mem.Allocator, status: u16, body_bytes: []const u8, input_receipt: catalog.DdlReceipt) !catalog.DdlOutcome {
    var receipt = input_receipt;
    if (status != 202 and status != 200 and status != 503) return switch (status) {
        403 => error.Forbidden,
        404 => error.TableNotFound,
        409 => error.SchemaVersionChanged,
        400 => error.InvalidSchemaUpdateRequest,
        // A generic server response does not prove whether a durable begin
        // happened before response construction failed. Keep the pre-owned
        // recovery handle and forbid replay.
        else => return unknownRewriteOutcome(receipt),
    };
    var body = std.json.parseFromSlice(struct {
        job_id: ?[]const u8 = null,
        idempotency_key: ?[]const u8 = null,
        admission_outcome: ?[]const u8 = null,
    }, alloc, body_bytes, .{ .ignore_unknown_fields = true }) catch return unknownRewriteOutcome(receipt);
    defer body.deinit();
    if (status == 503 and (body.value.admission_outcome == null or !std.mem.eql(u8, body.value.admission_outcome.?, "unknown"))) return error.SqlWriteCapacityUnavailable;
    if (body.value.job_id == null or !std.mem.eql(u8, body.value.job_id.?, receipt.restore_job_id orelse return unknownRewriteOutcome(receipt))) return unknownRewriteOutcome(receipt);
    if (body.value.idempotency_key) |key| if (!std.mem.eql(u8, key, receipt.idempotency_key orelse return unknownRewriteOutcome(receipt))) return unknownRewriteOutcome(receipt);
    if (status == 503) {
        receipt.state = .admission_unknown;
        receipt.diagnostic = "Schema rewrite admission is unresolved. Poll the restore job using this receipt and do not replay the DDL.";
        return .{ .mutation_outcome = null, .receipt = receipt };
    }
    receipt.diagnostic = "The schema rewrite was durably admitted. Poll the native restore job for atomic publication or failure; do not replay the DDL.";
    return .{ .mutation_outcome = .committed_pending, .receipt = receipt };
}

fn unknownRewriteOutcome(input: catalog.DdlReceipt) catalog.DdlOutcome {
    var receipt = input;
    receipt.state = .admission_unknown;
    receipt.diagnostic = "Schema rewrite admission is unresolved. Poll this restore job and do not replay the DDL.";
    return .{ .mutation_outcome = null, .receipt = receipt };
}

fn rewriteCallFailure(receipt: catalog.DdlReceipt, err: anyerror) anyerror!catalog.DdlOutcome {
    std.log.warn("SQL schema rewrite failure err={s}", .{@errorName(err)});
    // These errors are emitted before startRecoverable. Other errors may
    // occur after durable admission, including response-construction OOM.
    return switch (err) {
        error.RestoreValidationPending,
        error.TableNotFound,
        error.SchemaVersionChanged,
        error.InvalidIdempotencyKey,
        error.Forbidden,
        error.UnsupportedSqlExecution,
        error.AsyncRestoreUnavailable,
        // Immutable program compilation precedes startRecoverable. These
        // errors prove nonadmission; inventing an unknown job here leaves a
        // caller polling an identity that was never durably reserved.
        error.RelationalExpressionBudgetExceeded,
        error.RelationalRewriteBudgetExceeded,
        => err,
        error.RelationalRewriteTypeChange,
        error.RelationalRewriteColumnDrop,
        => error.UnsupportedSqlShape,
        error.InvalidRelationalExpressionType => error.SqlTypeMismatch,
        error.StoredDestinationAuthorizationRevoked => error.Forbidden,
        error.GroupLeaderUnavailable => error.RestoreValidationPending,
        else => unknownRewriteOutcome(receipt),
    };
}

test "SQL rewrite response retains admitted and uncertain restore handles" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    var receipt = try newReceipt(alloc, .{ .database = "default", .namespace = "public", .table = "items" }, 7, 2);
    receipt.restore_job_id = "41";
    receipt.idempotency_key = "auto:abc";
    const accepted = try rewriteResponse(alloc, 202, "{\"job_id\":\"41\",\"idempotency_key\":\"auto:abc\"}", receipt);
    try std.testing.expectEqual(catalog.MutationOutcome.committed_pending, accepted.mutation_outcome.?);
    try std.testing.expectEqualStrings("41", accepted.receipt.?.restore_job_id.?);
    try std.testing.expectEqualStrings("auto:abc", accepted.receipt.?.idempotency_key.?);
    const unknown = try rewriteResponse(alloc, 503, "{\"admission_outcome\":\"unknown\",\"job_id\":\"41\",\"idempotency_key\":\"auto:abc\"}", receipt);
    try std.testing.expect(unknown.mutation_outcome == null);
    try std.testing.expectEqual(.admission_unknown, unknown.receipt.?.state);
    try std.testing.expectEqualStrings("41", unknown.receipt.?.restore_job_id.?);
    try std.testing.expectEqualStrings("auto:abc", unknown.receipt.?.idempotency_key.?);
    try std.testing.expectError(error.SqlWriteCapacityUnavailable, rewriteResponse(alloc, 503, "{\"error\":\"worker unavailable\"}", receipt));
    // A separately owned index can change after SQL preparation. Its current
    // predecessor recheck is definite nonadmission, not a nonexistent job.
    try std.testing.expectError(error.SchemaVersionChanged, rewriteResponse(alloc, 409, "{\"error\":\"Artifact index binding changed\"}", receipt));
    const unclassified = try rewriteResponse(alloc, 500, "{\"error\":\"failed to create restore job\"}", receipt);
    try std.testing.expectEqual(.admission_unknown, unclassified.receipt.?.state);
    try std.testing.expectEqualStrings("41", unclassified.receipt.?.restore_job_id.?);
    try std.testing.expectEqual(.admission_unknown, (try rewriteResponse(alloc, 503, "{\"admission_outcome\":\"unknown\"}", receipt)).receipt.?.state);
    try std.testing.expectEqual(.admission_unknown, (try rewriteResponse(alloc, 503, "{malformed", receipt)).receipt.?.state);
    try std.testing.expectError(error.RestoreValidationPending, rewriteCallFailure(receipt, error.RestoreValidationPending));
    try std.testing.expectError(error.RestoreValidationPending, rewriteCallFailure(receipt, error.GroupLeaderUnavailable));
    try std.testing.expectError(error.Forbidden, rewriteCallFailure(receipt, error.StoredDestinationAuthorizationRevoked));
    try std.testing.expectError(error.UnsupportedSqlShape, rewriteCallFailure(receipt, error.RelationalRewriteTypeChange));
    try std.testing.expectError(error.RelationalExpressionBudgetExceeded, rewriteCallFailure(receipt, error.RelationalExpressionBudgetExceeded));
    const response_oom = try rewriteCallFailure(receipt, error.OutOfMemory);
    try std.testing.expectEqual(.admission_unknown, response_oom.receipt.?.state);
    try std.testing.expectEqualStrings("41", response_oom.receipt.?.restore_job_id.?);
}

fn requiresRetirement(alloc: std.mem.Allocator, before: []const u8, after: std.json.Value) !bool {
    const prior = try std.json.parseFromSliceLeaky(std.json.Value, alloc, before, .{});
    for ([_][]const u8{ "unique_constraints", "foreign_keys" }) |key| {
        const old = prior.object.get(key) orelse continue;
        if (old != .array) continue;
        const current = after.object.get(key);
        for (old.array.items) |entry| {
            const name = entry.object.get("name").?.string;
            var found = false;
            if (current) |items| if (items == .array) {
                for (items.array.items) |item| if (std.mem.eql(u8, name, item.object.get("name").?.string)) {
                    found = true;
                    break;
                };
            };
            if (!found) return true;
        }
    }
    return false;
}

/// A narrow relation lookup may already prove read-layout retirement while a
/// cached full catalog cut still contains its predecessor. Retirement needs
/// the coherent full dependency/range cut, but must not turn that lag into a
/// user-visible busy rejection or prepare against the older CAS image.
fn retirementSnapshot(server: *server_mod.ApiHttpServer, context: operation.RequestContext, physical: []const u8, table_id: u64, version: u32) !@import("../metadata/api.zig").AdminSnapshot {
    var bounded = try context.platformDeadline();
    bounded.deadline_ns = @min(bounded.deadline_ns orelse std.math.maxInt(u64), @import("antfly_platform").time.monotonicNs() +| 5 * std.time.ns_per_s);
    while (true) {
        try bounded.ensureActive();
        var snapshot = (try server.source.adminSnapshot()) orelse return error.UnsupportedSqlExecution;
        var transferred = false;
        defer if (!transferred) server.source.freeAdminSnapshot(&snapshot);
        const table = tables.findTableByName(&snapshot, physical) orelse return error.TableNotFound;
        if (table.table_id != table_id) return error.SchemaVersionChanged;
        const observed_version = try tables.schemaVersion(table.schema_json);
        if (observed_version > version) return error.SchemaVersionChanged;
        if (observed_version == version and table.read_schema_json.len == 0) {
            transferred = true;
            return snapshot;
        }
        // Read-only observation only: neither the mutation nor any owner
        // command has been submitted. Every later identity/CAS guard remains.
        try activationPause(server, bounded);
    }
}

fn submitSchema(server: *server_mod.ApiHttpServer, context: operation.RequestContext, alloc: std.mem.Allocator, physical: []const u8, table_id: u64, expected_version: u32, body: []const u8, retirement: bool, validation: bool, guard: domain.RelationMutationGuard) !u32 {
    const next_version = if (validation) expected_version else std.math.add(u32, expected_version, 1) catch return error.SqlLimitExceeded;
    if (!retirement and !validation) {
        var private = context;
        private.setting_admin = true;
        const bytes = try server.source.systemCatalog(alloc, private, .{ .relation_schema_mutate = .{ .guard = guard, .schema_json = body } });
        defer alloc.free(bytes);
        var result = std.json.parseFromSlice(@import("../system_catalog/server_call.zig").RelationReplacementResult, alloc, bytes, .{}) catch return error.MetadataMutationOutcomeUnknown;
        defer result.deinit();
        if (result.value.schema_version != next_version) return error.MetadataMutationOutcomeUnknown;
        return result.value.schema_version;
    }
    var snapshot = if (retirement) try retirementSnapshot(server, context, physical, table_id, expected_version) else (try server.source.adminSnapshot()) orelse return error.UnsupportedSqlExecution;
    defer server.source.freeAdminSnapshot(&snapshot);
    const table = tables.findTableByName(&snapshot, physical) orelse return error.TableNotFound;
    if (table.table_id != table_id or try tables.schemaVersion(table.schema_json) != expected_version) return error.SchemaVersionChanged;
    const reader = server.table_reads orelse return error.UnsupportedSqlExecution;
    if (validation) {
        const writer = server.table_writes orelse return error.UnsupportedSqlExecution;
        try @import("relational_constraint_recovery.zig").retry(alloc, reader, writer, snapshot.tables, snapshot.ranges, .{ .table_name = physical, .relational_schema_version = expected_version }, context);
        return expected_version;
    }
    var replacement = try @import("relational_retirement_worker.zig").beginControlled(alloc, reader, snapshot.tables, snapshot.ranges, physical, body, false, context);
    defer replacement.deinit();
    try context.ensureActive();
    var private = context;
    private.setting_admin = true;
    const bytes = try server.source.systemCatalog(alloc, private, .{ .relation_replace = .{ .guard = guard, .expected = table.*, .replacement = replacement.table } });
    defer alloc.free(bytes);
    var result = std.json.parseFromSlice(@import("../system_catalog/server_call.zig").RelationReplacementResult, alloc, bytes, .{}) catch return error.MetadataMutationOutcomeUnknown;
    defer result.deinit();
    // The retirement reservation still serves the predecessor schema.
    // Await the target epoch, not the currently active predecessor epoch.
    return next_version;
}

fn authorizeTable(identity: ?server_mod.AuthenticatedIdentity, alloc: std.mem.Allocator, target: domain.Target) !void {
    const resource = try target.resourceNameAlloc(alloc);
    defer alloc.free(resource);
    if (identity) |authenticated| if (!server_mod.permissionsAllow(authenticated.permissions, .table, resource, .admin)) return error.Forbidden;
}

test "SQL catalog schema exhaustion is definite nonadmission for replacement and retirement" {
    const Source = struct {
        fn status(_: *anyopaque) !@import("../metadata/api.zig").MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }
    };
    var source: Source = .{};
    const alloc = std.testing.allocator;
    // No mutation or snapshot callbacks: overflow must be detected before
    // preparing a reservation or calling any durable admission machinery.
    var server = server_mod.ApiHttpServer.init(alloc, .{}, .{ .ptr = &source, .vtable = &.{ .status = Source.status } }, null, null);
    defer server.deinit();
    for ([_]bool{ false, true }) |retirement| {
        try std.testing.expectError(error.SqlLimitExceeded, submitSchema(&server, .{}, alloc, "items", 17, std.math.maxInt(u32), "{}", retirement, false, .{
            .target = .{ .name = "items" },
            .logical_table = "items",
            .owner = .{ .table_id = 17, .schema_version = std.math.maxInt(u32), .schema_digest = @splat(1), .kind = .table },
            .incarnation = @splat(1),
        }));
    }
}

fn executeIndex(server: *server_mod.ApiHttpServer, identity: ?server_mod.AuthenticatedIdentity, context: operation.RequestContext, database: []const u8, namespace: []const u8, allocators: catalog.DdlAllocators, ddl: @import("antfly_local_sources").sql_ast.CatalogDdl, path: ?*const SearchPath) !catalog.DdlOutcome {
    // The private UNIQUE publication protocol drains predecessor writers.
    // Nonunique indexes instead use the normal online generation: writes
    // maintain it from installation while bounded backfill catches history.
    // Never advertise the fenced UNIQUE protocol as CONCURRENTLY.
    if (ddl.cascade or ddl.index_targets.len > 1 or
        (ddl.concurrently and ddl.action == .create and ddl.schema_change.?.create_index.unique)) return error.UnsupportedSqlShape;
    if (!server.source.vtable.supports_query_definitions) return error.UnsupportedSqlExecution;
    var arena = std.heap.ArenaAllocator.init(allocators.scratch);
    defer arena.deinit();
    const a = arena.allocator();
    const creating = ddl.action == .create;
    if (!creating and ddl.action != .drop) return error.UnsupportedSqlShape;
    const name = if (creating) ddl.index_table orelse return error.InvalidSqlSyntax else ddl.name;
    var target: domain.Target = .{ .database = name.database orelse database, .namespace = name.namespace orelse namespace, .table = name.table };
    try target.validate();
    const use_path = name.namespace == null and name.database == null and path != null;
    if (creating and !use_path) try authorizeTable(identity, a, target);
    // Resolve the complete bounded search path in one immutable catalog cut.
    // Independent lookups could otherwise select a shadowed owner after churn.
    var candidates: [SearchPath.max_namespaces * 2]domain.RelationTarget = undefined;
    const stride: usize = if (creating) 2 else 1;
    const namespaces: usize = if (use_path) path.?.len else 1;
    if (namespaces > SearchPath.max_namespaces) return error.InvalidParameter;
    for (0..namespaces) |i| {
        const scope = if (use_path) path.?.entries[i].slice() else target.namespace;
        candidates[i * stride] = .{ .database = target.database, .namespace = scope, .name = name.table };
        if (creating) candidates[i * stride + 1] = .{ .database = target.database, .namespace = scope, .name = ddl.name.table };
    }
    // An empty lookup path proves absence without contacting the catalog.
    // IF EXISTS has the same no-op contract as a completed missing lookup;
    // explicitly qualified names still bypass the path above.
    if (namespaces == 0) {
        if (!creating and ddl.conditional) return .{};
        return if (creating) error.TableNotFound else error.SqlIndexNotFound;
    }
    const lookup: domain.ResolveMany = .{ .relations = candidates[0 .. namespaces * stride], .include_query_definitions = true };
    const bytes = try server.source.systemCatalog(a, context, .{ .resolve_many = lookup });
    const resolved = try std.json.parseFromSliceLeaky(domain.ResolvedMany, a, bytes, .{ .allocate = .alloc_always });
    try resolved.validateRelations(lookup);
    var selected: usize = 0;
    while (selected < lookup.relations.len and resolved.relations[selected] == null) : (selected += stride) {}
    const owner = if (selected < lookup.relations.len) resolved.relations[selected].? else {
        if (!creating and ddl.conditional) return .{};
        return if (creating) error.TableNotFound else error.SqlIndexNotFound;
    };
    const owner_target = lookup.relations[selected];
    target.namespace = owner_target.namespace;
    const table_target: domain.Target = .{ .database = target.database, .namespace = target.namespace, .table = owner.logical_table };
    // Object-kind diagnostics must not expose another table's index or
    // constraint ownership before authorizing its logical table.
    try authorizeTable(identity, a, table_target);
    if (creating) {
        if (owner.owner.kind != .table) return error.SqlWrongObjectType;
        if (resolved.relations[selected + 1] != null) {
            if (ddl.conditional) return .{};
            return error.SqlIndexAlreadyExists;
        }
    } else switch (owner.owner.kind) {
        .index => {},
        .constraint_index => return error.SqlConstraintOwnedIndex,
        .constraint_name => return error.InvalidCatalogRecord,
        else => return error.SqlWrongObjectType,
    }
    var transition = ddl;
    transition.kind = .table;
    transition.action = .alter_schema;
    transition.name = .{ .database = table_target.database, .namespace = table_target.namespace, .table = table_target.table };
    return alterSchema(server, identity, context, allocators, table_target, transition, .{
        .table = owner.table,
        .guard = .{ .target = owner_target, .logical_table = owner.logical_table, .owner = owner.owner, .incarnation = resolved.relation_epoch.?.incarnation },
    }, null);
}

pub fn execute(server: *server_mod.ApiHttpServer, identity: ?server_mod.AuthenticatedIdentity, context: operation.RequestContext, database: []const u8, namespace: []const u8, alloc: std.mem.Allocator, input: catalog.Ddl) !catalog.DdlOutcome {
    return executeWithPath(server, identity, context, database, namespace, alloc, input, null);
}

pub fn executeWithPath(server: *server_mod.ApiHttpServer, identity: ?server_mod.AuthenticatedIdentity, context: operation.RequestContext, database: []const u8, namespace: []const u8, alloc: std.mem.Allocator, input: catalog.Ddl, path: ?*const SearchPath) !catalog.DdlOutcome {
    return executeWithAllocators(server, identity, context, database, namespace, .{ .result = alloc, .scratch = alloc }, input, path);
}

/// IF NOT EXISTS applies to the requested table, never to an index-name
/// conflict. A taken automatically selected name can be retried safely only
/// after the authority has rejected the create before durable admission.
fn createConflict(server: *server_mod.ApiHttpServer, context: operation.RequestContext, alloc: std.mem.Allocator, target: domain.Target, names: []const []const u8, conditional: bool, err: anyerror, scratch: std.mem.Allocator) !catalog.DdlOutcome {
    if (err != error.CatalogAlreadyExists and err != error.TableAlreadyExists) return err;
    const lookup: domain.ResolveMany = .{ .targets = &.{target} };
    const bytes = try server.source.systemCatalog(alloc, context, .{ .resolve_many = lookup });
    const resolved = try std.json.parseFromSliceLeaky(domain.ResolvedMany, alloc, bytes, .{ .allocate = .alloc_always });
    try resolved.validateRelations(lookup);
    if (resolved.tables.len != 1) return error.InvalidCatalogRecord;
    if (resolved.tables[0] != null) return if (conditional) .{} else err;
    // The authority's table-exists result needs no relation publication or
    // name batch. Only a rejected create of an absent table needs that read.
    var epoch: ?@import("antfly_local_sources").system_catalog_relation_reconciliation.Epoch = null;
    var occupied = false;
    var offset: usize = 0;
    while (offset < names.len) {
        try context.ensureActive();
        var batch_arena = std.heap.ArenaAllocator.init(scratch);
        defer batch_arena.deinit();
        const a = batch_arena.allocator();
        const count = @min(@as(usize, 256), names.len - offset);
        const relations = try a.alloc(domain.RelationTarget, count);
        for (relations, names[offset..][0..count]) |*relation, name| relation.* = .{ .database = target.database, .namespace = target.namespace, .name = name };
        const batch: domain.ResolveMany = .{ .relations = relations, .name_allocation = true, .expected_relation_epoch = epoch };
        const reply = try server.source.systemCatalog(a, context, .{ .resolve_many = batch });
        const cut = try std.json.parseFromSliceLeaky(domain.ResolvedMany, a, reply, .{ .allocate = .alloc_always });
        try cut.validateRelations(batch);
        epoch = cut.relation_epoch;
        for (cut.relations) |relation| if (relation != null) {
            occupied = true;
        };
        offset += count;
    }
    if (occupied) return error.CatalogGenerationChanged;
    return err;
}

pub fn executeWithAllocators(server: *server_mod.ApiHttpServer, identity: ?server_mod.AuthenticatedIdentity, context: operation.RequestContext, database: []const u8, namespace: []const u8, allocators: catalog.DdlAllocators, input: catalog.Ddl, path: ?*const SearchPath) !catalog.DdlOutcome {
    const alloc = allocators.result;
    try context.ensureActive();
    if (input == .policy_ddl) return @import("sql_policy_ddl.zig").execute(server, identity, context, database, namespace, alloc, input.policy_ddl);
    if (input == .catalog_ddl and input.catalog_ddl.kind == .sequence)
        return @import("sql_sequence_ddl.zig").execute(server, identity, context, database, namespace, allocators, input.catalog_ddl, path);
    if (input == .catalog_ddl and input.catalog_ddl.action == .comment and input.catalog_ddl.kind != .table)
        return commentResource(server, identity, context, database, allocators, input.catalog_ddl);
    if (input == .catalog_ddl and input.catalog_ddl.action == .comment)
        return commentTable(server, identity, context, database, namespace, allocators, input.catalog_ddl, path);
    if (input == .catalog_ddl and input.catalog_ddl.kind == .role)
        return @import("sql_role_ddl.zig").execute(server, identity, context, allocators, input.catalog_ddl);
    if (input == .catalog_ddl and input.catalog_ddl.kind == .index)
        return executeIndex(server, identity, context, database, namespace, allocators, input.catalog_ddl, path);
    if (input == .catalog_ddl and input.catalog_ddl.kind == .view)
        return @import("sql_view_ddl.zig").execute(server, identity, context, database, namespace, allocators, input.catalog_ddl, path);
    const name = switch (input) {
        .create_table => |v| v.name,
        .drop_table => |v| v.table,
        .catalog_ddl => |v| v.name,
        .policy_ddl => unreachable,
    };
    var target: domain.Target = .{ .database = name.database orelse database, .namespace = name.namespace orelse namespace, .table = name.table };
    try target.validate();
    const kind: domain.Kind = switch (input) {
        .catalog_ddl => |ddl| switch (ddl.kind) {
            .index, .view, .role, .sequence => unreachable,
            inline else => |tag| @field(domain.Kind, @tagName(tag)),
        },
        .policy_ddl => unreachable,
        else => .table,
    };
    var route: @import("../system_catalog/routes.zig").Route = .{ .kind = kind, .database = target.database, .namespace = target.namespace, .name = target.table };
    const resource = try @import("../system_catalog/routes.zig").resourceNameAlloc(alloc, route);
    defer alloc.free(resource);
    // Check logical scope before any catalog lookup, including IF EXISTS.
    const permission_kind: @import("../usermgr/mod.zig").ResourceType = switch (kind) {
        .query_source, .view, .sequence => .table,
        inline else => |tag| @field(@import("../usermgr/mod.zig").ResourceType, @tagName(tag)),
    };
    const table_path_mutation = kind == .table and path != null and name.namespace == null and name.database == null and
        (input == .drop_table or (input == .catalog_ddl and (input.catalog_ddl.action == .rename or input.catalog_ddl.action == .set_tablespace)));
    const alter_path_lookup = input == .catalog_ddl and input.catalog_ddl.action == .alter_schema and
        path != null and name.namespace == null and name.database == null;
    if (!alter_path_lookup and !table_path_mutation) if (identity) |authenticated| if (!server_mod.permissionsAllow(authenticated.permissions, permission_kind, resource, .admin)) return error.Forbidden;
    if (input == .catalog_ddl and input.catalog_ddl.action == .alter_schema)
        return alterSchema(server, identity, context, allocators, target, input.catalog_ddl, null, path);
    if (input == .catalog_ddl and input.catalog_ddl.action == .truncate)
        return @import("sql_truncate.zig").execute(server, identity, context, database, namespace, alloc, input.catalog_ddl);
    var request: domain.Request = .{ .mutation = .{ .action = switch (input) {
        .create_table => .create,
        .drop_table => .drop,
        .catalog_ddl => |ddl| switch (ddl.action) {
            .alter_schema, .truncate, .role_default, .comment, .alter_sequence => unreachable,
            inline else => |tag| @field(domain.Action, @tagName(tag)),
        },
        .policy_ddl => unreachable,
    }, .kind = kind, .database = target.database, .namespace = target.namespace, .name = target.table } };
    var arena = std.heap.ArenaAllocator.init(allocators.scratch);
    defer arena.deinit();
    const a = arena.allocator();
    if (table_path_mutation or input == .drop_table) {
        const captured = (try captureTableNameMutation(server, identity, context, a, target, if (table_path_mutation) path else null)) orelse {
            const conditional = switch (input) {
                .drop_table => |drop| drop.if_exists,
                .catalog_ddl => |ddl| ddl.conditional,
                else => false,
            };
            if (conditional) return .{};
            return error.TableNotFound;
        };
        target = captured.target;
        route.namespace = target.namespace;
        route.name = target.table;
        request.mutation.namespace = target.namespace;
        request.mutation.name = target.table;
        request.binding_guard = captured.guard;
    }
    var creation_receipt: ?catalog.DdlReceipt = null;
    const generated_names: []const []const u8 = &.{};
    var receipt_transferred = false;
    defer if (!receipt_transferred) if (creation_receipt) |receipt| {
        alloc.free(receipt.database);
        alloc.free(receipt.namespace);
        alloc.free(receipt.table);
        alloc.free(receipt.table_id);
    };
    switch (input) {
        .create_table => |create| {
            request.column_sequences = create.column_sequences;
            request.mutation.tablespace = create.tablespace;
            request.physical_name = try server.catalogStorageNameAlloc(a);
            // Explicit empty search indexes avoids building a full-text index
            // on every SQL table. SQL indexes are added through native DDL.
            const bound_schema = try server.bindForeignKeySchema(a, target, request.physical_name.?, create.schema_json, "", identity, context);
            // Initial FK publication owns support-index parent before/after
            // pairs atomically with the hidden child and name reservation.
            // Do not reject MATCH PARTIAL here or publish parent support in
            // a separate pre-admission mutation.
            const schema_value = try std.json.parseFromSliceLeaky(std.json.Value, a, bound_schema, .{ .parse_numbers = false });
            const body = try std.json.Stringify.valueAlloc(a, .{ .schema = schema_value, .indexes = std.json.Value{ .object = .empty } }, .{});
            var parsed = try tables.parseStoredCreateTableRequest(allocators.scratch, body);
            parsed.storage = .{};
            defer parsed.deinit(allocators.scratch);
            if (create.tablespace) |tablespace| parsed.tablespace_name = try allocators.scratch.dupe(u8, tablespace);
            if (try @import("../metadata/fk_generation_publication.zig").schemaHasForeignKeys(a, bound_schema)) {
                // Initial FK publication has a different atomic owner. Until
                // it accepts sequence intents, reject before reserving names
                // rather than silently publishing a table without defaults.
                if (create.column_sequences.len != 0) return error.UnsupportedSqlShape;
                const generated = if (@import("../metadata/topology_protocol.zig").current_version >= @import("../metadata/topology_protocol.zig").constraint_occupancy_version) create.generated_constraint_names else &.{};
                const plan = @import("fk_initial_create_plan_builder.zig").buildWithGeneratedNamesAndScratch(server, a, allocators.scratch, context, identity, target, parsed, generated) catch |err| switch (err) {
                    error.CatalogAlreadyExists, error.TableAlreadyExists => if (create.if_not_exists) return .{} else return err,
                    else => return err,
                };
                var receipt = try newReceipt(alloc, target, plan.child.table_id, 0);
                errdefer {
                    alloc.free(receipt.database);
                    alloc.free(receipt.namespace);
                    alloc.free(receipt.table);
                    alloc.free(receipt.table_id);
                    if (receipt.fk_generation_publication_id) |id| alloc.free(id);
                }
                const hex = std.fmt.bytesToHex(plan.id, .lower);
                receipt.fk_generation_publication_id = try alloc.dupe(u8, &hex);
                const accepted = server.submitFkInitialCreatePlan(allocators.scratch, context, plan) catch |err| return createConflict(server, context, a, target, generated_names, create.if_not_exists, err, allocators.scratch);
                receipt.state = if (accepted.state == .admission_unknown) .admission_unknown else .pending;
                receipt.diagnostic = if (accepted.state == .admission_unknown)
                    "Initial FK table admission is unresolved. Retain the publication ID, refresh table status, and do not replay CREATE."
                else
                    "Initial FK table publication was admitted. Poll for the table; do not replay CREATE.";
                return .{ .mutation_outcome = if (accepted.state == .admission_unknown) null else .committed_pending, .receipt = receipt };
            }
            for ([_][]const u8{ "unique_constraints", "checks" }) |key| {
                if (schema_value.object.get(key)) |constraints| if (constraints == .array and constraints.array.items.len != 0) {
                    creation_receipt = try newReceipt(alloc, target, 0, 0);
                    break;
                };
            }
            request.create_table_json = try tables.encodeStoredCreateTableRequestAlloc(a, parsed);
            // Rollout waits for native/standby/FK qualification before this
            // binary advertises occupancy support. Never interpret legacy
            // roots as proof that generated names are free.
            if (@import("../metadata/topology_protocol.zig").current_version >= @import("../metadata/topology_protocol.zig").constraint_occupancy_version)
                request.generated_constraint_names = create.generated_constraint_names;
        },
        .drop_table => {},
        .catalog_ddl => |ddl| {
            request.mutation.new_name = ddl.new_name;
            request.mutation.tablespace = ddl.tablespace;
            if (ddl.location) |location| request.mutation.location_json = try std.json.Stringify.valueAlloc(a, location, .{});
            if (ddl.new_name) |new_name| {
                var destination = route;
                destination.name = new_name;
                const destination_resource = try @import("../system_catalog/routes.zig").resourceNameAlloc(a, destination);
                if (kind == .table) {
                    try @import("sql_catalog_auth.zig").authorizeTarget(server, identity, a, .{ .database = target.database, .namespace = target.namespace, .table = new_name });
                } else if (identity) |authenticated| if (!server_mod.permissionsAllow(authenticated.permissions, permission_kind, destination_resource, .admin)) return error.Forbidden;
            }
        },
        .policy_ddl => unreachable,
    }
    if (request.mutation.tablespace) |tablespace| if (identity) |authenticated| {
        if (!server_mod.permissionsAllow(authenticated.permissions, .tablespace, tablespace, .read)) return error.Forbidden;
    };
    const response = server.source.systemCatalog(alloc, context, .{ .mutate = request }) catch |err| {
        // Conditional DDL observes the authority's atomic outcome. Owner binding
        // never permits retrying an ambiguous durable submission.
        switch (input) {
            .create_table => |create| if (err == error.CatalogAlreadyExists or err == error.TableAlreadyExists) return createConflict(server, context, a, target, generated_names, create.if_not_exists, err, allocators.scratch),
            .drop_table => |drop| if (drop.if_exists and (err == error.CatalogNotFound or err == error.TableNotFound)) return .{},
            .catalog_ddl => |ddl| if (ddl.conditional and ((ddl.action == .create and err == error.CatalogAlreadyExists) or (ddl.action == .drop and err == error.CatalogNotFound))) return .{},
            .policy_ddl => unreachable,
        }
        if (err == error.ForeignKeyReferenced and input == .drop_table) return error.SqlDependentConstraint;
        if (err == error.ConstraintRetirementRequired and input == .drop_table)
            return beginTableDrop(server, identity, context, allocators, target, request.binding_guard);
        if (err == error.MetadataMutationOutcomeUnknown) return error.SqlMutationOutcomeUnknown;
        // Native catalog errors are intentionally resource-neutral. SQL's
        // diagnostic identity depends on the admitted target kind; translate
        // only after atomic conditional outcomes and uncertain commits.
        if (err == error.CatalogAlreadyExists) return switch (kind) {
            .database => error.SqlDuplicateDatabase,
            .namespace => error.SqlDuplicateNamespace,
            .tablespace => error.SqlDuplicateTablespace,
            .table, .query_source, .view, .sequence => err,
        };
        if (err == error.CatalogNotFound) return switch (kind) {
            .database => error.DatabaseNotFound,
            .namespace => error.NamespaceNotFound,
            .tablespace => error.TablespaceNotFound,
            .table, .query_source, .view, .sequence => err,
        };
        return err;
    };
    defer alloc.free(response);
    if (creation_receipt) |*receipt| {
        const result = std.json.parseFromSliceLeaky(domain.MutationResult, a, response, .{}) catch return error.SqlMutationOutcomeUnknown;
        const resource_record = result.resource orelse return error.SqlMutationOutcomeUnknown;
        const table_id = std.fmt.allocPrint(alloc, "{d}", .{resource_record.id}) catch return error.SqlMutationOutcomeUnknown;
        alloc.free(receipt.table_id);
        receipt.table_id = table_id;
        awaitActivation(server, context, alloc, request.physical_name.?, receipt, allocators.completion) catch |err| {
            std.log.warn("SQL table creation committed but activation observation failed table_id={s} err={s}", .{ receipt.table_id, @errorName(err) });
            receipt.state = .pending;
            receipt.diagnostic = "Table creation committed; constraints are still being validated. Inspect table constraint status; do not replay the DDL.";
        };
        receipt_transferred = true;
        return .{ .mutation_outcome = if (receipt.state == .ready) .committed else if (receipt.state == .invalid) .committed_repair_required else .committed_pending, .receipt = receipt.* };
    }
    return .{};
}

/// Completion of a live nonunique generation requires every current owner.
/// The /indexes collector validates schema, comparison identity, complete
/// range coverage and a second metadata cut; a ready first shard is not proof.
fn awaitIndexActivation(server: *server_mod.ApiHttpServer, context: operation.RequestContext, alloc: std.mem.Allocator, physical: []const u8, name: []const u8, receipt: *catalog.DdlReceipt, completion: catalog.DdlAllocators.Completion) !void {
    const reader = server.table_reads orelse return error.ConstraintActivationPending;
    var bounded = context;
    if (bounded.deadline_ns == null)
        bounded.deadline_ns = (try activationNow(context)) +| (@as(u64, (@import("../pgwire/protocol.zig").Limits{}).statement_timeout_ms) * std.time.ns_per_ms);
    const observation_deadline = if (completion == .bounded) (try activationNow(context)) +| 2 * std.time.ns_per_s else std.math.maxInt(u64);
    const expected_id = try std.fmt.parseInt(u64, receipt.table_id, 10);
    while (true) {
        try bounded.ensureActive();
        if (try activationNow(context) >= observation_deadline) return error.ConstraintActivationPending;
        // Reclaim each observation independently of the result region. Memory
        // does not grow with build duration or number of historical rows.
        const state = observeIndexActivation(server, reader, bounded, alloc, physical, name, expected_id, receipt.schema_version) catch |err| switch (err) {
            error.PreparedGenerationChanged,
            error.TopologyChanged,
            error.Unavailable,
            error.StorageReadTemporarilyUnavailable,
            error.StorageKernelOwnerUnavailable,
            error.NotLeader,
            error.GroupLeaderUnavailable,
            error.LeaderUnavailable,
            error.UnknownGroup,
            error.ReadIndexTimeout,
            error.CatalogRoutingSnapshotTimeout,
            error.IndexNotFound,
            error.TableNotFound,
            => {
                try activationPause(server, bounded);
                continue;
            },
            else => return err,
        };
        switch (state) {
            .ready => {
                receipt.state = .ready;
                return;
            },
            .failed => {
                receipt.state = .invalid;
                receipt.diagnostic = "The committed online index build failed. Inspect its /indexes status and repair or drop the declaration; do not replay the DDL.";
                return;
            },
            .building => try activationPause(server, bounded),
        }
    }
}

fn observeIndexActivation(server: *server_mod.ApiHttpServer, reader: @import("antfly_local_sources").api_table_read_source.TableReadSource, context: operation.RequestContext, alloc: std.mem.Allocator, physical: []const u8, name: []const u8, table_id: u64, version: u32) !@import("antfly_indexes_openapi").types.RelationalIndexBuildState {
    var snapshot = (try server.source.adminSnapshot()) orelse return error.Unavailable;
    defer server.source.freeAdminSnapshot(&snapshot);
    const table = tables.findTableByName(&snapshot, physical) orelse return error.TableNotFound;
    if (table.table_id != table_id or try tables.schemaVersion(table.schema_json) != version) return error.SchemaVersionChanged;
    const bytes = try @import("relational_index_status.zig").collect(alloc, server.source, reader, physical, name, table.schema_json, context);
    defer alloc.free(bytes);
    var result = try std.json.parseFromSlice(@import("antfly_indexes_openapi").types.RelationalIndexStatus, alloc, bytes, .{});
    defer result.deinit();
    if ((std.fmt.parseInt(u64, result.value.table_id, 10) catch return error.InvalidSqlBackendResponse) != table_id or result.value.schema_version != version or !std.mem.eql(u8, result.value.index_name, name)) return error.InvalidSqlBackendResponse;
    return result.value.state;
}

fn awaitActivation(server: *server_mod.ApiHttpServer, context: operation.RequestContext, alloc: std.mem.Allocator, physical: []const u8, receipt: *catalog.DdlReceipt, completion: catalog.DdlAllocators.Completion) !void {
    const reader = server.table_reads orelse return error.ConstraintActivationPending;
    var bounded = context;
    // SQL DDL is synchronous within the admitted statement budget. A shorter
    // observation deadline also reaches owner admission and can abort a cold
    // open after the declaration commits. Preserve the caller's absolute
    // deadline/clock and use the normal SQL budget only for unbounded callers.
    if (bounded.deadline_ns == null) {
        const now = if (context.deadline_io) |borrow| blk: {
            var receiver = try borrow.receive();
            break :blk @as(u64, @intCast(@max(0, std.Io.Clock.now(.awake, receiver.io()).nanoseconds)));
        } else @import("antfly_platform").time.monotonicNs();
        bounded.deadline_ns = now +| (@as(u64, (@import("../pgwire/protocol.zig").Limits{}).statement_timeout_ms) * std.time.ns_per_ms);
    }
    // Bound observation separately: its shorter budget must never reach an
    // owner cold-open, which needs the admitted statement deadline above.
    const observation_deadline = if (completion == .bounded)
        (try activationNow(context)) +| (2 * std.time.ns_per_s)
    else
        std.math.maxInt(u64);
    while (true) {
        try bounded.ensureActive();
        if (try activationNow(context) >= observation_deadline) return error.ConstraintActivationPending;
        const bytes = @import("relational_constraint_status.zig").collect(alloc, server.source, reader, physical, bounded) catch |err| switch (err) {
            // The declaration is already durable, but its new owner/route may
            // not be readable yet. Retry only the observation within the same
            // deadline; never resubmit the DDL or infer coverage from absence.
            error.ConstraintActivationPending,
            error.PreparedGenerationChanged,
            error.TopologyChanged,
            error.StorageReadTemporarilyUnavailable,
            error.StorageKernelOwnerUnavailable,
            error.NotLeader,
            error.GroupLeaderUnavailable,
            error.LeaderUnavailable,
            error.UnknownGroup,
            error.ReadIndexTimeout,
            error.CatalogRoutingSnapshotTimeout,
            => {
                try activationPause(server, bounded);
                continue;
            },
            else => return err,
        };
        defer alloc.free(bytes);
        var status = try std.json.parseFromSlice(struct { schema_version: u32, state: enum { enforced, validating, invalid }, retirement: ?struct { failure: ?[]const u8 = null } = null }, alloc, bytes, .{ .ignore_unknown_fields = true });
        defer status.deinit();
        if (status.value.retirement) |job| {
            if (job.failure != null) {
                receipt.state = .invalid;
                receipt.diagnostic = "The committed retirement needs repair; inspect the table constraint retirement status. Do not replay the DDL.";
                return;
            }
            try activationPause(server, bounded);
            continue;
        }
        if (status.value.schema_version != receipt.schema_version) return error.SchemaVersionChanged;
        switch (status.value.state) {
            .enforced => {
                receipt.state = .ready;
                return;
            },
            .invalid => {
                receipt.state = .invalid;
                receipt.diagnostic = "Constraint validation failed. The committed declaration remains inspectable in the table constraint status; repair the data or drop the index.";
                return;
            },
            .validating => {},
        }
        try activationPause(server, bounded);
    }
}

test "SQL catalog DDL readiness preserves the statement deadline through owner admission" {
    const metadata_api = @import("../metadata/api.zig");
    const types = @import("antfly_local_sources").storage_db_types;
    const Fixture = struct {
        expected_deadline: ?u64,
        expected_io: ?@import("antfly_runtime_abi").io_abi.Borrow,
        calls: usize = 0,
        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }
        fn snapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast(&[_]@import("../metadata/table_manager.zig").TableRecord{.{
                    .table_id = 7,
                    .name = "new_table",
                    .schema_json =
                    \\{"version":0,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"}},"additionalProperties":false}}}}
                    ,
                }}),
                .ranges = &.{},
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }
        fn free(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
        fn lookup(ptr: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, opts: types.LookupOptions, _: @import("../raft/read_gate.zig").ReadConsistency) !?@import("antfly_local_sources").api_table_read_source.LookupResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            try std.testing.expect(opts.relational_integrity_catalog);
            try std.testing.expectEqualDeep(self.expected_io, opts.execution_io);
            if (self.expected_deadline) |expected| {
                try std.testing.expectEqual(expected, opts.execution_deadline_ns.?);
            } else {
                const remaining = opts.execution_deadline_ns.? -| @import("antfly_platform").time.monotonicNs();
                try std.testing.expect(remaining > 29 * std.time.ns_per_s);
                try std.testing.expect(remaining <= 30 * std.time.ns_per_s);
            }
            return error.UnexpectedCall;
        }
    };
    const alloc = std.testing.allocator;
    for ([_]?u64{ 1, 10, null }) |seconds| {
        const deadline = if (seconds) |value| @import("antfly_platform").time.monotonicNs() + value * std.time.ns_per_s else null;
        var fixture: Fixture = .{ .expected_deadline = deadline, .expected_io = null };
        var server = server_mod.ApiHttpServer.init(alloc, .{}, .{ .ptr = &fixture, .vtable = &.{ .status = Fixture.status, .admin_snapshot = Fixture.snapshot, .free_admin_snapshot = Fixture.free } }, null, null);
        defer server.deinit();
        server.table_reads = .{ .ptr = &fixture, .vtable = &.{ .lookup = Fixture.lookup, .scan = undefined, .query = undefined } };
        var receipt: catalog.DdlReceipt = .{ .database = "default", .namespace = "public", .table = "new_table", .table_id = "7", .schema_version = 0, .state = .pending };
        try std.testing.expectError(error.UnexpectedCall, awaitActivation(&server, .{ .deadline_ns = deadline }, alloc, "new_table", &receipt, .bounded));
        try std.testing.expectEqual(@as(usize, 1), fixture.calls);
        if (seconds != null) {
            fixture.expected_io = @import("antfly_runtime_abi").io_abi.Borrow.init(&std.testing.io);
            fixture.expected_deadline = @intCast(std.Io.Clock.now(.awake, std.testing.io).nanoseconds + 10 * std.time.ns_per_s);
            try std.testing.expectError(error.UnexpectedCall, awaitActivation(&server, .{ .deadline_ns = fixture.expected_deadline, .deadline_io = fixture.expected_io }, alloc, "new_table", &receipt, .until_deadline));
        }
        try std.testing.expectEqual(.pending, receipt.state);
    }
}

test "SQL catalog DDL readiness retries transient observations without replaying the declaration" {
    const metadata_api = @import("../metadata/api.zig");
    const Fixture = struct {
        failure: anyerror,
        calls: usize = 0,
        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }
        fn snapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            if (self.calls == 1) return self.failure;
            return .{
                .status = try status(ptr),
                .tables = @constCast(&[_]@import("../metadata/table_manager.zig").TableRecord{.{
                    .table_id = 7,
                    .name = "new_table",
                    .schema_json =
                    \\{"version":0,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"}},"additionalProperties":false}}}}
                    ,
                }}),
                .ranges = &.{},
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }
        fn free(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };
    const alloc = std.testing.allocator;
    for ([_]anyerror{ error.StorageReadTemporarilyUnavailable, error.StorageKernelOwnerUnavailable, error.NotLeader, error.ReadIndexTimeout }) |failure| {
        var fixture: Fixture = .{ .failure = failure };
        var server = server_mod.ApiHttpServer.init(alloc, .{}, .{ .ptr = &fixture, .vtable = &.{ .status = Fixture.status, .admin_snapshot = Fixture.snapshot, .free_admin_snapshot = Fixture.free } }, null, null);
        defer server.deinit();
        // No owner read is needed for a declaration without constraints.
        server.table_reads = .{ .ptr = &fixture, .vtable = &.{ .lookup = undefined, .scan = undefined, .query = undefined } };
        var receipt: catalog.DdlReceipt = .{ .database = "default", .namespace = "public", .table = "new_table", .table_id = "7", .schema_version = 0, .state = .pending };
        try awaitActivation(&server, .{ .deadline_io = @import("antfly_runtime_abi").io_abi.Borrow.init(&std.testing.io) }, alloc, "new_table", &receipt, .bounded);
        try std.testing.expectEqual(.ready, receipt.state);
        try std.testing.expectEqual(@as(usize, 2), fixture.calls);
        // Invalid data remains a terminal observation error, not a retry.
        fixture.failure = error.InvalidIntegrityDefinition;
        fixture.calls = 0;
        try std.testing.expectError(error.InvalidIntegrityDefinition, awaitActivation(&server, .{}, alloc, "new_table", &receipt, .bounded));
        try std.testing.expectEqual(@as(usize, 1), fixture.calls);
        fixture.calls = 0;
        try std.testing.expectError(error.DeadlineExceeded, awaitActivation(&server, .{ .deadline_ns = 0 }, alloc, "new_table", &receipt, .bounded));
        try std.testing.expectEqual(@as(usize, 0), fixture.calls);
    }
}

fn activationNow(context: operation.RequestContext) !u64 {
    if (context.deadline_io) |borrow| {
        var receiver = try borrow.receive();
        return @intCast(@max(0, std.Io.Clock.now(.awake, receiver.io()).nanoseconds));
    }
    return @import("antfly_platform").time.monotonicNs();
}

fn activationPause(server: *server_mod.ApiHttpServer, context: operation.RequestContext) !void {
    try context.ensureActive();
    if (context.fanout_io orelse context.deadline_io) |borrow| {
        var receiver = try borrow.receive();
        return receiver.io().sleep(.fromMilliseconds(20), .awake);
    }
    const io = server.sharedApiIo() orelse return error.ConstraintActivationPending;
    try io.sleep(.fromMilliseconds(20), .awake);
}

test "SQL catalog DDL schema validates through native public admission" {
    const alloc = std.testing.allocator;
    var compiled = try @import("antfly_local_sources").sql_compiler.compile(alloc, "CREATE TABLE items (id BIGINT NOT NULL DEFAULT 9007199254740993, label TEXT DEFAULT NULL, payload JSON DEFAULT NULL, created TIMESTAMPTZ DEFAULT '2026-09-21T00:00:00Z', enabled BOOLEAN DEFAULT TRUE, amount DOUBLE PRECISION DEFAULT 1.5, exact_amount NUMERIC DEFAULT 9007199254740993.2500 CHECK (exact_amount>=9007199254740993.25))", .{});
    defer compiled.deinit();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const schema_json = try @import("antfly_local_sources").sql_ddl_runtime.createSchemaAlloc(a, compiled.statement.create_table);
    const value = try std.json.parseFromSliceLeaky(std.json.Value, a, schema_json, .{ .parse_numbers = false });
    const body = try std.json.Stringify.valueAlloc(a, .{ .schema = value, .indexes = std.json.Value{ .object = .empty } }, .{});
    var request = try tables.parseStoredCreateTableRequest(alloc, body);
    defer request.deinit(alloc);
    try std.testing.expectEqualStrings("{}", request.indexes_json.?);
    var parsed = try @import("antfly_local_sources").schema_mod.parseValidatedTableSchema(alloc, request.schema_json.?);
    defer parsed.deinit(alloc);
    const native = try @import("antfly_local_sources").schema_mod.deriveRuntimeTableSchema(alloc, parsed);
    defer @import("antfly_local_sources").storage_schema.freeSchema(alloc, native);
    try std.testing.expectEqual(@as(usize, 7), native.relational_columns.len);
    try std.testing.expectEqual(@import("antfly_local_sources").storage_schema.StorageMode.relational, native.storage_mode);
    try std.testing.expect(parsed.column_defaults != null);
    try std.testing.expect(native.requires_exact_numeric_expressions);
    try std.testing.expect(native.requires_exact_numeric_validation);
    var validator = try @import("antfly_local_sources").schema_mod.CompiledTableValidator.init(alloc, request.schema_json.?);
    defer validator.deinit(alloc);
    var row: std.json.Value = .{ .object = .empty };
    try validator.prepareValue(a, alloc, &row);
    try std.testing.expectEqualStrings("9007199254740993.2500", row.object.get("exact_amount").?.number_string);
}

test "SQL index DDL drops the exact qualified owner without a catalog snapshot" {
    const Source = struct {
        present: bool = true,
        attested: bool = true,
        invalid_reply: bool = false,
        reply_version: u32 = 8,
        path_lookup: bool = false,
        shadow_index: bool = false,
        kind: @import("antfly_local_sources").system_catalog_relation_names.Kind = .index,
        writes: usize = 0,
        reads: usize = 0,
        const schema =
            \\{"version":7,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}},"relational_indexes":[{"name":"items_id","keys":[{"column":"id"}]}]}
        ;
        fn status(_: *anyopaque) !@import("../metadata/api.zig").MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }
        fn run(raw: *anyopaque, alloc: std.mem.Allocator, context: operation.RequestContext, call: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (call == .resolve_many) {
                self.reads += 1;
                const request = call.resolve_many;
                try std.testing.expectEqual(@as(usize, 0), request.targets.len);
                try std.testing.expectEqual(@as(usize, if (self.path_lookup) 2 else 1), request.relations.len);
                try std.testing.expect(request.include_query_definitions);
                if (self.path_lookup) try std.testing.expectEqualStrings("public", request.relations[0].namespace);
                try std.testing.expectEqualStrings("tenant", request.relations[request.relations.len - 1].namespace);
                try std.testing.expectEqualStrings("items_id", request.relations[0].name);
                const table: @import("../metadata/table_manager.zig").TableRecord = .{ .table_id = 17, .name = "table:immutable-17", .schema_json = schema };
                var resolved = domain.ResolvedTable.fromTable(table);
                resolved.query_definition = domain.QueryDefinition.fromTable(table);
                var relations: [2]?domain.ResolvedRelation = @splat(null);
                relations[request.relations.len - 1] = if (self.present) .{ .table = resolved, .logical_table = "items", .owner = .{ .table_id = 17, .schema_version = 7, .schema_digest = @splat(1), .kind = self.kind } } else null;
                if (self.path_lookup and self.shadow_index) {
                    var shadow = resolved;
                    shadow.table_id = 18;
                    shadow.name = "table:immutable-18";
                    shadow.query_definition.?.table_id = 18;
                    relations[0] = .{ .table = shadow, .logical_table = "shadow", .owner = .{ .table_id = 18, .schema_version = 7, .schema_digest = @splat(1), .kind = .index } };
                }
                return std.json.Stringify.valueAlloc(alloc, domain.ResolvedMany{ .revision = 9, .tables = &.{}, .relations = relations[0..request.relations.len], .relation_epoch = if (self.attested) .{ .revision = 9, .incarnation = @splat(1) } else null }, .{});
            }
            try std.testing.expect(call == .relation_schema_mutate);
            try std.testing.expect(context.setting_admin);
            const mutation = call.relation_schema_mutate;
            try std.testing.expectEqualStrings(if (self.shadow_index) "public" else "tenant", mutation.guard.target.namespace);
            try std.testing.expectEqualStrings("items_id", mutation.guard.target.name);
            try std.testing.expectEqualStrings(if (self.shadow_index) "shadow" else "items", mutation.guard.logical_table);
            try std.testing.expectEqual(@as(u64, if (self.shadow_index) 18 else 17), mutation.guard.owner.table_id);
            try std.testing.expectEqual(@as(u32, 7), mutation.guard.owner.schema_version);
            var candidate = try std.json.parseFromSlice(std.json.Value, alloc, mutation.schema_json, .{});
            defer candidate.deinit();
            try std.testing.expectEqual(@as(usize, 0), candidate.value.object.get("relational_indexes").?.array.items.len);
            self.writes += 1;
            if (self.invalid_reply) return alloc.dupe(u8, "{invalid");
            return std.json.Stringify.valueAlloc(alloc, @import("../system_catalog/server_call.zig").RelationReplacementResult{ .schema_version = self.reply_version }, .{});
        }
    };
    const alloc = std.testing.allocator;
    var source: Source = .{};
    // No admin_snapshot or mutate_schema callback: ordinary index DDL must
    // use the captured relation and the metadata owner's table-local prepare.
    var server = server_mod.ApiHttpServer.init(alloc, .{}, .{ .ptr = &source, .vtable = &.{ .status = Source.status, .system_catalog = Source.run, .supports_query_definitions = true } }, null, null);
    defer server.deinit();
    var statement = try @import("antfly_local_sources").sql_compiler.compile(alloc, "DROP INDEX tenant.items_id", .{});
    defer statement.deinit();
    const input: catalog.Ddl = .{ .catalog_ddl = statement.statement.catalog_ddl };
    _ = try execute(&server, null, .{}, "default", "public", alloc, input);
    try std.testing.expectEqual(@as(usize, 1), source.writes);
    const denied: server_mod.AuthenticatedIdentity = .{ .username = @constCast("reader") };
    try std.testing.expectError(error.Forbidden, execute(&server, denied, .{}, "default", "public", alloc, input));
    try std.testing.expectEqual(@as(usize, 1), source.writes);
    source.kind = .table;
    try std.testing.expectError(error.Forbidden, execute(&server, denied, .{}, "default", "public", alloc, input));
    try std.testing.expectError(error.SqlWrongObjectType, execute(&server, null, .{}, "default", "public", alloc, input));
    source.kind = .constraint_index;
    try std.testing.expectError(error.Forbidden, execute(&server, denied, .{}, "default", "public", alloc, input));
    try std.testing.expectError(error.SqlConstraintOwnedIndex, execute(&server, null, .{}, "default", "public", alloc, input));
    try std.testing.expectEqual(@as(usize, 1), source.writes);
    source.kind = .index;
    source.present = false;
    try std.testing.expectError(error.SqlIndexNotFound, execute(&server, null, .{}, "default", "public", alloc, input));
    var conditional = try @import("antfly_local_sources").sql_compiler.compile(alloc, "DROP INDEX IF EXISTS tenant.items_id", .{});
    defer conditional.deinit();
    _ = try execute(&server, null, .{}, "default", "public", alloc, .{ .catalog_ddl = conditional.statement.catalog_ddl });
    source.attested = false;
    try std.testing.expectError(error.TableTopologyUpgradeRequired, execute(&server, null, .{}, "default", "public", alloc, .{ .catalog_ddl = conditional.statement.catalog_ddl }));
    source.attested = true;
    source.present = true;
    source.invalid_reply = true;
    try std.testing.expectError(error.SqlMutationOutcomeUnknown, execute(&server, null, .{}, "default", "public", alloc, input));
    try std.testing.expectEqual(@as(usize, 2), source.writes);
    source.invalid_reply = false;
    source.reply_version = 7;
    try std.testing.expectError(error.SqlMutationOutcomeUnknown, execute(&server, null, .{}, "default", "public", alloc, input));
    try std.testing.expectEqual(@as(usize, 3), source.writes);
    var path: SearchPath = .{};
    try path.append(try @import("../pgwire/search_path.zig").Namespace.init("public"));
    try path.append(try @import("../pgwire/search_path.zig").Namespace.init("tenant"));
    source.reply_version = 8;
    // Qualified names ignore the connection path entirely.
    _ = try executeWithPath(&server, null, .{}, "default", "public", alloc, input, &path);
    source.path_lookup = true;
    var unqualified = try @import("antfly_local_sources").sql_compiler.compile(alloc, "DROP INDEX items_id", .{});
    defer unqualified.deinit();
    const path_input: catalog.Ddl = .{ .catalog_ddl = unqualified.statement.catalog_ddl };
    const reads_before = source.reads;
    _ = try executeWithPath(&server, null, .{}, "default", "public", alloc, path_input, &path);
    try std.testing.expectEqual(reads_before + 1, source.reads);
    try std.testing.expectEqual(@as(usize, 5), source.writes);
    try std.testing.expectError(error.Forbidden, executeWithPath(&server, denied, .{}, "default", "public", alloc, path_input, &path));
    try std.testing.expectEqual(@as(usize, 5), source.writes);
    // DROP binds the index itself, not the namespace of a preferred table.
    // Authorization failure on that first owner cannot fall through the path.
    source.shadow_index = true;
    _ = try executeWithPath(&server, null, .{}, "default", "public", alloc, path_input, &path);
    try std.testing.expectError(error.Forbidden, executeWithPath(&server, denied, .{}, "default", "public", alloc, path_input, &path));
    try std.testing.expectEqual(@as(usize, 6), source.writes);
    // An empty connection path must not fall back to the request namespace,
    // and a conditional missing drop must not require catalog availability.
    const empty_path: SearchPath = .{};
    const empty_reads = source.reads;
    const empty_writes = source.writes;
    try std.testing.expectError(error.SqlIndexNotFound, executeWithPath(&server, null, .{}, "default", "public", alloc, path_input, &empty_path));
    var missing = try @import("antfly_local_sources").sql_compiler.compile(alloc, "DROP INDEX IF EXISTS items_id", .{});
    defer missing.deinit();
    const missing_input: catalog.Ddl = .{ .catalog_ddl = missing.statement.catalog_ddl };
    const outcome = try executeWithPath(&server, null, .{}, "default", "public", alloc, missing_input, &empty_path);
    try std.testing.expect(outcome.receipt == null);
    try std.testing.expectEqual(empty_reads, source.reads);
    try std.testing.expectEqual(empty_writes, source.writes);
    var creating = try @import("antfly_local_sources").sql_compiler.compile(alloc, "CREATE INDEX items_id ON items (id)", .{});
    defer creating.deinit();
    try std.testing.expectError(error.TableNotFound, executeWithPath(&server, null, .{}, "default", "public", alloc, .{ .catalog_ddl = creating.statement.catalog_ddl }, &empty_path));
    try std.testing.expectEqual(empty_reads, source.reads);
    try std.testing.expectEqual(empty_writes, source.writes);
    source.path_lookup = false;
    source.shadow_index = false;
    _ = try executeWithPath(&server, null, .{}, "default", "public", alloc, input, &empty_path);
    try std.testing.expectEqual(empty_reads + 1, source.reads);
    try std.testing.expectEqual(empty_writes + 1, source.writes);
}

test "SQL sequence intent cannot fall back to physical resource creation before activation" {
    const Source = struct {
        calls: usize = 0,
        fn status(raw: *anyopaque) !@import("../metadata/api.zig").MetadataStatus {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }
        fn catalogCall(raw: *anyopaque, _: std.mem.Allocator, _: operation.RequestContext, _: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return error.UnexpectedSequenceCatalogAccess;
        }
    };
    const a = std.testing.allocator;
    var source: Source = .{};
    var server = server_mod.ApiHttpServer.init(a, .{}, .{ .ptr = &source, .vtable = &.{ .status = Source.status, .system_catalog = Source.catalogCall, .supports_query_definitions = true } }, null, null);
    defer server.deinit();
    for ([_][]const u8{
        "CREATE SEQUENCE counter CACHE 32", "ALTER SEQUENCE counter RESTART 1000", "DROP SEQUENCE IF EXISTS counter CASCADE",
    }) |sql| {
        var compiled = try @import("antfly_local_sources").sql_compiler.compile(a, sql, .{});
        defer compiled.deinit();
        try std.testing.expectError(error.UnsupportedSqlExecution, execute(&server, null, .{}, "default", "public", a, .{ .catalog_ddl = compiled.statement.catalog_ddl }));
    }
    try std.testing.expectEqual(@as(usize, 0), source.calls);
}

const SequenceProducerFixture = struct {
    const seq = @import("../system_catalog/sequence_catalog.zig");
    const Reply = enum { success, rejected, lost, malformed, invalid_identity, missing };
    existing: enum { absent, sequence, other } = .absent,
    reply: Reply = .success,
    observed: Reply = .success,
    reads: usize = 0,
    writes: usize = 0,
    observations: usize = 0,
    expected_integer: seq.sequence.Integer = .int64,
    expected_action: enum { create, alter, rename, drop } = .create,
    admitted_nonce: [16]u8 = @splat(0),
    owner_missing: bool = false,
    owner_stale: bool = false,
    owner_reads: usize = 0,
    auth_enabled: bool = false,
    identity: ?server_mod.AuthenticatedIdentity = null,
    context: operation.RequestContext = .{},
    path: ?*const SearchPath = null,
    missing_first_namespace: bool = false,
    later_sequence: bool = false,
    expected_namespace: []const u8 = "public",
    cancel_after_mutation: ?*std.atomic.Value(bool) = null,

    fn status(_: *anyopaque) !@import("../metadata/api.zig").MetadataStatus {
        return .{ .metadata_group_id = 1, .metrics = .{} };
    }
    fn result(a: std.mem.Allocator, request: seq.Mutation, reply: Reply) ![]u8 {
        return switch (reply) {
            .lost => error.OutOfMemory,
            .malformed => a.dupe(u8, "{\"success\":"),
            .missing => a.dupe(u8, "null"),
            .rejected => std.json.Stringify.valueAlloc(a, seq.Outcome{ .rejected = .InvalidSequenceDefinition }, .{}),
            .success, .invalid_identity => std.json.Stringify.valueAlloc(a, seq.Outcome{ .success = .{
                .revision = 10,
                .identity = switch (request.operation) {
                    .create => .{ .id = 92, .generation = 1 },
                    .alter => |v| .{ .id = if (reply == .invalid_identity) 93 else v.expected.id, .generation = v.expected.generation + 1 },
                    .rename => |v| v.expected,
                    .drop => null,
                },
                .removed = if (request.operation == .drop) 1 else 0,
            } }, .{}),
        };
    }
    fn call(raw: *anyopaque, a: std.mem.Allocator, context: operation.RequestContext, request: @import("../system_catalog/server_call.zig").Call) ![]u8 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (request != .resolve_many) try std.testing.expect(context.setting_admin);
        switch (request) {
            .resolve_many => |value| {
                self.owner_reads += 1;
                try std.testing.expect(value.include_query_definitions);
                try std.testing.expectEqual(@as(usize, 1), value.relations.len);
                const target = value.relations[0];
                try std.testing.expectEqualStrings("owner_rows", target.name);
                const table: @import("../metadata/table_manager.zig").TableRecord = .{ .table_id = 17, .name = "table:17", .schema_json = "{\"version\":1}" };
                var resolved = domain.ResolvedTable.fromTable(table);
                resolved.query_definition = domain.QueryDefinition.fromTable(table);
                return std.json.Stringify.valueAlloc(a, domain.ResolvedMany{
                    .revision = 9,
                    .tables = &.{},
                    .relations = &.{.{ .table = resolved, .logical_table = target.name, .owner = .{ .table_id = 17, .schema_version = 1, .schema_digest = @splat(1), .kind = .table } }},
                    .relation_epoch = .{ .revision = 9, .incarnation = @splat(7) },
                }, .{});
            },
            .sql_sequence_prepare => return a.dupe(u8, "null"),
            .sql_sequence_snapshot => |value| {
                self.reads += 1;
                try value.validate();
                const record = try seq.sequence.Record.create(.{ .start = 100, .restart = .{ .value = 7 } });
                const entries = try a.alloc(seq.Snapshot.Entry, value.targets.len);
                for (entries, value.targets, 0..) |*entry, target, index| {
                    entry.* = .{
                        .target = target,
                        .namespace_id = if (std.mem.eql(u8, target.namespace, "public")) 2 else 3,
                        .value = switch (if (index > 0 and self.later_sequence) .sequence else self.existing) {
                            .absent => .absent,
                            .other => .other,
                            .sequence => .{ .sequence = try seq.sequence.Descriptor.init(44, record.definition) },
                        },
                        .counter = if (value.include_counters and self.existing == .sequence) record.state else null,
                    };
                    if (index == 0 and self.missing_first_namespace) {
                        entry.namespace_id = null;
                        entry.value = .missing_namespace;
                        entry.counter = null;
                    }
                }
                if (value.owner != null and self.owner_stale) return error.CatalogGenerationChanged;
                return std.json.Stringify.valueAlloc(a, seq.Snapshot{ .revision = 9, .head = 3, .incarnation = @splat(7), .entries = entries, .owner = if (value.owner != null and !self.owner_missing) .{
                    .namespace_id = if (std.mem.eql(u8, value.owner.?.guard.target.namespace, "public")) 2 else 3,
                    .column = .{ .table_id = 17, .column_id = 9 },
                } else null }, .{});
            },
            .sql_sequence_mutate => |value| {
                self.writes += 1;
                try value.validate();
                try std.testing.expectEqual(@as(u64, 3), value.admission.head);
                try std.testing.expectEqualSlices(u8, &@as([16]u8, @splat(7)), &value.incarnation);
                try std.testing.expectEqualStrings("counter", value.target.table);
                try std.testing.expectEqualStrings(self.expected_namespace, value.target.namespace);
                try std.testing.expectEqual(@as(u64, if (std.mem.eql(u8, self.expected_namespace, "public")) 2 else 3), value.namespace_id);
                self.admitted_nonce = value.admission.nonce;
                try std.testing.expectEqualStrings(@tagName(self.expected_action), @tagName(value.operation));
                switch (value.operation) {
                    .create => |v| {
                        try std.testing.expectEqual(self.expected_integer, v.options.integer orelse .int64);
                        if (self.owner_reads != 0) {
                            try std.testing.expectEqualDeep(seq.sequence.OwnedBy{ .table_id = 17, .column_id = 9 }, v.options.owned_by.column);
                            try std.testing.expectEqual(@as(u64, 17), value.owner_guard.?.owner.table_id);
                        }
                    },
                    .alter => |v| {
                        try std.testing.expectEqualDeep(seq.Identity{ .id = 44, .generation = 1 }, v.expected);
                        try std.testing.expectEqual(@as(?i64, 8), v.options.restart.?.value);
                    },
                    .rename => |v| try std.testing.expectEqualStrings("renamed", v.name),
                    .drop => {},
                }
                if (self.cancel_after_mutation) |token| token.store(true, .release);
                return result(a, value, self.reply);
            },
            .sql_sequence_outcome => |value| {
                self.observations += 1;
                try std.testing.expectEqualSlices(u8, &self.admitted_nonce, &value.admission.nonce);
                try std.testing.expectEqual(@as(u64, 3), value.admission.head);
                return result(a, value, self.observed);
            },
            else => return error.UnexpectedSequenceCatalogAccess,
        }
    }
    fn run(self: *@This(), sql: []const u8) !catalog.DdlOutcome {
        const a = std.testing.allocator;
        var server = server_mod.ApiHttpServer.init(a, .{ .auth_enabled = self.auth_enabled, .imported_runtime_io = .{ .api = std.testing.io } }, .{ .ptr = self, .vtable = &.{ .status = status, .system_catalog = call, .supports_query_definitions = true, .supports_sql_sequences = true } }, null, null);
        defer server.deinit();
        var compiled = try @import("antfly_local_sources").sql_compiler.compile(a, sql, .{});
        defer compiled.deinit();
        return executeWithPath(&server, self.identity, self.context, "default", "public", a, .{ .catalog_ddl = compiled.statement.catalog_ddl }, self.path);
    }
    fn runCommitted(self: *@This(), sql: []const u8) !void {
        const outcome = try self.run(sql);
        defer if (outcome.catalog_receipt) |receipt| receipt.deinit(std.testing.allocator);
        try std.testing.expectEqual(catalog.MutationOutcome.committed, outcome.mutation_outcome.?);
        if (outcome.catalog_receipt) |receipt| try std.testing.expectEqual(.committed, receipt.state);
    }
    fn recover(self: *@This(), token: []const u8) !catalog.CatalogOperationReceipt {
        const a = std.testing.allocator;
        // Disabling new DDL must not disable observation of old admissions.
        var server = server_mod.ApiHttpServer.init(a, .{ .auth_enabled = self.auth_enabled }, .{ .ptr = self, .vtable = &.{ .status = status, .system_catalog = call, .supports_query_definitions = true } }, null, null);
        defer server.deinit();
        return @import("sql_catalog_recovery.zig").recover(&server, self.identity, self.context, a, token);
    }
};

test "SQL sequence intent producer binds typed options and actual identities without table receipts" {
    for ([_]struct { sql: []const u8, integer: SequenceProducerFixture.seq.sequence.Integer }{
        .{ .sql = "CREATE SEQUENCE counter AS smallint", .integer = .int16 },
        .{ .sql = "CREATE SEQUENCE counter AS integer", .integer = .int32 },
        .{ .sql = "CREATE SEQUENCE counter AS pg_catalog.int8", .integer = .int64 },
    }) |example| {
        var source: SequenceProducerFixture = .{ .expected_integer = example.integer };
        const outcome = try source.run(example.sql);
        defer outcome.catalog_receipt.?.deinit(std.testing.allocator);
        try std.testing.expect(outcome.receipt == null);
        try std.testing.expectEqual(@as(?u64, 92), outcome.catalog_receipt.?.object_id);
        try std.testing.expectEqual(@as(usize, 1), source.reads);
        try std.testing.expectEqual(@as(usize, 1), source.writes);
        try std.testing.expectEqual(@as(usize, 0), source.observations);
    }
    var alter: SequenceProducerFixture = .{ .existing = .sequence, .expected_action = .alter };
    try alter.runCommitted("ALTER SEQUENCE counter RESTART 8");
    var rename: SequenceProducerFixture = .{ .existing = .sequence, .expected_action = .rename };
    try rename.runCommitted("ALTER SEQUENCE counter RENAME TO renamed");
    var drop: SequenceProducerFixture = .{ .existing = .sequence, .expected_action = .drop };
    try drop.runCommitted("DROP SEQUENCE counter");
}

test "SQL sequence intent producer preserves PostgreSQL conditional and option error precedence" {
    var source: SequenceProducerFixture = .{ .existing = .other };
    try source.runCommitted("CREATE SEQUENCE IF NOT EXISTS counter AS text CACHE 0 OWNED BY missing.id");
    try std.testing.expectEqual(@as(usize, 0), source.writes);
    try std.testing.expectError(error.CatalogAlreadyExists, source.run("CREATE SEQUENCE counter CACHE 0"));
    try std.testing.expectError(error.SqlWrongObjectType, source.run("ALTER SEQUENCE counter CACHE 0"));
    source.existing = .absent;
    try source.runCommitted("ALTER SEQUENCE IF EXISTS counter CACHE 0");
    try source.runCommitted("DROP SEQUENCE IF EXISTS counter");
    try std.testing.expectError(error.SequenceNotFound, source.run("DROP SEQUENCE counter"));
    try std.testing.expectError(error.InvalidSequenceDefinition, source.run("CREATE SEQUENCE counter CACHE 0 OWNED BY missing.id"));
    try std.testing.expectError(error.InvalidSequenceDefinition, source.run("CREATE SEQUENCE counter AS text"));
    try std.testing.expectError(error.SqlTypeNotFound, source.run("CREATE SEQUENCE counter AS \"integer\""));
    try std.testing.expectError(error.SqlTypeNotFound, source.run("CREATE SEQUENCE counter AS pg_catalog.bigint"));
    try std.testing.expectError(error.UnsupportedSqlExecution, source.run("DROP SEQUENCE counter, another"));
    try std.testing.expectEqual(@as(usize, 0), source.writes);
}

test "SQL sequence intent producer binds stable owned attributes and rejects stale or cross schema owners" {
    var source: SequenceProducerFixture = .{};
    try source.runCommitted("CREATE SEQUENCE counter OWNED BY owner_rows.id");
    try std.testing.expectEqual(@as(usize, 2), source.reads);
    try std.testing.expectEqual(@as(usize, 1), source.owner_reads);
    try std.testing.expectEqual(@as(usize, 1), source.writes);
    source = .{ .owner_missing = true };
    try std.testing.expectError(error.UndefinedColumn, source.run("CREATE SEQUENCE counter OWNED BY owner_rows.missing"));
    try std.testing.expectEqual(@as(usize, 0), source.writes);
    source = .{ .owner_stale = true };
    try std.testing.expectError(error.CatalogGenerationChanged, source.run("CREATE SEQUENCE counter OWNED BY owner_rows.id"));
    try std.testing.expectEqual(@as(usize, 0), source.writes);
    source = .{};
    try std.testing.expectError(error.SqlSequenceOwnershipScope, source.run("CREATE SEQUENCE counter OWNED BY tenant.owner_rows.id"));
    try std.testing.expectEqual(@as(usize, 0), source.writes);
}

test "SQL sequence intent producer rejects unauthenticated denied expired and cancelled requests before catalog access" {
    var source: SequenceProducerFixture = .{ .auth_enabled = true };
    try std.testing.expectError(error.Forbidden, source.run("CREATE SEQUENCE counter"));
    source.identity = .{ .username = @constCast("reader") };
    try std.testing.expectError(error.Forbidden, source.run("CREATE SEQUENCE counter"));
    source.auth_enabled = false;
    source.identity = null;
    source.context.deadline_ns = 0;
    try std.testing.expectError(error.DeadlineExceeded, source.run("CREATE SEQUENCE counter"));
    var cancelled = std.atomic.Value(bool).init(true);
    source.context = .{ .cancellation = operation.CancellationToken.fromAtomic(&cancelled) };
    try std.testing.expectError(error.Canceled, source.run("CREATE SEQUENCE counter"));
    try std.testing.expectEqual(@as(usize, 0), source.reads);
    try std.testing.expectEqual(@as(usize, 0), source.writes);
}

test "SQL sequence intent producer observes ambiguous admissions once without replay" {
    for ([_]SequenceProducerFixture.Reply{ .lost, .malformed }) |reply| {
        var source: SequenceProducerFixture = .{ .reply = reply };
        try source.runCommitted("CREATE SEQUENCE counter");
        try std.testing.expectEqual(@as(usize, 1), source.writes);
        try std.testing.expectEqual(@as(usize, 1), source.observations);
        source = .{ .reply = reply, .observed = .rejected };
        try std.testing.expectError(error.InvalidSequenceDefinition, source.run("CREATE SEQUENCE counter"));
        try std.testing.expectEqual(@as(usize, 1), source.writes);
        try std.testing.expectEqual(@as(usize, 1), source.observations);
        for ([_]SequenceProducerFixture.Reply{ .missing, .lost, .malformed }) |observation| {
            source = .{ .reply = reply, .observed = observation };
            const unknown = try source.run("CREATE SEQUENCE counter");
            defer unknown.catalog_receipt.?.deinit(std.testing.allocator);
            try std.testing.expect(unknown.mutation_outcome == null);
            try std.testing.expectEqual(.unknown, unknown.catalog_receipt.?.state);
            try std.testing.expectEqualStrings("40003", unknown.catalog_receipt.?.diagnostic.?.code);
            try std.testing.expect(!unknown.catalog_receipt.?.diagnostic.?.retryable);
            try std.testing.expectEqual(@as(usize, 1), source.writes);
            try std.testing.expectEqual(@as(usize, 1), source.observations);
        }
    }
    var rejected: SequenceProducerFixture = .{ .reply = .rejected };
    try std.testing.expectError(error.InvalidSequenceDefinition, rejected.run("CREATE SEQUENCE counter"));
    try std.testing.expectEqual(@as(usize, 0), rejected.observations);
    var altered: SequenceProducerFixture = .{ .existing = .sequence, .expected_action = .alter, .reply = .invalid_identity };
    try altered.runCommitted("ALTER SEQUENCE counter RESTART 8");
    try std.testing.expectEqual(@as(usize, 1), altered.writes);
    try std.testing.expectEqual(@as(usize, 1), altered.observations);
}

test "SQL sequence intent producer honors ordered shadows and the first existing create namespace" {
    var path: SearchPath = .{};
    try path.append(try @import("../pgwire/search_path.zig").Namespace.init("public"));
    try path.append(try @import("../pgwire/search_path.zig").Namespace.init("tenant"));
    var source: SequenceProducerFixture = .{ .path = &path, .later_sequence = true };
    // CREATE uses the first existing namespace; a later same-named sequence
    // does not make the selected empty namespace look occupied.
    try source.runCommitted("CREATE SEQUENCE counter");
    source = .{ .path = &path, .existing = .other, .later_sequence = true };
    try std.testing.expectError(error.SqlWrongObjectType, source.run("DROP SEQUENCE counter"));
    try std.testing.expectEqual(@as(usize, 0), source.writes);
    source = .{ .path = &path, .missing_first_namespace = true, .expected_namespace = "tenant" };
    try source.runCommitted("CREATE SEQUENCE counter");
    const empty: SearchPath = .{};
    source = .{ .path = &empty };
    try std.testing.expectError(error.NamespaceNotFound, source.run("CREATE SEQUENCE counter"));
    try source.runCommitted("DROP SEQUENCE IF EXISTS counter");
    try std.testing.expectEqual(@as(usize, 0), source.reads);
}

test "SQL sequence intent producer preserves unknown outcome after post admission cancellation" {
    var cancelled = std.atomic.Value(bool).init(false);
    var source: SequenceProducerFixture = .{
        .reply = .lost,
        .cancel_after_mutation = &cancelled,
        .context = .{ .cancellation = operation.CancellationToken.fromAtomic(&cancelled) },
    };
    const unknown = try source.run("CREATE SEQUENCE counter");
    defer unknown.catalog_receipt.?.deinit(std.testing.allocator);
    try std.testing.expect(unknown.mutation_outcome == null);
    try std.testing.expectEqual(.unknown, unknown.catalog_receipt.?.state);
    try std.testing.expectEqual(@as(usize, 1), source.writes);
    try std.testing.expectEqual(@as(usize, 0), source.observations);
}

test "SQL catalog recovery observes retained receipts after ingress disable without writes or rebinding" {
    var source: SequenceProducerFixture = .{ .reply = .lost, .observed = .missing };
    const unknown = try source.run("CREATE SEQUENCE counter");
    defer unknown.catalog_receipt.?.deinit(std.testing.allocator);
    const token = unknown.catalog_receipt.?.recovery_token;
    try std.testing.expectEqual(.unknown, unknown.catalog_receipt.?.state);
    const reads = source.reads;
    for ([_]SequenceProducerFixture.Reply{ .success, .rejected, .missing, .malformed, .lost }) |reply| {
        source.observed = reply;
        const receipt = try source.recover(token);
        defer receipt.deinit(std.testing.allocator);
        try std.testing.expectEqualStrings(token, receipt.recovery_token);
        try std.testing.expectEqualStrings("counter", receipt.name);
        if (reply == .success) {
            try std.testing.expectEqual(.committed, receipt.state);
            try std.testing.expectEqual(@as(?u64, 92), receipt.object_id);
        } else if (reply == .rejected) {
            try std.testing.expectEqual(.rejected, receipt.state);
            try std.testing.expectEqualStrings("22023", receipt.diagnostic.?.code);
        } else {
            try std.testing.expectEqual(.unknown, receipt.state);
            try std.testing.expectEqualStrings("40003", receipt.diagnostic.?.code);
            try std.testing.expect(!receipt.diagnostic.?.retryable);
        }
    }
    try std.testing.expectEqual(reads, source.reads);
    try std.testing.expectEqual(@as(usize, 1), source.writes);
    const observed = source.observations;
    source.auth_enabled = true;
    try std.testing.expectError(error.Forbidden, source.recover(token));
    source.identity = .{ .username = @constCast("reader") };
    try std.testing.expectError(error.Forbidden, source.recover(token));
    source.auth_enabled = false;
    source.identity = null;
    try std.testing.expectError(error.InvalidSqlRecoveryToken, source.recover("not.a.token"));
    source.context.deadline_ns = 0;
    try std.testing.expectError(error.DeadlineExceeded, source.recover(token));
    try std.testing.expectEqual(observed, source.observations);
}

test "SQL catalog recovery tokens are bounded versioned owned and retain exact unsigned identities" {
    const recovery = @import("sql_catalog_recovery.zig");
    const a = std.testing.allocator;
    var source: SequenceProducerFixture = .{};
    const outcome = try source.run("CREATE SEQUENCE counter");
    defer outcome.catalog_receipt.?.deinit(a);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const decoded = try recovery.decode(arena.allocator(), outcome.catalog_receipt.?.recovery_token);
    try std.testing.expectEqualStrings("counter", decoded.request.target.table);
    try std.testing.expectEqualSlices(u8, &source.admitted_nonce, &decoded.request.admission.nonce);
    var receipt = try recovery.newReceipt(a, arena.allocator(), decoded.request, decoded.observed_revision);
    defer receipt.deinit(a);
    {
        // C/embedded encoders need the same omission policy as generated HTTP,
        // even when their parent encoder defaults to emitting optional nulls.
        const unknown_json = try std.json.Stringify.valueAlloc(a, receipt, .{});
        defer a.free(unknown_json);
        var parsed = try std.json.parseFromSlice(std.json.Value, a, unknown_json, .{});
        defer parsed.deinit();
        try std.testing.expect(!parsed.value.object.contains("object_id"));
        try std.testing.expect(!parsed.value.object.contains("generation"));
        try std.testing.expect(!parsed.value.object.contains("catalog_revision"));
    }
    recovery.complete(&receipt, .{ .success = .{ .revision = std.math.maxInt(u64), .identity = .{ .id = std.math.maxInt(u64), .generation = 9007199254740993 }, .removed = 0 } });
    const json = try std.json.Stringify.valueAlloc(a, receipt, .{ .emit_null_optional_fields = false });
    defer a.free(json);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"object_id\":\"18446744073709551615\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"generation\":\"9007199254740993\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"catalog_revision\":\"18446744073709551615\"") != null);
    {
        const committed_json = try std.json.Stringify.valueAlloc(a, receipt, .{});
        defer a.free(committed_json);
        var parsed = try std.json.parseFromSlice(std.json.Value, a, committed_json, .{});
        defer parsed.deinit();
        try std.testing.expect(!parsed.value.object.contains("diagnostic"));
        try std.testing.expectEqualStrings("18446744073709551615", parsed.value.object.get("object_id").?.string);
    }
    for ([_][]const u8{ "", "!", "e30", "bnVsbA", "AA" }) |invalid| try std.testing.expectError(error.InvalidSqlRecoveryToken, recovery.decode(arena.allocator(), invalid));
    const oversized = try a.alloc(u8, recovery.max_token_bytes + 1);
    defer a.free(oversized);
    @memset(oversized, 'a');
    try std.testing.expectError(error.InvalidSqlRecoveryToken, recovery.decode(arena.allocator(), oversized));
    const future = try std.json.Stringify.valueAlloc(a, recovery.Token{ .version = 2, .kind = .sequence, .observed_revision = 10, .request = decoded.request }, .{});
    defer a.free(future);
    const encoded = try a.alloc(u8, std.base64.url_safe_no_pad.Encoder.calcSize(future.len));
    defer a.free(encoded);
    _ = std.base64.url_safe_no_pad.Encoder.encode(encoded, future);
    try std.testing.expectError(error.InvalidSqlRecoveryToken, recovery.decode(arena.allocator(), encoded));
}

test "SQL catalog recovery HTTP observes exact tokens with disabled ingress and rejects malformed requests before reads" {
    const httpx = @import("httpx");
    const a = std.testing.allocator;
    var source: SequenceProducerFixture = .{ .reply = .lost, .observed = .missing };
    const outcome = try source.run("CREATE SEQUENCE counter");
    defer outcome.catalog_receipt.?.deinit(a);
    const token = outcome.catalog_receipt.?.recovery_token;
    var server = server_mod.ApiHttpServer.init(a, .{ .imported_runtime_io = .{ .api = std.testing.io } }, .{ .ptr = &source, .vtable = &.{ .status = SequenceProducerFixture.status, .system_catalog = SequenceProducerFixture.call, .supports_query_definitions = true } }, null, null);
    defer server.deinit();
    var handler: @import("httpx_handler.zig").AntflyApiHandler = .{ .api_server = &server };
    for ([_]SequenceProducerFixture.Reply{ .missing, .success, .rejected }) |reply| {
        source.observed = reply;
        var request = try httpx.Request.init(a, .POST, "http://localhost/db/v1/sql/operations/status");
        defer request.deinit();
        const body = try std.json.Stringify.valueAlloc(a, .{ .recovery_token = token }, .{});
        defer a.free(body);
        request.body = body;
        var context = httpx.Context.init(a, std.testing.io, &request);
        defer context.deinit();
        var response = try handler.getSQLCatalogOperation(&context);
        defer response.deinit();
        try std.testing.expectEqual(@as(u16, 200), response.status.code);
        const parsed = try std.json.parseFromSlice(std.json.Value, a, response.body.?, .{});
        defer parsed.deinit();
        const object = parsed.value.object;
        try std.testing.expectEqualStrings(token, object.get("recovery_token").?.string);
        try std.testing.expectEqualStrings(if (reply == .missing) "unknown" else if (reply == .success) "committed" else "rejected", object.get("state").?.string);
        if (reply == .success) try std.testing.expectEqualStrings("92", object.get("object_id").?.string);
    }
    const observations = source.observations;
    for ([_][]const u8{ "{}", "{\"recovery_token\":\"bad.token\"}", "{\"recovery_token\":123}", "{\"recovery_token\":\"x\",\"unknown\":true}" }) |body| {
        var request = try httpx.Request.init(a, .POST, "http://localhost/db/v1/sql/operations/status");
        defer request.deinit();
        request.body = body;
        var context = httpx.Context.init(a, std.testing.io, &request);
        defer context.deinit();
        var response = try handler.getSQLCatalogOperation(&context);
        defer response.deinit();
        try std.testing.expectEqual(@as(u16, 400), response.status.code);
    }
    try std.testing.expectEqual(observations, source.observations);
    try std.testing.expectEqual(@as(usize, 1), source.writes);
    try std.testing.expectEqual(@as(usize, 1), source.reads);
}

test "SQL catalog recovery receipt allocation failures reject before mutation admission" {
    const Check = struct {
        fn run(result_alloc: std.mem.Allocator) !void {
            const a = std.testing.allocator;
            var source: SequenceProducerFixture = .{};
            var server = server_mod.ApiHttpServer.init(a, .{ .imported_runtime_io = .{ .api = std.testing.io } }, .{ .ptr = &source, .vtable = &.{ .status = SequenceProducerFixture.status, .system_catalog = SequenceProducerFixture.call, .supports_query_definitions = true, .supports_sql_sequences = true } }, null, null);
            defer server.deinit();
            var compiled = try @import("antfly_local_sources").sql_compiler.compile(a, "CREATE SEQUENCE counter", .{});
            defer compiled.deinit();
            const outcome = executeWithAllocators(&server, null, .{}, "default", "public", .{ .result = result_alloc, .scratch = a }, .{ .catalog_ddl = compiled.statement.catalog_ddl }, null) catch |err| {
                try std.testing.expectEqual(@as(usize, 0), source.writes);
                try std.testing.expectEqual(@as(usize, 0), source.observations);
                return err;
            };
            defer outcome.catalog_receipt.?.deinit(result_alloc);
            try std.testing.expectEqual(@as(usize, 1), source.writes);
            try std.testing.expectEqual(.committed, outcome.catalog_receipt.?.state);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}

test "SQL catalog ALTER submits native schema CAS without client generations" {
    const Source = struct {
        updates: usize = 0,
        table_lookup: bool = false,
        wrong_kind: bool = false,
        attested: bool = true,
        mutation_failure: ?anyerror = null,
        mutation_attempts: usize = 0,
        resolutions: usize = 0,
        earlier_wrong_kind: bool = false,
        path_lookup: bool = false,
        index_exists: bool = false,
        pending_read_schema: bool = false,
        finalization_seen: bool = false,
        foreign_finalized_owner: bool = false,
        external_lake: bool = false,
        uncertain: bool = false,
        scheduler_server: ?*server_mod.ApiHttpServer = null,
        fn schema(self: *const @This()) []const u8 {
            return if (self.external_lake)
                "{\"version\":7,\"storage_mode\":\"relational\",\"base_source\":{\"kind\":\"external\",\"format\":\"parquet\",\"uri\":\"file:///fixture\",\"table_id\":\"items\",\"schema_fingerprint\":\"auto\"},\"default_type\":\"row\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"}},\"additionalProperties\":false}}}}"
            else
                "{\"version\":7,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"}},\"additionalProperties\":false}}}}";
        }
        fn status(_: *anyopaque) !@import("../metadata/api.zig").MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }
        fn run(raw: *anyopaque, alloc: std.mem.Allocator, context: operation.RequestContext, call: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (call == .relation_schema_mutate) {
                self.mutation_attempts += 1;
                try std.testing.expect(context.setting_admin);
                try std.testing.expectEqual(@as(u64, 17), call.relation_schema_mutate.guard.owner.table_id);
                try std.testing.expectEqualStrings("items", call.relation_schema_mutate.guard.logical_table);
                try std.testing.expectEqualSlices(u8, &@as([16]u8, @splat(1)), &call.relation_schema_mutate.guard.incarnation);
                if (self.table_lookup) try std.testing.expectEqual(.table, call.relation_schema_mutate.guard.owner.kind);
                if (self.path_lookup) try std.testing.expectEqualStrings("tenant", call.relation_schema_mutate.guard.target.namespace);
                if (self.mutation_failure) |err| return err;
                var result = try mutate(raw, alloc, "table:immutable-17", .replace, call.relation_schema_mutate.schema_json, call.relation_schema_mutate.guard.owner.schema_version);
                defer result.deinit(alloc);
                return std.json.Stringify.valueAlloc(alloc, @import("../system_catalog/server_call.zig").RelationReplacementResult{ .schema_version = result.version }, .{});
            }
            try std.testing.expect(call == .resolve_many);
            self.resolutions += 1;
            try std.testing.expectEqual(@as(usize, 0), call.resolve_many.targets.len);
            if (call.resolve_many.relations.len != 0) {
                const stride: usize = if (self.table_lookup) 1 else 2;
                try std.testing.expectEqual(stride * @as(usize, if (self.path_lookup) 2 else 1), call.resolve_many.relations.len);
                try std.testing.expectEqualStrings("items", call.resolve_many.relations[0].name);
                var table: @import("../metadata/table_manager.zig").TableRecord = .{ .table_id = 17, .name = "table:immutable-17", .schema_json = self.schema() };
                const pending = self.pending_read_schema;
                self.pending_read_schema = false;
                if (self.foreign_finalized_owner and self.finalization_seen) {
                    table.table_id = 18;
                    table.name = "table:immutable-18";
                }
                if (pending) {
                    table.read_schema_json = "{\"version\":6}";
                    self.finalization_seen = true;
                }
                var resolved = domain.ResolvedTable.fromTable(table);
                resolved.query_definition = domain.QueryDefinition.fromTable(table);
                var relations: [4]?domain.ResolvedRelation = @splat(null);
                const selected: usize = if (self.path_lookup) stride else 0;
                const table_owner: domain.ResolvedRelation = .{ .table = resolved, .logical_table = "items", .owner = .{ .table_id = table.table_id, .schema_version = 7, .schema_digest = @splat(1), .kind = .table } };
                var index_owner = table_owner;
                index_owner.owner.kind = .index;
                relations[selected] = if (self.wrong_kind) index_owner else table_owner;
                if (self.index_exists) relations[selected + 1] = index_owner;
                if (self.path_lookup) {
                    try std.testing.expectEqualStrings("public", call.resolve_many.relations[0].namespace);
                    try std.testing.expectEqualStrings("tenant", call.resolve_many.relations[stride].namespace);
                    // An index in an earlier namespace with no matching
                    // table must not collide with the selected table's index.
                    if (!self.table_lookup) relations[1] = index_owner;
                    if (self.earlier_wrong_kind) relations[0] = index_owner;
                }
                return std.json.Stringify.valueAlloc(alloc, domain.ResolvedMany{ .revision = 9, .tables = &.{}, .relations = relations[0..call.resolve_many.relations.len], .relation_epoch = if (self.attested) .{ .revision = 9, .incarnation = @splat(1) } else null }, .{});
            }
            return std.json.Stringify.valueAlloc(alloc, .{ .revision = 9, .tables = .{.{ .table_id = 17, .name = "table:immutable-17", .query_definition = .{ .schema_json = self.schema(), .read_schema_json = "", .indexes_json = "{}" } }} }, .{});
        }
        fn mutate(raw: *anyopaque, alloc: std.mem.Allocator, name: []const u8, mode: tables.SchemaMutationMode, body: []const u8, expected: ?u32) !tables.SchemaMutationResult {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.scheduler_server) |scheduler| {
                try std.testing.expectEqual(@as(usize, 1), scheduler.index_installation_reserved_slots);
                if (self.uncertain) return error.MetadataMutationOutcomeUnknown;
            }
            self.updates += 1;
            try std.testing.expectEqualStrings("table:immutable-17", name);
            try std.testing.expectEqual(tables.SchemaMutationMode.replace, mode);
            try std.testing.expectEqual(@as(?u32, 7), expected);
            var parsed_json = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
            defer parsed_json.deinit();
            try std.testing.expect(parsed_json.value.object.get("version") == null);
            var parsed = try tables.parseValidatedTableSchema(alloc, body);
            defer parsed.deinit(alloc);
            return .{ .version = 8, .schema_json = try alloc.dupe(u8, body) };
        }
    };
    const alloc = std.testing.allocator;
    var source: Source = .{};
    var server = server_mod.ApiHttpServer.init(alloc, .{}, .{ .ptr = &source, .vtable = &.{ .status = Source.status, .system_catalog = Source.run, .supports_query_definitions = true } }, null, null);
    defer server.deinit();
    for ([_][]const u8{ "CREATE INDEX items_id ON items (id)", "ALTER TABLE items ADD COLUMN label TEXT", "CREATE INDEX expression_key ON items ((id + 1) DESC NULLS LAST) WHERE id > 0 AND id < 100" }) |sql| {
        var compiled = try @import("antfly_local_sources").sql_compiler.compile(alloc, sql, .{});
        defer compiled.deinit();
        source.table_lookup = compiled.statement.catalog_ddl.action == .alter_schema;
        try std.testing.expectEqual(catalog.MutationOutcome.committed, (try execute(&server, null, .{}, "default", "public", alloc, .{ .catalog_ddl = compiled.statement.catalog_ddl })).mutation_outcome);
    }
    try std.testing.expectEqual(@as(usize, 3), source.updates);
    var result_arena = std.heap.ArenaAllocator.init(alloc);
    defer result_arena.deinit();
    var unique = try @import("antfly_local_sources").sql_compiler.compile(alloc, "CREATE UNIQUE INDEX unique_id ON items (id)", .{});
    defer unique.deinit();
    // This schema-CAS fixture has no data-owner reader or private build port.
    // It cannot publish a regular UNIQUE declaration before validating rows.
    try std.testing.expectError(error.UnsupportedSqlExecution, execute(&server, null, .{}, "default", "public", result_arena.allocator(), .{ .catalog_ddl = unique.statement.catalog_ddl }));
    try std.testing.expectEqual(@as(usize, 3), source.updates);
    // Only an explicitly unvalidated CHECK may use this metadata-only port.
    // Validated constraints require the staged historical-row barrier below.
    var check = try @import("antfly_local_sources").sql_compiler.compile(alloc, "ALTER TABLE items ADD CONSTRAINT positive CHECK (id > 0 AND id < 100) NOT VALID", .{});
    defer check.deinit();
    source.table_lookup = true;
    const checked = try execute(&server, null, .{}, "default", "public", result_arena.allocator(), .{ .catalog_ddl = check.statement.catalog_ddl });
    try std.testing.expectEqual(catalog.MutationOutcome.committed_pending, checked.mutation_outcome);
    try std.testing.expectEqual(@as(usize, 4), source.updates);
    for ([_][]const u8{
        "ALTER TABLE items ADD COLUMN n integer DEFAULT (2+3)",
        "ALTER TABLE items ADD COLUMN g bigint GENERATED ALWAYS AS (id+1) STORED",
        "ALTER TABLE items ADD CONSTRAINT positive CHECK (id > 0 AND id < 100)",
        "ALTER TABLE items ADD CONSTRAINT unique_id UNIQUE(id)",
    }) |sql| {
        var compiled = try @import("antfly_local_sources").sql_compiler.compile(alloc, sql, .{});
        defer compiled.deinit();
        try std.testing.expectError(error.SqlSchemaRewriteRequiresMetadataOwner, execute(&server, null, .{}, "default", "public", result_arena.allocator(), .{ .catalog_ddl = compiled.statement.catalog_ddl }));
        try std.testing.expectEqual(@as(usize, 4), source.updates);
    }
    // SET NOT NULL uses the retained validation scan rather than a rewrite.
    // This metadata-only fixture has no native reader or shared Io provider.
    var not_null = try @import("antfly_local_sources").sql_compiler.compile(alloc, "ALTER TABLE items ALTER COLUMN id SET NOT NULL", .{});
    defer not_null.deinit();
    try std.testing.expectError(error.UnsupportedSqlExecution, execute(&server, null, .{}, "default", "public", result_arena.allocator(), .{ .catalog_ddl = not_null.statement.catalog_ddl }));
    try std.testing.expectEqual(@as(usize, 4), source.updates);
    var path: SearchPath = .{};
    var guarded_alter = try @import("antfly_local_sources").sql_compiler.compile(alloc, "ALTER TABLE items ADD COLUMN spare TEXT", .{});
    defer guarded_alter.deinit();
    const guarded_input: catalog.Ddl = .{ .catalog_ddl = guarded_alter.statement.catalog_ddl };
    const attempts = source.mutation_attempts;
    source.wrong_kind = true;
    try std.testing.expectError(error.SqlWrongObjectType, execute(&server, null, .{}, "default", "public", alloc, guarded_input));
    source.wrong_kind = false;
    source.attested = false;
    try std.testing.expectError(error.TableTopologyUpgradeRequired, execute(&server, null, .{}, "default", "public", alloc, guarded_input));
    source.attested = true;
    try std.testing.expectEqual(attempts, source.mutation_attempts);
    source.mutation_failure = error.CatalogGenerationChanged;
    try std.testing.expectError(error.CatalogGenerationChanged, execute(&server, null, .{}, "default", "public", alloc, guarded_input));
    try std.testing.expectEqual(attempts + 1, source.mutation_attempts);
    try std.testing.expectEqual(@as(usize, 4), source.updates);
    source.mutation_failure = null;
    try path.append(try @import("../pgwire/search_path.zig").Namespace.init("public"));
    try path.append(try @import("../pgwire/search_path.zig").Namespace.init("tenant"));
    source.path_lookup = true;
    source.table_lookup = false;
    var scoped = try @import("antfly_local_sources").sql_compiler.compile(alloc, "CREATE INDEX items_id ON items (id)", .{});
    defer scoped.deinit();
    const scoped_input: catalog.Ddl = .{ .catalog_ddl = scoped.statement.catalog_ddl };
    _ = try executeWithPath(&server, null, .{}, "default", "public", alloc, scoped_input, &path);
    try std.testing.expectEqual(@as(usize, 5), source.updates);
    source.index_exists = true;
    try std.testing.expectError(error.SqlIndexAlreadyExists, executeWithPath(&server, null, .{}, "default", "public", alloc, scoped_input, &path));
    var conditional_index = try @import("antfly_local_sources").sql_compiler.compile(alloc, "CREATE INDEX IF NOT EXISTS items_id ON items (id)", .{});
    defer conditional_index.deinit();
    _ = try executeWithPath(&server, null, .{}, "default", "public", alloc, .{ .catalog_ddl = conditional_index.statement.catalog_ddl }, &path);
    try std.testing.expectEqual(@as(usize, 5), source.updates);
    source.table_lookup = true;
    source.index_exists = false;
    const scoped_reads = source.resolutions;
    const scoped_check = try executeWithPath(&server, null, .{}, "default", "public", result_arena.allocator(), .{ .catalog_ddl = check.statement.catalog_ddl }, &path);
    try std.testing.expectEqual(scoped_reads + 1, source.resolutions);
    try std.testing.expectEqual(@as(usize, 6), source.updates);
    try std.testing.expectEqualStrings("tenant", scoped_check.receipt.?.namespace);
    const scoped_attempts = source.mutation_attempts;
    source.earlier_wrong_kind = true;
    try std.testing.expectError(error.SqlWrongObjectType, executeWithPath(&server, null, .{}, "default", "public", alloc, guarded_input, &path));
    const denied: server_mod.AuthenticatedIdentity = .{ .username = @constCast("reader") };
    try std.testing.expectError(error.Forbidden, executeWithPath(&server, denied, .{}, "default", "public", alloc, guarded_input, &path));
    source.earlier_wrong_kind = false;
    try std.testing.expectError(error.Forbidden, executeWithPath(&server, denied, .{}, "default", "public", alloc, guarded_input, &path));
    try std.testing.expectEqual(scoped_attempts, source.mutation_attempts);
    const empty_path: SearchPath = .{};
    const empty_reads = source.resolutions;
    try std.testing.expectError(error.TableNotFound, executeWithPath(&server, null, .{}, "default", "public", alloc, guarded_input, &empty_path));
    try std.testing.expectEqual(empty_reads, source.resolutions);
    try std.testing.expectEqual(scoped_attempts, source.mutation_attempts);
    source.path_lookup = false;
    var qualified_alter = try @import("antfly_local_sources").sql_compiler.compile(alloc, "ALTER TABLE public.items ADD COLUMN direct TEXT", .{});
    defer qualified_alter.deinit();
    _ = try executeWithPath(&server, null, .{}, "default", "tenant", alloc, .{ .catalog_ddl = qualified_alter.statement.catalog_ddl }, &empty_path);
    try std.testing.expectEqual(empty_reads + 1, source.resolutions);
    try std.testing.expectEqual(@as(usize, 7), source.updates);
    // Relaxation needs only the same guarded metadata CAS, never an unguarded
    // table update or a full-table rewrite. Tightening above requires the
    // metadata-owned staging/validation/publication path before any mutation.
    var relaxed = try @import("antfly_local_sources").sql_compiler.compile(alloc, "ALTER TABLE items ALTER COLUMN id DROP NOT NULL", .{});
    defer relaxed.deinit();
    const relaxed_outcome = try execute(&server, null, .{}, "default", "public", alloc, .{ .catalog_ddl = relaxed.statement.catalog_ddl });
    try std.testing.expectEqual(catalog.MutationOutcome.committed, relaxed_outcome.mutation_outcome);
    try std.testing.expectEqual(@as(usize, 8), source.updates);
    // A prior migration may finalize between capture and CAS preparation.
    // Refresh the same owner before issuing exactly one mutation, rather than
    // discovering an uncertain outcome and replaying an admitted ALTER.
    const finalization_reads = source.resolutions;
    const finalization_attempts = source.mutation_attempts;
    source.pending_read_schema = true;
    const ShiftedClock = struct {
        fn now(_: ?*anyopaque, _: std.Io.Clock) std.Io.Timestamp {
            return .{ .nanoseconds = 10 };
        }
    };
    var clock_vtable = std.testing.io.vtable.*;
    clock_vtable.now = ShiftedClock.now;
    const shifted_io: std.Io = .{ .userdata = std.testing.io.userdata, .vtable = &clock_vtable };
    const shifted_context: operation.RequestContext = .{
        .deadline_ns = 10 + std.time.ns_per_s,
        .deadline_io = @import("antfly_runtime_abi").io_abi.Borrow.init(&shifted_io),
    };
    // A deadline from another clock origin retains its remaining budget; it
    // must not look expired to the native-monotonic readiness loop.
    _ = try execute(&server, null, shifted_context, "default", "public", alloc, guarded_input);
    try std.testing.expectEqual(finalization_reads + 2, source.resolutions);
    try std.testing.expectEqual(finalization_attempts + 1, source.mutation_attempts);
    try std.testing.expectEqual(@as(usize, 9), source.updates);
    source.pending_read_schema = true;
    source.finalization_seen = false;
    source.foreign_finalized_owner = true;
    try std.testing.expectError(error.CatalogGenerationChanged, execute(&server, null, .{}, "default", "public", alloc, guarded_input));
    try std.testing.expectEqual(finalization_attempts + 1, source.mutation_attempts);
    try std.testing.expectEqual(@as(usize, 9), source.updates);
    // Hold worker admission independently of the periodic sweep. Successful
    // external SQL DDL must still install a keyed publication obligation.
    var backend = try @import("antfly_local_sources").storage_background_runtime.BackendRuntimeHandle.init(alloc, .{});
    defer backend.deinit();
    source.external_lake = true;
    source.path_lookup = false;
    source.index_exists = false;
    source.table_lookup = false;
    source.foreign_finalized_owner = false;
    source.finalization_seen = false;
    var lake_server = server_mod.ApiHttpServer.init(alloc, .{ .backend_runtime = backend.ptr() }, server.source, null, null);
    defer lake_server.deinit();
    source.scheduler_server = &lake_server;
    lake_server.lake_index_recovery_after_ns = std.math.maxInt(u64);
    try backend.ptr().durable_jobs.pauseOwner(lake_server.index_installation_owner_id);
    _ = try execute(&lake_server, null, .{}, "default", "public", alloc, scoped_input);
    try std.testing.expectEqual(@as(usize, 0), lake_server.index_installation_reserved_slots);
    try std.testing.expectEqual(@as(usize, 1), lake_server.index_installation_queue.items.len);
    try std.testing.expectEqualStrings("table:immutable-17", lake_server.index_installation_queue.items[0].table_name);
    try std.testing.expectEqualStrings("__lake_publication", lake_server.index_installation_queue.items[0].index_name);
    const admitted_generation = lake_server.index_installation_queue.items[0].generation;
    source.uncertain = true;
    try std.testing.expectError(error.SqlMutationOutcomeUnknown, execute(&lake_server, null, .{}, "default", "public", alloc, scoped_input));
    try std.testing.expectEqual(@as(usize, 0), lake_server.index_installation_reserved_slots);
    try std.testing.expectEqual(admitted_generation, lake_server.index_installation_queue.items[0].generation);
}

test "SQL catalog regular UNIQUE admits an owned private build without publishing through schema CAS" {
    const local = @import("antfly_local_sources");
    const publication = @import("../metadata/relational_index_publication.zig");
    const reads = local.api_table_read_source;
    const consistency = local.storage_read_consistency.ReadConsistency;
    const Source = struct {
        plan: publication.Plan,
        lost_reply: bool,
        resolutions: usize = 0,
        preparations: usize = 0,
        begins: usize = 0,
        observations: usize = 0,
        owner_reads: usize = 0,
        admitted: ?publication.AdmissionStatus = null,
        fn status(_: *anyopaque) !@import("../metadata/api.zig").MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }
        fn run(raw: *anyopaque, a: std.mem.Allocator, context: operation.RequestContext, call: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const plan = self.plan;
            switch (call) {
                .resolve_many => |request| {
                    self.resolutions += 1;
                    try std.testing.expectEqual(@as(usize, 2), request.relations.len);
                    try std.testing.expectEqualStrings("items", request.relations[0].name);
                    try std.testing.expectEqualStrings("email_idx", request.relations[1].name);
                    return std.json.Stringify.valueAlloc(a, domain.ResolvedMany{ .revision = 9, .tables = &.{}, .relation_epoch = .{ .revision = 9, .incarnation = plan.table_guard.incarnation }, .relations = &.{ .{ .owner = plan.table_guard.owner, .logical_table = "items", .table = .{ .name = plan.before.name, .table_id = plan.before.table_id, .query_definition = .{ .table_id = plan.before.table_id, .schema_json = plan.before.schema_json, .read_schema_json = "", .indexes_json = "{}" } } }, null } }, .{});
                },
                .relational_index_prepare => |request| {
                    try std.testing.expect(context.setting_admin and context.index_publication_authority);
                    try std.testing.expectEqualStrings("email_idx", request.index_name);
                    self.preparations += 1;
                    return (publication.PrepareCut{ .metadata_incarnation = "01010101010101010101010101010101".*, .namespace_id = plan.namespace_id, .table_guard = plan.table_guard, .before = plan.before, .ranges = &.{plan.owners[0].range}, .authority = .native }).encode(a);
                },
                .relational_index_begin => |request| {
                    try std.testing.expect(context.setting_admin and context.index_publication_authority);
                    self.begins += 1;
                    try std.testing.expectEqual(@as(usize, 1), self.begins);
                    var prepared = try request.begin.plan.prepare(a);
                    defer prepared.deinit();
                    var head = prepared.head;
                    head.phase = .published;
                    head.revision = 10;
                    head.completed_sources = 1;
                    head.completion_revision = 3;
                    head.counts = .{ 1, 1, 1, 1, 1, 0, 0 };
                    self.admitted = .{ .metadata_incarnation = request.metadata_incarnation, .head = head };
                    if (self.lost_reply) return error.MetadataMutationOutcomeUnknown;
                    return std.json.Stringify.valueAlloc(a, self.admitted.?, .{});
                },
                .relational_index_admission_status => |request| {
                    self.observations += 1;
                    try std.testing.expect(try self.admitted.?.isAdmitted(request));
                    return std.json.Stringify.valueAlloc(a, self.admitted.?, .{});
                },
                else => return error.UnexpectedIndexAdmissionOperation,
            }
        }
        fn lookupGroup(raw: *anyopaque, a: std.mem.Allocator, group: u64, table: []const u8, _: []const u8, opts: local.storage_db_types.LookupOptions, gate: consistency) !?reads.LookupResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(@as(u64, 301), group);
            try std.testing.expectEqualStrings("physical:101", table);
            try std.testing.expectEqual(consistency.read_index, gate);
            self.owner_reads += 1;
            const bytes = if (opts.relational_integrity_catalog)
                try std.json.Stringify.valueAlloc(a, .{ .catalog = self.plan.catalog_before_b64, .schema_version = @as(u32, 1), .table_id = "101" }, .{})
            else
                try std.json.Stringify.valueAlloc(a, .{ .namespace = self.plan.owners[0].namespace, .catalog_digest = self.plan.owners[0].catalog_digest, .next_epoch = @as(u64, 7), .catalog_checks_identity_supported = true }, .{});
            return .{ .json = bytes, .version = 0 };
        }
        fn lookup(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: local.storage_db_types.LookupOptions, _: consistency) !?reads.LookupResponse {
            return error.UnexpectedUnroutedIndexRead;
        }
        fn scan(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, _: local.storage_db_types.ScanOptions, _: consistency) !?reads.ScanResponse {
            return error.UnexpectedIndexScan;
        }
        fn query(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: local.storage_db_types.SearchRequest, _: consistency) !?local.api_query_response.QueryResponse {
            return error.UnexpectedIndexQuery;
        }
    };
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const plan = try publication.testing.plan(arena.allocator());
    var runtime = try local.storage_background_runtime.BackendRuntimeHandle.init(a, .{ .backend = .io_threaded });
    defer runtime.deinit();
    var compiled = try local.sql_compiler.compile(a, "CREATE UNIQUE INDEX email_idx ON items (email)", .{});
    defer compiled.deinit();
    for ([_]bool{ false, true }) |lost_reply| {
        var source: Source = .{ .plan = plan, .lost_reply = lost_reply };
        var server = server_mod.ApiHttpServer.init(a, .{ .backend_runtime = runtime.ptr() }, .{ .ptr = &source, .vtable = &.{ .status = Source.status, .system_catalog = Source.run, .supports_query_definitions = true } }, .{ .ptr = &source, .vtable = &.{ .lookup = Source.lookup, .lookup_group_local = Source.lookupGroup, .scan = Source.scan, .query = Source.query } }, null);
        defer server.deinit();
        var result_arena = std.heap.ArenaAllocator.init(a);
        defer result_arena.deinit();
        const result = try executeWithAllocators(&server, null, .{}, "default", "public", .{ .result = result_arena.allocator(), .scratch = a, .completion = .until_deadline }, .{ .catalog_ddl = compiled.statement.catalog_ddl }, null);
        try std.testing.expectEqual(catalog.MutationOutcome.committed, result.mutation_outcome);
        try std.testing.expectEqual(catalog.DdlReceipt.State.ready, result.receipt.?.state);
        try std.testing.expectEqual(@as(u32, 2), result.receipt.?.schema_version);
        const identity = try @import("index_publication_recovery.zig").Token.decode(result.receipt.?.index_publication_token.?);
        try std.testing.expectEqual(@as(u64, 101), identity.table_id);
        try std.testing.expectEqual(@as(usize, 1), source.resolutions);
        try std.testing.expectEqual(@as(usize, 1), source.preparations);
        try std.testing.expectEqual(@as(usize, 1), source.begins);
        try std.testing.expectEqual(@as(usize, 2), source.owner_reads);
        try std.testing.expectEqual(@as(usize, if (lost_reply) 1 else 0), source.observations);
    }
}

test "SQL catalog DDL authorizes before lookup and handles atomic conditional outcomes" {
    const Source = struct {
        calls: usize = 0,
        failure: ?anyerror = null,
        fn status(_: *anyopaque) !@import("../metadata/api.zig").MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }
        fn run(raw: *anyopaque, alloc: std.mem.Allocator, _: operation.RequestContext, call: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (call == .resolve_many) {
                if (call.resolve_many.relations.len != 0) {
                    try std.testing.expectEqual(@as(usize, 1), call.resolve_many.relations.len);
                    try std.testing.expectEqualStrings("events", call.resolve_many.relations[0].name);
                    try std.testing.expect(!call.resolve_many.include_query_definitions);
                    return std.json.Stringify.valueAlloc(alloc, domain.ResolvedMany{
                        .revision = 1,
                        .tables = &.{},
                        .relations = &.{.{ .logical_table = "events", .owner = .{ .table_id = 17, .schema_version = 0, .schema_digest = @splat(1), .kind = .table }, .table = .{ .table_id = 17, .name = "table:events" } }},
                        .relation_epoch = .{ .revision = 1, .incarnation = @splat(2) },
                    }, .{});
                }
                try std.testing.expectEqual(@as(usize, 1), call.resolve_many.targets.len);
                try std.testing.expectEqualStrings("events", call.resolve_many.targets[0].table);
                return std.json.Stringify.valueAlloc(alloc, domain.ResolvedMany{ .revision = 1, .tables = &.{.{ .table_id = 17, .name = "table:events" }} }, .{});
            }
            try std.testing.expect(call == .mutate);
            try std.testing.expectEqualStrings("analytics", call.mutate.mutation.database);
            try std.testing.expectEqualStrings("reporting", call.mutate.mutation.namespace);
            if (call.mutate.mutation.action == .create) {
                try std.testing.expect(call.mutate.create_table_json != null);
                try std.testing.expect(call.mutate.physical_name != null);
            }
            if (call.mutate.mutation.action == .drop) {
                const guard = call.mutate.binding_guard orelse return error.TestUnexpectedResult;
                try std.testing.expectEqual(@as(u64, 17), guard.owner.table_id);
                try std.testing.expectEqualDeep(@as([16]u8, @splat(2)), guard.incarnation);
            }
            if (self.failure) |err| return err;
            return alloc.dupe(u8, "{}");
        }
    };
    const alloc = std.testing.allocator;
    var source: Source = .{};
    var server = server_mod.ApiHttpServer.init(alloc, .{}, .{ .ptr = &source, .vtable = &.{ .status = Source.status, .system_catalog = Source.run } }, null, null);
    defer server.deinit();
    const input: catalog.Ddl = .{ .create_table = .{ .name = .{ .database = "analytics", .namespace = "reporting", .table = "events" }, .schema_json = "{\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"}},\"additionalProperties\":false}}}}", .if_not_exists = true } };
    const denied: server_mod.AuthenticatedIdentity = .{ .username = @constCast("reader") };
    try std.testing.expectError(error.Forbidden, execute(&server, denied, .{}, "default", "public", alloc, input));
    try std.testing.expectEqual(@as(usize, 0), source.calls);
    source.failure = error.CatalogAlreadyExists;
    try std.testing.expectEqual(catalog.MutationOutcome.committed, (try execute(&server, null, .{}, "default", "public", alloc, input)).mutation_outcome);
    try std.testing.expectEqual(@as(usize, 2), source.calls);
    source.failure = error.MetadataMutationOutcomeUnknown;
    try std.testing.expectError(error.SqlMutationOutcomeUnknown, execute(&server, null, .{}, "default", "public", alloc, input));
    try std.testing.expectEqual(@as(usize, 3), source.calls);
    source.failure = error.CatalogNotFound;
    try std.testing.expectEqual(catalog.MutationOutcome.committed, (try execute(&server, null, .{}, "default", "public", alloc, .{ .drop_table = .{ .table = input.create_table.name, .if_exists = true } })).mutation_outcome);
    try std.testing.expectEqual(@as(usize, 5), source.calls);
}

test "SQL UUID prepared CREATE TABLE commits and replays catalog topology only on execute" {
    // sql-0004: exact original PREPARE/EXECUTE, then metadata and owner recovery.
    const metadata_store = @import("../metadata/storage/raft_apply_store.zig");
    const table_manager = @import("../metadata/table_manager.zig");
    const Source = struct {
        store: *metadata_store.RaftApplyStore,
        metadata_group_id: u64 = 21,
        next_index: u64 = 1,
        mutations: usize = 0,
        output: ?*std.Io.Writer.Allocating = null,
        fn status(_: *anyopaque) !@import("../metadata/api.zig").MetadataStatus {
            return .{ .metadata_group_id = 21, .metrics = .{} };
        }
        pub fn ensureTableTopologyProtocolReadyWithContext(_: *@This(), _: operation.RequestContext, _: u16) !void {}
        pub fn validateTableTopologyProtocolReadinessWithContext(_: *@This(), _: operation.RequestContext, _: void) !void {}
        pub fn ensureLinearizableReadWithContext(_: *@This(), _: operation.RequestContext) !void {}
        pub fn lockCatalogMutation(_: *@This()) void {}
        pub fn unlockCatalogMutation(_: *@This()) void {}
        pub fn projectedStore(self: *@This()) ?*metadata_store.RaftApplyStore {
            return self.store;
        }
        pub fn captureTableCreateGeneration(self: *@This(), alloc: std.mem.Allocator, table_id: u64) !u64 {
            try self.store.ensureDerivedCatalogIndexes(self.metadata_group_id);
            return self.store.captureTableCreateGeneration(alloc, self.metadata_group_id, table_id);
        }
        pub fn captureTableDropAdmission(self: *@This(), allocator: std.mem.Allocator, name: []const u8) !@import("../metadata/service.zig").TableDropAdmission {
            try self.store.ensureDerivedCatalogIndexes(self.metadata_group_id);
            var projection = (try self.store.captureTableDropProjection(allocator, self.metadata_group_id, name)) orelse return error.TableNotFound;
            defer projection.deinit(allocator);
            if (projection.fence.active()) return error.TableTransitionActive;
            const expected_name = try allocator.dupe(u8, projection.table.name);
            const ids = projection.range_group_ids;
            projection.range_group_ids = &.{};
            return .{ .table_id = projection.table.table_id, .expected_name = expected_name, .expected_transition_generation = projection.fence.generation, .range_membership = projection.fence.membership(projection.table.table_id), .range_group_ids = ids };
        }
        pub fn proposeTransitionCommandWithReceipt(self: *@This(), command: metadata_store.TransitionCommand) !u64 {
            const alloc = std.testing.allocator;
            const encoded = try metadata_store.encodeTransitionCommand(alloc, command);
            defer alloc.free(encoded);
            const entries = try @import("../raft/state_machine/mod.zig").encodeCommittedEntries(alloc, &.{.{ .term = 1, .index = self.next_index, .entry_type = .normal, .data = encoded }});
            defer alloc.free(entries);
            try self.store.snapshotBuilder().applyBatch(.{ .group_id = self.metadata_group_id, .commit_index = self.next_index, .entries_bytes = entries });
            const receipt = self.next_index;
            self.next_index += 1;
            return receipt;
        }
        pub fn waitForTransitionAppliedWithContext(_: *@This(), _: u64, _: operation.RequestContext) !void {}
        pub fn verifyTableDropProjection(self: *@This(), alloc: std.mem.Allocator, table_id: u64) !void {
            if (try self.store.getTable(alloc, self.metadata_group_id, table_id)) |table| {
                table_manager.freeTable(alloc, table);
                return error.TableTransitionActive;
            }
        }
        pub fn verifyTableCreateProjection(self: *@This(), alloc: std.mem.Allocator, table: table_manager.TableRecord, ranges: []const table_manager.RangeRecord) !void {
            try self.store.verifyTableCreateProjectionExact(alloc, self.metadata_group_id, table, ranges);
        }
        fn run(raw: *anyopaque, alloc: std.mem.Allocator, context: operation.RequestContext, call: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (call != .mutate) return error.UnexpectedCall;
            try std.testing.expect(std.mem.indexOf(u8, self.output.?.written(), "PREPARE\x00") != null);
            const request = call.mutate;
            try std.testing.expectEqual(domain.Kind.table, request.mutation.kind);
            try std.testing.expectEqual(domain.Action.create, request.mutation.action);
            try std.testing.expectEqualStrings("prepared_usage_records", request.mutation.name);
            try std.testing.expectEqualStrings("default", request.mutation.database);
            try std.testing.expectEqualStrings("public", request.mutation.namespace);
            try std.testing.expect(std.mem.startsWith(u8, request.physical_name.?, "table:"));
            var stored = try tables.parseStoredCreateTableRequest(alloc, request.create_table_json.?);
            defer stored.deinit(alloc);
            var validated = try tables.parseValidatedTableSchema(alloc, stored.schema_json.?);
            defer validated.deinit(alloc);
            const property = validated.document_schemas[0].properties[0];
            try std.testing.expectEqualStrings("id", property.name);
            try std.testing.expectEqualStrings("uuid", property.format.?);
            self.mutations += 1;
            return @import("../system_catalog/operations.zig").mutate(self, alloc, context, request);
        }
    };
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/sql-uuid-catalog", .{tmp.sub_path});
    defer alloc.free(root);
    var store = try metadata_store.RaftApplyStore.init(alloc, .{ .root_dir = root });
    var store_open = true;
    defer if (store_open) store.deinit();
    const usermgr = @import("../usermgr/mod.zig");
    const casbin = @import("antfly_casbin");
    var user_store = usermgr.MemoryStore.init(alloc);
    defer user_store.deinit();
    var policies = casbin.MemoryAdapter.init(alloc);
    defer policies.deinit();
    var manager = try usermgr.UserManager.init(alloc, user_store.iface(), try usermgr.initDefaultEnforcer(alloc, policies.iface()));
    defer manager.deinit();
    var permission = try usermgr.Permission.initOwned(alloc, .table, "prepared_usage_records", .admin);
    defer permission.deinit(alloc);
    var user = try manager.createUser("ddl_admin", "secret", &.{permission});
    defer user.deinit(alloc);
    var runtime = try @import("antfly_local_sources").storage_background_runtime.BackendRuntimeHandle.init(alloc, .{ .backend = .io_threaded });
    defer runtime.deinit();
    var source: Source = .{ .store = &store };
    var server = server_mod.ApiHttpServer.init(alloc, .{ .user_manager = &manager, .backend_runtime = runtime.ptr() }, .{ .ptr = &source, .vtable = &.{ .status = Source.status, .system_catalog = Source.run } }, null, null);
    defer server.deinit();
    var adapter: @import("sql_pgwire.zig").Adapter = .{ .server = &server };
    const Wire = struct {
        fn frame(out: *std.Io.Writer, tag: u8, payload: []const u8) !void {
            try out.writeByte(tag);
            try out.writeInt(u32, @intCast(payload.len + 4), .big);
            try out.writeAll(payload);
        }
    };
    var input = std.Io.Writer.Allocating.init(alloc);
    defer input.deinit();
    const startup = "user\x00ddl_admin\x00database\x00default\x00\x00";
    try input.writer.writeInt(u32, @intCast(startup.len + 8), .big);
    try input.writer.writeInt(u32, 196608, .big);
    try input.writer.writeAll(startup);
    try Wire.frame(&input.writer, 'p', "secret\x00");
    try Wire.frame(&input.writer, 'Q', "PREPARE create_usage_plan AS CREATE TABLE prepared_usage_records (id uuid)\x00");
    try Wire.frame(&input.writer, 'Q', "EXECUTE create_usage_plan\x00");
    try Wire.frame(&input.writer, 'X', "");
    var reader = std.Io.Reader.fixed(input.written());
    var output = std.Io.Writer.Allocating.init(alloc);
    defer output.deinit();
    source.output = &output;
    var session: @import("../pgwire/protocol.zig").Session = .{ .alloc = alloc, .io = std.testing.io, .source = adapter.backend(), .reader = &reader, .writer = &output.writer };
    defer session.deinit();
    try session.run();
    var frames: @import("../pgwire/protocol.zig").Cursor = .{ .bytes = output.written() };
    var completions: usize = 0;
    while (frames.offset < frames.bytes.len) {
        const tag = try frames.int(u8);
        const size = try frames.int(u32);
        const payload = try frames.take(size - 4);
        if (tag == 'E') return error.UnexpectedPgwireError;
        if (tag == 'C') {
            try std.testing.expectEqualStrings(if (completions == 0) "PREPARE\x00" else "CREATE TABLE\x00", payload);
            completions += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 2), completions);
    try std.testing.expectEqual(@as(usize, 1), source.mutations);
    const Verify = struct {
        fn run(owner: *metadata_store.RaftApplyStore, allocator: std.mem.Allocator) !void {
            const physical = (try owner.resolveSystemCatalogTable(allocator, 21, .{ .table = "prepared_usage_records" })) orelse return error.MissingSqlTable;
            defer table_manager.freeTable(allocator, physical);
            var snapshot = try owner.systemCatalogSnapshot(allocator, 21);
            defer snapshot.deinit();
            try std.testing.expectEqualStrings("prepared_usage_records", snapshot.value.find(.table, domain.default_namespace_id, "prepared_usage_records").?.name);
            var schema = try tables.parseValidatedTableSchema(allocator, physical.schema_json);
            defer schema.deinit(allocator);
            try std.testing.expectEqualStrings("uuid", schema.document_schemas[0].properties[0].format.?);
            const ranges = try owner.listRanges(allocator, 21);
            defer owner.freeRanges(allocator, ranges);
            try std.testing.expectEqual(@as(usize, 1), ranges.len);
            try std.testing.expectEqual(physical.table_id, ranges[0].table_id);
        }
    };
    try Verify.run(&store, alloc);
    const snapshot_bytes = try store.snapshotBuilder().buildSnapshot(alloc, 21);
    defer alloc.free(snapshot_bytes);
    store.deinit();
    store_open = false;
    store = try metadata_store.RaftApplyStore.init(alloc, .{ .root_dir = root });
    store_open = true;
    try Verify.run(&store, alloc);
    const restored_root = try std.fmt.allocPrint(alloc, "{s}-snapshot", .{root});
    defer alloc.free(restored_root);
    var restored = try metadata_store.RaftApplyStore.init(alloc, .{ .root_dir = restored_root });
    defer restored.deinit();
    try std.testing.expect(try restored.snapshotBuilder().installSnapshot(alloc, 21, 1, snapshot_bytes));
    try Verify.run(&restored, alloc);
    const published_tables = try restored.listTables(alloc, 21);
    defer restored.freeTables(alloc, published_tables);
    const published_ranges = try restored.listRanges(alloc, 21);
    defer restored.freeRanges(alloc, published_ranges);
    const replica_root = try std.fmt.allocPrint(alloc, "{s}-owner", .{root});
    defer alloc.free(replica_root);
    const provisioner = @import("../metadata/table_provisioner.zig");
    const summary = try provisioner.reconcileReplicaRoot(
        alloc,
        replica_root,
        21,
        &.{ 21, published_ranges[0].group_id },
        published_tables,
        published_ranges,
    );
    try std.testing.expectEqual(@as(usize, 1), summary.groups_considered);
    try std.testing.expectEqual(@as(usize, 1), summary.dbs_opened);
    const owner_path = try provisioner.groupDbPathFromReplicaRoot(alloc, replica_root, published_ranges[0].group_id);
    defer alloc.free(owner_path);
    {
        var owner = try @import("antfly_local_sources").storage_db_selected_root.db.DB.open(alloc, owner_path, .{});
        defer owner.close();
        try owner.batch(.{ .writes = &.{.{ .key = "doc:1", .value = "{\"id\":\"550E8400E29B41D4A716446655440000\"}" }}, .sync_level = .write });
    }
    {
        var owner = try @import("antfly_local_sources").storage_db_selected_root.db.DB.open(alloc, owner_path, .{});
        defer owner.close();
        const row = (try owner.get(alloc, "doc:1")) orelse return error.MissingSqlRow;
        defer alloc.free(row);
        try std.testing.expect(std.mem.indexOf(u8, row, "550e8400-e29b-41d4-a716-446655440000") != null);
    }
    // Exercise the generic proposal/receipt/post-commit verification path,
    // including the DROP arm after CREATE has survived restart.
    const dropped = try @import("../system_catalog/operations.zig").mutate(&source, alloc, .{}, .{
        .mutation = .{ .action = .drop, .kind = .table, .name = "prepared_usage_records" },
    });
    defer alloc.free(dropped);
    try std.testing.expect(try store.resolveSystemCatalogTable(alloc, 21, .{ .table = "prepared_usage_records" }) == null);
    const remaining = try store.listRanges(alloc, 21);
    defer store.freeRanges(alloc, remaining);
    try std.testing.expectEqual(@as(usize, 0), remaining.len);
}

test "SQL catalog conditional CREATE does not hide an absent-table name conflict" {
    const Source = struct {
        reads: usize = 0,
        churn: bool = false,
        unattested: bool = false,
        target_exists: bool = false,
        claimed: bool = false,
        fn status(_: *anyopaque) !@import("../metadata/api.zig").MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }
        fn run(raw: *anyopaque, alloc: std.mem.Allocator, _: operation.RequestContext, call: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(call == .resolve_many);
            const lookup = call.resolve_many;
            self.reads += 1;
            const relations = try alloc.alloc(?domain.ResolvedRelation, lookup.relations.len);
            defer alloc.free(relations);
            @memset(relations, null);
            const table: domain.ResolvedTable = .{ .table_id = 17, .name = "table:17" };
            if (lookup.relations.len != 0) {
                try std.testing.expect(lookup.relations.len <= 256);
                try std.testing.expect(lookup.name_allocation);
                try std.testing.expectEqualStrings("tenant", lookup.relations[0].namespace);
                for (lookup.relations, relations) |relation, *slot| {
                    const name = relation.name;
                    if (self.claimed or std.mem.eql(u8, name, "foo_pkey") or std.mem.eql(u8, name, "foo_pkey1"))
                        slot.* = .{ .table = table, .logical_table = "holder", .owner = .{ .table_id = 17, .schema_version = 1, .schema_digest = @splat(1), .kind = .index } };
                }
            }
            const targets = [_]?domain.ResolvedTable{if (self.target_exists) table else null};
            return std.json.Stringify.valueAlloc(alloc, domain.ResolvedMany{
                .revision = 9,
                .tables = targets[0..lookup.targets.len],
                .relations = relations[0..lookup.relations.len],
                .relation_epoch = if (self.unattested) null else .{ .revision = if (self.churn and ((lookup.targets.len == 0 and lookup.expected_relation_epoch != null))) 10 else 9, .incarnation = @splat(1) },
            }, .{});
        }
    };
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var source: Source = .{};
    var server = server_mod.ApiHttpServer.init(alloc, .{}, .{ .ptr = &source, .vtable = &.{ .status = Source.status, .system_catalog = Source.run, .supports_query_definitions = true } }, null, null);
    defer server.deinit();
    const target: domain.Target = .{ .database = "default", .namespace = "tenant", .table = "foo" };
    const selected = .{ .names = &[_][]const u8{"foo_pkey3"} };
    source.claimed = true;
    try std.testing.expectError(error.CatalogGenerationChanged, createConflict(&server, .{}, a, target, selected.names, true, error.CatalogAlreadyExists, alloc));
    source.target_exists = true;
    _ = try createConflict(&server, .{}, a, target, selected.names, true, error.CatalogAlreadyExists, alloc);
    try std.testing.expectError(error.CatalogAlreadyExists, createConflict(&server, .{}, a, target, selected.names, false, error.CatalogAlreadyExists, alloc));
    source.target_exists = false;
    source.claimed = false;
    try std.testing.expectError(error.CatalogAlreadyExists, createConflict(&server, .{}, a, target, &.{}, true, error.CatalogAlreadyExists, alloc));
    try std.testing.expectError(error.MetadataMutationOutcomeUnknown, createConflict(&server, .{}, a, target, selected.names, true, error.MetadataMutationOutcomeUnknown, alloc));
    // An existing table short-circuits before any relation batch, even at
    // the largest supported schema width. An absent table uses bounded batches
    // whose answers must all belong to the same epoch.
    const many_names = try a.alloc([]const u8, 513);
    @memset(many_names, "available_key");
    source.target_exists = true;
    source.reads = 0;
    _ = try createConflict(&server, .{}, a, target, many_names, true, error.CatalogAlreadyExists, alloc);
    try std.testing.expectEqual(@as(usize, 1), source.reads);
    source.target_exists = false;
    source.reads = 0;
    try std.testing.expectError(error.CatalogAlreadyExists, createConflict(&server, .{}, a, target, many_names, true, error.CatalogAlreadyExists, alloc));
    try std.testing.expectEqual(@as(usize, 4), source.reads);
    source.claimed = true;
    source.reads = 0;
    try std.testing.expectError(error.CatalogGenerationChanged, createConflict(&server, .{}, a, target, many_names, true, error.CatalogAlreadyExists, alloc));
    source.churn = true;
    source.reads = 0;
    try std.testing.expectError(error.CatalogGenerationChanged, createConflict(&server, .{}, a, target, many_names, true, error.CatalogAlreadyExists, alloc));
}

test "SQL concurrent index admission owns uncertain receipts and guards fenced UNIQUE before mutation" {
    // sql-0707, sql-0725: uncertain admission owns a handle before any proposal.
    const Source = struct {
        proposals: usize = 0,
        resolutions: usize = 0,
        index_lookup: bool = false,
        failure: anyerror = error.MetadataMutationOutcomeUnknown,
        const schema_json =
            \\{"version":7,"storage_mode":"relational","default_type":"row","relational_indexes":[{"name":"old_status","keys":[{"column":"status"}]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"status":{"type":"string"}},"additionalProperties":false}}}}
        ;
        fn status(_: *anyopaque) !@import("../metadata/api.zig").MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }
        fn run(raw: *anyopaque, a: std.mem.Allocator, context: operation.RequestContext, call: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            switch (call) {
                .resolve_many => |request| {
                    self.resolutions += 1;
                    try std.testing.expectEqual(@as(usize, if (self.index_lookup) 1 else 2), request.relations.len);
                    const owner: domain.ResolvedRelation = .{
                        .owner = .{ .kind = if (self.index_lookup) .index else .table, .table_id = 17, .schema_version = 7, .schema_digest = @splat(1) },
                        .logical_table = "items",
                        .table = .{ .table_id = 17, .name = "immutable-17", .query_definition = .{ .table_id = 17, .schema_json = schema_json, .read_schema_json = "", .indexes_json = "{}" } },
                    };
                    return std.json.Stringify.valueAlloc(a, domain.ResolvedMany{
                        .revision = 9,
                        .tables = &.{},
                        .relation_epoch = .{ .revision = 9, .incarnation = @splat(1) },
                        .relations = if (self.index_lookup) &.{owner} else &.{ owner, null },
                    }, .{});
                },
                .relation_schema_mutate => |request| {
                    self.proposals += 1;
                    try std.testing.expect(context.setting_admin);
                    try std.testing.expectEqual(@as(u64, 17), request.guard.owner.table_id);
                    return self.failure;
                },
                else => return error.UnexpectedIndexAdmissionCall,
            }
        }
    };
    const a = std.testing.allocator;
    var source: Source = .{};
    var server = server_mod.ApiHttpServer.init(a, .{}, .{ .ptr = &source, .vtable = &.{ .status = Source.status, .system_catalog = Source.run, .supports_query_definitions = true } }, null, null);
    defer server.deinit();
    var compiled = try @import("antfly_local_sources").sql_compiler.compile(a, "CREATE INDEX CONCURRENTLY items_status ON items(status) WHERE status='pending'", .{});
    defer compiled.deinit();
    const input: catalog.Ddl = .{ .catalog_ddl = compiled.statement.catalog_ddl };
    for ([_]anyerror{ error.MetadataMutationOutcomeUnknown, error.OutOfMemory }) |failure| {
        source.failure = failure;
        var result = std.heap.ArenaAllocator.init(a);
        defer result.deinit();
        const before = source.proposals;
        const outcome = try executeWithAllocators(&server, null, .{}, "default", "public", .{ .result = result.allocator(), .scratch = a }, input, null);
        try std.testing.expectEqual(before + 1, source.proposals);
        try std.testing.expectEqual(null, outcome.mutation_outcome);
        const receipt = outcome.receipt.?;
        try std.testing.expectEqual(catalog.DdlReceipt.State.admission_unknown, receipt.state);
        try std.testing.expectEqualStrings("17", receipt.table_id);
        try std.testing.expectEqualStrings("items", receipt.table);
        try std.testing.expectEqual(@as(u32, 8), receipt.schema_version);
        try std.testing.expect(std.mem.indexOf(u8, receipt.diagnostic.?, "do not replay") != null);
    }
    source.failure = error.SchemaVersionChanged;
    try std.testing.expectError(error.SchemaVersionChanged, execute(&server, null, .{}, "default", "public", a, input));
    source.index_lookup = true;
    source.failure = error.MetadataMutationOutcomeUnknown;
    var dropping = try @import("antfly_local_sources").sql_compiler.compile(a, "DROP INDEX CONCURRENTLY old_status", .{});
    defer dropping.deinit();
    {
        var result = std.heap.ArenaAllocator.init(a);
        defer result.deinit();
        const before = source.proposals;
        const outcome = try executeWithAllocators(&server, null, .{}, "default", "public", .{ .result = result.allocator(), .scratch = a }, .{ .catalog_ddl = dropping.statement.catalog_ddl }, null);
        try std.testing.expectEqual(before + 1, source.proposals);
        try std.testing.expectEqual(null, outcome.mutation_outcome);
        try std.testing.expectEqual(catalog.DdlReceipt.State.admission_unknown, outcome.receipt.?.state);
        try std.testing.expectEqualStrings("17", outcome.receipt.?.table_id);
        try std.testing.expectEqualStrings("items", outcome.receipt.?.table);
        try std.testing.expectEqual(@as(u32, 8), outcome.receipt.?.schema_version);
    }
    const proposals = source.proposals;
    const resolutions = source.resolutions;
    var unique = try @import("antfly_local_sources").sql_compiler.compile(a, "CREATE UNIQUE INDEX CONCURRENTLY items_status ON items(status)", .{});
    defer unique.deinit();
    try std.testing.expectError(error.UnsupportedSqlShape, execute(&server, null, .{}, "default", "public", a, .{ .catalog_ddl = unique.statement.catalog_ddl }));
    try std.testing.expectEqual(proposals, source.proposals);
    try std.testing.expectEqual(resolutions, source.resolutions);
}

test "SQL concurrent index retirement capture waits for finalized catalog cuts without changing identity" {
    // sql-0725: cached predecessor cuts cannot cause a false busy rejection.
    const Fixture = struct {
        snapshots: usize = 0,
        replacement: bool = false,
        table: [1]@import("../metadata/table_manager.zig").TableRecord = undefined,
        fn status(_: *anyopaque) !@import("../metadata/api.zig").MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }
        fn snapshot(raw: *anyopaque) !@import("../metadata/api.zig").AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.snapshots += 1;
            const older = self.snapshots == 1;
            self.table[0] = .{ .table_id = if (self.replacement) 18 else 17, .name = "immutable-17", .schema_json = "{\"version\":7}", .read_schema_json = if (older) "{\"version\":6}" else "" };
            return .{ .status = .{ .metadata_group_id = 1, .metrics = .{} }, .tables = &self.table, .ranges = &.{}, .stores = &.{}, .placement_intents = &.{}, .split_transitions = &.{}, .merge_transitions = &.{} };
        }
        fn free(_: *anyopaque, _: *@import("../metadata/api.zig").AdminSnapshot) void {}
    };
    var fixture: Fixture = .{};
    var server = server_mod.ApiHttpServer.init(std.testing.allocator, .{ .imported_runtime_io = .{ .api = std.testing.io } }, .{ .ptr = &fixture, .vtable = &.{ .status = Fixture.status, .admin_snapshot = Fixture.snapshot, .free_admin_snapshot = Fixture.free } }, null, null);
    defer server.deinit();
    var snapshot = try retirementSnapshot(&server, .{}, "immutable-17", 17, 7);
    defer server.source.freeAdminSnapshot(&snapshot);
    try std.testing.expectEqual(@as(usize, 2), fixture.snapshots);
    try std.testing.expectEqualStrings("", snapshot.tables[0].read_schema_json);
    fixture.replacement = true;
    try std.testing.expectError(error.SchemaVersionChanged, retirementSnapshot(&server, .{}, "immutable-17", 17, 7));
    const before = fixture.snapshots;
    try std.testing.expectError(error.DeadlineExceeded, retirementSnapshot(&server, .{ .deadline_ns = 0 }, "immutable-17", 17, 7));
    try std.testing.expectEqual(before, fixture.snapshots);
}

test "SQL catalog name DDL pins first search path owner without fetching schema or replaying admission" {
    const Source = struct {
        const Mode = enum { table, index, missing, unattested, stale, unknown };
        mode: Mode = .table,
        qualified: bool = false,
        reads: usize = 0,
        writes: usize = 0,
        fn status(_: *anyopaque) !@import("../metadata/api.zig").MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }
        fn run(raw: *anyopaque, a: std.mem.Allocator, _: operation.RequestContext, call: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (call == .resolve_many) {
                self.reads += 1;
                const request = call.resolve_many;
                try std.testing.expect(!request.include_query_definitions);
                if (self.qualified) {
                    try std.testing.expectEqual(@as(usize, 1), request.relations.len);
                    try std.testing.expectEqualStrings("tenant", request.relations[0].namespace);
                } else {
                    try std.testing.expectEqual(@as(usize, 3), request.relations.len);
                    try std.testing.expectEqualStrings("absent", request.relations[0].namespace);
                    try std.testing.expectEqualStrings("tenant", request.relations[1].namespace);
                    try std.testing.expectEqualStrings("public", request.relations[2].namespace);
                }
                const owner: domain.ResolvedRelation = .{ .logical_table = "items", .table = .{ .table_id = 17, .name = "table:17" }, .owner = .{ .table_id = 17, .schema_version = 3, .schema_digest = @splat(1), .kind = if (self.mode == .index) .index else .table } };
                return std.json.Stringify.valueAlloc(a, domain.ResolvedMany{
                    .revision = 19,
                    .tables = &.{},
                    .relations = if (self.qualified) (if (self.mode == .missing) &.{null} else &.{owner}) else (if (self.mode == .missing) &.{ null, null, null } else &.{ null, owner, owner }),
                    .relation_epoch = if (self.mode == .unattested) null else .{ .incarnation = @splat(1), .revision = 7 },
                }, .{});
            }
            if (call != .mutate) return error.UnexpectedCatalogOperation;
            self.writes += 1;
            const request = call.mutate;
            const guard = request.binding_guard orelse return error.MissingNameBinding;
            try request.validateBinding();
            try std.testing.expectEqual(@as(u64, 17), guard.owner.table_id);
            try std.testing.expectEqual(@as(u32, 3), guard.owner.schema_version);
            try std.testing.expectEqualSlices(u8, &@as([16]u8, @splat(1)), &guard.incarnation);
            try std.testing.expectEqualStrings("tenant", request.mutation.namespace);
            try std.testing.expectEqualStrings("items", request.mutation.name);
            if (self.mode == .stale) return error.CatalogGenerationChanged;
            if (self.mode == .unknown) return error.MetadataMutationOutcomeUnknown;
            return a.dupe(u8, "{}");
        }
    };
    const a = std.testing.allocator;
    var arena: std.heap.ArenaAllocator = .init(a);
    defer arena.deinit();
    var source: Source = .{};
    var server = server_mod.ApiHttpServer.init(a, .{}, .{ .ptr = &source, .vtable = &.{ .status = Source.status, .system_catalog = Source.run } }, null, null);
    defer server.deinit();
    var path: SearchPath = .{};
    for ([_][]const u8{ "absent", "tenant", "public" }) |name| try path.append(try @import("../pgwire/search_path.zig").Namespace.init(name));
    for ([_][]const u8{ "DROP TABLE items", "ALTER TABLE items RENAME TO renamed", "ALTER TABLE items SET TABLESPACE cold" }) |sql| {
        var compiled = try @import("antfly_local_sources").sql_compiler.compile(a, sql, .{});
        defer compiled.deinit();
        const input: catalog.Ddl = switch (compiled.statement) {
            .drop_table => |drop| .{ .drop_table = drop },
            .catalog_ddl => |ddl| .{ .catalog_ddl = ddl },
            else => unreachable,
        };
        source.mode = .table;
        const reads = source.reads;
        const writes = source.writes;
        _ = try executeWithAllocators(&server, null, .{}, "default", "absent", .{ .result = arena.allocator(), .scratch = a }, input, &path);
        try std.testing.expectEqual(reads + 1, source.reads);
        try std.testing.expectEqual(writes + 1, source.writes);
        source.mode = .stale;
        try std.testing.expectError(error.CatalogGenerationChanged, executeWithAllocators(&server, null, .{}, "default", "absent", .{ .result = arena.allocator(), .scratch = a }, input, &path));
        try std.testing.expectEqual(writes + 2, source.writes);
        source.mode = .unknown;
        try std.testing.expectError(error.SqlMutationOutcomeUnknown, executeWithAllocators(&server, null, .{}, "default", "absent", .{ .result = arena.allocator(), .scratch = a }, input, &path));
        try std.testing.expectEqual(writes + 3, source.writes);
        for ([_]Source.Mode{ .index, .missing, .unattested }) |mode| {
            source.mode = mode;
            const expected = switch (mode) {
                .index => error.SqlWrongObjectType,
                .missing => error.TableNotFound,
                .unattested => error.TableTopologyUpgradeRequired,
                else => unreachable,
            };
            try std.testing.expectError(expected, executeWithAllocators(&server, null, .{}, "default", "absent", .{ .result = arena.allocator(), .scratch = a }, input, &path));
            try std.testing.expectEqual(writes + 3, source.writes);
        }
    }
    source.qualified = true;
    var qualified = try @import("antfly_local_sources").sql_compiler.compile(a, "DROP TABLE tenant.items", .{});
    defer qualified.deinit();
    for ([_]Source.Mode{ .table, .stale, .unknown }) |mode| {
        source.mode = mode;
        const writes = source.writes;
        const input: catalog.Ddl = .{ .drop_table = qualified.statement.drop_table };
        if (mode == .table) {
            _ = try executeWithAllocators(&server, null, .{}, "default", "public", .{ .result = arena.allocator(), .scratch = a }, input, null);
        } else try std.testing.expectError(if (mode == .stale) error.CatalogGenerationChanged else error.SqlMutationOutcomeUnknown, executeWithAllocators(&server, null, .{}, "default", "public", .{ .result = arena.allocator(), .scratch = a }, input, null));
        try std.testing.expectEqual(writes + 1, source.writes);
    }
    source.qualified = false;
    var missing = try @import("antfly_local_sources").sql_compiler.compile(a, "DROP TABLE IF EXISTS items", .{});
    defer missing.deinit();
    source.mode = .missing;
    const before = source.writes;
    _ = try executeWithAllocators(&server, null, .{}, "default", "absent", .{ .result = arena.allocator(), .scratch = a }, .{ .drop_table = missing.statement.drop_table }, &path);
    const reads = source.reads;
    path.len = 0;
    _ = try executeWithAllocators(&server, null, .{}, "default", "absent", .{ .result = arena.allocator(), .scratch = a }, .{ .drop_table = missing.statement.drop_table }, &path);
    try std.testing.expectEqual(before, source.writes);
    try std.testing.expectEqual(reads, source.reads);
}

test "SQL catalog captured name mutations retain exact owners across forwarding" {
    const a = std.testing.allocator;
    const request: domain.Request = .{ .mutation = .{ .kind = .table, .action = .set_tablespace, .name = "items", .namespace = "tenant", .tablespace = "cold" }, .binding_guard = .{
        .target = .{ .namespace = "tenant", .name = "items" },
        .logical_table = "items",
        .owner = .{ .table_id = 17, .schema_version = 3, .schema_digest = @splat(1), .kind = .table },
        .incarnation = @splat(2),
    } };
    const bytes = try std.json.Stringify.valueAlloc(a, request, .{});
    defer a.free(bytes);
    var decoded = try std.json.parseFromSlice(domain.Request, a, bytes, .{});
    defer decoded.deinit();
    try decoded.value.validateBinding();
    try std.testing.expectEqualDeep(request.binding_guard.?.owner, decoded.value.binding_guard.?.owner);
    try std.testing.expectEqualDeep(request.binding_guard.?.incarnation, decoded.value.binding_guard.?.incarnation);
    var wrong = request;
    wrong.mutation.namespace = "public";
    try std.testing.expectError(error.InvalidCatalogMutation, wrong.validateBinding());
    wrong = request;
    wrong.mutation.name = "another";
    try std.testing.expectError(error.InvalidCatalogMutation, wrong.validateBinding());
    wrong = request;
    wrong.binding_guard.?.owner.kind = .index;
    try std.testing.expectError(error.InvalidCatalogMutation, wrong.validateBinding());
    wrong = request;
    wrong.mutation.action = .create;
    try std.testing.expectError(error.InvalidCatalogMutation, wrong.validateBinding());
    const direct: domain.Request = .{ .mutation = request.mutation };
    const direct_bytes = try std.json.Stringify.valueAlloc(a, direct, .{});
    defer a.free(direct_bytes);
    try std.testing.expect(std.mem.indexOf(u8, direct_bytes, "binding_guard") == null);
}

/// The failed ordinary DROP was definitely not proposed. Fencing and parent
/// tombstones retain one immutable deletion intent; no weaker schema is visible.
fn beginTableDrop(server: *server_mod.ApiHttpServer, identity: ?server_mod.AuthenticatedIdentity, context: operation.RequestContext, allocators: catalog.DdlAllocators, target: domain.Target, captured: ?domain.RelationMutationGuard) !catalog.DdlOutcome {
    var scratch: std.heap.ArenaAllocator = .init(allocators.scratch);
    defer scratch.deinit();
    const a = scratch.allocator();
    const admission = (try captureTableAdmission(server, identity, context, a, target, true, null)) orelse return error.TableNotFound;
    if (captured) |guard| if (!admission.guard.owner.eql(guard.owner) or !std.mem.eql(u8, &admission.guard.incarnation, &guard.incarnation)) return error.CatalogGenerationChanged;
    const settled = try waitPriorSchemaFinalization(server, context, a, allocators.scratch, admission);
    var snapshot = try retirementSnapshot(server, context, settled.table.name, settled.table.table_id, settled.guard.owner.schema_version);
    defer server.source.freeAdminSnapshot(&snapshot);
    const before = tables.findTableByName(&snapshot, settled.table.name) orelse return error.TableNotFound;
    const plan = try @import("fk_generation_plan_builder.zig").buildSqlDrop(server, a, context, identity, before.*, settled.guard);
    var receipt = try newReceipt(allocators.result, target, before.table_id, settled.guard.owner.schema_version);
    errdefer {
        allocators.result.free(receipt.database);
        allocators.result.free(receipt.namespace);
        allocators.result.free(receipt.table);
        allocators.result.free(receipt.table_id);
        if (receipt.fk_generation_publication_id) |token| allocators.result.free(token);
    }
    receipt.fk_generation_publication_id = try allocators.result.dupe(u8, &std.fmt.bytesToHex(plan.id, .lower));
    try @import("sql_catalog_auth.zig").authorizeTarget(server, identity, a, target);
    try context.ensureActive();
    const admitted = try server.submitFkGenerationPlan(a, context, plan);
    if (admitted.state == .admission_unknown) {
        receipt.state = .admission_unknown;
        receipt.diagnostic = "DROP admission is unresolved. Inspect this table ID and FK publication token; do not replay the DDL.";
        return .{ .mutation_outcome = null, .receipt = receipt };
    }
    awaitTableDrop(server, context, allocators.scratch, before.table_id, plan.id, &receipt, allocators.completion) catch {
        receipt.state = .pending;
        receipt.diagnostic = "DROP intent committed; source fencing, reference retirement and catalog deletion continue after cancellation or restart. Inspect its table ID and publication token; do not replay the DDL.";
    };
    return .{ .mutation_outcome = if (receipt.state == .ready) .committed else if (receipt.state == .invalid) .committed_repair_required else .committed_pending, .receipt = receipt };
}

fn awaitTableDrop(server: *server_mod.ApiHttpServer, context: operation.RequestContext, scratch_backing: std.mem.Allocator, table_id: u64, id: [16]u8, receipt: *catalog.DdlReceipt, completion: catalog.DdlAllocators.Completion) !void {
    const deadline = if (completion == .bounded) (try activationNow(context)) +| 2 * std.time.ns_per_s else std.math.maxInt(u64);
    while (true) {
        try context.ensureActive();
        if (try activationNow(context) >= deadline) return error.ConstraintActivationPending;
        var scratch: std.heap.ArenaAllocator = .init(scratch_backing);
        defer scratch.deinit();
        const a = scratch.allocator();
        var private = context;
        private.setting_admin = true;
        private.fk_generation_publication_authority = true;
        const bytes = try server.source.systemCatalog(a, private, .{ .fk_generation_publication_status = table_id });
        const observed = try std.json.parseFromSliceLeaky(@import("../metadata/fk_generation_publication.zig").Publication, a, bytes, .{});
        if (!std.mem.eql(u8, &observed.plan.id, &id) or observed.plan.sql_drop == null or observed.plan.child_before.table_id != table_id) return error.GenerationPublicationChanged;
        if (observed.phase == .published) {
            receipt.state = .ready;
            return;
        }
        if (observed.phase == .canceled) {
            receipt.state = .invalid;
            receipt.diagnostic = "The admitted DROP was canceled before catalog deletion. Inspect its publication status; the table remains present.";
            return;
        }
        try activationPause(server, context);
    }
}
