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

const RelationAdmission = struct { table: domain.ResolvedTable, guard: domain.RelationMutationGuard };
const SearchPath = @import("../pgwire/search_path.zig").Path;

fn alterSchema(server: *server_mod.ApiHttpServer, identity: ?server_mod.AuthenticatedIdentity, context: operation.RequestContext, alloc: std.mem.Allocator, target: domain.Target, ddl: @import("antfly_local_sources").sql_ast.CatalogDdl, admission: ?RelationAdmission) !catalog.DdlOutcome {
    if (!server.source.vtable.supports_query_definitions) return error.UnsupportedSqlExecution;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const table = if (admission) |captured| captured.table else blk: {
        const bytes = try server.source.systemCatalog(a, context, .{ .resolve_many = .{ .targets = &.{target}, .include_query_definitions = true } });
        const snapshot = try std.json.parseFromSliceLeaky(domain.ResolvedMany, a, bytes, .{ .allocate = .alloc_always });
        if (snapshot.tables.len != 1) return error.InvalidSqlBackendResponse;
        break :blk snapshot.tables[0] orelse return error.TableNotFound;
    };
    const definition = table.query_definition orelse return error.InvalidSqlBackendResponse;
    var parsed = try @import("antfly_local_sources").schema_mod.parseValidatedTableSchema(a, definition.schema_json);
    defer parsed.deinit(a);
    const native = try @import("antfly_local_sources").schema_mod.deriveRuntimeTableSchema(a, parsed);
    if (native.storage_mode != .relational) return error.UnsupportedSqlShape;
    if (native.external_base_source != null) {
        const change = ddl.schema_change orelse return error.ExternalLakeReadOnly;
        switch (change) {
            .create_index => |index| if (index.unique) return error.ExternalLakeReadOnly,
            .drop_index => {},
            else => return error.ExternalLakeReadOnly,
        }
    }
    var schema = try std.json.parseFromSliceLeaky(std.json.Value, a, definition.schema_json, .{ .parse_numbers = false });
    if (!try @import("antfly_local_sources").sql_schema_ddl.apply(a, &schema, ddl)) return .{};
    _ = schema.object.swapRemove("version");
    const proposed = try std.json.Stringify.valueAlloc(a, schema, .{});
    const updated = try server.bindForeignKeySchema(a, target, table.name, proposed, definition.schema_json, identity, context);
    try context.ensureActive();
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
        const publication = server.beginFkGenerationPublication(alloc, context, identity, before.*, updated) catch |err| switch (err) {
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
        .add_column => |column| if (!column.nullable or column.default_expression != null or column.generated_expression != null)
            return rewriteSchema(server, identity, context, alloc, target, table.name, table.table_id, native.version, updated, if (column.default_expression != null) &.{column.name} else &.{}),
        .add_unique => |constraint| if (constraint.primary)
            return rewriteSchema(server, identity, context, alloc, target, table.name, table.table_id, native.version, updated, &.{}),
        else => {},
    };
    const retirement = try requiresRetirement(a, definition.schema_json, schema);
    const validation = ddl.schema_change.? == .validate_constraint;
    const activation_required = if (ddl.schema_change) |change| switch (change) {
        .create_index => |index| index.unique,
        .add_unique, .add_check, .add_foreign_key, .validate_constraint, .drop_constraint => true,
        else => false,
    } else false;
    // Allocate all durable identity fields before submission. Later timeout,
    // cancellation, or status-read failure cannot erase this acknowledgement.
    var receipt: ?catalog.DdlReceipt = if (activation_required or retirement) try newReceipt(alloc, target, table.table_id, native.version) else null;
    errdefer if (receipt) |value| {
        alloc.free(value.database);
        alloc.free(value.namespace);
        alloc.free(value.table);
        alloc.free(value.table_id);
    };
    // Physical identity is immutable and never reused; native schema CAS also
    // fences concurrent schema changes. Never replay an uncertain submission.
    const version = submitSchema(server, context, alloc, table.name, table.table_id, native.version, updated, retirement, validation, if (admission) |captured| captured.guard else null) catch |err| {
        if (err == error.ForeignKeyReferenced) return error.SqlDependentConstraint;
        if (err == error.MetadataMutationOutcomeUnknown or err == error.OutOfMemory) return error.SqlMutationOutcomeUnknown;
        return err;
    };
    if (receipt) |*value| {
        value.schema_version = version;
        awaitActivation(server, context, alloc, table.name, value) catch {
            value.state = .pending;
            value.diagnostic = "The declaration committed; activation is still pending. Inspect the table constraint status before using the index; do not replay the DDL.";
        };
        return .{ .mutation_outcome = if (value.state == .ready) .committed else if (value.state == .invalid) .committed_repair_required else .committed_pending, .receipt = value.* };
    }
    return .{};
}

fn rewriteSchema(server: *server_mod.ApiHttpServer, identity: ?server_mod.AuthenticatedIdentity, context: operation.RequestContext, alloc: std.mem.Allocator, target: domain.Target, physical: []const u8, table_id: u64, version: u32, schema: []const u8, default_columns: []const []const u8) !catalog.DdlOutcome {
    // Rewrite jobs and their staging plan must be committed by the metadata
    // owner. A data node's local restore history cannot admit this operation.
    if (server.restore_job_store.replicated == null) return error.SqlSchemaRewriteRequiresMetadataOwner;
    var snapshot = (try server.source.adminSnapshot()) orelse return error.UnsupportedSqlExecution;
    defer server.source.freeAdminSnapshot(&snapshot);
    const table = tables.findTableByName(&snapshot, physical) orelse return error.TableNotFound;
    if (table.table_id != table_id or try tables.schemaVersion(table.schema_json) != version) return error.SchemaVersionChanged;
    const target_version = std.math.add(u32, version, 1) catch return error.SqlLimitExceeded;
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
    var response = server.handleSchemaRewrite(table.*, schema, receipt.idempotency_key, identity, context, default_columns) catch |err| return rewriteCallFailure(receipt, err);
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
        => err,
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
    const unclassified = try rewriteResponse(alloc, 500, "{\"error\":\"failed to create restore job\"}", receipt);
    try std.testing.expectEqual(.admission_unknown, unclassified.receipt.?.state);
    try std.testing.expectEqualStrings("41", unclassified.receipt.?.restore_job_id.?);
    try std.testing.expectEqual(.admission_unknown, (try rewriteResponse(alloc, 503, "{\"admission_outcome\":\"unknown\"}", receipt)).receipt.?.state);
    try std.testing.expectEqual(.admission_unknown, (try rewriteResponse(alloc, 503, "{malformed", receipt)).receipt.?.state);
    try std.testing.expectError(error.RestoreValidationPending, rewriteCallFailure(receipt, error.RestoreValidationPending));
    try std.testing.expectError(error.RestoreValidationPending, rewriteCallFailure(receipt, error.GroupLeaderUnavailable));
    try std.testing.expectError(error.Forbidden, rewriteCallFailure(receipt, error.StoredDestinationAuthorizationRevoked));
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

fn submitSchema(server: *server_mod.ApiHttpServer, context: operation.RequestContext, alloc: std.mem.Allocator, physical: []const u8, table_id: u64, expected_version: u32, body: []const u8, retirement: bool, validation: bool, guard: ?domain.RelationMutationGuard) !u32 {
    const next_version = if (validation) expected_version else std.math.add(u32, expected_version, 1) catch return error.SqlLimitExceeded;
    if (!retirement and !validation) {
        if (guard) |owner| {
            var private = context;
            private.setting_admin = true;
            const bytes = try server.source.systemCatalog(alloc, private, .{ .relation_schema_mutate = .{ .guard = owner, .schema_json = body } });
            defer alloc.free(bytes);
            var result = std.json.parseFromSlice(@import("../system_catalog/server_call.zig").RelationReplacementResult, alloc, bytes, .{}) catch return error.MetadataMutationOutcomeUnknown;
            defer result.deinit();
            if (result.value.schema_version != next_version) return error.MetadataMutationOutcomeUnknown;
            return result.value.schema_version;
        }
        var result = try server.source.mutateSchema(alloc, physical, .replace, body, expected_version);
        defer result.deinit(alloc);
        return result.version;
    }
    var snapshot = (try server.source.adminSnapshot()) orelse return error.UnsupportedSqlExecution;
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
    if (guard) |owner| {
        var private = context;
        private.setting_admin = true;
        const bytes = try server.source.systemCatalog(alloc, private, .{ .relation_replace = .{ .guard = owner, .expected = table.*, .replacement = replacement.table } });
        defer alloc.free(bytes);
        var result = std.json.parseFromSlice(@import("../system_catalog/server_call.zig").RelationReplacementResult, alloc, bytes, .{}) catch return error.MetadataMutationOutcomeUnknown;
        defer result.deinit();
        // The retirement reservation still serves the predecessor schema.
        // Await the target epoch, not the currently active predecessor epoch.
        return next_version;
    }
    try server.source.replaceTableDefinition(table.*, replacement.table);
    return next_version;
}

fn authorizeIndexTable(identity: ?server_mod.AuthenticatedIdentity, alloc: std.mem.Allocator, target: domain.Target) !void {
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
        try std.testing.expectError(error.SqlLimitExceeded, submitSchema(&server, .{}, alloc, "items", 17, std.math.maxInt(u32), "{}", retirement, false, null));
    }
}

fn executeIndex(server: *server_mod.ApiHttpServer, identity: ?server_mod.AuthenticatedIdentity, context: operation.RequestContext, database: []const u8, namespace: []const u8, alloc: std.mem.Allocator, ddl: @import("antfly_local_sources").sql_ast.CatalogDdl, path: ?*const SearchPath) !catalog.DdlOutcome {
    // Preserve these statement contracts until their distributed build,
    // dependency and atomic multi-owner protocols are active. Never implement
    // them as a blocking alias or a sequence of independently committed drops.
    if (ddl.concurrently or ddl.cascade or ddl.index_targets.len > 1) return error.UnsupportedSqlShape;
    if (!server.source.vtable.supports_query_definitions) return error.UnsupportedSqlExecution;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const creating = ddl.action == .create;
    if (!creating and ddl.action != .drop) return error.UnsupportedSqlShape;
    const name = if (creating) ddl.index_table orelse return error.InvalidSqlSyntax else ddl.name;
    var target: domain.Target = .{ .database = name.database orelse database, .namespace = name.namespace orelse namespace, .table = name.table };
    try target.validate();
    const use_path = name.namespace == null and name.database == null and path != null;
    if (creating and !use_path) try authorizeIndexTable(identity, a, target);
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
    try authorizeIndexTable(identity, a, table_target);
    if (creating) {
        if (owner.owner.kind != .table) return error.SqlWrongObjectType;
        if (resolved.relations[selected + 1] != null) {
            if (ddl.conditional) return .{};
            return error.SqlIndexAlreadyExists;
        }
    } else switch (owner.owner.kind) {
        .index => {},
        .constraint_index => return error.SqlConstraintOwnedIndex,
        else => return error.SqlWrongObjectType,
    }
    var transition = ddl;
    transition.kind = .table;
    transition.action = .alter_schema;
    transition.name = .{ .database = table_target.database, .namespace = table_target.namespace, .table = table_target.table };
    return alterSchema(server, identity, context, alloc, table_target, transition, .{
        .table = owner.table,
        .guard = .{ .target = owner_target, .logical_table = owner.logical_table, .owner = owner.owner, .incarnation = resolved.relation_epoch.?.incarnation },
    });
}

pub fn execute(server: *server_mod.ApiHttpServer, identity: ?server_mod.AuthenticatedIdentity, context: operation.RequestContext, database: []const u8, namespace: []const u8, alloc: std.mem.Allocator, input: catalog.Ddl) !catalog.DdlOutcome {
    return executeWithPath(server, identity, context, database, namespace, alloc, input, null);
}

pub fn executeWithPath(server: *server_mod.ApiHttpServer, identity: ?server_mod.AuthenticatedIdentity, context: operation.RequestContext, database: []const u8, namespace: []const u8, alloc: std.mem.Allocator, input: catalog.Ddl, path: ?*const SearchPath) !catalog.DdlOutcome {
    try context.ensureActive();
    if (input == .policy_ddl) return @import("sql_policy_ddl.zig").execute(server, identity, context, database, namespace, alloc, input.policy_ddl);
    if (input == .catalog_ddl and input.catalog_ddl.kind == .index)
        return executeIndex(server, identity, context, database, namespace, alloc, input.catalog_ddl, path);
    const name = switch (input) {
        .create_table => |v| v.name,
        .drop_table => |v| v.table,
        .catalog_ddl => |v| v.name,
        .policy_ddl => unreachable,
    };
    const target: domain.Target = .{ .database = name.database orelse database, .namespace = name.namespace orelse namespace, .table = name.table };
    try target.validate();
    const kind: domain.Kind = switch (input) {
        .catalog_ddl => |ddl| switch (ddl.kind) {
            .index => unreachable,
            inline else => |tag| @field(domain.Kind, @tagName(tag)),
        },
        .policy_ddl => unreachable,
        else => .table,
    };
    const route: @import("../system_catalog/routes.zig").Route = .{ .kind = kind, .database = target.database, .namespace = target.namespace, .name = target.table };
    const resource = try @import("../system_catalog/routes.zig").resourceNameAlloc(alloc, route);
    defer alloc.free(resource);
    // Check logical scope before any catalog lookup, including IF EXISTS.
    const permission_kind: @import("../usermgr/mod.zig").ResourceType = switch (kind) {
        inline else => |tag| @field(@import("../usermgr/mod.zig").ResourceType, @tagName(tag)),
    };
    if (identity) |authenticated| if (!server_mod.permissionsAllow(authenticated.permissions, permission_kind, resource, .admin)) return error.Forbidden;
    if (input == .catalog_ddl and input.catalog_ddl.action == .alter_schema)
        return alterSchema(server, identity, context, alloc, target, input.catalog_ddl, null);
    if (input == .catalog_ddl and input.catalog_ddl.action == .truncate)
        return @import("sql_truncate.zig").execute(server, identity, context, database, namespace, alloc, input.catalog_ddl);
    var request: domain.Request = .{ .mutation = .{ .action = switch (input) {
        .create_table => .create,
        .drop_table => .drop,
        .catalog_ddl => |ddl| switch (ddl.action) {
            .alter_schema, .truncate => unreachable,
            inline else => |tag| @field(domain.Action, @tagName(tag)),
        },
        .policy_ddl => unreachable,
    }, .kind = kind, .database = target.database, .namespace = target.namespace, .name = target.table } };
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var creation_receipt: ?catalog.DdlReceipt = null;
    var receipt_transferred = false;
    defer if (!receipt_transferred) if (creation_receipt) |receipt| {
        alloc.free(receipt.database);
        alloc.free(receipt.namespace);
        alloc.free(receipt.table);
        alloc.free(receipt.table_id);
    };
    switch (input) {
        .create_table => |create| {
            request.mutation.tablespace = create.tablespace;
            request.physical_name = try server.catalogStorageNameAlloc(a);
            // Explicit empty search indexes avoids building a full-text index
            // on every SQL table. SQL indexes are added through native DDL.
            const bound_schema = try server.bindForeignKeySchema(a, target, request.physical_name.?, create.schema_json, "", identity, context);
            if (try @import("../metadata/fk_generation_publication.zig").schemaHasForeignKeys(a, bound_schema) and
                try @import("relational_witness_ddl.zig").needed(a, bound_schema, ""))
                return error.ForeignKeyPartialSupportIndexRequired;
            const schema_value = try std.json.parseFromSliceLeaky(std.json.Value, a, bound_schema, .{ .parse_numbers = false });
            const body = try std.json.Stringify.valueAlloc(a, .{ .schema = schema_value, .indexes = std.json.Value{ .object = .empty } }, .{});
            var parsed = try tables.parseStoredCreateTableRequest(alloc, body);
            parsed.storage = .{};
            defer parsed.deinit(alloc);
            if (create.tablespace) |tablespace| parsed.tablespace_name = try alloc.dupe(u8, tablespace);
            if (try @import("../metadata/fk_generation_publication.zig").schemaHasForeignKeys(a, bound_schema)) {
                const plan = @import("fk_initial_create_plan_builder.zig").build(server, a, context, identity, target, parsed) catch |err| switch (err) {
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
                const accepted = try server.submitFkInitialCreatePlan(a, context, plan);
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
                if (identity) |authenticated| if (!server_mod.permissionsAllow(authenticated.permissions, permission_kind, destination_resource, .admin)) return error.Forbidden;
            }
        },
        .policy_ddl => unreachable,
    }
    if (request.mutation.tablespace) |tablespace| if (identity) |authenticated| {
        if (!server_mod.permissionsAllow(authenticated.permissions, .tablespace, tablespace, .read)) return error.Forbidden;
    };
    const response = server.source.systemCatalog(alloc, context, .{ .mutate = request }) catch |err| {
        // Conditional DDL observes the authority's atomic outcome; there is no
        // existence preflight and no retry of an ambiguous durable submission.
        switch (input) {
            .create_table => |create| if (create.if_not_exists and (err == error.CatalogAlreadyExists or err == error.TableAlreadyExists)) return .{},
            .drop_table => |drop| if (drop.if_exists and (err == error.CatalogNotFound or err == error.TableNotFound)) return .{},
            .catalog_ddl => |ddl| if (ddl.conditional and ((ddl.action == .create and err == error.CatalogAlreadyExists) or (ddl.action == .drop and err == error.CatalogNotFound))) return .{},
            .policy_ddl => unreachable,
        }
        if (err == error.MetadataMutationOutcomeUnknown) return error.SqlMutationOutcomeUnknown;
        // Native catalog errors are intentionally resource-neutral. SQL's
        // diagnostic identity depends on the admitted target kind; translate
        // only after atomic conditional outcomes and uncertain commits.
        if (err == error.CatalogAlreadyExists) return switch (kind) {
            .database => error.SqlDuplicateDatabase,
            .namespace => error.SqlDuplicateNamespace,
            .tablespace => error.SqlDuplicateTablespace,
            .table => err,
        };
        if (err == error.CatalogNotFound) return switch (kind) {
            .database => error.DatabaseNotFound,
            .namespace => error.NamespaceNotFound,
            .tablespace => error.TablespaceNotFound,
            .table => err,
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
        awaitActivation(server, context, alloc, request.physical_name.?, receipt) catch {
            receipt.state = .pending;
            receipt.diagnostic = "Table creation committed; constraints are still being validated. Inspect table constraint status; do not replay the DDL.";
        };
        receipt_transferred = true;
        return .{ .mutation_outcome = if (receipt.state == .ready) .committed else if (receipt.state == .invalid) .committed_repair_required else .committed_pending, .receipt = receipt.* };
    }
    return .{};
}

fn awaitActivation(server: *server_mod.ApiHttpServer, context: operation.RequestContext, alloc: std.mem.Allocator, physical: []const u8, receipt: *catalog.DdlReceipt) !void {
    const reader = server.table_reads orelse return error.ConstraintActivationPending;
    var bounded = context;
    const now = if (context.deadline_io) |borrow| blk: {
        var receiver = try borrow.receive();
        break :blk @as(u64, @intCast(@max(0, std.Io.Clock.now(.awake, receiver.io()).nanoseconds)));
    } else @import("antfly_platform").time.monotonicNs();
    bounded.deadline_ns = @min(context.deadline_ns orelse std.math.maxInt(u64), now +| (2 * std.time.ns_per_s));
    while (true) {
        try bounded.ensureActive();
        const bytes = @import("relational_constraint_status.zig").collect(alloc, server.source, reader, physical, bounded) catch |err| switch (err) {
            error.ConstraintActivationPending, error.PreparedGenerationChanged, error.TopologyChanged => {
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

test "SQL catalog ALTER submits native schema CAS without client generations" {
    const Source = struct {
        updates: usize = 0,
        path_lookup: bool = false,
        index_exists: bool = false,
        fn status(_: *anyopaque) !@import("../metadata/api.zig").MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }
        fn run(raw: *anyopaque, alloc: std.mem.Allocator, context: operation.RequestContext, call: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (call == .relation_schema_mutate) {
                try std.testing.expect(context.setting_admin);
                try std.testing.expectEqual(@as(u64, 17), call.relation_schema_mutate.guard.owner.table_id);
                try std.testing.expectEqualStrings("items", call.relation_schema_mutate.guard.logical_table);
                if (self.path_lookup) try std.testing.expectEqualStrings("tenant", call.relation_schema_mutate.guard.target.namespace);
                var result = try mutate(raw, alloc, "table:immutable-17", .replace, call.relation_schema_mutate.schema_json, call.relation_schema_mutate.guard.owner.schema_version);
                defer result.deinit(alloc);
                return std.json.Stringify.valueAlloc(alloc, @import("../system_catalog/server_call.zig").RelationReplacementResult{ .schema_version = result.version }, .{});
            }
            try std.testing.expect(call == .resolve_many);
            if (call.resolve_many.relations.len != 0) {
                try std.testing.expectEqual(@as(usize, if (self.path_lookup) 4 else 2), call.resolve_many.relations.len);
                try std.testing.expectEqualStrings("items", call.resolve_many.relations[0].name);
                const table: @import("../metadata/table_manager.zig").TableRecord = .{ .table_id = 17, .name = "table:immutable-17", .schema_json = "{\"version\":7,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"}},\"additionalProperties\":false}}}}" };
                var resolved = domain.ResolvedTable.fromTable(table);
                resolved.query_definition = domain.QueryDefinition.fromTable(table);
                var relations: [4]?domain.ResolvedRelation = @splat(null);
                const selected: usize = if (self.path_lookup) 2 else 0;
                const table_owner: domain.ResolvedRelation = .{ .table = resolved, .logical_table = "items", .owner = .{ .table_id = 17, .schema_version = 7, .schema_digest = @splat(1), .kind = .table } };
                var index_owner = table_owner;
                index_owner.owner.kind = .index;
                relations[selected] = table_owner;
                if (self.index_exists) relations[selected + 1] = index_owner;
                if (self.path_lookup) {
                    try std.testing.expectEqualStrings("public", call.resolve_many.relations[0].namespace);
                    try std.testing.expectEqualStrings("tenant", call.resolve_many.relations[2].namespace);
                    // An index in an earlier namespace with no matching
                    // table must not collide with the selected table's index.
                    relations[1] = index_owner;
                }
                return std.json.Stringify.valueAlloc(alloc, domain.ResolvedMany{ .revision = 9, .tables = &.{}, .relations = relations[0..call.resolve_many.relations.len], .relation_epoch = .{ .revision = 9, .incarnation = @splat(1) } }, .{});
            }
            return std.json.Stringify.valueAlloc(alloc, .{ .revision = 9, .tables = .{.{ .table_id = 17, .name = "table:immutable-17", .query_definition = .{ .schema_json = "{\"version\":7,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"}},\"additionalProperties\":false}}}}", .read_schema_json = "", .indexes_json = "{}" } }} }, .{});
        }
        fn mutate(raw: *anyopaque, alloc: std.mem.Allocator, name: []const u8, mode: tables.SchemaMutationMode, body: []const u8, expected: ?u32) !tables.SchemaMutationResult {
            const self: *@This() = @ptrCast(@alignCast(raw));
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
    var server = server_mod.ApiHttpServer.init(alloc, .{}, .{ .ptr = &source, .vtable = &.{ .status = Source.status, .system_catalog = Source.run, .supports_query_definitions = true, .mutate_schema = Source.mutate } }, null, null);
    defer server.deinit();
    for ([_][]const u8{ "CREATE INDEX items_id ON items (id)", "ALTER TABLE items ADD COLUMN label TEXT", "CREATE INDEX expression_key ON items ((id + 1) DESC NULLS LAST) WHERE id > 0 AND id < 100" }) |sql| {
        var compiled = try @import("antfly_local_sources").sql_compiler.compile(alloc, sql, .{});
        defer compiled.deinit();
        try std.testing.expectEqual(catalog.MutationOutcome.committed, (try execute(&server, null, .{}, "default", "public", alloc, .{ .catalog_ddl = compiled.statement.catalog_ddl })).mutation_outcome);
    }
    try std.testing.expectEqual(@as(usize, 3), source.updates);
    var result_arena = std.heap.ArenaAllocator.init(alloc);
    defer result_arena.deinit();
    var unique = try @import("antfly_local_sources").sql_compiler.compile(alloc, "CREATE UNIQUE INDEX unique_id ON items (id)", .{});
    defer unique.deinit();
    const outcome = try execute(&server, null, .{}, "default", "public", result_arena.allocator(), .{ .catalog_ddl = unique.statement.catalog_ddl });
    try std.testing.expectEqual(catalog.MutationOutcome.committed_pending, outcome.mutation_outcome);
    try std.testing.expectEqual(.pending, outcome.receipt.?.state);
    try std.testing.expectEqual(@as(u32, 8), outcome.receipt.?.schema_version);
    try std.testing.expectEqualStrings("17", outcome.receipt.?.table_id);
    try std.testing.expectEqual(@as(usize, 4), source.updates);
    var check = try @import("antfly_local_sources").sql_compiler.compile(alloc, "ALTER TABLE items ADD CONSTRAINT positive CHECK (id > 0 AND id < 100)", .{});
    defer check.deinit();
    const checked = try execute(&server, null, .{}, "default", "public", result_arena.allocator(), .{ .catalog_ddl = check.statement.catalog_ddl });
    try std.testing.expectEqual(catalog.MutationOutcome.committed_pending, checked.mutation_outcome);
    try std.testing.expectEqual(@as(usize, 5), source.updates);
    for ([_][]const u8{
        "ALTER TABLE items ADD COLUMN n integer DEFAULT (2+3)",
        "ALTER TABLE items ADD COLUMN g bigint GENERATED ALWAYS AS (id+1) STORED",
    }) |sql| {
        var compiled = try @import("antfly_local_sources").sql_compiler.compile(alloc, sql, .{});
        defer compiled.deinit();
        try std.testing.expectError(error.SqlSchemaRewriteRequiresMetadataOwner, execute(&server, null, .{}, "default", "public", result_arena.allocator(), .{ .catalog_ddl = compiled.statement.catalog_ddl }));
        try std.testing.expectEqual(@as(usize, 5), source.updates);
    }
    var path: SearchPath = .{};
    try path.append(try @import("../pgwire/search_path.zig").Namespace.init("public"));
    try path.append(try @import("../pgwire/search_path.zig").Namespace.init("tenant"));
    source.path_lookup = true;
    var scoped = try @import("antfly_local_sources").sql_compiler.compile(alloc, "CREATE INDEX items_id ON items (id)", .{});
    defer scoped.deinit();
    const scoped_input: catalog.Ddl = .{ .catalog_ddl = scoped.statement.catalog_ddl };
    _ = try executeWithPath(&server, null, .{}, "default", "public", alloc, scoped_input, &path);
    try std.testing.expectEqual(@as(usize, 6), source.updates);
    source.index_exists = true;
    try std.testing.expectError(error.SqlIndexAlreadyExists, executeWithPath(&server, null, .{}, "default", "public", alloc, scoped_input, &path));
    var conditional_index = try @import("antfly_local_sources").sql_compiler.compile(alloc, "CREATE INDEX IF NOT EXISTS items_id ON items (id)", .{});
    defer conditional_index.deinit();
    _ = try executeWithPath(&server, null, .{}, "default", "public", alloc, .{ .catalog_ddl = conditional_index.statement.catalog_ddl }, &path);
    try std.testing.expectEqual(@as(usize, 6), source.updates);
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
            try std.testing.expect(call == .mutate);
            try std.testing.expectEqualStrings("analytics", call.mutate.mutation.database);
            try std.testing.expectEqualStrings("reporting", call.mutate.mutation.namespace);
            if (call.mutate.mutation.action == .create) {
                try std.testing.expect(call.mutate.create_table_json != null);
                try std.testing.expect(call.mutate.physical_name != null);
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
    try std.testing.expectEqual(@as(usize, 1), source.calls);
    source.failure = error.MetadataMutationOutcomeUnknown;
    try std.testing.expectError(error.SqlMutationOutcomeUnknown, execute(&server, null, .{}, "default", "public", alloc, input));
    try std.testing.expectEqual(@as(usize, 2), source.calls);
    source.failure = error.CatalogNotFound;
    try std.testing.expectEqual(catalog.MutationOutcome.committed, (try execute(&server, null, .{}, "default", "public", alloc, .{ .drop_table = .{ .table = input.create_table.name, .if_exists = true } })).mutation_outcome);
    try std.testing.expectEqual(@as(usize, 3), source.calls);
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
