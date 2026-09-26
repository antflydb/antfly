// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Authorized SQL catalog mutations use the same durable authority as REST.
const std = @import("std");
const server_mod = @import("http_server.zig");
const catalog = @import("../sql/catalog.zig");
const domain = @import("../system_catalog/domain.zig");
const operation = @import("operation.zig");
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

fn alterSchema(server: *server_mod.ApiHttpServer, identity: ?server_mod.AuthenticatedIdentity, context: operation.RequestContext, alloc: std.mem.Allocator, target: domain.Target, ddl: @import("../sql/ast.zig").CatalogDdl) !catalog.DdlOutcome {
    if (!server.source.vtable.supports_query_definitions) return error.UnsupportedSqlExecution;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const bytes = try server.source.systemCatalog(a, context, .{ .resolve_many = .{ .targets = &.{target}, .include_query_definitions = true } });
    const snapshot = try std.json.parseFromSliceLeaky(domain.ResolvedMany, a, bytes, .{ .allocate = .alloc_always });
    if (snapshot.tables.len != 1) return error.InvalidSqlBackendResponse;
    const table = snapshot.tables[0] orelse return error.TableNotFound;
    const definition = table.query_definition orelse return error.InvalidSqlBackendResponse;
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(a, definition.schema_json);
    defer parsed.deinit(a);
    const native = try @import("../schema/mod.zig").deriveRuntimeTableSchema(a, parsed);
    if (native.storage_mode != .relational) return error.UnsupportedSqlShape;
    var schema = try std.json.parseFromSliceLeaky(std.json.Value, a, definition.schema_json, .{ .parse_numbers = false });
    if (!try @import("../sql/schema_ddl.zig").apply(a, &schema, ddl)) return .{};
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
        .add_column => |column| if (!column.nullable or column.default_value != null)
            return rewriteSchema(server, identity, context, alloc, target, table.name, table.table_id, native.version, updated),
        .add_unique => |constraint| if (constraint.primary)
            return rewriteSchema(server, identity, context, alloc, target, table.name, table.table_id, native.version, updated),
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
    const version = submitSchema(server, context, alloc, table.name, table.table_id, native.version, updated, retirement, validation) catch |err| {
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

fn rewriteSchema(server: *server_mod.ApiHttpServer, identity: ?server_mod.AuthenticatedIdentity, context: operation.RequestContext, alloc: std.mem.Allocator, target: domain.Target, physical: []const u8, table_id: u64, version: u32, schema: []const u8) !catalog.DdlOutcome {
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
    var response = server.handlePublicSchemaRewrite(table.*, schema, receipt.idempotency_key, identity, context) catch |err| return rewriteCallFailure(receipt, err);
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

fn submitSchema(server: *server_mod.ApiHttpServer, context: operation.RequestContext, alloc: std.mem.Allocator, physical: []const u8, table_id: u64, expected_version: u32, body: []const u8, retirement: bool, validation: bool) !u32 {
    if (!retirement and !validation) {
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
    try server.source.replaceTableDefinition(table.*, replacement.table);
    return std.math.add(u32, expected_version, 1) catch unreachable;
}

pub fn execute(server: *server_mod.ApiHttpServer, identity: ?server_mod.AuthenticatedIdentity, context: operation.RequestContext, database: []const u8, namespace: []const u8, alloc: std.mem.Allocator, input: catalog.Ddl) !catalog.DdlOutcome {
    try context.ensureActive();
    if (input == .policy_ddl) return @import("sql_policy_ddl.zig").execute(server, identity, context, database, namespace, alloc, input.policy_ddl);
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
        return alterSchema(server, identity, context, alloc, target, input.catalog_ddl);
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
            var parsed = try tables.parseCreateTableRequest(alloc, body);
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
    var compiled = try @import("../sql/compiler.zig").compile(alloc, "CREATE TABLE items (id BIGINT NOT NULL DEFAULT 9007199254740993, label TEXT DEFAULT NULL, payload JSON DEFAULT NULL, created TIMESTAMPTZ DEFAULT '2026-09-21T00:00:00Z', enabled BOOLEAN DEFAULT TRUE, amount DOUBLE PRECISION DEFAULT 1.5)", .{});
    defer compiled.deinit();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const schema_json = try @import("../sql/ddl_runtime.zig").createSchemaAlloc(a, compiled.statement.create_table);
    const value = try std.json.parseFromSliceLeaky(std.json.Value, a, schema_json, .{ .parse_numbers = false });
    const body = try std.json.Stringify.valueAlloc(a, .{ .schema = value, .indexes = std.json.Value{ .object = .empty } }, .{});
    var request = try tables.parseCreateTableRequest(alloc, body);
    defer request.deinit(alloc);
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(alloc, request.schema_json.?);
    defer parsed.deinit(alloc);
    const native = try @import("../schema/mod.zig").deriveRuntimeTableSchema(alloc, parsed);
    defer @import("../storage/schema.zig").freeSchema(alloc, native);
    try std.testing.expectEqual(@as(usize, 6), native.relational_columns.len);
    try std.testing.expectEqual(@import("../storage/schema.zig").StorageMode.relational, native.storage_mode);
    try std.testing.expect(parsed.column_defaults != null);
}

test "SQL catalog ALTER submits native schema CAS without client generations" {
    const Source = struct {
        updates: usize = 0,
        fn status(_: *anyopaque) !@import("../metadata/api.zig").MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }
        fn run(_: *anyopaque, alloc: std.mem.Allocator, _: operation.RequestContext, call: domain.Call) ![]u8 {
            try std.testing.expect(call == .resolve_many);
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
        var compiled = try @import("../sql/compiler.zig").compile(alloc, sql, .{});
        defer compiled.deinit();
        try std.testing.expectEqual(catalog.MutationOutcome.committed, (try execute(&server, null, .{}, "default", "public", alloc, .{ .catalog_ddl = compiled.statement.catalog_ddl })).mutation_outcome);
    }
    try std.testing.expectEqual(@as(usize, 3), source.updates);
    var result_arena = std.heap.ArenaAllocator.init(alloc);
    defer result_arena.deinit();
    var unique = try @import("../sql/compiler.zig").compile(alloc, "CREATE UNIQUE INDEX unique_id ON items (id)", .{});
    defer unique.deinit();
    const outcome = try execute(&server, null, .{}, "default", "public", result_arena.allocator(), .{ .catalog_ddl = unique.statement.catalog_ddl });
    try std.testing.expectEqual(catalog.MutationOutcome.committed_pending, outcome.mutation_outcome);
    try std.testing.expectEqual(.pending, outcome.receipt.?.state);
    try std.testing.expectEqual(@as(u32, 8), outcome.receipt.?.schema_version);
    try std.testing.expectEqualStrings("17", outcome.receipt.?.table_id);
    try std.testing.expectEqual(@as(usize, 4), source.updates);
    var check = try @import("../sql/compiler.zig").compile(alloc, "ALTER TABLE items ADD CONSTRAINT positive CHECK (id > 0 AND id < 100)", .{});
    defer check.deinit();
    const checked = try execute(&server, null, .{}, "default", "public", result_arena.allocator(), .{ .catalog_ddl = check.statement.catalog_ddl });
    try std.testing.expectEqual(catalog.MutationOutcome.committed_pending, checked.mutation_outcome);
    try std.testing.expectEqual(@as(usize, 5), source.updates);
}

test "SQL catalog DDL authorizes before lookup and handles atomic conditional outcomes" {
    const Source = struct {
        calls: usize = 0,
        failure: ?anyerror = null,
        fn status(_: *anyopaque) !@import("../metadata/api.zig").MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }
        fn run(raw: *anyopaque, alloc: std.mem.Allocator, _: operation.RequestContext, call: domain.Call) ![]u8 {
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
