// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Destination authorization and immutable-ID mapping before the shared staged
//! restore reservation. No routine bodies are imported and no VM runs here.
const std = @import("std");
const local = @import("antfly_local_sources");
const http = @import("http_server.zig");
const calls = @import("../system_catalog/routines.zig");
const driver = @import("restore_staging_driver.zig");
const mapping = @import("restore_routine_mapping.zig");
const operation = local.api_operation;
const domain = local.system_catalog_domain;
const A = std.mem.Allocator;
fn identityForPrincipal(a: A, server: *http.ApiHttpServer, principal: []const u8, captured: ?mapping.Actor) !?http.AuthenticatedIdentity {
    if (!server.cfg.auth_enabled) return null;
    const manager = server.cfg.user_manager orelse return error.RestoreDestinationReauthorizationRequired;
    const actor = captured orelse return error.RestoreDestinationReauthorizationRequired;
    try actor.validate();
    if (!try manager.sqlCredentialPermits(actor.username, actor.instance, principal, "", 0)) return error.StoredDestinationAuthorizationRevoked;

    const permissions = if (std.mem.startsWith(u8, principal, "basic:")) try manager.getPermissionsForUser(principal[6..]) else if (std.mem.startsWith(u8, principal, "api-key:")) try manager.effectiveApiKeyPermissions(principal[8..]) else return error.RestoreDestinationReauthorizationRequired;
    defer {
        for (permissions) |*permission| permission.deinit(manager.alloc);
        manager.alloc.free(permissions);
    }
    const username = actor.username;
    return http.cloneCatalogIdentity(a, .{ .username = @constCast(username), .permissions = permissions, .live_user_manager = manager, .credential_principal = @constCast(principal), .user_instance_id = actor.instance });
}
fn snapshot(a: A, server: *http.ApiHttpServer, context: operation.RequestContext, request: calls.SnapshotRequest) !calls.Snapshot {
    const bytes = try server.source.systemCatalog(a, context, .{ .sql_routine_snapshot = request });
    const result = try std.json.parseFromSliceLeaky(calls.Snapshot, a, bytes, .{ .allocate = .alloc_always });
    try result.validate(request);
    return result;
}
pub fn prepare(a: A, server: *http.ApiHttpServer, input_context: operation.RequestContext, principal: []const u8, actor: ?mapping.Actor, values: ?[]const mapping.Mapping, index: *const domain.StateIndex, revision: u64, sources: []driver.SourceTable) !void {
    const mappings = values orelse &.{};
    try mapping.validate(mappings);
    var has_attachments = false;
    var demands_execution = false;
    for (sources) |source| {
        const root = try std.json.parseFromSliceLeaky(std.json.Value, a, if (source.manifest.schema_json.len == 0) "{}" else source.manifest.schema_json, .{});
        if (root == .object) if (root.object.get("sql_triggers")) |raw| {
            var checked = try local.schema_mod.table_triggers.parseValue(a, raw);
            defer checked.deinit();
            has_attachments = true;
            demands_execution = demands_execution or checked.value.entries.len != 0;
        };
    }
    if (!has_attachments) {
        if (mappings.len != 0) return error.InvalidRequest;
        return;
    }
    if (demands_execution) try @import("sql_preparation_authority_keys.zig").requireConfiguration(server.cfg);
    var identity = if (demands_execution) try identityForPrincipal(a, server, principal, actor) else null;
    defer if (identity) |*owned| owned.deinit(a);
    var context = input_context;
    context.setting_admin = true;
    context.row_policy_credential = &identity;
    // This read obtains the authoritative realm/revision only. The absent
    // spelling is not a destination lookup and can never substitute an ID.
    if (!server.source.vtable.supports_sql_routines) return error.MetadataCapabilityUnavailable;
    const realm = try snapshot(a, server, context, .{ .targets = &.{.{ .database = domain.default_database_name, .namespace = domain.default_namespace_name, .table = "__restore_catalog_identity__" }}, .expected_revision = revision });
    const used = try a.alloc(bool, mappings.len);
    @memset(used, false);
    for (sources) |*source| {
        const raw = try std.json.parseFromSliceLeaky(std.json.Value, a, source.manifest.schema_json, .{});
        if (raw != .object) return error.InvalidSqlTriggerDefinition;
        const value = raw.object.get("sql_triggers") orelse continue;
        var attachments = try local.schema_mod.table_triggers.parseValue(a, value);
        defer attachments.deinit();
        if (source.manifest.sql_column_identities_json.len == 0) return error.RestoreSourceProofMissing;
        source.trigger_destination_incarnation = realm.incarnation;
        var moves: std.ArrayList(local.sql_catalog.trigger_restore.RoutineMove) = .empty;
        for (attachments.value.entries) |entry| {
            const already = for (moves.items) |move| {
                if (move.source_id == entry.function.id) break true;
            } else false;
            if (already) continue;
            var destination_id = entry.function.id;
            var explicitly_mapped = false;
            for (mappings, 0..) |candidate, i| {
                const decoded = try mapping.decode(candidate);
                if (decoded.routine_id != entry.function.id or !std.mem.eql(u8, &decoded.incarnation, &attachments.value.metadata_incarnation)) continue;
                destination_id = decoded.destination_id;
                used[i] = true;
                explicitly_mapped = true;
                break;
            }
            if (!explicitly_mapped and !std.mem.eql(u8, &realm.incarnation, &attachments.value.metadata_incarnation)) return error.RestoreDependencyMissing;
            const cut = try snapshot(a, server, context, .{ .targets = &.{}, .immutable_id = .{ .id = destination_id, .incarnation = realm.incarnation }, .expected_revision = realm.revision, .kind = .function });
            const found = cut.found orelse return error.RestoreDependencyMissing;
            const scope = cut.scopes[found.scope];
            @import("sql_trigger_function_admission.zig").authorize(a, server.cfg.auth_enabled, identity, found.record, scope.target) catch |failure| return switch (failure) {
                error.Forbidden => error.StoredDestinationAuthorizationRevoked,
                error.InvalidSqlTriggerDefinition => error.InvalidRequest,
                else => failure,
            };
            if (source.catalog_binding) |binding| {
                const namespace = index.byId(.namespace, binding.parent_id) orelse return error.CatalogGenerationChanged;
                const database = index.byId(.database, namespace.parent_id) orelse return error.CatalogGenerationChanged;
                if (!std.mem.eql(u8, database.name, scope.target.database)) return error.InvalidSqlTriggerDefinition;
            } else if (!std.mem.eql(u8, scope.target.database, domain.default_database_name)) return error.InvalidSqlTriggerDefinition;
            try moves.append(a, .{ .source_id = entry.function.id, .destination = .{ .database = scope.target.database, .namespace = scope.target.namespace, .name = scope.target.table, .namespace_id = scope.namespace_id orelse return error.InvalidSqlBackendResponse, .id = destination_id }, .destination_generation = found.record.generation });
        }
        source.trigger_routines = try moves.toOwnedSlice(a);
    }
    for (used) |selected| if (!selected) return error.InvalidRequest;
}

/// Capture at public admission, never reconstruct a dropped/recreated user
/// from a username when a durable job resumes. Callers omit this field on
/// ordinary archives so predecessor restore-job wire/idempotency stays intact.
pub fn actorForAdmission(server: *http.ApiHttpServer, identity: ?http.AuthenticatedIdentity) !?mapping.Actor {
    if (!server.cfg.auth_enabled) return null;
    const authenticated = identity orelse return error.Forbidden;
    const instance = (try http.sqlPrincipalInstance(server, identity)) orelse return null;
    return .{ .username = authenticated.username, .instance = instance };
}
pub fn needsActorForSchema(a: A, json: []const u8) !bool {
    if (json.len == 0) return false;
    var root = try std.json.parseFromSlice(std.json.Value, a, json, .{});
    defer root.deinit();
    if (root.value != .object) return error.BackupIntegrityFailure;
    const raw = root.value.object.get("sql_triggers") orelse return false;
    var state = try local.schema_mod.table_triggers.parseValue(a, raw);
    defer state.deinit();
    return state.value.entries.len != 0;
}
/// Native cohort proofs expose a bounded source-definition flag without
/// opening each artifact. Legacy aggregates must inspect the selected table
/// manifests; no absence is inferred from a missing new aggregate field.
pub fn needsActorForCluster(a: A, location: *@import("backups.zig").BackupLocation, manifest: *const @import("backups.zig").ClusterBackupManifest, selected: ?[]const []const u8) !bool {
    const backups = @import("backups.zig");
    if (manifest.cohort_json.len != 0) {
        var proof = try std.json.parseFromSlice(local.metadata_backup_cohort.Job, a, manifest.cohort_json, .{});
        defer proof.deinit();
        try proof.value.validate();
        for (proof.value.tables) |table| {
            if (selected) |names| {
                const included = for (names) |name| {
                    if (std.mem.eql(u8, name, table.name)) break true;
                } else false;
                if (!included) continue;
            }
            if (table.trigger_source_columns_digest != null) return true;
        }
        return false;
    }
    for (manifest.tables) |entry| {
        if (selected) |names| {
            const included = for (names) |name| {
                if (std.mem.eql(u8, name, entry.name)) break true;
            } else false;
            if (!included) continue;
        }
        var table = try backups.readManifestFromLocationWithArtifactBackupId(a, location, entry.table_backup_id, entry.artifact_backup_id orelse entry.table_backup_id);
        defer table.deinit(a);
        if (try needsActorForSchema(a, table.schema_json)) return true;
    }
    return false;
}

pub fn reauthorize(a: A, server: *http.ApiHttpServer, input_context: operation.RequestContext, principal: []const u8, actor: ?mapping.Actor, plan: @import("../metadata/restore_staging.zig").Plan) !void {
    var required = false;
    for (plan.targets) |target| if (try needsActorForSchema(a, target.table.schema_json)) {
        required = true;
        break;
    };
    if (!required) return;
    try @import("sql_preparation_authority_keys.zig").requireConfiguration(server.cfg);
    var identity = try identityForPrincipal(a, server, principal, actor);
    defer if (identity) |*owned| owned.deinit(a);
    var context = input_context;
    context.setting_admin = true;
    context.row_policy_credential = &identity;
    var seen: std.AutoHashMap(u64, void) = .init(a);
    defer seen.deinit();
    var revision: ?u64 = null;
    for (plan.targets) |target| {
        const parsed = try std.json.parseFromSliceLeaky(std.json.Value, a, target.table.schema_json, .{});
        if (parsed != .object) return error.BackupIntegrityFailure;
        const raw = parsed.object.get("sql_triggers") orelse continue;
        var state = try local.schema_mod.table_triggers.parseValue(a, raw);
        defer state.deinit();
        for (state.value.entries) |entry| {
            if ((try seen.getOrPut(entry.function.id)).found_existing) continue;
            if (seen.count() > 256) return error.SqlProgramLimitExceeded;
            const cut = try snapshot(a, server, context, .{ .targets = &.{}, .immutable_id = .{ .id = entry.function.id, .incarnation = state.value.metadata_incarnation }, .expected_revision = revision, .kind = .function });
            revision = cut.revision;
            const found = cut.found orelse return error.StoredDestinationAuthorizationRevoked;
            const scope = cut.scopes[found.scope];
            @import("sql_trigger_function_admission.zig").authorize(a, server.cfg.auth_enabled, identity, found.record, scope.target) catch |failure| return switch (failure) {
                error.Forbidden => error.StoredDestinationAuthorizationRevoked,
                error.InvalidSqlTriggerDefinition => error.InvalidRequest,
                else => failure,
            };
        }
    }
    if (server.cfg.auth_enabled) {
        const captured = actor orelse return error.RestoreDestinationReauthorizationRequired;
        if (!try server.cfg.user_manager.?.sqlCredentialPermits(captured.username, captured.instance, principal, "", 0)) return error.StoredDestinationAuthorizationRevoked;
    }
}
/// Completion of an already published cut is maintenance, not a fresh user
/// admission. Observe the real native decision before revocation checks so a
/// lost publish reply cannot turn a committed restore into an API failure.
pub fn needsActorForTables(a: A, tables: []const local.common_topology_records.TableRecord) !bool {
    for (tables) |table| if (try needsActorForSchema(a, table.schema_json)) return true;
    return false;
}
pub fn needsActorForPlan(a: A, plan: @import("../metadata/restore_staging.zig").Plan) !bool {
    for (plan.targets) |target| if (try needsActorForSchema(a, target.table.schema_json)) return true;
    return false;
}
pub fn reauthorizeJob(a: A, server: *http.ApiHttpServer, context: operation.RequestContext, expected: @import("../metadata/restore_staging.zig").Job) !void {
    var required = false;
    for (expected.plan.targets) |target| if (try needsActorForSchema(a, target.table.schema_json)) {
        required = true;
        break;
    };
    if (!required or expected.state == .canceled or expected.state == .canceling) return;
    const recovery = @import("sql_relation_recovery_authority.zig");
    if (recovery.requiresNativeRecovery(expected.plan)) {
        try recovery.requireAdmitted(server, a, expected.plan, context);
        return;
    }
    const job_id = std.mem.readInt(u64, expected.plan.id[0..8], .little);
    const state_json = (try server.restore_job_store.load(a, job_id)) orelse return error.RestoreJobFenced;
    defer a.free(state_json);
    var state = try std.json.parseFromSlice(@import("restore_jobs.zig").JobState, a, state_json, .{});
    defer state.deinit();
    const native_json = (try server.source.getRestoreStaging(a, expected.plan.id, context)) orelse return error.RestoreJobFenced;
    defer a.free(native_json);
    var native = try std.json.parseFromSlice(@import("../metadata/restore_staging.zig").Job, a, native_json, .{});
    defer native.deinit();
    if (!std.mem.eql(u8, &native.value.plan_digest, &expected.plan_digest)) return error.RestoreStagingScopeChanged;
    if (@import("../metadata/restore_staging.zig").decisionCommitted(native.value) or native.value.state == .canceled or native.value.state == .canceling) return;
    if (state.value.source_kind == .table_snapshot or state.value.source_kind == .cluster_cohort) {
        // A new restored relation installs bindings under current destination
        // authority. Normal generation evolution preserves existing bindings
        // and keeps its ordinary table ACL checks, not creator privileges.
        try reauthorize(a, server, context, state.value.destination_authorization_principal, state.value.routine_actor, expected.plan);
    } else if (server.cfg.auth_enabled) {
        var actor = try identityForPrincipal(a, server, state.value.destination_authorization_principal, state.value.routine_actor);
        defer if (actor) |*identity| identity.deinit(a);
    }
}
