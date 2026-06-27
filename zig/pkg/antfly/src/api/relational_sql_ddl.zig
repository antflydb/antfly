// Copyright 2026 Antfly, Inc.
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

const std = @import("std");

const catalog_jobs = @import("catalog_jobs.zig");
const catalog_resources = @import("catalog_resources.zig");
const extension_domain = @import("../extensions/mod.zig");
const metadata_api = @import("../metadata/api.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const metadata_table_workflow = @import("../metadata/table_workflow.zig");
const sql_adapter = @import("../sql/mod.zig");
const tables_api = @import("tables.zig");

pub fn applyDurablePlanOnServiceWithSessionAlloc(
    alloc: std.mem.Allocator,
    svc: anytype,
    plan: *sql_adapter.DurableSqlPlan,
    session: catalog_resources.SqlCatalogSession,
) !tables_api.AppliedRelationalSqlDdlRecord {
    switch (plan.*) {
        .table_ddl => |*table_plan| return try applyTableDdlPlanOnServiceWithSessionAlloc(alloc, svc, table_plan, session),
        .catalog_ddl => |catalog_plan| return try applyCatalogDdlPlanOnServiceWithSessionAlloc(alloc, svc, catalog_plan, session),
        .extension => |extension| return try applyExtensionLogicalPlanWithSessionAlloc(alloc, svc, extension, session),
        .auth => |auth_plan| return try applyAuthorizationLogicalPlanWithSessionAlloc(alloc, svc, auth_plan, session),
        .routine => |*routine_plan| return try applyRoutineLogicalPlanWithSessionAlloc(alloc, svc, routine_plan, session),
        .maintenance => |maintenance| return try applyMaintenanceLogicalPlanWithSessionAlloc(alloc, svc, maintenance, session),
        .bulk_io => return error.UnsupportedSqlShape,
    }
}

fn applyTableDdlPlanOnServiceWithSessionAlloc(
    alloc: std.mem.Allocator,
    svc: anytype,
    plan: *sql_adapter.TableDdlLogicalPlan,
    session: catalog_resources.SqlCatalogSession,
) !tables_api.AppliedRelationalSqlDdlRecord {
    var snapshot = try svc.adminSnapshot();
    defer svc.freeAdminSnapshot(&snapshot);

    if (try tables_api.applyUntargetedRelationalDerivedIndexTablePlanOnServiceWithSessionAlloc(alloc, svc, &snapshot, plan, session)) |applied| {
        return applied;
    }

    var target = try tables_api.relationalSqlDdlTargetForTablePlanWithSessionAlloc(alloc, plan.*, session);
    defer target.deinit(alloc);

    if (target.createsTable()) {
        try tables_api.validateRelationalSqlDdlNamespace(&snapshot, target);
        if (tables_api.findTableByQualifiedName(&snapshot, target.database_name, target.namespace_name, target.table_name) != null) return error.TableAlreadyExists;

        const base_table = tables_api.deriveRelationalSqlDdlTargetTableRecord(target);
        var policy_table: ?metadata_table_manager.TableRecord = null;
        defer if (policy_table) |record| metadata_table_manager.freeTable(alloc, record);
        const resolved_table = if (tables_api.effectiveTablespaceForTarget(&snapshot, target.database_name, target.namespace_name, null)) |tablespace| blk: {
            policy_table = try tables_api.applyTablespacePlacementPolicyAlloc(alloc, base_table, tablespace);
            break :blk policy_table.?;
        } else base_table;

        var applied = try tables_api.applyTableDdlPlanToTableRecordWithSessionAlloc(alloc, &resolved_table, plan, session);
        errdefer applied.deinit(alloc);
        applied.created_table = true;
        try tables_api.validateRelationalForeignKeyCatalogReferences(alloc, &snapshot, applied.table);

        const ranges = try tables_api.deriveInitialRanges(alloc, applied.table);
        defer {
            for (ranges) |record| metadata_table_manager.freeRange(alloc, record);
            alloc.free(ranges);
        }
        var workflow = metadata_table_workflow.TableWorkflow.init(alloc);
        defer workflow.deinit();
        _ = try workflow.createTableWithRanges(svc, applied.table, ranges);
        try catalog_jobs.scheduleSchemaRewriteJobsForAppliedDdlOnService(svc, alloc, applied);
        return applied;
    }

    const table = tables_api.findTableByQualifiedName(&snapshot, target.database_name, target.namespace_name, target.table_name) orelse {
        if (target.dropsTable() and target.if_exists) {
            return try tables_api.missingQualifiedDropTableIfExistsNoopAlloc(alloc, target.database_name, target.namespace_name, target.table_name);
        }
        return error.TableNotFound;
    };

    if (target.dropsTable()) {
        if (target.cascade) {
            const cascade_applied = try applyDropTableCascadeReferencesAlloc(svc, alloc, &snapshot, table.*);
            defer {
                for (cascade_applied) |*applied| applied.deinit(alloc);
                alloc.free(cascade_applied);
            }
        } else {
            try tables_api.validateRelationalTableDropAllowed(alloc, &snapshot, table.*);
        }
        var dropped = try droppedTableRecordAlloc(alloc, table.*);
        errdefer dropped.deinit(alloc);
        var workflow = metadata_table_workflow.TableWorkflow.init(alloc);
        defer workflow.deinit();
        _ = try workflow.dropTable(svc, table.table_id);
        return dropped;
    }

    if (try tables_api.applyRelationalDerivedIndexTablePlanOnServiceAlloc(alloc, svc, table, target, plan.*)) |derived_applied| {
        return derived_applied;
    }

    var applied = try tables_api.applyTableDdlPlanToTableRecordWithSessionAlloc(alloc, table, plan, session);
    errdefer applied.deinit(alloc);
    try tables_api.validateRelationalForeignKeyCatalogReferences(alloc, &snapshot, applied.table);
    const rewrite_jobs = try catalog_jobs.schemaRewriteJobsForAppliedDdlSnapshotAlloc(alloc, snapshot.ranges, applied);
    defer {
        for (rewrite_jobs) |record| metadata_table_manager.freeSchemaRewriteJob(alloc, record);
        alloc.free(rewrite_jobs);
    }
    try applyTableCatalogUpdateWithSchemaRewriteJobsOnService(svc, .{
        .table = applied.table,
        .schema_rewrite_jobs = rewrite_jobs,
    });
    return applied;
}

fn applyTableCatalogUpdateWithSchemaRewriteJobsOnService(
    svc: anytype,
    request: metadata_table_manager.TableCatalogUpdateWithSchemaRewriteJobsRequest,
) !void {
    const ServiceDeclType = serviceDeclType(@TypeOf(svc));
    if (comptime @hasDecl(ServiceDeclType, "applyTableCatalogUpdateWithSchemaRewriteJobs")) {
        return try svc.applyTableCatalogUpdateWithSchemaRewriteJobs(request);
    }
    return error.UnsupportedOperation;
}

fn applyCatalogDdlPlanOnServiceWithSessionAlloc(
    alloc: std.mem.Allocator,
    svc: anytype,
    plan: sql_adapter.CatalogDdlLogicalPlan,
    session: catalog_resources.SqlCatalogSession,
) !tables_api.AppliedRelationalSqlDdlRecord {
    var snapshot = try svc.adminSnapshot();
    defer svc.freeAdminSnapshot(&snapshot);
    if (try tables_api.applyCatalogDdlPlanOnServiceWithSessionAlloc(alloc, svc, &snapshot, plan, session)) |applied| {
        try catalog_jobs.scheduleSchemaRewriteJobsForAppliedDdlOnService(svc, alloc, applied);
        return applied;
    }
    return error.UnsupportedSqlShape;
}

fn applyExtensionLogicalPlanWithSessionAlloc(
    alloc: std.mem.Allocator,
    svc: anytype,
    plan: sql_adapter.ExtensionCatalogPlan,
    session: catalog_resources.SqlCatalogSession,
) !tables_api.AppliedRelationalSqlDdlRecord {
    _ = session;
    return try extension_domain.sql_adapter.executeRelationalSqlExtensionPlanOnService(svc, alloc, plan);
}

pub fn applyAuthorizationLogicalPlanWithSessionAlloc(
    alloc: std.mem.Allocator,
    svc: anytype,
    plan: sql_adapter.AuthorizationLogicalPlan,
    session: catalog_resources.SqlCatalogSession,
) !tables_api.AppliedRelationalSqlDdlRecord {
    const ServiceDeclType = serviceDeclType(@TypeOf(svc));
    if (comptime @hasDecl(ServiceDeclType, "applyAuthorizationLogicalPlanWithSessionAlloc")) {
        return try svc.applyAuthorizationLogicalPlanWithSessionAlloc(alloc, plan, session);
    }
    return error.UnsupportedSqlShape;
}

pub fn applyRoutineLogicalPlanWithSessionAlloc(
    alloc: std.mem.Allocator,
    svc: anytype,
    plan: *sql_adapter.RoutineLogicalPlan,
    session: catalog_resources.SqlCatalogSession,
) !tables_api.AppliedRelationalSqlDdlRecord {
    const ServiceDeclType = serviceDeclType(@TypeOf(svc));
    if (comptime @hasDecl(ServiceDeclType, "applyRoutineLogicalPlanWithSessionAlloc")) {
        return try svc.applyRoutineLogicalPlanWithSessionAlloc(alloc, plan, session);
    }
    return error.UnsupportedSqlShape;
}

pub fn applyMaintenanceLogicalPlanWithSessionAlloc(
    alloc: std.mem.Allocator,
    svc: anytype,
    plan: sql_adapter.MaintenanceJobPlan,
    session: catalog_resources.SqlCatalogSession,
) !tables_api.AppliedRelationalSqlDdlRecord {
    return switch (plan) {
        .reindex => |reindex| try applyReindexMaintenancePlanWithSessionAlloc(alloc, svc, reindex, session),
        .vacuum, .analyze, .cluster => error.UnsupportedSqlShape,
    };
}

fn applyReindexMaintenancePlanWithSessionAlloc(
    alloc: std.mem.Allocator,
    svc: anytype,
    plan: sql_adapter.ReindexMaintenancePlan,
    session: catalog_resources.SqlCatalogSession,
) !tables_api.AppliedRelationalSqlDdlRecord {
    var snapshot = try svc.adminSnapshot();
    defer svc.freeAdminSnapshot(&snapshot);
    const table = switch (plan.target) {
        .table => try reindexTableTargetWithSessionAlloc(alloc, svc, &snapshot, plan.name, session),
        .index => try reindexIndexTargetWithSessionAlloc(alloc, svc, &snapshot, plan.name, session),
        .schema, .database, .system => return error.UnsupportedSqlShape,
    };
    return table;
}

fn reindexTableTargetWithSessionAlloc(
    alloc: std.mem.Allocator,
    svc: anytype,
    snapshot: *const metadata_api.AdminSnapshot,
    table_name: []const u8,
    session: catalog_resources.SqlCatalogSession,
) !tables_api.AppliedRelationalSqlDdlRecord {
    const target = try session.tableTargetFromObjectName(table_name);
    const table = tables_api.findTableByQualifiedName(snapshot, target.database_name, target.namespace_name, target.table_name) orelse return error.TableNotFound;
    const schema_json = try schemaWithAllSecondaryIndexesBuildingAlloc(alloc, table.schema_json);
    defer alloc.free(schema_json);
    return try applySecondaryIndexRebuildSchemaAlloc(alloc, svc, table, schema_json);
}

fn reindexIndexTargetWithSessionAlloc(
    alloc: std.mem.Allocator,
    svc: anytype,
    snapshot: *const metadata_api.AdminSnapshot,
    index_name: []const u8,
    session: catalog_resources.SqlCatalogSession,
) !tables_api.AppliedRelationalSqlDdlRecord {
    const target = try session.tableTargetFromObjectName(index_name);
    var found_table: ?*const metadata_table_manager.TableRecord = null;
    var matched_generation: u64 = 0;
    for (snapshot.tables) |*table| {
        if (!std.mem.eql(u8, table.database_name, target.database_name)) continue;
        if (!std.mem.eql(u8, table.namespace_name, target.namespace_name)) continue;
        var parsed = try tables_api.parseValidatedTableSchema(alloc, table.schema_json);
        defer parsed.deinit(alloc);
        const runtime = try tables_api.deriveRuntimeTableSchema(alloc, parsed);
        defer @import("../storage/schema.zig").freeSchema(alloc, runtime);
        if (runtime.storage_mode != .relational) continue;
        for (runtime.relational_columns) |column| {
            if (!column.indexed) continue;
            const identity = column.index_name orelse column.name;
            if (!std.mem.eql(u8, identity, target.table_name)) continue;
            if (found_table != null) return error.InvalidSqlCatalog;
            found_table = table;
            matched_generation = column.index_generation;
        }
    }
    const table = found_table orelse return error.IndexNotFound;
    if (matched_generation == std.math.maxInt(u64)) return error.InvalidSchemaUpdateRequest;
    const schema_json = try tables_api.schemaWithSecondaryIndexBuildingAlloc(alloc, table.schema_json, target.table_name, matched_generation + 1);
    defer alloc.free(schema_json);
    return try applySecondaryIndexRebuildSchemaAlloc(alloc, svc, table, schema_json);
}

fn schemaWithAllSecondaryIndexesBuildingAlloc(
    alloc: std.mem.Allocator,
    schema_json: []const u8,
) ![]u8 {
    var parsed = try tables_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const runtime = try tables_api.deriveRuntimeTableSchema(alloc, parsed);
    defer @import("../storage/schema.zig").freeSchema(alloc, runtime);
    if (runtime.storage_mode != .relational) return error.UnsupportedSqlShape;

    var current_schema_json = try alloc.dupe(u8, schema_json);
    errdefer alloc.free(current_schema_json);
    var changed = false;
    for (runtime.relational_columns) |column| {
        if (!column.indexed) continue;
        if (column.index_generation == std.math.maxInt(u64)) return error.InvalidSchemaUpdateRequest;
        const identity = column.index_name orelse column.name;
        const updated = try tables_api.schemaWithSecondaryIndexBuildingAlloc(alloc, current_schema_json, identity, column.index_generation + 1);
        alloc.free(current_schema_json);
        current_schema_json = updated;
        changed = true;
    }
    if (!changed) return error.IndexNotFound;
    return current_schema_json;
}

fn applySecondaryIndexRebuildSchemaAlloc(
    alloc: std.mem.Allocator,
    svc: anytype,
    table: *const metadata_table_manager.TableRecord,
    schema_json: []const u8,
) !tables_api.AppliedRelationalSqlDdlRecord {
    var applied = try tables_api.emptyAppliedRelationalSqlDdlRecordAlloc(alloc);
    errdefer applied.deinit(alloc);
    metadata_table_manager.freeTable(alloc, applied.table);
    applied.table = try tables_api.applySchemaUpdateRecord(alloc, table, schema_json);
    applied.requires_rebuild = true;
    try applyTableCatalogUpdateWithSchemaRewriteJobsOnService(svc, .{ .table = applied.table });
    return applied;
}

fn serviceDeclType(comptime ServiceType: type) type {
    return switch (@typeInfo(ServiceType)) {
        .pointer => |pointer| pointer.child,
        else => ServiceType,
    };
}

fn applyDropTableCascadeReferencesAlloc(
    svc: anytype,
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    target_table: metadata_table_manager.TableRecord,
) ![]tables_api.AppliedRelationalSqlDdlRecord {
    var applied_updates = std.ArrayListUnmanaged(tables_api.AppliedRelationalSqlDdlRecord).empty;
    errdefer {
        for (applied_updates.items) |*applied| applied.deinit(alloc);
        applied_updates.deinit(alloc);
    }

    for (snapshot.tables) |candidate_table| {
        if (candidate_table.table_id == target_table.table_id) continue;
        const next_schema_json = (try tables_api.schemaWithoutForeignKeysReferencingTableAlloc(
            alloc,
            candidate_table.schema_json,
            target_table.name,
        )) orelse continue;
        defer alloc.free(next_schema_json);

        const updated = try tables_api.applySchemaUpdateRecord(alloc, &candidate_table, next_schema_json);
        var updated_transferred = false;
        errdefer if (!updated_transferred) metadata_table_manager.freeTable(alloc, updated);
        var applied = tables_api.AppliedRelationalSqlDdlRecord{
            .table = updated,
        };
        var applied_transferred = false;
        errdefer if (!applied_transferred) applied.deinit(alloc);

        const rewrite_jobs = try catalog_jobs.schemaRewriteJobsForAppliedDdlSnapshotAlloc(alloc, snapshot.ranges, applied);
        defer {
            for (rewrite_jobs) |record| metadata_table_manager.freeSchemaRewriteJob(alloc, record);
            alloc.free(rewrite_jobs);
        }
        try applyTableCatalogUpdateWithSchemaRewriteJobsOnService(svc, .{
            .table = applied.table,
            .schema_rewrite_jobs = rewrite_jobs,
        });
        try applied_updates.append(alloc, applied);
        applied_transferred = true;
        updated_transferred = true;
    }

    return try applied_updates.toOwnedSlice(alloc);
}

fn droppedTableRecordAlloc(
    alloc: std.mem.Allocator,
    table: metadata_table_manager.TableRecord,
) !tables_api.AppliedRelationalSqlDdlRecord {
    const dropped = try metadata_table_manager.cloneTable(alloc, table);
    errdefer metadata_table_manager.freeTable(alloc, dropped);
    const work_items = try sql_adapter.appliedDdlTableWorkItemsForFlagsAlloc(alloc, true, true, true);
    errdefer if (work_items.len > 0) {
        for (work_items) |item| {
            var mutable = item;
            mutable.deinit(alloc);
        }
        alloc.free(work_items);
    };
    return .{
        .table = dropped,
        .dropped_table = true,
        .requires_rebuild = true,
        .validation_required = true,
        .rewrite_required = true,
        .work_items = work_items,
    };
}
