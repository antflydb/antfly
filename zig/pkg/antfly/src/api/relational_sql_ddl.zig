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
        .moved => return error.UnsupportedSqlShape,
        .table_ddl => |*table_plan| return try applyTableDdlPlanOnServiceWithSessionAlloc(alloc, svc, table_plan, session),
        .catalog_ddl => |catalog_plan| return try applyCatalogDdlPlanOnServiceWithSessionAlloc(alloc, svc, catalog_plan, session),
        .other_ddl => |*other_plan| return try applyOtherDdlPlanOnServiceWithSessionAlloc(alloc, svc, other_plan, session),
        .session => |session_plan| return try applySessionLogicalPlanWithSessionAlloc(alloc, svc, session_plan, session),
        .transaction => |transaction| switch (transaction) {
            .control, .prepared, .savepoint => return try applyTransactionLogicalPlanWithSessionAlloc(alloc, svc, transaction, session),
        },
        .prepared_statement => |prepared_statement| return try applyPreparedStatementLogicalPlanWithSessionAlloc(alloc, svc, prepared_statement, session),
        .cursor => |cursor| return try applyCursorLogicalPlanWithSessionAlloc(alloc, svc, cursor, session),
        .notification => |notification| return try applyNotificationLogicalPlanWithSessionAlloc(alloc, svc, notification, session),
        .routine => |routine| return try applyRoutineLogicalPlanWithSessionAlloc(alloc, svc, routine, session),
        .auth => |auth| return try applyAuthLogicalPlanWithSessionAlloc(alloc, svc, auth, session),
        .extension => |extension| return try applyExtensionLogicalPlanWithSessionAlloc(alloc, svc, extension, session),
        .maintenance => |maintenance| return try applyMaintenanceLogicalPlanWithSessionAlloc(alloc, svc, maintenance, session),
        .bulk_io => |bulk_io| return try applyBulkIoLogicalPlanWithSessionAlloc(alloc, svc, bulk_io, session),
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
            try applyDropTableCascadeReferences(svc, alloc, &snapshot, table.*);
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
    try svc.upsertTable(applied.table);
    try catalog_jobs.scheduleSchemaRewriteJobsForAppliedDdlOnService(svc, alloc, applied);
    return applied;
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

fn applyOtherDdlPlanOnServiceWithSessionAlloc(
    alloc: std.mem.Allocator,
    svc: anytype,
    plan: *sql_adapter.OtherDdlLogicalPlan,
    session: catalog_resources.SqlCatalogSession,
) !tables_api.AppliedRelationalSqlDdlRecord {
    _ = svc;
    _ = session;
    return switch (plan.*) {
        .adapter_noop => |noop| switch (noop.reason) {
            .extension => try extension_domain.sql_adapter.executeRelationalSqlExtensionNoopAlloc(alloc),
            else => error.UnsupportedSqlShape,
        },
        .moved => error.UnsupportedSqlShape,
    };
}

fn applySessionLogicalPlanWithSessionAlloc(
    alloc: std.mem.Allocator,
    svc: anytype,
    plan: sql_adapter.SessionCatalogPlan,
    session: catalog_resources.SqlCatalogSession,
) !tables_api.AppliedRelationalSqlDdlRecord {
    _ = alloc;
    _ = svc;
    _ = plan;
    _ = session;
    return error.UnsupportedSqlShape;
}

fn applyTransactionLogicalPlanWithSessionAlloc(
    alloc: std.mem.Allocator,
    svc: anytype,
    plan: sql_adapter.TransactionLogicalPlan,
    session: catalog_resources.SqlCatalogSession,
) !tables_api.AppliedRelationalSqlDdlRecord {
    _ = alloc;
    _ = svc;
    _ = plan;
    _ = session;
    return error.UnsupportedSqlShape;
}

fn applyPreparedStatementLogicalPlanWithSessionAlloc(
    alloc: std.mem.Allocator,
    svc: anytype,
    plan: sql_adapter.PreparedStatementPlan,
    session: catalog_resources.SqlCatalogSession,
) !tables_api.AppliedRelationalSqlDdlRecord {
    _ = alloc;
    _ = svc;
    _ = plan;
    _ = session;
    return error.UnsupportedSqlShape;
}

fn applyCursorLogicalPlanWithSessionAlloc(
    alloc: std.mem.Allocator,
    svc: anytype,
    plan: sql_adapter.CursorPortalPlan,
    session: catalog_resources.SqlCatalogSession,
) !tables_api.AppliedRelationalSqlDdlRecord {
    _ = alloc;
    _ = svc;
    _ = plan;
    _ = session;
    return error.UnsupportedSqlShape;
}

fn applyNotificationLogicalPlanWithSessionAlloc(
    alloc: std.mem.Allocator,
    svc: anytype,
    plan: sql_adapter.NotificationChannelPlan,
    session: catalog_resources.SqlCatalogSession,
) !tables_api.AppliedRelationalSqlDdlRecord {
    _ = alloc;
    _ = svc;
    _ = plan;
    _ = session;
    return error.UnsupportedSqlShape;
}

fn applyRoutineLogicalPlanWithSessionAlloc(
    alloc: std.mem.Allocator,
    svc: anytype,
    plan: sql_adapter.RoutineLogicalPlan,
    session: catalog_resources.SqlCatalogSession,
) !tables_api.AppliedRelationalSqlDdlRecord {
    _ = alloc;
    _ = svc;
    _ = plan;
    _ = session;
    return error.UnsupportedSqlShape;
}

fn applyAuthLogicalPlanWithSessionAlloc(
    alloc: std.mem.Allocator,
    svc: anytype,
    plan: sql_adapter.AuthorizationLogicalPlan,
    session: catalog_resources.SqlCatalogSession,
) !tables_api.AppliedRelationalSqlDdlRecord {
    _ = alloc;
    _ = svc;
    _ = plan;
    _ = session;
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

fn applyMaintenanceLogicalPlanWithSessionAlloc(
    alloc: std.mem.Allocator,
    svc: anytype,
    plan: sql_adapter.MaintenanceJobPlan,
    session: catalog_resources.SqlCatalogSession,
) !tables_api.AppliedRelationalSqlDdlRecord {
    _ = alloc;
    _ = svc;
    _ = plan;
    _ = session;
    return error.UnsupportedSqlShape;
}

fn applyBulkIoLogicalPlanWithSessionAlloc(
    alloc: std.mem.Allocator,
    svc: anytype,
    plan: sql_adapter.BulkIoPlan,
    session: catalog_resources.SqlCatalogSession,
) !tables_api.AppliedRelationalSqlDdlRecord {
    _ = alloc;
    _ = svc;
    _ = plan;
    _ = session;
    return error.UnsupportedSqlShape;
}

fn applyDropTableCascadeReferences(
    svc: anytype,
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    target_table: metadata_table_manager.TableRecord,
) !void {
    for (snapshot.tables) |candidate_table| {
        if (candidate_table.table_id == target_table.table_id) continue;
        const next_schema_json = (try tables_api.schemaWithoutForeignKeysReferencingTableAlloc(
            alloc,
            candidate_table.schema_json,
            target_table.name,
        )) orelse continue;
        defer alloc.free(next_schema_json);

        const updated = try tables_api.applySchemaUpdateRecord(alloc, &candidate_table, next_schema_json);
        defer metadata_table_manager.freeTable(alloc, updated);
        try svc.upsertTable(updated);
    }
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
