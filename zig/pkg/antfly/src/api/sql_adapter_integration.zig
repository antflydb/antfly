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

const catalog_resources = @import("catalog_resources.zig");
const db_mod = @import("../storage/db/mod.zig");
const relational_rows = @import("relational_rows.zig");
const sql_adapter_runtime = @import("../sql/runtime.zig");
const runtime_schema = @import("../storage/schema.zig");
const schema_api = @import("../schema/mod.zig");
const sql_adapter = @import("../sql/mod.zig");
const ddl_plan = @import("../sql/ddl.zig");
const table_catalog = @import("table_catalog.zig");
const transactions_mod = @import("../storage/transactions.zig");
const usermgr = @import("../usermgr/mod.zig");

const AppParitySourceSchemaCatalog = sql_adapter.AppParitySourceSchemaCatalog;

const AppParityDdlSummaryPlan = union(enum) {
    session_catalog: ddl_plan.SessionCatalogPlan,
    create_table: ddl_plan.CreateTablePlan,
    table_clone: ddl_plan.TableClonePlan,
    view_catalog: ddl_plan.ViewCatalogPlan,
    materialized_view_catalog: ddl_plan.MaterializedViewCatalogPlan,
    relation_lifetime: ddl_plan.RelationLifetimePlan,
    enum_type_catalog: ddl_plan.EnumTypeCatalogPlan,
    domain_catalog: ddl_plan.DomainCatalogPlan,
    sequence_catalog: ddl_plan.SequenceCatalogPlan,
    identity_allocator_catalog: ddl_plan.IdentityAllocatorPlan,
    schema_namespace_catalog: ddl_plan.SchemaNamespaceCatalogPlan,
    extension_catalog: ddl_plan.ExtensionCatalogPlan,
    function_catalog: ddl_plan.FunctionCatalogPlan,
    trigger_catalog: ddl_plan.TriggerCatalogPlan,
    procedure_call: ddl_plan.ProcedureCallPlan,
    authorization_catalog: ddl_plan.AuthorizationCatalogPlan,
    bulk_io: ddl_plan.BulkIoPlan,
    table_partition_catalog: ddl_plan.TablePartitionCatalogPlan,
    row_security_catalog: ddl_plan.RowSecurityCatalogPlan,
    database_catalog: ddl_plan.DatabaseCatalogPlan,
    tablespace_catalog: ddl_plan.TablespaceCatalogPlan,
    notification_channel: ddl_plan.NotificationChannelPlan,
    logical_replication: ddl_plan.LogicalReplicationPlan,
    type_system_catalog: ddl_plan.TypeSystemCatalogPlan,
    maintenance_job: ddl_plan.MaintenanceJobPlan,
    prepared_statement: ddl_plan.PreparedStatementPlan,
    prepared_transaction: ddl_plan.PreparedTransactionPlan,
    cursor_portal: ddl_plan.CursorPortalPlan,
    savepoint_transaction: ddl_plan.SavepointTransactionPlan,
    comment_metadata: ddl_plan.CommentMetadataPlan,
    transaction_control: ddl_plan.TransactionControlPlan,
    create_index: ddl_plan.CreateIndexPlan,
    drop_index: ddl_plan.DropIndexPlan,
    drop_table: ddl_plan.DropTablePlan,
    alter_table: ddl_plan.AlterTablePlan,
    create_update_policy: ddl_plan.CreateUpdatePolicyPlan,
};

const Parser = sql_adapter.ParserState;

const TestPrimaryResolver = sql_adapter.TestPrimaryResolver;

const AppParityCorpusPlanFamily = sql_adapter.AppParityCorpusPlanFamily;
const AppParityDdlTag = sql_adapter.AppParityDdlTag;
const AppParityPlanSummary = sql_adapter.AppParityPlanSummary;
const AppParityCorpusEntry = sql_adapter.AppParityCorpusEntry;

const expectOptionalUsize = sql_adapter.expectOptionalUsize;
const expectOptionalU32 = sql_adapter.expectOptionalU32;
const expectOptionalTableName = sql_adapter.expectOptionalTableName;
const expectAppParityPlan = sql_adapter.expectAppParityPlan;
const expectAppParityReturningRows = sql_adapter.expectAppParityReturningRows;

const adapterNoopFingerprintAlloc = sql_adapter.appParityAdapterNoopFingerprintAlloc;
const expectFailClosedUnsupported = sql_adapter.expectFailClosedUnsupported;
const expectTypedInvalid = sql_adapter.expectTypedInvalid;

const appParityLimitValue = sql_adapter.appParityLimitValue;
const appParityBoolValue = sql_adapter.appParityBoolValue;
const sqlJoinTypeFingerprintName = sql_adapter.sqlJoinTypeFingerprintName;
const queryFingerprintAlloc = sql_adapter.queryFingerprintAlloc;
const aggregatePlanFingerprintAlloc = sql_adapter.aggregatePlanFingerprintAlloc;
const joinFingerprintAlloc = sql_adapter.joinFingerprintAlloc;
const lateralFingerprintAlloc = sql_adapter.lateralFingerprintAlloc;
const readPlanFingerprintAlloc = sql_adapter.readPlanFingerprintAlloc;
const windowFingerprintAlloc = sql_adapter.windowFingerprintAlloc;
const appendQueryAccessPathFingerprintAlloc = sql_adapter.appendQueryAccessPathFingerprintAlloc;
const appendSourceQueryAccessPathFingerprintAlloc = sql_adapter.appendSourceQueryAccessPathFingerprintAlloc;
const appendSourceQueryAccessOnlyFingerprintAlloc = sql_adapter.appendSourceQueryAccessOnlyFingerprintAlloc;
const appendSideQueryAccessOnlyFingerprintAlloc = sql_adapter.appendSideQueryAccessOnlyFingerprintAlloc;
const appendCteAccessPathFingerprintAlloc = sql_adapter.appendCteAccessPathFingerprintAlloc;

fn expectAppliedDdlCorpusPlan(
    alloc: std.mem.Allocator,
    base_schema_json: []const u8,
    entry: AppParityCorpusEntry,
    logical: sql_adapter.LogicalSqlPlan,
) !void {
    if (entry.applied_plan.len == 0) return;

    var current_schema_json: []const u8 = if (try sql_adapter.corpusDdlFixtureAppliesFromEmptyCatalog(entry)) "" else base_schema_json;
    var owned_current_schema_json: ?[]u8 = null;
    defer if (owned_current_schema_json) |schema_json| alloc.free(schema_json);

    if (entry.apply_setup_sql.len > 0) current_schema_json = "";
    for (entry.apply_setup_sql) |setup_sql| {
        var parsed_setup_sql = try sql_adapter.ParsedSql.initAlloc(alloc, setup_sql);
        defer parsed_setup_sql.deinit(alloc);
        var setup_plan = try sql_adapter.planDdlLogicalPlanParsedSqlWithFunctionBindingsAlloc(alloc, &parsed_setup_sql, .{});
        defer setup_plan.deinit(alloc);
        var setup_applied = try applyLogicalDdlPlanToSchemaJsonAlloc(alloc, current_schema_json, setup_plan);
        defer setup_applied.deinit(alloc);
        const next_schema_json = setup_applied.takeSchemaJson();
        if (owned_current_schema_json) |schema_json| alloc.free(schema_json);
        owned_current_schema_json = next_schema_json;
        current_schema_json = next_schema_json;
    }

    var applied = try applyLogicalDdlPlanToSchemaJsonAlloc(alloc, current_schema_json, logical);
    defer applied.deinit(alloc);
    const fingerprint = try ddlAppliedFingerprintAlloc(alloc, applied);
    defer alloc.free(fingerprint);
    try expectAppParityPlan(entry.applied_plan, fingerprint);
}

fn expectDdlExecutionCorpusPlan(
    alloc: std.mem.Allocator,
    entry: AppParityCorpusEntry,
    logical: sql_adapter.LogicalSqlPlan,
) !void {
    if (entry.execution_plan.len == 0) return;
    if (std.mem.startsWith(u8, entry.execution_plan, "unsupported:")) {
        switch (logical) {
            .bulk_io => |plan| try std.testing.expectError(error.UnsupportedSqlShape, bulkSqlIoExecutionPlanFromDdlPlan(plan)),
            else => return error.TestUnexpectedResult,
        }
        const fingerprint = try sql_adapter.unsupportedFingerprintAlloc(alloc, .ddl, .bulk_io_plan);
        defer alloc.free(fingerprint);
        try expectAppParityPlan(entry.execution_plan, fingerprint);
        return;
    }
    const fingerprint = switch (logical) {
        .bulk_io => |plan| blk: {
            const execution_plan = try bulkSqlIoExecutionPlanFromDdlPlan(plan);
            break :blk try bulkSqlIoExecutionFingerprintAlloc(alloc, execution_plan);
        },
        .transaction => |transaction| switch (transaction) {
            .prepared => |plan| blk: {
                const intent = preparedTransactionRecoveryIntentFromPlan(plan);
                break :blk try preparedTransactionRecoveryFingerprintAlloc(alloc, intent);
            },
            else => return error.TestUnexpectedResult,
        },
        else => return error.TestUnexpectedResult,
    };
    defer alloc.free(fingerprint);
    try expectAppParityPlan(entry.execution_plan, fingerprint);
}

fn schemaJsonFromSetupSqlAlloc(
    alloc: std.mem.Allocator,
    setup_sql: []const []const u8,
) ![]u8 {
    var current_schema_json: []const u8 = "";
    var owned_current_schema_json: ?[]u8 = null;
    errdefer if (owned_current_schema_json) |schema_json| alloc.free(schema_json);

    for (setup_sql) |sql| {
        var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
        defer parsed_sql.deinit(alloc);
        var setup_plan = try sql_adapter.planDdlLogicalPlanParsedSqlWithFunctionBindingsAlloc(alloc, &parsed_sql, .{});
        defer setup_plan.deinit(alloc);
        var setup_applied = try applyLogicalDdlPlanToSchemaJsonAlloc(alloc, current_schema_json, setup_plan);
        defer setup_applied.deinit(alloc);
        const next_schema_json = setup_applied.takeSchemaJson();
        if (owned_current_schema_json) |schema_json| alloc.free(schema_json);
        owned_current_schema_json = next_schema_json;
        current_schema_json = next_schema_json;
    }

    return owned_current_schema_json orelse try alloc.dupe(u8, current_schema_json);
}

const expectCombinedQuerySourceSummary = sql_adapter.expectCombinedQuerySourceSummary;

fn expectDdlSummary(summary: AppParityPlanSummary, logical: sql_adapter.LogicalSqlPlan) !void {
    switch (logical) {
        .table_ddl => |plan| return try expectTableDdlSummary(summary, plan),
        .catalog_ddl => |plan| return try expectCatalogDdlSummary(summary, plan),
        .other_ddl => return error.TestUnexpectedResult,
        .session => |plan| return try expectDdlSummaryPayload(summary, .{ .session_catalog = plan }),
        .transaction => |plan| return try expectTransactionDdlSummary(summary, plan),
        .prepared_statement => |plan| return try expectDdlSummaryPayload(summary, .{ .prepared_statement = plan }),
        .cursor => |plan| return try expectDdlSummaryPayload(summary, .{ .cursor_portal = plan }),
        .notification => |plan| return try expectDdlSummaryPayload(summary, .{ .notification_channel = plan }),
        .routine => |plan| return try expectRoutineDdlSummary(summary, plan),
        .auth => |plan| return try expectAuthDdlSummary(summary, plan),
        .extension => |plan| return try expectDdlSummaryPayload(summary, .{ .extension_catalog = plan }),
        .maintenance => |plan| return try expectDdlSummaryPayload(summary, .{ .maintenance_job = plan }),
        .bulk_io => |plan| return try expectDdlSummaryPayload(summary, .{ .bulk_io = plan }),
        .read, .write, .catalog_read, .catalog_write => return error.TestUnexpectedResult,
    }
}

fn expectTableDdlSummary(summary: AppParityPlanSummary, plan: sql_adapter.TableDdlLogicalPlan) !void {
    switch (plan) {
        .moved => return error.TestUnexpectedResult,
        .create_table => |payload| return try expectDdlSummaryPayload(summary, .{ .create_table = payload }),
        .table_clone => |payload| return try expectDdlSummaryPayload(summary, .{ .table_clone = payload }),
        .view_catalog => |payload| return try expectDdlSummaryPayload(summary, .{ .view_catalog = payload }),
        .materialized_view_catalog => |payload| return try expectDdlSummaryPayload(summary, .{ .materialized_view_catalog = payload }),
        .relation_lifetime => |payload| return try expectDdlSummaryPayload(summary, .{ .relation_lifetime = payload }),
        .table_partition_catalog => |payload| return try expectDdlSummaryPayload(summary, .{ .table_partition_catalog = payload }),
        .create_index => |payload| return try expectDdlSummaryPayload(summary, .{ .create_index = payload }),
        .drop_index => |payload| return try expectDdlSummaryPayload(summary, .{ .drop_index = payload }),
        .drop_table => |payload| return try expectDdlSummaryPayload(summary, .{ .drop_table = payload }),
        .alter_table => |payload| return try expectDdlSummaryPayload(summary, .{ .alter_table = payload }),
        .create_update_policy => |payload| return try expectDdlSummaryPayload(summary, .{ .create_update_policy = payload }),
    }
}

fn expectCatalogDdlSummary(summary: AppParityPlanSummary, plan: sql_adapter.CatalogDdlLogicalPlan) !void {
    switch (plan) {
        .moved => return error.TestUnexpectedResult,
        .enum_type_catalog => |payload| return try expectDdlSummaryPayload(summary, .{ .enum_type_catalog = payload }),
        .domain_catalog => |payload| return try expectDdlSummaryPayload(summary, .{ .domain_catalog = payload }),
        .sequence_catalog => |payload| return try expectDdlSummaryPayload(summary, .{ .sequence_catalog = payload }),
        .identity_allocator_catalog => |payload| return try expectDdlSummaryPayload(summary, .{ .identity_allocator_catalog = payload }),
        .schema_namespace_catalog => |payload| return try expectDdlSummaryPayload(summary, .{ .schema_namespace_catalog = payload }),
        .database_catalog => |payload| return try expectDdlSummaryPayload(summary, .{ .database_catalog = payload }),
        .tablespace_catalog => |payload| return try expectDdlSummaryPayload(summary, .{ .tablespace_catalog = payload }),
        .logical_replication => |payload| return try expectDdlSummaryPayload(summary, .{ .logical_replication = payload }),
        .type_system_catalog => |payload| return try expectDdlSummaryPayload(summary, .{ .type_system_catalog = payload }),
        .comment_metadata => |payload| return try expectDdlSummaryPayload(summary, .{ .comment_metadata = payload }),
    }
}

fn expectTransactionDdlSummary(summary: AppParityPlanSummary, plan: sql_adapter.TransactionLogicalPlan) !void {
    switch (plan) {
        .control => |payload| return try expectDdlSummaryPayload(summary, .{ .transaction_control = payload }),
        .prepared => |payload| return try expectDdlSummaryPayload(summary, .{ .prepared_transaction = payload }),
        .savepoint => |payload| return try expectDdlSummaryPayload(summary, .{ .savepoint_transaction = payload }),
    }
}

fn expectRoutineDdlSummary(summary: AppParityPlanSummary, plan: sql_adapter.RoutineLogicalPlan) !void {
    switch (plan) {
        .function_catalog => |payload| return try expectDdlSummaryPayload(summary, .{ .function_catalog = payload }),
        .trigger_catalog => |payload| return try expectDdlSummaryPayload(summary, .{ .trigger_catalog = payload }),
        .procedure_call => |payload| return try expectDdlSummaryPayload(summary, .{ .procedure_call = payload }),
    }
}

fn expectAuthDdlSummary(summary: AppParityPlanSummary, plan: sql_adapter.AuthorizationLogicalPlan) !void {
    switch (plan) {
        .authorization_catalog => |payload| return try expectDdlSummaryPayload(summary, .{ .authorization_catalog = payload }),
        .row_security_catalog => |payload| return try expectDdlSummaryPayload(summary, .{ .row_security_catalog = payload }),
    }
}

fn expectDdlSummaryPayload(summary: AppParityPlanSummary, payload: AppParityDdlSummaryPlan) !void {
    const expected = summary.ddl_tag orelse return;
    switch (payload) {
        .adapter_noop => return error.TestUnexpectedResult,
        .session_catalog => |plan| switch (plan) {
            .set_search_path => try std.testing.expectEqual(AppParityDdlTag.set_search_path, expected),
            .set_setting => try std.testing.expectEqual(AppParityDdlTag.set_setting, expected),
            .reset_search_path => try std.testing.expectEqual(AppParityDdlTag.reset_search_path, expected),
            .reset_setting => try std.testing.expectEqual(AppParityDdlTag.reset_setting, expected),
            .show_search_path => try std.testing.expectEqual(AppParityDdlTag.show_search_path, expected),
            .discard_all => try std.testing.expectEqual(AppParityDdlTag.discard_all, expected),
        },
        .create_table => |plan| {
            try std.testing.expectEqual(AppParityDdlTag.create_table, expected);
            try expectOptionalTableName(summary.table_name, plan.table_name);
            try expectOptionalUsize(summary.select, plan.columns.len);
            try expectOptionalUsize(summary.operations, plan.unique_constraints.len + plan.foreign_keys.len + plan.checks.len);
        },
        .table_clone => |plan| {
            try std.testing.expectEqual(AppParityDdlTag.table_clone, expected);
            try expectOptionalTableName(summary.table_name, plan.table_name);
        },
        .view_catalog => |plan| switch (plan) {
            .create => |create| {
                try std.testing.expectEqual(AppParityDdlTag.create_view, expected);
                try expectOptionalTableName(summary.table_name, create.view_name);
                try expectOptionalUsize(summary.select, create.output_fields.len);
            },
            .rename => |rename| {
                try std.testing.expectEqual(AppParityDdlTag.rename_view, expected);
                try expectOptionalTableName(summary.table_name, rename.view_name);
            },
            .drop => |drop| {
                try std.testing.expectEqual(AppParityDdlTag.drop_view, expected);
                try expectOptionalTableName(summary.table_name, drop.view_name);
            },
        },
        .materialized_view_catalog => |plan| switch (plan) {
            .create => |create| {
                try std.testing.expectEqual(AppParityDdlTag.create_materialized_view, expected);
                try expectOptionalTableName(summary.table_name, create.view_name);
                try expectOptionalUsize(summary.select, create.output_fields.len);
            },
            .refresh => |refresh| {
                try std.testing.expectEqual(AppParityDdlTag.refresh_materialized_view, expected);
                try expectOptionalTableName(summary.table_name, refresh.view_name);
            },
            .drop => |drop| {
                try std.testing.expectEqual(AppParityDdlTag.drop_materialized_view, expected);
                try expectOptionalTableName(summary.table_name, drop.view_name);
            },
        },
        .relation_lifetime => |plan| {
            try std.testing.expectEqual(AppParityDdlTag.relation_lifetime, expected);
            try expectOptionalTableName(summary.table_name, plan.create_table.table_name);
            try expectOptionalUsize(summary.select, plan.create_table.columns.len);
            try expectOptionalUsize(summary.operations, plan.create_table.unique_constraints.len + plan.create_table.foreign_keys.len + plan.create_table.checks.len);
        },
        .enum_type_catalog => |plan| switch (plan) {
            .create => |create| {
                try std.testing.expectEqual(AppParityDdlTag.create_enum_type, expected);
                try expectOptionalTableName(summary.table_name, create.type_name);
                try expectOptionalUsize(summary.select, create.values.len);
            },
            .add_value => |add| {
                try std.testing.expectEqual(AppParityDdlTag.add_enum_value, expected);
                try expectOptionalTableName(summary.table_name, add.type_name);
            },
            .drop => |drop| {
                try std.testing.expectEqual(AppParityDdlTag.drop_enum_type, expected);
                try expectOptionalTableName(summary.table_name, drop.type_name);
            },
        },
        .domain_catalog => |plan| switch (plan) {
            .create => |create| {
                try std.testing.expectEqual(AppParityDdlTag.create_domain, expected);
                try expectOptionalTableName(summary.table_name, create.domain_name);
                try expectOptionalUsize(summary.predicates, create.checks.len);
            },
            .alter => |alter| {
                try std.testing.expectEqual(AppParityDdlTag.alter_domain, expected);
                try expectOptionalTableName(summary.table_name, alter.domain_name);
                try expectOptionalUsize(summary.operations, alter.operations.len);
            },
            .drop => |drop| {
                try std.testing.expectEqual(AppParityDdlTag.drop_domain, expected);
                try expectOptionalTableName(summary.table_name, drop.domain_name);
            },
        },
        .sequence_catalog => |plan| switch (plan) {
            .create => |create| {
                try std.testing.expectEqual(AppParityDdlTag.create_sequence, expected);
                try expectOptionalTableName(summary.table_name, create.sequence_name);
                try expectOptionalUsize(summary.operations, sequenceOptionCount(create.options));
            },
            .alter => |alter| {
                try std.testing.expectEqual(AppParityDdlTag.alter_sequence, expected);
                try expectOptionalTableName(summary.table_name, alter.sequence_name);
                try expectOptionalUsize(summary.operations, alter.operations.len);
            },
            .drop => |drop| {
                try std.testing.expectEqual(AppParityDdlTag.drop_sequence, expected);
                try expectOptionalTableName(summary.table_name, drop.sequence_name);
            },
        },
        .identity_allocator_catalog => |plan| {
            try std.testing.expectEqual(AppParityDdlTag.identity_allocator, expected);
            try expectOptionalTableName(summary.table_name, plan.table_name);
            try expectOptionalUsize(summary.select, 1 + plan.additional_columns.len);
            try expectOptionalUsize(summary.operations, @intFromBool(plan.primary_key));
        },
        .schema_namespace_catalog => |plan| switch (plan) {
            .create => |create| {
                try std.testing.expectEqual(AppParityDdlTag.create_schema_namespace, expected);
                try expectOptionalTableName(summary.table_name, create.schema_name);
            },
            .rename => |rename| {
                try std.testing.expectEqual(AppParityDdlTag.rename_schema_namespace, expected);
                try expectOptionalTableName(summary.table_name, rename.schema_name);
            },
            .drop => |drop| {
                try std.testing.expectEqual(AppParityDdlTag.drop_schema_namespace, expected);
                try expectOptionalTableName(summary.table_name, drop.schema_name);
            },
        },
        .extension_catalog => |plan| switch (plan) {
            .create => |create| {
                try std.testing.expectEqual(AppParityDdlTag.create_extension, expected);
                try expectOptionalTableName(summary.table_name, create.extension_name);
            },
            .update => |update| {
                try std.testing.expectEqual(AppParityDdlTag.alter_extension_update, expected);
                try expectOptionalTableName(summary.table_name, update.extension_name);
            },
            .drop => |drop| {
                try std.testing.expectEqual(AppParityDdlTag.drop_extension, expected);
                try expectOptionalTableName(summary.table_name, drop.extension_name);
            },
        },
        .function_catalog => |plan| switch (plan) {
            .create => |create| {
                try std.testing.expectEqual(switch (create.kind) {
                    .function => AppParityDdlTag.create_function,
                    .procedure => AppParityDdlTag.create_procedure,
                }, expected);
                try expectOptionalTableName(summary.table_name, create.routine_name);
                try expectOptionalUsize(summary.operations, create.argument_count);
            },
            .drop => |drop| {
                try std.testing.expectEqual(switch (drop.kind) {
                    .function => AppParityDdlTag.drop_function,
                    .procedure => AppParityDdlTag.drop_procedure,
                }, expected);
                try expectOptionalTableName(summary.table_name, drop.routine_name);
                try expectOptionalUsize(summary.operations, drop.argument_count);
            },
        },
        .procedure_call => |call| {
            try std.testing.expectEqual(AppParityDdlTag.call_procedure, expected);
            try expectOptionalTableName(summary.table_name, call.routine_name);
            try expectOptionalUsize(summary.operations, call.argument_count);
        },
        .authorization_catalog => |plan| switch (plan) {
            .create_role => |create| {
                try std.testing.expectEqual(AppParityDdlTag.create_role, expected);
                try expectOptionalTableName(summary.table_name, create.role_name);
            },
            .alter_role => |alter| {
                try std.testing.expectEqual(AppParityDdlTag.alter_role, expected);
                try expectOptionalTableName(summary.table_name, alter.role_name);
                try expectOptionalUsize(summary.operations, 1);
            },
            .drop_role => |drop| {
                try std.testing.expectEqual(AppParityDdlTag.drop_role, expected);
                try expectOptionalTableName(summary.table_name, drop.role_name);
            },
            .grant_privilege => |grant| {
                try std.testing.expectEqual(AppParityDdlTag.grant_privilege, expected);
                try expectOptionalTableName(summary.table_name, grant.object_name);
                try expectOptionalUsize(summary.operations, grant.privileges.len);
            },
            .revoke_privilege => |revoke| {
                try std.testing.expectEqual(AppParityDdlTag.revoke_privilege, expected);
                try expectOptionalTableName(summary.table_name, revoke.object_name);
                try expectOptionalUsize(summary.operations, revoke.privileges.len);
            },
        },
        .bulk_io => |plan| {
            try std.testing.expectEqual(switch (plan.direction) {
                .from => AppParityDdlTag.copy_from,
                .to => AppParityDdlTag.copy_to,
            }, expected);
            try expectOptionalTableName(summary.table_name, plan.table_name);
            try expectOptionalUsize(summary.operations, plan.columns.len);
        },
        .table_partition_catalog => |plan| switch (plan) {
            .create_partitioned => |create| {
                try std.testing.expectEqual(AppParityDdlTag.create_partitioned_table, expected);
                try expectOptionalTableName(summary.table_name, create.create_table.table_name);
                try expectOptionalUsize(summary.select, create.create_table.columns.len);
                try expectOptionalUsize(summary.operations, create.keys.len);
            },
            .create_partition => |create| {
                try std.testing.expectEqual(AppParityDdlTag.create_table_partition, expected);
                try expectOptionalTableName(summary.table_name, create.table_name);
            },
            .attach => |attach| {
                try std.testing.expectEqual(AppParityDdlTag.attach_table_partition, expected);
                try expectOptionalTableName(summary.table_name, attach.parent_table_name);
            },
            .detach => |detach| {
                try std.testing.expectEqual(AppParityDdlTag.detach_table_partition, expected);
                try expectOptionalTableName(summary.table_name, detach.parent_table_name);
            },
        },
        .row_security_catalog => |plan| switch (plan) {
            .alter_table => |alter| {
                try std.testing.expectEqual(if (alter.enabled) AppParityDdlTag.enable_row_security else AppParityDdlTag.disable_row_security, expected);
                try expectOptionalTableName(summary.table_name, alter.table_name);
                try expectOptionalUsize(summary.operations, 1);
            },
            .create_policy => |create| {
                try std.testing.expectEqual(AppParityDdlTag.create_row_policy, expected);
                try expectOptionalTableName(summary.table_name, create.table_name);
                try expectOptionalUsize(summary.operations, 1);
            },
            .alter_policy => |alter| {
                try std.testing.expectEqual(AppParityDdlTag.alter_row_policy, expected);
                try expectOptionalTableName(summary.table_name, alter.table_name);
                try expectOptionalUsize(summary.operations, 1);
            },
            .drop_policy => |drop| {
                try std.testing.expectEqual(AppParityDdlTag.drop_row_policy, expected);
                try expectOptionalTableName(summary.table_name, drop.table_name);
                try expectOptionalUsize(summary.operations, 1);
            },
        },
        .database_catalog => |plan| switch (plan) {
            .create => |create| {
                try std.testing.expectEqual(AppParityDdlTag.create_database, expected);
                try expectOptionalTableName(summary.table_name, create.database_name);
            },
            .alter => |alter| {
                try std.testing.expectEqual(AppParityDdlTag.alter_database, expected);
                try expectOptionalTableName(summary.table_name, alter.database_name);
                try expectOptionalUsize(summary.operations, alter.operations.len);
            },
            .drop => |drop| {
                try std.testing.expectEqual(AppParityDdlTag.drop_database, expected);
                try expectOptionalTableName(summary.table_name, drop.database_name);
            },
        },
        .tablespace_catalog => |plan| switch (plan) {
            .create => |create| {
                try std.testing.expectEqual(AppParityDdlTag.create_tablespace, expected);
                try expectOptionalTableName(summary.table_name, create.tablespace_name);
            },
            .rename => |rename| {
                try std.testing.expectEqual(AppParityDdlTag.rename_tablespace, expected);
                try expectOptionalTableName(summary.table_name, rename.tablespace_name);
            },
            .drop => |drop| {
                try std.testing.expectEqual(AppParityDdlTag.drop_tablespace, expected);
                try expectOptionalTableName(summary.table_name, drop.tablespace_name);
            },
        },
        .notification_channel => |plan| switch (plan) {
            .listen => |listen| {
                try std.testing.expectEqual(AppParityDdlTag.listen_notification, expected);
                try expectOptionalTableName(summary.table_name, listen.channel_name);
            },
            .notify => |notify| {
                try std.testing.expectEqual(AppParityDdlTag.notify_notification, expected);
                try expectOptionalTableName(summary.table_name, notify.channel_name);
                if (summary.operations) |expected_operations| {
                    try std.testing.expectEqual(expected_operations, if (notify.payload_json != null) @as(usize, 1) else @as(usize, 0));
                }
            },
            .unlisten => |unlisten| {
                try std.testing.expectEqual(AppParityDdlTag.unlisten_notification, expected);
                if (unlisten.channel_name) |channel_name| {
                    try expectOptionalTableName(summary.table_name, channel_name);
                }
            },
        },
        .logical_replication => |plan| switch (plan) {
            .publication => |publication| switch (publication) {
                .create => |create| {
                    try std.testing.expectEqual(AppParityDdlTag.create_publication, expected);
                    try expectOptionalTableName(summary.table_name, create.publication_name);
                    try expectOptionalUsize(summary.operations, create.table_names.len);
                },
                .alter => |alter| {
                    try std.testing.expectEqual(AppParityDdlTag.alter_publication, expected);
                    try expectOptionalTableName(summary.table_name, alter.publication_name);
                    switch (alter.operation) {
                        .add_tables => |tables| try expectOptionalUsize(summary.operations, tables.len),
                    }
                },
                .drop => |drop| {
                    try std.testing.expectEqual(AppParityDdlTag.drop_publication, expected);
                    try expectOptionalTableName(summary.table_name, drop.publication_name);
                },
            },
            .subscription => |subscription| switch (subscription) {
                .create => |create| {
                    try std.testing.expectEqual(AppParityDdlTag.create_subscription, expected);
                    try expectOptionalTableName(summary.table_name, create.subscription_name);
                    try expectOptionalUsize(summary.operations, create.publication_names.len);
                },
                .alter => |alter| {
                    try std.testing.expectEqual(AppParityDdlTag.alter_subscription, expected);
                    try expectOptionalTableName(summary.table_name, alter.subscription_name);
                },
                .drop => |drop| {
                    try std.testing.expectEqual(AppParityDdlTag.drop_subscription, expected);
                    try expectOptionalTableName(summary.table_name, drop.subscription_name);
                },
            },
        },
        .type_system_catalog => |plan| switch (plan) {
            .collation => |collation| switch (collation) {
                .create => |create| {
                    try std.testing.expectEqual(AppParityDdlTag.create_collation, expected);
                    try expectOptionalTableName(summary.table_name, create.collation_name);
                    try expectOptionalUsize(summary.operations, create.option_count);
                },
                .rename => |rename| {
                    try std.testing.expectEqual(AppParityDdlTag.rename_collation, expected);
                    try expectOptionalTableName(summary.table_name, rename.collation_name);
                },
                .drop => |drop| {
                    try std.testing.expectEqual(AppParityDdlTag.drop_collation, expected);
                    try expectOptionalTableName(summary.table_name, drop.collation_name);
                },
            },
            .operator => |operator| switch (operator) {
                .create => |create| {
                    try std.testing.expectEqual(AppParityDdlTag.create_operator, expected);
                    try expectOptionalTableName(summary.table_name, create.operator_name);
                    try expectOptionalUsize(summary.operations, create.option_count);
                },
                .drop => |drop| {
                    try std.testing.expectEqual(AppParityDdlTag.drop_operator, expected);
                    try expectOptionalTableName(summary.table_name, drop.operator_name);
                    try expectOptionalUsize(summary.operations, drop.argument_count);
                },
            },
            .aggregate => |aggregate| switch (aggregate) {
                .create => |create| {
                    try std.testing.expectEqual(AppParityDdlTag.create_aggregate, expected);
                    try expectOptionalTableName(summary.table_name, create.aggregate_name);
                    try expectOptionalUsize(summary.operations, create.option_count);
                    try expectOptionalUsize(summary.select, create.argument_count);
                },
                .drop => |drop| {
                    try std.testing.expectEqual(AppParityDdlTag.drop_aggregate, expected);
                    try expectOptionalTableName(summary.table_name, drop.aggregate_name);
                    try expectOptionalUsize(summary.operations, drop.argument_count);
                },
            },
            .cast => |cast| switch (cast) {
                .create => |create| {
                    try std.testing.expectEqual(AppParityDdlTag.create_cast, expected);
                    try expectOptionalTableName(summary.table_name, create.source_type);
                },
                .drop => |drop| {
                    try std.testing.expectEqual(AppParityDdlTag.drop_cast, expected);
                    try expectOptionalTableName(summary.table_name, drop.source_type);
                },
            },
        },
        .maintenance_job => |plan| switch (plan) {
            .vacuum => |vacuum| {
                try std.testing.expectEqual(AppParityDdlTag.vacuum_maintenance, expected);
                try expectOptionalTableName(summary.table_name, vacuum.table_name);
                try expectOptionalUsize(summary.operations, @as(usize, @intFromBool(vacuum.full)) + @as(usize, @intFromBool(vacuum.freeze)) + @as(usize, @intFromBool(vacuum.verbose)) + @as(usize, @intFromBool(vacuum.analyze)));
            },
            .analyze => |analyze| {
                try std.testing.expectEqual(AppParityDdlTag.analyze_maintenance, expected);
                try expectOptionalTableName(summary.table_name, analyze.table_name);
                try expectOptionalUsize(summary.operations, analyze.column_count + @as(usize, @intFromBool(analyze.verbose)));
            },
            .reindex => |reindex| {
                try std.testing.expectEqual(AppParityDdlTag.reindex_maintenance, expected);
                try expectOptionalTableName(summary.table_name, reindex.name);
                try expectOptionalUsize(summary.operations, @intFromBool(reindex.concurrently));
            },
            .cluster => |cluster| {
                try std.testing.expectEqual(AppParityDdlTag.cluster_maintenance, expected);
                try expectOptionalTableName(summary.table_name, cluster.table_name);
                try expectOptionalUsize(summary.operations, if (cluster.index_name != null) @as(usize, 1) else @as(usize, 0));
            },
        },
        .prepared_statement => |plan| switch (plan) {
            .prepare => |prepare| {
                try std.testing.expectEqual(AppParityDdlTag.prepare_statement, expected);
                try expectOptionalTableName(summary.table_name, prepare.statement_name);
                try expectOptionalUsize(summary.operations, prepare.parameter_count);
            },
            .execute => |execute| {
                try std.testing.expectEqual(AppParityDdlTag.execute_statement, expected);
                try expectOptionalTableName(summary.table_name, execute.statement_name);
                try expectOptionalUsize(summary.operations, execute.argument_count);
            },
            .deallocate => |deallocate| {
                try std.testing.expectEqual(AppParityDdlTag.deallocate_statement, expected);
                if (deallocate.statement_name) |name| {
                    try expectOptionalTableName(summary.table_name, name);
                }
            },
        },
        .prepared_transaction => |plan| {
            try std.testing.expectEqual(switch (plan.action) {
                .prepare => AppParityDdlTag.prepare_transaction,
                .commit => AppParityDdlTag.commit_prepared,
                .rollback => AppParityDdlTag.rollback_prepared,
            }, expected);
            try expectOptionalTableName(summary.table_name, plan.gid);
        },
        .cursor_portal => |plan| switch (plan) {
            .declare => |declare| {
                try std.testing.expectEqual(AppParityDdlTag.declare_cursor, expected);
                try expectOptionalTableName(summary.table_name, declare.portal_name);
            },
            .fetch => |fetch| {
                try std.testing.expectEqual(AppParityDdlTag.fetch_cursor, expected);
                try expectOptionalTableName(summary.table_name, fetch.portal_name);
                if (fetch.count) |count| try expectOptionalUsize(summary.operations, @intCast(count));
            },
            .move => |move| {
                try std.testing.expectEqual(AppParityDdlTag.fetch_cursor, expected);
                try expectOptionalTableName(summary.table_name, move.portal_name);
                if (move.count) |count| try expectOptionalUsize(summary.operations, @intCast(count));
            },
            .close => |close| {
                try std.testing.expectEqual(AppParityDdlTag.close_cursor, expected);
                if (close.portal_name) |portal| {
                    try expectOptionalTableName(summary.table_name, portal);
                }
            },
        },
        .savepoint_transaction => |plan| switch (plan) {
            .savepoint => |savepoint| {
                try std.testing.expectEqual(AppParityDdlTag.savepoint_transaction, expected);
                try expectOptionalTableName(summary.table_name, savepoint.savepoint_name);
            },
            .release => |release| {
                try std.testing.expectEqual(AppParityDdlTag.release_savepoint, expected);
                try expectOptionalTableName(summary.table_name, release.savepoint_name);
            },
            .rollback_to => |rollback| {
                try std.testing.expectEqual(AppParityDdlTag.rollback_to_savepoint, expected);
                try expectOptionalTableName(summary.table_name, rollback.savepoint_name);
            },
        },
        .comment_metadata => |plan| {
            try std.testing.expectEqual(AppParityDdlTag.comment_metadata, expected);
            try expectOptionalTableName(summary.table_name, plan.object_name);
            if (summary.operations) |expected_operations| {
                try std.testing.expectEqual(expected_operations, @intFromBool(plan.comment_json != null));
            }
        },
        .transaction_control => |plan| switch (plan) {
            .table_lock => |lock| {
                try std.testing.expectEqual(AppParityDdlTag.table_lock, expected);
                if (summary.table_name) |table_name| try std.testing.expectEqualStrings(table_name, lock.table_names[0]);
                try expectOptionalUsize(summary.operations, lock.table_names.len);
            },
            .constraint_mode => |constraints| {
                try std.testing.expectEqual(AppParityDdlTag.constraint_mode, expected);
                try expectOptionalUsize(summary.operations, constraints.constraint_names.len + @as(usize, @intFromBool(constraints.all)));
            },
            .transaction_mode => |transaction| {
                try std.testing.expectEqual(AppParityDdlTag.transaction_mode, expected);
                const option_count: usize = @as(usize, @intFromBool(transaction.isolation_level != null)) +
                    @as(usize, @intFromBool(transaction.access_mode != null)) +
                    @as(usize, @intFromBool(transaction.deferrable != null));
                try expectOptionalUsize(summary.operations, option_count);
            },
            .advisory_lock => |lock| {
                try std.testing.expectEqual(AppParityDdlTag.advisory_lock, expected);
                try expectOptionalUsize(summary.operations, if (lock.key2 != null) @as(usize, 2) else @as(usize, 1));
            },
        },
        .create_index => |plan| {
            try std.testing.expectEqual(AppParityDdlTag.create_index, expected);
            try expectOptionalTableName(summary.table_name, plan.table_name);
            try expectOptionalUsize(summary.select, plan.columns.len + plan.expressions.len + createIndexPlanGeneratedExpressionCount(plan));
            try expectOptionalUsize(summary.predicates, plan.where.len);
        },
        .drop_index => |plan| {
            try std.testing.expectEqual(AppParityDdlTag.drop_index, expected);
            _ = plan;
        },
        .drop_table => |plan| {
            try std.testing.expectEqual(AppParityDdlTag.drop_table, expected);
            try expectOptionalTableName(summary.table_name, plan.table_name);
        },
        .alter_table => |plan| {
            try std.testing.expectEqual(AppParityDdlTag.alter_table, expected);
            try expectOptionalTableName(summary.table_name, plan.table_name);
            try expectOptionalUsize(summary.operations, plan.operations.len);
        },
        .create_update_policy => |plan| {
            try std.testing.expectEqual(AppParityDdlTag.create_update_policy, expected);
            try expectOptionalTableName(summary.table_name, plan.table_name);
        },
    }
}

const insertSourceFingerprintAlloc = sql_adapter.insertSourceFingerprintAlloc;
const recursiveInsertSourceFingerprintAlloc = sql_adapter.recursiveInsertSourceFingerprintAlloc;
const mergeMutationFingerprintAlloc = sql_adapter.mergeMutationFingerprintAlloc;
const recursiveMergeMutationFingerprintAlloc = sql_adapter.recursiveMergeMutationFingerprintAlloc;
const updateSourceFingerprintAlloc = sql_adapter.updateSourceFingerprintAlloc;
const deleteSourceFingerprintAlloc = sql_adapter.deleteSourceFingerprintAlloc;
const truncateSourceFingerprintAlloc = sql_adapter.truncateSourceFingerprintAlloc;
const joinedSourceFingerprintAlloc = sql_adapter.joinedSourceFingerprintAlloc;
const recursiveJoinedSourceFingerprintAlloc = sql_adapter.recursiveJoinedSourceFingerprintAlloc;
const relationPopulationFingerprintAlloc = sql_adapter.relationPopulationFingerprintAlloc;
const writePlanFingerprintAlloc = sql_adapter.writePlanFingerprintAlloc;
const explainPlanFingerprintAlloc = sql_adapter.explainPlanFingerprintAlloc;
const invalidPlanFingerprintAlloc = sql_adapter.invalidPlanFingerprintAlloc;

fn lowerAppParityReadPlanParsedSqlAlloc(
    alloc: std.mem.Allocator,
    effective_schema: runtime_schema.TableSchema,
    entry: AppParityCorpusEntry,
    parsed_sql: *const sql_adapter.ParsedSql,
) !LoweredReadPlan {
    var catalog_opt = try sql_adapter.appParityCatalogForEntryParsedSqlAlloc(alloc, entry, parsed_sql);
    if (catalog_opt) |*catalog| {
        defer catalog.deinit(alloc);
        return try lowerReadPlanWithCatalogAndFunctionBindingsParsedSqlAlloc(
            alloc,
            parsed_sql,
            effective_schema,
            entry.params,
            catalog.iface(),
            .{},
        );
    }
    return try lowerReadPlanWithFunctionBindingsParsedSqlAlloc(alloc, parsed_sql, effective_schema, entry.params, .{});
}

fn lowerAppParityExplainPlanParsedSqlAlloc(
    alloc: std.mem.Allocator,
    effective_schema: runtime_schema.TableSchema,
    entry: AppParityCorpusEntry,
    parsed_sql: *const sql_adapter.ParsedSql,
    unique_resolver: relational_rows.UniqueSelectorResolver,
    row_claim: db_mod.types.RowClaimRequest,
) !LoweredExplainPlan {
    var catalog_opt = try sql_adapter.appParityCatalogForEntryParsedSqlAlloc(alloc, entry, parsed_sql);
    if (catalog_opt) |*catalog| {
        defer catalog.deinit(alloc);
        return try lowerExplainPlanWithOptionsCatalogAndFunctionBindingsParsedSqlAlloc(
            alloc,
            parsed_sql,
            effective_schema,
            entry.params,
            .{
                .unique_resolver = unique_resolver,
                .row_claim = row_claim,
            },
            catalog.iface(),
            .{},
        );
    }
    return try lowerExplainPlanWithOptionsCatalogAndFunctionBindingsParsedSqlAlloc(
        alloc,
        parsed_sql,
        effective_schema,
        entry.params,
        .{
            .unique_resolver = unique_resolver,
            .row_claim = row_claim,
        },
        null,
        .{},
    );
}

fn expectAppParityReadPlanEntry(
    alloc: std.mem.Allocator,
    effective_schema: runtime_schema.TableSchema,
    entry: AppParityCorpusEntry,
    parsed_sql: *const sql_adapter.ParsedSql,
) !void {
    var lowered = try lowerAppParityReadPlanParsedSqlAlloc(alloc, effective_schema, entry, parsed_sql);
    defer lowered.deinit(alloc);
    try expectAppParityReadSummary(entry.summary, lowered);

    switch (entry.family) {
        .query => switch (lowered) {
            .query => |query| {
                var fingerprint = try queryFingerprintAlloc(alloc, "query", query.table_name, query.plan.query, query.plan.ctes.len);
                fingerprint = try appendNonZeroU32FingerprintAlloc(alloc, fingerprint, "offset", query.plan.query.offset);
                fingerprint = try appendTrueBoolFingerprintAlloc(alloc, fingerprint, "select_all", query.plan.query.select_all);
                fingerprint = try appendQueryAccessPathFingerprintAlloc(alloc, fingerprint, query.plan.query);
                fingerprint = try appendCteAccessPathFingerprintAlloc(alloc, fingerprint, query.plan.ctes);
                defer alloc.free(fingerprint);
                try expectAppParityPlan(entry.plan, fingerprint);
            },
            else => return error.TestUnexpectedResult,
        },
        .read => {
            const fingerprint = try readPlanFingerprintAlloc(alloc, lowered);
            defer alloc.free(fingerprint);
            try expectAppParityPlan(entry.plan, fingerprint);
        },
        .aggregate => switch (lowered) {
            .aggregate => |aggregate| {
                var fingerprint = try aggregatePlanFingerprintAlloc(alloc, aggregate);
                fingerprint = try appendNonZeroU32FingerprintAlloc(alloc, fingerprint, "offset", aggregate.plan.aggregate.offset);
                defer alloc.free(fingerprint);
                try expectAppParityPlan(entry.plan, fingerprint);
            },
            else => return error.TestUnexpectedResult,
        },
        .join => switch (lowered) {
            .join => |join| {
                var fingerprint = try joinFingerprintAlloc(alloc, join);
                fingerprint = try appendNonZeroU32FingerprintAlloc(alloc, fingerprint, "offset", join.join.offset);
                defer alloc.free(fingerprint);
                try expectAppParityPlan(entry.plan, fingerprint);
            },
            else => return error.TestUnexpectedResult,
        },
        .lateral => switch (lowered) {
            .lateral => |lateral| {
                var fingerprint = try lateralFingerprintAlloc(alloc, lateral);
                fingerprint = try appendNonZeroU32FingerprintAlloc(alloc, fingerprint, "right_offset", lateral.plan.lateral.right.offset);
                fingerprint = try appendNonZeroU32FingerprintAlloc(alloc, fingerprint, "offset", lateral.plan.lateral.offset);
                defer alloc.free(fingerprint);
                try expectAppParityPlan(entry.plan, fingerprint);
            },
            else => return error.TestUnexpectedResult,
        },
        .window => switch (lowered) {
            .window => |window| {
                const fingerprint = try windowFingerprintAlloc(alloc, window);
                defer alloc.free(fingerprint);
                try expectAppParityPlan(entry.plan, fingerprint);
            },
            else => return error.TestUnexpectedResult,
        },
        else => return error.TestUnexpectedResult,
    }
}

const expectAppParityReadSummary = sql_adapter.expectAppParityReadSummary;
const expectAppParityWriteSummary = sql_adapter.expectAppParityWriteSummary;
const expectAppParityExplainSummary = sql_adapter.expectAppParityExplainSummary;

fn appParityPlanFamilyIsSupportedRead(family: AppParityCorpusPlanFamily) bool {
    return switch (family) {
        .read,
        .query,
        .aggregate,
        .join,
        .lateral,
        .window,
        => true,
        else => false,
    };
}

fn lowerAppParityWritePlanParsedSqlAlloc(
    alloc: std.mem.Allocator,
    effective_schema: runtime_schema.TableSchema,
    entry: AppParityCorpusEntry,
    parsed_sql: *const sql_adapter.ParsedSql,
    unique_resolver: relational_rows.UniqueSelectorResolver,
    row_claim: db_mod.types.RowClaimRequest,
) !LoweredWritePlan {
    var catalog_opt = try sql_adapter.appParityCatalogForEntryParsedSqlAlloc(alloc, entry, parsed_sql);
    if (catalog_opt) |*catalog| {
        defer catalog.deinit(alloc);
        return try lowerWritePlanWithCatalogParsedSqlAlloc(alloc, parsed_sql, effective_schema, entry.params, .{
            .unique_resolver = unique_resolver,
            .row_claim = row_claim,
        }, catalog.iface());
    }
    return try lowerWritePlanParsedSqlAlloc(alloc, parsed_sql, effective_schema, entry.params, .{
        .unique_resolver = unique_resolver,
        .row_claim = row_claim,
    });
}

fn expectAppParityWritePlanEntry(
    alloc: std.mem.Allocator,
    effective_schema: runtime_schema.TableSchema,
    entry: AppParityCorpusEntry,
    parsed_sql: *const sql_adapter.ParsedSql,
    unique_resolver: relational_rows.UniqueSelectorResolver,
    row_claim: db_mod.types.RowClaimRequest,
) !void {
    var lowered = try lowerAppParityWritePlanParsedSqlAlloc(alloc, effective_schema, entry, parsed_sql, unique_resolver, row_claim);
    defer lowered.deinit(alloc);
    try expectAppParityWriteSummary(entry.summary, lowered);

    switch (entry.family) {
        .insert => switch (lowered) {
            .insert => |insert| {
                const fingerprint = try writePlanFingerprintAlloc(alloc, lowered);
                defer alloc.free(fingerprint);
                try expectAppParityPlan(entry.plan, fingerprint);
                try expectAppParityReturningRows(entry.returning_rows, insert.batch.returning_rows);
            },
            else => return error.TestUnexpectedResult,
        },
        .insert_source => switch (lowered) {
            .insert_source => |insert_source| {
                var fingerprint = try insertSourceFingerprintAlloc(alloc, insert_source);
                fingerprint = try appendNonZeroU32FingerprintAlloc(alloc, fingerprint, "source_offset", insert_source.insert_source.req.source.offset);
                fingerprint = try appendTrueBoolFingerprintAlloc(alloc, fingerprint, "conflict_where", insert_source.conflict_where);
                defer alloc.free(fingerprint);
                try expectAppParityPlan(entry.plan, fingerprint);
            },
            else => return error.TestUnexpectedResult,
        },
        .recursive_insert_source => switch (lowered) {
            .recursive_insert_source => |recursive_insert_source| {
                const fingerprint = try recursiveInsertSourceFingerprintAlloc(alloc, recursive_insert_source);
                defer alloc.free(fingerprint);
                try expectAppParityPlan(entry.plan, fingerprint);
            },
            else => return error.TestUnexpectedResult,
        },
        .update => switch (lowered) {
            .update => |update| {
                const fingerprint = try writePlanFingerprintAlloc(alloc, lowered);
                defer alloc.free(fingerprint);
                try expectAppParityPlan(entry.plan, fingerprint);
                try expectAppParityReturningRows(entry.returning_rows, update.batch.returning_rows);
            },
            else => return error.TestUnexpectedResult,
        },
        .delete => switch (lowered) {
            .delete => |delete| {
                var fingerprint = try std.fmt.allocPrint(
                    alloc,
                    "delete:table={s}:deletes={d}:returning_rows={d}:returning_expr={d}",
                    .{ delete.table_name, delete.batch.deletes.len, delete.batch.returning_rows.len, delete.returning_expression_count },
                );
                fingerprint = try appendTrueBoolFingerprintAlloc(alloc, fingerprint, "returning_all", delete.returning_all);
                defer alloc.free(fingerprint);
                try expectAppParityPlan(entry.plan, fingerprint);
                try expectAppParityReturningRows(entry.returning_rows, delete.batch.returning_rows);
            },
            else => return error.TestUnexpectedResult,
        },
        .update_source => switch (lowered) {
            .update_source => |update_source| {
                var fingerprint = try updateSourceFingerprintAlloc(alloc, update_source);
                fingerprint = try appendNonZeroU32FingerprintAlloc(alloc, fingerprint, "source_offset", update_source.mutation.req.source.offset);
                defer alloc.free(fingerprint);
                try expectAppParityPlan(entry.plan, fingerprint);
            },
            else => return error.TestUnexpectedResult,
        },
        .delete_source => switch (lowered) {
            .delete_source => |delete_source| {
                var fingerprint = try deleteSourceFingerprintAlloc(alloc, delete_source);
                fingerprint = try appendNonZeroU32FingerprintAlloc(alloc, fingerprint, "source_offset", delete_source.mutation.req.source.offset);
                defer alloc.free(fingerprint);
                try expectAppParityPlan(entry.plan, fingerprint);
            },
            else => return error.TestUnexpectedResult,
        },
        .truncate_source => switch (lowered) {
            .truncate_source => |truncate_source| {
                var fingerprint = try truncateSourceFingerprintAlloc(alloc, truncate_source);
                fingerprint = try appendNonZeroU32FingerprintAlloc(alloc, fingerprint, "source_offset", truncate_source.mutation.req.source.offset);
                defer alloc.free(fingerprint);
                try expectAppParityPlan(entry.plan, fingerprint);
            },
            else => return error.TestUnexpectedResult,
        },
        .update_joined_source => switch (lowered) {
            .update_joined_source => |update_joined_source| {
                const fingerprint = try joinedSourceFingerprintAlloc(alloc, "update_joined_source", update_joined_source);
                defer alloc.free(fingerprint);
                try expectAppParityPlan(entry.plan, fingerprint);
            },
            .recursive_update_joined_source => |recursive_update_joined_source| {
                const fingerprint = try recursiveJoinedSourceFingerprintAlloc(alloc, "update_joined_source", recursive_update_joined_source);
                defer alloc.free(fingerprint);
                try expectAppParityPlan(entry.plan, fingerprint);
            },
            else => return error.TestUnexpectedResult,
        },
        .delete_joined_source => switch (lowered) {
            .delete_joined_source => |delete_joined_source| {
                const fingerprint = try joinedSourceFingerprintAlloc(alloc, "delete_joined_source", delete_joined_source);
                defer alloc.free(fingerprint);
                try expectAppParityPlan(entry.plan, fingerprint);
            },
            .recursive_delete_joined_source => |recursive_delete_joined_source| {
                const fingerprint = try recursiveJoinedSourceFingerprintAlloc(alloc, "delete_joined_source", recursive_delete_joined_source);
                defer alloc.free(fingerprint);
                try expectAppParityPlan(entry.plan, fingerprint);
            },
            else => return error.TestUnexpectedResult,
        },
        .merge_mutation => switch (lowered) {
            .merge_mutation => {
                const fingerprint = try writePlanFingerprintAlloc(alloc, lowered);
                defer alloc.free(fingerprint);
                try expectAppParityPlan(entry.plan, fingerprint);
            },
            .recursive_merge_mutation => |recursive_merge_mutation| {
                const fingerprint = try recursiveMergeMutationFingerprintAlloc(alloc, recursive_merge_mutation);
                defer alloc.free(fingerprint);
                try expectAppParityPlan(entry.plan, fingerprint);
            },
            else => return error.TestUnexpectedResult,
        },
        else => return error.TestUnexpectedResult,
    }
}

fn appParityPlanFamilyIsSupportedWrite(family: AppParityCorpusPlanFamily) bool {
    return switch (family) {
        .insert,
        .insert_source,
        .recursive_insert_source,
        .update,
        .delete,
        .update_source,
        .delete_source,
        .truncate_source,
        .update_joined_source,
        .delete_joined_source,
        .merge_mutation,
        => true,
        else => false,
    };
}

fn expectAppParityUnsupportedPlanEntry(
    alloc: std.mem.Allocator,
    effective_schema: runtime_schema.TableSchema,
    entry: AppParityCorpusEntry,
    parsed_sql: *const sql_adapter.ParsedSql,
    unique_resolver: relational_rows.UniqueSelectorResolver,
    row_claim: db_mod.types.RowClaimRequest,
) !void {
    switch (entry.family) {
        .unsupported => try expectFailClosedUnsupported(lowerQueryPlanWithFunctionBindingsParsedSqlAlloc(alloc, parsed_sql, effective_schema, entry.params, .{})),
        .unsupported_read => try expectFailClosedUnsupported(lowerReadPlanWithFunctionBindingsParsedSqlAlloc(alloc, parsed_sql, effective_schema, entry.params, .{})),
        .unsupported_ddl => try expectFailClosedUnsupported(sql_adapter.planDdlLogicalPlanParsedSqlWithFunctionBindingsAlloc(alloc, parsed_sql, .{})),
        .unsupported_write => try expectFailClosedUnsupported(lowerAppParityWritePlanParsedSqlAlloc(alloc, effective_schema, entry, parsed_sql, unique_resolver, row_claim)),
        .unsupported_insert => try expectFailClosedUnsupported(lowerInsertWithResolverParsedSqlAlloc(alloc, parsed_sql, effective_schema, entry.params, unique_resolver)),
        .unsupported_update => try expectFailClosedUnsupported(lowerUpdateParsedSqlAlloc(alloc, parsed_sql, effective_schema, entry.params, unique_resolver)),
        .unsupported_update_source => try expectFailClosedUnsupported(lowerUpdateMutationSourceParsedSqlAlloc(alloc, parsed_sql, effective_schema, entry.params, row_claim)),
        .unsupported_delete => try expectFailClosedUnsupported(lowerDeleteParsedSqlAlloc(alloc, parsed_sql, effective_schema, entry.params, unique_resolver)),
        .unsupported_update_joined_source => try expectFailClosedUnsupported(lowerUpdateJoinedMutationSourceWithSchemasParsedSqlAlloc(alloc, parsed_sql, effective_schema, effective_schema, entry.params, row_claim)),
        .unsupported_delete_joined_source => try expectFailClosedUnsupported(lowerDeleteJoinedMutationSourceWithSchemasParsedSqlAlloc(alloc, parsed_sql, effective_schema, effective_schema, entry.params, row_claim)),
        .unsupported_merge_mutation => try expectFailClosedUnsupported(lowerMergeMutationPlanParsedSqlAlloc(alloc, parsed_sql, effective_schema, effective_schema, entry.params)),
        else => return error.TestUnexpectedResult,
    }

    const fingerprint_family = sql_adapter.corpusUnsupportedPlanFamily(entry.family) orelse return error.TestUnexpectedResult;
    const diagnostic_reason = sql_adapter.classificationReasonFromToken(entry.classification_reason) orelse return error.TestUnexpectedResult;
    const fingerprint = sql_adapter.unsupportedFingerprintAlloc(alloc, fingerprint_family, diagnostic_reason) catch |err| switch (err) {
        error.UnsupportedSqlShape => return error.TestUnexpectedResult,
        else => return err,
    };
    defer alloc.free(fingerprint);
    try expectAppParityPlan(entry.plan, fingerprint);
}

fn expectAppParityInvalidPlanEntry(
    alloc: std.mem.Allocator,
    effective_schema: runtime_schema.TableSchema,
    entry: AppParityCorpusEntry,
    parsed_sql: *const sql_adapter.ParsedSql,
    unique_resolver: relational_rows.UniqueSelectorResolver,
    row_claim: db_mod.types.RowClaimRequest,
) !void {
    switch (entry.family) {
        .invalid_read => try expectTypedInvalid(lowerReadPlanWithFunctionBindingsParsedSqlAlloc(alloc, parsed_sql, effective_schema, entry.params, .{})),
        .invalid_insert => try expectTypedInvalid(lowerInsertWithResolverStrictParsedSqlAlloc(alloc, parsed_sql, effective_schema, entry.params, unique_resolver)),
        .invalid_update => try expectTypedInvalid(lowerUpdateStrictParsedSqlAlloc(alloc, parsed_sql, effective_schema, entry.params, unique_resolver)),
        .invalid_delete => try expectTypedInvalid(lowerDeleteParsedSqlAlloc(alloc, parsed_sql, effective_schema, entry.params, unique_resolver)),
        .invalid_update_source => try expectTypedInvalid(lowerUpdateMutationSourceParsedSqlAlloc(alloc, parsed_sql, effective_schema, entry.params, row_claim)),
        .invalid_update_joined_source => try expectTypedInvalid(lowerUpdateJoinedMutationSourceWithSchemasParsedSqlAlloc(alloc, parsed_sql, effective_schema, effective_schema, entry.params, row_claim)),
        else => return error.TestUnexpectedResult,
    }

    const diagnostic_reason = sql_adapter.classificationReasonFromToken(entry.classification_reason) orelse return error.TestUnexpectedResult;
    const invalid_family = switch (entry.family) {
        .invalid_read => "read",
        .invalid_insert => "insert",
        .invalid_update => "update",
        .invalid_delete => "delete",
        .invalid_update_source => "update_source",
        .invalid_update_joined_source => "update_joined_source",
        else => return error.TestUnexpectedResult,
    };
    const fingerprint = try invalidPlanFingerprintAlloc(alloc, invalid_family, diagnostic_reason);
    defer alloc.free(fingerprint);
    try expectAppParityPlan(entry.plan, fingerprint);
}

fn expectAppParityCorpusEntry(
    alloc: std.mem.Allocator,
    base_schema_json: []const u8,
    schema: runtime_schema.TableSchema,
    entry: AppParityCorpusEntry,
    unique_resolver: relational_rows.UniqueSelectorResolver,
    row_claim: db_mod.types.RowClaimRequest,
) !void {
    var effective_schema = schema;
    var owned_setup_schema: ?runtime_schema.TableSchema = null;
    defer if (owned_setup_schema) |value| runtime_schema.freeSchema(alloc, value);
    if (entry.family != .ddl and entry.apply_setup_sql.len > 0) {
        const setup_schema_json = try schemaJsonFromSetupSqlAlloc(alloc, entry.apply_setup_sql);
        defer alloc.free(setup_schema_json);
        var parsed_setup_schema = try schema_api.parseValidatedTableSchema(alloc, setup_schema_json);
        defer parsed_setup_schema.deinit(alloc);
        owned_setup_schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed_setup_schema);
        effective_schema = owned_setup_schema.?;
    }
    var override_resolver_ctx = TestPrimaryResolver{
        .row_json = entry.resolver_row_json,
        .version = entry.resolver_version,
        .exists = entry.resolver_exists orelse true,
    };
    const effective_unique_resolver = if (entry.resolver_row_json.len > 0 or entry.resolver_exists != null)
        override_resolver_ctx.resolver()
    else
        unique_resolver;

    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, entry.sql);
    defer parsed_sql.deinit(alloc);

    if (appParityPlanFamilyIsSupportedRead(entry.family)) {
        return try expectAppParityReadPlanEntry(alloc, effective_schema, entry, &parsed_sql);
    }
    if (appParityPlanFamilyIsSupportedWrite(entry.family)) {
        return try expectAppParityWritePlanEntry(alloc, effective_schema, entry, &parsed_sql, effective_unique_resolver, row_claim);
    }
    if (sql_adapter.corpusPlanFamilyIsUnsupported(entry.family)) {
        return try expectAppParityUnsupportedPlanEntry(alloc, effective_schema, entry, &parsed_sql, effective_unique_resolver, row_claim);
    }
    if (sql_adapter.corpusPlanFamilyIsInvalid(entry.family)) {
        return try expectAppParityInvalidPlanEntry(alloc, effective_schema, entry, &parsed_sql, effective_unique_resolver, row_claim);
    }
    if (entry.family == .query_function) {
        return try sql_adapter.expectAppParityQueryFunctionParsedSqlEntry(alloc, entry, &parsed_sql);
    }

    switch (entry.family) {
        .ddl => {
            var logical = try sql_adapter.planDdlLogicalPlanParsedSqlWithFunctionBindingsAlloc(alloc, &parsed_sql, .{});
            defer logical.deinit(alloc);
            try expectDdlSummary(entry.summary, logical);
            const fingerprint = try ddlFingerprintAlloc(alloc, logical);
            defer alloc.free(fingerprint);
            try expectAppParityPlan(entry.plan, fingerprint);
            try expectAppliedDdlCorpusPlan(alloc, base_schema_json, entry, logical);
            try expectDdlExecutionCorpusPlan(alloc, entry, logical);
        },
        .query_function => return error.TestUnexpectedResult,
        .read => {
            var lowered = try lowerAppParityReadPlanParsedSqlAlloc(alloc, effective_schema, entry, &parsed_sql);
            defer lowered.deinit(alloc);
            try expectAppParityReadSummary(entry.summary, lowered);
            const fingerprint = try readPlanFingerprintAlloc(alloc, lowered);
            defer alloc.free(fingerprint);
            try expectAppParityPlan(entry.plan, fingerprint);
        },
        .query,
        .aggregate,
        .join,
        .lateral,
        .window,
        => return error.TestUnexpectedResult,
        .explain => {
            var lowered = try lowerAppParityExplainPlanParsedSqlAlloc(
                alloc,
                effective_schema,
                entry,
                &parsed_sql,
                effective_unique_resolver,
                row_claim,
            );
            defer lowered.deinit(alloc);
            try expectAppParityExplainSummary(entry.summary, lowered);
            const fingerprint = try explainPlanFingerprintAlloc(alloc, lowered);
            defer alloc.free(fingerprint);
            try expectAppParityPlan(entry.plan, fingerprint);
        },
        .relation_population => {
            var lowered = try lowerRelationPopulationPlanWithCatalogAndFunctionBindingsParsedSqlAlloc(alloc, &parsed_sql, effective_schema, entry.params, null, .{});
            defer lowered.deinit(alloc);
            try expectOptionalTableName(entry.summary.table_name, lowered.target_table_name);
            const fingerprint = try relationPopulationFingerprintAlloc(alloc, lowered);
            defer alloc.free(fingerprint);
            try expectAppParityPlan(entry.plan, fingerprint);
        },
        .insert,
        .insert_source,
        .recursive_insert_source,
        .update,
        .delete,
        .update_source,
        .delete_source,
        .truncate_source,
        .update_joined_source,
        .delete_joined_source,
        .merge_mutation,
        => return error.TestUnexpectedResult,
        .adapter_noop_ddl => {
            if (sql_adapter.planDdlLogicalPlanParsedSqlWithFunctionBindingsAlloc(alloc, &parsed_sql, .{})) |logical_value| {
                var logical = logical_value;
                defer logical.deinit(alloc);
                switch (logical) {
                    .other_ddl => |plan| switch (plan) {
                        .adapter_noop => |noop| try std.testing.expectEqualStrings(entry.classification_reason, @tagName(noop.reason)),
                        else => return error.TestUnexpectedResult,
                    },
                    else => return error.TestUnexpectedResult,
                }
                const fingerprint = try adapterNoopFingerprintAlloc(alloc, "ddl", entry.classification_reason);
                defer alloc.free(fingerprint);
                try expectAppParityPlan(entry.plan, fingerprint);
            } else |err| {
                switch (err) {
                    error.UnsupportedSqlShape, error.InvalidSqlCatalog => {},
                    else => return err,
                }
                const fingerprint = try adapterNoopFingerprintAlloc(alloc, "ddl", entry.classification_reason);
                defer alloc.free(fingerprint);
                try expectAppParityPlan(entry.plan, fingerprint);
            }
        },
        .invalid_read,
        .invalid_insert,
        .invalid_update,
        .invalid_delete,
        .invalid_update_source,
        .invalid_update_joined_source,
        => return error.TestUnexpectedResult,
        .unsupported,
        .unsupported_read,
        .unsupported_ddl,
        .unsupported_write,
        .unsupported_insert,
        .unsupported_update,
        .unsupported_update_source,
        .unsupported_delete,
        .unsupported_update_joined_source,
        .unsupported_delete_joined_source,
        .unsupported_merge_mutation,
        => return error.TestUnexpectedResult,
    }
}

fn appParityAppliedDdlPlanAlloc(
    alloc: std.mem.Allocator,
    base_schema_json: []const u8,
    entry: AppParityCorpusEntry,
    parsed_sql: *const sql_adapter.ParsedSql,
) ![]u8 {
    var current_schema_json: []const u8 = if (try sql_adapter.corpusDdlFixtureAppliesFromEmptyCatalog(entry)) "" else base_schema_json;
    var owned_current_schema_json: ?[]u8 = null;
    defer if (owned_current_schema_json) |schema_json| alloc.free(schema_json);

    if (entry.apply_setup_sql.len > 0) current_schema_json = "";
    for (entry.apply_setup_sql) |setup_sql| {
        var parsed_setup_sql = try sql_adapter.ParsedSql.initAlloc(alloc, setup_sql);
        defer parsed_setup_sql.deinit(alloc);
        var setup_plan = try sql_adapter.planDdlLogicalPlanParsedSqlWithFunctionBindingsAlloc(alloc, &parsed_setup_sql, .{});
        defer setup_plan.deinit(alloc);
        var setup_applied = try applyLogicalDdlPlanToSchemaJsonAlloc(alloc, current_schema_json, setup_plan);
        defer setup_applied.deinit(alloc);
        const next_schema_json = setup_applied.takeSchemaJson();
        if (owned_current_schema_json) |schema_json| alloc.free(schema_json);
        owned_current_schema_json = next_schema_json;
        current_schema_json = next_schema_json;
    }

    var logical = try sql_adapter.planDdlLogicalPlanParsedSqlWithFunctionBindingsAlloc(alloc, parsed_sql, .{});
    defer logical.deinit(alloc);
    var applied = try applyLogicalDdlPlanToSchemaJsonAlloc(alloc, current_schema_json, logical);
    defer applied.deinit(alloc);
    return try ddlAppliedFingerprintAlloc(alloc, applied);
}

const app_parity_fixture_generation_callbacks = sql_adapter.AppParityFixtureGenerationCallbacks{
    .applied_ddl_plan = appParityAppliedDdlPlanAlloc,
};

fn maybeCheckOrPromoteAppParityFixture(
    alloc: std.mem.Allocator,
    schema_json: []const u8,
    source_sha256: []const u8,
    corpus: []const AppParityCorpusEntry,
) !void {
    return sql_adapter.maybeCheckOrPromoteAppParityFixture(
        alloc,
        schema_json,
        source_sha256,
        corpus,
        app_parity_fixture_generation_callbacks,
    );
}

const app_parity_fixture_metadata_callbacks = sql_adapter.AppParityFixtureMetadataCallbacks{
    .schema_json_from_setup_sql = schemaJsonFromSetupSqlAlloc,
    .applied_ddl_plan = appParityAppliedDdlPlanAlloc,
};

fn validateAppParityFixtureMetadataWithBaseSchema(
    entry: AppParityCorpusEntry,
    base_schema_json: []const u8,
    seen_names: *std.StringHashMapUnmanaged(void),
    alloc: std.mem.Allocator,
) !void {
    return sql_adapter.validateAppParityFixtureMetadataWithBaseSchema(
        alloc,
        entry,
        base_schema_json,
        seen_names,
        app_parity_fixture_metadata_callbacks,
    );
}

fn validateAppParityFixtureMetadata(
    entry: AppParityCorpusEntry,
    seen_names: *std.StringHashMapUnmanaged(void),
    alloc: std.mem.Allocator,
) !void {
    return sql_adapter.validateAppParityFixtureMetadata(
        alloc,
        entry,
        seen_names,
        app_parity_fixture_metadata_callbacks,
    );
}

test "postgres sql adapter validates app parity fixture metadata with applied schema context" {
    const alloc = std.testing.allocator;
    var seen = std.StringHashMapUnmanaged(void){};
    defer seen.deinit(alloc);

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "missing query table",
        .sql = "SELECT id FROM usage_records",
        .family = .query,
        .plan = "query:table=usage_records",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "missing ddl tag",
        .sql = "CREATE TABLE usage_records (id text PRIMARY KEY)",
        .family = .ddl,
        .plan = "ddl:create_table:table=usage_records",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "family plan mismatch",
        .sql = "SELECT id FROM usage_records",
        .family = .query,
        .summary = .{ .table_name = "usage_records" },
        .plan = "read:query:query:table=usage_records",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "non ddl ddl tag",
        .sql = "SELECT id FROM usage_records",
        .family = .query,
        .summary = .{ .ddl_tag = .create_table, .table_name = "usage_records" },
        .plan = "query:table=usage_records:pred=0:array_any=0:in=0:json_path_eq=0:json_contains=0:json_exists=0:array_contains=0:array_eq=0:text_patterns=0:expr_pred=0:expr_or=0:expr_not=0:select=1:order=0:limit=none",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "stale ddl select summary",
        .sql = "CREATE UNIQUE INDEX usage_records_expr_idx ON usage_records (status, lower(id))",
        .family = .ddl,
        .summary = .{ .ddl_tag = .create_index, .table_name = "usage_records", .select = 2 },
        .plan = "ddl:create_index:table=usage_records:columns=1:expr=0:generated_expr=0:where=0:unique=true:if_not_exists=false",
        .applied_plan = "applied:rebuild=true:validation=true:rewrite=false:building_indexes=0:unvalidated_unique=1:unvalidated_fk=0:unvalidated_check=0:update_policy=0:work_items=2:work=rebuild/table/derived_artifacts,validate/table/constraints",
    }, &seen, alloc));

    try validateAppParityFixtureMetadata(.{
        .name = "valid ddl select summary",
        .sql = "CREATE UNIQUE INDEX usage_records_expr_idx ON usage_records (status, lower(id))",
        .family = .ddl,
        .summary = .{ .ddl_tag = .create_index, .table_name = "usage_records", .select = 2 },
        .plan = "ddl:create_index:table=usage_records:columns=1:expr=1:generated_expr=0:where=0:unique=true:if_not_exists=false",
        .apply_setup_sql = &.{"CREATE TABLE usage_records (id text PRIMARY KEY, status text);"},
        .applied_plan = "applied:rebuild=true:validation=true:rewrite=false:building_indexes=0:unvalidated_unique=1:unvalidated_fk=0:unvalidated_check=0:update_policy=0:work_items=2:work=rebuild/table/derived_artifacts,validate/table/constraints",
    }, &seen, alloc);

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "stale ddl predicate summary",
        .sql = "CREATE DOMAIN positive_amount AS numeric CHECK (VALUE > 0)",
        .family = .ddl,
        .summary = .{ .ddl_tag = .create_domain, .table_name = "positive_amount", .predicates = 1 },
        .plan = "ddl:create_domain:domain=positive_amount:type=numeric:checks=0:not_null=false:default=false",
    }, &seen, alloc));

    try validateAppParityFixtureMetadata(.{
        .name = "valid ddl predicate summary",
        .sql = "CREATE DOMAIN positive_amount AS numeric CHECK (VALUE > 0)",
        .family = .ddl,
        .summary = .{ .ddl_tag = .create_domain, .table_name = "positive_amount", .predicates = 1 },
        .plan = "ddl:create_domain:domain=positive_amount:type=numeric:checks=1:not_null=false:default=false",
    }, &seen, alloc);

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "stale ddl operations summary",
        .sql = "CREATE TABLE usage_records (id text PRIMARY KEY, status text UNIQUE, source_id text REFERENCES sources(id), CHECK (status <> ''))",
        .family = .ddl,
        .summary = .{ .ddl_tag = .create_table, .table_name = "usage_records", .select = 2, .operations = 3 },
        .plan = "ddl:create_table:table=usage_records:columns=2:unique=1:fk=1:checks=0:if_not_exists=false:pk=1",
        .applied_plan = "applied:rebuild=false:validation=false:rewrite=false:building_indexes=0:unvalidated_unique=0:unvalidated_fk=0:unvalidated_check=0:update_policy=0:work_items=0:work=none",
    }, &seen, alloc));

    try validateAppParityFixtureMetadata(.{
        .name = "valid ddl operations summary",
        .sql = "CREATE TABLE usage_records (id text PRIMARY KEY, status text UNIQUE, source_id text REFERENCES sources(id), CHECK (status <> ''))",
        .family = .ddl,
        .summary = .{ .ddl_tag = .create_table, .table_name = "usage_records", .select = 2, .operations = 3 },
        .plan = "ddl:create_table:table=usage_records:columns=2:unique=1:fk=1:checks=1:if_not_exists=false:pk=1",
        .applied_plan = "applied:rebuild=false:validation=false:rewrite=false:building_indexes=0:unvalidated_unique=0:unvalidated_fk=0:unvalidated_check=0:update_policy=0:work_items=0:work=none",
    }, &seen, alloc);

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "stale ddl transaction operations summary",
        .sql = "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY",
        .family = .ddl,
        .summary = .{ .ddl_tag = .transaction_mode, .operations = 2 },
        .plan = "ddl:transaction_control:kind=transaction_mode:starter=set_transaction:isolation=serializable:access=none:deferrable=none",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "duplicate ddl transaction operations summary",
        .sql = "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY",
        .family = .ddl,
        .summary = .{ .ddl_tag = .transaction_mode, .operations = 2 },
        .plan = "ddl:transaction_control:kind=transaction_mode:starter=set_transaction:isolation=serializable:access=none:access=read_only:deferrable=none",
    }, &seen, alloc));

    try validateAppParityFixtureMetadata(.{
        .name = "valid ddl transaction operations summary",
        .sql = "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY",
        .family = .ddl,
        .summary = .{ .ddl_tag = .transaction_mode, .operations = 2 },
        .plan = "ddl:transaction_control:kind=transaction_mode:starter=set_transaction:isolation=serializable:access=read_only:deferrable=none",
    }, &seen, alloc);

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "ignored operations summary",
        .sql = "SELECT lower(valid_at) AS valid_start, upper(valid_at) AS valid_end FROM price_intervals",
        .family = .query,
        .summary = .{ .table_name = "price_intervals", .operations = 2 },
        .plan = "query:table=price_intervals:pred=0:array_any=0:in=0:json_path_eq=0:json_contains=0:json_exists=0:array_contains=0:array_eq=0:text_patterns=0:expr_pred=0:expr_or=0:expr_not=0:select=0:order=0:limit=none",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "stale returning all false summary",
        .sql = "INSERT INTO usage_records (id, status) VALUES ('u1', 'active') RETURNING *",
        .family = .insert,
        .summary = .{ .table_name = "usage_records", .returning_all = false },
        .plan = "insert:table=usage_records:writes=1:transforms=0:ops=0:deletes=0:returning_rows=0:returning_expr=0:returning_all=1",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "malformed returning all false summary",
        .sql = "INSERT INTO usage_records (id, status) VALUES ('u1', 'active')",
        .family = .insert,
        .summary = .{ .table_name = "usage_records", .returning_all = false },
        .plan = "insert:table=usage_records:writes=1:transforms=0:ops=0:deletes=0:returning_rows=0:returning_expr=0:returning_all=2",
    }, &seen, alloc));

    try validateAppParityFixtureMetadata(.{
        .name = "valid compact returning all false summary",
        .sql = "INSERT INTO usage_records (id, status) VALUES ('u1', 'active')",
        .family = .insert,
        .summary = .{ .table_name = "usage_records", .returning_all = false },
        .plan = "insert:table=usage_records:writes=1:transforms=0:ops=0:deletes=0:returning_rows=0:returning_expr=0",
    }, &seen, alloc);

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "stale conflict where true summary",
        .sql = "INSERT INTO usage_records (id, status) VALUES ('u1', 'active') ON CONFLICT (id) DO UPDATE SET status = excluded.status",
        .family = .insert,
        .summary = .{ .table_name = "usage_records", .conflict_where = true },
        .plan = "insert:table=usage_records:writes=1:transforms=0:ops=1:deletes=0:returning_rows=0:returning_expr=0",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "stale conflict where false summary",
        .sql = "INSERT INTO usage_records (id, status) VALUES ('u1', 'active') ON CONFLICT (id) WHERE status = 'active' DO UPDATE SET status = excluded.status",
        .family = .insert,
        .summary = .{ .table_name = "usage_records", .conflict_where = false },
        .plan = "insert:table=usage_records:writes=1:transforms=0:ops=1:deletes=0:returning_rows=0:returning_expr=0:conflict_where=1",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "malformed conflict where false summary",
        .sql = "INSERT INTO usage_records (id, status) VALUES ('u1', 'active') ON CONFLICT (id) DO UPDATE SET status = excluded.status",
        .family = .insert,
        .summary = .{ .table_name = "usage_records", .conflict_where = false },
        .plan = "insert:table=usage_records:writes=1:transforms=0:ops=1:deletes=0:returning_rows=0:returning_expr=0:conflict_where=2",
    }, &seen, alloc));

    try validateAppParityFixtureMetadata(.{
        .name = "valid compact conflict where false summary",
        .sql = "INSERT INTO usage_records (id, status) VALUES ('u1', 'active') ON CONFLICT (id) DO UPDATE SET status = excluded.status",
        .family = .insert,
        .summary = .{ .table_name = "usage_records", .conflict_where = false },
        .plan = "insert:table=usage_records:writes=1:transforms=0:ops=1:deletes=0:returning_rows=0:returning_expr=0",
    }, &seen, alloc);

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "point insert cte summary",
        .sql = "INSERT INTO usage_records (id, status) VALUES ('u1', 'active')",
        .family = .insert,
        .summary = .{ .table_name = "usage_records", .ctes = 1 },
        .plan = "insert:table=usage_records:writes=1:transforms=0:ops=0:deletes=0:returning_rows=0:returning_expr=0",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "point insert pagination summary",
        .sql = "INSERT INTO usage_records (id, status) VALUES ('u1', 'active')",
        .family = .insert,
        .summary = .{ .table_name = "usage_records", .order_by = 1 },
        .plan = "insert:table=usage_records:writes=1:transforms=0:ops=0:deletes=0:returning_rows=0:returning_expr=0",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "merge pagination summary",
        .sql = "MERGE INTO usage_records AS target USING source_records AS source ON target.id = source.id WHEN MATCHED THEN UPDATE SET status = source.status",
        .family = .merge_mutation,
        .summary = .{ .table_name = "usage_records", .join_on = 1, .operations = 1, .offset = 3 },
        .plan = "merge_mutation:target=usage_records:source=source_records:ctes=0:source_cte=0:match=1:matched_pred=0:matched_update=1:matched_delete=0:matched_noop=0:not_matched_pred=0:not_matched_insert=0:not_matched_noop=0:returning=0:returning_expr=0:returning_all=0",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "relation population source pagination summary",
        .sql = "CREATE TABLE usage_archive AS SELECT id, status FROM usage_records WHERE status = 'closed' ORDER BY id LIMIT 5",
        .family = .relation_population,
        .summary = .{ .table_name = "usage_archive", .limit = 5 },
        .plan = "relation_population:mode=create_table_as:target=usage_archive:lifetime=durable:if_not_exists=false:populate=true:source=read:query:query:table=usage_records:ctes=0:pred=1:expr_pred=0:json_eq=0:or=0:not=0:select=2:expr=0:alias=0:order=1:order_expr=0:limit=5:claim=none",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "relation population source projection summary",
        .sql = "CREATE TABLE usage_archive AS SELECT id, status FROM usage_records WHERE status = 'closed'",
        .family = .relation_population,
        .summary = .{ .table_name = "usage_archive", .select = 2 },
        .plan = "relation_population:mode=create_table_as:target=usage_archive:lifetime=durable:if_not_exists=false:populate=true:source=read:query:query:table=usage_records:ctes=0:pred=1:expr_pred=0:json_eq=0:or=0:not=0:select=2:expr=0:alias=0:order=0:order_expr=0:limit=none:claim=none",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "relation population source predicate summary",
        .sql = "SELECT id, status INTO usage_archive FROM usage_records WHERE status = 'closed'",
        .family = .relation_population,
        .summary = .{ .table_name = "usage_archive", .predicates = 1 },
        .plan = "relation_population:mode=select_into:target=usage_archive:lifetime=durable:if_not_exists=false:populate=true:source=read:query:query:table=usage_records:ctes=0:pred=1:expr_pred=0:json_eq=0:or=0:not=0:select=2:expr=0:alias=0:order=0:order_expr=0:limit=none:claim=none",
    }, &seen, alloc));

    try validateAppParityFixtureMetadata(.{
        .name = "valid relation population target summary",
        .sql = "CREATE TABLE usage_archive AS SELECT id, status FROM usage_records WHERE status = 'closed'",
        .family = .relation_population,
        .summary = .{ .table_name = "usage_archive" },
        .plan = "relation_population:mode=create_table_as:target=usage_archive:lifetime=durable:if_not_exists=false:populate=true:source=read:query:query:table=usage_records:ctes=0:pred=1:expr_pred=0:json_eq=0:or=0:not=0:select=2:expr=0:alias=0:order=0:order_expr=0:limit=none:claim=none",
    }, &seen, alloc);

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "stale query cte summary",
        .sql = "WITH active_usage AS (SELECT id FROM usage_records WHERE status = 'active') SELECT id FROM active_usage",
        .family = .query,
        .summary = .{ .table_name = "usage_records", .ctes = 1 },
        .plan = "query:table=usage_records:ctes=0:pred=0:expr_pred=0:json_eq=0:or=0:not=0:select=1:expr=0:alias=0:order=0:order_expr=0:limit=none:claim=none",
    }, &seen, alloc));

    try validateAppParityFixtureMetadata(.{
        .name = "valid query cte summary",
        .sql = "WITH active_usage AS (SELECT id FROM usage_records WHERE status = 'active') SELECT id FROM active_usage",
        .family = .query,
        .summary = .{ .table_name = "usage_records", .ctes = 1 },
        .plan = "query:table=usage_records:ctes=1:pred=0:expr_pred=0:json_eq=0:or=0:not=0:select=1:expr=0:alias=0:order=0:order_expr=0:limit=none:claim=none",
    }, &seen, alloc);

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "stale query select summary",
        .sql = "SELECT id FROM usage_records",
        .family = .query,
        .summary = .{ .table_name = "usage_records", .select = 1 },
        .plan = "query:table=usage_records:ctes=0:pred=0:expr_pred=0:json_eq=0:or=0:not=0:select=0:expr=0:alias=0:order=0:order_expr=0:limit=none:claim=none",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "malformed query select summary",
        .sql = "SELECT id FROM usage_records",
        .family = .query,
        .summary = .{ .table_name = "usage_records", .select = 1 },
        .plan = "query:table=usage_records:ctes=0:pred=0:expr_pred=0:json_eq=0:or=0:not=0:select=1x:expr=0:alias=0:order=0:order_expr=0:limit=none:claim=none",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "duplicate query select summary",
        .sql = "SELECT id FROM usage_records",
        .family = .query,
        .summary = .{ .table_name = "usage_records", .select = 1 },
        .plan = "query:table=usage_records:ctes=0:pred=0:expr_pred=0:json_eq=0:or=0:not=0:select=2:select=1:expr=0:alias=0:order=0:order_expr=0:limit=none:claim=none",
    }, &seen, alloc));

    try validateAppParityFixtureMetadata(.{
        .name = "valid query select summary",
        .sql = "SELECT id FROM usage_records",
        .family = .query,
        .summary = .{ .table_name = "usage_records", .select = 1 },
        .plan = "query:table=usage_records:ctes=0:pred=0:expr_pred=0:json_eq=0:or=0:not=0:select=1:expr=0:alias=0:order=0:order_expr=0:limit=none:claim=none",
    }, &seen, alloc);

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "stale query predicate summary",
        .sql = "SELECT id FROM usage_records WHERE status = 'active'",
        .family = .query,
        .summary = .{ .table_name = "usage_records", .predicates = 1, .select = 1 },
        .plan = "query:table=usage_records:ctes=0:pred=0:expr_pred=0:json_eq=0:or=0:not=0:select=1:expr=0:alias=0:order=0:order_expr=0:limit=none:claim=none",
    }, &seen, alloc));

    try validateAppParityFixtureMetadata(.{
        .name = "valid query predicate summary",
        .sql = "SELECT id FROM usage_records WHERE status = 'active'",
        .family = .query,
        .summary = .{ .table_name = "usage_records", .predicates = 1, .select = 1 },
        .plan = "query:table=usage_records:ctes=0:pred=1:expr_pred=0:json_eq=0:or=0:not=0:select=1:expr=0:alias=0:order=0:order_expr=0:limit=none:claim=none",
    }, &seen, alloc);

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "stale query expression predicate summary",
        .sql = "SELECT id FROM usage_records WHERE lower(status) = 'active'",
        .family = .query,
        .summary = .{ .table_name = "usage_records", .expression_predicates = 1, .select = 1 },
        .plan = "query:table=usage_records:ctes=0:pred=0:expr_pred=0:json_eq=0:or=0:not=0:select=1:expr=0:alias=0:order=0:order_expr=0:limit=none:claim=none",
    }, &seen, alloc));

    try validateAppParityFixtureMetadata(.{
        .name = "valid query expression predicate summary",
        .sql = "SELECT id FROM usage_records WHERE lower(status) = 'active'",
        .family = .query,
        .summary = .{ .table_name = "usage_records", .expression_predicates = 1, .select = 1 },
        .plan = "query:table=usage_records:ctes=0:pred=0:expr_pred=1:json_eq=0:or=0:not=0:select=1:expr=0:alias=0:order=0:order_expr=0:limit=none:claim=none",
    }, &seen, alloc);

    try validateAppParityFixtureMetadata(.{
        .name = "valid mutation source pagination summary",
        .sql = "UPDATE usage_records SET status = 'archived' WHERE status = 'closed' ORDER BY id LIMIT 5 OFFSET 2",
        .family = .update_source,
        .summary = .{ .table_name = "usage_records", .predicates = 1, .order_by = 1, .limit = 5, .offset = 2, .operations = 1 },
        .plan = "update_source:table=usage_records:source_pred=1:source_order=1:source_limit=5:claim=locked:ops=1:returning=0:returning_expr=0:returning_all=0:source_offset=2",
    }, &seen, alloc);

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "stale query pagination summary",
        .sql = "SELECT id FROM usage_records ORDER BY id LIMIT 5",
        .family = .query,
        .summary = .{ .table_name = "usage_records", .select = 1, .order_by = 1, .limit = 5 },
        .plan = "query:table=usage_records:ctes=0:pred=0:expr_pred=0:json_eq=0:or=0:not=0:select=1:expr=0:alias=0:order=0:order_expr=0:limit=5:claim=none",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "stale source pagination summary",
        .sql = "UPDATE usage_records SET status = 'archived' WHERE status = 'closed' ORDER BY id LIMIT 5 OFFSET 2",
        .family = .update_source,
        .summary = .{ .table_name = "usage_records", .predicates = 1, .order_by = 1, .limit = 5, .offset = 2, .operations = 1 },
        .plan = "update_source:table=usage_records:source_pred=1:source_order=1:source_limit=5:claim=locked:ops=1:returning=0:returning_expr=0:returning_all=0:source_offset=1",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "stale source predicate summary",
        .sql = "UPDATE usage_records SET status = 'archived' WHERE status = 'closed'",
        .family = .update_source,
        .summary = .{ .table_name = "usage_records", .predicates = 1, .operations = 1 },
        .plan = "update_source:table=usage_records:source_pred=0:source_order=0:source_limit=-1:claim=locked:ops=1:returning=0:returning_expr=0:returning_all=0",
    }, &seen, alloc));

    try validateAppParityFixtureMetadata(.{
        .name = "valid source predicate summary",
        .sql = "UPDATE usage_records SET status = 'archived' WHERE status = 'closed'",
        .family = .update_source,
        .summary = .{ .table_name = "usage_records", .predicates = 1, .operations = 1 },
        .plan = "update_source:table=usage_records:source_pred=1:source_order=0:source_limit=-1:claim=locked:ops=1:returning=0:returning_expr=0:returning_all=0",
    }, &seen, alloc);

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "stale source expression-or summary",
        .sql = "UPDATE usage_records SET status = 'archived' WHERE lower(status) = 'closed' OR lower(status) = 'old'",
        .family = .update_source,
        .summary = .{ .table_name = "usage_records", .expression_or_predicates = 2, .operations = 1 },
        .plan = "update_source:table=usage_records:source_pred=0:source_array_any=0:source_expr_pred=0:source_expr_or=1:source_expr_not=0:source_expr_array=0:source_order=0:source_limit=-1:claim=locked:ops=1:returning=0:returning_expr=0:returning_all=0",
    }, &seen, alloc));

    try validateAppParityFixtureMetadata(.{
        .name = "valid source expression-or summary",
        .sql = "UPDATE usage_records SET status = 'archived' WHERE lower(status) = 'closed' OR lower(status) = 'old'",
        .family = .update_source,
        .summary = .{ .table_name = "usage_records", .expression_or_predicates = 2, .operations = 1 },
        .plan = "update_source:table=usage_records:source_pred=0:source_array_any=0:source_expr_pred=0:source_expr_or=2:source_expr_not=0:source_expr_array=0:source_order=0:source_limit=-1:claim=locked:ops=1:returning=0:returning_expr=0:returning_all=0",
    }, &seen, alloc);

    try validateAppParityFixtureMetadata(.{
        .name = "valid joined pagination summary",
        .sql = "UPDATE usage_records SET status = source_records.status FROM source_records WHERE usage_records.id = source_records.id ORDER BY usage_records.id LIMIT 2 OFFSET 1",
        .family = .update_joined_source,
        .summary = .{ .table_name = "usage_records", .join_on = 1, .order_by = 1, .limit = 2, .offset = 1, .operations = 1 },
        .plan = "update_joined_source:target=usage_records:source=source_records:left_pred=0:right_pred=0:on=1:order=1:limit=2:claim=locked:source_assignments=0:ops=1:returning=0:returning_expr=0:returning_all=0:offset=1",
    }, &seen, alloc);

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "stale joined predicate summary",
        .sql = "UPDATE usage_records SET status = source_records.status FROM source_records WHERE usage_records.id = source_records.id AND usage_records.status = 'open' AND source_records.status = 'archived'",
        .family = .update_joined_source,
        .summary = .{ .table_name = "usage_records", .predicates = 2, .join_on = 1, .operations = 1 },
        .plan = "update_joined_source:target=usage_records:source=source_records:left_pred=0:right_pred=1:on=1:order=0:limit=-1:claim=locked:source_assignments=0:ops=1:returning=0:returning_expr=0:returning_all=0",
    }, &seen, alloc));

    try validateAppParityFixtureMetadata(.{
        .name = "valid joined predicate summary",
        .sql = "UPDATE usage_records SET status = source_records.status FROM source_records WHERE usage_records.id = source_records.id AND usage_records.status = 'open' AND source_records.status = 'archived'",
        .family = .update_joined_source,
        .summary = .{ .table_name = "usage_records", .predicates = 2, .join_on = 1, .operations = 1 },
        .plan = "update_joined_source:target=usage_records:source=source_records:left_pred=1:right_pred=1:on=1:order=0:limit=-1:claim=locked:source_assignments=0:ops=1:returning=0:returning_expr=0:returning_all=0",
    }, &seen, alloc);

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "stale joined structured access summary",
        .sql = "UPDATE usage_records SET status = source_records.status FROM source_records WHERE usage_records.id = source_records.id AND usage_records.metadata @> '{\"tier\":\"gold\"}'::jsonb AND source_records.metadata @> '{\"tier\":\"gold\"}'::jsonb",
        .family = .update_joined_source,
        .summary = .{ .table_name = "usage_records", .json_contains = 2, .join_on = 1, .operations = 1 },
        .plan = "update_joined_source:target=usage_records:source=source_records:left_pred=0:right_pred=0:on=1:order=0:limit=-1:claim=locked:source_assignments=0:ops=1:returning=0:returning_expr=0:returning_all=0:left_json_contains=1",
    }, &seen, alloc));

    try validateAppParityFixtureMetadata(.{
        .name = "valid joined structured access summary",
        .sql = "UPDATE usage_records SET status = source_records.status FROM source_records WHERE usage_records.id = source_records.id AND usage_records.metadata @> '{\"tier\":\"gold\"}'::jsonb AND source_records.metadata @> '{\"tier\":\"gold\"}'::jsonb",
        .family = .update_joined_source,
        .summary = .{ .table_name = "usage_records", .json_contains = 2, .join_on = 1, .operations = 1 },
        .plan = "update_joined_source:target=usage_records:source=source_records:left_pred=0:right_pred=0:on=1:order=0:limit=-1:claim=locked:source_assignments=0:ops=1:returning=0:returning_expr=0:returning_all=0:left_json_contains=1:right_json_contains=1",
    }, &seen, alloc);

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "stale insert source operations summary",
        .sql = "INSERT INTO usage_records (id) SELECT id FROM usage_records",
        .family = .insert_source,
        .summary = .{ .table_name = "usage_records", .operations = 1 },
        .plan = "insert_source:table=usage_records:source_table=usage_records:source_pred=0:source_order=0:source_limit=-1:assignments=0:conflict=0:returning=0:returning_expr=0:returning_all=0",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "stale merge operations summary",
        .sql = "MERGE INTO usage_records AS target USING source_records AS source ON target.id = source.id WHEN MATCHED THEN UPDATE SET status = source.status",
        .family = .merge_mutation,
        .summary = .{ .table_name = "usage_records", .join_on = 1, .operations = 1 },
        .plan = "merge_mutation:target=usage_records:source=source_records:ctes=0:source_cte=0:match=1:matched_pred=0:matched_update=0:matched_delete=0:matched_noop=0:not_matched_pred=0:not_matched_insert=0:not_matched_noop=0:returning=0:returning_expr=0:returning_all=0",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "stale merge select summary",
        .sql = "MERGE INTO usage_records AS target USING source_records AS source ON target.id = source.id WHEN NOT MATCHED THEN INSERT (id, status) VALUES (source.id, source.status)",
        .family = .merge_mutation,
        .summary = .{ .table_name = "usage_records", .join_on = 1, .select = 2 },
        .plan = "merge_mutation:target=usage_records:source=source_records:ctes=0:source_cte=0:match=1:matched_pred=0:matched_update=0:matched_delete=0:matched_noop=0:not_matched_pred=0:not_matched_insert=1:not_matched_noop=0:returning=0:returning_expr=0:returning_all=0",
    }, &seen, alloc));

    try validateAppParityFixtureMetadata(.{
        .name = "valid merge select summary",
        .sql = "MERGE INTO usage_records AS target USING source_records AS source ON target.id = source.id WHEN NOT MATCHED THEN INSERT (id, status) VALUES (source.id, source.status)",
        .family = .merge_mutation,
        .summary = .{ .table_name = "usage_records", .join_on = 1, .select = 2 },
        .plan = "merge_mutation:target=usage_records:source=source_records:ctes=0:source_cte=0:match=1:matched_pred=0:matched_update=0:matched_delete=0:matched_noop=0:not_matched_pred=0:not_matched_insert=2:not_matched_noop=0:returning=0:returning_expr=0:returning_all=0",
    }, &seen, alloc);

    try validateAppParityFixtureMetadata(.{
        .name = "valid insert source operations summary",
        .sql = "INSERT INTO usage_records (id) SELECT id FROM usage_records",
        .family = .insert_source,
        .summary = .{ .table_name = "usage_records", .operations = 1 },
        .plan = "insert_source:table=usage_records:source_table=usage_records:source_pred=0:source_order=0:source_limit=-1:assignments=1:conflict=0:returning=0:returning_expr=0:returning_all=0",
    }, &seen, alloc);

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "ignored returning summary",
        .sql = "SELECT id FROM usage_records",
        .family = .query,
        .summary = .{ .table_name = "usage_records", .returning = 1 },
        .plan = "query:table=usage_records:pred=0:array_any=0:in=0:json_path_eq=0:json_contains=0:json_exists=0:array_contains=0:array_eq=0:text_patterns=0:expr_pred=0:expr_or=0:expr_not=0:select=1:order=0:limit=none",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "stale point returning summary",
        .sql = "INSERT INTO usage_records (id, status) VALUES ('u1', 'active')",
        .family = .insert,
        .summary = .{ .table_name = "usage_records", .returning = 1 },
        .plan = "insert:table=usage_records:writes=1:transforms=0:ops=0:deletes=0:returning_rows=0:returning_expr=0",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "stale source returning summary",
        .sql = "UPDATE usage_records SET status = 'archived' WHERE status = 'closed' RETURNING id",
        .family = .update_source,
        .summary = .{ .table_name = "usage_records", .predicates = 1, .operations = 1, .returning = 1 },
        .plan = "update_source:table=usage_records:source_pred=1:source_order=0:source_limit=-1:claim=locked:ops=1:returning=0:returning_expr=0:returning_all=0",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "stale returning all summary",
        .sql = "DELETE FROM usage_records WHERE status = 'closed' RETURNING *",
        .family = .delete_source,
        .summary = .{ .table_name = "usage_records", .predicates = 1, .returning = 0, .returning_all = true },
        .plan = "delete_source:table=usage_records:source_pred=1:source_order=0:source_limit=-1:claim=locked:returning=0:returning_expr=0:returning_all=0",
    }, &seen, alloc));

    try validateAppParityFixtureMetadata(.{
        .name = "valid merge returning all false summary",
        .sql = "MERGE INTO usage_records AS target USING source_records AS source ON target.id = source.id WHEN MATCHED THEN UPDATE SET status = source.status RETURNING target.id",
        .family = .merge_mutation,
        .summary = .{ .table_name = "usage_records", .join_on = 1, .operations = 1, .returning = 1, .returning_all = false },
        .plan = "merge_mutation:target=usage_records:source=source_records:ctes=0:source_cte=0:match=1:matched_pred=0:matched_update=1:matched_delete=0:matched_noop=0:not_matched_pred=0:not_matched_insert=0:not_matched_noop=0:returning=1:returning_expr=0:returning_all=0",
    }, &seen, alloc);

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "truncate returning summary",
        .sql = "TRUNCATE usage_records",
        .family = .truncate_source,
        .summary = .{ .table_name = "usage_records", .returning = 0 },
        .plan = "truncate_source:table=usage_records:source_pred=0:source_order=0:source_limit=-1:claim=locked:returning=0:returning_expr=0:returning_all=0",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "query mutation transform summary",
        .sql = "SELECT id FROM usage_records",
        .family = .query,
        .summary = .{ .table_name = "usage_records", .patch_expressions = 1 },
        .plan = "query:table=usage_records:pred=0:array_any=0:in=0:json_path_eq=0:json_contains=0:json_exists=0:array_contains=0:array_eq=0:text_patterns=0:expr_pred=0:expr_or=0:expr_not=0:select=1:order=0:limit=none",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "point insert mutation transform summary",
        .sql = "INSERT INTO usage_records (id, status) VALUES ('u1', 'active')",
        .family = .insert,
        .summary = .{ .table_name = "usage_records", .patch_expressions = 1 },
        .plan = "insert:table=usage_records:writes=1:transforms=0:ops=0:deletes=0:returning_rows=0:returning_expr=0",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "delete source mutation transform summary",
        .sql = "DELETE FROM usage_records WHERE status = 'closed'",
        .family = .delete_source,
        .summary = .{ .table_name = "usage_records", .json_set_expressions = 1 },
        .plan = "delete_source:table=usage_records:source_pred=1:source_order=0:source_limit=-1:claim=locked:returning=0:returning_expr=0:returning_all=0",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "stale update source transform summary",
        .sql = "UPDATE usage_records SET status = lower(status) WHERE id = 'u1'",
        .family = .update_source,
        .summary = .{ .table_name = "usage_records", .patch_expressions = 1 },
        .plan = "update_source:table=usage_records:source_pred=1:source_order=0:source_limit=-1:claim=locked:ops=0:patch_expr=0:increment_expr=0:json_set_expr=0:returning=0:returning_expr=0:returning_all=0",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "stale insert source transform summary",
        .sql = "INSERT INTO usage_records (id) SELECT id FROM usage_records ON CONFLICT (id) DO UPDATE SET status = lower(excluded.status)",
        .family = .insert_source,
        .summary = .{ .table_name = "usage_records", .patch_expressions = 1 },
        .plan = "insert_source:table=usage_records:source_table=usage_records:source_pred=0:source_order=0:source_limit=-1:assignments=1:conflict=1:returning=0:returning_expr=0:returning_all=0",
    }, &seen, alloc));

    try validateAppParityFixtureMetadata(.{
        .name = "valid update source transform summary",
        .sql = "UPDATE usage_records SET status = lower(status), priority = priority + 1, metadata = jsonb_set(metadata, '{billing,status}', to_jsonb(status), true) WHERE id = 'u1'",
        .family = .update_source,
        .summary = .{ .table_name = "usage_records", .patch_expressions = 1, .increment_expressions = 1, .json_set_expressions = 1 },
        .plan = "update_source:table=usage_records:source_pred=1:source_order=0:source_limit=-1:claim=locked:ops=0:patch_expr=1:increment_expr=1:json_set_expr=1:returning=0:returning_expr=0:returning_all=0",
    }, &seen, alloc);

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "update source source assignment summary",
        .sql = "UPDATE usage_records SET status = 'active' WHERE id = 'u1'",
        .family = .update_source,
        .summary = .{ .table_name = "usage_records", .source_assignments = 1 },
        .plan = "update_source:table=usage_records:source_pred=1:source_order=0:source_limit=-1:claim=locked:ops=1:returning=0:returning_expr=0:returning_all=0",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "stale joined source assignment summary",
        .sql = "UPDATE usage_records SET status = source_records.status FROM source_records WHERE usage_records.id = source_records.id",
        .family = .update_joined_source,
        .summary = .{ .table_name = "usage_records", .join_on = 1, .source_assignments = 1 },
        .plan = "update_joined_source:target=usage_records:source=source_records:left_pred=0:right_pred=0:on=1:order=0:limit=-1:claim=locked:source_assignments=0:ops=1:returning=0:returning_expr=0:returning_all=0",
    }, &seen, alloc));

    try validateAppParityFixtureMetadata(.{
        .name = "valid joined source assignment summary",
        .sql = "UPDATE usage_records SET status = source_records.status FROM source_records WHERE usage_records.id = source_records.id",
        .family = .update_joined_source,
        .summary = .{ .table_name = "usage_records", .join_on = 1, .source_assignments = 1 },
        .plan = "update_joined_source:target=usage_records:source=source_records:left_pred=0:right_pred=0:on=1:order=0:limit=-1:claim=locked:source_assignments=1:ops=0:returning=0:returning_expr=0:returning_all=0",
    }, &seen, alloc);

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "update source merge arm summary",
        .sql = "UPDATE usage_records SET status = 'active' WHERE id = 'u1'",
        .family = .update_source,
        .summary = .{ .table_name = "usage_records", .matched_predicates = 1 },
        .plan = "update_source:table=usage_records:source_pred=1:source_order=0:source_limit=-1:claim=locked:ops=1:returning=0:returning_expr=0:returning_all=0",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "update conflict guard summary",
        .sql = "UPDATE usage_records SET status = 'active' WHERE id = 'u1'",
        .family = .update,
        .summary = .{ .table_name = "usage_records", .operations = 1, .conflict_where = true },
        .plan = "update:table=usage_records:transforms=1:ops=1:returning_rows=0:returning_expr=0",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "delete source conflict guard summary",
        .sql = "DELETE FROM usage_records WHERE status = 'closed'",
        .family = .delete_source,
        .summary = .{ .table_name = "usage_records", .conflict_where = true },
        .plan = "delete_source:table=usage_records:source_pred=1:source_order=0:source_limit=-1:claim=locked:returning=0:returning_expr=0:returning_all=0",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "stale insert conflict guard summary",
        .sql = "INSERT INTO usage_records (id, status) VALUES ('u1', 'active') ON CONFLICT (id) DO UPDATE SET status = excluded.status",
        .family = .insert,
        .summary = .{ .table_name = "usage_records", .operations = 1, .conflict_where = true },
        .plan = "insert:table=usage_records:writes=0:transforms=1:ops=1:deletes=0:returning_rows=0:returning_expr=0:op_set=1",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "explicit false conflict guard summary",
        .sql = "INSERT INTO usage_records (id, status) VALUES ('u1', 'active') ON CONFLICT (id) DO UPDATE SET status = excluded.status",
        .family = .insert,
        .summary = .{ .table_name = "usage_records", .operations = 1, .conflict_where = false },
        .plan = "insert:table=usage_records:writes=0:transforms=1:ops=1:deletes=0:returning_rows=0:returning_expr=0:op_set=1:conflict_where=1",
    }, &seen, alloc));

    try validateAppParityFixtureMetadata(.{
        .name = "valid insert conflict guard summary",
        .sql = "INSERT INTO usage_records (id, status) VALUES ('u1', 'active') ON CONFLICT (id) DO UPDATE SET status = excluded.status WHERE usage_records.status = 'old'",
        .family = .insert,
        .summary = .{ .table_name = "usage_records", .operations = 1, .conflict_where = true },
        .plan = "insert:table=usage_records:writes=0:transforms=1:ops=1:deletes=0:returning_rows=0:returning_expr=0:op_set=1:conflict_where=1",
    }, &seen, alloc);

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "query aggregate summary",
        .sql = "SELECT id FROM usage_records",
        .family = .query,
        .summary = .{ .table_name = "usage_records", .group_by = 1 },
        .plan = "query:table=usage_records:pred=0:array_any=0:in=0:json_path_eq=0:json_contains=0:json_exists=0:array_contains=0:array_eq=0:text_patterns=0:expr_pred=0:expr_or=0:expr_not=0:select=1:order=0:limit=none",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "generic read query aggregate summary",
        .sql = "SELECT id FROM usage_records",
        .family = .read,
        .summary = .{ .table_name = "usage_records", .aggregations = 1 },
        .plan = "read:query:query:table=usage_records:ctes=0:pred=0:expr_pred=0:json_eq=0:or=0:not=0:select=1:expr=0:alias=0:order=0:order_expr=0:limit=none:claim=none",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "aggregate join select summary",
        .sql = "SELECT status, count(*) AS count FROM usage_records GROUP BY status",
        .family = .aggregate,
        .summary = .{ .table_name = "usage_records", .join_select = 1 },
        .plan = "aggregate:table=usage_records:ctes=0:source_cte=0:source_pred=0:group_by=1:group_expr=0:agg=1:filter_groups=0:having=0:having_expr=0:having_any=0:having_not=0:order=0:limit=none",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "stale join select summary",
        .sql = "SELECT usage_records.id FROM usage_records JOIN archived_records ON usage_records.id = archived_records.id",
        .family = .join,
        .summary = .{ .table_name = "usage_records", .join_on = 1, .join_select = 1 },
        .plan = "join:left=usage_records:right=archived_records:left_pred=0:right_pred=0:on=1:select=0:order=0:limit=none",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "join lateral offset summary",
        .sql = "SELECT usage_records.id FROM usage_records JOIN archived_records ON usage_records.id = archived_records.id",
        .family = .join,
        .summary = .{ .table_name = "usage_records", .right_offset = 1 },
        .plan = "join:left=usage_records:right=archived_records:left_pred=0:right_pred=0:on=1:select=1:order=0:limit=none",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "stale join on summary",
        .sql = "SELECT usage_records.id FROM usage_records JOIN archived_records ON usage_records.id = archived_records.id",
        .family = .join,
        .summary = .{ .table_name = "usage_records", .join_on = 1 },
        .plan = "join:left=usage_records:right=archived_records:left_pred=0:right_pred=0:on=0:select=1:order=0:limit=none",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "stale merge join on summary",
        .sql = "MERGE INTO usage_records AS target USING source_records AS source ON target.id = source.id WHEN MATCHED THEN UPDATE SET status = source.status",
        .family = .merge_mutation,
        .summary = .{ .table_name = "usage_records", .join_on = 1, .operations = 1 },
        .plan = "merge_mutation:target=usage_records:source=source_records:ctes=0:source_cte=0:match=0:matched_pred=0:matched_update=1:matched_delete=0:matched_noop=0:not_matched_pred=0:not_matched_insert=0:not_matched_noop=0:returning=0:returning_expr=0:returning_all=0",
    }, &seen, alloc));

    try validateAppParityFixtureMetadata(.{
        .name = "valid join on summary",
        .sql = "SELECT usage_records.id FROM usage_records JOIN archived_records ON usage_records.id = archived_records.id",
        .family = .join,
        .summary = .{ .table_name = "usage_records", .join_on = 1, .join_select = 1 },
        .plan = "join:left=usage_records:right=archived_records:left_pred=0:right_pred=0:on=1:select=1:order=0:limit=none",
    }, &seen, alloc);

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "lateral window summary",
        .sql = "SELECT usage_records.id FROM usage_records LEFT JOIN LATERAL (SELECT id FROM archived_records WHERE archived_records.id = usage_records.id LIMIT 1) AS latest ON true",
        .family = .lateral,
        .summary = .{ .table_name = "usage_records", .windows = 1 },
        .plan = "lateral:left=usage_records:right=archived_records:left_pred=0:right_pred=1:correlations=1:select=1:order=0:limit=none",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "stale lateral correlation summary",
        .sql = "SELECT usage_records.id FROM usage_records LEFT JOIN LATERAL (SELECT id FROM archived_records WHERE archived_records.id = usage_records.id LIMIT 1) AS latest ON true",
        .family = .lateral,
        .summary = .{ .table_name = "usage_records", .lateral_correlations = 1, .join_select = 1 },
        .plan = "lateral:left=usage_records:right=archived_records:ctes=0:left_pred=0:left_array_any=0:left_expr_pred=0:left_expr_or=0:left_expr_not=0:left_expr_array=0:left_json_eq=0:left_text=0:right_pred=1:right_array_any=0:right_expr_pred=0:right_expr_or=0:right_expr_not=0:right_expr_array=0:right_json_eq=0:right_text=0:right_order=0:right_order_expr=0:right_limit=1:corr=0:select=1:order=0:order_expr=0:limit=-1",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "stale lateral right offset summary",
        .sql = "SELECT usage_records.id FROM usage_records LEFT JOIN LATERAL (SELECT id FROM archived_records WHERE archived_records.id = usage_records.id LIMIT 1 OFFSET 2) AS latest ON true",
        .family = .lateral,
        .summary = .{ .table_name = "usage_records", .lateral_correlations = 1, .join_select = 1, .right_offset = 2 },
        .plan = "lateral:left=usage_records:right=archived_records:ctes=0:left_pred=0:left_array_any=0:left_expr_pred=0:left_expr_or=0:left_expr_not=0:left_expr_array=0:left_json_eq=0:left_text=0:right_pred=1:right_array_any=0:right_expr_pred=0:right_expr_or=0:right_expr_not=0:right_expr_array=0:right_json_eq=0:right_text=0:right_order=0:right_order_expr=0:right_limit=1:corr=1:select=1:order=0:order_expr=0:limit=-1:right_offset=1",
    }, &seen, alloc));

    try validateAppParityFixtureMetadata(.{
        .name = "valid lateral stage summary",
        .sql = "SELECT usage_records.id FROM usage_records LEFT JOIN LATERAL (SELECT id FROM archived_records WHERE archived_records.id = usage_records.id LIMIT 1 OFFSET 2) AS latest ON true",
        .family = .lateral,
        .summary = .{ .table_name = "usage_records", .lateral_correlations = 1, .join_select = 1, .right_offset = 2 },
        .plan = "lateral:left=usage_records:right=archived_records:ctes=0:left_pred=0:left_array_any=0:left_expr_pred=0:left_expr_or=0:left_expr_not=0:left_expr_array=0:left_json_eq=0:left_text=0:right_pred=1:right_array_any=0:right_expr_pred=0:right_expr_or=0:right_expr_not=0:right_expr_array=0:right_json_eq=0:right_text=0:right_order=0:right_order_expr=0:right_limit=1:corr=1:select=1:order=0:order_expr=0:limit=-1:right_offset=2",
    }, &seen, alloc);

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "stale window summary",
        .sql = "SELECT id, row_number() OVER (ORDER BY id) AS rn FROM usage_records",
        .family = .window,
        .summary = .{ .table_name = "usage_records", .select = 1, .windows = 1, .order_by = 1 },
        .plan = "window:table=usage_records:ctes=0:source_cte=0:source_pred=0:windows=0:window_expr=0:window_default=0:window_frame_sig=0:select=1:order=1:limit=-1",
    }, &seen, alloc));

    try validateAppParityFixtureMetadata(.{
        .name = "valid window summary",
        .sql = "SELECT id, row_number() OVER (ORDER BY id) AS rn FROM usage_records",
        .family = .window,
        .summary = .{ .table_name = "usage_records", .select = 1, .windows = 1, .order_by = 1 },
        .plan = "window:table=usage_records:ctes=0:source_cte=0:source_pred=0:windows=1:window_expr=0:window_default=0:window_frame_sig=0:select=1:order=1:limit=-1",
    }, &seen, alloc);

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "aggregate full query output summary",
        .sql = "SELECT status, count(*) AS count FROM usage_records GROUP BY status",
        .family = .aggregate,
        .summary = .{ .table_name = "usage_records", .aggregations = 1, .select_all = true },
        .plan = "aggregate:table=usage_records:ctes=0:source_cte=0:source_pred=0:group_by=1:group_expr=0:agg=1:filter_groups=0:having=0:having_expr=0:having_any=0:having_not=0:order=0:limit=none",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "stale aggregate group summary",
        .sql = "SELECT status, count(*) AS count FROM usage_records GROUP BY status",
        .family = .aggregate,
        .summary = .{ .table_name = "usage_records", .group_by = 1, .aggregations = 1 },
        .plan = "aggregate:table=usage_records:source_pred=0:source_json_eq=0:group=0:group_expr=0:aggs=1:agg_expr=0:filter_expr=0:having=0:order=0:limit=-1",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "stale aggregate filter summary",
        .sql = "SELECT status, count(*) FILTER (WHERE enabled = true) AS enabled_count FROM usage_records GROUP BY status",
        .family = .aggregate,
        .summary = .{ .table_name = "usage_records", .group_by = 1, .aggregations = 1, .filter_groups = 2, .having_expressions = 0, .having_any = 0, .having_not = 0 },
        .plan = "aggregate:table=usage_records:source_pred=0:source_array_any=0:source_expr_pred=0:source_expr_or=0:source_expr_not=0:source_expr_array=0:source_json_eq=0:group=1:group_expr=0:aggs=1:agg_expr=0:filter_expr=0:filter_groups=1:having=0:having_expr=0:having_any=0:having_not=0:order=0:limit=-1",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "malformed aggregate optional zero summary",
        .sql = "SELECT status, count(*) AS count FROM usage_records GROUP BY status",
        .family = .aggregate,
        .summary = .{ .table_name = "usage_records", .group_by = 1, .aggregations = 1, .filter_groups = 0 },
        .plan = "aggregate:table=usage_records:source_pred=0:source_array_any=0:source_expr_pred=0:source_expr_or=0:source_expr_not=0:source_expr_array=0:source_json_eq=0:group=1:group_expr=0:aggs=1:agg_expr=0:filter_expr=0:filter_groups=none:having=0:having_expr=0:having_any=0:having_not=0:order=0:limit=-1",
    }, &seen, alloc));

    try validateAppParityFixtureMetadata(.{
        .name = "valid aggregate extended summary",
        .sql = "SELECT status, count(*) FILTER (WHERE enabled = true) AS enabled_count FROM usage_records GROUP BY status HAVING enabled_count > 0",
        .family = .aggregate,
        .summary = .{ .table_name = "usage_records", .group_by = 1, .aggregations = 1, .filter_groups = 1, .having_expressions = 0, .having_any = 0, .having_not = 0 },
        .plan = "aggregate:table=usage_records:source_pred=0:source_array_any=0:source_expr_pred=0:source_expr_or=0:source_expr_not=0:source_expr_array=0:source_json_eq=0:group=1:group_expr=0:aggs=1:agg_expr=0:filter_expr=0:filter_groups=1:having=0:having_expr=0:having_any=0:having_not=0:order=0:limit=-1",
    }, &seen, alloc);

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "join full query distinct summary",
        .sql = "SELECT usage_records.id FROM usage_records JOIN archived_records ON usage_records.id = archived_records.id",
        .family = .join,
        .summary = .{ .table_name = "usage_records", .join_on = 1, .distinct_on = 1 },
        .plan = "join:left=usage_records:right=archived_records:left_pred=0:right_pred=0:on=1:select=1:order=0:limit=none",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "stale query select all summary",
        .sql = "SELECT * FROM usage_records",
        .family = .query,
        .summary = .{ .table_name = "usage_records", .select_all = true },
        .plan = "query:table=usage_records:ctes=0:pred=0:expr_pred=0:json_eq=0:or=0:not=0:select=0:expr=0:alias=0:order=0:order_expr=0:limit=none:claim=none",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "stale query select all false summary",
        .sql = "SELECT id FROM usage_records",
        .family = .query,
        .summary = .{ .table_name = "usage_records", .select = 1, .select_all = false },
        .plan = "query:table=usage_records:ctes=0:pred=0:expr_pred=0:json_eq=0:or=0:not=0:select=1:expr=0:alias=0:order=0:order_expr=0:limit=none:claim=none:select_all=1",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "malformed query select all false summary",
        .sql = "SELECT id FROM usage_records",
        .family = .query,
        .summary = .{ .table_name = "usage_records", .select = 1, .select_all = false },
        .plan = "query:table=usage_records:ctes=0:pred=0:expr_pred=0:json_eq=0:or=0:not=0:select=1:expr=0:alias=0:order=0:order_expr=0:limit=none:claim=none:select_all=2",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "stale query distinct on summary",
        .sql = "SELECT DISTINCT ON (customer) customer, id FROM usage_records ORDER BY customer, id",
        .family = .query,
        .summary = .{ .table_name = "usage_records", .select = 2, .distinct_on = 1, .order_by = 2 },
        .plan = "query:table=usage_records:ctes=0:pred=0:expr_pred=0:json_eq=0:or=0:not=0:select=2:expr=0:alias=0:distinct_on=2:order=2:order_expr=0:limit=-1:claim=none",
    }, &seen, alloc));

    try validateAppParityFixtureMetadata(.{
        .name = "valid query full output summary",
        .sql = "SELECT *, lower(status) AS status_lower FROM usage_records",
        .family = .query,
        .summary = .{ .table_name = "usage_records", .select = 0, .select_all = true },
        .plan = "query:table=usage_records:ctes=0:pred=0:expr_pred=0:json_eq=0:or=0:not=0:select=0:expr=1:alias=0:order=0:order_expr=0:limit=none:claim=none:select_all=1",
    }, &seen, alloc);

    try validateAppParityFixtureMetadata(.{
        .name = "valid compact query select all false summary",
        .sql = "SELECT id FROM usage_records",
        .family = .query,
        .summary = .{ .table_name = "usage_records", .select = 1, .select_all = false },
        .plan = "query:table=usage_records:ctes=0:pred=0:expr_pred=0:json_eq=0:or=0:not=0:select=1:expr=0:alias=0:order=0:order_expr=0:limit=none:claim=none",
    }, &seen, alloc);

    try validateAppParityFixtureMetadata(.{
        .name = "valid query distinct on summary",
        .sql = "SELECT DISTINCT ON (customer) customer, id FROM usage_records ORDER BY customer, id",
        .family = .query,
        .summary = .{ .table_name = "usage_records", .select = 2, .distinct_on = 1, .order_by = 2 },
        .plan = "query:table=usage_records:ctes=0:pred=0:expr_pred=0:json_eq=0:or=0:not=0:select=2:expr=0:alias=0:distinct_on=1:order=2:order_expr=0:limit=-1:claim=none",
    }, &seen, alloc);

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "query join summary",
        .sql = "SELECT id FROM usage_records",
        .family = .query,
        .summary = .{ .table_name = "usage_records", .join_on = 1 },
        .plan = "query:table=usage_records:pred=0:array_any=0:in=0:json_path_eq=0:json_contains=0:json_exists=0:array_contains=0:array_eq=0:text_patterns=0:expr_pred=0:expr_or=0:expr_not=0:select=1:order=0:limit=none",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "lateral join summary",
        .sql = "SELECT usage_records.id FROM usage_records LEFT JOIN LATERAL (SELECT id FROM archived_records WHERE archived_records.id = usage_records.id LIMIT 1) AS latest ON true",
        .family = .lateral,
        .summary = .{ .table_name = "usage_records", .lateral_correlations = 1, .join_on = 1 },
        .plan = "lateral:left=usage_records:right=archived_records:left_pred=0:right_pred=1:correlations=1:select=1:order=0:limit=none",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "aggregate row claim summary",
        .sql = "SELECT status, count(*) AS count FROM usage_records GROUP BY status",
        .family = .aggregate,
        .summary = .{ .table_name = "usage_records", .aggregations = 1, .row_claim_skip_locked = true },
        .plan = "aggregate:table=usage_records:ctes=0:source_cte=0:source_pred=0:group_by=1:group_expr=0:agg=1:filter_groups=0:having=0:having_expr=0:having_any=0:having_not=0:order=0:limit=none",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "merge row claim summary",
        .sql = "MERGE INTO usage_records AS target USING source_records AS source ON target.id = source.id WHEN MATCHED THEN UPDATE SET status = source.status",
        .family = .merge_mutation,
        .summary = .{ .table_name = "usage_records", .join_on = 1, .operations = 1, .row_claim_skip_locked = false },
        .plan = "merge_mutation:target=usage_records:source=source_records:ctes=0:source_cte=0:match=1:matched_pred=0:matched_update=1:matched_delete=0:matched_noop=0:not_matched_pred=0:not_matched_insert=0:not_matched_noop=0:returning=0:returning_expr=0:returning_all=0",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "stale query row claim summary",
        .sql = "SELECT id FROM usage_records",
        .family = .query,
        .summary = .{ .table_name = "usage_records", .select = 1, .row_claim_skip_locked = false },
        .plan = "query:table=usage_records:ctes=0:pred=0:expr_pred=0:json_eq=0:or=0:not=0:select=1:expr=0:alias=0:order=0:order_expr=0:limit=none:claim=none",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "stale skip locked row claim summary",
        .sql = "SELECT id FROM usage_records FOR UPDATE SKIP LOCKED",
        .family = .query,
        .summary = .{ .table_name = "usage_records", .select = 1, .row_claim_skip_locked = true },
        .plan = "query:table=usage_records:ctes=0:pred=0:expr_pred=0:json_eq=0:or=0:not=0:select=1:expr=0:alias=0:order=0:order_expr=0:limit=none:claim=locked",
    }, &seen, alloc));

    try validateAppParityFixtureMetadata(.{
        .name = "valid nowait row claim summary",
        .sql = "SELECT id FROM usage_records FOR UPDATE NOWAIT",
        .family = .query,
        .summary = .{ .table_name = "usage_records", .select = 1, .row_claim_skip_locked = false },
        .plan = "query:table=usage_records:ctes=0:pred=0:expr_pred=0:json_eq=0:or=0:not=0:select=1:expr=0:alias=0:order=0:order_expr=0:limit=none:claim=nowait",
    }, &seen, alloc);

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "stale merge matched predicate summary",
        .sql = "MERGE INTO usage_records AS target USING source_records AS source ON target.id = source.id WHEN MATCHED AND target.status = 'old' THEN UPDATE SET status = source.status",
        .family = .merge_mutation,
        .summary = .{ .table_name = "usage_records", .join_on = 1, .operations = 1, .matched_predicates = 1 },
        .plan = "merge_mutation:target=usage_records:source=source_records:ctes=0:source_cte=0:match=1:matched_pred=0:matched_update=1:matched_delete=0:matched_noop=0:not_matched_pred=0:not_matched_insert=0:not_matched_noop=0:returning=0:returning_expr=0:returning_all=0",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "stale merge matched delete summary",
        .sql = "MERGE INTO usage_records AS target USING source_records AS source ON target.id = source.id WHEN MATCHED THEN DELETE",
        .family = .merge_mutation,
        .summary = .{ .table_name = "usage_records", .join_on = 1, .matched_delete = true },
        .plan = "merge_mutation:target=usage_records:source=source_records:ctes=0:source_cte=0:match=1:matched_pred=0:matched_update=0:matched_delete=0:matched_noop=0:not_matched_pred=0:not_matched_insert=0:not_matched_noop=0:returning=0:returning_expr=0:returning_all=0",
    }, &seen, alloc));

    try validateAppParityFixtureMetadata(.{
        .name = "valid merge arm false summaries",
        .sql = "MERGE INTO usage_records AS target USING source_records AS source ON target.id = source.id WHEN MATCHED THEN UPDATE SET status = source.status",
        .family = .merge_mutation,
        .summary = .{ .table_name = "usage_records", .join_on = 1, .operations = 1, .matched_delete = false, .matched_do_nothing = false, .not_matched_do_nothing = false },
        .plan = "merge_mutation:target=usage_records:source=source_records:ctes=0:source_cte=0:match=1:matched_pred=0:matched_update=1:matched_delete=0:matched_noop=0:not_matched_pred=0:not_matched_insert=0:not_matched_noop=0:returning=0:returning_expr=0:returning_all=0",
    }, &seen, alloc);

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "query temporal ddl summary",
        .sql = "SELECT id FROM usage_records",
        .family = .query,
        .summary = .{ .table_name = "usage_records", .temporal_periods = 1 },
        .plan = "query:table=usage_records:pred=0:array_any=0:in=0:json_path_eq=0:json_contains=0:json_exists=0:array_contains=0:array_eq=0:text_patterns=0:expr_pred=0:expr_or=0:expr_not=0:select=1:order=0:limit=none",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "stale temporal periods summary",
        .sql = "CREATE TABLE products (product_id int NOT NULL, valid_at daterange NOT NULL, PRIMARY KEY (product_id, valid_at WITHOUT OVERLAPS));",
        .family = .ddl,
        .summary = .{ .ddl_tag = .create_table, .table_name = "products", .select = 3, .temporal_periods = 2, .temporal_primary_key = true, .temporal_unique = 0, .temporal_foreign_keys = 0 },
        .plan = "ddl:create_table:table=products:columns=3:unique=0:fk=0:checks=0:if_not_exists=false:periods=1:temporal_pk=true:temporal_unique=0:temporal_fk=0:pk=1",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "stale temporal primary summary",
        .sql = "CREATE TABLE usage_records (id uuid PRIMARY KEY)",
        .family = .ddl,
        .summary = .{ .ddl_tag = .create_table, .table_name = "usage_records", .select = 1, .temporal_primary_key = true },
        .plan = "ddl:create_table:table=usage_records:columns=1:unique=0:fk=0:checks=0:if_not_exists=false:pk=1",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "duplicate temporal primary summary",
        .sql = "CREATE TABLE products (product_id int NOT NULL, valid_at daterange NOT NULL, PRIMARY KEY (product_id, valid_at WITHOUT OVERLAPS));",
        .family = .ddl,
        .summary = .{ .ddl_tag = .create_table, .table_name = "products", .select = 3, .temporal_periods = 1, .temporal_primary_key = true, .temporal_unique = 0, .temporal_foreign_keys = 0 },
        .plan = "ddl:create_table:table=products:columns=3:unique=0:fk=0:checks=0:if_not_exists=false:periods=1:temporal_pk=false:temporal_pk=true:temporal_unique=0:temporal_fk=0:pk=1",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "unsupported family plan mismatch",
        .sql = "DELETE FROM usage_records WHERE status = 'closed'",
        .family = .unsupported_delete,
        .classification_reason = "multi_output_subquery_delete_selector",
        .plan = "unsupported:update:requires=multi_output_subquery_delete_selector",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "free form unsupported reason",
        .sql = "CREATE SEQUENCE usage_records_id_seq START WITH 1",
        .family = .unsupported_ddl,
        .classification_reason = "sequence catalog plan",
        .plan = "unsupported:ddl:requires=sequence catalog plan",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "unknown stable unsupported reason",
        .sql = "CREATE SEQUENCE usage_records_id_seq START WITH 1",
        .family = .unsupported_ddl,
        .classification_reason = "sequence_catalog_plan",
        .plan = "unsupported:ddl:requires=sequence_catalog_plan",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "unsupported reason missing from plan",
        .sql = "TRUNCATE usage_records CASCADE",
        .family = .unsupported_write,
        .classification_reason = "multi_table_generation_barrier",
        .plan = "unsupported:write",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "unsupported summary",
        .sql = "DELETE FROM usage_records WHERE status = 'closed'",
        .family = .unsupported_delete,
        .summary = .{ .table_name = "usage_records", .predicates = 1 },
        .classification_reason = "multi_output_subquery_delete_selector",
        .plan = "unsupported:delete:requires=multi_output_subquery_delete_selector",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "adapter noop summary",
        .sql = "SET client_encoding = 'UTF8'",
        .family = .adapter_noop_ddl,
        .summary = .{ .table_name = "client_encoding" },
        .classification_reason = "session_setting",
        .plan = "adapter_noop:ddl:reason=session_setting",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "supported entry with ignored reason",
        .sql = "SELECT id FROM usage_records",
        .family = .query,
        .summary = .{ .table_name = "usage_records" },
        .classification_reason = "set_operation_plan",
        .plan = "query:table=usage_records:pred=0:array_any=0:in=0:json_path_eq=0:json_contains=0:json_exists=0:array_contains=0:array_eq=0:text_patterns=0:expr_pred=0:expr_or=0:expr_not=0:select=1:order=0:limit=none",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "adapter noop reason mismatch",
        .sql = "SET client_encoding = 'UTF8'",
        .family = .adapter_noop_ddl,
        .classification_reason = "session_setting",
        .plan = "adapter_noop:ddl:reason=transaction_control",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "adapter noop reason prefix mismatch",
        .sql = "SET client_encoding = 'UTF8'",
        .family = .adapter_noop_ddl,
        .classification_reason = "session_setting",
        .plan = "adapter_noop:ddl:reason=session_setting_extra",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "unsupported reason prefix mismatch",
        .sql = "TRUNCATE usage_records CASCADE",
        .family = .unsupported_write,
        .classification_reason = "multi_table_generation_barrier",
        .plan = "unsupported:write:requires=multi_table_generation_barrier_extra",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "malformed applied plan",
        .sql = "CREATE INDEX usage_records_status_idx ON usage_records (status)",
        .family = .ddl,
        .summary = .{ .ddl_tag = .create_index, .table_name = "usage_records" },
        .plan = "ddl:create_index:table=usage_records:columns=1:expr=0:generated_expr=0:where=0:unique=false:if_not_exists=false",
        .applied_plan = "applied:rebuild=true:rewrite=false",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "stale applied plan",
        .sql = "CREATE INDEX usage_records_status_idx ON usage_records (status)",
        .family = .ddl,
        .summary = .{ .ddl_tag = .create_index, .table_name = "usage_records" },
        .plan = "ddl:create_index:table=usage_records:columns=1:expr=0:generated_expr=0:where=0:unique=false:if_not_exists=false",
        .apply_setup_sql = &.{"CREATE TABLE usage_records (id text PRIMARY KEY, status text)"},
        .applied_plan = "applied:rebuild=false:validation=false:rewrite=false:building_indexes=0:unvalidated_unique=0:unvalidated_fk=0:unvalidated_check=0:update_policy=0:work_items=0:work=none",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "non-ddl applied plan",
        .sql = "SELECT id FROM usage_records",
        .family = .query,
        .summary = .{ .table_name = "usage_records" },
        .plan = "query:table=usage_records:pred=0:array_any=0:in=0:json_path_eq=0:json_contains=0:json_exists=0:array_contains=0:array_eq=0:text_patterns=0:expr_pred=0:expr_or=0:expr_not=0:select=1:order=0:limit=none",
        .applied_plan = "applied:rebuild=false:validation=false:rewrite=false:building_indexes=0:unvalidated_unique=0:unvalidated_fk=0:unvalidated_check=0:update_policy=0:work_items=0:work=none",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "empty setup sql",
        .sql = "SELECT id FROM usage_records",
        .family = .query,
        .summary = .{ .table_name = "usage_records" },
        .plan = "query:table=usage_records:pred=0:array_any=0:in=0:json_path_eq=0:json_contains=0:json_exists=0:array_contains=0:array_eq=0:text_patterns=0:expr_pred=0:expr_or=0:expr_not=0:select=1:order=0:limit=none",
        .apply_setup_sql = &.{""},
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "non-ddl setup sql",
        .sql = "SELECT id FROM usage_records",
        .family = .query,
        .summary = .{ .table_name = "usage_records" },
        .plan = "query:table=usage_records:pred=0:array_any=0:in=0:json_path_eq=0:json_contains=0:json_exists=0:array_contains=0:array_eq=0:text_patterns=0:expr_pred=0:expr_or=0:expr_not=0:select=1:order=0:limit=none",
        .apply_setup_sql = &.{"SELECT id FROM usage_records"},
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "ddl setup without applied plan",
        .sql = "CREATE VIEW active_usage AS SELECT id FROM usage_records",
        .family = .ddl,
        .summary = .{ .ddl_tag = .create_view, .table_name = "active_usage" },
        .plan = "ddl:create_view:name=active_usage:replace=false",
        .apply_setup_sql = &.{"CREATE TABLE usage_records (id text PRIMARY KEY)"},
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "unsupported ddl setup sql",
        .sql = "COPY usage_records TO STDIN",
        .family = .unsupported_ddl,
        .classification_reason = "bulk_io_plan",
        .plan = "unsupported:ddl:requires=bulk_io_plan",
        .apply_setup_sql = &.{"CREATE TABLE usage_records (id text PRIMARY KEY)"},
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "adapter noop setup sql",
        .sql = "SET client_encoding = 'UTF8'",
        .family = .adapter_noop_ddl,
        .classification_reason = "session_setting",
        .plan = "adapter_noop:ddl:reason=session_setting",
        .apply_setup_sql = &.{"CREATE TABLE usage_records (id text PRIMARY KEY)"},
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "ignored source schema",
        .sql = "SELECT id FROM usage_records",
        .family = .query,
        .summary = .{ .table_name = "usage_records" },
        .plan = "query:table=usage_records:pred=0:array_any=0:in=0:json_path_eq=0:json_contains=0:json_exists=0:array_contains=0:array_eq=0:text_patterns=0:expr_pred=0:expr_or=0:expr_not=0:select=1:order=0:limit=none",
        .source_schema_json = "{}",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "malformed source schema",
        .sql = "SELECT usage_records.id FROM usage_records JOIN archived_records ON usage_records.id = archived_records.id",
        .family = .join,
        .summary = .{ .table_name = "usage_records" },
        .plan = "join:left=usage_records:right=archived_records:on=1",
        .source_schema_json = "{\"fields\":",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "source schema without primary key",
        .sql = "SELECT usage_records.id FROM usage_records JOIN archived_records ON usage_records.id = archived_records.id",
        .family = .join,
        .summary = .{ .table_name = "usage_records" },
        .plan = "join:left=usage_records:right=archived_records:on=1",
        .source_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}}}
        ,
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "source schema without source table",
        .sql = "SELECT id FROM usage_records",
        .family = .join,
        .summary = .{ .table_name = "usage_records" },
        .plan = "join:left=usage_records:right=archived_records:on=1",
        .source_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
        ,
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "source schema matches target table",
        .sql = "INSERT INTO usage_records (id) SELECT id FROM usage_records",
        .family = .insert_source,
        .summary = .{ .table_name = "usage_records" },
        .plan = "insert_source:table=usage_records:source_table=usage_records:source_pred=0:source_order=0:source_limit=-1:assignments=1:conflict=0:returning=0:returning_expr=0:returning_all=0",
        .source_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
        ,
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "source schema stale insert source plan",
        .sql = "INSERT INTO usage_records (id) SELECT id FROM archived_records",
        .family = .insert_source,
        .summary = .{ .table_name = "usage_records" },
        .plan = "insert_source:table=usage_records:source_table=stale_records:source_pred=0:source_order=0:source_limit=-1:assignments=1:conflict=0:returning=0:returning_expr=0:returning_all=0",
        .source_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
        ,
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "source schema stale join plan",
        .sql = "SELECT usage_records.id FROM usage_records JOIN archived_records ON usage_records.id = archived_records.id",
        .family = .join,
        .summary = .{ .table_name = "usage_records", .join_on = 1 },
        .plan = "join:left=usage_records:right=stale_records:left_pred=0:right_pred=0:on=1:select=1:order=0:limit=none",
        .source_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
        ,
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "ignored returning rows",
        .sql = "SELECT id FROM usage_records",
        .family = .query,
        .summary = .{ .table_name = "usage_records" },
        .plan = "query:table=usage_records:pred=0:array_any=0:in=0:json_path_eq=0:json_contains=0:json_exists=0:array_contains=0:array_eq=0:text_patterns=0:expr_pred=0:expr_or=0:expr_not=0:select=1:order=0:limit=none",
        .returning_rows = &.{"{\"id\":\"u1\"}"},
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "non-object returning row",
        .sql = "INSERT INTO usage_records (id, status) VALUES ('u1', 'active') RETURNING id",
        .family = .insert,
        .summary = .{ .table_name = "usage_records", .returning = 1 },
        .plan = "insert:table=usage_records:writes=1:transforms=0:ops=0:deletes=0:returning_rows=1:returning_expr=0",
        .returning_rows = &.{"[\"u1\"]"},
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "returning rows without returning summary",
        .sql = "INSERT INTO usage_records (id, status) VALUES ('u1', 'active') RETURNING id",
        .family = .insert,
        .summary = .{ .table_name = "usage_records" },
        .plan = "insert:table=usage_records:writes=1:transforms=0:ops=0:deletes=0:returning_rows=1:returning_expr=0",
        .returning_rows = &.{"{\"id\":\"u1\"}"},
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "returning rows count mismatch",
        .sql = "INSERT INTO usage_records (id, status) VALUES ('u1', 'active'), ('u2', 'active') RETURNING id",
        .family = .insert,
        .summary = .{ .table_name = "usage_records", .returning = 2 },
        .plan = "insert:table=usage_records:writes=2:transforms=0:ops=0:deletes=0:returning_rows=2:returning_expr=0",
        .returning_rows = &.{"{\"id\":\"u1\"}"},
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "returning rows plan count mismatch",
        .sql = "INSERT INTO usage_records (id, status) VALUES ('u1', 'active') RETURNING id",
        .family = .insert,
        .summary = .{ .table_name = "usage_records", .returning = 1 },
        .plan = "insert:table=usage_records:writes=1:transforms=0:ops=0:deletes=0:returning_rows=0:returning_expr=0",
        .returning_rows = &.{"{\"id\":\"u1\"}"},
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "resolver row without version",
        .sql = "INSERT INTO usage_records (id, status) VALUES ('u1', 'active') ON CONFLICT (id) DO UPDATE SET status = excluded.status RETURNING id",
        .family = .insert,
        .summary = .{ .table_name = "usage_records", .operations = 1, .returning = 1 },
        .plan = "insert:table=usage_records:writes=0:transforms=1:ops=1:deletes=0:returning_rows=1:returning_expr=0:op_set=1",
        .resolver_row_json = "{\"id\":\"u1\",\"status\":\"old\"}",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "ignored resolver hint",
        .sql = "SELECT id FROM usage_records",
        .family = .query,
        .summary = .{ .table_name = "usage_records" },
        .plan = "query:table=usage_records:pred=0:array_any=0:in=0:json_path_eq=0:json_contains=0:json_exists=0:array_contains=0:array_eq=0:text_patterns=0:expr_pred=0:expr_or=0:expr_not=0:select=1:order=0:limit=none",
        .resolver_row_json = "{\"id\":\"u1\",\"status\":\"old\"}",
        .resolver_version = 17,
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "plain insert resolver hint",
        .sql = "INSERT INTO usage_records (id, status) VALUES ('u1', 'active') RETURNING id",
        .family = .insert,
        .summary = .{ .table_name = "usage_records", .returning = 1 },
        .plan = "insert:table=usage_records:writes=1:transforms=0:ops=0:deletes=0:returning_rows=1:returning_expr=0",
        .resolver_row_json = "{\"id\":\"u1\",\"status\":\"old\"}",
        .resolver_version = 17,
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "resolver version without row",
        .sql = "INSERT INTO usage_records (id, status) VALUES ('u1', 'active') ON CONFLICT (id) DO UPDATE SET status = excluded.status RETURNING id",
        .family = .insert,
        .summary = .{ .table_name = "usage_records", .operations = 1, .returning = 1 },
        .plan = "insert:table=usage_records:writes=0:transforms=1:ops=1:deletes=0:returning_rows=1:returning_expr=0:op_set=1",
        .resolver_version = 17,
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "resolver miss with row",
        .sql = "INSERT INTO usage_records (id, status) VALUES ('u1', 'active') ON CONFLICT DO NOTHING RETURNING id",
        .family = .insert,
        .summary = .{ .table_name = "usage_records", .returning = 1 },
        .plan = "insert:table=usage_records:writes=1:transforms=0:ops=0:deletes=0:returning_rows=1:returning_expr=0",
        .resolver_row_json = "{\"id\":\"u1\",\"status\":\"old\"}",
        .resolver_version = 17,
        .resolver_exists = false,
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "non-object resolver row",
        .sql = "INSERT INTO usage_records (id, status) VALUES ('u1', 'active') ON CONFLICT (id) DO UPDATE SET status = excluded.status RETURNING id",
        .family = .insert,
        .summary = .{ .table_name = "usage_records", .operations = 1, .returning = 1 },
        .plan = "insert:table=usage_records:writes=0:transforms=1:ops=1:deletes=0:returning_rows=1:returning_expr=0:op_set=1",
        .resolver_row_json = "[\"u1\"]",
        .resolver_version = 17,
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "params without placeholder",
        .sql = "SELECT id FROM usage_records",
        .family = .query,
        .summary = .{ .table_name = "usage_records" },
        .plan = "query:table=usage_records",
        .params = &.{.{ .string = "u1" }},
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "placeholder without params",
        .sql = "SELECT id FROM usage_records WHERE id = $1",
        .family = .query,
        .summary = .{ .table_name = "usage_records" },
        .plan = "query:table=usage_records",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "malformed placeholder suffix",
        .sql = "SELECT id FROM usage_records WHERE id = $1abc",
        .family = .query,
        .summary = .{ .table_name = "usage_records" },
        .plan = "query:table=usage_records",
        .params = &.{.{ .string = "u1" }},
    }, &seen, alloc));

    try validateAppParityFixtureMetadata(.{
        .name = "literal placeholder text ignored",
        .sql = "SELECT '$1' AS literal, id FROM usage_records -- $2\nWHERE id = $1 /* $3 */",
        .family = .query,
        .summary = .{ .table_name = "usage_records" },
        .plan = "query:table=usage_records",
        .params = &.{.{ .string = "u1" }},
    }, &seen, alloc);

    try validateAppParityFixtureMetadata(.{
        .name = "dollar quoted placeholder text ignored",
        .sql = "SELECT $$ $1 $$ AS literal, $tag$ $2abc $tag$ AS tagged, id FROM usage_records",
        .family = .query,
        .summary = .{ .table_name = "usage_records" },
        .plan = "query:table=usage_records",
    }, &seen, alloc);

    try validateAppParityFixtureMetadata(.{
        .name = "malformed placeholder suffix ignored in comment",
        .sql = "SELECT id FROM usage_records /* $1abc */ -- $2abc",
        .family = .query,
        .summary = .{ .table_name = "usage_records" },
        .plan = "query:table=usage_records",
    }, &seen, alloc);

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "prepare statement placeholder mismatch",
        .sql = "PREPARE usage_plan(text) AS SELECT id FROM usage_records WHERE status = 'open'",
        .family = .ddl,
        .summary = .{ .ddl_tag = .prepare_statement, .table_name = "usage_plan", .operations = 1 },
        .plan = "ddl:prepare_statement:name=usage_plan:params=1:subject=read:statement=read",
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "extra bind param",
        .sql = "SELECT id FROM usage_records WHERE id = $1",
        .family = .query,
        .summary = .{ .table_name = "usage_records" },
        .plan = "query:table=usage_records",
        .params = &.{ .{ .string = "u1" }, .{ .string = "unused" } },
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "skipped bind param",
        .sql = "SELECT id FROM usage_records WHERE id = $2",
        .family = .query,
        .summary = .{ .table_name = "usage_records" },
        .plan = "query:table=usage_records",
        .params = &.{ .{ .string = "unused" }, .{ .string = "u1" } },
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "zero bind param",
        .sql = "SELECT id FROM usage_records WHERE id = $0",
        .family = .query,
        .summary = .{ .table_name = "usage_records" },
        .plan = "query:table=usage_records",
        .params = &.{.{ .string = "u1" }},
    }, &seen, alloc));

    try std.testing.expectError(error.TestUnexpectedResult, expectCombinedQuerySourceSummary(.{
        .row_claim_skip_locked = false,
    }, .{}, .{}));

    try expectCombinedQuerySourceSummary(.{
        .row_claim_skip_locked = true,
    }, .{
        .row_claim = .{ .skip_locked = true },
    }, .{});

    try validateAppParityFixtureMetadata(.{
        .name = "valid unsupported reason",
        .sql = "TRUNCATE usage_records CASCADE",
        .family = .unsupported_write,
        .classification_reason = "multi_table_generation_barrier",
        .plan = "unsupported:write:requires=multi_table_generation_barrier",
    }, &seen, alloc);

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "unsupported with adapter noop reason",
        .sql = "TRUNCATE usage_records CASCADE",
        .family = .unsupported_write,
        .classification_reason = "session_setting",
        .plan = "unsupported:write:requires=session_setting",
    }, &seen, alloc));

    try validateAppParityFixtureMetadata(.{
        .name = "valid adapter noop reason",
        .sql = "SET client_encoding = 'UTF8'",
        .family = .adapter_noop_ddl,
        .classification_reason = "session_setting",
        .plan = "adapter_noop:ddl:reason=session_setting",
    }, &seen, alloc);

    try std.testing.expectError(error.TestUnexpectedResult, validateAppParityFixtureMetadata(.{
        .name = "adapter noop with required feature reason",
        .sql = "SET client_encoding = 'UTF8'",
        .family = .adapter_noop_ddl,
        .classification_reason = "set_operation_plan",
        .plan = "adapter_noop:ddl:reason=set_operation_plan",
    }, &seen, alloc));

    try validateAppParityFixtureMetadata(.{
        .name = "valid unsupported insert setup sql",
        .sql = "INSERT INTO usage_records (id, email) VALUES ('u1', 'a@example.test') ON CONFLICT (email) DO UPDATE SET email = excluded.email",
        .family = .unsupported_insert,
        .classification_reason = "enforced_unique_conflict_target",
        .plan = "unsupported:insert:requires=enforced_unique_conflict_target",
        .apply_setup_sql = &.{
            "CREATE TABLE usage_records (id text PRIMARY KEY, email text);",
            "ALTER TABLE usage_records ADD CONSTRAINT usage_records_email_key UNIQUE (email);",
        },
        .resolver_exists = false,
    }, &seen, alloc);

    try validateAppParityFixtureMetadata(.{
        .name = "valid source schema insert plan",
        .sql = "INSERT INTO usage_records (id) SELECT id FROM archived_records",
        .family = .insert_source,
        .summary = .{ .table_name = "usage_records" },
        .plan = "insert_source:table=usage_records:source_table=archived_records:source_pred=0:source_order=0:source_limit=-1:assignments=1:conflict=0:returning=0:returning_expr=0:returning_all=0",
        .source_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
        ,
    }, &seen, alloc);

    try validateAppParityFixtureMetadata(.{
        .name = "valid applied plan",
        .sql = "CREATE INDEX usage_records_status_idx ON usage_records (status)",
        .family = .ddl,
        .summary = .{ .ddl_tag = .create_index, .table_name = "usage_records" },
        .plan = "ddl:create_index:table=usage_records:columns=1:expr=0:generated_expr=0:where=0:unique=false:if_not_exists=false",
        .apply_setup_sql = &.{"CREATE TABLE usage_records (id text PRIMARY KEY, status text)"},
        .applied_plan = "applied:rebuild=true:validation=false:rewrite=false:building_indexes=1:unvalidated_unique=0:unvalidated_fk=0:unvalidated_check=0:update_policy=0:work_items=1:work=rebuild/table/derived_artifacts",
    }, &seen, alloc);

    try validateAppParityFixtureMetadata(.{
        .name = "valid drop-table applied plan",
        .sql = "DROP TABLE usage_records",
        .family = .ddl,
        .summary = .{ .ddl_tag = .drop_table, .table_name = "usage_records" },
        .plan = "ddl:drop_table:table=usage_records:if_exists=false:cascade=false",
        .apply_setup_sql = &.{"CREATE TABLE usage_records (id text PRIMARY KEY, status text)"},
        .applied_plan = "applied:drop_table:rebuild=false:validation=false:rewrite=false:work_items=0:work=none",
    }, &seen, alloc);

    try validateAppParityFixtureMetadata(.{
        .name = "valid resolver miss",
        .sql = "INSERT INTO usage_records (id, status) VALUES ('u1', 'active') ON CONFLICT DO NOTHING RETURNING id",
        .family = .insert,
        .summary = .{ .table_name = "usage_records", .returning = 1 },
        .plan = "insert:table=usage_records:writes=1:transforms=0:ops=0:deletes=0:returning_rows=1:returning_expr=0",
        .returning_rows = &.{"{\"id\":\"u1\"}"},
        .resolver_exists = false,
    }, &seen, alloc);

    try validateAppParityFixtureMetadata(.{
        .name = "valid params with placeholder",
        .sql = "SELECT id FROM usage_records WHERE id = $1",
        .family = .query,
        .summary = .{ .table_name = "usage_records" },
        .plan = "query:table=usage_records",
        .params = &.{.{ .string = "u1" }},
    }, &seen, alloc);
}

const AppParityCorpusCoverage = sql_adapter.AppParityCorpusCoverage;
const app_parity_default_schema_json = sql_adapter.app_parity_default_schema_json;

const SqlValue = sql_adapter.SqlValue;
const LoweredExplainPlan = sql_adapter.LoweredExplainPlan;
const LoweredReadPlan = sql_adapter.LoweredReadPlan;
const LoweredWritePlan = sql_adapter.LoweredWritePlan;
const MergeExecutionTargetRow = sql_adapter.MergeExecutionTargetRow;

const appendNonZeroU32FingerprintAlloc = sql_adapter.appendNonZeroU32FingerprintAlloc;
const appendTrueBoolFingerprintAlloc = sql_adapter.appendTrueBoolFingerprintAlloc;
const applyLogicalDdlPlanToSchemaJsonAlloc = sql_adapter.applyLogicalDdlPlanToSchemaJsonAlloc;
const buildMergeMutationBatchAlloc = sql_adapter.buildMergeMutationBatchAlloc;
const buildMergeMutationBatchFromDbAcrossRangesAlloc = sql_adapter_runtime.buildMergeMutationBatchFromDbAcrossRangesAlloc;
const buildMergeMutationBatchFromDbsAcrossRangesAlloc = sql_adapter_runtime.buildMergeMutationBatchFromDbsAcrossRangesAlloc;
const bulkSqlIoExecutionFingerprintAlloc = sql_adapter.bulkSqlIoExecutionFingerprintAlloc;
const bulkSqlIoExecutionPlanFromDdlPlan = sql_adapter.bulkSqlIoExecutionPlanFromDdlPlan;
const createIndexPlanGeneratedExpressionCount = sql_adapter.createIndexPlanGeneratedExpressionCount;
const ddlAppliedFingerprintAlloc = sql_adapter.ddlAppliedFingerprintAlloc;
const ddlFingerprintAlloc = sql_adapter.ddlFingerprintAlloc;
const lowerDeleteParsedSqlAlloc = sql_adapter_runtime.lowerDeleteParsedSqlAlloc;
const lowerDeleteJoinedMutationSourceWithSchemasAlloc = sql_adapter_runtime.lowerDeleteJoinedMutationSourceWithSchemasAlloc;
const lowerDeleteJoinedMutationSourceWithSchemasParsedSqlAlloc = sql_adapter_runtime.lowerDeleteJoinedMutationSourceWithSchemasParsedSqlAlloc;
const lowerExplainPlanWithOptionsCatalogAndFunctionBindingsParsedSqlAlloc = sql_adapter_runtime.lowerExplainPlanWithOptionsCatalogAndFunctionBindingsParsedSqlAlloc;
const lowerInsertWithResolverParsedSqlAlloc = sql_adapter_runtime.lowerInsertWithResolverParsedSqlAlloc;
const lowerInsertWithResolverStrictParsedSqlAlloc = sql_adapter_runtime.lowerInsertWithResolverStrictParsedSqlAlloc;
const lowerMergeMutationPlanParsedSqlAlloc = sql_adapter_runtime.lowerMergeMutationPlanParsedSqlAlloc;
const lowerQueryPlanWithFunctionBindingsParsedSqlAlloc = sql_adapter_runtime.lowerQueryPlanWithFunctionBindingsParsedSqlAlloc;
const lowerReadPlanAlloc = sql_adapter_runtime.lowerReadPlanAlloc;
const lowerReadPlanWithCatalogAndFunctionBindingsParsedSqlAlloc = sql_adapter_runtime.lowerReadPlanWithCatalogAndFunctionBindingsParsedSqlAlloc;
const lowerReadPlanWithCatalogAlloc = sql_adapter_runtime.lowerReadPlanWithCatalogAlloc;
const lowerReadPlanWithFunctionBindingsParsedSqlAlloc = sql_adapter_runtime.lowerReadPlanWithFunctionBindingsParsedSqlAlloc;
const lowerRelationPopulationPlanWithCatalogAndFunctionBindingsParsedSqlAlloc = sql_adapter_runtime.lowerRelationPopulationPlanWithCatalogAndFunctionBindingsParsedSqlAlloc;
const lowerSelectParsedSqlAlloc = sql_adapter_runtime.lowerSelectParsedSqlAlloc;
const lowerUpdateJoinedMutationSourceWithSchemasAlloc = sql_adapter_runtime.lowerUpdateJoinedMutationSourceWithSchemasAlloc;
const lowerUpdateJoinedMutationSourceWithSchemasParsedSqlAlloc = sql_adapter_runtime.lowerUpdateJoinedMutationSourceWithSchemasParsedSqlAlloc;
const lowerUpdateMutationSourceParsedSqlAlloc = sql_adapter_runtime.lowerUpdateMutationSourceParsedSqlAlloc;
const lowerUpdateParsedSqlAlloc = sql_adapter_runtime.lowerUpdateParsedSqlAlloc;
const lowerUpdateStrictParsedSqlAlloc = sql_adapter_runtime.lowerUpdateStrictParsedSqlAlloc;
const lowerWritePlanAlloc = sql_adapter.lowerWritePlanAlloc;
const lowerWritePlanWithCatalogAlloc = sql_adapter.lowerWritePlanWithCatalogAlloc;
const lowerWritePlanWithCatalogParsedSqlAlloc = sql_adapter.lowerWritePlanWithCatalogParsedSqlAlloc;
const lowerWritePlanParsedSqlAlloc = sql_adapter.lowerWritePlanParsedSqlAlloc;
const sequenceOptionCount = sql_adapter.sequenceOptionCount;
const preparedTransactionRecoveryIntentFromPlan = sql_adapter.preparedTransactionRecoveryIntentFromPlan;
const preparedTransactionRecoveryFingerprintAlloc = sql_adapter.preparedTransactionRecoveryFingerprintAlloc;

test "postgres sql adapter classifies application parity corpus" {
    const alloc = std.testing.allocator;
    const schema_json = app_parity_default_schema_json;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var resolver_ctx = TestPrimaryResolver{
        .row_json = "{\"id\":\"u1\",\"tenant_id\":\"t1\",\"status\":\"queued\",\"quantity\":1,\"amount\":5,\"priority\":1,\"updated_at_ns\":1,\"metadata\":{}}",
        .version = 7,
    };
    const txn_id = [_]u8{ 0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3a, 0x3b, 0x3c, 0x3d, 0x3e, 0x3f };
    const row_claim: db_mod.types.RowClaimRequest = .{
        .mode = .for_update,
        .owner_id = "parity-worker",
        .txn_id = txn_id,
    };

    var external_source = try sql_adapter.parseAppParityExternalSourceCorpusAlloc(alloc);
    defer external_source.deinit(alloc);
    const corpus = external_source.root.entries;
    var required_coverage = try sql_adapter.parseAppParityCoverageRequirementsAlloc(alloc);
    defer required_coverage.deinit(alloc);

    var coverage = AppParityCorpusCoverage{};
    var entry_arena = std.heap.ArenaAllocator.init(alloc);
    defer entry_arena.deinit();
    for (corpus) |entry| {
        _ = entry_arena.reset(.retain_capacity);
        const entry_alloc = entry_arena.allocator();
        errdefer std.debug.print("application parity corpus entry failed: {s}\n", .{entry.name});
        try coverage.observe(entry_alloc, entry);
        try expectAppParityCorpusEntry(entry_alloc, schema_json, schema, entry, resolver_ctx.resolver(), row_claim);
    }
    try sql_adapter.expectAppParityCoverageRequirements(coverage, required_coverage.root.required);
}

test "postgres sql adapter checks application parity fixture freshness" {
    const alloc = std.testing.allocator;
    var external_source = try sql_adapter.parseAppParityExternalSourceCorpusAlloc(alloc);
    defer external_source.deinit(alloc);
    try maybeCheckOrPromoteAppParityFixture(
        alloc,
        app_parity_default_schema_json,
        external_source.source_sha256,
        external_source.root.entries,
    );
}

test "postgres sql adapter classifies fixture-backed application parity corpus" {
    const alloc = std.testing.allocator;
    const fixture_json = @embedFile("fixtures/sql_api_parity_corpus.json");
    var parsed_fixture = try std.json.parseFromSlice(std.json.Value, alloc, fixture_json, .{});
    defer parsed_fixture.deinit();

    const fixture_root = try sql_adapter.parseFixtureRootAlloc(alloc, parsed_fixture.value);
    defer sql_adapter.freeFixtureRoot(alloc, fixture_root);
    const source_json = @embedFile("fixtures/sql_api_parity_source_corpus.json");
    const source_sha256 = try sql_adapter.sourceCorpusSha256HexAlloc(alloc, source_json);
    defer alloc.free(source_sha256);
    try std.testing.expectEqualStrings(source_sha256, fixture_root.source_sha256);
    const skipped_entries = fixture_root.skipped_entries;
    const schema_json = fixture_root.schema_json;
    try std.testing.expectEqual(@as(usize, 0), skipped_entries.len);

    var parsed_schema = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer runtime_schema.freeSchema(alloc, schema);

    var resolver_ctx = TestPrimaryResolver{
        .row_json = "{\"id\":\"u1\",\"tenant_id\":\"t1\",\"status\":\"queued\",\"quantity\":1,\"amount\":5,\"priority\":1,\"updated_at_ns\":1,\"metadata\":{}}",
        .version = 7,
    };
    const txn_id = [_]u8{ 0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3a, 0x3b, 0x3c, 0x3d, 0x3e, 0x3f };
    const row_claim: db_mod.types.RowClaimRequest = .{
        .mode = .for_update,
        .owner_id = "fixture-parity-worker",
        .txn_id = txn_id,
    };
    var seen_names = std.StringHashMapUnmanaged(void){};
    defer seen_names.deinit(alloc);
    var seen_skipped_names = std.StringHashMapUnmanaged(void){};
    defer seen_skipped_names.deinit(alloc);
    var required_coverage = try sql_adapter.parseAppParityCoverageRequirementsAlloc(alloc);
    defer required_coverage.deinit(alloc);
    for (skipped_entries) |name| {
        if (name.len == 0 or seen_skipped_names.contains(name)) return error.TestUnexpectedResult;
        try seen_skipped_names.put(alloc, name, {});
    }
    var coverage = AppParityCorpusCoverage{};
    var entry_arena = std.heap.ArenaAllocator.init(alloc);
    defer entry_arena.deinit();

    for (fixture_root.entries) |entry_value| {
        _ = entry_arena.reset(.retain_capacity);
        const entry_alloc = entry_arena.allocator();
        const entry = try sql_adapter.parseFixtureEntryAlloc(entry_alloc, entry_value);
        errdefer std.debug.print("fixture parity corpus entry failed: {s}\n", .{entry.name});
        if (seen_skipped_names.contains(entry.name)) return error.TestUnexpectedResult;
        try validateAppParityFixtureMetadataWithBaseSchema(entry, schema_json, &seen_names, alloc);
        try coverage.observe(entry_alloc, entry);
        try expectAppParityCorpusEntry(entry_alloc, schema_json, schema, entry, resolver_ctx.resolver(), row_claim);
    }
    try sql_adapter.expectAppParityCoverageRequirements(coverage, required_coverage.root.required);
}

fn expectSqlTemporalJsonNumberEqual(expected: f64, actual: std.json.Value) !void {
    switch (actual) {
        .integer => |value| try std.testing.expectEqual(expected, @as(f64, @floatFromInt(value))),
        .float => |value| try std.testing.expectEqual(expected, value),
        else => return error.TestExpectedEqual,
    }
}

fn expectSqlTemporalPriceRow(
    alloc: std.mem.Allocator,
    row_json: []const u8,
    sku: []const u8,
    valid_from: f64,
    valid_to: f64,
    price: f64,
) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, row_json, .{});
    defer parsed.deinit();
    const object = parsed.value.object;
    try std.testing.expectEqualStrings(sku, object.get("sku").?.string);
    try expectSqlTemporalJsonNumberEqual(valid_from, object.get("valid_from").?);
    try expectSqlTemporalJsonNumberEqual(valid_to, object.get("valid_to").?);
    try expectSqlTemporalJsonNumberEqual(price, object.get("price").?);
}

test "postgres sql adapter temporal portion mutation sources execute through relational storage" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"sku":{"type":"keyword"},"valid_from":{"type":"numeric"},"valid_to":{"type":"numeric"},"price":{"type":"numeric"}},"required":["sku","valid_from","valid_to"],"additionalProperties":false}}},"periods":[{"name":"valid_time","start_column":"valid_from","end_column":"valid_to"}],"primary_key":{"columns":["sku"],"without_overlaps_period":"valid_time"}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/sql-temporal-portion-execution", .{tmp.sub_path});
    defer alloc.free(path);

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();
    try db.applyTableSchemaJson(alloc, schema_json, .{});

    try db.batch(.{
        .writes = &.{
            .{ .key = "price:a:v1", .value = "{\"sku\":\"sku:a\",\"valid_from\":0,\"valid_to\":10,\"price\":10}" },
            .{ .key = "price:b:v1", .value = "{\"sku\":\"sku:b\",\"valid_from\":0,\"valid_to\":10,\"price\":20}" },
        },
        .sync_level = .write,
    });

    const update_txn = try db.beginTransaction(2_000);
    var update_committed = false;
    defer if (!update_committed) db.abortTransaction(update_txn, 2_001) catch {};
    const update_claim: db_mod.types.RowClaimRequest = .{
        .mode = .for_update,
        .owner_id = "sql-temporal-update",
        .txn_id = update_txn,
    };
    var update_plan = try lowerWritePlanAlloc(
        alloc,
        "UPDATE prices FOR PORTION OF valid_time FROM 3 TO 7 SET price = 99 WHERE sku = 'sku:a' FOR UPDATE RETURNING *",
        schema,
        &.{},
        .{ .row_claim = update_claim },
    );
    defer update_plan.deinit(alloc);

    switch (update_plan) {
        .update_source => |update_source| {
            var result = try db.mutateRelationalRowsFromSource(alloc, schema, update_source.mutation.req);
            defer result.deinit(alloc);
            try std.testing.expectEqual(@as(u32, 1), result.matched);
            try std.testing.expectEqual(@as(u32, 1), result.staged);
            try std.testing.expectEqual(@as(usize, 1), result.returning_rows.len);
            try expectSqlTemporalPriceRow(alloc, result.returning_rows[0], "sku:a", 3, 7, 99);
        },
        else => return error.TestUnexpectedResult,
    }
    try db.commitTransaction(update_txn, 2_010);
    update_committed = true;

    const order_by = [_]db_mod.types.RelationalRowsQueryOrder{.{
        .field = "valid_from",
        .direction = .asc,
    }};
    const sku_a_predicates = [_]runtime_schema.RelationalCheck{.{
        .name = "",
        .field = "sku",
        .op = .eq,
        .value_json = "\"sku:a\"",
    }};
    var sku_a_rows = try db.queryRelationalRows(alloc, schema, .{
        .predicates = sku_a_predicates[0..],
        .select_all = true,
        .order_by = order_by[0..],
    });
    defer sku_a_rows.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 3), sku_a_rows.total);
    try std.testing.expectEqual(@as(usize, 3), sku_a_rows.rows.len);
    try expectSqlTemporalPriceRow(alloc, sku_a_rows.rows[0], "sku:a", 0, 3, 10);
    try expectSqlTemporalPriceRow(alloc, sku_a_rows.rows[1], "sku:a", 3, 7, 99);
    try expectSqlTemporalPriceRow(alloc, sku_a_rows.rows[2], "sku:a", 7, 10, 10);

    const delete_txn = try db.beginTransaction(2_020);
    var delete_committed = false;
    defer if (!delete_committed) db.abortTransaction(delete_txn, 2_021) catch {};
    const delete_claim: db_mod.types.RowClaimRequest = .{
        .mode = .for_update,
        .owner_id = "sql-temporal-delete",
        .txn_id = delete_txn,
    };
    var delete_plan = try lowerWritePlanAlloc(
        alloc,
        "DELETE FROM prices FOR PORTION OF valid_time FROM 2 TO 8 WHERE sku = 'sku:b' FOR UPDATE RETURNING *",
        schema,
        &.{},
        .{ .row_claim = delete_claim },
    );
    defer delete_plan.deinit(alloc);

    switch (delete_plan) {
        .delete_source => |delete_source| {
            var result = try db.mutateRelationalRowsFromSource(alloc, schema, delete_source.mutation.req);
            defer result.deinit(alloc);
            try std.testing.expectEqual(@as(u32, 1), result.matched);
            try std.testing.expectEqual(@as(u32, 1), result.staged);
            try std.testing.expectEqual(@as(usize, 1), result.returning_rows.len);
            try expectSqlTemporalPriceRow(alloc, result.returning_rows[0], "sku:b", 2, 8, 20);
        },
        else => return error.TestUnexpectedResult,
    }
    try db.commitTransaction(delete_txn, 2_030);
    delete_committed = true;

    const sku_b_predicates = [_]runtime_schema.RelationalCheck{.{
        .name = "",
        .field = "sku",
        .op = .eq,
        .value_json = "\"sku:b\"",
    }};
    var sku_b_rows = try db.queryRelationalRows(alloc, schema, .{
        .predicates = sku_b_predicates[0..],
        .select_all = true,
        .order_by = order_by[0..],
    });
    defer sku_b_rows.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 2), sku_b_rows.total);
    try std.testing.expectEqual(@as(usize, 2), sku_b_rows.rows.len);
    try expectSqlTemporalPriceRow(alloc, sku_b_rows.rows[0], "sku:b", 0, 2, 20);
    try expectSqlTemporalPriceRow(alloc, sku_b_rows.rows[1], "sku:b", 8, 10, 20);
}

test "postgres sql adapter typed write plans execute through relational storage" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"organization_id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/sql-write-plan-execution", .{tmp.sub_path});
    defer alloc.free(path);

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();
    try db.applyTableSchemaJson(alloc, schema_json, .{});

    var no_existing_primary = TestPrimaryResolver{ .row_json = "", .version = 0, .exists = false };
    var insert_plan = try lowerWritePlanAlloc(
        alloc,
        "INSERT INTO usage_records (id, status, organization_id) VALUES ('u1', 'queued', 'o1'), ('u2', 'queued', 'o1'), ('u3', 'queued', 'o2') RETURNING id, status",
        schema,
        &.{},
        .{ .unique_resolver = no_existing_primary.resolver() },
    );
    defer insert_plan.deinit(alloc);

    switch (insert_plan) {
        .insert => |insert| {
            try std.testing.expectEqual(@as(u32, 3), insert.batch.inserted);
            try std.testing.expectEqual(@as(usize, 3), insert.batch.returning_rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"u1\",\"status\":\"queued\"}", insert.batch.returning_rows[0]);
            try std.testing.expectEqualStrings("{\"id\":\"u2\",\"status\":\"queued\"}", insert.batch.returning_rows[1]);
            try std.testing.expectEqualStrings("{\"id\":\"u3\",\"status\":\"queued\"}", insert.batch.returning_rows[2]);
            try db.batch(insert.batch.req);
        },
        else => return error.TestUnexpectedResult,
    }

    const update_txn = try db.beginTransaction(2_001);
    var update_committed = false;
    defer if (!update_committed) db.abortTransaction(update_txn, 2_002) catch {};
    const update_claim: db_mod.types.RowClaimRequest = .{
        .mode = .for_update,
        .owner_id = "sql-write-update",
        .txn_id = update_txn,
    };
    var update_plan = try lowerWritePlanAlloc(
        alloc,
        "UPDATE usage_records SET status = 'processed' WHERE organization_id = 'o1' ORDER BY id ASC LIMIT 2 FOR UPDATE SKIP LOCKED RETURNING id, status",
        schema,
        &.{},
        .{ .row_claim = update_claim },
    );
    defer update_plan.deinit(alloc);

    switch (update_plan) {
        .update_source => |update_source| {
            try std.testing.expect(update_source.mutation.req.source.row_claim.?.skip_locked);
            var result = try db.mutateRelationalRowsFromSource(alloc, schema, update_source.mutation.req);
            defer result.deinit(alloc);
            try std.testing.expectEqual(@as(u32, 2), result.matched);
            try std.testing.expectEqual(@as(u32, 2), result.staged);
            try std.testing.expectEqual(@as(usize, 2), result.returning_rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"u1\",\"status\":\"processed\"}", result.returning_rows[0]);
            try std.testing.expectEqualStrings("{\"id\":\"u2\",\"status\":\"processed\"}", result.returning_rows[1]);
        },
        else => return error.TestUnexpectedResult,
    }
    try db.commitTransaction(update_txn, 2_010);
    update_committed = true;

    const delete_txn = try db.beginTransaction(2_101);
    var delete_committed = false;
    defer if (!delete_committed) db.abortTransaction(delete_txn, 2_102) catch {};
    const delete_claim: db_mod.types.RowClaimRequest = .{
        .mode = .for_update,
        .owner_id = "sql-write-delete",
        .txn_id = delete_txn,
    };
    var delete_plan = try lowerWritePlanAlloc(
        alloc,
        "DELETE FROM usage_records WHERE status = 'processed' ORDER BY id ASC FOR UPDATE RETURNING id",
        schema,
        &.{},
        .{ .row_claim = delete_claim },
    );
    defer delete_plan.deinit(alloc);

    switch (delete_plan) {
        .delete_source => |delete_source| {
            var result = try db.mutateRelationalRowsFromSource(alloc, schema, delete_source.mutation.req);
            defer result.deinit(alloc);
            try std.testing.expectEqual(@as(u32, 2), result.matched);
            try std.testing.expectEqual(@as(u32, 2), result.staged);
            try std.testing.expectEqual(@as(usize, 2), result.returning_rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"u1\"}", result.returning_rows[0]);
            try std.testing.expectEqualStrings("{\"id\":\"u2\"}", result.returning_rows[1]);
        },
        else => return error.TestUnexpectedResult,
    }
    try db.commitTransaction(delete_txn, 2_110);
    delete_committed = true;

    const select = [_][]const u8{ "id", "status" };
    const order_by = [_]db_mod.types.RelationalRowsQueryOrder{.{
        .field = "id",
        .direction = .asc,
    }};
    var rows = try db.queryRelationalRows(alloc, schema, .{
        .select = select[0..],
        .order_by = order_by[0..],
    });
    defer rows.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), rows.total);
    try std.testing.expectEqual(@as(usize, 1), rows.rows.len);
    try std.testing.expectEqualStrings("{\"id\":\"u3\",\"status\":\"queued\"}", rows.rows[0]);

    var insert_source_plan = try lowerWritePlanAlloc(
        alloc,
        "INSERT INTO usage_records (id, status, organization_id) SELECT id || '_copy' AS id, status, organization_id FROM usage_records WHERE id = 'u3' RETURNING *, lower(status) AS status_key",
        schema,
        &.{},
        .{ .unique_resolver = no_existing_primary.resolver() },
    );
    defer insert_source_plan.deinit(alloc);

    switch (insert_source_plan) {
        .insert_source => |insert_source| {
            try std.testing.expect(insert_source.insert_source.req.returning_all);
            try std.testing.expectEqual(@as(usize, 1), insert_source.insert_source.req.returning_expressions.len);
            const u3_key = try relational_rows.physicalPrimaryKeyFromRowJsonAlloc(alloc, schema, "{\"id\":\"u3\"}");
            defer alloc.free(u3_key);
            const source_ranges = [_]db_mod.types.RelationalRowsDocKeyRange{.{
                .start = u3_key,
                .end = "",
            }};

            var batch = try relational_rows.buildRowsInsertSourceBatchFromDbAcrossRangesAlloc(
                alloc,
                &db,
                insert_source.table_name,
                schema,
                schema,
                insert_source.insert_source.req,
                source_ranges[0..],
                no_existing_primary.resolver(),
            );
            defer batch.deinit(alloc);
            try std.testing.expectEqual(@as(u32, 1), batch.inserted);
            try std.testing.expectEqual(@as(usize, 1), batch.returning_rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"u3_copy\",\"status\":\"queued\",\"organization_id\":\"o2\",\"status_key\":\"queued\"}", batch.returning_rows[0]);
            try db.batch(batch.req);
        },
        else => return error.TestUnexpectedResult,
    }

    const queued_cte_predicates = [_]runtime_schema.RelationalCheck{.{
        .name = "",
        .field = "id",
        .op = .eq,
        .value_json = "\"u3_copy\"",
    }};
    const queued_cte_select = [_][]const u8{ "id", "status", "organization_id" };
    const id_copy_operands = [_]db_mod.types.RelationalRowsExpression{
        .{ .kind = .field, .field = "id" },
        .{ .kind = .value, .value_json = "\"_cte_copy\"" },
    };
    const upper_status_operands = [_]db_mod.types.RelationalRowsExpression{.{
        .kind = .field,
        .field = "status",
    }};
    const cte_assignments = [_]db_mod.types.RelationalRowsExpressionAssignment{
        .{ .field = "id", .expression = .{ .kind = .concat, .operands = id_copy_operands[0..] } },
        .{ .field = "status", .expression = .{ .kind = .upper, .operands = upper_status_operands[0..] } },
        .{ .field = "organization_id", .expression = .{ .kind = .field, .field = "organization_id" } },
    };
    const cte_returning = [_][]const u8{ "id", "status" };
    const u2_key = try relational_rows.physicalPrimaryKeyFromRowJsonAlloc(alloc, schema, "{\"id\":\"u2\"}");
    defer alloc.free(u2_key);
    const cte_ranges = [_]db_mod.types.RelationalRowsDocKeyRange{.{
        .start = u2_key,
        .end = "",
    }};
    const cte_base_preimages = try db.collectRelationalRowsPreimagesAcrossRangesAlloc(alloc, schema, .{ .select_all = true }, cte_ranges[0..]);
    defer db_mod.types.freeRelationalRowsCollectedRows(alloc, cte_base_preimages);
    try std.testing.expect(cte_base_preimages.len > 0);
    const insert_source_cte_plan = db_mod.types.RelationalRowsInsertSourcePlan{
        .ctes = &.{.{
            .name = "queued_sources",
            .query = .{
                .predicates = queued_cte_predicates[0..],
                .select = queued_cte_select[0..],
            },
        }},
        .ranges = cte_ranges[0..],
        .insert_source = .{
            .source = .{ .source_cte = "queued_sources", .select_all = true },
            .assignments = cte_assignments[0..],
            .returning = cte_returning[0..],
        },
    };
    var cte_batch = try relational_rows.buildRowsInsertSourcePlanBatchFromDbAlloc(
        alloc,
        &db,
        "usage_records",
        schema,
        schema,
        insert_source_cte_plan,
        no_existing_primary.resolver(),
    );
    defer cte_batch.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), cte_batch.inserted);
    try std.testing.expectEqual(@as(usize, 1), cte_batch.returning_rows.len);
    try std.testing.expectEqualStrings("{\"id\":\"u3_copy_cte_copy\",\"status\":\"QUEUED\"}", cte_batch.returning_rows[0]);
    try db.batch(cte_batch.req);

    const u3_copy_key = try relational_rows.physicalPrimaryKeyFromRowJsonAlloc(alloc, schema, "{\"id\":\"u3_copy\"}");
    defer alloc.free(u3_copy_key);
    const u3_copy_version = try db.getTimestamp(alloc, u3_copy_key);
    var insert_source_conflict_resolver = TestPrimaryResolver{
        .row_json = "{\"id\":\"u3_copy\",\"status\":\"queued\",\"organization_id\":\"o2\"}",
        .version = u3_copy_version,
    };
    var insert_source_conflict_plan = try lowerWritePlanAlloc(
        alloc,
        "INSERT INTO usage_records (id, status, organization_id) SELECT id || '_copy' AS id, upper(status) AS status, organization_id FROM usage_records WHERE id = 'u3' ON CONFLICT (id) DO UPDATE SET status = excluded.status RETURNING *, lower(status) AS status_key",
        schema,
        &.{},
        .{ .unique_resolver = insert_source_conflict_resolver.resolver() },
    );
    defer insert_source_conflict_plan.deinit(alloc);

    switch (insert_source_conflict_plan) {
        .insert_source => |insert_source| {
            try std.testing.expect(insert_source.insert_source.req.on_conflict != null);
            try std.testing.expectEqual(db_mod.types.RelationalRowsConflictAction.update, insert_source.insert_source.req.on_conflict.?.action);
            try std.testing.expect(insert_source.insert_source.req.returning_all);
            try std.testing.expectEqual(@as(usize, 1), insert_source.insert_source.req.returning_expressions.len);
            var source_result = try db.queryRelationalRows(alloc, schema, insert_source.insert_source.req.source);
            defer source_result.deinit(alloc);
            try std.testing.expectEqual(@as(u32, 1), source_result.total);

            var batch = try relational_rows.buildRowsInsertSourceBatchAlloc(
                alloc,
                insert_source.table_name,
                schema,
                insert_source.insert_source.req,
                source_result.rows,
                insert_source_conflict_resolver.resolver(),
            );
            defer batch.deinit(alloc);
            try std.testing.expectEqual(@as(u32, 0), batch.inserted);
            try std.testing.expectEqual(@as(u32, 1), batch.transformed);
            try std.testing.expectEqual(@as(usize, 1), batch.returning_rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"u3_copy\",\"status\":\"QUEUED\",\"organization_id\":\"o2\",\"status_key\":\"queued\"}", batch.returning_rows[0]);
            try db.batch(batch.req);
        },
        else => return error.TestUnexpectedResult,
    }

    const u3_copy_do_nothing_version = try db.getTimestamp(alloc, u3_copy_key);
    var insert_source_existing_nothing_resolver = TestPrimaryResolver{
        .row_json = "{\"id\":\"u3_copy\",\"status\":\"QUEUED\",\"organization_id\":\"o2\"}",
        .version = u3_copy_do_nothing_version,
    };
    var insert_source_existing_nothing_plan = try lowerWritePlanAlloc(
        alloc,
        "INSERT INTO usage_records (id, status, organization_id) SELECT id || '_copy' AS id, lower(status) AS status, organization_id FROM usage_records WHERE id = 'u3' ON CONFLICT (id) DO NOTHING RETURNING *, lower(status) AS status_key",
        schema,
        &.{},
        .{ .unique_resolver = insert_source_existing_nothing_resolver.resolver() },
    );
    defer insert_source_existing_nothing_plan.deinit(alloc);

    switch (insert_source_existing_nothing_plan) {
        .insert_source => |insert_source| {
            const conflict = insert_source.insert_source.req.on_conflict orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(db_mod.types.RelationalRowsConflictAction.nothing, conflict.action);
            try std.testing.expect(insert_source.insert_source.req.returning_all);
            try std.testing.expectEqual(@as(usize, 1), insert_source.insert_source.req.returning_expressions.len);
            var source_result = try db.queryRelationalRows(alloc, schema, insert_source.insert_source.req.source);
            defer source_result.deinit(alloc);
            try std.testing.expectEqual(@as(u32, 1), source_result.total);

            var batch = try relational_rows.buildRowsInsertSourceBatchAlloc(
                alloc,
                insert_source.table_name,
                schema,
                insert_source.insert_source.req,
                source_result.rows,
                insert_source_existing_nothing_resolver.resolver(),
            );
            defer batch.deinit(alloc);
            try std.testing.expectEqual(@as(u32, 0), batch.inserted);
            try std.testing.expectEqual(@as(u32, 0), batch.transformed);
            try std.testing.expectEqual(@as(usize, 0), batch.returning_rows.len);
            try db.batch(batch.req);
        },
        else => return error.TestUnexpectedResult,
    }

    const u3_copy_after_conflict_version = try db.getTimestamp(alloc, u3_copy_key);
    var insert_source_conflict_skip_resolver = TestPrimaryResolver{
        .row_json = "{\"id\":\"u3_copy\",\"status\":\"QUEUED\",\"organization_id\":\"o2\"}",
        .version = u3_copy_after_conflict_version,
    };
    var insert_source_conflict_skip_plan = try lowerWritePlanAlloc(
        alloc,
        "INSERT INTO usage_records (id, status, organization_id) SELECT id || '_copy' AS id, lower(status) AS status, organization_id FROM usage_records WHERE id = 'u3' ON CONFLICT (id) DO UPDATE SET status = excluded.status WHERE excluded.status = status RETURNING *, lower(status) AS status_key",
        schema,
        &.{},
        .{ .unique_resolver = insert_source_conflict_skip_resolver.resolver() },
    );
    defer insert_source_conflict_skip_plan.deinit(alloc);

    switch (insert_source_conflict_skip_plan) {
        .insert_source => |insert_source| {
            const conflict = insert_source.insert_source.req.on_conflict orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(db_mod.types.RelationalRowsConflictAction.update, conflict.action);
            try std.testing.expect(conflict.where_expression != null);
            try std.testing.expect(insert_source.insert_source.req.returning_all);
            try std.testing.expectEqual(@as(usize, 1), insert_source.insert_source.req.returning_expressions.len);
            var source_result = try db.queryRelationalRows(alloc, schema, insert_source.insert_source.req.source);
            defer source_result.deinit(alloc);
            try std.testing.expectEqual(@as(u32, 1), source_result.total);

            var batch = try relational_rows.buildRowsInsertSourceBatchAlloc(
                alloc,
                insert_source.table_name,
                schema,
                insert_source.insert_source.req,
                source_result.rows,
                insert_source_conflict_skip_resolver.resolver(),
            );
            defer batch.deinit(alloc);
            try std.testing.expectEqual(@as(u32, 0), batch.inserted);
            try std.testing.expectEqual(@as(u32, 0), batch.transformed);
            try std.testing.expectEqual(@as(usize, 0), batch.returning_rows.len);
            try db.batch(batch.req);
        },
        else => return error.TestUnexpectedResult,
    }

    try db.batch(.{
        .writes = &.{
            .{ .key = "row:dup_a", .value = "{\"id\":\"dup_a\",\"status\":\"dupe\",\"organization_id\":\"dup_target\"}" },
            .{ .key = "row:dup_b", .value = "{\"id\":\"dup_b\",\"status\":\"dupe\",\"organization_id\":\"dup_target\"}" },
        },
        .sync_level = .write,
    });

    var insert_source_duplicate_nothing_plan = try lowerWritePlanAlloc(
        alloc,
        "INSERT INTO usage_records (id, status, organization_id) SELECT organization_id AS id, status, organization_id FROM usage_records WHERE id IN ('dup_a', 'dup_b') ORDER BY id ASC ON CONFLICT (id) DO NOTHING RETURNING id, status",
        schema,
        &.{},
        .{ .unique_resolver = no_existing_primary.resolver() },
    );
    defer insert_source_duplicate_nothing_plan.deinit(alloc);

    switch (insert_source_duplicate_nothing_plan) {
        .insert_source => |insert_source| {
            try std.testing.expect(insert_source.insert_source.req.on_conflict != null);
            try std.testing.expectEqual(db_mod.types.RelationalRowsConflictAction.nothing, insert_source.insert_source.req.on_conflict.?.action);
            var source_result = try db.queryRelationalRows(alloc, schema, insert_source.insert_source.req.source);
            defer source_result.deinit(alloc);
            try std.testing.expectEqual(@as(u32, 2), source_result.total);
            try std.testing.expectEqual(@as(usize, 2), source_result.rows.len);

            var batch = try relational_rows.buildRowsInsertSourceBatchAlloc(
                alloc,
                insert_source.table_name,
                schema,
                insert_source.insert_source.req,
                source_result.rows,
                no_existing_primary.resolver(),
            );
            defer batch.deinit(alloc);
            try std.testing.expectEqual(@as(u32, 1), batch.inserted);
            try std.testing.expectEqual(@as(u32, 0), batch.transformed);
            try std.testing.expectEqual(@as(usize, 1), batch.returning_rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"dup_target\",\"status\":\"dupe\"}", batch.returning_rows[0]);
            try db.batch(batch.req);
        },
        else => return error.TestUnexpectedResult,
    }

    const dup_target_key = try relational_rows.physicalPrimaryKeyFromRowJsonAlloc(alloc, schema, "{\"id\":\"dup_target\"}");
    defer alloc.free(dup_target_key);
    const dup_target_version = try db.getTimestamp(alloc, dup_target_key);
    var insert_source_duplicate_update_resolver = TestPrimaryResolver{
        .row_json = "{\"id\":\"dup_target\",\"status\":\"dupe\",\"organization_id\":\"dup_target\"}",
        .version = dup_target_version,
    };
    var insert_source_duplicate_update_plan = try lowerWritePlanAlloc(
        alloc,
        "INSERT INTO usage_records (id, status, organization_id) SELECT organization_id AS id, upper(status) AS status, organization_id FROM usage_records WHERE id IN ('dup_a', 'dup_b') ORDER BY id ASC ON CONFLICT (id) DO UPDATE SET status = excluded.status RETURNING id, status",
        schema,
        &.{},
        .{ .unique_resolver = insert_source_duplicate_update_resolver.resolver() },
    );
    defer insert_source_duplicate_update_plan.deinit(alloc);

    switch (insert_source_duplicate_update_plan) {
        .insert_source => |insert_source| {
            try std.testing.expect(insert_source.insert_source.req.on_conflict != null);
            try std.testing.expectEqual(db_mod.types.RelationalRowsConflictAction.update, insert_source.insert_source.req.on_conflict.?.action);
            var source_result = try db.queryRelationalRows(alloc, schema, insert_source.insert_source.req.source);
            defer source_result.deinit(alloc);
            try std.testing.expectEqual(@as(u32, 2), source_result.total);
            try std.testing.expectEqual(@as(usize, 2), source_result.rows.len);

            try std.testing.expectError(error.InvalidRowsRequest, relational_rows.buildRowsInsertSourceBatchAlloc(
                alloc,
                insert_source.table_name,
                schema,
                insert_source.insert_source.req,
                source_result.rows,
                insert_source_duplicate_update_resolver.resolver(),
            ));
        },
        else => return error.TestUnexpectedResult,
    }

    try db.batch(.{
        .writes = &.{
            .{ .key = "row:s1", .value = "{\"id\":\"s1\",\"kind\":\"source\",\"status\":\"synced\",\"quantity\":42}" },
            .{ .key = "row:s2", .value = "{\"id\":\"s2\",\"kind\":\"source\",\"status\":\"stale\",\"quantity\":7}" },
            .{ .key = "row:t1", .value = "{\"id\":\"t1\",\"kind\":\"target\",\"source_id\":\"s1\",\"status\":\"ready\",\"quantity\":1}" },
            .{ .key = "row:t2", .value = "{\"id\":\"t2\",\"kind\":\"target\",\"source_id\":\"s1\",\"status\":\"expired\",\"quantity\":2}" },
            .{ .key = "row:t3", .value = "{\"id\":\"t3\",\"kind\":\"target\",\"source_id\":\"s2\",\"status\":\"ready\",\"quantity\":3}" },
        },
        .sync_level = .write,
    });

    const joined_update_txn = try db.beginTransaction(3_001);
    var joined_update_committed = false;
    defer if (!joined_update_committed) db.abortTransaction(joined_update_txn, 3_002) catch {};
    const joined_update_claim: db_mod.types.RowClaimRequest = .{
        .mode = .for_update,
        .owner_id = "sql-write-joined-update",
        .txn_id = joined_update_txn,
    };
    var joined_update_plan = try lowerWritePlanAlloc(
        alloc,
        "UPDATE usage_records SET status = 'joined' FROM usage_records AS source WHERE usage_records.id = source.id AND source.id = 't1' FOR UPDATE RETURNING id, status, lower(source.status) AS source_status_key",
        schema,
        &.{},
        .{ .row_claim = joined_update_claim },
    );
    defer joined_update_plan.deinit(alloc);

    switch (joined_update_plan) {
        .update_joined_source => |joined_update| {
            var result = try db.mutateRelationalRowsJoinedSourceAlloc(alloc, schema, joined_update.mutation.req);
            defer result.deinit(alloc);
            try std.testing.expectEqual(@as(u32, 1), result.matched);
            try std.testing.expectEqual(@as(u32, 1), result.staged);
            try std.testing.expectEqual(@as(usize, 1), result.returning_rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"t1\",\"status\":\"joined\",\"source_status_key\":\"ready\"}", result.returning_rows[0]);
        },
        else => return error.TestUnexpectedResult,
    }
    try db.commitTransaction(joined_update_txn, 3_010);
    joined_update_committed = true;

    const joined_source_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"source_rows","enforce_types":true,"document_schemas":{"source_rows":{"schema":{"type":"object","properties":{"source_pk":{"type":"keyword"},"source_status":{"type":"keyword"},"source_quantity":{"type":"numeric"}},"required":["source_pk"],"additionalProperties":false}}},"primary_key":{"columns":["source_pk"]}}
    ;
    var parsed_joined_source_schema = try schema_api.parseValidatedTableSchema(alloc, joined_source_schema_json);
    defer parsed_joined_source_schema.deinit(alloc);
    const joined_source_schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed_joined_source_schema);
    defer runtime_schema.freeSchema(alloc, joined_source_schema);

    const joined_expression_txn = try db.beginTransaction(3_011);
    var joined_expression_committed = false;
    defer if (!joined_expression_committed) db.abortTransaction(joined_expression_txn, 3_012) catch {};
    const joined_expression_claim: db_mod.types.RowClaimRequest = .{
        .mode = .for_update,
        .owner_id = "sql-write-joined-expression-update",
        .txn_id = joined_expression_txn,
    };
    var joined_expression_plan = try lowerUpdateJoinedMutationSourceWithSchemasAlloc(
        alloc,
        "UPDATE usage_records SET status = lower(source.source_status) FROM source_records AS source WHERE usage_records.id = source.source_pk FOR UPDATE RETURNING id, status, lower(source.source_status) AS source_status_key",
        schema,
        joined_source_schema,
        &.{},
        joined_expression_claim,
    );
    defer joined_expression_plan.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), joined_expression_plan.mutation.req.patch_expressions.len);
    var joined_expression_targets = try db.collectRelationalRowsJoinedMutationTargetCandidatesForTargetRangeAlloc(
        alloc,
        schema,
        joined_expression_plan.mutation.req,
        null,
    );
    errdefer {
        for (joined_expression_targets) |*candidate| candidate.deinit(alloc);
        if (joined_expression_targets.len > 0) alloc.free(joined_expression_targets);
    }
    const joined_expression_source_rows = [_][]const u8{
        "{\"source_pk\":\"t3\",\"source_status\":\"stale\",\"source_quantity\":7}",
    };
    var joined_expression_candidates = try db_mod.DB.buildRelationalRowsJoinedMutationSourceCandidatesFromCollectedRowsAlloc(
        alloc,
        joined_expression_plan.mutation.req,
        &joined_expression_targets,
        joined_expression_source_rows[0..],
    );
    errdefer {
        for (joined_expression_candidates) |*candidate| candidate.deinit(alloc);
        if (joined_expression_candidates.len > 0) alloc.free(joined_expression_candidates);
    }
    var joined_expression_storage_plan = try db_mod.DB.selectPlannedRelationalRowsJoinedMutationSourceCandidatesAlloc(
        alloc,
        joined_expression_plan.mutation.req,
        &joined_expression_candidates,
    );
    defer joined_expression_storage_plan.deinit(alloc);
    var joined_expression_result = try db.stagePlannedRelationalRowsJoinedMutationSourceWithSourceSchemaAlloc(
        alloc,
        schema,
        joined_source_schema,
        joined_expression_plan.mutation.req,
        joined_expression_storage_plan.matched,
        joined_expression_storage_plan.candidates,
    );
    defer joined_expression_result.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), joined_expression_result.matched);
    try std.testing.expectEqual(@as(u32, 1), joined_expression_result.staged);
    try std.testing.expectEqual(@as(usize, 1), joined_expression_result.returning_rows.len);
    try std.testing.expectEqualStrings("{\"id\":\"t3\",\"status\":\"stale\",\"source_status_key\":\"stale\"}", joined_expression_result.returning_rows[0]);

    try db.commitTransaction(joined_expression_txn, 3_020);
    joined_expression_committed = true;

    const joined_delete_txn = try db.beginTransaction(3_101);
    var joined_delete_committed = false;
    defer if (!joined_delete_committed) db.abortTransaction(joined_delete_txn, 3_102) catch {};
    const joined_delete_claim: db_mod.types.RowClaimRequest = .{
        .mode = .for_update,
        .owner_id = "sql-write-joined-delete",
        .txn_id = joined_delete_txn,
    };
    var joined_delete_plan = try lowerWritePlanAlloc(
        alloc,
        "DELETE FROM usage_records USING usage_records AS source WHERE usage_records.id = source.id AND source.id = 't2' FOR UPDATE RETURNING id, lower(source.status) AS source_status_key",
        schema,
        &.{},
        .{ .row_claim = joined_delete_claim },
    );
    defer joined_delete_plan.deinit(alloc);

    switch (joined_delete_plan) {
        .delete_joined_source => |joined_delete| {
            var result = try db.mutateRelationalRowsJoinedSourceAlloc(alloc, schema, joined_delete.mutation.req);
            defer result.deinit(alloc);
            try std.testing.expectEqual(@as(u32, 1), result.matched);
            try std.testing.expectEqual(@as(u32, 1), result.staged);
            try std.testing.expectEqual(@as(usize, 1), result.returning_rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"t2\",\"source_status_key\":\"expired\"}", result.returning_rows[0]);
        },
        else => return error.TestUnexpectedResult,
    }
    try db.commitTransaction(joined_delete_txn, 3_110);
    joined_delete_committed = true;

    const joined_select = [_][]const u8{ "id", "status" };
    const joined_order_by = [_]db_mod.types.RelationalRowsQueryOrder{.{
        .field = "id",
        .direction = .asc,
    }};
    var joined_rows = try db.queryRelationalRows(alloc, schema, .{
        .select = joined_select[0..],
        .order_by = joined_order_by[0..],
    });
    defer joined_rows.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 10), joined_rows.total);
    try std.testing.expectEqual(@as(usize, 10), joined_rows.rows.len);
    try std.testing.expectEqualStrings("{\"id\":\"dup_a\",\"status\":\"dupe\"}", joined_rows.rows[0]);
    try std.testing.expectEqualStrings("{\"id\":\"dup_b\",\"status\":\"dupe\"}", joined_rows.rows[1]);
    try std.testing.expectEqualStrings("{\"id\":\"dup_target\",\"status\":\"dupe\"}", joined_rows.rows[2]);
    try std.testing.expectEqualStrings("{\"id\":\"s1\",\"status\":\"synced\"}", joined_rows.rows[3]);
    try std.testing.expectEqualStrings("{\"id\":\"s2\",\"status\":\"stale\"}", joined_rows.rows[4]);
    try std.testing.expectEqualStrings("{\"id\":\"t1\",\"status\":\"joined\"}", joined_rows.rows[5]);
    try std.testing.expectEqualStrings("{\"id\":\"t3\",\"status\":\"stale\"}", joined_rows.rows[6]);
    try std.testing.expectEqualStrings("{\"id\":\"u3\",\"status\":\"queued\"}", joined_rows.rows[7]);
    try std.testing.expectEqualStrings("{\"id\":\"u3_copy\",\"status\":\"QUEUED\"}", joined_rows.rows[8]);
    try std.testing.expectEqualStrings("{\"id\":\"u3_copy_cte_copy\",\"status\":\"QUEUED\"}", joined_rows.rows[9]);
}

test "postgres sql adapter insert source plan collects cross-schema CTE source rows" {
    const alloc = std.testing.allocator;
    const target_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"amount":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed_target = try schema_api.parseValidatedTableSchema(alloc, target_schema_json);
    defer parsed_target.deinit(alloc);
    const target_schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed_target);
    defer runtime_schema.freeSchema(alloc, target_schema);

    const source_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"archive_id":{"type":"keyword"},"archive_status":{"type":"keyword"},"archive_amount":{"type":"numeric"}},"required":["archive_id"],"additionalProperties":false}}},"primary_key":{"columns":["archive_id"]}}
    ;
    var parsed_source = try schema_api.parseValidatedTableSchema(alloc, source_schema_json);
    defer parsed_source.deinit(alloc);
    const source_schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed_source);
    defer runtime_schema.freeSchema(alloc, source_schema);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const target_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/sql-insert-source-cross-schema-cte-target", .{tmp.sub_path});
    defer alloc.free(target_path);
    const source_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/sql-insert-source-cross-schema-cte-source", .{tmp.sub_path});
    defer alloc.free(source_path);

    var target_db = try db_mod.DB.open(alloc, target_path, .{});
    defer target_db.close();
    try target_db.applyTableSchemaJson(alloc, target_schema_json, .{});
    var source_db = try db_mod.DB.open(alloc, source_path, .{});
    defer source_db.close();
    try source_db.applyTableSchemaJson(alloc, source_schema_json, .{});

    const source_jsons = [_][]const u8{
        "{\"archive_id\":\"a1\",\"archive_status\":\"READY\",\"archive_amount\":10}",
        "{\"archive_id\":\"a2\",\"archive_status\":\"ready\",\"archive_amount\":30}",
        "{\"archive_id\":\"a3\",\"archive_status\":\"ready\",\"archive_amount\":20}",
        "{\"archive_id\":\"skip\",\"archive_status\":\"closed\",\"archive_amount\":99}",
    };
    var source_keys: [source_jsons.len][]u8 = undefined;
    for (source_jsons, 0..) |row_json, i| source_keys[i] = try relational_rows.physicalPrimaryKeyFromRowJsonAlloc(alloc, source_schema, row_json);
    defer {
        for (source_keys) |key| alloc.free(key);
    }

    try source_db.batch(.{
        .writes = &.{
            .{ .key = source_keys[0], .value = source_jsons[0] },
            .{ .key = source_keys[1], .value = source_jsons[1] },
            .{ .key = source_keys[2], .value = source_jsons[2] },
            .{ .key = source_keys[3], .value = source_jsons[3] },
        },
        .sync_level = .write,
    });

    var no_existing_primary = TestPrimaryResolver{ .row_json = "", .version = 0, .exists = false };
    var catalog = AppParitySourceSchemaCatalog.init("archived_records", source_schema_json);
    var write_plan = try lowerWritePlanWithCatalogAlloc(
        alloc,
        "WITH ready_archives AS (SELECT archive_id, lower(archive_status) AS status_key, archive_amount + 1 AS next_amount FROM archived_records WHERE lower(archive_status) = 'ready') INSERT INTO usage_records (id, status, amount) SELECT archive_id, status_key, next_amount FROM ready_archives ORDER BY next_amount DESC LIMIT 2 RETURNING id, status, amount",
        target_schema,
        &.{},
        .{ .unique_resolver = no_existing_primary.resolver() },
        catalog.iface(),
    );
    defer write_plan.deinit(alloc);

    const source_ranges = [_]db_mod.types.RelationalRowsDocKeyRange{.{
        .start = source_keys[0],
        .end = "",
    }};

    switch (write_plan) {
        .insert_source => |insert_source| {
            try std.testing.expectEqualStrings("usage_records", insert_source.table_name);
            try std.testing.expectEqualStrings("archived_records", insert_source.insert_source.req.source_table);
            try std.testing.expectEqual(@as(usize, 1), insert_source.ctes.len);
            try std.testing.expectEqualStrings("ready_archives", insert_source.ctes[0].name);
            try std.testing.expectEqualStrings("ready_archives", insert_source.insert_source.req.source.source_cte);
            try std.testing.expectEqual(@as(?u32, 2), insert_source.insert_source.req.source.limit);
            try std.testing.expectEqual(@as(usize, 1), insert_source.insert_source.req.source.order_by.len);

            var batch = try relational_rows.buildRowsInsertSourcePlanBatchFromDbAlloc(
                alloc,
                &source_db,
                insert_source.table_name,
                target_schema,
                source_schema,
                .{
                    .ctes = insert_source.ctes,
                    .ranges = source_ranges[0..],
                    .insert_source = insert_source.insert_source.req,
                },
                no_existing_primary.resolver(),
            );
            defer batch.deinit(alloc);
            try std.testing.expectEqual(@as(u32, 2), batch.inserted);
            try std.testing.expectEqual(@as(u32, 0), batch.transformed);
            try std.testing.expectEqual(@as(usize, 2), batch.returning_rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"a2\",\"status\":\"ready\",\"amount\":31}", batch.returning_rows[0]);
            try std.testing.expectEqualStrings("{\"id\":\"a3\",\"status\":\"ready\",\"amount\":21}", batch.returning_rows[1]);
            try target_db.batch(batch.req);
        },
        else => return error.TestUnexpectedResult,
    }

    const select = [_][]const u8{ "id", "status", "amount" };
    const order_by = [_]db_mod.types.RelationalRowsQueryOrder{.{
        .field = "id",
        .direction = .asc,
    }};
    var rows = try target_db.queryRelationalRows(alloc, target_schema, .{
        .select = select[0..],
        .order_by = order_by[0..],
    });
    defer rows.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 2), rows.total);
    try std.testing.expectEqual(@as(usize, 2), rows.rows.len);
    try std.testing.expectEqualStrings("{\"id\":\"a2\",\"status\":\"ready\",\"amount\":31}", rows.rows[0]);
    try std.testing.expectEqualStrings("{\"id\":\"a3\",\"status\":\"ready\",\"amount\":21}", rows.rows[1]);
}

test "postgres sql adapter joined mutation consumes cross-schema CTE source rows" {
    const alloc = std.testing.allocator;
    const target_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"source_id":{"type":"keyword"},"status":{"type":"keyword"},"quantity":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const source_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"source_pk":{"type":"keyword"},"source_status":{"type":"keyword"},"source_quantity":{"type":"numeric"}},"required":["source_pk"],"additionalProperties":false}}},"primary_key":{"columns":["source_pk"]}}
    ;
    var parsed_target = try schema_api.parseValidatedTableSchema(alloc, target_schema_json);
    defer parsed_target.deinit(alloc);
    const target_schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed_target);
    defer runtime_schema.freeSchema(alloc, target_schema);
    var parsed_source = try schema_api.parseValidatedTableSchema(alloc, source_schema_json);
    defer parsed_source.deinit(alloc);
    const source_schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed_source);
    defer runtime_schema.freeSchema(alloc, source_schema);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const target_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/sql-joined-mutation-cross-schema-cte-target", .{tmp.sub_path});
    defer alloc.free(target_path);
    const source_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/sql-joined-mutation-cross-schema-cte-source", .{tmp.sub_path});
    defer alloc.free(source_path);

    var target_db = try db_mod.DB.open(alloc, target_path, .{});
    defer target_db.close();
    try target_db.applyTableSchemaJson(alloc, target_schema_json, .{});
    var source_db = try db_mod.DB.open(alloc, source_path, .{});
    defer source_db.close();
    try source_db.applyTableSchemaJson(alloc, source_schema_json, .{});

    const target_jsons = [_][]const u8{
        "{\"id\":\"t1\",\"source_id\":\"s1\",\"status\":\"open\",\"quantity\":1}",
        "{\"id\":\"t2\",\"source_id\":\"s2\",\"status\":\"open\",\"quantity\":2}",
    };
    var target_keys: [target_jsons.len][]u8 = undefined;
    for (target_jsons, 0..) |row_json, i| target_keys[i] = try relational_rows.physicalPrimaryKeyFromRowJsonAlloc(alloc, target_schema, row_json);
    defer {
        for (target_keys) |key| alloc.free(key);
    }
    const source_jsons = [_][]const u8{
        "{\"source_pk\":\"s1\",\"source_status\":\"synced\",\"source_quantity\":42}",
        "{\"source_pk\":\"s2\",\"source_status\":\"stale\",\"source_quantity\":99}",
    };
    var source_keys: [source_jsons.len][]u8 = undefined;
    for (source_jsons, 0..) |row_json, i| source_keys[i] = try relational_rows.physicalPrimaryKeyFromRowJsonAlloc(alloc, source_schema, row_json);
    defer {
        for (source_keys) |key| alloc.free(key);
    }

    try target_db.batch(.{
        .writes = &.{
            .{ .key = target_keys[0], .value = target_jsons[0] },
            .{ .key = target_keys[1], .value = target_jsons[1] },
        },
        .sync_level = .write,
    });
    try source_db.batch(.{
        .writes = &.{
            .{ .key = source_keys[0], .value = source_jsons[0] },
            .{ .key = source_keys[1], .value = source_jsons[1] },
        },
        .sync_level = .write,
    });

    const txn_id = try target_db.beginTransaction(31_000);
    var committed = false;
    defer if (!committed) target_db.abortTransaction(txn_id, 31_001) catch {};
    const row_claim: db_mod.types.RowClaimRequest = .{
        .mode = .for_update,
        .owner_id = "sql-joined-cross-schema-cte",
        .txn_id = txn_id,
    };

    var lowered = try lowerUpdateJoinedMutationSourceWithSchemasAlloc(
        alloc,
        "WITH ready_sources AS (SELECT source_pk, source_status, source_quantity FROM source_records WHERE source_status = 'synced') UPDATE usage_records SET status = source.source_status, quantity = source.source_quantity FROM ready_sources AS source WHERE usage_records.source_id = source.source_pk FOR UPDATE RETURNING id, status, quantity",
        target_schema,
        source_schema,
        &.{},
        row_claim,
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqualStrings("usage_records", lowered.target_table_name);
    try std.testing.expectEqualStrings("source_records", lowered.source_table_name);
    try std.testing.expectEqual(@as(usize, 1), lowered.mutation.req.ctes.len);
    try std.testing.expectEqualStrings("ready_sources", lowered.mutation.req.ctes[0].name);
    try std.testing.expectEqualStrings("ready_sources", lowered.mutation.req.join.right.source_cte);
    try std.testing.expectEqual(@as(usize, 2), lowered.mutation.req.source_assignments.len);

    var target_candidates = try target_db.collectRelationalRowsJoinedMutationTargetCandidatesForTargetRangeAlloc(
        alloc,
        target_schema,
        lowered.mutation.req,
        null,
    );
    errdefer {
        for (target_candidates) |*candidate| candidate.deinit(alloc);
        if (target_candidates.len > 0) alloc.free(target_candidates);
    }
    var source_rows = try source_db.queryRelationalRowsJoinedMutationSourceSideOnlyForRangeAlloc(
        alloc,
        source_schema,
        lowered.mutation.req,
        null,
    );
    defer source_rows.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), source_rows.total);
    try std.testing.expectEqual(@as(usize, 1), source_rows.rows.len);

    var joined_candidates = try db_mod.DB.buildRelationalRowsJoinedMutationSourceCandidatesFromCollectedRowsAlloc(
        alloc,
        lowered.mutation.req,
        &target_candidates,
        source_rows.rows,
    );
    errdefer {
        for (joined_candidates) |*candidate| candidate.deinit(alloc);
        if (joined_candidates.len > 0) alloc.free(joined_candidates);
    }
    var plan = try db_mod.DB.selectPlannedRelationalRowsJoinedMutationSourceCandidatesAlloc(
        alloc,
        lowered.mutation.req,
        &joined_candidates,
    );
    defer plan.deinit(alloc);

    var result = try target_db.stagePlannedRelationalRowsJoinedMutationSourceWithSourceSchemaAlloc(
        alloc,
        target_schema,
        source_schema,
        lowered.mutation.req,
        plan.matched,
        plan.candidates,
    );
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), result.matched);
    try std.testing.expectEqual(@as(u32, 1), result.staged);
    try std.testing.expectEqual(@as(usize, 1), result.returning_rows.len);
    try std.testing.expectEqualStrings("{\"id\":\"t1\",\"status\":\"synced\",\"quantity\":42}", result.returning_rows[0]);

    try target_db.commitTransaction(txn_id, 31_010);
    committed = true;

    const select = [_][]const u8{ "id", "status", "quantity" };
    const order_by = [_]db_mod.types.RelationalRowsQueryOrder{.{
        .field = "id",
        .direction = .asc,
    }};
    var rows = try target_db.queryRelationalRows(alloc, target_schema, .{
        .select = select[0..],
        .order_by = order_by[0..],
    });
    defer rows.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 2), rows.total);
    try std.testing.expectEqual(@as(usize, 2), rows.rows.len);
    try std.testing.expectEqualStrings("{\"id\":\"t1\",\"status\":\"synced\",\"quantity\":42}", rows.rows[0]);
    try std.testing.expectEqualStrings("{\"id\":\"t2\",\"status\":\"open\",\"quantity\":2}", rows.rows[1]);

    const delete_txn_id = try target_db.beginTransaction(31_100);
    var delete_committed = false;
    defer if (!delete_committed) target_db.abortTransaction(delete_txn_id, 31_101) catch {};
    const delete_claim: db_mod.types.RowClaimRequest = .{
        .mode = .for_update,
        .owner_id = "sql-joined-cross-schema-cte-delete",
        .txn_id = delete_txn_id,
    };

    var delete_lowered = try lowerDeleteJoinedMutationSourceWithSchemasAlloc(
        alloc,
        "WITH stale_sources AS (SELECT source_pk FROM source_records WHERE source_status = 'stale') DELETE FROM usage_records USING stale_sources AS source WHERE usage_records.source_id = source.source_pk FOR UPDATE RETURNING id, status",
        target_schema,
        source_schema,
        &.{},
        delete_claim,
    );
    defer delete_lowered.deinit(alloc);

    try std.testing.expectEqualStrings("usage_records", delete_lowered.target_table_name);
    try std.testing.expectEqualStrings("source_records", delete_lowered.source_table_name);
    try std.testing.expectEqual(db_mod.types.RelationalRowsMutationKind.delete, delete_lowered.mutation.req.kind);
    try std.testing.expectEqual(@as(usize, 1), delete_lowered.mutation.req.ctes.len);
    try std.testing.expectEqualStrings("stale_sources", delete_lowered.mutation.req.ctes[0].name);
    try std.testing.expectEqualStrings("stale_sources", delete_lowered.mutation.req.join.right.source_cte);

    var delete_target_candidates = try target_db.collectRelationalRowsJoinedMutationTargetCandidatesForTargetRangeAlloc(
        alloc,
        target_schema,
        delete_lowered.mutation.req,
        null,
    );
    errdefer {
        for (delete_target_candidates) |*candidate| candidate.deinit(alloc);
        if (delete_target_candidates.len > 0) alloc.free(delete_target_candidates);
    }
    var delete_source_rows = try source_db.queryRelationalRowsJoinedMutationSourceSideOnlyForRangeAlloc(
        alloc,
        source_schema,
        delete_lowered.mutation.req,
        null,
    );
    defer delete_source_rows.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), delete_source_rows.total);
    try std.testing.expectEqual(@as(usize, 1), delete_source_rows.rows.len);

    var delete_joined_candidates = try db_mod.DB.buildRelationalRowsJoinedMutationSourceCandidatesFromCollectedRowsAlloc(
        alloc,
        delete_lowered.mutation.req,
        &delete_target_candidates,
        delete_source_rows.rows,
    );
    errdefer {
        for (delete_joined_candidates) |*candidate| candidate.deinit(alloc);
        if (delete_joined_candidates.len > 0) alloc.free(delete_joined_candidates);
    }
    var delete_plan = try db_mod.DB.selectPlannedRelationalRowsJoinedMutationSourceCandidatesAlloc(
        alloc,
        delete_lowered.mutation.req,
        &delete_joined_candidates,
    );
    defer delete_plan.deinit(alloc);

    var delete_result = try target_db.stagePlannedRelationalRowsJoinedMutationSourceWithSourceSchemaAlloc(
        alloc,
        target_schema,
        source_schema,
        delete_lowered.mutation.req,
        delete_plan.matched,
        delete_plan.candidates,
    );
    defer delete_result.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), delete_result.matched);
    try std.testing.expectEqual(@as(u32, 1), delete_result.staged);
    try std.testing.expectEqual(@as(usize, 1), delete_result.returning_rows.len);
    try std.testing.expectEqualStrings("{\"id\":\"t2\",\"status\":\"open\"}", delete_result.returning_rows[0]);

    try target_db.commitTransaction(delete_txn_id, 31_110);
    delete_committed = true;

    var rows_after_delete = try target_db.queryRelationalRows(alloc, target_schema, .{
        .select = select[0..],
        .order_by = order_by[0..],
    });
    defer rows_after_delete.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), rows_after_delete.total);
    try std.testing.expectEqual(@as(usize, 1), rows_after_delete.rows.len);
    try std.testing.expectEqualStrings("{\"id\":\"t1\",\"status\":\"synced\",\"quantity\":42}", rows_after_delete.rows[0]);
}

test "postgres sql adapter merge mutation batch executes through relational storage" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"source_id":{"type":"keyword"},"status":{"type":"keyword"},"organization_id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/sql-merge-plan-execution", .{tmp.sub_path});
    defer alloc.free(path);

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();
    try db.applyTableSchemaJson(alloc, schema_json, .{});

    const target_jsons = [_][]const u8{
        "{\"id\":\"t1\",\"status\":\"open\",\"organization_id\":\"org:1\"}",
        "{\"id\":\"t_skip\",\"status\":\"closed\",\"organization_id\":\"org:1\"}",
    };
    const target_keys = [_][]u8{
        try relational_rows.physicalPrimaryKeyFromRowJsonAlloc(alloc, schema, target_jsons[0]),
        try relational_rows.physicalPrimaryKeyFromRowJsonAlloc(alloc, schema, target_jsons[1]),
    };
    defer {
        for (target_keys) |key| alloc.free(key);
    }
    try db.batch(.{
        .writes = &.{
            .{ .key = target_keys[0], .value = target_jsons[0] },
            .{ .key = target_keys[1], .value = target_jsons[1] },
        },
        .sync_level = .write,
    });

    const target_rows = [_]MergeExecutionTargetRow{
        .{ .key = target_keys[0], .json = target_jsons[0], .version = try db.getTimestamp(alloc, target_keys[0]) },
        .{ .key = target_keys[1], .json = target_jsons[1], .version = try db.getTimestamp(alloc, target_keys[1]) },
    };
    const source_rows = [_][]const u8{
        "{\"id\":\"s1\",\"source_id\":\"t1\",\"status\":\"UPDATED\",\"organization_id\":\"org:1\"}",
        "{\"id\":\"s2\",\"source_id\":\"new1\",\"status\":\"inserted\",\"organization_id\":\"org:2\"}",
        "{\"id\":\"s3\",\"source_id\":\"t_skip\",\"status\":\"updated\",\"organization_id\":\"org:3\"}",
        "{\"id\":\"s4\",\"source_id\":\"blocked1\",\"status\":\"inserted\",\"organization_id\":\"org:blocked\"}",
    };

    var write_plan = try lowerWritePlanAlloc(
        alloc,
        "MERGE INTO usage_records AS target USING usage_records AS source ON target.id = source.source_id WHEN MATCHED AND target.status = 'closed' THEN DO NOTHING WHEN MATCHED AND lower(source.status) != lower(target.status) AND (target.status = 'open' OR upper(source.status) = 'READY') AND NOT (target.organization_id = 'org:blocked') THEN UPDATE SET status = lower(source.status) WHEN NOT MATCHED AND source.organization_id = 'org:blocked' THEN DO NOTHING WHEN NOT MATCHED AND (lower(source.status) = 'inserted' OR source.status = 'INSERTED') AND NOT (source.organization_id = 'org:blocked') THEN INSERT (id, status, organization_id) VALUES (source.source_id, upper(source.status), source.organization_id) RETURNING id, status, lower(status) AS status_key",
        schema,
        &.{},
        .{ .sync_level = .enrichments },
    );
    defer write_plan.deinit(alloc);

    switch (write_plan) {
        .merge_mutation => |merge| {
            try std.testing.expectEqual(db_mod.types.SyncLevel.enrichments, merge.sync_level);
            var batch = try buildMergeMutationBatchAlloc(alloc, schema, schema, merge, target_rows[0..], source_rows[0..]);
            defer batch.deinit(alloc);
            try std.testing.expectEqual(db_mod.types.SyncLevel.enrichments, batch.req.sync_level);
            try std.testing.expectEqual(@as(u32, 1), batch.inserted);
            try std.testing.expectEqual(@as(u32, 0), batch.deleted);
            try std.testing.expectEqual(@as(u32, 1), batch.transformed);
            try std.testing.expectEqual(@as(usize, 2), batch.returning_rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"t1\",\"status\":\"updated\",\"status_key\":\"updated\"}", batch.returning_rows[0]);
            try std.testing.expectEqualStrings("{\"id\":\"new1\",\"status\":\"INSERTED\",\"status_key\":\"inserted\"}", batch.returning_rows[1]);
            try db.batch(batch.req);
        },
        else => return error.TestUnexpectedResult,
    }

    const select = [_][]const u8{ "id", "status", "organization_id" };
    const order_by = [_]db_mod.types.RelationalRowsQueryOrder{.{
        .field = "id",
        .direction = .asc,
    }};
    var rows = try db.queryRelationalRows(alloc, schema, .{
        .select = select[0..],
        .order_by = order_by[0..],
    });
    defer rows.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 3), rows.total);
    try std.testing.expectEqual(@as(usize, 3), rows.rows.len);
    try std.testing.expectEqualStrings("{\"id\":\"new1\",\"status\":\"INSERTED\",\"organization_id\":\"org:2\"}", rows.rows[0]);
    try std.testing.expectEqualStrings("{\"id\":\"t1\",\"status\":\"updated\",\"organization_id\":\"org:1\"}", rows.rows[1]);
    try std.testing.expectEqualStrings("{\"id\":\"t_skip\",\"status\":\"closed\",\"organization_id\":\"org:1\"}", rows.rows[2]);
}

test "postgres sql adapter merge mutation batch applies default expressions" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"source_id":{"type":"keyword"},"status":{"type":"keyword","default":"active"},"organization_id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    const target_json = "{\"id\":\"t1\",\"status\":\"old\",\"organization_id\":\"org:1\"}";
    const target_key = try relational_rows.physicalPrimaryKeyFromRowJsonAlloc(alloc, schema, target_json);
    defer alloc.free(target_key);
    const target_rows = [_]MergeExecutionTargetRow{.{
        .key = target_key,
        .json = target_json,
        .version = 41,
    }};
    const source_rows = [_][]const u8{
        "{\"id\":\"s1\",\"source_id\":\"t1\",\"status\":\"ignored\",\"organization_id\":\"org:source-ignored\"}",
        "{\"id\":\"s2\",\"source_id\":\"new1\",\"status\":\"ignored\",\"organization_id\":\"org:2\"}",
    };

    var write_plan = try lowerWritePlanAlloc(
        alloc,
        "MERGE INTO usage_records AS target USING usage_records AS source ON target.id = source.source_id WHEN MATCHED THEN UPDATE SET status = DEFAULT WHEN NOT MATCHED THEN INSERT (id, status, organization_id) VALUES (source.source_id, DEFAULT, source.organization_id) RETURNING id, status, organization_id",
        schema,
        &.{},
        .{},
    );
    defer write_plan.deinit(alloc);

    switch (write_plan) {
        .merge_mutation => |merge| {
            try std.testing.expectEqual(@as(usize, 1), merge.matched_arms[0].update_expressions.len);
            try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.value, merge.matched_arms[0].update_expressions[0].expression.kind);
            try std.testing.expectEqualStrings("\"active\"", merge.matched_arms[0].update_expressions[0].expression.value_json);
            try std.testing.expectEqual(@as(usize, 1), merge.not_matched_arms[0].insert_expressions.len);
            try std.testing.expectEqualStrings("\"active\"", merge.not_matched_arms[0].insert_expressions[0].expression.value_json);

            var batch = try buildMergeMutationBatchAlloc(alloc, schema, schema, merge, target_rows[0..], source_rows[0..]);
            defer batch.deinit(alloc);
            try std.testing.expectEqual(@as(u32, 1), batch.inserted);
            try std.testing.expectEqual(@as(u32, 1), batch.transformed);
            try std.testing.expectEqual(@as(usize, 2), batch.returning_rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"t1\",\"status\":\"active\",\"organization_id\":\"org:1\"}", batch.returning_rows[0]);
            try std.testing.expectEqualStrings("{\"id\":\"new1\",\"status\":\"active\",\"organization_id\":\"org:2\"}", batch.returning_rows[1]);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "postgres sql adapter merge mutation batch resolves temporal primary targets" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"sku":{"type":"keyword"},"source_id":{"type":"keyword"},"valid_from":{"type":"numeric"},"valid_to":{"type":"numeric"},"status":{"type":"keyword"}},"required":["sku","valid_from","valid_to"],"additionalProperties":false}}},"periods":[{"name":"valid_time","start_column":"valid_from","end_column":"valid_to"}],"primary_key":{"columns":["sku"],"without_overlaps_period":"valid_time"}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/sql-merge-temporal-primary-execution", .{tmp.sub_path});
    defer alloc.free(path);

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();
    try db.applyTableSchemaJson(alloc, schema_json, .{});

    const target_json = "{\"sku\":\"sku:a\",\"valid_from\":10,\"valid_to\":20,\"status\":\"open\"}";
    const target_key = try relational_rows.physicalPrimaryKeyFromRowJsonAlloc(alloc, schema, target_json);
    defer alloc.free(target_key);
    try db.batch(.{
        .writes = &.{.{ .key = target_key, .value = target_json }},
        .sync_level = .write,
    });

    const target_rows = [_]MergeExecutionTargetRow{
        .{ .key = target_key, .json = target_json, .version = try db.getTimestamp(alloc, target_key) },
    };
    const source_rows = [_][]const u8{
        "{\"sku\":\"incoming\",\"source_id\":\"sku:a\",\"valid_from\":0,\"valid_to\":1,\"status\":\"merged\"}",
    };

    var write_plan = try lowerWritePlanAlloc(
        alloc,
        "MERGE INTO prices AS target USING prices AS source ON target.sku = source.source_id WHEN MATCHED THEN UPDATE SET status = source.status RETURNING sku, status",
        schema,
        &.{},
        .{},
    );
    defer write_plan.deinit(alloc);

    switch (write_plan) {
        .merge_mutation => |merge| {
            var batch = try buildMergeMutationBatchAlloc(alloc, schema, schema, merge, target_rows[0..], source_rows[0..]);
            defer batch.deinit(alloc);
            try std.testing.expectEqual(@as(u32, 0), batch.inserted);
            try std.testing.expectEqual(@as(u32, 0), batch.deleted);
            try std.testing.expectEqual(@as(u32, 1), batch.transformed);
            try std.testing.expectEqual(@as(usize, 1), batch.returning_rows.len);
            try std.testing.expectEqualStrings("{\"sku\":\"sku:a\",\"status\":\"merged\"}", batch.returning_rows[0]);
            try db.batch(batch.req);
        },
        else => return error.TestUnexpectedResult,
    }

    const select = [_][]const u8{ "sku", "status", "valid_from", "valid_to" };
    var rows = try db.queryRelationalRows(alloc, schema, .{
        .select = select[0..],
    });
    defer rows.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), rows.total);
    try std.testing.expectEqual(@as(usize, 1), rows.rows.len);
    try std.testing.expectEqualStrings("{\"sku\":\"sku:a\",\"status\":\"merged\",\"valid_from\":10,\"valid_to\":20}", rows.rows[0]);
}

test "postgres sql adapter merge mutation batch collects local relational preimages" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"source_id":{"type":"keyword"},"status":{"type":"keyword"},"organization_id":{"type":"keyword"},"kind":{"type":"keyword"}},"required":["id","kind"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/sql-merge-local-preimages", .{tmp.sub_path});
    defer alloc.free(path);

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();
    try db.applyTableSchemaJson(alloc, schema_json, .{});

    const row_jsons = [_][]const u8{
        "{\"id\":\"t1\",\"kind\":\"target\",\"status\":\"open\",\"organization_id\":\"org:1\"}",
        "{\"id\":\"t_skip\",\"kind\":\"target\",\"status\":\"closed\",\"organization_id\":\"org:1\"}",
        "{\"id\":\"s1\",\"kind\":\"source\",\"source_id\":\"t1\",\"status\":\"UPDATED\",\"organization_id\":\"org:1\"}",
        "{\"id\":\"s2\",\"kind\":\"source\",\"source_id\":\"new1\",\"status\":\"inserted\",\"organization_id\":\"org:2\"}",
        "{\"id\":\"s3\",\"kind\":\"source\",\"source_id\":\"t_skip\",\"status\":\"updated\",\"organization_id\":\"org:3\"}",
        "{\"id\":\"s4\",\"kind\":\"source\",\"source_id\":\"blocked1\",\"status\":\"inserted\",\"organization_id\":\"org:blocked\"}",
    };
    var row_keys: [row_jsons.len][]u8 = undefined;
    for (row_jsons, 0..) |row_json, i| row_keys[i] = try relational_rows.physicalPrimaryKeyFromRowJsonAlloc(alloc, schema, row_json);
    defer {
        for (row_keys) |key| alloc.free(key);
    }

    try db.batch(.{
        .writes = &.{
            .{ .key = row_keys[0], .value = row_jsons[0] },
            .{ .key = row_keys[1], .value = row_jsons[1] },
            .{ .key = row_keys[2], .value = row_jsons[2] },
            .{ .key = row_keys[3], .value = row_jsons[3] },
            .{ .key = row_keys[4], .value = row_jsons[4] },
            .{ .key = row_keys[5], .value = row_jsons[5] },
        },
        .sync_level = .write,
    });

    var write_plan = try lowerWritePlanAlloc(
        alloc,
        "MERGE INTO usage_records AS target USING usage_records AS source ON target.id = source.source_id WHEN MATCHED AND target.status = 'closed' THEN DO NOTHING WHEN MATCHED AND lower(source.status) != lower(target.status) THEN UPDATE SET status = lower(source.status) WHEN NOT MATCHED AND source.organization_id = 'org:blocked' THEN DO NOTHING WHEN NOT MATCHED AND lower(source.status) = 'inserted' THEN INSERT (id, status, organization_id, kind) VALUES (source.source_id, upper(source.status), source.organization_id, 'target') RETURNING id, status, lower(status) AS status_key",
        schema,
        &.{},
        .{},
    );
    defer write_plan.deinit(alloc);

    const target_predicates = [_]runtime_schema.RelationalCheck{.{
        .name = "",
        .field = "kind",
        .op = .eq,
        .value_json = "\"target\"",
    }};
    const source_predicates = [_]runtime_schema.RelationalCheck{.{
        .name = "",
        .field = "kind",
        .op = .eq,
        .value_json = "\"source\"",
    }};
    const order_by = [_]db_mod.types.RelationalRowsQueryOrder{.{
        .field = "id",
        .direction = .asc,
    }};
    const target_query: db_mod.types.RelationalRowsQueryRequest = .{
        .predicates = target_predicates[0..],
        .order_by = order_by[0..],
    };
    const source_query: db_mod.types.RelationalRowsQueryRequest = .{
        .predicates = source_predicates[0..],
        .order_by = order_by[0..],
    };
    const target_ranges = [_]db_mod.types.RelationalRowsDocKeyRange{.{
        .start = row_keys[0],
        .end = "",
    }};
    const source_ranges = [_]db_mod.types.RelationalRowsDocKeyRange{.{
        .start = row_keys[2],
        .end = "",
    }};

    switch (write_plan) {
        .merge_mutation => |merge| {
            var batch = try buildMergeMutationBatchFromDbAcrossRangesAlloc(alloc, &db, schema, schema, merge, target_query, target_ranges[0..], source_query, source_ranges[0..]);
            defer batch.deinit(alloc);
            try std.testing.expectEqual(@as(u32, 1), batch.inserted);
            try std.testing.expectEqual(@as(u32, 0), batch.deleted);
            try std.testing.expectEqual(@as(u32, 1), batch.transformed);
            try std.testing.expectEqual(@as(usize, 2), batch.returning_rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"t1\",\"status\":\"updated\",\"status_key\":\"updated\"}", batch.returning_rows[0]);
            try std.testing.expectEqualStrings("{\"id\":\"new1\",\"status\":\"INSERTED\",\"status_key\":\"inserted\"}", batch.returning_rows[1]);
            try db.batch(batch.req);
        },
        else => return error.TestUnexpectedResult,
    }

    const select = [_][]const u8{ "id", "status", "organization_id", "kind" };
    var rows = try db.queryRelationalRows(alloc, schema, .{
        .select = select[0..],
        .predicates = target_predicates[0..],
        .order_by = order_by[0..],
    });
    defer rows.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 3), rows.total);
    try std.testing.expectEqual(@as(usize, 3), rows.rows.len);
    try std.testing.expectEqualStrings("{\"id\":\"new1\",\"status\":\"INSERTED\",\"organization_id\":\"org:2\",\"kind\":\"target\"}", rows.rows[0]);
    try std.testing.expectEqualStrings("{\"id\":\"t1\",\"status\":\"updated\",\"organization_id\":\"org:1\",\"kind\":\"target\"}", rows.rows[1]);
    try std.testing.expectEqualStrings("{\"id\":\"t_skip\",\"status\":\"closed\",\"organization_id\":\"org:1\",\"kind\":\"target\"}", rows.rows[2]);
}

test "postgres sql adapter merge mutation batch collects CTE source preimages" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"source_id":{"type":"keyword"},"status":{"type":"keyword"},"organization_id":{"type":"keyword"},"kind":{"type":"keyword"}},"required":["id","kind"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/sql-merge-cte-source-preimages", .{tmp.sub_path});
    defer alloc.free(path);

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();
    try db.applyTableSchemaJson(alloc, schema_json, .{});

    const row_jsons = [_][]const u8{
        "{\"id\":\"t1\",\"kind\":\"target\",\"status\":\"open\",\"organization_id\":\"org:1\"}",
        "{\"id\":\"s1\",\"kind\":\"source\",\"source_id\":\"t1\",\"status\":\"UPDATED\",\"organization_id\":\"org:1\"}",
        "{\"id\":\"s2\",\"kind\":\"source\",\"source_id\":\"new1\",\"status\":\"inserted\",\"organization_id\":\"org:2\"}",
    };
    var row_keys: [row_jsons.len][]u8 = undefined;
    for (row_jsons, 0..) |row_json, i| row_keys[i] = try relational_rows.physicalPrimaryKeyFromRowJsonAlloc(alloc, schema, row_json);
    defer {
        for (row_keys) |key| alloc.free(key);
    }

    try db.batch(.{
        .writes = &.{
            .{ .key = row_keys[0], .value = row_jsons[0] },
            .{ .key = row_keys[1], .value = row_jsons[1] },
            .{ .key = row_keys[2], .value = row_jsons[2] },
        },
        .sync_level = .write,
    });

    var write_plan = try lowerWritePlanAlloc(
        alloc,
        "WITH ready_sources AS (SELECT source_id, status, organization_id FROM usage_records WHERE kind = 'source') MERGE INTO usage_records AS target USING ready_sources AS source ON target.id = source.source_id WHEN MATCHED THEN UPDATE SET status = lower(source.status) WHEN NOT MATCHED THEN INSERT (id, status, organization_id, kind) VALUES (source.source_id, upper(source.status), source.organization_id, 'target') RETURNING id, status, lower(status) AS status_key",
        schema,
        &.{},
        .{},
    );
    defer write_plan.deinit(alloc);

    const target_predicates = [_]runtime_schema.RelationalCheck{.{
        .name = "",
        .field = "kind",
        .op = .eq,
        .value_json = "\"target\"",
    }};
    const order_by = [_]db_mod.types.RelationalRowsQueryOrder{.{
        .field = "id",
        .direction = .asc,
    }};
    const target_query: db_mod.types.RelationalRowsQueryRequest = .{
        .predicates = target_predicates[0..],
        .order_by = order_by[0..],
    };
    const source_ranges = [_]db_mod.types.RelationalRowsDocKeyRange{.{
        .start = row_keys[1],
        .end = "",
    }};

    switch (write_plan) {
        .merge_mutation => |merge| {
            try std.testing.expectEqual(@as(usize, 1), merge.ctes.len);
            try std.testing.expectEqualStrings("ready_sources", merge.source.source_cte);
            var batch = try buildMergeMutationBatchFromDbAcrossRangesAlloc(alloc, &db, schema, schema, merge, target_query, &.{}, .{}, source_ranges[0..]);
            defer batch.deinit(alloc);
            try std.testing.expectEqual(@as(u32, 1), batch.inserted);
            try std.testing.expectEqual(@as(u32, 0), batch.deleted);
            try std.testing.expectEqual(@as(u32, 1), batch.transformed);
            try std.testing.expectEqual(@as(usize, 2), batch.returning_rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"t1\",\"status\":\"updated\",\"status_key\":\"updated\"}", batch.returning_rows[0]);
            try std.testing.expectEqualStrings("{\"id\":\"new1\",\"status\":\"INSERTED\",\"status_key\":\"inserted\"}", batch.returning_rows[1]);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "postgres sql adapter merge mutation batch collects cross-schema local preimages" {
    const alloc = std.testing.allocator;
    const target_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"organization_id":{"type":"keyword"},"amount":{"type":"numeric"},"kind":{"type":"keyword"}},"required":["id","kind"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed_target = try schema_api.parseValidatedTableSchema(alloc, target_schema_json);
    defer parsed_target.deinit(alloc);
    const target_schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed_target);
    defer runtime_schema.freeSchema(alloc, target_schema);

    const source_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"source_pk":{"type":"keyword"},"archive_id":{"type":"keyword"},"archive_status":{"type":"keyword"},"archive_amount":{"type":"numeric"},"archive_org":{"type":"keyword"},"archive_kind":{"type":"keyword"}},"required":["source_pk","archive_id","archive_kind"],"additionalProperties":false}}},"primary_key":{"columns":["source_pk"]}}
    ;
    var parsed_source = try schema_api.parseValidatedTableSchema(alloc, source_schema_json);
    defer parsed_source.deinit(alloc);
    const source_schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed_source);
    defer runtime_schema.freeSchema(alloc, source_schema);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const target_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/sql-merge-cross-schema-local-target-preimages", .{tmp.sub_path});
    defer alloc.free(target_path);
    const source_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/sql-merge-cross-schema-local-source-preimages", .{tmp.sub_path});
    defer alloc.free(source_path);

    var db = try db_mod.DB.open(alloc, target_path, .{});
    defer db.close();
    try db.applyTableSchemaJson(alloc, target_schema_json, .{});
    var source_db = try db_mod.DB.open(alloc, source_path, .{});
    defer source_db.close();
    try source_db.applyTableSchemaJson(alloc, source_schema_json, .{});

    const target_jsons = [_][]const u8{
        "{\"id\":\"t1\",\"kind\":\"target\",\"status\":\"open\",\"organization_id\":\"org:1\",\"amount\":1}",
        "{\"id\":\"t_skip\",\"kind\":\"target\",\"status\":\"closed\",\"organization_id\":\"org:1\",\"amount\":2}",
    };
    var target_keys: [target_jsons.len][]u8 = undefined;
    for (target_jsons, 0..) |row_json, i| target_keys[i] = try relational_rows.physicalPrimaryKeyFromRowJsonAlloc(alloc, target_schema, row_json);
    defer {
        for (target_keys) |key| alloc.free(key);
    }

    const source_jsons = [_][]const u8{
        "{\"source_pk\":\"s1\",\"archive_id\":\"t1\",\"archive_kind\":\"source\",\"archive_status\":\"UPDATED\",\"archive_amount\":11,\"archive_org\":\"org:1\"}",
        "{\"source_pk\":\"s2\",\"archive_id\":\"new1\",\"archive_kind\":\"source\",\"archive_status\":\"inserted\",\"archive_amount\":22,\"archive_org\":\"org:2\"}",
        "{\"source_pk\":\"s3\",\"archive_id\":\"t_skip\",\"archive_kind\":\"source\",\"archive_status\":\"ignored\",\"archive_amount\":33,\"archive_org\":\"org:1\"}",
    };
    var source_keys: [source_jsons.len][]u8 = undefined;
    for (source_jsons, 0..) |row_json, i| source_keys[i] = try relational_rows.physicalPrimaryKeyFromRowJsonAlloc(alloc, source_schema, row_json);
    defer {
        for (source_keys) |key| alloc.free(key);
    }

    try db.batch(.{
        .writes = &.{
            .{ .key = target_keys[0], .value = target_jsons[0] },
            .{ .key = target_keys[1], .value = target_jsons[1] },
        },
        .sync_level = .write,
    });
    try source_db.batch(.{
        .writes = &.{
            .{ .key = source_keys[0], .value = source_jsons[0] },
            .{ .key = source_keys[1], .value = source_jsons[1] },
            .{ .key = source_keys[2], .value = source_jsons[2] },
        },
        .sync_level = .write,
    });

    var catalog = AppParitySourceSchemaCatalog.init("archived_records", source_schema_json);
    var write_plan = try lowerWritePlanWithCatalogAlloc(
        alloc,
        "MERGE INTO usage_records AS target USING archived_records AS source ON target.id = source.archive_id WHEN MATCHED AND target.status = 'closed' THEN DO NOTHING WHEN MATCHED THEN UPDATE SET status = lower(source.archive_status), amount = source.archive_amount WHEN NOT MATCHED THEN INSERT (id, status, organization_id, amount, kind) VALUES (source.archive_id, upper(source.archive_status), source.archive_org, source.archive_amount, 'target') RETURNING target.id, target.status, target.amount",
        target_schema,
        &.{},
        .{},
        catalog.iface(),
    );
    defer write_plan.deinit(alloc);

    const target_predicates = [_]runtime_schema.RelationalCheck{.{
        .name = "",
        .field = "kind",
        .op = .eq,
        .value_json = "\"target\"",
    }};
    const source_predicates = [_]runtime_schema.RelationalCheck{.{
        .name = "",
        .field = "archive_kind",
        .op = .eq,
        .value_json = "\"source\"",
    }};
    const target_order_by = [_]db_mod.types.RelationalRowsQueryOrder{.{
        .field = "id",
        .direction = .asc,
    }};
    const source_order_by = [_]db_mod.types.RelationalRowsQueryOrder{.{
        .field = "archive_id",
        .direction = .asc,
    }};
    const target_query: db_mod.types.RelationalRowsQueryRequest = .{
        .predicates = target_predicates[0..],
        .order_by = target_order_by[0..],
    };
    const source_query: db_mod.types.RelationalRowsQueryRequest = .{
        .predicates = source_predicates[0..],
        .order_by = source_order_by[0..],
    };
    const target_ranges = [_]db_mod.types.RelationalRowsDocKeyRange{.{
        .start = target_keys[0],
        .end = "",
    }};
    const source_ranges = [_]db_mod.types.RelationalRowsDocKeyRange{.{
        .start = source_keys[0],
        .end = "",
    }};

    switch (write_plan) {
        .merge_mutation => |merge| {
            var batch = try buildMergeMutationBatchFromDbsAcrossRangesAlloc(alloc, &db, &source_db, target_schema, source_schema, merge, target_query, target_ranges[0..], source_query, source_ranges[0..]);
            defer batch.deinit(alloc);
            try std.testing.expectEqual(@as(u32, 1), batch.inserted);
            try std.testing.expectEqual(@as(u32, 0), batch.deleted);
            try std.testing.expectEqual(@as(u32, 1), batch.transformed);
            try std.testing.expectEqual(@as(usize, 2), batch.returning_rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"new1\",\"status\":\"INSERTED\",\"amount\":22}", batch.returning_rows[0]);
            try std.testing.expectEqualStrings("{\"id\":\"t1\",\"status\":\"updated\",\"amount\":11}", batch.returning_rows[1]);
            try db.batch(batch.req);
        },
        else => return error.TestUnexpectedResult,
    }

    const select = [_][]const u8{ "id", "status", "organization_id", "amount", "kind" };
    var rows = try db.queryRelationalRows(alloc, target_schema, .{
        .select = select[0..],
        .predicates = target_predicates[0..],
        .order_by = target_order_by[0..],
    });
    defer rows.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 3), rows.total);
    try std.testing.expectEqual(@as(usize, 3), rows.rows.len);
    try std.testing.expectEqualStrings("{\"id\":\"new1\",\"status\":\"INSERTED\",\"organization_id\":\"org:2\",\"amount\":22,\"kind\":\"target\"}", rows.rows[0]);
    try std.testing.expectEqualStrings("{\"id\":\"t1\",\"status\":\"updated\",\"organization_id\":\"org:1\",\"amount\":11,\"kind\":\"target\"}", rows.rows[1]);
    try std.testing.expectEqualStrings("{\"id\":\"t_skip\",\"status\":\"closed\",\"organization_id\":\"org:1\",\"amount\":2,\"kind\":\"target\"}", rows.rows[2]);
}

test "postgres sql adapter merge mutation batch collects cross-schema CTE source preimages" {
    const alloc = std.testing.allocator;
    const target_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"organization_id":{"type":"keyword"},"amount":{"type":"numeric"},"kind":{"type":"keyword"}},"required":["id","kind"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed_target = try schema_api.parseValidatedTableSchema(alloc, target_schema_json);
    defer parsed_target.deinit(alloc);
    const target_schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed_target);
    defer runtime_schema.freeSchema(alloc, target_schema);

    const source_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"source_pk":{"type":"keyword"},"archive_id":{"type":"keyword"},"archive_status":{"type":"keyword"},"archive_amount":{"type":"numeric"},"archive_org":{"type":"keyword"},"archive_kind":{"type":"keyword"}},"required":["source_pk","archive_id","archive_kind"],"additionalProperties":false}}},"primary_key":{"columns":["source_pk"]}}
    ;
    var parsed_source = try schema_api.parseValidatedTableSchema(alloc, source_schema_json);
    defer parsed_source.deinit(alloc);
    const source_schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed_source);
    defer runtime_schema.freeSchema(alloc, source_schema);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const target_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/sql-merge-cross-schema-cte-target-preimages", .{tmp.sub_path});
    defer alloc.free(target_path);
    const source_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/sql-merge-cross-schema-cte-source-preimages", .{tmp.sub_path});
    defer alloc.free(source_path);

    var db = try db_mod.DB.open(alloc, target_path, .{});
    defer db.close();
    try db.applyTableSchemaJson(alloc, target_schema_json, .{});
    var source_db = try db_mod.DB.open(alloc, source_path, .{});
    defer source_db.close();
    try source_db.applyTableSchemaJson(alloc, source_schema_json, .{});

    const target_jsons = [_][]const u8{
        "{\"id\":\"t1\",\"kind\":\"target\",\"status\":\"open\",\"organization_id\":\"org:1\",\"amount\":1}",
        "{\"id\":\"t_skip\",\"kind\":\"target\",\"status\":\"closed\",\"organization_id\":\"org:1\",\"amount\":2}",
    };
    var target_keys: [target_jsons.len][]u8 = undefined;
    for (target_jsons, 0..) |row_json, i| target_keys[i] = try relational_rows.physicalPrimaryKeyFromRowJsonAlloc(alloc, target_schema, row_json);
    defer {
        for (target_keys) |key| alloc.free(key);
    }

    const source_jsons = [_][]const u8{
        "{\"source_pk\":\"s1\",\"archive_id\":\"t1\",\"archive_kind\":\"source\",\"archive_status\":\"UPDATED\",\"archive_amount\":11,\"archive_org\":\"org:1\"}",
        "{\"source_pk\":\"s2\",\"archive_id\":\"new1\",\"archive_kind\":\"source\",\"archive_status\":\"inserted\",\"archive_amount\":22,\"archive_org\":\"org:2\"}",
        "{\"source_pk\":\"s3\",\"archive_id\":\"t_skip\",\"archive_kind\":\"skip\",\"archive_status\":\"ignored\",\"archive_amount\":33,\"archive_org\":\"org:1\"}",
    };
    var source_keys: [source_jsons.len][]u8 = undefined;
    for (source_jsons, 0..) |row_json, i| source_keys[i] = try relational_rows.physicalPrimaryKeyFromRowJsonAlloc(alloc, source_schema, row_json);
    defer {
        for (source_keys) |key| alloc.free(key);
    }

    try db.batch(.{
        .writes = &.{
            .{ .key = target_keys[0], .value = target_jsons[0] },
            .{ .key = target_keys[1], .value = target_jsons[1] },
        },
        .sync_level = .write,
    });
    try source_db.batch(.{
        .writes = &.{
            .{ .key = source_keys[0], .value = source_jsons[0] },
            .{ .key = source_keys[1], .value = source_jsons[1] },
            .{ .key = source_keys[2], .value = source_jsons[2] },
        },
        .sync_level = .write,
    });

    var catalog = AppParitySourceSchemaCatalog.init("archived_records", source_schema_json);
    var write_plan = try lowerWritePlanWithCatalogAlloc(
        alloc,
        "WITH ready_archives AS (SELECT archive_id, archive_status, archive_amount, archive_org FROM archived_records WHERE archive_kind = 'source') MERGE INTO usage_records AS target USING ready_archives AS source ON target.id = source.archive_id WHEN MATCHED THEN UPDATE SET status = lower(source.archive_status), amount = source.archive_amount WHEN NOT MATCHED THEN INSERT (id, status, organization_id, amount, kind) VALUES (source.archive_id, upper(source.archive_status), source.archive_org, source.archive_amount, 'target') RETURNING target.id, target.status, target.amount",
        target_schema,
        &.{},
        .{},
        catalog.iface(),
    );
    defer write_plan.deinit(alloc);

    const target_predicates = [_]runtime_schema.RelationalCheck{.{
        .name = "",
        .field = "kind",
        .op = .eq,
        .value_json = "\"target\"",
    }};
    const target_order_by = [_]db_mod.types.RelationalRowsQueryOrder{.{
        .field = "id",
        .direction = .asc,
    }};
    const target_query: db_mod.types.RelationalRowsQueryRequest = .{
        .predicates = target_predicates[0..],
        .order_by = target_order_by[0..],
    };
    const target_ranges = [_]db_mod.types.RelationalRowsDocKeyRange{.{
        .start = target_keys[0],
        .end = "",
    }};
    const source_ranges = [_]db_mod.types.RelationalRowsDocKeyRange{.{
        .start = source_keys[0],
        .end = "",
    }};

    switch (write_plan) {
        .merge_mutation => |merge| {
            try std.testing.expectEqual(@as(usize, 1), merge.ctes.len);
            try std.testing.expectEqualStrings("ready_archives", merge.source.source_cte);
            try std.testing.expectEqualStrings("archived_records", merge.source_table_name);
            var batch = try buildMergeMutationBatchFromDbsAcrossRangesAlloc(alloc, &db, &source_db, target_schema, source_schema, merge, target_query, target_ranges[0..], .{}, source_ranges[0..]);
            defer batch.deinit(alloc);
            try std.testing.expectEqual(@as(u32, 1), batch.inserted);
            try std.testing.expectEqual(@as(u32, 0), batch.deleted);
            try std.testing.expectEqual(@as(u32, 1), batch.transformed);
            try std.testing.expectEqual(@as(usize, 2), batch.returning_rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"t1\",\"status\":\"updated\",\"amount\":11}", batch.returning_rows[0]);
            try std.testing.expectEqualStrings("{\"id\":\"new1\",\"status\":\"INSERTED\",\"amount\":22}", batch.returning_rows[1]);
            try db.batch(batch.req);
        },
        else => return error.TestUnexpectedResult,
    }

    const select = [_][]const u8{ "id", "status", "organization_id", "amount", "kind" };
    var rows = try db.queryRelationalRows(alloc, target_schema, .{
        .select = select[0..],
        .predicates = target_predicates[0..],
        .order_by = target_order_by[0..],
    });
    defer rows.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 3), rows.total);
    try std.testing.expectEqual(@as(usize, 3), rows.rows.len);
    try std.testing.expectEqualStrings("{\"id\":\"new1\",\"status\":\"INSERTED\",\"organization_id\":\"org:2\",\"amount\":22,\"kind\":\"target\"}", rows.rows[0]);
    try std.testing.expectEqualStrings("{\"id\":\"t1\",\"status\":\"updated\",\"organization_id\":\"org:1\",\"amount\":11,\"kind\":\"target\"}", rows.rows[1]);
    try std.testing.expectEqualStrings("{\"id\":\"t_skip\",\"status\":\"closed\",\"organization_id\":\"org:1\",\"amount\":2,\"kind\":\"target\"}", rows.rows[2]);
}

test "postgres sql adapter mutation source jsonb_set executes through relational storage" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"metadata":{"type":"json"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/sql-mutation-source-jsonb-set", .{tmp.sub_path});
    defer alloc.free(path);

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();
    try db.applyTableSchemaJson(alloc, schema_json, .{});

    try db.batch(.{
        .writes = &.{
            .{ .key = "row:u1", .value = "{\"id\":\"u1\",\"status\":\"QUEUED\",\"metadata\":{\"billing\":{\"plan\":\"free\"}}}" },
            .{ .key = "row:u2", .value = "{\"id\":\"u2\",\"status\":\"done\",\"metadata\":{\"billing\":{\"plan\":\"team\"}}}" },
        },
        .sync_level = .write,
    });

    const txn_id = try db.beginTransaction(4_001);
    var committed = false;
    defer if (!committed) db.abortTransaction(txn_id, 4_002) catch {};
    const claim: db_mod.types.RowClaimRequest = .{
        .mode = .for_update,
        .owner_id = "sql-jsonb-set-update",
        .txn_id = txn_id,
    };

    try std.testing.expectError(error.InvalidSqlCatalog, lowerWritePlanAlloc(
        alloc,
        "UPDATE usage_records SET metadata = jsonb_set(metadata, '{billing,status_key}', to_jsonb(lower(status)), true) WHERE status = 'QUEUED' FOR UPDATE RETURNING status.detail",
        schema,
        &.{},
        .{ .row_claim = claim },
    ));

    var plan = try lowerWritePlanAlloc(
        alloc,
        "UPDATE usage_records SET metadata = jsonb_set(metadata, '{billing,status_key}', to_jsonb(lower(status)), true) WHERE status = 'QUEUED' FOR UPDATE RETURNING id, metadata.billing.status_key",
        schema,
        &.{},
        .{ .row_claim = claim },
    );
    defer plan.deinit(alloc);

    switch (plan) {
        .update_source => |update_source| {
            try std.testing.expectEqual(@as(usize, 1), update_source.mutation.req.json_set_expressions.len);
            try std.testing.expectEqualStrings("metadata", update_source.mutation.req.json_set_expressions[0].field);
            try std.testing.expectEqualStrings("billing", update_source.mutation.req.json_set_expressions[0].path[0]);
            try std.testing.expectEqualStrings("status_key", update_source.mutation.req.json_set_expressions[0].path[1]);

            var result = try db.mutateRelationalRowsFromSource(alloc, schema, update_source.mutation.req);
            defer result.deinit(alloc);
            try std.testing.expectEqual(@as(u32, 1), result.matched);
            try std.testing.expectEqual(@as(u32, 1), result.staged);
            try std.testing.expectEqual(@as(usize, 1), result.returning_rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"u1\",\"metadata.billing.status_key\":\"queued\"}", result.returning_rows[0]);
        },
        else => return error.TestUnexpectedResult,
    }

    try db.commitTransaction(txn_id, 4_010);
    committed = true;

    const select = [_][]const u8{ "id", "metadata.billing.status_key" };
    const predicates = [_]runtime_schema.RelationalCheck{
        .{ .name = "", .field = "id", .op = .eq, .value_json = "\"u1\"" },
    };
    var rows = try db.queryRelationalRows(alloc, schema, .{
        .select = select[0..],
        .predicates = predicates[0..],
    });
    defer rows.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), rows.total);
    try std.testing.expectEqual(@as(usize, 1), rows.rows.len);
    try std.testing.expectEqualStrings("{\"id\":\"u1\",\"metadata\":{\"billing\":{\"status_key\":\"queued\"}}}", rows.rows[0]);

    var untouched = (try db.lookup(alloc, "row:u2", .{})).?;
    defer untouched.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, untouched.json, "status_key") == null);
}

test "postgres sql adapter mutation source expression transforms execute through relational storage" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"amount":{"type":"numeric"},"organization_id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/sql-mutation-source-expression-transforms", .{tmp.sub_path});
    defer alloc.free(path);

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();
    try db.applyTableSchemaJson(alloc, schema_json, .{});

    try db.batch(.{
        .writes = &.{
            .{ .key = "row:u1", .value = "{\"id\":\"u1\",\"status\":\"QUEUED\",\"amount\":2,\"organization_id\":\"o1\"}" },
            .{ .key = "row:u2", .value = "{\"id\":\"u2\",\"status\":\"READY\",\"amount\":4,\"organization_id\":\"o1\"}" },
            .{ .key = "row:u3", .value = "{\"id\":\"u3\",\"status\":\"SKIP\",\"amount\":8,\"organization_id\":\"o2\"}" },
        },
        .sync_level = .write,
    });

    const txn_id = try db.beginTransaction(4_101);
    var committed = false;
    defer if (!committed) db.abortTransaction(txn_id, 4_102) catch {};
    const claim: db_mod.types.RowClaimRequest = .{
        .mode = .for_update,
        .owner_id = "sql-expression-update",
        .txn_id = txn_id,
    };

    var plan = try lowerWritePlanAlloc(
        alloc,
        "UPDATE usage_records SET status = lower(status) || ':' || id, amount = amount + coalesce(amount, 1) WHERE organization_id = 'o1' ORDER BY id ASC FOR UPDATE RETURNING id, status, amount, lower(status) AS status_key",
        schema,
        &.{},
        .{ .row_claim = claim },
    );
    defer plan.deinit(alloc);

    switch (plan) {
        .update_source => |update_source| {
            try std.testing.expectEqual(@as(usize, 1), update_source.mutation.req.patch_expressions.len);
            try std.testing.expectEqualStrings("status", update_source.mutation.req.patch_expressions[0].field);
            try std.testing.expectEqual(@as(usize, 1), update_source.mutation.req.increment_expressions.len);
            try std.testing.expectEqualStrings("amount", update_source.mutation.req.increment_expressions[0].field);
            try std.testing.expectEqual(@as(usize, 1), update_source.mutation.req.returning_expressions.len);

            var result = try db.mutateRelationalRowsFromSource(alloc, schema, update_source.mutation.req);
            defer result.deinit(alloc);
            try std.testing.expectEqual(@as(u32, 2), result.matched);
            try std.testing.expectEqual(@as(u32, 2), result.staged);
            try std.testing.expectEqual(@as(usize, 2), result.returning_rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"u1\",\"status\":\"queued:u1\",\"amount\":4,\"status_key\":\"queued:u1\"}", result.returning_rows[0]);
            try std.testing.expectEqualStrings("{\"id\":\"u2\",\"status\":\"ready:u2\",\"amount\":8,\"status_key\":\"ready:u2\"}", result.returning_rows[1]);
        },
        else => return error.TestUnexpectedResult,
    }

    try db.commitTransaction(txn_id, 4_110);
    committed = true;

    const select = [_][]const u8{ "id", "status", "amount" };
    const order_by = [_]db_mod.types.RelationalRowsQueryOrder{.{
        .field = "id",
        .direction = .asc,
    }};
    var rows = try db.queryRelationalRows(alloc, schema, .{
        .select = select[0..],
        .order_by = order_by[0..],
    });
    defer rows.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 3), rows.total);
    try std.testing.expectEqual(@as(usize, 3), rows.rows.len);
    try std.testing.expectEqualStrings("{\"id\":\"u1\",\"status\":\"queued:u1\",\"amount\":4}", rows.rows[0]);
    try std.testing.expectEqualStrings("{\"id\":\"u2\",\"status\":\"ready:u2\",\"amount\":8}", rows.rows[1]);
    try std.testing.expectEqualStrings("{\"id\":\"u3\",\"status\":\"SKIP\",\"amount\":8}", rows.rows[2]);

    const text_txn_id = try db.beginTransaction(4_201);
    var text_committed = false;
    defer if (!text_committed) db.abortTransaction(text_txn_id, 4_202) catch {};
    const text_claim: db_mod.types.RowClaimRequest = .{
        .mode = .for_update,
        .owner_id = "sql-expression-text-update",
        .txn_id = text_txn_id,
    };

    var text_plan = try lowerWritePlanAlloc(
        alloc,
        "UPDATE usage_records SET status = split_part(status, ':', 1) || '-done', amount = regexp_count(status, '[a-z]+') + position(':' in status) WHERE organization_id = 'o1' ORDER BY id ASC FOR UPDATE RETURNING id, status, amount, split_part(status, '-', 1) AS status_prefix",
        schema,
        &.{},
        .{ .row_claim = text_claim },
    );
    defer text_plan.deinit(alloc);

    switch (text_plan) {
        .update_source => |update_source| {
            try std.testing.expectEqual(@as(usize, 2), update_source.mutation.req.patch_expressions.len);
            try std.testing.expectEqualStrings("status", update_source.mutation.req.patch_expressions[0].field);
            try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.concat, update_source.mutation.req.patch_expressions[0].expression.kind);
            try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.split_part, update_source.mutation.req.patch_expressions[0].expression.operands[0].kind);
            try std.testing.expectEqualStrings("amount", update_source.mutation.req.patch_expressions[1].field);
            try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.add, update_source.mutation.req.patch_expressions[1].expression.kind);
            try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.regexp_count, update_source.mutation.req.patch_expressions[1].expression.operands[0].kind);
            try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.strpos, update_source.mutation.req.patch_expressions[1].expression.operands[1].kind);
            try std.testing.expectEqual(@as(usize, 1), update_source.mutation.req.returning_expressions.len);
            try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.split_part, update_source.mutation.req.returning_expressions[0].expression.kind);

            var result = try db.mutateRelationalRowsFromSource(alloc, schema, update_source.mutation.req);
            defer result.deinit(alloc);
            try std.testing.expectEqual(@as(u32, 2), result.matched);
            try std.testing.expectEqual(@as(u32, 2), result.staged);
            try std.testing.expectEqual(@as(usize, 2), result.returning_rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"u1\",\"status\":\"queued-done\",\"amount\":9,\"status_prefix\":\"queued\"}", result.returning_rows[0]);
            try std.testing.expectEqualStrings("{\"id\":\"u2\",\"status\":\"ready-done\",\"amount\":8,\"status_prefix\":\"ready\"}", result.returning_rows[1]);
        },
        else => return error.TestUnexpectedResult,
    }

    try db.commitTransaction(text_txn_id, 4_210);
    text_committed = true;

    var text_rows = try db.queryRelationalRows(alloc, schema, .{
        .select = select[0..],
        .order_by = order_by[0..],
    });
    defer text_rows.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 3), text_rows.total);
    try std.testing.expectEqual(@as(usize, 3), text_rows.rows.len);
    try std.testing.expectEqualStrings("{\"id\":\"u1\",\"status\":\"queued-done\",\"amount\":9}", text_rows.rows[0]);
    try std.testing.expectEqualStrings("{\"id\":\"u2\",\"status\":\"ready-done\",\"amount\":8}", text_rows.rows[1]);
    try std.testing.expectEqualStrings("{\"id\":\"u3\",\"status\":\"SKIP\",\"amount\":8}", text_rows.rows[2]);
}

test "postgres sql adapter insert source unique conflict executes through relational storage" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"source_id":{"type":"keyword"},"target_source_id":{"type":"keyword"},"status":{"type":"keyword","default":"active"}},"required":["id","source_id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"unique_constraints":[{"name":"usage_records_source_id_key","columns":["source_id"]}]}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/sql-insert-source-unique-conflict", .{tmp.sub_path});
    defer alloc.free(path);

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();
    try db.applyTableSchemaJson(alloc, schema_json, .{});

    try db.batch(.{
        .writes = &.{
            .{ .key = "row:existing", .value = "{\"id\":\"existing\",\"source_id\":\"source:1\",\"status\":\"old\"}" },
            .{ .key = "row:source", .value = "{\"id\":\"source\",\"source_id\":\"source:input\",\"target_source_id\":\"source:1\",\"status\":\"new\"}" },
            .{ .key = "row:source_default", .value = "{\"id\":\"source_default\",\"source_id\":\"source:input:default\",\"target_source_id\":\"source:1\",\"status\":\"ignored\"}" },
            .{ .key = "row:source_guard", .value = "{\"id\":\"source_guard\",\"source_id\":\"source:input:guard\",\"target_source_id\":\"source:1\",\"status\":\"guarded\"}" },
            .{ .key = "row:source_nothing", .value = "{\"id\":\"source_nothing\",\"source_id\":\"source:input:2\",\"target_source_id\":\"source:1\",\"status\":\"ignored\"}" },
        },
        .sync_level = .write,
    });

    const existing_version = try db.getTimestamp(alloc, "row:existing");
    var resolver_ctx = TestPrimaryResolver{
        .row_json = "{\"id\":\"existing\",\"source_id\":\"source:1\",\"status\":\"old\"}",
        .version = existing_version,
        .resolved_key = "row:existing",
    };
    var plan = try lowerWritePlanAlloc(
        alloc,
        "INSERT INTO usage_records (id, source_id, status) SELECT id || '_copy' AS id, target_source_id AS source_id, upper(status) AS status FROM usage_records WHERE id = 'source' ON CONFLICT (source_id) DO UPDATE SET status = excluded.status RETURNING id, source_id, status",
        schema,
        &.{},
        .{ .unique_resolver = resolver_ctx.resolver() },
    );
    defer plan.deinit(alloc);

    switch (plan) {
        .insert_source => |insert_source| {
            const conflict = insert_source.insert_source.req.on_conflict orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(db_mod.types.RelationalRowsConflictTargetKind.unique, conflict.target.kind);
            try std.testing.expectEqualStrings("usage_records_source_id_key", conflict.target.unique_name);
            try std.testing.expectEqual(db_mod.types.RelationalRowsConflictAction.update, conflict.action);

            var source_result = try db.queryRelationalRows(alloc, schema, insert_source.insert_source.req.source);
            defer source_result.deinit(alloc);
            try std.testing.expectEqual(@as(u32, 1), source_result.total);
            try std.testing.expectEqual(@as(usize, 1), source_result.rows.len);

            var batch = try relational_rows.buildRowsInsertSourceBatchAlloc(
                alloc,
                insert_source.table_name,
                schema,
                insert_source.insert_source.req,
                source_result.rows,
                resolver_ctx.resolver(),
            );
            defer batch.deinit(alloc);
            try std.testing.expectEqual(@as(u32, 0), batch.inserted);
            try std.testing.expectEqual(@as(u32, 1), batch.transformed);
            try std.testing.expectEqual(@as(usize, 1), batch.returning_rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"existing\",\"source_id\":\"source:1\",\"status\":\"NEW\"}", batch.returning_rows[0]);
            try db.batch(batch.req);
        },
        else => return error.TestUnexpectedResult,
    }

    const existing_after_update_version = try db.getTimestamp(alloc, "row:existing");
    var default_resolver_ctx = TestPrimaryResolver{
        .row_json = "{\"id\":\"existing\",\"source_id\":\"source:1\",\"status\":\"NEW\"}",
        .version = existing_after_update_version,
        .resolved_key = "row:existing",
    };
    var default_plan = try lowerWritePlanAlloc(
        alloc,
        "INSERT INTO usage_records (id, source_id, status) SELECT id || '_copy' AS id, target_source_id AS source_id, upper(status) AS status FROM usage_records WHERE id = 'source_default' ON CONFLICT (source_id) DO UPDATE SET status = DEFAULT RETURNING id, source_id, status",
        schema,
        &.{},
        .{ .unique_resolver = default_resolver_ctx.resolver() },
    );
    defer default_plan.deinit(alloc);

    switch (default_plan) {
        .insert_source => |insert_source| {
            const conflict = insert_source.insert_source.req.on_conflict orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(db_mod.types.RelationalRowsConflictAction.update, conflict.action);
            try std.testing.expectEqual(@as(usize, 1), conflict.operations.len);
            try std.testing.expectEqualStrings("status", conflict.operations[0].path);
            try std.testing.expectEqualStrings("\"active\"", conflict.operations[0].value_json.?);

            var source_result = try db.queryRelationalRows(alloc, schema, insert_source.insert_source.req.source);
            defer source_result.deinit(alloc);
            try std.testing.expectEqual(@as(u32, 1), source_result.total);
            try std.testing.expectEqual(@as(usize, 1), source_result.rows.len);

            var batch = try relational_rows.buildRowsInsertSourceBatchAlloc(
                alloc,
                insert_source.table_name,
                schema,
                insert_source.insert_source.req,
                source_result.rows,
                default_resolver_ctx.resolver(),
            );
            defer batch.deinit(alloc);
            try std.testing.expectEqual(@as(u32, 0), batch.inserted);
            try std.testing.expectEqual(@as(u32, 1), batch.transformed);
            try std.testing.expectEqual(@as(usize, 1), batch.returning_rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"existing\",\"source_id\":\"source:1\",\"status\":\"active\"}", batch.returning_rows[0]);
            try db.batch(batch.req);
        },
        else => return error.TestUnexpectedResult,
    }

    const existing_after_default_version = try db.getTimestamp(alloc, "row:existing");
    var guarded_resolver_ctx = TestPrimaryResolver{
        .row_json = "{\"id\":\"existing\",\"source_id\":\"source:1\",\"status\":\"active\"}",
        .version = existing_after_default_version,
        .resolved_key = "row:existing",
    };
    var guarded_plan = try lowerWritePlanAlloc(
        alloc,
        "INSERT INTO usage_records (id, source_id, status) SELECT id || '_copy' AS id, target_source_id AS source_id, upper(status) AS status FROM usage_records WHERE id = 'source_guard' ON CONFLICT (source_id) DO UPDATE SET status = excluded.status WHERE excluded.status = status RETURNING id, source_id, status",
        schema,
        &.{},
        .{ .unique_resolver = guarded_resolver_ctx.resolver() },
    );
    defer guarded_plan.deinit(alloc);

    switch (guarded_plan) {
        .insert_source => |insert_source| {
            const conflict = insert_source.insert_source.req.on_conflict orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(db_mod.types.RelationalRowsConflictTargetKind.unique, conflict.target.kind);
            try std.testing.expectEqualStrings("usage_records_source_id_key", conflict.target.unique_name);
            try std.testing.expectEqual(db_mod.types.RelationalRowsConflictAction.update, conflict.action);
            try std.testing.expect(conflict.where_expression != null);

            var source_result = try db.queryRelationalRows(alloc, schema, insert_source.insert_source.req.source);
            defer source_result.deinit(alloc);
            try std.testing.expectEqual(@as(u32, 1), source_result.total);
            try std.testing.expectEqual(@as(usize, 1), source_result.rows.len);

            var batch = try relational_rows.buildRowsInsertSourceBatchAlloc(
                alloc,
                insert_source.table_name,
                schema,
                insert_source.insert_source.req,
                source_result.rows,
                guarded_resolver_ctx.resolver(),
            );
            defer batch.deinit(alloc);
            try std.testing.expectEqual(@as(u32, 0), batch.inserted);
            try std.testing.expectEqual(@as(u32, 0), batch.transformed);
            try std.testing.expectEqual(@as(usize, 0), batch.returning_rows.len);
            try db.batch(batch.req);
        },
        else => return error.TestUnexpectedResult,
    }

    const existing_after_guard_version = try db.getTimestamp(alloc, "row:existing");
    var do_nothing_resolver_ctx = TestPrimaryResolver{
        .row_json = "{\"id\":\"existing\",\"source_id\":\"source:1\",\"status\":\"active\"}",
        .version = existing_after_guard_version,
        .resolved_key = "row:existing",
    };
    var do_nothing_plan = try lowerWritePlanAlloc(
        alloc,
        "INSERT INTO usage_records (id, source_id, status) SELECT id || '_copy' AS id, target_source_id AS source_id, upper(status) AS status FROM usage_records WHERE id = 'source_nothing' ON CONFLICT (source_id) DO NOTHING RETURNING id, source_id, status",
        schema,
        &.{},
        .{ .unique_resolver = do_nothing_resolver_ctx.resolver() },
    );
    defer do_nothing_plan.deinit(alloc);

    switch (do_nothing_plan) {
        .insert_source => |insert_source| {
            const conflict = insert_source.insert_source.req.on_conflict orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(db_mod.types.RelationalRowsConflictTargetKind.unique, conflict.target.kind);
            try std.testing.expectEqualStrings("usage_records_source_id_key", conflict.target.unique_name);
            try std.testing.expectEqual(db_mod.types.RelationalRowsConflictAction.nothing, conflict.action);

            var source_result = try db.queryRelationalRows(alloc, schema, insert_source.insert_source.req.source);
            defer source_result.deinit(alloc);
            try std.testing.expectEqual(@as(u32, 1), source_result.total);
            try std.testing.expectEqual(@as(usize, 1), source_result.rows.len);

            var batch = try relational_rows.buildRowsInsertSourceBatchAlloc(
                alloc,
                insert_source.table_name,
                schema,
                insert_source.insert_source.req,
                source_result.rows,
                do_nothing_resolver_ctx.resolver(),
            );
            defer batch.deinit(alloc);
            try std.testing.expectEqual(@as(u32, 0), batch.inserted);
            try std.testing.expectEqual(@as(u32, 0), batch.transformed);
            try std.testing.expectEqual(@as(usize, 0), batch.returning_rows.len);
            try db.batch(batch.req);
        },
        else => return error.TestUnexpectedResult,
    }

    const select = [_][]const u8{ "id", "source_id", "status" };
    var rows = try db.queryRelationalRows(alloc, schema, .{
        .select = select[0..],
        .predicates = &.{.{
            .name = "",
            .field = "id",
            .op = .eq,
            .value_json = "\"existing\"",
        }},
    });
    defer rows.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), rows.total);
    try std.testing.expectEqual(@as(usize, 1), rows.rows.len);
    try std.testing.expectEqualStrings("{\"id\":\"existing\",\"source_id\":\"source:1\",\"status\":\"active\"}", rows.rows[0]);
}

test "postgres sql adapter insert source temporal unique conflict executes through relational storage" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"sku":{"type":"keyword"},"target_sku":{"type":"keyword"},"valid_from":{"type":"numeric"},"valid_to":{"type":"numeric"},"price":{"type":"numeric"}},"required":["id","sku","valid_from","valid_to"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"periods":[{"name":"valid_time","start_column":"valid_from","end_column":"valid_to"}],"unique_constraints":[{"name":"prices_sku_time_key","columns":["sku"],"without_overlaps_period":"valid_time"}]}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/sql-insert-source-temporal-unique-conflict", .{tmp.sub_path});
    defer alloc.free(path);

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();
    try db.applyTableSchemaJson(alloc, schema_json, .{});

    try db.batch(.{
        .writes = &.{
            .{ .key = "row:existing", .value = "{\"id\":\"existing\",\"sku\":\"sku:a\",\"valid_from\":0,\"valid_to\":10,\"price\":10}" },
            .{ .key = "row:source", .value = "{\"id\":\"source\",\"sku\":\"sku:source\",\"target_sku\":\"sku:a\",\"valid_from\":5,\"valid_to\":15,\"price\":12}" },
        },
        .sync_level = .write,
    });

    const existing_version = try db.getTimestamp(alloc, "row:existing");
    var resolver_ctx = TestPrimaryResolver{
        .row_json = "{\"id\":\"existing\",\"sku\":\"sku:a\",\"valid_from\":0,\"valid_to\":10,\"price\":10}",
        .version = existing_version,
        .resolved_key = "row:existing",
    };
    var plan = try lowerWritePlanAlloc(
        alloc,
        "INSERT INTO prices (id, sku, valid_from, valid_to, price) SELECT id || '_copy' AS id, target_sku AS sku, valid_from, valid_to, price + 1 AS price FROM prices WHERE id = 'source' ON CONFLICT ON CONSTRAINT prices_sku_time_key DO UPDATE SET price = excluded.price RETURNING id, sku, valid_from, valid_to, price",
        schema,
        &.{},
        .{ .unique_resolver = resolver_ctx.resolver() },
    );
    defer plan.deinit(alloc);

    switch (plan) {
        .insert_source => |insert_source| {
            const conflict = insert_source.insert_source.req.on_conflict orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(db_mod.types.RelationalRowsConflictTargetKind.unique, conflict.target.kind);
            try std.testing.expectEqualStrings("prices_sku_time_key", conflict.target.unique_name);
            try std.testing.expectEqual(db_mod.types.RelationalRowsConflictAction.update, conflict.action);

            var source_result = try db.queryRelationalRows(alloc, schema, insert_source.insert_source.req.source);
            defer source_result.deinit(alloc);
            try std.testing.expectEqual(@as(u32, 1), source_result.total);
            try std.testing.expectEqual(@as(usize, 1), source_result.rows.len);

            var batch = try relational_rows.buildRowsInsertSourceBatchAlloc(
                alloc,
                insert_source.table_name,
                schema,
                insert_source.insert_source.req,
                source_result.rows,
                resolver_ctx.resolver(),
            );
            defer batch.deinit(alloc);
            try std.testing.expectEqual(@as(u32, 0), batch.inserted);
            try std.testing.expectEqual(@as(u32, 1), batch.transformed);
            try std.testing.expectEqual(@as(usize, 1), batch.returning_rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"existing\",\"sku\":\"sku:a\",\"valid_from\":0,\"valid_to\":10,\"price\":13}", batch.returning_rows[0]);
            try db.batch(batch.req);
        },
        else => return error.TestUnexpectedResult,
    }

    const select = [_][]const u8{ "id", "sku", "valid_from", "valid_to", "price" };
    var rows = try db.queryRelationalRows(alloc, schema, .{
        .select = select[0..],
        .predicates = &.{.{
            .name = "",
            .field = "id",
            .op = .eq,
            .value_json = "\"existing\"",
        }},
    });
    defer rows.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), rows.total);
    try std.testing.expectEqual(@as(usize, 1), rows.rows.len);
    try std.testing.expectEqualStrings("{\"id\":\"existing\",\"sku\":\"sku:a\",\"valid_from\":0,\"valid_to\":10,\"price\":13}", rows.rows[0]);
}

test "postgres sql adapter typed read plans execute through relational storage" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"kind":{"type":"keyword"},"tenant":{"type":"keyword"},"id":{"type":"keyword"},"customer_id":{"type":"keyword"},"organization_id":{"type":"keyword"},"status":{"type":"keyword"},"name":{"type":"keyword"},"enabled":{"type":"boolean"},"amount":{"type":"numeric"},"created_at":{"type":"numeric"},"scope":{"type":"keyword"},"tags":{"type":"array","items":{"type":"keyword"}},"metadata":{"type":"json"}},"required":["kind","tenant","id"],"additionalProperties":false}}},"primary_key":{"columns":["kind","tenant","id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/sql-read-plan-execution", .{tmp.sub_path});
    defer alloc.free(path);

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();
    try db.applyTableSchemaJson(alloc, schema_json, .{});

    try db.batch(.{
        .writes = &.{
            .{ .key = "row:c1", .value = "{\"kind\":\"customer\",\"tenant\":\"t1\",\"id\":\"c1\",\"name\":\"Alice\"}" },
            .{ .key = "row:c2", .value = "{\"kind\":\"customer\",\"tenant\":\"t1\",\"id\":\"c2\",\"name\":\"Bob\"}" },
            .{ .key = "row:o1", .value = "{\"kind\":\"order\",\"tenant\":\"t1\",\"id\":\"o1\",\"customer_id\":\"c1\",\"status\":\"open\",\"enabled\":true,\"amount\":10,\"scope\":\"read write\",\"tags\":[\"hot\",\"new\"],\"metadata\":{\"source\":\"api\",\"flags\":[\"rated\"]}}" },
            .{ .key = "row:o2", .value = "{\"kind\":\"order\",\"tenant\":\"t1\",\"id\":\"o2\",\"customer_id\":\"missing\",\"status\":\"open\",\"enabled\":false,\"amount\":5,\"scope\":\"read\",\"tags\":[\"cold\"],\"metadata\":{\"source\":\"batch\",\"flags\":[\"manual\"]}}" },
            .{ .key = "row:o3", .value = "{\"kind\":\"order\",\"tenant\":\"t2\",\"id\":\"o3\",\"customer_id\":\"c1\",\"status\":\"open\",\"enabled\":true,\"amount\":7,\"scope\":\"write\",\"tags\":[\"hot\"],\"metadata\":{\"source\":\"api\",\"flags\":[\"rated\"]}}" },
            .{ .key = "row:p1", .value = "{\"kind\":\"pattern\",\"tenant\":\"t1\",\"id\":\"p1\",\"status\":\"op_en\"}" },
            .{ .key = "row:p2", .value = "{\"kind\":\"pattern\",\"tenant\":\"t1\",\"id\":\"p2\",\"status\":\"open\"}" },
            .{ .key = "row:p3", .value = "{\"kind\":\"pattern\",\"tenant\":\"t1\",\"id\":\"p3\",\"status\":\"OP_en\"}" },
            .{ .key = "row:org1", .value = "{\"kind\":\"organization\",\"tenant\":\"t1\",\"id\":\"org1\"}" },
            .{ .key = "row:org2", .value = "{\"kind\":\"organization\",\"tenant\":\"t1\",\"id\":\"org2\"}" },
            .{ .key = "row:org3", .value = "{\"kind\":\"organization\",\"tenant\":\"t1\",\"id\":\"org3\"}" },
            .{ .key = "row:b1", .value = "{\"kind\":\"balance\",\"tenant\":\"t1\",\"id\":\"b1\",\"organization_id\":\"org1\",\"amount\":4,\"created_at\":10}" },
            .{ .key = "row:b2", .value = "{\"kind\":\"balance\",\"tenant\":\"t1\",\"id\":\"b2\",\"organization_id\":\"org1\",\"amount\":9,\"created_at\":20}" },
            .{ .key = "row:b3", .value = "{\"kind\":\"balance\",\"tenant\":\"t1\",\"id\":\"b3\",\"organization_id\":\"org2\",\"amount\":7,\"created_at\":15}" },
            .{ .key = "row:a1", .value = "{\"kind\":\"array\",\"tenant\":\"t1\",\"id\":\"a1\",\"scope\":\"read write\",\"tags\":[\"hot\",\"old\",\"hot\"]}" },
            .{ .key = "row:a2", .value = "{\"kind\":\"array\",\"tenant\":\"t1\",\"id\":\"a2\",\"scope\":\"read\",\"tags\":[\"cold\"]}" },
        },
        .sync_level = .write,
    });

    var escaped_pattern_plan = try lowerReadPlanAlloc(
        alloc,
        "SELECT id, status FROM usage_records WHERE kind = 'pattern' AND status ILIKE $1 ESCAPE $2 ORDER BY id",
        schema,
        &.{ .{ .string = "OP!_%" }, .{ .string = "!" } },
    );
    defer escaped_pattern_plan.deinit(alloc);

    switch (escaped_pattern_plan) {
        .query => |lowered| {
            try std.testing.expectEqual(@as(usize, 1), lowered.plan.query.predicates.len);
            try std.testing.expectEqual(@as(usize, 1), lowered.plan.query.text_patterns.len);
            try std.testing.expectEqualStrings("OP\\_%", lowered.plan.query.text_patterns[0].pattern);
            try std.testing.expect(lowered.plan.query.text_patterns[0].case_insensitive);
            var result = try db.queryRelationalRows(alloc, schema, lowered.plan.query);
            defer result.deinit(alloc);

            try std.testing.expectEqual(@as(u32, 2), result.total);
            try std.testing.expectEqual(@as(usize, 2), result.rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"p1\",\"status\":\"op_en\"}", result.rows[0]);
            try std.testing.expectEqualStrings("{\"id\":\"p3\",\"status\":\"OP_en\"}", result.rows[1]);
        },
        else => return error.TestUnexpectedResult,
    }

    var computed_pattern_plan = try lowerReadPlanAlloc(
        alloc,
        "SELECT id, status FROM usage_records WHERE kind = 'pattern' AND lower(status) LIKE $1 ESCAPE $2 ORDER BY id",
        schema,
        &.{ .{ .string = "%!_en" }, .{ .string = "!" } },
    );
    defer computed_pattern_plan.deinit(alloc);

    switch (computed_pattern_plan) {
        .query => |lowered| {
            try std.testing.expectEqual(@as(usize, 1), lowered.plan.query.predicates.len);
            try std.testing.expectEqual(@as(usize, 1), lowered.plan.query.expression_predicates.len);
            try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.like, lowered.plan.query.expression_predicates[0].lhs.kind);
            var result = try db.queryRelationalRows(alloc, schema, lowered.plan.query);
            defer result.deinit(alloc);

            try std.testing.expectEqual(@as(u32, 2), result.total);
            try std.testing.expectEqual(@as(usize, 2), result.rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"p1\",\"status\":\"op_en\"}", result.rows[0]);
            try std.testing.expectEqualStrings("{\"id\":\"p3\",\"status\":\"OP_en\"}", result.rows[1]);
        },
        else => return error.TestUnexpectedResult,
    }

    var regexp_match_plan = try lowerReadPlanAlloc(
        alloc,
        "SELECT id, status FROM usage_records WHERE kind = 'pattern' AND status ~ $1 AND status !~* 'closed' ORDER BY id",
        schema,
        &.{.{ .string = "^op" }},
    );
    defer regexp_match_plan.deinit(alloc);

    switch (regexp_match_plan) {
        .query => |lowered| {
            try std.testing.expectEqual(@as(usize, 1), lowered.plan.query.predicates.len);
            try std.testing.expectEqual(@as(usize, 2), lowered.plan.query.expression_predicates.len);
            try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.regexp_match, lowered.plan.query.expression_predicates[0].lhs.kind);
            try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.regexp_match, lowered.plan.query.expression_predicates[1].lhs.kind);
            var result = try db.queryRelationalRows(alloc, schema, lowered.plan.query);
            defer result.deinit(alloc);

            try std.testing.expectEqual(@as(u32, 2), result.total);
            try std.testing.expectEqual(@as(usize, 2), result.rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"p1\",\"status\":\"op_en\"}", result.rows[0]);
            try std.testing.expectEqualStrings("{\"id\":\"p2\",\"status\":\"open\"}", result.rows[1]);
        },
        else => return error.TestUnexpectedResult,
    }

    var regexp_like_plan = try lowerReadPlanAlloc(
        alloc,
        "SELECT id, regexp_like(status, $2, true) AS matches_suffix FROM usage_records WHERE kind = 'pattern' AND regexp_like(status, $1) ORDER BY id",
        schema,
        &.{ .{ .string = "^op" }, .{ .string = "_EN$" } },
    );
    defer regexp_like_plan.deinit(alloc);

    switch (regexp_like_plan) {
        .query => |lowered| {
            try std.testing.expectEqual(@as(usize, 1), lowered.plan.query.predicates.len);
            try std.testing.expectEqual(@as(usize, 1), lowered.plan.query.expression_predicates.len);
            try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.regexp_match, lowered.plan.query.expression_predicates[0].lhs.kind);
            try std.testing.expectEqual(@as(usize, 1), lowered.plan.query.expressions.len);
            try std.testing.expectEqualStrings("matches_suffix", lowered.plan.query.expressions[0].output);
            try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.regexp_match, lowered.plan.query.expressions[0].expression.kind);
            try std.testing.expectEqual(@as(usize, 3), lowered.plan.query.expressions[0].expression.operands.len);
            var result = try db.queryRelationalRows(alloc, schema, lowered.plan.query);
            defer result.deinit(alloc);

            try std.testing.expectEqual(@as(u32, 2), result.total);
            try std.testing.expectEqual(@as(usize, 2), result.rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"p1\",\"matches_suffix\":true}", result.rows[0]);
            try std.testing.expectEqualStrings("{\"id\":\"p2\",\"matches_suffix\":false}", result.rows[1]);
        },
        else => return error.TestUnexpectedResult,
    }

    var computed_pattern_any_plan = try lowerReadPlanAlloc(
        alloc,
        "SELECT id, status FROM usage_records WHERE kind = 'pattern' AND lower(status) LIKE ANY(ARRAY['op_en', 'open']) ORDER BY id",
        schema,
        &.{},
    );
    defer computed_pattern_any_plan.deinit(alloc);

    switch (computed_pattern_any_plan) {
        .query => |lowered| {
            try std.testing.expectEqual(@as(usize, 1), lowered.plan.query.predicates.len);
            try std.testing.expectEqual(@as(usize, 1), lowered.plan.query.expression_predicates.len);
            try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.bool_or, lowered.plan.query.expression_predicates[0].lhs.kind);
            var result = try db.queryRelationalRows(alloc, schema, lowered.plan.query);
            defer result.deinit(alloc);

            try std.testing.expectEqual(@as(u32, 3), result.total);
            try std.testing.expectEqual(@as(usize, 3), result.rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"p1\",\"status\":\"op_en\"}", result.rows[0]);
            try std.testing.expectEqualStrings("{\"id\":\"p2\",\"status\":\"open\"}", result.rows[1]);
            try std.testing.expectEqualStrings("{\"id\":\"p3\",\"status\":\"OP_en\"}", result.rows[2]);
        },
        else => return error.TestUnexpectedResult,
    }

    var computed_pattern_some_plan = try lowerReadPlanAlloc(
        alloc,
        "SELECT id, status FROM usage_records WHERE kind = 'pattern' AND lower(status) LIKE SOME(ARRAY['op_en', 'open']) ORDER BY id",
        schema,
        &.{},
    );
    defer computed_pattern_some_plan.deinit(alloc);

    switch (computed_pattern_some_plan) {
        .query => |lowered| {
            try std.testing.expectEqual(@as(usize, 1), lowered.plan.query.predicates.len);
            try std.testing.expectEqual(@as(usize, 1), lowered.plan.query.expression_predicates.len);
            try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.bool_or, lowered.plan.query.expression_predicates[0].lhs.kind);
            var result = try db.queryRelationalRows(alloc, schema, lowered.plan.query);
            defer result.deinit(alloc);

            try std.testing.expectEqual(@as(u32, 3), result.total);
            try std.testing.expectEqual(@as(usize, 3), result.rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"p1\",\"status\":\"op_en\"}", result.rows[0]);
            try std.testing.expectEqualStrings("{\"id\":\"p2\",\"status\":\"open\"}", result.rows[1]);
            try std.testing.expectEqualStrings("{\"id\":\"p3\",\"status\":\"OP_en\"}", result.rows[2]);
        },
        else => return error.TestUnexpectedResult,
    }

    var array_expression_plan = try lowerReadPlanAlloc(
        alloc,
        "SELECT id, array_position(tags, 'hot') AS hot_pos, array_positions(tags, 'hot') AS hot_positions, array_append(tags, 'fresh') AS tags_plus, array_prepend('first', tags) AS tags_prefixed, array_cat(tags, string_to_array('tail last', ' ')) AS tags_cat, array_remove(tags, 'old') AS tags_clean, array_replace(tags, 'old', 'fresh') AS tags_replaced FROM usage_records WHERE kind = 'array' AND array_position(tags, 'hot') > 0 AND string_to_array(scope, ' ') @> ARRAY['write'] ORDER BY array_position(tags, 'old') ASC NULLS LAST, id ASC",
        schema,
        &.{},
    );
    defer array_expression_plan.deinit(alloc);

    switch (array_expression_plan) {
        .query => |lowered| {
            try std.testing.expectEqual(@as(usize, 1), lowered.plan.query.predicates.len);
            try std.testing.expectEqual(@as(usize, 1), lowered.plan.query.expression_predicates.len);
            try std.testing.expectEqual(@as(usize, 1), lowered.plan.query.expression_array_contains.len);
            try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.array_position, lowered.plan.query.expressions[0].expression.kind);
            try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.array_positions, lowered.plan.query.expressions[1].expression.kind);
            try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.array_append, lowered.plan.query.expressions[2].expression.kind);
            try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.array_prepend, lowered.plan.query.expressions[3].expression.kind);
            try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.array_cat, lowered.plan.query.expressions[4].expression.kind);
            try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.array_remove, lowered.plan.query.expressions[5].expression.kind);
            try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.array_replace, lowered.plan.query.expressions[6].expression.kind);
            try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.string_to_array, lowered.plan.query.expression_array_contains[0].expression.kind);
            var result = try db.queryRelationalRows(alloc, schema, lowered.plan.query);
            defer result.deinit(alloc);

            try std.testing.expectEqual(@as(u32, 1), result.total);
            try std.testing.expectEqual(@as(usize, 1), result.rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"a1\",\"hot_pos\":1,\"hot_positions\":[1,3],\"tags_plus\":[\"hot\",\"old\",\"hot\",\"fresh\"],\"tags_prefixed\":[\"first\",\"hot\",\"old\",\"hot\"],\"tags_cat\":[\"hot\",\"old\",\"hot\",\"tail\",\"last\"],\"tags_clean\":[\"hot\",\"hot\"],\"tags_replaced\":[\"hot\",\"fresh\",\"hot\"]}", result.rows[0]);
        },
        else => return error.TestUnexpectedResult,
    }

    var mixed_except_plan = try lowerReadPlanAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE kind = 'order' EXCEPT SELECT id FROM usage_records WHERE status = 'open' AND lower(tenant) = 't1' ORDER BY id ASC",
        schema,
        &.{},
    );
    defer mixed_except_plan.deinit(alloc);

    switch (mixed_except_plan) {
        .query => |lowered| {
            try std.testing.expectEqual(@as(usize, 1), lowered.plan.query.predicates.len);
            try std.testing.expectEqual(@as(usize, 1), lowered.plan.query.expression_not_predicates.len);
            try std.testing.expectEqual(@as(usize, 2), lowered.plan.query.expression_not_predicates[0].conditions.len);
            var result = try db.queryRelationalRows(alloc, schema, lowered.plan.query);
            defer result.deinit(alloc);

            try std.testing.expectEqual(@as(u32, 1), result.total);
            try std.testing.expectEqual(@as(usize, 1), result.rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"o3\"}", result.rows[0]);
        },
        else => return error.TestUnexpectedResult,
    }

    var join_plan = try lowerReadPlanAlloc(
        alloc,
        "SELECT o.id AS order_id, c.name AS customer_name, o.amount AS amount FROM usage_records AS o LEFT JOIN usage_records AS c ON o.tenant = c.tenant AND o.customer_id = c.id WHERE o.kind = 'order' ORDER BY amount DESC LIMIT 10",
        schema,
        &.{},
    );
    defer join_plan.deinit(alloc);

    switch (join_plan) {
        .join => |lowered| {
            try std.testing.expectEqual(db_mod.types.RelationalRowsJoinType.left, lowered.join.join_type);
            var result = try db.joinRelationalRows(alloc, schema, lowered.join);
            defer result.deinit(alloc);

            try std.testing.expectEqual(@as(u32, 3), result.total_rows);
            try std.testing.expectEqual(@as(usize, 3), result.rows.len);
            try std.testing.expectEqualStrings("{\"order_id\":\"o1\",\"customer_name\":\"Alice\",\"amount\":10}", result.rows[0]);
            try std.testing.expectEqualStrings("{\"order_id\":\"o3\",\"customer_name\":null,\"amount\":7}", result.rows[1]);
            try std.testing.expectEqualStrings("{\"order_id\":\"o2\",\"customer_name\":null,\"amount\":5}", result.rows[2]);
        },
        else => return error.TestUnexpectedResult,
    }

    var cte_query_plan = try lowerReadPlanAlloc(
        alloc,
        "WITH normalized_orders AS (SELECT tenant, id, amount, lower(status) AS status_key FROM usage_records WHERE kind = 'order'), open_orders AS (SELECT tenant, id, amount FROM normalized_orders WHERE status_key = 'open' AND amount > 6) SELECT tenant, id FROM open_orders ORDER BY amount DESC LIMIT 2",
        schema,
        &.{},
    );
    defer cte_query_plan.deinit(alloc);

    switch (cte_query_plan) {
        .query => |lowered| {
            try std.testing.expectEqual(@as(usize, 2), lowered.plan.ctes.len);
            try std.testing.expectEqualStrings("normalized_orders", lowered.plan.ctes[0].name);
            try std.testing.expectEqual(@as(usize, 1), lowered.plan.ctes[0].query.expressions.len);
            try std.testing.expectEqualStrings("status_key", lowered.plan.ctes[0].query.expressions[0].output);
            try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.lower, lowered.plan.ctes[0].query.expressions[0].expression.kind);
            try std.testing.expectEqualStrings("normalized_orders", lowered.plan.ctes[1].query.source_cte);
            try std.testing.expectEqualStrings("open_orders", lowered.plan.query.source_cte);

            var result = try db.queryRelationalRowsPlan(alloc, schema, lowered.plan);
            defer result.deinit(alloc);

            try std.testing.expectEqual(@as(u32, 2), result.total);
            try std.testing.expectEqual(@as(usize, 2), result.rows.len);
            try std.testing.expectEqualStrings("{\"tenant\":\"t1\",\"id\":\"o1\"}", result.rows[0]);
            try std.testing.expectEqualStrings("{\"tenant\":\"t2\",\"id\":\"o3\"}", result.rows[1]);
        },
        else => return error.TestUnexpectedResult,
    }

    var aggregate_plan = try lowerReadPlanAlloc(
        alloc,
        "WITH open_usage AS (SELECT tenant, id, amount, status FROM usage_records WHERE kind = 'order' AND status = 'open') SELECT tenant, SUM(amount) AS total_amount, string_agg(id, '|' ORDER BY amount DESC) AS order_ids FROM open_usage GROUP BY tenant ORDER BY total_amount DESC LIMIT 5",
        schema,
        &.{},
    );
    defer aggregate_plan.deinit(alloc);

    switch (aggregate_plan) {
        .aggregate => |lowered| {
            try std.testing.expectEqual(@as(usize, 1), lowered.plan.ctes.len);
            try std.testing.expectEqualStrings("open_usage", lowered.plan.aggregate.source.source_cte);
            var result = try db.aggregateRelationalRowsPlan(alloc, schema, lowered.plan);
            defer result.deinit(alloc);

            try std.testing.expectEqual(@as(u32, 2), result.total_groups);
            try std.testing.expectEqual(@as(usize, 2), result.rows.len);
            try std.testing.expectEqualStrings("{\"tenant\":\"t1\",\"total_amount\":15,\"order_ids\":\"o1|o2\"}", result.rows[0]);
            try std.testing.expectEqualStrings("{\"tenant\":\"t2\",\"total_amount\":7,\"order_ids\":\"o3\"}", result.rows[1]);
        },
        else => return error.TestUnexpectedResult,
    }

    var distinct_aggregate_plan = try lowerReadPlanAlloc(
        alloc,
        "SELECT tenant, COUNT(DISTINCT metadata->>'source') AS source_count, array_agg(DISTINCT metadata->'flags') AS distinct_flags, array_agg(DISTINCT id ORDER BY amount DESC) AS ordered_ids, string_agg(DISTINCT status, '|' ORDER BY amount DESC) AS status_text FROM usage_records WHERE kind = 'order' GROUP BY tenant ORDER BY tenant ASC",
        schema,
        &.{},
    );
    defer distinct_aggregate_plan.deinit(alloc);

    switch (distinct_aggregate_plan) {
        .aggregate => |lowered| {
            try std.testing.expectEqual(@as(usize, 4), lowered.plan.aggregate.aggregations.len);
            try std.testing.expectEqual(db_mod.types.RelationalRowsAggregateOp.count, lowered.plan.aggregate.aggregations[0].op);
            try std.testing.expect(lowered.plan.aggregate.aggregations[0].distinct);
            try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.json_extract, lowered.plan.aggregate.aggregations[0].expression.?.kind);
            try std.testing.expect(lowered.plan.aggregate.aggregations[0].expression.?.json_as_text);
            try std.testing.expectEqual(db_mod.types.RelationalRowsAggregateOp.array_agg, lowered.plan.aggregate.aggregations[1].op);
            try std.testing.expect(lowered.plan.aggregate.aggregations[1].distinct);
            try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.json_extract, lowered.plan.aggregate.aggregations[1].expression.?.kind);
            try std.testing.expectEqual(db_mod.types.RelationalRowsAggregateOp.array_agg, lowered.plan.aggregate.aggregations[2].op);
            try std.testing.expect(lowered.plan.aggregate.aggregations[2].distinct);
            try std.testing.expectEqual(@as(usize, 1), lowered.plan.aggregate.aggregations[2].array_order_by.len);
            try std.testing.expectEqual(db_mod.types.RelationalRowsAggregateOp.string_agg, lowered.plan.aggregate.aggregations[3].op);
            try std.testing.expect(lowered.plan.aggregate.aggregations[3].distinct);
            try std.testing.expectEqual(@as(usize, 1), lowered.plan.aggregate.aggregations[3].array_order_by.len);

            var result = try db.aggregateRelationalRowsPlan(alloc, schema, lowered.plan);
            defer result.deinit(alloc);

            try std.testing.expectEqual(@as(u32, 2), result.total_groups);
            try std.testing.expectEqual(@as(usize, 2), result.rows.len);
            try std.testing.expectEqualStrings("{\"tenant\":\"t1\",\"source_count\":2,\"distinct_flags\":[[\"rated\"],[\"manual\"]],\"ordered_ids\":[\"o1\",\"o2\"],\"status_text\":\"open\"}", result.rows[0]);
            try std.testing.expectEqualStrings("{\"tenant\":\"t2\",\"source_count\":1,\"distinct_flags\":[[\"rated\"]],\"ordered_ids\":[\"o3\"],\"status_text\":\"open\"}", result.rows[1]);
        },
        else => return error.TestUnexpectedResult,
    }

    var filtered_aggregate_plan = try lowerReadPlanAlloc(
        alloc,
        "SELECT tenant, SUM(amount) FILTER (WHERE lower(status) = 'open') AS open_amount, COUNT(*) FILTER (WHERE string_to_array(scope, ' ') @> ARRAY['write']) AS writable_count, COUNT(*) FILTER (WHERE metadata @> '{\"source\":\"api\"}'::jsonb) AS api_count, COUNT(*) FILTER (WHERE tags @> ARRAY['hot']) AS hot_count FROM usage_records WHERE kind = 'order' GROUP BY tenant ORDER BY tenant ASC",
        schema,
        &.{},
    );
    defer filtered_aggregate_plan.deinit(alloc);

    switch (filtered_aggregate_plan) {
        .aggregate => |lowered| {
            try std.testing.expectEqual(@as(usize, 4), lowered.plan.aggregate.aggregations.len);
            try std.testing.expectEqual(@as(usize, 1), lowered.plan.aggregate.aggregations[0].filter_expressions.len);
            try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.lower, lowered.plan.aggregate.aggregations[0].filter_expressions[0].lhs.kind);
            try std.testing.expectEqual(@as(usize, 1), lowered.plan.aggregate.aggregations[1].filter_expression_array_contains.len);
            try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.string_to_array, lowered.plan.aggregate.aggregations[1].filter_expression_array_contains[0].expression.kind);
            try std.testing.expectEqual(@as(usize, 1), lowered.plan.aggregate.aggregations[2].filter_json_contains.len);
            try std.testing.expectEqual(@as(usize, 1), lowered.plan.aggregate.aggregations[3].filter_array_contains.len);
            var result = try db.aggregateRelationalRowsPlan(alloc, schema, lowered.plan);
            defer result.deinit(alloc);

            try std.testing.expectEqual(@as(u32, 2), result.total_groups);
            try std.testing.expectEqual(@as(usize, 2), result.rows.len);
            try std.testing.expectEqualStrings("{\"tenant\":\"t1\",\"open_amount\":15,\"writable_count\":1,\"api_count\":1,\"hot_count\":1}", result.rows[0]);
            try std.testing.expectEqualStrings("{\"tenant\":\"t2\",\"open_amount\":7,\"writable_count\":1,\"api_count\":1,\"hot_count\":1}", result.rows[1]);
        },
        else => return error.TestUnexpectedResult,
    }

    var window_plan = try lowerReadPlanAlloc(
        alloc,
        "SELECT tenant, id, row_number() OVER (PARTITION BY tenant ORDER BY amount DESC, id ASC) AS row_num FROM usage_records WHERE kind = 'order' ORDER BY row_num ASC, id ASC LIMIT 10",
        schema,
        &.{},
    );
    defer window_plan.deinit(alloc);

    switch (window_plan) {
        .window => |lowered| {
            var result = try db.windowRelationalRowsPlan(alloc, schema, lowered.plan);
            defer result.deinit(alloc);

            try std.testing.expectEqual(@as(u32, 3), result.total_rows);
            try std.testing.expectEqual(@as(usize, 3), result.rows.len);
            try std.testing.expectEqualStrings("{\"tenant\":\"t1\",\"id\":\"o1\",\"row_num\":1}", result.rows[0]);
            try std.testing.expectEqualStrings("{\"tenant\":\"t2\",\"id\":\"o3\",\"row_num\":1}", result.rows[1]);
            try std.testing.expectEqualStrings("{\"tenant\":\"t1\",\"id\":\"o2\",\"row_num\":2}", result.rows[2]);
        },
        else => return error.TestUnexpectedResult,
    }

    var unordered_count_window_plan = try lowerReadPlanAlloc(
        alloc,
        "SELECT id, count(*) OVER () AS total_orders FROM usage_records WHERE kind = 'order' ORDER BY id ASC",
        schema,
        &.{},
    );
    defer unordered_count_window_plan.deinit(alloc);

    switch (unordered_count_window_plan) {
        .window => |lowered| {
            var result = try db.windowRelationalRowsPlan(alloc, schema, lowered.plan);
            defer result.deinit(alloc);

            try std.testing.expectEqual(@as(u32, 3), result.total_rows);
            try std.testing.expectEqual(@as(usize, 3), result.rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"o1\",\"total_orders\":3}", result.rows[0]);
            try std.testing.expectEqualStrings("{\"id\":\"o2\",\"total_orders\":3}", result.rows[1]);
            try std.testing.expectEqualStrings("{\"id\":\"o3\",\"total_orders\":3}", result.rows[2]);
        },
        else => return error.TestUnexpectedResult,
    }

    var unordered_partition_window_plan = try lowerReadPlanAlloc(
        alloc,
        "SELECT tenant, id, sum(amount) OVER (PARTITION BY tenant) AS tenant_amount FROM usage_records WHERE kind = 'order' ORDER BY id ASC",
        schema,
        &.{},
    );
    defer unordered_partition_window_plan.deinit(alloc);

    switch (unordered_partition_window_plan) {
        .window => |lowered| {
            var result = try db.windowRelationalRowsPlan(alloc, schema, lowered.plan);
            defer result.deinit(alloc);

            try std.testing.expectEqual(@as(u32, 3), result.total_rows);
            try std.testing.expectEqual(@as(usize, 3), result.rows.len);
            try std.testing.expectEqualStrings("{\"tenant\":\"t1\",\"id\":\"o1\",\"tenant_amount\":15}", result.rows[0]);
            try std.testing.expectEqualStrings("{\"tenant\":\"t1\",\"id\":\"o2\",\"tenant_amount\":15}", result.rows[1]);
            try std.testing.expectEqualStrings("{\"tenant\":\"t2\",\"id\":\"o3\",\"tenant_amount\":7}", result.rows[2]);
        },
        else => return error.TestUnexpectedResult,
    }

    var scalar_extrema_window_plan = try lowerReadPlanAlloc(
        alloc,
        "SELECT tenant, id, min(status) OVER (PARTITION BY tenant) AS first_status, max(lower(status)) OVER (PARTITION BY tenant) AS last_status_key FROM usage_records WHERE kind = 'order' ORDER BY id ASC",
        schema,
        &.{},
    );
    defer scalar_extrema_window_plan.deinit(alloc);

    switch (scalar_extrema_window_plan) {
        .window => |lowered| {
            var result = try db.windowRelationalRowsPlan(alloc, schema, lowered.plan);
            defer result.deinit(alloc);

            try std.testing.expectEqual(@as(u32, 3), result.total_rows);
            try std.testing.expectEqual(@as(usize, 3), result.rows.len);
            try std.testing.expectEqualStrings("{\"tenant\":\"t1\",\"id\":\"o1\",\"first_status\":\"open\",\"last_status_key\":\"open\"}", result.rows[0]);
            try std.testing.expectEqualStrings("{\"tenant\":\"t1\",\"id\":\"o2\",\"first_status\":\"open\",\"last_status_key\":\"open\"}", result.rows[1]);
            try std.testing.expectEqualStrings("{\"tenant\":\"t2\",\"id\":\"o3\",\"first_status\":\"open\",\"last_status_key\":\"open\"}", result.rows[2]);
        },
        else => return error.TestUnexpectedResult,
    }

    var following_rows_window_plan = try lowerReadPlanAlloc(
        alloc,
        "SELECT id, count(*) OVER (ORDER BY amount DESC, id ASC ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS current_and_next FROM usage_records WHERE kind = 'order' ORDER BY current_and_next DESC, id ASC",
        schema,
        &.{},
    );
    defer following_rows_window_plan.deinit(alloc);

    switch (following_rows_window_plan) {
        .window => |lowered| {
            var result = try db.windowRelationalRowsPlan(alloc, schema, lowered.plan);
            defer result.deinit(alloc);

            try std.testing.expectEqual(@as(u32, 3), result.total_rows);
            try std.testing.expectEqual(@as(usize, 3), result.rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"o1\",\"current_and_next\":2}", result.rows[0]);
            try std.testing.expectEqualStrings("{\"id\":\"o3\",\"current_and_next\":2}", result.rows[1]);
            try std.testing.expectEqualStrings("{\"id\":\"o2\",\"current_and_next\":1}", result.rows[2]);
        },
        else => return error.TestUnexpectedResult,
    }

    var advanced_window_plan = try lowerReadPlanAlloc(
        alloc,
        "SELECT tenant, id, RANK() OVER (PARTITION BY tenant ORDER BY amount DESC, id ASC) AS amount_rank, DENSE_RANK() OVER (PARTITION BY tenant ORDER BY amount DESC, id ASC) AS dense_amount_rank, LAG(amount, 1, 0) OVER (PARTITION BY tenant ORDER BY amount DESC, id ASC) AS prev_amount, LEAD(amount) OVER (PARTITION BY tenant ORDER BY amount DESC, id ASC) AS next_amount, FIRST_VALUE(amount) OVER (PARTITION BY tenant ORDER BY amount DESC, id ASC) AS first_amount, LAST_VALUE(amount) OVER (PARTITION BY tenant ORDER BY amount DESC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_amount, NTH_VALUE(amount, 2) OVER (PARTITION BY tenant ORDER BY amount DESC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_amount, PERCENT_RANK() OVER (PARTITION BY tenant ORDER BY amount DESC, id ASC) AS percent_rank, CUME_DIST() OVER (PARTITION BY tenant ORDER BY amount DESC, id ASC) AS cume_dist, NTILE(2) OVER (PARTITION BY tenant ORDER BY amount DESC, id ASC) AS amount_bucket FROM usage_records WHERE kind = 'order' ORDER BY tenant ASC, id ASC",
        schema,
        &.{},
    );
    defer advanced_window_plan.deinit(alloc);

    switch (advanced_window_plan) {
        .window => |lowered| {
            try std.testing.expectEqual(@as(usize, 10), lowered.plan.window.windows.len);
            try std.testing.expectEqual(db_mod.types.RelationalRowsWindowFunction.rank, lowered.plan.window.windows[0].function);
            try std.testing.expectEqual(db_mod.types.RelationalRowsWindowFunction.dense_rank, lowered.plan.window.windows[1].function);
            try std.testing.expectEqual(db_mod.types.RelationalRowsWindowFunction.lag, lowered.plan.window.windows[2].function);
            try std.testing.expectEqualStrings("0", lowered.plan.window.windows[2].default_json);
            try std.testing.expectEqual(db_mod.types.RelationalRowsWindowFunction.lead, lowered.plan.window.windows[3].function);
            try std.testing.expectEqual(db_mod.types.RelationalRowsWindowFunction.first_value, lowered.plan.window.windows[4].function);
            try std.testing.expectEqual(db_mod.types.RelationalRowsWindowFunction.last_value, lowered.plan.window.windows[5].function);
            try std.testing.expectEqual(db_mod.types.RelationalRowsWindowFunction.nth_value, lowered.plan.window.windows[6].function);
            try std.testing.expectEqual(@as(u32, 2), lowered.plan.window.windows[6].offset);
            try std.testing.expectEqual(db_mod.types.RelationalRowsWindowFunction.percent_rank, lowered.plan.window.windows[7].function);
            try std.testing.expectEqual(db_mod.types.RelationalRowsWindowFunction.cume_dist, lowered.plan.window.windows[8].function);
            try std.testing.expectEqual(db_mod.types.RelationalRowsWindowFunction.ntile, lowered.plan.window.windows[9].function);
            try std.testing.expectEqual(@as(u32, 2), lowered.plan.window.windows[9].offset);

            var result = try db.windowRelationalRowsPlan(alloc, schema, lowered.plan);
            defer result.deinit(alloc);

            try std.testing.expectEqual(@as(u32, 3), result.total_rows);
            try std.testing.expectEqual(@as(usize, 3), result.rows.len);
            try std.testing.expectEqualStrings("{\"tenant\":\"t1\",\"id\":\"o1\",\"amount_rank\":1,\"dense_amount_rank\":1,\"prev_amount\":0,\"next_amount\":5,\"first_amount\":10,\"last_amount\":5,\"second_amount\":5,\"percent_rank\":0,\"cume_dist\":0.5,\"amount_bucket\":1}", result.rows[0]);
            try std.testing.expectEqualStrings("{\"tenant\":\"t1\",\"id\":\"o2\",\"amount_rank\":2,\"dense_amount_rank\":2,\"prev_amount\":10,\"next_amount\":null,\"first_amount\":10,\"last_amount\":5,\"second_amount\":5,\"percent_rank\":1,\"cume_dist\":1,\"amount_bucket\":2}", result.rows[1]);
            try std.testing.expectEqualStrings("{\"tenant\":\"t2\",\"id\":\"o3\",\"amount_rank\":1,\"dense_amount_rank\":1,\"prev_amount\":0,\"next_amount\":null,\"first_amount\":7,\"last_amount\":7,\"second_amount\":null,\"percent_rank\":0,\"cume_dist\":1,\"amount_bucket\":1}", result.rows[2]);
        },
        else => return error.TestUnexpectedResult,
    }

    var filtered_window_plan = try lowerReadPlanAlloc(
        alloc,
        "SELECT tenant, id, COUNT(*) FILTER (WHERE string_to_array(scope, ' ') @> ARRAY['write']) OVER (PARTITION BY tenant) AS writable_orders, SUM(amount) FILTER (WHERE lower(status) = 'open') OVER (PARTITION BY tenant) AS open_amount, COUNT(*) FILTER (WHERE metadata @> '{\"source\":\"api\"}'::jsonb) OVER (PARTITION BY tenant) AS api_orders, COUNT(*) FILTER (WHERE tags @> ARRAY['hot']) OVER (PARTITION BY tenant) AS hot_orders FROM usage_records WHERE kind = 'order' ORDER BY id ASC",
        schema,
        &.{},
    );
    defer filtered_window_plan.deinit(alloc);

    switch (filtered_window_plan) {
        .window => |lowered| {
            try std.testing.expectEqual(@as(usize, 4), lowered.plan.window.windows.len);
            try std.testing.expectEqual(@as(usize, 1), lowered.plan.window.windows[0].filter_expression_array_contains.len);
            try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.string_to_array, lowered.plan.window.windows[0].filter_expression_array_contains[0].expression.kind);
            try std.testing.expectEqual(@as(usize, 1), lowered.plan.window.windows[1].filter_expressions.len);
            try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.lower, lowered.plan.window.windows[1].filter_expressions[0].lhs.kind);
            try std.testing.expectEqual(@as(usize, 1), lowered.plan.window.windows[2].filter_json_contains.len);
            try std.testing.expectEqual(@as(usize, 1), lowered.plan.window.windows[3].filter_array_contains.len);

            var result = try db.windowRelationalRowsPlan(alloc, schema, lowered.plan);
            defer result.deinit(alloc);

            try std.testing.expectEqual(@as(u32, 3), result.total_rows);
            try std.testing.expectEqual(@as(usize, 3), result.rows.len);
            try std.testing.expectEqualStrings("{\"tenant\":\"t1\",\"id\":\"o1\",\"writable_orders\":1,\"open_amount\":15,\"api_orders\":1,\"hot_orders\":1}", result.rows[0]);
            try std.testing.expectEqualStrings("{\"tenant\":\"t1\",\"id\":\"o2\",\"writable_orders\":1,\"open_amount\":15,\"api_orders\":1,\"hot_orders\":1}", result.rows[1]);
            try std.testing.expectEqualStrings("{\"tenant\":\"t2\",\"id\":\"o3\",\"writable_orders\":1,\"open_amount\":7,\"api_orders\":1,\"hot_orders\":1}", result.rows[2]);
        },
        else => return error.TestUnexpectedResult,
    }

    var boolean_window_plan = try lowerReadPlanAlloc(
        alloc,
        "SELECT tenant, id, BOOL_OR(enabled) FILTER (WHERE lower(status) = 'open') OVER (PARTITION BY tenant) AS any_enabled, BOOL_AND(enabled) FILTER (WHERE lower(status) = 'open') OVER (PARTITION BY tenant) AS all_enabled FROM usage_records WHERE kind = 'order' ORDER BY id ASC",
        schema,
        &.{},
    );
    defer boolean_window_plan.deinit(alloc);

    switch (boolean_window_plan) {
        .window => |lowered| {
            try std.testing.expectEqual(@as(usize, 2), lowered.plan.window.windows.len);
            try std.testing.expectEqual(db_mod.types.RelationalRowsWindowFunction.bool_or, lowered.plan.window.windows[0].function);
            try std.testing.expectEqualStrings("enabled", lowered.plan.window.windows[0].value_expression.?.field);
            try std.testing.expectEqual(@as(usize, 1), lowered.plan.window.windows[0].filter_expressions.len);
            try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.lower, lowered.plan.window.windows[0].filter_expressions[0].lhs.kind);
            try std.testing.expectEqual(db_mod.types.RelationalRowsWindowFunction.bool_and, lowered.plan.window.windows[1].function);
            try std.testing.expectEqualStrings("enabled", lowered.plan.window.windows[1].value_expression.?.field);
            try std.testing.expectEqual(@as(usize, 1), lowered.plan.window.windows[1].filter_expressions.len);

            var result = try db.windowRelationalRowsPlan(alloc, schema, lowered.plan);
            defer result.deinit(alloc);

            try std.testing.expectEqual(@as(u32, 3), result.total_rows);
            try std.testing.expectEqual(@as(usize, 3), result.rows.len);
            try std.testing.expectEqualStrings("{\"tenant\":\"t1\",\"id\":\"o1\",\"any_enabled\":true,\"all_enabled\":false}", result.rows[0]);
            try std.testing.expectEqualStrings("{\"tenant\":\"t1\",\"id\":\"o2\",\"any_enabled\":true,\"all_enabled\":false}", result.rows[1]);
            try std.testing.expectEqualStrings("{\"tenant\":\"t2\",\"id\":\"o3\",\"any_enabled\":true,\"all_enabled\":true}", result.rows[2]);
        },
        else => return error.TestUnexpectedResult,
    }

    var lateral_plan = try lowerReadPlanAlloc(
        alloc,
        "SELECT org.id AS organization_id, latest.amount AS latest_amount, latest.created_at AS latest_created_at FROM usage_records AS org LEFT JOIN LATERAL (SELECT amount, created_at FROM usage_records AS bal WHERE bal.organization_id = org.id AND bal.kind = 'balance' ORDER BY 2 DESC LIMIT 1) AS latest ON true WHERE org.kind = 'organization' ORDER BY latest_amount DESC NULLS LAST LIMIT 10",
        schema,
        &.{},
    );
    defer lateral_plan.deinit(alloc);

    switch (lateral_plan) {
        .lateral => |lowered| {
            var result = try db.lateralRelationalRowsPlan(alloc, schema, lowered.plan);
            defer result.deinit(alloc);

            try std.testing.expectEqual(@as(u32, 3), result.total_rows);
            try std.testing.expectEqual(@as(usize, 3), result.rows.len);
            try std.testing.expectEqualStrings("{\"organization_id\":\"org1\",\"latest_amount\":9,\"latest_created_at\":20}", result.rows[0]);
            try std.testing.expectEqualStrings("{\"organization_id\":\"org2\",\"latest_amount\":7,\"latest_created_at\":15}", result.rows[1]);
            try std.testing.expectEqualStrings("{\"organization_id\":\"org3\",\"latest_amount\":null,\"latest_created_at\":null}", result.rows[2]);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "postgres sql adapter catalog-backed read plans execute across source schemas" {
    const alloc = std.testing.allocator;
    const target_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"tenant_id":{"type":"keyword"},"customer_id":{"type":"keyword"},"kind":{"type":"keyword"},"amount":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const customer_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"tenant_id":{"type":"keyword"},"kind":{"type":"keyword"},"name":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const balance_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"organization_id":{"type":"keyword"},"kind":{"type":"keyword"},"amount":{"type":"numeric"},"created_at":{"type":"numeric"}},"required":["id","organization_id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;

    var parsed_target = try schema_api.parseValidatedTableSchema(alloc, target_schema_json);
    defer parsed_target.deinit(alloc);
    const target_schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed_target);
    defer runtime_schema.freeSchema(alloc, target_schema);
    var parsed_customer = try schema_api.parseValidatedTableSchema(alloc, customer_schema_json);
    defer parsed_customer.deinit(alloc);
    const customer_schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed_customer);
    defer runtime_schema.freeSchema(alloc, customer_schema);
    var parsed_balance = try schema_api.parseValidatedTableSchema(alloc, balance_schema_json);
    defer parsed_balance.deinit(alloc);
    const balance_schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed_balance);
    defer runtime_schema.freeSchema(alloc, balance_schema);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const target_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/sql-catalog-read-target", .{tmp.sub_path});
    defer alloc.free(target_path);
    const customer_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/sql-catalog-read-customers", .{tmp.sub_path});
    defer alloc.free(customer_path);
    const balance_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/sql-catalog-read-balances", .{tmp.sub_path});
    defer alloc.free(balance_path);

    var target_db = try db_mod.DB.open(alloc, target_path, .{});
    defer target_db.close();
    try target_db.applyTableSchemaJson(alloc, target_schema_json, .{});
    var customer_db = try db_mod.DB.open(alloc, customer_path, .{});
    defer customer_db.close();
    try customer_db.applyTableSchemaJson(alloc, customer_schema_json, .{});
    var balance_db = try db_mod.DB.open(alloc, balance_path, .{});
    defer balance_db.close();
    try balance_db.applyTableSchemaJson(alloc, balance_schema_json, .{});

    const target_jsons = [_][]const u8{
        "{\"id\":\"o1\",\"tenant_id\":\"t1\",\"customer_id\":\"c1\",\"kind\":\"order\",\"amount\":10}",
        "{\"id\":\"o2\",\"tenant_id\":\"t1\",\"customer_id\":\"c2\",\"kind\":\"order\",\"amount\":5}",
        "{\"id\":\"o3\",\"tenant_id\":\"t2\",\"customer_id\":\"missing\",\"kind\":\"order\",\"amount\":7}",
        "{\"id\":\"skip\",\"tenant_id\":\"t1\",\"customer_id\":\"c1\",\"kind\":\"invoice\",\"amount\":100}",
        "{\"id\":\"org1\",\"tenant_id\":\"t1\",\"kind\":\"organization\",\"amount\":0}",
        "{\"id\":\"org2\",\"tenant_id\":\"t1\",\"kind\":\"organization\",\"amount\":0}",
        "{\"id\":\"org3\",\"tenant_id\":\"t1\",\"kind\":\"organization\",\"amount\":0}",
    };
    var target_keys: [target_jsons.len][]u8 = undefined;
    for (target_jsons, 0..) |row_json, i| target_keys[i] = try relational_rows.physicalPrimaryKeyFromRowJsonAlloc(alloc, target_schema, row_json);
    defer {
        for (target_keys) |key| alloc.free(key);
    }
    try target_db.batch(.{
        .writes = &.{
            .{ .key = target_keys[0], .value = target_jsons[0] },
            .{ .key = target_keys[1], .value = target_jsons[1] },
            .{ .key = target_keys[2], .value = target_jsons[2] },
            .{ .key = target_keys[3], .value = target_jsons[3] },
            .{ .key = target_keys[4], .value = target_jsons[4] },
            .{ .key = target_keys[5], .value = target_jsons[5] },
            .{ .key = target_keys[6], .value = target_jsons[6] },
        },
        .sync_level = .write,
    });

    const customer_jsons = [_][]const u8{
        "{\"id\":\"c1\",\"tenant_id\":\"t1\",\"kind\":\"customer\",\"name\":\"Alice\"}",
        "{\"id\":\"c2\",\"tenant_id\":\"t1\",\"kind\":\"lead\",\"name\":\"Bot\"}",
        "{\"id\":\"c3\",\"tenant_id\":\"t2\",\"kind\":\"customer\",\"name\":\"Cara\"}",
    };
    var customer_keys: [customer_jsons.len][]u8 = undefined;
    for (customer_jsons, 0..) |row_json, i| customer_keys[i] = try relational_rows.physicalPrimaryKeyFromRowJsonAlloc(alloc, customer_schema, row_json);
    defer {
        for (customer_keys) |key| alloc.free(key);
    }
    try customer_db.batch(.{
        .writes = &.{
            .{ .key = customer_keys[0], .value = customer_jsons[0] },
            .{ .key = customer_keys[1], .value = customer_jsons[1] },
            .{ .key = customer_keys[2], .value = customer_jsons[2] },
        },
        .sync_level = .write,
    });

    const balance_jsons = [_][]const u8{
        "{\"id\":\"b1\",\"organization_id\":\"org1\",\"kind\":\"balance\",\"amount\":4,\"created_at\":10}",
        "{\"id\":\"b2\",\"organization_id\":\"org1\",\"kind\":\"balance\",\"amount\":9,\"created_at\":20}",
        "{\"id\":\"b3\",\"organization_id\":\"org2\",\"kind\":\"balance\",\"amount\":7,\"created_at\":15}",
        "{\"id\":\"b4\",\"organization_id\":\"org2\",\"kind\":\"forecast\",\"amount\":100,\"created_at\":30}",
    };
    var balance_keys: [balance_jsons.len][]u8 = undefined;
    for (balance_jsons, 0..) |row_json, i| balance_keys[i] = try relational_rows.physicalPrimaryKeyFromRowJsonAlloc(alloc, balance_schema, row_json);
    defer {
        for (balance_keys) |key| alloc.free(key);
    }
    try balance_db.batch(.{
        .writes = &.{
            .{ .key = balance_keys[0], .value = balance_jsons[0] },
            .{ .key = balance_keys[1], .value = balance_jsons[1] },
            .{ .key = balance_keys[2], .value = balance_jsons[2] },
            .{ .key = balance_keys[3], .value = balance_jsons[3] },
        },
        .sync_level = .write,
    });

    var customer_catalog = AppParitySourceSchemaCatalog.init("customer_records", customer_schema_json);
    var catalog_join_plan = try lowerReadPlanWithCatalogAlloc(
        alloc,
        "SELECT o.id AS order_id, c.name AS customer_name, o.amount AS amount FROM usage_records AS o LEFT JOIN customer_records AS c ON o.tenant_id = c.tenant_id AND o.customer_id = c.id WHERE o.kind = 'order' AND c.kind = 'customer' ORDER BY amount DESC NULLS LAST, order_id ASC LIMIT 5",
        target_schema,
        &.{},
        customer_catalog.iface(),
    );
    defer catalog_join_plan.deinit(alloc);
    switch (catalog_join_plan) {
        .join => |lowered| {
            try std.testing.expectEqualStrings("usage_records", lowered.left_table_name);
            try std.testing.expectEqualStrings("customer_records", lowered.right_table_name);

            var left_source = lowered.join.left;
            left_source.select = &.{};
            left_source.select_all = true;
            var right_source = lowered.join.right;
            right_source.select = &.{};
            right_source.select_all = true;

            var left_rows = try target_db.queryRelationalRows(alloc, target_schema, left_source);
            defer left_rows.deinit(alloc);
            var right_rows = try customer_db.queryRelationalRows(alloc, customer_schema, right_source);
            defer right_rows.deinit(alloc);
            try std.testing.expectEqual(@as(u32, 3), left_rows.total);
            try std.testing.expectEqual(@as(u32, 2), right_rows.total);

            var result = try db_mod.DB.joinRelationalRowsFromSourceRowsAlloc(alloc, lowered.join, left_rows.rows, right_rows.rows);
            defer result.deinit(alloc);
            try std.testing.expectEqual(@as(u32, 3), result.total_rows);
            try std.testing.expectEqual(@as(usize, 3), result.rows.len);
            try std.testing.expectEqualStrings("{\"order_id\":\"o1\",\"customer_name\":\"Alice\",\"amount\":10}", result.rows[0]);
            try std.testing.expectEqualStrings("{\"order_id\":\"o3\",\"customer_name\":null,\"amount\":7}", result.rows[1]);
            try std.testing.expectEqualStrings("{\"order_id\":\"o2\",\"customer_name\":null,\"amount\":5}", result.rows[2]);
        },
        else => return error.TestUnexpectedResult,
    }

    var balance_catalog = AppParitySourceSchemaCatalog.init("balance_records", balance_schema_json);
    var catalog_lateral_plan = try lowerReadPlanWithCatalogAlloc(
        alloc,
        "SELECT org.id AS organization_id, latest.amount AS latest_amount FROM usage_records AS org LEFT JOIN LATERAL (SELECT amount, created_at FROM balance_records AS bal WHERE bal.organization_id = org.id AND bal.kind = 'balance' ORDER BY 2 DESC LIMIT 1) AS latest ON true WHERE org.kind = 'organization' ORDER BY latest_amount DESC NULLS LAST, organization_id ASC LIMIT 10",
        target_schema,
        &.{},
        balance_catalog.iface(),
    );
    defer catalog_lateral_plan.deinit(alloc);
    switch (catalog_lateral_plan) {
        .lateral => |lowered| {
            try std.testing.expectEqualStrings("usage_records", lowered.left_table_name);
            try std.testing.expectEqualStrings("balance_records", lowered.right_table_name);

            var left_source = lowered.plan.lateral.left;
            left_source.select = &.{};
            left_source.select_all = true;

            var left_rows = try target_db.queryRelationalRows(alloc, target_schema, left_source);
            defer left_rows.deinit(alloc);
            var right_rows = try balance_db.queryRelationalRows(alloc, balance_schema, .{ .select_all = true });
            defer right_rows.deinit(alloc);
            try std.testing.expectEqual(@as(u32, 3), left_rows.total);
            try std.testing.expectEqual(@as(u32, 4), right_rows.total);

            var result = try db_mod.DB.lateralRelationalRowsFromSourceRowsStaticAlloc(alloc, lowered.plan.lateral, left_rows.rows, right_rows.rows);
            defer result.deinit(alloc);
            try std.testing.expectEqual(@as(u32, 3), result.total_rows);
            try std.testing.expectEqual(@as(usize, 3), result.rows.len);
            try std.testing.expectEqualStrings("{\"organization_id\":\"org1\",\"latest_amount\":9}", result.rows[0]);
            try std.testing.expectEqualStrings("{\"organization_id\":\"org2\",\"latest_amount\":7}", result.rows[1]);
            try std.testing.expectEqualStrings("{\"organization_id\":\"org3\",\"latest_amount\":null}", result.rows[2]);
        },
        else => return error.TestUnexpectedResult,
    }
}

fn lowerWritePlanForSqlAdapterEdgeCaseAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    resolver: ?relational_rows.UniqueSelectorResolver,
) !LoweredWritePlan {
    return try lowerWritePlanParsedSqlAlloc(alloc, parsed_sql, schema, params, .{ .unique_resolver = resolver });
}

fn planDdlLogicalPlanParsedSqlAllocForEdgeCase(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
) !sql_adapter.LogicalSqlPlan {
    return try sql_adapter.planDdlLogicalPlanParsedSqlWithFunctionBindingsAlloc(alloc, parsed_sql, .{});
}

test "postgres sql adapter rejects data-driven application edge cases explicitly" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"organization_id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);
    var resolver_ctx = TestPrimaryResolver{ .row_json = "{\"id\":\"u1\",\"organization_id\":\"o1\"}", .version = 3 };

    const fixture_json = @embedFile("fixtures/sql_api_adapter_edge_cases.json");
    var parsed_fixture = try std.json.parseFromSlice(std.json.Value, alloc, fixture_json, .{});
    defer parsed_fixture.deinit();
    const root = try sql_adapter.parseSqlAdapterEdgeCaseRootAlloc(alloc, parsed_fixture.value);
    defer sql_adapter.freeSqlAdapterEdgeCaseRoot(alloc, root);
    var required_coverage = try sql_adapter.parseSqlAdapterEdgeCaseCoverageRequirementsAlloc(alloc);
    defer required_coverage.deinit(alloc);

    var coverage = sql_adapter.SqlAdapterEdgeCaseCoverage{};
    const callbacks = sql_adapter.SqlAdapterEdgeCaseLoweringCallbacks{
        .lower_select = lowerSelectParsedSqlAlloc,
        .lower_update = lowerUpdateParsedSqlAlloc,
        .lower_delete = lowerDeleteParsedSqlAlloc,
        .lower_insert = lowerInsertWithResolverParsedSqlAlloc,
        .plan_ddl = planDdlLogicalPlanParsedSqlAllocForEdgeCase,
        .lower_write_plan = lowerWritePlanForSqlAdapterEdgeCaseAlloc,
    };
    for (root.cases) |edge_case| {
        try coverage.observe(edge_case);
        try sql_adapter.expectSqlAdapterEdgeCase(alloc, edge_case, schema, resolver_ctx.resolver(), callbacks);
    }
    try sql_adapter.expectSqlAdapterEdgeCaseCoverageRequirements(coverage, required_coverage.root.required);
}
