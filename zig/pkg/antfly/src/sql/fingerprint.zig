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

const binder = @import("binder.zig");
const corpus = @import("corpus.zig");
const ddl_plan = @import("ddl_plan.zig");
const diagnostics = @import("diagnostics.zig");
const expr_type = @import("expr/type.zig");
const lower_expr = @import("lower_expr.zig");
const runtime_schema = @import("../storage/schema.zig");
const schema_api = @import("../schema/mod.zig");
const schema_json = @import("schema_json.zig");
const tokenized = @import("tokenized.zig");

const AlterTablePlan = ddl_plan.AlterTablePlan;
const AppliedDdlSchemaJson = ddl_plan.AppliedDdlSchemaJson;
const AppliedDdlWorkAction = ddl_plan.AppliedDdlWorkAction;
const AppliedDdlWorkItem = ddl_plan.AppliedDdlWorkItem;
const AppliedDdlWorkReason = ddl_plan.AppliedDdlWorkReason;
const AppliedDdlWorkSubject = ddl_plan.AppliedDdlWorkSubject;
const BulkIoDirection = ddl_plan.BulkIoDirection;
const BulkIoEndpointKind = ddl_plan.BulkIoEndpointKind;
const BulkIoLogVerbosity = ddl_plan.BulkIoLogVerbosity;
const BulkIoOnErrorPolicy = ddl_plan.BulkIoOnErrorPolicy;
const BulkIoPlan = ddl_plan.BulkIoPlan;
const CreateIndexPlan = ddl_plan.CreateIndexPlan;
const CreateRoutinePlan = ddl_plan.CreateRoutinePlan;
const CreateTablePlan = ddl_plan.CreateTablePlan;
const EnumValuePosition = ddl_plan.EnumValuePosition;
const IdentityAllocatorKind = ddl_plan.IdentityAllocatorKind;
const RelationLifetimeKind = ddl_plan.RelationLifetimeKind;
const RoutineBodyKind = ddl_plan.RoutineBodyKind;
const RoutineExecutionHook = ddl_plan.RoutineExecutionHook;
const RoutineKind = ddl_plan.RoutineKind;
const RoutineNullInput = ddl_plan.RoutineNullInput;
const RoutineParallelSafety = ddl_plan.RoutineParallelSafety;
const RoutineSecurity = ddl_plan.RoutineSecurity;
const RoutineVolatility = ddl_plan.RoutineVolatility;
const SequenceOptions = ddl_plan.SequenceOptions;
const TablePartitionMethod = ddl_plan.TablePartitionMethod;
const TransactionAccessMode = ddl_plan.TransactionAccessMode;
const TransactionIsolationLevel = ddl_plan.TransactionIsolationLevel;

const antflyTypeSchemaName = ddl_plan.antflyTypeSchemaName;
const appendNonZeroUsizeFingerprintAlloc = corpus.appendNonZeroUsizeFingerprintAlloc;
const appendStringFingerprintAlloc = corpus.appendStringFingerprintAlloc;
const appendTrueBoolFingerprintAlloc = corpus.appendTrueBoolFingerprintAlloc;
const schemaJsonCommentCountInRoot = schema_json.schemaJsonCommentCountInRoot;

fn adapterNoopFingerprintAlloc(
    alloc: std.mem.Allocator,
    family: []const u8,
    reason: []const u8,
) ![]u8 {
    const diagnostic_reason = diagnostics.classificationReasonFromToken(reason) orelse return error.TestUnexpectedResult;
    return corpus.adapterNoopFingerprintAlloc(alloc, family, diagnostic_reason) catch |err| switch (err) {
        error.UnsupportedSqlShape => return error.TestUnexpectedResult,
        else => return err,
    };
}

pub fn createIndexPlanGeneratedExpressionCount(plan: CreateIndexPlan) usize {
    return if (plan.generated_expression != null) 1 else 0;
}

fn createIndexPlanGeneratedExpressionOp(plan: CreateIndexPlan) ?[]const u8 {
    const generated = plan.generated_expression orelse return null;
    return @tagName(generated.op);
}

fn createTablePlanPrimaryKeyColumnCount(plan: CreateTablePlan) usize {
    return if (plan.primary_key) |primary_key| primary_key.columns.len else 0;
}

fn createTablePlanDefaultColumnCount(plan: CreateTablePlan) usize {
    var count: usize = 0;
    for (plan.columns) |column| {
        if (column.default_value != null) count += 1;
    }
    return count;
}

fn createTablePlanGeneratedColumnCount(plan: CreateTablePlan) usize {
    var count: usize = 0;
    for (plan.columns) |column| {
        if (column.generated != null) count += 1;
    }
    return count;
}

fn createTablePlanUpdatePolicyColumnCount(plan: CreateTablePlan) usize {
    var count: usize = 0;
    for (plan.columns) |column| {
        if (column.on_update_value != null) count += 1;
    }
    return count;
}

const NamedConstraintFingerprintCounts = struct {
    primary_key: usize = 0,
    unique: usize = 0,
    foreign_key: usize = 0,
    check: usize = 0,
};

fn countNamedCreateTableConstraints(plan: CreateTablePlan) NamedConstraintFingerprintCounts {
    var counts: NamedConstraintFingerprintCounts = .{};
    if (plan.primary_key) |primary_key| {
        if (primary_key.name != null) counts.primary_key += 1;
    }
    for (plan.unique_constraints) |constraint| {
        if (constraint.name.len > 0) counts.unique += 1;
    }
    for (plan.foreign_keys) |foreign_key| {
        if (foreign_key.name.len > 0) counts.foreign_key += 1;
    }
    for (plan.checks) |check| {
        if (check.name.len > 0) counts.check += 1;
    }
    return counts;
}

fn appendNamedConstraintFingerprintsAlloc(
    alloc: std.mem.Allocator,
    owned_base: []u8,
    counts: NamedConstraintFingerprintCounts,
) ![]u8 {
    var fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, owned_base, "pk_named", counts.primary_key);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "unique_named", counts.unique);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "fk_named", counts.foreign_key);
    return try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "check_named", counts.check);
}

const ForeignKeyOptionFingerprintCounts = struct {
    deferrable: usize = 0,
    deferred: usize = 0,
    match_full: usize = 0,
    match_partial: usize = 0,
};

fn countForeignKeyOptionFingerprints(foreign_keys: []const runtime_schema.ForeignKey) ForeignKeyOptionFingerprintCounts {
    var counts: ForeignKeyOptionFingerprintCounts = .{};
    for (foreign_keys) |foreign_key| {
        if (foreign_key.deferrable) counts.deferrable += 1;
        if (foreign_key.timing == .deferred) counts.deferred += 1;
        switch (foreign_key.match) {
            .simple => {},
            .full => counts.match_full += 1,
            .partial => counts.match_partial += 1,
        }
    }
    return counts;
}

fn addForeignKeyOptionFingerprintCounts(
    counts: *ForeignKeyOptionFingerprintCounts,
    foreign_keys: []const runtime_schema.ForeignKey,
) void {
    const next = countForeignKeyOptionFingerprints(foreign_keys);
    counts.deferrable += next.deferrable;
    counts.deferred += next.deferred;
    counts.match_full += next.match_full;
    counts.match_partial += next.match_partial;
}

fn appendForeignKeyOptionFingerprintsAlloc(
    alloc: std.mem.Allocator,
    owned_base: []u8,
    counts: ForeignKeyOptionFingerprintCounts,
) ![]u8 {
    var fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, owned_base, "fk_deferrable", counts.deferrable);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "fk_deferred", counts.deferred);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "fk_match_full", counts.match_full);
    return try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "fk_match_partial", counts.match_partial);
}

const ConstraintTimingFingerprintCounts = struct {
    primary_key_deferrable: usize = 0,
    primary_key_deferred: usize = 0,
    unique_deferrable: usize = 0,
    unique_deferred: usize = 0,
};

fn addPrimaryKeyTimingFingerprintCount(counts: *ConstraintTimingFingerprintCounts, primary_key: runtime_schema.PrimaryKey) void {
    if (primary_key.deferrable) counts.primary_key_deferrable += 1;
    if (primary_key.timing == .deferred) counts.primary_key_deferred += 1;
}

fn addUniqueTimingFingerprintCounts(counts: *ConstraintTimingFingerprintCounts, unique_constraints: []const runtime_schema.UniqueConstraint) void {
    for (unique_constraints) |constraint| {
        if (constraint.deferrable) counts.unique_deferrable += 1;
        if (constraint.timing == .deferred) counts.unique_deferred += 1;
    }
}

fn countCreateTableConstraintTimingFingerprints(plan: CreateTablePlan) ConstraintTimingFingerprintCounts {
    var counts: ConstraintTimingFingerprintCounts = .{};
    if (plan.primary_key) |primary_key| addPrimaryKeyTimingFingerprintCount(&counts, primary_key);
    addUniqueTimingFingerprintCounts(&counts, plan.unique_constraints);
    return counts;
}

fn appendConstraintTimingFingerprintsAlloc(
    alloc: std.mem.Allocator,
    owned_base: []u8,
    counts: ConstraintTimingFingerprintCounts,
) ![]u8 {
    var fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, owned_base, "pk_deferrable", counts.primary_key_deferrable);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "pk_deferred", counts.primary_key_deferred);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "unique_deferrable", counts.unique_deferrable);
    return try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "unique_deferred", counts.unique_deferred);
}

const AlterTablePlanFingerprintCounts = struct {
    add_column: usize = 0,
    add_column_if_not_exists: usize = 0,
    add_column_default: usize = 0,
    add_column_generated: usize = 0,
    add_column_update_policy: usize = 0,
    add_column_unique: usize = 0,
    add_column_fk: usize = 0,
    add_column_check: usize = 0,
    add_period: usize = 0,
    add_primary_key: usize = 0,
    rename_column: usize = 0,
    rename_constraint: usize = 0,
    drop_column: usize = 0,
    drop_column_if_exists: usize = 0,
    drop_constraint: usize = 0,
    drop_constraint_if_exists: usize = 0,
    drop_update_policy: usize = 0,
    drop_update_policy_if_exists: usize = 0,
    set_default: usize = 0,
    drop_default: usize = 0,
    set_not_null: usize = 0,
    drop_not_null: usize = 0,
    alter_type: usize = 0,
    alter_type_rewrite_expr: usize = 0,
    add_unique: usize = 0,
    add_foreign_key: usize = 0,
    add_check: usize = 0,
    validate_constraint: usize = 0,
    constraint_timing: ConstraintTimingFingerprintCounts = .{},
    foreign_key_options: ForeignKeyOptionFingerprintCounts = .{},
};

fn alterTablePlanFingerprintCounts(plan: AlterTablePlan) AlterTablePlanFingerprintCounts {
    var counts: AlterTablePlanFingerprintCounts = .{};
    for (plan.operations) |operation| switch (operation) {
        .add_column => |add| {
            counts.add_column += 1;
            if (add.if_not_exists) counts.add_column_if_not_exists += 1;
            if (add.column.default_value != null) counts.add_column_default += 1;
            if (add.column.generated != null) counts.add_column_generated += 1;
            if (add.column.on_update_value != null) counts.add_column_update_policy += 1;
            counts.add_column_unique += add.unique_constraints.len;
            counts.add_column_fk += add.foreign_keys.len;
            counts.add_column_check += add.checks.len;
            addUniqueTimingFingerprintCounts(&counts.constraint_timing, add.unique_constraints);
            addForeignKeyOptionFingerprintCounts(&counts.foreign_key_options, add.foreign_keys);
        },
        .add_period => counts.add_period += 1,
        .add_primary_key => |primary_key| {
            counts.add_primary_key += 1;
            addPrimaryKeyTimingFingerprintCount(&counts.constraint_timing, primary_key);
        },
        .rename_column => counts.rename_column += 1,
        .rename_constraint => counts.rename_constraint += 1,
        .drop_column => |drop| {
            counts.drop_column += 1;
            if (drop.if_exists) counts.drop_column_if_exists += 1;
        },
        .drop_constraint => |drop| {
            counts.drop_constraint += 1;
            if (drop.if_exists) counts.drop_constraint_if_exists += 1;
        },
        .drop_update_policy => |drop| {
            counts.drop_update_policy += 1;
            if (drop.if_exists) counts.drop_update_policy_if_exists += 1;
        },
        .alter_column_default => |alter| {
            if (alter.default_value != null) {
                counts.set_default += 1;
            } else {
                counts.drop_default += 1;
            }
        },
        .alter_column_nullability => |alter| {
            if (alter.nullable) {
                counts.drop_not_null += 1;
            } else {
                counts.set_not_null += 1;
            }
        },
        .alter_column_type => |alter| {
            counts.alter_type += 1;
            if (alter.rewrite_expression != null) counts.alter_type_rewrite_expr += 1;
        },
        .add_unique_constraint => |constraint| {
            counts.add_unique += 1;
            addUniqueTimingFingerprintCounts(&counts.constraint_timing, &.{constraint});
        },
        .add_foreign_key => |foreign_key| {
            counts.add_foreign_key += 1;
            addForeignKeyOptionFingerprintCounts(&counts.foreign_key_options, &.{foreign_key});
        },
        .add_check => counts.add_check += 1,
        .validate_constraint => counts.validate_constraint += 1,
    };
    return counts;
}

fn transactionIsolationLevelName(level: ?TransactionIsolationLevel) []const u8 {
    return if (level) |value| @tagName(value) else "none";
}

fn transactionAccessModeName(mode: ?TransactionAccessMode) []const u8 {
    return if (mode) |value| @tagName(value) else "none";
}

fn transactionDeferrableName(deferrable: ?bool) []const u8 {
    return if (deferrable) |value| if (value) "true" else "false" else "none";
}

const FingerprintDdlPayload = union(enum) {
    adapter_noop: ddl_plan.AdapterNoopDdlPlan,
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

pub fn ddlFingerprintAlloc(alloc: std.mem.Allocator, logical: binder.LogicalSqlPlan) ![]u8 {
    return switch (logical) {
        .table_ddl => |plan| try tableDdlFingerprintAlloc(alloc, plan),
        .catalog_ddl => |plan| try catalogDdlFingerprintAlloc(alloc, plan),
        .other_ddl => |plan| try otherDdlFingerprintAlloc(alloc, plan),
        .session => |plan| try ddlPayloadFingerprintAlloc(alloc, .{ .session_catalog = plan }),
        .transaction => |plan| try transactionDdlFingerprintAlloc(alloc, plan),
        .prepared_statement => |plan| try ddlPayloadFingerprintAlloc(alloc, .{ .prepared_statement = plan }),
        .cursor => |plan| try ddlPayloadFingerprintAlloc(alloc, .{ .cursor_portal = plan }),
        .notification => |plan| try ddlPayloadFingerprintAlloc(alloc, .{ .notification_channel = plan }),
        .routine => |plan| try routineDdlFingerprintAlloc(alloc, plan),
        .auth => |plan| try authDdlFingerprintAlloc(alloc, plan),
        .extension => |plan| try ddlPayloadFingerprintAlloc(alloc, .{ .extension_catalog = plan }),
        .maintenance => |plan| try ddlPayloadFingerprintAlloc(alloc, .{ .maintenance_job = plan }),
        .bulk_io => |plan| try ddlPayloadFingerprintAlloc(alloc, .{ .bulk_io = plan }),
        .read, .write, .catalog_read, .catalog_write => error.UnsupportedSqlShape,
    };
}

fn tableDdlFingerprintAlloc(alloc: std.mem.Allocator, plan: binder.TableDdlLogicalPlan) ![]u8 {
    return switch (plan) {
        .moved => error.UnsupportedSqlShape,
        .create_table => |payload| try ddlPayloadFingerprintAlloc(alloc, .{ .create_table = payload }),
        .table_clone => |payload| try ddlPayloadFingerprintAlloc(alloc, .{ .table_clone = payload }),
        .view_catalog => |payload| try ddlPayloadFingerprintAlloc(alloc, .{ .view_catalog = payload }),
        .materialized_view_catalog => |payload| try ddlPayloadFingerprintAlloc(alloc, .{ .materialized_view_catalog = payload }),
        .relation_lifetime => |payload| try ddlPayloadFingerprintAlloc(alloc, .{ .relation_lifetime = payload }),
        .table_partition_catalog => |payload| try ddlPayloadFingerprintAlloc(alloc, .{ .table_partition_catalog = payload }),
        .create_index => |payload| try ddlPayloadFingerprintAlloc(alloc, .{ .create_index = payload }),
        .drop_index => |payload| try ddlPayloadFingerprintAlloc(alloc, .{ .drop_index = payload }),
        .drop_table => |payload| try ddlPayloadFingerprintAlloc(alloc, .{ .drop_table = payload }),
        .alter_table => |payload| try ddlPayloadFingerprintAlloc(alloc, .{ .alter_table = payload }),
        .create_update_policy => |payload| try ddlPayloadFingerprintAlloc(alloc, .{ .create_update_policy = payload }),
    };
}

fn catalogDdlFingerprintAlloc(alloc: std.mem.Allocator, plan: binder.CatalogDdlLogicalPlan) ![]u8 {
    return switch (plan) {
        .moved => error.UnsupportedSqlShape,
        .enum_type_catalog => |payload| try ddlPayloadFingerprintAlloc(alloc, .{ .enum_type_catalog = payload }),
        .domain_catalog => |payload| try ddlPayloadFingerprintAlloc(alloc, .{ .domain_catalog = payload }),
        .sequence_catalog => |payload| try ddlPayloadFingerprintAlloc(alloc, .{ .sequence_catalog = payload }),
        .identity_allocator_catalog => |payload| try ddlPayloadFingerprintAlloc(alloc, .{ .identity_allocator_catalog = payload }),
        .schema_namespace_catalog => |payload| try ddlPayloadFingerprintAlloc(alloc, .{ .schema_namespace_catalog = payload }),
        .database_catalog => |payload| try ddlPayloadFingerprintAlloc(alloc, .{ .database_catalog = payload }),
        .tablespace_catalog => |payload| try ddlPayloadFingerprintAlloc(alloc, .{ .tablespace_catalog = payload }),
        .logical_replication => |payload| try ddlPayloadFingerprintAlloc(alloc, .{ .logical_replication = payload }),
        .type_system_catalog => |payload| try ddlPayloadFingerprintAlloc(alloc, .{ .type_system_catalog = payload }),
        .comment_metadata => |payload| try ddlPayloadFingerprintAlloc(alloc, .{ .comment_metadata = payload }),
    };
}

fn otherDdlFingerprintAlloc(alloc: std.mem.Allocator, plan: binder.OtherDdlLogicalPlan) ![]u8 {
    return switch (plan) {
        .moved => error.UnsupportedSqlShape,
        .adapter_noop => |payload| try ddlPayloadFingerprintAlloc(alloc, .{ .adapter_noop = payload }),
    };
}

fn transactionDdlFingerprintAlloc(alloc: std.mem.Allocator, plan: binder.TransactionLogicalPlan) ![]u8 {
    return switch (plan) {
        .control => |payload| try ddlPayloadFingerprintAlloc(alloc, .{ .transaction_control = payload }),
        .prepared => |payload| try ddlPayloadFingerprintAlloc(alloc, .{ .prepared_transaction = payload }),
        .savepoint => |payload| try ddlPayloadFingerprintAlloc(alloc, .{ .savepoint_transaction = payload }),
    };
}

fn routineDdlFingerprintAlloc(alloc: std.mem.Allocator, plan: binder.RoutineLogicalPlan) ![]u8 {
    return switch (plan) {
        .function_catalog => |payload| try ddlPayloadFingerprintAlloc(alloc, .{ .function_catalog = payload }),
        .trigger_catalog => |payload| try ddlPayloadFingerprintAlloc(alloc, .{ .trigger_catalog = payload }),
        .procedure_call => |payload| try ddlPayloadFingerprintAlloc(alloc, .{ .procedure_call = payload }),
    };
}

fn authDdlFingerprintAlloc(alloc: std.mem.Allocator, plan: binder.AuthorizationLogicalPlan) ![]u8 {
    return switch (plan) {
        .authorization_catalog => |payload| try ddlPayloadFingerprintAlloc(alloc, .{ .authorization_catalog = payload }),
        .row_security_catalog => |payload| try ddlPayloadFingerprintAlloc(alloc, .{ .row_security_catalog = payload }),
    };
}

fn ddlPayloadFingerprintAlloc(alloc: std.mem.Allocator, payload: FingerprintDdlPayload) ![]u8 {
    return switch (payload) {
        .adapter_noop => |plan| try adapterNoopFingerprintAlloc(alloc, "ddl", @tagName(plan.reason)),
        .session_catalog => |plan| switch (plan) {
            .set_search_path => |set| try std.fmt.allocPrint(alloc, "ddl:session:set_search_path:namespaces={d}:local={}", .{ set.namespaces.len, set.local }),
            .set_setting => |set| try std.fmt.allocPrint(alloc, "ddl:session:set_setting:setting={s}:setting_kind={s}:local={}", .{ set.name, @tagName(set.kind), set.local }),
            .reset_setting => |reset| try std.fmt.allocPrint(alloc, "ddl:session:reset_setting:setting={s}:setting_kind={s}", .{ reset.name, @tagName(reset.kind) }),
            .reset_search_path => try alloc.dupe(u8, "ddl:session:reset_search_path"),
            .show_search_path => try alloc.dupe(u8, "ddl:session:show_search_path"),
            .discard_all => try alloc.dupe(u8, "ddl:session:discard_all"),
        },
        .create_table => |plan| blk: {
            var fingerprint = if (plan.periods.len > 0) temporal: {
                break :temporal try std.fmt.allocPrint(
                    alloc,
                    "ddl:create_table:table={s}:columns={d}:unique={d}:fk={d}:checks={d}:if_not_exists={}:periods={d}:temporal_pk={}:temporal_unique={d}:temporal_fk={d}",
                    .{
                        plan.table_name,
                        plan.columns.len,
                        plan.unique_constraints.len,
                        plan.foreign_keys.len,
                        plan.checks.len,
                        plan.if_not_exists,
                        plan.periods.len,
                        createTablePlanHasTemporalPrimaryKey(plan),
                        createTablePlanTemporalUniqueCount(plan),
                        createTablePlanTemporalForeignKeyCount(plan),
                    },
                );
            } else if (plan.replace_existing) replace: {
                break :replace try std.fmt.allocPrint(
                    alloc,
                    "ddl:create_table:table={s}:columns={d}:unique={d}:fk={d}:checks={d}:if_not_exists={}:replace=true",
                    .{ plan.table_name, plan.columns.len, plan.unique_constraints.len, plan.foreign_keys.len, plan.checks.len, plan.if_not_exists },
                );
            } else try std.fmt.allocPrint(
                alloc,
                "ddl:create_table:table={s}:columns={d}:unique={d}:fk={d}:checks={d}:if_not_exists={}",
                .{ plan.table_name, plan.columns.len, plan.unique_constraints.len, plan.foreign_keys.len, plan.checks.len, plan.if_not_exists },
            );
            fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "pk", createTablePlanPrimaryKeyColumnCount(plan));
            fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "defaults", createTablePlanDefaultColumnCount(plan));
            fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "generated", createTablePlanGeneratedColumnCount(plan));
            fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "on_update", createTablePlanUpdatePolicyColumnCount(plan));
            if (plan.storage_mode == .document) {
                fingerprint = try appendStringFingerprintAlloc(alloc, fingerprint, "storage_mode", "document");
                fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "document_schemas", plan.document_schemas.len);
            }
            fingerprint = try appendTrueBoolFingerprintAlloc(alloc, fingerprint, "system_versioned", plan.system_versioned);
            fingerprint = try appendConstraintTimingFingerprintsAlloc(alloc, fingerprint, countCreateTableConstraintTimingFingerprints(plan));
            fingerprint = try appendNamedConstraintFingerprintsAlloc(alloc, fingerprint, countNamedCreateTableConstraints(plan));
            fingerprint = try appendForeignKeyOptionFingerprintsAlloc(alloc, fingerprint, countForeignKeyOptionFingerprints(plan.foreign_keys));
            break :blk fingerprint;
        },
        .table_clone => |plan| try std.fmt.allocPrint(
            alloc,
            "ddl:table_clone:table={s}:source={s}:if_not_exists={}:columns={}:defaults={}:generated={}:checks={}:constraints={}:indexes={}:periods={}:update_policies={}",
            .{
                plan.table_name,
                plan.source_table_name,
                plan.if_not_exists,
                plan.options.columns,
                plan.options.defaults,
                plan.options.generated,
                plan.options.checks,
                plan.options.constraints,
                plan.options.indexes,
                plan.options.periods,
                plan.options.update_policies,
            },
        ),
        .view_catalog => |plan| switch (plan) {
            .create => |create| try std.fmt.allocPrint(
                alloc,
                "ddl:create_view:view={s}:source={s}:source_fields={d}:fields={d}:replace={}:if_not_exists={}",
                .{ create.view_name, create.source_table_name, create.source_fields.len, create.output_fields.len, create.replace_existing, create.if_not_exists },
            ),
            .rename => |rename| try std.fmt.allocPrint(
                alloc,
                "ddl:rename_view:view={s}:new={s}",
                .{ rename.view_name, rename.new_view_name },
            ),
            .drop => |drop| if (drop.cascade)
                try std.fmt.allocPrint(
                    alloc,
                    "ddl:drop_view:view={s}:if_exists={}:cascade=true",
                    .{ drop.view_name, drop.if_exists },
                )
            else
                try std.fmt.allocPrint(
                    alloc,
                    "ddl:drop_view:view={s}:if_exists={}",
                    .{ drop.view_name, drop.if_exists },
                ),
        },
        .materialized_view_catalog => |plan| switch (plan) {
            .create => |create| try std.fmt.allocPrint(
                alloc,
                "ddl:create_materialized_view:view={s}:source={s}:source_fields={d}:fields={d}:replace={}:if_not_exists={}:populate={}",
                .{ create.view_name, create.source_table_name, create.source_fields.len, create.output_fields.len, create.replace_existing, create.if_not_exists, create.populate_on_create },
            ),
            .refresh => |refresh| try std.fmt.allocPrint(
                alloc,
                "ddl:refresh_materialized_view:view={s}:concurrently={}:populate={}",
                .{ refresh.view_name, refresh.concurrently, refresh.populate },
            ),
            .drop => |drop| if (drop.cascade)
                try std.fmt.allocPrint(
                    alloc,
                    "ddl:drop_materialized_view:view={s}:if_exists={}:cascade=true",
                    .{ drop.view_name, drop.if_exists },
                )
            else
                try std.fmt.allocPrint(
                    alloc,
                    "ddl:drop_materialized_view:view={s}:if_exists={}",
                    .{ drop.view_name, drop.if_exists },
                ),
        },
        .relation_lifetime => |plan| blk: {
            const base = try std.fmt.allocPrint(
                alloc,
                "ddl:relation_lifetime:kind={s}:table={s}:columns={d}:unique={d}:fk={d}:checks={d}:if_not_exists={}",
                .{
                    relationLifetimeKindName(plan.kind),
                    plan.create_table.table_name,
                    plan.create_table.columns.len,
                    plan.create_table.unique_constraints.len,
                    plan.create_table.foreign_keys.len,
                    plan.create_table.checks.len,
                    plan.create_table.if_not_exists,
                },
            );
            break :blk try appendForeignKeyOptionFingerprintsAlloc(alloc, base, countForeignKeyOptionFingerprints(plan.create_table.foreign_keys));
        },
        .enum_type_catalog => |plan| switch (plan) {
            .create => |create| try std.fmt.allocPrint(
                alloc,
                "ddl:create_enum_type:type={s}:values={d}",
                .{ create.type_name, create.values.len },
            ),
            .add_value => |add| try std.fmt.allocPrint(
                alloc,
                "ddl:add_enum_value:type={s}:if_not_exists={}:position={s}",
                .{ add.type_name, add.if_not_exists, enumValuePositionName(add.position) },
            ),
            .drop => |drop| if (drop.cascade)
                try std.fmt.allocPrint(
                    alloc,
                    "ddl:drop_enum_type:type={s}:if_exists={}:cascade=true",
                    .{ drop.type_name, drop.if_exists },
                )
            else
                try std.fmt.allocPrint(
                    alloc,
                    "ddl:drop_enum_type:type={s}:if_exists={}",
                    .{ drop.type_name, drop.if_exists },
                ),
        },
        .domain_catalog => |plan| switch (plan) {
            .create => |create| try std.fmt.allocPrint(
                alloc,
                "ddl:create_domain:domain={s}:type={s}:checks={d}:not_null={}:default={}",
                .{
                    create.domain_name,
                    ddlTypeFingerprintName(create.field_type, create.array_item_type),
                    create.checks.len,
                    create.not_null,
                    create.default_value != null,
                },
            ),
            .alter => |alter| try std.fmt.allocPrint(
                alloc,
                "ddl:alter_domain:domain={s}:ops={d}",
                .{ alter.domain_name, alter.operations.len },
            ),
            .drop => |drop| if (drop.cascade)
                try std.fmt.allocPrint(
                    alloc,
                    "ddl:drop_domain:domain={s}:if_exists={}:cascade=true",
                    .{ drop.domain_name, drop.if_exists },
                )
            else
                try std.fmt.allocPrint(
                    alloc,
                    "ddl:drop_domain:domain={s}:if_exists={}",
                    .{ drop.domain_name, drop.if_exists },
                ),
        },
        .sequence_catalog => |plan| switch (plan) {
            .create => |create| try std.fmt.allocPrint(
                alloc,
                "ddl:create_sequence:sequence={s}:if_not_exists={}:options={d}",
                .{ create.sequence_name, create.if_not_exists, sequenceOptionCount(create.options) },
            ),
            .alter => |alter| try std.fmt.allocPrint(
                alloc,
                "ddl:alter_sequence:sequence={s}:if_exists={}:ops={d}",
                .{ alter.sequence_name, alter.if_exists, alter.operations.len },
            ),
            .drop => |drop| if (drop.cascade)
                try std.fmt.allocPrint(
                    alloc,
                    "ddl:drop_sequence:sequence={s}:if_exists={}:cascade=true",
                    .{ drop.sequence_name, drop.if_exists },
                )
            else
                try std.fmt.allocPrint(
                    alloc,
                    "ddl:drop_sequence:sequence={s}:if_exists={}",
                    .{ drop.sequence_name, drop.if_exists },
                ),
        },
        .identity_allocator_catalog => |plan| blk: {
            var fingerprint = try std.fmt.allocPrint(
                alloc,
                "ddl:identity_allocator:table={s}:column={s}:kind={s}:primary={}:columns={d}",
                .{
                    plan.table_name,
                    plan.column.name,
                    identityAllocatorKindName(plan.kind),
                    plan.primary_key,
                    plan.additional_columns.len,
                },
            );
            fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "options", sequenceOptionCount(plan.options));
            break :blk fingerprint;
        },
        .schema_namespace_catalog => |plan| switch (plan) {
            .create => |create| try std.fmt.allocPrint(
                alloc,
                "ddl:create_schema_namespace:schema={s}:if_not_exists={}",
                .{ create.schema_name, create.if_not_exists },
            ),
            .rename => |rename| try std.fmt.allocPrint(
                alloc,
                "ddl:rename_schema_namespace:schema={s}:new={s}",
                .{ rename.schema_name, rename.new_schema_name },
            ),
            .drop => |drop| if (drop.cascade)
                try std.fmt.allocPrint(
                    alloc,
                    "ddl:drop_schema_namespace:schema={s}:if_exists={}:cascade=true",
                    .{ drop.schema_name, drop.if_exists },
                )
            else
                try std.fmt.allocPrint(
                    alloc,
                    "ddl:drop_schema_namespace:schema={s}:if_exists={}",
                    .{ drop.schema_name, drop.if_exists },
                ),
        },
        .extension_catalog => |plan| switch (plan) {
            .create => |create| if (create.version) |version|
                try std.fmt.allocPrint(
                    alloc,
                    "ddl:create_extension:extension={s}:if_not_exists={}:version={s}",
                    .{ create.extension_name, create.if_not_exists, version },
                )
            else
                try std.fmt.allocPrint(
                    alloc,
                    "ddl:create_extension:extension={s}:if_not_exists={}",
                    .{ create.extension_name, create.if_not_exists },
                ),
            .update => |update| if (update.target_version) |version|
                try std.fmt.allocPrint(
                    alloc,
                    "ddl:alter_extension_update:extension={s}:version={s}",
                    .{ update.extension_name, version },
                )
            else
                try std.fmt.allocPrint(
                    alloc,
                    "ddl:alter_extension_update:extension={s}:version=latest",
                    .{update.extension_name},
                ),
            .drop => |drop| if (drop.cascade)
                try std.fmt.allocPrint(
                    alloc,
                    "ddl:drop_extension:extension={s}:if_exists={}:cascade=true",
                    .{ drop.extension_name, drop.if_exists },
                )
            else
                try std.fmt.allocPrint(
                    alloc,
                    "ddl:drop_extension:extension={s}:if_exists={}",
                    .{ drop.extension_name, drop.if_exists },
                ),
        },
        .function_catalog => |plan| switch (plan) {
            .create => |create| try createRoutineFingerprintAlloc(alloc, create),
            .drop => |drop| if (drop.cascade)
                try std.fmt.allocPrint(
                    alloc,
                    "ddl:drop_{s}:name={s}:args={d}:if_exists={}:cascade=true",
                    .{ routineKindName(drop.kind), drop.routine_name, drop.argument_count, drop.if_exists },
                )
            else
                try std.fmt.allocPrint(
                    alloc,
                    "ddl:drop_{s}:name={s}:args={d}:if_exists={}",
                    .{ routineKindName(drop.kind), drop.routine_name, drop.argument_count, drop.if_exists },
                ),
        },
        .trigger_catalog => |plan| switch (plan) {
            .create => |create| try std.fmt.allocPrint(
                alloc,
                "ddl:create_trigger:name={s}:table={s}:function={s}:event={s}:replace={}",
                .{ create.trigger_name, create.table_name, create.function_name, @tagName(create.event), create.replace_existing },
            ),
            .drop => |drop| try std.fmt.allocPrint(
                alloc,
                "ddl:drop_trigger:name={s}:table={s}:if_exists={}:cascade={}",
                .{ drop.trigger_name, drop.table_name, drop.if_exists, drop.cascade },
            ),
        },
        .procedure_call => |call| try std.fmt.allocPrint(
            alloc,
            "ddl:call_procedure:name={s}:args={d}",
            .{ call.routine_name, call.argument_count },
        ),
        .authorization_catalog => |plan| switch (plan) {
            .create_role => |create| try std.fmt.allocPrint(
                alloc,
                "ddl:create_role:role={s}",
                .{create.role_name},
            ),
            .alter_role => |alter| blk: {
                var fingerprint = if (alter.database_name) |database_name|
                    try std.fmt.allocPrint(
                        alloc,
                        "ddl:alter_role:role={s}:database={s}:operation={s}:setting={s}",
                        .{ alter.role_name, database_name, @tagName(alter.operation), alter.setting_name },
                    )
                else
                    try std.fmt.allocPrint(
                        alloc,
                        "ddl:alter_role:role={s}:operation={s}:setting={s}",
                        .{ alter.role_name, @tagName(alter.operation), alter.setting_name },
                    );
                if (alter.setting_kind != .app) {
                    fingerprint = try appendStringFingerprintAlloc(alloc, fingerprint, "setting_kind", @tagName(alter.setting_kind));
                }
                if (alter.setting_value) |setting_value| switch (setting_value) {
                    .literal => {},
                    .current_setting => fingerprint = try appendStringFingerprintAlloc(alloc, fingerprint, "value_source", "current_setting"),
                };
                break :blk fingerprint;
            },
            .drop_role => |drop| try std.fmt.allocPrint(
                alloc,
                "ddl:drop_role:role={s}:if_exists={}",
                .{ drop.role_name, drop.if_exists },
            ),
            .grant_privilege => |grant| try std.fmt.allocPrint(
                alloc,
                "ddl:grant_privilege:object={s}:{s}:principal={s}:privileges={d}",
                .{ grant.object_kind, grant.object_name, grant.principal_name, grant.privileges.len },
            ),
            .revoke_privilege => |revoke| try std.fmt.allocPrint(
                alloc,
                "ddl:revoke_privilege:object={s}:{s}:principal={s}:privileges={d}",
                .{ revoke.object_kind, revoke.object_name, revoke.principal_name, revoke.privileges.len },
            ),
        },
        .bulk_io => |plan| blk: {
            const delimiter_hex = try bulkIoByteOptionHexAlloc(alloc, plan.delimiter);
            defer alloc.free(delimiter_hex);
            const quote_hex = try bulkIoByteOptionHexAlloc(alloc, plan.quote);
            defer alloc.free(quote_hex);
            const escape_hex = try bulkIoByteOptionHexAlloc(alloc, plan.escape);
            defer alloc.free(escape_hex);
            const null_marker_hex = try bulkIoStringOptionHexAlloc(alloc, plan.null_marker);
            defer alloc.free(null_marker_hex);
            const default_marker_hex = try bulkIoStringOptionHexAlloc(alloc, plan.default_marker);
            defer alloc.free(default_marker_hex);
            const encoding_hex = try bulkIoStringOptionHexAlloc(alloc, plan.encoding);
            defer alloc.free(encoding_hex);
            const reject_limit = if (plan.reject_limit) |limit|
                try std.fmt.allocPrint(alloc, "{}", .{limit})
            else
                try alloc.dupe(u8, "none");
            defer alloc.free(reject_limit);
            break :blk if (plan.format) |format|
                try std.fmt.allocPrint(
                    alloc,
                    "ddl:copy_{s}:table={s}:columns={d}:endpoint={s}:format={s}:header={}:freeze={}:on_error={s}:reject_limit={s}:log_verbosity={s}:force_quote={s}:force_quote_columns={d}:force_not_null_columns={d}:force_null_columns={d}:delimiter_hex={s}:quote_hex={s}:escape_hex={s}:null_marker_hex={s}:default_marker_hex={s}:encoding_hex={s}:where_expressions={d}",
                    .{ bulkIoDirectionName(plan.direction), plan.table_name, plan.columns.len, plan.endpoint, format, plan.header, plan.freeze, bulkIoOnErrorName(plan.on_error), reject_limit, bulkIoLogVerbosityName(plan.log_verbosity), bulkIoForceQuoteName(plan), plan.force_quote_columns.len, plan.force_not_null_columns.len, plan.force_null_columns.len, delimiter_hex, quote_hex, escape_hex, null_marker_hex, default_marker_hex, encoding_hex, plan.where_expressions.len },
                )
            else
                try std.fmt.allocPrint(
                    alloc,
                    "ddl:copy_{s}:table={s}:columns={d}:endpoint={s}:header={}:freeze={}:on_error={s}:reject_limit={s}:log_verbosity={s}:force_quote={s}:force_quote_columns={d}:force_not_null_columns={d}:force_null_columns={d}:delimiter_hex={s}:quote_hex={s}:escape_hex={s}:null_marker_hex={s}:default_marker_hex={s}:encoding_hex={s}:where_expressions={d}",
                    .{ bulkIoDirectionName(plan.direction), plan.table_name, plan.columns.len, plan.endpoint, plan.header, plan.freeze, bulkIoOnErrorName(plan.on_error), reject_limit, bulkIoLogVerbosityName(plan.log_verbosity), bulkIoForceQuoteName(plan), plan.force_quote_columns.len, plan.force_not_null_columns.len, plan.force_null_columns.len, delimiter_hex, quote_hex, escape_hex, null_marker_hex, default_marker_hex, encoding_hex, plan.where_expressions.len },
                );
        },
        .table_partition_catalog => |plan| switch (plan) {
            .create_partitioned => |create| try std.fmt.allocPrint(
                alloc,
                "ddl:create_partitioned_table:table={s}:columns={d}:method={s}:keys={d}",
                .{
                    create.create_table.table_name,
                    create.create_table.columns.len,
                    tablePartitionMethodName(create.method),
                    create.keys.len,
                },
            ),
            .create_partition => |create| try std.fmt.allocPrint(
                alloc,
                "ddl:create_table_partition:table={s}:parent={s}:lower={s}:upper={s}",
                .{ create.table_name, create.parent_table_name, create.bounds.lower_json, create.bounds.upper_json },
            ),
            .attach => |attach| try std.fmt.allocPrint(
                alloc,
                "ddl:attach_table_partition:parent={s}:partition={s}:lower={s}:upper={s}",
                .{ attach.parent_table_name, attach.partition_table_name, attach.bounds.lower_json, attach.bounds.upper_json },
            ),
            .detach => |detach| try std.fmt.allocPrint(
                alloc,
                "ddl:detach_table_partition:parent={s}:partition={s}",
                .{ detach.parent_table_name, detach.partition_table_name },
            ),
        },
        .row_security_catalog => |plan| switch (plan) {
            .alter_table => |alter| try std.fmt.allocPrint(
                alloc,
                "ddl:{s}_row_security:table={s}",
                .{ if (alter.enabled) "enable" else "disable", alter.table_name },
            ),
            .create_policy => |create| blk: {
                const predicate_suffix = try expr_type.rowSecurityPredicateFingerprintSuffixAlloc(alloc, create.predicate);
                defer alloc.free(predicate_suffix);
                var base = try std.fmt.allocPrint(
                    alloc,
                    "ddl:create_row_policy:policy={s}:table={s}:{s}",
                    .{ create.policy_name, create.table_name, predicate_suffix },
                );
                if (create.check_predicate) |check_predicate| {
                    const check_suffix = try expr_type.rowSecurityPredicateFingerprintSuffixAlloc(alloc, check_predicate);
                    defer alloc.free(check_suffix);
                    const with_check = try std.fmt.allocPrint(alloc, "{s}:check={s}", .{ base, check_suffix });
                    alloc.free(base);
                    base = with_check;
                }
                break :blk try appendRowSecurityRoleTargetsAlloc(alloc, base, create.role_targets);
            },
            .alter_policy => |alter| blk: {
                var base = try std.fmt.allocPrint(
                    alloc,
                    "ddl:alter_row_policy:policy={s}:table={s}",
                    .{ alter.policy_name, alter.table_name },
                );
                if (alter.predicate) |predicate| {
                    const predicate_suffix = try expr_type.rowSecurityPredicateFingerprintSuffixAlloc(alloc, predicate);
                    defer alloc.free(predicate_suffix);
                    const with_predicate = try std.fmt.allocPrint(alloc, "{s}:{s}", .{ base, predicate_suffix });
                    alloc.free(base);
                    base = with_predicate;
                }
                if (alter.check_predicate) |check_predicate| {
                    const check_suffix = try expr_type.rowSecurityPredicateFingerprintSuffixAlloc(alloc, check_predicate);
                    defer alloc.free(check_suffix);
                    const with_check = try std.fmt.allocPrint(alloc, "{s}:check={s}", .{ base, check_suffix });
                    alloc.free(base);
                    base = with_check;
                }
                if (alter.role_targets_present) break :blk try appendRowSecurityRoleTargetsAlloc(alloc, base, alter.role_targets);
                break :blk base;
            },
            .drop_policy => |drop| try std.fmt.allocPrint(
                alloc,
                "ddl:drop_row_policy:policy={s}:table={s}:if_exists={}",
                .{ drop.policy_name, drop.table_name, drop.if_exists },
            ),
        },
        .database_catalog => |plan| switch (plan) {
            .create => |create| try std.fmt.allocPrint(
                alloc,
                "ddl:create_database:database={s}",
                .{create.database_name},
            ),
            .alter => |alter| try std.fmt.allocPrint(
                alloc,
                "ddl:alter_database:database={s}:ops={d}",
                .{ alter.database_name, alter.operations.len },
            ),
            .drop => |drop| if (drop.force)
                try std.fmt.allocPrint(
                    alloc,
                    "ddl:drop_database:database={s}:if_exists={}:force=true",
                    .{ drop.database_name, drop.if_exists },
                )
            else
                try std.fmt.allocPrint(
                    alloc,
                    "ddl:drop_database:database={s}:if_exists={}",
                    .{ drop.database_name, drop.if_exists },
                ),
        },
        .tablespace_catalog => |plan| switch (plan) {
            .create => |create| try std.fmt.allocPrint(
                alloc,
                "ddl:create_tablespace:tablespace={s}:location=true",
                .{create.tablespace_name},
            ),
            .rename => |rename| try std.fmt.allocPrint(
                alloc,
                "ddl:rename_tablespace:tablespace={s}:new={s}",
                .{ rename.tablespace_name, rename.new_tablespace_name },
            ),
            .drop => |drop| try std.fmt.allocPrint(
                alloc,
                "ddl:drop_tablespace:tablespace={s}:if_exists={}",
                .{ drop.tablespace_name, drop.if_exists },
            ),
        },
        .notification_channel => |plan| switch (plan) {
            .listen => |listen| try std.fmt.allocPrint(
                alloc,
                "ddl:listen_notification:channel={s}",
                .{listen.channel_name},
            ),
            .notify => |notify| try std.fmt.allocPrint(
                alloc,
                "ddl:notify_notification:channel={s}:payload={}",
                .{ notify.channel_name, notify.payload_json != null },
            ),
            .unlisten => |unlisten| if (unlisten.all)
                try std.fmt.allocPrint(
                    alloc,
                    "ddl:unlisten_notification:all=true",
                    .{},
                )
            else
                try std.fmt.allocPrint(
                    alloc,
                    "ddl:unlisten_notification:channel={s}",
                    .{unlisten.channel_name orelse return error.TestUnexpectedResult},
                ),
        },
        .logical_replication => |plan| switch (plan) {
            .publication => |publication| switch (publication) {
                .create => |create| try std.fmt.allocPrint(
                    alloc,
                    "ddl:create_publication:publication={s}:tables={d}:all={}",
                    .{ create.publication_name, create.table_names.len, create.all_tables },
                ),
                .alter => |alter| switch (alter.operation) {
                    .add_tables => |tables| try std.fmt.allocPrint(
                        alloc,
                        "ddl:alter_publication:publication={s}:add_tables={d}",
                        .{ alter.publication_name, tables.len },
                    ),
                },
                .drop => |drop| try std.fmt.allocPrint(
                    alloc,
                    "ddl:drop_publication:publication={s}:if_exists={}",
                    .{ drop.publication_name, drop.if_exists },
                ),
            },
            .subscription => |subscription| switch (subscription) {
                .create => |create| try std.fmt.allocPrint(
                    alloc,
                    "ddl:create_subscription:subscription={s}:connection=true:publications={d}",
                    .{ create.subscription_name, create.publication_names.len },
                ),
                .alter => |alter| try std.fmt.allocPrint(
                    alloc,
                    "ddl:alter_subscription:subscription={s}:enabled={}",
                    .{ alter.subscription_name, alter.enabled },
                ),
                .drop => |drop| try std.fmt.allocPrint(
                    alloc,
                    "ddl:drop_subscription:subscription={s}:if_exists={}",
                    .{ drop.subscription_name, drop.if_exists },
                ),
            },
        },
        .type_system_catalog => |plan| switch (plan) {
            .collation => |collation| switch (collation) {
                .create => |create| try std.fmt.allocPrint(
                    alloc,
                    "ddl:create_collation:collation={s}:options={d}",
                    .{ create.collation_name, create.option_count },
                ),
                .rename => |rename| try std.fmt.allocPrint(
                    alloc,
                    "ddl:rename_collation:collation={s}:new={s}",
                    .{ rename.collation_name, rename.new_collation_name },
                ),
                .drop => |drop| try std.fmt.allocPrint(
                    alloc,
                    "ddl:drop_collation:collation={s}:if_exists={}",
                    .{ drop.collation_name, drop.if_exists },
                ),
            },
            .operator => |operator| switch (operator) {
                .create => |create| try std.fmt.allocPrint(
                    alloc,
                    "ddl:create_operator:operator={s}:options={d}",
                    .{ create.operator_name, create.option_count },
                ),
                .drop => |drop| try std.fmt.allocPrint(
                    alloc,
                    "ddl:drop_operator:operator={s}:args={d}:if_exists={}",
                    .{ drop.operator_name, drop.argument_count, drop.if_exists },
                ),
            },
            .aggregate => |aggregate| switch (aggregate) {
                .create => |create| try std.fmt.allocPrint(
                    alloc,
                    "ddl:create_aggregate:aggregate={s}:args={d}:options={d}",
                    .{ create.aggregate_name, create.argument_count, create.option_count },
                ),
                .drop => |drop| try std.fmt.allocPrint(
                    alloc,
                    "ddl:drop_aggregate:aggregate={s}:args={d}:if_exists={}",
                    .{ drop.aggregate_name, drop.argument_count, drop.if_exists },
                ),
            },
            .cast => |cast| switch (cast) {
                .create => |create| try std.fmt.allocPrint(
                    alloc,
                    "ddl:create_cast:source={s}:target={s}:function={s}:assignment={}",
                    .{ create.source_type, create.target_type, create.function_name, create.assignment },
                ),
                .drop => |drop| try std.fmt.allocPrint(
                    alloc,
                    "ddl:drop_cast:source={s}:target={s}:if_exists={}",
                    .{ drop.source_type, drop.target_type, drop.if_exists },
                ),
            },
        },
        .maintenance_job => |plan| switch (plan) {
            .vacuum => |vacuum| try std.fmt.allocPrint(
                alloc,
                "ddl:maintenance:kind=vacuum:table={s}:full={}:freeze={}:verbose={}:analyze={}",
                .{ vacuum.table_name, vacuum.full, vacuum.freeze, vacuum.verbose, vacuum.analyze },
            ),
            .analyze => |analyze| try std.fmt.allocPrint(
                alloc,
                "ddl:maintenance:kind=analyze:table={s}:verbose={}:columns={d}",
                .{ analyze.table_name, analyze.verbose, analyze.column_count },
            ),
            .reindex => |reindex| try std.fmt.allocPrint(
                alloc,
                "ddl:maintenance:kind=reindex:target={s}:name={s}:concurrently={}",
                .{ @tagName(reindex.target), reindex.name, reindex.concurrently },
            ),
            .cluster => |cluster| try std.fmt.allocPrint(
                alloc,
                "ddl:maintenance:kind=cluster:table={s}:index={s}:verbose={}",
                .{ cluster.table_name, cluster.index_name orelse "", cluster.verbose },
            ),
        },
        .prepared_statement => |plan| switch (plan) {
            .prepare => |prepare| try std.fmt.allocPrint(
                alloc,
                "ddl:prepare_statement:name={s}:params={d}:subject={s}:statement={s}",
                .{ prepare.statement_name, prepare.parameter_count, @tagName(prepare.statement_kind), @tagName(prepare.statement_family) },
            ),
            .execute => |execute| try std.fmt.allocPrint(
                alloc,
                "ddl:execute_statement:name={s}:args={d}",
                .{ execute.statement_name, execute.argument_count },
            ),
            .deallocate => |deallocate| if (deallocate.all)
                try std.fmt.allocPrint(
                    alloc,
                    "ddl:deallocate_statement:all=true",
                    .{},
                )
            else
                try std.fmt.allocPrint(
                    alloc,
                    "ddl:deallocate_statement:name={s}",
                    .{deallocate.statement_name orelse return error.TestUnexpectedResult},
                ),
        },
        .prepared_transaction => |plan| try std.fmt.allocPrint(
            alloc,
            "ddl:prepared_transaction:action={s}:gid={s}",
            .{ @tagName(plan.action), plan.gid },
        ),
        .cursor_portal => |plan| switch (plan) {
            .declare => |declare| try std.fmt.allocPrint(
                alloc,
                "ddl:declare_cursor:portal={s}:scroll={s}:binary={}:hold={}:subject={s}",
                .{ declare.portal_name, @tagName(declare.scroll), declare.binary, declare.hold, @tagName(declare.statement_kind) },
            ),
            .fetch => |fetch| if (fetch.count) |count|
                try std.fmt.allocPrint(
                    alloc,
                    "ddl:fetch_cursor:portal={s}:direction={s}:count={d}",
                    .{ fetch.portal_name, @tagName(fetch.direction), count },
                )
            else
                try std.fmt.allocPrint(
                    alloc,
                    "ddl:fetch_cursor:portal={s}:direction={s}",
                    .{ fetch.portal_name, @tagName(fetch.direction) },
                ),
            .move => |move| if (move.count) |count|
                try std.fmt.allocPrint(
                    alloc,
                    "ddl:move_cursor:portal={s}:direction={s}:count={d}",
                    .{ move.portal_name, @tagName(move.direction), count },
                )
            else
                try std.fmt.allocPrint(
                    alloc,
                    "ddl:move_cursor:portal={s}:direction={s}",
                    .{ move.portal_name, @tagName(move.direction) },
                ),
            .close => |close| if (close.all)
                try std.fmt.allocPrint(
                    alloc,
                    "ddl:close_cursor:all=true",
                    .{},
                )
            else
                try std.fmt.allocPrint(
                    alloc,
                    "ddl:close_cursor:portal={s}",
                    .{close.portal_name orelse return error.TestUnexpectedResult},
                ),
        },
        .savepoint_transaction => |plan| switch (plan) {
            .savepoint => |savepoint| try std.fmt.allocPrint(
                alloc,
                "ddl:savepoint:name={s}",
                .{savepoint.savepoint_name},
            ),
            .release => |release| try std.fmt.allocPrint(
                alloc,
                "ddl:release_savepoint:name={s}",
                .{release.savepoint_name},
            ),
            .rollback_to => |rollback| try std.fmt.allocPrint(
                alloc,
                "ddl:rollback_to_savepoint:name={s}",
                .{rollback.savepoint_name},
            ),
        },
        .comment_metadata => |plan| if (plan.parent_table_name) |parent_table|
            try std.fmt.allocPrint(
                alloc,
                "ddl:comment:kind={s}:object={s}:table={s}:comment={}",
                .{ @tagName(plan.target), plan.object_name, parent_table, plan.comment_json != null },
            )
        else
            try std.fmt.allocPrint(
                alloc,
                "ddl:comment:kind={s}:object={s}:comment={}",
                .{ @tagName(plan.target), plan.object_name, plan.comment_json != null },
            ),
        .transaction_control => |plan| switch (plan) {
            .table_lock => |lock| try std.fmt.allocPrint(
                alloc,
                "ddl:transaction_control:kind=table_lock:tables={d}:mode={s}",
                .{ lock.table_names.len, @tagName(lock.mode) },
            ),
            .constraint_mode => |constraints| try std.fmt.allocPrint(
                alloc,
                "ddl:transaction_control:kind=constraint_mode:all={}:constraints={d}:mode={s}",
                .{ constraints.all, constraints.constraint_names.len, @tagName(constraints.mode) },
            ),
            .transaction_mode => |transaction| try std.fmt.allocPrint(
                alloc,
                "ddl:transaction_control:kind=transaction_mode:starter={s}:isolation={s}:access={s}:deferrable={s}",
                .{
                    @tagName(transaction.starter),
                    transactionIsolationLevelName(transaction.isolation_level),
                    transactionAccessModeName(transaction.access_mode),
                    transactionDeferrableName(transaction.deferrable),
                },
            ),
            .advisory_lock => |lock| try std.fmt.allocPrint(
                alloc,
                "ddl:transaction_control:kind=advisory_lock:action={s}:keys={d}",
                .{ @tagName(lock.action), if (lock.key2 != null) @as(usize, 2) else @as(usize, 1) },
            ),
        },
        .create_index => |plan| blk: {
            const base = if (plan.method == .gin)
                try std.fmt.allocPrint(
                    alloc,
                    "ddl:create_index:table={s}:columns={d}:expr={d}:generated_expr={d}:where={d}:unique={}:if_not_exists={}:method=gin",
                    .{ plan.table_name, plan.columns.len, plan.expressions.len, createIndexPlanGeneratedExpressionCount(plan), plan.where.len, plan.unique, plan.if_not_exists },
                )
            else
                try std.fmt.allocPrint(
                    alloc,
                    "ddl:create_index:table={s}:columns={d}:expr={d}:generated_expr={d}:where={d}:unique={}:if_not_exists={}",
                    .{ plan.table_name, plan.columns.len, plan.expressions.len, createIndexPlanGeneratedExpressionCount(plan), plan.where.len, plan.unique, plan.if_not_exists },
                );
            const with_generated_op = if (createIndexPlanGeneratedExpressionOp(plan)) |generated_op|
                try appendStringFingerprintAlloc(alloc, base, "generated_op", generated_op)
            else
                base;
            const with_include = try appendNonZeroUsizeFingerprintAlloc(alloc, with_generated_op, "include", plan.include_columns.len);
            const with_index_keys = try appendNonZeroUsizeFingerprintAlloc(alloc, with_include, "index_keys", plan.index_keys.len);
            const with_where_expr = try appendNonZeroUsizeFingerprintAlloc(alloc, with_index_keys, "where_expr", plan.where_expressions.len);
            const with_temporal = try appendTrueBoolFingerprintAlloc(alloc, with_where_expr, "temporal_unique", plan.without_overlaps_period != null);
            break :blk try appendTrueBoolFingerprintAlloc(alloc, with_temporal, "nulls_not_distinct", plan.nulls_not_distinct);
        },
        .drop_index => |plan| try std.fmt.allocPrint(
            alloc,
            "ddl:drop_index:index={s}:if_exists={}",
            .{ plan.index_name, plan.if_exists },
        ),
        .drop_table => |plan| if (plan.cascade)
            try std.fmt.allocPrint(
                alloc,
                "ddl:drop_table:table={s}:if_exists={}:cascade=true",
                .{ plan.table_name, plan.if_exists },
            )
        else
            try std.fmt.allocPrint(
                alloc,
                "ddl:drop_table:table={s}:if_exists={}",
                .{ plan.table_name, plan.if_exists },
            ),
        .alter_table => |plan| blk: {
            if (plan.operations.len == 1) switch (plan.operations[0]) {
                .drop_update_policy => |drop| break :blk try std.fmt.allocPrint(
                    alloc,
                    "ddl:drop_trigger:name={s}:table={s}:if_exists={}:cascade=false",
                    .{ drop.trigger_name, plan.table_name, drop.if_exists },
                ),
                else => {},
            };
            var fingerprint = try std.fmt.allocPrint(
                alloc,
                "ddl:alter_table:table={s}:ops={d}:if_exists={}",
                .{ plan.table_name, plan.operations.len, plan.if_exists },
            );
            const counts = alterTablePlanFingerprintCounts(plan);
            fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "add_col", counts.add_column);
            fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "add_col_if_not_exists", counts.add_column_if_not_exists);
            fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "add_col_default", counts.add_column_default);
            fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "add_col_generated", counts.add_column_generated);
            fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "add_col_update_policy", counts.add_column_update_policy);
            fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "add_col_unique", counts.add_column_unique);
            fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "add_col_fk", counts.add_column_fk);
            fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "add_col_check", counts.add_column_check);
            fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "add_period", counts.add_period);
            fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "add_pk", counts.add_primary_key);
            fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "rename_col", counts.rename_column);
            fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "rename_constraint", counts.rename_constraint);
            fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "drop_col", counts.drop_column);
            fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "drop_col_if_exists", counts.drop_column_if_exists);
            fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "drop_constraint", counts.drop_constraint);
            fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "drop_constraint_if_exists", counts.drop_constraint_if_exists);
            fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "drop_update_policy", counts.drop_update_policy);
            fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "drop_update_policy_if_exists", counts.drop_update_policy_if_exists);
            fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "set_default", counts.set_default);
            fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "drop_default", counts.drop_default);
            fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "set_not_null", counts.set_not_null);
            fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "drop_not_null", counts.drop_not_null);
            fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "alter_type", counts.alter_type);
            fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "alter_type_rewrite_expr", counts.alter_type_rewrite_expr);
            fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "add_unique", counts.add_unique);
            fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "add_fk", counts.add_foreign_key);
            fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "add_check", counts.add_check);
            fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "validate", counts.validate_constraint);
            fingerprint = try appendConstraintTimingFingerprintsAlloc(alloc, fingerprint, counts.constraint_timing);
            fingerprint = try appendForeignKeyOptionFingerprintsAlloc(alloc, fingerprint, counts.foreign_key_options);
            break :blk fingerprint;
        },
        .create_update_policy => |plan| try std.fmt.allocPrint(
            alloc,
            "ddl:create_update_policy:table={s}:column={s}",
            .{ plan.table_name, plan.column_name },
        ),
    };
}

pub fn ddlAppliedFingerprintAlloc(alloc: std.mem.Allocator, applied: AppliedDdlSchemaJson) ![]u8 {
    if (applied.schema_json.len == 0) {
        const base = try std.fmt.allocPrint(
            alloc,
            "applied:drop_table:rebuild={}:validation={}:rewrite={}",
            .{ applied.requires_rebuild, applied.validation_required, applied.rewrite_required },
        );
        return try appendAppliedDdlWorkItemsFingerprintAlloc(alloc, base, applied.work_items);
    }

    var parsed = try schema_api.parseValidatedTableSchema(alloc, applied.schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var raw_arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer raw_arena_impl.deinit();
    var raw_parsed = try std.json.parseFromSlice(std.json.Value, raw_arena_impl.allocator(), applied.schema_json, .{});
    const raw_root = switch (raw_parsed.value) {
        .object => |*object| object,
        else => return error.InvalidSqlCatalog,
    };
    const comments = try schemaJsonCommentCountInRoot(raw_root);

    var building_indexes: usize = 0;
    var update_policies: usize = 0;
    for (schema.relational_columns) |column| {
        if (column.index_lifecycle != .ready) building_indexes += 1;
        if (column.on_update_value != null) update_policies += 1;
    }

    var unvalidated_unique: usize = 0;
    for (schema.unique_constraints) |constraint| {
        if (constraint.validation_state != .enforced) unvalidated_unique += 1;
    }
    var unvalidated_fk: usize = 0;
    for (schema.foreign_keys) |foreign_key| {
        if (foreign_key.validation_state != .enforced) unvalidated_fk += 1;
    }
    var unvalidated_check: usize = 0;
    for (schema.checks) |check| {
        if (check.validation_state != .enforced) unvalidated_check += 1;
    }

    const base = try std.fmt.allocPrint(
        alloc,
        "applied:rebuild={}:validation={}:rewrite={}:building_indexes={d}:unvalidated_unique={d}:unvalidated_fk={d}:unvalidated_check={d}:update_policy={d}",
        .{
            applied.requires_rebuild,
            applied.validation_required,
            applied.rewrite_required,
            building_indexes,
            unvalidated_unique,
            unvalidated_fk,
            unvalidated_check,
            update_policies,
        },
    );
    const with_work = try appendAppliedDdlWorkItemsFingerprintAlloc(alloc, base, applied.work_items);
    return try appendNonZeroUsizeFingerprintAlloc(alloc, with_work, "comments", comments);
}

fn appliedDdlWorkActionName(action: AppliedDdlWorkAction) []const u8 {
    return switch (action) {
        .rebuild => "rebuild",
        .validate => "validate",
        .rewrite => "rewrite",
    };
}

fn appliedDdlWorkSubjectName(subject: AppliedDdlWorkSubject) []const u8 {
    return switch (subject) {
        .table => "table",
    };
}

fn appliedDdlWorkReasonName(reason: AppliedDdlWorkReason) []const u8 {
    return switch (reason) {
        .derived_artifacts => "derived_artifacts",
        .constraints => "constraints",
        .row_images => "row_images",
    };
}

fn appliedDdlWorkItemFingerprintAlloc(alloc: std.mem.Allocator, item: AppliedDdlWorkItem) ![]u8 {
    const base = try std.fmt.allocPrint(
        alloc,
        "{s}/{s}/{s}",
        .{
            appliedDdlWorkActionName(item.action),
            appliedDdlWorkSubjectName(item.subject),
            appliedDdlWorkReasonName(item.reason),
        },
    );
    if (item.rewrite_expression) |rewrite| {
        const expression = try expr_type.rowRewriteExpressionFingerprintAlloc(alloc, rewrite.expression);
        defer alloc.free(expression);
        const with_expression = try std.fmt.allocPrint(
            alloc,
            "{s}(target={s}:expr={s})",
            .{ base, rewrite.target_column, expression },
        );
        alloc.free(base);
        return with_expression;
    }
    if (!item.row_rewrite_plan.empty()) {
        var payload = try alloc.dupe(u8, "");
        defer alloc.free(payload);
        for (item.row_rewrite_plan.renames) |rename| {
            const next = try std.fmt.allocPrint(
                alloc,
                "{s}:rename({s}->{s})",
                .{ payload, rename.old_path, rename.new_path },
            );
            alloc.free(payload);
            payload = next;
        }
        for (item.row_rewrite_plan.drops) |drop| {
            const next = try std.fmt.allocPrint(
                alloc,
                "{s}:drop({s})",
                .{ payload, drop },
            );
            alloc.free(payload);
            payload = next;
        }
        const with_row_plan = try std.fmt.allocPrint(
            alloc,
            "{s}(row_plan={s})",
            .{ base, payload },
        );
        alloc.free(base);
        return with_row_plan;
    }
    return base;
}

fn appendAppliedDdlWorkItemsFingerprintAlloc(
    alloc: std.mem.Allocator,
    owned_base: []u8,
    work_items: []const AppliedDdlWorkItem,
) ![]u8 {
    errdefer alloc.free(owned_base);
    if (work_items.len == 0) {
        const fingerprint = try std.fmt.allocPrint(
            alloc,
            "{s}:work_items=0:work=none",
            .{owned_base},
        );
        alloc.free(owned_base);
        return fingerprint;
    }

    var work = try alloc.dupe(u8, "");
    defer alloc.free(work);
    for (work_items, 0..) |item, i| {
        const item_fingerprint = try appliedDdlWorkItemFingerprintAlloc(alloc, item);
        defer alloc.free(item_fingerprint);
        const next = try std.fmt.allocPrint(
            alloc,
            "{s}{s}{s}",
            .{
                work,
                if (i == 0) "" else ",",
                item_fingerprint,
            },
        );
        alloc.free(work);
        work = next;
    }

    const fingerprint = try std.fmt.allocPrint(
        alloc,
        "{s}:work_items={d}:work={s}",
        .{ owned_base, work_items.len, work },
    );
    alloc.free(owned_base);
    return fingerprint;
}

fn createTablePlanHasTemporalPrimaryKey(plan: CreateTablePlan) bool {
    const primary_key = plan.primary_key orelse return false;
    return primary_key.without_overlaps_period != null;
}

fn createTablePlanTemporalUniqueCount(plan: CreateTablePlan) usize {
    var count: usize = 0;
    for (plan.unique_constraints) |constraint| {
        if (constraint.without_overlaps_period != null) count += 1;
    }
    return count;
}

fn createTablePlanTemporalForeignKeyCount(plan: CreateTablePlan) usize {
    var count: usize = 0;
    for (plan.foreign_keys) |foreign_key| {
        if (foreign_key.child_period != null or foreign_key.parent_period != null) count += 1;
    }
    return count;
}

pub fn relationLifetimeKindName(kind: RelationLifetimeKind) []const u8 {
    return switch (kind) {
        .temporary => "temporary",
        .unlogged => "unlogged",
    };
}

fn routineKindName(kind: RoutineKind) []const u8 {
    return switch (kind) {
        .function => "function",
        .procedure => "procedure",
    };
}

fn routineVolatilityName(volatility: RoutineVolatility) []const u8 {
    return switch (volatility) {
        .immutable => "immutable",
        .stable => "stable",
        .@"volatile" => "volatile",
    };
}

fn routineSecurityName(security: RoutineSecurity) []const u8 {
    return switch (security) {
        .invoker => "invoker",
        .definer => "definer",
    };
}

fn routineNullInputName(null_input: RoutineNullInput) []const u8 {
    return switch (null_input) {
        .called => "called",
        .returns_null => "returns_null",
    };
}

fn routineParallelSafetyName(parallel_safety: RoutineParallelSafety) []const u8 {
    return switch (parallel_safety) {
        .safe => "safe",
        .restricted => "restricted",
        .unsafe => "unsafe",
    };
}

fn routineBodyKindName(kind: RoutineBodyKind) []const u8 {
    return switch (kind) {
        .sql_expression => "sql_expression",
        .plpgsql_trigger => "plpgsql_trigger",
        .plpgsql_procedure => "plpgsql_procedure",
    };
}

fn routineExecutionHookName(hook: RoutineExecutionHook) []const u8 {
    return switch (hook) {
        .expression => "expression",
        .trigger_return_new => "trigger_return_new",
        .trigger_return_old => "trigger_return_old",
        .trigger_return_null => "trigger_return_null",
        .procedure_noop => "procedure_noop",
    };
}

fn createRoutineFingerprintAlloc(alloc: std.mem.Allocator, create: CreateRoutinePlan) ![]u8 {
    var base = if (create.language) |language|
        try std.fmt.allocPrint(
            alloc,
            "ddl:create_{s}:name={s}:args={d}:replace={}:returns={s}:language={s}",
            .{
                routineKindName(create.kind),
                create.routine_name,
                create.argument_count,
                create.replace_existing,
                create.returns_type orelse "",
                language,
            },
        )
    else
        try std.fmt.allocPrint(
            alloc,
            "ddl:create_{s}:name={s}:args={d}:replace={}:returns={s}",
            .{
                routineKindName(create.kind),
                create.routine_name,
                create.argument_count,
                create.replace_existing,
                create.returns_type orelse "",
            },
        );
    errdefer alloc.free(base);
    if (create.volatility) |volatility| {
        const next = try std.fmt.allocPrint(
            alloc,
            "{s}:volatility={s}",
            .{ base, routineVolatilityName(volatility) },
        );
        alloc.free(base);
        base = next;
    }
    if (create.security) |security| {
        const next = try std.fmt.allocPrint(
            alloc,
            "{s}:security={s}",
            .{ base, routineSecurityName(security) },
        );
        alloc.free(base);
        base = next;
    }
    if (create.null_input) |null_input| {
        const next = try std.fmt.allocPrint(
            alloc,
            "{s}:null_input={s}",
            .{ base, routineNullInputName(null_input) },
        );
        alloc.free(base);
        base = next;
    }
    if (create.parallel_safety) |parallel_safety| {
        const next = try std.fmt.allocPrint(
            alloc,
            "{s}:parallel={s}",
            .{ base, routineParallelSafetyName(parallel_safety) },
        );
        alloc.free(base);
        base = next;
    }
    if (create.leakproof) {
        const next = try std.fmt.allocPrint(alloc, "{s}:leakproof=true", .{base});
        alloc.free(base);
        base = next;
    }
    if (create.window) {
        const next = try std.fmt.allocPrint(alloc, "{s}:window=true", .{base});
        alloc.free(base);
        base = next;
    }
    if (create.support_function) |support_function| {
        const next = try std.fmt.allocPrint(
            alloc,
            "{s}:support={s}",
            .{ base, support_function },
        );
        alloc.free(base);
        base = next;
    }
    if (create.transform_types.len != 0) {
        var next = try std.fmt.allocPrint(
            alloc,
            "{s}:transforms={d}",
            .{ base, create.transform_types.len },
        );
        alloc.free(base);
        errdefer alloc.free(next);
        for (create.transform_types) |transform_type| {
            const appended = try std.fmt.allocPrint(
                alloc,
                "{s}:transform={s}",
                .{ next, transform_type },
            );
            alloc.free(next);
            next = appended;
        }
        base = next;
    }
    if (create.settings.len != 0) {
        var next = try std.fmt.allocPrint(
            alloc,
            "{s}:settings={d}",
            .{ base, create.settings.len },
        );
        alloc.free(base);
        errdefer alloc.free(next);
        for (create.settings) |setting| {
            const setting_base = if (setting.from_current)
                try std.fmt.allocPrint(
                    alloc,
                    "{s}:setting={s}:from_current=true",
                    .{ next, setting.name },
                )
            else
                try std.fmt.allocPrint(
                    alloc,
                    "{s}:setting={s}:values={d}",
                    .{ next, setting.name, setting.values.len },
                );
            alloc.free(next);
            next = setting_base;
            if (!setting.from_current) {
                for (setting.values) |value| {
                    const appended = try std.fmt.allocPrint(
                        alloc,
                        "{s}:value={s}",
                        .{ next, value },
                    );
                    alloc.free(next);
                    next = appended;
                }
            }
        }
        base = next;
    }
    if (create.cost) |cost| {
        const next = try std.fmt.allocPrint(
            alloc,
            "{s}:cost={s}",
            .{ base, cost },
        );
        alloc.free(base);
        base = next;
    }
    if (create.rows) |rows| {
        const next = try std.fmt.allocPrint(
            alloc,
            "{s}:rows={s}",
            .{ base, rows },
        );
        alloc.free(base);
        base = next;
    }
    if (create.body) |body| {
        var next = try std.fmt.allocPrint(
            alloc,
            "{s}:body={s}:hook={s}",
            .{
                base,
                routineBodyKindName(body.kind),
                routineExecutionHookName(body.hook),
            },
        );
        alloc.free(base);
        if (body.expression) |body_expression| {
            errdefer alloc.free(next);
            const expression = try expr_type.rowRewriteExpressionFingerprintAlloc(alloc, body_expression);
            defer alloc.free(expression);
            const with_expression = try std.fmt.allocPrint(
                alloc,
                "{s}:expr={s}",
                .{ next, expression },
            );
            alloc.free(next);
            next = with_expression;
        }
        if (body.perform_calls.len != 0) {
            const with_count = try appendNonZeroUsizeFingerprintAlloc(alloc, next, "perform", body.perform_calls.len);
            next = with_count;
            for (body.perform_calls) |call| {
                const with_routine = try std.fmt.allocPrint(
                    alloc,
                    "{s}:perform={s}:perform_args={d}",
                    .{ next, call.routine_name, call.argument_json.len },
                );
                alloc.free(next);
                var with_args = with_routine;
                for (call.argument_json) |argument_json| {
                    with_args = try appendStringFingerprintAlloc(alloc, with_args, "arg", argument_json);
                }
                next = with_args;
            }
        }
        base = next;
    }
    return base;
}

fn bulkIoDirectionName(direction: BulkIoDirection) []const u8 {
    return switch (direction) {
        .from => "from",
        .to => "to",
    };
}

fn bulkIoEndpointKindName(kind: BulkIoEndpointKind) []const u8 {
    return switch (kind) {
        .stream => "stream",
        .file => "file",
        .program => "program",
    };
}

fn bulkIoOnErrorName(policy: BulkIoOnErrorPolicy) []const u8 {
    return switch (policy) {
        .stop => "stop",
        .ignore => "ignore",
    };
}

fn bulkIoLogVerbosityName(verbosity: BulkIoLogVerbosity) []const u8 {
    return switch (verbosity) {
        .default => "default",
        .verbose => "verbose",
        .terse => "terse",
    };
}

fn bulkIoForceQuoteName(plan: BulkIoPlan) []const u8 {
    if (plan.force_quote_all) return "all";
    if (plan.force_quote_columns.len != 0) return "columns";
    return "none";
}

fn bulkIoByteOptionHexAlloc(alloc: std.mem.Allocator, option: ?[]const u8) ![]const u8 {
    const value = option orelse return try alloc.dupe(u8, "default");
    if (value.len != 1) return error.UnsupportedSqlShape;
    return try std.fmt.allocPrint(alloc, "{x:0>2}", .{value[0]});
}

fn appendRowSecurityRoleTargetsAlloc(
    alloc: std.mem.Allocator,
    base: []u8,
    role_targets: []const []const u8,
) ![]u8 {
    if (role_targets.len == 0) return base;
    var out = try std.fmt.allocPrint(alloc, "{s}:roles={d}", .{ base, role_targets.len });
    alloc.free(base);
    errdefer alloc.free(out);
    for (role_targets) |role| {
        const next = try std.fmt.allocPrint(alloc, "{s}:role={s}", .{ out, role });
        alloc.free(out);
        out = next;
    }
    return out;
}

fn bulkIoStringOptionHexAlloc(alloc: std.mem.Allocator, option: ?[]const u8) ![]const u8 {
    const value = option orelse return try alloc.dupe(u8, "default");
    if (value.len == 0) return try alloc.dupe(u8, "empty");
    const out = try alloc.alloc(u8, value.len * 2);
    const hex = "0123456789abcdef";
    for (value, 0..) |byte, i| {
        out[i * 2] = hex[byte >> 4];
        out[i * 2 + 1] = hex[byte & 0x0f];
    }
    return out;
}

fn tablePartitionMethodName(method: TablePartitionMethod) []const u8 {
    return switch (method) {
        .range => "range",
    };
}

fn enumValuePositionName(position: EnumValuePosition) []const u8 {
    return switch (position) {
        .none => "none",
        .before => "before",
        .after => "after",
    };
}

fn identityAllocatorKindName(kind: IdentityAllocatorKind) []const u8 {
    return switch (kind) {
        .serial => "serial",
        .bigserial => "bigserial",
        .generated_by_default => "generated_by_default",
        .generated_always => "generated_always",
    };
}

fn ddlTypeFingerprintName(field_type: runtime_schema.AntflyType, array_item_type: ?runtime_schema.AntflyType) []const u8 {
    if (field_type != .array) return antflyTypeSchemaName(field_type);
    return switch (array_item_type orelse return "array") {
        .text => "array_text",
        .keyword => "array_keyword",
        .numeric => "array_numeric",
        .embedding => "array_embedding",
        .boolean => "array_boolean",
        .datetime => "array_datetime",
        .geopoint => "array_geopoint",
        .geoshape => "array_geoshape",
        .blob => "array_blob",
        .html => "array_html",
        .search_as_you_type => "array_search_as_you_type",
        .json => "array_json",
        .array => "array_array",
        .link => "array_link",
    };
}

pub fn sequenceOptionCount(options: SequenceOptions) usize {
    var count: usize = 0;
    if (options.as_type != null) count += 1;
    if (options.start_with != null) count += 1;
    if (options.increment_by != null) count += 1;
    if (options.min_value_specified) count += 1;
    if (options.max_value_specified) count += 1;
    if (options.cache != null) count += 1;
    if (options.cycle != null) count += 1;
    if (options.owned_by != null) count += 1;
    return count;
}

fn logicalDdlPlanForFingerprintTestAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
) !binder.LogicalSqlPlan {
    var parsed_sql = try tokenized.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try logicalDdlPlanParsedSqlForFingerprintTestAlloc(alloc, &parsed_sql);
}

fn logicalDdlPlanParsedSqlForFingerprintTestAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
) !binder.LogicalSqlPlan {
    return try ddl_plan.parseLogicalDdlPlanAlloc(alloc, parsed_sql, .{});
}

test "sql adapter ddl fingerprint owns catalog-only ddl surfaces" {
    const alloc = std.testing.allocator;
    const cases = [_]struct {
        sql: []const u8,
        fingerprint: []const u8,
    }{
        .{
            .sql = "CREATE TABLE IF NOT EXISTS users_copy (LIKE users INCLUDING ALL EXCLUDING COMMENTS);",
            .fingerprint = "ddl:table_clone:table=users_copy:source=users:if_not_exists=true:columns=true:defaults=true:generated=true:checks=true:constraints=true:indexes=true:periods=true:update_policies=true",
        },
        .{
            .sql = "CREATE VIEW users_v AS SELECT id, email FROM users;",
            .fingerprint = "ddl:create_view:view=users_v:source=users:source_fields=2:fields=2:replace=false:if_not_exists=false",
        },
        .{
            .sql = "CREATE OR REPLACE VIEW users_v AS SELECT id, email FROM users;",
            .fingerprint = "ddl:create_view:view=users_v:source=users:source_fields=2:fields=2:replace=true:if_not_exists=false",
        },
        .{
            .sql = "CREATE OR REPLACE VIEW IF NOT EXISTS users_v AS SELECT id, email FROM users;",
            .fingerprint = "ddl:create_view:view=users_v:source=users:source_fields=2:fields=2:replace=true:if_not_exists=true",
        },
        .{
            .sql = "ALTER VIEW users_v RENAME TO active_users_v;",
            .fingerprint = "ddl:rename_view:view=users_v:new=active_users_v",
        },
        .{
            .sql = "DROP VIEW IF EXISTS active_users_v CASCADE;",
            .fingerprint = "ddl:drop_view:view=active_users_v:if_exists=true:cascade=true",
        },
        .{
            .sql = "DROP VIEW users_v RESTRICT;",
            .fingerprint = "ddl:drop_view:view=users_v:if_exists=false",
        },
        .{
            .sql = "CREATE MATERIALIZED VIEW IF NOT EXISTS users_mv AS SELECT id, email FROM users WITH NO DATA;",
            .fingerprint = "ddl:create_materialized_view:view=users_mv:source=users:source_fields=2:fields=2:replace=false:if_not_exists=true:populate=false",
        },
        .{
            .sql = "CREATE MATERIALIZED VIEW users_mv(user_id, contact_email) AS SELECT id, email FROM users WITH DATA;",
            .fingerprint = "ddl:create_materialized_view:view=users_mv:source=users:source_fields=2:fields=2:replace=false:if_not_exists=false:populate=true",
        },
        .{
            .sql = "CREATE OR REPLACE MATERIALIZED VIEW users_mv AS SELECT id, email FROM users WITH NO DATA;",
            .fingerprint = "ddl:create_materialized_view:view=users_mv:source=users:source_fields=2:fields=2:replace=true:if_not_exists=false:populate=false",
        },
        .{
            .sql = "CREATE OR REPLACE MATERIALIZED VIEW IF NOT EXISTS users_mv AS SELECT id, email FROM users;",
            .fingerprint = "ddl:create_materialized_view:view=users_mv:source=users:source_fields=2:fields=2:replace=true:if_not_exists=true:populate=true",
        },
        .{
            .sql = "REFRESH MATERIALIZED VIEW CONCURRENTLY users_mv WITH DATA;",
            .fingerprint = "ddl:refresh_materialized_view:view=users_mv:concurrently=true:populate=true",
        },
        .{
            .sql = "REFRESH MATERIALIZED VIEW CONCURRENTLY users_mv WITH NO DATA;",
            .fingerprint = "ddl:refresh_materialized_view:view=users_mv:concurrently=true:populate=false",
        },
        .{
            .sql = "DROP MATERIALIZED VIEW IF EXISTS users_mv RESTRICT;",
            .fingerprint = "ddl:drop_materialized_view:view=users_mv:if_exists=true",
        },
        .{
            .sql = "DROP MATERIALIZED VIEW IF EXISTS users_mv CASCADE;",
            .fingerprint = "ddl:drop_materialized_view:view=users_mv:if_exists=true:cascade=true",
        },
        .{
            .sql = "CREATE TEMPORARY TABLE users_session (id uuid PRIMARY KEY, status text);",
            .fingerprint = "ddl:relation_lifetime:kind=temporary:table=users_session:columns=2:unique=0:fk=0:checks=0:if_not_exists=false",
        },
        .{
            .sql = "CREATE TEMPORARY TABLE IF NOT EXISTS users_session (id uuid PRIMARY KEY, status text);",
            .fingerprint = "ddl:relation_lifetime:kind=temporary:table=users_session:columns=2:unique=0:fk=0:checks=0:if_not_exists=true",
        },
        .{
            .sql = "CREATE UNLOGGED TABLE users_ingest (id uuid PRIMARY KEY, payload jsonb);",
            .fingerprint = "ddl:relation_lifetime:kind=unlogged:table=users_ingest:columns=2:unique=0:fk=0:checks=0:if_not_exists=false",
        },
        .{
            .sql = "CREATE UNLOGGED TABLE IF NOT EXISTS users_ingest (id uuid PRIMARY KEY, payload jsonb);",
            .fingerprint = "ddl:relation_lifetime:kind=unlogged:table=users_ingest:columns=2:unique=0:fk=0:checks=0:if_not_exists=true",
        },
        .{
            .sql = "CREATE TABLE usage_records (id bigserial PRIMARY KEY, status text);",
            .fingerprint = "ddl:identity_allocator:table=usage_records:column=id:kind=bigserial:primary=true:columns=1",
        },
        .{
            .sql = "CREATE TABLE usage_records (id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, status text);",
            .fingerprint = "ddl:identity_allocator:table=usage_records:column=id:kind=generated_by_default:primary=true:columns=1",
        },
        .{
            .sql = "CREATE TABLE usage_records (id bigint GENERATED ALWAYS AS IDENTITY (START WITH 100 INCREMENT BY 10 NO MINVALUE NO MAXVALUE CACHE 4 NO CYCLE) PRIMARY KEY, status text);",
            .fingerprint = "ddl:identity_allocator:table=usage_records:column=id:kind=generated_always:primary=true:columns=1:options=6",
        },
        .{
            .sql = "CREATE EXTENSION postgis;",
            .fingerprint = "ddl:create_extension:extension=postgis:if_not_exists=false",
        },
        .{
            .sql = "CREATE EXTENSION postgis VERSION '3.4.0';",
            .fingerprint = "ddl:create_extension:extension=postgis:if_not_exists=false:version=3.4.0",
        },
        .{
            .sql = "CREATE EXTENSION IF NOT EXISTS postgis VERSION '3.4.0';",
            .fingerprint = "ddl:create_extension:extension=postgis:if_not_exists=true:version=3.4.0",
        },
        .{
            .sql = "ALTER EXTENSION postgis UPDATE TO '3.5.0';",
            .fingerprint = "ddl:alter_extension_update:extension=postgis:version=3.5.0",
        },
        .{
            .sql = "ALTER EXTENSION postgis UPDATE;",
            .fingerprint = "ddl:alter_extension_update:extension=postgis:version=latest",
        },
        .{
            .sql = "DROP EXTENSION IF EXISTS postgis CASCADE;",
            .fingerprint = "ddl:drop_extension:extension=postgis:if_exists=true:cascade=true",
        },
        .{
            .sql = "CREATE TYPE usage_status AS ENUM ('queued', 'processing', 'done');",
            .fingerprint = "ddl:create_enum_type:type=usage_status:values=3",
        },
        .{
            .sql = "ALTER TYPE usage_status ADD VALUE IF NOT EXISTS 'archived' AFTER 'done';",
            .fingerprint = "ddl:add_enum_value:type=usage_status:if_not_exists=true:position=after",
        },
        .{
            .sql = "DROP TYPE IF EXISTS usage_status CASCADE;",
            .fingerprint = "ddl:drop_enum_type:type=usage_status:if_exists=true:cascade=true",
        },
        .{
            .sql = "CREATE DOMAIN positive_amount AS numeric CHECK (VALUE > 0);",
            .fingerprint = "ddl:create_domain:domain=positive_amount:type=numeric:checks=1:not_null=false:default=false",
        },
        .{
            .sql = "CREATE DOMAIN status_text AS text DEFAULT 'queued' NOT NULL;",
            .fingerprint = "ddl:create_domain:domain=status_text:type=keyword:checks=0:not_null=true:default=true",
        },
        .{
            .sql = "ALTER DOMAIN positive_amount SET NOT NULL;",
            .fingerprint = "ddl:alter_domain:domain=positive_amount:ops=1",
        },
        .{
            .sql = "DROP DOMAIN IF EXISTS positive_amount CASCADE;",
            .fingerprint = "ddl:drop_domain:domain=positive_amount:if_exists=true:cascade=true",
        },
        .{
            .sql = "CREATE SEQUENCE IF NOT EXISTS users_id_seq START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 32 NO CYCLE;",
            .fingerprint = "ddl:create_sequence:sequence=users_id_seq:if_not_exists=true:options=6",
        },
        .{
            .sql = "CREATE SEQUENCE public.users_owned_id_seq AS bigint START WITH 10 OWNED BY public.users.id;",
            .fingerprint = "ddl:create_sequence:sequence=users_owned_id_seq:if_not_exists=false:options=3",
        },
        .{
            .sql = "ALTER SEQUENCE users_id_seq RESTART WITH 1000 INCREMENT BY 5;",
            .fingerprint = "ddl:alter_sequence:sequence=users_id_seq:if_exists=false:ops=2",
        },
        .{
            .sql = "ALTER SEQUENCE IF EXISTS users_owned_id_seq AS integer OWNED BY NONE;",
            .fingerprint = "ddl:alter_sequence:sequence=users_owned_id_seq:if_exists=true:ops=2",
        },
        .{
            .sql = "DROP SEQUENCE IF EXISTS users_id_seq RESTRICT;",
            .fingerprint = "ddl:drop_sequence:sequence=users_id_seq:if_exists=true",
        },
        .{
            .sql = "DROP SEQUENCE IF EXISTS users_id_seq CASCADE;",
            .fingerprint = "ddl:drop_sequence:sequence=users_id_seq:if_exists=true:cascade=true",
        },
    };

    for (cases) |case| {
        var logical = try logicalDdlPlanForFingerprintTestAlloc(alloc, case.sql);
        defer logical.deinit(alloc);
        const fingerprint = try ddlFingerprintAlloc(alloc, logical);
        defer alloc.free(fingerprint);
        try std.testing.expectEqualStrings(case.fingerprint, fingerprint);
    }
}

test "sql adapter ddl fingerprint owns routine catalog ddl surfaces" {
    const alloc = std.testing.allocator;
    const cases = [_]struct {
        sql: []const u8,
        fingerprint: []const u8,
    }{
        .{
            .sql = "CREATE FUNCTION audit_changes() RETURNS trigger LANGUAGE plpgsql;",
            .fingerprint = "ddl:create_function:name=audit_changes:args=0:replace=false:returns=trigger:language=plpgsql",
        },
        .{
            .sql = "CREATE OR REPLACE FUNCTION touch_updated_at() RETURNS trigger LANGUAGE plpgsql;",
            .fingerprint = "ddl:create_function:name=touch_updated_at:args=0:replace=true:returns=trigger:language=plpgsql",
        },
        .{
            .sql = "CREATE FUNCTION stable_audit() RETURNS trigger LANGUAGE plpgsql STABLE;",
            .fingerprint = "ddl:create_function:name=stable_audit:args=0:replace=false:returns=trigger:language=plpgsql:volatility=stable",
        },
        .{
            .sql = "CREATE FUNCTION secure_audit() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER;",
            .fingerprint = "ddl:create_function:name=secure_audit:args=0:replace=false:returns=trigger:language=plpgsql:security=definer",
        },
        .{
            .sql = "CREATE FUNCTION external_secure_audit() RETURNS trigger LANGUAGE plpgsql EXTERNAL SECURITY DEFINER;",
            .fingerprint = "ddl:create_function:name=external_secure_audit:args=0:replace=false:returns=trigger:language=plpgsql:security=definer",
        },
        .{
            .sql = "CREATE FUNCTION called_null_audit() RETURNS trigger LANGUAGE plpgsql CALLED ON NULL INPUT;",
            .fingerprint = "ddl:create_function:name=called_null_audit:args=0:replace=false:returns=trigger:language=plpgsql:null_input=called",
        },
        .{
            .sql = "CREATE FUNCTION returns_null_audit() RETURNS trigger LANGUAGE plpgsql RETURNS NULL ON NULL INPUT;",
            .fingerprint = "ddl:create_function:name=returns_null_audit:args=0:replace=false:returns=trigger:language=plpgsql:null_input=returns_null",
        },
        .{
            .sql = "CREATE FUNCTION costed_audit() RETURNS trigger LANGUAGE plpgsql COST 10;",
            .fingerprint = "ddl:create_function:name=costed_audit:args=0:replace=false:returns=trigger:language=plpgsql:cost=10",
        },
        .{
            .sql = "CREATE FUNCTION rows_audit() RETURNS trigger LANGUAGE plpgsql ROWS 10;",
            .fingerprint = "ddl:create_function:name=rows_audit:args=0:replace=false:returns=trigger:language=plpgsql:rows=10",
        },
        .{
            .sql = "CREATE FUNCTION parallel_audit() RETURNS trigger LANGUAGE plpgsql PARALLEL SAFE;",
            .fingerprint = "ddl:create_function:name=parallel_audit:args=0:replace=false:returns=trigger:language=plpgsql:parallel=safe",
        },
        .{
            .sql = "CREATE FUNCTION leakproof_audit() RETURNS trigger LANGUAGE plpgsql LEAKPROOF;",
            .fingerprint = "ddl:create_function:name=leakproof_audit:args=0:replace=false:returns=trigger:language=plpgsql:leakproof=true",
        },
        .{
            .sql = "CREATE FUNCTION window_audit() RETURNS trigger LANGUAGE plpgsql WINDOW;",
            .fingerprint = "ddl:create_function:name=window_audit:args=0:replace=false:returns=trigger:language=plpgsql:window=true",
        },
        .{
            .sql = "CREATE FUNCTION support_audit() RETURNS trigger LANGUAGE plpgsql SUPPORT audit_support;",
            .fingerprint = "ddl:create_function:name=support_audit:args=0:replace=false:returns=trigger:language=plpgsql:support=audit_support",
        },
        .{
            .sql = "CREATE FUNCTION transform_audit() RETURNS trigger LANGUAGE plpgsql TRANSFORM FOR TYPE jsonb, public.hstore;",
            .fingerprint = "ddl:create_function:name=transform_audit:args=0:replace=false:returns=trigger:language=plpgsql:transforms=2:transform=jsonb:transform=hstore",
        },
        .{
            .sql = "CREATE FUNCTION setting_audit() RETURNS trigger LANGUAGE plpgsql SET search_path TO public;",
            .fingerprint = "ddl:create_function:name=setting_audit:args=0:replace=false:returns=trigger:language=plpgsql:settings=1:setting=search_path:values=1:value=public",
        },
        .{
            .sql = "CREATE FUNCTION audit_body() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN NEW; END$$;",
            .fingerprint = "ddl:create_function:name=audit_body:args=0:replace=false:returns=trigger:language=plpgsql:body=plpgsql_trigger:hook=trigger_return_new",
        },
        .{
            .sql = "CREATE FUNCTION old_audit_body() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN OLD; END$$;",
            .fingerprint = "ddl:create_function:name=old_audit_body:args=0:replace=false:returns=trigger:language=plpgsql:body=plpgsql_trigger:hook=trigger_return_old",
        },
        .{
            .sql = "CREATE FUNCTION null_audit_body() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN NULL; END$$;",
            .fingerprint = "ddl:create_function:name=null_audit_body:args=0:replace=false:returns=trigger:language=plpgsql:body=plpgsql_trigger:hook=trigger_return_null",
        },
        .{
            .sql = "CREATE FUNCTION audit_notice_body() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE NOTICE 'audit'; RETURN NEW; END$$;",
            .fingerprint = "ddl:create_function:name=audit_notice_body:args=0:replace=false:returns=trigger:language=plpgsql:body=plpgsql_trigger:hook=trigger_return_new",
        },
        .{
            .sql = "CREATE FUNCTION audit_perform_body() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN PERFORM audit_log(); RETURN NEW; END$$;",
            .fingerprint = "ddl:create_function:name=audit_perform_body:args=0:replace=false:returns=trigger:language=plpgsql:body=plpgsql_trigger:hook=trigger_return_new:perform=1:perform=audit_log:perform_args=0",
        },
        .{
            .sql = "DROP FUNCTION IF EXISTS audit_changes();",
            .fingerprint = "ddl:drop_function:name=audit_changes:args=0:if_exists=true",
        },
        .{
            .sql = "DROP FUNCTION IF EXISTS audit_changes(text) CASCADE;",
            .fingerprint = "ddl:drop_function:name=audit_changes:args=1:if_exists=true:cascade=true",
        },
        .{
            .sql = "CREATE PROCEDURE rotate_usage() LANGUAGE plpgsql;",
            .fingerprint = "ddl:create_procedure:name=rotate_usage:args=0:replace=false:returns=:language=plpgsql",
        },
        .{
            .sql = "CREATE PROCEDURE rotate_usage() LANGUAGE plpgsql AS $$BEGIN NULL; END$$;",
            .fingerprint = "ddl:create_procedure:name=rotate_usage:args=0:replace=false:returns=:language=plpgsql:body=plpgsql_procedure:hook=procedure_noop",
        },
        .{
            .sql = "CREATE PROCEDURE rotate_usage_notice() LANGUAGE plpgsql AS $$BEGIN RAISE NOTICE 'rotate'; END$$;",
            .fingerprint = "ddl:create_procedure:name=rotate_usage_notice:args=0:replace=false:returns=:language=plpgsql:body=plpgsql_procedure:hook=procedure_noop",
        },
        .{
            .sql = "CREATE PROCEDURE rotate_usage_perform() LANGUAGE plpgsql AS $$BEGIN PERFORM rotate_usage_now(); END$$;",
            .fingerprint = "ddl:create_procedure:name=rotate_usage_perform:args=0:replace=false:returns=:language=plpgsql:body=plpgsql_procedure:hook=procedure_noop:perform=1:perform=rotate_usage_now:perform_args=0",
        },
        .{
            .sql = "CREATE PROCEDURE rotate_usage_perform_arg() LANGUAGE plpgsql AS $$BEGIN PERFORM rotate_usage_now(1); END$$;",
            .fingerprint = "ddl:create_procedure:name=rotate_usage_perform_arg:args=0:replace=false:returns=:language=plpgsql:body=plpgsql_procedure:hook=procedure_noop:perform=1:perform=rotate_usage_now:perform_args=1:arg=1",
        },
        .{
            .sql = "CALL rotate_usage();",
            .fingerprint = "ddl:call_procedure:name=rotate_usage:args=0",
        },
        .{
            .sql = "DROP PROCEDURE rotate_usage();",
            .fingerprint = "ddl:drop_procedure:name=rotate_usage:args=0:if_exists=false",
        },
        .{
            .sql = "DROP PROCEDURE IF EXISTS rotate_usage(text) CASCADE;",
            .fingerprint = "ddl:drop_procedure:name=rotate_usage:args=1:if_exists=true:cascade=true",
        },
    };

    for (cases) |case| {
        var logical = try logicalDdlPlanForFingerprintTestAlloc(alloc, case.sql);
        defer logical.deinit(alloc);
        const fingerprint = try ddlFingerprintAlloc(alloc, logical);
        defer alloc.free(fingerprint);
        try std.testing.expectEqualStrings(case.fingerprint, fingerprint);
    }

    try std.testing.expectError(error.UnsupportedSqlShape, logicalDdlPlanForFingerprintTestAlloc(alloc, "CREATE FUNCTION stable_audit() RETURNS trigger LANGUAGE plpgsql SUPPORT audit_support SUPPORT audit_support;"));
    try std.testing.expectError(error.UnsupportedSqlShape, logicalDdlPlanForFingerprintTestAlloc(alloc, "CALL rotate_usage(1);"));
}

test "sql adapter ddl fingerprint owns authorization catalog ddl surfaces" {
    const alloc = std.testing.allocator;
    const cases = [_]struct {
        sql: []const u8,
        fingerprint: []const u8,
    }{
        .{
            .sql = "GRANT SELECT, INSERT ON TABLE usage_records TO app_writer;",
            .fingerprint = "ddl:grant_privilege:object=TABLE:usage_records:principal=app_writer:privileges=2",
        },
        .{
            .sql = "GRANT ALL PRIVILEGES ON TABLE usage_records TO app_writer;",
            .fingerprint = "ddl:grant_privilege:object=TABLE:usage_records:principal=app_writer:privileges=1",
        },
        .{
            .sql = "GRANT SELECT ON ALL TABLES IN SCHEMA public TO app_writer;",
            .fingerprint = "ddl:grant_privilege:object=ALL_TABLES_IN_SCHEMA:public:principal=app_writer:privileges=1",
        },
        .{
            .sql = "REVOKE INSERT ON TABLE usage_records FROM app_writer;",
            .fingerprint = "ddl:revoke_privilege:object=TABLE:usage_records:principal=app_writer:privileges=1",
        },
        .{
            .sql = "CREATE ROLE app_writer;",
            .fingerprint = "ddl:create_role:role=app_writer",
        },
        .{
            .sql = "ALTER ROLE app_writer SET app.tenant_id = 'acme';",
            .fingerprint = "ddl:alter_role:role=app_writer:operation=set:setting=app.tenant_id",
        },
        .{
            .sql = "ALTER ROLE app_writer IN DATABASE appdb SET app.tenant_id = 'acme';",
            .fingerprint = "ddl:alter_role:role=app_writer:database=appdb:operation=set:setting=app.tenant_id",
        },
        .{
            .sql = "ALTER ROLE app_writer RESET app.tenant_id;",
            .fingerprint = "ddl:alter_role:role=app_writer:operation=reset:setting=app.tenant_id",
        },
        .{
            .sql = "ALTER ROLE app_writer IN DATABASE appdb RESET app.tenant_id;",
            .fingerprint = "ddl:alter_role:role=app_writer:database=appdb:operation=reset:setting=app.tenant_id",
        },
        .{
            .sql = "ALTER ROLE app_writer SET statement_timeout = '1ms';",
            .fingerprint = "ddl:alter_role:role=app_writer:operation=set:setting=statement_timeout:setting_kind=runtime",
        },
        .{
            .sql = "ALTER ROLE app_writer IN DATABASE appdb SET statement_timeout = '1ms';",
            .fingerprint = "ddl:alter_role:role=app_writer:database=appdb:operation=set:setting=statement_timeout:setting_kind=runtime",
        },
        .{
            .sql = "ALTER ROLE app_writer RESET statement_timeout;",
            .fingerprint = "ddl:alter_role:role=app_writer:operation=reset:setting=statement_timeout:setting_kind=runtime",
        },
        .{
            .sql = "ALTER ROLE app_writer SET app.tenant_id = current_setting('app.tenant_id');",
            .fingerprint = "ddl:alter_role:role=app_writer:operation=set:setting=app.tenant_id:value_source=current_setting",
        },
        .{
            .sql = "DROP ROLE IF EXISTS app_writer;",
            .fingerprint = "ddl:drop_role:role=app_writer:if_exists=true",
        },
    };

    for (cases) |case| {
        var logical = try logicalDdlPlanForFingerprintTestAlloc(alloc, case.sql);
        defer logical.deinit(alloc);
        const fingerprint = try ddlFingerprintAlloc(alloc, logical);
        defer alloc.free(fingerprint);
        try std.testing.expectEqualStrings(case.fingerprint, fingerprint);
    }
}

test "sql adapter ddl fingerprint owns partition and row security catalog ddl surfaces" {
    const alloc = std.testing.allocator;
    const cases = [_]struct {
        sql: []const u8,
        fingerprint: []const u8,
    }{
        .{
            .sql = "CREATE TABLE usage_events (tenant_id text, id uuid, created_at timestamptz, PRIMARY KEY (tenant_id, id)) PARTITION BY RANGE (created_at);",
            .fingerprint = "ddl:create_partitioned_table:table=usage_events:columns=3:method=range:keys=1",
        },
        .{
            .sql = "CREATE TABLE usage_events_2026 PARTITION OF usage_events FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');",
            .fingerprint = "ddl:create_table_partition:table=usage_events_2026:parent=usage_events:lower=\"2026-01-01\":upper=\"2027-01-01\"",
        },
        .{
            .sql = "ALTER TABLE usage_events ATTACH PARTITION usage_events_2026 FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');",
            .fingerprint = "ddl:attach_table_partition:parent=usage_events:partition=usage_events_2026:lower=\"2026-01-01\":upper=\"2027-01-01\"",
        },
        .{
            .sql = "ALTER TABLE usage_events DETACH PARTITION usage_events_2026;",
            .fingerprint = "ddl:detach_table_partition:parent=usage_events:partition=usage_events_2026",
        },
        .{
            .sql = "ALTER TABLE usage_records ENABLE ROW LEVEL SECURITY;",
            .fingerprint = "ddl:enable_row_security:table=usage_records",
        },
        .{
            .sql = "ALTER TABLE usage_records DISABLE ROW LEVEL SECURITY;",
            .fingerprint = "ddl:disable_row_security:table=usage_records",
        },
        .{
            .sql = "CREATE POLICY usage_records_tenant_policy ON usage_records USING (tenant_id = current_setting('app.tenant_id'));",
            .fingerprint = "ddl:create_row_policy:policy=usage_records_tenant_policy:table=usage_records:kind=current_setting_eq:field=tenant_id:setting=app.tenant_id",
        },
        .{
            .sql = "CREATE POLICY usage_records_targeted_policy ON usage_records TO app_reader, app_writer USING (tenant_id = current_setting('app.tenant_id'));",
            .fingerprint = "ddl:create_row_policy:policy=usage_records_targeted_policy:table=usage_records:kind=current_setting_eq:field=tenant_id:setting=app.tenant_id:roles=2:role=app_reader:role=app_writer",
        },
        .{
            .sql = "CREATE POLICY usage_records_active_policy ON usage_records USING (status = 'active');",
            .fingerprint = "ddl:create_row_policy:policy=usage_records_active_policy:table=usage_records:kind=literal_eq:field=status:value_json_hex=2261637469766522",
        },
        .{
            .sql = "CREATE POLICY usage_records_write_policy ON usage_records USING (tenant_id = 'tenant-a') WITH CHECK (status = 'active');",
            .fingerprint = "ddl:create_row_policy:policy=usage_records_write_policy:table=usage_records:kind=literal_eq:field=tenant_id:value_json_hex=2274656e616e742d6122:check=kind=literal_eq:field=status:value_json_hex=2261637469766522",
        },
        .{
            .sql = "CREATE POLICY usage_records_compound_policy ON usage_records USING (tenant_id = 'tenant-a' AND status = 'active');",
            .fingerprint = "ddl:create_row_policy:policy=usage_records_compound_policy:table=usage_records:kind=and:terms=2:term=kind=literal_eq:field=tenant_id:value_json_hex=2274656e616e742d6122:term=kind=literal_eq:field=status:value_json_hex=2261637469766522",
        },
        .{
            .sql = "CREATE POLICY usage_records_or_policy ON usage_records USING (tenant_id = 'tenant-a' OR status = 'active');",
            .fingerprint = "ddl:create_row_policy:policy=usage_records_or_policy:table=usage_records:kind=or:terms=2:term=kind=literal_eq:field=tenant_id:value_json_hex=2274656e616e742d6122:term=kind=literal_eq:field=status:value_json_hex=2261637469766522",
        },
        .{
            .sql = "CREATE POLICY usage_records_mixed_policy ON usage_records USING (tenant_id = 'tenant-a' OR status = 'active' AND region = 'us');",
            .fingerprint = "ddl:create_row_policy:policy=usage_records_mixed_policy:table=usage_records:kind=or:terms=2:term=kind=literal_eq:field=tenant_id:value_json_hex=2274656e616e742d6122:term=kind=and:terms=2:term=kind=literal_eq:field=status:value_json_hex=2261637469766522:term=kind=literal_eq:field=region:value_json_hex=22757322",
        },
        .{
            .sql = "ALTER POLICY usage_records_tenant_policy ON usage_records USING (status = 'active');",
            .fingerprint = "ddl:alter_row_policy:policy=usage_records_tenant_policy:table=usage_records:kind=literal_eq:field=status:value_json_hex=2261637469766522",
        },
        .{
            .sql = "ALTER POLICY usage_records_tenant_policy ON usage_records TO app_writer;",
            .fingerprint = "ddl:alter_row_policy:policy=usage_records_tenant_policy:table=usage_records:roles=1:role=app_writer",
        },
        .{
            .sql = "ALTER POLICY usage_records_tenant_policy ON usage_records WITH CHECK (status = 'ready');",
            .fingerprint = "ddl:alter_row_policy:policy=usage_records_tenant_policy:table=usage_records:check=kind=literal_eq:field=status:value_json_hex=22726561647922",
        },
        .{
            .sql = "DROP POLICY usage_records_tenant_policy ON usage_records;",
            .fingerprint = "ddl:drop_row_policy:policy=usage_records_tenant_policy:table=usage_records:if_exists=false",
        },
    };

    for (cases) |case| {
        var logical = try logicalDdlPlanForFingerprintTestAlloc(alloc, case.sql);
        defer logical.deinit(alloc);
        const fingerprint = try ddlFingerprintAlloc(alloc, logical);
        defer alloc.free(fingerprint);
        try std.testing.expectEqualStrings(case.fingerprint, fingerprint);
    }

    var expression_policy = try logicalDdlPlanForFingerprintTestAlloc(alloc, "CREATE POLICY usage_records_lower_policy ON usage_records USING (lower(status) = 'active');");
    defer expression_policy.deinit(alloc);
    const expression_fingerprint = try ddlFingerprintAlloc(alloc, expression_policy);
    defer alloc.free(expression_fingerprint);
    try std.testing.expect(std.mem.indexOf(u8, expression_fingerprint, "ddl:create_row_policy:policy=usage_records_lower_policy:table=usage_records:kind=expression:json_hex=") != null);
}

test "sql adapter ddl fingerprint owns namespace database and tablespace catalog ddl surfaces" {
    const alloc = std.testing.allocator;
    const cases = [_]struct {
        sql: []const u8,
        fingerprint: []const u8,
    }{
        .{
            .sql = "CREATE SCHEMA IF NOT EXISTS tenant_ops;",
            .fingerprint = "ddl:create_schema_namespace:schema=tenant_ops:if_not_exists=true",
        },
        .{
            .sql = "ALTER SCHEMA tenant_ops RENAME TO tenant_ops_archive;",
            .fingerprint = "ddl:rename_schema_namespace:schema=tenant_ops:new=tenant_ops_archive",
        },
        .{
            .sql = "DROP SCHEMA IF EXISTS tenant_ops CASCADE;",
            .fingerprint = "ddl:drop_schema_namespace:schema=tenant_ops:if_exists=true:cascade=true",
        },
        .{
            .sql = "CREATE DATABASE tenant_ops;",
            .fingerprint = "ddl:create_database:database=tenant_ops",
        },
        .{
            .sql = "ALTER DATABASE tenant_ops SET timezone TO 'UTC';",
            .fingerprint = "ddl:alter_database:database=tenant_ops:ops=1",
        },
        .{
            .sql = "DROP DATABASE IF EXISTS tenant_ops WITH (FORCE);",
            .fingerprint = "ddl:drop_database:database=tenant_ops:if_exists=true:force=true",
        },
        .{
            .sql = "CREATE TABLESPACE fastspace LOCATION '/var/lib/antfly/fastspace';",
            .fingerprint = "ddl:create_tablespace:tablespace=fastspace:location=true",
        },
        .{
            .sql = "ALTER TABLESPACE fastspace RENAME TO fastspace_archive;",
            .fingerprint = "ddl:rename_tablespace:tablespace=fastspace:new=fastspace_archive",
        },
        .{
            .sql = "DROP TABLESPACE IF EXISTS fastspace_archive;",
            .fingerprint = "ddl:drop_tablespace:tablespace=fastspace_archive:if_exists=true",
        },
    };

    for (cases) |case| {
        var logical = try logicalDdlPlanForFingerprintTestAlloc(alloc, case.sql);
        defer logical.deinit(alloc);
        const fingerprint = try ddlFingerprintAlloc(alloc, logical);
        defer alloc.free(fingerprint);
        try std.testing.expectEqualStrings(case.fingerprint, fingerprint);
    }
}

test "sql adapter ddl fingerprint owns notification and logical replication ddl surfaces" {
    const alloc = std.testing.allocator;
    const cases = [_]struct {
        sql: []const u8,
        fingerprint: []const u8,
    }{
        .{
            .sql = "LISTEN usage_events;",
            .fingerprint = "ddl:listen_notification:channel=usage_events",
        },
        .{
            .sql = "NOTIFY usage_events, 'updated';",
            .fingerprint = "ddl:notify_notification:channel=usage_events:payload=true",
        },
        .{
            .sql = "UNLISTEN usage_events;",
            .fingerprint = "ddl:unlisten_notification:channel=usage_events",
        },
        .{
            .sql = "UNLISTEN *;",
            .fingerprint = "ddl:unlisten_notification:all=true",
        },
        .{
            .sql = "CREATE PUBLICATION usage_pub FOR TABLE usage_records;",
            .fingerprint = "ddl:create_publication:publication=usage_pub:tables=1:all=false",
        },
        .{
            .sql = "ALTER PUBLICATION usage_pub ADD TABLE usage_events;",
            .fingerprint = "ddl:alter_publication:publication=usage_pub:add_tables=1",
        },
        .{
            .sql = "DROP PUBLICATION IF EXISTS usage_pub;",
            .fingerprint = "ddl:drop_publication:publication=usage_pub:if_exists=true",
        },
        .{
            .sql = "CREATE SUBSCRIPTION usage_sub CONNECTION 'host=localhost dbname=usage' PUBLICATION usage_pub;",
            .fingerprint = "ddl:create_subscription:subscription=usage_sub:connection=true:publications=1",
        },
        .{
            .sql = "ALTER SUBSCRIPTION usage_sub DISABLE;",
            .fingerprint = "ddl:alter_subscription:subscription=usage_sub:enabled=false",
        },
        .{
            .sql = "DROP SUBSCRIPTION IF EXISTS usage_sub;",
            .fingerprint = "ddl:drop_subscription:subscription=usage_sub:if_exists=true",
        },
    };

    for (cases) |case| {
        var logical = try logicalDdlPlanForFingerprintTestAlloc(alloc, case.sql);
        defer logical.deinit(alloc);
        const fingerprint = try ddlFingerprintAlloc(alloc, logical);
        defer alloc.free(fingerprint);
        try std.testing.expectEqualStrings(case.fingerprint, fingerprint);
    }
}

test "sql adapter ddl fingerprint owns type system ddl surfaces" {
    const alloc = std.testing.allocator;
    const cases = [_]struct {
        sql: []const u8,
        fingerprint: []const u8,
    }{
        .{
            .sql = "CREATE COLLATION case_insensitive (provider = icu, locale = 'und-u-ks-level2');",
            .fingerprint = "ddl:create_collation:collation=case_insensitive:options=2",
        },
        .{
            .sql = "ALTER COLLATION case_insensitive RENAME TO ci_text;",
            .fingerprint = "ddl:rename_collation:collation=case_insensitive:new=ci_text",
        },
        .{
            .sql = "DROP COLLATION IF EXISTS ci_text;",
            .fingerprint = "ddl:drop_collation:collation=ci_text:if_exists=true",
        },
        .{
            .sql = "CREATE OPERATOR === (FUNCTION = text_eq, LEFTARG = text, RIGHTARG = text);",
            .fingerprint = "ddl:create_operator:operator====:options=3",
        },
        .{
            .sql = "DROP OPERATOR === (text, text);",
            .fingerprint = "ddl:drop_operator:operator====:args=2:if_exists=false",
        },
        .{
            .sql = "DROP OPERATOR IF EXISTS === (text, text);",
            .fingerprint = "ddl:drop_operator:operator====:args=2:if_exists=true",
        },
        .{
            .sql = "CREATE AGGREGATE first_value_text(text) (SFUNC = first_sfunc, STYPE = text);",
            .fingerprint = "ddl:create_aggregate:aggregate=first_value_text:args=1:options=2",
        },
        .{
            .sql = "DROP AGGREGATE first_value_text(text);",
            .fingerprint = "ddl:drop_aggregate:aggregate=first_value_text:args=1:if_exists=false",
        },
        .{
            .sql = "DROP AGGREGATE IF EXISTS first_value_text(text);",
            .fingerprint = "ddl:drop_aggregate:aggregate=first_value_text:args=1:if_exists=true",
        },
        .{
            .sql = "CREATE CAST (jsonb AS text) WITH FUNCTION jsonb_to_text(jsonb) AS ASSIGNMENT;",
            .fingerprint = "ddl:create_cast:source=jsonb:target=text:function=jsonb_to_text:assignment=true",
        },
        .{
            .sql = "DROP CAST (jsonb AS text);",
            .fingerprint = "ddl:drop_cast:source=jsonb:target=text:if_exists=false",
        },
        .{
            .sql = "DROP CAST IF EXISTS (jsonb AS text);",
            .fingerprint = "ddl:drop_cast:source=jsonb:target=text:if_exists=true",
        },
    };

    for (cases) |case| {
        var logical = try logicalDdlPlanForFingerprintTestAlloc(alloc, case.sql);
        defer logical.deinit(alloc);
        const fingerprint = try ddlFingerprintAlloc(alloc, logical);
        defer alloc.free(fingerprint);
        try std.testing.expectEqualStrings(case.fingerprint, fingerprint);
    }
}

test "sql adapter ddl fingerprint owns maintenance job ddl surfaces" {
    const alloc = std.testing.allocator;
    const cases = [_]struct {
        sql: []const u8,
        fingerprint: []const u8,
    }{
        .{
            .sql = "VACUUM usage_records;",
            .fingerprint = "ddl:maintenance:kind=vacuum:table=usage_records:full=false:freeze=false:verbose=false:analyze=false",
        },
        .{
            .sql = "ANALYZE usage_records;",
            .fingerprint = "ddl:maintenance:kind=analyze:table=usage_records:verbose=false:columns=0",
        },
        .{
            .sql = "REINDEX TABLE usage_records;",
            .fingerprint = "ddl:maintenance:kind=reindex:target=table:name=usage_records:concurrently=false",
        },
        .{
            .sql = "CLUSTER usage_records USING usage_records_status_idx;",
            .fingerprint = "ddl:maintenance:kind=cluster:table=usage_records:index=usage_records_status_idx:verbose=false",
        },
    };

    for (cases) |case| {
        var logical = try logicalDdlPlanForFingerprintTestAlloc(alloc, case.sql);
        defer logical.deinit(alloc);
        const fingerprint = try ddlFingerprintAlloc(alloc, logical);
        defer alloc.free(fingerprint);
        try std.testing.expectEqualStrings(case.fingerprint, fingerprint);
    }
}

test "sql adapter ddl fingerprint owns bulk io ddl surfaces" {
    const alloc = std.testing.allocator;
    const cases = [_]struct {
        sql: []const u8,
        fingerprint: []const u8,
    }{
        .{
            .sql = "COPY usage_records (id, status) FROM STDIN WITH (FORMAT csv);",
            .fingerprint = "ddl:copy_from:table=usage_records:columns=2:endpoint=STDIN:format=csv:header=false:freeze=false:on_error=stop:reject_limit=none:log_verbosity=default:force_quote=none:force_quote_columns=0:force_not_null_columns=0:force_null_columns=0:delimiter_hex=default:quote_hex=default:escape_hex=default:null_marker_hex=default:default_marker_hex=default:encoding_hex=default:where_expressions=0",
        },
        .{
            .sql = "COPY usage_records (id, status) FROM STDIN WITH (FORMAT csv, HEADER true, FREEZE true, ON_ERROR ignore, REJECT_LIMIT 10, LOG_VERBOSITY verbose, FORCE_NOT_NULL (id, status), FORCE_NULL (status), DELIMITER ',', QUOTE '\"', ESCAPE '!', NULL '', DEFAULT 'n/a', ENCODING 'UTF8');",
            .fingerprint = "ddl:copy_from:table=usage_records:columns=2:endpoint=STDIN:format=csv:header=true:freeze=true:on_error=ignore:reject_limit=10:log_verbosity=verbose:force_quote=none:force_quote_columns=0:force_not_null_columns=2:force_null_columns=1:delimiter_hex=2c:quote_hex=22:escape_hex=21:null_marker_hex=empty:default_marker_hex=6e2f61:encoding_hex=55544638:where_expressions=0",
        },
        .{
            .sql = "COPY usage_records (id, status) FROM STDIN WITH (FORMAT csv) WHERE status = 'active';",
            .fingerprint = "ddl:copy_from:table=usage_records:columns=2:endpoint=STDIN:format=csv:header=false:freeze=false:on_error=stop:reject_limit=none:log_verbosity=default:force_quote=none:force_quote_columns=0:force_not_null_columns=0:force_null_columns=0:delimiter_hex=default:quote_hex=default:escape_hex=default:null_marker_hex=default:default_marker_hex=default:encoding_hex=default:where_expressions=1",
        },
        .{
            .sql = "COPY usage_records (id, status) TO STDOUT WITH (FORMAT csv, FORCE_QUOTE *);",
            .fingerprint = "ddl:copy_to:table=usage_records:columns=2:endpoint=STDOUT:format=csv:header=false:freeze=false:on_error=stop:reject_limit=none:log_verbosity=default:force_quote=all:force_quote_columns=0:force_not_null_columns=0:force_null_columns=0:delimiter_hex=default:quote_hex=default:escape_hex=default:null_marker_hex=default:default_marker_hex=default:encoding_hex=default:where_expressions=0",
        },
    };

    for (cases) |case| {
        var logical = try logicalDdlPlanForFingerprintTestAlloc(alloc, case.sql);
        defer logical.deinit(alloc);
        const fingerprint = try ddlFingerprintAlloc(alloc, logical);
        defer alloc.free(fingerprint);
        try std.testing.expectEqualStrings(case.fingerprint, fingerprint);
    }
}

test "sql adapter ddl fingerprint owns session catalog ddl surfaces" {
    const alloc = std.testing.allocator;
    const cases = [_]struct {
        sql: []const u8,
        fingerprint: []const u8,
    }{
        .{ .sql = "SET LOCAL client_min_messages = warning;", .fingerprint = "adapter_noop:ddl:reason=session_setting" },
        .{ .sql = "SET client_encoding = 'UTF8';", .fingerprint = "adapter_noop:ddl:reason=session_setting" },
        .{ .sql = "SET search_path TO public;", .fingerprint = "ddl:session:set_search_path:namespaces=1:local=false" },
        .{ .sql = "SET search_path TO tenant_schema, public;", .fingerprint = "ddl:session:set_search_path:namespaces=2:local=false" },
        .{ .sql = "SET LOCAL search_path TO public;", .fingerprint = "ddl:session:set_search_path:namespaces=1:local=true" },
        .{ .sql = "SET LOCAL search_path TO tenant_schema, public;", .fingerprint = "ddl:session:set_search_path:namespaces=2:local=true" },
        .{ .sql = "SET app.tenant_id = 'tenant-a';", .fingerprint = "ddl:session:set_setting:setting=app.tenant_id:setting_kind=app:local=false" },
        .{ .sql = "SET LOCAL app.tenant_id = 'tenant-b';", .fingerprint = "ddl:session:set_setting:setting=app.tenant_id:setting_kind=app:local=true" },
        .{ .sql = "SET antfly.sync_level = 'full_index';", .fingerprint = "ddl:session:set_setting:setting=antfly.sync_level:setting_kind=antfly:local=false" },
        .{ .sql = "SET LOCAL antfly.sync_level = 'propose';", .fingerprint = "ddl:session:set_setting:setting=antfly.sync_level:setting_kind=antfly:local=true" },
        .{ .sql = "SET statement_timeout = '1ms';", .fingerprint = "ddl:session:set_setting:setting=statement_timeout:setting_kind=runtime:local=false" },
        .{ .sql = "RESET app.tenant_id;", .fingerprint = "ddl:session:reset_setting:setting=app.tenant_id:setting_kind=app" },
        .{ .sql = "RESET ALL;", .fingerprint = "ddl:session:discard_all" },
        .{ .sql = "RESET search_path;", .fingerprint = "ddl:session:reset_search_path" },
        .{ .sql = "RESET client_min_messages;", .fingerprint = "adapter_noop:ddl:reason=session_setting" },
        .{ .sql = "SHOW search_path;", .fingerprint = "ddl:session:show_search_path" },
        .{ .sql = "DISCARD ALL;", .fingerprint = "ddl:session:discard_all" },
    };

    for (cases) |case| {
        var logical = try logicalDdlPlanForFingerprintTestAlloc(alloc, case.sql);
        defer logical.deinit(alloc);
        const fingerprint = try ddlFingerprintAlloc(alloc, logical);
        defer alloc.free(fingerprint);
        try std.testing.expectEqualStrings(case.fingerprint, fingerprint);
    }
}

test "sql adapter ddl fingerprint owns transaction protocol ddl surfaces" {
    const alloc = std.testing.allocator;
    const cases = [_]struct {
        sql: []const u8,
        fingerprint: []const u8,
    }{
        .{
            .sql = "LOCK TABLE usage_records IN ACCESS EXCLUSIVE MODE;",
            .fingerprint = "ddl:transaction_control:kind=table_lock:tables=1:mode=access_exclusive",
        },
        .{
            .sql = "SET CONSTRAINTS ALL DEFERRED;",
            .fingerprint = "ddl:transaction_control:kind=constraint_mode:all=true:constraints=0:mode=deferred",
        },
        .{
            .sql = "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY;",
            .fingerprint = "ddl:transaction_control:kind=transaction_mode:starter=set_transaction:isolation=serializable:access=read_only:deferrable=none",
        },
        .{
            .sql = "START TRANSACTION ISOLATION LEVEL REPEATABLE READ;",
            .fingerprint = "ddl:transaction_control:kind=transaction_mode:starter=start_transaction:isolation=repeatable_read:access=none:deferrable=none",
        },
        .{
            .sql = "BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE;",
            .fingerprint = "ddl:transaction_control:kind=transaction_mode:starter=begin:isolation=serializable:access=read_write:deferrable=none",
        },
        .{
            .sql = "BEGIN;",
            .fingerprint = "adapter_noop:ddl:reason=transaction_control",
        },
        .{
            .sql = "BEGIN WORK;",
            .fingerprint = "adapter_noop:ddl:reason=transaction_control",
        },
        .{
            .sql = "START TRANSACTION;",
            .fingerprint = "adapter_noop:ddl:reason=transaction_control",
        },
        .{
            .sql = "COMMIT;",
            .fingerprint = "adapter_noop:ddl:reason=transaction_control",
        },
        .{
            .sql = "COMMIT TRANSACTION;",
            .fingerprint = "adapter_noop:ddl:reason=transaction_control",
        },
        .{
            .sql = "ROLLBACK;",
            .fingerprint = "adapter_noop:ddl:reason=transaction_control",
        },
        .{
            .sql = "ROLLBACK WORK;",
            .fingerprint = "adapter_noop:ddl:reason=transaction_control",
        },
        .{
            .sql = "SELECT pg_advisory_lock(42);",
            .fingerprint = "ddl:transaction_control:kind=advisory_lock:action=lock:keys=1",
        },
        .{
            .sql = "SELECT pg_advisory_unlock(42);",
            .fingerprint = "ddl:transaction_control:kind=advisory_lock:action=unlock:keys=1",
        },
    };

    for (cases) |case| {
        var logical = try logicalDdlPlanForFingerprintTestAlloc(alloc, case.sql);
        defer logical.deinit(alloc);
        const fingerprint = try ddlFingerprintAlloc(alloc, logical);
        defer alloc.free(fingerprint);
        try std.testing.expectEqualStrings(case.fingerprint, fingerprint);
    }
}

test "sql adapter ddl fingerprint owns prepared transaction ddl surfaces" {
    const alloc = std.testing.allocator;
    const cases = [_]struct {
        sql: []const u8,
        fingerprint: []const u8,
    }{
        .{
            .sql = "PREPARE TRANSACTION 'usage_batch';",
            .fingerprint = "ddl:prepared_transaction:action=prepare:gid=usage_batch",
        },
        .{
            .sql = "COMMIT PREPARED 'usage_batch';",
            .fingerprint = "ddl:prepared_transaction:action=commit:gid=usage_batch",
        },
        .{
            .sql = "ROLLBACK PREPARED 'usage_batch';",
            .fingerprint = "ddl:prepared_transaction:action=rollback:gid=usage_batch",
        },
    };

    for (cases) |case| {
        var logical = try logicalDdlPlanForFingerprintTestAlloc(alloc, case.sql);
        defer logical.deinit(alloc);
        const fingerprint = try ddlFingerprintAlloc(alloc, logical);
        defer alloc.free(fingerprint);
        try std.testing.expectEqualStrings(case.fingerprint, fingerprint);
    }
}

test "sql adapter ddl fingerprint owns prepared statement cursor and savepoint ddl surfaces" {
    const alloc = std.testing.allocator;
    const cases = [_]struct {
        sql: []const u8,
        fingerprint: []const u8,
    }{
        .{
            .sql = "PREPARE usage_plan(text) AS SELECT id FROM usage_records WHERE status = $1;",
            .fingerprint = "ddl:prepare_statement:name=usage_plan:params=1:subject=read:statement=read",
        },
        .{
            .sql = "PREPARE recursive_usage_read_plan AS WITH RECURSIVE source_rows AS (SELECT id FROM usage_records UNION ALL SELECT child.id FROM usage_records AS child JOIN source_rows AS parent ON child.organization_id = parent.id) SELECT id FROM source_rows;",
            .fingerprint = "ddl:prepare_statement:name=recursive_usage_read_plan:params=0:subject=read:statement=read",
        },
        .{
            .sql = "PREPARE merge_plan AS MERGE INTO usage_records USING source_records ON usage_records.id = source_records.id WHEN MATCHED THEN UPDATE SET status = source_records.status;",
            .fingerprint = "ddl:prepare_statement:name=merge_plan:params=0:subject=write:statement=merge",
        },
        .{
            .sql = "PREPARE cte_write_plan AS WITH source_rows AS (SELECT id FROM usage_records) UPDATE usage_records SET status = 'done' WHERE id IN (SELECT id FROM source_rows);",
            .fingerprint = "ddl:prepare_statement:name=cte_write_plan:params=0:subject=write:statement=update",
        },
        .{
            .sql = "PREPARE recursive_usage_plan AS WITH RECURSIVE source_rows AS (SELECT id FROM usage_records UNION ALL SELECT child.id FROM usage_records AS child JOIN source_rows AS parent ON child.organization_id = parent.id) UPDATE usage_records SET status = 'done' WHERE id IN (SELECT id FROM source_rows);",
            .fingerprint = "ddl:prepare_statement:name=recursive_usage_plan:params=0:subject=write:statement=update",
        },
        .{
            .sql = "EXECUTE usage_plan('open');",
            .fingerprint = "ddl:execute_statement:name=usage_plan:args=1",
        },
        .{
            .sql = "DEALLOCATE usage_plan;",
            .fingerprint = "ddl:deallocate_statement:name=usage_plan",
        },
        .{
            .sql = "DEALLOCATE PREPARE usage_plan;",
            .fingerprint = "ddl:deallocate_statement:name=usage_plan",
        },
        .{
            .sql = "DEALLOCATE ALL;",
            .fingerprint = "ddl:deallocate_statement:all=true",
        },
        .{
            .sql = "DECLARE usage_cursor CURSOR FOR SELECT id FROM usage_records ORDER BY id;",
            .fingerprint = "ddl:declare_cursor:portal=usage_cursor:scroll=default:binary=false:hold=false:subject=read",
        },
        .{
            .sql = "DECLARE usage_scroll_cursor BINARY SCROLL CURSOR WITH HOLD FOR SELECT id FROM usage_records ORDER BY id;",
            .fingerprint = "ddl:declare_cursor:portal=usage_scroll_cursor:scroll=scroll:binary=true:hold=true:subject=read",
        },
        .{
            .sql = "DECLARE merge_cursor CURSOR FOR MERGE INTO usage_records USING source_records ON usage_records.id = source_records.id WHEN MATCHED THEN UPDATE SET status = source_records.status;",
            .fingerprint = "ddl:declare_cursor:portal=merge_cursor:scroll=default:binary=false:hold=false:subject=write",
        },
        .{
            .sql = "FETCH NEXT FROM usage_cursor;",
            .fingerprint = "ddl:fetch_cursor:portal=usage_cursor:direction=next",
        },
        .{
            .sql = "FETCH FORWARD 10 IN usage_cursor;",
            .fingerprint = "ddl:fetch_cursor:portal=usage_cursor:direction=forward:count=10",
        },
        .{
            .sql = "FETCH usage_cursor;",
            .fingerprint = "ddl:fetch_cursor:portal=usage_cursor:direction=next",
        },
        .{
            .sql = "FETCH 10 usage_cursor;",
            .fingerprint = "ddl:fetch_cursor:portal=usage_cursor:direction=forward:count=10",
        },
        .{
            .sql = "FETCH FORWARD usage_cursor;",
            .fingerprint = "ddl:fetch_cursor:portal=usage_cursor:direction=forward",
        },
        .{
            .sql = "CLOSE usage_cursor;",
            .fingerprint = "ddl:close_cursor:portal=usage_cursor",
        },
        .{
            .sql = "CLOSE ALL;",
            .fingerprint = "ddl:close_cursor:all=true",
        },
        .{
            .sql = "SAVEPOINT before_retry;",
            .fingerprint = "ddl:savepoint:name=before_retry",
        },
        .{
            .sql = "RELEASE SAVEPOINT before_retry;",
            .fingerprint = "ddl:release_savepoint:name=before_retry",
        },
        .{
            .sql = "RELEASE before_retry;",
            .fingerprint = "ddl:release_savepoint:name=before_retry",
        },
        .{
            .sql = "ROLLBACK TO SAVEPOINT before_retry;",
            .fingerprint = "ddl:rollback_to_savepoint:name=before_retry",
        },
        .{
            .sql = "ROLLBACK TO before_retry;",
            .fingerprint = "ddl:rollback_to_savepoint:name=before_retry",
        },
    };

    for (cases) |case| {
        var logical = try logicalDdlPlanForFingerprintTestAlloc(alloc, case.sql);
        defer logical.deinit(alloc);
        const fingerprint = try ddlFingerprintAlloc(alloc, logical);
        defer alloc.free(fingerprint);
        try std.testing.expectEqualStrings(case.fingerprint, fingerprint);
    }
}
