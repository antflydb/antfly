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
const db_mod = @import("../../storage/db/mod.zig");
const grammar = @import("grammar.zig");
const plan_mod = @import("plan.zig");
const runtime_schema = @import("../../storage/schema.zig");
const value_mod = @import("value.zig");

pub const AdapterNoopDdlReason = enum {
    schema_namespace,
    extension,
    transaction_control,
    session_setting,
};

pub const AdapterNoopDdlPlan = struct {
    reason: AdapterNoopDdlReason,
};

pub const SessionCatalogPlan = union(enum) {
    set_search_path: SetSearchPathPlan,
    set_setting: SetSessionSettingPlan,
    reset_setting: ResetSessionSettingPlan,
    reset_search_path,
    show_search_path,
    discard_all,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .set_search_path => |*plan| plan.deinit(alloc),
            .set_setting => |*plan| plan.deinit(alloc),
            .reset_setting => |*plan| plan.deinit(alloc),
            else => {},
        }
        self.* = undefined;
    }
};

pub const SetSearchPathPlan = struct {
    namespaces: []const []const u8 = &.{},
    local: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        freeStringSlice(alloc, self.namespaces);
        self.* = undefined;
    }
};

pub const SessionSettingKind = enum {
    app,
    runtime,
};

pub const SetSessionSettingPlan = struct {
    name: []const u8,
    value: []const u8,
    kind: SessionSettingKind = .app,
    local: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.name);
        alloc.free(self.value);
        self.* = undefined;
    }
};

pub const ResetSessionSettingPlan = struct {
    name: []const u8,
    kind: SessionSettingKind = .app,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.name);
        self.* = undefined;
    }
};

pub const EnumTypeCatalogPlan = union(enum) {
    create: CreateEnumTypePlan,
    add_value: AddEnumValuePlan,
    drop: DropEnumTypePlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .create => |*plan| plan.deinit(alloc),
            .add_value => |*plan| plan.deinit(alloc),
            .drop => |*plan| plan.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const CreateEnumTypePlan = struct {
    type_name: []const u8,
    values: []const []const u8 = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.type_name);
        freeStringSlice(alloc, self.values);
        self.* = undefined;
    }
};

pub const AddEnumValuePlan = struct {
    type_name: []const u8,
    value: []const u8,
    if_not_exists: bool = false,
    position: EnumValuePosition = .none,
    neighbor_value: ?[]const u8 = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.type_name);
        alloc.free(self.value);
        if (self.neighbor_value) |value| alloc.free(value);
        self.* = undefined;
    }
};

pub const EnumValuePosition = enum {
    none,
    before,
    after,
};

pub const DropEnumTypePlan = struct {
    type_name: []const u8,
    if_exists: bool = false,
    cascade: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.type_name);
        self.* = undefined;
    }
};

pub const DomainCatalogPlan = union(enum) {
    create: CreateDomainPlan,
    alter: AlterDomainPlan,
    drop: DropDomainPlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .create => |*plan| plan.deinit(alloc),
            .alter => |*plan| plan.deinit(alloc),
            .drop => |*plan| plan.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const CreateDomainPlan = struct {
    domain_name: []const u8,
    field_type: runtime_schema.AntflyType,
    array_item_type: ?runtime_schema.AntflyType = null,
    not_null: bool = false,
    default_value: ?runtime_schema.RelationalDefaultValue = null,
    checks: []const runtime_schema.RelationalCheck = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.domain_name);
        if (self.default_value) |default| alloc.free(default.value_json);
        freeDdlRelationalChecks(alloc, self.checks);
        self.* = undefined;
    }
};

pub const AlterDomainPlan = struct {
    domain_name: []const u8,
    operations: []const DomainAlterOperation = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.domain_name);
        freeDomainAlterOperations(alloc, self.operations);
        self.* = undefined;
    }
};

pub const DomainAlterOperation = union(enum) {
    set_not_null,
    drop_not_null,
    set_default: runtime_schema.RelationalDefaultValue,
    drop_default,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .set_default => |default| alloc.free(default.value_json),
            else => {},
        }
        self.* = undefined;
    }
};

pub const DropDomainPlan = struct {
    domain_name: []const u8,
    if_exists: bool = false,
    cascade: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.domain_name);
        self.* = undefined;
    }
};

pub const IdentityAllocatorPlan = struct {
    table_name: []const u8,
    column: runtime_schema.RelationalColumn,
    kind: IdentityAllocatorKind,
    options: SequenceOptions = .{},
    primary_key: bool = false,
    additional_columns: []const runtime_schema.RelationalColumn = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        freeDdlRelationalColumn(alloc, self.column);
        self.options.deinit(alloc);
        clearDdlRelationalColumns(alloc, self.additional_columns);
        if (self.additional_columns.len > 0) alloc.free(self.additional_columns);
        self.* = undefined;
    }
};

pub const IdentityAllocatorKind = enum {
    serial,
    bigserial,
    generated_by_default,
    generated_always,
};

pub const IdentityAllocatorSpec = struct {
    kind: IdentityAllocatorKind,
    options: SequenceOptions = .{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        self.options.deinit(alloc);
        self.* = undefined;
    }
};

pub const LoweredDdlPlan = union(enum) {
    adapter_noop: AdapterNoopDdlPlan,
    session_catalog: SessionCatalogPlan,
    create_table: CreateTablePlan,
    table_clone: TableClonePlan,
    view_catalog: ViewCatalogPlan,
    materialized_view_catalog: MaterializedViewCatalogPlan,
    relation_lifetime: RelationLifetimePlan,
    enum_type_catalog: EnumTypeCatalogPlan,
    domain_catalog: DomainCatalogPlan,
    sequence_catalog: SequenceCatalogPlan,
    identity_allocator_catalog: IdentityAllocatorPlan,
    schema_namespace_catalog: SchemaNamespaceCatalogPlan,
    extension_catalog: ExtensionCatalogPlan,
    function_catalog: FunctionCatalogPlan,
    authorization_catalog: AuthorizationCatalogPlan,
    bulk_io: BulkIoPlan,
    table_partition_catalog: TablePartitionCatalogPlan,
    row_security_catalog: RowSecurityCatalogPlan,
    database_catalog: DatabaseCatalogPlan,
    tablespace_catalog: TablespaceCatalogPlan,
    notification_channel: NotificationChannelPlan,
    logical_replication: LogicalReplicationPlan,
    type_system_catalog: TypeSystemCatalogPlan,
    maintenance_job: MaintenanceJobPlan,
    prepared_statement: PreparedStatementPlan,
    prepared_transaction: PreparedTransactionPlan,
    cursor_portal: CursorPortalPlan,
    savepoint_transaction: SavepointTransactionPlan,
    comment_metadata: CommentMetadataPlan,
    transaction_control: TransactionControlPlan,
    create_index: CreateIndexPlan,
    drop_index: DropIndexPlan,
    drop_table: DropTablePlan,
    alter_table: AlterTablePlan,
    create_update_policy: CreateUpdatePolicyPlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .adapter_noop => {},
            .session_catalog => |*plan| plan.deinit(alloc),
            .create_table => |*plan| plan.deinit(alloc),
            .table_clone => |*plan| plan.deinit(alloc),
            .view_catalog => |*plan| plan.deinit(alloc),
            .materialized_view_catalog => |*plan| plan.deinit(alloc),
            .relation_lifetime => |*plan| plan.deinit(alloc),
            .enum_type_catalog => |*plan| plan.deinit(alloc),
            .domain_catalog => |*plan| plan.deinit(alloc),
            .sequence_catalog => |*plan| plan.deinit(alloc),
            .identity_allocator_catalog => |*plan| plan.deinit(alloc),
            .schema_namespace_catalog => |*plan| plan.deinit(alloc),
            .extension_catalog => |*plan| plan.deinit(alloc),
            .function_catalog => |*plan| plan.deinit(alloc),
            .authorization_catalog => |*plan| plan.deinit(alloc),
            .bulk_io => |*plan| plan.deinit(alloc),
            .table_partition_catalog => |*plan| plan.deinit(alloc),
            .row_security_catalog => |*plan| plan.deinit(alloc),
            .database_catalog => |*plan| plan.deinit(alloc),
            .tablespace_catalog => |*plan| plan.deinit(alloc),
            .notification_channel => |*plan| plan.deinit(alloc),
            .logical_replication => |*plan| plan.deinit(alloc),
            .type_system_catalog => |*plan| plan.deinit(alloc),
            .maintenance_job => |*plan| plan.deinit(alloc),
            .prepared_statement => |*plan| plan.deinit(alloc),
            .prepared_transaction => |*plan| plan.deinit(alloc),
            .cursor_portal => |*plan| plan.deinit(alloc),
            .savepoint_transaction => |*plan| plan.deinit(alloc),
            .comment_metadata => |*plan| plan.deinit(alloc),
            .transaction_control => |*plan| plan.deinit(alloc),
            .create_index => |*plan| plan.deinit(alloc),
            .drop_index => |*plan| plan.deinit(alloc),
            .drop_table => |*plan| plan.deinit(alloc),
            .alter_table => |*plan| plan.deinit(alloc),
            .create_update_policy => |*plan| plan.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const RelationLifetimePlan = struct {
    kind: RelationLifetimeKind,
    create_table: CreateTablePlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        self.create_table.deinit(alloc);
        self.* = undefined;
    }
};

pub const RelationLifetimeKind = grammar.RelationLifetimeKind;

pub const TablePartitionCatalogPlan = union(enum) {
    create_partitioned: CreatePartitionedTablePlan,
    create_partition: CreateTablePartitionPlan,
    attach: AttachTablePartitionPlan,
    detach: DetachTablePartitionPlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .create_partitioned => |*plan| plan.deinit(alloc),
            .create_partition => |*plan| plan.deinit(alloc),
            .attach => |*plan| plan.deinit(alloc),
            .detach => |*plan| plan.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const CreatePartitionedTablePlan = struct {
    create_table: CreateTablePlan,
    method: TablePartitionMethod,
    keys: []const []const u8 = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        self.create_table.deinit(alloc);
        freeStringSlice(alloc, self.keys);
        self.* = undefined;
    }
};

pub const CreateTablePartitionPlan = struct {
    table_name: []const u8,
    parent_table_name: []const u8,
    bounds: TablePartitionBounds,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        alloc.free(self.parent_table_name);
        self.bounds.deinit(alloc);
        self.* = undefined;
    }
};

pub const AttachTablePartitionPlan = struct {
    parent_table_name: []const u8,
    partition_table_name: []const u8,
    bounds: TablePartitionBounds,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.parent_table_name);
        alloc.free(self.partition_table_name);
        self.bounds.deinit(alloc);
        self.* = undefined;
    }
};

pub const DetachTablePartitionPlan = struct {
    parent_table_name: []const u8,
    partition_table_name: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.parent_table_name);
        alloc.free(self.partition_table_name);
        self.* = undefined;
    }
};

pub const TablePartitionBounds = struct {
    lower_json: []const u8,
    upper_json: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.lower_json);
        alloc.free(self.upper_json);
        self.* = undefined;
    }
};

pub const TablePartitionMethod = enum {
    range,
};

pub const CreateTablePlan = struct {
    table_name: []const u8,
    if_not_exists: bool = false,
    replace_existing: bool = false,
    columns: []const runtime_schema.RelationalColumn = &.{},
    primary_key: ?runtime_schema.PrimaryKey = null,
    periods: []const runtime_schema.RelationalPeriod = &.{},
    unique_constraints: []const runtime_schema.UniqueConstraint = &.{},
    foreign_keys: []const runtime_schema.ForeignKey = &.{},
    checks: []const runtime_schema.RelationalCheck = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        freeDdlRelationalColumns(alloc, self.columns);
        if (self.primary_key) |primary_key| freeDdlPrimaryKey(alloc, primary_key);
        freeDdlPeriods(alloc, self.periods);
        freeDdlUniqueConstraints(alloc, self.unique_constraints);
        freeDdlForeignKeys(alloc, self.foreign_keys);
        freeDdlRelationalChecks(alloc, self.checks);
        self.* = undefined;
    }
};

pub const AlterTablePlan = struct {
    table_name: []const u8,
    if_exists: bool = false,
    operations: []const AlterTableOperation = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        for (self.operations) |operation| freeAlterTableOperation(alloc, operation);
        if (self.operations.len > 0) alloc.free(self.operations);
        self.* = undefined;
    }
};

pub const AlterTableOperation = union(enum) {
    add_column: AddColumnOperation,
    add_period: runtime_schema.RelationalPeriod,
    add_primary_key: runtime_schema.PrimaryKey,
    rename_column: RenameColumnOperation,
    rename_constraint: RenameConstraintOperation,
    drop_column: DropColumnOperation,
    drop_constraint: DropConstraintOperation,
    drop_update_policy: DropUpdatePolicyOperation,
    alter_column_default: AlterColumnDefaultOperation,
    alter_column_nullability: AlterColumnNullabilityOperation,
    alter_column_type: AlterColumnTypeOperation,
    add_unique_constraint: runtime_schema.UniqueConstraint,
    add_foreign_key: runtime_schema.ForeignKey,
    add_check: runtime_schema.RelationalCheck,
    validate_constraint: []const u8,
};

pub const AddColumnOperation = struct {
    column: runtime_schema.RelationalColumn,
    if_not_exists: bool = false,
    unique_constraints: []const runtime_schema.UniqueConstraint = &.{},
    foreign_keys: []const runtime_schema.ForeignKey = &.{},
    checks: []const runtime_schema.RelationalCheck = &.{},
};

pub const RenameColumnOperation = struct {
    old_name: []const u8,
    new_name: []const u8,
};

pub const RenameConstraintOperation = struct {
    old_name: []const u8,
    new_name: []const u8,
};

pub const DropDependencyMode = enum {
    cascade,
    restrict,
};

pub const DropColumnOperation = struct {
    name: []const u8,
    if_exists: bool = false,
    dependency_mode: DropDependencyMode = .cascade,
};

pub const DropConstraintOperation = struct {
    name: []const u8,
    if_exists: bool = false,
    dependency_mode: DropDependencyMode = .restrict,
};

pub const DropUpdatePolicyOperation = struct {
    trigger_name: []const u8,
    if_exists: bool = false,
};

pub const AlterColumnDefaultOperation = struct {
    column_name: []const u8,
    default_value: ?runtime_schema.RelationalDefaultValue = null,
};

pub const AlterColumnNullabilityOperation = struct {
    column_name: []const u8,
    nullable: bool,
};

pub const AlterColumnTypeOperation = struct {
    column_name: []const u8,
    field_type: runtime_schema.AntflyType,
    array_item_type: ?runtime_schema.AntflyType = null,
    collation: ?[]const u8 = null,
    rewrite_expression: ?AlterColumnRewriteExpression = null,
};

pub const AlterColumnRewriteExpression = struct {
    expression: db_mod.types.RelationalRowsExpression,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        runtime_schema.freeRelationalRowsExpression(alloc, self.expression);
        self.* = undefined;
    }
};

pub const CreateIndexPlan = struct {
    index_name: []const u8,
    table_name: []const u8,
    if_not_exists: bool = false,
    unique: bool = false,
    method: DdlIndexMethod = .btree,
    opclass: DdlIndexOpClass = .default,
    columns: []const []const u8 = &.{},
    include_columns: []const []const u8 = &.{},
    expressions: []const runtime_schema.UniqueExpression = &.{},
    generated_expression: ?runtime_schema.RelationalGeneratedValue = null,
    without_overlaps_period: ?[]const u8 = null,
    nulls_not_distinct: bool = false,
    where: []const runtime_schema.UniquePredicate = &.{},
    where_expressions: []const db_mod.types.RelationalRowsExpressionCondition = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.index_name);
        alloc.free(self.table_name);
        freeStringSlice(alloc, self.columns);
        freeStringSlice(alloc, self.include_columns);
        freeDdlUniqueExpressions(alloc, self.expressions);
        if (self.generated_expression) |generated| freeDdlGeneratedValue(alloc, generated);
        if (self.without_overlaps_period) |period| alloc.free(period);
        freeDdlUniquePredicates(alloc, self.where);
        freeExpressionConditions(alloc, self.where_expressions);
        if (self.where_expressions.len > 0) alloc.free(self.where_expressions);
        self.* = undefined;
    }
};

pub const DdlIndexMethod = enum {
    btree,
    gin,
};

pub const DdlIndexOpClass = enum {
    default,
    jsonb_path_ops,
    array_ops,
};

pub const AppliedDdlWorkAction = enum {
    rebuild,
    validate,
    rewrite,
};

pub const AppliedDdlWorkSubject = enum {
    table,
};

pub const AppliedDdlWorkReason = enum {
    derived_artifacts,
    constraints,
    row_images,
};

pub const AppliedDdlRewriteExpression = struct {
    target_column: []const u8,
    expression: db_mod.types.RelationalRowsExpression,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.target_column);
        runtime_schema.freeRelationalRowsExpression(alloc, self.expression);
        self.* = undefined;
    }
};

pub const AppliedDdlWorkItem = struct {
    action: AppliedDdlWorkAction,
    subject: AppliedDdlWorkSubject,
    reason: AppliedDdlWorkReason,
    rewrite_expression: ?AppliedDdlRewriteExpression = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.rewrite_expression) |*rewrite| rewrite.deinit(alloc);
        self.* = undefined;
    }
};

pub const AppliedDdlSchemaJson = struct {
    schema_json: []u8,
    requires_rebuild: bool = false,
    validation_required: bool = false,
    rewrite_required: bool = false,
    work_items: []const AppliedDdlWorkItem = &.{},

    pub fn takeSchemaJson(self: *@This()) []u8 {
        const schema_json = self.schema_json;
        self.schema_json = &.{};
        return schema_json;
    }

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.schema_json.len > 0) alloc.free(self.schema_json);
        for (self.work_items) |item| {
            var mutable = item;
            mutable.deinit(alloc);
        }
        if (self.work_items.len > 0) alloc.free(self.work_items);
        self.* = undefined;
    }
};

pub const SchemaNamespaceCatalogPlan = union(enum) {
    create: CreateSchemaNamespacePlan,
    rename: RenameSchemaNamespacePlan,
    drop: DropSchemaNamespacePlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .create => |*plan| plan.deinit(alloc),
            .rename => |*plan| plan.deinit(alloc),
            .drop => |*plan| plan.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const CreateSchemaNamespacePlan = struct {
    schema_name: []const u8,
    if_not_exists: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.schema_name);
        self.* = undefined;
    }
};

pub const RenameSchemaNamespacePlan = struct {
    schema_name: []const u8,
    new_schema_name: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.schema_name);
        alloc.free(self.new_schema_name);
        self.* = undefined;
    }
};

pub const DropSchemaNamespacePlan = struct {
    schema_name: []const u8,
    if_exists: bool = false,
    cascade: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.schema_name);
        self.* = undefined;
    }
};

pub const ExtensionCatalogPlan = union(enum) {
    create: CreateExtensionPlan,
    update: UpdateExtensionPlan,
    drop: DropExtensionPlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .create => |*plan| plan.deinit(alloc),
            .update => |*plan| plan.deinit(alloc),
            .drop => |*plan| plan.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const CreateExtensionPlan = struct {
    extension_name: []const u8,
    version: ?[]const u8 = null,
    if_not_exists: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.extension_name);
        if (self.version) |version| alloc.free(version);
        self.* = undefined;
    }
};

pub const UpdateExtensionPlan = struct {
    extension_name: []const u8,
    target_version: ?[]const u8 = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.extension_name);
        if (self.target_version) |version| alloc.free(version);
        self.* = undefined;
    }
};

pub const DropExtensionPlan = struct {
    extension_name: []const u8,
    if_exists: bool = false,
    cascade: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.extension_name);
        self.* = undefined;
    }
};

pub const FunctionCatalogPlan = union(enum) {
    create: CreateRoutinePlan,
    drop: DropRoutinePlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .create => |*plan| plan.deinit(alloc),
            .drop => |*plan| plan.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const RoutineKind = enum {
    function,
    procedure,
};

pub const RoutineVolatility = enum {
    immutable,
    stable,
    @"volatile",
};

pub const RoutineSecurity = enum {
    invoker,
    definer,
};

pub const RoutineParallelSafety = enum {
    safe,
    restricted,
    unsafe,
};

pub const RoutineNullInput = enum {
    called,
    returns_null,
};

pub const RoutineSetting = struct {
    name: []const u8,
    values: []const []const u8 = &.{},
    from_current: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.name);
        freeStringSlice(alloc, self.values);
        self.* = undefined;
    }
};

pub const RoutineBodyKind = enum {
    sql_expression,
};

pub const RoutineExecutionHook = enum {
    expression,
};

pub const RoutineBodyPlan = struct {
    kind: RoutineBodyKind,
    hook: RoutineExecutionHook,
    expression: db_mod.types.RelationalRowsExpression,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        runtime_schema.freeRelationalRowsExpression(alloc, self.expression);
        self.* = undefined;
    }
};

pub const CreateRoutinePlan = struct {
    kind: RoutineKind,
    routine_name: []const u8,
    replace_existing: bool = false,
    argument_count: usize = 0,
    returns_type: ?[]const u8 = null,
    language: ?[]const u8 = null,
    volatility: ?RoutineVolatility = null,
    security: ?RoutineSecurity = null,
    null_input: ?RoutineNullInput = null,
    parallel_safety: ?RoutineParallelSafety = null,
    leakproof: bool = false,
    window: bool = false,
    support_function: ?[]const u8 = null,
    transform_types: []const []const u8 = &.{},
    settings: []const RoutineSetting = &.{},
    cost: ?[]const u8 = null,
    rows: ?[]const u8 = null,
    body: ?RoutineBodyPlan = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.routine_name);
        if (self.returns_type) |returns_type| alloc.free(returns_type);
        if (self.language) |language| alloc.free(language);
        if (self.support_function) |support_function| alloc.free(support_function);
        freeStringSlice(alloc, self.transform_types);
        for (self.settings) |*setting_const| {
            var setting = setting_const.*;
            setting.deinit(alloc);
        }
        if (self.settings.len > 0) alloc.free(@constCast(self.settings));
        if (self.cost) |cost| alloc.free(cost);
        if (self.rows) |rows| alloc.free(rows);
        if (self.body) |*body| body.deinit(alloc);
        self.* = undefined;
    }
};

pub const DropRoutinePlan = struct {
    kind: RoutineKind,
    routine_name: []const u8,
    if_exists: bool = false,
    argument_count: usize = 0,
    cascade: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.routine_name);
        self.* = undefined;
    }
};

pub const AuthorizationCatalogPlan = union(enum) {
    create_role: CreateRolePlan,
    alter_role: AlterRolePlan,
    drop_role: DropRolePlan,
    grant_privilege: PrivilegeChangePlan,
    revoke_privilege: PrivilegeChangePlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .create_role => |*plan| plan.deinit(alloc),
            .alter_role => |*plan| plan.deinit(alloc),
            .drop_role => |*plan| plan.deinit(alloc),
            .grant_privilege => |*plan| plan.deinit(alloc),
            .revoke_privilege => |*plan| plan.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const PrivilegeChangeAction = enum {
    grant,
    revoke,
};

pub const CreateRolePlan = struct {
    role_name: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.role_name);
        self.* = undefined;
    }
};

pub const AlterRolePlan = struct {
    pub const Operation = enum {
        set,
        reset,
    };
    pub const SettingKind = enum {
        app,
        runtime,
    };
    pub const SettingValue = union(enum) {
        literal: []const u8,
        current_setting: []const u8,

        pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            switch (self.*) {
                .literal => |value| alloc.free(value),
                .current_setting => |name| alloc.free(name),
            }
            self.* = undefined;
        }
    };

    role_name: []const u8,
    database_name: ?[]const u8 = null,
    operation: Operation = .set,
    setting_kind: SettingKind = .app,
    setting_name: []const u8,
    setting_value: ?SettingValue = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.role_name);
        if (self.database_name) |database_name| alloc.free(database_name);
        alloc.free(self.setting_name);
        if (self.setting_value) |*setting_value| setting_value.deinit(alloc);
        self.* = undefined;
    }
};

pub const DropRolePlan = struct {
    role_name: []const u8,
    if_exists: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.role_name);
        self.* = undefined;
    }
};

pub const PrivilegeChangePlan = struct {
    privileges: []const []const u8 = &.{},
    object_kind: []const u8,
    object_name: []const u8,
    principal_name: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        freeStringSlice(alloc, self.privileges);
        alloc.free(self.object_kind);
        alloc.free(self.object_name);
        alloc.free(self.principal_name);
        self.* = undefined;
    }
};

pub const PreparedStatementPlan = union(enum) {
    prepare: PrepareStatementPlan,
    execute: ExecutePreparedStatementPlan,
    deallocate: DeallocatePreparedStatementPlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .prepare => |*plan| plan.deinit(alloc),
            .execute => |*plan| plan.deinit(alloc),
            .deallocate => |*plan| plan.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const PreparedTransactionPlan = struct {
    action: PreparedTransactionAction,
    gid: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.gid);
        self.* = undefined;
    }
};

pub const PreparedTransactionAction = enum {
    prepare,
    commit,
    rollback,
};

pub const PrepareStatementPlan = struct {
    statement_name: []const u8,
    parameter_count: usize = 0,
    statement_kind: PreparedStatementSubjectKind,
    statement_family: PreparedStatementStatementKind,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.statement_name);
        self.* = undefined;
    }
};

pub const PreparedStatementSubjectKind = enum {
    read,
    write,
    ddl,
};

pub const PreparedStatementStatementKind = enum {
    read,
    insert,
    insert_source,
    update,
    delete,
    truncate,
    merge,
    ddl,
};

pub const ExecutePreparedStatementPlan = struct {
    statement_name: []const u8,
    argument_count: usize = 0,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.statement_name);
        self.* = undefined;
    }
};

pub const DeallocatePreparedStatementPlan = struct {
    statement_name: ?[]const u8 = null,
    all: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.statement_name) |name| alloc.free(name);
        self.* = undefined;
    }
};

pub const CursorPortalPlan = union(enum) {
    declare: DeclareCursorPortalPlan,
    fetch: FetchCursorPortalPlan,
    close: CloseCursorPortalPlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .declare => |*plan| plan.deinit(alloc),
            .fetch => |*plan| plan.deinit(alloc),
            .close => |*plan| plan.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const DeclareCursorPortalPlan = struct {
    portal_name: []const u8,
    scroll: CursorScrollMode = .default,
    binary: bool = false,
    hold: bool = false,
    statement_kind: PreparedStatementSubjectKind,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.portal_name);
        self.* = undefined;
    }
};

pub const CursorScrollMode = enum {
    default,
    scroll,
    no_scroll,
};

pub const FetchCursorPortalPlan = struct {
    portal_name: []const u8,
    direction: CursorFetchDirection = .next,
    count: ?i64 = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.portal_name);
        self.* = undefined;
    }
};

pub const CursorFetchDirection = enum {
    next,
    prior,
    first,
    last,
    absolute,
    relative,
    forward,
    backward,
    all,
};

pub const CloseCursorPortalPlan = struct {
    portal_name: ?[]const u8 = null,
    all: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.portal_name) |portal| alloc.free(portal);
        self.* = undefined;
    }
};

pub const SavepointTransactionPlan = union(enum) {
    savepoint: SavepointNamePlan,
    release: SavepointNamePlan,
    rollback_to: SavepointNamePlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .savepoint => |*plan| plan.deinit(alloc),
            .release => |*plan| plan.deinit(alloc),
            .rollback_to => |*plan| plan.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const SavepointNamePlan = struct {
    savepoint_name: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.savepoint_name);
        self.* = undefined;
    }
};

pub const CommentMetadataPlan = struct {
    target: CommentMetadataTarget,
    object_name: []const u8,
    parent_table_name: ?[]const u8 = null,
    comment_json: ?[]const u8 = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.object_name);
        if (self.parent_table_name) |parent| alloc.free(parent);
        if (self.comment_json) |comment| alloc.free(comment);
        self.* = undefined;
    }
};

pub const CommentMetadataTarget = enum {
    table,
    column,
    index,
    constraint,
};

pub const TransactionControlPlan = union(enum) {
    table_lock: TableLockPlan,
    constraint_mode: ConstraintModePlan,
    transaction_mode: TransactionModePlan,
    advisory_lock: AdvisoryLockPlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .table_lock => |*plan| plan.deinit(alloc),
            .constraint_mode => |*plan| plan.deinit(alloc),
            .transaction_mode => {},
            .advisory_lock => {},
        }
        self.* = undefined;
    }
};

pub const TableLockPlan = struct {
    table_names: []const []const u8,
    mode: TableLockMode,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        freeStringSlice(alloc, self.table_names);
        self.* = undefined;
    }
};

pub const TableLockMode = enum {
    access_share,
    row_share,
    row_exclusive,
    share_update_exclusive,
    share,
    share_row_exclusive,
    exclusive,
    access_exclusive,
};

pub const ConstraintModePlan = struct {
    all: bool = false,
    constraint_names: []const []const u8 = &.{},
    mode: ConstraintCheckMode,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        freeStringSlice(alloc, self.constraint_names);
        self.* = undefined;
    }
};

pub const ConstraintCheckMode = enum {
    immediate,
    deferred,
};

pub const TransactionModePlan = struct {
    starter: TransactionModeStarter,
    isolation_level: ?TransactionIsolationLevel = null,
    access_mode: ?TransactionAccessMode = null,
    deferrable: ?bool = null,
};

pub const TransactionModeStarter = enum {
    set_transaction,
    start_transaction,
    begin,
};

pub const TransactionIsolationLevel = enum {
    serializable,
    repeatable_read,
    read_committed,
    read_uncommitted,
};

pub const TransactionAccessMode = enum {
    read_only,
    read_write,
};

pub const SequenceCatalogPlan = union(enum) {
    create: CreateSequencePlan,
    alter: AlterSequencePlan,
    drop: DropSequencePlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .create => |*plan| plan.deinit(alloc),
            .alter => |*plan| plan.deinit(alloc),
            .drop => |*plan| plan.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const CreateSequencePlan = struct {
    sequence_name: []const u8,
    if_not_exists: bool = false,
    options: SequenceOptions = .{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.sequence_name);
        self.options.deinit(alloc);
        self.* = undefined;
    }
};

pub const AlterSequencePlan = struct {
    sequence_name: []const u8,
    if_exists: bool = false,
    operations: []const SequenceAlterOperation = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.sequence_name);
        freeSequenceAlterOperations(alloc, self.operations);
        if (self.operations.len > 0) alloc.free(self.operations);
        self.* = undefined;
    }
};

pub const DropSequencePlan = struct {
    sequence_name: []const u8,
    if_exists: bool = false,
    cascade: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.sequence_name);
        self.* = undefined;
    }
};

pub const SequenceOptions = struct {
    as_type: ?[]const u8 = null,
    start_with: ?i64 = null,
    increment_by: ?i64 = null,
    min_value_specified: bool = false,
    min_value: ?i64 = null,
    max_value_specified: bool = false,
    max_value: ?i64 = null,
    cache: ?i64 = null,
    cycle: ?bool = null,
    owned_by: ?SequenceOwnedBy = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.as_type) |as_type| alloc.free(as_type);
        if (self.owned_by) |*owned_by| owned_by.deinit(alloc);
        self.* = undefined;
    }
};

pub const SequenceOwnedBy = struct {
    table_name: []const u8 = "",
    column_name: []const u8 = "",

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.table_name.len > 0) alloc.free(self.table_name);
        if (self.column_name.len > 0) alloc.free(self.column_name);
        self.* = undefined;
    }
};

pub const SequenceAlterOperation = union(enum) {
    set_type: []const u8,
    restart: ?i64,
    set_start: i64,
    set_increment: i64,
    set_min: ?i64,
    set_max: ?i64,
    set_cache: i64,
    set_cycle: bool,
    set_owned_by: SequenceOwnedBy,
};

pub const DatabaseCatalogPlan = union(enum) {
    create: CreateDatabasePlan,
    alter: AlterDatabasePlan,
    drop: DropDatabasePlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .create => |*plan| plan.deinit(alloc),
            .alter => |*plan| plan.deinit(alloc),
            .drop => |*plan| plan.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const CreateDatabasePlan = struct {
    database_name: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.database_name);
        self.* = undefined;
    }
};

pub const AlterDatabasePlan = struct {
    database_name: []const u8,
    operations: []const DatabaseAlterOperation = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.database_name);
        freeDatabaseAlterOperations(alloc, self.operations);
        self.* = undefined;
    }
};

pub const DatabaseAlterOperation = union(enum) {
    set_parameter: DatabaseSetParameterOperation,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .set_parameter => |operation| operation.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const DatabaseSetParameterOperation = struct {
    name: []const u8,
    value_json: []const u8,

    fn deinit(self: @This(), alloc: std.mem.Allocator) void {
        alloc.free(self.name);
        alloc.free(self.value_json);
    }
};

pub const DropDatabasePlan = struct {
    database_name: []const u8,
    if_exists: bool = false,
    force: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.database_name);
        self.* = undefined;
    }
};

pub const TablespaceCatalogPlan = union(enum) {
    create: CreateTablespacePlan,
    rename: RenameTablespacePlan,
    drop: DropTablespacePlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .create => |*plan| plan.deinit(alloc),
            .rename => |*plan| plan.deinit(alloc),
            .drop => |*plan| plan.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const CreateTablespacePlan = struct {
    tablespace_name: []const u8,
    location_json: []const u8,
    placement_policy_json: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.tablespace_name);
        alloc.free(self.location_json);
        alloc.free(self.placement_policy_json);
        self.* = undefined;
    }
};

pub const RenameTablespacePlan = struct {
    tablespace_name: []const u8,
    new_tablespace_name: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.tablespace_name);
        alloc.free(self.new_tablespace_name);
        self.* = undefined;
    }
};

pub const DropTablespacePlan = struct {
    tablespace_name: []const u8,
    if_exists: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.tablespace_name);
        self.* = undefined;
    }
};

pub const NotificationChannelPlan = union(enum) {
    listen: ListenNotificationPlan,
    notify: NotifyNotificationPlan,
    unlisten: UnlistenNotificationPlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .listen => |*plan| plan.deinit(alloc),
            .notify => |*plan| plan.deinit(alloc),
            .unlisten => |*plan| plan.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const ListenNotificationPlan = struct {
    channel_name: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.channel_name);
        self.* = undefined;
    }
};

pub const NotifyNotificationPlan = struct {
    channel_name: []const u8,
    payload_json: ?[]const u8 = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.channel_name);
        if (self.payload_json) |payload| alloc.free(payload);
        self.* = undefined;
    }
};

pub const UnlistenNotificationPlan = struct {
    channel_name: ?[]const u8 = null,
    all: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.channel_name) |channel_name| alloc.free(channel_name);
        self.* = undefined;
    }
};

pub const LogicalReplicationPlan = union(enum) {
    publication: PublicationCatalogPlan,
    subscription: SubscriptionCatalogPlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .publication => |*plan| plan.deinit(alloc),
            .subscription => |*plan| plan.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const PublicationCatalogPlan = union(enum) {
    create: CreatePublicationPlan,
    alter: AlterPublicationPlan,
    drop: DropPublicationPlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .create => |*plan| plan.deinit(alloc),
            .alter => |*plan| plan.deinit(alloc),
            .drop => |*plan| plan.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const CreatePublicationPlan = struct {
    publication_name: []const u8,
    table_names: []const []const u8 = &.{},
    all_tables: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.publication_name);
        freeStringSlice(alloc, self.table_names);
        self.* = undefined;
    }
};

pub const AlterPublicationPlan = struct {
    publication_name: []const u8,
    operation: PublicationAlterOperation,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.publication_name);
        self.operation.deinit(alloc);
        self.* = undefined;
    }
};

pub const PublicationAlterOperation = union(enum) {
    add_tables: []const []const u8,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .add_tables => |tables| freeStringSlice(alloc, tables),
        }
        self.* = undefined;
    }
};

pub const DropPublicationPlan = struct {
    publication_name: []const u8,
    if_exists: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.publication_name);
        self.* = undefined;
    }
};

pub const SubscriptionCatalogPlan = union(enum) {
    create: CreateSubscriptionPlan,
    alter: AlterSubscriptionPlan,
    drop: DropSubscriptionPlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .create => |*plan| plan.deinit(alloc),
            .alter => |*plan| plan.deinit(alloc),
            .drop => |*plan| plan.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const CreateSubscriptionPlan = struct {
    subscription_name: []const u8,
    connection_json: []const u8,
    publication_names: []const []const u8 = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.subscription_name);
        alloc.free(self.connection_json);
        freeStringSlice(alloc, self.publication_names);
        self.* = undefined;
    }
};

pub const AlterSubscriptionPlan = struct {
    subscription_name: []const u8,
    enabled: bool,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.subscription_name);
        self.* = undefined;
    }
};

pub const DropSubscriptionPlan = struct {
    subscription_name: []const u8,
    if_exists: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.subscription_name);
        self.* = undefined;
    }
};

pub const TypeSystemCatalogPlan = union(enum) {
    collation: CollationCatalogPlan,
    operator: OperatorCatalogPlan,
    aggregate: AggregateCatalogPlan,
    cast: CastCatalogPlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .collation => |*plan| plan.deinit(alloc),
            .operator => |*plan| plan.deinit(alloc),
            .aggregate => |*plan| plan.deinit(alloc),
            .cast => |*plan| plan.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const CollationCatalogPlan = union(enum) {
    create: CreateCollationPlan,
    rename: RenameCollationPlan,
    drop: DropCollationPlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .create => |*plan| plan.deinit(alloc),
            .rename => |*plan| plan.deinit(alloc),
            .drop => |*plan| plan.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const CreateCollationPlan = struct {
    collation_name: []const u8,
    option_count: usize = 0,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.collation_name);
        self.* = undefined;
    }
};

pub const RenameCollationPlan = struct {
    collation_name: []const u8,
    new_collation_name: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.collation_name);
        alloc.free(self.new_collation_name);
        self.* = undefined;
    }
};

pub const DropCollationPlan = struct {
    collation_name: []const u8,
    if_exists: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.collation_name);
        self.* = undefined;
    }
};

pub const OperatorCatalogPlan = union(enum) {
    create: CreateOperatorPlan,
    drop: DropOperatorPlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .create => |*plan| plan.deinit(alloc),
            .drop => |*plan| plan.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const CreateOperatorPlan = struct {
    operator_name: []const u8,
    option_count: usize = 0,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.operator_name);
        self.* = undefined;
    }
};

pub const DropOperatorPlan = struct {
    operator_name: []const u8,
    argument_count: usize = 0,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.operator_name);
        self.* = undefined;
    }
};

pub const AggregateCatalogPlan = union(enum) {
    create: CreateAggregatePlan,
    drop: DropAggregatePlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .create => |*plan| plan.deinit(alloc),
            .drop => |*plan| plan.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const CreateAggregatePlan = struct {
    aggregate_name: []const u8,
    argument_count: usize = 0,
    option_count: usize = 0,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.aggregate_name);
        self.* = undefined;
    }
};

pub const DropAggregatePlan = struct {
    aggregate_name: []const u8,
    argument_count: usize = 0,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.aggregate_name);
        self.* = undefined;
    }
};

pub const CastCatalogPlan = union(enum) {
    create: CreateCastPlan,
    drop: DropCastPlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .create => |*plan| plan.deinit(alloc),
            .drop => |*plan| plan.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const CreateCastPlan = struct {
    source_type: []const u8,
    target_type: []const u8,
    function_name: []const u8,
    assignment: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.source_type);
        alloc.free(self.target_type);
        alloc.free(self.function_name);
        self.* = undefined;
    }
};

pub const DropCastPlan = struct {
    source_type: []const u8,
    target_type: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.source_type);
        alloc.free(self.target_type);
        self.* = undefined;
    }
};

pub const MaintenanceJobPlan = union(enum) {
    vacuum: VacuumMaintenancePlan,
    analyze: AnalyzeMaintenancePlan,
    reindex: ReindexMaintenancePlan,
    cluster: ClusterMaintenancePlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .vacuum => |*plan| plan.deinit(alloc),
            .analyze => |*plan| plan.deinit(alloc),
            .reindex => |*plan| plan.deinit(alloc),
            .cluster => |*plan| plan.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const VacuumMaintenancePlan = struct {
    table_name: []const u8,
    full: bool = false,
    freeze: bool = false,
    verbose: bool = false,
    analyze: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        self.* = undefined;
    }
};

pub const AnalyzeMaintenancePlan = struct {
    table_name: []const u8,
    verbose: bool = false,
    column_count: usize = 0,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        self.* = undefined;
    }
};

pub const ReindexMaintenancePlan = struct {
    target: ReindexMaintenanceTarget,
    name: []const u8,
    concurrently: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.name);
        self.* = undefined;
    }
};

pub const ReindexMaintenanceTarget = enum {
    index,
    table,
    schema,
    database,
    system,
};

pub const ClusterMaintenancePlan = struct {
    table_name: []const u8,
    index_name: ?[]const u8 = null,
    verbose: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        if (self.index_name) |index_name| alloc.free(index_name);
        self.* = undefined;
    }
};

pub const BulkIoPlan = struct {
    direction: BulkIoDirection,
    table_name: []const u8,
    columns: []const []const u8 = &.{},
    endpoint_kind: BulkIoEndpointKind = .stream,
    endpoint: []const u8,
    format: ?[]const u8 = null,
    header: bool = false,
    freeze: bool = false,
    on_error: BulkIoOnErrorPolicy = .stop,
    reject_limit: ?usize = null,
    log_verbosity: BulkIoLogVerbosity = .default,
    force_quote_all: bool = false,
    force_quote_columns: []const []const u8 = &.{},
    force_not_null_columns: []const []const u8 = &.{},
    force_null_columns: []const []const u8 = &.{},
    delimiter: ?[]const u8 = null,
    quote: ?[]const u8 = null,
    escape: ?[]const u8 = null,
    null_marker: ?[]const u8 = null,
    default_marker: ?[]const u8 = null,
    encoding: ?[]const u8 = null,
    where_expressions: []const db_mod.types.RelationalRowsExpressionCondition = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        freeStringSlice(alloc, self.columns);
        freeStringSlice(alloc, self.force_quote_columns);
        freeStringSlice(alloc, self.force_not_null_columns);
        freeStringSlice(alloc, self.force_null_columns);
        alloc.free(self.endpoint);
        if (self.format) |format| alloc.free(format);
        if (self.delimiter) |delimiter| alloc.free(delimiter);
        if (self.quote) |quote| alloc.free(quote);
        if (self.escape) |escape| alloc.free(escape);
        if (self.null_marker) |null_marker| alloc.free(null_marker);
        if (self.default_marker) |default_marker| alloc.free(default_marker);
        if (self.encoding) |encoding| alloc.free(encoding);
        freeExpressionConditions(alloc, self.where_expressions);
        if (self.where_expressions.len > 0) alloc.free(self.where_expressions);
        self.* = undefined;
    }
};

pub const BulkIoDirection = enum {
    from,
    to,
};

pub const BulkIoEndpointKind = enum {
    stream,
    file,
    program,
};

pub const BulkIoOnErrorPolicy = enum {
    stop,
    ignore,
};

pub const BulkIoLogVerbosity = enum {
    default,
    verbose,
    terse,
};

pub const MaterializedViewCatalogPlan = union(enum) {
    create: CreateMaterializedViewPlan,
    refresh: RefreshMaterializedViewPlan,
    drop: DropMaterializedViewPlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .create => |*plan| plan.deinit(alloc),
            .refresh => |*plan| plan.deinit(alloc),
            .drop => |*plan| plan.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const CreateMaterializedViewPlan = struct {
    view_name: []const u8,
    source_table_name: []const u8,
    source_fields: []const []const u8 = &.{},
    output_fields: []const []const u8 = &.{},
    replace_existing: bool = false,
    if_not_exists: bool = false,
    populate_on_create: bool = true,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.view_name);
        alloc.free(self.source_table_name);
        freeStringSlice(alloc, self.source_fields);
        freeStringSlice(alloc, self.output_fields);
        self.* = undefined;
    }
};

pub const RefreshMaterializedViewPlan = struct {
    view_name: []const u8,
    concurrently: bool = false,
    populate: bool = true,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.view_name);
        self.* = undefined;
    }
};

pub const DropMaterializedViewPlan = struct {
    view_name: []const u8,
    if_exists: bool = false,
    cascade: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.view_name);
        self.* = undefined;
    }
};

pub const ViewCatalogPlan = union(enum) {
    create: CreateViewPlan,
    rename: RenameViewPlan,
    drop: DropViewPlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .create => |*plan| plan.deinit(alloc),
            .rename => |*plan| plan.deinit(alloc),
            .drop => |*plan| plan.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const CreateViewPlan = struct {
    view_name: []const u8,
    source_table_name: []const u8,
    source_fields: []const []const u8 = &.{},
    output_fields: []const []const u8 = &.{},
    replace_existing: bool = false,
    if_not_exists: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.view_name);
        alloc.free(self.source_table_name);
        freeStringSlice(alloc, self.source_fields);
        freeStringSlice(alloc, self.output_fields);
        self.* = undefined;
    }
};

pub const RenameViewPlan = struct {
    view_name: []const u8,
    new_view_name: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.view_name);
        alloc.free(self.new_view_name);
        self.* = undefined;
    }
};

pub const DropViewPlan = struct {
    view_name: []const u8,
    if_exists: bool = false,
    cascade: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.view_name);
        self.* = undefined;
    }
};

pub const TableClonePlan = struct {
    table_name: []const u8,
    source_table_name: []const u8,
    if_not_exists: bool = false,
    options: TableCloneOptions = .{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        alloc.free(self.source_table_name);
        self.* = undefined;
    }
};

pub const TableCloneOptions = struct {
    columns: bool = true,
    defaults: bool = false,
    generated: bool = false,
    checks: bool = false,
    constraints: bool = false,
    indexes: bool = false,
    periods: bool = false,
    update_policies: bool = false,

    pub fn includingAll() TableCloneOptions {
        return .{
            .columns = true,
            .defaults = true,
            .generated = true,
            .checks = true,
            .constraints = true,
            .indexes = true,
            .periods = true,
            .update_policies = true,
        };
    }
};

pub const DropTablePlan = struct {
    table_name: []const u8,
    if_exists: bool = false,
    cascade: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        self.* = undefined;
    }
};

pub const DropIndexPlan = struct {
    index_name: []const u8,
    if_exists: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.index_name);
        self.* = undefined;
    }
};

pub const CreateUpdatePolicyPlan = struct {
    trigger_name: []const u8,
    table_name: []const u8,
    column_name: []const u8,
    on_update_value: runtime_schema.RelationalDefaultValue,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.trigger_name);
        alloc.free(self.table_name);
        alloc.free(self.column_name);
        alloc.free(self.on_update_value.value_json);
        self.* = undefined;
    }
};

pub const RowSecurityCatalogPlan = union(enum) {
    alter_table: AlterRowSecurityPlan,
    create_policy: CreateRowSecurityPolicyPlan,
    alter_policy: AlterRowSecurityPolicyPlan,
    drop_policy: DropRowSecurityPolicyPlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .alter_table => |*plan| plan.deinit(alloc),
            .create_policy => |*plan| plan.deinit(alloc),
            .alter_policy => |*plan| plan.deinit(alloc),
            .drop_policy => |*plan| plan.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const AlterRowSecurityPlan = struct {
    table_name: []const u8,
    enabled: bool,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        self.* = undefined;
    }
};

pub const CreateRowSecurityPolicyPlan = struct {
    policy_name: []const u8,
    table_name: []const u8,
    role_targets: []const []const u8 = &.{},
    predicate: RowSecurityPolicyPredicate,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.policy_name);
        alloc.free(self.table_name);
        freeStringSlice(alloc, self.role_targets);
        self.predicate.deinit(alloc);
        self.* = undefined;
    }
};

pub const AlterRowSecurityPolicyPlan = struct {
    policy_name: []const u8,
    table_name: []const u8,
    role_targets: []const []const u8 = &.{},
    predicate: RowSecurityPolicyPredicate,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.policy_name);
        alloc.free(self.table_name);
        freeStringSlice(alloc, self.role_targets);
        self.predicate.deinit(alloc);
        self.* = undefined;
    }
};

pub const DropRowSecurityPolicyPlan = struct {
    policy_name: []const u8,
    table_name: []const u8,
    if_exists: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.policy_name);
        alloc.free(self.table_name);
        self.* = undefined;
    }
};

pub const RowSecurityPolicyPredicate = union(enum) {
    current_setting_equals: RowSecurityCurrentSettingPredicate,
    literal_equals: RowSecurityLiteralPredicate,
    expression: db_mod.types.RelationalRowsExpressionCondition,
    conjunction: RowSecurityConjunctionPredicate,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .current_setting_equals => |*predicate| predicate.deinit(alloc),
            .literal_equals => |*predicate| predicate.deinit(alloc),
            .expression => |condition| runtime_schema.freeRelationalRowsExpressionCondition(alloc, condition),
            .conjunction => |*predicate| predicate.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const RowSecurityConjunctionPredicate = struct {
    predicates: []RowSecurityPolicyPredicate,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.predicates) |*predicate| predicate.deinit(alloc);
        alloc.free(self.predicates);
        self.* = undefined;
    }
};

pub const RowSecurityCurrentSettingPredicate = struct {
    field: []const u8,
    setting_name: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.field);
        alloc.free(self.setting_name);
        self.* = undefined;
    }
};

pub const RowSecurityLiteralPredicate = struct {
    field: []const u8,
    value_json: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.field);
        alloc.free(self.value_json);
        self.* = undefined;
    }
};

pub const AdvisoryLockPlan = struct {
    action: AdvisoryLockAction,
    key1: i64,
    key2: ?i64 = null,
};

pub const AdvisoryLockAction = enum {
    lock,
    unlock,
};

pub fn cursorScrollModeFromSyntax(syntax: grammar.CursorScrollSyntax) CursorScrollMode {
    return switch (syntax) {
        .default => .default,
        .scroll => .scroll,
        .no_scroll => .no_scroll,
    };
}

pub fn cursorFetchDirectionFromSyntax(syntax: grammar.CursorFetchDirectionSyntax) CursorFetchDirection {
    return switch (syntax) {
        .next => .next,
        .prior => .prior,
        .first => .first,
        .last => .last,
        .absolute => .absolute,
        .relative => .relative,
        .forward => .forward,
        .backward => .backward,
        .all => .all,
    };
}

pub fn preparedStatementSubjectKindFromSyntax(syntax: grammar.PreparedStatementSubjectSyntax) PreparedStatementSubjectKind {
    return switch (syntax) {
        .read => .read,
        .write => .write,
        .ddl => .ddl,
    };
}

pub fn preparedStatementStatementKindFromSyntax(syntax: grammar.PreparedStatementStatementSyntax) PreparedStatementStatementKind {
    return switch (syntax) {
        .read => .read,
        .insert => .insert,
        .insert_source => .insert_source,
        .update => .update,
        .delete => .delete,
        .truncate => .truncate,
        .merge => .merge,
        .ddl => .ddl,
    };
}

pub fn savepointNamePlanFromSyntaxAlloc(alloc: std.mem.Allocator, syntax: grammar.SavepointNameSyntax) !SavepointNamePlan {
    return .{ .savepoint_name = try alloc.dupe(u8, syntax.savepoint_name) };
}

pub fn deallocatePreparedStatementPlanFromSyntaxAlloc(
    alloc: std.mem.Allocator,
    syntax: grammar.NamedOrAllSyntax,
) !DeallocatePreparedStatementPlan {
    if (syntax.all) return .{ .all = true };
    const statement_name = syntax.name orelse return error.UnsupportedSqlShape;
    return .{ .statement_name = try alloc.dupe(u8, statement_name) };
}

pub fn closeCursorPortalPlanFromSyntaxAlloc(
    alloc: std.mem.Allocator,
    syntax: grammar.NamedOrAllSyntax,
) !CloseCursorPortalPlan {
    if (syntax.all) return .{ .all = true };
    const portal_name = syntax.name orelse return error.UnsupportedSqlShape;
    return .{ .portal_name = try alloc.dupe(u8, portal_name) };
}

pub fn commentMetadataPlanFromSyntax(syntax: *grammar.CommentMetadataSyntax) CommentMetadataPlan {
    const object_name = syntax.object_name;
    const parent_table_name = syntax.parent_table_name;
    const comment_json = syntax.comment_json;
    syntax.object_name = "";
    syntax.parent_table_name = null;
    syntax.comment_json = null;
    return .{
        .target = syntax.target,
        .object_name = object_name,
        .parent_table_name = parent_table_name,
        .comment_json = comment_json,
    };
}

pub fn tableLockPlanFromSyntax(syntax: *grammar.TableLockSyntax) TableLockPlan {
    const table_names = syntax.table_names;
    syntax.table_names = &.{};
    return .{
        .table_names = table_names,
        .mode = tableLockModeFromSyntax(syntax.mode),
    };
}

pub fn constraintModePlanFromSyntax(syntax: *grammar.ConstraintModeSyntax) ConstraintModePlan {
    const constraint_names = syntax.constraint_names;
    syntax.constraint_names = &.{};
    return .{
        .all = syntax.all,
        .constraint_names = constraint_names,
        .mode = constraintCheckModeFromSyntax(syntax.mode),
    };
}

pub fn transactionModePlanFromSyntax(syntax: grammar.TransactionModeSyntax) TransactionModePlan {
    return .{
        .starter = transactionModeStarterFromSyntax(syntax.starter),
        .isolation_level = transactionIsolationLevelFromSyntax(syntax.isolation_level),
        .access_mode = transactionAccessModeFromSyntax(syntax.access_mode),
        .deferrable = syntax.deferrable,
    };
}

pub fn advisoryLockPlanFromSyntax(syntax: grammar.AdvisoryLockSyntax) AdvisoryLockPlan {
    return .{
        .action = advisoryLockActionFromSyntax(syntax.action),
        .key1 = syntax.key1,
        .key2 = syntax.key2,
    };
}

pub fn tableLockModeFromSyntax(syntax: grammar.TableLockModeSyntax) TableLockMode {
    return switch (syntax) {
        .access_share => .access_share,
        .row_share => .row_share,
        .row_exclusive => .row_exclusive,
        .share_update_exclusive => .share_update_exclusive,
        .share => .share,
        .share_row_exclusive => .share_row_exclusive,
        .exclusive => .exclusive,
        .access_exclusive => .access_exclusive,
    };
}

pub fn constraintCheckModeFromSyntax(syntax: grammar.ConstraintCheckModeSyntax) ConstraintCheckMode {
    return switch (syntax) {
        .immediate => .immediate,
        .deferred => .deferred,
    };
}

pub fn ddlForeignKeyActionFromSyntax(action: grammar.DdlForeignKeyActionSyntax) runtime_schema.ForeignKeyAction {
    return switch (action) {
        .restrict => .restrict,
        .cascade => .cascade,
        .no_action => .no_action,
        .set_null => .set_null,
    };
}

pub fn ddlForeignKeyTimingFromSyntax(timing: grammar.DdlForeignKeyTimingSyntax) runtime_schema.ForeignKeyTiming {
    return switch (timing) {
        .immediate => .immediate,
        .deferred => .deferred,
    };
}

pub fn ddlForeignKeyMatchFromSyntax(match_syntax: grammar.DdlForeignKeyMatchSyntax) runtime_schema.ForeignKeyMatch {
    return switch (match_syntax) {
        .simple => .simple,
        .full => .full,
    };
}

pub fn transactionModeStarterToSyntax(starter: TransactionModeStarter) grammar.TransactionModeStarterSyntax {
    return switch (starter) {
        .set_transaction => .set_transaction,
        .start_transaction => .start_transaction,
        .begin => .begin,
    };
}

pub fn transactionModeStarterFromSyntax(syntax: grammar.TransactionModeStarterSyntax) TransactionModeStarter {
    return switch (syntax) {
        .set_transaction => .set_transaction,
        .start_transaction => .start_transaction,
        .begin => .begin,
    };
}

pub fn transactionIsolationLevelFromSyntax(syntax: ?grammar.TransactionIsolationLevelSyntax) ?TransactionIsolationLevel {
    return switch (syntax orelse return null) {
        .serializable => .serializable,
        .repeatable_read => .repeatable_read,
        .read_committed => .read_committed,
        .read_uncommitted => .read_uncommitted,
    };
}

pub fn transactionAccessModeFromSyntax(syntax: ?grammar.TransactionAccessModeSyntax) ?TransactionAccessMode {
    return switch (syntax orelse return null) {
        .read_only => .read_only,
        .read_write => .read_write,
    };
}

pub fn advisoryLockActionFromSyntax(syntax: grammar.AdvisoryLockActionSyntax) AdvisoryLockAction {
    return switch (syntax) {
        .lock => .lock,
        .unlock => .unlock,
    };
}

pub fn reindexMaintenanceTargetFromSyntax(syntax: grammar.ReindexMaintenanceTargetSyntax) ReindexMaintenanceTarget {
    return switch (syntax) {
        .index => .index,
        .table => .table,
        .schema => .schema,
        .database => .database,
        .system => .system,
    };
}

pub fn bulkIoDirectionFromSyntax(syntax: grammar.BulkIoDirectionSyntax) BulkIoDirection {
    return switch (syntax) {
        .from => .from,
        .to => .to,
    };
}

pub fn privilegeChangeActionToSyntax(action: PrivilegeChangeAction) grammar.PrivilegeChangeActionSyntax {
    return switch (action) {
        .grant => .grant,
        .revoke => .revoke,
    };
}

pub fn isAdapterNoopExtensionName(extension_name: []const u8) bool {
    return std.ascii.eqlIgnoreCase(extension_name, "pgcrypto") or
        std.ascii.eqlIgnoreCase(extension_name, "uuid-ossp");
}

pub fn createExtensionPlanFromSyntax(
    syntax: *grammar.CreateExtensionSyntax,
    default_namespace_name: []const u8,
) !CreateExtensionPlan {
    if (syntax.schema_name) |schema_name| {
        if (!syntax.if_not_exists or
            !isAdapterNoopExtensionName(syntax.extension_name) or
            !std.ascii.eqlIgnoreCase(schema_name, default_namespace_name))
        {
            return error.UnsupportedSqlShape;
        }
    }
    const extension_name = syntax.extension_name;
    const version = syntax.version;
    syntax.extension_name = "";
    syntax.version = null;
    return .{
        .extension_name = extension_name,
        .version = version,
        .if_not_exists = syntax.if_not_exists,
    };
}

pub fn updateExtensionPlanFromSyntax(syntax: *grammar.UpdateExtensionSyntax) UpdateExtensionPlan {
    const extension_name = syntax.extension_name;
    const target_version = syntax.target_version;
    syntax.extension_name = "";
    syntax.target_version = null;
    return .{
        .extension_name = extension_name,
        .target_version = target_version,
    };
}

pub fn dropExtensionPlanFromSyntax(syntax: *grammar.DropExtensionSyntax) DropExtensionPlan {
    const extension_name = syntax.extension_name;
    syntax.extension_name = "";
    return .{
        .extension_name = extension_name,
        .if_exists = syntax.if_exists,
        .cascade = syntax.cascade,
    };
}

pub fn createSchemaNamespacePlanFromSyntax(syntax: *grammar.CreateSchemaNamespaceSyntax) CreateSchemaNamespacePlan {
    const schema_name = syntax.schema_name;
    syntax.schema_name = "";
    return .{
        .schema_name = schema_name,
        .if_not_exists = syntax.if_not_exists,
    };
}

pub fn renameSchemaNamespacePlanFromSyntax(syntax: *grammar.RenameSchemaNamespaceSyntax) RenameSchemaNamespacePlan {
    const schema_name = syntax.schema_name;
    const new_schema_name = syntax.new_schema_name;
    syntax.schema_name = "";
    syntax.new_schema_name = "";
    return .{
        .schema_name = schema_name,
        .new_schema_name = new_schema_name,
    };
}

pub fn dropSchemaNamespacePlanFromSyntax(syntax: *grammar.DropSchemaNamespaceSyntax) DropSchemaNamespacePlan {
    const schema_name = syntax.schema_name;
    syntax.schema_name = "";
    return .{
        .schema_name = schema_name,
        .if_exists = syntax.if_exists,
        .cascade = syntax.cascade,
    };
}

pub fn createDatabasePlanFromSyntax(syntax: *grammar.CreateDatabaseSyntax) CreateDatabasePlan {
    const database_name = syntax.database_name;
    syntax.database_name = "";
    return .{ .database_name = database_name };
}

pub fn alterDatabasePlanFromSyntaxAlloc(
    alloc: std.mem.Allocator,
    syntax: *grammar.AlterDatabaseSyntax,
) !AlterDatabasePlan {
    const operations = try alloc.alloc(DatabaseAlterOperation, 1);
    errdefer alloc.free(operations);
    operations[0] = .{ .set_parameter = .{
        .name = syntax.setting_name,
        .value_json = syntax.value_json,
    } };
    const database_name = syntax.database_name;
    syntax.database_name = "";
    syntax.setting_name = "";
    syntax.value_json = "";
    return .{
        .database_name = database_name,
        .operations = operations,
    };
}

pub fn dropDatabasePlanFromSyntax(syntax: *grammar.DropDatabaseSyntax) DropDatabasePlan {
    const database_name = syntax.database_name;
    syntax.database_name = "";
    return .{
        .database_name = database_name,
        .if_exists = syntax.if_exists,
        .force = syntax.force,
    };
}

pub fn createTablespacePlanFromSyntax(syntax: *grammar.CreateTablespaceSyntax) CreateTablespacePlan {
    const tablespace_name = syntax.tablespace_name;
    const location_json = syntax.location_json;
    const placement_policy_json = syntax.placement_policy_json;
    syntax.tablespace_name = "";
    syntax.location_json = "";
    syntax.placement_policy_json = "";
    return .{
        .tablespace_name = tablespace_name,
        .location_json = location_json,
        .placement_policy_json = placement_policy_json,
    };
}

pub fn renameTablespacePlanFromSyntax(syntax: *grammar.RenameTablespaceSyntax) RenameTablespacePlan {
    const tablespace_name = syntax.tablespace_name;
    const new_tablespace_name = syntax.new_tablespace_name;
    syntax.tablespace_name = "";
    syntax.new_tablespace_name = "";
    return .{
        .tablespace_name = tablespace_name,
        .new_tablespace_name = new_tablespace_name,
    };
}

pub fn dropTablespacePlanFromSyntax(syntax: *grammar.DropTablespaceSyntax) DropTablespacePlan {
    const tablespace_name = syntax.tablespace_name;
    syntax.tablespace_name = "";
    return .{
        .tablespace_name = tablespace_name,
        .if_exists = syntax.if_exists,
    };
}

pub fn listenNotificationPlanFromSyntax(syntax: *grammar.ListenNotificationSyntax) ListenNotificationPlan {
    const channel_name = syntax.channel_name;
    syntax.channel_name = "";
    return .{ .channel_name = channel_name };
}

pub fn notifyNotificationPlanFromSyntax(syntax: *grammar.NotifyNotificationSyntax) NotifyNotificationPlan {
    const channel_name = syntax.channel_name;
    const payload_json = syntax.payload_json;
    syntax.channel_name = "";
    syntax.payload_json = null;
    return .{
        .channel_name = channel_name,
        .payload_json = payload_json,
    };
}

pub fn unlistenNotificationPlanFromSyntax(syntax: *grammar.UnlistenNotificationSyntax) UnlistenNotificationPlan {
    const channel_name = syntax.channel_name;
    syntax.channel_name = null;
    return .{
        .channel_name = channel_name,
        .all = syntax.all,
    };
}

pub fn createPublicationPlanFromSyntax(syntax: *grammar.CreatePublicationSyntax) CreatePublicationPlan {
    const publication_name = syntax.publication_name;
    const table_names = syntax.table_names;
    syntax.publication_name = "";
    syntax.table_names = &.{};
    return .{
        .publication_name = publication_name,
        .table_names = table_names,
        .all_tables = syntax.all_tables,
    };
}

pub fn alterPublicationPlanFromSyntax(syntax: *grammar.AlterPublicationSyntax) AlterPublicationPlan {
    const publication_name = syntax.publication_name;
    const table_names = syntax.table_names;
    syntax.publication_name = "";
    syntax.table_names = &.{};
    return .{
        .publication_name = publication_name,
        .operation = .{ .add_tables = table_names },
    };
}

pub fn dropPublicationPlanFromSyntax(syntax: *grammar.DropPublicationSyntax) DropPublicationPlan {
    const publication_name = syntax.publication_name;
    syntax.publication_name = "";
    return .{
        .publication_name = publication_name,
        .if_exists = syntax.if_exists,
    };
}

pub fn createSubscriptionPlanFromSyntax(syntax: *grammar.CreateSubscriptionSyntax) CreateSubscriptionPlan {
    const subscription_name = syntax.subscription_name;
    const connection_json = syntax.connection_json;
    const publication_names = syntax.publication_names;
    syntax.subscription_name = "";
    syntax.connection_json = "";
    syntax.publication_names = &.{};
    return .{
        .subscription_name = subscription_name,
        .connection_json = connection_json,
        .publication_names = publication_names,
    };
}

pub fn alterSubscriptionPlanFromSyntax(syntax: *grammar.AlterSubscriptionSyntax) AlterSubscriptionPlan {
    const subscription_name = syntax.subscription_name;
    syntax.subscription_name = "";
    return .{
        .subscription_name = subscription_name,
        .enabled = syntax.enabled,
    };
}

pub fn dropSubscriptionPlanFromSyntax(syntax: *grammar.DropSubscriptionSyntax) DropSubscriptionPlan {
    const subscription_name = syntax.subscription_name;
    syntax.subscription_name = "";
    return .{
        .subscription_name = subscription_name,
        .if_exists = syntax.if_exists,
    };
}

pub fn vacuumMaintenancePlanFromSyntax(syntax: *grammar.VacuumMaintenanceSyntax) VacuumMaintenancePlan {
    const table_name = syntax.table_name;
    syntax.table_name = "";
    return .{
        .table_name = table_name,
        .full = syntax.full,
        .freeze = syntax.freeze,
        .verbose = syntax.verbose,
        .analyze = syntax.analyze,
    };
}

pub fn analyzeMaintenancePlanFromSyntax(syntax: *grammar.AnalyzeMaintenanceSyntax) AnalyzeMaintenancePlan {
    const table_name = syntax.table_name;
    syntax.table_name = "";
    return .{
        .table_name = table_name,
        .verbose = syntax.verbose,
        .column_count = syntax.column_count,
    };
}

pub fn reindexMaintenancePlanFromSyntax(syntax: *grammar.ReindexMaintenanceSyntax) ReindexMaintenancePlan {
    const name = syntax.name;
    syntax.name = "";
    return .{
        .target = reindexMaintenanceTargetFromSyntax(syntax.target),
        .name = name,
        .concurrently = syntax.concurrently,
    };
}

pub fn clusterMaintenancePlanFromSyntax(syntax: *grammar.ClusterMaintenanceSyntax) ClusterMaintenancePlan {
    const table_name = syntax.table_name;
    const index_name = syntax.index_name;
    syntax.table_name = "";
    syntax.index_name = null;
    return .{
        .table_name = table_name,
        .index_name = index_name,
        .verbose = syntax.verbose,
    };
}

pub fn prepareStatementPlanFromSyntaxAlloc(
    alloc: std.mem.Allocator,
    syntax: grammar.PrepareStatementSyntax,
) !PrepareStatementPlan {
    return .{
        .statement_name = try alloc.dupe(u8, syntax.statement_name),
        .parameter_count = syntax.parameter_count,
        .statement_kind = preparedStatementSubjectKindFromSyntax(syntax.statement_kind),
        .statement_family = preparedStatementStatementKindFromSyntax(syntax.statement_family),
    };
}

pub fn executePreparedStatementPlanFromSyntaxAlloc(
    alloc: std.mem.Allocator,
    syntax: grammar.ExecutePreparedStatementSyntax,
) !ExecutePreparedStatementPlan {
    return .{
        .statement_name = try alloc.dupe(u8, syntax.statement_name),
        .argument_count = syntax.argument_count,
    };
}

pub fn declareCursorPortalPlanFromSyntaxAlloc(
    alloc: std.mem.Allocator,
    syntax: grammar.DeclareCursorPortalSyntax,
) !DeclareCursorPortalPlan {
    return .{
        .portal_name = try alloc.dupe(u8, syntax.portal_name),
        .scroll = cursorScrollModeFromSyntax(syntax.scroll),
        .binary = syntax.binary,
        .hold = syntax.hold,
        .statement_kind = preparedStatementSubjectKindFromSyntax(syntax.statement_kind orelse return error.UnsupportedSqlShape),
    };
}

pub fn fetchCursorPortalPlanFromSyntaxAlloc(
    alloc: std.mem.Allocator,
    syntax: grammar.FetchCursorPortalSyntax,
) !FetchCursorPortalPlan {
    return .{
        .portal_name = try alloc.dupe(u8, syntax.portal_name),
        .direction = cursorFetchDirectionFromSyntax(syntax.direction),
        .count = syntax.count,
    };
}

pub fn dropDomainPlanFromSyntax(syntax: *grammar.DropDomainSyntax) DropDomainPlan {
    const domain_name = syntax.domain_name;
    syntax.domain_name = "";
    return .{
        .domain_name = domain_name,
        .if_exists = syntax.if_exists,
        .cascade = syntax.cascade,
    };
}

pub fn createSequencePlanFromSyntax(syntax: *grammar.CreateSequenceSyntax) CreateSequencePlan {
    const sequence_name = syntax.sequence_name;
    const options = syntax.options;
    syntax.sequence_name = "";
    syntax.options = .{};
    return .{
        .sequence_name = sequence_name,
        .if_not_exists = syntax.if_not_exists,
        .options = options,
    };
}

pub fn alterSequencePlanFromSyntax(syntax: *grammar.AlterSequenceSyntax) AlterSequencePlan {
    const sequence_name = syntax.sequence_name;
    const operations = syntax.operations;
    syntax.sequence_name = "";
    syntax.operations = &.{};
    return .{
        .sequence_name = sequence_name,
        .if_exists = syntax.if_exists,
        .operations = operations,
    };
}

pub fn dropSequencePlanFromSyntax(syntax: *grammar.DropSequenceSyntax) DropSequencePlan {
    const sequence_name = syntax.sequence_name;
    syntax.sequence_name = "";
    return .{
        .sequence_name = sequence_name,
        .if_exists = syntax.if_exists,
        .cascade = syntax.cascade,
    };
}

pub fn createEnumTypePlanFromSyntax(syntax: *grammar.CreateEnumTypeSyntax) CreateEnumTypePlan {
    const type_name = syntax.type_name;
    const values = syntax.values;
    syntax.type_name = "";
    syntax.values = &.{};
    return .{
        .type_name = type_name,
        .values = values,
    };
}

pub fn addEnumValuePlanFromSyntax(syntax: *grammar.AddEnumValueSyntax) AddEnumValuePlan {
    const type_name = syntax.type_name;
    const value = syntax.value;
    const neighbor_value = syntax.neighbor_value;
    syntax.type_name = "";
    syntax.value = "";
    syntax.neighbor_value = null;
    return .{
        .type_name = type_name,
        .value = value,
        .if_not_exists = syntax.if_not_exists,
        .position = syntax.position,
        .neighbor_value = neighbor_value,
    };
}

pub fn dropEnumTypePlanFromSyntax(syntax: *grammar.DropEnumTypeSyntax) DropEnumTypePlan {
    const type_name = syntax.type_name;
    syntax.type_name = "";
    return .{
        .type_name = type_name,
        .if_exists = syntax.if_exists,
        .cascade = syntax.cascade,
    };
}

pub fn createCollationPlanFromSyntax(syntax: *grammar.CreateCollationSyntax) CreateCollationPlan {
    const collation_name = syntax.collation_name;
    syntax.collation_name = "";
    return .{
        .collation_name = collation_name,
        .option_count = syntax.option_count,
    };
}

pub fn renameCollationPlanFromSyntax(syntax: *grammar.RenameCollationSyntax) RenameCollationPlan {
    const collation_name = syntax.collation_name;
    const new_collation_name = syntax.new_collation_name;
    syntax.collation_name = "";
    syntax.new_collation_name = "";
    return .{
        .collation_name = collation_name,
        .new_collation_name = new_collation_name,
    };
}

pub fn dropCollationPlanFromSyntax(syntax: *grammar.DropCollationSyntax) DropCollationPlan {
    const collation_name = syntax.collation_name;
    syntax.collation_name = "";
    return .{
        .collation_name = collation_name,
        .if_exists = syntax.if_exists,
    };
}

pub fn createOperatorPlanFromSyntax(syntax: *grammar.CreateOperatorSyntax) CreateOperatorPlan {
    const operator_name = syntax.operator_name;
    syntax.operator_name = "";
    return .{
        .operator_name = operator_name,
        .option_count = syntax.option_count,
    };
}

pub fn dropOperatorPlanFromSyntax(syntax: *grammar.DropOperatorSyntax) DropOperatorPlan {
    const operator_name = syntax.operator_name;
    syntax.operator_name = "";
    return .{
        .operator_name = operator_name,
        .argument_count = syntax.argument_count,
    };
}

pub fn createAggregatePlanFromSyntax(syntax: *grammar.CreateAggregateSyntax) CreateAggregatePlan {
    const aggregate_name = syntax.aggregate_name;
    syntax.aggregate_name = "";
    return .{
        .aggregate_name = aggregate_name,
        .argument_count = syntax.argument_count,
        .option_count = syntax.option_count,
    };
}

pub fn dropAggregatePlanFromSyntax(syntax: *grammar.DropAggregateSyntax) DropAggregatePlan {
    const aggregate_name = syntax.aggregate_name;
    syntax.aggregate_name = "";
    return .{
        .aggregate_name = aggregate_name,
        .argument_count = syntax.argument_count,
    };
}

pub fn createCastPlanFromSyntax(syntax: *grammar.CreateCastSyntax) CreateCastPlan {
    const source_type = syntax.source_type;
    const target_type = syntax.target_type;
    const function_name = syntax.function_name;
    syntax.source_type = "";
    syntax.target_type = "";
    syntax.function_name = "";
    return .{
        .source_type = source_type,
        .target_type = target_type,
        .function_name = function_name,
        .assignment = syntax.assignment,
    };
}

pub fn dropCastPlanFromSyntax(syntax: *grammar.DropCastSyntax) DropCastPlan {
    const source_type = syntax.source_type;
    const target_type = syntax.target_type;
    syntax.source_type = "";
    syntax.target_type = "";
    return .{
        .source_type = source_type,
        .target_type = target_type,
    };
}

pub fn createMaterializedViewPlanFromSyntax(syntax: *grammar.CreateMaterializedViewSyntax) CreateMaterializedViewPlan {
    const view_name = syntax.view_name;
    const source_table_name = syntax.source_table_name;
    const source_fields = syntax.source_fields;
    const output_fields = syntax.output_fields;
    syntax.view_name = "";
    syntax.source_table_name = "";
    syntax.source_fields = &.{};
    syntax.output_fields = &.{};
    return .{
        .view_name = view_name,
        .source_table_name = source_table_name,
        .source_fields = source_fields,
        .output_fields = output_fields,
        .replace_existing = syntax.replace_existing,
        .if_not_exists = syntax.if_not_exists,
        .populate_on_create = syntax.populate_on_create,
    };
}

pub fn refreshMaterializedViewPlanFromSyntax(syntax: *grammar.RefreshMaterializedViewSyntax) RefreshMaterializedViewPlan {
    const view_name = syntax.view_name;
    syntax.view_name = "";
    return .{
        .view_name = view_name,
        .concurrently = syntax.concurrently,
        .populate = syntax.populate,
    };
}

pub fn dropMaterializedViewPlanFromSyntax(syntax: *grammar.DropMaterializedViewSyntax) DropMaterializedViewPlan {
    const view_name = syntax.view_name;
    syntax.view_name = "";
    return .{
        .view_name = view_name,
        .if_exists = syntax.if_exists,
        .cascade = syntax.cascade,
    };
}

pub fn createViewPlanFromSyntax(syntax: *grammar.CreateViewSyntax) CreateViewPlan {
    const view_name = syntax.view_name;
    const source_table_name = syntax.source_table_name;
    const source_fields = syntax.source_fields;
    const output_fields = syntax.output_fields;
    syntax.view_name = "";
    syntax.source_table_name = "";
    syntax.source_fields = &.{};
    syntax.output_fields = &.{};
    return .{
        .view_name = view_name,
        .source_table_name = source_table_name,
        .source_fields = source_fields,
        .output_fields = output_fields,
        .replace_existing = syntax.replace_existing,
        .if_not_exists = syntax.if_not_exists,
    };
}

pub fn renameViewPlanFromSyntax(syntax: *grammar.RenameViewSyntax) RenameViewPlan {
    const view_name = syntax.view_name;
    const new_view_name = syntax.new_view_name;
    syntax.view_name = "";
    syntax.new_view_name = "";
    return .{
        .view_name = view_name,
        .new_view_name = new_view_name,
    };
}

pub fn dropViewPlanFromSyntax(syntax: *grammar.DropViewSyntax) DropViewPlan {
    const view_name = syntax.view_name;
    syntax.view_name = "";
    return .{
        .view_name = view_name,
        .if_exists = syntax.if_exists,
        .cascade = syntax.cascade,
    };
}

pub fn dropTablePlanFromSyntax(syntax: *grammar.DropTableSyntax) DropTablePlan {
    const table_name = syntax.table_name;
    syntax.table_name = "";
    return .{
        .table_name = table_name,
        .if_exists = syntax.if_exists,
        .cascade = syntax.cascade,
    };
}

pub fn dropIndexPlanFromSyntax(syntax: *grammar.DropIndexSyntax) DropIndexPlan {
    const index_name = syntax.index_name;
    syntax.index_name = "";
    return .{
        .index_name = index_name,
        .if_exists = syntax.if_exists,
    };
}

pub fn dropUpdatePolicyTriggerPlanFromSyntaxAlloc(
    alloc: std.mem.Allocator,
    syntax: *grammar.DropUpdatePolicySyntax,
) !AlterTablePlan {
    const operations = try alloc.alloc(AlterTableOperation, 1);
    var operations_transferred = false;
    errdefer if (!operations_transferred) alloc.free(operations);

    const table_name = syntax.table_name;
    const trigger_name = syntax.trigger_name;
    syntax.table_name = "";
    syntax.trigger_name = "";
    operations[0] = .{ .drop_update_policy = .{
        .trigger_name = trigger_name,
        .if_exists = syntax.if_exists,
    } };

    operations_transferred = true;
    return .{
        .table_name = table_name,
        .operations = operations,
    };
}

pub fn createUpdatePolicyTriggerPlanFromSyntaxAlloc(
    alloc: std.mem.Allocator,
    syntax: *grammar.CreateUpdatePolicySyntax,
) !CreateUpdatePolicyPlan {
    const value_json = try alloc.dupe(u8, "");
    var value_transferred = false;
    errdefer if (!value_transferred) alloc.free(value_json);

    const trigger_name = syntax.trigger_name;
    const table_name = syntax.table_name;
    const column_name = syntax.column_name;
    syntax.trigger_name = "";
    syntax.table_name = "";
    syntax.column_name = "";

    value_transferred = true;
    return .{
        .trigger_name = trigger_name,
        .table_name = table_name,
        .column_name = column_name,
        .on_update_value = .{ .kind = .now_ns, .value_json = value_json },
    };
}

pub fn alterRowSecurityPlanFromSyntaxAlloc(
    alloc: std.mem.Allocator,
    syntax: grammar.RowSecurityAlterSyntax,
) !AlterRowSecurityPlan {
    return .{
        .table_name = try grammar.normalizeSqlObjectIdentifierAlloc(alloc, syntax.table_identifier),
        .enabled = syntax.enabled,
    };
}

pub fn createRowSecurityPolicyPlanFromSyntax(syntax: *grammar.CreateRowSecurityPolicySyntax) CreateRowSecurityPolicyPlan {
    const policy_name = syntax.policy_name;
    const table_name = syntax.table_name;
    const role_targets = syntax.role_targets;
    const predicate = syntax.predicate;
    syntax.policy_name = "";
    syntax.table_name = "";
    syntax.role_targets = &.{};
    return .{
        .policy_name = policy_name,
        .table_name = table_name,
        .role_targets = role_targets,
        .predicate = predicate,
    };
}

pub fn alterRowSecurityPolicyPlanFromSyntax(syntax: *grammar.AlterRowSecurityPolicySyntax) AlterRowSecurityPolicyPlan {
    const policy_name = syntax.policy_name;
    const table_name = syntax.table_name;
    const role_targets = syntax.role_targets;
    const predicate = syntax.predicate;
    syntax.policy_name = "";
    syntax.table_name = "";
    syntax.role_targets = &.{};
    return .{
        .policy_name = policy_name,
        .table_name = table_name,
        .role_targets = role_targets,
        .predicate = predicate,
    };
}

pub fn dropRowSecurityPolicyPlanFromSyntax(syntax: *grammar.DropRowSecurityPolicySyntax) DropRowSecurityPolicyPlan {
    const policy_name = syntax.policy_name;
    const table_name = syntax.table_name;
    syntax.policy_name = "";
    syntax.table_name = "";
    return .{
        .policy_name = policy_name,
        .table_name = table_name,
        .if_exists = syntax.if_exists,
    };
}

pub fn alterTableValidateConstraintOperationFromSyntax(
    syntax: *grammar.AlterTableValidateConstraintSyntax,
) AlterTableOperation {
    const constraint_name = syntax.constraint_name;
    syntax.constraint_name = "";
    return .{ .validate_constraint = constraint_name };
}

pub fn alterTableRenameOperationFromSyntax(
    syntax: *grammar.AlterTableRenameOperationSyntax,
) AlterTableOperation {
    const old_name = syntax.old_name;
    const new_name = syntax.new_name;
    syntax.old_name = "";
    syntax.new_name = "";
    return switch (syntax.target) {
        .column => .{ .rename_column = .{
            .old_name = old_name,
            .new_name = new_name,
        } },
        .constraint => .{ .rename_constraint = .{
            .old_name = old_name,
            .new_name = new_name,
        } },
    };
}

pub fn alterTableDropOperationFromSyntax(
    syntax: *grammar.AlterTableDropOperationSyntax,
) AlterTableOperation {
    const name = syntax.name;
    syntax.name = "";
    return switch (syntax.target) {
        .column => .{ .drop_column = .{
            .name = name,
            .if_exists = syntax.if_exists,
            .dependency_mode = syntax.dependency_mode,
        } },
        .constraint => .{ .drop_constraint = .{
            .name = name,
            .if_exists = syntax.if_exists,
            .dependency_mode = syntax.dependency_mode,
        } },
    };
}

pub fn alterTableColumnNullabilityOperationFromSyntax(
    syntax: *grammar.AlterTableColumnNullabilitySyntax,
) AlterTableOperation {
    const column_name = syntax.column_name;
    syntax.column_name = "";
    return .{ .alter_column_nullability = .{
        .column_name = column_name,
        .nullable = syntax.nullable,
    } };
}

pub fn routineKindFromSyntax(syntax: grammar.RoutineKindSyntax) RoutineKind {
    return switch (syntax) {
        .function => .function,
        .procedure => .procedure,
    };
}

pub fn ddlRangeBoundTypeForName(name: []const u8) ?runtime_schema.AntflyType {
    if (std.ascii.eqlIgnoreCase(name, "daterange")) return .datetime;
    if (std.ascii.eqlIgnoreCase(name, "tsrange")) return .datetime;
    if (std.ascii.eqlIgnoreCase(name, "tstzrange")) return .datetime;
    if (std.ascii.eqlIgnoreCase(name, "numrange")) return .numeric;
    return null;
}

pub fn ddlRangeTypeForName(name: []const u8) ?runtime_schema.RelationalPeriodRangeType {
    if (std.ascii.eqlIgnoreCase(name, "numrange")) return .numrange;
    if (std.ascii.eqlIgnoreCase(name, "daterange")) return .daterange;
    if (std.ascii.eqlIgnoreCase(name, "tsrange")) return .tsrange;
    if (std.ascii.eqlIgnoreCase(name, "tstzrange")) return .tstzrange;
    return null;
}

pub fn relationalPeriodRangeTypeName(range_type: runtime_schema.RelationalPeriodRangeType) []const u8 {
    return switch (range_type) {
        .numrange => "numrange",
        .daterange => "daterange",
        .tsrange => "tsrange",
        .tstzrange => "tstzrange",
    };
}

pub fn relationalCheckOpToken(op: runtime_schema.RelationalCheckOp) []const u8 {
    return switch (op) {
        .is_null => "is_null",
        .is_not_null => "is_not_null",
        .is_distinct => "is_distinct",
        .is_not_distinct => "is_not_distinct",
        .eq => "eq",
        .ne => "ne",
        .gt => "gt",
        .gte => "gte",
        .lt => "lt",
        .lte => "lte",
    };
}

pub fn uniqueConstraintValidationStateString(state: runtime_schema.UniqueConstraintValidationState) []const u8 {
    return switch (state) {
        .enforced => "enforced",
        .unvalidated => "unvalidated",
        .validating => "validating",
        .invalid => "invalid",
    };
}

pub fn relationalCheckValidationStateName(state: runtime_schema.RelationalCheckValidationState) []const u8 {
    return switch (state) {
        .enforced => "enforced",
        .unvalidated => "unvalidated",
        .validating => "validating",
        .invalid => "invalid",
    };
}

pub fn antflyTypeSchemaName(field_type: runtime_schema.AntflyType) []const u8 {
    return switch (field_type) {
        .text => "text",
        .keyword => "keyword",
        .numeric => "numeric",
        .embedding => "embedding",
        .boolean => "boolean",
        .datetime => "datetime",
        .geopoint => "geopoint",
        .geoshape => "geoshape",
        .blob => "blob",
        .html => "html",
        .search_as_you_type => "search_as_you_type",
        .json => "json",
        .array => "array",
        .link => "link",
    };
}

pub fn foreignKeyActionName(action: runtime_schema.ForeignKeyAction) []const u8 {
    return switch (action) {
        .restrict => "restrict",
        .set_null => "set_null",
        .cascade => "cascade",
        .no_action => "no_action",
    };
}

pub fn foreignKeyTimingName(timing: runtime_schema.ForeignKeyTiming) []const u8 {
    return switch (timing) {
        .immediate => "immediate",
        .deferred => "deferred",
    };
}

pub fn foreignKeyMatchName(match: runtime_schema.ForeignKeyMatch) []const u8 {
    return switch (match) {
        .simple => "simple",
        .full => "full",
        .partial => "partial",
    };
}

pub fn foreignKeyValidationStateName(state: runtime_schema.ForeignKeyValidationState) []const u8 {
    return switch (state) {
        .enforced => "enforced",
        .unvalidated => "unvalidated",
        .validating => "validating",
        .invalid => "invalid",
    };
}

pub fn relationalIndexLifecycleName(lifecycle: runtime_schema.RelationalIndexLifecycle) []const u8 {
    return switch (lifecycle) {
        .ready => "ready",
        .building => "building",
        .invalid => "invalid",
        .dropping => "dropping",
    };
}

test "SQL adapter DDL syntax conversions map grammar enums to plan enums" {
    try std.testing.expectEqual(CursorScrollMode.no_scroll, cursorScrollModeFromSyntax(.no_scroll));
    try std.testing.expectEqual(CursorFetchDirection.backward, cursorFetchDirectionFromSyntax(.backward));
    try std.testing.expectEqual(PreparedStatementSubjectKind.write, preparedStatementSubjectKindFromSyntax(.write));
    try std.testing.expectEqual(PreparedStatementStatementKind.insert_source, preparedStatementStatementKindFromSyntax(.insert_source));
    try std.testing.expectEqual(TableLockMode.share_row_exclusive, tableLockModeFromSyntax(.share_row_exclusive));
    try std.testing.expectEqual(ConstraintCheckMode.deferred, constraintCheckModeFromSyntax(.deferred));
    try std.testing.expectEqual(runtime_schema.ForeignKeyAction.cascade, ddlForeignKeyActionFromSyntax(.cascade));
    try std.testing.expectEqual(runtime_schema.ForeignKeyTiming.deferred, ddlForeignKeyTimingFromSyntax(.deferred));
    try std.testing.expectEqual(runtime_schema.ForeignKeyMatch.full, ddlForeignKeyMatchFromSyntax(.full));
    try std.testing.expectEqual(grammar.TransactionModeStarterSyntax.begin, transactionModeStarterToSyntax(.begin));
    try std.testing.expectEqual(TransactionModeStarter.start_transaction, transactionModeStarterFromSyntax(.start_transaction));
    try std.testing.expectEqual(TransactionIsolationLevel.repeatable_read, transactionIsolationLevelFromSyntax(.repeatable_read).?);
    try std.testing.expectEqual(@as(?TransactionIsolationLevel, null), transactionIsolationLevelFromSyntax(null));
    try std.testing.expectEqual(TransactionAccessMode.read_write, transactionAccessModeFromSyntax(.read_write).?);
    try std.testing.expectEqual(@as(?TransactionAccessMode, null), transactionAccessModeFromSyntax(null));
    try std.testing.expectEqual(AdvisoryLockAction.unlock, advisoryLockActionFromSyntax(.unlock));
    try std.testing.expectEqual(ReindexMaintenanceTarget.system, reindexMaintenanceTargetFromSyntax(.system));
    try std.testing.expectEqual(BulkIoDirection.to, bulkIoDirectionFromSyntax(.to));
    try std.testing.expectEqual(grammar.PrivilegeChangeActionSyntax.grant, privilegeChangeActionToSyntax(.grant));
    try std.testing.expectEqual(RoutineKind.procedure, routineKindFromSyntax(.procedure));
    try std.testing.expectEqual(runtime_schema.AntflyType.datetime, ddlRangeBoundTypeForName("tstzrange").?);
    try std.testing.expectEqual(runtime_schema.AntflyType.numeric, ddlRangeBoundTypeForName("numrange").?);
    try std.testing.expectEqual(runtime_schema.RelationalPeriodRangeType.tsrange, ddlRangeTypeForName("tsrange").?);
    try std.testing.expectEqualStrings("daterange", relationalPeriodRangeTypeName(.daterange));
    try std.testing.expectEqualStrings("is_not_distinct", relationalCheckOpToken(.is_not_distinct));
    try std.testing.expectEqualStrings("validating", uniqueConstraintValidationStateString(.validating));
    try std.testing.expectEqualStrings("invalid", relationalCheckValidationStateName(.invalid));
    try std.testing.expectEqualStrings("search_as_you_type", antflyTypeSchemaName(.search_as_you_type));
    try std.testing.expectEqualStrings("set_null", foreignKeyActionName(.set_null));
    try std.testing.expectEqualStrings("deferred", foreignKeyTimingName(.deferred));
    try std.testing.expectEqualStrings("partial", foreignKeyMatchName(.partial));
    try std.testing.expectEqualStrings("unvalidated", foreignKeyValidationStateName(.unvalidated));
    try std.testing.expectEqualStrings("dropping", relationalIndexLifecycleName(.dropping));
    try std.testing.expect(isAdapterNoopExtensionName("pgcrypto"));
    try std.testing.expect(isAdapterNoopExtensionName("uuid-ossp"));
    try std.testing.expect(!isAdapterNoopExtensionName("postgis"));

    const alloc = std.testing.allocator;
    var syntax: grammar.CreateExtensionSyntax = .{
        .extension_name = try alloc.dupe(u8, "pgcrypto"),
        .schema_name = try alloc.dupe(u8, "public"),
        .version = try alloc.dupe(u8, "1.3"),
        .if_not_exists = true,
    };
    defer syntax.deinit(alloc);
    var plan = try createExtensionPlanFromSyntax(&syntax, "public");
    defer plan.deinit(alloc);
    try std.testing.expectEqualStrings("pgcrypto", plan.extension_name);
    try std.testing.expectEqualStrings("1.3", plan.version.?);
    try std.testing.expect(plan.if_not_exists);

    var unsupported_schema: grammar.CreateExtensionSyntax = .{
        .extension_name = try alloc.dupe(u8, "pgcrypto"),
        .schema_name = try alloc.dupe(u8, "tenant"),
        .if_not_exists = true,
    };
    defer unsupported_schema.deinit(alloc);
    try std.testing.expectError(error.UnsupportedSqlShape, createExtensionPlanFromSyntax(&unsupported_schema, "public"));

    var create_schema_syntax: grammar.CreateSchemaNamespaceSyntax = .{
        .schema_name = try alloc.dupe(u8, "tenant"),
        .if_not_exists = true,
    };
    var create_schema = createSchemaNamespacePlanFromSyntax(&create_schema_syntax);
    defer create_schema.deinit(alloc);
    try std.testing.expectEqualStrings("tenant", create_schema.schema_name);
    try std.testing.expect(create_schema.if_not_exists);

    var alter_database_syntax: grammar.AlterDatabaseSyntax = .{
        .database_name = try alloc.dupe(u8, "app"),
        .setting_name = try alloc.dupe(u8, "statement_timeout"),
        .value_json = try alloc.dupe(u8, "5000"),
    };
    var alter_database = try alterDatabasePlanFromSyntaxAlloc(alloc, &alter_database_syntax);
    defer alter_database.deinit(alloc);
    try std.testing.expectEqualStrings("app", alter_database.database_name);
    try std.testing.expectEqual(@as(usize, 1), alter_database.operations.len);
    switch (alter_database.operations[0]) {
        .set_parameter => |parameter| {
            try std.testing.expectEqualStrings("statement_timeout", parameter.name);
            try std.testing.expectEqualStrings("5000", parameter.value_json);
        },
    }

    var notify_syntax: grammar.NotifyNotificationSyntax = .{
        .channel_name = try alloc.dupe(u8, "jobs"),
        .payload_json = try alloc.dupe(u8, "\"ready\""),
    };
    var notify = notifyNotificationPlanFromSyntax(&notify_syntax);
    defer notify.deinit(alloc);
    try std.testing.expectEqualStrings("jobs", notify.channel_name);
    try std.testing.expectEqualStrings("\"ready\"", notify.payload_json.?);

    var publication_tables = try alloc.alloc([]const u8, 1);
    publication_tables[0] = try alloc.dupe(u8, "usage_records");
    var publication_syntax: grammar.CreatePublicationSyntax = .{
        .publication_name = try alloc.dupe(u8, "usage_pub"),
        .table_names = publication_tables,
    };
    var publication = createPublicationPlanFromSyntax(&publication_syntax);
    defer publication.deinit(alloc);
    try std.testing.expectEqualStrings("usage_pub", publication.publication_name);
    try std.testing.expectEqualStrings("usage_records", publication.table_names[0]);

    var subscription_publications = try alloc.alloc([]const u8, 1);
    subscription_publications[0] = try alloc.dupe(u8, "usage_pub");
    var subscription_syntax: grammar.CreateSubscriptionSyntax = .{
        .subscription_name = try alloc.dupe(u8, "usage_sub"),
        .connection_json = try alloc.dupe(u8, "\"host=primary\""),
        .publication_names = subscription_publications,
    };
    var subscription = createSubscriptionPlanFromSyntax(&subscription_syntax);
    defer subscription.deinit(alloc);
    try std.testing.expectEqualStrings("usage_sub", subscription.subscription_name);
    try std.testing.expectEqualStrings("\"host=primary\"", subscription.connection_json);
    try std.testing.expectEqualStrings("usage_pub", subscription.publication_names[0]);

    var vacuum_syntax: grammar.VacuumMaintenanceSyntax = .{
        .table_name = try alloc.dupe(u8, "usage_records"),
        .full = true,
        .analyze = true,
    };
    var vacuum = vacuumMaintenancePlanFromSyntax(&vacuum_syntax);
    defer vacuum.deinit(alloc);
    try std.testing.expectEqualStrings("usage_records", vacuum.table_name);
    try std.testing.expect(vacuum.full);
    try std.testing.expect(vacuum.analyze);

    const prepare = try prepareStatementPlanFromSyntaxAlloc(alloc, .{
        .statement_name = "usage_plan",
        .parameter_count = 2,
        .statement_kind = .read,
        .statement_family = .read,
    });
    var mutable_prepare = prepare;
    defer mutable_prepare.deinit(alloc);
    try std.testing.expectEqualStrings("usage_plan", mutable_prepare.statement_name);
    try std.testing.expectEqual(@as(usize, 2), mutable_prepare.parameter_count);

    const declare = try declareCursorPortalPlanFromSyntaxAlloc(alloc, .{
        .portal_name = "usage_cursor",
        .scroll = .scroll,
        .hold = true,
        .statement_kind = .read,
    });
    var mutable_declare = declare;
    defer mutable_declare.deinit(alloc);
    try std.testing.expectEqualStrings("usage_cursor", mutable_declare.portal_name);
    try std.testing.expectEqual(CursorScrollMode.scroll, mutable_declare.scroll);
    try std.testing.expect(mutable_declare.hold);

    var comment_syntax: grammar.CommentMetadataSyntax = .{
        .target = .column,
        .object_name = try alloc.dupe(u8, "amount"),
        .parent_table_name = try alloc.dupe(u8, "usage_records"),
        .comment_json = try alloc.dupe(u8, "\"billable amount\""),
    };
    var comment = commentMetadataPlanFromSyntax(&comment_syntax);
    defer comment.deinit(alloc);
    try std.testing.expectEqual(CommentMetadataTarget.column, comment.target);
    try std.testing.expectEqualStrings("amount", comment.object_name);
    try std.testing.expectEqualStrings("usage_records", comment.parent_table_name.?);

    var lock_tables = try alloc.alloc([]const u8, 1);
    lock_tables[0] = try alloc.dupe(u8, "usage_records");
    var lock_syntax: grammar.TableLockSyntax = .{
        .table_names = lock_tables,
        .mode = .share,
    };
    var table_lock = tableLockPlanFromSyntax(&lock_syntax);
    defer table_lock.deinit(alloc);
    try std.testing.expectEqual(TableLockMode.share, table_lock.mode);
    try std.testing.expectEqualStrings("usage_records", table_lock.table_names[0]);

    var constraint_names = try alloc.alloc([]const u8, 1);
    constraint_names[0] = try alloc.dupe(u8, "usage_tenant_fk");
    var constraint_syntax: grammar.ConstraintModeSyntax = .{
        .constraint_names = constraint_names,
        .mode = .deferred,
    };
    var constraint_mode = constraintModePlanFromSyntax(&constraint_syntax);
    defer constraint_mode.deinit(alloc);
    try std.testing.expectEqual(ConstraintCheckMode.deferred, constraint_mode.mode);
    try std.testing.expectEqualStrings("usage_tenant_fk", constraint_mode.constraint_names[0]);

    const transaction_mode = transactionModePlanFromSyntax(.{
        .starter = .start_transaction,
        .isolation_level = .serializable,
        .access_mode = .read_only,
        .deferrable = true,
    });
    try std.testing.expectEqual(TransactionModeStarter.start_transaction, transaction_mode.starter);
    try std.testing.expectEqual(TransactionIsolationLevel.serializable, transaction_mode.isolation_level.?);
    try std.testing.expectEqual(TransactionAccessMode.read_only, transaction_mode.access_mode.?);
    try std.testing.expect(transaction_mode.deferrable.?);

    const advisory_lock = advisoryLockPlanFromSyntax(.{
        .action = .lock,
        .key1 = 42,
        .key2 = 7,
    });
    try std.testing.expectEqual(AdvisoryLockAction.lock, advisory_lock.action);
    try std.testing.expectEqual(@as(i64, 42), advisory_lock.key1);
    try std.testing.expectEqual(@as(i64, 7), advisory_lock.key2.?);

    var validate_syntax: grammar.AlterTableValidateConstraintSyntax = .{
        .constraint_name = try alloc.dupe(u8, "usage_amount_check"),
    };
    const validate_operation = alterTableValidateConstraintOperationFromSyntax(&validate_syntax);
    defer freeAlterTableOperation(alloc, validate_operation);
    switch (validate_operation) {
        .validate_constraint => |constraint_name| try std.testing.expectEqualStrings("usage_amount_check", constraint_name),
        else => return error.TestExpectedEqual,
    }

    var rename_syntax: grammar.AlterTableRenameOperationSyntax = .{
        .target = .constraint,
        .old_name = try alloc.dupe(u8, "usage_amount_check"),
        .new_name = try alloc.dupe(u8, "usage_amount_positive"),
    };
    const rename_operation = alterTableRenameOperationFromSyntax(&rename_syntax);
    defer freeAlterTableOperation(alloc, rename_operation);
    switch (rename_operation) {
        .rename_constraint => |rename| {
            try std.testing.expectEqualStrings("usage_amount_check", rename.old_name);
            try std.testing.expectEqualStrings("usage_amount_positive", rename.new_name);
        },
        else => return error.TestExpectedEqual,
    }

    var drop_syntax: grammar.AlterTableDropOperationSyntax = .{
        .target = .column,
        .name = try alloc.dupe(u8, "legacy_amount"),
        .if_exists = true,
        .dependency_mode = .restrict,
    };
    const drop_operation = alterTableDropOperationFromSyntax(&drop_syntax);
    defer freeAlterTableOperation(alloc, drop_operation);
    switch (drop_operation) {
        .drop_column => |drop| {
            try std.testing.expectEqualStrings("legacy_amount", drop.name);
            try std.testing.expect(drop.if_exists);
            try std.testing.expectEqual(DropDependencyMode.restrict, drop.dependency_mode);
        },
        else => return error.TestExpectedEqual,
    }

    var nullability_syntax: grammar.AlterTableColumnNullabilitySyntax = .{
        .column_name = try alloc.dupe(u8, "amount"),
        .nullable = false,
    };
    const nullability_operation = alterTableColumnNullabilityOperationFromSyntax(&nullability_syntax);
    defer freeAlterTableOperation(alloc, nullability_operation);
    switch (nullability_operation) {
        .alter_column_nullability => |nullability| {
            try std.testing.expectEqualStrings("amount", nullability.column_name);
            try std.testing.expect(!nullability.nullable);
        },
        else => return error.TestExpectedEqual,
    }

    var sequence_syntax: grammar.CreateSequenceSyntax = .{
        .sequence_name = try alloc.dupe(u8, "usage_seq"),
        .if_not_exists = true,
        .options = .{ .start_with = 10 },
    };
    var sequence = createSequencePlanFromSyntax(&sequence_syntax);
    defer sequence.deinit(alloc);
    try std.testing.expectEqualStrings("usage_seq", sequence.sequence_name);
    try std.testing.expect(sequence.if_not_exists);
    try std.testing.expectEqual(@as(i64, 10), sequence.options.start_with.?);

    var enum_values = try alloc.alloc([]const u8, 2);
    enum_values[0] = try alloc.dupe(u8, "queued");
    enum_values[1] = try alloc.dupe(u8, "running");
    var enum_syntax: grammar.CreateEnumTypeSyntax = .{
        .type_name = try alloc.dupe(u8, "job_state"),
        .values = enum_values,
    };
    var enum_plan = createEnumTypePlanFromSyntax(&enum_syntax);
    defer enum_plan.deinit(alloc);
    try std.testing.expectEqualStrings("job_state", enum_plan.type_name);
    try std.testing.expectEqualStrings("running", enum_plan.values[1]);

    var collation_syntax: grammar.RenameCollationSyntax = .{
        .collation_name = try alloc.dupe(u8, "old_collation"),
        .new_collation_name = try alloc.dupe(u8, "new_collation"),
    };
    var collation = renameCollationPlanFromSyntax(&collation_syntax);
    defer collation.deinit(alloc);
    try std.testing.expectEqualStrings("old_collation", collation.collation_name);
    try std.testing.expectEqualStrings("new_collation", collation.new_collation_name);

    var cast_syntax: grammar.CreateCastSyntax = .{
        .source_type = try alloc.dupe(u8, "text"),
        .target_type = try alloc.dupe(u8, "jsonb"),
        .function_name = try alloc.dupe(u8, "text_to_jsonb"),
        .assignment = true,
    };
    var cast_plan = createCastPlanFromSyntax(&cast_syntax);
    defer cast_plan.deinit(alloc);
    try std.testing.expectEqualStrings("text", cast_plan.source_type);
    try std.testing.expectEqualStrings("jsonb", cast_plan.target_type);
    try std.testing.expect(cast_plan.assignment);

    var view_fields = try alloc.alloc([]const u8, 1);
    view_fields[0] = try alloc.dupe(u8, "tenant_id");
    var view_outputs = try alloc.alloc([]const u8, 1);
    view_outputs[0] = try alloc.dupe(u8, "tenant_id");
    var view_syntax: grammar.CreateViewSyntax = .{
        .view_name = try alloc.dupe(u8, "active_usage"),
        .source_table_name = try alloc.dupe(u8, "usage_records"),
        .source_fields = view_fields,
        .output_fields = view_outputs,
        .replace_existing = true,
    };
    var view_plan = createViewPlanFromSyntax(&view_syntax);
    defer view_plan.deinit(alloc);
    try std.testing.expectEqualStrings("active_usage", view_plan.view_name);
    try std.testing.expectEqualStrings("usage_records", view_plan.source_table_name);
    try std.testing.expectEqualStrings("tenant_id", view_plan.output_fields[0]);
    try std.testing.expect(view_plan.replace_existing);

    var materialized_syntax: grammar.CreateMaterializedViewSyntax = .{
        .view_name = try alloc.dupe(u8, "usage_rollup"),
        .source_table_name = try alloc.dupe(u8, "usage_records"),
        .if_not_exists = true,
        .populate_on_create = false,
    };
    var materialized_plan = createMaterializedViewPlanFromSyntax(&materialized_syntax);
    defer materialized_plan.deinit(alloc);
    try std.testing.expectEqualStrings("usage_rollup", materialized_plan.view_name);
    try std.testing.expect(materialized_plan.if_not_exists);
    try std.testing.expect(!materialized_plan.populate_on_create);

    var drop_table_syntax: grammar.DropTableSyntax = .{
        .table_name = try alloc.dupe(u8, "usage_records"),
        .if_exists = true,
        .cascade = true,
    };
    var drop_table = dropTablePlanFromSyntax(&drop_table_syntax);
    defer drop_table.deinit(alloc);
    try std.testing.expectEqualStrings("usage_records", drop_table.table_name);
    try std.testing.expect(drop_table.if_exists);
    try std.testing.expect(drop_table.cascade);

    var drop_index_syntax: grammar.DropIndexSyntax = .{
        .index_name = try alloc.dupe(u8, "usage_tenant_idx"),
        .if_exists = true,
    };
    var drop_index = dropIndexPlanFromSyntax(&drop_index_syntax);
    defer drop_index.deinit(alloc);
    try std.testing.expectEqualStrings("usage_tenant_idx", drop_index.index_name);
    try std.testing.expect(drop_index.if_exists);

    var create_policy_syntax: grammar.CreateUpdatePolicySyntax = .{
        .trigger_name = try alloc.dupe(u8, "usage_updated_at"),
        .table_name = try alloc.dupe(u8, "usage_records"),
        .column_name = try alloc.dupe(u8, "updated_at"),
    };
    var create_policy = try createUpdatePolicyTriggerPlanFromSyntaxAlloc(alloc, &create_policy_syntax);
    defer create_policy.deinit(alloc);
    try std.testing.expectEqualStrings("usage_updated_at", create_policy.trigger_name);
    try std.testing.expectEqualStrings("usage_records", create_policy.table_name);
    try std.testing.expectEqualStrings("updated_at", create_policy.column_name);
    try std.testing.expectEqual(runtime_schema.RelationalDefaultKind.now_ns, create_policy.on_update_value.kind);

    var drop_policy_syntax: grammar.DropUpdatePolicySyntax = .{
        .trigger_name = try alloc.dupe(u8, "usage_updated_at"),
        .table_name = try alloc.dupe(u8, "usage_records"),
        .if_exists = true,
    };
    var drop_policy = try dropUpdatePolicyTriggerPlanFromSyntaxAlloc(alloc, &drop_policy_syntax);
    defer drop_policy.deinit(alloc);
    try std.testing.expectEqualStrings("usage_records", drop_policy.table_name);
    try std.testing.expectEqual(@as(usize, 1), drop_policy.operations.len);
    switch (drop_policy.operations[0]) {
        .drop_update_policy => |operation| {
            try std.testing.expectEqualStrings("usage_updated_at", operation.trigger_name);
            try std.testing.expect(operation.if_exists);
        },
        else => return error.TestExpectedEqual,
    }

    var alter_row_security = try alterRowSecurityPlanFromSyntaxAlloc(alloc, .{
        .table_identifier = "public.usage_records",
        .enabled = true,
    });
    defer alter_row_security.deinit(alloc);
    try std.testing.expectEqualStrings("usage_records", alter_row_security.table_name);
    try std.testing.expect(alter_row_security.enabled);

    var policy_roles = try alloc.alloc([]const u8, 1);
    policy_roles[0] = try alloc.dupe(u8, "tenant_reader");
    var create_row_policy_syntax: grammar.CreateRowSecurityPolicySyntax = .{
        .policy_name = try alloc.dupe(u8, "tenant_visible"),
        .table_name = try alloc.dupe(u8, "usage_records"),
        .role_targets = policy_roles,
        .predicate = .{ .literal_equals = .{
            .field = try alloc.dupe(u8, "tenant_id"),
            .value_json = try alloc.dupe(u8, "\"tenant_a\""),
        } },
    };
    var create_row_policy = createRowSecurityPolicyPlanFromSyntax(&create_row_policy_syntax);
    defer create_row_policy.deinit(alloc);
    try std.testing.expectEqualStrings("tenant_visible", create_row_policy.policy_name);
    try std.testing.expectEqualStrings("tenant_reader", create_row_policy.role_targets[0]);
    switch (create_row_policy.predicate) {
        .literal_equals => |predicate| try std.testing.expectEqualStrings("tenant_id", predicate.field),
        else => return error.TestExpectedEqual,
    }

    var alter_row_policy_syntax: grammar.AlterRowSecurityPolicySyntax = .{
        .policy_name = try alloc.dupe(u8, "tenant_visible"),
        .table_name = try alloc.dupe(u8, "usage_records"),
        .predicate = .{ .literal_equals = .{
            .field = try alloc.dupe(u8, "region"),
            .value_json = try alloc.dupe(u8, "\"us\""),
        } },
    };
    var alter_row_policy = alterRowSecurityPolicyPlanFromSyntax(&alter_row_policy_syntax);
    defer alter_row_policy.deinit(alloc);
    try std.testing.expectEqualStrings("tenant_visible", alter_row_policy.policy_name);
    switch (alter_row_policy.predicate) {
        .literal_equals => |predicate| try std.testing.expectEqualStrings("region", predicate.field),
        else => return error.TestExpectedEqual,
    }

    var drop_row_policy_syntax: grammar.DropRowSecurityPolicySyntax = .{
        .policy_name = try alloc.dupe(u8, "tenant_visible"),
        .table_name = try alloc.dupe(u8, "usage_records"),
        .if_exists = true,
    };
    var drop_row_policy = dropRowSecurityPolicyPlanFromSyntax(&drop_row_policy_syntax);
    defer drop_row_policy.deinit(alloc);
    try std.testing.expectEqualStrings("tenant_visible", drop_row_policy.policy_name);
    try std.testing.expect(drop_row_policy.if_exists);
}

fn freeStringSlice(alloc: std.mem.Allocator, values: []const []const u8) void {
    for (values) |value| alloc.free(value);
    if (values.len > 0) alloc.free(values);
}

fn cloneStringSlice(alloc: std.mem.Allocator, values: []const []const u8) ![]const []const u8 {
    if (values.len == 0) return &.{};
    const out = try alloc.alloc([]const u8, values.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |value| alloc.free(value);
        alloc.free(out);
    }
    for (values, 0..) |value, i| {
        out[i] = try alloc.dupe(u8, value);
        initialized += 1;
    }
    return out;
}

pub fn cloneEmptyRuntimeSchemaAlloc(alloc: std.mem.Allocator, current: runtime_schema.TableSchema) !runtime_schema.TableSchema {
    const default_type = try alloc.dupe(u8, current.default_type);
    const ttl_field = alloc.dupe(u8, current.ttl_field) catch |err| {
        alloc.free(default_type);
        return err;
    };
    return .{
        .version = current.version,
        .default_type = default_type,
        .ttl_duration_ns = current.ttl_duration_ns,
        .ttl_field = ttl_field,
        .enforce_types = current.enforce_types,
        .storage_mode = current.storage_mode,
    };
}

pub fn cloneRelationalRuntimeSchemaAlloc(alloc: std.mem.Allocator, current: runtime_schema.TableSchema) !runtime_schema.TableSchema {
    if (current.storage_mode != .relational) return error.InvalidSqlCatalog;
    if (current.dynamic_templates.len != 0 or current.full_text_documents.len != 0) return error.UnsupportedSqlShape;

    var schema = try cloneEmptyRuntimeSchemaAlloc(alloc, current);
    errdefer runtime_schema.freeSchema(alloc, schema);
    schema.relational_columns = try cloneDdlRelationalColumns(alloc, current.relational_columns);
    schema.primary_key = try cloneDdlPrimaryKeyMaybe(alloc, current.primary_key);
    schema.periods = try cloneDdlPeriods(alloc, current.periods);
    schema.foreign_keys = try cloneDdlForeignKeys(alloc, current.foreign_keys);
    schema.unique_constraints = try cloneDdlUniqueConstraints(alloc, current.unique_constraints);
    schema.checks = try cloneDdlRelationalChecks(alloc, current.checks);
    return schema;
}

pub fn appendRelationalColumnAlloc(
    alloc: std.mem.Allocator,
    schema: *runtime_schema.TableSchema,
    column: runtime_schema.RelationalColumn,
) !void {
    if (binder.relationalColumnIndex(schema.relational_columns, column.name) != null) return error.InvalidSqlCatalog;
    const len = schema.relational_columns.len;
    const out = try alloc.alloc(runtime_schema.RelationalColumn, len + 1);
    errdefer alloc.free(out);
    @memcpy(out[0..len], schema.relational_columns);
    out[len] = try cloneDdlRelationalColumn(alloc, column);
    if (len > 0) alloc.free(schema.relational_columns);
    schema.relational_columns = out;
}

pub fn appendUniqueConstraintAlloc(
    alloc: std.mem.Allocator,
    schema: *runtime_schema.TableSchema,
    constraint: runtime_schema.UniqueConstraint,
) !void {
    if (binder.uniqueConstraintNameExists(schema.unique_constraints, constraint.name)) return error.InvalidSqlCatalog;
    const len = schema.unique_constraints.len;
    const out = try alloc.alloc(runtime_schema.UniqueConstraint, len + 1);
    errdefer alloc.free(out);
    @memcpy(out[0..len], schema.unique_constraints);
    out[len] = try cloneDdlUniqueConstraint(alloc, constraint);
    if (len > 0) alloc.free(schema.unique_constraints);
    schema.unique_constraints = out;
}

pub fn appendForeignKeyAlloc(
    alloc: std.mem.Allocator,
    schema: *runtime_schema.TableSchema,
    foreign_key: runtime_schema.ForeignKey,
) !void {
    if (binder.foreignKeyNameExists(schema.foreign_keys, foreign_key.name)) return error.InvalidSqlCatalog;
    const len = schema.foreign_keys.len;
    const out = try alloc.alloc(runtime_schema.ForeignKey, len + 1);
    errdefer alloc.free(out);
    @memcpy(out[0..len], schema.foreign_keys);
    out[len] = try cloneDdlForeignKey(alloc, foreign_key);
    if (len > 0) alloc.free(schema.foreign_keys);
    schema.foreign_keys = out;
}

pub fn appendRelationalCheckAlloc(
    alloc: std.mem.Allocator,
    schema: *runtime_schema.TableSchema,
    check: runtime_schema.RelationalCheck,
) !void {
    if (binder.relationalCheckNameExists(schema.checks, check.name)) return error.InvalidSqlCatalog;
    const len = schema.checks.len;
    const out = try alloc.alloc(runtime_schema.RelationalCheck, len + 1);
    errdefer alloc.free(out);
    @memcpy(out[0..len], schema.checks);
    out[len] = try cloneDdlRelationalCheck(alloc, check);
    if (len > 0) alloc.free(schema.checks);
    schema.checks = out;
}

pub fn renameRelationalColumnAlloc(
    alloc: std.mem.Allocator,
    schema: *runtime_schema.TableSchema,
    operation: RenameColumnOperation,
) !void {
    if (std.mem.eql(u8, operation.old_name, operation.new_name)) return error.InvalidSqlCatalog;
    const index = binder.relationalColumnIndex(schema.relational_columns, operation.old_name) orelse return error.InvalidSqlCatalog;
    if (binder.relationalColumnIndex(schema.relational_columns, operation.new_name) != null) return error.InvalidSqlCatalog;

    const columns = @constCast(schema.relational_columns);
    const new_name = try alloc.dupe(u8, operation.new_name);
    errdefer alloc.free(new_name);
    const new_path = try alloc.dupe(u8, operation.new_name);
    errdefer alloc.free(new_path);
    alloc.free(columns[index].name);
    alloc.free(columns[index].path);
    columns[index].name = new_name;
    columns[index].path = new_path;

    for (columns) |*column| {
        if (column.generated) |*generated| try renameGeneratedValueFieldsAlloc(alloc, generated, operation.old_name, operation.new_name);
        try renameUniquePredicatesAlloc(alloc, column.index_where, operation.old_name, operation.new_name);
        try renameExpressionConditionsAlloc(alloc, column.index_where_expressions, operation.old_name, operation.new_name);
    }
    if (schema.primary_key) |*primary_key| try renameStringSliceValuesAlloc(alloc, primary_key.columns, operation.old_name, operation.new_name);
    if (schema.primary_key) |*primary_key| try renameStringSliceValuesAlloc(alloc, primary_key.include_columns, operation.old_name, operation.new_name);
    for (@constCast(schema.unique_constraints)) |*constraint| {
        try renameStringSliceValuesAlloc(alloc, constraint.columns, operation.old_name, operation.new_name);
        for (@constCast(constraint.expressions)) |*expression| {
            try replaceOwnedStringIfEqualAlloc(alloc, &expression.field, operation.old_name, operation.new_name);
            if (expression.expression) |*row_expression| try renameRowExpressionFieldsAlloc(alloc, row_expression, operation.old_name, operation.new_name);
        }
        try renameStringSliceValuesAlloc(alloc, constraint.include_columns, operation.old_name, operation.new_name);
        try renameUniquePredicatesAlloc(alloc, constraint.where, operation.old_name, operation.new_name);
        try renameExpressionConditionsAlloc(alloc, constraint.where_expressions, operation.old_name, operation.new_name);
    }
    for (@constCast(schema.foreign_keys)) |*foreign_key| {
        try renameStringSliceValuesAlloc(alloc, foreign_key.child_columns, operation.old_name, operation.new_name);
    }
    for (@constCast(schema.checks)) |*check| {
        try replaceOwnedStringIfEqualAlloc(alloc, &check.field, operation.old_name, operation.new_name);
        if (check.expression) |*expression| try renameExpressionConditionAlloc(alloc, expression, operation.old_name, operation.new_name);
    }
}

pub fn renameRelationalConstraintAlloc(
    alloc: std.mem.Allocator,
    schema: *runtime_schema.TableSchema,
    table_name: []const u8,
    operation: RenameConstraintOperation,
) !void {
    if (std.mem.eql(u8, operation.old_name, operation.new_name)) return error.InvalidSqlCatalog;
    if (binder.relationalConstraintNameExists(schema.*, table_name, operation.new_name)) return error.InvalidSqlCatalog;
    if (try renamePrimaryKeyConstraintByNameAlloc(alloc, schema, table_name, operation.old_name, operation.new_name)) return;
    if (try renameUniqueConstraintByNameAlloc(alloc, schema, operation.old_name, operation.new_name)) return;
    if (try renameForeignKeyByNameAlloc(alloc, schema, operation.old_name, operation.new_name)) return;
    if (try renameRelationalCheckByNameAlloc(alloc, schema, operation.old_name, operation.new_name)) return;
    return error.InvalidSqlCatalog;
}

fn renameGeneratedValueFieldsAlloc(
    alloc: std.mem.Allocator,
    generated: *runtime_schema.RelationalGeneratedValue,
    old_name: []const u8,
    new_name: []const u8,
) !void {
    if (generated.field) |*field| try replaceOwnedStringIfEqualAlloc(alloc, field, old_name, new_name);
    try renameStringSliceValuesAlloc(alloc, generated.fields, old_name, new_name);
    if (generated.expression) |*expression| try renameRowExpressionFieldsAlloc(alloc, expression, old_name, new_name);
}

fn renameUniquePredicatesAlloc(
    alloc: std.mem.Allocator,
    predicates: []const runtime_schema.UniquePredicate,
    old_name: []const u8,
    new_name: []const u8,
) !void {
    for (@constCast(predicates)) |*predicate| {
        try replaceOwnedStringIfEqualAlloc(alloc, &predicate.field, old_name, new_name);
    }
}

fn renameExpressionConditionsAlloc(
    alloc: std.mem.Allocator,
    conditions: []const db_mod.types.RelationalRowsExpressionCondition,
    old_name: []const u8,
    new_name: []const u8,
) !void {
    for (@constCast(conditions)) |*condition| {
        try renameExpressionConditionAlloc(alloc, condition, old_name, new_name);
    }
}

fn renameExpressionConditionAlloc(
    alloc: std.mem.Allocator,
    condition: *db_mod.types.RelationalRowsExpressionCondition,
    old_name: []const u8,
    new_name: []const u8,
) anyerror!void {
    try renameRowExpressionFieldsAlloc(alloc, &condition.lhs, old_name, new_name);
    for (@constCast(condition.rhs)) |*rhs| {
        try renameRowExpressionFieldsAlloc(alloc, rhs, old_name, new_name);
    }
}

fn renameRowExpressionFieldsAlloc(
    alloc: std.mem.Allocator,
    expression: *db_mod.types.RelationalRowsExpression,
    old_name: []const u8,
    new_name: []const u8,
) anyerror!void {
    if (expression.field_source == .row) try replaceOwnedStringIfEqualAlloc(alloc, &expression.field, old_name, new_name);
    for (@constCast(expression.operands)) |*operand| {
        try renameRowExpressionFieldsAlloc(alloc, operand, old_name, new_name);
    }
    for (@constCast(expression.case_branches)) |*branch| {
        try renameExpressionConditionAlloc(alloc, &branch.when, old_name, new_name);
        try renameRowExpressionFieldsAlloc(alloc, &branch.then, old_name, new_name);
    }
    for (@constCast(expression.case_else)) |*case_else| {
        try renameRowExpressionFieldsAlloc(alloc, case_else, old_name, new_name);
    }
}

fn renameStringSliceValuesAlloc(
    alloc: std.mem.Allocator,
    values: []const []const u8,
    old_name: []const u8,
    new_name: []const u8,
) !void {
    for (@constCast(values)) |*value| {
        try replaceOwnedStringIfEqualAlloc(alloc, value, old_name, new_name);
    }
}

fn renamePrimaryKeyConstraintByNameAlloc(
    alloc: std.mem.Allocator,
    schema: *runtime_schema.TableSchema,
    table_name: []const u8,
    old_name: []const u8,
    new_name: []const u8,
) !bool {
    if (schema.primary_key) |*primary_key| {
        if (primary_key.name) |*name| {
            if (!std.mem.eql(u8, name.*, old_name)) return false;
            try replaceOwnedStringIfEqualAlloc(alloc, name, old_name, new_name);
            return true;
        }
        if (!binder.defaultPrimaryKeyNameEquals(table_name, old_name)) return false;
        primary_key.name = try alloc.dupe(u8, new_name);
        return true;
    }
    return false;
}

fn renameUniqueConstraintByNameAlloc(
    alloc: std.mem.Allocator,
    schema: *runtime_schema.TableSchema,
    old_name: []const u8,
    new_name: []const u8,
) !bool {
    for (@constCast(schema.unique_constraints)) |*constraint| {
        if (!std.mem.eql(u8, constraint.name, old_name)) continue;
        try replaceOwnedStringIfEqualAlloc(alloc, &constraint.name, old_name, new_name);
        return true;
    }
    return false;
}

fn renameForeignKeyByNameAlloc(
    alloc: std.mem.Allocator,
    schema: *runtime_schema.TableSchema,
    old_name: []const u8,
    new_name: []const u8,
) !bool {
    for (@constCast(schema.foreign_keys)) |*foreign_key| {
        if (!std.mem.eql(u8, foreign_key.name, old_name)) continue;
        try replaceOwnedStringIfEqualAlloc(alloc, &foreign_key.name, old_name, new_name);
        return true;
    }
    return false;
}

fn renameRelationalCheckByNameAlloc(
    alloc: std.mem.Allocator,
    schema: *runtime_schema.TableSchema,
    old_name: []const u8,
    new_name: []const u8,
) !bool {
    for (@constCast(schema.checks)) |*check| {
        if (!std.mem.eql(u8, check.name, old_name)) continue;
        try replaceOwnedStringIfEqualAlloc(alloc, &check.name, old_name, new_name);
        return true;
    }
    return false;
}

fn replaceOwnedStringIfEqualAlloc(
    alloc: std.mem.Allocator,
    value: *[]const u8,
    old_name: []const u8,
    new_name: []const u8,
) !void {
    if (!std.mem.eql(u8, value.*, old_name)) return;
    const replacement = try alloc.dupe(u8, new_name);
    alloc.free(value.*);
    value.* = replacement;
}

pub fn dropRelationalColumnAlloc(
    alloc: std.mem.Allocator,
    schema: *runtime_schema.TableSchema,
    drop_column: DropColumnOperation,
) !void {
    if (binder.relationalColumnIndex(schema.relational_columns, drop_column.name) == null) {
        if (drop_column.if_exists) return;
        return error.InvalidSqlCatalog;
    }

    var dropped = std.ArrayListUnmanaged([]const u8).empty;
    defer dropped.deinit(alloc);
    try appendUniqueBorrowedString(alloc, &dropped, drop_column.name);

    var changed = true;
    while (changed) {
        changed = false;
        for (schema.relational_columns) |column| {
            if (stringSlicesContains(dropped.items, column.name)) continue;
            if (generatedColumnReferencesAny(column, dropped.items)) {
                try appendUniqueBorrowedString(alloc, &dropped, column.name);
                changed = true;
            }
        }
    }

    if (schema.primary_key) |primary_key| {
        if (stringSlicesIntersect(primary_key.columns, dropped.items)) return error.UnsupportedSqlShape;
    }
    if (drop_column.dependency_mode == .restrict and schemaHasDropDependencies(schema.*, dropped.items)) {
        return error.InvalidSqlCatalog;
    }

    try dropRelationalColumnsAlloc(alloc, schema, dropped.items);
    try dropDependentUniqueConstraintsAlloc(alloc, schema, dropped.items);
    try dropDependentForeignKeysAlloc(alloc, schema, dropped.items);
    try dropDependentChecksAlloc(alloc, schema, dropped.items);
}

pub fn dropRelationalConstraintAlloc(
    alloc: std.mem.Allocator,
    schema: *runtime_schema.TableSchema,
    table_name: []const u8,
    drop_constraint: DropConstraintOperation,
) !void {
    if (try dropPrimaryKeyByNameAlloc(alloc, schema, table_name, drop_constraint.name)) return;
    if (try dropUniqueConstraintByNameAlloc(alloc, schema, drop_constraint.name)) return;
    if (try dropForeignKeyByNameAlloc(alloc, schema, drop_constraint.name)) return;
    if (try dropRelationalCheckByNameAlloc(alloc, schema, drop_constraint.name)) return;
    if (drop_constraint.if_exists) return;
    return error.InvalidSqlCatalog;
}

pub fn dropIndexFromRuntimeSchemaAlloc(
    alloc: std.mem.Allocator,
    schema: *runtime_schema.TableSchema,
    plan: DropIndexPlan,
) !void {
    if (try dropUniqueConstraintByNameAlloc(alloc, schema, plan.index_name)) return;
    if (try dropSecondaryIndexByNameAlloc(alloc, schema, plan.index_name)) return;
    if (plan.if_exists) return;
    return error.InvalidSqlCatalog;
}

pub fn validateConstraintByName(schema: *runtime_schema.TableSchema, table_name: []const u8, constraint_name: []const u8) !void {
    if (binder.primaryKeyNameEquals(schema.primary_key, table_name, constraint_name)) return;
    {
        const constraints = @constCast(schema.unique_constraints);
        for (constraints) |*constraint| {
            if (!std.mem.eql(u8, constraint.name, constraint_name)) continue;
            constraint.validation_state = .enforced;
            return;
        }
    }
    {
        const foreign_keys = @constCast(schema.foreign_keys);
        for (foreign_keys) |*foreign_key| {
            if (!std.mem.eql(u8, foreign_key.name, constraint_name)) continue;
            foreign_key.validation_state = .enforced;
            return;
        }
    }
    {
        const checks = @constCast(schema.checks);
        for (checks) |*check| {
            if (!std.mem.eql(u8, check.name, constraint_name)) continue;
            check.validation_state = .enforced;
            return;
        }
    }
    return error.InvalidSqlCatalog;
}

pub fn alterRelationalColumnDefaultAlloc(
    alloc: std.mem.Allocator,
    schema: *runtime_schema.TableSchema,
    operation: AlterColumnDefaultOperation,
) !void {
    const index = binder.relationalColumnIndex(schema.relational_columns, operation.column_name) orelse return error.InvalidSqlCatalog;
    const columns = @constCast(schema.relational_columns);
    if (columns[index].generated != null) return error.InvalidSqlCatalog;
    if (operation.default_value) |default_value| {
        try value_mod.validateDefaultValueForColumnAlloc(alloc, columns[index], default_value);
    }
    if (columns[index].default_value) |existing| {
        alloc.free(existing.value_json);
        columns[index].default_value = null;
    }
    if (operation.default_value) |default_value| {
        columns[index].default_value = try cloneDdlDefaultValue(alloc, default_value);
    }
}

pub fn alterRelationalColumnNullability(
    schema: *runtime_schema.TableSchema,
    operation: AlterColumnNullabilityOperation,
) !void {
    const index = binder.relationalColumnIndex(schema.relational_columns, operation.column_name) orelse return error.InvalidSqlCatalog;
    if (operation.nullable) {
        if (schema.primary_key) |primary_key| {
            if (binder.primaryKeyContains(primary_key, operation.column_name)) return error.UnsupportedSqlShape;
        }
    }
    const columns = @constCast(schema.relational_columns);
    columns[index].nullable = operation.nullable;
}

pub fn markColumnIndexedAlloc(
    alloc: std.mem.Allocator,
    schema: *runtime_schema.TableSchema,
    index_name: []const u8,
    column_name: []const u8,
    include_columns: []const []const u8,
    predicates: []const runtime_schema.UniquePredicate,
    expression_predicates: []const db_mod.types.RelationalRowsExpressionCondition,
    index_generation: u64,
) !void {
    const index = binder.relationalColumnIndex(schema.relational_columns, column_name) orelse return error.InvalidSqlCatalog;
    const columns = @constCast(schema.relational_columns);
    if (columns[index].index_name) |existing| {
        if (!std.mem.eql(u8, existing, index_name)) return error.InvalidSqlCatalog;
    }
    const cloned_predicates = try cloneDdlUniquePredicates(alloc, predicates);
    errdefer freeDdlUniquePredicates(alloc, cloned_predicates);
    const cloned_expression_predicates = try plan_mod.cloneExpressionConditionsAlloc(alloc, expression_predicates);
    errdefer {
        freeExpressionConditions(alloc, cloned_expression_predicates);
        if (cloned_expression_predicates.len > 0) alloc.free(cloned_expression_predicates);
    }
    const cloned_index_name = try alloc.dupe(u8, index_name);
    errdefer alloc.free(cloned_index_name);
    const cloned_include_columns = try cloneStringSlice(alloc, include_columns);
    errdefer freeStringSlice(alloc, cloned_include_columns);
    freeDdlUniquePredicates(alloc, columns[index].index_where);
    freeExpressionConditions(alloc, columns[index].index_where_expressions);
    if (columns[index].index_where_expressions.len > 0) alloc.free(columns[index].index_where_expressions);
    if (columns[index].index_name) |existing| alloc.free(existing);
    freeStringSlice(alloc, columns[index].index_include_columns);
    columns[index].indexed = true;
    columns[index].index_lifecycle = .building;
    columns[index].index_generation = index_generation;
    columns[index].index_name = cloned_index_name;
    columns[index].index_include_columns = cloned_include_columns;
    columns[index].index_where = cloned_predicates;
    columns[index].index_where_expressions = cloned_expression_predicates;
}

pub fn markColumnsIndexedAlloc(
    alloc: std.mem.Allocator,
    schema: *runtime_schema.TableSchema,
    index_name: []const u8,
    column_names: []const []const u8,
    include_columns: []const []const u8,
    predicates: []const runtime_schema.UniquePredicate,
    expression_predicates: []const db_mod.types.RelationalRowsExpressionCondition,
    index_generation: u64,
) !void {
    for (column_names) |column_name| {
        const index = binder.relationalColumnIndex(schema.relational_columns, column_name) orelse return error.InvalidSqlCatalog;
        if (schema.relational_columns[index].index_name) |existing| {
            if (!std.mem.eql(u8, existing, index_name)) return error.InvalidSqlCatalog;
        }
    }
    for (column_names) |column_name| {
        try markColumnIndexedAlloc(alloc, schema, index_name, column_name, include_columns, predicates, expression_predicates, index_generation);
    }
}

pub fn stableSecondaryIndexGeneration(plan: CreateIndexPlan) u64 {
    var hasher = std.hash.Wyhash.init(0x5149_2026_5345_4349);
    hashPlanField(&hasher, "secondary-index-v1");
    hashPlanField(&hasher, plan.index_name);
    hashPlanU64(&hasher, @intFromEnum(plan.method));
    hashPlanU64(&hasher, @intCast(plan.columns.len));
    for (plan.columns) |column| hashPlanField(&hasher, column);
    hashPlanU64(&hasher, @intCast(plan.include_columns.len));
    for (plan.include_columns) |column| hashPlanField(&hasher, column);
    hashPlanU64(&hasher, @intCast(plan.expressions.len));
    for (plan.expressions) |expression| {
        hashPlanU64(&hasher, @intFromEnum(expression.op));
        hashPlanField(&hasher, expression.field);
        if (expression.expression) |row_expression| {
            hasher.update(&.{1});
            hashPlanExpression(&hasher, row_expression);
        } else {
            hasher.update(&.{0});
        }
    }
    if (plan.generated_expression) |generated| {
        hasher.update(&.{1});
        hashPlanU64(&hasher, @intFromEnum(generated.op));
        if (generated.field) |field| {
            hasher.update(&.{1});
            hashPlanField(&hasher, field);
        } else {
            hasher.update(&.{0});
        }
        hashPlanU64(&hasher, @intCast(generated.fields.len));
        for (generated.fields) |field| hashPlanField(&hasher, field);
        hashPlanField(&hasher, generated.separator);
        if (generated.expression) |expression| {
            hasher.update(&.{1});
            hashPlanExpression(&hasher, expression);
        } else {
            hasher.update(&.{0});
        }
    } else {
        hasher.update(&.{0});
    }
    hashPlanU64(&hasher, @intCast(plan.where.len));
    for (plan.where) |predicate| {
        hashPlanU64(&hasher, @intFromEnum(predicate.op));
        hashPlanField(&hasher, predicate.field);
        if (predicate.value_json) |value| {
            hasher.update(&.{1});
            hashPlanField(&hasher, value);
        } else {
            hasher.update(&.{0});
        }
    }
    hashPlanU64(&hasher, @intCast(plan.where_expressions.len));
    for (plan.where_expressions) |condition| {
        hashPlanExpressionCondition(&hasher, condition);
    }
    const json_integer_max: u64 = @intCast(std.math.maxInt(i64));
    const generation = hasher.final() & json_integer_max;
    return if (generation == 0) 1 else generation;
}

pub fn setColumnOnUpdatePolicyAlloc(
    alloc: std.mem.Allocator,
    schema: *runtime_schema.TableSchema,
    column_name: []const u8,
    value: runtime_schema.RelationalDefaultValue,
) !void {
    const index = binder.relationalColumnIndex(schema.relational_columns, column_name) orelse return error.InvalidSqlCatalog;
    const columns = @constCast(schema.relational_columns);
    if (columns[index].field_type != .numeric and columns[index].field_type != .datetime) return error.InvalidSqlCatalog;
    const cloned_value = try cloneDdlDefaultValue(alloc, value);
    errdefer alloc.free(cloned_value.value_json);
    if (columns[index].on_update_value) |existing| alloc.free(existing.value_json);
    columns[index].on_update_value = cloned_value;
}

fn hashPlanExpressionCondition(
    hasher: *std.hash.Wyhash,
    condition: db_mod.types.RelationalRowsExpressionCondition,
) void {
    hashPlanU64(hasher, @intFromEnum(condition.op));
    hashPlanExpression(hasher, condition.lhs);
    hashPlanU64(hasher, @intCast(condition.rhs.len));
    for (condition.rhs) |rhs| hashPlanExpression(hasher, rhs);
}

fn hashPlanExpression(
    hasher: *std.hash.Wyhash,
    expression: db_mod.types.RelationalRowsExpression,
) void {
    hashPlanU64(hasher, @intFromEnum(expression.kind));
    hashPlanU64(hasher, @intFromEnum(expression.field_source));
    hashPlanU64(hasher, @intCast(@intFromBool(expression.json_as_text)));
    hashPlanField(hasher, expression.field);
    hashPlanField(hasher, expression.value_json);
    hashPlanField(hasher, expression.json_path);
    if (expression.cast_type) |cast_type| {
        hasher.update(&.{1});
        hashPlanU64(hasher, @intFromEnum(cast_type));
    } else {
        hasher.update(&.{0});
    }
    hashPlanU64(hasher, @intCast(expression.operands.len));
    for (expression.operands) |operand| hashPlanExpression(hasher, operand);
    hashPlanU64(hasher, @intCast(expression.case_branches.len));
    for (expression.case_branches) |branch| {
        hashPlanExpressionCondition(hasher, branch.when);
        hashPlanExpression(hasher, branch.then);
    }
    hashPlanU64(hasher, @intCast(expression.case_else.len));
    for (expression.case_else) |case_else| hashPlanExpression(hasher, case_else);
}

fn hashPlanField(hasher: *std.hash.Wyhash, value: []const u8) void {
    hashPlanU64(hasher, @intCast(value.len));
    hasher.update(value);
}

fn hashPlanU64(hasher: *std.hash.Wyhash, value: u64) void {
    var buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &buf, value, .big);
    hasher.update(&buf);
}

fn schemaHasDropDependencies(schema: runtime_schema.TableSchema, dropped: []const []const u8) bool {
    if (dropped.len > 1) return true;
    for (schema.unique_constraints) |constraint| {
        if (uniqueConstraintReferencesAny(constraint, dropped)) return true;
    }
    for (schema.foreign_keys) |foreign_key| {
        if (stringSlicesIntersect(foreign_key.child_columns, dropped)) return true;
    }
    for (schema.checks) |check| {
        if (stringSlicesContains(dropped, check.field)) return true;
    }
    return false;
}

fn dropRelationalColumnsAlloc(
    alloc: std.mem.Allocator,
    schema: *runtime_schema.TableSchema,
    dropped: []const []const u8,
) !void {
    var kept: usize = 0;
    for (schema.relational_columns) |column| {
        if (!stringSlicesContains(dropped, column.name)) kept += 1;
    }
    const out: []runtime_schema.RelationalColumn = if (kept > 0) try alloc.alloc(runtime_schema.RelationalColumn, kept) else &.{};
    var out_i: usize = 0;
    for (schema.relational_columns) |column| {
        if (stringSlicesContains(dropped, column.name)) {
            freeDdlRelationalColumn(alloc, column);
        } else {
            out[out_i] = column;
            out_i += 1;
        }
    }
    if (schema.relational_columns.len > 0) alloc.free(schema.relational_columns);
    schema.relational_columns = out;
}

fn dropDependentUniqueConstraintsAlloc(
    alloc: std.mem.Allocator,
    schema: *runtime_schema.TableSchema,
    dropped: []const []const u8,
) !void {
    var kept: usize = 0;
    for (schema.unique_constraints) |constraint| {
        if (!uniqueConstraintReferencesAny(constraint, dropped)) kept += 1;
    }
    const out: []runtime_schema.UniqueConstraint = if (kept > 0) try alloc.alloc(runtime_schema.UniqueConstraint, kept) else &.{};
    var out_i: usize = 0;
    for (schema.unique_constraints) |constraint| {
        if (uniqueConstraintReferencesAny(constraint, dropped)) {
            freeDdlUniqueConstraint(alloc, constraint);
        } else {
            out[out_i] = constraint;
            out_i += 1;
        }
    }
    if (schema.unique_constraints.len > 0) alloc.free(schema.unique_constraints);
    schema.unique_constraints = out;
}

fn dropDependentForeignKeysAlloc(
    alloc: std.mem.Allocator,
    schema: *runtime_schema.TableSchema,
    dropped: []const []const u8,
) !void {
    var kept: usize = 0;
    for (schema.foreign_keys) |foreign_key| {
        if (!stringSlicesIntersect(foreign_key.child_columns, dropped)) kept += 1;
    }
    const out: []runtime_schema.ForeignKey = if (kept > 0) try alloc.alloc(runtime_schema.ForeignKey, kept) else &.{};
    var out_i: usize = 0;
    for (schema.foreign_keys) |foreign_key| {
        if (stringSlicesIntersect(foreign_key.child_columns, dropped)) {
            freeDdlForeignKey(alloc, foreign_key);
        } else {
            out[out_i] = foreign_key;
            out_i += 1;
        }
    }
    if (schema.foreign_keys.len > 0) alloc.free(schema.foreign_keys);
    schema.foreign_keys = out;
}

fn dropDependentChecksAlloc(
    alloc: std.mem.Allocator,
    schema: *runtime_schema.TableSchema,
    dropped: []const []const u8,
) !void {
    var kept: usize = 0;
    for (schema.checks) |check| {
        if (!stringSlicesContains(dropped, check.field)) kept += 1;
    }
    const out: []runtime_schema.RelationalCheck = if (kept > 0) try alloc.alloc(runtime_schema.RelationalCheck, kept) else &.{};
    var out_i: usize = 0;
    for (schema.checks) |check| {
        if (stringSlicesContains(dropped, check.field)) {
            freeDdlRelationalCheck(alloc, check);
        } else {
            out[out_i] = check;
            out_i += 1;
        }
    }
    if (schema.checks.len > 0) alloc.free(schema.checks);
    schema.checks = out;
}

fn dropPrimaryKeyByNameAlloc(
    alloc: std.mem.Allocator,
    schema: *runtime_schema.TableSchema,
    table_name: []const u8,
    constraint_name: []const u8,
) !bool {
    const primary_key = schema.primary_key orelse return false;
    if (!binder.primaryKeyNameEquals(primary_key, table_name, constraint_name)) return false;
    freeDdlPrimaryKey(alloc, primary_key);
    schema.primary_key = null;
    return true;
}

fn dropSecondaryIndexByNameAlloc(
    alloc: std.mem.Allocator,
    schema: *runtime_schema.TableSchema,
    index_name: []const u8,
) !bool {
    const index = binder.relationalColumnIndexForIndexName(schema.relational_columns, index_name) orelse return false;
    if (schema.relational_columns[index].generated != null) {
        const column_name = schema.relational_columns[index].name;
        try dropRelationalColumnAlloc(alloc, schema, .{ .name = column_name, .if_exists = false, .dependency_mode = .cascade });
        return true;
    }

    const columns = @constCast(schema.relational_columns);
    var removed = false;
    for (columns) |*column| {
        if (!binder.relationalColumnHasIndexName(column.*, index_name)) continue;
        if (column.generated != null) return error.InvalidSqlCatalog;
        column.indexed = false;
        column.index_lifecycle = .ready;
        column.index_generation = 0;
        if (column.index_name) |existing| {
            alloc.free(existing);
            column.index_name = null;
        }
        freeStringSlice(alloc, column.index_include_columns);
        column.index_include_columns = &.{};
        freeDdlUniquePredicates(alloc, column.index_where);
        column.index_where = &.{};
        freeExpressionConditions(alloc, column.index_where_expressions);
        if (column.index_where_expressions.len > 0) alloc.free(column.index_where_expressions);
        column.index_where_expressions = &.{};
        removed = true;
    }
    return removed;
}

fn dropUniqueConstraintByNameAlloc(
    alloc: std.mem.Allocator,
    schema: *runtime_schema.TableSchema,
    constraint_name: []const u8,
) !bool {
    var found = false;
    for (schema.unique_constraints) |constraint| {
        if (std.mem.eql(u8, constraint.name, constraint_name)) {
            found = true;
            break;
        }
    }
    if (!found) return false;

    const kept = schema.unique_constraints.len - 1;
    const out: []runtime_schema.UniqueConstraint = if (kept > 0) try alloc.alloc(runtime_schema.UniqueConstraint, kept) else &.{};
    var out_i: usize = 0;
    for (schema.unique_constraints) |constraint| {
        if (std.mem.eql(u8, constraint.name, constraint_name)) {
            freeDdlUniqueConstraint(alloc, constraint);
        } else {
            out[out_i] = constraint;
            out_i += 1;
        }
    }
    if (schema.unique_constraints.len > 0) alloc.free(schema.unique_constraints);
    schema.unique_constraints = out;
    return true;
}

fn dropForeignKeyByNameAlloc(
    alloc: std.mem.Allocator,
    schema: *runtime_schema.TableSchema,
    constraint_name: []const u8,
) !bool {
    var found = false;
    for (schema.foreign_keys) |foreign_key| {
        if (std.mem.eql(u8, foreign_key.name, constraint_name)) {
            found = true;
            break;
        }
    }
    if (!found) return false;

    const kept = schema.foreign_keys.len - 1;
    const out: []runtime_schema.ForeignKey = if (kept > 0) try alloc.alloc(runtime_schema.ForeignKey, kept) else &.{};
    var out_i: usize = 0;
    for (schema.foreign_keys) |foreign_key| {
        if (std.mem.eql(u8, foreign_key.name, constraint_name)) {
            freeDdlForeignKey(alloc, foreign_key);
        } else {
            out[out_i] = foreign_key;
            out_i += 1;
        }
    }
    if (schema.foreign_keys.len > 0) alloc.free(schema.foreign_keys);
    schema.foreign_keys = out;
    return true;
}

fn dropRelationalCheckByNameAlloc(
    alloc: std.mem.Allocator,
    schema: *runtime_schema.TableSchema,
    constraint_name: []const u8,
) !bool {
    var found = false;
    for (schema.checks) |check| {
        if (std.mem.eql(u8, check.name, constraint_name)) {
            found = true;
            break;
        }
    }
    if (!found) return false;

    const kept = schema.checks.len - 1;
    const out: []runtime_schema.RelationalCheck = if (kept > 0) try alloc.alloc(runtime_schema.RelationalCheck, kept) else &.{};
    var out_i: usize = 0;
    for (schema.checks) |check| {
        if (std.mem.eql(u8, check.name, constraint_name)) {
            freeDdlRelationalCheck(alloc, check);
        } else {
            out[out_i] = check;
            out_i += 1;
        }
    }
    if (schema.checks.len > 0) alloc.free(schema.checks);
    schema.checks = out;
    return true;
}

fn appendUniqueBorrowedString(
    alloc: std.mem.Allocator,
    values: *std.ArrayListUnmanaged([]const u8),
    value: []const u8,
) !void {
    if (stringSlicesContains(values.items, value)) return;
    try values.append(alloc, value);
}

fn generatedColumnReferencesAny(column: runtime_schema.RelationalColumn, fields: []const []const u8) bool {
    const generated = column.generated orelse return false;
    if (generated.field) |field| {
        if (stringSlicesContains(fields, field)) return true;
    }
    if (generated.expression) |expression| {
        if (expressionReferencesAny(expression, fields)) return true;
    }
    return stringSlicesIntersect(generated.fields, fields);
}

fn uniqueConstraintReferencesAny(
    constraint: runtime_schema.UniqueConstraint,
    fields: []const []const u8,
) bool {
    if (stringSlicesIntersect(constraint.columns, fields)) return true;
    for (constraint.expressions) |expression| {
        switch (expression.op) {
            .lower, .upper, .md5 => if (stringSlicesContains(fields, expression.field)) return true,
            .expression => if (expression.expression) |row_expression| {
                if (expressionReferencesAny(row_expression, fields)) return true;
            },
        }
    }
    for (constraint.where) |predicate| {
        if (stringSlicesContains(fields, predicate.field)) return true;
    }
    for (constraint.where_expressions) |condition| {
        if (expressionConditionReferencesAny(condition, fields)) return true;
    }
    return false;
}

fn expressionReferencesAny(expression: runtime_schema.RelationalRowsExpression, fields: []const []const u8) bool {
    if (expression.kind == .field and stringSlicesContains(fields, expression.field)) return true;
    for (expression.operands) |operand| {
        if (expressionReferencesAny(operand, fields)) return true;
    }
    for (expression.case_branches) |branch| {
        if (expressionConditionReferencesAny(branch.when, fields)) return true;
        if (expressionReferencesAny(branch.then, fields)) return true;
    }
    for (expression.case_else) |case_else| {
        if (expressionReferencesAny(case_else, fields)) return true;
    }
    return false;
}

fn expressionConditionReferencesAny(condition: runtime_schema.RelationalRowsExpressionCondition, fields: []const []const u8) bool {
    if (expressionReferencesAny(condition.lhs, fields)) return true;
    for (condition.rhs) |rhs| {
        if (expressionReferencesAny(rhs, fields)) return true;
    }
    return false;
}

fn stringSlicesContains(values: []const []const u8, value: []const u8) bool {
    for (values) |candidate| {
        if (std.mem.eql(u8, candidate, value)) return true;
    }
    return false;
}

fn stringSlicesIntersect(a: []const []const u8, b: []const []const u8) bool {
    for (a) |value| {
        if (stringSlicesContains(b, value)) return true;
    }
    return false;
}

pub fn cloneDdlRelationalColumn(alloc: std.mem.Allocator, column: runtime_schema.RelationalColumn) !runtime_schema.RelationalColumn {
    const name = try alloc.dupe(u8, column.name);
    const path = alloc.dupe(u8, column.path) catch |err| {
        alloc.free(name);
        return err;
    };
    var out: runtime_schema.RelationalColumn = .{
        .name = name,
        .path = path,
        .field_type = column.field_type,
        .array_item_type = column.array_item_type,
        .nullable = column.nullable,
        .indexed = column.indexed,
        .index_lifecycle = column.index_lifecycle,
        .index_generation = column.index_generation,
    };
    errdefer freeDdlRelationalColumn(alloc, out);
    out.collation = if (column.collation) |collation| try alloc.dupe(u8, collation) else null;
    out.index_name = if (column.index_name) |index_name| try alloc.dupe(u8, index_name) else null;
    out.index_include_columns = try cloneStringSlice(alloc, column.index_include_columns);
    out.default_value = if (column.default_value) |value| try cloneDdlDefaultValue(alloc, value) else null;
    out.on_update_value = if (column.on_update_value) |value| try cloneDdlDefaultValue(alloc, value) else null;
    out.generated = if (column.generated) |generated| try cloneDdlGeneratedValue(alloc, generated) else null;
    out.index_where = try cloneDdlUniquePredicates(alloc, column.index_where);
    out.index_where_expressions = try plan_mod.cloneExpressionConditionsAlloc(alloc, column.index_where_expressions);
    return out;
}

pub fn cloneDdlRelationalColumns(alloc: std.mem.Allocator, columns: []const runtime_schema.RelationalColumn) ![]const runtime_schema.RelationalColumn {
    if (columns.len == 0) return &.{};
    const out = try alloc.alloc(runtime_schema.RelationalColumn, columns.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |column| freeDdlRelationalColumn(alloc, column);
        alloc.free(out);
    }
    for (columns, 0..) |column, i| {
        out[i] = try cloneDdlRelationalColumn(alloc, column);
        initialized += 1;
    }
    return out;
}

pub fn cloneDdlDefaultValue(alloc: std.mem.Allocator, value: runtime_schema.RelationalDefaultValue) !runtime_schema.RelationalDefaultValue {
    return .{
        .kind = value.kind,
        .value_json = try alloc.dupe(u8, value.value_json),
    };
}

pub fn cloneDdlGeneratedValue(alloc: std.mem.Allocator, generated: runtime_schema.RelationalGeneratedValue) !runtime_schema.RelationalGeneratedValue {
    var out: runtime_schema.RelationalGeneratedValue = .{
        .op = generated.op,
        .separator = try alloc.dupe(u8, generated.separator),
    };
    errdefer freeDdlGeneratedValue(alloc, out);
    out.field = if (generated.field) |field| try alloc.dupe(u8, field) else null;
    out.fields = try cloneStringSlice(alloc, generated.fields);
    out.expression = if (generated.expression) |expression| try plan_mod.cloneExpressionAlloc(alloc, expression) else null;
    return out;
}

pub fn cloneDdlPrimaryKeyMaybe(alloc: std.mem.Allocator, primary_key: ?runtime_schema.PrimaryKey) !?runtime_schema.PrimaryKey {
    return if (primary_key) |key| try cloneDdlPrimaryKey(alloc, key) else null;
}

pub fn cloneDdlPrimaryKey(alloc: std.mem.Allocator, primary_key: runtime_schema.PrimaryKey) !runtime_schema.PrimaryKey {
    const name = if (primary_key.name) |value| try alloc.dupe(u8, value) else null;
    errdefer if (name) |value| alloc.free(value);
    const columns = try cloneStringSlice(alloc, primary_key.columns);
    errdefer freeStringSlice(alloc, columns);
    const include_columns = try cloneStringSlice(alloc, primary_key.include_columns);
    errdefer freeStringSlice(alloc, include_columns);
    const period = if (primary_key.without_overlaps_period) |value| try alloc.dupe(u8, value) else null;
    return .{
        .name = name,
        .columns = columns,
        .include_columns = include_columns,
        .without_overlaps_period = period,
        .deferrable = primary_key.deferrable,
        .timing = primary_key.timing,
    };
}

pub fn cloneDdlPeriod(alloc: std.mem.Allocator, period: runtime_schema.RelationalPeriod) !runtime_schema.RelationalPeriod {
    return .{
        .name = try alloc.dupe(u8, period.name),
        .start_column = try alloc.dupe(u8, period.start_column),
        .end_column = try alloc.dupe(u8, period.end_column),
        .range_type = period.range_type,
    };
}

pub fn cloneDdlPeriods(alloc: std.mem.Allocator, periods: []const runtime_schema.RelationalPeriod) ![]const runtime_schema.RelationalPeriod {
    if (periods.len == 0) return &.{};
    const out = try alloc.alloc(runtime_schema.RelationalPeriod, periods.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |period| freeDdlPeriod(alloc, period);
        alloc.free(out);
    }
    for (periods, 0..) |period, i| {
        out[i] = try cloneDdlPeriod(alloc, period);
        initialized += 1;
    }
    return out;
}

pub fn cloneDdlUniqueExpression(alloc: std.mem.Allocator, expression: runtime_schema.UniqueExpression) !runtime_schema.UniqueExpression {
    const field = try alloc.dupe(u8, expression.field);
    var field_transferred = false;
    errdefer if (!field_transferred) alloc.free(field);
    const row_expression = if (expression.expression) |value| try plan_mod.cloneExpressionAlloc(alloc, value) else null;
    errdefer if (row_expression) |value| plan_mod.freeExpression(alloc, value);
    field_transferred = true;
    return .{
        .op = expression.op,
        .field = field,
        .expression = row_expression,
    };
}

pub fn cloneDdlUniqueExpressions(alloc: std.mem.Allocator, expressions: []const runtime_schema.UniqueExpression) ![]const runtime_schema.UniqueExpression {
    if (expressions.len == 0) return &.{};
    const out = try alloc.alloc(runtime_schema.UniqueExpression, expressions.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |expression| freeDdlUniqueExpression(alloc, expression);
        alloc.free(out);
    }
    for (expressions, 0..) |expression, i| {
        out[i] = try cloneDdlUniqueExpression(alloc, expression);
        initialized += 1;
    }
    return out;
}

pub fn cloneDdlUniquePredicate(alloc: std.mem.Allocator, predicate: runtime_schema.UniquePredicate) !runtime_schema.UniquePredicate {
    const field = try alloc.dupe(u8, predicate.field);
    const value_json = if (predicate.value_json) |value|
        alloc.dupe(u8, value) catch |err| {
            alloc.free(field);
            return err;
        }
    else
        null;
    return .{
        .field = field,
        .op = predicate.op,
        .value_json = value_json,
    };
}

pub fn cloneDdlUniquePredicates(alloc: std.mem.Allocator, predicates: []const runtime_schema.UniquePredicate) ![]const runtime_schema.UniquePredicate {
    if (predicates.len == 0) return &.{};
    const out = try alloc.alloc(runtime_schema.UniquePredicate, predicates.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |predicate| {
            alloc.free(predicate.field);
            if (predicate.value_json) |value| alloc.free(value);
        }
        alloc.free(out);
    }
    for (predicates, 0..) |predicate, i| {
        out[i] = try cloneDdlUniquePredicate(alloc, predicate);
        initialized += 1;
    }
    return out;
}

pub fn cloneDdlUniqueConstraint(alloc: std.mem.Allocator, constraint: runtime_schema.UniqueConstraint) !runtime_schema.UniqueConstraint {
    var out: runtime_schema.UniqueConstraint = .{
        .name = try alloc.dupe(u8, constraint.name),
    };
    errdefer freeDdlUniqueConstraint(alloc, out);
    out.columns = try cloneStringSlice(alloc, constraint.columns);
    out.expressions = try cloneDdlUniqueExpressions(alloc, constraint.expressions);
    out.include_columns = try cloneStringSlice(alloc, constraint.include_columns);
    out.without_overlaps_period = if (constraint.without_overlaps_period) |period| try alloc.dupe(u8, period) else null;
    out.nulls_not_distinct = constraint.nulls_not_distinct;
    out.deferrable = constraint.deferrable;
    out.timing = constraint.timing;
    out.where = try cloneDdlUniquePredicates(alloc, constraint.where);
    out.where_expressions = try plan_mod.cloneExpressionConditionsAlloc(alloc, constraint.where_expressions);
    out.validation_state = constraint.validation_state;
    return out;
}

pub fn cloneDdlUniqueConstraints(alloc: std.mem.Allocator, constraints: []const runtime_schema.UniqueConstraint) ![]const runtime_schema.UniqueConstraint {
    if (constraints.len == 0) return &.{};
    const out = try alloc.alloc(runtime_schema.UniqueConstraint, constraints.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |constraint| freeDdlUniqueConstraint(alloc, constraint);
        alloc.free(out);
    }
    for (constraints, 0..) |constraint, i| {
        out[i] = try cloneDdlUniqueConstraint(alloc, constraint);
        initialized += 1;
    }
    return out;
}

pub fn cloneDdlForeignKey(alloc: std.mem.Allocator, foreign_key: runtime_schema.ForeignKey) !runtime_schema.ForeignKey {
    const name = try alloc.dupe(u8, foreign_key.name);
    const parent_table = alloc.dupe(u8, foreign_key.parent_table) catch |err| {
        alloc.free(name);
        return err;
    };
    var out: runtime_schema.ForeignKey = .{
        .name = name,
        .parent_table = parent_table,
        .on_delete = foreign_key.on_delete,
        .on_update = foreign_key.on_update,
        .timing = foreign_key.timing,
        .deferrable = foreign_key.deferrable,
        .match = foreign_key.match,
        .validation_state = foreign_key.validation_state,
    };
    errdefer freeDdlForeignKey(alloc, out);
    out.child_period = if (foreign_key.child_period) |period| try alloc.dupe(u8, period) else null;
    out.parent_period = if (foreign_key.parent_period) |period| try alloc.dupe(u8, period) else null;
    out.child_columns = try cloneStringSlice(alloc, foreign_key.child_columns);
    out.parent_columns = try cloneStringSlice(alloc, foreign_key.parent_columns);
    return out;
}

pub fn cloneDdlForeignKeys(alloc: std.mem.Allocator, foreign_keys: []const runtime_schema.ForeignKey) ![]const runtime_schema.ForeignKey {
    if (foreign_keys.len == 0) return &.{};
    const out = try alloc.alloc(runtime_schema.ForeignKey, foreign_keys.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |foreign_key| freeDdlForeignKey(alloc, foreign_key);
        alloc.free(out);
    }
    for (foreign_keys, 0..) |foreign_key, i| {
        out[i] = try cloneDdlForeignKey(alloc, foreign_key);
        initialized += 1;
    }
    return out;
}

pub fn cloneDdlRelationalCheck(alloc: std.mem.Allocator, check: runtime_schema.RelationalCheck) !runtime_schema.RelationalCheck {
    const name = try alloc.dupe(u8, check.name);
    const field = alloc.dupe(u8, check.field) catch |err| {
        alloc.free(name);
        return err;
    };
    var out: runtime_schema.RelationalCheck = .{
        .name = name,
        .field = field,
        .op = check.op,
        .validation_state = check.validation_state,
    };
    errdefer freeDdlRelationalCheck(alloc, out);
    out.value_json = if (check.value_json) |value| try alloc.dupe(u8, value) else null;
    out.expression = if (check.expression) |expression| try plan_mod.cloneExpressionConditionAlloc(alloc, expression) else null;
    return out;
}

pub fn cloneDdlRelationalChecks(alloc: std.mem.Allocator, checks: []const runtime_schema.RelationalCheck) ![]const runtime_schema.RelationalCheck {
    if (checks.len == 0) return &.{};
    const out = try alloc.alloc(runtime_schema.RelationalCheck, checks.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |check| freeDdlRelationalCheck(alloc, check);
        alloc.free(out);
    }
    for (checks, 0..) |check, i| {
        out[i] = try cloneDdlRelationalCheck(alloc, check);
        initialized += 1;
    }
    return out;
}

pub fn freeSequenceAlterOperation(alloc: std.mem.Allocator, operation: SequenceAlterOperation) void {
    switch (operation) {
        .set_type => |type_name| alloc.free(type_name),
        .set_owned_by => |owned_by| {
            var owned = owned_by;
            owned.deinit(alloc);
        },
        else => {},
    }
}

pub fn freeSequenceAlterOperations(alloc: std.mem.Allocator, operations: []const SequenceAlterOperation) void {
    for (operations) |operation| freeSequenceAlterOperation(alloc, operation);
}

pub fn freeDatabaseAlterOperations(alloc: std.mem.Allocator, operations: []const DatabaseAlterOperation) void {
    for (operations) |operation| {
        var mutable = operation;
        mutable.deinit(alloc);
    }
    if (operations.len > 0) alloc.free(operations);
}

pub fn freeAlterTableOperation(alloc: std.mem.Allocator, operation: AlterTableOperation) void {
    switch (operation) {
        .add_column => |add_column| {
            freeDdlRelationalColumn(alloc, add_column.column);
            freeDdlUniqueConstraints(alloc, add_column.unique_constraints);
            freeDdlForeignKeys(alloc, add_column.foreign_keys);
            freeDdlRelationalChecks(alloc, add_column.checks);
        },
        .add_period => |period| freeDdlPeriod(alloc, period),
        .add_primary_key => |primary_key| freeDdlPrimaryKey(alloc, primary_key),
        .rename_column => |rename_column| {
            alloc.free(rename_column.old_name);
            alloc.free(rename_column.new_name);
        },
        .rename_constraint => |rename_constraint| {
            alloc.free(rename_constraint.old_name);
            alloc.free(rename_constraint.new_name);
        },
        .drop_column => |drop_column| alloc.free(drop_column.name),
        .drop_constraint => |drop_constraint| alloc.free(drop_constraint.name),
        .drop_update_policy => |drop_update_policy| alloc.free(drop_update_policy.trigger_name),
        .alter_column_default => |alter_column_default| {
            alloc.free(alter_column_default.column_name);
            if (alter_column_default.default_value) |default_value| alloc.free(default_value.value_json);
        },
        .alter_column_nullability => |alter_column_nullability| alloc.free(alter_column_nullability.column_name),
        .alter_column_type => |alter_column_type| {
            alloc.free(alter_column_type.column_name);
            if (alter_column_type.collation) |collation| alloc.free(collation);
            if (alter_column_type.rewrite_expression) |rewrite| {
                var mutable_rewrite = rewrite;
                mutable_rewrite.deinit(alloc);
            }
        },
        .add_unique_constraint => |constraint| freeDdlUniqueConstraint(alloc, constraint),
        .add_foreign_key => |foreign_key| freeDdlForeignKey(alloc, foreign_key),
        .add_check => |check| freeDdlRelationalCheck(alloc, check),
        .validate_constraint => |constraint_name| alloc.free(constraint_name),
    }
}

pub fn freeDdlRelationalCheck(alloc: std.mem.Allocator, check: runtime_schema.RelationalCheck) void {
    alloc.free(check.name);
    alloc.free(check.field);
    if (check.value_json) |value| alloc.free(value);
    if (check.expression) |expression| plan_mod.freeExpressionCondition(alloc, expression);
}

pub fn clearDdlRelationalChecks(alloc: std.mem.Allocator, checks: []const runtime_schema.RelationalCheck) void {
    for (checks) |check| freeDdlRelationalCheck(alloc, check);
}

pub fn freeDdlRelationalChecks(alloc: std.mem.Allocator, checks: []const runtime_schema.RelationalCheck) void {
    clearDdlRelationalChecks(alloc, checks);
    if (checks.len > 0) alloc.free(checks);
}

pub fn freeDdlRelationalColumn(alloc: std.mem.Allocator, column: runtime_schema.RelationalColumn) void {
    alloc.free(column.name);
    alloc.free(column.path);
    if (column.collation) |collation| alloc.free(collation);
    if (column.index_name) |index_name| alloc.free(index_name);
    freeStringSlice(alloc, column.index_include_columns);
    if (column.default_value) |value| alloc.free(value.value_json);
    if (column.on_update_value) |value| alloc.free(value.value_json);
    if (column.generated) |generated| freeDdlGeneratedValue(alloc, generated);
    freeDdlUniquePredicates(alloc, column.index_where);
    freeExpressionConditions(alloc, column.index_where_expressions);
    if (column.index_where_expressions.len > 0) alloc.free(column.index_where_expressions);
}

pub fn clearDdlRelationalColumns(alloc: std.mem.Allocator, columns: []const runtime_schema.RelationalColumn) void {
    for (columns) |column| freeDdlRelationalColumn(alloc, column);
}

pub fn freeDdlRelationalColumns(alloc: std.mem.Allocator, columns: []const runtime_schema.RelationalColumn) void {
    clearDdlRelationalColumns(alloc, columns);
    if (columns.len > 0) alloc.free(columns);
}

pub fn freeDdlGeneratedValue(alloc: std.mem.Allocator, generated: runtime_schema.RelationalGeneratedValue) void {
    if (generated.field) |field| alloc.free(field);
    freeStringSlice(alloc, generated.fields);
    if (generated.separator.len > 0) alloc.free(generated.separator);
    if (generated.expression) |expression| runtime_schema.freeRelationalRowsExpression(alloc, expression);
}

pub fn freeDdlPrimaryKey(alloc: std.mem.Allocator, primary_key: runtime_schema.PrimaryKey) void {
    if (primary_key.name) |name| alloc.free(name);
    freeStringSlice(alloc, primary_key.columns);
    freeStringSlice(alloc, primary_key.include_columns);
    if (primary_key.without_overlaps_period) |period| alloc.free(period);
}

pub fn freeDdlPeriod(alloc: std.mem.Allocator, period: runtime_schema.RelationalPeriod) void {
    alloc.free(period.name);
    alloc.free(period.start_column);
    alloc.free(period.end_column);
}

pub fn freeDdlPeriods(alloc: std.mem.Allocator, periods: []const runtime_schema.RelationalPeriod) void {
    for (periods) |period| freeDdlPeriod(alloc, period);
    if (periods.len > 0) alloc.free(periods);
}

pub fn freeDdlUniqueConstraint(alloc: std.mem.Allocator, constraint: runtime_schema.UniqueConstraint) void {
    alloc.free(constraint.name);
    freeStringSlice(alloc, constraint.columns);
    freeDdlUniqueExpressions(alloc, constraint.expressions);
    freeStringSlice(alloc, constraint.include_columns);
    if (constraint.without_overlaps_period) |period| alloc.free(period);
    freeDdlUniquePredicates(alloc, constraint.where);
    freeExpressionConditions(alloc, constraint.where_expressions);
    if (constraint.where_expressions.len > 0) alloc.free(constraint.where_expressions);
}

pub fn clearDdlUniqueConstraints(alloc: std.mem.Allocator, constraints: []const runtime_schema.UniqueConstraint) void {
    for (constraints) |constraint| freeDdlUniqueConstraint(alloc, constraint);
}

pub fn freeDdlUniqueConstraints(alloc: std.mem.Allocator, constraints: []const runtime_schema.UniqueConstraint) void {
    clearDdlUniqueConstraints(alloc, constraints);
    if (constraints.len > 0) alloc.free(constraints);
}

pub fn freeDdlUniqueExpression(alloc: std.mem.Allocator, expression: runtime_schema.UniqueExpression) void {
    if (expression.field.len > 0) alloc.free(expression.field);
    if (expression.expression) |row_expression| runtime_schema.freeRelationalRowsExpression(alloc, row_expression);
}

pub fn clearDdlUniqueExpressions(alloc: std.mem.Allocator, expressions: []const runtime_schema.UniqueExpression) void {
    for (expressions) |expression| freeDdlUniqueExpression(alloc, expression);
}

pub fn freeDdlUniqueExpressions(alloc: std.mem.Allocator, expressions: []const runtime_schema.UniqueExpression) void {
    clearDdlUniqueExpressions(alloc, expressions);
    if (expressions.len > 0) alloc.free(expressions);
}

pub fn freeDdlUniquePredicates(alloc: std.mem.Allocator, predicates: []const runtime_schema.UniquePredicate) void {
    for (predicates) |predicate| {
        alloc.free(predicate.field);
        if (predicate.value_json) |value| alloc.free(value);
    }
    if (predicates.len > 0) alloc.free(predicates);
}

pub fn freeDdlForeignKey(alloc: std.mem.Allocator, foreign_key: runtime_schema.ForeignKey) void {
    alloc.free(foreign_key.name);
    freeStringSlice(alloc, foreign_key.child_columns);
    if (foreign_key.child_period) |period| alloc.free(period);
    alloc.free(foreign_key.parent_table);
    freeStringSlice(alloc, foreign_key.parent_columns);
    if (foreign_key.parent_period) |period| alloc.free(period);
}

pub fn clearDdlForeignKeys(alloc: std.mem.Allocator, foreign_keys: []const runtime_schema.ForeignKey) void {
    for (foreign_keys) |foreign_key| freeDdlForeignKey(alloc, foreign_key);
}

pub fn freeDdlForeignKeys(alloc: std.mem.Allocator, foreign_keys: []const runtime_schema.ForeignKey) void {
    clearDdlForeignKeys(alloc, foreign_keys);
    if (foreign_keys.len > 0) alloc.free(foreign_keys);
}

fn freeExpressionConditions(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsExpressionCondition) void {
    for (values) |value| plan_mod.freeExpressionCondition(alloc, value);
}

pub fn freeDomainAlterOperations(alloc: std.mem.Allocator, operations: []const DomainAlterOperation) void {
    clearDomainAlterOperations(alloc, operations);
    if (operations.len > 0) alloc.free(operations);
}

pub fn clearDomainAlterOperations(alloc: std.mem.Allocator, operations: []const DomainAlterOperation) void {
    for (operations) |operation_const| {
        var operation = operation_const;
        operation.deinit(alloc);
    }
}

test "SQL adapter DDL enum plans own nested values" {
    const alloc = std.testing.allocator;

    var values = try alloc.alloc([]const u8, 2);
    values[0] = try alloc.dupe(u8, "queued");
    values[1] = try alloc.dupe(u8, "running");

    var create: EnumTypeCatalogPlan = .{ .create = .{
        .type_name = try alloc.dupe(u8, "job_state"),
        .values = values,
    } };
    create.deinit(alloc);

    var add: EnumTypeCatalogPlan = .{ .add_value = .{
        .type_name = try alloc.dupe(u8, "job_state"),
        .value = try alloc.dupe(u8, "done"),
        .position = .after,
        .neighbor_value = try alloc.dupe(u8, "running"),
    } };
    add.deinit(alloc);

    var drop: EnumTypeCatalogPlan = .{ .drop = .{
        .type_name = try alloc.dupe(u8, "job_state"),
        .if_exists = true,
        .cascade = true,
    } };
    drop.deinit(alloc);
}

test "SQL adapter DDL domain plans own nested defaults and checks" {
    const alloc = std.testing.allocator;

    var checks = try alloc.alloc(runtime_schema.RelationalCheck, 1);
    checks[0] = .{
        .name = try alloc.dupe(u8, "status_present"),
        .field = try alloc.dupe(u8, "value"),
        .op = .is_not_null,
        .expression = .{
            .lhs = .{
                .kind = .field,
                .field = try alloc.dupe(u8, "value"),
            },
            .op = .is_not_null,
        },
    };
    var create: DomainCatalogPlan = .{ .create = .{
        .domain_name = try alloc.dupe(u8, "status_text"),
        .field_type = .string,
        .not_null = true,
        .default_value = .{
            .kind = .literal,
            .value_json = try alloc.dupe(u8, "\"queued\""),
        },
        .checks = checks,
    } };
    create.deinit(alloc);

    var operations = try alloc.alloc(DomainAlterOperation, 2);
    operations[0] = .set_not_null;
    operations[1] = .{ .set_default = .{
        .kind = .literal,
        .value_json = try alloc.dupe(u8, "\"ready\""),
    } };
    var alter: DomainCatalogPlan = .{ .alter = .{
        .domain_name = try alloc.dupe(u8, "status_text"),
        .operations = operations,
    } };
    alter.deinit(alloc);

    var drop: DomainCatalogPlan = .{ .drop = .{
        .domain_name = try alloc.dupe(u8, "status_text"),
        .if_exists = true,
        .cascade = true,
    } };
    drop.deinit(alloc);
}

test "SQL adapter DDL identity allocator plans own column metadata" {
    const alloc = std.testing.allocator;

    var include_columns = try alloc.alloc([]const u8, 1);
    include_columns[0] = try alloc.dupe(u8, "tenant_id");

    var generated_fields = try alloc.alloc([]const u8, 2);
    generated_fields[0] = try alloc.dupe(u8, "tenant_id");
    generated_fields[1] = try alloc.dupe(u8, "id");

    var index_where = try alloc.alloc(runtime_schema.UniquePredicate, 1);
    index_where[0] = .{
        .field = try alloc.dupe(u8, "tenant_id"),
        .op = .eq,
        .value_json = try alloc.dupe(u8, "\"tenant-a\""),
    };

    var index_where_expressions = try alloc.alloc(db_mod.types.RelationalRowsExpressionCondition, 1);
    index_where_expressions[0] = .{
        .lhs = .{
            .kind = .field,
            .field = try alloc.dupe(u8, "tenant_id"),
        },
        .op = .eq,
        .rhs = &.{},
    };

    var additional = try alloc.alloc(runtime_schema.RelationalColumn, 1);
    additional[0] = .{
        .name = try alloc.dupe(u8, "tenant_id"),
        .path = try alloc.dupe(u8, "tenant_id"),
        .field_type = .keyword,
        .index_name = try alloc.dupe(u8, "usage_records_tenant_idx"),
    };

    var plan = IdentityAllocatorPlan{
        .table_name = try alloc.dupe(u8, "usage_records"),
        .column = .{
            .name = try alloc.dupe(u8, "id"),
            .path = try alloc.dupe(u8, "id"),
            .field_type = .numeric,
            .collation = try alloc.dupe(u8, "C"),
            .index_name = try alloc.dupe(u8, "usage_records_id_idx"),
            .index_include_columns = include_columns,
            .default_value = .{
                .kind = .literal,
                .value_json = try alloc.dupe(u8, "1"),
            },
            .on_update_value = .{
                .kind = .now_ns,
                .value_json = try alloc.dupe(u8, "{\"now_ns\":true}"),
            },
            .generated = .{
                .op = .concat_ws,
                .fields = generated_fields,
                .separator = try alloc.dupe(u8, ":"),
            },
            .index_where = index_where,
            .index_where_expressions = index_where_expressions,
        },
        .kind = .generated_always,
        .options = .{
            .start_with = 100,
            .increment_by = 10,
            .owned_by = .{
                .table_name = try alloc.dupe(u8, "usage_records"),
                .column_name = try alloc.dupe(u8, "id"),
            },
        },
        .primary_key = true,
        .additional_columns = additional,
    };
    plan.deinit(alloc);

    var spec = IdentityAllocatorSpec{
        .kind = .generated_by_default,
        .options = .{
            .as_type = try alloc.dupe(u8, "bigint"),
        },
    };
    spec.deinit(alloc);
}

test "SQL adapter DDL namespace and extension plans own strings" {
    const alloc = std.testing.allocator;

    var rename_schema: SchemaNamespaceCatalogPlan = .{ .rename = .{
        .schema_name = try alloc.dupe(u8, "public"),
        .new_schema_name = try alloc.dupe(u8, "app"),
    } };
    rename_schema.deinit(alloc);

    var create_extension: ExtensionCatalogPlan = .{ .create = .{
        .extension_name = try alloc.dupe(u8, "pgcrypto"),
        .version = try alloc.dupe(u8, "1.3"),
        .if_not_exists = true,
    } };
    create_extension.deinit(alloc);

    var update_extension: ExtensionCatalogPlan = .{ .update = .{
        .extension_name = try alloc.dupe(u8, "pgcrypto"),
        .target_version = try alloc.dupe(u8, "1.4"),
    } };
    update_extension.deinit(alloc);
}

test "SQL adapter DDL function and authorization plans own strings" {
    const alloc = std.testing.allocator;

    var create_routine: FunctionCatalogPlan = .{ .create = .{
        .kind = .function,
        .routine_name = try alloc.dupe(u8, "normalize_status"),
        .returns_type = try alloc.dupe(u8, "text"),
        .language = try alloc.dupe(u8, "sql"),
        .volatility = .stable,
    } };
    create_routine.deinit(alloc);

    var alter_role: AuthorizationCatalogPlan = .{ .alter_role = .{
        .role_name = try alloc.dupe(u8, "app_user"),
        .setting_name = try alloc.dupe(u8, "app.tenant_id"),
        .setting_value = .{ .literal = try alloc.dupe(u8, "tenant-a") },
    } };
    alter_role.deinit(alloc);

    var privileges = try alloc.alloc([]const u8, 2);
    privileges[0] = try alloc.dupe(u8, "select");
    privileges[1] = try alloc.dupe(u8, "insert");
    var grant: AuthorizationCatalogPlan = .{ .grant_privilege = .{
        .privileges = privileges,
        .object_kind = try alloc.dupe(u8, "table"),
        .object_name = try alloc.dupe(u8, "docs"),
        .principal_name = try alloc.dupe(u8, "app_user"),
    } };
    grant.deinit(alloc);
}

test "SQL adapter DDL prepared statement and cursor plans own strings" {
    const alloc = std.testing.allocator;

    var prepare: PreparedStatementPlan = .{ .prepare = .{
        .statement_name = try alloc.dupe(u8, "usage_plan"),
        .parameter_count = 2,
        .statement_kind = .write,
        .statement_family = .update,
    } };
    prepare.deinit(alloc);

    var execute: PreparedStatementPlan = .{ .execute = .{
        .statement_name = try alloc.dupe(u8, "usage_plan"),
        .argument_count = 2,
    } };
    execute.deinit(alloc);

    var deallocate: PreparedStatementPlan = .{ .deallocate = .{
        .statement_name = try alloc.dupe(u8, "usage_plan"),
    } };
    deallocate.deinit(alloc);

    var declare: CursorPortalPlan = .{ .declare = .{
        .portal_name = try alloc.dupe(u8, "usage_cursor"),
        .scroll = .scroll,
        .binary = true,
        .hold = true,
        .statement_kind = .read,
    } };
    declare.deinit(alloc);

    var fetch: CursorPortalPlan = .{ .fetch = .{
        .portal_name = try alloc.dupe(u8, "usage_cursor"),
        .direction = .forward,
        .count = 10,
    } };
    fetch.deinit(alloc);

    var close: CursorPortalPlan = .{ .close = .{
        .portal_name = try alloc.dupe(u8, "usage_cursor"),
    } };
    close.deinit(alloc);
}

test "SQL adapter DDL savepoint and comment plans own strings" {
    const alloc = std.testing.allocator;

    var savepoint: SavepointTransactionPlan = .{ .savepoint = .{
        .savepoint_name = try alloc.dupe(u8, "before_retry"),
    } };
    savepoint.deinit(alloc);

    var release: SavepointTransactionPlan = .{ .release = .{
        .savepoint_name = try alloc.dupe(u8, "before_retry"),
    } };
    release.deinit(alloc);

    var rollback: SavepointTransactionPlan = .{ .rollback_to = .{
        .savepoint_name = try alloc.dupe(u8, "before_retry"),
    } };
    rollback.deinit(alloc);

    var column_comment = CommentMetadataPlan{
        .target = .column,
        .object_name = try alloc.dupe(u8, "status"),
        .parent_table_name = try alloc.dupe(u8, "usage_records"),
        .comment_json = try alloc.dupe(u8, "\"Current processing state\""),
    };
    column_comment.deinit(alloc);

    var table_comment = CommentMetadataPlan{
        .target = .table,
        .object_name = try alloc.dupe(u8, "usage_records"),
        .comment_json = try alloc.dupe(u8, "\"Usage event rows\""),
    };
    table_comment.deinit(alloc);
}

test "SQL adapter DDL transaction-control plans own strings" {
    const alloc = std.testing.allocator;

    var table_names = try alloc.alloc([]const u8, 2);
    table_names[0] = try alloc.dupe(u8, "usage_records");
    table_names[1] = try alloc.dupe(u8, "jobs");
    var table_lock: TransactionControlPlan = .{ .table_lock = .{
        .table_names = table_names,
        .mode = .share_row_exclusive,
    } };
    table_lock.deinit(alloc);

    var constraint_names = try alloc.alloc([]const u8, 2);
    constraint_names[0] = try alloc.dupe(u8, "usage_records_status_check");
    constraint_names[1] = try alloc.dupe(u8, "usage_records_account_fkey");
    var constraint_mode: TransactionControlPlan = .{ .constraint_mode = .{
        .constraint_names = constraint_names,
        .mode = .deferred,
    } };
    constraint_mode.deinit(alloc);

    var transaction_mode: TransactionControlPlan = .{ .transaction_mode = .{
        .starter = .begin,
        .isolation_level = .serializable,
        .access_mode = .read_write,
        .deferrable = false,
    } };
    transaction_mode.deinit(alloc);

    var advisory_lock: TransactionControlPlan = .{ .advisory_lock = .{
        .action = .lock,
        .key1 = 42,
        .key2 = 7,
    } };
    advisory_lock.deinit(alloc);
}

test "SQL adapter DDL sequence plans own options and operations" {
    const alloc = std.testing.allocator;

    var create: SequenceCatalogPlan = .{ .create = .{
        .sequence_name = try alloc.dupe(u8, "usage_records_id_seq"),
        .if_not_exists = true,
        .options = .{
            .as_type = try alloc.dupe(u8, "bigint"),
            .start_with = 100,
            .increment_by = 5,
            .owned_by = .{
                .table_name = try alloc.dupe(u8, "usage_records"),
                .column_name = try alloc.dupe(u8, "id"),
            },
        },
    } };
    create.deinit(alloc);

    var operations = try alloc.alloc(SequenceAlterOperation, 3);
    operations[0] = .{ .set_type = try alloc.dupe(u8, "bigint") };
    operations[1] = .{ .restart = 42 };
    operations[2] = .{ .set_owned_by = .{
        .table_name = try alloc.dupe(u8, "usage_records"),
        .column_name = try alloc.dupe(u8, "id"),
    } };
    var alter: SequenceCatalogPlan = .{ .alter = .{
        .sequence_name = try alloc.dupe(u8, "usage_records_id_seq"),
        .operations = operations,
    } };
    alter.deinit(alloc);

    var drop: SequenceCatalogPlan = .{ .drop = .{
        .sequence_name = try alloc.dupe(u8, "usage_records_id_seq"),
        .if_exists = true,
        .cascade = true,
    } };
    drop.deinit(alloc);
}

test "SQL adapter DDL database tablespace and notification plans own strings" {
    const alloc = std.testing.allocator;

    var database_ops = try alloc.alloc(DatabaseAlterOperation, 1);
    database_ops[0] = .{ .set_parameter = .{
        .name = try alloc.dupe(u8, "search_path"),
        .value_json = try alloc.dupe(u8, "\"public\""),
    } };
    var database: DatabaseCatalogPlan = .{ .alter = .{
        .database_name = try alloc.dupe(u8, "analytics"),
        .operations = database_ops,
    } };
    database.deinit(alloc);

    var tablespace: TablespaceCatalogPlan = .{ .create = .{
        .tablespace_name = try alloc.dupe(u8, "fastspace"),
        .location_json = try alloc.dupe(u8, "\"s3://bucket/tablespaces/fastspace\""),
        .placement_policy_json = try alloc.dupe(u8, "{}"),
    } };
    tablespace.deinit(alloc);

    var notification: NotificationChannelPlan = .{ .notify = .{
        .channel_name = try alloc.dupe(u8, "catalog_events"),
        .payload_json = try alloc.dupe(u8, "{\"event\":\"reload\"}"),
    } };
    notification.deinit(alloc);
}

test "SQL adapter DDL logical replication plans own nested strings" {
    const alloc = std.testing.allocator;

    var publication_tables = try alloc.alloc([]const u8, 2);
    publication_tables[0] = try alloc.dupe(u8, "usage_records");
    publication_tables[1] = try alloc.dupe(u8, "account_events");
    var create_publication: LogicalReplicationPlan = .{ .publication = .{ .create = .{
        .publication_name = try alloc.dupe(u8, "usage_publication"),
        .table_names = publication_tables,
    } } };
    create_publication.deinit(alloc);

    var alter_tables = try alloc.alloc([]const u8, 1);
    alter_tables[0] = try alloc.dupe(u8, "audit_events");
    var alter_publication: LogicalReplicationPlan = .{ .publication = .{ .alter = .{
        .publication_name = try alloc.dupe(u8, "usage_publication"),
        .operation = .{ .add_tables = alter_tables },
    } } };
    alter_publication.deinit(alloc);

    var publication_names = try alloc.alloc([]const u8, 2);
    publication_names[0] = try alloc.dupe(u8, "usage_publication");
    publication_names[1] = try alloc.dupe(u8, "audit_publication");
    var create_subscription: LogicalReplicationPlan = .{ .subscription = .{ .create = .{
        .subscription_name = try alloc.dupe(u8, "usage_subscription"),
        .connection_json = try alloc.dupe(u8, "{\"uri\":\"postgres://source\"}"),
        .publication_names = publication_names,
    } } };
    create_subscription.deinit(alloc);

    var drop_subscription: LogicalReplicationPlan = .{ .subscription = .{ .drop = .{
        .subscription_name = try alloc.dupe(u8, "usage_subscription"),
        .if_exists = true,
    } } };
    drop_subscription.deinit(alloc);
}

test "SQL adapter DDL type-system plans own strings" {
    const alloc = std.testing.allocator;

    var collation: TypeSystemCatalogPlan = .{ .collation = .{ .rename = .{
        .collation_name = try alloc.dupe(u8, "en_us_ci"),
        .new_collation_name = try alloc.dupe(u8, "en_us_case_insensitive"),
    } } };
    collation.deinit(alloc);

    var operator_plan: TypeSystemCatalogPlan = .{ .operator = .{ .create = .{
        .operator_name = try alloc.dupe(u8, "###"),
        .option_count = 2,
    } } };
    operator_plan.deinit(alloc);

    var aggregate_plan: TypeSystemCatalogPlan = .{ .aggregate = .{ .drop = .{
        .aggregate_name = try alloc.dupe(u8, "weighted_avg"),
        .argument_count = 2,
    } } };
    aggregate_plan.deinit(alloc);

    var cast_plan: TypeSystemCatalogPlan = .{ .cast = .{ .create = .{
        .source_type = try alloc.dupe(u8, "jsonb"),
        .target_type = try alloc.dupe(u8, "text"),
        .function_name = try alloc.dupe(u8, "jsonb_to_text"),
        .assignment = true,
    } } };
    cast_plan.deinit(alloc);
}

test "SQL adapter DDL maintenance plans own strings" {
    const alloc = std.testing.allocator;

    var vacuum: MaintenanceJobPlan = .{ .vacuum = .{
        .table_name = try alloc.dupe(u8, "usage_records"),
        .full = true,
        .analyze = true,
    } };
    vacuum.deinit(alloc);

    var analyze: MaintenanceJobPlan = .{ .analyze = .{
        .table_name = try alloc.dupe(u8, "usage_records"),
        .verbose = true,
        .column_count = 2,
    } };
    analyze.deinit(alloc);

    var reindex: MaintenanceJobPlan = .{ .reindex = .{
        .target = .index,
        .name = try alloc.dupe(u8, "usage_records_status_idx"),
        .concurrently = true,
    } };
    reindex.deinit(alloc);

    var cluster: MaintenanceJobPlan = .{ .cluster = .{
        .table_name = try alloc.dupe(u8, "usage_records"),
        .index_name = try alloc.dupe(u8, "usage_records_pkey"),
        .verbose = true,
    } };
    cluster.deinit(alloc);
}

test "SQL adapter DDL bulk io plans own strings" {
    const alloc = std.testing.allocator;

    var columns = try alloc.alloc([]const u8, 2);
    columns[0] = try alloc.dupe(u8, "id");
    columns[1] = try alloc.dupe(u8, "amount");

    var bulk_io = BulkIoPlan{
        .direction = .from,
        .table_name = try alloc.dupe(u8, "usage_records"),
        .columns = columns,
        .endpoint = try alloc.dupe(u8, "s3://bucket/usage.csv"),
        .format = try alloc.dupe(u8, "csv"),
    };
    bulk_io.deinit(alloc);
}

test "SQL adapter DDL view plans own nested fields" {
    const alloc = std.testing.allocator;

    var source_fields = try alloc.alloc([]const u8, 2);
    source_fields[0] = try alloc.dupe(u8, "id");
    source_fields[1] = try alloc.dupe(u8, "amount");
    var output_fields = try alloc.alloc([]const u8, 2);
    output_fields[0] = try alloc.dupe(u8, "record_id");
    output_fields[1] = try alloc.dupe(u8, "total_amount");
    var create_view: ViewCatalogPlan = .{ .create = .{
        .view_name = try alloc.dupe(u8, "usage_summary"),
        .source_table_name = try alloc.dupe(u8, "usage_records"),
        .source_fields = source_fields,
        .output_fields = output_fields,
        .replace_existing = true,
    } };
    create_view.deinit(alloc);

    var rename_view: ViewCatalogPlan = .{ .rename = .{
        .view_name = try alloc.dupe(u8, "usage_summary"),
        .new_view_name = try alloc.dupe(u8, "usage_summary_v2"),
    } };
    rename_view.deinit(alloc);
}

test "SQL adapter DDL materialized view plans own nested fields" {
    const alloc = std.testing.allocator;

    var source_fields = try alloc.alloc([]const u8, 1);
    source_fields[0] = try alloc.dupe(u8, "amount");
    var output_fields = try alloc.alloc([]const u8, 1);
    output_fields[0] = try alloc.dupe(u8, "amount_sum");
    var create_mv: MaterializedViewCatalogPlan = .{ .create = .{
        .view_name = try alloc.dupe(u8, "usage_amounts_mv"),
        .source_table_name = try alloc.dupe(u8, "usage_records"),
        .source_fields = source_fields,
        .output_fields = output_fields,
        .if_not_exists = true,
    } };
    create_mv.deinit(alloc);

    var refresh_mv: MaterializedViewCatalogPlan = .{ .refresh = .{
        .view_name = try alloc.dupe(u8, "usage_amounts_mv"),
        .concurrently = true,
    } };
    refresh_mv.deinit(alloc);
}

test "SQL adapter DDL table clone plans own strings and expose all options" {
    const alloc = std.testing.allocator;

    const all = TableCloneOptions.includingAll();
    try std.testing.expect(all.columns);
    try std.testing.expect(all.defaults);
    try std.testing.expect(all.generated);
    try std.testing.expect(all.checks);
    try std.testing.expect(all.constraints);
    try std.testing.expect(all.indexes);
    try std.testing.expect(all.periods);
    try std.testing.expect(all.update_policies);

    var clone = TableClonePlan{
        .table_name = try alloc.dupe(u8, "usage_archive"),
        .source_table_name = try alloc.dupe(u8, "usage_records"),
        .if_not_exists = true,
        .options = all,
    };
    clone.deinit(alloc);
}

test "SQL adapter DDL drop table and index plans own strings" {
    const alloc = std.testing.allocator;

    var table = DropTablePlan{
        .table_name = try alloc.dupe(u8, "usage_archive"),
        .if_exists = true,
        .cascade = true,
    };
    table.deinit(alloc);

    var index = DropIndexPlan{
        .index_name = try alloc.dupe(u8, "usage_archive_status_idx"),
        .if_exists = true,
    };
    index.deinit(alloc);
}

test "SQL adapter DDL update policy plans own metadata" {
    const alloc = std.testing.allocator;

    var policy = CreateUpdatePolicyPlan{
        .trigger_name = try alloc.dupe(u8, "usage_records_touch_updated_at"),
        .table_name = try alloc.dupe(u8, "usage_records"),
        .column_name = try alloc.dupe(u8, "updated_at_ns"),
        .on_update_value = .{
            .kind = .now_ns,
            .value_json = try alloc.dupe(u8, "{\"now_ns\":true}"),
        },
    };
    policy.deinit(alloc);
}

test "SQL adapter DDL row security plans own nested fields" {
    const alloc = std.testing.allocator;

    var alter: RowSecurityCatalogPlan = .{ .alter_table = .{
        .table_name = try alloc.dupe(u8, "tenant_events"),
        .enabled = true,
    } };
    alter.deinit(alloc);

    var create_policy: RowSecurityCatalogPlan = .{ .create_policy = .{
        .policy_name = try alloc.dupe(u8, "tenant_isolation"),
        .table_name = try alloc.dupe(u8, "tenant_events"),
        .predicate = .{ .current_setting_equals = .{
            .field = try alloc.dupe(u8, "tenant_id"),
            .setting_name = try alloc.dupe(u8, "app.tenant_id"),
        } },
    } };
    create_policy.deinit(alloc);

    var literal_policy: RowSecurityCatalogPlan = .{ .create_policy = .{
        .policy_name = try alloc.dupe(u8, "tenant_literal"),
        .table_name = try alloc.dupe(u8, "tenant_events"),
        .predicate = .{ .literal_equals = .{
            .field = try alloc.dupe(u8, "tenant_id"),
            .value_json = try alloc.dupe(u8, "\"tenant-a\""),
        } },
    } };
    literal_policy.deinit(alloc);

    var conjunction_terms = try alloc.alloc(RowSecurityPolicyPredicate, 2);
    conjunction_terms[0] = .{ .literal_equals = .{
        .field = try alloc.dupe(u8, "tenant_id"),
        .value_json = try alloc.dupe(u8, "\"tenant-a\""),
    } };
    conjunction_terms[1] = .{ .literal_equals = .{
        .field = try alloc.dupe(u8, "status"),
        .value_json = try alloc.dupe(u8, "\"active\""),
    } };
    var conjunction_policy: RowSecurityCatalogPlan = .{ .create_policy = .{
        .policy_name = try alloc.dupe(u8, "tenant_conjunction"),
        .table_name = try alloc.dupe(u8, "tenant_events"),
        .predicate = .{ .conjunction = .{ .predicates = conjunction_terms } },
    } };
    conjunction_policy.deinit(alloc);

    var alter_policy: RowSecurityCatalogPlan = .{ .alter_policy = .{
        .policy_name = try alloc.dupe(u8, "tenant_isolation"),
        .table_name = try alloc.dupe(u8, "tenant_events"),
        .predicate = .{ .literal_equals = .{
            .field = try alloc.dupe(u8, "status"),
            .value_json = try alloc.dupe(u8, "\"active\""),
        } },
    } };
    alter_policy.deinit(alloc);

    var drop_policy: RowSecurityCatalogPlan = .{ .drop_policy = .{
        .policy_name = try alloc.dupe(u8, "tenant_isolation"),
        .table_name = try alloc.dupe(u8, "tenant_events"),
        .if_exists = true,
    } };
    drop_policy.deinit(alloc);
}
