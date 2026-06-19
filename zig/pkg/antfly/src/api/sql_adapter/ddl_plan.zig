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
const db_mod = @import("../../storage/db/mod.zig");
const grammar = @import("grammar.zig");
const plan_mod = @import("plan.zig");
const runtime_schema = @import("../../storage/schema.zig");

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
    reset_search_path,
    show_search_path,
    discard_all,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .set_search_path => |*plan| plan.deinit(alloc),
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

pub const AppliedDdlWorkItem = struct {
    action: AppliedDdlWorkAction,
    subject: AppliedDdlWorkSubject,
    reason: AppliedDdlWorkReason,
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

    role_name: []const u8,
    database_name: ?[]const u8 = null,
    operation: Operation = .set,
    setting_name: []const u8,
    setting_value: ?[]const u8 = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.role_name);
        if (self.database_name) |database_name| alloc.free(database_name);
        alloc.free(self.setting_name);
        if (self.setting_value) |setting_value| alloc.free(setting_value);
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
    conjunction: RowSecurityConjunctionPredicate,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .current_setting_equals => |*predicate| predicate.deinit(alloc),
            .literal_equals => |*predicate| predicate.deinit(alloc),
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

fn freeStringSlice(alloc: std.mem.Allocator, values: []const []const u8) void {
    for (values) |value| alloc.free(value);
    if (values.len > 0) alloc.free(values);
}

fn freeSequenceAlterOperation(alloc: std.mem.Allocator, operation: SequenceAlterOperation) void {
    switch (operation) {
        .set_type => |type_name| alloc.free(type_name),
        .set_owned_by => |owned_by| {
            var owned = owned_by;
            owned.deinit(alloc);
        },
        else => {},
    }
}

fn freeSequenceAlterOperations(alloc: std.mem.Allocator, operations: []const SequenceAlterOperation) void {
    for (operations) |operation| freeSequenceAlterOperation(alloc, operation);
}

fn freeDatabaseAlterOperations(alloc: std.mem.Allocator, operations: []const DatabaseAlterOperation) void {
    for (operations) |operation| {
        var mutable = operation;
        mutable.deinit(alloc);
    }
    if (operations.len > 0) alloc.free(operations);
}

fn freeAlterTableOperation(alloc: std.mem.Allocator, operation: AlterTableOperation) void {
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
        },
        .add_unique_constraint => |constraint| freeDdlUniqueConstraint(alloc, constraint),
        .add_foreign_key => |foreign_key| freeDdlForeignKey(alloc, foreign_key),
        .add_check => |check| freeDdlRelationalCheck(alloc, check),
        .validate_constraint => |constraint_name| alloc.free(constraint_name),
    }
}

fn freeDdlRelationalCheck(alloc: std.mem.Allocator, check: runtime_schema.RelationalCheck) void {
    alloc.free(check.name);
    alloc.free(check.field);
    if (check.value_json) |value| alloc.free(value);
    if (check.expression) |expression| plan_mod.freeExpressionCondition(alloc, expression);
}

fn freeDdlRelationalChecks(alloc: std.mem.Allocator, checks: []const runtime_schema.RelationalCheck) void {
    for (checks) |check| freeDdlRelationalCheck(alloc, check);
    if (checks.len > 0) alloc.free(checks);
}

fn freeDdlRelationalColumn(alloc: std.mem.Allocator, column: runtime_schema.RelationalColumn) void {
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

fn clearDdlRelationalColumns(alloc: std.mem.Allocator, columns: []const runtime_schema.RelationalColumn) void {
    for (columns) |column| freeDdlRelationalColumn(alloc, column);
}

fn freeDdlRelationalColumns(alloc: std.mem.Allocator, columns: []const runtime_schema.RelationalColumn) void {
    clearDdlRelationalColumns(alloc, columns);
    if (columns.len > 0) alloc.free(columns);
}

fn freeDdlGeneratedValue(alloc: std.mem.Allocator, generated: runtime_schema.RelationalGeneratedValue) void {
    if (generated.field) |field| alloc.free(field);
    freeStringSlice(alloc, generated.fields);
    if (generated.separator.len > 0) alloc.free(generated.separator);
    if (generated.expression) |expression| runtime_schema.freeRelationalRowsExpression(alloc, expression);
}

fn freeDdlPrimaryKey(alloc: std.mem.Allocator, primary_key: runtime_schema.PrimaryKey) void {
    if (primary_key.name) |name| alloc.free(name);
    freeStringSlice(alloc, primary_key.columns);
    freeStringSlice(alloc, primary_key.include_columns);
    if (primary_key.without_overlaps_period) |period| alloc.free(period);
}

fn freeDdlPeriod(alloc: std.mem.Allocator, period: runtime_schema.RelationalPeriod) void {
    alloc.free(period.name);
    alloc.free(period.start_column);
    alloc.free(period.end_column);
}

fn freeDdlPeriods(alloc: std.mem.Allocator, periods: []const runtime_schema.RelationalPeriod) void {
    for (periods) |period| freeDdlPeriod(alloc, period);
    if (periods.len > 0) alloc.free(periods);
}

fn freeDdlUniqueConstraint(alloc: std.mem.Allocator, constraint: runtime_schema.UniqueConstraint) void {
    alloc.free(constraint.name);
    freeStringSlice(alloc, constraint.columns);
    freeDdlUniqueExpressions(alloc, constraint.expressions);
    freeStringSlice(alloc, constraint.include_columns);
    if (constraint.without_overlaps_period) |period| alloc.free(period);
    freeDdlUniquePredicates(alloc, constraint.where);
    freeExpressionConditions(alloc, constraint.where_expressions);
    if (constraint.where_expressions.len > 0) alloc.free(constraint.where_expressions);
}

fn freeDdlUniqueConstraints(alloc: std.mem.Allocator, constraints: []const runtime_schema.UniqueConstraint) void {
    for (constraints) |constraint| freeDdlUniqueConstraint(alloc, constraint);
    if (constraints.len > 0) alloc.free(constraints);
}

fn freeDdlUniqueExpressions(alloc: std.mem.Allocator, expressions: []const runtime_schema.UniqueExpression) void {
    for (expressions) |expression| {
        if (expression.field.len > 0) alloc.free(expression.field);
        if (expression.expression) |row_expression| runtime_schema.freeRelationalRowsExpression(alloc, row_expression);
    }
    if (expressions.len > 0) alloc.free(expressions);
}

fn freeDdlUniquePredicates(alloc: std.mem.Allocator, predicates: []const runtime_schema.UniquePredicate) void {
    for (predicates) |predicate| {
        alloc.free(predicate.field);
        if (predicate.value_json) |value| alloc.free(value);
    }
    if (predicates.len > 0) alloc.free(predicates);
}

fn freeDdlForeignKey(alloc: std.mem.Allocator, foreign_key: runtime_schema.ForeignKey) void {
    alloc.free(foreign_key.name);
    freeStringSlice(alloc, foreign_key.child_columns);
    if (foreign_key.child_period) |period| alloc.free(period);
    alloc.free(foreign_key.parent_table);
    freeStringSlice(alloc, foreign_key.parent_columns);
    if (foreign_key.parent_period) |period| alloc.free(period);
}

fn freeDdlForeignKeys(alloc: std.mem.Allocator, foreign_keys: []const runtime_schema.ForeignKey) void {
    for (foreign_keys) |foreign_key| freeDdlForeignKey(alloc, foreign_key);
    if (foreign_keys.len > 0) alloc.free(foreign_keys);
}

fn freeExpressionConditions(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsExpressionCondition) void {
    for (values) |value| plan_mod.freeExpressionCondition(alloc, value);
}

fn freeDomainAlterOperations(alloc: std.mem.Allocator, operations: []const DomainAlterOperation) void {
    for (operations) |operation_const| {
        var operation = operation_const;
        operation.deinit(alloc);
    }
    if (operations.len > 0) alloc.free(operations);
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
        .setting_value = try alloc.dupe(u8, "tenant-a"),
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
