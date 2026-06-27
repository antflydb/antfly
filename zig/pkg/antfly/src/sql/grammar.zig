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

const ast = @import("ast.zig");
const classifier = @import("classifier.zig");
const db_mod = @import("../storage/db/mod.zig");
const ddl_plan = @import("ddl.zig");
const generated_parser = @import("generated_parser.zig");
const lexer = @import("lexer.zig");
const lower_expr = @import("lower_expr.zig");
const parser = @import("parser.zig");
const runtime_schema = @import("../storage/schema.zig");
const token_mod = @import("token.zig");
const tokenized = @import("tokenized.zig");
const sql_value = @import("value.zig");

pub const Token = token_mod.Token;
const TokenKind = token_mod.TokenKind;

pub const RowSecurityAlterSyntax = struct {
    table_identifier: []const u8,
    enabled: bool,
};

pub const CreateRowSecurityPolicySyntax = struct {
    policy_name: []const u8,
    table_name: []const u8,
    role_targets: []const []const u8 = &.{},
    predicate: ddl_plan.RowSecurityPolicyPredicate,
    check_predicate: ?ddl_plan.RowSecurityPolicyPredicate = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.policy_name));
        alloc.free(@constCast(self.table_name));
        freeStringSlice(alloc, self.role_targets);
        self.predicate.deinit(alloc);
        if (self.check_predicate) |*predicate| predicate.deinit(alloc);
        self.* = undefined;
    }
};

pub const AlterRowSecurityPolicySyntax = struct {
    policy_name: []const u8,
    table_name: []const u8,
    role_targets_present: bool = false,
    role_targets: []const []const u8 = &.{},
    predicate: ?ddl_plan.RowSecurityPolicyPredicate = null,
    check_predicate: ?ddl_plan.RowSecurityPolicyPredicate = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.policy_name));
        alloc.free(@constCast(self.table_name));
        freeStringSlice(alloc, self.role_targets);
        if (self.predicate) |*predicate| predicate.deinit(alloc);
        if (self.check_predicate) |*predicate| predicate.deinit(alloc);
        self.* = undefined;
    }
};

pub const DropRowSecurityPolicySyntax = struct {
    policy_name: []const u8,
    table_name: []const u8,
    if_exists: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.policy_name));
        alloc.free(@constCast(self.table_name));
        self.* = undefined;
    }
};

pub const DropUpdatePolicySyntax = struct {
    trigger_name: []const u8,
    table_name: []const u8,
    if_exists: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.trigger_name));
        alloc.free(@constCast(self.table_name));
        self.* = undefined;
    }
};

pub const CreateUpdatePolicySyntax = struct {
    trigger_name: []const u8,
    table_name: []const u8,
    column_name: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.trigger_name));
        alloc.free(@constCast(self.table_name));
        alloc.free(@constCast(self.column_name));
        self.* = undefined;
    }
};

pub const AdapterNoopTransactionBoundaryTail = struct {
    work: bool = false,
    transaction: bool = false,
};

pub const SetSearchPathSyntax = struct {
    namespaces: []const []const u8 = &.{},
    local: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        freeStringSlice(alloc, self.namespaces);
        self.* = undefined;
    }
};

pub const SavepointNameSyntax = struct {
    savepoint_name: []const u8,
};

pub const PreparedStatementSubjectSyntax = classifier.SqlPreparedStatementSubjectKind;
pub const PreparedStatementStatementSyntax = classifier.SqlPreparedStatementStatementKind;

pub const PrepareStatementSyntax = struct {
    statement_name: []const u8,
    parameter_count: usize = 0,
    statement_kind: PreparedStatementSubjectSyntax,
    statement_family: PreparedStatementStatementSyntax,
};

pub const ExecutePreparedStatementSyntax = struct {
    statement_name: []const u8,
    argument_count: usize = 0,
};

pub const TableLockModeSyntax = enum {
    access_share,
    row_share,
    row_exclusive,
    share_update_exclusive,
    share,
    share_row_exclusive,
    exclusive,
    access_exclusive,
};

pub const TableLockSyntax = struct {
    table_names: []const []const u8 = &.{},
    mode: TableLockModeSyntax,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        freeStringSlice(alloc, self.table_names);
        self.* = undefined;
    }
};

pub const ConstraintCheckModeSyntax = enum {
    immediate,
    deferred,
};

pub const ConstraintModeSyntax = struct {
    all: bool = false,
    constraint_names: []const []const u8 = &.{},
    mode: ConstraintCheckModeSyntax,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        freeStringSlice(alloc, self.constraint_names);
        self.* = undefined;
    }
};

pub const TransactionModeStarterSyntax = enum {
    set_transaction,
    start_transaction,
    begin,
};

pub const TransactionIsolationLevelSyntax = enum {
    serializable,
    repeatable_read,
    read_committed,
    read_uncommitted,
};

pub const TransactionAccessModeSyntax = enum {
    read_only,
    read_write,
};

pub const TransactionModeSyntax = struct {
    starter: TransactionModeStarterSyntax,
    isolation_level: ?TransactionIsolationLevelSyntax = null,
    access_mode: ?TransactionAccessModeSyntax = null,
    deferrable: ?bool = null,
};

pub const AdvisoryLockActionSyntax = enum {
    lock,
    unlock,
};

pub const AdvisoryLockSyntax = struct {
    action: AdvisoryLockActionSyntax,
    key1: i64,
    key2: ?i64 = null,
};

pub const VacuumMaintenanceSyntax = struct {
    table_name: []const u8,
    full: bool = false,
    freeze: bool = false,
    verbose: bool = false,
    analyze: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.table_name));
        self.* = undefined;
    }
};

pub const AnalyzeMaintenanceSyntax = struct {
    table_name: []const u8,
    verbose: bool = false,
    column_count: usize = 0,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.table_name));
        self.* = undefined;
    }
};

pub const ReindexMaintenanceTargetSyntax = enum {
    index,
    table,
    schema,
    database,
    system,
};

pub const ReindexMaintenanceSyntax = struct {
    target: ReindexMaintenanceTargetSyntax,
    name: []const u8,
    concurrently: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.name));
        self.* = undefined;
    }
};

pub const ClusterMaintenanceSyntax = struct {
    table_name: []const u8,
    index_name: ?[]const u8 = null,
    verbose: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.table_name));
        if (self.index_name) |index_name| alloc.free(@constCast(index_name));
        self.* = undefined;
    }
};

pub const CreateDatabaseSyntax = struct {
    database_name: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.database_name));
        self.* = undefined;
    }
};

pub const AlterDatabaseSyntax = struct {
    database_name: []const u8,
    setting_name: []const u8,
    value_json: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.database_name));
        alloc.free(@constCast(self.setting_name));
        alloc.free(@constCast(self.value_json));
        self.* = undefined;
    }
};

pub const DropDatabaseSyntax = struct {
    database_name: []const u8,
    if_exists: bool = false,
    force: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.database_name));
        self.* = undefined;
    }
};

pub const CreateTablespaceSyntax = struct {
    tablespace_name: []const u8,
    location_json: []const u8,
    placement_policy_json: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.tablespace_name));
        alloc.free(@constCast(self.location_json));
        alloc.free(@constCast(self.placement_policy_json));
        self.* = undefined;
    }
};

pub const RenameTablespaceSyntax = struct {
    tablespace_name: []const u8,
    new_tablespace_name: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.tablespace_name));
        alloc.free(@constCast(self.new_tablespace_name));
        self.* = undefined;
    }
};

pub const DropTablespaceSyntax = struct {
    tablespace_name: []const u8,
    if_exists: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.tablespace_name));
        self.* = undefined;
    }
};

pub const CreateSchemaNamespaceSyntax = struct {
    schema_name: []const u8,
    if_not_exists: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.schema_name));
        self.* = undefined;
    }
};

pub const RenameSchemaNamespaceSyntax = struct {
    schema_name: []const u8,
    new_schema_name: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.schema_name));
        alloc.free(@constCast(self.new_schema_name));
        self.* = undefined;
    }
};

pub const DropSchemaNamespaceSyntax = struct {
    schema_name: []const u8,
    if_exists: bool = false,
    cascade: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.schema_name));
        self.* = undefined;
    }
};

pub const CreateExtensionSyntax = struct {
    extension_name: []const u8,
    schema_name: ?[]const u8 = null,
    version: ?[]const u8 = null,
    if_not_exists: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.extension_name));
        if (self.schema_name) |schema_name| alloc.free(@constCast(schema_name));
        if (self.version) |version| alloc.free(@constCast(version));
        self.* = undefined;
    }
};

pub const UpdateExtensionSyntax = struct {
    extension_name: []const u8,
    target_version: ?[]const u8 = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.extension_name));
        if (self.target_version) |version| alloc.free(@constCast(version));
        self.* = undefined;
    }
};

pub const DropExtensionSyntax = struct {
    extension_name: []const u8,
    if_exists: bool = false,
    cascade: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.extension_name));
        self.* = undefined;
    }
};

pub const RoutineKindSyntax = enum {
    function,
    procedure,
};

const RoutineSignatureSyntax = struct {
    argument_count: usize = 0,
    argument_names: []const []const u8 = &.{},

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.argument_names) |name| {
            if (name.len > 0) alloc.free(@constCast(name));
        }
        if (self.argument_names.len > 0) alloc.free(@constCast(self.argument_names));
        self.* = undefined;
    }
};

pub const CreateRoutineSyntax = struct {
    kind: RoutineKindSyntax,
    routine_name: []const u8,
    argument_count: usize = 0,
    returns_type: ?[]const u8 = null,
    language: ?[]const u8 = null,
    volatility: ?ddl_plan.RoutineVolatility = null,
    security: ?ddl_plan.RoutineSecurity = null,
    null_input: ?ddl_plan.RoutineNullInput = null,
    parallel_safety: ?ddl_plan.RoutineParallelSafety = null,
    leakproof: bool = false,
    window: bool = false,
    support_function: ?[]const u8 = null,
    transform_types: []const []const u8 = &.{},
    settings: []const ddl_plan.RoutineSetting = &.{},
    cost: ?[]const u8 = null,
    rows: ?[]const u8 = null,
    body: ?ddl_plan.RoutineBodyPlan = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.routine_name));
        if (self.returns_type) |returns_type| alloc.free(@constCast(returns_type));
        if (self.language) |language| alloc.free(@constCast(language));
        if (self.support_function) |support_function| alloc.free(@constCast(support_function));
        freeStringSlice(alloc, self.transform_types);
        freeRoutineSettingSlice(alloc, self.settings);
        if (self.cost) |cost| alloc.free(@constCast(cost));
        if (self.rows) |rows| alloc.free(@constCast(rows));
        if (self.body) |*body| body.deinit(alloc);
        self.* = undefined;
    }
};

pub const DropRoutineSyntax = struct {
    kind: RoutineKindSyntax,
    routine_name: []const u8,
    if_exists: bool = false,
    argument_count: usize = 0,
    cascade: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.routine_name));
        self.* = undefined;
    }
};

pub const CreateSequenceSyntax = struct {
    sequence_name: []const u8,
    if_not_exists: bool = false,
    options: ddl_plan.SequenceOptions = .{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.sequence_name));
        self.options.deinit(alloc);
        self.* = undefined;
    }
};

pub const AlterSequenceSyntax = struct {
    sequence_name: []const u8,
    if_exists: bool = false,
    operations: []const ddl_plan.SequenceAlterOperation = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.sequence_name));
        ddl_plan.freeSequenceAlterOperations(alloc, self.operations);
        if (self.operations.len > 0) alloc.free(@constCast(self.operations));
        self.* = undefined;
    }
};

pub const DropSequenceSyntax = struct {
    sequence_name: []const u8,
    if_exists: bool = false,
    cascade: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.sequence_name));
        self.* = undefined;
    }
};

pub const CreateEnumTypeSyntax = struct {
    type_name: []const u8,
    values: []const []const u8 = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.type_name));
        freeStringSlice(alloc, self.values);
        self.* = undefined;
    }
};

pub const AddEnumValueSyntax = struct {
    type_name: []const u8,
    value: []const u8,
    if_not_exists: bool = false,
    position: ddl_plan.EnumValuePosition = .none,
    neighbor_value: ?[]const u8 = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.type_name));
        alloc.free(@constCast(self.value));
        if (self.neighbor_value) |neighbor_value| alloc.free(@constCast(neighbor_value));
        self.* = undefined;
    }
};

pub const DropEnumTypeSyntax = struct {
    type_name: []const u8,
    if_exists: bool = false,
    cascade: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.type_name));
        self.* = undefined;
    }
};

pub const DropDomainSyntax = struct {
    domain_name: []const u8,
    if_exists: bool = false,
    cascade: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.domain_name));
        self.* = undefined;
    }
};

pub const CreateDomainHeaderSyntax = struct {
    domain_name: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.domain_name));
        self.* = undefined;
    }
};

pub const AlterDomainHeaderSyntax = struct {
    domain_name: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.domain_name));
        self.* = undefined;
    }
};

pub const CommentMetadataSyntax = struct {
    target: ddl_plan.CommentMetadataTarget,
    object_name: []const u8,
    parent_table_name: ?[]const u8 = null,
    comment_json: ?[]const u8 = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.object_name));
        if (self.parent_table_name) |parent| alloc.free(@constCast(parent));
        if (self.comment_json) |comment| alloc.free(@constCast(comment));
        self.* = undefined;
    }
};

pub const DropTableSyntax = struct {
    table_name: []const u8,
    if_exists: bool = false,
    cascade: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.table_name));
        self.* = undefined;
    }
};

pub const DropIndexSyntax = struct {
    index_name: []const u8,
    if_exists: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.index_name));
        self.* = undefined;
    }
};

pub const AlterTableHeaderSyntax = struct {
    table_name: []const u8,
    if_exists: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.table_name));
        self.* = undefined;
    }
};

pub const AlterTableValidateConstraintSyntax = struct {
    constraint_name: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.constraint_name));
        self.* = undefined;
    }
};

pub const AlterTableRenameTargetSyntax = enum {
    column,
    constraint,
};

pub const AlterTableRenameOperationSyntax = struct {
    target: AlterTableRenameTargetSyntax,
    old_name: []const u8,
    new_name: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.old_name));
        alloc.free(@constCast(self.new_name));
        self.* = undefined;
    }
};

pub const AlterTableDropTargetSyntax = enum {
    column,
    constraint,
};

pub const AlterTableDropOperationSyntax = struct {
    target: AlterTableDropTargetSyntax,
    name: []const u8,
    if_exists: bool = false,
    dependency_mode: ddl_plan.DropDependencyMode,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.name));
        self.* = undefined;
    }
};

pub const AlterTableColumnNullabilitySyntax = struct {
    column_name: []const u8,
    nullable: bool,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.column_name));
        self.* = undefined;
    }
};

pub const AlterTableColumnDefaultActionSyntax = enum {
    set,
    drop,
};

pub const AlterTableColumnDefaultSyntax = struct {
    column_name: []const u8,
    action: AlterTableColumnDefaultActionSyntax,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.column_name));
        self.* = undefined;
    }
};

pub const AlterTableColumnTypeHeaderSyntax = struct {
    column_name: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.column_name));
        self.* = undefined;
    }
};

pub const AlterTableColumnUsingSyntax = struct {
    column_name: []const u8,
    wrapped: bool = false,
};

pub const AlterTableAddColumnHeaderSyntax = struct {
    if_not_exists: bool = false,
};

pub const AlterTableAddOperationKindSyntax = enum {
    period,
    primary_key,
    unique,
    foreign_key,
    check,
};

pub const AlterTableAddOperationPrefixSyntax = struct {
    kind: AlterTableAddOperationKindSyntax,
    constraint_name: ?[]const u8 = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.constraint_name) |constraint_name| alloc.free(@constCast(constraint_name));
        self.* = undefined;
    }
};

pub const IdentityAllocatorTableHeaderSyntax = struct {
    table_name: []const u8,
    column_name: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.table_name));
        alloc.free(@constCast(self.column_name));
        self.* = undefined;
    }
};

pub const CreateIndexHeaderSyntax = struct {
    index_name: []const u8,
    table_name: []const u8,
    if_not_exists: bool = false,
    method: ddl_plan.DdlIndexMethod = .btree,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.index_name));
        alloc.free(@constCast(self.table_name));
        self.* = undefined;
    }
};

pub const CreateIndexElementOrderDirectionSyntax = enum {
    default,
    asc,
    desc,
};

pub const CreateIndexElementNullsOrderSyntax = enum {
    default,
    first,
    last,
};

pub const CreateIndexElementSuffixSyntax = struct {
    opclass: ddl_plan.DdlIndexOpClass = .default,
    order_direction: CreateIndexElementOrderDirectionSyntax = .default,
    nulls_order: CreateIndexElementNullsOrderSyntax = .default,
};

pub const DdlTemporalColumnListSyntax = struct {
    columns: []const []const u8,
    without_overlaps_period: ?[]const u8 = null,

    pub fn deinit(self: @This(), alloc: std.mem.Allocator) void {
        freeStringSlice(alloc, self.columns);
        if (self.without_overlaps_period) |period| alloc.free(@constCast(period));
    }
};

pub const DdlForeignKeyActionSyntax = enum {
    restrict,
    cascade,
    no_action,
    set_null,
};

pub const DdlForeignKeyTimingSyntax = enum {
    immediate,
    deferred,
};

pub const DdlForeignKeyMatchSyntax = enum {
    simple,
    full,
};

pub const DdlForeignKeyOptionsSyntax = struct {
    on_delete: DdlForeignKeyActionSyntax = .restrict,
    on_update: DdlForeignKeyActionSyntax = .restrict,
    deferrable: bool = false,
    timing: DdlForeignKeyTimingSyntax = .immediate,
    match: DdlForeignKeyMatchSyntax = .simple,
};

pub const DdlForeignKeyColumnListSyntax = struct {
    columns: []const []const u8,
    period: ?[]const u8 = null,

    pub fn deinit(self: @This(), alloc: std.mem.Allocator) void {
        freeStringSlice(alloc, self.columns);
        if (self.period) |period| alloc.free(@constCast(period));
    }
};

pub const DdlPeriodSyntax = struct {
    name: []const u8,
    start_column: []const u8,
    end_column: []const u8,

    pub fn deinit(self: @This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.name));
        alloc.free(@constCast(self.start_column));
        alloc.free(@constCast(self.end_column));
    }
};

pub const DdlKnownDefaultSyntax = enum {
    null_literal,
    uuid_v4,
    now_ns,
    current_date_ns,
};

pub const DdlTypeSyntax = struct {
    field_type: runtime_schema.AntflyType,
    array_item_type: ?runtime_schema.AntflyType = null,
};

pub const TableClonePlan = ddl_plan.TableClonePlan;
pub const TableCloneOptions = ddl_plan.TableCloneOptions;
pub const CreateTableDefinitionHeaderSyntax = struct {
    table_name: []const u8,
    if_not_exists: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.table_name));
        self.* = undefined;
    }
};

pub const CreateTableElementKindSyntax = enum {
    column,
    period,
    primary_key,
    unique,
    foreign_key,
    check,
};

pub const CreateTableElementPrefixSyntax = struct {
    kind: CreateTableElementKindSyntax,
    constraint_name: ?[]const u8 = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.constraint_name) |constraint_name| alloc.free(@constCast(constraint_name));
        self.* = undefined;
    }
};

pub const CreateTableColumnConstraintKindSyntax = enum {
    primary_key,
    unique,
    check,
    references,
};

pub const CreateTableColumnConstraintPrefixSyntax = struct {
    kind: CreateTableColumnConstraintKindSyntax,
    constraint_name: ?[]const u8 = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.constraint_name) |constraint_name| alloc.free(@constCast(constraint_name));
        self.* = undefined;
    }
};

pub const CreateTablePartitionPlan = ddl_plan.CreateTablePartitionPlan;
pub const CreatePartitionedTableTailSyntax = struct {
    method: ddl_plan.TablePartitionMethod,
    keys: []const []const u8 = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        freeStringSlice(alloc, self.keys);
        self.* = undefined;
    }
};
pub const TablePartitionBounds = ddl_plan.TablePartitionBounds;
pub const TablePartitionCatalogPlan = ddl_plan.TablePartitionCatalogPlan;

pub const CreateViewSyntax = struct {
    view_name: []const u8,
    source_table_name: []const u8,
    source_fields: []const []const u8 = &.{},
    output_fields: []const []const u8 = &.{},
    replace_existing: bool = false,
    if_not_exists: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.view_name));
        alloc.free(@constCast(self.source_table_name));
        freeStringSlice(alloc, self.source_fields);
        freeStringSlice(alloc, self.output_fields);
        self.* = undefined;
    }
};

pub const CreateMaterializedViewSyntax = struct {
    view_name: []const u8,
    source_table_name: []const u8,
    source_fields: []const []const u8 = &.{},
    output_fields: []const []const u8 = &.{},
    replace_existing: bool = false,
    if_not_exists: bool = false,
    populate_on_create: bool = true,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.view_name));
        alloc.free(@constCast(self.source_table_name));
        freeStringSlice(alloc, self.source_fields);
        freeStringSlice(alloc, self.output_fields);
        self.* = undefined;
    }
};

pub const DropViewSyntax = struct {
    view_name: []const u8,
    if_exists: bool = false,
    cascade: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.view_name));
        self.* = undefined;
    }
};

pub const DropMaterializedViewSyntax = struct {
    view_name: []const u8,
    if_exists: bool = false,
    cascade: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.view_name));
        self.* = undefined;
    }
};

pub const RefreshMaterializedViewSyntax = struct {
    view_name: []const u8,
    concurrently: bool = false,
    populate: bool = true,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.view_name));
        self.* = undefined;
    }
};

pub const RenameViewSyntax = struct {
    view_name: []const u8,
    new_view_name: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.view_name));
        alloc.free(@constCast(self.new_view_name));
        self.* = undefined;
    }
};

pub const PrivilegeChangeActionSyntax = enum {
    grant,
    revoke,
};

pub const CreateRoleSyntax = struct {
    role_name: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.role_name));
        self.* = undefined;
    }
};

pub const AlterRoleSyntax = struct {
    role_name: []const u8,
    database_name: ?[]const u8 = null,
    operation: ddl_plan.AlterRolePlan.Operation = .set,
    setting_kind: ddl_plan.AlterRolePlan.SettingKind = .app,
    setting_name: []const u8,
    setting_value: ?ddl_plan.AlterRolePlan.SettingValue = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.role_name));
        if (self.database_name) |database_name| alloc.free(@constCast(database_name));
        alloc.free(@constCast(self.setting_name));
        if (self.setting_value) |*setting_value| setting_value.deinit(alloc);
        self.* = undefined;
    }
};

pub const DropRoleSyntax = struct {
    role_name: []const u8,
    if_exists: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.role_name));
        self.* = undefined;
    }
};

pub const PrivilegeChangeSyntax = struct {
    privileges: []const []const u8 = &.{},
    object_kind: []const u8,
    object_name: []const u8,
    principal_name: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        freeStringSlice(alloc, self.privileges);
        alloc.free(@constCast(self.object_kind));
        alloc.free(@constCast(self.object_name));
        alloc.free(@constCast(self.principal_name));
        self.* = undefined;
    }
};

pub const BulkIoDirectionSyntax = enum {
    from,
    to,
};

pub const BulkIoSyntax = struct {
    direction: BulkIoDirectionSyntax,
    table_name: []const u8,
    columns: []const []const u8 = &.{},
    endpoint_kind: ddl_plan.BulkIoEndpointKind = .stream,
    endpoint: []const u8,
    format: ?[]const u8 = null,
    header: bool = false,
    freeze: bool = false,
    on_error: ddl_plan.BulkIoOnErrorPolicy = .stop,
    reject_limit: ?usize = null,
    log_verbosity: ddl_plan.BulkIoLogVerbosity = .default,
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

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.table_name));
        freeStringSlice(alloc, self.columns);
        freeStringSlice(alloc, self.force_quote_columns);
        freeStringSlice(alloc, self.force_not_null_columns);
        freeStringSlice(alloc, self.force_null_columns);
        alloc.free(@constCast(self.endpoint));
        if (self.format) |format| alloc.free(@constCast(format));
        if (self.delimiter) |delimiter| alloc.free(@constCast(delimiter));
        if (self.quote) |quote| alloc.free(@constCast(quote));
        if (self.escape) |escape| alloc.free(@constCast(escape));
        if (self.null_marker) |null_marker| alloc.free(@constCast(null_marker));
        if (self.default_marker) |default_marker| alloc.free(@constCast(default_marker));
        if (self.encoding) |encoding| alloc.free(@constCast(encoding));
        self.* = undefined;
    }
};

pub const ListenNotificationSyntax = struct {
    channel_name: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.channel_name));
        self.* = undefined;
    }
};

pub const NotifyNotificationSyntax = struct {
    channel_name: []const u8,
    payload_json: ?[]const u8 = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.channel_name));
        if (self.payload_json) |payload| alloc.free(@constCast(payload));
        self.* = undefined;
    }
};

pub const UnlistenNotificationSyntax = struct {
    channel_name: ?[]const u8 = null,
    all: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.channel_name) |channel_name| alloc.free(@constCast(channel_name));
        self.* = undefined;
    }
};

pub const TruncateMutationSourceSyntax = struct {
    table_name: []const u8,
    additional_table_names: []const []const u8 = &.{},
    restart_identity: bool = false,
    cascade: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.table_name));
        freeStringSlice(alloc, self.additional_table_names);
        self.* = undefined;
    }
};

pub const CreatePublicationSyntax = struct {
    publication_name: []const u8,
    table_names: []const []const u8 = &.{},
    all_tables: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.publication_name));
        freeStringSlice(alloc, self.table_names);
        self.* = undefined;
    }
};

pub const AlterPublicationSyntax = struct {
    publication_name: []const u8,
    table_names: []const []const u8 = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.publication_name));
        freeStringSlice(alloc, self.table_names);
        self.* = undefined;
    }
};

pub const DropPublicationSyntax = struct {
    publication_name: []const u8,
    if_exists: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.publication_name));
        self.* = undefined;
    }
};

pub const CreateSubscriptionSyntax = struct {
    subscription_name: []const u8,
    connection_json: []const u8,
    publication_names: []const []const u8 = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.subscription_name));
        alloc.free(@constCast(self.connection_json));
        freeStringSlice(alloc, self.publication_names);
        self.* = undefined;
    }
};

pub const AlterSubscriptionSyntax = struct {
    subscription_name: []const u8,
    enabled: bool,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.subscription_name));
        self.* = undefined;
    }
};

pub const DropSubscriptionSyntax = struct {
    subscription_name: []const u8,
    if_exists: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.subscription_name));
        self.* = undefined;
    }
};

pub const CreateCollationSyntax = struct {
    collation_name: []const u8,
    option_count: usize = 0,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.collation_name));
        self.* = undefined;
    }
};

pub const RenameCollationSyntax = struct {
    collation_name: []const u8,
    new_collation_name: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.collation_name));
        alloc.free(@constCast(self.new_collation_name));
        self.* = undefined;
    }
};

pub const DropCollationSyntax = struct {
    collation_name: []const u8,
    if_exists: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.collation_name));
        self.* = undefined;
    }
};

pub const CreateOperatorSyntax = struct {
    operator_name: []const u8,
    option_count: usize = 0,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.operator_name));
        self.* = undefined;
    }
};

pub const DropOperatorSyntax = struct {
    operator_name: []const u8,
    argument_count: usize = 0,
    if_exists: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.operator_name));
        self.* = undefined;
    }
};

pub const CreateAggregateSyntax = struct {
    aggregate_name: []const u8,
    argument_count: usize = 0,
    option_count: usize = 0,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.aggregate_name));
        self.* = undefined;
    }
};

pub const DropAggregateSyntax = struct {
    aggregate_name: []const u8,
    argument_count: usize = 0,
    if_exists: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.aggregate_name));
        self.* = undefined;
    }
};

pub const CreateCastSyntax = struct {
    source_type: []const u8,
    target_type: []const u8,
    function_name: []const u8,
    assignment: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.source_type));
        alloc.free(@constCast(self.target_type));
        alloc.free(@constCast(self.function_name));
        self.* = undefined;
    }
};

pub const DropCastSyntax = struct {
    source_type: []const u8,
    target_type: []const u8,
    if_exists: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.source_type));
        alloc.free(@constCast(self.target_type));
        self.* = undefined;
    }
};

pub const RowClaimSyntax = struct {
    clause: ast.SqlRowClaimClause,
    targets: []const []const u8 = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.targets) |target| alloc.free(@constCast(target));
        if (self.targets.len > 0) alloc.free(self.targets);
        self.* = undefined;
    }
};

pub const NamedOrAllSyntax = struct {
    name: ?[]const u8 = null,
    all: bool = false,
};

pub const CursorScrollSyntax = enum {
    default,
    scroll,
    no_scroll,
};

pub const DeclareCursorPortalSyntax = struct {
    portal_name: []const u8,
    scroll: CursorScrollSyntax = .default,
    binary: bool = false,
    hold: bool = false,
    statement_kind: ?PreparedStatementSubjectSyntax = null,
};

pub const CursorFetchDirectionSyntax = enum {
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

pub const FetchCursorPortalSyntax = struct {
    portal_name: []const u8,
    direction: CursorFetchDirectionSyntax = .next,
    count: ?i64 = null,
};

pub const RelationPopulationMode = enum {
    create_table_as,
    select_into,
};

pub const RelationLifetimeKind = enum {
    temporary,
    unlogged,
};

pub const RelationLifetimePrefixSyntax = struct {
    kind: RelationLifetimeKind,
};

pub const RelationPopulationSyntax = struct {
    mode: RelationPopulationMode,
    target_identifier: []const u8,
    target_lifetime: ?RelationLifetimeKind = null,
    if_not_exists: bool = false,
    populate: bool = true,
    source_token_start: ?usize = null,
    source_token_end: ?usize = null,
    source_suffix_token_start: ?usize = null,
    source_suffix_token_end: ?usize = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        _ = alloc;
        self.* = undefined;
    }
};

fn skipSqlWhitespace(sql: []const u8, start: usize) usize {
    var index = start;
    while (index < sql.len and std.ascii.isWhitespace(sql[index])) : (index += 1) {}
    return index;
}

fn consumeSqlKeyword(sql: []const u8, index: *usize, keyword: []const u8) bool {
    const start = index.*;
    const end = start + keyword.len;
    if (end > sql.len) return false;
    if (!std.ascii.eqlIgnoreCase(sql[start..end], keyword)) return false;
    if (end < sql.len and isSqlIdentifierByte(sql[end])) return false;
    if (start > 0 and isSqlIdentifierByte(sql[start - 1])) return false;
    index.* = end;
    return true;
}

fn isSqlIdentifierByte(byte: u8) bool {
    return std.ascii.isAlphanumeric(byte) or byte == '_';
}

pub fn parseAlterRowSecurity(tokens: []const Token, pos: *usize) !?RowSecurityAlterSyntax {
    const start = pos.*;
    const cursor = parser.Cursor.init(tokens, pos);
    if (!cursor.matchKeyword("table")) return null;
    const table_token = cursor.matchToken(.identifier) orelse {
        pos.* = start;
        return error.UnsupportedSqlShape;
    };
    const enabled = if (cursor.matchKeyword("enable"))
        true
    else if (cursor.matchKeyword("disable"))
        false
    else {
        pos.* = start;
        return null;
    };
    try cursor.expectKeyword("row");
    try cursor.expectKeyword("level");
    try cursor.expectKeyword("security");
    if (cursor.matchToken(.semicolon) != null and !cursor.atEnd()) return error.UnsupportedSqlShape;
    if (!cursor.atEnd()) return error.UnsupportedSqlShape;
    return .{ .table_identifier = table_token.text, .enabled = enabled };
}

pub fn parseCreateRowSecurityPolicyCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !CreateRowSecurityPolicySyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("policy");
    const policy_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var policy_transferred = false;
    errdefer if (!policy_transferred) alloc.free(policy_name);
    try cursor.expectKeyword("on");
    const table_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    var table_transferred = false;
    errdefer if (!table_transferred) alloc.free(table_name);
    const role_targets = try parseOptionalRowSecurityPolicyRoleTargetsAlloc(alloc, tokens, pos);
    var role_targets_transferred = false;
    errdefer if (!role_targets_transferred) freeStringSlice(alloc, role_targets);
    try cursor.expectKeyword("using");
    var predicate = try parseRowSecurityPolicyPredicateAlloc(alloc, cursor, tokens, pos);
    var predicate_transferred = false;
    errdefer if (!predicate_transferred) predicate.deinit(alloc);
    var check_predicate: ?ddl_plan.RowSecurityPolicyPredicate = null;
    var check_predicate_transferred = true;
    if (cursor.matchKeyword("with")) {
        try cursor.expectKeyword("check");
        check_predicate = try parseRowSecurityPolicyPredicateAlloc(alloc, cursor, tokens, pos);
        check_predicate_transferred = false;
        errdefer if (!check_predicate_transferred) if (check_predicate) |*value| value.deinit(alloc);
    }
    try adapterNoopStatementEnd(cursor);

    policy_transferred = true;
    table_transferred = true;
    role_targets_transferred = true;
    predicate_transferred = true;
    check_predicate_transferred = true;
    return .{
        .policy_name = policy_name,
        .table_name = table_name,
        .role_targets = role_targets,
        .predicate = predicate,
        .check_predicate = check_predicate,
    };
}

pub fn parseAlterRowSecurityPolicyCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !AlterRowSecurityPolicySyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("policy");
    const policy_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var policy_transferred = false;
    errdefer if (!policy_transferred) alloc.free(policy_name);
    try cursor.expectKeyword("on");
    const table_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    var table_transferred = false;
    errdefer if (!table_transferred) alloc.free(table_name);
    var role_targets_present = false;
    var role_targets: []const []const u8 = &.{};
    if (cursor.matchKeyword("to")) {
        role_targets_present = true;
        role_targets = try parseRowSecurityPolicyRoleTargetsAfterToAlloc(alloc, tokens, pos);
    }
    var role_targets_transferred = false;
    errdefer if (!role_targets_transferred) freeStringSlice(alloc, role_targets);
    var predicate: ?ddl_plan.RowSecurityPolicyPredicate = null;
    var predicate_transferred = true;
    if (cursor.matchKeyword("using")) {
        predicate = try parseRowSecurityPolicyPredicateAlloc(alloc, cursor, tokens, pos);
        predicate_transferred = false;
        errdefer if (!predicate_transferred) if (predicate) |*value| value.deinit(alloc);
    }
    var check_predicate: ?ddl_plan.RowSecurityPolicyPredicate = null;
    var check_predicate_transferred = true;
    if (cursor.matchKeyword("with")) {
        try cursor.expectKeyword("check");
        check_predicate = try parseRowSecurityPolicyPredicateAlloc(alloc, cursor, tokens, pos);
        check_predicate_transferred = false;
        errdefer if (!check_predicate_transferred) if (check_predicate) |*value| value.deinit(alloc);
    }
    if (!role_targets_present and predicate == null and check_predicate == null) return error.UnsupportedSqlShape;
    try adapterNoopStatementEnd(cursor);

    policy_transferred = true;
    table_transferred = true;
    role_targets_transferred = true;
    predicate_transferred = true;
    check_predicate_transferred = true;
    return .{
        .policy_name = policy_name,
        .table_name = table_name,
        .role_targets_present = role_targets_present,
        .role_targets = role_targets,
        .predicate = predicate,
        .check_predicate = check_predicate,
    };
}

pub fn parseOptionalRowSecurityPolicyRoleTargetsAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) ![]const []const u8 {
    const cursor = parser.Cursor.init(tokens, pos);
    if (!cursor.matchKeyword("to")) return &.{};
    return try parseRowSecurityPolicyRoleTargetsAfterToAlloc(alloc, tokens, pos);
}

pub fn parseRowSecurityPolicyRoleTargetsAfterToAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) ![]const []const u8 {
    const cursor = parser.Cursor.init(tokens, pos);
    if (cursor.matchKeyword("public")) return &.{};
    return try parseIdentifierListAlloc(alloc, tokens, pos);
}

pub fn parseDropRowSecurityPolicyCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !DropRowSecurityPolicySyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("policy");
    var if_exists = false;
    if (cursor.matchKeyword("if")) {
        try cursor.expectKeyword("exists");
        if_exists = true;
    }
    const policy_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var policy_transferred = false;
    errdefer if (!policy_transferred) alloc.free(policy_name);
    try cursor.expectKeyword("on");
    const table_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    var table_transferred = false;
    errdefer if (!table_transferred) alloc.free(table_name);
    _ = cursor.matchKeyword("cascade") or cursor.matchKeyword("restrict");
    try adapterNoopStatementEnd(cursor);
    policy_transferred = true;
    table_transferred = true;
    return .{ .policy_name = policy_name, .table_name = table_name, .if_exists = if_exists };
}

fn parseRowSecurityPolicyPredicateAlloc(
    alloc: std.mem.Allocator,
    cursor: parser.Cursor,
    tokens: []const Token,
    pos: *usize,
) !ddl_plan.RowSecurityPolicyPredicate {
    try cursor.expectToken(.lparen);
    var predicate = try parseRowSecurityPolicyOrPredicateAlloc(alloc, cursor, tokens, pos);
    errdefer predicate.deinit(alloc);
    try cursor.expectToken(.rparen);
    return predicate;
}

fn parseRowSecurityPolicyOrPredicateAlloc(
    alloc: std.mem.Allocator,
    cursor: parser.Cursor,
    tokens: []const Token,
    pos: *usize,
) !ddl_plan.RowSecurityPolicyPredicate {
    var predicates = std.ArrayList(ddl_plan.RowSecurityPolicyPredicate).empty;
    var predicates_transferred = false;
    errdefer if (!predicates_transferred) freeRowSecurityPolicyPredicateList(alloc, &predicates);

    var first = try parseRowSecurityPolicyAndPredicateAlloc(alloc, cursor, tokens, pos);
    var first_transferred = false;
    errdefer if (!first_transferred) first.deinit(alloc);
    try predicates.append(alloc, first);
    first_transferred = true;
    while (true) {
        if (!cursor.matchKeyword("or")) break;
        var term = try parseRowSecurityPolicyAndPredicateAlloc(alloc, cursor, tokens, pos);
        var term_transferred = false;
        errdefer if (!term_transferred) term.deinit(alloc);
        try predicates.append(alloc, term);
        term_transferred = true;
    }

    if (predicates.items.len == 1) {
        const single = predicates.items[0];
        predicates_transferred = true;
        predicates.deinit(alloc);
        return single;
    }

    const owned = try predicates.toOwnedSlice(alloc);
    predicates_transferred = true;
    return .{ .disjunction = .{ .predicates = owned } };
}

fn parseRowSecurityPolicyAndPredicateAlloc(
    alloc: std.mem.Allocator,
    cursor: parser.Cursor,
    tokens: []const Token,
    pos: *usize,
) !ddl_plan.RowSecurityPolicyPredicate {
    var predicates = std.ArrayList(ddl_plan.RowSecurityPolicyPredicate).empty;
    var predicates_transferred = false;
    errdefer if (!predicates_transferred) freeRowSecurityPolicyPredicateList(alloc, &predicates);

    var first = try parseRowSecurityPolicyPredicateAtomAlloc(alloc, cursor, tokens, pos);
    var first_transferred = false;
    errdefer if (!first_transferred) first.deinit(alloc);
    try predicates.append(alloc, first);
    first_transferred = true;
    while (true) {
        if (!cursor.matchKeyword("and")) break;
        var term = try parseRowSecurityPolicyPredicateAtomAlloc(alloc, cursor, tokens, pos);
        var term_transferred = false;
        errdefer if (!term_transferred) term.deinit(alloc);
        try predicates.append(alloc, term);
        term_transferred = true;
    }

    if (predicates.items.len == 1) {
        const single = predicates.items[0];
        predicates_transferred = true;
        predicates.deinit(alloc);
        return single;
    }

    const owned = try predicates.toOwnedSlice(alloc);
    predicates_transferred = true;
    return .{ .conjunction = .{ .predicates = owned } };
}

fn freeRowSecurityPolicyPredicateList(
    alloc: std.mem.Allocator,
    predicates: *std.ArrayList(ddl_plan.RowSecurityPolicyPredicate),
) void {
    for (predicates.items) |*predicate| predicate.deinit(alloc);
    predicates.deinit(alloc);
}

fn parseRowSecurityPolicyPredicateAtomAlloc(
    alloc: std.mem.Allocator,
    cursor: parser.Cursor,
    tokens: []const Token,
    pos: *usize,
) !ddl_plan.RowSecurityPolicyPredicate {
    const start = pos.*;
    if (parseRowSecurityPolicySimplePredicateAtomAlloc(alloc, cursor, tokens, pos)) |predicate| {
        return predicate;
    } else |err| {
        switch (err) {
            error.UnsupportedSqlShape => pos.* = start,
            else => return err,
        }
    }
    return try parseRowSecurityPolicyExpressionPredicateAtomAlloc(alloc, cursor, tokens, pos);
}

fn parseRowSecurityPolicySimplePredicateAtomAlloc(
    alloc: std.mem.Allocator,
    cursor: parser.Cursor,
    tokens: []const Token,
    pos: *usize,
) !ddl_plan.RowSecurityPolicyPredicate {
    const field = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    var field_transferred = false;
    errdefer if (!field_transferred) alloc.free(field);
    try cursor.expectToken(.eq);
    if (cursor.matchKeyword("current_setting")) {
        try cursor.expectToken(.lparen);
        const setting_token = cursor.matchToken(.string) orelse return error.UnsupportedSqlShape;
        if (setting_token.text.len == 0) return error.UnsupportedSqlShape;
        const setting_name = try alloc.dupe(u8, setting_token.text);
        var setting_transferred = false;
        errdefer if (!setting_transferred) alloc.free(setting_name);
        try cursor.expectToken(.rparen);

        field_transferred = true;
        setting_transferred = true;
        return .{ .current_setting_equals = .{
            .field = field,
            .setting_name = setting_name,
        } };
    }

    const value_json = try sql_value.parseSqlUntypedValueJsonAlloc(alloc, tokens, pos);
    var value_transferred = false;
    errdefer if (!value_transferred) alloc.free(value_json);

    field_transferred = true;
    value_transferred = true;
    return .{ .literal_equals = .{
        .field = field,
        .value_json = value_json,
    } };
}

fn parseRowSecurityPolicyExpressionPredicateAtomAlloc(
    alloc: std.mem.Allocator,
    cursor: parser.Cursor,
    tokens: []const Token,
    pos: *usize,
) !ddl_plan.RowSecurityPolicyPredicate {
    _ = pos;
    _ = tokens;
    const lhs = try parseDdlGeneratedRowExpressionAlloc(alloc, cursor);
    var lhs_transferred = false;
    errdefer if (!lhs_transferred) runtime_schema.freeRelationalRowsExpression(alloc, lhs);

    const op = try parseRowSecurityExpressionComparisonOp(cursor);

    const rhs = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
    var rhs_transferred = false;
    errdefer {
        if (!rhs_transferred) alloc.free(rhs);
    }
    rhs[0] = try parseDdlGeneratedRowExpressionAlloc(alloc, cursor);
    errdefer if (!rhs_transferred) runtime_schema.freeRelationalRowsExpression(alloc, rhs[0]);

    lhs_transferred = true;
    rhs_transferred = true;
    return .{ .expression = .{
        .lhs = lhs,
        .op = op,
        .rhs = rhs,
    } };
}

fn parseRowSecurityExpressionComparisonOp(cursor: parser.Cursor) !runtime_schema.RelationalCheckOp {
    if (cursor.matchToken(.eq) != null) return .eq;
    if (cursor.matchToken(.neq) != null) return .ne;
    if (cursor.matchToken(.gt) != null) return .gt;
    if (cursor.matchToken(.gte) != null) return .gte;
    if (cursor.matchToken(.lt) != null) return .lt;
    if (cursor.matchToken(.lte) != null) return .lte;
    return error.UnsupportedSqlShape;
}

pub fn parseDropUpdatePolicyTriggerCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !DropUpdatePolicySyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("trigger");
    var if_exists = false;
    if (cursor.matchKeyword("if")) {
        try cursor.expectKeyword("exists");
        if_exists = true;
    }
    const trigger_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var trigger_transferred = false;
    errdefer if (!trigger_transferred) alloc.free(trigger_name);
    try cursor.expectKeyword("on");
    const table_name = try parseSqlTableReferenceIdentifierOwnedAlloc(alloc, tokens, pos);
    var table_transferred = false;
    errdefer if (!table_transferred) alloc.free(table_name);
    _ = cursor.matchKeyword("cascade") or cursor.matchKeyword("restrict");
    try adapterNoopStatementEnd(cursor);
    trigger_transferred = true;
    table_transferred = true;
    return .{ .trigger_name = trigger_name, .table_name = table_name, .if_exists = if_exists };
}

pub fn parseCreateUpdatePolicyTriggerCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !CreateUpdatePolicySyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("trigger");
    const trigger_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var trigger_transferred = false;
    errdefer if (!trigger_transferred) alloc.free(trigger_name);
    try cursor.expectKeyword("before");
    try cursor.expectKeyword("update");
    if (cursor.matchKeyword("of")) {
        while (!cursor.peekKeyword("on")) {
            const column = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
            alloc.free(column);
            if (cursor.matchToken(.comma) == null) break;
        }
    }
    try cursor.expectKeyword("on");
    const table_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    var table_transferred = false;
    errdefer if (!table_transferred) alloc.free(table_name);
    if (cursor.matchKeyword("for")) {
        try cursor.expectKeyword("each");
        try cursor.expectKeyword("row");
    }
    try cursor.expectKeyword("execute");
    if (!(cursor.matchKeyword("function") or cursor.matchKeyword("procedure"))) return error.UnsupportedSqlShape;
    const function_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    defer alloc.free(function_name);
    if (!isSupportedUpdatedAtTriggerFunction(function_name)) return error.UnsupportedSqlShape;
    try cursor.expectToken(.lparen);
    const column_name = if (cursor.matchToken(.rparen) != null)
        try alloc.dupe(u8, "updated_at")
    else blk: {
        const parsed_column = if (cursor.matchToken(.string)) |token|
            try alloc.dupe(u8, token.text)
        else
            try parseIdentifierOwnedAlloc(alloc, tokens, pos);
        var parsed_transferred = false;
        errdefer if (!parsed_transferred) alloc.free(parsed_column);
        if (cursor.matchToken(.comma) != null) return error.UnsupportedSqlShape;
        try cursor.expectToken(.rparen);
        parsed_transferred = true;
        break :blk parsed_column;
    };
    var column_transferred = false;
    errdefer if (!column_transferred) alloc.free(column_name);
    try adapterNoopStatementEnd(cursor);

    trigger_transferred = true;
    table_transferred = true;
    column_transferred = true;
    return .{ .trigger_name = trigger_name, .table_name = table_name, .column_name = column_name };
}

fn isSupportedUpdatedAtTriggerFunction(name: []const u8) bool {
    const dot = std.mem.lastIndexOfScalar(u8, name, '.');
    const base = if (dot) |idx| name[idx + 1 ..] else name;
    return std.ascii.eqlIgnoreCase(base, "touch_updated_at") or
        std.ascii.eqlIgnoreCase(base, "set_updated_at") or
        std.ascii.eqlIgnoreCase(base, "update_updated_at") or
        std.ascii.eqlIgnoreCase(base, "antfly_on_update_now") or
        std.ascii.eqlIgnoreCase(base, "antfly_touch_updated_at");
}

pub fn parseAdapterNoopSetStatementTail(tokens: []const Token, pos: *usize) !void {
    var cursor = parser.Cursor.init(tokens, pos);
    const local = cursor.matchKeyword("local");
    if (!local) _ = cursor.matchKeyword("session");

    const setting = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
    if (std.ascii.eqlIgnoreCase(setting.text, "search_path")) {
        try parseAdapterNoopPublicSearchPathTail(cursor);
        return;
    }
    if (!adapterNoopSetSessionSettingAllowed(setting.text)) return error.UnsupportedSqlShape;

    if (cursor.matchToken(.eq) == null and !cursor.matchKeyword("to")) return error.UnsupportedSqlShape;
    try parseAdapterNoopSetValueTail(cursor, setting.text);
}

pub fn parseSetSearchPathTailAlloc(alloc: std.mem.Allocator, tokens: []const Token, pos: *usize) !SetSearchPathSyntax {
    var cursor = parser.Cursor.init(tokens, pos);
    const local = cursor.matchKeyword("local");
    if (!local) _ = cursor.matchKeyword("session");
    const setting = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
    if (!std.ascii.eqlIgnoreCase(setting.text, "search_path")) return error.UnsupportedSqlShape;
    if (cursor.matchToken(.eq) == null and !cursor.matchKeyword("to")) return error.UnsupportedSqlShape;

    var namespaces = std.ArrayListUnmanaged([]const u8).empty;
    errdefer freeStringList(alloc, &namespaces);
    while (true) {
        const token = cursor.matchToken(.identifier) orelse cursor.matchToken(.string) orelse return error.UnsupportedSqlShape;
        if (!sqlSessionNamespaceNameValid(token.text)) return error.UnsupportedSqlShape;
        try namespaces.append(alloc, try alloc.dupe(u8, token.text));
        if (cursor.matchToken(.comma) == null) break;
    }
    try adapterNoopStatementEnd(cursor);
    return .{ .namespaces = try namespaces.toOwnedSlice(alloc), .local = local };
}

pub fn parseSetSessionSettingTailAlloc(alloc: std.mem.Allocator, tokens: []const Token, pos: *usize) !ddl_plan.SetSessionSettingPlan {
    var cursor = parser.Cursor.init(tokens, pos);
    const local = cursor.matchKeyword("local");
    if (!local) _ = cursor.matchKeyword("session");
    const setting = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
    const kind = sessionSettingKindForName(setting.text) orelse return error.UnsupportedSqlShape;
    if (cursor.matchToken(.eq) == null and !cursor.matchKeyword("to")) return error.UnsupportedSqlShape;
    const value = cursor.matchToken(.identifier) orelse cursor.matchToken(.string) orelse cursor.matchToken(.number) orelse return error.UnsupportedSqlShape;
    if (value.text.len == 0) return error.UnsupportedSqlShape;
    try validateSetSessionSettingValue(setting.text, kind, value.text);
    const name_owned = try alloc.dupe(u8, setting.text);
    errdefer alloc.free(name_owned);
    const value_owned = try alloc.dupe(u8, value.text);
    errdefer alloc.free(value_owned);
    try adapterNoopStatementEnd(cursor);
    return .{
        .name = name_owned,
        .value = value_owned,
        .kind = kind,
        .local = local,
    };
}

pub fn parseAdapterNoopResetStatementTail(tokens: []const Token, pos: *usize) !void {
    var cursor = parser.Cursor.init(tokens, pos);
    if (cursor.matchKeyword("all")) {
        try adapterNoopStatementEnd(cursor);
        return;
    }

    const setting = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
    if (!adapterNoopResetSessionSettingAllowed(setting.text)) return error.UnsupportedSqlShape;
    try adapterNoopStatementEnd(cursor);
}

pub fn parseResetAllTail(tokens: []const Token, pos: *usize) !void {
    var cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("all");
    try adapterNoopStatementEnd(cursor);
}

pub fn parseResetSessionSettingTailAlloc(alloc: std.mem.Allocator, tokens: []const Token, pos: *usize) !ddl_plan.ResetSessionSettingPlan {
    var cursor = parser.Cursor.init(tokens, pos);
    const setting = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
    const kind = sessionSettingKindForName(setting.text) orelse return error.UnsupportedSqlShape;
    const name_owned = try alloc.dupe(u8, setting.text);
    errdefer alloc.free(name_owned);
    try adapterNoopStatementEnd(cursor);
    return .{ .name = name_owned, .kind = kind };
}

pub fn parseResetSearchPathTail(tokens: []const Token, pos: *usize) !void {
    var cursor = parser.Cursor.init(tokens, pos);
    const setting = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
    if (!std.ascii.eqlIgnoreCase(setting.text, "search_path")) return error.UnsupportedSqlShape;
    try adapterNoopStatementEnd(cursor);
}

pub fn parseAdapterNoopShowStatementTail(tokens: []const Token, pos: *usize) !void {
    var cursor = parser.Cursor.init(tokens, pos);
    const setting = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
    if (!adapterNoopShowSessionSettingAllowed(setting.text)) return error.UnsupportedSqlShape;
    try adapterNoopStatementEnd(cursor);
}

pub fn parseShowSearchPathTail(tokens: []const Token, pos: *usize) !void {
    var cursor = parser.Cursor.init(tokens, pos);
    const setting = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
    if (!std.ascii.eqlIgnoreCase(setting.text, "search_path")) return error.UnsupportedSqlShape;
    try adapterNoopStatementEnd(cursor);
}

pub fn parseAdapterNoopDiscardStatementTail(tokens: []const Token, pos: *usize) !void {
    var cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("all");
    try adapterNoopStatementEnd(cursor);
}

pub fn parseDiscardAllTail(tokens: []const Token, pos: *usize) !void {
    var cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("all");
    try adapterNoopStatementEnd(cursor);
}

pub fn matchAdapterNoopTransactionBoundaryTail(
    tokens: []const Token,
    pos: *usize,
    options: AdapterNoopTransactionBoundaryTail,
) !bool {
    var cursor = parser.Cursor.init(tokens, pos);
    const checkpoint = cursor.checkpoint();
    if (try matchAdapterNoopStatementEnd(cursor)) return true;
    if ((options.work and cursor.matchKeyword("work")) or
        (options.transaction and cursor.matchKeyword("transaction")))
    {
        if (try matchAdapterNoopStatementEnd(cursor)) return true;
    }
    cursor.restore(checkpoint);
    return false;
}

pub fn parseSavepointTransactionTail(tokens: []const Token, pos: *usize) !SavepointNameSyntax {
    return try parseSavepointNameTail(tokens, pos);
}

pub fn parseReleaseSavepointTail(tokens: []const Token, pos: *usize) !SavepointNameSyntax {
    var cursor = parser.Cursor.init(tokens, pos);
    _ = cursor.matchKeyword("savepoint");
    return try parseSavepointNameTailFromCursor(cursor);
}

pub fn parseRollbackToSavepointTail(tokens: []const Token, pos: *usize) !SavepointNameSyntax {
    var cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("to");
    _ = cursor.matchKeyword("savepoint");
    return try parseSavepointNameTailFromCursor(cursor);
}

pub fn parsePreparedTransactionTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    action: ddl_plan.PreparedTransactionAction,
) !ddl_plan.PreparedTransactionPlan {
    var cursor = parser.Cursor.init(tokens, pos);
    switch (action) {
        .prepare => try cursor.expectKeyword("transaction"),
        .commit, .rollback => try cursor.expectKeyword("prepared"),
    }
    const gid_token = cursor.matchToken(.string) orelse return error.UnsupportedSqlShape;
    if (gid_token.text.len == 0) return error.UnsupportedSqlShape;
    try adapterNoopStatementEnd(cursor);
    return .{
        .action = action,
        .gid = try alloc.dupe(u8, gid_token.text),
    };
}

pub fn parseForRowClaimClauseAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !RowClaimSyntax {
    var cursor = parser.Cursor.init(tokens, pos);
    const mode: db_mod.types.RowClaimMode = if (cursor.matchKeyword("no")) blk: {
        try cursor.expectKeyword("key");
        try cursor.expectKeyword("update");
        break :blk .for_no_key_update;
    } else if (cursor.matchKeyword("key")) blk: {
        try cursor.expectKeyword("share");
        break :blk .for_key_share;
    } else if (cursor.matchKeyword("share")) blk: {
        break :blk .for_share;
    } else blk: {
        try cursor.expectKeyword("update");
        break :blk .for_update;
    };

    var targets = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (targets.items) |target| alloc.free(@constCast(target));
        targets.deinit(alloc);
    }
    if (cursor.matchKeyword("of")) {
        while (true) {
            _ = cursor.matchKeyword("only");
            const target = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
            try targets.append(alloc, try alloc.dupe(u8, target.text));
            if (cursor.matchToken(.comma) == null) break;
        }
    }

    const wait_policy: db_mod.types.RowClaimWaitPolicy = if (cursor.matchKeyword("skip")) blk: {
        try cursor.expectKeyword("locked");
        break :blk .skip_locked;
    } else if (cursor.matchKeyword("nowait"))
        .nowait
    else
        .wait;

    return .{
        .clause = .{ .mode = mode, .wait_policy = wait_policy },
        .targets = try targets.toOwnedSlice(alloc),
    };
}

pub fn rowClaimTargetAllowed(alloc: std.mem.Allocator, target: []const u8, allowed_targets: []const []const u8) bool {
    for (allowed_targets) |allowed| {
        if (allowed.len > 0 and std.mem.eql(u8, target, allowed)) return true;
    }
    const normalized = normalizeSqlObjectIdentifierAlloc(alloc, target) catch return false;
    defer alloc.free(normalized);
    for (allowed_targets) |allowed| {
        if (allowed.len > 0 and std.mem.eql(u8, normalized, allowed)) return true;
    }
    return false;
}

pub fn parseCheckedForRowClaimClauseAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    allowed_targets: []const []const u8,
) !ast.SqlRowClaimClause {
    var syntax = try parseForRowClaimClauseAlloc(alloc, tokens, pos);
    defer syntax.deinit(alloc);
    for (syntax.targets) |target| {
        if (!rowClaimTargetAllowed(alloc, target, allowed_targets)) return error.UnsupportedSqlShape;
    }
    return syntax.clause;
}

pub fn parseExclusiveForRowClaimClauseAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    allowed_targets: []const []const u8,
) !ast.SqlRowClaimClause {
    const clause = try parseCheckedForRowClaimClauseAlloc(alloc, tokens, pos, allowed_targets);
    if (!clause.mode.isExclusiveWriteClaim()) return error.UnsupportedSqlShape;
    return clause;
}

pub fn parseDeallocatePreparedStatementTail(tokens: []const Token, pos: *usize) !NamedOrAllSyntax {
    var cursor = parser.Cursor.init(tokens, pos);
    _ = cursor.matchKeyword("prepare");
    return try parseNamedOrAllTail(cursor);
}

pub fn parsePrepareStatementTailAlloc(alloc: std.mem.Allocator, tokens: []const Token, pos: *usize) !PrepareStatementSyntax {
    var cursor = parser.Cursor.init(tokens, pos);
    const statement_token = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
    const parameter_count = if (cursor.peekKind(.lparen)) try countParenthesizedTypeList(cursor) else 0;
    try cursor.expectKeyword("as");
    const statement_family = try generatedPreparedStatementStatementKindAlloc(alloc, tokens[cursor.checkpoint()..]);
    const statement_kind = preparedStatementSubjectKindFromStatementKind(statement_family);
    try consumePreparedStatementSubjectTail(cursor);
    return .{
        .statement_name = statement_token.text,
        .parameter_count = parameter_count,
        .statement_kind = statement_kind,
        .statement_family = statement_family,
    };
}

pub fn parseExecutePreparedStatementTail(tokens: []const Token, pos: *usize) !ExecutePreparedStatementSyntax {
    var cursor = parser.Cursor.init(tokens, pos);
    const statement_token = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
    const argument_count = try countParenthesizedUntypedValues(cursor);
    try adapterNoopStatementEnd(cursor);
    return .{
        .statement_name = statement_token.text,
        .argument_count = argument_count,
    };
}

pub fn parseDeclareCursorPortalPrefix(tokens: []const Token, pos: *usize) !DeclareCursorPortalSyntax {
    var cursor = parser.Cursor.init(tokens, pos);
    const portal_token = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
    var binary = false;
    var scroll: CursorScrollSyntax = .default;
    var hold = false;
    while (true) {
        if (cursor.matchKeyword("binary")) {
            binary = true;
        } else if (cursor.matchKeyword("scroll")) {
            scroll = .scroll;
        } else if (cursor.matchKeyword("no")) {
            try cursor.expectKeyword("scroll");
            scroll = .no_scroll;
        } else {
            break;
        }
    }
    try cursor.expectKeyword("cursor");
    if (cursor.matchKeyword("with")) {
        try cursor.expectKeyword("hold");
        hold = true;
    } else if (cursor.matchKeyword("without")) {
        try cursor.expectKeyword("hold");
        hold = false;
    }
    try cursor.expectKeyword("for");
    return .{
        .portal_name = portal_token.text,
        .scroll = scroll,
        .binary = binary,
        .hold = hold,
    };
}

pub fn parseDeclareCursorPortalTailAlloc(alloc: std.mem.Allocator, tokens: []const Token, pos: *usize) !DeclareCursorPortalSyntax {
    var syntax = try parseDeclareCursorPortalPrefix(tokens, pos);
    const cursor = parser.Cursor.init(tokens, pos);
    syntax.statement_kind = try generatedPreparedStatementSubjectKindAlloc(alloc, tokens[cursor.checkpoint()..]);
    try consumePreparedStatementSubjectTail(cursor);
    return syntax;
}

pub fn parseFetchCursorPortalTail(tokens: []const Token, pos: *usize) !FetchCursorPortalSyntax {
    var cursor = parser.Cursor.init(tokens, pos);
    var direction: CursorFetchDirectionSyntax = .next;
    var count: ?i64 = null;
    if (cursor.matchKeyword("next")) {
        direction = .next;
    } else if (cursor.matchKeyword("prior")) {
        direction = .prior;
    } else if (cursor.matchKeyword("first")) {
        direction = .first;
    } else if (cursor.matchKeyword("last")) {
        direction = .last;
    } else if (cursor.matchKeyword("all")) {
        direction = .all;
    } else if (cursor.matchKeyword("forward")) {
        direction = .forward;
        count = try parseOptionalCursorFetchCount(cursor);
    } else if (cursor.matchKeyword("backward")) {
        direction = .backward;
        count = try parseOptionalCursorFetchCount(cursor);
    } else if (cursor.matchKeyword("absolute")) {
        direction = .absolute;
        count = try parseCursorFetchCount(cursor);
    } else if (cursor.matchKeyword("relative")) {
        direction = .relative;
        count = try parseCursorFetchCount(cursor);
    } else if (peekCursorFetchCount(cursor)) {
        direction = .forward;
        count = try parseCursorFetchCount(cursor);
    }
    _ = cursor.matchKeyword("from") or cursor.matchKeyword("in");
    const portal_token = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
    try adapterNoopStatementEnd(cursor);
    return .{
        .portal_name = portal_token.text,
        .direction = direction,
        .count = count,
    };
}

pub fn parseCloseCursorPortalTail(tokens: []const Token, pos: *usize) !NamedOrAllSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    return try parseNamedOrAllTail(cursor);
}

pub fn parseTableLockTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !TableLockSyntax {
    var cursor = parser.Cursor.init(tokens, pos);
    _ = cursor.matchKeyword("table");
    var table_names = std.ArrayListUnmanaged([]const u8).empty;
    errdefer freeStringList(alloc, &table_names);
    while (true) {
        const table_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
        var table_transferred = false;
        errdefer if (!table_transferred) alloc.free(table_name);
        try table_names.append(alloc, table_name);
        table_transferred = true;
        if (cursor.matchToken(.comma) == null) break;
    }
    if (table_names.items.len == 0) return error.UnsupportedSqlShape;
    try cursor.expectKeyword("in");
    const mode = try parseTableLockMode(cursor);
    try cursor.expectKeyword("mode");
    try adapterNoopStatementEnd(cursor);
    return .{
        .table_names = try table_names.toOwnedSlice(alloc),
        .mode = mode,
    };
}

pub fn parseConstraintModeTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !ConstraintModeSyntax {
    var cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("constraints");
    var all = false;
    var constraint_names = std.ArrayListUnmanaged([]const u8).empty;
    errdefer freeStringList(alloc, &constraint_names);
    if (cursor.matchKeyword("all")) {
        all = true;
    } else {
        while (true) {
            const constraint_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
            var constraint_transferred = false;
            errdefer if (!constraint_transferred) alloc.free(constraint_name);
            try constraint_names.append(alloc, constraint_name);
            constraint_transferred = true;
            if (cursor.matchToken(.comma) == null) break;
        }
    }
    const mode: ConstraintCheckModeSyntax = if (cursor.matchKeyword("immediate"))
        .immediate
    else if (cursor.matchKeyword("deferred"))
        .deferred
    else
        return error.UnsupportedSqlShape;
    try adapterNoopStatementEnd(cursor);
    return .{
        .all = all,
        .constraint_names = try constraint_names.toOwnedSlice(alloc),
        .mode = mode,
    };
}

pub fn parseTransactionModeTail(
    tokens: []const Token,
    pos: *usize,
    starter: TransactionModeStarterSyntax,
) !TransactionModeSyntax {
    var cursor = parser.Cursor.init(tokens, pos);
    switch (starter) {
        .set_transaction, .start_transaction => try cursor.expectKeyword("transaction"),
        .begin => _ = cursor.matchKeyword("transaction"),
    }
    var syntax = TransactionModeSyntax{ .starter = starter };
    var saw_mode = false;
    while (!cursor.atEnd()) {
        if (cursor.matchToken(.semicolon) != null) {
            if (!cursor.atEnd()) return error.UnsupportedSqlShape;
            break;
        }
        _ = cursor.matchToken(.comma);
        if (cursor.matchKeyword("isolation")) {
            try cursor.expectKeyword("level");
            if (syntax.isolation_level != null) return error.UnsupportedSqlShape;
            syntax.isolation_level = try parseTransactionIsolationLevel(cursor);
            saw_mode = true;
        } else if (cursor.matchKeyword("read")) {
            if (syntax.access_mode != null) return error.UnsupportedSqlShape;
            if (cursor.matchKeyword("only")) {
                syntax.access_mode = .read_only;
            } else if (cursor.matchKeyword("write")) {
                syntax.access_mode = .read_write;
            } else {
                return error.UnsupportedSqlShape;
            }
            saw_mode = true;
        } else if (cursor.matchKeyword("not")) {
            try cursor.expectKeyword("deferrable");
            if (syntax.deferrable != null) return error.UnsupportedSqlShape;
            syntax.deferrable = false;
            saw_mode = true;
        } else if (cursor.matchKeyword("deferrable")) {
            if (syntax.deferrable != null) return error.UnsupportedSqlShape;
            syntax.deferrable = true;
            saw_mode = true;
        } else {
            return error.UnsupportedSqlShape;
        }
    }
    if (!saw_mode) return error.UnsupportedSqlShape;
    return syntax;
}

pub fn parseAdvisoryLockTail(tokens: []const Token, pos: *usize) !AdvisoryLockSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    const action: AdvisoryLockActionSyntax = if (cursor.matchKeyword("pg_advisory_lock"))
        .lock
    else if (cursor.matchKeyword("pg_advisory_unlock"))
        .unlock
    else
        return error.UnsupportedSqlShape;
    try cursor.expectToken(.lparen);
    const key1 = try parseSequenceInteger(cursor);
    const key2 = if (cursor.matchToken(.comma) != null) try parseSequenceInteger(cursor) else null;
    try cursor.expectToken(.rparen);
    try adapterNoopStatementEnd(cursor);
    return .{
        .action = action,
        .key1 = key1,
        .key2 = key2,
    };
}

pub fn parseVacuumMaintenanceTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !VacuumMaintenanceSyntax {
    var cursor = parser.Cursor.init(tokens, pos);
    var full = false;
    var freeze = false;
    var verbose = false;
    var analyze = false;
    if (cursor.matchToken(.lparen) != null) {
        while (true) {
            try parseVacuumMaintenanceOption(cursor, &full, &freeze, &verbose, &analyze);
            if (cursor.matchToken(.comma) == null) break;
        }
        try cursor.expectToken(.rparen);
    } else {
        while (parseOptionalVacuumMaintenanceOption(cursor, &full, &freeze, &verbose, &analyze)) {}
    }
    const table_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    var table_transferred = false;
    errdefer if (!table_transferred) alloc.free(table_name);
    if (cursor.matchToken(.lparen) != null) return error.UnsupportedSqlShape;
    try adapterNoopStatementEnd(cursor);
    table_transferred = true;
    return .{ .table_name = table_name, .full = full, .freeze = freeze, .verbose = verbose, .analyze = analyze };
}

pub fn parseAnalyzeMaintenanceTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !AnalyzeMaintenanceSyntax {
    var cursor = parser.Cursor.init(tokens, pos);
    const verbose = cursor.matchKeyword("verbose");
    const table_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    var table_transferred = false;
    errdefer if (!table_transferred) alloc.free(table_name);
    var column_count: usize = 0;
    if (cursor.matchToken(.lparen) != null) {
        while (true) {
            _ = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
            column_count += 1;
            if (cursor.matchToken(.comma) == null) break;
        }
        try cursor.expectToken(.rparen);
    }
    try adapterNoopStatementEnd(cursor);
    table_transferred = true;
    return .{ .table_name = table_name, .verbose = verbose, .column_count = column_count };
}

pub fn parseReindexMaintenanceTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !ReindexMaintenanceSyntax {
    var cursor = parser.Cursor.init(tokens, pos);
    const target: ReindexMaintenanceTargetSyntax = if (cursor.matchKeyword("index"))
        .index
    else if (cursor.matchKeyword("table"))
        .table
    else if (cursor.matchKeyword("schema"))
        .schema
    else if (cursor.matchKeyword("database"))
        .database
    else if (cursor.matchKeyword("system"))
        .system
    else
        return error.UnsupportedSqlShape;
    const concurrently = cursor.matchKeyword("concurrently");
    const name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    var name_transferred = false;
    errdefer if (!name_transferred) alloc.free(name);
    try adapterNoopStatementEnd(cursor);
    name_transferred = true;
    return .{ .target = target, .name = name, .concurrently = concurrently };
}

pub fn parseClusterMaintenanceTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !ClusterMaintenanceSyntax {
    var cursor = parser.Cursor.init(tokens, pos);
    const verbose = cursor.matchKeyword("verbose");
    const table_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    var table_transferred = false;
    errdefer if (!table_transferred) alloc.free(table_name);
    var index_name: ?[]const u8 = null;
    var index_transferred = false;
    errdefer if (!index_transferred) if (index_name) |name| alloc.free(@constCast(name));
    if (cursor.matchKeyword("using")) {
        index_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    }
    try adapterNoopStatementEnd(cursor);
    table_transferred = true;
    index_transferred = true;
    return .{ .table_name = table_name, .index_name = index_name, .verbose = verbose };
}

pub fn parseCreateDatabaseCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !CreateDatabaseSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("database");
    const database_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var database_transferred = false;
    errdefer if (!database_transferred) alloc.free(database_name);
    if (cursor.matchKeyword("with") or
        cursor.peekKeyword("owner") or
        cursor.peekKeyword("template") or
        cursor.peekKeyword("encoding") or
        cursor.peekKeyword("locale") or
        cursor.peekKeyword("tablespace") or
        cursor.peekKeyword("connection"))
        return error.UnsupportedSqlShape;
    try adapterNoopStatementEnd(cursor);
    database_transferred = true;
    return .{ .database_name = database_name };
}

pub fn parseAlterDatabaseCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !AlterDatabaseSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("database");
    const database_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var database_transferred = false;
    errdefer if (!database_transferred) alloc.free(database_name);
    if (cursor.matchKeyword("rename") or
        cursor.matchKeyword("owner") or
        cursor.matchKeyword("refresh") or
        cursor.matchKeyword("reset"))
        return error.UnsupportedSqlShape;
    try cursor.expectKeyword("set");
    const setting_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var setting_transferred = false;
    errdefer if (!setting_transferred) alloc.free(setting_name);
    if (cursor.matchKeyword("to") == false and cursor.matchToken(.eq) == null) return error.UnsupportedSqlShape;
    const value_json = try sql_value.parseSqlUntypedValueJsonAlloc(alloc, tokens, pos);
    var value_transferred = false;
    errdefer if (!value_transferred) alloc.free(@constCast(value_json));
    try adapterNoopStatementEnd(cursor);
    database_transferred = true;
    setting_transferred = true;
    value_transferred = true;
    return .{
        .database_name = database_name,
        .setting_name = setting_name,
        .value_json = value_json,
    };
}

pub fn parseDropDatabaseCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !DropDatabaseSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("database");
    var if_exists = false;
    if (cursor.matchKeyword("if")) {
        try cursor.expectKeyword("exists");
        if_exists = true;
    }
    const database_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var database_transferred = false;
    errdefer if (!database_transferred) alloc.free(database_name);
    var force = false;
    if (cursor.matchKeyword("with")) {
        try cursor.expectToken(.lparen);
        try cursor.expectKeyword("force");
        try cursor.expectToken(.rparen);
        force = true;
    }
    try adapterNoopStatementEnd(cursor);
    database_transferred = true;
    return .{ .database_name = database_name, .if_exists = if_exists, .force = force };
}

pub fn parseCreateTablespaceCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !CreateTablespaceSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("tablespace");
    const tablespace_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var tablespace_transferred = false;
    errdefer if (!tablespace_transferred) alloc.free(tablespace_name);
    if (cursor.peekKeyword("owner")) return error.UnsupportedSqlShape;
    try cursor.expectKeyword("location");
    const location_json = try sql_value.parseSqlUntypedValueJsonAlloc(alloc, tokens, pos);
    var location_transferred = false;
    errdefer if (!location_transferred) alloc.free(@constCast(location_json));
    if (cursor.peekKeyword("with")) return error.UnsupportedSqlShape;
    try adapterNoopStatementEnd(cursor);
    const placement_policy_json = try alloc.dupe(u8, "{}");
    errdefer alloc.free(placement_policy_json);
    tablespace_transferred = true;
    location_transferred = true;
    return .{ .tablespace_name = tablespace_name, .location_json = location_json, .placement_policy_json = placement_policy_json };
}

pub fn parseRenameTablespaceCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !RenameTablespaceSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("tablespace");
    const tablespace_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var tablespace_transferred = false;
    errdefer if (!tablespace_transferred) alloc.free(tablespace_name);
    try cursor.expectKeyword("rename");
    try cursor.expectKeyword("to");
    const new_tablespace_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var new_tablespace_transferred = false;
    errdefer if (!new_tablespace_transferred) alloc.free(new_tablespace_name);
    try adapterNoopStatementEnd(cursor);
    tablespace_transferred = true;
    new_tablespace_transferred = true;
    return .{ .tablespace_name = tablespace_name, .new_tablespace_name = new_tablespace_name };
}

pub fn parseDropTablespaceCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !DropTablespaceSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("tablespace");
    var if_exists = false;
    if (cursor.matchKeyword("if")) {
        try cursor.expectKeyword("exists");
        if_exists = true;
    }
    const tablespace_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var tablespace_transferred = false;
    errdefer if (!tablespace_transferred) alloc.free(tablespace_name);
    try adapterNoopStatementEnd(cursor);
    tablespace_transferred = true;
    return .{ .tablespace_name = tablespace_name, .if_exists = if_exists };
}

pub fn parseCreateSchemaNamespaceCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !CreateSchemaNamespaceSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("schema");
    var if_not_exists = false;
    if (cursor.matchKeyword("if")) {
        try cursor.expectKeyword("not");
        try cursor.expectKeyword("exists");
        if_not_exists = true;
    }
    const schema_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    var schema_transferred = false;
    errdefer if (!schema_transferred) alloc.free(schema_name);
    if (cursor.peekKeyword("authorization") or cursor.peekKeyword("create") or cursor.peekKeyword("grant")) return error.UnsupportedSqlShape;
    try adapterNoopStatementEnd(cursor);
    schema_transferred = true;
    return .{ .schema_name = schema_name, .if_not_exists = if_not_exists };
}

pub fn parseRenameSchemaNamespaceCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !RenameSchemaNamespaceSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("schema");
    const schema_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    var schema_transferred = false;
    errdefer if (!schema_transferred) alloc.free(schema_name);
    try cursor.expectKeyword("rename");
    try cursor.expectKeyword("to");
    const new_schema_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    var new_schema_transferred = false;
    errdefer if (!new_schema_transferred) alloc.free(new_schema_name);
    try adapterNoopStatementEnd(cursor);
    schema_transferred = true;
    new_schema_transferred = true;
    return .{ .schema_name = schema_name, .new_schema_name = new_schema_name };
}

pub fn parseDropSchemaNamespaceCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !DropSchemaNamespaceSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("schema");
    var if_exists = false;
    if (cursor.matchKeyword("if")) {
        try cursor.expectKeyword("exists");
        if_exists = true;
    }
    const schema_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    var schema_transferred = false;
    errdefer if (!schema_transferred) alloc.free(schema_name);
    if (cursor.matchToken(.comma) != null) return error.UnsupportedSqlShape;
    const cascade = cursor.matchKeyword("cascade");
    if (!cascade) _ = cursor.matchKeyword("restrict");
    try adapterNoopStatementEnd(cursor);
    schema_transferred = true;
    return .{ .schema_name = schema_name, .if_exists = if_exists, .cascade = cascade };
}

pub fn parseCreateExtensionCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !CreateExtensionSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("extension");
    var if_not_exists = false;
    if (cursor.matchKeyword("if")) {
        try cursor.expectKeyword("not");
        try cursor.expectKeyword("exists");
        if_not_exists = true;
    }
    const extension_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    var extension_transferred = false;
    errdefer if (!extension_transferred) alloc.free(extension_name);

    var schema_name: ?[]const u8 = null;
    var schema_transferred = false;
    errdefer if (!schema_transferred) if (schema_name) |owned| alloc.free(@constCast(owned));
    if (cursor.matchKeyword("with")) {
        try cursor.expectKeyword("schema");
        schema_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    } else if (cursor.matchKeyword("schema")) {
        schema_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    }

    var version: ?[]const u8 = null;
    var version_transferred = false;
    errdefer if (!version_transferred) if (version) |owned| alloc.free(@constCast(owned));
    if (cursor.matchKeyword("version")) {
        version = try parseSqlStringLiteralValueAlloc(alloc, cursor);
    }
    if (cursor.peekKeyword("from") or cursor.peekKeyword("cascade")) return error.UnsupportedSqlShape;
    try adapterNoopStatementEnd(cursor);
    extension_transferred = true;
    schema_transferred = true;
    version_transferred = true;
    return .{
        .extension_name = extension_name,
        .schema_name = schema_name,
        .version = version,
        .if_not_exists = if_not_exists,
    };
}

pub fn parseUpdateExtensionCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !UpdateExtensionSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("extension");
    const extension_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    var extension_transferred = false;
    errdefer if (!extension_transferred) alloc.free(extension_name);
    try cursor.expectKeyword("update");

    var target_version: ?[]const u8 = null;
    var version_transferred = false;
    errdefer if (!version_transferred) if (target_version) |owned| alloc.free(@constCast(owned));
    if (cursor.matchKeyword("to")) {
        target_version = try parseSqlStringLiteralValueAlloc(alloc, cursor);
    }
    try adapterNoopStatementEnd(cursor);
    extension_transferred = true;
    version_transferred = true;
    return .{ .extension_name = extension_name, .target_version = target_version };
}

pub fn parseDropExtensionCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !DropExtensionSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("extension");
    var if_exists = false;
    if (cursor.matchKeyword("if")) {
        try cursor.expectKeyword("exists");
        if_exists = true;
    }
    const extension_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    var extension_transferred = false;
    errdefer if (!extension_transferred) alloc.free(extension_name);
    const cascade = cursor.matchKeyword("cascade");
    if (!cascade) _ = cursor.matchKeyword("restrict");
    try adapterNoopStatementEnd(cursor);
    extension_transferred = true;
    return .{ .extension_name = extension_name, .if_exists = if_exists, .cascade = cascade };
}

pub fn parseCreateRoutineCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !CreateRoutineSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    const kind = try parseRoutineKindKeyword(cursor);
    const routine_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    var routine_transferred = false;
    errdefer if (!routine_transferred) alloc.free(routine_name);
    var signature = try parseCreateRoutineSignatureAlloc(alloc, cursor);
    defer signature.deinit(alloc);
    var returns_type: ?[]const u8 = null;
    errdefer if (returns_type) |value| alloc.free(@constCast(value));
    var language: ?[]const u8 = null;
    errdefer if (language) |value| alloc.free(@constCast(value));
    var volatility: ?ddl_plan.RoutineVolatility = null;
    var security: ?ddl_plan.RoutineSecurity = null;
    var null_input: ?ddl_plan.RoutineNullInput = null;
    var parallel_safety: ?ddl_plan.RoutineParallelSafety = null;
    var leakproof = false;
    var window = false;
    var support_function: ?[]const u8 = null;
    errdefer if (support_function) |value| alloc.free(@constCast(value));
    var transform_types: []const []const u8 = &.{};
    var transform_types_transferred = true;
    errdefer if (!transform_types_transferred) freeStringSlice(alloc, transform_types);
    var settings = std.ArrayListUnmanaged(ddl_plan.RoutineSetting).empty;
    var settings_transferred = false;
    errdefer if (!settings_transferred) freeRoutineSettingList(alloc, &settings);
    var cost: ?[]const u8 = null;
    errdefer if (cost) |value| alloc.free(@constCast(value));
    var rows: ?[]const u8 = null;
    errdefer if (rows) |value| alloc.free(@constCast(value));
    var body: ?ddl_plan.RoutineBodyPlan = null;
    errdefer if (body) |*value| value.deinit(alloc);
    while (!cursor.atEnd() and !cursor.peekKind(.semicolon)) {
        if (cursor.matchKeyword("returns")) {
            if (cursor.peekKeyword("null")) {
                if (null_input != null) return error.UnsupportedSqlShape;
                try cursor.expectKeyword("null");
                try cursor.expectKeyword("on");
                try cursor.expectKeyword("null");
                try cursor.expectKeyword("input");
                null_input = .returns_null;
                continue;
            }
            if (kind != .function or returns_type != null) return error.UnsupportedSqlShape;
            returns_type = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
            continue;
        }
        if (cursor.matchKeyword("language")) {
            if (language != null) return error.UnsupportedSqlShape;
            language = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
            continue;
        }
        if (cursor.matchKeyword("immutable")) {
            if (volatility != null) return error.UnsupportedSqlShape;
            volatility = .immutable;
            continue;
        }
        if (cursor.matchKeyword("stable")) {
            if (volatility != null) return error.UnsupportedSqlShape;
            volatility = .stable;
            continue;
        }
        if (cursor.matchKeyword("volatile")) {
            if (volatility != null) return error.UnsupportedSqlShape;
            volatility = .@"volatile";
            continue;
        }
        if (cursor.matchKeyword("security")) {
            if (security != null) return error.UnsupportedSqlShape;
            if (cursor.matchKeyword("definer")) {
                security = .definer;
            } else if (cursor.matchKeyword("invoker")) {
                security = .invoker;
            } else {
                return error.UnsupportedSqlShape;
            }
            continue;
        }
        if (cursor.matchKeyword("external")) {
            try cursor.expectKeyword("security");
            if (security != null) return error.UnsupportedSqlShape;
            if (cursor.matchKeyword("definer")) {
                security = .definer;
            } else if (cursor.matchKeyword("invoker")) {
                security = .invoker;
            } else {
                return error.UnsupportedSqlShape;
            }
            continue;
        }
        if (cursor.matchKeyword("called")) {
            if (null_input != null) return error.UnsupportedSqlShape;
            try cursor.expectKeyword("on");
            try cursor.expectKeyword("null");
            try cursor.expectKeyword("input");
            null_input = .called;
            continue;
        }
        if (cursor.matchKeyword("strict")) {
            if (null_input != null) return error.UnsupportedSqlShape;
            null_input = .returns_null;
            continue;
        }
        if (cursor.matchKeyword("parallel")) {
            if (parallel_safety != null) return error.UnsupportedSqlShape;
            if (cursor.matchKeyword("safe")) {
                parallel_safety = .safe;
            } else if (cursor.matchKeyword("restricted")) {
                parallel_safety = .restricted;
            } else if (cursor.matchKeyword("unsafe")) {
                parallel_safety = .unsafe;
            } else {
                return error.UnsupportedSqlShape;
            }
            continue;
        }
        if (cursor.matchKeyword("leakproof")) {
            if (leakproof) return error.UnsupportedSqlShape;
            leakproof = true;
            continue;
        }
        if (cursor.matchKeyword("window")) {
            if (window) return error.UnsupportedSqlShape;
            window = true;
            continue;
        }
        if (cursor.matchKeyword("support")) {
            if (support_function != null) return error.UnsupportedSqlShape;
            support_function = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
            continue;
        }
        if (cursor.matchKeyword("transform")) {
            if (transform_types.len != 0) return error.UnsupportedSqlShape;
            try cursor.expectKeyword("for");
            try cursor.expectKeyword("type");
            transform_types = try parseSqlObjectIdentifierListAlloc(alloc, tokens, pos);
            transform_types_transferred = false;
            continue;
        }
        if (cursor.matchKeyword("cost")) {
            if (cost != null) return error.UnsupportedSqlShape;
            const token = cursor.matchToken(.number) orelse return error.UnsupportedSqlShape;
            const parsed = std.fmt.parseFloat(f64, token.text) catch return error.UnsupportedSqlShape;
            if (!(parsed > 0)) return error.UnsupportedSqlShape;
            cost = try alloc.dupe(u8, token.text);
            continue;
        }
        if (cursor.matchKeyword("rows")) {
            if (rows != null) return error.UnsupportedSqlShape;
            const token = cursor.matchToken(.number) orelse return error.UnsupportedSqlShape;
            const parsed = std.fmt.parseFloat(f64, token.text) catch return error.UnsupportedSqlShape;
            if (!(parsed > 0)) return error.UnsupportedSqlShape;
            rows = try alloc.dupe(u8, token.text);
            continue;
        }
        if (cursor.matchKeyword("set")) {
            var setting = try parseRoutineSettingAlloc(alloc, cursor, tokens, pos);
            var setting_transferred = false;
            errdefer if (!setting_transferred) setting.deinit(alloc);
            try settings.append(alloc, setting);
            setting_transferred = true;
            continue;
        }
        if (cursor.matchKeyword("as")) {
            if (body != null) return error.UnsupportedSqlShape;
            const body_token = cursor.matchToken(.string) orelse return error.UnsupportedSqlShape;
            body = try parseRoutineBodyPlanAlloc(alloc, kind, language, returns_type, signature.argument_count, signature.argument_names, body_token.text);
            continue;
        }
        return error.UnsupportedSqlShape;
    }
    if (kind == .function and returns_type == null) return error.UnsupportedSqlShape;
    try adapterNoopStatementEnd(cursor);
    const owned_settings = try settings.toOwnedSlice(alloc);
    settings_transferred = true;
    routine_transferred = true;
    transform_types_transferred = true;
    const out = CreateRoutineSyntax{
        .kind = kind,
        .routine_name = routine_name,
        .argument_count = signature.argument_count,
        .returns_type = returns_type,
        .language = language,
        .volatility = volatility,
        .security = security,
        .null_input = null_input,
        .parallel_safety = parallel_safety,
        .leakproof = leakproof,
        .window = window,
        .support_function = support_function,
        .transform_types = transform_types,
        .settings = owned_settings,
        .cost = cost,
        .rows = rows,
        .body = body,
    };
    returns_type = null;
    language = null;
    support_function = null;
    cost = null;
    rows = null;
    body = null;
    return out;
}

fn parseRoutineBodyPlanAlloc(
    alloc: std.mem.Allocator,
    kind: RoutineKindSyntax,
    language: ?[]const u8,
    returns_type: ?[]const u8,
    argument_count: usize,
    argument_names: []const []const u8,
    body_text: []const u8,
) !ddl_plan.RoutineBodyPlan {
    const lang = language orelse return error.UnsupportedSqlShape;
    if (std.ascii.eqlIgnoreCase(lang, "plpgsql")) {
        return switch (kind) {
            .function => try parsePlpgsqlTriggerRoutineBodyPlanAlloc(alloc, returns_type, argument_count, body_text),
            .procedure => try parsePlpgsqlProcedureRoutineBodyPlanAlloc(alloc, returns_type, argument_count, body_text),
        };
    }
    if (kind != .function) return error.UnsupportedSqlShape;
    if (!std.ascii.eqlIgnoreCase(lang, "sql")) return error.UnsupportedSqlShape;
    const trimmed = std.mem.trim(u8, body_text, " \t\r\n");
    if (!std.ascii.startsWithIgnoreCase(trimmed, "select ")) return error.UnsupportedSqlShape;
    var expression = std.mem.trim(u8, trimmed["select ".len..], " \t\r\n");
    if (std.mem.endsWith(u8, expression, ";")) {
        expression = std.mem.trim(u8, expression[0 .. expression.len - 1], " \t\r\n");
    }
    var expression_tokens = try lexer.tokenizeAlloc(alloc, expression);
    defer lexer.freeTokens(alloc, &expression_tokens);
    const body_expression = try routineBodyExpressionAlloc(alloc, expression_tokens.items, argument_count, argument_names);
    errdefer runtime_schema.freeRelationalRowsExpression(alloc, body_expression);
    return .{
        .kind = .sql_expression,
        .hook = .expression,
        .expression = body_expression,
    };
}

fn parsePlpgsqlTriggerRoutineBodyPlanAlloc(
    alloc: std.mem.Allocator,
    returns_type: ?[]const u8,
    argument_count: usize,
    body_text: []const u8,
) !ddl_plan.RoutineBodyPlan {
    if (argument_count != 0) return error.UnsupportedSqlShape;
    const returns = returns_type orelse return error.UnsupportedSqlShape;
    if (!std.ascii.eqlIgnoreCase(returns, "trigger")) return error.UnsupportedSqlShape;
    var body_tokens = try lexer.tokenizeAlloc(alloc, body_text);
    defer lexer.freeTokens(alloc, &body_tokens);
    var pos: usize = 0;
    const cursor = parser.Cursor.init(body_tokens.items, &pos);
    var perform_calls = std.ArrayListUnmanaged(ddl_plan.RoutinePerformCall).empty;
    errdefer {
        freeRoutinePerformCallItems(alloc, perform_calls.items);
        perform_calls.deinit(alloc);
    }
    try cursor.expectKeyword("begin");
    _ = try consumeBenignPlpgsqlStatements(alloc, cursor, &perform_calls);
    try cursor.expectKeyword("return");
    const hook: ddl_plan.RoutineExecutionHook = if (cursor.matchKeyword("new"))
        .trigger_return_new
    else if (cursor.matchKeyword("old"))
        .trigger_return_old
    else if (cursor.matchKeyword("null"))
        .trigger_return_null
    else
        return error.UnsupportedSqlShape;
    try cursor.expectToken(.semicolon);
    try cursor.expectKeyword("end");
    _ = cursor.matchToken(.semicolon);
    if (!cursor.atEnd()) return error.UnsupportedSqlShape;
    return .{
        .kind = .plpgsql_trigger,
        .hook = hook,
        .perform_calls = try perform_calls.toOwnedSlice(alloc),
    };
}

fn parsePlpgsqlProcedureRoutineBodyPlanAlloc(
    alloc: std.mem.Allocator,
    returns_type: ?[]const u8,
    argument_count: usize,
    body_text: []const u8,
) !ddl_plan.RoutineBodyPlan {
    if (returns_type != null) return error.UnsupportedSqlShape;
    if (argument_count != 0) return error.UnsupportedSqlShape;
    var body_tokens = try lexer.tokenizeAlloc(alloc, body_text);
    defer lexer.freeTokens(alloc, &body_tokens);
    var pos: usize = 0;
    const cursor = parser.Cursor.init(body_tokens.items, &pos);
    var perform_calls = std.ArrayListUnmanaged(ddl_plan.RoutinePerformCall).empty;
    errdefer {
        freeRoutinePerformCallItems(alloc, perform_calls.items);
        perform_calls.deinit(alloc);
    }
    try cursor.expectKeyword("begin");
    _ = try consumeBenignPlpgsqlStatements(alloc, cursor, &perform_calls);
    if (cursor.matchKeyword("null")) {
        try cursor.expectToken(.semicolon);
    } else if (!cursor.peekKeyword("end")) {
        return error.UnsupportedSqlShape;
    }
    try cursor.expectKeyword("end");
    _ = cursor.matchToken(.semicolon);
    if (!cursor.atEnd()) return error.UnsupportedSqlShape;
    return .{
        .kind = .plpgsql_procedure,
        .hook = .procedure_noop,
        .perform_calls = try perform_calls.toOwnedSlice(alloc),
    };
}

fn freeRoutinePerformCallItems(alloc: std.mem.Allocator, calls: []const ddl_plan.RoutinePerformCall) void {
    for (calls) |call| {
        var owned = call;
        owned.deinit(alloc);
    }
}

fn consumeBenignPlpgsqlStatements(
    alloc: std.mem.Allocator,
    cursor: parser.Cursor,
    perform_calls: *std.ArrayListUnmanaged(ddl_plan.RoutinePerformCall),
) !bool {
    var consumed = false;
    while (true) {
        const checkpoint = cursor.checkpoint();
        if (cursor.matchKeyword("raise")) {
            if (!cursor.matchKeyword("notice")) {
                cursor.restore(checkpoint);
                return error.UnsupportedSqlShape;
            }
            _ = cursor.matchToken(.string) orelse return error.UnsupportedSqlShape;
            try cursor.expectToken(.semicolon);
            consumed = true;
            continue;
        }
        if (cursor.matchKeyword("perform")) {
            const routine = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
            const owned_routine = try alloc.dupe(u8, routine.text);
            errdefer alloc.free(owned_routine);
            try cursor.expectToken(.lparen);
            var argument_json = std.ArrayListUnmanaged([]const u8).empty;
            errdefer freeStringSlice(alloc, argument_json.items);
            if (!cursor.peekKind(.rparen)) {
                while (true) {
                    const value_json = try sql_value.parseJsonValueAlloc(alloc, cursor.tokens, cursor.pos, &.{});
                    errdefer alloc.free(value_json);
                    try argument_json.append(alloc, value_json);
                    if (cursor.matchToken(.comma) == null) break;
                }
            }
            try cursor.expectToken(.rparen);
            try cursor.expectToken(.semicolon);
            const owned_arguments = try argument_json.toOwnedSlice(alloc);
            try perform_calls.append(alloc, .{
                .routine_name = owned_routine,
                .argument_json = owned_arguments,
            });
            consumed = true;
            continue;
        }
        return consumed;
    }
}

fn routineBodyExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    argument_count: usize,
    argument_names: []const []const u8,
) !runtime_schema.RelationalRowsExpression {
    var normalized_tokens = try routineExpressionArgumentTokensAlloc(alloc, tokens, argument_count, argument_names);
    defer lexer.freeTokens(alloc, &normalized_tokens);

    var pos: usize = 0;
    var expression = try parseDdlGeneratedRowExpressionAlloc(alloc, parser.Cursor.init(normalized_tokens.items, &pos));
    errdefer runtime_schema.freeRelationalRowsExpression(alloc, expression);
    if (pos != normalized_tokens.items.len) return error.UnsupportedSqlShape;
    try rewriteRoutineBodyExpressionArguments(&expression, argument_count);
    return expression;
}

fn routineExpressionArgumentTokensAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    argument_count: usize,
    argument_names: []const []const u8,
) !std.ArrayListUnmanaged(Token) {
    var normalized = std.ArrayListUnmanaged(Token).empty;
    errdefer lexer.freeTokens(alloc, &normalized);

    for (tokens, 0..) |token, i| {
        if (token.kind != .placeholder) {
            if (token.kind == .identifier and !routineIdentifierIsCallName(tokens, i)) {
                if (routineNamedArgumentIndex(argument_names, token.text)) |argument_index| {
                    try appendRoutineArgumentTokenAlloc(alloc, &normalized, token, argument_index);
                    continue;
                }
            }
            try normalized.append(alloc, borrowedRoutineToken(token.kind, token.text, token));
            continue;
        }
        const argument_index = routineArgumentIndex(token.text) orelse return error.UnsupportedSqlShape;
        if (argument_index > argument_count) return error.UnsupportedSqlShape;
        if (routinePlaceholderCastType(token.text)) |cast_type| {
            if (ddlGeneratedCastType(cast_type) == null) return error.UnsupportedSqlShape;
            try appendRoutineCastArgumentTokensAlloc(alloc, &normalized, token, argument_index, cast_type);
        } else {
            try appendRoutineArgumentTokenAlloc(alloc, &normalized, token, argument_index);
        }
    }

    return normalized;
}

fn routineIdentifierIsCallName(tokens: []const Token, index: usize) bool {
    return index + 1 < tokens.len and tokens[index + 1].kind == .lparen;
}

fn routineNamedArgumentIndex(argument_names: []const []const u8, text: []const u8) ?usize {
    for (argument_names, 0..) |name, i| {
        if (name.len != 0 and std.ascii.eqlIgnoreCase(name, text)) return i + 1;
    }
    return null;
}

fn routinePlaceholderCastType(text: []const u8) ?[]const u8 {
    const trimmed = std.mem.trim(u8, text, " \t\r\n");
    if (!std.mem.startsWith(u8, trimmed, "$")) return null;
    var end: usize = 1;
    while (end < trimmed.len and trimmed[end] >= '0' and trimmed[end] <= '9') : (end += 1) {}
    if (end == 1 or end == trimmed.len) return null;
    if (!std.mem.startsWith(u8, trimmed[end..], "::")) return null;
    const cast_type = trimmed[end + "::".len ..];
    if (cast_type.len == 0) return null;
    return cast_type;
}

fn appendRoutineCastArgumentTokensAlloc(
    alloc: std.mem.Allocator,
    normalized: *std.ArrayListUnmanaged(Token),
    token: Token,
    argument_index: usize,
    cast_type: []const u8,
) !void {
    const start_len = normalized.items.len;
    errdefer freeRoutineTokensAfter(alloc, normalized, start_len);

    try normalized.append(alloc, borrowedRoutineKeywordToken(.identifier, "cast", token, .cast));
    try normalized.append(alloc, borrowedRoutineToken(.lparen, "(", token));
    try appendRoutineArgumentTokenAlloc(alloc, normalized, token, argument_index);
    try normalized.append(alloc, borrowedRoutineTrailingKeywordToken(.identifier, "as", token, .as));
    try normalized.append(alloc, borrowedRoutineTrailingToken(.identifier, cast_type, token));
    try normalized.append(alloc, borrowedRoutineTrailingToken(.rparen, ")", token));
}

fn appendRoutineArgumentTokenAlloc(
    alloc: std.mem.Allocator,
    normalized: *std.ArrayListUnmanaged(Token),
    token: Token,
    argument_index: usize,
) !void {
    const arg_name = try std.fmt.allocPrint(alloc, "arg{d}", .{argument_index});
    errdefer alloc.free(arg_name);
    try normalized.append(alloc, .{
        .kind = .identifier,
        .text = arg_name,
        .owned = true,
        .source_start = token.source_start,
        .source_end = token.source_end,
    });
}

fn borrowedRoutineToken(kind: TokenKind, text: []const u8, source: Token) Token {
    return .{
        .kind = kind,
        .text = text,
        .owned = false,
        .source_start = source.source_start,
        .source_end = source.source_end,
        .keyword = source.keyword,
    };
}

fn borrowedRoutineKeywordToken(kind: TokenKind, text: []const u8, source: Token, keyword: token_mod.TokenKeyword) Token {
    var token = borrowedRoutineToken(kind, text, source);
    token.keyword = keyword;
    return token;
}

fn borrowedRoutineTrailingToken(kind: TokenKind, text: []const u8, source: Token) Token {
    return .{
        .kind = kind,
        .text = text,
        .owned = false,
        .source_start = source.source_end,
        .source_end = source.source_end,
    };
}

fn borrowedRoutineTrailingKeywordToken(kind: TokenKind, text: []const u8, source: Token, keyword: token_mod.TokenKeyword) Token {
    var token = borrowedRoutineTrailingToken(kind, text, source);
    token.keyword = keyword;
    return token;
}

fn freeRoutineTokensAfter(alloc: std.mem.Allocator, tokens: *std.ArrayListUnmanaged(Token), start_len: usize) void {
    while (tokens.items.len > start_len) {
        const removed = tokens.pop().?;
        if (removed.owned) alloc.free(removed.text);
    }
}

fn rewriteRoutineBodyExpressionArguments(
    expression: *runtime_schema.RelationalRowsExpression,
    argument_count: usize,
) error{UnsupportedSqlShape}!void {
    if (expression.kind == .field) {
        const argument_index = routineArgumentIndex(expression.field) orelse return error.UnsupportedSqlShape;
        if (argument_index > argument_count) return error.UnsupportedSqlShape;
        expression.field_source = .source;
    }
    for (@constCast(expression.operands)) |*operand| try rewriteRoutineBodyExpressionArguments(operand, argument_count);
    for (@constCast(expression.case_branches)) |*branch| {
        try rewriteRoutineBodyExpressionConditionArguments(&branch.when, argument_count);
        try rewriteRoutineBodyExpressionArguments(&branch.then, argument_count);
    }
    for (@constCast(expression.case_else)) |*fallback| try rewriteRoutineBodyExpressionArguments(fallback, argument_count);
}

fn rewriteRoutineBodyExpressionConditionArguments(
    condition: *runtime_schema.RelationalRowsExpressionCondition,
    argument_count: usize,
) error{UnsupportedSqlShape}!void {
    try rewriteRoutineBodyExpressionArguments(&condition.lhs, argument_count);
    for (@constCast(condition.rhs)) |*rhs| try rewriteRoutineBodyExpressionArguments(rhs, argument_count);
}

fn routineArgumentIndex(text: []const u8) ?usize {
    const trimmed = std.mem.trim(u8, text, " \t\r\n");
    const digits = if (std.mem.startsWith(u8, trimmed, "$")) blk: {
        var end: usize = 1;
        while (end < trimmed.len and trimmed[end] >= '0' and trimmed[end] <= '9') : (end += 1) {}
        if (end == 1) return null;
        if (end != trimmed.len and !std.mem.startsWith(u8, trimmed[end..], "::")) return null;
        break :blk trimmed[1..end];
    } else if (std.ascii.startsWithIgnoreCase(trimmed, "arg"))
        trimmed["arg".len..]
    else
        return null;
    if (digits.len == 0) return null;
    for (digits) |byte| {
        if (byte < '0' or byte > '9') return null;
    }
    const index = std.fmt.parseInt(usize, digits, 10) catch return null;
    if (index == 0) return null;
    return index;
}

fn parseRoutineSettingAlloc(
    alloc: std.mem.Allocator,
    cursor: parser.Cursor,
    tokens: []const Token,
    pos: *usize,
) !ddl_plan.RoutineSetting {
    const name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var name_transferred = false;
    errdefer if (!name_transferred) alloc.free(@constCast(name));

    if (cursor.matchKeyword("from")) {
        try cursor.expectKeyword("current");
        name_transferred = true;
        return .{ .name = name, .from_current = true };
    }

    if (cursor.matchToken(.eq) == null and !cursor.matchKeyword("to")) return error.UnsupportedSqlShape;
    var values = std.ArrayListUnmanaged([]const u8).empty;
    var values_transferred = false;
    errdefer if (!values_transferred) freeStringList(alloc, &values);
    while (true) {
        const value = cursor.matchToken(.identifier) orelse
            cursor.matchToken(.string) orelse
            cursor.matchToken(.number) orelse
            return error.UnsupportedSqlShape;
        const owned_value = try alloc.dupe(u8, value.text);
        var owned_value_transferred = false;
        errdefer if (!owned_value_transferred) alloc.free(owned_value);
        try values.append(alloc, owned_value);
        owned_value_transferred = true;
        if (cursor.matchToken(.comma) == null) break;
    }

    const owned_values = try values.toOwnedSlice(alloc);
    values_transferred = true;
    name_transferred = true;
    return .{ .name = name, .values = owned_values };
}

pub fn parseDropRoutineCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !DropRoutineSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    const kind = try parseRoutineKindKeyword(cursor);
    var if_exists = false;
    if (cursor.matchKeyword("if")) {
        try cursor.expectKeyword("exists");
        if_exists = true;
    }
    const routine_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    var routine_transferred = false;
    errdefer if (!routine_transferred) alloc.free(routine_name);
    const argument_count = if (cursor.peekKind(.lparen))
        try parseRoutineSignatureArgumentCount(cursor)
    else
        0;
    var cascade = false;
    if (cursor.matchKeyword("cascade")) {
        cascade = true;
    } else if (cursor.matchKeyword("restrict")) {
        cascade = false;
    }
    try adapterNoopStatementEnd(cursor);
    routine_transferred = true;
    return .{
        .kind = kind,
        .routine_name = routine_name,
        .if_exists = if_exists,
        .argument_count = argument_count,
        .cascade = cascade,
    };
}

pub fn parseCreateSequenceCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !CreateSequenceSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("sequence");
    var if_not_exists = false;
    if (cursor.matchKeyword("if")) {
        try cursor.expectKeyword("not");
        try cursor.expectKeyword("exists");
        if_not_exists = true;
    }
    const sequence_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    var sequence_transferred = false;
    errdefer if (!sequence_transferred) alloc.free(sequence_name);
    var options: ddl_plan.SequenceOptions = .{};
    errdefer options.deinit(alloc);
    while (!cursor.atEnd() and !cursor.peekKind(.semicolon)) {
        try parseCreateSequenceOption(alloc, cursor, tokens, pos, &options);
    }
    try adapterNoopStatementEnd(cursor);
    sequence_transferred = true;
    const out = CreateSequenceSyntax{
        .sequence_name = sequence_name,
        .if_not_exists = if_not_exists,
        .options = options,
    };
    options = .{};
    return out;
}

pub fn parseAlterSequenceCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !AlterSequenceSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("sequence");
    var if_exists = false;
    if (cursor.matchKeyword("if")) {
        try cursor.expectKeyword("exists");
        if_exists = true;
    }
    const sequence_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    var sequence_transferred = false;
    errdefer if (!sequence_transferred) alloc.free(sequence_name);
    var operations = std.ArrayListUnmanaged(ddl_plan.SequenceAlterOperation).empty;
    errdefer {
        ddl_plan.freeSequenceAlterOperations(alloc, operations.items);
        operations.deinit(alloc);
    }
    while (!cursor.atEnd() and !cursor.peekKind(.semicolon)) {
        try parseAlterSequenceOperation(alloc, cursor, tokens, pos, &operations);
    }
    if (operations.items.len == 0) return error.UnsupportedSqlShape;
    try adapterNoopStatementEnd(cursor);
    sequence_transferred = true;
    return .{
        .sequence_name = sequence_name,
        .if_exists = if_exists,
        .operations = try operations.toOwnedSlice(alloc),
    };
}

pub fn parseDropSequenceCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !DropSequenceSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("sequence");
    var if_exists = false;
    if (cursor.matchKeyword("if")) {
        try cursor.expectKeyword("exists");
        if_exists = true;
    }
    const sequence_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    var sequence_transferred = false;
    errdefer if (!sequence_transferred) alloc.free(sequence_name);
    if (cursor.matchToken(.comma) != null) return error.UnsupportedSqlShape;
    const cascade = cursor.matchKeyword("cascade");
    if (!cascade) _ = cursor.matchKeyword("restrict");
    try adapterNoopStatementEnd(cursor);
    sequence_transferred = true;
    return .{ .sequence_name = sequence_name, .if_exists = if_exists, .cascade = cascade };
}

pub fn parseIdentitySequenceOptionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    options: *ddl_plan.SequenceOptions,
) !void {
    const cursor = parser.Cursor.init(tokens, pos);
    if (cursor.peekKeyword("as") or cursor.peekKeyword("owned")) return error.UnsupportedSqlShape;
    try parseCreateSequenceOption(alloc, cursor, tokens, pos, options);
}

pub fn parseIdentityAllocatorSpecAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !ddl_plan.IdentityAllocatorSpec {
    const cursor = parser.Cursor.init(tokens, pos);
    if (cursor.matchKeyword("serial")) return .{ .kind = .serial };
    if (cursor.matchKeyword("bigserial")) return .{ .kind = .bigserial };

    const ddl_type = try parseDdlType(tokens, pos);
    if (ddl_type.field_type != .numeric) return error.UnsupportedSqlShape;
    try cursor.expectKeyword("generated");
    const kind: ddl_plan.IdentityAllocatorKind = if (cursor.matchKeyword("always"))
        .generated_always
    else blk: {
        try cursor.expectKeyword("by");
        try cursor.expectKeyword("default");
        break :blk .generated_by_default;
    };
    try cursor.expectKeyword("as");
    try cursor.expectKeyword("identity");

    var options: ddl_plan.SequenceOptions = .{};
    errdefer options.deinit(alloc);
    if (cursor.matchToken(.lparen) != null) {
        while (!cursor.peekKind(.rparen)) {
            try parseIdentitySequenceOptionAlloc(alloc, tokens, pos, &options);
        }
        try cursor.expectToken(.rparen);
    }
    return .{ .kind = kind, .options = options };
}

pub fn parseCreateEnumTypeCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !CreateEnumTypeSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("type");
    const type_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    var type_transferred = false;
    errdefer if (!type_transferred) alloc.free(type_name);
    try cursor.expectKeyword("as");
    try cursor.expectKeyword("enum");
    try cursor.expectToken(.lparen);
    var values = std.ArrayListUnmanaged([]const u8).empty;
    errdefer freeStringList(alloc, &values);
    while (true) {
        const value = try parseEnumLabelOwnedAlloc(alloc, cursor);
        var value_transferred = false;
        errdefer if (!value_transferred) alloc.free(value);
        for (values.items) |existing| {
            if (std.mem.eql(u8, existing, value)) return error.UnsupportedSqlShape;
        }
        try values.append(alloc, value);
        value_transferred = true;
        if (cursor.matchToken(.comma) == null) break;
    }
    if (values.items.len == 0) return error.UnsupportedSqlShape;
    try cursor.expectToken(.rparen);
    try adapterNoopStatementEnd(cursor);
    type_transferred = true;
    return .{
        .type_name = type_name,
        .values = try values.toOwnedSlice(alloc),
    };
}

pub fn parseAlterEnumTypeCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !AddEnumValueSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("type");
    const type_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    var type_transferred = false;
    errdefer if (!type_transferred) alloc.free(type_name);
    try cursor.expectKeyword("add");
    try cursor.expectKeyword("value");
    var if_not_exists = false;
    if (cursor.matchKeyword("if")) {
        try cursor.expectKeyword("not");
        try cursor.expectKeyword("exists");
        if_not_exists = true;
    }
    const value = try parseEnumLabelOwnedAlloc(alloc, cursor);
    var value_transferred = false;
    errdefer if (!value_transferred) alloc.free(value);
    var position: ddl_plan.EnumValuePosition = .none;
    var neighbor_value: ?[]const u8 = null;
    errdefer if (neighbor_value) |neighbor| alloc.free(@constCast(neighbor));
    if (cursor.matchKeyword("before")) {
        position = .before;
        neighbor_value = try parseEnumLabelOwnedAlloc(alloc, cursor);
    } else if (cursor.matchKeyword("after")) {
        position = .after;
        neighbor_value = try parseEnumLabelOwnedAlloc(alloc, cursor);
    }
    try adapterNoopStatementEnd(cursor);
    type_transferred = true;
    value_transferred = true;
    return .{
        .type_name = type_name,
        .value = value,
        .if_not_exists = if_not_exists,
        .position = position,
        .neighbor_value = neighbor_value,
    };
}

pub fn parseDropEnumTypeCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !DropEnumTypeSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("type");
    var if_exists = false;
    if (cursor.matchKeyword("if")) {
        try cursor.expectKeyword("exists");
        if_exists = true;
    }
    const type_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    var type_transferred = false;
    errdefer if (!type_transferred) alloc.free(type_name);
    if (cursor.matchToken(.comma) != null) return error.UnsupportedSqlShape;
    const cascade = cursor.matchKeyword("cascade");
    if (!cascade) _ = cursor.matchKeyword("restrict");
    try adapterNoopStatementEnd(cursor);
    type_transferred = true;
    return .{ .type_name = type_name, .if_exists = if_exists, .cascade = cascade };
}

pub fn parseCreateDomainHeaderAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !CreateDomainHeaderSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("domain");
    if (cursor.matchKeyword("if")) return error.UnsupportedSqlShape;
    const domain_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    var domain_transferred = false;
    errdefer if (!domain_transferred) alloc.free(domain_name);
    try cursor.expectKeyword("as");

    domain_transferred = true;
    return .{ .domain_name = domain_name };
}

pub fn parseAlterDomainHeaderAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !AlterDomainHeaderSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("domain");
    if (cursor.matchKeyword("if")) return error.UnsupportedSqlShape;
    const domain_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    var domain_transferred = false;
    errdefer if (!domain_transferred) alloc.free(domain_name);

    domain_transferred = true;
    return .{ .domain_name = domain_name };
}

pub fn parseAlterDomainCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !ddl_plan.AlterDomainPlan {
    const cursor = parser.Cursor.init(tokens, pos);
    var header = try parseAlterDomainHeaderAlloc(alloc, tokens, pos);
    var header_transferred = false;
    errdefer if (!header_transferred) header.deinit(alloc);

    var operations = std.ArrayListUnmanaged(ddl_plan.DomainAlterOperation).empty;
    errdefer {
        ddl_plan.clearDomainAlterOperations(alloc, operations.items);
        operations.deinit(alloc);
    }

    while (true) {
        if (cursor.matchKeyword("set")) {
            if (cursor.matchKeyword("not")) {
                try cursor.expectKeyword("null");
                try operations.append(alloc, .set_not_null);
            } else if (cursor.matchKeyword("default")) {
                const default_value = try parseDdlDefaultValueUntypedAlloc(alloc, tokens, pos);
                errdefer alloc.free(default_value.value_json);
                try operations.append(alloc, .{ .set_default = default_value });
            } else {
                return error.UnsupportedSqlShape;
            }
        } else if (cursor.matchKeyword("drop")) {
            if (cursor.matchKeyword("not")) {
                try cursor.expectKeyword("null");
                try operations.append(alloc, .drop_not_null);
            } else if (cursor.matchKeyword("default")) {
                try operations.append(alloc, .drop_default);
            } else {
                return error.UnsupportedSqlShape;
            }
        } else {
            return error.UnsupportedSqlShape;
        }
        if (cursor.matchToken(.comma) == null) break;
    }
    if (operations.items.len == 0) return error.UnsupportedSqlShape;
    try adapterNoopStatementEnd(cursor);

    const owned_operations = try operations.toOwnedSlice(alloc);
    var operations_transferred = false;
    errdefer if (!operations_transferred) ddl_plan.freeDomainAlterOperations(alloc, owned_operations);

    header_transferred = true;
    operations_transferred = true;
    return .{
        .domain_name = header.domain_name,
        .operations = owned_operations,
    };
}

pub fn parseDropDomainCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !DropDomainSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("domain");
    var if_exists = false;
    if (cursor.matchKeyword("if")) {
        try cursor.expectKeyword("exists");
        if_exists = true;
    }
    const domain_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    var domain_transferred = false;
    errdefer if (!domain_transferred) alloc.free(domain_name);
    if (cursor.matchToken(.comma) != null) return error.UnsupportedSqlShape;
    const cascade = cursor.matchKeyword("cascade");
    if (!cascade) _ = cursor.matchKeyword("restrict");
    try adapterNoopStatementEnd(cursor);
    domain_transferred = true;
    return .{ .domain_name = domain_name, .if_exists = if_exists, .cascade = cascade };
}

pub fn parseCommentMetadataCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !CommentMetadataSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("on");
    const target: ddl_plan.CommentMetadataTarget = if (cursor.matchKeyword("table"))
        .table
    else if (cursor.matchKeyword("column"))
        .column
    else if (cursor.matchKeyword("index"))
        .index
    else if (cursor.matchKeyword("constraint"))
        .constraint
    else
        return error.UnsupportedSqlShape;

    const object_name = switch (target) {
        .table, .index => try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos),
        .column, .constraint => try parseIdentifierOwnedAlloc(alloc, tokens, pos),
    };
    var object_transferred = false;
    errdefer if (!object_transferred) alloc.free(object_name);

    var parent_table_name: ?[]const u8 = null;
    var parent_transferred = false;
    errdefer if (!parent_transferred) if (parent_table_name) |parent| alloc.free(@constCast(parent));
    if (target == .constraint) {
        try cursor.expectKeyword("on");
        parent_table_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    }

    try cursor.expectKeyword("is");
    var comment_json: ?[]const u8 = null;
    var comment_transferred = false;
    errdefer if (!comment_transferred) if (comment_json) |comment| alloc.free(@constCast(comment));
    if (!cursor.matchKeyword("null")) {
        comment_json = try sql_value.parseSqlUntypedValueJsonAlloc(alloc, tokens, pos);
    }
    try adapterNoopStatementEnd(cursor);

    object_transferred = true;
    parent_transferred = true;
    comment_transferred = true;
    return .{
        .target = target,
        .object_name = object_name,
        .parent_table_name = parent_table_name,
        .comment_json = comment_json,
    };
}

pub fn parseDropTableCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !DropTableSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("table");
    var if_exists = false;
    if (cursor.matchKeyword("if")) {
        try cursor.expectKeyword("exists");
        if_exists = true;
    }
    const table_name = try parseSqlTableReferenceIdentifierOwnedAlloc(alloc, tokens, pos);
    var table_transferred = false;
    errdefer if (!table_transferred) alloc.free(table_name);
    if (cursor.matchToken(.comma) != null) return error.UnsupportedSqlShape;
    const cascade = cursor.matchKeyword("cascade");
    if (!cascade) _ = cursor.matchKeyword("restrict");
    try adapterNoopStatementEnd(cursor);
    table_transferred = true;
    return .{ .table_name = table_name, .if_exists = if_exists, .cascade = cascade };
}

pub fn parseDropIndexCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !DropIndexSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("index");
    _ = cursor.matchKeyword("concurrently");
    var if_exists = false;
    if (cursor.matchKeyword("if")) {
        try cursor.expectKeyword("exists");
        if_exists = true;
    }
    const index_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    var index_transferred = false;
    errdefer if (!index_transferred) alloc.free(index_name);
    _ = cursor.matchKeyword("cascade") or cursor.matchKeyword("restrict");
    try adapterNoopStatementEnd(cursor);
    index_transferred = true;
    return .{ .index_name = index_name, .if_exists = if_exists };
}

pub fn parseAlterTableHeaderAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !AlterTableHeaderSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("table");
    var if_exists = false;
    if (cursor.matchKeyword("if")) {
        try cursor.expectKeyword("exists");
        if_exists = true;
    }
    const table_name = try parseSqlTableReferenceIdentifierOwnedAlloc(alloc, tokens, pos);
    var table_transferred = false;
    errdefer if (!table_transferred) alloc.free(table_name);

    table_transferred = true;
    return .{ .table_name = table_name, .if_exists = if_exists };
}

pub fn parseAlterTableValidateConstraintOperationAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !AlterTableValidateConstraintSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("validate");
    try cursor.expectKeyword("constraint");
    const constraint_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var constraint_transferred = false;
    errdefer if (!constraint_transferred) alloc.free(constraint_name);

    constraint_transferred = true;
    return .{ .constraint_name = constraint_name };
}

pub fn parseAlterTableRenameOperationAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !AlterTableRenameOperationSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("rename");
    const target: AlterTableRenameTargetSyntax = if (cursor.matchKeyword("constraint"))
        .constraint
    else blk: {
        _ = cursor.matchKeyword("column");
        break :blk .column;
    };

    const old_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var old_transferred = false;
    errdefer if (!old_transferred) alloc.free(old_name);
    try cursor.expectKeyword("to");
    const new_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var new_transferred = false;
    errdefer if (!new_transferred) alloc.free(new_name);

    old_transferred = true;
    new_transferred = true;
    return .{ .target = target, .old_name = old_name, .new_name = new_name };
}

pub fn parseAlterTableDropOperationAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !AlterTableDropOperationSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("drop");
    const target: AlterTableDropTargetSyntax = if (cursor.matchKeyword("constraint"))
        .constraint
    else blk: {
        _ = cursor.matchKeyword("column");
        break :blk .column;
    };
    var if_exists = false;
    if (cursor.matchKeyword("if")) {
        try cursor.expectKeyword("exists");
        if_exists = true;
    }
    const name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var name_transferred = false;
    errdefer if (!name_transferred) alloc.free(name);

    var dependency_mode: ddl_plan.DropDependencyMode = if (target == .constraint) .restrict else .cascade;
    if (cursor.matchKeyword("cascade")) {
        dependency_mode = .cascade;
    } else if (cursor.matchKeyword("restrict")) {
        dependency_mode = .restrict;
    }

    name_transferred = true;
    return .{
        .target = target,
        .name = name,
        .if_exists = if_exists,
        .dependency_mode = dependency_mode,
    };
}

pub fn parseAlterTableColumnNullabilityOperationAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !AlterTableColumnNullabilitySyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("alter");
    _ = cursor.matchKeyword("column");
    const column_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var column_transferred = false;
    errdefer if (!column_transferred) alloc.free(column_name);

    const nullable: bool = if (cursor.matchKeyword("set")) blk: {
        try cursor.expectKeyword("not");
        try cursor.expectKeyword("null");
        break :blk false;
    } else if (cursor.matchKeyword("drop")) blk: {
        try cursor.expectKeyword("not");
        try cursor.expectKeyword("null");
        break :blk true;
    } else return error.UnsupportedSqlShape;

    column_transferred = true;
    return .{ .column_name = column_name, .nullable = nullable };
}

pub fn parseAlterTableColumnDefaultOperationAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !AlterTableColumnDefaultSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("alter");
    _ = cursor.matchKeyword("column");
    const column_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var column_transferred = false;
    errdefer if (!column_transferred) alloc.free(column_name);

    const action: AlterTableColumnDefaultActionSyntax = if (cursor.matchKeyword("set")) blk: {
        try cursor.expectKeyword("default");
        break :blk .set;
    } else if (cursor.matchKeyword("drop")) blk: {
        try cursor.expectKeyword("default");
        break :blk .drop;
    } else return error.UnsupportedSqlShape;

    column_transferred = true;
    return .{ .column_name = column_name, .action = action };
}

pub fn parseAlterTableColumnTypeHeaderAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !AlterTableColumnTypeHeaderSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("alter");
    _ = cursor.matchKeyword("column");
    const column_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var column_transferred = false;
    errdefer if (!column_transferred) alloc.free(column_name);

    if (cursor.matchKeyword("set")) {
        try cursor.expectKeyword("data");
        try cursor.expectKeyword("type");
    } else {
        try cursor.expectKeyword("type");
    }

    column_transferred = true;
    return .{ .column_name = column_name };
}

pub fn parseOptionalAlterTableColumnUsing(
    tokens: []const Token,
    pos: *usize,
) !?AlterTableColumnUsingSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    if (!cursor.matchKeyword("using")) return null;
    const wrapped = cursor.matchToken(.lparen) != null;
    const using_column = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
    if (wrapped) try cursor.expectToken(.rparen);
    return .{ .column_name = using_column.text, .wrapped = wrapped };
}

pub fn parseAlterTableAddColumnHeader(
    tokens: []const Token,
    pos: *usize,
) !AlterTableAddColumnHeaderSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("add");
    try cursor.expectKeyword("column");
    const if_not_exists = try parseOptionalIfNotExists(cursor);
    return .{ .if_not_exists = if_not_exists };
}

pub fn parseAlterTableAddOperationPrefixAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !AlterTableAddOperationPrefixSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("add");
    if (cursor.matchKeyword("period")) return .{ .kind = .period };

    const constraint_name: ?[]const u8 = if (cursor.matchKeyword("constraint"))
        try parseIdentifierOwnedAlloc(alloc, tokens, pos)
    else
        null;
    var constraint_name_transferred = false;
    errdefer if (!constraint_name_transferred) if (constraint_name) |name| alloc.free(name);

    const kind: AlterTableAddOperationKindSyntax = if (cursor.peekKeyword("primary")) blk: {
        break :blk .primary_key;
    } else if (cursor.peekKeyword("unique")) blk: {
        break :blk .unique;
    } else if (cursor.peekKeyword("foreign")) blk: {
        break :blk .foreign_key;
    } else if (cursor.peekKeyword("check")) blk: {
        break :blk .check;
    } else {
        return error.UnsupportedSqlShape;
    };

    constraint_name_transferred = true;
    return .{
        .kind = kind,
        .constraint_name = constraint_name,
    };
}

pub fn parseIdentityAllocatorTableHeaderAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !IdentityAllocatorTableHeaderSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("table");
    if (cursor.matchKeyword("if")) return error.UnsupportedSqlShape;

    const table_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    var table_transferred = false;
    errdefer if (!table_transferred) alloc.free(table_name);
    try cursor.expectToken(.lparen);

    const column_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var column_transferred = false;
    errdefer if (!column_transferred) alloc.free(column_name);

    table_transferred = true;
    column_transferred = true;
    return .{ .table_name = table_name, .column_name = column_name };
}

pub fn parseCreateIndexHeaderAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !CreateIndexHeaderSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("index");
    _ = cursor.matchKeyword("concurrently");
    const if_not_exists = try parseOptionalIfNotExists(cursor);

    const index_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    var index_transferred = false;
    errdefer if (!index_transferred) alloc.free(index_name);
    try cursor.expectKeyword("on");
    const table_name = try parseSqlTableReferenceIdentifierOwnedAlloc(alloc, tokens, pos);
    var table_transferred = false;
    errdefer if (!table_transferred) alloc.free(table_name);

    var method: ddl_plan.DdlIndexMethod = .btree;
    if (cursor.matchKeyword("using")) {
        const method_token = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
        if (std.ascii.eqlIgnoreCase(method_token.text, "btree")) {
            method = .btree;
        } else if (std.ascii.eqlIgnoreCase(method_token.text, "gin")) {
            method = .gin;
        } else if (std.ascii.eqlIgnoreCase(method_token.text, "antfly_full_text")) {
            method = .antfly_full_text;
        } else if (std.ascii.eqlIgnoreCase(method_token.text, "hnsw")) {
            method = .hnsw;
        } else if (std.ascii.eqlIgnoreCase(method_token.text, "antfly_aknn")) {
            method = .antfly_aknn;
        } else if (std.ascii.eqlIgnoreCase(method_token.text, "antfly_graph")) {
            method = .antfly_graph;
        } else if (std.ascii.eqlIgnoreCase(method_token.text, "antfly_graph_metric")) {
            method = .antfly_graph_metric;
        } else if (std.ascii.eqlIgnoreCase(method_token.text, "antfly_hybrid")) {
            method = .antfly_hybrid;
        } else if (std.ascii.eqlIgnoreCase(method_token.text, "antfly_algebraic")) {
            method = .antfly_algebraic;
        } else return error.UnsupportedSqlShape;
    }

    index_transferred = true;
    table_transferred = true;
    return .{
        .index_name = index_name,
        .table_name = table_name,
        .if_not_exists = if_not_exists,
        .method = method,
    };
}

pub fn parseCreateIndexElementSuffix(
    tokens: []const Token,
    pos: *usize,
    method: ddl_plan.DdlIndexMethod,
    allow_opclass: bool,
) !CreateIndexElementSuffixSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    var suffix: CreateIndexElementSuffixSyntax = .{};
    if (allow_opclass and method == .gin and cursor.peekKind(.identifier)) {
        const opclass_token = cursor.matchToken(.identifier) orelse unreachable;
        if (std.ascii.eqlIgnoreCase(opclass_token.text, "jsonb_path_ops")) {
            suffix.opclass = .jsonb_path_ops;
        } else if (std.ascii.eqlIgnoreCase(opclass_token.text, "array_ops")) {
            suffix.opclass = .array_ops;
        } else return error.UnsupportedSqlShape;
    }
    if (allow_opclass and method == .hnsw and cursor.peekKind(.identifier)) {
        const opclass_token = cursor.matchToken(.identifier) orelse unreachable;
        if (std.ascii.eqlIgnoreCase(opclass_token.text, "vector_l2_ops")) {
            suffix.opclass = .vector_l2_ops;
        } else if (std.ascii.eqlIgnoreCase(opclass_token.text, "vector_ip_ops")) {
            suffix.opclass = .vector_ip_ops;
        } else if (std.ascii.eqlIgnoreCase(opclass_token.text, "vector_cosine_ops")) {
            suffix.opclass = .vector_cosine_ops;
        } else return error.UnsupportedSqlShape;
    }

    if (cursor.peekKind(.lparen)) return error.UnsupportedSqlShape;
    if (cursor.matchKeyword("asc")) {
        suffix.order_direction = .asc;
    } else if (cursor.matchKeyword("desc")) {
        suffix.order_direction = .desc;
    }

    if (cursor.matchKeyword("nulls")) {
        if (cursor.matchKeyword("first")) {
            suffix.nulls_order = .first;
        } else if (cursor.matchKeyword("last")) {
            suffix.nulls_order = .last;
        } else return error.UnsupportedSqlShape;
    }

    return suffix;
}

pub fn consumeOptionalDdlNotValid(tokens: []const Token, pos: *usize) bool {
    if (!peekDdlNotValid(tokens, pos.*)) return false;
    pos.* += 2;
    return true;
}

pub fn parseOptionalDdlUniqueNullsDistinct(tokens: []const Token, pos: *usize) !?bool {
    const cursor = parser.Cursor.init(tokens, pos);
    if (!cursor.matchKeyword("nulls")) return null;
    const not_distinct = cursor.matchKeyword("not");
    try cursor.expectKeyword("distinct");
    return not_distinct;
}

pub fn consumeOptionalDdlImmediateConstraintTiming(tokens: []const Token, pos: *usize) !void {
    const cursor = parser.Cursor.init(tokens, pos);
    if (cursor.matchKeyword("deferrable")) return error.UnsupportedSqlShape;
    const checkpoint = cursor.checkpoint();
    if (!cursor.matchKeyword("not")) return;
    if (!cursor.matchKeyword("deferrable")) {
        cursor.restore(checkpoint);
        return;
    }
    if (cursor.matchKeyword("initially")) {
        if (cursor.matchKeyword("deferred")) return error.UnsupportedSqlShape;
        try cursor.expectKeyword("immediate");
    }
}

pub const DdlConstraintTimingSyntax = struct {
    deferrable: bool = false,
    timing: runtime_schema.ForeignKeyTiming = .immediate,
};

pub fn parseOptionalDdlConstraintTiming(tokens: []const Token, pos: *usize) !DdlConstraintTimingSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    var timing: DdlConstraintTimingSyntax = .{};
    var saw_deferrability = false;
    var saw_initially = false;
    while (true) {
        if (cursor.matchKeyword("deferrable")) {
            if (saw_deferrability) return error.UnsupportedSqlShape;
            saw_deferrability = true;
            timing.deferrable = true;
            continue;
        }
        if (cursor.peekKeyword("not")) {
            const checkpoint = cursor.checkpoint();
            _ = cursor.matchKeyword("not");
            if (!cursor.matchKeyword("deferrable")) {
                cursor.restore(checkpoint);
                break;
            }
            if (saw_deferrability) return error.UnsupportedSqlShape;
            if (timing.timing == .deferred) return error.UnsupportedSqlShape;
            saw_deferrability = true;
            timing.deferrable = false;
            continue;
        }
        if (!cursor.matchKeyword("initially")) break;
        {
            if (saw_initially) return error.UnsupportedSqlShape;
            saw_initially = true;
            if (cursor.matchKeyword("deferred")) {
                if (saw_deferrability and !timing.deferrable) return error.UnsupportedSqlShape;
                timing.deferrable = true;
                timing.timing = .deferred;
            } else {
                try cursor.expectKeyword("immediate");
                timing.timing = .immediate;
            }
            continue;
        }
    }
    return timing;
}

pub fn peekDdlNotValid(tokens: []const Token, pos: usize) bool {
    if (pos + 1 >= tokens.len) return false;
    const not_token = tokens[pos];
    const valid_token = tokens[pos + 1];
    return not_token.kind == .identifier and valid_token.kind == .identifier and
        std.ascii.eqlIgnoreCase(not_token.text, "not") and
        std.ascii.eqlIgnoreCase(valid_token.text, "valid");
}

pub fn parseDdlColumnListAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) ![]const []const u8 {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectToken(.lparen);
    var columns = std.ArrayListUnmanaged([]const u8).empty;
    errdefer freeStringList(alloc, &columns);
    while (true) {
        const column = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
        var transferred = false;
        errdefer if (!transferred) alloc.free(column);
        try columns.append(alloc, column);
        transferred = true;
        if (cursor.matchToken(.comma) == null) break;
    }
    try cursor.expectToken(.rparen);
    if (columns.items.len == 0) return error.UnsupportedSqlShape;
    return try columns.toOwnedSlice(alloc);
}

pub fn parseDdlUniqueColumnListAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) ![]const []const u8 {
    const columns = try parseDdlColumnListAlloc(alloc, tokens, pos);
    errdefer freeStringSlice(alloc, columns);
    try validateSqlIdentifierListUnique(columns);
    return columns;
}

pub fn parseOptionalDdlConstraintIncludeAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) ![]const []const u8 {
    const cursor = parser.Cursor.init(tokens, pos);
    if (!cursor.matchKeyword("include")) return &.{};
    return try parseDdlColumnListAlloc(alloc, tokens, pos);
}

pub fn parseOptionalDdlConstraintIncludeUniqueAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    key_columns: []const []const u8,
) ![]const []const u8 {
    const include_columns = try parseOptionalDdlConstraintIncludeAlloc(alloc, tokens, pos);
    errdefer freeStringSlice(alloc, include_columns);
    try validateSqlIdentifierListUnique(include_columns);
    try validateSqlIdentifierListsDisjoint(key_columns, include_columns);
    return include_columns;
}

pub fn parseDdlTemporalColumnListAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !DdlTemporalColumnListSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectToken(.lparen);
    var columns = std.ArrayListUnmanaged([]const u8).empty;
    errdefer freeStringList(alloc, &columns);
    var without_overlaps_period: ?[]const u8 = null;
    errdefer if (without_overlaps_period) |period| alloc.free(period);
    while (true) {
        const column = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
        var column_transferred = false;
        errdefer if (!column_transferred) alloc.free(column);
        if (cursor.matchKeyword("without")) {
            try cursor.expectKeyword("overlaps");
            if (without_overlaps_period != null) return error.UnsupportedSqlShape;
            without_overlaps_period = column;
            column_transferred = true;
        } else {
            try columns.append(alloc, column);
            column_transferred = true;
        }
        if (cursor.matchToken(.comma) == null) break;
    }
    try cursor.expectToken(.rparen);
    if (columns.items.len == 0) return error.UnsupportedSqlShape;
    const owned_columns = try columns.toOwnedSlice(alloc);
    errdefer freeStringSlice(alloc, owned_columns);
    const period = without_overlaps_period;
    without_overlaps_period = null;
    return .{ .columns = owned_columns, .without_overlaps_period = period };
}

pub fn parseDdlUniqueTemporalColumnListAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !DdlTemporalColumnListSyntax {
    const columns = try parseDdlTemporalColumnListAlloc(alloc, tokens, pos);
    errdefer columns.deinit(alloc);
    try validateSqlIdentifierListUnique(columns.columns);
    return columns;
}

pub fn parseDdlForeignKeyColumnListAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !DdlForeignKeyColumnListSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectToken(.lparen);
    var columns = std.ArrayListUnmanaged([]const u8).empty;
    errdefer freeStringList(alloc, &columns);
    var period: ?[]const u8 = null;
    errdefer if (period) |value| alloc.free(value);
    while (true) {
        if (cursor.matchKeyword("period")) {
            if (period != null) return error.UnsupportedSqlShape;
            period = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
        } else {
            const column = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
            var transferred = false;
            errdefer if (!transferred) alloc.free(column);
            try columns.append(alloc, column);
            transferred = true;
        }
        if (cursor.matchToken(.comma) == null) break;
    }
    try cursor.expectToken(.rparen);
    if (columns.items.len == 0) return error.UnsupportedSqlShape;
    const owned_columns = try columns.toOwnedSlice(alloc);
    errdefer freeStringSlice(alloc, owned_columns);
    const owned_period = period;
    period = null;
    return .{ .columns = owned_columns, .period = owned_period };
}

pub fn parseDdlPeriodConstraintAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !DdlPeriodSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("for");
    const name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var name_transferred = false;
    errdefer if (!name_transferred) alloc.free(name);
    try cursor.expectToken(.lparen);
    const start_column = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var start_transferred = false;
    errdefer if (!start_transferred) alloc.free(start_column);
    try cursor.expectToken(.comma);
    const end_column = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var end_transferred = false;
    errdefer if (!end_transferred) alloc.free(end_column);
    try cursor.expectToken(.rparen);

    name_transferred = true;
    start_transferred = true;
    end_transferred = true;
    return .{ .name = name, .start_column = start_column, .end_column = end_column };
}

pub fn consumeDdlStoredGeneratedValuePrefix(tokens: []const Token, pos: *usize) !void {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("always");
    try cursor.expectKeyword("as");
    try cursor.expectToken(.lparen);
}

pub fn consumeDdlStoredGeneratedValueSuffix(tokens: []const Token, pos: *usize) !void {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectToken(.rparen);
    try cursor.expectKeyword("stored");
}

pub fn parseDdlStoredGeneratedValueAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !runtime_schema.RelationalGeneratedValue {
    try consumeDdlStoredGeneratedValuePrefix(tokens, pos);
    const generated = try parseDdlGeneratedExpressionAlloc(alloc, tokens, pos);
    var generated_transferred = false;
    errdefer if (!generated_transferred) freeDdlGeneratedValue(alloc, generated);
    try consumeDdlStoredGeneratedValueSuffix(tokens, pos);
    generated_transferred = true;
    return generated;
}

pub fn peekDdlIndexElementExpression(
    tokens: []const Token,
    pos: usize,
    include_generated_expression: bool,
) bool {
    var scan = pos;
    while (scan < tokens.len and tokens[scan].kind == .lparen) : (scan += 1) {}
    if (scan >= tokens.len or tokens[scan].kind != .identifier) return false;
    if (scan + 1 >= tokens.len or tokens[scan + 1].kind != .lparen) return false;
    const token = tokens[scan];
    if (token.matchesKeywordTag(.lower) or
        token.matchesKeywordTag(.upper) or
        lower_expr.sqlTokenIsMd5Function(token))
    {
        return true;
    }
    return include_generated_expression;
}

pub fn consumeDdlIndexExpressionWrappers(tokens: []const Token, pos: *usize) usize {
    var count: usize = 0;
    while (pos.* < tokens.len and tokens[pos.*].kind == .lparen) {
        pos.* += 1;
        count += 1;
    }
    return count;
}

pub fn closeDdlIndexExpressionWrappers(tokens: []const Token, pos: *usize, count: usize) !void {
    const cursor = parser.Cursor.init(tokens, pos);
    var remaining = count;
    while (remaining > 0) : (remaining -= 1) {
        try cursor.expectToken(.rparen);
    }
}

pub fn parseDdlUniqueExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !runtime_schema.UniqueExpression {
    const cursor = parser.Cursor.init(tokens, pos);
    const start = cursor.checkpoint();
    const op: ?runtime_schema.UniqueExpressionOp = if (cursor.matchKeyword("lower"))
        .lower
    else if (cursor.matchKeyword("upper"))
        .upper
    else blk: {
        if (cursor.matchIdentifierTokenIf(lower_expr.sqlTokenIsMd5Function) == null) break :blk null;
        break :blk .md5;
    };
    if (op) |simple_op| {
        if (cursor.matchToken(.lparen) != null) {
            const field = parseIdentifierOwnedAlloc(alloc, tokens, pos) catch |err| {
                cursor.restore(start);
                if (err == error.OutOfMemory) return err;
                const expression = try parseDdlGeneratedRowExpressionAlloc(alloc, cursor);
                errdefer runtime_schema.freeRelationalRowsExpression(alloc, expression);
                return .{ .op = .expression, .expression = expression };
            };
            var field_transferred = false;
            errdefer if (!field_transferred) alloc.free(field);
            if (cursor.matchToken(.rparen) != null) {
                field_transferred = true;
                return .{ .op = simple_op, .field = field };
            }
            alloc.free(field);
            field_transferred = true;
        }
        cursor.restore(start);
    }

    const expression = try parseDdlGeneratedRowExpressionAlloc(alloc, cursor);
    errdefer runtime_schema.freeRelationalRowsExpression(alloc, expression);
    return .{ .op = .expression, .expression = expression };
}

pub fn parseDdlGeneratedExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !runtime_schema.RelationalGeneratedValue {
    const cursor = parser.Cursor.init(tokens, pos);
    const start = cursor.checkpoint();
    if (cursor.peekKeywordTag(.lower) or cursor.peekKeywordTag(.upper) or cursor.peekFunctionCallTokenIf(lower_expr.sqlTokenIsMd5Function)) {
        const op: runtime_schema.RelationalGeneratedOp = if (cursor.matchKeywordTag(.lower))
            .lower
        else if (cursor.matchKeywordTag(.upper))
            .upper
        else blk: {
            if (cursor.matchIdentifierTokenIf(lower_expr.sqlTokenIsMd5Function) == null) return error.UnsupportedSqlShape;
            break :blk .md5;
        };
        try cursor.expectToken(.lparen);
        const field = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
        var field_transferred = false;
        errdefer if (!field_transferred) alloc.free(field);
        try cursor.expectToken(.rparen);
        const separator = try alloc.dupe(u8, "");
        var separator_transferred = false;
        errdefer if (!separator_transferred) alloc.free(separator);
        field_transferred = true;
        separator_transferred = true;
        return .{ .op = op, .field = field, .separator = separator };
    }
    if (cursor.matchKeyword("concat")) return parseDdlGeneratedConcatExpressionAlloc(alloc, cursor, tokens, pos, .concat) catch |err| {
        cursor.restore(start);
        if (err != error.UnsupportedSqlShape) return err;
        return try parseDdlGeneratedRowExpressionGeneratedValueAlloc(alloc, cursor);
    };
    if (cursor.matchKeyword("concat_ws")) return parseDdlGeneratedConcatExpressionAlloc(alloc, cursor, tokens, pos, .concat_ws) catch |err| {
        cursor.restore(start);
        if (err != error.UnsupportedSqlShape) return err;
        return try parseDdlGeneratedRowExpressionGeneratedValueAlloc(alloc, cursor);
    };
    cursor.restore(start);
    return try parseDdlGeneratedRowExpressionGeneratedValueAlloc(alloc, cursor);
}

fn parseDdlGeneratedRowExpressionGeneratedValueAlloc(
    alloc: std.mem.Allocator,
    cursor: parser.Cursor,
) !runtime_schema.RelationalGeneratedValue {
    const expression = try parseDdlGeneratedRowExpressionAlloc(alloc, cursor);
    var expression_transferred = false;
    errdefer if (!expression_transferred) runtime_schema.freeRelationalRowsExpression(alloc, expression);
    const separator = try alloc.dupe(u8, "");
    var separator_transferred = false;
    errdefer if (!separator_transferred) alloc.free(separator);
    expression_transferred = true;
    separator_transferred = true;
    return .{
        .op = .expression,
        .separator = separator,
        .expression = expression,
    };
}

pub fn parseDdlGeneratedRowExpressionAlloc(
    alloc: std.mem.Allocator,
    cursor: parser.Cursor,
) anyerror!runtime_schema.RelationalRowsExpression {
    return try parseDdlGeneratedConcatPipeExpressionAlloc(alloc, cursor);
}

fn parseDdlGeneratedConcatPipeExpressionAlloc(
    alloc: std.mem.Allocator,
    cursor: parser.Cursor,
) anyerror!runtime_schema.RelationalRowsExpression {
    var expression = try parseDdlGeneratedAdditiveExpressionAlloc(alloc, cursor);
    var expression_transferred = false;
    errdefer if (!expression_transferred) runtime_schema.freeRelationalRowsExpression(alloc, expression);
    while (cursor.matchToken(.pipe_concat) != null) {
        const rhs = try parseDdlGeneratedAdditiveExpressionAlloc(alloc, cursor);
        var rhs_transferred = false;
        errdefer if (!rhs_transferred) runtime_schema.freeRelationalRowsExpression(alloc, rhs);
        const operands = try alloc.alloc(runtime_schema.RelationalRowsExpression, 2);
        operands[0] = expression;
        operands[1] = rhs;
        expression_transferred = true;
        rhs_transferred = true;
        expression = try ddlGeneratedOperationExpressionAlloc(alloc, .concat, operands);
        expression_transferred = false;
    }
    expression_transferred = true;
    return expression;
}

fn parseDdlGeneratedAdditiveExpressionAlloc(
    alloc: std.mem.Allocator,
    cursor: parser.Cursor,
) anyerror!runtime_schema.RelationalRowsExpression {
    var expression = try parseDdlGeneratedMultiplicativeExpressionAlloc(alloc, cursor);
    var expression_transferred = false;
    errdefer if (!expression_transferred) runtime_schema.freeRelationalRowsExpression(alloc, expression);
    while (true) {
        const kind: runtime_schema.RelationalRowsExpressionKind = if (cursor.matchToken(.plus) != null)
            .add
        else if (cursor.matchToken(.minus) != null)
            .sub
        else
            break;
        const rhs = try parseDdlGeneratedMultiplicativeExpressionAlloc(alloc, cursor);
        var rhs_transferred = false;
        errdefer if (!rhs_transferred) runtime_schema.freeRelationalRowsExpression(alloc, rhs);
        const operands = try alloc.alloc(runtime_schema.RelationalRowsExpression, 2);
        operands[0] = expression;
        operands[1] = rhs;
        expression_transferred = true;
        rhs_transferred = true;
        expression = try ddlGeneratedOperationExpressionAlloc(alloc, kind, operands);
        expression_transferred = false;
    }
    expression_transferred = true;
    return expression;
}

fn parseDdlGeneratedMultiplicativeExpressionAlloc(
    alloc: std.mem.Allocator,
    cursor: parser.Cursor,
) anyerror!runtime_schema.RelationalRowsExpression {
    var expression = try parseDdlGeneratedPrimaryExpressionAlloc(alloc, cursor);
    var expression_transferred = false;
    errdefer if (!expression_transferred) runtime_schema.freeRelationalRowsExpression(alloc, expression);
    while (true) {
        const kind: runtime_schema.RelationalRowsExpressionKind = if (cursor.matchToken(.star) != null)
            .mul
        else if (cursor.matchToken(.slash) != null)
            .div
        else if (cursor.matchToken(.percent) != null)
            .mod
        else
            break;
        const rhs = try parseDdlGeneratedPrimaryExpressionAlloc(alloc, cursor);
        var rhs_transferred = false;
        errdefer if (!rhs_transferred) runtime_schema.freeRelationalRowsExpression(alloc, rhs);
        const operands = try alloc.alloc(runtime_schema.RelationalRowsExpression, 2);
        operands[0] = expression;
        operands[1] = rhs;
        expression_transferred = true;
        rhs_transferred = true;
        expression = try ddlGeneratedOperationExpressionAlloc(alloc, kind, operands);
        expression_transferred = false;
    }
    expression_transferred = true;
    return expression;
}

fn parseDdlGeneratedPrimaryExpressionAlloc(
    alloc: std.mem.Allocator,
    cursor: parser.Cursor,
) anyerror!runtime_schema.RelationalRowsExpression {
    if (cursor.matchToken(.lparen) != null) {
        const expression = try parseDdlGeneratedRowExpressionAlloc(alloc, cursor);
        var expression_transferred = false;
        errdefer if (!expression_transferred) runtime_schema.freeRelationalRowsExpression(alloc, expression);
        try cursor.expectToken(.rparen);
        expression_transferred = true;
        return expression;
    }
    if (cursor.matchToken(.minus) != null) {
        const number = cursor.matchToken(.number) orelse return error.UnsupportedSqlShape;
        const value_json = try std.fmt.allocPrint(alloc, "-{s}", .{number.text});
        return try ddlGeneratedValueExpressionWithOwnedJsonAlloc(alloc, value_json);
    }
    if (cursor.peekFunctionCallTag(.cast)) return try parseDdlGeneratedCastExpressionAlloc(alloc, cursor);
    if (cursor.peekFunctionCallIf(sqlKeywordIsJsonExtractPathFunction)) return try parseDdlGeneratedJsonExtractPathExpressionAlloc(alloc, cursor);
    if (cursor.peekKind(.identifier) and cursor.pos.* + 1 < cursor.tokens.len and cursor.tokens[cursor.pos.* + 1].kind == .lparen) {
        return try parseDdlGeneratedFunctionExpressionAlloc(alloc, cursor);
    }
    const literal_start = cursor.checkpoint();
    if (sql_value.parseSqlUntypedValueJsonAlloc(alloc, cursor.tokens, cursor.pos)) |value_json| {
        return try ddlGeneratedValueExpressionWithOwnedJsonAlloc(alloc, value_json);
    } else |_| {
        cursor.restore(literal_start);
    }
    const field = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
    return try ddlGeneratedFieldExpressionAlloc(alloc, field.text);
}

fn parseDdlGeneratedFunctionExpressionAlloc(
    alloc: std.mem.Allocator,
    cursor: parser.Cursor,
) !runtime_schema.RelationalRowsExpression {
    const name = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
    const kind = ddlGeneratedFunctionExpressionKind(name.text) orelse return error.UnsupportedSqlShape;
    try cursor.expectToken(.lparen);
    var operands = std.ArrayListUnmanaged(runtime_schema.RelationalRowsExpression).empty;
    errdefer freeDdlGeneratedExpressionList(alloc, &operands);
    if (cursor.matchToken(.rparen) == null) {
        while (true) {
            const operand = try parseDdlGeneratedRowExpressionAlloc(alloc, cursor);
            try operands.append(alloc, operand);
            if (cursor.matchToken(.comma) == null) break;
        }
        try cursor.expectToken(.rparen);
    }
    if (operands.items.len == 0) return error.UnsupportedSqlShape;
    const owned_operands = try operands.toOwnedSlice(alloc);
    return try ddlGeneratedOperationExpressionAlloc(alloc, kind, owned_operands);
}

fn parseDdlGeneratedCastExpressionAlloc(
    alloc: std.mem.Allocator,
    cursor: parser.Cursor,
) !runtime_schema.RelationalRowsExpression {
    try cursor.expectKeyword("cast");
    try cursor.expectToken(.lparen);
    const operand = try parseDdlGeneratedRowExpressionAlloc(alloc, cursor);
    var operand_transferred = false;
    errdefer if (!operand_transferred) runtime_schema.freeRelationalRowsExpression(alloc, operand);
    try cursor.expectKeyword("as");
    const type_token = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
    const cast_type = ddlGeneratedCastType(type_token.text) orelse return error.UnsupportedSqlShape;
    try cursor.expectToken(.rparen);
    const operands = try alloc.alloc(runtime_schema.RelationalRowsExpression, 1);
    operands[0] = operand;
    operand_transferred = true;
    return try ddlGeneratedCastExpressionAlloc(alloc, operands, cast_type);
}

fn parseDdlGeneratedJsonExtractPathExpressionAlloc(
    alloc: std.mem.Allocator,
    cursor: parser.Cursor,
) !runtime_schema.RelationalRowsExpression {
    const function_name = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
    const as_text = std.ascii.eqlIgnoreCase(function_name.text, "json_extract_path_text") or
        std.ascii.eqlIgnoreCase(function_name.text, "jsonb_extract_path_text");
    try cursor.expectToken(.lparen);
    const root = try parseDdlGeneratedRowExpressionAlloc(alloc, cursor);
    var root_transferred = false;
    errdefer if (!root_transferred) runtime_schema.freeRelationalRowsExpression(alloc, root);
    try cursor.expectToken(.comma);
    var path = std.ArrayListUnmanaged(u8).empty;
    errdefer path.deinit(alloc);
    while (true) {
        const path_part = cursor.matchToken(.string) orelse return error.UnsupportedSqlShape;
        if (path.items.len != 0) try path.append(alloc, '.');
        try path.appendSlice(alloc, path_part.text);
        if (cursor.matchToken(.comma) == null) break;
    }
    try cursor.expectToken(.rparen);
    const operands = try alloc.alloc(runtime_schema.RelationalRowsExpression, 1);
    operands[0] = root;
    root_transferred = true;
    const owned_path = try path.toOwnedSlice(alloc);
    return try ddlGeneratedJsonExtractExpressionAlloc(alloc, operands, owned_path, as_text);
}

fn ddlGeneratedFunctionExpressionKind(name: []const u8) ?runtime_schema.RelationalRowsExpressionKind {
    if (std.ascii.eqlIgnoreCase(name, "lower")) return .lower;
    if (std.ascii.eqlIgnoreCase(name, "upper")) return .upper;
    if (std.ascii.eqlIgnoreCase(name, "initcap")) return .initcap;
    if (std.ascii.eqlIgnoreCase(name, "trim") or std.ascii.eqlIgnoreCase(name, "btrim")) return .trim;
    if (std.ascii.eqlIgnoreCase(name, "ltrim")) return .ltrim;
    if (std.ascii.eqlIgnoreCase(name, "rtrim")) return .rtrim;
    if (std.ascii.eqlIgnoreCase(name, "replace")) return .replace;
    if (std.ascii.eqlIgnoreCase(name, "regexp_replace")) return .regexp_replace;
    if (std.ascii.eqlIgnoreCase(name, "translate")) return .translate;
    if (std.ascii.eqlIgnoreCase(name, "substring") or std.ascii.eqlIgnoreCase(name, "substr")) return .substring;
    if (std.ascii.eqlIgnoreCase(name, "overlay")) return .overlay;
    if (std.ascii.eqlIgnoreCase(name, "split_part")) return .split_part;
    if (std.ascii.eqlIgnoreCase(name, "strpos")) return .strpos;
    if (std.ascii.eqlIgnoreCase(name, "left")) return .left;
    if (std.ascii.eqlIgnoreCase(name, "right")) return .right;
    if (std.ascii.eqlIgnoreCase(name, "lpad")) return .lpad;
    if (std.ascii.eqlIgnoreCase(name, "rpad")) return .rpad;
    if (std.ascii.eqlIgnoreCase(name, "repeat")) return .repeat;
    if (std.ascii.eqlIgnoreCase(name, "reverse")) return .reverse;
    if (std.ascii.eqlIgnoreCase(name, "starts_with")) return .starts_with;
    if (std.ascii.eqlIgnoreCase(name, "ends_with")) return .ends_with;
    if (std.ascii.eqlIgnoreCase(name, "ascii")) return .ascii;
    if (std.ascii.eqlIgnoreCase(name, "chr")) return .chr;
    if (std.ascii.eqlIgnoreCase(name, "md5")) return .md5;
    if (std.ascii.eqlIgnoreCase(name, "concat")) return .concat;
    if (std.ascii.eqlIgnoreCase(name, "concat_ws")) return .concat_ws;
    if (std.ascii.eqlIgnoreCase(name, "length") or std.ascii.eqlIgnoreCase(name, "char_length") or std.ascii.eqlIgnoreCase(name, "character_length")) return .length;
    if (std.ascii.eqlIgnoreCase(name, "octet_length")) return .octet_length;
    if (std.ascii.eqlIgnoreCase(name, "bit_length")) return .bit_length;
    if (std.ascii.eqlIgnoreCase(name, "coalesce")) return .coalesce;
    if (std.ascii.eqlIgnoreCase(name, "nullif")) return .nullif;
    if (std.ascii.eqlIgnoreCase(name, "greatest")) return .greatest;
    if (std.ascii.eqlIgnoreCase(name, "least")) return .least;
    if (std.ascii.eqlIgnoreCase(name, "abs")) return .abs;
    if (std.ascii.eqlIgnoreCase(name, "round")) return .round;
    if (std.ascii.eqlIgnoreCase(name, "trunc")) return .trunc;
    if (std.ascii.eqlIgnoreCase(name, "floor")) return .floor;
    if (std.ascii.eqlIgnoreCase(name, "ceil")) return .ceil;
    if (std.ascii.eqlIgnoreCase(name, "sqrt")) return .sqrt;
    if (std.ascii.eqlIgnoreCase(name, "sign")) return .sign;
    if (std.ascii.eqlIgnoreCase(name, "power")) return .power;
    if (std.ascii.eqlIgnoreCase(name, "mod")) return .mod;
    if (std.ascii.eqlIgnoreCase(name, "date_trunc")) return .date_trunc;
    if (std.ascii.eqlIgnoreCase(name, "date_bin")) return .date_bin;
    if (std.ascii.eqlIgnoreCase(name, "date_part")) return .date_part;
    if (std.ascii.eqlIgnoreCase(name, "json_typeof") or std.ascii.eqlIgnoreCase(name, "jsonb_typeof")) return .json_typeof;
    if (std.ascii.eqlIgnoreCase(name, "json_array_length") or std.ascii.eqlIgnoreCase(name, "jsonb_array_length")) return .json_array_length;
    if (std.ascii.eqlIgnoreCase(name, "json_build_object") or std.ascii.eqlIgnoreCase(name, "jsonb_build_object")) return .json_build_object;
    if (std.ascii.eqlIgnoreCase(name, "to_jsonb")) return .to_jsonb;
    if (std.ascii.eqlIgnoreCase(name, "array_length") or std.ascii.eqlIgnoreCase(name, "cardinality")) return .array_length;
    if (std.ascii.eqlIgnoreCase(name, "array_position")) return .array_position;
    if (std.ascii.eqlIgnoreCase(name, "array_positions")) return .array_positions;
    if (std.ascii.eqlIgnoreCase(name, "array_append")) return .array_append;
    if (std.ascii.eqlIgnoreCase(name, "array_prepend")) return .array_prepend;
    if (std.ascii.eqlIgnoreCase(name, "array_cat")) return .array_cat;
    if (std.ascii.eqlIgnoreCase(name, "array_remove")) return .array_remove;
    if (std.ascii.eqlIgnoreCase(name, "array_replace")) return .array_replace;
    if (std.ascii.eqlIgnoreCase(name, "array_to_string")) return .array_to_string;
    if (std.ascii.eqlIgnoreCase(name, "string_to_array")) return .string_to_array;
    return null;
}

fn ddlGeneratedCastType(name: []const u8) ?runtime_schema.RelationalRowsExpressionCastType {
    if (std.ascii.eqlIgnoreCase(name, "text") or std.ascii.eqlIgnoreCase(name, "varchar") or std.ascii.eqlIgnoreCase(name, "uuid")) return .text;
    if (std.ascii.eqlIgnoreCase(name, "numeric") or std.ascii.eqlIgnoreCase(name, "integer") or std.ascii.eqlIgnoreCase(name, "int") or std.ascii.eqlIgnoreCase(name, "bigint")) return .numeric;
    if (std.ascii.eqlIgnoreCase(name, "boolean") or std.ascii.eqlIgnoreCase(name, "bool")) return .bool;
    if (std.ascii.eqlIgnoreCase(name, "timestamp") or std.ascii.eqlIgnoreCase(name, "timestamptz") or std.ascii.eqlIgnoreCase(name, "datetime")) return .datetime;
    return null;
}

fn ddlGeneratedFieldExpressionAlloc(alloc: std.mem.Allocator, field: []const u8) !runtime_schema.RelationalRowsExpression {
    return .{
        .kind = .field,
        .field = try alloc.dupe(u8, field),
        .value_json = try alloc.dupe(u8, ""),
        .json_path = try alloc.dupe(u8, ""),
    };
}

fn ddlGeneratedValueExpressionWithOwnedJsonAlloc(
    alloc: std.mem.Allocator,
    value_json: []const u8,
) !runtime_schema.RelationalRowsExpression {
    errdefer alloc.free(@constCast(value_json));
    return .{
        .kind = .value,
        .field = try alloc.dupe(u8, ""),
        .value_json = value_json,
        .json_path = try alloc.dupe(u8, ""),
    };
}

fn ddlGeneratedOperationExpressionAlloc(
    alloc: std.mem.Allocator,
    kind: runtime_schema.RelationalRowsExpressionKind,
    operands: []const runtime_schema.RelationalRowsExpression,
) !runtime_schema.RelationalRowsExpression {
    errdefer {
        for (operands) |operand| runtime_schema.freeRelationalRowsExpression(alloc, operand);
        alloc.free(@constCast(operands));
    }
    return .{
        .kind = kind,
        .field = try alloc.dupe(u8, ""),
        .value_json = try alloc.dupe(u8, ""),
        .json_path = try alloc.dupe(u8, ""),
        .operands = operands,
    };
}

fn ddlGeneratedCastExpressionAlloc(
    alloc: std.mem.Allocator,
    operands: []const runtime_schema.RelationalRowsExpression,
    cast_type: runtime_schema.RelationalRowsExpressionCastType,
) !runtime_schema.RelationalRowsExpression {
    var expression = try ddlGeneratedOperationExpressionAlloc(alloc, .cast, operands);
    expression.cast_type = cast_type;
    return expression;
}

fn ddlGeneratedJsonExtractExpressionAlloc(
    alloc: std.mem.Allocator,
    operands: []const runtime_schema.RelationalRowsExpression,
    json_path: []const u8,
    as_text: bool,
) !runtime_schema.RelationalRowsExpression {
    errdefer {
        for (operands) |operand| runtime_schema.freeRelationalRowsExpression(alloc, operand);
        alloc.free(@constCast(operands));
        alloc.free(@constCast(json_path));
    }
    return .{
        .kind = .json_extract,
        .field = try alloc.dupe(u8, ""),
        .value_json = try alloc.dupe(u8, ""),
        .json_path = json_path,
        .json_as_text = as_text,
        .operands = operands,
    };
}

fn freeDdlGeneratedExpressionList(
    alloc: std.mem.Allocator,
    list: *std.ArrayListUnmanaged(runtime_schema.RelationalRowsExpression),
) void {
    for (list.items) |expression| runtime_schema.freeRelationalRowsExpression(alloc, expression);
    list.deinit(alloc);
}

fn parseDdlGeneratedConcatExpressionAlloc(
    alloc: std.mem.Allocator,
    cursor: parser.Cursor,
    tokens: []const Token,
    pos: *usize,
    op: runtime_schema.RelationalGeneratedOp,
) !runtime_schema.RelationalGeneratedValue {
    try cursor.expectToken(.lparen);
    var fields = std.ArrayListUnmanaged([]const u8).empty;
    errdefer freeStringList(alloc, &fields);

    var separator: ?[]const u8 = null;
    errdefer if (separator) |value| alloc.free(@constCast(value));
    if (op == .concat_ws) {
        const separator_token = cursor.matchToken(.string) orelse return error.UnsupportedSqlShape;
        separator = try alloc.dupe(u8, separator_token.text);
        try cursor.expectToken(.comma);
    }

    const first = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var first_transferred = false;
    errdefer if (!first_transferred) alloc.free(first);
    try fields.append(alloc, first);
    first_transferred = true;

    while (cursor.matchToken(.comma) != null) {
        if (op == .concat) {
            if (cursor.matchToken(.string)) |token| {
                if (separator) |existing| {
                    if (!std.mem.eql(u8, existing, token.text)) return error.UnsupportedSqlShape;
                } else {
                    separator = try alloc.dupe(u8, token.text);
                }
                try cursor.expectToken(.comma);
            }
        } else if (separator == null) {
            separator = try alloc.dupe(u8, "");
        }
        const field = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
        var field_transferred = false;
        errdefer if (!field_transferred) alloc.free(field);
        try fields.append(alloc, field);
        field_transferred = true;
    }
    try cursor.expectToken(.rparen);
    if (fields.items.len < 2) return error.UnsupportedSqlShape;

    const owned_fields = try fields.toOwnedSlice(alloc);
    var fields_transferred = false;
    errdefer if (!fields_transferred) freeStringSlice(alloc, owned_fields);
    const owned_separator = separator orelse try alloc.dupe(u8, "");
    separator = null;
    fields_transferred = true;
    return .{ .op = op, .fields = owned_fields, .separator = owned_separator };
}

pub fn parseOptionalDdlKnownDefault(tokens: []const Token, pos: *usize) !?DdlKnownDefaultSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    if (cursor.matchKeyword("null")) return .null_literal;
    if (cursor.matchKeyword("gen_random_uuid") or cursor.matchKeyword("uuid_generate_v4")) {
        try cursor.expectToken(.lparen);
        try cursor.expectToken(.rparen);
        return .uuid_v4;
    }
    if (cursor.matchKeyword("now")) {
        try cursor.expectToken(.lparen);
        try cursor.expectToken(.rparen);
        return .now_ns;
    }
    if (cursor.matchKeyword("current_timestamp")) {
        try parseOptionalCurrentTimestampPrecision(tokens, pos);
        return .now_ns;
    }
    if (cursor.matchKeyword("current_date")) return .current_date_ns;
    return null;
}

pub fn parseDdlDefaultValueUntypedAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !runtime_schema.RelationalDefaultValue {
    if (try parseOptionalDdlKnownDefault(tokens, pos)) |known| {
        return try ddlDefaultValueFromKnownSyntaxAlloc(alloc, known, null);
    }
    return .{ .kind = .literal, .value_json = try sql_value.parseSqlUntypedValueJsonAlloc(alloc, tokens, pos) };
}

pub fn ddlDefaultValueFromKnownSyntaxAlloc(
    alloc: std.mem.Allocator,
    known: DdlKnownDefaultSyntax,
    field_type: ?runtime_schema.AntflyType,
) !runtime_schema.RelationalDefaultValue {
    return switch (known) {
        .null_literal => .{ .kind = .literal, .value_json = try alloc.dupe(u8, "null") },
        .uuid_v4 => blk: {
            if (field_type) |ty| {
                if (ty != .keyword and ty != .text and ty != .link) return error.UnsupportedSqlShape;
            }
            break :blk .{ .kind = .uuid_v4, .value_json = try alloc.dupe(u8, "") };
        },
        .now_ns => blk: {
            if (field_type) |ty| {
                if (ty != .numeric and ty != .datetime) return error.UnsupportedSqlShape;
            }
            break :blk .{ .kind = .now_ns, .value_json = try alloc.dupe(u8, "") };
        },
        .current_date_ns => blk: {
            if (field_type) |ty| {
                if (ty != .numeric and ty != .datetime) return error.UnsupportedSqlShape;
            }
            break :blk .{ .kind = .current_date_ns, .value_json = try alloc.dupe(u8, "") };
        },
    };
}

pub fn parseDdlForeignKeyOptions(tokens: []const Token, pos: *usize) !DdlForeignKeyOptionsSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    var options: DdlForeignKeyOptionsSyntax = .{};
    var match_seen = false;
    while (!cursor.atEnd() and !cursor.peekKind(.comma) and !cursor.peekKind(.rparen) and !cursor.peekKind(.semicolon)) {
        if (peekDdlNotValid(tokens, pos.*)) break;
        if (cursor.matchKeyword("on")) {
            if (cursor.matchKeyword("delete")) {
                options.on_delete = try parseDdlForeignKeyActionSyntax(cursor);
            } else if (cursor.matchKeyword("update")) {
                options.on_update = try parseDdlForeignKeyActionSyntax(cursor);
            } else {
                return error.UnsupportedSqlShape;
            }
        } else if (cursor.matchKeyword("deferrable")) {
            options.deferrable = true;
        } else if (cursor.matchKeyword("not")) {
            try cursor.expectKeyword("deferrable");
            options.deferrable = false;
        } else if (cursor.matchKeyword("initially")) {
            if (cursor.matchKeyword("deferred")) {
                options.timing = .deferred;
                options.deferrable = true;
            } else {
                try cursor.expectKeyword("immediate");
                options.timing = .immediate;
            }
        } else if (cursor.matchKeyword("match")) {
            if (match_seen) return error.UnsupportedSqlShape;
            match_seen = true;
            if (cursor.matchKeyword("simple")) {
                options.match = .simple;
            } else if (cursor.matchKeyword("full")) {
                options.match = .full;
            } else if (cursor.matchKeyword("partial")) {
                return error.UnsupportedSqlShape;
            } else {
                return error.UnsupportedSqlShape;
            }
        } else {
            return error.UnsupportedSqlShape;
        }
    }
    return options;
}

fn parseDdlForeignKeyActionSyntax(cursor: parser.Cursor) !DdlForeignKeyActionSyntax {
    if (cursor.matchKeyword("cascade")) return .cascade;
    if (cursor.matchKeyword("restrict")) return .restrict;
    if (cursor.matchKeyword("no")) {
        try cursor.expectKeyword("action");
        return .no_action;
    }
    if (cursor.matchKeyword("set")) {
        try cursor.expectKeyword("null");
        return .set_null;
    }
    return error.UnsupportedSqlShape;
}

pub fn parseOptionalDdlCollationAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !?[]const u8 {
    const cursor = parser.Cursor.init(tokens, pos);
    if (!cursor.matchKeyword("collate")) return null;
    const first = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
    if (std.mem.endsWith(u8, first.text, ".")) {
        const second = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
        return try std.fmt.allocPrint(alloc, "{s}{s}", .{ first.text, second.text });
    }
    return try alloc.dupe(u8, first.text);
}

pub fn parseOptionalCurrentTimestampPrecision(tokens: []const Token, pos: *usize) !void {
    const cursor = parser.Cursor.init(tokens, pos);
    if (cursor.matchToken(.lparen) == null) return;
    const token = cursor.matchToken(.number) orelse return error.UnsupportedSqlShape;
    const precision = std.fmt.parseUnsigned(u8, token.text, 10) catch return error.UnsupportedSqlShape;
    if (precision > 6) return error.UnsupportedSqlShape;
    try cursor.expectToken(.rparen);
}

pub fn parseDdlType(tokens: []const Token, pos: *usize) !DdlTypeSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    const first = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
    const base = ddlBaseTypeForName(first.text) orelse blk: {
        if (std.ascii.eqlIgnoreCase(first.text, "character")) {
            try cursor.expectKeyword("varying");
            break :blk runtime_schema.AntflyType.keyword;
        }
        if (std.ascii.eqlIgnoreCase(first.text, "double")) {
            try cursor.expectKeyword("precision");
            break :blk runtime_schema.AntflyType.numeric;
        }
        if (std.ascii.eqlIgnoreCase(first.text, "timestamp")) {
            if (cursor.matchKeyword("with")) {
                try cursor.expectKeyword("time");
                try cursor.expectKeyword("zone");
            } else if (cursor.matchKeyword("without")) {
                try cursor.expectKeyword("time");
                try cursor.expectKeyword("zone");
            }
            break :blk runtime_schema.AntflyType.datetime;
        }
        return error.UnsupportedSqlShape;
    };

    if (cursor.peekKind(.lparen)) try skipParenthesizedTokens(cursor);
    const is_array = if (cursor.matchToken(.lbracket) != null) blk: {
        try cursor.expectToken(.rbracket);
        break :blk true;
    } else false;
    if (!is_array) return .{ .field_type = base };
    if (base == .json or base == .array or base == .blob) return error.UnsupportedSqlShape;
    return .{ .field_type = .array, .array_item_type = base };
}

fn ddlBaseTypeForName(name: []const u8) ?runtime_schema.AntflyType {
    if (std.ascii.eqlIgnoreCase(name, "uuid")) return .keyword;
    if (std.ascii.eqlIgnoreCase(name, "text")) return .keyword;
    if (std.ascii.eqlIgnoreCase(name, "varchar")) return .keyword;
    if (std.ascii.eqlIgnoreCase(name, "citext")) return .keyword;
    if (std.ascii.eqlIgnoreCase(name, "integer")) return .numeric;
    if (std.ascii.eqlIgnoreCase(name, "int")) return .numeric;
    if (std.ascii.eqlIgnoreCase(name, "int4")) return .numeric;
    if (std.ascii.eqlIgnoreCase(name, "bigint")) return .numeric;
    if (std.ascii.eqlIgnoreCase(name, "int8")) return .numeric;
    if (std.ascii.eqlIgnoreCase(name, "smallint")) return .numeric;
    if (std.ascii.eqlIgnoreCase(name, "numeric")) return .numeric;
    if (std.ascii.eqlIgnoreCase(name, "decimal")) return .numeric;
    if (std.ascii.eqlIgnoreCase(name, "real")) return .numeric;
    if (std.ascii.eqlIgnoreCase(name, "float4")) return .numeric;
    if (std.ascii.eqlIgnoreCase(name, "float8")) return .numeric;
    if (std.ascii.eqlIgnoreCase(name, "boolean")) return .boolean;
    if (std.ascii.eqlIgnoreCase(name, "bool")) return .boolean;
    if (std.ascii.eqlIgnoreCase(name, "date")) return .datetime;
    if (std.ascii.eqlIgnoreCase(name, "timestamptz")) return .datetime;
    if (std.ascii.eqlIgnoreCase(name, "json")) return .json;
    if (std.ascii.eqlIgnoreCase(name, "jsonb")) return .json;
    if (std.ascii.eqlIgnoreCase(name, "bytea")) return .blob;
    return null;
}

fn skipParenthesizedTokens(cursor: parser.Cursor) !void {
    try cursor.expectToken(.lparen);
    var depth: usize = 1;
    while (depth > 0) {
        if (cursor.atEnd()) return error.UnsupportedSqlShape;
        if (cursor.matchToken(.lparen) != null) {
            depth += 1;
        } else if (cursor.matchToken(.rparen) != null) {
            depth -= 1;
        } else {
            try cursor.advance(1);
        }
    }
}

pub fn parseCreateTableDefinitionHeaderAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !CreateTableDefinitionHeaderSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("table");
    const if_not_exists = try parseOptionalIfNotExists(cursor);
    const table_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    var table_transferred = false;
    errdefer if (!table_transferred) alloc.free(table_name);
    try cursor.expectToken(.lparen);

    table_transferred = true;
    return .{ .table_name = table_name, .if_not_exists = if_not_exists };
}

pub fn parseCreateTableElementPrefixAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !CreateTableElementPrefixSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    const constraint_name: ?[]const u8 = if (cursor.matchKeyword("constraint"))
        try parseIdentifierOwnedAlloc(alloc, tokens, pos)
    else
        null;
    var constraint_name_transferred = false;
    errdefer if (!constraint_name_transferred) if (constraint_name) |name| alloc.free(name);

    const kind: CreateTableElementKindSyntax = if (cursor.peekKeyword("period")) blk: {
        break :blk .period;
    } else if (cursor.peekKeyword("primary")) blk: {
        break :blk .primary_key;
    } else if (cursor.peekKeyword("unique")) blk: {
        break :blk .unique;
    } else if (cursor.peekKeyword("foreign")) blk: {
        break :blk .foreign_key;
    } else if (cursor.peekKeyword("check")) blk: {
        break :blk .check;
    } else blk: {
        if (constraint_name != null) return error.UnsupportedSqlShape;
        break :blk .column;
    };

    constraint_name_transferred = true;
    return .{
        .kind = kind,
        .constraint_name = constraint_name,
    };
}

pub fn parseCreateTableColumnConstraintPrefixAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !CreateTableColumnConstraintPrefixSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    const constraint_name: ?[]const u8 = if (cursor.matchKeyword("constraint"))
        try parseIdentifierOwnedAlloc(alloc, tokens, pos)
    else
        null;
    var constraint_name_transferred = false;
    errdefer if (!constraint_name_transferred) if (constraint_name) |name| alloc.free(name);

    const kind: CreateTableColumnConstraintKindSyntax = if (cursor.peekKeyword("primary")) blk: {
        break :blk .primary_key;
    } else if (cursor.peekKeyword("unique")) blk: {
        break :blk .unique;
    } else if (cursor.peekKeyword("check")) blk: {
        break :blk .check;
    } else if (cursor.peekKeyword("references")) blk: {
        break :blk .references;
    } else {
        return error.UnsupportedSqlShape;
    };

    constraint_name_transferred = true;
    return .{
        .kind = kind,
        .constraint_name = constraint_name,
    };
}

pub fn parseCreateTableCloneCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !TableClonePlan {
    const cursor = parser.Cursor.init(tokens, pos);
    if (cursor.matchKeyword("temporary") or cursor.matchKeyword("temp") or cursor.matchKeyword("unlogged")) return error.UnsupportedSqlShape;
    try cursor.expectKeyword("table");
    const if_not_exists = try parseOptionalIfNotExists(cursor);

    const table_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    var table_name_transferred = false;
    errdefer if (!table_name_transferred) alloc.free(table_name);
    try cursor.expectToken(.lparen);
    try cursor.expectKeyword("like");
    const source_table_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    var source_transferred = false;
    errdefer if (!source_transferred) alloc.free(source_table_name);

    var options: TableCloneOptions = .{};
    while (!cursor.atEnd() and !cursor.peekKind(.rparen)) {
        const include = if (cursor.matchKeyword("including"))
            true
        else if (cursor.matchKeyword("excluding"))
            false
        else
            return error.UnsupportedSqlShape;
        try parseTableCloneOption(cursor, &options, include);
    }
    try cursor.expectToken(.rparen);
    try adapterNoopStatementEnd(cursor);

    table_name_transferred = true;
    source_transferred = true;
    return .{
        .table_name = table_name,
        .source_table_name = source_table_name,
        .if_not_exists = if_not_exists,
        .options = options,
    };
}

fn parseTableCloneOption(cursor: parser.Cursor, options: *TableCloneOptions, include: bool) !void {
    if (cursor.matchKeyword("all")) {
        options.* = if (include) TableCloneOptions.includingAll() else .{};
        return;
    }
    if (cursor.matchKeyword("defaults")) {
        options.defaults = include;
        return;
    }
    if (cursor.matchKeyword("generated")) {
        options.generated = include;
        return;
    }
    if (cursor.matchKeyword("constraints")) {
        options.checks = include;
        options.constraints = include;
        return;
    }
    if (cursor.matchKeyword("indexes")) {
        options.indexes = include;
        return;
    }
    if (cursor.matchKeyword("periods")) {
        options.periods = include;
        return;
    }
    if (cursor.matchKeyword("update")) {
        try cursor.expectKeyword("policies");
        options.update_policies = include;
        return;
    }
    if (cursor.matchKeyword("comments") or
        cursor.matchKeyword("storage") or
        cursor.matchKeyword("statistics") or
        cursor.matchKeyword("compression") or
        cursor.matchKeyword("identity"))
    {
        return;
    }
    return error.UnsupportedSqlShape;
}

pub fn parseCreateTablePartitionCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !CreateTablePartitionPlan {
    const cursor = parser.Cursor.init(tokens, pos);
    if (cursor.matchKeyword("temporary") or cursor.matchKeyword("temp") or cursor.matchKeyword("unlogged")) return error.UnsupportedSqlShape;
    try cursor.expectKeyword("table");
    if (try parseOptionalIfNotExists(cursor)) return error.UnsupportedSqlShape;

    const table_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    var table_transferred = false;
    errdefer if (!table_transferred) alloc.free(table_name);
    try cursor.expectKeyword("partition");
    try cursor.expectKeyword("of");
    const parent_table_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    var parent_transferred = false;
    errdefer if (!parent_transferred) alloc.free(parent_table_name);
    const bounds = try parseTablePartitionBoundsAlloc(alloc, cursor, tokens, pos);
    var bounds_transferred = false;
    errdefer if (!bounds_transferred) {
        var mutable_bounds = bounds;
        mutable_bounds.deinit(alloc);
    };
    try adapterNoopStatementEnd(cursor);

    table_transferred = true;
    parent_transferred = true;
    bounds_transferred = true;
    return .{
        .table_name = table_name,
        .parent_table_name = parent_table_name,
        .bounds = bounds,
    };
}

pub fn parseCreatePartitionedTableCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !CreatePartitionedTableTailSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("partition");
    try cursor.expectKeyword("by");
    const method: ddl_plan.TablePartitionMethod = if (cursor.matchKeyword("range"))
        .range
    else
        return error.UnsupportedSqlShape;
    const keys = try parseParenthesizedIdentifierListAlloc(alloc, cursor, tokens, pos);
    var keys_transferred = false;
    errdefer if (!keys_transferred) freeStringSlice(alloc, keys);
    try adapterNoopStatementEnd(cursor);

    keys_transferred = true;
    return .{
        .method = method,
        .keys = keys,
    };
}

pub fn parseAlterTablePartitionCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !TablePartitionCatalogPlan {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("table");
    const parent_table_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    var parent_transferred = false;
    errdefer if (!parent_transferred) alloc.free(parent_table_name);

    if (cursor.matchKeyword("attach")) {
        try cursor.expectKeyword("partition");
        const partition_table_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
        var partition_transferred = false;
        errdefer if (!partition_transferred) alloc.free(partition_table_name);
        const bounds = try parseTablePartitionBoundsAlloc(alloc, cursor, tokens, pos);
        var bounds_transferred = false;
        errdefer if (!bounds_transferred) {
            var mutable_bounds = bounds;
            mutable_bounds.deinit(alloc);
        };
        try adapterNoopStatementEnd(cursor);

        parent_transferred = true;
        partition_transferred = true;
        bounds_transferred = true;
        return .{ .attach = .{
            .parent_table_name = parent_table_name,
            .partition_table_name = partition_table_name,
            .bounds = bounds,
        } };
    }

    if (cursor.matchKeyword("detach")) {
        try cursor.expectKeyword("partition");
        const partition_table_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
        var partition_transferred = false;
        errdefer if (!partition_transferred) alloc.free(partition_table_name);
        try adapterNoopStatementEnd(cursor);

        parent_transferred = true;
        partition_transferred = true;
        return .{ .detach = .{
            .parent_table_name = parent_table_name,
            .partition_table_name = partition_table_name,
        } };
    }

    return error.UnsupportedSqlShape;
}

fn parseTablePartitionBoundsAlloc(
    alloc: std.mem.Allocator,
    cursor: parser.Cursor,
    tokens: []const Token,
    pos: *usize,
) !TablePartitionBounds {
    try cursor.expectKeyword("for");
    try cursor.expectKeyword("values");
    try cursor.expectKeyword("from");
    try cursor.expectToken(.lparen);
    const lower_json = try sql_value.parseSqlUntypedValueJsonAlloc(alloc, tokens, pos);
    var lower_transferred = false;
    errdefer if (!lower_transferred) alloc.free(lower_json);
    try cursor.expectToken(.rparen);
    try cursor.expectKeyword("to");
    try cursor.expectToken(.lparen);
    const upper_json = try sql_value.parseSqlUntypedValueJsonAlloc(alloc, tokens, pos);
    var upper_transferred = false;
    errdefer if (!upper_transferred) alloc.free(upper_json);
    try cursor.expectToken(.rparen);

    lower_transferred = true;
    upper_transferred = true;
    return .{
        .lower_json = lower_json,
        .upper_json = upper_json,
    };
}

pub fn parseCreateViewCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    replace_existing: bool,
) !CreateViewSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("view");
    const if_not_exists = try parseOptionalIfNotExists(cursor);
    var syntax = try parseSimpleViewDefinitionAlloc(alloc, cursor, tokens, pos);
    errdefer syntax.deinit(alloc);
    try adapterNoopStatementEnd(cursor);
    syntax.replace_existing = replace_existing;
    syntax.if_not_exists = if_not_exists;
    return syntax;
}

pub fn parseCreateMaterializedViewCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    replace_existing: bool,
) !CreateMaterializedViewSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("materialized");
    try cursor.expectKeyword("view");
    const if_not_exists = try parseOptionalIfNotExists(cursor);
    var definition = try parseSimpleViewDefinitionAlloc(alloc, cursor, tokens, pos);
    var definition_transferred = false;
    errdefer if (!definition_transferred) definition.deinit(alloc);
    var populate_on_create = true;
    if (cursor.matchKeyword("with")) {
        if (cursor.matchKeyword("no")) {
            try cursor.expectKeyword("data");
            populate_on_create = false;
        } else {
            try cursor.expectKeyword("data");
            populate_on_create = true;
        }
    }
    try adapterNoopStatementEnd(cursor);
    definition_transferred = true;
    return .{
        .view_name = definition.view_name,
        .source_table_name = definition.source_table_name,
        .source_fields = definition.source_fields,
        .output_fields = definition.output_fields,
        .replace_existing = replace_existing,
        .if_not_exists = if_not_exists,
        .populate_on_create = populate_on_create,
    };
}

pub fn parseDropViewCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !DropViewSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("view");
    var if_exists = false;
    if (cursor.matchKeyword("if")) {
        try cursor.expectKeyword("exists");
        if_exists = true;
    }
    const view_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    var view_transferred = false;
    errdefer if (!view_transferred) alloc.free(view_name);
    if (cursor.matchToken(.comma) != null) return error.UnsupportedSqlShape;
    const cascade = cursor.matchKeyword("cascade");
    if (!cascade) _ = cursor.matchKeyword("restrict");
    try adapterNoopStatementEnd(cursor);
    view_transferred = true;
    return .{ .view_name = view_name, .if_exists = if_exists, .cascade = cascade };
}

pub fn parseDropMaterializedViewCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !DropMaterializedViewSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("materialized");
    try cursor.expectKeyword("view");
    var if_exists = false;
    if (cursor.matchKeyword("if")) {
        try cursor.expectKeyword("exists");
        if_exists = true;
    }
    const view_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    var view_transferred = false;
    errdefer if (!view_transferred) alloc.free(view_name);
    if (cursor.matchToken(.comma) != null) return error.UnsupportedSqlShape;
    const cascade = cursor.matchKeyword("cascade");
    if (!cascade) _ = cursor.matchKeyword("restrict");
    try adapterNoopStatementEnd(cursor);
    view_transferred = true;
    return .{ .view_name = view_name, .if_exists = if_exists, .cascade = cascade };
}

pub fn parseRefreshMaterializedViewCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !RefreshMaterializedViewSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("materialized");
    try cursor.expectKeyword("view");
    const concurrently = cursor.matchKeyword("concurrently");
    const view_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    var view_transferred = false;
    errdefer if (!view_transferred) alloc.free(view_name);
    var populate = true;
    if (cursor.matchKeyword("with")) {
        if (cursor.matchKeyword("no")) {
            try cursor.expectKeyword("data");
            populate = false;
        } else {
            try cursor.expectKeyword("data");
            populate = true;
        }
    }
    try adapterNoopStatementEnd(cursor);
    view_transferred = true;
    return .{ .view_name = view_name, .concurrently = concurrently, .populate = populate };
}

pub fn parseRenameViewCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !RenameViewSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("view");
    const view_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    var view_transferred = false;
    errdefer if (!view_transferred) alloc.free(view_name);
    try cursor.expectKeyword("rename");
    try cursor.expectKeyword("to");
    const new_view_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    var new_transferred = false;
    errdefer if (!new_transferred) alloc.free(new_view_name);
    try adapterNoopStatementEnd(cursor);
    view_transferred = true;
    new_transferred = true;
    return .{ .view_name = view_name, .new_view_name = new_view_name };
}

const SimpleViewSelectFieldsSyntax = struct {
    source_fields: []const []const u8 = &.{},
    output_fields: []const []const u8 = &.{},

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        freeStringSlice(alloc, self.source_fields);
        freeStringSlice(alloc, self.output_fields);
        self.* = undefined;
    }
};

fn parseOptionalIfNotExists(cursor: parser.Cursor) !bool {
    if (!cursor.matchKeyword("if")) return false;
    try cursor.expectKeyword("not");
    try cursor.expectKeyword("exists");
    return true;
}

fn parseSimpleViewDefinitionAlloc(
    alloc: std.mem.Allocator,
    cursor: parser.Cursor,
    tokens: []const Token,
    pos: *usize,
) !CreateViewSyntax {
    const view_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    var view_transferred = false;
    errdefer if (!view_transferred) alloc.free(view_name);
    const declared_output_fields = try parseOptionalViewColumnListAlloc(alloc, cursor, tokens, pos);
    var declared_output_transferred = false;
    errdefer if (!declared_output_transferred) if (declared_output_fields) |fields| freeStringSlice(alloc, fields);
    try cursor.expectKeyword("as");
    try cursor.expectKeyword("select");

    var selected_fields = try parseSimpleViewSelectFieldsAlloc(alloc, cursor, tokens, pos);
    var selected_fields_transferred = false;
    errdefer if (!selected_fields_transferred) selected_fields.deinit(alloc);
    try cursor.expectKeyword("from");
    const source_table_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    var source_transferred = false;
    errdefer if (!source_transferred) alloc.free(source_table_name);

    const output_fields = if (declared_output_fields) |aliases| blk: {
        if (aliases.len != selected_fields.source_fields.len) return error.UnsupportedSqlShape;
        freeStringSlice(alloc, selected_fields.output_fields);
        selected_fields.output_fields = &.{};
        declared_output_transferred = true;
        break :blk aliases;
    } else blk: {
        const fields = selected_fields.output_fields;
        selected_fields.output_fields = &.{};
        break :blk fields;
    };
    var output_fields_transferred = false;
    errdefer if (!output_fields_transferred) freeStringSlice(alloc, output_fields);

    const source_fields = selected_fields.source_fields;
    selected_fields.source_fields = &.{};
    selected_fields_transferred = true;
    view_transferred = true;
    source_transferred = true;
    output_fields_transferred = true;
    return .{
        .view_name = view_name,
        .source_table_name = source_table_name,
        .source_fields = source_fields,
        .output_fields = output_fields,
    };
}

fn parseOptionalViewColumnListAlloc(
    alloc: std.mem.Allocator,
    cursor: parser.Cursor,
    tokens: []const Token,
    pos: *usize,
) !?[]const []const u8 {
    if (cursor.matchToken(.lparen) == null) return null;
    var fields = std.ArrayListUnmanaged([]const u8).empty;
    errdefer freeStringList(alloc, &fields);
    while (true) {
        if (cursor.peekKind(.rparen)) return error.UnsupportedSqlShape;
        const field = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
        var field_transferred = false;
        errdefer if (!field_transferred) alloc.free(field);
        try fields.append(alloc, field);
        field_transferred = true;
        if (cursor.matchToken(.comma) == null) break;
    }
    try cursor.expectToken(.rparen);
    if (fields.items.len == 0) return error.UnsupportedSqlShape;
    return try fields.toOwnedSlice(alloc);
}

fn parseSimpleViewSelectFieldsAlloc(
    alloc: std.mem.Allocator,
    cursor: parser.Cursor,
    tokens: []const Token,
    pos: *usize,
) !SimpleViewSelectFieldsSyntax {
    var source_fields = std.ArrayListUnmanaged([]const u8).empty;
    var output_fields = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        freeStringList(alloc, &source_fields);
        freeStringList(alloc, &output_fields);
    }
    while (true) {
        if (cursor.peekKind(.star)) return error.UnsupportedSqlShape;
        const field = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
        var field_transferred = false;
        errdefer if (!field_transferred) alloc.free(field);
        const output = if (cursor.matchKeyword("as"))
            try parseIdentifierOwnedAlloc(alloc, tokens, pos)
        else if (cursor.peekKind(.identifier) and !cursor.peekKeyword("from"))
            try parseIdentifierOwnedAlloc(alloc, tokens, pos)
        else
            try alloc.dupe(u8, field);
        var output_transferred = false;
        errdefer if (!output_transferred) alloc.free(output);
        try source_fields.append(alloc, field);
        field_transferred = true;
        try output_fields.append(alloc, output);
        output_transferred = true;
        if (cursor.matchToken(.comma) == null) break;
    }
    if (source_fields.items.len == 0 or source_fields.items.len != output_fields.items.len) return error.UnsupportedSqlShape;
    const owned_source_fields = try source_fields.toOwnedSlice(alloc);
    var source_fields_transferred = false;
    errdefer if (!source_fields_transferred) freeStringSlice(alloc, owned_source_fields);
    const owned_output_fields = try output_fields.toOwnedSlice(alloc);
    source_fields_transferred = true;
    return .{
        .source_fields = owned_source_fields,
        .output_fields = owned_output_fields,
    };
}

pub fn parseCreateRoleCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !CreateRoleSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try expectRoleAliasKeyword(cursor);
    const role_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var role_transferred = false;
    errdefer if (!role_transferred) alloc.free(role_name);
    try adapterNoopStatementEnd(cursor);
    role_transferred = true;
    return .{ .role_name = role_name };
}

pub fn parseAlterRoleCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !AlterRoleSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try expectRoleAliasKeyword(cursor);
    const role_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var role_transferred = false;
    errdefer if (!role_transferred) alloc.free(role_name);
    var database_name: ?[]const u8 = null;
    var database_transferred = true;
    if (cursor.matchKeyword("in")) {
        try cursor.expectKeyword("database");
        database_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
        database_transferred = false;
    }
    errdefer if (!database_transferred) if (database_name) |value| alloc.free(value);
    const operation: ddl_plan.AlterRolePlan.Operation = if (cursor.matchKeyword("set"))
        .set
    else if (cursor.matchKeyword("reset"))
        .reset
    else
        return error.UnsupportedSqlShape;
    const setting_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var setting_transferred = false;
    errdefer if (!setting_transferred) alloc.free(setting_name);
    const setting_kind: ddl_plan.AlterRolePlan.SettingKind = if (std.mem.startsWith(u8, setting_name, "app."))
        .app
    else
        .runtime;
    var setting_value: ?ddl_plan.AlterRolePlan.SettingValue = null;
    var value_transferred = true;
    if (operation == .set) {
        try cursor.expectToken(.eq);
        if (cursor.atEnd() or cursor.peekKind(.semicolon)) return error.UnsupportedSqlShape;
        if (cursor.matchToken(.string)) |value| {
            setting_value = .{ .literal = try alloc.dupe(u8, value.text) };
        } else if (cursor.matchKeyword("current_setting")) {
            try cursor.expectToken(.lparen);
            const source = cursor.matchToken(.string) orelse return error.UnsupportedSqlShape;
            if (source.text.len == 0) return error.UnsupportedSqlShape;
            setting_value = .{ .current_setting = try alloc.dupe(u8, source.text) };
            try cursor.expectToken(.rparen);
        } else {
            return error.UnsupportedSqlShape;
        }
        value_transferred = false;
        errdefer if (!value_transferred) if (setting_value) |*value| value.deinit(alloc);
    }
    try adapterNoopStatementEnd(cursor);
    role_transferred = true;
    database_transferred = true;
    setting_transferred = true;
    value_transferred = true;
    return .{
        .role_name = role_name,
        .database_name = database_name,
        .operation = operation,
        .setting_kind = setting_kind,
        .setting_name = setting_name,
        .setting_value = setting_value,
    };
}

pub fn parseDropRoleCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !DropRoleSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try expectRoleAliasKeyword(cursor);
    var if_exists = false;
    if (cursor.matchKeyword("if")) {
        try cursor.expectKeyword("exists");
        if_exists = true;
    }
    const role_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var role_transferred = false;
    errdefer if (!role_transferred) alloc.free(role_name);
    try adapterNoopStatementEnd(cursor);
    role_transferred = true;
    return .{ .role_name = role_name, .if_exists = if_exists };
}

pub fn isRoleAliasKeywordToken(token: Token) bool {
    return token.matchesKeywordTag(.role) or
        token.matchesKeyword("user") or
        token.matchesKeywordTag(.group);
}

fn expectRoleAliasKeyword(cursor: parser.Cursor) !void {
    if (cursor.matchKeyword("role") or
        cursor.matchKeyword("user") or
        cursor.matchKeyword("group"))
    {
        return;
    }
    return error.UnsupportedSqlShape;
}

pub fn parsePrivilegeChangeTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    action: PrivilegeChangeActionSyntax,
) !PrivilegeChangeSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    const privileges = try parsePrivilegeListAlloc(alloc, cursor, tokens, pos);
    var privileges_transferred = false;
    errdefer if (!privileges_transferred) freeStringSlice(alloc, privileges);
    try cursor.expectKeyword("on");

    var object_kind: ?[]const u8 = null;
    var object_kind_transferred = false;
    errdefer if (!object_kind_transferred) if (object_kind) |value| alloc.free(@constCast(value));
    var object_name: ?[]const u8 = null;
    var object_name_transferred = false;
    errdefer if (!object_name_transferred) if (object_name) |value| alloc.free(@constCast(value));

    if (cursor.matchKeyword("all")) {
        try cursor.expectKeyword("tables");
        try cursor.expectKeyword("in");
        try cursor.expectKeyword("schema");
        object_kind = try alloc.dupe(u8, "ALL_TABLES_IN_SCHEMA");
        object_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    } else {
        object_kind = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
        object_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    }

    switch (action) {
        .grant => try cursor.expectKeyword("to"),
        .revoke => try cursor.expectKeyword("from"),
    }
    const principal_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var principal_transferred = false;
    errdefer if (!principal_transferred) alloc.free(principal_name);

    try adapterNoopStatementEnd(cursor);
    privileges_transferred = true;
    object_kind_transferred = true;
    object_name_transferred = true;
    principal_transferred = true;
    return .{
        .privileges = privileges,
        .object_kind = object_kind.?,
        .object_name = object_name.?,
        .principal_name = principal_name,
    };
}

pub fn parseBulkIoTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !BulkIoSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    const table_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    var table_transferred = false;
    errdefer if (!table_transferred) alloc.free(table_name);

    var columns = std.ArrayListUnmanaged([]const u8).empty;
    errdefer freeStringList(alloc, &columns);
    if (cursor.matchToken(.lparen) != null) {
        while (true) {
            const column_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
            try columns.append(alloc, column_name);
            if (cursor.matchToken(.comma) != null) continue;
            try cursor.expectToken(.rparen);
            break;
        }
    }

    const direction: BulkIoDirectionSyntax = if (cursor.matchKeyword("from"))
        .from
    else if (cursor.matchKeyword("to"))
        .to
    else
        return error.UnsupportedSqlShape;
    var endpoint_kind: ddl_plan.BulkIoEndpointKind = .stream;
    const endpoint = blk: {
        if (cursor.matchKeyword("program")) {
            endpoint_kind = .program;
            break :blk try parseSqlStringLiteralValueAlloc(alloc, cursor);
        }
        if (cursor.matchToken(.string)) |token| {
            endpoint_kind = .file;
            break :blk try alloc.dupe(u8, token.text);
        }
        break :blk try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    };
    var endpoint_transferred = false;
    errdefer if (!endpoint_transferred) alloc.free(endpoint);
    if (endpoint_kind == .stream) {
        switch (direction) {
            .from => if (!std.ascii.eqlIgnoreCase(endpoint, "STDIN")) return error.UnsupportedSqlShape,
            .to => if (!std.ascii.eqlIgnoreCase(endpoint, "STDOUT")) return error.UnsupportedSqlShape,
        }
    }

    var format: ?[]const u8 = null;
    var header = false;
    var freeze = false;
    var on_error: ddl_plan.BulkIoOnErrorPolicy = .stop;
    var reject_limit: ?usize = null;
    var log_verbosity: ddl_plan.BulkIoLogVerbosity = .default;
    var force_quote_all = false;
    var force_quote_columns: []const []const u8 = &.{};
    var force_not_null_columns: []const []const u8 = &.{};
    var force_null_columns: []const []const u8 = &.{};
    var delimiter: ?[]const u8 = null;
    var quote: ?[]const u8 = null;
    var escape: ?[]const u8 = null;
    var null_marker: ?[]const u8 = null;
    var default_marker: ?[]const u8 = null;
    var encoding: ?[]const u8 = null;
    var format_transferred = false;
    var force_quote_columns_transferred = false;
    var force_not_null_columns_transferred = false;
    var force_null_columns_transferred = false;
    var delimiter_transferred = false;
    var quote_transferred = false;
    var escape_transferred = false;
    var null_marker_transferred = false;
    var default_marker_transferred = false;
    var encoding_transferred = false;
    errdefer if (!format_transferred) if (format) |value| alloc.free(@constCast(value));
    errdefer if (!force_quote_columns_transferred) freeStringSlice(alloc, force_quote_columns);
    errdefer if (!force_not_null_columns_transferred) freeStringSlice(alloc, force_not_null_columns);
    errdefer if (!force_null_columns_transferred) freeStringSlice(alloc, force_null_columns);
    errdefer if (!delimiter_transferred) if (delimiter) |value| alloc.free(@constCast(value));
    errdefer if (!quote_transferred) if (quote) |value| alloc.free(@constCast(value));
    errdefer if (!escape_transferred) if (escape) |value| alloc.free(@constCast(value));
    errdefer if (!null_marker_transferred) if (null_marker) |value| alloc.free(@constCast(value));
    errdefer if (!default_marker_transferred) if (default_marker) |value| alloc.free(@constCast(value));
    errdefer if (!encoding_transferred) if (encoding) |value| alloc.free(@constCast(value));
    if (cursor.matchKeyword("with")) {
        try cursor.expectToken(.lparen);
        while (true) {
            if (cursor.matchKeyword("format")) {
                if (format != null) return error.UnsupportedSqlShape;
                format = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
            } else if (cursor.matchKeyword("header")) {
                const value = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
                if (std.ascii.eqlIgnoreCase(value.text, "true")) {
                    header = true;
                } else if (std.ascii.eqlIgnoreCase(value.text, "false")) {
                    header = false;
                } else return error.UnsupportedSqlShape;
            } else if (cursor.matchKeyword("freeze")) {
                const value = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
                if (std.ascii.eqlIgnoreCase(value.text, "true")) {
                    freeze = true;
                } else if (std.ascii.eqlIgnoreCase(value.text, "false")) {
                    freeze = false;
                } else return error.UnsupportedSqlShape;
            } else if (cursor.matchKeyword("on_error")) {
                const value = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
                if (std.ascii.eqlIgnoreCase(value.text, "stop")) {
                    on_error = .stop;
                } else if (std.ascii.eqlIgnoreCase(value.text, "ignore")) {
                    on_error = .ignore;
                } else return error.UnsupportedSqlShape;
            } else if (cursor.matchKeyword("reject_limit")) {
                if (reject_limit != null) return error.UnsupportedSqlShape;
                reject_limit = try parseBulkIoRejectLimit(cursor);
            } else if (cursor.matchKeyword("log_verbosity")) {
                const value = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
                if (std.ascii.eqlIgnoreCase(value.text, "default")) {
                    log_verbosity = .default;
                } else if (std.ascii.eqlIgnoreCase(value.text, "verbose")) {
                    log_verbosity = .verbose;
                } else if (std.ascii.eqlIgnoreCase(value.text, "terse")) {
                    log_verbosity = .terse;
                } else return error.UnsupportedSqlShape;
            } else if (cursor.matchKeyword("force_quote")) {
                if (force_quote_all or force_quote_columns.len != 0) return error.UnsupportedSqlShape;
                if (cursor.matchToken(.star) != null) {
                    force_quote_all = true;
                } else if (cursor.matchToken(.lparen) != null) {
                    force_quote_columns = try parseIdentifierListAlloc(alloc, tokens, pos);
                    try cursor.expectToken(.rparen);
                } else return error.UnsupportedSqlShape;
            } else if (cursor.matchKeyword("force_not_null")) {
                if (force_not_null_columns.len != 0) return error.UnsupportedSqlShape;
                try cursor.expectToken(.lparen);
                force_not_null_columns = try parseIdentifierListAlloc(alloc, tokens, pos);
                try cursor.expectToken(.rparen);
            } else if (cursor.matchKeyword("force_null")) {
                if (force_null_columns.len != 0) return error.UnsupportedSqlShape;
                try cursor.expectToken(.lparen);
                force_null_columns = try parseIdentifierListAlloc(alloc, tokens, pos);
                try cursor.expectToken(.rparen);
            } else if (cursor.matchKeyword("delimiter")) {
                if (delimiter != null) return error.UnsupportedSqlShape;
                const value = try parseSqlStringLiteralValueAlloc(alloc, cursor);
                if (value.len != 1) {
                    alloc.free(value);
                    return error.UnsupportedSqlShape;
                }
                delimiter = value;
            } else if (cursor.matchKeyword("quote")) {
                if (quote != null) return error.UnsupportedSqlShape;
                const value = try parseSqlStringLiteralValueAlloc(alloc, cursor);
                if (value.len != 1) {
                    alloc.free(value);
                    return error.UnsupportedSqlShape;
                }
                quote = value;
            } else if (cursor.matchKeyword("escape")) {
                if (escape != null) return error.UnsupportedSqlShape;
                const value = try parseSqlStringLiteralValueAlloc(alloc, cursor);
                if (value.len != 1) {
                    alloc.free(value);
                    return error.UnsupportedSqlShape;
                }
                escape = value;
            } else if (cursor.matchKeyword("null")) {
                if (null_marker != null) return error.UnsupportedSqlShape;
                null_marker = try parseSqlStringLiteralValueAlloc(alloc, cursor);
            } else if (cursor.matchKeyword("default")) {
                if (default_marker != null) return error.UnsupportedSqlShape;
                default_marker = try parseSqlStringLiteralValueAlloc(alloc, cursor);
            } else if (cursor.matchKeyword("encoding")) {
                if (encoding != null) return error.UnsupportedSqlShape;
                const value = try parseSqlStringLiteralValueAlloc(alloc, cursor);
                if (value.len == 0) {
                    alloc.free(value);
                    return error.UnsupportedSqlShape;
                }
                encoding = value;
            } else if (cursor.matchKeyword("oids")) {
                const value = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
                if (!std.ascii.eqlIgnoreCase(value.text, "false")) return error.UnsupportedSqlShape;
            } else return error.UnsupportedSqlShape;
            if (cursor.matchToken(.comma) != null) continue;
            break;
        }
        try cursor.expectToken(.rparen);
    }
    if (freeze and direction != .from) return error.UnsupportedSqlShape;
    if (on_error != .stop and direction != .from) return error.UnsupportedSqlShape;
    if (reject_limit != null and direction != .from) return error.UnsupportedSqlShape;
    if (reject_limit != null and on_error != .ignore) return error.UnsupportedSqlShape;
    if (log_verbosity != .default and direction != .from) return error.UnsupportedSqlShape;
    if (log_verbosity != .default and on_error != .ignore) return error.UnsupportedSqlShape;
    if (default_marker != null and direction != .from) return error.UnsupportedSqlShape;
    if ((force_quote_all or force_quote_columns.len != 0) and direction != .to) return error.UnsupportedSqlShape;
    if (force_not_null_columns.len != 0 and direction != .from) return error.UnsupportedSqlShape;
    if (force_null_columns.len != 0 and direction != .from) return error.UnsupportedSqlShape;

    if (!cursor.peekKeyword("where")) try adapterNoopStatementEnd(cursor);
    table_transferred = true;
    endpoint_transferred = true;
    format_transferred = true;
    force_quote_columns_transferred = true;
    force_not_null_columns_transferred = true;
    force_null_columns_transferred = true;
    delimiter_transferred = true;
    quote_transferred = true;
    escape_transferred = true;
    null_marker_transferred = true;
    default_marker_transferred = true;
    encoding_transferred = true;
    return .{
        .direction = direction,
        .table_name = table_name,
        .columns = try columns.toOwnedSlice(alloc),
        .endpoint_kind = endpoint_kind,
        .endpoint = endpoint,
        .format = format,
        .header = header,
        .freeze = freeze,
        .on_error = on_error,
        .reject_limit = reject_limit,
        .log_verbosity = log_verbosity,
        .force_quote_all = force_quote_all,
        .force_quote_columns = force_quote_columns,
        .force_not_null_columns = force_not_null_columns,
        .force_null_columns = force_null_columns,
        .delimiter = delimiter,
        .quote = quote,
        .escape = escape,
        .null_marker = null_marker,
        .default_marker = default_marker,
        .encoding = encoding,
    };
}

fn parseBulkIoRejectLimit(cursor: parser.Cursor) !usize {
    const token = cursor.matchToken(.number) orelse return error.UnsupportedSqlShape;
    if (std.mem.indexOfScalar(u8, token.text, '.') != null) return error.UnsupportedSqlShape;
    const value = std.fmt.parseInt(usize, token.text, 10) catch return error.UnsupportedSqlShape;
    if (value == 0) return error.UnsupportedSqlShape;
    return value;
}

pub fn parseListenNotificationTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !ListenNotificationSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    const channel_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var channel_transferred = false;
    errdefer if (!channel_transferred) alloc.free(channel_name);
    try adapterNoopStatementEnd(cursor);
    channel_transferred = true;
    return .{ .channel_name = channel_name };
}

pub fn parseNotifyNotificationTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !NotifyNotificationSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    const channel_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var channel_transferred = false;
    errdefer if (!channel_transferred) alloc.free(channel_name);
    var payload_json: ?[]const u8 = null;
    errdefer if (payload_json) |payload| alloc.free(@constCast(payload));
    if (cursor.matchToken(.comma) != null) {
        payload_json = try sql_value.parseSqlUntypedValueJsonAlloc(alloc, tokens, pos);
    }
    try adapterNoopStatementEnd(cursor);
    channel_transferred = true;
    const syntax = NotifyNotificationSyntax{
        .channel_name = channel_name,
        .payload_json = payload_json,
    };
    payload_json = null;
    return syntax;
}

pub fn parseUnlistenNotificationTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !UnlistenNotificationSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    if (cursor.matchToken(.star) != null) {
        try adapterNoopStatementEnd(cursor);
        return .{ .all = true };
    }
    const channel_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var channel_transferred = false;
    errdefer if (!channel_transferred) alloc.free(channel_name);
    try adapterNoopStatementEnd(cursor);
    channel_transferred = true;
    return .{ .channel_name = channel_name };
}

pub fn parseTruncateMutationSourceSqlAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !TruncateMutationSourceSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("truncate");
    _ = cursor.matchKeyword("table");
    const table_name = try parseSqlTableReferenceIdentifierOwnedAlloc(alloc, tokens, pos);
    var table_transferred = false;
    errdefer if (!table_transferred) alloc.free(table_name);
    var additional_table_names = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (additional_table_names.items) |name| alloc.free(name);
        additional_table_names.deinit(alloc);
    }
    while (cursor.matchToken(.comma) != null) {
        try additional_table_names.append(alloc, try parseSqlTableReferenceIdentifierOwnedAlloc(alloc, tokens, pos));
    }

    var restart_identity = false;
    if (cursor.matchKeyword("restart")) {
        try cursor.expectKeyword("identity");
        restart_identity = true;
    } else if (cursor.matchKeyword("continue")) {
        try cursor.expectKeyword("identity");
    } else if (cursor.matchKeyword("identity")) {
        return error.UnsupportedSqlShape;
    }
    const cascade = cursor.matchKeyword("cascade");
    if (cascade) {
        if (cursor.matchKeyword("restrict")) return error.UnsupportedSqlShape;
    } else {
        _ = cursor.matchKeyword("restrict");
    }
    try adapterNoopStatementEnd(cursor);

    const owned_additional_table_names = try additional_table_names.toOwnedSlice(alloc);
    table_transferred = true;
    return .{
        .table_name = table_name,
        .additional_table_names = owned_additional_table_names,
        .restart_identity = restart_identity,
        .cascade = cascade,
    };
}

pub fn parseCreatePublicationCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !CreatePublicationSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("publication");
    const publication_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var publication_transferred = false;
    errdefer if (!publication_transferred) alloc.free(publication_name);
    try cursor.expectKeyword("for");
    var all_tables = false;
    var table_names: []const []const u8 = &.{};
    errdefer freeStringSlice(alloc, table_names);
    if (cursor.matchKeyword("all")) {
        try cursor.expectKeyword("tables");
        all_tables = true;
    } else {
        try cursor.expectKeyword("table");
        table_names = try parseSqlObjectIdentifierListAlloc(alloc, tokens, pos);
    }
    if (cursor.peekKeyword("with")) return error.UnsupportedSqlShape;
    try adapterNoopStatementEnd(cursor);
    publication_transferred = true;
    const out = CreatePublicationSyntax{
        .publication_name = publication_name,
        .table_names = table_names,
        .all_tables = all_tables,
    };
    table_names = &.{};
    return out;
}

pub fn parseAlterPublicationCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !AlterPublicationSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("publication");
    const publication_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var publication_transferred = false;
    errdefer if (!publication_transferred) alloc.free(publication_name);
    try cursor.expectKeyword("add");
    try cursor.expectKeyword("table");
    var table_names = try parseSqlObjectIdentifierListAlloc(alloc, tokens, pos);
    errdefer freeStringSlice(alloc, table_names);
    try adapterNoopStatementEnd(cursor);
    publication_transferred = true;
    const out = AlterPublicationSyntax{
        .publication_name = publication_name,
        .table_names = table_names,
    };
    table_names = &.{};
    return out;
}

pub fn parseDropPublicationCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !DropPublicationSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("publication");
    var if_exists = false;
    if (cursor.matchKeyword("if")) {
        try cursor.expectKeyword("exists");
        if_exists = true;
    }
    const publication_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var publication_transferred = false;
    errdefer if (!publication_transferred) alloc.free(publication_name);
    try adapterNoopStatementEnd(cursor);
    publication_transferred = true;
    return .{ .publication_name = publication_name, .if_exists = if_exists };
}

pub fn parseCreateSubscriptionCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !CreateSubscriptionSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("subscription");
    const subscription_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var subscription_transferred = false;
    errdefer if (!subscription_transferred) alloc.free(subscription_name);
    try cursor.expectKeyword("connection");
    const connection_json = try sql_value.parseSqlUntypedValueJsonAlloc(alloc, tokens, pos);
    var connection_transferred = false;
    errdefer if (!connection_transferred) alloc.free(@constCast(connection_json));
    try cursor.expectKeyword("publication");
    var publication_names = try parseIdentifierListAlloc(alloc, tokens, pos);
    errdefer freeStringSlice(alloc, publication_names);
    if (cursor.peekKeyword("with")) return error.UnsupportedSqlShape;
    try adapterNoopStatementEnd(cursor);
    subscription_transferred = true;
    connection_transferred = true;
    const out = CreateSubscriptionSyntax{
        .subscription_name = subscription_name,
        .connection_json = connection_json,
        .publication_names = publication_names,
    };
    publication_names = &.{};
    return out;
}

pub fn parseAlterSubscriptionCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !AlterSubscriptionSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("subscription");
    const subscription_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var subscription_transferred = false;
    errdefer if (!subscription_transferred) alloc.free(subscription_name);
    const enabled = if (cursor.matchKeyword("enable"))
        true
    else if (cursor.matchKeyword("disable"))
        false
    else
        return error.UnsupportedSqlShape;
    try adapterNoopStatementEnd(cursor);
    subscription_transferred = true;
    return .{ .subscription_name = subscription_name, .enabled = enabled };
}

pub fn parseDropSubscriptionCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !DropSubscriptionSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("subscription");
    var if_exists = false;
    if (cursor.matchKeyword("if")) {
        try cursor.expectKeyword("exists");
        if_exists = true;
    }
    const subscription_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var subscription_transferred = false;
    errdefer if (!subscription_transferred) alloc.free(subscription_name);
    try adapterNoopStatementEnd(cursor);
    subscription_transferred = true;
    return .{ .subscription_name = subscription_name, .if_exists = if_exists };
}

pub fn parseCreateCollationCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !CreateCollationSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("collation");
    const collation_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var collation_transferred = false;
    errdefer if (!collation_transferred) alloc.free(collation_name);
    const option_count = try countParenthesizedAssignments(cursor);
    try adapterNoopStatementEnd(cursor);
    collation_transferred = true;
    return .{ .collation_name = collation_name, .option_count = option_count };
}

pub fn parseRenameCollationCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !RenameCollationSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("collation");
    const collation_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var collation_transferred = false;
    errdefer if (!collation_transferred) alloc.free(collation_name);
    try cursor.expectKeyword("rename");
    try cursor.expectKeyword("to");
    const new_collation_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var new_collation_transferred = false;
    errdefer if (!new_collation_transferred) alloc.free(new_collation_name);
    try adapterNoopStatementEnd(cursor);
    collation_transferred = true;
    new_collation_transferred = true;
    return .{ .collation_name = collation_name, .new_collation_name = new_collation_name };
}

pub fn parseDropCollationCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !DropCollationSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("collation");
    var if_exists = false;
    if (cursor.matchKeyword("if")) {
        try cursor.expectKeyword("exists");
        if_exists = true;
    }
    const collation_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var collation_transferred = false;
    errdefer if (!collation_transferred) alloc.free(collation_name);
    try adapterNoopStatementEnd(cursor);
    collation_transferred = true;
    return .{ .collation_name = collation_name, .if_exists = if_exists };
}

pub fn parseCreateOperatorCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !CreateOperatorSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("operator");
    const operator_name = try parseSqlOperatorNameOwnedAlloc(alloc, cursor);
    var operator_transferred = false;
    errdefer if (!operator_transferred) alloc.free(operator_name);
    const option_count = try countParenthesizedAssignments(cursor);
    try adapterNoopStatementEnd(cursor);
    operator_transferred = true;
    return .{ .operator_name = operator_name, .option_count = option_count };
}

pub fn parseDropOperatorCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !DropOperatorSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("operator");
    var if_exists = false;
    if (cursor.matchKeyword("if")) {
        try cursor.expectKeyword("exists");
        if_exists = true;
    }
    const operator_name = try parseSqlOperatorNameOwnedAlloc(alloc, cursor);
    var operator_transferred = false;
    errdefer if (!operator_transferred) alloc.free(operator_name);
    const argument_count = try countParenthesizedTypeList(cursor);
    try adapterNoopStatementEnd(cursor);
    operator_transferred = true;
    return .{ .operator_name = operator_name, .argument_count = argument_count, .if_exists = if_exists };
}

pub fn parseCreateAggregateCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !CreateAggregateSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("aggregate");
    const aggregate_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var aggregate_transferred = false;
    errdefer if (!aggregate_transferred) alloc.free(aggregate_name);
    const argument_count = try countParenthesizedTypeList(cursor);
    const option_count = try countParenthesizedAssignments(cursor);
    try adapterNoopStatementEnd(cursor);
    aggregate_transferred = true;
    return .{ .aggregate_name = aggregate_name, .argument_count = argument_count, .option_count = option_count };
}

pub fn parseDropAggregateCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !DropAggregateSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("aggregate");
    var if_exists = false;
    if (cursor.matchKeyword("if")) {
        try cursor.expectKeyword("exists");
        if_exists = true;
    }
    const aggregate_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var aggregate_transferred = false;
    errdefer if (!aggregate_transferred) alloc.free(aggregate_name);
    const argument_count = try countParenthesizedTypeList(cursor);
    try adapterNoopStatementEnd(cursor);
    aggregate_transferred = true;
    return .{ .aggregate_name = aggregate_name, .argument_count = argument_count, .if_exists = if_exists };
}

pub fn parseCreateCastCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !CreateCastSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("cast");
    try cursor.expectToken(.lparen);
    const source_type = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var source_transferred = false;
    errdefer if (!source_transferred) alloc.free(source_type);
    try cursor.expectKeyword("as");
    const target_type = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var target_transferred = false;
    errdefer if (!target_transferred) alloc.free(target_type);
    try cursor.expectToken(.rparen);
    try cursor.expectKeyword("with");
    try cursor.expectKeyword("function");
    const function_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var function_transferred = false;
    errdefer if (!function_transferred) alloc.free(function_name);
    _ = try countParenthesizedTypeList(cursor);
    const assignment = if (cursor.matchKeyword("as")) blk: {
        if (cursor.matchKeyword("assignment")) break :blk true;
        if (cursor.matchKeyword("implicit")) break :blk false;
        return error.UnsupportedSqlShape;
    } else false;
    try adapterNoopStatementEnd(cursor);
    source_transferred = true;
    target_transferred = true;
    function_transferred = true;
    return .{
        .source_type = source_type,
        .target_type = target_type,
        .function_name = function_name,
        .assignment = assignment,
    };
}

pub fn parseDropCastCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !DropCastSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("cast");
    var if_exists = false;
    if (cursor.matchKeyword("if")) {
        try cursor.expectKeyword("exists");
        if_exists = true;
    }
    try cursor.expectToken(.lparen);
    const source_type = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var source_transferred = false;
    errdefer if (!source_transferred) alloc.free(source_type);
    try cursor.expectKeyword("as");
    const target_type = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var target_transferred = false;
    errdefer if (!target_transferred) alloc.free(target_type);
    try cursor.expectToken(.rparen);
    try adapterNoopStatementEnd(cursor);
    source_transferred = true;
    target_transferred = true;
    return .{ .source_type = source_type, .target_type = target_type, .if_exists = if_exists };
}

pub fn parseRelationLifetimePrefix(tokens: []const Token, pos: *usize) !RelationLifetimePrefixSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    if (cursor.matchKeyword("temporary") or cursor.matchKeyword("temp")) return .{ .kind = .temporary };
    if (cursor.matchKeyword("unlogged")) return .{ .kind = .unlogged };
    return error.UnsupportedSqlShape;
}

pub fn parseSelectSetOperation(tokens: []const Token, pos: *usize) !ast.SelectSetOperation {
    const cursor = parser.Cursor.init(tokens, pos);
    const op: ast.SelectSetOperation = if (cursor.matchKeyword("union")) blk: {
        if (cursor.matchKeyword("all")) break :blk .union_all;
        break :blk .union_distinct;
    } else if (cursor.matchKeyword("intersect"))
        .intersect
    else if (cursor.matchKeyword("except"))
        .except
    else
        return error.UnsupportedSqlShape;
    _ = cursor.matchKeyword("distinct");
    return op;
}

pub fn nextIsSelectSetOperationKeyword(tokens: []const Token, pos: usize) bool {
    return parser.peekKeywordTag(tokens, pos, .@"union") or
        parser.peekKeywordTag(tokens, pos, .intersect) or
        parser.peekKeywordTag(tokens, pos, .except);
}

pub fn peekUpdateJoinedMutationSourceAlias(tokens: []const Token, pos: usize) ?[]const u8 {
    var depth: usize = 0;
    var i = pos;
    while (i < tokens.len) : (i += 1) {
        const token = tokens[i];
        switch (token.kind) {
            .lparen => depth += 1,
            .rparen => {
                if (depth == 0) return null;
                depth -= 1;
            },
            .identifier => {
                if (depth != 0 or !token.matchesKeywordTag(.from)) continue;
                var j = i + 1;
                if (j >= tokens.len or tokens[j].kind != .identifier) return null;
                const table_name = tokens[j].text;
                j += 1;
                if (j < tokens.len and tokens[j].matchesKeywordTag(.as)) {
                    j += 1;
                }
                if (j < tokens.len and tokens[j].kind == .identifier and !lower_expr.sqlJoinedSourceAliasTerminatorToken(tokens[j])) {
                    return tokens[j].text;
                }
                return table_name;
            },
            else => {},
        }
    }
    return null;
}

pub fn peekStaticToJsonbValue(tokens: []const Token, pos: usize) bool {
    if (parser.peekKeyword(tokens, pos, "null") or
        parser.peekKeyword(tokens, pos, "true") or
        parser.peekKeyword(tokens, pos, "false"))
    {
        return true;
    }
    if (pos >= tokens.len) return false;
    if (tokens[pos].kind == .placeholder or tokens[pos].kind == .string or tokens[pos].kind == .number) return true;
    return tokens[pos].kind == .minus and pos + 1 < tokens.len and tokens[pos + 1].kind == .number;
}

pub fn peekArrayTransformSelfAssignment(tokens: []const Token, pos: usize, field: []const u8) bool {
    if (!(parser.peekKeyword(tokens, pos, "array_append") or parser.peekKeyword(tokens, pos, "array_remove"))) return false;
    if (pos + 3 >= tokens.len) return false;
    return tokens[pos + 1].kind == .lparen and
        tokens[pos + 2].kind == .identifier and
        std.mem.eql(u8, tokens[pos + 2].text, field) and
        tokens[pos + 3].kind == .comma;
}

pub fn matchArrayTransformUpdateOp(tokens: []const Token, pos: *usize) ?db_mod.types.TransformOpType {
    if (parser.matchKeyword(tokens, pos, "array_append")) return .push;
    if (parser.matchKeyword(tokens, pos, "array_remove")) return .pull;
    return null;
}

pub fn peekDdlRangeColumnDefinition(tokens: []const Token, pos: usize) bool {
    if (pos + 1 >= tokens.len) return false;
    return tokens[pos].kind == .identifier and
        tokens[pos + 1].kind == .identifier and
        ddl_plan.ddlRangeBoundTypeForName(tokens[pos + 1].text) != null;
}

pub fn nextIsSelectSetResultTailKeyword(tokens: []const Token, pos: usize) bool {
    return parser.peekKeyword(tokens, pos, "order") or
        parser.peekKeyword(tokens, pos, "limit") or
        parser.peekKeyword(tokens, pos, "offset") or
        parser.peekKeyword(tokens, pos, "fetch") or
        (pos < tokens.len and tokens[pos].kind == .semicolon);
}

pub fn normalizeSqlObjectIdentifierAlloc(alloc: std.mem.Allocator, identifier: []const u8) ![]const u8 {
    const dot = std.mem.indexOfScalar(u8, identifier, '.') orelse return try alloc.dupe(u8, identifier);
    if (dot == 0) return error.UnsupportedSqlShape;
    const object_name = identifier[dot + 1 ..];
    if (object_name.len == 0 or std.mem.indexOfScalar(u8, object_name, '.') != null) return error.UnsupportedSqlShape;
    if (!std.ascii.eqlIgnoreCase(identifier[0..dot], "public")) return try alloc.dupe(u8, identifier);
    return try alloc.dupe(u8, object_name);
}

pub fn parseIdentifierOwnedAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) ![]const u8 {
    const token = parser.matchToken(tokens, pos, .identifier) orelse return error.UnsupportedSqlShape;
    return try alloc.dupe(u8, token.text);
}

pub fn parseOptionalProjectionAliasAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !?[]const u8 {
    if (!parser.matchKeyword(tokens, pos, "as")) return null;
    return try parseIdentifierOwnedAlloc(alloc, tokens, pos);
}

pub fn parseProjectionOutputOwnedAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    default_output: []const u8,
) ![]const u8 {
    return (try parseOptionalProjectionAliasAlloc(alloc, tokens, pos)) orelse
        try alloc.dupe(u8, default_output);
}

pub fn parseRequiredAliasAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) ![]const u8 {
    _ = parser.matchKeyword(tokens, pos, "as");
    return try parseIdentifierOwnedAlloc(alloc, tokens, pos);
}

pub fn consumeProjectionAlias(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    field: []const u8,
) !void {
    const alias = (try parseOptionalProjectionAliasAlloc(alloc, tokens, pos)) orelse return;
    defer alloc.free(alias);
    if (!std.mem.eql(u8, alias, field)) return error.UnsupportedSqlShape;
}

pub fn parseSqlStringLiteralValueAlloc(alloc: std.mem.Allocator, cursor: parser.Cursor) ![]const u8 {
    const token = cursor.matchToken(.string) orelse return error.UnsupportedSqlShape;
    return try alloc.dupe(u8, token.text);
}

pub fn parseIdentifierListAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) ![]const []const u8 {
    var out = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (out.items) |item| alloc.free(item);
        out.deinit(alloc);
    }
    while (true) {
        try out.append(alloc, try parseIdentifierOwnedAlloc(alloc, tokens, pos));
        if (parser.matchToken(tokens, pos, .comma) == null) break;
    }
    return try out.toOwnedSlice(alloc);
}

pub fn validateSqlIdentifierListUnique(columns: []const []const u8) !void {
    for (columns, 0..) |lhs, i| {
        for (columns[i + 1 ..]) |rhs| {
            if (std.ascii.eqlIgnoreCase(lhs, rhs)) return error.UnsupportedSqlShape;
        }
    }
}

pub fn validateSqlIdentifierListsDisjoint(left: []const []const u8, right: []const []const u8) !void {
    for (left) |lhs| {
        for (right) |rhs| {
            if (std.ascii.eqlIgnoreCase(lhs, rhs)) return error.UnsupportedSqlShape;
        }
    }
}

pub fn parseOptionalCteColumnAliasesAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) ![]const []const u8 {
    if (parser.matchToken(tokens, pos, .lparen) == null) return &.{};

    var aliases = std.ArrayListUnmanaged([]const u8).empty;
    errdefer freeStringList(alloc, &aliases);
    while (true) {
        const alias = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
        var alias_transferred = false;
        errdefer if (!alias_transferred) alloc.free(@constCast(alias));
        for (aliases.items) |existing| {
            if (std.ascii.eqlIgnoreCase(existing, alias)) return error.UnsupportedSqlShape;
        }
        try aliases.append(alloc, alias);
        alias_transferred = true;
        if (parser.matchToken(tokens, pos, .comma) == null) break;
    }

    try parser.expectToken(tokens, pos, .rparen);
    return try aliases.toOwnedSlice(alloc);
}

fn parseParenthesizedIdentifierListAlloc(
    alloc: std.mem.Allocator,
    cursor: parser.Cursor,
    tokens: []const Token,
    pos: *usize,
) ![]const []const u8 {
    try cursor.expectToken(.lparen);
    const values = try parseIdentifierListAlloc(alloc, tokens, pos);
    errdefer freeStringSlice(alloc, values);
    try cursor.expectToken(.rparen);
    return values;
}

pub fn parseSqlObjectIdentifierOwnedAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) ![]const u8 {
    const token = parser.matchToken(tokens, pos, .identifier) orelse return error.UnsupportedSqlShape;
    return try normalizeSqlObjectIdentifierAlloc(alloc, token.text);
}

pub fn parseSqlObjectIdentifierListAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) ![]const []const u8 {
    var out = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (out.items) |item| alloc.free(item);
        out.deinit(alloc);
    }
    while (true) {
        try out.append(alloc, try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos));
        if (parser.matchToken(tokens, pos, .comma) == null) break;
    }
    return try out.toOwnedSlice(alloc);
}

pub fn parseDdlUniquePredicatesAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) ![]const runtime_schema.UniquePredicate {
    var predicates = std.ArrayListUnmanaged(runtime_schema.UniquePredicate).empty;
    errdefer {
        for (predicates.items) |predicate| freeDdlUniquePredicate(alloc, predicate);
        predicates.deinit(alloc);
    }
    while (true) {
        const atom_start = pos.*;
        var depth: usize = 0;
        while (pos.* < tokens.len) {
            const token = tokens[pos.*];
            if (depth == 0 and token.matchesKeywordTag(.@"and")) break;
            if (depth == 0 and token.kind == .semicolon) break;
            switch (token.kind) {
                .lparen => depth += 1,
                .rparen => {
                    if (depth == 0) return error.UnsupportedSqlShape;
                    depth -= 1;
                },
                else => {},
            }
            pos.* += 1;
        }
        if (depth != 0 or atom_start == pos.*) return error.UnsupportedSqlShape;

        const predicate = try parseDdlUniquePredicateAtomAlloc(alloc, tokens[atom_start..pos.*]);
        var predicate_transferred = false;
        errdefer if (!predicate_transferred) freeDdlUniquePredicate(alloc, predicate);
        try predicates.append(alloc, predicate);
        predicate_transferred = true;
        if (!parser.matchKeywordTag(tokens, pos, .@"and")) break;
    }
    return try predicates.toOwnedSlice(alloc);
}

pub fn uniquePredicateWhereJsonAlloc(
    alloc: std.mem.Allocator,
    predicates: []const runtime_schema.UniquePredicate,
) ![]const u8 {
    if (predicates.len == 0) return "";
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.writeAll("{\"all\":[");
    for (predicates, 0..) |predicate, i| {
        if (i != 0) try writer.writeByte(',');
        try writer.print("{{\"field\":{f},\"op\":{f}", .{
            std.json.fmt(predicate.field, .{}),
            std.json.fmt(lower_expr.uniquePredicateOpToken(predicate.op), .{}),
        });
        if (predicate.value_json) |value_json| {
            try writer.writeAll(",\"value\":");
            try writer.writeAll(value_json);
        }
        try writer.writeByte('}');
    }
    try writer.writeAll("]}");
    return try out.toOwnedSlice();
}

pub fn parseDdlUniquePredicateWhereJsonAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    columns: []const runtime_schema.RelationalColumn,
) ![]const u8 {
    var predicates = std.ArrayListUnmanaged(runtime_schema.UniquePredicate).empty;
    defer {
        for (predicates.items) |predicate| freeDdlUniquePredicate(alloc, predicate);
        predicates.deinit(alloc);
    }
    while (true) {
        const atom_start = pos.*;
        var depth: usize = 0;
        while (pos.* < tokens.len) {
            const token = tokens[pos.*];
            if (depth == 0 and (token.matchesKeywordTag(.@"and") or token.matchesKeywordTag(.do))) break;
            if (depth == 0 and token.kind == .semicolon) break;
            switch (token.kind) {
                .lparen => depth += 1,
                .rparen => {
                    if (depth == 0) return error.UnsupportedSqlShape;
                    depth -= 1;
                },
                else => {},
            }
            pos.* += 1;
        }
        if (depth != 0 or atom_start == pos.*) return error.UnsupportedSqlShape;

        const predicate = try parseDdlUniquePredicateAtomAlloc(alloc, tokens[atom_start..pos.*]);
        var predicate_transferred = false;
        errdefer if (!predicate_transferred) freeDdlUniquePredicate(alloc, predicate);
        try predicates.append(alloc, predicate);
        predicate_transferred = true;
        if (!parser.matchKeywordTag(tokens, pos, .@"and")) break;
    }
    try lower_expr.validateUniquePredicatesForColumns(columns, predicates.items);
    return try uniquePredicateWhereJsonAlloc(alloc, predicates.items);
}

fn parseDdlUniquePredicateAtomAlloc(
    alloc: std.mem.Allocator,
    raw_tokens: []const Token,
) !runtime_schema.UniquePredicate {
    const tokens = parser.stripBalancedOuterParens(raw_tokens);
    if (tokens.len == 0) return error.UnsupportedSqlShape;

    var idx: usize = 0;
    const field_token = try parser.parseWrappedIdentifierOperand(tokens, &idx);
    const field = try alloc.dupe(u8, field_token.text);
    var field_transferred = false;
    errdefer if (!field_transferred) alloc.free(field);

    if (idx >= tokens.len) return error.UnsupportedSqlShape;
    if (tokens[idx].matchesKeywordTag(.is)) {
        idx += 1;
        const op: runtime_schema.UniquePredicateOp = if (idx < tokens.len and tokens[idx].matchesKeywordTag(.not)) blk: {
            idx += 1;
            if (idx >= tokens.len or !tokens[idx].matchesKeywordTag(.null)) return error.UnsupportedSqlShape;
            idx += 1;
            break :blk .is_not_null;
        } else blk: {
            if (idx >= tokens.len or !tokens[idx].matchesKeywordTag(.null)) return error.UnsupportedSqlShape;
            idx += 1;
            break :blk .is_null;
        };
        if (idx != tokens.len) return error.UnsupportedSqlShape;
        field_transferred = true;
        return .{ .field = field, .op = op };
    }

    const op: runtime_schema.UniquePredicateOp = if (tokens[idx].kind == .eq) .eq else if (tokens[idx].kind == .neq) .ne else return error.UnsupportedSqlShape;
    idx += 1;
    if (idx >= tokens.len) return error.UnsupportedSqlShape;
    const value_json = try sql_value.parseSqlUntypedValueJsonAlloc(alloc, tokens, &idx);
    var value_transferred = false;
    errdefer if (!value_transferred) alloc.free(value_json);
    if (idx != tokens.len) return error.UnsupportedSqlShape;

    field_transferred = true;
    value_transferred = true;
    return .{ .field = field, .op = op, .value_json = value_json };
}

fn freeDdlUniquePredicate(alloc: std.mem.Allocator, predicate: runtime_schema.UniquePredicate) void {
    alloc.free(predicate.field);
    if (predicate.value_json) |value| alloc.free(value);
}

pub fn parseSqlTableReferenceIdentifierOwnedAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) ![]const u8 {
    _ = parser.matchKeyword(tokens, pos, "only");
    return try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
}

fn parseCreateSequenceOption(
    alloc: std.mem.Allocator,
    cursor: parser.Cursor,
    tokens: []const Token,
    pos: *usize,
    options: *ddl_plan.SequenceOptions,
) !void {
    if (cursor.matchKeyword("as")) {
        if (options.as_type != null) return error.UnsupportedSqlShape;
        options.as_type = try parseSequenceTypeNameOwnedAlloc(alloc, cursor);
    } else if (cursor.matchKeyword("owned")) {
        try cursor.expectKeyword("by");
        if (options.owned_by != null) return error.UnsupportedSqlShape;
        options.owned_by = try parseSequenceOwnedByAlloc(alloc, tokens, pos);
    } else if (cursor.matchKeyword("start")) {
        _ = cursor.matchKeyword("with");
        if (options.start_with != null) return error.UnsupportedSqlShape;
        options.start_with = try parseSequenceInteger(cursor);
    } else if (cursor.matchKeyword("increment")) {
        _ = cursor.matchKeyword("by");
        if (options.increment_by != null) return error.UnsupportedSqlShape;
        options.increment_by = try parseSequenceInteger(cursor);
    } else if (cursor.matchKeyword("minvalue")) {
        if (options.min_value_specified) return error.UnsupportedSqlShape;
        options.min_value_specified = true;
        options.min_value = try parseSequenceInteger(cursor);
    } else if (cursor.matchKeyword("maxvalue")) {
        if (options.max_value_specified) return error.UnsupportedSqlShape;
        options.max_value_specified = true;
        options.max_value = try parseSequenceInteger(cursor);
    } else if (cursor.matchKeyword("cache")) {
        if (options.cache != null) return error.UnsupportedSqlShape;
        options.cache = try parseSequenceInteger(cursor);
    } else if (cursor.matchKeyword("cycle")) {
        if (options.cycle != null) return error.UnsupportedSqlShape;
        options.cycle = true;
    } else if (cursor.matchKeyword("no")) {
        if (cursor.matchKeyword("minvalue")) {
            if (options.min_value_specified) return error.UnsupportedSqlShape;
            options.min_value_specified = true;
            options.min_value = null;
        } else if (cursor.matchKeyword("maxvalue")) {
            if (options.max_value_specified) return error.UnsupportedSqlShape;
            options.max_value_specified = true;
            options.max_value = null;
        } else if (cursor.matchKeyword("cycle")) {
            if (options.cycle != null) return error.UnsupportedSqlShape;
            options.cycle = false;
        } else {
            return error.UnsupportedSqlShape;
        }
    } else {
        return error.UnsupportedSqlShape;
    }
}

fn parseAlterSequenceOperation(
    alloc: std.mem.Allocator,
    cursor: parser.Cursor,
    tokens: []const Token,
    pos: *usize,
    operations: *std.ArrayListUnmanaged(ddl_plan.SequenceAlterOperation),
) !void {
    if (cursor.matchKeyword("as")) {
        const type_name = try parseSequenceTypeNameOwnedAlloc(alloc, cursor);
        errdefer alloc.free(type_name);
        try operations.append(alloc, .{ .set_type = type_name });
    } else if (cursor.matchKeyword("owned")) {
        try cursor.expectKeyword("by");
        var owned_by = try parseSequenceOwnedByAlloc(alloc, tokens, pos);
        errdefer owned_by.deinit(alloc);
        try operations.append(alloc, .{ .set_owned_by = owned_by });
    } else if (cursor.matchKeyword("restart")) {
        const value = if (cursor.matchKeyword("with")) try parseSequenceInteger(cursor) else null;
        try operations.append(alloc, .{ .restart = value });
    } else if (cursor.matchKeyword("start")) {
        _ = cursor.matchKeyword("with");
        try operations.append(alloc, .{ .set_start = try parseSequenceInteger(cursor) });
    } else if (cursor.matchKeyword("increment")) {
        _ = cursor.matchKeyword("by");
        try operations.append(alloc, .{ .set_increment = try parseSequenceInteger(cursor) });
    } else if (cursor.matchKeyword("minvalue")) {
        try operations.append(alloc, .{ .set_min = try parseSequenceInteger(cursor) });
    } else if (cursor.matchKeyword("maxvalue")) {
        try operations.append(alloc, .{ .set_max = try parseSequenceInteger(cursor) });
    } else if (cursor.matchKeyword("cache")) {
        try operations.append(alloc, .{ .set_cache = try parseSequenceInteger(cursor) });
    } else if (cursor.matchKeyword("cycle")) {
        try operations.append(alloc, .{ .set_cycle = true });
    } else if (cursor.matchKeyword("no")) {
        if (cursor.matchKeyword("minvalue")) {
            try operations.append(alloc, .{ .set_min = null });
        } else if (cursor.matchKeyword("maxvalue")) {
            try operations.append(alloc, .{ .set_max = null });
        } else if (cursor.matchKeyword("cycle")) {
            try operations.append(alloc, .{ .set_cycle = false });
        } else {
            return error.UnsupportedSqlShape;
        }
    } else {
        return error.UnsupportedSqlShape;
    }
}

fn parseSequenceTypeNameOwnedAlloc(alloc: std.mem.Allocator, cursor: parser.Cursor) ![]const u8 {
    const token = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
    if (std.ascii.eqlIgnoreCase(token.text, "smallint")) return try alloc.dupe(u8, "smallint");
    if (std.ascii.eqlIgnoreCase(token.text, "integer") or std.ascii.eqlIgnoreCase(token.text, "int")) return try alloc.dupe(u8, "integer");
    if (std.ascii.eqlIgnoreCase(token.text, "bigint")) return try alloc.dupe(u8, "bigint");
    return error.UnsupportedSqlShape;
}

fn parseSequenceOwnedByAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !ddl_plan.SequenceOwnedBy {
    const cursor = parser.Cursor.init(tokens, pos);
    if (cursor.matchKeyword("none")) return .{};
    const token = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
    const raw = token.text;
    const first_dot = std.mem.indexOfScalar(u8, raw, '.') orelse return error.UnsupportedSqlShape;
    const table_start = if (std.ascii.eqlIgnoreCase(raw[0..first_dot], "public")) first_dot + 1 else 0;
    const table_and_column = raw[table_start..];
    const dot = std.mem.indexOfScalar(u8, table_and_column, '.') orelse return error.UnsupportedSqlShape;
    if (std.mem.indexOfScalar(u8, table_and_column[dot + 1 ..], '.') != null) return error.UnsupportedSqlShape;
    const table_name = table_and_column[0..dot];
    const column_name = table_and_column[dot + 1 ..];
    if (table_name.len == 0 or column_name.len == 0) return error.UnsupportedSqlShape;
    const owned_table_name = try alloc.dupe(u8, table_name);
    errdefer alloc.free(owned_table_name);
    return .{
        .table_name = owned_table_name,
        .column_name = try alloc.dupe(u8, column_name),
    };
}

fn parseSequenceInteger(cursor: parser.Cursor) !i64 {
    const negative = cursor.matchToken(.minus) != null;
    const token = cursor.matchToken(.number) orelse return error.UnsupportedSqlShape;
    if (std.mem.indexOfScalar(u8, token.text, '.') != null) return error.UnsupportedSqlShape;
    const value = std.fmt.parseInt(i64, token.text, 10) catch return error.UnsupportedSqlShape;
    return if (negative) -value else value;
}

fn parseEnumLabelOwnedAlloc(alloc: std.mem.Allocator, cursor: parser.Cursor) ![]const u8 {
    const token = cursor.matchToken(.string) orelse return error.UnsupportedSqlShape;
    if (token.text.len == 0) return error.UnsupportedSqlShape;
    return try alloc.dupe(u8, token.text);
}

fn parsePrivilegeListAlloc(
    alloc: std.mem.Allocator,
    cursor: parser.Cursor,
    tokens: []const Token,
    pos: *usize,
) ![]const []const u8 {
    var privileges = std.ArrayListUnmanaged([]const u8).empty;
    errdefer freeStringList(alloc, &privileges);
    while (true) {
        if (cursor.peekKeyword("on")) break;
        const privilege = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
        var privilege_transferred = false;
        errdefer if (!privilege_transferred) alloc.free(privilege);
        try privileges.append(alloc, privilege);
        privilege_transferred = true;
        if (std.ascii.eqlIgnoreCase(privilege, "all")) _ = cursor.matchKeyword("privileges");
        if (cursor.matchToken(.comma) != null) continue;
        break;
    }
    if (privileges.items.len == 0) return error.UnsupportedSqlShape;
    return try privileges.toOwnedSlice(alloc);
}

fn parseAdapterNoopPublicSearchPathTail(cursor: parser.Cursor) !void {
    if (cursor.matchToken(.eq) == null and !cursor.matchKeyword("to")) return error.UnsupportedSqlShape;
    const path = cursor.matchToken(.identifier) orelse cursor.matchToken(.string) orelse return error.UnsupportedSqlShape;
    if (!std.ascii.eqlIgnoreCase(path.text, "public")) return error.UnsupportedSqlShape;
    try adapterNoopStatementEnd(cursor);
}

fn parseAdapterNoopSetValueTail(cursor: parser.Cursor, setting: []const u8) !void {
    const value = cursor.matchToken(.identifier) orelse cursor.matchToken(.string) orelse cursor.matchToken(.number) orelse return error.UnsupportedSqlShape;
    if (!adapterNoopSetSessionSettingValueAllowed(setting, value.text)) return error.UnsupportedSqlShape;
    try adapterNoopStatementEnd(cursor);
}

pub fn parseAdapterNoopStatementEnd(tokens: []const Token, pos: *usize) !void {
    try adapterNoopStatementEnd(parser.Cursor.init(tokens, pos));
}

fn adapterNoopStatementEnd(cursor: parser.Cursor) !void {
    if (!cursor.atEnd() and !cursor.peekKind(.semicolon)) return error.UnsupportedSqlShape;
    if (cursor.matchToken(.semicolon) != null and !cursor.atEnd()) return error.UnsupportedSqlShape;
    if (!cursor.atEnd()) return error.UnsupportedSqlShape;
}

fn matchAdapterNoopStatementEnd(cursor: parser.Cursor) !bool {
    if (cursor.matchToken(.semicolon) != null) {
        if (!cursor.atEnd()) return error.UnsupportedSqlShape;
        return true;
    }
    return cursor.atEnd();
}

fn parseSavepointNameTail(tokens: []const Token, pos: *usize) !SavepointNameSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    return try parseSavepointNameTailFromCursor(cursor);
}

fn parseSavepointNameTailFromCursor(cursor: parser.Cursor) !SavepointNameSyntax {
    const name = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
    try adapterNoopStatementEnd(cursor);
    return .{ .savepoint_name = name.text };
}

fn parseNamedOrAllTail(cursor: parser.Cursor) !NamedOrAllSyntax {
    if (cursor.matchKeyword("all")) {
        try adapterNoopStatementEnd(cursor);
        return .{ .all = true };
    }
    const name = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
    try adapterNoopStatementEnd(cursor);
    return .{ .name = name.text };
}

fn parseTableLockMode(cursor: parser.Cursor) !TableLockModeSyntax {
    if (cursor.matchKeyword("access")) {
        if (cursor.matchKeyword("share")) return .access_share;
        if (cursor.matchKeyword("exclusive")) return .access_exclusive;
        return error.UnsupportedSqlShape;
    }
    if (cursor.matchKeyword("row")) {
        if (cursor.matchKeyword("share")) return .row_share;
        if (cursor.matchKeyword("exclusive")) return .row_exclusive;
        return error.UnsupportedSqlShape;
    }
    if (cursor.matchKeyword("share")) {
        if (cursor.matchKeyword("update")) {
            try cursor.expectKeyword("exclusive");
            return .share_update_exclusive;
        }
        if (cursor.matchKeyword("row")) {
            try cursor.expectKeyword("exclusive");
            return .share_row_exclusive;
        }
        return .share;
    }
    if (cursor.matchKeyword("exclusive")) return .exclusive;
    return error.UnsupportedSqlShape;
}

fn parseTransactionIsolationLevel(cursor: parser.Cursor) !TransactionIsolationLevelSyntax {
    if (cursor.matchKeyword("serializable")) return .serializable;
    if (cursor.matchKeyword("repeatable")) {
        try cursor.expectKeyword("read");
        return .repeatable_read;
    }
    if (cursor.matchKeyword("read")) {
        if (cursor.matchKeyword("committed")) return .read_committed;
        if (cursor.matchKeyword("uncommitted")) return .read_uncommitted;
    }
    return error.UnsupportedSqlShape;
}

fn parseOptionalVacuumMaintenanceOption(
    cursor: parser.Cursor,
    full: *bool,
    freeze: *bool,
    verbose: *bool,
    analyze: *bool,
) bool {
    parseVacuumMaintenanceOption(cursor, full, freeze, verbose, analyze) catch return false;
    return true;
}

fn parseVacuumMaintenanceOption(
    cursor: parser.Cursor,
    full: *bool,
    freeze: *bool,
    verbose: *bool,
    analyze: *bool,
) !void {
    if (cursor.matchKeyword("full")) {
        full.* = true;
    } else if (cursor.matchKeyword("freeze")) {
        freeze.* = true;
    } else if (cursor.matchKeyword("verbose")) {
        verbose.* = true;
    } else if (cursor.matchKeyword("analyze")) {
        analyze.* = true;
    } else {
        return error.UnsupportedSqlShape;
    }
}

fn parseOptionalCursorFetchCount(cursor: parser.Cursor) !?i64 {
    if (cursor.peekKeyword("from") or cursor.peekKeyword("in")) return null;
    if (cursor.matchKeyword("all")) return null;
    if (!peekCursorFetchCount(cursor)) return null;
    return try parseCursorFetchCount(cursor);
}

fn peekCursorFetchCount(cursor: parser.Cursor) bool {
    if (cursor.peekKind(.number)) return true;
    const checkpoint = cursor.checkpoint();
    defer cursor.restore(checkpoint);
    if (cursor.matchToken(.minus) == null) return false;
    return cursor.peekKind(.number);
}

fn parseCursorFetchCount(cursor: parser.Cursor) !i64 {
    const negative = cursor.matchToken(.minus) != null;
    const count_token = cursor.matchToken(.number) orelse return error.UnsupportedSqlShape;
    var count = std.fmt.parseInt(i64, count_token.text, 10) catch return error.UnsupportedSqlShape;
    if (negative) count = -count;
    return count;
}

fn countParenthesizedTypeList(cursor: parser.Cursor) !usize {
    try cursor.expectToken(.lparen);
    if (cursor.matchToken(.rparen) != null) return 0;
    var count: usize = 0;
    while (true) {
        _ = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
        count += 1;
        if (cursor.matchToken(.comma) == null) break;
    }
    try cursor.expectToken(.rparen);
    return count;
}

fn parseRoutineKindKeyword(cursor: parser.Cursor) !RoutineKindSyntax {
    if (cursor.matchKeyword("function")) return .function;
    try cursor.expectKeyword("procedure");
    return .procedure;
}

fn parseCreateRoutineSignatureAlloc(alloc: std.mem.Allocator, cursor: parser.Cursor) !RoutineSignatureSyntax {
    try cursor.expectToken(.lparen);
    if (cursor.matchToken(.rparen) != null) return .{};

    var names = std.ArrayListUnmanaged([]const u8).empty;
    var names_transferred = false;
    errdefer if (!names_transferred) freeRoutineSignatureNameList(alloc, &names);

    while (true) {
        const start = cursor.pos.*;
        var saw_argument_token = false;
        while (!cursor.peekKind(.comma) and !cursor.peekKind(.rparen)) {
            if (cursor.atEnd() or cursor.peekKind(.semicolon)) return error.UnsupportedSqlShape;
            try cursor.advance(1);
            saw_argument_token = true;
        }
        if (!saw_argument_token) return error.UnsupportedSqlShape;
        const argument_tokens = cursor.tokens[start..cursor.pos.*];
        const argument_name = try routineSignatureArgumentNameAlloc(alloc, argument_tokens);
        var argument_name_transferred = false;
        errdefer if (!argument_name_transferred and argument_name.len > 0) alloc.free(@constCast(argument_name));
        try names.append(alloc, argument_name);
        argument_name_transferred = true;
        if (cursor.matchToken(.comma) != null) continue;
        try cursor.expectToken(.rparen);
        const owned_names = try names.toOwnedSlice(alloc);
        names_transferred = true;
        return .{
            .argument_count = owned_names.len,
            .argument_names = owned_names,
        };
    }
}

fn freeRoutineSignatureNameList(alloc: std.mem.Allocator, list: *std.ArrayListUnmanaged([]const u8)) void {
    for (list.items) |name| {
        if (name.len > 0) alloc.free(@constCast(name));
    }
    list.deinit(alloc);
}

fn routineSignatureArgumentNameAlloc(alloc: std.mem.Allocator, tokens: []const Token) ![]const u8 {
    if (tokens.len < 2) return "";
    const first = tokens[0];
    if (first.kind != .identifier) return "";
    if (routineSignatureLeadingMode(first.text)) return "";
    if (routineSignatureTypeStartsWith(first.text)) return "";
    if (!routineSignatureHasTypeAfterName(tokens[1..])) return "";
    return try alloc.dupe(u8, first.text);
}

fn routineSignatureLeadingMode(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "in") or
        std.ascii.eqlIgnoreCase(text, "out") or
        std.ascii.eqlIgnoreCase(text, "inout") or
        std.ascii.eqlIgnoreCase(text, "variadic");
}

fn routineSignatureTypeStartsWith(text: []const u8) bool {
    const known_type_heads = [_][]const u8{
        "bigint",
        "bool",
        "boolean",
        "character",
        "date",
        "double",
        "integer",
        "int",
        "json",
        "jsonb",
        "numeric",
        "real",
        "smallint",
        "text",
        "time",
        "timestamp",
        "timestamptz",
        "uuid",
        "varchar",
    };
    for (known_type_heads) |known| {
        if (std.ascii.eqlIgnoreCase(text, known)) return true;
    }
    return false;
}

fn routineSignatureHasTypeAfterName(tokens: []const Token) bool {
    for (tokens) |token| {
        switch (token.kind) {
            .identifier => return true,
            .string, .number, .placeholder => return false,
            else => {},
        }
    }
    return false;
}

fn parseRoutineSignatureArgumentCount(cursor: parser.Cursor) !usize {
    try cursor.expectToken(.lparen);
    if (cursor.matchToken(.rparen) != null) return 0;
    var count: usize = 0;
    while (true) {
        var saw_argument_token = false;
        while (!cursor.peekKind(.comma) and !cursor.peekKind(.rparen)) {
            if (cursor.atEnd() or cursor.peekKind(.semicolon)) return error.UnsupportedSqlShape;
            try cursor.advance(1);
            saw_argument_token = true;
        }
        if (!saw_argument_token) return error.UnsupportedSqlShape;
        count += 1;
        if (cursor.matchToken(.comma) != null) continue;
        try cursor.expectToken(.rparen);
        return count;
    }
}

fn parseSqlOperatorNameOwnedAlloc(alloc: std.mem.Allocator, cursor: parser.Cursor) ![]const u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    while (!cursor.atEnd() and !cursor.peekKind(.lparen)) {
        const token = cursor.tokens[cursor.pos.*];
        switch (token.kind) {
            .identifier,
            .eq,
            .neq,
            .gt,
            .gte,
            .lt,
            .lte,
            .plus,
            .minus,
            .star,
            .slash,
            .percent,
            .at_contains,
            .range_overlap,
            .pipe_concat,
            .question,
            .question_any,
            .question_all,
            => {
                try out.appendSlice(alloc, token.text);
                cursor.pos.* += 1;
            },
            else => return error.UnsupportedSqlShape,
        }
    }
    if (out.items.len == 0) return error.UnsupportedSqlShape;
    return try out.toOwnedSlice(alloc);
}

fn countParenthesizedAssignments(cursor: parser.Cursor) !usize {
    try cursor.expectToken(.lparen);
    var depth: usize = 1;
    var count: usize = 0;
    var expect_assignment_name = true;
    while (!cursor.atEnd() and depth > 0) {
        if (cursor.matchToken(.lparen) != null) {
            depth += 1;
            continue;
        }
        if (cursor.matchToken(.rparen) != null) {
            depth -= 1;
            continue;
        }
        if (depth == 1 and expect_assignment_name) {
            _ = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
            try cursor.expectToken(.eq);
            count += 1;
            expect_assignment_name = false;
            continue;
        }
        if (depth == 1 and cursor.matchToken(.comma) != null) {
            expect_assignment_name = true;
            continue;
        }
        try cursor.advance(1);
    }
    if (depth != 0 or expect_assignment_name) return error.UnsupportedSqlShape;
    return count;
}

fn countParenthesizedUntypedValues(cursor: parser.Cursor) !usize {
    if (cursor.matchToken(.lparen) == null) return 0;
    if (cursor.matchToken(.rparen) != null) return 0;
    var count: usize = 0;
    while (true) {
        try parseUntypedValue(cursor);
        count += 1;
        if (cursor.matchToken(.comma) == null) break;
    }
    try cursor.expectToken(.rparen);
    return count;
}

fn parseUntypedValue(cursor: parser.Cursor) !void {
    if (cursor.matchKeyword("true")) return;
    if (cursor.matchKeyword("false")) return;
    if (cursor.matchKeyword("null")) return;
    if (cursor.matchToken(.string) != null) return;
    if (cursor.matchToken(.number) != null) return;
    if (cursor.matchToken(.minus) != null) {
        try cursor.expectToken(.number);
        return;
    }
    return error.UnsupportedSqlShape;
}

fn consumePreparedStatementSubjectTail(cursor: parser.Cursor) !void {
    while (!cursor.atEnd()) {
        if (cursor.matchToken(.semicolon) != null) {
            if (!cursor.atEnd()) return error.UnsupportedSqlShape;
            return;
        }
        try cursor.advance(1);
    }
}

fn generatedPreparedStatementSubjectKindAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
) !PreparedStatementSubjectSyntax {
    return preparedStatementSubjectKindFromStatementKind(try generatedPreparedStatementStatementKindAlloc(alloc, tokens));
}

fn generatedPreparedStatementStatementKindAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
) !PreparedStatementStatementSyntax {
    var parsed = try generated_parser.parseTokensAlloc(alloc, tokens);
    defer parsed.deinit(alloc);
    return switch (parsed.ast orelse return error.UnsupportedSqlShape) {
        .read => .read,
        .dml => |dml| generatedPreparedStatementStatementKindFromDml(dml.kind),
        .ddl => .ddl,
        .extension_index => .ddl,
        else => error.UnsupportedSqlShape,
    };
}

fn generatedPreparedStatementStatementKindFromDml(kind: generated_parser.GeneratedSqlDmlKind) PreparedStatementStatementSyntax {
    return switch (kind) {
        .insert_values => .insert,
        .insert_select => .insert_source,
        .update => .update,
        .delete => .delete,
        .truncate => .truncate,
        .merge => .merge,
    };
}

fn preparedStatementSubjectKindFromStatementKind(kind: PreparedStatementStatementSyntax) PreparedStatementSubjectSyntax {
    return switch (kind) {
        .read => .read,
        .insert,
        .insert_source,
        .update,
        .delete,
        .truncate,
        .merge,
        => .write,
        .ddl => .ddl,
    };
}

fn freeStringList(alloc: std.mem.Allocator, list: *std.ArrayListUnmanaged([]const u8)) void {
    for (list.items) |value| alloc.free(@constCast(value));
    list.deinit(alloc);
}

fn freeStringSlice(alloc: std.mem.Allocator, values: []const []const u8) void {
    for (values) |value| alloc.free(@constCast(value));
    if (values.len > 0) alloc.free(values);
}

fn freeRoutineSettingList(alloc: std.mem.Allocator, list: *std.ArrayListUnmanaged(ddl_plan.RoutineSetting)) void {
    for (list.items) |setting_value| {
        var setting = setting_value;
        setting.deinit(alloc);
    }
    list.deinit(alloc);
}

fn freeRoutineSettingSlice(alloc: std.mem.Allocator, values: []const ddl_plan.RoutineSetting) void {
    for (values) |setting_value| {
        var setting = setting_value;
        setting.deinit(alloc);
    }
    if (values.len > 0) alloc.free(@constCast(values));
}

fn freeDdlGeneratedValue(alloc: std.mem.Allocator, generated: runtime_schema.RelationalGeneratedValue) void {
    if (generated.field) |field| alloc.free(@constCast(field));
    freeStringSlice(alloc, generated.fields);
    alloc.free(@constCast(generated.separator));
    if (generated.expression) |expression| runtime_schema.freeRelationalRowsExpression(alloc, expression);
}

pub fn parseRelationPopulationParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
) !RelationPopulationSyntax {
    const raw = parsed_sql.raw_statement;
    return try parseRelationPopulationTokensAlloc(
        alloc,
        parsed_sql.sql(),
        parsed_sql.items()[raw.token_start..raw.token_end],
    );
}

pub fn parseRelationPopulationTokensAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    tokens: []const Token,
) !RelationPopulationSyntax {
    _ = alloc;
    _ = sql;
    if (tokens.len == 0 or tokens[0].kind != .identifier) return error.UnsupportedSqlShape;
    if (tokens[0].matchesKeywordTag(.select)) {
        return try parseSelectIntoPopulationSqlAlloc(tokens);
    }
    if (tokens[0].matchesKeywordTag(.create)) {
        return try parseCreateTableAsPopulationSqlAlloc(tokens);
    }
    return error.UnsupportedSqlShape;
}

fn parseSelectIntoPopulationSqlAlloc(tokens: []const Token) !RelationPopulationSyntax {
    const into_relative = parser.findTopLevelKeyword(tokens[1..], "into") orelse return error.UnsupportedSqlShape;
    const into_index = 1 + into_relative;
    var target_index = into_index + 1;
    const target_lifetime: ?RelationLifetimeKind = if (parser.matchKeyword(tokens, &target_index, "temporary") or parser.matchKeyword(tokens, &target_index, "temp"))
        .temporary
    else if (parser.matchKeyword(tokens, &target_index, "unlogged"))
        .unlogged
    else
        null;
    _ = parser.matchKeyword(tokens, &target_index, "table");
    if (target_index >= tokens.len or tokens[target_index].kind != .identifier) return error.UnsupportedSqlShape;

    const from_relative = parser.findTopLevelKeyword(tokens[target_index + 1 ..], "from") orelse return error.UnsupportedSqlShape;
    const from_index = target_index + 1 + from_relative;
    if (from_index != target_index + 1) return error.UnsupportedSqlShape;

    var source_token_end = tokens.len;
    while (source_token_end > from_index and tokens[source_token_end - 1].kind == .semicolon) source_token_end -= 1;
    if (source_token_end <= from_index) return error.UnsupportedSqlShape;

    return .{
        .mode = .select_into,
        .target_identifier = tokens[target_index].text,
        .target_lifetime = target_lifetime,
        .if_not_exists = false,
        .populate = true,
        .source_token_start = 0,
        .source_token_end = into_index,
        .source_suffix_token_start = from_index,
        .source_suffix_token_end = source_token_end,
    };
}

const RelationPopulationDataClause = struct {
    start_index: usize,
    populate: bool,
};

fn parseTrailingRelationPopulationDataClause(tokens: []const Token, select_index: usize) ?RelationPopulationDataClause {
    var end = tokens.len;
    if (end > select_index and tokens[end - 1].kind == .semicolon) end -= 1;
    if (end < select_index + 2) return null;

    const with_data_index = end - 2;
    if (tokens[with_data_index].matchesKeywordTag(.with) and
        tokens[with_data_index + 1].matchesKeywordTag(.data) and
        topLevelTokenAt(tokens, select_index, with_data_index))
    {
        return .{ .start_index = with_data_index, .populate = true };
    }

    if (end < select_index + 3) return null;
    const with_no_data_index = end - 3;
    if (tokens[with_no_data_index].matchesKeywordTag(.with) and
        tokens[with_no_data_index + 1].matchesKeywordTag(.no) and
        tokens[with_no_data_index + 2].matchesKeywordTag(.data) and
        topLevelTokenAt(tokens, select_index, with_no_data_index))
    {
        return .{ .start_index = with_no_data_index, .populate = false };
    }

    return null;
}

fn topLevelTokenAt(tokens: []const Token, start_index: usize, token_index: usize) bool {
    var depth: usize = 0;
    var index = start_index;
    while (index < token_index) : (index += 1) {
        switch (tokens[index].kind) {
            .lparen => depth += 1,
            .rparen => {
                if (depth == 0) return false;
                depth -= 1;
            },
            else => {},
        }
    }
    return depth == 0;
}

fn parseCreateTableAsPopulationSqlAlloc(tokens: []const Token) !RelationPopulationSyntax {
    var index: usize = 1;
    const target_lifetime: ?RelationLifetimeKind = if (parser.matchKeyword(tokens, &index, "temporary") or parser.matchKeyword(tokens, &index, "temp"))
        .temporary
    else if (parser.matchKeyword(tokens, &index, "unlogged"))
        .unlogged
    else
        null;
    if (!parser.matchKeyword(tokens, &index, "table")) return error.UnsupportedSqlShape;
    var if_not_exists = false;
    if (parser.matchKeyword(tokens, &index, "if")) {
        try parser.expectKeyword(tokens, &index, "not");
        try parser.expectKeyword(tokens, &index, "exists");
        if_not_exists = true;
    }
    if (index >= tokens.len or tokens[index].kind != .identifier) return error.UnsupportedSqlShape;
    const target_identifier = tokens[index].text;
    index += 1;
    if (!parser.matchKeyword(tokens, &index, "as")) return error.UnsupportedSqlShape;
    if (index >= tokens.len or !tokens[index].matchesKeywordTag(.select)) return error.UnsupportedSqlShape;
    const data_clause = parseTrailingRelationPopulationDataClause(tokens, index);
    var source_token_end = if (data_clause) |clause| clause.start_index else tokens.len;
    while (source_token_end > index and tokens[source_token_end - 1].kind == .semicolon) source_token_end -= 1;
    return .{
        .mode = .create_table_as,
        .target_identifier = target_identifier,
        .target_lifetime = target_lifetime,
        .if_not_exists = if_not_exists,
        .populate = if (data_clause) |clause| clause.populate else true,
        .source_token_start = index,
        .source_token_end = source_token_end,
    };
}

fn adapterNoopSetSessionSettingAllowed(setting: []const u8) bool {
    return std.ascii.eqlIgnoreCase(setting, "client_encoding") or
        std.ascii.eqlIgnoreCase(setting, "standard_conforming_strings") or
        std.ascii.eqlIgnoreCase(setting, "check_function_bodies") or
        std.ascii.eqlIgnoreCase(setting, "xmloption") or
        std.ascii.eqlIgnoreCase(setting, "client_min_messages");
}

fn sessionSettingKindForName(setting: []const u8) ?ddl_plan.SessionSettingKind {
    if (std.mem.startsWith(u8, setting, "app.") and setting.len > "app.".len) return .app;
    if (std.ascii.eqlIgnoreCase(setting, "antfly.sync_level")) return .antfly;
    if (std.ascii.eqlIgnoreCase(setting, "statement_timeout") or
        std.ascii.eqlIgnoreCase(setting, "timezone") or
        std.ascii.eqlIgnoreCase(setting, "default_transaction_read_only") or
        std.ascii.eqlIgnoreCase(setting, "transaction_read_only"))
    {
        return .runtime;
    }
    return null;
}

fn validateSetSessionSettingValue(setting: []const u8, kind: ddl_plan.SessionSettingKind, value: []const u8) !void {
    switch (kind) {
        .app => {
            if (value.len == 0) return error.UnsupportedSqlShape;
        },
        .antfly => {
            if (!std.ascii.eqlIgnoreCase(setting, "antfly.sync_level")) return error.UnsupportedSqlShape;
            _ = db_mod.types.parsePublicSyncLevelText(value) orelse return error.UnsupportedSqlShape;
        },
        .runtime => {},
    }
}

fn sqlSessionNamespaceNameValid(name: []const u8) bool {
    if (name.len == 0) return false;
    const first = name[0];
    if (!(std.ascii.isAlphabetic(first) or first == '_')) return false;
    for (name[1..]) |ch| {
        if (!(std.ascii.isAlphanumeric(ch) or ch == '_')) return false;
    }
    return true;
}

fn adapterNoopResetSessionSettingAllowed(setting: []const u8) bool {
    return adapterNoopSetSessionSettingAllowed(setting);
}

fn adapterNoopShowSessionSettingAllowed(setting: []const u8) bool {
    return adapterNoopSetSessionSettingAllowed(setting) or std.ascii.eqlIgnoreCase(setting, "search_path");
}

fn adapterNoopSetSessionSettingValueAllowed(setting: []const u8, value: []const u8) bool {
    if (std.ascii.eqlIgnoreCase(setting, "client_encoding")) {
        return std.ascii.eqlIgnoreCase(value, "UTF8") or std.ascii.eqlIgnoreCase(value, "UTF-8");
    }
    if (std.ascii.eqlIgnoreCase(setting, "standard_conforming_strings")) {
        return std.ascii.eqlIgnoreCase(value, "on") or std.ascii.eqlIgnoreCase(value, "true");
    }
    if (std.ascii.eqlIgnoreCase(setting, "check_function_bodies")) {
        return std.ascii.eqlIgnoreCase(value, "off") or std.ascii.eqlIgnoreCase(value, "false");
    }
    if (std.ascii.eqlIgnoreCase(setting, "xmloption")) {
        return std.ascii.eqlIgnoreCase(value, "content");
    }
    if (std.ascii.eqlIgnoreCase(setting, "client_min_messages")) {
        return std.ascii.eqlIgnoreCase(value, "warning") or
            std.ascii.eqlIgnoreCase(value, "notice") or
            std.ascii.eqlIgnoreCase(value, "error");
    }
    return false;
}

pub const sqlKeywordIsAnyOrSome = lower_expr.sqlKeywordIsAnyOrSome;
pub const sqlKeywordStartsScalarPredicate = lower_expr.sqlKeywordStartsScalarPredicate;
pub const sqlJoinedSourceAliasTerminator = lower_expr.sqlJoinedSourceAliasTerminator;
pub const sqlAssignmentTailKeyword = lower_expr.sqlAssignmentTailKeyword;
pub const sqlKeywordIsLengthFunction = lower_expr.sqlKeywordIsLengthFunction;
pub const sqlKeywordIsOctetLengthFunction = lower_expr.sqlKeywordIsOctetLengthFunction;
pub const sqlKeywordIsBitLengthFunction = lower_expr.sqlKeywordIsBitLengthFunction;
pub const sqlKeywordIsJsonArrayLengthFunction = lower_expr.sqlKeywordIsJsonArrayLengthFunction;
pub const sqlKeywordIsCardinalityFunction = lower_expr.sqlKeywordIsCardinalityFunction;
pub const sqlKeywordIsArrayLengthFunction = lower_expr.sqlKeywordIsArrayLengthFunction;
pub const sqlKeywordIsArrayPositionFunction = lower_expr.sqlKeywordIsArrayPositionFunction;
pub const sqlKeywordIsArrayToStringFunction = lower_expr.sqlKeywordIsArrayToStringFunction;
pub const arrayLengthDefaultOutput = lower_expr.arrayLengthDefaultOutput;
pub const sqlKeywordIsJsonTypeofFunction = lower_expr.sqlKeywordIsJsonTypeofFunction;
pub const sqlKeywordIsJsonExtractPathFunction = lower_expr.sqlKeywordIsJsonExtractPathFunction;
pub const sqlKeywordIsJsonBuildObjectFunction = lower_expr.sqlKeywordIsJsonBuildObjectFunction;
pub const sqlJsonExtractPathFunctionAsText = lower_expr.sqlJsonExtractPathFunctionAsText;
pub const sqlKeywordIsAsciiFunction = lower_expr.sqlKeywordIsAsciiFunction;
pub const sqlKeywordIsChrFunction = lower_expr.sqlKeywordIsChrFunction;
pub const sqlKeywordIsSubstringFunction = lower_expr.sqlKeywordIsSubstringFunction;
pub const sqlKeywordIsOverlayFunction = lower_expr.sqlKeywordIsOverlayFunction;
pub const sqlKeywordIsTranslateFunction = lower_expr.sqlKeywordIsTranslateFunction;
pub const sqlKeywordIsSplitPartFunction = lower_expr.sqlKeywordIsSplitPartFunction;
pub const sqlKeywordIsStrposFunction = lower_expr.sqlKeywordIsStrposFunction;
pub const sqlKeywordIsLeftRightFunction = lower_expr.sqlKeywordIsLeftRightFunction;
pub const sqlKeywordIsPadFunction = lower_expr.sqlKeywordIsPadFunction;
pub const sqlKeywordIsRepeatFunction = lower_expr.sqlKeywordIsRepeatFunction;
pub const sqlKeywordIsReverseFunction = lower_expr.sqlKeywordIsReverseFunction;
pub const sqlKeywordIsInitcapFunction = lower_expr.sqlKeywordIsInitcapFunction;
pub const sqlKeywordIsMd5Function = lower_expr.sqlKeywordIsMd5Function;
pub const sqlKeywordIsStartsWithFunction = lower_expr.sqlKeywordIsStartsWithFunction;
pub const sqlKeywordIsEndsWithFunction = lower_expr.sqlKeywordIsEndsWithFunction;
pub const sqlKeywordIsDateTruncFunction = lower_expr.sqlKeywordIsDateTruncFunction;
pub const sqlKeywordIsDateBinFunction = lower_expr.sqlKeywordIsDateBinFunction;
pub const sqlKeywordIsDatePartFunction = lower_expr.sqlKeywordIsDatePartFunction;
pub const sqlKeywordIsTrimVariantFunction = lower_expr.sqlKeywordIsTrimVariantFunction;
pub const sqlKeywordIsUuidV4Function = lower_expr.sqlKeywordIsUuidV4Function;
pub const sqlKeywordIsRegexpMatchFunction = lower_expr.sqlKeywordIsRegexpMatchFunction;
pub const sqlKeywordIsRegexpCountFunction = lower_expr.sqlKeywordIsRegexpCountFunction;
pub const sqlKeywordIsRegexpSubstrFunction = lower_expr.sqlKeywordIsRegexpSubstrFunction;
pub const sqlKeywordIsRegexpInstrFunction = lower_expr.sqlKeywordIsRegexpInstrFunction;
pub const rowExpressionBoundaryKeyword = lower_expr.rowExpressionBoundaryKeyword;
pub const sqlWhereTailClauseKeyword = lower_expr.sqlWhereTailClauseKeyword;
pub const sqlWindowTailClauseKeyword = lower_expr.sqlWindowTailClauseKeyword;

test "sql adapter grammar parses row security catalog tails" {
    const alloc = std.testing.allocator;
    var tokens = try lexer.tokenizeAlloc(alloc, "TABLE public.usage_records DISABLE ROW LEVEL SECURITY;");
    defer lexer.freeTokens(alloc, &tokens);

    var pos: usize = 0;
    const syntax = (try parseAlterRowSecurity(tokens.items, &pos)) orelse return error.TestUnexpectedResult;
    try std.testing.expect(!syntax.enabled);
    try std.testing.expectEqualStrings("public.usage_records", syntax.table_identifier);
    try std.testing.expectEqual(tokens.items.len, pos);

    var create_tokens = try lexer.tokenizeAlloc(alloc, "POLICY tenant_policy ON public.usage_records USING (tenant_id = current_setting('app.tenant_id'));");
    defer lexer.freeTokens(alloc, &create_tokens);
    var create_pos: usize = 0;
    var create = try parseCreateRowSecurityPolicyCatalogTailAlloc(alloc, create_tokens.items, &create_pos);
    defer create.deinit(alloc);
    try std.testing.expectEqual(create_tokens.items.len, create_pos);
    try std.testing.expectEqualStrings("tenant_policy", create.policy_name);
    try std.testing.expectEqualStrings("usage_records", create.table_name);
    const predicate = switch (create.predicate) {
        .current_setting_equals => |predicate| predicate,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqualStrings("tenant_id", predicate.field);
    try std.testing.expectEqualStrings("app.tenant_id", predicate.setting_name);

    var targeted_tokens = try lexer.tokenizeAlloc(alloc, "POLICY tenant_policy ON usage_records TO app_reader, app_writer USING (tenant_id = current_setting('app.tenant_id'));");
    defer lexer.freeTokens(alloc, &targeted_tokens);
    var targeted_pos: usize = 0;
    var targeted = try parseCreateRowSecurityPolicyCatalogTailAlloc(alloc, targeted_tokens.items, &targeted_pos);
    defer targeted.deinit(alloc);
    try std.testing.expectEqual(targeted_tokens.items.len, targeted_pos);
    try std.testing.expectEqualStrings("tenant_policy", targeted.policy_name);
    try std.testing.expectEqual(@as(usize, 2), targeted.role_targets.len);
    try std.testing.expectEqualStrings("app_reader", targeted.role_targets[0]);
    try std.testing.expectEqualStrings("app_writer", targeted.role_targets[1]);

    var role_tokens = try lexer.tokenizeAlloc(alloc, "TO app_reader, app_writer USING");
    defer lexer.freeTokens(alloc, &role_tokens);
    var role_pos: usize = 0;
    const role_targets = try parseOptionalRowSecurityPolicyRoleTargetsAlloc(alloc, role_tokens.items, &role_pos);
    defer freeStringSlice(alloc, role_targets);
    try std.testing.expectEqual(@as(usize, 2), role_targets.len);
    try std.testing.expectEqualStrings("app_reader", role_targets[0]);
    try std.testing.expectEqualStrings("app_writer", role_targets[1]);
    try std.testing.expectEqualStrings("USING", role_tokens.items[role_pos].text);

    var literal_tokens = try lexer.tokenizeAlloc(alloc, "POLICY tenant_policy_literal ON usage_records USING (tenant_id = 'tenant-a');");
    defer lexer.freeTokens(alloc, &literal_tokens);
    var literal_pos: usize = 0;
    var literal = try parseCreateRowSecurityPolicyCatalogTailAlloc(alloc, literal_tokens.items, &literal_pos);
    defer literal.deinit(alloc);
    try std.testing.expectEqual(literal_tokens.items.len, literal_pos);
    const literal_predicate = switch (literal.predicate) {
        .literal_equals => |predicate_value| predicate_value,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqualStrings("tenant_id", literal_predicate.field);
    try std.testing.expectEqualStrings("\"tenant-a\"", literal_predicate.value_json);

    var check_tokens = try lexer.tokenizeAlloc(alloc, "POLICY tenant_policy_check ON usage_records USING (tenant_id = 'tenant-a') WITH CHECK (status = 'active');");
    defer lexer.freeTokens(alloc, &check_tokens);
    var check_pos: usize = 0;
    var check = try parseCreateRowSecurityPolicyCatalogTailAlloc(alloc, check_tokens.items, &check_pos);
    defer check.deinit(alloc);
    try std.testing.expectEqual(check_tokens.items.len, check_pos);
    const check_predicate = switch (check.check_predicate orelse return error.TestUnexpectedResult) {
        .literal_equals => |predicate_value| predicate_value,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqualStrings("status", check_predicate.field);
    try std.testing.expectEqualStrings("\"active\"", check_predicate.value_json);

    var alter_tokens = try lexer.tokenizeAlloc(alloc, "POLICY tenant_policy ON usage_records USING (status = 'active');");
    defer lexer.freeTokens(alloc, &alter_tokens);
    var alter_pos: usize = 0;
    var alter = try parseAlterRowSecurityPolicyCatalogTailAlloc(alloc, alter_tokens.items, &alter_pos);
    defer alter.deinit(alloc);
    try std.testing.expectEqual(alter_tokens.items.len, alter_pos);
    try std.testing.expectEqualStrings("tenant_policy", alter.policy_name);
    try std.testing.expectEqualStrings("usage_records", alter.table_name);
    const alter_predicate = switch (alter.predicate orelse return error.TestUnexpectedResult) {
        .literal_equals => |alter_predicate| alter_predicate,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqualStrings("status", alter_predicate.field);
    try std.testing.expectEqualStrings("\"active\"", alter_predicate.value_json);

    var alter_targets_tokens = try lexer.tokenizeAlloc(alloc, "POLICY tenant_policy ON usage_records TO app_writer;");
    defer lexer.freeTokens(alloc, &alter_targets_tokens);
    var alter_targets_pos: usize = 0;
    var alter_targets = try parseAlterRowSecurityPolicyCatalogTailAlloc(alloc, alter_targets_tokens.items, &alter_targets_pos);
    defer alter_targets.deinit(alloc);
    try std.testing.expectEqual(alter_targets_tokens.items.len, alter_targets_pos);
    try std.testing.expect(alter_targets.role_targets_present);
    try std.testing.expectEqual(@as(usize, 1), alter_targets.role_targets.len);
    try std.testing.expectEqualStrings("app_writer", alter_targets.role_targets[0]);
    try std.testing.expect(alter_targets.predicate == null);
    try std.testing.expect(alter_targets.check_predicate == null);

    var alter_check_tokens = try lexer.tokenizeAlloc(alloc, "POLICY tenant_policy ON usage_records WITH CHECK (status = 'ready');");
    defer lexer.freeTokens(alloc, &alter_check_tokens);
    var alter_check_pos: usize = 0;
    var alter_check = try parseAlterRowSecurityPolicyCatalogTailAlloc(alloc, alter_check_tokens.items, &alter_check_pos);
    defer alter_check.deinit(alloc);
    try std.testing.expectEqual(alter_check_tokens.items.len, alter_check_pos);
    try std.testing.expect(!alter_check.role_targets_present);
    try std.testing.expect(alter_check.predicate == null);
    const alter_check_predicate = switch (alter_check.check_predicate orelse return error.TestUnexpectedResult) {
        .literal_equals => |predicate_value| predicate_value,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqualStrings("status", alter_check_predicate.field);
    try std.testing.expectEqualStrings("\"ready\"", alter_check_predicate.value_json);

    var compound_tokens = try lexer.tokenizeAlloc(alloc, "POLICY tenant_policy ON usage_records USING (tenant_id = 'tenant-a' AND status = 'active');");
    defer lexer.freeTokens(alloc, &compound_tokens);
    var compound_pos: usize = 0;
    var compound = try parseCreateRowSecurityPolicyCatalogTailAlloc(alloc, compound_tokens.items, &compound_pos);
    defer compound.deinit(alloc);
    try std.testing.expectEqual(compound_tokens.items.len, compound_pos);
    const compound_predicate = switch (compound.predicate) {
        .conjunction => |conjunction| conjunction,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqual(@as(usize, 2), compound_predicate.predicates.len);
    const compound_first = switch (compound_predicate.predicates[0]) {
        .literal_equals => |predicate_value| predicate_value,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqualStrings("tenant_id", compound_first.field);
    try std.testing.expectEqualStrings("\"tenant-a\"", compound_first.value_json);
    const compound_second = switch (compound_predicate.predicates[1]) {
        .literal_equals => |predicate_value| predicate_value,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqualStrings("status", compound_second.field);
    try std.testing.expectEqualStrings("\"active\"", compound_second.value_json);

    var disjunction_tokens = try lexer.tokenizeAlloc(alloc, "POLICY tenant_policy ON usage_records USING (tenant_id = 'tenant-a' OR status = 'active');");
    defer lexer.freeTokens(alloc, &disjunction_tokens);
    var disjunction_pos: usize = 0;
    var disjunction = try parseCreateRowSecurityPolicyCatalogTailAlloc(alloc, disjunction_tokens.items, &disjunction_pos);
    defer disjunction.deinit(alloc);
    try std.testing.expectEqual(disjunction_tokens.items.len, disjunction_pos);
    const disjunction_predicate = switch (disjunction.predicate) {
        .disjunction => |or_predicate| or_predicate,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqual(@as(usize, 2), disjunction_predicate.predicates.len);
    const disjunction_first = switch (disjunction_predicate.predicates[0]) {
        .literal_equals => |predicate_value| predicate_value,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqualStrings("tenant_id", disjunction_first.field);
    try std.testing.expectEqualStrings("\"tenant-a\"", disjunction_first.value_json);
    const disjunction_second = switch (disjunction_predicate.predicates[1]) {
        .literal_equals => |predicate_value| predicate_value,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqualStrings("status", disjunction_second.field);
    try std.testing.expectEqualStrings("\"active\"", disjunction_second.value_json);

    var mixed_boolean_tokens = try lexer.tokenizeAlloc(alloc, "POLICY tenant_policy ON usage_records USING (tenant_id = 'tenant-a' OR status = 'active' AND region = 'us');");
    defer lexer.freeTokens(alloc, &mixed_boolean_tokens);
    var mixed_boolean_pos: usize = 0;
    var mixed_boolean = try parseCreateRowSecurityPolicyCatalogTailAlloc(alloc, mixed_boolean_tokens.items, &mixed_boolean_pos);
    defer mixed_boolean.deinit(alloc);
    try std.testing.expectEqual(mixed_boolean_tokens.items.len, mixed_boolean_pos);
    const mixed_disjunction = switch (mixed_boolean.predicate) {
        .disjunction => |or_predicate| or_predicate,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqual(@as(usize, 2), mixed_disjunction.predicates.len);
    const mixed_first = switch (mixed_disjunction.predicates[0]) {
        .literal_equals => |predicate_value| predicate_value,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqualStrings("tenant_id", mixed_first.field);
    const mixed_second = switch (mixed_disjunction.predicates[1]) {
        .conjunction => |and_predicate| and_predicate,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqual(@as(usize, 2), mixed_second.predicates.len);

    var drop_tokens = try lexer.tokenizeAlloc(alloc, "POLICY IF EXISTS tenant_policy ON public.usage_records CASCADE;");
    defer lexer.freeTokens(alloc, &drop_tokens);
    var drop_pos: usize = 0;
    var drop = try parseDropRowSecurityPolicyCatalogTailAlloc(alloc, drop_tokens.items, &drop_pos);
    defer drop.deinit(alloc);
    try std.testing.expectEqual(drop_tokens.items.len, drop_pos);
    try std.testing.expectEqualStrings("tenant_policy", drop.policy_name);
    try std.testing.expectEqualStrings("usage_records", drop.table_name);
    try std.testing.expect(drop.if_exists);
}

test "sql adapter grammar parses update policy trigger catalog tails" {
    const alloc = std.testing.allocator;
    var create_tokens = try lexer.tokenizeAlloc(alloc, "TRIGGER touch_updated_at BEFORE UPDATE OF amount, status ON public.usage_records FOR EACH ROW EXECUTE FUNCTION public.touch_updated_at('updated_at_ns');");
    defer lexer.freeTokens(alloc, &create_tokens);

    var create_pos: usize = 0;
    var create = try parseCreateUpdatePolicyTriggerCatalogTailAlloc(alloc, create_tokens.items, &create_pos);
    defer create.deinit(alloc);
    try std.testing.expectEqual(create_tokens.items.len, create_pos);
    try std.testing.expectEqualStrings("touch_updated_at", create.trigger_name);
    try std.testing.expectEqualStrings("usage_records", create.table_name);
    try std.testing.expectEqualStrings("updated_at_ns", create.column_name);

    var default_tokens = try lexer.tokenizeAlloc(alloc, "TRIGGER touch_updated_at BEFORE UPDATE ON usage_records EXECUTE PROCEDURE set_updated_at();");
    defer lexer.freeTokens(alloc, &default_tokens);
    var default_pos: usize = 0;
    var default = try parseCreateUpdatePolicyTriggerCatalogTailAlloc(alloc, default_tokens.items, &default_pos);
    defer default.deinit(alloc);
    try std.testing.expectEqual(default_tokens.items.len, default_pos);
    try std.testing.expectEqualStrings("updated_at", default.column_name);

    var unsupported_tokens = try lexer.tokenizeAlloc(alloc, "TRIGGER audit_changes BEFORE UPDATE ON usage_records EXECUTE FUNCTION audit_changes();");
    defer lexer.freeTokens(alloc, &unsupported_tokens);
    var unsupported_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseCreateUpdatePolicyTriggerCatalogTailAlloc(alloc, unsupported_tokens.items, &unsupported_pos));

    var tokens = try lexer.tokenizeAlloc(alloc, "TRIGGER IF EXISTS touch_updated_at ON ONLY public.usage_records CASCADE;");
    defer lexer.freeTokens(alloc, &tokens);

    var pos: usize = 0;
    var syntax = try parseDropUpdatePolicyTriggerCatalogTailAlloc(alloc, tokens.items, &pos);
    defer syntax.deinit(alloc);
    try std.testing.expectEqual(tokens.items.len, pos);
    try std.testing.expectEqualStrings("touch_updated_at", syntax.trigger_name);
    try std.testing.expectEqualStrings("usage_records", syntax.table_name);
    try std.testing.expect(syntax.if_exists);
}

test "sql adapter grammar leaves non row security alter table to ddl parser" {
    const alloc = std.testing.allocator;
    var tokens = try lexer.tokenizeAlloc(alloc, "TABLE usage_records ADD COLUMN status text;");
    defer lexer.freeTokens(alloc, &tokens);

    var pos: usize = 0;
    try std.testing.expect((try parseAlterRowSecurity(tokens.items, &pos)) == null);
    try std.testing.expectEqual(@as(usize, 0), pos);
}

test "sql adapter grammar accepts allowlisted adapter session cleanup" {
    const alloc = std.testing.allocator;

    var set_tokens = try lexer.tokenizeAlloc(alloc, "LOCAL client_min_messages = warning;");
    defer lexer.freeTokens(alloc, &set_tokens);
    var set_pos: usize = 0;
    try parseAdapterNoopSetStatementTail(set_tokens.items, &set_pos);
    try std.testing.expectEqual(set_tokens.items.len, set_pos);

    var search_path_tokens = try lexer.tokenizeAlloc(alloc, "search_path TO public;");
    defer lexer.freeTokens(alloc, &search_path_tokens);
    var search_path_pos: usize = 0;
    try parseAdapterNoopSetStatementTail(search_path_tokens.items, &search_path_pos);
    try std.testing.expectEqual(search_path_tokens.items.len, search_path_pos);

    var local_search_path_tokens = try lexer.tokenizeAlloc(alloc, "LOCAL search_path TO public;");
    defer lexer.freeTokens(alloc, &local_search_path_tokens);
    var local_search_path_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseAdapterNoopSetStatementTail(local_search_path_tokens.items, &local_search_path_pos));

    var reset_tokens = try lexer.tokenizeAlloc(alloc, "client_min_messages;");
    defer lexer.freeTokens(alloc, &reset_tokens);
    var reset_pos: usize = 0;
    try parseAdapterNoopResetStatementTail(reset_tokens.items, &reset_pos);
    try std.testing.expectEqual(reset_tokens.items.len, reset_pos);

    var show_tokens = try lexer.tokenizeAlloc(alloc, "search_path;");
    defer lexer.freeTokens(alloc, &show_tokens);
    var show_pos: usize = 0;
    try parseAdapterNoopShowStatementTail(show_tokens.items, &show_pos);
    try std.testing.expectEqual(show_tokens.items.len, show_pos);

    var discard_tokens = try lexer.tokenizeAlloc(alloc, "ALL;");
    defer lexer.freeTokens(alloc, &discard_tokens);
    var discard_pos: usize = 0;
    try parseAdapterNoopDiscardStatementTail(discard_tokens.items, &discard_pos);
    try std.testing.expectEqual(discard_tokens.items.len, discard_pos);

    var end_tokens = try lexer.tokenizeAlloc(alloc, ";");
    defer lexer.freeTokens(alloc, &end_tokens);
    var end_pos: usize = 0;
    try parseAdapterNoopStatementEnd(end_tokens.items, &end_pos);
    try std.testing.expectEqual(end_tokens.items.len, end_pos);

    var extra_tokens = try lexer.tokenizeAlloc(alloc, "; SELECT 1");
    defer lexer.freeTokens(alloc, &extra_tokens);
    var extra_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseAdapterNoopStatementEnd(extra_tokens.items, &extra_pos));
}

test "sql adapter grammar rejects semantic session changes as noops" {
    const alloc = std.testing.allocator;

    var tenant_path_tokens = try lexer.tokenizeAlloc(alloc, "search_path TO tenant_schema, public;");
    defer lexer.freeTokens(alloc, &tenant_path_tokens);
    var tenant_path_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseAdapterNoopSetStatementTail(tenant_path_tokens.items, &tenant_path_pos));
    tenant_path_pos = 0;
    var tenant_path = try parseSetSearchPathTailAlloc(alloc, tenant_path_tokens.items, &tenant_path_pos);
    defer tenant_path.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), tenant_path.namespaces.len);
    try std.testing.expectEqualStrings("tenant_schema", tenant_path.namespaces[0]);
    try std.testing.expectEqualStrings("public", tenant_path.namespaces[1]);

    var latin1_tokens = try lexer.tokenizeAlloc(alloc, "client_encoding = 'LATIN1';");
    defer lexer.freeTokens(alloc, &latin1_tokens);
    var latin1_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseAdapterNoopSetStatementTail(latin1_tokens.items, &latin1_pos));

    var timeout_tokens = try lexer.tokenizeAlloc(alloc, "statement_timeout = '1ms';");
    defer lexer.freeTokens(alloc, &timeout_tokens);
    var timeout_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseAdapterNoopSetStatementTail(timeout_tokens.items, &timeout_pos));
    timeout_pos = 0;
    var timeout_setting = try parseSetSessionSettingTailAlloc(alloc, timeout_tokens.items, &timeout_pos);
    defer timeout_setting.deinit(alloc);
    try std.testing.expectEqual(timeout_tokens.items.len, timeout_pos);
    try std.testing.expectEqual(ddl_plan.SessionSettingKind.runtime, timeout_setting.kind);
    try std.testing.expectEqualStrings("statement_timeout", timeout_setting.name);
    try std.testing.expectEqualStrings("1ms", timeout_setting.value);

    var app_setting_tokens = try lexer.tokenizeAlloc(alloc, "LOCAL app.tenant_id = 'tenant-a';");
    defer lexer.freeTokens(alloc, &app_setting_tokens);
    var app_setting_pos: usize = 0;
    var app_setting = try parseSetSessionSettingTailAlloc(alloc, app_setting_tokens.items, &app_setting_pos);
    defer app_setting.deinit(alloc);
    try std.testing.expectEqual(app_setting_tokens.items.len, app_setting_pos);
    try std.testing.expectEqual(ddl_plan.SessionSettingKind.app, app_setting.kind);
    try std.testing.expect(app_setting.local);
    try std.testing.expectEqualStrings("app.tenant_id", app_setting.name);
    try std.testing.expectEqualStrings("tenant-a", app_setting.value);

    var sync_level_tokens = try lexer.tokenizeAlloc(alloc, "antfly.sync_level = 'full_index';");
    defer lexer.freeTokens(alloc, &sync_level_tokens);
    var sync_level_pos: usize = 0;
    var sync_level_setting = try parseSetSessionSettingTailAlloc(alloc, sync_level_tokens.items, &sync_level_pos);
    defer sync_level_setting.deinit(alloc);
    try std.testing.expectEqual(sync_level_tokens.items.len, sync_level_pos);
    try std.testing.expectEqual(ddl_plan.SessionSettingKind.antfly, sync_level_setting.kind);
    try std.testing.expectEqualStrings("antfly.sync_level", sync_level_setting.name);
    try std.testing.expectEqualStrings("full_index", sync_level_setting.value);

    var invalid_sync_level_tokens = try lexer.tokenizeAlloc(alloc, "antfly.sync_level = 'eventual';");
    defer lexer.freeTokens(alloc, &invalid_sync_level_tokens);
    var invalid_sync_level_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseSetSessionSettingTailAlloc(alloc, invalid_sync_level_tokens.items, &invalid_sync_level_pos));

    var reset_app_tokens = try lexer.tokenizeAlloc(alloc, "app.tenant_id;");
    defer lexer.freeTokens(alloc, &reset_app_tokens);
    var reset_app_pos: usize = 0;
    var reset_app = try parseResetSessionSettingTailAlloc(alloc, reset_app_tokens.items, &reset_app_pos);
    defer reset_app.deinit(alloc);
    try std.testing.expectEqual(reset_app_tokens.items.len, reset_app_pos);
    try std.testing.expectEqual(ddl_plan.SessionSettingKind.app, reset_app.kind);
    try std.testing.expectEqualStrings("app.tenant_id", reset_app.name);

    var show_all_tokens = try lexer.tokenizeAlloc(alloc, "ALL;");
    defer lexer.freeTokens(alloc, &show_all_tokens);
    var show_all_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseAdapterNoopShowStatementTail(show_all_tokens.items, &show_all_pos));

    var discard_temp_tokens = try lexer.tokenizeAlloc(alloc, "TEMP;");
    defer lexer.freeTokens(alloc, &discard_temp_tokens);
    var discard_temp_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseAdapterNoopDiscardStatementTail(discard_temp_tokens.items, &discard_temp_pos));
}

test "sql adapter grammar matches transaction boundary noops" {
    const alloc = std.testing.allocator;

    var bare_tokens = try lexer.tokenizeAlloc(alloc, ";");
    defer lexer.freeTokens(alloc, &bare_tokens);
    var bare_pos: usize = 0;
    try std.testing.expect(try matchAdapterNoopTransactionBoundaryTail(bare_tokens.items, &bare_pos, .{}));
    try std.testing.expectEqual(bare_tokens.items.len, bare_pos);

    var work_tokens = try lexer.tokenizeAlloc(alloc, "WORK;");
    defer lexer.freeTokens(alloc, &work_tokens);
    var work_pos: usize = 0;
    try std.testing.expect(try matchAdapterNoopTransactionBoundaryTail(work_tokens.items, &work_pos, .{ .work = true }));
    try std.testing.expectEqual(work_tokens.items.len, work_pos);

    var transaction_tokens = try lexer.tokenizeAlloc(alloc, "TRANSACTION;");
    defer lexer.freeTokens(alloc, &transaction_tokens);
    var transaction_pos: usize = 0;
    try std.testing.expect(try matchAdapterNoopTransactionBoundaryTail(transaction_tokens.items, &transaction_pos, .{ .transaction = true }));
    try std.testing.expectEqual(transaction_tokens.items.len, transaction_pos);

    var prepared_tokens = try lexer.tokenizeAlloc(alloc, "PREPARED 'x';");
    defer lexer.freeTokens(alloc, &prepared_tokens);
    var prepared_pos: usize = 0;
    try std.testing.expect(!try matchAdapterNoopTransactionBoundaryTail(prepared_tokens.items, &prepared_pos, .{ .work = true, .transaction = true }));
    try std.testing.expectEqual(@as(usize, 0), prepared_pos);

    var extra_tokens = try lexer.tokenizeAlloc(alloc, "; SELECT 1");
    defer lexer.freeTokens(alloc, &extra_tokens);
    var extra_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, matchAdapterNoopTransactionBoundaryTail(extra_tokens.items, &extra_pos, .{}));
}

test "sql adapter grammar parses transaction control tails" {
    const alloc = std.testing.allocator;

    var lock_tokens = try lexer.tokenizeAlloc(alloc, "TABLE public.usage_records, audit_records IN SHARE ROW EXCLUSIVE MODE;");
    defer lexer.freeTokens(alloc, &lock_tokens);
    var lock_pos: usize = 0;
    var lock = try parseTableLockTailAlloc(alloc, lock_tokens.items, &lock_pos);
    defer lock.deinit(alloc);
    try std.testing.expectEqual(lock_tokens.items.len, lock_pos);
    try std.testing.expectEqual(@as(usize, 2), lock.table_names.len);
    try std.testing.expectEqualStrings("usage_records", lock.table_names[0]);
    try std.testing.expectEqualStrings("audit_records", lock.table_names[1]);
    try std.testing.expectEqual(TableLockModeSyntax.share_row_exclusive, lock.mode);

    var constraints_tokens = try lexer.tokenizeAlloc(alloc, "CONSTRAINTS public.fk_usage_account, fk_usage_org DEFERRED;");
    defer lexer.freeTokens(alloc, &constraints_tokens);
    var constraints_pos: usize = 0;
    var constraints = try parseConstraintModeTailAlloc(alloc, constraints_tokens.items, &constraints_pos);
    defer constraints.deinit(alloc);
    try std.testing.expectEqual(constraints_tokens.items.len, constraints_pos);
    try std.testing.expect(!constraints.all);
    try std.testing.expectEqual(@as(usize, 2), constraints.constraint_names.len);
    try std.testing.expectEqualStrings("fk_usage_account", constraints.constraint_names[0]);
    try std.testing.expectEqualStrings("fk_usage_org", constraints.constraint_names[1]);
    try std.testing.expectEqual(ConstraintCheckModeSyntax.deferred, constraints.mode);

    var constraints_all_tokens = try lexer.tokenizeAlloc(alloc, "CONSTRAINTS ALL IMMEDIATE;");
    defer lexer.freeTokens(alloc, &constraints_all_tokens);
    var constraints_all_pos: usize = 0;
    var constraints_all = try parseConstraintModeTailAlloc(alloc, constraints_all_tokens.items, &constraints_all_pos);
    defer constraints_all.deinit(alloc);
    try std.testing.expectEqual(constraints_all_tokens.items.len, constraints_all_pos);
    try std.testing.expect(constraints_all.all);
    try std.testing.expectEqual(@as(usize, 0), constraints_all.constraint_names.len);
    try std.testing.expectEqual(ConstraintCheckModeSyntax.immediate, constraints_all.mode);

    var transaction_tokens = try lexer.tokenizeAlloc(alloc, "TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY, NOT DEFERRABLE;");
    defer lexer.freeTokens(alloc, &transaction_tokens);
    var transaction_pos: usize = 0;
    const transaction = try parseTransactionModeTail(transaction_tokens.items, &transaction_pos, .set_transaction);
    try std.testing.expectEqual(transaction_tokens.items.len, transaction_pos);
    try std.testing.expectEqual(TransactionModeStarterSyntax.set_transaction, transaction.starter);
    try std.testing.expectEqual(TransactionIsolationLevelSyntax.repeatable_read, transaction.isolation_level.?);
    try std.testing.expectEqual(TransactionAccessModeSyntax.read_only, transaction.access_mode.?);
    try std.testing.expectEqual(false, transaction.deferrable.?);

    var begin_tokens = try lexer.tokenizeAlloc(alloc, "READ WRITE DEFERRABLE;");
    defer lexer.freeTokens(alloc, &begin_tokens);
    var begin_pos: usize = 0;
    const begin = try parseTransactionModeTail(begin_tokens.items, &begin_pos, .begin);
    try std.testing.expectEqual(begin_tokens.items.len, begin_pos);
    try std.testing.expectEqual(TransactionModeStarterSyntax.begin, begin.starter);
    try std.testing.expect(begin.isolation_level == null);
    try std.testing.expectEqual(TransactionAccessModeSyntax.read_write, begin.access_mode.?);
    try std.testing.expectEqual(true, begin.deferrable.?);

    var duplicate_tokens = try lexer.tokenizeAlloc(alloc, "TRANSACTION READ ONLY READ WRITE;");
    defer lexer.freeTokens(alloc, &duplicate_tokens);
    var duplicate_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseTransactionModeTail(duplicate_tokens.items, &duplicate_pos, .start_transaction));

    var advisory_tokens = try lexer.tokenizeAlloc(alloc, "pg_advisory_lock(-42, 7);");
    defer lexer.freeTokens(alloc, &advisory_tokens);
    var advisory_pos: usize = 0;
    const advisory = try parseAdvisoryLockTail(advisory_tokens.items, &advisory_pos);
    try std.testing.expectEqual(advisory_tokens.items.len, advisory_pos);
    try std.testing.expectEqual(AdvisoryLockActionSyntax.lock, advisory.action);
    try std.testing.expectEqual(@as(i64, -42), advisory.key1);
    try std.testing.expectEqual(@as(i64, 7), advisory.key2.?);

    var advisory_unlock_tokens = try lexer.tokenizeAlloc(alloc, "pg_advisory_unlock(42);");
    defer lexer.freeTokens(alloc, &advisory_unlock_tokens);
    var advisory_unlock_pos: usize = 0;
    const advisory_unlock = try parseAdvisoryLockTail(advisory_unlock_tokens.items, &advisory_unlock_pos);
    try std.testing.expectEqual(advisory_unlock_tokens.items.len, advisory_unlock_pos);
    try std.testing.expectEqual(AdvisoryLockActionSyntax.unlock, advisory_unlock.action);
    try std.testing.expectEqual(@as(i64, 42), advisory_unlock.key1);
    try std.testing.expect(advisory_unlock.key2 == null);

    var fractional_key_tokens = try lexer.tokenizeAlloc(alloc, "pg_advisory_lock(4.2);");
    defer lexer.freeTokens(alloc, &fractional_key_tokens);
    var fractional_key_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseAdvisoryLockTail(fractional_key_tokens.items, &fractional_key_pos));
}

test "sql adapter grammar parses maintenance job tails" {
    const alloc = std.testing.allocator;

    var vacuum_tokens = try lexer.tokenizeAlloc(alloc, "(FULL, VERBOSE, ANALYZE) public.usage_records;");
    defer lexer.freeTokens(alloc, &vacuum_tokens);
    var vacuum_pos: usize = 0;
    var vacuum = try parseVacuumMaintenanceTailAlloc(alloc, vacuum_tokens.items, &vacuum_pos);
    defer vacuum.deinit(alloc);
    try std.testing.expectEqual(vacuum_tokens.items.len, vacuum_pos);
    try std.testing.expectEqualStrings("usage_records", vacuum.table_name);
    try std.testing.expect(vacuum.full);
    try std.testing.expect(!vacuum.freeze);
    try std.testing.expect(vacuum.verbose);
    try std.testing.expect(vacuum.analyze);

    var vacuum_legacy_tokens = try lexer.tokenizeAlloc(alloc, "FULL FREEZE VERBOSE usage_records;");
    defer lexer.freeTokens(alloc, &vacuum_legacy_tokens);
    var vacuum_legacy_pos: usize = 0;
    var vacuum_legacy = try parseVacuumMaintenanceTailAlloc(alloc, vacuum_legacy_tokens.items, &vacuum_legacy_pos);
    defer vacuum_legacy.deinit(alloc);
    try std.testing.expectEqual(vacuum_legacy_tokens.items.len, vacuum_legacy_pos);
    try std.testing.expectEqualStrings("usage_records", vacuum_legacy.table_name);
    try std.testing.expect(vacuum_legacy.full);
    try std.testing.expect(vacuum_legacy.freeze);
    try std.testing.expect(vacuum_legacy.verbose);
    try std.testing.expect(!vacuum_legacy.analyze);

    var analyze_tokens = try lexer.tokenizeAlloc(alloc, "VERBOSE public.usage_records (status, amount);");
    defer lexer.freeTokens(alloc, &analyze_tokens);
    var analyze_pos: usize = 0;
    var analyze = try parseAnalyzeMaintenanceTailAlloc(alloc, analyze_tokens.items, &analyze_pos);
    defer analyze.deinit(alloc);
    try std.testing.expectEqual(analyze_tokens.items.len, analyze_pos);
    try std.testing.expectEqualStrings("usage_records", analyze.table_name);
    try std.testing.expect(analyze.verbose);
    try std.testing.expectEqual(@as(usize, 2), analyze.column_count);

    var reindex_tokens = try lexer.tokenizeAlloc(alloc, "INDEX CONCURRENTLY public.usage_status_idx;");
    defer lexer.freeTokens(alloc, &reindex_tokens);
    var reindex_pos: usize = 0;
    var reindex = try parseReindexMaintenanceTailAlloc(alloc, reindex_tokens.items, &reindex_pos);
    defer reindex.deinit(alloc);
    try std.testing.expectEqual(reindex_tokens.items.len, reindex_pos);
    try std.testing.expectEqual(ReindexMaintenanceTargetSyntax.index, reindex.target);
    try std.testing.expect(reindex.concurrently);
    try std.testing.expectEqualStrings("usage_status_idx", reindex.name);

    var cluster_tokens = try lexer.tokenizeAlloc(alloc, "VERBOSE public.usage_records USING public.usage_status_idx;");
    defer lexer.freeTokens(alloc, &cluster_tokens);
    var cluster_pos: usize = 0;
    var cluster = try parseClusterMaintenanceTailAlloc(alloc, cluster_tokens.items, &cluster_pos);
    defer cluster.deinit(alloc);
    try std.testing.expectEqual(cluster_tokens.items.len, cluster_pos);
    try std.testing.expectEqualStrings("usage_records", cluster.table_name);
    try std.testing.expectEqualStrings("usage_status_idx", cluster.index_name.?);
    try std.testing.expect(cluster.verbose);

    var unsupported_tokens = try lexer.tokenizeAlloc(alloc, "usage_records (status);");
    defer lexer.freeTokens(alloc, &unsupported_tokens);
    var unsupported_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseVacuumMaintenanceTailAlloc(alloc, unsupported_tokens.items, &unsupported_pos));
}

test "sql adapter grammar parses database and tablespace catalog tails" {
    const alloc = std.testing.allocator;

    var create_database_tokens = try lexer.tokenizeAlloc(alloc, "DATABASE tenant_ops;");
    defer lexer.freeTokens(alloc, &create_database_tokens);
    var create_database_pos: usize = 0;
    var create_database = try parseCreateDatabaseCatalogTailAlloc(alloc, create_database_tokens.items, &create_database_pos);
    defer create_database.deinit(alloc);
    try std.testing.expectEqual(create_database_tokens.items.len, create_database_pos);
    try std.testing.expectEqualStrings("tenant_ops", create_database.database_name);

    var alter_database_tokens = try lexer.tokenizeAlloc(alloc, "DATABASE tenant_ops SET timezone TO 'UTC';");
    defer lexer.freeTokens(alloc, &alter_database_tokens);
    var alter_database_pos: usize = 0;
    var alter_database = try parseAlterDatabaseCatalogTailAlloc(alloc, alter_database_tokens.items, &alter_database_pos);
    defer alter_database.deinit(alloc);
    try std.testing.expectEqual(alter_database_tokens.items.len, alter_database_pos);
    try std.testing.expectEqualStrings("tenant_ops", alter_database.database_name);
    try std.testing.expectEqualStrings("timezone", alter_database.setting_name);
    try std.testing.expectEqualStrings("\"UTC\"", alter_database.value_json);

    var drop_database_tokens = try lexer.tokenizeAlloc(alloc, "DATABASE IF EXISTS tenant_ops WITH (FORCE);");
    defer lexer.freeTokens(alloc, &drop_database_tokens);
    var drop_database_pos: usize = 0;
    var drop_database = try parseDropDatabaseCatalogTailAlloc(alloc, drop_database_tokens.items, &drop_database_pos);
    defer drop_database.deinit(alloc);
    try std.testing.expectEqual(drop_database_tokens.items.len, drop_database_pos);
    try std.testing.expectEqualStrings("tenant_ops", drop_database.database_name);
    try std.testing.expect(drop_database.if_exists);
    try std.testing.expect(drop_database.force);

    var create_tablespace_tokens = try lexer.tokenizeAlloc(alloc, "TABLESPACE fastspace LOCATION '/var/lib/antfly/fastspace';");
    defer lexer.freeTokens(alloc, &create_tablespace_tokens);
    var create_tablespace_pos: usize = 0;
    var create_tablespace = try parseCreateTablespaceCatalogTailAlloc(alloc, create_tablespace_tokens.items, &create_tablespace_pos);
    defer create_tablespace.deinit(alloc);
    try std.testing.expectEqual(create_tablespace_tokens.items.len, create_tablespace_pos);
    try std.testing.expectEqualStrings("fastspace", create_tablespace.tablespace_name);
    try std.testing.expectEqualStrings("\"/var/lib/antfly/fastspace\"", create_tablespace.location_json);

    var rename_tablespace_tokens = try lexer.tokenizeAlloc(alloc, "TABLESPACE fastspace RENAME TO fastspace_archive;");
    defer lexer.freeTokens(alloc, &rename_tablespace_tokens);
    var rename_tablespace_pos: usize = 0;
    var rename_tablespace = try parseRenameTablespaceCatalogTailAlloc(alloc, rename_tablespace_tokens.items, &rename_tablespace_pos);
    defer rename_tablespace.deinit(alloc);
    try std.testing.expectEqual(rename_tablespace_tokens.items.len, rename_tablespace_pos);
    try std.testing.expectEqualStrings("fastspace", rename_tablespace.tablespace_name);
    try std.testing.expectEqualStrings("fastspace_archive", rename_tablespace.new_tablespace_name);

    var drop_tablespace_tokens = try lexer.tokenizeAlloc(alloc, "TABLESPACE IF EXISTS fastspace_archive;");
    defer lexer.freeTokens(alloc, &drop_tablespace_tokens);
    var drop_tablespace_pos: usize = 0;
    var drop_tablespace = try parseDropTablespaceCatalogTailAlloc(alloc, drop_tablespace_tokens.items, &drop_tablespace_pos);
    defer drop_tablespace.deinit(alloc);
    try std.testing.expectEqual(drop_tablespace_tokens.items.len, drop_tablespace_pos);
    try std.testing.expectEqualStrings("fastspace_archive", drop_tablespace.tablespace_name);
    try std.testing.expect(drop_tablespace.if_exists);

    var unsupported_database_tokens = try lexer.tokenizeAlloc(alloc, "DATABASE tenant_ops WITH OWNER app;");
    defer lexer.freeTokens(alloc, &unsupported_database_tokens);
    var unsupported_database_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseCreateDatabaseCatalogTailAlloc(alloc, unsupported_database_tokens.items, &unsupported_database_pos));

    var unsupported_tablespace_tokens = try lexer.tokenizeAlloc(alloc, "TABLESPACE fastspace OWNER app LOCATION '/tmp';");
    defer lexer.freeTokens(alloc, &unsupported_tablespace_tokens);
    var unsupported_tablespace_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseCreateTablespaceCatalogTailAlloc(alloc, unsupported_tablespace_tokens.items, &unsupported_tablespace_pos));
}

test "sql adapter grammar parses schema namespace catalog tails" {
    const alloc = std.testing.allocator;

    var create_tokens = try lexer.tokenizeAlloc(alloc, "SCHEMA IF NOT EXISTS tenant_ops;");
    defer lexer.freeTokens(alloc, &create_tokens);
    var create_pos: usize = 0;
    var create = try parseCreateSchemaNamespaceCatalogTailAlloc(alloc, create_tokens.items, &create_pos);
    defer create.deinit(alloc);
    try std.testing.expectEqual(create_tokens.items.len, create_pos);
    try std.testing.expectEqualStrings("tenant_ops", create.schema_name);
    try std.testing.expect(create.if_not_exists);

    var rename_tokens = try lexer.tokenizeAlloc(alloc, "SCHEMA public.tenant_ops RENAME TO public.tenant_ops_archive;");
    defer lexer.freeTokens(alloc, &rename_tokens);
    var rename_pos: usize = 0;
    var rename = try parseRenameSchemaNamespaceCatalogTailAlloc(alloc, rename_tokens.items, &rename_pos);
    defer rename.deinit(alloc);
    try std.testing.expectEqual(rename_tokens.items.len, rename_pos);
    try std.testing.expectEqualStrings("tenant_ops", rename.schema_name);
    try std.testing.expectEqualStrings("tenant_ops_archive", rename.new_schema_name);

    var drop_tokens = try lexer.tokenizeAlloc(alloc, "SCHEMA IF EXISTS tenant_ops CASCADE;");
    defer lexer.freeTokens(alloc, &drop_tokens);
    var drop_pos: usize = 0;
    var drop = try parseDropSchemaNamespaceCatalogTailAlloc(alloc, drop_tokens.items, &drop_pos);
    defer drop.deinit(alloc);
    try std.testing.expectEqual(drop_tokens.items.len, drop_pos);
    try std.testing.expectEqualStrings("tenant_ops", drop.schema_name);
    try std.testing.expect(drop.if_exists);
    try std.testing.expect(drop.cascade);

    var unsupported_create_tokens = try lexer.tokenizeAlloc(alloc, "SCHEMA tenant_ops AUTHORIZATION app;");
    defer lexer.freeTokens(alloc, &unsupported_create_tokens);
    var unsupported_create_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseCreateSchemaNamespaceCatalogTailAlloc(alloc, unsupported_create_tokens.items, &unsupported_create_pos));

    var unsupported_drop_tokens = try lexer.tokenizeAlloc(alloc, "SCHEMA tenant_ops, other_ops;");
    defer lexer.freeTokens(alloc, &unsupported_drop_tokens);
    var unsupported_drop_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseDropSchemaNamespaceCatalogTailAlloc(alloc, unsupported_drop_tokens.items, &unsupported_drop_pos));
}

test "sql adapter grammar parses extension catalog tails" {
    const alloc = std.testing.allocator;

    var create_tokens = try lexer.tokenizeAlloc(alloc, "EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;");
    defer lexer.freeTokens(alloc, &create_tokens);
    var create_pos: usize = 0;
    var create = try parseCreateExtensionCatalogTailAlloc(alloc, create_tokens.items, &create_pos);
    defer create.deinit(alloc);
    try std.testing.expectEqual(create_tokens.items.len, create_pos);
    try std.testing.expectEqualStrings("pgcrypto", create.extension_name);
    try std.testing.expectEqualStrings("public", create.schema_name.?);
    try std.testing.expect(create.if_not_exists);
    try std.testing.expect(create.version == null);

    var create_version_tokens = try lexer.tokenizeAlloc(alloc, "EXTENSION postgis VERSION '3.4.0';");
    defer lexer.freeTokens(alloc, &create_version_tokens);
    var create_version_pos: usize = 0;
    var create_version = try parseCreateExtensionCatalogTailAlloc(alloc, create_version_tokens.items, &create_version_pos);
    defer create_version.deinit(alloc);
    try std.testing.expectEqual(create_version_tokens.items.len, create_version_pos);
    try std.testing.expectEqualStrings("postgis", create_version.extension_name);
    try std.testing.expectEqualStrings("3.4.0", create_version.version.?);

    var update_tokens = try lexer.tokenizeAlloc(alloc, "EXTENSION postgis UPDATE TO '3.5.0';");
    defer lexer.freeTokens(alloc, &update_tokens);
    var update_pos: usize = 0;
    var update = try parseUpdateExtensionCatalogTailAlloc(alloc, update_tokens.items, &update_pos);
    defer update.deinit(alloc);
    try std.testing.expectEqual(update_tokens.items.len, update_pos);
    try std.testing.expectEqualStrings("postgis", update.extension_name);
    try std.testing.expectEqualStrings("3.5.0", update.target_version.?);

    var drop_tokens = try lexer.tokenizeAlloc(alloc, "EXTENSION IF EXISTS postgis CASCADE;");
    defer lexer.freeTokens(alloc, &drop_tokens);
    var drop_pos: usize = 0;
    var drop = try parseDropExtensionCatalogTailAlloc(alloc, drop_tokens.items, &drop_pos);
    defer drop.deinit(alloc);
    try std.testing.expectEqual(drop_tokens.items.len, drop_pos);
    try std.testing.expectEqualStrings("postgis", drop.extension_name);
    try std.testing.expect(drop.if_exists);
    try std.testing.expect(drop.cascade);

    var unsupported_tokens = try lexer.tokenizeAlloc(alloc, "EXTENSION postgis VERSION '3.4.0' FROM '3.3.0';");
    defer lexer.freeTokens(alloc, &unsupported_tokens);
    var unsupported_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseCreateExtensionCatalogTailAlloc(alloc, unsupported_tokens.items, &unsupported_pos));
}

test "sql adapter grammar parses routine catalog tails" {
    const alloc = std.testing.allocator;

    var create_function_tokens = try lexer.tokenizeAlloc(alloc, "FUNCTION public.normalize_status(input text) RETURNS text LANGUAGE sql;");
    defer lexer.freeTokens(alloc, &create_function_tokens);
    var create_function_pos: usize = 0;
    var create_function = try parseCreateRoutineCatalogTailAlloc(alloc, create_function_tokens.items, &create_function_pos);
    defer create_function.deinit(alloc);
    try std.testing.expectEqual(create_function_tokens.items.len, create_function_pos);
    try std.testing.expectEqual(RoutineKindSyntax.function, create_function.kind);
    try std.testing.expectEqualStrings("normalize_status", create_function.routine_name);
    try std.testing.expectEqual(@as(usize, 1), create_function.argument_count);
    try std.testing.expectEqualStrings("text", create_function.returns_type.?);
    try std.testing.expectEqualStrings("sql", create_function.language.?);

    var stable_function_tokens = try lexer.tokenizeAlloc(alloc, "FUNCTION normalize_status(input text) RETURNS text LANGUAGE sql STABLE;");
    defer lexer.freeTokens(alloc, &stable_function_tokens);
    var stable_function_pos: usize = 0;
    var stable_function = try parseCreateRoutineCatalogTailAlloc(alloc, stable_function_tokens.items, &stable_function_pos);
    defer stable_function.deinit(alloc);
    try std.testing.expectEqual(stable_function_tokens.items.len, stable_function_pos);
    try std.testing.expectEqual(RoutineKindSyntax.function, stable_function.kind);
    try std.testing.expectEqual(ddl_plan.RoutineVolatility.stable, stable_function.volatility.?);

    var cost_function_tokens = try lexer.tokenizeAlloc(alloc, "FUNCTION normalize_status(input text) RETURNS text LANGUAGE sql COST 10;");
    defer lexer.freeTokens(alloc, &cost_function_tokens);
    var cost_function_pos: usize = 0;
    var cost_function = try parseCreateRoutineCatalogTailAlloc(alloc, cost_function_tokens.items, &cost_function_pos);
    defer cost_function.deinit(alloc);
    try std.testing.expectEqual(cost_function_tokens.items.len, cost_function_pos);
    try std.testing.expectEqual(RoutineKindSyntax.function, cost_function.kind);
    try std.testing.expectEqualStrings("10", cost_function.cost.?);

    var rows_function_tokens = try lexer.tokenizeAlloc(alloc, "FUNCTION normalize_status(input text) RETURNS text LANGUAGE sql ROWS 10;");
    defer lexer.freeTokens(alloc, &rows_function_tokens);
    var rows_function_pos: usize = 0;
    var rows_function = try parseCreateRoutineCatalogTailAlloc(alloc, rows_function_tokens.items, &rows_function_pos);
    defer rows_function.deinit(alloc);
    try std.testing.expectEqual(rows_function_tokens.items.len, rows_function_pos);
    try std.testing.expectEqual(RoutineKindSyntax.function, rows_function.kind);
    try std.testing.expectEqualStrings("10", rows_function.rows.?);

    var called_null_function_tokens = try lexer.tokenizeAlloc(alloc, "FUNCTION normalize_status(input text) RETURNS text LANGUAGE sql CALLED ON NULL INPUT;");
    defer lexer.freeTokens(alloc, &called_null_function_tokens);
    var called_null_function_pos: usize = 0;
    var called_null_function = try parseCreateRoutineCatalogTailAlloc(alloc, called_null_function_tokens.items, &called_null_function_pos);
    defer called_null_function.deinit(alloc);
    try std.testing.expectEqual(called_null_function_tokens.items.len, called_null_function_pos);
    try std.testing.expectEqual(RoutineKindSyntax.function, called_null_function.kind);
    try std.testing.expectEqual(ddl_plan.RoutineNullInput.called, called_null_function.null_input.?);

    var returns_null_function_tokens = try lexer.tokenizeAlloc(alloc, "FUNCTION normalize_status(input text) RETURNS text LANGUAGE sql RETURNS NULL ON NULL INPUT;");
    defer lexer.freeTokens(alloc, &returns_null_function_tokens);
    var returns_null_function_pos: usize = 0;
    var returns_null_function = try parseCreateRoutineCatalogTailAlloc(alloc, returns_null_function_tokens.items, &returns_null_function_pos);
    defer returns_null_function.deinit(alloc);
    try std.testing.expectEqual(returns_null_function_tokens.items.len, returns_null_function_pos);
    try std.testing.expectEqual(RoutineKindSyntax.function, returns_null_function.kind);
    try std.testing.expectEqual(ddl_plan.RoutineNullInput.returns_null, returns_null_function.null_input.?);

    var strict_function_tokens = try lexer.tokenizeAlloc(alloc, "FUNCTION normalize_status(input text) RETURNS text LANGUAGE sql STRICT;");
    defer lexer.freeTokens(alloc, &strict_function_tokens);
    var strict_function_pos: usize = 0;
    var strict_function = try parseCreateRoutineCatalogTailAlloc(alloc, strict_function_tokens.items, &strict_function_pos);
    defer strict_function.deinit(alloc);
    try std.testing.expectEqual(strict_function_tokens.items.len, strict_function_pos);
    try std.testing.expectEqual(RoutineKindSyntax.function, strict_function.kind);
    try std.testing.expectEqual(ddl_plan.RoutineNullInput.returns_null, strict_function.null_input.?);

    var parallel_function_tokens = try lexer.tokenizeAlloc(alloc, "FUNCTION normalize_status(input text) RETURNS text LANGUAGE sql PARALLEL SAFE;");
    defer lexer.freeTokens(alloc, &parallel_function_tokens);
    var parallel_function_pos: usize = 0;
    var parallel_function = try parseCreateRoutineCatalogTailAlloc(alloc, parallel_function_tokens.items, &parallel_function_pos);
    defer parallel_function.deinit(alloc);
    try std.testing.expectEqual(parallel_function_tokens.items.len, parallel_function_pos);
    try std.testing.expectEqual(RoutineKindSyntax.function, parallel_function.kind);
    try std.testing.expectEqual(ddl_plan.RoutineParallelSafety.safe, parallel_function.parallel_safety.?);

    var leakproof_function_tokens = try lexer.tokenizeAlloc(alloc, "FUNCTION normalize_status(input text) RETURNS text LANGUAGE sql LEAKPROOF;");
    defer lexer.freeTokens(alloc, &leakproof_function_tokens);
    var leakproof_function_pos: usize = 0;
    var leakproof_function = try parseCreateRoutineCatalogTailAlloc(alloc, leakproof_function_tokens.items, &leakproof_function_pos);
    defer leakproof_function.deinit(alloc);
    try std.testing.expectEqual(leakproof_function_tokens.items.len, leakproof_function_pos);
    try std.testing.expectEqual(RoutineKindSyntax.function, leakproof_function.kind);
    try std.testing.expect(leakproof_function.leakproof);

    var window_function_tokens = try lexer.tokenizeAlloc(alloc, "FUNCTION normalize_status(input text) RETURNS text LANGUAGE sql WINDOW;");
    defer lexer.freeTokens(alloc, &window_function_tokens);
    var window_function_pos: usize = 0;
    var window_function = try parseCreateRoutineCatalogTailAlloc(alloc, window_function_tokens.items, &window_function_pos);
    defer window_function.deinit(alloc);
    try std.testing.expectEqual(window_function_tokens.items.len, window_function_pos);
    try std.testing.expectEqual(RoutineKindSyntax.function, window_function.kind);
    try std.testing.expect(window_function.window);

    var support_function_tokens = try lexer.tokenizeAlloc(alloc, "FUNCTION normalize_status(input text) RETURNS text LANGUAGE sql SUPPORT audit_support;");
    defer lexer.freeTokens(alloc, &support_function_tokens);
    var support_function_pos: usize = 0;
    var support_function = try parseCreateRoutineCatalogTailAlloc(alloc, support_function_tokens.items, &support_function_pos);
    defer support_function.deinit(alloc);
    try std.testing.expectEqual(support_function_tokens.items.len, support_function_pos);
    try std.testing.expectEqual(RoutineKindSyntax.function, support_function.kind);
    try std.testing.expectEqualStrings("audit_support", support_function.support_function.?);

    var transform_function_tokens = try lexer.tokenizeAlloc(alloc, "FUNCTION normalize_status(input text) RETURNS text LANGUAGE sql TRANSFORM FOR TYPE jsonb, public.hstore;");
    defer lexer.freeTokens(alloc, &transform_function_tokens);
    var transform_function_pos: usize = 0;
    var transform_function = try parseCreateRoutineCatalogTailAlloc(alloc, transform_function_tokens.items, &transform_function_pos);
    defer transform_function.deinit(alloc);
    try std.testing.expectEqual(transform_function_tokens.items.len, transform_function_pos);
    try std.testing.expectEqual(RoutineKindSyntax.function, transform_function.kind);
    try std.testing.expectEqual(@as(usize, 2), transform_function.transform_types.len);
    try std.testing.expectEqualStrings("jsonb", transform_function.transform_types[0]);
    try std.testing.expectEqualStrings("hstore", transform_function.transform_types[1]);

    var setting_function_tokens = try lexer.tokenizeAlloc(alloc, "FUNCTION normalize_status(input text) RETURNS text LANGUAGE sql SET search_path TO public;");
    defer lexer.freeTokens(alloc, &setting_function_tokens);
    var setting_function_pos: usize = 0;
    var setting_function = try parseCreateRoutineCatalogTailAlloc(alloc, setting_function_tokens.items, &setting_function_pos);
    defer setting_function.deinit(alloc);
    try std.testing.expectEqual(setting_function_tokens.items.len, setting_function_pos);
    try std.testing.expectEqual(RoutineKindSyntax.function, setting_function.kind);
    try std.testing.expectEqual(@as(usize, 1), setting_function.settings.len);
    try std.testing.expectEqualStrings("search_path", setting_function.settings[0].name);
    try std.testing.expectEqual(@as(usize, 1), setting_function.settings[0].values.len);
    try std.testing.expectEqualStrings("public", setting_function.settings[0].values[0]);
    try std.testing.expect(!setting_function.settings[0].from_current);

    var current_setting_function_tokens = try lexer.tokenizeAlloc(alloc, "FUNCTION normalize_status(input text) RETURNS text LANGUAGE sql SET work_mem FROM CURRENT;");
    defer lexer.freeTokens(alloc, &current_setting_function_tokens);
    var current_setting_function_pos: usize = 0;
    var current_setting_function = try parseCreateRoutineCatalogTailAlloc(alloc, current_setting_function_tokens.items, &current_setting_function_pos);
    defer current_setting_function.deinit(alloc);
    try std.testing.expectEqual(current_setting_function_tokens.items.len, current_setting_function_pos);
    try std.testing.expectEqual(@as(usize, 1), current_setting_function.settings.len);
    try std.testing.expectEqualStrings("work_mem", current_setting_function.settings[0].name);
    try std.testing.expectEqual(@as(usize, 0), current_setting_function.settings[0].values.len);
    try std.testing.expect(current_setting_function.settings[0].from_current);

    var second_arg_body_tokens = try lexer.tokenizeAlloc(alloc, "FUNCTION second_arg(text, text) RETURNS text LANGUAGE sql AS 'SELECT upper($2)';");
    defer lexer.freeTokens(alloc, &second_arg_body_tokens);
    var second_arg_body_pos: usize = 0;
    var second_arg_body = try parseCreateRoutineCatalogTailAlloc(alloc, second_arg_body_tokens.items, &second_arg_body_pos);
    defer second_arg_body.deinit(alloc);
    try std.testing.expectEqual(second_arg_body_tokens.items.len, second_arg_body_pos);
    try std.testing.expectEqual(@as(usize, 2), second_arg_body.argument_count);
    try std.testing.expectEqual(ddl_plan.RoutineBodyKind.sql_expression, second_arg_body.body.?.kind);
    try std.testing.expectEqual(runtime_schema.RelationalRowsExpressionKind.upper, second_arg_body.body.?.expression.?.kind);
    try std.testing.expectEqualStrings("arg2", second_arg_body.body.?.expression.?.operands[0].field);

    var cast_arg_body_tokens = try lexer.tokenizeAlloc(alloc, "FUNCTION normalize_dash(text) RETURNS text LANGUAGE sql AS 'SELECT replace($1::text, ''-'', ''_'')';");
    defer lexer.freeTokens(alloc, &cast_arg_body_tokens);
    var cast_arg_body_pos: usize = 0;
    var cast_arg_body = try parseCreateRoutineCatalogTailAlloc(alloc, cast_arg_body_tokens.items, &cast_arg_body_pos);
    defer cast_arg_body.deinit(alloc);
    try std.testing.expectEqual(cast_arg_body_tokens.items.len, cast_arg_body_pos);
    try std.testing.expectEqual(runtime_schema.RelationalRowsExpressionKind.replace, cast_arg_body.body.?.expression.?.kind);
    try std.testing.expectEqual(runtime_schema.RelationalRowsExpressionKind.cast, cast_arg_body.body.?.expression.?.operands[0].kind);
    try std.testing.expectEqual(runtime_schema.RelationalRowsExpressionCastType.text, cast_arg_body.body.?.expression.?.operands[0].cast_type.?);
    try std.testing.expectEqual(runtime_schema.RelationalRowsExpressionFieldSource.source, cast_arg_body.body.?.expression.?.operands[0].operands[0].field_source);
    try std.testing.expectEqualStrings("arg1", cast_arg_body.body.?.expression.?.operands[0].operands[0].field);

    var regexp_arg_body_tokens = try lexer.tokenizeAlloc(alloc, "FUNCTION redact_digits(text) RETURNS text LANGUAGE sql AS 'SELECT regexp_replace($1, ''[0-9]'', ''#'', ''g'')';");
    defer lexer.freeTokens(alloc, &regexp_arg_body_tokens);
    var regexp_arg_body_pos: usize = 0;
    var regexp_arg_body = try parseCreateRoutineCatalogTailAlloc(alloc, regexp_arg_body_tokens.items, &regexp_arg_body_pos);
    defer regexp_arg_body.deinit(alloc);
    try std.testing.expectEqual(regexp_arg_body_tokens.items.len, regexp_arg_body_pos);
    try std.testing.expectEqual(runtime_schema.RelationalRowsExpressionKind.regexp_replace, regexp_arg_body.body.?.expression.?.kind);
    try std.testing.expectEqualStrings("arg1", regexp_arg_body.body.?.expression.?.operands[0].field);

    var missing_arg_body_tokens = try lexer.tokenizeAlloc(alloc, "FUNCTION missing_arg(text) RETURNS text LANGUAGE sql AS 'SELECT lower($2)';");
    defer lexer.freeTokens(alloc, &missing_arg_body_tokens);
    var missing_arg_body_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseCreateRoutineCatalogTailAlloc(alloc, missing_arg_body_tokens.items, &missing_arg_body_pos));

    var add_body_tokens = try lexer.tokenizeAlloc(alloc, "FUNCTION add_amounts(numeric, numeric) RETURNS numeric LANGUAGE sql AS 'SELECT $1+$2';");
    defer lexer.freeTokens(alloc, &add_body_tokens);
    var add_body_pos: usize = 0;
    var add_body = try parseCreateRoutineCatalogTailAlloc(alloc, add_body_tokens.items, &add_body_pos);
    defer add_body.deinit(alloc);
    try std.testing.expectEqual(add_body_tokens.items.len, add_body_pos);
    try std.testing.expectEqual(runtime_schema.RelationalRowsExpressionKind.add, add_body.body.?.expression.?.kind);
    try std.testing.expectEqualStrings("arg1", add_body.body.?.expression.?.operands[0].field);
    try std.testing.expectEqualStrings("arg2", add_body.body.?.expression.?.operands[1].field);

    var literal_add_body_tokens = try lexer.tokenizeAlloc(alloc, "FUNCTION shift_amount(numeric) RETURNS numeric LANGUAGE sql AS 'SELECT 1 + arg1';");
    defer lexer.freeTokens(alloc, &literal_add_body_tokens);
    var literal_add_body_pos: usize = 0;
    var literal_add_body = try parseCreateRoutineCatalogTailAlloc(alloc, literal_add_body_tokens.items, &literal_add_body_pos);
    defer literal_add_body.deinit(alloc);
    try std.testing.expectEqual(literal_add_body_tokens.items.len, literal_add_body_pos);
    try std.testing.expectEqual(runtime_schema.RelationalRowsExpressionKind.add, literal_add_body.body.?.expression.?.kind);
    try std.testing.expectEqualStrings("1", literal_add_body.body.?.expression.?.operands[0].value_json);
    try std.testing.expectEqualStrings("arg1", literal_add_body.body.?.expression.?.operands[1].field);

    var concat_body_tokens = try lexer.tokenizeAlloc(alloc, "FUNCTION status_label(text, text) RETURNS text LANGUAGE sql AS 'SELECT concat_ws('' '', $1, $2)';");
    defer lexer.freeTokens(alloc, &concat_body_tokens);
    var concat_body_pos: usize = 0;
    var concat_body = try parseCreateRoutineCatalogTailAlloc(alloc, concat_body_tokens.items, &concat_body_pos);
    defer concat_body.deinit(alloc);
    try std.testing.expectEqual(concat_body_tokens.items.len, concat_body_pos);
    try std.testing.expectEqual(runtime_schema.RelationalRowsExpressionKind.concat_ws, concat_body.body.?.expression.?.kind);
    try std.testing.expectEqualStrings("\" \"", concat_body.body.?.expression.?.operands[0].value_json);
    try std.testing.expectEqualStrings("arg1", concat_body.body.?.expression.?.operands[1].field);
    try std.testing.expectEqualStrings("arg2", concat_body.body.?.expression.?.operands[2].field);

    var coalesce_body_tokens = try lexer.tokenizeAlloc(alloc, "FUNCTION status_or_fallback(text) RETURNS text LANGUAGE sql AS 'SELECT coalesce($1, ''missing'')';");
    defer lexer.freeTokens(alloc, &coalesce_body_tokens);
    var coalesce_body_pos: usize = 0;
    var coalesce_body = try parseCreateRoutineCatalogTailAlloc(alloc, coalesce_body_tokens.items, &coalesce_body_pos);
    defer coalesce_body.deinit(alloc);
    try std.testing.expectEqual(coalesce_body_tokens.items.len, coalesce_body_pos);
    try std.testing.expectEqual(runtime_schema.RelationalRowsExpressionKind.coalesce, coalesce_body.body.?.expression.?.kind);
    try std.testing.expectEqualStrings("arg1", coalesce_body.body.?.expression.?.operands[0].field);
    try std.testing.expectEqualStrings("\"missing\"", coalesce_body.body.?.expression.?.operands[1].value_json);

    var nested_body_tokens = try lexer.tokenizeAlloc(alloc, "FUNCTION status_label_nested(text, text) RETURNS text LANGUAGE sql AS 'SELECT concat_ws('':'', lower($1), coalesce($2, ''missing''))';");
    defer lexer.freeTokens(alloc, &nested_body_tokens);
    var nested_body_pos: usize = 0;
    var nested_body = try parseCreateRoutineCatalogTailAlloc(alloc, nested_body_tokens.items, &nested_body_pos);
    defer nested_body.deinit(alloc);
    try std.testing.expectEqual(nested_body_tokens.items.len, nested_body_pos);
    try std.testing.expectEqual(runtime_schema.RelationalRowsExpressionKind.concat_ws, nested_body.body.?.expression.?.kind);
    try std.testing.expectEqualStrings("\":\"", nested_body.body.?.expression.?.operands[0].value_json);
    try std.testing.expectEqual(runtime_schema.RelationalRowsExpressionKind.lower, nested_body.body.?.expression.?.operands[1].kind);
    try std.testing.expectEqualStrings("arg1", nested_body.body.?.expression.?.operands[1].operands[0].field);
    try std.testing.expectEqual(runtime_schema.RelationalRowsExpressionKind.coalesce, nested_body.body.?.expression.?.operands[2].kind);
    try std.testing.expectEqualStrings("arg2", nested_body.body.?.expression.?.operands[2].operands[0].field);
    try std.testing.expectEqualStrings("\"missing\"", nested_body.body.?.expression.?.operands[2].operands[1].value_json);

    var create_procedure_tokens = try lexer.tokenizeAlloc(alloc, "PROCEDURE touch_usage(id text) LANGUAGE sql;");
    defer lexer.freeTokens(alloc, &create_procedure_tokens);
    var create_procedure_pos: usize = 0;
    var create_procedure = try parseCreateRoutineCatalogTailAlloc(alloc, create_procedure_tokens.items, &create_procedure_pos);
    defer create_procedure.deinit(alloc);
    try std.testing.expectEqual(create_procedure_tokens.items.len, create_procedure_pos);
    try std.testing.expectEqual(RoutineKindSyntax.procedure, create_procedure.kind);
    try std.testing.expectEqualStrings("touch_usage", create_procedure.routine_name);
    try std.testing.expectEqual(@as(usize, 1), create_procedure.argument_count);
    try std.testing.expect(create_procedure.returns_type == null);
    try std.testing.expectEqualStrings("sql", create_procedure.language.?);

    var noop_procedure_body_tokens = try lexer.tokenizeAlloc(alloc, "PROCEDURE rotate_usage() LANGUAGE plpgsql AS 'BEGIN NULL; END';");
    defer lexer.freeTokens(alloc, &noop_procedure_body_tokens);
    var noop_procedure_body_pos: usize = 0;
    var noop_procedure_body = try parseCreateRoutineCatalogTailAlloc(alloc, noop_procedure_body_tokens.items, &noop_procedure_body_pos);
    defer noop_procedure_body.deinit(alloc);
    try std.testing.expectEqual(noop_procedure_body_tokens.items.len, noop_procedure_body_pos);
    try std.testing.expectEqual(RoutineKindSyntax.procedure, noop_procedure_body.kind);
    try std.testing.expectEqual(ddl_plan.RoutineBodyKind.plpgsql_procedure, noop_procedure_body.body.?.kind);
    try std.testing.expectEqual(ddl_plan.RoutineExecutionHook.procedure_noop, noop_procedure_body.body.?.hook);
    try std.testing.expect(noop_procedure_body.body.?.expression == null);

    var notice_procedure_body_tokens = try lexer.tokenizeAlloc(alloc, "PROCEDURE rotate_usage_notice() LANGUAGE plpgsql AS 'BEGIN RAISE NOTICE ''rotate''; END';");
    defer lexer.freeTokens(alloc, &notice_procedure_body_tokens);
    var notice_procedure_body_pos: usize = 0;
    var notice_procedure_body = try parseCreateRoutineCatalogTailAlloc(alloc, notice_procedure_body_tokens.items, &notice_procedure_body_pos);
    defer notice_procedure_body.deinit(alloc);
    try std.testing.expectEqual(notice_procedure_body_tokens.items.len, notice_procedure_body_pos);
    try std.testing.expectEqual(RoutineKindSyntax.procedure, notice_procedure_body.kind);
    try std.testing.expectEqual(ddl_plan.RoutineBodyKind.plpgsql_procedure, notice_procedure_body.body.?.kind);
    try std.testing.expectEqual(ddl_plan.RoutineExecutionHook.procedure_noop, notice_procedure_body.body.?.hook);
    try std.testing.expect(notice_procedure_body.body.?.expression == null);

    var drop_function_tokens = try lexer.tokenizeAlloc(alloc, "FUNCTION IF EXISTS public.normalize_status(text) CASCADE;");
    defer lexer.freeTokens(alloc, &drop_function_tokens);
    var drop_function_pos: usize = 0;
    var drop_function = try parseDropRoutineCatalogTailAlloc(alloc, drop_function_tokens.items, &drop_function_pos);
    defer drop_function.deinit(alloc);
    try std.testing.expectEqual(drop_function_tokens.items.len, drop_function_pos);
    try std.testing.expectEqual(RoutineKindSyntax.function, drop_function.kind);
    try std.testing.expectEqualStrings("normalize_status", drop_function.routine_name);
    try std.testing.expect(drop_function.if_exists);
    try std.testing.expectEqual(@as(usize, 1), drop_function.argument_count);
    try std.testing.expect(drop_function.cascade);

    var drop_procedure_tokens = try lexer.tokenizeAlloc(alloc, "PROCEDURE touch_usage RESTRICT;");
    defer lexer.freeTokens(alloc, &drop_procedure_tokens);
    var drop_procedure_pos: usize = 0;
    var drop_procedure = try parseDropRoutineCatalogTailAlloc(alloc, drop_procedure_tokens.items, &drop_procedure_pos);
    defer drop_procedure.deinit(alloc);
    try std.testing.expectEqual(drop_procedure_tokens.items.len, drop_procedure_pos);
    try std.testing.expectEqual(RoutineKindSyntax.procedure, drop_procedure.kind);
    try std.testing.expectEqualStrings("touch_usage", drop_procedure.routine_name);
    try std.testing.expectEqual(@as(usize, 0), drop_procedure.argument_count);
    try std.testing.expect(!drop_procedure.cascade);

    var unsupported_tokens = try lexer.tokenizeAlloc(alloc, "PROCEDURE touch_usage() RETURNS text LANGUAGE sql AS 'select 1';");
    defer lexer.freeTokens(alloc, &unsupported_tokens);
    var unsupported_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseCreateRoutineCatalogTailAlloc(alloc, unsupported_tokens.items, &unsupported_pos));

    var unsupported_sql_procedure_body_tokens = try lexer.tokenizeAlloc(alloc, "PROCEDURE touch_usage() LANGUAGE sql AS 'select 1';");
    defer lexer.freeTokens(alloc, &unsupported_sql_procedure_body_tokens);
    var unsupported_sql_procedure_body_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseCreateRoutineCatalogTailAlloc(alloc, unsupported_sql_procedure_body_tokens.items, &unsupported_sql_procedure_body_pos));

    var body_tokens = try lexer.tokenizeAlloc(alloc, "FUNCTION normalize_status(input text) RETURNS text LANGUAGE sql AS 'select lower(input)';");
    defer lexer.freeTokens(alloc, &body_tokens);
    var body_pos: usize = 0;
    var body = try parseCreateRoutineCatalogTailAlloc(alloc, body_tokens.items, &body_pos);
    defer body.deinit(alloc);
    try std.testing.expectEqual(body_tokens.items.len, body_pos);
    try std.testing.expectEqual(ddl_plan.RoutineBodyKind.sql_expression, body.body.?.kind);
    try std.testing.expectEqual(runtime_schema.RelationalRowsExpressionKind.lower, body.body.?.expression.?.kind);
    try std.testing.expectEqualStrings("arg1", body.body.?.expression.?.operands[0].field);

    var trigger_body_tokens = try lexer.tokenizeAlloc(alloc, "FUNCTION audit_body() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN NEW; END';");
    defer lexer.freeTokens(alloc, &trigger_body_tokens);
    var trigger_body_pos: usize = 0;
    var trigger_body = try parseCreateRoutineCatalogTailAlloc(alloc, trigger_body_tokens.items, &trigger_body_pos);
    defer trigger_body.deinit(alloc);
    try std.testing.expectEqual(trigger_body_tokens.items.len, trigger_body_pos);
    try std.testing.expectEqual(ddl_plan.RoutineBodyKind.plpgsql_trigger, trigger_body.body.?.kind);
    try std.testing.expectEqual(ddl_plan.RoutineExecutionHook.trigger_return_new, trigger_body.body.?.hook);
    try std.testing.expect(trigger_body.body.?.expression == null);

    var old_trigger_body_tokens = try lexer.tokenizeAlloc(alloc, "FUNCTION audit_old_body() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN OLD; END';");
    defer lexer.freeTokens(alloc, &old_trigger_body_tokens);
    var old_trigger_body_pos: usize = 0;
    var old_trigger_body = try parseCreateRoutineCatalogTailAlloc(alloc, old_trigger_body_tokens.items, &old_trigger_body_pos);
    defer old_trigger_body.deinit(alloc);
    try std.testing.expectEqual(old_trigger_body_tokens.items.len, old_trigger_body_pos);
    try std.testing.expectEqual(ddl_plan.RoutineBodyKind.plpgsql_trigger, old_trigger_body.body.?.kind);
    try std.testing.expectEqual(ddl_plan.RoutineExecutionHook.trigger_return_old, old_trigger_body.body.?.hook);
    try std.testing.expect(old_trigger_body.body.?.expression == null);

    var null_trigger_body_tokens = try lexer.tokenizeAlloc(alloc, "FUNCTION skip_audit_body() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN NULL; END';");
    defer lexer.freeTokens(alloc, &null_trigger_body_tokens);
    var null_trigger_body_pos: usize = 0;
    var null_trigger_body = try parseCreateRoutineCatalogTailAlloc(alloc, null_trigger_body_tokens.items, &null_trigger_body_pos);
    defer null_trigger_body.deinit(alloc);
    try std.testing.expectEqual(null_trigger_body_tokens.items.len, null_trigger_body_pos);
    try std.testing.expectEqual(ddl_plan.RoutineBodyKind.plpgsql_trigger, null_trigger_body.body.?.kind);
    try std.testing.expectEqual(ddl_plan.RoutineExecutionHook.trigger_return_null, null_trigger_body.body.?.hook);
    try std.testing.expect(null_trigger_body.body.?.expression == null);

    var notice_trigger_body_tokens = try lexer.tokenizeAlloc(alloc, "FUNCTION audit_notice_body() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RAISE NOTICE ''audit''; RETURN NEW; END';");
    defer lexer.freeTokens(alloc, &notice_trigger_body_tokens);
    var notice_trigger_body_pos: usize = 0;
    var notice_trigger_body = try parseCreateRoutineCatalogTailAlloc(alloc, notice_trigger_body_tokens.items, &notice_trigger_body_pos);
    defer notice_trigger_body.deinit(alloc);
    try std.testing.expectEqual(notice_trigger_body_tokens.items.len, notice_trigger_body_pos);
    try std.testing.expectEqual(ddl_plan.RoutineBodyKind.plpgsql_trigger, notice_trigger_body.body.?.kind);
    try std.testing.expectEqual(ddl_plan.RoutineExecutionHook.trigger_return_new, notice_trigger_body.body.?.hook);
    try std.testing.expect(notice_trigger_body.body.?.expression == null);

    var perform_trigger_body_tokens = try lexer.tokenizeAlloc(alloc, "FUNCTION audit_perform_body() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN PERFORM audit_log(); RETURN NEW; END';");
    defer lexer.freeTokens(alloc, &perform_trigger_body_tokens);
    var perform_trigger_body_pos: usize = 0;
    var perform_trigger_body = try parseCreateRoutineCatalogTailAlloc(alloc, perform_trigger_body_tokens.items, &perform_trigger_body_pos);
    defer perform_trigger_body.deinit(alloc);
    try std.testing.expectEqual(perform_trigger_body_tokens.items.len, perform_trigger_body_pos);
    try std.testing.expectEqual(ddl_plan.RoutineBodyKind.plpgsql_trigger, perform_trigger_body.body.?.kind);
    try std.testing.expectEqual(ddl_plan.RoutineExecutionHook.trigger_return_new, perform_trigger_body.body.?.hook);
    try std.testing.expectEqual(@as(usize, 1), perform_trigger_body.body.?.perform_calls.len);
    try std.testing.expectEqualStrings("audit_log", perform_trigger_body.body.?.perform_calls[0].routine_name);
    try std.testing.expectEqual(@as(usize, 0), perform_trigger_body.body.?.perform_calls[0].argument_json.len);

    var perform_procedure_body_tokens = try lexer.tokenizeAlloc(alloc, "PROCEDURE rotate_usage_perform() LANGUAGE plpgsql AS 'BEGIN PERFORM rotate_usage_now(); END';");
    defer lexer.freeTokens(alloc, &perform_procedure_body_tokens);
    var perform_procedure_body_pos: usize = 0;
    var perform_procedure_body = try parseCreateRoutineCatalogTailAlloc(alloc, perform_procedure_body_tokens.items, &perform_procedure_body_pos);
    defer perform_procedure_body.deinit(alloc);
    try std.testing.expectEqual(perform_procedure_body_tokens.items.len, perform_procedure_body_pos);
    try std.testing.expectEqual(ddl_plan.RoutineBodyKind.plpgsql_procedure, perform_procedure_body.body.?.kind);
    try std.testing.expectEqual(ddl_plan.RoutineExecutionHook.procedure_noop, perform_procedure_body.body.?.hook);
    try std.testing.expectEqual(@as(usize, 1), perform_procedure_body.body.?.perform_calls.len);
    try std.testing.expectEqualStrings("rotate_usage_now", perform_procedure_body.body.?.perform_calls[0].routine_name);
    try std.testing.expectEqual(@as(usize, 0), perform_procedure_body.body.?.perform_calls[0].argument_json.len);

    var perform_arg_body_tokens = try lexer.tokenizeAlloc(alloc, "FUNCTION audit_perform_arg_body() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN PERFORM audit_log(1); RETURN NEW; END';");
    defer lexer.freeTokens(alloc, &perform_arg_body_tokens);
    var perform_arg_body_pos: usize = 0;
    var perform_arg_body = try parseCreateRoutineCatalogTailAlloc(alloc, perform_arg_body_tokens.items, &perform_arg_body_pos);
    defer perform_arg_body.deinit(alloc);
    try std.testing.expectEqual(perform_arg_body_tokens.items.len, perform_arg_body_pos);
    try std.testing.expectEqual(ddl_plan.RoutineBodyKind.plpgsql_trigger, perform_arg_body.body.?.kind);
    try std.testing.expectEqual(ddl_plan.RoutineExecutionHook.trigger_return_new, perform_arg_body.body.?.hook);
    try std.testing.expectEqual(@as(usize, 1), perform_arg_body.body.?.perform_calls.len);
    try std.testing.expectEqualStrings("audit_log", perform_arg_body.body.?.perform_calls[0].routine_name);
    try std.testing.expectEqual(@as(usize, 1), perform_arg_body.body.?.perform_calls[0].argument_json.len);
    try std.testing.expectEqualStrings("1", perform_arg_body.body.?.perform_calls[0].argument_json[0]);

    var security_tokens = try lexer.tokenizeAlloc(alloc, "FUNCTION normalize_status(input text) RETURNS text LANGUAGE sql SECURITY DEFINER;");
    defer lexer.freeTokens(alloc, &security_tokens);
    var security_pos: usize = 0;
    var security = try parseCreateRoutineCatalogTailAlloc(alloc, security_tokens.items, &security_pos);
    defer security.deinit(alloc);
    try std.testing.expectEqual(security_tokens.items.len, security_pos);
    try std.testing.expectEqual(ddl_plan.RoutineSecurity.definer, security.security.?);

    var external_security_tokens = try lexer.tokenizeAlloc(alloc, "FUNCTION normalize_status(input text) RETURNS text LANGUAGE sql EXTERNAL SECURITY DEFINER;");
    defer lexer.freeTokens(alloc, &external_security_tokens);
    var external_security_pos: usize = 0;
    var external_security = try parseCreateRoutineCatalogTailAlloc(alloc, external_security_tokens.items, &external_security_pos);
    defer external_security.deinit(alloc);
    try std.testing.expectEqual(external_security_tokens.items.len, external_security_pos);
    try std.testing.expectEqual(ddl_plan.RoutineSecurity.definer, external_security.security.?);

    var option_tokens = try lexer.tokenizeAlloc(alloc, "FUNCTION normalize_status(input text) RETURNS text LANGUAGE sql SUPPORT audit_support SUPPORT audit_support;");
    defer lexer.freeTokens(alloc, &option_tokens);
    var option_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseCreateRoutineCatalogTailAlloc(alloc, option_tokens.items, &option_pos));
}

test "sql adapter grammar parses sequence catalog tails" {
    const alloc = std.testing.allocator;

    var create_tokens = try lexer.tokenizeAlloc(alloc, "SEQUENCE IF NOT EXISTS public.order_id_seq AS bigint START WITH 10 INCREMENT BY 2 MINVALUE 1 MAXVALUE 99 CACHE 5 CYCLE OWNED BY public.orders.id;");
    defer lexer.freeTokens(alloc, &create_tokens);
    var create_pos: usize = 0;
    var create = try parseCreateSequenceCatalogTailAlloc(alloc, create_tokens.items, &create_pos);
    defer create.deinit(alloc);
    try std.testing.expectEqual(create_tokens.items.len, create_pos);
    try std.testing.expectEqualStrings("order_id_seq", create.sequence_name);
    try std.testing.expect(create.if_not_exists);
    try std.testing.expectEqualStrings("bigint", create.options.as_type.?);
    try std.testing.expectEqual(@as(i64, 10), create.options.start_with.?);
    try std.testing.expectEqual(@as(i64, 2), create.options.increment_by.?);
    try std.testing.expect(create.options.min_value_specified);
    try std.testing.expectEqual(@as(i64, 1), create.options.min_value.?);
    try std.testing.expect(create.options.max_value_specified);
    try std.testing.expectEqual(@as(i64, 99), create.options.max_value.?);
    try std.testing.expectEqual(@as(i64, 5), create.options.cache.?);
    try std.testing.expectEqual(true, create.options.cycle.?);
    try std.testing.expectEqualStrings("orders", create.options.owned_by.?.table_name);
    try std.testing.expectEqualStrings("id", create.options.owned_by.?.column_name);

    var alter_tokens = try lexer.tokenizeAlloc(alloc, "SEQUENCE IF EXISTS order_id_seq RESTART WITH 1000 INCREMENT BY -5 NO CYCLE;");
    defer lexer.freeTokens(alloc, &alter_tokens);
    var alter_pos: usize = 0;
    var alter = try parseAlterSequenceCatalogTailAlloc(alloc, alter_tokens.items, &alter_pos);
    defer alter.deinit(alloc);
    try std.testing.expectEqual(alter_tokens.items.len, alter_pos);
    try std.testing.expectEqualStrings("order_id_seq", alter.sequence_name);
    try std.testing.expect(alter.if_exists);
    try std.testing.expectEqual(@as(usize, 3), alter.operations.len);
    switch (alter.operations[0]) {
        .restart => |value| try std.testing.expectEqual(@as(i64, 1000), value.?),
        else => return error.TestExpectedEqual,
    }
    switch (alter.operations[1]) {
        .set_increment => |value| try std.testing.expectEqual(@as(i64, -5), value),
        else => return error.TestExpectedEqual,
    }
    switch (alter.operations[2]) {
        .set_cycle => |value| try std.testing.expectEqual(false, value),
        else => return error.TestExpectedEqual,
    }

    var drop_tokens = try lexer.tokenizeAlloc(alloc, "SEQUENCE IF EXISTS public.order_id_seq CASCADE;");
    defer lexer.freeTokens(alloc, &drop_tokens);
    var drop_pos: usize = 0;
    var drop = try parseDropSequenceCatalogTailAlloc(alloc, drop_tokens.items, &drop_pos);
    defer drop.deinit(alloc);
    try std.testing.expectEqual(drop_tokens.items.len, drop_pos);
    try std.testing.expectEqualStrings("order_id_seq", drop.sequence_name);
    try std.testing.expect(drop.if_exists);
    try std.testing.expect(drop.cascade);

    var duplicate_tokens = try lexer.tokenizeAlloc(alloc, "SEQUENCE order_id_seq START 1 START 2;");
    defer lexer.freeTokens(alloc, &duplicate_tokens);
    var duplicate_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseCreateSequenceCatalogTailAlloc(alloc, duplicate_tokens.items, &duplicate_pos));
}

test "sql adapter grammar parses enum type catalog tails" {
    const alloc = std.testing.allocator;

    var create_tokens = try lexer.tokenizeAlloc(alloc, "TYPE public.order_status AS ENUM ('new', 'paid', 'shipped');");
    defer lexer.freeTokens(alloc, &create_tokens);
    var create_pos: usize = 0;
    var create = try parseCreateEnumTypeCatalogTailAlloc(alloc, create_tokens.items, &create_pos);
    defer create.deinit(alloc);
    try std.testing.expectEqual(create_tokens.items.len, create_pos);
    try std.testing.expectEqualStrings("order_status", create.type_name);
    try std.testing.expectEqual(@as(usize, 3), create.values.len);
    try std.testing.expectEqualStrings("new", create.values[0]);
    try std.testing.expectEqualStrings("paid", create.values[1]);
    try std.testing.expectEqualStrings("shipped", create.values[2]);

    var alter_tokens = try lexer.tokenizeAlloc(alloc, "TYPE order_status ADD VALUE IF NOT EXISTS 'returned' AFTER 'shipped';");
    defer lexer.freeTokens(alloc, &alter_tokens);
    var alter_pos: usize = 0;
    var alter = try parseAlterEnumTypeCatalogTailAlloc(alloc, alter_tokens.items, &alter_pos);
    defer alter.deinit(alloc);
    try std.testing.expectEqual(alter_tokens.items.len, alter_pos);
    try std.testing.expectEqualStrings("order_status", alter.type_name);
    try std.testing.expectEqualStrings("returned", alter.value);
    try std.testing.expect(alter.if_not_exists);
    try std.testing.expectEqual(ddl_plan.EnumValuePosition.after, alter.position);
    try std.testing.expectEqualStrings("shipped", alter.neighbor_value.?);

    var drop_tokens = try lexer.tokenizeAlloc(alloc, "TYPE IF EXISTS public.order_status RESTRICT;");
    defer lexer.freeTokens(alloc, &drop_tokens);
    var drop_pos: usize = 0;
    var drop = try parseDropEnumTypeCatalogTailAlloc(alloc, drop_tokens.items, &drop_pos);
    defer drop.deinit(alloc);
    try std.testing.expectEqual(drop_tokens.items.len, drop_pos);
    try std.testing.expectEqualStrings("order_status", drop.type_name);
    try std.testing.expect(drop.if_exists);
    try std.testing.expect(!drop.cascade);

    var duplicate_tokens = try lexer.tokenizeAlloc(alloc, "TYPE order_status AS ENUM ('new', 'new');");
    defer lexer.freeTokens(alloc, &duplicate_tokens);
    var duplicate_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseCreateEnumTypeCatalogTailAlloc(alloc, duplicate_tokens.items, &duplicate_pos));
}

test "sql adapter grammar parses domain catalog tails" {
    const alloc = std.testing.allocator;

    var create_tokens = try lexer.tokenizeAlloc(alloc, "DOMAIN public.positive_amount AS numeric CHECK (VALUE > 0);");
    defer lexer.freeTokens(alloc, &create_tokens);
    var create_pos: usize = 0;
    var create = try parseCreateDomainHeaderAlloc(alloc, create_tokens.items, &create_pos);
    defer create.deinit(alloc);
    try std.testing.expectEqualStrings("positive_amount", create.domain_name);
    try std.testing.expect(std.ascii.eqlIgnoreCase(create_tokens.items[create_pos].text, "numeric"));

    var alter_tokens = try lexer.tokenizeAlloc(alloc, "DOMAIN public.positive_amount SET NOT NULL, SET DEFAULT 42, DROP DEFAULT;");
    defer lexer.freeTokens(alloc, &alter_tokens);
    var alter_pos: usize = 0;
    var alter = try parseAlterDomainCatalogTailAlloc(alloc, alter_tokens.items, &alter_pos);
    defer alter.deinit(alloc);
    try std.testing.expectEqual(alter_tokens.items.len, alter_pos);
    try std.testing.expectEqualStrings("positive_amount", alter.domain_name);
    try std.testing.expectEqual(@as(usize, 3), alter.operations.len);
    switch (alter.operations[0]) {
        .set_not_null => {},
        else => return error.TestExpectedEqual,
    }
    switch (alter.operations[1]) {
        .set_default => |default| {
            try std.testing.expectEqual(runtime_schema.RelationalDefaultKind.literal, default.kind);
            try std.testing.expectEqualStrings("42", default.value_json);
        },
        else => return error.TestExpectedEqual,
    }
    switch (alter.operations[2]) {
        .drop_default => {},
        else => return error.TestExpectedEqual,
    }

    var drop_tokens = try lexer.tokenizeAlloc(alloc, "DOMAIN IF EXISTS public.positive_amount CASCADE;");
    defer lexer.freeTokens(alloc, &drop_tokens);
    var drop_pos: usize = 0;
    var drop = try parseDropDomainCatalogTailAlloc(alloc, drop_tokens.items, &drop_pos);
    defer drop.deinit(alloc);
    try std.testing.expectEqual(drop_tokens.items.len, drop_pos);
    try std.testing.expectEqualStrings("positive_amount", drop.domain_name);
    try std.testing.expect(drop.if_exists);
    try std.testing.expect(drop.cascade);

    var multi_tokens = try lexer.tokenizeAlloc(alloc, "DOMAIN amount_domain, other_domain;");
    defer lexer.freeTokens(alloc, &multi_tokens);
    var multi_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseDropDomainCatalogTailAlloc(alloc, multi_tokens.items, &multi_pos));

    var if_not_exists_tokens = try lexer.tokenizeAlloc(alloc, "DOMAIN IF NOT EXISTS positive_amount AS numeric;");
    defer lexer.freeTokens(alloc, &if_not_exists_tokens);
    var if_not_exists_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseCreateDomainHeaderAlloc(alloc, if_not_exists_tokens.items, &if_not_exists_pos));
}

test "sql adapter grammar parses comment metadata catalog tails" {
    const alloc = std.testing.allocator;

    var table_tokens = try lexer.tokenizeAlloc(alloc, "ON TABLE public.usage_records IS 'metered usage rows';");
    defer lexer.freeTokens(alloc, &table_tokens);
    var table_pos: usize = 0;
    var table = try parseCommentMetadataCatalogTailAlloc(alloc, table_tokens.items, &table_pos);
    defer table.deinit(alloc);
    try std.testing.expectEqual(table_tokens.items.len, table_pos);
    try std.testing.expectEqual(ddl_plan.CommentMetadataTarget.table, table.target);
    try std.testing.expectEqualStrings("usage_records", table.object_name);
    try std.testing.expect(table.parent_table_name == null);
    try std.testing.expectEqualStrings("\"metered usage rows\"", table.comment_json.?);

    var column_tokens = try lexer.tokenizeAlloc(alloc, "ON COLUMN usage_records.updated_at_ns IS NULL;");
    defer lexer.freeTokens(alloc, &column_tokens);
    var column_pos: usize = 0;
    var column = try parseCommentMetadataCatalogTailAlloc(alloc, column_tokens.items, &column_pos);
    defer column.deinit(alloc);
    try std.testing.expectEqual(column_tokens.items.len, column_pos);
    try std.testing.expectEqual(ddl_plan.CommentMetadataTarget.column, column.target);
    try std.testing.expectEqualStrings("usage_records.updated_at_ns", column.object_name);
    try std.testing.expect(column.comment_json == null);

    var constraint_tokens = try lexer.tokenizeAlloc(alloc, "ON CONSTRAINT usage_records_updated_check ON public.usage_records IS 'valid update clock';");
    defer lexer.freeTokens(alloc, &constraint_tokens);
    var constraint_pos: usize = 0;
    var constraint = try parseCommentMetadataCatalogTailAlloc(alloc, constraint_tokens.items, &constraint_pos);
    defer constraint.deinit(alloc);
    try std.testing.expectEqual(constraint_tokens.items.len, constraint_pos);
    try std.testing.expectEqual(ddl_plan.CommentMetadataTarget.constraint, constraint.target);
    try std.testing.expectEqualStrings("usage_records_updated_check", constraint.object_name);
    try std.testing.expectEqualStrings("usage_records", constraint.parent_table_name.?);
    try std.testing.expectEqualStrings("\"valid update clock\"", constraint.comment_json.?);

    var unsupported_tokens = try lexer.tokenizeAlloc(alloc, "ON SEQUENCE usage_records_id_seq IS 'unsupported';");
    defer lexer.freeTokens(alloc, &unsupported_tokens);
    var unsupported_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseCommentMetadataCatalogTailAlloc(alloc, unsupported_tokens.items, &unsupported_pos));
}

test "sql adapter grammar parses drop table and index catalog tails" {
    const alloc = std.testing.allocator;

    var drop_table_tokens = try lexer.tokenizeAlloc(alloc, "TABLE IF EXISTS ONLY public.usage_records CASCADE;");
    defer lexer.freeTokens(alloc, &drop_table_tokens);
    var drop_table_pos: usize = 0;
    var drop_table = try parseDropTableCatalogTailAlloc(alloc, drop_table_tokens.items, &drop_table_pos);
    defer drop_table.deinit(alloc);
    try std.testing.expectEqual(drop_table_tokens.items.len, drop_table_pos);
    try std.testing.expectEqualStrings("usage_records", drop_table.table_name);
    try std.testing.expect(drop_table.if_exists);
    try std.testing.expect(drop_table.cascade);

    var drop_index_tokens = try lexer.tokenizeAlloc(alloc, "INDEX CONCURRENTLY IF EXISTS public.usage_records_status_idx RESTRICT;");
    defer lexer.freeTokens(alloc, &drop_index_tokens);
    var drop_index_pos: usize = 0;
    var drop_index = try parseDropIndexCatalogTailAlloc(alloc, drop_index_tokens.items, &drop_index_pos);
    defer drop_index.deinit(alloc);
    try std.testing.expectEqual(drop_index_tokens.items.len, drop_index_pos);
    try std.testing.expectEqualStrings("usage_records_status_idx", drop_index.index_name);
    try std.testing.expect(drop_index.if_exists);

    var multi_table_tokens = try lexer.tokenizeAlloc(alloc, "TABLE usage_records, usage_archives;");
    defer lexer.freeTokens(alloc, &multi_table_tokens);
    var multi_table_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseDropTableCatalogTailAlloc(alloc, multi_table_tokens.items, &multi_table_pos));
}

test "sql adapter grammar parses create index headers" {
    const alloc = std.testing.allocator;

    var gin_tokens = try lexer.tokenizeAlloc(alloc, "INDEX CONCURRENTLY IF NOT EXISTS public.usage_records_status_idx ON ONLY public.usage_records USING gin (metadata jsonb_path_ops);");
    defer lexer.freeTokens(alloc, &gin_tokens);
    var gin_pos: usize = 0;
    var gin = try parseCreateIndexHeaderAlloc(alloc, gin_tokens.items, &gin_pos);
    defer gin.deinit(alloc);
    try std.testing.expectEqualStrings("usage_records_status_idx", gin.index_name);
    try std.testing.expectEqualStrings("usage_records", gin.table_name);
    try std.testing.expect(gin.if_not_exists);
    try std.testing.expectEqual(ddl_plan.DdlIndexMethod.gin, gin.method);
    try std.testing.expect(gin_tokens.items[gin_pos].kind == .lparen);

    var btree_tokens = try lexer.tokenizeAlloc(alloc, "INDEX usage_records_status_idx ON usage_records (status);");
    defer lexer.freeTokens(alloc, &btree_tokens);
    var btree_pos: usize = 0;
    var btree = try parseCreateIndexHeaderAlloc(alloc, btree_tokens.items, &btree_pos);
    defer btree.deinit(alloc);
    try std.testing.expectEqualStrings("usage_records_status_idx", btree.index_name);
    try std.testing.expectEqualStrings("usage_records", btree.table_name);
    try std.testing.expect(!btree.if_not_exists);
    try std.testing.expectEqual(ddl_plan.DdlIndexMethod.btree, btree.method);
    try std.testing.expect(btree_tokens.items[btree_pos].kind == .lparen);

    var unsupported_tokens = try lexer.tokenizeAlloc(alloc, "INDEX usage_records_status_idx ON usage_records USING gist (status);");
    defer lexer.freeTokens(alloc, &unsupported_tokens);
    var unsupported_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseCreateIndexHeaderAlloc(alloc, unsupported_tokens.items, &unsupported_pos));

    var opclass_tokens = try lexer.tokenizeAlloc(alloc, "metadata jsonb_path_ops)");
    defer lexer.freeTokens(alloc, &opclass_tokens);
    var opclass_pos: usize = 1;
    const opclass = try parseCreateIndexElementSuffix(opclass_tokens.items, &opclass_pos, .gin, true);
    try std.testing.expectEqual(ddl_plan.DdlIndexOpClass.jsonb_path_ops, opclass.opclass);
    try std.testing.expectEqual(CreateIndexElementOrderDirectionSyntax.default, opclass.order_direction);
    try std.testing.expectEqual(CreateIndexElementNullsOrderSyntax.default, opclass.nulls_order);
    try std.testing.expect(opclass_tokens.items[opclass_pos].kind == .rparen);

    var ordered_tokens = try lexer.tokenizeAlloc(alloc, "status DESC NULLS LAST,");
    defer lexer.freeTokens(alloc, &ordered_tokens);
    var ordered_pos: usize = 1;
    const ordered = try parseCreateIndexElementSuffix(ordered_tokens.items, &ordered_pos, .btree, true);
    try std.testing.expectEqual(ddl_plan.DdlIndexOpClass.default, ordered.opclass);
    try std.testing.expectEqual(CreateIndexElementOrderDirectionSyntax.desc, ordered.order_direction);
    try std.testing.expectEqual(CreateIndexElementNullsOrderSyntax.last, ordered.nulls_order);
    try std.testing.expect(ordered_tokens.items[ordered_pos].kind == .comma);

    var invalid_nulls_tokens = try lexer.tokenizeAlloc(alloc, "status NULLS MIDDLE)");
    defer lexer.freeTokens(alloc, &invalid_nulls_tokens);
    var invalid_nulls_pos: usize = 1;
    try std.testing.expectError(error.UnsupportedSqlShape, parseCreateIndexElementSuffix(invalid_nulls_tokens.items, &invalid_nulls_pos, .btree, true));

    var invalid_opclass_tokens = try lexer.tokenizeAlloc(alloc, "metadata ASC)");
    defer lexer.freeTokens(alloc, &invalid_opclass_tokens);
    var invalid_opclass_pos: usize = 1;
    try std.testing.expectError(error.UnsupportedSqlShape, parseCreateIndexElementSuffix(invalid_opclass_tokens.items, &invalid_opclass_pos, .gin, true));
}

test "sql adapter grammar parses ddl constraint suffixes" {
    const alloc = std.testing.allocator;

    var not_valid_tokens = try lexer.tokenizeAlloc(alloc, "NOT VALID,");
    defer lexer.freeTokens(alloc, &not_valid_tokens);
    var not_valid_pos: usize = 0;
    try std.testing.expect(peekDdlNotValid(not_valid_tokens.items, not_valid_pos));
    try std.testing.expect(consumeOptionalDdlNotValid(not_valid_tokens.items, &not_valid_pos));
    try std.testing.expect(not_valid_tokens.items[not_valid_pos].kind == .comma);

    var distinct_tokens = try lexer.tokenizeAlloc(alloc, "NULLS DISTINCT (tenant_id)");
    defer lexer.freeTokens(alloc, &distinct_tokens);
    var distinct_pos: usize = 0;
    try std.testing.expectEqual(false, (try parseOptionalDdlUniqueNullsDistinct(distinct_tokens.items, &distinct_pos)).?);
    try std.testing.expect(distinct_tokens.items[distinct_pos].kind == .lparen);

    var not_distinct_tokens = try lexer.tokenizeAlloc(alloc, "NULLS NOT DISTINCT (tenant_id)");
    defer lexer.freeTokens(alloc, &not_distinct_tokens);
    var not_distinct_pos: usize = 0;
    try std.testing.expectEqual(true, (try parseOptionalDdlUniqueNullsDistinct(not_distinct_tokens.items, &not_distinct_pos)).?);
    try std.testing.expect(not_distinct_tokens.items[not_distinct_pos].kind == .lparen);

    var missing_distinct_tokens = try lexer.tokenizeAlloc(alloc, "NULLS NOT (tenant_id)");
    defer lexer.freeTokens(alloc, &missing_distinct_tokens);
    var missing_distinct_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseOptionalDdlUniqueNullsDistinct(missing_distinct_tokens.items, &missing_distinct_pos));

    var timing_tokens = try lexer.tokenizeAlloc(alloc, "NOT DEFERRABLE INITIALLY IMMEDIATE,");
    defer lexer.freeTokens(alloc, &timing_tokens);
    var timing_pos: usize = 0;
    try consumeOptionalDdlImmediateConstraintTiming(timing_tokens.items, &timing_pos);
    try std.testing.expect(timing_tokens.items[timing_pos].kind == .comma);

    var deferred_tokens = try lexer.tokenizeAlloc(alloc, "NOT DEFERRABLE INITIALLY DEFERRED,");
    defer lexer.freeTokens(alloc, &deferred_tokens);
    var deferred_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, consumeOptionalDdlImmediateConstraintTiming(deferred_tokens.items, &deferred_pos));

    var deferrable_tokens = try lexer.tokenizeAlloc(alloc, "DEFERRABLE,");
    defer lexer.freeTokens(alloc, &deferrable_tokens);
    var deferrable_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, consumeOptionalDdlImmediateConstraintTiming(deferrable_tokens.items, &deferrable_pos));

    var flexible_timing_tokens = try lexer.tokenizeAlloc(alloc, "DEFERRABLE INITIALLY DEFERRED,");
    defer lexer.freeTokens(alloc, &flexible_timing_tokens);
    var flexible_timing_pos: usize = 0;
    const flexible_timing = try parseOptionalDdlConstraintTiming(flexible_timing_tokens.items, &flexible_timing_pos);
    try std.testing.expect(flexible_timing.deferrable);
    try std.testing.expectEqual(runtime_schema.ForeignKeyTiming.deferred, flexible_timing.timing);
    try std.testing.expect(flexible_timing_tokens.items[flexible_timing_pos].kind == .comma);

    var flexible_not_deferrable_tokens = try lexer.tokenizeAlloc(alloc, "NOT DEFERRABLE INITIALLY IMMEDIATE,");
    defer lexer.freeTokens(alloc, &flexible_not_deferrable_tokens);
    var flexible_not_deferrable_pos: usize = 0;
    const flexible_not_deferrable = try parseOptionalDdlConstraintTiming(flexible_not_deferrable_tokens.items, &flexible_not_deferrable_pos);
    try std.testing.expect(!flexible_not_deferrable.deferrable);
    try std.testing.expectEqual(runtime_schema.ForeignKeyTiming.immediate, flexible_not_deferrable.timing);
    try std.testing.expect(flexible_not_deferrable_tokens.items[flexible_not_deferrable_pos].kind == .comma);

    var contradictory_timing_tokens = try lexer.tokenizeAlloc(alloc, "NOT DEFERRABLE INITIALLY DEFERRED,");
    defer lexer.freeTokens(alloc, &contradictory_timing_tokens);
    var contradictory_timing_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseOptionalDdlConstraintTiming(contradictory_timing_tokens.items, &contradictory_timing_pos));

    var duplicate_timing_tokens = try lexer.tokenizeAlloc(alloc, "DEFERRABLE NOT DEFERRABLE,");
    defer lexer.freeTokens(alloc, &duplicate_timing_tokens);
    var duplicate_timing_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseOptionalDdlConstraintTiming(duplicate_timing_tokens.items, &duplicate_timing_pos));
}

test "sql adapter grammar parses ddl constraint column lists" {
    const alloc = std.testing.allocator;

    var list_tokens = try lexer.tokenizeAlloc(alloc, "(tenant_id, usage_id) INCLUDE (status)");
    defer lexer.freeTokens(alloc, &list_tokens);
    var list_pos: usize = 0;
    const columns = try parseDdlColumnListAlloc(alloc, list_tokens.items, &list_pos);
    defer freeStringSlice(alloc, columns);
    try std.testing.expectEqual(@as(usize, 2), columns.len);
    try std.testing.expectEqualStrings("tenant_id", columns[0]);
    try std.testing.expectEqualStrings("usage_id", columns[1]);
    try std.testing.expect(std.ascii.eqlIgnoreCase(list_tokens.items[list_pos].text, "include"));

    const include = try parseOptionalDdlConstraintIncludeAlloc(alloc, list_tokens.items, &list_pos);
    defer freeStringSlice(alloc, include);
    try std.testing.expectEqual(@as(usize, 1), include.len);
    try std.testing.expectEqualStrings("status", include[0]);
    try std.testing.expectEqual(list_tokens.items.len, list_pos);

    var temporal_tokens = try lexer.tokenizeAlloc(alloc, "(tenant_id, valid_at WITHOUT OVERLAPS)");
    defer lexer.freeTokens(alloc, &temporal_tokens);
    var temporal_pos: usize = 0;
    const temporal = try parseDdlTemporalColumnListAlloc(alloc, temporal_tokens.items, &temporal_pos);
    defer temporal.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), temporal.columns.len);
    try std.testing.expectEqualStrings("tenant_id", temporal.columns[0]);
    try std.testing.expectEqualStrings("valid_at", temporal.without_overlaps_period.?);
    try std.testing.expectEqual(temporal_tokens.items.len, temporal_pos);

    var duplicate_tokens = try lexer.tokenizeAlloc(alloc, "(tenant_id, tenant_id)");
    defer lexer.freeTokens(alloc, &duplicate_tokens);
    var duplicate_pos: usize = 0;
    const duplicate = try parseDdlColumnListAlloc(alloc, duplicate_tokens.items, &duplicate_pos);
    defer freeStringSlice(alloc, duplicate);
    try std.testing.expectEqual(@as(usize, 2), duplicate.len);

    var duplicate_unique_tokens = try lexer.tokenizeAlloc(alloc, "(tenant_id, TENANT_ID)");
    defer lexer.freeTokens(alloc, &duplicate_unique_tokens);
    var duplicate_unique_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseDdlUniqueColumnListAlloc(alloc, duplicate_unique_tokens.items, &duplicate_unique_pos));

    var duplicate_include_tokens = try lexer.tokenizeAlloc(alloc, "INCLUDE (status, STATUS)");
    defer lexer.freeTokens(alloc, &duplicate_include_tokens);
    var duplicate_include_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseOptionalDdlConstraintIncludeUniqueAlloc(alloc, duplicate_include_tokens.items, &duplicate_include_pos, &.{"tenant_id"}));

    var overlapping_include_tokens = try lexer.tokenizeAlloc(alloc, "INCLUDE (usage_id)");
    defer lexer.freeTokens(alloc, &overlapping_include_tokens);
    var overlapping_include_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseOptionalDdlConstraintIncludeUniqueAlloc(alloc, overlapping_include_tokens.items, &overlapping_include_pos, &.{ "tenant_id", "usage_id" }));

    var duplicate_temporal_tokens = try lexer.tokenizeAlloc(alloc, "(tenant_id, TENANT_ID WITHOUT OVERLAPS)");
    defer lexer.freeTokens(alloc, &duplicate_temporal_tokens);
    var duplicate_temporal_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseDdlUniqueTemporalColumnListAlloc(alloc, duplicate_temporal_tokens.items, &duplicate_temporal_pos));

    var empty_tokens = try lexer.tokenizeAlloc(alloc, "()");
    defer lexer.freeTokens(alloc, &empty_tokens);
    var empty_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseDdlColumnListAlloc(alloc, empty_tokens.items, &empty_pos));

    var double_period_tokens = try lexer.tokenizeAlloc(alloc, "(valid_from WITHOUT OVERLAPS, valid_to WITHOUT OVERLAPS)");
    defer lexer.freeTokens(alloc, &double_period_tokens);
    var double_period_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseDdlTemporalColumnListAlloc(alloc, double_period_tokens.items, &double_period_pos));
}

test "sql adapter grammar validates identifier lists" {
    const alloc = std.testing.allocator;

    try validateSqlIdentifierListUnique(&.{ "tenant_id", "usage_id" });
    try std.testing.expectError(error.UnsupportedSqlShape, validateSqlIdentifierListUnique(&.{ "tenant_id", "TENANT_ID" }));

    try validateSqlIdentifierListsDisjoint(&.{ "tenant_id", "usage_id" }, &.{"status"});
    try std.testing.expectError(error.UnsupportedSqlShape, validateSqlIdentifierListsDisjoint(&.{ "tenant_id", "usage_id" }, &.{"USAGE_ID"}));

    var alias_tokens = try lexer.tokenizeAlloc(alloc, "AS projected_name");
    defer lexer.freeTokens(alloc, &alias_tokens);
    var alias_pos: usize = 0;
    const alias = (try parseOptionalProjectionAliasAlloc(alloc, alias_tokens.items, &alias_pos)) orelse return error.TestUnexpectedResult;
    defer alloc.free(alias);
    try std.testing.expectEqualStrings("projected_name", alias);
    try std.testing.expectEqual(alias_tokens.items.len, alias_pos);

    var consume_tokens = try lexer.tokenizeAlloc(alloc, "AS status");
    defer lexer.freeTokens(alloc, &consume_tokens);
    var consume_pos: usize = 0;
    try consumeProjectionAlias(alloc, consume_tokens.items, &consume_pos, "status");
    try std.testing.expectEqual(consume_tokens.items.len, consume_pos);
}

test "sql adapter grammar parses ddl foreign key options" {
    const alloc = std.testing.allocator;

    var action_tokens = try lexer.tokenizeAlloc(alloc, "ON DELETE CASCADE ON UPDATE SET NULL DEFERRABLE INITIALLY DEFERRED NOT VALID");
    defer lexer.freeTokens(alloc, &action_tokens);
    var action_pos: usize = 0;
    const action = try parseDdlForeignKeyOptions(action_tokens.items, &action_pos);
    try std.testing.expectEqual(DdlForeignKeyActionSyntax.cascade, action.on_delete);
    try std.testing.expectEqual(DdlForeignKeyActionSyntax.set_null, action.on_update);
    try std.testing.expect(action.deferrable);
    try std.testing.expectEqual(DdlForeignKeyTimingSyntax.deferred, action.timing);
    try std.testing.expectEqual(DdlForeignKeyMatchSyntax.simple, action.match);
    try std.testing.expect(peekDdlNotValid(action_tokens.items, action_pos));

    var no_action_tokens = try lexer.tokenizeAlloc(alloc, "ON DELETE NO ACTION ON UPDATE RESTRICT,");
    defer lexer.freeTokens(alloc, &no_action_tokens);
    var no_action_pos: usize = 0;
    const no_action = try parseDdlForeignKeyOptions(no_action_tokens.items, &no_action_pos);
    try std.testing.expectEqual(DdlForeignKeyActionSyntax.no_action, no_action.on_delete);
    try std.testing.expectEqual(DdlForeignKeyActionSyntax.restrict, no_action.on_update);
    try std.testing.expect(!no_action.deferrable);
    try std.testing.expectEqual(DdlForeignKeyTimingSyntax.immediate, no_action.timing);
    try std.testing.expectEqual(DdlForeignKeyMatchSyntax.simple, no_action.match);
    try std.testing.expect(no_action_tokens.items[no_action_pos].kind == .comma);

    var immediate_tokens = try lexer.tokenizeAlloc(alloc, "NOT DEFERRABLE INITIALLY IMMEDIATE)");
    defer lexer.freeTokens(alloc, &immediate_tokens);
    var immediate_pos: usize = 0;
    const immediate = try parseDdlForeignKeyOptions(immediate_tokens.items, &immediate_pos);
    try std.testing.expect(!immediate.deferrable);
    try std.testing.expectEqual(DdlForeignKeyTimingSyntax.immediate, immediate.timing);
    try std.testing.expect(immediate_tokens.items[immediate_pos].kind == .rparen);

    var match_full_tokens = try lexer.tokenizeAlloc(alloc, "MATCH FULL DEFERRABLE INITIALLY DEFERRED)");
    defer lexer.freeTokens(alloc, &match_full_tokens);
    var match_full_pos: usize = 0;
    const match_full = try parseDdlForeignKeyOptions(match_full_tokens.items, &match_full_pos);
    try std.testing.expectEqual(DdlForeignKeyMatchSyntax.full, match_full.match);
    try std.testing.expect(match_full.deferrable);
    try std.testing.expectEqual(DdlForeignKeyTimingSyntax.deferred, match_full.timing);
    try std.testing.expect(match_full_tokens.items[match_full_pos].kind == .rparen);

    var match_simple_tokens = try lexer.tokenizeAlloc(alloc, "MATCH SIMPLE NOT DEFERRABLE)");
    defer lexer.freeTokens(alloc, &match_simple_tokens);
    var match_simple_pos: usize = 0;
    const match_simple = try parseDdlForeignKeyOptions(match_simple_tokens.items, &match_simple_pos);
    try std.testing.expectEqual(DdlForeignKeyMatchSyntax.simple, match_simple.match);
    try std.testing.expect(!match_simple.deferrable);
    try std.testing.expect(match_simple_tokens.items[match_simple_pos].kind == .rparen);

    var match_partial_tokens = try lexer.tokenizeAlloc(alloc, "MATCH PARTIAL)");
    defer lexer.freeTokens(alloc, &match_partial_tokens);
    var match_partial_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseDdlForeignKeyOptions(match_partial_tokens.items, &match_partial_pos));

    var duplicate_match_tokens = try lexer.tokenizeAlloc(alloc, "MATCH FULL MATCH SIMPLE)");
    defer lexer.freeTokens(alloc, &duplicate_match_tokens);
    var duplicate_match_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseDdlForeignKeyOptions(duplicate_match_tokens.items, &duplicate_match_pos));

    var invalid_action_tokens = try lexer.tokenizeAlloc(alloc, "ON DELETE SET DEFAULT)");
    defer lexer.freeTokens(alloc, &invalid_action_tokens);
    var invalid_action_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseDdlForeignKeyOptions(invalid_action_tokens.items, &invalid_action_pos));

    var invalid_on_tokens = try lexer.tokenizeAlloc(alloc, "ON TRUNCATE CASCADE)");
    defer lexer.freeTokens(alloc, &invalid_on_tokens);
    var invalid_on_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseDdlForeignKeyOptions(invalid_on_tokens.items, &invalid_on_pos));
}

test "sql adapter grammar parses ddl foreign key column lists" {
    const alloc = std.testing.allocator;

    var list_tokens = try lexer.tokenizeAlloc(alloc, "(tenant_id, customer_id) REFERENCES customers");
    defer lexer.freeTokens(alloc, &list_tokens);
    var list_pos: usize = 0;
    const list = try parseDdlForeignKeyColumnListAlloc(alloc, list_tokens.items, &list_pos);
    defer list.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), list.columns.len);
    try std.testing.expectEqualStrings("tenant_id", list.columns[0]);
    try std.testing.expectEqualStrings("customer_id", list.columns[1]);
    try std.testing.expect(list.period == null);
    try std.testing.expect(std.ascii.eqlIgnoreCase(list_tokens.items[list_pos].text, "references"));

    var temporal_tokens = try lexer.tokenizeAlloc(alloc, "(tenant_id, PERIOD valid_time)");
    defer lexer.freeTokens(alloc, &temporal_tokens);
    var temporal_pos: usize = 0;
    const temporal = try parseDdlForeignKeyColumnListAlloc(alloc, temporal_tokens.items, &temporal_pos);
    defer temporal.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), temporal.columns.len);
    try std.testing.expectEqualStrings("tenant_id", temporal.columns[0]);
    try std.testing.expectEqualStrings("valid_time", temporal.period.?);
    try std.testing.expectEqual(temporal_tokens.items.len, temporal_pos);

    var duplicate_tokens = try lexer.tokenizeAlloc(alloc, "(tenant_id, tenant_id)");
    defer lexer.freeTokens(alloc, &duplicate_tokens);
    var duplicate_pos: usize = 0;
    const duplicate = try parseDdlForeignKeyColumnListAlloc(alloc, duplicate_tokens.items, &duplicate_pos);
    defer duplicate.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), duplicate.columns.len);

    var empty_tokens = try lexer.tokenizeAlloc(alloc, "()");
    defer lexer.freeTokens(alloc, &empty_tokens);
    var empty_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseDdlForeignKeyColumnListAlloc(alloc, empty_tokens.items, &empty_pos));

    var double_period_tokens = try lexer.tokenizeAlloc(alloc, "(tenant_id, PERIOD valid_time, PERIOD valid_time_2)");
    defer lexer.freeTokens(alloc, &double_period_tokens);
    var double_period_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseDdlForeignKeyColumnListAlloc(alloc, double_period_tokens.items, &double_period_pos));
}

test "sql adapter grammar parses ddl period constraints" {
    const alloc = std.testing.allocator;

    var period_tokens = try lexer.tokenizeAlloc(alloc, "FOR valid_time (valid_from, valid_to),");
    defer lexer.freeTokens(alloc, &period_tokens);
    var period_pos: usize = 0;
    const period = try parseDdlPeriodConstraintAlloc(alloc, period_tokens.items, &period_pos);
    defer period.deinit(alloc);
    try std.testing.expectEqualStrings("valid_time", period.name);
    try std.testing.expectEqualStrings("valid_from", period.start_column);
    try std.testing.expectEqualStrings("valid_to", period.end_column);
    try std.testing.expect(period_tokens.items[period_pos].kind == .comma);

    var qualified_tokens = try lexer.tokenizeAlloc(alloc, "FOR application_time (sys_from, sys_to)");
    defer lexer.freeTokens(alloc, &qualified_tokens);
    var qualified_pos: usize = 0;
    const qualified = try parseDdlPeriodConstraintAlloc(alloc, qualified_tokens.items, &qualified_pos);
    defer qualified.deinit(alloc);
    try std.testing.expectEqualStrings("application_time", qualified.name);
    try std.testing.expectEqual(qualified_tokens.items.len, qualified_pos);

    var missing_for_tokens = try lexer.tokenizeAlloc(alloc, "valid_time (valid_from, valid_to)");
    defer lexer.freeTokens(alloc, &missing_for_tokens);
    var missing_for_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseDdlPeriodConstraintAlloc(alloc, missing_for_tokens.items, &missing_for_pos));

    var missing_end_tokens = try lexer.tokenizeAlloc(alloc, "FOR valid_time (valid_from)");
    defer lexer.freeTokens(alloc, &missing_end_tokens);
    var missing_end_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseDdlPeriodConstraintAlloc(alloc, missing_end_tokens.items, &missing_end_pos));
}

test "sql adapter grammar parses stored generated value wrappers" {
    const alloc = std.testing.allocator;

    var generated_tokens = try lexer.tokenizeAlloc(alloc, "ALWAYS AS (lower(email)) STORED NOT NULL");
    defer lexer.freeTokens(alloc, &generated_tokens);
    var generated_pos: usize = 0;
    try consumeDdlStoredGeneratedValuePrefix(generated_tokens.items, &generated_pos);
    try std.testing.expect(std.ascii.eqlIgnoreCase(generated_tokens.items[generated_pos].text, "lower"));
    while (generated_pos + 1 < generated_tokens.items.len and !std.ascii.eqlIgnoreCase(generated_tokens.items[generated_pos + 1].text, "stored")) generated_pos += 1;
    try consumeDdlStoredGeneratedValueSuffix(generated_tokens.items, &generated_pos);
    try std.testing.expect(std.ascii.eqlIgnoreCase(generated_tokens.items[generated_pos].text, "not"));

    var missing_always_tokens = try lexer.tokenizeAlloc(alloc, "AS (lower(email)) STORED");
    defer lexer.freeTokens(alloc, &missing_always_tokens);
    var missing_always_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, consumeDdlStoredGeneratedValuePrefix(missing_always_tokens.items, &missing_always_pos));

    var virtual_tokens = try lexer.tokenizeAlloc(alloc, "ALWAYS AS (lower(email)) VIRTUAL");
    defer lexer.freeTokens(alloc, &virtual_tokens);
    var virtual_pos: usize = 0;
    try consumeDdlStoredGeneratedValuePrefix(virtual_tokens.items, &virtual_pos);
    while (virtual_pos + 1 < virtual_tokens.items.len and !std.ascii.eqlIgnoreCase(virtual_tokens.items[virtual_pos + 1].text, "virtual")) virtual_pos += 1;
    try std.testing.expectError(error.UnsupportedSqlShape, consumeDdlStoredGeneratedValueSuffix(virtual_tokens.items, &virtual_pos));

    var suffix_without_close_tokens = try lexer.tokenizeAlloc(alloc, "STORED");
    defer lexer.freeTokens(alloc, &suffix_without_close_tokens);
    var suffix_without_close_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, consumeDdlStoredGeneratedValueSuffix(suffix_without_close_tokens.items, &suffix_without_close_pos));

    var stored_tokens = try lexer.tokenizeAlloc(alloc, "ALWAYS AS (concat_ws(':', tenant_id, status)) STORED NOT NULL");
    defer lexer.freeTokens(alloc, &stored_tokens);
    var stored_pos: usize = 0;
    const stored = try parseDdlStoredGeneratedValueAlloc(alloc, stored_tokens.items, &stored_pos);
    defer freeDdlGeneratedValue(alloc, stored);
    try std.testing.expectEqual(runtime_schema.RelationalGeneratedOp.concat_ws, stored.op);
    try std.testing.expectEqual(@as(usize, 2), stored.fields.len);
    try std.testing.expectEqualStrings("tenant_id", stored.fields[0]);
    try std.testing.expectEqualStrings("status", stored.fields[1]);
    try std.testing.expectEqualStrings(":", stored.separator);
    try std.testing.expect(std.ascii.eqlIgnoreCase(stored_tokens.items[stored_pos].text, "not"));
}

test "sql adapter grammar parses ddl generated expressions" {
    const alloc = std.testing.allocator;

    var lower_tokens = try lexer.tokenizeAlloc(alloc, "lower(email)) STORED");
    defer lexer.freeTokens(alloc, &lower_tokens);
    var lower_pos: usize = 0;
    const lower = try parseDdlGeneratedExpressionAlloc(alloc, lower_tokens.items, &lower_pos);
    defer freeDdlGeneratedValue(alloc, lower);
    try std.testing.expectEqual(runtime_schema.RelationalGeneratedOp.lower, lower.op);
    try std.testing.expectEqualStrings("email", lower.field.?);
    try std.testing.expect(lower_tokens.items[lower_pos].kind == .rparen);

    var upper_tokens = try lexer.tokenizeAlloc(alloc, "upper(status)");
    defer lexer.freeTokens(alloc, &upper_tokens);
    var upper_pos: usize = 0;
    const upper = try parseDdlGeneratedExpressionAlloc(alloc, upper_tokens.items, &upper_pos);
    defer freeDdlGeneratedValue(alloc, upper);
    try std.testing.expectEqual(runtime_schema.RelationalGeneratedOp.upper, upper.op);
    try std.testing.expectEqualStrings("status", upper.field.?);
    try std.testing.expectEqual(upper_tokens.items.len, upper_pos);

    var md5_tokens = try lexer.tokenizeAlloc(alloc, "md5(request_id)");
    defer lexer.freeTokens(alloc, &md5_tokens);
    var md5_pos: usize = 0;
    const md5 = try parseDdlGeneratedExpressionAlloc(alloc, md5_tokens.items, &md5_pos);
    defer freeDdlGeneratedValue(alloc, md5);
    try std.testing.expectEqual(runtime_schema.RelationalGeneratedOp.md5, md5.op);
    try std.testing.expectEqualStrings("request_id", md5.field.?);

    var concat_tokens = try lexer.tokenizeAlloc(alloc, "concat(tenant_id, ':', status)");
    defer lexer.freeTokens(alloc, &concat_tokens);
    var concat_pos: usize = 0;
    const concat = try parseDdlGeneratedExpressionAlloc(alloc, concat_tokens.items, &concat_pos);
    defer freeDdlGeneratedValue(alloc, concat);
    try std.testing.expectEqual(runtime_schema.RelationalGeneratedOp.concat, concat.op);
    try std.testing.expectEqual(@as(usize, 2), concat.fields.len);
    try std.testing.expectEqualStrings("tenant_id", concat.fields[0]);
    try std.testing.expectEqualStrings("status", concat.fields[1]);
    try std.testing.expectEqualStrings(":", concat.separator);

    var concat_ws_tokens = try lexer.tokenizeAlloc(alloc, "concat_ws(':', tenant_id, status)");
    defer lexer.freeTokens(alloc, &concat_ws_tokens);
    var concat_ws_pos: usize = 0;
    const concat_ws = try parseDdlGeneratedExpressionAlloc(alloc, concat_ws_tokens.items, &concat_ws_pos);
    defer freeDdlGeneratedValue(alloc, concat_ws);
    try std.testing.expectEqual(runtime_schema.RelationalGeneratedOp.concat_ws, concat_ws.op);
    try std.testing.expectEqual(@as(usize, 2), concat_ws.fields.len);
    try std.testing.expectEqualStrings(":", concat_ws.separator);

    var single_concat_tokens = try lexer.tokenizeAlloc(alloc, "concat(status)");
    defer lexer.freeTokens(alloc, &single_concat_tokens);
    var single_concat_pos: usize = 0;
    const single_concat = try parseDdlGeneratedExpressionAlloc(alloc, single_concat_tokens.items, &single_concat_pos);
    defer freeDdlGeneratedValue(alloc, single_concat);
    try std.testing.expectEqual(runtime_schema.RelationalGeneratedOp.expression, single_concat.op);
    try std.testing.expectEqual(runtime_schema.RelationalRowsExpressionKind.concat, single_concat.expression.?.kind);

    var mismatched_separator_tokens = try lexer.tokenizeAlloc(alloc, "concat(tenant_id, ':', status, '-', id)");
    defer lexer.freeTokens(alloc, &mismatched_separator_tokens);
    var mismatched_separator_pos: usize = 0;
    const mismatched_separator = try parseDdlGeneratedExpressionAlloc(alloc, mismatched_separator_tokens.items, &mismatched_separator_pos);
    defer freeDdlGeneratedValue(alloc, mismatched_separator);
    try std.testing.expectEqual(runtime_schema.RelationalGeneratedOp.expression, mismatched_separator.op);
    try std.testing.expectEqual(runtime_schema.RelationalRowsExpressionKind.concat, mismatched_separator.expression.?.kind);

    var replace_tokens = try lexer.tokenizeAlloc(alloc, "replace(status, 'a', 'b')");
    defer lexer.freeTokens(alloc, &replace_tokens);
    var replace_pos: usize = 0;
    const replace = try parseDdlGeneratedExpressionAlloc(alloc, replace_tokens.items, &replace_pos);
    defer freeDdlGeneratedValue(alloc, replace);
    try std.testing.expectEqual(runtime_schema.RelationalGeneratedOp.expression, replace.op);
    try std.testing.expectEqual(runtime_schema.RelationalRowsExpressionKind.replace, replace.expression.?.kind);
    try std.testing.expectEqual(@as(usize, 3), replace.expression.?.operands.len);

    var arithmetic_tokens = try lexer.tokenizeAlloc(alloc, "round((amount + fee) * 100)");
    defer lexer.freeTokens(alloc, &arithmetic_tokens);
    var arithmetic_pos: usize = 0;
    const arithmetic = try parseDdlGeneratedExpressionAlloc(alloc, arithmetic_tokens.items, &arithmetic_pos);
    defer freeDdlGeneratedValue(alloc, arithmetic);
    try std.testing.expectEqual(runtime_schema.RelationalGeneratedOp.expression, arithmetic.op);
    try std.testing.expectEqual(runtime_schema.RelationalRowsExpressionKind.round, arithmetic.expression.?.kind);
    try std.testing.expectEqual(runtime_schema.RelationalRowsExpressionKind.mul, arithmetic.expression.?.operands[0].kind);

    var json_extract_tokens = try lexer.tokenizeAlloc(alloc, "jsonb_extract_path_text(metadata, 'source')");
    defer lexer.freeTokens(alloc, &json_extract_tokens);
    var json_extract_pos: usize = 0;
    const json_extract = try parseDdlGeneratedExpressionAlloc(alloc, json_extract_tokens.items, &json_extract_pos);
    defer freeDdlGeneratedValue(alloc, json_extract);
    try std.testing.expectEqual(runtime_schema.RelationalGeneratedOp.expression, json_extract.op);
    try std.testing.expectEqual(runtime_schema.RelationalRowsExpressionKind.json_extract, json_extract.expression.?.kind);
    try std.testing.expect(json_extract.expression.?.json_as_text);
    try std.testing.expectEqualStrings("source", json_extract.expression.?.json_path);

    var volatile_tokens = try lexer.tokenizeAlloc(alloc, "now()");
    defer lexer.freeTokens(alloc, &volatile_tokens);
    var volatile_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseDdlGeneratedExpressionAlloc(alloc, volatile_tokens.items, &volatile_pos));
}

test "sql adapter grammar parses conflict-target unique predicate JSON" {
    const alloc = std.testing.allocator;
    const columns = [_]runtime_schema.RelationalColumn{
        .{ .name = "status", .path = "status", .field_type = .keyword },
        .{ .name = "tenant_id", .path = "tenant_id", .field_type = .keyword },
    };

    var tokens = try lexer.tokenizeAlloc(alloc, "status = 'active' AND tenant_id IS NOT NULL DO UPDATE");
    defer lexer.freeTokens(alloc, &tokens);
    var pos: usize = 0;
    const where_json = try parseDdlUniquePredicateWhereJsonAlloc(alloc, tokens.items, &pos, &columns);
    defer alloc.free(where_json);
    try std.testing.expectEqualStrings("{\"all\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"active\"},{\"field\":\"tenant_id\",\"op\":\"is_not_null\"}]}", where_json);
    try std.testing.expect(pos < tokens.items.len);
    try std.testing.expect(std.ascii.eqlIgnoreCase(tokens.items[pos].text, "do"));

    var invalid_tokens = try lexer.tokenizeAlloc(alloc, "missing = 'active' DO UPDATE");
    defer lexer.freeTokens(alloc, &invalid_tokens);
    var invalid_pos: usize = 0;
    try std.testing.expectError(error.InvalidSqlCatalog, parseDdlUniquePredicateWhereJsonAlloc(alloc, invalid_tokens.items, &invalid_pos, &columns));
}

test "sql adapter grammar parses ddl unique expressions" {
    const alloc = std.testing.allocator;

    var lower_tokens = try lexer.tokenizeAlloc(alloc, "lower(email) WHERE");
    defer lexer.freeTokens(alloc, &lower_tokens);
    var lower_pos: usize = 0;
    const lower = try parseDdlUniqueExpressionAlloc(alloc, lower_tokens.items, &lower_pos);
    defer alloc.free(lower.field);
    try std.testing.expectEqual(runtime_schema.UniqueExpressionOp.lower, lower.op);
    try std.testing.expectEqualStrings("email", lower.field);
    try std.testing.expect(std.ascii.eqlIgnoreCase(lower_tokens.items[lower_pos].text, "where"));

    var upper_tokens = try lexer.tokenizeAlloc(alloc, "upper(status)");
    defer lexer.freeTokens(alloc, &upper_tokens);
    var upper_pos: usize = 0;
    const upper = try parseDdlUniqueExpressionAlloc(alloc, upper_tokens.items, &upper_pos);
    defer alloc.free(upper.field);
    try std.testing.expectEqual(runtime_schema.UniqueExpressionOp.upper, upper.op);
    try std.testing.expectEqualStrings("status", upper.field);
    try std.testing.expectEqual(upper_tokens.items.len, upper_pos);

    var md5_tokens = try lexer.tokenizeAlloc(alloc, "md5(request_id)");
    defer lexer.freeTokens(alloc, &md5_tokens);
    var md5_pos: usize = 0;
    const md5 = try parseDdlUniqueExpressionAlloc(alloc, md5_tokens.items, &md5_pos);
    defer alloc.free(md5.field);
    try std.testing.expectEqual(runtime_schema.UniqueExpressionOp.md5, md5.op);
    try std.testing.expectEqualStrings("request_id", md5.field);

    var concat_tokens = try lexer.tokenizeAlloc(alloc, "concat(tenant_id, ':', status)");
    defer lexer.freeTokens(alloc, &concat_tokens);
    var concat_pos: usize = 0;
    const concat = try parseDdlUniqueExpressionAlloc(alloc, concat_tokens.items, &concat_pos);
    defer runtime_schema.freeRelationalRowsExpression(alloc, concat.expression.?);
    try std.testing.expectEqual(runtime_schema.UniqueExpressionOp.expression, concat.op);
    try std.testing.expectEqual(runtime_schema.RelationalRowsExpressionKind.concat, concat.expression.?.kind);
    try std.testing.expectEqual(@as(usize, 3), concat.expression.?.operands.len);
    try std.testing.expectEqual(concat_tokens.items.len, concat_pos);
}

test "sql adapter grammar parses ddl index expression wrappers" {
    const alloc = std.testing.allocator;

    var wrapped_tokens = try lexer.tokenizeAlloc(alloc, "((lower(email))) ASC");
    defer lexer.freeTokens(alloc, &wrapped_tokens);
    try std.testing.expect(peekDdlIndexElementExpression(wrapped_tokens.items, 0, false));
    var wrapped_pos: usize = 0;
    const wrappers = consumeDdlIndexExpressionWrappers(wrapped_tokens.items, &wrapped_pos);
    try std.testing.expectEqual(@as(usize, 2), wrappers);
    const expression = try parseDdlUniqueExpressionAlloc(alloc, wrapped_tokens.items, &wrapped_pos);
    defer alloc.free(expression.field);
    try std.testing.expectEqual(runtime_schema.UniqueExpressionOp.lower, expression.op);
    try std.testing.expectEqualStrings("email", expression.field);
    try closeDdlIndexExpressionWrappers(wrapped_tokens.items, &wrapped_pos, wrappers);
    try std.testing.expect(std.ascii.eqlIgnoreCase(wrapped_tokens.items[wrapped_pos].text, "asc"));

    var concat_tokens = try lexer.tokenizeAlloc(alloc, "(concat(tenant_id, ':', status))");
    defer lexer.freeTokens(alloc, &concat_tokens);
    try std.testing.expect(!peekDdlIndexElementExpression(concat_tokens.items, 0, false));
    try std.testing.expect(peekDdlIndexElementExpression(concat_tokens.items, 0, true));

    var replace_tokens = try lexer.tokenizeAlloc(alloc, "(replace(status, 'a', 'b'))");
    defer lexer.freeTokens(alloc, &replace_tokens);
    try std.testing.expect(!peekDdlIndexElementExpression(replace_tokens.items, 0, false));
    try std.testing.expect(peekDdlIndexElementExpression(replace_tokens.items, 0, true));

    var column_tokens = try lexer.tokenizeAlloc(alloc, "(email)");
    defer lexer.freeTokens(alloc, &column_tokens);
    try std.testing.expect(!peekDdlIndexElementExpression(column_tokens.items, 0, true));

    var function_name_column_tokens = try lexer.tokenizeAlloc(alloc, "(lower)");
    defer lexer.freeTokens(alloc, &function_name_column_tokens);
    try std.testing.expect(!peekDdlIndexElementExpression(function_name_column_tokens.items, 0, true));
}

test "sql adapter grammar parses ddl collations" {
    const alloc = std.testing.allocator;

    var simple_tokens = try lexer.tokenizeAlloc(alloc, "COLLATE \"C\" NOT NULL");
    defer lexer.freeTokens(alloc, &simple_tokens);
    var simple_pos: usize = 0;
    const simple = (try parseOptionalDdlCollationAlloc(alloc, simple_tokens.items, &simple_pos)).?;
    defer alloc.free(simple);
    try std.testing.expectEqualStrings("C", simple);
    try std.testing.expect(std.ascii.eqlIgnoreCase(simple_tokens.items[simple_pos].text, "not"));

    var qualified_tokens = try lexer.tokenizeAlloc(alloc, "COLLATE public.en_US DEFAULT 'active'");
    defer lexer.freeTokens(alloc, &qualified_tokens);
    var qualified_pos: usize = 0;
    const qualified = (try parseOptionalDdlCollationAlloc(alloc, qualified_tokens.items, &qualified_pos)).?;
    defer alloc.free(qualified);
    try std.testing.expectEqualStrings("public.en_US", qualified);
    try std.testing.expect(std.ascii.eqlIgnoreCase(qualified_tokens.items[qualified_pos].text, "default"));

    var absent_tokens = try lexer.tokenizeAlloc(alloc, "NOT NULL");
    defer lexer.freeTokens(alloc, &absent_tokens);
    var absent_pos: usize = 0;
    try std.testing.expect((try parseOptionalDdlCollationAlloc(alloc, absent_tokens.items, &absent_pos)) == null);
    try std.testing.expectEqual(@as(usize, 0), absent_pos);

    var missing_name_tokens = try lexer.tokenizeAlloc(alloc, "COLLATE");
    defer lexer.freeTokens(alloc, &missing_name_tokens);
    var missing_name_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseOptionalDdlCollationAlloc(alloc, missing_name_tokens.items, &missing_name_pos));
}

test "sql adapter grammar parses current timestamp precision" {
    const alloc = std.testing.allocator;

    var absent_tokens = try lexer.tokenizeAlloc(alloc, "DEFAULT");
    defer lexer.freeTokens(alloc, &absent_tokens);
    var absent_pos: usize = 0;
    try parseOptionalCurrentTimestampPrecision(absent_tokens.items, &absent_pos);
    try std.testing.expectEqual(@as(usize, 0), absent_pos);

    var valid_tokens = try lexer.tokenizeAlloc(alloc, "(6) + interval '1 day'");
    defer lexer.freeTokens(alloc, &valid_tokens);
    var valid_pos: usize = 0;
    try parseOptionalCurrentTimestampPrecision(valid_tokens.items, &valid_pos);
    try std.testing.expect(valid_tokens.items[valid_pos].kind == .plus);

    var zero_tokens = try lexer.tokenizeAlloc(alloc, "(0)");
    defer lexer.freeTokens(alloc, &zero_tokens);
    var zero_pos: usize = 0;
    try parseOptionalCurrentTimestampPrecision(zero_tokens.items, &zero_pos);
    try std.testing.expectEqual(zero_tokens.items.len, zero_pos);

    var too_large_tokens = try lexer.tokenizeAlloc(alloc, "(7)");
    defer lexer.freeTokens(alloc, &too_large_tokens);
    var too_large_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseOptionalCurrentTimestampPrecision(too_large_tokens.items, &too_large_pos));

    var missing_number_tokens = try lexer.tokenizeAlloc(alloc, "()");
    defer lexer.freeTokens(alloc, &missing_number_tokens);
    var missing_number_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseOptionalCurrentTimestampPrecision(missing_number_tokens.items, &missing_number_pos));
}

test "sql adapter grammar parses ddl known defaults" {
    const alloc = std.testing.allocator;

    var null_tokens = try lexer.tokenizeAlloc(alloc, "NULL,");
    defer lexer.freeTokens(alloc, &null_tokens);
    var null_pos: usize = 0;
    try std.testing.expectEqual(DdlKnownDefaultSyntax.null_literal, (try parseOptionalDdlKnownDefault(null_tokens.items, &null_pos)).?);
    try std.testing.expect(null_tokens.items[null_pos].kind == .comma);

    var uuid_tokens = try lexer.tokenizeAlloc(alloc, "gen_random_uuid() PRIMARY KEY");
    defer lexer.freeTokens(alloc, &uuid_tokens);
    var uuid_pos: usize = 0;
    try std.testing.expectEqual(DdlKnownDefaultSyntax.uuid_v4, (try parseOptionalDdlKnownDefault(uuid_tokens.items, &uuid_pos)).?);
    try std.testing.expect(std.ascii.eqlIgnoreCase(uuid_tokens.items[uuid_pos].text, "primary"));

    var uuid_v4_tokens = try lexer.tokenizeAlloc(alloc, "uuid_generate_v4()");
    defer lexer.freeTokens(alloc, &uuid_v4_tokens);
    var uuid_v4_pos: usize = 0;
    try std.testing.expectEqual(DdlKnownDefaultSyntax.uuid_v4, (try parseOptionalDdlKnownDefault(uuid_v4_tokens.items, &uuid_v4_pos)).?);
    try std.testing.expectEqual(uuid_v4_tokens.items.len, uuid_v4_pos);

    var now_tokens = try lexer.tokenizeAlloc(alloc, "now() NOT NULL");
    defer lexer.freeTokens(alloc, &now_tokens);
    var now_pos: usize = 0;
    try std.testing.expectEqual(DdlKnownDefaultSyntax.now_ns, (try parseOptionalDdlKnownDefault(now_tokens.items, &now_pos)).?);
    try std.testing.expect(std.ascii.eqlIgnoreCase(now_tokens.items[now_pos].text, "not"));

    var timestamp_tokens = try lexer.tokenizeAlloc(alloc, "CURRENT_TIMESTAMP(6)");
    defer lexer.freeTokens(alloc, &timestamp_tokens);
    var timestamp_pos: usize = 0;
    try std.testing.expectEqual(DdlKnownDefaultSyntax.now_ns, (try parseOptionalDdlKnownDefault(timestamp_tokens.items, &timestamp_pos)).?);
    try std.testing.expectEqual(timestamp_tokens.items.len, timestamp_pos);

    var date_tokens = try lexer.tokenizeAlloc(alloc, "CURRENT_DATE");
    defer lexer.freeTokens(alloc, &date_tokens);
    var date_pos: usize = 0;
    try std.testing.expectEqual(DdlKnownDefaultSyntax.current_date_ns, (try parseOptionalDdlKnownDefault(date_tokens.items, &date_pos)).?);
    try std.testing.expectEqual(date_tokens.items.len, date_pos);

    var literal_tokens = try lexer.tokenizeAlloc(alloc, "'active'");
    defer lexer.freeTokens(alloc, &literal_tokens);
    var literal_pos: usize = 0;
    try std.testing.expect((try parseOptionalDdlKnownDefault(literal_tokens.items, &literal_pos)) == null);
    try std.testing.expectEqual(@as(usize, 0), literal_pos);

    var malformed_uuid_tokens = try lexer.tokenizeAlloc(alloc, "gen_random_uuid");
    defer lexer.freeTokens(alloc, &malformed_uuid_tokens);
    var malformed_uuid_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseOptionalDdlKnownDefault(malformed_uuid_tokens.items, &malformed_uuid_pos));
}

test "sql adapter grammar parses ddl type names" {
    const alloc = std.testing.allocator;
    _ = alloc;

    var numeric_tokens = try lexer.tokenizeAlloc(std.testing.allocator, "numeric(18, 2) DEFAULT 0");
    defer lexer.freeTokens(std.testing.allocator, &numeric_tokens);
    var numeric_pos: usize = 0;
    const numeric = try parseDdlType(numeric_tokens.items, &numeric_pos);
    try std.testing.expectEqual(runtime_schema.AntflyType.numeric, numeric.field_type);
    try std.testing.expect(numeric.array_item_type == null);
    try std.testing.expect(std.ascii.eqlIgnoreCase(numeric_tokens.items[numeric_pos].text, "default"));

    var character_tokens = try lexer.tokenizeAlloc(std.testing.allocator, "character varying(255) COLLATE \"C\"");
    defer lexer.freeTokens(std.testing.allocator, &character_tokens);
    var character_pos: usize = 0;
    const character = try parseDdlType(character_tokens.items, &character_pos);
    try std.testing.expectEqual(runtime_schema.AntflyType.keyword, character.field_type);
    try std.testing.expect(std.ascii.eqlIgnoreCase(character_tokens.items[character_pos].text, "collate"));

    var double_tokens = try lexer.tokenizeAlloc(std.testing.allocator, "double precision NOT NULL");
    defer lexer.freeTokens(std.testing.allocator, &double_tokens);
    var double_pos: usize = 0;
    const double = try parseDdlType(double_tokens.items, &double_pos);
    try std.testing.expectEqual(runtime_schema.AntflyType.numeric, double.field_type);
    try std.testing.expect(std.ascii.eqlIgnoreCase(double_tokens.items[double_pos].text, "not"));

    var timestamp_tokens = try lexer.tokenizeAlloc(std.testing.allocator, "timestamp with time zone DEFAULT now()");
    defer lexer.freeTokens(std.testing.allocator, &timestamp_tokens);
    var timestamp_pos: usize = 0;
    const timestamp = try parseDdlType(timestamp_tokens.items, &timestamp_pos);
    try std.testing.expectEqual(runtime_schema.AntflyType.datetime, timestamp.field_type);
    try std.testing.expect(std.ascii.eqlIgnoreCase(timestamp_tokens.items[timestamp_pos].text, "default"));

    var array_tokens = try lexer.tokenizeAlloc(std.testing.allocator, "text[] NOT NULL");
    defer lexer.freeTokens(std.testing.allocator, &array_tokens);
    var array_pos: usize = 0;
    const array = try parseDdlType(array_tokens.items, &array_pos);
    try std.testing.expectEqual(runtime_schema.AntflyType.array, array.field_type);
    try std.testing.expectEqual(runtime_schema.AntflyType.keyword, array.array_item_type.?);

    var json_array_tokens = try lexer.tokenizeAlloc(std.testing.allocator, "jsonb[]");
    defer lexer.freeTokens(std.testing.allocator, &json_array_tokens);
    var json_array_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseDdlType(json_array_tokens.items, &json_array_pos));

    var unknown_tokens = try lexer.tokenizeAlloc(std.testing.allocator, "money");
    defer lexer.freeTokens(std.testing.allocator, &unknown_tokens);
    var unknown_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseDdlType(unknown_tokens.items, &unknown_pos));

    var unclosed_tokens = try lexer.tokenizeAlloc(std.testing.allocator, "numeric(18, 2");
    defer lexer.freeTokens(std.testing.allocator, &unclosed_tokens);
    var unclosed_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseDdlType(unclosed_tokens.items, &unclosed_pos));
}

test "sql adapter grammar parses alter table headers" {
    const alloc = std.testing.allocator;

    var if_exists_tokens = try lexer.tokenizeAlloc(alloc, "TABLE IF EXISTS ONLY public.usage_records ADD COLUMN status text;");
    defer lexer.freeTokens(alloc, &if_exists_tokens);
    var if_exists_pos: usize = 0;
    var if_exists = try parseAlterTableHeaderAlloc(alloc, if_exists_tokens.items, &if_exists_pos);
    defer if_exists.deinit(alloc);
    try std.testing.expectEqualStrings("usage_records", if_exists.table_name);
    try std.testing.expect(if_exists.if_exists);
    try std.testing.expect(std.ascii.eqlIgnoreCase(if_exists_tokens.items[if_exists_pos].text, "add"));

    var plain_tokens = try lexer.tokenizeAlloc(alloc, "TABLE usage_records DROP COLUMN status;");
    defer lexer.freeTokens(alloc, &plain_tokens);
    var plain_pos: usize = 0;
    var plain = try parseAlterTableHeaderAlloc(alloc, plain_tokens.items, &plain_pos);
    defer plain.deinit(alloc);
    try std.testing.expectEqualStrings("usage_records", plain.table_name);
    try std.testing.expect(!plain.if_exists);
    try std.testing.expect(std.ascii.eqlIgnoreCase(plain_tokens.items[plain_pos].text, "drop"));

    var missing_table_tokens = try lexer.tokenizeAlloc(alloc, "ONLY usage_records ADD COLUMN status text;");
    defer lexer.freeTokens(alloc, &missing_table_tokens);
    var missing_table_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseAlterTableHeaderAlloc(alloc, missing_table_tokens.items, &missing_table_pos));

    var validate_tokens = try lexer.tokenizeAlloc(alloc, "VALIDATE CONSTRAINT usage_records_amount_check, ADD COLUMN status text;");
    defer lexer.freeTokens(alloc, &validate_tokens);
    var validate_pos: usize = 0;
    var validate = try parseAlterTableValidateConstraintOperationAlloc(alloc, validate_tokens.items, &validate_pos);
    defer validate.deinit(alloc);
    try std.testing.expectEqualStrings("usage_records_amount_check", validate.constraint_name);
    try std.testing.expect(validate_tokens.items[validate_pos].kind == .comma);

    var invalid_validate_tokens = try lexer.tokenizeAlloc(alloc, "VALIDATE usage_records_amount_check;");
    defer lexer.freeTokens(alloc, &invalid_validate_tokens);
    var invalid_validate_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseAlterTableValidateConstraintOperationAlloc(alloc, invalid_validate_tokens.items, &invalid_validate_pos));

    var rename_column_tokens = try lexer.tokenizeAlloc(alloc, "RENAME COLUMN old_status TO status, DROP COLUMN legacy_status;");
    defer lexer.freeTokens(alloc, &rename_column_tokens);
    var rename_column_pos: usize = 0;
    var rename_column = try parseAlterTableRenameOperationAlloc(alloc, rename_column_tokens.items, &rename_column_pos);
    defer rename_column.deinit(alloc);
    try std.testing.expectEqual(AlterTableRenameTargetSyntax.column, rename_column.target);
    try std.testing.expectEqualStrings("old_status", rename_column.old_name);
    try std.testing.expectEqualStrings("status", rename_column.new_name);
    try std.testing.expect(rename_column_tokens.items[rename_column_pos].kind == .comma);

    var rename_constraint_tokens = try lexer.tokenizeAlloc(alloc, "RENAME CONSTRAINT usage_status_key TO usage_status_unique;");
    defer lexer.freeTokens(alloc, &rename_constraint_tokens);
    var rename_constraint_pos: usize = 0;
    var rename_constraint = try parseAlterTableRenameOperationAlloc(alloc, rename_constraint_tokens.items, &rename_constraint_pos);
    defer rename_constraint.deinit(alloc);
    try std.testing.expectEqual(AlterTableRenameTargetSyntax.constraint, rename_constraint.target);
    try std.testing.expectEqualStrings("usage_status_key", rename_constraint.old_name);
    try std.testing.expectEqualStrings("usage_status_unique", rename_constraint.new_name);

    var rename_default_tokens = try lexer.tokenizeAlloc(alloc, "RENAME old_status TO status;");
    defer lexer.freeTokens(alloc, &rename_default_tokens);
    var rename_default_pos: usize = 0;
    var rename_default = try parseAlterTableRenameOperationAlloc(alloc, rename_default_tokens.items, &rename_default_pos);
    defer rename_default.deinit(alloc);
    try std.testing.expectEqual(AlterTableRenameTargetSyntax.column, rename_default.target);
    try std.testing.expectEqualStrings("old_status", rename_default.old_name);
    try std.testing.expectEqualStrings("status", rename_default.new_name);

    var invalid_rename_tokens = try lexer.tokenizeAlloc(alloc, "RENAME COLUMN old_status status;");
    defer lexer.freeTokens(alloc, &invalid_rename_tokens);
    var invalid_rename_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseAlterTableRenameOperationAlloc(alloc, invalid_rename_tokens.items, &invalid_rename_pos));

    var drop_column_tokens = try lexer.tokenizeAlloc(alloc, "DROP COLUMN IF EXISTS legacy_status RESTRICT, ADD COLUMN status text;");
    defer lexer.freeTokens(alloc, &drop_column_tokens);
    var drop_column_pos: usize = 0;
    var drop_column = try parseAlterTableDropOperationAlloc(alloc, drop_column_tokens.items, &drop_column_pos);
    defer drop_column.deinit(alloc);
    try std.testing.expectEqual(AlterTableDropTargetSyntax.column, drop_column.target);
    try std.testing.expect(drop_column.if_exists);
    try std.testing.expectEqualStrings("legacy_status", drop_column.name);
    try std.testing.expectEqual(ddl_plan.DropDependencyMode.restrict, drop_column.dependency_mode);
    try std.testing.expect(drop_column_tokens.items[drop_column_pos].kind == .comma);

    var drop_constraint_tokens = try lexer.tokenizeAlloc(alloc, "DROP CONSTRAINT usage_status_key CASCADE;");
    defer lexer.freeTokens(alloc, &drop_constraint_tokens);
    var drop_constraint_pos: usize = 0;
    var drop_constraint = try parseAlterTableDropOperationAlloc(alloc, drop_constraint_tokens.items, &drop_constraint_pos);
    defer drop_constraint.deinit(alloc);
    try std.testing.expectEqual(AlterTableDropTargetSyntax.constraint, drop_constraint.target);
    try std.testing.expect(!drop_constraint.if_exists);
    try std.testing.expectEqualStrings("usage_status_key", drop_constraint.name);
    try std.testing.expectEqual(ddl_plan.DropDependencyMode.cascade, drop_constraint.dependency_mode);

    var drop_default_tokens = try lexer.tokenizeAlloc(alloc, "DROP legacy_status;");
    defer lexer.freeTokens(alloc, &drop_default_tokens);
    var drop_default_pos: usize = 0;
    var drop_default = try parseAlterTableDropOperationAlloc(alloc, drop_default_tokens.items, &drop_default_pos);
    defer drop_default.deinit(alloc);
    try std.testing.expectEqual(AlterTableDropTargetSyntax.column, drop_default.target);
    try std.testing.expectEqualStrings("legacy_status", drop_default.name);
    try std.testing.expectEqual(ddl_plan.DropDependencyMode.cascade, drop_default.dependency_mode);

    var invalid_drop_tokens = try lexer.tokenizeAlloc(alloc, "DROP COLUMN IF legacy_status;");
    defer lexer.freeTokens(alloc, &invalid_drop_tokens);
    var invalid_drop_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseAlterTableDropOperationAlloc(alloc, invalid_drop_tokens.items, &invalid_drop_pos));

    var set_not_null_tokens = try lexer.tokenizeAlloc(alloc, "ALTER COLUMN status SET NOT NULL, DROP COLUMN legacy_status;");
    defer lexer.freeTokens(alloc, &set_not_null_tokens);
    var set_not_null_pos: usize = 0;
    var set_not_null = try parseAlterTableColumnNullabilityOperationAlloc(alloc, set_not_null_tokens.items, &set_not_null_pos);
    defer set_not_null.deinit(alloc);
    try std.testing.expectEqualStrings("status", set_not_null.column_name);
    try std.testing.expect(!set_not_null.nullable);
    try std.testing.expect(set_not_null_tokens.items[set_not_null_pos].kind == .comma);

    var drop_not_null_tokens = try lexer.tokenizeAlloc(alloc, "ALTER status DROP NOT NULL;");
    defer lexer.freeTokens(alloc, &drop_not_null_tokens);
    var drop_not_null_pos: usize = 0;
    var drop_not_null = try parseAlterTableColumnNullabilityOperationAlloc(alloc, drop_not_null_tokens.items, &drop_not_null_pos);
    defer drop_not_null.deinit(alloc);
    try std.testing.expectEqualStrings("status", drop_not_null.column_name);
    try std.testing.expect(drop_not_null.nullable);

    var set_default_tokens = try lexer.tokenizeAlloc(alloc, "ALTER COLUMN status SET DEFAULT 'active';");
    defer lexer.freeTokens(alloc, &set_default_tokens);
    var set_default_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseAlterTableColumnNullabilityOperationAlloc(alloc, set_default_tokens.items, &set_default_pos));

    set_default_pos = 0;
    var set_default = try parseAlterTableColumnDefaultOperationAlloc(alloc, set_default_tokens.items, &set_default_pos);
    defer set_default.deinit(alloc);
    try std.testing.expectEqualStrings("status", set_default.column_name);
    try std.testing.expectEqual(AlterTableColumnDefaultActionSyntax.set, set_default.action);
    try std.testing.expect(set_default_tokens.items[set_default_pos].kind == .string);

    var drop_column_default_tokens = try lexer.tokenizeAlloc(alloc, "ALTER COLUMN status DROP DEFAULT;");
    defer lexer.freeTokens(alloc, &drop_column_default_tokens);
    var drop_column_default_pos: usize = 0;
    var drop_column_default = try parseAlterTableColumnDefaultOperationAlloc(alloc, drop_column_default_tokens.items, &drop_column_default_pos);
    defer drop_column_default.deinit(alloc);
    try std.testing.expectEqualStrings("status", drop_column_default.column_name);
    try std.testing.expectEqual(AlterTableColumnDefaultActionSyntax.drop, drop_column_default.action);

    var drop_default_shorthand_tokens = try lexer.tokenizeAlloc(alloc, "ALTER status DROP DEFAULT;");
    defer lexer.freeTokens(alloc, &drop_default_shorthand_tokens);
    var drop_default_shorthand_pos: usize = 0;
    var drop_default_shorthand = try parseAlterTableColumnDefaultOperationAlloc(alloc, drop_default_shorthand_tokens.items, &drop_default_shorthand_pos);
    defer drop_default_shorthand.deinit(alloc);
    try std.testing.expectEqualStrings("status", drop_default_shorthand.column_name);
    try std.testing.expectEqual(AlterTableColumnDefaultActionSyntax.drop, drop_default_shorthand.action);

    var type_tokens = try lexer.tokenizeAlloc(alloc, "ALTER COLUMN status TYPE text;");
    defer lexer.freeTokens(alloc, &type_tokens);
    var type_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseAlterTableColumnDefaultOperationAlloc(alloc, type_tokens.items, &type_pos));

    type_pos = 0;
    var type_header = try parseAlterTableColumnTypeHeaderAlloc(alloc, type_tokens.items, &type_pos);
    defer type_header.deinit(alloc);
    try std.testing.expectEqualStrings("status", type_header.column_name);
    try std.testing.expect(std.ascii.eqlIgnoreCase(type_tokens.items[type_pos].text, "text"));

    var set_data_type_tokens = try lexer.tokenizeAlloc(alloc, "ALTER COLUMN status SET DATA TYPE varchar;");
    defer lexer.freeTokens(alloc, &set_data_type_tokens);
    var set_data_type_pos: usize = 0;
    var set_data_type = try parseAlterTableColumnTypeHeaderAlloc(alloc, set_data_type_tokens.items, &set_data_type_pos);
    defer set_data_type.deinit(alloc);
    try std.testing.expectEqualStrings("status", set_data_type.column_name);
    try std.testing.expect(std.ascii.eqlIgnoreCase(set_data_type_tokens.items[set_data_type_pos].text, "varchar"));

    var type_shorthand_tokens = try lexer.tokenizeAlloc(alloc, "ALTER status TYPE text;");
    defer lexer.freeTokens(alloc, &type_shorthand_tokens);
    var type_shorthand_pos: usize = 0;
    var type_shorthand = try parseAlterTableColumnTypeHeaderAlloc(alloc, type_shorthand_tokens.items, &type_shorthand_pos);
    defer type_shorthand.deinit(alloc);
    try std.testing.expectEqualStrings("status", type_shorthand.column_name);
    try std.testing.expect(std.ascii.eqlIgnoreCase(type_shorthand_tokens.items[type_shorthand_pos].text, "text"));

    var invalid_type_tokens = try lexer.tokenizeAlloc(alloc, "ALTER COLUMN status SET TYPE text;");
    defer lexer.freeTokens(alloc, &invalid_type_tokens);
    var invalid_type_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseAlterTableColumnTypeHeaderAlloc(alloc, invalid_type_tokens.items, &invalid_type_pos));

    var using_tokens = try lexer.tokenizeAlloc(alloc, "USING status,");
    defer lexer.freeTokens(alloc, &using_tokens);
    var using_pos: usize = 0;
    const using = (try parseOptionalAlterTableColumnUsing(using_tokens.items, &using_pos)).?;
    try std.testing.expectEqualStrings("status", using.column_name);
    try std.testing.expect(!using.wrapped);
    try std.testing.expect(using_tokens.items[using_pos].kind == .comma);

    var wrapped_using_tokens = try lexer.tokenizeAlloc(alloc, "USING (status);");
    defer lexer.freeTokens(alloc, &wrapped_using_tokens);
    var wrapped_using_pos: usize = 0;
    const wrapped_using = (try parseOptionalAlterTableColumnUsing(wrapped_using_tokens.items, &wrapped_using_pos)).?;
    try std.testing.expectEqualStrings("status", wrapped_using.column_name);
    try std.testing.expect(wrapped_using.wrapped);
    try std.testing.expect(wrapped_using_tokens.items[wrapped_using_pos].kind == .semicolon);

    var absent_using_tokens = try lexer.tokenizeAlloc(alloc, "COLLATE \"C\"");
    defer lexer.freeTokens(alloc, &absent_using_tokens);
    var absent_using_pos: usize = 0;
    try std.testing.expect((try parseOptionalAlterTableColumnUsing(absent_using_tokens.items, &absent_using_pos)) == null);
    try std.testing.expectEqual(@as(usize, 0), absent_using_pos);

    var malformed_using_tokens = try lexer.tokenizeAlloc(alloc, "USING (status;");
    defer lexer.freeTokens(alloc, &malformed_using_tokens);
    var malformed_using_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseOptionalAlterTableColumnUsing(malformed_using_tokens.items, &malformed_using_pos));

    var add_column_tokens = try lexer.tokenizeAlloc(alloc, "ADD COLUMN status text;");
    defer lexer.freeTokens(alloc, &add_column_tokens);
    var add_column_pos: usize = 0;
    const add_column = try parseAlterTableAddColumnHeader(add_column_tokens.items, &add_column_pos);
    try std.testing.expect(!add_column.if_not_exists);
    try std.testing.expect(std.ascii.eqlIgnoreCase(add_column_tokens.items[add_column_pos].text, "status"));

    var add_if_not_exists_tokens = try lexer.tokenizeAlloc(alloc, "ADD COLUMN IF NOT EXISTS status text;");
    defer lexer.freeTokens(alloc, &add_if_not_exists_tokens);
    var add_if_not_exists_pos: usize = 0;
    const add_if_not_exists = try parseAlterTableAddColumnHeader(add_if_not_exists_tokens.items, &add_if_not_exists_pos);
    try std.testing.expect(add_if_not_exists.if_not_exists);
    try std.testing.expect(std.ascii.eqlIgnoreCase(add_if_not_exists_tokens.items[add_if_not_exists_pos].text, "status"));

    var add_period_tokens = try lexer.tokenizeAlloc(alloc, "ADD PERIOD FOR valid_at (valid_from, valid_to);");
    defer lexer.freeTokens(alloc, &add_period_tokens);
    var add_period_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseAlterTableAddColumnHeader(add_period_tokens.items, &add_period_pos));

    add_period_pos = 0;
    var add_period_prefix = try parseAlterTableAddOperationPrefixAlloc(alloc, add_period_tokens.items, &add_period_pos);
    defer add_period_prefix.deinit(alloc);
    try std.testing.expectEqual(AlterTableAddOperationKindSyntax.period, add_period_prefix.kind);
    try std.testing.expect(std.ascii.eqlIgnoreCase(add_period_tokens.items[add_period_pos].text, "for"));

    var add_unique_tokens = try lexer.tokenizeAlloc(alloc, "ADD CONSTRAINT usage_records_status_key UNIQUE (tenant_id, status);");
    defer lexer.freeTokens(alloc, &add_unique_tokens);
    var add_unique_pos: usize = 0;
    var add_unique = try parseAlterTableAddOperationPrefixAlloc(alloc, add_unique_tokens.items, &add_unique_pos);
    defer add_unique.deinit(alloc);
    try std.testing.expectEqual(AlterTableAddOperationKindSyntax.unique, add_unique.kind);
    try std.testing.expectEqualStrings("usage_records_status_key", add_unique.constraint_name.?);
    try std.testing.expect(std.ascii.eqlIgnoreCase(add_unique_tokens.items[add_unique_pos].text, "unique"));

    var add_foreign_tokens = try lexer.tokenizeAlloc(alloc, "ADD FOREIGN KEY (tenant_id) REFERENCES tenants(id);");
    defer lexer.freeTokens(alloc, &add_foreign_tokens);
    var add_foreign_pos: usize = 0;
    var add_foreign = try parseAlterTableAddOperationPrefixAlloc(alloc, add_foreign_tokens.items, &add_foreign_pos);
    defer add_foreign.deinit(alloc);
    try std.testing.expectEqual(AlterTableAddOperationKindSyntax.foreign_key, add_foreign.kind);
    try std.testing.expect(add_foreign.constraint_name == null);
    try std.testing.expect(std.ascii.eqlIgnoreCase(add_foreign_tokens.items[add_foreign_pos].text, "foreign"));

    var add_primary_tokens = try lexer.tokenizeAlloc(alloc, "ADD CONSTRAINT usage_records_pk PRIMARY KEY (tenant_id, id);");
    defer lexer.freeTokens(alloc, &add_primary_tokens);
    var add_primary_pos: usize = 0;
    var add_primary = try parseAlterTableAddOperationPrefixAlloc(alloc, add_primary_tokens.items, &add_primary_pos);
    defer add_primary.deinit(alloc);
    try std.testing.expectEqual(AlterTableAddOperationKindSyntax.primary_key, add_primary.kind);
    try std.testing.expectEqualStrings("usage_records_pk", add_primary.constraint_name.?);
    try std.testing.expect(std.ascii.eqlIgnoreCase(add_primary_tokens.items[add_primary_pos].text, "primary"));

    var add_check_tokens = try lexer.tokenizeAlloc(alloc, "ADD CHECK (amount >= 0);");
    defer lexer.freeTokens(alloc, &add_check_tokens);
    var add_check_pos: usize = 0;
    var add_check = try parseAlterTableAddOperationPrefixAlloc(alloc, add_check_tokens.items, &add_check_pos);
    defer add_check.deinit(alloc);
    try std.testing.expectEqual(AlterTableAddOperationKindSyntax.check, add_check.kind);
    try std.testing.expect(add_check.constraint_name == null);
    try std.testing.expect(std.ascii.eqlIgnoreCase(add_check_tokens.items[add_check_pos].text, "check"));

    var add_unsupported_tokens = try lexer.tokenizeAlloc(alloc, "ADD COLUMN status text;");
    defer lexer.freeTokens(alloc, &add_unsupported_tokens);
    var add_unsupported_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseAlterTableAddOperationPrefixAlloc(alloc, add_unsupported_tokens.items, &add_unsupported_pos));
}

test "sql adapter grammar parses identity allocator table headers" {
    const alloc = std.testing.allocator;

    var serial_tokens = try lexer.tokenizeAlloc(alloc, "TABLE public.usage_records (id bigserial PRIMARY KEY);");
    defer lexer.freeTokens(alloc, &serial_tokens);
    var serial_pos: usize = 0;
    var serial = try parseIdentityAllocatorTableHeaderAlloc(alloc, serial_tokens.items, &serial_pos);
    defer serial.deinit(alloc);
    try std.testing.expectEqualStrings("usage_records", serial.table_name);
    try std.testing.expectEqualStrings("id", serial.column_name);
    try std.testing.expect(std.ascii.eqlIgnoreCase(serial_tokens.items[serial_pos].text, "bigserial"));

    var generated_tokens = try lexer.tokenizeAlloc(alloc, "TABLE usage_records (id bigint GENERATED BY DEFAULT AS IDENTITY);");
    defer lexer.freeTokens(alloc, &generated_tokens);
    var generated_pos: usize = 0;
    var generated = try parseIdentityAllocatorTableHeaderAlloc(alloc, generated_tokens.items, &generated_pos);
    defer generated.deinit(alloc);
    try std.testing.expectEqualStrings("usage_records", generated.table_name);
    try std.testing.expectEqualStrings("id", generated.column_name);
    try std.testing.expect(std.ascii.eqlIgnoreCase(generated_tokens.items[generated_pos].text, "bigint"));

    var if_not_exists_tokens = try lexer.tokenizeAlloc(alloc, "TABLE IF NOT EXISTS usage_records (id bigserial);");
    defer lexer.freeTokens(alloc, &if_not_exists_tokens);
    var if_not_exists_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseIdentityAllocatorTableHeaderAlloc(alloc, if_not_exists_tokens.items, &if_not_exists_pos));
}

test "sql adapter grammar parses identity allocator specs" {
    const alloc = std.testing.allocator;

    var serial_tokens = try lexer.tokenizeAlloc(alloc, "serial PRIMARY KEY");
    defer lexer.freeTokens(alloc, &serial_tokens);
    var serial_pos: usize = 0;
    var serial = try parseIdentityAllocatorSpecAlloc(alloc, serial_tokens.items, &serial_pos);
    defer serial.deinit(alloc);
    try std.testing.expectEqual(ddl_plan.IdentityAllocatorKind.serial, serial.kind);
    try std.testing.expect(std.ascii.eqlIgnoreCase(serial_tokens.items[serial_pos].text, "primary"));

    var bigserial_tokens = try lexer.tokenizeAlloc(alloc, "bigserial)");
    defer lexer.freeTokens(alloc, &bigserial_tokens);
    var bigserial_pos: usize = 0;
    var bigserial = try parseIdentityAllocatorSpecAlloc(alloc, bigserial_tokens.items, &bigserial_pos);
    defer bigserial.deinit(alloc);
    try std.testing.expectEqual(ddl_plan.IdentityAllocatorKind.bigserial, bigserial.kind);
    try std.testing.expect(bigserial_tokens.items[bigserial_pos].kind == .rparen);

    var generated_tokens = try lexer.tokenizeAlloc(alloc, "bigint GENERATED BY DEFAULT AS IDENTITY (START WITH 10 INCREMENT BY 2 CACHE 8) PRIMARY KEY");
    defer lexer.freeTokens(alloc, &generated_tokens);
    var generated_pos: usize = 0;
    var generated = try parseIdentityAllocatorSpecAlloc(alloc, generated_tokens.items, &generated_pos);
    defer generated.deinit(alloc);
    try std.testing.expectEqual(ddl_plan.IdentityAllocatorKind.generated_by_default, generated.kind);
    try std.testing.expectEqual(@as(?i64, 10), generated.options.start_with);
    try std.testing.expectEqual(@as(?i64, 2), generated.options.increment_by);
    try std.testing.expectEqual(@as(?i64, 8), generated.options.cache);
    try std.testing.expect(std.ascii.eqlIgnoreCase(generated_tokens.items[generated_pos].text, "primary"));

    var always_tokens = try lexer.tokenizeAlloc(alloc, "numeric(18, 0) GENERATED ALWAYS AS IDENTITY");
    defer lexer.freeTokens(alloc, &always_tokens);
    var always_pos: usize = 0;
    var always = try parseIdentityAllocatorSpecAlloc(alloc, always_tokens.items, &always_pos);
    defer always.deinit(alloc);
    try std.testing.expectEqual(ddl_plan.IdentityAllocatorKind.generated_always, always.kind);
    try std.testing.expectEqual(always_tokens.items.len, always_pos);

    var text_tokens = try lexer.tokenizeAlloc(alloc, "text GENERATED BY DEFAULT AS IDENTITY");
    defer lexer.freeTokens(alloc, &text_tokens);
    var text_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseIdentityAllocatorSpecAlloc(alloc, text_tokens.items, &text_pos));
}

test "sql adapter grammar parses create table definition headers" {
    const alloc = std.testing.allocator;

    var create_tokens = try lexer.tokenizeAlloc(alloc, "TABLE IF NOT EXISTS public.usage_records (id uuid PRIMARY KEY);");
    defer lexer.freeTokens(alloc, &create_tokens);
    var create_pos: usize = 0;
    var create = try parseCreateTableDefinitionHeaderAlloc(alloc, create_tokens.items, &create_pos);
    defer create.deinit(alloc);
    try std.testing.expectEqualStrings("usage_records", create.table_name);
    try std.testing.expect(create.if_not_exists);
    try std.testing.expect(create_tokens.items[create_pos].kind == .identifier);
    try std.testing.expectEqualStrings("id", create_tokens.items[create_pos].text);

    var plain_tokens = try lexer.tokenizeAlloc(alloc, "TABLE usage_records (id uuid);");
    defer lexer.freeTokens(alloc, &plain_tokens);
    var plain_pos: usize = 0;
    var plain = try parseCreateTableDefinitionHeaderAlloc(alloc, plain_tokens.items, &plain_pos);
    defer plain.deinit(alloc);
    try std.testing.expectEqualStrings("usage_records", plain.table_name);
    try std.testing.expect(!plain.if_not_exists);

    var missing_open_tokens = try lexer.tokenizeAlloc(alloc, "TABLE usage_records id uuid);");
    defer lexer.freeTokens(alloc, &missing_open_tokens);
    var missing_open_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseCreateTableDefinitionHeaderAlloc(alloc, missing_open_tokens.items, &missing_open_pos));

    var column_tokens = try lexer.tokenizeAlloc(alloc, "id uuid PRIMARY KEY, status text);");
    defer lexer.freeTokens(alloc, &column_tokens);
    var column_pos: usize = 0;
    var column = try parseCreateTableElementPrefixAlloc(alloc, column_tokens.items, &column_pos);
    defer column.deinit(alloc);
    try std.testing.expectEqual(CreateTableElementKindSyntax.column, column.kind);
    try std.testing.expect(column.constraint_name == null);
    try std.testing.expectEqualStrings("id", column_tokens.items[column_pos].text);

    var named_primary_tokens = try lexer.tokenizeAlloc(alloc, "CONSTRAINT usage_records_pk PRIMARY KEY (tenant_id, id), status text);");
    defer lexer.freeTokens(alloc, &named_primary_tokens);
    var named_primary_pos: usize = 0;
    var named_primary = try parseCreateTableElementPrefixAlloc(alloc, named_primary_tokens.items, &named_primary_pos);
    defer named_primary.deinit(alloc);
    try std.testing.expectEqual(CreateTableElementKindSyntax.primary_key, named_primary.kind);
    try std.testing.expectEqualStrings("usage_records_pk", named_primary.constraint_name.?);
    try std.testing.expect(std.ascii.eqlIgnoreCase(named_primary_tokens.items[named_primary_pos].text, "primary"));

    var foreign_tokens = try lexer.tokenizeAlloc(alloc, "FOREIGN KEY (tenant_id) REFERENCES tenants(id));");
    defer lexer.freeTokens(alloc, &foreign_tokens);
    var foreign_pos: usize = 0;
    var foreign = try parseCreateTableElementPrefixAlloc(alloc, foreign_tokens.items, &foreign_pos);
    defer foreign.deinit(alloc);
    try std.testing.expectEqual(CreateTableElementKindSyntax.foreign_key, foreign.kind);
    try std.testing.expect(foreign.constraint_name == null);
    try std.testing.expect(std.ascii.eqlIgnoreCase(foreign_tokens.items[foreign_pos].text, "foreign"));

    var period_tokens = try lexer.tokenizeAlloc(alloc, "PERIOD FOR valid_at (valid_from, valid_to));");
    defer lexer.freeTokens(alloc, &period_tokens);
    var period_pos: usize = 0;
    var period = try parseCreateTableElementPrefixAlloc(alloc, period_tokens.items, &period_pos);
    defer period.deinit(alloc);
    try std.testing.expectEqual(CreateTableElementKindSyntax.period, period.kind);
    try std.testing.expect(period.constraint_name == null);
    try std.testing.expect(std.ascii.eqlIgnoreCase(period_tokens.items[period_pos].text, "period"));

    var invalid_constraint_tokens = try lexer.tokenizeAlloc(alloc, "CONSTRAINT usage_records_status text);");
    defer lexer.freeTokens(alloc, &invalid_constraint_tokens);
    var invalid_constraint_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseCreateTableElementPrefixAlloc(alloc, invalid_constraint_tokens.items, &invalid_constraint_pos));

    var inline_primary_tokens = try lexer.tokenizeAlloc(alloc, "PRIMARY KEY, status text);");
    defer lexer.freeTokens(alloc, &inline_primary_tokens);
    var inline_primary_pos: usize = 0;
    var inline_primary = try parseCreateTableColumnConstraintPrefixAlloc(alloc, inline_primary_tokens.items, &inline_primary_pos);
    defer inline_primary.deinit(alloc);
    try std.testing.expectEqual(CreateTableColumnConstraintKindSyntax.primary_key, inline_primary.kind);
    try std.testing.expect(inline_primary.constraint_name == null);
    try std.testing.expect(std.ascii.eqlIgnoreCase(inline_primary_tokens.items[inline_primary_pos].text, "primary"));

    var inline_named_unique_tokens = try lexer.tokenizeAlloc(alloc, "CONSTRAINT usage_status_key UNIQUE, status text);");
    defer lexer.freeTokens(alloc, &inline_named_unique_tokens);
    var inline_named_unique_pos: usize = 0;
    var inline_named_unique = try parseCreateTableColumnConstraintPrefixAlloc(alloc, inline_named_unique_tokens.items, &inline_named_unique_pos);
    defer inline_named_unique.deinit(alloc);
    try std.testing.expectEqual(CreateTableColumnConstraintKindSyntax.unique, inline_named_unique.kind);
    try std.testing.expectEqualStrings("usage_status_key", inline_named_unique.constraint_name.?);
    try std.testing.expect(std.ascii.eqlIgnoreCase(inline_named_unique_tokens.items[inline_named_unique_pos].text, "unique"));

    var inline_named_check_tokens = try lexer.tokenizeAlloc(alloc, "CONSTRAINT usage_amount_check CHECK (amount >= 0));");
    defer lexer.freeTokens(alloc, &inline_named_check_tokens);
    var inline_named_check_pos: usize = 0;
    var inline_named_check = try parseCreateTableColumnConstraintPrefixAlloc(alloc, inline_named_check_tokens.items, &inline_named_check_pos);
    defer inline_named_check.deinit(alloc);
    try std.testing.expectEqual(CreateTableColumnConstraintKindSyntax.check, inline_named_check.kind);
    try std.testing.expectEqualStrings("usage_amount_check", inline_named_check.constraint_name.?);
    try std.testing.expect(std.ascii.eqlIgnoreCase(inline_named_check_tokens.items[inline_named_check_pos].text, "check"));

    var inline_refs_tokens = try lexer.tokenizeAlloc(alloc, "REFERENCES tenants(id));");
    defer lexer.freeTokens(alloc, &inline_refs_tokens);
    var inline_refs_pos: usize = 0;
    var inline_refs = try parseCreateTableColumnConstraintPrefixAlloc(alloc, inline_refs_tokens.items, &inline_refs_pos);
    defer inline_refs.deinit(alloc);
    try std.testing.expectEqual(CreateTableColumnConstraintKindSyntax.references, inline_refs.kind);
    try std.testing.expect(inline_refs.constraint_name == null);
    try std.testing.expect(std.ascii.eqlIgnoreCase(inline_refs_tokens.items[inline_refs_pos].text, "references"));

    var inline_invalid_tokens = try lexer.tokenizeAlloc(alloc, "CONSTRAINT usage_status NOT NULL);");
    defer lexer.freeTokens(alloc, &inline_invalid_tokens);
    var inline_invalid_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseCreateTableColumnConstraintPrefixAlloc(alloc, inline_invalid_tokens.items, &inline_invalid_pos));
}

test "sql adapter grammar parses table clone catalog tails" {
    const alloc = std.testing.allocator;

    var clone_tokens = try lexer.tokenizeAlloc(alloc, "TABLE IF NOT EXISTS public.usage_records_copy (LIKE public.usage_records INCLUDING ALL EXCLUDING COMMENTS EXCLUDING INDEXES INCLUDING UPDATE POLICIES);");
    defer lexer.freeTokens(alloc, &clone_tokens);
    var clone_pos: usize = 0;
    var clone = try parseCreateTableCloneCatalogTailAlloc(alloc, clone_tokens.items, &clone_pos);
    defer clone.deinit(alloc);
    try std.testing.expectEqual(clone_tokens.items.len, clone_pos);
    try std.testing.expectEqualStrings("usage_records_copy", clone.table_name);
    try std.testing.expectEqualStrings("usage_records", clone.source_table_name);
    try std.testing.expect(clone.if_not_exists);
    try std.testing.expect(clone.options.defaults);
    try std.testing.expect(clone.options.generated);
    try std.testing.expect(clone.options.checks);
    try std.testing.expect(clone.options.constraints);
    try std.testing.expect(!clone.options.indexes);
    try std.testing.expect(clone.options.periods);
    try std.testing.expect(clone.options.update_policies);

    var minimal_tokens = try lexer.tokenizeAlloc(alloc, "TABLE usage_records_copy (LIKE usage_records);");
    defer lexer.freeTokens(alloc, &minimal_tokens);
    var minimal_pos: usize = 0;
    var minimal = try parseCreateTableCloneCatalogTailAlloc(alloc, minimal_tokens.items, &minimal_pos);
    defer minimal.deinit(alloc);
    try std.testing.expectEqual(minimal_tokens.items.len, minimal_pos);
    try std.testing.expect(!minimal.if_not_exists);
    try std.testing.expect(!minimal.options.defaults);
    try std.testing.expect(!minimal.options.constraints);

    var temporary_tokens = try lexer.tokenizeAlloc(alloc, "TEMPORARY TABLE usage_records_copy (LIKE usage_records);");
    defer lexer.freeTokens(alloc, &temporary_tokens);
    var temporary_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseCreateTableCloneCatalogTailAlloc(alloc, temporary_tokens.items, &temporary_pos));
}

test "sql adapter grammar parses table partition catalog tails" {
    const alloc = std.testing.allocator;

    var partitioned_tokens = try lexer.tokenizeAlloc(alloc, "PARTITION BY RANGE (tenant_id, event_date);");
    defer lexer.freeTokens(alloc, &partitioned_tokens);
    var partitioned_pos: usize = 0;
    var partitioned = try parseCreatePartitionedTableCatalogTailAlloc(alloc, partitioned_tokens.items, &partitioned_pos);
    defer partitioned.deinit(alloc);
    try std.testing.expectEqual(partitioned_tokens.items.len, partitioned_pos);
    try std.testing.expectEqual(ddl_plan.TablePartitionMethod.range, partitioned.method);
    try std.testing.expectEqual(@as(usize, 2), partitioned.keys.len);
    try std.testing.expectEqualStrings("tenant_id", partitioned.keys[0]);
    try std.testing.expectEqualStrings("event_date", partitioned.keys[1]);

    var create_tokens = try lexer.tokenizeAlloc(alloc, "TABLE public.usage_events_2026 PARTITION OF public.usage_events FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');");
    defer lexer.freeTokens(alloc, &create_tokens);
    var create_pos: usize = 0;
    var create = try parseCreateTablePartitionCatalogTailAlloc(alloc, create_tokens.items, &create_pos);
    defer create.deinit(alloc);
    try std.testing.expectEqual(create_tokens.items.len, create_pos);
    try std.testing.expectEqualStrings("usage_events_2026", create.table_name);
    try std.testing.expectEqualStrings("usage_events", create.parent_table_name);
    try std.testing.expectEqualStrings("\"2026-01-01\"", create.bounds.lower_json);
    try std.testing.expectEqualStrings("\"2027-01-01\"", create.bounds.upper_json);

    var attach_tokens = try lexer.tokenizeAlloc(alloc, "TABLE usage_events ATTACH PARTITION usage_events_2026 FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');");
    defer lexer.freeTokens(alloc, &attach_tokens);
    var attach_pos: usize = 0;
    var attach = try parseAlterTablePartitionCatalogTailAlloc(alloc, attach_tokens.items, &attach_pos);
    defer attach.deinit(alloc);
    try std.testing.expectEqual(attach_tokens.items.len, attach_pos);
    switch (attach) {
        .attach => |plan| {
            try std.testing.expectEqualStrings("usage_events", plan.parent_table_name);
            try std.testing.expectEqualStrings("usage_events_2026", plan.partition_table_name);
            try std.testing.expectEqualStrings("\"2026-01-01\"", plan.bounds.lower_json);
            try std.testing.expectEqualStrings("\"2027-01-01\"", plan.bounds.upper_json);
        },
        else => return error.TestExpectedEqual,
    }

    var detach_tokens = try lexer.tokenizeAlloc(alloc, "TABLE usage_events DETACH PARTITION usage_events_2026;");
    defer lexer.freeTokens(alloc, &detach_tokens);
    var detach_pos: usize = 0;
    var detach = try parseAlterTablePartitionCatalogTailAlloc(alloc, detach_tokens.items, &detach_pos);
    defer detach.deinit(alloc);
    try std.testing.expectEqual(detach_tokens.items.len, detach_pos);
    switch (detach) {
        .detach => |plan| {
            try std.testing.expectEqualStrings("usage_events", plan.parent_table_name);
            try std.testing.expectEqualStrings("usage_events_2026", plan.partition_table_name);
        },
        else => return error.TestExpectedEqual,
    }

    var if_not_exists_tokens = try lexer.tokenizeAlloc(alloc, "TABLE IF NOT EXISTS usage_events_2026 PARTITION OF usage_events FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');");
    defer lexer.freeTokens(alloc, &if_not_exists_tokens);
    var if_not_exists_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseCreateTablePartitionCatalogTailAlloc(alloc, if_not_exists_tokens.items, &if_not_exists_pos));

    var hash_tokens = try lexer.tokenizeAlloc(alloc, "PARTITION BY HASH (tenant_id);");
    defer lexer.freeTokens(alloc, &hash_tokens);
    var hash_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseCreatePartitionedTableCatalogTailAlloc(alloc, hash_tokens.items, &hash_pos));
}

test "sql adapter grammar parses view catalog tails" {
    const alloc = std.testing.allocator;

    var create_view_tokens = try lexer.tokenizeAlloc(alloc, "VIEW public.active_accounts(account_id, contact_email) AS SELECT id, email FROM public.accounts;");
    defer lexer.freeTokens(alloc, &create_view_tokens);
    var create_view_pos: usize = 0;
    var create_view = try parseCreateViewCatalogTailAlloc(alloc, create_view_tokens.items, &create_view_pos, false);
    defer create_view.deinit(alloc);
    try std.testing.expectEqual(create_view_tokens.items.len, create_view_pos);
    try std.testing.expectEqualStrings("active_accounts", create_view.view_name);
    try std.testing.expectEqualStrings("accounts", create_view.source_table_name);
    try std.testing.expect(!create_view.replace_existing);
    try std.testing.expect(!create_view.if_not_exists);
    try std.testing.expectEqual(@as(usize, 2), create_view.source_fields.len);
    try std.testing.expectEqualStrings("id", create_view.source_fields[0]);
    try std.testing.expectEqualStrings("email", create_view.source_fields[1]);
    try std.testing.expectEqualStrings("account_id", create_view.output_fields[0]);
    try std.testing.expectEqualStrings("contact_email", create_view.output_fields[1]);

    var replace_view_tokens = try lexer.tokenizeAlloc(alloc, "VIEW IF NOT EXISTS active_accounts AS SELECT id AS account_id FROM accounts;");
    defer lexer.freeTokens(alloc, &replace_view_tokens);
    var replace_view_pos: usize = 0;
    var replace_view = try parseCreateViewCatalogTailAlloc(alloc, replace_view_tokens.items, &replace_view_pos, true);
    defer replace_view.deinit(alloc);
    try std.testing.expectEqual(replace_view_tokens.items.len, replace_view_pos);
    try std.testing.expect(replace_view.replace_existing);
    try std.testing.expect(replace_view.if_not_exists);
    try std.testing.expectEqualStrings("account_id", replace_view.output_fields[0]);

    var create_mv_tokens = try lexer.tokenizeAlloc(alloc, "MATERIALIZED VIEW account_rollups(record_id, record_status) AS SELECT id, status FROM usage_records WITH NO DATA;");
    defer lexer.freeTokens(alloc, &create_mv_tokens);
    var create_mv_pos: usize = 0;
    var create_mv = try parseCreateMaterializedViewCatalogTailAlloc(alloc, create_mv_tokens.items, &create_mv_pos, true);
    defer create_mv.deinit(alloc);
    try std.testing.expectEqual(create_mv_tokens.items.len, create_mv_pos);
    try std.testing.expectEqualStrings("account_rollups", create_mv.view_name);
    try std.testing.expect(create_mv.replace_existing);
    try std.testing.expect(!create_mv.if_not_exists);
    try std.testing.expect(!create_mv.populate_on_create);
    try std.testing.expectEqualStrings("id", create_mv.source_fields[0]);
    try std.testing.expectEqualStrings("record_id", create_mv.output_fields[0]);

    var bad_alias_tokens = try lexer.tokenizeAlloc(alloc, "VIEW active_accounts(account_id) AS SELECT id, email FROM accounts;");
    defer lexer.freeTokens(alloc, &bad_alias_tokens);
    var bad_alias_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseCreateViewCatalogTailAlloc(alloc, bad_alias_tokens.items, &bad_alias_pos, false));

    var drop_view_tokens = try lexer.tokenizeAlloc(alloc, "VIEW IF EXISTS public.active_accounts CASCADE;");
    defer lexer.freeTokens(alloc, &drop_view_tokens);
    var drop_view_pos: usize = 0;
    var drop_view = try parseDropViewCatalogTailAlloc(alloc, drop_view_tokens.items, &drop_view_pos);
    defer drop_view.deinit(alloc);
    try std.testing.expectEqual(drop_view_tokens.items.len, drop_view_pos);
    try std.testing.expectEqualStrings("active_accounts", drop_view.view_name);
    try std.testing.expect(drop_view.if_exists);
    try std.testing.expect(drop_view.cascade);

    var drop_mv_tokens = try lexer.tokenizeAlloc(alloc, "MATERIALIZED VIEW IF EXISTS public.account_rollups RESTRICT;");
    defer lexer.freeTokens(alloc, &drop_mv_tokens);
    var drop_mv_pos: usize = 0;
    var drop_mv = try parseDropMaterializedViewCatalogTailAlloc(alloc, drop_mv_tokens.items, &drop_mv_pos);
    defer drop_mv.deinit(alloc);
    try std.testing.expectEqual(drop_mv_tokens.items.len, drop_mv_pos);
    try std.testing.expectEqualStrings("account_rollups", drop_mv.view_name);
    try std.testing.expect(drop_mv.if_exists);
    try std.testing.expect(!drop_mv.cascade);

    var multi_view_tokens = try lexer.tokenizeAlloc(alloc, "VIEW active_accounts, inactive_accounts;");
    defer lexer.freeTokens(alloc, &multi_view_tokens);
    var multi_view_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseDropViewCatalogTailAlloc(alloc, multi_view_tokens.items, &multi_view_pos));

    var refresh_tokens = try lexer.tokenizeAlloc(alloc, "MATERIALIZED VIEW CONCURRENTLY public.account_rollups WITH NO DATA;");
    defer lexer.freeTokens(alloc, &refresh_tokens);
    var refresh_pos: usize = 0;
    var refresh = try parseRefreshMaterializedViewCatalogTailAlloc(alloc, refresh_tokens.items, &refresh_pos);
    defer refresh.deinit(alloc);
    try std.testing.expectEqual(refresh_tokens.items.len, refresh_pos);
    try std.testing.expectEqualStrings("account_rollups", refresh.view_name);
    try std.testing.expect(refresh.concurrently);
    try std.testing.expect(!refresh.populate);

    var rename_tokens = try lexer.tokenizeAlloc(alloc, "VIEW public.active_accounts RENAME TO current_accounts;");
    defer lexer.freeTokens(alloc, &rename_tokens);
    var rename_pos: usize = 0;
    var rename = try parseRenameViewCatalogTailAlloc(alloc, rename_tokens.items, &rename_pos);
    defer rename.deinit(alloc);
    try std.testing.expectEqual(rename_tokens.items.len, rename_pos);
    try std.testing.expectEqualStrings("active_accounts", rename.view_name);
    try std.testing.expectEqualStrings("current_accounts", rename.new_view_name);
}

fn expectAlterRoleLiteralSettingValue(expected: []const u8, value: ?ddl_plan.AlterRolePlan.SettingValue) !void {
    const actual = value orelse return error.TestUnexpectedResult;
    switch (actual) {
        .literal => |literal| try std.testing.expectEqualStrings(expected, literal),
        .current_setting => return error.TestUnexpectedResult,
    }
}

test "sql adapter grammar parses authorization catalog tails" {
    const alloc = std.testing.allocator;

    var create_role_tokens = try lexer.tokenizeAlloc(alloc, "ROLE app_writer;");
    defer lexer.freeTokens(alloc, &create_role_tokens);
    var create_role_pos: usize = 0;
    var create_role = try parseCreateRoleCatalogTailAlloc(alloc, create_role_tokens.items, &create_role_pos);
    defer create_role.deinit(alloc);
    try std.testing.expectEqual(create_role_tokens.items.len, create_role_pos);
    try std.testing.expectEqualStrings("app_writer", create_role.role_name);

    var create_user_tokens = try lexer.tokenizeAlloc(alloc, "USER app_writer;");
    defer lexer.freeTokens(alloc, &create_user_tokens);
    var create_user_pos: usize = 0;
    var create_user = try parseCreateRoleCatalogTailAlloc(alloc, create_user_tokens.items, &create_user_pos);
    defer create_user.deinit(alloc);
    try std.testing.expectEqual(create_user_tokens.items.len, create_user_pos);
    try std.testing.expectEqualStrings("app_writer", create_user.role_name);

    var create_group_tokens = try lexer.tokenizeAlloc(alloc, "GROUP app_readers;");
    defer lexer.freeTokens(alloc, &create_group_tokens);
    var create_group_pos: usize = 0;
    var create_group = try parseCreateRoleCatalogTailAlloc(alloc, create_group_tokens.items, &create_group_pos);
    defer create_group.deinit(alloc);
    try std.testing.expectEqual(create_group_tokens.items.len, create_group_pos);
    try std.testing.expectEqualStrings("app_readers", create_group.role_name);

    var alter_role_tokens = try lexer.tokenizeAlloc(alloc, "ROLE app_writer SET app.tenant_id = 'acme';");
    defer lexer.freeTokens(alloc, &alter_role_tokens);
    var alter_role_pos: usize = 0;
    var alter_role = try parseAlterRoleCatalogTailAlloc(alloc, alter_role_tokens.items, &alter_role_pos);
    defer alter_role.deinit(alloc);
    try std.testing.expectEqual(alter_role_tokens.items.len, alter_role_pos);
    try std.testing.expectEqualStrings("app_writer", alter_role.role_name);
    try std.testing.expectEqual(ddl_plan.AlterRolePlan.Operation.set, alter_role.operation);
    try std.testing.expectEqual(ddl_plan.AlterRolePlan.SettingKind.app, alter_role.setting_kind);
    try std.testing.expectEqualStrings("app.tenant_id", alter_role.setting_name);
    try expectAlterRoleLiteralSettingValue("acme", alter_role.setting_value);

    var alter_user_tokens = try lexer.tokenizeAlloc(alloc, "USER app_writer RESET statement_timeout;");
    defer lexer.freeTokens(alloc, &alter_user_tokens);
    var alter_user_pos: usize = 0;
    var alter_user = try parseAlterRoleCatalogTailAlloc(alloc, alter_user_tokens.items, &alter_user_pos);
    defer alter_user.deinit(alloc);
    try std.testing.expectEqual(alter_user_tokens.items.len, alter_user_pos);
    try std.testing.expectEqualStrings("app_writer", alter_user.role_name);
    try std.testing.expectEqual(ddl_plan.AlterRolePlan.Operation.reset, alter_user.operation);
    try std.testing.expectEqualStrings("statement_timeout", alter_user.setting_name);

    var alter_group_tokens = try lexer.tokenizeAlloc(alloc, "GROUP app_readers RESET statement_timeout;");
    defer lexer.freeTokens(alloc, &alter_group_tokens);
    var alter_group_pos: usize = 0;
    var alter_group = try parseAlterRoleCatalogTailAlloc(alloc, alter_group_tokens.items, &alter_group_pos);
    defer alter_group.deinit(alloc);
    try std.testing.expectEqual(alter_group_tokens.items.len, alter_group_pos);
    try std.testing.expectEqualStrings("app_readers", alter_group.role_name);
    try std.testing.expectEqual(ddl_plan.AlterRolePlan.Operation.reset, alter_group.operation);
    try std.testing.expectEqualStrings("statement_timeout", alter_group.setting_name);

    var scoped_alter_role_tokens = try lexer.tokenizeAlloc(alloc, "ROLE app_writer IN DATABASE appdb SET app.tenant_id = 'acme';");
    defer lexer.freeTokens(alloc, &scoped_alter_role_tokens);
    var scoped_alter_role_pos: usize = 0;
    var scoped_alter_role = try parseAlterRoleCatalogTailAlloc(alloc, scoped_alter_role_tokens.items, &scoped_alter_role_pos);
    defer scoped_alter_role.deinit(alloc);
    try std.testing.expectEqual(scoped_alter_role_tokens.items.len, scoped_alter_role_pos);
    try std.testing.expectEqualStrings("app_writer", scoped_alter_role.role_name);
    try std.testing.expectEqualStrings("appdb", scoped_alter_role.database_name.?);
    try std.testing.expectEqual(ddl_plan.AlterRolePlan.Operation.set, scoped_alter_role.operation);
    try std.testing.expectEqual(ddl_plan.AlterRolePlan.SettingKind.app, scoped_alter_role.setting_kind);
    try std.testing.expectEqualStrings("app.tenant_id", scoped_alter_role.setting_name);
    try expectAlterRoleLiteralSettingValue("acme", scoped_alter_role.setting_value);

    var reset_role_tokens = try lexer.tokenizeAlloc(alloc, "ROLE app_writer RESET app.tenant_id;");
    defer lexer.freeTokens(alloc, &reset_role_tokens);
    var reset_role_pos: usize = 0;
    var reset_role = try parseAlterRoleCatalogTailAlloc(alloc, reset_role_tokens.items, &reset_role_pos);
    defer reset_role.deinit(alloc);
    try std.testing.expectEqual(reset_role_tokens.items.len, reset_role_pos);
    try std.testing.expectEqualStrings("app_writer", reset_role.role_name);
    try std.testing.expectEqual(ddl_plan.AlterRolePlan.Operation.reset, reset_role.operation);
    try std.testing.expectEqual(ddl_plan.AlterRolePlan.SettingKind.app, reset_role.setting_kind);
    try std.testing.expectEqualStrings("app.tenant_id", reset_role.setting_name);
    try std.testing.expect(reset_role.setting_value == null);

    var scoped_reset_role_tokens = try lexer.tokenizeAlloc(alloc, "ROLE app_writer IN DATABASE appdb RESET app.tenant_id;");
    defer lexer.freeTokens(alloc, &scoped_reset_role_tokens);
    var scoped_reset_role_pos: usize = 0;
    var scoped_reset_role = try parseAlterRoleCatalogTailAlloc(alloc, scoped_reset_role_tokens.items, &scoped_reset_role_pos);
    defer scoped_reset_role.deinit(alloc);
    try std.testing.expectEqual(scoped_reset_role_tokens.items.len, scoped_reset_role_pos);
    try std.testing.expectEqualStrings("app_writer", scoped_reset_role.role_name);
    try std.testing.expectEqualStrings("appdb", scoped_reset_role.database_name.?);
    try std.testing.expectEqual(ddl_plan.AlterRolePlan.Operation.reset, scoped_reset_role.operation);
    try std.testing.expectEqual(ddl_plan.AlterRolePlan.SettingKind.app, scoped_reset_role.setting_kind);
    try std.testing.expectEqualStrings("app.tenant_id", scoped_reset_role.setting_name);
    try std.testing.expect(scoped_reset_role.setting_value == null);

    var runtime_role_setting_tokens = try lexer.tokenizeAlloc(alloc, "ROLE app_writer SET statement_timeout = '1ms';");
    defer lexer.freeTokens(alloc, &runtime_role_setting_tokens);
    var runtime_role_setting_pos: usize = 0;
    var runtime_role_setting = try parseAlterRoleCatalogTailAlloc(alloc, runtime_role_setting_tokens.items, &runtime_role_setting_pos);
    defer runtime_role_setting.deinit(alloc);
    try std.testing.expectEqual(runtime_role_setting_tokens.items.len, runtime_role_setting_pos);
    try std.testing.expectEqual(ddl_plan.AlterRolePlan.Operation.set, runtime_role_setting.operation);
    try std.testing.expectEqual(ddl_plan.AlterRolePlan.SettingKind.runtime, runtime_role_setting.setting_kind);
    try std.testing.expectEqualStrings("statement_timeout", runtime_role_setting.setting_name);
    try expectAlterRoleLiteralSettingValue("1ms", runtime_role_setting.setting_value);

    var runtime_reset_role_tokens = try lexer.tokenizeAlloc(alloc, "ROLE app_writer RESET statement_timeout;");
    defer lexer.freeTokens(alloc, &runtime_reset_role_tokens);
    var runtime_reset_role_pos: usize = 0;
    var runtime_reset_role = try parseAlterRoleCatalogTailAlloc(alloc, runtime_reset_role_tokens.items, &runtime_reset_role_pos);
    defer runtime_reset_role.deinit(alloc);
    try std.testing.expectEqual(runtime_reset_role_tokens.items.len, runtime_reset_role_pos);
    try std.testing.expectEqual(ddl_plan.AlterRolePlan.Operation.reset, runtime_reset_role.operation);
    try std.testing.expectEqual(ddl_plan.AlterRolePlan.SettingKind.runtime, runtime_reset_role.setting_kind);
    try std.testing.expectEqualStrings("statement_timeout", runtime_reset_role.setting_name);
    try std.testing.expect(runtime_reset_role.setting_value == null);

    var current_role_value_tokens = try lexer.tokenizeAlloc(alloc, "ROLE app_writer SET app.tenant_id = current_setting('app.tenant_id');");
    defer lexer.freeTokens(alloc, &current_role_value_tokens);
    var current_role_value_pos: usize = 0;
    var current_role_value = try parseAlterRoleCatalogTailAlloc(alloc, current_role_value_tokens.items, &current_role_value_pos);
    defer current_role_value.deinit(alloc);
    try std.testing.expectEqual(current_role_value_tokens.items.len, current_role_value_pos);
    switch (current_role_value.setting_value orelse return error.TestUnexpectedResult) {
        .current_setting => |name| try std.testing.expectEqualStrings("app.tenant_id", name),
        .literal => return error.TestUnexpectedResult,
    }

    var drop_role_tokens = try lexer.tokenizeAlloc(alloc, "ROLE IF EXISTS app_writer;");
    defer lexer.freeTokens(alloc, &drop_role_tokens);
    var drop_role_pos: usize = 0;
    var drop_role = try parseDropRoleCatalogTailAlloc(alloc, drop_role_tokens.items, &drop_role_pos);
    defer drop_role.deinit(alloc);
    try std.testing.expectEqual(drop_role_tokens.items.len, drop_role_pos);
    try std.testing.expectEqualStrings("app_writer", drop_role.role_name);
    try std.testing.expect(drop_role.if_exists);

    var drop_user_tokens = try lexer.tokenizeAlloc(alloc, "USER IF EXISTS app_writer;");
    defer lexer.freeTokens(alloc, &drop_user_tokens);
    var drop_user_pos: usize = 0;
    var drop_user = try parseDropRoleCatalogTailAlloc(alloc, drop_user_tokens.items, &drop_user_pos);
    defer drop_user.deinit(alloc);
    try std.testing.expectEqual(drop_user_tokens.items.len, drop_user_pos);
    try std.testing.expectEqualStrings("app_writer", drop_user.role_name);
    try std.testing.expect(drop_user.if_exists);

    var drop_group_tokens = try lexer.tokenizeAlloc(alloc, "GROUP IF EXISTS app_readers;");
    defer lexer.freeTokens(alloc, &drop_group_tokens);
    var drop_group_pos: usize = 0;
    var drop_group = try parseDropRoleCatalogTailAlloc(alloc, drop_group_tokens.items, &drop_group_pos);
    defer drop_group.deinit(alloc);
    try std.testing.expectEqual(drop_group_tokens.items.len, drop_group_pos);
    try std.testing.expectEqualStrings("app_readers", drop_group.role_name);
    try std.testing.expect(drop_group.if_exists);

    var grant_tokens = try lexer.tokenizeAlloc(alloc, "SELECT, INSERT ON TABLE usage_records TO app_writer;");
    defer lexer.freeTokens(alloc, &grant_tokens);
    var grant_pos: usize = 0;
    var grant = try parsePrivilegeChangeTailAlloc(alloc, grant_tokens.items, &grant_pos, .grant);
    defer grant.deinit(alloc);
    try std.testing.expectEqual(grant_tokens.items.len, grant_pos);
    try std.testing.expectEqual(@as(usize, 2), grant.privileges.len);
    try std.testing.expectEqualStrings("SELECT", grant.privileges[0]);
    try std.testing.expectEqualStrings("INSERT", grant.privileges[1]);
    try std.testing.expectEqualStrings("TABLE", grant.object_kind);
    try std.testing.expectEqualStrings("usage_records", grant.object_name);
    try std.testing.expectEqualStrings("app_writer", grant.principal_name);

    var grant_all_tokens = try lexer.tokenizeAlloc(alloc, "ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO app_writer;");
    defer lexer.freeTokens(alloc, &grant_all_tokens);
    var grant_all_pos: usize = 0;
    var grant_all = try parsePrivilegeChangeTailAlloc(alloc, grant_all_tokens.items, &grant_all_pos, .grant);
    defer grant_all.deinit(alloc);
    try std.testing.expectEqual(grant_all_tokens.items.len, grant_all_pos);
    try std.testing.expectEqual(@as(usize, 1), grant_all.privileges.len);
    try std.testing.expectEqualStrings("ALL", grant_all.privileges[0]);
    try std.testing.expectEqualStrings("ALL_TABLES_IN_SCHEMA", grant_all.object_kind);
    try std.testing.expectEqualStrings("public", grant_all.object_name);

    var revoke_tokens = try lexer.tokenizeAlloc(alloc, "INSERT ON TABLE public.usage_records FROM app_writer;");
    defer lexer.freeTokens(alloc, &revoke_tokens);
    var revoke_pos: usize = 0;
    var revoke = try parsePrivilegeChangeTailAlloc(alloc, revoke_tokens.items, &revoke_pos, .revoke);
    defer revoke.deinit(alloc);
    try std.testing.expectEqual(revoke_tokens.items.len, revoke_pos);
    try std.testing.expectEqualStrings("usage_records", revoke.object_name);
    try std.testing.expectEqualStrings("app_writer", revoke.principal_name);
}

test "sql adapter grammar parses logical replication catalog tails" {
    const alloc = std.testing.allocator;

    var create_publication_tokens = try lexer.tokenizeAlloc(alloc, "PUBLICATION pub_usage FOR TABLE public.usage_records, audit_records;");
    defer lexer.freeTokens(alloc, &create_publication_tokens);
    var create_publication_pos: usize = 0;
    var create_publication = try parseCreatePublicationCatalogTailAlloc(alloc, create_publication_tokens.items, &create_publication_pos);
    defer create_publication.deinit(alloc);
    try std.testing.expectEqual(create_publication_tokens.items.len, create_publication_pos);
    try std.testing.expectEqualStrings("pub_usage", create_publication.publication_name);
    try std.testing.expect(!create_publication.all_tables);
    try std.testing.expectEqual(@as(usize, 2), create_publication.table_names.len);
    try std.testing.expectEqualStrings("usage_records", create_publication.table_names[0]);
    try std.testing.expectEqualStrings("audit_records", create_publication.table_names[1]);

    var create_all_tokens = try lexer.tokenizeAlloc(alloc, "PUBLICATION pub_all FOR ALL TABLES;");
    defer lexer.freeTokens(alloc, &create_all_tokens);
    var create_all_pos: usize = 0;
    var create_all = try parseCreatePublicationCatalogTailAlloc(alloc, create_all_tokens.items, &create_all_pos);
    defer create_all.deinit(alloc);
    try std.testing.expectEqual(create_all_tokens.items.len, create_all_pos);
    try std.testing.expect(create_all.all_tables);
    try std.testing.expectEqual(@as(usize, 0), create_all.table_names.len);

    var alter_publication_tokens = try lexer.tokenizeAlloc(alloc, "PUBLICATION pub_usage ADD TABLE public.usage_records;");
    defer lexer.freeTokens(alloc, &alter_publication_tokens);
    var alter_publication_pos: usize = 0;
    var alter_publication = try parseAlterPublicationCatalogTailAlloc(alloc, alter_publication_tokens.items, &alter_publication_pos);
    defer alter_publication.deinit(alloc);
    try std.testing.expectEqual(alter_publication_tokens.items.len, alter_publication_pos);
    try std.testing.expectEqualStrings("pub_usage", alter_publication.publication_name);
    try std.testing.expectEqualStrings("usage_records", alter_publication.table_names[0]);

    var drop_publication_tokens = try lexer.tokenizeAlloc(alloc, "PUBLICATION IF EXISTS pub_usage;");
    defer lexer.freeTokens(alloc, &drop_publication_tokens);
    var drop_publication_pos: usize = 0;
    var drop_publication = try parseDropPublicationCatalogTailAlloc(alloc, drop_publication_tokens.items, &drop_publication_pos);
    defer drop_publication.deinit(alloc);
    try std.testing.expectEqual(drop_publication_tokens.items.len, drop_publication_pos);
    try std.testing.expectEqualStrings("pub_usage", drop_publication.publication_name);
    try std.testing.expect(drop_publication.if_exists);

    var create_subscription_tokens = try lexer.tokenizeAlloc(alloc, "SUBSCRIPTION sub_usage CONNECTION 'host=db' PUBLICATION pub_usage, pub_audit;");
    defer lexer.freeTokens(alloc, &create_subscription_tokens);
    var create_subscription_pos: usize = 0;
    var create_subscription = try parseCreateSubscriptionCatalogTailAlloc(alloc, create_subscription_tokens.items, &create_subscription_pos);
    defer create_subscription.deinit(alloc);
    try std.testing.expectEqual(create_subscription_tokens.items.len, create_subscription_pos);
    try std.testing.expectEqualStrings("sub_usage", create_subscription.subscription_name);
    try std.testing.expectEqualStrings("\"host=db\"", create_subscription.connection_json);
    try std.testing.expectEqual(@as(usize, 2), create_subscription.publication_names.len);
    try std.testing.expectEqualStrings("pub_usage", create_subscription.publication_names[0]);
    try std.testing.expectEqualStrings("pub_audit", create_subscription.publication_names[1]);

    var alter_subscription_tokens = try lexer.tokenizeAlloc(alloc, "SUBSCRIPTION sub_usage DISABLE;");
    defer lexer.freeTokens(alloc, &alter_subscription_tokens);
    var alter_subscription_pos: usize = 0;
    var alter_subscription = try parseAlterSubscriptionCatalogTailAlloc(alloc, alter_subscription_tokens.items, &alter_subscription_pos);
    defer alter_subscription.deinit(alloc);
    try std.testing.expectEqual(alter_subscription_tokens.items.len, alter_subscription_pos);
    try std.testing.expectEqualStrings("sub_usage", alter_subscription.subscription_name);
    try std.testing.expect(!alter_subscription.enabled);

    var drop_subscription_tokens = try lexer.tokenizeAlloc(alloc, "SUBSCRIPTION IF EXISTS sub_usage;");
    defer lexer.freeTokens(alloc, &drop_subscription_tokens);
    var drop_subscription_pos: usize = 0;
    var drop_subscription = try parseDropSubscriptionCatalogTailAlloc(alloc, drop_subscription_tokens.items, &drop_subscription_pos);
    defer drop_subscription.deinit(alloc);
    try std.testing.expectEqual(drop_subscription_tokens.items.len, drop_subscription_pos);
    try std.testing.expectEqualStrings("sub_usage", drop_subscription.subscription_name);
    try std.testing.expect(drop_subscription.if_exists);

    var unsupported_tokens = try lexer.tokenizeAlloc(alloc, "PUBLICATION pub_usage FOR ALL TABLES WITH (publish = 'insert');");
    defer lexer.freeTokens(alloc, &unsupported_tokens);
    var unsupported_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseCreatePublicationCatalogTailAlloc(alloc, unsupported_tokens.items, &unsupported_pos));
}

test "sql adapter grammar parses type system catalog tails" {
    const alloc = std.testing.allocator;

    var create_collation_tokens = try lexer.tokenizeAlloc(alloc, "COLLATION en_us (locale = 'en_US', deterministic = true);");
    defer lexer.freeTokens(alloc, &create_collation_tokens);
    var create_collation_pos: usize = 0;
    var create_collation = try parseCreateCollationCatalogTailAlloc(alloc, create_collation_tokens.items, &create_collation_pos);
    defer create_collation.deinit(alloc);
    try std.testing.expectEqual(create_collation_tokens.items.len, create_collation_pos);
    try std.testing.expectEqualStrings("en_us", create_collation.collation_name);
    try std.testing.expectEqual(@as(usize, 2), create_collation.option_count);

    var rename_collation_tokens = try lexer.tokenizeAlloc(alloc, "COLLATION en_us RENAME TO en_us_v2;");
    defer lexer.freeTokens(alloc, &rename_collation_tokens);
    var rename_collation_pos: usize = 0;
    var rename_collation = try parseRenameCollationCatalogTailAlloc(alloc, rename_collation_tokens.items, &rename_collation_pos);
    defer rename_collation.deinit(alloc);
    try std.testing.expectEqual(rename_collation_tokens.items.len, rename_collation_pos);
    try std.testing.expectEqualStrings("en_us", rename_collation.collation_name);
    try std.testing.expectEqualStrings("en_us_v2", rename_collation.new_collation_name);

    var drop_collation_tokens = try lexer.tokenizeAlloc(alloc, "COLLATION IF EXISTS en_us_v2;");
    defer lexer.freeTokens(alloc, &drop_collation_tokens);
    var drop_collation_pos: usize = 0;
    var drop_collation = try parseDropCollationCatalogTailAlloc(alloc, drop_collation_tokens.items, &drop_collation_pos);
    defer drop_collation.deinit(alloc);
    try std.testing.expectEqual(drop_collation_tokens.items.len, drop_collation_pos);
    try std.testing.expectEqualStrings("en_us_v2", drop_collation.collation_name);
    try std.testing.expect(drop_collation.if_exists);

    var create_operator_tokens = try lexer.tokenizeAlloc(alloc, "OPERATOR + (leftarg = int, rightarg = int, procedure = int4pl);");
    defer lexer.freeTokens(alloc, &create_operator_tokens);
    var create_operator_pos: usize = 0;
    var create_operator = try parseCreateOperatorCatalogTailAlloc(alloc, create_operator_tokens.items, &create_operator_pos);
    defer create_operator.deinit(alloc);
    try std.testing.expectEqual(create_operator_tokens.items.len, create_operator_pos);
    try std.testing.expectEqualStrings("+", create_operator.operator_name);
    try std.testing.expectEqual(@as(usize, 3), create_operator.option_count);

    var drop_operator_tokens = try lexer.tokenizeAlloc(alloc, "OPERATOR + (int, int);");
    defer lexer.freeTokens(alloc, &drop_operator_tokens);
    var drop_operator_pos: usize = 0;
    var drop_operator = try parseDropOperatorCatalogTailAlloc(alloc, drop_operator_tokens.items, &drop_operator_pos);
    defer drop_operator.deinit(alloc);
    try std.testing.expectEqual(drop_operator_tokens.items.len, drop_operator_pos);
    try std.testing.expectEqualStrings("+", drop_operator.operator_name);
    try std.testing.expectEqual(@as(usize, 2), drop_operator.argument_count);

    var create_aggregate_tokens = try lexer.tokenizeAlloc(alloc, "AGGREGATE sum2 (int) (sfunc = int4pl, stype = int);");
    defer lexer.freeTokens(alloc, &create_aggregate_tokens);
    var create_aggregate_pos: usize = 0;
    var create_aggregate = try parseCreateAggregateCatalogTailAlloc(alloc, create_aggregate_tokens.items, &create_aggregate_pos);
    defer create_aggregate.deinit(alloc);
    try std.testing.expectEqual(create_aggregate_tokens.items.len, create_aggregate_pos);
    try std.testing.expectEqualStrings("sum2", create_aggregate.aggregate_name);
    try std.testing.expectEqual(@as(usize, 1), create_aggregate.argument_count);
    try std.testing.expectEqual(@as(usize, 2), create_aggregate.option_count);

    var drop_aggregate_tokens = try lexer.tokenizeAlloc(alloc, "AGGREGATE sum2 (int);");
    defer lexer.freeTokens(alloc, &drop_aggregate_tokens);
    var drop_aggregate_pos: usize = 0;
    var drop_aggregate = try parseDropAggregateCatalogTailAlloc(alloc, drop_aggregate_tokens.items, &drop_aggregate_pos);
    defer drop_aggregate.deinit(alloc);
    try std.testing.expectEqual(drop_aggregate_tokens.items.len, drop_aggregate_pos);
    try std.testing.expectEqualStrings("sum2", drop_aggregate.aggregate_name);
    try std.testing.expectEqual(@as(usize, 1), drop_aggregate.argument_count);

    var create_cast_tokens = try lexer.tokenizeAlloc(alloc, "CAST (text AS int) WITH FUNCTION text_to_int(text) AS ASSIGNMENT;");
    defer lexer.freeTokens(alloc, &create_cast_tokens);
    var create_cast_pos: usize = 0;
    var create_cast = try parseCreateCastCatalogTailAlloc(alloc, create_cast_tokens.items, &create_cast_pos);
    defer create_cast.deinit(alloc);
    try std.testing.expectEqual(create_cast_tokens.items.len, create_cast_pos);
    try std.testing.expectEqualStrings("text", create_cast.source_type);
    try std.testing.expectEqualStrings("int", create_cast.target_type);
    try std.testing.expectEqualStrings("text_to_int", create_cast.function_name);
    try std.testing.expect(create_cast.assignment);

    var drop_cast_tokens = try lexer.tokenizeAlloc(alloc, "CAST (text AS int);");
    defer lexer.freeTokens(alloc, &drop_cast_tokens);
    var drop_cast_pos: usize = 0;
    var drop_cast = try parseDropCastCatalogTailAlloc(alloc, drop_cast_tokens.items, &drop_cast_pos);
    defer drop_cast.deinit(alloc);
    try std.testing.expectEqual(drop_cast_tokens.items.len, drop_cast_pos);
    try std.testing.expectEqualStrings("text", drop_cast.source_type);
    try std.testing.expectEqualStrings("int", drop_cast.target_type);
}

test "sql adapter grammar parses relation lifetime prefixes" {
    const alloc = std.testing.allocator;

    var temporary_tokens = try lexer.tokenizeAlloc(alloc, "TEMPORARY TABLE usage_session_records (id uuid);");
    defer lexer.freeTokens(alloc, &temporary_tokens);
    var temporary_pos: usize = 0;
    const temporary = try parseRelationLifetimePrefix(temporary_tokens.items, &temporary_pos);
    try std.testing.expectEqual(RelationLifetimeKind.temporary, temporary.kind);
    try std.testing.expect(std.ascii.eqlIgnoreCase(temporary_tokens.items[temporary_pos].text, "table"));

    var temp_tokens = try lexer.tokenizeAlloc(alloc, "TEMP TABLE usage_session_records (id uuid);");
    defer lexer.freeTokens(alloc, &temp_tokens);
    var temp_pos: usize = 0;
    const temp = try parseRelationLifetimePrefix(temp_tokens.items, &temp_pos);
    try std.testing.expectEqual(RelationLifetimeKind.temporary, temp.kind);
    try std.testing.expect(std.ascii.eqlIgnoreCase(temp_tokens.items[temp_pos].text, "table"));

    var unlogged_tokens = try lexer.tokenizeAlloc(alloc, "UNLOGGED TABLE usage_ingest_records (id uuid);");
    defer lexer.freeTokens(alloc, &unlogged_tokens);
    var unlogged_pos: usize = 0;
    const unlogged = try parseRelationLifetimePrefix(unlogged_tokens.items, &unlogged_pos);
    try std.testing.expectEqual(RelationLifetimeKind.unlogged, unlogged.kind);
    try std.testing.expect(std.ascii.eqlIgnoreCase(unlogged_tokens.items[unlogged_pos].text, "table"));

    var durable_tokens = try lexer.tokenizeAlloc(alloc, "TABLE usage_records (id uuid);");
    defer lexer.freeTokens(alloc, &durable_tokens);
    var durable_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseRelationLifetimePrefix(durable_tokens.items, &durable_pos));
}

test "sql adapter grammar parses select set operation tokens" {
    const alloc = std.testing.allocator;

    var union_all_tokens = try lexer.tokenizeAlloc(alloc, "UNION ALL SELECT id FROM usage_archive");
    defer lexer.freeTokens(alloc, &union_all_tokens);
    var union_all_pos: usize = 0;
    try std.testing.expectEqual(ast.SelectSetOperation.union_all, try parseSelectSetOperation(union_all_tokens.items, &union_all_pos));
    try std.testing.expect(std.ascii.eqlIgnoreCase(union_all_tokens.items[union_all_pos].text, "select"));

    var union_distinct_tokens = try lexer.tokenizeAlloc(alloc, "UNION DISTINCT SELECT id FROM usage_archive");
    defer lexer.freeTokens(alloc, &union_distinct_tokens);
    var union_distinct_pos: usize = 0;
    try std.testing.expectEqual(ast.SelectSetOperation.union_distinct, try parseSelectSetOperation(union_distinct_tokens.items, &union_distinct_pos));
    try std.testing.expect(std.ascii.eqlIgnoreCase(union_distinct_tokens.items[union_distinct_pos].text, "select"));

    var intersect_tokens = try lexer.tokenizeAlloc(alloc, "INTERSECT SELECT id FROM usage_archive");
    defer lexer.freeTokens(alloc, &intersect_tokens);
    var intersect_pos: usize = 0;
    try std.testing.expectEqual(ast.SelectSetOperation.intersect, try parseSelectSetOperation(intersect_tokens.items, &intersect_pos));

    var except_tokens = try lexer.tokenizeAlloc(alloc, "EXCEPT SELECT id FROM usage_archive");
    defer lexer.freeTokens(alloc, &except_tokens);
    var except_pos: usize = 0;
    try std.testing.expectEqual(ast.SelectSetOperation.except, try parseSelectSetOperation(except_tokens.items, &except_pos));

    try std.testing.expect(nextIsSelectSetOperationKeyword(union_all_tokens.items, 0));
    try std.testing.expect(!nextIsSelectSetOperationKeyword(union_all_tokens.items, union_all_pos));

    var tail_tokens = try lexer.tokenizeAlloc(alloc, "ORDER BY id LIMIT 10 OFFSET 1 FETCH NEXT ROW ONLY ;");
    defer lexer.freeTokens(alloc, &tail_tokens);
    try std.testing.expect(nextIsSelectSetResultTailKeyword(tail_tokens.items, 0));
    try std.testing.expect(nextIsSelectSetResultTailKeyword(tail_tokens.items, 3));
    try std.testing.expect(nextIsSelectSetResultTailKeyword(tail_tokens.items, 5));
    try std.testing.expect(nextIsSelectSetResultTailKeyword(tail_tokens.items, 7));
    try std.testing.expect(nextIsSelectSetResultTailKeyword(tail_tokens.items, tail_tokens.items.len - 1));

    var invalid_tokens = try lexer.tokenizeAlloc(alloc, "SELECT id FROM usage_records");
    defer lexer.freeTokens(alloc, &invalid_tokens);
    var invalid_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseSelectSetOperation(invalid_tokens.items, &invalid_pos));
}

test "sql adapter grammar parses bulk io tails" {
    const alloc = std.testing.allocator;

    var copy_from_tokens = try lexer.tokenizeAlloc(alloc, "usage_records (id, status) FROM STDIN WITH (FORMAT csv);");
    defer lexer.freeTokens(alloc, &copy_from_tokens);
    var copy_from_pos: usize = 0;
    var copy_from = try parseBulkIoTailAlloc(alloc, copy_from_tokens.items, &copy_from_pos);
    defer copy_from.deinit(alloc);
    try std.testing.expectEqual(copy_from_tokens.items.len, copy_from_pos);
    try std.testing.expectEqual(BulkIoDirectionSyntax.from, copy_from.direction);
    try std.testing.expectEqualStrings("usage_records", copy_from.table_name);
    try std.testing.expectEqual(@as(usize, 2), copy_from.columns.len);
    try std.testing.expectEqualStrings("id", copy_from.columns[0]);
    try std.testing.expectEqualStrings("status", copy_from.columns[1]);
    try std.testing.expectEqualStrings("STDIN", copy_from.endpoint);
    try std.testing.expectEqual(ddl_plan.BulkIoEndpointKind.stream, copy_from.endpoint_kind);
    try std.testing.expectEqualStrings("csv", copy_from.format.?);
    try std.testing.expect(!copy_from.header);

    var copy_header_tokens = try lexer.tokenizeAlloc(alloc, "usage_records (id, status) FROM STDIN WITH (HEADER true, FREEZE true, ON_ERROR ignore, REJECT_LIMIT 10, LOG_VERBOSITY verbose, FORMAT csv, FORCE_NOT_NULL (id, status), FORCE_NULL (status), DELIMITER ',', QUOTE '\"', ESCAPE '!', NULL '', DEFAULT 'n/a', ENCODING 'UTF8');");
    defer lexer.freeTokens(alloc, &copy_header_tokens);
    var copy_header_pos: usize = 0;
    var copy_header = try parseBulkIoTailAlloc(alloc, copy_header_tokens.items, &copy_header_pos);
    defer copy_header.deinit(alloc);
    try std.testing.expectEqual(copy_header_tokens.items.len, copy_header_pos);
    try std.testing.expectEqual(BulkIoDirectionSyntax.from, copy_header.direction);
    try std.testing.expectEqualStrings("csv", copy_header.format.?);
    try std.testing.expect(copy_header.header);
    try std.testing.expect(copy_header.freeze);
    try std.testing.expectEqual(ddl_plan.BulkIoOnErrorPolicy.ignore, copy_header.on_error);
    try std.testing.expectEqual(@as(?usize, 10), copy_header.reject_limit);
    try std.testing.expectEqual(ddl_plan.BulkIoLogVerbosity.verbose, copy_header.log_verbosity);
    try std.testing.expectEqual(@as(usize, 2), copy_header.force_not_null_columns.len);
    try std.testing.expectEqualStrings("id", copy_header.force_not_null_columns[0]);
    try std.testing.expectEqualStrings("status", copy_header.force_not_null_columns[1]);
    try std.testing.expectEqual(@as(usize, 1), copy_header.force_null_columns.len);
    try std.testing.expectEqualStrings("status", copy_header.force_null_columns[0]);
    try std.testing.expectEqualStrings(",", copy_header.delimiter.?);
    try std.testing.expectEqualStrings("\"", copy_header.quote.?);
    try std.testing.expectEqualStrings("!", copy_header.escape.?);
    try std.testing.expectEqualStrings("", copy_header.null_marker.?);
    try std.testing.expectEqualStrings("n/a", copy_header.default_marker.?);
    try std.testing.expectEqualStrings("UTF8", copy_header.encoding.?);

    var copy_oids_false_tokens = try lexer.tokenizeAlloc(alloc, "usage_records (id, status) FROM STDIN WITH (OIDS false, FORMAT csv);");
    defer lexer.freeTokens(alloc, &copy_oids_false_tokens);
    var copy_oids_false_pos: usize = 0;
    var copy_oids_false = try parseBulkIoTailAlloc(alloc, copy_oids_false_tokens.items, &copy_oids_false_pos);
    defer copy_oids_false.deinit(alloc);
    try std.testing.expectEqual(copy_oids_false_tokens.items.len, copy_oids_false_pos);
    try std.testing.expectEqual(BulkIoDirectionSyntax.from, copy_oids_false.direction);
    try std.testing.expectEqualStrings("csv", copy_oids_false.format.?);

    var copy_oids_true_tokens = try lexer.tokenizeAlloc(alloc, "usage_records (id, status) FROM STDIN WITH (OIDS true);");
    defer lexer.freeTokens(alloc, &copy_oids_true_tokens);
    var copy_oids_true_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseBulkIoTailAlloc(alloc, copy_oids_true_tokens.items, &copy_oids_true_pos));

    var copy_to_tokens = try lexer.tokenizeAlloc(alloc, "public.usage_records TO STDOUT WITH (FORCE_QUOTE *);");
    defer lexer.freeTokens(alloc, &copy_to_tokens);
    var copy_to_pos: usize = 0;
    var copy_to = try parseBulkIoTailAlloc(alloc, copy_to_tokens.items, &copy_to_pos);
    defer copy_to.deinit(alloc);
    try std.testing.expectEqual(copy_to_tokens.items.len, copy_to_pos);
    try std.testing.expectEqual(BulkIoDirectionSyntax.to, copy_to.direction);
    try std.testing.expectEqualStrings("usage_records", copy_to.table_name);
    try std.testing.expectEqual(@as(usize, 0), copy_to.columns.len);
    try std.testing.expectEqualStrings("STDOUT", copy_to.endpoint);
    try std.testing.expectEqual(ddl_plan.BulkIoEndpointKind.stream, copy_to.endpoint_kind);
    try std.testing.expect(copy_to.format == null);
    try std.testing.expect(!copy_to.header);
    try std.testing.expect(copy_to.force_quote_all);

    var copy_from_file_tokens = try lexer.tokenizeAlloc(alloc, "usage_records (id, status) FROM '/tmp/usage.csv' WITH (FORMAT csv, HEADER true);");
    defer lexer.freeTokens(alloc, &copy_from_file_tokens);
    var copy_from_file_pos: usize = 0;
    var copy_from_file = try parseBulkIoTailAlloc(alloc, copy_from_file_tokens.items, &copy_from_file_pos);
    defer copy_from_file.deinit(alloc);
    try std.testing.expectEqual(copy_from_file_tokens.items.len, copy_from_file_pos);
    try std.testing.expectEqual(BulkIoDirectionSyntax.from, copy_from_file.direction);
    try std.testing.expectEqual(ddl_plan.BulkIoEndpointKind.file, copy_from_file.endpoint_kind);
    try std.testing.expectEqualStrings("/tmp/usage.csv", copy_from_file.endpoint);
    try std.testing.expectEqualStrings("csv", copy_from_file.format.?);
    try std.testing.expect(copy_from_file.header);

    var copy_to_file_tokens = try lexer.tokenizeAlloc(alloc, "usage_records (id, status) TO '/tmp/usage.csv' WITH (FORMAT csv);");
    defer lexer.freeTokens(alloc, &copy_to_file_tokens);
    var copy_to_file_pos: usize = 0;
    var copy_to_file = try parseBulkIoTailAlloc(alloc, copy_to_file_tokens.items, &copy_to_file_pos);
    defer copy_to_file.deinit(alloc);
    try std.testing.expectEqual(copy_to_file_tokens.items.len, copy_to_file_pos);
    try std.testing.expectEqual(BulkIoDirectionSyntax.to, copy_to_file.direction);
    try std.testing.expectEqual(ddl_plan.BulkIoEndpointKind.file, copy_to_file.endpoint_kind);
    try std.testing.expectEqualStrings("/tmp/usage.csv", copy_to_file.endpoint);

    var copy_program_tokens = try lexer.tokenizeAlloc(alloc, "usage_records FROM PROGRAM 'cat /tmp/usage.csv';");
    defer lexer.freeTokens(alloc, &copy_program_tokens);
    var copy_program_pos: usize = 0;
    var copy_program = try parseBulkIoTailAlloc(alloc, copy_program_tokens.items, &copy_program_pos);
    defer copy_program.deinit(alloc);
    try std.testing.expectEqual(copy_program_tokens.items.len, copy_program_pos);
    try std.testing.expectEqual(BulkIoDirectionSyntax.from, copy_program.direction);
    try std.testing.expectEqual(ddl_plan.BulkIoEndpointKind.program, copy_program.endpoint_kind);
    try std.testing.expectEqualStrings("cat /tmp/usage.csv", copy_program.endpoint);

    var force_quote_columns_tokens = try lexer.tokenizeAlloc(alloc, "usage_records TO STDOUT WITH (FORCE_QUOTE (id, status));");
    defer lexer.freeTokens(alloc, &force_quote_columns_tokens);
    var force_quote_columns_pos: usize = 0;
    var force_quote_columns = try parseBulkIoTailAlloc(alloc, force_quote_columns_tokens.items, &force_quote_columns_pos);
    defer force_quote_columns.deinit(alloc);
    try std.testing.expect(!force_quote_columns.force_quote_all);
    try std.testing.expectEqual(@as(usize, 2), force_quote_columns.force_quote_columns.len);
    try std.testing.expectEqualStrings("id", force_quote_columns.force_quote_columns[0]);
    try std.testing.expectEqualStrings("status", force_quote_columns.force_quote_columns[1]);

    var force_not_null_to_tokens = try lexer.tokenizeAlloc(alloc, "usage_records TO STDOUT WITH (FORCE_NOT_NULL (id));");
    defer lexer.freeTokens(alloc, &force_not_null_to_tokens);
    var force_not_null_to_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseBulkIoTailAlloc(alloc, force_not_null_to_tokens.items, &force_not_null_to_pos));

    var force_null_to_tokens = try lexer.tokenizeAlloc(alloc, "usage_records TO STDOUT WITH (FORCE_NULL (id));");
    defer lexer.freeTokens(alloc, &force_null_to_tokens);
    var force_null_to_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseBulkIoTailAlloc(alloc, force_null_to_tokens.items, &force_null_to_pos));

    var on_error_to_tokens = try lexer.tokenizeAlloc(alloc, "usage_records TO STDOUT WITH (ON_ERROR ignore);");
    defer lexer.freeTokens(alloc, &on_error_to_tokens);
    var on_error_to_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseBulkIoTailAlloc(alloc, on_error_to_tokens.items, &on_error_to_pos));

    var reject_limit_stop_tokens = try lexer.tokenizeAlloc(alloc, "usage_records FROM STDIN WITH (REJECT_LIMIT 10);");
    defer lexer.freeTokens(alloc, &reject_limit_stop_tokens);
    var reject_limit_stop_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseBulkIoTailAlloc(alloc, reject_limit_stop_tokens.items, &reject_limit_stop_pos));

    var reject_limit_to_tokens = try lexer.tokenizeAlloc(alloc, "usage_records TO STDOUT WITH (ON_ERROR ignore, REJECT_LIMIT 10);");
    defer lexer.freeTokens(alloc, &reject_limit_to_tokens);
    var reject_limit_to_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseBulkIoTailAlloc(alloc, reject_limit_to_tokens.items, &reject_limit_to_pos));

    var log_verbosity_stop_tokens = try lexer.tokenizeAlloc(alloc, "usage_records FROM STDIN WITH (LOG_VERBOSITY verbose);");
    defer lexer.freeTokens(alloc, &log_verbosity_stop_tokens);
    var log_verbosity_stop_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseBulkIoTailAlloc(alloc, log_verbosity_stop_tokens.items, &log_verbosity_stop_pos));

    var log_verbosity_to_tokens = try lexer.tokenizeAlloc(alloc, "usage_records TO STDOUT WITH (ON_ERROR ignore, LOG_VERBOSITY verbose);");
    defer lexer.freeTokens(alloc, &log_verbosity_to_tokens);
    var log_verbosity_to_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseBulkIoTailAlloc(alloc, log_verbosity_to_tokens.items, &log_verbosity_to_pos));

    var default_to_tokens = try lexer.tokenizeAlloc(alloc, "usage_records TO STDOUT WITH (DEFAULT 'n/a');");
    defer lexer.freeTokens(alloc, &default_to_tokens);
    var default_to_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseBulkIoTailAlloc(alloc, default_to_tokens.items, &default_to_pos));

    var where_tokens = try lexer.tokenizeAlloc(alloc, "usage_records FROM STDIN WITH (FORMAT csv) WHERE status = 'active';");
    defer lexer.freeTokens(alloc, &where_tokens);
    var where_pos: usize = 0;
    var where_copy = try parseBulkIoTailAlloc(alloc, where_tokens.items, &where_pos);
    defer where_copy.deinit(alloc);
    try std.testing.expect(where_pos < where_tokens.items.len);
    try std.testing.expectEqual(BulkIoDirectionSyntax.from, where_copy.direction);
    try std.testing.expectEqualStrings("usage_records", where_copy.table_name);
    try std.testing.expectEqualStrings("where", where_tokens.items[where_pos].text);

    var freeze_to_tokens = try lexer.tokenizeAlloc(alloc, "usage_records TO STDOUT WITH (FREEZE true);");
    defer lexer.freeTokens(alloc, &freeze_to_tokens);
    var freeze_to_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseBulkIoTailAlloc(alloc, freeze_to_tokens.items, &freeze_to_pos));

    var wrong_endpoint_tokens = try lexer.tokenizeAlloc(alloc, "usage_records TO STDIN;");
    defer lexer.freeTokens(alloc, &wrong_endpoint_tokens);
    var wrong_endpoint_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseBulkIoTailAlloc(alloc, wrong_endpoint_tokens.items, &wrong_endpoint_pos));
}

test "sql adapter grammar parses notification channel tails" {
    const alloc = std.testing.allocator;

    var listen_tokens = try lexer.tokenizeAlloc(alloc, "usage_events;");
    defer lexer.freeTokens(alloc, &listen_tokens);
    var listen_pos: usize = 0;
    var listen = try parseListenNotificationTailAlloc(alloc, listen_tokens.items, &listen_pos);
    defer listen.deinit(alloc);
    try std.testing.expectEqual(listen_tokens.items.len, listen_pos);
    try std.testing.expectEqualStrings("usage_events", listen.channel_name);

    var notify_tokens = try lexer.tokenizeAlloc(alloc, "usage_events, 'queued';");
    defer lexer.freeTokens(alloc, &notify_tokens);
    var notify_pos: usize = 0;
    var notify = try parseNotifyNotificationTailAlloc(alloc, notify_tokens.items, &notify_pos);
    defer notify.deinit(alloc);
    try std.testing.expectEqual(notify_tokens.items.len, notify_pos);
    try std.testing.expectEqualStrings("usage_events", notify.channel_name);
    try std.testing.expectEqualStrings("\"queued\"", notify.payload_json.?);

    var notify_no_payload_tokens = try lexer.tokenizeAlloc(alloc, "usage_events;");
    defer lexer.freeTokens(alloc, &notify_no_payload_tokens);
    var notify_no_payload_pos: usize = 0;
    var notify_no_payload = try parseNotifyNotificationTailAlloc(alloc, notify_no_payload_tokens.items, &notify_no_payload_pos);
    defer notify_no_payload.deinit(alloc);
    try std.testing.expectEqual(notify_no_payload_tokens.items.len, notify_no_payload_pos);
    try std.testing.expect(notify_no_payload.payload_json == null);

    var unlisten_tokens = try lexer.tokenizeAlloc(alloc, "*;");
    defer lexer.freeTokens(alloc, &unlisten_tokens);
    var unlisten_pos: usize = 0;
    var unlisten = try parseUnlistenNotificationTailAlloc(alloc, unlisten_tokens.items, &unlisten_pos);
    defer unlisten.deinit(alloc);
    try std.testing.expectEqual(unlisten_tokens.items.len, unlisten_pos);
    try std.testing.expect(unlisten.all);
    try std.testing.expect(unlisten.channel_name == null);

    var unsupported_tokens = try lexer.tokenizeAlloc(alloc, "usage_events trailing;");
    defer lexer.freeTokens(alloc, &unsupported_tokens);
    var unsupported_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseListenNotificationTailAlloc(alloc, unsupported_tokens.items, &unsupported_pos));
}

test "sql adapter grammar parses truncate mutation-source syntax" {
    const alloc = std.testing.allocator;

    var truncate_tokens = try lexer.tokenizeAlloc(alloc, "TRUNCATE TABLE ONLY public.usage_records RESTRICT;");
    defer lexer.freeTokens(alloc, &truncate_tokens);
    var truncate_pos: usize = 0;
    var truncate = try parseTruncateMutationSourceSqlAlloc(alloc, truncate_tokens.items, &truncate_pos);
    defer truncate.deinit(alloc);
    try std.testing.expectEqual(truncate_tokens.items.len, truncate_pos);
    try std.testing.expectEqualStrings("usage_records", truncate.table_name);
    try std.testing.expectEqual(@as(usize, 0), truncate.additional_table_names.len);
    try std.testing.expect(!truncate.restart_identity);
    try std.testing.expect(!truncate.cascade);

    var restart_tokens = try lexer.tokenizeAlloc(alloc, "TRUNCATE usage_records RESTART IDENTITY;");
    defer lexer.freeTokens(alloc, &restart_tokens);
    var restart_pos: usize = 0;
    var restart = try parseTruncateMutationSourceSqlAlloc(alloc, restart_tokens.items, &restart_pos);
    defer restart.deinit(alloc);
    try std.testing.expectEqual(restart_tokens.items.len, restart_pos);
    try std.testing.expectEqualStrings("usage_records", restart.table_name);
    try std.testing.expect(restart.restart_identity);
    try std.testing.expect(!restart.cascade);

    var multi_tokens = try lexer.tokenizeAlloc(alloc, "TRUNCATE usage_records, audit_records;");
    defer lexer.freeTokens(alloc, &multi_tokens);
    var multi_pos: usize = 0;
    var multi = try parseTruncateMutationSourceSqlAlloc(alloc, multi_tokens.items, &multi_pos);
    defer multi.deinit(alloc);
    try std.testing.expectEqual(multi_tokens.items.len, multi_pos);
    try std.testing.expectEqualStrings("usage_records", multi.table_name);
    try std.testing.expectEqual(@as(usize, 1), multi.additional_table_names.len);
    try std.testing.expectEqualStrings("audit_records", multi.additional_table_names[0]);

    var cascade_tokens = try lexer.tokenizeAlloc(alloc, "TRUNCATE usage_records CASCADE;");
    defer lexer.freeTokens(alloc, &cascade_tokens);
    var cascade_pos: usize = 0;
    var cascade = try parseTruncateMutationSourceSqlAlloc(alloc, cascade_tokens.items, &cascade_pos);
    defer cascade.deinit(alloc);
    try std.testing.expectEqual(cascade_tokens.items.len, cascade_pos);
    try std.testing.expectEqualStrings("usage_records", cascade.table_name);
    try std.testing.expect(cascade.cascade);

    var identity_tokens = try lexer.tokenizeAlloc(alloc, "TRUNCATE usage_records IDENTITY;");
    defer lexer.freeTokens(alloc, &identity_tokens);
    var identity_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseTruncateMutationSourceSqlAlloc(alloc, identity_tokens.items, &identity_pos));
}

test "sql adapter grammar parses savepoint transaction tails" {
    const alloc = std.testing.allocator;

    var savepoint_tokens = try lexer.tokenizeAlloc(alloc, "before_retry;");
    defer lexer.freeTokens(alloc, &savepoint_tokens);
    var savepoint_pos: usize = 0;
    const savepoint = try parseSavepointTransactionTail(savepoint_tokens.items, &savepoint_pos);
    try std.testing.expectEqualStrings("before_retry", savepoint.savepoint_name);
    try std.testing.expectEqual(savepoint_tokens.items.len, savepoint_pos);

    var release_tokens = try lexer.tokenizeAlloc(alloc, "SAVEPOINT before_retry;");
    defer lexer.freeTokens(alloc, &release_tokens);
    var release_pos: usize = 0;
    const release = try parseReleaseSavepointTail(release_tokens.items, &release_pos);
    try std.testing.expectEqualStrings("before_retry", release.savepoint_name);
    try std.testing.expectEqual(release_tokens.items.len, release_pos);

    var release_shorthand_tokens = try lexer.tokenizeAlloc(alloc, "before_retry;");
    defer lexer.freeTokens(alloc, &release_shorthand_tokens);
    var release_shorthand_pos: usize = 0;
    const release_shorthand = try parseReleaseSavepointTail(release_shorthand_tokens.items, &release_shorthand_pos);
    try std.testing.expectEqualStrings("before_retry", release_shorthand.savepoint_name);
    try std.testing.expectEqual(release_shorthand_tokens.items.len, release_shorthand_pos);

    var rollback_tokens = try lexer.tokenizeAlloc(alloc, "TO SAVEPOINT before_retry;");
    defer lexer.freeTokens(alloc, &rollback_tokens);
    var rollback_pos: usize = 0;
    const rollback = try parseRollbackToSavepointTail(rollback_tokens.items, &rollback_pos);
    try std.testing.expectEqualStrings("before_retry", rollback.savepoint_name);
    try std.testing.expectEqual(rollback_tokens.items.len, rollback_pos);

    var rollback_shorthand_tokens = try lexer.tokenizeAlloc(alloc, "TO before_retry;");
    defer lexer.freeTokens(alloc, &rollback_shorthand_tokens);
    var rollback_shorthand_pos: usize = 0;
    const rollback_shorthand = try parseRollbackToSavepointTail(rollback_shorthand_tokens.items, &rollback_shorthand_pos);
    try std.testing.expectEqualStrings("before_retry", rollback_shorthand.savepoint_name);
    try std.testing.expectEqual(rollback_shorthand_tokens.items.len, rollback_shorthand_pos);

    var extra_tokens = try lexer.tokenizeAlloc(alloc, "before_retry RELEASE;");
    defer lexer.freeTokens(alloc, &extra_tokens);
    var extra_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseSavepointTransactionTail(extra_tokens.items, &extra_pos));
}

test "sql adapter grammar parses protocol cleanup tails" {
    const alloc = std.testing.allocator;

    var deallocate_tokens = try lexer.tokenizeAlloc(alloc, "usage_plan;");
    defer lexer.freeTokens(alloc, &deallocate_tokens);
    var deallocate_pos: usize = 0;
    const deallocate = try parseDeallocatePreparedStatementTail(deallocate_tokens.items, &deallocate_pos);
    try std.testing.expectEqualStrings("usage_plan", deallocate.name.?);
    try std.testing.expect(!deallocate.all);
    try std.testing.expectEqual(deallocate_tokens.items.len, deallocate_pos);

    var deallocate_prepare_tokens = try lexer.tokenizeAlloc(alloc, "PREPARE usage_plan;");
    defer lexer.freeTokens(alloc, &deallocate_prepare_tokens);
    var deallocate_prepare_pos: usize = 0;
    const deallocate_prepare = try parseDeallocatePreparedStatementTail(deallocate_prepare_tokens.items, &deallocate_prepare_pos);
    try std.testing.expectEqualStrings("usage_plan", deallocate_prepare.name.?);
    try std.testing.expect(!deallocate_prepare.all);
    try std.testing.expectEqual(deallocate_prepare_tokens.items.len, deallocate_prepare_pos);

    var deallocate_all_tokens = try lexer.tokenizeAlloc(alloc, "ALL;");
    defer lexer.freeTokens(alloc, &deallocate_all_tokens);
    var deallocate_all_pos: usize = 0;
    const deallocate_all = try parseDeallocatePreparedStatementTail(deallocate_all_tokens.items, &deallocate_all_pos);
    try std.testing.expect(deallocate_all.all);
    try std.testing.expect(deallocate_all.name == null);
    try std.testing.expectEqual(deallocate_all_tokens.items.len, deallocate_all_pos);

    var close_tokens = try lexer.tokenizeAlloc(alloc, "usage_cursor;");
    defer lexer.freeTokens(alloc, &close_tokens);
    var close_pos: usize = 0;
    const close = try parseCloseCursorPortalTail(close_tokens.items, &close_pos);
    try std.testing.expectEqualStrings("usage_cursor", close.name.?);
    try std.testing.expect(!close.all);
    try std.testing.expectEqual(close_tokens.items.len, close_pos);

    var close_all_tokens = try lexer.tokenizeAlloc(alloc, "ALL;");
    defer lexer.freeTokens(alloc, &close_all_tokens);
    var close_all_pos: usize = 0;
    const close_all = try parseCloseCursorPortalTail(close_all_tokens.items, &close_all_pos);
    try std.testing.expect(close_all.all);
    try std.testing.expect(close_all.name == null);
    try std.testing.expectEqual(close_all_tokens.items.len, close_all_pos);

    var extra_tokens = try lexer.tokenizeAlloc(alloc, "usage_cursor; CLOSE ALL;");
    defer lexer.freeTokens(alloc, &extra_tokens);
    var extra_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseCloseCursorPortalTail(extra_tokens.items, &extra_pos));

    var prepare_transaction_tokens = try lexer.tokenizeAlloc(alloc, "TRANSACTION 'gid-1';");
    defer lexer.freeTokens(alloc, &prepare_transaction_tokens);
    var prepare_transaction_pos: usize = 0;
    var prepare_transaction = try parsePreparedTransactionTailAlloc(alloc, prepare_transaction_tokens.items, &prepare_transaction_pos, .prepare);
    defer prepare_transaction.deinit(alloc);
    try std.testing.expectEqual(ddl_plan.PreparedTransactionAction.prepare, prepare_transaction.action);
    try std.testing.expectEqualStrings("gid-1", prepare_transaction.gid);
    try std.testing.expectEqual(prepare_transaction_tokens.items.len, prepare_transaction_pos);

    var commit_prepared_tokens = try lexer.tokenizeAlloc(alloc, "PREPARED 'gid-1';");
    defer lexer.freeTokens(alloc, &commit_prepared_tokens);
    var commit_prepared_pos: usize = 0;
    var commit_prepared = try parsePreparedTransactionTailAlloc(alloc, commit_prepared_tokens.items, &commit_prepared_pos, .commit);
    defer commit_prepared.deinit(alloc);
    try std.testing.expectEqual(ddl_plan.PreparedTransactionAction.commit, commit_prepared.action);
    try std.testing.expectEqualStrings("gid-1", commit_prepared.gid);
    try std.testing.expectEqual(commit_prepared_tokens.items.len, commit_prepared_pos);

    var empty_gid_tokens = try lexer.tokenizeAlloc(alloc, "PREPARED '';");
    defer lexer.freeTokens(alloc, &empty_gid_tokens);
    var empty_gid_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parsePreparedTransactionTailAlloc(alloc, empty_gid_tokens.items, &empty_gid_pos, .commit));
}

test "sql adapter grammar parses prepared statement syntax" {
    const alloc = std.testing.allocator;

    var prepare_tokens = try lexer.tokenizeAlloc(alloc, "usage_plan(text, uuid) AS SELECT id FROM usage_records WHERE status = $1;");
    defer lexer.freeTokens(alloc, &prepare_tokens);
    var prepare_pos: usize = 0;
    const prepare = try parsePrepareStatementTailAlloc(alloc, prepare_tokens.items, &prepare_pos);
    try std.testing.expectEqualStrings("usage_plan", prepare.statement_name);
    try std.testing.expectEqual(@as(usize, 2), prepare.parameter_count);
    try std.testing.expectEqual(PreparedStatementSubjectSyntax.read, prepare.statement_kind);
    try std.testing.expectEqual(PreparedStatementStatementSyntax.read, prepare.statement_family);
    try std.testing.expectEqual(prepare_tokens.items.len, prepare_pos);

    var prepare_insert_tokens = try lexer.tokenizeAlloc(alloc, "insert_plan(text) AS INSERT INTO usage_records (id) VALUES ($1);");
    defer lexer.freeTokens(alloc, &prepare_insert_tokens);
    var prepare_insert_pos: usize = 0;
    const prepare_insert = try parsePrepareStatementTailAlloc(alloc, prepare_insert_tokens.items, &prepare_insert_pos);
    try std.testing.expectEqualStrings("insert_plan", prepare_insert.statement_name);
    try std.testing.expectEqual(@as(usize, 1), prepare_insert.parameter_count);
    try std.testing.expectEqual(PreparedStatementSubjectSyntax.write, prepare_insert.statement_kind);
    try std.testing.expectEqual(PreparedStatementStatementSyntax.insert, prepare_insert.statement_family);
    try std.testing.expectEqual(prepare_insert_tokens.items.len, prepare_insert_pos);

    var prepare_truncate_tokens = try lexer.tokenizeAlloc(alloc, "truncate_plan AS TRUNCATE usage_records;");
    defer lexer.freeTokens(alloc, &prepare_truncate_tokens);
    var prepare_truncate_pos: usize = 0;
    const prepare_truncate = try parsePrepareStatementTailAlloc(alloc, prepare_truncate_tokens.items, &prepare_truncate_pos);
    try std.testing.expectEqualStrings("truncate_plan", prepare_truncate.statement_name);
    try std.testing.expectEqual(@as(usize, 0), prepare_truncate.parameter_count);
    try std.testing.expectEqual(PreparedStatementSubjectSyntax.write, prepare_truncate.statement_kind);
    try std.testing.expectEqual(PreparedStatementStatementSyntax.truncate, prepare_truncate.statement_family);
    try std.testing.expectEqual(prepare_truncate_tokens.items.len, prepare_truncate_pos);

    var prepare_ddl_tokens = try lexer.tokenizeAlloc(alloc, "ddl_plan AS CREATE TABLE prepared_usage_records (id uuid);");
    defer lexer.freeTokens(alloc, &prepare_ddl_tokens);
    var prepare_ddl_pos: usize = 0;
    const prepare_ddl = try parsePrepareStatementTailAlloc(alloc, prepare_ddl_tokens.items, &prepare_ddl_pos);
    try std.testing.expectEqualStrings("ddl_plan", prepare_ddl.statement_name);
    try std.testing.expectEqual(@as(usize, 0), prepare_ddl.parameter_count);
    try std.testing.expectEqual(PreparedStatementSubjectSyntax.ddl, prepare_ddl.statement_kind);
    try std.testing.expectEqual(PreparedStatementStatementSyntax.ddl, prepare_ddl.statement_family);
    try std.testing.expectEqual(prepare_ddl_tokens.items.len, prepare_ddl_pos);

    var prepare_merge_tokens = try lexer.tokenizeAlloc(alloc, "merge_plan AS MERGE INTO usage_records USING source_records ON usage_records.id = source_records.id WHEN MATCHED THEN UPDATE SET status = source_records.status;");
    defer lexer.freeTokens(alloc, &prepare_merge_tokens);
    var prepare_merge_pos: usize = 0;
    const prepare_merge = try parsePrepareStatementTailAlloc(alloc, prepare_merge_tokens.items, &prepare_merge_pos);
    try std.testing.expectEqualStrings("merge_plan", prepare_merge.statement_name);
    try std.testing.expectEqual(@as(usize, 0), prepare_merge.parameter_count);
    try std.testing.expectEqual(PreparedStatementSubjectSyntax.write, prepare_merge.statement_kind);
    try std.testing.expectEqual(PreparedStatementStatementSyntax.merge, prepare_merge.statement_family);
    try std.testing.expectEqual(prepare_merge_tokens.items.len, prepare_merge_pos);

    var prepare_cte_write_tokens = try lexer.tokenizeAlloc(alloc, "usage_cte_plan AS WITH source_rows AS (SELECT id FROM usage_records) UPDATE usage_records SET status = 'done' WHERE id IN (SELECT id FROM source_rows);");
    defer lexer.freeTokens(alloc, &prepare_cte_write_tokens);
    var prepare_cte_write_pos: usize = 0;
    const prepare_cte_write = try parsePrepareStatementTailAlloc(alloc, prepare_cte_write_tokens.items, &prepare_cte_write_pos);
    try std.testing.expectEqualStrings("usage_cte_plan", prepare_cte_write.statement_name);
    try std.testing.expectEqual(@as(usize, 0), prepare_cte_write.parameter_count);
    try std.testing.expectEqual(PreparedStatementSubjectSyntax.write, prepare_cte_write.statement_kind);
    try std.testing.expectEqual(PreparedStatementStatementSyntax.update, prepare_cte_write.statement_family);
    try std.testing.expectEqual(prepare_cte_write_tokens.items.len, prepare_cte_write_pos);

    var execute_tokens = try lexer.tokenizeAlloc(alloc, "usage_plan('open', -3, true, null);");
    defer lexer.freeTokens(alloc, &execute_tokens);
    var execute_pos: usize = 0;
    const execute = try parseExecutePreparedStatementTail(execute_tokens.items, &execute_pos);
    try std.testing.expectEqualStrings("usage_plan", execute.statement_name);
    try std.testing.expectEqual(@as(usize, 4), execute.argument_count);
    try std.testing.expectEqual(execute_tokens.items.len, execute_pos);

    var execute_bare_tokens = try lexer.tokenizeAlloc(alloc, "usage_plan;");
    defer lexer.freeTokens(alloc, &execute_bare_tokens);
    var execute_bare_pos: usize = 0;
    const execute_bare = try parseExecutePreparedStatementTail(execute_bare_tokens.items, &execute_bare_pos);
    try std.testing.expectEqualStrings("usage_plan", execute_bare.statement_name);
    try std.testing.expectEqual(@as(usize, 0), execute_bare.argument_count);
    try std.testing.expectEqual(execute_bare_tokens.items.len, execute_bare_pos);

    var extra_tokens = try lexer.tokenizeAlloc(alloc, "usage_plan('open'); EXECUTE other_plan;");
    defer lexer.freeTokens(alloc, &extra_tokens);
    var extra_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseExecutePreparedStatementTail(extra_tokens.items, &extra_pos));
}

test "sql adapter grammar parses cursor portal syntax" {
    const alloc = std.testing.allocator;

    var declare_tokens = try lexer.tokenizeAlloc(alloc, "usage_cursor BINARY NO SCROLL CURSOR WITH HOLD FOR SELECT id FROM usage_records;");
    defer lexer.freeTokens(alloc, &declare_tokens);
    var declare_pos: usize = 0;
    const declare = try parseDeclareCursorPortalPrefix(declare_tokens.items, &declare_pos);
    try std.testing.expectEqualStrings("usage_cursor", declare.portal_name);
    try std.testing.expectEqual(CursorScrollSyntax.no_scroll, declare.scroll);
    try std.testing.expect(declare.binary);
    try std.testing.expect(declare.hold);
    try std.testing.expect(parser.peekKeyword(declare_tokens.items, declare_pos, "select"));

    var declare_tail_tokens = try lexer.tokenizeAlloc(alloc, "usage_cursor BINARY NO SCROLL CURSOR WITH HOLD FOR SELECT id FROM usage_records;");
    defer lexer.freeTokens(alloc, &declare_tail_tokens);
    var declare_tail_pos: usize = 0;
    const declare_tail = try parseDeclareCursorPortalTailAlloc(alloc, declare_tail_tokens.items, &declare_tail_pos);
    try std.testing.expectEqualStrings("usage_cursor", declare_tail.portal_name);
    try std.testing.expectEqual(CursorScrollSyntax.no_scroll, declare_tail.scroll);
    try std.testing.expectEqual(PreparedStatementSubjectSyntax.read, declare_tail.statement_kind.?);
    try std.testing.expectEqual(declare_tail_tokens.items.len, declare_tail_pos);

    var fetch_tokens = try lexer.tokenizeAlloc(alloc, "BACKWARD -5 FROM usage_cursor;");
    defer lexer.freeTokens(alloc, &fetch_tokens);
    var fetch_pos: usize = 0;
    const fetch = try parseFetchCursorPortalTail(fetch_tokens.items, &fetch_pos);
    try std.testing.expectEqualStrings("usage_cursor", fetch.portal_name);
    try std.testing.expectEqual(CursorFetchDirectionSyntax.backward, fetch.direction);
    try std.testing.expectEqual(@as(?i64, -5), fetch.count);
    try std.testing.expectEqual(fetch_tokens.items.len, fetch_pos);

    var fetch_all_tokens = try lexer.tokenizeAlloc(alloc, "FORWARD ALL IN usage_cursor;");
    defer lexer.freeTokens(alloc, &fetch_all_tokens);
    var fetch_all_pos: usize = 0;
    const fetch_all = try parseFetchCursorPortalTail(fetch_all_tokens.items, &fetch_all_pos);
    try std.testing.expectEqualStrings("usage_cursor", fetch_all.portal_name);
    try std.testing.expectEqual(CursorFetchDirectionSyntax.forward, fetch_all.direction);
    try std.testing.expect(fetch_all.count == null);
    try std.testing.expectEqual(fetch_all_tokens.items.len, fetch_all_pos);

    var extra_tokens = try lexer.tokenizeAlloc(alloc, "NEXT FROM usage_cursor; FETCH NEXT FROM other_cursor;");
    defer lexer.freeTokens(alloc, &extra_tokens);
    var extra_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseFetchCursorPortalTail(extra_tokens.items, &extra_pos));
}

test "sql adapter grammar parses row claim clauses" {
    const alloc = std.testing.allocator;

    var skip_tokens = try lexer.tokenizeAlloc(alloc, "NO KEY UPDATE OF usage_records, public.jobs SKIP LOCKED");
    defer lexer.freeTokens(alloc, &skip_tokens);
    var skip_pos: usize = 0;
    var skip_clause = try parseForRowClaimClauseAlloc(alloc, skip_tokens.items, &skip_pos);
    defer skip_clause.deinit(alloc);
    try std.testing.expectEqual(skip_tokens.items.len, skip_pos);
    try std.testing.expectEqual(db_mod.types.RowClaimMode.for_no_key_update, skip_clause.clause.mode);
    try std.testing.expectEqual(db_mod.types.RowClaimWaitPolicy.skip_locked, skip_clause.clause.wait_policy);
    try std.testing.expectEqual(@as(usize, 2), skip_clause.targets.len);
    try std.testing.expectEqualStrings("usage_records", skip_clause.targets[0]);
    try std.testing.expectEqualStrings("public.jobs", skip_clause.targets[1]);

    var share_tokens = try lexer.tokenizeAlloc(alloc, "KEY SHARE OF ONLY usage_records NOWAIT");
    defer lexer.freeTokens(alloc, &share_tokens);
    var share_pos: usize = 0;
    var share_clause = try parseForRowClaimClauseAlloc(alloc, share_tokens.items, &share_pos);
    defer share_clause.deinit(alloc);
    try std.testing.expectEqual(share_tokens.items.len, share_pos);
    try std.testing.expectEqual(db_mod.types.RowClaimMode.for_key_share, share_clause.clause.mode);
    try std.testing.expectEqual(db_mod.types.RowClaimWaitPolicy.nowait, share_clause.clause.wait_policy);
    try std.testing.expectEqual(@as(usize, 1), share_clause.targets.len);
    try std.testing.expectEqualStrings("usage_records", share_clause.targets[0]);

    var default_tokens = try lexer.tokenizeAlloc(alloc, "UPDATE");
    defer lexer.freeTokens(alloc, &default_tokens);
    var default_pos: usize = 0;
    var default_clause = try parseForRowClaimClauseAlloc(alloc, default_tokens.items, &default_pos);
    defer default_clause.deinit(alloc);
    try std.testing.expectEqual(default_tokens.items.len, default_pos);
    try std.testing.expectEqual(db_mod.types.RowClaimMode.for_update, default_clause.clause.mode);
    try std.testing.expectEqual(db_mod.types.RowClaimWaitPolicy.wait, default_clause.clause.wait_policy);

    var invalid_tokens = try lexer.tokenizeAlloc(alloc, "SHARE SKIP");
    defer lexer.freeTokens(alloc, &invalid_tokens);
    var invalid_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseForRowClaimClauseAlloc(alloc, invalid_tokens.items, &invalid_pos));

    try std.testing.expect(rowClaimTargetAllowed(alloc, "public.jobs", &.{"jobs"}));
    try std.testing.expect(rowClaimTargetAllowed(alloc, "usage_records", &.{ "usage_records", "u" }));
    try std.testing.expect(!rowClaimTargetAllowed(alloc, "tenant.jobs", &.{"jobs"}));

    var checked_tokens = try lexer.tokenizeAlloc(alloc, "UPDATE OF public.jobs NOWAIT");
    defer lexer.freeTokens(alloc, &checked_tokens);
    var checked_pos: usize = 0;
    const checked_clause = try parseCheckedForRowClaimClauseAlloc(alloc, checked_tokens.items, &checked_pos, &.{"jobs"});
    try std.testing.expectEqual(checked_tokens.items.len, checked_pos);
    try std.testing.expectEqual(db_mod.types.RowClaimMode.for_update, checked_clause.mode);
    try std.testing.expectEqual(db_mod.types.RowClaimWaitPolicy.nowait, checked_clause.wait_policy);

    var rejected_tokens = try lexer.tokenizeAlloc(alloc, "UPDATE OF tenant.jobs");
    defer lexer.freeTokens(alloc, &rejected_tokens);
    var rejected_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseCheckedForRowClaimClauseAlloc(alloc, rejected_tokens.items, &rejected_pos, &.{"jobs"}));

    var exclusive_tokens = try lexer.tokenizeAlloc(alloc, "NO KEY UPDATE OF jobs");
    defer lexer.freeTokens(alloc, &exclusive_tokens);
    var exclusive_pos: usize = 0;
    const exclusive_clause = try parseExclusiveForRowClaimClauseAlloc(alloc, exclusive_tokens.items, &exclusive_pos, &.{"jobs"});
    try std.testing.expectEqual(exclusive_tokens.items.len, exclusive_pos);
    try std.testing.expectEqual(db_mod.types.RowClaimMode.for_no_key_update, exclusive_clause.mode);

    var shared_tokens = try lexer.tokenizeAlloc(alloc, "SHARE OF jobs");
    defer lexer.freeTokens(alloc, &shared_tokens);
    var shared_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseExclusiveForRowClaimClauseAlloc(alloc, shared_tokens.items, &shared_pos, &.{"jobs"}));
}

test "sql adapter grammar parses relation population syntax" {
    const alloc = std.testing.allocator;

    const select_into_sql = "SELECT account_id, total INTO public.usage_archive FROM usage_records WHERE total > 10";
    var select_into = try parseRelationPopulationParsedForTestAlloc(
        alloc,
        select_into_sql,
    );
    defer select_into.deinit(alloc);
    try std.testing.expectEqual(RelationPopulationMode.select_into, select_into.mode);
    try std.testing.expectEqualStrings("public.usage_archive", select_into.target_identifier);
    try std.testing.expect(select_into.target_lifetime == null);
    try std.testing.expect(!select_into.if_not_exists);
    try expectRelationPopulationSourceRanges(
        alloc,
        select_into_sql,
        "SELECT account_id, total",
        "FROM usage_records WHERE total > 10",
    );

    const select_into_temp_sql = "SELECT account_id, total INTO TEMP TABLE usage_session_archive FROM usage_records WHERE total > 10";
    var select_into_temp = try parseRelationPopulationParsedForTestAlloc(
        alloc,
        select_into_temp_sql,
    );
    defer select_into_temp.deinit(alloc);
    try std.testing.expectEqual(RelationPopulationMode.select_into, select_into_temp.mode);
    try std.testing.expectEqualStrings("usage_session_archive", select_into_temp.target_identifier);
    try std.testing.expectEqual(RelationLifetimeKind.temporary, select_into_temp.target_lifetime.?);
    try std.testing.expect(!select_into_temp.if_not_exists);
    try expectRelationPopulationSourceRanges(
        alloc,
        select_into_temp_sql,
        "SELECT account_id, total",
        "FROM usage_records WHERE total > 10",
    );

    const select_into_unlogged_sql = "SELECT account_id INTO UNLOGGED usage_ingest_archive FROM usage_records WHERE total > 10";
    var select_into_unlogged = try parseRelationPopulationParsedForTestAlloc(
        alloc,
        select_into_unlogged_sql,
    );
    defer select_into_unlogged.deinit(alloc);
    try std.testing.expectEqual(RelationPopulationMode.select_into, select_into_unlogged.mode);
    try std.testing.expectEqualStrings("usage_ingest_archive", select_into_unlogged.target_identifier);
    try std.testing.expectEqual(RelationLifetimeKind.unlogged, select_into_unlogged.target_lifetime.?);
    try std.testing.expect(!select_into_unlogged.if_not_exists);
    try expectRelationPopulationSourceRanges(
        alloc,
        select_into_unlogged_sql,
        "SELECT account_id",
        "FROM usage_records WHERE total > 10",
    );

    const create_as_sql = "CREATE TEMP TABLE IF NOT EXISTS usage_session_archive AS SELECT account_id FROM usage_records";
    var create_as = try parseRelationPopulationParsedForTestAlloc(
        alloc,
        create_as_sql,
    );
    defer create_as.deinit(alloc);
    try std.testing.expectEqual(RelationPopulationMode.create_table_as, create_as.mode);
    try std.testing.expectEqualStrings("usage_session_archive", create_as.target_identifier);
    try std.testing.expectEqual(RelationLifetimeKind.temporary, create_as.target_lifetime.?);
    try std.testing.expect(create_as.if_not_exists);
    try std.testing.expect(create_as.populate);
    try std.testing.expect(create_as.source_token_start != null);
    try std.testing.expect(create_as.source_token_end.? > create_as.source_token_start.?);
    try std.testing.expect(create_as.source_suffix_token_start == null);
    try std.testing.expect(create_as.source_suffix_token_end == null);
    try expectRelationPopulationSourceRanges(
        alloc,
        create_as_sql,
        "SELECT account_id FROM usage_records",
        null,
    );

    const padded_create_as_sql = "  CREATE TABLE usage_archive AS SELECT account_id FROM usage_records WITH DATA;  ";
    var padded_tokens = try lexer.tokenizeAlloc(alloc, padded_create_as_sql);
    defer lexer.freeTokens(alloc, &padded_tokens);
    var parsed_from_tokens = try parseRelationPopulationTokensAlloc(
        alloc,
        std.mem.trim(u8, padded_create_as_sql, " \t\r\n;"),
        padded_tokens.items,
    );
    defer parsed_from_tokens.deinit(alloc);
    try std.testing.expectEqual(RelationPopulationMode.create_table_as, parsed_from_tokens.mode);
    try std.testing.expectEqualStrings("usage_archive", parsed_from_tokens.target_identifier);
    try std.testing.expect(parsed_from_tokens.populate);
    try std.testing.expect(parsed_from_tokens.source_token_start != null);
    try std.testing.expect(parsed_from_tokens.source_token_end.? > parsed_from_tokens.source_token_start.?);
    try expectRelationPopulationSourceRanges(
        alloc,
        padded_create_as_sql,
        "SELECT account_id FROM usage_records",
        null,
    );

    const create_as_no_data_sql = "CREATE TABLE usage_archive AS SELECT account_id FROM usage_records WITH NO DATA;";
    var create_as_no_data = try parseRelationPopulationParsedForTestAlloc(
        alloc,
        create_as_no_data_sql,
    );
    defer create_as_no_data.deinit(alloc);
    try std.testing.expectEqual(RelationPopulationMode.create_table_as, create_as_no_data.mode);
    try std.testing.expectEqualStrings("usage_archive", create_as_no_data.target_identifier);
    try std.testing.expect(create_as_no_data.target_lifetime == null);
    try std.testing.expect(!create_as_no_data.if_not_exists);
    try std.testing.expect(!create_as_no_data.populate);
    try std.testing.expect(create_as_no_data.source_token_end.? > create_as_no_data.source_token_start.?);
    try expectRelationPopulationSourceRanges(
        alloc,
        create_as_no_data_sql,
        "SELECT account_id FROM usage_records",
        null,
    );

    const create_as_with_data_sql = "CREATE TABLE usage_archive AS SELECT account_id FROM usage_records WITH DATA;";
    var create_as_with_data = try parseRelationPopulationParsedForTestAlloc(
        alloc,
        create_as_with_data_sql,
    );
    defer create_as_with_data.deinit(alloc);
    try std.testing.expectEqual(RelationPopulationMode.create_table_as, create_as_with_data.mode);
    try std.testing.expect(create_as_with_data.populate);
    try std.testing.expect(create_as_with_data.source_token_end.? > create_as_with_data.source_token_start.?);
    try expectRelationPopulationSourceRanges(
        alloc,
        create_as_with_data_sql,
        "SELECT account_id FROM usage_records",
        null,
    );

    try std.testing.expectError(
        error.UnsupportedSqlShape,
        parseRelationPopulationParsedForTestAlloc(alloc, "CREATE TABLE usage_archive SELECT account_id FROM usage_records"),
    );
}

fn parseRelationPopulationParsedForTestAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
) !RelationPopulationSyntax {
    var parsed_sql = try tokenized.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try parseRelationPopulationParsedSqlAlloc(alloc, &parsed_sql);
}

fn expectRelationPopulationSourceRanges(
    alloc: std.mem.Allocator,
    sql: []const u8,
    expected_prefix: []const u8,
    expected_suffix: ?[]const u8,
) !void {
    var parsed_sql = try tokenized.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    var parsed = try parseRelationPopulationParsedSqlAlloc(alloc, &parsed_sql);
    defer parsed.deinit(alloc);
    const tokens = parsed_sql.items();

    const start = parsed.source_token_start orelse return error.TestUnexpectedResult;
    const end = parsed.source_token_end orelse return error.TestUnexpectedResult;
    if (end <= start or end > tokens.len) return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings(expected_prefix, sql[tokens[start].source_start..tokens[end - 1].source_end]);

    if (expected_suffix) |suffix| {
        const suffix_start = parsed.source_suffix_token_start orelse return error.TestUnexpectedResult;
        const suffix_end = parsed.source_suffix_token_end orelse return error.TestUnexpectedResult;
        if (suffix_end <= suffix_start or suffix_end > tokens.len) return error.TestUnexpectedResult;
        try std.testing.expectEqualStrings(suffix, sql[tokens[suffix_start].source_start..tokens[suffix_end - 1].source_end]);
    } else {
        try std.testing.expect(parsed.source_suffix_token_start == null);
        try std.testing.expect(parsed.source_suffix_token_end == null);
    }
}

test "sql adapter grammar normalizes public object identifiers" {
    const alloc = std.testing.allocator;

    const bare = try normalizeSqlObjectIdentifierAlloc(alloc, "usage_records");
    defer alloc.free(bare);
    try std.testing.expectEqualStrings("usage_records", bare);

    const public_qualified = try normalizeSqlObjectIdentifierAlloc(alloc, "public.usage_records");
    defer alloc.free(public_qualified);
    try std.testing.expectEqualStrings("usage_records", public_qualified);

    const other_schema = try normalizeSqlObjectIdentifierAlloc(alloc, "tenant_1.usage_records");
    defer alloc.free(other_schema);
    try std.testing.expectEqualStrings("tenant_1.usage_records", other_schema);

    try std.testing.expectError(error.UnsupportedSqlShape, normalizeSqlObjectIdentifierAlloc(alloc, ".usage_records"));
    try std.testing.expectError(error.UnsupportedSqlShape, normalizeSqlObjectIdentifierAlloc(alloc, "public."));
    try std.testing.expectError(error.UnsupportedSqlShape, normalizeSqlObjectIdentifierAlloc(alloc, "public.analytics.usage_records"));
}

test "sql adapter grammar peeks joined mutation and assignment syntax" {
    const alloc = std.testing.allocator;

    var joined_alias = try lexer.tokenizeAlloc(alloc, "set total = src.total from usage_delta as src where src.id = usage.id");
    defer lexer.freeTokens(alloc, &joined_alias);
    try std.testing.expectEqualStrings("src", peekUpdateJoinedMutationSourceAlias(joined_alias.items, 0).?);

    var joined_table = try lexer.tokenizeAlloc(alloc, "set total = usage_delta.total from usage_delta where usage_delta.id = usage.id");
    defer lexer.freeTokens(alloc, &joined_table);
    try std.testing.expectEqualStrings("usage_delta", peekUpdateJoinedMutationSourceAlias(joined_table.items, 0).?);

    var nested_from = try lexer.tokenizeAlloc(alloc, "set payload = jsonb_set(payload, '{x}', to_jsonb(select from nested))");
    defer lexer.freeTokens(alloc, &nested_from);
    try std.testing.expect(peekUpdateJoinedMutationSourceAlias(nested_from.items, 0) == null);

    var static_value = try lexer.tokenizeAlloc(alloc, "-42");
    defer lexer.freeTokens(alloc, &static_value);
    try std.testing.expect(peekStaticToJsonbValue(static_value.items, 0));

    var expression_value = try lexer.tokenizeAlloc(alloc, "source.total + 1");
    defer lexer.freeTokens(alloc, &expression_value);
    try std.testing.expect(!peekStaticToJsonbValue(expression_value.items, 0));

    var array_append = try lexer.tokenizeAlloc(alloc, "array_append(tags, 'urgent')");
    defer lexer.freeTokens(alloc, &array_append);
    try std.testing.expect(peekArrayTransformSelfAssignment(array_append.items, 0, "tags"));
    try std.testing.expect(!peekArrayTransformSelfAssignment(array_append.items, 0, "labels"));
    var array_append_pos: usize = 0;
    try std.testing.expectEqual(db_mod.types.TransformOpType.push, matchArrayTransformUpdateOp(array_append.items, &array_append_pos).?);
    try std.testing.expectEqual(@as(usize, 1), array_append_pos);

    var array_remove = try lexer.tokenizeAlloc(alloc, "array_remove(tags, 'stale')");
    defer lexer.freeTokens(alloc, &array_remove);
    var array_remove_pos: usize = 0;
    try std.testing.expectEqual(db_mod.types.TransformOpType.pull, matchArrayTransformUpdateOp(array_remove.items, &array_remove_pos).?);
    try std.testing.expectEqual(@as(usize, 1), array_remove_pos);
}

test "sql adapter grammar parses owned identifiers and normalized object lists" {
    const alloc = std.testing.allocator;

    var identifiers = try lexer.tokenizeAlloc(alloc, "tenant_id, order_id, status");
    defer lexer.freeTokens(alloc, &identifiers);
    var identifier_pos: usize = 0;
    const identifier_list = try parseIdentifierListAlloc(alloc, identifiers.items, &identifier_pos);
    defer {
        for (identifier_list) |item| alloc.free(item);
        alloc.free(identifier_list);
    }
    try std.testing.expectEqual(identifiers.items.len, identifier_pos);
    try std.testing.expectEqual(@as(usize, 3), identifier_list.len);
    try std.testing.expectEqualStrings("tenant_id", identifier_list[0]);
    try std.testing.expectEqualStrings("order_id", identifier_list[1]);
    try std.testing.expectEqualStrings("status", identifier_list[2]);

    var objects = try lexer.tokenizeAlloc(alloc, "public.usage_records, tenant_1.audit_records");
    defer lexer.freeTokens(alloc, &objects);
    var object_pos: usize = 0;
    const object_list = try parseSqlObjectIdentifierListAlloc(alloc, objects.items, &object_pos);
    defer {
        for (object_list) |item| alloc.free(item);
        alloc.free(object_list);
    }
    try std.testing.expectEqual(objects.items.len, object_pos);
    try std.testing.expectEqual(@as(usize, 2), object_list.len);
    try std.testing.expectEqualStrings("usage_records", object_list[0]);
    try std.testing.expectEqualStrings("tenant_1.audit_records", object_list[1]);

    var table_ref = try lexer.tokenizeAlloc(alloc, "ONLY public.usage_records");
    defer lexer.freeTokens(alloc, &table_ref);
    var table_pos: usize = 0;
    const table_name = try parseSqlTableReferenceIdentifierOwnedAlloc(alloc, table_ref.items, &table_pos);
    defer alloc.free(table_name);
    try std.testing.expectEqual(table_ref.items.len, table_pos);
    try std.testing.expectEqualStrings("usage_records", table_name);

    var invalid = try lexer.tokenizeAlloc(alloc, "usage_records,");
    defer lexer.freeTokens(alloc, &invalid);
    var invalid_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseIdentifierListAlloc(alloc, invalid.items, &invalid_pos));
}

test "sql adapter grammar parses optional CTE column aliases" {
    const alloc = std.testing.allocator;

    var none = try lexer.tokenizeAlloc(alloc, "as");
    defer lexer.freeTokens(alloc, &none);
    var none_pos: usize = 0;
    const no_aliases = try parseOptionalCteColumnAliasesAlloc(alloc, none.items, &none_pos);
    try std.testing.expectEqual(@as(usize, 0), no_aliases.len);
    try std.testing.expectEqual(@as(usize, 0), none_pos);

    var aliases = try lexer.tokenizeAlloc(alloc, "(tenant_id, order_id) as");
    defer lexer.freeTokens(alloc, &aliases);
    var alias_pos: usize = 0;
    const parsed = try parseOptionalCteColumnAliasesAlloc(alloc, aliases.items, &alias_pos);
    defer freeStringSlice(alloc, parsed);
    try std.testing.expectEqual(@as(usize, 5), alias_pos);
    try std.testing.expectEqual(@as(usize, 2), parsed.len);
    try std.testing.expectEqualStrings("tenant_id", parsed[0]);
    try std.testing.expectEqualStrings("order_id", parsed[1]);

    var duplicate = try lexer.tokenizeAlloc(alloc, "(tenant_id, TENANT_ID)");
    defer lexer.freeTokens(alloc, &duplicate);
    var duplicate_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseOptionalCteColumnAliasesAlloc(alloc, duplicate.items, &duplicate_pos));

    var trailing = try lexer.tokenizeAlloc(alloc, "(tenant_id,)");
    defer lexer.freeTokens(alloc, &trailing);
    var trailing_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseOptionalCteColumnAliasesAlloc(alloc, trailing.items, &trailing_pos));
}
