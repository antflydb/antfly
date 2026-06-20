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
const ddl_plan = @import("ddl_plan.zig");
const grammar = @import("grammar.zig");
const lower_expr = @import("lower_expr.zig");
const parser = @import("parser.zig");
const plan_mod = @import("plan.zig");
const relational_rows = @import("../relational_rows.zig");
const runtime_schema = @import("../../storage/schema.zig");
const strings = @import("strings.zig");
const sql_value = @import("value.zig");

const Token = lower_expr.Token;

pub const InsertSourceParserHooks = struct {
    ptr: *anyopaque,
    parse_insert_source: *const fn (
        *anyopaque,
        []const db_mod.types.RelationalRowsCte,
        ?*?[]const u8,
    ) anyerror!plan_mod.LoweredInsertSource,
};

pub const MergeMutationParserHooks = struct {
    ptr: *anyopaque,
    parse_merge_mutation: *const fn (
        *anyopaque,
        []const db_mod.types.RelationalRowsCte,
        ?*?[]const u8,
    ) anyerror!plan_mod.LoweredMergeMutationPlan,
};

pub const JoinedMutationSourceParserHooks = struct {
    ptr: *anyopaque,
    parse_joined_mutation_source: *const fn (
        *anyopaque,
        []const db_mod.types.RelationalRowsCte,
        ?*?[]const u8,
    ) anyerror!plan_mod.LoweredJoinedMutationSource,
};

pub const ConflictTargetParserHooks = struct {
    ptr: *anyopaque,
    parse_where_expression_conditions: *const fn (*anyopaque) anyerror![]const db_mod.types.RelationalRowsExpressionCondition,
};

const ConflictCoalesceValueParserHooks = struct {
    ptr: *anyopaque,
    insert_columns: []const []const u8,
    insert_values: []const []const u8,
    parse_operand_value_json: *const fn (
        *anyopaque,
        runtime_schema.RelationalColumn,
        []const []const u8,
        []const []const u8,
    ) anyerror![]const u8,
};

const ConflictJsonbBuildObjectParserHooks = struct {
    ptr: *anyopaque,
    insert_columns: []const []const u8,
    insert_values: []const []const u8,
    parse_value_json: *const fn (
        *anyopaque,
        []const []const u8,
        []const []const u8,
    ) anyerror![]const u8,
};

pub const ConflictValueParserHooks = struct {
    ptr: *anyopaque,
    insert_columns: []const []const u8,
    insert_values: []const []const u8,
    parse_column_value_json: *const fn (*anyopaque, runtime_schema.RelationalColumn) anyerror![]const u8,
    parse_json_value_json: *const fn (*anyopaque) anyerror![]const u8,
    parse_coalesce_operand_value_json: *const fn (
        *anyopaque,
        runtime_schema.RelationalColumn,
        []const []const u8,
        []const []const u8,
    ) anyerror![]const u8,
    parse_jsonb_build_object_value_json: *const fn (
        *anyopaque,
        []const []const u8,
        []const []const u8,
    ) anyerror![]const u8,
};

pub const ConflictIncrementParserHooks = struct {
    ptr: *anyopaque,
    insert_columns: []const []const u8,
    insert_values: []const []const u8,
    parse_coalesce_expression: *const fn (
        *anyopaque,
        runtime_schema.RelationalColumn,
        []const []const u8,
    ) anyerror!db_mod.types.RelationalRowsExpression,
    parse_value_json: *const fn (
        *anyopaque,
        runtime_schema.RelationalColumn,
        []const []const u8,
        []const []const u8,
    ) anyerror![]const u8,
};

pub const JsonSetSqlValueParserHooks = struct {
    ptr: *anyopaque,
    insert_columns: []const []const u8,
    parse_json_value: *const fn (*anyopaque) anyerror![]const u8,
    parse_expression: *const fn (
        *anyopaque,
        runtime_schema.RelationalColumn,
        []const []const u8,
    ) anyerror!db_mod.types.RelationalRowsExpression,
};

pub const ConflictUpdateAssignmentValueParserHooks = struct {
    ptr: *anyopaque,
    insert_columns: []const []const u8,
    insert_values: []const []const u8,
    parse_json_set_value: *const fn (
        *anyopaque,
        runtime_schema.RelationalColumn,
        []const []const u8,
    ) anyerror!JsonSetParsedValue,
    parse_array_element_value_json: *const fn (
        *anyopaque,
        runtime_schema.RelationalColumn,
        []const []const u8,
        []const []const u8,
    ) anyerror![]const u8,
    parse_json_document_value: *const fn (*anyopaque) anyerror![]const u8,
    parse_boolean_expression: *const fn (
        *anyopaque,
        runtime_schema.RelationalColumn,
        []const []const u8,
    ) anyerror!db_mod.types.RelationalRowsExpression,
    parse_assignment_expression: *const fn (
        *anyopaque,
        runtime_schema.RelationalColumn,
        []const []const u8,
    ) anyerror!db_mod.types.RelationalRowsExpression,
    parse_coalesce_expression: *const fn (
        *anyopaque,
        runtime_schema.RelationalColumn,
        []const []const u8,
    ) anyerror!db_mod.types.RelationalRowsExpression,
    parse_value_json: *const fn (
        *anyopaque,
        runtime_schema.RelationalColumn,
        []const []const u8,
        []const []const u8,
    ) anyerror![]const u8,
};

pub const ConflictAssignmentExpressionParserHooks = struct {
    ptr: *anyopaque,
    parse_expression: *const fn (
        *anyopaque,
        runtime_schema.RelationalColumn,
        []const []const u8,
    ) anyerror!db_mod.types.RelationalRowsExpression,
    parse_arithmetic_rest: *const fn (
        *anyopaque,
        db_mod.types.RelationalRowsExpression,
        runtime_schema.RelationalColumn,
        []const []const u8,
    ) anyerror!db_mod.types.RelationalRowsExpression,
    parse_pipe_concat_rest: *const fn (
        *anyopaque,
        db_mod.types.RelationalRowsExpression,
        runtime_schema.RelationalColumn,
        []const []const u8,
    ) anyerror!db_mod.types.RelationalRowsExpression,
};

pub const ConflictPipeConcatExpressionParserHooks = struct {
    ptr: *anyopaque,
    parse_operand: *const fn (
        *anyopaque,
        runtime_schema.RelationalColumn,
        []const []const u8,
    ) anyerror!db_mod.types.RelationalRowsExpression,
};

pub const ConflictArithmeticExpressionParserHooks = struct {
    ptr: *anyopaque,
    parse_expression: *const fn (
        *anyopaque,
        runtime_schema.RelationalColumn,
        []const []const u8,
    ) anyerror!db_mod.types.RelationalRowsExpression,
};

pub const ConflictCoalesceExpressionParserHooks = struct {
    ptr: *anyopaque,
    parse_operand: *const fn (
        *anyopaque,
        runtime_schema.RelationalColumn,
        []const []const u8,
        ?runtime_schema.AntflyType,
    ) anyerror!db_mod.types.RelationalRowsExpression,
};

pub const ConflictNullifExpressionParserHooks = struct {
    ptr: *anyopaque,
    parse_expression: *const fn (
        *anyopaque,
        runtime_schema.RelationalColumn,
        []const []const u8,
    ) anyerror!db_mod.types.RelationalRowsExpression,
};

pub const ConflictUnaryExpressionParserHooks = struct {
    ptr: *anyopaque,
    parse_operand: *const fn (
        *anyopaque,
        runtime_schema.RelationalColumn,
        []const []const u8,
        ?runtime_schema.AntflyType,
    ) anyerror!db_mod.types.RelationalRowsExpression,
};

pub const ConflictJsonBuildObjectExpressionParserHooks = struct {
    ptr: *anyopaque,
    parse_operand: *const fn (
        *anyopaque,
        runtime_schema.RelationalColumn,
        []const []const u8,
        ?runtime_schema.AntflyType,
    ) anyerror!db_mod.types.RelationalRowsExpression,
    parse_json_value: *const fn (*anyopaque) anyerror![]const u8,
};

pub const ConflictArrayLengthExpressionParserHooks = struct {
    ptr: *anyopaque,
    parse_field: *const fn (
        *anyopaque,
        []const []const u8,
        ?runtime_schema.AntflyType,
    ) anyerror!db_mod.types.RelationalRowsExpression,
};

pub const ConflictBooleanExpressionParserHooks = struct {
    ptr: *anyopaque,
    parse_operand: *const fn (
        *anyopaque,
        runtime_schema.RelationalColumn,
        []const []const u8,
        ?runtime_schema.AntflyType,
    ) anyerror!db_mod.types.RelationalRowsExpression,
};

pub const ConflictExpressionConditionParserHooks = struct {
    ptr: *anyopaque,
    parse_untyped_expression: *const fn (
        *anyopaque,
        runtime_schema.RelationalColumn,
        []const []const u8,
    ) anyerror!db_mod.types.RelationalRowsExpression,
    parse_boolean_operand: *const fn (
        *anyopaque,
        runtime_schema.RelationalColumn,
        []const []const u8,
        ?runtime_schema.AntflyType,
    ) anyerror!db_mod.types.RelationalRowsExpression,
    parse_json_array_value: *const fn (*anyopaque) anyerror![]const u8,
};

pub const ConflictCaseExpressionParserHooks = struct {
    ptr: *anyopaque,
    parse_expression: *const fn (
        *anyopaque,
        runtime_schema.RelationalColumn,
        []const []const u8,
    ) anyerror!db_mod.types.RelationalRowsExpression,
    parse_untyped_expression: *const fn (
        *anyopaque,
        runtime_schema.RelationalColumn,
        []const []const u8,
    ) anyerror!db_mod.types.RelationalRowsExpression,
    parse_boolean_operand: *const fn (
        *anyopaque,
        runtime_schema.RelationalColumn,
        []const []const u8,
        ?runtime_schema.AntflyType,
    ) anyerror!db_mod.types.RelationalRowsExpression,
    parse_json_array_value: *const fn (*anyopaque) anyerror![]const u8,
};

pub const ConflictCaseFoldExpressionParserHooks = struct {
    ptr: *anyopaque,
    parse_operand: *const fn (
        *anyopaque,
        runtime_schema.RelationalColumn,
        []const []const u8,
        ?runtime_schema.AntflyType,
    ) anyerror!db_mod.types.RelationalRowsExpression,
    parse_period_bound: *const fn (
        *anyopaque,
        db_mod.types.RelationalRowsExpressionKind,
    ) anyerror!?db_mod.types.RelationalRowsExpression,
};

pub const JoinedMutationJsonSetSqlValueParserHooks = struct {
    ptr: *anyopaque,
    parse_json_value: *const fn (*anyopaque) anyerror![]const u8,
    parse_expression: *const fn (
        *anyopaque,
        []const u8,
    ) anyerror!db_mod.types.RelationalRowsExpression,
};

pub const JoinedMutationAssignmentValueParserHooks = struct {
    ptr: *anyopaque,
    parse_json_set_value: *const fn (
        *anyopaque,
        runtime_schema.RelationalColumn,
        []const u8,
    ) anyerror!JsonSetParsedValue,
    parse_json_document_value: *const fn (*anyopaque) anyerror![]const u8,
    parse_boolean_expression: *const fn (
        *anyopaque,
        []const u8,
    ) anyerror!db_mod.types.RelationalRowsExpression,
    parse_assignment_expression: *const fn (
        *anyopaque,
        runtime_schema.RelationalColumn,
        []const u8,
    ) anyerror!db_mod.types.RelationalRowsExpression,
    parse_column_value_json: *const fn (*anyopaque, runtime_schema.RelationalColumn) anyerror![]const u8,
};

pub const JoinedMutationAssignmentExpressionParserHooks = struct {
    ptr: *anyopaque,
    parse_row_expression: *const fn (
        *anyopaque,
        []const u8,
    ) anyerror!db_mod.types.RelationalRowsExpression,
    parse_boolean_row_expression: *const fn (
        *anyopaque,
        []const u8,
    ) anyerror!db_mod.types.RelationalRowsExpression,
};

pub const SemiJoinTargetFieldsParserHooks = struct {
    ptr: *anyopaque,
    parse_field_expression_owned: *const fn (*anyopaque) anyerror![]const u8,
};

pub const MergeAssignmentParserHooks = struct {
    ptr: *anyopaque,
    parse_assignment_expression: *const fn (
        *anyopaque,
        runtime_schema.RelationalColumn,
        TableAlias,
        TableAlias,
    ) anyerror!db_mod.types.RelationalRowsExpression,
};

pub const MergeAssignmentExpressionParserHooks = struct {
    ptr: *anyopaque,
    parse_row_expression: *const fn (
        *anyopaque,
        TableAlias,
        TableAlias,
    ) anyerror!db_mod.types.RelationalRowsExpression,
};

pub const MergeArmPredicateParserHooks = struct {
    ptr: *anyopaque,
    parse_column_value_json: *const fn (*anyopaque, runtime_schema.RelationalColumn) anyerror![]const u8,
};

pub const MergeArmExpressionPredicatesParserHooks = struct {
    ptr: *anyopaque,
    parse_expression_not_where: *const fn (
        *anyopaque,
        TableAlias,
        TableAlias,
        *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    ) anyerror!void,
    parse_expression_or_where: *const fn (
        *anyopaque,
        TableAlias,
        TableAlias,
        *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    ) anyerror!void,
    parse_expression_where_conditions: *const fn (
        *anyopaque,
        TableAlias,
        TableAlias,
        *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionCondition),
        *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
        *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    ) anyerror!void,
};

pub const MergeArmConditionParserHooks = struct {
    ptr: *anyopaque,
    parse_column_value_json: *const fn (*anyopaque, runtime_schema.RelationalColumn) anyerror![]const u8,
    parse_expression_predicates: *const fn (
        *anyopaque,
        TableAlias,
        TableAlias,
        bool,
        *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionCondition),
        *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
        *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    ) anyerror!void,
};

pub const IncrementParserHooks = struct {
    ptr: *anyopaque,
    parse_value_json: *const fn (*anyopaque, runtime_schema.RelationalColumn) anyerror![]const u8,
};

const LoweredMergeMutationPlan = plan_mod.LoweredMergeMutationPlan;
const MergeArmPredicate = plan_mod.MergeArmPredicate;
const MergeExpressionAssignment = plan_mod.MergeExpressionAssignment;
const MergeFieldMapping = plan_mod.MergeFieldMapping;
const MergeMatchedArm = plan_mod.MergeMatchedArm;
const MergeNotMatchedArm = plan_mod.MergeNotMatchedArm;
const MergePredicateSide = plan_mod.MergePredicateSide;
const ReturningProjection = plan_mod.ReturningProjection;
const SelectOutputRef = plan_mod.SelectOutputRef;
const TableAlias = plan_mod.TableAlias;

const cloneExpressionAlloc = plan_mod.cloneExpressionAlloc;
const cloneExpressionConditionAlloc = plan_mod.cloneExpressionConditionAlloc;
const cloneExpressionConditionsAlloc = plan_mod.cloneExpressionConditionsAlloc;
const cloneExpressionPredicateGroupsAlloc = plan_mod.cloneExpressionPredicateGroupsAlloc;
const freeArrayLengthProjections = plan_mod.freeArrayLengthProjections;
const freeCoalesceProjections = plan_mod.freeCoalesceProjections;
const freeExpression = plan_mod.freeExpression;
const freeExpressionAssignments = plan_mod.freeExpressionAssignments;
const freeExpressionArrayContains = plan_mod.freeExpressionArrayContains;
const freeExpressionCaseBranch = plan_mod.freeExpressionCaseBranch;
const freeExpressionCondition = plan_mod.freeExpressionCondition;
const freeExpressionConditions = plan_mod.freeExpressionConditions;
const freeExpressionPredicateGroups = plan_mod.freeExpressionPredicateGroups;
const freeExpressionProjections = plan_mod.freeExpressionProjections;
const freeFieldAliasProjections = plan_mod.freeFieldAliasProjections;
const freeInPredicates = plan_mod.freeInPredicates;
const freeJoinOn = plan_mod.freeJoinOn;
const freeJsonExtract = plan_mod.freeJsonExtract;
const freeMergeArmPredicateValue = plan_mod.freeMergeArmPredicateValue;
const freeRelationalChecks = plan_mod.freeRelationalChecks;
const freeRowsJsonSetExpressionAssignments = plan_mod.freeRowsJsonSetExpressionAssignments;
const freeTableAlias = plan_mod.freeTableAlias;
const clearDdlUniqueExpressions = ddl_plan.clearDdlUniqueExpressions;
const freeDdlUniqueExpression = ddl_plan.freeDdlUniqueExpression;

pub const InsertValueRows = []const []const []const u8;

pub const InsertColumnSpec = union(enum) {
    column: []const u8,
    period: runtime_schema.RelationalPeriod,
};

pub const FieldJsonValue = struct {
    field: []const u8,
    value_json: []const u8,
};

pub fn parseInsertSourceAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    cte_hooks: plan_mod.CteSelectParserHooks,
    hooks: InsertSourceParserHooks,
) !plan_mod.LoweredInsertSource {
    if (!parser.peekKeyword(tokens, pos.*, "with")) return try hooks.parse_insert_source(hooks.ptr, &.{}, null);

    var base_table_name: ?[]const u8 = null;
    defer if (base_table_name) |table| alloc.free(table);
    var ctes = try plan_mod.parseCtesForPlanAlloc(alloc, tokens, pos, &base_table_name, cte_hooks);
    errdefer plan_mod.freePlanCtes(alloc, ctes);

    var lowered = try hooks.parse_insert_source(hooks.ptr, ctes, &base_table_name);
    errdefer lowered.deinit(alloc);
    lowered.ctes = ctes;
    ctes = &.{};
    return lowered;
}

pub fn parseMergeMutationPlanAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    cte_hooks: plan_mod.CteSelectParserHooks,
    hooks: MergeMutationParserHooks,
) !plan_mod.LoweredMergeMutationPlan {
    if (!parser.peekKeyword(tokens, pos.*, "with")) return try hooks.parse_merge_mutation(hooks.ptr, &.{}, null);

    var base_table_name: ?[]const u8 = null;
    defer if (base_table_name) |table| alloc.free(table);
    var ctes = try plan_mod.parseCtesForPlanAlloc(alloc, tokens, pos, &base_table_name, cte_hooks);
    errdefer plan_mod.freePlanCtes(alloc, ctes);

    var final = try hooks.parse_merge_mutation(hooks.ptr, ctes, &base_table_name);
    errdefer final.deinit(alloc);
    if (parser.matchToken(tokens, pos, .semicolon) != null and !parser.atEnd(tokens, pos.*)) return error.UnsupportedSqlShape;
    if (!parser.atEnd(tokens, pos.*)) return error.UnsupportedSqlShape;
    _ = base_table_name orelse return error.UnsupportedSqlShape;
    final.ctes = ctes;
    ctes = &.{};
    return final;
}

pub fn parseJoinedMutationSourceAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    cte_hooks: plan_mod.CteSelectParserHooks,
    hooks: JoinedMutationSourceParserHooks,
) !plan_mod.LoweredJoinedMutationSource {
    if (!parser.peekKeyword(tokens, pos.*, "with")) return try hooks.parse_joined_mutation_source(hooks.ptr, &.{}, null);

    var base_table_name: ?[]const u8 = null;
    defer if (base_table_name) |table| alloc.free(table);
    var ctes = try plan_mod.parseCtesForPlanAlloc(alloc, tokens, pos, &base_table_name, cte_hooks);
    errdefer plan_mod.freePlanCtes(alloc, ctes);

    const lowered = try hooks.parse_joined_mutation_source(hooks.ptr, ctes, &base_table_name);
    plan_mod.freePlanCtes(alloc, ctes);
    ctes = &.{};
    return lowered;
}

pub const JoinedMutationSourceAssignment = struct {
    field: []const u8,
    source_qualifier: []const u8,
    source_field: []const u8,
};

pub const FieldExpressionValue = struct {
    field: []const u8,
    expression: db_mod.types.RelationalRowsExpression,
};

pub const FieldPredicate = struct {
    field: []const u8,
    op: runtime_schema.UniquePredicateOp,
    value_json: ?[]const u8 = null,
};

pub const JsonSetValue = struct {
    field: []const u8,
    path: []const []const u8,
    value_json: ?[]const u8 = null,
    expression: ?db_mod.types.RelationalRowsExpression = null,
};

pub const JsonSetParsedValue = struct {
    value_json: ?[]const u8 = null,
    expression: ?db_mod.types.RelationalRowsExpression = null,
};

pub const ArrayTransformValue = struct {
    field: []const u8,
    op: db_mod.types.TransformOpType,
    value_json: []const u8,
};

pub fn validateInsertSourceAssignmentExpressionType(
    type_context: lower_expr.RowExpressionTypeContext,
    target_column: runtime_schema.RelationalColumn,
    expression: db_mod.types.RelationalRowsExpression,
) !void {
    const expression_type = try type_context.rowExpressionOutputType(expression);
    const compatible = if (target_column.field_type == .json or target_column.field_type == .array)
        expression_type == target_column.field_type
    else
        lower_expr.sqlExpressionTypesComparable(target_column.field_type, expression_type);
    if (!compatible) return error.UnsupportedSqlShape;
}

pub const AssignmentExpressionValidationOptions = struct {
    validate_json: bool = false,
};

pub fn validateMutationAssignmentExpressionType(
    type_context: lower_expr.RowExpressionTypeContext,
    target_column: runtime_schema.RelationalColumn,
    expression: db_mod.types.RelationalRowsExpression,
    options: AssignmentExpressionValidationOptions,
) !void {
    if (lower_expr.sqlExpressionIsInterval(expression)) return error.UnsupportedSqlShape;
    try validateInsertSourceAssignmentExpressionType(type_context, target_column, expression);
    if (target_column.field_type == .numeric) {
        try type_context.validateNumericRowExpression(expression);
    } else if (target_column.field_type == .datetime) {
        try type_context.validateNumericOrDatetimeRowExpression(expression);
    } else if (target_column.field_type == .keyword or target_column.field_type == .text or target_column.field_type == .link) {
        try type_context.validateTextRowExpression(expression);
    } else if (target_column.field_type == .boolean) {
        try type_context.validateBooleanRowExpression(expression);
    } else if (target_column.field_type == .json and options.validate_json) {
        try type_context.validateJsonRowExpression(expression);
    }
}

pub fn insertSourceSelectSourceCteSchema(
    tokens: []const Token,
    start: usize,
    end: usize,
    planned_ctes: []const relational_rows.RowsPlannedCte,
) !?runtime_schema.TableSchema {
    if (planned_ctes.len == 0) return null;
    const from_index = parser.findTopLevelKeyword(tokens[start..end], "from") orelse return null;
    var source_index = start + from_index + 1;
    _ = parser.matchKeyword(tokens, &source_index, "only");
    if (source_index >= end or tokens[source_index].kind != .identifier) return error.UnsupportedSqlShape;
    return relational_rows.rowsPlannedCteSchema(planned_ctes, tokens[source_index].text);
}

pub fn insertSourceAssignmentsFromSelectAlloc(
    alloc: std.mem.Allocator,
    target_schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    type_context: lower_expr.RowExpressionTypeContext,
    target_columns: []const []const u8,
    source_query: db_mod.types.RelationalRowsQueryRequest,
    source_outputs: []const SelectOutputRef,
) ![]const db_mod.types.RelationalRowsExpressionAssignment {
    if (target_columns.len != source_outputs.len) return error.UnsupportedSqlShape;
    const assignments = try alloc.alloc(db_mod.types.RelationalRowsExpressionAssignment, target_columns.len);
    var initialized: usize = 0;
    errdefer {
        for (assignments[0..initialized]) |assignment| {
            alloc.free(@constCast(assignment.field));
            freeExpression(alloc, assignment.expression);
        }
        alloc.free(assignments);
    }
    for (target_columns, source_outputs) |target_name, output| {
        const target_column = binder.relationalColumnForField(target_schema, target_name, null) orelse return error.InvalidSqlCatalog;
        const expression = try insertSourceExpressionFromSelectOutputAlloc(alloc, source_schema, source_query, output);
        var expression_transferred = false;
        errdefer if (!expression_transferred) freeExpression(alloc, expression);
        try validateInsertSourceAssignmentExpressionType(type_context, target_column, expression);
        const target_field = try alloc.dupe(u8, target_column.path);
        var target_transferred = false;
        errdefer if (!target_transferred) alloc.free(target_field);
        assignments[initialized] = .{
            .field = target_field,
            .expression = expression,
        };
        target_transferred = true;
        expression_transferred = true;
        initialized += 1;
    }
    return assignments;
}

pub fn insertSourceExpressionFromSelectOutputAlloc(
    alloc: std.mem.Allocator,
    source_schema: runtime_schema.TableSchema,
    source_query: db_mod.types.RelationalRowsQueryRequest,
    output: SelectOutputRef,
) !db_mod.types.RelationalRowsExpression {
    var expression = switch (output.kind) {
        .field => blk: {
            if (output.index >= source_query.select.len) return error.UnsupportedSqlShape;
            const source_column = binder.relationalColumnForField(source_schema, source_query.select[output.index], null) orelse return error.InvalidSqlCatalog;
            break :blk db_mod.types.RelationalRowsExpression{
                .kind = .field,
                .field = try alloc.dupe(u8, source_column.path),
                .field_source = .source,
            };
        },
        .json_extract => blk: {
            if (output.index >= source_query.json_extract.len) return error.UnsupportedSqlShape;
            break :blk try lower_expr.expressionFromJsonExtractProjectionAlloc(alloc, source_query.json_extract[output.index]);
        },
        .array_length => blk: {
            if (output.index >= source_query.array_length.len) return error.UnsupportedSqlShape;
            break :blk try lower_expr.expressionFromArrayLengthProjectionAlloc(alloc, source_query.array_length[output.index]);
        },
        .coalesce => blk: {
            if (output.index >= source_query.coalesce.len) return error.UnsupportedSqlShape;
            break :blk try lower_expr.expressionFromCoalesceProjectionAlloc(alloc, source_query.coalesce[output.index]);
        },
        .field_alias => blk: {
            if (output.index >= source_query.field_aliases.len) return error.UnsupportedSqlShape;
            const source_column = binder.relationalColumnForField(source_schema, source_query.field_aliases[output.index].field, null) orelse return error.InvalidSqlCatalog;
            break :blk db_mod.types.RelationalRowsExpression{
                .kind = .field,
                .field = try alloc.dupe(u8, source_column.path),
                .field_source = .source,
            };
        },
        .expression => blk: {
            if (output.index >= source_query.expressions.len) return error.UnsupportedSqlShape;
            break :blk try cloneExpressionAlloc(alloc, source_query.expressions[output.index].expression);
        },
    };
    errdefer freeExpression(alloc, expression);
    plan_mod.rewriteExpressionFieldsToSource(&expression);
    return expression;
}

pub fn clearInsertSourceQueryProjection(alloc: std.mem.Allocator, query: *db_mod.types.RelationalRowsQueryRequest) void {
    strings.freeStringSlice(alloc, query.select);
    freeJsonExtract(alloc, query.json_extract);
    freeArrayLengthProjections(alloc, query.array_length);
    freeCoalesceProjections(alloc, query.coalesce);
    freeFieldAliasProjections(alloc, query.field_aliases);
    freeExpressionProjections(alloc, query.expressions);
    query.select = &.{};
    query.json_extract = &.{};
    query.array_length = &.{};
    query.coalesce = &.{};
    query.field_aliases = &.{};
    query.expressions = &.{};
    query.select_all = true;
}

pub fn insertSourceOnConflictFromClauseAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    clause: ConflictClause,
) !db_mod.types.RelationalRowsOnConflict {
    const target = try insertSourceConflictTargetFromClauseAlloc(alloc, schema, clause.target);
    var target_transferred = false;
    errdefer if (!target_transferred) freeRowsConflictTargetValue(alloc, target);

    if (clause.action == .nothing) {
        target_transferred = true;
        return .{
            .target = target,
            .action = .nothing,
        };
    }

    const operations = try insertSourceConflictOperationsFromClauseAlloc(alloc, schema, clause);
    var operations_transferred = false;
    errdefer if (!operations_transferred) freeTransformOps(alloc, operations);

    const patch_expressions = try insertSourceConflictExpressionAssignmentsFromClauseAlloc(alloc, schema, clause.patch_expr);
    var patch_transferred = false;
    errdefer if (!patch_transferred) freeExpressionAssignments(alloc, patch_expressions);

    const increment_expressions = try insertSourceConflictExpressionAssignmentsFromClauseAlloc(alloc, schema, clause.increment_expr);
    var increment_transferred = false;
    errdefer if (!increment_transferred) freeExpressionAssignments(alloc, increment_expressions);

    const json_set_expressions = try insertSourceConflictJsonSetExpressionAssignmentsFromClauseAlloc(alloc, schema, clause.json_set);
    var json_set_transferred = false;
    errdefer if (!json_set_transferred) freeRowsJsonSetExpressionAssignments(alloc, json_set_expressions);

    const where_expression: ?db_mod.types.RelationalRowsExpressionCondition = if (clause.where_expression) |condition|
        try cloneExpressionConditionAlloc(alloc, condition)
    else
        null;
    var where_transferred = false;
    errdefer if (!where_transferred) if (where_expression) |condition| freeExpressionCondition(alloc, condition);

    const where_expressions = try cloneExpressionConditionsAlloc(alloc, clause.where_expressions);
    var where_expressions_transferred = false;
    errdefer if (!where_expressions_transferred) {
        freeExpressionConditions(alloc, where_expressions);
        if (where_expressions.len > 0) alloc.free(where_expressions);
    };

    const where_any = try cloneExpressionPredicateGroupsAlloc(alloc, clause.where_any);
    var where_any_transferred = false;
    errdefer if (!where_any_transferred) {
        freeExpressionPredicateGroups(alloc, where_any);
        if (where_any.len > 0) alloc.free(where_any);
    };

    const where_not = try cloneExpressionPredicateGroupsAlloc(alloc, clause.where_not);
    var where_not_transferred = false;
    errdefer if (!where_not_transferred) {
        freeExpressionPredicateGroups(alloc, where_not);
        if (where_not.len > 0) alloc.free(where_not);
    };

    target_transferred = true;
    operations_transferred = true;
    patch_transferred = true;
    increment_transferred = true;
    json_set_transferred = true;
    where_transferred = true;
    where_expressions_transferred = true;
    where_any_transferred = true;
    where_not_transferred = true;
    return .{
        .target = target,
        .action = .update,
        .operations = operations,
        .patch_expressions = patch_expressions,
        .increment_expressions = increment_expressions,
        .json_set_expressions = json_set_expressions,
        .where_expression = where_expression,
        .where_expressions = where_expressions,
        .where_any = where_any,
        .where_not = where_not,
    };
}

pub fn insertSourceConflictTargetFromClauseAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    target: ConflictTarget,
) !db_mod.types.RelationalRowsConflictTarget {
    return switch (target) {
        .primary => .{ .kind = .primary },
        .unique => |unique| blk: {
            const constraint = binder.findUniqueConstraintByName(schema, unique.name) orelse return error.InvalidSqlCatalog;
            if (unique.where_json.len > 0 and constraint.where.len == 0) return error.UnsupportedSqlShape;
            if (unique.where_expressions.len > 0 and !lower_expr.relationalRowsExpressionConditionsEqual(constraint.where_expressions, unique.where_expressions)) return error.UnsupportedSqlShape;

            const name = try alloc.dupe(u8, unique.name);
            var name_transferred = false;
            errdefer if (!name_transferred) alloc.free(name);

            const predicates = try insertSourceUniquePredicatesFromConstraintAlloc(alloc, constraint.where);
            var predicates_transferred = false;
            errdefer if (!predicates_transferred) {
                freeRelationalChecks(alloc, predicates);
                if (predicates.len > 0) alloc.free(predicates);
            };

            const predicate_expressions = try cloneExpressionConditionsAlloc(alloc, constraint.where_expressions);
            var predicate_expressions_transferred = false;
            errdefer if (!predicate_expressions_transferred) {
                freeExpressionConditions(alloc, predicate_expressions);
                if (predicate_expressions.len > 0) alloc.free(predicate_expressions);
            };

            name_transferred = true;
            predicates_transferred = true;
            predicate_expressions_transferred = true;
            break :blk .{
                .kind = .unique,
                .unique_name = name,
                .unique_predicates = predicates,
                .unique_predicate_expressions = predicate_expressions,
            };
        },
    };
}

pub fn conflictTargetIdentityAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    insert_columns: []const []const u8,
    row: []const []const u8,
    target: ConflictTarget,
) ![]const u8 {
    return switch (target) {
        .primary => try conflictColumnIdentityAlloc(alloc, "primary", schema.primary_key.?.columns, insert_columns, row),
        .unique => |unique| blk: {
            const constraint = binder.findUniqueConstraintByName(schema, unique.name) orelse return error.InvalidSqlCatalog;
            break :blk try conflictUniqueConstraintIdentityAlloc(alloc, unique.name, constraint, insert_columns, row);
        },
    };
}

pub fn conflictUniqueConstraintIdentityAlloc(
    alloc: std.mem.Allocator,
    label: []const u8,
    constraint: runtime_schema.UniqueConstraint,
    insert_columns: []const []const u8,
    row: []const []const u8,
) ![]const u8 {
    if (constraint.columns.len == 0 and constraint.expressions.len == 0) return error.UnsupportedSqlShape;
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.print("{f}:[", .{std.json.fmt(label, .{})});
    var wrote = false;
    for (constraint.columns) |target_column| {
        const value_json = lower_expr.conflictInsertedValueForColumn(insert_columns, row, target_column) orelse return error.UnsupportedSqlShape;
        if (!constraint.nulls_not_distinct and std.mem.eql(u8, value_json, "null")) return error.UnsupportedSqlShape;
        if (wrote) try writer.writeByte(',');
        try writer.print("{{\"column\":{f},\"value\":", .{std.json.fmt(target_column, .{})});
        try writer.writeAll(value_json);
        try writer.writeByte('}');
        wrote = true;
    }
    for (constraint.expressions) |expression| {
        const value = try conflictExpressionValueAlloc(alloc, expression, insert_columns, row);
        defer alloc.free(value);
        if (wrote) try writer.writeByte(',');
        try writer.writeAll("{\"expression\":");
        try writeUniqueExpressionIdentityJson(writer, expression);
        try writer.print(",\"value\":{f}}}", .{std.json.fmt(value, .{})});
        wrote = true;
    }
    try writer.writeByte(']');
    return try out.toOwnedSlice();
}

pub fn conflictColumnIdentityAlloc(
    alloc: std.mem.Allocator,
    label: []const u8,
    target_columns: []const []const u8,
    insert_columns: []const []const u8,
    row: []const []const u8,
) ![]const u8 {
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.print("{f}:[", .{std.json.fmt(label, .{})});
    for (target_columns, 0..) |target_column, i| {
        const value_json = lower_expr.conflictInsertedValueForColumn(insert_columns, row, target_column) orelse return error.UnsupportedSqlShape;
        if (i != 0) try writer.writeByte(',');
        try writer.print("{f}:", .{std.json.fmt(target_column, .{})});
        try writer.writeAll(value_json);
    }
    try writer.writeByte(']');
    return try out.toOwnedSlice();
}

pub fn conflictExpressionValueAlloc(
    alloc: std.mem.Allocator,
    expression: runtime_schema.UniqueExpression,
    insert_columns: []const []const u8,
    row: []const []const u8,
) ![]const u8 {
    if (expression.op == .expression) {
        const row_expression = expression.expression orelse return error.UnsupportedSqlShape;
        var row_object: std.Io.Writer.Allocating = .init(alloc);
        errdefer row_object.deinit();
        const writer = &row_object.writer;
        try writer.writeByte('{');
        var wrote = false;
        for (insert_columns, row) |column, value_json| {
            if (column.len == 0) return error.UnsupportedSqlShape;
            if (wrote) try writer.writeByte(',');
            try writer.print("{f}:", .{std.json.fmt(column, .{})});
            try writer.writeAll(value_json);
            wrote = true;
        }
        try writer.writeByte('}');
        const row_json = try row_object.toOwnedSlice();
        defer alloc.free(row_json);
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, row_json, .{}) catch return error.UnsupportedSqlShape;
        defer parsed.deinit();
        return try relational_rows.expressionValueJsonAlloc(alloc, parsed.value, row_expression);
    }
    const value_json = lower_expr.conflictInsertedValueForColumn(insert_columns, row, expression.field) orelse return error.UnsupportedSqlShape;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.UnsupportedSqlShape;
    defer parsed.deinit();
    if (parsed.value != .string) return error.UnsupportedSqlShape;
    return switch (expression.op) {
        .lower => try std.ascii.allocLowerString(alloc, parsed.value.string),
        .upper => try std.ascii.allocUpperString(alloc, parsed.value.string),
        .md5 => try lower_expr.md5HexTextAlloc(alloc, parsed.value.string),
        .expression => unreachable,
    };
}

pub fn writeUniqueExpressionIdentityJson(
    writer: *std.Io.Writer,
    expression: runtime_schema.UniqueExpression,
) !void {
    try writer.writeAll("{\"op\":");
    try writer.print("{f}", .{std.json.fmt(lower_expr.uniqueExpressionOpToken(expression.op), .{})});
    switch (expression.op) {
        .lower, .upper, .md5 => try writer.print(",\"field\":{f}", .{std.json.fmt(expression.field, .{})}),
        .expression => {
            try writer.writeAll(",\"expression\":");
            try lower_expr.writeRowExpressionJson(writer, expression.expression orelse return error.UnsupportedSqlShape);
        },
    }
    try writer.writeByte('}');
}

pub fn conflictExpressionIdentityAlloc(
    alloc: std.mem.Allocator,
    label: []const u8,
    expression: runtime_schema.UniqueExpression,
    insert_columns: []const []const u8,
    row: []const []const u8,
) ![]const u8 {
    const folded = try conflictExpressionValueAlloc(alloc, expression, insert_columns, row);
    defer alloc.free(folded);
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.print("{f}:[{{\"expression\":", .{std.json.fmt(label, .{})});
    try writeUniqueExpressionIdentityJson(writer, expression);
    try writer.print(",\"value\":{f}}}]", .{std.json.fmt(folded, .{})});
    return try out.toOwnedSlice();
}

pub fn conflictExcludedJsonObjectValueAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    source: []const u8,
    insert_columns: []const []const u8,
    insert_values: []const []const u8,
) ![]const u8 {
    _ = binder.relationalColumnForField(schema, source, null) orelse return error.InvalidSqlCatalog;
    const value_json = lower_expr.conflictInsertedValueForColumn(insert_columns, insert_values, source) orelse return error.UnsupportedSqlShape;
    return try alloc.dupe(u8, value_json);
}

pub fn conflictExcludedValueJsonAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    column: runtime_schema.RelationalColumn,
    source: []const u8,
    insert_columns: []const []const u8,
    insert_values: []const []const u8,
) ![]const u8 {
    const source_column = binder.relationalColumnForField(schema, source, null) orelse return error.InvalidSqlCatalog;
    if (source_column.field_type != column.field_type) return error.UnsupportedSqlShape;
    const value_json = lower_expr.conflictInsertedValueForColumn(insert_columns, insert_values, source) orelse return error.UnsupportedSqlShape;
    return try alloc.dupe(u8, value_json);
}

pub fn conflictExcludedArrayElementValueJsonAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    column: runtime_schema.RelationalColumn,
    source: []const u8,
    insert_columns: []const []const u8,
    insert_values: []const []const u8,
) ![]const u8 {
    const item_type = column.array_item_type orelse return error.InvalidSqlCatalog;
    const source_column = binder.relationalColumnForField(schema, source, null) orelse return error.InvalidSqlCatalog;
    if (source_column.field_type != item_type) return error.UnsupportedSqlShape;
    const insert_value = lower_expr.conflictInsertedValueForColumn(insert_columns, insert_values, source) orelse return error.UnsupportedSqlShape;
    try sql_value.validateSqlArrayElementValueJson(alloc, column, insert_value);
    return try alloc.dupe(u8, insert_value);
}

fn parseConflictCoalesceValueJsonAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    column: runtime_schema.RelationalColumn,
    hooks: ConflictCoalesceValueParserHooks,
) ![]const u8 {
    try lower_expr.parseCoalesceFunctionCallStart(tokens, pos);

    var selected: ?[]const u8 = null;
    errdefer if (selected) |value| alloc.free(value);
    var operands: usize = 0;
    while (true) {
        const value_json = try hooks.parse_operand_value_json(hooks.ptr, column, hooks.insert_columns, hooks.insert_values);
        var value_transferred = false;
        defer if (!value_transferred) alloc.free(value_json);
        operands += 1;
        if (selected == null and !std.mem.eql(u8, value_json, "null")) {
            selected = value_json;
            value_transferred = true;
        }
        if (parser.matchToken(tokens, pos, .comma) == null) break;
    }
    if (operands == 0) return error.UnsupportedSqlShape;
    try parser.expectToken(tokens, pos, .rparen);

    if (selected) |value| {
        selected = null;
        return value;
    }
    return try alloc.dupe(u8, "null");
}

fn parseConflictJsonbBuildObjectAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const sql_value.SqlValue,
    hooks: ConflictJsonbBuildObjectParserHooks,
) ![]const u8 {
    try sql_value.parseJsonbBuildObjectFunctionCallStart(tokens, pos);
    if (parser.matchToken(tokens, pos, .rparen) != null) return try alloc.dupe(u8, "{}");

    var seen = std.StringHashMapUnmanaged(void).empty;
    defer seen.deinit(alloc);
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.writeByte('{');
    var first = true;
    while (true) {
        const key = try sql_value.parseJsonbBuildObjectKey(tokens, pos, params);
        const entry = try seen.getOrPut(alloc, key);
        if (entry.found_existing) return error.UnsupportedSqlShape;
        try parser.expectToken(tokens, pos, .comma);
        const value_json = try hooks.parse_value_json(hooks.ptr, hooks.insert_columns, hooks.insert_values);
        defer alloc.free(value_json);
        if (!first) try writer.writeByte(',');
        first = false;
        try writer.print("{f}:", .{std.json.fmt(key, .{})});
        try writer.writeAll(value_json);
        if (parser.matchToken(tokens, pos, .comma) == null) break;
    }
    try parser.expectToken(tokens, pos, .rparen);
    try writer.writeByte('}');
    return try out.toOwnedSlice();
}

pub fn parseConflictValueJsonAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    params: []const sql_value.SqlValue,
    column: runtime_schema.RelationalColumn,
    hooks: ConflictValueParserHooks,
) ![]const u8 {
    if (sql_value.peekJsonbBuildObjectFunctionCall(tokens, pos.*)) {
        if (column.field_type != .json) return error.InvalidSqlCatalog;
        return try parseConflictJsonbBuildObjectAlloc(alloc, tokens, pos, params, .{
            .ptr = hooks.ptr,
            .insert_columns = hooks.insert_columns,
            .insert_values = hooks.insert_values,
            .parse_value_json = hooks.parse_jsonb_build_object_value_json,
        });
    }
    if (lower_expr.peekCoalesceFunctionCall(tokens, pos.*)) {
        return try parseConflictCoalesceValueJsonAlloc(alloc, tokens, pos, column, .{
            .ptr = hooks.ptr,
            .insert_columns = hooks.insert_columns,
            .insert_values = hooks.insert_values,
            .parse_operand_value_json = hooks.parse_coalesce_operand_value_json,
        });
    }
    if (pos.* < tokens.len and tokens[pos.*].kind == .identifier) {
        const token = tokens[pos.*];
        if (std.mem.startsWith(u8, token.text, "excluded.")) {
            pos.* += 1;
            return try conflictExcludedValueJsonAlloc(alloc, schema, column, token.text["excluded.".len..], hooks.insert_columns, hooks.insert_values);
        }
    }
    return try hooks.parse_column_value_json(hooks.ptr, column);
}

pub fn parseConflictArrayElementValueJsonAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    column: runtime_schema.RelationalColumn,
    hooks: ConflictValueParserHooks,
) ![]const u8 {
    const item_type = column.array_item_type orelse return error.InvalidSqlCatalog;
    const element_column: runtime_schema.RelationalColumn = .{
        .name = column.name,
        .path = column.path,
        .field_type = item_type,
    };
    if (lower_expr.peekCoalesceFunctionCall(tokens, pos.*)) {
        const value_json = try parseConflictCoalesceValueJsonAlloc(alloc, tokens, pos, element_column, .{
            .ptr = hooks.ptr,
            .insert_columns = hooks.insert_columns,
            .insert_values = hooks.insert_values,
            .parse_operand_value_json = hooks.parse_coalesce_operand_value_json,
        });
        errdefer alloc.free(value_json);
        try sql_value.validateSqlArrayElementValueJson(alloc, column, value_json);
        return value_json;
    }
    if (pos.* < tokens.len and tokens[pos.*].kind == .identifier) {
        const token = tokens[pos.*];
        if (std.mem.startsWith(u8, token.text, "excluded.")) {
            pos.* += 1;
            return try conflictExcludedArrayElementValueJsonAlloc(alloc, schema, column, token.text["excluded.".len..], hooks.insert_columns, hooks.insert_values);
        }
    }
    const value_json = try hooks.parse_json_value_json(hooks.ptr);
    errdefer alloc.free(value_json);
    try sql_value.validateSqlArrayElementValueJson(alloc, column, value_json);
    return value_json;
}

pub fn parseJsonSetSqlValueAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    column: runtime_schema.RelationalColumn,
    hooks: JsonSetSqlValueParserHooks,
) anyerror!JsonSetParsedValue {
    if (!sql_value.peekToJsonbFunctionCall(tokens, pos.*)) {
        return .{ .value_json = try hooks.parse_json_value(hooks.ptr) };
    }

    try sql_value.parseToJsonbFunctionCallStart(tokens, pos);
    if (grammar.peekStaticToJsonbValue(tokens, pos.*)) {
        const value_json = try hooks.parse_json_value(hooks.ptr);
        errdefer alloc.free(value_json);
        try parser.expectToken(tokens, pos, .rparen);
        return .{ .value_json = value_json };
    }

    const expression = try hooks.parse_expression(hooks.ptr, column, hooks.insert_columns);
    var expression_transferred = false;
    errdefer if (!expression_transferred) freeExpression(alloc, expression);
    try parser.expectToken(tokens, pos, .rparen);
    expression_transferred = true;
    return .{ .expression = expression };
}

pub fn parseJoinedMutationJsonSetSqlValueAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    column: runtime_schema.RelationalColumn,
    target_alias: []const u8,
    hooks: JoinedMutationJsonSetSqlValueParserHooks,
) anyerror!JsonSetParsedValue {
    if (!sql_value.peekToJsonbFunctionCall(tokens, pos.*)) {
        return .{ .value_json = try hooks.parse_json_value(hooks.ptr) };
    }

    try sql_value.parseToJsonbFunctionCallStart(tokens, pos);
    if (grammar.peekStaticToJsonbValue(tokens, pos.*)) {
        const value_json = try hooks.parse_json_value(hooks.ptr);
        errdefer alloc.free(value_json);
        try parser.expectToken(tokens, pos, .rparen);
        return .{ .value_json = value_json };
    }

    const expression = try hooks.parse_expression(hooks.ptr, target_alias);
    var expression_transferred = false;
    errdefer if (!expression_transferred) freeExpression(alloc, expression);
    _ = column;
    try parser.expectToken(tokens, pos, .rparen);
    expression_transferred = true;
    return .{ .expression = expression };
}

pub fn parseConflictIncrementAssignmentAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    field: []const u8,
    column: runtime_schema.RelationalColumn,
    increment: *std.ArrayListUnmanaged(FieldJsonValue),
    increment_expr: *std.ArrayListUnmanaged(FieldExpressionValue),
    hooks: ConflictIncrementParserHooks,
) !void {
    if (column.field_type != .numeric) return error.InvalidSqlCatalog;
    const source = try grammar.parseIdentifierOwnedAlloc(alloc, tokens, pos);
    defer alloc.free(source);
    if (!std.mem.eql(u8, source, field)) return error.UnsupportedSqlShape;
    const negated = if (parser.matchToken(tokens, pos, .plus) != null)
        false
    else if (parser.matchToken(tokens, pos, .minus) != null)
        true
    else
        return error.UnsupportedSqlShape;
    if (lower_expr.peekCoalesceFunctionCall(tokens, pos.*)) {
        const expression = try hooks.parse_coalesce_expression(hooks.ptr, column, hooks.insert_columns);
        var expression_transferred = false;
        errdefer if (!expression_transferred) freeExpression(alloc, expression);
        const owned_field = try alloc.dupe(u8, field);
        var field_transferred = false;
        errdefer if (!field_transferred) alloc.free(owned_field);
        const final_expression = if (negated) try lower_expr.buildUnaryNegativeExpressionAlloc(alloc, expression) else expression;
        var final_expression_transferred = !negated;
        errdefer if (!final_expression_transferred) freeExpression(alloc, final_expression);
        if (negated) expression_transferred = true;
        try increment_expr.append(alloc, .{
            .field = owned_field,
            .expression = final_expression,
        });
        field_transferred = true;
        final_expression_transferred = true;
        return;
    }

    const raw_value_json = try hooks.parse_value_json(hooks.ptr, column, hooks.insert_columns, hooks.insert_values);
    defer alloc.free(raw_value_json);
    const value_json = try normalizedIncrementJsonAlloc(alloc, raw_value_json, negated);
    var value_transferred = false;
    errdefer if (!value_transferred) alloc.free(value_json);
    const owned_field = try alloc.dupe(u8, field);
    var field_transferred = false;
    errdefer if (!field_transferred) alloc.free(owned_field);
    try increment.append(alloc, .{
        .field = owned_field,
        .value_json = value_json,
    });
    field_transferred = true;
    value_transferred = true;
}

pub fn parseIncrementAssignmentAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    field: []const u8,
    column: runtime_schema.RelationalColumn,
    increment: *std.ArrayListUnmanaged(FieldJsonValue),
    hooks: IncrementParserHooks,
) !void {
    if (column.field_type != .numeric) return error.InvalidSqlCatalog;
    const source = try grammar.parseIdentifierOwnedAlloc(alloc, tokens, pos);
    defer alloc.free(source);
    if (!std.mem.eql(u8, source, field)) return error.UnsupportedSqlShape;
    const negated = if (parser.matchToken(tokens, pos, .plus) != null)
        false
    else if (parser.matchToken(tokens, pos, .minus) != null)
        true
    else
        return error.UnsupportedSqlShape;
    const raw_value_json = try hooks.parse_value_json(hooks.ptr, column);
    defer alloc.free(raw_value_json);
    const value_json = try normalizedIncrementJsonAlloc(alloc, raw_value_json, negated);
    var value_transferred = false;
    errdefer if (!value_transferred) alloc.free(value_json);
    const owned_field = try alloc.dupe(u8, field);
    var field_transferred = false;
    errdefer if (!field_transferred) alloc.free(owned_field);
    try increment.append(alloc, .{
        .field = owned_field,
        .value_json = value_json,
    });
    field_transferred = true;
    value_transferred = true;
}

pub fn parseConflictUpdateAssignmentValueAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    params: []const sql_value.SqlValue,
    conflict_existing_qualifiers: []const []const u8,
    field: []const u8,
    column: runtime_schema.RelationalColumn,
    patch: *std.ArrayListUnmanaged(FieldJsonValue),
    patch_expr: *std.ArrayListUnmanaged(FieldExpressionValue),
    increment: *std.ArrayListUnmanaged(FieldJsonValue),
    increment_expr: *std.ArrayListUnmanaged(FieldExpressionValue),
    json_set: *std.ArrayListUnmanaged(JsonSetValue),
    array_update: *std.ArrayListUnmanaged(ArrayTransformValue),
    field_transferred: *bool,
    hooks: ConflictUpdateAssignmentValueParserHooks,
) !void {
    if (parser.matchKeyword(tokens, pos, "jsonb_set")) {
        if (column.field_type != .json) return error.InvalidSqlCatalog;
        try parser.expectToken(tokens, pos, .lparen);
        const json_field = try grammar.parseIdentifierOwnedAlloc(alloc, tokens, pos);
        defer alloc.free(json_field);
        if (!std.mem.eql(u8, json_field, field)) return error.UnsupportedSqlShape;
        try parser.expectToken(tokens, pos, .comma);
        const path = try sql_value.parsePostgresJsonPathAlloc(alloc, tokens, pos, params);
        var path_transferred = false;
        errdefer if (!path_transferred) strings.freeStringSlice(alloc, path);
        try parser.expectToken(tokens, pos, .comma);
        const value = try hooks.parse_json_set_value(hooks.ptr, column, hooks.insert_columns);
        var value_transferred = false;
        errdefer if (!value_transferred) freeJsonSetParsedValue(alloc, value);
        if (parser.matchToken(tokens, pos, .comma) != null) {
            if (!parser.matchKeyword(tokens, pos, "true") and !parser.matchKeyword(tokens, pos, "false")) return error.UnsupportedSqlShape;
        }
        try parser.expectToken(tokens, pos, .rparen);
        try json_set.append(alloc, .{
            .field = field,
            .path = path,
            .value_json = value.value_json,
            .expression = value.expression,
        });
        field_transferred.* = true;
        path_transferred = true;
        value_transferred = true;
        return;
    }

    if (grammar.peekArrayTransformSelfAssignment(tokens, pos.*, field)) {
        const op = grammar.matchArrayTransformUpdateOp(tokens, pos) orelse unreachable;
        if (column.field_type != .array) return error.InvalidSqlCatalog;
        try parser.expectToken(tokens, pos, .lparen);
        const array_field = try grammar.parseIdentifierOwnedAlloc(alloc, tokens, pos);
        defer alloc.free(array_field);
        if (!std.mem.eql(u8, array_field, field)) return error.UnsupportedSqlShape;
        try parser.expectToken(tokens, pos, .comma);
        const value_json = try hooks.parse_array_element_value_json(hooks.ptr, column, hooks.insert_columns, hooks.insert_values);
        var value_transferred = false;
        errdefer if (!value_transferred) alloc.free(value_json);
        try parser.expectToken(tokens, pos, .rparen);
        try array_update.append(alloc, .{
            .field = field,
            .op = op,
            .value_json = value_json,
        });
        field_transferred.* = true;
        value_transferred = true;
        return;
    }

    if (column.field_type == .json and pos.* + 1 < tokens.len and tokens[pos.*].kind == .identifier and tokens[pos.* + 1].kind == .pipe_concat) {
        const json_field_token = tokens[pos.*];
        pos.* += 1;
        if (!std.mem.eql(u8, json_field_token.text, field)) return error.UnsupportedSqlShape;
        try parser.expectToken(tokens, pos, .pipe_concat);
        const object_json = try hooks.parse_json_document_value(hooks.ptr);
        defer alloc.free(object_json);
        try appendJsonObjectConcatSetValuesAlloc(alloc, field, object_json, json_set);
        return;
    }

    if (lower_expr.peekConflictExistingFieldIncrement(tokens, pos.*, field, column)) {
        try parseConflictIncrementAssignmentAlloc(alloc, tokens, pos, field, column, increment, increment_expr, .{
            .ptr = hooks.ptr,
            .insert_columns = hooks.insert_columns,
            .insert_values = hooks.insert_values,
            .parse_coalesce_expression = hooks.parse_coalesce_expression,
            .parse_value_json = hooks.parse_value_json,
        });
        return;
    }

    if (column.field_type == .boolean and canParseConflictBooleanAssignmentExpression(alloc, tokens, pos.*, schema, conflict_existing_qualifiers, hooks.insert_columns)) {
        const expression = try hooks.parse_boolean_expression(hooks.ptr, column, hooks.insert_columns);
        var expression_transferred = false;
        errdefer if (!expression_transferred) freeExpression(alloc, expression);
        try patch_expr.append(alloc, .{
            .field = field,
            .expression = expression,
        });
        field_transferred.* = true;
        expression_transferred = true;
        return;
    }

    if (canParseConflictAssignmentExpression(alloc, tokens, pos.*, schema, conflict_existing_qualifiers, hooks.insert_columns)) {
        const expression = try hooks.parse_assignment_expression(hooks.ptr, column, hooks.insert_columns);
        var expression_transferred = false;
        errdefer if (!expression_transferred) freeExpression(alloc, expression);
        try patch_expr.append(alloc, .{
            .field = field,
            .expression = expression,
        });
        field_transferred.* = true;
        expression_transferred = true;
        return;
    }

    const value_json = try hooks.parse_value_json(hooks.ptr, column, hooks.insert_columns, hooks.insert_values);
    var value_transferred = false;
    errdefer if (!value_transferred) alloc.free(value_json);
    try patch.append(alloc, .{
        .field = field,
        .value_json = value_json,
    });
    field_transferred.* = true;
    value_transferred = true;
}

pub fn parseJoinedMutationAssignmentValueAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    joined_source_schema: ?runtime_schema.TableSchema,
    params: []const sql_value.SqlValue,
    target: []const u8,
    target_column: runtime_schema.RelationalColumn,
    target_alias: []const u8,
    pending_joined_source_alias: ?[]const u8,
    source_assignments: *std.ArrayListUnmanaged(JoinedMutationSourceAssignment),
    patch: *std.ArrayListUnmanaged(FieldJsonValue),
    patch_expr: *std.ArrayListUnmanaged(FieldExpressionValue),
    json_set: *std.ArrayListUnmanaged(JsonSetValue),
    target_transferred: *bool,
    hooks: JoinedMutationAssignmentValueParserHooks,
) !void {
    if (parser.matchKeyword(tokens, pos, "jsonb_set")) {
        if (target_column.field_type != .json) return error.InvalidSqlCatalog;
        try parser.expectToken(tokens, pos, .lparen);
        const json_field = try grammar.parseIdentifierOwnedAlloc(alloc, tokens, pos);
        defer alloc.free(json_field);
        if (!(try binder.joinedMutationTargetFieldMatches(alloc, json_field, target_alias, target))) return error.UnsupportedSqlShape;
        try parser.expectToken(tokens, pos, .comma);
        const path = try sql_value.parsePostgresJsonPathAlloc(alloc, tokens, pos, params);
        var path_transferred = false;
        errdefer if (!path_transferred) strings.freeStringSlice(alloc, path);
        try parser.expectToken(tokens, pos, .comma);
        const value = try hooks.parse_json_set_value(hooks.ptr, target_column, target_alias);
        var value_transferred = false;
        errdefer if (!value_transferred) freeJsonSetParsedValue(alloc, value);
        if (parser.matchToken(tokens, pos, .comma) != null) {
            if (!parser.matchKeyword(tokens, pos, "true") and !parser.matchKeyword(tokens, pos, "false")) return error.UnsupportedSqlShape;
        }
        try parser.expectToken(tokens, pos, .rparen);
        try json_set.append(alloc, .{
            .field = target,
            .path = path,
            .value_json = value.value_json,
            .expression = value.expression,
        });
        target_transferred.* = true;
        path_transferred = true;
        value_transferred = true;
        return;
    }

    if (target_column.field_type == .json and pos.* + 1 < tokens.len and tokens[pos.*].kind == .identifier and tokens[pos.* + 1].kind == .pipe_concat) {
        const json_field = try grammar.parseIdentifierOwnedAlloc(alloc, tokens, pos);
        defer alloc.free(json_field);
        if (!(try binder.joinedMutationTargetFieldMatches(alloc, json_field, target_alias, target))) return error.UnsupportedSqlShape;
        try parser.expectToken(tokens, pos, .pipe_concat);
        const object_json = try hooks.parse_json_document_value(hooks.ptr);
        defer alloc.free(object_json);
        try appendJsonObjectConcatSetValuesAlloc(alloc, target, object_json, json_set);
        return;
    }

    if (target_column.field_type == .boolean and canParseJoinedBooleanAssignmentExpression(tokens, pos.*, schema, joined_source_schema, pending_joined_source_alias, target_alias)) {
        const expression = try hooks.parse_boolean_expression(hooks.ptr, target_alias);
        var expression_transferred = false;
        errdefer if (!expression_transferred) freeExpression(alloc, expression);
        try patch_expr.append(alloc, .{
            .field = target,
            .expression = expression,
        });
        target_transferred.* = true;
        expression_transferred = true;
        return;
    }

    if (canParseJoinedAssignmentExpression(tokens, pos.*, schema, joined_source_schema, target_alias)) {
        const expression = try hooks.parse_assignment_expression(hooks.ptr, target_column, target_alias);
        var expression_transferred = false;
        errdefer if (!expression_transferred) freeExpression(alloc, expression);
        try patch_expr.append(alloc, .{
            .field = target,
            .expression = expression,
        });
        target_transferred.* = true;
        expression_transferred = true;
        return;
    }

    if (pos.* < tokens.len and tokens[pos.*].kind == .identifier and lower_expr.identifierContainsQualifier(tokens[pos.*].text)) {
        const source = try plan_mod.parseQualifiedFieldAlloc(alloc, tokens, pos);
        defer plan_mod.freeQualifiedField(alloc, source);
        if (std.mem.eql(u8, source.qualifier, target_alias)) return error.UnsupportedSqlShape;
        const source_column = binder.relationalColumnForField(joined_source_schema orelse schema, source.field, null) orelse return error.InvalidSqlCatalog;
        if (source_column.field_type != target_column.field_type) return error.UnsupportedSqlShape;
        const field = try alloc.dupe(u8, target);
        var field_transferred = false;
        errdefer if (!field_transferred) alloc.free(field);
        const source_qualifier = try alloc.dupe(u8, source.qualifier);
        var source_qualifier_transferred = false;
        errdefer if (!source_qualifier_transferred) alloc.free(source_qualifier);
        const source_field = try alloc.dupe(u8, source.field);
        var source_field_transferred = false;
        errdefer if (!source_field_transferred) alloc.free(source_field);
        try source_assignments.append(alloc, .{
            .field = field,
            .source_qualifier = source_qualifier,
            .source_field = source_field,
        });
        field_transferred = true;
        source_qualifier_transferred = true;
        source_field_transferred = true;
        return;
    }

    const value_json = try hooks.parse_column_value_json(hooks.ptr, target_column);
    var value_transferred = false;
    errdefer if (!value_transferred) alloc.free(value_json);
    try patch.append(alloc, .{
        .field = target,
        .value_json = value_json,
    });
    target_transferred.* = true;
    value_transferred = true;
}

pub fn parseJoinedMutationSetAssignmentAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    joined_source_schema: ?runtime_schema.TableSchema,
    params: []const sql_value.SqlValue,
    target_alias: []const u8,
    pending_joined_source_alias: ?[]const u8,
    source_assignments: *std.ArrayListUnmanaged(JoinedMutationSourceAssignment),
    patch: *std.ArrayListUnmanaged(FieldJsonValue),
    patch_expr: *std.ArrayListUnmanaged(FieldExpressionValue),
    json_set: *std.ArrayListUnmanaged(JsonSetValue),
    saw_primary_key_assignment: *bool,
    hooks: JoinedMutationAssignmentValueParserHooks,
) !void {
    if (parser.peekKind(tokens, pos.*, .lparen)) {
        return try parseJoinedMutationRowAssignmentAlloc(
            alloc,
            tokens,
            pos,
            schema,
            joined_source_schema,
            params,
            target_alias,
            pending_joined_source_alias,
            source_assignments,
            patch,
            patch_expr,
            json_set,
            saw_primary_key_assignment,
            hooks,
        );
    }
    return try parseJoinedMutationScalarAssignmentAlloc(
        alloc,
        tokens,
        pos,
        schema,
        joined_source_schema,
        params,
        target_alias,
        pending_joined_source_alias,
        source_assignments,
        patch,
        patch_expr,
        json_set,
        saw_primary_key_assignment,
        hooks,
    );
}

fn parseJoinedMutationRowAssignmentAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    joined_source_schema: ?runtime_schema.TableSchema,
    params: []const sql_value.SqlValue,
    target_alias: []const u8,
    pending_joined_source_alias: ?[]const u8,
    source_assignments: *std.ArrayListUnmanaged(JoinedMutationSourceAssignment),
    patch: *std.ArrayListUnmanaged(FieldJsonValue),
    patch_expr: *std.ArrayListUnmanaged(FieldExpressionValue),
    json_set: *std.ArrayListUnmanaged(JsonSetValue),
    saw_primary_key_assignment: *bool,
    hooks: JoinedMutationAssignmentValueParserHooks,
) !void {
    try parser.expectToken(tokens, pos, .lparen);
    var targets = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (targets.items) |target| {
            if (target.len != 0) alloc.free(target);
        }
        targets.deinit(alloc);
    }
    while (true) {
        const target = try parseJoinedMutationTargetFieldAlloc(alloc, tokens, pos, target_alias);
        var target_transferred = false;
        errdefer if (!target_transferred) alloc.free(target);
        try targets.append(alloc, target);
        target_transferred = true;
        if (parser.matchToken(tokens, pos, .comma) == null) break;
    }
    if (targets.items.len == 0) return error.UnsupportedSqlShape;
    try grammar.validateSqlIdentifierListUnique(targets.items);
    try parser.expectToken(tokens, pos, .rparen);
    try parser.expectToken(tokens, pos, .eq);
    _ = parser.matchKeyword(tokens, pos, "row");
    try parser.expectToken(tokens, pos, .lparen);

    const primary_key = schema.primary_key orelse return error.InvalidSqlCatalog;
    for (targets.items, 0..) |target, i| {
        const target_column = binder.relationalColumnForField(schema, target, null) orelse return error.InvalidSqlCatalog;
        if (binder.primaryKeyContains(primary_key, target)) saw_primary_key_assignment.* = true;
        var target_transferred = false;
        try parseJoinedMutationAssignmentValueAlloc(
            alloc,
            tokens,
            pos,
            schema,
            joined_source_schema,
            params,
            target,
            target_column,
            target_alias,
            pending_joined_source_alias,
            source_assignments,
            patch,
            patch_expr,
            json_set,
            &target_transferred,
            hooks,
        );
        if (target_transferred) targets.items[i] = "";
        if (i + 1 < targets.items.len) {
            try parser.expectToken(tokens, pos, .comma);
        }
    }
    try parser.expectToken(tokens, pos, .rparen);
}

fn parseJoinedMutationScalarAssignmentAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    joined_source_schema: ?runtime_schema.TableSchema,
    params: []const sql_value.SqlValue,
    target_alias: []const u8,
    pending_joined_source_alias: ?[]const u8,
    source_assignments: *std.ArrayListUnmanaged(JoinedMutationSourceAssignment),
    patch: *std.ArrayListUnmanaged(FieldJsonValue),
    patch_expr: *std.ArrayListUnmanaged(FieldExpressionValue),
    json_set: *std.ArrayListUnmanaged(JsonSetValue),
    saw_primary_key_assignment: *bool,
    hooks: JoinedMutationAssignmentValueParserHooks,
) !void {
    const target = try parseJoinedMutationTargetFieldAlloc(alloc, tokens, pos, target_alias);
    var target_transferred = false;
    defer if (!target_transferred) alloc.free(target);
    const target_column = binder.relationalColumnForField(schema, target, null) orelse return error.InvalidSqlCatalog;
    const primary_key = schema.primary_key orelse return error.InvalidSqlCatalog;
    if (binder.primaryKeyContains(primary_key, target)) saw_primary_key_assignment.* = true;
    try parser.expectToken(tokens, pos, .eq);
    try parseJoinedMutationAssignmentValueAlloc(
        alloc,
        tokens,
        pos,
        schema,
        joined_source_schema,
        params,
        target,
        target_column,
        target_alias,
        pending_joined_source_alias,
        source_assignments,
        patch,
        patch_expr,
        json_set,
        &target_transferred,
        hooks,
    );
}

pub fn parseMergeTargetFieldOwnedAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    target_table: TableAlias,
) ![]const u8 {
    const identifier = try grammar.parseIdentifierOwnedAlloc(alloc, tokens, pos);
    errdefer alloc.free(identifier);
    if (std.mem.indexOfScalar(u8, identifier, '.')) |dot| {
        if (dot == 0 or dot + 1 >= identifier.len) return error.UnsupportedSqlShape;
        const qualifier = identifier[0..dot];
        if (!grammar.rowClaimTargetAllowed(alloc, qualifier, &.{ target_table.name, target_table.alias })) return error.UnsupportedSqlShape;
        const field = try alloc.dupe(u8, identifier[dot + 1 ..]);
        alloc.free(identifier);
        return field;
    }
    return identifier;
}

pub fn parseMergeQualifiedSourceMappingAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    joined_source_schema: ?runtime_schema.TableSchema,
    target_table: TableAlias,
    source_table: TableAlias,
) !MergeFieldMapping {
    const lhs = try plan_mod.parseQualifiedFieldAlloc(alloc, tokens, pos);
    defer plan_mod.freeQualifiedField(alloc, lhs);
    try parser.expectToken(tokens, pos, .eq);
    const rhs = try plan_mod.parseQualifiedFieldAlloc(alloc, tokens, pos);
    defer plan_mod.freeQualifiedField(alloc, rhs);
    const lhs_is_target = grammar.rowClaimTargetAllowed(alloc, lhs.qualifier, &.{ target_table.name, target_table.alias });
    const lhs_is_source = grammar.rowClaimTargetAllowed(alloc, lhs.qualifier, &.{ source_table.name, source_table.alias });
    const rhs_is_target = grammar.rowClaimTargetAllowed(alloc, rhs.qualifier, &.{ target_table.name, target_table.alias });
    const rhs_is_source = grammar.rowClaimTargetAllowed(alloc, rhs.qualifier, &.{ source_table.name, source_table.alias });
    if (lhs_is_target and rhs_is_source) {
        try binder.validateMergeFields(schema, joined_source_schema, lhs.field, rhs.field);
        const target_field = try alloc.dupe(u8, lhs.field);
        var target_transferred = false;
        errdefer if (!target_transferred) alloc.free(target_field);
        const source_field = try alloc.dupe(u8, rhs.field);
        var source_transferred = false;
        errdefer if (!source_transferred) alloc.free(source_field);
        target_transferred = true;
        source_transferred = true;
        return .{
            .target_field = target_field,
            .source_field = source_field,
        };
    }
    if (lhs_is_source and rhs_is_target) {
        try binder.validateMergeFields(schema, joined_source_schema, rhs.field, lhs.field);
        const target_field = try alloc.dupe(u8, rhs.field);
        var target_transferred = false;
        errdefer if (!target_transferred) alloc.free(target_field);
        const source_field = try alloc.dupe(u8, lhs.field);
        var source_transferred = false;
        errdefer if (!source_transferred) alloc.free(source_field);
        target_transferred = true;
        source_transferred = true;
        return .{
            .target_field = target_field,
            .source_field = source_field,
        };
    }
    return error.UnsupportedSqlShape;
}

pub fn parseMergeArmPredicateAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    joined_source_schema: ?runtime_schema.TableSchema,
    target_table: TableAlias,
    source_table: TableAlias,
    allow_target: bool,
    hooks: MergeArmPredicateParserHooks,
) !MergeArmPredicate {
    const lhs = try plan_mod.parseQualifiedFieldAlloc(alloc, tokens, pos);
    defer plan_mod.freeQualifiedField(alloc, lhs);
    const lhs_is_target = grammar.rowClaimTargetAllowed(alloc, lhs.qualifier, &.{ target_table.name, target_table.alias });
    const lhs_is_source = grammar.rowClaimTargetAllowed(alloc, lhs.qualifier, &.{ source_table.name, source_table.alias });
    if (lhs_is_target == lhs_is_source) return error.UnsupportedSqlShape;
    const side: MergePredicateSide = if (lhs_is_target) .target else .source;
    if (side == .target and !allow_target) return error.UnsupportedSqlShape;

    const column = switch (side) {
        .target => binder.relationalColumnForField(schema, lhs.field, null) orelse return error.InvalidSqlCatalog,
        .source => blk: {
            const source_schema = joined_source_schema orelse schema;
            break :blk binder.relationalColumnForField(source_schema, lhs.field, null) orelse return error.InvalidSqlCatalog;
        },
    };

    const op: runtime_schema.RelationalCheckOp = if (try lower_expr.parseExpressionIsTailIf(tokens, pos, .{
        .allow_distinct = false,
        .allow_boolean_unknown = true,
        .allow_boolean_literal = true,
    })) |is_tail| blk: {
        switch (is_tail.kind) {
            .distinct_comparison => unreachable,
            .null_test => {},
            .boolean_unknown => {
                if (column.field_type != .boolean) return error.InvalidSqlCatalog;
                break :blk is_tail.op;
            },
            .boolean_literal => {
                if (column.field_type != .boolean) return error.InvalidSqlCatalog;
                const value_json = try alloc.dupe(u8, sql_value.booleanJson(is_tail.boolean_value));
                var value_transferred = false;
                errdefer if (!value_transferred) alloc.free(value_json);
                const field = try alloc.dupe(u8, lhs.field);
                var field_transferred = false;
                errdefer if (!field_transferred) alloc.free(field);
                field_transferred = true;
                value_transferred = true;
                return MergeArmPredicate{
                    .side = side,
                    .field = field,
                    .op = .eq,
                    .value_json = value_json,
                };
            },
        }
        break :blk is_tail.op;
    } else try lower_expr.parseComparisonOp(tokens, pos);
    const value_json = if (op == .is_null or op == .is_not_null)
        null
    else
        try hooks.parse_column_value_json(hooks.ptr, column);
    var value_transferred = false;
    errdefer if (!value_transferred) if (value_json) |value| alloc.free(value);
    const field = try alloc.dupe(u8, lhs.field);
    var field_transferred = false;
    errdefer if (!field_transferred) alloc.free(field);
    field_transferred = true;
    value_transferred = true;
    return MergeArmPredicate{
        .side = side,
        .field = field,
        .op = op,
        .value_json = value_json,
    };
}

pub fn parseMergeAssignmentExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    column: runtime_schema.RelationalColumn,
    target_table: TableAlias,
    source_table: TableAlias,
    type_context: lower_expr.RowExpressionTypeContext,
    hooks: MergeAssignmentExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    if (parser.matchKeyword(tokens, pos, "default")) {
        const default_value = column.default_value orelse return error.UnsupportedSqlShape;
        const value_json = try relational_rows.relationalDefaultValueJsonAlloc(alloc, default_value);
        var value_transferred = false;
        errdefer if (!value_transferred) alloc.free(value_json);
        value_transferred = true;
        return .{
            .kind = .value,
            .value_json = value_json,
        };
    }

    const expression = try hooks.parse_row_expression(hooks.ptr, target_table, source_table);
    var expression_transferred = false;
    errdefer if (!expression_transferred) freeExpression(alloc, expression);
    try validateMutationAssignmentExpressionType(type_context, column, expression, .{ .validate_json = true });
    expression_transferred = true;
    return expression;
}

pub fn parseJoinedMutationAssignmentExpressionAlloc(
    alloc: std.mem.Allocator,
    column: runtime_schema.RelationalColumn,
    target_alias: []const u8,
    type_context: lower_expr.RowExpressionTypeContext,
    hooks: JoinedMutationAssignmentExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    const expression = try hooks.parse_row_expression(hooks.ptr, target_alias);
    var expression_transferred = false;
    errdefer if (!expression_transferred) freeExpression(alloc, expression);
    try validateMutationAssignmentExpressionType(type_context, column, expression, .{});
    expression_transferred = true;
    return expression;
}

pub fn parseJoinedMutationBooleanAssignmentExpressionAlloc(
    alloc: std.mem.Allocator,
    target_alias: []const u8,
    type_context: lower_expr.RowExpressionTypeContext,
    hooks: JoinedMutationAssignmentExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    const expression = try hooks.parse_boolean_row_expression(hooks.ptr, target_alias);
    var expression_transferred = false;
    errdefer if (!expression_transferred) freeExpression(alloc, expression);
    try type_context.validateBooleanRowExpression(expression);
    expression_transferred = true;
    return expression;
}

pub fn parseConflictAssignmentExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    column: runtime_schema.RelationalColumn,
    insert_columns: []const []const u8,
    type_context: lower_expr.RowExpressionTypeContext,
    hooks: ConflictAssignmentExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    var expression = try hooks.parse_expression(hooks.ptr, column, insert_columns);
    var expression_owned = true;
    errdefer if (expression_owned) freeExpression(alloc, expression);
    if (lower_expr.peekArithmeticOperator(tokens, pos.*)) |_| {
        try type_context.validateNumericOrDatetimeRowExpression(expression);
        expression_owned = false;
        expression = try hooks.parse_arithmetic_rest(hooks.ptr, expression, column, insert_columns);
        expression_owned = true;
        try type_context.validateNumericRowExpression(expression);
    }
    if (parser.peekKind(tokens, pos.*, .pipe_concat)) {
        if (column.field_type != .keyword and column.field_type != .text and column.field_type != .link) return error.InvalidSqlCatalog;
        expression_owned = false;
        expression = try hooks.parse_pipe_concat_rest(hooks.ptr, expression, column, insert_columns);
        expression_owned = true;
        try type_context.validateTextRowExpression(expression);
    }
    if (lower_expr.sqlExpressionIsInterval(expression)) return error.UnsupportedSqlShape;
    if (column.field_type == .datetime) {
        try type_context.validateNumericOrDatetimeRowExpression(expression);
    } else if (column.field_type == .numeric) {
        try type_context.validateNumericRowExpression(expression);
    }
    expression_owned = false;
    return expression;
}

pub fn parseConflictPipeConcatExpressionRestAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    left: db_mod.types.RelationalRowsExpression,
    column: runtime_schema.RelationalColumn,
    insert_columns: []const []const u8,
    type_context: lower_expr.RowExpressionTypeContext,
    hooks: ConflictPipeConcatExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    try type_context.validateTextRowExpression(left);
    var operands = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpression).empty;
    errdefer {
        for (operands.items) |operand| freeExpression(alloc, operand);
        operands.deinit(alloc);
    }
    try operands.append(alloc, left);
    while (parser.matchToken(tokens, pos, .pipe_concat) != null) {
        const rhs = try hooks.parse_operand(hooks.ptr, column, insert_columns);
        var rhs_transferred = false;
        errdefer if (!rhs_transferred) freeExpression(alloc, rhs);
        try type_context.validateTextRowExpression(rhs);
        try operands.append(alloc, rhs);
        rhs_transferred = true;
    }
    if (operands.items.len < 2) return error.UnsupportedSqlShape;
    return try lower_expr.buildFunctionExpressionFromOperandListAlloc(alloc, .concat, &operands);
}

pub fn parseConflictArithmeticExpressionRestAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    lhs: db_mod.types.RelationalRowsExpression,
    column: runtime_schema.RelationalColumn,
    insert_columns: []const []const u8,
    min_precedence: u8,
    type_context: lower_expr.RowExpressionTypeContext,
    hooks: ConflictArithmeticExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    var current = lhs;
    var current_owned = true;
    errdefer if (current_owned) freeExpression(alloc, current);

    while (lower_expr.peekArithmeticOperator(tokens, pos.*)) |op| {
        if (op.precedence < min_precedence) break;
        _ = parser.matchToken(tokens, pos, op.token) orelse unreachable;
        if (lower_expr.peekSqlIntervalExpressionSyntax(tokens, pos.*)) {
            try type_context.validateNumericOrDatetimeRowExpression(current);
            const interval = try sql_value.parseSqlIntervalLiteral(tokens, pos);
            const next = try lower_expr.buildIntervalLiteralArithmeticAlloc(alloc, current, op.kind, interval);
            current_owned = false;
            current = next;
            current_owned = true;
            try type_context.validateNumericRowExpression(current);
            continue;
        }

        var rhs = try hooks.parse_expression(hooks.ptr, column, insert_columns);
        var rhs_owned = true;
        errdefer if (rhs_owned) freeExpression(alloc, rhs);

        while (lower_expr.peekArithmeticOperator(tokens, pos.*)) |next_op| {
            if (next_op.precedence <= op.precedence) break;
            rhs_owned = false;
            rhs = try parseConflictArithmeticExpressionRestAlloc(alloc, tokens, pos, rhs, column, insert_columns, next_op.precedence, type_context, hooks);
            rhs_owned = true;
        }

        const expression = try lower_expr.buildBinaryExpressionAlloc(alloc, op.kind, current, rhs);
        current_owned = false;
        rhs_owned = false;
        current = expression;
        current_owned = true;
        try type_context.validateNumericRowExpression(current);
    }

    current_owned = false;
    return current;
}

pub fn parseConflictCoalesceExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    column: runtime_schema.RelationalColumn,
    insert_columns: []const []const u8,
    expected_type: ?runtime_schema.AntflyType,
    hooks: ConflictCoalesceExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    try lower_expr.parseCoalesceFunctionCallStart(tokens, pos);

    var operands = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpression).empty;
    errdefer {
        for (operands.items) |operand| freeExpression(alloc, operand);
        operands.deinit(alloc);
    }
    while (true) {
        const operand = try hooks.parse_operand(hooks.ptr, column, insert_columns, expected_type);
        var operand_transferred = false;
        errdefer if (!operand_transferred) freeExpression(alloc, operand);
        try operands.append(alloc, operand);
        operand_transferred = true;
        if (parser.matchToken(tokens, pos, .comma) == null) break;
    }
    if (operands.items.len == 0) return error.UnsupportedSqlShape;
    try parser.expectToken(tokens, pos, .rparen);

    return try lower_expr.buildFunctionExpressionFromOperandListAlloc(alloc, .coalesce, &operands);
}

pub fn parseConflictNullifExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    column: runtime_schema.RelationalColumn,
    insert_columns: []const []const u8,
    hooks: ConflictNullifExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    try lower_expr.parseNullifFunctionCallStart(tokens, pos);
    const lhs = try hooks.parse_expression(hooks.ptr, column, insert_columns);
    var lhs_transferred = false;
    errdefer if (!lhs_transferred) freeExpression(alloc, lhs);
    try parser.expectToken(tokens, pos, .comma);
    const rhs = try hooks.parse_expression(hooks.ptr, column, insert_columns);
    var rhs_transferred = false;
    errdefer if (!rhs_transferred) freeExpression(alloc, rhs);
    try parser.expectToken(tokens, pos, .rparen);

    const expression = try lower_expr.buildBinaryFunctionExpressionAlloc(alloc, .nullif, lhs, rhs);
    lhs_transferred = true;
    rhs_transferred = true;
    return expression;
}

pub fn parseConflictLengthExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    column: runtime_schema.RelationalColumn,
    insert_columns: []const []const u8,
    expected_type: ?runtime_schema.AntflyType,
    type_context: lower_expr.RowExpressionTypeContext,
    hooks: ConflictUnaryExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    if (expected_type) |field_type| if (field_type != .numeric) return error.UnsupportedSqlShape;
    const kind = try lower_expr.parseTextLengthFunctionCallStart(tokens, pos);
    const operand = try hooks.parse_operand(hooks.ptr, column, insert_columns, null);
    var operand_transferred = false;
    errdefer if (!operand_transferred) freeExpression(alloc, operand);
    try type_context.validateTextRowExpression(operand);
    try parser.expectToken(tokens, pos, .rparen);

    const expression = try lower_expr.buildUnaryFunctionExpressionAlloc(alloc, kind, operand);
    operand_transferred = true;
    return expression;
}

pub fn parseConflictAsciiExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    column: runtime_schema.RelationalColumn,
    insert_columns: []const []const u8,
    expected_type: ?runtime_schema.AntflyType,
    type_context: lower_expr.RowExpressionTypeContext,
    hooks: ConflictUnaryExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    if (expected_type) |field_type| if (field_type != .numeric) return error.UnsupportedSqlShape;
    try lower_expr.parseFixedUnaryFunctionCallStart(tokens, pos, .ascii);
    const operand = try hooks.parse_operand(hooks.ptr, column, insert_columns, null);
    var operand_transferred = false;
    errdefer if (!operand_transferred) freeExpression(alloc, operand);
    try type_context.validateTextRowExpression(operand);
    try parser.expectToken(tokens, pos, .rparen);

    const expression = try lower_expr.buildUnaryFunctionExpressionAlloc(alloc, .ascii, operand);
    operand_transferred = true;
    return expression;
}

pub fn parseConflictChrExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    column: runtime_schema.RelationalColumn,
    insert_columns: []const []const u8,
    expected_type: ?runtime_schema.AntflyType,
    type_context: lower_expr.RowExpressionTypeContext,
    hooks: ConflictUnaryExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    if (expected_type) |field_type| if (!lower_expr.sqlExpressionTypeIsTextLike(field_type)) return error.UnsupportedSqlShape;
    try lower_expr.parseFixedUnaryFunctionCallStart(tokens, pos, .chr);
    const operand = try hooks.parse_operand(hooks.ptr, column, insert_columns, .numeric);
    var operand_transferred = false;
    errdefer if (!operand_transferred) freeExpression(alloc, operand);
    try type_context.validateNumericRowExpression(operand);
    try parser.expectToken(tokens, pos, .rparen);

    const expression = try lower_expr.buildUnaryFunctionExpressionAlloc(alloc, .chr, operand);
    operand_transferred = true;
    return expression;
}

pub fn parseConflictCastExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    column: runtime_schema.RelationalColumn,
    insert_columns: []const []const u8,
    hooks: ConflictUnaryExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    try lower_expr.parseCastExpressionCallStart(tokens, pos);
    const operand = try hooks.parse_operand(hooks.ptr, column, insert_columns, null);
    var operand_transferred = false;
    errdefer if (!operand_transferred) freeExpression(alloc, operand);
    try lower_expr.parseCastExpressionAs(tokens, pos);
    const cast_type = try lower_expr.parseExpressionCastType(tokens, pos);
    try parser.expectToken(tokens, pos, .rparen);
    const expression = try lower_expr.buildCastExpressionAlloc(alloc, operand, cast_type);
    operand_transferred = true;
    return expression;
}

fn conflictBooleanHooksFromConditionHooks(hooks: ConflictExpressionConditionParserHooks) ConflictBooleanExpressionParserHooks {
    return .{
        .ptr = hooks.ptr,
        .parse_operand = hooks.parse_boolean_operand,
    };
}

fn conflictConditionHooksFromCaseHooks(hooks: ConflictCaseExpressionParserHooks) ConflictExpressionConditionParserHooks {
    return .{
        .ptr = hooks.ptr,
        .parse_untyped_expression = hooks.parse_untyped_expression,
        .parse_boolean_operand = hooks.parse_boolean_operand,
        .parse_json_array_value = hooks.parse_json_array_value,
    };
}

pub fn parseConflictCaseExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    conflict_existing_qualifiers: []const []const u8,
    column: runtime_schema.RelationalColumn,
    insert_columns: []const []const u8,
    type_context: lower_expr.RowExpressionTypeContext,
    defer_row_expression_field_validation: bool,
    hooks: ConflictCaseExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    try lower_expr.parseCaseExpressionStart(tokens, pos);

    var branches = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionCaseBranch).empty;
    errdefer {
        for (branches.items) |branch| freeExpressionCaseBranch(alloc, branch);
        branches.deinit(alloc);
    }

    const condition_hooks = conflictConditionHooksFromCaseHooks(hooks);
    while (lower_expr.matchCaseExpressionWhen(tokens, pos)) {
        const condition = try parseConflictExpressionConditionAlloc(alloc, tokens, pos, schema, conflict_existing_qualifiers, column, insert_columns, type_context, defer_row_expression_field_validation, condition_hooks);
        var condition_transferred = false;
        errdefer if (!condition_transferred) freeExpressionCondition(alloc, condition);
        try lower_expr.parseCaseExpressionThen(tokens, pos);
        const then_expression = try hooks.parse_expression(hooks.ptr, column, insert_columns);
        var then_transferred = false;
        errdefer if (!then_transferred) freeExpression(alloc, then_expression);
        try branches.append(alloc, .{ .when = condition, .then = then_expression });
        condition_transferred = true;
        then_transferred = true;
    }
    if (branches.items.len == 0) return error.UnsupportedSqlShape;

    try lower_expr.parseCaseExpressionElse(tokens, pos);
    const else_expression = try hooks.parse_expression(hooks.ptr, column, insert_columns);
    var else_transferred = false;
    errdefer if (!else_transferred) freeExpression(alloc, else_expression);
    try lower_expr.parseCaseExpressionEnd(tokens, pos);

    const owned_branches = try branches.toOwnedSlice(alloc);
    var branches_transferred = false;
    errdefer if (!branches_transferred) {
        for (owned_branches) |branch| freeExpressionCaseBranch(alloc, branch);
        alloc.free(owned_branches);
    };
    const fallback = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
    var fallback_transferred = false;
    errdefer if (!fallback_transferred) alloc.free(fallback);
    fallback[0] = else_expression;

    branches_transferred = true;
    fallback_transferred = true;
    else_transferred = true;
    return .{
        .kind = .case,
        .case_branches = owned_branches,
        .case_else = fallback,
    };
}

pub fn parseConflictExpressionConditionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    conflict_existing_qualifiers: []const []const u8,
    column: runtime_schema.RelationalColumn,
    insert_columns: []const []const u8,
    type_context: lower_expr.RowExpressionTypeContext,
    defer_row_expression_field_validation: bool,
    hooks: ConflictExpressionConditionParserHooks,
) !db_mod.types.RelationalRowsExpressionCondition {
    if (canParseBareBooleanConflictExpression(alloc, tokens, pos.*, schema, conflict_existing_qualifiers, insert_columns)) {
        return try parseBareBooleanConflictExpressionConditionAlloc(alloc, tokens, pos, column, insert_columns, type_context, conflictBooleanHooksFromConditionHooks(hooks));
    }

    const lhs = try hooks.parse_untyped_expression(hooks.ptr, column, insert_columns);
    var lhs_transferred = false;
    errdefer if (!lhs_transferred) freeExpression(alloc, lhs);

    const op: runtime_schema.RelationalCheckOp = if (try lower_expr.parseExpressionIsTailIf(tokens, pos, .{
        .allow_boolean_unknown = true,
        .allow_boolean_literal = true,
    })) |is_tail| blk: {
        switch (is_tail.kind) {
            .distinct_comparison, .null_test => {},
            .boolean_unknown => {
                try type_context.validateBooleanRowExpression(lhs);
                lhs_transferred = true;
                return lower_expr.expressionNullTestCondition(lhs, is_tail.op);
            },
            .boolean_literal => {
                try type_context.validateBooleanRowExpression(lhs);
                const condition = try lower_expr.expressionBooleanComparisonConditionAlloc(alloc, lhs, is_tail.op, is_tail.boolean_value);
                lhs_transferred = true;
                return condition;
            },
        }
        break :blk is_tail.op;
    } else if (lower_expr.matchPostfixNullTest(tokens, pos)) |postfix_null_test|
        postfix_null_test
    else
        try lower_expr.parseComparisonOp(tokens, pos);

    if (op == .eq and lower_expr.matchAnyOrSomeKeyword(tokens, pos)) {
        try parser.expectToken(tokens, pos, .lparen);
        const values_json = try hooks.parse_json_array_value(hooks.ptr);
        defer alloc.free(values_json);
        try parser.expectToken(tokens, pos, .rparen);
        return error.UnsupportedSqlShape;
    }

    const rhs = switch (op) {
        .is_null, .is_not_null => &.{},
        else => blk: {
            const out = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
            var out_transferred = false;
            errdefer if (!out_transferred) alloc.free(out);
            out[0] = try hooks.parse_untyped_expression(hooks.ptr, column, insert_columns);
            out_transferred = true;
            break :blk out;
        },
    };
    var rhs_transferred = false;
    errdefer if (!rhs_transferred and rhs.len > 0) {
        for (rhs) |expression| freeExpression(alloc, expression);
        alloc.free(rhs);
    };
    try lower_expr.validateExpressionConditionTypes(type_context, defer_row_expression_field_validation, lhs, op, rhs);

    lhs_transferred = true;
    rhs_transferred = true;
    return .{
        .lhs = lhs,
        .op = op,
        .rhs = rhs,
    };
}

fn parseBareBooleanConflictExpressionConditionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    column: runtime_schema.RelationalColumn,
    insert_columns: []const []const u8,
    type_context: lower_expr.RowExpressionTypeContext,
    hooks: ConflictBooleanExpressionParserHooks,
) !db_mod.types.RelationalRowsExpressionCondition {
    const expression = try parseConflictBooleanExpressionAlloc(alloc, tokens, pos, column, insert_columns, type_context, hooks);
    var expression_transferred = false;
    errdefer if (!expression_transferred) freeExpression(alloc, expression);
    try type_context.validateBooleanRowExpression(expression);

    const value_json = try alloc.dupe(u8, "true");
    var value_transferred = false;
    errdefer if (!value_transferred) alloc.free(value_json);
    const rhs = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
    var rhs_transferred = false;
    errdefer if (!rhs_transferred) alloc.free(rhs);
    rhs[0] = .{
        .kind = .value,
        .value_json = value_json,
    };

    expression_transferred = true;
    value_transferred = true;
    rhs_transferred = true;
    return .{
        .lhs = expression,
        .op = .eq,
        .rhs = rhs,
    };
}

pub fn parseConflictBooleanExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    column: runtime_schema.RelationalColumn,
    insert_columns: []const []const u8,
    type_context: lower_expr.RowExpressionTypeContext,
    hooks: ConflictBooleanExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    var expression = try parseConflictBooleanOperandAlloc(alloc, tokens, pos, column, insert_columns, type_context, hooks);
    var expression_owned = true;
    errdefer if (expression_owned) freeExpression(alloc, expression);
    if (lower_expr.peekBooleanOperator(tokens, pos.*)) |_| {
        try type_context.validateBooleanRowExpression(expression);
        expression_owned = false;
        expression = try parseConflictBooleanExpressionRestAlloc(alloc, tokens, pos, expression, column, insert_columns, type_context, hooks, 0);
        expression_owned = true;
        try type_context.validateBooleanRowExpression(expression);
    }
    expression_owned = false;
    return expression;
}

fn parseConflictBooleanExpressionRestAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    lhs: db_mod.types.RelationalRowsExpression,
    column: runtime_schema.RelationalColumn,
    insert_columns: []const []const u8,
    type_context: lower_expr.RowExpressionTypeContext,
    hooks: ConflictBooleanExpressionParserHooks,
    min_precedence: u8,
) !db_mod.types.RelationalRowsExpression {
    try type_context.validateBooleanRowExpression(lhs);
    var current = lhs;
    var current_owned = true;
    errdefer if (current_owned) freeExpression(alloc, current);

    while (lower_expr.peekBooleanOperator(tokens, pos.*)) |op| {
        if (op.precedence < min_precedence) break;
        if (!parser.matchKeyword(tokens, pos, op.keyword)) unreachable;
        var rhs = try parseConflictBooleanOperandAlloc(alloc, tokens, pos, column, insert_columns, type_context, hooks);
        var rhs_owned = true;
        errdefer if (rhs_owned) freeExpression(alloc, rhs);
        try type_context.validateBooleanRowExpression(rhs);

        while (lower_expr.peekBooleanOperator(tokens, pos.*)) |next_op| {
            if (next_op.precedence <= op.precedence) break;
            rhs_owned = false;
            rhs = try parseConflictBooleanExpressionRestAlloc(alloc, tokens, pos, rhs, column, insert_columns, type_context, hooks, next_op.precedence);
            rhs_owned = true;
        }

        const expression = try lower_expr.buildBinaryExpressionAlloc(alloc, op.kind, current, rhs);
        current_owned = false;
        rhs_owned = false;
        current = expression;
        current_owned = true;
    }

    current_owned = false;
    return current;
}

fn parseConflictBooleanOperandAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    column: runtime_schema.RelationalColumn,
    insert_columns: []const []const u8,
    type_context: lower_expr.RowExpressionTypeContext,
    hooks: ConflictBooleanExpressionParserHooks,
) anyerror!db_mod.types.RelationalRowsExpression {
    if (lower_expr.peekBooleanNotExpressionSyntax(tokens, pos.*)) return try parseConflictBooleanNotExpressionAlloc(alloc, tokens, pos, column, insert_columns, type_context, hooks);
    if (lower_expr.peekParenthesizedExpressionSyntax(tokens, pos.*)) return try parseParenthesizedConflictBooleanExpressionAlloc(alloc, tokens, pos, column, insert_columns, type_context, hooks);
    return try hooks.parse_operand(hooks.ptr, column, insert_columns, .boolean);
}

fn parseConflictBooleanNotExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    column: runtime_schema.RelationalColumn,
    insert_columns: []const []const u8,
    type_context: lower_expr.RowExpressionTypeContext,
    hooks: ConflictBooleanExpressionParserHooks,
) anyerror!db_mod.types.RelationalRowsExpression {
    try lower_expr.parseBooleanNotExpressionStart(tokens, pos);
    const operand = try parseConflictBooleanOperandAlloc(alloc, tokens, pos, column, insert_columns, type_context, hooks);
    var operand_transferred = false;
    errdefer if (!operand_transferred) freeExpression(alloc, operand);
    try type_context.validateBooleanRowExpression(operand);
    const expression = try lower_expr.wrapBooleanNotExpressionAlloc(alloc, operand);
    operand_transferred = true;
    return expression;
}

fn parseParenthesizedConflictBooleanExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    column: runtime_schema.RelationalColumn,
    insert_columns: []const []const u8,
    type_context: lower_expr.RowExpressionTypeContext,
    hooks: ConflictBooleanExpressionParserHooks,
) anyerror!db_mod.types.RelationalRowsExpression {
    try parser.expectToken(tokens, pos, .lparen);
    const expression = try parseConflictBooleanExpressionAlloc(alloc, tokens, pos, column, insert_columns, type_context, hooks);
    var expression_transferred = false;
    errdefer if (!expression_transferred) freeExpression(alloc, expression);
    try parser.expectToken(tokens, pos, .rparen);
    expression_transferred = true;
    return expression;
}

pub fn parseConflictCaseFoldExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    column: runtime_schema.RelationalColumn,
    insert_columns: []const []const u8,
    expected_type: ?runtime_schema.AntflyType,
    type_context: lower_expr.RowExpressionTypeContext,
    hooks: ConflictCaseFoldExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    if (expected_type) |field_type| if (!lower_expr.sqlExpressionTypeIsTextLike(field_type)) return error.UnsupportedSqlShape;
    const kind = try lower_expr.parseCaseFoldFunctionCallStart(tokens, pos);
    if (try hooks.parse_period_bound(hooks.ptr, kind)) |expression| return expression;

    var operands = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpression).empty;
    errdefer {
        for (operands.items) |operand| freeExpression(alloc, operand);
        operands.deinit(alloc);
    }

    const operand = try hooks.parse_operand(hooks.ptr, column, insert_columns, null);
    var operand_transferred = false;
    errdefer if (!operand_transferred) freeExpression(alloc, operand);
    try type_context.validateTextRowExpression(operand);
    try operands.append(alloc, operand);
    operand_transferred = true;

    if (kind == .trim or kind == .ltrim or kind == .rtrim) {
        if (parser.matchToken(tokens, pos, .comma) != null) {
            const trim_operand = try hooks.parse_operand(hooks.ptr, column, insert_columns, null);
            var trim_transferred = false;
            errdefer if (!trim_transferred) freeExpression(alloc, trim_operand);
            try type_context.validateTextRowExpression(trim_operand);
            try operands.append(alloc, trim_operand);
            trim_transferred = true;
        }
    }
    try parser.expectToken(tokens, pos, .rparen);

    return try lower_expr.buildFunctionExpressionFromOperandListAlloc(alloc, kind, &operands);
}

pub fn parseConflictConcatExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    column: runtime_schema.RelationalColumn,
    insert_columns: []const []const u8,
    expected_type: ?runtime_schema.AntflyType,
    type_context: lower_expr.RowExpressionTypeContext,
    hooks: ConflictUnaryExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    if (expected_type) |field_type| if (!lower_expr.sqlExpressionTypeIsTextLike(field_type)) return error.UnsupportedSqlShape;
    const kind = try lower_expr.parseConcatFunctionCallStart(tokens, pos);

    var operands = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpression).empty;
    errdefer {
        for (operands.items) |operand| freeExpression(alloc, operand);
        operands.deinit(alloc);
    }
    while (true) {
        const operand = try hooks.parse_operand(hooks.ptr, column, insert_columns, null);
        var operand_transferred = false;
        errdefer if (!operand_transferred) freeExpression(alloc, operand);
        try operands.append(alloc, operand);
        operand_transferred = true;
        if (parser.matchToken(tokens, pos, .comma) == null) break;
    }
    if ((kind == .concat and operands.items.len == 0) or (kind == .concat_ws and operands.items.len < 2)) return error.UnsupportedSqlShape;
    if (kind == .concat_ws) {
        for (operands.items) |operand| try type_context.validateTextRowExpression(operand);
    }
    try parser.expectToken(tokens, pos, .rparen);

    return try lower_expr.buildFunctionExpressionFromOperandListAlloc(alloc, kind, &operands);
}

pub fn parseConflictTextUnaryExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    column: runtime_schema.RelationalColumn,
    insert_columns: []const []const u8,
    expected_type: ?runtime_schema.AntflyType,
    kind: db_mod.types.RelationalRowsExpressionKind,
    type_context: lower_expr.RowExpressionTypeContext,
    hooks: ConflictUnaryExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    if (expected_type) |field_type| if (!lower_expr.sqlExpressionTypeIsTextLike(field_type)) return error.UnsupportedSqlShape;
    switch (kind) {
        .reverse, .md5 => try lower_expr.parseFixedUnaryFunctionCallStart(tokens, pos, kind),
        else => return error.UnsupportedSqlShape,
    }
    const operand = try hooks.parse_operand(hooks.ptr, column, insert_columns, null);
    var operand_transferred = false;
    errdefer if (!operand_transferred) freeExpression(alloc, operand);
    try type_context.validateTextRowExpression(operand);
    try parser.expectToken(tokens, pos, .rparen);

    const expression = try lower_expr.buildUnaryFunctionExpressionAlloc(alloc, kind, operand);
    operand_transferred = true;
    return expression;
}

pub fn parseConflictNumericUnaryExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    column: runtime_schema.RelationalColumn,
    insert_columns: []const []const u8,
    kind: db_mod.types.RelationalRowsExpressionKind,
    type_context: lower_expr.RowExpressionTypeContext,
    unary_hooks: ConflictUnaryExpressionParserHooks,
    arithmetic_hooks: ConflictArithmeticExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    try lower_expr.parseFixedUnaryFunctionCallStart(tokens, pos, kind);
    var operand = try unary_hooks.parse_operand(unary_hooks.ptr, column, insert_columns, .numeric);
    var operand_transferred = false;
    errdefer if (!operand_transferred) freeExpression(alloc, operand);
    try type_context.validateNumericRowExpression(operand);
    if (lower_expr.peekArithmeticOperator(tokens, pos.*)) |_| {
        operand_transferred = true;
        operand = try parseConflictArithmeticExpressionRestAlloc(alloc, tokens, pos, operand, column, insert_columns, 0, type_context, arithmetic_hooks);
        operand_transferred = false;
    }
    try parser.expectToken(tokens, pos, .rparen);

    const expression = try lower_expr.buildUnaryFunctionExpressionAlloc(alloc, kind, operand);
    operand_transferred = true;
    return expression;
}

pub fn parseConflictNumericBinaryExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    column: runtime_schema.RelationalColumn,
    insert_columns: []const []const u8,
    kind: db_mod.types.RelationalRowsExpressionKind,
    type_context: lower_expr.RowExpressionTypeContext,
    hooks: ConflictUnaryExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    try lower_expr.parseFixedBinaryFunctionCallStart(tokens, pos, kind);
    const lhs = try hooks.parse_operand(hooks.ptr, column, insert_columns, .numeric);
    var lhs_transferred = false;
    errdefer if (!lhs_transferred) freeExpression(alloc, lhs);
    try type_context.validateNumericRowExpression(lhs);
    try parser.expectToken(tokens, pos, .comma);
    const rhs = try hooks.parse_operand(hooks.ptr, column, insert_columns, .numeric);
    var rhs_transferred = false;
    errdefer if (!rhs_transferred) freeExpression(alloc, rhs);
    try type_context.validateNumericRowExpression(rhs);
    try parser.expectToken(tokens, pos, .rparen);

    const expression = try lower_expr.buildBinaryFunctionExpressionAlloc(alloc, kind, lhs, rhs);
    lhs_transferred = true;
    rhs_transferred = true;
    return expression;
}

pub fn parseConflictTextTernaryExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    column: runtime_schema.RelationalColumn,
    insert_columns: []const []const u8,
    expected_type: ?runtime_schema.AntflyType,
    kind: db_mod.types.RelationalRowsExpressionKind,
    type_context: lower_expr.RowExpressionTypeContext,
    hooks: ConflictUnaryExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    if (expected_type) |field_type| if (!lower_expr.sqlExpressionTypeIsTextLike(field_type)) return error.UnsupportedSqlShape;
    switch (kind) {
        .replace => try lower_expr.parseReplaceFunctionCallStart(tokens, pos),
        .translate => try lower_expr.parseFunctionCallStartIf(tokens, pos, lower_expr.sqlKeywordIsTranslateFunction),
        else => return error.UnsupportedSqlShape,
    }

    const first = try hooks.parse_operand(hooks.ptr, column, insert_columns, null);
    var first_transferred = false;
    errdefer if (!first_transferred) freeExpression(alloc, first);
    try type_context.validateTextRowExpression(first);
    try parser.expectToken(tokens, pos, .comma);

    const second = try hooks.parse_operand(hooks.ptr, column, insert_columns, null);
    var second_transferred = false;
    errdefer if (!second_transferred) freeExpression(alloc, second);
    try type_context.validateTextRowExpression(second);
    try parser.expectToken(tokens, pos, .comma);

    const third = try hooks.parse_operand(hooks.ptr, column, insert_columns, null);
    var third_transferred = false;
    errdefer if (!third_transferred) freeExpression(alloc, third);
    try type_context.validateTextRowExpression(third);
    try parser.expectToken(tokens, pos, .rparen);

    const expression = try lower_expr.buildTernaryFunctionExpressionAlloc(alloc, kind, first, second, third);
    first_transferred = true;
    second_transferred = true;
    third_transferred = true;
    return expression;
}

pub fn parseConflictTextBinaryExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    column: runtime_schema.RelationalColumn,
    insert_columns: []const []const u8,
    expected_type: ?runtime_schema.AntflyType,
    kind: db_mod.types.RelationalRowsExpressionKind,
    type_context: lower_expr.RowExpressionTypeContext,
    hooks: ConflictUnaryExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    if (expected_type) |field_type| {
        const compatible = switch (kind) {
            .regexp_count, .regexp_instr => field_type == .numeric,
            .regexp_substr => lower_expr.sqlExpressionTypeIsTextLike(field_type),
            .starts_with, .ends_with => field_type == .boolean,
            else => false,
        };
        if (!compatible) return error.UnsupportedSqlShape;
    }
    switch (kind) {
        .regexp_count => try lower_expr.parseFunctionCallStartIf(tokens, pos, lower_expr.sqlKeywordIsRegexpCountFunction),
        .regexp_instr => try lower_expr.parseFunctionCallStartIf(tokens, pos, lower_expr.sqlKeywordIsRegexpInstrFunction),
        .regexp_substr => try lower_expr.parseFunctionCallStartIf(tokens, pos, lower_expr.sqlKeywordIsRegexpSubstrFunction),
        .starts_with => try lower_expr.parseFunctionCallStartIf(tokens, pos, lower_expr.sqlKeywordIsStartsWithFunction),
        .ends_with => try lower_expr.parseFunctionCallStartIf(tokens, pos, lower_expr.sqlKeywordIsEndsWithFunction),
        else => return error.UnsupportedSqlShape,
    }
    const lhs = try hooks.parse_operand(hooks.ptr, column, insert_columns, null);
    var lhs_transferred = false;
    errdefer if (!lhs_transferred) freeExpression(alloc, lhs);
    try type_context.validateTextRowExpression(lhs);
    try parser.expectToken(tokens, pos, .comma);
    const rhs = try hooks.parse_operand(hooks.ptr, column, insert_columns, null);
    var rhs_transferred = false;
    errdefer if (!rhs_transferred) freeExpression(alloc, rhs);
    try type_context.validateTextRowExpression(rhs);
    try parser.expectToken(tokens, pos, .rparen);

    const expression = try lower_expr.buildBinaryFunctionExpressionAlloc(alloc, kind, lhs, rhs);
    lhs_transferred = true;
    rhs_transferred = true;
    return expression;
}

pub fn parseConflictRegexpListExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    column: runtime_schema.RelationalColumn,
    insert_columns: []const []const u8,
    expected_type: ?runtime_schema.AntflyType,
    kind: db_mod.types.RelationalRowsExpressionKind,
    type_context: lower_expr.RowExpressionTypeContext,
    hooks: ConflictUnaryExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    if (expected_type) |field_type| {
        const compatible = switch (kind) {
            .regexp_replace => lower_expr.sqlExpressionTypeIsTextLike(field_type),
            .regexp_match => field_type == .boolean,
            else => false,
        };
        if (!compatible) return error.UnsupportedSqlShape;
    }

    switch (kind) {
        .regexp_replace => try lower_expr.parseRegexpReplaceFunctionCallStart(tokens, pos),
        .regexp_match => try lower_expr.parseFunctionCallStartIf(tokens, pos, lower_expr.sqlKeywordIsRegexpMatchFunction),
        else => return error.UnsupportedSqlShape,
    }

    var operands = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpression).empty;
    errdefer {
        for (operands.items) |operand| freeExpression(alloc, operand);
        operands.deinit(alloc);
    }

    switch (kind) {
        .regexp_replace => {
            while (true) {
                const operand = try hooks.parse_operand(hooks.ptr, column, insert_columns, null);
                var operand_transferred = false;
                errdefer if (!operand_transferred) freeExpression(alloc, operand);
                try type_context.validateTextRowExpression(operand);
                try operands.append(alloc, operand);
                operand_transferred = true;
                if (parser.matchToken(tokens, pos, .comma) == null) break;
            }
            if (operands.items.len != 3 and operands.items.len != 4) return error.UnsupportedSqlShape;
        },
        .regexp_match => {
            const source = try hooks.parse_operand(hooks.ptr, column, insert_columns, null);
            var source_transferred = false;
            errdefer if (!source_transferred) freeExpression(alloc, source);
            try type_context.validateTextRowExpression(source);
            try operands.append(alloc, source);
            source_transferred = true;

            try parser.expectToken(tokens, pos, .comma);
            const pattern = try hooks.parse_operand(hooks.ptr, column, insert_columns, null);
            var pattern_transferred = false;
            errdefer if (!pattern_transferred) freeExpression(alloc, pattern);
            try type_context.validateTextRowExpression(pattern);
            try operands.append(alloc, pattern);
            pattern_transferred = true;

            if (parser.matchToken(tokens, pos, .comma) != null) {
                const case_insensitive = try hooks.parse_operand(hooks.ptr, column, insert_columns, .boolean);
                var case_transferred = false;
                errdefer if (!case_transferred) freeExpression(alloc, case_insensitive);
                try type_context.validateBooleanRowExpression(case_insensitive);
                try operands.append(alloc, case_insensitive);
                case_transferred = true;
            }
        },
        else => unreachable,
    }

    try parser.expectToken(tokens, pos, .rparen);
    return try lower_expr.buildFunctionExpressionFromOperandListAlloc(alloc, kind, &operands);
}

pub fn parseConflictTextNumericBinaryExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    column: runtime_schema.RelationalColumn,
    insert_columns: []const []const u8,
    expected_type: ?runtime_schema.AntflyType,
    kind: db_mod.types.RelationalRowsExpressionKind,
    type_context: lower_expr.RowExpressionTypeContext,
    text_hooks: ConflictUnaryExpressionParserHooks,
    numeric_hooks: ConflictCoalesceExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    if (expected_type) |field_type| if (!lower_expr.sqlExpressionTypeIsTextLike(field_type)) return error.UnsupportedSqlShape;
    const expression_kind: db_mod.types.RelationalRowsExpressionKind = switch (kind) {
        .left, .right => blk: {
            const matched_kind = lower_expr.matchLeftRightFunctionKind(tokens, pos) orelse return error.UnsupportedSqlShape;
            try parser.expectToken(tokens, pos, .lparen);
            break :blk matched_kind;
        },
        .repeat => blk: {
            try lower_expr.parseFunctionCallStartIf(tokens, pos, lower_expr.sqlKeywordIsRepeatFunction);
            break :blk .repeat;
        },
        else => return error.UnsupportedSqlShape,
    };

    const source = try text_hooks.parse_operand(text_hooks.ptr, column, insert_columns, null);
    var source_transferred = false;
    errdefer if (!source_transferred) freeExpression(alloc, source);
    try type_context.validateTextRowExpression(source);

    try parser.expectToken(tokens, pos, .comma);
    const count = try numeric_hooks.parse_operand(numeric_hooks.ptr, column, insert_columns, .numeric);
    var count_transferred = false;
    errdefer if (!count_transferred) freeExpression(alloc, count);
    try type_context.validateNumericRowExpression(count);

    try parser.expectToken(tokens, pos, .rparen);
    const expression = try lower_expr.buildBinaryFunctionExpressionAlloc(alloc, expression_kind, source, count);
    source_transferred = true;
    count_transferred = true;
    return expression;
}

pub fn parseConflictMixedTextFunctionExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    column: runtime_schema.RelationalColumn,
    insert_columns: []const []const u8,
    expected_type: ?runtime_schema.AntflyType,
    kind: db_mod.types.RelationalRowsExpressionKind,
    type_context: lower_expr.RowExpressionTypeContext,
    text_hooks: ConflictUnaryExpressionParserHooks,
    numeric_hooks: ConflictCoalesceExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    if (expected_type) |field_type| if (!lower_expr.sqlExpressionTypeIsTextLike(field_type)) return error.UnsupportedSqlShape;

    const expression_kind: db_mod.types.RelationalRowsExpressionKind = switch (kind) {
        .substring => blk: {
            try lower_expr.parseFunctionCallStartIf(tokens, pos, lower_expr.sqlKeywordIsSubstringFunction);
            break :blk .substring;
        },
        .overlay => blk: {
            try lower_expr.parseFunctionCallStartIf(tokens, pos, lower_expr.sqlKeywordIsOverlayFunction);
            break :blk .overlay;
        },
        .split_part => blk: {
            try lower_expr.parseFunctionCallStartIf(tokens, pos, lower_expr.sqlKeywordIsSplitPartFunction);
            break :blk .split_part;
        },
        .lpad, .rpad => blk: {
            const matched_kind = lower_expr.matchPadFunctionKind(tokens, pos) orelse return error.UnsupportedSqlShape;
            try parser.expectToken(tokens, pos, .lparen);
            break :blk matched_kind;
        },
        else => return error.UnsupportedSqlShape,
    };

    var operands = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpression).empty;
    errdefer {
        for (operands.items) |operand| freeExpression(alloc, operand);
        operands.deinit(alloc);
    }

    switch (expression_kind) {
        .substring => {
            const text_operand = try text_hooks.parse_operand(text_hooks.ptr, column, insert_columns, null);
            var text_transferred = false;
            errdefer if (!text_transferred) freeExpression(alloc, text_operand);
            try type_context.validateTextRowExpression(text_operand);
            try operands.append(alloc, text_operand);
            text_transferred = true;

            if (parser.matchToken(tokens, pos, .comma) != null) {
                const start_operand = try numeric_hooks.parse_operand(numeric_hooks.ptr, column, insert_columns, .numeric);
                var start_transferred = false;
                errdefer if (!start_transferred) freeExpression(alloc, start_operand);
                try type_context.validateNumericRowExpression(start_operand);
                try operands.append(alloc, start_operand);
                start_transferred = true;

                if (parser.matchToken(tokens, pos, .comma) != null) {
                    const length_operand = try numeric_hooks.parse_operand(numeric_hooks.ptr, column, insert_columns, .numeric);
                    var length_transferred = false;
                    errdefer if (!length_transferred) freeExpression(alloc, length_operand);
                    try type_context.validateNumericRowExpression(length_operand);
                    try operands.append(alloc, length_operand);
                    length_transferred = true;
                }
            } else if (parser.matchKeyword(tokens, pos, "from")) {
                const start_operand = try numeric_hooks.parse_operand(numeric_hooks.ptr, column, insert_columns, .numeric);
                var start_transferred = false;
                errdefer if (!start_transferred) freeExpression(alloc, start_operand);
                try type_context.validateNumericRowExpression(start_operand);
                try operands.append(alloc, start_operand);
                start_transferred = true;

                if (parser.matchKeyword(tokens, pos, "for")) {
                    const length_operand = try numeric_hooks.parse_operand(numeric_hooks.ptr, column, insert_columns, .numeric);
                    var length_transferred = false;
                    errdefer if (!length_transferred) freeExpression(alloc, length_operand);
                    try type_context.validateNumericRowExpression(length_operand);
                    try operands.append(alloc, length_operand);
                    length_transferred = true;
                }
            } else return error.UnsupportedSqlShape;
        },
        .overlay => {
            const source_operand = try text_hooks.parse_operand(text_hooks.ptr, column, insert_columns, null);
            var source_transferred = false;
            errdefer if (!source_transferred) freeExpression(alloc, source_operand);
            try type_context.validateTextRowExpression(source_operand);
            try operands.append(alloc, source_operand);
            source_transferred = true;

            try parser.expectKeyword(tokens, pos, "placing");
            const replacement_operand = try text_hooks.parse_operand(text_hooks.ptr, column, insert_columns, null);
            var replacement_transferred = false;
            errdefer if (!replacement_transferred) freeExpression(alloc, replacement_operand);
            try type_context.validateTextRowExpression(replacement_operand);
            try operands.append(alloc, replacement_operand);
            replacement_transferred = true;

            try parser.expectKeyword(tokens, pos, "from");
            const start_operand = try numeric_hooks.parse_operand(numeric_hooks.ptr, column, insert_columns, .numeric);
            var start_transferred = false;
            errdefer if (!start_transferred) freeExpression(alloc, start_operand);
            try type_context.validateNumericRowExpression(start_operand);
            try operands.append(alloc, start_operand);
            start_transferred = true;

            if (parser.matchKeyword(tokens, pos, "for")) {
                const length_operand = try numeric_hooks.parse_operand(numeric_hooks.ptr, column, insert_columns, .numeric);
                var length_transferred = false;
                errdefer if (!length_transferred) freeExpression(alloc, length_operand);
                try type_context.validateNumericRowExpression(length_operand);
                try operands.append(alloc, length_operand);
                length_transferred = true;
            }
        },
        .split_part => {
            const source = try text_hooks.parse_operand(text_hooks.ptr, column, insert_columns, null);
            var source_transferred = false;
            errdefer if (!source_transferred) freeExpression(alloc, source);
            try type_context.validateTextRowExpression(source);
            try operands.append(alloc, source);
            source_transferred = true;

            try parser.expectToken(tokens, pos, .comma);
            const delimiter = try text_hooks.parse_operand(text_hooks.ptr, column, insert_columns, null);
            var delimiter_transferred = false;
            errdefer if (!delimiter_transferred) freeExpression(alloc, delimiter);
            try type_context.validateTextRowExpression(delimiter);
            try operands.append(alloc, delimiter);
            delimiter_transferred = true;

            try parser.expectToken(tokens, pos, .comma);
            const position = try numeric_hooks.parse_operand(numeric_hooks.ptr, column, insert_columns, .numeric);
            var position_transferred = false;
            errdefer if (!position_transferred) freeExpression(alloc, position);
            try type_context.validateNumericRowExpression(position);
            try operands.append(alloc, position);
            position_transferred = true;
        },
        .lpad, .rpad => {
            const source = try text_hooks.parse_operand(text_hooks.ptr, column, insert_columns, null);
            var source_transferred = false;
            errdefer if (!source_transferred) freeExpression(alloc, source);
            try type_context.validateTextRowExpression(source);
            try operands.append(alloc, source);
            source_transferred = true;

            try parser.expectToken(tokens, pos, .comma);
            const target = try numeric_hooks.parse_operand(numeric_hooks.ptr, column, insert_columns, .numeric);
            var target_transferred = false;
            errdefer if (!target_transferred) freeExpression(alloc, target);
            try type_context.validateNumericRowExpression(target);
            try operands.append(alloc, target);
            target_transferred = true;

            if (parser.matchToken(tokens, pos, .comma) != null) {
                const fill = try text_hooks.parse_operand(text_hooks.ptr, column, insert_columns, null);
                var fill_transferred = false;
                errdefer if (!fill_transferred) freeExpression(alloc, fill);
                try type_context.validateTextRowExpression(fill);
                try operands.append(alloc, fill);
                fill_transferred = true;
            }
        },
        else => unreachable,
    }

    try parser.expectToken(tokens, pos, .rparen);
    return try lower_expr.buildFunctionExpressionFromOperandListAlloc(alloc, expression_kind, &operands);
}

pub fn parseConflictStrposExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    column: runtime_schema.RelationalColumn,
    insert_columns: []const []const u8,
    expected_type: ?runtime_schema.AntflyType,
    type_context: lower_expr.RowExpressionTypeContext,
    hooks: ConflictUnaryExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    if (expected_type) |field_type| if (field_type != .numeric) return error.UnsupportedSqlShape;

    if (lower_expr.matchFunctionKeyword(tokens, pos, lower_expr.sqlKeywordIsStrposFunction)) {
        try parser.expectToken(tokens, pos, .lparen);
        const source = try hooks.parse_operand(hooks.ptr, column, insert_columns, null);
        var source_transferred = false;
        errdefer if (!source_transferred) freeExpression(alloc, source);
        try type_context.validateTextRowExpression(source);

        try parser.expectToken(tokens, pos, .comma);
        const needle = try hooks.parse_operand(hooks.ptr, column, insert_columns, null);
        var needle_transferred = false;
        errdefer if (!needle_transferred) freeExpression(alloc, needle);
        try type_context.validateTextRowExpression(needle);

        try parser.expectToken(tokens, pos, .rparen);
        const expression = try lower_expr.buildBinaryFunctionExpressionAlloc(alloc, .strpos, source, needle);
        source_transferred = true;
        needle_transferred = true;
        return expression;
    } else if (parser.matchKeyword(tokens, pos, "position")) {
        try parser.expectToken(tokens, pos, .lparen);
        const needle = try hooks.parse_operand(hooks.ptr, column, insert_columns, null);
        var needle_transferred = false;
        errdefer if (!needle_transferred) freeExpression(alloc, needle);
        try type_context.validateTextRowExpression(needle);

        try parser.expectKeyword(tokens, pos, "in");
        const source = try hooks.parse_operand(hooks.ptr, column, insert_columns, null);
        var source_transferred = false;
        errdefer if (!source_transferred) freeExpression(alloc, source);
        try type_context.validateTextRowExpression(source);

        try parser.expectToken(tokens, pos, .rparen);
        const expression = try lower_expr.buildBinaryFunctionExpressionAlloc(alloc, .strpos, source, needle);
        source_transferred = true;
        needle_transferred = true;
        return expression;
    } else return error.UnsupportedSqlShape;
}

pub fn parseDatePartUnitLiteralExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !db_mod.types.RelationalRowsExpression {
    const token = if (parser.matchToken(tokens, pos, .identifier)) |token|
        token
    else if (parser.matchToken(tokens, pos, .string)) |token|
        token
    else
        return error.UnsupportedSqlShape;
    if (std.ascii.eqlIgnoreCase(token.text, "from")) return error.UnsupportedSqlShape;
    return .{
        .kind = .value,
        .value_json = try std.json.Stringify.valueAlloc(alloc, token.text, .{}),
    };
}

pub fn parseConflictDateExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    column: runtime_schema.RelationalColumn,
    insert_columns: []const []const u8,
    expected_type: ?runtime_schema.AntflyType,
    kind: db_mod.types.RelationalRowsExpressionKind,
    type_context: lower_expr.RowExpressionTypeContext,
    hooks: ConflictUnaryExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    switch (kind) {
        .date_trunc, .date_bin => {
            if (expected_type) |field_type| if (field_type != .numeric and field_type != .datetime) return error.UnsupportedSqlShape;
        },
        .date_part => {
            if (expected_type) |field_type| if (field_type != .numeric) return error.UnsupportedSqlShape;
        },
        else => return error.UnsupportedSqlShape,
    }

    switch (kind) {
        .date_trunc => {
            try lower_expr.parseFunctionCallStartIf(tokens, pos, lower_expr.sqlKeywordIsDateTruncFunction);
            const unit = try hooks.parse_operand(hooks.ptr, column, insert_columns, null);
            var unit_transferred = false;
            errdefer if (!unit_transferred) freeExpression(alloc, unit);
            try type_context.validateTextRowExpression(unit);

            try parser.expectToken(tokens, pos, .comma);
            const value = try hooks.parse_operand(hooks.ptr, column, insert_columns, null);
            var value_transferred = false;
            errdefer if (!value_transferred) freeExpression(alloc, value);
            try type_context.validateNumericOrDatetimeRowExpression(value);

            try parser.expectToken(tokens, pos, .rparen);
            const expression = try lower_expr.buildBinaryFunctionExpressionAlloc(alloc, .date_trunc, unit, value);
            unit_transferred = true;
            value_transferred = true;
            return expression;
        },
        .date_part => {
            const extract_syntax = try lower_expr.parseDatePartFunctionCallStart(tokens, pos);
            var unit: db_mod.types.RelationalRowsExpression = undefined;
            var unit_initialized = false;
            var unit_transferred = false;
            errdefer if (unit_initialized and !unit_transferred) freeExpression(alloc, unit);
            var value: db_mod.types.RelationalRowsExpression = undefined;
            var value_initialized = false;
            var value_transferred = false;
            errdefer if (value_initialized and !value_transferred) freeExpression(alloc, value);

            if (extract_syntax) {
                unit = try parseDatePartUnitLiteralExpressionAlloc(alloc, tokens, pos);
                unit_initialized = true;
                try lower_expr.parseDatePartExtractSeparator(tokens, pos);
                value = try hooks.parse_operand(hooks.ptr, column, insert_columns, null);
                value_initialized = true;
                try type_context.validateNumericOrDatetimeRowExpression(value);
            } else {
                unit = try hooks.parse_operand(hooks.ptr, column, insert_columns, null);
                unit_initialized = true;
                try type_context.validateTextRowExpression(unit);
                try parser.expectToken(tokens, pos, .comma);
                value = try hooks.parse_operand(hooks.ptr, column, insert_columns, null);
                value_initialized = true;
                try type_context.validateNumericOrDatetimeRowExpression(value);
            }

            try parser.expectToken(tokens, pos, .rparen);
            const expression = try lower_expr.buildBinaryFunctionExpressionAlloc(alloc, .date_part, unit, value);
            unit_transferred = true;
            value_transferred = true;
            return expression;
        },
        .date_bin => {
            try lower_expr.parseFunctionCallStartIf(tokens, pos, lower_expr.sqlKeywordIsDateBinFunction);
            const stride = try hooks.parse_operand(hooks.ptr, column, insert_columns, null);
            var stride_transferred = false;
            errdefer if (!stride_transferred) freeExpression(alloc, stride);
            try type_context.validateDateBinStrideRowExpression(stride);

            try parser.expectToken(tokens, pos, .comma);
            const source = try hooks.parse_operand(hooks.ptr, column, insert_columns, null);
            var source_transferred = false;
            errdefer if (!source_transferred) freeExpression(alloc, source);
            try type_context.validateNumericOrDatetimeRowExpression(source);

            try parser.expectToken(tokens, pos, .comma);
            const origin = try hooks.parse_operand(hooks.ptr, column, insert_columns, null);
            var origin_transferred = false;
            errdefer if (!origin_transferred) freeExpression(alloc, origin);
            try type_context.validateNumericOrDatetimeRowExpression(origin);

            try parser.expectToken(tokens, pos, .rparen);
            const expression = try lower_expr.buildTernaryFunctionExpressionAlloc(alloc, .date_bin, stride, source, origin);
            stride_transferred = true;
            source_transferred = true;
            origin_transferred = true;
            return expression;
        },
        else => unreachable,
    }
}

pub fn parseConflictGreatestLeastExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    column: runtime_schema.RelationalColumn,
    insert_columns: []const []const u8,
    expected_type: ?runtime_schema.AntflyType,
    hooks: ConflictCoalesceExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    const kind = try lower_expr.parseGreatestLeastFunctionCallStart(tokens, pos);

    var operands = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpression).empty;
    errdefer {
        for (operands.items) |operand| freeExpression(alloc, operand);
        operands.deinit(alloc);
    }
    while (true) {
        const operand = try hooks.parse_operand(hooks.ptr, column, insert_columns, expected_type);
        var operand_transferred = false;
        errdefer if (!operand_transferred) freeExpression(alloc, operand);
        try operands.append(alloc, operand);
        operand_transferred = true;
        if (parser.matchToken(tokens, pos, .comma) == null) break;
    }
    if (operands.items.len == 0) return error.UnsupportedSqlShape;
    try parser.expectToken(tokens, pos, .rparen);

    return try lower_expr.buildFunctionExpressionFromOperandListAlloc(alloc, kind, &operands);
}

pub fn parseConflictJsonUnaryExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    column: runtime_schema.RelationalColumn,
    insert_columns: []const []const u8,
    kind: db_mod.types.RelationalRowsExpressionKind,
    type_context: lower_expr.RowExpressionTypeContext,
    hooks: ConflictUnaryExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    switch (kind) {
        .json_array_length => try lower_expr.parseJsonArrayLengthFunctionCallStart(tokens, pos),
        .json_typeof => try lower_expr.parseJsonTypeofFunctionCallStart(tokens, pos),
        else => return error.UnsupportedSqlShape,
    }
    const operand = try hooks.parse_operand(hooks.ptr, column, insert_columns, .json);
    var operand_transferred = false;
    errdefer if (!operand_transferred) freeExpression(alloc, operand);
    try type_context.validateJsonRowExpression(operand);
    try parser.expectToken(tokens, pos, .rparen);

    const expression = try lower_expr.buildUnaryFunctionExpressionAlloc(alloc, kind, operand);
    operand_transferred = true;
    return expression;
}

pub fn parseConflictJsonExtractPathExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const sql_value.SqlValue,
    column: runtime_schema.RelationalColumn,
    insert_columns: []const []const u8,
    expected_type: ?runtime_schema.AntflyType,
    type_context: lower_expr.RowExpressionTypeContext,
    hooks: ConflictUnaryExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    const keyword = lower_expr.matchFunctionKeywordText(tokens, pos, lower_expr.sqlKeywordIsJsonExtractPathFunction) orelse return error.UnsupportedSqlShape;
    const as_text = lower_expr.sqlJsonExtractPathFunctionAsText(keyword);
    const output_type: runtime_schema.AntflyType = if (as_text) .keyword else .json;
    if (expected_type) |field_type| {
        if (!lower_expr.sqlExpressionTypesComparable(field_type, output_type)) return error.UnsupportedSqlShape;
    }

    try parser.expectToken(tokens, pos, .lparen);
    const operand = try hooks.parse_operand(hooks.ptr, column, insert_columns, .json);
    var operand_transferred = false;
    errdefer if (!operand_transferred) freeExpression(alloc, operand);
    try type_context.validateJsonRowExpression(operand);
    const path = try sql_value.parseJsonExtractPathSegmentsAlloc(alloc, tokens, pos, params);
    var path_transferred = false;
    errdefer if (!path_transferred) alloc.free(path);
    try parser.expectToken(tokens, pos, .rparen);

    const expression = try lower_expr.buildJsonExtractExpressionAlloc(alloc, operand, path, as_text);
    operand_transferred = true;
    path_transferred = true;
    return expression;
}

fn parseConflictJsonBuildObjectOperandExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    column: runtime_schema.RelationalColumn,
    insert_columns: []const []const u8,
    hooks: ConflictJsonBuildObjectExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    if (sql_value.peekToJsonbFunctionCall(tokens, pos.*)) {
        try lower_expr.parseFixedUnaryFunctionCallStart(tokens, pos, .to_jsonb);
        const expression = try parseConflictJsonBuildObjectOperandExpressionAlloc(alloc, tokens, pos, column, insert_columns, hooks);
        var expression_transferred = false;
        errdefer if (!expression_transferred) freeExpression(alloc, expression);
        try parser.expectToken(tokens, pos, .rparen);
        expression_transferred = true;
        return expression;
    }
    if (sql_value.peekConvertFromFunctionCall(tokens, pos.*)) {
        const value_json = try hooks.parse_json_value(hooks.ptr);
        errdefer alloc.free(value_json);
        return .{ .kind = .value, .value_json = value_json };
    }
    return try hooks.parse_operand(hooks.ptr, column, insert_columns, null);
}

pub fn parseConflictJsonBuildObjectExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    column: runtime_schema.RelationalColumn,
    insert_columns: []const []const u8,
    expected_type: ?runtime_schema.AntflyType,
    type_context: lower_expr.RowExpressionTypeContext,
    hooks: ConflictJsonBuildObjectExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    if (expected_type) |field_type| {
        if (field_type != .json) return error.UnsupportedSqlShape;
    }
    try lower_expr.parseJsonBuildObjectFunctionCallStart(tokens, pos);
    var operands = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpression).empty;
    errdefer {
        for (operands.items) |operand| freeExpression(alloc, operand);
        operands.deinit(alloc);
    }
    if (parser.matchToken(tokens, pos, .rparen) == null) {
        while (true) {
            const key = try hooks.parse_operand(hooks.ptr, column, insert_columns, .text);
            var key_transferred = false;
            errdefer if (!key_transferred) freeExpression(alloc, key);
            try type_context.validateTextRowExpression(key);
            try operands.append(alloc, key);
            key_transferred = true;

            try parser.expectToken(tokens, pos, .comma);
            const value = try parseConflictJsonBuildObjectOperandExpressionAlloc(alloc, tokens, pos, column, insert_columns, hooks);
            var value_transferred = false;
            errdefer if (!value_transferred) freeExpression(alloc, value);
            _ = try type_context.rowExpressionOutputType(value);
            try operands.append(alloc, value);
            value_transferred = true;

            if (parser.matchToken(tokens, pos, .comma) == null) break;
        }
        try parser.expectToken(tokens, pos, .rparen);
    }
    const expression = try lower_expr.buildFunctionExpressionFromOperandListAlloc(alloc, .json_build_object, &operands);
    var expression_transferred = false;
    errdefer if (!expression_transferred) freeExpression(alloc, expression);
    try type_context.validateJsonBuildObjectExpression(expression);
    expression_transferred = true;
    return expression;
}

pub fn parseConflictTextArrayExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    column: runtime_schema.RelationalColumn,
    insert_columns: []const []const u8,
    expected_type: ?runtime_schema.AntflyType,
    kind: db_mod.types.RelationalRowsExpressionKind,
    type_context: lower_expr.RowExpressionTypeContext,
    hooks: ConflictUnaryExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    if (expected_type) |field_type| {
        const compatible = switch (kind) {
            .string_to_array => field_type == .array,
            .array_to_string => lower_expr.sqlExpressionTypeIsTextLike(field_type),
            else => false,
        };
        if (!compatible) return error.UnsupportedSqlShape;
    }

    switch (kind) {
        .string_to_array => {
            try lower_expr.parseStringToArrayFunctionCallStart(tokens, pos);
            const text_expression = try hooks.parse_operand(hooks.ptr, column, insert_columns, null);
            var text_transferred = false;
            errdefer if (!text_transferred) freeExpression(alloc, text_expression);
            try type_context.validateTextRowExpression(text_expression);

            try parser.expectToken(tokens, pos, .comma);
            const delimiter_expression = try hooks.parse_operand(hooks.ptr, column, insert_columns, null);
            var delimiter_transferred = false;
            errdefer if (!delimiter_transferred) freeExpression(alloc, delimiter_expression);
            try type_context.validateTextRowExpression(delimiter_expression);
            try type_context.validateStringToArrayDelimiterLiteral(delimiter_expression);

            try parser.expectToken(tokens, pos, .rparen);
            const expression = try lower_expr.buildBinaryFunctionExpressionAlloc(alloc, .string_to_array, text_expression, delimiter_expression);
            text_transferred = true;
            delimiter_transferred = true;
            return expression;
        },
        .array_to_string => {
            try lower_expr.parseArrayToStringFunctionCallStart(tokens, pos);
            var operands = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpression).empty;
            errdefer {
                for (operands.items) |operand| freeExpression(alloc, operand);
                operands.deinit(alloc);
            }

            const array_expression = try hooks.parse_operand(hooks.ptr, column, insert_columns, .array);
            var array_transferred = false;
            errdefer if (!array_transferred) freeExpression(alloc, array_expression);
            try operands.append(alloc, array_expression);
            array_transferred = true;

            try parser.expectToken(tokens, pos, .comma);
            const delimiter_expression = try hooks.parse_operand(hooks.ptr, column, insert_columns, null);
            var delimiter_transferred = false;
            errdefer if (!delimiter_transferred) freeExpression(alloc, delimiter_expression);
            try type_context.validateTextRowExpression(delimiter_expression);
            try operands.append(alloc, delimiter_expression);
            delimiter_transferred = true;

            if (parser.matchToken(tokens, pos, .comma) != null) {
                const null_expression = try hooks.parse_operand(hooks.ptr, column, insert_columns, null);
                var null_transferred = false;
                errdefer if (!null_transferred) freeExpression(alloc, null_expression);
                try type_context.validateTextRowExpression(null_expression);
                try operands.append(alloc, null_expression);
                null_transferred = true;
            }
            try parser.expectToken(tokens, pos, .rparen);

            const expression = try lower_expr.buildFunctionExpressionFromOperandListAlloc(alloc, .array_to_string, &operands);
            var expression_transferred = false;
            errdefer if (!expression_transferred) freeExpression(alloc, expression);
            try type_context.validateArrayToStringExpression(expression);
            expression_transferred = true;
            return expression;
        },
        else => return error.UnsupportedSqlShape,
    }
}

pub fn parseConflictArrayLengthExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const sql_value.SqlValue,
    insert_columns: []const []const u8,
    hooks: ConflictArrayLengthExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    const keyword = try lower_expr.parseArrayLengthFunctionCallStart(tokens, pos);
    const field = try hooks.parse_field(hooks.ptr, insert_columns, .array);
    var field_transferred = false;
    errdefer if (!field_transferred) freeExpression(alloc, field);
    try sql_value.parseArrayLengthFunctionTail(tokens, pos, params, keyword);

    const expression = try lower_expr.buildUnaryFunctionExpressionAlloc(alloc, .array_length, field);
    field_transferred = true;
    return expression;
}

pub fn parseConflictFieldExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    conflict_existing_qualifiers: []const []const u8,
    insert_columns: []const []const u8,
    expected_type: ?runtime_schema.AntflyType,
) !db_mod.types.RelationalRowsExpression {
    const token = parser.matchToken(tokens, pos, .identifier) orelse return error.UnsupportedSqlShape;
    const source: db_mod.types.RelationalRowsExpressionFieldSource = if (std.mem.startsWith(u8, token.text, "excluded.")) {
        const field = token.text["excluded.".len..];
        const source_column = binder.relationalColumnForField(schema, field, null) orelse return error.InvalidSqlCatalog;
        if (expected_type) |field_type| if (source_column.field_type != field_type) return error.UnsupportedSqlShape;
        if (!conflictProposedColumnAvailable(insert_columns, source_column, field)) return error.UnsupportedSqlShape;
        return .{
            .kind = .field,
            .field = try alloc.dupe(u8, field),
            .field_source = .proposed,
        };
    } else blk: {
        const field = conflictExistingFieldName(alloc, conflict_existing_qualifiers, token.text) orelse token.text;
        const source_column = binder.relationalColumnForField(schema, field, null) orelse return error.InvalidSqlCatalog;
        if (expected_type) |field_type| if (source_column.field_type != field_type) return error.UnsupportedSqlShape;
        break :blk .existing;
    };
    const field = conflictExistingFieldName(alloc, conflict_existing_qualifiers, token.text) orelse token.text;
    return .{
        .kind = .field,
        .field = try alloc.dupe(u8, field),
        .field_source = source,
    };
}

pub fn parseConflictFieldOrJsonExtractExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const sql_value.SqlValue,
    schema: runtime_schema.TableSchema,
    conflict_existing_qualifiers: []const []const u8,
    insert_columns: []const []const u8,
    expected_type: ?runtime_schema.AntflyType,
    type_context: lower_expr.RowExpressionTypeContext,
) !db_mod.types.RelationalRowsExpression {
    const field_expression = try parseConflictFieldExpressionAlloc(alloc, tokens, pos, schema, conflict_existing_qualifiers, insert_columns, null);
    var field_transferred = false;
    errdefer if (!field_transferred) freeExpression(alloc, field_expression);

    if (lower_expr.matchJsonExtractOperator(tokens, pos)) |operator| {
        const as_text = lower_expr.tokenKindIsJsonExtractTextOperator(operator);
        _ = binder.relationalColumnForField(type_context.schemaForRowExpressionField(field_expression), field_expression.field, .json) orelse return error.InvalidSqlCatalog;
        const output_type: runtime_schema.AntflyType = if (as_text) .keyword else .json;
        if (expected_type) |field_type| {
            if (!lower_expr.sqlExpressionTypesComparable(field_type, output_type)) return error.UnsupportedSqlShape;
        }

        const path = try sql_value.parseJsonExtractOperatorPathOwnedAlloc(alloc, tokens, pos, params, operator);
        var path_transferred = false;
        errdefer if (!path_transferred) alloc.free(path);
        const expression = try lower_expr.buildJsonExtractExpressionAlloc(alloc, field_expression, path, as_text);
        field_transferred = true;
        path_transferred = true;
        return expression;
    }

    if (parser.matchToken(tokens, pos, .question) != null) {
        _ = binder.relationalColumnForField(type_context.schemaForRowExpressionField(field_expression), field_expression.field, .json) orelse return error.InvalidSqlCatalog;
        if (expected_type) |field_type| {
            if (field_type != .boolean) return error.UnsupportedSqlShape;
        }

        const path = try sql_value.parseJsonPathOwnedAlloc(alloc, tokens, pos, params);
        var path_transferred = false;
        errdefer if (!path_transferred) alloc.free(path);
        const expression = try lower_expr.buildJsonPathExistsExpressionAlloc(alloc, field_expression, path);
        field_transferred = true;
        path_transferred = true;
        return expression;
    }

    if (expected_type) |field_type| {
        const output_type = try type_context.rowExpressionOutputType(field_expression);
        if (!lower_expr.sqlExpressionTypesComparable(field_type, output_type)) return error.UnsupportedSqlShape;
    }
    field_transferred = true;
    return field_expression;
}

pub fn parseConflictArrayPositionExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    column: runtime_schema.RelationalColumn,
    insert_columns: []const []const u8,
    expected_type: ?runtime_schema.AntflyType,
    type_context: lower_expr.RowExpressionTypeContext,
    hooks: ConflictUnaryExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    const kind = try lower_expr.parseArrayPositionFunctionCallStart(tokens, pos);
    if (expected_type) |field_type| {
        const expected_result_type: runtime_schema.AntflyType = if (kind == .array_positions) .array else .numeric;
        if (field_type != expected_result_type) return error.UnsupportedSqlShape;
    }

    const array_expression = try hooks.parse_operand(hooks.ptr, column, insert_columns, .array);
    var array_transferred = false;
    errdefer if (!array_transferred) freeExpression(alloc, array_expression);
    const array_type = try type_context.rowExpressionOutputType(array_expression);
    if (array_type != .array) return error.UnsupportedSqlShape;
    const item_type = (try type_context.rowExpressionOutputArrayItemType(array_expression)) orelse .json;

    try parser.expectToken(tokens, pos, .comma);
    const needle_expression = try hooks.parse_operand(hooks.ptr, column, insert_columns, item_type);
    var needle_transferred = false;
    errdefer if (!needle_transferred) freeExpression(alloc, needle_expression);
    try parser.expectToken(tokens, pos, .rparen);

    const expression = try lower_expr.buildBinaryFunctionExpressionAlloc(alloc, kind, array_expression, needle_expression);
    var expression_transferred = false;
    errdefer if (!expression_transferred) freeExpression(alloc, expression);
    array_transferred = true;
    needle_transferred = true;
    try type_context.validateArrayPositionExpression(expression);

    expression_transferred = true;
    return expression;
}

pub fn parseConflictArrayElementTransformExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    column: runtime_schema.RelationalColumn,
    insert_columns: []const []const u8,
    expected_type: ?runtime_schema.AntflyType,
    type_context: lower_expr.RowExpressionTypeContext,
    hooks: ConflictUnaryExpressionParserHooks,
) !db_mod.types.RelationalRowsExpression {
    if (expected_type) |field_type| if (field_type != .array) return error.UnsupportedSqlShape;
    const kind = try lower_expr.parseArrayElementTransformFunctionCallStart(tokens, pos);

    const first_expected: ?runtime_schema.AntflyType = if (kind == .array_prepend) null else .array;
    const first_expression = try hooks.parse_operand(hooks.ptr, column, insert_columns, first_expected);
    var first_transferred = false;
    errdefer if (!first_transferred) freeExpression(alloc, first_expression);

    try parser.expectToken(tokens, pos, .comma);
    const second_expected: ?runtime_schema.AntflyType = if (kind == .array_prepend or kind == .array_cat) .array else null;
    const second_expression = try hooks.parse_operand(hooks.ptr, column, insert_columns, second_expected);
    var second_transferred = false;
    errdefer if (!second_transferred) freeExpression(alloc, second_expression);

    var third_expression: db_mod.types.RelationalRowsExpression = undefined;
    var third_transferred = true;
    if (kind == .array_replace) {
        third_transferred = false;
        try parser.expectToken(tokens, pos, .comma);
        third_expression = try hooks.parse_operand(hooks.ptr, column, insert_columns, null);
        errdefer if (!third_transferred) freeExpression(alloc, third_expression);
    }
    try parser.expectToken(tokens, pos, .rparen);

    const expression = if (kind == .array_prepend) blk: {
        const out = try lower_expr.buildBinaryFunctionExpressionAlloc(alloc, kind, second_expression, first_expression);
        second_transferred = true;
        first_transferred = true;
        break :blk out;
    } else if (kind == .array_replace) blk: {
        const out = try lower_expr.buildTernaryFunctionExpressionAlloc(alloc, kind, first_expression, second_expression, third_expression);
        first_transferred = true;
        second_transferred = true;
        third_transferred = true;
        break :blk out;
    } else blk: {
        const out = try lower_expr.buildBinaryFunctionExpressionAlloc(alloc, kind, first_expression, second_expression);
        first_transferred = true;
        second_transferred = true;
        break :blk out;
    };
    var expression_transferred = false;
    errdefer if (!expression_transferred) freeExpression(alloc, expression);
    if (kind == .array_cat) {
        try type_context.validateArrayCatExpression(expression);
    } else if (kind == .array_replace) {
        try type_context.validateArrayReplaceExpression(expression);
    } else {
        try type_context.validateArrayElementTransformExpression(expression);
    }
    expression_transferred = true;
    return expression;
}

pub fn parseMergeArmConditionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    joined_source_schema: ?runtime_schema.TableSchema,
    target_table: TableAlias,
    source_table: TableAlias,
    allow_target: bool,
    predicates: *std.ArrayListUnmanaged(MergeArmPredicate),
    expression_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionCondition),
    expression_or_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    expression_not_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    hooks: MergeArmConditionParserHooks,
) !void {
    const checkpoint = pos.*;
    if (parseMergeArmPredicateAlloc(alloc, tokens, pos, schema, joined_source_schema, target_table, source_table, allow_target, .{
        .ptr = hooks.ptr,
        .parse_column_value_json = hooks.parse_column_value_json,
    })) |predicate| {
        var predicate_transferred = false;
        errdefer if (!predicate_transferred) freeMergeArmPredicateValue(alloc, predicate);
        if (parser.peekKeyword(tokens, pos.*, "or")) {
            freeMergeArmPredicateValue(alloc, predicate);
            predicate_transferred = true;
            pos.* = checkpoint;
            try hooks.parse_expression_predicates(hooks.ptr, target_table, source_table, allow_target, expression_predicates, expression_or_predicates, expression_not_predicates);
            return;
        }
        try predicates.append(alloc, predicate);
        predicate_transferred = true;
        return;
    } else |err| switch (err) {
        error.UnsupportedSqlShape => pos.* = checkpoint,
        else => return err,
    }

    try hooks.parse_expression_predicates(hooks.ptr, target_table, source_table, allow_target, expression_predicates, expression_or_predicates, expression_not_predicates);
}

pub fn parseMergeArmExpressionPredicatesAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: usize,
    target_table: TableAlias,
    source_table: TableAlias,
    allow_target: bool,
    expression_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionCondition),
    expression_or_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    expression_not_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    hooks: MergeArmExpressionPredicatesParserHooks,
) !void {
    var conditions = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionCondition).empty;
    defer conditions.deinit(alloc);
    errdefer freeExpressionConditions(alloc, conditions.items);
    var or_groups = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup).empty;
    defer or_groups.deinit(alloc);
    errdefer freeExpressionPredicateGroups(alloc, or_groups.items);
    var not_groups = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup).empty;
    defer not_groups.deinit(alloc);
    errdefer freeExpressionPredicateGroups(alloc, not_groups.items);

    if (mergeCanParseExpressionNotWhere(tokens, pos)) {
        try hooks.parse_expression_not_where(hooks.ptr, target_table, source_table, &not_groups);
    } else if (parser.hasTopLevelOrBeforeTail(tokens, pos, lower_expr.sqlWhereTailClauseKeyword) or mergeParenthesizedExpressionOrWhereCanStart(tokens, pos)) {
        try hooks.parse_expression_or_where(hooks.ptr, target_table, source_table, &or_groups);
    } else {
        try hooks.parse_expression_where_conditions(hooks.ptr, target_table, source_table, &conditions, &or_groups, &not_groups);
    }
    if (conditions.items.len == 0 and or_groups.items.len == 0 and not_groups.items.len == 0) return error.UnsupportedSqlShape;
    if (!allow_target) {
        for (conditions.items) |condition| {
            if (mergeExpressionConditionUsesTargetRow(condition)) return error.UnsupportedSqlShape;
        }
        if (mergeExpressionPredicateGroupsUseTargetRow(or_groups.items) or
            mergeExpressionPredicateGroupsUseTargetRow(not_groups.items))
        {
            return error.UnsupportedSqlShape;
        }
    }
    try expression_predicates.appendSlice(alloc, conditions.items);
    conditions.clearRetainingCapacity();
    try expression_or_predicates.appendSlice(alloc, or_groups.items);
    or_groups.clearRetainingCapacity();
    try expression_not_predicates.appendSlice(alloc, not_groups.items);
    not_groups.clearRetainingCapacity();
}

pub fn mergeParenthesizedExpressionOrWhereCanStart(tokens: []const Token, pos: usize) bool {
    if (!parser.peekKind(tokens, pos, .lparen)) return false;
    const inner = parser.predicateStartIndexAfterOpenParens(tokens, pos);
    if (!lower_expr.expressionPredicateCanStartAt(tokens, inner)) return false;

    var depth: usize = 0;
    var i = pos;
    while (i < tokens.len) : (i += 1) {
        const token = tokens[i];
        switch (token.kind) {
            .lparen => depth += 1,
            .rparen => {
                if (depth == 0) return false;
                depth -= 1;
                if (depth == 0) return false;
            },
            .identifier => if (depth == 1 and std.ascii.eqlIgnoreCase(token.text, "or")) return true,
            .semicolon => return false,
            else => {},
        }
    }
    return false;
}

pub fn mergeCanParseExpressionNotWhere(tokens: []const Token, pos: usize) bool {
    if (!parser.peekKeyword(tokens, pos, "not") or pos + 2 >= tokens.len or tokens[pos + 1].kind != .lparen) return false;
    const inner = parser.predicateStartIndexAfterOpenParens(tokens, pos + 1);
    return lower_expr.expressionPredicateCanStartAt(tokens, inner);
}

pub fn parseMergeUpdateAssignmentAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    joined_source_schema: ?runtime_schema.TableSchema,
    target_table: TableAlias,
    source_table: TableAlias,
    hooks: MergeAssignmentParserHooks,
) !MergeParsedAssignment {
    const target_field = try parseMergeTargetFieldOwnedAlloc(alloc, tokens, pos, target_table);
    errdefer alloc.free(target_field);
    try parser.expectToken(tokens, pos, .eq);
    const target_column = binder.relationalColumnForField(schema, target_field, null) orelse return error.InvalidSqlCatalog;
    const expression = try hooks.parse_assignment_expression(hooks.ptr, target_column, target_table, source_table);
    var expression_transferred = false;
    errdefer if (!expression_transferred) freeExpression(alloc, expression);
    if (expression.kind == .field and expression.field_source == .source) {
        try binder.validateMergeFields(schema, joined_source_schema, target_field, expression.field);
        const source_field = expression.field;
        expression_transferred = true;
        return .{ .mapping = .{
            .target_field = target_field,
            .source_field = source_field,
        } };
    }
    expression_transferred = true;
    return .{ .expression = .{
        .target_field = target_field,
        .expression = expression,
    } };
}

pub fn parseMergeInsertMappingsAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    joined_source_schema: ?runtime_schema.TableSchema,
    target_table: TableAlias,
    source_table: TableAlias,
    mappings: *std.ArrayListUnmanaged(MergeFieldMapping),
    expressions: *std.ArrayListUnmanaged(MergeExpressionAssignment),
    hooks: MergeAssignmentParserHooks,
) !void {
    try parser.expectToken(tokens, pos, .lparen);
    const target_fields = try grammar.parseIdentifierListAlloc(alloc, tokens, pos);
    defer strings.freeStringSlice(alloc, target_fields);
    if (target_fields.len == 0) return error.UnsupportedSqlShape;
    try parser.expectToken(tokens, pos, .rparen);
    try parser.expectKeyword(tokens, pos, "values");
    try parser.expectToken(tokens, pos, .lparen);
    for (target_fields, 0..) |target_field, i| {
        if (i != 0) try parser.expectToken(tokens, pos, .comma);
        const owned_target_field = try alloc.dupe(u8, target_field);
        var target_transferred = false;
        errdefer if (!target_transferred) alloc.free(owned_target_field);
        const target_column = binder.relationalColumnForField(schema, owned_target_field, null) orelse return error.InvalidSqlCatalog;
        const expression = try hooks.parse_assignment_expression(hooks.ptr, target_column, target_table, source_table);
        var expression_transferred = false;
        errdefer if (!expression_transferred) freeExpression(alloc, expression);
        if (expression.kind == .field and expression.field_source == .source) {
            try binder.validateMergeFields(schema, joined_source_schema, owned_target_field, expression.field);
            const source_field = expression.field;
            try mappings.append(alloc, .{
                .target_field = owned_target_field,
                .source_field = source_field,
            });
            target_transferred = true;
            expression_transferred = true;
        } else {
            if (mergeExpressionUsesTargetRow(expression)) return error.UnsupportedSqlShape;
            try expressions.append(alloc, .{
                .target_field = owned_target_field,
                .expression = expression,
            });
            target_transferred = true;
            expression_transferred = true;
        }
    }
    try parser.expectToken(tokens, pos, .rparen);
}

pub fn parseJoinedMutationTargetFieldAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    target_alias: []const u8,
) ![]const u8 {
    if (pos.* < tokens.len and tokens[pos.*].kind == .identifier and lower_expr.identifierContainsQualifier(tokens[pos.*].text)) {
        const source = try plan_mod.parseQualifiedFieldAlloc(alloc, tokens, pos);
        defer plan_mod.freeQualifiedField(alloc, source);
        if (!std.mem.eql(u8, source.qualifier, target_alias)) return error.UnsupportedSqlShape;
        return try alloc.dupe(u8, source.field);
    }
    return try grammar.parseIdentifierOwnedAlloc(alloc, tokens, pos);
}

pub fn insertSourceUniquePredicatesFromConstraintAlloc(
    alloc: std.mem.Allocator,
    predicates: []const runtime_schema.UniquePredicate,
) ![]const runtime_schema.RelationalCheck {
    if (predicates.len == 0) return &.{};
    const out = try alloc.alloc(runtime_schema.RelationalCheck, predicates.len);
    var initialized: usize = 0;
    errdefer {
        freeRelationalChecks(alloc, out[0..initialized]);
        alloc.free(out);
    }
    for (predicates) |predicate| {
        out[initialized] = .{
            .name = "",
            .field = try alloc.dupe(u8, predicate.field),
            .op = lower_expr.uniquePredicateAsRelationalCheckOp(predicate.op),
            .value_json = if (predicate.value_json) |value| try alloc.dupe(u8, value) else null,
            .validation_state = .enforced,
        };
        initialized += 1;
    }
    return out;
}

pub fn insertSourceConflictOperationsFromClauseAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    clause: ConflictClause,
) ![]const db_mod.types.TransformOp {
    const count = clause.patch.len + clause.increment.len + clause.array_update.len + insertSourceStaticJsonSetCount(clause.json_set);
    if (count == 0) return &.{};
    const operations = try alloc.alloc(db_mod.types.TransformOp, count);
    var initialized: usize = 0;
    errdefer {
        for (operations[0..initialized]) |op| {
            alloc.free(@constCast(op.path));
            if (op.value_json) |value_json| alloc.free(@constCast(value_json));
        }
        alloc.free(operations);
    }

    for (clause.patch) |item| {
        const column = binder.relationalColumnForField(schema, item.field, null) orelse return error.InvalidSqlCatalog;
        operations[initialized] = .{
            .op = .set,
            .path = try alloc.dupe(u8, column.path),
            .value_json = try alloc.dupe(u8, item.value_json),
        };
        initialized += 1;
    }
    for (clause.increment) |item| {
        const column = binder.relationalColumnForField(schema, item.field, null) orelse return error.InvalidSqlCatalog;
        operations[initialized] = .{
            .op = .inc,
            .path = try alloc.dupe(u8, column.path),
            .value_json = try alloc.dupe(u8, item.value_json),
        };
        initialized += 1;
    }
    for (clause.json_set) |item| {
        const value_json = item.value_json orelse continue;
        const column = binder.relationalColumnForField(schema, item.field, null) orelse return error.InvalidSqlCatalog;
        operations[initialized] = .{
            .op = .set,
            .path = try jsonSetTypedTransformPathAlloc(alloc, column.path, item.path),
            .value_json = try alloc.dupe(u8, value_json),
        };
        initialized += 1;
    }
    for (clause.array_update) |item| {
        const column = binder.relationalColumnForField(schema, item.field, null) orelse return error.InvalidSqlCatalog;
        operations[initialized] = .{
            .op = item.op,
            .path = try alloc.dupe(u8, column.path),
            .value_json = try alloc.dupe(u8, item.value_json),
        };
        initialized += 1;
    }
    return operations;
}

pub fn insertSourceConflictExpressionAssignmentsFromClauseAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    values: []const FieldExpressionValue,
) ![]const db_mod.types.RelationalRowsExpressionAssignment {
    if (values.len == 0) return &.{};
    const out = try alloc.alloc(db_mod.types.RelationalRowsExpressionAssignment, values.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |value| {
            alloc.free(@constCast(value.field));
            freeExpression(alloc, value.expression);
        }
        alloc.free(out);
    }
    for (values) |value| {
        const column = binder.relationalColumnForField(schema, value.field, null) orelse return error.InvalidSqlCatalog;
        out[initialized] = .{
            .field = try alloc.dupe(u8, column.path),
            .expression = try cloneExpressionAlloc(alloc, value.expression),
        };
        initialized += 1;
    }
    return out;
}

pub fn insertSourceConflictJsonSetExpressionAssignmentsFromClauseAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    values: []const JsonSetValue,
) ![]const db_mod.types.RelationalRowsJsonSetExpressionAssignment {
    const count = insertSourceExpressionJsonSetCount(values);
    if (count == 0) return &.{};
    const out = try alloc.alloc(db_mod.types.RelationalRowsJsonSetExpressionAssignment, count);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |value| {
            alloc.free(@constCast(value.field));
            for (value.path) |segment| alloc.free(@constCast(segment));
            if (value.path.len > 0) alloc.free(value.path);
            freeExpression(alloc, value.expression);
        }
        alloc.free(out);
    }
    for (values) |value| {
        const expression = value.expression orelse continue;
        const column = binder.relationalColumnForField(schema, value.field, null) orelse return error.InvalidSqlCatalog;
        out[initialized] = .{
            .field = try alloc.dupe(u8, column.path),
            .path = try strings.cloneStringSlice(alloc, value.path),
            .expression = try cloneExpressionAlloc(alloc, expression),
        };
        initialized += 1;
    }
    return out;
}

pub fn appendJoinedMutationInPredicate(
    alloc: std.mem.Allocator,
    in_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsInPredicate),
    field: []const u8,
    column: runtime_schema.RelationalColumn,
    values_json: []const u8,
    negated: bool,
) !void {
    if (column.field_type == .array or column.field_type == .json) return error.InvalidSqlCatalog;
    try sql_value.validateSqlScalarValuesJson(alloc, column, values_json);
    const owned_field = try alloc.dupe(u8, field);
    var field_transferred = false;
    errdefer if (!field_transferred) alloc.free(owned_field);
    try in_predicates.append(alloc, .{
        .field = owned_field,
        .values_json = values_json,
        .negated = negated,
    });
    field_transferred = true;
}

pub fn appendJsonObjectConcatSetValuesAlloc(
    alloc: std.mem.Allocator,
    field: []const u8,
    object_json: []const u8,
    json_set: *std.ArrayListUnmanaged(JsonSetValue),
) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, object_json, .{}) catch return error.UnsupportedSqlShape;
    defer parsed.deinit();
    if (parsed.value != .object) return error.UnsupportedSqlShape;
    if (parsed.value.object.count() == 0) return error.UnsupportedSqlShape;

    var it = parsed.value.object.iterator();
    while (it.next()) |entry| {
        if (std.mem.indexOfScalar(u8, entry.key_ptr.*, '.') != null) return error.UnsupportedSqlShape;
        const owned_field = try alloc.dupe(u8, field);
        var field_transferred = false;
        errdefer if (!field_transferred) alloc.free(owned_field);
        const path = try alloc.alloc([]const u8, 1);
        var path_transferred = false;
        errdefer if (!path_transferred) alloc.free(path);
        path[0] = try alloc.dupe(u8, entry.key_ptr.*);
        var path_item_transferred = false;
        errdefer if (!path_item_transferred) alloc.free(path[0]);
        const value_json = try std.json.Stringify.valueAlloc(alloc, entry.value_ptr.*, .{});
        var value_transferred = false;
        errdefer if (!value_transferred) alloc.free(value_json);
        try json_set.append(alloc, .{
            .field = owned_field,
            .path = path,
            .value_json = value_json,
        });
        field_transferred = true;
        path_transferred = true;
        path_item_transferred = true;
        value_transferred = true;
    }
}

pub const ConflictAction = enum {
    nothing,
    update,
};

pub const ConflictTarget = union(enum) {
    primary,
    unique: UniqueConflictTarget,
};

pub const UniqueConflictTarget = struct {
    name: []const u8,
    where_json: []const u8 = "",
    where_expressions: []const db_mod.types.RelationalRowsExpressionCondition = &.{},
};

pub fn parseConflictTargetAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    table_name: []const u8,
    hooks: ConflictTargetParserHooks,
) !ConflictTarget {
    const cursor = parser.Cursor.init(tokens, pos);
    if (cursor.matchKeyword("on")) {
        try cursor.expectKeyword("constraint");
        const constraint_name = try grammar.parseIdentifierOwnedAlloc(alloc, tokens, pos);
        defer alloc.free(constraint_name);
        return try conflictTargetForNamedConstraintAlloc(alloc, schema, table_name, constraint_name);
    }

    try cursor.expectToken(.lparen);
    var columns = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (columns.items) |column| alloc.free(column);
        columns.deinit(alloc);
    }
    var expressions = std.ArrayListUnmanaged(runtime_schema.UniqueExpression).empty;
    defer {
        clearDdlUniqueExpressions(alloc, expressions.items);
        expressions.deinit(alloc);
    }
    while (true) {
        if (grammar.peekDdlIndexElementExpression(tokens, pos.*, true)) {
            const wrapper_count = grammar.consumeDdlIndexExpressionWrappers(tokens, pos);
            const expression = try parseConflictTargetUniqueExpressionAlloc(alloc, tokens, pos, schema);
            var expression_transferred = false;
            errdefer if (!expression_transferred) freeDdlUniqueExpression(alloc, expression);
            try expressions.append(alloc, expression);
            expression_transferred = true;
            try grammar.closeDdlIndexExpressionWrappers(tokens, pos, wrapper_count);
        } else {
            const column = try grammar.parseIdentifierOwnedAlloc(alloc, tokens, pos);
            var column_transferred = false;
            errdefer if (!column_transferred) alloc.free(column);
            if (cursor.peekKind(.lparen)) return error.UnsupportedSqlShape;
            if (binder.relationalColumnForField(schema, column, null) == null) return error.InvalidSqlCatalog;
            try columns.append(alloc, column);
            column_transferred = true;
        }
        if (cursor.matchToken(.comma) == null) break;
    }
    try cursor.expectToken(.rparen);
    if (columns.items.len == 0 and expressions.items.len == 0) return error.UnsupportedSqlShape;
    try validateSqlInsertColumnsUnique(schema, columns.items);
    try lower_expr.validateSqlUniqueExpressionListUnique(expressions.items);

    var where_json: []const u8 = "";
    defer if (where_json.len > 0) alloc.free(where_json);
    var where_expressions: []const db_mod.types.RelationalRowsExpressionCondition = &.{};
    defer {
        freeExpressionConditions(alloc, where_expressions);
        if (where_expressions.len > 0) alloc.free(where_expressions);
    }
    if (cursor.matchKeyword("where")) {
        const where_start = cursor.checkpoint();
        where_json = grammar.parseDdlUniquePredicateWhereJsonAlloc(alloc, tokens, pos, schema.relational_columns) catch |err| switch (err) {
            error.UnsupportedSqlShape, error.InvalidSqlCatalog => blk: {
                cursor.restore(where_start);
                break :blk "";
            },
            else => return err,
        };
        if (where_json.len == 0) {
            where_expressions = try hooks.parse_where_expression_conditions(hooks.ptr);
        }
    }

    if (expressions.items.len == 0 and binder.columnsMatchPrimaryKey(schema.primary_key.?, columns.items)) {
        if (where_json.len > 0 or where_expressions.len > 0) return error.UnsupportedSqlShape;
        return .primary;
    }

    const constraint = try lower_expr.findUniqueConstraintByColumnsExpressionsAndConflictWhere(alloc, schema, columns.items, expressions.items, where_json, where_expressions) orelse return error.InvalidSqlCatalog;
    return try conflictTargetForUniqueConstraintAlloc(alloc, constraint);
}

fn conflictTargetForNamedConstraintAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    table_name: []const u8,
    constraint_name: []const u8,
) !ConflictTarget {
    if (try namedConstraintIsPrimaryKeyAlloc(alloc, schema, table_name, constraint_name)) return .primary;

    const constraint = binder.findUniqueConstraintByName(schema, constraint_name) orelse return error.InvalidSqlCatalog;
    return try conflictTargetForUniqueConstraintAlloc(alloc, constraint);
}

fn conflictTargetForUniqueConstraintAlloc(
    alloc: std.mem.Allocator,
    constraint: runtime_schema.UniqueConstraint,
) !ConflictTarget {
    const name = try alloc.dupe(u8, constraint.name);
    errdefer alloc.free(name);
    const where_json = try grammar.uniquePredicateWhereJsonAlloc(alloc, constraint.where);
    errdefer if (where_json.len > 0) alloc.free(where_json);
    const where_expressions = try cloneExpressionConditionsAlloc(alloc, constraint.where_expressions);
    errdefer {
        freeExpressionConditions(alloc, where_expressions);
        if (where_expressions.len > 0) alloc.free(where_expressions);
    }
    return .{ .unique = .{
        .name = name,
        .where_json = where_json,
        .where_expressions = where_expressions,
    } };
}

pub fn namedConstraintIsPrimaryKeyAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    table_name: []const u8,
    constraint_name: []const u8,
) !bool {
    if (schema.primary_key) |primary_key| {
        if (primary_key.name) |name| {
            if (std.mem.eql(u8, constraint_name, name)) return true;
        }
    }
    const default_primary_name = try std.fmt.allocPrint(alloc, "{s}_pkey", .{table_name});
    defer alloc.free(default_primary_name);
    return std.mem.eql(u8, constraint_name, default_primary_name);
}

fn validateConflictTargetUniqueExpression(
    schema: runtime_schema.TableSchema,
    expression: runtime_schema.UniqueExpression,
) !void {
    if (expression.op == .expression) {
        const row_expression = expression.expression orelse return error.InvalidSqlCatalog;
        try lower_expr.validateCheckExpressionForColumns(schema.relational_columns, row_expression);
        if (!lower_expr.rowExpressionDeterministic(row_expression)) return error.InvalidSqlCatalog;
        if (!lower_expr.checkExpressionTypeOrderable(try lower_expr.checkExpressionTypeForColumns(schema.relational_columns, row_expression))) return error.InvalidSqlCatalog;
    } else if (binder.relationalColumnForField(schema, expression.field, null) == null) {
        return error.InvalidSqlCatalog;
    }
}

fn parseConflictTargetUniqueExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
) !runtime_schema.UniqueExpression {
    const expression = try grammar.parseDdlUniqueExpressionAlloc(alloc, tokens, pos);
    errdefer freeDdlUniqueExpression(alloc, expression);
    try validateConflictTargetUniqueExpression(schema, expression);
    return expression;
}

pub const ConflictClause = struct {
    target: ConflictTarget,
    action: ConflictAction,
    patch: []const FieldJsonValue = &.{},
    patch_expr: []const FieldExpressionValue = &.{},
    increment: []const FieldJsonValue = &.{},
    increment_expr: []const FieldExpressionValue = &.{},
    json_set: []const JsonSetValue = &.{},
    array_update: []const ArrayTransformValue = &.{},
    where_expression: ?db_mod.types.RelationalRowsExpressionCondition = null,
    where_expressions: []const db_mod.types.RelationalRowsExpressionCondition = &.{},
    where_any: []const db_mod.types.RelationalRowsExpressionPredicateGroup = &.{},
    where_not: []const db_mod.types.RelationalRowsExpressionPredicateGroup = &.{},
};

pub const MergeParsedAssignment = union(enum) {
    mapping: MergeFieldMapping,
    expression: MergeExpressionAssignment,
};

pub const ParsedExistsSemiJoinMutationTail = struct {
    source_table: TableAlias,
    tail: ParsedJoinedMutationTail,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        freeTableAlias(alloc, self.source_table);
        self.tail.deinit(alloc);
        self.* = undefined;
    }
};

pub const SemiJoinSource = struct {
    table: TableAlias,
    fields: []const []const u8,
};

pub fn parseExistsSemiJoinSourceTableAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !TableAlias {
    var saw_select_item = false;
    var depth: usize = 0;
    while (!parser.atEnd(tokens, pos.*)) {
        if (depth == 0 and parser.peekKeyword(tokens, pos.*, "from")) break;
        const token = tokens[pos.*];
        switch (token.kind) {
            .lparen => depth += 1,
            .rparen => {
                if (depth == 0) return error.UnsupportedSqlShape;
                depth -= 1;
            },
            .semicolon => return error.UnsupportedSqlShape,
            else => {},
        }
        saw_select_item = true;
        pos.* += 1;
    }
    if (!saw_select_item) return error.UnsupportedSqlShape;
    try parser.expectKeyword(tokens, pos, "from");
    return try plan_mod.parseTableAliasAlloc(alloc, tokens, pos);
}

pub fn parseSemiJoinTargetFieldsAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    target_qualifiers: []const []const u8,
    hooks: SemiJoinTargetFieldsParserHooks,
) ![]const []const u8 {
    var fields = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (fields.items) |field| alloc.free(field);
        fields.deinit(alloc);
    }

    if (parser.matchToken(tokens, pos, .lparen) != null) {
        while (true) {
            const parsed_target_field = try hooks.parse_field_expression_owned(hooks.ptr);
            defer alloc.free(parsed_target_field);
            try binder.appendSemiJoinTargetFieldAlloc(alloc, &fields, schema, parsed_target_field, target_qualifiers);
            if (parser.matchToken(tokens, pos, .comma) == null) break;
        }
        try parser.expectToken(tokens, pos, .rparen);
    } else {
        const parsed_target_field = try hooks.parse_field_expression_owned(hooks.ptr);
        defer alloc.free(parsed_target_field);
        try binder.appendSemiJoinTargetFieldAlloc(alloc, &fields, schema, parsed_target_field, target_qualifiers);
    }
    if (fields.items.len == 0) return error.UnsupportedSqlShape;
    return try fields.toOwnedSlice(alloc);
}

pub const ParsedSemiJoinSourceQuery = struct {
    query: db_mod.types.RelationalRowsQueryRequest = .{},
    on: []const db_mod.types.RelationalRowsJoinOn = &.{},
    match_expression_predicates: []const db_mod.types.RelationalRowsExpressionCondition = &.{},
    match_expression_or_predicates: []const db_mod.types.RelationalRowsExpressionPredicateGroup = &.{},
    match_expression_not_predicates: []const db_mod.types.RelationalRowsExpressionPredicateGroup = &.{},
    match_expression_array_contains: []const db_mod.types.RelationalRowsExpressionArrayContainsPredicate = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        self.query.deinit(alloc);
        freeJoinOn(alloc, self.on);
        if (self.on.len > 0) alloc.free(self.on);
        freeExpressionConditions(alloc, self.match_expression_predicates);
        if (self.match_expression_predicates.len > 0) alloc.free(self.match_expression_predicates);
        freeExpressionPredicateGroups(alloc, self.match_expression_or_predicates);
        if (self.match_expression_or_predicates.len > 0) alloc.free(self.match_expression_or_predicates);
        freeExpressionPredicateGroups(alloc, self.match_expression_not_predicates);
        if (self.match_expression_not_predicates.len > 0) alloc.free(self.match_expression_not_predicates);
        freeExpressionArrayContains(alloc, self.match_expression_array_contains);
        if (self.match_expression_array_contains.len > 0) alloc.free(self.match_expression_array_contains);
        self.* = undefined;
    }
};

pub const ParsedJoinedMutationTail = struct {
    join: db_mod.types.RelationalRowsJoinRequest,
    match_expression_predicates: []const db_mod.types.RelationalRowsExpressionCondition = &.{},
    match_expression_or_predicates: []const db_mod.types.RelationalRowsExpressionPredicateGroup = &.{},
    match_expression_not_predicates: []const db_mod.types.RelationalRowsExpressionPredicateGroup = &.{},
    match_expression_array_contains: []const db_mod.types.RelationalRowsExpressionArrayContainsPredicate = &.{},
    returning: ReturningProjection = .{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        self.join.deinit(alloc);
        freeExpressionConditions(alloc, self.match_expression_predicates);
        if (self.match_expression_predicates.len > 0) alloc.free(self.match_expression_predicates);
        freeExpressionPredicateGroups(alloc, self.match_expression_or_predicates);
        if (self.match_expression_or_predicates.len > 0) alloc.free(self.match_expression_or_predicates);
        freeExpressionPredicateGroups(alloc, self.match_expression_not_predicates);
        if (self.match_expression_not_predicates.len > 0) alloc.free(self.match_expression_not_predicates);
        freeExpressionArrayContains(alloc, self.match_expression_array_contains);
        if (self.match_expression_array_contains.len > 0) alloc.free(self.match_expression_array_contains);
        self.returning.deinit(alloc);
    }
};

pub fn resolveJoinedMutationSourceForCtesAlloc(
    alloc: std.mem.Allocator,
    tail: *ParsedJoinedMutationTail,
    source_table_name: []const u8,
    ctes: []const db_mod.types.RelationalRowsCte,
    base_table_name: ?*?[]const u8,
) ![]const u8 {
    const base_ptr = base_table_name orelse return source_table_name;
    if (plan_mod.findCteByName(ctes, source_table_name) != null) {
        if (tail.join.right.source_cte.len != 0) return error.UnsupportedSqlShape;
        tail.join.right.source_cte = try alloc.dupe(u8, source_table_name);
        return base_ptr.* orelse return error.UnsupportedSqlShape;
    }
    if (base_ptr.*) |base| {
        if (!std.mem.eql(u8, base, source_table_name)) return error.UnsupportedSqlShape;
    } else {
        base_ptr.* = try alloc.dupe(u8, source_table_name);
    }
    return source_table_name;
}

pub const ParsedMutationSourceQuery = struct {
    query: db_mod.types.RelationalRowsQueryRequest,
    returning: ReturningProjection = .{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        self.query.deinit(alloc);
        self.returning.deinit(alloc);
    }
};

pub const ParsedTemporalPortion = struct {
    period: []const u8,
    from_json: []const u8,
    to_json: []const u8,

    pub fn deinit(self: @This(), alloc: std.mem.Allocator) void {
        alloc.free(self.period);
        alloc.free(self.from_json);
        alloc.free(self.to_json);
    }
};

pub const JoinedMutationExpressionSide = union(enum) {
    single: db_mod.types.RelationalRowsJoinProjectionSide,
    mixed,
};

pub fn joinedMutationExpressionSideAt(
    tokens: []const Token,
    pos: usize,
    target_alias: []const u8,
    source_alias: []const u8,
    string_to_array_predicate_is_containment: bool,
) !?JoinedMutationExpressionSide {
    if (!joinedMutationExpressionCanStartAt(tokens, pos, string_to_array_predicate_is_containment)) return null;

    var side: ?db_mod.types.RelationalRowsJoinProjectionSide = null;
    var mixed = false;
    var depth: usize = 0;
    var i = pos;
    while (i < tokens.len) : (i += 1) {
        const token = tokens[i];
        switch (token.kind) {
            .lparen => depth += 1,
            .rparen => {
                if (depth == 0) break;
                depth -= 1;
            },
            .semicolon => if (depth == 0) break,
            .identifier => {
                if (depth == 0 and (std.ascii.eqlIgnoreCase(token.text, "and") or
                    std.ascii.eqlIgnoreCase(token.text, "or") or
                    lower_expr.sqlWhereTailClauseKeyword(token.text)))
                {
                    break;
                }
                if (lower_expr.identifierContainsQualifier(token.text)) {
                    const dot = std.mem.indexOfScalar(u8, token.text, '.') orelse unreachable;
                    const next_side = try binder.joinSideForQualifier(token.text[0..dot], target_alias, source_alias);
                    if (side) |existing| {
                        if (existing != next_side) mixed = true;
                    } else {
                        side = next_side;
                    }
                }
            },
            else => {},
        }
    }
    if (mixed) return .mixed;
    if (side) |single| return .{ .single = single };
    return null;
}

pub fn joinedMutationExpressionCanStartAt(
    tokens: []const Token,
    pos: usize,
    string_to_array_predicate_is_containment: bool,
) bool {
    if (pos >= tokens.len) return false;
    if (tokens[pos].kind == .identifier and pos + 1 < tokens.len and lower_expr.identifierContainsQualifier(tokens[pos].text)) {
        return switch (tokens[pos + 1].kind) {
            .plus, .minus, .star, .slash, .percent, .arrow_json, .arrow_text, .path_arrow_json, .path_arrow_text, .pipe_concat, .question_any, .question_all => true,
            else => false,
        };
    }
    if (lower_expr.expressionPredicateCanStartAt(tokens, pos)) return true;
    if (lower_expr.canParseExpressionNotWhere(tokens, pos)) return true;
    if (parser.peekKeyword(tokens, pos, "string_to_array") and string_to_array_predicate_is_containment) return true;
    if (tokens[pos].kind == .lparen) {
        const inner = parser.predicateStartIndexAfterOpenParens(tokens, pos);
        return lower_expr.expressionPredicateCanStartAt(tokens, inner);
    }
    return false;
}

pub fn sqlCanonicalMutationFieldPath(
    schema: runtime_schema.TableSchema,
    field: []const u8,
) ![]const u8 {
    const column = binder.relationalColumnForField(schema, field, null) orelse return error.InvalidSqlCatalog;
    return column.path;
}

pub fn validateSqlUpdateTargetPaths(
    schema: runtime_schema.TableSchema,
    patch: []const FieldJsonValue,
    patch_expr: []const FieldExpressionValue,
    increment: []const FieldJsonValue,
    increment_expr: []const FieldExpressionValue,
    json_set: []const JsonSetValue,
    array_update: []const ArrayTransformValue,
) !void {
    for (patch, 0..) |lhs, i| {
        try validateSqlFieldDoesNotConflictFieldJson(schema, lhs.field, patch[i + 1 ..]);
        try validateSqlFieldDoesNotConflictFieldExpressions(schema, lhs.field, patch_expr);
        try validateSqlFieldDoesNotConflictFieldJson(schema, lhs.field, increment);
        try validateSqlFieldDoesNotConflictFieldExpressions(schema, lhs.field, increment_expr);
        try validateSqlFieldDoesNotConflictJsonSet(schema, lhs.field, json_set);
        try validateSqlFieldDoesNotConflictArrayUpdates(schema, lhs.field, array_update);
    }
    for (patch_expr, 0..) |lhs, i| {
        try validateSqlFieldDoesNotConflictFieldExpressions(schema, lhs.field, patch_expr[i + 1 ..]);
        try validateSqlFieldDoesNotConflictFieldJson(schema, lhs.field, increment);
        try validateSqlFieldDoesNotConflictFieldExpressions(schema, lhs.field, increment_expr);
        try validateSqlFieldDoesNotConflictJsonSet(schema, lhs.field, json_set);
        try validateSqlFieldDoesNotConflictArrayUpdates(schema, lhs.field, array_update);
    }
    for (increment, 0..) |lhs, i| {
        try validateSqlFieldDoesNotConflictFieldJson(schema, lhs.field, increment[i + 1 ..]);
        try validateSqlFieldDoesNotConflictFieldExpressions(schema, lhs.field, increment_expr);
        try validateSqlFieldDoesNotConflictJsonSet(schema, lhs.field, json_set);
        try validateSqlFieldDoesNotConflictArrayUpdates(schema, lhs.field, array_update);
    }
    for (increment_expr, 0..) |lhs, i| {
        try validateSqlFieldDoesNotConflictFieldExpressions(schema, lhs.field, increment_expr[i + 1 ..]);
        try validateSqlFieldDoesNotConflictJsonSet(schema, lhs.field, json_set);
        try validateSqlFieldDoesNotConflictArrayUpdates(schema, lhs.field, array_update);
    }
    for (json_set, 0..) |lhs, i| {
        for (json_set[i + 1 ..]) |rhs| {
            if (sqlJsonSetValuesConflict(schema, lhs, rhs)) return error.InvalidRowsRequest;
        }
        try validateSqlJsonSetDoesNotConflictArrayUpdates(schema, lhs, array_update);
    }
}

pub fn validateSqlJoinedMutationTargetPaths(
    schema: runtime_schema.TableSchema,
    source_assignments: []const JoinedMutationSourceAssignment,
    patch: []const FieldJsonValue,
    patch_expr: []const FieldExpressionValue,
    json_set: []const JsonSetValue,
) !void {
    for (source_assignments, 0..) |lhs, i| {
        try validateSqlJoinedAssignmentDoesNotConflictAssignments(schema, lhs.field, source_assignments[i + 1 ..]);
        try validateSqlFieldDoesNotConflictFieldJson(schema, lhs.field, patch);
        try validateSqlFieldDoesNotConflictFieldExpressions(schema, lhs.field, patch_expr);
        try validateSqlFieldDoesNotConflictJsonSet(schema, lhs.field, json_set);
    }
    for (patch, 0..) |lhs, i| {
        try validateSqlFieldDoesNotConflictFieldJson(schema, lhs.field, patch[i + 1 ..]);
        try validateSqlFieldDoesNotConflictFieldExpressions(schema, lhs.field, patch_expr);
        try validateSqlFieldDoesNotConflictJsonSet(schema, lhs.field, json_set);
    }
    for (patch_expr, 0..) |lhs, i| {
        try validateSqlFieldDoesNotConflictFieldExpressions(schema, lhs.field, patch_expr[i + 1 ..]);
        try validateSqlFieldDoesNotConflictJsonSet(schema, lhs.field, json_set);
    }
    for (json_set, 0..) |lhs, i| {
        for (json_set[i + 1 ..]) |rhs| {
            if (sqlJsonSetValuesConflict(schema, lhs, rhs)) return error.InvalidRowsRequest;
        }
    }
}

fn validateSqlFieldDoesNotConflictFieldJson(
    schema: runtime_schema.TableSchema,
    field: []const u8,
    values: []const FieldJsonValue,
) !void {
    const lhs = try sqlCanonicalMutationFieldPath(schema, field);
    for (values) |value| {
        if (sqlDottedPathsConflict(lhs, try sqlCanonicalMutationFieldPath(schema, value.field))) return error.InvalidRowsRequest;
    }
}

fn validateSqlFieldDoesNotConflictFieldExpressions(
    schema: runtime_schema.TableSchema,
    field: []const u8,
    values: []const FieldExpressionValue,
) !void {
    const lhs = try sqlCanonicalMutationFieldPath(schema, field);
    for (values) |value| {
        if (sqlDottedPathsConflict(lhs, try sqlCanonicalMutationFieldPath(schema, value.field))) return error.InvalidRowsRequest;
    }
}

fn validateSqlFieldDoesNotConflictJsonSet(
    schema: runtime_schema.TableSchema,
    field: []const u8,
    values: []const JsonSetValue,
) !void {
    const lhs = try sqlCanonicalMutationFieldPath(schema, field);
    for (values) |value| {
        if (sqlDottedPathConflictsJsonSetValue(schema, lhs, value)) return error.InvalidRowsRequest;
    }
}

fn validateSqlFieldDoesNotConflictArrayUpdates(
    schema: runtime_schema.TableSchema,
    field: []const u8,
    values: []const ArrayTransformValue,
) !void {
    const lhs = try sqlCanonicalMutationFieldPath(schema, field);
    for (values) |value| {
        if (sqlDottedPathsConflict(lhs, try sqlCanonicalMutationFieldPath(schema, value.field))) return error.InvalidRowsRequest;
    }
}

fn validateSqlJsonSetDoesNotConflictArrayUpdates(
    schema: runtime_schema.TableSchema,
    value: JsonSetValue,
    values: []const ArrayTransformValue,
) !void {
    for (values) |array_update| {
        if (sqlDottedPathConflictsJsonSetValue(schema, try sqlCanonicalMutationFieldPath(schema, array_update.field), value)) return error.InvalidRowsRequest;
    }
}

fn validateSqlJoinedAssignmentDoesNotConflictAssignments(
    schema: runtime_schema.TableSchema,
    field: []const u8,
    values: []const JoinedMutationSourceAssignment,
) !void {
    const lhs = try sqlCanonicalMutationFieldPath(schema, field);
    for (values) |value| {
        if (sqlDottedPathsConflict(lhs, try sqlCanonicalMutationFieldPath(schema, value.field))) return error.InvalidRowsRequest;
    }
}

fn sqlJsonSetValuesConflict(schema: runtime_schema.TableSchema, lhs: JsonSetValue, rhs: JsonSetValue) bool {
    const lhs_field = sqlCanonicalMutationFieldPath(schema, lhs.field) catch return true;
    const rhs_field = sqlCanonicalMutationFieldPath(schema, rhs.field) catch return true;
    return sqlJsonSetPathsConflict(lhs_field, lhs.path, rhs_field, rhs.path);
}

fn sqlDottedPathConflictsJsonSetValue(schema: runtime_schema.TableSchema, path: []const u8, value: JsonSetValue) bool {
    const json_field = sqlCanonicalMutationFieldPath(schema, value.field) catch return true;
    return sqlDottedPathConflictsJsonSetPath(path, json_field, value.path);
}

pub fn validateSqlInsertColumnsUnique(
    schema: runtime_schema.TableSchema,
    columns: []const []const u8,
) !void {
    for (columns, 0..) |lhs, i| {
        const lhs_path = try sqlCanonicalMutationFieldPath(schema, lhs);
        for (columns[i + 1 ..]) |rhs| {
            if (sqlDottedPathsConflict(lhs_path, try sqlCanonicalMutationFieldPath(schema, rhs))) return error.UnsupportedSqlShape;
        }
    }
}

pub fn mergeExpressionUsesTargetRow(expression: runtime_schema.RelationalRowsExpression) bool {
    if (expression.kind == .field and expression.field_source != .source) return true;
    for (expression.operands) |operand| {
        if (mergeExpressionUsesTargetRow(operand)) return true;
    }
    for (expression.case_branches) |branch| {
        if (mergeExpressionConditionUsesTargetRow(branch.when)) return true;
        if (mergeExpressionUsesTargetRow(branch.then)) return true;
    }
    for (expression.case_else) |case_else| {
        if (mergeExpressionUsesTargetRow(case_else)) return true;
    }
    return false;
}

pub fn mergeExpressionConditionUsesTargetRow(condition: runtime_schema.RelationalRowsExpressionCondition) bool {
    if (mergeExpressionUsesTargetRow(condition.lhs)) return true;
    for (condition.rhs) |rhs| {
        if (mergeExpressionUsesTargetRow(rhs)) return true;
    }
    return false;
}

pub fn mergeExpressionPredicateGroupsUseTargetRow(groups: []const runtime_schema.RelationalRowsExpressionPredicateGroup) bool {
    for (groups) |group| {
        for (group.conditions) |condition| {
            if (mergeExpressionConditionUsesTargetRow(condition)) return true;
        }
    }
    return false;
}

pub fn updateWillLookupExistingRow(schema: runtime_schema.TableSchema, returning: ReturningProjection) bool {
    if (returning.hasProjection() or schema.checks.len > 0) return true;
    for (schema.relational_columns) |column| {
        if (column.generated != null) return true;
    }
    return false;
}

pub fn validateMergeAssignmentsUnique(
    mappings: []const MergeFieldMapping,
    expressions: []const MergeExpressionAssignment,
) !void {
    for (mappings, 0..) |mapping, i| {
        for (mappings[i + 1 ..]) |other| {
            if (std.mem.eql(u8, mapping.target_field, other.target_field)) return error.UnsupportedSqlShape;
        }
        for (expressions) |expression| {
            if (std.mem.eql(u8, mapping.target_field, expression.target_field)) return error.UnsupportedSqlShape;
        }
    }
    for (expressions, 0..) |expression, i| {
        for (expressions[i + 1 ..]) |other| {
            if (std.mem.eql(u8, expression.target_field, other.target_field)) return error.UnsupportedSqlShape;
        }
    }
}

pub fn validateJoinedMutationSourceAssignments(
    assignments: []const JoinedMutationSourceAssignment,
    source_alias: []const u8,
) !void {
    for (assignments) |assignment| {
        if (!std.mem.eql(u8, assignment.source_qualifier, source_alias)) return error.UnsupportedSqlShape;
    }
}

pub fn arrayTransformOpToken(op: db_mod.types.TransformOpType) []const u8 {
    return switch (op) {
        .push => "append",
        .pull => "remove",
        .add_to_set => "add_to_set",
        else => unreachable,
    };
}

pub fn updateBodyJsonAlloc(
    alloc: std.mem.Allocator,
    where_json: []const u8,
    patch: []const FieldJsonValue,
    patch_expr: []const FieldExpressionValue,
    increment: []const FieldJsonValue,
    increment_expr: []const FieldExpressionValue,
    json_set: []const JsonSetValue,
    array_update: []const ArrayTransformValue,
    returning: ReturningProjection,
    expected_version: ?u64,
    rewrite_identity: bool,
) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.print("{{\"operations\":[{{\"op\":{f},\"where\":", .{std.json.fmt(if (rewrite_identity) "rewrite_identity" else "update", .{})});
    try writer.writeAll(where_json);
    if (expected_version) |version| try writer.print(",\"expected_version\":{d}", .{version});
    try writeMutationAssignmentsJson(writer, patch, patch_expr, increment, increment_expr, json_set, array_update);
    try writeReturningProjectionJson(writer, returning);
    try writer.writeAll("}]}");
    return try out.toOwnedSlice();
}

pub fn deleteBodyJsonAlloc(
    alloc: std.mem.Allocator,
    where_json: []const u8,
    returning: ReturningProjection,
    expected_version: ?u64,
) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.writeAll("{\"operations\":[{\"op\":\"delete\",\"where\":");
    try writer.writeAll(where_json);
    if (expected_version) |version| try writer.print(",\"expected_version\":{d}", .{version});
    try writeReturningProjectionJson(writer, returning);
    try writer.writeAll("}]}");
    return try out.toOwnedSlice();
}

pub fn insertBodyJsonAlloc(
    alloc: std.mem.Allocator,
    columns: []const []const u8,
    rows: InsertValueRows,
    conflicts: []const ConflictClause,
    returning: ReturningProjection,
) ![]u8 {
    if (rows.len == 0) return error.UnsupportedSqlShape;
    if (conflicts.len != 0 and conflicts.len != rows.len) return error.UnsupportedSqlShape;
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.writeAll("{\"operations\":[");
    for (rows, 0..) |values, row_i| {
        if (values.len != columns.len) return error.UnsupportedSqlShape;
        if (row_i != 0) try writer.writeByte(',');
        try writer.writeAll("{\"op\":\"insert\",\"row\":{");
        for (columns, values, 0..) |column, value_json, i| {
            if (i != 0) try writer.writeByte(',');
            try writer.print("{f}:", .{std.json.fmt(column, .{})});
            try writer.writeAll(value_json);
        }
        try writer.writeByte('}');
        if (conflicts.len != 0) {
            const clause = conflicts[row_i];
            try writer.writeAll(",\"on_conflict\":{\"target\":");
            try writeConflictTargetJson(writer, clause.target);
            try writer.print(",\"action\":{f}", .{std.json.fmt(conflictActionToken(clause.action), .{})});
            try writeMutationAssignmentsJson(writer, clause.patch, clause.patch_expr, clause.increment, clause.increment_expr, clause.json_set, clause.array_update);
            if (clause.where_expression) |condition| {
                try writer.writeAll(",\"where_expression\":");
                try lower_expr.writeRowExpressionConditionJson(writer, condition);
            }
            if (clause.where_expressions.len > 0) {
                try writer.writeAll(",\"where_expressions\":[");
                for (clause.where_expressions, 0..) |condition, i| {
                    if (i != 0) try writer.writeByte(',');
                    try lower_expr.writeRowExpressionConditionJson(writer, condition);
                }
                try writer.writeByte(']');
            }
            if (clause.where_any.len > 0) {
                try writer.writeByte(',');
                try lower_expr.writeRowExpressionPredicateGroupsJson(writer, "where_any", clause.where_any);
            }
            if (clause.where_not.len > 0) {
                try writer.writeByte(',');
                try lower_expr.writeRowExpressionPredicateGroupsJson(writer, "where_not", clause.where_not);
            }
            try writer.writeByte('}');
        }
        try writeReturningProjectionJson(writer, returning);
        try writer.writeByte('}');
    }
    try writer.writeAll("]}");
    return try out.toOwnedSlice();
}

pub fn writeConflictTargetJson(writer: *std.Io.Writer, target: ConflictTarget) !void {
    switch (target) {
        .primary => try writer.writeAll("{\"primary\":true}"),
        .unique => |unique| {
            try writer.print("{{\"unique\":{{\"name\":{f}", .{std.json.fmt(unique.name, .{})});
            if (unique.where_json.len > 0) {
                try writer.writeAll(",\"where\":");
                try writer.writeAll(unique.where_json);
            }
            if (unique.where_expressions.len > 0) {
                try writer.writeAll(",\"where_expressions\":[");
                for (unique.where_expressions, 0..) |condition, i| {
                    if (i != 0) try writer.writeByte(',');
                    try lower_expr.writeRowExpressionConditionJson(writer, condition);
                }
                try writer.writeByte(']');
            }
            try writer.writeAll("}}");
        },
    }
}

pub fn mutationSourceBodyJsonAlloc(
    alloc: std.mem.Allocator,
    op: []const u8,
    source: db_mod.types.RelationalRowsQueryRequest,
    rewrite_identity: bool,
    temporal_portion: ?ParsedTemporalPortion,
    patch: []const FieldJsonValue,
    patch_expr: []const FieldExpressionValue,
    increment: []const FieldJsonValue,
    increment_expr: []const FieldExpressionValue,
    json_set: []const JsonSetValue,
    array_update: []const ArrayTransformValue,
    returning: ReturningProjection,
) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.print("{{\"op\":{f},\"source\":", .{std.json.fmt(op, .{})});
    try writeMutationSourceQueryJson(writer, source);
    if (rewrite_identity) try writer.writeAll(",\"rewrite_identity\":true");
    if (temporal_portion) |portion| {
        try writer.print(",\"temporal_portion\":{{\"period\":{f},\"from\":", .{std.json.fmt(portion.period, .{})});
        try writer.writeAll(portion.from_json);
        try writer.writeAll(",\"to\":");
        try writer.writeAll(portion.to_json);
        try writer.writeByte('}');
    }
    try writeMutationAssignmentsJson(writer, patch, patch_expr, increment, increment_expr, json_set, array_update);
    try writeReturningProjectionJson(writer, returning);
    try writer.writeByte('}');
    return try out.toOwnedSlice();
}

pub fn joinedMutationSourceBodyJsonAlloc(
    alloc: std.mem.Allocator,
    op: []const u8,
    source_table: []const u8,
    ctes: []const db_mod.types.RelationalRowsCte,
    tail: ParsedJoinedMutationTail,
    rewrite_identity: bool,
    source_assignments: []const JoinedMutationSourceAssignment,
    patch: []const FieldJsonValue,
    patch_expr: []const FieldExpressionValue,
    json_set: []const JsonSetValue,
) ![]u8 {
    const join = tail.join;
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.print("{{\"op\":{f},\"source_table\":{f},\"target_side\":\"left\"", .{
        std.json.fmt(op, .{}),
        std.json.fmt(source_table, .{}),
    });
    if (ctes.len > 0) {
        try writer.writeAll(",\"ctes\":[");
        for (ctes, 0..) |cte, i| {
            if (i != 0) try writer.writeByte(',');
            try writer.print("{{\"name\":{f},\"query\":", .{std.json.fmt(cte.name, .{})});
            try writeMutationSourceQueryJson(writer, cte.query);
            if (cte.max_rows) |max_rows| try writer.print(",\"max_rows\":{d}", .{max_rows});
            if (cte.max_bytes) |max_bytes| try writer.print(",\"max_bytes\":{d}", .{max_bytes});
            if (cte.spill_after_bytes) |spill_after_bytes| try writer.print(",\"spill_after_bytes\":{d}", .{spill_after_bytes});
            try writer.writeByte('}');
        }
        try writer.writeByte(']');
    }
    try writer.writeAll(",\"join\":{\"left\":");
    try writeMutationSourceQueryJson(writer, join.left);
    try writer.writeAll(",\"right\":");
    try writeMutationSourceQueryJson(writer, join.right);
    try writer.writeAll(",\"on\":[");
    for (join.on, 0..) |predicate, i| {
        if (i != 0) try writer.writeByte(',');
        try writer.print("{{\"left_field\":{f},\"right_field\":{f}}}", .{
            std.json.fmt(predicate.left_field, .{}),
            std.json.fmt(predicate.right_field, .{}),
        });
    }
    try writer.writeByte(']');
    if (join.order_by.len > 0) {
        try writer.writeAll(",\"order_by\":[");
        for (join.order_by, 0..) |order, i| {
            if (i != 0) try writer.writeByte(',');
            if (order.expression != null) return error.UnsupportedSqlShape;
            try writer.print("{{\"field\":{f}", .{std.json.fmt(order.field, .{})});
            if (order.direction == .desc) try writer.writeAll(",\"direction\":\"desc\"");
            try writer.writeByte('}');
        }
        try writer.writeByte(']');
    }
    if (join.limit) |limit| try writer.print(",\"limit\":{d}", .{limit});
    if (join.offset != 0) try writer.print(",\"offset\":{d}", .{join.offset});
    try writer.writeByte('}');
    if (tail.match_expression_predicates.len > 0) {
        try writer.writeAll(",\"match_expression_where\":[");
        for (tail.match_expression_predicates, 0..) |condition, i| {
            if (i != 0) try writer.writeByte(',');
            try lower_expr.writeRowExpressionConditionJson(writer, condition);
        }
        try writer.writeByte(']');
    }
    if (tail.match_expression_or_predicates.len > 0) {
        try writer.writeByte(',');
        try lower_expr.writeRowExpressionPredicateGroupsJson(writer, "match_expression_any", tail.match_expression_or_predicates);
    }
    if (tail.match_expression_not_predicates.len > 0) {
        try writer.writeByte(',');
        try lower_expr.writeRowExpressionPredicateGroupsJson(writer, "match_expression_not", tail.match_expression_not_predicates);
    }
    if (tail.match_expression_array_contains.len > 0) {
        try writer.writeAll(",\"match_expression_array_contains\":[");
        for (tail.match_expression_array_contains, 0..) |predicate, i| {
            if (i != 0) try writer.writeByte(',');
            try writer.writeAll("{\"expr\":");
            try lower_expr.writeRowExpressionJson(writer, predicate.expression);
            try writer.writeAll(",\"value\":");
            try writer.writeAll(predicate.value_json);
            try writer.writeByte('}');
        }
        try writer.writeByte(']');
    }
    if (rewrite_identity) try writer.writeAll(",\"rewrite_identity\":true");
    if (source_assignments.len > 0) {
        try writer.writeAll(",\"source_assignments\":[");
        for (source_assignments, 0..) |assignment, i| {
            if (i != 0) try writer.writeByte(',');
            try writer.print("{{\"target_field\":{f},\"side\":{f},\"field\":{f}}}", .{
                std.json.fmt(assignment.field, .{}),
                std.json.fmt("right", .{}),
                std.json.fmt(assignment.source_field, .{}),
            });
        }
        try writer.writeByte(']');
    }
    try writeMutationAssignmentsJson(writer, patch, patch_expr, &.{}, &.{}, json_set, &.{});
    try writeReturningProjectionJson(writer, tail.returning);
    try writer.writeByte('}');
    return try out.toOwnedSlice();
}

fn writeMutationAssignmentsJson(
    writer: *std.Io.Writer,
    patch: []const FieldJsonValue,
    patch_expr: []const FieldExpressionValue,
    increment: []const FieldJsonValue,
    increment_expr: []const FieldExpressionValue,
    json_set: []const JsonSetValue,
    array_update: []const ArrayTransformValue,
) !void {
    if (patch.len > 0) {
        try writer.writeAll(",\"patch\":{");
        for (patch, 0..) |item, i| {
            if (i != 0) try writer.writeByte(',');
            try writer.print("{f}:", .{std.json.fmt(item.field, .{})});
            try writer.writeAll(item.value_json);
        }
        try writer.writeByte('}');
    }
    if (patch_expr.len > 0) {
        try writer.writeAll(",\"patch_expr\":{");
        for (patch_expr, 0..) |item, i| {
            if (i != 0) try writer.writeByte(',');
            try writer.print("{f}:", .{std.json.fmt(item.field, .{})});
            try lower_expr.writeRowExpressionJson(writer, item.expression);
        }
        try writer.writeByte('}');
    }
    if (increment.len > 0) {
        try writer.writeAll(",\"increment\":{");
        for (increment, 0..) |item, i| {
            if (i != 0) try writer.writeByte(',');
            try writer.print("{f}:", .{std.json.fmt(item.field, .{})});
            try writer.writeAll(item.value_json);
        }
        try writer.writeByte('}');
    }
    if (increment_expr.len > 0) {
        try writer.writeAll(",\"increment_expr\":{");
        for (increment_expr, 0..) |item, i| {
            if (i != 0) try writer.writeByte(',');
            try writer.print("{f}:", .{std.json.fmt(item.field, .{})});
            try lower_expr.writeRowExpressionJson(writer, item.expression);
        }
        try writer.writeByte('}');
    }
    if (json_set.len > 0) {
        try writeJsonSetValuesJson(writer, json_set);
    }
    if (array_update.len > 0) {
        try writer.writeAll(",\"array_update\":[");
        for (array_update, 0..) |item, i| {
            if (i != 0) try writer.writeByte(',');
            try writer.print("{{\"field\":{f},\"op\":{f},\"value\":", .{
                std.json.fmt(item.field, .{}),
                std.json.fmt(arrayTransformOpToken(item.op), .{}),
            });
            try writer.writeAll(item.value_json);
            try writer.writeByte('}');
        }
        try writer.writeByte(']');
    }
}

pub fn writeJsonSetValuesJson(writer: *std.Io.Writer, json_set: []const JsonSetValue) !void {
    try writer.writeAll(",\"json_set\":[");
    for (json_set, 0..) |item, i| {
        if (i != 0) try writer.writeByte(',');
        try writer.print("{{\"field\":{f},\"path\":[", .{std.json.fmt(item.field, .{})});
        for (item.path, 0..) |segment, segment_i| {
            if (segment_i != 0) try writer.writeByte(',');
            try writer.print("{f}", .{std.json.fmt(segment, .{})});
        }
        try writer.writeByte(']');
        if (item.expression) |expression| {
            try writer.writeAll(",\"expr\":");
            try lower_expr.writeRowExpressionJson(writer, expression);
        } else if (item.value_json) |value_json| {
            try writer.writeAll(",\"value\":");
            try writer.writeAll(value_json);
        } else {
            return error.UnsupportedSqlShape;
        }
        try writer.writeByte('}');
    }
    try writer.writeByte(']');
}

pub fn writeMutationSourceQueryJson(
    writer: *std.Io.Writer,
    source: db_mod.types.RelationalRowsQueryRequest,
) !void {
    try writer.writeByte('{');
    var wrote = false;
    if (source.source_cte.len > 0) {
        try writer.print("\"source_cte\":{f}", .{std.json.fmt(source.source_cte, .{})});
        wrote = true;
    }
    if (!source.select_all) {
        if (source.select.len == 0) return error.UnsupportedSqlShape;
        if (wrote) try writer.writeByte(',');
        try writer.writeAll("\"select\":[");
        for (source.select, 0..) |field, i| {
            if (i != 0) try writer.writeByte(',');
            try writer.print("{f}", .{std.json.fmt(field, .{})});
        }
        try writer.writeByte(']');
        wrote = true;
    }
    if (source.predicates.len > 0 or
        source.array_contains.len > 0 or
        source.array_eq.len > 0 or
        source.array_any.len > 0 or
        source.in_predicates.len > 0 or
        source.json_contains.len > 0 or
        source.json_path_eq.len > 0 or
        source.json_path_exists.len > 0 or
        source.text_patterns.len > 0 or
        source.or_predicates.len > 0 or
        source.not_predicates.len > 0)
    {
        if (wrote) try writer.writeByte(',');
        try writer.writeAll("\"where\":{");
        var wrote_where = false;
        if (source.predicates.len > 0 or source.array_contains.len > 0 or source.array_eq.len > 0 or source.array_any.len > 0 or source.in_predicates.len > 0 or source.json_contains.len > 0 or source.json_path_eq.len > 0 or source.json_path_exists.len > 0 or source.text_patterns.len > 0) {
            try writer.writeAll("\"all\":[");
            var wrote_atom = false;
            try lower_expr.writeRelationalCheckAtomsJson(writer, &wrote_atom, source.predicates);
            try lower_expr.writeInPredicateAtomsJson(writer, &wrote_atom, source.in_predicates);
            try lower_expr.writeTextPatternPredicateAtomsJson(writer, &wrote_atom, source.text_patterns);
            try lower_expr.writeStructuredValuePredicateAtomsJson(writer, &wrote_atom, "array_any", source.array_any);
            try lower_expr.writeStructuredValuePredicateAtomsJson(writer, &wrote_atom, "array_contains", source.array_contains);
            try lower_expr.writeStructuredValuePredicateAtomsJson(writer, &wrote_atom, "array_eq", source.array_eq);
            try lower_expr.writeStructuredValuePredicateAtomsJson(writer, &wrote_atom, "json_contains", source.json_contains);
            try lower_expr.writeJsonPathEqPredicateAtomsJson(writer, &wrote_atom, source.json_path_eq);
            try lower_expr.writeJsonPathExistsPredicateAtomsJson(writer, &wrote_atom, source.json_path_exists);
            try writer.writeByte(']');
            wrote_where = true;
        }
        if (source.or_predicates.len > 0) {
            if (wrote_where) try writer.writeByte(',');
            try writer.writeAll("\"any\":[");
            for (source.or_predicates, 0..) |group, group_i| {
                if (group_i != 0) try writer.writeByte(',');
                if (group.predicates.len == 1) {
                    try lower_expr.writeRelationalCheckAtomJson(writer, group.predicates[0]);
                } else {
                    try writer.writeAll("{\"all\":[");
                    var wrote_atom = false;
                    try lower_expr.writeRelationalCheckAtomsJson(writer, &wrote_atom, group.predicates);
                    try writer.writeAll("]}");
                }
            }
            try writer.writeByte(']');
            wrote_where = true;
        }
        if (source.not_predicates.len > 0) {
            if (wrote_where) try writer.writeByte(',');
            try writer.writeAll("\"not\":[");
            for (source.not_predicates, 0..) |group, group_i| {
                if (group_i != 0) try writer.writeByte(',');
                if (group.predicates.len == 1) {
                    try lower_expr.writeRelationalCheckAtomJson(writer, group.predicates[0]);
                } else {
                    try writer.writeAll("{\"all\":[");
                    var wrote_atom = false;
                    try lower_expr.writeRelationalCheckAtomsJson(writer, &wrote_atom, group.predicates);
                    try writer.writeAll("]}");
                }
            }
            try writer.writeByte(']');
        }
        try writer.writeByte('}');
        wrote = true;
    }
    if (source.expression_predicates.len > 0) {
        if (wrote) try writer.writeByte(',');
        try writer.writeAll("\"expression_where\":[");
        for (source.expression_predicates, 0..) |condition, i| {
            if (i != 0) try writer.writeByte(',');
            try lower_expr.writeRowExpressionConditionJson(writer, condition);
        }
        try writer.writeByte(']');
        wrote = true;
    }
    if (source.expression_or_predicates.len > 0) {
        if (wrote) try writer.writeByte(',');
        try lower_expr.writeRowExpressionPredicateGroupsJson(writer, "expression_any", source.expression_or_predicates);
        wrote = true;
    }
    if (source.expression_not_predicates.len > 0) {
        if (wrote) try writer.writeByte(',');
        try lower_expr.writeRowExpressionPredicateGroupsJson(writer, "expression_not", source.expression_not_predicates);
        wrote = true;
    }
    if (source.expression_array_contains.len > 0) {
        if (wrote) try writer.writeByte(',');
        try writer.writeAll("\"expression_array_contains\":[");
        for (source.expression_array_contains, 0..) |predicate, i| {
            if (i != 0) try writer.writeByte(',');
            try writer.writeAll("{\"expr\":");
            try lower_expr.writeRowExpressionJson(writer, predicate.expression);
            try writer.writeAll(",\"value\":");
            try writer.writeAll(predicate.value_json);
            try writer.writeByte('}');
        }
        try writer.writeByte(']');
        wrote = true;
    }
    if (source.order_by.len > 0) {
        if (wrote) try writer.writeByte(',');
        try writer.writeAll("\"order_by\":[");
        for (source.order_by, 0..) |order, i| {
            if (i != 0) try writer.writeByte(',');
            if (order.expression) |expression| {
                try writer.writeAll("{\"expr\":");
                try lower_expr.writeRowExpressionJson(writer, expression);
            } else {
                try writer.print("{{\"field\":{f}", .{std.json.fmt(order.field, .{})});
            }
            if (order.direction == .desc) try writer.writeAll(",\"direction\":\"desc\"");
            try writer.writeByte('}');
        }
        try writer.writeByte(']');
        wrote = true;
    }
    if (source.limit) |limit| {
        if (wrote) try writer.writeByte(',');
        try writer.print("\"limit\":{d}", .{limit});
        wrote = true;
    }
    if (source.offset != 0) {
        if (wrote) try writer.writeByte(',');
        try writer.print("\"offset\":{d}", .{source.offset});
        wrote = true;
    }
    if (source.row_claim) |claim| {
        if (wrote) try writer.writeByte(',');
        const txn_id = claim.txn_id orelse return error.UnsupportedRowsQuery;
        const txn_hex = sql_value.encodeSqlTxnIdHex(txn_id);
        try writer.print("\"row_claim\":{{\"mode\":\"{s}\",\"wait_policy\":\"{s}\",\"lease_ms\":{d},\"owner_id\":{f},\"transaction_id\":\"{s}\"}}", .{
            lower_expr.sqlRowClaimModeName(claim.mode),
            lower_expr.sqlRowClaimWaitPolicyName(claim.effectiveWaitPolicy()),
            claim.lease_ms,
            std.json.fmt(claim.owner_id, .{}),
            txn_hex,
        });
    }
    try writer.writeByte('}');
}

pub fn writeReturningProjectionJson(writer: *std.Io.Writer, returning: ReturningProjection) !void {
    if (returning.fields.len > 0) try writeReturningJson(writer, returning.fields);
    if (returning.expressions.len == 0) return;
    try writer.writeAll(",\"returning_expressions\":[");
    for (returning.expressions, 0..) |projection, i| {
        if (i != 0) try writer.writeByte(',');
        try writer.print("{{\"as\":{f},\"expr\":", .{std.json.fmt(projection.output, .{})});
        try lower_expr.writeRowExpressionJson(writer, projection.expression);
        try writer.writeByte('}');
    }
    try writer.writeByte(']');
}

fn writeReturningJson(writer: *std.Io.Writer, returning_fields: []const []const u8) !void {
    if (returning_fields.len == 0) return;
    try writer.writeAll(",\"returning\":[");
    for (returning_fields, 0..) |field, i| {
        if (i != 0) try writer.writeByte(',');
        try writer.print("{f}", .{std.json.fmt(field, .{})});
    }
    try writer.writeByte(']');
}

pub fn conflictActionName(action: db_mod.types.RelationalRowsConflictAction) []const u8 {
    return switch (action) {
        .update => "update",
        .nothing => "nothing",
    };
}

pub fn conflictActionToken(action: ConflictAction) []const u8 {
    return switch (action) {
        .nothing => "nothing",
        .update => "update",
    };
}

pub fn conflictProposedColumnAvailable(
    insert_columns: []const []const u8,
    source_column: runtime_schema.RelationalColumn,
    source: []const u8,
) bool {
    for (insert_columns) |insert_column| {
        if (std.mem.eql(u8, insert_column, source)) return true;
    }
    return source_column.default_value != null or source_column.generated != null;
}

pub fn conflictExistingFieldName(
    alloc: std.mem.Allocator,
    existing_qualifiers: []const []const u8,
    text: []const u8,
) ?[]const u8 {
    var dot: ?usize = std.mem.indexOfScalar(u8, text, '.');
    while (dot) |index| {
        if (index > 0 and index + 1 < text.len) {
            const qualifier = text[0..index];
            const field = text[index + 1 ..];
            if (conflictQualifierMatches(alloc, existing_qualifiers, qualifier)) return field;
        }
        dot = std.mem.indexOfScalarPos(u8, text, index + 1, '.');
    }
    return null;
}

pub fn conflictQualifierMatches(
    alloc: std.mem.Allocator,
    existing_qualifiers: []const []const u8,
    qualifier: []const u8,
) bool {
    for (existing_qualifiers) |expected| {
        if (std.mem.eql(u8, qualifier, expected)) return true;
    }
    const normalized = grammar.normalizeSqlObjectIdentifierAlloc(alloc, qualifier) catch return false;
    defer alloc.free(normalized);
    for (existing_qualifiers) |expected| {
        if (std.mem.eql(u8, normalized, expected)) return true;
    }
    return false;
}

pub fn canParseJoinedBooleanAssignmentExpression(
    tokens: []const Token,
    pos: usize,
    schema: runtime_schema.TableSchema,
    joined_source_schema: ?runtime_schema.TableSchema,
    pending_joined_source_alias: ?[]const u8,
    target_alias: []const u8,
) bool {
    if (!joinedBooleanExpressionCanStartAt(tokens, pos, schema, joined_source_schema, pending_joined_source_alias, target_alias)) return false;
    return scanBooleanAssignmentTail(tokens, pos, true);
}

pub fn canParseJoinedAssignmentExpression(
    tokens: []const Token,
    pos: usize,
    schema: runtime_schema.TableSchema,
    joined_source_schema: ?runtime_schema.TableSchema,
    target_alias: []const u8,
) bool {
    if (pos >= tokens.len) return false;
    if (tokens[pos].kind == .minus) {
        if (pos + 1 >= tokens.len) return false;
        return tokens[pos + 1].kind != .number;
    }
    if (lower_expr.rowExpressionHasTopLevelPipeConcat(tokens, pos)) return true;
    if (assignmentExpressionKeywordCanStartAt(tokens, pos, false)) return true;
    if (tokens[pos].kind == .lparen) return true;
    if (tokens[pos].kind != .identifier or
        keywordAt(tokens, pos, "default") or
        keywordAt(tokens, pos, "null") or
        keywordAt(tokens, pos, "true") or
        keywordAt(tokens, pos, "false"))
    {
        return false;
    }
    const token = tokens[pos];
    const next_is_expression_operator = pos + 1 < tokens.len and switch (tokens[pos + 1].kind) {
        .plus, .minus, .star, .slash, .percent, .arrow_json, .arrow_text, .path_arrow_json, .path_arrow_text, .pipe_concat => true,
        else => false,
    };
    if (!next_is_expression_operator) return false;
    if (lower_expr.identifierContainsQualifier(token.text)) {
        const dot = std.mem.indexOfScalar(u8, token.text, '.') orelse return false;
        const qualifier = token.text[0..dot];
        const field = token.text[dot + 1 ..];
        if (std.mem.eql(u8, qualifier, target_alias)) return binder.relationalColumnForField(schema, field, null) != null;
        return binder.relationalColumnForField(joined_source_schema orelse schema, field, null) != null;
    }
    return binder.relationalColumnForField(schema, token.text, null) != null;
}

pub fn canParseConflictBooleanAssignmentExpression(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: usize,
    schema: runtime_schema.TableSchema,
    conflict_existing_qualifiers: []const []const u8,
    insert_columns: []const []const u8,
) bool {
    if (!conflictBooleanExpressionCanStartAt(alloc, tokens, pos, schema, conflict_existing_qualifiers, insert_columns)) return false;
    return scanBooleanAssignmentTail(tokens, pos, true);
}

pub fn canParseConflictAssignmentExpression(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: usize,
    schema: runtime_schema.TableSchema,
    conflict_existing_qualifiers: []const []const u8,
    insert_columns: []const []const u8,
) bool {
    if (pos >= tokens.len) return false;
    if (tokens[pos].kind == .minus) {
        if (pos + 1 >= tokens.len) return false;
        return tokens[pos + 1].kind != .number;
    }
    if (lower_expr.rowExpressionHasTopLevelPipeConcat(tokens, pos)) return true;
    if (assignmentExpressionKeywordCanStartAt(tokens, pos, true)) return true;
    if (tokens[pos].kind == .lparen) return true;
    if (tokens[pos].kind != .identifier or
        keywordAt(tokens, pos, "default") or
        keywordAt(tokens, pos, "null") or
        keywordAt(tokens, pos, "true") or
        keywordAt(tokens, pos, "false"))
    {
        return false;
    }
    const token = tokens[pos];
    if (std.mem.startsWith(u8, token.text, "excluded.")) {
        const source = token.text["excluded.".len..];
        const source_column = binder.relationalColumnForField(schema, source, null) orelse return false;
        return conflictProposedColumnAvailable(insert_columns, source_column, source);
    }
    if (conflictExistingFieldName(alloc, conflict_existing_qualifiers, token.text)) |field| {
        return binder.relationalColumnForField(schema, field, null) != null;
    }
    return binder.relationalColumnForField(schema, token.text, null) != null;
}

pub fn canParseBareBooleanConflictExpression(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: usize,
    schema: runtime_schema.TableSchema,
    conflict_existing_qualifiers: []const []const u8,
    insert_columns: []const []const u8,
) bool {
    if (!conflictBooleanExpressionCanStartAt(alloc, tokens, pos, schema, conflict_existing_qualifiers, insert_columns)) return false;
    const scan = scanBareBooleanExpressionTail(tokens, pos);
    if (!scan.saw_token) return false;
    return scan.saw_boolean_syntax or singleConflictBooleanExpressionCanStartAt(alloc, tokens, pos, schema, conflict_existing_qualifiers, insert_columns);
}

pub fn joinedBooleanExpressionCanStartAt(
    tokens: []const Token,
    index: usize,
    schema: runtime_schema.TableSchema,
    joined_source_schema: ?runtime_schema.TableSchema,
    pending_joined_source_alias: ?[]const u8,
    target_alias: []const u8,
) bool {
    if (index >= tokens.len) return false;
    if (tokens[index].kind == .lparen) {
        const inner = parser.predicateStartIndexAfterOpenParens(tokens, index);
        return joinedBooleanExpressionCanStartAt(tokens, inner, schema, joined_source_schema, pending_joined_source_alias, target_alias);
    }
    return singleJoinedBooleanExpressionCanStartAt(tokens, index, schema, joined_source_schema, pending_joined_source_alias, target_alias);
}

fn singleJoinedBooleanExpressionCanStartAt(
    tokens: []const Token,
    index: usize,
    schema: runtime_schema.TableSchema,
    joined_source_schema: ?runtime_schema.TableSchema,
    pending_joined_source_alias: ?[]const u8,
    target_alias: []const u8,
) bool {
    if (index >= tokens.len) return false;
    const token = tokens[index];
    if (token.kind != .identifier) return false;
    if (booleanKeywordCanStart(token.text)) return true;
    if (lower_expr.identifierContainsQualifier(token.text)) {
        const dot = std.mem.indexOfScalar(u8, token.text, '.') orelse return false;
        const qualifier = token.text[0..dot];
        const field = token.text[dot + 1 ..];
        if (std.mem.eql(u8, qualifier, target_alias)) {
            return binder.relationalColumnForField(schema, field, .boolean) != null;
        }
        const source_alias = pending_joined_source_alias orelse return false;
        if (std.mem.eql(u8, qualifier, source_alias)) {
            return binder.relationalColumnForField(joined_source_schema orelse schema, field, .boolean) != null;
        }
        return false;
    }
    return binder.relationalColumnForField(schema, token.text, .boolean) != null;
}

pub fn conflictBooleanExpressionCanStartAt(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    index: usize,
    schema: runtime_schema.TableSchema,
    conflict_existing_qualifiers: []const []const u8,
    insert_columns: []const []const u8,
) bool {
    if (index >= tokens.len) return false;
    if (tokens[index].kind == .lparen) {
        const inner = parser.predicateStartIndexAfterOpenParens(tokens, index);
        return conflictBooleanExpressionCanStartAt(alloc, tokens, inner, schema, conflict_existing_qualifiers, insert_columns);
    }
    return singleConflictBooleanExpressionCanStartAt(alloc, tokens, index, schema, conflict_existing_qualifiers, insert_columns);
}

fn singleConflictBooleanExpressionCanStartAt(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    index: usize,
    schema: runtime_schema.TableSchema,
    conflict_existing_qualifiers: []const []const u8,
    insert_columns: []const []const u8,
) bool {
    if (index >= tokens.len) return false;
    const token = tokens[index];
    if (token.kind != .identifier) return false;
    if (booleanKeywordCanStart(token.text)) return true;
    if (std.mem.startsWith(u8, token.text, "excluded.")) {
        const field = token.text["excluded.".len..];
        const source_column = binder.relationalColumnForField(schema, field, .boolean) orelse return false;
        return conflictProposedColumnAvailable(insert_columns, source_column, field);
    }
    const field = conflictExistingFieldName(alloc, conflict_existing_qualifiers, token.text) orelse token.text;
    return binder.relationalColumnForField(schema, field, .boolean) != null;
}

fn assignmentExpressionKeywordCanStartAt(tokens: []const Token, pos: usize, include_md5: bool) bool {
    return keywordAt(tokens, pos, "case") or
        keywordAt(tokens, pos, "cast") or
        keywordAt(tokens, pos, "coalesce") or
        keywordAt(tokens, pos, "lower") or
        keywordAt(tokens, pos, "upper") or
        lower_expr.peekFunctionCallIf(tokens, pos, lower_expr.sqlKeywordIsInitcapFunction) or
        keywordAt(tokens, pos, "trim") or
        lower_expr.peekFunctionCallIf(tokens, pos, lower_expr.sqlKeywordIsTrimVariantFunction) or
        keywordAt(tokens, pos, "replace") or
        keywordAt(tokens, pos, "regexp_replace") or
        lower_expr.peekFunctionCallIf(tokens, pos, lower_expr.sqlKeywordIsRegexpSubstrFunction) or
        lower_expr.peekFunctionCallIf(tokens, pos, lower_expr.sqlKeywordIsRegexpMatchFunction) or
        lower_expr.peekFunctionCallIf(tokens, pos, lower_expr.sqlKeywordIsRegexpCountFunction) or
        lower_expr.peekFunctionCallIf(tokens, pos, lower_expr.sqlKeywordIsRegexpInstrFunction) or
        lower_expr.peekFunctionCallIf(tokens, pos, lower_expr.sqlKeywordIsTranslateFunction) or
        keywordAt(tokens, pos, "concat") or
        keywordAt(tokens, pos, "concat_ws") or
        keywordAt(tokens, pos, "nullif") or
        lower_expr.peekTextLengthFunctionKeyword(tokens, pos) or
        lower_expr.peekFunctionCallIf(tokens, pos, lower_expr.sqlKeywordIsAsciiFunction) or
        lower_expr.peekFunctionCallIf(tokens, pos, lower_expr.sqlKeywordIsChrFunction) or
        lower_expr.peekSubstringFunctionKeyword(tokens, pos) or
        lower_expr.peekFunctionCallIf(tokens, pos, lower_expr.sqlKeywordIsOverlayFunction) or
        lower_expr.peekSplitPartFunctionKeyword(tokens, pos) or
        lower_expr.peekStrposFunctionKeyword(tokens, pos) or
        lower_expr.peekFunctionCallIf(tokens, pos, lower_expr.sqlKeywordIsLeftRightFunction) or
        lower_expr.peekFunctionCallIf(tokens, pos, lower_expr.sqlKeywordIsPadFunction) or
        lower_expr.peekFunctionCallIf(tokens, pos, lower_expr.sqlKeywordIsRepeatFunction) or
        lower_expr.peekFunctionCallIf(tokens, pos, lower_expr.sqlKeywordIsReverseFunction) or
        (include_md5 and lower_expr.peekFunctionCallIf(tokens, pos, lower_expr.sqlKeywordIsMd5Function)) or
        lower_expr.peekFunctionCallIf(tokens, pos, lower_expr.sqlKeywordIsStartsWithFunction) or
        lower_expr.peekFunctionCallIf(tokens, pos, lower_expr.sqlKeywordIsEndsWithFunction) or
        lower_expr.peekFunctionCallIf(tokens, pos, lower_expr.sqlKeywordIsDateTruncFunction) or
        lower_expr.peekFunctionCallIf(tokens, pos, lower_expr.sqlKeywordIsDateBinFunction) or
        lower_expr.peekFunctionCallIf(tokens, pos, lower_expr.sqlKeywordIsDatePartFunction) or
        keywordAt(tokens, pos, "position") or
        keywordAt(tokens, pos, "abs") or
        keywordAt(tokens, pos, "round") or
        lower_expr.peekFunctionCall(tokens, pos, "trunc") or
        lower_expr.peekFunctionCall(tokens, pos, "floor") or
        lower_expr.peekFunctionCall(tokens, pos, "ceil") or
        lower_expr.peekFunctionCall(tokens, pos, "sqrt") or
        lower_expr.peekFunctionCall(tokens, pos, "sign") or
        lower_expr.peekFunctionCall(tokens, pos, "mod") or
        lower_expr.peekFunctionCall(tokens, pos, "power") or
        keywordAt(tokens, pos, "greatest") or
        keywordAt(tokens, pos, "least") or
        lower_expr.peekFunctionCallIf(tokens, pos, lower_expr.sqlKeywordIsJsonExtractPathFunction) or
        lower_expr.peekFunctionCallIf(tokens, pos, lower_expr.sqlKeywordIsJsonTypeofFunction) or
        lower_expr.peekFunctionCallIf(tokens, pos, lower_expr.sqlKeywordIsJsonArrayLengthFunction) or
        lower_expr.peekFunctionCallIf(tokens, pos, lower_expr.sqlKeywordIsJsonBuildObjectFunction) or
        lower_expr.functionCallStartsAtIf(tokens, pos, lower_expr.sqlKeywordIsArrayLengthFunction) or
        lower_expr.functionCallStartsAtIf(tokens, pos, lower_expr.sqlKeywordIsArrayPositionFunction) or
        keywordAt(tokens, pos, "array_append") or
        keywordAt(tokens, pos, "array_prepend") or
        keywordAt(tokens, pos, "array_cat") or
        keywordAt(tokens, pos, "array_remove") or
        keywordAt(tokens, pos, "array_replace") or
        keywordAt(tokens, pos, "array_to_string") or
        keywordAt(tokens, pos, "string_to_array") or
        keywordAt(tokens, pos, "now") or
        keywordAt(tokens, pos, "current_timestamp") or
        keywordAt(tokens, pos, "current_date") or
        lower_expr.peekSqlTypedDatetimeLiteral(tokens, pos) or
        keywordAt(tokens, pos, "interval");
}

fn scanBooleanAssignmentTail(tokens: []const Token, pos: usize, assignment_tail: bool) bool {
    var depth: usize = 0;
    var i = pos;
    var saw_boolean_operator = false;
    while (i < tokens.len) : (i += 1) {
        const token = tokens[i];
        switch (token.kind) {
            .semicolon, .comma => if (depth == 0) break,
            .rparen => {
                if (depth == 0) break;
                depth -= 1;
            },
            .lparen => depth += 1,
            .eq, .neq, .gt, .gte, .lt, .lte, .arrow_text, .arrow_json, .path_arrow_text, .path_arrow_json, .at_contains, .range_overlap, .question, .regex_match, .regex_imatch, .regex_not_match, .regex_not_imatch => return false,
            .identifier => {
                if (depth == 0 and assignment_tail and lower_expr.sqlAssignmentTailKeyword(token.text)) break;
                if (lower_expr.sqlKeywordStartsScalarPredicate(token.text)) return false;
                if (std.ascii.eqlIgnoreCase(token.text, "and") or
                    std.ascii.eqlIgnoreCase(token.text, "or") or
                    std.ascii.eqlIgnoreCase(token.text, "not"))
                {
                    saw_boolean_operator = true;
                }
            },
            else => {},
        }
    }
    return saw_boolean_operator;
}

const BareBooleanScan = struct {
    saw_boolean_syntax: bool,
    saw_token: bool,
};

fn scanBareBooleanExpressionTail(tokens: []const Token, pos: usize) BareBooleanScan {
    var depth: usize = 0;
    var i = pos;
    var saw_boolean_syntax = false;
    var saw_token = false;
    while (i < tokens.len) : (i += 1) {
        const token = tokens[i];
        switch (token.kind) {
            .semicolon, .comma => if (depth == 0) break,
            .rparen => {
                if (depth == 0) break;
                depth -= 1;
            },
            .lparen => {
                depth += 1;
                saw_token = true;
            },
            .eq, .neq, .gt, .gte, .lt, .lte, .arrow_text, .arrow_json, .path_arrow_text, .path_arrow_json, .at_contains, .range_overlap, .question, .regex_match, .regex_imatch, .regex_not_match, .regex_not_imatch => return .{
                .saw_boolean_syntax = false,
                .saw_token = false,
            },
            .identifier => {
                if (depth == 0 and lower_expr.sqlWhereTailClauseKeyword(token.text)) break;
                if (lower_expr.sqlKeywordStartsScalarPredicate(token.text)) return .{
                    .saw_boolean_syntax = false,
                    .saw_token = false,
                };
                if (std.ascii.eqlIgnoreCase(token.text, "and") or
                    std.ascii.eqlIgnoreCase(token.text, "or") or
                    std.ascii.eqlIgnoreCase(token.text, "not"))
                {
                    saw_boolean_syntax = true;
                }
                saw_token = true;
            },
            else => saw_token = true,
        }
    }
    return .{ .saw_boolean_syntax = saw_boolean_syntax, .saw_token = saw_token };
}

fn booleanKeywordCanStart(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "not") or
        std.ascii.eqlIgnoreCase(text, "true") or
        std.ascii.eqlIgnoreCase(text, "false") or
        std.ascii.eqlIgnoreCase(text, "case") or
        std.ascii.eqlIgnoreCase(text, "cast") or
        std.ascii.eqlIgnoreCase(text, "coalesce") or
        std.ascii.eqlIgnoreCase(text, "nullif") or
        lower_expr.sqlKeywordIsStartsWithFunction(text);
}

fn keywordAt(tokens: []const Token, pos: usize, keyword: []const u8) bool {
    return pos < tokens.len and tokens[pos].kind == .identifier and std.ascii.eqlIgnoreCase(tokens[pos].text, keyword);
}

pub fn conflictParenthesizedConjunctionCanStart(tokens: []const Token, pos: usize) bool {
    return conflictParenthesizedKeywordCanStart(tokens, pos, "and");
}

pub fn conflictParenthesizedDisjunctionCanStart(tokens: []const Token, pos: usize) bool {
    return conflictParenthesizedKeywordCanStart(tokens, pos, "or");
}

fn conflictParenthesizedKeywordCanStart(tokens: []const Token, pos: usize, keyword: []const u8) bool {
    if (pos >= tokens.len or tokens[pos].kind != .lparen) return false;

    var depth: usize = 0;
    var i = pos;
    while (i < tokens.len) : (i += 1) {
        const token = tokens[i];
        switch (token.kind) {
            .lparen => depth += 1,
            .rparen => {
                if (depth == 0) return false;
                depth -= 1;
                if (depth == 0) return false;
            },
            .identifier => if (depth == 1 and std.ascii.eqlIgnoreCase(token.text, keyword)) return true,
            .semicolon => if (depth == 0) return false,
            else => {},
        }
    }
    return false;
}

pub fn expressionAssignmentComputedCount(assignments: []const db_mod.types.RelationalRowsExpressionAssignment) usize {
    var count: usize = 0;
    for (assignments) |assignment| {
        if (assignment.expression.kind != .field) count += 1;
    }
    return count;
}

pub fn transformOperationCount(transforms: []const db_mod.types.DocumentTransform) usize {
    var count: usize = 0;
    for (transforms) |transform| count += transform.operations.len;
    return count;
}

pub fn normalizedIncrementJsonAlloc(alloc: std.mem.Allocator, value_json: []const u8, negated: bool) ![]const u8 {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.UnsupportedSqlShape;
    defer parsed.deinit();
    if (!negated) {
        switch (parsed.value) {
            .integer, .float, .number_string => return try alloc.dupe(u8, value_json),
            else => return error.UnsupportedSqlShape,
        }
    }
    return switch (parsed.value) {
        .integer => |value| if (value == std.math.minInt(i64))
            error.UnsupportedSqlShape
        else
            try std.fmt.allocPrint(alloc, "{d}", .{-value}),
        .float => |value| try std.fmt.allocPrint(alloc, "{d}", .{-value}),
        .number_string => |text| blk: {
            const value = std.fmt.parseFloat(f64, text) catch return error.UnsupportedSqlShape;
            break :blk try std.fmt.allocPrint(alloc, "{d}", .{-value});
        },
        else => error.UnsupportedSqlShape,
    };
}

pub fn insertSourceStaticJsonSetCount(values: []const JsonSetValue) usize {
    var count: usize = 0;
    for (values) |value| {
        if (value.value_json != null) count += 1;
    }
    return count;
}

pub fn insertSourceExpressionJsonSetCount(values: []const JsonSetValue) usize {
    var count: usize = 0;
    for (values) |value| {
        if (value.expression != null) count += 1;
    }
    return count;
}

pub fn jsonSetHasExpression(values: []const JsonSetValue) bool {
    for (values) |value| {
        if (value.expression != null) return true;
    }
    return false;
}

pub fn freeFieldJsonValues(alloc: std.mem.Allocator, values: []const FieldJsonValue) void {
    for (values) |value| {
        alloc.free(value.field);
        alloc.free(value.value_json);
    }
}

pub fn freeFieldExpressionValues(alloc: std.mem.Allocator, values: []const FieldExpressionValue) void {
    for (values) |value| {
        alloc.free(value.field);
        freeExpression(alloc, value.expression);
    }
}

pub fn freeTransformOps(alloc: std.mem.Allocator, values: []const db_mod.types.TransformOp) void {
    for (values) |value| {
        alloc.free(@constCast(value.path));
        if (value.value_json) |json| alloc.free(@constCast(json));
    }
    if (values.len > 0) alloc.free(@constCast(values));
}

pub fn freeRowsOnConflictValue(alloc: std.mem.Allocator, value: db_mod.types.RelationalRowsOnConflict) void {
    freeRowsConflictTargetValue(alloc, value.target);
    freeTransformOps(alloc, value.operations);
    freeExpressionAssignments(alloc, value.patch_expressions);
    freeExpressionAssignments(alloc, value.increment_expressions);
    freeRowsJsonSetExpressionAssignments(alloc, value.json_set_expressions);
    if (value.where_expression) |condition| freeExpressionCondition(alloc, condition);
    freeExpressionConditions(alloc, value.where_expressions);
    if (value.where_expressions.len > 0) alloc.free(value.where_expressions);
    freeExpressionPredicateGroups(alloc, value.where_any);
    if (value.where_any.len > 0) alloc.free(value.where_any);
    freeExpressionPredicateGroups(alloc, value.where_not);
    if (value.where_not.len > 0) alloc.free(value.where_not);
}

pub fn freeRowsConflictTargetValue(alloc: std.mem.Allocator, value: db_mod.types.RelationalRowsConflictTarget) void {
    if (value.unique_name.len > 0) alloc.free(@constCast(value.unique_name));
    plan_mod.freeRelationalChecks(alloc, value.unique_predicates);
    if (value.unique_predicates.len > 0) alloc.free(value.unique_predicates);
    freeExpressionConditions(alloc, value.unique_predicate_expressions);
    if (value.unique_predicate_expressions.len > 0) alloc.free(value.unique_predicate_expressions);
}

pub fn freeInsertValueRows(alloc: std.mem.Allocator, rows: InsertValueRows) void {
    for (rows) |row| {
        for (row) |value| alloc.free(value);
        alloc.free(row);
    }
}

pub fn freeJoinedMutationSourceAssignments(
    alloc: std.mem.Allocator,
    values: []const JoinedMutationSourceAssignment,
) void {
    for (values) |value| {
        alloc.free(value.field);
        alloc.free(value.source_qualifier);
        alloc.free(value.source_field);
    }
}

pub fn freeArrayTransformValues(alloc: std.mem.Allocator, values: []const ArrayTransformValue) void {
    for (values) |value| {
        alloc.free(value.field);
        alloc.free(value.value_json);
    }
}

pub fn freeFieldPredicates(alloc: std.mem.Allocator, values: []const FieldPredicate) void {
    for (values) |value| {
        alloc.free(value.field);
        if (value.value_json) |json| alloc.free(json);
    }
}

pub fn freeJsonSetValues(alloc: std.mem.Allocator, values: []const JsonSetValue) void {
    for (values) |value| {
        alloc.free(value.field);
        strings.freeStringSlice(alloc, value.path);
        if (value.value_json) |json| alloc.free(json);
        if (value.expression) |expression| freeExpression(alloc, expression);
    }
}

pub fn freeJsonSetParsedValue(alloc: std.mem.Allocator, value: JsonSetParsedValue) void {
    if (value.value_json) |json| alloc.free(json);
    if (value.expression) |expression| freeExpression(alloc, expression);
}

pub fn freeConflictTarget(alloc: std.mem.Allocator, target: ConflictTarget) void {
    switch (target) {
        .primary => {},
        .unique => |unique| {
            alloc.free(unique.name);
            if (unique.where_json.len > 0) alloc.free(unique.where_json);
            freeExpressionConditions(alloc, unique.where_expressions);
            if (unique.where_expressions.len > 0) alloc.free(unique.where_expressions);
        },
    }
}

pub fn freeConflictClause(alloc: std.mem.Allocator, clause: ConflictClause) void {
    freeConflictTarget(alloc, clause.target);
    freeFieldJsonValues(alloc, clause.patch);
    if (clause.patch.len > 0) alloc.free(clause.patch);
    freeFieldExpressionValues(alloc, clause.patch_expr);
    if (clause.patch_expr.len > 0) alloc.free(clause.patch_expr);
    freeFieldJsonValues(alloc, clause.increment);
    if (clause.increment.len > 0) alloc.free(clause.increment);
    freeFieldExpressionValues(alloc, clause.increment_expr);
    if (clause.increment_expr.len > 0) alloc.free(clause.increment_expr);
    freeJsonSetValues(alloc, clause.json_set);
    if (clause.json_set.len > 0) alloc.free(clause.json_set);
    freeArrayTransformValues(alloc, clause.array_update);
    if (clause.array_update.len > 0) alloc.free(clause.array_update);
    if (clause.where_expression) |condition| freeExpressionCondition(alloc, condition);
    freeExpressionConditions(alloc, clause.where_expressions);
    if (clause.where_expressions.len > 0) alloc.free(clause.where_expressions);
    freeExpressionPredicateGroups(alloc, clause.where_any);
    if (clause.where_any.len > 0) alloc.free(clause.where_any);
    freeExpressionPredicateGroups(alloc, clause.where_not);
    if (clause.where_not.len > 0) alloc.free(clause.where_not);
}

pub fn freeConflictClauses(alloc: std.mem.Allocator, clauses: []const ConflictClause) void {
    for (clauses) |clause| freeConflictClause(alloc, clause);
}

pub fn mergeSourceQueryIsDefault(req: db_mod.types.RelationalRowsQueryRequest) bool {
    return req.source_cte.len == 0 and
        req.predicates.len == 0 and
        req.array_any.len == 0 and
        req.array_contains.len == 0 and
        req.array_eq.len == 0 and
        req.in_predicates.len == 0 and
        req.json_contains.len == 0 and
        req.json_path_eq.len == 0 and
        req.json_path_exists.len == 0 and
        req.text_patterns.len == 0 and
        req.or_predicates.len == 0 and
        req.not_predicates.len == 0 and
        req.access_or_predicates.len == 0 and
        req.access_not_predicates.len == 0 and
        req.expression_predicates.len == 0 and
        req.expression_or_predicates.len == 0 and
        req.expression_not_predicates.len == 0 and
        req.expression_array_contains.len == 0 and
        req.select.len == 0 and
        req.json_extract.len == 0 and
        req.array_length.len == 0 and
        req.coalesce.len == 0 and
        req.field_aliases.len == 0 and
        req.expressions.len == 0 and
        req.select_all and
        req.distinct_on.len == 0 and
        req.distinct_on_expressions.len == 0 and
        req.order_by.len == 0 and
        req.row_claim == null and
        req.doc_key_range == null and
        req.limit == null and
        req.offset == 0;
}

pub const MergeExecutionTargetRow = struct {
    key: []const u8,
    json: []const u8,
    version: u64,
};

pub fn buildMergeMutationBatchAlloc(
    alloc: std.mem.Allocator,
    target_schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    plan: LoweredMergeMutationPlan,
    target_rows: []const MergeExecutionTargetRow,
    source_rows: []const []const u8,
) !relational_rows.OwnedRowsBatchRequest {
    if (target_schema.storage_mode != .relational or target_schema.primary_key == null) return error.InvalidSqlCatalog;
    if (source_schema.storage_mode != .relational) return error.InvalidSqlCatalog;

    var target_parsed = std.ArrayListUnmanaged(std.json.Parsed(std.json.Value)).empty;
    defer {
        for (target_parsed.items) |*parsed| parsed.deinit();
        target_parsed.deinit(alloc);
    }
    try target_parsed.ensureUnusedCapacity(alloc, target_rows.len);
    for (target_rows) |row| {
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, row.json, .{}) catch return error.InvalidRowsRequest;
        errdefer parsed.deinit();
        if (parsed.value != .object) return error.InvalidRowsRequest;
        target_parsed.appendAssumeCapacity(parsed);
    }

    var source_parsed = std.ArrayListUnmanaged(std.json.Parsed(std.json.Value)).empty;
    defer {
        for (source_parsed.items) |*parsed| parsed.deinit();
        source_parsed.deinit(alloc);
    }
    try source_parsed.ensureUnusedCapacity(alloc, source_rows.len);
    for (source_rows) |row| {
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, row, .{}) catch return error.InvalidRowsRequest;
        errdefer parsed.deinit();
        if (parsed.value != .object) return error.InvalidRowsRequest;
        source_parsed.appendAssumeCapacity(parsed);
    }

    var matched_target_keys = std.StringHashMapUnmanaged(void).empty;
    defer matched_target_keys.deinit(alloc);

    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.writeAll("{\"operations\":[");
    var wrote_operation = false;

    for (source_parsed.items) |source| {
        var matched_index: ?usize = null;
        for (target_parsed.items, 0..) |target, target_index| {
            if (try mergeRowsMatch(source.value, target.value, plan.match_fields)) {
                if (matched_index != null) return error.InvalidRowsRequest;
                matched_index = target_index;
            }
        }

        if (matched_index) |target_index| {
            const target = target_parsed.items[target_index].value;
            const arm = (try selectMergeMatchedArm(alloc, target, source.value, plan.matched_arms)) orelse continue;
            if (arm.do_nothing) continue;
            if (arm.update.len == 0 and arm.update_expressions.len == 0 and !arm.delete) continue;
            const gop = try matched_target_keys.getOrPut(alloc, target_rows[target_index].key);
            if (gop.found_existing) return error.InvalidRowsRequest;
            if (wrote_operation) try writer.writeByte(',');
            wrote_operation = true;
            if (arm.delete) {
                try writer.writeAll("{\"op\":\"delete\",\"where\":");
                try writeMergePrimaryWhereJson(writer, target_schema, target);
                try writer.print(",\"expected_version\":{d}", .{target_rows[target_index].version});
                try writeMergeReturningProjectionJson(writer, plan.returning);
                try writer.writeByte('}');
            } else {
                try writer.writeAll("{\"op\":\"update\",\"where\":");
                try writeMergePrimaryWhereJson(writer, target_schema, target);
                try writer.print(",\"expected_version\":{d},\"patch\":{{", .{target_rows[target_index].version});
                var wrote_patch_field = false;
                for (arm.update) |mapping| {
                    if (wrote_patch_field) try writer.writeByte(',');
                    wrote_patch_field = true;
                    try writer.print("{f}:", .{std.json.fmt(mapping.target_field, .{})});
                    try std.json.Stringify.value(try mergeObjectField(source.value, mapping.source_field), .{}, writer);
                }
                for (arm.update_expressions) |assignment| {
                    if (wrote_patch_field) try writer.writeByte(',');
                    wrote_patch_field = true;
                    const value_json = try relational_rows.expressionValueJsonWithTargetSourceAlloc(alloc, target, source.value, assignment.expression);
                    defer alloc.free(value_json);
                    try writer.print("{f}:{s}", .{ std.json.fmt(assignment.target_field, .{}), value_json });
                }
                try writer.writeByte('}');
                try writeMergeReturningProjectionJson(writer, plan.returning);
                try writer.writeByte('}');
            }
        } else {
            const arm = (try selectMergeNotMatchedArm(alloc, source.value, plan.not_matched_arms)) orelse continue;
            if (arm.do_nothing or (arm.insert.len == 0 and arm.insert_expressions.len == 0)) continue;
            if (wrote_operation) try writer.writeByte(',');
            wrote_operation = true;
            try writer.writeAll("{\"op\":\"insert\",\"row\":{");
            var wrote_insert_field = false;
            for (arm.insert) |mapping| {
                if (wrote_insert_field) try writer.writeByte(',');
                wrote_insert_field = true;
                try writer.print("{f}:", .{std.json.fmt(mapping.target_field, .{})});
                try std.json.Stringify.value(try mergeObjectField(source.value, mapping.source_field), .{}, writer);
            }
            for (arm.insert_expressions) |assignment| {
                if (wrote_insert_field) try writer.writeByte(',');
                wrote_insert_field = true;
                const value_json = try relational_rows.expressionValueJsonWithTargetSourceAlloc(alloc, source.value, source.value, assignment.expression);
                defer alloc.free(value_json);
                try writer.print("{f}:{s}", .{ std.json.fmt(assignment.target_field, .{}), value_json });
            }
            try writer.writeByte('}');
            try writeMergeReturningProjectionJson(writer, plan.returning);
            try writer.writeByte('}');
        }
    }

    try writer.writeAll("]}");
    const body_json = try out.toOwnedSlice();
    defer alloc.free(body_json);

    var resolver_ctx = MergeBatchResolverContext{
        .table_name = plan.target_table_name,
        .schema = target_schema,
        .rows = target_rows,
    };
    return try relational_rows.parseRowsBatchRequestWithResolver(
        alloc,
        plan.target_table_name,
        body_json,
        target_schema,
        resolver_ctx.resolver(),
    );
}

const MergeBatchResolverContext = struct {
    table_name: []const u8,
    schema: runtime_schema.TableSchema,
    rows: []const MergeExecutionTargetRow,

    fn resolver(self: *@This()) relational_rows.UniqueSelectorResolver {
        return .{
            .ptr = self,
            .resolve = resolveUnique,
            .resolve_temporal = resolveTemporalUnique,
            .resolve_primary = primaryExists,
            .lookup_primary = lookupPrimary,
        };
    }

    fn resolveUnique(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8) !?[]u8 {
        return null;
    }

    fn resolveTemporalUnique(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        constraint_name: []const u8,
        encoded_value: []const u8,
        encoded_point: []const u8,
    ) !?[]u8 {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        if (!std.mem.eql(u8, constraint_name, db_mod.relational_store.primary_key_constraint_name)) return null;
        var found: ?[]const u8 = null;
        for (self.rows) |row| {
            if (!try relational_rows.temporalPrimaryKeyRowContainsPointAlloc(alloc, self.schema, row.json, encoded_value, encoded_point)) continue;
            if (found) |existing| {
                if (!std.mem.eql(u8, existing, row.key)) return error.UniqueConstraintViolation;
                continue;
            }
            found = row.key;
        }
        return if (found) |key| try alloc.dupe(u8, key) else null;
    }

    fn primaryExists(ptr: *anyopaque, _: std.mem.Allocator, table_name: []const u8, physical_key: []const u8) !bool {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return false;
        for (self.rows) |row| {
            if (std.mem.eql(u8, row.key, physical_key)) return true;
        }
        return false;
    }

    fn lookupPrimary(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, physical_key: []const u8) !?relational_rows.ResolvedPrimaryRow {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        for (self.rows) |row| {
            if (!std.mem.eql(u8, row.key, physical_key)) continue;
            return .{
                .json = try alloc.dupe(u8, row.json),
                .version = row.version,
            };
        }
        return null;
    }
};

fn mergeRowsMatch(
    source: std.json.Value,
    target: std.json.Value,
    mappings: []const MergeFieldMapping,
) !bool {
    for (mappings) |mapping| {
        if (!mergeJsonValuesEqual(try mergeObjectField(target, mapping.target_field), try mergeObjectField(source, mapping.source_field))) return false;
    }
    return true;
}

fn mergePredicatesMatch(
    alloc: std.mem.Allocator,
    target: std.json.Value,
    source: std.json.Value,
    predicates: []const MergeArmPredicate,
) !bool {
    for (predicates) |predicate| {
        const row = switch (predicate.side) {
            .target => target,
            .source => source,
        };
        if (!try mergePredicateMatches(alloc, row, predicate)) return false;
    }
    return true;
}

fn selectMergeMatchedArm(
    alloc: std.mem.Allocator,
    target: std.json.Value,
    source: std.json.Value,
    arms: []const MergeMatchedArm,
) !?MergeMatchedArm {
    for (arms) |arm| {
        if (!try mergePredicatesMatch(alloc, target, source, arm.predicates)) continue;
        if (!try mergeExpressionPredicatesMatch(
            alloc,
            target,
            source,
            arm.expression_predicates,
            arm.expression_or_predicates,
            arm.expression_not_predicates,
        )) continue;
        return arm;
    }
    return null;
}

fn selectMergeNotMatchedArm(
    alloc: std.mem.Allocator,
    source: std.json.Value,
    arms: []const MergeNotMatchedArm,
) !?MergeNotMatchedArm {
    for (arms) |arm| {
        if (!try mergePredicatesMatch(alloc, .{ .null = {} }, source, arm.predicates)) continue;
        if (!try mergeExpressionPredicatesMatch(
            alloc,
            source,
            source,
            arm.expression_predicates,
            arm.expression_or_predicates,
            arm.expression_not_predicates,
        )) continue;
        return arm;
    }
    return null;
}

fn mergeExpressionPredicatesMatch(
    alloc: std.mem.Allocator,
    target: std.json.Value,
    source: std.json.Value,
    predicates: []const db_mod.types.RelationalRowsExpressionCondition,
    or_predicates: []const db_mod.types.RelationalRowsExpressionPredicateGroup,
    not_predicates: []const db_mod.types.RelationalRowsExpressionPredicateGroup,
) !bool {
    for (predicates) |predicate| {
        if (!try relational_rows.expressionConditionMatchesWithTargetSource(alloc, target, source, predicate)) return false;
    }
    if (!try mergeExpressionOrPredicateGroupsMatch(alloc, target, source, or_predicates)) return false;
    if (!try mergeExpressionNotPredicateGroupsMatch(alloc, target, source, not_predicates)) return false;
    return true;
}

fn mergeExpressionOrPredicateGroupsMatch(
    alloc: std.mem.Allocator,
    target: std.json.Value,
    source: std.json.Value,
    groups: []const db_mod.types.RelationalRowsExpressionPredicateGroup,
) !bool {
    if (groups.len == 0) return true;
    for (groups) |group| {
        var group_matches = true;
        for (group.conditions) |condition| {
            if (!try relational_rows.expressionConditionMatchesWithTargetSource(alloc, target, source, condition)) {
                group_matches = false;
                break;
            }
        }
        if (group_matches) return true;
    }
    return false;
}

fn mergeExpressionNotPredicateGroupsMatch(
    alloc: std.mem.Allocator,
    target: std.json.Value,
    source: std.json.Value,
    groups: []const db_mod.types.RelationalRowsExpressionPredicateGroup,
) !bool {
    for (groups) |group| {
        var group_matches = true;
        for (group.conditions) |condition| {
            if (!try relational_rows.expressionConditionMatchesWithTargetSource(alloc, target, source, condition)) {
                group_matches = false;
                break;
            }
        }
        if (group_matches) return false;
    }
    return true;
}

fn mergePredicateMatches(alloc: std.mem.Allocator, row: std.json.Value, predicate: MergeArmPredicate) !bool {
    if (predicate.op == .is_null) return row == .object and mergeObjectFieldOrNull(row, predicate.field) == null;
    if (predicate.op == .is_not_null) return row == .object and mergeObjectFieldOrNull(row, predicate.field) != null;
    const actual = try mergeObjectField(row, predicate.field);
    const value_json = predicate.value_json orelse return error.InvalidRowsRequest;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    if (predicate.op == .is_distinct or predicate.op == .is_not_distinct) {
        const equal = mergeJsonValuesNotDistinct(actual, parsed.value);
        return if (predicate.op == .is_distinct) !equal else equal;
    }
    const cmp = mergeJsonCompare(actual, parsed.value) orelse return false;
    return switch (predicate.op) {
        .eq => cmp == .eq,
        .ne => cmp != .eq,
        .gt => cmp == .gt,
        .gte => cmp == .gt or cmp == .eq,
        .lt => cmp == .lt,
        .lte => cmp == .lt or cmp == .eq,
        .is_distinct, .is_not_distinct => unreachable,
        .is_null, .is_not_null => unreachable,
    };
}

const MergeScalarComparison = enum { lt, eq, gt };

fn mergeJsonValuesEqual(left: std.json.Value, right: std.json.Value) bool {
    const cmp = mergeJsonCompare(left, right) orelse return false;
    return cmp == .eq;
}

fn mergeJsonValuesNotDistinct(left: std.json.Value, right: std.json.Value) bool {
    if (left == .null and right == .null) return true;
    if (left == .null or right == .null) return false;
    return mergeJsonValuesEqual(left, right);
}

fn mergeJsonCompare(left: std.json.Value, right: std.json.Value) ?MergeScalarComparison {
    if (mergeJsonNumericValue(left)) |left_num| {
        const right_num = mergeJsonNumericValue(right) orelse return null;
        if (left_num < right_num) return .lt;
        if (left_num > right_num) return .gt;
        return .eq;
    }
    if (left == .string and right == .string) {
        return switch (std.mem.order(u8, left.string, right.string)) {
            .lt => .lt,
            .eq => .eq,
            .gt => .gt,
        };
    }
    if (left == .bool and right == .bool) {
        if (left.bool == right.bool) return .eq;
        return if (!left.bool and right.bool) .lt else .gt;
    }
    return null;
}

fn mergeJsonNumericValue(value: std.json.Value) ?f64 {
    return switch (value) {
        .integer => |integer| @floatFromInt(integer),
        .float => |float| float,
        else => null,
    };
}

fn mergeObjectField(value: std.json.Value, field: []const u8) !std.json.Value {
    if (value != .object) return error.InvalidRowsRequest;
    return mergeObjectFieldOrNull(value, field) orelse return error.InvalidRowsRequest;
}

fn mergeObjectFieldOrNull(value: std.json.Value, field: []const u8) ?std.json.Value {
    if (value != .object) return null;
    return value.object.get(field);
}

fn writeMergePrimaryWhereJson(
    writer: *std.Io.Writer,
    schema: runtime_schema.TableSchema,
    row: std.json.Value,
) !void {
    const primary_key = schema.primary_key orelse return error.InvalidRowsRequest;
    if (primary_key.without_overlaps_period) |period_name| {
        const period = binder.relationalPeriodForDdl(schema.periods, period_name) orelse return error.InvalidRowsRequest;
        try writer.writeAll("{\"primary\":{\"values\":{");
        for (primary_key.columns, 0..) |column, i| {
            if (i != 0) try writer.writeByte(',');
            try writer.print("{f}:", .{std.json.fmt(column, .{})});
            try std.json.Stringify.value(try mergeObjectField(row, column), .{}, writer);
        }
        try writer.print("}},\"period\":{{\"name\":{f},\"at\":", .{std.json.fmt(period.name, .{})});
        try std.json.Stringify.value(try mergeObjectField(row, period.start_column), .{}, writer);
        try writer.writeAll("}}}");
        return;
    }
    try writer.writeAll("{\"primary\":{");
    for (primary_key.columns, 0..) |column, i| {
        if (i != 0) try writer.writeByte(',');
        try writer.print("{f}:", .{std.json.fmt(column, .{})});
        try std.json.Stringify.value(try mergeObjectField(row, column), .{}, writer);
    }
    try writer.writeAll("}}");
}

fn writeMergeReturningProjectionJson(writer: *std.Io.Writer, returning: ReturningProjection) !void {
    if (returning.fields.len > 0) {
        try writer.writeAll(",\"returning\":[");
        for (returning.fields, 0..) |field, i| {
            if (i != 0) try writer.writeByte(',');
            try writer.print("{f}", .{std.json.fmt(field, .{})});
        }
        try writer.writeByte(']');
    }
    if (returning.expressions.len == 0) return;
    try writer.writeAll(",\"returning_expressions\":[");
    for (returning.expressions, 0..) |projection, i| {
        if (i != 0) try writer.writeByte(',');
        try writer.print("{{\"as\":{f},\"expr\":", .{std.json.fmt(projection.output, .{})});
        try lower_expr.writeRowExpressionJson(writer, projection.expression);
        try writer.writeByte('}');
    }
    try writer.writeByte(']');
}

pub fn sqlJsonSetPathsConflict(
    lhs_field_path: []const u8,
    lhs_json_path: []const []const u8,
    rhs_field_path: []const u8,
    rhs_json_path: []const []const u8,
) bool {
    if (!std.mem.eql(u8, lhs_field_path, rhs_field_path)) return sqlDottedPathsConflict(lhs_field_path, rhs_field_path);
    const shared = @min(lhs_json_path.len, rhs_json_path.len);
    for (lhs_json_path[0..shared], rhs_json_path[0..shared]) |lhs_segment, rhs_segment| {
        if (!std.mem.eql(u8, lhs_segment, rhs_segment)) return false;
    }
    return true;
}

pub fn sqlDottedPathConflictsJsonSetPath(
    path: []const u8,
    json_field_path: []const u8,
    json_path: []const []const u8,
) bool {
    if (sqlDottedPathsConflict(path, json_field_path)) return true;
    if (path.len <= json_field_path.len + 1) return false;
    if (!std.mem.startsWith(u8, path, json_field_path) or path[json_field_path.len] != '.') return false;
    return sqlJsonSegmentsConflictDottedPath(json_path, path[json_field_path.len + 1 ..]);
}

pub fn jsonSetTypedTransformPathAlloc(
    alloc: std.mem.Allocator,
    field: []const u8,
    path_segments: []const []const u8,
) ![]u8 {
    if (field.len == 0 or path_segments.len == 0) return error.UnsupportedSqlShape;
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.writeAll(field);
    for (path_segments) |segment| {
        if (segment.len == 0 or std.mem.indexOfScalar(u8, segment, '.') != null) return error.UnsupportedSqlShape;
        try writer.writeByte('.');
        try writer.writeAll(segment);
    }
    return try out.toOwnedSlice();
}

pub fn sqlDottedPathsConflict(lhs: []const u8, rhs: []const u8) bool {
    if (std.mem.eql(u8, lhs, rhs)) return true;
    return sqlDottedPathIsAncestor(lhs, rhs) or sqlDottedPathIsAncestor(rhs, lhs);
}

pub fn sqlDottedPathIsAncestor(parent: []const u8, child: []const u8) bool {
    return parent.len < child.len and
        std.mem.startsWith(u8, child, parent) and
        child[parent.len] == '.';
}

pub fn sqlJsonSegmentsConflictDottedPath(json_path: []const []const u8, dotted_path: []const u8) bool {
    if (json_path.len == 0 or dotted_path.len == 0) return false;
    var offset: usize = 0;
    for (json_path, 0..) |segment, i| {
        if (offset >= dotted_path.len) return true;
        if (!std.mem.startsWith(u8, dotted_path[offset..], segment)) return false;
        offset += segment.len;
        const dotted_done = offset == dotted_path.len;
        const json_done = i + 1 == json_path.len;
        if (!dotted_done and dotted_path[offset] != '.') return false;
        if (dotted_done or json_done) return true;
        offset += 1;
    }
    return offset == dotted_path.len;
}

test "sql adapter lower dml detects dotted path conflicts" {
    try std.testing.expect(sqlDottedPathsConflict("metadata", "metadata.status"));
    try std.testing.expect(sqlDottedPathsConflict("metadata.status", "metadata"));
    try std.testing.expect(sqlDottedPathsConflict("metadata.status", "metadata.status"));
    try std.testing.expect(!sqlDottedPathsConflict("metadata.status", "metadata_status"));
    try std.testing.expect(!sqlDottedPathsConflict("metadata.status", "metadata.state"));
}

test "sql adapter lower dml detects json set path conflicts" {
    const status_path = [_][]const u8{"status"};
    const nested_status_path = [_][]const u8{ "profile", "status" };
    const profile_path = [_][]const u8{"profile"};
    const settings_path = [_][]const u8{"settings"};

    try std.testing.expect(sqlJsonSetPathsConflict("metadata", &status_path, "metadata", &status_path));
    try std.testing.expect(sqlJsonSetPathsConflict("metadata", &profile_path, "metadata", &nested_status_path));
    try std.testing.expect(!sqlJsonSetPathsConflict("metadata", &status_path, "metadata", &settings_path));
    try std.testing.expect(sqlJsonSetPathsConflict("metadata", &status_path, "metadata.status", &settings_path));
    try std.testing.expect(sqlDottedPathConflictsJsonSetPath("metadata.profile.status", "metadata", &profile_path));
    try std.testing.expect(sqlDottedPathConflictsJsonSetPath("metadata.profile.status", "metadata", &settings_path));
    try std.testing.expect(!sqlDottedPathConflictsJsonSetPath("profile.status", "metadata", &settings_path));

    const alloc = std.testing.allocator;
    const typed_path = try jsonSetTypedTransformPathAlloc(alloc, "metadata", &nested_status_path);
    defer alloc.free(typed_path);
    try std.testing.expectEqualStrings("metadata.profile.status", typed_path);
    try std.testing.expectError(error.UnsupportedSqlShape, jsonSetTypedTransformPathAlloc(alloc, "metadata", &.{""}));
    try std.testing.expectError(error.UnsupportedSqlShape, jsonSetTypedTransformPathAlloc(alloc, "metadata", &.{"bad.segment"}));
    try std.testing.expectEqualStrings("append", arrayTransformOpToken(.push));
    try std.testing.expectEqualStrings("remove", arrayTransformOpToken(.pull));
    try std.testing.expectEqualStrings("add_to_set", arrayTransformOpToken(.add_to_set));
    try std.testing.expectEqualStrings("update", conflictActionName(.update));
    try std.testing.expectEqualStrings("nothing", conflictActionName(.nothing));
    const conjunction_tokens = [_]Token{
        .{ .kind = .lparen, .text = "(" },
        .{ .kind = .identifier, .text = "active" },
        .{ .kind = .identifier, .text = "and" },
        .{ .kind = .identifier, .text = "verified" },
        .{ .kind = .rparen, .text = ")" },
    };
    try std.testing.expect(conflictParenthesizedConjunctionCanStart(&conjunction_tokens, 0));
    try std.testing.expect(!conflictParenthesizedDisjunctionCanStart(&conjunction_tokens, 0));
    const existing_qualifiers = [_][]const u8{ "usage_records", "public.usage_records" };
    try std.testing.expect(conflictQualifierMatches(alloc, &existing_qualifiers, "usage_records"));
    try std.testing.expect(conflictQualifierMatches(alloc, &existing_qualifiers, "public.usage_records"));
    try std.testing.expectEqualStrings("status", conflictExistingFieldName(alloc, &existing_qualifiers, "public.usage_records.status").?);
    try std.testing.expect(conflictExistingFieldName(alloc, &existing_qualifiers, "other.status") == null);
    const default_column: runtime_schema.RelationalColumn = .{
        .name = "status",
        .path = "status",
        .field_type = .keyword,
        .default_value = .{ .kind = .literal, .value_json = "\"pending\"" },
    };
    try std.testing.expect(conflictProposedColumnAvailable(&.{}, default_column, "status"));
    const required_column: runtime_schema.RelationalColumn = .{
        .name = "status",
        .path = "status",
        .field_type = .keyword,
    };
    try std.testing.expect(!conflictProposedColumnAvailable(&.{}, required_column, "status"));
    try std.testing.expect(conflictProposedColumnAvailable(&.{"status"}, required_column, "status"));
    const source_assignments = [_]JoinedMutationSourceAssignment{
        .{ .field = "status", .source_qualifier = "src", .source_field = "status" },
        .{ .field = "amount", .source_qualifier = "src", .source_field = "amount" },
    };
    try validateJoinedMutationSourceAssignments(&source_assignments, "src");
    try std.testing.expectError(error.UnsupportedSqlShape, validateJoinedMutationSourceAssignments(&source_assignments, "other"));

    const assignments = [_]db_mod.types.RelationalRowsExpressionAssignment{
        .{ .field = "status", .expression = .{ .kind = .field, .field = "source_status" } },
        .{ .field = "status_lower", .expression = .{ .kind = .lower, .operands = &.{.{ .kind = .field, .field = "source_status" }} } },
    };
    try std.testing.expectEqual(@as(usize, 1), expressionAssignmentComputedCount(&assignments));

    const transforms = [_]db_mod.types.DocumentTransform{
        .{ .key = "doc-1", .operations = &.{.{ .op = .set, .path = "status", .value_json = "\"open\"" }} },
        .{ .key = "doc-2", .operations = &.{
            .{ .op = .inc, .path = "count", .value_json = "1" },
            .{ .op = .push, .path = "events", .value_json = "\"opened\"" },
        } },
    };
    try std.testing.expectEqual(@as(usize, 3), transformOperationCount(&transforms));

    const positive = try normalizedIncrementJsonAlloc(alloc, "3.25", false);
    defer alloc.free(positive);
    try std.testing.expectEqualStrings("3.25", positive);
    const negative = try normalizedIncrementJsonAlloc(alloc, "3.25", true);
    defer alloc.free(negative);
    try std.testing.expectEqualStrings("-3.25", negative);
    try std.testing.expectError(error.UnsupportedSqlShape, normalizedIncrementJsonAlloc(alloc, "\"3\"", false));
}

test "sql adapter lower dml detects merge target row usage" {
    const alloc = std.testing.allocator;
    const source_field = runtime_schema.RelationalRowsExpression{
        .kind = .field,
        .field = "status",
        .field_source = .source,
    };
    const target_field = runtime_schema.RelationalRowsExpression{
        .kind = .field,
        .field = "status",
        .field_source = .row,
    };
    const source_condition = runtime_schema.RelationalRowsExpressionCondition{
        .lhs = source_field,
        .op = .eq,
        .rhs = &.{source_field},
    };
    const target_condition = runtime_schema.RelationalRowsExpressionCondition{
        .lhs = source_field,
        .op = .eq,
        .rhs = &.{target_field},
    };
    const source_groups = [_]runtime_schema.RelationalRowsExpressionPredicateGroup{.{ .conditions = &.{source_condition} }};
    const target_groups = [_]runtime_schema.RelationalRowsExpressionPredicateGroup{.{ .conditions = &.{target_condition} }};

    try std.testing.expect(!mergeExpressionUsesTargetRow(source_field));
    try std.testing.expect(mergeExpressionUsesTargetRow(target_field));
    try std.testing.expect(!mergeExpressionPredicateGroupsUseTargetRow(&source_groups));
    try std.testing.expect(mergeExpressionPredicateGroupsUseTargetRow(&target_groups));

    var distinct_row = try std.json.parseFromSlice(std.json.Value, alloc, "{\"status\":\"open\",\"optional\":null}", .{});
    defer distinct_row.deinit();
    try std.testing.expect(try mergePredicateMatches(alloc, distinct_row.value, .{
        .side = .source,
        .field = "status",
        .op = .is_distinct,
        .value_json = "\"closed\"",
    }));
    try std.testing.expect(!try mergePredicateMatches(alloc, distinct_row.value, .{
        .side = .source,
        .field = "status",
        .op = .is_not_distinct,
        .value_json = "\"closed\"",
    }));
    try std.testing.expect(!try mergePredicateMatches(alloc, distinct_row.value, .{
        .side = .source,
        .field = "optional",
        .op = .is_distinct,
        .value_json = "null",
    }));
    try std.testing.expect(try mergePredicateMatches(alloc, distinct_row.value, .{
        .side = .source,
        .field = "optional",
        .op = .is_not_distinct,
        .value_json = "null",
    }));
}

test "sql adapter lower dml appends joined mutation in predicates" {
    const alloc = std.testing.allocator;
    const status_column: runtime_schema.RelationalColumn = .{ .name = "status", .path = "status", .field_type = .keyword };
    const tags_column: runtime_schema.RelationalColumn = .{ .name = "tags", .path = "tags", .field_type = .array, .array_item_type = .keyword };

    var predicates = std.ArrayListUnmanaged(db_mod.types.RelationalRowsInPredicate).empty;
    defer {
        freeInPredicates(alloc, predicates.items);
        predicates.deinit(alloc);
    }
    const values_json = try alloc.dupe(u8, "[\"open\",\"closed\"]");
    var values_transferred = false;
    errdefer if (!values_transferred) alloc.free(values_json);
    try appendJoinedMutationInPredicate(alloc, &predicates, "status", status_column, values_json, true);
    values_transferred = true;
    try std.testing.expectEqual(@as(usize, 1), predicates.items.len);
    try std.testing.expectEqualStrings("status", predicates.items[0].field);
    try std.testing.expectEqualStrings("[\"open\",\"closed\"]", predicates.items[0].values_json);
    try std.testing.expect(predicates.items[0].negated);

    const invalid_values_json = try alloc.dupe(u8, "[\"open\"]");
    defer alloc.free(invalid_values_json);
    try std.testing.expectError(
        error.InvalidSqlCatalog,
        appendJoinedMutationInPredicate(alloc, &predicates, "tags", tags_column, invalid_values_json, false),
    );

    var json_set = std.ArrayListUnmanaged(JsonSetValue).empty;
    defer {
        freeJsonSetValues(alloc, json_set.items);
        json_set.deinit(alloc);
    }
    try appendJsonObjectConcatSetValuesAlloc(alloc, "metadata", "{\"tier\":\"gold\",\"score\":7}", &json_set);
    try std.testing.expectEqual(@as(usize, 2), json_set.items.len);
    var saw_tier = false;
    var saw_score = false;
    for (json_set.items) |value| {
        try std.testing.expectEqualStrings("metadata", value.field);
        try std.testing.expectEqual(@as(usize, 1), value.path.len);
        if (std.mem.eql(u8, value.path[0], "tier")) {
            try std.testing.expectEqualStrings("\"gold\"", value.value_json.?);
            saw_tier = true;
        } else if (std.mem.eql(u8, value.path[0], "score")) {
            try std.testing.expectEqualStrings("7", value.value_json.?);
            saw_score = true;
        }
    }
    try std.testing.expect(saw_tier);
    try std.testing.expect(saw_score);
    try std.testing.expectError(error.UnsupportedSqlShape, appendJsonObjectConcatSetValuesAlloc(alloc, "metadata", "{}", &json_set));
    try std.testing.expectError(error.UnsupportedSqlShape, appendJsonObjectConcatSetValuesAlloc(alloc, "metadata", "{\"bad.key\":1}", &json_set));
}

test "sql adapter lower dml resolves joined mutation CTE source" {
    const alloc = std.testing.allocator;
    const ctes = [_]db_mod.types.RelationalRowsCte{.{ .name = "recent_usage" }};

    var no_cte_tail = ParsedJoinedMutationTail{ .join = .{} };
    defer no_cte_tail.deinit(alloc);
    try std.testing.expectEqualStrings(
        "usage_records",
        try resolveJoinedMutationSourceForCtesAlloc(alloc, &no_cte_tail, "usage_records", &ctes, null),
    );
    try std.testing.expectEqualStrings("", no_cte_tail.join.right.source_cte);

    var base_table_name: ?[]const u8 = try alloc.dupe(u8, "usage_records");
    defer if (base_table_name) |table| alloc.free(table);

    var cte_tail = ParsedJoinedMutationTail{ .join = .{} };
    defer cte_tail.deinit(alloc);
    const resolved = try resolveJoinedMutationSourceForCtesAlloc(alloc, &cte_tail, "recent_usage", &ctes, &base_table_name);
    try std.testing.expectEqualStrings("usage_records", resolved);
    try std.testing.expectEqualStrings("recent_usage", cte_tail.join.right.source_cte);

    var mismatch_tail = ParsedJoinedMutationTail{ .join = .{} };
    defer mismatch_tail.deinit(alloc);
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        resolveJoinedMutationSourceForCtesAlloc(alloc, &mismatch_tail, "other_records", &ctes, &base_table_name),
    );
}
