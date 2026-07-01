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

const ddl_plan = @import("ddl_plan.zig");
const expr_type = @import("expr/type.zig");
const expr_projection = @import("expr/projection.zig");
const generated_parser = @import("generated_parser.zig");
const lower_dml = @import("lower_dml.zig");
const lower_expr = @import("lower_expr.zig");
const expr_row_parse = @import("expr/row_parse.zig");
const plan = @import("plan.zig");
const relational_rows = @import("relational_rows.zig");
const runtime_schema = @import("../storage/schema.zig");
const db_mod = @import("../storage/db/mod.zig");
const token_mod = @import("token.zig");
const value_mod = @import("value.zig");

const Token = token_mod.Token;

pub const ParserState = struct {
    pub const ContextAccessors = ParserContextAccessors(@This());

    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: usize = 0,
    schema: runtime_schema.TableSchema = .{},
    joined_source_schema: ?runtime_schema.TableSchema = null,
    params: []const value_mod.SqlValue = &.{},
    function_bindings: expr_row_parse.SqlFunctionBindings = .{},
    unique_resolver: ?relational_rows.UniqueSelectorResolver = null,
    default_context: relational_rows.DefaultValueContext = .{},
    available_ctes: []const db_mod.types.RelationalRowsCte = &.{},
    mutation_claim: ?db_mod.types.RowClaimRequest = null,
    returning_expression_qualifiers: []const []const u8 = &.{},
    field_expression_qualifiers: []const []const u8 = &.{},
    joined_target_expression_qualifiers: []const []const u8 = &.{},
    joined_source_expression_qualifiers: []const []const u8 = &.{},
    pending_joined_source_alias: ?[]const u8 = null,
    named_window_specs: []const plan.NamedWindowSpec = &.{},
    insert_source_allows_different_table: bool = false,
    row_expression_field_source_override: ?db_mod.types.RelationalRowsExpressionFieldSource = null,
    defer_row_expression_field_validation: bool = false,
    conflict_existing_qualifiers: []const []const u8 = &.{},
    allow_primary_key_assignment: bool = false,
    saw_primary_key_assignment: bool = false,
    allow_select_set_boundary: bool = false,
    allow_select_set_result_tail_boundary: bool = false,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst = null,
    generated_returning_items: ?*const generated_parser.GeneratedSqlListAst = null,
    generated_assignment_items: ?*const generated_parser.GeneratedSqlListAst = null,
    generated_conflict_assignments_tokens: ?generated_parser.GeneratedSqlTokenRange = null,
    generated_insert_column_items: ?*const generated_parser.GeneratedSqlListAst = null,
    generated_conflict_target_tokens: ?generated_parser.GeneratedSqlTokenRange = null,
    generated_conflict_target_kind: ?generated_parser.GeneratedSqlConflictTargetKind = null,
    generated_conflict_target_items: ?*const generated_parser.GeneratedSqlListAst = null,
    generated_conflict_target_where_tokens: ?generated_parser.GeneratedSqlTokenRange = null,
    generated_conflict_target_where_expression: ?*const generated_parser.GeneratedSqlExpressionAst = null,
    generated_conflict_action_tokens: ?generated_parser.GeneratedSqlTokenRange = null,
    generated_conflict_action_kind: ?generated_parser.GeneratedSqlConflictActionKind = null,
    generated_conflict_action_where_tokens: ?generated_parser.GeneratedSqlTokenRange = null,
    generated_conflict_action_where_expression: ?*const generated_parser.GeneratedSqlExpressionAst = null,
    generated_dml_ast: ?*const generated_parser.GeneratedSqlDmlAst = null,
    generated_merge_arms: ?*const generated_parser.GeneratedSqlMergeArmListAst = null,
};

pub fn ParserContextAccessors(comptime ParserType: type) type {
    return struct {
        const Accessors = @This();

        pub fn selectParserContextHooks(ptr: *ParserType) lower_expr.SelectParserContextHooks {
            return .{
                .ptr = ptr,
                .get_context = Accessors.getSelectParserContextHook,
                .set_context = Accessors.setSelectParserContextHook,
                .row_expression_type_context = Accessors.selectParserRowExpressionTypeContextHook,
            };
        }

        pub fn namedWindowSpecsContextHooks(ptr: *ParserType) lower_expr.NamedWindowSpecsContextHooks {
            return .{
                .ptr = ptr,
                .get_specs = Accessors.getNamedWindowSpecsHook,
                .set_specs = Accessors.setNamedWindowSpecsHook,
            };
        }

        pub fn joinParserContextHooks(ptr: *ParserType) lower_expr.JoinParserContextHooks {
            return .{
                .ptr = ptr,
                .get_context = Accessors.getJoinParserContextHook,
                .set_context = Accessors.setJoinParserContextHook,
            };
        }

        pub fn joinedExpressionParserContextHooks(ptr: *ParserType) lower_expr.JoinedExpressionParserContextHooks {
            return .{
                .ptr = ptr,
                .get_context = Accessors.getJoinedExpressionParserContextHook,
                .set_context = Accessors.setJoinedExpressionParserContextHook,
                .row_expression_type_context = Accessors.selectParserRowExpressionTypeContextHook,
            };
        }

        pub fn mutationSourceParserContextHooks(ptr: *ParserType) lower_dml.MutationSourceParserContextHooks {
            return .{
                .ptr = ptr,
                .get_context = Accessors.getMutationSourceParserContextHook,
                .set_context = Accessors.setMutationSourceParserContextHook,
            };
        }

        pub fn readPlanParserContextHooks(ptr: *ParserType) plan.ReadPlanParserContextHooks {
            return .{
                .ptr = ptr,
                .get_context = Accessors.getReadPlanParserContextHook,
                .set_context = Accessors.setReadPlanParserContextHook,
            };
        }

        pub fn mergeMutationParserContextHooks(ptr: *ParserType) lower_dml.MergeMutationParserContextHooks {
            return .{
                .ptr = ptr,
                .get_context = Accessors.getMergeMutationParserContextHook,
                .set_context = Accessors.setMergeMutationParserContextHook,
            };
        }

        pub fn conflictClauseParserContextHooks(ptr: *ParserType) lower_dml.ConflictClauseParserContextHooks {
            return .{
                .ptr = ptr,
                .get_context = Accessors.getConflictClauseParserContextHook,
                .set_context = Accessors.setConflictClauseParserContextHook,
                .row_expression_type_context = Accessors.selectParserRowExpressionTypeContextHook,
            };
        }

        pub fn joinedMutationSourceParserContextHooks(ptr: *ParserType) lower_dml.JoinedMutationSourceParserContextHooks {
            return .{
                .ptr = ptr,
                .get_context = Accessors.getJoinedMutationSourceParserContextHook,
                .set_context = Accessors.setJoinedMutationSourceParserContextHook,
            };
        }

        pub fn semiJoinSourceQueryParserContextHooks(ptr: *ParserType) lower_dml.SemiJoinSourceQueryParserContextHooks {
            return .{
                .ptr = ptr,
                .get_context = Accessors.getSemiJoinSourceQueryParserContextHook,
                .set_context = Accessors.setSemiJoinSourceQueryParserContextHook,
            };
        }

        pub fn cteSelectParserHooks(ptr: *ParserType) plan.CteSelectParserHooks {
            return .{
                .ptr = ptr,
                .generated_read_ast = ptr.generated_read_ast,
                .parse_select = Accessors.parseCteSelectHook,
            };
        }

        pub fn joinCteSelectParserHooks(ptr: *ParserType) plan.CteSelectParserHooks {
            return .{
                .ptr = ptr,
                .generated_read_ast = ptr.generated_read_ast,
                .parse_select = Accessors.parseJoinCteSelectHook,
            };
        }

        pub fn queryPlanParserHooks(ptr: *ParserType) lower_expr.QueryPlanParserHooks {
            return .{
                .ptr = ptr,
                .context_hooks = Accessors.readPlanParserContextHooks(ptr),
                .parse_select = Accessors.parseQueryPlanSelectHook,
            };
        }

        pub fn windowPlanParserHooks(ptr: *ParserType) plan.WindowPlanParserHooks {
            return .{
                .ptr = ptr,
                .context_hooks = Accessors.readPlanParserContextHooks(ptr),
                .parse_window = Accessors.parseWindowPlanHook,
            };
        }

        pub fn aggregatePlanParserHooks(ptr: *ParserType) plan.AggregatePlanParserHooks {
            return .{
                .ptr = ptr,
                .context_hooks = Accessors.readPlanParserContextHooks(ptr),
                .parse_aggregate = Accessors.parseAggregatePlanHook,
            };
        }

        pub fn joinPlanParserHooks(ptr: *ParserType) plan.JoinPlanParserHooks {
            return .{
                .ptr = ptr,
                .context_hooks = Accessors.readPlanParserContextHooks(ptr),
                .parse_join = Accessors.parseJoinPlanHook,
            };
        }

        pub fn lateralPlanParserHooks(ptr: *ParserType) plan.LateralPlanParserHooks {
            return .{
                .ptr = ptr,
                .context_hooks = Accessors.readPlanParserContextHooks(ptr),
                .parse_lateral = Accessors.parseLateralPlanHook,
            };
        }

        pub fn recursiveCteParserHooks(ptr: *ParserType) plan.RecursiveCteParserHooks {
            return .{
                .ptr = ptr,
                .generated_read_ast = ptr.generated_read_ast,
                .parse_select_with_set_boundary = Accessors.parseRecursiveCteSelectWithSetBoundaryHook,
                .select_output_columns = Accessors.selectOutputColumnsHook,
                .parse_recursive_member = Accessors.parseRecursiveCteMemberHook,
            };
        }

        pub fn recursiveCteMemberParserHooks(ptr: *ParserType) plan.RecursiveCteMemberParserHooks {
            return .{
                .ptr = ptr,
                .generated_read_ast = ptr.generated_read_ast,
                .parse_projection_expression = Accessors.parseRecursiveCteMemberProjectionExpressionHook,
                .parse_join_on = Accessors.parseRecursiveCteMemberJoinOnHook,
            };
        }

        pub fn setOperationParserHooks(ptr: *ParserType) plan.SetOperationParserHooks {
            return .{
                .ptr = ptr,
                .cte_hooks = Accessors.cteSelectParserHooks(ptr),
                .context_hooks = Accessors.readPlanParserContextHooks(ptr),
                .parse_select = Accessors.parseSetOperationSelectHook,
                .select_output_columns = Accessors.selectOutputColumnsHook,
                .parse_result_tail = Accessors.parseSetOperationResultTailHook,
            };
        }

        pub fn simpleSelectSetTailHooks(ptr: *ParserType) plan.SimpleSelectSetTailHooks {
            return .{
                .ptr = ptr,
                .parse_order_by = Accessors.parseSimpleSelectSetTailOrderByHook,
            };
        }

        pub fn updateJoinedMutationSourceParserHooks(ptr: *ParserType) lower_dml.JoinedMutationSourceParserHooks {
            return .{
                .ptr = ptr,
                .parse_joined_mutation_source = Accessors.parseUpdateJoinedMutationSourceWithCtesHook,
            };
        }

        pub fn deleteJoinedMutationSourceParserHooks(ptr: *ParserType) lower_dml.JoinedMutationSourceParserHooks {
            return .{
                .ptr = ptr,
                .parse_joined_mutation_source = Accessors.parseDeleteJoinedMutationSourceWithCtesHook,
            };
        }

        pub fn insertSourceParserHooks(ptr: *ParserType) lower_dml.InsertSourceParserHooks {
            return .{
                .ptr = ptr,
                .params = ptr.params,
                .schema = ptr.schema,
                .joined_source_schema = ptr.joined_source_schema,
                .insert_source_allows_different_table = ptr.insert_source_allows_different_table,
                .defer_row_expression_field_validation = ptr.defer_row_expression_field_validation,
                .conflict_context_hooks = Accessors.conflictClauseParserContextHooks(ptr),
                .conflict_target_options = Accessors.conflictTargetParserOptions(ptr),
                .conflict_assignment_hooks = Accessors.conflictUpdateSetAssignmentParserOptions(ptr, &.{}, &.{}),
                .generated_dml_ast = ptr.generated_dml_ast,
                .generated_conflict_assignment_items = ptr.generated_assignment_items,
                .generated_conflict_assignments_tokens = ptr.generated_conflict_assignments_tokens,
                .generated_conflict_action_tokens = ptr.generated_conflict_action_tokens,
                .generated_conflict_action_kind = ptr.generated_conflict_action_kind,
                .generated_insert_column_items = ptr.generated_insert_column_items,
                .conflict_condition_options = Accessors.conflictAssignmentExpressionParserOptions(ptr),
                .conflict_dispatch_options = Accessors.conflictExpressionDispatchOptions(ptr),
                .returning_hooks = Accessors.returningProjectionParserOptions(ptr),
                .parse_select = Accessors.parseInsertSourceSelectHook,
            };
        }

        pub fn rowExpressionTypeContext(ptr: *ParserType) expr_type.RowExpressionTypeContext {
            return expr_type.rowExpressionTypeContext(
                ptr.alloc,
                ptr.schema,
                ptr.joined_source_schema,
                ptr.defer_row_expression_field_validation,
            );
        }

        pub fn ddlColumnDefinitionOptions(ptr: *ParserType) ddl_plan.DdlColumnDefinitionOptions {
            return .{
                .schema = ptr.schema,
                .params = ptr.params,
                .function_bindings = ptr.function_bindings,
                .field_expression_qualifiers = ptr.field_expression_qualifiers,
                .returning_expression_qualifiers = ptr.returning_expression_qualifiers,
                .defer_row_expression_field_validation = ptr.defer_row_expression_field_validation,
                .expression_options = Accessors.ddlExpressionOptions(ptr),
                .parse_rewrite_expression = true,
            };
        }

        pub fn ddlDomainOptions(ptr: *ParserType) ddl_plan.DdlDomainOptions {
            return .{
                .params = ptr.params,
                .realtime_ns = value_mod.currentRealtimeNs(),
            };
        }

        pub fn ddlExpressionOptions(ptr: *ParserType) ddl_plan.DdlExpressionOptions {
            return .{
                .params = ptr.params,
                .realtime_ns = value_mod.currentRealtimeNs(),
                .function_bindings = ptr.function_bindings,
                .context_hooks = Accessors.selectParserContextHooks(ptr),
                .row_expression_hooks = Accessors.rowExpressionParserHooks(ptr),
                .arithmetic_hooks = Accessors.arithmeticExpressionParserHooks(ptr),
                .variadic_hooks = Accessors.variadicRowExpressionParserHooks(ptr),
                .routine_expression = Accessors.routineExpressionRowExpressionParserOptions(ptr),
                .condition_alternatives = Accessors.expressionWhereConditionAlternativesParserHooks(ptr),
            };
        }

        pub fn createIndexOptions(ptr: *ParserType) ddl_plan.CreateIndexOptions {
            return .{
                .schema = ptr.schema,
                .params = ptr.params,
                .function_bindings = ptr.function_bindings,
                .field_expression_qualifiers = ptr.field_expression_qualifiers,
                .returning_expression_qualifiers = ptr.returning_expression_qualifiers,
                .defer_row_expression_field_validation = ptr.defer_row_expression_field_validation,
                .context_hooks = Accessors.selectParserContextHooks(ptr),
                .case_expression_hooks = Accessors.caseExpressionParserHooks(ptr),
            };
        }

        pub fn rowSecurityPolicyOptions(ptr: *ParserType) ddl_plan.RowSecurityPolicyOptions {
            return .{
                .function_bindings = ptr.function_bindings,
            };
        }

        pub fn lateralSubqueryParserHooks(ptr: *ParserType) lower_expr.LateralSubqueryParserHooks {
            return .{
                .ptr = ptr,
                .parse_subquery = Accessors.parseLateralSubqueryHook,
            };
        }

        pub fn rowExpressionParserHooks(ptr: *ParserType) expr_row_parse.RowExpressionParserHooks {
            return .{
                .ptr = ptr,
                .parse_operand = Accessors.parseRowExpressionOperandHook,
            };
        }

        pub fn variadicRowExpressionParserHooks(ptr: *ParserType) expr_row_parse.VariadicRowExpressionParserHooks {
            return .{
                .ptr = ptr,
                .parse_expression = Accessors.parseFixedUnaryRowExpressionOperandHook,
                .parse_operand = Accessors.parseRowExpressionOperandHook,
            };
        }

        pub fn arithmeticExpressionParserHooks(ptr: *ParserType) expr_row_parse.ArithmeticExpressionParserHooks {
            return .{
                .ptr = ptr,
                .parse_operand = Accessors.parseRowExpressionOperandHook,
                .parenthesized = Accessors.parenthesizedRowExpressionParserOptions(ptr),
            };
        }

        pub fn booleanExpressionParserHooks(ptr: *ParserType) expr_row_parse.BooleanExpressionParserHooks {
            return .{
                .ptr = ptr,
                .parse_operand = Accessors.parseRowExpressionOperandHook,
            };
        }

        pub fn booleanRowExpressionParserHooks(ptr: *ParserType) expr_row_parse.BooleanRowExpressionParserHooks {
            return .{
                .ptr = ptr,
                .parse_expression = Accessors.parseFixedUnaryRowExpressionOperandHook,
                .parse_operand = Accessors.parseRowExpressionOperandHook,
            };
        }

        pub fn caseExpressionParserHooks(ptr: *ParserType) expr_row_parse.CaseExpressionParserHooks {
            return .{
                .ptr = ptr,
                .parse_expression = Accessors.parseFixedUnaryRowExpressionOperandHook,
                .parse_operand = Accessors.parseRowExpressionOperandHook,
            };
        }

        pub fn extensionFunctionRowExpressionParserOptions(ptr: *ParserType) expr_row_parse.ExtensionFunctionRowExpressionParserOptions {
            return .{
                .type_context = Accessors.rowExpressionTypeContext(ptr),
                .row_expression_hooks = Accessors.rowExpressionParserHooks(ptr),
                .arithmetic_hooks = Accessors.arithmeticExpressionParserHooks(ptr),
                .variadic_hooks = Accessors.variadicRowExpressionParserHooks(ptr),
            };
        }

        pub fn routineExpressionRowExpressionParserOptions(ptr: *ParserType) expr_row_parse.RoutineExpressionRowExpressionParserOptions {
            return .{
                .type_context = Accessors.rowExpressionTypeContext(ptr),
                .boolean_hooks = Accessors.booleanRowExpressionParserHooks(ptr),
            };
        }

        pub fn parenthesizedRowExpressionParserOptions(ptr: *ParserType) expr_row_parse.ParenthesizedRowExpressionParserOptions {
            return .{
                .type_context = Accessors.rowExpressionTypeContext(ptr),
                .boolean_hooks = Accessors.booleanRowExpressionParserHooks(ptr),
            };
        }

        pub fn castRowExpressionParserOptions(ptr: *ParserType) expr_row_parse.CastRowExpressionParserOptions {
            return .{
                .type_context = Accessors.rowExpressionTypeContext(ptr),
                .row_expression_hooks = Accessors.rowExpressionParserHooks(ptr),
                .arithmetic_hooks = Accessors.arithmeticExpressionParserHooks(ptr),
                .variadic_hooks = Accessors.variadicRowExpressionParserHooks(ptr),
            };
        }

        pub fn coalesceRowExpressionParserOptions(ptr: *ParserType) expr_row_parse.CoalesceRowExpressionParserOptions {
            return .{
                .type_context = Accessors.rowExpressionTypeContext(ptr),
                .row_expression_hooks = Accessors.rowExpressionParserHooks(ptr),
                .arithmetic_hooks = Accessors.arithmeticExpressionParserHooks(ptr),
                .variadic_hooks = Accessors.variadicRowExpressionParserHooks(ptr),
            };
        }

        pub fn expressionProjectionParserOptions(ptr: *ParserType) expr_projection.ExpressionProjectionParserOptions {
            return .{
                .type_context = Accessors.rowExpressionTypeContext(ptr),
                .row_expression_hooks = Accessors.rowExpressionParserHooks(ptr),
                .arithmetic_hooks = Accessors.arithmeticExpressionParserHooks(ptr),
                .variadic_hooks = Accessors.variadicRowExpressionParserHooks(ptr),
                .boolean_hooks = Accessors.booleanRowExpressionParserHooks(ptr),
            };
        }

        pub fn fixedUnaryRowExpressionParserOptions(ptr: *ParserType) expr_row_parse.FixedUnaryRowExpressionParserOptions {
            return .{
                .row_expression_hooks = Accessors.rowExpressionParserHooks(ptr),
                .arithmetic_hooks = Accessors.arithmeticExpressionParserHooks(ptr),
                .variadic_hooks = Accessors.variadicRowExpressionParserHooks(ptr),
            };
        }

        pub fn fixedBinaryRowExpressionParserOptions(ptr: *ParserType) expr_row_parse.FixedBinaryRowExpressionParserOptions {
            return .{
                .row_expression_hooks = Accessors.rowExpressionParserHooks(ptr),
                .arithmetic_hooks = Accessors.arithmeticExpressionParserHooks(ptr),
                .variadic_hooks = Accessors.variadicRowExpressionParserHooks(ptr),
            };
        }

        pub fn caseFoldRowExpressionParserOptions(ptr: *ParserType) expr_row_parse.CaseFoldRowExpressionParserOptions {
            return .{
                .row_expression_hooks = Accessors.rowExpressionParserHooks(ptr),
                .arithmetic_hooks = Accessors.arithmeticExpressionParserHooks(ptr),
                .variadic_hooks = Accessors.variadicRowExpressionParserHooks(ptr),
            };
        }

        pub fn jsonBuildObjectRowExpressionParserOptions(ptr: *ParserType) expr_row_parse.JsonBuildObjectRowExpressionParserOptions {
            return .{
                .row_expression_hooks = Accessors.rowExpressionParserHooks(ptr),
                .arithmetic_hooks = Accessors.arithmeticExpressionParserHooks(ptr),
                .variadic_hooks = Accessors.variadicRowExpressionParserHooks(ptr),
            };
        }

        pub fn aggregateOutputFieldExpressionConditionParserOptions(ptr: *ParserType) lower_expr.AggregateOutputFieldExpressionConditionParserOptions {
            return .{
                .params = ptr.params,
                .output_field_options = Accessors.aggregateOutputFieldParserOptions(ptr),
            };
        }

        pub fn bareBooleanWhereExpressionParserOptions(ptr: *ParserType) lower_expr.BareBooleanWhereExpressionParserOptions {
            return .{
                .boolean_hooks = Accessors.booleanRowExpressionParserHooks(ptr),
            };
        }

        pub fn outputOrderExpressionParserOptions(ptr: *ParserType) lower_expr.OutputOrderExpressionParserOptions {
            return .{
                .function_bindings = ptr.function_bindings,
                .context_hooks = Accessors.selectParserContextHooks(ptr),
                .order_expression_hooks = Accessors.orderExpressionParserOptions(ptr),
            };
        }

        pub fn returningProjectionParserOptions(ptr: *ParserType) lower_expr.ReturningProjectionParserOptions {
            return .{
                .params = ptr.params,
                .function_bindings = ptr.function_bindings,
                .field_source = expr_type.rowExpressionFieldSourceOrDefault(ptr.row_expression_field_source_override),
                .context_hooks = Accessors.selectParserContextHooks(ptr),
                .select_item_options = Accessors.selectItemParserOptions(ptr),
                .generated_returning_items = ptr.generated_returning_items,
            };
        }

        pub fn joinedMutationReturningProjectionParserOptions(ptr: *ParserType) lower_expr.JoinedMutationReturningProjectionParserOptions {
            return .{
                .params = ptr.params,
                .function_bindings = ptr.function_bindings,
                .field_source = expr_type.rowExpressionFieldSourceOrDefault(ptr.row_expression_field_source_override),
                .select_context_hooks = Accessors.selectParserContextHooks(ptr),
                .joined_context_hooks = Accessors.joinedExpressionParserContextHooks(ptr),
                .select_item_options = Accessors.selectItemParserOptions(ptr),
                .generated_returning_items = ptr.generated_returning_items,
            };
        }

        pub fn orderByParserOptions(ptr: *ParserType) lower_expr.OrderByParserOptions {
            return .{
                .schema = ptr.schema,
                .function_bindings = ptr.function_bindings,
                .field_expression_qualifiers = ptr.field_expression_qualifiers,
                .returning_expression_qualifiers = ptr.returning_expression_qualifiers,
                .defer_row_expression_field_validation = ptr.defer_row_expression_field_validation,
                .type_context = Accessors.rowExpressionTypeContext(ptr),
                .order_expression_hooks = Accessors.orderExpressionParserOptions(ptr),
                .generated_read_ast = ptr.generated_read_ast,
            };
        }

        pub fn orderExpressionParserOptions(ptr: *ParserType) lower_expr.OrderExpressionParserOptions {
            return .{
                .field_source = expr_type.rowExpressionFieldSourceOrDefault(ptr.row_expression_field_source_override),
                .row_expression_hooks = Accessors.rowExpressionParserHooks(ptr),
                .arithmetic_hooks = Accessors.arithmeticExpressionParserHooks(ptr),
                .variadic_hooks = Accessors.variadicRowExpressionParserHooks(ptr),
                .parenthesized = Accessors.parenthesizedRowExpressionParserOptions(ptr),
                .case_fold_hooks = Accessors.caseFoldRowExpressionParserOptions(ptr),
                .fixed_unary = Accessors.fixedUnaryRowExpressionParserOptions(ptr),
            };
        }

        pub fn aggregateSpecParserOptions(ptr: *ParserType) lower_expr.AggregateSpecParserOptions {
            return .{
                .function_bindings = ptr.function_bindings,
                .order_expression_hooks = Accessors.orderExpressionParserOptions(ptr),
                .aggregate_input = .{
                    .context_hooks = Accessors.selectParserContextHooks(ptr),
                    .row_expression_hooks = Accessors.rowExpressionParserHooks(ptr),
                    .arithmetic_hooks = Accessors.arithmeticExpressionParserHooks(ptr),
                    .variadic_hooks = Accessors.variadicRowExpressionParserHooks(ptr),
                },
                .expression_alternatives = Accessors.expressionWhereConditionAlternativesParserHooks(ptr),
                .expression_conditions = Accessors.expressionWhereConditionsParserHooks(ptr),
                .fixed_binary = Accessors.fixedBinaryRowExpressionParserOptions(ptr),
                .realtime_ns = value_mod.currentRealtimeNs(),
            };
        }

        pub fn aggregateOutputFieldParserOptions(ptr: *ParserType) lower_expr.AggregateOutputFieldParserOptions {
            return .{
                .params = ptr.params,
                .context_hooks = Accessors.selectParserContextHooks(ptr),
                .aggregate_spec_options = Accessors.aggregateSpecParserOptions(ptr),
            };
        }

        pub fn joinedMutationExpressionWhereOptions(ptr: *ParserType) lower_expr.JoinedMutationExpressionWhereConditionParserOptions {
            return .{
                .params = ptr.params,
                .joined_source_schema = ptr.joined_source_schema,
                .select_context_hooks = Accessors.selectParserContextHooks(ptr),
                .joined_context_hooks = Accessors.joinedExpressionParserContextHooks(ptr),
                .alternatives_hooks = Accessors.expressionWhereConditionAlternativesParserHooks(ptr),
                .fixed_binary_hooks = Accessors.fixedBinaryRowExpressionParserOptions(ptr),
                .bare_boolean_hooks = Accessors.bareBooleanWhereExpressionParserOptions(ptr),
                .expression_condition_hooks = Accessors.expressionWhereConditionsParserHooks(ptr),
            };
        }

        pub fn expressionWhereConditionsParserHooks(ptr: *ParserType) lower_expr.ExpressionWhereConditionsParserOptions {
            return .{
                .select_context_hooks = Accessors.selectParserContextHooks(ptr),
                .joined_context_hooks = Accessors.joinedExpressionParserContextHooks(ptr),
                .row_expression_hooks = Accessors.rowExpressionParserHooks(ptr),
                .arithmetic_hooks = Accessors.arithmeticExpressionParserHooks(ptr),
                .variadic_hooks = Accessors.variadicRowExpressionParserHooks(ptr),
            };
        }

        pub fn expressionWhereConditionAlternativesParserHooks(ptr: *ParserType) lower_expr.ExpressionWhereConditionAlternativesParserOptions {
            return .{
                .select_context_hooks = Accessors.selectParserContextHooks(ptr),
                .joined_context_hooks = Accessors.joinedExpressionParserContextHooks(ptr),
                .row_expression_hooks = Accessors.rowExpressionParserHooks(ptr),
                .arithmetic_hooks = Accessors.arithmeticExpressionParserHooks(ptr),
                .variadic_hooks = Accessors.variadicRowExpressionParserHooks(ptr),
            };
        }

        pub fn selectFieldItemParserOptions(ptr: *ParserType) lower_expr.SelectFieldItemParserOptions {
            return .{
                .type_context = Accessors.rowExpressionTypeContext(ptr),
                .arithmetic_hooks = Accessors.arithmeticExpressionParserHooks(ptr),
                .boolean_hooks = Accessors.booleanExpressionParserHooks(ptr),
            };
        }

        pub fn coalesceProjectionParserOptions(ptr: *ParserType) lower_expr.CoalesceProjectionParserOptions {
            return .{
                .params = ptr.params,
            };
        }

        pub fn jsonValueExpressionProjectionParserOptions(ptr: *ParserType) expr_projection.JsonValueExpressionProjectionParserOptions {
            return .{
                .params = ptr.params,
            };
        }

        pub fn selectItemParserOptions(ptr: *ParserType) lower_expr.SelectItemParserOptions {
            return .{
                .expression = Accessors.expressionProjectionParserOptions(ptr),
                .json_value_expression = Accessors.jsonValueExpressionProjectionParserOptions(ptr),
                .select_field = Accessors.selectFieldItemParserOptions(ptr),
                .extension_function = Accessors.extensionFunctionRowExpressionParserOptions(ptr),
                .routine_expression = Accessors.routineExpressionRowExpressionParserOptions(ptr),
            };
        }

        pub fn windowSpecParserOptions(ptr: *ParserType) lower_expr.WindowSpecParserOptions {
            return .{
                .row_expression_hooks = Accessors.rowExpressionParserHooks(ptr),
                .arithmetic_hooks = Accessors.arithmeticExpressionParserHooks(ptr),
                .variadic_hooks = Accessors.variadicRowExpressionParserHooks(ptr),
                .boolean_hooks = Accessors.booleanRowExpressionParserHooks(ptr),
                .expression_alternatives = Accessors.expressionWhereConditionAlternativesParserHooks(ptr),
                .expression_conditions = Accessors.expressionWhereConditionsParserHooks(ptr),
                .fixed_binary = Accessors.fixedBinaryRowExpressionParserOptions(ptr),
                .realtime_ns = value_mod.currentRealtimeNs(),
            };
        }

        pub fn rowExpressionOperandParserOptions(ptr: *ParserType) lower_expr.RowExpressionOperandParserOptions {
            return .{
                .extension_function = Accessors.extensionFunctionRowExpressionParserOptions(ptr),
                .routine_expression = Accessors.routineExpressionRowExpressionParserOptions(ptr),
                .parenthesized = Accessors.parenthesizedRowExpressionParserOptions(ptr),
                .cast = Accessors.castRowExpressionParserOptions(ptr),
                .case_expression = Accessors.caseExpressionParserHooks(ptr),
                .case_fold = Accessors.caseFoldRowExpressionParserOptions(ptr),
                .coalesce = Accessors.coalesceRowExpressionParserOptions(ptr),
                .fixed_unary = Accessors.fixedUnaryRowExpressionParserOptions(ptr),
                .fixed_binary = Accessors.fixedBinaryRowExpressionParserOptions(ptr),
                .variadic = Accessors.variadicRowExpressionParserHooks(ptr),
                .json_build_object = Accessors.jsonBuildObjectRowExpressionParserOptions(ptr),
            };
        }

        pub fn selectParserOptions(ptr: *ParserType) lower_expr.SelectParserOptions {
            return .{
                .params = ptr.params,
                .available_ctes = ptr.available_ctes,
                .function_bindings = ptr.function_bindings,
                .field_source = expr_type.rowExpressionFieldSourceOrDefault(ptr.row_expression_field_source_override),
                .allow_select_set_boundary = ptr.allow_select_set_boundary,
                .allow_select_set_result_tail_boundary = ptr.allow_select_set_result_tail_boundary,
                .generated_read_ast = ptr.generated_read_ast,
                .context_hooks = Accessors.selectParserContextHooks(ptr),
                .select_item_options = Accessors.selectItemParserOptions(ptr),
                .row_expression_hooks = Accessors.rowExpressionParserHooks(ptr),
                .arithmetic_hooks = Accessors.arithmeticExpressionParserHooks(ptr),
                .variadic_hooks = Accessors.variadicRowExpressionParserHooks(ptr),
                .fixed_binary_hooks = Accessors.fixedBinaryRowExpressionParserOptions(ptr),
                .bare_boolean_hooks = Accessors.bareBooleanWhereExpressionParserOptions(ptr),
                .expression_alternatives_hooks = Accessors.expressionWhereConditionAlternativesParserHooks(ptr),
                .expression_condition_hooks = Accessors.expressionWhereConditionsParserHooks(ptr),
                .order_expression_hooks = Accessors.orderExpressionParserOptions(ptr),
                .realtime_ns = value_mod.currentRealtimeNs(),
            };
        }

        pub fn windowParserOptions(ptr: *ParserType) lower_expr.WindowParserOptions {
            return .{
                .params = ptr.params,
                .available_ctes = ptr.available_ctes,
                .context_hooks = Accessors.selectParserContextHooks(ptr),
                .named_window_hooks = Accessors.namedWindowSpecsContextHooks(ptr),
                .generated_read_ast = ptr.generated_read_ast,
                .fixed_binary_hooks = Accessors.fixedBinaryRowExpressionParserOptions(ptr),
                .bare_boolean_hooks = Accessors.bareBooleanWhereExpressionParserOptions(ptr),
                .expression_alternatives_hooks = Accessors.expressionWhereConditionAlternativesParserHooks(ptr),
                .expression_condition_hooks = Accessors.expressionWhereConditionsParserHooks(ptr),
                .function_bindings = ptr.function_bindings,
                .order_expression_hooks = Accessors.orderExpressionParserOptions(ptr),
                .window_spec_options = Accessors.windowSpecParserOptions(ptr),
                .output_order_expression_options = Accessors.outputOrderExpressionParserOptions(ptr),
                .realtime_ns = value_mod.currentRealtimeNs(),
            };
        }

        pub fn aggregateParserOptions(ptr: *ParserType) lower_expr.AggregateParserOptions {
            return .{
                .params = ptr.params,
                .available_ctes = ptr.available_ctes,
                .function_bindings = ptr.function_bindings,
                .field_source = expr_type.rowExpressionFieldSourceOrDefault(ptr.row_expression_field_source_override),
                .generated_read_ast = ptr.generated_read_ast,
                .context_hooks = Accessors.selectParserContextHooks(ptr),
                .aggregate_spec_options = Accessors.aggregateSpecParserOptions(ptr),
                .select_item_options = Accessors.selectItemParserOptions(ptr),
                .fixed_binary_hooks = Accessors.fixedBinaryRowExpressionParserOptions(ptr),
                .bare_boolean_hooks = Accessors.bareBooleanWhereExpressionParserOptions(ptr),
                .expression_alternatives_hooks = Accessors.expressionWhereConditionAlternativesParserHooks(ptr),
                .expression_condition_hooks = Accessors.expressionWhereConditionsParserHooks(ptr),
                .output_field_options = Accessors.aggregateOutputFieldParserOptions(ptr),
                .output_field_condition_options = Accessors.aggregateOutputFieldExpressionConditionParserOptions(ptr),
                .case_expression_hooks = Accessors.caseExpressionParserHooks(ptr),
                .output_order_expression_options = Accessors.outputOrderExpressionParserOptions(ptr),
                .realtime_ns = value_mod.currentRealtimeNs(),
            };
        }

        pub fn joinParserOptions(ptr: *ParserType) lower_expr.JoinParserOptions {
            return .{
                .params = ptr.params,
                .available_ctes = ptr.available_ctes,
                .generated_read_ast = ptr.generated_read_ast,
                .context_hooks = Accessors.joinParserContextHooks(ptr),
                .expression_where_options = Accessors.joinedMutationExpressionWhereOptions(ptr),
                .output_order_expression_options = Accessors.outputOrderExpressionParserOptions(ptr),
                .string_to_array_predicate_is_containment = lower_expr.stringToArrayPredicateIsContainment(ptr.tokens, ptr.pos),
                .realtime_ns = value_mod.currentRealtimeNs(),
            };
        }

        pub fn lateralParserOptions(ptr: *ParserType) lower_expr.LateralParserOptions {
            return .{
                .params = ptr.params,
                .available_ctes = ptr.available_ctes,
                .generated_read_ast = ptr.generated_read_ast,
                .context_hooks = Accessors.joinParserContextHooks(ptr),
                .subquery_hooks = Accessors.lateralSubqueryParserHooks(ptr),
                .expression_where_options = Accessors.joinedMutationExpressionWhereOptions(ptr),
                .output_order_expression_options = Accessors.outputOrderExpressionParserOptions(ptr),
                .string_to_array_predicate_is_containment = lower_expr.stringToArrayPredicateIsContainment(ptr.tokens, ptr.pos),
                .realtime_ns = value_mod.currentRealtimeNs(),
            };
        }

        pub fn parseSelect(ptr: *ParserType) !plan.LoweredSelect {
            return try lower_expr.parseSelectAlloc(ptr.alloc, ptr.tokens, &ptr.pos, Accessors.selectParserOptions(ptr));
        }

        pub fn parseWindowSelect(ptr: *ParserType) !plan.LoweredWindowPlan {
            return try lower_expr.parseWindowSelectAlloc(ptr.alloc, ptr.tokens, &ptr.pos, Accessors.windowParserOptions(ptr));
        }

        pub fn parseAggregate(ptr: *ParserType) !plan.LoweredAggregate {
            return try lower_expr.parseAggregateAlloc(ptr.alloc, ptr.tokens, &ptr.pos, Accessors.aggregateParserOptions(ptr));
        }

        pub fn parseJoin(ptr: *ParserType) !plan.LoweredJoin {
            return try lower_expr.parseJoinAlloc(ptr.alloc, ptr.tokens, &ptr.pos, Accessors.joinParserOptions(ptr));
        }

        pub fn parseLateral(ptr: *ParserType) !plan.LoweredLateralPlan {
            return try lower_expr.parseLateralAlloc(ptr.alloc, ptr.tokens, &ptr.pos, Accessors.lateralParserOptions(ptr));
        }

        pub fn conflictTargetParserOptions(ptr: *ParserType) lower_dml.ConflictTargetParserOptions {
            return .{
                .type_context = Accessors.rowExpressionTypeContext(ptr),
                .defer_row_expression_field_validation = ptr.defer_row_expression_field_validation,
                .case_expression_hooks = Accessors.caseExpressionParserHooks(ptr),
                .generated_target_tokens = ptr.generated_conflict_target_tokens,
                .generated_target_kind = ptr.generated_conflict_target_kind,
                .generated_target_items = ptr.generated_conflict_target_items,
                .generated_where_tokens = ptr.generated_conflict_target_where_tokens,
                .generated_where_expression = ptr.generated_conflict_target_where_expression,
            };
        }

        pub fn conflictJsonSetSqlValueParserOptions(
            ptr: *ParserType,
            insert_columns: []const []const u8,
        ) lower_dml.JsonSetSqlValueParserOptions {
            return .{
                .params = ptr.params,
                .schema = ptr.schema,
                .conflict_existing_qualifiers = ptr.conflict_existing_qualifiers,
                .insert_columns = insert_columns,
                .type_context = Accessors.rowExpressionTypeContext(ptr),
                .defer_row_expression_field_validation = ptr.defer_row_expression_field_validation,
                .row_expression_hooks = Accessors.rowExpressionParserHooks(ptr),
                .arithmetic_hooks = Accessors.arithmeticExpressionParserHooks(ptr),
                .variadic_hooks = Accessors.variadicRowExpressionParserHooks(ptr),
                .conflict_dispatch_options = Accessors.conflictExpressionDispatchOptions(ptr),
            };
        }

        pub fn conflictUpdateAssignmentValueParserOptions(
            ptr: *ParserType,
            insert_columns: []const []const u8,
            insert_values: []const []const u8,
        ) lower_dml.ConflictUpdateAssignmentValueParserOptions {
            return .{
                .schema = ptr.schema,
                .params = ptr.params,
                .insert_columns = insert_columns,
                .insert_values = insert_values,
                .realtime_ns = value_mod.currentRealtimeNs(),
                .json_set = Accessors.conflictJsonSetSqlValueParserOptions(ptr, insert_columns),
                .expression_options = .{
                    .type_context = Accessors.rowExpressionTypeContext(ptr),
                    .assignment_options = Accessors.conflictAssignmentExpressionParserOptions(ptr),
                },
            };
        }

        pub fn conflictUpdateSetAssignmentParserOptions(
            ptr: *ParserType,
            insert_columns: []const []const u8,
            insert_values: []const []const u8,
        ) lower_dml.ConflictUpdateSetAssignmentParserOptions {
            return .{
                .value = Accessors.conflictUpdateAssignmentValueParserOptions(ptr, insert_columns, insert_values),
                .primary_key_assignment = .{ .allow_and_mark = .{
                    .allow = ptr.allow_primary_key_assignment,
                    .saw = &ptr.saw_primary_key_assignment,
                } },
            };
        }

        pub fn conflictAssignmentExpressionParserOptions(ptr: *ParserType) lower_dml.ConflictAssignmentExpressionParserOptions {
            return .{
                .params = ptr.params,
                .schema = ptr.schema,
                .conflict_existing_qualifiers = ptr.conflict_existing_qualifiers,
                .type_context = Accessors.rowExpressionTypeContext(ptr),
                .defer_row_expression_field_validation = ptr.defer_row_expression_field_validation,
                .dispatch_options = Accessors.conflictExpressionDispatchOptions(ptr),
                .generated_action_where_tokens = ptr.generated_conflict_action_where_tokens,
                .generated_expression_ast = ptr.generated_conflict_action_where_expression,
            };
        }

        pub fn conflictExpressionDispatchOptions(ptr: *ParserType) lower_dml.ConflictExpressionDispatchOptions {
            return .{
                .array_length = Accessors.conflictArrayLengthExpressionParserOptions(ptr),
                .case_fold = Accessors.conflictCaseFoldExpressionParserOptions(ptr),
            };
        }

        pub fn conflictArrayLengthExpressionParserOptions(ptr: *ParserType) lower_dml.ConflictArrayLengthExpressionParserOptions {
            return .{
                .schema = ptr.schema,
                .conflict_existing_qualifiers = ptr.conflict_existing_qualifiers,
            };
        }

        pub fn conflictCaseFoldExpressionParserOptions(ptr: *ParserType) lower_dml.ConflictCaseFoldExpressionParserOptions {
            return .{
                .period_bound = .{
                    .schema = ptr.schema,
                    .field_expression_qualifiers = ptr.field_expression_qualifiers,
                    .returning_expression_qualifiers = ptr.returning_expression_qualifiers,
                    .defer_row_expression_field_validation = ptr.defer_row_expression_field_validation,
                    .field_source = expr_type.rowExpressionFieldSourceOrDefault(ptr.row_expression_field_source_override),
                },
            };
        }

        pub fn joinedMutationJsonSetSqlValueParserOptions(ptr: *ParserType) lower_dml.JoinedMutationJsonSetSqlValueParserOptions {
            return .{
                .params = ptr.params,
                .pending_joined_source_alias = ptr.pending_joined_source_alias,
                .row_expression_options = .{
                    .context_hooks = Accessors.joinedExpressionParserContextHooks(ptr),
                    .row_expression_hooks = Accessors.rowExpressionParserHooks(ptr),
                    .arithmetic_hooks = Accessors.arithmeticExpressionParserHooks(ptr),
                    .variadic_hooks = Accessors.variadicRowExpressionParserHooks(ptr),
                },
            };
        }

        pub fn joinedMutationAssignmentValueParserOptions(ptr: *ParserType) lower_dml.JoinedMutationAssignmentValueParserOptions {
            return .{
                .json_set = Accessors.joinedMutationJsonSetSqlValueParserOptions(ptr),
                .assignment_expression = Accessors.joinedMutationAssignmentExpressionParserOptions(ptr),
            };
        }

        pub fn joinedMutationAssignmentExpressionParserOptions(ptr: *ParserType) lower_dml.JoinedMutationAssignmentExpressionParserOptions {
            return .{
                .pending_joined_source_alias = ptr.pending_joined_source_alias,
                .type_context = Accessors.rowExpressionTypeContext(ptr),
                .row_expression_options = .{
                    .context_hooks = Accessors.joinedExpressionParserContextHooks(ptr),
                    .row_expression_hooks = Accessors.rowExpressionParserHooks(ptr),
                    .arithmetic_hooks = Accessors.arithmeticExpressionParserHooks(ptr),
                    .variadic_hooks = Accessors.variadicRowExpressionParserHooks(ptr),
                },
                .boolean_expression_options = .{
                    .context_hooks = Accessors.joinedExpressionParserContextHooks(ptr),
                    .boolean_hooks = Accessors.booleanRowExpressionParserHooks(ptr),
                },
            };
        }

        pub fn mergeAssignmentParserOptions(ptr: *ParserType) lower_dml.MergeAssignmentParserOptions {
            return .{
                .assignment_expression = Accessors.mergeAssignmentExpressionParserOptions(ptr),
            };
        }

        pub fn mergeAssignmentExpressionParserOptions(ptr: *ParserType) lower_dml.MergeAssignmentExpressionParserOptions {
            return .{
                .default_context = ptr.default_context,
                .type_context = Accessors.rowExpressionTypeContext(ptr),
                .row_expression_options = .{
                    .context_hooks = Accessors.joinedExpressionParserContextHooks(ptr),
                    .row_expression_hooks = Accessors.rowExpressionParserHooks(ptr),
                    .arithmetic_hooks = Accessors.arithmeticExpressionParserHooks(ptr),
                    .variadic_hooks = Accessors.variadicRowExpressionParserHooks(ptr),
                },
            };
        }

        pub fn mergeArmConditionParserOptions(ptr: *ParserType) lower_dml.MergeArmConditionParserOptions {
            return .{
                .expression_predicates_options = Accessors.mergeArmExpressionPredicatesParserOptions(ptr),
            };
        }

        pub fn mergeArmExpressionPredicatesParserOptions(ptr: *ParserType) lower_dml.MergeArmExpressionPredicatesParserOptions {
            return .{
                .not_where_options = .{
                    .params = ptr.params,
                    .context_hooks = Accessors.joinedExpressionParserContextHooks(ptr),
                    .alternatives_hooks = Accessors.expressionWhereConditionAlternativesParserHooks(ptr),
                },
                .or_where_options = .{
                    .params = ptr.params,
                    .context_hooks = Accessors.joinedExpressionParserContextHooks(ptr),
                    .alternatives_hooks = Accessors.expressionWhereConditionAlternativesParserHooks(ptr),
                },
                .where_condition_options = .{
                    .params = ptr.params,
                    .context_hooks = Accessors.joinedExpressionParserContextHooks(ptr),
                    .condition_hooks = Accessors.expressionWhereConditionsParserHooks(ptr),
                },
            };
        }

        pub fn mergeMutationParserOptions(ptr: *ParserType) lower_dml.MergeMutationParserOptions {
            return .{
                .params = ptr.params,
                .schema = ptr.schema,
                .function_bindings = ptr.function_bindings,
                .joined_source_schema = ptr.joined_source_schema,
                .defer_row_expression_field_validation = ptr.defer_row_expression_field_validation,
                .context_hooks = Accessors.mergeMutationParserContextHooks(ptr),
                .condition_options = Accessors.mergeArmConditionParserOptions(ptr),
                .assignment_options = Accessors.mergeAssignmentParserOptions(ptr),
                .generated_dml_ast = ptr.generated_dml_ast,
                .generated_merge_arms = ptr.generated_merge_arms,
                .mutation_context_hooks = Accessors.mutationSourceParserContextHooks(ptr),
                .mutation_assignment_value_hooks = Accessors.conflictUpdateAssignmentValueParserOptions(ptr, &.{}, &.{}),
                .fixed_binary_hooks = Accessors.fixedBinaryRowExpressionParserOptions(ptr),
                .bare_boolean_hooks = Accessors.bareBooleanWhereExpressionParserOptions(ptr),
                .expression_alternatives_hooks = Accessors.expressionWhereConditionAlternativesParserHooks(ptr),
                .expression_condition_hooks = Accessors.expressionWhereConditionsParserHooks(ptr),
                .order_expression_hooks = Accessors.orderExpressionParserOptions(ptr),
                .returning_hooks = Accessors.returningProjectionParserOptions(ptr),
                .realtime_ns = value_mod.currentRealtimeNs(),
            };
        }

        pub fn updateJoinedMutationSourceParserOptions(
            ptr: *ParserType,
            ctes: []const db_mod.types.RelationalRowsCte,
            base_table_name: ?*?[]const u8,
        ) !lower_dml.UpdateJoinedMutationSourceParserOptions {
            return .{
                .params = ptr.params,
                .schema = ptr.schema,
                .joined_source_schema = ptr.joined_source_schema,
                .row_claim = try lower_dml.mutationRowClaimAlloc(ptr.alloc, ptr.mutation_claim, false),
                .ctes = ctes,
                .base_table_name = base_table_name,
                .string_to_array_predicate_is_containment = lower_expr.stringToArrayPredicateIsContainment(ptr.tokens, ptr.pos),
                .context_hooks = Accessors.joinedMutationSourceParserContextHooks(ptr),
                .assignment_options = Accessors.joinedMutationAssignmentValueParserOptions(ptr),
                .generated_assignment_items = ptr.generated_assignment_items,
                .generated_dml_ast = ptr.generated_dml_ast,
                .source_query_context_hooks = Accessors.semiJoinSourceQueryParserContextHooks(ptr),
                .fixed_binary_hooks = Accessors.fixedBinaryRowExpressionParserOptions(ptr),
                .bare_boolean_hooks = Accessors.bareBooleanWhereExpressionParserOptions(ptr),
                .expression_alternatives_hooks = Accessors.expressionWhereConditionAlternativesParserHooks(ptr),
                .expression_condition_hooks = Accessors.expressionWhereConditionsParserHooks(ptr),
                .expression_where_options = Accessors.joinedMutationExpressionWhereOptions(ptr),
                .returning_hooks = Accessors.returningProjectionParserOptions(ptr),
                .joined_returning_hooks = Accessors.joinedMutationReturningProjectionParserOptions(ptr),
                .realtime_ns = value_mod.currentRealtimeNs(),
                .field_expression_qualifiers = ptr.field_expression_qualifiers,
                .returning_expression_qualifiers = ptr.returning_expression_qualifiers,
                .defer_row_expression_field_validation = ptr.defer_row_expression_field_validation,
            };
        }

        pub fn deleteJoinedMutationSourceParserOptions(
            ptr: *ParserType,
            ctes: []const db_mod.types.RelationalRowsCte,
            base_table_name: ?*?[]const u8,
        ) !lower_dml.DeleteJoinedMutationSourceParserOptions {
            return .{
                .params = ptr.params,
                .schema = ptr.schema,
                .joined_source_schema = ptr.joined_source_schema,
                .row_claim = try lower_dml.mutationRowClaimAlloc(ptr.alloc, ptr.mutation_claim, false),
                .ctes = ctes,
                .base_table_name = base_table_name,
                .string_to_array_predicate_is_containment = lower_expr.stringToArrayPredicateIsContainment(ptr.tokens, ptr.pos),
                .context_hooks = Accessors.joinedMutationSourceParserContextHooks(ptr),
                .generated_dml_ast = ptr.generated_dml_ast,
                .source_query_context_hooks = Accessors.semiJoinSourceQueryParserContextHooks(ptr),
                .fixed_binary_hooks = Accessors.fixedBinaryRowExpressionParserOptions(ptr),
                .bare_boolean_hooks = Accessors.bareBooleanWhereExpressionParserOptions(ptr),
                .expression_alternatives_hooks = Accessors.expressionWhereConditionAlternativesParserHooks(ptr),
                .expression_condition_hooks = Accessors.expressionWhereConditionsParserHooks(ptr),
                .expression_where_options = Accessors.joinedMutationExpressionWhereOptions(ptr),
                .returning_hooks = Accessors.returningProjectionParserOptions(ptr),
                .joined_returning_hooks = Accessors.joinedMutationReturningProjectionParserOptions(ptr),
                .realtime_ns = value_mod.currentRealtimeNs(),
                .field_expression_qualifiers = ptr.field_expression_qualifiers,
                .returning_expression_qualifiers = ptr.returning_expression_qualifiers,
                .defer_row_expression_field_validation = ptr.defer_row_expression_field_validation,
            };
        }

        pub fn insertParserOptions(ptr: *ParserType) lower_dml.InsertParserOptions {
            return .{
                .params = ptr.params,
                .schema = ptr.schema,
                .unique_resolver = ptr.unique_resolver,
                .default_context = ptr.default_context,
                .type_context = Accessors.rowExpressionTypeContext(ptr),
                .defer_row_expression_field_validation = ptr.defer_row_expression_field_validation,
                .target_options = Accessors.conflictTargetParserOptions(ptr),
                .assignment_value_hooks = Accessors.conflictUpdateAssignmentValueParserOptions(ptr, &.{}, &.{}),
                .generated_conflict_assignment_items = ptr.generated_assignment_items,
                .generated_conflict_assignments_tokens = ptr.generated_conflict_assignments_tokens,
                .generated_conflict_action_tokens = ptr.generated_conflict_action_tokens,
                .generated_conflict_action_kind = ptr.generated_conflict_action_kind,
                .condition_options = Accessors.conflictAssignmentExpressionParserOptions(ptr),
                .dispatch_options = Accessors.conflictExpressionDispatchOptions(ptr),
                .returning_hooks = Accessors.returningProjectionParserOptions(ptr),
                .realtime_ns = value_mod.currentRealtimeNs(),
            };
        }

        pub fn updateParserOptions(ptr: *ParserType) lower_dml.UpdateParserOptions {
            return .{
                .params = ptr.params,
                .schema = ptr.schema,
                .unique_resolver = ptr.unique_resolver,
                .default_context = ptr.default_context,
                .conflict_existing_qualifiers = ptr.conflict_existing_qualifiers,
                .assignment_value_hooks = Accessors.conflictUpdateAssignmentValueParserOptions(ptr, &.{}, &.{}),
                .returning_hooks = Accessors.returningProjectionParserOptions(ptr),
                .realtime_ns = value_mod.currentRealtimeNs(),
            };
        }

        pub fn deleteParserOptions(ptr: *ParserType) lower_dml.DeleteParserOptions {
            return .{
                .params = ptr.params,
                .schema = ptr.schema,
                .unique_resolver = ptr.unique_resolver,
                .default_context = ptr.default_context,
                .returning_hooks = Accessors.returningProjectionParserOptions(ptr),
                .realtime_ns = value_mod.currentRealtimeNs(),
            };
        }

        pub fn mutationSourceParserOptions(ptr: *ParserType) !lower_dml.MutationSourceParserOptions {
            return .{
                .params = ptr.params,
                .schema = ptr.schema,
                .row_claim = try lower_dml.mutationRowClaimAlloc(ptr.alloc, ptr.mutation_claim, false),
                .realtime_ns = value_mod.currentRealtimeNs(),
                .defer_row_expression_field_validation = ptr.defer_row_expression_field_validation,
                .context_hooks = Accessors.mutationSourceParserContextHooks(ptr),
                .assignment_value_hooks = Accessors.conflictUpdateAssignmentValueParserOptions(ptr, &.{}, &.{}),
                .generated_assignment_items = ptr.generated_assignment_items,
                .generated_dml_ast = ptr.generated_dml_ast,
                .fixed_binary_hooks = Accessors.fixedBinaryRowExpressionParserOptions(ptr),
                .bare_boolean_hooks = Accessors.bareBooleanWhereExpressionParserOptions(ptr),
                .expression_alternatives_hooks = Accessors.expressionWhereConditionAlternativesParserHooks(ptr),
                .expression_condition_hooks = Accessors.expressionWhereConditionsParserHooks(ptr),
                .function_bindings = ptr.function_bindings,
                .order_expression_hooks = Accessors.orderExpressionParserOptions(ptr),
                .returning_hooks = Accessors.returningProjectionParserOptions(ptr),
            };
        }

        pub fn parseInsert(ptr: *ParserType) !plan.LoweredInsert {
            return try lower_dml.parseInsertAlloc(ptr.alloc, ptr.tokens, &ptr.pos, Accessors.insertParserOptions(ptr));
        }

        pub fn parseUpdate(ptr: *ParserType) !plan.LoweredMutation {
            return try lower_dml.parseUpdateAlloc(ptr.alloc, ptr.tokens, &ptr.pos, Accessors.updateParserOptions(ptr));
        }

        pub fn parseDelete(ptr: *ParserType) !plan.LoweredMutation {
            return try lower_dml.parseDeleteAlloc(ptr.alloc, ptr.tokens, &ptr.pos, Accessors.deleteParserOptions(ptr));
        }

        pub fn parseUpdateMutationSource(ptr: *ParserType) !plan.LoweredMutationSource {
            return try lower_dml.parseUpdateMutationSourceAlloc(ptr.alloc, ptr.tokens, &ptr.pos, try Accessors.mutationSourceParserOptions(ptr));
        }

        pub fn parseDeleteMutationSource(ptr: *ParserType) !plan.LoweredMutationSource {
            return try lower_dml.parseDeleteMutationSourceAlloc(ptr.alloc, ptr.tokens, &ptr.pos, try Accessors.mutationSourceParserOptions(ptr));
        }

        pub fn parseTruncateMutationSource(ptr: *ParserType) !plan.LoweredMutationSource {
            return try lower_dml.parseTruncateMutationSourceAlloc(
                ptr.alloc,
                ptr.tokens,
                &ptr.pos,
                ptr.schema,
                try lower_dml.mutationRowClaimAlloc(ptr.alloc, ptr.mutation_claim, false),
            );
        }

        pub fn parseMergeMutationBody(
            ptr: *ParserType,
            ctes: []const db_mod.types.RelationalRowsCte,
            base_table_name: ?*?[]const u8,
        ) !plan.LoweredMergeMutationPlan {
            return try lower_dml.parseMergeMutationBodyAlloc(ptr.alloc, ptr.tokens, &ptr.pos, Accessors.mergeMutationParserOptions(ptr), .{
                .ctes = ctes,
                .base_table_name = base_table_name,
            });
        }

        pub fn parseUpdateJoinedMutationSourceWithCtes(
            ptr: *ParserType,
            ctes: []const db_mod.types.RelationalRowsCte,
            base_table_name: ?*?[]const u8,
        ) !plan.LoweredJoinedMutationSource {
            return try lower_dml.parseUpdateJoinedMutationSourceWithCtesAlloc(
                ptr.alloc,
                ptr.tokens,
                &ptr.pos,
                try Accessors.updateJoinedMutationSourceParserOptions(ptr, ctes, base_table_name),
            );
        }

        pub fn parseDeleteJoinedMutationSourceWithCtes(
            ptr: *ParserType,
            ctes: []const db_mod.types.RelationalRowsCte,
            base_table_name: ?*?[]const u8,
        ) !plan.LoweredJoinedMutationSource {
            return try lower_dml.parseDeleteJoinedMutationSourceWithCtesAlloc(
                ptr.alloc,
                ptr.tokens,
                &ptr.pos,
                try Accessors.deleteJoinedMutationSourceParserOptions(ptr, ctes, base_table_name),
            );
        }

        pub fn parseRecursiveInsertSource(ptr: *ParserType) !plan.LoweredRecursiveInsertSource {
            return try lower_dml.parseRecursiveInsertSourceAlloc(
                ptr.alloc,
                ptr.tokens,
                &ptr.pos,
                Accessors.recursiveCteParserHooks(ptr),
                Accessors.insertSourceParserHooks(ptr),
            );
        }

        pub fn parseRecursiveUpdateJoinedMutationSource(ptr: *ParserType) !plan.LoweredRecursiveJoinedMutationSource {
            return try lower_dml.parseRecursiveJoinedMutationSourceAlloc(
                ptr.alloc,
                ptr.tokens,
                &ptr.pos,
                .update,
                Accessors.recursiveCteParserHooks(ptr),
                Accessors.updateJoinedMutationSourceParserHooks(ptr),
            );
        }

        pub fn parseRecursiveDeleteJoinedMutationSource(ptr: *ParserType) !plan.LoweredRecursiveJoinedMutationSource {
            return try lower_dml.parseRecursiveJoinedMutationSourceAlloc(
                ptr.alloc,
                ptr.tokens,
                &ptr.pos,
                .delete,
                Accessors.recursiveCteParserHooks(ptr),
                Accessors.deleteJoinedMutationSourceParserHooks(ptr),
            );
        }

        pub fn parseRecursiveMergeMutation(ptr: *ParserType) !plan.LoweredRecursiveMergeMutation {
            return try lower_dml.parseRecursiveMergeMutationAlloc(
                ptr.alloc,
                ptr.tokens,
                &ptr.pos,
                Accessors.recursiveCteParserHooks(ptr),
                Accessors.mergeMutationParserOptions(ptr),
            );
        }

        pub fn parseInsertSourceWithCtes(
            ptr: *ParserType,
            ctes: []const db_mod.types.RelationalRowsCte,
            base_table_name: ?*?[]const u8,
        ) !plan.LoweredInsertSource {
            return try lower_dml.parseInsertSourceWithCtesAlloc(ptr.alloc, ptr.tokens, &ptr.pos, Accessors.insertSourceParserHooks(ptr), .{
                .ctes = ctes,
                .base_table_name = base_table_name,
            });
        }

        pub fn parseInsertSourceSelectHook(
            ptr: *anyopaque,
            tokens: []const Token,
            schema: runtime_schema.TableSchema,
        ) anyerror!plan.LoweredSelect {
            const self: *ParserType = @ptrCast(@alignCast(ptr));
            var sub = ParserType{
                .alloc = self.alloc,
                .tokens = tokens,
                .schema = schema,
                .params = self.params,
                .function_bindings = self.function_bindings,
                .unique_resolver = self.unique_resolver,
            };
            return try Accessors.parseSelect(&sub);
        }

        pub fn parseUpdateJoinedMutationSourceWithCtesHook(
            ptr: *anyopaque,
            ctes: []const db_mod.types.RelationalRowsCte,
            base_table_name: ?*?[]const u8,
        ) anyerror!plan.LoweredJoinedMutationSource {
            const self: *ParserType = @ptrCast(@alignCast(ptr));
            return try Accessors.parseUpdateJoinedMutationSourceWithCtes(self, ctes, base_table_name);
        }

        pub fn parseDeleteJoinedMutationSourceWithCtesHook(
            ptr: *anyopaque,
            ctes: []const db_mod.types.RelationalRowsCte,
            base_table_name: ?*?[]const u8,
        ) anyerror!plan.LoweredJoinedMutationSource {
            const self: *ParserType = @ptrCast(@alignCast(ptr));
            return try Accessors.parseDeleteJoinedMutationSourceWithCtes(self, ctes, base_table_name);
        }

        pub fn parseLateralSubqueryHook(
            ptr: *anyopaque,
            tokens: []const Token,
            schema: runtime_schema.TableSchema,
            joined_source_schema: runtime_schema.TableSchema,
            left_alias: []const u8,
            available_ctes: []const db_mod.types.RelationalRowsCte,
            generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
        ) anyerror!plan.LateralSubquery {
            const self: *ParserType = @ptrCast(@alignCast(ptr));
            var sub = ParserType{
                .alloc = self.alloc,
                .tokens = tokens,
                .schema = schema,
                .joined_source_schema = joined_source_schema,
                .params = self.params,
                .function_bindings = self.function_bindings,
                .unique_resolver = self.unique_resolver,
                .available_ctes = available_ctes,
            };
            return try lower_expr.parseLateralSubqueryAlloc(sub.alloc, sub.tokens, &sub.pos, .{
                .params = sub.params,
                .available_ctes = sub.available_ctes,
                .function_bindings = sub.function_bindings,
                .field_source = expr_type.rowExpressionFieldSourceOrDefault(sub.row_expression_field_source_override),
                .left_alias = left_alias,
                .context_hooks = Accessors.joinParserContextHooks(&sub),
                .select_item_options = Accessors.selectItemParserOptions(&sub),
                .order_expression_hooks = Accessors.orderExpressionParserOptions(&sub),
                .expression_where_options = Accessors.joinedMutationExpressionWhereOptions(&sub),
                .generated_read_ast = generated_read_ast,
                .realtime_ns = value_mod.currentRealtimeNs(),
            });
        }

        pub fn parseRowExpressionOperandHook(ptr: *anyopaque) anyerror!db_mod.types.RelationalRowsExpression {
            const self: *ParserType = @ptrCast(@alignCast(ptr));
            return try lower_expr.parseRowExpressionOperandFromContextAlloc(
                self.alloc,
                self.tokens,
                &self.pos,
                self.params,
                self.joined_source_schema,
                self.function_bindings,
                expr_type.rowExpressionFieldSourceOrDefault(self.row_expression_field_source_override),
                Accessors.selectParserContextHooks(self),
                Accessors.joinedExpressionParserContextHooks(self),
                Accessors.rowExpressionOperandParserOptions(self),
            );
        }

        pub fn parseFixedUnaryRowExpressionOperandHook(ptr: *anyopaque) anyerror!db_mod.types.RelationalRowsExpression {
            const self: *ParserType = @ptrCast(@alignCast(ptr));
            return try lower_expr.parseRowExpressionFromContextAlloc(
                self.alloc,
                self.tokens,
                &self.pos,
                Accessors.selectParserContextHooks(self),
                Accessors.rowExpressionParserHooks(self),
                Accessors.arithmeticExpressionParserHooks(self),
                Accessors.variadicRowExpressionParserHooks(self),
            );
        }

        fn generatedCteBodyReadAstForTokensAlloc(
            ptr: *ParserType,
            tokens: []const Token,
        ) !?generated_parser.GeneratedSqlReadAst {
            const parent = ptr.generated_read_ast orelse return null;
            if (parent.kind != .cte) return error.UnsupportedSqlShape;
            if (tokens.len == 0) return error.UnsupportedSqlShape;
            for (parent.cte_items) |cte| {
                const body = cte.body_tokens orelse continue;
                if (body.end <= body.start or body.end > ptr.tokens.len) return error.UnsupportedSqlShape;
                if (body.end - body.start != tokens.len) continue;
                if (ptr.tokens[body.start].source_start != tokens[0].source_start) continue;
                if (ptr.tokens[body.end - 1].source_end != tokens[tokens.len - 1].source_end) continue;
                var cloned = try generated_parser.cloneCteBodyReadAstAlloc(ptr.alloc, parent.statement_span, cte);
                errdefer cloned.deinit(ptr.alloc);
                try lower_expr.validateGeneratedReadAstPayloads(tokens, cloned);
                return cloned;
            }
            return error.UnsupportedSqlShape;
        }

        fn generatedCteBodyReadAstForCteAlloc(
            ptr: *ParserType,
            tokens: []const Token,
            cte: *const generated_parser.GeneratedSqlCteAst,
        ) !generated_parser.GeneratedSqlReadAst {
            const parent = ptr.generated_read_ast orelse return error.UnsupportedSqlShape;
            if (parent.kind != .cte) return error.UnsupportedSqlShape;
            if (tokens.len == 0) return error.UnsupportedSqlShape;
            const body = cte.body_tokens orelse return error.UnsupportedSqlShape;
            if (body.end <= body.start or body.end > ptr.tokens.len) return error.UnsupportedSqlShape;
            if (body.end - body.start != tokens.len) return error.UnsupportedSqlShape;
            if (ptr.tokens[body.start].source_start != tokens[0].source_start) return error.UnsupportedSqlShape;
            if (ptr.tokens[body.end - 1].source_end != tokens[tokens.len - 1].source_end) return error.UnsupportedSqlShape;
            var cloned = try generated_parser.cloneCteBodyReadAstAlloc(ptr.alloc, parent.statement_span, cte.*);
            errdefer cloned.deinit(ptr.alloc);
            try lower_expr.validateGeneratedReadAstPayloads(tokens, cloned);
            return cloned;
        }

        fn generatedChildReadAstForTokensAlloc(
            ptr: *ParserType,
            tokens: []const Token,
        ) !?generated_parser.GeneratedSqlReadAst {
            if (ptr.generated_read_ast) |parent| {
                if (parent.kind == .cte) return try Accessors.generatedCteBodyReadAstForTokensAlloc(ptr, tokens);
                if (tokens.len != ptr.tokens.len) return error.UnsupportedSqlShape;
            }
            return null;
        }

        pub fn parseCteSelectHook(
            ptr: *anyopaque,
            tokens: []const Token,
            available_ctes: []const db_mod.types.RelationalRowsCte,
            generated_cte: ?*const generated_parser.GeneratedSqlCteAst,
        ) anyerror!plan.LoweredSelect {
            const self: *ParserType = @ptrCast(@alignCast(ptr));
            var generated_body_ast = if (generated_cte) |cte|
                try Accessors.generatedCteBodyReadAstForCteAlloc(self, tokens, cte)
            else
                try Accessors.generatedChildReadAstForTokensAlloc(self, tokens);
            defer if (generated_body_ast) |*ast| ast.deinit(self.alloc);
            var sub = ParserType{
                .alloc = self.alloc,
                .tokens = tokens,
                .schema = self.schema,
                .params = self.params,
                .function_bindings = self.function_bindings,
                .unique_resolver = self.unique_resolver,
                .available_ctes = available_ctes,
                .generated_read_ast = if (generated_body_ast) |*ast| ast else null,
            };
            return try Accessors.parseSelect(&sub);
        }

        pub fn parseJoinCteSelectHook(
            ptr: *anyopaque,
            tokens: []const Token,
            available_ctes: []const db_mod.types.RelationalRowsCte,
            generated_cte: ?*const generated_parser.GeneratedSqlCteAst,
        ) anyerror!plan.LoweredSelect {
            const self: *ParserType = @ptrCast(@alignCast(ptr));
            var generated_body_ast = if (generated_cte) |cte|
                try Accessors.generatedCteBodyReadAstForCteAlloc(self, tokens, cte)
            else
                try Accessors.generatedChildReadAstForTokensAlloc(self, tokens);
            defer if (generated_body_ast) |*ast| ast.deinit(self.alloc);
            var sub = ParserType{
                .alloc = self.alloc,
                .tokens = tokens,
                .schema = self.joined_source_schema orelse self.schema,
                .params = self.params,
                .function_bindings = self.function_bindings,
                .unique_resolver = self.unique_resolver,
                .available_ctes = available_ctes,
                .generated_read_ast = if (generated_body_ast) |*ast| ast else null,
            };
            return try Accessors.parseSelect(&sub);
        }

        pub fn parseWindowPlanHook(ptr: *anyopaque) anyerror!plan.LoweredWindowPlan {
            const self: *ParserType = @ptrCast(@alignCast(ptr));
            return try Accessors.parseWindowSelect(self);
        }

        pub fn parseAggregatePlanHook(ptr: *anyopaque) anyerror!plan.LoweredAggregate {
            const self: *ParserType = @ptrCast(@alignCast(ptr));
            return try Accessors.parseAggregate(self);
        }

        pub fn parseJoinPlanHook(ptr: *anyopaque) anyerror!plan.LoweredJoin {
            const self: *ParserType = @ptrCast(@alignCast(ptr));
            return try Accessors.parseJoin(self);
        }

        pub fn parseLateralPlanHook(ptr: *anyopaque) anyerror!plan.LoweredLateralPlan {
            const self: *ParserType = @ptrCast(@alignCast(ptr));
            return try Accessors.parseLateral(self);
        }

        pub fn parseQueryPlanSelectHook(ptr: *anyopaque) anyerror!plan.LoweredSelect {
            const self: *ParserType = @ptrCast(@alignCast(ptr));
            return try Accessors.parseSelect(self);
        }

        pub fn parseRecursiveCteSelectWithSetBoundaryHook(
            ptr: *anyopaque,
            tokens: []const Token,
            pos: *usize,
            available_ctes: []const db_mod.types.RelationalRowsCte,
        ) anyerror!plan.LoweredSelect {
            const self: *ParserType = @ptrCast(@alignCast(ptr));
            var generated_body_ast: ?generated_parser.GeneratedSqlReadAst = null;
            if (tokens.len != self.tokens.len) {
                generated_body_ast = try Accessors.generatedChildReadAstForTokensAlloc(self, tokens);
            }
            defer if (generated_body_ast) |*ast| ast.deinit(self.alloc);
            var sub = ParserType{
                .alloc = self.alloc,
                .tokens = tokens,
                .pos = pos.*,
                .schema = self.schema,
                .params = self.params,
                .function_bindings = self.function_bindings,
                .unique_resolver = self.unique_resolver,
                .available_ctes = available_ctes,
                .allow_select_set_boundary = true,
                .generated_read_ast = if (generated_body_ast) |*ast| ast else self.generated_read_ast,
            };
            const lowered = try Accessors.parseSelect(&sub);
            pos.* = sub.pos;
            return lowered;
        }

        pub fn selectOutputColumnsHook(
            ptr: *anyopaque,
            lowered: plan.LoweredSelect,
        ) anyerror![]runtime_schema.RelationalColumn {
            const self: *ParserType = @ptrCast(@alignCast(ptr));
            return try expr_projection.loweredSelectOutputColumnsAlloc(self.alloc, Accessors.rowExpressionTypeContext(self), lowered);
        }

        pub fn parseRecursiveCteMemberHook(
            ptr: *anyopaque,
            tokens: []const Token,
            pos: *usize,
            available_ctes: []const db_mod.types.RelationalRowsCte,
            cte_name: []const u8,
        ) anyerror!plan.LoweredRecursiveCteMemberPlan {
            const self: *ParserType = @ptrCast(@alignCast(ptr));
            var generated_body_ast = try Accessors.generatedChildReadAstForTokensAlloc(self, tokens);
            defer if (generated_body_ast) |*ast| ast.deinit(self.alloc);
            var sub = ParserType{
                .alloc = self.alloc,
                .tokens = tokens,
                .pos = pos.*,
                .schema = self.schema,
                .params = self.params,
                .function_bindings = self.function_bindings,
                .unique_resolver = self.unique_resolver,
                .available_ctes = available_ctes,
                .generated_read_ast = if (generated_body_ast) |*ast| ast else null,
            };
            const member = try plan.parseRecursiveCteMemberPlanAlloc(
                self.alloc,
                tokens,
                &sub.pos,
                self.schema,
                available_ctes,
                cte_name,
                Accessors.recursiveCteMemberParserHooks(&sub),
            );
            pos.* = sub.pos;
            return member;
        }

        pub fn parseRecursiveCteMemberProjectionExpressionHook(
            ptr: *anyopaque,
            tokens: []const Token,
            pos: *usize,
            cte_schema: runtime_schema.TableSchema,
            target_qualifiers: []const []const u8,
            source_qualifiers: []const []const u8,
        ) anyerror!plan.RecursiveCteMemberProjectionExpression {
            const self: *ParserType = @ptrCast(@alignCast(ptr));
            var projection_parser = ParserType{
                .alloc = self.alloc,
                .tokens = tokens,
                .pos = pos.*,
                .schema = self.schema,
                .joined_source_schema = cte_schema,
                .params = self.params,
                .function_bindings = self.function_bindings,
                .joined_target_expression_qualifiers = target_qualifiers,
                .joined_source_expression_qualifiers = source_qualifiers,
            };
            const parsed = try lower_expr.parseRecursiveCteMemberProjectionExpressionAlloc(
                projection_parser.alloc,
                projection_parser.tokens,
                &projection_parser.pos,
                .{
                    .type_context = Accessors.rowExpressionTypeContext(&projection_parser),
                    .row_expression_hooks = Accessors.rowExpressionParserHooks(&projection_parser),
                    .arithmetic_hooks = Accessors.arithmeticExpressionParserHooks(&projection_parser),
                    .variadic_hooks = Accessors.variadicRowExpressionParserHooks(&projection_parser),
                },
            );
            pos.* = projection_parser.pos;
            return parsed;
        }

        pub fn parseRecursiveCteMemberJoinOnHook(
            ptr: *anyopaque,
            tokens: []const Token,
            pos: *usize,
            cte_schema: runtime_schema.TableSchema,
            left_is_cte: bool,
            right_is_cte: bool,
            join_type: db_mod.types.RelationalRowsJoinType,
            left_alias: []const u8,
            right_alias: []const u8,
        ) anyerror![]const db_mod.types.RelationalRowsJoinOn {
            const self: *ParserType = @ptrCast(@alignCast(ptr));
            var sub = ParserType{
                .alloc = self.alloc,
                .tokens = tokens,
                .pos = pos.*,
                .schema = if (left_is_cte) cte_schema else self.schema,
                .joined_source_schema = if (right_is_cte) cte_schema else self.schema,
                .params = self.params,
                .function_bindings = self.function_bindings,
                .unique_resolver = self.unique_resolver,
            };
            const on = try lower_expr.parseRecursiveCteMemberJoinOnAlloc(self.alloc, sub.tokens, &sub.pos, .{
                .params = sub.params,
                .schema = sub.schema,
                .joined_source_schema = sub.joined_source_schema,
                .join_type = join_type,
                .left_alias = left_alias,
                .right_alias = right_alias,
                .string_to_array_predicate_is_containment = lower_expr.stringToArrayPredicateIsContainment(sub.tokens, sub.pos),
                .expression_where_options = Accessors.joinedMutationExpressionWhereOptions(&sub),
                .realtime_ns = value_mod.currentRealtimeNs(),
            });
            pos.* = sub.pos;
            return on;
        }

        pub fn parseSetOperationSelectHook(ptr: *anyopaque) anyerror!plan.LoweredSelect {
            const self: *ParserType = @ptrCast(@alignCast(ptr));
            return try Accessors.parseSelect(self);
        }

        pub fn parseSetOperationResultTailHook(
            ptr: *anyopaque,
            lowered: plan.LoweredSelect,
        ) anyerror!plan.SetOperationResultTail {
            const self: *ParserType = @ptrCast(@alignCast(ptr));
            return try plan.parseSetOperationResultTailAlloc(self.alloc, self.tokens, &self.pos, self.params, self.generated_read_ast, lowered, Accessors.simpleSelectSetTailHooks(self));
        }

        pub fn parseSimpleSelectSetTailOrderByHook(
            ptr: *anyopaque,
            order_by: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsQueryOrder),
            select: plan.SelectList,
        ) anyerror!void {
            const self: *ParserType = @ptrCast(@alignCast(ptr));
            return try lower_expr.parseSelectOutputOrderByAlloc(
                self.alloc,
                self.tokens,
                &self.pos,
                order_by,
                select,
                Accessors.orderByParserOptions(self),
            );
        }

        pub fn getReadPlanParserContextHook(ptr: *anyopaque) plan.ReadPlanParserContext {
            const self: *ParserType = @ptrCast(@alignCast(ptr));
            return .{
                .schema = self.schema,
                .available_ctes = self.available_ctes,
                .allow_select_set_boundary = self.allow_select_set_boundary,
                .allow_select_set_result_tail_boundary = self.allow_select_set_result_tail_boundary,
            };
        }

        pub fn setReadPlanParserContextHook(ptr: *anyopaque, context: plan.ReadPlanParserContext) void {
            const self: *ParserType = @ptrCast(@alignCast(ptr));
            self.schema = context.schema;
            self.available_ctes = context.available_ctes;
            self.allow_select_set_boundary = context.allow_select_set_boundary;
            self.allow_select_set_result_tail_boundary = context.allow_select_set_result_tail_boundary;
        }

        pub fn getMutationSourceParserContextHook(ptr: *anyopaque) lower_dml.MutationSourceParserContext {
            const self: *ParserType = @ptrCast(@alignCast(ptr));
            return .{
                .field_expression_qualifiers = self.field_expression_qualifiers,
            };
        }

        pub fn setMutationSourceParserContextHook(ptr: *anyopaque, context: lower_dml.MutationSourceParserContext) void {
            const self: *ParserType = @ptrCast(@alignCast(ptr));
            self.field_expression_qualifiers = context.field_expression_qualifiers;
        }

        pub fn getMergeMutationParserContextHook(ptr: *anyopaque) lower_dml.MergeMutationParserContext {
            const self: *ParserType = @ptrCast(@alignCast(ptr));
            return .{
                .joined_source_schema = self.joined_source_schema,
                .available_ctes = self.available_ctes,
            };
        }

        pub fn setMergeMutationParserContextHook(ptr: *anyopaque, context: lower_dml.MergeMutationParserContext) void {
            const self: *ParserType = @ptrCast(@alignCast(ptr));
            self.joined_source_schema = context.joined_source_schema;
            self.available_ctes = context.available_ctes;
        }

        pub fn getJoinedMutationSourceParserContextHook(ptr: *anyopaque) lower_dml.JoinedMutationSourceParserContext {
            const self: *ParserType = @ptrCast(@alignCast(ptr));
            return .{
                .joined_source_schema = self.joined_source_schema,
                .pending_joined_source_alias = self.pending_joined_source_alias,
            };
        }

        pub fn setJoinedMutationSourceParserContextHook(ptr: *anyopaque, context: lower_dml.JoinedMutationSourceParserContext) void {
            const self: *ParserType = @ptrCast(@alignCast(ptr));
            self.joined_source_schema = context.joined_source_schema;
            self.pending_joined_source_alias = context.pending_joined_source_alias;
        }

        pub fn getJoinedExpressionParserContextHook(ptr: *anyopaque) lower_expr.JoinedExpressionParserContext {
            const self: *ParserType = @ptrCast(@alignCast(ptr));
            return .{
                .joined_target_expression_qualifiers = self.joined_target_expression_qualifiers,
                .joined_source_expression_qualifiers = self.joined_source_expression_qualifiers,
                .defer_row_expression_field_validation = self.defer_row_expression_field_validation,
            };
        }

        pub fn setJoinedExpressionParserContextHook(ptr: *anyopaque, context: lower_expr.JoinedExpressionParserContext) void {
            const self: *ParserType = @ptrCast(@alignCast(ptr));
            self.joined_target_expression_qualifiers = context.joined_target_expression_qualifiers;
            self.joined_source_expression_qualifiers = context.joined_source_expression_qualifiers;
            self.defer_row_expression_field_validation = context.defer_row_expression_field_validation;
        }

        pub fn getConflictClauseParserContextHook(ptr: *anyopaque) lower_dml.ConflictClauseParserContext {
            const self: *ParserType = @ptrCast(@alignCast(ptr));
            return .{
                .conflict_existing_qualifiers = self.conflict_existing_qualifiers,
                .joined_source_schema = self.joined_source_schema,
            };
        }

        pub fn setConflictClauseParserContextHook(ptr: *anyopaque, context: lower_dml.ConflictClauseParserContext) void {
            const self: *ParserType = @ptrCast(@alignCast(ptr));
            self.conflict_existing_qualifiers = context.conflict_existing_qualifiers;
            self.joined_source_schema = context.joined_source_schema;
        }

        pub fn getSemiJoinSourceQueryParserContextHook(ptr: *anyopaque) lower_dml.SemiJoinSourceQueryParserContext {
            const self: *ParserType = @ptrCast(@alignCast(ptr));
            return .{
                .schema = self.schema,
                .field_expression_qualifiers = self.field_expression_qualifiers,
            };
        }

        pub fn setSemiJoinSourceQueryParserContextHook(ptr: *anyopaque, context: lower_dml.SemiJoinSourceQueryParserContext) void {
            const self: *ParserType = @ptrCast(@alignCast(ptr));
            self.schema = context.schema;
            self.field_expression_qualifiers = context.field_expression_qualifiers;
        }

        pub fn getSelectParserContextHook(ptr: *anyopaque) lower_expr.SelectParserContext {
            const self: *ParserType = @ptrCast(@alignCast(ptr));
            return .{
                .schema = self.schema,
                .field_expression_qualifiers = self.field_expression_qualifiers,
                .returning_expression_qualifiers = self.returning_expression_qualifiers,
                .defer_row_expression_field_validation = self.defer_row_expression_field_validation,
            };
        }

        pub fn setSelectParserContextHook(ptr: *anyopaque, context: lower_expr.SelectParserContext) void {
            const self: *ParserType = @ptrCast(@alignCast(ptr));
            self.schema = context.schema;
            self.field_expression_qualifiers = context.field_expression_qualifiers;
            self.returning_expression_qualifiers = context.returning_expression_qualifiers;
            self.defer_row_expression_field_validation = context.defer_row_expression_field_validation;
        }

        pub fn selectParserRowExpressionTypeContextHook(ptr: *anyopaque) expr_type.RowExpressionTypeContext {
            const self: *ParserType = @ptrCast(@alignCast(ptr));
            return Accessors.rowExpressionTypeContext(self);
        }

        pub fn getNamedWindowSpecsHook(ptr: *anyopaque) []const plan.NamedWindowSpec {
            const self: *ParserType = @ptrCast(@alignCast(ptr));
            return self.named_window_specs;
        }

        pub fn setNamedWindowSpecsHook(ptr: *anyopaque, specs: []const plan.NamedWindowSpec) void {
            const self: *ParserType = @ptrCast(@alignCast(ptr));
            self.named_window_specs = specs;
        }

        pub fn getJoinParserContextHook(ptr: *anyopaque) lower_expr.JoinParserContext {
            const self: *ParserType = @ptrCast(@alignCast(ptr));
            return .{
                .schema = self.schema,
                .joined_source_schema = self.joined_source_schema,
            };
        }

        pub fn setJoinParserContextHook(ptr: *anyopaque, context: lower_expr.JoinParserContext) void {
            const self: *ParserType = @ptrCast(@alignCast(ptr));
            self.schema = context.schema;
            self.joined_source_schema = context.joined_source_schema;
        }
    };
}
