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
const classifier = @import("classifier.zig");
const db_mod = @import("../storage/db/mod.zig");
const generated_parser = @import("generated_parser.zig");
const lower_expr = @import("lower_expr.zig");
const metadata_api = @import("../metadata/api.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const metadata_transition_state = @import("../metadata/transition_state.zig");
const parser_mod = @import("parser.zig");
const parser_context = @import("parser_context.zig");
const plan = @import("plan.zig");
const raft_reconciler = @import("../raft/reconciler.zig");
const relational_rows = @import("../api/relational_rows.zig");
const table_catalog = @import("../api/table_catalog.zig");
const runtime_schema = @import("../storage/schema.zig");
const schema_api = @import("../schema/mod.zig");
const source_binding = @import("source_binding.zig");
const token_mod = @import("token.zig");
const tokenized = @import("tokenized.zig");
const value_mod = @import("value.zig");

pub const ReadPlanLoweringCallbacks = struct {
    lower_lateral_with_schemas: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
    ) anyerror!plan.LoweredLateralPlan,
    lower_window: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
    ) anyerror!plan.LoweredWindowPlan,
    lower_aggregate_plan: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
    ) anyerror!plan.LoweredAggregatePlan,
    lower_recursive_cte_plan: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        lower_expr.SqlFunctionBindings,
    ) anyerror!plan.LoweredRecursiveCtePlan,
    lower_join_with_schemas: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
    ) anyerror!plan.LoweredJoin,
    lower_query_plan: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        lower_expr.SqlFunctionBindings,
    ) anyerror!plan.LoweredQueryPlan,
    lower_set_operation_optional_source_schema: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        ?runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        lower_expr.SqlFunctionBindings,
    ) anyerror!plan.LoweredSetOperationPlan,
};

pub const ReadPlanLoweringContext = struct {
    alloc: std.mem.Allocator,
    sql: []const u8 = "",
    schema: runtime_schema.TableSchema,
    source_schema: ?runtime_schema.TableSchema,
    params: []const value_mod.SqlValue,
    function_bindings: lower_expr.SqlFunctionBindings,
    callbacks: ReadPlanLoweringCallbacks,
    statement_kind: ?classifier.SqlReadStatementKind = null,
    parsed_sql: ?*const tokenized.ParsedSql = null,

    pub fn lower(self: *@This()) !plan.LoweredReadPlan {
        var parsed_sql = try tokenized.ParsedSql.initAlloc(self.alloc, self.sql);
        defer parsed_sql.deinit(self.alloc);
        return try self.lowerParsed(&parsed_sql);
    }

    pub fn lowerParsed(self: *@This(), parsed_sql: *const tokenized.ParsedSql) !plan.LoweredReadPlan {
        const old_statement_kind = self.statement_kind;
        const old_parsed_sql = self.parsed_sql;
        self.statement_kind = parsed_sql.readStatementKind();
        self.parsed_sql = parsed_sql;
        defer self.statement_kind = old_statement_kind;
        defer self.parsed_sql = old_parsed_sql;
        return try plan.lowerReadPlanWithHooks(self.hooks());
    }

    fn hooks(self: *@This()) plan.ReadPlanLoweringHooks {
        return .{
            .ptr = self,
            .statement_kind = self.statement_kind,
            .lower_lateral = lowerLateralHook,
            .lower_window = lowerWindowHook,
            .lower_aggregate = lowerAggregateHook,
            .lower_recursive_cte = lowerRecursiveCteHook,
            .lower_join = lowerJoinHook,
            .lower_query = lowerQueryHook,
            .lower_set_operation = lowerSetOperationHook,
        };
    }

    fn joinedSourceSchema(self: *@This()) runtime_schema.TableSchema {
        return self.source_schema orelse self.schema;
    }

    fn lowerLateralHook(ptr: *anyopaque) anyerror!plan.LoweredLateralPlan {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.callbacks.lower_lateral_with_schemas(self.alloc, self.parsed_sql.?, self.schema, self.joinedSourceSchema(), self.params);
    }

    fn lowerWindowHook(ptr: *anyopaque) anyerror!plan.LoweredWindowPlan {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.callbacks.lower_window(self.alloc, self.parsed_sql.?, self.schema, self.params);
    }

    fn lowerAggregateHook(ptr: *anyopaque) anyerror!plan.LoweredAggregatePlan {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.callbacks.lower_aggregate_plan(self.alloc, self.parsed_sql.?, self.schema, self.params);
    }

    fn lowerRecursiveCteHook(ptr: *anyopaque) anyerror!plan.LoweredRecursiveCtePlan {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.callbacks.lower_recursive_cte_plan(self.alloc, self.parsed_sql.?, self.schema, self.params, self.function_bindings);
    }

    fn lowerJoinHook(ptr: *anyopaque) anyerror!plan.LoweredJoin {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.callbacks.lower_join_with_schemas(self.alloc, self.parsed_sql.?, self.schema, self.joinedSourceSchema(), self.params);
    }

    fn lowerQueryHook(ptr: *anyopaque) anyerror!plan.LoweredQueryPlan {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.callbacks.lower_query_plan(self.alloc, self.parsed_sql.?, self.schema, self.params, self.function_bindings);
    }

    fn lowerSetOperationHook(ptr: *anyopaque) anyerror!plan.LoweredSetOperationPlan {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.callbacks.lower_set_operation_optional_source_schema(self.alloc, self.parsed_sql.?, self.schema, self.source_schema, self.params, self.function_bindings);
    }
};

pub fn lowerReadPlanFromGeneratedReadAstAlloc(
    context: *ReadPlanLoweringContext,
    parsed_sql: *const tokenized.ParsedSql,
    read_ast: generated_parser.GeneratedSqlReadAst,
) !plan.LoweredReadPlan {
    const read_kind = parsed_sql.readStatementKind() orelse return error.UnsupportedSqlShape;
    if (!generatedReadAstMatchesReadKind(read_ast, read_kind)) return error.UnsupportedSqlShape;
    try validateGeneratedReadAstRanges(parsed_sql.items(), read_ast);
    return switch (read_ast.kind) {
        .query => blk: {
            try validateGeneratedSimpleQueryReadAst(parsed_sql.items(), read_ast);
            break :blk .{ .query = try context.callbacks.lower_query_plan(
                context.alloc,
                parsed_sql,
                context.schema,
                context.params,
                context.function_bindings,
            ) };
        },
        .aggregate => blk: {
            try validateGeneratedAggregateReadAst(read_ast);
            break :blk .{ .aggregate = try context.callbacks.lower_aggregate_plan(
                context.alloc,
                parsed_sql,
                context.schema,
                context.params,
            ) };
        },
        .join => blk: {
            try validateGeneratedJoinedReadAst(parsed_sql.items(), read_ast, .join);
            break :blk .{ .join = try context.callbacks.lower_join_with_schemas(
                context.alloc,
                parsed_sql,
                context.schema,
                context.source_schema orelse context.schema,
                context.params,
            ) };
        },
        .lateral => blk: {
            try validateGeneratedJoinedReadAst(parsed_sql.items(), read_ast, .lateral);
            break :blk .{ .lateral = try context.callbacks.lower_lateral_with_schemas(
                context.alloc,
                parsed_sql,
                context.schema,
                context.source_schema orelse context.schema,
                context.params,
            ) };
        },
        .window => blk: {
            try validateGeneratedWindowReadAst(parsed_sql.items(), read_ast);
            break :blk .{ .window = try context.callbacks.lower_window(
                context.alloc,
                parsed_sql,
                context.schema,
                context.params,
            ) };
        },
        .set_operation => blk: {
            try validateGeneratedSetOperationReadAst(read_ast);
            break :blk .{ .set_operation = try context.callbacks.lower_set_operation_optional_source_schema(
                context.alloc,
                parsed_sql,
                context.schema,
                context.source_schema,
                context.params,
                context.function_bindings,
            ) };
        },
        .cte => blk: {
            if (validateGeneratedCteReadAst(parsed_sql.items(), read_ast)) |_| {
                break :blk try lowerGeneratedCteReadPlanAlloc(context, parsed_sql, read_kind);
            } else |err| switch (err) {
                error.UnsupportedSqlShape => break :blk try context.lowerParsed(parsed_sql),
                else => return err,
            }
        },
    };
}

fn generatedReadAstMatchesReadKind(
    read_ast: generated_parser.GeneratedSqlReadAst,
    read_kind: classifier.SqlReadStatementKind,
) bool {
    return switch (read_ast.kind) {
        .query => read_kind == .query,
        .aggregate => read_kind == .aggregate,
        .join => read_kind == .join,
        .lateral => read_kind == .lateral,
        .window => read_kind == .window,
        .set_operation => read_kind == .set_operation,
        .cte => switch (read_kind) {
            .query, .aggregate, .join, .lateral, .window => !read_ast.cte_recursive,
            .recursive_cte => read_ast.cte_recursive,
            .set_operation => false,
        },
    };
}

fn validateGeneratedReadAstRanges(tokens: []const tokenized.Token, read_ast: generated_parser.GeneratedSqlReadAst) !void {
    const ranges = [_]?generated_parser.GeneratedSqlTokenRange{
        read_ast.cte_tokens,
        read_ast.cte_name_tokens,
        read_ast.cte_body_tokens,
        read_ast.distinct_tokens,
        read_ast.projection_tokens,
        read_ast.source_tokens,
        read_ast.join_tokens,
        read_ast.join_operator_tokens,
        read_ast.join_left_tokens,
        read_ast.join_right_tokens,
        read_ast.join_predicate_tokens,
        read_ast.where_tokens,
        read_ast.group_tokens,
        read_ast.having_tokens,
        read_ast.window_tokens,
        read_ast.order_tokens,
        read_ast.limit_tokens,
        read_ast.offset_tokens,
        read_ast.fetch_tokens,
        read_ast.set_operation_tokens,
    };
    for (ranges) |range| {
        if (range) |value| try validateGeneratedReadTokenRange(tokens, read_ast, value);
    }
    try validateGeneratedReadListAstRanges(tokens, read_ast, read_ast.projection_items);
    try validateGeneratedExpressionAstRangesIfPresent(tokens, read_ast, read_ast.projection_first_expression);
    try validateGeneratedExpressionAstRangesIfPresent(tokens, read_ast, read_ast.projection_last_expression);
    try validateGeneratedReadListAstRanges(tokens, read_ast, read_ast.group_items);
    try validateGeneratedExpressionAstRangesIfPresent(tokens, read_ast, read_ast.group_first_expression);
    try validateGeneratedExpressionAstRangesIfPresent(tokens, read_ast, read_ast.group_last_expression);
    try validateGeneratedReadListAstRanges(tokens, read_ast, read_ast.order_items);
    try validateGeneratedExpressionAstRangesIfPresent(tokens, read_ast, read_ast.order_first_expression);
    try validateGeneratedExpressionAstRangesIfPresent(tokens, read_ast, read_ast.order_last_expression);
    try validateGeneratedExpressionAstRangesIfPresent(tokens, read_ast, read_ast.where_expression);
    try validateGeneratedExpressionAstRangesIfPresent(tokens, read_ast, read_ast.having_expression);
    try validateGeneratedExpressionAstRangesIfPresent(tokens, read_ast, read_ast.limit_expression);
    try validateGeneratedExpressionAstRangesIfPresent(tokens, read_ast, read_ast.offset_expression);
    try validateGeneratedExpressionAstRangesIfPresent(tokens, read_ast, read_ast.fetch_count_expression);
    for (read_ast.cte_items) |cte| {
        try validateGeneratedReadTokenRange(tokens, read_ast, cte.name_tokens);
        if (cte.body_tokens) |body_tokens| try validateGeneratedReadTokenRange(tokens, read_ast, body_tokens);
    }
    for (read_ast.join_items) |join| {
        try validateGeneratedReadTokenRange(tokens, read_ast, join.tokens);
        try validateGeneratedReadTokenRange(tokens, read_ast, join.operator_tokens);
        try validateGeneratedReadTokenRange(tokens, read_ast, join.left_tokens);
        try validateGeneratedReadTokenRange(tokens, read_ast, join.right_tokens);
        try validateGeneratedReadTokenRange(tokens, read_ast, join.predicate_tokens);
        try validateGeneratedExpressionAstRangesIfPresent(tokens, read_ast, join.predicate_expression);
    }

    switch (read_ast.kind) {
        .query => {
            if (read_ast.projection_tokens == null) return error.UnsupportedSqlShape;
        },
        .aggregate => {
            if (read_ast.projection_tokens == null) return error.UnsupportedSqlShape;
            if (read_ast.group_tokens == null and read_ast.having_tokens == null and read_ast.distinct_tokens == null) return error.UnsupportedSqlShape;
        },
        .join => {
            if (read_ast.projection_tokens == null or read_ast.source_tokens == null) return error.UnsupportedSqlShape;
            try validateGeneratedReadRangeContainsKeyword(tokens, read_ast.source_tokens.?, .join);
        },
        .lateral => {
            if (read_ast.projection_tokens == null or read_ast.source_tokens == null) return error.UnsupportedSqlShape;
            try validateGeneratedReadRangeContainsKeyword(tokens, read_ast.source_tokens.?, .lateral);
        },
        .window => {
            if (read_ast.projection_tokens == null or read_ast.source_tokens == null) return error.UnsupportedSqlShape;
            try validateGeneratedReadRangeContainsKeyword(tokens, read_ast.projection_tokens.?, .over);
        },
        .cte => {
            if (read_ast.cte_tokens == null or read_ast.projection_tokens == null) return error.UnsupportedSqlShape;
        },
    }
}

fn validateGeneratedSimpleQueryReadAst(tokens: []const tokenized.Token, read_ast: generated_parser.GeneratedSqlReadAst) !void {
    if (read_ast.cte_tokens != null or
        read_ast.group_tokens != null or
        read_ast.having_tokens != null or
        read_ast.window_tokens != null or
        read_ast.set_operation_tokens != null)
    {
        return error.UnsupportedSqlShape;
    }
    const projection = read_ast.projection_tokens orelse return error.UnsupportedSqlShape;
    const source = read_ast.source_tokens orelse return error.UnsupportedSqlShape;
    if (read_ast.distinct_tokens) |range| {
        try validateGeneratedReadRangePrecededByKeyword(tokens, range, .select);
        if (projection.start != range.end) return error.UnsupportedSqlShape;
    } else {
        try validateGeneratedReadRangePrecededByKeyword(tokens, projection, .select);
    }
    try validateGeneratedReadRangePrecededByKeyword(tokens, source, .from);
    if (read_ast.where_tokens) |range| try validateGeneratedReadRangePrecededByKeyword(tokens, range, .where);
    if (read_ast.order_tokens) |range| try validateGeneratedReadOrderRange(tokens, range);
    if (read_ast.limit_tokens) |range| try validateGeneratedReadRangePrecededByKeyword(tokens, range, .limit);
    if (read_ast.offset_tokens) |range| try validateGeneratedReadRangePrecededByKeyword(tokens, range, .offset);
    if (read_ast.fetch_tokens) |range| try validateGeneratedReadRangePrecededByKeyword(tokens, range, .fetch);
}

fn validateGeneratedAggregateReadAst(read_ast: generated_parser.GeneratedSqlReadAst) !void {
    if (read_ast.cte_tokens != null or read_ast.window_tokens != null or read_ast.set_operation_tokens != null) {
        return error.UnsupportedSqlShape;
    }
    if (read_ast.projection_tokens == null or read_ast.source_tokens == null) return error.UnsupportedSqlShape;
    if (read_ast.group_tokens == null and read_ast.having_tokens == null and read_ast.distinct_tokens == null) return error.UnsupportedSqlShape;
}

fn validateGeneratedJoinedReadAst(
    tokens: []const tokenized.Token,
    read_ast: generated_parser.GeneratedSqlReadAst,
    keyword: token_mod.TokenKeyword,
) !void {
    if (read_ast.cte_tokens != null or read_ast.group_tokens != null or read_ast.having_tokens != null or
        read_ast.window_tokens != null or read_ast.set_operation_tokens != null)
    {
        return error.UnsupportedSqlShape;
    }
    if (read_ast.projection_tokens == null or read_ast.source_tokens == null) return error.UnsupportedSqlShape;
    try validateGeneratedReadRangeContainsKeyword(tokens, read_ast.source_tokens.?, keyword);
}

fn validateGeneratedWindowReadAst(tokens: []const tokenized.Token, read_ast: generated_parser.GeneratedSqlReadAst) !void {
    if (read_ast.cte_tokens != null or read_ast.group_tokens != null or read_ast.having_tokens != null or
        read_ast.set_operation_tokens != null)
    {
        return error.UnsupportedSqlShape;
    }
    if (read_ast.projection_tokens == null or read_ast.source_tokens == null) return error.UnsupportedSqlShape;
    try validateGeneratedReadRangeContainsKeyword(tokens, read_ast.projection_tokens.?, .over);
    if (read_ast.window_tokens) |range| try validateGeneratedReadRangePrecededByKeyword(tokens, range, .window);
}

fn validateGeneratedSetOperationReadAst(read_ast: generated_parser.GeneratedSqlReadAst) !void {
    if (read_ast.cte_tokens != null) return error.UnsupportedSqlShape;
    if (read_ast.projection_tokens == null or read_ast.source_tokens == null or read_ast.set_operation_tokens == null) {
        return error.UnsupportedSqlShape;
    }
}

fn validateGeneratedCteReadAst(tokens: []const tokenized.Token, read_ast: generated_parser.GeneratedSqlReadAst) !void {
    const cte = read_ast.cte_tokens orelse return error.UnsupportedSqlShape;
    const name = read_ast.cte_name_tokens orelse return error.UnsupportedSqlShape;
    const body = read_ast.cte_body_tokens orelse return error.UnsupportedSqlShape;
    if (tokens.len == 0 or !tokens[0].matchesKeywordTag(.with)) return error.UnsupportedSqlShape;
    if (cte.start != 1 or name.end != name.start + 1) return error.UnsupportedSqlShape;
    if (read_ast.cte_recursive) {
        if (tokens.len < 2 or !tokens[1].matchesKeywordTag(.recursive) or name.start != 2) return error.UnsupportedSqlShape;
    } else if (tokens.len > 1 and tokens[1].matchesKeywordTag(.recursive)) {
        return error.UnsupportedSqlShape;
    } else if (name.start != cte.start) {
        return error.UnsupportedSqlShape;
    }
    if (name.end >= body.start) return error.UnsupportedSqlShape;
    var saw_as = false;
    var index = name.end;
    while (index < body.start) : (index += 1) {
        if (tokens[index].matchesKeywordTag(.as)) {
            saw_as = true;
            break;
        }
    }
    if (!saw_as) return error.UnsupportedSqlShape;
    if (body.start == 0 or tokens[body.start - 1].kind != .lparen) return error.UnsupportedSqlShape;
    if (body.end >= tokens.len or tokens[body.end].kind != .rparen) return error.UnsupportedSqlShape;
    if (cte.end >= tokens.len or !tokens[cte.end].matchesKeywordTag(.select)) return error.UnsupportedSqlShape;
    if (read_ast.projection_tokens == null or read_ast.source_tokens == null) return error.UnsupportedSqlShape;
}

fn lowerGeneratedCteReadPlanAlloc(
    context: *ReadPlanLoweringContext,
    parsed_sql: *const tokenized.ParsedSql,
    read_kind: classifier.SqlReadStatementKind,
) !plan.LoweredReadPlan {
    return switch (read_kind) {
        .query => .{ .query = try context.callbacks.lower_query_plan(
            context.alloc,
            parsed_sql,
            context.schema,
            context.params,
            context.function_bindings,
        ) },
        .aggregate => .{ .aggregate = try context.callbacks.lower_aggregate_plan(
            context.alloc,
            parsed_sql,
            context.schema,
            context.params,
        ) },
        .join => .{ .join = try context.callbacks.lower_join_with_schemas(
            context.alloc,
            parsed_sql,
            context.schema,
            context.source_schema orelse context.schema,
            context.params,
        ) },
        .lateral => .{ .lateral = try context.callbacks.lower_lateral_with_schemas(
            context.alloc,
            parsed_sql,
            context.schema,
            context.source_schema orelse context.schema,
            context.params,
        ) },
        .window => .{ .window = try context.callbacks.lower_window(
            context.alloc,
            parsed_sql,
            context.schema,
            context.params,
        ) },
        .set_operation => .{ .set_operation = try context.callbacks.lower_set_operation_optional_source_schema(
            context.alloc,
            parsed_sql,
            context.schema,
            context.source_schema,
            context.params,
            context.function_bindings,
        ) },
        .recursive_cte => .{ .recursive_cte = try context.callbacks.lower_recursive_cte_plan(
            context.alloc,
            parsed_sql,
            context.schema,
            context.params,
            context.function_bindings,
        ) },
    };
}

fn validateGeneratedReadTokenRange(
    tokens: []const tokenized.Token,
    read_ast: generated_parser.GeneratedSqlReadAst,
    range: generated_parser.GeneratedSqlTokenRange,
) !void {
    if (range.start >= range.end or range.end > tokens.len) return error.UnsupportedSqlShape;
    const first = tokens[range.start];
    const last = tokens[range.end - 1];
    if (first.source_start < read_ast.statement_span.start) return error.UnsupportedSqlShape;
    if (last.source_end > read_ast.statement_span.end) return error.UnsupportedSqlShape;
}

fn validateGeneratedReadRangeContainsKeyword(
    tokens: []const tokenized.Token,
    range: generated_parser.GeneratedSqlTokenRange,
    keyword: token_mod.TokenKeyword,
) !void {
    var index = range.start;
    while (index < range.end) : (index += 1) {
        if (tokens[index].matchesKeywordTag(keyword)) return;
    }
    return error.UnsupportedSqlShape;
}

fn validateGeneratedReadRangePrecededByKeyword(
    tokens: []const tokenized.Token,
    range: generated_parser.GeneratedSqlTokenRange,
    keyword: token_mod.TokenKeyword,
) !void {
    if (range.start == 0 or !tokens[range.start - 1].matchesKeywordTag(keyword)) return error.UnsupportedSqlShape;
}

fn generatedExpressionAstHasMetadata(expression: generated_parser.GeneratedSqlExpressionAst) bool {
    if (expression.tokens != null or
        expression.inner_tokens != null or
        expression.function_name_tokens != null or
        expression.argument_tokens != null or
        expression.argument_distinct_tokens != null or
        expression.argument_value_tokens != null or
        expression.argument_order_tokens != null or
        expression.within_group_tokens != null or
        expression.within_group_order_tokens != null or
        expression.filter_tokens != null or
        expression.filter_predicate_tokens != null or
        expression.array_tokens != null or
        expression.cast_expression_tokens != null or
        expression.cast_type_tokens != null or
        expression.case_first_when_tokens != null or
        expression.case_last_when_tokens != null or
        expression.case_first_condition_tokens != null or
        expression.case_first_result_tokens != null or
        expression.case_else_tokens != null or
        expression.case_else_expression_tokens != null or
        expression.interval_value_tokens != null or
        expression.timestamp_type_tokens != null or
        expression.timestamp_value_tokens != null or
        expression.current_timestamp_precision_tokens != null or
        expression.extract_field_tokens != null or
        expression.extract_source_tokens != null or
        expression.left_tokens != null or
        expression.negation_tokens != null or
        expression.operator_tokens != null or
        expression.between_modifier_tokens != null or
        expression.quantifier_tokens != null or
        expression.right_tokens != null or
        expression.escape_tokens != null)
    {
        return true;
    }
    return expression.inner_expression != null or
        expression.left_expression != null or
        expression.right_expression != null or
        expression.filter_expression != null or
        expression.escape_expression != null or
        expression.cast_expression != null or
        expression.case_first_condition != null or
        expression.case_first_result != null or
        expression.case_else_expression != null or
        expression.argument_items.count != 0 or
        expression.argument_order_items.count != 0 or
        expression.within_group_order_items.count != 0 or
        expression.array_items.count != 0;
}

fn validateGeneratedExpressionAstRangesIfPresent(
    tokens: []const tokenized.Token,
    read_ast: generated_parser.GeneratedSqlReadAst,
    expression: generated_parser.GeneratedSqlExpressionAst,
) !void {
    if (!generatedExpressionAstHasMetadata(expression)) return;
    try validateGeneratedExpressionAstRanges(tokens, read_ast, expression);
}

fn validateGeneratedExpressionAstRanges(
    tokens: []const tokenized.Token,
    read_ast: generated_parser.GeneratedSqlReadAst,
    expression: generated_parser.GeneratedSqlExpressionAst,
) !void {
    const ranges = [_]?generated_parser.GeneratedSqlTokenRange{
        expression.tokens,
        expression.inner_tokens,
        expression.function_name_tokens,
        expression.argument_tokens,
        expression.argument_distinct_tokens,
        expression.argument_value_tokens,
        expression.argument_order_tokens,
        expression.within_group_tokens,
        expression.within_group_order_tokens,
        expression.filter_tokens,
        expression.filter_predicate_tokens,
        expression.array_tokens,
        expression.cast_expression_tokens,
        expression.cast_type_tokens,
        expression.case_first_when_tokens,
        expression.case_last_when_tokens,
        expression.case_first_condition_tokens,
        expression.case_first_result_tokens,
        expression.case_else_tokens,
        expression.case_else_expression_tokens,
        expression.interval_value_tokens,
        expression.timestamp_type_tokens,
        expression.timestamp_value_tokens,
        expression.current_timestamp_precision_tokens,
        expression.extract_field_tokens,
        expression.extract_source_tokens,
        expression.left_tokens,
        expression.negation_tokens,
        expression.operator_tokens,
        expression.between_modifier_tokens,
        expression.quantifier_tokens,
        expression.right_tokens,
        expression.escape_tokens,
    };
    for (ranges) |range| {
        if (range) |value| try validateGeneratedReadTokenRange(tokens, read_ast, value);
    }
    if (expression.inner_expression) |inner| try validateGeneratedExpressionAstRanges(tokens, read_ast, inner.*);
    if (expression.left_expression) |left| try validateGeneratedExpressionAstRanges(tokens, read_ast, left.*);
    if (expression.right_expression) |right| try validateGeneratedExpressionAstRanges(tokens, read_ast, right.*);
    if (expression.filter_expression) |filter| try validateGeneratedExpressionAstRanges(tokens, read_ast, filter.*);
    if (expression.escape_expression) |escape| try validateGeneratedExpressionAstRanges(tokens, read_ast, escape.*);
    if (expression.cast_expression) |cast_expression| try validateGeneratedExpressionAstRanges(tokens, read_ast, cast_expression.*);
    if (expression.case_first_condition) |case_first_condition| try validateGeneratedExpressionAstRanges(tokens, read_ast, case_first_condition.*);
    if (expression.case_first_result) |case_first_result| try validateGeneratedExpressionAstRanges(tokens, read_ast, case_first_result.*);
    if (expression.case_else_expression) |case_else_expression| try validateGeneratedExpressionAstRanges(tokens, read_ast, case_else_expression.*);
    try validateGeneratedReadListAstRanges(tokens, read_ast, expression.argument_items);
    try validateGeneratedReadListAstRanges(tokens, read_ast, expression.argument_order_items);
    try validateGeneratedReadListAstRanges(tokens, read_ast, expression.within_group_order_items);
    try validateGeneratedReadListAstRanges(tokens, read_ast, expression.array_items);
}

fn validateGeneratedReadListAstRanges(
    tokens: []const tokenized.Token,
    read_ast: generated_parser.GeneratedSqlReadAst,
    list: generated_parser.GeneratedSqlListAst,
) !void {
    if (list.count == 0) {
        if (list.items.len != 0 or
            list.expression_items.len != 0 or
            list.expressions.len != 0 or
            list.first_tokens != null or
            list.last_tokens != null)
        {
            return error.UnsupportedSqlShape;
        }
        return;
    }
    if (list.items.len != list.count or list.expression_items.len != list.count) return error.UnsupportedSqlShape;
    if (list.expressions.len != 0 and list.expressions.len != list.count) return error.UnsupportedSqlShape;
    if (list.alias_items.len != 0 and list.alias_items.len != list.count) return error.UnsupportedSqlShape;
    if (list.alias_name_items.len != 0 and list.alias_name_items.len != list.count) return error.UnsupportedSqlShape;
    if (list.direction_items.len != 0 and list.direction_items.len != list.count) return error.UnsupportedSqlShape;
    if (list.directions.len != 0 and list.directions.len != list.count) return error.UnsupportedSqlShape;
    if (list.order_using_operator_items.len != 0 and list.order_using_operator_items.len != list.count) return error.UnsupportedSqlShape;
    if (list.nulls_order_items.len != 0 and list.nulls_order_items.len != list.count) return error.UnsupportedSqlShape;
    if (list.nulls_orders.len != 0 and list.nulls_orders.len != list.count) return error.UnsupportedSqlShape;

    const first = list.first_tokens orelse return error.UnsupportedSqlShape;
    const last = list.last_tokens orelse return error.UnsupportedSqlShape;
    if (!std.meta.eql(first, list.items[0])) return error.UnsupportedSqlShape;
    if (!std.meta.eql(last, list.items[list.items.len - 1])) return error.UnsupportedSqlShape;

    for (list.items, 0..) |item, index| {
        try validateGeneratedReadTokenRange(tokens, read_ast, item);
        const expression_item = list.expression_items[index];
        try validateGeneratedReadTokenRange(tokens, read_ast, expression_item);
        if (expression_item.start < item.start or expression_item.end > item.end) return error.UnsupportedSqlShape;
        if (list.alias_items.len != 0) {
            if (list.alias_items[index]) |alias| {
                try validateGeneratedReadTokenRange(tokens, read_ast, alias);
                if (alias.start < item.start or alias.end > item.end) return error.UnsupportedSqlShape;
            }
        }
        if (list.alias_name_items.len != 0) {
            if (list.alias_name_items[index]) |alias_name| {
                try validateGeneratedReadTokenRange(tokens, read_ast, alias_name);
                if (alias_name.start < item.start or alias_name.end > item.end) return error.UnsupportedSqlShape;
            }
        }
        if (list.direction_items.len != 0) {
            if (list.direction_items[index]) |direction| {
                try validateGeneratedReadTokenRange(tokens, read_ast, direction);
                if (direction.start < item.start or direction.end > item.end) return error.UnsupportedSqlShape;
            }
        }
        if (list.order_using_operator_items.len != 0) {
            if (list.order_using_operator_items[index]) |operator| {
                try validateGeneratedReadTokenRange(tokens, read_ast, operator);
                if (operator.start < item.start or operator.end > item.end) return error.UnsupportedSqlShape;
            }
        }
        if (list.nulls_order_items.len != 0) {
            if (list.nulls_order_items[index]) |nulls_order| {
                try validateGeneratedReadTokenRange(tokens, read_ast, nulls_order);
                if (nulls_order.start < item.start or nulls_order.end > item.end) return error.UnsupportedSqlShape;
            }
        }
        if (list.expressions.len != 0) try validateGeneratedExpressionAstRanges(tokens, read_ast, list.expressions[index]);
    }
}

fn validateGeneratedReadOrderRange(tokens: []const tokenized.Token, range: generated_parser.GeneratedSqlTokenRange) !void {
    if (range.start < 2) return error.UnsupportedSqlShape;
    if (!tokens[range.start - 2].matchesKeywordTag(.order) or !tokens[range.start - 1].matchesKeywordTag(.by)) {
        return error.UnsupportedSqlShape;
    }
}

pub const CatalogReadPlanLoweringCallbacks = struct {
    lower_document_target: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        source_binding.DocumentBinding,
        []const value_mod.SqlValue,
        lower_expr.SqlFunctionBindings,
    ) anyerror!plan.LoweredReadPlan,
    lower_with_source_schema: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        lower_expr.SqlFunctionBindings,
    ) anyerror!plan.LoweredReadPlan,
    lower_without_source_schema: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        lower_expr.SqlFunctionBindings,
    ) anyerror!plan.LoweredReadPlan,
};

pub const CatalogReadPlanLoweringContext = struct {
    alloc: std.mem.Allocator,
    sql: []const u8 = "",
    schema: runtime_schema.TableSchema,
    params: []const value_mod.SqlValue,
    function_bindings: lower_expr.SqlFunctionBindings,
    callbacks: CatalogReadPlanLoweringCallbacks,
    parsed_sql: ?*const tokenized.ParsedSql = null,

    pub fn lower(
        self: *@This(),
        catalog: table_catalog.CatalogSource,
    ) !plan.LoweredReadPlan {
        var parsed_sql = try tokenized.ParsedSql.initAlloc(self.alloc, self.sql);
        defer parsed_sql.deinit(self.alloc);
        return try self.lowerParsed(&parsed_sql, catalog);
    }

    pub fn lowerParsed(
        self: *@This(),
        parsed_sql: *const tokenized.ParsedSql,
        catalog: table_catalog.CatalogSource,
    ) !plan.LoweredReadPlan {
        const old_parsed_sql = self.parsed_sql;
        self.parsed_sql = parsed_sql;
        defer self.parsed_sql = old_parsed_sql;
        var bound = try binder.bindReadPlanCatalogStatementAlloc(self.alloc, parsed_sql, catalog);
        defer bound.deinit(self.alloc);
        return try binder.lowerReadPlanWithBoundStatementAlloc(self.alloc, &bound, self.hooks());
    }

    fn hooks(self: *@This()) binder.ReadPlanCatalogLoweringHooks {
        return .{
            .ptr = self,
            .lower_document_target = lowerDocumentTarget,
            .lower_with_source_schema = lowerWithSourceSchema,
            .lower_without_source_schema = lowerWithoutSourceSchema,
        };
    }

    fn lowerDocumentTarget(ptr: *anyopaque, document: source_binding.DocumentBinding) anyerror!plan.LoweredReadPlan {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.callbacks.lower_document_target(self.alloc, self.parsed_sql.?, document, self.params, self.function_bindings);
    }

    fn lowerWithSourceSchema(ptr: *anyopaque, source_schema: runtime_schema.TableSchema) anyerror!plan.LoweredReadPlan {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.callbacks.lower_with_source_schema(self.alloc, self.parsed_sql.?, self.schema, source_schema, self.params, self.function_bindings);
    }

    fn lowerWithoutSourceSchema(ptr: *anyopaque) anyerror!plan.LoweredReadPlan {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.callbacks.lower_without_source_schema(self.alloc, self.parsed_sql.?, self.schema, self.params, self.function_bindings);
    }
};

fn runtimeSchemaFromJsonForLoweringContextTestAlloc(alloc: std.mem.Allocator, schema_json: []const u8) !runtime_schema.TableSchema {
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    return try schema_api.deriveRuntimeTableSchema(alloc, parsed);
}

fn lowerReadPlanWithCatalogForLoweringContextTestAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const value_mod.SqlValue,
    catalog: table_catalog.CatalogSource,
) !plan.LoweredReadPlan {
    var context = CatalogReadPlanLoweringContext{
        .alloc = alloc,
        .sql = sql,
        .schema = schema,
        .params = params,
        .function_bindings = .{},
        .callbacks = .{
            .lower_document_target = lowerDocumentTargetParsedSqlForLoweringContextTestAlloc,
            .lower_with_source_schema = lowerReadPlanWithSourceSchemaParsedSqlForLoweringContextTestAlloc,
            .lower_without_source_schema = lowerReadPlanParsedSqlForLoweringContextTestAlloc,
        },
    };
    return try context.lower(catalog);
}

fn lowerDocumentTargetParsedSqlForLoweringContextTestAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    document: source_binding.DocumentBinding,
    params: []const value_mod.SqlValue,
    function_bindings: lower_expr.SqlFunctionBindings,
) !plan.LoweredReadPlan {
    _ = params;
    _ = function_bindings;
    const document_plan = @import("document_plan.zig");
    return switch (parsed_sql.statement.readKind() orelse return error.UnsupportedSqlShape) {
        .aggregate => .{
            .document_aggregate = try document_plan.lowerDocumentAggregatePlanWithOptionalIndexesAndVirtualSchemaCapabilitiesParsedSqlAlloc(
                alloc,
                parsed_sql,
                document.schema,
                document.virtual_schema,
                document.indexes_json,
                document.capabilities,
            ),
        },
        .query => .{
            .document_query = try document_plan.lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(
                alloc,
                parsed_sql,
                document.schema,
                document.virtual_schema,
                document.capabilities,
            ),
        },
        else => error.UnsupportedSqlShape,
    };
}

fn lowerReadPlanWithSourceSchemaParsedSqlForLoweringContextTestAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const value_mod.SqlValue,
    function_bindings: lower_expr.SqlFunctionBindings,
) !plan.LoweredReadPlan {
    return try lowerReadPlanWithOptionalSourceSchemaParsedSqlForLoweringContextTestAlloc(alloc, parsed_sql, schema, source_schema, params, function_bindings);
}

fn lowerReadPlanParsedSqlForLoweringContextTestAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const value_mod.SqlValue,
    function_bindings: lower_expr.SqlFunctionBindings,
) !plan.LoweredReadPlan {
    return try lowerReadPlanWithOptionalSourceSchemaParsedSqlForLoweringContextTestAlloc(alloc, parsed_sql, schema, null, params, function_bindings);
}

fn lowerReadPlanWithOptionalSourceSchemaParsedSqlForLoweringContextTestAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    schema: runtime_schema.TableSchema,
    source_schema: ?runtime_schema.TableSchema,
    params: []const value_mod.SqlValue,
    function_bindings: lower_expr.SqlFunctionBindings,
) !plan.LoweredReadPlan {
    var context = ReadPlanLoweringContext{
        .alloc = alloc,
        .schema = schema,
        .source_schema = source_schema,
        .params = params,
        .function_bindings = function_bindings,
        .callbacks = .{
            .lower_lateral_with_schemas = lowerLateralWithSchemasParsedSqlForLoweringContextTestAlloc,
            .lower_window = lowerWindowParsedSqlForLoweringContextTestAlloc,
            .lower_aggregate_plan = lowerAggregateParsedSqlForLoweringContextTestAlloc,
            .lower_recursive_cte_plan = lowerRecursiveCteParsedSqlForLoweringContextTestAlloc,
            .lower_join_with_schemas = lowerJoinWithSchemasParsedSqlForLoweringContextTestAlloc,
            .lower_query_plan = lowerQueryParsedSqlForLoweringContextTestAlloc,
            .lower_set_operation_optional_source_schema = unsupportedSetOperationParsedSqlForLoweringContextTestAlloc,
        },
    };
    return try context.lowerParsed(parsed_sql);
}

fn lowerReadPlanForLoweringContextTestAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const value_mod.SqlValue,
) !plan.LoweredReadPlan {
    var parsed_sql = try tokenized.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerReadPlanWithOptionalSourceSchemaParsedSqlForLoweringContextTestAlloc(alloc, &parsed_sql, schema, null, params, .{});
}

fn lowerGeneratedReadPlanForLoweringContextTestAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const value_mod.SqlValue,
) !plan.LoweredReadPlan {
    var parsed_sql = try tokenized.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    const generated_raw = parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    const read_ast = switch (generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    var context = ReadPlanLoweringContext{
        .alloc = alloc,
        .schema = schema,
        .source_schema = null,
        .params = params,
        .function_bindings = .{},
        .callbacks = .{
            .lower_lateral_with_schemas = lowerLateralWithSchemasParsedSqlForLoweringContextTestAlloc,
            .lower_window = lowerWindowParsedSqlForLoweringContextTestAlloc,
            .lower_aggregate_plan = lowerAggregateParsedSqlForLoweringContextTestAlloc,
            .lower_recursive_cte_plan = lowerRecursiveCteParsedSqlForLoweringContextTestAlloc,
            .lower_join_with_schemas = lowerJoinWithSchemasParsedSqlForLoweringContextTestAlloc,
            .lower_query_plan = lowerQueryParsedSqlForLoweringContextTestAlloc,
            .lower_set_operation_optional_source_schema = unsupportedSetOperationParsedSqlForLoweringContextTestAlloc,
        },
    };
    return try lowerReadPlanFromGeneratedReadAstAlloc(&context, &parsed_sql, read_ast);
}

fn lowerQueryParsedSqlForLoweringContextTestAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const value_mod.SqlValue,
    function_bindings: lower_expr.SqlFunctionBindings,
) !plan.LoweredQueryPlan {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    const tokens = parsed_sql.items();
    const cte_adapter_shape = parser_mod.tokensStartWithKeywordTag(tokens, .with);

    var parser_state = parser_context.ParserState{
        .alloc = alloc,
        .tokens = tokens,
        .schema = schema,
        .params = params,
        .function_bindings = function_bindings,
    };
    var lowered = lower_expr.parseQueryPlanAlloc(
        alloc,
        tokens,
        &parser_state.pos,
        params,
        parser_context.ParserState.ContextAccessors.cteSelectParserHooks(&parser_state),
        parser_context.ParserState.ContextAccessors.queryPlanParserHooks(&parser_state),
        parser_context.ParserState.ContextAccessors.simpleSelectSetTailHooks(&parser_state),
    ) catch |err| switch (err) {
        error.InvalidRowsRequest => return error.UnsupportedSqlShape,
        error.InvalidSqlCatalog => if (cte_adapter_shape) return error.UnsupportedSqlShape else return err,
        else => return err,
    };
    errdefer lowered.deinit(alloc);
    relational_rows.validateRowsQueryPlanCteOutputAlloc(alloc, schema, lowered.plan) catch |err| switch (err) {
        error.InvalidRowsRequest => return error.UnsupportedSqlShape,
        error.InvalidSqlCatalog => if (cte_adapter_shape) return error.UnsupportedSqlShape else return err,
        else => return err,
    };
    return lowered;
}

fn lowerWindowParsedSqlForLoweringContextTestAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const value_mod.SqlValue,
) !plan.LoweredWindowPlan {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    const tokens = parsed_sql.items();
    const cte_adapter_shape = parser_mod.tokensStartWithKeywordTag(tokens, .with);

    var parser_state = parser_context.ParserState{
        .alloc = alloc,
        .tokens = tokens,
        .schema = schema,
        .params = params,
    };
    var lowered = plan.parseWindowPlanAlloc(
        alloc,
        tokens,
        &parser_state.pos,
        parser_context.ParserState.ContextAccessors.cteSelectParserHooks(&parser_state),
        parser_context.ParserState.ContextAccessors.windowPlanParserHooks(&parser_state),
    ) catch |err| switch (err) {
        error.InvalidRowsRequest => return error.UnsupportedSqlShape,
        error.InvalidSqlCatalog => if (cte_adapter_shape) return error.UnsupportedSqlShape else return err,
        else => return err,
    };
    errdefer lowered.deinit(alloc);
    relational_rows.validateRowsWindowPlanCteOutputAlloc(alloc, schema, lowered.plan) catch |err| switch (err) {
        error.InvalidRowsRequest => return error.UnsupportedSqlShape,
        error.InvalidSqlCatalog => if (cte_adapter_shape) return error.UnsupportedSqlShape else return err,
        else => return err,
    };
    return lowered;
}

fn lowerAggregateParsedSqlForLoweringContextTestAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const value_mod.SqlValue,
) !plan.LoweredAggregatePlan {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    const tokens = parsed_sql.items();
    const cte_adapter_shape = parser_mod.tokensStartWithKeywordTag(tokens, .with);

    var parser_state = parser_context.ParserState{
        .alloc = alloc,
        .tokens = tokens,
        .schema = schema,
        .params = params,
    };
    var lowered = plan.parseAggregatePlanAlloc(
        alloc,
        tokens,
        &parser_state.pos,
        parser_context.ParserState.ContextAccessors.cteSelectParserHooks(&parser_state),
        parser_context.ParserState.ContextAccessors.aggregatePlanParserHooks(&parser_state),
    ) catch |err| switch (err) {
        error.InvalidRowsRequest => return error.UnsupportedSqlShape,
        error.InvalidSqlCatalog => if (cte_adapter_shape) return error.UnsupportedSqlShape else return err,
        else => return err,
    };
    errdefer lowered.deinit(alloc);
    relational_rows.validateRowsAggregatePlanCteOutputAlloc(alloc, schema, lowered.plan) catch |err| switch (err) {
        error.InvalidRowsRequest => return error.UnsupportedSqlShape,
        error.InvalidSqlCatalog => if (cte_adapter_shape) return error.UnsupportedSqlShape else return err,
        else => return err,
    };
    return lowered;
}

fn lowerJoinWithSchemasParsedSqlForLoweringContextTestAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const value_mod.SqlValue,
) !plan.LoweredJoin {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    if (source_schema.storage_mode != .relational or source_schema.primary_key == null) return error.InvalidSqlCatalog;
    const tokens = parsed_sql.items();
    const cte_adapter_shape = parser_mod.tokensStartWithKeywordTag(tokens, .with);

    var parser_state = parser_context.ParserState{
        .alloc = alloc,
        .tokens = tokens,
        .schema = schema,
        .joined_source_schema = source_schema,
        .params = params,
    };
    var lowered = plan.parseJoinPlanAlloc(
        alloc,
        tokens,
        &parser_state.pos,
        parser_context.ParserState.ContextAccessors.joinCteSelectParserHooks(&parser_state),
        parser_context.ParserState.ContextAccessors.joinPlanParserHooks(&parser_state),
    ) catch |err| switch (err) {
        error.InvalidRowsRequest => return error.UnsupportedSqlShape,
        error.InvalidSqlCatalog => if (cte_adapter_shape) return error.UnsupportedSqlShape else return err,
        else => return err,
    };
    errdefer lowered.deinit(alloc);
    relational_rows.validateRowsJoinPlanCteOutputAlloc(alloc, schema, lowered.asPlan()) catch |err| switch (err) {
        error.InvalidRowsRequest => return error.UnsupportedSqlShape,
        error.InvalidSqlCatalog => if (cte_adapter_shape) return error.UnsupportedSqlShape else return err,
        else => return err,
    };
    return lowered;
}

fn lowerLateralWithSchemasParsedSqlForLoweringContextTestAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const value_mod.SqlValue,
) !plan.LoweredLateralPlan {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    if (source_schema.storage_mode != .relational or source_schema.primary_key == null) return error.InvalidSqlCatalog;
    const tokens = parsed_sql.items();
    const cte_adapter_shape = parser_mod.tokensStartWithKeywordTag(tokens, .with);

    var parser_state = parser_context.ParserState{
        .alloc = alloc,
        .tokens = tokens,
        .schema = schema,
        .joined_source_schema = source_schema,
        .params = params,
    };
    var lowered = plan.parseLateralPlanAlloc(
        alloc,
        tokens,
        &parser_state.pos,
        parser_context.ParserState.ContextAccessors.cteSelectParserHooks(&parser_state),
        parser_context.ParserState.ContextAccessors.lateralPlanParserHooks(&parser_state),
    ) catch |err| switch (err) {
        error.InvalidRowsRequest => return error.UnsupportedSqlShape,
        error.InvalidSqlCatalog => if (cte_adapter_shape) return error.UnsupportedSqlShape else return err,
        else => return err,
    };
    errdefer lowered.deinit(alloc);
    relational_rows.validateRowsLateralPlanCteOutputAlloc(alloc, schema, lowered.plan) catch |err| switch (err) {
        error.InvalidRowsRequest => return error.UnsupportedSqlShape,
        error.InvalidSqlCatalog => if (cte_adapter_shape) return error.UnsupportedSqlShape else return err,
        else => return err,
    };
    return lowered;
}

fn lowerRecursiveCteParsedSqlForLoweringContextTestAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const value_mod.SqlValue,
    function_bindings: lower_expr.SqlFunctionBindings,
) anyerror!plan.LoweredRecursiveCtePlan {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    const tokens = parsed_sql.items();
    var parser_state = parser_context.ParserState{
        .alloc = alloc,
        .tokens = tokens,
        .schema = schema,
        .params = params,
        .function_bindings = function_bindings,
    };
    return try plan.parseRecursiveCtePlanAlloc(
        alloc,
        tokens,
        &parser_state.pos,
        parser_context.ParserState.ContextAccessors.recursiveCteParserHooks(&parser_state),
    );
}

fn unsupportedSetOperationParsedSqlForLoweringContextTestAlloc(
    _: std.mem.Allocator,
    _: *const tokenized.ParsedSql,
    _: runtime_schema.TableSchema,
    _: ?runtime_schema.TableSchema,
    _: []const value_mod.SqlValue,
    _: lower_expr.SqlFunctionBindings,
) anyerror!plan.LoweredSetOperationPlan {
    return error.UnsupportedSqlShape;
}

test "sql adapter lowering context lowers generated read AST through typed read plans" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"kind":{"type":"keyword"},"tenant":{"type":"keyword"},"id":{"type":"keyword"},"organization_id":{"type":"keyword"},"customer_id":{"type":"keyword"},"status":{"type":"keyword"},"amount":{"type":"numeric"},"created_at":{"type":"numeric"},"name":{"type":"keyword"}},"required":["kind","tenant","id"],"additionalProperties":false}}},"primary_key":{"columns":["kind","tenant","id"]}}
    ;
    const schema = try runtimeSchemaFromJsonForLoweringContextTestAlloc(alloc, schema_json);
    defer runtime_schema.freeSchema(alloc, schema);

    const cases = [_][]const u8{
        "SELECT id, status FROM usage_records WHERE kind = 'order' ORDER BY created_at DESC LIMIT 5",
        "SELECT status, SUM(amount) AS total FROM usage_records WHERE kind = 'order' GROUP BY status LIMIT 5",
        "SELECT o.id AS order_id, c.name AS customer_name FROM usage_records AS o LEFT JOIN usage_records AS c ON o.tenant = c.tenant AND o.customer_id = c.id WHERE o.kind = 'order' AND c.kind = 'customer' LIMIT 5",
        "SELECT org.id AS organization_id, latest.amount AS latest_amount FROM usage_records AS org LEFT JOIN LATERAL (SELECT amount, created_at FROM usage_records AS bal WHERE bal.organization_id = org.id AND bal.kind = 'balance' ORDER BY 2 DESC LIMIT 1) AS latest ON true WHERE org.kind = 'organization' LIMIT 10",
        "WITH open_usage AS (SELECT tenant, amount, status FROM usage_records WHERE status = 'open') SELECT tenant, SUM(amount) AS total FROM open_usage GROUP BY tenant LIMIT 5",
        "WITH RECURSIVE source_rows AS (SELECT id FROM usage_records WHERE kind = 'order' UNION ALL SELECT child.id FROM usage_records AS child JOIN source_rows AS parent ON child.customer_id = parent.id) SELECT id FROM source_rows",
    };

    for (cases) |sql| {
        var legacy = try lowerReadPlanForLoweringContextTestAlloc(alloc, sql, schema, &.{});
        defer legacy.deinit(alloc);
        var generated = try lowerGeneratedReadPlanForLoweringContextTestAlloc(alloc, sql, schema, &.{});
        defer generated.deinit(alloc);
        try std.testing.expectEqual(std.meta.activeTag(legacy), std.meta.activeTag(generated));
    }
}

test "sql adapter lowering context rejects malformed generated read AST ranges" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"kind":{"type":"keyword"},"tenant":{"type":"keyword"},"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["kind","tenant","id"],"additionalProperties":false}}},"primary_key":{"columns":["kind","tenant","id"]}}
    ;
    const schema = try runtimeSchemaFromJsonForLoweringContextTestAlloc(alloc, schema_json);
    defer runtime_schema.freeSchema(alloc, schema);

    var parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id, status FROM usage_records WHERE kind = 'order'",
    );
    defer parsed_sql.deinit(alloc);
    const generated_raw = parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var read_ast = switch (generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    read_ast.projection_tokens = .{ .start = 2, .end = 2 };

    var context = ReadPlanLoweringContext{
        .alloc = alloc,
        .schema = schema,
        .source_schema = null,
        .params = &.{},
        .function_bindings = .{},
        .callbacks = .{
            .lower_lateral_with_schemas = lowerLateralWithSchemasParsedSqlForLoweringContextTestAlloc,
            .lower_window = lowerWindowParsedSqlForLoweringContextTestAlloc,
            .lower_aggregate_plan = lowerAggregateParsedSqlForLoweringContextTestAlloc,
            .lower_recursive_cte_plan = lowerRecursiveCteParsedSqlForLoweringContextTestAlloc,
            .lower_join_with_schemas = lowerJoinWithSchemasParsedSqlForLoweringContextTestAlloc,
            .lower_query_plan = lowerQueryParsedSqlForLoweringContextTestAlloc,
            .lower_set_operation_optional_source_schema = unsupportedSetOperationParsedSqlForLoweringContextTestAlloc,
        },
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &parsed_sql, read_ast),
    );

    read_ast = switch (generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    read_ast.source_tokens = null;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &parsed_sql, read_ast),
    );

    var malformed_projection_list_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id, status FROM usage_records WHERE kind = 'order'",
    );
    defer malformed_projection_list_parsed_sql.deinit(alloc);
    const malformed_projection_list_generated_raw = malformed_projection_list_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_projection_list_read_ast = switch (malformed_projection_list_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_projection_list_read_ast.projection_items.expression_items[0] = .{ .start = 2, .end = 2 };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_projection_list_parsed_sql, malformed_projection_list_read_ast),
    );

    var malformed_projection_alias_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id AS order_id, status FROM usage_records WHERE kind = 'order'",
    );
    defer malformed_projection_alias_parsed_sql.deinit(alloc);
    const malformed_projection_alias_generated_raw = malformed_projection_alias_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_projection_alias_read_ast = switch (malformed_projection_alias_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_projection_alias_read_ast.projection_items.alias_items[0] = .{ .start = 1, .end = malformed_projection_alias_read_ast.projection_items.items[0].end + 1 };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_projection_alias_parsed_sql, malformed_projection_alias_read_ast),
    );

    var cte_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "WITH source_rows AS (SELECT id FROM usage_records) SELECT id FROM source_rows",
    );
    defer cte_parsed_sql.deinit(alloc);
    const cte_generated_raw = cte_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var cte_read_ast = switch (cte_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    cte_read_ast.cte_body_tokens = .{ .start = 4, .end = 4 };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &cte_parsed_sql, cte_read_ast),
    );

    var recursive_cte_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "WITH RECURSIVE source_rows AS (SELECT id FROM usage_records WHERE kind = 'order' UNION ALL SELECT child.id FROM usage_records AS child JOIN source_rows AS parent ON child.customer_id = parent.id) SELECT id FROM source_rows",
    );
    defer recursive_cte_parsed_sql.deinit(alloc);
    const recursive_cte_generated_raw = recursive_cte_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var recursive_cte_read_ast = switch (recursive_cte_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    recursive_cte_read_ast.cte_recursive = false;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &recursive_cte_parsed_sql, recursive_cte_read_ast),
    );

    recursive_cte_read_ast = switch (recursive_cte_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    recursive_cte_read_ast.cte_name_tokens = .{ .start = 1, .end = 2 };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &recursive_cte_parsed_sql, recursive_cte_read_ast),
    );
}

test "sql adapter lowering context classifies read sql into typed plan families" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"kind":{"type":"keyword"},"tenant":{"type":"keyword"},"id":{"type":"keyword"},"organization_id":{"type":"keyword"},"customer_id":{"type":"keyword"},"status":{"type":"keyword"},"amount":{"type":"numeric"},"created_at":{"type":"numeric"},"name":{"type":"keyword"}},"required":["kind","tenant","id"],"additionalProperties":false}}},"primary_key":{"columns":["kind","tenant","id"]}}
    ;
    const schema = try runtimeSchemaFromJsonForLoweringContextTestAlloc(alloc, schema_json);
    defer runtime_schema.freeSchema(alloc, schema);

    var query = try lowerReadPlanForLoweringContextTestAlloc(
        alloc,
        "SELECT id, status FROM usage_records WHERE kind = 'order' ORDER BY created_at DESC LIMIT 5",
        schema,
        &.{},
    );
    defer query.deinit(alloc);
    switch (query) {
        .query => |lowered| {
            try std.testing.expectEqualStrings("usage_records", lowered.table_name);
            try std.testing.expectEqual(@as(usize, 1), lowered.plan.query.predicates.len);
        },
        else => return error.TestUnexpectedResult,
    }

    var aggregate = try lowerReadPlanForLoweringContextTestAlloc(
        alloc,
        "SELECT status, SUM(amount) AS total FROM usage_records WHERE kind = 'order' GROUP BY status ORDER BY total DESC LIMIT 5",
        schema,
        &.{},
    );
    defer aggregate.deinit(alloc);
    switch (aggregate) {
        .aggregate => |lowered| {
            try std.testing.expectEqualStrings("usage_records", lowered.table_name);
            try std.testing.expectEqual(@as(usize, 0), lowered.plan.ctes.len);
            try std.testing.expectEqual(@as(usize, 1), lowered.plan.aggregate.aggregations.len);
            try std.testing.expectEqual(db_mod.types.RelationalRowsAggregateOp.sum, lowered.plan.aggregate.aggregations[0].op);
        },
        else => return error.TestUnexpectedResult,
    }

    var cte_aggregate = try lowerReadPlanForLoweringContextTestAlloc(
        alloc,
        "WITH open_usage AS (SELECT tenant, amount, status FROM usage_records WHERE status = 'open') SELECT tenant, SUM(amount) AS total FROM open_usage GROUP BY tenant ORDER BY total DESC LIMIT 5",
        schema,
        &.{},
    );
    defer cte_aggregate.deinit(alloc);
    switch (cte_aggregate) {
        .aggregate => |lowered| {
            try std.testing.expectEqualStrings("usage_records", lowered.table_name);
            try std.testing.expectEqual(@as(usize, 1), lowered.plan.ctes.len);
            try std.testing.expectEqualStrings("open_usage", lowered.plan.aggregate.source.source_cte);
            try std.testing.expectEqual(@as(usize, 1), lowered.plan.aggregate.aggregations.len);
        },
        else => return error.TestUnexpectedResult,
    }

    var join = try lowerReadPlanForLoweringContextTestAlloc(
        alloc,
        "SELECT o.id AS order_id, c.name AS customer_name FROM usage_records AS o LEFT JOIN usage_records AS c ON o.tenant = c.tenant AND o.customer_id = c.id WHERE o.kind = 'order' AND c.kind = 'customer' ORDER BY order_id ASC LIMIT 5",
        schema,
        &.{},
    );
    defer join.deinit(alloc);
    switch (join) {
        .join => |lowered| {
            try std.testing.expectEqualStrings("usage_records", lowered.left_table_name);
            try std.testing.expectEqualStrings("usage_records", lowered.right_table_name);
            try std.testing.expectEqual(@as(usize, 2), lowered.join.on.len);
            try std.testing.expectEqual(@as(usize, 2), lowered.join.select.len);
        },
        else => return error.TestUnexpectedResult,
    }

    var lateral = try lowerReadPlanForLoweringContextTestAlloc(
        alloc,
        "SELECT org.id AS organization_id, latest.amount AS latest_amount FROM usage_records AS org LEFT JOIN LATERAL (SELECT amount, created_at FROM usage_records AS bal WHERE bal.organization_id = org.id AND bal.kind = 'balance' ORDER BY 2 DESC LIMIT 1) AS latest ON true WHERE org.kind = 'organization' ORDER BY latest_amount DESC LIMIT 10",
        schema,
        &.{},
    );
    defer lateral.deinit(alloc);
    switch (lateral) {
        .lateral => |lowered| {
            try std.testing.expectEqualStrings("usage_records", lowered.left_table_name);
            try std.testing.expectEqual(@as(usize, 1), lowered.plan.lateral.correlations.len);
        },
        else => return error.TestUnexpectedResult,
    }

    var window = try lowerReadPlanForLoweringContextTestAlloc(
        alloc,
        "SELECT tenant, id, row_number() OVER (PARTITION BY tenant ORDER BY amount DESC, id ASC) AS row_num FROM usage_records WHERE status = 'open' ORDER BY row_num ASC LIMIT 5",
        schema,
        &.{},
    );
    defer window.deinit(alloc);
    switch (window) {
        .window => |lowered| {
            try std.testing.expectEqualStrings("usage_records", lowered.table_name);
            try std.testing.expectEqual(@as(usize, 1), lowered.plan.window.windows.len);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "sql adapter lowering context derives document virtual fields from typed paths and full text fields" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"document","default_type":"doc","enforce_types":false,"document_schemas":{"doc":{"schema":{"type":"object","properties":{"body":{"type":"text"},"status":{"type":"keyword","x-antfly-index":false}},"additionalProperties":true}}}}
    ;
    const schema = try runtimeSchemaFromJsonForLoweringContextTestAlloc(alloc, schema_json);
    defer runtime_schema.freeSchema(alloc, schema);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{
                        .table_id = 31,
                        .name = "docs",
                        .placement_role = "data",
                        .schema_json = schema_json,
                        .indexes_json = "{\"body_fts\":{\"type\":\"full_text\",\"field\":\"body\"},\"category_fts\":{\"type\":\"full_text\",\"field\":\"category\"},\"typed_paths\":{\"keyword\":[\"metadata.plan\"]}}",
                    },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var covered = try lowerReadPlanWithCatalogForLoweringContextTestAlloc(
        alloc,
        "SELECT _id, body FROM docs WHERE body LIKE 'alpha%' LIMIT 10",
        schema,
        &.{},
        Catalog.iface(),
    );
    defer covered.deinit(alloc);
    switch (covered) {
        .document_query => |document| {
            try std.testing.expectEqualStrings("{\"prefix\":{\"path\":\"/body\",\"value\":\"alpha\"}}", document.producer.indexed_query.filter_query_json.?);
        },
        else => return error.TestUnexpectedResult,
    }

    var star = try lowerReadPlanWithCatalogForLoweringContextTestAlloc(
        alloc,
        "SELECT * FROM docs WHERE _id = 'doc:a'",
        schema,
        &.{},
        Catalog.iface(),
    );
    defer star.deinit(alloc);
    switch (star) {
        .document_query => |document| {
            try std.testing.expectEqual(@as(usize, 6), document.projection.len);
            try std.testing.expectEqualStrings("_id", document.projection[0].output);
            try std.testing.expectEqualStrings("_doc", document.projection[1].output);
            try std.testing.expectEqualStrings("body", document.projection[2].output);
            try std.testing.expectEqualStrings("status", document.projection[3].output);
            try std.testing.expectEqualStrings("category", document.projection[4].output);
            try std.testing.expectEqualStrings("category", document.projection[4].field);
            try std.testing.expectEqualStrings("metadata", document.projection[5].output);
            try std.testing.expectEqualStrings("metadata", document.projection[5].field);
        },
        else => return error.TestUnexpectedResult,
    }

    var explicit_virtual_projection = try lowerReadPlanWithCatalogForLoweringContextTestAlloc(
        alloc,
        "SELECT category FROM docs WHERE _id = 'doc:a'",
        schema,
        &.{},
        Catalog.iface(),
    );
    defer explicit_virtual_projection.deinit(alloc);
    switch (explicit_virtual_projection) {
        .document_query => |document| {
            try std.testing.expectEqual(@as(usize, 1), document.projection.len);
            try std.testing.expectEqualStrings("category", document.projection[0].field);
            try std.testing.expectEqualStrings("category", document.projection[0].output);
        },
        else => return error.TestUnexpectedResult,
    }

    var full_text_virtual_filter = try lowerReadPlanWithCatalogForLoweringContextTestAlloc(
        alloc,
        "SELECT _id, category FROM docs WHERE category = 'release' LIMIT 10",
        schema,
        &.{},
        Catalog.iface(),
    );
    defer full_text_virtual_filter.deinit(alloc);
    switch (full_text_virtual_filter) {
        .document_query => |document| {
            try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/category\",\"value\":\"release\"}}", document.producer.indexed_query.filter_query_json.?);
            try std.testing.expectEqualStrings("category", document.projection[1].field);
            try std.testing.expectEqualStrings("category", document.projection[1].output);
        },
        else => return error.TestUnexpectedResult,
    }

    var residual = try lowerReadPlanWithCatalogForLoweringContextTestAlloc(
        alloc,
        "SELECT _id, status FROM docs WHERE status = 'active' LIMIT 10",
        schema,
        &.{},
        Catalog.iface(),
    );
    defer residual.deinit(alloc);
    switch (residual) {
        .document_query => |document| {
            try std.testing.expectEqual(source_binding.default_document_sql_bounded_scan_rows, document.producer.bounded_scan.max_rows);
            try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/status\",\"value\":\"active\"}}", document.producer.bounded_scan.residual_filter_json.?);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "sql adapter lowering context lowers catalog-backed equality join read plans" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"kind":{"type":"keyword"},"tenant":{"type":"keyword"},"id":{"type":"keyword"},"customer_id":{"type":"keyword"},"name":{"type":"keyword"},"scope":{"type":"keyword"},"amount":{"type":"numeric"},"enabled":{"type":"boolean"}},"required":["kind","tenant","id"],"additionalProperties":false}}},"primary_key":{"columns":["kind","tenant","id"]}}
    ;
    const schema = try runtimeSchemaFromJsonForLoweringContextTestAlloc(alloc, schema_json);
    defer runtime_schema.freeSchema(alloc, schema);

    const customer_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"name":{"type":"keyword"},"enabled":{"type":"boolean"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{
                        .table_id = 11,
                        .name = "usage_records",
                        .placement_role = "data",
                        .schema_json = schema_json,
                    },
                    .{
                        .table_id = 12,
                        .name = "customers",
                        .placement_role = "data",
                        .schema_json = customer_schema_json,
                    },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var catalog_plan = try lowerReadPlanWithCatalogForLoweringContextTestAlloc(
        alloc,
        "SELECT o.id AS order_id, c.name AS customer_name FROM public.usage_records AS o LEFT JOIN public.customers AS c ON o.customer_id = c.id WHERE c.enabled IS TRUE ORDER BY 1 ASC LIMIT 2",
        schema,
        &.{},
        Catalog.iface(),
    );
    defer catalog_plan.deinit(alloc);
    switch (catalog_plan) {
        .join => |catalog_join| {
            try std.testing.expectEqualStrings("usage_records", catalog_join.left_table_name);
            try std.testing.expectEqualStrings("customers", catalog_join.right_table_name);
            const catalog_join_plan = catalog_join.asPlan();
            try std.testing.expectEqualStrings("usage_records", catalog_join_plan.left_table);
            try std.testing.expectEqualStrings("customers", catalog_join_plan.right_table);
            try std.testing.expectEqual(@as(usize, 1), catalog_join.join.right.predicates.len);
            try std.testing.expectEqualStrings("enabled", catalog_join.join.right.predicates[0].field);
            try std.testing.expectEqualStrings("true", catalog_join.join.right.predicates[0].value_json.?);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "sql adapter lowering context lowers catalog-backed bounded left join lateral read plans" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"kind":{"type":"keyword"},"tenant":{"type":"keyword"},"id":{"type":"keyword"},"organization_id":{"type":"keyword"},"scope":{"type":"keyword"},"amount":{"type":"numeric"},"created_at":{"type":"numeric"},"enabled":{"type":"boolean"}},"required":["kind","tenant","id"],"additionalProperties":false}}},"primary_key":{"columns":["kind","tenant","id"]}}
    ;
    const schema = try runtimeSchemaFromJsonForLoweringContextTestAlloc(alloc, schema_json);
    defer runtime_schema.freeSchema(alloc, schema);

    const balance_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"organization_id":{"type":"keyword"},"kind":{"type":"keyword"},"amount":{"type":"numeric"},"created_at":{"type":"numeric"}},"required":["id","organization_id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{
                        .table_id = 21,
                        .name = "usage_records",
                        .placement_role = "data",
                        .schema_json = schema_json,
                    },
                    .{
                        .table_id = 22,
                        .name = "balance_records",
                        .placement_role = "data",
                        .schema_json = balance_schema_json,
                    },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var catalog_plan = try lowerReadPlanWithCatalogForLoweringContextTestAlloc(
        alloc,
        "SELECT org.id AS organization_id, latest.amount AS latest_amount FROM public.usage_records AS org LEFT JOIN LATERAL (SELECT amount, created_at FROM public.balance_records AS bal WHERE bal.organization_id = org.id AND bal.kind = 'balance' ORDER BY 2 DESC LIMIT 1) AS latest ON true WHERE org.kind = 'organization' ORDER BY latest_amount DESC LIMIT 10",
        schema,
        &.{},
        Catalog.iface(),
    );
    defer catalog_plan.deinit(alloc);
    switch (catalog_plan) {
        .lateral => |catalog_lateral| {
            try std.testing.expectEqualStrings("usage_records", catalog_lateral.left_table_name);
            try std.testing.expectEqualStrings("balance_records", catalog_lateral.right_table_name);
            try std.testing.expectEqualStrings("usage_records", catalog_lateral.plan.left_table);
            try std.testing.expectEqualStrings("balance_records", catalog_lateral.plan.right_table);
            try std.testing.expectEqual(@as(usize, 1), catalog_lateral.plan.lateral.right.predicates.len);
            try std.testing.expectEqualStrings("kind", catalog_lateral.plan.lateral.right.predicates[0].field);
            try std.testing.expectEqualStrings("\"balance\"", catalog_lateral.plan.lateral.right.predicates[0].value_json.?);
            try std.testing.expectEqual(@as(usize, 1), catalog_lateral.plan.lateral.correlations.len);
            try std.testing.expectEqualStrings("organization_id", catalog_lateral.plan.lateral.correlations[0].right_field);
        },
        else => return error.TestUnexpectedResult,
    }
}

pub const CatalogWritePlanLoweringCallbacks = struct {
    lower_with_options: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        plan.LowerWritePlanOptions,
    ) anyerror!plan.LoweredWritePlan,
};

pub const CatalogWritePlanLoweringContext = struct {
    alloc: std.mem.Allocator,
    sql: []const u8 = "",
    schema: runtime_schema.TableSchema,
    params: []const value_mod.SqlValue,
    callbacks: CatalogWritePlanLoweringCallbacks,
    parsed_sql: ?*const tokenized.ParsedSql = null,

    pub fn lower(
        self: *@This(),
        options: plan.LowerWritePlanOptions,
        catalog: table_catalog.CatalogSource,
    ) !plan.LoweredWritePlan {
        var parsed_sql = try tokenized.ParsedSql.initAlloc(self.alloc, self.sql);
        defer parsed_sql.deinit(self.alloc);
        return try self.lowerParsed(&parsed_sql, options, catalog);
    }

    pub fn lowerParsed(
        self: *@This(),
        parsed_sql: *const tokenized.ParsedSql,
        options: plan.LowerWritePlanOptions,
        catalog: table_catalog.CatalogSource,
    ) !plan.LoweredWritePlan {
        const old_parsed_sql = self.parsed_sql;
        self.parsed_sql = parsed_sql;
        defer self.parsed_sql = old_parsed_sql;
        var bound = try binder.bindWritePlanCatalogStatementAlloc(self.alloc, parsed_sql, options, catalog);
        defer bound.deinit(self.alloc);
        return try binder.lowerWritePlanWithBoundStatementAlloc(self.alloc, &bound, self.hooks());
    }

    fn hooks(self: *@This()) binder.WritePlanCatalogLoweringHooks {
        return .{
            .ptr = self,
            .lower_with_options = lowerWithOptions,
        };
    }

    fn lowerWithOptions(ptr: *anyopaque, resolved_options: plan.LowerWritePlanOptions) anyerror!plan.LoweredWritePlan {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.callbacks.lower_with_options(self.alloc, self.parsed_sql.?, self.schema, self.params, resolved_options);
    }
};

pub const WritePlanLoweringCallbacks = struct {
    lower_recursive_insert_source_with_schemas: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        relational_rows.UniqueSelectorResolver,
    ) anyerror!plan.LoweredRecursiveInsertSource,
    lower_recursive_update_joined_source_with_schemas: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        db_mod.types.RowClaimRequest,
    ) anyerror!plan.LoweredRecursiveJoinedMutationSource,
    lower_recursive_delete_joined_source_with_schemas: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        db_mod.types.RowClaimRequest,
    ) anyerror!plan.LoweredRecursiveJoinedMutationSource,
    lower_recursive_merge_mutation_with_schemas: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
    ) anyerror!plan.LoweredRecursiveMergeMutation,
    lower_insert_with_resolver: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        relational_rows.UniqueSelectorResolver,
    ) anyerror!plan.LoweredInsert,
    lower_insert_source_with_resolver: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        relational_rows.UniqueSelectorResolver,
    ) anyerror!plan.LoweredInsertSource,
    lower_insert_source_with_schemas: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        relational_rows.UniqueSelectorResolver,
    ) anyerror!plan.LoweredInsertSource,
    lower_update_joined_source_with_schemas: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        db_mod.types.RowClaimRequest,
    ) anyerror!plan.LoweredJoinedMutationSource,
    classify_update_selector: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        ?relational_rows.UniqueSelectorResolver,
    ) anyerror!plan.MutationSelectorKind,
    lower_update_with_resolver: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        relational_rows.UniqueSelectorResolver,
    ) anyerror!plan.LoweredMutation,
    lower_update_source: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        db_mod.types.RowClaimRequest,
    ) anyerror!plan.LoweredMutationSource,
    lower_delete_joined_source_with_schemas: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        db_mod.types.RowClaimRequest,
    ) anyerror!plan.LoweredJoinedMutationSource,
    classify_delete_selector: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        ?relational_rows.UniqueSelectorResolver,
    ) anyerror!plan.MutationSelectorKind,
    lower_delete_with_resolver: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        relational_rows.UniqueSelectorResolver,
    ) anyerror!plan.LoweredMutation,
    lower_delete_source: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        db_mod.types.RowClaimRequest,
    ) anyerror!plan.LoweredMutationSource,
    lower_truncate_source: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        db_mod.types.RowClaimRequest,
    ) anyerror!plan.LoweredMutationSource,
    lower_merge_mutation_with_schemas: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
    ) anyerror!plan.LoweredMergeMutationPlan,
};

pub const WritePlanLoweringContext = struct {
    alloc: std.mem.Allocator,
    sql: []const u8 = "",
    schema: runtime_schema.TableSchema,
    params: []const value_mod.SqlValue,
    callbacks: WritePlanLoweringCallbacks,
    parsed_sql: ?*const tokenized.ParsedSql = null,

    pub fn lower(self: *@This(), options: plan.LowerWritePlanOptions) !plan.LoweredWritePlan {
        var parsed_sql = try tokenized.ParsedSql.initAlloc(self.alloc, self.sql);
        defer parsed_sql.deinit(self.alloc);
        return try self.lowerParsed(&parsed_sql, options);
    }

    pub fn lowerParsed(self: *@This(), parsed_sql: *const tokenized.ParsedSql, options: plan.LowerWritePlanOptions) !plan.LoweredWritePlan {
        const old_parsed_sql = self.parsed_sql;
        self.parsed_sql = parsed_sql;
        defer self.parsed_sql = old_parsed_sql;
        return try plan.lowerWritePlanWithParsedSqlAlloc(parsed_sql, self.schema, options, self.hooks());
    }

    fn hooks(self: *@This()) plan.WritePlanLoweringHooks {
        return .{
            .ptr = self,
            .lower_recursive_insert_source = lowerRecursiveInsertSource,
            .lower_recursive_update_joined_source = lowerRecursiveUpdateJoinedSource,
            .lower_recursive_delete_joined_source = lowerRecursiveDeleteJoinedSource,
            .lower_recursive_merge_mutation = lowerRecursiveMergeMutation,
            .lower_insert = lowerInsert,
            .lower_insert_source = lowerInsertSource,
            .lower_insert_source_with_schema = lowerInsertSourceWithSchema,
            .lower_update_joined_source = lowerUpdateJoinedSource,
            .classify_update_selector = classifyUpdateSelector,
            .lower_update = lowerUpdate,
            .lower_update_source = lowerUpdateSource,
            .lower_delete_joined_source = lowerDeleteJoinedSource,
            .classify_delete_selector = classifyDeleteSelector,
            .lower_delete = lowerDelete,
            .lower_delete_source = lowerDeleteSource,
            .lower_truncate_source = lowerTruncateSource,
            .lower_merge_mutation = lowerMergeMutation,
        };
    }

    fn fromPtr(ptr: *anyopaque) *@This() {
        return @ptrCast(@alignCast(ptr));
    }

    fn lowerRecursiveInsertSource(ptr: *anyopaque, source_schema: runtime_schema.TableSchema, resolver: relational_rows.UniqueSelectorResolver) anyerror!plan.LoweredRecursiveInsertSource {
        const self = fromPtr(ptr);
        return try self.callbacks.lower_recursive_insert_source_with_schemas(self.alloc, self.parsed_sql.?, self.schema, source_schema, self.params, resolver);
    }

    fn lowerRecursiveUpdateJoinedSource(ptr: *anyopaque, source_schema: runtime_schema.TableSchema, row_claim: db_mod.types.RowClaimRequest) anyerror!plan.LoweredRecursiveJoinedMutationSource {
        const self = fromPtr(ptr);
        return try self.callbacks.lower_recursive_update_joined_source_with_schemas(self.alloc, self.parsed_sql.?, self.schema, source_schema, self.params, row_claim);
    }

    fn lowerRecursiveDeleteJoinedSource(ptr: *anyopaque, source_schema: runtime_schema.TableSchema, row_claim: db_mod.types.RowClaimRequest) anyerror!plan.LoweredRecursiveJoinedMutationSource {
        const self = fromPtr(ptr);
        return try self.callbacks.lower_recursive_delete_joined_source_with_schemas(self.alloc, self.parsed_sql.?, self.schema, source_schema, self.params, row_claim);
    }

    fn lowerRecursiveMergeMutation(ptr: *anyopaque, source_schema: runtime_schema.TableSchema) anyerror!plan.LoweredRecursiveMergeMutation {
        const self = fromPtr(ptr);
        return try self.callbacks.lower_recursive_merge_mutation_with_schemas(self.alloc, self.parsed_sql.?, self.schema, source_schema, self.params);
    }

    fn lowerInsert(ptr: *anyopaque, resolver: relational_rows.UniqueSelectorResolver) anyerror!plan.LoweredInsert {
        const self = fromPtr(ptr);
        return try self.callbacks.lower_insert_with_resolver(self.alloc, self.parsed_sql.?, self.schema, self.params, resolver);
    }

    fn lowerInsertSource(ptr: *anyopaque, resolver: relational_rows.UniqueSelectorResolver) anyerror!plan.LoweredInsertSource {
        const self = fromPtr(ptr);
        return try self.callbacks.lower_insert_source_with_resolver(self.alloc, self.parsed_sql.?, self.schema, self.params, resolver);
    }

    fn lowerInsertSourceWithSchema(ptr: *anyopaque, source_schema: runtime_schema.TableSchema, resolver: relational_rows.UniqueSelectorResolver) anyerror!plan.LoweredInsertSource {
        const self = fromPtr(ptr);
        return try self.callbacks.lower_insert_source_with_schemas(self.alloc, self.parsed_sql.?, self.schema, source_schema, self.params, resolver);
    }

    fn lowerUpdateJoinedSource(ptr: *anyopaque, source_schema: runtime_schema.TableSchema, row_claim: db_mod.types.RowClaimRequest) anyerror!plan.LoweredJoinedMutationSource {
        const self = fromPtr(ptr);
        return try self.callbacks.lower_update_joined_source_with_schemas(self.alloc, self.parsed_sql.?, self.schema, source_schema, self.params, row_claim);
    }

    fn classifyUpdateSelector(ptr: *anyopaque, resolver: ?relational_rows.UniqueSelectorResolver) anyerror!plan.MutationSelectorKind {
        const self = fromPtr(ptr);
        return try self.callbacks.classify_update_selector(self.alloc, self.parsed_sql.?, self.schema, self.params, resolver);
    }

    fn lowerUpdate(ptr: *anyopaque, resolver: relational_rows.UniqueSelectorResolver) anyerror!plan.LoweredMutation {
        const self = fromPtr(ptr);
        return try self.callbacks.lower_update_with_resolver(self.alloc, self.parsed_sql.?, self.schema, self.params, resolver);
    }

    fn lowerUpdateSource(ptr: *anyopaque, row_claim: db_mod.types.RowClaimRequest) anyerror!plan.LoweredMutationSource {
        const self = fromPtr(ptr);
        return try self.callbacks.lower_update_source(self.alloc, self.parsed_sql.?, self.schema, self.params, row_claim);
    }

    fn lowerDeleteJoinedSource(ptr: *anyopaque, source_schema: runtime_schema.TableSchema, row_claim: db_mod.types.RowClaimRequest) anyerror!plan.LoweredJoinedMutationSource {
        const self = fromPtr(ptr);
        return try self.callbacks.lower_delete_joined_source_with_schemas(self.alloc, self.parsed_sql.?, self.schema, source_schema, self.params, row_claim);
    }

    fn classifyDeleteSelector(ptr: *anyopaque, resolver: ?relational_rows.UniqueSelectorResolver) anyerror!plan.MutationSelectorKind {
        const self = fromPtr(ptr);
        return try self.callbacks.classify_delete_selector(self.alloc, self.parsed_sql.?, self.schema, self.params, resolver);
    }

    fn lowerDelete(ptr: *anyopaque, resolver: relational_rows.UniqueSelectorResolver) anyerror!plan.LoweredMutation {
        const self = fromPtr(ptr);
        return try self.callbacks.lower_delete_with_resolver(self.alloc, self.parsed_sql.?, self.schema, self.params, resolver);
    }

    fn lowerDeleteSource(ptr: *anyopaque, row_claim: db_mod.types.RowClaimRequest) anyerror!plan.LoweredMutationSource {
        const self = fromPtr(ptr);
        return try self.callbacks.lower_delete_source(self.alloc, self.parsed_sql.?, self.schema, self.params, row_claim);
    }

    fn lowerTruncateSource(ptr: *anyopaque, row_claim: db_mod.types.RowClaimRequest) anyerror!plan.LoweredMutationSource {
        const self = fromPtr(ptr);
        return try self.callbacks.lower_truncate_source(self.alloc, self.parsed_sql.?, self.schema, row_claim);
    }

    fn lowerMergeMutation(ptr: *anyopaque, source_schema: runtime_schema.TableSchema) anyerror!plan.LoweredMergeMutationPlan {
        const self = fromPtr(ptr);
        return try self.callbacks.lower_merge_mutation_with_schemas(self.alloc, self.parsed_sql.?, self.schema, source_schema, self.params);
    }
};

pub const ExplainPlanLoweringCallbacks = struct {
    lower_read_with_catalog: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        table_catalog.CatalogSource,
        lower_expr.SqlFunctionBindings,
    ) anyerror!plan.LoweredReadPlan,
    lower_read_without_catalog: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        lower_expr.SqlFunctionBindings,
    ) anyerror!plan.LoweredReadPlan,
    lower_write_with_catalog: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        plan.LowerWritePlanOptions,
        table_catalog.CatalogSource,
    ) anyerror!plan.LoweredWritePlan,
    lower_write_without_catalog: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        plan.LowerWritePlanOptions,
    ) anyerror!plan.LoweredWritePlan,
};

pub const ExplainPlanLoweringContext = struct {
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    params: []const value_mod.SqlValue,
    options: plan.LowerWritePlanOptions,
    catalog: ?table_catalog.CatalogSource,
    function_bindings: lower_expr.SqlFunctionBindings,
    callbacks: ExplainPlanLoweringCallbacks,

    pub fn lower(self: *@This(), sql: []const u8) !plan.LoweredExplainPlan {
        var parsed_sql = try tokenized.ParsedSql.initAlloc(self.alloc, sql);
        defer parsed_sql.deinit(self.alloc);
        return try self.lowerParsed(&parsed_sql);
    }

    pub fn lowerParsed(self: *@This(), parsed_sql: *const tokenized.ParsedSql) !plan.LoweredExplainPlan {
        return try plan.lowerExplainPlanWithParsedSqlAlloc(self.alloc, parsed_sql, self.hooks());
    }

    fn hooks(self: *@This()) plan.ExplainPlanLoweringHooks {
        return .{
            .ptr = self,
            .lower_read = lowerReadHook,
            .lower_write = lowerWriteHook,
        };
    }

    fn lowerReadHook(ptr: *anyopaque, parsed_sql: *const tokenized.ParsedSql) anyerror!plan.LoweredReadPlan {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (self.catalog) |source_catalog| {
            return try self.callbacks.lower_read_with_catalog(self.alloc, parsed_sql, self.schema, self.params, source_catalog, self.function_bindings);
        }
        return try self.callbacks.lower_read_without_catalog(self.alloc, parsed_sql, self.schema, self.params, self.function_bindings);
    }

    fn lowerWriteHook(ptr: *anyopaque, parsed_sql: *const tokenized.ParsedSql) anyerror!plan.LoweredWritePlan {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (self.catalog) |source_catalog| {
            return try self.callbacks.lower_write_with_catalog(self.alloc, parsed_sql, self.schema, self.params, self.options, source_catalog);
        }
        return try self.callbacks.lower_write_without_catalog(self.alloc, parsed_sql, self.schema, self.params, self.options);
    }
};

pub const RelationPopulationLoweringCallbacks = struct {
    lower_read_with_catalog: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        table_catalog.CatalogSource,
        lower_expr.SqlFunctionBindings,
    ) anyerror!plan.LoweredReadPlan,
    lower_read_without_catalog: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        lower_expr.SqlFunctionBindings,
    ) anyerror!plan.LoweredReadPlan,
};

pub const RelationPopulationLoweringContext = struct {
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    params: []const value_mod.SqlValue,
    catalog: ?table_catalog.CatalogSource,
    function_bindings: lower_expr.SqlFunctionBindings,
    callbacks: RelationPopulationLoweringCallbacks,

    pub fn lower(self: *@This(), sql: []const u8) !plan.LoweredRelationPopulationPlan {
        var parsed_sql = try tokenized.ParsedSql.initAlloc(self.alloc, sql);
        defer parsed_sql.deinit(self.alloc);
        return try self.lowerParsed(&parsed_sql);
    }

    pub fn lowerParsed(self: *@This(), parsed_sql: *const tokenized.ParsedSql) !plan.LoweredRelationPopulationPlan {
        return try plan.lowerRelationPopulationPlanWithParsedSqlAlloc(self.alloc, parsed_sql, self.hooks());
    }

    fn hooks(self: *@This()) plan.RelationPopulationLoweringHooks {
        return .{
            .ptr = self,
            .lower_read = lowerRead,
        };
    }

    fn lowerRead(ptr: *anyopaque, parsed_sql: *const tokenized.ParsedSql) anyerror!plan.LoweredReadPlan {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (self.catalog) |source_catalog| {
            return try self.callbacks.lower_read_with_catalog(self.alloc, parsed_sql, self.schema, self.params, source_catalog, self.function_bindings);
        }
        return try self.callbacks.lower_read_without_catalog(self.alloc, parsed_sql, self.schema, self.params, self.function_bindings);
    }
};
