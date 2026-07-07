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
const catalog_resources = @import("catalog_resources.zig");
const sql_statement_kind = @import("statement_kind.zig");
const db_mod = @import("../storage/db/mod.zig");
const generated_parser = @import("generated_parser.zig");
const generated_read_validate = @import("generated_read_validate.zig");
const lower_expr = @import("lower_expr.zig");
const expr_row_parse = @import("expr/row_parse.zig");
const metadata_api = @import("../metadata/api.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const metadata_transition_state = @import("../metadata/transition_state.zig");
const parser_mod = @import("parser.zig");
const parser_context = @import("parser_context.zig");
const plan = @import("plan.zig");
const query_function = @import("query_function.zig");
const raft_reconciler = @import("../raft/reconciler.zig");
const relational_rows = @import("relational_rows.zig");
const table_catalog = @import("../metadata/catalog/source.zig");
const runtime_schema = @import("../storage/schema.zig");
const schema_api = @import("../schema/mod.zig");
const source_binding = @import("source_binding.zig");
const token_mod = @import("token.zig");
const tokenized = @import("tokenized.zig");
const value_mod = @import("value.zig");

const GeneratedReadValidationError = error{UnsupportedSqlShape};

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
        expr_row_parse.SqlFunctionBindings,
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
        ?runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        expr_row_parse.SqlFunctionBindings,
    ) anyerror!plan.LoweredQueryPlan,
    lower_set_operation_optional_source_schema: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        ?runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        expr_row_parse.SqlFunctionBindings,
    ) anyerror!plan.LoweredSetOperationPlan,
};

pub const ReadPlanLoweringContext = struct {
    alloc: std.mem.Allocator,
    sql: []const u8 = "",
    schema: runtime_schema.TableSchema,
    source_schema: ?runtime_schema.TableSchema,
    params: []const value_mod.SqlValue,
    function_bindings: expr_row_parse.SqlFunctionBindings,
    callbacks: ReadPlanLoweringCallbacks,
    statement_kind: ?sql_statement_kind.SqlReadStatementKind = null,
    parsed_sql: ?*const tokenized.ParsedSql = null,

    pub fn lower(self: *@This()) !plan.LoweredReadPlan {
        var parsed_sql = try tokenized.ParsedSql.initAlloc(self.alloc, self.sql);
        defer parsed_sql.deinit(self.alloc);
        return try self.lowerParsed(&parsed_sql);
    }

    pub fn lowerParsed(self: *@This(), parsed_sql: *const tokenized.ParsedSql) !plan.LoweredReadPlan {
        if (try self.lowerNativeGraphParsed(parsed_sql)) |lowered| return lowered;
        if (parsed_sql.generatedStatementKind() == .read) {
            const read_ast = (try generatedReadAstForParsedSql(parsed_sql)) orelse return error.UnsupportedSqlShape;
            try validateGeneratedReadPublishedKind(parsed_sql);
            return try lowerReadPlanFromGeneratedReadAstAlloc(self, parsed_sql, read_ast);
        }
        return try self.lowerParsedWithClassifier(parsed_sql);
    }

    fn lowerNativeGraphParsed(self: *@This(), parsed_sql: *const tokenized.ParsedSql) !?plan.LoweredReadPlan {
        if (parsed_sql.generatedStatementKind() != .unsupported) return null;
        const unsupported_kind = parsed_sql.unsupportedStatementKindIncludingGeneratedAst() orelse return error.UnsupportedSqlShape;
        if (unsupported_kind != .graph_query) return null;
        const unsupported = nativeGraphUnsupportedAst(parsed_sql) orelse return null;
        if (unsupported.graph_source_binding_tokens == null) return null;
        return .{ .query = try nativeGraphLoweredQueryPlanAlloc(self.alloc, parsed_sql.sql(), parsed_sql.items(), self.params, unsupported) };
    }

    fn lowerParsedWithClassifier(self: *@This(), parsed_sql: *const tokenized.ParsedSql) !plan.LoweredReadPlan {
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
        return try self.callbacks.lower_query_plan(self.alloc, self.parsed_sql.?, self.schema, self.source_schema, self.params, self.function_bindings);
    }

    fn lowerSetOperationHook(ptr: *anyopaque) anyerror!plan.LoweredSetOperationPlan {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.callbacks.lower_set_operation_optional_source_schema(self.alloc, self.parsed_sql.?, self.schema, self.source_schema, self.params, self.function_bindings);
    }
};

fn nativeGraphUnsupportedAst(parsed_sql: *const tokenized.ParsedSql) ?generated_parser.GeneratedSqlUnsupportedAst {
    const generated_statement = parsed_sql.generated_statement orelse return null;
    if (generated_statement.statement != .unsupported) return null;
    if (generated_statement.statement.unsupported != .graph_query) return null;
    const generated_ast = generated_statement.ast orelse return null;
    return switch (generated_ast) {
        .unsupported => |unsupported| if (unsupported.kind == .graph_query) unsupported else null,
        else => null,
    };
}

fn nativeGraphLoweredQueryPlanAlloc(
    alloc: std.mem.Allocator,
    source_sql: []const u8,
    tokens: []const token_mod.Token,
    params: []const value_mod.SqlValue,
    unsupported: generated_parser.GeneratedSqlUnsupportedAst,
) !plan.LoweredQueryPlan {
    const projection = unsupported.graph_return_projection_tokens orelse return error.UnsupportedSqlShape;
    const path = unsupported.graph_path_tokens orelse return error.UnsupportedSqlShape;
    const table = unsupported.graph_source_table_tokens orelse return error.UnsupportedSqlShape;
    const index = unsupported.graph_source_index_tokens orelse return error.UnsupportedSqlShape;
    const start = unsupported.graph_source_start_tokens orelse return error.UnsupportedSqlShape;
    if (projection.start >= projection.end or projection.end > tokens.len) return error.UnsupportedSqlShape;

    const alias_context = try makeNativeGraphAliasContext(tokens, unsupported);
    var alias_fields = std.ArrayListUnmanaged(NativeGraphAliasField).empty;
    defer alias_fields.deinit(alloc);

    const select = try nativeGraphSelectAlloc(alloc, tokens, unsupported.graph_return_projection_items, alias_context, &alias_fields);
    var select_transferred = false;
    errdefer if (!select_transferred) nativeGraphFreeStringSlice(alloc, select);

    const order_by = try nativeGraphOrderByAlloc(alloc, tokens, unsupported.graph_order_items, alias_context, &alias_fields);
    var order_transferred = false;
    errdefer if (!order_transferred) nativeGraphFreeOrderBy(alloc, order_by);

    const predicates = try nativeGraphWherePredicatesAlloc(alloc, tokens, params, unsupported.graph_where_expression);
    var predicates_transferred = false;
    errdefer if (!predicates_transferred) nativeGraphFreePredicates(alloc, predicates);

    const limit = try nativeGraphOptionalU32(tokens, params, unsupported.graph_limit_expression);
    const offset = (try nativeGraphOptionalU32(tokens, params, unsupported.graph_offset_expression)) orelse 0;
    const cte_name = try alloc.dupe(u8, "__antfly_native_graph");
    var cte_name_transferred = false;
    errdefer if (!cte_name_transferred) alloc.free(cte_name);
    const query_source_cte = try alloc.dupe(u8, "__antfly_native_graph");
    var query_source_cte_transferred = false;
    errdefer if (!query_source_cte_transferred) alloc.free(query_source_cte);

    const aliases = try nativeGraphReturnAliasesAlloc(alloc, tokens, unsupported);
    defer alloc.free(aliases);
    const alias_fields_text = if (alias_fields.items.len > 0)
        try nativeGraphAliasFieldsTextAlloc(alloc, alias_fields.items)
    else
        null;
    defer if (alias_fields_text) |text| alloc.free(text);

    var table_function = try query_function.lowerNativeGraphMatchTableFunctionAlloc(
        alloc,
        nativeGraphSingleTokenValue(tokens, table),
        nativeGraphSingleTokenValue(tokens, index),
        nativeGraphSingleTokenValue(tokens, start),
        nativeGraphSourceText(source_sql, tokens, path),
        aliases,
        alias_fields_text,
    );
    var table_function_transferred = false;
    errdefer if (!table_function_transferred) table_function.deinit(alloc);

    const ctes = try alloc.alloc(db_mod.types.RelationalRowsCte, 1);
    var ctes_initialized = false;
    errdefer {
        if (ctes_initialized) {
            var cte = ctes[0];
            cte.deinit(alloc);
        }
        alloc.free(ctes);
    }
    ctes[0] = .{
        .name = cte_name,
        .table_function = table_function,
    };
    ctes_initialized = true;
    cte_name_transferred = true;
    table_function_transferred = true;

    const table_name = try alloc.dupe(u8, nativeGraphSingleTokenValue(tokens, table));
    errdefer alloc.free(table_name);
    select_transferred = true;
    order_transferred = true;
    predicates_transferred = true;
    query_source_cte_transferred = true;
    return .{
        .table_name = table_name,
        .plan = .{
            .ctes = ctes,
            .query = .{
                .source_cte = query_source_cte,
                .select = select,
                .predicates = predicates,
                .order_by = order_by,
                .limit = limit,
                .offset = offset,
            },
        },
    };
}

fn nativeGraphReturnAliasesAlloc(
    alloc: std.mem.Allocator,
    tokens: []const token_mod.Token,
    unsupported: generated_parser.GeneratedSqlUnsupportedAst,
) ![]const u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    const source_alias = unsupported.graph_source_alias_tokens orelse return error.UnsupportedSqlShape;
    try out.appendSlice(alloc, nativeGraphSingleTokenValue(tokens, source_alias));
    if (unsupported.graph_target_alias_tokens) |target_alias| {
        try out.append(alloc, ',');
        try out.appendSlice(alloc, nativeGraphSingleTokenValue(tokens, target_alias));
    }
    return try out.toOwnedSlice(alloc);
}

fn nativeGraphSelectAlloc(
    alloc: std.mem.Allocator,
    tokens: []const token_mod.Token,
    items: generated_parser.GeneratedSqlListAst,
    alias_context: NativeGraphAliasContext,
    alias_fields: *std.ArrayListUnmanaged(NativeGraphAliasField),
) ![]const []const u8 {
    if (items.items.len != items.count or items.items.len == 0) return error.UnsupportedSqlShape;
    const out = try alloc.alloc([]const u8, items.items.len);
    var initialized: usize = 0;
    errdefer nativeGraphFreeStringSlice(alloc, out[0..initialized]);
    for (items.items) |item| {
        out[initialized] = try nativeGraphProjectionFieldAlloc(alloc, tokens, item, alias_context, alias_fields);
        initialized += 1;
    }
    return out;
}

fn nativeGraphProjectionFieldAlloc(
    alloc: std.mem.Allocator,
    tokens: []const token_mod.Token,
    item: generated_parser.GeneratedSqlTokenRange,
    alias_context: NativeGraphAliasContext,
    alias_fields: *std.ArrayListUnmanaged(NativeGraphAliasField),
) ![]const u8 {
    if (item.start >= item.end or item.end > tokens.len) return error.UnsupportedSqlShape;
    const output = try nativeGraphFieldOutputAlloc(alloc, alias_context, tokens[item.start], alias_fields);
    var output_transferred = false;
    errdefer if (!output_transferred) alloc.free(output);
    var pos = item.start + 1;
    if (pos < item.end) {
        if (!tokens[pos].matchesKeywordTag(.as)) return error.UnsupportedSqlShape;
        pos += 1;
        if (pos >= item.end or tokens[pos].kind != .identifier) return error.UnsupportedSqlShape;
        pos += 1;
    }
    if (pos != item.end) return error.UnsupportedSqlShape;
    output_transferred = true;
    return output;
}

fn nativeGraphOrderByAlloc(
    alloc: std.mem.Allocator,
    tokens: []const token_mod.Token,
    items: generated_parser.GeneratedSqlListAst,
    alias_context: NativeGraphAliasContext,
    alias_fields: *std.ArrayListUnmanaged(NativeGraphAliasField),
) ![]const db_mod.types.RelationalRowsQueryOrder {
    if (items.items.len != items.count) return error.UnsupportedSqlShape;
    if (items.items.len == 0) return &.{};
    const out = try alloc.alloc(db_mod.types.RelationalRowsQueryOrder, items.items.len);
    var initialized: usize = 0;
    errdefer nativeGraphFreeOrderBy(alloc, out[0..initialized]);
    for (items.items) |item| {
        out[initialized] = try nativeGraphOrderItemAlloc(alloc, tokens, item, alias_context, alias_fields);
        initialized += 1;
    }
    return out;
}

fn nativeGraphOrderItemAlloc(
    alloc: std.mem.Allocator,
    tokens: []const token_mod.Token,
    item: generated_parser.GeneratedSqlTokenRange,
    alias_context: NativeGraphAliasContext,
    alias_fields: *std.ArrayListUnmanaged(NativeGraphAliasField),
) !db_mod.types.RelationalRowsQueryOrder {
    if (item.start >= item.end or item.end > tokens.len) return error.UnsupportedSqlShape;
    const field = try nativeGraphFieldOutputAlloc(alloc, alias_context, tokens[item.start], alias_fields);
    var field_transferred = false;
    errdefer if (!field_transferred) alloc.free(field);
    var pos = item.start + 1;
    const direction: db_mod.types.RelationalRowsQueryOrderDirection = if (pos < item.end and tokens[pos].matchesKeywordTag(.desc)) blk: {
        pos += 1;
        break :blk .desc;
    } else if (pos < item.end and tokens[pos].matchesKeywordTag(.asc)) blk: {
        pos += 1;
        break :blk .asc;
    } else .asc;
    const null_test: ?db_mod.types.RelationalRowsQueryOrderNullTest = if (pos < item.end and tokens[pos].matchesKeywordTag(.nulls)) blk: {
        pos += 1;
        if (pos >= item.end) return error.UnsupportedSqlShape;
        if (tokens[pos].matchesKeywordTag(.first)) {
            pos += 1;
            break :blk .is_null;
        }
        if (tokens[pos].matchesKeywordTag(.last)) {
            pos += 1;
            break :blk .is_not_null;
        }
        return error.UnsupportedSqlShape;
    } else null;
    if (pos != item.end) return error.UnsupportedSqlShape;
    field_transferred = true;
    return .{ .field = field, .direction = direction, .null_test = null_test };
}

fn nativeGraphWherePredicatesAlloc(
    alloc: std.mem.Allocator,
    tokens: []const token_mod.Token,
    params: []const value_mod.SqlValue,
    expression: generated_parser.GeneratedSqlExpressionAst,
) ![]const runtime_schema.RelationalCheck {
    if (expression.tokens == null) return &.{};
    if (expression.kind != .comparison and expression.kind != .is_null and expression.kind != .is_not_null) return error.UnsupportedSqlShape;
    const out = try alloc.alloc(runtime_schema.RelationalCheck, 1);
    errdefer alloc.free(out);
    out[0] = switch (expression.kind) {
        .comparison => try nativeGraphComparisonPredicateAlloc(alloc, tokens, params, expression),
        .is_null, .is_not_null => try nativeGraphNullPredicateAlloc(alloc, tokens, expression),
        else => unreachable,
    };
    return out;
}

fn nativeGraphComparisonPredicateAlloc(
    alloc: std.mem.Allocator,
    tokens: []const token_mod.Token,
    params: []const value_mod.SqlValue,
    expression: generated_parser.GeneratedSqlExpressionAst,
) !runtime_schema.RelationalCheck {
    const left = expression.left_tokens orelse return error.UnsupportedSqlShape;
    const op = expression.operator_tokens orelse return error.UnsupportedSqlShape;
    const right = expression.right_tokens orelse return error.UnsupportedSqlShape;
    if (left.end != left.start + 1 or op.end != op.start + 1 or right.end != right.start + 1) return error.UnsupportedSqlShape;
    if (left.end > tokens.len or op.end > tokens.len or right.end > tokens.len) return error.UnsupportedSqlShape;
    const field = try nativeGraphPlainFieldAlloc(alloc, tokens[left.start]);
    var field_transferred = false;
    errdefer if (!field_transferred) alloc.free(field);
    const value_json = try nativeGraphValueJsonAlloc(alloc, tokens[right.start], params);
    var value_transferred = false;
    errdefer if (!value_transferred) alloc.free(value_json);
    const check_op = nativeGraphComparisonOp(tokens[op.start]) orelse return error.UnsupportedSqlShape;
    field_transferred = true;
    value_transferred = true;
    return .{ .name = "", .field = field, .op = check_op, .value_json = value_json };
}

fn nativeGraphNullPredicateAlloc(
    alloc: std.mem.Allocator,
    tokens: []const token_mod.Token,
    expression: generated_parser.GeneratedSqlExpressionAst,
) !runtime_schema.RelationalCheck {
    const left = expression.left_tokens orelse return error.UnsupportedSqlShape;
    if (left.end != left.start + 1 or left.end > tokens.len) return error.UnsupportedSqlShape;
    const field = try nativeGraphPlainFieldAlloc(alloc, tokens[left.start]);
    return .{
        .name = "",
        .field = field,
        .op = if (expression.kind == .is_null) .is_null else .is_not_null,
    };
}

fn nativeGraphComparisonOp(token: token_mod.Token) ?runtime_schema.RelationalCheckOp {
    return switch (token.kind) {
        .eq => .eq,
        .neq => .ne,
        .gt => .gt,
        .gte => .gte,
        .lt => .lt,
        .lte => .lte,
        else => null,
    };
}

fn nativeGraphValueJsonAlloc(
    alloc: std.mem.Allocator,
    token: token_mod.Token,
    params: []const value_mod.SqlValue,
) ![]const u8 {
    return switch (token.kind) {
        .string => try std.json.Stringify.valueAlloc(alloc, token.text, .{}),
        .number => try alloc.dupe(u8, token.text),
        .placeholder => try value_mod.boundSqlValueJsonAlloc(alloc, token, params),
        .identifier => if (token.matchesKeywordTag(.true))
            try alloc.dupe(u8, "true")
        else if (token.matchesKeywordTag(.false))
            try alloc.dupe(u8, "false")
        else if (token.matchesKeywordTag(.null))
            try alloc.dupe(u8, "null")
        else
            error.UnsupportedSqlShape,
        else => error.UnsupportedSqlShape,
    };
}

fn nativeGraphOptionalU32(
    tokens: []const token_mod.Token,
    params: []const value_mod.SqlValue,
    expression: generated_parser.GeneratedSqlExpressionAst,
) !?u32 {
    const range = expression.tokens orelse return null;
    if (range.end != range.start + 1 or range.end > tokens.len) return error.UnsupportedSqlShape;
    const token = tokens[range.start];
    return switch (token.kind) {
        .number => try std.fmt.parseUnsigned(u32, token.text, 10),
        .placeholder => try (try value_mod.boundSqlValue(token, params)).asU32(),
        else => error.UnsupportedSqlShape,
    };
}

fn nativeGraphFieldOutputAlloc(
    alloc: std.mem.Allocator,
    alias_context: NativeGraphAliasContext,
    token: token_mod.Token,
    alias_fields: *std.ArrayListUnmanaged(NativeGraphAliasField),
) ![]const u8 {
    if (token.kind != .identifier) return error.UnsupportedSqlShape;
    if (try nativeGraphAliasFieldForToken(alias_context, token)) |alias_field| {
        try nativeGraphRecordAliasField(alloc, alias_fields, alias_field);
        return try std.fmt.allocPrint(alloc, "{s}_{s}", .{ alias_field.alias, alias_field.field });
    }
    if (!nativeGraphIdentifierIsValid(token.text)) return error.UnsupportedSqlShape;
    return try alloc.dupe(u8, token.text);
}

fn nativeGraphPlainFieldAlloc(alloc: std.mem.Allocator, token: token_mod.Token) ![]const u8 {
    if (token.kind != .identifier or !nativeGraphIdentifierIsValid(token.text)) return error.UnsupportedSqlShape;
    return try alloc.dupe(u8, token.text);
}

fn nativeGraphAliasFieldsTextAlloc(
    alloc: std.mem.Allocator,
    alias_fields: []const NativeGraphAliasField,
) ![]const u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    for (alias_fields, 0..) |alias_field, index| {
        if (index > 0) try out.append(alloc, ',');
        try out.appendSlice(alloc, alias_field.alias);
        try out.append(alloc, '.');
        try out.appendSlice(alloc, alias_field.field);
    }
    return try out.toOwnedSlice(alloc);
}

fn nativeGraphFreeStringSlice(alloc: std.mem.Allocator, values: []const []const u8) void {
    for (values) |value| alloc.free(@constCast(value));
    if (values.len > 0) alloc.free(@constCast(values));
}

fn nativeGraphFreeOrderBy(alloc: std.mem.Allocator, order_by: []const db_mod.types.RelationalRowsQueryOrder) void {
    for (order_by) |order| {
        if (order.field.len > 0) alloc.free(@constCast(order.field));
        if (order.collation) |collation| alloc.free(@constCast(collation));
    }
    if (order_by.len > 0) alloc.free(@constCast(order_by));
}

fn nativeGraphFreePredicates(alloc: std.mem.Allocator, predicates: []const runtime_schema.RelationalCheck) void {
    for (predicates) |predicate| {
        if (predicate.field.len > 0) alloc.free(@constCast(predicate.field));
        if (predicate.value_json) |value_json| alloc.free(@constCast(value_json));
        if (predicate.collation) |collation| alloc.free(@constCast(collation));
    }
    if (predicates.len > 0) alloc.free(@constCast(predicates));
}

const NativeGraphAliasContext = struct {
    source_alias: []const u8,
    target_alias: ?[]const u8,

    fn contains(self: @This(), alias: []const u8) bool {
        if (std.mem.eql(u8, self.source_alias, alias)) return true;
        if (self.target_alias) |target| return std.mem.eql(u8, target, alias);
        return false;
    }
};

const NativeGraphAliasField = struct {
    alias: []const u8,
    field: []const u8,
};

fn makeNativeGraphAliasContext(
    tokens: []const token_mod.Token,
    unsupported: generated_parser.GeneratedSqlUnsupportedAst,
) !NativeGraphAliasContext {
    const source_alias_range = unsupported.graph_source_alias_tokens orelse return error.UnsupportedSqlShape;
    const source_alias = nativeGraphSingleTokenValue(tokens, source_alias_range);
    if (!nativeGraphIdentifierIsValid(source_alias)) return error.UnsupportedSqlShape;
    const target_alias = if (unsupported.graph_target_alias_tokens) |target_alias_range| blk: {
        const alias = nativeGraphSingleTokenValue(tokens, target_alias_range);
        if (!nativeGraphIdentifierIsValid(alias)) return error.UnsupportedSqlShape;
        if (std.mem.eql(u8, source_alias, alias)) return error.UnsupportedSqlShape;
        break :blk alias;
    } else null;
    return .{
        .source_alias = source_alias,
        .target_alias = target_alias,
    };
}

fn nativeGraphAliasFieldForToken(
    alias_context: NativeGraphAliasContext,
    token: token_mod.Token,
) !?NativeGraphAliasField {
    if (token.kind != .identifier) return null;
    const dot = std.mem.indexOfScalar(u8, token.text, '.') orelse return null;
    if (std.mem.indexOfScalar(u8, token.text[dot + 1 ..], '.') != null) return error.UnsupportedSqlShape;
    const alias = token.text[0..dot];
    const field = token.text[dot + 1 ..];
    if (!alias_context.contains(alias)) return null;
    if (!nativeGraphAliasFieldIsValid(field)) return error.UnsupportedSqlShape;
    return .{ .alias = alias, .field = field };
}

fn nativeGraphRecordAliasField(
    alloc: std.mem.Allocator,
    alias_fields: *std.ArrayListUnmanaged(NativeGraphAliasField),
    alias_field: NativeGraphAliasField,
) !void {
    for (alias_fields.items) |existing| {
        if (std.mem.eql(u8, existing.alias, alias_field.alias) and
            std.mem.eql(u8, existing.field, alias_field.field))
        {
            return;
        }
    }
    try alias_fields.append(alloc, alias_field);
}

fn nativeGraphIdentifierIsValid(value: []const u8) bool {
    if (value.len == 0) return false;
    for (value, 0..) |ch, index| {
        if (index == 0) {
            if (!(std.ascii.isAlphabetic(ch) or ch == '_')) return false;
        } else if (!(std.ascii.isAlphanumeric(ch) or ch == '_')) {
            return false;
        }
    }
    return true;
}

fn nativeGraphAliasFieldIsValid(value: []const u8) bool {
    return std.mem.eql(u8, value, "key") or
        std.mem.eql(u8, value, "depth") or
        std.mem.eql(u8, value, "distance");
}

fn nativeGraphSourceText(
    source_sql: []const u8,
    tokens: []const token_mod.Token,
    range: generated_parser.GeneratedSqlTokenRange,
) []const u8 {
    return source_sql[tokens[range.start].source_start..tokens[range.end - 1].source_end];
}

fn nativeGraphSingleTokenValue(tokens: []const token_mod.Token, range: generated_parser.GeneratedSqlTokenRange) []const u8 {
    if (range.end != range.start + 1 or range.end > tokens.len) return "";
    return tokens[range.start].text;
}

fn generatedReadAstForParsedSql(parsed_sql: *const tokenized.ParsedSql) !?*const generated_parser.GeneratedSqlReadAst {
    if (parsed_sql.generatedStatementKind() != .read) return null;
    _ = parsed_sql.readStatementKindIncludingGeneratedAst() orelse return error.UnsupportedSqlShape;
    if (parsed_sql.generated_statement) |*generated_statement| {
        if (generated_statement.ast) |*generated_ast| {
            return switch (generated_ast.*) {
                .read => |read| blk: {
                    try validateGeneratedReadAstForStatement(parsed_sql.items(), read);
                    break :blk read;
                },
                else => error.UnsupportedSqlShape,
            };
        }
    }
    return error.UnsupportedSqlShape;
}

fn validateGeneratedReadPublishedKind(parsed_sql: *const tokenized.ParsedSql) !void {
    const parsed_kind = parsed_sql.readStatementKind() orelse return error.UnsupportedSqlShape;
    const generated_kind = parsed_sql.generatedReadStatementKind() orelse return error.UnsupportedSqlShape;
    if (parsed_kind != generated_kind) return error.UnsupportedSqlShape;
}

pub fn lowerReadPlanFromGeneratedReadAstAlloc(
    context: *ReadPlanLoweringContext,
    parsed_sql: *const tokenized.ParsedSql,
    read_ast: *const generated_parser.GeneratedSqlReadAst,
) !plan.LoweredReadPlan {
    try validateGeneratedReadPublishedKind(parsed_sql);
    const read_kind = try generatedReadStatementKind(parsed_sql.items(), read_ast);
    const published_kind = parsed_sql.readStatementKindIncludingGeneratedAst() orelse return error.UnsupportedSqlShape;
    if (read_kind != published_kind) return error.UnsupportedSqlShape;
    try validateGeneratedReadAstForStatement(parsed_sql.items(), read_ast);
    return switch (read_ast.kind) {
        .query => .{ .query = try context.callbacks.lower_query_plan(
            context.alloc,
            parsed_sql,
            context.schema,
            context.source_schema,
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
        .set_operation => blk: {
            if (try lowerGeneratedSetOperationQueryPlanAlloc(context, parsed_sql, read_ast)) |lowered| {
                break :blk .{ .query = lowered };
            }
            break :blk .{ .set_operation = try context.callbacks.lower_set_operation_optional_source_schema(
                context.alloc,
                parsed_sql,
                context.schema,
                context.source_schema,
                context.params,
                context.function_bindings,
            ) };
        },
        .cte => try lowerGeneratedCteReadPlanAlloc(context, parsed_sql, read_kind),
    };
}

fn lowerGeneratedSetOperationQueryPlanAlloc(
    context: *ReadPlanLoweringContext,
    parsed_sql: *const tokenized.ParsedSql,
    read_ast: *const generated_parser.GeneratedSqlReadAst,
) !?plan.LoweredQueryPlan {
    if (!generatedSetOperationAllowsQueryPlanLowering(read_ast)) return null;
    const tokens = parsed_sql.items();
    var parser_state = parser_context.ParserState{
        .alloc = context.alloc,
        .tokens = tokens,
        .schema = context.schema,
        .params = context.params,
        .function_bindings = context.function_bindings,
        .generated_read_ast = null,
    };
    return lower_expr.lowerTokenizedQueryPlanAlloc(
        context.alloc,
        tokens,
        &parser_state.pos,
        context.params,
        null,
        parser_context.ParserState.ContextAccessors.cteSelectParserHooks(&parser_state),
        parser_context.ParserState.ContextAccessors.queryPlanParserHooks(&parser_state),
        parser_context.ParserState.ContextAccessors.simpleSelectSetTailHooks(&parser_state),
    ) catch |err| switch (err) {
        error.UnsupportedSqlShape => null,
        error.InvalidSqlCatalog => null,
        else => return err,
    };
}

fn generatedSetOperationAllowsQueryPlanLowering(read_ast: *const generated_parser.GeneratedSqlReadAst) bool {
    const set_operation_tokens = read_ast.set_operation.tokens orelse return false;
    const right_query_tokens = read_ast.set_operation.right_query_tokens orelse return false;
    return right_query_tokens.end < set_operation_tokens.end;
}

pub fn validateGeneratedReadAstForStatement(
    tokens: []const tokenized.Token,
    read_ast: *const generated_parser.GeneratedSqlReadAst,
) !void {
    _ = try generatedReadStatementKind(tokens, read_ast);
    try generated_read_validate.validateGeneratedReadAstPayloads(tokens, read_ast.*);
    try validateGeneratedReadAstRanges(tokens, read_ast);
    switch (read_ast.kind) {
        .query => try validateGeneratedSimpleQueryReadAst(tokens, read_ast),
        .aggregate => try validateGeneratedAggregateReadAst(tokens, read_ast),
        .join => try validateGeneratedJoinedReadAst(tokens, read_ast, .join),
        .lateral => try validateGeneratedJoinedReadAst(tokens, read_ast, .lateral),
        .window => try validateGeneratedWindowReadAst(tokens, read_ast),
        .set_operation => try validateGeneratedSetOperationReadAst(tokens, read_ast),
        .cte => try validateGeneratedCteReadAst(tokens, read_ast),
    }
}

fn generatedReadStatementKind(
    tokens: []const tokenized.Token,
    read_ast: *const generated_parser.GeneratedSqlReadAst,
) !sql_statement_kind.SqlReadStatementKind {
    return switch (read_ast.kind) {
        .query => .query,
        .aggregate => .aggregate,
        .join => .join,
        .lateral => .lateral,
        .window => .window,
        .set_operation => .set_operation,
        .cte => {
            if (read_ast.cte_recursive) return .recursive_cte;
            _ = tokens;
            return try generatedCteFinalReadStatementKind(read_ast);
        },
    };
}

fn generatedCteFinalReadStatementKind(
    read_ast: *const generated_parser.GeneratedSqlReadAst,
) !sql_statement_kind.SqlReadStatementKind {
    if (read_ast.projection_tokens == null or read_ast.source_tokens == null) return error.UnsupportedSqlShape;
    return generatedReadStatementKindFromGeneratedReadKind(read_ast.cte_final_kind orelse return error.UnsupportedSqlShape) orelse error.UnsupportedSqlShape;
}

fn generatedReadStatementKindFromGeneratedReadKind(
    kind: generated_parser.GeneratedSqlReadKind,
) ?sql_statement_kind.SqlReadStatementKind {
    return switch (kind) {
        .query => .query,
        .aggregate => .aggregate,
        .join => .join,
        .lateral => .lateral,
        .window => .window,
        .set_operation => .set_operation,
        .cte => null,
    };
}

fn generatedReadStatementKindFromStructuredClauses(
    tokens: []const tokenized.Token,
    read_ast: *const generated_parser.GeneratedSqlReadAst,
) sql_statement_kind.SqlReadStatementKind {
    if (read_ast.set_operation_tokens != null) return .set_operation;
    if (read_ast.source_tokens) |source| {
        if (generatedReadRangeContainsKeyword(tokens, source, .lateral) and read_ast.join_items.len != 0) return .lateral;
    }
    if (read_ast.projection_tokens) |projection| {
        if (generatedReadRangeContainsKeyword(tokens, projection, .over)) return .window;
    }
    const aggregate_projection = if (read_ast.projection_tokens) |projection|
        generatedReadRangeHasAggregateFunction(tokens, projection)
    else
        false;
    if ((read_ast.distinct_tokens != null and read_ast.distinct_on_items.count == 0) or
        read_ast.group_tokens != null or
        read_ast.having_tokens != null or
        aggregate_projection)
    {
        return .aggregate;
    }
    if (read_ast.source_tokens) |source| {
        if (generatedReadRangeContainsKeyword(tokens, source, .join)) return .join;
    }
    return .query;
}

fn validateGeneratedReadAstRanges(tokens: []const tokenized.Token, read_ast: *const generated_parser.GeneratedSqlReadAst) !void {
    try validateGeneratedReadStatementSpans(tokens, read_ast);
    const ranges = [_]?generated_parser.GeneratedSqlTokenRange{
        read_ast.cte_tokens,
        read_ast.cte_name_tokens,
        read_ast.cte_body_tokens,
        read_ast.distinct_tokens,
        read_ast.projection_tokens,
        read_ast.source_tokens,
        read_ast.source_system_time_tokens,
        read_ast.source_system_time_sequence_tokens,
        read_ast.source_graph_function_tokens,
        read_ast.source_graph_function_name_tokens,
        read_ast.source_graph_function_argument_tokens,
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
        read_ast.fetch_count_tokens,
        read_ast.row_lock_tokens,
        read_ast.set_operation_tokens,
    };
    for (ranges) |range| {
        if (range) |value| try validateGeneratedReadTokenRange(tokens, read_ast, value);
    }
    try validateGeneratedReadClauseMetadata(tokens, read_ast);
    try validateGeneratedDistinctOnListAstRanges(tokens, read_ast, read_ast.distinct_tokens, read_ast.distinct_on_items);
    try validateGeneratedReadListAstRanges(tokens, read_ast, read_ast.projection_items);
    try validateGeneratedReadListAstContainedByOptionalRange(read_ast.projection_items, read_ast.projection_tokens);
    if (read_ast.projection_tokens) |projection_tokens| try validateGeneratedReadCommaDelimitedList(tokens, projection_tokens, read_ast.projection_items);
    try validateGeneratedReadListAstBoundaryExpressions(tokens, read_ast, read_ast.projection_items, &read_ast.projection_first_expression, &read_ast.projection_last_expression);
    try validateGeneratedAntflySourceMetadata(tokens, read_ast);
    try validateGeneratedGraphSourceMetadata(tokens, read_ast);
    try validateGeneratedReadListAstRanges(tokens, read_ast, read_ast.group_items);
    try validateGeneratedReadListAstContainedByOptionalRange(read_ast.group_items, read_ast.group_tokens);
    if (read_ast.group_tokens) |group_tokens| try validateGeneratedReadCommaDelimitedList(tokens, group_tokens, read_ast.group_items);
    try validateGeneratedReadListAstBoundaryExpressions(tokens, read_ast, read_ast.group_items, &read_ast.group_first_expression, &read_ast.group_last_expression);
    try validateGeneratedReadListAstRanges(tokens, read_ast, read_ast.order_items);
    try validateGeneratedReadListAstContainedByOptionalRange(read_ast.order_items, read_ast.order_tokens);
    if (read_ast.order_tokens) |order_tokens| try validateGeneratedReadCommaDelimitedList(tokens, order_tokens, read_ast.order_items);
    try validateGeneratedReadListAstBoundaryExpressions(tokens, read_ast, read_ast.order_items, &read_ast.order_first_expression, &read_ast.order_last_expression);
    try validateGeneratedExpressionAstRangesIfPresent(tokens, read_ast, read_ast.where_expression);
    try validateGeneratedExpressionAstRangesIfPresent(tokens, read_ast, read_ast.having_expression);
    try validateGeneratedReadPaginationPayloads(
        tokens,
        read_ast,
        read_ast.limit_tokens,
        &read_ast.limit_expression,
        read_ast.limit_all,
        read_ast.offset_tokens,
        &read_ast.offset_expression,
        read_ast.fetch_tokens,
        read_ast.fetch_count_tokens,
        &read_ast.fetch_count_expression,
    );
    try validateGeneratedWindowAstListRanges(tokens, read_ast, read_ast.window_tokens, read_ast.window_items, read_ast.window_count);
    try validateGeneratedSetOperationAstRanges(tokens, read_ast, read_ast.set_operation_tokens, read_ast.set_operation);
    for (read_ast.cte_items) |cte| {
        try validateGeneratedReadTokenRange(tokens, read_ast, cte.name_tokens);
        if (cte.column_tokens) |column_tokens| try validateGeneratedReadTokenRange(tokens, read_ast, column_tokens);
        if (cte.column_name_tokens) |column_name_tokens| try validateGeneratedReadTokenRange(tokens, read_ast, column_name_tokens);
        if (cte.materialization_tokens) |materialization_tokens| try validateGeneratedReadTokenRange(tokens, read_ast, materialization_tokens);
        if (cte.body_tokens) |body_tokens| try validateGeneratedReadTokenRange(tokens, read_ast, body_tokens);
        if (cte.body_select_tokens) |body_select_tokens| try validateGeneratedReadTokenRange(tokens, read_ast, body_select_tokens);
        if (cte.body_distinct_tokens) |body_distinct_tokens| try validateGeneratedReadTokenRange(tokens, read_ast, body_distinct_tokens);
        if (cte.body_projection_tokens) |body_projection_tokens| try validateGeneratedReadTokenRange(tokens, read_ast, body_projection_tokens);
        if (cte.body_source_tokens) |body_source_tokens| try validateGeneratedReadTokenRange(tokens, read_ast, body_source_tokens);
        if (cte.body_source_system_time_tokens) |body_source_system_time_tokens| try validateGeneratedReadTokenRange(tokens, read_ast, body_source_system_time_tokens);
        if (cte.body_source_system_time_sequence_tokens) |body_source_system_time_sequence_tokens| try validateGeneratedReadTokenRange(tokens, read_ast, body_source_system_time_sequence_tokens);
        if (cte.body_join_tokens) |body_join_tokens| try validateGeneratedReadTokenRange(tokens, read_ast, body_join_tokens);
        if (cte.body_join_operator_tokens) |body_join_operator_tokens| try validateGeneratedReadTokenRange(tokens, read_ast, body_join_operator_tokens);
        if (cte.body_join_left_tokens) |body_join_left_tokens| try validateGeneratedReadTokenRange(tokens, read_ast, body_join_left_tokens);
        if (cte.body_join_right_tokens) |body_join_right_tokens| try validateGeneratedReadTokenRange(tokens, read_ast, body_join_right_tokens);
        if (cte.body_join_predicate_tokens) |body_join_predicate_tokens| try validateGeneratedReadTokenRange(tokens, read_ast, body_join_predicate_tokens);
        if (cte.body_where_tokens) |body_where_tokens| try validateGeneratedReadTokenRange(tokens, read_ast, body_where_tokens);
        if (cte.body_group_tokens) |body_group_tokens| try validateGeneratedReadTokenRange(tokens, read_ast, body_group_tokens);
        if (cte.body_having_tokens) |body_having_tokens| try validateGeneratedReadTokenRange(tokens, read_ast, body_having_tokens);
        if (cte.body_window_tokens) |body_window_tokens| try validateGeneratedReadTokenRange(tokens, read_ast, body_window_tokens);
        if (cte.body_order_tokens) |body_order_tokens| try validateGeneratedReadTokenRange(tokens, read_ast, body_order_tokens);
        if (cte.body_limit_tokens) |body_limit_tokens| try validateGeneratedReadTokenRange(tokens, read_ast, body_limit_tokens);
        if (cte.body_offset_tokens) |body_offset_tokens| try validateGeneratedReadTokenRange(tokens, read_ast, body_offset_tokens);
        if (cte.body_fetch_tokens) |body_fetch_tokens| try validateGeneratedReadTokenRange(tokens, read_ast, body_fetch_tokens);
        if (cte.body_fetch_count_tokens) |body_fetch_count_tokens| try validateGeneratedReadTokenRange(tokens, read_ast, body_fetch_count_tokens);
        if (cte.body_row_lock_tokens) |body_row_lock_tokens| try validateGeneratedReadTokenRange(tokens, read_ast, body_row_lock_tokens);
        if (cte.body_set_operation_tokens) |body_set_operation_tokens| try validateGeneratedReadTokenRange(tokens, read_ast, body_set_operation_tokens);
        try validateGeneratedAntflySourceItemsMetadata(tokens, cte.body_source_tokens, cte.body_source_antfly_function_items, cte.body_source_antfly_function_count);
        try validateGeneratedGraphSourceItemsMetadata(tokens, cte.body_source_tokens, cte.body_source_antfly_function_items, cte.body_source_graph_function_items, cte.body_source_graph_function_count);
        try validateGeneratedReadListAstRanges(tokens, read_ast, cte.column_names);
        try validateGeneratedReadListAstContainedByOptionalRange(cte.column_names, cte.column_name_tokens);
        if (cte.column_name_tokens) |column_name_tokens| try validateGeneratedReadCommaDelimitedList(tokens, column_name_tokens, cte.column_names);
        try validateGeneratedDistinctOnListAstRanges(tokens, read_ast, cte.body_distinct_tokens, cte.body_distinct_on_items);
        try validateGeneratedReadListAstRanges(tokens, read_ast, cte.body_projection_items);
        try validateGeneratedReadListAstContainedByOptionalRange(cte.body_projection_items, cte.body_projection_tokens);
        if (cte.body_projection_tokens) |projection_tokens| try validateGeneratedReadCommaDelimitedList(tokens, projection_tokens, cte.body_projection_items);
        try validateGeneratedReadListAstBoundaryExpressions(tokens, read_ast, cte.body_projection_items, &cte.body_projection_first_expression, &cte.body_projection_last_expression);
        try validateGeneratedExpressionAstRangesIfPresent(tokens, read_ast, cte.body_join_predicate_expression);
        try validateGeneratedExpressionAstRangesIfPresent(tokens, read_ast, cte.body_where_expression);
        try validateGeneratedReadListAstRanges(tokens, read_ast, cte.body_group_items);
        try validateGeneratedReadListAstContainedByOptionalRange(cte.body_group_items, cte.body_group_tokens);
        if (cte.body_group_tokens) |group_tokens| try validateGeneratedReadCommaDelimitedList(tokens, group_tokens, cte.body_group_items);
        try validateGeneratedReadListAstBoundaryExpressions(tokens, read_ast, cte.body_group_items, &cte.body_group_first_expression, &cte.body_group_last_expression);
        try validateGeneratedExpressionAstRangesIfPresent(tokens, read_ast, cte.body_having_expression);
        try validateGeneratedWindowAstListRanges(tokens, read_ast, cte.body_window_tokens, cte.body_window_items, cte.body_window_count);
        try validateGeneratedReadListAstRanges(tokens, read_ast, cte.body_order_items);
        try validateGeneratedReadListAstContainedByOptionalRange(cte.body_order_items, cte.body_order_tokens);
        if (cte.body_order_tokens) |order_tokens| try validateGeneratedReadCommaDelimitedList(tokens, order_tokens, cte.body_order_items);
        try validateGeneratedReadListAstBoundaryExpressions(tokens, read_ast, cte.body_order_items, &cte.body_order_first_expression, &cte.body_order_last_expression);
        try validateGeneratedReadPaginationPayloads(
            tokens,
            read_ast,
            cte.body_limit_tokens,
            &cte.body_limit_expression,
            cte.body_limit_all,
            cte.body_offset_tokens,
            &cte.body_offset_expression,
            cte.body_fetch_tokens,
            cte.body_fetch_count_tokens,
            &cte.body_fetch_count_expression,
        );
        try validateGeneratedJoinAstRanges(tokens, read_ast, cte.body_join_items);
        try validateGeneratedSetOperationAstRanges(tokens, read_ast, cte.body_set_operation_tokens, cte.body_set_operation);
    }
    try validateGeneratedCteListMetadata(tokens, read_ast);
    for (read_ast.join_items) |join| {
        try validateGeneratedReadTokenRange(tokens, read_ast, join.tokens);
        try validateGeneratedReadTokenRange(tokens, read_ast, join.operator_tokens);
        try validateGeneratedReadTokenRange(tokens, read_ast, join.left_tokens);
        try validateGeneratedReadTokenRange(tokens, read_ast, join.right_tokens);
        if (join.predicate_tokens) |predicate_tokens| try validateGeneratedReadTokenRange(tokens, read_ast, predicate_tokens);
        if (join.using_tokens) |using_tokens| try validateGeneratedReadTokenRange(tokens, read_ast, using_tokens);
        if (join.using_column_tokens) |using_column_tokens| try validateGeneratedReadTokenRange(tokens, read_ast, using_column_tokens);
        try validateGeneratedExpressionAstRangesIfPresent(tokens, read_ast, join.predicate_expression);
        try validateGeneratedReadListAstRanges(tokens, read_ast, join.using_columns);
        try validateGeneratedReadListAstContainedByOptionalRange(join.using_columns, join.using_column_tokens);
        if (join.using_column_tokens) |using_column_tokens| try validateGeneratedReadCommaDelimitedList(tokens, using_column_tokens, join.using_columns);
    }
    try validateGeneratedJoinAstRanges(tokens, read_ast, read_ast.join_items);
    try validateGeneratedJoinTreeMetadata(tokens, read_ast);

    switch (read_ast.kind) {
        .query => {
            if (read_ast.projection_tokens == null) return error.UnsupportedSqlShape;
        },
        .aggregate => {
            if (read_ast.projection_tokens == null) return error.UnsupportedSqlShape;
            const aggregate_projection = if (read_ast.projection_tokens) |projection|
                generatedReadRangeHasAggregateFunction(tokens, projection)
            else
                false;
            if (read_ast.group_tokens == null and read_ast.having_tokens == null and read_ast.distinct_tokens == null and !aggregate_projection) {
                return error.UnsupportedSqlShape;
            }
        },
        .join => {
            if (read_ast.projection_tokens == null or read_ast.source_tokens == null) return error.UnsupportedSqlShape;
            try validateGeneratedReadRangeContainsKeyword(tokens, read_ast.source_tokens.?, .join);
        },
        .lateral => {
            if (read_ast.projection_tokens == null or read_ast.source_tokens == null) return error.UnsupportedSqlShape;
            if (read_ast.join_items.len == 0 or read_ast.join_tokens == null) return error.UnsupportedSqlShape;
            try validateGeneratedReadRangeContainsKeyword(tokens, read_ast.source_tokens.?, .join);
            try validateGeneratedReadRangeContainsKeyword(tokens, read_ast.source_tokens.?, .lateral);
        },
        .window => {
            if (read_ast.projection_tokens == null or read_ast.source_tokens == null) return error.UnsupportedSqlShape;
            try validateGeneratedReadRangeContainsKeyword(tokens, read_ast.projection_tokens.?, .over);
        },
        .set_operation => {
            if (read_ast.projection_tokens == null or read_ast.source_tokens == null or read_ast.set_operation_tokens == null) return error.UnsupportedSqlShape;
        },
        .cte => {
            if (read_ast.cte_tokens == null or read_ast.projection_tokens == null) return error.UnsupportedSqlShape;
        },
    }
}

fn validateGeneratedCteListMetadata(tokens: []const tokenized.Token, read_ast: *const generated_parser.GeneratedSqlReadAst) !void {
    if (read_ast.cte_items.len == 0) {
        if (read_ast.cte_count != 0 or
            read_ast.cte_tokens != null or
            read_ast.cte_list_tokens != null or
            read_ast.cte_name_tokens != null or
            read_ast.cte_body_tokens != null or
            read_ast.cte_last_name_tokens != null or
            read_ast.cte_last_body_tokens != null or
            read_ast.cte_final_kind != null)
        {
            return error.UnsupportedSqlShape;
        }
        return;
    }

    const cte_tokens = read_ast.cte_tokens orelse return error.UnsupportedSqlShape;
    const cte_list_tokens = read_ast.cte_list_tokens orelse return error.UnsupportedSqlShape;
    const cte_final_kind = read_ast.cte_final_kind orelse return error.UnsupportedSqlShape;
    if (cte_final_kind == .cte) return error.UnsupportedSqlShape;
    if (read_ast.cte_count != read_ast.cte_items.len) return error.UnsupportedSqlShape;
    try validateGeneratedCtePrefixLayout(tokens, cte_tokens, cte_list_tokens, read_ast.cte_recursive);
    const first = read_ast.cte_items[0];
    const last = read_ast.cte_items[read_ast.cte_items.len - 1];
    if (!std.meta.eql(read_ast.cte_name_tokens orelse return error.UnsupportedSqlShape, first.name_tokens)) return error.UnsupportedSqlShape;
    if (!optionalGeneratedTokenRangeEql(read_ast.cte_body_tokens, first.body_tokens)) return error.UnsupportedSqlShape;
    if (!std.meta.eql(read_ast.cte_last_name_tokens orelse return error.UnsupportedSqlShape, last.name_tokens)) return error.UnsupportedSqlShape;
    if (!optionalGeneratedTokenRangeEql(read_ast.cte_last_body_tokens, last.body_tokens)) return error.UnsupportedSqlShape;
    const derived_final_kind = generatedReadStatementKindFromStructuredClauses(tokens, read_ast);
    if ((generatedReadStatementKindFromGeneratedReadKind(cte_final_kind) orelse return error.UnsupportedSqlShape) != derived_final_kind) return error.UnsupportedSqlShape;

    for (read_ast.cte_items, 0..) |cte, index| {
        if (cte.name_tokens.start < cte_list_tokens.start or cte.name_tokens.end > cte_list_tokens.end) {
            return error.UnsupportedSqlShape;
        }
        const body = cte.body_tokens orelse return error.UnsupportedSqlShape;
        try validateGeneratedCteItemLayout(tokens, cte_list_tokens, cte, index, if (index == 0) null else read_ast.cte_items[index - 1]);
        try validateGeneratedCteBodyMetadata(tokens, cte);
        if (cte.column_tokens) |column_tokens| {
            const column_name_tokens = cte.column_name_tokens orelse return error.UnsupportedSqlShape;
            if (column_tokens.start < cte.name_tokens.end or column_tokens.end > body.start) return error.UnsupportedSqlShape;
            if (column_name_tokens.start <= column_tokens.start or column_name_tokens.end >= column_tokens.end) {
                return error.UnsupportedSqlShape;
            }
            if (cte.column_names.count == 0) return error.UnsupportedSqlShape;
        } else if (cte.column_name_tokens != null or cte.column_names.count != 0) {
            return error.UnsupportedSqlShape;
        }
        if (cte.materialization_tokens) |materialization_tokens| {
            if (cte.materialization == null) return error.UnsupportedSqlShape;
            if (materialization_tokens.start <= cte.name_tokens.end or materialization_tokens.end > body.start) {
                return error.UnsupportedSqlShape;
            }
        } else if (cte.materialization != null) {
            return error.UnsupportedSqlShape;
        }
    }
    const last_body = last.body_tokens orelse return error.UnsupportedSqlShape;
    if (last_body.end + 1 != cte_list_tokens.end) return error.UnsupportedSqlShape;
}

fn validateGeneratedCtePrefixLayout(
    tokens: []const tokenized.Token,
    cte_tokens: generated_parser.GeneratedSqlTokenRange,
    cte_list_tokens: generated_parser.GeneratedSqlTokenRange,
    recursive: bool,
) !void {
    if (cte_tokens.start == 0 or cte_tokens.end > tokens.len or cte_tokens.start >= cte_tokens.end) return error.UnsupportedSqlShape;
    if (!tokens[cte_tokens.start - 1].matchesKeywordTag(.with)) return error.UnsupportedSqlShape;
    if (cte_list_tokens.end != cte_tokens.end) return error.UnsupportedSqlShape;
    if (recursive) {
        if (cte_list_tokens.start != cte_tokens.start + 1) return error.UnsupportedSqlShape;
        if (!tokens[cte_tokens.start].matchesKeywordTag(.recursive)) return error.UnsupportedSqlShape;
    } else {
        if (cte_list_tokens.start != cte_tokens.start) return error.UnsupportedSqlShape;
        if (tokens[cte_tokens.start].matchesKeywordTag(.recursive)) return error.UnsupportedSqlShape;
    }
}

fn validateGeneratedCteItemLayout(
    tokens: []const tokenized.Token,
    cte_list_tokens: generated_parser.GeneratedSqlTokenRange,
    cte: generated_parser.GeneratedSqlCteAst,
    index: usize,
    previous: ?generated_parser.GeneratedSqlCteAst,
) !void {
    const body = cte.body_tokens orelse return error.UnsupportedSqlShape;
    if (cte.name_tokens.start < cte_list_tokens.start or cte.name_tokens.end > cte_list_tokens.end or cte.name_tokens.start >= cte.name_tokens.end) {
        return error.UnsupportedSqlShape;
    }

    if (previous) |previous_cte| {
        const previous_body = previous_cte.body_tokens orelse return error.UnsupportedSqlShape;
        const comma_index = previous_body.end + 1;
        if (comma_index >= tokens.len or comma_index >= cte_list_tokens.end or tokens[comma_index].kind != .comma) return error.UnsupportedSqlShape;
        if (cte.name_tokens.start != comma_index + 1) return error.UnsupportedSqlShape;
    } else {
        if (index != 0 or cte.name_tokens.start != cte_list_tokens.start) return error.UnsupportedSqlShape;
    }

    var cursor = cte.name_tokens.end;
    if (cte.column_tokens) |column_tokens| {
        if (column_tokens.start != cursor or column_tokens.end <= column_tokens.start + 1 or column_tokens.end > body.start) return error.UnsupportedSqlShape;
        if (tokens[column_tokens.start].kind != .lparen or tokens[column_tokens.end - 1].kind != .rparen) return error.UnsupportedSqlShape;
        cursor = column_tokens.end;
    }

    if (cursor >= tokens.len or !tokens[cursor].matchesKeywordTag(.as)) return error.UnsupportedSqlShape;
    cursor += 1;

    if (cte.materialization_tokens) |materialization_tokens| {
        if (materialization_tokens.start != cursor or materialization_tokens.end > body.start) return error.UnsupportedSqlShape;
        switch (cte.materialization orelse return error.UnsupportedSqlShape) {
            .materialized => {
                if (materialization_tokens.end != materialization_tokens.start + 1) return error.UnsupportedSqlShape;
                if (!tokens[materialization_tokens.start].matchesKeywordTag(.materialized)) return error.UnsupportedSqlShape;
            },
            .not_materialized => {
                if (materialization_tokens.end != materialization_tokens.start + 2) return error.UnsupportedSqlShape;
                if (!tokens[materialization_tokens.start].matchesKeywordTag(.not) or
                    !tokens[materialization_tokens.start + 1].matchesKeywordTag(.materialized))
                {
                    return error.UnsupportedSqlShape;
                }
            },
        }
        cursor = materialization_tokens.end;
    } else if (cte.materialization != null) {
        return error.UnsupportedSqlShape;
    }

    if (body.start != cursor + 1 or body.end >= tokens.len or body.end >= cte_list_tokens.end) return error.UnsupportedSqlShape;
    if (tokens[cursor].kind != .lparen or tokens[body.end].kind != .rparen) return error.UnsupportedSqlShape;
    if (body.end + 1 > cte_list_tokens.end) return error.UnsupportedSqlShape;
    if (body.end + 1 == cte_list_tokens.end) return;
    if (tokens[body.end + 1].kind != .comma) return error.UnsupportedSqlShape;
}

fn validateGeneratedCteBodyMetadata(tokens: []const tokenized.Token, cte: generated_parser.GeneratedSqlCteAst) !void {
    const body = cte.body_tokens orelse return error.UnsupportedSqlShape;
    const body_kind = cte.body_kind orelse return error.UnsupportedSqlShape;
    const select_tokens = cte.body_select_tokens orelse return error.UnsupportedSqlShape;
    const projection_tokens = cte.body_projection_tokens orelse return error.UnsupportedSqlShape;
    if (select_tokens.start != body.start or select_tokens.end != body.start + 1) return error.UnsupportedSqlShape;
    if (!tokens[select_tokens.start].matchesKeywordTag(.select)) return error.UnsupportedSqlShape;
    if (projection_tokens.start < select_tokens.end or projection_tokens.end > body.end) return error.UnsupportedSqlShape;
    if (cte.body_projection_items.count == 0) return error.UnsupportedSqlShape;
    if (cte.body_projection_items.count != 0 and !std.meta.eql(cte.body_projection_items.first_tokens orelse return error.UnsupportedSqlShape, cte.body_projection_items.items[0])) {
        return error.UnsupportedSqlShape;
    }
    if (!generatedExpressionAstHasMetadata(cte.body_projection_first_expression)) return error.UnsupportedSqlShape;
    const body_ranges = [_]?generated_parser.GeneratedSqlTokenRange{
        cte.body_distinct_tokens,
        cte.body_projection_tokens,
        cte.body_source_tokens,
        cte.body_source_system_time_tokens,
        cte.body_source_system_time_sequence_tokens,
        cte.body_join_tokens,
        cte.body_join_operator_tokens,
        cte.body_join_left_tokens,
        cte.body_join_right_tokens,
        cte.body_join_predicate_tokens,
        cte.body_where_tokens,
        cte.body_group_tokens,
        cte.body_having_tokens,
        cte.body_window_tokens,
        cte.body_order_tokens,
        cte.body_limit_tokens,
        cte.body_offset_tokens,
        cte.body_fetch_tokens,
        cte.body_fetch_count_tokens,
        cte.body_row_lock_tokens,
        cte.body_set_operation_tokens,
    };
    for (body_ranges) |maybe_range| {
        if (maybe_range) |range| {
            if (range.start < body.start or range.end > body.end) return error.UnsupportedSqlShape;
        }
    }
    try validateGeneratedProjectionStartAfterSelectModifier(
        tokens,
        select_tokens,
        projection_tokens,
        cte.body_distinct_tokens,
        cte.body_distinct_on_items,
    );
    if (cte.body_source_tokens) |source_tokens| {
        if (source_tokens.start <= projection_tokens.end) return error.UnsupportedSqlShape;
        try validateGeneratedReadRangePrecededByKeyword(tokens, source_tokens, .from);
        try validateGeneratedReadSystemTimePayload(
            tokens,
            cte.body_source_tokens,
            cte.body_source_system_time_tokens,
            cte.body_source_system_time_sequence_tokens,
        );
        try validateGeneratedReadSourceTableMetadata(
            tokens,
            cte.body_source_tokens,
            cte.body_source_table_tokens,
            cte.body_source_alias_tokens,
            cte.body_source_alias_name_tokens,
            cte.body_source_system_time_tokens,
            null,
        );
        try validateGeneratedJoinTreeMetadataForSource(
            tokens,
            source_tokens,
            cte.body_join_items,
            cte.body_join_tree_root_index,
            cte.body_join_tree_depth,
            cte.body_join_tokens,
            cte.body_join_operator_tokens,
            cte.body_join_kind,
            cte.body_join_left_tokens,
            cte.body_join_right_tokens,
            cte.body_join_predicate_tokens,
        );
    } else if (cte.body_join_items.len != 0 or cte.body_join_tree_root_index != null or cte.body_join_tree_depth != 0 or
        cte.body_join_tokens != null or cte.body_join_operator_tokens != null or cte.body_join_kind != null or
        cte.body_join_left_tokens != null or cte.body_join_right_tokens != null or cte.body_join_predicate_tokens != null or
        cte.body_source_antfly_function_items.len != 0 or cte.body_source_antfly_function_count != 0 or
        cte.body_source_graph_function_items.len != 0 or cte.body_source_graph_function_count != 0 or
        cte.body_source_table_tokens != null or cte.body_source_alias_tokens != null or cte.body_source_alias_name_tokens != null or
        cte.body_source_system_time_tokens != null or cte.body_source_system_time_sequence_tokens != null or
        generatedExpressionAstHasMetadata(cte.body_join_predicate_expression))
    {
        return error.UnsupportedSqlShape;
    }
    if (cte.body_where_tokens) |where_tokens| {
        if (where_tokens.start <= projection_tokens.end) return error.UnsupportedSqlShape;
        try validateGeneratedReadRangePrecededByKeyword(tokens, where_tokens, .where);
        try validateGeneratedPredicateExpressionMatchesRange(tokens, cte.body_where_expression, where_tokens);
    }
    if (cte.body_group_tokens) |group_tokens| {
        if (group_tokens.start <= projection_tokens.end) return error.UnsupportedSqlShape;
        try validateGeneratedReadGroupRange(tokens, group_tokens);
        if (cte.body_group_items.count == 0) return error.UnsupportedSqlShape;
        if (!generatedExpressionAstHasMetadata(cte.body_group_first_expression)) return error.UnsupportedSqlShape;
    } else if (cte.body_group_items.count != 0 or generatedExpressionAstHasMetadata(cte.body_group_first_expression) or generatedExpressionAstHasMetadata(cte.body_group_last_expression)) {
        return error.UnsupportedSqlShape;
    }
    if (cte.body_having_tokens) |having_tokens| {
        if (having_tokens.start <= projection_tokens.end) return error.UnsupportedSqlShape;
        try validateGeneratedReadRangePrecededByKeyword(tokens, having_tokens, .having);
        try validateGeneratedPredicateExpressionMatchesRange(tokens, cte.body_having_expression, having_tokens);
    } else if (generatedExpressionAstHasMetadata(cte.body_having_expression)) {
        return error.UnsupportedSqlShape;
    }
    if (cte.body_window_tokens) |window_tokens| {
        if (window_tokens.start <= projection_tokens.end) return error.UnsupportedSqlShape;
        try validateGeneratedReadRangePrecededByKeyword(tokens, window_tokens, .window);
        if (cte.body_window_count == 0 or cte.body_window_items.len != cte.body_window_count) return error.UnsupportedSqlShape;
    } else if (cte.body_window_count != 0 or cte.body_window_items.len != 0) {
        return error.UnsupportedSqlShape;
    }
    if (cte.body_order_tokens) |order_tokens| {
        if (order_tokens.start <= projection_tokens.end) return error.UnsupportedSqlShape;
        try validateGeneratedReadOrderRange(tokens, order_tokens);
        if (cte.body_order_items.count == 0) return error.UnsupportedSqlShape;
        if (!generatedExpressionAstHasMetadata(cte.body_order_first_expression)) return error.UnsupportedSqlShape;
    } else if (cte.body_order_items.count != 0 or generatedExpressionAstHasMetadata(cte.body_order_first_expression) or generatedExpressionAstHasMetadata(cte.body_order_last_expression)) {
        return error.UnsupportedSqlShape;
    }
    if (cte.body_limit_tokens) |limit_tokens| {
        if (limit_tokens.start <= projection_tokens.end) return error.UnsupportedSqlShape;
        try validateGeneratedReadRangePrecededByKeyword(tokens, limit_tokens, .limit);
        if (cte.body_limit_all) {
            if (generatedExpressionAstHasMetadata(cte.body_limit_expression)) return error.UnsupportedSqlShape;
        } else if (!generatedExpressionAstHasMetadata(cte.body_limit_expression)) {
            return error.UnsupportedSqlShape;
        }
    } else if (cte.body_limit_all or generatedExpressionAstHasMetadata(cte.body_limit_expression)) {
        return error.UnsupportedSqlShape;
    }
    if (cte.body_offset_tokens) |offset_tokens| {
        if (offset_tokens.start <= projection_tokens.end) return error.UnsupportedSqlShape;
        try validateGeneratedReadRangePrecededByKeyword(tokens, offset_tokens, .offset);
        if (!generatedExpressionAstHasMetadata(cte.body_offset_expression)) return error.UnsupportedSqlShape;
    } else if (generatedExpressionAstHasMetadata(cte.body_offset_expression)) {
        return error.UnsupportedSqlShape;
    }
    if (cte.body_fetch_tokens) |fetch_tokens| {
        if (fetch_tokens.start <= projection_tokens.end) return error.UnsupportedSqlShape;
        try validateGeneratedReadRangePrecededByKeyword(tokens, fetch_tokens, .fetch);
        if (cte.body_fetch_count_tokens) |count_tokens| {
            if (count_tokens.start < fetch_tokens.start or count_tokens.end > fetch_tokens.end) return error.UnsupportedSqlShape;
            if (!generatedExpressionAstHasMetadata(cte.body_fetch_count_expression)) return error.UnsupportedSqlShape;
        } else if (generatedExpressionAstHasMetadata(cte.body_fetch_count_expression)) {
            return error.UnsupportedSqlShape;
        }
    } else if (cte.body_fetch_count_tokens != null or generatedExpressionAstHasMetadata(cte.body_fetch_count_expression)) {
        return error.UnsupportedSqlShape;
    }
    if (cte.body_row_lock_tokens) |row_lock_tokens| {
        if (row_lock_tokens.start <= projection_tokens.end) return error.UnsupportedSqlShape;
        try validateGeneratedReadRowLockClauseLayout(tokens, cte.body_row_lock_tokens);
    }
    if (cte.body_set_operation_tokens) |set_operation_tokens| {
        if (set_operation_tokens.start <= projection_tokens.end) return error.UnsupportedSqlShape;
        try validateGeneratedSetOperationMetadata(cte.body_set_operation_tokens, cte.body_set_operation);
    } else if (generatedSetOperationAstHasMetadata(cte.body_set_operation)) {
        return error.UnsupportedSqlShape;
    }
    try validateGeneratedReadClauseOrder(projection_tokens.end, &.{
        cte.body_source_tokens,
        cte.body_where_tokens,
        cte.body_group_tokens,
        cte.body_having_tokens,
        cte.body_window_tokens,
        cte.body_set_operation_tokens,
        cte.body_order_tokens,
        cte.body_limit_tokens,
        cte.body_offset_tokens,
        cte.body_fetch_tokens,
        cte.body_row_lock_tokens,
    });
    switch (body_kind) {
        .set_operation => if (cte.body_set_operation_tokens == null) return error.UnsupportedSqlShape,
        .aggregate => if (cte.body_group_tokens == null and cte.body_having_tokens == null and cte.body_distinct_tokens == null) {
            return error.UnsupportedSqlShape;
        },
        .query => if (cte.body_set_operation_tokens != null or cte.body_group_tokens != null or cte.body_having_tokens != null) {
            return error.UnsupportedSqlShape;
        },
        .join => if (cte.body_source_tokens == null or cte.body_join_items.len == 0) return error.UnsupportedSqlShape,
        .lateral => if (cte.body_source_tokens == null) return error.UnsupportedSqlShape,
        .window => if (cte.body_source_tokens == null) return error.UnsupportedSqlShape,
        .cte => return error.UnsupportedSqlShape,
    }
}

fn validateGeneratedJoinTreeMetadata(
    tokens: []const tokenized.Token,
    read_ast: *const generated_parser.GeneratedSqlReadAst,
) !void {
    return validateGeneratedJoinTreeMetadataForSource(
        tokens,
        read_ast.source_tokens,
        read_ast.join_items,
        read_ast.join_tree_root_index,
        read_ast.join_tree_depth,
        read_ast.join_tokens,
        read_ast.join_operator_tokens,
        read_ast.join_kind,
        read_ast.join_left_tokens,
        read_ast.join_right_tokens,
        read_ast.join_predicate_tokens,
    );
}

fn validateGeneratedJoinTreeMetadataForSource(
    tokens: []const tokenized.Token,
    source_tokens: ?generated_parser.GeneratedSqlTokenRange,
    join_items: []const generated_parser.GeneratedSqlJoinAst,
    join_tree_root_index: ?usize,
    join_tree_depth: usize,
    join_tokens: ?generated_parser.GeneratedSqlTokenRange,
    join_operator_tokens: ?generated_parser.GeneratedSqlTokenRange,
    join_kind: ?generated_parser.GeneratedSqlJoinKind,
    join_left_tokens: ?generated_parser.GeneratedSqlTokenRange,
    join_right_tokens: ?generated_parser.GeneratedSqlTokenRange,
    join_predicate_tokens: ?generated_parser.GeneratedSqlTokenRange,
) !void {
    if (join_items.len == 0) {
        if (join_tree_root_index != null or join_tree_depth != 0 or join_tokens != null or
            join_operator_tokens != null or join_kind != null or join_left_tokens != null or
            join_right_tokens != null or join_predicate_tokens != null)
        {
            return error.UnsupportedSqlShape;
        }
        return;
    }

    const source = source_tokens orelse return error.UnsupportedSqlShape;
    const root_index = join_tree_root_index orelse return error.UnsupportedSqlShape;
    if (root_index != join_items.len - 1) return error.UnsupportedSqlShape;
    if (join_tree_depth != join_items.len) return error.UnsupportedSqlShape;

    const first = join_items[0];
    if (!std.meta.eql(join_tokens orelse return error.UnsupportedSqlShape, source)) return error.UnsupportedSqlShape;
    if (!std.meta.eql(join_operator_tokens orelse return error.UnsupportedSqlShape, first.operator_tokens)) return error.UnsupportedSqlShape;
    if ((join_kind orelse return error.UnsupportedSqlShape) != first.kind) return error.UnsupportedSqlShape;
    if (!std.meta.eql(join_left_tokens orelse return error.UnsupportedSqlShape, first.left_tokens)) return error.UnsupportedSqlShape;
    if (!std.meta.eql(join_right_tokens orelse return error.UnsupportedSqlShape, first.right_tokens)) return error.UnsupportedSqlShape;
    if (!optionalGeneratedTokenRangeEql(join_predicate_tokens, first.predicate_tokens)) return error.UnsupportedSqlShape;

    for (join_items, 0..) |join, index| {
        if (join.tree_index != index) return error.UnsupportedSqlShape;
        if (join.tree_depth != index + 1) return error.UnsupportedSqlShape;
        if (join.tokens.start != source.start or join.tokens.end > source.end) return error.UnsupportedSqlShape;
        if (index == 0) {
            if (join.left_child_index != null) return error.UnsupportedSqlShape;
            if (join.left_tokens.start != source.start) return error.UnsupportedSqlShape;
        } else {
            const expected_left = join_items[index - 1];
            if (join.left_child_index == null or join.left_child_index.? != index - 1) return error.UnsupportedSqlShape;
            if (!std.meta.eql(join.left_tokens, expected_left.tokens)) return error.UnsupportedSqlShape;
        }
        if (join.operator_tokens.start < join.left_tokens.end or join.operator_tokens.end > join.right_tokens.start) {
            return error.UnsupportedSqlShape;
        }
        if (join.left_tokens.end != join.operator_tokens.start or join.operator_tokens.end != join.right_tokens.start) {
            return error.UnsupportedSqlShape;
        }
        try validateGeneratedReadSourceTableMetadata(
            tokens,
            join.left_tokens,
            join.left_table_tokens,
            join.left_alias_tokens,
            join.left_alias_name_tokens,
            null,
            null,
        );
        try validateGeneratedReadSourceTableMetadata(
            tokens,
            join.right_tokens,
            join.right_table_tokens,
            join.right_alias_tokens,
            join.right_alias_name_tokens,
            null,
            null,
        );
        try validateGeneratedJoinKindForOperator(tokens, join.operator_tokens, join.kind);
        if (join.right_tokens.start < join.operator_tokens.end or join.right_tokens.end > join.condition_tokens.start) {
            return error.UnsupportedSqlShape;
        }
        if (join.condition_kind != .none and join.right_tokens.end != join.condition_tokens.start) return error.UnsupportedSqlShape;
        if (join.condition_tokens.start < join.right_tokens.end or join.condition_tokens.end > join.tokens.end) {
            return error.UnsupportedSqlShape;
        }
        if (join.tokens.end != join.condition_tokens.end) return error.UnsupportedSqlShape;
        switch (join.condition_kind) {
            .none => {
                if (join.kind != .cross and join.kind != .natural) return error.UnsupportedSqlShape;
                if (join.condition_tokens.start != join.tokens.end or join.condition_tokens.end != join.tokens.end) {
                    return error.UnsupportedSqlShape;
                }
                if (join.predicate_tokens != null or generatedExpressionAstHasMetadata(join.predicate_expression)) {
                    return error.UnsupportedSqlShape;
                }
                if (join.using_tokens != null or join.using_column_tokens != null or join.using_columns.count != 0) {
                    return error.UnsupportedSqlShape;
                }
            },
            .on => {
                const predicate = join.predicate_tokens orelse return error.UnsupportedSqlShape;
                if (!tokens[join.condition_tokens.start].matchesKeywordTag(.on)) return error.UnsupportedSqlShape;
                if (join.using_tokens != null or join.using_column_tokens != null or join.using_columns.count != 0) {
                    return error.UnsupportedSqlShape;
                }
                if (predicate.start != join.condition_tokens.start + 1 or predicate.end != join.condition_tokens.end) {
                    return error.UnsupportedSqlShape;
                }
                try validateGeneratedPredicateExpressionMatchesRange(tokens, join.predicate_expression, predicate);
            },
            .using => {
                const using_tokens = join.using_tokens orelse return error.UnsupportedSqlShape;
                const using_column_tokens = join.using_column_tokens orelse return error.UnsupportedSqlShape;
                if (!tokens[join.condition_tokens.start].matchesKeywordTag(.using)) return error.UnsupportedSqlShape;
                if (join.predicate_tokens != null or generatedExpressionAstHasMetadata(join.predicate_expression)) {
                    return error.UnsupportedSqlShape;
                }
                if (!std.meta.eql(using_tokens, join.condition_tokens)) return error.UnsupportedSqlShape;
                if (using_tokens.start + 1 >= using_tokens.end or
                    tokens[using_tokens.start + 1].kind != .lparen or
                    tokens[using_tokens.end - 1].kind != .rparen)
                {
                    return error.UnsupportedSqlShape;
                }
                if (using_column_tokens.start != using_tokens.start + 2 or using_column_tokens.end != using_tokens.end - 1) {
                    return error.UnsupportedSqlShape;
                }
                if (join.using_columns.count == 0) return error.UnsupportedSqlShape;
                try validateGeneratedReadCommaDelimitedList(tokens, using_column_tokens, join.using_columns);
            },
        }
    }
}

fn validateGeneratedJoinKindForOperator(
    tokens: []const tokenized.Token,
    operator_tokens: generated_parser.GeneratedSqlTokenRange,
    kind: generated_parser.GeneratedSqlJoinKind,
) !void {
    if (operator_tokens.start >= operator_tokens.end or operator_tokens.end > tokens.len) return error.UnsupportedSqlShape;
    switch (kind) {
        .inner => {
            if (operator_tokens.end == operator_tokens.start + 1) {
                if (!tokens[operator_tokens.start].matchesKeywordTag(.join)) return error.UnsupportedSqlShape;
            } else if (operator_tokens.end == operator_tokens.start + 2) {
                if (!tokens[operator_tokens.start].matchesKeywordTag(.inner) or !tokens[operator_tokens.start + 1].matchesKeywordTag(.join)) {
                    return error.UnsupportedSqlShape;
                }
            } else {
                return error.UnsupportedSqlShape;
            }
        },
        .left, .right, .full => {
            const expected_keyword: token_mod.TokenKeyword = switch (kind) {
                .left => .left,
                .right => .right,
                .full => .full,
                else => unreachable,
            };
            if (!tokens[operator_tokens.start].matchesKeywordTag(expected_keyword)) return error.UnsupportedSqlShape;
            if (operator_tokens.end == operator_tokens.start + 2) {
                if (!tokens[operator_tokens.start + 1].matchesKeywordTag(.join)) return error.UnsupportedSqlShape;
            } else if (operator_tokens.end == operator_tokens.start + 3) {
                if (!tokens[operator_tokens.start + 1].matchesKeywordTag(.outer) or !tokens[operator_tokens.start + 2].matchesKeywordTag(.join)) {
                    return error.UnsupportedSqlShape;
                }
            } else {
                return error.UnsupportedSqlShape;
            }
        },
        .cross, .natural => {
            const expected_keyword: token_mod.TokenKeyword = switch (kind) {
                .cross => .cross,
                .natural => .natural,
                else => unreachable,
            };
            if (operator_tokens.end != operator_tokens.start + 2) return error.UnsupportedSqlShape;
            if (!tokens[operator_tokens.start].matchesKeywordTag(expected_keyword) or !tokens[operator_tokens.start + 1].matchesKeywordTag(.join)) {
                return error.UnsupportedSqlShape;
            }
        },
    }
}

fn validateGeneratedJoinAstRanges(
    tokens: []const tokenized.Token,
    read_ast: *const generated_parser.GeneratedSqlReadAst,
    join_items: []const generated_parser.GeneratedSqlJoinAst,
) !void {
    for (join_items) |join| {
        try validateGeneratedReadTokenRange(tokens, read_ast, join.tokens);
        try validateGeneratedReadTokenRange(tokens, read_ast, join.operator_tokens);
        try validateGeneratedReadTokenRange(tokens, read_ast, join.left_tokens);
        try validateGeneratedReadTokenRange(tokens, read_ast, join.right_tokens);
        if (join.condition_kind != .none) try validateGeneratedReadTokenRange(tokens, read_ast, join.condition_tokens);
        if (join.predicate_tokens) |predicate_tokens| try validateGeneratedReadTokenRange(tokens, read_ast, predicate_tokens);
        if (join.using_tokens) |using_tokens| try validateGeneratedReadTokenRange(tokens, read_ast, using_tokens);
        if (join.using_column_tokens) |using_column_tokens| try validateGeneratedReadTokenRange(tokens, read_ast, using_column_tokens);
        try validateGeneratedReadJoinLateralPayload(tokens, read_ast, join);
        try validateGeneratedExpressionAstRangesIfPresent(tokens, read_ast, join.predicate_expression);
        try validateGeneratedReadListAstRanges(tokens, read_ast, join.using_columns);
        try validateGeneratedReadListAstContainedByOptionalRange(join.using_columns, join.using_column_tokens);
    }
}

fn validateGeneratedReadJoinLateralPayload(
    tokens: []const tokenized.Token,
    read_ast: *const generated_parser.GeneratedSqlReadAst,
    join: generated_parser.GeneratedSqlJoinAst,
) GeneratedReadValidationError!void {
    if (join.right_tokens.start >= join.right_tokens.end or join.right_tokens.end > tokens.len) return error.UnsupportedSqlShape;
    if (!tokens[join.right_tokens.start].matchesKeywordTag(.lateral)) {
        if (join.right_lateral_subquery_tokens != null or
            join.right_lateral_subquery_read_ast != null or
            join.right_lateral_alias_tokens != null or
            join.right_lateral_alias_name_tokens != null)
        {
            return error.UnsupportedSqlShape;
        }
        return;
    }

    if (join.right_tokens.start + 1 >= join.right_tokens.end or tokens[join.right_tokens.start + 1].kind != .lparen) {
        if (join.right_lateral_subquery_tokens != null or
            join.right_lateral_subquery_read_ast != null or
            join.right_lateral_alias_tokens != null or
            join.right_lateral_alias_name_tokens != null)
        {
            return error.UnsupportedSqlShape;
        }
        try validateGeneratedReadLateralTableFunctionSource(tokens, join.right_tokens);
        return;
    }

    const subquery_tokens = join.right_lateral_subquery_tokens orelse return error.UnsupportedSqlShape;
    const subquery_read = join.right_lateral_subquery_read_ast orelse return error.UnsupportedSqlShape;
    const alias_tokens = join.right_lateral_alias_tokens orelse return error.UnsupportedSqlShape;
    const alias_name_tokens = join.right_lateral_alias_name_tokens orelse return error.UnsupportedSqlShape;
    try validateGeneratedReadTokenRange(tokens, read_ast, subquery_tokens);
    try validateGeneratedReadTokenRange(tokens, read_ast, alias_tokens);
    try validateGeneratedReadTokenRange(tokens, read_ast, alias_name_tokens);
    if (join.right_tokens.start + 2 != subquery_tokens.start) return error.UnsupportedSqlShape;
    if (subquery_tokens.start >= subquery_tokens.end or subquery_tokens.end >= join.right_tokens.end) return error.UnsupportedSqlShape;
    if (tokens[join.right_tokens.start + 1].kind != .lparen or tokens[subquery_tokens.end].kind != .rparen) return error.UnsupportedSqlShape;
    if (alias_tokens.start != subquery_tokens.end + 1 or alias_tokens.end != join.right_tokens.end) return error.UnsupportedSqlShape;
    if (alias_name_tokens.end != alias_name_tokens.start + 1 or alias_name_tokens.end > alias_tokens.end) return error.UnsupportedSqlShape;
    if (tokens[alias_name_tokens.start].kind != .identifier) return error.UnsupportedSqlShape;
    if (alias_tokens.end == alias_tokens.start + 2) {
        if (!tokens[alias_tokens.start].matchesKeywordTag(.as) or alias_name_tokens.start != alias_tokens.start + 1) return error.UnsupportedSqlShape;
    } else if (alias_tokens.end == alias_tokens.start + 1) {
        if (alias_name_tokens.start != alias_tokens.start) return error.UnsupportedSqlShape;
    } else {
        return error.UnsupportedSqlShape;
    }
    generated_read_validate.validateGeneratedReadAstPayloads(tokens[subquery_tokens.start..subquery_tokens.end], subquery_read.*) catch return error.UnsupportedSqlShape;
}

fn validateGeneratedReadLateralTableFunctionSource(
    tokens: []const tokenized.Token,
    source: generated_parser.GeneratedSqlTokenRange,
) GeneratedReadValidationError!void {
    if (source.start + 2 >= source.end or source.end > tokens.len) return error.UnsupportedSqlShape;
    if (!tokens[source.start].matchesKeywordTag(.lateral)) return error.UnsupportedSqlShape;
    if (tokens[source.start + 1].kind != .identifier) return error.UnsupportedSqlShape;
    const lparen_index = source.start + 2;
    if (tokens[lparen_index].kind != .lparen) return error.UnsupportedSqlShape;
    const rparen_index = generatedReadMatchingParenInRange(tokens, lparen_index, source.end) orelse return error.UnsupportedSqlShape;
    const alias_start = rparen_index + 1;
    if (alias_start == source.end) return;
    if (alias_start + 1 == source.end) {
        if (tokens[alias_start].kind != .identifier) return error.UnsupportedSqlShape;
        return;
    }
    if (alias_start + 2 != source.end or
        !tokens[alias_start].matchesKeywordTag(.as) or
        tokens[alias_start + 1].kind != .identifier)
    {
        return error.UnsupportedSqlShape;
    }
}

fn generatedReadMatchingParenInRange(
    tokens: []const tokenized.Token,
    open_index: usize,
    end: usize,
) ?usize {
    if (open_index >= end or end > tokens.len or tokens[open_index].kind != .lparen) return null;
    var depth: usize = 0;
    var index = open_index;
    while (index < end) : (index += 1) {
        switch (tokens[index].kind) {
            .lparen => depth += 1,
            .rparen => {
                if (depth == 0) return null;
                depth -= 1;
                if (depth == 0) return index;
            },
            else => {},
        }
    }
    return null;
}

fn optionalGeneratedTokenRangeEql(
    lhs: ?generated_parser.GeneratedSqlTokenRange,
    rhs: ?generated_parser.GeneratedSqlTokenRange,
) bool {
    if (lhs) |left| {
        if (rhs) |right| return std.meta.eql(left, right);
        return false;
    }
    return rhs == null;
}

fn validateGeneratedProjectionStartAfterSelectModifier(
    tokens: []const tokenized.Token,
    select_tokens: generated_parser.GeneratedSqlTokenRange,
    projection_tokens: generated_parser.GeneratedSqlTokenRange,
    distinct_tokens: ?generated_parser.GeneratedSqlTokenRange,
    distinct_on_items: generated_parser.GeneratedSqlListAst,
) !void {
    if (distinct_tokens) |distinct| {
        if (distinct.start != select_tokens.end or projection_tokens.start != distinct.end) return error.UnsupportedSqlShape;
        try validateGeneratedReadRangePrecededByKeyword(tokens, distinct, .select);
        try validateGeneratedDistinctOnListMetadata(distinct_tokens, distinct_on_items);
        return;
    }
    if (select_tokens.end < tokens.len and tokens[select_tokens.end].matchesKeywordTag(.all)) {
        if (projection_tokens.start != select_tokens.end + 1) return error.UnsupportedSqlShape;
        if (distinct_on_items.count != 0) return error.UnsupportedSqlShape;
        return;
    }
    if (projection_tokens.start != select_tokens.end) return error.UnsupportedSqlShape;
    if (distinct_on_items.count != 0) return error.UnsupportedSqlShape;
}

fn validateGeneratedReadClauseOrder(
    projection_end: usize,
    ranges: []const ?generated_parser.GeneratedSqlTokenRange,
) !void {
    var previous_end = projection_end;
    for (ranges) |maybe_range| {
        const range = maybe_range orelse continue;
        if (range.start < previous_end or range.start >= range.end) return error.UnsupportedSqlShape;
        previous_end = range.end;
    }
}

fn validateGeneratedReadClauseMetadata(tokens: []const tokenized.Token, read_ast: *const generated_parser.GeneratedSqlReadAst) !void {
    const projection_tokens = read_ast.projection_tokens orelse return error.UnsupportedSqlShape;
    const select_tokens: generated_parser.GeneratedSqlTokenRange = if (read_ast.cte_tokens) |cte_tokens| blk: {
        if (cte_tokens.end >= tokens.len or !tokens[cte_tokens.end].matchesKeywordTag(.select)) {
            return error.UnsupportedSqlShape;
        }
        break :blk .{ .start = cte_tokens.end, .end = cte_tokens.end + 1 };
    } else blk: {
        if (tokens.len == 0 or !tokens[0].matchesKeywordTag(.select)) return error.UnsupportedSqlShape;
        break :blk .{ .start = 0, .end = 1 };
    };

    if (projection_tokens.start < select_tokens.end) return error.UnsupportedSqlShape;
    if (read_ast.projection_items.count == 0) return error.UnsupportedSqlShape;
    if (!generatedExpressionAstHasMetadata(read_ast.projection_first_expression)) return error.UnsupportedSqlShape;
    if (!generatedExpressionAstHasMetadata(read_ast.projection_last_expression)) return error.UnsupportedSqlShape;

    try validateGeneratedProjectionStartAfterSelectModifier(
        tokens,
        select_tokens,
        projection_tokens,
        read_ast.distinct_tokens,
        read_ast.distinct_on_items,
    );

    if (read_ast.source_tokens) |source_tokens| {
        if (source_tokens.start <= projection_tokens.end) return error.UnsupportedSqlShape;
        try validateGeneratedReadRangePrecededByKeyword(tokens, source_tokens, .from);
        try validateGeneratedReadSystemTimePayload(
            tokens,
            read_ast.source_tokens,
            read_ast.source_system_time_tokens,
            read_ast.source_system_time_sequence_tokens,
        );
        try validateGeneratedReadSourceTableMetadata(
            tokens,
            read_ast.source_tokens,
            read_ast.source_table_tokens,
            read_ast.source_alias_tokens,
            read_ast.source_alias_name_tokens,
            read_ast.source_system_time_tokens,
            read_ast.source_unnest_tokens,
        );
    } else if (read_ast.source_antfly_function_items.len != 0 or
        read_ast.source_antfly_function_count != 0 or
        read_ast.source_graph_function_items.len != 0 or
        read_ast.source_graph_function_count != 0 or
        read_ast.source_graph_function_tokens != null or
        read_ast.source_graph_function_name_tokens != null or
        read_ast.source_graph_function_argument_tokens != null or
        read_ast.source_graph_function_kind != null or
        read_ast.source_unnest_tokens != null or
        read_ast.source_unnest_name_tokens != null or
        read_ast.source_unnest_argument_tokens != null or
        read_ast.source_unnest_argument_expression.tokens != null or
        read_ast.source_unnest_alias_tokens != null or
        read_ast.source_unnest_alias_name_tokens != null or
        read_ast.source_table_tokens != null or
        read_ast.source_alias_tokens != null or
        read_ast.source_alias_name_tokens != null or
        read_ast.source_system_time_tokens != null or
        read_ast.source_system_time_sequence_tokens != null)
    {
        return error.UnsupportedSqlShape;
    }

    if (read_ast.where_tokens) |where_tokens| {
        if (where_tokens.start <= projection_tokens.end) return error.UnsupportedSqlShape;
        try validateGeneratedReadRangePrecededByKeyword(tokens, where_tokens, .where);
        try validateGeneratedPredicateExpressionMatchesRange(tokens, read_ast.where_expression, where_tokens);
    } else if (generatedExpressionAstHasMetadata(read_ast.where_expression)) {
        return error.UnsupportedSqlShape;
    }

    if (read_ast.group_tokens) |group_tokens| {
        if (group_tokens.start <= projection_tokens.end) return error.UnsupportedSqlShape;
        try validateGeneratedReadGroupRange(tokens, group_tokens);
        if (read_ast.group_items.count == 0) return error.UnsupportedSqlShape;
        if (!generatedExpressionAstHasMetadata(read_ast.group_first_expression)) return error.UnsupportedSqlShape;
        if (!generatedExpressionAstHasMetadata(read_ast.group_last_expression)) return error.UnsupportedSqlShape;
    } else if (read_ast.group_items.count != 0 or
        generatedExpressionAstHasMetadata(read_ast.group_first_expression) or
        generatedExpressionAstHasMetadata(read_ast.group_last_expression))
    {
        return error.UnsupportedSqlShape;
    }

    if (read_ast.having_tokens) |having_tokens| {
        if (having_tokens.start <= projection_tokens.end) return error.UnsupportedSqlShape;
        try validateGeneratedReadRangePrecededByKeyword(tokens, having_tokens, .having);
        try validateGeneratedPredicateExpressionMatchesRange(tokens, read_ast.having_expression, having_tokens);
    } else if (generatedExpressionAstHasMetadata(read_ast.having_expression)) {
        return error.UnsupportedSqlShape;
    }

    if (read_ast.window_tokens) |window_tokens| {
        if (window_tokens.start <= projection_tokens.end) return error.UnsupportedSqlShape;
        try validateGeneratedReadRangePrecededByKeyword(tokens, window_tokens, .window);
        if (read_ast.window_count == 0 or read_ast.window_items.len != read_ast.window_count) return error.UnsupportedSqlShape;
    } else if (read_ast.window_count != 0 or read_ast.window_items.len != 0) {
        return error.UnsupportedSqlShape;
    }

    if (read_ast.order_tokens) |order_tokens| {
        if (order_tokens.start <= projection_tokens.end) return error.UnsupportedSqlShape;
        try validateGeneratedReadOrderRange(tokens, order_tokens);
        if (read_ast.order_items.count == 0) return error.UnsupportedSqlShape;
        if (!generatedExpressionAstHasMetadata(read_ast.order_first_expression)) return error.UnsupportedSqlShape;
        if (!generatedExpressionAstHasMetadata(read_ast.order_last_expression)) return error.UnsupportedSqlShape;
    } else if (read_ast.order_items.count != 0 or
        generatedExpressionAstHasMetadata(read_ast.order_first_expression) or
        generatedExpressionAstHasMetadata(read_ast.order_last_expression))
    {
        return error.UnsupportedSqlShape;
    }

    if (read_ast.limit_tokens) |limit_tokens| {
        if (limit_tokens.start <= projection_tokens.end) return error.UnsupportedSqlShape;
        try validateGeneratedReadRangePrecededByKeyword(tokens, limit_tokens, .limit);
    }
    if (read_ast.offset_tokens) |offset_tokens| {
        if (offset_tokens.start <= projection_tokens.end) return error.UnsupportedSqlShape;
        try validateGeneratedReadRangePrecededByKeyword(tokens, offset_tokens, .offset);
    }
    if (read_ast.fetch_tokens) |fetch_tokens| {
        if (fetch_tokens.start <= projection_tokens.end) return error.UnsupportedSqlShape;
        try validateGeneratedReadRangePrecededByKeyword(tokens, fetch_tokens, .fetch);
    }
    if (read_ast.row_lock_tokens) |row_lock_tokens| {
        if (row_lock_tokens.start <= projection_tokens.end) return error.UnsupportedSqlShape;
        try validateGeneratedReadRowLockClauseLayout(tokens, read_ast.row_lock_tokens);
    }
    if (read_ast.set_operation_tokens) |set_operation_tokens| {
        if (set_operation_tokens.start <= projection_tokens.end) return error.UnsupportedSqlShape;
        try validateGeneratedSetOperationMetadata(read_ast.set_operation_tokens, read_ast.set_operation);
    } else if (generatedSetOperationAstHasMetadata(read_ast.set_operation)) {
        return error.UnsupportedSqlShape;
    }
    try validateGeneratedReadClauseOrder(projection_tokens.end, &.{
        read_ast.source_tokens,
        read_ast.where_tokens,
        read_ast.group_tokens,
        read_ast.having_tokens,
        read_ast.window_tokens,
        read_ast.set_operation_tokens,
        read_ast.order_tokens,
        read_ast.limit_tokens,
        read_ast.offset_tokens,
        read_ast.fetch_tokens,
        read_ast.row_lock_tokens,
    });
}

fn validateGeneratedReadSystemTimePayload(
    tokens: []const tokenized.Token,
    maybe_source: ?generated_parser.GeneratedSqlTokenRange,
    maybe_system_time: ?generated_parser.GeneratedSqlTokenRange,
    maybe_sequence: ?generated_parser.GeneratedSqlTokenRange,
) !void {
    const system_time = maybe_system_time orelse {
        if (maybe_sequence != null) return error.UnsupportedSqlShape;
        return;
    };
    const source = maybe_source orelse return error.UnsupportedSqlShape;
    const sequence = maybe_sequence orelse return error.UnsupportedSqlShape;
    if (system_time.start < source.start or system_time.end != source.end or system_time.end > tokens.len) return error.UnsupportedSqlShape;
    if (system_time.end != system_time.start + 5 and system_time.end != system_time.start + 6) return error.UnsupportedSqlShape;
    if (!tokens[system_time.start].matchesKeywordTag(.@"for")) return error.UnsupportedSqlShape;
    if (!std.ascii.eqlIgnoreCase(tokens[system_time.start + 1].text, "system_time")) return error.UnsupportedSqlShape;
    if (!tokens[system_time.start + 2].matchesKeywordTag(.as)) return error.UnsupportedSqlShape;
    if (!tokens[system_time.start + 3].matchesKeywordTag(.of)) return error.UnsupportedSqlShape;
    if (sequence.start != system_time.start + 4 or sequence.end != system_time.end) return error.UnsupportedSqlShape;
    try validateGeneratedReadSystemTimePayloadValue(tokens, sequence);
}

fn validateGeneratedReadSourceTableMetadata(
    tokens: []const tokenized.Token,
    maybe_source: ?generated_parser.GeneratedSqlTokenRange,
    maybe_source_table: ?generated_parser.GeneratedSqlTokenRange,
    maybe_source_alias: ?generated_parser.GeneratedSqlTokenRange,
    maybe_source_alias_name: ?generated_parser.GeneratedSqlTokenRange,
    maybe_source_system_time: ?generated_parser.GeneratedSqlTokenRange,
    maybe_trailing_source: ?generated_parser.GeneratedSqlTokenRange,
) !void {
    const source = maybe_source orelse {
        if (maybe_source_table != null or maybe_source_alias != null or maybe_source_alias_name != null or maybe_source_system_time != null or maybe_trailing_source != null) {
            return error.UnsupportedSqlShape;
        }
        return;
    };
    if (source.start >= source.end or source.end > tokens.len) return error.UnsupportedSqlShape;
    const source_body_end = if (maybe_source_system_time) |system_time| blk: {
        if (system_time.start <= source.start or system_time.end != source.end) return error.UnsupportedSqlShape;
        break :blk system_time.start;
    } else source.end;

    const source_table = maybe_source_table orelse {
        if (maybe_source_alias != null or maybe_source_alias_name != null or maybe_source_system_time != null) return error.UnsupportedSqlShape;
        if (generatedReadSourceLooksLikeSingleTableSource(tokens, .{ .start = source.start, .end = source_body_end })) {
            return error.UnsupportedSqlShape;
        }
        return;
    };

    var expected_table_start = source.start;
    if (tokens[expected_table_start].matchesKeywordTag(.only)) expected_table_start += 1;
    if (source_table.start != expected_table_start or
        source_table.end != expected_table_start + 1 or
        source_table.end > source_body_end or
        tokens[source_table.start].kind != .identifier)
    {
        return error.UnsupportedSqlShape;
    }

    const alias_end = try generatedReadSingleSourceAliasEnd(
        tokens,
        source_table,
        maybe_source_alias,
        maybe_source_alias_name,
    );
    if (maybe_trailing_source) |trailing| {
        if (trailing.start <= alias_end or trailing.end != source_body_end or trailing.end > tokens.len) return error.UnsupportedSqlShape;
        if (tokens[alias_end].kind != .comma or alias_end + 1 != trailing.start) return error.UnsupportedSqlShape;
    } else if (alias_end != source_body_end) {
        return error.UnsupportedSqlShape;
    }
}

fn generatedReadSingleSourceAliasEnd(
    tokens: []const tokenized.Token,
    source_table: generated_parser.GeneratedSqlTokenRange,
    source_alias_tokens: ?generated_parser.GeneratedSqlTokenRange,
    source_alias_name_tokens: ?generated_parser.GeneratedSqlTokenRange,
) !usize {
    if (source_table.start >= source_table.end or source_table.end > tokens.len) return error.UnsupportedSqlShape;
    const alias = source_alias_tokens orelse {
        if (source_alias_name_tokens != null) return error.UnsupportedSqlShape;
        return source_table.end;
    };
    const alias_name = source_alias_name_tokens orelse return error.UnsupportedSqlShape;
    if (alias.start != source_table.end or alias.start >= alias.end or alias.end > tokens.len) return error.UnsupportedSqlShape;

    const expected_name = if (alias.end == source_table.end + 2 and
        tokens[source_table.end].matchesKeywordTag(.as) and
        tokens[source_table.end + 1].kind == .identifier)
    blk: {
        break :blk generated_parser.GeneratedSqlTokenRange{ .start = source_table.end + 1, .end = source_table.end + 2 };
    } else if (alias.end == source_table.end + 1 and
        tokens[source_table.end].kind == .identifier and
        !plan.nextIsJoinClauseKeyword(tokens, source_table.end))
    blk: {
        break :blk alias;
    } else return error.UnsupportedSqlShape;

    if (!std.meta.eql(alias_name, expected_name)) return error.UnsupportedSqlShape;
    return alias.end;
}

fn generatedReadSourceLooksLikeSingleTableSource(
    tokens: []const tokenized.Token,
    source: generated_parser.GeneratedSqlTokenRange,
) bool {
    if (source.start >= source.end or source.end > tokens.len) return false;
    var table_start = source.start;
    if (tokens[table_start].matchesKeywordTag(.only)) table_start += 1;
    if (table_start >= source.end or tokens[table_start].kind != .identifier) return false;

    const table_end = table_start + 1;
    if (table_end == source.end) return true;
    if (table_end + 2 == source.end and
        tokens[table_end].matchesKeywordTag(.as) and
        tokens[table_end + 1].kind == .identifier)
    {
        return true;
    }
    if (table_end + 1 == source.end and
        tokens[table_end].kind == .identifier and
        !plan.nextIsJoinClauseKeyword(tokens, table_end))
    {
        return true;
    }
    return false;
}

fn validateGeneratedReadSystemTimePayloadValue(
    tokens: []const tokenized.Token,
    range: generated_parser.GeneratedSqlTokenRange,
) !void {
    if (range.start >= range.end or range.end > tokens.len) return error.UnsupportedSqlShape;
    if (range.end == range.start + 1) {
        switch (tokens[range.start].kind) {
            .number => if (std.mem.indexOfAny(u8, tokens[range.start].text, ".eE+-") != null) return error.UnsupportedSqlShape,
            .string => {},
            else => return error.UnsupportedSqlShape,
        }
        return;
    }
    if (range.end == range.start + 2 and tokens[range.start + 1].kind == .string) {
        if (!generatedReadSystemTimePayloadTypeToken(tokens[range.start])) return error.UnsupportedSqlShape;
        return;
    }
    return error.UnsupportedSqlShape;
}

fn generatedReadSystemTimePayloadTypeToken(token: tokenized.Token) bool {
    const keyword = token.keyword orelse return false;
    return keyword == .date or keyword == .timestamp or std.ascii.eqlIgnoreCase(token.text, "timestamptz");
}

fn validateGeneratedSimpleQueryReadAst(tokens: []const tokenized.Token, read_ast: *const generated_parser.GeneratedSqlReadAst) !void {
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

fn validateGeneratedAggregateReadAst(tokens: []const tokenized.Token, read_ast: *const generated_parser.GeneratedSqlReadAst) !void {
    if (read_ast.cte_tokens != null or read_ast.window_tokens != null or read_ast.set_operation_tokens != null) {
        return error.UnsupportedSqlShape;
    }
    if (read_ast.projection_tokens == null or read_ast.source_tokens == null) return error.UnsupportedSqlShape;
    const aggregate_projection = if (read_ast.projection_tokens) |projection|
        generatedReadRangeHasAggregateFunction(tokens, projection)
    else
        false;
    if (read_ast.group_tokens == null and read_ast.having_tokens == null and read_ast.distinct_tokens == null and !aggregate_projection) {
        return error.UnsupportedSqlShape;
    }
}

fn validateGeneratedJoinedReadAst(
    tokens: []const tokenized.Token,
    read_ast: *const generated_parser.GeneratedSqlReadAst,
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

fn validateGeneratedWindowReadAst(tokens: []const tokenized.Token, read_ast: *const generated_parser.GeneratedSqlReadAst) !void {
    if (read_ast.cte_tokens != null or read_ast.group_tokens != null or read_ast.having_tokens != null or
        read_ast.set_operation_tokens != null)
    {
        return error.UnsupportedSqlShape;
    }
    if (read_ast.projection_tokens == null or read_ast.source_tokens == null) return error.UnsupportedSqlShape;
    try validateGeneratedReadRangeContainsKeyword(tokens, read_ast.projection_tokens.?, .over);
    if (read_ast.window_tokens) |range| try validateGeneratedReadRangePrecededByKeyword(tokens, range, .window);
}

fn validateGeneratedSetOperationReadAst(tokens: []const tokenized.Token, read_ast: *const generated_parser.GeneratedSqlReadAst) !void {
    if (read_ast.cte_tokens != null) return error.UnsupportedSqlShape;
    if (read_ast.projection_tokens == null or read_ast.source_tokens == null or read_ast.set_operation_tokens == null) {
        return error.UnsupportedSqlShape;
    }
    if (read_ast.order_tokens) |range| try validateGeneratedReadOrderRange(tokens, range);
    if (read_ast.limit_tokens) |range| try validateGeneratedReadRangePrecededByKeyword(tokens, range, .limit);
    if (read_ast.offset_tokens) |range| try validateGeneratedReadRangePrecededByKeyword(tokens, range, .offset);
    if (read_ast.fetch_tokens) |range| try validateGeneratedReadRangePrecededByKeyword(tokens, range, .fetch);
}

fn validateGeneratedCteReadAst(tokens: []const tokenized.Token, read_ast: *const generated_parser.GeneratedSqlReadAst) !void {
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
    try validateGeneratedCteFinalReadKind(tokens, read_ast, try generatedCteFinalReadStatementKind(read_ast));
}

fn validateGeneratedCteFinalReadKind(
    tokens: []const tokenized.Token,
    read_ast: *const generated_parser.GeneratedSqlReadAst,
    final_read_kind: sql_statement_kind.SqlReadStatementKind,
) !void {
    if (read_ast.projection_tokens == null or read_ast.source_tokens == null) return error.UnsupportedSqlShape;
    switch (final_read_kind) {
        .query => {
            if (read_ast.group_tokens != null or read_ast.having_tokens != null or read_ast.window_tokens != null or
                read_ast.set_operation_tokens != null)
            {
                return error.UnsupportedSqlShape;
            }
            if (read_ast.source_tokens) |source| {
                try validateGeneratedReadRangeDoesNotContainKeyword(tokens, source, .join);
                try validateGeneratedReadRangeDoesNotContainKeyword(tokens, source, .lateral);
            }
            if (read_ast.projection_tokens) |projection| try validateGeneratedReadRangeDoesNotContainKeyword(tokens, projection, .over);
            if (read_ast.projection_tokens) |projection| {
                if (generatedReadRangeHasAggregateFunction(tokens, projection)) return error.UnsupportedSqlShape;
            }
        },
        .aggregate => {
            if (read_ast.window_tokens != null or read_ast.set_operation_tokens != null) return error.UnsupportedSqlShape;
            const aggregate_projection = if (read_ast.projection_tokens) |projection|
                generatedReadRangeHasAggregateFunction(tokens, projection)
            else
                false;
            if (read_ast.group_tokens == null and read_ast.having_tokens == null and read_ast.distinct_tokens == null and !aggregate_projection) {
                return error.UnsupportedSqlShape;
            }
        },
        .join => {
            if (read_ast.group_tokens != null or read_ast.having_tokens != null or read_ast.window_tokens != null or
                read_ast.set_operation_tokens != null)
            {
                return error.UnsupportedSqlShape;
            }
            if (read_ast.projection_tokens) |projection| {
                if (generatedReadRangeHasAggregateFunction(tokens, projection)) return error.UnsupportedSqlShape;
            }
            try validateGeneratedReadRangeContainsKeyword(tokens, read_ast.source_tokens.?, .join);
        },
        .lateral => {
            if (read_ast.group_tokens != null or read_ast.having_tokens != null or read_ast.window_tokens != null or
                read_ast.set_operation_tokens != null)
            {
                return error.UnsupportedSqlShape;
            }
            if (read_ast.join_items.len == 0 or read_ast.join_tokens == null) return error.UnsupportedSqlShape;
            if (read_ast.projection_tokens) |projection| {
                if (generatedReadRangeHasAggregateFunction(tokens, projection)) return error.UnsupportedSqlShape;
            }
            try validateGeneratedReadRangeContainsKeyword(tokens, read_ast.source_tokens.?, .join);
            try validateGeneratedReadRangeContainsKeyword(tokens, read_ast.source_tokens.?, .lateral);
        },
        .window => {
            if (read_ast.group_tokens != null or read_ast.having_tokens != null or read_ast.set_operation_tokens != null) {
                return error.UnsupportedSqlShape;
            }
            try validateGeneratedReadRangeContainsKeyword(tokens, read_ast.projection_tokens.?, .over);
        },
        .set_operation => {
            if (read_ast.set_operation_tokens == null) return error.UnsupportedSqlShape;
        },
        .recursive_cte => return error.UnsupportedSqlShape,
    }
}

fn lowerGeneratedCteReadPlanAlloc(
    context: *ReadPlanLoweringContext,
    parsed_sql: *const tokenized.ParsedSql,
    read_kind: sql_statement_kind.SqlReadStatementKind,
) !plan.LoweredReadPlan {
    return switch (read_kind) {
        .query => .{ .query = try context.callbacks.lower_query_plan(
            context.alloc,
            parsed_sql,
            context.schema,
            context.source_schema,
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
        .set_operation => blk: {
            break :blk .{ .set_operation = try context.callbacks.lower_set_operation_optional_source_schema(
                context.alloc,
                parsed_sql,
                context.schema,
                context.source_schema,
                context.params,
                context.function_bindings,
            ) };
        },
        .recursive_cte => .{ .recursive_cte = try context.callbacks.lower_recursive_cte_plan(
            context.alloc,
            parsed_sql,
            context.schema,
            context.params,
            context.function_bindings,
        ) },
    };
}

fn validateGeneratedReadStatementSpans(
    tokens: []const tokenized.Token,
    read_ast: *const generated_parser.GeneratedSqlReadAst,
) !void {
    const end = generatedReadStatementTokenEnd(tokens);
    if (end == 0) return error.UnsupportedSqlShape;
    const first = tokens[0];
    const last = tokens[end - 1];
    if (read_ast.statement_span.start != first.source_start or read_ast.statement_span.end != last.source_end) return error.UnsupportedSqlShape;

    const command_start = if (tokens[0].matchesKeywordTag(.with))
        parser_mod.findTopLevelKeywordTag(tokens[0..end], .select) orelse return error.UnsupportedSqlShape
    else
        0;
    const command = tokens[command_start];
    if (read_ast.command_span.start != command.source_start or read_ast.command_span.end != command.source_end) return error.UnsupportedSqlShape;
}

fn generatedReadStatementTokenEnd(tokens: []const tokenized.Token) usize {
    var end = tokens.len;
    while (end > 0 and tokens[end - 1].kind == .semicolon) end -= 1;
    return end;
}

fn validateGeneratedReadTokenRange(
    tokens: []const tokenized.Token,
    read_ast: *const generated_parser.GeneratedSqlReadAst,
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
    if (generatedReadRangeContainsKeyword(tokens, range, keyword)) return;
    return error.UnsupportedSqlShape;
}

fn generatedReadRangeContainsKeyword(
    tokens: []const tokenized.Token,
    range: generated_parser.GeneratedSqlTokenRange,
    keyword: token_mod.TokenKeyword,
) bool {
    var index = range.start;
    while (index < range.end) : (index += 1) {
        if (tokens[index].matchesKeywordTag(keyword)) return true;
    }
    return false;
}

fn generatedReadRangeHasAggregateFunction(
    tokens: []const tokenized.Token,
    range: generated_parser.GeneratedSqlTokenRange,
) bool {
    var depth: usize = 0;
    var index = range.start;
    while (index < range.end) : (index += 1) {
        const token = tokens[index];
        switch (token.kind) {
            .lparen, .lbracket => depth += 1,
            .rparen, .rbracket => {
                if (depth > 0) depth -= 1;
            },
            .identifier => if (depth == 0 and index + 1 < range.end and tokens[index + 1].kind == .lparen and generatedSqlAggregateFunctionName(token)) {
                return true;
            },
            else => {},
        }
    }
    return false;
}

fn generatedSqlAggregateFunctionName(token: tokenized.Token) bool {
    return token.matchesKeywordTag(.count) or
        token.matchesKeywordTag(.sum) or
        token.matchesKeywordTag(.avg) or
        token.matchesKeywordTag(.min) or
        token.matchesKeywordTag(.max) or
        token.matchesKeywordTag(.bool_or) or
        token.matchesKeywordTag(.bool_and) or
        token.matchesKeywordTag(.array_agg) or
        token.matchesKeywordTag(.string_agg) or
        token.matchesKeywordTag(.percentile_cont) or
        token.matchesKeywordTag(.percentile_disc) or
        token.matchesKeyword("mode");
}

fn validateGeneratedReadRangeDoesNotContainKeyword(
    tokens: []const tokenized.Token,
    range: generated_parser.GeneratedSqlTokenRange,
    keyword: token_mod.TokenKeyword,
) !void {
    var index = range.start;
    while (index < range.end) : (index += 1) {
        if (tokens[index].matchesKeywordTag(keyword)) return error.UnsupportedSqlShape;
    }
}

fn validateGeneratedReadRangePrecededByKeyword(
    tokens: []const tokenized.Token,
    range: generated_parser.GeneratedSqlTokenRange,
    keyword: token_mod.TokenKeyword,
) !void {
    if (range.start == 0 or !tokens[range.start - 1].matchesKeywordTag(keyword)) return error.UnsupportedSqlShape;
}

fn validateGeneratedPredicateExpressionMatchesRange(
    tokens: []const tokenized.Token,
    expression: generated_parser.GeneratedSqlExpressionAst,
    range: generated_parser.GeneratedSqlTokenRange,
) !void {
    if (!generatedExpressionAstHasMetadata(expression)) return error.UnsupportedSqlShape;
    try validateGeneratedExpressionAstStructure(expression);
    try validateGeneratedExpressionOperatorTokens(tokens, expression);
    try validateGeneratedExpressionOwnedTokenRanges(expression);
    const expression_tokens = expression.tokens orelse return error.UnsupportedSqlShape;
    if (!std.meta.eql(expression_tokens, range)) return error.UnsupportedSqlShape;
}

fn generatedReadListAstHasMetadata(list: generated_parser.GeneratedSqlListAst) bool {
    return list.count != 0 or
        list.items.len != 0 or
        list.expression_items.len != 0 or
        list.expressions.len != 0 or
        list.alias_items.len != 0 or
        list.alias_name_items.len != 0 or
        list.direction_items.len != 0 or
        list.directions.len != 0 or
        list.order_using_operator_items.len != 0 or
        list.nulls_order_items.len != 0 or
        list.nulls_orders.len != 0 or
        list.first_tokens != null or
        list.last_tokens != null;
}

fn generatedExpressionAstHasMetadata(expression: generated_parser.GeneratedSqlExpressionAst) bool {
    if (generatedExpressionAstHasScalarShapeMetadata(expression)) return true;
    if (expression.tokens != null or
        expression.inner_tokens != null or
        expression.function_name_tokens != null or
        expression.argument_tokens != null or
        expression.argument_distinct_tokens != null or
        expression.argument_value_tokens != null or
        expression.subquery_select_tokens != null or
        expression.subquery_projection_tokens != null or
        generatedReadListAstHasMetadata(expression.subquery_projection_items) or
        expression.subquery_source_tokens != null or
        expression.subquery_where_tokens != null or
        expression.subquery_set_operation_tokens != null or
        expression.subquery_tail != null or
        expression.argument_order_tokens != null or
        expression.within_group_tokens != null or
        expression.within_group_order_tokens != null or
        expression.filter_tokens != null or
        expression.filter_predicate_tokens != null or
        expression.over_tokens != null or
        expression.over_name_tokens != null or
        expression.over_definition_tokens != null or
        expression.over_partition_tokens != null or
        expression.over_order_tokens != null or
        expression.over_frame_tokens != null or
        expression.over_frame_start_expression_tokens != null or
        expression.over_frame_end_expression_tokens != null or
        expression.array_tokens != null or
        expression.cast_expression_tokens != null or
        expression.cast_type_tokens != null or
        expression.case_first_when_tokens != null or
        expression.case_last_when_tokens != null or
        expression.case_first_condition_tokens != null or
        expression.case_first_result_tokens != null or
        generatedReadListAstHasMetadata(expression.case_condition_items) or
        generatedReadListAstHasMetadata(expression.case_result_items) or
        expression.case_else_tokens != null or
        expression.case_else_expression_tokens != null or
        expression.boolean_first_condition_tokens != null or
        expression.boolean_last_condition_tokens != null or
        expression.interval_value_tokens != null or
        expression.timestamp_type_tokens != null or
        expression.timestamp_value_tokens != null or
        expression.current_timestamp_precision_tokens != null or
        expression.extract_field_tokens != null or
        expression.extract_source_tokens != null or
        expression.extract_source_expression != null or
        expression.left_tokens != null or
        expression.negation_tokens != null or
        expression.operator_tokens != null or
        expression.between_modifier_tokens != null or
        expression.between_lower_tokens != null or
        expression.between_upper_tokens != null or
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
        expression.over_frame_start_expression != null or
        expression.over_frame_end_expression != null or
        expression.between_lower_expression != null or
        expression.between_upper_expression != null or
        expression.case_first_condition != null or
        expression.case_first_result != null or
        expression.case_else_expression != null or
        expression.boolean_first_condition != null or
        expression.boolean_last_condition != null or
        expression.subquery_where_expression != null or
        expression.subquery_set_operation != null or
        expression.boolean_condition_count != 0 or
        generatedReadListAstHasMetadata(expression.boolean_condition_items) or
        generatedReadListAstHasMetadata(expression.argument_items) or
        generatedReadListAstHasMetadata(expression.argument_order_items) or
        generatedReadListAstHasMetadata(expression.within_group_order_items) or
        generatedReadListAstHasMetadata(expression.over_partition_items) or
        generatedReadListAstHasMetadata(expression.over_order_items) or
        generatedReadListAstHasMetadata(expression.array_items);
}

fn validateGeneratedExpressionAstRangesIfPresent(
    tokens: []const tokenized.Token,
    read_ast: *const generated_parser.GeneratedSqlReadAst,
    expression: generated_parser.GeneratedSqlExpressionAst,
) !void {
    if (!generatedExpressionAstHasMetadata(expression)) return;
    try validateGeneratedExpressionAstRanges(tokens, read_ast, expression);
}

fn generatedGraphTableFunctionKindForToken(token: tokenized.Token) ?generated_parser.GeneratedSqlGraphTableFunctionKind {
    return generatedGraphTableFunctionKindFromAntfly(generatedAntflyTableFunctionKindForToken(token) orelse return null);
}

fn generatedGraphTableFunctionKindFromAntfly(kind: generated_parser.GeneratedSqlAntflyTableFunctionKind) ?generated_parser.GeneratedSqlGraphTableFunctionKind {
    return switch (kind) {
        .graph_traverse => .traverse,
        .graph_neighbors => .neighbors,
        .graph_shortest_path => .shortest_path,
        .graph_k_shortest_paths => .k_shortest_paths,
        .graph_match => .match,
        .graph_metric => .metric,
        .graph_metric_rerank => .metric_rerank,
        else => null,
    };
}

fn generatedAntflyTableFunctionKindForToken(token: tokenized.Token) ?generated_parser.GeneratedSqlAntflyTableFunctionKind {
    if (token.matchesQualifiedKeywordTag("antfly", .full_text_search)) return .full_text_search;
    if (token.matchesQualifiedKeywordTag("antfly", .semantic_search)) return .semantic_search;
    if (token.matchesQualifiedKeywordTag("antfly", .vector_search)) return .vector_search;
    if (token.matchesQualifiedKeywordTag("antfly", .graph_traverse)) return .graph_traverse;
    if (token.matchesQualifiedKeywordTag("antfly", .graph_neighbors)) return .graph_neighbors;
    if (token.matchesQualifiedKeywordTag("antfly", .graph_shortest_path)) return .graph_shortest_path;
    if (token.matchesQualifiedKeywordTag("antfly", .graph_k_shortest_paths)) return .graph_k_shortest_paths;
    if (token.matchesQualifiedKeywordTag("antfly", .graph_match)) return .graph_match;
    if (token.matchesQualifiedKeywordTag("antfly", .graph_metric)) return .graph_metric;
    if (token.matchesQualifiedKeywordTag("antfly", .graph_metric_rerank)) return .graph_metric_rerank;
    if (token.matchesQualifiedKeywordTag("antfly", .hybrid_search)) return .hybrid_search;
    return null;
}

fn validateGeneratedAntflySourceMetadata(
    tokens: []const tokenized.Token,
    read_ast: *const generated_parser.GeneratedSqlReadAst,
) !void {
    try validateGeneratedAntflySourceItemsMetadata(tokens, read_ast.source_tokens, read_ast.source_antfly_function_items, read_ast.source_antfly_function_count);
}

fn validateGeneratedAntflySourceItemsMetadata(
    tokens: []const tokenized.Token,
    maybe_source_tokens: ?generated_parser.GeneratedSqlTokenRange,
    items: []const generated_parser.GeneratedSqlAntflyTableFunctionAst,
    count: usize,
) !void {
    if (items.len != count) return error.UnsupportedSqlShape;
    const source_tokens = maybe_source_tokens orelse {
        if (items.len == 0 and count == 0) return;
        return error.UnsupportedSqlShape;
    };
    var previous_end: usize = source_tokens.start;
    for (items) |item| {
        if (item.tokens.start < previous_end) return error.UnsupportedSqlShape;
        try validateGeneratedAntflySourceItemMetadata(tokens, source_tokens, item);
        previous_end = item.tokens.end;
    }
}

fn validateGeneratedAntflySourceItemMetadata(
    tokens: []const tokenized.Token,
    source_tokens: generated_parser.GeneratedSqlTokenRange,
    item: generated_parser.GeneratedSqlAntflyTableFunctionAst,
) !void {
    const function_tokens = item.tokens;
    const name_tokens = item.name_tokens;
    const argument_tokens = item.argument_tokens;
    if (function_tokens.start < source_tokens.start or function_tokens.end > source_tokens.end) return error.UnsupportedSqlShape;
    if (function_tokens.end > tokens.len or function_tokens.end < function_tokens.start + 3) return error.UnsupportedSqlShape;
    if (!std.meta.eql(name_tokens, generated_parser.GeneratedSqlTokenRange{ .start = function_tokens.start, .end = function_tokens.start + 1 })) return error.UnsupportedSqlShape;
    if (!std.meta.eql(argument_tokens, generated_parser.GeneratedSqlTokenRange{ .start = function_tokens.start + 2, .end = function_tokens.end - 1 })) return error.UnsupportedSqlShape;
    if (tokens[function_tokens.start + 1].kind != .lparen or tokens[function_tokens.end - 1].kind != .rparen) return error.UnsupportedSqlShape;
    if (generatedAntflyTableFunctionKindForToken(tokens[name_tokens.start]) != item.kind) return error.UnsupportedSqlShape;
    try validateGeneratedAntflySourceArgumentItems(tokens, item);
}

fn validateGeneratedAntflySourceArgumentItems(
    tokens: []const tokenized.Token,
    item: generated_parser.GeneratedSqlAntflyTableFunctionAst,
) !void {
    if (item.argument_items.len != item.argument_count) return error.UnsupportedSqlShape;
    if (item.argument_items.len == 0) {
        if (item.argument_tokens.start != item.argument_tokens.end) return error.UnsupportedSqlShape;
        return;
    }
    var expected_start: usize = item.argument_tokens.start;
    for (item.argument_items, 0..) |argument, index| {
        if (argument.tokens.start != expected_start) return error.UnsupportedSqlShape;
        if (argument.tokens.start < item.argument_tokens.start or argument.tokens.end > item.argument_tokens.end) return error.UnsupportedSqlShape;
        if (!std.meta.eql(argument.name_tokens, generated_parser.GeneratedSqlTokenRange{ .start = argument.tokens.start, .end = argument.tokens.start + 1 })) return error.UnsupportedSqlShape;
        if (argument.operator_tokens.start != argument.name_tokens.end or argument.operator_tokens.end > argument.value_tokens.start) return error.UnsupportedSqlShape;
        if (argument.value_tokens.start < argument.operator_tokens.end or argument.value_tokens.end != argument.tokens.end) return error.UnsupportedSqlShape;
        if (argument.value_tokens.start >= argument.value_tokens.end) return error.UnsupportedSqlShape;
        if (tokens[argument.name_tokens.start].kind != .identifier) return error.UnsupportedSqlShape;
        if (tokens[argument.operator_tokens.start].kind != .eq) return error.UnsupportedSqlShape;
        if (argument.operator_tokens.end == argument.operator_tokens.start + 2 and tokens[argument.operator_tokens.start + 1].kind != .gt) return error.UnsupportedSqlShape;
        if (argument.operator_tokens.end != argument.operator_tokens.start + 1 and argument.operator_tokens.end != argument.operator_tokens.start + 2) return error.UnsupportedSqlShape;
        for (item.argument_items[0..index]) |previous| {
            if (std.ascii.eqlIgnoreCase(tokens[previous.name_tokens.start].text, tokens[argument.name_tokens.start].text)) {
                return error.UnsupportedSqlShape;
            }
        }
        if (index + 1 < item.argument_items.len) {
            if (argument.tokens.end >= item.argument_tokens.end or tokens[argument.tokens.end].kind != .comma) return error.UnsupportedSqlShape;
            expected_start = argument.tokens.end + 1;
        } else {
            expected_start = argument.tokens.end;
        }
    }
    if (expected_start != item.argument_tokens.end) return error.UnsupportedSqlShape;
}

fn generatedGraphSourceItemMatchesAntflyItem(
    graph_item: generated_parser.GeneratedSqlGraphTableFunctionAst,
    antfly_item: generated_parser.GeneratedSqlAntflyTableFunctionAst,
) bool {
    const antfly_graph_kind = generatedGraphTableFunctionKindFromAntfly(antfly_item.kind) orelse return false;
    return graph_item.kind == antfly_graph_kind and
        std.meta.eql(graph_item.tokens, antfly_item.tokens) and
        std.meta.eql(graph_item.name_tokens, antfly_item.name_tokens) and
        std.meta.eql(graph_item.argument_tokens, antfly_item.argument_tokens);
}

fn generatedAntflyArgumentNameMatches(
    tokens: []const tokenized.Token,
    argument: generated_parser.GeneratedSqlNamedArgumentAst,
    names: []const []const u8,
) bool {
    if (argument.name_tokens.end != argument.name_tokens.start + 1) return false;
    const name = tokens[argument.name_tokens.start].text;
    for (names) |expected| {
        if (std.ascii.eqlIgnoreCase(name, expected)) return true;
    }
    return false;
}

fn generatedAntflyArgumentValueByNames(
    tokens: []const tokenized.Token,
    item: generated_parser.GeneratedSqlAntflyTableFunctionAst,
    names: []const []const u8,
) !?generated_parser.GeneratedSqlTokenRange {
    var value: ?generated_parser.GeneratedSqlTokenRange = null;
    for (item.argument_items) |argument| {
        if (!generatedAntflyArgumentNameMatches(tokens, argument, names)) continue;
        if (value != null) return error.UnsupportedSqlShape;
        value = argument.value_tokens;
    }
    return value;
}

fn generatedOptionalTokenRangeEquals(
    left: ?generated_parser.GeneratedSqlTokenRange,
    right: ?generated_parser.GeneratedSqlTokenRange,
) bool {
    if (left == null and right == null) return true;
    if (left == null or right == null) return false;
    return std.meta.eql(left.?, right.?);
}

fn validateGeneratedGraphSemanticValue(
    actual: ?generated_parser.GeneratedSqlTokenRange,
    expected: ?generated_parser.GeneratedSqlTokenRange,
) !void {
    if (!generatedOptionalTokenRangeEquals(actual, expected)) return error.UnsupportedSqlShape;
}

fn validateGeneratedGraphRequiredValue(
    value: ?generated_parser.GeneratedSqlTokenRange,
) !void {
    if (value == null) return error.UnsupportedSqlShape;
}

fn validateGeneratedGraphRequiredEitherValue(
    left: ?generated_parser.GeneratedSqlTokenRange,
    right: ?generated_parser.GeneratedSqlTokenRange,
) !void {
    if (left == null and right == null) return error.UnsupportedSqlShape;
}

fn validateGeneratedGraphSourceSemanticMetadata(
    tokens: []const tokenized.Token,
    graph_item: generated_parser.GeneratedSqlGraphTableFunctionAst,
    antfly_item: generated_parser.GeneratedSqlAntflyTableFunctionAst,
) !void {
    if (graph_item.argument_count != antfly_item.argument_count) return error.UnsupportedSqlShape;
    try validateGeneratedGraphSemanticValue(graph_item.table_name_value_tokens, try generatedAntflyArgumentValueByNames(tokens, antfly_item, &.{ "table_name", "table" }));
    try validateGeneratedGraphSemanticValue(graph_item.index_value_tokens, try generatedAntflyArgumentValueByNames(tokens, antfly_item, &.{ "index", "graph_index" }));
    try validateGeneratedGraphSemanticValue(graph_item.start_value_tokens, try generatedAntflyArgumentValueByNames(tokens, antfly_item, &.{ "start", "start_node" }));
    try validateGeneratedGraphSemanticValue(graph_item.start_result_ref_value_tokens, try generatedAntflyArgumentValueByNames(tokens, antfly_item, &.{ "start_result_ref", "result_ref" }));
    try validateGeneratedGraphSemanticValue(graph_item.target_value_tokens, try generatedAntflyArgumentValueByNames(tokens, antfly_item, &.{ "target", "target_node" }));
    try validateGeneratedGraphSemanticValue(graph_item.target_result_ref_value_tokens, try generatedAntflyArgumentValueByNames(tokens, antfly_item, &.{"target_result_ref"}));
    try validateGeneratedGraphSemanticValue(graph_item.pattern_value_tokens, try generatedAntflyArgumentValueByNames(tokens, antfly_item, &.{"pattern"}));
    try validateGeneratedGraphSemanticValue(graph_item.return_value_tokens, try generatedAntflyArgumentValueByNames(tokens, antfly_item, &.{ "return", "return_aliases" }));
    try validateGeneratedGraphSemanticValue(graph_item.metric_value_tokens, try generatedAntflyArgumentValueByNames(tokens, antfly_item, &.{ "metric", "graph_metric" }));
    try validateGeneratedGraphSemanticValue(graph_item.query_value_tokens, try generatedAntflyArgumentValueByNames(tokens, antfly_item, &.{ "query", "text" }));

    switch (graph_item.kind) {
        .traverse, .neighbors => {
            try validateGeneratedGraphRequiredValue(graph_item.index_value_tokens);
            try validateGeneratedGraphRequiredEitherValue(graph_item.start_value_tokens, graph_item.start_result_ref_value_tokens);
        },
        .shortest_path, .k_shortest_paths => {
            try validateGeneratedGraphRequiredValue(graph_item.index_value_tokens);
            try validateGeneratedGraphRequiredEitherValue(graph_item.start_value_tokens, graph_item.start_result_ref_value_tokens);
            try validateGeneratedGraphRequiredEitherValue(graph_item.target_value_tokens, graph_item.target_result_ref_value_tokens);
        },
        .match => {
            try validateGeneratedGraphRequiredValue(graph_item.index_value_tokens);
            try validateGeneratedGraphRequiredEitherValue(graph_item.start_value_tokens, graph_item.start_result_ref_value_tokens);
            try validateGeneratedGraphRequiredValue(graph_item.pattern_value_tokens);
        },
        .metric, .metric_rerank => {
            try validateGeneratedGraphRequiredValue(graph_item.index_value_tokens);
            try validateGeneratedGraphRequiredValue(graph_item.metric_value_tokens);
        },
    }
}

fn validateGeneratedGraphSourceMetadata(
    tokens: []const tokenized.Token,
    read_ast: *const generated_parser.GeneratedSqlReadAst,
) !void {
    if (read_ast.source_graph_function_items.len != read_ast.source_graph_function_count) return error.UnsupportedSqlShape;
    if (read_ast.source_graph_function_items.len == 0) {
        if (read_ast.source_graph_function_count != 0 or
            read_ast.source_graph_function_tokens != null or
            read_ast.source_graph_function_name_tokens != null or
            read_ast.source_graph_function_argument_tokens != null or
            read_ast.source_graph_function_kind != null)
        {
            return error.UnsupportedSqlShape;
        }
        return;
    }

    const first = read_ast.source_graph_function_items[0];
    const function_tokens = read_ast.source_graph_function_tokens orelse return error.UnsupportedSqlShape;
    const name_tokens = read_ast.source_graph_function_name_tokens orelse return error.UnsupportedSqlShape;
    const argument_tokens = read_ast.source_graph_function_argument_tokens orelse return error.UnsupportedSqlShape;
    const kind = read_ast.source_graph_function_kind orelse return error.UnsupportedSqlShape;
    if (!std.meta.eql(function_tokens, first.tokens) or
        !std.meta.eql(name_tokens, first.name_tokens) or
        !std.meta.eql(argument_tokens, first.argument_tokens) or
        kind != first.kind)
    {
        return error.UnsupportedSqlShape;
    }

    try validateGeneratedGraphSourceItemsMetadata(
        tokens,
        read_ast.source_tokens,
        read_ast.source_antfly_function_items,
        read_ast.source_graph_function_items,
        read_ast.source_graph_function_count,
    );
}

fn validateGeneratedGraphSourceItemsMetadata(
    tokens: []const tokenized.Token,
    maybe_source_tokens: ?generated_parser.GeneratedSqlTokenRange,
    antfly_items: []const generated_parser.GeneratedSqlAntflyTableFunctionAst,
    graph_items: []const generated_parser.GeneratedSqlGraphTableFunctionAst,
    count: usize,
) !void {
    if (graph_items.len != count) return error.UnsupportedSqlShape;
    const source_tokens = maybe_source_tokens orelse {
        if (graph_items.len == 0 and count == 0) return;
        return error.UnsupportedSqlShape;
    };

    var previous_end: usize = source_tokens.start;
    for (graph_items) |item| {
        if (item.tokens.start < previous_end) return error.UnsupportedSqlShape;
        try validateGeneratedGraphSourceItemMetadata(tokens, source_tokens, item);
        var matching_antfly_item: ?generated_parser.GeneratedSqlAntflyTableFunctionAst = null;
        for (antfly_items) |antfly_item| {
            if (!generatedGraphSourceItemMatchesAntflyItem(item, antfly_item)) continue;
            matching_antfly_item = antfly_item;
            break;
        }
        const antfly_item = matching_antfly_item orelse return error.UnsupportedSqlShape;
        try validateGeneratedGraphSourceSemanticMetadata(tokens, item, antfly_item);
        previous_end = item.tokens.end;
    }
}

fn validateGeneratedGraphSourceItemMetadata(
    tokens: []const tokenized.Token,
    source_tokens: generated_parser.GeneratedSqlTokenRange,
    item: generated_parser.GeneratedSqlGraphTableFunctionAst,
) !void {
    const function_tokens = item.tokens;
    const name_tokens = item.name_tokens;
    const argument_tokens = item.argument_tokens;
    const kind = item.kind;
    if (function_tokens.start < source_tokens.start or function_tokens.end > source_tokens.end) return error.UnsupportedSqlShape;
    if (function_tokens.end > tokens.len or function_tokens.end < function_tokens.start + 3) return error.UnsupportedSqlShape;
    if (!std.meta.eql(name_tokens, generated_parser.GeneratedSqlTokenRange{ .start = function_tokens.start, .end = function_tokens.start + 1 })) return error.UnsupportedSqlShape;
    if (!std.meta.eql(argument_tokens, generated_parser.GeneratedSqlTokenRange{ .start = function_tokens.start + 2, .end = function_tokens.end - 1 })) return error.UnsupportedSqlShape;
    if (tokens[function_tokens.start + 1].kind != .lparen or tokens[function_tokens.end - 1].kind != .rparen) return error.UnsupportedSqlShape;
    if (generatedGraphTableFunctionKindForToken(tokens[name_tokens.start]) != kind) return error.UnsupportedSqlShape;
}

fn validateGeneratedOptionalExpressionAstContainedByRange(
    tokens: []const tokenized.Token,
    read_ast: *const generated_parser.GeneratedSqlReadAst,
    range: generated_parser.GeneratedSqlTokenRange,
    expression: generated_parser.GeneratedSqlExpressionAst,
) !void {
    if (!generatedExpressionAstHasMetadata(expression)) return error.UnsupportedSqlShape;
    try validateGeneratedExpressionAstRanges(tokens, read_ast, expression);
    const expression_tokens = expression.tokens orelse return error.UnsupportedSqlShape;
    if (expression_tokens.start < range.start or expression_tokens.end > range.end) return error.UnsupportedSqlShape;
}

fn validateGeneratedOffsetExpressionAstMatchesRange(
    tokens: []const tokenized.Token,
    read_ast: *const generated_parser.GeneratedSqlReadAst,
    range: generated_parser.GeneratedSqlTokenRange,
    expression: generated_parser.GeneratedSqlExpressionAst,
) !void {
    try validateGeneratedOptionalExpressionAstContainedByRange(tokens, read_ast, range, expression);
    const expression_tokens = expression.tokens orelse return error.UnsupportedSqlShape;
    if (expression_tokens.start != range.start) return error.UnsupportedSqlShape;
    if (expression_tokens.end == range.end) return;
    if (expression_tokens.end + 1 != range.end) return error.UnsupportedSqlShape;
    if (!tokens[expression_tokens.end].matchesKeywordTag(.row) and !tokens[expression_tokens.end].matchesKeywordTag(.rows)) {
        return error.UnsupportedSqlShape;
    }
}

fn validateGeneratedReadPaginationPayloads(
    tokens: []const tokenized.Token,
    read_ast: *const generated_parser.GeneratedSqlReadAst,
    limit_tokens: ?generated_parser.GeneratedSqlTokenRange,
    limit_expression: ?*const generated_parser.GeneratedSqlExpressionAst,
    limit_all: bool,
    offset_tokens: ?generated_parser.GeneratedSqlTokenRange,
    offset_expression: ?*const generated_parser.GeneratedSqlExpressionAst,
    fetch_tokens: ?generated_parser.GeneratedSqlTokenRange,
    fetch_count_tokens: ?generated_parser.GeneratedSqlTokenRange,
    fetch_count_expression: ?*const generated_parser.GeneratedSqlExpressionAst,
) !void {
    if (limit_tokens) |range| {
        if (limit_all) {
            try validateGeneratedReadLimitAllRangeLayout(tokens, range);
            if (limit_expression) |expression| {
                if (generatedExpressionAstHasMetadata(expression.*)) return error.UnsupportedSqlShape;
            }
        } else {
            const expression = limit_expression orelse return error.UnsupportedSqlShape;
            try validateGeneratedOptionalExpressionAstContainedByRange(tokens, read_ast, range, expression.*);
            const expression_tokens = expression.tokens orelse return error.UnsupportedSqlShape;
            if (!std.meta.eql(expression_tokens, range)) return error.UnsupportedSqlShape;
        }
    } else if (limit_all or (limit_expression != null and generatedExpressionAstHasMetadata(limit_expression.?.*))) {
        return error.UnsupportedSqlShape;
    }

    if (offset_tokens) |range| {
        const expression = offset_expression orelse return error.UnsupportedSqlShape;
        try validateGeneratedOffsetExpressionAstMatchesRange(tokens, read_ast, range, expression.*);
    } else if (offset_expression != null and generatedExpressionAstHasMetadata(offset_expression.?.*)) {
        return error.UnsupportedSqlShape;
    }

    if (fetch_tokens) |range| {
        try validateGeneratedReadFetchRangeLayout(tokens, range, fetch_count_tokens);
        if (fetch_count_tokens) |count_tokens| {
            if (count_tokens.start < range.start or count_tokens.end > range.end) return error.UnsupportedSqlShape;
            const expression = fetch_count_expression orelse return error.UnsupportedSqlShape;
            try validateGeneratedOptionalExpressionAstContainedByRange(tokens, read_ast, count_tokens, expression.*);
            const expression_tokens = expression.tokens orelse return error.UnsupportedSqlShape;
            if (!std.meta.eql(expression_tokens, count_tokens)) return error.UnsupportedSqlShape;
        } else if (fetch_count_expression != null and generatedExpressionAstHasMetadata(fetch_count_expression.?.*)) {
            return error.UnsupportedSqlShape;
        }
    } else if (fetch_count_tokens != null or (fetch_count_expression != null and generatedExpressionAstHasMetadata(fetch_count_expression.?.*))) {
        return error.UnsupportedSqlShape;
    }
}

fn validateGeneratedReadLimitAllRangeLayout(
    tokens: []const tokenized.Token,
    range: generated_parser.GeneratedSqlTokenRange,
) !void {
    if (range.end != range.start + 1 or range.end > tokens.len or !tokens[range.start].matchesKeywordTag(.all)) {
        return error.UnsupportedSqlShape;
    }
}

fn validateGeneratedReadFetchRangeLayout(
    tokens: []const tokenized.Token,
    range: generated_parser.GeneratedSqlTokenRange,
    fetch_count_tokens: ?generated_parser.GeneratedSqlTokenRange,
) !void {
    if (range.start + 3 > range.end or range.end > tokens.len) return error.UnsupportedSqlShape;
    if (!tokens[range.start].matchesKeywordTag(.first) and !tokens[range.start].matchesKeywordTag(.next)) return error.UnsupportedSqlShape;
    if (!tokens[range.end - 1].matchesKeywordTag(.only)) return error.UnsupportedSqlShape;
    const row_index = range.end - 2;
    if (!tokens[row_index].matchesKeywordTag(.row) and !tokens[row_index].matchesKeywordTag(.rows)) return error.UnsupportedSqlShape;
    if (fetch_count_tokens) |count_tokens| {
        if (count_tokens.start != range.start + 1 or count_tokens.end != row_index) return error.UnsupportedSqlShape;
    } else if (row_index != range.start + 1) {
        return error.UnsupportedSqlShape;
    }
}

fn validateGeneratedReadRowLockClauseLayout(
    tokens: []const tokenized.Token,
    range: ?generated_parser.GeneratedSqlTokenRange,
) !void {
    const lock_tokens = range orelse return;
    if (lock_tokens.start >= lock_tokens.end or lock_tokens.end > tokens.len) return error.UnsupportedSqlShape;
    if (!tokens[lock_tokens.start].matchesKeywordTag(.@"for")) return error.UnsupportedSqlShape;

    var cursor = lock_tokens.start + 1;
    if (cursor >= lock_tokens.end) return error.UnsupportedSqlShape;
    if (tokens[cursor].matchesKeywordTag(.update) or tokens[cursor].matchesKeywordTag(.share)) {
        cursor += 1;
    } else if (cursor + 2 < lock_tokens.end and
        tokens[cursor].matchesKeywordTag(.no) and
        tokens[cursor + 1].matchesKeywordTag(.key) and
        tokens[cursor + 2].matchesKeywordTag(.update))
    {
        cursor += 3;
    } else if (cursor + 1 < lock_tokens.end and
        tokens[cursor].matchesKeywordTag(.key) and
        tokens[cursor + 1].matchesKeywordTag(.share))
    {
        cursor += 2;
    } else {
        return error.UnsupportedSqlShape;
    }

    if (cursor < lock_tokens.end and tokens[cursor].matchesKeywordTag(.of)) {
        cursor += 1;
        if (cursor >= lock_tokens.end) return error.UnsupportedSqlShape;
        var saw_target = false;
        var previous_was_comma = true;
        while (cursor < lock_tokens.end and
            !tokens[cursor].matchesKeywordTag(.nowait) and
            !tokens[cursor].matchesKeywordTag(.skip))
        {
            if (tokens[cursor].kind == .comma) {
                if (previous_was_comma) return error.UnsupportedSqlShape;
                previous_was_comma = true;
            } else {
                saw_target = true;
                previous_was_comma = false;
            }
            cursor += 1;
        }
        if (!saw_target or previous_was_comma) return error.UnsupportedSqlShape;
    }

    if (cursor < lock_tokens.end) {
        if (tokens[cursor].matchesKeywordTag(.nowait)) {
            cursor += 1;
        } else if (cursor + 1 < lock_tokens.end and
            tokens[cursor].matchesKeywordTag(.skip) and
            tokens[cursor + 1].matchesKeywordTag(.locked))
        {
            cursor += 2;
        } else {
            return error.UnsupportedSqlShape;
        }
    }

    if (cursor != lock_tokens.end) return error.UnsupportedSqlShape;
}

fn validateGeneratedReadWindowFrameClauseLayout(
    tokens: []const tokenized.Token,
    frame_tokens: generated_parser.GeneratedSqlTokenRange,
    start_expression_kind: ?generated_parser.GeneratedSqlExpressionKind,
    start_expression_tokens: ?generated_parser.GeneratedSqlTokenRange,
    start_expression: ?*generated_parser.GeneratedSqlExpressionAst,
    end_expression_kind: ?generated_parser.GeneratedSqlExpressionKind,
    end_expression_tokens: ?generated_parser.GeneratedSqlTokenRange,
    end_expression: ?*generated_parser.GeneratedSqlExpressionAst,
) !void {
    if (frame_tokens.start + 1 >= frame_tokens.end or frame_tokens.end > tokens.len) return error.UnsupportedSqlShape;
    if (!tokens[frame_tokens.start].matchesKeywordTag(.rows) and !tokens[frame_tokens.start].matchesKeywordTag(.range)) {
        return error.UnsupportedSqlShape;
    }

    var cursor = frame_tokens.start + 1;
    if (tokens[cursor].matchesKeywordTag(.between)) {
        cursor = try validateGeneratedReadWindowFrameBoundLayout(
            tokens,
            cursor + 1,
            frame_tokens.end,
            start_expression_kind,
            start_expression_tokens,
            start_expression,
        );
        if (cursor >= frame_tokens.end or !tokens[cursor].matchesKeywordTag(.@"and")) return error.UnsupportedSqlShape;
        cursor = try validateGeneratedReadWindowFrameBoundLayout(
            tokens,
            cursor + 1,
            frame_tokens.end,
            end_expression_kind,
            end_expression_tokens,
            end_expression,
        );
    } else {
        cursor = try validateGeneratedReadWindowFrameBoundLayout(
            tokens,
            cursor,
            frame_tokens.end,
            start_expression_kind,
            start_expression_tokens,
            start_expression,
        );
        if (end_expression_kind != null or end_expression_tokens != null or end_expression != null) return error.UnsupportedSqlShape;
    }
    if (cursor != frame_tokens.end) return error.UnsupportedSqlShape;
}

fn validateGeneratedReadWindowFrameBoundLayout(
    tokens: []const tokenized.Token,
    start: usize,
    end: usize,
    expression_kind: ?generated_parser.GeneratedSqlExpressionKind,
    expression_tokens: ?generated_parser.GeneratedSqlTokenRange,
    expression: ?*generated_parser.GeneratedSqlExpressionAst,
) !usize {
    if (start >= end or end > tokens.len) return error.UnsupportedSqlShape;

    if (tokens[start].matchesKeywordTag(.unbounded)) {
        if (expression_kind != null or expression_tokens != null or expression != null) return error.UnsupportedSqlShape;
        if (start + 1 >= end) return error.UnsupportedSqlShape;
        if (!tokens[start + 1].matchesKeywordTag(.preceding) and !tokens[start + 1].matchesKeywordTag(.following)) {
            return error.UnsupportedSqlShape;
        }
        return start + 2;
    }
    if (tokens[start].matchesKeywordTag(.current)) {
        if (expression_kind != null or expression_tokens != null or expression != null) return error.UnsupportedSqlShape;
        if (start + 1 >= end or !tokens[start + 1].matchesKeywordTag(.row)) return error.UnsupportedSqlShape;
        return start + 2;
    }

    const value_tokens = expression_tokens orelse return error.UnsupportedSqlShape;
    if (value_tokens.start != start or value_tokens.start >= value_tokens.end or value_tokens.end >= end) return error.UnsupportedSqlShape;
    try requireGeneratedExpressionAstChild(expression_kind, value_tokens, expression);
    if (!tokens[value_tokens.end].matchesKeywordTag(.preceding) and !tokens[value_tokens.end].matchesKeywordTag(.following)) {
        return error.UnsupportedSqlShape;
    }
    return value_tokens.end + 1;
}

fn validateGeneratedExpressionAstChild(
    expected_kind: ?generated_parser.GeneratedSqlExpressionKind,
    expected_tokens: ?generated_parser.GeneratedSqlTokenRange,
    child: *const generated_parser.GeneratedSqlExpressionAst,
) !void {
    if (expected_kind) |kind| {
        if (child.kind != kind) return error.UnsupportedSqlShape;
    } else if (child.kind != .token_range) {
        return error.UnsupportedSqlShape;
    }
    if (expected_tokens) |tokens| {
        const child_tokens = child.tokens orelse return error.UnsupportedSqlShape;
        if (!std.meta.eql(child_tokens, tokens)) return error.UnsupportedSqlShape;
    }
}

fn requireGeneratedExpressionAstChild(
    expected_kind: ?generated_parser.GeneratedSqlExpressionKind,
    expected_tokens: ?generated_parser.GeneratedSqlTokenRange,
    child: ?*generated_parser.GeneratedSqlExpressionAst,
) !void {
    const required_child = child orelse return error.UnsupportedSqlShape;
    try validateGeneratedExpressionAstChild(expected_kind, expected_tokens, required_child);
}

fn validateGeneratedExpressionAstOptionalChild(
    expected_kind: ?generated_parser.GeneratedSqlExpressionKind,
    expected_tokens: ?generated_parser.GeneratedSqlTokenRange,
    child: ?*generated_parser.GeneratedSqlExpressionAst,
) !void {
    if (child) |value| {
        try validateGeneratedExpressionAstChild(expected_kind, expected_tokens, value);
    } else if (expected_kind != null or expected_tokens != null) {
        return error.UnsupportedSqlShape;
    }
}

fn generatedExpressionAstHasScalarShapeMetadata(expression: generated_parser.GeneratedSqlExpressionAst) bool {
    return expression.inner_expression_kind != null or
        expression.subquery_read_kind != null or
        expression.subquery_where_expression_kind != null or
        expression.filter_expression_kind != null or
        expression.over_frame_start_expression_kind != null or
        expression.over_frame_end_expression_kind != null or
        expression.cast_expression_kind != null or
        expression.case_branch_count != 0 or
        expression.case_first_condition_kind != null or
        expression.case_first_result_kind != null or
        expression.case_else_expression_kind != null or
        expression.boolean_condition_count != 0 or
        expression.boolean_first_condition_kind != null or
        expression.boolean_last_condition_kind != null or
        expression.extract_source_expression_kind != null or
        expression.left_expression_kind != null or
        expression.between_modifier != null or
        expression.between_lower_expression_kind != null or
        expression.between_upper_expression_kind != null or
        expression.right_expression_kind != null or
        expression.escape_expression_kind != null;
}

fn validateGeneratedExpressionAstHasNoUnexpectedPayload(expression: generated_parser.GeneratedSqlExpressionAst) !void {
    if (generatedExpressionAstHasMetadata(expression) or generatedExpressionAstHasScalarShapeMetadata(expression)) {
        return error.UnsupportedSqlShape;
    }
}

fn validateGeneratedTokenRangeExpressionAstStructure(expression: generated_parser.GeneratedSqlExpressionAst) !void {
    if (expression.tokens == null) return error.UnsupportedSqlShape;
    var payload = expression;
    payload.tokens = null;
    try validateGeneratedExpressionAstHasNoUnexpectedPayload(payload);
}

fn validateGeneratedIntervalExpressionAstStructure(expression: generated_parser.GeneratedSqlExpressionAst) !void {
    if (expression.tokens == null or expression.interval_value_tokens == null) return error.UnsupportedSqlShape;
    var payload = expression;
    payload.tokens = null;
    payload.interval_value_tokens = null;
    try validateGeneratedExpressionAstHasNoUnexpectedPayload(payload);
}

fn validateGeneratedTimestampExpressionAstStructure(expression: generated_parser.GeneratedSqlExpressionAst) !void {
    if (expression.tokens == null or expression.timestamp_type_tokens == null or expression.timestamp_value_tokens == null) {
        return error.UnsupportedSqlShape;
    }
    var payload = expression;
    payload.tokens = null;
    payload.timestamp_type_tokens = null;
    payload.timestamp_value_tokens = null;
    try validateGeneratedExpressionAstHasNoUnexpectedPayload(payload);
}

fn validateGeneratedCurrentDateExpressionAstStructure(expression: generated_parser.GeneratedSqlExpressionAst) !void {
    if (expression.tokens == null) return error.UnsupportedSqlShape;
    var payload = expression;
    payload.tokens = null;
    try validateGeneratedExpressionAstHasNoUnexpectedPayload(payload);
}

fn validateGeneratedCurrentTimestampExpressionAstStructure(expression: generated_parser.GeneratedSqlExpressionAst) !void {
    if (expression.tokens == null) return error.UnsupportedSqlShape;
    var payload = expression;
    payload.tokens = null;
    payload.current_timestamp_precision_tokens = null;
    try validateGeneratedExpressionAstHasNoUnexpectedPayload(payload);
}

fn validateGeneratedGroupedExpressionAstStructure(expression: generated_parser.GeneratedSqlExpressionAst) !void {
    if (expression.tokens == null or expression.inner_tokens == null) return error.UnsupportedSqlShape;
    try requireGeneratedExpressionAstChild(expression.inner_expression_kind, expression.inner_tokens, expression.inner_expression);

    var payload = expression;
    payload.tokens = null;
    payload.inner_tokens = null;
    payload.inner_expression_kind = null;
    payload.inner_expression = null;
    payload.argument_items = .{};
    try validateGeneratedExpressionAstHasNoUnexpectedPayload(payload);
}

fn validateGeneratedCastExpressionAstStructure(expression: generated_parser.GeneratedSqlExpressionAst) !void {
    if (expression.tokens == null or expression.cast_expression_tokens == null or expression.cast_type_tokens == null) {
        return error.UnsupportedSqlShape;
    }
    try requireGeneratedExpressionAstChild(expression.cast_expression_kind, expression.cast_expression_tokens, expression.cast_expression);

    var payload = expression;
    payload.tokens = null;
    payload.cast_expression_tokens = null;
    payload.cast_expression_kind = null;
    payload.cast_expression = null;
    payload.cast_type_tokens = null;
    try validateGeneratedExpressionAstHasNoUnexpectedPayload(payload);
}

fn validateGeneratedExtractExpressionAstStructure(expression: generated_parser.GeneratedSqlExpressionAst) !void {
    if (expression.tokens == null or expression.extract_field_tokens == null or expression.extract_source_tokens == null) {
        return error.UnsupportedSqlShape;
    }
    try requireGeneratedExpressionAstChild(expression.extract_source_expression_kind, expression.extract_source_tokens, expression.extract_source_expression);

    var payload = expression;
    payload.tokens = null;
    payload.extract_field_tokens = null;
    payload.extract_source_tokens = null;
    payload.extract_source_expression_kind = null;
    payload.extract_source_expression = null;
    try validateGeneratedExpressionAstHasNoUnexpectedPayload(payload);
}

fn validateGeneratedArrayExpressionAstStructure(expression: generated_parser.GeneratedSqlExpressionAst) !void {
    if (expression.tokens == null) return error.UnsupportedSqlShape;
    if (expression.array_tokens == null and generatedReadListAstHasMetadata(expression.array_items)) return error.UnsupportedSqlShape;

    var payload = expression;
    payload.tokens = null;
    payload.array_tokens = null;
    payload.array_items = .{};
    try validateGeneratedExpressionAstHasNoUnexpectedPayload(payload);
}

fn validateGeneratedSubqueryExpressionAstStructure(expression: generated_parser.GeneratedSqlExpressionAst) !void {
    if (expression.tokens == null or
        expression.inner_tokens == null or
        expression.subquery_read_kind == null or
        expression.subquery_select_tokens == null or
        expression.subquery_projection_tokens == null or
        expression.subquery_projection_items.count == 0)
    {
        return error.UnsupportedSqlShape;
    }
    if (expression.subquery_where_tokens != null and expression.subquery_where_expression == null) {
        return error.UnsupportedSqlShape;
    }
    try validateGeneratedExpressionAstOptionalChild(
        expression.subquery_where_expression_kind,
        expression.subquery_where_tokens,
        expression.subquery_where_expression,
    );

    var payload = expression;
    payload.tokens = null;
    payload.inner_tokens = null;
    payload.subquery_read_kind = null;
    payload.subquery_select_tokens = null;
    payload.subquery_projection_tokens = null;
    payload.subquery_projection_items = .{};
    payload.subquery_source_tokens = null;
    payload.subquery_where_tokens = null;
    payload.subquery_where_expression_kind = null;
    payload.subquery_where_expression = null;
    payload.subquery_set_operation_tokens = null;
    payload.subquery_set_operation = null;
    payload.subquery_tail = null;
    try validateGeneratedExpressionAstHasNoUnexpectedPayload(payload);
}

fn validateGeneratedCaseExpressionAstStructure(expression: generated_parser.GeneratedSqlExpressionAst) !void {
    if (expression.tokens == null or
        expression.case_branch_count == 0 or
        expression.case_first_when_tokens == null or
        expression.case_first_condition_tokens == null or
        expression.case_first_result_tokens == null)
    {
        return error.UnsupportedSqlShape;
    }
    if (expression.case_branch_count > 1 and expression.case_last_when_tokens == null) return error.UnsupportedSqlShape;
    try requireGeneratedExpressionAstChild(expression.case_first_condition_kind, expression.case_first_condition_tokens, expression.case_first_condition);
    try requireGeneratedExpressionAstChild(expression.case_first_result_kind, expression.case_first_result_tokens, expression.case_first_result);
    if (expression.case_condition_items.count != expression.case_branch_count or
        expression.case_result_items.count != expression.case_branch_count)
    {
        return error.UnsupportedSqlShape;
    }
    if (!std.meta.eql(
        expression.case_first_condition_tokens.?,
        expression.case_condition_items.first_tokens orelse return error.UnsupportedSqlShape,
    )) return error.UnsupportedSqlShape;
    if (!std.meta.eql(
        expression.case_first_result_tokens.?,
        expression.case_result_items.first_tokens orelse return error.UnsupportedSqlShape,
    )) return error.UnsupportedSqlShape;
    if (expression.case_else_tokens != null and expression.case_else_expression_tokens == null) return error.UnsupportedSqlShape;
    if (expression.case_else_expression_tokens != null and expression.case_else_tokens == null) return error.UnsupportedSqlShape;
    try validateGeneratedExpressionAstOptionalChild(expression.case_else_expression_kind, expression.case_else_expression_tokens, expression.case_else_expression);

    var payload = expression;
    payload.tokens = null;
    payload.case_branch_count = 0;
    payload.case_first_when_tokens = null;
    payload.case_last_when_tokens = null;
    payload.case_first_condition_tokens = null;
    payload.case_first_condition_kind = null;
    payload.case_first_condition = null;
    payload.case_first_result_tokens = null;
    payload.case_first_result_kind = null;
    payload.case_first_result = null;
    payload.case_condition_items = .{};
    payload.case_result_items = .{};
    payload.case_else_tokens = null;
    payload.case_else_expression_tokens = null;
    payload.case_else_expression_kind = null;
    payload.case_else_expression = null;
    try validateGeneratedExpressionAstHasNoUnexpectedPayload(payload);
}

fn validateGeneratedFunctionCallExpressionAstStructure(expression: generated_parser.GeneratedSqlExpressionAst) !void {
    if (expression.tokens == null or expression.function_name_tokens == null) return error.UnsupportedSqlShape;
    if (expression.argument_distinct_tokens != null and expression.argument_value_tokens == null) return error.UnsupportedSqlShape;
    if (expression.argument_value_tokens != null and expression.argument_tokens == null) return error.UnsupportedSqlShape;
    if (expression.argument_order_tokens != null and expression.argument_order_items.count == 0) return error.UnsupportedSqlShape;
    if (expression.within_group_tokens != null and
        (expression.within_group_order_tokens == null or expression.within_group_order_items.count == 0))
    {
        return error.UnsupportedSqlShape;
    }
    if (expression.within_group_order_tokens != null and expression.within_group_tokens == null) return error.UnsupportedSqlShape;
    if (expression.filter_tokens != null and expression.filter_predicate_tokens == null) return error.UnsupportedSqlShape;
    if (expression.filter_predicate_tokens != null and expression.filter_tokens == null) return error.UnsupportedSqlShape;
    try validateGeneratedExpressionAstOptionalChild(expression.filter_expression_kind, expression.filter_predicate_tokens, expression.filter_expression);
    try validateGeneratedFunctionOverMetadata(expression);

    var payload = expression;
    payload.tokens = null;
    payload.function_name_tokens = null;
    payload.argument_tokens = null;
    payload.argument_distinct_tokens = null;
    payload.argument_value_tokens = null;
    payload.argument_items = .{};
    payload.argument_order_tokens = null;
    payload.argument_order_items = .{};
    payload.within_group_tokens = null;
    payload.within_group_order_tokens = null;
    payload.within_group_order_items = .{};
    payload.filter_tokens = null;
    payload.filter_predicate_tokens = null;
    payload.filter_expression_kind = null;
    payload.filter_expression = null;
    payload.over_tokens = null;
    payload.over_name_tokens = null;
    payload.over_definition_tokens = null;
    payload.over_partition_tokens = null;
    payload.over_partition_items = .{};
    payload.over_order_tokens = null;
    payload.over_order_items = .{};
    payload.over_frame_tokens = null;
    payload.over_frame_start_expression_tokens = null;
    payload.over_frame_start_expression_kind = null;
    payload.over_frame_start_expression = null;
    payload.over_frame_end_expression_tokens = null;
    payload.over_frame_end_expression_kind = null;
    payload.over_frame_end_expression = null;
    try validateGeneratedExpressionAstHasNoUnexpectedPayload(payload);
}

fn validateGeneratedReadListAstContainedByRange(
    list: generated_parser.GeneratedSqlListAst,
    containing: generated_parser.GeneratedSqlTokenRange,
) !void {
    if (list.count == 0) {
        if (generatedReadListAstHasMetadata(list)) return error.UnsupportedSqlShape;
        return;
    }
    if (list.items.len != list.count) return error.UnsupportedSqlShape;
    if (list.expression_items.len != 0 and list.expression_items.len != list.count) return error.UnsupportedSqlShape;
    if (list.alias_items.len != 0 and list.alias_items.len != list.count) return error.UnsupportedSqlShape;
    if (list.alias_name_items.len != 0 and list.alias_name_items.len != list.count) return error.UnsupportedSqlShape;
    if (list.direction_items.len != 0 and list.direction_items.len != list.count) return error.UnsupportedSqlShape;
    if (list.directions.len != 0 and list.directions.len != list.count) return error.UnsupportedSqlShape;
    if (list.order_using_operator_items.len != 0 and list.order_using_operator_items.len != list.count) return error.UnsupportedSqlShape;
    if (list.nulls_order_items.len != 0 and list.nulls_order_items.len != list.count) return error.UnsupportedSqlShape;
    if (list.nulls_orders.len != 0 and list.nulls_orders.len != list.count) return error.UnsupportedSqlShape;
    const first = list.first_tokens orelse return error.UnsupportedSqlShape;
    const last = list.last_tokens orelse return error.UnsupportedSqlShape;
    if (first.start < containing.start or first.end > containing.end) return error.UnsupportedSqlShape;
    if (last.start < containing.start or last.end > containing.end) return error.UnsupportedSqlShape;

    for (list.items, 0..) |item, index| {
        if (item.start < containing.start or item.end > containing.end) return error.UnsupportedSqlShape;
        if (list.expression_items.len != 0) {
            const expression = list.expression_items[index];
            if (expression.start < containing.start or expression.end > containing.end) return error.UnsupportedSqlShape;
        }
        if (list.alias_items.len != 0) {
            if (list.alias_items[index]) |alias| {
                if (alias.start < containing.start or alias.end > containing.end) return error.UnsupportedSqlShape;
            }
        }
        if (list.alias_name_items.len != 0) {
            if (list.alias_name_items[index]) |alias_name| {
                if (alias_name.start < containing.start or alias_name.end > containing.end) return error.UnsupportedSqlShape;
            }
        }
        if (list.direction_items.len != 0) {
            if (list.direction_items[index]) |direction| {
                if (direction.start < containing.start or direction.end > containing.end) return error.UnsupportedSqlShape;
            }
        }
        if (list.order_using_operator_items.len != 0) {
            if (list.order_using_operator_items[index]) |operator| {
                if (operator.start < containing.start or operator.end > containing.end) return error.UnsupportedSqlShape;
            }
        }
        if (list.nulls_order_items.len != 0) {
            if (list.nulls_order_items[index]) |nulls_order| {
                if (nulls_order.start < containing.start or nulls_order.end > containing.end) return error.UnsupportedSqlShape;
            }
        }
    }
}

fn validateGeneratedReadListAstContainedByOptionalRange(
    list: generated_parser.GeneratedSqlListAst,
    containing: ?generated_parser.GeneratedSqlTokenRange,
) !void {
    if (containing) |range| {
        try validateGeneratedReadListAstContainedByRange(list, range);
    } else if (generatedReadListAstHasMetadata(list)) {
        return error.UnsupportedSqlShape;
    }
}

fn validateGeneratedReadCommaDelimitedList(
    tokens: []const tokenized.Token,
    range: generated_parser.GeneratedSqlTokenRange,
    list: generated_parser.GeneratedSqlListAst,
) !void {
    if (list.count == 0) {
        if (range.start != range.end) return error.UnsupportedSqlShape;
        return;
    }
    if (list.items.len != list.count) return error.UnsupportedSqlShape;
    var expected_start = range.start;
    for (list.items, 0..) |item, index| {
        if (item.start != expected_start or item.start >= item.end or item.end > range.end) return error.UnsupportedSqlShape;
        if (index + 1 < list.items.len) {
            if (item.end >= range.end or item.end >= tokens.len or tokens[item.end].kind != .comma) return error.UnsupportedSqlShape;
            expected_start = item.end + 1;
        } else {
            expected_start = item.end;
        }
    }
    if (expected_start != range.end) return error.UnsupportedSqlShape;
}

fn validateGeneratedBooleanChainListLayout(
    tokens: []const tokenized.Token,
    read_ast: *const generated_parser.GeneratedSqlReadAst,
    expression: generated_parser.GeneratedSqlExpressionAst,
) !void {
    const range = expression.tokens orelse return error.UnsupportedSqlShape;
    const operator: token_mod.TokenKeyword = switch (expression.kind) {
        .logical_and => .@"and",
        .logical_or => .@"or",
        else => return,
    };
    const list = expression.boolean_condition_items;
    if (list.count < 2 or
        list.count != expression.boolean_condition_count or
        list.items.len != list.count or
        list.expression_items.len != list.count or
        list.expressions.len != list.count)
    {
        return error.UnsupportedSqlShape;
    }

    var expected_start = range.start;
    for (list.items, 0..) |item, index| {
        if (item.start != expected_start or item.start >= item.end or item.end > range.end) return error.UnsupportedSqlShape;
        if (!std.meta.eql(list.expression_items[index], item)) return error.UnsupportedSqlShape;
        const child_tokens = list.expressions[index].tokens orelse return error.UnsupportedSqlShape;
        if (!std.meta.eql(child_tokens, item)) return error.UnsupportedSqlShape;
        try validateGeneratedExpressionAstRanges(tokens, read_ast, list.expressions[index]);
        if (index + 1 < list.items.len) {
            if (item.end >= range.end or item.end >= tokens.len or !tokens[item.end].matchesKeywordTag(operator)) {
                return error.UnsupportedSqlShape;
            }
            expected_start = item.end + 1;
        } else {
            expected_start = item.end;
        }
    }
    if (expected_start != range.end) return error.UnsupportedSqlShape;
}

fn validateGeneratedExpressionListPayloads(
    tokens: []const tokenized.Token,
    read_ast: *const generated_parser.GeneratedSqlReadAst,
    list: generated_parser.GeneratedSqlListAst,
    expected_count: usize,
) !void {
    if (expected_count == 0 or
        list.count != expected_count or
        list.items.len != list.count or
        list.expression_items.len != list.count or
        list.expressions.len != list.count)
    {
        return error.UnsupportedSqlShape;
    }

    for (list.items, 0..) |item, index| {
        if (!std.meta.eql(list.expression_items[index], item)) return error.UnsupportedSqlShape;
        const child_tokens = list.expressions[index].tokens orelse return error.UnsupportedSqlShape;
        if (!std.meta.eql(child_tokens, item)) return error.UnsupportedSqlShape;
        try validateGeneratedExpressionAstRanges(tokens, read_ast, list.expressions[index]);
    }
}

fn validateGeneratedExpressionOwnedListPayloads(
    tokens: []const tokenized.Token,
    read_ast: *const generated_parser.GeneratedSqlReadAst,
    list: generated_parser.GeneratedSqlListAst,
    comptime require_expression_item_equals_item: bool,
) !void {
    if (list.count == 0) {
        if (list.expressions.len != 0) return error.UnsupportedSqlShape;
        return;
    }
    if (list.items.len != list.count or
        list.expression_items.len != list.count or
        list.expressions.len != list.count)
    {
        return error.UnsupportedSqlShape;
    }
    for (list.items, 0..) |item, index| {
        if (require_expression_item_equals_item and !std.meta.eql(list.expression_items[index], item)) {
            return error.UnsupportedSqlShape;
        }
        const child_tokens = list.expressions[index].tokens orelse return error.UnsupportedSqlShape;
        if (!std.meta.eql(child_tokens, list.expression_items[index])) return error.UnsupportedSqlShape;
        try validateGeneratedExpressionAstRanges(tokens, read_ast, list.expressions[index]);
    }
}

fn validateGeneratedReadListAstBoundaryExpressions(
    tokens: []const tokenized.Token,
    read_ast: *const generated_parser.GeneratedSqlReadAst,
    list: generated_parser.GeneratedSqlListAst,
    first_expression: ?*const generated_parser.GeneratedSqlExpressionAst,
    last_expression: ?*const generated_parser.GeneratedSqlExpressionAst,
) !void {
    if (list.count == 0) {
        if (generatedReadListAstHasMetadata(list)) return error.UnsupportedSqlShape;
        if (first_expression != null and generatedExpressionAstHasMetadata(first_expression.?.*)) return error.UnsupportedSqlShape;
        if (last_expression != null and generatedExpressionAstHasMetadata(last_expression.?.*)) return error.UnsupportedSqlShape;
        return;
    }
    if (list.items.len != list.count or list.expression_items.len != list.count) return error.UnsupportedSqlShape;
    const first = first_expression orelse return error.UnsupportedSqlShape;
    const last = last_expression orelse return error.UnsupportedSqlShape;
    if (!generatedExpressionAstHasMetadata(first.*) or !generatedExpressionAstHasMetadata(last.*)) return error.UnsupportedSqlShape;
    try validateGeneratedExpressionAstRanges(tokens, read_ast, first.*);
    try validateGeneratedExpressionAstRanges(tokens, read_ast, last.*);
    const first_tokens = first.tokens orelse return error.UnsupportedSqlShape;
    const last_tokens = last.tokens orelse return error.UnsupportedSqlShape;
    if (!std.meta.eql(first_tokens, list.expression_items[0])) return error.UnsupportedSqlShape;
    if (!std.meta.eql(last_tokens, list.expression_items[list.count - 1])) return error.UnsupportedSqlShape;
}

const GeneratedExpressionBinaryPayloadOptions = struct {
    negation: bool = false,
    quantifier: bool = false,
    between: bool = false,
    escape: bool = false,
    boolean_chain: bool = false,
};

fn validateGeneratedExpressionAstBinaryStructure(
    expression: generated_parser.GeneratedSqlExpressionAst,
    options: GeneratedExpressionBinaryPayloadOptions,
) !void {
    if (expression.tokens == null or
        expression.left_tokens == null or
        expression.operator_tokens == null or
        expression.right_tokens == null)
    {
        return error.UnsupportedSqlShape;
    }
    try requireGeneratedExpressionAstChild(expression.left_expression_kind, expression.left_tokens, expression.left_expression);
    try requireGeneratedExpressionAstChild(expression.right_expression_kind, expression.right_tokens, expression.right_expression);

    var payload = expression;
    payload.tokens = null;
    payload.left_tokens = null;
    payload.left_expression_kind = null;
    payload.left_expression = null;
    payload.operator_tokens = null;
    payload.right_tokens = null;
    payload.right_expression_kind = null;
    payload.right_expression = null;
    if (options.negation) payload.negation_tokens = null;
    if (options.quantifier) payload.quantifier_tokens = null;
    if (options.between) {
        payload.between_modifier_tokens = null;
        payload.between_modifier = null;
        payload.between_lower_tokens = null;
        payload.between_lower_expression_kind = null;
        payload.between_lower_expression = null;
        payload.between_upper_tokens = null;
        payload.between_upper_expression_kind = null;
        payload.between_upper_expression = null;
    }
    if (options.escape) {
        payload.escape_tokens = null;
        payload.escape_expression_kind = null;
        payload.escape_expression = null;
    }
    if (options.boolean_chain) {
        payload.boolean_condition_count = 0;
        payload.boolean_first_condition_tokens = null;
        payload.boolean_first_condition_kind = null;
        payload.boolean_first_condition = null;
        payload.boolean_last_condition_tokens = null;
        payload.boolean_last_condition_kind = null;
        payload.boolean_last_condition = null;
        payload.boolean_condition_items = .{};
    }
    try validateGeneratedExpressionAstHasNoUnexpectedPayload(payload);
}

fn validateGeneratedExpressionOperatorTokens(
    tokens: []const tokenized.Token,
    expression: generated_parser.GeneratedSqlExpressionAst,
) !void {
    const operator = expression.operator_tokens orelse return;
    if (operator.start >= operator.end or operator.end > tokens.len) return error.UnsupportedSqlShape;
    const operator_len = operator.end - operator.start;

    switch (expression.kind) {
        .comparison => {
            if (operator_len != 1) return error.UnsupportedSqlShape;
            switch (tokens[operator.start].kind) {
                .eq, .neq, .lt, .lte, .gt, .gte => {},
                else => return error.UnsupportedSqlShape,
            }
            if (expression.quantifier_tokens != null or expression.negation_tokens != null) return error.UnsupportedSqlShape;
        },
        .quantified_comparison => {
            if (operator_len != 1) return error.UnsupportedSqlShape;
            switch (tokens[operator.start].kind) {
                .eq, .neq, .lt, .lte, .gt, .gte => {},
                else => return error.UnsupportedSqlShape,
            }
            const quantifier = expression.quantifier_tokens orelse return error.UnsupportedSqlShape;
            if (quantifier.start != operator.end or quantifier.end != quantifier.start + 1) return error.UnsupportedSqlShape;
            if (!tokens[quantifier.start].matchesKeywordTag(.any) and
                !tokens[quantifier.start].matchesKeywordTag(.all) and
                !tokens[quantifier.start].matchesKeywordTag(.some))
            {
                return error.UnsupportedSqlShape;
            }
            if (expression.negation_tokens != null) return error.UnsupportedSqlShape;
        },
        .like,
        .ilike,
        .not_like,
        .not_ilike,
        => {
            if (operator_len != 1) return error.UnsupportedSqlShape;
            const expected: token_mod.TokenKeyword = switch (expression.kind) {
                .like, .not_like => .like,
                .ilike, .not_ilike => .ilike,
                else => unreachable,
            };
            if (!tokens[operator.start].matchesKeywordTag(expected)) return error.UnsupportedSqlShape;
            const negated = expression.kind == .not_like or expression.kind == .not_ilike;
            if (negated) {
                const negation = expression.negation_tokens orelse return error.UnsupportedSqlShape;
                if (negation.end != operator.start or negation.start + 1 != negation.end) return error.UnsupportedSqlShape;
                if (!tokens[negation.start].matchesKeywordTag(.not)) return error.UnsupportedSqlShape;
            } else if (expression.negation_tokens != null) {
                return error.UnsupportedSqlShape;
            }
            if (expression.quantifier_tokens) |quantifier| {
                if (quantifier.start != operator.end or quantifier.end != quantifier.start + 1) return error.UnsupportedSqlShape;
                if (!tokens[quantifier.start].matchesKeywordTag(.any) and
                    !tokens[quantifier.start].matchesKeywordTag(.all) and
                    !tokens[quantifier.start].matchesKeywordTag(.some))
                {
                    return error.UnsupportedSqlShape;
                }
            }
        },
        .in_list,
        .not_in_list,
        .between,
        .not_between,
        => {
            if (operator_len != 1) return error.UnsupportedSqlShape;
            const expected: token_mod.TokenKeyword = switch (expression.kind) {
                .in_list, .not_in_list => .in,
                .between, .not_between => .between,
                else => unreachable,
            };
            if (!tokens[operator.start].matchesKeywordTag(expected)) return error.UnsupportedSqlShape;
            const negated = expression.kind == .not_in_list or expression.kind == .not_between;
            if (negated) {
                const negation = expression.negation_tokens orelse return error.UnsupportedSqlShape;
                if (negation.end != operator.start or negation.start + 1 != negation.end) return error.UnsupportedSqlShape;
                if (!tokens[negation.start].matchesKeywordTag(.not)) return error.UnsupportedSqlShape;
            } else if (expression.negation_tokens != null) {
                return error.UnsupportedSqlShape;
            }
            if (expression.quantifier_tokens != null) return error.UnsupportedSqlShape;
        },
        .logical_or,
        .logical_and,
        .logical_not,
        => {
            if (operator_len != 1) return error.UnsupportedSqlShape;
            const expected: token_mod.TokenKeyword = switch (expression.kind) {
                .logical_or => .@"or",
                .logical_and => .@"and",
                .logical_not => .not,
                else => unreachable,
            };
            if (!tokens[operator.start].matchesKeywordTag(expected)) return error.UnsupportedSqlShape;
            if (expression.quantifier_tokens != null or expression.negation_tokens != null) return error.UnsupportedSqlShape;
        },
        .exists_subquery,
        .not_exists_subquery,
        => {
            if (operator_len != 1 or !tokens[operator.start].matchesKeywordTag(.exists)) return error.UnsupportedSqlShape;
            if (expression.kind == .not_exists_subquery) {
                const negation = expression.negation_tokens orelse return error.UnsupportedSqlShape;
                if (negation.end != operator.start or negation.start + 1 != negation.end) return error.UnsupportedSqlShape;
                if (!tokens[negation.start].matchesKeywordTag(.not)) return error.UnsupportedSqlShape;
            } else if (expression.negation_tokens != null) {
                return error.UnsupportedSqlShape;
            }
            if (expression.quantifier_tokens != null) return error.UnsupportedSqlShape;
        },
        .unary_positive,
        .unary_negative,
        .additive,
        .subtractive,
        .multiplicative,
        .divisive,
        .modulo,
        .contains,
        .overlaps,
        .json_key_exists,
        .json_key_any,
        .json_key_all,
        .regex_match,
        .regex_imatch,
        .regex_not_match,
        .regex_not_imatch,
        .string_concat,
        .json_access,
        .json_text_access,
        .json_path_access,
        .json_path_text_access,
        => {
            if (operator_len != 1) return error.UnsupportedSqlShape;
            const expected_kind: token_mod.TokenKind = switch (expression.kind) {
                .unary_positive, .additive => .plus,
                .unary_negative, .subtractive => .minus,
                .multiplicative => .star,
                .divisive => .slash,
                .modulo => .percent,
                .contains => .at_contains,
                .overlaps => .range_overlap,
                .json_key_exists => .question,
                .json_key_any => .question_any,
                .json_key_all => .question_all,
                .regex_match => .regex_match,
                .regex_imatch => .regex_imatch,
                .regex_not_match => .regex_not_match,
                .regex_not_imatch => .regex_not_imatch,
                .string_concat => .pipe_concat,
                .json_access => .arrow_json,
                .json_text_access => .arrow_text,
                .json_path_access => .path_arrow_json,
                .json_path_text_access => .path_arrow_text,
                else => unreachable,
            };
            if (tokens[operator.start].kind != expected_kind) return error.UnsupportedSqlShape;
            if (expression.quantifier_tokens != null or expression.negation_tokens != null) return error.UnsupportedSqlShape;
        },
        .is_null,
        .is_not_null,
        .is_true,
        .is_false,
        .is_unknown,
        .is_not_true,
        .is_not_false,
        .is_not_unknown,
        => {
            if (operator_len != 1) return error.UnsupportedSqlShape;
            const operator_token = tokens[operator.start];
            if (expression.kind == .is_null and operator_token.matchesKeywordTag(.isnull)) {
                if (expression.right_tokens != null or expression.right_expression != null or expression.right_expression_kind != null) return error.UnsupportedSqlShape;
                if (expression.quantifier_tokens != null or expression.negation_tokens != null) return error.UnsupportedSqlShape;
                return;
            }
            if (expression.kind == .is_not_null and operator_token.matchesKeywordTag(.notnull)) {
                if (expression.right_tokens != null or expression.right_expression != null or expression.right_expression_kind != null) return error.UnsupportedSqlShape;
                if (expression.quantifier_tokens != null or expression.negation_tokens != null) return error.UnsupportedSqlShape;
                return;
            }
            if (!operator_token.matchesKeywordTag(.is)) return error.UnsupportedSqlShape;
            if (expression.quantifier_tokens != null or expression.negation_tokens != null) return error.UnsupportedSqlShape;
            const right = expression.right_tokens orelse return error.UnsupportedSqlShape;
            if (right.start != operator.end) return error.UnsupportedSqlShape;
            const right_keyword: token_mod.TokenKeyword = switch (expression.kind) {
                .is_null, .is_not_null => .null,
                .is_true, .is_not_true => .true,
                .is_false, .is_not_false => .false,
                .is_unknown, .is_not_unknown => .unknown,
                else => unreachable,
            };
            const negated = switch (expression.kind) {
                .is_not_null, .is_not_true, .is_not_false, .is_not_unknown => true,
                else => false,
            };
            if (negated) {
                if (right.end != right.start + 2) return error.UnsupportedSqlShape;
                if (!tokens[right.start].matchesKeywordTag(.not)) return error.UnsupportedSqlShape;
                if (!tokens[right.start + 1].matchesKeywordTag(right_keyword)) return error.UnsupportedSqlShape;
            } else {
                if (right.end != right.start + 1) return error.UnsupportedSqlShape;
                if (!tokens[right.start].matchesKeywordTag(right_keyword)) return error.UnsupportedSqlShape;
            }
        },
        .is_distinct_from,
        .is_not_distinct_from,
        => {
            const negated = expression.kind == .is_not_distinct_from;
            const expected_len: usize = if (negated) 4 else 3;
            if (operator_len != expected_len) return error.UnsupportedSqlShape;
            if (!tokens[operator.start].matchesKeywordTag(.is)) return error.UnsupportedSqlShape;
            if (negated) {
                if (!tokens[operator.start + 1].matchesKeywordTag(.not) or
                    !tokens[operator.start + 2].matchesKeywordTag(.distinct) or
                    !tokens[operator.start + 3].matchesKeywordTag(.from))
                {
                    return error.UnsupportedSqlShape;
                }
                const negation = expression.negation_tokens orelse return error.UnsupportedSqlShape;
                if (negation.start != operator.start + 1 or negation.end != operator.start + 2) return error.UnsupportedSqlShape;
            } else {
                if (!tokens[operator.start + 1].matchesKeywordTag(.distinct) or
                    !tokens[operator.start + 2].matchesKeywordTag(.from))
                {
                    return error.UnsupportedSqlShape;
                }
                if (expression.negation_tokens != null) return error.UnsupportedSqlShape;
            }
            if (expression.quantifier_tokens != null) return error.UnsupportedSqlShape;
        },
        else => {},
    }
}

fn validateGeneratedExpressionRangeContainedByExpression(
    expression_tokens: generated_parser.GeneratedSqlTokenRange,
    range: generated_parser.GeneratedSqlTokenRange,
) !void {
    if (range.start < expression_tokens.start or range.end > expression_tokens.end) return error.UnsupportedSqlShape;
}

fn validateGeneratedExpressionOwnedTokenRanges(expression: generated_parser.GeneratedSqlExpressionAst) !void {
    const expression_tokens = expression.tokens orelse return error.UnsupportedSqlShape;
    const ranges = [_]?generated_parser.GeneratedSqlTokenRange{
        expression.inner_tokens,
        expression.subquery_select_tokens,
        expression.subquery_projection_tokens,
        expression.subquery_source_tokens,
        expression.subquery_where_tokens,
        expression.subquery_set_operation_tokens,
        expression.function_name_tokens,
        expression.argument_tokens,
        expression.argument_distinct_tokens,
        expression.argument_value_tokens,
        expression.argument_order_tokens,
        expression.within_group_tokens,
        expression.within_group_order_tokens,
        expression.filter_tokens,
        expression.filter_predicate_tokens,
        expression.over_tokens,
        expression.over_name_tokens,
        expression.over_definition_tokens,
        expression.over_partition_tokens,
        expression.over_order_tokens,
        expression.over_frame_tokens,
        expression.over_frame_start_expression_tokens,
        expression.over_frame_end_expression_tokens,
        expression.array_tokens,
        expression.cast_expression_tokens,
        expression.cast_type_tokens,
        expression.case_first_when_tokens,
        expression.case_last_when_tokens,
        expression.case_first_condition_tokens,
        expression.case_first_result_tokens,
        expression.case_else_tokens,
        expression.case_else_expression_tokens,
        expression.boolean_first_condition_tokens,
        expression.boolean_last_condition_tokens,
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
        expression.between_lower_tokens,
        expression.between_upper_tokens,
        expression.quantifier_tokens,
        expression.right_tokens,
        expression.escape_tokens,
    };
    for (ranges) |range| {
        if (range) |value| try validateGeneratedExpressionRangeContainedByExpression(expression_tokens, value);
    }
}

fn generatedExpressionAstHasSubqueryMetadata(expression: generated_parser.GeneratedSqlExpressionAst) bool {
    return expression.subquery_read_kind != null or
        expression.subquery_select_tokens != null or
        expression.subquery_projection_tokens != null or
        expression.subquery_projection_items.count != 0 or
        expression.subquery_source_tokens != null or
        expression.subquery_where_tokens != null or
        expression.subquery_where_expression_kind != null or
        expression.subquery_where_expression != null or
        expression.subquery_set_operation_tokens != null or
        expression.subquery_set_operation != null or
        expression.subquery_tail != null;
}

fn validateGeneratedSubqueryTailAstRanges(
    tokens: []const tokenized.Token,
    read_ast: *const generated_parser.GeneratedSqlReadAst,
    expression: generated_parser.GeneratedSqlExpressionAst,
    tail: generated_parser.GeneratedSqlSubqueryTailAst,
) !void {
    const expression_tokens = expression.tokens orelse return error.UnsupportedSqlShape;
    const inner = expression.inner_tokens orelse return error.UnsupportedSqlShape;
    const ranges = [_]?generated_parser.GeneratedSqlTokenRange{
        tail.order_tokens,
        tail.limit_tokens,
        tail.offset_tokens,
        tail.fetch_tokens,
        tail.fetch_count_tokens,
    };
    var has_range = false;
    for (ranges) |range| {
        if (range) |value| {
            has_range = true;
            try validateGeneratedReadTokenRange(tokens, read_ast, value);
            try validateGeneratedExpressionRangeContainedByExpression(expression_tokens, value);
            if (value.start < inner.start or value.end > inner.end) return error.UnsupportedSqlShape;
        }
    }
    if (!has_range) return error.UnsupportedSqlShape;

    if (tail.order_tokens) |order_tokens| {
        try validateGeneratedReadOrderRange(tokens, order_tokens);
        try validateGeneratedReadListAstRanges(tokens, read_ast, tail.order_items);
        try validateGeneratedReadListAstContainedByOptionalRange(tail.order_items, tail.order_tokens);
        try validateGeneratedReadCommaDelimitedList(tokens, order_tokens, tail.order_items);
        try validateGeneratedReadListAstBoundaryExpressions(tokens, read_ast, tail.order_items, tail.order_first_expression, tail.order_last_expression);
    } else {
        try validateGeneratedReadListAstContainedByOptionalRange(tail.order_items, null);
        try validateGeneratedReadListAstBoundaryExpressions(tokens, read_ast, tail.order_items, tail.order_first_expression, tail.order_last_expression);
    }

    try validateGeneratedReadPaginationPayloads(
        tokens,
        read_ast,
        tail.limit_tokens,
        tail.limit_expression,
        tail.limit_all,
        tail.offset_tokens,
        tail.offset_expression,
        tail.fetch_tokens,
        tail.fetch_count_tokens,
        tail.fetch_count_expression,
    );
    try validateGeneratedSubqueryTailClauseOrder(tokens, inner, tail);
}

fn validateGeneratedSubqueryTailClauseOrder(
    tokens: []const tokenized.Token,
    inner: generated_parser.GeneratedSqlTokenRange,
    tail: generated_parser.GeneratedSqlSubqueryTailAst,
) !void {
    var saw_tail = false;
    var previous_end = inner.start;

    if (tail.order_tokens) |order_tokens| {
        try validateGeneratedReadOrderRange(tokens, order_tokens);
        if (order_tokens.start < inner.start + 2 or order_tokens.end > inner.end) return error.UnsupportedSqlShape;
        saw_tail = true;
        previous_end = order_tokens.end;
    }

    if (tail.limit_tokens) |limit_tokens| {
        try validateGeneratedSubqueryTailValueRangeOrder(tokens, inner, limit_tokens, .limit, saw_tail, previous_end);
        saw_tail = true;
        previous_end = limit_tokens.end;
    }

    if (tail.offset_tokens) |offset_tokens| {
        try validateGeneratedSubqueryTailValueRangeOrder(tokens, inner, offset_tokens, .offset, saw_tail, previous_end);
        saw_tail = true;
        previous_end = offset_tokens.end;
    }

    if (tail.fetch_tokens) |fetch_tokens| {
        try validateGeneratedSubqueryTailValueRangeOrder(tokens, inner, fetch_tokens, .fetch, saw_tail, previous_end);
        saw_tail = true;
        previous_end = fetch_tokens.end;
    }

    if (!saw_tail or previous_end != inner.end) return error.UnsupportedSqlShape;
}

fn validateGeneratedSubqueryTailValueRangeOrder(
    tokens: []const tokenized.Token,
    inner: generated_parser.GeneratedSqlTokenRange,
    range: generated_parser.GeneratedSqlTokenRange,
    keyword: token_mod.TokenKeyword,
    saw_previous: bool,
    previous_end: usize,
) !void {
    if (range.start <= inner.start or range.end > inner.end or range.start >= range.end) return error.UnsupportedSqlShape;
    try validateGeneratedReadRangePrecededByKeyword(tokens, range, keyword);
    if (saw_previous and range.start != previous_end + 1) return error.UnsupportedSqlShape;
}

fn validateGeneratedBooleanChainExpressionAstStructure(expression: generated_parser.GeneratedSqlExpressionAst) !void {
    try validateGeneratedExpressionAstBinaryStructure(expression, .{ .boolean_chain = true });
    if (expression.boolean_condition_count < 2 or
        expression.boolean_first_condition_tokens == null or
        expression.boolean_last_condition_tokens == null or
        expression.boolean_condition_items.count != expression.boolean_condition_count)
    {
        return error.UnsupportedSqlShape;
    }
    if (!std.meta.eql(
        expression.boolean_first_condition_tokens.?,
        expression.boolean_condition_items.first_tokens orelse return error.UnsupportedSqlShape,
    )) return error.UnsupportedSqlShape;
    if (!std.meta.eql(
        expression.boolean_last_condition_tokens.?,
        expression.boolean_condition_items.last_tokens orelse return error.UnsupportedSqlShape,
    )) return error.UnsupportedSqlShape;
    try requireGeneratedExpressionAstChild(
        expression.boolean_first_condition_kind,
        expression.boolean_first_condition_tokens,
        expression.boolean_first_condition,
    );
    try requireGeneratedExpressionAstChild(
        expression.boolean_last_condition_kind,
        expression.boolean_last_condition_tokens,
        expression.boolean_last_condition,
    );
}

fn validateGeneratedExistsSubqueryExpressionAstStructure(
    expression: generated_parser.GeneratedSqlExpressionAst,
    require_negation: bool,
) !void {
    if (expression.tokens == null or expression.operator_tokens == null or expression.right_tokens == null) {
        return error.UnsupportedSqlShape;
    }
    if (expression.left_tokens != null or expression.left_expression != null or expression.left_expression_kind != null) {
        return error.UnsupportedSqlShape;
    }
    if (require_negation) {
        if (expression.negation_tokens == null) return error.UnsupportedSqlShape;
    } else if (expression.negation_tokens != null) {
        return error.UnsupportedSqlShape;
    }
    try requireGeneratedExpressionAstChild(.subquery, expression.right_tokens, expression.right_expression);

    var payload = expression;
    payload.tokens = null;
    payload.operator_tokens = null;
    payload.right_tokens = null;
    payload.right_expression_kind = null;
    payload.right_expression = null;
    if (require_negation) payload.negation_tokens = null;
    try validateGeneratedExpressionAstHasNoUnexpectedPayload(payload);
}

fn validateGeneratedPrefixExpressionAstStructure(expression: generated_parser.GeneratedSqlExpressionAst) !void {
    if (expression.tokens == null or expression.operator_tokens == null or expression.right_tokens == null) {
        return error.UnsupportedSqlShape;
    }
    if (expression.left_tokens != null or expression.left_expression != null or expression.left_expression_kind != null) {
        return error.UnsupportedSqlShape;
    }
    try requireGeneratedExpressionAstChild(expression.right_expression_kind, expression.right_tokens, expression.right_expression);

    var payload = expression;
    payload.tokens = null;
    payload.operator_tokens = null;
    payload.right_tokens = null;
    payload.right_expression_kind = null;
    payload.right_expression = null;
    try validateGeneratedExpressionAstHasNoUnexpectedPayload(payload);
}

fn validateGeneratedIsExpressionAstStructure(expression: generated_parser.GeneratedSqlExpressionAst) !void {
    if (expression.tokens == null or expression.left_tokens == null or expression.operator_tokens == null) {
        return error.UnsupportedSqlShape;
    }
    try requireGeneratedExpressionAstChild(expression.left_expression_kind, expression.left_tokens, expression.left_expression);
    try validateGeneratedExpressionAstOptionalChild(expression.right_expression_kind, expression.right_tokens, expression.right_expression);

    var payload = expression;
    payload.tokens = null;
    payload.left_tokens = null;
    payload.left_expression_kind = null;
    payload.left_expression = null;
    payload.operator_tokens = null;
    payload.right_tokens = null;
    payload.right_expression_kind = null;
    payload.right_expression = null;
    try validateGeneratedExpressionAstHasNoUnexpectedPayload(payload);
}

fn validateGeneratedFunctionOverMetadata(expression: generated_parser.GeneratedSqlExpressionAst) !void {
    if (expression.over_tokens == null) {
        if (expression.over_name_tokens != null or expression.over_definition_tokens != null or
            expression.over_partition_tokens != null or generatedReadListAstHasMetadata(expression.over_partition_items) or
            expression.over_order_tokens != null or generatedReadListAstHasMetadata(expression.over_order_items) or
            expression.over_frame_tokens != null or expression.over_frame_start_expression_tokens != null or
            expression.over_frame_start_expression_kind != null or expression.over_frame_start_expression != null or
            expression.over_frame_end_expression_tokens != null or expression.over_frame_end_expression_kind != null or
            expression.over_frame_end_expression != null)
        {
            return error.UnsupportedSqlShape;
        }
        return;
    }
    if (expression.over_name_tokens != null and expression.over_definition_tokens != null) return error.UnsupportedSqlShape;
    const over_tokens = expression.over_tokens.?;
    if (expression.over_name_tokens) |name_tokens| {
        if (name_tokens.start != over_tokens.start + 1 or name_tokens.end != over_tokens.end or name_tokens.start >= name_tokens.end) {
            return error.UnsupportedSqlShape;
        }
        if (expression.over_partition_tokens != null or generatedReadListAstHasMetadata(expression.over_partition_items) or
            expression.over_order_tokens != null or generatedReadListAstHasMetadata(expression.over_order_items) or
            expression.over_frame_tokens != null or expression.over_frame_start_expression_tokens != null or
            expression.over_frame_start_expression_kind != null or expression.over_frame_start_expression != null or
            expression.over_frame_end_expression_tokens != null or expression.over_frame_end_expression_kind != null or
            expression.over_frame_end_expression != null)
        {
            return error.UnsupportedSqlShape;
        }
        return;
    }
    if (expression.over_definition_tokens == null and over_tokens.end != over_tokens.start + 3) return error.UnsupportedSqlShape;
    if (expression.over_definition_tokens) |definition_tokens| {
        if (definition_tokens.start != over_tokens.start + 2 or definition_tokens.end != over_tokens.end - 1) return error.UnsupportedSqlShape;
    }
    if (expression.over_partition_tokens) |_| {
        if (expression.over_partition_items.count == 0) return error.UnsupportedSqlShape;
    } else if (generatedReadListAstHasMetadata(expression.over_partition_items)) {
        return error.UnsupportedSqlShape;
    }
    if (expression.over_order_tokens) |_| {
        if (expression.over_order_items.count == 0) return error.UnsupportedSqlShape;
    } else if (generatedReadListAstHasMetadata(expression.over_order_items)) {
        return error.UnsupportedSqlShape;
    }
    if (expression.over_frame_tokens == null) {
        if (expression.over_frame_start_expression_tokens != null or expression.over_frame_start_expression != null or
            expression.over_frame_start_expression_kind != null or expression.over_frame_end_expression_tokens != null or
            expression.over_frame_end_expression_kind != null or expression.over_frame_end_expression != null)
        {
            return error.UnsupportedSqlShape;
        }
    } else {
        try validateGeneratedExpressionAstOptionalChild(
            expression.over_frame_start_expression_kind,
            expression.over_frame_start_expression_tokens,
            expression.over_frame_start_expression,
        );
        try validateGeneratedExpressionAstOptionalChild(
            expression.over_frame_end_expression_kind,
            expression.over_frame_end_expression_tokens,
            expression.over_frame_end_expression,
        );
    }
}

fn validateGeneratedFunctionCallClauseMetadata(
    tokens: []const tokenized.Token,
    expression: generated_parser.GeneratedSqlExpressionAst,
) !void {
    const expression_tokens = expression.tokens orelse return error.UnsupportedSqlShape;
    const name_tokens = expression.function_name_tokens orelse return error.UnsupportedSqlShape;
    if (name_tokens.start != expression_tokens.start or name_tokens.start >= name_tokens.end) return error.UnsupportedSqlShape;
    if (name_tokens.end + 1 >= tokens.len or tokens[name_tokens.end].kind != .lparen) return error.UnsupportedSqlShape;

    const close_index = if (expression.argument_tokens) |argument_tokens| blk: {
        if (argument_tokens.start != name_tokens.end + 1) return error.UnsupportedSqlShape;
        if (argument_tokens.start > argument_tokens.end) return error.UnsupportedSqlShape;
        if (argument_tokens.end >= tokens.len or tokens[argument_tokens.end].kind != .rparen) return error.UnsupportedSqlShape;
        break :blk argument_tokens.end;
    } else blk: {
        if (expression.argument_distinct_tokens != null or expression.argument_value_tokens != null or
            expression.argument_order_tokens != null or generatedReadListAstHasMetadata(expression.argument_items) or
            generatedReadListAstHasMetadata(expression.argument_order_items))
        {
            return error.UnsupportedSqlShape;
        }
        const empty_close = name_tokens.end + 1;
        if (empty_close >= tokens.len or tokens[empty_close].kind != .rparen) return error.UnsupportedSqlShape;
        break :blk empty_close;
    };

    if (expression.argument_tokens) |argument_tokens| {
        const value_tokens = expression.argument_value_tokens orelse return error.UnsupportedSqlShape;
        if (value_tokens.start < argument_tokens.start or value_tokens.end > argument_tokens.end) return error.UnsupportedSqlShape;
        if (expression.argument_distinct_tokens) |distinct_tokens| {
            if (distinct_tokens.start != argument_tokens.start or distinct_tokens.end != distinct_tokens.start + 1) return error.UnsupportedSqlShape;
            if (!tokens[distinct_tokens.start].matchesKeywordTag(.distinct)) return error.UnsupportedSqlShape;
            if (value_tokens.start != distinct_tokens.end) return error.UnsupportedSqlShape;
        } else if (value_tokens.start != argument_tokens.start) {
            return error.UnsupportedSqlShape;
        }
        if (expression.argument_order_tokens) |order_tokens| {
            if (order_tokens.end != argument_tokens.end) return error.UnsupportedSqlShape;
            if (value_tokens.end + 2 != order_tokens.start) return error.UnsupportedSqlShape;
            if (!tokens[value_tokens.end].matchesKeywordTag(.order) or !tokens[value_tokens.end + 1].matchesKeywordTag(.by)) {
                return error.UnsupportedSqlShape;
            }
        } else if (value_tokens.end != argument_tokens.end) {
            return error.UnsupportedSqlShape;
        }
    }

    var cursor = close_index + 1;
    if (expression.within_group_tokens) |within_group_tokens| {
        const order_tokens = expression.within_group_order_tokens orelse return error.UnsupportedSqlShape;
        if (within_group_tokens.start != cursor or within_group_tokens.end > expression_tokens.end) return error.UnsupportedSqlShape;
        if (within_group_tokens.end <= within_group_tokens.start + 6) return error.UnsupportedSqlShape;
        if (!tokens[within_group_tokens.start].matchesKeywordTag(.within) or
            !tokens[within_group_tokens.start + 1].matchesKeywordTag(.group) or
            tokens[within_group_tokens.start + 2].kind != .lparen or
            !tokens[within_group_tokens.start + 3].matchesKeywordTag(.order) or
            !tokens[within_group_tokens.start + 4].matchesKeywordTag(.by) or
            tokens[within_group_tokens.end - 1].kind != .rparen)
        {
            return error.UnsupportedSqlShape;
        }
        if (order_tokens.start != within_group_tokens.start + 5 or order_tokens.end != within_group_tokens.end - 1) {
            return error.UnsupportedSqlShape;
        }
        cursor = within_group_tokens.end;
    } else if (expression.within_group_order_tokens != null or generatedReadListAstHasMetadata(expression.within_group_order_items)) {
        return error.UnsupportedSqlShape;
    }

    if (expression.filter_tokens) |filter_tokens| {
        const predicate_tokens = expression.filter_predicate_tokens orelse return error.UnsupportedSqlShape;
        if (filter_tokens.start != cursor or filter_tokens.end > expression_tokens.end) return error.UnsupportedSqlShape;
        if (filter_tokens.end <= filter_tokens.start + 4) return error.UnsupportedSqlShape;
        if (!tokens[filter_tokens.start].matchesKeywordTag(.filter) or
            tokens[filter_tokens.start + 1].kind != .lparen or
            !tokens[filter_tokens.start + 2].matchesKeywordTag(.where) or
            tokens[filter_tokens.end - 1].kind != .rparen)
        {
            return error.UnsupportedSqlShape;
        }
        if (predicate_tokens.start != filter_tokens.start + 3 or predicate_tokens.end != filter_tokens.end - 1) {
            return error.UnsupportedSqlShape;
        }
        cursor = filter_tokens.end;
    } else if (expression.filter_predicate_tokens != null or expression.filter_expression != null or expression.filter_expression_kind != null) {
        return error.UnsupportedSqlShape;
    }

    if (expression.over_tokens) |over_tokens| {
        if (over_tokens.start != cursor or over_tokens.end != expression_tokens.end) return error.UnsupportedSqlShape;
        if (over_tokens.start >= over_tokens.end or over_tokens.end > tokens.len) return error.UnsupportedSqlShape;
        if (!tokens[over_tokens.start].matchesKeywordTag(.over)) return error.UnsupportedSqlShape;
        if (expression.over_name_tokens) |over_name_tokens| {
            if (over_name_tokens.start != over_tokens.start + 1 or over_name_tokens.end != over_tokens.end or over_name_tokens.start >= over_name_tokens.end) {
                return error.UnsupportedSqlShape;
            }
        } else if (expression.over_definition_tokens) |definition_tokens| {
            if (over_tokens.start + 2 > over_tokens.end or tokens[over_tokens.start + 1].kind != .lparen or tokens[over_tokens.end - 1].kind != .rparen) {
                return error.UnsupportedSqlShape;
            }
            if (definition_tokens.start != over_tokens.start + 2 or definition_tokens.end != over_tokens.end - 1) {
                return error.UnsupportedSqlShape;
            }
        } else {
            if (over_tokens.end != over_tokens.start + 3 or tokens[over_tokens.start + 1].kind != .lparen or tokens[over_tokens.end - 1].kind != .rparen) {
                return error.UnsupportedSqlShape;
            }
        }
    } else if (cursor != expression_tokens.end) {
        return error.UnsupportedSqlShape;
    }
}

fn validateGeneratedGroupedExpressionClauseMetadata(
    tokens: []const tokenized.Token,
    expression: generated_parser.GeneratedSqlExpressionAst,
) !void {
    const expression_tokens = expression.tokens orelse return error.UnsupportedSqlShape;
    const inner_tokens = expression.inner_tokens orelse return error.UnsupportedSqlShape;
    if (expression_tokens.end <= expression_tokens.start + 1) return error.UnsupportedSqlShape;
    if (tokens[expression_tokens.start].kind != .lparen or tokens[expression_tokens.end - 1].kind != .rparen) {
        return error.UnsupportedSqlShape;
    }
    if (inner_tokens.start != expression_tokens.start + 1 or inner_tokens.end != expression_tokens.end - 1) {
        return error.UnsupportedSqlShape;
    }
}

fn validateGeneratedIntervalExpressionClauseMetadata(
    tokens: []const tokenized.Token,
    expression: generated_parser.GeneratedSqlExpressionAst,
) !void {
    const expression_tokens = expression.tokens orelse return error.UnsupportedSqlShape;
    const value_tokens = expression.interval_value_tokens orelse return error.UnsupportedSqlShape;
    if (expression_tokens.end != expression_tokens.start + 2) return error.UnsupportedSqlShape;
    if (!tokens[expression_tokens.start].matchesKeywordTag(.interval)) return error.UnsupportedSqlShape;
    if (tokens[expression_tokens.start + 1].kind != .string) return error.UnsupportedSqlShape;
    if (value_tokens.start != expression_tokens.start + 1 or value_tokens.end != expression_tokens.end) return error.UnsupportedSqlShape;
}

fn validateGeneratedTimestampExpressionClauseMetadata(
    tokens: []const tokenized.Token,
    expression: generated_parser.GeneratedSqlExpressionAst,
) !void {
    const expression_tokens = expression.tokens orelse return error.UnsupportedSqlShape;
    const type_tokens = expression.timestamp_type_tokens orelse return error.UnsupportedSqlShape;
    const value_tokens = expression.timestamp_value_tokens orelse return error.UnsupportedSqlShape;
    if (expression_tokens.end != expression_tokens.start + 2) return error.UnsupportedSqlShape;
    if (!tokens[expression_tokens.start].matchesKeywordTag(.timestamp) and !tokens[expression_tokens.start].matchesKeywordTag(.timestamptz)) {
        return error.UnsupportedSqlShape;
    }
    if (tokens[expression_tokens.start + 1].kind != .string) return error.UnsupportedSqlShape;
    if (type_tokens.start != expression_tokens.start or type_tokens.end != expression_tokens.start + 1) return error.UnsupportedSqlShape;
    if (value_tokens.start != expression_tokens.start + 1 or value_tokens.end != expression_tokens.end) return error.UnsupportedSqlShape;
}

fn validateGeneratedCurrentTimestampExpressionClauseMetadata(
    tokens: []const tokenized.Token,
    expression: generated_parser.GeneratedSqlExpressionAst,
) !void {
    const expression_tokens = expression.tokens orelse return error.UnsupportedSqlShape;
    if (expression_tokens.start >= expression_tokens.end or expression_tokens.end > tokens.len) return error.UnsupportedSqlShape;
    if (!tokens[expression_tokens.start].matchesKeywordTag(.current_timestamp)) return error.UnsupportedSqlShape;
    if (expression.current_timestamp_precision_tokens) |precision_tokens| {
        if (expression_tokens.end != expression_tokens.start + 4) return error.UnsupportedSqlShape;
        if (tokens[expression_tokens.start + 1].kind != .lparen or tokens[expression_tokens.end - 1].kind != .rparen) {
            return error.UnsupportedSqlShape;
        }
        if (precision_tokens.start != expression_tokens.start + 2 or precision_tokens.end != expression_tokens.start + 3) return error.UnsupportedSqlShape;
        if (tokens[precision_tokens.start].kind != .number) return error.UnsupportedSqlShape;
    } else if (expression_tokens.end != expression_tokens.start + 1) {
        return error.UnsupportedSqlShape;
    }
}

fn validateGeneratedExtractExpressionClauseMetadata(
    tokens: []const tokenized.Token,
    expression: generated_parser.GeneratedSqlExpressionAst,
) !void {
    const expression_tokens = expression.tokens orelse return error.UnsupportedSqlShape;
    const field_tokens = expression.extract_field_tokens orelse return error.UnsupportedSqlShape;
    const source_tokens = expression.extract_source_tokens orelse return error.UnsupportedSqlShape;
    if (expression_tokens.end <= expression_tokens.start + 5) return error.UnsupportedSqlShape;
    if (!tokens[expression_tokens.start].matchesKeywordTag(.extract) or
        tokens[expression_tokens.start + 1].kind != .lparen or
        tokens[expression_tokens.end - 1].kind != .rparen)
    {
        return error.UnsupportedSqlShape;
    }
    if (field_tokens.start != expression_tokens.start + 2 or field_tokens.end != expression_tokens.start + 3) return error.UnsupportedSqlShape;
    if (tokens[field_tokens.start].kind != .identifier) return error.UnsupportedSqlShape;
    if (!tokens[field_tokens.end].matchesKeywordTag(.from)) return error.UnsupportedSqlShape;
    if (source_tokens.start != field_tokens.end + 1 or source_tokens.end != expression_tokens.end - 1) return error.UnsupportedSqlShape;
}

fn validateGeneratedArrayExpressionClauseMetadata(
    tokens: []const tokenized.Token,
    expression: generated_parser.GeneratedSqlExpressionAst,
) !void {
    const expression_tokens = expression.tokens orelse return error.UnsupportedSqlShape;
    if (expression_tokens.end <= expression_tokens.start + 2) return error.UnsupportedSqlShape;
    if (!tokens[expression_tokens.start].matchesKeywordTag(.array) or
        tokens[expression_tokens.start + 1].kind != .lbracket or
        tokens[expression_tokens.end - 1].kind != .rbracket)
    {
        return error.UnsupportedSqlShape;
    }
    if (expression.array_tokens) |array_tokens| {
        if (array_tokens.start != expression_tokens.start + 2 or array_tokens.end != expression_tokens.end - 1) return error.UnsupportedSqlShape;
        if (expression.array_items.count == 0) return error.UnsupportedSqlShape;
    } else {
        if (expression_tokens.end != expression_tokens.start + 3 or generatedReadListAstHasMetadata(expression.array_items)) return error.UnsupportedSqlShape;
    }
}

fn validateGeneratedCastExpressionClauseMetadata(
    tokens: []const tokenized.Token,
    expression: generated_parser.GeneratedSqlExpressionAst,
) !void {
    const expression_tokens = expression.tokens orelse return error.UnsupportedSqlShape;
    const value_tokens = expression.cast_expression_tokens orelse return error.UnsupportedSqlShape;
    const type_tokens = expression.cast_type_tokens orelse return error.UnsupportedSqlShape;
    if (expression_tokens.end <= expression_tokens.start + 5) return error.UnsupportedSqlShape;
    if (!tokens[expression_tokens.start].matchesKeywordTag(.cast) or
        tokens[expression_tokens.start + 1].kind != .lparen or
        tokens[expression_tokens.end - 1].kind != .rparen)
    {
        return error.UnsupportedSqlShape;
    }
    if (value_tokens.start != expression_tokens.start + 2) return error.UnsupportedSqlShape;
    if (value_tokens.end + 1 != type_tokens.start) return error.UnsupportedSqlShape;
    if (!tokens[value_tokens.end].matchesKeywordTag(.as)) return error.UnsupportedSqlShape;
    if (type_tokens.end != expression_tokens.end - 1) return error.UnsupportedSqlShape;
}

fn validateGeneratedCaseExpressionClauseMetadata(
    tokens: []const tokenized.Token,
    expression: generated_parser.GeneratedSqlExpressionAst,
) !void {
    const expression_tokens = expression.tokens orelse return error.UnsupportedSqlShape;
    if (expression_tokens.end <= expression_tokens.start + 4) return error.UnsupportedSqlShape;
    if (!tokens[expression_tokens.start].matchesKeywordTag(.case) or
        !tokens[expression_tokens.end - 1].matchesKeywordTag(.end))
    {
        return error.UnsupportedSqlShape;
    }

    const first_when = expression.case_first_when_tokens orelse return error.UnsupportedSqlShape;
    if (first_when.start != expression_tokens.start + 1 or !tokens[first_when.start].matchesKeywordTag(.when)) {
        return error.UnsupportedSqlShape;
    }
    const first_condition = expression.case_first_condition_tokens orelse return error.UnsupportedSqlShape;
    const first_result = expression.case_first_result_tokens orelse return error.UnsupportedSqlShape;
    if (first_condition.start != first_when.start + 1) return error.UnsupportedSqlShape;
    if (first_condition.end + 1 != first_result.start) return error.UnsupportedSqlShape;
    if (!tokens[first_condition.end].matchesKeywordTag(.then)) return error.UnsupportedSqlShape;
    if (first_result.end > first_when.end) return error.UnsupportedSqlShape;

    if (expression.case_branch_count == 1) {
        if (first_when.end != first_result.end) return error.UnsupportedSqlShape;
        if (expression.case_last_when_tokens != null and !std.meta.eql(expression.case_last_when_tokens.?, first_when)) {
            return error.UnsupportedSqlShape;
        }
    } else {
        const last_when = expression.case_last_when_tokens orelse return error.UnsupportedSqlShape;
        if (last_when.start <= first_when.start or last_when.end > expression_tokens.end - 1) return error.UnsupportedSqlShape;
        if (!tokens[last_when.start].matchesKeywordTag(.when)) return error.UnsupportedSqlShape;
    }

    if (expression.case_else_tokens) |else_tokens| {
        const else_expression_tokens = expression.case_else_expression_tokens orelse return error.UnsupportedSqlShape;
        if (else_tokens.end != expression_tokens.end - 1) return error.UnsupportedSqlShape;
        if (else_expression_tokens.start != else_tokens.start + 1 or else_expression_tokens.end != else_tokens.end) {
            return error.UnsupportedSqlShape;
        }
        if (!tokens[else_tokens.start].matchesKeywordTag(.@"else")) return error.UnsupportedSqlShape;
    } else if (expression.case_else_expression_tokens != null or expression.case_else_expression != null or expression.case_else_expression_kind != null) {
        return error.UnsupportedSqlShape;
    }

    var previous_end = first_when.start;
    for (expression.case_condition_items.items, expression.case_result_items.items) |condition, result| {
        if (condition.start <= previous_end or condition.end >= result.start) return error.UnsupportedSqlShape;
        if (condition.start == 0 or !tokens[condition.start - 1].matchesKeywordTag(.when)) return error.UnsupportedSqlShape;
        if (condition.end >= tokens.len or !tokens[condition.end].matchesKeywordTag(.then)) return error.UnsupportedSqlShape;
        if (result.start != condition.end + 1) return error.UnsupportedSqlShape;
        previous_end = result.end;
    }
    const end_before_case_end = if (expression.case_else_tokens) |else_tokens| else_tokens.start else expression_tokens.end - 1;
    if (previous_end != end_before_case_end) return error.UnsupportedSqlShape;
}

fn validateGeneratedExpressionAstStructure(expression: generated_parser.GeneratedSqlExpressionAst) !void {
    switch (expression.kind) {
        .token_range => try validateGeneratedTokenRangeExpressionAstStructure(expression),
        .subquery => try validateGeneratedSubqueryExpressionAstStructure(expression),
        .grouped => try validateGeneratedGroupedExpressionAstStructure(expression),
        .cast => try validateGeneratedCastExpressionAstStructure(expression),
        .case_expression => try validateGeneratedCaseExpressionAstStructure(expression),
        .interval_literal => try validateGeneratedIntervalExpressionAstStructure(expression),
        .timestamp_literal => try validateGeneratedTimestampExpressionAstStructure(expression),
        .current_date => try validateGeneratedCurrentDateExpressionAstStructure(expression),
        .current_timestamp => try validateGeneratedCurrentTimestampExpressionAstStructure(expression),
        .extract_expression => try validateGeneratedExtractExpressionAstStructure(expression),
        .function_call => try validateGeneratedFunctionCallExpressionAstStructure(expression),
        .array_constructor => try validateGeneratedArrayExpressionAstStructure(expression),
        .logical_not => try validateGeneratedPrefixExpressionAstStructure(expression),
        .exists_subquery => try validateGeneratedExistsSubqueryExpressionAstStructure(expression, false),
        .not_exists_subquery => try validateGeneratedExistsSubqueryExpressionAstStructure(expression, true),
        .unary_positive,
        .unary_negative,
        => try validateGeneratedPrefixExpressionAstStructure(expression),
        .is_null,
        .is_not_null,
        .is_true,
        .is_false,
        .is_unknown,
        .is_not_true,
        .is_not_false,
        .is_not_unknown,
        => try validateGeneratedIsExpressionAstStructure(expression),
        .not_like,
        .not_ilike,
        .not_in_list,
        .not_between,
        => {
            if (expression.negation_tokens == null) return error.UnsupportedSqlShape;
            try validateGeneratedExpressionAstBinaryStructure(expression, .{
                .negation = true,
                .quantifier = expression.kind == .not_like or expression.kind == .not_ilike,
                .between = expression.kind == .not_between,
                .escape = expression.kind == .not_like or expression.kind == .not_ilike,
            });
            if (expression.kind == .not_between) {
                if (expression.between_modifier_tokens != null and expression.between_modifier == null) return error.UnsupportedSqlShape;
                if (expression.between_lower_tokens == null or expression.between_upper_tokens == null) return error.UnsupportedSqlShape;
                try requireGeneratedExpressionAstChild(expression.between_lower_expression_kind, expression.between_lower_tokens, expression.between_lower_expression);
                try requireGeneratedExpressionAstChild(expression.between_upper_expression_kind, expression.between_upper_tokens, expression.between_upper_expression);
            } else if (expression.kind == .not_like or expression.kind == .not_ilike) {
                if (expression.escape_tokens != null and expression.escape_expression == null) return error.UnsupportedSqlShape;
                try validateGeneratedExpressionAstOptionalChild(expression.escape_expression_kind, null, expression.escape_expression);
            }
        },
        .quantified_comparison => {
            if (expression.quantifier_tokens == null) return error.UnsupportedSqlShape;
            try validateGeneratedExpressionAstBinaryStructure(expression, .{ .quantifier = true });
        },
        .like,
        .ilike,
        => {
            try validateGeneratedExpressionAstBinaryStructure(expression, .{ .quantifier = true, .escape = true });
            if (expression.escape_tokens != null and expression.escape_expression == null) return error.UnsupportedSqlShape;
            try validateGeneratedExpressionAstOptionalChild(expression.escape_expression_kind, null, expression.escape_expression);
        },
        .between => {
            if (expression.between_modifier_tokens != null and expression.between_modifier == null) return error.UnsupportedSqlShape;
            try validateGeneratedExpressionAstBinaryStructure(expression, .{ .between = true });
            if (expression.between_lower_tokens == null or expression.between_upper_tokens == null) return error.UnsupportedSqlShape;
            try requireGeneratedExpressionAstChild(expression.between_lower_expression_kind, expression.between_lower_tokens, expression.between_lower_expression);
            try requireGeneratedExpressionAstChild(expression.between_upper_expression_kind, expression.between_upper_tokens, expression.between_upper_expression);
        },
        .comparison,
        .in_list,
        .is_distinct_from,
        .is_not_distinct_from,
        .additive,
        .subtractive,
        .multiplicative,
        .divisive,
        .modulo,
        .contains,
        .overlaps,
        .json_key_exists,
        .json_key_any,
        .json_key_all,
        .regex_match,
        .regex_imatch,
        .regex_not_match,
        .regex_not_imatch,
        .string_concat,
        .json_access,
        .json_text_access,
        .json_path_access,
        .json_path_text_access,
        => try validateGeneratedExpressionAstBinaryStructure(expression, .{ .negation = expression.kind == .is_not_distinct_from }),
        .logical_or,
        .logical_and,
        => try validateGeneratedBooleanChainExpressionAstStructure(expression),
    }
}

fn validateGeneratedExpressionAstRanges(
    tokens: []const tokenized.Token,
    read_ast: *const generated_parser.GeneratedSqlReadAst,
    expression: generated_parser.GeneratedSqlExpressionAst,
) GeneratedReadValidationError!void {
    try validateGeneratedExpressionAstStructure(expression);
    try validateGeneratedExpressionOperatorTokens(tokens, expression);
    try validateGeneratedExpressionOwnedTokenRanges(expression);
    const ranges = [_]?generated_parser.GeneratedSqlTokenRange{
        expression.tokens,
        expression.inner_tokens,
        expression.subquery_select_tokens,
        expression.subquery_projection_tokens,
        expression.subquery_source_tokens,
        expression.subquery_where_tokens,
        expression.subquery_set_operation_tokens,
        expression.function_name_tokens,
        expression.argument_tokens,
        expression.argument_distinct_tokens,
        expression.argument_value_tokens,
        expression.argument_order_tokens,
        expression.within_group_tokens,
        expression.within_group_order_tokens,
        expression.filter_tokens,
        expression.filter_predicate_tokens,
        expression.over_tokens,
        expression.over_name_tokens,
        expression.over_definition_tokens,
        expression.over_partition_tokens,
        expression.over_order_tokens,
        expression.over_frame_tokens,
        expression.over_frame_start_expression_tokens,
        expression.over_frame_end_expression_tokens,
        expression.array_tokens,
        expression.cast_expression_tokens,
        expression.cast_type_tokens,
        expression.case_first_when_tokens,
        expression.case_last_when_tokens,
        expression.case_first_condition_tokens,
        expression.case_first_result_tokens,
        expression.case_else_tokens,
        expression.case_else_expression_tokens,
        expression.boolean_first_condition_tokens,
        expression.boolean_last_condition_tokens,
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
        expression.between_lower_tokens,
        expression.between_upper_tokens,
        expression.quantifier_tokens,
        expression.right_tokens,
        expression.escape_tokens,
    };
    for (ranges) |range| {
        if (range) |value| try validateGeneratedReadTokenRange(tokens, read_ast, value);
    }
    switch (expression.kind) {
        .grouped => try validateGeneratedGroupedExpressionClauseMetadata(tokens, expression),
        .cast => try validateGeneratedCastExpressionClauseMetadata(tokens, expression),
        .case_expression => try validateGeneratedCaseExpressionClauseMetadata(tokens, expression),
        .interval_literal => try validateGeneratedIntervalExpressionClauseMetadata(tokens, expression),
        .timestamp_literal => try validateGeneratedTimestampExpressionClauseMetadata(tokens, expression),
        .current_timestamp => try validateGeneratedCurrentTimestampExpressionClauseMetadata(tokens, expression),
        .extract_expression => try validateGeneratedExtractExpressionClauseMetadata(tokens, expression),
        .array_constructor => try validateGeneratedArrayExpressionClauseMetadata(tokens, expression),
        .function_call => try validateGeneratedFunctionCallClauseMetadata(tokens, expression),
        else => {},
    }
    if (expression.inner_expression) |inner| try validateGeneratedExpressionAstRanges(tokens, read_ast, inner.*);
    if (expression.left_expression) |left| try validateGeneratedExpressionAstRanges(tokens, read_ast, left.*);
    if (expression.right_expression) |right| try validateGeneratedExpressionAstRanges(tokens, read_ast, right.*);
    if (expression.filter_expression) |filter| try validateGeneratedExpressionAstRanges(tokens, read_ast, filter.*);
    if (expression.escape_expression) |escape| try validateGeneratedExpressionAstRanges(tokens, read_ast, escape.*);
    if (expression.cast_expression) |cast_expression| try validateGeneratedExpressionAstRanges(tokens, read_ast, cast_expression.*);
    if (expression.over_frame_start_expression) |frame_start_expression| try validateGeneratedExpressionAstRanges(tokens, read_ast, frame_start_expression.*);
    if (expression.over_frame_end_expression) |frame_end_expression| try validateGeneratedExpressionAstRanges(tokens, read_ast, frame_end_expression.*);
    if (expression.between_lower_expression) |between_lower_expression| try validateGeneratedExpressionAstRanges(tokens, read_ast, between_lower_expression.*);
    if (expression.between_upper_expression) |between_upper_expression| try validateGeneratedExpressionAstRanges(tokens, read_ast, between_upper_expression.*);
    if (expression.case_first_condition) |case_first_condition| try validateGeneratedExpressionAstRanges(tokens, read_ast, case_first_condition.*);
    if (expression.case_first_result) |case_first_result| try validateGeneratedExpressionAstRanges(tokens, read_ast, case_first_result.*);
    if (expression.case_else_expression) |case_else_expression| try validateGeneratedExpressionAstRanges(tokens, read_ast, case_else_expression.*);
    if (expression.boolean_first_condition) |boolean_first_condition| try validateGeneratedExpressionAstRanges(tokens, read_ast, boolean_first_condition.*);
    if (expression.boolean_last_condition) |boolean_last_condition| try validateGeneratedExpressionAstRanges(tokens, read_ast, boolean_last_condition.*);
    if (expression.extract_source_expression) |extract_source_expression| try validateGeneratedExpressionAstRanges(tokens, read_ast, extract_source_expression.*);
    if (expression.escape_tokens) |escape_tokens| {
        if (escape_tokens.start + 1 >= escape_tokens.end or !tokens[escape_tokens.start].matchesKeywordTag(.escape)) {
            return error.UnsupportedSqlShape;
        }
        try requireGeneratedExpressionAstChild(
            expression.escape_expression_kind,
            .{ .start = escape_tokens.start + 1, .end = escape_tokens.end },
            expression.escape_expression,
        );
    }
    try validateGeneratedReadListAstRanges(tokens, read_ast, expression.argument_items);
    try validateGeneratedReadListAstRanges(tokens, read_ast, expression.argument_order_items);
    try validateGeneratedReadListAstRanges(tokens, read_ast, expression.within_group_order_items);
    try validateGeneratedReadListAstRanges(tokens, read_ast, expression.over_partition_items);
    try validateGeneratedReadListAstRanges(tokens, read_ast, expression.over_order_items);
    try validateGeneratedReadListAstRanges(tokens, read_ast, expression.array_items);
    try validateGeneratedReadListAstRanges(tokens, read_ast, expression.case_condition_items);
    try validateGeneratedReadListAstRanges(tokens, read_ast, expression.case_result_items);
    try validateGeneratedReadListAstRanges(tokens, read_ast, expression.boolean_condition_items);
    try validateGeneratedReadListAstRanges(tokens, read_ast, expression.subquery_projection_items);
    if (expression.argument_value_tokens) |argument_value_tokens| {
        try validateGeneratedReadListAstContainedByRange(expression.argument_items, argument_value_tokens);
        try validateGeneratedReadCommaDelimitedList(tokens, argument_value_tokens, expression.argument_items);
        try validateGeneratedExpressionOwnedListPayloads(tokens, read_ast, expression.argument_items, true);
    } else if (expression.argument_items.count != 0 and expression.kind != .grouped) {
        return error.UnsupportedSqlShape;
    }
    if (expression.argument_order_tokens) |argument_order_tokens| {
        try validateGeneratedReadListAstContainedByRange(expression.argument_order_items, argument_order_tokens);
        try validateGeneratedReadCommaDelimitedList(tokens, argument_order_tokens, expression.argument_order_items);
        try validateGeneratedExpressionOwnedListPayloads(tokens, read_ast, expression.argument_order_items, false);
    } else if (expression.argument_order_items.count != 0) {
        return error.UnsupportedSqlShape;
    }
    if (expression.within_group_order_tokens) |within_group_order_tokens| {
        try validateGeneratedReadListAstContainedByRange(expression.within_group_order_items, within_group_order_tokens);
        try validateGeneratedReadCommaDelimitedList(tokens, within_group_order_tokens, expression.within_group_order_items);
        try validateGeneratedExpressionOwnedListPayloads(tokens, read_ast, expression.within_group_order_items, false);
    } else if (expression.within_group_order_items.count != 0) {
        return error.UnsupportedSqlShape;
    }
    if (expression.over_partition_tokens) |over_partition_tokens| {
        try validateGeneratedReadListAstContainedByRange(expression.over_partition_items, over_partition_tokens);
        try validateGeneratedReadCommaDelimitedList(tokens, over_partition_tokens, expression.over_partition_items);
        try validateGeneratedExpressionOwnedListPayloads(tokens, read_ast, expression.over_partition_items, true);
    } else if (expression.over_partition_items.count != 0) {
        return error.UnsupportedSqlShape;
    }
    if (expression.over_order_tokens) |over_order_tokens| {
        try validateGeneratedReadListAstContainedByRange(expression.over_order_items, over_order_tokens);
        try validateGeneratedReadCommaDelimitedList(tokens, over_order_tokens, expression.over_order_items);
        try validateGeneratedExpressionOwnedListPayloads(tokens, read_ast, expression.over_order_items, false);
    } else if (expression.over_order_items.count != 0) {
        return error.UnsupportedSqlShape;
    }
    if (expression.array_tokens) |array_tokens| {
        try validateGeneratedReadListAstContainedByRange(expression.array_items, array_tokens);
        try validateGeneratedReadCommaDelimitedList(tokens, array_tokens, expression.array_items);
        try validateGeneratedExpressionOwnedListPayloads(tokens, read_ast, expression.array_items, true);
    } else if (generatedReadListAstHasMetadata(expression.array_items)) {
        return error.UnsupportedSqlShape;
    }
    if (expression.tokens) |expression_tokens| {
        try validateGeneratedReadListAstContainedByRange(expression.case_condition_items, expression_tokens);
        try validateGeneratedReadListAstContainedByRange(expression.case_result_items, expression_tokens);
        try validateGeneratedReadListAstContainedByRange(expression.boolean_condition_items, expression_tokens);
        if (expression.kind == .case_expression) {
            try validateGeneratedExpressionListPayloads(tokens, read_ast, expression.case_condition_items, expression.case_branch_count);
            try validateGeneratedExpressionListPayloads(tokens, read_ast, expression.case_result_items, expression.case_branch_count);
        }
        try validateGeneratedReadListAstBoundaryExpressions(
            tokens,
            read_ast,
            expression.boolean_condition_items,
            expression.boolean_first_condition,
            expression.boolean_last_condition,
        );
        try validateGeneratedBooleanChainListLayout(tokens, read_ast, expression);
    } else if (expression.case_condition_items.count != 0 or
        expression.case_result_items.count != 0 or
        expression.boolean_condition_items.count != 0)
    {
        return error.UnsupportedSqlShape;
    }
    if (expression.subquery_set_operation) |subquery_set_operation| {
        try validateGeneratedSetOperationAstRanges(tokens, read_ast, expression.subquery_set_operation_tokens, subquery_set_operation.*);
    } else if (expression.subquery_set_operation_tokens != null) {
        return error.UnsupportedSqlShape;
    }
    if (expression.subquery_where_expression) |subquery_where_expression| {
        try validateGeneratedExpressionAstRanges(tokens, read_ast, subquery_where_expression.*);
    }
    if (expression.kind != .subquery and generatedExpressionAstHasSubqueryMetadata(expression)) return error.UnsupportedSqlShape;
    if (expression.kind == .subquery) {
        const inner = expression.inner_tokens orelse return error.UnsupportedSqlShape;
        const select_tokens = expression.subquery_select_tokens orelse return error.UnsupportedSqlShape;
        const projection_tokens = expression.subquery_projection_tokens orelse return error.UnsupportedSqlShape;
        if (select_tokens.start < inner.start or select_tokens.end > inner.end) return error.UnsupportedSqlShape;
        if (!tokens[select_tokens.start].matchesKeywordTag(.select)) return error.UnsupportedSqlShape;
        if (projection_tokens.start < select_tokens.end or projection_tokens.end > inner.end) return error.UnsupportedSqlShape;
        if (expression.subquery_projection_items.count == 0) return error.UnsupportedSqlShape;
        try validateGeneratedReadListAstContainedByRange(expression.subquery_projection_items, projection_tokens);
        try validateGeneratedReadCommaDelimitedList(tokens, projection_tokens, expression.subquery_projection_items);
        for (expression.subquery_projection_items.items) |item| {
            if (item.start < projection_tokens.start or item.end > projection_tokens.end) return error.UnsupportedSqlShape;
        }
        if (expression.subquery_source_tokens) |source_tokens| {
            if (source_tokens.start < projection_tokens.end or source_tokens.end > inner.end) return error.UnsupportedSqlShape;
            try validateGeneratedReadRangePrecededByKeyword(tokens, source_tokens, .from);
        }
        if (expression.subquery_where_tokens) |where_tokens| {
            if (where_tokens.start <= projection_tokens.end or where_tokens.end > inner.end) return error.UnsupportedSqlShape;
            try validateGeneratedReadRangePrecededByKeyword(tokens, where_tokens, .where);
            const where_expression = expression.subquery_where_expression orelse return error.UnsupportedSqlShape;
            try validateGeneratedPredicateExpressionMatchesRange(tokens, where_expression.*, where_tokens);
        }
        if (expression.subquery_set_operation_tokens) |set_operation_tokens| {
            if (set_operation_tokens.start <= projection_tokens.end or set_operation_tokens.end > inner.end) return error.UnsupportedSqlShape;
            const set_operation = expression.subquery_set_operation orelse return error.UnsupportedSqlShape;
            try validateGeneratedSetOperationMetadata(expression.subquery_set_operation_tokens, set_operation.*);
        } else if (expression.subquery_set_operation != null) {
            return error.UnsupportedSqlShape;
        }
        if (expression.subquery_tail) |tail| {
            try validateGeneratedSubqueryTailAstRanges(tokens, read_ast, expression, tail.*);
            if (tail.order_tokens) |order_tokens| {
                if (order_tokens.start <= projection_tokens.end or order_tokens.end > inner.end) return error.UnsupportedSqlShape;
            }
            if (tail.limit_tokens) |limit_tokens| {
                if (limit_tokens.start <= projection_tokens.end or limit_tokens.end > inner.end) return error.UnsupportedSqlShape;
            }
            if (tail.offset_tokens) |offset_tokens| {
                if (offset_tokens.start <= projection_tokens.end or offset_tokens.end > inner.end) return error.UnsupportedSqlShape;
            }
            if (tail.fetch_tokens) |fetch_tokens| {
                if (fetch_tokens.start <= projection_tokens.end or fetch_tokens.end > inner.end) return error.UnsupportedSqlShape;
            }
        }
    }
    if (expression.over_definition_tokens) |definition| {
        if (expression.over_partition_tokens) |partition| {
            if (partition.start < definition.start or partition.end > definition.end) return error.UnsupportedSqlShape;
        }
        if (expression.over_order_tokens) |order| {
            if (order.start < definition.start or order.end > definition.end) return error.UnsupportedSqlShape;
        }
        if (expression.over_frame_tokens) |frame| {
            if (frame.start < definition.start or frame.end > definition.end) return error.UnsupportedSqlShape;
            try validateGeneratedReadWindowFrameClauseLayout(
                tokens,
                frame,
                expression.over_frame_start_expression_kind,
                expression.over_frame_start_expression_tokens,
                expression.over_frame_start_expression,
                expression.over_frame_end_expression_kind,
                expression.over_frame_end_expression_tokens,
                expression.over_frame_end_expression,
            );
            if (expression.over_frame_start_expression_tokens) |frame_expression| {
                if (frame_expression.start < frame.start or frame_expression.end > frame.end) return error.UnsupportedSqlShape;
            }
            if (expression.over_frame_end_expression_tokens) |frame_expression| {
                if (frame_expression.start < frame.start or frame_expression.end > frame.end) return error.UnsupportedSqlShape;
            }
        }
    }
    if (expression.kind == .between or expression.kind == .not_between) {
        const right = expression.right_tokens orelse return error.UnsupportedSqlShape;
        const lower = expression.between_lower_tokens orelse return error.UnsupportedSqlShape;
        const upper = expression.between_upper_tokens orelse return error.UnsupportedSqlShape;
        if (lower.start < right.start or lower.end > right.end) return error.UnsupportedSqlShape;
        if (upper.start < right.start or upper.end > right.end) return error.UnsupportedSqlShape;
        if (lower.end >= upper.start) return error.UnsupportedSqlShape;
    }
}

fn validateGeneratedWindowAstListRanges(
    tokens: []const tokenized.Token,
    read_ast: *const generated_parser.GeneratedSqlReadAst,
    window_tokens: ?generated_parser.GeneratedSqlTokenRange,
    window_items: []const generated_parser.GeneratedSqlWindowAst,
    window_count: usize,
) !void {
    if (window_tokens == null) {
        if (window_count != 0 or window_items.len != 0) return error.UnsupportedSqlShape;
        return;
    }
    const containing = window_tokens.?;
    if (window_count == 0 or window_items.len != window_count) return error.UnsupportedSqlShape;

    for (window_items, 0..) |window, index| {
        try validateGeneratedReadTokenRange(tokens, read_ast, window.tokens);
        try validateGeneratedReadTokenRange(tokens, read_ast, window.name_tokens);
        try validateGeneratedReadTokenRange(tokens, read_ast, window.definition_tokens);
        if (window.tokens.start < containing.start or window.tokens.end > containing.end) return error.UnsupportedSqlShape;
        if (window.name_tokens.start != window.tokens.start or window.name_tokens.end >= window.definition_tokens.start) return error.UnsupportedSqlShape;
        if (window.name_tokens.end != window.name_tokens.start + 1) return error.UnsupportedSqlShape;
        if (window.definition_tokens.start <= window.name_tokens.end or window.definition_tokens.end >= window.tokens.end) return error.UnsupportedSqlShape;
        if (window.definition_tokens.end + 1 != window.tokens.end) return error.UnsupportedSqlShape;
        if (window.definition_tokens.start < 2 or !tokens[window.definition_tokens.start - 2].matchesKeywordTag(.as)) return error.UnsupportedSqlShape;
        if (tokens[window.definition_tokens.start - 1].kind != .lparen or tokens[window.definition_tokens.end].kind != .rparen) {
            return error.UnsupportedSqlShape;
        }
        if (index == 0) {
            if (window.tokens.start != containing.start) return error.UnsupportedSqlShape;
        } else {
            const previous = window_items[index - 1].tokens;
            if (previous.end >= containing.end or previous.end >= tokens.len or tokens[previous.end].kind != .comma) return error.UnsupportedSqlShape;
            if (window.tokens.start != previous.end + 1) return error.UnsupportedSqlShape;
        }
        if (index + 1 == window_items.len and window.tokens.end != containing.end) return error.UnsupportedSqlShape;

        if (window.partition_tokens) |partition_tokens| {
            try validateGeneratedReadTokenRange(tokens, read_ast, partition_tokens);
            if (partition_tokens.start < window.definition_tokens.start or partition_tokens.end > window.definition_tokens.end) return error.UnsupportedSqlShape;
            if (partition_tokens.start < 2 or
                !tokens[partition_tokens.start - 2].matchesKeywordTag(.partition) or
                !tokens[partition_tokens.start - 1].matchesKeywordTag(.by))
            {
                return error.UnsupportedSqlShape;
            }
            if (window.partition_items.count == 0) return error.UnsupportedSqlShape;
        } else if (window.partition_items.count != 0) {
            return error.UnsupportedSqlShape;
        }
        try validateGeneratedReadListAstRanges(tokens, read_ast, window.partition_items);
        try validateGeneratedReadListAstContainedByOptionalRange(window.partition_items, window.partition_tokens);
        if (window.partition_tokens) |partition_tokens| try validateGeneratedReadCommaDelimitedList(tokens, partition_tokens, window.partition_items);

        if (window.order_tokens) |order_tokens| {
            try validateGeneratedReadTokenRange(tokens, read_ast, order_tokens);
            if (order_tokens.start < window.definition_tokens.start or order_tokens.end > window.definition_tokens.end) return error.UnsupportedSqlShape;
            if (order_tokens.start < 2 or
                !tokens[order_tokens.start - 2].matchesKeywordTag(.order) or
                !tokens[order_tokens.start - 1].matchesKeywordTag(.by))
            {
                return error.UnsupportedSqlShape;
            }
            if (window.order_items.count == 0) return error.UnsupportedSqlShape;
        } else if (window.order_items.count != 0) {
            return error.UnsupportedSqlShape;
        }
        try validateGeneratedReadListAstRanges(tokens, read_ast, window.order_items);
        try validateGeneratedReadListAstContainedByOptionalRange(window.order_items, window.order_tokens);
        if (window.order_tokens) |order_tokens| try validateGeneratedReadCommaDelimitedList(tokens, order_tokens, window.order_items);

        if (window.frame_tokens) |frame_tokens| {
            try validateGeneratedReadTokenRange(tokens, read_ast, frame_tokens);
            if (frame_tokens.start < window.definition_tokens.start or frame_tokens.end > window.definition_tokens.end) return error.UnsupportedSqlShape;
            try validateGeneratedReadWindowFrameClauseLayout(
                tokens,
                frame_tokens,
                window.frame_start_expression_kind,
                window.frame_start_expression_tokens,
                window.frame_start_expression,
                window.frame_end_expression_kind,
                window.frame_end_expression_tokens,
                window.frame_end_expression,
            );
            if (window.frame_start_expression_tokens) |frame_expression| {
                try validateGeneratedReadTokenRange(tokens, read_ast, frame_expression);
                if (frame_expression.start < frame_tokens.start or frame_expression.end > frame_tokens.end) return error.UnsupportedSqlShape;
                try requireGeneratedExpressionAstChild(window.frame_start_expression_kind, window.frame_start_expression_tokens, window.frame_start_expression);
                try validateGeneratedExpressionAstRanges(tokens, read_ast, window.frame_start_expression.?.*);
            } else if (window.frame_start_expression != null) {
                return error.UnsupportedSqlShape;
            }
            if (window.frame_end_expression_tokens) |frame_expression| {
                try validateGeneratedReadTokenRange(tokens, read_ast, frame_expression);
                if (frame_expression.start < frame_tokens.start or frame_expression.end > frame_tokens.end) return error.UnsupportedSqlShape;
                try requireGeneratedExpressionAstChild(window.frame_end_expression_kind, window.frame_end_expression_tokens, window.frame_end_expression);
                try validateGeneratedExpressionAstRanges(tokens, read_ast, window.frame_end_expression.?.*);
            } else if (window.frame_end_expression != null) {
                return error.UnsupportedSqlShape;
            }
        } else if (window.frame_start_expression_tokens != null or window.frame_start_expression != null or
            window.frame_end_expression_tokens != null or window.frame_end_expression != null)
        {
            return error.UnsupportedSqlShape;
        }
    }
}

fn validateGeneratedDistinctOnListAstRanges(
    tokens: []const tokenized.Token,
    read_ast: *const generated_parser.GeneratedSqlReadAst,
    distinct_tokens: ?generated_parser.GeneratedSqlTokenRange,
    distinct_on_items: generated_parser.GeneratedSqlListAst,
) !void {
    if (distinct_tokens == null) {
        if (generatedReadListAstHasMetadata(distinct_on_items)) return error.UnsupportedSqlShape;
        try validateGeneratedReadListAstRanges(tokens, read_ast, distinct_on_items);
        return;
    }
    const distinct = distinct_tokens.?;
    try validateGeneratedDistinctOnListMetadata(distinct_tokens, distinct_on_items);
    try validateGeneratedReadListAstRanges(tokens, read_ast, distinct_on_items);
    if (distinct.start >= distinct.end or distinct.end > tokens.len) return error.UnsupportedSqlShape;
    if (!tokens[distinct.start].matchesKeywordTag(.distinct)) return error.UnsupportedSqlShape;
    if (distinct_on_items.count == 0) return;

    if (distinct.start + 4 > distinct.end or distinct.end > tokens.len) return error.UnsupportedSqlShape;
    if (!tokens[distinct.start + 1].matchesKeywordTag(.on) or
        tokens[distinct.start + 2].kind != .lparen or
        tokens[distinct.end - 1].kind != .rparen)
    {
        return error.UnsupportedSqlShape;
    }
    const inner = generated_parser.GeneratedSqlTokenRange{ .start = distinct.start + 3, .end = distinct.end - 1 };
    try validateGeneratedReadCommaDelimitedList(tokens, inner, distinct_on_items);
    for (distinct_on_items.items) |item| {
        if (item.start < inner.start or item.end > inner.end) return error.UnsupportedSqlShape;
    }
}

fn validateGeneratedDistinctOnListMetadata(
    distinct_tokens: ?generated_parser.GeneratedSqlTokenRange,
    distinct_on_items: generated_parser.GeneratedSqlListAst,
) !void {
    const distinct = distinct_tokens orelse {
        if (generatedReadListAstHasMetadata(distinct_on_items)) return error.UnsupportedSqlShape;
        return;
    };
    const has_distinct_on_shape = distinct.end > distinct.start + 1;
    if (!has_distinct_on_shape) {
        if (generatedReadListAstHasMetadata(distinct_on_items)) return error.UnsupportedSqlShape;
        return;
    }
    if (distinct_on_items.count == 0) return error.UnsupportedSqlShape;
}

fn validateGeneratedSetOperationAstRanges(
    tokens: []const tokenized.Token,
    read_ast: *const generated_parser.GeneratedSqlReadAst,
    set_operation_tokens: ?generated_parser.GeneratedSqlTokenRange,
    set_operation: generated_parser.GeneratedSqlSetOperationAst,
) !void {
    try validateGeneratedSetOperationMetadata(set_operation_tokens, set_operation);
    if (set_operation.tokens) |range| try validateGeneratedReadTokenRange(tokens, read_ast, range);
    if (set_operation.operator_tokens) |range| try validateGeneratedReadTokenRange(tokens, read_ast, range);
    if (set_operation.all_tokens) |range| try validateGeneratedReadTokenRange(tokens, read_ast, range);
    if (set_operation.right_query_tokens) |range| try validateGeneratedReadTokenRange(tokens, read_ast, range);
    if (set_operation.right_select_tokens) |range| try validateGeneratedReadTokenRange(tokens, read_ast, range);
    if (set_operation.right_distinct_tokens) |range| try validateGeneratedReadTokenRange(tokens, read_ast, range);
    if (set_operation.right_projection_tokens) |range| try validateGeneratedReadTokenRange(tokens, read_ast, range);
    if (set_operation.right_source_tokens) |range| try validateGeneratedReadTokenRange(tokens, read_ast, range);
    if (set_operation.right_source_table_tokens) |range| try validateGeneratedReadTokenRange(tokens, read_ast, range);
    if (set_operation.right_source_alias_tokens) |range| try validateGeneratedReadTokenRange(tokens, read_ast, range);
    if (set_operation.right_source_alias_name_tokens) |range| try validateGeneratedReadTokenRange(tokens, read_ast, range);
    if (set_operation.right_source_system_time_tokens) |range| try validateGeneratedReadTokenRange(tokens, read_ast, range);
    if (set_operation.right_source_system_time_sequence_tokens) |range| try validateGeneratedReadTokenRange(tokens, read_ast, range);
    if (set_operation.right_where_tokens) |range| try validateGeneratedReadTokenRange(tokens, read_ast, range);
    try validateGeneratedJoinAstRanges(tokens, read_ast, set_operation.right_join_items);

    if (set_operation_tokens == null) return;
    const containing = set_operation_tokens.?;
    const operator = set_operation.operator_tokens orelse return error.UnsupportedSqlShape;
    if (operator.start != containing.start or operator.end != containing.start + 1) return error.UnsupportedSqlShape;
    switch (set_operation.kind orelse return error.UnsupportedSqlShape) {
        .@"union" => if (!tokens[operator.start].matchesKeywordTag(.@"union")) return error.UnsupportedSqlShape,
        .intersect => if (!tokens[operator.start].matchesKeywordTag(.intersect)) return error.UnsupportedSqlShape,
        .except => if (!tokens[operator.start].matchesKeywordTag(.except)) return error.UnsupportedSqlShape,
    }
    if (set_operation.all_tokens) |all_tokens| {
        if ((set_operation.kind orelse return error.UnsupportedSqlShape) != .@"union") return error.UnsupportedSqlShape;
        if (all_tokens.start != operator.end or all_tokens.end != all_tokens.start + 1) return error.UnsupportedSqlShape;
        if (!tokens[all_tokens.start].matchesKeywordTag(.all)) return error.UnsupportedSqlShape;
    }

    const right_query = set_operation.right_query_tokens orelse return error.UnsupportedSqlShape;
    const right_select = set_operation.right_select_tokens orelse return error.UnsupportedSqlShape;
    if (right_query.start < operator.end or right_query.end > containing.end) return error.UnsupportedSqlShape;
    if (right_query.end < containing.end and !generatedReadNextIsSetOperationKeyword(tokens, right_query.end)) return error.UnsupportedSqlShape;
    if (set_operation.all_tokens) |all_tokens| {
        if (right_query.start != all_tokens.end) return error.UnsupportedSqlShape;
    } else if (right_query.start != operator.end) {
        return error.UnsupportedSqlShape;
    }
    if (right_select.start != right_query.start or right_select.end != right_query.start + 1) return error.UnsupportedSqlShape;
    if (!tokens[right_select.start].matchesKeywordTag(.select)) return error.UnsupportedSqlShape;

    try validateGeneratedDistinctOnListAstRanges(tokens, read_ast, set_operation.right_distinct_tokens, set_operation.right_distinct_on_items);
    const projection = set_operation.right_projection_tokens orelse return error.UnsupportedSqlShape;
    if (set_operation.right_distinct_tokens) |distinct| {
        if (distinct.start != right_select.end or projection.start != distinct.end) return error.UnsupportedSqlShape;
    } else if (right_select.end < right_query.end and tokens[right_select.end].matchesKeywordTag(.all)) {
        if (projection.start != right_select.end + 1) return error.UnsupportedSqlShape;
    } else if (projection.start != right_select.end) {
        return error.UnsupportedSqlShape;
    }
    if (projection.end > right_query.end) return error.UnsupportedSqlShape;
    if (set_operation.right_projection_items.count == 0) return error.UnsupportedSqlShape;
    if (!generatedExpressionAstHasMetadata(set_operation.right_projection_first_expression)) return error.UnsupportedSqlShape;
    try validateGeneratedReadListAstRanges(tokens, read_ast, set_operation.right_projection_items);
    try validateGeneratedReadListAstContainedByRange(set_operation.right_projection_items, projection);
    try validateGeneratedReadCommaDelimitedList(tokens, projection, set_operation.right_projection_items);
    try validateGeneratedReadListAstBoundaryExpressions(tokens, read_ast, set_operation.right_projection_items, &set_operation.right_projection_first_expression, &set_operation.right_projection_last_expression);
    if (set_operation.right_source_tokens) |source| {
        if (source.start <= projection.end or source.end > right_query.end) return error.UnsupportedSqlShape;
        try validateGeneratedReadRangePrecededByKeyword(tokens, source, .from);
        try validateGeneratedReadSystemTimePayload(
            tokens,
            set_operation.right_source_tokens,
            set_operation.right_source_system_time_tokens,
            set_operation.right_source_system_time_sequence_tokens,
        );
        try validateGeneratedReadSourceTableMetadata(
            tokens,
            set_operation.right_source_tokens,
            set_operation.right_source_table_tokens,
            set_operation.right_source_alias_tokens,
            set_operation.right_source_alias_name_tokens,
            set_operation.right_source_system_time_tokens,
            null,
        );
        try validateGeneratedSetOperationRightJoinMetadata(tokens, source, set_operation);
    } else if (set_operation.right_source_table_tokens != null or
        set_operation.right_source_alias_tokens != null or
        set_operation.right_source_alias_name_tokens != null or
        set_operation.right_source_system_time_tokens != null or
        set_operation.right_source_system_time_sequence_tokens != null or
        set_operation.right_join_items.len != 0 or
        set_operation.right_join_tree_root_index != null or
        set_operation.right_join_tree_depth != 0)
    {
        return error.UnsupportedSqlShape;
    }
    if (set_operation.right_where_tokens) |where| {
        if (where.start <= projection.end or where.end > right_query.end) return error.UnsupportedSqlShape;
        try validateGeneratedReadRangePrecededByKeyword(tokens, where, .where);
        try validateGeneratedExpressionAstRanges(tokens, read_ast, set_operation.right_where_expression);
        try validateGeneratedPredicateExpressionMatchesRange(tokens, set_operation.right_where_expression, where);
    } else if (generatedExpressionAstHasMetadata(set_operation.right_where_expression)) {
        return error.UnsupportedSqlShape;
    }
}

fn generatedReadNextIsSetOperationKeyword(tokens: []const tokenized.Token, pos: usize) bool {
    if (pos >= tokens.len) return false;
    return tokens[pos].matchesKeywordTag(.@"union") or
        tokens[pos].matchesKeywordTag(.intersect) or
        tokens[pos].matchesKeywordTag(.except);
}

fn validateGeneratedSetOperationMetadata(
    set_operation_tokens: ?generated_parser.GeneratedSqlTokenRange,
    set_operation: generated_parser.GeneratedSqlSetOperationAst,
) !void {
    if (set_operation_tokens == null) {
        if (generatedSetOperationAstHasMetadata(set_operation)) return error.UnsupportedSqlShape;
        return;
    }
    if (!std.meta.eql(set_operation.tokens orelse return error.UnsupportedSqlShape, set_operation_tokens.?)) {
        return error.UnsupportedSqlShape;
    }
    if (set_operation.operator_tokens == null or set_operation.kind == null or set_operation.right_query_tokens == null or
        set_operation.right_select_tokens == null or set_operation.right_projection_tokens == null)
    {
        return error.UnsupportedSqlShape;
    }
}

fn generatedSetOperationAstHasMetadata(set_operation: generated_parser.GeneratedSqlSetOperationAst) bool {
    return set_operation.tokens != null or
        set_operation.operator_tokens != null or
        set_operation.kind != null or
        set_operation.all_tokens != null or
        set_operation.right_query_tokens != null or
        set_operation.right_select_tokens != null or
        set_operation.right_distinct_tokens != null or
        generatedReadListAstHasMetadata(set_operation.right_distinct_on_items) or
        set_operation.right_projection_tokens != null or
        generatedReadListAstHasMetadata(set_operation.right_projection_items) or
        generatedExpressionAstHasMetadata(set_operation.right_projection_first_expression) or
        generatedExpressionAstHasMetadata(set_operation.right_projection_last_expression) or
        set_operation.right_source_tokens != null or
        set_operation.right_source_table_tokens != null or
        set_operation.right_source_alias_tokens != null or
        set_operation.right_source_alias_name_tokens != null or
        set_operation.right_source_system_time_tokens != null or
        set_operation.right_source_system_time_sequence_tokens != null or
        set_operation.right_join_items.len != 0 or
        set_operation.right_join_tree_root_index != null or
        set_operation.right_join_tree_depth != 0 or
        set_operation.right_where_tokens != null or
        generatedExpressionAstHasMetadata(set_operation.right_where_expression);
}

fn validateGeneratedSetOperationRightJoinMetadata(
    tokens: []const tokenized.Token,
    source: generated_parser.GeneratedSqlTokenRange,
    set_operation: generated_parser.GeneratedSqlSetOperationAst,
) !void {
    if (set_operation.right_join_items.len == 0) {
        if (set_operation.right_join_tree_root_index != null or set_operation.right_join_tree_depth != 0) return error.UnsupportedSqlShape;
        return;
    }
    if (set_operation.right_source_table_tokens != null or
        set_operation.right_source_alias_tokens != null or
        set_operation.right_source_alias_name_tokens != null or
        set_operation.right_source_system_time_tokens != null or
        set_operation.right_source_system_time_sequence_tokens != null)
    {
        return error.UnsupportedSqlShape;
    }
    const first = set_operation.right_join_items[0];
    try validateGeneratedJoinTreeMetadataForSource(
        tokens,
        source,
        set_operation.right_join_items,
        set_operation.right_join_tree_root_index,
        set_operation.right_join_tree_depth,
        source,
        first.operator_tokens,
        first.kind,
        first.left_tokens,
        first.right_tokens,
        first.predicate_tokens,
    );
}

fn validateGeneratedReadListAstRanges(
    tokens: []const tokenized.Token,
    read_ast: *const generated_parser.GeneratedSqlReadAst,
    list: generated_parser.GeneratedSqlListAst,
) GeneratedReadValidationError!void {
    if (list.count == 0) {
        if (generatedReadListAstHasMetadata(list)) return error.UnsupportedSqlShape;
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
    if ((list.alias_items.len == 0) != (list.alias_name_items.len == 0)) return error.UnsupportedSqlShape;
    if ((list.direction_items.len == 0) != (list.directions.len == 0)) return error.UnsupportedSqlShape;
    if ((list.nulls_order_items.len == 0) != (list.nulls_orders.len == 0)) return error.UnsupportedSqlShape;

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
        try validateGeneratedReadListAliasMetadata(tokens, list, index);
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
        try validateGeneratedReadListOrderMetadata(tokens, list, index);
        if (list.expressions.len != 0) {
            const expression = list.expressions[index];
            try validateGeneratedExpressionAstRanges(tokens, read_ast, expression);
            const expression_tokens = expression.tokens orelse return error.UnsupportedSqlShape;
            if (!std.meta.eql(expression_tokens, expression_item)) return error.UnsupportedSqlShape;
        }
    }
}

fn validateGeneratedReadListAliasMetadata(
    tokens: []const tokenized.Token,
    list: generated_parser.GeneratedSqlListAst,
    index: usize,
) !void {
    const alias_tokens = if (list.alias_items.len != 0) list.alias_items[index] else null;
    const alias_name_tokens = if (list.alias_name_items.len != 0) list.alias_name_items[index] else null;
    if (alias_tokens) |alias| {
        const alias_name = alias_name_tokens orelse return error.UnsupportedSqlShape;
        if (alias.start >= alias.end or alias_name.start >= alias_name.end) return error.UnsupportedSqlShape;
        if (alias.end != list.items[index].end or alias_name.end != alias.end) return error.UnsupportedSqlShape;
        if (alias.start == alias_name.start) {
            if (alias.end != alias.start + 1) return error.UnsupportedSqlShape;
            if (tokens[alias.start].kind != .identifier) return error.UnsupportedSqlShape;
            return;
        }
        if (alias_name.start != alias.start + 1) return error.UnsupportedSqlShape;
        if (!tokens[alias.start].matchesKeywordTag(.as)) return error.UnsupportedSqlShape;
    } else if (alias_name_tokens != null) {
        return error.UnsupportedSqlShape;
    }
}

fn validateGeneratedReadListOrderMetadata(
    tokens: []const tokenized.Token,
    list: generated_parser.GeneratedSqlListAst,
    index: usize,
) !void {
    const direction_tokens = if (list.direction_items.len != 0) list.direction_items[index] else null;
    const direction = if (list.directions.len != 0) list.directions[index] else null;
    const using_operator_tokens = if (list.order_using_operator_items.len != 0) list.order_using_operator_items[index] else null;
    if (direction_tokens) |range| {
        if (direction) |value| {
            if (using_operator_tokens != null) return error.UnsupportedSqlShape;
            if (range.end != range.start + 1) return error.UnsupportedSqlShape;
            const expected: token_mod.TokenKeyword = switch (value) {
                .asc => .asc,
                .desc => .desc,
            };
            if (!tokens[range.start].matchesKeywordTag(expected)) return error.UnsupportedSqlShape;
        } else if (using_operator_tokens) |operator_range| {
            if (range.end != range.start + 2) return error.UnsupportedSqlShape;
            if (!tokens[range.start].matchesKeywordTag(.using)) return error.UnsupportedSqlShape;
            if (operator_range.start != range.start + 1 or operator_range.end != range.end) return error.UnsupportedSqlShape;
            switch (tokens[operator_range.start].kind) {
                .eq, .neq, .lt, .lte, .gt, .gte => {},
                else => return error.UnsupportedSqlShape,
            }
        } else {
            return error.UnsupportedSqlShape;
        }
    } else if (direction != null or using_operator_tokens != null) {
        return error.UnsupportedSqlShape;
    }

    const nulls_order_tokens = if (list.nulls_order_items.len != 0) list.nulls_order_items[index] else null;
    const nulls_order = if (list.nulls_orders.len != 0) list.nulls_orders[index] else null;
    if (nulls_order_tokens) |range| {
        const value = nulls_order orelse return error.UnsupportedSqlShape;
        if (range.end != range.start + 2) return error.UnsupportedSqlShape;
        if (!tokens[range.start].matchesKeywordTag(.nulls)) return error.UnsupportedSqlShape;
        const expected: token_mod.TokenKeyword = switch (value) {
            .first => .first,
            .last => .last,
        };
        if (!tokens[range.start + 1].matchesKeywordTag(expected)) return error.UnsupportedSqlShape;
    } else if (nulls_order != null) {
        return error.UnsupportedSqlShape;
    }
}

fn validateGeneratedReadOrderRange(tokens: []const tokenized.Token, range: generated_parser.GeneratedSqlTokenRange) !void {
    if (range.start < 2) return error.UnsupportedSqlShape;
    if (!tokens[range.start - 2].matchesKeywordTag(.order) or !tokens[range.start - 1].matchesKeywordTag(.by)) {
        return error.UnsupportedSqlShape;
    }
}

fn validateGeneratedReadGroupRange(tokens: []const tokenized.Token, range: generated_parser.GeneratedSqlTokenRange) !void {
    if (range.start < 2) return error.UnsupportedSqlShape;
    if (!tokens[range.start - 2].matchesKeywordTag(.group) or !tokens[range.start - 1].matchesKeywordTag(.by)) {
        return error.UnsupportedSqlShape;
    }
}

pub const CatalogReadPlanLoweringCallbacks = struct {
    lower_document_target: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        source_binding.DocumentBinding,
        []const value_mod.SqlValue,
        expr_row_parse.SqlFunctionBindings,
    ) anyerror!plan.LoweredReadPlan,
    lower_with_source_schema: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        expr_row_parse.SqlFunctionBindings,
    ) anyerror!plan.LoweredReadPlan,
    lower_without_source_schema: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        expr_row_parse.SqlFunctionBindings,
    ) anyerror!plan.LoweredReadPlan,
};

pub const CatalogReadPlanLoweringContext = struct {
    alloc: std.mem.Allocator,
    sql: []const u8 = "",
    schema: runtime_schema.TableSchema,
    params: []const value_mod.SqlValue,
    function_bindings: expr_row_parse.SqlFunctionBindings,
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
        return try self.lowerParsedWithSession(parsed_sql, catalog, catalog_resources.SqlCatalogSession.default());
    }

    pub fn lowerParsedWithSession(
        self: *@This(),
        parsed_sql: *const tokenized.ParsedSql,
        catalog: table_catalog.CatalogSource,
        session: catalog_resources.SqlCatalogSession,
    ) !plan.LoweredReadPlan {
        var bound = try binder.bindReadPlanCatalogStatementWithSessionAlloc(self.alloc, parsed_sql, catalog, session);
        defer bound.deinit(self.alloc);
        return try self.lowerBoundParsed(parsed_sql, &bound);
    }

    pub fn lowerBoundParsed(
        self: *@This(),
        parsed_sql: *const tokenized.ParsedSql,
        bound: *binder.BoundSqlStatement,
    ) !plan.LoweredReadPlan {
        const old_parsed_sql = self.parsed_sql;
        self.parsed_sql = parsed_sql;
        defer self.parsed_sql = old_parsed_sql;
        return try binder.lowerReadPlanWithBoundStatementAlloc(self.alloc, bound, self.hooks());
    }

    pub fn lowerLogicalParsed(
        self: *@This(),
        parsed_sql: *const tokenized.ParsedSql,
        logical: *binder.LogicalSqlPlan,
    ) !plan.LoweredReadPlan {
        const old_parsed_sql = self.parsed_sql;
        self.parsed_sql = parsed_sql;
        defer self.parsed_sql = old_parsed_sql;
        return try binder.lowerReadCatalogLogicalPlan(logical, self.hooks());
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
    function_bindings: expr_row_parse.SqlFunctionBindings,
) !plan.LoweredReadPlan {
    _ = params;
    _ = function_bindings;
    const document_plan = @import("document_plan.zig");
    return switch (parsed_sql.readStatementKindIncludingGeneratedAst() orelse return error.UnsupportedSqlShape) {
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
        .join, .lateral => blk: {
            const document_query = document_plan.lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(
                alloc,
                parsed_sql,
                document.schema,
                document.virtual_schema,
                document.capabilities,
            ) catch |err| switch (err) {
                error.UnsupportedSqlShape => return error.DocumentSqlUnsupportedJoin,
                else => return err,
            };
            break :blk .{ .document_query = document_query };
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
    function_bindings: expr_row_parse.SqlFunctionBindings,
) !plan.LoweredReadPlan {
    return try lowerReadPlanWithOptionalSourceSchemaParsedSqlForLoweringContextTestAlloc(alloc, parsed_sql, schema, source_schema, params, function_bindings);
}

fn lowerReadPlanParsedSqlForLoweringContextTestAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const value_mod.SqlValue,
    function_bindings: expr_row_parse.SqlFunctionBindings,
) !plan.LoweredReadPlan {
    return try lowerReadPlanWithOptionalSourceSchemaParsedSqlForLoweringContextTestAlloc(alloc, parsed_sql, schema, null, params, function_bindings);
}

fn lowerReadPlanWithOptionalSourceSchemaParsedSqlForLoweringContextTestAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    schema: runtime_schema.TableSchema,
    source_schema: ?runtime_schema.TableSchema,
    params: []const value_mod.SqlValue,
    function_bindings: expr_row_parse.SqlFunctionBindings,
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
            .lower_set_operation_optional_source_schema = lowerSetOperationParsedSqlForLoweringContextTestAlloc,
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
            .lower_set_operation_optional_source_schema = lowerSetOperationParsedSqlForLoweringContextTestAlloc,
        },
    };
    return try lowerReadPlanFromGeneratedReadAstAlloc(&context, &parsed_sql, read_ast);
}

fn lowerQueryParsedSqlForLoweringContextTestAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    schema: runtime_schema.TableSchema,
    source_schema: ?runtime_schema.TableSchema,
    params: []const value_mod.SqlValue,
    function_bindings: expr_row_parse.SqlFunctionBindings,
) !plan.LoweredQueryPlan {
    _ = source_schema;
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    const tokens = parsed_sql.items();
    const cte_adapter_shape = parser_mod.tokensStartWithKeywordTag(tokens, .with);

    var parser_state = parser_context.ParserState{
        .alloc = alloc,
        .tokens = tokens,
        .schema = schema,
        .params = params,
        .function_bindings = function_bindings,
        .generated_read_ast = try generatedReadAstForParsedSql(parsed_sql),
    };
    var lowered = lower_expr.lowerTokenizedQueryPlanAlloc(
        alloc,
        tokens,
        &parser_state.pos,
        params,
        parser_state.generated_read_ast,
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

fn lowerQueryUnexpectedForGeneratedSetOperationTestAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    schema: runtime_schema.TableSchema,
    source_schema: ?runtime_schema.TableSchema,
    params: []const value_mod.SqlValue,
    function_bindings: expr_row_parse.SqlFunctionBindings,
) !plan.LoweredQueryPlan {
    _ = alloc;
    _ = parsed_sql;
    _ = schema;
    _ = source_schema;
    _ = params;
    _ = function_bindings;
    return error.TestUnexpectedResult;
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
        .generated_read_ast = try generatedReadAstForParsedSql(parsed_sql),
    };
    var lowered = plan.lowerTokenizedWindowPlanAlloc(
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
        .generated_read_ast = try generatedReadAstForParsedSql(parsed_sql),
    };
    var lowered = plan.lowerTokenizedAggregatePlanAlloc(
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
        .generated_read_ast = try generatedReadAstForParsedSql(parsed_sql),
    };
    var lowered = plan.lowerTokenizedJoinPlanAlloc(
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
        .generated_read_ast = try generatedReadAstForParsedSql(parsed_sql),
    };
    var lowered = plan.lowerTokenizedLateralPlanAlloc(
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
    function_bindings: expr_row_parse.SqlFunctionBindings,
) anyerror!plan.LoweredRecursiveCtePlan {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    const tokens = parsed_sql.items();
    var parser_state = parser_context.ParserState{
        .alloc = alloc,
        .tokens = tokens,
        .schema = schema,
        .params = params,
        .function_bindings = function_bindings,
        .generated_read_ast = try generatedReadAstForParsedSql(parsed_sql),
    };
    return try plan.lowerTokenizedRecursiveCtePlanAlloc(
        alloc,
        tokens,
        &parser_state.pos,
        parser_context.ParserState.ContextAccessors.recursiveCteParserHooks(&parser_state),
    );
}

fn lowerSetOperationParsedSqlForLoweringContextTestAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    schema: runtime_schema.TableSchema,
    source_schema: ?runtime_schema.TableSchema,
    params: []const value_mod.SqlValue,
    function_bindings: expr_row_parse.SqlFunctionBindings,
) !plan.LoweredSetOperationPlan {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    if (source_schema) |joined_source_schema| {
        if (joined_source_schema.storage_mode != .relational or joined_source_schema.primary_key == null) return error.InvalidSqlCatalog;
    }
    const tokens = parsed_sql.items();

    var parser_state = parser_context.ParserState{
        .alloc = alloc,
        .tokens = tokens,
        .schema = source_schema orelse schema,
        .params = params,
        .generated_read_ast = try generatedReadAstForParsedSql(parsed_sql),
        .function_bindings = function_bindings,
    };
    return try plan.lowerTokenizedSetOperationPlanAlloc(
        alloc,
        tokens,
        &parser_state.pos,
        source_schema orelse schema,
        source_schema != null,
        parser_context.ParserState.ContextAccessors.setOperationParserHooks(&parser_state),
    );
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
        "SELECT COUNT(*) AS total FROM usage_records WHERE kind = 'order'",
        "SELECT status, SUM(amount) AS total FROM usage_records WHERE kind = 'order' GROUP BY status LIMIT 5",
        "SELECT o.id AS order_id, c.name AS customer_name FROM usage_records AS o LEFT JOIN usage_records AS c ON o.tenant = c.tenant AND o.customer_id = c.id WHERE o.kind = 'order' AND c.kind = 'customer' LIMIT 5",
        "SELECT org.id AS organization_id, latest.amount AS latest_amount FROM usage_records AS org LEFT JOIN LATERAL (SELECT amount, created_at FROM usage_records AS bal WHERE bal.organization_id = org.id AND bal.kind = 'balance' ORDER BY 2 DESC LIMIT 1) AS latest ON true WHERE org.kind = 'organization' LIMIT 10",
        "SELECT tenant, id, row_number() OVER (PARTITION BY tenant ORDER BY amount DESC, id ASC) AS row_num FROM usage_records WHERE status = 'open' ORDER BY row_num ASC LIMIT 5",
        "SELECT id FROM usage_records WHERE kind = 'order' UNION ALL SELECT id FROM usage_records WHERE kind = 'return'",
        "WITH open_usage AS (SELECT tenant, amount, status FROM usage_records WHERE status = 'open') SELECT tenant, SUM(amount) AS total FROM open_usage GROUP BY tenant LIMIT 5",
        "WITH RECURSIVE source_rows AS (SELECT id FROM usage_records WHERE kind = 'order' UNION ALL SELECT child.id FROM usage_records AS child JOIN source_rows AS parent ON child.customer_id = parent.id) SELECT id FROM source_rows",
    };

    for (cases) |sql| {
        var tokenized_lowered = try lowerReadPlanForLoweringContextTestAlloc(alloc, sql, schema, &.{});
        defer tokenized_lowered.deinit(alloc);
        var generated = try lowerGeneratedReadPlanForLoweringContextTestAlloc(alloc, sql, schema, &.{});
        defer generated.deinit(alloc);
        try std.testing.expectEqual(std.meta.activeTag(tokenized_lowered), std.meta.activeTag(generated));
    }
}

test "sql adapter lowering context lowers generated set operations without query fallback" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"kind":{"type":"keyword"},"tenant":{"type":"keyword"},"id":{"type":"keyword"},"status":{"type":"keyword"},"amount":{"type":"numeric"},"created_at":{"type":"numeric"},"name":{"type":"keyword"}},"required":["kind","tenant","id"],"additionalProperties":false}}},"primary_key":{"columns":["kind","tenant","id"]}}
    ;
    const schema = try runtimeSchemaFromJsonForLoweringContextTestAlloc(alloc, schema_json);
    defer runtime_schema.freeSchema(alloc, schema);

    var parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE status = 'open' UNION ALL SELECT id FROM usage_records WHERE status = 'closed' UNION ALL SELECT id FROM usage_records WHERE status = 'pending' ORDER BY id ASC LIMIT 5",
    );
    defer parsed_sql.deinit(alloc);
    try std.testing.expectEqual(sql_statement_kind.SqlReadStatementKind.set_operation, parsed_sql.generatedReadStatementKind().?);
    const read_ast = switch ((parsed_sql.generated_statement orelse return error.UnsupportedSqlShape).ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    try std.testing.expectEqual(generated_parser.GeneratedSqlReadKind.set_operation, read_ast.kind);

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
            .lower_query_plan = lowerQueryUnexpectedForGeneratedSetOperationTestAlloc,
            .lower_set_operation_optional_source_schema = lowerSetOperationParsedSqlForLoweringContextTestAlloc,
        },
    };

    var lowered = try lowerReadPlanFromGeneratedReadAstAlloc(&context, &parsed_sql, read_ast);
    defer lowered.deinit(alloc);
    switch (lowered) {
        .query => |query| {
            try std.testing.expectEqualStrings("usage_records", query.table_name);
            try std.testing.expectEqual(@as(usize, 3), query.plan.query.or_predicates.len);
            try std.testing.expectEqualStrings("\"open\"", query.plan.query.or_predicates[0].predicates[0].value_json.?);
            try std.testing.expectEqualStrings("\"closed\"", query.plan.query.or_predicates[1].predicates[0].value_json.?);
            try std.testing.expectEqualStrings("\"pending\"", query.plan.query.or_predicates[2].predicates[0].value_json.?);
            try std.testing.expectEqual(@as(usize, 1), query.plan.query.order_by.len);
            try std.testing.expectEqualStrings("id", query.plan.query.order_by[0].field);
            try std.testing.expectEqual(@as(u32, 5), query.plan.query.limit.?);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "sql adapter lowering context requires generated read publication before generated lowering" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"kind":{"type":"keyword"},"tenant":{"type":"keyword"},"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["kind","tenant","id"],"additionalProperties":false}}},"primary_key":{"columns":["kind","tenant","id"]}}
    ;
    const schema = try runtimeSchemaFromJsonForLoweringContextTestAlloc(alloc, schema_json);
    defer runtime_schema.freeSchema(alloc, schema);

    var parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT u.id FROM usage_records AS u WHERE u.kind = 'order'",
    );
    defer parsed_sql.deinit(alloc);
    try std.testing.expectEqual(sql_statement_kind.SqlReadStatementKind.query, parsed_sql.readStatementKind().?);
    try std.testing.expectEqual(sql_statement_kind.SqlReadStatementKind.query, parsed_sql.generatedReadStatementKind().?);

    if (parsed_sql.generated_statement) |*generated_statement| {
        if (generated_statement.ast) |*generated_ast| {
            switch (generated_ast.*) {
                .read => |read_ast| read_ast.source_alias_tokens = null,
                else => return error.TestUnexpectedResult,
            }
        }
    }

    try std.testing.expect(parsed_sql.generatedReadStatementKind() == null);
    try std.testing.expectError(error.UnsupportedSqlShape, validateGeneratedReadPublishedKind(&parsed_sql));
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanParsedSqlForLoweringContextTestAlloc(alloc, &parsed_sql, schema, &.{}, .{}),
    );

    var missing_ast_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE kind = 'order'",
    );
    defer missing_ast_sql.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.read, missing_ast_sql.generatedStatementKind().?);
    try std.testing.expectEqual(sql_statement_kind.SqlReadStatementKind.query, missing_ast_sql.readStatementKind().?);
    if (missing_ast_sql.generated_statement) |*generated_statement| {
        if (generated_statement.ast) |*generated_ast| generated_ast.deinit(alloc);
        generated_statement.ast = null;
    }
    try std.testing.expect(missing_ast_sql.readStatementKindIncludingGeneratedAst() == null);
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanParsedSqlForLoweringContextTestAlloc(alloc, &missing_ast_sql, schema, &.{}, .{}),
    );

    var stale_published_read = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE kind = 'order'",
    );
    defer stale_published_read.deinit(alloc);
    const stale_read_ast = blk: {
        if (stale_published_read.generated_statement) |*generated_statement| {
            switch (generated_statement.ast.?) {
                .read => |read| break :blk read,
                else => return error.TestUnexpectedResult,
            }
        } else return error.TestUnexpectedResult;
    };
    var stale_context = ReadPlanLoweringContext{
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
            .lower_set_operation_optional_source_schema = lowerSetOperationParsedSqlForLoweringContextTestAlloc,
        },
    };
    stale_published_read.statement = .{ .unknown = stale_published_read.raw_statement };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&stale_context, &stale_published_read, stale_read_ast),
    );
}

test "sql adapter lowering context lowers native graph match through graph table function" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"body":{"type":"text"},"status":{"type":"keyword"},"amount":{"type":"numeric"},"created_at":{"type":"datetime"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const schema = try runtimeSchemaFromJsonForLoweringContextTestAlloc(alloc, schema_json);
    defer runtime_schema.freeSchema(alloc, schema);

    var native_graph = try tokenized.ParsedSql.initAlloc(
        alloc,
        "MATCH (doc)-[:cites]->(target) WITH GRAPH docs_edge_graph ON usage_records START 'doc:root' WHERE graph_name = 'graph_match' RETURN id ORDER BY score DESC LIMIT 5 OFFSET 1",
    );
    defer native_graph.deinit(alloc);

    var lowered = try lowerReadPlanParsedSqlForLoweringContextTestAlloc(alloc, &native_graph, schema, &.{}, .{});
    defer lowered.deinit(alloc);
    switch (lowered) {
        .query => |query| {
            try std.testing.expectEqualStrings("usage_records", query.table_name);
            try std.testing.expectEqual(@as(usize, 1), query.plan.ctes.len);
            try std.testing.expectEqualStrings("__antfly_native_graph", query.plan.ctes[0].name);
            try std.testing.expectEqualStrings("__antfly_native_graph", query.plan.query.source_cte);
            try std.testing.expectEqual(@as(usize, 1), query.plan.query.predicates.len);
            try std.testing.expectEqualStrings("graph_name", query.plan.query.predicates[0].field);
            try std.testing.expectEqual(@as(usize, 1), query.plan.query.order_by.len);
            try std.testing.expectEqual(@as(?u32, 5), query.plan.query.limit);
            try std.testing.expectEqual(@as(u32, 1), query.plan.query.offset);
            switch (query.plan.ctes[0].table_function orelse return error.TestUnexpectedResult) {
                .graph_query => |graph_query| {
                    try std.testing.expectEqualStrings("usage_records", graph_query.table_name);
                    try std.testing.expectEqualStrings("graph_match", graph_query.query.name);
                    try std.testing.expectEqual(@as(@TypeOf(graph_query.query.query.query_type), .pattern), graph_query.query.query.query_type);
                    try std.testing.expectEqualStrings("docs_edge_graph", graph_query.query.query.index_name);
                    try std.testing.expectEqual(@as(usize, 2), graph_query.query.query.return_aliases.len);
                    try std.testing.expectEqualStrings("doc", graph_query.query.query.return_aliases[0]);
                    try std.testing.expectEqualStrings("target", graph_query.query.query.return_aliases[1]);
                },
                else => return error.TestUnexpectedResult,
            }
        },
        else => return error.TestUnexpectedResult,
    }

    var native_graph_alias_fields = try tokenized.ParsedSql.initAlloc(
        alloc,
        "MATCH (doc)-[:cites]->(target) WITH GRAPH docs_edge_graph ON usage_records START 'doc:root' RETURN doc.key AS source_id, target.key AS target_id ORDER BY target.depth ASC LIMIT 5",
    );
    defer native_graph_alias_fields.deinit(alloc);

    var alias_lowered = try lowerReadPlanParsedSqlForLoweringContextTestAlloc(alloc, &native_graph_alias_fields, schema, &.{}, .{});
    defer alias_lowered.deinit(alloc);
    switch (alias_lowered) {
        .query => |query| {
            try std.testing.expectEqualStrings("usage_records", query.table_name);
            try std.testing.expectEqual(@as(usize, 1), query.plan.ctes.len);
            try std.testing.expectEqualStrings("__antfly_native_graph", query.plan.query.source_cte);
            try std.testing.expectEqual(@as(usize, 2), query.plan.query.select.len);
            try std.testing.expectEqualStrings("doc_key", query.plan.query.select[0]);
            try std.testing.expectEqualStrings("target_key", query.plan.query.select[1]);
            try std.testing.expectEqual(@as(usize, 1), query.plan.query.order_by.len);
            try std.testing.expectEqualStrings("target_depth", query.plan.query.order_by[0].field);
            switch (query.plan.ctes[0].table_function orelse return error.TestUnexpectedResult) {
                .graph_query => |graph_query| {
                    try std.testing.expectEqual(@as(usize, 3), graph_query.alias_projections.len);
                    try std.testing.expectEqualStrings("doc", graph_query.alias_projections[0].alias);
                    try std.testing.expectEqualStrings("key", graph_query.alias_projections[0].field);
                    try std.testing.expectEqualStrings("doc_key", graph_query.alias_projections[0].output);
                    try std.testing.expectEqualStrings("target", graph_query.alias_projections[1].alias);
                    try std.testing.expectEqualStrings("key", graph_query.alias_projections[1].field);
                    try std.testing.expectEqualStrings("target_key", graph_query.alias_projections[1].output);
                    try std.testing.expectEqualStrings("target", graph_query.alias_projections[2].alias);
                    try std.testing.expectEqualStrings("depth", graph_query.alias_projections[2].field);
                    try std.testing.expectEqualStrings("target_depth", graph_query.alias_projections[2].output);
                },
                else => return error.TestUnexpectedResult,
            }
        },
        else => return error.TestUnexpectedResult,
    }
}

test "sql adapter lowering context rejects stale native graph retained metadata" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"body":{"type":"text"},"status":{"type":"keyword"},"amount":{"type":"numeric"},"created_at":{"type":"datetime"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const schema = try runtimeSchemaFromJsonForLoweringContextTestAlloc(alloc, schema_json);
    defer runtime_schema.freeSchema(alloc, schema);

    var stale_projection = try tokenized.ParsedSql.initAlloc(
        alloc,
        "MATCH (doc)-[:cites]->(target) WITH GRAPH docs_edge_graph ON usage_records START 'doc:root' RETURN doc.key AS source_id, target.key AS target_id ORDER BY target.depth ASC LIMIT 5",
    );
    defer stale_projection.deinit(alloc);
    if (stale_projection.generated_statement) |*generated_statement| {
        if (generated_statement.ast) |*generated_ast| {
            switch (generated_ast.*) {
                .unsupported => |*unsupported| {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlUnsupportedKind.graph_query, unsupported.kind);
                    try std.testing.expectEqual(@as(usize, 2), unsupported.graph_return_projection_items.count);
                    unsupported.graph_return_projection_items.count += 1;
                },
                else => return error.TestUnexpectedResult,
            }
        } else return error.TestUnexpectedResult;
    } else return error.TestUnexpectedResult;
    try std.testing.expect(stale_projection.unsupportedStatementKindIncludingGeneratedAst() == null);
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanParsedSqlForLoweringContextTestAlloc(alloc, &stale_projection, schema, &.{}, .{}),
    );

    var stale_source_binding = try tokenized.ParsedSql.initAlloc(
        alloc,
        "MATCH (doc)-[:cites]->(target) WITH GRAPH docs_edge_graph ON usage_records START 'doc:root' RETURN doc.key AS source_id",
    );
    defer stale_source_binding.deinit(alloc);
    if (stale_source_binding.generated_statement) |*generated_statement| {
        if (generated_statement.ast) |*generated_ast| {
            switch (generated_ast.*) {
                .unsupported => |*unsupported| {
                    _ = unsupported.graph_source_binding_tokens orelse return error.TestUnexpectedResult;
                    unsupported.graph_source_binding_tokens = null;
                },
                else => return error.TestUnexpectedResult,
            }
        } else return error.TestUnexpectedResult;
    } else return error.TestUnexpectedResult;
    try std.testing.expect(stale_source_binding.unsupportedStatementKindIncludingGeneratedAst() == null);
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanParsedSqlForLoweringContextTestAlloc(alloc, &stale_source_binding, schema, &.{}, .{}),
    );
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
            .lower_set_operation_optional_source_schema = lowerSetOperationParsedSqlForLoweringContextTestAlloc,
        },
    };

    var select_all_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT ALL id, status FROM usage_records",
    );
    defer select_all_parsed_sql.deinit(alloc);
    const select_all_generated_raw = select_all_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var select_all_read_ast = switch (select_all_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast.*,
        else => return error.UnsupportedSqlShape,
    };
    try validateGeneratedReadAstForStatement(select_all_parsed_sql.items(), &select_all_read_ast);
    const select_all_projection = select_all_read_ast.projection_tokens orelse return error.TestUnexpectedResult;
    select_all_read_ast.projection_tokens = .{ .start = select_all_projection.start - 1, .end = select_all_projection.end };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(select_all_parsed_sql.items(), &select_all_read_ast),
    );

    var cte_select_all_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "WITH source_rows AS (SELECT ALL id, status FROM usage_records) SELECT id FROM source_rows",
    );
    defer cte_select_all_parsed_sql.deinit(alloc);
    const cte_select_all_generated_raw = cte_select_all_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var cte_select_all_read_ast = switch (cte_select_all_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast.*,
        else => return error.UnsupportedSqlShape,
    };
    try validateGeneratedReadAstForStatement(cte_select_all_parsed_sql.items(), &cte_select_all_read_ast);
    const cte_select_all_projection = cte_select_all_read_ast.cte_items[0].body_projection_tokens orelse return error.TestUnexpectedResult;
    cte_select_all_read_ast.cte_items[0].body_projection_tokens = .{
        .start = cte_select_all_projection.start - 1,
        .end = cte_select_all_projection.end,
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(cte_select_all_parsed_sql.items(), &cte_select_all_read_ast),
    );

    var missing_where_read_ast = switch (generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast.*,
        else => return error.UnsupportedSqlShape,
    };
    _ = missing_where_read_ast.where_tokens orelse return error.TestUnexpectedResult;
    _ = missing_where_read_ast.where_expression.tokens orelse return error.TestUnexpectedResult;
    missing_where_read_ast.where_tokens = null;
    missing_where_read_ast.where_expression = .{};
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &parsed_sql, &missing_where_read_ast),
    );

    read_ast.statement_span.start += 1;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &parsed_sql, read_ast),
    );

    read_ast = switch (generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    read_ast.command_span.end += 1;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &parsed_sql, read_ast),
    );

    var cte_command_span_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "WITH open_usage AS (SELECT id FROM usage_records WHERE kind = 'order') SELECT id FROM open_usage",
    );
    defer cte_command_span_parsed_sql.deinit(alloc);
    const cte_command_span_generated_raw = cte_command_span_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var cte_command_span_read_ast = switch (cte_command_span_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    cte_command_span_read_ast.command_span = cte_command_span_parsed_sql.items()[0].sourceSpan();
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &cte_command_span_parsed_sql, cte_command_span_read_ast),
    );

    read_ast = switch (generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    read_ast.projection_tokens = .{ .start = 2, .end = 2 };
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

    var system_time_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records FOR system_time AS OF 42 WHERE kind = 'order'",
    );
    defer system_time_parsed_sql.deinit(alloc);
    const system_time_generated_raw = system_time_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var system_time_read_ast = switch (system_time_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast.*,
        else => return error.UnsupportedSqlShape,
    };
    try validateGeneratedReadAstForStatement(system_time_parsed_sql.items(), &system_time_read_ast);
    system_time_read_ast.source_system_time_sequence_tokens = .{
        .start = (system_time_read_ast.source_system_time_sequence_tokens orelse return error.TestUnexpectedResult).start - 1,
        .end = (system_time_read_ast.source_system_time_sequence_tokens orelse return error.TestUnexpectedResult).end,
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(system_time_parsed_sql.items(), &system_time_read_ast),
    );

    system_time_read_ast = switch (system_time_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast.*,
        else => return error.UnsupportedSqlShape,
    };
    system_time_read_ast.source_system_time_tokens = null;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(system_time_parsed_sql.items(), &system_time_read_ast),
    );

    var malformed_where_expression_span_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id, status FROM usage_records WHERE kind = 'order'",
    );
    defer malformed_where_expression_span_parsed_sql.deinit(alloc);
    const malformed_where_expression_span_generated_raw = malformed_where_expression_span_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_where_expression_span_read_ast = switch (malformed_where_expression_span_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_where_expression_span_read_ast.where_expression.tokens =
        malformed_where_expression_span_read_ast.projection_items.expression_items[0];
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_where_expression_span_parsed_sql, malformed_where_expression_span_read_ast),
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

    var stale_projection_leading_gap_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id, status FROM usage_records WHERE kind = 'order'",
    );
    defer stale_projection_leading_gap_parsed_sql.deinit(alloc);
    var stale_projection_leading_gap_read_ast = switch ((stale_projection_leading_gap_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape).ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast.*,
        else => return error.UnsupportedSqlShape,
    };
    if (stale_projection_leading_gap_read_ast.projection_items.count < 2 or
        stale_projection_leading_gap_read_ast.projection_items.expressions.len < 2)
    {
        return error.TestUnexpectedResult;
    }
    stale_projection_leading_gap_read_ast.projection_items.count = 1;
    stale_projection_leading_gap_read_ast.projection_items.first_tokens = stale_projection_leading_gap_read_ast.projection_items.items[1];
    stale_projection_leading_gap_read_ast.projection_items.last_tokens = stale_projection_leading_gap_read_ast.projection_items.items[1];
    stale_projection_leading_gap_read_ast.projection_items.items = stale_projection_leading_gap_read_ast.projection_items.items[1..2];
    stale_projection_leading_gap_read_ast.projection_items.expression_items = stale_projection_leading_gap_read_ast.projection_items.expression_items[1..2];
    stale_projection_leading_gap_read_ast.projection_items.alias_items = stale_projection_leading_gap_read_ast.projection_items.alias_items[1..2];
    stale_projection_leading_gap_read_ast.projection_items.alias_name_items = stale_projection_leading_gap_read_ast.projection_items.alias_name_items[1..2];
    stale_projection_leading_gap_read_ast.projection_items.expressions = stale_projection_leading_gap_read_ast.projection_items.expressions[1..2];
    stale_projection_leading_gap_read_ast.projection_first_expression = stale_projection_leading_gap_read_ast.projection_items.expressions[0];
    stale_projection_leading_gap_read_ast.projection_last_expression = stale_projection_leading_gap_read_ast.projection_items.expressions[0];
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(stale_projection_leading_gap_parsed_sql.items(), &stale_projection_leading_gap_read_ast),
    );

    var malformed_projection_expression_span_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id, status FROM usage_records WHERE kind = 'order'",
    );
    defer malformed_projection_expression_span_parsed_sql.deinit(alloc);
    const malformed_projection_expression_span_generated_raw = malformed_projection_expression_span_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_projection_expression_span_read_ast = switch (malformed_projection_expression_span_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_projection_expression_span_read_ast.projection_items.expressions[0].tokens = malformed_projection_expression_span_read_ast.projection_items.expression_items[1];
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_projection_expression_span_parsed_sql, malformed_projection_expression_span_read_ast),
    );

    var malformed_token_range_payload_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id, status FROM usage_records WHERE kind = 'order'",
    );
    defer malformed_token_range_payload_parsed_sql.deinit(alloc);
    const malformed_token_range_payload_generated_raw = malformed_token_range_payload_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_token_range_payload_read_ast = switch (malformed_token_range_payload_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_token_range_payload_read_ast.projection_items.expressions[0].operator_tokens =
        malformed_token_range_payload_read_ast.projection_items.expressions[0].tokens;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_token_range_payload_parsed_sql, malformed_token_range_payload_read_ast),
    );

    var malformed_projection_boundary_expression_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id, status FROM usage_records WHERE kind = 'order'",
    );
    defer malformed_projection_boundary_expression_parsed_sql.deinit(alloc);
    const malformed_projection_boundary_expression_generated_raw = malformed_projection_boundary_expression_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_projection_boundary_expression_read_ast = switch (malformed_projection_boundary_expression_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_projection_boundary_expression_read_ast.projection_first_expression.tokens =
        malformed_projection_boundary_expression_read_ast.projection_items.expression_items[1];
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_projection_boundary_expression_parsed_sql, malformed_projection_boundary_expression_read_ast),
    );

    var malformed_group_list_containment_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT status, COUNT(*) FROM usage_records GROUP BY status",
    );
    defer malformed_group_list_containment_parsed_sql.deinit(alloc);
    const malformed_group_list_containment_generated_raw = malformed_group_list_containment_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_group_list_containment_read_ast = switch (malformed_group_list_containment_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_group_list_containment_read_ast.group_items.first_tokens =
        malformed_group_list_containment_read_ast.projection_items.items[0];
    malformed_group_list_containment_read_ast.group_items.last_tokens =
        malformed_group_list_containment_read_ast.projection_items.items[0];
    malformed_group_list_containment_read_ast.group_items.items[0] =
        malformed_group_list_containment_read_ast.projection_items.items[0];
    malformed_group_list_containment_read_ast.group_items.expression_items[0] =
        malformed_group_list_containment_read_ast.projection_items.expression_items[0];
    malformed_group_list_containment_read_ast.group_items.expressions[0].tokens =
        malformed_group_list_containment_read_ast.projection_items.expression_items[0];
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_group_list_containment_parsed_sql, malformed_group_list_containment_read_ast),
    );

    var malformed_group_clause_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT status, COUNT(*) FROM usage_records GROUP BY status",
    );
    defer malformed_group_clause_parsed_sql.deinit(alloc);
    const malformed_group_clause_generated_raw = malformed_group_clause_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_group_clause_read_ast = switch (malformed_group_clause_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    const malformed_group_clause_tokens = malformed_group_clause_read_ast.group_tokens orelse return error.UnsupportedSqlShape;
    malformed_group_clause_read_ast.group_tokens = .{
        .start = malformed_group_clause_tokens.start - 1,
        .end = malformed_group_clause_tokens.end,
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_group_clause_parsed_sql, malformed_group_clause_read_ast),
    );

    var malformed_having_clause_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT status, COUNT(*) FROM usage_records GROUP BY status HAVING COUNT(*) > 1",
    );
    defer malformed_having_clause_parsed_sql.deinit(alloc);
    const malformed_having_clause_generated_raw = malformed_having_clause_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_having_clause_read_ast = switch (malformed_having_clause_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    const malformed_having_clause_tokens = malformed_having_clause_read_ast.having_tokens orelse return error.UnsupportedSqlShape;
    malformed_having_clause_read_ast.having_tokens = .{
        .start = malformed_having_clause_tokens.start - 1,
        .end = malformed_having_clause_tokens.end,
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_having_clause_parsed_sql, malformed_having_clause_read_ast),
    );

    malformed_having_clause_read_ast = switch (malformed_having_clause_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_having_clause_read_ast.having_expression.tokens =
        malformed_having_clause_read_ast.group_items.expression_items[0];
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_having_clause_parsed_sql, malformed_having_clause_read_ast),
    );

    var stale_having_shape_metadata_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT status, COUNT(*) FROM usage_records GROUP BY status",
    );
    defer stale_having_shape_metadata_parsed_sql.deinit(alloc);
    const stale_having_shape_metadata_generated_raw = stale_having_shape_metadata_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var stale_having_shape_metadata_read_ast = switch (stale_having_shape_metadata_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    stale_having_shape_metadata_read_ast.having_expression.case_branch_count = 1;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &stale_having_shape_metadata_parsed_sql, stale_having_shape_metadata_read_ast),
    );

    var malformed_order_list_containment_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records ORDER BY status",
    );
    defer malformed_order_list_containment_parsed_sql.deinit(alloc);
    const malformed_order_list_containment_generated_raw = malformed_order_list_containment_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_order_list_containment_read_ast = switch (malformed_order_list_containment_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_order_list_containment_read_ast.order_items.first_tokens =
        malformed_order_list_containment_read_ast.projection_items.items[0];
    malformed_order_list_containment_read_ast.order_items.last_tokens =
        malformed_order_list_containment_read_ast.projection_items.items[0];
    malformed_order_list_containment_read_ast.order_items.items[0] =
        malformed_order_list_containment_read_ast.projection_items.items[0];
    malformed_order_list_containment_read_ast.order_items.expression_items[0] =
        malformed_order_list_containment_read_ast.projection_items.expression_items[0];
    malformed_order_list_containment_read_ast.order_items.expressions[0].tokens =
        malformed_order_list_containment_read_ast.projection_items.expression_items[0];
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_order_list_containment_parsed_sql, malformed_order_list_containment_read_ast),
    );

    var malformed_order_direction_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records ORDER BY status DESC NULLS LAST",
    );
    defer malformed_order_direction_parsed_sql.deinit(alloc);
    const malformed_order_direction_generated_raw = malformed_order_direction_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_order_direction_read_ast = switch (malformed_order_direction_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_order_direction_read_ast.order_items.directions[0] = .asc;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_order_direction_parsed_sql, malformed_order_direction_read_ast),
    );

    var malformed_nulls_order_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records ORDER BY status DESC NULLS LAST",
    );
    defer malformed_nulls_order_parsed_sql.deinit(alloc);
    const malformed_nulls_order_generated_raw = malformed_nulls_order_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_nulls_order_read_ast = switch (malformed_nulls_order_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_nulls_order_read_ast.order_items.nulls_orders[0] = .first;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_nulls_order_parsed_sql, malformed_nulls_order_read_ast),
    );

    var malformed_order_using_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records ORDER BY status USING <",
    );
    defer malformed_order_using_parsed_sql.deinit(alloc);
    const malformed_order_using_generated_raw = malformed_order_using_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_order_using_read_ast = switch (malformed_order_using_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_order_using_read_ast.order_items.order_using_operator_items[0] = null;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_order_using_parsed_sql, malformed_order_using_read_ast),
    );

    var malformed_graph_source_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM antfly.graph_match(table_name => 'usage_records', index => 'docs_edge_graph', start => 'doc:root', pattern => '(a)-[:cites]->(b)', return => 'b') AS gm",
    );
    defer malformed_graph_source_parsed_sql.deinit(alloc);
    const malformed_graph_source_generated_raw = malformed_graph_source_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_graph_source_read_ast = switch (malformed_graph_source_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_graph_source_read_ast.source_graph_function_kind = null;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_graph_source_parsed_sql, malformed_graph_source_read_ast),
    );
    malformed_graph_source_read_ast = switch (malformed_graph_source_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_graph_source_read_ast.source_antfly_function_count += 1;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_graph_source_parsed_sql, malformed_graph_source_read_ast),
    );
    malformed_graph_source_read_ast = switch (malformed_graph_source_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_graph_source_read_ast.source_antfly_function_items[0].argument_count += 1;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_graph_source_parsed_sql, malformed_graph_source_read_ast),
    );

    var malformed_cte_graph_source_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "WITH ranked AS (SELECT * FROM antfly.graph_metric(table_name => 'usage_records', index => 'docs_edge_graph', metric => 'pagerank', top_k => 5) AS gm) SELECT id FROM ranked",
    );
    defer malformed_cte_graph_source_parsed_sql.deinit(alloc);
    const malformed_cte_graph_source_generated_raw = malformed_cte_graph_source_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_cte_graph_source_read_ast = switch (malformed_cte_graph_source_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_cte_graph_source_read_ast.cte_items[0].body_source_antfly_function_count += 1;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_cte_graph_source_parsed_sql, malformed_cte_graph_source_read_ast),
    );
    malformed_cte_graph_source_read_ast = switch (malformed_cte_graph_source_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_cte_graph_source_read_ast.cte_items[0].body_source_graph_function_items[0].metric_value_tokens = null;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_cte_graph_source_parsed_sql, malformed_cte_graph_source_read_ast),
    );

    var duplicate_antfly_argument_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT * FROM antfly.full_text_search(index => 'docs_body_fts', query => 'refund', query => 'policy', limit => 10) AS hits",
    );
    defer duplicate_antfly_argument_parsed_sql.deinit(alloc);
    const duplicate_antfly_argument_generated_raw = duplicate_antfly_argument_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    const duplicate_antfly_argument_read_ast = switch (duplicate_antfly_argument_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &duplicate_antfly_argument_parsed_sql, duplicate_antfly_argument_read_ast),
    );

    var stale_antfly_argument_empty_list_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT * FROM antfly.full_text_search(index => 'docs_body_fts', query => 'refund', limit => 10) AS hits",
    );
    defer stale_antfly_argument_empty_list_parsed_sql.deinit(alloc);
    var stale_antfly_argument_empty_list_read_ast = switch ((stale_antfly_argument_empty_list_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape).ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    if (stale_antfly_argument_empty_list_read_ast.source_antfly_function_items.len == 0 or
        stale_antfly_argument_empty_list_read_ast.source_antfly_function_items[0].argument_items.len == 0)
    {
        return error.TestUnexpectedResult;
    }
    stale_antfly_argument_empty_list_read_ast.source_antfly_function_items[0].argument_items = &.{};
    stale_antfly_argument_empty_list_read_ast.source_antfly_function_items[0].argument_count = 0;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(stale_antfly_argument_empty_list_parsed_sql.items(), stale_antfly_argument_empty_list_read_ast),
    );

    var stale_antfly_argument_leading_gap_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT * FROM antfly.full_text_search(index => 'docs_body_fts', query => 'refund', limit => 10) AS hits",
    );
    defer stale_antfly_argument_leading_gap_parsed_sql.deinit(alloc);
    var stale_antfly_argument_leading_gap_read_ast = switch ((stale_antfly_argument_leading_gap_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape).ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    if (stale_antfly_argument_leading_gap_read_ast.source_antfly_function_items.len == 0 or
        stale_antfly_argument_leading_gap_read_ast.source_antfly_function_items[0].argument_items.len < 2)
    {
        return error.TestUnexpectedResult;
    }
    stale_antfly_argument_leading_gap_read_ast.source_antfly_function_items[0].argument_items =
        stale_antfly_argument_leading_gap_read_ast.source_antfly_function_items[0].argument_items[1..];
    stale_antfly_argument_leading_gap_read_ast.source_antfly_function_items[0].argument_count =
        stale_antfly_argument_leading_gap_read_ast.source_antfly_function_items[0].argument_items.len;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(stale_antfly_argument_leading_gap_parsed_sql.items(), stale_antfly_argument_leading_gap_read_ast),
    );

    malformed_graph_source_read_ast = switch (malformed_graph_source_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_graph_source_read_ast.source_graph_function_count += 1;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_graph_source_parsed_sql, malformed_graph_source_read_ast),
    );
    malformed_graph_source_read_ast = switch (malformed_graph_source_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_graph_source_read_ast.source_graph_function_items[0].pattern_value_tokens = null;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_graph_source_parsed_sql, malformed_graph_source_read_ast),
    );
    malformed_graph_source_read_ast = switch (malformed_graph_source_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_graph_source_read_ast.source_graph_function_items[0].index_value_tokens = malformed_graph_source_read_ast.source_graph_function_items[0].table_name_value_tokens;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_graph_source_parsed_sql, malformed_graph_source_read_ast),
    );

    var malformed_limit_expression_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records LIMIT 5",
    );
    defer malformed_limit_expression_parsed_sql.deinit(alloc);
    const malformed_limit_expression_generated_raw = malformed_limit_expression_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_limit_expression_read_ast = switch (malformed_limit_expression_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_limit_expression_read_ast.limit_expression = .{};
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_limit_expression_parsed_sql, malformed_limit_expression_read_ast),
    );

    var malformed_limit_all_expression_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records LIMIT ALL",
    );
    defer malformed_limit_all_expression_parsed_sql.deinit(alloc);
    const malformed_limit_all_expression_generated_raw = malformed_limit_all_expression_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_limit_all_expression_read_ast = switch (malformed_limit_all_expression_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_limit_all_expression_read_ast.limit_expression =
        malformed_limit_all_expression_read_ast.projection_first_expression;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_limit_all_expression_parsed_sql, malformed_limit_all_expression_read_ast),
    );
    malformed_limit_all_expression_read_ast = switch (malformed_limit_all_expression_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_limit_all_expression_read_ast.limit_tokens.?.end += 1;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(malformed_limit_all_expression_parsed_sql.items(), malformed_limit_all_expression_read_ast),
    );

    var malformed_offset_expression_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records OFFSET 2 ROWS",
    );
    defer malformed_offset_expression_parsed_sql.deinit(alloc);
    const malformed_offset_expression_generated_raw = malformed_offset_expression_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_offset_expression_read_ast = switch (malformed_offset_expression_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_offset_expression_read_ast.offset_expression.tokens =
        malformed_offset_expression_read_ast.projection_items.expression_items[0];
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_offset_expression_parsed_sql, malformed_offset_expression_read_ast),
    );
    malformed_offset_expression_read_ast = switch (malformed_offset_expression_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_offset_expression_read_ast.offset_expression.tokens = malformed_offset_expression_read_ast.offset_tokens.?;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(malformed_offset_expression_parsed_sql.items(), malformed_offset_expression_read_ast),
    );

    var malformed_fetch_count_expression_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records FETCH FIRST 3 ROWS ONLY",
    );
    defer malformed_fetch_count_expression_parsed_sql.deinit(alloc);
    const malformed_fetch_count_expression_generated_raw = malformed_fetch_count_expression_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_fetch_count_expression_read_ast = switch (malformed_fetch_count_expression_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_fetch_count_expression_read_ast.fetch_count_expression.tokens =
        malformed_fetch_count_expression_read_ast.projection_items.expression_items[0];
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_fetch_count_expression_parsed_sql, malformed_fetch_count_expression_read_ast),
    );

    malformed_fetch_count_expression_read_ast = switch (malformed_fetch_count_expression_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_fetch_count_expression_read_ast.fetch_tokens.?.start += 1;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(malformed_fetch_count_expression_parsed_sql.items(), malformed_fetch_count_expression_read_ast),
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

    var malformed_projection_alias_name_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id AS order_id, status FROM usage_records WHERE kind = 'order'",
    );
    defer malformed_projection_alias_name_parsed_sql.deinit(alloc);
    const malformed_projection_alias_name_generated_raw = malformed_projection_alias_name_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_projection_alias_name_read_ast = switch (malformed_projection_alias_name_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_projection_alias_name_read_ast.projection_items.alias_name_items[0] =
        malformed_projection_alias_name_read_ast.projection_items.alias_items[0];
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_projection_alias_name_parsed_sql, malformed_projection_alias_name_read_ast),
    );

    var malformed_projection_missing_alias_name_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id AS order_id, status FROM usage_records WHERE kind = 'order'",
    );
    defer malformed_projection_missing_alias_name_parsed_sql.deinit(alloc);
    const malformed_projection_missing_alias_name_generated_raw = malformed_projection_missing_alias_name_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_projection_missing_alias_name_read_ast = switch (malformed_projection_missing_alias_name_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_projection_missing_alias_name_read_ast.projection_items.alias_name_items[0] = null;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_projection_missing_alias_name_parsed_sql, malformed_projection_missing_alias_name_read_ast),
    );

    var malformed_projection_bare_alias_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id order_id, status FROM usage_records WHERE kind = 'order'",
    );
    defer malformed_projection_bare_alias_parsed_sql.deinit(alloc);
    const malformed_projection_bare_alias_generated_raw = malformed_projection_bare_alias_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_projection_bare_alias_read_ast = switch (malformed_projection_bare_alias_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    const malformed_projection_bare_alias = malformed_projection_bare_alias_read_ast.projection_items.alias_items[0] orelse return error.UnsupportedSqlShape;
    malformed_projection_bare_alias_read_ast.projection_items.alias_items[0] = .{
        .start = malformed_projection_bare_alias.start - 1,
        .end = malformed_projection_bare_alias.end,
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_projection_bare_alias_parsed_sql, malformed_projection_bare_alias_read_ast),
    );

    var malformed_function_expression_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT lower(status) AS status_key FROM usage_records WHERE kind = 'order'",
    );
    defer malformed_function_expression_parsed_sql.deinit(alloc);
    const malformed_function_expression_generated_raw = malformed_function_expression_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_function_expression_read_ast = switch (malformed_function_expression_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_function_expression_read_ast.projection_items.expressions[0].function_name_tokens = null;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_function_expression_parsed_sql, malformed_function_expression_read_ast),
    );
    malformed_function_expression_read_ast = switch (malformed_function_expression_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_function_expression_read_ast.projection_items.expressions[0].operator_tokens =
        malformed_function_expression_read_ast.projection_items.expressions[0].function_name_tokens;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_function_expression_parsed_sql, malformed_function_expression_read_ast),
    );

    var malformed_function_argument_list_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT concat(status, tenant) AS status_key FROM usage_records WHERE kind = 'order'",
    );
    defer malformed_function_argument_list_parsed_sql.deinit(alloc);
    const malformed_function_argument_list_generated_raw = malformed_function_argument_list_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_function_argument_list_read_ast = switch (malformed_function_argument_list_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_function_argument_list_read_ast.projection_items.expressions[0].argument_items.first_tokens =
        malformed_function_argument_list_read_ast.projection_items.items[0];
    malformed_function_argument_list_read_ast.projection_items.expressions[0].argument_items.items[0] =
        malformed_function_argument_list_read_ast.projection_items.items[0];
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_function_argument_list_parsed_sql, malformed_function_argument_list_read_ast),
    );

    malformed_function_argument_list_read_ast = switch (malformed_function_argument_list_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_function_argument_list_read_ast.projection_items.expressions[0].argument_items.expressions[0].tokens =
        malformed_function_argument_list_read_ast.projection_items.expression_items[0];
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(malformed_function_argument_list_parsed_sql.items(), malformed_function_argument_list_read_ast),
    );

    malformed_function_argument_list_read_ast = switch (malformed_function_argument_list_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    const malformed_function_argument_gap_list = &malformed_function_argument_list_read_ast.projection_items.expressions[0].argument_items;
    if (malformed_function_argument_gap_list.count < 2) return error.TestUnexpectedResult;
    malformed_function_argument_gap_list.first_tokens = malformed_function_argument_gap_list.items[1];
    malformed_function_argument_gap_list.last_tokens = malformed_function_argument_gap_list.items[1];
    malformed_function_argument_gap_list.items = malformed_function_argument_gap_list.items[1..2];
    malformed_function_argument_gap_list.expression_items = malformed_function_argument_gap_list.expression_items[1..2];
    malformed_function_argument_gap_list.expressions = malformed_function_argument_gap_list.expressions[1..2];
    if (malformed_function_argument_gap_list.alias_items.len != 0) malformed_function_argument_gap_list.alias_items = malformed_function_argument_gap_list.alias_items[1..2];
    if (malformed_function_argument_gap_list.alias_name_items.len != 0) malformed_function_argument_gap_list.alias_name_items = malformed_function_argument_gap_list.alias_name_items[1..2];
    if (malformed_function_argument_gap_list.direction_items.len != 0) malformed_function_argument_gap_list.direction_items = malformed_function_argument_gap_list.direction_items[1..2];
    if (malformed_function_argument_gap_list.directions.len != 0) malformed_function_argument_gap_list.directions = malformed_function_argument_gap_list.directions[1..2];
    if (malformed_function_argument_gap_list.order_using_operator_items.len != 0) malformed_function_argument_gap_list.order_using_operator_items = malformed_function_argument_gap_list.order_using_operator_items[1..2];
    if (malformed_function_argument_gap_list.nulls_order_items.len != 0) malformed_function_argument_gap_list.nulls_order_items = malformed_function_argument_gap_list.nulls_order_items[1..2];
    if (malformed_function_argument_gap_list.nulls_orders.len != 0) malformed_function_argument_gap_list.nulls_orders = malformed_function_argument_gap_list.nulls_orders[1..2];
    malformed_function_argument_gap_list.count = 1;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(malformed_function_argument_list_parsed_sql.items(), malformed_function_argument_list_read_ast),
    );

    var malformed_function_argument_order_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT array_agg(DISTINCT status ORDER BY id DESC) AS statuses FROM usage_records WHERE kind = 'order'",
    );
    defer malformed_function_argument_order_parsed_sql.deinit(alloc);
    const malformed_function_argument_order_generated_raw = malformed_function_argument_order_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_function_argument_order_read_ast = switch (malformed_function_argument_order_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_function_argument_order_read_ast.projection_items.expressions[0].argument_order_tokens =
        malformed_function_argument_order_read_ast.projection_items.expressions[0].argument_value_tokens;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_function_argument_order_parsed_sql, malformed_function_argument_order_read_ast),
    );

    var malformed_function_filter_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT count(*) FILTER (WHERE status = 'open') AS open_count FROM usage_records WHERE kind = 'order'",
    );
    defer malformed_function_filter_parsed_sql.deinit(alloc);
    const malformed_function_filter_generated_raw = malformed_function_filter_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_function_filter_read_ast = switch (malformed_function_filter_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_function_filter_read_ast.projection_items.expressions[0].filter_predicate_tokens =
        malformed_function_filter_read_ast.projection_items.expressions[0].filter_tokens;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_function_filter_parsed_sql, malformed_function_filter_read_ast),
    );

    var malformed_function_within_group_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY id DESC) AS median_id FROM usage_records WHERE kind = 'order'",
    );
    defer malformed_function_within_group_parsed_sql.deinit(alloc);
    const malformed_function_within_group_generated_raw = malformed_function_within_group_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_function_within_group_read_ast = switch (malformed_function_within_group_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_function_within_group_read_ast.projection_items.expressions[0].within_group_order_tokens =
        malformed_function_within_group_read_ast.projection_items.expressions[0].within_group_tokens;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_function_within_group_parsed_sql, malformed_function_within_group_read_ast),
    );

    var malformed_comparison_expression_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE kind = 'order'",
    );
    defer malformed_comparison_expression_parsed_sql.deinit(alloc);
    const malformed_comparison_expression_generated_raw = malformed_comparison_expression_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_comparison_expression_read_ast = switch (malformed_comparison_expression_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_comparison_expression_read_ast.where_expression.operator_tokens = null;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_comparison_expression_parsed_sql, malformed_comparison_expression_read_ast),
    );
    malformed_comparison_expression_read_ast = switch (malformed_comparison_expression_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_comparison_expression_read_ast.where_expression.kind = .contains;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_comparison_expression_parsed_sql, malformed_comparison_expression_read_ast),
    );
    malformed_comparison_expression_read_ast = switch (malformed_comparison_expression_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_comparison_expression_read_ast.where_expression.function_name_tokens =
        malformed_comparison_expression_read_ast.where_expression.left_tokens;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_comparison_expression_parsed_sql, malformed_comparison_expression_read_ast),
    );

    var malformed_json_operator_kind_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE metadata->>'source' = 'api'",
    );
    defer malformed_json_operator_kind_parsed_sql.deinit(alloc);
    const malformed_json_operator_kind_generated_raw = malformed_json_operator_kind_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_json_operator_kind_read_ast = switch (malformed_json_operator_kind_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_json_operator_kind_read_ast.where_expression.left_expression.?.kind = .json_access;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_json_operator_kind_parsed_sql, malformed_json_operator_kind_read_ast),
    );

    var malformed_concat_operator_kind_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE status || ':' || id = 'open:u1'",
    );
    defer malformed_concat_operator_kind_parsed_sql.deinit(alloc);
    const malformed_concat_operator_kind_generated_raw = malformed_concat_operator_kind_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_concat_operator_kind_read_ast = switch (malformed_concat_operator_kind_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_concat_operator_kind_read_ast.where_expression.left_expression.?.kind = .additive;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_concat_operator_kind_parsed_sql, malformed_concat_operator_kind_read_ast),
    );

    var malformed_regex_operator_kind_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE status ~ 'op.*'",
    );
    defer malformed_regex_operator_kind_parsed_sql.deinit(alloc);
    const malformed_regex_operator_kind_generated_raw = malformed_regex_operator_kind_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_regex_operator_kind_read_ast = switch (malformed_regex_operator_kind_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_regex_operator_kind_read_ast.where_expression.kind = .regex_imatch;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_regex_operator_kind_parsed_sql, malformed_regex_operator_kind_read_ast),
    );

    var malformed_not_like_payload_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE status NOT LIKE 'op%'",
    );
    defer malformed_not_like_payload_parsed_sql.deinit(alloc);
    const malformed_not_like_payload_generated_raw = malformed_not_like_payload_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_not_like_payload_read_ast = switch (malformed_not_like_payload_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_not_like_payload_read_ast.where_expression.argument_tokens =
        malformed_not_like_payload_read_ast.where_expression.right_tokens;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_not_like_payload_parsed_sql, malformed_not_like_payload_read_ast),
    );

    var malformed_quantified_comparison_payload_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE status = ANY(ARRAY['active','pending']::text[])",
    );
    defer malformed_quantified_comparison_payload_parsed_sql.deinit(alloc);
    const malformed_quantified_comparison_payload_generated_raw = malformed_quantified_comparison_payload_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_quantified_comparison_payload_read_ast = switch (malformed_quantified_comparison_payload_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_quantified_comparison_payload_read_ast.where_expression.filter_tokens =
        malformed_quantified_comparison_payload_read_ast.where_expression.right_tokens;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_quantified_comparison_payload_parsed_sql, malformed_quantified_comparison_payload_read_ast),
    );

    var malformed_is_true_expression_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE status IS TRUE",
    );
    defer malformed_is_true_expression_parsed_sql.deinit(alloc);
    const malformed_is_true_expression_generated_raw = malformed_is_true_expression_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_is_true_expression_read_ast = switch (malformed_is_true_expression_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_is_true_expression_read_ast.where_expression.kind = .is_false;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_is_true_expression_parsed_sql, malformed_is_true_expression_read_ast),
    );
    malformed_is_true_expression_read_ast = switch (malformed_is_true_expression_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_is_true_expression_read_ast.where_expression.function_name_tokens =
        malformed_is_true_expression_read_ast.where_expression.left_tokens;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_is_true_expression_parsed_sql, malformed_is_true_expression_read_ast),
    );

    var malformed_is_not_false_expression_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE status IS NOT FALSE",
    );
    defer malformed_is_not_false_expression_parsed_sql.deinit(alloc);
    const malformed_is_not_false_expression_generated_raw = malformed_is_not_false_expression_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_is_not_false_expression_read_ast = switch (malformed_is_not_false_expression_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_is_not_false_expression_read_ast.where_expression.kind = .is_false;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_is_not_false_expression_parsed_sql, malformed_is_not_false_expression_read_ast),
    );

    var malformed_postfix_isnull_expression_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE status ISNULL",
    );
    defer malformed_postfix_isnull_expression_parsed_sql.deinit(alloc);
    const malformed_postfix_isnull_expression_generated_raw = malformed_postfix_isnull_expression_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_postfix_isnull_expression_read_ast = switch (malformed_postfix_isnull_expression_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_postfix_isnull_expression_read_ast.where_expression.right_tokens =
        malformed_postfix_isnull_expression_read_ast.where_expression.left_tokens;
    malformed_postfix_isnull_expression_read_ast.where_expression.right_expression_kind =
        malformed_postfix_isnull_expression_read_ast.where_expression.left_expression_kind;
    malformed_postfix_isnull_expression_read_ast.where_expression.right_expression =
        malformed_postfix_isnull_expression_read_ast.where_expression.left_expression;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_postfix_isnull_expression_parsed_sql, malformed_postfix_isnull_expression_read_ast),
    );

    var malformed_comparison_child_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE kind = 'order'",
    );
    defer malformed_comparison_child_parsed_sql.deinit(alloc);
    const malformed_comparison_child_generated_raw = malformed_comparison_child_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_comparison_child_read_ast = switch (malformed_comparison_child_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_comparison_child_read_ast.where_expression.left_expression.?.tokens = malformed_comparison_child_read_ast.projection_items.expression_items[0];
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_comparison_child_parsed_sql, malformed_comparison_child_read_ast),
    );

    var malformed_expression_owned_range_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE status LIKE 'op%' ESCAPE '\\'",
    );
    defer malformed_expression_owned_range_parsed_sql.deinit(alloc);
    const malformed_expression_owned_range_generated_raw = malformed_expression_owned_range_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_expression_owned_range_read_ast = switch (malformed_expression_owned_range_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_expression_owned_range_read_ast.where_expression.escape_tokens =
        malformed_expression_owned_range_read_ast.projection_items.expression_items[0];
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_expression_owned_range_parsed_sql, malformed_expression_owned_range_read_ast),
    );
    malformed_expression_owned_range_read_ast = switch (malformed_expression_owned_range_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_expression_owned_range_read_ast.where_expression.escape_expression.?.tokens =
        malformed_expression_owned_range_read_ast.projection_items.expression_items[0];
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(malformed_expression_owned_range_parsed_sql.items(), malformed_expression_owned_range_read_ast),
    );

    var malformed_boolean_chain_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE kind = 'order' AND tenant = 'acme' AND id = '1'",
    );
    defer malformed_boolean_chain_parsed_sql.deinit(alloc);
    const malformed_boolean_chain_generated_raw = malformed_boolean_chain_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_boolean_chain_read_ast = switch (malformed_boolean_chain_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_boolean_chain_read_ast.where_expression.boolean_condition_count = 1;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_boolean_chain_parsed_sql, malformed_boolean_chain_read_ast),
    );

    malformed_boolean_chain_read_ast = switch (malformed_boolean_chain_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_boolean_chain_read_ast.where_expression.boolean_condition_items.first_tokens =
        malformed_boolean_chain_read_ast.where_expression.boolean_condition_items.last_tokens;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_boolean_chain_parsed_sql, malformed_boolean_chain_read_ast),
    );

    malformed_boolean_chain_read_ast = switch (malformed_boolean_chain_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    if (malformed_boolean_chain_read_ast.where_expression.boolean_condition_items.items.len < 2) return error.TestUnexpectedResult;
    malformed_boolean_chain_read_ast.where_expression.boolean_condition_items.items[1].start += 1;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(malformed_boolean_chain_parsed_sql.items(), malformed_boolean_chain_read_ast),
    );

    malformed_boolean_chain_read_ast = switch (malformed_boolean_chain_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_boolean_chain_read_ast.where_expression.boolean_condition_items.expression_items[0] =
        malformed_boolean_chain_read_ast.projection_items.expression_items[0];
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_boolean_chain_parsed_sql, malformed_boolean_chain_read_ast),
    );

    malformed_boolean_chain_read_ast = switch (malformed_boolean_chain_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_boolean_chain_read_ast.where_expression.boolean_condition_items.expressions[2].tokens =
        malformed_boolean_chain_read_ast.projection_items.expression_items[0];
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_boolean_chain_parsed_sql, malformed_boolean_chain_read_ast),
    );

    var malformed_grouped_expression_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE (status = 'open')",
    );
    defer malformed_grouped_expression_parsed_sql.deinit(alloc);
    const malformed_grouped_expression_generated_raw = malformed_grouped_expression_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_grouped_expression_read_ast = switch (malformed_grouped_expression_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_grouped_expression_read_ast.where_expression.inner_tokens =
        malformed_grouped_expression_read_ast.where_expression.tokens;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_grouped_expression_parsed_sql, malformed_grouped_expression_read_ast),
    );
    malformed_grouped_expression_read_ast = switch (malformed_grouped_expression_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_grouped_expression_read_ast.where_expression.function_name_tokens =
        malformed_grouped_expression_read_ast.where_expression.inner_tokens;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_grouped_expression_parsed_sql, malformed_grouped_expression_read_ast),
    );

    var malformed_cast_expression_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT CAST(id AS text) AS id_text FROM usage_records WHERE kind = 'order'",
    );
    defer malformed_cast_expression_parsed_sql.deinit(alloc);
    const malformed_cast_expression_generated_raw = malformed_cast_expression_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_cast_expression_read_ast = switch (malformed_cast_expression_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_cast_expression_read_ast.projection_items.expressions[0].cast_type_tokens =
        malformed_cast_expression_read_ast.projection_items.expressions[0].cast_expression_tokens;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_cast_expression_parsed_sql, malformed_cast_expression_read_ast),
    );
    malformed_cast_expression_read_ast = switch (malformed_cast_expression_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_cast_expression_read_ast.projection_items.expressions[0].operator_tokens =
        malformed_cast_expression_read_ast.projection_items.expressions[0].cast_expression_tokens;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_cast_expression_parsed_sql, malformed_cast_expression_read_ast),
    );

    var malformed_unary_expression_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT -id AS neg_id FROM usage_records WHERE kind = 'order'",
    );
    defer malformed_unary_expression_parsed_sql.deinit(alloc);
    const malformed_unary_expression_generated_raw = malformed_unary_expression_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_unary_expression_read_ast = switch (malformed_unary_expression_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_unary_expression_read_ast.projection_items.expressions[0].right_expression_kind = .function_call;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_unary_expression_parsed_sql, malformed_unary_expression_read_ast),
    );
    malformed_unary_expression_read_ast = switch (malformed_unary_expression_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_unary_expression_read_ast.projection_items.expressions[0].function_name_tokens =
        malformed_unary_expression_read_ast.projection_items.expressions[0].right_tokens;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_unary_expression_parsed_sql, malformed_unary_expression_read_ast),
    );

    var malformed_between_expression_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE score BETWEEN 1 AND 10",
    );
    defer malformed_between_expression_parsed_sql.deinit(alloc);
    const malformed_between_expression_generated_raw = malformed_between_expression_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_between_expression_read_ast = switch (malformed_between_expression_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_between_expression_read_ast.where_expression.between_upper_expression = null;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_between_expression_parsed_sql, malformed_between_expression_read_ast),
    );

    var malformed_exists_subquery_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE EXISTS (SELECT 1 FROM thresholds WHERE active IS TRUE)",
    );
    defer malformed_exists_subquery_parsed_sql.deinit(alloc);
    const malformed_exists_subquery_generated_raw = malformed_exists_subquery_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_exists_subquery_read_ast = switch (malformed_exists_subquery_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_exists_subquery_read_ast.where_expression.right_expression_kind = .token_range;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_exists_subquery_parsed_sql, malformed_exists_subquery_read_ast),
    );
    malformed_exists_subquery_read_ast = switch (malformed_exists_subquery_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_exists_subquery_read_ast.where_expression.filter_tokens =
        malformed_exists_subquery_read_ast.where_expression.right_tokens;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_exists_subquery_parsed_sql, malformed_exists_subquery_read_ast),
    );

    var malformed_exists_subquery_projection_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE EXISTS (SELECT 1 FROM thresholds WHERE active IS TRUE)",
    );
    defer malformed_exists_subquery_projection_parsed_sql.deinit(alloc);
    const malformed_exists_subquery_projection_generated_raw = malformed_exists_subquery_projection_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_exists_subquery_projection_read_ast = switch (malformed_exists_subquery_projection_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_exists_subquery_projection_read_ast.where_expression.right_expression.?.subquery_projection_tokens = null;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_exists_subquery_projection_parsed_sql, malformed_exists_subquery_projection_read_ast),
    );

    malformed_exists_subquery_projection_read_ast = switch (malformed_exists_subquery_projection_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_exists_subquery_projection_read_ast.where_expression.right_expression.?.subquery_projection_items.count = 0;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_exists_subquery_projection_parsed_sql, malformed_exists_subquery_projection_read_ast),
    );

    var malformed_subquery_projection_gap_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE EXISTS (SELECT tenant, id FROM thresholds WHERE active IS TRUE)",
    );
    defer malformed_subquery_projection_gap_parsed_sql.deinit(alloc);
    const malformed_subquery_projection_gap_generated_raw = malformed_subquery_projection_gap_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_subquery_projection_gap_read_ast = switch (malformed_subquery_projection_gap_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    const malformed_subquery_projection_gap_list = &malformed_subquery_projection_gap_read_ast.where_expression.right_expression.?.subquery_projection_items;
    if (malformed_subquery_projection_gap_list.count < 2) return error.TestUnexpectedResult;
    malformed_subquery_projection_gap_list.first_tokens = malformed_subquery_projection_gap_list.items[1];
    malformed_subquery_projection_gap_list.last_tokens = malformed_subquery_projection_gap_list.items[1];
    malformed_subquery_projection_gap_list.items = malformed_subquery_projection_gap_list.items[1..2];
    malformed_subquery_projection_gap_list.expression_items = malformed_subquery_projection_gap_list.expression_items[1..2];
    malformed_subquery_projection_gap_list.expressions = malformed_subquery_projection_gap_list.expressions[1..2];
    if (malformed_subquery_projection_gap_list.alias_items.len != 0) malformed_subquery_projection_gap_list.alias_items = malformed_subquery_projection_gap_list.alias_items[1..2];
    if (malformed_subquery_projection_gap_list.alias_name_items.len != 0) malformed_subquery_projection_gap_list.alias_name_items = malformed_subquery_projection_gap_list.alias_name_items[1..2];
    if (malformed_subquery_projection_gap_list.direction_items.len != 0) malformed_subquery_projection_gap_list.direction_items = malformed_subquery_projection_gap_list.direction_items[1..2];
    if (malformed_subquery_projection_gap_list.directions.len != 0) malformed_subquery_projection_gap_list.directions = malformed_subquery_projection_gap_list.directions[1..2];
    if (malformed_subquery_projection_gap_list.order_using_operator_items.len != 0) malformed_subquery_projection_gap_list.order_using_operator_items = malformed_subquery_projection_gap_list.order_using_operator_items[1..2];
    if (malformed_subquery_projection_gap_list.nulls_order_items.len != 0) malformed_subquery_projection_gap_list.nulls_order_items = malformed_subquery_projection_gap_list.nulls_order_items[1..2];
    if (malformed_subquery_projection_gap_list.nulls_orders.len != 0) malformed_subquery_projection_gap_list.nulls_orders = malformed_subquery_projection_gap_list.nulls_orders[1..2];
    malformed_subquery_projection_gap_list.count = 1;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(malformed_subquery_projection_gap_parsed_sql.items(), malformed_subquery_projection_gap_read_ast),
    );

    var malformed_exists_subquery_select_keyword_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE EXISTS (SELECT 1 FROM thresholds WHERE active IS TRUE)",
    );
    defer malformed_exists_subquery_select_keyword_parsed_sql.deinit(alloc);
    const malformed_exists_subquery_select_keyword_generated_raw = malformed_exists_subquery_select_keyword_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    const malformed_exists_subquery_select_keyword_read_ast = switch (malformed_exists_subquery_select_keyword_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    const malformed_exists_subquery_select_tokens =
        malformed_exists_subquery_select_keyword_read_ast.where_expression.right_expression.?.subquery_select_tokens orelse return error.UnsupportedSqlShape;
    @constCast(malformed_exists_subquery_select_keyword_parsed_sql.items())[malformed_exists_subquery_select_tokens.start].keyword = .from;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_exists_subquery_select_keyword_parsed_sql, malformed_exists_subquery_select_keyword_read_ast),
    );

    malformed_exists_subquery_projection_read_ast = switch (malformed_exists_subquery_projection_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_exists_subquery_projection_read_ast.where_expression.right_expression.?.subquery_source_tokens =
        malformed_exists_subquery_projection_read_ast.where_expression.right_expression.?.subquery_projection_tokens;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_exists_subquery_projection_parsed_sql, malformed_exists_subquery_projection_read_ast),
    );

    malformed_exists_subquery_projection_read_ast = switch (malformed_exists_subquery_projection_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_exists_subquery_projection_read_ast.where_expression.right_expression.?.subquery_where_tokens =
        malformed_exists_subquery_projection_read_ast.where_expression.right_expression.?.subquery_source_tokens;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_exists_subquery_projection_parsed_sql, malformed_exists_subquery_projection_read_ast),
    );

    malformed_exists_subquery_projection_read_ast = switch (malformed_exists_subquery_projection_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_exists_subquery_projection_read_ast.where_expression.right_expression.?.subquery_projection_items.first_tokens =
        malformed_exists_subquery_projection_read_ast.projection_items.items[0];
    malformed_exists_subquery_projection_read_ast.where_expression.right_expression.?.subquery_projection_items.last_tokens =
        malformed_exists_subquery_projection_read_ast.projection_items.items[0];
    malformed_exists_subquery_projection_read_ast.where_expression.right_expression.?.subquery_projection_items.items[0] =
        malformed_exists_subquery_projection_read_ast.projection_items.items[0];
    malformed_exists_subquery_projection_read_ast.where_expression.right_expression.?.subquery_projection_items.expression_items[0] =
        malformed_exists_subquery_projection_read_ast.projection_items.expression_items[0];
    malformed_exists_subquery_projection_read_ast.where_expression.right_expression.?.subquery_projection_items.expressions[0].tokens =
        malformed_exists_subquery_projection_read_ast.projection_items.expression_items[0];
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_exists_subquery_projection_parsed_sql, malformed_exists_subquery_projection_read_ast),
    );

    malformed_exists_subquery_projection_read_ast = switch (malformed_exists_subquery_projection_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_exists_subquery_projection_read_ast.where_expression.right_expression.?.subquery_where_expression_kind = .comparison;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_exists_subquery_projection_parsed_sql, malformed_exists_subquery_projection_read_ast),
    );
    malformed_exists_subquery_projection_read_ast = switch (malformed_exists_subquery_projection_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_exists_subquery_projection_read_ast.where_expression.right_expression.?.function_name_tokens =
        malformed_exists_subquery_projection_read_ast.where_expression.right_expression.?.subquery_select_tokens;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_exists_subquery_projection_parsed_sql, malformed_exists_subquery_projection_read_ast),
    );

    var malformed_subquery_set_operation_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE EXISTS (SELECT id FROM thresholds UNION SELECT id FROM archived_thresholds)",
    );
    defer malformed_subquery_set_operation_parsed_sql.deinit(alloc);
    const malformed_subquery_set_operation_generated_raw = malformed_subquery_set_operation_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_subquery_set_operation_read_ast = switch (malformed_subquery_set_operation_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_subquery_set_operation_read_ast.where_expression.right_expression.?.subquery_set_operation.?.right_projection_items.count = 0;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_subquery_set_operation_parsed_sql, malformed_subquery_set_operation_read_ast),
    );

    var malformed_subquery_tail_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE id IN (SELECT id FROM archived_records ORDER BY id LIMIT 5)",
    );
    defer malformed_subquery_tail_parsed_sql.deinit(alloc);
    const malformed_subquery_tail_generated_raw = malformed_subquery_tail_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_subquery_tail_read_ast = switch (malformed_subquery_tail_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_subquery_tail_read_ast.where_expression.right_expression.?.subquery_tail.?.order_items.count = 0;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_subquery_tail_parsed_sql, malformed_subquery_tail_read_ast),
    );
    malformed_subquery_tail_read_ast = switch (malformed_subquery_tail_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_subquery_tail_read_ast.where_expression.right_expression.?.subquery_tail.?.limit_expression.?.tokens =
        malformed_subquery_tail_read_ast.projection_items.expression_items[0];
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_subquery_tail_parsed_sql, malformed_subquery_tail_read_ast),
    );
    malformed_subquery_tail_read_ast = switch (malformed_subquery_tail_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    var malformed_subquery_tail_missing_limit_expression = malformed_subquery_tail_read_ast.where_expression.right_expression.?.*;
    var malformed_subquery_tail_missing_limit_tail = malformed_subquery_tail_missing_limit_expression.subquery_tail.?.*;
    malformed_subquery_tail_missing_limit_expression.subquery_tail = &malformed_subquery_tail_missing_limit_tail;
    malformed_subquery_tail_read_ast.where_expression.right_expression = &malformed_subquery_tail_missing_limit_expression;
    malformed_subquery_tail_missing_limit_tail.limit_tokens = null;
    malformed_subquery_tail_missing_limit_tail.limit_expression = null;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(malformed_subquery_tail_parsed_sql.items(), malformed_subquery_tail_read_ast),
    );

    var malformed_subquery_tail_order_gap_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE id IN (SELECT id FROM archived_records ORDER BY tenant, id LIMIT 5)",
    );
    defer malformed_subquery_tail_order_gap_parsed_sql.deinit(alloc);
    const malformed_subquery_tail_order_gap_generated_raw = malformed_subquery_tail_order_gap_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_subquery_tail_order_gap_read_ast = switch (malformed_subquery_tail_order_gap_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    var malformed_subquery_tail_order_gap_expression = malformed_subquery_tail_order_gap_read_ast.where_expression.right_expression.?.*;
    var malformed_subquery_tail_order_gap_tail = malformed_subquery_tail_order_gap_expression.subquery_tail.?.*;
    malformed_subquery_tail_order_gap_expression.subquery_tail = &malformed_subquery_tail_order_gap_tail;
    malformed_subquery_tail_order_gap_read_ast.where_expression.right_expression = &malformed_subquery_tail_order_gap_expression;
    var malformed_subquery_tail_order_gap_list = &malformed_subquery_tail_order_gap_tail.order_items;
    if (malformed_subquery_tail_order_gap_list.count < 2) return error.TestUnexpectedResult;
    malformed_subquery_tail_order_gap_list.first_tokens = malformed_subquery_tail_order_gap_list.items[1];
    malformed_subquery_tail_order_gap_list.last_tokens = malformed_subquery_tail_order_gap_list.items[1];
    malformed_subquery_tail_order_gap_list.items = malformed_subquery_tail_order_gap_list.items[1..2];
    malformed_subquery_tail_order_gap_list.expression_items = malformed_subquery_tail_order_gap_list.expression_items[1..2];
    malformed_subquery_tail_order_gap_list.expressions = malformed_subquery_tail_order_gap_list.expressions[1..2];
    if (malformed_subquery_tail_order_gap_list.alias_items.len != 0) malformed_subquery_tail_order_gap_list.alias_items = malformed_subquery_tail_order_gap_list.alias_items[1..2];
    if (malformed_subquery_tail_order_gap_list.alias_name_items.len != 0) malformed_subquery_tail_order_gap_list.alias_name_items = malformed_subquery_tail_order_gap_list.alias_name_items[1..2];
    if (malformed_subquery_tail_order_gap_list.direction_items.len != 0) malformed_subquery_tail_order_gap_list.direction_items = malformed_subquery_tail_order_gap_list.direction_items[1..2];
    if (malformed_subquery_tail_order_gap_list.directions.len != 0) malformed_subquery_tail_order_gap_list.directions = malformed_subquery_tail_order_gap_list.directions[1..2];
    if (malformed_subquery_tail_order_gap_list.order_using_operator_items.len != 0) malformed_subquery_tail_order_gap_list.order_using_operator_items = malformed_subquery_tail_order_gap_list.order_using_operator_items[1..2];
    if (malformed_subquery_tail_order_gap_list.nulls_order_items.len != 0) malformed_subquery_tail_order_gap_list.nulls_order_items = malformed_subquery_tail_order_gap_list.nulls_order_items[1..2];
    if (malformed_subquery_tail_order_gap_list.nulls_orders.len != 0) malformed_subquery_tail_order_gap_list.nulls_orders = malformed_subquery_tail_order_gap_list.nulls_orders[1..2];
    malformed_subquery_tail_order_gap_list.count = 1;
    malformed_subquery_tail_order_gap_tail.order_first_expression = &malformed_subquery_tail_order_gap_list.expressions[0];
    malformed_subquery_tail_order_gap_tail.order_last_expression = &malformed_subquery_tail_order_gap_list.expressions[0];
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(malformed_subquery_tail_order_gap_parsed_sql.items(), malformed_subquery_tail_order_gap_read_ast),
    );

    var malformed_in_subquery_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE id IN (SELECT id FROM archived_records WHERE archived IS TRUE)",
    );
    defer malformed_in_subquery_parsed_sql.deinit(alloc);
    const malformed_in_subquery_generated_raw = malformed_in_subquery_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_in_subquery_read_ast = switch (malformed_in_subquery_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_in_subquery_read_ast.where_expression.right_expression_kind = .token_range;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_in_subquery_parsed_sql, malformed_in_subquery_read_ast),
    );

    var malformed_like_any_subquery_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE lower(status) LIKE ANY (SELECT pattern FROM active_patterns)",
    );
    defer malformed_like_any_subquery_parsed_sql.deinit(alloc);
    const malformed_like_any_subquery_generated_raw = malformed_like_any_subquery_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_like_any_subquery_read_ast = switch (malformed_like_any_subquery_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_like_any_subquery_read_ast.where_expression.right_expression_kind = .token_range;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_like_any_subquery_parsed_sql, malformed_like_any_subquery_read_ast),
    );

    var malformed_scalar_subquery_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT (SELECT status FROM usage_records WHERE id = 'u1') AS status_scalar FROM usage_records",
    );
    defer malformed_scalar_subquery_parsed_sql.deinit(alloc);
    const malformed_scalar_subquery_generated_raw = malformed_scalar_subquery_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_scalar_subquery_read_ast = switch (malformed_scalar_subquery_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_scalar_subquery_read_ast.projection_items.expressions[0].subquery_where_tokens =
        malformed_scalar_subquery_read_ast.projection_items.expressions[0].subquery_source_tokens;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(malformed_scalar_subquery_parsed_sql.items(), malformed_scalar_subquery_read_ast),
    );

    var malformed_not_exists_subquery_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE NOT EXISTS (SELECT 1 FROM usage_records WHERE status = 'archived')",
    );
    defer malformed_not_exists_subquery_parsed_sql.deinit(alloc);
    const malformed_not_exists_subquery_generated_raw = malformed_not_exists_subquery_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_not_exists_subquery_read_ast = switch (malformed_not_exists_subquery_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_not_exists_subquery_read_ast.where_expression.negation_tokens = null;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(malformed_not_exists_subquery_parsed_sql.items(), malformed_not_exists_subquery_read_ast),
    );

    var malformed_not_in_subquery_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE status NOT IN (SELECT status FROM usage_records WHERE kind = 'archived')",
    );
    defer malformed_not_in_subquery_parsed_sql.deinit(alloc);
    const malformed_not_in_subquery_generated_raw = malformed_not_in_subquery_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_not_in_subquery_read_ast = switch (malformed_not_in_subquery_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_not_in_subquery_read_ast.where_expression.negation_tokens = null;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(malformed_not_in_subquery_parsed_sql.items(), malformed_not_in_subquery_read_ast),
    );

    var malformed_quantified_subquery_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE status = ANY (SELECT status FROM usage_records WHERE kind = 'active')",
    );
    defer malformed_quantified_subquery_parsed_sql.deinit(alloc);
    const malformed_quantified_subquery_generated_raw = malformed_quantified_subquery_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_quantified_subquery_read_ast = switch (malformed_quantified_subquery_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_quantified_subquery_read_ast.where_expression.quantifier_tokens = null;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(malformed_quantified_subquery_parsed_sql.items(), malformed_quantified_subquery_read_ast),
    );

    var malformed_correlated_subquery_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT outer_usage.id FROM usage_records AS outer_usage WHERE EXISTS (SELECT 1 FROM usage_records WHERE usage_records.tenant = outer_usage.tenant)",
    );
    defer malformed_correlated_subquery_parsed_sql.deinit(alloc);
    const malformed_correlated_subquery_generated_raw = malformed_correlated_subquery_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_correlated_subquery_read_ast = switch (malformed_correlated_subquery_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_correlated_subquery_read_ast.where_expression.right_expression.?.subquery_where_expression_kind = .token_range;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(malformed_correlated_subquery_parsed_sql.items(), malformed_correlated_subquery_read_ast),
    );

    var malformed_nested_subquery_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE status IN (SELECT status FROM usage_records WHERE tenant IN (SELECT tenant FROM usage_records WHERE kind = 'active'))",
    );
    defer malformed_nested_subquery_parsed_sql.deinit(alloc);
    const malformed_nested_subquery_generated_raw = malformed_nested_subquery_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_nested_subquery_read_ast = switch (malformed_nested_subquery_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_nested_subquery_read_ast.where_expression.right_expression.?.subquery_where_expression.?.right_expression_kind = .token_range;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(malformed_nested_subquery_parsed_sql.items(), malformed_nested_subquery_read_ast),
    );

    var malformed_cte_contained_subquery_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "WITH open_usage AS (SELECT id, status FROM usage_records WHERE status = 'open') SELECT id FROM usage_records WHERE status IN (SELECT status FROM open_usage)",
    );
    defer malformed_cte_contained_subquery_parsed_sql.deinit(alloc);
    const malformed_cte_contained_subquery_generated_raw = malformed_cte_contained_subquery_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_cte_contained_subquery_read_ast = switch (malformed_cte_contained_subquery_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_cte_contained_subquery_read_ast.where_expression.right_expression.?.subquery_source_tokens =
        malformed_cte_contained_subquery_read_ast.where_expression.right_expression.?.subquery_projection_tokens;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(malformed_cte_contained_subquery_parsed_sql.items(), malformed_cte_contained_subquery_read_ast),
    );

    var malformed_distinct_keyword_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT DISTINCT status FROM usage_records",
    );
    defer malformed_distinct_keyword_parsed_sql.deinit(alloc);
    const malformed_distinct_keyword_generated_raw = malformed_distinct_keyword_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    const malformed_distinct_keyword_read_ast = switch (malformed_distinct_keyword_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    const malformed_distinct_tokens = malformed_distinct_keyword_read_ast.distinct_tokens orelse return error.UnsupportedSqlShape;
    @constCast(malformed_distinct_keyword_parsed_sql.items())[malformed_distinct_tokens.start].keyword = .from;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_distinct_keyword_parsed_sql, malformed_distinct_keyword_read_ast),
    );

    var malformed_distinct_on_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT DISTINCT ON (organization_id) organization_id, id FROM usage_records ORDER BY organization_id ASC, created_at DESC",
    );
    defer malformed_distinct_on_parsed_sql.deinit(alloc);
    const malformed_distinct_on_generated_raw = malformed_distinct_on_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_distinct_on_read_ast = switch (malformed_distinct_on_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_distinct_on_read_ast.distinct_on_items.count = 0;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_distinct_on_parsed_sql, malformed_distinct_on_read_ast),
    );

    var malformed_case_expression_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT CASE WHEN status IS NULL THEN 'missing' ELSE status END AS status_key FROM usage_records WHERE kind = 'order'",
    );
    defer malformed_case_expression_parsed_sql.deinit(alloc);
    const malformed_case_expression_generated_raw = malformed_case_expression_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_case_expression_read_ast = switch (malformed_case_expression_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_case_expression_read_ast.projection_items.expressions[0].case_condition_items.count = 0;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_case_expression_parsed_sql, malformed_case_expression_read_ast),
    );
    malformed_case_expression_read_ast = switch (malformed_case_expression_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_case_expression_read_ast.projection_items.expressions[0].case_else_expression_tokens =
        malformed_case_expression_read_ast.projection_items.expressions[0].case_else_tokens;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_case_expression_parsed_sql, malformed_case_expression_read_ast),
    );
    malformed_case_expression_read_ast = switch (malformed_case_expression_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_case_expression_read_ast.projection_items.expressions[0].case_result_items.expressions[0].tokens =
        malformed_case_expression_read_ast.projection_items.expression_items[0];
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(malformed_case_expression_parsed_sql.items(), malformed_case_expression_read_ast),
    );
    malformed_case_expression_read_ast = switch (malformed_case_expression_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_case_expression_read_ast.projection_items.expressions[0].operator_tokens =
        malformed_case_expression_read_ast.projection_items.expressions[0].case_first_condition_tokens;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_case_expression_parsed_sql, malformed_case_expression_read_ast),
    );

    var malformed_extract_expression_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT EXTRACT(dow FROM amount) AS amount_dow FROM usage_records WHERE id = 'u1'",
    );
    defer malformed_extract_expression_parsed_sql.deinit(alloc);
    const malformed_extract_expression_generated_raw = malformed_extract_expression_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_extract_expression_read_ast = switch (malformed_extract_expression_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_extract_expression_read_ast.projection_items.expressions[0].extract_source_expression = null;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_extract_expression_parsed_sql, malformed_extract_expression_read_ast),
    );
    malformed_extract_expression_read_ast = switch (malformed_extract_expression_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_extract_expression_read_ast.projection_items.expressions[0].operator_tokens =
        malformed_extract_expression_read_ast.projection_items.expressions[0].extract_source_tokens;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_extract_expression_parsed_sql, malformed_extract_expression_read_ast),
    );
    malformed_extract_expression_read_ast = switch (malformed_extract_expression_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    const malformed_extract_source_tokens = malformed_extract_expression_read_ast.projection_items.expressions[0].extract_source_tokens orelse return error.UnsupportedSqlShape;
    malformed_extract_expression_read_ast.projection_items.expressions[0].extract_source_tokens = .{
        .start = malformed_extract_source_tokens.start - 1,
        .end = malformed_extract_source_tokens.end,
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(malformed_extract_expression_parsed_sql.items(), malformed_extract_expression_read_ast),
    );

    var malformed_array_expression_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE status = ANY(ARRAY['active','pending']::text[])",
    );
    defer malformed_array_expression_parsed_sql.deinit(alloc);
    const malformed_array_expression_generated_raw = malformed_array_expression_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_array_expression_read_ast = switch (malformed_array_expression_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_array_expression_read_ast.where_expression.right_expression.?.inner_expression.?.function_name_tokens =
        malformed_array_expression_read_ast.where_expression.right_expression.?.inner_expression.?.array_tokens;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_array_expression_parsed_sql, malformed_array_expression_read_ast),
    );
    malformed_array_expression_read_ast = switch (malformed_array_expression_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    const malformed_array_inner_expression = malformed_array_expression_read_ast.where_expression.right_expression.?.inner_expression orelse return error.UnsupportedSqlShape;
    const malformed_array_tokens = malformed_array_inner_expression.array_tokens orelse return error.UnsupportedSqlShape;
    malformed_array_inner_expression.array_tokens = .{
        .start = malformed_array_tokens.start - 1,
        .end = malformed_array_tokens.end,
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(malformed_array_expression_parsed_sql.items(), malformed_array_expression_read_ast),
    );

    var malformed_logical_not_expression_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE NOT (status IS NULL)",
    );
    defer malformed_logical_not_expression_parsed_sql.deinit(alloc);
    const malformed_logical_not_expression_generated_raw = malformed_logical_not_expression_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_logical_not_expression_read_ast = switch (malformed_logical_not_expression_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_logical_not_expression_read_ast.where_expression.function_name_tokens =
        malformed_logical_not_expression_read_ast.where_expression.right_tokens;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_logical_not_expression_parsed_sql, malformed_logical_not_expression_read_ast),
    );

    var malformed_current_timestamp_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT CURRENT_TIMESTAMP FROM usage_records WHERE id = 'u1'",
    );
    defer malformed_current_timestamp_parsed_sql.deinit(alloc);
    const malformed_current_timestamp_generated_raw = malformed_current_timestamp_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_current_timestamp_read_ast = switch (malformed_current_timestamp_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_current_timestamp_read_ast.projection_items.expressions[0].operator_tokens =
        malformed_current_timestamp_read_ast.projection_items.expressions[0].tokens;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_current_timestamp_parsed_sql, malformed_current_timestamp_read_ast),
    );
    var malformed_current_timestamp_precision_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT CURRENT_TIMESTAMP(6) FROM usage_records WHERE id = 'u1'",
    );
    defer malformed_current_timestamp_precision_parsed_sql.deinit(alloc);
    const malformed_current_timestamp_precision_generated_raw = malformed_current_timestamp_precision_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_current_timestamp_precision_read_ast = switch (malformed_current_timestamp_precision_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    const malformed_current_timestamp_precision_tokens = malformed_current_timestamp_precision_read_ast.projection_items.expressions[0].current_timestamp_precision_tokens orelse return error.UnsupportedSqlShape;
    malformed_current_timestamp_precision_read_ast.projection_items.expressions[0].current_timestamp_precision_tokens = .{
        .start = malformed_current_timestamp_precision_tokens.start - 1,
        .end = malformed_current_timestamp_precision_tokens.end,
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(malformed_current_timestamp_precision_parsed_sql.items(), malformed_current_timestamp_precision_read_ast),
    );

    var malformed_interval_literal_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT INTERVAL '1 day' FROM usage_records WHERE id = 'u1'",
    );
    defer malformed_interval_literal_parsed_sql.deinit(alloc);
    const malformed_interval_literal_generated_raw = malformed_interval_literal_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_interval_literal_read_ast = switch (malformed_interval_literal_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_interval_literal_read_ast.projection_items.expressions[0].right_expression_kind = .token_range;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_interval_literal_parsed_sql, malformed_interval_literal_read_ast),
    );
    malformed_interval_literal_read_ast = switch (malformed_interval_literal_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    const malformed_interval_value_tokens = malformed_interval_literal_read_ast.projection_items.expressions[0].interval_value_tokens orelse return error.UnsupportedSqlShape;
    malformed_interval_literal_read_ast.projection_items.expressions[0].interval_value_tokens = .{
        .start = malformed_interval_value_tokens.start - 1,
        .end = malformed_interval_value_tokens.end,
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(malformed_interval_literal_parsed_sql.items(), malformed_interval_literal_read_ast),
    );

    var malformed_timestamp_literal_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT TIMESTAMP '2026-01-01 00:00:00' FROM usage_records WHERE id = 'u1'",
    );
    defer malformed_timestamp_literal_parsed_sql.deinit(alloc);
    const malformed_timestamp_literal_generated_raw = malformed_timestamp_literal_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_timestamp_literal_read_ast = switch (malformed_timestamp_literal_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_timestamp_literal_read_ast.projection_items.expressions[0].interval_value_tokens =
        malformed_timestamp_literal_read_ast.projection_items.expressions[0].timestamp_value_tokens;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_timestamp_literal_parsed_sql, malformed_timestamp_literal_read_ast),
    );
    malformed_timestamp_literal_read_ast = switch (malformed_timestamp_literal_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    const malformed_timestamp_type_tokens = malformed_timestamp_literal_read_ast.projection_items.expressions[0].timestamp_type_tokens orelse return error.UnsupportedSqlShape;
    malformed_timestamp_literal_read_ast.projection_items.expressions[0].timestamp_type_tokens = .{
        .start = malformed_timestamp_type_tokens.start,
        .end = malformed_timestamp_type_tokens.end + 1,
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(malformed_timestamp_literal_parsed_sql.items(), malformed_timestamp_literal_read_ast),
    );

    var malformed_join_tree_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT usage_records.id FROM usage_records JOIN accounts ON usage_records.id = accounts.id JOIN tenants ON accounts.tenant = tenants.id",
    );
    defer malformed_join_tree_parsed_sql.deinit(alloc);
    const malformed_join_tree_generated_raw = malformed_join_tree_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_join_tree_read_ast = switch (malformed_join_tree_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_join_tree_read_ast.join_tree_root_index = 0;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_join_tree_parsed_sql, malformed_join_tree_read_ast),
    );

    var malformed_join_child_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT usage_records.id FROM usage_records JOIN accounts ON usage_records.id = accounts.id JOIN tenants ON accounts.tenant = tenants.id",
    );
    defer malformed_join_child_parsed_sql.deinit(alloc);
    const malformed_join_child_generated_raw = malformed_join_child_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_join_child_read_ast = switch (malformed_join_child_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_join_child_read_ast.join_items[1].left_child_index = null;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_join_child_parsed_sql, malformed_join_child_read_ast),
    );

    var malformed_join_gap_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT usage_records.id FROM usage_records JOIN accounts AS a ON usage_records.id = a.id",
    );
    defer malformed_join_gap_parsed_sql.deinit(alloc);
    const malformed_join_gap_generated_raw = malformed_join_gap_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_join_gap_read_ast = switch (malformed_join_gap_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_join_gap_read_ast.join_items[0].right_tokens.start += 1;
    malformed_join_gap_read_ast.join_right_tokens = malformed_join_gap_read_ast.join_items[0].right_tokens;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(malformed_join_gap_parsed_sql.items(), malformed_join_gap_read_ast),
    );

    var malformed_join_kind_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT usage_records.id FROM usage_records LEFT JOIN accounts ON usage_records.id = accounts.id",
    );
    defer malformed_join_kind_parsed_sql.deinit(alloc);
    const malformed_join_kind_generated_raw = malformed_join_kind_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_join_kind_read_ast = switch (malformed_join_kind_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_join_kind_read_ast.join_items[0].kind = .inner;
    malformed_join_kind_read_ast.join_kind = .inner;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(malformed_join_kind_parsed_sql.items(), malformed_join_kind_read_ast),
    );

    var malformed_join_side_table_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT usage_records.id FROM usage_records JOIN accounts ON usage_records.id = accounts.id",
    );
    defer malformed_join_side_table_parsed_sql.deinit(alloc);
    const malformed_join_side_table_generated_raw = malformed_join_side_table_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_join_side_table_read_ast = switch (malformed_join_side_table_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    try validateGeneratedReadAstForStatement(malformed_join_side_table_parsed_sql.items(), malformed_join_side_table_read_ast);
    malformed_join_side_table_read_ast.join_items[0].right_table_tokens = null;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(malformed_join_side_table_parsed_sql.items(), malformed_join_side_table_read_ast),
    );

    var stale_non_lateral_join_payload_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT usage_records.id FROM usage_records JOIN accounts AS a ON usage_records.id = a.id",
    );
    defer stale_non_lateral_join_payload_parsed_sql.deinit(alloc);
    const stale_non_lateral_join_payload_generated_raw = stale_non_lateral_join_payload_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var stale_non_lateral_join_payload_read_ast = switch (stale_non_lateral_join_payload_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    try validateGeneratedReadAstForStatement(stale_non_lateral_join_payload_parsed_sql.items(), stale_non_lateral_join_payload_read_ast);
    stale_non_lateral_join_payload_read_ast.join_items[0].right_lateral_alias_tokens =
        stale_non_lateral_join_payload_read_ast.join_items[0].right_alias_tokens;
    stale_non_lateral_join_payload_read_ast.join_items[0].right_lateral_alias_name_tokens =
        stale_non_lateral_join_payload_read_ast.join_items[0].right_alias_name_tokens;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(stale_non_lateral_join_payload_parsed_sql.items(), stale_non_lateral_join_payload_read_ast),
    );

    var standalone_lateral_source_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM LATERAL (SELECT id FROM usage_records) AS source_rows",
    );
    defer standalone_lateral_source_parsed_sql.deinit(alloc);
    const standalone_lateral_source_generated_raw = standalone_lateral_source_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    const standalone_lateral_source_read_ast = switch (standalone_lateral_source_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(standalone_lateral_source_parsed_sql.items(), standalone_lateral_source_read_ast),
    );

    var malformed_join_tail_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT usage_records.id FROM usage_records JOIN accounts ON usage_records.id = accounts.id ORDER BY usage_records.id",
    );
    defer malformed_join_tail_parsed_sql.deinit(alloc);
    const malformed_join_tail_generated_raw = malformed_join_tail_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_join_tail_read_ast = switch (malformed_join_tail_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    const malformed_join_tail_end = (malformed_join_tail_read_ast.order_tokens orelse return error.UnsupportedSqlShape).end;
    malformed_join_tail_read_ast.source_tokens = .{
        .start = (malformed_join_tail_read_ast.source_tokens orelse return error.UnsupportedSqlShape).start,
        .end = malformed_join_tail_end,
    };
    malformed_join_tail_read_ast.join_tokens = .{
        .start = (malformed_join_tail_read_ast.join_tokens orelse return error.UnsupportedSqlShape).start,
        .end = malformed_join_tail_end,
    };
    malformed_join_tail_read_ast.join_items[0].tokens.end = malformed_join_tail_end;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedJoinTreeMetadata(malformed_join_tail_parsed_sql.items(), malformed_join_tail_read_ast),
    );

    var malformed_join_condition_keyword_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT usage_records.id FROM usage_records JOIN accounts ON usage_records.id = accounts.id",
    );
    defer malformed_join_condition_keyword_parsed_sql.deinit(alloc);
    const malformed_join_condition_keyword_generated_raw = malformed_join_condition_keyword_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_join_condition_keyword_read_ast = switch (malformed_join_condition_keyword_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    const malformed_join_predicate_tokens = malformed_join_condition_keyword_read_ast.join_items[0].predicate_tokens orelse return error.UnsupportedSqlShape;
    malformed_join_condition_keyword_read_ast.join_items[0].condition_tokens = malformed_join_predicate_tokens;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_join_condition_keyword_parsed_sql, malformed_join_condition_keyword_read_ast),
    );

    malformed_join_condition_keyword_read_ast = switch (malformed_join_condition_keyword_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_join_condition_keyword_read_ast.join_items[0].predicate_expression.tokens =
        malformed_join_condition_keyword_read_ast.projection_items.expression_items[0];
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_join_condition_keyword_parsed_sql, malformed_join_condition_keyword_read_ast),
    );

    var malformed_using_join_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT usage_records.id FROM usage_records JOIN accounts USING (id)",
    );
    defer malformed_using_join_parsed_sql.deinit(alloc);
    const malformed_using_join_generated_raw = malformed_using_join_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_using_join_read_ast = switch (malformed_using_join_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_using_join_read_ast.join_items[0].using_column_tokens = null;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_using_join_parsed_sql, malformed_using_join_read_ast),
    );

    malformed_using_join_read_ast = switch (malformed_using_join_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    const malformed_using_condition_tokens = malformed_using_join_read_ast.join_items[0].using_tokens orelse return error.UnsupportedSqlShape;
    malformed_using_join_read_ast.join_items[0].using_column_tokens = .{
        .start = malformed_using_condition_tokens.start + 1,
        .end = malformed_using_condition_tokens.end - 1,
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_using_join_parsed_sql, malformed_using_join_read_ast),
    );

    malformed_using_join_read_ast = switch (malformed_using_join_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_using_join_read_ast.join_items[0].using_columns.first_tokens =
        malformed_using_join_read_ast.projection_items.items[0];
    malformed_using_join_read_ast.join_items[0].using_columns.last_tokens =
        malformed_using_join_read_ast.projection_items.items[0];
    malformed_using_join_read_ast.join_items[0].using_columns.items[0] =
        malformed_using_join_read_ast.projection_items.items[0];
    malformed_using_join_read_ast.join_items[0].using_columns.expression_items[0] =
        malformed_using_join_read_ast.projection_items.expression_items[0];
    malformed_using_join_read_ast.join_items[0].using_columns.expressions[0].tokens =
        malformed_using_join_read_ast.projection_items.expression_items[0];
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_using_join_parsed_sql, malformed_using_join_read_ast),
    );

    var stale_using_list_leading_gap_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT usage_records.id FROM usage_records JOIN accounts USING (id, status)",
    );
    defer stale_using_list_leading_gap_parsed_sql.deinit(alloc);
    var stale_using_list_leading_gap_read_ast = switch ((stale_using_list_leading_gap_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape).ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    const using_columns = &stale_using_list_leading_gap_read_ast.join_items[0].using_columns;
    if (using_columns.count < 2 or using_columns.items.len < 2 or using_columns.expression_items.len < 2 or using_columns.expressions.len < 2) {
        return error.TestUnexpectedResult;
    }
    using_columns.count = 1;
    using_columns.first_tokens = using_columns.items[1];
    using_columns.last_tokens = using_columns.items[1];
    using_columns.items = using_columns.items[1..2];
    using_columns.expression_items = using_columns.expression_items[1..2];
    using_columns.expressions = using_columns.expressions[1..2];
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(stale_using_list_leading_gap_parsed_sql.items(), stale_using_list_leading_gap_read_ast),
    );

    var stale_using_list_trailing_gap_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT usage_records.id FROM usage_records JOIN accounts USING (id, status)",
    );
    defer stale_using_list_trailing_gap_parsed_sql.deinit(alloc);
    var stale_using_list_trailing_gap_read_ast = switch ((stale_using_list_trailing_gap_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape).ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    const trailing_gap_using_columns = &stale_using_list_trailing_gap_read_ast.join_items[0].using_columns;
    if (trailing_gap_using_columns.count < 2 or
        trailing_gap_using_columns.items.len < 2 or
        trailing_gap_using_columns.expression_items.len < 2 or
        trailing_gap_using_columns.expressions.len < 2)
    {
        return error.TestUnexpectedResult;
    }
    trailing_gap_using_columns.count = 1;
    trailing_gap_using_columns.last_tokens = trailing_gap_using_columns.items[0];
    trailing_gap_using_columns.items = trailing_gap_using_columns.items[0..1];
    trailing_gap_using_columns.expression_items = trailing_gap_using_columns.expression_items[0..1];
    trailing_gap_using_columns.expressions = trailing_gap_using_columns.expressions[0..1];
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(stale_using_list_trailing_gap_parsed_sql.items(), stale_using_list_trailing_gap_read_ast),
    );

    var malformed_set_operation_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records UNION ALL SELECT id FROM usage_archive",
    );
    defer malformed_set_operation_parsed_sql.deinit(alloc);
    const malformed_set_operation_generated_raw = malformed_set_operation_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_set_operation_read_ast = switch (malformed_set_operation_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_set_operation_read_ast.set_operation.all_tokens = null;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_set_operation_parsed_sql, malformed_set_operation_read_ast),
    );

    var malformed_set_operation_distinct_keyword_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records UNION SELECT DISTINCT id FROM usage_archive",
    );
    defer malformed_set_operation_distinct_keyword_parsed_sql.deinit(alloc);
    const malformed_set_operation_distinct_keyword_generated_raw = malformed_set_operation_distinct_keyword_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    const malformed_set_operation_distinct_keyword_read_ast = switch (malformed_set_operation_distinct_keyword_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    const malformed_set_operation_distinct_tokens =
        malformed_set_operation_distinct_keyword_read_ast.set_operation.right_distinct_tokens orelse return error.UnsupportedSqlShape;
    @constCast(malformed_set_operation_distinct_keyword_parsed_sql.items())[malformed_set_operation_distinct_tokens.start].keyword = .from;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_set_operation_distinct_keyword_parsed_sql, malformed_set_operation_distinct_keyword_read_ast),
    );

    malformed_set_operation_read_ast = switch (malformed_set_operation_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_set_operation_read_ast.set_operation.right_projection_items.first_tokens =
        malformed_set_operation_read_ast.projection_items.items[0];
    malformed_set_operation_read_ast.set_operation.right_projection_items.last_tokens =
        malformed_set_operation_read_ast.projection_items.items[0];
    malformed_set_operation_read_ast.set_operation.right_projection_items.items[0] =
        malformed_set_operation_read_ast.projection_items.items[0];
    malformed_set_operation_read_ast.set_operation.right_projection_items.expression_items[0] =
        malformed_set_operation_read_ast.projection_items.expression_items[0];
    malformed_set_operation_read_ast.set_operation.right_projection_items.expressions[0].tokens =
        malformed_set_operation_read_ast.projection_items.expression_items[0];
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_set_operation_parsed_sql, malformed_set_operation_read_ast),
    );

    malformed_set_operation_read_ast = switch (malformed_set_operation_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_set_operation_read_ast.set_operation.right_projection_first_expression.tokens =
        malformed_set_operation_read_ast.projection_items.expression_items[0];
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_set_operation_parsed_sql, malformed_set_operation_read_ast),
    );

    malformed_set_operation_read_ast = switch (malformed_set_operation_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_set_operation_read_ast.set_operation.right_source_tokens =
        malformed_set_operation_read_ast.set_operation.right_projection_tokens;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_set_operation_parsed_sql, malformed_set_operation_read_ast),
    );

    var malformed_set_operation_where_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records UNION SELECT id FROM usage_archive WHERE status = 'ready'",
    );
    defer malformed_set_operation_where_parsed_sql.deinit(alloc);
    const malformed_set_operation_where_generated_raw = malformed_set_operation_where_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_set_operation_where_read_ast = switch (malformed_set_operation_where_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_set_operation_where_read_ast.set_operation.right_where_tokens =
        malformed_set_operation_where_read_ast.set_operation.right_source_tokens;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_set_operation_where_parsed_sql, malformed_set_operation_where_read_ast),
    );

    malformed_set_operation_where_read_ast = switch (malformed_set_operation_where_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_set_operation_where_read_ast.set_operation.right_where_expression.tokens =
        malformed_set_operation_where_read_ast.set_operation.right_projection_items.expression_items[0];
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_set_operation_where_parsed_sql, malformed_set_operation_where_read_ast),
    );

    var malformed_set_operation_where_child_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records UNION SELECT id FROM usage_archive WHERE lower(status) = 'ready'",
    );
    defer malformed_set_operation_where_child_parsed_sql.deinit(alloc);
    const malformed_set_operation_where_child_generated_raw = malformed_set_operation_where_child_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_set_operation_where_child_read_ast = switch (malformed_set_operation_where_child_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_set_operation_where_child_read_ast.set_operation.right_where_expression.left_expression.?.argument_items.expressions[0].tokens =
        malformed_set_operation_where_child_read_ast.set_operation.right_projection_items.expression_items[0];
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(malformed_set_operation_where_child_parsed_sql.items(), malformed_set_operation_where_child_read_ast),
    );

    var malformed_window_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id, row_number() OVER usage_window AS rn FROM usage_records WINDOW usage_window AS (ORDER BY id)",
    );
    defer malformed_window_parsed_sql.deinit(alloc);
    const malformed_window_generated_raw = malformed_window_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_window_read_ast = switch (malformed_window_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_window_read_ast.window_count = 0;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_window_parsed_sql, malformed_window_read_ast),
    );

    var stale_named_window_leading_gap_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id, row_number() OVER usage_window AS rn FROM usage_records WINDOW first_window AS (ORDER BY tenant), usage_window AS (ORDER BY id)",
    );
    defer stale_named_window_leading_gap_parsed_sql.deinit(alloc);
    var stale_named_window_leading_gap_read_ast = switch ((stale_named_window_leading_gap_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape).ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    if (stale_named_window_leading_gap_read_ast.window_items.len < 2) return error.TestUnexpectedResult;
    stale_named_window_leading_gap_read_ast.window_items = stale_named_window_leading_gap_read_ast.window_items[1..2];
    stale_named_window_leading_gap_read_ast.window_count = 1;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(stale_named_window_leading_gap_parsed_sql.items(), stale_named_window_leading_gap_read_ast),
    );

    var stale_named_window_frame_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id, sum(amount) OVER usage_window AS running FROM usage_records WINDOW usage_window AS (ORDER BY amount ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)",
    );
    defer stale_named_window_frame_parsed_sql.deinit(alloc);
    const stale_named_window_frame_generated_raw = stale_named_window_frame_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var stale_named_window_frame_read_ast = switch (stale_named_window_frame_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    const stale_named_window_frame_tokens = stale_named_window_frame_read_ast.window_items[0].frame_tokens orelse return error.UnsupportedSqlShape;
    stale_named_window_frame_read_ast.window_items[0].frame_tokens = .{
        .start = stale_named_window_frame_tokens.start,
        .end = stale_named_window_frame_tokens.end - 1,
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(stale_named_window_frame_parsed_sql.items(), stale_named_window_frame_read_ast),
    );

    var malformed_inline_window_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id, row_number() OVER (PARTITION BY tenant ORDER BY id) AS rn FROM usage_records",
    );
    defer malformed_inline_window_parsed_sql.deinit(alloc);
    const malformed_inline_window_generated_raw = malformed_inline_window_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_inline_window_read_ast = switch (malformed_inline_window_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_inline_window_read_ast.projection_items.expressions[1].over_order_items.count = 0;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_inline_window_parsed_sql, malformed_inline_window_read_ast),
    );
    malformed_inline_window_read_ast = switch (malformed_inline_window_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    const malformed_inline_window_definition_tokens = malformed_inline_window_read_ast.projection_items.expressions[1].over_definition_tokens orelse return error.UnsupportedSqlShape;
    malformed_inline_window_read_ast.projection_items.expressions[1].over_definition_tokens = .{
        .start = malformed_inline_window_definition_tokens.start - 1,
        .end = malformed_inline_window_definition_tokens.end,
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(malformed_inline_window_parsed_sql.items(), malformed_inline_window_read_ast),
    );

    var malformed_inline_window_frame_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id, count(*) OVER (ORDER BY amount ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS current_and_next FROM usage_records",
    );
    defer malformed_inline_window_frame_parsed_sql.deinit(alloc);
    const malformed_inline_window_frame_generated_raw = malformed_inline_window_frame_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_inline_window_frame_read_ast = switch (malformed_inline_window_frame_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_inline_window_frame_read_ast.projection_items.expressions[1].over_frame_end_expression = null;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_inline_window_frame_parsed_sql, malformed_inline_window_frame_read_ast),
    );
    malformed_inline_window_frame_read_ast = switch (malformed_inline_window_frame_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    const stale_inline_window_frame_tokens = malformed_inline_window_frame_read_ast.projection_items.expressions[1].over_frame_tokens orelse return error.UnsupportedSqlShape;
    malformed_inline_window_frame_read_ast.projection_items.expressions[1].over_frame_tokens = .{
        .start = stale_inline_window_frame_tokens.start,
        .end = stale_inline_window_frame_tokens.end - 1,
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(malformed_inline_window_frame_parsed_sql.items(), malformed_inline_window_frame_read_ast),
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

    cte_read_ast = switch (cte_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    cte_read_ast.cte_count = 2;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &cte_parsed_sql, cte_read_ast),
    );

    cte_read_ast = switch (cte_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    cte_read_ast.cte_final_kind = null;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &cte_parsed_sql, cte_read_ast),
    );

    cte_read_ast = switch (cte_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    cte_read_ast.cte_final_kind = .aggregate;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &cte_parsed_sql, cte_read_ast),
    );

    cte_read_ast = switch (cte_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    cte_read_ast.cte_items[0].name_tokens = .{
        .start = cte_read_ast.cte_items[0].name_tokens.start,
        .end = cte_read_ast.cte_items[0].name_tokens.end + 1,
    };
    cte_read_ast.cte_name_tokens = cte_read_ast.cte_items[0].name_tokens;
    cte_read_ast.cte_last_name_tokens = cte_read_ast.cte_items[0].name_tokens;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(cte_parsed_sql.items(), cte_read_ast),
    );

    var cte_system_time_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "WITH source_rows AS (SELECT id FROM usage_records FOR system_time AS OF 42) SELECT id FROM source_rows",
    );
    defer cte_system_time_parsed_sql.deinit(alloc);
    const cte_system_time_generated_raw = cte_system_time_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var cte_system_time_read_ast = switch (cte_system_time_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast.*,
        else => return error.UnsupportedSqlShape,
    };
    try validateGeneratedReadAstForStatement(cte_system_time_parsed_sql.items(), &cte_system_time_read_ast);
    cte_system_time_read_ast.cte_items[0].body_source_system_time_sequence_tokens = .{
        .start = (cte_system_time_read_ast.cte_items[0].body_source_system_time_sequence_tokens orelse return error.TestUnexpectedResult).start - 1,
        .end = (cte_system_time_read_ast.cte_items[0].body_source_system_time_sequence_tokens orelse return error.TestUnexpectedResult).end,
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(cte_system_time_parsed_sql.items(), &cte_system_time_read_ast),
    );

    cte_system_time_read_ast = switch (cte_system_time_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast.*,
        else => return error.UnsupportedSqlShape,
    };
    cte_system_time_read_ast.cte_items[0].body_source_system_time_tokens = null;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(cte_system_time_parsed_sql.items(), &cte_system_time_read_ast),
    );

    var stale_source_table_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE id = 'u1'",
    );
    defer stale_source_table_parsed_sql.deinit(alloc);
    const stale_source_table_generated_raw = stale_source_table_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var stale_source_table_read_ast = switch (stale_source_table_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    try validateGeneratedReadAstForStatement(stale_source_table_parsed_sql.items(), stale_source_table_read_ast);
    stale_source_table_read_ast.source_table_tokens = null;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(stale_source_table_parsed_sql.items(), stale_source_table_read_ast),
    );

    var stale_row_lock_tail_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records FOR UPDATE SKIP LOCKED",
    );
    defer stale_row_lock_tail_parsed_sql.deinit(alloc);
    const stale_row_lock_tail_generated_raw = stale_row_lock_tail_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var stale_row_lock_tail_read_ast = switch (stale_row_lock_tail_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    try validateGeneratedReadAstForStatement(stale_row_lock_tail_parsed_sql.items(), stale_row_lock_tail_read_ast);
    var stale_row_lock_tokens = stale_row_lock_tail_read_ast.row_lock_tokens orelse return error.TestUnexpectedResult;
    stale_row_lock_tokens.end -= 1;
    stale_row_lock_tail_read_ast.row_lock_tokens = stale_row_lock_tokens;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(stale_row_lock_tail_parsed_sql.items(), stale_row_lock_tail_read_ast),
    );

    var stale_cte_body_source_table_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "WITH source_rows AS (SELECT id FROM usage_records WHERE id = 'u1') SELECT id FROM source_rows",
    );
    defer stale_cte_body_source_table_parsed_sql.deinit(alloc);
    const stale_cte_body_source_table_generated_raw = stale_cte_body_source_table_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var stale_cte_body_source_table_read_ast = switch (stale_cte_body_source_table_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    try validateGeneratedReadAstForStatement(stale_cte_body_source_table_parsed_sql.items(), stale_cte_body_source_table_read_ast);
    stale_cte_body_source_table_read_ast.cte_items[0].body_source_table_tokens = null;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(stale_cte_body_source_table_parsed_sql.items(), stale_cte_body_source_table_read_ast),
    );

    var stale_set_operation_source_table_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records UNION ALL SELECT id FROM usage_archive",
    );
    defer stale_set_operation_source_table_parsed_sql.deinit(alloc);
    const stale_set_operation_source_table_generated_raw = stale_set_operation_source_table_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var stale_set_operation_source_table_read_ast = switch (stale_set_operation_source_table_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    try validateGeneratedReadAstForStatement(stale_set_operation_source_table_parsed_sql.items(), stale_set_operation_source_table_read_ast);
    stale_set_operation_source_table_read_ast.set_operation.right_source_table_tokens = null;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(stale_set_operation_source_table_parsed_sql.items(), stale_set_operation_source_table_read_ast),
    );

    var stale_set_operation_right_join_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records UNION ALL SELECT child.id FROM usage_records AS child JOIN accounts AS a ON child.id = a.id",
    );
    defer stale_set_operation_right_join_parsed_sql.deinit(alloc);
    const stale_set_operation_right_join_generated_raw = stale_set_operation_right_join_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var stale_set_operation_right_join_read_ast = switch (stale_set_operation_right_join_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    try validateGeneratedReadAstForStatement(stale_set_operation_right_join_parsed_sql.items(), stale_set_operation_right_join_read_ast);
    try std.testing.expect(stale_set_operation_right_join_read_ast.set_operation.right_join_items.len != 0);
    stale_set_operation_right_join_read_ast.set_operation.right_join_tree_root_index = null;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(stale_set_operation_right_join_parsed_sql.items(), stale_set_operation_right_join_read_ast),
    );

    var stale_absent_where_expression_list_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records",
    );
    defer stale_absent_where_expression_list_parsed_sql.deinit(alloc);
    const stale_absent_where_expression_list_generated_raw = stale_absent_where_expression_list_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var stale_absent_where_expression_list_read_ast = switch (stale_absent_where_expression_list_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    try validateGeneratedReadAstForStatement(stale_absent_where_expression_list_parsed_sql.items(), stale_absent_where_expression_list_read_ast);
    try std.testing.expect(stale_absent_where_expression_list_read_ast.where_tokens == null);
    try std.testing.expect(stale_absent_where_expression_list_read_ast.where_expression.argument_items.count == 0);
    stale_absent_where_expression_list_read_ast.where_expression.argument_items.items =
        stale_absent_where_expression_list_read_ast.projection_items.items;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(stale_absent_where_expression_list_parsed_sql.items(), stale_absent_where_expression_list_read_ast),
    );

    var stale_absent_set_operation_list_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records",
    );
    defer stale_absent_set_operation_list_parsed_sql.deinit(alloc);
    const stale_absent_set_operation_list_generated_raw = stale_absent_set_operation_list_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var stale_absent_set_operation_list_read_ast = switch (stale_absent_set_operation_list_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    try validateGeneratedReadAstForStatement(stale_absent_set_operation_list_parsed_sql.items(), stale_absent_set_operation_list_read_ast);
    try std.testing.expect(stale_absent_set_operation_list_read_ast.set_operation.tokens == null);
    try std.testing.expect(stale_absent_set_operation_list_read_ast.set_operation.right_projection_items.count == 0);
    stale_absent_set_operation_list_read_ast.set_operation.right_projection_items.items =
        stale_absent_set_operation_list_read_ast.projection_items.items;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(stale_absent_set_operation_list_parsed_sql.items(), stale_absent_set_operation_list_read_ast),
    );

    var stale_absent_order_list_direction_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records ORDER BY id DESC NULLS LAST",
    );
    defer stale_absent_order_list_direction_parsed_sql.deinit(alloc);
    const stale_absent_order_list_direction_generated_raw = stale_absent_order_list_direction_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var stale_absent_order_list_direction_read_ast = switch (stale_absent_order_list_direction_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    try validateGeneratedReadAstForStatement(stale_absent_order_list_direction_parsed_sql.items(), stale_absent_order_list_direction_read_ast);
    try std.testing.expect(stale_absent_order_list_direction_read_ast.order_tokens != null);
    try std.testing.expect(stale_absent_order_list_direction_read_ast.order_items.count != 0);
    try std.testing.expect(stale_absent_order_list_direction_read_ast.order_items.direction_items.len != 0);
    try std.testing.expect(stale_absent_order_list_direction_read_ast.order_items.nulls_order_items.len != 0);
    stale_absent_order_list_direction_read_ast.order_tokens = null;
    stale_absent_order_list_direction_read_ast.order_items.count = 0;
    stale_absent_order_list_direction_read_ast.order_items.first_tokens = null;
    stale_absent_order_list_direction_read_ast.order_items.last_tokens = null;
    stale_absent_order_list_direction_read_ast.order_items.items = &.{};
    stale_absent_order_list_direction_read_ast.order_items.expression_items = &.{};
    stale_absent_order_list_direction_read_ast.order_items.expressions = &.{};
    stale_absent_order_list_direction_read_ast.order_first_expression = .{};
    stale_absent_order_list_direction_read_ast.order_last_expression = .{};
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(stale_absent_order_list_direction_parsed_sql.items(), stale_absent_order_list_direction_read_ast),
    );

    var stale_empty_over_order_list_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT count(*) OVER () AS total_rows FROM usage_records",
    );
    defer stale_empty_over_order_list_parsed_sql.deinit(alloc);
    const stale_empty_over_order_list_generated_raw = stale_empty_over_order_list_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var stale_empty_over_order_list_read_ast = switch (stale_empty_over_order_list_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    try validateGeneratedReadAstForStatement(stale_empty_over_order_list_parsed_sql.items(), stale_empty_over_order_list_read_ast);
    try std.testing.expect(stale_empty_over_order_list_read_ast.projection_items.expressions.len != 0);
    try std.testing.expect(stale_empty_over_order_list_read_ast.projection_items.alias_items.len != 0);
    try std.testing.expect(stale_empty_over_order_list_read_ast.projection_items.expressions[0].over_tokens != null);
    try std.testing.expect(stale_empty_over_order_list_read_ast.projection_items.expressions[0].over_order_tokens == null);
    try std.testing.expect(stale_empty_over_order_list_read_ast.projection_items.expressions[0].over_order_items.count == 0);
    try std.testing.expect(stale_empty_over_order_list_read_ast.projection_items.expressions[0].over_order_items.items.len == 0);
    stale_empty_over_order_list_read_ast.projection_items.expressions[0].over_order_items.alias_items =
        stale_empty_over_order_list_read_ast.projection_items.alias_items;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedFunctionOverMetadata(stale_empty_over_order_list_read_ast.projection_items.expressions[0]),
    );

    var stale_empty_function_argument_list_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT row_number() OVER () AS rn FROM usage_records",
    );
    defer stale_empty_function_argument_list_parsed_sql.deinit(alloc);
    const stale_empty_function_argument_list_generated_raw = stale_empty_function_argument_list_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var stale_empty_function_argument_list_read_ast = switch (stale_empty_function_argument_list_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    try validateGeneratedReadAstForStatement(stale_empty_function_argument_list_parsed_sql.items(), stale_empty_function_argument_list_read_ast);
    try std.testing.expect(stale_empty_function_argument_list_read_ast.projection_items.expressions.len != 0);
    try std.testing.expect(stale_empty_function_argument_list_read_ast.projection_items.alias_items.len != 0);
    try std.testing.expect(stale_empty_function_argument_list_read_ast.projection_items.expressions[0].argument_tokens == null);
    try std.testing.expect(stale_empty_function_argument_list_read_ast.projection_items.expressions[0].argument_items.count == 0);
    try std.testing.expect(stale_empty_function_argument_list_read_ast.projection_items.expressions[0].argument_items.items.len == 0);
    stale_empty_function_argument_list_read_ast.projection_items.expressions[0].argument_items.alias_items =
        stale_empty_function_argument_list_read_ast.projection_items.alias_items;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedFunctionCallClauseMetadata(
            stale_empty_function_argument_list_parsed_sql.items(),
            stale_empty_function_argument_list_read_ast.projection_items.expressions[0],
        ),
    );

    var stale_empty_array_list_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id AS usage_id FROM usage_records WHERE metadata ?| ARRAY[]",
    );
    defer stale_empty_array_list_parsed_sql.deinit(alloc);
    const stale_empty_array_list_generated_raw = stale_empty_array_list_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    const stale_empty_array_list_read_ast = switch (stale_empty_array_list_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    try validateGeneratedReadAstForStatement(stale_empty_array_list_parsed_sql.items(), stale_empty_array_list_read_ast);
    const stale_empty_array_expression = stale_empty_array_list_read_ast.where_expression.right_expression orelse return error.UnsupportedSqlShape;
    try std.testing.expect(stale_empty_array_expression.array_tokens == null);
    try std.testing.expect(stale_empty_array_expression.array_items.count == 0);
    try std.testing.expect(stale_empty_array_expression.array_items.items.len == 0);
    try std.testing.expect(stale_empty_array_list_read_ast.projection_items.alias_items.len != 0);
    stale_empty_array_expression.array_items.alias_items = stale_empty_array_list_read_ast.projection_items.alias_items;
    defer stale_empty_array_expression.array_items.alias_items = &.{};
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedArrayExpressionAstStructure(stale_empty_array_expression.*),
    );
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedArrayExpressionClauseMetadata(
            stale_empty_array_list_parsed_sql.items(),
            stale_empty_array_expression.*,
        ),
    );
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedExpressionAstRanges(
            stale_empty_array_list_parsed_sql.items(),
            stale_empty_array_list_read_ast,
            stale_empty_array_expression.*,
        ),
    );

    var stale_plain_distinct_list_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT DISTINCT id AS usage_id FROM usage_records",
    );
    defer stale_plain_distinct_list_parsed_sql.deinit(alloc);
    const stale_plain_distinct_list_generated_raw = stale_plain_distinct_list_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var stale_plain_distinct_list_read_ast = switch (stale_plain_distinct_list_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    try validateGeneratedReadAstForStatement(stale_plain_distinct_list_parsed_sql.items(), stale_plain_distinct_list_read_ast);
    try std.testing.expect(stale_plain_distinct_list_read_ast.distinct_tokens != null);
    try std.testing.expect(stale_plain_distinct_list_read_ast.distinct_on_items.count == 0);
    try std.testing.expect(stale_plain_distinct_list_read_ast.distinct_on_items.items.len == 0);
    try std.testing.expect(stale_plain_distinct_list_read_ast.projection_items.alias_items.len != 0);
    stale_plain_distinct_list_read_ast.distinct_on_items.alias_items = stale_plain_distinct_list_read_ast.projection_items.alias_items;
    defer stale_plain_distinct_list_read_ast.distinct_on_items.alias_items = &.{};
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedDistinctOnListMetadata(
            stale_plain_distinct_list_read_ast.distinct_tokens,
            stale_plain_distinct_list_read_ast.distinct_on_items,
        ),
    );
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedDistinctOnListAstRanges(
            stale_plain_distinct_list_parsed_sql.items(),
            stale_plain_distinct_list_read_ast,
            stale_plain_distinct_list_read_ast.distinct_tokens,
            stale_plain_distinct_list_read_ast.distinct_on_items,
        ),
    );
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadListAstContainedByRange(
            stale_plain_distinct_list_read_ast.distinct_on_items,
            stale_plain_distinct_list_read_ast.projection_tokens orelse return error.UnsupportedSqlShape,
        ),
    );
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadListAstContainedByOptionalRange(stale_plain_distinct_list_read_ast.distinct_on_items, null),
    );
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadListAstBoundaryExpressions(
            stale_plain_distinct_list_parsed_sql.items(),
            stale_plain_distinct_list_read_ast,
            stale_plain_distinct_list_read_ast.distinct_on_items,
            null,
            null,
        ),
    );

    var stale_projection_list_slice_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id AS usage_id FROM usage_records",
    );
    defer stale_projection_list_slice_parsed_sql.deinit(alloc);
    const stale_projection_list_slice_generated_raw = stale_projection_list_slice_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var stale_projection_list_slice_read_ast = switch (stale_projection_list_slice_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    try validateGeneratedReadAstForStatement(stale_projection_list_slice_parsed_sql.items(), stale_projection_list_slice_read_ast);
    try std.testing.expectEqual(@as(usize, 1), stale_projection_list_slice_read_ast.projection_items.count);
    try std.testing.expectEqual(stale_projection_list_slice_read_ast.projection_items.count, stale_projection_list_slice_read_ast.projection_items.items.len);
    try std.testing.expectEqual(stale_projection_list_slice_read_ast.projection_items.count, stale_projection_list_slice_read_ast.projection_items.expression_items.len);
    stale_projection_list_slice_read_ast.projection_items.items = &.{};
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadListAstContainedByRange(
            stale_projection_list_slice_read_ast.projection_items,
            stale_projection_list_slice_read_ast.projection_tokens orelse return error.UnsupportedSqlShape,
        ),
    );
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadListAstBoundaryExpressions(
            stale_projection_list_slice_parsed_sql.items(),
            stale_projection_list_slice_read_ast,
            stale_projection_list_slice_read_ast.projection_items,
            &stale_projection_list_slice_read_ast.projection_first_expression,
            &stale_projection_list_slice_read_ast.projection_last_expression,
        ),
    );

    var multi_cte_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "WITH first_rows AS (SELECT id FROM usage_records), second_rows AS (SELECT id FROM first_rows) SELECT id FROM second_rows",
    );
    defer multi_cte_parsed_sql.deinit(alloc);
    const multi_cte_generated_raw = multi_cte_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var multi_cte_read_ast = switch (multi_cte_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    multi_cte_read_ast.cte_last_body_tokens = multi_cte_read_ast.cte_items[0].body_tokens;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &multi_cte_parsed_sql, multi_cte_read_ast),
    );
    multi_cte_read_ast = switch (multi_cte_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    multi_cte_read_ast.cte_items[1].name_tokens = .{
        .start = multi_cte_read_ast.cte_items[1].name_tokens.start - 1,
        .end = multi_cte_read_ast.cte_items[1].name_tokens.end,
    };
    multi_cte_read_ast.cte_last_name_tokens = multi_cte_read_ast.cte_items[1].name_tokens;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(multi_cte_parsed_sql.items(), multi_cte_read_ast),
    );

    var materialized_cte_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "WITH source_rows(id) AS NOT MATERIALIZED (SELECT id FROM usage_records) SELECT id FROM source_rows",
    );
    defer materialized_cte_parsed_sql.deinit(alloc);
    const materialized_cte_generated_raw = materialized_cte_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var materialized_cte_read_ast = switch (materialized_cte_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    materialized_cte_read_ast.cte_items[0].materialization = null;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &materialized_cte_parsed_sql, materialized_cte_read_ast),
    );
    materialized_cte_read_ast = switch (materialized_cte_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    const malformed_materialization_tokens = materialized_cte_read_ast.cte_items[0].materialization_tokens orelse return error.UnsupportedSqlShape;
    materialized_cte_read_ast.cte_items[0].materialization_tokens = .{
        .start = malformed_materialization_tokens.start - 1,
        .end = malformed_materialization_tokens.end,
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(materialized_cte_parsed_sql.items(), materialized_cte_read_ast),
    );

    var malformed_cte_body_kind_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "WITH source_rows AS (SELECT id FROM usage_records) SELECT id FROM source_rows",
    );
    defer malformed_cte_body_kind_parsed_sql.deinit(alloc);
    const malformed_cte_body_kind_generated_raw = malformed_cte_body_kind_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_cte_body_kind_read_ast = switch (malformed_cte_body_kind_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_cte_body_kind_read_ast.cte_items[0].body_kind = null;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_cte_body_kind_parsed_sql, malformed_cte_body_kind_read_ast),
    );

    var malformed_cte_body_select_keyword_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "WITH source_rows AS (SELECT id FROM usage_records) SELECT id FROM source_rows",
    );
    defer malformed_cte_body_select_keyword_parsed_sql.deinit(alloc);
    const malformed_cte_body_select_keyword_generated_raw = malformed_cte_body_select_keyword_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    const malformed_cte_body_select_keyword_read_ast = switch (malformed_cte_body_select_keyword_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    const malformed_cte_body_select_tokens =
        malformed_cte_body_select_keyword_read_ast.cte_items[0].body_select_tokens orelse return error.UnsupportedSqlShape;
    @constCast(malformed_cte_body_select_keyword_parsed_sql.items())[malformed_cte_body_select_tokens.start].keyword = .from;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_cte_body_select_keyword_parsed_sql, malformed_cte_body_select_keyword_read_ast),
    );

    var malformed_cte_body_projection_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "WITH source_rows AS (SELECT id FROM usage_records) SELECT id FROM source_rows",
    );
    defer malformed_cte_body_projection_parsed_sql.deinit(alloc);
    const malformed_cte_body_projection_generated_raw = malformed_cte_body_projection_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_cte_body_projection_read_ast = switch (malformed_cte_body_projection_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_cte_body_projection_read_ast.cte_items[0].body_projection_tokens = malformed_cte_body_projection_read_ast.projection_tokens;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_cte_body_projection_parsed_sql, malformed_cte_body_projection_read_ast),
    );

    var malformed_cte_body_source_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "WITH source_rows AS (SELECT id FROM usage_records WHERE active IS TRUE) SELECT id FROM source_rows",
    );
    defer malformed_cte_body_source_parsed_sql.deinit(alloc);
    const malformed_cte_body_source_generated_raw = malformed_cte_body_source_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_cte_body_source_read_ast = switch (malformed_cte_body_source_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_cte_body_source_read_ast.cte_items[0].body_source_tokens =
        malformed_cte_body_source_read_ast.cte_items[0].body_where_tokens;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_cte_body_source_parsed_sql, malformed_cte_body_source_read_ast),
    );

    malformed_cte_body_source_read_ast = switch (malformed_cte_body_source_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_cte_body_source_read_ast.cte_items[0].body_where_expression.tokens =
        malformed_cte_body_source_read_ast.cte_items[0].body_projection_items.expression_items[0];
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_cte_body_source_parsed_sql, malformed_cte_body_source_read_ast),
    );

    malformed_cte_body_source_read_ast = switch (malformed_cte_body_source_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_cte_body_source_read_ast.cte_items[0].body_where_expression.argument_items.count = 1;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedCteBodyMetadata(
            malformed_cte_body_source_parsed_sql.items(),
            malformed_cte_body_source_read_ast.cte_items[0],
        ),
    );

    var malformed_cte_body_projection_list_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "WITH source_rows AS (SELECT id FROM usage_records) SELECT id FROM source_rows",
    );
    defer malformed_cte_body_projection_list_parsed_sql.deinit(alloc);
    const malformed_cte_body_projection_list_generated_raw = malformed_cte_body_projection_list_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_cte_body_projection_list_read_ast = switch (malformed_cte_body_projection_list_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_cte_body_projection_list_read_ast.cte_items[0].body_projection_items.count = 0;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_cte_body_projection_list_parsed_sql, malformed_cte_body_projection_list_read_ast),
    );

    malformed_cte_body_projection_list_read_ast = switch (malformed_cte_body_projection_list_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_cte_body_projection_list_read_ast.cte_items[0].body_projection_items.first_tokens =
        malformed_cte_body_projection_list_read_ast.projection_items.items[0];
    malformed_cte_body_projection_list_read_ast.cte_items[0].body_projection_items.last_tokens =
        malformed_cte_body_projection_list_read_ast.projection_items.items[0];
    malformed_cte_body_projection_list_read_ast.cte_items[0].body_projection_items.items[0] =
        malformed_cte_body_projection_list_read_ast.projection_items.items[0];
    malformed_cte_body_projection_list_read_ast.cte_items[0].body_projection_items.expression_items[0] =
        malformed_cte_body_projection_list_read_ast.projection_items.expression_items[0];
    malformed_cte_body_projection_list_read_ast.cte_items[0].body_projection_items.expressions[0].tokens =
        malformed_cte_body_projection_list_read_ast.projection_items.expression_items[0];
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_cte_body_projection_list_parsed_sql, malformed_cte_body_projection_list_read_ast),
    );

    malformed_cte_body_projection_list_read_ast = switch (malformed_cte_body_projection_list_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_cte_body_projection_list_read_ast.cte_items[0].body_projection_first_expression.tokens =
        malformed_cte_body_projection_list_read_ast.projection_items.expression_items[0];
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_cte_body_projection_list_parsed_sql, malformed_cte_body_projection_list_read_ast),
    );

    var malformed_cte_body_distinct_keyword_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "WITH source_rows AS (SELECT DISTINCT status FROM usage_records) SELECT status FROM source_rows",
    );
    defer malformed_cte_body_distinct_keyword_parsed_sql.deinit(alloc);
    const malformed_cte_body_distinct_keyword_generated_raw = malformed_cte_body_distinct_keyword_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    const malformed_cte_body_distinct_keyword_read_ast = switch (malformed_cte_body_distinct_keyword_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    const malformed_cte_body_distinct_tokens =
        malformed_cte_body_distinct_keyword_read_ast.cte_items[0].body_distinct_tokens orelse return error.UnsupportedSqlShape;
    @constCast(malformed_cte_body_distinct_keyword_parsed_sql.items())[malformed_cte_body_distinct_tokens.start].keyword = .from;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_cte_body_distinct_keyword_parsed_sql, malformed_cte_body_distinct_keyword_read_ast),
    );

    var malformed_cte_body_distinct_on_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "WITH source_rows AS (SELECT DISTINCT ON (organization_id) organization_id FROM usage_records ORDER BY organization_id) SELECT organization_id FROM source_rows",
    );
    defer malformed_cte_body_distinct_on_parsed_sql.deinit(alloc);
    const malformed_cte_body_distinct_on_generated_raw = malformed_cte_body_distinct_on_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_cte_body_distinct_on_read_ast = switch (malformed_cte_body_distinct_on_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_cte_body_distinct_on_read_ast.cte_items[0].body_distinct_on_items.count = 0;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_cte_body_distinct_on_parsed_sql, malformed_cte_body_distinct_on_read_ast),
    );

    var malformed_cte_body_join_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "WITH joined_rows AS (SELECT usage_records.id FROM usage_records JOIN accounts ON usage_records.account_id = accounts.id JOIN tenants ON accounts.tenant_id = tenants.id) SELECT id FROM joined_rows",
    );
    defer malformed_cte_body_join_parsed_sql.deinit(alloc);
    const malformed_cte_body_join_generated_raw = malformed_cte_body_join_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_cte_body_join_read_ast = switch (malformed_cte_body_join_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_cte_body_join_read_ast.cte_items[0].body_join_items[1].left_child_index = null;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_cte_body_join_parsed_sql, malformed_cte_body_join_read_ast),
    );

    malformed_cte_body_join_read_ast = switch (malformed_cte_body_join_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    const malformed_cte_body_join_predicate = malformed_cte_body_join_read_ast.cte_items[0].body_join_items[0].predicate_tokens orelse return error.UnsupportedSqlShape;
    malformed_cte_body_join_read_ast.cte_items[0].body_join_items[0].condition_tokens = malformed_cte_body_join_predicate;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_cte_body_join_parsed_sql, malformed_cte_body_join_read_ast),
    );

    malformed_cte_body_join_read_ast = switch (malformed_cte_body_join_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_cte_body_join_read_ast.cte_items[0].body_join_items[0].predicate_expression.tokens =
        malformed_cte_body_join_read_ast.cte_items[0].body_projection_items.expression_items[0];
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_cte_body_join_parsed_sql, malformed_cte_body_join_read_ast),
    );

    var malformed_cte_body_set_operation_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "WITH source_rows AS (SELECT id FROM usage_records UNION SELECT id FROM usage_archive) SELECT id FROM source_rows",
    );
    defer malformed_cte_body_set_operation_parsed_sql.deinit(alloc);
    const malformed_cte_body_set_operation_generated_raw = malformed_cte_body_set_operation_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_cte_body_set_operation_read_ast = switch (malformed_cte_body_set_operation_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_cte_body_set_operation_read_ast.cte_items[0].body_set_operation.right_projection_items.count = 0;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_cte_body_set_operation_parsed_sql, malformed_cte_body_set_operation_read_ast),
    );

    var malformed_cte_body_having_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "WITH source_rows AS (SELECT status, COUNT(*) FROM usage_records GROUP BY status HAVING COUNT(*) > 1) SELECT status FROM source_rows",
    );
    defer malformed_cte_body_having_parsed_sql.deinit(alloc);
    const malformed_cte_body_having_generated_raw = malformed_cte_body_having_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_cte_body_having_read_ast = switch (malformed_cte_body_having_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_cte_body_having_read_ast.cte_items[0].body_having_expression.tokens =
        malformed_cte_body_having_read_ast.cte_items[0].body_group_items.expression_items[0];
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_cte_body_having_parsed_sql, malformed_cte_body_having_read_ast),
    );

    var malformed_cte_body_window_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "WITH source_rows AS (SELECT id, row_number() OVER usage_window AS rn FROM usage_records WINDOW usage_window AS (ORDER BY id)) SELECT rn FROM source_rows",
    );
    defer malformed_cte_body_window_parsed_sql.deinit(alloc);
    const malformed_cte_body_window_generated_raw = malformed_cte_body_window_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_cte_body_window_read_ast = switch (malformed_cte_body_window_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_cte_body_window_read_ast.cte_items[0].body_window_items[0].order_items.count = 0;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_cte_body_window_parsed_sql, malformed_cte_body_window_read_ast),
    );

    var malformed_cte_body_order_keyword_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "WITH source_rows AS (SELECT id FROM usage_records ORDER BY id) SELECT id FROM source_rows",
    );
    defer malformed_cte_body_order_keyword_parsed_sql.deinit(alloc);
    const malformed_cte_body_order_keyword_generated_raw = malformed_cte_body_order_keyword_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_cte_body_order_keyword_read_ast = switch (malformed_cte_body_order_keyword_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_cte_body_order_keyword_read_ast.cte_items[0].body_order_tokens =
        malformed_cte_body_order_keyword_read_ast.cte_items[0].body_source_tokens;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_cte_body_order_keyword_parsed_sql, malformed_cte_body_order_keyword_read_ast),
    );

    var malformed_cte_body_limit_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "WITH source_rows AS (SELECT id FROM usage_records LIMIT 5) SELECT id FROM source_rows",
    );
    defer malformed_cte_body_limit_parsed_sql.deinit(alloc);
    const malformed_cte_body_limit_generated_raw = malformed_cte_body_limit_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_cte_body_limit_read_ast = switch (malformed_cte_body_limit_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_cte_body_limit_read_ast.cte_items[0].body_limit_expression = .{};
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_cte_body_limit_parsed_sql, malformed_cte_body_limit_read_ast),
    );

    malformed_cte_body_limit_read_ast = switch (malformed_cte_body_limit_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_cte_body_limit_read_ast.cte_items[0].body_limit_tokens =
        malformed_cte_body_limit_read_ast.cte_items[0].body_source_tokens;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_cte_body_limit_parsed_sql, malformed_cte_body_limit_read_ast),
    );

    var malformed_cte_body_row_lock_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "WITH source_rows AS (SELECT id FROM usage_records FOR UPDATE SKIP LOCKED) SELECT id FROM source_rows",
    );
    defer malformed_cte_body_row_lock_parsed_sql.deinit(alloc);
    const malformed_cte_body_row_lock_generated_raw = malformed_cte_body_row_lock_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var malformed_cte_body_row_lock_read_ast = switch (malformed_cte_body_row_lock_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    malformed_cte_body_row_lock_read_ast.cte_items[0].body_row_lock_tokens =
        malformed_cte_body_row_lock_read_ast.cte_items[0].body_source_tokens;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_cte_body_row_lock_parsed_sql, malformed_cte_body_row_lock_read_ast),
    );
    malformed_cte_body_row_lock_read_ast = switch (malformed_cte_body_row_lock_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    var stale_cte_body_row_lock_tokens = malformed_cte_body_row_lock_read_ast.cte_items[0].body_row_lock_tokens orelse return error.TestUnexpectedResult;
    stale_cte_body_row_lock_tokens.end -= 1;
    malformed_cte_body_row_lock_read_ast.cte_items[0].body_row_lock_tokens = stale_cte_body_row_lock_tokens;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerReadPlanFromGeneratedReadAstAlloc(&context, &malformed_cte_body_row_lock_parsed_sql, malformed_cte_body_row_lock_read_ast),
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

    var recursive_cte_alias_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "WITH RECURSIVE walk(id, depth) AS (SELECT id, depth FROM usage_records WHERE kind = 'order' UNION ALL SELECT child.id, walk.depth + 1 FROM usage_records AS child JOIN walk ON child.customer_id = walk.id) SELECT id FROM walk WHERE depth > 1 ORDER BY id",
    );
    defer recursive_cte_alias_parsed_sql.deinit(alloc);
    const recursive_cte_alias_generated_raw = recursive_cte_alias_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    const recursive_cte_alias_read_ast = switch (recursive_cte_alias_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    var recursive_cte_alias_lowered = try lowerReadPlanFromGeneratedReadAstAlloc(&context, &recursive_cte_alias_parsed_sql, recursive_cte_alias_read_ast);
    defer recursive_cte_alias_lowered.deinit(alloc);
    try std.testing.expectEqual(@as(std.meta.Tag(plan.LoweredReadPlan), .recursive_cte), std.meta.activeTag(recursive_cte_alias_lowered));
}

test "sql adapter lowering context rejects stale generated read clause order metadata" {
    const alloc = std.testing.allocator;

    var parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE id IN (SELECT id FROM usage_archive) ORDER BY id",
    );
    defer parsed_sql.deinit(alloc);
    const generated_raw = parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var read_ast = switch (generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast.*,
        else => return error.UnsupportedSqlShape,
    };
    try validateGeneratedReadAstForStatement(parsed_sql.items(), &read_ast);
    const subquery_source_tokens = read_ast.where_expression.right_expression.?.subquery_source_tokens orelse return error.TestUnexpectedResult;
    read_ast.source_tokens = subquery_source_tokens;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(parsed_sql.items(), &read_ast),
    );

    var missing_final_where_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE status = 'open'",
    );
    defer missing_final_where_parsed_sql.deinit(alloc);
    const missing_final_where_generated_raw = missing_final_where_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var missing_final_where_read_ast = switch (missing_final_where_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast.*,
        else => return error.UnsupportedSqlShape,
    };
    try validateGeneratedReadAstForStatement(missing_final_where_parsed_sql.items(), &missing_final_where_read_ast);
    _ = missing_final_where_read_ast.where_tokens orelse return error.TestUnexpectedResult;
    _ = missing_final_where_read_ast.where_expression.tokens orelse return error.TestUnexpectedResult;
    missing_final_where_read_ast.where_tokens = null;
    missing_final_where_read_ast.where_expression = .{};
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(missing_final_where_parsed_sql.items(), &missing_final_where_read_ast),
    );

    var cte_body_parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "WITH source_rows AS (SELECT id FROM usage_records WHERE id IN (SELECT id FROM usage_archive)) SELECT id FROM source_rows",
    );
    defer cte_body_parsed_sql.deinit(alloc);
    const cte_body_generated_raw = cte_body_parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
    var cte_body_read_ast = switch (cte_body_generated_raw.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast.*,
        else => return error.UnsupportedSqlShape,
    };
    try validateGeneratedReadAstForStatement(cte_body_parsed_sql.items(), &cte_body_read_ast);
    const cte_body_subquery_source_tokens = cte_body_read_ast.cte_items[0].body_where_expression.right_expression.?.subquery_source_tokens orelse return error.TestUnexpectedResult;
    cte_body_read_ast.cte_items[0].body_source_tokens = cte_body_subquery_source_tokens;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedReadAstForStatement(cte_body_parsed_sql.items(), &cte_body_read_ast),
    );
}

test "sql adapter lowering context classifies read sql into typed plan families" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"kind":{"type":"keyword"},"tenant":{"type":"keyword"},"id":{"type":"keyword"},"organization_id":{"type":"keyword"},"customer_id":{"type":"keyword"},"status":{"type":"keyword"},"amount":{"type":"numeric"},"created_at":{"type":"numeric"},"name":{"type":"keyword"}},"required":["kind","tenant","id"],"additionalProperties":false}}},"primary_key":{"columns":["kind","tenant","id"]}}
    ;
    const schema = try runtimeSchemaFromJsonForLoweringContextTestAlloc(alloc, schema_json);
    defer runtime_schema.freeSchema(alloc, schema);

    var cte_query_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "WITH source_rows AS (SELECT id FROM usage_records) SELECT id FROM source_rows",
    );
    defer cte_query_sql.deinit(alloc);
    const cte_query_generated = cte_query_sql.generated_statement orelse return error.UnsupportedSqlShape;
    const cte_query_read_ast = switch (cte_query_generated.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    try std.testing.expectEqual(sql_statement_kind.SqlReadStatementKind.query, try generatedReadStatementKind(cte_query_sql.items(), cte_query_read_ast));

    var cte_set_operation_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "WITH source_rows AS (SELECT id FROM usage_records) SELECT id FROM source_rows UNION SELECT id FROM usage_archive",
    );
    defer cte_set_operation_sql.deinit(alloc);
    const cte_set_operation_generated = cte_set_operation_sql.generated_statement orelse return error.UnsupportedSqlShape;
    const cte_set_operation_read_ast = switch (cte_set_operation_generated.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    try std.testing.expectEqual(sql_statement_kind.SqlReadStatementKind.set_operation, try generatedReadStatementKind(cte_set_operation_sql.items(), cte_set_operation_read_ast));

    var cte_count_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "WITH source_rows AS (SELECT id FROM usage_records) SELECT COUNT(*) FROM source_rows",
    );
    defer cte_count_sql.deinit(alloc);
    const cte_count_generated = cte_count_sql.generated_statement orelse return error.UnsupportedSqlShape;
    const cte_count_read_ast = switch (cte_count_generated.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    try std.testing.expectEqual(sql_statement_kind.SqlReadStatementKind.aggregate, try generatedReadStatementKind(cte_count_sql.items(), cte_count_read_ast));

    var cte_join_aggregate_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "WITH source_rows AS (SELECT id FROM usage_records) SELECT source_rows.id, COUNT(*) FROM source_rows JOIN usage_records ON source_rows.id = usage_records.id GROUP BY source_rows.id",
    );
    defer cte_join_aggregate_sql.deinit(alloc);
    const cte_join_aggregate_generated = cte_join_aggregate_sql.generated_statement orelse return error.UnsupportedSqlShape;
    const cte_join_aggregate_read_ast = switch (cte_join_aggregate_generated.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    try std.testing.expectEqual(sql_statement_kind.SqlReadStatementKind.aggregate, try generatedReadStatementKind(cte_join_aggregate_sql.items(), cte_join_aggregate_read_ast));

    var recursive_cte_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "WITH RECURSIVE source_rows AS (SELECT id FROM usage_records) SELECT id FROM source_rows",
    );
    defer recursive_cte_sql.deinit(alloc);
    const recursive_cte_generated = recursive_cte_sql.generated_statement orelse return error.UnsupportedSqlShape;
    const recursive_cte_read_ast = switch (recursive_cte_generated.ast orelse return error.UnsupportedSqlShape) {
        .read => |ast| ast,
        else => return error.UnsupportedSqlShape,
    };
    try std.testing.expectEqual(sql_statement_kind.SqlReadStatementKind.recursive_cte, try generatedReadStatementKind(recursive_cte_sql.items(), recursive_cte_read_ast));

    var malformed_cte_read_ast = cte_query_read_ast;
    malformed_cte_read_ast.source_tokens = null;
    try std.testing.expectError(error.UnsupportedSqlShape, generatedReadStatementKind(cte_query_sql.items(), malformed_cte_read_ast));

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

    var global_aggregate = try lowerReadPlanForLoweringContextTestAlloc(
        alloc,
        "SELECT COUNT(*) AS total FROM usage_records WHERE kind = 'order'",
        schema,
        &.{},
    );
    defer global_aggregate.deinit(alloc);
    switch (global_aggregate) {
        .aggregate => |lowered| {
            try std.testing.expectEqualStrings("usage_records", lowered.table_name);
            try std.testing.expectEqual(@as(usize, 0), lowered.plan.ctes.len);
            try std.testing.expectEqual(@as(usize, 1), lowered.plan.aggregate.aggregations.len);
            try std.testing.expectEqual(db_mod.types.RelationalRowsAggregateOp.count, lowered.plan.aggregate.aggregations[0].op);
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
        expr_row_parse.SqlFunctionBindings,
    ) anyerror!plan.LoweredWritePlan,
};

pub const CatalogWritePlanLoweringContext = struct {
    alloc: std.mem.Allocator,
    sql: []const u8 = "",
    schema: runtime_schema.TableSchema,
    params: []const value_mod.SqlValue,
    function_bindings: expr_row_parse.SqlFunctionBindings = .{},
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
        return try self.lowerParsedWithSession(parsed_sql, options, catalog, catalog_resources.SqlCatalogSession.default());
    }

    pub fn lowerParsedWithSession(
        self: *@This(),
        parsed_sql: *const tokenized.ParsedSql,
        options: plan.LowerWritePlanOptions,
        catalog: table_catalog.CatalogSource,
        session: catalog_resources.SqlCatalogSession,
    ) !plan.LoweredWritePlan {
        var bound = try binder.bindWritePlanCatalogStatementWithSessionAlloc(self.alloc, parsed_sql, options, catalog, session);
        defer bound.deinit(self.alloc);
        return try self.lowerBoundParsed(parsed_sql, &bound);
    }

    pub fn lowerBoundParsed(
        self: *@This(),
        parsed_sql: *const tokenized.ParsedSql,
        bound: *binder.BoundSqlStatement,
    ) !plan.LoweredWritePlan {
        const old_parsed_sql = self.parsed_sql;
        self.parsed_sql = parsed_sql;
        defer self.parsed_sql = old_parsed_sql;
        return try binder.lowerWritePlanWithBoundStatementAlloc(self.alloc, bound, self.hooks());
    }

    pub fn lowerLogicalParsed(
        self: *@This(),
        parsed_sql: *const tokenized.ParsedSql,
        logical: *binder.LogicalSqlPlan,
    ) !plan.LoweredWritePlan {
        const old_parsed_sql = self.parsed_sql;
        self.parsed_sql = parsed_sql;
        defer self.parsed_sql = old_parsed_sql;
        return try binder.lowerWriteCatalogLogicalPlan(logical, self.hooks());
    }

    fn hooks(self: *@This()) binder.WritePlanCatalogLoweringHooks {
        return .{
            .ptr = self,
            .lower_with_options = lowerWithOptions,
        };
    }

    fn lowerWithOptions(ptr: *anyopaque, resolved_options: plan.LowerWritePlanOptions) anyerror!plan.LoweredWritePlan {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.callbacks.lower_with_options(self.alloc, self.parsed_sql.?, self.schema, self.params, resolved_options, self.function_bindings);
    }
};

pub const WritePlanLoweringCallbacks = struct {
    lower_generated_dml: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        generated_parser.GeneratedSqlDmlAst,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        plan.LowerWritePlanOptions,
        expr_row_parse.SqlFunctionBindings,
    ) anyerror!plan.LoweredWritePlan,
};

pub const WritePlanLoweringContext = struct {
    alloc: std.mem.Allocator,
    sql: []const u8 = "",
    schema: runtime_schema.TableSchema,
    params: []const value_mod.SqlValue,
    function_bindings: expr_row_parse.SqlFunctionBindings = .{},
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
        if (try generatedDmlAstForParsedSql(parsed_sql)) |dml_ast| {
            std.log.debug("write lowering using generated dml kind={}", .{dml_ast.kind});
            return self.callbacks.lower_generated_dml(self.alloc, parsed_sql, dml_ast.*, self.schema, self.params, options, self.function_bindings) catch |err| {
                if (err == error.UnsupportedSqlShape or err == error.UnsupportedRowsQuery or err == error.UnsupportedRowsSelector or err == error.InvalidSqlCatalog) {
                    std.log.debug("write lowering generated dml unsupported kind={} err={}", .{ dml_ast.kind, err });
                } else {
                    std.log.err("write lowering generated dml failed kind={} err={}", .{ dml_ast.kind, err });
                }
                return err;
            };
        }
        return error.UnsupportedSqlShape;
    }
};

fn generatedDmlAstForParsedSql(parsed_sql: *const tokenized.ParsedSql) !?*const generated_parser.GeneratedSqlDmlAst {
    if (parsed_sql.generatedStatementKind() != .dml) return null;
    const published = parsed_sql.writeStatementIncludingGeneratedAst() orelse return error.UnsupportedSqlShape;
    switch (parsed_sql.statement) {
        .write => |statement| {
            if (statement.kind != published.kind or statement.recursive != published.recursive) return error.UnsupportedSqlShape;
        },
        else => return error.UnsupportedSqlShape,
    }
    if (parsed_sql.generated_statement) |*generated_statement| {
        if (generated_statement.ast) |*generated_ast| {
            return switch (generated_ast.*) {
                .dml => |*dml| dml,
                else => error.UnsupportedSqlShape,
            };
        }
    }
    return error.UnsupportedSqlShape;
}

test "write lowering generated dml accessor validates published statement family" {
    const alloc = std.testing.allocator;

    var parsed_sql = try tokenized.ParsedSql.initAlloc(alloc, "INSERT INTO usage_records (id) VALUES ('u1')");
    defer parsed_sql.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.dml, parsed_sql.generatedStatementKind().?);
    try std.testing.expect((try generatedDmlAstForParsedSql(&parsed_sql)) != null);

    parsed_sql.statement = .{ .unknown = parsed_sql.raw_statement };
    try std.testing.expectError(error.UnsupportedSqlShape, generatedDmlAstForParsedSql(&parsed_sql));
}

const GeneratedDmlUnsupportedProbe = struct {
    fn lowerGeneratedDml(
        alloc: std.mem.Allocator,
        parsed_sql: *const tokenized.ParsedSql,
        dml_ast: generated_parser.GeneratedSqlDmlAst,
        schema: runtime_schema.TableSchema,
        params: []const value_mod.SqlValue,
        options: plan.LowerWritePlanOptions,
        function_bindings: expr_row_parse.SqlFunctionBindings,
    ) anyerror!plan.LoweredWritePlan {
        _ = alloc;
        _ = parsed_sql;
        _ = dml_ast;
        _ = schema;
        _ = params;
        _ = options;
        _ = function_bindings;
        return error.UnsupportedSqlShape;
    }

    fn callbacks() WritePlanLoweringCallbacks {
        return .{
            .lower_generated_dml = lowerGeneratedDml,
        };
    }
};

test "write lowering generated dml unsupported fails through generated callback" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"kind":{"type":"keyword"},"tenant":{"type":"keyword"},"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["kind","tenant","id"],"additionalProperties":false}}},"primary_key":{"columns":["kind","tenant","id"]}}
    ;
    const schema = try runtimeSchemaFromJsonForLoweringContextTestAlloc(alloc, schema_json);
    defer runtime_schema.freeSchema(alloc, schema);

    var parsed_sql = try tokenized.ParsedSql.initAlloc(alloc, "UPDATE usage_records SET status = 'done' WHERE id = 'u1'");
    defer parsed_sql.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.dml, parsed_sql.generatedStatementKind().?);

    var context = WritePlanLoweringContext{
        .alloc = alloc,
        .schema = schema,
        .params = &.{},
        .callbacks = GeneratedDmlUnsupportedProbe.callbacks(),
    };
    try std.testing.expectError(error.UnsupportedSqlShape, context.lowerParsed(&parsed_sql, .{}));
}

pub const ExplainPlanLoweringCallbacks = struct {
    lower_read_with_catalog: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        table_catalog.CatalogSource,
        expr_row_parse.SqlFunctionBindings,
    ) anyerror!plan.LoweredReadPlan,
    lower_read_without_catalog: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        expr_row_parse.SqlFunctionBindings,
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
    function_bindings: expr_row_parse.SqlFunctionBindings,
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
        expr_row_parse.SqlFunctionBindings,
    ) anyerror!plan.LoweredReadPlan,
    lower_read_without_catalog: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        expr_row_parse.SqlFunctionBindings,
    ) anyerror!plan.LoweredReadPlan,
};

pub const RelationPopulationLoweringContext = struct {
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    params: []const value_mod.SqlValue,
    catalog: ?table_catalog.CatalogSource,
    function_bindings: expr_row_parse.SqlFunctionBindings,
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
