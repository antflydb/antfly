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

const binder = @import("../binder.zig");
const db_mod = @import("../../storage/db/mod.zig");
const ddl_plan = @import("../ddl_plan.zig");
const expr_condition = @import("condition.zig");
const expr_equal = @import("equal.zig");
const expr_generated = @import("generated.zig");
const expr_generated_validate = @import("generated_validate.zig");
const expr_operator = @import("operator.zig");
const expr_parse = @import("parse.zig");
const expr_predicate = @import("predicate.zig");
const expr_projection = @import("projection.zig");
const expr_row_parse = @import("row_parse.zig");
const expr_order = @import("order.zig");
const expr_token = @import("token.zig");
const expr_type = @import("type.zig");
const expr_where_condition = @import("where_condition.zig");
const generated_parser = @import("../generated_parser.zig");
const generated_read_validate = @import("../generated_read_validate.zig");
const grammar = @import("../grammar.zig");
const parser = @import("../parser.zig");
const plan_mod = @import("../plan.zig");
const runtime_schema = @import("../../storage/schema.zig");
const select_set = @import("../select_set.zig");
const strings = @import("../strings.zig");
const token_mod = @import("../token.zig");
const value_mod = @import("../value.zig");

pub const Token = token_mod.Token;

const freeArrayAny = plan_mod.freeArrayAny;
const freeArrayContains = plan_mod.freeArrayContains;
const freeArrayEq = plan_mod.freeArrayEq;
const freeExpressionArrayContains = plan_mod.freeExpressionArrayContains;
const freeExpressionArrayContainsOne = plan_mod.freeExpressionArrayContainsOne;
const freeExpression = plan_mod.freeExpression;
const freeExpressionCondition = plan_mod.freeExpressionCondition;
const freeExpressionConditions = plan_mod.freeExpressionConditions;
const freeExpressionPredicateGroups = plan_mod.freeExpressionPredicateGroups;
const freeInPredicates = plan_mod.freeInPredicates;
const freeJsonContains = plan_mod.freeJsonContains;
const freeJsonPathEq = plan_mod.freeJsonPathEq;
const freeJsonPathExists = plan_mod.freeJsonPathExists;
const freeOrderBy = plan_mod.freeOrderBy;
const freePredicateGroups = plan_mod.freePredicateGroups;
const freeRelationalChecks = plan_mod.freeRelationalChecks;
const freeTextPatterns = plan_mod.freeTextPatterns;

pub const ProjectedColumnType = struct {
    field_type: runtime_schema.AntflyType,
    array_item_type: ?runtime_schema.AntflyType = null,
};

pub const Filter = struct {
    predicates: []const runtime_schema.RelationalCheck = &.{},
    array_any: []const db_mod.types.RelationalRowsArrayAnyPredicate = &.{},
    array_contains: []const db_mod.types.RelationalRowsArrayContainsPredicate = &.{},
    array_eq: []const db_mod.types.RelationalRowsArrayEqPredicate = &.{},
    in_predicates: []const db_mod.types.RelationalRowsInPredicate = &.{},
    json_contains: []const db_mod.types.RelationalRowsJsonContainsPredicate = &.{},
    json_path_eq: []const db_mod.types.RelationalRowsJsonPathEqPredicate = &.{},
    json_path_exists: []const db_mod.types.RelationalRowsJsonPathExistsPredicate = &.{},
    text_patterns: []const db_mod.types.RelationalRowsTextPatternPredicate = &.{},
    expressions: []const db_mod.types.RelationalRowsExpressionCondition = &.{},
    expression_array_contains: []const db_mod.types.RelationalRowsExpressionArrayContainsPredicate = &.{},
    any_groups: []const db_mod.types.RelationalRowsExpressionPredicateGroup = &.{},
    not_groups: []const db_mod.types.RelationalRowsExpressionPredicateGroup = &.{},
};

pub const PercentileArgument = struct {
    percentile: ?f64 = null,
    percentiles: []const f64 = &.{},
};

pub const InputExpressionParserOptions = struct {
    context_hooks: expr_row_parse.SelectParserContextHooks,
    row_expression_hooks: expr_row_parse.RowExpressionParserHooks,
    arithmetic_hooks: expr_row_parse.ArithmeticExpressionParserHooks,
    variadic_hooks: expr_row_parse.VariadicRowExpressionParserHooks,
};

pub const SpecParserOptions = struct {
    function_bindings: expr_row_parse.SqlFunctionBindings = .{},
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst = null,
    order_expression_hooks: expr_order.OrderExpressionParserOptions,
    aggregate_input: InputExpressionParserOptions,
    expression_alternatives: expr_where_condition.ExpressionWhereConditionAlternativesParserOptions,
    expression_conditions: expr_where_condition.ExpressionWhereConditionsParserOptions,
    fixed_binary: expr_row_parse.FixedBinaryRowExpressionParserOptions,
    realtime_ns: u64,
};

pub const OutputFieldParserOptions = struct {
    params: []const value_mod.SqlValue = &.{},
    context_hooks: expr_row_parse.SelectParserContextHooks,
    aggregate_spec_options: SpecParserOptions,
};

pub const OutputFieldExpressionConditionParserOptions = struct {
    params: []const value_mod.SqlValue = &.{},
    output_field_options: OutputFieldParserOptions,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst = null,
};

pub const OutputExpressionConditionParserOptions = struct {
    context_hooks: expr_row_parse.SelectParserContextHooks,
    case_expression_hooks: expr_row_parse.CaseExpressionParserHooks,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst = null,
};

pub const BareBooleanHavingExpressionParserOptions = struct {
    context_hooks: expr_row_parse.SelectParserContextHooks,
    bare_boolean_hooks: expr_predicate.BareBooleanWhereExpressionParserOptions,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst = null,
};

fn aggregateOpForGeneratedKind(
    kind: generated_parser.GeneratedSqlAggregateFunctionKind,
) db_mod.types.RelationalRowsAggregateOp {
    return switch (kind) {
        .count => .count,
        .sum => .sum,
        .avg => .avg,
        .min => .min,
        .max => .max,
        .array_agg => .array_agg,
        .string_agg => .string_agg,
        .percentile_cont => .percentile_cont,
        .percentile_disc => .percentile_disc,
        .mode => .mode,
        .bool_or => .bool_or,
        .bool_and => .bool_and,
    };
}

pub fn generatedOpForExpression(
    tokens: []const Token,
    expression: *const generated_parser.GeneratedSqlExpressionAst,
) !?db_mod.types.RelationalRowsAggregateOp {
    try expr_generated_validate.validateGeneratedExpressionPayloads(tokens, expression.*);
    if (expression.kind != .function_call) return null;
    return aggregateOpForGeneratedKind(expression.aggregate_function_kind orelse return null);
}

fn validateGeneratedEmptyList(list: generated_parser.GeneratedSqlListAst) !void {
    if (list.count != 0 or
        list.first_tokens != null or
        list.last_tokens != null or
        list.items.len != 0 or
        list.expression_items.len != 0 or
        list.alias_items.len != 0 or
        list.alias_name_items.len != 0 or
        list.direction_items.len != 0 or
        list.directions.len != 0 or
        list.order_using_operator_items.len != 0 or
        list.nulls_order_items.len != 0 or
        list.nulls_orders.len != 0 or
        list.expressions.len != 0)
    {
        return error.UnsupportedSqlShape;
    }
}

fn validateGeneratedExpressionListForClause(
    tokens: []const Token,
    range: generated_parser.GeneratedSqlTokenRange,
    list: generated_parser.GeneratedSqlListAst,
) !void {
    if (range.start >= range.end or range.end > tokens.len) return error.UnsupportedSqlShape;
    if (list.count == 0 or list.items.len != list.count or list.expression_items.len != list.count or list.expressions.len != list.count) return error.UnsupportedSqlShape;
    if (list.alias_items.len != list.count or list.alias_name_items.len != list.count) return error.UnsupportedSqlShape;
    if (list.direction_items.len != list.count or list.directions.len != list.count) return error.UnsupportedSqlShape;
    if (list.order_using_operator_items.len != list.count or list.nulls_order_items.len != list.count or list.nulls_orders.len != list.count) return error.UnsupportedSqlShape;
    if (list.first_tokens == null or !expr_generated.generatedTokenRangeEqual(list.first_tokens.?, list.items[0])) return error.UnsupportedSqlShape;
    if (list.last_tokens == null or !expr_generated.generatedTokenRangeEqual(list.last_tokens.?, list.items[list.count - 1])) return error.UnsupportedSqlShape;

    for (list.items, 0..) |item, index| {
        if (item.start >= item.end or item.start < range.start or item.end > range.end) return error.UnsupportedSqlShape;
        if (index == 0) {
            if (item.start != range.start) return error.UnsupportedSqlShape;
        } else {
            const previous = list.items[index - 1];
            if (previous.end + 1 != item.start or previous.end >= tokens.len or tokens[previous.end].kind != .comma) return error.UnsupportedSqlShape;
        }
        if (index + 1 == list.count and item.end != range.end) return error.UnsupportedSqlShape;
        if (list.alias_items[index] != null or list.alias_name_items[index] != null) return error.UnsupportedSqlShape;
        if (list.direction_items[index] != null or list.directions[index] != null) return error.UnsupportedSqlShape;
        if (list.order_using_operator_items[index] != null or list.nulls_order_items[index] != null or list.nulls_orders[index] != null) return error.UnsupportedSqlShape;

        const expression_range = list.expression_items[index];
        if (!expr_generated.generatedTokenRangeEqual(expression_range, item)) return error.UnsupportedSqlShape;
        if (!expr_generated.generatedTokenRangeEqual(list.expressions[index].tokens orelse return error.UnsupportedSqlShape, expression_range)) return error.UnsupportedSqlShape;
        try expr_generated_validate.validateGeneratedExpressionPayloads(tokens, list.expressions[index]);
    }
}

fn validateGeneratedOrderListForClause(
    tokens: []const Token,
    range: generated_parser.GeneratedSqlTokenRange,
    list: generated_parser.GeneratedSqlListAst,
) !void {
    if (range.start >= range.end or range.end > tokens.len) return error.UnsupportedSqlShape;
    if (list.count == 0 or list.items.len != list.count or list.expression_items.len != list.count or list.expressions.len != list.count) return error.UnsupportedSqlShape;
    if (list.alias_items.len != list.count or list.alias_name_items.len != list.count) return error.UnsupportedSqlShape;
    if (list.direction_items.len != list.count or list.directions.len != list.count) return error.UnsupportedSqlShape;
    if (list.order_using_operator_items.len != list.count or list.nulls_order_items.len != list.count or list.nulls_orders.len != list.count) return error.UnsupportedSqlShape;
    if (list.first_tokens == null or !expr_generated.generatedTokenRangeEqual(list.first_tokens.?, list.items[0])) return error.UnsupportedSqlShape;
    if (list.last_tokens == null or !expr_generated.generatedTokenRangeEqual(list.last_tokens.?, list.items[list.count - 1])) return error.UnsupportedSqlShape;

    for (list.items, 0..) |item, index| {
        if (item.start >= item.end or item.start < range.start or item.end > range.end) return error.UnsupportedSqlShape;
        if (index == 0) {
            if (item.start != range.start) return error.UnsupportedSqlShape;
        } else {
            const previous = list.items[index - 1];
            if (previous.end + 1 != item.start or previous.end >= tokens.len or tokens[previous.end].kind != .comma) return error.UnsupportedSqlShape;
        }
        if (index + 1 == list.count and item.end != range.end) return error.UnsupportedSqlShape;
        if (list.alias_items[index] != null or list.alias_name_items[index] != null) return error.UnsupportedSqlShape;

        const expression_range = list.expression_items[index];
        if (expression_range.start < item.start or expression_range.end > item.end or expression_range.start >= expression_range.end) return error.UnsupportedSqlShape;
        if (!expr_generated.generatedTokenRangeEqual(list.expressions[index].tokens orelse return error.UnsupportedSqlShape, expression_range)) return error.UnsupportedSqlShape;
        try expr_generated_validate.validateGeneratedExpressionPayloads(tokens, list.expressions[index]);

        const direction_end = if (list.direction_items[index]) |direction_range| blk: {
            if (direction_range.start != expression_range.end or direction_range.end > item.end or direction_range.start >= direction_range.end) return error.UnsupportedSqlShape;
            if (tokens[direction_range.start].matchesKeywordTag(.using)) {
                const using_operator = list.order_using_operator_items[index] orelse return error.UnsupportedSqlShape;
                if (list.directions[index] != null) return error.UnsupportedSqlShape;
                if (using_operator.start != direction_range.start + 1 or using_operator.end != direction_range.end) return error.UnsupportedSqlShape;
                if (using_operator.end != using_operator.start + 1) return error.UnsupportedSqlShape;
                switch (tokens[using_operator.start].kind) {
                    .lt, .lte, .gt, .gte => {},
                    else => return error.UnsupportedSqlShape,
                }
            } else {
                if (list.order_using_operator_items[index] != null) return error.UnsupportedSqlShape;
                const direction = list.directions[index] orelse return error.UnsupportedSqlShape;
                if (direction_range.end != direction_range.start + 1) return error.UnsupportedSqlShape;
                switch (direction) {
                    .asc => if (!tokens[direction_range.start].matchesKeywordTag(.asc)) return error.UnsupportedSqlShape,
                    .desc => if (!tokens[direction_range.start].matchesKeywordTag(.desc)) return error.UnsupportedSqlShape,
                }
            }
            break :blk direction_range.end;
        } else blk: {
            if (list.directions[index] != null or list.order_using_operator_items[index] != null) return error.UnsupportedSqlShape;
            break :blk expression_range.end;
        };

        const item_end = if (list.nulls_order_items[index]) |nulls_range| blk: {
            if (nulls_range.start != direction_end or nulls_range.end != nulls_range.start + 2 or nulls_range.end > item.end) return error.UnsupportedSqlShape;
            if (!tokens[nulls_range.start].matchesKeywordTag(.nulls)) return error.UnsupportedSqlShape;
            const nulls_order = list.nulls_orders[index] orelse return error.UnsupportedSqlShape;
            switch (nulls_order) {
                .first => if (!tokens[nulls_range.start + 1].matchesKeywordTag(.first)) return error.UnsupportedSqlShape,
                .last => if (!tokens[nulls_range.start + 1].matchesKeywordTag(.last)) return error.UnsupportedSqlShape,
            }
            break :blk nulls_range.end;
        } else blk: {
            if (list.nulls_orders[index] != null) return error.UnsupportedSqlShape;
            break :blk direction_end;
        };
        if (item_end != item.end) return error.UnsupportedSqlShape;
    }
}

pub fn validateGeneratedFunctionForSpec(
    tokens: []const Token,
    op: db_mod.types.RelationalRowsAggregateOp,
    function_start: usize,
    argument_start: usize,
    argument_end: usize,
    within_group_start: ?usize,
    within_group_order_start: ?usize,
    within_group_order_end: ?usize,
    within_group_end: ?usize,
    filter_start: ?usize,
    filter_predicate_start: ?usize,
    filter_predicate_end: ?usize,
    filter_end: ?usize,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
) !void {
    const expression = generated_expression_ast orelse return;
    if (expression.kind != .function_call) return error.UnsupportedSqlShape;
    const function_tokens = expression.tokens orelse return error.UnsupportedSqlShape;
    if (function_tokens.start != function_start or function_tokens.end > tokens.len) return error.UnsupportedSqlShape;
    if (expression.function_name_tokens == null or expression.function_name_tokens.?.start != function_start) return error.UnsupportedSqlShape;
    if (expression.function_name_tokens.?.end >= function_tokens.end) return error.UnsupportedSqlShape;
    const generated_op = aggregateOpForGeneratedKind(expression.aggregate_function_kind orelse return error.UnsupportedSqlShape);
    if (generated_op != op) return error.UnsupportedSqlShape;

    if (argument_start < argument_end) {
        if (expression.argument_tokens == null) return error.UnsupportedSqlShape;
        if (expression.argument_tokens.?.start != argument_start or expression.argument_tokens.?.end != argument_end) return error.UnsupportedSqlShape;
        const value_start = if (expression.argument_distinct_tokens) |distinct_tokens| blk: {
            if (distinct_tokens.start != argument_start or distinct_tokens.end != argument_start + 1) return error.UnsupportedSqlShape;
            if (!tokens[distinct_tokens.start].matchesKeywordTag(.distinct)) return error.UnsupportedSqlShape;
            break :blk distinct_tokens.end;
        } else argument_start;
        if (expression.argument_value_tokens == null) return error.UnsupportedSqlShape;
        const value_tokens = expression.argument_value_tokens.?;
        if (value_tokens.start != value_start or value_tokens.end > argument_end or value_tokens.start >= value_tokens.end) return error.UnsupportedSqlShape;
        if (expression.argument_order_tokens) |order_tokens| {
            if (value_tokens.end + 2 > argument_end or value_tokens.end >= tokens.len) return error.UnsupportedSqlShape;
            if (!tokens[value_tokens.end].matchesKeywordTag(.order) or !tokens[value_tokens.end + 1].matchesKeywordTag(.by)) return error.UnsupportedSqlShape;
            if (order_tokens.start != value_tokens.end + 2 or order_tokens.end != argument_end) return error.UnsupportedSqlShape;
            try validateGeneratedOrderListForClause(tokens, order_tokens, expression.argument_order_items);
        } else if (value_tokens.end != argument_end) {
            return error.UnsupportedSqlShape;
        } else {
            try validateGeneratedEmptyList(expression.argument_order_items);
        }
        try validateGeneratedExpressionListForClause(tokens, value_tokens, expression.argument_items);
    } else {
        if (expression.argument_tokens != null or expression.argument_distinct_tokens != null or expression.argument_value_tokens != null or expression.argument_order_tokens != null) return error.UnsupportedSqlShape;
        try validateGeneratedEmptyList(expression.argument_items);
        try validateGeneratedEmptyList(expression.argument_order_items);
    }

    if (within_group_start) |start| {
        const end = within_group_end orelse return error.UnsupportedSqlShape;
        const order_start = within_group_order_start orelse return error.UnsupportedSqlShape;
        const order_end = within_group_order_end orelse return error.UnsupportedSqlShape;
        if (expression.within_group_tokens == null or expression.within_group_order_tokens == null) return error.UnsupportedSqlShape;
        if (expression.within_group_tokens.?.start != start or expression.within_group_tokens.?.end != end) return error.UnsupportedSqlShape;
        if (expression.within_group_order_tokens.?.start != order_start or expression.within_group_order_tokens.?.end != order_end) return error.UnsupportedSqlShape;
        try validateGeneratedOrderListForClause(tokens, expression.within_group_order_tokens.?, expression.within_group_order_items);
    } else {
        if (within_group_end != null or within_group_order_start != null or within_group_order_end != null) return error.UnsupportedSqlShape;
        if (expression.within_group_tokens != null or expression.within_group_order_tokens != null) return error.UnsupportedSqlShape;
        try validateGeneratedEmptyList(expression.within_group_order_items);
    }

    if (filter_start) |start| {
        const end = filter_end orelse return error.UnsupportedSqlShape;
        const predicate_start = filter_predicate_start orelse return error.UnsupportedSqlShape;
        const predicate_end = filter_predicate_end orelse return error.UnsupportedSqlShape;
        if (expression.filter_tokens == null or expression.filter_predicate_tokens == null or expression.filter_expression == null) return error.UnsupportedSqlShape;
        if (expression.filter_tokens.?.start != start or expression.filter_tokens.?.end != end) return error.UnsupportedSqlShape;
        if (expression.filter_predicate_tokens.?.start != predicate_start or expression.filter_predicate_tokens.?.end != predicate_end) return error.UnsupportedSqlShape;
        if (!expr_generated.generatedTokenRangeEqual(expression.filter_expression.?.tokens orelse return error.UnsupportedSqlShape, expression.filter_predicate_tokens.?)) return error.UnsupportedSqlShape;
    } else {
        if (filter_end != null or filter_predicate_start != null or filter_predicate_end != null) return error.UnsupportedSqlShape;
        if (expression.filter_tokens != null or expression.filter_predicate_tokens != null or expression.filter_expression != null) return error.UnsupportedSqlShape;
    }
}

pub fn opForName(name: []const u8) ?db_mod.types.RelationalRowsAggregateOp {
    if (std.ascii.eqlIgnoreCase(name, "count")) return .count;
    if (std.ascii.eqlIgnoreCase(name, "sum")) return .sum;
    if (std.ascii.eqlIgnoreCase(name, "min")) return .min;
    if (std.ascii.eqlIgnoreCase(name, "max")) return .max;
    if (std.ascii.eqlIgnoreCase(name, "avg")) return .avg;
    if (std.ascii.eqlIgnoreCase(name, "percentile_cont")) return .percentile_cont;
    if (std.ascii.eqlIgnoreCase(name, "percentile_disc")) return .percentile_disc;
    if (std.ascii.eqlIgnoreCase(name, "mode")) return .mode;
    if (std.ascii.eqlIgnoreCase(name, "array_agg")) return .array_agg;
    if (std.ascii.eqlIgnoreCase(name, "string_agg")) return .string_agg;
    if (std.ascii.eqlIgnoreCase(name, "bool_or")) return .bool_or;
    if (std.ascii.eqlIgnoreCase(name, "bool_and")) return .bool_and;
    return null;
}

pub fn opName(op: db_mod.types.RelationalRowsAggregateOp) []const u8 {
    return switch (op) {
        .count => "count",
        .sum => "sum",
        .min => "min",
        .max => "max",
        .avg => "avg",
        .percentile_cont => "percentile_cont",
        .percentile_disc => "percentile_disc",
        .mode => "mode",
        .array_agg => "array_agg",
        .string_agg => "string_agg",
        .bool_or => "bool_or",
        .bool_and => "bool_and",
    };
}

pub fn nextIsFunction(tokens: []const Token, pos: usize) bool {
    if (pos + 1 >= tokens.len) return false;
    if (tokens[pos].kind != .identifier or tokens[pos + 1].kind != .lparen) return false;
    return opForName(tokens[pos].text) != null;
}

pub fn jsonPathEqFilterCanStartAt(tokens: []const Token, pos: usize) bool {
    if (pos + 4 >= tokens.len) return false;
    if (!expr_operator.jsonExtractExpressionCanStartAt(tokens, pos)) return false;
    if (tokens[pos + 3].kind != .eq) return false;
    const as_text = expr_operator.tokenKindIsJsonExtractTextOperator(tokens[pos + 1].kind);
    const rhs = tokens[pos + 4];
    return switch (rhs.kind) {
        .string, .placeholder => true,
        .number, .minus => as_text,
        .identifier => as_text and
            !rhs.matchesKeywordTag(.any) and
            !rhs.matchesKeywordTag(.some) and
            !rhs.matchesKeywordTag(.all) and
            (rhs.matchesKeywordTag(.null) or
                rhs.matchesKeywordTag(.true) or
                rhs.matchesKeywordTag(.false)),
        else => false,
    };
}

pub fn canParseFilterNot(tokens: []const Token, pos: usize) bool {
    return parser.peekKeywordTag(tokens, pos, .not) and pos + 1 < tokens.len and tokens[pos + 1].kind == .lparen;
}

pub fn havingHasBooleanIsNot(tokens: []const Token, pos: usize) bool {
    var depth: usize = 0;
    var i = pos;
    while (i + 2 < tokens.len) : (i += 1) {
        const token = tokens[i];
        switch (token.kind) {
            .lparen => depth += 1,
            .rparen => {
                if (depth == 0) return false;
                depth -= 1;
            },
            .identifier => {
                if (depth == 0 and (token.matchesKeywordTag(.order) or
                    token.matchesKeywordTag(.limit) or
                    token.matchesKeywordTag(.offset) or
                    token.matchesKeywordTag(.fetch)))
                {
                    return false;
                }
                if (token.matchesKeywordTag(.is) and
                    tokens[i + 1].matchesKeywordTag(.not) and
                    (tokens[i + 2].matchesKeywordTag(.true) or
                        tokens[i + 2].matchesKeywordTag(.false)))
                {
                    return true;
                }
            },
            else => {},
        }
    }
    return false;
}

pub fn canParseHavingNot(tokens: []const Token, pos: usize) bool {
    return parser.peekKeywordTag(tokens, pos, .not) and pos + 1 < tokens.len and tokens[pos + 1].kind == .lparen;
}

pub fn matchBooleanGroupOpen(tokens: []const Token, pos: *usize) bool {
    if (!parser.peekKind(tokens, pos.*, .lparen)) return false;
    if (peekParenthesizedExpressionCondition(tokens, pos.*)) return false;
    _ = parser.matchToken(tokens, pos, .lparen) orelse unreachable;
    return true;
}

pub fn parseOutputFieldAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    group_fields: []const []const u8,
    group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    aggregations: []const db_mod.types.RelationalRowsAggregateSpec,
    options: OutputFieldParserOptions,
) ![]const u8 {
    if (nextIsFunction(tokens, pos.*)) {
        const context = options.context_hooks.get_context(options.context_hooks.ptr);
        const candidate = try parseSpecAlloc(
            alloc,
            tokens,
            pos,
            options.params,
            context.schema,
            options.context_hooks.row_expression_type_context(options.context_hooks.ptr),
            context.field_expression_qualifiers,
            context.returning_expression_qualifiers,
            context.defer_row_expression_field_validation,
            options.aggregate_spec_options,
        );
        defer plan_mod.freeAggregateSpec(alloc, candidate);
        for (aggregations) |aggregation| {
            if (!expr_equal.aggregateSpecsEquivalent(candidate, aggregation)) continue;
            return try alloc.dupe(u8, aggregation.name);
        }
        return error.UnsupportedSqlShape;
    }

    const field = try grammar.parseIdentifierOwnedAlloc(alloc, tokens, pos);
    errdefer alloc.free(field);
    if (!outputFieldIsUnique(group_fields, group_expressions, aggregations, field)) return error.UnsupportedSqlShape;
    return field;
}

pub fn parseInputExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
    options: InputExpressionParserOptions,
) !db_mod.types.RelationalRowsExpression {
    if (peekExpressionInput(tokens, pos.*)) {
        return try expr_row_parse.parseRowExpressionAlloc(
            alloc,
            tokens,
            pos,
            options.context_hooks.row_expression_type_context(options.context_hooks.ptr),
            options.row_expression_hooks,
            options.arithmetic_hooks,
            options.variadic_hooks,
        );
    }

    const parsed_field = try expr_generated.parseRowExpressionFieldOwnedAlloc(
        alloc,
        tokens,
        pos,
        schema,
        field_expression_qualifiers,
        returning_expression_qualifiers,
        defer_row_expression_field_validation,
    );
    defer alloc.free(parsed_field);
    const field = try binder.normalizeRowExpressionFieldAlloc(
        alloc,
        schema,
        parsed_field,
        field_expression_qualifiers,
        returning_expression_qualifiers,
        defer_row_expression_field_validation,
    );
    var field_transferred = false;
    errdefer if (!field_transferred) alloc.free(field);
    const column = binder.relationalColumnForField(schema, field, null) orelse return error.InvalidSqlCatalog;
    if (column.field_type != .numeric) return error.InvalidSqlCatalog;
    if (expr_operator.peekArithmeticOperator(tokens, pos.*) == null) return error.UnsupportedSqlShape;
    field_transferred = true;
    return try expr_row_parse.parseArithmeticExpressionRestAlloc(
        alloc,
        tokens,
        pos,
        .{ .kind = .field, .field = field },
        0,
        options.context_hooks.row_expression_type_context(options.context_hooks.ptr),
        options.arithmetic_hooks,
    );
}

pub fn parseSpecAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
    schema: runtime_schema.TableSchema,
    type_context: expr_type.RowExpressionTypeContext,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
    options: SpecParserOptions,
) !db_mod.types.RelationalRowsAggregateSpec {
    const function_start = pos.*;
    const generated_op = if (options.generated_expression_ast) |expression| blk: {
        const op = try generatedOpForExpression(tokens, expression) orelse return error.UnsupportedSqlShape;
        const expression_tokens = expression.tokens orelse return error.UnsupportedSqlShape;
        if (expression_tokens.start != function_start) return error.UnsupportedSqlShape;
        break :blk op;
    } else null;
    const function_name = parser.matchToken(tokens, pos, .identifier) orelse return error.UnsupportedSqlShape;
    const token_op = opForName(function_name.text) orelse return error.UnsupportedSqlShape;
    const op = if (generated_op) |parsed_op| blk: {
        if (parsed_op != token_op) return error.UnsupportedSqlShape;
        break :blk parsed_op;
    } else token_op;
    try parser.expectToken(tokens, pos, .lparen);
    const argument_start = pos.*;
    const distinct = parser.matchKeyword(tokens, pos, "distinct");
    var field: ?[]const u8 = null;
    var field_transferred = false;
    errdefer if (!field_transferred) if (field) |owned| alloc.free(owned);
    var expression: ?db_mod.types.RelationalRowsExpression = null;
    var expression_transferred = false;
    errdefer if (!expression_transferred) if (expression) |owned| freeExpression(alloc, owned);
    var string_delimiter: ?[]const u8 = null;
    var string_delimiter_transferred = false;
    errdefer if (!string_delimiter_transferred) if (string_delimiter) |delimiter| alloc.free(delimiter);
    var percentile: ?f64 = null;
    var percentiles: []const f64 = &.{};
    var percentiles_transferred = false;
    errdefer if (!percentiles_transferred and percentiles.len > 0) alloc.free(percentiles);
    var percentile_order: db_mod.types.RelationalRowsQueryOrderDirection = .asc;
    var array_order_by = std.ArrayListUnmanaged(db_mod.types.RelationalRowsQueryOrder).empty;
    errdefer {
        freeOrderBy(alloc, array_order_by.items);
        array_order_by.deinit(alloc);
    }
    var argument_end: usize = undefined;
    var within_group_start: ?usize = null;
    var within_group_order_start: ?usize = null;
    var within_group_order_end: ?usize = null;
    var within_group_end: ?usize = null;
    if (isPercentileOp(op)) {
        if (distinct) return error.UnsupportedSqlShape;
        const percentile_argument = try parsePercentileArgumentAlloc(alloc, tokens, pos, params);
        percentile = percentile_argument.percentile;
        percentiles = percentile_argument.percentiles;
        argument_end = pos.*;
        try parser.expectToken(tokens, pos, .rparen);
        within_group_start = pos.*;
        try parser.expectKeyword(tokens, pos, "within");
        try parser.expectKeyword(tokens, pos, "group");
        try parser.expectToken(tokens, pos, .lparen);
        try parser.expectKeyword(tokens, pos, "order");
        try parser.expectKeyword(tokens, pos, "by");
        within_group_order_start = pos.*;
        var order = try expr_order.parseExpressionAlloc(
            alloc,
            tokens,
            pos,
            schema,
            options.function_bindings,
            field_expression_qualifiers,
            returning_expression_qualifiers,
            defer_row_expression_field_validation,
            type_context,
            options.order_expression_hooks,
        );
        defer {
            if (order.field.len > 0) alloc.free(order.field);
            if (order.expression) |owned| freeExpression(alloc, owned);
        }
        _ = try expr_order.parseModifiers(tokens, pos, &order);
        if (order.null_test != null) return error.UnsupportedSqlShape;
        percentile_order = order.direction;
        if (order.field.len > 0) {
            const column = binder.relationalColumnForField(schema, order.field, null) orelse return error.InvalidSqlCatalog;
            if (column.field_type != .numeric) return error.InvalidSqlCatalog;
            field = order.field;
            order.field = "";
        } else if (order.expression) |owned| {
            try type_context.validateNumericRowExpression(owned);
            expression = owned;
            order.expression = null;
        } else {
            return error.UnsupportedSqlShape;
        }
        within_group_order_end = pos.*;
        try parser.expectToken(tokens, pos, .rparen);
        within_group_end = pos.*;
    } else if (op == .mode) {
        if (distinct) return error.UnsupportedSqlShape;
        argument_end = pos.*;
        try parser.expectToken(tokens, pos, .rparen);
        within_group_start = pos.*;
        try parser.expectKeyword(tokens, pos, "within");
        try parser.expectKeyword(tokens, pos, "group");
        try parser.expectToken(tokens, pos, .lparen);
        try parser.expectKeyword(tokens, pos, "order");
        try parser.expectKeyword(tokens, pos, "by");
        within_group_order_start = pos.*;
        var order = try expr_order.parseExpressionAlloc(
            alloc,
            tokens,
            pos,
            schema,
            options.function_bindings,
            field_expression_qualifiers,
            returning_expression_qualifiers,
            defer_row_expression_field_validation,
            type_context,
            options.order_expression_hooks,
        );
        defer {
            if (order.field.len > 0) alloc.free(order.field);
            if (order.expression) |owned| freeExpression(alloc, owned);
        }
        _ = try expr_order.parseModifiers(tokens, pos, &order);
        if (order.null_test != null) return error.UnsupportedSqlShape;
        percentile_order = order.direction;
        if (order.field.len > 0) {
            const column = binder.relationalColumnForField(schema, order.field, null) orelse return error.InvalidSqlCatalog;
            if (!expr_type.sqlAggregateModeTypeAllowed(column.field_type)) return error.InvalidSqlCatalog;
            field = order.field;
            order.field = "";
        } else if (order.expression) |owned| {
            try expr_type.validateAggregateModeRowExpression(type_context, owned);
            expression = owned;
            order.expression = null;
        } else {
            return error.UnsupportedSqlShape;
        }
        within_group_order_end = pos.*;
        try parser.expectToken(tokens, pos, .rparen);
        within_group_end = pos.*;
    } else if (op == .count and parser.matchToken(tokens, pos, .star) != null) {
        if (distinct) return error.UnsupportedSqlShape;
        field = null;
    } else if (op == .count and parser.peekKind(tokens, pos.*, .number)) {
        if (distinct) return error.UnsupportedSqlShape;
        _ = parser.matchToken(tokens, pos, .number) orelse unreachable;
        field = null;
    } else {
        if (peekExpressionInput(tokens, pos.*)) {
            const parsed_expression = try parseInputExpressionAlloc(
                alloc,
                tokens,
                pos,
                schema,
                field_expression_qualifiers,
                returning_expression_qualifiers,
                defer_row_expression_field_validation,
                options.aggregate_input,
            );
            var parsed_expression_transferred = false;
            errdefer if (!parsed_expression_transferred) freeExpression(alloc, parsed_expression);
            try expr_type.validateAggregateInputExpression(type_context, op, parsed_expression);
            expression = parsed_expression;
            parsed_expression_transferred = true;
        } else {
            const raw_field = try expr_generated.parseRowExpressionFieldOwnedAlloc(
                alloc,
                tokens,
                pos,
                schema,
                field_expression_qualifiers,
                returning_expression_qualifiers,
                defer_row_expression_field_validation,
            );
            defer alloc.free(raw_field);
            const parsed_field = try binder.normalizeRowExpressionFieldAlloc(
                alloc,
                schema,
                raw_field,
                field_expression_qualifiers,
                returning_expression_qualifiers,
                defer_row_expression_field_validation,
            );
            var parsed_field_transferred = false;
            errdefer if (!parsed_field_transferred) alloc.free(parsed_field);
            if (binder.relationalColumnForField(schema, parsed_field, null)) |column| {
                switch (op) {
                    .count, .array_agg => {},
                    .string_agg => if (column.field_type != .keyword and column.field_type != .text and column.field_type != .link) return error.InvalidSqlCatalog,
                    .sum, .avg, .percentile_cont, .percentile_disc => if (column.field_type != .numeric) return error.InvalidSqlCatalog,
                    .mode => if (!expr_type.sqlAggregateModeTypeAllowed(column.field_type)) return error.InvalidSqlCatalog,
                    .min, .max => if (!expr_type.sqlAggregateMinMaxTypeAllowed(column.field_type)) return error.InvalidSqlCatalog,
                    .bool_or, .bool_and => if (column.field_type != .boolean) return error.InvalidSqlCatalog,
                }
            } else {
                return error.InvalidSqlCatalog;
            }
            field = parsed_field;
            parsed_field_transferred = true;
        }
    }
    if (op == .string_agg) {
        try parser.expectToken(tokens, pos, .comma);
        string_delimiter = try parseStringDelimiterAlloc(alloc, tokens, pos);
    }
    if ((op == .array_agg or op == .string_agg) and parser.matchKeyword(tokens, pos, "order")) {
        try parser.expectKeyword(tokens, pos, "by");
        try expr_order.parseByWithExpressionHooksAlloc(
            alloc,
            tokens,
            pos,
            &array_order_by,
            schema,
            options.function_bindings,
            field_expression_qualifiers,
            returning_expression_qualifiers,
            defer_row_expression_field_validation,
            type_context,
            options.order_expression_hooks,
            null,
            null,
        );
    }
    if (!isPercentileOp(op) and op != .mode) {
        argument_end = pos.*;
        try parser.expectToken(tokens, pos, .rparen);
    }
    const filter_start = if (parser.peekKeyword(tokens, pos.*, "filter")) pos.* else null;
    const filter_predicate_start = if (filter_start != null) pos.* + 3 else null;
    const generated_filter_expression: ?*const generated_parser.GeneratedSqlExpressionAst = if (filter_start != null) blk: {
        const generated_expression = options.generated_expression_ast orelse break :blk null;
        break :blk generated_expression.filter_expression;
    } else null;
    const filter = try parseFilterAlloc(
        alloc,
        tokens,
        pos,
        schema,
        field_expression_qualifiers,
        returning_expression_qualifiers,
        defer_row_expression_field_validation,
        params,
        type_context,
        options.expression_alternatives,
        options.expression_conditions,
        options.fixed_binary,
        options.realtime_ns,
        generated_filter_expression,
    );
    var filter_transferred = false;
    errdefer if (!filter_transferred) freeFilter(alloc, filter);
    const filter_end = if (filter_start != null) pos.* else null;
    const filter_predicate_end = if (filter_start != null) pos.* - 1 else null;
    try validateGeneratedFunctionForSpec(
        tokens,
        op,
        function_start,
        argument_start,
        argument_end,
        within_group_start,
        within_group_order_start,
        within_group_order_end,
        within_group_end,
        filter_start,
        filter_predicate_start,
        filter_predicate_end,
        filter_end,
        options.generated_expression_ast,
    );
    const explicit_alias = try grammar.parseOptionalProjectionAliasAlloc(alloc, tokens, pos);
    defer if (explicit_alias) |alias| alloc.free(alias);
    const name = try aliasOrDefaultAlloc(alloc, explicit_alias, op, field);
    var name_transferred = false;
    errdefer if (!name_transferred) alloc.free(name);
    field_transferred = true;
    expression_transferred = true;
    string_delimiter_transferred = true;
    percentiles_transferred = true;
    filter_transferred = true;
    name_transferred = true;
    return .{
        .name = name,
        .op = op,
        .field = field,
        .expression = expression,
        .distinct = distinct,
        .distinct_max_items = if (distinct) db_mod.types.default_relational_rows_aggregate_distinct_max_items else 0,
        .percentile = percentile,
        .percentiles = percentiles,
        .percentile_max_items = if (isPercentileOp(op)) db_mod.types.default_relational_rows_percentile_max_items else 0,
        .percentile_order = percentile_order,
        .array_max_items = if (op == .array_agg or op == .string_agg) db_mod.types.default_relational_rows_array_agg_max_items else 0,
        .array_order_by = try array_order_by.toOwnedSlice(alloc),
        .string_delimiter = string_delimiter,
        .filter_predicates = filter.predicates,
        .filter_array_any = filter.array_any,
        .filter_array_contains = filter.array_contains,
        .filter_array_eq = filter.array_eq,
        .filter_in_predicates = filter.in_predicates,
        .filter_json_contains = filter.json_contains,
        .filter_json_path_eq = filter.json_path_eq,
        .filter_json_path_exists = filter.json_path_exists,
        .filter_text_patterns = filter.text_patterns,
        .filter_expressions = filter.expressions,
        .filter_expression_array_contains = filter.expression_array_contains,
        .filter_any = filter.any_groups,
        .filter_not = filter.not_groups,
    };
}

pub fn parseFilterBooleanIsNotGroupsAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
    groups: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
) !bool {
    const saved_pos = pos.*;
    const parsed_field = expr_generated.parseRowExpressionFieldOwnedAlloc(
        alloc,
        tokens,
        pos,
        schema,
        field_expression_qualifiers,
        returning_expression_qualifiers,
        defer_row_expression_field_validation,
    ) catch |err| {
        pos.* = saved_pos;
        return switch (err) {
            error.UnsupportedSqlShape, error.InvalidSqlCatalog => false,
            else => err,
        };
    };
    defer alloc.free(parsed_field);
    const operator_token_index = pos.*;
    if (!parser.matchKeywordTag(tokens, pos, .is)) {
        pos.* = saved_pos;
        return false;
    }
    if (!parser.matchKeywordTag(tokens, pos, .not)) {
        pos.* = saved_pos;
        return false;
    }
    if (!(parser.peekKeywordTag(tokens, pos.*, .true) or parser.peekKeywordTag(tokens, pos.*, .false))) {
        pos.* = saved_pos;
        return false;
    }

    const field = try binder.normalizeRowExpressionFieldAlloc(
        alloc,
        schema,
        parsed_field,
        field_expression_qualifiers,
        returning_expression_qualifiers,
        defer_row_expression_field_validation,
    );
    defer alloc.free(field);
    const column = binder.relationalColumnForField(schema, field, .boolean) orelse return error.InvalidSqlCatalog;
    const value = (try value_mod.parseSqlBooleanIsValue(tokens, pos, column)) orelse return error.UnsupportedSqlShape;
    const expected_kind: generated_parser.GeneratedSqlExpressionKind = if (value) .is_not_true else .is_not_false;
    try expr_generated_validate.validateGeneratedBooleanIsPredicateExpression(generated_expression_ast, tokens, operator_token_index, expected_kind);
    try expr_condition.appendBooleanIsNotExpressionGroups(alloc, groups, field, value);
    return true;
}

pub fn parseFilterConditionAlternativesAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
    schema: runtime_schema.TableSchema,
    type_context: expr_type.RowExpressionTypeContext,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
    alternatives: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    expression_hooks: expr_where_condition.ExpressionWhereConditionAlternativesParserOptions,
    realtime_ns: u64,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
) !void {
    if (value_mod.matchStandaloneSqlBooleanLiteral(tokens, pos)) |enabled| {
        try expr_condition.appendBooleanConstantExpressionGroup(alloc, alternatives, enabled);
        return;
    }
    const generated_condition_expression = try expr_generated_validate.generatedPredicateExpressionAtStart(tokens, pos.*, generated_expression_ast);
    var expression_hooks_with_generated = expression_hooks;
    expression_hooks_with_generated.generated_expression_ast = generated_condition_expression;

    if (try parseFilterBooleanIsNotGroupsAlloc(
        alloc,
        tokens,
        pos,
        schema,
        field_expression_qualifiers,
        returning_expression_qualifiers,
        defer_row_expression_field_validation,
        alternatives,
        generated_condition_expression,
    )) {
        return;
    }

    if (peekExpressionFilter(tokens, pos.*) or expr_predicate.peekSimpleScalarSetPredicate(tokens, pos.*)) {
        try expr_where_condition.parseExpressionWhereConditionAlternativesAlloc(
            alloc,
            tokens,
            pos,
            params,
            type_context,
            defer_row_expression_field_validation,
            alternatives,
            expression_hooks_with_generated,
        );
        return;
    }

    const predicate = try expr_predicate.parseScalarWherePredicateAlloc(
        alloc,
        tokens,
        pos,
        schema,
        field_expression_qualifiers,
        returning_expression_qualifiers,
        defer_row_expression_field_validation,
        params,
        realtime_ns,
        generated_condition_expression,
    );
    const condition = try select_set.expressionConditionFromOwnedRelationalCheckAlloc(alloc, predicate);
    var condition_transferred = false;
    errdefer if (!condition_transferred) freeExpressionCondition(alloc, condition);
    try expr_condition.appendExpressionConditionGroup(alloc, alternatives, condition);
    condition_transferred = true;
}

pub fn parseFilterNotGroupAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
    schema: runtime_schema.TableSchema,
    type_context: expr_type.RowExpressionTypeContext,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
    not_groups: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    expression_hooks: expr_where_condition.ExpressionWhereConditionAlternativesParserOptions,
    realtime_ns: u64,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
) !void {
    try parser.expectKeyword(tokens, pos, "not");
    if (generated_expression_ast) |expression| {
        if (expression.kind != .logical_not) return error.UnsupportedSqlShape;
    }
    try parser.expectToken(tokens, pos, .lparen);
    var groups = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup).empty;
    var groups_transferred = false;
    defer groups.deinit(alloc);
    errdefer if (!groups_transferred) freeExpressionPredicateGroups(alloc, groups.items);
    try groups.append(alloc, .{ .conditions = &.{} });

    while (true) {
        var alternatives = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup).empty;
        defer alternatives.deinit(alloc);
        errdefer freeExpressionPredicateGroups(alloc, alternatives.items);
        try parseFilterConditionAlternativesAlloc(
            alloc,
            tokens,
            pos,
            params,
            schema,
            type_context,
            field_expression_qualifiers,
            returning_expression_qualifiers,
            defer_row_expression_field_validation,
            &alternatives,
            expression_hooks,
            realtime_ns,
            generated_expression_ast,
        );
        try expr_condition.andExpressionPredicateAlternatives(alloc, &groups, alternatives.items);
        freeExpressionPredicateGroups(alloc, alternatives.items);
        if (!parser.matchKeywordTag(tokens, pos, .@"and")) break;
    }
    if (groups.items.len == 0) return error.UnsupportedSqlShape;
    try parser.expectToken(tokens, pos, .rparen);

    try not_groups.appendSlice(alloc, groups.items);
    groups_transferred = true;
}

pub fn parseFilterOrGroupsAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
    schema: runtime_schema.TableSchema,
    type_context: expr_type.RowExpressionTypeContext,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
    any_groups: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    expression_hooks: expr_where_condition.ExpressionWhereConditionAlternativesParserOptions,
    realtime_ns: u64,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
) !void {
    while (true) {
        const parenthesized = matchBooleanGroupOpen(tokens, pos);
        var groups = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup).empty;
        var groups_transferred = false;
        defer groups.deinit(alloc);
        errdefer if (!groups_transferred) freeExpressionPredicateGroups(alloc, groups.items);
        try groups.append(alloc, .{ .conditions = &.{} });

        while (true) {
            var alternatives = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup).empty;
            defer alternatives.deinit(alloc);
            errdefer freeExpressionPredicateGroups(alloc, alternatives.items);
            try parseFilterConditionAlternativesAlloc(
                alloc,
                tokens,
                pos,
                params,
                schema,
                type_context,
                field_expression_qualifiers,
                returning_expression_qualifiers,
                defer_row_expression_field_validation,
                &alternatives,
                expression_hooks,
                realtime_ns,
                generated_expression_ast,
            );
            try expr_condition.andExpressionPredicateAlternatives(alloc, &groups, alternatives.items);
            freeExpressionPredicateGroups(alloc, alternatives.items);
            if (!parser.matchKeyword(tokens, pos, "and")) break;
        }
        if (groups.items.len == 0) return error.UnsupportedSqlShape;
        if (parenthesized) try parser.expectToken(tokens, pos, .rparen);
        try any_groups.appendSlice(alloc, groups.items);
        groups_transferred = true;

        if (!parser.matchKeyword(tokens, pos, "or")) break;
    }
}

fn parseFilterAtomAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
    schema: runtime_schema.TableSchema,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
    predicates: *std.ArrayListUnmanaged(runtime_schema.RelationalCheck),
    array_any: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayAnyPredicate),
    array_contains: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayContainsPredicate),
    array_eq: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayEqPredicate),
    in_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsInPredicate),
    json_contains: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonContainsPredicate),
    json_path_eq: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonPathEqPredicate),
    json_path_exists: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonPathExistsPredicate),
    text_patterns: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsTextPatternPredicate),
    realtime_ns: u64,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
) !void {
    var or_predicates = std.ArrayListUnmanaged(db_mod.types.RelationalRowsPredicateGroup).empty;
    defer {
        freePredicateGroups(alloc, or_predicates.items);
        or_predicates.deinit(alloc);
    }

    try expr_predicate.parseWhereAtomAlloc(
        alloc,
        tokens,
        pos,
        params,
        schema,
        field_expression_qualifiers,
        returning_expression_qualifiers,
        defer_row_expression_field_validation,
        predicates,
        json_contains,
        json_path_eq,
        json_path_exists,
        array_any,
        array_contains,
        array_eq,
        in_predicates,
        text_patterns,
        &or_predicates,
        false,
        realtime_ns,
        generated_expression_ast,
    );

    if (or_predicates.items.len > 0) return error.UnsupportedSqlShape;
}

pub fn parseFilterAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
    params: []const value_mod.SqlValue,
    type_context: expr_type.RowExpressionTypeContext,
    expression_hooks: expr_where_condition.ExpressionWhereConditionAlternativesParserOptions,
    expression_condition_hooks: expr_where_condition.ExpressionWhereConditionsParserOptions,
    fixed_binary_hooks: expr_row_parse.FixedBinaryRowExpressionParserOptions,
    realtime_ns: u64,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
) !Filter {
    if (!parser.matchKeyword(tokens, pos, "filter")) return .{};
    try parser.expectToken(tokens, pos, .lparen);
    try parser.expectKeyword(tokens, pos, "where");

    var predicates = std.ArrayListUnmanaged(runtime_schema.RelationalCheck).empty;
    errdefer {
        freeRelationalChecks(alloc, predicates.items);
        predicates.deinit(alloc);
    }
    var array_any = std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayAnyPredicate).empty;
    errdefer {
        freeArrayAny(alloc, array_any.items);
        array_any.deinit(alloc);
    }
    var array_contains = std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayContainsPredicate).empty;
    errdefer {
        freeArrayContains(alloc, array_contains.items);
        array_contains.deinit(alloc);
    }
    var array_eq = std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayEqPredicate).empty;
    errdefer {
        freeArrayEq(alloc, array_eq.items);
        array_eq.deinit(alloc);
    }
    var in_predicates = std.ArrayListUnmanaged(db_mod.types.RelationalRowsInPredicate).empty;
    errdefer {
        freeInPredicates(alloc, in_predicates.items);
        in_predicates.deinit(alloc);
    }
    var json_contains = std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonContainsPredicate).empty;
    errdefer {
        freeJsonContains(alloc, json_contains.items);
        json_contains.deinit(alloc);
    }
    var json_path_eq = std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonPathEqPredicate).empty;
    errdefer {
        freeJsonPathEq(alloc, json_path_eq.items);
        json_path_eq.deinit(alloc);
    }
    var json_path_exists = std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonPathExistsPredicate).empty;
    errdefer {
        freeJsonPathExists(alloc, json_path_exists.items);
        json_path_exists.deinit(alloc);
    }
    var text_patterns = std.ArrayListUnmanaged(db_mod.types.RelationalRowsTextPatternPredicate).empty;
    errdefer {
        freeTextPatterns(alloc, text_patterns.items);
        text_patterns.deinit(alloc);
    }
    var expressions = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionCondition).empty;
    errdefer {
        freeExpressionConditions(alloc, expressions.items);
        expressions.deinit(alloc);
    }
    var expression_array_contains = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionArrayContainsPredicate).empty;
    errdefer {
        freeExpressionArrayContains(alloc, expression_array_contains.items);
        expression_array_contains.deinit(alloc);
    }
    var any_groups = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup).empty;
    errdefer {
        freeExpressionPredicateGroups(alloc, any_groups.items);
        any_groups.deinit(alloc);
    }
    var not_groups = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup).empty;
    errdefer {
        freeExpressionPredicateGroups(alloc, not_groups.items);
        not_groups.deinit(alloc);
    }

    var saw_boolean_constant = false;
    if (parser.hasTopLevelOrBeforeCloseParen(tokens, pos.*)) {
        try parseFilterOrGroupsAlloc(
            alloc,
            tokens,
            pos,
            params,
            schema,
            type_context,
            field_expression_qualifiers,
            returning_expression_qualifiers,
            defer_row_expression_field_validation,
            &any_groups,
            expression_hooks,
            realtime_ns,
            generated_expression_ast,
        );
    } else {
        while (true) {
            if (canParseFilterNot(tokens, pos.*)) {
                const generated_condition_expression = try expr_generated_validate.generatedPredicateExpressionAtStart(tokens, pos.*, generated_expression_ast);
                try parseFilterNotGroupAlloc(
                    alloc,
                    tokens,
                    pos,
                    params,
                    schema,
                    type_context,
                    field_expression_qualifiers,
                    returning_expression_qualifiers,
                    defer_row_expression_field_validation,
                    &not_groups,
                    expression_hooks,
                    realtime_ns,
                    generated_condition_expression,
                );
            } else if (value_mod.matchStandaloneSqlBooleanLiteral(tokens, pos)) |enabled| {
                saw_boolean_constant = true;
                if (!enabled) try expr_condition.appendBooleanConstantExpressionCondition(alloc, &expressions, false);
            } else if (expr_predicate.stringToArrayPredicateIsContainment(tokens, pos.*)) {
                const operator_token_index = expr_predicate.stringToArrayContainmentOperatorTokenIndex(tokens, pos.*) orelse return error.UnsupportedSqlShape;
                try expr_generated_validate.validateGeneratedSingleOperatorPredicateIdentity(try expr_generated_validate.generatedPredicateExpressionAtStart(tokens, pos.*, generated_expression_ast), .contains, tokens, operator_token_index);
                const predicate = try expr_predicate.parseExpressionArrayContainsPredicateAlloc(
                    alloc,
                    tokens,
                    pos,
                    params,
                    type_context,
                    fixed_binary_hooks,
                );
                var predicate_transferred = false;
                errdefer if (!predicate_transferred) freeExpressionArrayContainsOne(alloc, predicate);
                try expression_array_contains.append(alloc, predicate);
                predicate_transferred = true;
            } else if (jsonPathEqFilterCanStartAt(tokens, pos.*)) {
                try parseFilterAtomAlloc(alloc, tokens, pos, params, schema, field_expression_qualifiers, returning_expression_qualifiers, defer_row_expression_field_validation, &predicates, &array_any, &array_contains, &array_eq, &in_predicates, &json_contains, &json_path_eq, &json_path_exists, &text_patterns, realtime_ns, try expr_generated_validate.generatedPredicateExpressionAtStart(tokens, pos.*, generated_expression_ast));
            } else if (peekExpressionFilter(tokens, pos.*)) {
                var expression_condition_hooks_with_generated = expression_condition_hooks;
                expression_condition_hooks_with_generated.generated_expression_ast = try expr_generated_validate.generatedPredicateExpressionAtStart(tokens, pos.*, generated_expression_ast);
                try expr_where_condition.parseExpressionWhereConditionsAlloc(
                    alloc,
                    tokens,
                    pos,
                    params,
                    type_context,
                    defer_row_expression_field_validation,
                    &expressions,
                    &any_groups,
                    &not_groups,
                    expression_condition_hooks_with_generated,
                );
            } else {
                const generated_condition_expression = try expr_generated_validate.generatedPredicateExpressionAtStart(tokens, pos.*, generated_expression_ast);
                if (try parseFilterBooleanIsNotGroupsAlloc(
                    alloc,
                    tokens,
                    pos,
                    schema,
                    field_expression_qualifiers,
                    returning_expression_qualifiers,
                    defer_row_expression_field_validation,
                    &any_groups,
                    generated_condition_expression,
                )) {
                    // Expanded as native expression OR groups.
                } else {
                    try parseFilterAtomAlloc(alloc, tokens, pos, params, schema, field_expression_qualifiers, returning_expression_qualifiers, defer_row_expression_field_validation, &predicates, &array_any, &array_contains, &array_eq, &in_predicates, &json_contains, &json_path_eq, &json_path_exists, &text_patterns, realtime_ns, generated_condition_expression);
                }
            }
            if (!parser.matchKeyword(tokens, pos, "and")) break;
        }
    }
    if (!saw_boolean_constant and predicates.items.len == 0 and array_any.items.len == 0 and array_contains.items.len == 0 and array_eq.items.len == 0 and in_predicates.items.len == 0 and json_contains.items.len == 0 and json_path_eq.items.len == 0 and json_path_exists.items.len == 0 and text_patterns.items.len == 0 and expressions.items.len == 0 and expression_array_contains.items.len == 0 and any_groups.items.len == 0 and not_groups.items.len == 0) return error.UnsupportedSqlShape;
    try parser.expectToken(tokens, pos, .rparen);
    return .{
        .predicates = try predicates.toOwnedSlice(alloc),
        .array_any = try array_any.toOwnedSlice(alloc),
        .array_contains = try array_contains.toOwnedSlice(alloc),
        .array_eq = try array_eq.toOwnedSlice(alloc),
        .in_predicates = try in_predicates.toOwnedSlice(alloc),
        .json_contains = try json_contains.toOwnedSlice(alloc),
        .json_path_eq = try json_path_eq.toOwnedSlice(alloc),
        .json_path_exists = try json_path_exists.toOwnedSlice(alloc),
        .text_patterns = try text_patterns.toOwnedSlice(alloc),
        .expressions = try expressions.toOwnedSlice(alloc),
        .expression_array_contains = try expression_array_contains.toOwnedSlice(alloc),
        .any_groups = try any_groups.toOwnedSlice(alloc),
        .not_groups = try not_groups.toOwnedSlice(alloc),
    };
}

pub fn canParseBareBooleanHavingExpression(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: usize,
    schema: runtime_schema.TableSchema,
    type_context: expr_type.RowExpressionTypeContext,
    group_fields: []const []const u8,
    group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    aggregations: []const db_mod.types.RelationalRowsAggregateSpec,
) !bool {
    const output_columns = try outputColumnsAlloc(alloc, schema, type_context, group_fields, group_expressions, aggregations);
    defer ddl_plan.freeDdlRelationalColumns(alloc, output_columns);
    const aggregate_schema: runtime_schema.TableSchema = .{
        .storage_mode = .relational,
        .relational_columns = output_columns,
    };
    return expr_predicate.canParseBareBooleanWhereExpression(tokens, pos, aggregate_schema);
}

pub fn parseOutputFieldExpressionConditionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    type_context: expr_type.RowExpressionTypeContext,
    group_fields: []const []const u8,
    group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    aggregations: []const db_mod.types.RelationalRowsAggregateSpec,
    options: OutputFieldExpressionConditionParserOptions,
) !db_mod.types.RelationalRowsExpressionCondition {
    const field = try parseOutputFieldAlloc(alloc, tokens, pos, group_fields, group_expressions, aggregations, options.output_field_options);
    var field_transferred = false;
    errdefer if (!field_transferred) alloc.free(field);
    if (!outputFieldIsUnique(group_fields, group_expressions, aggregations, field)) return error.UnsupportedSqlShape;
    const column = try outputColumnForFieldAlloc(alloc, schema, type_context, group_fields, group_expressions, aggregations, field);
    defer ddl_plan.freeDdlRelationalColumn(alloc, column);

    const op_token_index = pos.*;
    const op: runtime_schema.RelationalCheckOp = if (try expr_operator.parseExpressionIsTailIf(tokens, pos, .{
        .allow_boolean_unknown = true,
        .allow_boolean_literal = true,
    })) |is_tail| blk: {
        switch (is_tail.kind) {
            .distinct_comparison, .null_test => try expr_generated_validate.validateGeneratedIsTailPredicateExpression(options.generated_expression_ast, tokens, op_token_index, is_tail),
            .boolean_unknown => {
                try expr_generated_validate.validateGeneratedIsTailPredicateExpression(options.generated_expression_ast, tokens, op_token_index, is_tail);
                if (column.field_type != .boolean) return error.InvalidSqlCatalog;
                break :blk is_tail.op;
            },
            .boolean_literal => {
                try expr_generated_validate.validateGeneratedIsTailPredicateExpression(options.generated_expression_ast, tokens, op_token_index, is_tail);
                if (column.field_type != .boolean) return error.InvalidSqlCatalog;
                const value_json = try alloc.dupe(u8, value_mod.booleanJson(is_tail.boolean_value));
                errdefer alloc.free(value_json);
                const lhs_field = field;
                field_transferred = true;
                const rhs = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
                var rhs_transferred = false;
                errdefer if (!rhs_transferred) alloc.free(rhs);
                rhs[0] = .{
                    .kind = .value,
                    .value_json = value_json,
                };
                rhs_transferred = true;
                return .{
                    .lhs = .{
                        .kind = .field,
                        .field = lhs_field,
                    },
                    .op = .eq,
                    .rhs = rhs,
                };
            },
        }
        break :blk is_tail.op;
    } else try expr_operator.parseComparisonOp(tokens, pos);
    try expr_generated_validate.validateGeneratedRelationalPredicateExpression(options.generated_expression_ast, tokens, op_token_index, op);

    const lhs: db_mod.types.RelationalRowsExpression = .{
        .kind = .field,
        .field = field,
    };
    field_transferred = true;

    const rhs = switch (op) {
        .is_null, .is_not_null => &.{},
        else => blk: {
            const value_json = try value_mod.parseJsonValueAlloc(alloc, tokens, pos, options.params);
            var value_transferred = false;
            errdefer if (!value_transferred) alloc.free(value_json);
            const out = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
            var out_transferred = false;
            errdefer if (!out_transferred) alloc.free(out);
            out[0] = .{
                .kind = .value,
                .value_json = value_json,
            };
            value_transferred = true;
            out_transferred = true;
            break :blk out;
        },
    };

    return .{
        .lhs = lhs,
        .op = op,
        .rhs = rhs,
    };
}

pub fn parseOutputExpressionConditionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    type_context: expr_type.RowExpressionTypeContext,
    group_fields: []const []const u8,
    group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    aggregations: []const db_mod.types.RelationalRowsAggregateSpec,
    options: OutputExpressionConditionParserOptions,
) !db_mod.types.RelationalRowsExpressionCondition {
    const output_columns = try outputColumnsAlloc(alloc, schema, type_context, group_fields, group_expressions, aggregations);
    defer ddl_plan.freeDdlRelationalColumns(alloc, output_columns);
    const aggregate_schema: runtime_schema.TableSchema = .{
        .storage_mode = .relational,
        .relational_columns = output_columns,
    };
    var operator_token_index: usize = 0;
    const condition = try expr_where_condition.parseCaseExpressionConditionWithSelectSchemaAndOperatorAlloc(
        alloc,
        tokens,
        pos,
        aggregate_schema,
        options.context_hooks,
        options.case_expression_hooks,
        &operator_token_index,
    );
    var condition_transferred = false;
    errdefer if (!condition_transferred) freeExpressionCondition(alloc, condition);
    try expr_generated_validate.validateGeneratedRelationalPredicateExpression(options.generated_expression_ast, tokens, operator_token_index, condition.op);
    condition_transferred = true;
    return condition;
}

pub fn parseBareBooleanHavingExpression(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    type_context: expr_type.RowExpressionTypeContext,
    expressions: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionCondition),
    group_fields: []const []const u8,
    group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    aggregations: []const db_mod.types.RelationalRowsAggregateSpec,
    options: BareBooleanHavingExpressionParserOptions,
) !void {
    const output_columns = try outputColumnsAlloc(alloc, schema, type_context, group_fields, group_expressions, aggregations);
    defer ddl_plan.freeDdlRelationalColumns(alloc, output_columns);
    const aggregate_schema: runtime_schema.TableSchema = .{
        .storage_mode = .relational,
        .relational_columns = output_columns,
    };
    try expr_where_condition.parseBareBooleanWhereExpressionWithSelectSchemaAlloc(
        alloc,
        tokens,
        pos,
        aggregate_schema,
        expressions,
        options.context_hooks,
        options.bare_boolean_hooks,
    );
}

pub fn parseHavingBooleanIsNotGroups(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    type_context: expr_type.RowExpressionTypeContext,
    groups: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    group_fields: []const []const u8,
    group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    aggregations: []const db_mod.types.RelationalRowsAggregateSpec,
    options: OutputFieldExpressionConditionParserOptions,
) !bool {
    const saved_pos = pos.*;
    const field = parseOutputFieldAlloc(alloc, tokens, pos, group_fields, group_expressions, aggregations, options.output_field_options) catch |err| {
        pos.* = saved_pos;
        return switch (err) {
            error.UnsupportedSqlShape => false,
            else => err,
        };
    };
    defer alloc.free(field);
    const operator_token_index = pos.*;
    if (!parser.matchKeywordTag(tokens, pos, .is)) {
        pos.* = saved_pos;
        return false;
    }
    if (!parser.matchKeywordTag(tokens, pos, .not)) {
        pos.* = saved_pos;
        return false;
    }
    if (!(parser.peekKeywordTag(tokens, pos.*, .true) or parser.peekKeywordTag(tokens, pos.*, .false))) {
        pos.* = saved_pos;
        return false;
    }

    const column = try outputColumnForFieldAlloc(alloc, schema, type_context, group_fields, group_expressions, aggregations, field);
    defer ddl_plan.freeDdlRelationalColumn(alloc, column);
    const value = (try value_mod.parseSqlBooleanIsValue(tokens, pos, column)) orelse return error.UnsupportedSqlShape;
    const expected_kind: generated_parser.GeneratedSqlExpressionKind = if (value) .is_not_true else .is_not_false;
    try expr_generated_validate.validateGeneratedBooleanIsPredicateExpression(options.generated_expression_ast, tokens, operator_token_index, expected_kind);
    try expr_condition.appendBooleanIsNotExpressionGroups(alloc, groups, field, value);
    return true;
}

pub fn parseHavingConditionAlternativesAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    type_context: expr_type.RowExpressionTypeContext,
    alternatives: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    group_fields: []const []const u8,
    group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    aggregations: []const db_mod.types.RelationalRowsAggregateSpec,
    field_options: OutputFieldExpressionConditionParserOptions,
    expression_options: OutputExpressionConditionParserOptions,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
) !void {
    if (value_mod.matchStandaloneSqlBooleanLiteral(tokens, pos)) |enabled| {
        try expr_condition.appendBooleanConstantExpressionGroup(alloc, alternatives, enabled);
        return;
    }
    const generated_condition_expression = try expr_generated_validate.generatedPredicateExpressionAtStart(tokens, pos.*, generated_expression_ast);
    var field_options_with_generated = field_options;
    field_options_with_generated.generated_expression_ast = generated_condition_expression;
    var expression_options_with_generated = expression_options;
    expression_options_with_generated.generated_expression_ast = generated_condition_expression;

    if (try parseHavingBooleanIsNotGroups(alloc, tokens, pos, schema, type_context, alternatives, group_fields, group_expressions, aggregations, field_options_with_generated)) {
        return;
    }

    const condition = if (peekHavingExpression(tokens, pos.*))
        try parseOutputExpressionConditionAlloc(alloc, tokens, pos, schema, type_context, group_fields, group_expressions, aggregations, expression_options_with_generated)
    else
        try parseOutputFieldExpressionConditionAlloc(alloc, tokens, pos, schema, type_context, group_fields, group_expressions, aggregations, field_options_with_generated);
    var condition_transferred = false;
    errdefer if (!condition_transferred) freeExpressionCondition(alloc, condition);

    const conditions = try alloc.alloc(db_mod.types.RelationalRowsExpressionCondition, 1);
    var conditions_transferred = false;
    errdefer if (!conditions_transferred) alloc.free(conditions);
    conditions[0] = condition;
    condition_transferred = true;
    try alternatives.append(alloc, .{ .conditions = conditions });
    conditions_transferred = true;
}

pub fn parseHavingNotGroupAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    type_context: expr_type.RowExpressionTypeContext,
    not_groups: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    group_fields: []const []const u8,
    group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    aggregations: []const db_mod.types.RelationalRowsAggregateSpec,
    field_options: OutputFieldExpressionConditionParserOptions,
    expression_options: OutputExpressionConditionParserOptions,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
) !void {
    try parser.expectKeyword(tokens, pos, "not");
    if (generated_expression_ast) |expression| {
        if (expression.kind != .logical_not) return error.UnsupportedSqlShape;
    }
    try parser.expectToken(tokens, pos, .lparen);
    var groups = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup).empty;
    var groups_transferred = false;
    defer groups.deinit(alloc);
    errdefer if (!groups_transferred) freeExpressionPredicateGroups(alloc, groups.items);
    try groups.append(alloc, .{ .conditions = &.{} });

    while (true) {
        var alternatives = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup).empty;
        defer alternatives.deinit(alloc);
        errdefer freeExpressionPredicateGroups(alloc, alternatives.items);
        try parseHavingConditionAlternativesAlloc(
            alloc,
            tokens,
            pos,
            schema,
            type_context,
            &alternatives,
            group_fields,
            group_expressions,
            aggregations,
            field_options,
            expression_options,
            generated_expression_ast,
        );
        try expr_condition.andExpressionPredicateAlternatives(alloc, &groups, alternatives.items);
        freeExpressionPredicateGroups(alloc, alternatives.items);
        if (!parser.matchKeyword(tokens, pos, "and")) break;
    }
    if (groups.items.len == 0) return error.UnsupportedSqlShape;
    try parser.expectToken(tokens, pos, .rparen);

    try not_groups.appendSlice(alloc, groups.items);
    groups_transferred = true;
}

pub fn parseHavingOrGroupsAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    schema: runtime_schema.TableSchema,
    type_context: expr_type.RowExpressionTypeContext,
    any_groups: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    group_fields: []const []const u8,
    group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    aggregations: []const db_mod.types.RelationalRowsAggregateSpec,
    field_options: OutputFieldExpressionConditionParserOptions,
    expression_options: OutputExpressionConditionParserOptions,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
) !void {
    while (true) {
        const parenthesized = matchBooleanGroupOpen(tokens, pos);
        var groups = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup).empty;
        var groups_transferred = false;
        defer groups.deinit(alloc);
        errdefer if (!groups_transferred) freeExpressionPredicateGroups(alloc, groups.items);
        try groups.append(alloc, .{ .conditions = &.{} });

        while (true) {
            var alternatives = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup).empty;
            defer alternatives.deinit(alloc);
            errdefer freeExpressionPredicateGroups(alloc, alternatives.items);
            try parseHavingConditionAlternativesAlloc(
                alloc,
                tokens,
                pos,
                schema,
                type_context,
                &alternatives,
                group_fields,
                group_expressions,
                aggregations,
                field_options,
                expression_options,
                generated_expression_ast,
            );
            try expr_condition.andExpressionPredicateAlternatives(alloc, &groups, alternatives.items);
            freeExpressionPredicateGroups(alloc, alternatives.items);
            if (!parser.matchKeyword(tokens, pos, "and")) break;
        }
        if (groups.items.len == 0) return error.UnsupportedSqlShape;
        if (parenthesized) try parser.expectToken(tokens, pos, .rparen);
        try any_groups.appendSlice(alloc, groups.items);
        groups_transferred = true;

        if (!parser.matchKeyword(tokens, pos, "or")) break;
    }
}

pub fn parseHavingAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
    schema: runtime_schema.TableSchema,
    type_context: expr_type.RowExpressionTypeContext,
    predicates: *std.ArrayListUnmanaged(runtime_schema.RelationalCheck),
    expressions: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionCondition),
    any_groups: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    not_groups: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup),
    group_fields: []const []const u8,
    group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    aggregations: []const db_mod.types.RelationalRowsAggregateSpec,
    output_field_options: OutputFieldParserOptions,
    field_condition_options: OutputFieldExpressionConditionParserOptions,
    expression_condition_options: OutputExpressionConditionParserOptions,
    bare_boolean_options: BareBooleanHavingExpressionParserOptions,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
) !void {
    if (try canParseBareBooleanHavingExpression(alloc, tokens, pos.*, schema, type_context, group_fields, group_expressions, aggregations)) {
        var generated_bare_boolean_options = bare_boolean_options;
        generated_bare_boolean_options.generated_expression_ast = generated_expression_ast;
        generated_bare_boolean_options.bare_boolean_hooks.generated_expression_ast = generated_expression_ast;
        try parseBareBooleanHavingExpression(alloc, tokens, pos, schema, type_context, expressions, group_fields, group_expressions, aggregations, generated_bare_boolean_options);
        return;
    }
    if (parser.hasTopLevelOrBeforeTailToken(tokens, pos.*, expr_token.sqlWhereTailClauseKeywordToken) or havingHasBooleanIsNot(tokens, pos.*)) {
        try parseHavingOrGroupsAlloc(alloc, tokens, pos, schema, type_context, any_groups, group_fields, group_expressions, aggregations, field_condition_options, expression_condition_options, generated_expression_ast);
        return;
    }
    while (true) {
        if (canParseHavingNot(tokens, pos.*)) {
            const generated_condition_expression = try expr_generated_validate.generatedPredicateExpressionAtStart(tokens, pos.*, generated_expression_ast);
            try parseHavingNotGroupAlloc(alloc, tokens, pos, schema, type_context, not_groups, group_fields, group_expressions, aggregations, field_condition_options, expression_condition_options, generated_condition_expression);
        } else if (value_mod.matchStandaloneSqlBooleanLiteral(tokens, pos)) |enabled| {
            if (!enabled) try expr_condition.appendBooleanConstantExpressionCondition(alloc, expressions, false);
        } else if (peekHavingExpression(tokens, pos.*)) {
            var expression_options_with_generated = expression_condition_options;
            expression_options_with_generated.generated_expression_ast = try expr_generated_validate.generatedPredicateExpressionAtStart(tokens, pos.*, generated_expression_ast);
            const condition = try parseOutputExpressionConditionAlloc(alloc, tokens, pos, schema, type_context, group_fields, group_expressions, aggregations, expression_options_with_generated);
            var condition_transferred = false;
            errdefer if (!condition_transferred) freeExpressionCondition(alloc, condition);
            try expressions.append(alloc, condition);
            condition_transferred = true;
        } else {
            const generated_condition_expression = try expr_generated_validate.generatedPredicateExpressionAtStart(tokens, pos.*, generated_expression_ast);
            const field = try parseOutputFieldAlloc(alloc, tokens, pos, group_fields, group_expressions, aggregations, output_field_options);
            var field_transferred = false;
            errdefer if (!field_transferred) alloc.free(field);
            if (!outputFieldIsUnique(group_fields, group_expressions, aggregations, field)) return error.UnsupportedSqlShape;
            const column = try outputColumnForFieldAlloc(alloc, schema, type_context, group_fields, group_expressions, aggregations, field);
            defer ddl_plan.freeDdlRelationalColumn(alloc, column);
            const op_token_index = pos.*;
            const op: runtime_schema.RelationalCheckOp = if (try expr_operator.parseExpressionIsTailIf(tokens, pos, .{
                .allow_boolean_unknown = true,
                .allow_boolean_literal = true,
            })) |is_tail| blk: {
                switch (is_tail.kind) {
                    .distinct_comparison, .null_test => try expr_generated_validate.validateGeneratedIsTailPredicateExpression(generated_condition_expression, tokens, op_token_index, is_tail),
                    .boolean_unknown => {
                        try expr_generated_validate.validateGeneratedIsTailPredicateExpression(generated_condition_expression, tokens, op_token_index, is_tail);
                        if (column.field_type != .boolean) return error.InvalidSqlCatalog;
                        break :blk is_tail.op;
                    },
                    .boolean_literal => {
                        try expr_generated_validate.validateGeneratedIsTailPredicateExpression(generated_condition_expression, tokens, op_token_index, is_tail);
                        if (column.field_type != .boolean) return error.InvalidSqlCatalog;
                        const value_json = try alloc.dupe(u8, value_mod.booleanJson(is_tail.boolean_value));
                        var value_transferred = false;
                        errdefer if (!value_transferred) alloc.free(value_json);
                        try predicates.append(alloc, .{
                            .name = "",
                            .field = field,
                            .op = .eq,
                            .value_json = value_json,
                        });
                        field_transferred = true;
                        value_transferred = true;
                        if (!parser.matchKeyword(tokens, pos, "and")) break;
                        continue;
                    },
                }
                break :blk is_tail.op;
            } else try expr_operator.parseComparisonOp(tokens, pos);
            try expr_generated_validate.validateGeneratedRelationalPredicateExpression(generated_condition_expression, tokens, op_token_index, op);
            const value_json = switch (op) {
                .is_null, .is_not_null => null,
                else => try value_mod.parseJsonValueAlloc(alloc, tokens, pos, params),
            };
            var value_transferred = false;
            errdefer if (!value_transferred) if (value_json) |json| alloc.free(json);
            try predicates.append(alloc, .{
                .name = "",
                .field = field,
                .op = op,
                .value_json = value_json,
            });
            field_transferred = true;
            value_transferred = true;
        }
        if (!parser.matchKeyword(tokens, pos, "and")) break;
    }
}

pub fn peekExpressionInput(tokens: []const Token, pos: usize) bool {
    if (pos >= tokens.len) return false;
    if (tokens[pos].kind == .lparen or tokens[pos].kind == .minus) return true;
    if (expr_operator.jsonExtractExpressionCanStartAt(tokens, pos)) return true;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsJsonExtractPathFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsJsonTypeofFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsJsonArrayLengthFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsJsonBuildObjectFunction) or
        value_mod.peekConvertFromFunctionCall(tokens, pos) or
        expr_token.functionCallStartsAtTokenIf(tokens, pos, expr_token.sqlTokenIsArrayLengthFunction) or
        expr_token.functionCallStartsAtTokenIf(tokens, pos, expr_token.sqlTokenIsArrayPositionFunction) or
        parser.peekKeyword(tokens, pos, "case") or
        parser.peekKeyword(tokens, pos, "cast") or
        expr_token.peekCoalesceFunctionCall(tokens, pos) or
        expr_token.peekArrayElementTransformFunctionCall(tokens, pos) or
        expr_token.peekArrayToStringFunctionCall(tokens, pos) or
        expr_token.peekStringToArrayFunctionCall(tokens, pos) or
        expr_token.peekCaseFoldFunctionCall(tokens, pos) or
        expr_token.peekConcatFunctionCall(tokens, pos) or
        expr_token.peekReplaceFunctionCall(tokens, pos) or
        expr_token.peekRegexpReplaceFunctionCall(tokens, pos) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsRegexpSubstrFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsRegexpMatchFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsRegexpCountFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsRegexpInstrFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsTranslateFunction) or
        expr_token.peekNullifFunctionCall(tokens, pos) or
        expr_token.peekTextLengthFunctionKeyword(tokens, pos) or
        expr_token.peekFixedUnaryFunctionCall(tokens, pos, .ascii) or
        expr_token.peekFixedUnaryFunctionCall(tokens, pos, .chr) or
        expr_token.peekSubstringFunctionKeyword(tokens, pos) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsOverlayFunction) or
        expr_token.peekSplitPartFunctionKeyword(tokens, pos) or
        expr_token.peekStrposFunctionKeyword(tokens, pos) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsLeftRightFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsPadFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsRepeatFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsReverseFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsStartsWithFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsEndsWithFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsDateTruncFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsDateBinFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsDatePartFunction) or
        parser.peekKeyword(tokens, pos, "position") or
        expr_token.peekFixedUnaryFunctionCall(tokens, pos, .abs) or
        expr_token.peekFixedUnaryFunctionCall(tokens, pos, .round) or
        expr_token.peekFixedUnaryFunctionCall(tokens, pos, .trunc) or
        expr_token.peekFixedUnaryFunctionCall(tokens, pos, .floor) or
        expr_token.peekFixedUnaryFunctionCall(tokens, pos, .ceil) or
        expr_token.peekFixedUnaryFunctionCall(tokens, pos, .sqrt) or
        expr_token.peekFixedUnaryFunctionCall(tokens, pos, .sign) or
        expr_token.peekFixedBinaryFunctionCall(tokens, pos, .mod) or
        expr_token.peekFixedBinaryFunctionCall(tokens, pos, .power) or
        expr_token.peekGreatestLeastFunctionCall(tokens, pos))
    {
        return true;
    }
    if (tokens[pos].kind == .identifier and pos + 1 < tokens.len) {
        return switch (tokens[pos + 1].kind) {
            .plus, .minus, .star, .slash, .percent => true,
            else => false,
        };
    }
    return false;
}

pub fn peekParenthesizedExpressionCondition(tokens: []const Token, pos: usize) bool {
    if (!parser.peekKind(tokens, pos, .lparen)) return false;
    var depth: usize = 0;
    var i = pos;
    while (i < tokens.len) : (i += 1) {
        const token = tokens[i];
        switch (token.kind) {
            .lparen => depth += 1,
            .rparen => {
                if (depth == 0) return false;
                depth -= 1;
                if (depth == 0) {
                    if (i + 1 >= tokens.len) return false;
                    const next = tokens[i + 1];
                    return switch (next.kind) {
                        .eq, .neq, .gt, .gte, .lt, .lte => true,
                        .identifier => next.matchesKeywordTag(.is),
                        else => false,
                    };
                }
            },
            else => {},
        }
    }
    return false;
}

pub fn peekStandaloneGroupIdentifier(tokens: []const Token, pos: usize) bool {
    if (!parser.peekKind(tokens, pos, .identifier)) return false;
    if (pos + 1 >= tokens.len) return true;
    return switch (tokens[pos + 1].kind) {
        .lparen, .arrow_json, .arrow_text, .path_arrow_json, .path_arrow_text, .plus, .minus, .star, .slash, .percent, .pipe_concat => false,
        else => true,
    };
}

pub fn appendGroupByFieldOrAlias(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    group_by: *std.ArrayListUnmanaged([]const u8),
    group_expressions: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionProjection),
    select: plan_mod.AggregateSelectList,
    raw_field: []const u8,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
) !void {
    var raw_owned = true;
    defer if (raw_owned) alloc.free(raw_field);

    const normalized_field: ?[]const u8 = binder.normalizeRowExpressionFieldAlloc(alloc, schema, raw_field, field_expression_qualifiers, returning_expression_qualifiers, defer_row_expression_field_validation) catch |err| switch (err) {
        error.InvalidSqlCatalog => null,
        else => return err,
    };
    if (normalized_field) |field| {
        var field_owned = true;
        errdefer if (field_owned) alloc.free(field);
        if (binder.relationalColumnForField(schema, field, null) != null) {
            try group_by.append(alloc, field);
            field_owned = false;
            raw_owned = false;
            alloc.free(raw_field);
            return;
        }
        alloc.free(field);
        field_owned = false;
    }

    for (select.group_expressions) |projection| {
        if (std.mem.eql(u8, projection.output, raw_field)) {
            try group_expressions.append(alloc, try plan_mod.cloneExpressionProjection(alloc, projection));
            return;
        }
    }
    return error.InvalidSqlCatalog;
}

pub fn appendGroupByOrdinal(
    alloc: std.mem.Allocator,
    group_by: *std.ArrayListUnmanaged([]const u8),
    group_expressions: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionProjection),
    select: plan_mod.AggregateSelectList,
    ordinal: u32,
) !void {
    if (ordinal == 0) return error.UnsupportedSqlShape;
    const index: usize = @intCast(ordinal - 1);
    if (index >= select.outputs.len) return error.UnsupportedSqlShape;
    const output = select.outputs[index];
    switch (output.kind) {
        .group_field => try group_by.append(alloc, try alloc.dupe(u8, select.group_fields[output.index])),
        .group_expression => try group_expressions.append(alloc, try plan_mod.cloneExpressionProjection(alloc, select.group_expressions[output.index])),
        .aggregation => return error.UnsupportedSqlShape,
    }
}

pub fn peekExpressionFilter(tokens: []const Token, pos: usize) bool {
    if (pos >= tokens.len) return false;
    if (peekParenthesizedExpressionCondition(tokens, pos)) return true;
    if (tokens[pos].kind == .minus) return true;
    if (expr_operator.jsonExtractExpressionCanStartAt(tokens, pos)) return true;
    if (expr_operator.jsonKeySetExpressionCanStartAt(tokens, pos)) return true;
    if (expr_parse.rowExpressionHasTopLevelPipeConcat(tokens, pos)) return true;
    if (parser.peekKeyword(tokens, pos, "lower") or
        parser.peekKeyword(tokens, pos, "upper") or
        parser.peekKeyword(tokens, pos, "trim") or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsTrimVariantFunction) or
        parser.peekKeyword(tokens, pos, "replace") or
        parser.peekKeyword(tokens, pos, "regexp_replace") or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsRegexpSubstrFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsRegexpMatchFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsRegexpCountFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsRegexpInstrFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsTranslateFunction) or
        parser.peekKeyword(tokens, pos, "case") or
        parser.peekKeyword(tokens, pos, "cast") or
        parser.peekKeyword(tokens, pos, "coalesce") or
        parser.peekKeyword(tokens, pos, "nullif") or
        expr_token.peekTextLengthFunctionKeyword(tokens, pos) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsAsciiFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsChrFunction) or
        expr_token.peekSubstringFunctionKeyword(tokens, pos) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsOverlayFunction) or
        expr_token.peekSplitPartFunctionKeyword(tokens, pos) or
        expr_token.peekStrposFunctionKeyword(tokens, pos) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsLeftRightFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsPadFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsRepeatFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsReverseFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsStartsWithFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsEndsWithFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsDateTruncFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsDateBinFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsDatePartFunction) or
        parser.peekKeyword(tokens, pos, "position") or
        parser.peekKeyword(tokens, pos, "abs") or
        parser.peekKeyword(tokens, pos, "round") or
        expr_token.peekFunctionCallTag(tokens, pos, .trunc) or
        expr_token.peekFunctionCallTag(tokens, pos, .floor) or
        expr_token.peekFunctionCallTag(tokens, pos, .ceil) or
        expr_token.peekFunctionCallTag(tokens, pos, .sqrt) or
        expr_token.peekFunctionCallTag(tokens, pos, .sign) or
        expr_token.peekFunctionCallTag(tokens, pos, .mod) or
        expr_token.peekFunctionCallTag(tokens, pos, .power) or
        parser.peekKeyword(tokens, pos, "greatest") or
        parser.peekKeyword(tokens, pos, "least") or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsJsonExtractPathFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsJsonTypeofFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsJsonArrayLengthFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsJsonBuildObjectFunction) or
        expr_token.functionCallStartsAtTokenIf(tokens, pos, expr_token.sqlTokenIsArrayLengthFunction) or
        expr_token.functionCallStartsAtTokenIf(tokens, pos, expr_token.sqlTokenIsArrayPositionFunction) or
        parser.peekKeywordTag(tokens, pos, .now) or
        parser.peekKeywordTag(tokens, pos, .current_timestamp) or
        parser.peekKeywordTag(tokens, pos, .current_date))
    {
        return true;
    }
    if (tokens[pos].kind == .identifier and pos + 1 < tokens.len) {
        return switch (tokens[pos + 1].kind) {
            .plus, .minus, .star, .slash, .percent, .arrow_json, .arrow_text, .path_arrow_json, .path_arrow_text, .pipe_concat => true,
            else => false,
        };
    }
    return false;
}

pub fn peekOutputOrderExpression(tokens: []const Token, pos: usize) bool {
    if (pos >= tokens.len) return false;
    if (tokens[pos].kind == .lparen or tokens[pos].kind == .minus) return true;
    if (expr_parse.rowExpressionHasTopLevelPipeConcat(tokens, pos)) return true;
    if (parser.peekKeyword(tokens, pos, "lower") or
        parser.peekKeyword(tokens, pos, "upper") or
        parser.peekKeyword(tokens, pos, "trim") or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsTrimVariantFunction) or
        parser.peekKeyword(tokens, pos, "replace") or
        parser.peekKeyword(tokens, pos, "regexp_replace") or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsRegexpSubstrFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsRegexpMatchFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsRegexpCountFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsRegexpInstrFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsTranslateFunction) or
        parser.peekKeyword(tokens, pos, "concat") or
        parser.peekKeyword(tokens, pos, "concat_ws") or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsJsonExtractPathFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsJsonTypeofFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsJsonArrayLengthFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsJsonBuildObjectFunction) or
        expr_token.functionCallStartsAtTokenIf(tokens, pos, expr_token.sqlTokenIsArrayLengthFunction) or
        expr_token.functionCallStartsAtTokenIf(tokens, pos, expr_token.sqlTokenIsArrayPositionFunction) or
        parser.peekKeyword(tokens, pos, "array_append") or
        parser.peekKeyword(tokens, pos, "array_prepend") or
        parser.peekKeyword(tokens, pos, "array_cat") or
        parser.peekKeyword(tokens, pos, "array_remove") or
        parser.peekKeyword(tokens, pos, "array_replace") or
        parser.peekKeyword(tokens, pos, "array_to_string") or
        parser.peekKeyword(tokens, pos, "string_to_array") or
        parser.peekKeyword(tokens, pos, "case") or
        parser.peekKeyword(tokens, pos, "cast") or
        parser.peekKeyword(tokens, pos, "coalesce") or
        parser.peekKeyword(tokens, pos, "nullif") or
        expr_token.peekTextLengthFunctionKeyword(tokens, pos) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsAsciiFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsChrFunction) or
        expr_token.peekSubstringFunctionKeyword(tokens, pos) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsOverlayFunction) or
        expr_token.peekSplitPartFunctionKeyword(tokens, pos) or
        expr_token.peekStrposFunctionKeyword(tokens, pos) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsLeftRightFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsPadFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsRepeatFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsReverseFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsMd5Function) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsStartsWithFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsEndsWithFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsDateTruncFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsDateBinFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsDatePartFunction) or
        parser.peekKeyword(tokens, pos, "position") or
        parser.peekKeyword(tokens, pos, "abs") or
        parser.peekKeyword(tokens, pos, "round") or
        expr_token.peekFunctionCallTag(tokens, pos, .trunc) or
        expr_token.peekFunctionCallTag(tokens, pos, .floor) or
        expr_token.peekFunctionCallTag(tokens, pos, .ceil) or
        expr_token.peekFunctionCallTag(tokens, pos, .sqrt) or
        expr_token.peekFunctionCallTag(tokens, pos, .sign) or
        expr_token.peekFunctionCallTag(tokens, pos, .mod) or
        expr_token.peekFunctionCallTag(tokens, pos, .power) or
        parser.peekKeyword(tokens, pos, "greatest") or
        parser.peekKeyword(tokens, pos, "least"))
    {
        return true;
    }
    if (tokens[pos].kind == .identifier and pos + 1 < tokens.len) {
        return switch (tokens[pos + 1].kind) {
            .plus, .minus, .star, .slash, .percent, .pipe_concat, .arrow_json, .arrow_text, .path_arrow_json, .path_arrow_text => true,
            else => false,
        };
    }
    return false;
}

pub fn peekHavingExpression(tokens: []const Token, pos: usize) bool {
    if (nextIsFunction(tokens, pos)) return false;
    if (pos >= tokens.len) return false;
    const token = tokens[pos];
    if (peekParenthesizedExpressionCondition(tokens, pos)) return true;
    if (token.kind == .identifier) {
        if (pos + 1 >= tokens.len) return false;
        const next = tokens[pos + 1];
        if (next.kind == .eq or next.kind == .neq or next.kind == .gt or next.kind == .gte or next.kind == .lt or next.kind == .lte) return false;
        if (next.matchesKeywordTag(.is)) return false;
        return true;
    }
    return token.kind == .minus or
        token.kind == .number or
        parser.peekKeyword(tokens, pos, "lower") or
        parser.peekKeyword(tokens, pos, "upper") or
        parser.peekKeyword(tokens, pos, "trim") or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsTrimVariantFunction) or
        parser.peekKeyword(tokens, pos, "replace") or
        parser.peekKeyword(tokens, pos, "regexp_replace") or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsRegexpSubstrFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsRegexpMatchFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsRegexpCountFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsRegexpInstrFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsTranslateFunction) or
        parser.peekKeyword(tokens, pos, "case") or
        parser.peekKeyword(tokens, pos, "cast") or
        parser.peekKeyword(tokens, pos, "coalesce") or
        parser.peekKeyword(tokens, pos, "nullif") or
        expr_token.peekTextLengthFunctionKeyword(tokens, pos) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsAsciiFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsChrFunction) or
        expr_token.peekSubstringFunctionKeyword(tokens, pos) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsOverlayFunction) or
        expr_token.peekSplitPartFunctionKeyword(tokens, pos) or
        expr_token.peekStrposFunctionKeyword(tokens, pos) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsLeftRightFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsPadFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsRepeatFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsReverseFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsMd5Function) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsStartsWithFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsEndsWithFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsDateTruncFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsDateBinFunction) or
        expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsDatePartFunction) or
        parser.peekKeyword(tokens, pos, "position") or
        parser.peekKeyword(tokens, pos, "abs") or
        parser.peekKeyword(tokens, pos, "round") or
        expr_token.peekFunctionCallTag(tokens, pos, .trunc) or
        expr_token.peekFunctionCallTag(tokens, pos, .floor) or
        expr_token.peekFunctionCallTag(tokens, pos, .ceil) or
        expr_token.peekFunctionCallTag(tokens, pos, .sqrt) or
        expr_token.peekFunctionCallTag(tokens, pos, .sign) or
        expr_token.peekFunctionCallTag(tokens, pos, .mod) or
        expr_token.peekFunctionCallTag(tokens, pos, .power) or
        parser.peekKeyword(tokens, pos, "greatest") or
        parser.peekKeyword(tokens, pos, "least");
}

pub fn validatePercentile(percentile: f64) !void {
    if (!std.math.isFinite(percentile) or percentile < 0 or percentile > 1) return error.UnsupportedSqlShape;
}

pub fn aliasOrDefaultAlloc(
    alloc: std.mem.Allocator,
    explicit_alias: ?[]const u8,
    op: db_mod.types.RelationalRowsAggregateOp,
    field: ?[]const u8,
) ![]const u8 {
    if (explicit_alias) |alias| return try alloc.dupe(u8, alias);
    if (field) |field_name| return try std.fmt.allocPrint(alloc, "{s}_{s}", .{ opName(op), field_name });
    return try alloc.dupe(u8, opName(op));
}

pub fn isPercentileOp(op: db_mod.types.RelationalRowsAggregateOp) bool {
    return op == .percentile_cont or op == .percentile_disc;
}

pub fn parseStringDelimiterAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) ![]const u8 {
    const delimiter_json = try value_mod.parseSqlUntypedValueJsonAlloc(alloc, tokens, pos);
    defer alloc.free(delimiter_json);
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, delimiter_json, .{}) catch return error.UnsupportedSqlShape;
    defer parsed.deinit();
    if (parsed.value != .string) return error.UnsupportedSqlShape;
    return try alloc.dupe(u8, parsed.value.string);
}

pub fn parsePercentileArgumentAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
) !PercentileArgument {
    const value_json = if (parser.peekKeyword(tokens, pos.*, "array"))
        try value_mod.parseSqlArrayConstructorJsonAlloc(alloc, tokens, pos, params)
    else
        try value_mod.parseJsonValueAlloc(alloc, tokens, pos, params);
    defer alloc.free(value_json);
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.UnsupportedSqlShape;
    defer parsed.deinit();
    if (parsed.value == .array) {
        if (parsed.value.array.items.len == 0) return error.UnsupportedSqlShape;
        const percentiles = try alloc.alloc(f64, parsed.value.array.items.len);
        errdefer alloc.free(percentiles);
        for (parsed.value.array.items, 0..) |item, i| {
            const percentile = sqlJsonNumberAsF64(item) orelse return error.UnsupportedSqlShape;
            try validatePercentile(percentile);
            percentiles[i] = percentile;
        }
        return .{ .percentiles = percentiles };
    }
    const percentile = sqlJsonNumberAsF64(parsed.value) orelse return error.UnsupportedSqlShape;
    try validatePercentile(percentile);
    return .{ .percentile = percentile };
}

pub fn sqlJsonNumberAsF64(value: std.json.Value) ?f64 {
    return switch (value) {
        .integer => |number| @floatFromInt(number),
        .float => |number| number,
        .number_string => |number| std.fmt.parseFloat(f64, number) catch null,
        else => null,
    };
}

pub fn outputProjectedType(
    aggregation: db_mod.types.RelationalRowsAggregateSpec,
    input_type: ?runtime_schema.AntflyType,
) !ProjectedColumnType {
    return switch (aggregation.op) {
        .array_agg => .{
            .field_type = .array,
            .array_item_type = input_type orelse return error.UnsupportedSqlShape,
        },
        .string_agg => .{ .field_type = .keyword },
        .percentile_cont, .percentile_disc => .{
            .field_type = if (aggregation.percentiles.len > 0) .array else .numeric,
            .array_item_type = if (aggregation.percentiles.len > 0) .numeric else null,
        },
        .count, .sum, .avg => .{ .field_type = .numeric },
        .min, .max, .mode => .{ .field_type = input_type orelse return error.UnsupportedSqlShape },
        .bool_or, .bool_and => .{ .field_type = .boolean },
    };
}

pub fn filterIsEmpty(filter: Filter) bool {
    return filter.predicates.len == 0 and
        filter.array_any.len == 0 and
        filter.array_contains.len == 0 and
        filter.array_eq.len == 0 and
        filter.in_predicates.len == 0 and
        filter.json_contains.len == 0 and
        filter.json_path_eq.len == 0 and
        filter.json_path_exists.len == 0 and
        filter.text_patterns.len == 0 and
        filter.expressions.len == 0 and
        filter.expression_array_contains.len == 0 and
        filter.any_groups.len == 0 and
        filter.not_groups.len == 0;
}

pub fn freeFilter(alloc: std.mem.Allocator, filter: Filter) void {
    plan_mod.freeRelationalChecks(alloc, filter.predicates);
    if (filter.predicates.len > 0) alloc.free(filter.predicates);
    plan_mod.freeArrayAny(alloc, filter.array_any);
    if (filter.array_any.len > 0) alloc.free(filter.array_any);
    plan_mod.freeArrayContains(alloc, filter.array_contains);
    if (filter.array_contains.len > 0) alloc.free(filter.array_contains);
    plan_mod.freeArrayEq(alloc, filter.array_eq);
    if (filter.array_eq.len > 0) alloc.free(filter.array_eq);
    plan_mod.freeInPredicates(alloc, filter.in_predicates);
    if (filter.in_predicates.len > 0) alloc.free(filter.in_predicates);
    plan_mod.freeJsonContains(alloc, filter.json_contains);
    if (filter.json_contains.len > 0) alloc.free(filter.json_contains);
    plan_mod.freeJsonPathEq(alloc, filter.json_path_eq);
    if (filter.json_path_eq.len > 0) alloc.free(filter.json_path_eq);
    plan_mod.freeJsonPathExists(alloc, filter.json_path_exists);
    if (filter.json_path_exists.len > 0) alloc.free(filter.json_path_exists);
    plan_mod.freeTextPatterns(alloc, filter.text_patterns);
    if (filter.text_patterns.len > 0) alloc.free(filter.text_patterns);
    plan_mod.freeExpressionConditions(alloc, filter.expressions);
    if (filter.expressions.len > 0) alloc.free(filter.expressions);
    plan_mod.freeExpressionArrayContains(alloc, filter.expression_array_contains);
    if (filter.expression_array_contains.len > 0) alloc.free(filter.expression_array_contains);
    plan_mod.freeExpressionPredicateGroups(alloc, filter.any_groups);
    if (filter.any_groups.len > 0) alloc.free(filter.any_groups);
    plan_mod.freeExpressionPredicateGroups(alloc, filter.not_groups);
    if (filter.not_groups.len > 0) alloc.free(filter.not_groups);
}

pub fn filterExpressionCount(aggregations: []const db_mod.types.RelationalRowsAggregateSpec) usize {
    var count: usize = 0;
    for (aggregations) |aggregation| {
        count += aggregation.filter_expressions.len;
    }
    return count;
}

pub fn filterExpressionArrayCount(aggregations: []const db_mod.types.RelationalRowsAggregateSpec) usize {
    var count: usize = 0;
    for (aggregations) |aggregation| {
        count += aggregation.filter_expression_array_contains.len;
    }
    return count;
}

pub fn filterJsonAccessCount(aggregations: []const db_mod.types.RelationalRowsAggregateSpec) usize {
    var count: usize = 0;
    for (aggregations) |aggregation| {
        count += aggregation.filter_json_contains.len +
            aggregation.filter_json_path_eq.len +
            aggregation.filter_json_path_exists.len;
    }
    return count;
}

pub fn filterStructuredAccessCount(aggregations: []const db_mod.types.RelationalRowsAggregateSpec) usize {
    var count: usize = 0;
    for (aggregations) |aggregation| {
        count += aggregation.filter_array_any.len +
            aggregation.filter_array_contains.len +
            aggregation.filter_array_eq.len +
            aggregation.filter_in_predicates.len +
            aggregation.filter_text_patterns.len;
    }
    return count;
}

pub fn filterGroupCount(aggregations: []const db_mod.types.RelationalRowsAggregateSpec) usize {
    var count: usize = 0;
    for (aggregations) |aggregation| {
        count += aggregation.filter_any.len + aggregation.filter_not.len;
    }
    return count;
}

pub fn inputExpressionCount(aggregations: []const db_mod.types.RelationalRowsAggregateSpec) usize {
    var count: usize = 0;
    for (aggregations) |aggregation| {
        if (aggregation.expression != null) count += 1;
    }
    return count;
}

pub fn descendingPercentileCount(aggregations: []const db_mod.types.RelationalRowsAggregateSpec) usize {
    var count: usize = 0;
    for (aggregations) |aggregation| {
        if (isPercentileOp(aggregation.op) and aggregation.percentile_order == .desc) count += 1;
    }
    return count;
}

pub fn percentileArrayCount(aggregations: []const db_mod.types.RelationalRowsAggregateSpec) usize {
    var count: usize = 0;
    for (aggregations) |aggregation| {
        if (isPercentileOp(aggregation.op) and aggregation.percentiles.len > 0) count += 1;
    }
    return count;
}

pub fn modeCount(aggregations: []const db_mod.types.RelationalRowsAggregateSpec) usize {
    var count: usize = 0;
    for (aggregations) |aggregation| {
        if (aggregation.op == .mode) count += 1;
    }
    return count;
}

pub fn validateGroupBy(
    group_fields: []const []const u8,
    group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    group_by: []const []const u8,
    parsed_group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
) !void {
    if (!stringSlicesEqual(group_fields, group_by)) return error.UnsupportedSqlShape;
    if (group_expressions.len != parsed_group_expressions.len) return error.UnsupportedSqlShape;
    for (group_expressions, parsed_group_expressions) |selected, parsed| {
        if (!expr_equal.relationalRowsExpressionEqual(selected.expression, parsed.expression)) return error.UnsupportedSqlShape;
    }
}

fn stringSlicesEqual(left: []const []const u8, right: []const []const u8) bool {
    if (left.len != right.len) return false;
    for (left, right) |left_item, right_item| {
        if (!std.mem.eql(u8, left_item, right_item)) return false;
    }
    return true;
}

pub fn outputFieldIsUnique(
    group_fields: []const []const u8,
    group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    aggregations: []const db_mod.types.RelationalRowsAggregateSpec,
    field: []const u8,
) bool {
    var matches: usize = 0;
    for (group_fields) |group_field| {
        if (std.mem.eql(u8, group_field, field)) matches += 1;
    }
    for (group_expressions) |projection| {
        if (std.mem.eql(u8, projection.output, field)) matches += 1;
    }
    for (aggregations) |aggregation| {
        if (std.mem.eql(u8, aggregation.name, field)) matches += 1;
    }
    return matches == 1;
}

pub fn validateSelectListOutputs(
    group_fields: []const []const u8,
    group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    aggregations: []const db_mod.types.RelationalRowsAggregateSpec,
) !void {
    for (group_fields, 0..) |field, i| {
        for (group_fields[i + 1 ..]) |other| {
            if (std.mem.eql(u8, field, other)) return error.UnsupportedSqlShape;
        }
        for (group_expressions) |projection| {
            if (std.mem.eql(u8, field, projection.output)) return error.UnsupportedSqlShape;
        }
        for (aggregations) |aggregation| {
            if (std.mem.eql(u8, field, aggregation.name)) return error.UnsupportedSqlShape;
        }
    }
    for (group_expressions, 0..) |projection, i| {
        for (group_expressions[i + 1 ..]) |other| {
            if (std.mem.eql(u8, projection.output, other.output)) return error.UnsupportedSqlShape;
        }
        for (aggregations) |aggregation| {
            if (std.mem.eql(u8, projection.output, aggregation.name)) return error.UnsupportedSqlShape;
        }
    }
    for (aggregations, 0..) |aggregation, i| {
        for (aggregations[i + 1 ..]) |other| {
            if (std.mem.eql(u8, aggregation.name, other.name)) return error.UnsupportedSqlShape;
        }
    }
}

pub fn outputFieldByOrdinalAlloc(
    alloc: std.mem.Allocator,
    group_fields: []const []const u8,
    group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    aggregations: []const db_mod.types.RelationalRowsAggregateSpec,
    ordinal: u32,
) ![]const u8 {
    if (ordinal == 0) return error.UnsupportedSqlShape;
    var index: usize = @intCast(ordinal - 1);
    if (index < group_fields.len) return try alloc.dupe(u8, group_fields[index]);
    index -= group_fields.len;
    if (index < group_expressions.len) return try alloc.dupe(u8, group_expressions[index].output);
    index -= group_expressions.len;
    if (index < aggregations.len) return try alloc.dupe(u8, aggregations[index].name);
    return error.UnsupportedSqlShape;
}

fn specNameCollision(
    name: []const u8,
    group_fields: []const []const u8,
    group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    aggregations: []const db_mod.types.RelationalRowsAggregateSpec,
) bool {
    for (group_fields) |field| {
        if (std.mem.eql(u8, field, name)) return true;
    }
    for (group_expressions) |projection| {
        if (std.mem.eql(u8, projection.output, name)) return true;
    }
    for (aggregations) |aggregation| {
        if (std.mem.eql(u8, aggregation.name, name)) return true;
    }
    return false;
}

pub fn specNameCollidesWithGroupOutput(
    name: []const u8,
    group_fields: []const []const u8,
    group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
) bool {
    for (group_fields) |field| {
        if (std.mem.eql(u8, field, name)) return true;
    }
    for (group_expressions) |projection| {
        if (std.mem.eql(u8, projection.output, name)) return true;
    }
    return false;
}

pub fn allocateDisambiguatedSpecNameAlloc(
    alloc: std.mem.Allocator,
    name: []const u8,
    group_fields: []const []const u8,
    group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    aggregations: []const db_mod.types.RelationalRowsAggregateSpec,
) !?[]const u8 {
    if (!specNameCollision(name, group_fields, group_expressions, aggregations)) return null;

    var suffix: usize = 2;
    while (true) : (suffix += 1) {
        const candidate = try std.fmt.allocPrint(alloc, "{s}_{d}", .{ name, suffix });
        if (!specNameCollision(candidate, group_fields, group_expressions, aggregations)) return candidate;
        alloc.free(candidate);
    }
}

pub fn specWithName(
    spec: db_mod.types.RelationalRowsAggregateSpec,
    name: []const u8,
) db_mod.types.RelationalRowsAggregateSpec {
    return .{
        .name = name,
        .op = spec.op,
        .field = spec.field,
        .expression = spec.expression,
        .distinct = spec.distinct,
        .distinct_max_items = spec.distinct_max_items,
        .percentile = spec.percentile,
        .percentiles = spec.percentiles,
        .percentile_max_items = spec.percentile_max_items,
        .percentile_order = spec.percentile_order,
        .array_max_items = spec.array_max_items,
        .array_order_by = spec.array_order_by,
        .string_delimiter = spec.string_delimiter,
        .filter_predicates = spec.filter_predicates,
        .filter_array_any = spec.filter_array_any,
        .filter_array_contains = spec.filter_array_contains,
        .filter_array_eq = spec.filter_array_eq,
        .filter_in_predicates = spec.filter_in_predicates,
        .filter_json_contains = spec.filter_json_contains,
        .filter_json_path_eq = spec.filter_json_path_eq,
        .filter_json_path_exists = spec.filter_json_path_exists,
        .filter_text_patterns = spec.filter_text_patterns,
        .filter_expressions = spec.filter_expressions,
        .filter_expression_array_contains = spec.filter_expression_array_contains,
        .filter_any = spec.filter_any,
        .filter_not = spec.filter_not,
    };
}

pub fn inputType(
    schema: runtime_schema.TableSchema,
    type_context: expr_type.RowExpressionTypeContext,
    aggregation: db_mod.types.RelationalRowsAggregateSpec,
) !runtime_schema.AntflyType {
    if (aggregation.field) |field| {
        const column = binder.relationalColumnForField(schema, field, null) orelse return error.InvalidSqlCatalog;
        return column.field_type;
    }
    if (aggregation.expression) |expression| {
        return try type_context.rowExpressionOutputType(expression);
    }
    return error.UnsupportedSqlShape;
}

pub fn outputColumnsAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    type_context: expr_type.RowExpressionTypeContext,
    group_fields: []const []const u8,
    group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    aggregations: []const db_mod.types.RelationalRowsAggregateSpec,
) ![]runtime_schema.RelationalColumn {
    const total = group_fields.len + group_expressions.len + aggregations.len;
    if (total == 0) return &.{};
    const out = try alloc.alloc(runtime_schema.RelationalColumn, total);
    var initialized: usize = 0;
    errdefer {
        ddl_plan.clearDdlRelationalColumns(alloc, out[0..initialized]);
        alloc.free(out);
    }
    for (group_fields) |field| {
        if (expr_projection.outputColumnExists(out[0..initialized], field)) return error.UnsupportedSqlShape;
        const column = binder.relationalColumnForField(schema, field, null) orelse return error.InvalidSqlCatalog;
        out[initialized] = try expr_projection.projectedSourceColumnAlloc(alloc, field, column);
        initialized += 1;
    }
    for (group_expressions) |projection| {
        if (expr_projection.outputColumnExists(out[0..initialized], projection.output)) return error.UnsupportedSqlShape;
        out[initialized] = try expr_projection.projectedColumnAlloc(alloc, projection.output, try type_context.rowExpressionOutputType(projection.expression), null, true);
        initialized += 1;
    }
    for (aggregations) |aggregation| {
        if (expr_projection.outputColumnExists(out[0..initialized], aggregation.name)) return error.UnsupportedSqlShape;
        const aggregation_input_type = if (aggregation.field != null or aggregation.expression != null)
            try inputType(schema, type_context, aggregation)
        else
            null;
        const projected_type = try outputProjectedType(aggregation, aggregation_input_type);
        out[initialized] = try expr_projection.projectedColumnAlloc(alloc, aggregation.name, projected_type.field_type, projected_type.array_item_type, false);
        initialized += 1;
    }
    return out;
}

pub fn outputColumnForFieldAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    type_context: expr_type.RowExpressionTypeContext,
    group_fields: []const []const u8,
    group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    aggregations: []const db_mod.types.RelationalRowsAggregateSpec,
    field: []const u8,
) !runtime_schema.RelationalColumn {
    const output_columns = try outputColumnsAlloc(alloc, schema, type_context, group_fields, group_expressions, aggregations);
    defer ddl_plan.freeDdlRelationalColumns(alloc, output_columns);
    for (output_columns) |column| {
        if (std.mem.eql(u8, column.name, field)) return try expr_projection.projectedSourceColumnAlloc(alloc, column.name, column);
    }
    return error.UnsupportedSqlShape;
}

pub fn parseGroupByAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
    schema: runtime_schema.TableSchema,
    function_bindings: expr_row_parse.SqlFunctionBindings,
    type_context: expr_type.RowExpressionTypeContext,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
    field_source: db_mod.types.RelationalRowsExpressionFieldSource,
    group_by: *std.ArrayListUnmanaged([]const u8),
    group_expressions: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionProjection),
    select: plan_mod.AggregateSelectList,
    select_item_options: expr_projection.SelectItemParserOptions,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
) !void {
    while (true) {
        const item_start = pos.*;
        const generated_item = try generated_read_validate.generatedGroupItemAtStart(tokens, item_start, generated_read_ast);
        if (generated_item == null and generated_read_ast != null) return error.UnsupportedSqlShape;
        const generated_expression = if (generated_item) |item| item.expression else null;
        if (parser.matchToken(tokens, pos, .number)) |token| {
            try generated_read_validate.validateGeneratedSimpleGroupExpression(tokens, generated_expression);
            const ordinal = std.fmt.parseInt(u32, token.text, 10) catch return error.UnsupportedSqlShape;
            try appendGroupByOrdinal(alloc, group_by, group_expressions, select, ordinal);
        } else if (peekStandaloneGroupIdentifier(tokens, pos.*)) {
            try generated_read_validate.validateGeneratedSimpleGroupExpression(tokens, generated_expression);
            const raw_field = try expr_generated.parseRowExpressionFieldOwnedAlloc(
                alloc,
                tokens,
                pos,
                schema,
                field_expression_qualifiers,
                returning_expression_qualifiers,
                defer_row_expression_field_validation,
            );
            try appendGroupByFieldOrAlias(
                alloc,
                schema,
                group_by,
                group_expressions,
                select,
                raw_field,
                field_expression_qualifiers,
                returning_expression_qualifiers,
                defer_row_expression_field_validation,
            );
        } else {
            var item_options = select_item_options;
            item_options.generated_expression_ast = generated_expression;
            const item = try expr_projection.parseSelectItemAlloc(
                alloc,
                tokens,
                pos,
                params,
                schema,
                function_bindings,
                field_expression_qualifiers,
                returning_expression_qualifiers,
                defer_row_expression_field_validation,
                field_source,
                type_context,
                item_options,
            );
            var item_transferred = false;
            errdefer if (!item_transferred) plan_mod.freeSelectItem(alloc, item);
            switch (item) {
                .field => |field| try group_by.append(alloc, field),
                .expression => |projection| try group_expressions.append(alloc, projection),
                else => return error.UnsupportedSqlShape,
            }
            item_transferred = true;
        }
        try generated_read_validate.validateGeneratedExpressionItemEnd(generated_item, pos.*);
        if (parser.matchToken(tokens, pos, .comma) == null) break;
    }
}

pub fn parseSelectListAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
    schema: runtime_schema.TableSchema,
    function_bindings: expr_row_parse.SqlFunctionBindings,
    type_context: expr_type.RowExpressionTypeContext,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
    field_source: db_mod.types.RelationalRowsExpressionFieldSource,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
    aggregate_spec_options: SpecParserOptions,
    select_item_options: expr_projection.SelectItemParserOptions,
) !plan_mod.AggregateSelectList {
    var group_fields = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (group_fields.items) |field| alloc.free(field);
        group_fields.deinit(alloc);
    }
    var group_expressions = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionProjection).empty;
    errdefer {
        for (group_expressions.items) |projection| plan_mod.freeExpressionProjection(alloc, projection);
        group_expressions.deinit(alloc);
    }
    var aggregations = std.ArrayListUnmanaged(db_mod.types.RelationalRowsAggregateSpec).empty;
    errdefer {
        plan_mod.freeAggregateSpecs(alloc, aggregations.items);
        aggregations.deinit(alloc);
    }
    var outputs = std.ArrayListUnmanaged(plan_mod.AggregateSelectOutputRef).empty;
    errdefer outputs.deinit(alloc);

    while (true) {
        const item_start = pos.*;
        const generated_item = try generated_read_validate.generatedProjectionItemAtStart(tokens, item_start, generated_read_ast);
        if (generated_item == null and generated_read_ast != null) return error.UnsupportedSqlShape;
        const generated_expression = if (generated_item) |item| item.expression else null;
        const generated_aggregate_op = if (generated_expression) |expression|
            try generatedOpForExpression(tokens, expression)
        else
            null;
        const parse_aggregate = if (generated_read_ast != null) blk: {
            if (nextIsFunction(tokens, pos.*) and generated_aggregate_op == null) return error.UnsupportedSqlShape;
            break :blk generated_aggregate_op != null;
        } else nextIsFunction(tokens, pos.*);

        if (parse_aggregate) {
            var spec_options = aggregate_spec_options;
            spec_options.generated_expression_ast = generated_expression;
            const parsed_spec = try parseSpecAlloc(
                alloc,
                tokens,
                pos,
                params,
                schema,
                type_context,
                field_expression_qualifiers,
                returning_expression_qualifiers,
                defer_row_expression_field_validation,
                spec_options,
            );
            var parsed_spec_transferred = false;
            errdefer if (!parsed_spec_transferred) plan_mod.freeAggregateSpec(alloc, parsed_spec);
            const maybe_spec_name = try allocateDisambiguatedSpecNameAlloc(
                alloc,
                parsed_spec.name,
                group_fields.items,
                group_expressions.items,
                aggregations.items,
            );
            var spec_name_transferred = false;
            errdefer if (!spec_name_transferred) {
                if (maybe_spec_name) |spec_name| alloc.free(spec_name);
            };
            const spec = if (maybe_spec_name) |spec_name| specWithName(parsed_spec, spec_name) else parsed_spec;
            parsed_spec_transferred = true;
            if (maybe_spec_name != null) {
                alloc.free(parsed_spec.name);
                spec_name_transferred = true;
            }
            var spec_transferred = false;
            errdefer if (!spec_transferred) plan_mod.freeAggregateSpec(alloc, spec);
            try generated_read_validate.validateGeneratedExpressionItemEnd(generated_item, pos.*);
            try outputs.append(alloc, .{ .kind = .aggregation, .index = aggregations.items.len });
            try aggregations.append(alloc, spec);
            spec_transferred = true;
        } else {
            var item_options = select_item_options;
            item_options.generated_expression_ast = generated_expression;
            const item = try expr_projection.parseSelectItemAlloc(
                alloc,
                tokens,
                pos,
                params,
                schema,
                function_bindings,
                field_expression_qualifiers,
                returning_expression_qualifiers,
                defer_row_expression_field_validation,
                field_source,
                type_context,
                item_options,
            );
            var item_transferred = false;
            errdefer if (!item_transferred) plan_mod.freeSelectItem(alloc, item);
            switch (item) {
                .field => |field| {
                    try outputs.append(alloc, .{ .kind = .group_field, .index = group_fields.items.len });
                    try group_fields.append(alloc, field);
                },
                .expression => |projection_value| {
                    item_transferred = true;
                    const projection = projection_value;
                    var projection_transferred = false;
                    errdefer if (!projection_transferred) plan_mod.freeExpressionProjection(alloc, projection);
                    try outputs.append(alloc, .{ .kind = .group_expression, .index = group_expressions.items.len });
                    try group_expressions.append(alloc, projection);
                    projection_transferred = true;
                },
                else => return error.UnsupportedSqlShape,
            }
            try generated_read_validate.validateGeneratedExpressionItemEnd(generated_item, pos.*);
            item_transferred = true;
        }
        if (parser.matchToken(tokens, pos, .comma) == null) break;
    }
    try validateSelectListOutputs(group_fields.items, group_expressions.items, aggregations.items);

    const owned_group_fields = try group_fields.toOwnedSlice(alloc);
    var group_fields_transferred = false;
    errdefer if (!group_fields_transferred) strings.freeStringSlice(alloc, owned_group_fields);
    const owned_group_expressions = try group_expressions.toOwnedSlice(alloc);
    var group_expressions_transferred = false;
    errdefer if (!group_expressions_transferred) {
        plan_mod.freeExpressionProjections(alloc, owned_group_expressions);
        if (owned_group_expressions.len > 0) alloc.free(owned_group_expressions);
    };
    const owned_aggregations = try aggregations.toOwnedSlice(alloc);
    var aggregations_transferred = false;
    errdefer if (!aggregations_transferred) {
        plan_mod.freeAggregateSpecs(alloc, owned_aggregations);
        if (owned_aggregations.len > 0) alloc.free(owned_aggregations);
    };
    const owned_outputs = try outputs.toOwnedSlice(alloc);
    var outputs_transferred = false;
    errdefer if (!outputs_transferred and owned_outputs.len > 0) alloc.free(owned_outputs);

    group_fields_transferred = true;
    group_expressions_transferred = true;
    aggregations_transferred = true;
    outputs_transferred = true;
    return .{
        .group_fields = owned_group_fields,
        .group_expressions = owned_group_expressions,
        .aggregations = owned_aggregations,
        .outputs = owned_outputs,
    };
}

test "sql expr_aggregate validates output type and filter ownership" {
    const alloc = std.testing.allocator;

    try std.testing.expectEqual(db_mod.types.RelationalRowsAggregateOp.percentile_cont, opForName("PERCENTILE_CONT").?);
    try std.testing.expectEqualStrings("array_agg", opName(.array_agg));
    try validatePercentile(0);
    try validatePercentile(1);
    try std.testing.expectError(error.UnsupportedSqlShape, validatePercentile(-0.01));
    try std.testing.expectError(error.UnsupportedSqlShape, validatePercentile(1.01));
    try std.testing.expectError(error.UnsupportedSqlShape, validatePercentile(std.math.inf(f64)));
    const explicit_alias = try aliasOrDefaultAlloc(alloc, "total_amount", .sum, "amount");
    defer alloc.free(explicit_alias);
    try std.testing.expectEqualStrings("total_amount", explicit_alias);
    const field_alias = try aliasOrDefaultAlloc(alloc, null, .sum, "amount");
    defer alloc.free(field_alias);
    try std.testing.expectEqualStrings("sum_amount", field_alias);
    const op_alias = try aliasOrDefaultAlloc(alloc, null, .count, null);
    defer alloc.free(op_alias);
    try std.testing.expectEqualStrings("count", op_alias);
    try std.testing.expect(isPercentileOp(.percentile_disc));
    try std.testing.expect(!isPercentileOp(.array_agg));
    try std.testing.expectEqual(@as(?f64, 1.5), sqlJsonNumberAsF64(.{ .number_string = "1.5" }));
    try std.testing.expect(sqlJsonNumberAsF64(.{ .string = "1.5" }) == null);

    const count_tokens = [_]Token{
        .{ .kind = .identifier, .text = "count" },
        .{ .kind = .lparen, .text = "(" },
    };
    try std.testing.expect(nextIsFunction(count_tokens[0..], 0));
    const parenthesized_condition_tokens = [_]Token{
        .{ .kind = .lparen, .text = "(" },
        .{ .kind = .identifier, .text = "sum" },
        .{ .kind = .lparen, .text = "(" },
        .{ .kind = .identifier, .text = "amount" },
        .{ .kind = .rparen, .text = ")" },
        .{ .kind = .rparen, .text = ")" },
        .{ .kind = .gt, .text = ">" },
        .{ .kind = .number, .text = "0" },
    };
    try std.testing.expect(peekParenthesizedExpressionCondition(parenthesized_condition_tokens[0..], 0));
    const aggregate_json_eq_tokens = [_]Token{
        .{ .kind = .identifier, .text = "metadata" },
        .{ .kind = .arrow_text, .text = "->>" },
        .{ .kind = .string, .text = "status" },
        .{ .kind = .eq, .text = "=" },
        .{ .kind = .identifier, .text = "true" },
    };
    try std.testing.expect(jsonPathEqFilterCanStartAt(&aggregate_json_eq_tokens, 0));
    const not_group_tokens = [_]Token{
        .{ .kind = .identifier, .text = "not" },
        .{ .kind = .lparen, .text = "(" },
    };
    try std.testing.expect(canParseFilterNot(&not_group_tokens, 0));
    try std.testing.expect(canParseHavingNot(&not_group_tokens, 0));
    const having_boolean_tokens = [_]Token{
        .{ .kind = .identifier, .text = "open" },
        .{ .kind = .identifier, .text = "is" },
        .{ .kind = .identifier, .text = "not" },
        .{ .kind = .identifier, .text = "false" },
    };
    try std.testing.expect(havingHasBooleanIsNot(&having_boolean_tokens, 0));
    var group_pos: usize = 0;
    const parenthesized_group_tokens = [_]Token{
        .{ .kind = .lparen, .text = "(" },
        .{ .kind = .identifier, .text = "status" },
        .{ .kind = .eq, .text = "=" },
        .{ .kind = .string, .text = "active" },
        .{ .kind = .rparen, .text = ")" },
    };
    try std.testing.expect(matchBooleanGroupOpen(&parenthesized_group_tokens, &group_pos));
    try std.testing.expectEqual(@as(usize, 1), group_pos);
    const group_identifier_tokens = [_]Token{
        .{ .kind = .identifier, .text = "status" },
        .{ .kind = .comma, .text = "," },
    };
    const function_identifier_tokens = [_]Token{
        .{ .kind = .identifier, .text = "lower" },
        .{ .kind = .lparen, .text = "(" },
    };
    try std.testing.expect(peekStandaloneGroupIdentifier(&group_identifier_tokens, 0));
    try std.testing.expect(!peekStandaloneGroupIdentifier(&function_identifier_tokens, 0));

    const delimiter_tokens = [_]Token{.{ .kind = .string, .text = "|" }};
    var delimiter_pos: usize = 0;
    const delimiter = try parseStringDelimiterAlloc(alloc, delimiter_tokens[0..], &delimiter_pos);
    defer alloc.free(delimiter);
    try std.testing.expectEqual(@as(usize, 1), delimiter_pos);
    try std.testing.expectEqualStrings("|", delimiter);

    const percentile_tokens = [_]Token{.{ .kind = .number, .text = "0.95" }};
    var percentile_pos: usize = 0;
    const percentile_argument = try parsePercentileArgumentAlloc(alloc, percentile_tokens[0..], &percentile_pos, &.{});
    try std.testing.expectEqual(@as(usize, 1), percentile_pos);
    try std.testing.expectEqual(@as(?f64, 0.95), percentile_argument.percentile);

    try std.testing.expectEqual(runtime_schema.AntflyType.array, (try outputProjectedType(.{
        .name = "items",
        .op = .array_agg,
    }, .keyword)).field_type);
    try std.testing.expectEqual(runtime_schema.AntflyType.keyword, (try outputProjectedType(.{
        .name = "items",
        .op = .array_agg,
    }, .keyword)).array_item_type.?);
    try std.testing.expectEqual(runtime_schema.AntflyType.keyword, (try outputProjectedType(.{
        .name = "labels",
        .op = .string_agg,
    }, null)).field_type);
    try std.testing.expectEqual(runtime_schema.AntflyType.numeric, (try outputProjectedType(.{
        .name = "count",
        .op = .count,
    }, null)).field_type);
    try std.testing.expectEqual(runtime_schema.AntflyType.numeric, (try outputProjectedType(.{
        .name = "p95",
        .op = .percentile_cont,
    }, null)).field_type);
    try std.testing.expectEqual(runtime_schema.AntflyType.array, (try outputProjectedType(.{
        .name = "percentiles",
        .op = .percentile_disc,
        .percentiles = &.{ 0.5, 0.95 },
    }, null)).field_type);
    try std.testing.expectEqual(runtime_schema.AntflyType.numeric, (try outputProjectedType(.{
        .name = "percentiles",
        .op = .percentile_disc,
        .percentiles = &.{ 0.5, 0.95 },
    }, null)).array_item_type.?);
    try std.testing.expectEqual(runtime_schema.AntflyType.datetime, (try outputProjectedType(.{
        .name = "latest",
        .op = .max,
    }, .datetime)).field_type);
    try std.testing.expectError(error.UnsupportedSqlShape, outputProjectedType(.{
        .name = "missing",
        .op = .array_agg,
    }, null));
    try std.testing.expect(filterIsEmpty(.{}));

    const owned_filter_predicates = try alloc.alloc(runtime_schema.RelationalCheck, 1);
    const owned_filter_field = try alloc.dupe(u8, "status");
    const owned_filter_value = try alloc.dupe(u8, "\"open\"");
    owned_filter_predicates[0] = .{
        .name = "",
        .field = owned_filter_field,
        .op = .eq,
        .value_json = owned_filter_value,
    };
    const owned_filter: Filter = .{ .predicates = owned_filter_predicates };
    defer freeFilter(alloc, owned_filter);
    try std.testing.expect(!filterIsEmpty(owned_filter));

    const aggregate_specs = [_]db_mod.types.RelationalRowsAggregateSpec{.{
        .name = "statuses",
        .op = .array_agg,
        .field = "status",
        .filter_json_contains = &.{.{ .field = "metadata", .value_json = "{\"source\":\"sql\"}" }},
    }};
    try std.testing.expectEqual(@as(usize, 0), filterGroupCount(&aggregate_specs));
    try std.testing.expectEqual(@as(usize, 0), filterExpressionCount(&aggregate_specs));
    try std.testing.expectEqual(@as(usize, 0), filterExpressionArrayCount(&aggregate_specs));
    try std.testing.expectEqual(@as(usize, 1), filterJsonAccessCount(&aggregate_specs));
    try std.testing.expectEqual(@as(usize, 0), filterStructuredAccessCount(&aggregate_specs));
    try std.testing.expectEqual(@as(usize, 0), inputExpressionCount(&aggregate_specs));

    const percentile_specs = [_]db_mod.types.RelationalRowsAggregateSpec{
        .{ .name = "p", .op = .percentile_cont, .field = "amount", .percentile_order = .desc, .percentiles = &.{ 0.5, 0.9 } },
        .{ .name = "m", .op = .mode, .field = "status" },
    };
    try std.testing.expectEqual(@as(usize, 1), descendingPercentileCount(&percentile_specs));
    try std.testing.expectEqual(@as(usize, 1), percentileArrayCount(&percentile_specs));
    try std.testing.expectEqual(@as(usize, 1), modeCount(&percentile_specs));

    const schema = runtime_schema.TableSchema{
        .storage_mode = .relational,
        .relational_columns = &.{.{ .name = "status", .path = "status", .field_type = .keyword }},
    };
    const group_projection = db_mod.types.RelationalRowsExpressionProjection{
        .output = "status_lower",
        .expression = .{ .kind = .field, .field = "status" },
    };
    const aggregate_select = plan_mod.AggregateSelectList{
        .group_fields = &.{"status"},
        .group_expressions = &.{group_projection},
        .aggregations = &aggregate_specs,
        .outputs = &.{
            .{ .kind = .group_field, .index = 0 },
            .{ .kind = .group_expression, .index = 0 },
            .{ .kind = .aggregation, .index = 0 },
        },
    };
    var bound_group_fields = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (bound_group_fields.items) |field| alloc.free(field);
        bound_group_fields.deinit(alloc);
    }
    var bound_group_expressions = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionProjection).empty;
    defer {
        for (bound_group_expressions.items) |projection| plan_mod.freeExpressionProjection(alloc, projection);
        bound_group_expressions.deinit(alloc);
    }
    try appendGroupByOrdinal(alloc, &bound_group_fields, &bound_group_expressions, aggregate_select, 1);
    try std.testing.expectEqualStrings("status", bound_group_fields.items[0]);
    try appendGroupByOrdinal(alloc, &bound_group_fields, &bound_group_expressions, aggregate_select, 2);
    try std.testing.expectEqualStrings("status_lower", bound_group_expressions.items[0].output);
    try std.testing.expectError(error.UnsupportedSqlShape, appendGroupByOrdinal(alloc, &bound_group_fields, &bound_group_expressions, aggregate_select, 3));
    try appendGroupByFieldOrAlias(alloc, schema, &bound_group_fields, &bound_group_expressions, aggregate_select, try alloc.dupe(u8, "status_lower"), &.{}, &.{}, false);
    try std.testing.expectEqualStrings("status_lower", bound_group_expressions.items[1].output);
    try appendGroupByFieldOrAlias(alloc, schema, &bound_group_fields, &bound_group_expressions, aggregate_select, try alloc.dupe(u8, "status"), &.{}, &.{}, false);
    try std.testing.expectEqualStrings("status", bound_group_fields.items[1]);
}
