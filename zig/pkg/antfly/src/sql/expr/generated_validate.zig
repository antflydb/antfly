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
const expr_generated = @import("generated.zig");
const expr_operator = @import("operator.zig");
const expr_token = @import("token.zig");
const generated_parser = @import("../generated_parser.zig");
const runtime_schema = @import("../../storage/schema.zig");
const token_mod = @import("../token.zig");

pub const Token = token_mod.Token;
pub const TokenKind = token_mod.TokenKind;

fn validateGeneratedExpressionOperatorTokens(
    tokens: []const Token,
    kind: generated_parser.GeneratedSqlExpressionKind,
    operator_tokens: generated_parser.GeneratedSqlTokenRange,
) !void {
    if (operator_tokens.start >= operator_tokens.end or operator_tokens.end > tokens.len) return error.UnsupportedSqlShape;
    const first = tokens[operator_tokens.start];
    switch (kind) {
        .comparison, .quantified_comparison => {
            if (operator_tokens.end != operator_tokens.start + 1 or !expr_generated.generatedTokenKindIsComparisonOperator(first.kind)) return error.UnsupportedSqlShape;
        },
        .like, .not_like => {
            if (operator_tokens.end != operator_tokens.start + 1 or !first.matchesKeywordTag(.like)) return error.UnsupportedSqlShape;
        },
        .ilike, .not_ilike => {
            if (operator_tokens.end != operator_tokens.start + 1 or !first.matchesKeywordTag(.ilike)) return error.UnsupportedSqlShape;
        },
        .in_list, .not_in_list => {
            if (operator_tokens.end != operator_tokens.start + 1 or !first.matchesKeywordTag(.in)) return error.UnsupportedSqlShape;
        },
        .between, .not_between => {
            if (operator_tokens.end != operator_tokens.start + 1 or !first.matchesKeywordTag(.between)) return error.UnsupportedSqlShape;
        },
        .exists_subquery, .not_exists_subquery => {
            if (operator_tokens.end != operator_tokens.start + 1 or !first.matchesKeywordTag(.exists)) return error.UnsupportedSqlShape;
        },
        .is_null, .is_not_null, .is_true, .is_false, .is_unknown, .is_not_true, .is_not_false, .is_not_unknown => {
            if (operator_tokens.end == operator_tokens.start + 1) {
                if (first.matchesKeywordTag(.is)) return;
                switch (kind) {
                    .is_null => if (!first.matchesKeywordTag(.isnull)) return error.UnsupportedSqlShape,
                    .is_not_null => if (!first.matchesKeywordTag(.notnull)) return error.UnsupportedSqlShape,
                    else => return error.UnsupportedSqlShape,
                }
            } else if (operator_tokens.end == operator_tokens.start + 2) {
                if (!first.matchesKeywordTag(.is)) return error.UnsupportedSqlShape;
                switch (kind) {
                    .is_null => if (!tokens[operator_tokens.start + 1].matchesKeywordTag(.null)) return error.UnsupportedSqlShape,
                    .is_true => if (!tokens[operator_tokens.start + 1].matchesKeywordTag(.true)) return error.UnsupportedSqlShape,
                    .is_false => if (!tokens[operator_tokens.start + 1].matchesKeywordTag(.false)) return error.UnsupportedSqlShape,
                    .is_unknown => if (!tokens[operator_tokens.start + 1].matchesKeywordTag(.unknown)) return error.UnsupportedSqlShape,
                    else => return error.UnsupportedSqlShape,
                }
            } else if (operator_tokens.end == operator_tokens.start + 3) {
                if (!first.matchesKeywordTag(.is) or !tokens[operator_tokens.start + 1].matchesKeywordTag(.not)) return error.UnsupportedSqlShape;
                switch (kind) {
                    .is_not_null => if (!tokens[operator_tokens.start + 2].matchesKeywordTag(.null)) return error.UnsupportedSqlShape,
                    .is_not_true => if (!tokens[operator_tokens.start + 2].matchesKeywordTag(.true)) return error.UnsupportedSqlShape,
                    .is_not_false => if (!tokens[operator_tokens.start + 2].matchesKeywordTag(.false)) return error.UnsupportedSqlShape,
                    .is_not_unknown => if (!tokens[operator_tokens.start + 2].matchesKeywordTag(.unknown)) return error.UnsupportedSqlShape,
                    else => return error.UnsupportedSqlShape,
                }
            } else {
                return error.UnsupportedSqlShape;
            }
        },
        .is_distinct_from => {
            if (operator_tokens.end != operator_tokens.start + 3 or
                !first.matchesKeywordTag(.is) or
                !tokens[operator_tokens.start + 1].matchesKeywordTag(.distinct) or
                !tokens[operator_tokens.start + 2].matchesKeywordTag(.from))
            {
                return error.UnsupportedSqlShape;
            }
        },
        .is_not_distinct_from => {
            if (operator_tokens.end != operator_tokens.start + 4 or
                !first.matchesKeywordTag(.is) or
                !tokens[operator_tokens.start + 1].matchesKeywordTag(.not) or
                !tokens[operator_tokens.start + 2].matchesKeywordTag(.distinct) or
                !tokens[operator_tokens.start + 3].matchesKeywordTag(.from))
            {
                return error.UnsupportedSqlShape;
            }
        },
        .logical_or => {
            if (operator_tokens.end != operator_tokens.start + 1 or !first.matchesKeywordTag(.@"or")) return error.UnsupportedSqlShape;
        },
        .logical_and => {
            if (operator_tokens.end != operator_tokens.start + 1 or !first.matchesKeywordTag(.@"and")) return error.UnsupportedSqlShape;
        },
        .logical_not => {
            if (operator_tokens.end != operator_tokens.start + 1 or !first.matchesKeywordTag(.not)) return error.UnsupportedSqlShape;
        },
        .unary_positive, .additive => {
            if (operator_tokens.end != operator_tokens.start + 1 or first.kind != .plus) return error.UnsupportedSqlShape;
        },
        .unary_negative, .subtractive => {
            if (operator_tokens.end != operator_tokens.start + 1 or first.kind != .minus) return error.UnsupportedSqlShape;
        },
        .multiplicative => {
            if (operator_tokens.end != operator_tokens.start + 1 or first.kind != .star) return error.UnsupportedSqlShape;
        },
        .divisive => {
            if (operator_tokens.end != operator_tokens.start + 1 or first.kind != .slash) return error.UnsupportedSqlShape;
        },
        .modulo => {
            if (operator_tokens.end != operator_tokens.start + 1 or first.kind != .percent) return error.UnsupportedSqlShape;
        },
        .contains => {
            if (operator_tokens.end != operator_tokens.start + 1 or first.kind != .at_contains) return error.UnsupportedSqlShape;
        },
        .overlaps => {
            if (operator_tokens.end != operator_tokens.start + 1 or first.kind != .range_overlap) return error.UnsupportedSqlShape;
        },
        .json_key_exists => {
            if (operator_tokens.end != operator_tokens.start + 1 or first.kind != .question) return error.UnsupportedSqlShape;
        },
        .json_key_any => {
            if (operator_tokens.end != operator_tokens.start + 1 or first.kind != .question_any) return error.UnsupportedSqlShape;
        },
        .json_key_all => {
            if (operator_tokens.end != operator_tokens.start + 1 or first.kind != .question_all) return error.UnsupportedSqlShape;
        },
        .regex_match => {
            if (operator_tokens.end != operator_tokens.start + 1 or first.kind != .regex_match) return error.UnsupportedSqlShape;
        },
        .regex_imatch => {
            if (operator_tokens.end != operator_tokens.start + 1 or first.kind != .regex_imatch) return error.UnsupportedSqlShape;
        },
        .regex_not_match => {
            if (operator_tokens.end != operator_tokens.start + 1 or first.kind != .regex_not_match) return error.UnsupportedSqlShape;
        },
        .regex_not_imatch => {
            if (operator_tokens.end != operator_tokens.start + 1 or first.kind != .regex_not_imatch) return error.UnsupportedSqlShape;
        },
        .string_concat => {
            if (operator_tokens.end != operator_tokens.start + 1 or first.kind != .pipe_concat) return error.UnsupportedSqlShape;
        },
        .json_access => {
            if (operator_tokens.end != operator_tokens.start + 1 or first.kind != .arrow_json) return error.UnsupportedSqlShape;
        },
        .json_text_access => {
            if (operator_tokens.end != operator_tokens.start + 1 or first.kind != .arrow_text) return error.UnsupportedSqlShape;
        },
        .json_path_access => {
            if (operator_tokens.end != operator_tokens.start + 1 or first.kind != .path_arrow_json) return error.UnsupportedSqlShape;
        },
        .json_path_text_access => {
            if (operator_tokens.end != operator_tokens.start + 1 or first.kind != .path_arrow_text) return error.UnsupportedSqlShape;
        },
        else => return error.UnsupportedSqlShape,
    }
}

fn validateGeneratedEmptyExpression(expression: generated_parser.GeneratedSqlExpressionAst) !void {
    if (expression.tokens != null) return error.UnsupportedSqlShape;
}

pub fn validateGeneratedOptionalExpression(
    tokens: []const Token,
    range: ?generated_parser.GeneratedSqlTokenRange,
    expression: generated_parser.GeneratedSqlExpressionAst,
) !void {
    if (range) |expression_range| {
        if (expression_range.start >= expression_range.end or expression_range.end > tokens.len) return error.UnsupportedSqlShape;
        if (!expr_generated.generatedTokenRangeEqual(expression.tokens orelse return error.UnsupportedSqlShape, expression_range)) return error.UnsupportedSqlShape;
        try validateGeneratedExpressionPayloads(tokens, expression);
    } else {
        try validateGeneratedEmptyExpression(expression);
    }
}

pub fn validateGeneratedOptionalExpressionForExactRange(
    tokens: []const Token,
    range: generated_parser.GeneratedSqlTokenRange,
    expression: *const generated_parser.GeneratedSqlExpressionAst,
) !bool {
    if (range.start >= range.end or range.end > tokens.len) return error.UnsupportedSqlShape;
    const exact_expression = generatedExpressionForExactRange(expression, range) orelse return false;
    try validateGeneratedOptionalExpression(tokens, range, exact_expression.*);
    return true;
}

pub fn generatedPredicateExpressionAtStart(
    tokens: []const Token,
    pos: usize,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
) !?*const generated_parser.GeneratedSqlExpressionAst {
    const expression = generated_expression_ast orelse return null;
    const expression_tokens = expression.tokens orelse return null;
    if (expression_tokens.start == pos) {
        switch (expression.kind) {
            .logical_or, .logical_and, .grouped => {},
            else => {
                try validateGeneratedExpressionPayloads(tokens, expression.*);
                return expression;
            },
        }
    }

    switch (expression.kind) {
        .logical_or, .logical_and => {
            const list = expression.boolean_condition_items;
            if (list.count == 0 or list.items.len != list.count or list.expressions.len != list.count) return error.UnsupportedSqlShape;
            for (list.items, 0..) |item, index| {
                if (item.start == pos) {
                    try validateGeneratedExpressionPayloads(tokens, list.expressions[index]);
                    return &list.expressions[index];
                }
                if (item.start <= pos and pos < item.end) {
                    if (try generatedPredicateExpressionAtStart(tokens, pos, &list.expressions[index])) |child| return child;
                }
            }
            return null;
        },
        .grouped => return try generatedPredicateExpressionAtStart(tokens, pos, expression.inner_expression),
        .logical_not => return try generatedPredicateExpressionAtStart(tokens, pos, expression.right_expression),
        else => return null,
    }
}

fn validateGeneratedChildExpressionPayloads(
    tokens: []const Token,
    range: generated_parser.GeneratedSqlTokenRange,
    kind: ?generated_parser.GeneratedSqlExpressionKind,
    expression: ?*const generated_parser.GeneratedSqlExpressionAst,
) !void {
    const child = expression orelse return error.UnsupportedSqlShape;
    if (kind == null or kind.? != child.kind) return error.UnsupportedSqlShape;
    if (!expr_generated.generatedTokenRangeEqual(child.tokens orelse return error.UnsupportedSqlShape, range)) return error.UnsupportedSqlShape;
    try validateGeneratedExpressionPayloads(tokens, child.*);
}

fn validateGeneratedChildExpressionPayloadsAllowUnknownKind(
    tokens: []const Token,
    range: generated_parser.GeneratedSqlTokenRange,
    kind: ?generated_parser.GeneratedSqlExpressionKind,
    expression: ?*const generated_parser.GeneratedSqlExpressionAst,
) !void {
    const child = expression orelse return error.UnsupportedSqlShape;
    if (kind) |expected_kind| {
        if (expected_kind != child.kind) return error.UnsupportedSqlShape;
    }
    if (!expr_generated.generatedTokenRangeEqual(child.tokens orelse return error.UnsupportedSqlShape, range)) return error.UnsupportedSqlShape;
    try validateGeneratedExpressionPayloads(tokens, child.*);
}

pub fn validateGeneratedExpressionPayloads(
    tokens: []const Token,
    expression: generated_parser.GeneratedSqlExpressionAst,
) anyerror!void {
    const expression_tokens = expression.tokens orelse return error.UnsupportedSqlShape;
    if (expression_tokens.start >= expression_tokens.end or expression_tokens.end > tokens.len) return error.UnsupportedSqlShape;
    if (expression.operator_tokens) |operator_tokens| try validateGeneratedExpressionOperatorTokens(tokens, expression.kind, operator_tokens);
    if (expression.kind == .array_constructor) try validateGeneratedArrayConstructorPayloads(tokens, expression, expression_tokens);
}

pub const GeneratedExpressionItem = struct {
    tokens: generated_parser.GeneratedSqlTokenRange,
    expression: *const generated_parser.GeneratedSqlExpressionAst,
};

pub fn generatedOrderItemAtStart(
    tokens: []const Token,
    pos: usize,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
) !?GeneratedExpressionItem {
    const read = generated_read_ast orelse return null;
    if (read.order_items.items.len != read.order_items.count or read.order_items.expressions.len != read.order_items.count) return error.UnsupportedSqlShape;
    for (read.order_items.items, 0..) |item, index| {
        if (item.start == pos) {
            try validateGeneratedExpressionPayloads(tokens, read.order_items.expressions[index]);
            return .{
                .tokens = item,
                .expression = &read.order_items.expressions[index],
            };
        }
    }
    return null;
}

pub fn generatedExpressionListItemAtStart(
    tokens: []const Token,
    pos: usize,
    list: *const generated_parser.GeneratedSqlListAst,
) !?GeneratedExpressionItem {
    if (list.items.len != list.count or list.expressions.len != list.count) return error.UnsupportedSqlShape;
    for (list.items, 0..) |item, index| {
        if (item.start == pos) {
            try validateGeneratedExpressionPayloads(tokens, list.expressions[index]);
            return .{
                .tokens = item,
                .expression = &list.expressions[index],
            };
        }
    }
    return null;
}

pub fn validateGeneratedExpressionItemEnd(generated_item: ?GeneratedExpressionItem, pos: usize) !void {
    if (generated_item) |item| {
        if (pos != item.tokens.end) return error.UnsupportedSqlShape;
    }
}

fn validateGeneratedExpressionPayloadsIfRetained(
    tokens: []const Token,
    expression: generated_parser.GeneratedSqlExpressionAst,
) !void {
    if (expression.tokens != null) try validateGeneratedExpressionPayloads(tokens, expression);
}

fn validateGeneratedArrayConstructorPayloads(
    tokens: []const Token,
    expression: generated_parser.GeneratedSqlExpressionAst,
    expression_tokens: generated_parser.GeneratedSqlTokenRange,
) anyerror!void {
    if (expression_tokens.end < expression_tokens.start + 3 or
        !tokens[expression_tokens.start].matchesKeywordTag(.array) or
        tokens[expression_tokens.start + 1].kind != .lbracket or
        tokens[expression_tokens.end - 1].kind != .rbracket)
    {
        return error.UnsupportedSqlShape;
    }
    if (expression.array_tokens) |array_tokens| {
        if (array_tokens.start != expression_tokens.start + 2 or
            array_tokens.end != expression_tokens.end - 1 or
            array_tokens.start >= array_tokens.end)
        {
            return error.UnsupportedSqlShape;
        }
    } else if (expression_tokens.end != expression_tokens.start + 3) {
        return error.UnsupportedSqlShape;
    }

    const list = expression.array_items;
    if (list.count == 0) {
        if (list.items.len != 0 or list.expression_items.len != 0 or list.expressions.len != 0) return error.UnsupportedSqlShape;
        return;
    }
    if (expression.array_tokens == null or
        list.count != list.items.len or
        list.count != list.expression_items.len or
        list.count != list.expressions.len)
    {
        return error.UnsupportedSqlShape;
    }
    const array_tokens = expression.array_tokens.?;
    for (list.items, 0..) |item, index| {
        if (item.start < array_tokens.start or item.end > array_tokens.end or item.start >= item.end) return error.UnsupportedSqlShape;
        if (!expr_generated.generatedTokenRangeEqual(list.expression_items[index], item)) return error.UnsupportedSqlShape;
        if (!expr_generated.generatedTokenRangeEqual(list.expressions[index].tokens orelse return error.UnsupportedSqlShape, item)) return error.UnsupportedSqlShape;
        try validateGeneratedExpressionPayloads(tokens, list.expressions[index]);
    }
}

pub fn generatedExpressionFunctionNameToken(
    tokens: []const Token,
    expression: generated_parser.GeneratedSqlExpressionAst,
) !Token {
    if (expression.kind != .function_call) return error.UnsupportedSqlShape;
    const name_tokens = expression.function_name_tokens orelse return error.UnsupportedSqlShape;
    if (name_tokens.end != name_tokens.start + 1 or name_tokens.end > tokens.len) return error.UnsupportedSqlShape;
    return tokens[name_tokens.start];
}

pub fn validateGeneratedProjectionListForClause(
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
        if (list.direction_items[index] != null or list.directions[index] != null) return error.UnsupportedSqlShape;
        if (list.order_using_operator_items[index] != null or list.nulls_order_items[index] != null or list.nulls_orders[index] != null) return error.UnsupportedSqlShape;

        const expression_range = list.expression_items[index];
        if (expression_range.start < item.start or expression_range.end > item.end or expression_range.start >= expression_range.end) return error.UnsupportedSqlShape;
        if (!expr_generated.generatedTokenRangeEqual(list.expressions[index].tokens orelse return error.UnsupportedSqlShape, expression_range)) return error.UnsupportedSqlShape;
        try validateGeneratedExpressionPayloads(tokens, list.expressions[index]);

        if (list.alias_items[index]) |alias_range| {
            const alias_name_range = list.alias_name_items[index] orelse return error.UnsupportedSqlShape;
            if (alias_range.start != expression_range.end or alias_range.end != item.end) return error.UnsupportedSqlShape;
            if (alias_name_range.start < alias_range.start or alias_name_range.end != alias_range.end or alias_name_range.start >= alias_name_range.end) return error.UnsupportedSqlShape;
            if (tokens[alias_range.start].matchesKeywordTag(.as)) {
                if (alias_name_range.start != alias_range.start + 1) return error.UnsupportedSqlShape;
            } else if (!expr_generated.generatedTokenRangeEqual(alias_name_range, alias_range)) return error.UnsupportedSqlShape;
        } else if (list.alias_name_items[index] != null) {
            return error.UnsupportedSqlShape;
        } else if (!expr_generated.generatedTokenRangeEqual(expression_range, item)) {
            return error.UnsupportedSqlShape;
        }
    }
}

pub fn generatedExpressionForExactRange(
    expression: *const generated_parser.GeneratedSqlExpressionAst,
    range: generated_parser.GeneratedSqlTokenRange,
) ?*const generated_parser.GeneratedSqlExpressionAst {
    if (expression.tokens) |tokens_range| {
        if (expr_generated.generatedTokenRangeEqual(tokens_range, range)) return expression;
    }
    return generatedChildExpressionForExactRange(expression, range);
}

fn generatedChildExpressionForExactRange(
    expression: *const generated_parser.GeneratedSqlExpressionAst,
    range: generated_parser.GeneratedSqlTokenRange,
) ?*const generated_parser.GeneratedSqlExpressionAst {
    if (generatedOptionalExpressionForExactRange(expression.inner_expression, expression.inner_tokens, range)) |found| return found;
    if (generatedOptionalExpressionForExactRange(expression.left_expression, expression.left_tokens, range)) |found| return found;
    if (generatedOptionalExpressionForExactRange(expression.right_expression, expression.right_tokens, range)) |found| return found;
    if (generatedOptionalExpressionForExactRange(expression.between_lower_expression, expression.between_lower_tokens, range)) |found| return found;
    if (generatedOptionalExpressionForExactRange(expression.between_upper_expression, expression.between_upper_tokens, range)) |found| return found;
    if (generatedOptionalExpressionForExactRange(expression.escape_expression, expression.escape_tokens, range)) |found| return found;
    if (generatedOptionalExpressionForExactRange(expression.cast_expression, expression.cast_expression_tokens, range)) |found| return found;
    if (generatedOptionalExpressionForExactRange(expression.case_first_condition, expression.case_first_condition_tokens, range)) |found| return found;
    if (generatedOptionalExpressionForExactRange(expression.case_first_result, expression.case_first_result_tokens, range)) |found| return found;
    if (generatedOptionalExpressionForExactRange(expression.case_else_expression, expression.case_else_expression_tokens, range)) |found| return found;
    if (generatedOptionalExpressionForExactRange(expression.boolean_first_condition, expression.boolean_first_condition_tokens, range)) |found| return found;
    if (generatedOptionalExpressionForExactRange(expression.boolean_last_condition, expression.boolean_last_condition_tokens, range)) |found| return found;
    if (generatedOptionalExpressionForExactRange(expression.extract_source_expression, expression.extract_source_tokens, range)) |found| return found;
    if (generatedExpressionListForExactRange(expression.argument_items, range)) |found| return found;
    if (generatedExpressionListForExactRange(expression.argument_order_items, range)) |found| return found;
    if (generatedExpressionListForExactRange(expression.within_group_order_items, range)) |found| return found;
    if (generatedExpressionListForExactRange(expression.over_partition_items, range)) |found| return found;
    if (generatedExpressionListForExactRange(expression.over_order_items, range)) |found| return found;
    if (generatedExpressionListForExactRange(expression.array_items, range)) |found| return found;
    if (generatedExpressionListForExactRange(expression.boolean_condition_items, range)) |found| return found;
    if (generatedExpressionListForExactRange(expression.case_condition_items, range)) |found| return found;
    if (generatedExpressionListForExactRange(expression.case_result_items, range)) |found| return found;
    if (generatedOptionalExpressionForExactRange(expression.filter_expression, expression.filter_predicate_tokens, range)) |found| return found;
    if (generatedOptionalExpressionForExactRange(expression.over_frame_start_expression, expression.over_frame_start_expression_tokens, range)) |found| return found;
    if (generatedOptionalExpressionForExactRange(expression.over_frame_end_expression, expression.over_frame_end_expression_tokens, range)) |found| return found;
    if (generatedOptionalExpressionForExactRange(expression.subquery_where_expression, expression.subquery_where_tokens, range)) |found| return found;
    if (expression.subquery_tail) |tail| {
        if (tail.limit_expression) |limit_expression| {
            if (generatedExpressionForExactRange(limit_expression, range)) |found| return found;
        }
        if (tail.offset_expression) |offset_expression| {
            if (generatedExpressionForExactRange(offset_expression, range)) |found| return found;
        }
        if (tail.fetch_count_expression) |fetch_expression| {
            if (generatedExpressionForExactRange(fetch_expression, range)) |found| return found;
        }
    }
    return null;
}

fn generatedExpressionListHasConsistentExpressions(list: generated_parser.GeneratedSqlListAst) bool {
    return list.count == list.items.len and list.count == list.expressions.len;
}

fn generatedExpressionListForExactRange(
    list: generated_parser.GeneratedSqlListAst,
    range: generated_parser.GeneratedSqlTokenRange,
) ?*const generated_parser.GeneratedSqlExpressionAst {
    if (!generatedExpressionListHasConsistentExpressions(list)) return null;
    for (list.expressions) |*item| {
        if (generatedExpressionForExactRange(item, range)) |found| return found;
    }
    return null;
}

fn generatedOptionalExpressionForExactRange(
    expression: ?*const generated_parser.GeneratedSqlExpressionAst,
    tokens: ?generated_parser.GeneratedSqlTokenRange,
    range: generated_parser.GeneratedSqlTokenRange,
) ?*const generated_parser.GeneratedSqlExpressionAst {
    if (tokens == null) return null;
    const child = expression orelse return null;
    return generatedExpressionForExactRange(child, range);
}

fn generatedExpressionListForOperatorStart(
    list: generated_parser.GeneratedSqlListAst,
    expected_kind: generated_parser.GeneratedSqlExpressionKind,
    operator_token_index: usize,
) ?*const generated_parser.GeneratedSqlExpressionAst {
    if (!generatedExpressionListHasConsistentExpressions(list)) return null;
    for (list.expressions) |*item| {
        if (generatedExpressionForOperatorStart(item, expected_kind, operator_token_index)) |found| return found;
    }
    return null;
}

fn generatedExpressionForOperatorStart(
    expression: *const generated_parser.GeneratedSqlExpressionAst,
    expected_kind: generated_parser.GeneratedSqlExpressionKind,
    operator_token_index: usize,
) ?*const generated_parser.GeneratedSqlExpressionAst {
    if (expression.kind == expected_kind) {
        if (expression.operator_tokens) |operator_tokens| {
            if (operator_tokens.start == operator_token_index) return expression;
        }
    }
    if (generatedOptionalExpressionForOperatorStart(expression.inner_expression, expression.inner_tokens, expected_kind, operator_token_index)) |found| return found;
    if (generatedOptionalExpressionForOperatorStart(expression.left_expression, expression.left_tokens, expected_kind, operator_token_index)) |found| return found;
    if (generatedOptionalExpressionForOperatorStart(expression.right_expression, expression.right_tokens, expected_kind, operator_token_index)) |found| return found;
    if (generatedOptionalExpressionForOperatorStart(expression.between_lower_expression, expression.between_lower_tokens, expected_kind, operator_token_index)) |found| return found;
    if (generatedOptionalExpressionForOperatorStart(expression.between_upper_expression, expression.between_upper_tokens, expected_kind, operator_token_index)) |found| return found;
    if (generatedOptionalExpressionForOperatorStart(expression.escape_expression, expression.escape_tokens, expected_kind, operator_token_index)) |found| return found;
    if (generatedOptionalExpressionForOperatorStart(expression.cast_expression, expression.cast_expression_tokens, expected_kind, operator_token_index)) |found| return found;
    if (generatedOptionalExpressionForOperatorStart(expression.case_first_condition, expression.case_first_condition_tokens, expected_kind, operator_token_index)) |found| return found;
    if (generatedOptionalExpressionForOperatorStart(expression.case_first_result, expression.case_first_result_tokens, expected_kind, operator_token_index)) |found| return found;
    if (generatedOptionalExpressionForOperatorStart(expression.case_else_expression, expression.case_else_expression_tokens, expected_kind, operator_token_index)) |found| return found;
    if (generatedOptionalExpressionForOperatorStart(expression.boolean_first_condition, expression.boolean_first_condition_tokens, expected_kind, operator_token_index)) |found| return found;
    if (generatedOptionalExpressionForOperatorStart(expression.boolean_last_condition, expression.boolean_last_condition_tokens, expected_kind, operator_token_index)) |found| return found;
    if (generatedOptionalExpressionForOperatorStart(expression.extract_source_expression, expression.extract_source_tokens, expected_kind, operator_token_index)) |found| return found;
    if (generatedOptionalExpressionForOperatorStart(expression.filter_expression, expression.filter_predicate_tokens, expected_kind, operator_token_index)) |found| return found;
    if (generatedOptionalExpressionForOperatorStart(expression.over_frame_start_expression, expression.over_frame_start_expression_tokens, expected_kind, operator_token_index)) |found| return found;
    if (generatedOptionalExpressionForOperatorStart(expression.over_frame_end_expression, expression.over_frame_end_expression_tokens, expected_kind, operator_token_index)) |found| return found;
    if (generatedOptionalExpressionForOperatorStart(expression.subquery_where_expression, expression.subquery_where_tokens, expected_kind, operator_token_index)) |found| return found;
    if (expression.subquery_tail) |tail| {
        if (tail.limit_expression) |limit_expression| {
            if (generatedExpressionForOperatorStart(limit_expression, expected_kind, operator_token_index)) |found| return found;
        }
        if (tail.offset_expression) |offset_expression| {
            if (generatedExpressionForOperatorStart(offset_expression, expected_kind, operator_token_index)) |found| return found;
        }
        if (tail.fetch_count_expression) |fetch_expression| {
            if (generatedExpressionForOperatorStart(fetch_expression, expected_kind, operator_token_index)) |found| return found;
        }
    }
    if (generatedExpressionListForOperatorStart(expression.argument_items, expected_kind, operator_token_index)) |found| return found;
    if (generatedExpressionListForOperatorStart(expression.argument_order_items, expected_kind, operator_token_index)) |found| return found;
    if (generatedExpressionListForOperatorStart(expression.within_group_order_items, expected_kind, operator_token_index)) |found| return found;
    if (generatedExpressionListForOperatorStart(expression.over_partition_items, expected_kind, operator_token_index)) |found| return found;
    if (generatedExpressionListForOperatorStart(expression.over_order_items, expected_kind, operator_token_index)) |found| return found;
    if (generatedExpressionListForOperatorStart(expression.array_items, expected_kind, operator_token_index)) |found| return found;
    if (generatedExpressionListForOperatorStart(expression.case_condition_items, expected_kind, operator_token_index)) |found| return found;
    if (generatedExpressionListForOperatorStart(expression.case_result_items, expected_kind, operator_token_index)) |found| return found;
    if (generatedExpressionListForOperatorStart(expression.boolean_condition_items, expected_kind, operator_token_index)) |found| return found;
    return null;
}

fn generatedOptionalExpressionForOperatorStart(
    expression: ?*const generated_parser.GeneratedSqlExpressionAst,
    tokens: ?generated_parser.GeneratedSqlTokenRange,
    expected_kind: generated_parser.GeneratedSqlExpressionKind,
    operator_token_index: usize,
) ?*const generated_parser.GeneratedSqlExpressionAst {
    if (tokens == null) return null;
    const child = expression orelse return null;
    return generatedExpressionForOperatorStart(child, expected_kind, operator_token_index);
}

fn generatedFunctionNameMatchesRowExpressionKind(
    token: Token,
    kind: db_mod.types.RelationalRowsExpressionKind,
) ?bool {
    return switch (kind) {
        .uuid_v4 => expr_token.sqlTokenIsUuidV4Function(token),
        .coalesce => token.matchesKeywordTag(.coalesce),
        .lower => token.matchesKeywordTag(.lower),
        .upper => token.matchesKeywordTag(.upper),
        .initcap => expr_token.sqlTokenIsInitcapFunction(token),
        .trim => token.matchesKeywordTag(.trim) or token.matchesKeywordTag(.btrim),
        .ltrim => token.matchesKeywordTag(.ltrim),
        .rtrim => token.matchesKeywordTag(.rtrim),
        .replace => token.matchesKeywordTag(.replace),
        .regexp_replace => token.matchesKeywordTag(.regexp_replace),
        .regexp_substr => expr_token.sqlTokenIsRegexpSubstrFunction(token),
        .regexp_count => expr_token.sqlTokenIsRegexpCountFunction(token),
        .regexp_instr => expr_token.sqlTokenIsRegexpInstrFunction(token),
        .translate => expr_token.sqlTokenIsTranslateFunction(token),
        .concat => token.matchesKeywordTag(.concat),
        .concat_ws => token.matchesKeywordTag(.concat_ws),
        .nullif => token.matchesKeywordTag(.nullif),
        .length => expr_token.sqlTokenIsLengthFunction(token),
        .octet_length => expr_token.sqlTokenIsOctetLengthFunction(token),
        .bit_length => expr_token.sqlTokenIsBitLengthFunction(token),
        .ascii => expr_token.sqlTokenIsAsciiFunction(token),
        .chr => expr_token.sqlTokenIsChrFunction(token),
        .substring => expr_token.sqlTokenIsSubstringFunction(token),
        .overlay => expr_token.sqlTokenIsOverlayFunction(token),
        .split_part => expr_token.sqlTokenIsSplitPartFunction(token),
        .strpos => expr_token.sqlTokenIsStrposFunction(token) or token.matchesKeywordTag(.position),
        .left => token.matchesKeywordTag(.left),
        .right => token.matchesKeywordTag(.right),
        .lpad => token.matchesKeywordTag(.lpad),
        .rpad => token.matchesKeywordTag(.rpad),
        .repeat => expr_token.sqlTokenIsRepeatFunction(token),
        .reverse => expr_token.sqlTokenIsReverseFunction(token),
        .md5 => expr_token.sqlTokenIsMd5Function(token),
        .starts_with => expr_token.sqlTokenIsStartsWithFunction(token),
        .ends_with => expr_token.sqlTokenIsEndsWithFunction(token),
        .regexp_match => expr_token.sqlTokenIsRegexpMatchFunction(token),
        .date_trunc => expr_token.sqlTokenIsDateTruncFunction(token),
        .date_bin => expr_token.sqlTokenIsDateBinFunction(token),
        .date_part => expr_token.sqlTokenIsDatePartFunction(token),
        .abs => token.matchesKeywordTag(.abs),
        .round => token.matchesKeywordTag(.round),
        .trunc => token.matchesKeywordTag(.trunc),
        .floor => token.matchesKeywordTag(.floor),
        .ceil => token.matchesKeywordTag(.ceil),
        .sqrt => token.matchesKeywordTag(.sqrt),
        .sign => token.matchesKeywordTag(.sign),
        .power => token.matchesKeywordTag(.power),
        .mod => token.matchesKeywordTag(.mod),
        .greatest => token.matchesKeywordTag(.greatest),
        .least => token.matchesKeywordTag(.least),
        .json_extract => expr_token.sqlTokenIsJsonExtractPathFunction(token),
        .json_typeof => expr_token.sqlTokenIsJsonTypeofFunction(token),
        .json_array_length => expr_token.sqlTokenIsJsonArrayLengthFunction(token),
        .json_build_object => expr_token.sqlTokenIsJsonBuildObjectFunction(token),
        .to_jsonb => token.matchesKeywordTag(.to_jsonb),
        .array_length => expr_token.sqlTokenIsArrayLengthFunction(token),
        .array_position => token.matchesKeywordTag(.array_position),
        .array_positions => token.matchesKeywordTag(.array_positions),
        .array_append => token.matchesKeywordTag(.array_append),
        .array_prepend => token.matchesKeywordTag(.array_prepend),
        .array_cat => token.matchesKeywordTag(.array_cat),
        .array_remove => token.matchesKeywordTag(.array_remove),
        .array_replace => token.matchesKeywordTag(.array_replace),
        .array_to_string => expr_token.sqlTokenIsArrayToStringFunction(token),
        .string_to_array => token.matchesKeywordTag(.string_to_array),
        else => null,
    };
}

fn generatedOperatorKindMatchesRowExpressionKind(
    generated_kind: generated_parser.GeneratedSqlExpressionKind,
    parsed_expression: db_mod.types.RelationalRowsExpression,
) ?bool {
    return switch (generated_kind) {
        .unary_negative => generatedUnaryNegativeMatchesRowExpression(parsed_expression),
        .additive => parsed_expression.kind == .add,
        .subtractive => parsed_expression.kind == .sub,
        .multiplicative => parsed_expression.kind == .mul,
        .divisive => parsed_expression.kind == .div,
        .modulo => parsed_expression.kind == .mod,
        .logical_or => parsed_expression.kind == .bool_or,
        .logical_and => parsed_expression.kind == .bool_and,
        .logical_not => parsed_expression.kind == .bool_not,
        .string_concat => parsed_expression.kind == .concat,
        .json_access, .json_text_access, .json_path_access, .json_path_text_access => parsed_expression.kind == .json_extract,
        .json_key_exists => parsed_expression.kind == .json_path_exists,
        .json_key_any => parsed_expression.kind == .json_path_exists or parsed_expression.kind == .bool_or,
        .json_key_all => parsed_expression.kind == .json_path_exists or parsed_expression.kind == .bool_and,
        else => null,
    };
}

fn generatedUnaryNegativeMatchesRowExpression(
    parsed_expression: db_mod.types.RelationalRowsExpression,
) bool {
    if (parsed_expression.kind == .value) return std.mem.startsWith(u8, parsed_expression.value_json, "-");
    return parsed_expression.kind == .mul and
        parsed_expression.operands.len == 2 and
        parsed_expression.operands[0].kind == .value and
        std.mem.eql(u8, parsed_expression.operands[0].value_json, "-1");
}

fn generatedArithmeticOperatorKindMayRepresentRowExpressionKind(
    generated_kind: generated_parser.GeneratedSqlExpressionKind,
    parsed_kind: db_mod.types.RelationalRowsExpressionKind,
) bool {
    return switch (generated_kind) {
        .additive, .subtractive => parsed_kind == .add or parsed_kind == .sub,
        .multiplicative, .divisive, .modulo => parsed_kind == .mul or parsed_kind == .div or parsed_kind == .mod,
        else => false,
    };
}

fn generatedLeafKindMayRepresentRowExpression(
    generated_kind: generated_parser.GeneratedSqlExpressionKind,
    parsed_expression: db_mod.types.RelationalRowsExpression,
) bool {
    return switch (generated_kind) {
        .token_range => true,
        .current_timestamp => parsed_expression.kind == .now,
        .current_date => parsed_expression.kind == .date_trunc,
        .timestamp_literal => parsed_expression.kind == .value,
        .interval_literal => parsed_expression.kind == .interval_ns or parsed_expression.kind == .interval_months,
        .cast => parsed_expression.kind == .cast,
        .case_expression => parsed_expression.kind == .case,
        .extract_expression => parsed_expression.kind == .date_part,
        .array_constructor => parsed_expression.kind == .value,
        else => false,
    };
}

fn validateGeneratedConcatExpressionOperandStrict(
    tokens: []const Token,
    generated_expression: *const generated_parser.GeneratedSqlExpressionAst,
    parsed_expression: db_mod.types.RelationalRowsExpression,
    parsed_operand_index: *usize,
) anyerror!void {
    if (generated_expression.kind == .string_concat) {
        const left_expression = generated_expression.left_expression orelse return error.UnsupportedSqlShape;
        try validateGeneratedConcatExpressionOperandStrict(tokens, left_expression, parsed_expression, parsed_operand_index);
        const right_expression = generated_expression.right_expression orelse return error.UnsupportedSqlShape;
        try validateGeneratedConcatExpressionOperandStrict(tokens, right_expression, parsed_expression, parsed_operand_index);
        return;
    }
    if (parsed_operand_index.* >= parsed_expression.operands.len) return error.UnsupportedSqlShape;
    const range = generated_expression.tokens orelse return error.UnsupportedSqlShape;
    try validateGeneratedRowExpressionIdentityStrict(
        tokens,
        range.start,
        range.end,
        parsed_expression.operands[parsed_operand_index.*],
        generated_expression,
    );
    parsed_operand_index.* += 1;
}

fn validateGeneratedConcatExpressionOperandsStrict(
    tokens: []const Token,
    parsed_expression: db_mod.types.RelationalRowsExpression,
    generated_expression: *const generated_parser.GeneratedSqlExpressionAst,
) anyerror!void {
    var parsed_operand_index: usize = 0;
    try validateGeneratedConcatExpressionOperandStrict(tokens, generated_expression, parsed_expression, &parsed_operand_index);
    if (parsed_operand_index != parsed_expression.operands.len) return error.UnsupportedSqlShape;
}

fn generatedPositionFunctionArgumentSeparator(
    tokens: []const Token,
    argument_tokens: generated_parser.GeneratedSqlTokenRange,
) ?usize {
    var depth: usize = 0;
    var i = argument_tokens.start;
    while (i < argument_tokens.end) : (i += 1) {
        switch (tokens[i].kind) {
            .lparen, .lbracket => depth += 1,
            .rparen, .rbracket => {
                if (depth == 0) return null;
                depth -= 1;
            },
            .identifier => if (depth == 0 and tokens[i].matchesKeywordTag(.in)) return i,
            else => {},
        }
    }
    return null;
}

fn validateGeneratedPositionFunctionOperandsStrict(
    tokens: []const Token,
    parsed_expression: db_mod.types.RelationalRowsExpression,
    generated_expression: *const generated_parser.GeneratedSqlExpressionAst,
) anyerror!bool {
    if (parsed_expression.kind != .strpos or generated_expression.kind != .function_call) return false;
    const token = try generatedExpressionFunctionNameToken(tokens, generated_expression.*);
    if (!token.matchesKeywordTag(.position)) return false;
    if (parsed_expression.operands.len != 2) return error.UnsupportedSqlShape;
    const argument_tokens = generated_expression.argument_tokens orelse return error.UnsupportedSqlShape;
    const separator = generatedPositionFunctionArgumentSeparator(tokens, argument_tokens) orelse return error.UnsupportedSqlShape;
    if (separator == argument_tokens.start or separator + 1 >= argument_tokens.end) return error.UnsupportedSqlShape;
    try validateGeneratedRowExpressionIdentityStrict(
        tokens,
        separator + 1,
        argument_tokens.end,
        parsed_expression.operands[0],
        generated_expression,
    );
    try validateGeneratedRowExpressionIdentityStrict(
        tokens,
        argument_tokens.start,
        separator,
        parsed_expression.operands[1],
        generated_expression,
    );
    return true;
}

fn jsonPathSegmentCount(path: []const u8) ?usize {
    if (path.len == 0) return null;
    var count: usize = 1;
    var previous_dot = false;
    for (path) |ch| {
        if (ch == '.') {
            if (previous_dot) return null;
            previous_dot = true;
            count += 1;
        } else {
            previous_dot = false;
        }
    }
    if (previous_dot) return null;
    return count;
}

fn validateGeneratedJsonExtractPathFunctionOperandsStrict(
    tokens: []const Token,
    parsed_expression: db_mod.types.RelationalRowsExpression,
    generated_expression: *const generated_parser.GeneratedSqlExpressionAst,
) anyerror!bool {
    if (parsed_expression.kind != .json_extract or generated_expression.kind != .function_call) return false;
    const token = try generatedExpressionFunctionNameToken(tokens, generated_expression.*);
    if (!expr_token.sqlTokenIsJsonExtractPathFunction(token)) return false;
    if (expr_token.sqlJsonExtractPathTokenAsText(token) != parsed_expression.json_as_text) return error.UnsupportedSqlShape;
    if (parsed_expression.operands.len != 1) return error.UnsupportedSqlShape;
    const segment_count = jsonPathSegmentCount(parsed_expression.json_path) orelse return error.UnsupportedSqlShape;
    if (generated_expression.argument_items.count != segment_count + 1 or
        generated_expression.argument_items.items.len != generated_expression.argument_items.count or
        generated_expression.argument_items.expressions.len != generated_expression.argument_items.count)
    {
        return error.UnsupportedSqlShape;
    }
    const source_tokens = generated_expression.argument_items.items[0];
    try validateGeneratedRowExpressionIdentityStrict(
        tokens,
        source_tokens.start,
        source_tokens.end,
        parsed_expression.operands[0],
        generated_expression,
    );
    for (generated_expression.argument_items.items[1..], 1..) |path_tokens, index| {
        try validateGeneratedOptionalExpression(tokens, path_tokens, generated_expression.argument_items.expressions[index]);
    }
    return true;
}

fn generatedJsonKeySetArrayExpression(
    expression: *const generated_parser.GeneratedSqlExpressionAst,
) ?*const generated_parser.GeneratedSqlExpressionAst {
    var current = expression;
    while (true) {
        switch (current.kind) {
            .grouped => current = current.inner_expression orelse return null,
            .cast => current = current.cast_expression orelse return null,
            .array_constructor => return current,
            else => return null,
        }
    }
}

fn validateGeneratedJsonKeySetExpressionOperandsStrict(
    tokens: []const Token,
    parsed_expression: db_mod.types.RelationalRowsExpression,
    generated_expression: *const generated_parser.GeneratedSqlExpressionAst,
) anyerror!bool {
    const expected_compound_kind: db_mod.types.RelationalRowsExpressionKind = switch (generated_expression.kind) {
        .json_key_any => .bool_or,
        .json_key_all => .bool_and,
        else => return false,
    };
    if (parsed_expression.kind != expected_compound_kind and parsed_expression.kind != .json_path_exists) return false;
    const left_tokens = generated_expression.left_tokens orelse return error.UnsupportedSqlShape;
    const right_expression = generated_expression.right_expression orelse return error.UnsupportedSqlShape;
    const array_expression = generatedJsonKeySetArrayExpression(right_expression) orelse return error.UnsupportedSqlShape;
    if (array_expression.array_items.count == 0 or
        array_expression.array_items.items.len != array_expression.array_items.count or
        array_expression.array_items.expressions.len != array_expression.array_items.count)
    {
        return error.UnsupportedSqlShape;
    }

    const path_exists_operands = if (parsed_expression.kind == .json_path_exists) blk: {
        if (parsed_expression.operands.len != 1) return error.UnsupportedSqlShape;
        break :blk parsed_expression.operands[0..1];
    } else parsed_expression.operands;
    if (path_exists_operands.len != array_expression.array_items.items.len) return error.UnsupportedSqlShape;
    for (path_exists_operands, array_expression.array_items.items) |path_exists, item_tokens| {
        if (path_exists.kind != .json_path_exists or path_exists.operands.len != 1) return error.UnsupportedSqlShape;
        if (item_tokens.end != item_tokens.start + 1 or tokens[item_tokens.start].kind != .string) return error.UnsupportedSqlShape;
        if (!std.mem.eql(u8, path_exists.json_path, tokens[item_tokens.start].text)) return error.UnsupportedSqlShape;
        try validateGeneratedRowExpressionIdentityStrict(
            tokens,
            left_tokens.start,
            left_tokens.end,
            path_exists.operands[0],
            generated_expression,
        );
    }
    return true;
}

fn validateGeneratedRowExpressionOperandsStrict(
    tokens: []const Token,
    parsed_expression: db_mod.types.RelationalRowsExpression,
    generated_expression: *const generated_parser.GeneratedSqlExpressionAst,
) anyerror!void {
    if (parsed_expression.operands.len == 0) return;
    if (parsed_expression.kind == .concat and generated_expression.kind == .string_concat) {
        try validateGeneratedConcatExpressionOperandsStrict(tokens, parsed_expression, generated_expression);
        return;
    }
    if (try validateGeneratedPositionFunctionOperandsStrict(tokens, parsed_expression, generated_expression)) return;
    if (try validateGeneratedJsonExtractPathFunctionOperandsStrict(tokens, parsed_expression, generated_expression)) return;
    if (try validateGeneratedJsonKeySetExpressionOperandsStrict(tokens, parsed_expression, generated_expression)) return;
    var expected_operand_count: usize = 0;
    if (generated_expression.left_tokens) |left_tokens| {
        if (parsed_expression.operands.len <= expected_operand_count) return error.UnsupportedSqlShape;
        try validateGeneratedRowExpressionIdentityStrict(
            tokens,
            left_tokens.start,
            left_tokens.end,
            parsed_expression.operands[expected_operand_count],
            generated_expression,
        );
        expected_operand_count += 1;
    }
    if (generated_expression.right_tokens) |right_tokens| {
        if (parsed_expression.operands.len <= expected_operand_count) return error.UnsupportedSqlShape;
        try validateGeneratedRowExpressionIdentityStrict(
            tokens,
            right_tokens.start,
            right_tokens.end,
            parsed_expression.operands[expected_operand_count],
            generated_expression,
        );
        expected_operand_count += 1;
    }
    if (generated_expression.kind == .function_call and generated_expression.argument_items.count != 0) {
        if (expected_operand_count != 0) return error.UnsupportedSqlShape;
        if (generated_expression.argument_items.count != parsed_expression.operands.len) return error.UnsupportedSqlShape;
        for (generated_expression.argument_items.items, 0..) |argument_tokens, index| {
            try validateGeneratedRowExpressionIdentityStrict(
                tokens,
                argument_tokens.start,
                argument_tokens.end,
                parsed_expression.operands[index],
                generated_expression,
            );
        }
        return;
    }
}

pub fn validateGeneratedRowExpressionIdentity(
    tokens: []const Token,
    start: usize,
    end: usize,
    parsed_expression: db_mod.types.RelationalRowsExpression,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
) !void {
    try validateGeneratedRowExpressionIdentityWithMode(tokens, start, end, parsed_expression, generated_expression_ast, false);
}

pub fn validateGeneratedRowExpressionIdentityStrict(
    tokens: []const Token,
    start: usize,
    end: usize,
    parsed_expression: db_mod.types.RelationalRowsExpression,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
) !void {
    try validateGeneratedRowExpressionIdentityWithMode(tokens, start, end, parsed_expression, generated_expression_ast, true);
}

fn validateGeneratedRowExpressionIdentityWithMode(
    tokens: []const Token,
    start: usize,
    end: usize,
    parsed_expression: db_mod.types.RelationalRowsExpression,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
    comptime require_exact_expression: bool,
) !void {
    const root = generated_expression_ast orelse return;
    const range: generated_parser.GeneratedSqlTokenRange = .{ .start = start, .end = end };
    const generated_expression = generatedExpressionForExactRange(root, range) orelse {
        if (require_exact_expression) return error.UnsupportedSqlShape;
        return;
    };
    try validateGeneratedExpressionPayloads(tokens, generated_expression.*);
    if (generated_expression.kind == .grouped) {
        const inner_expression = generated_expression.inner_expression orelse return error.UnsupportedSqlShape;
        const inner_tokens = inner_expression.tokens orelse return error.UnsupportedSqlShape;
        try validateGeneratedRowExpressionIdentityWithMode(tokens, inner_tokens.start, inner_tokens.end, parsed_expression, inner_expression, require_exact_expression);
        return;
    }
    switch (generated_expression.kind) {
        .function_call => {
            const token = try generatedExpressionFunctionNameToken(tokens, generated_expression.*);
            if (generatedFunctionNameMatchesRowExpressionKind(token, parsed_expression.kind)) |matches| {
                if (!matches and (require_exact_expression or token.kind != .identifier)) return error.UnsupportedSqlShape;
            }
        },
        else => {
            if (generatedOperatorKindMatchesRowExpressionKind(generated_expression.kind, parsed_expression)) |matches| {
                if (!matches and !generatedArithmeticOperatorKindMayRepresentRowExpressionKind(generated_expression.kind, parsed_expression.kind)) return error.UnsupportedSqlShape;
                const operator_tokens = generated_expression.operator_tokens orelse return error.UnsupportedSqlShape;
                try validateGeneratedExpressionOperatorTokens(tokens, generated_expression.kind, operator_tokens);
            } else if (require_exact_expression and !generatedLeafKindMayRepresentRowExpression(generated_expression.kind, parsed_expression)) {
                return error.UnsupportedSqlShape;
            }
        },
    }
    if (require_exact_expression) try validateGeneratedRowExpressionOperandsStrict(tokens, parsed_expression, generated_expression);
}

pub fn validateGeneratedExpressionConditionIdentity(
    tokens: []const Token,
    start: usize,
    end: usize,
    parsed_condition: db_mod.types.RelationalRowsExpressionCondition,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
) !void {
    try validateGeneratedExpressionConditionIdentityWithMode(tokens, start, end, parsed_condition, generated_expression_ast, false);
}

pub fn validateGeneratedExpressionConditionIdentityStrict(
    tokens: []const Token,
    start: usize,
    end: usize,
    parsed_condition: db_mod.types.RelationalRowsExpressionCondition,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
) !void {
    try validateGeneratedExpressionConditionIdentityWithMode(tokens, start, end, parsed_condition, generated_expression_ast, true);
}

fn validateGeneratedExpressionConditionIdentityWithMode(
    tokens: []const Token,
    start: usize,
    end: usize,
    parsed_condition: db_mod.types.RelationalRowsExpressionCondition,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
    comptime require_exact_child_expressions: bool,
) !void {
    const root = generated_expression_ast orelse return;
    const range: generated_parser.GeneratedSqlTokenRange = .{ .start = start, .end = end };
    const generated_expression = generatedExpressionForExactRange(root, range) orelse return error.UnsupportedSqlShape;
    const operator_tokens = generated_expression.operator_tokens orelse return error.UnsupportedSqlShape;
    try validateGeneratedRelationalPredicateExpression(generated_expression, tokens, operator_tokens.start, parsed_condition.op);
    const lhs_tokens = generated_expression.left_tokens orelse return error.UnsupportedSqlShape;
    if (require_exact_child_expressions) {
        try validateGeneratedRowExpressionIdentityStrict(tokens, lhs_tokens.start, lhs_tokens.end, parsed_condition.lhs, generated_expression);
    } else {
        try validateGeneratedRowExpressionIdentity(tokens, lhs_tokens.start, lhs_tokens.end, parsed_condition.lhs, generated_expression);
    }
    switch (parsed_condition.op) {
        .is_null, .is_not_null => {
            if (parsed_condition.rhs.len != 0 or generated_expression.right_tokens != null) return error.UnsupportedSqlShape;
        },
        else => {
            if (parsed_condition.rhs.len != 1) return error.UnsupportedSqlShape;
            const rhs_tokens = generated_expression.right_tokens orelse return error.UnsupportedSqlShape;
            if (require_exact_child_expressions) {
                try validateGeneratedRowExpressionIdentityStrict(tokens, rhs_tokens.start, rhs_tokens.end, parsed_condition.rhs[0], generated_expression);
            } else {
                try validateGeneratedRowExpressionIdentity(tokens, rhs_tokens.start, rhs_tokens.end, parsed_condition.rhs[0], generated_expression);
            }
        },
    }
}

pub fn validateGeneratedQuantifiedPredicateIdentity(
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
    tokens: []const Token,
    operator_token_index: usize,
    quantifier_token_index: usize,
) !void {
    const root = generated_expression_ast orelse return;
    const expression = generatedExpressionForOperatorStart(root, .quantified_comparison, operator_token_index) orelse return error.UnsupportedSqlShape;
    try validateGeneratedExpressionPayloadsIfRetained(tokens, expression.*);
    try rejectGeneratedUnsupportedSubqueryRightExpression(tokens, expression);
    const operator_tokens = expression.operator_tokens orelse return error.UnsupportedSqlShape;
    if (operator_token_index >= tokens.len or
        operator_tokens.start != operator_token_index or
        operator_tokens.end != operator_token_index + 1 or
        !expr_generated.generatedTokenKindIsComparisonOperator(tokens[operator_token_index].kind))
    {
        return error.UnsupportedSqlShape;
    }
    const quantifier_tokens = expression.quantifier_tokens orelse return error.UnsupportedSqlShape;
    if (quantifier_token_index >= tokens.len or
        quantifier_tokens.start != quantifier_token_index or
        quantifier_tokens.end != quantifier_token_index + 1 or
        !expr_token.tokenAtIsAnySomeOrAll(tokens, quantifier_token_index))
    {
        return error.UnsupportedSqlShape;
    }
    const right_tokens = expression.right_tokens orelse return error.UnsupportedSqlShape;
    if (operator_tokens.end != quantifier_tokens.start or quantifier_tokens.end != right_tokens.start) return error.UnsupportedSqlShape;
    try validateGeneratedChildExpressionPayloadsAllowUnknownKind(tokens, right_tokens, expression.right_expression_kind, expression.right_expression);
}

pub fn validateGeneratedExistsSubqueryPredicateIdentity(
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
    tokens: []const Token,
    operator_token_index: usize,
) !void {
    const expression = generated_expression_ast orelse return;
    if (expression.kind != .exists_subquery) return error.UnsupportedSqlShape;
    try validateGeneratedExpressionPayloads(tokens, expression.*);
    const operator_tokens = expression.operator_tokens orelse return error.UnsupportedSqlShape;
    if (operator_token_index >= tokens.len or
        operator_tokens.start != operator_token_index or
        operator_tokens.end != operator_token_index + 1 or
        !tokens[operator_token_index].matchesKeywordTag(.exists))
    {
        return error.UnsupportedSqlShape;
    }
}

pub fn validateGeneratedPatternPredicateIdentity(
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
    expected_kind: generated_parser.GeneratedSqlExpressionKind,
    tokens: []const Token,
    operator_token_index: usize,
    quantifier_token_index: ?usize,
) !void {
    const root = generated_expression_ast orelse return;
    const expression = generatedExpressionForOperatorStart(root, expected_kind, operator_token_index) orelse return error.UnsupportedSqlShape;
    try validateGeneratedExpressionPayloads(tokens, expression.*);
    try rejectGeneratedUnsupportedSubqueryRightExpression(tokens, expression);
    const operator_tokens = expression.operator_tokens orelse return error.UnsupportedSqlShape;
    if (operator_token_index >= tokens.len or
        operator_tokens.start != operator_token_index or
        operator_tokens.end != operator_token_index + 1)
    {
        return error.UnsupportedSqlShape;
    }
    switch (expected_kind) {
        .like, .not_like => if (!tokens[operator_token_index].matchesKeywordTag(.like)) return error.UnsupportedSqlShape,
        .ilike, .not_ilike => if (!tokens[operator_token_index].matchesKeywordTag(.ilike)) return error.UnsupportedSqlShape,
        else => return error.UnsupportedSqlShape,
    }
    if (quantifier_token_index) |index| {
        const quantifier_tokens = expression.quantifier_tokens orelse return error.UnsupportedSqlShape;
        if (index >= tokens.len or
            quantifier_tokens.start != index or
            quantifier_tokens.end != index + 1 or
            !expr_token.tokenAtIsAnySomeOrAll(tokens, index))
        {
            return error.UnsupportedSqlShape;
        }
        const right_tokens = expression.right_tokens orelse return error.UnsupportedSqlShape;
        if (operator_tokens.end != quantifier_tokens.start or quantifier_tokens.end != right_tokens.start) return error.UnsupportedSqlShape;
        try validateGeneratedPatternEscapePayload(tokens, expression, right_tokens);
    } else {
        if (expression.quantifier_tokens != null) return error.UnsupportedSqlShape;
        const right_tokens = expression.right_tokens orelse return error.UnsupportedSqlShape;
        if (operator_tokens.end != right_tokens.start) return error.UnsupportedSqlShape;
        try validateGeneratedPatternEscapePayload(tokens, expression, right_tokens);
    }
}

fn validateGeneratedPatternEscapePayload(
    tokens: []const Token,
    expression: *const generated_parser.GeneratedSqlExpressionAst,
    right_tokens: generated_parser.GeneratedSqlTokenRange,
) !void {
    const expression_tokens = expression.tokens orelse return error.UnsupportedSqlShape;
    if (expression.escape_tokens) |escape_tokens| {
        if (escape_tokens.start != right_tokens.end or
            escape_tokens.end <= escape_tokens.start or
            escape_tokens.end > expression_tokens.end or
            !tokens[escape_tokens.start].matchesKeywordTag(.escape))
        {
            return error.UnsupportedSqlShape;
        }
        const escape_expression_tokens: generated_parser.GeneratedSqlTokenRange = .{
            .start = escape_tokens.start + 1,
            .end = escape_tokens.end,
        };
        try validateGeneratedChildExpressionPayloadsAllowUnknownKind(tokens, escape_expression_tokens, expression.escape_expression_kind, expression.escape_expression);
    } else {
        if (expression.escape_expression_kind != null or expression.escape_expression != null) return error.UnsupportedSqlShape;
        if (right_tokens.end < expression_tokens.end and tokens[right_tokens.end].matchesKeywordTag(.escape)) return error.UnsupportedSqlShape;
    }
}

pub fn validateGeneratedExpressionPredicateKind(
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
    expected_kind: generated_parser.GeneratedSqlExpressionKind,
) !void {
    const expression = generated_expression_ast orelse return;
    if (expression.kind != expected_kind) return error.UnsupportedSqlShape;
}

fn validateGeneratedNullishPredicateKind(
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
    tokens: []const Token,
    operator_token_index: usize,
    op: runtime_schema.RelationalCheckOp,
) !void {
    const expression = generated_expression_ast orelse return;
    switch (op) {
        .is_null => switch (expression.kind) {
            .is_null, .is_unknown => {},
            else => return error.UnsupportedSqlShape,
        },
        .is_not_null => switch (expression.kind) {
            .is_not_null, .is_not_unknown => {},
            else => return error.UnsupportedSqlShape,
        },
        else => return error.UnsupportedSqlShape,
    }
    try validateGeneratedExpressionPayloadsIfRetained(tokens, expression.*);
    const operator_tokens = expression.operator_tokens orelse return error.UnsupportedSqlShape;
    if (operator_token_index >= tokens.len or operator_tokens.start != operator_token_index) return error.UnsupportedSqlShape;
    try validateGeneratedExpressionOperatorTokens(tokens, expression.kind, operator_tokens);
}

pub fn validateGeneratedBooleanIsPredicateExpression(
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
    tokens: []const Token,
    operator_token_index: usize,
    expected_kind: generated_parser.GeneratedSqlExpressionKind,
) !void {
    switch (expected_kind) {
        .is_true, .is_false, .is_not_true, .is_not_false => {},
        else => return error.UnsupportedSqlShape,
    }
    const root = generated_expression_ast orelse return;
    if (root.kind == expected_kind) {
        const root_operator_tokens = root.operator_tokens orelse return error.UnsupportedSqlShape;
        if (root_operator_tokens.start != operator_token_index) return error.UnsupportedSqlShape;
    }
    const expression = generatedExpressionForOperatorStart(root, expected_kind, operator_token_index) orelse return error.UnsupportedSqlShape;
    try validateGeneratedExpressionPayloadsIfRetained(tokens, expression.*);
    const operator_tokens = expression.operator_tokens orelse return error.UnsupportedSqlShape;
    if (operator_token_index >= tokens.len or operator_tokens.start != operator_token_index) return error.UnsupportedSqlShape;
    try validateGeneratedExpressionOperatorTokens(tokens, expected_kind, operator_tokens);
}

pub fn rejectGeneratedUnsupportedSubqueryPredicateExpression(
    tokens: []const Token,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
) !void {
    const expression = generated_expression_ast orelse return;
    switch (expression.kind) {
        .exists_subquery,
        .not_exists_subquery,
        => {
            try validateGeneratedExpressionPayloads(tokens, expression.*);
            return error.UnsupportedSqlShape;
        },
        .quantified_comparison => {
            if (expression.right_expression_kind == .subquery) {
                try rejectGeneratedUnsupportedSubqueryRightExpression(tokens, expression);
            }
        },
        else => {},
    }
}

pub fn rejectGeneratedUnsupportedSubqueryRightExpression(
    tokens: []const Token,
    expression: *const generated_parser.GeneratedSqlExpressionAst,
) !void {
    if (expression.right_expression_kind != .subquery) return;
    try validateGeneratedExpressionPayloads(tokens, expression.*);
    return error.UnsupportedSqlShape;
}

pub fn validateGeneratedIsTailPredicateExpression(
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
    tokens: []const Token,
    operator_token_index: usize,
    is_tail: expr_operator.ExpressionIsTail,
) !void {
    const root = generated_expression_ast orelse return;
    const expected_kind = generatedIsTailExpressionKind(is_tail);
    const expression = generatedExpressionForOperatorStart(root, expected_kind, operator_token_index) orelse return error.UnsupportedSqlShape;
    try validateGeneratedExpressionPayloadsIfRetained(tokens, expression.*);
    const operator_tokens = expression.operator_tokens orelse return error.UnsupportedSqlShape;
    if (operator_token_index >= tokens.len or operator_tokens.start != operator_token_index) return error.UnsupportedSqlShape;
    try validateGeneratedExpressionOperatorTokens(tokens, expected_kind, operator_tokens);
}

pub fn validateGeneratedPostfixNullPredicateExpression(
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
    tokens: []const Token,
    operator_token_index: usize,
    op: runtime_schema.RelationalCheckOp,
) !void {
    const root = generated_expression_ast orelse return;
    const expected_kind = generatedComparisonExpressionKindForOp(op);
    const expression = generatedExpressionForOperatorStart(root, expected_kind, operator_token_index) orelse return error.UnsupportedSqlShape;
    try validateGeneratedExpressionPayloadsIfRetained(tokens, expression.*);
    const operator_tokens = expression.operator_tokens orelse return error.UnsupportedSqlShape;
    if (operator_token_index >= tokens.len or
        operator_tokens.start != operator_token_index or
        operator_tokens.end != operator_token_index + 1)
    {
        return error.UnsupportedSqlShape;
    }
    try validateGeneratedExpressionOperatorTokens(tokens, expected_kind, operator_tokens);
}

pub fn betweenModifierTokenIndex(tokens: []const Token, index: usize) ?usize {
    if (index >= tokens.len) return null;
    if (tokens[index].matchesKeywordTag(.asymmetric) or tokens[index].matchesKeywordTag(.symmetric)) return index;
    return null;
}

pub fn validateGeneratedSingleOperatorPredicateIdentity(
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
    expected_kind: generated_parser.GeneratedSqlExpressionKind,
    tokens: []const Token,
    operator_token_index: usize,
) !void {
    const root = generated_expression_ast orelse return;
    const expression = generatedExpressionForOperatorStart(root, expected_kind, operator_token_index) orelse return error.UnsupportedSqlShape;
    try validateGeneratedExpressionPayloadsIfRetained(tokens, expression.*);
    try rejectGeneratedUnsupportedSubqueryRightExpression(tokens, expression);
    const operator_tokens = expression.operator_tokens orelse return error.UnsupportedSqlShape;
    if (operator_token_index >= tokens.len or
        operator_tokens.start != operator_token_index or
        operator_tokens.end != operator_token_index + 1)
    {
        return error.UnsupportedSqlShape;
    }
    try validateGeneratedExpressionOperatorTokens(tokens, expected_kind, operator_tokens);
    if (expression.negation_tokens != null or
        expression.quantifier_tokens != null or
        expression.between_modifier_tokens != null or
        expression.between_modifier != null)
    {
        return error.UnsupportedSqlShape;
    }
    const right_tokens = expression.right_tokens orelse return error.UnsupportedSqlShape;
    if (right_tokens.start >= right_tokens.end or right_tokens.end > tokens.len) return error.UnsupportedSqlShape;
    if (operator_tokens.end != right_tokens.start) return error.UnsupportedSqlShape;
    try validateGeneratedChildExpressionPayloadsAllowUnknownKind(tokens, right_tokens, expression.right_expression_kind, expression.right_expression);
}

pub fn validateGeneratedSetOrBetweenPredicateIdentity(
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
    expected_kind: generated_parser.GeneratedSqlExpressionKind,
    tokens: []const Token,
    operator_token_index: usize,
    negation_token_index: ?usize,
    between_modifier_token_index: ?usize,
) !void {
    const root = generated_expression_ast orelse return;
    const expression = generatedExpressionForOperatorStart(root, expected_kind, operator_token_index) orelse return error.UnsupportedSqlShape;
    try validateGeneratedExpressionPayloadsIfRetained(tokens, expression.*);
    const operator_tokens = expression.operator_tokens orelse return error.UnsupportedSqlShape;
    if (operator_token_index >= tokens.len or
        operator_tokens.start != operator_token_index or
        operator_tokens.end != operator_token_index + 1)
    {
        return error.UnsupportedSqlShape;
    }
    try validateGeneratedExpressionOperatorTokens(tokens, expected_kind, operator_tokens);

    switch (expected_kind) {
        .not_in_list, .not_between => {
            const index = negation_token_index orelse return error.UnsupportedSqlShape;
            const negation_tokens = expression.negation_tokens orelse return error.UnsupportedSqlShape;
            if (index >= tokens.len or
                negation_tokens.start != index or
                negation_tokens.end != index + 1 or
                negation_tokens.end != operator_tokens.start or
                !tokens[index].matchesKeywordTag(.not))
            {
                return error.UnsupportedSqlShape;
            }
        },
        .in_list, .between => {
            if (negation_token_index != null or expression.negation_tokens != null) return error.UnsupportedSqlShape;
        },
        else => return error.UnsupportedSqlShape,
    }

    switch (expected_kind) {
        .in_list, .not_in_list => {
            if (between_modifier_token_index != null or expression.between_modifier_tokens != null or expression.between_modifier != null) return error.UnsupportedSqlShape;
            const right_tokens = expression.right_tokens orelse return error.UnsupportedSqlShape;
            if (operator_tokens.end != right_tokens.start) return error.UnsupportedSqlShape;
            try validateGeneratedChildExpressionPayloadsAllowUnknownKind(tokens, right_tokens, expression.right_expression_kind, expression.right_expression);
        },
        .between, .not_between => {
            const lower_tokens = expression.between_lower_tokens orelse return error.UnsupportedSqlShape;
            const upper_tokens = expression.between_upper_tokens orelse return error.UnsupportedSqlShape;
            const lower_start = if (between_modifier_token_index) |index| blk: {
                const modifier_tokens = expression.between_modifier_tokens orelse return error.UnsupportedSqlShape;
                if (index >= tokens.len or
                    modifier_tokens.start != index or
                    modifier_tokens.end != index + 1 or
                    operator_tokens.end != modifier_tokens.start)
                {
                    return error.UnsupportedSqlShape;
                }
                const modifier = expression.between_modifier orelse return error.UnsupportedSqlShape;
                switch (modifier) {
                    .asymmetric => if (!tokens[index].matchesKeywordTag(.asymmetric)) return error.UnsupportedSqlShape,
                    .symmetric => if (!tokens[index].matchesKeywordTag(.symmetric)) return error.UnsupportedSqlShape,
                }
                break :blk modifier_tokens.end;
            } else blk: {
                if (expression.between_modifier_tokens != null or expression.between_modifier != null) return error.UnsupportedSqlShape;
                break :blk operator_tokens.end;
            };
            if (lower_start != lower_tokens.start or lower_tokens.end >= upper_tokens.start) return error.UnsupportedSqlShape;
            try validateGeneratedChildExpressionPayloadsAllowUnknownKind(tokens, lower_tokens, expression.between_lower_expression_kind, expression.between_lower_expression);
            try validateGeneratedChildExpressionPayloadsAllowUnknownKind(tokens, upper_tokens, expression.between_upper_expression_kind, expression.between_upper_expression);
        },
        else => return error.UnsupportedSqlShape,
    }
}

pub fn validateGeneratedRegexPredicateExpression(
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
    tokens: []const Token,
    operator_token_index: usize,
    case_insensitive: bool,
    negated: bool,
) !void {
    const root = generated_expression_ast orelse return;
    const expected_kind = generatedRegexPredicateKind(case_insensitive, negated);
    const expression = generatedExpressionForOperatorStart(root, expected_kind, operator_token_index) orelse return error.UnsupportedSqlShape;
    try validateGeneratedExpressionPayloadsIfRetained(tokens, expression.*);
    const operator_tokens = expression.operator_tokens orelse return error.UnsupportedSqlShape;
    if (operator_token_index >= tokens.len or
        operator_tokens.start != operator_token_index or
        operator_tokens.end != operator_token_index + 1)
    {
        return error.UnsupportedSqlShape;
    }
    const expected_token_kind: TokenKind = switch (expected_kind) {
        .regex_match => .regex_match,
        .regex_imatch => .regex_imatch,
        .regex_not_match => .regex_not_match,
        .regex_not_imatch => .regex_not_imatch,
        else => return error.UnsupportedSqlShape,
    };
    if (tokens[operator_token_index].kind != expected_token_kind) return error.UnsupportedSqlShape;
    try validateGeneratedExpressionOperatorTokens(tokens, expected_kind, operator_tokens);
    if (expression.negation_tokens != null or
        expression.quantifier_tokens != null or
        expression.between_modifier_tokens != null or
        expression.between_modifier != null or
        expression.escape_tokens != null or
        expression.escape_expression_kind != null or
        expression.escape_expression != null)
    {
        return error.UnsupportedSqlShape;
    }
    const right_tokens = expression.right_tokens orelse return error.UnsupportedSqlShape;
    if (right_tokens.start >= right_tokens.end or right_tokens.end > tokens.len) return error.UnsupportedSqlShape;
    if (operator_tokens.end != right_tokens.start) return error.UnsupportedSqlShape;
}

fn generatedRegexPredicateKind(
    case_insensitive: bool,
    negated: bool,
) generated_parser.GeneratedSqlExpressionKind {
    if (case_insensitive) {
        return if (negated) .regex_not_imatch else .regex_imatch;
    }
    return if (negated) .regex_not_match else .regex_match;
}

fn generatedIsTailExpressionKind(
    is_tail: expr_operator.ExpressionIsTail,
) generated_parser.GeneratedSqlExpressionKind {
    return switch (is_tail.kind) {
        .distinct_comparison => if (is_tail.op == .is_not_distinct) .is_not_distinct_from else .is_distinct_from,
        .null_test => if (is_tail.op == .is_not_null) .is_not_null else .is_null,
        .boolean_unknown => if (is_tail.op == .is_not_null) .is_not_unknown else .is_unknown,
        .boolean_literal => if (is_tail.boolean_negated)
            if (is_tail.boolean_value) .is_not_true else .is_not_false
        else if (is_tail.boolean_value)
            .is_true
        else
            .is_false,
    };
}

fn generatedComparisonExpressionKindForOp(
    op: runtime_schema.RelationalCheckOp,
) generated_parser.GeneratedSqlExpressionKind {
    return switch (op) {
        .is_null => .is_null,
        .is_not_null => .is_not_null,
        .is_distinct => .is_distinct_from,
        .is_not_distinct => .is_not_distinct_from,
        .eq, .ne, .gt, .gte, .lt, .lte => .comparison,
    };
}

fn tokenKindForComparisonOp(op: runtime_schema.RelationalCheckOp) !TokenKind {
    return switch (op) {
        .eq => .eq,
        .ne => .neq,
        .gt => .gt,
        .gte => .gte,
        .lt => .lt,
        .lte => .lte,
        else => error.UnsupportedSqlShape,
    };
}

pub fn validateGeneratedComparisonPredicateExpression(
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
    tokens: []const Token,
    operator_token_index: usize,
    op: runtime_schema.RelationalCheckOp,
) !void {
    const root = generated_expression_ast orelse return;
    const expression = generatedExpressionForOperatorStart(root, .comparison, operator_token_index) orelse {
        try validateRebasedGeneratedComparisonPredicateRoot(root, tokens, operator_token_index, op);
        return;
    };
    try validateGeneratedExpressionPayloadsIfRetained(tokens, expression.*);
    try rejectGeneratedUnsupportedSubqueryRightExpression(tokens, expression);
    const expected_token_kind = try tokenKindForComparisonOp(op);
    const operator_tokens = expression.operator_tokens orelse return error.UnsupportedSqlShape;
    if (operator_token_index >= tokens.len or
        operator_tokens.start != operator_token_index or
        operator_tokens.end != operator_token_index + 1 or
        tokens[operator_token_index].kind != expected_token_kind)
    {
        return error.UnsupportedSqlShape;
    }
    if (expression.negation_tokens != null or
        expression.quantifier_tokens != null or
        expression.between_modifier_tokens != null or
        expression.between_modifier != null)
    {
        return error.UnsupportedSqlShape;
    }
    const right_tokens = expression.right_tokens orelse return error.UnsupportedSqlShape;
    if (operator_tokens.end != right_tokens.start) return error.UnsupportedSqlShape;
    try validateGeneratedChildExpressionPayloadsAllowUnknownKind(tokens, right_tokens, expression.right_expression_kind, expression.right_expression);
}

fn validateRebasedGeneratedComparisonPredicateRoot(
    root: *const generated_parser.GeneratedSqlExpressionAst,
    tokens: []const Token,
    operator_token_index: usize,
    op: runtime_schema.RelationalCheckOp,
) !void {
    if (root.kind != .comparison or
        root.tokens != null or
        root.operator_tokens != null or
        root.left_tokens != null or
        root.negation_tokens != null or
        root.quantifier_tokens != null or
        root.between_modifier_tokens != null or
        root.between_modifier != null)
    {
        return error.UnsupportedSqlShape;
    }
    const right_tokens = root.right_tokens orelse return error.UnsupportedSqlShape;
    if (operator_token_index >= tokens.len or right_tokens.start != operator_token_index + 1 or right_tokens.start >= right_tokens.end or right_tokens.end > tokens.len) {
        return error.UnsupportedSqlShape;
    }
    const expected_token_kind = try tokenKindForComparisonOp(op);
    if (tokens[operator_token_index].kind != expected_token_kind) return error.UnsupportedSqlShape;
}

fn validateGeneratedDistinctPredicateExpression(
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
    tokens: []const Token,
    operator_token_index: usize,
    op: runtime_schema.RelationalCheckOp,
) !void {
    const expected_kind = generatedComparisonExpressionKindForOp(op);
    switch (expected_kind) {
        .is_distinct_from, .is_not_distinct_from => {},
        else => return error.UnsupportedSqlShape,
    }
    const root = generated_expression_ast orelse return;
    const expression = generatedExpressionForOperatorStart(root, expected_kind, operator_token_index) orelse return error.UnsupportedSqlShape;
    try validateGeneratedExpressionPayloadsIfRetained(tokens, expression.*);
    const operator_tokens = expression.operator_tokens orelse return error.UnsupportedSqlShape;
    if (operator_token_index >= tokens.len or operator_tokens.start != operator_token_index) return error.UnsupportedSqlShape;
    try validateGeneratedExpressionOperatorTokens(tokens, expected_kind, operator_tokens);
    const right_tokens = expression.right_tokens orelse return error.UnsupportedSqlShape;
    if (operator_tokens.end != right_tokens.start) return error.UnsupportedSqlShape;
    try validateGeneratedChildExpressionPayloadsAllowUnknownKind(tokens, right_tokens, expression.right_expression_kind, expression.right_expression);
}

pub fn validateGeneratedRelationalPredicateExpression(
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
    tokens: []const Token,
    operator_token_index: usize,
    op: runtime_schema.RelationalCheckOp,
) !void {
    switch (op) {
        .eq, .ne, .gt, .gte, .lt, .lte => try validateGeneratedComparisonPredicateExpression(generated_expression_ast, tokens, operator_token_index, op),
        .is_null, .is_not_null => try validateGeneratedNullishPredicateKind(generated_expression_ast, tokens, operator_token_index, op),
        .is_distinct, .is_not_distinct => try validateGeneratedDistinctPredicateExpression(generated_expression_ast, tokens, operator_token_index, op),
    }
}

pub fn validateGeneratedRelationalPredicateIdentity(
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
    tokens: []const Token,
    operator_token_index: usize,
    op: runtime_schema.RelationalCheckOp,
) !void {
    try validateGeneratedRelationalPredicateExpression(generated_expression_ast, tokens, operator_token_index, op);
}

pub fn validateGeneratedIsTailPredicateIdentity(
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
    tokens: []const Token,
    operator_token_index: usize,
    is_tail: expr_operator.ExpressionIsTail,
) !void {
    try validateGeneratedIsTailPredicateExpression(generated_expression_ast, tokens, operator_token_index, is_tail);
}

pub fn testGeneratedValidationChecksPredicateAndRowExpressionIdentity() !void {
    const regex_tokens = [_]Token{.{ .kind = .regex_not_imatch, .text = "!~*" }};
    const regex_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .regex_not_imatch,
        .operator_tokens = .{ .start = 0, .end = 1 },
        .right_tokens = .{ .start = 1, .end = 2 },
    };
    const regex_expression_tokens = [_]Token{
        .{ .kind = .identifier, .text = "status" },
        .{ .kind = .regex_not_imatch, .text = "!~*" },
        .{ .kind = .string, .text = "closed.*" },
    };
    var ranged_regex_right_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 2, .end = 3 },
    };
    const ranged_regex_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .regex_not_imatch,
        .tokens = .{ .start = 0, .end = 3 },
        .operator_tokens = .{ .start = 1, .end = 2 },
        .right_tokens = .{ .start = 2, .end = 3 },
        .right_expression_kind = .token_range,
        .right_expression = &ranged_regex_right_expression,
    };
    try validateGeneratedRegexPredicateExpression(&ranged_regex_expression, &regex_expression_tokens, 1, true, true);
    const stale_regex_range_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .regex_not_imatch,
        .tokens = .{ .start = 0, .end = 3 },
        .operator_tokens = .{ .start = 1, .end = 2 },
        .right_tokens = .{ .start = 3, .end = 2 },
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedRegexPredicateExpression(&stale_regex_range_expression, &regex_expression_tokens, 1, true, true),
    );
    const stale_regex_tokens = [_]Token{.{ .kind = .regex_match, .text = "~" }};
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedRegexPredicateExpression(&regex_expression, &stale_regex_tokens, 0, true, true),
    );
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedRegexPredicateExpression(&regex_expression, &regex_tokens, 0, false, false),
    );
    const regex_expression_without_right = generated_parser.GeneratedSqlExpressionAst{
        .kind = .regex_not_imatch,
        .operator_tokens = .{ .start = 0, .end = 1 },
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedRegexPredicateExpression(&regex_expression_without_right, &regex_tokens, 0, true, true),
    );

    const in_tokens = [_]Token{
        .{ .kind = .identifier, .text = "status" },
        .{ .kind = .identifier, .text = "in", .keyword = .in },
        .{ .kind = .lparen, .text = "(" },
        .{ .kind = .string, .text = "active" },
        .{ .kind = .rparen, .text = ")" },
    };
    var in_right_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 2, .end = 5 },
    };
    const in_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .in_list,
        .operator_tokens = .{ .start = 1, .end = 2 },
        .right_tokens = .{ .start = 2, .end = 5 },
        .right_expression_kind = .token_range,
        .right_expression = &in_right_expression,
    };
    try validateGeneratedSetOrBetweenPredicateIdentity(&in_expression, .in_list, &in_tokens, 1, null, null);
    const not_in_tokens = [_]Token{
        .{ .kind = .identifier, .text = "status" },
        .{ .kind = .identifier, .text = "not", .keyword = .not },
        .{ .kind = .identifier, .text = "in", .keyword = .in },
        .{ .kind = .lparen, .text = "(" },
        .{ .kind = .string, .text = "active" },
        .{ .kind = .rparen, .text = ")" },
    };
    var not_in_right_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 3, .end = 6 },
    };
    const not_in_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .not_in_list,
        .negation_tokens = .{ .start = 1, .end = 2 },
        .operator_tokens = .{ .start = 2, .end = 3 },
        .right_tokens = .{ .start = 3, .end = 6 },
        .right_expression_kind = .token_range,
        .right_expression = &not_in_right_expression,
    };
    try validateGeneratedSetOrBetweenPredicateIdentity(&not_in_expression, .not_in_list, &not_in_tokens, 2, 1, null);
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedSetOrBetweenPredicateIdentity(&not_in_expression, .not_in_list, &not_in_tokens, 2, null, null),
    );
    const between_tokens = [_]Token{
        .{ .kind = .identifier, .text = "amount" },
        .{ .kind = .identifier, .text = "between", .keyword = .between },
        .{ .kind = .identifier, .text = "symmetric", .keyword = .symmetric },
        .{ .kind = .number, .text = "1" },
        .{ .kind = .identifier, .text = "and", .keyword = .@"and" },
        .{ .kind = .number, .text = "3" },
    };
    var between_lower_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 3, .end = 4 },
    };
    var between_upper_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 5, .end = 6 },
    };
    const between_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .between,
        .operator_tokens = .{ .start = 1, .end = 2 },
        .between_modifier_tokens = .{ .start = 2, .end = 3 },
        .between_modifier = .symmetric,
        .between_lower_tokens = .{ .start = 3, .end = 4 },
        .between_lower_expression_kind = .token_range,
        .between_lower_expression = &between_lower_expression,
        .between_upper_tokens = .{ .start = 5, .end = 6 },
        .between_upper_expression_kind = .token_range,
        .between_upper_expression = &between_upper_expression,
    };
    try validateGeneratedSetOrBetweenPredicateIdentity(&between_expression, .between, &between_tokens, 1, null, 2);
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedSetOrBetweenPredicateIdentity(&between_expression, .between, &between_tokens, 1, null, null),
    );

    const contains_tokens = [_]Token{
        .{ .kind = .identifier, .text = "metadata" },
        .{ .kind = .at_contains, .text = "@>" },
        .{ .kind = .string, .text = "{\"status\":\"active\"}" },
    };
    var contains_right_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 2, .end = 3 },
    };
    const contains_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .contains,
        .operator_tokens = .{ .start = 1, .end = 2 },
        .right_tokens = .{ .start = 2, .end = 3 },
        .right_expression = &contains_right_expression,
    };
    try validateGeneratedSingleOperatorPredicateIdentity(&contains_expression, .contains, &contains_tokens, 1);
    const stale_contains_tokens = [_]Token{
        .{ .kind = .identifier, .text = "metadata" },
        .{ .kind = .range_overlap, .text = "&&" },
        .{ .kind = .string, .text = "{\"status\":\"active\"}" },
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedSingleOperatorPredicateIdentity(&contains_expression, .contains, &stale_contains_tokens, 1),
    );
    const stale_contains_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .contains,
        .operator_tokens = .{ .start = 2, .end = 3 },
        .right_tokens = .{ .start = 2, .end = 3 },
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedSingleOperatorPredicateIdentity(&stale_contains_expression, .contains, &contains_tokens, 1),
    );

    const generated_comparison_tokens = [_]Token{
        .{ .kind = .identifier, .text = "amount" },
        .{ .kind = .gte, .text = ">=" },
        .{ .kind = .number, .text = "10" },
    };
    var generated_comparison_right_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 2, .end = 3 },
    };
    const generated_comparison_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .comparison,
        .operator_tokens = .{ .start = 1, .end = 2 },
        .right_tokens = .{ .start = 2, .end = 3 },
        .right_expression_kind = .token_range,
        .right_expression = &generated_comparison_right_expression,
    };
    try validateGeneratedComparisonPredicateExpression(&generated_comparison_expression, &generated_comparison_tokens, 1, .gte);
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedComparisonPredicateExpression(&generated_comparison_expression, &generated_comparison_tokens, 1, .gt),
    );

    const distinct_tokens = [_]Token{
        .{ .kind = .identifier, .text = "status" },
        .{ .kind = .identifier, .text = "is", .keyword = .is },
        .{ .kind = .identifier, .text = "not", .keyword = .not },
        .{ .kind = .identifier, .text = "distinct", .keyword = .distinct },
        .{ .kind = .identifier, .text = "from", .keyword = .from },
        .{ .kind = .string, .text = "active" },
    };
    const distinct_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .is_not_distinct_from,
        .tokens = .{ .start = 0, .end = 6 },
        .operator_tokens = .{ .start = 1, .end = 5 },
    };
    try validateGeneratedIsTailPredicateExpression(&distinct_expression, &distinct_tokens, 1, .{
        .op = .is_not_distinct,
        .kind = .distinct_comparison,
    });
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedIsTailPredicateExpression(&distinct_expression, &distinct_tokens, 2, .{
            .op = .is_not_distinct,
            .kind = .distinct_comparison,
        }),
    );
    var relational_distinct_right_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 5, .end = 6 },
    };
    const relational_distinct_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .is_not_distinct_from,
        .operator_tokens = .{ .start = 1, .end = 5 },
        .right_tokens = .{ .start = 5, .end = 6 },
        .right_expression_kind = .token_range,
        .right_expression = &relational_distinct_right_expression,
    };
    try validateGeneratedRelationalPredicateIdentity(&relational_distinct_expression, &distinct_tokens, 1, .is_not_distinct);
    const stale_relational_distinct_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .is_not_distinct_from,
        .operator_tokens = .{ .start = 2, .end = 5 },
        .right_tokens = .{ .start = 5, .end = 6 },
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedRelationalPredicateIdentity(&stale_relational_distinct_expression, &distinct_tokens, 1, .is_not_distinct),
    );

    const postfix_null_tokens = [_]Token{
        .{ .kind = .identifier, .text = "status" },
        .{ .kind = .identifier, .text = "notnull", .keyword = .notnull },
    };
    const postfix_null_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .is_not_null,
        .tokens = .{ .start = 0, .end = 2 },
        .operator_tokens = .{ .start = 1, .end = 2 },
    };
    try validateGeneratedPostfixNullPredicateExpression(&postfix_null_expression, &postfix_null_tokens, 1, .is_not_null);
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedPostfixNullPredicateExpression(&postfix_null_expression, &postfix_null_tokens, 1, .is_null),
    );
    const infix_null_tokens = [_]Token{
        .{ .kind = .identifier, .text = "status" },
        .{ .kind = .identifier, .text = "is", .keyword = .is },
        .{ .kind = .identifier, .text = "null", .keyword = .null },
    };
    const infix_null_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .is_null,
        .tokens = .{ .start = 0, .end = 3 },
        .operator_tokens = .{ .start = 1, .end = 3 },
    };
    try validateGeneratedRelationalPredicateIdentity(&infix_null_expression, &infix_null_tokens, 1, .is_null);
    const stale_infix_null_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .is_null,
        .operator_tokens = .{ .start = 2, .end = 3 },
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedRelationalPredicateIdentity(&stale_infix_null_expression, &infix_null_tokens, 1, .is_null),
    );

    const boolean_is_not_tokens = [_]Token{
        .{ .kind = .identifier, .text = "enabled" },
        .{ .kind = .identifier, .text = "is", .keyword = .is },
        .{ .kind = .identifier, .text = "not", .keyword = .not },
        .{ .kind = .identifier, .text = "true", .keyword = .true },
    };
    const boolean_is_not_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .is_not_true,
        .operator_tokens = .{ .start = 1, .end = 4 },
    };
    try validateGeneratedBooleanIsPredicateExpression(&boolean_is_not_expression, &boolean_is_not_tokens, 1, .is_not_true);
    const stale_boolean_is_not_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .is_not_true,
        .operator_tokens = .{ .start = 2, .end = 4 },
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedBooleanIsPredicateExpression(&stale_boolean_is_not_expression, &boolean_is_not_tokens, 1, .is_not_true),
    );

    const generated_field_tokens = [_]Token{.{ .kind = .identifier, .text = "status" }};
    const generated_field_expression = db_mod.types.RelationalRowsExpression{ .kind = .field, .field = "status" };
    const generated_field_ast = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 0, .end = 1 },
    };
    try validateGeneratedRowExpressionIdentity(&generated_field_tokens, 0, 1, generated_field_expression, &generated_field_ast);
    const stale_generated_field_ast = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 1, .end = 1 },
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedRowExpressionIdentityStrict(&generated_field_tokens, 0, 1, generated_field_expression, &stale_generated_field_ast),
    );
}

test "sql expr generated validation checks predicate and row-expression identity" {
    try testGeneratedValidationChecksPredicateAndRowExpressionIdentity();
}
