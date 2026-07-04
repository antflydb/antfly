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
const value_mod = @import("../value.zig");

pub const Token = token_mod.Token;
pub const TokenKind = token_mod.TokenKind;

pub const StrictValidationContext = struct {
    alloc: ?std.mem.Allocator = null,
    params: []const value_mod.SqlValue = &.{},
};

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
    if (!std.meta.eql(expression, generated_parser.GeneratedSqlExpressionAst{})) return error.UnsupportedSqlShape;
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
    if (expression.kind == .grouped) try validateGeneratedGroupedExpressionPayloads(tokens, expression, expression_tokens);
    if (expression.operator_tokens != null) try validateGeneratedOperatorExpressionPayloads(tokens, expression, expression_tokens);
    if (expression.kind == .function_call) try validateGeneratedFunctionCallPayloads(tokens, expression, expression_tokens);
    if (expression.kind == .array_constructor) try validateGeneratedArrayConstructorPayloads(tokens, expression, expression_tokens);
    if (expression.kind == .cast) try validateGeneratedCastExpressionPayloads(tokens, expression, expression_tokens);
    if (expression.kind == .case_expression) try validateGeneratedCaseExpressionPayloads(tokens, expression, expression_tokens);
    if (expression.kind == .extract_expression) try validateGeneratedExtractExpressionPayloads(tokens, expression, expression_tokens);
    if (expression.kind == .interval_literal) try validateGeneratedIntervalLiteralPayloads(tokens, expression, expression_tokens);
    if (expression.kind == .timestamp_literal) try validateGeneratedTimestampLiteralPayloads(tokens, expression, expression_tokens);
    if (expression.kind == .current_timestamp) try validateGeneratedCurrentTimestampPayloads(tokens, expression, expression_tokens);
    if (expression.kind == .current_date) try validateGeneratedCurrentDatePayloads(tokens, expression, expression_tokens);
}

pub const GeneratedExpressionItem = struct {
    tokens: generated_parser.GeneratedSqlTokenRange,
    expression: *const generated_parser.GeneratedSqlExpressionAst,
    alias_tokens: ?generated_parser.GeneratedSqlTokenRange = null,
    alias_name_tokens: ?generated_parser.GeneratedSqlTokenRange = null,
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

fn validateGeneratedOptionalChildPayload(
    tokens: []const Token,
    range: ?generated_parser.GeneratedSqlTokenRange,
    kind: ?generated_parser.GeneratedSqlExpressionKind,
    expression: ?*const generated_parser.GeneratedSqlExpressionAst,
) !void {
    if (range) |child_tokens| {
        try validateGeneratedChildExpressionPayloadsAllowUnknownKind(tokens, child_tokens, kind, expression);
    } else if (kind != null or expression != null) {
        return error.UnsupportedSqlShape;
    }
}

fn validateGeneratedGroupedExpressionPayloads(
    tokens: []const Token,
    expression: generated_parser.GeneratedSqlExpressionAst,
    expression_tokens: generated_parser.GeneratedSqlTokenRange,
) !void {
    if (expression_tokens.end < expression_tokens.start + 3 or
        tokens[expression_tokens.start].kind != .lparen or
        tokens[expression_tokens.end - 1].kind != .rparen)
    {
        return error.UnsupportedSqlShape;
    }
    const inner_tokens = expression.inner_tokens orelse return error.UnsupportedSqlShape;
    if (inner_tokens.start != expression_tokens.start + 1 or
        inner_tokens.end != expression_tokens.end - 1 or
        inner_tokens.start >= inner_tokens.end)
    {
        return error.UnsupportedSqlShape;
    }
    try validateGeneratedChildExpressionPayloadsAllowUnknownKind(
        tokens,
        inner_tokens,
        expression.inner_expression_kind,
        expression.inner_expression,
    );
}

fn validateGeneratedOperatorExpressionPayloads(
    tokens: []const Token,
    expression: generated_parser.GeneratedSqlExpressionAst,
    expression_tokens: generated_parser.GeneratedSqlTokenRange,
) !void {
    try validateGeneratedOptionalChildPayload(tokens, expression.left_tokens, expression.left_expression_kind, expression.left_expression);
    try validateGeneratedOptionalChildPayload(tokens, expression.right_tokens, expression.right_expression_kind, expression.right_expression);
    try validateGeneratedOptionalChildPayload(tokens, expression.between_lower_tokens, expression.between_lower_expression_kind, expression.between_lower_expression);
    try validateGeneratedOptionalChildPayload(tokens, expression.between_upper_tokens, expression.between_upper_expression_kind, expression.between_upper_expression);
    if (expression.escape_tokens) |escape_tokens| {
        if (escape_tokens.start < expression_tokens.start or
            escape_tokens.end > expression_tokens.end or
            escape_tokens.end <= escape_tokens.start + 1 or
            !tokens[escape_tokens.start].matchesKeywordTag(.escape))
        {
            return error.UnsupportedSqlShape;
        }
        try validateGeneratedChildExpressionPayloadsAllowUnknownKind(
            tokens,
            .{ .start = escape_tokens.start + 1, .end = escape_tokens.end },
            expression.escape_expression_kind,
            expression.escape_expression,
        );
    } else if (expression.escape_expression_kind != null or expression.escape_expression != null) {
        return error.UnsupportedSqlShape;
    }

    if (expression.negation_tokens) |negation_tokens| {
        if (negation_tokens.start < expression_tokens.start or
            negation_tokens.end > expression_tokens.end or
            negation_tokens.end != negation_tokens.start + 1 or
            !tokens[negation_tokens.start].matchesKeywordTag(.not))
        {
            return error.UnsupportedSqlShape;
        }
    }
    if (expression.quantifier_tokens) |quantifier_tokens| {
        if (quantifier_tokens.start < expression_tokens.start or
            quantifier_tokens.end > expression_tokens.end or
            quantifier_tokens.end != quantifier_tokens.start + 1 or
            !expr_token.tokenAtIsAnySomeOrAll(tokens, quantifier_tokens.start))
        {
            return error.UnsupportedSqlShape;
        }
    }
    if (expression.between_modifier_tokens) |modifier_tokens| {
        if (modifier_tokens.start < expression_tokens.start or
            modifier_tokens.end > expression_tokens.end or
            modifier_tokens.end != modifier_tokens.start + 1)
        {
            return error.UnsupportedSqlShape;
        }
        if (tokens[modifier_tokens.start].matchesKeywordTag(.symmetric)) {
            if (expression.between_modifier != .symmetric) return error.UnsupportedSqlShape;
        } else if (tokens[modifier_tokens.start].matchesKeywordTag(.asymmetric)) {
            if (expression.between_modifier != .asymmetric) return error.UnsupportedSqlShape;
        } else {
            return error.UnsupportedSqlShape;
        }
    } else if (expression.between_modifier != null) {
        return error.UnsupportedSqlShape;
    }

    if (expression.boolean_condition_count != 0) {
        if (expression.boolean_condition_items.count != expression.boolean_condition_count) return error.UnsupportedSqlShape;
        try validateGeneratedExpressionRangeListPayloads(tokens, expression_tokens, expression.boolean_condition_items);
        const first_tokens = expression.boolean_first_condition_tokens orelse return error.UnsupportedSqlShape;
        const last_tokens = expression.boolean_last_condition_tokens orelse return error.UnsupportedSqlShape;
        if (!expr_generated.generatedTokenRangeEqual(expression.boolean_condition_items.items[0], first_tokens) or
            !expr_generated.generatedTokenRangeEqual(expression.boolean_condition_items.items[expression.boolean_condition_items.count - 1], last_tokens))
        {
            return error.UnsupportedSqlShape;
        }
        try validateGeneratedChildExpressionPayloadsAllowUnknownKind(
            tokens,
            first_tokens,
            expression.boolean_first_condition_kind,
            expression.boolean_first_condition,
        );
        try validateGeneratedChildExpressionPayloadsAllowUnknownKind(
            tokens,
            last_tokens,
            expression.boolean_last_condition_kind,
            expression.boolean_last_condition,
        );
    } else if (expression.boolean_first_condition_tokens != null or
        expression.boolean_first_condition_kind != null or
        expression.boolean_first_condition != null or
        expression.boolean_last_condition_tokens != null or
        expression.boolean_last_condition_kind != null or
        expression.boolean_last_condition != null or
        expression.boolean_condition_items.count != 0 or
        expression.boolean_condition_items.items.len != 0 or
        expression.boolean_condition_items.expressions.len != 0)
    {
        return error.UnsupportedSqlShape;
    }
}

fn validateGeneratedExpressionListPayloads(
    tokens: []const Token,
    range: generated_parser.GeneratedSqlTokenRange,
    list: generated_parser.GeneratedSqlListAst,
    allow_order_modifiers: bool,
) anyerror!void {
    if (range.start >= range.end or range.end > tokens.len) return error.UnsupportedSqlShape;
    if (list.count == 0) {
        if (list.first_tokens != null or
            list.last_tokens != null or
            list.items.len != 0 or
            list.expression_items.len != 0 or
            list.expressions.len != 0)
        {
            return error.UnsupportedSqlShape;
        }
        return;
    }
    if (list.items.len != list.count or
        list.expression_items.len != list.count or
        list.expressions.len != list.count or
        list.alias_items.len != list.count or
        list.alias_name_items.len != list.count or
        list.direction_items.len != list.count or
        list.directions.len != list.count or
        list.order_using_operator_items.len != list.count or
        list.nulls_order_items.len != list.count or
        list.nulls_orders.len != list.count)
    {
        return error.UnsupportedSqlShape;
    }
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
        try validateGeneratedExpressionPayloads(tokens, list.expressions[index]);

        if (!allow_order_modifiers) {
            if (list.direction_items[index] != null or
                list.directions[index] != null or
                list.order_using_operator_items[index] != null or
                list.nulls_order_items[index] != null or
                list.nulls_orders[index] != null)
            {
                return error.UnsupportedSqlShape;
            }
            if (!expr_generated.generatedTokenRangeEqual(expression_range, item)) return error.UnsupportedSqlShape;
            continue;
        }

        if (list.direction_items[index]) |direction_range| {
            if (direction_range.start < expression_range.end or direction_range.end > item.end or direction_range.start >= direction_range.end) return error.UnsupportedSqlShape;
            if (list.directions[index] == null) return error.UnsupportedSqlShape;
        } else if (list.directions[index] != null) {
            return error.UnsupportedSqlShape;
        }
        if (list.order_using_operator_items[index]) |operator_range| {
            if (operator_range.start < expression_range.end or operator_range.end > item.end or operator_range.start >= operator_range.end) return error.UnsupportedSqlShape;
        }
        if (list.nulls_order_items[index]) |nulls_range| {
            if (nulls_range.start < expression_range.end or nulls_range.end > item.end or nulls_range.start >= nulls_range.end) return error.UnsupportedSqlShape;
            if (list.nulls_orders[index] == null) return error.UnsupportedSqlShape;
        } else if (list.nulls_orders[index] != null) {
            return error.UnsupportedSqlShape;
        }
    }
}

fn validateGeneratedExpressionRangeListPayloads(
    tokens: []const Token,
    expression_tokens: generated_parser.GeneratedSqlTokenRange,
    list: generated_parser.GeneratedSqlListAst,
) anyerror!void {
    if (expression_tokens.start >= expression_tokens.end or expression_tokens.end > tokens.len) return error.UnsupportedSqlShape;
    if (list.count == 0) {
        if (list.first_tokens != null or
            list.last_tokens != null or
            list.items.len != 0 or
            list.expression_items.len != 0 or
            list.expressions.len != 0)
        {
            return error.UnsupportedSqlShape;
        }
        return;
    }
    if (list.items.len != list.count or
        list.expression_items.len != list.count or
        list.expressions.len != list.count or
        list.alias_items.len != 0 or
        list.alias_name_items.len != 0 or
        list.direction_items.len != 0 or
        list.directions.len != 0 or
        list.order_using_operator_items.len != 0 or
        list.nulls_order_items.len != 0 or
        list.nulls_orders.len != 0)
    {
        return error.UnsupportedSqlShape;
    }
    if (list.first_tokens == null or !expr_generated.generatedTokenRangeEqual(list.first_tokens.?, list.items[0])) return error.UnsupportedSqlShape;
    if (list.last_tokens == null or !expr_generated.generatedTokenRangeEqual(list.last_tokens.?, list.items[list.count - 1])) return error.UnsupportedSqlShape;

    for (list.items, 0..) |item, index| {
        if (item.start >= item.end or item.start < expression_tokens.start or item.end > expression_tokens.end) return error.UnsupportedSqlShape;
        if (!expr_generated.generatedTokenRangeEqual(list.expression_items[index], item)) return error.UnsupportedSqlShape;
        if (!expr_generated.generatedTokenRangeEqual(list.expressions[index].tokens orelse return error.UnsupportedSqlShape, item)) return error.UnsupportedSqlShape;
        try validateGeneratedExpressionPayloads(tokens, list.expressions[index]);
    }
}

fn validateGeneratedFunctionCallPayloads(
    tokens: []const Token,
    expression: generated_parser.GeneratedSqlExpressionAst,
    expression_tokens: generated_parser.GeneratedSqlTokenRange,
) anyerror!void {
    const name_tokens = expression.function_name_tokens orelse return error.UnsupportedSqlShape;
    if (name_tokens.start != expression_tokens.start or name_tokens.end != expression_tokens.start + 1 or name_tokens.end > tokens.len) return error.UnsupportedSqlShape;
    if (tokens[name_tokens.start].kind != .identifier) return error.UnsupportedSqlShape;
    if (name_tokens.end >= expression_tokens.end or tokens[name_tokens.end].kind != .lparen) return error.UnsupportedSqlShape;
    if (expression.argument_tokens) |argument_tokens| {
        if (argument_tokens.start != name_tokens.end + 1 or
            argument_tokens.start >= argument_tokens.end or
            argument_tokens.end >= expression_tokens.end or
            tokens[argument_tokens.end].kind != .rparen)
        {
            return error.UnsupportedSqlShape;
        }
    } else {
        const close_index = name_tokens.end + 1;
        if (close_index >= expression_tokens.end or tokens[close_index].kind != .rparen) return error.UnsupportedSqlShape;
    }

    if (expression.argument_items.count != 0) {
        const argument_value_tokens = expression.argument_value_tokens orelse return error.UnsupportedSqlShape;
        try validateGeneratedExpressionListPayloads(tokens, argument_value_tokens, expression.argument_items, false);
    } else if (expression.argument_items.items.len != 0 or expression.argument_items.expressions.len != 0) {
        return error.UnsupportedSqlShape;
    }
    if (expression.argument_order_items.count != 0) {
        const argument_order_tokens = expression.argument_order_tokens orelse return error.UnsupportedSqlShape;
        try validateGeneratedExpressionListPayloads(tokens, argument_order_tokens, expression.argument_order_items, true);
    } else if (expression.argument_order_items.items.len != 0 or expression.argument_order_items.expressions.len != 0) {
        return error.UnsupportedSqlShape;
    }
    if (expression.within_group_order_items.count != 0) {
        const within_group_order_tokens = expression.within_group_order_tokens orelse return error.UnsupportedSqlShape;
        try validateGeneratedExpressionListPayloads(tokens, within_group_order_tokens, expression.within_group_order_items, true);
    } else if (expression.within_group_order_items.items.len != 0 or expression.within_group_order_items.expressions.len != 0) {
        return error.UnsupportedSqlShape;
    }
    if (expression.filter_predicate_tokens) |filter_tokens| {
        try validateGeneratedChildExpressionPayloadsAllowUnknownKind(
            tokens,
            filter_tokens,
            expression.filter_expression_kind,
            expression.filter_expression,
        );
    } else if (expression.filter_expression != null or expression.filter_expression_kind != null) {
        return error.UnsupportedSqlShape;
    }
    if (expression.over_partition_items.count != 0) {
        const over_partition_tokens = expression.over_partition_tokens orelse return error.UnsupportedSqlShape;
        try validateGeneratedExpressionListPayloads(tokens, over_partition_tokens, expression.over_partition_items, false);
    } else if (expression.over_partition_items.items.len != 0 or expression.over_partition_items.expressions.len != 0) {
        return error.UnsupportedSqlShape;
    }
    if (expression.over_order_items.count != 0) {
        const over_order_tokens = expression.over_order_tokens orelse return error.UnsupportedSqlShape;
        try validateGeneratedExpressionListPayloads(tokens, over_order_tokens, expression.over_order_items, true);
    } else if (expression.over_order_items.items.len != 0 or expression.over_order_items.expressions.len != 0) {
        return error.UnsupportedSqlShape;
    }
    if (expression.over_frame_start_expression_tokens) |frame_start_tokens| {
        try validateGeneratedChildExpressionPayloadsAllowUnknownKind(
            tokens,
            frame_start_tokens,
            expression.over_frame_start_expression_kind,
            expression.over_frame_start_expression,
        );
    } else if (expression.over_frame_start_expression != null or expression.over_frame_start_expression_kind != null) {
        return error.UnsupportedSqlShape;
    }
    if (expression.over_frame_end_expression_tokens) |frame_end_tokens| {
        try validateGeneratedChildExpressionPayloadsAllowUnknownKind(
            tokens,
            frame_end_tokens,
            expression.over_frame_end_expression_kind,
            expression.over_frame_end_expression,
        );
    } else if (expression.over_frame_end_expression != null or expression.over_frame_end_expression_kind != null) {
        return error.UnsupportedSqlShape;
    }
}

fn generatedListIsEmpty(list: generated_parser.GeneratedSqlListAst) bool {
    return list.count == 0 and
        list.first_tokens == null and
        list.last_tokens == null and
        list.items.len == 0 and
        list.expression_items.len == 0 and
        list.alias_items.len == 0 and
        list.alias_name_items.len == 0 and
        list.direction_items.len == 0 and
        list.directions.len == 0 and
        list.order_using_operator_items.len == 0 and
        list.nulls_order_items.len == 0 and
        list.nulls_orders.len == 0 and
        list.expressions.len == 0;
}

fn validateGeneratedScalarFunctionHasNoAggregateOrWindowPayloads(
    expression: *const generated_parser.GeneratedSqlExpressionAst,
) !void {
    if (expression.kind != .function_call) return;
    if (expression.aggregate_function_kind != null or expression.window_function_kind != null) return error.UnsupportedSqlShape;
    if (expression.argument_order_tokens != null or !generatedListIsEmpty(expression.argument_order_items)) return error.UnsupportedSqlShape;
    if (expression.within_group_tokens != null or
        expression.within_group_order_tokens != null or
        !generatedListIsEmpty(expression.within_group_order_items))
    {
        return error.UnsupportedSqlShape;
    }
    if (expression.filter_tokens != null or
        expression.filter_predicate_tokens != null or
        expression.filter_expression_kind != null or
        expression.filter_expression != null)
    {
        return error.UnsupportedSqlShape;
    }
    if (expression.over_tokens != null or
        expression.over_name_tokens != null or
        expression.over_definition_tokens != null or
        expression.over_partition_tokens != null or
        !generatedListIsEmpty(expression.over_partition_items) or
        expression.over_order_tokens != null or
        !generatedListIsEmpty(expression.over_order_items) or
        expression.over_frame_tokens != null or
        expression.over_frame_unit != null or
        expression.over_frame_start_bound != null or
        expression.over_frame_start_expression_tokens != null or
        expression.over_frame_start_expression_kind != null or
        expression.over_frame_start_expression != null or
        expression.over_frame_end_bound != null or
        expression.over_frame_end_expression_tokens != null or
        expression.over_frame_end_expression_kind != null or
        expression.over_frame_end_expression != null)
    {
        return error.UnsupportedSqlShape;
    }
}

fn validateGeneratedCaseExpressionPayloads(
    tokens: []const Token,
    expression: generated_parser.GeneratedSqlExpressionAst,
    expression_tokens: generated_parser.GeneratedSqlTokenRange,
) anyerror!void {
    if (expression_tokens.end < expression_tokens.start + 5 or
        !tokens[expression_tokens.start].matchesKeywordTag(.case) or
        !tokens[expression_tokens.end - 1].matchesKeywordTag(.end))
    {
        return error.UnsupportedSqlShape;
    }
    if (expression.case_branch_count == 0) return error.UnsupportedSqlShape;

    const first_when_tokens = expression.case_first_when_tokens orelse return error.UnsupportedSqlShape;
    const last_when_tokens = expression.case_last_when_tokens orelse return error.UnsupportedSqlShape;
    const first_condition_tokens = expression.case_first_condition_tokens orelse return error.UnsupportedSqlShape;
    const first_result_tokens = expression.case_first_result_tokens orelse return error.UnsupportedSqlShape;
    if (first_when_tokens.start != expression_tokens.start + 1 or
        first_when_tokens.start >= first_when_tokens.end or
        first_when_tokens.end > expression_tokens.end - 1 or
        last_when_tokens.start < first_when_tokens.start or
        last_when_tokens.start >= last_when_tokens.end or
        last_when_tokens.end > expression_tokens.end - 1)
    {
        return error.UnsupportedSqlShape;
    }
    if (!tokens[first_when_tokens.start].matchesKeywordTag(.when) or
        !tokens[last_when_tokens.start].matchesKeywordTag(.when))
    {
        return error.UnsupportedSqlShape;
    }

    if (expression.case_condition_items.count != expression.case_branch_count or
        expression.case_result_items.count != expression.case_branch_count)
    {
        return error.UnsupportedSqlShape;
    }
    try validateGeneratedExpressionRangeListPayloads(tokens, expression_tokens, expression.case_condition_items);
    try validateGeneratedExpressionRangeListPayloads(tokens, expression_tokens, expression.case_result_items);
    if (!expr_generated.generatedTokenRangeEqual(expression.case_condition_items.items[0], first_condition_tokens)) return error.UnsupportedSqlShape;
    if (!expr_generated.generatedTokenRangeEqual(expression.case_result_items.items[0], first_result_tokens)) return error.UnsupportedSqlShape;

    try validateGeneratedChildExpressionPayloadsAllowUnknownKind(
        tokens,
        first_condition_tokens,
        expression.case_first_condition_kind,
        expression.case_first_condition,
    );
    try validateGeneratedChildExpressionPayloadsAllowUnknownKind(
        tokens,
        first_result_tokens,
        expression.case_first_result_kind,
        expression.case_first_result,
    );

    if (expression.case_else_tokens) |else_tokens| {
        const else_expression_tokens = expression.case_else_expression_tokens orelse return error.UnsupportedSqlShape;
        if (else_tokens.start < last_when_tokens.end or
            else_tokens.end != expression_tokens.end - 1 or
            else_tokens.start + 1 != else_expression_tokens.start or
            else_tokens.end != else_expression_tokens.end or
            else_expression_tokens.start >= else_expression_tokens.end or
            !tokens[else_tokens.start].matchesKeywordTag(.@"else"))
        {
            return error.UnsupportedSqlShape;
        }
        try validateGeneratedChildExpressionPayloadsAllowUnknownKind(
            tokens,
            else_expression_tokens,
            expression.case_else_expression_kind,
            expression.case_else_expression,
        );
    } else if (expression.case_else_expression_tokens != null or
        expression.case_else_expression_kind != null or
        expression.case_else_expression != null)
    {
        return error.UnsupportedSqlShape;
    }
}

fn validateGeneratedCastExpressionPayloads(
    tokens: []const Token,
    expression: generated_parser.GeneratedSqlExpressionAst,
    expression_tokens: generated_parser.GeneratedSqlTokenRange,
) anyerror!void {
    if (expression_tokens.end < expression_tokens.start + 6 or
        !tokens[expression_tokens.start].matchesKeywordTag(.cast) or
        tokens[expression_tokens.start + 1].kind != .lparen or
        tokens[expression_tokens.end - 1].kind != .rparen)
    {
        return error.UnsupportedSqlShape;
    }
    const cast_expression_tokens = expression.cast_expression_tokens orelse return error.UnsupportedSqlShape;
    const cast_type_tokens = expression.cast_type_tokens orelse return error.UnsupportedSqlShape;
    if (cast_expression_tokens.start != expression_tokens.start + 2 or
        cast_expression_tokens.end >= cast_type_tokens.start or
        cast_expression_tokens.start >= cast_expression_tokens.end or
        cast_type_tokens.end != expression_tokens.end - 1 or
        cast_type_tokens.start >= cast_type_tokens.end)
    {
        return error.UnsupportedSqlShape;
    }
    if (!tokens[cast_expression_tokens.end].matchesKeywordTag(.as)) return error.UnsupportedSqlShape;
    try validateGeneratedChildExpressionPayloadsAllowUnknownKind(
        tokens,
        cast_expression_tokens,
        expression.cast_expression_kind,
        expression.cast_expression,
    );
}

fn validateGeneratedExtractExpressionPayloads(
    tokens: []const Token,
    expression: generated_parser.GeneratedSqlExpressionAst,
    expression_tokens: generated_parser.GeneratedSqlTokenRange,
) anyerror!void {
    if (expression_tokens.end < expression_tokens.start + 6 or
        !tokens[expression_tokens.start].matchesKeywordTag(.extract) or
        tokens[expression_tokens.start + 1].kind != .lparen or
        tokens[expression_tokens.end - 1].kind != .rparen)
    {
        return error.UnsupportedSqlShape;
    }
    const field_tokens = expression.extract_field_tokens orelse return error.UnsupportedSqlShape;
    const source_tokens = expression.extract_source_tokens orelse return error.UnsupportedSqlShape;
    if (field_tokens.start < expression_tokens.start + 2 or
        field_tokens.end > expression_tokens.end - 3 or
        field_tokens.start >= field_tokens.end or
        source_tokens.start <= field_tokens.end or
        source_tokens.end > expression_tokens.end - 1 or
        source_tokens.start >= source_tokens.end)
    {
        return error.UnsupportedSqlShape;
    }
    if (!tokens[field_tokens.end].matchesKeywordTag(.from)) return error.UnsupportedSqlShape;
    try validateGeneratedChildExpressionPayloadsAllowUnknownKind(
        tokens,
        source_tokens,
        expression.extract_source_expression_kind,
        expression.extract_source_expression,
    );
}

fn validateGeneratedIntervalLiteralPayloads(
    tokens: []const Token,
    expression: generated_parser.GeneratedSqlExpressionAst,
    expression_tokens: generated_parser.GeneratedSqlTokenRange,
) !void {
    if (expression_tokens.end != expression_tokens.start + 2 or
        !tokens[expression_tokens.start].matchesKeywordTag(.interval) or
        tokens[expression_tokens.start + 1].kind != .string)
    {
        return error.UnsupportedSqlShape;
    }
    const value_tokens = expression.interval_value_tokens orelse return error.UnsupportedSqlShape;
    if (value_tokens.start != expression_tokens.start + 1 or value_tokens.end != expression_tokens.end) return error.UnsupportedSqlShape;
}

fn validateGeneratedTimestampLiteralPayloads(
    tokens: []const Token,
    expression: generated_parser.GeneratedSqlExpressionAst,
    expression_tokens: generated_parser.GeneratedSqlTokenRange,
) !void {
    if (expression_tokens.end != expression_tokens.start + 2 or
        (!tokens[expression_tokens.start].matchesKeywordTag(.timestamp) and
            !tokens[expression_tokens.start].matchesKeywordTag(.timestamptz)) or
        tokens[expression_tokens.start + 1].kind != .string)
    {
        return error.UnsupportedSqlShape;
    }
    const type_tokens = expression.timestamp_type_tokens orelse return error.UnsupportedSqlShape;
    const value_tokens = expression.timestamp_value_tokens orelse return error.UnsupportedSqlShape;
    if (type_tokens.start != expression_tokens.start or
        type_tokens.end != expression_tokens.start + 1 or
        value_tokens.start != expression_tokens.start + 1 or
        value_tokens.end != expression_tokens.end)
    {
        return error.UnsupportedSqlShape;
    }
}

fn validateGeneratedCurrentTimestampPayloads(
    tokens: []const Token,
    expression: generated_parser.GeneratedSqlExpressionAst,
    expression_tokens: generated_parser.GeneratedSqlTokenRange,
) !void {
    if (!tokens[expression_tokens.start].matchesKeywordTag(.current_timestamp)) return error.UnsupportedSqlShape;
    if (expression.current_timestamp_precision_tokens) |precision_tokens| {
        if (expression_tokens.end != expression_tokens.start + 4 or
            tokens[expression_tokens.start + 1].kind != .lparen or
            precision_tokens.start != expression_tokens.start + 2 or
            precision_tokens.end != expression_tokens.start + 3 or
            tokens[precision_tokens.start].kind != .number or
            tokens[expression_tokens.start + 3].kind != .rparen)
        {
            return error.UnsupportedSqlShape;
        }
    } else if (expression_tokens.end != expression_tokens.start + 1) {
        return error.UnsupportedSqlShape;
    }
}

fn validateGeneratedCurrentDatePayloads(
    tokens: []const Token,
    expression: generated_parser.GeneratedSqlExpressionAst,
    expression_tokens: generated_parser.GeneratedSqlTokenRange,
) !void {
    if (expression_tokens.end != expression_tokens.start + 1 or
        !tokens[expression_tokens.start].matchesKeywordTag(.current_date) or
        expression.current_timestamp_precision_tokens != null)
    {
        return error.UnsupportedSqlShape;
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
        .soundex => std.ascii.eqlIgnoreCase(token.text, "soundex"),
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
    context: StrictValidationContext,
    tokens: []const Token,
    generated_expression: *const generated_parser.GeneratedSqlExpressionAst,
    parsed_expression: db_mod.types.RelationalRowsExpression,
    parsed_operand_index: *usize,
) anyerror!void {
    if (generated_expression.kind == .string_concat) {
        const left_expression = generated_expression.left_expression orelse return error.UnsupportedSqlShape;
        try validateGeneratedConcatExpressionOperandStrict(context, tokens, left_expression, parsed_expression, parsed_operand_index);
        const right_expression = generated_expression.right_expression orelse return error.UnsupportedSqlShape;
        try validateGeneratedConcatExpressionOperandStrict(context, tokens, right_expression, parsed_expression, parsed_operand_index);
        return;
    }
    if (parsed_operand_index.* >= parsed_expression.operands.len) return error.UnsupportedSqlShape;
    const range = generated_expression.tokens orelse return error.UnsupportedSqlShape;
    try validateGeneratedRowExpressionIdentityStrictWithContext(
        context,
        tokens,
        range.start,
        range.end,
        parsed_expression.operands[parsed_operand_index.*],
        generated_expression,
    );
    parsed_operand_index.* += 1;
}

fn validateGeneratedConcatExpressionOperandsStrict(
    context: StrictValidationContext,
    tokens: []const Token,
    parsed_expression: db_mod.types.RelationalRowsExpression,
    generated_expression: *const generated_parser.GeneratedSqlExpressionAst,
) anyerror!void {
    var parsed_operand_index: usize = 0;
    try validateGeneratedConcatExpressionOperandStrict(context, tokens, generated_expression, parsed_expression, &parsed_operand_index);
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
    context: StrictValidationContext,
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
    try validateGeneratedRowExpressionIdentityStrictWithContext(
        context,
        tokens,
        separator + 1,
        argument_tokens.end,
        parsed_expression.operands[0],
        generated_expression,
    );
    try validateGeneratedRowExpressionIdentityStrictWithContext(
        context,
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
    context: StrictValidationContext,
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
    try validateGeneratedRowExpressionIdentityStrictWithContext(
        context,
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
    context: StrictValidationContext,
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
        try validateGeneratedRowExpressionIdentityStrictWithContext(
            context,
            tokens,
            left_tokens.start,
            left_tokens.end,
            path_exists.operands[0],
            generated_expression,
        );
    }
    return true;
}

fn validateGeneratedCastExpressionOperandsStrict(
    context: StrictValidationContext,
    tokens: []const Token,
    parsed_expression: db_mod.types.RelationalRowsExpression,
    generated_expression: *const generated_parser.GeneratedSqlExpressionAst,
) anyerror!bool {
    if (parsed_expression.kind != .cast or generated_expression.kind != .cast) return false;
    if (parsed_expression.operands.len != 1) return error.UnsupportedSqlShape;

    const cast_expression_tokens = generated_expression.cast_expression_tokens orelse return error.UnsupportedSqlShape;
    try validateGeneratedRowExpressionIdentityStrictWithContext(
        context,
        tokens,
        cast_expression_tokens.start,
        cast_expression_tokens.end,
        parsed_expression.operands[0],
        generated_expression,
    );

    const cast_type_tokens = generated_expression.cast_type_tokens orelse return error.UnsupportedSqlShape;
    var cast_type_pos = cast_type_tokens.start;
    const generated_cast_type = try expr_operator.parseExpressionCastType(tokens, &cast_type_pos);
    if (cast_type_pos != cast_type_tokens.end) return error.UnsupportedSqlShape;
    if (parsed_expression.cast_type == null or parsed_expression.cast_type.? != generated_cast_type) return error.UnsupportedSqlShape;
    return true;
}

fn validateGeneratedExtractExpressionOperandsStrict(
    context: StrictValidationContext,
    tokens: []const Token,
    parsed_expression: db_mod.types.RelationalRowsExpression,
    generated_expression: *const generated_parser.GeneratedSqlExpressionAst,
) anyerror!bool {
    if (parsed_expression.kind != .date_part or generated_expression.kind != .extract_expression) return false;
    if (parsed_expression.operands.len != 2) return error.UnsupportedSqlShape;

    const source_tokens = generated_expression.extract_source_tokens orelse return error.UnsupportedSqlShape;
    try validateGeneratedRowExpressionIdentityStrictWithContext(
        context,
        tokens,
        source_tokens.start,
        source_tokens.end,
        parsed_expression.operands[1],
        generated_expression,
    );
    return true;
}

fn validateGeneratedCaseExpressionOperandsStrict(
    context: StrictValidationContext,
    tokens: []const Token,
    parsed_expression: db_mod.types.RelationalRowsExpression,
    generated_expression: *const generated_parser.GeneratedSqlExpressionAst,
) anyerror!bool {
    if (parsed_expression.kind != .case or generated_expression.kind != .case_expression) return false;
    if (parsed_expression.case_branches.len == 0 or parsed_expression.case_else.len != 1) return error.UnsupportedSqlShape;
    if (generated_expression.case_branch_count != parsed_expression.case_branches.len) return error.UnsupportedSqlShape;
    if (generated_expression.case_condition_items.count != parsed_expression.case_branches.len or
        generated_expression.case_result_items.count != parsed_expression.case_branches.len)
    {
        return error.UnsupportedSqlShape;
    }

    for (parsed_expression.case_branches, 0..) |branch, index| {
        const condition_tokens = generated_expression.case_condition_items.items[index];
        try validateGeneratedExpressionConditionIdentityStrictWithContext(
            context,
            tokens,
            condition_tokens.start,
            condition_tokens.end,
            branch.when,
            generated_expression,
        );

        const result_tokens = generated_expression.case_result_items.items[index];
        try validateGeneratedRowExpressionIdentityStrictWithContext(
            context,
            tokens,
            result_tokens.start,
            result_tokens.end,
            branch.then,
            generated_expression,
        );
    }

    const else_tokens = generated_expression.case_else_expression_tokens orelse return error.UnsupportedSqlShape;
    try validateGeneratedRowExpressionIdentityStrictWithContext(
        context,
        tokens,
        else_tokens.start,
        else_tokens.end,
        parsed_expression.case_else[0],
        generated_expression,
    );
    return true;
}

fn jsonTopLevelArrayElementCount(value_json: []const u8) ?usize {
    var i: usize = 0;
    while (i < value_json.len and std.ascii.isWhitespace(value_json[i])) : (i += 1) {}
    if (i >= value_json.len or value_json[i] != '[') return null;
    i += 1;
    while (i < value_json.len and std.ascii.isWhitespace(value_json[i])) : (i += 1) {}
    if (i < value_json.len and value_json[i] == ']') {
        i += 1;
        while (i < value_json.len and std.ascii.isWhitespace(value_json[i])) : (i += 1) {}
        return if (i == value_json.len) 0 else null;
    }

    var count: usize = 0;
    while (i < value_json.len) {
        count += 1;
        var depth: usize = 0;
        var in_string = false;
        var escaped = false;
        var saw_value_byte = false;
        while (i < value_json.len) : (i += 1) {
            const ch = value_json[i];
            if (in_string) {
                saw_value_byte = true;
                if (escaped) {
                    escaped = false;
                } else if (ch == '\\') {
                    escaped = true;
                } else if (ch == '"') {
                    in_string = false;
                }
                continue;
            }
            switch (ch) {
                '"' => {
                    in_string = true;
                    saw_value_byte = true;
                },
                '[', '{' => {
                    depth += 1;
                    saw_value_byte = true;
                },
                ']', '}' => {
                    if (depth == 0) {
                        if (ch == ']') break;
                        return null;
                    }
                    depth -= 1;
                    saw_value_byte = true;
                },
                ',' => {
                    if (depth == 0) break;
                    saw_value_byte = true;
                },
                else => {
                    if (!std.ascii.isWhitespace(ch)) saw_value_byte = true;
                },
            }
        }
        if (!saw_value_byte or in_string or escaped) return null;
        while (i < value_json.len and std.ascii.isWhitespace(value_json[i])) : (i += 1) {}
        if (i >= value_json.len) return null;
        if (value_json[i] == ',') {
            i += 1;
            while (i < value_json.len and std.ascii.isWhitespace(value_json[i])) : (i += 1) {}
            if (i >= value_json.len or value_json[i] == ']') return null;
            continue;
        }
        if (value_json[i] == ']') {
            i += 1;
            while (i < value_json.len and std.ascii.isWhitespace(value_json[i])) : (i += 1) {}
            return if (i == value_json.len) count else null;
        }
        return null;
    }
    return null;
}

fn validateGeneratedArrayConstructorExpressionStrict(
    context: StrictValidationContext,
    tokens: []const Token,
    parsed_expression: db_mod.types.RelationalRowsExpression,
    generated_expression: *const generated_parser.GeneratedSqlExpressionAst,
) !bool {
    if (generated_expression.kind != .array_constructor or parsed_expression.kind != .value) return false;
    if (parsed_expression.operands.len != 0) return error.UnsupportedSqlShape;
    if (context.alloc) |alloc| {
        const expression_tokens = generated_expression.tokens orelse return error.UnsupportedSqlShape;
        var pos = expression_tokens.start;
        const generated_json = try value_mod.parseSqlArrayConstructorJsonAlloc(alloc, tokens, &pos, context.params);
        defer alloc.free(generated_json);
        if (pos != expression_tokens.end or !std.mem.eql(u8, parsed_expression.value_json, generated_json)) return error.UnsupportedSqlShape;
        return true;
    }
    const parsed_count = jsonTopLevelArrayElementCount(parsed_expression.value_json) orelse return error.UnsupportedSqlShape;
    if (parsed_count != generated_expression.array_items.count) return error.UnsupportedSqlShape;
    return true;
}

fn validateGeneratedIntervalLiteralExpressionStrict(
    tokens: []const Token,
    parsed_expression: db_mod.types.RelationalRowsExpression,
    generated_expression: *const generated_parser.GeneratedSqlExpressionAst,
) !bool {
    if (generated_expression.kind != .interval_literal) return false;
    if (parsed_expression.kind != .interval_ns and parsed_expression.kind != .interval_months) return false;
    if (parsed_expression.operands.len != 1 or parsed_expression.operands[0].kind != .value) return error.UnsupportedSqlShape;

    const expression_tokens = generated_expression.tokens orelse return error.UnsupportedSqlShape;
    var pos = expression_tokens.start;
    const interval = try value_mod.parseSqlIntervalLiteral(tokens, &pos);
    if (pos != expression_tokens.end) return error.UnsupportedSqlShape;

    const parsed_value = std.fmt.parseUnsigned(u64, parsed_expression.operands[0].value_json, 10) catch return error.UnsupportedSqlShape;
    const has_calendar = interval.calendar_months != 0 or (interval.saw_calendar and interval.fixed_ns == 0);
    const has_fixed = interval.fixed_ns != 0 or (interval.saw_fixed and !has_calendar);
    switch (parsed_expression.kind) {
        .interval_months => if (!has_calendar or parsed_value != interval.calendar_months) return error.UnsupportedSqlShape,
        .interval_ns => if (!has_fixed or parsed_value != interval.fixed_ns) return error.UnsupportedSqlShape,
        else => return error.UnsupportedSqlShape,
    }
    return true;
}

fn validateGeneratedTemporalLeafExpressionStrict(
    context: StrictValidationContext,
    tokens: []const Token,
    parsed_expression: db_mod.types.RelationalRowsExpression,
    generated_expression: *const generated_parser.GeneratedSqlExpressionAst,
) !bool {
    if (try validateGeneratedIntervalLiteralExpressionStrict(tokens, parsed_expression, generated_expression)) return true;
    switch (generated_expression.kind) {
        .timestamp_literal => {
            if (parsed_expression.kind != .value or parsed_expression.operands.len != 0) return error.UnsupportedSqlShape;
            if (context.alloc) |alloc| {
                const expression_tokens = generated_expression.tokens orelse return error.UnsupportedSqlShape;
                var pos = expression_tokens.start;
                const generated_json = try value_mod.parseSqlTypedDatetimeLiteralValueJsonAlloc(alloc, tokens, &pos);
                defer alloc.free(generated_json);
                if (pos != expression_tokens.end or !std.mem.eql(u8, parsed_expression.value_json, generated_json)) return error.UnsupportedSqlShape;
            }
            return true;
        },
        .current_timestamp => {
            if (parsed_expression.kind != .now or parsed_expression.operands.len != 0) return error.UnsupportedSqlShape;
            return true;
        },
        .current_date => {
            if (parsed_expression.kind != .date_trunc or parsed_expression.operands.len != 2) return error.UnsupportedSqlShape;
            if (parsed_expression.operands[0].kind != .value or
                !std.mem.eql(u8, parsed_expression.operands[0].value_json, "\"day\"") or
                parsed_expression.operands[1].kind != .now)
            {
                return error.UnsupportedSqlShape;
            }
            return true;
        },
        else => return false,
    }
}

fn generatedConditionBooleanRhs(
    parsed_condition: db_mod.types.RelationalRowsExpressionCondition,
) ?bool {
    if (parsed_condition.op != .eq or parsed_condition.rhs.len != 1) return null;
    const rhs = parsed_condition.rhs[0];
    if (rhs.kind != .value) return null;
    if (std.mem.eql(u8, rhs.value_json, "true")) return true;
    if (std.mem.eql(u8, rhs.value_json, "false")) return false;
    return null;
}

const JsonStringByteCursor = struct {
    json: []const u8,
    index: usize = 1,

    fn init(json: []const u8) ?JsonStringByteCursor {
        if (json.len < 2 or json[0] != '"' or json[json.len - 1] != '"') return null;
        return .{ .json = json };
    }

    fn next(self: *JsonStringByteCursor) ?u8 {
        if (self.index >= self.json.len - 1) return null;
        const byte = self.json[self.index];
        self.index += 1;
        if (byte != '\\') return byte;
        if (self.index >= self.json.len - 1) return null;
        const escaped = self.json[self.index];
        self.index += 1;
        return switch (escaped) {
            '"', '\\', '/' => escaped,
            'b' => 0x08,
            'f' => 0x0c,
            'n' => '\n',
            'r' => '\r',
            't' => '\t',
            else => null,
        };
    }

    fn expect(self: *JsonStringByteCursor, expected: u8) bool {
        return (self.next() orelse return false) == expected;
    }

    fn finish(self: JsonStringByteCursor) bool {
        return self.index == self.json.len - 1;
    }
};

fn normalizedSqlLikePatternEqualsJson(pattern: []const u8, escape: []const u8, parsed_pattern_json: []const u8) !bool {
    if (escape.len != 1) return error.UnsupportedSqlShape;
    var cursor = JsonStringByteCursor.init(parsed_pattern_json) orelse return false;
    const escape_char = escape[0];
    if (escape_char == '\\') {
        for (pattern) |byte| {
            if (!cursor.expect(byte)) return false;
        }
        return cursor.finish();
    }

    var index: usize = 0;
    while (index < pattern.len) {
        const byte = pattern[index];
        if (byte == escape_char) {
            if (index + 1 >= pattern.len) return error.UnsupportedSqlShape;
            if (!cursor.expect('\\') or !cursor.expect(pattern[index + 1])) return false;
            index += 2;
            continue;
        }
        if (byte == '\\') {
            if (!cursor.expect('\\') or !cursor.expect('\\')) return false;
            index += 1;
            continue;
        }
        if (!cursor.expect(byte)) return false;
        index += 1;
    }
    return cursor.finish();
}

fn singleStringLiteralTokenRange(tokens: []const Token, range: generated_parser.GeneratedSqlTokenRange) ?[]const u8 {
    if (range.end != range.start + 1 or range.end > tokens.len) return null;
    const token = tokens[range.start];
    if (token.kind != .string) return null;
    return token.text;
}

fn parseGeneratedStringPayloadAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    range: generated_parser.GeneratedSqlTokenRange,
    params: []const value_mod.SqlValue,
) ![]const u8 {
    if (range.start > range.end or range.end > tokens.len) return error.UnsupportedSqlShape;
    var pos: usize = 0;
    const value = try value_mod.parseSqlStringValueAlloc(alloc, tokens[range.start..range.end], &pos, params);
    errdefer alloc.free(value);
    if (pos != range.end - range.start) return error.UnsupportedSqlShape;
    return value;
}

fn validateGeneratedPatternEscapeConditionStrict(
    context: StrictValidationContext,
    tokens: []const Token,
    parsed_pattern_json: []const u8,
    generated_expression: *const generated_parser.GeneratedSqlExpressionAst,
    right_tokens: generated_parser.GeneratedSqlTokenRange,
) !void {
    const escape_tokens = generated_expression.escape_tokens orelse return error.UnsupportedSqlShape;
    if (escape_tokens.start + 1 >= escape_tokens.end or escape_tokens.end > tokens.len) return error.UnsupportedSqlShape;
    const escape_expression_tokens: generated_parser.GeneratedSqlTokenRange = .{
        .start = escape_tokens.start + 1,
        .end = escape_tokens.end,
    };

    if (context.alloc) |alloc| {
        const pattern = try parseGeneratedStringPayloadAlloc(alloc, tokens, right_tokens, context.params);
        defer alloc.free(pattern);
        const escape = try parseGeneratedStringPayloadAlloc(alloc, tokens, escape_expression_tokens, context.params);
        defer alloc.free(escape);
        if (!try normalizedSqlLikePatternEqualsJson(pattern, escape, parsed_pattern_json)) return error.UnsupportedSqlShape;
        return;
    }

    const pattern = singleStringLiteralTokenRange(tokens, right_tokens) orelse return error.UnsupportedSqlShape;
    const escape = singleStringLiteralTokenRange(tokens, escape_expression_tokens) orelse return error.UnsupportedSqlShape;
    if (!try normalizedSqlLikePatternEqualsJson(pattern, escape, parsed_pattern_json)) return error.UnsupportedSqlShape;
}

fn validateGeneratedPatternBooleanConditionStrict(
    context: StrictValidationContext,
    tokens: []const Token,
    parsed_condition: db_mod.types.RelationalRowsExpressionCondition,
    generated_expression: *const generated_parser.GeneratedSqlExpressionAst,
    expected_condition_value: bool,
    expected_expression_kind: db_mod.types.RelationalRowsExpressionKind,
) !void {
    const condition_value = generatedConditionBooleanRhs(parsed_condition) orelse return error.UnsupportedSqlShape;
    if (condition_value != expected_condition_value) return error.UnsupportedSqlShape;
    if (parsed_condition.lhs.kind != expected_expression_kind or parsed_condition.lhs.operands.len != 2) return error.UnsupportedSqlShape;

    const left_tokens = generated_expression.left_tokens orelse return error.UnsupportedSqlShape;
    try validateGeneratedRowExpressionIdentityStrictWithContext(
        context,
        tokens,
        left_tokens.start,
        left_tokens.end,
        parsed_condition.lhs.operands[0],
        generated_expression,
    );
    const right_tokens = generated_expression.right_tokens orelse return error.UnsupportedSqlShape;
    if (generated_expression.escape_tokens != null) {
        try validateGeneratedPatternEscapeConditionStrict(context, tokens, parsed_condition.lhs.operands[1].value_json, generated_expression, right_tokens);
    } else {
        try validateGeneratedRowExpressionIdentityStrictWithContext(
            context,
            tokens,
            right_tokens.start,
            right_tokens.end,
            parsed_condition.lhs.operands[1],
            generated_expression,
        );
    }
}

fn validateGeneratedRegexBooleanConditionStrict(
    context: StrictValidationContext,
    tokens: []const Token,
    parsed_condition: db_mod.types.RelationalRowsExpressionCondition,
    generated_expression: *const generated_parser.GeneratedSqlExpressionAst,
    expected_condition_value: bool,
    expected_case_insensitive: bool,
) !void {
    const condition_value = generatedConditionBooleanRhs(parsed_condition) orelse return error.UnsupportedSqlShape;
    if (condition_value != expected_condition_value) return error.UnsupportedSqlShape;
    if (parsed_condition.lhs.kind != .regexp_match or parsed_condition.lhs.operands.len != 3) return error.UnsupportedSqlShape;
    if (parsed_condition.lhs.operands[2].kind != .value or
        !std.mem.eql(u8, parsed_condition.lhs.operands[2].value_json, value_mod.booleanJson(expected_case_insensitive)))
    {
        return error.UnsupportedSqlShape;
    }

    const left_tokens = generated_expression.left_tokens orelse return error.UnsupportedSqlShape;
    try validateGeneratedRowExpressionIdentityStrictWithContext(
        context,
        tokens,
        left_tokens.start,
        left_tokens.end,
        parsed_condition.lhs.operands[0],
        generated_expression,
    );
    const right_tokens = generated_expression.right_tokens orelse return error.UnsupportedSqlShape;
    try validateGeneratedRowExpressionIdentityStrictWithContext(
        context,
        tokens,
        right_tokens.start,
        right_tokens.end,
        parsed_condition.lhs.operands[1],
        generated_expression,
    );
}

fn validateGeneratedBooleanOperatorConditionStrict(
    context: StrictValidationContext,
    tokens: []const Token,
    parsed_condition: db_mod.types.RelationalRowsExpressionCondition,
    generated_expression: *const generated_parser.GeneratedSqlExpressionAst,
) !bool {
    try validateGeneratedExpressionPayloadsIfRetained(tokens, generated_expression.*);
    switch (generated_expression.kind) {
        .like => {
            try validateGeneratedPatternBooleanConditionStrict(context, tokens, parsed_condition, generated_expression, true, .like);
            return true;
        },
        .not_like => {
            try validateGeneratedPatternBooleanConditionStrict(context, tokens, parsed_condition, generated_expression, false, .like);
            return true;
        },
        .ilike => {
            try validateGeneratedPatternBooleanConditionStrict(context, tokens, parsed_condition, generated_expression, true, .ilike);
            return true;
        },
        .not_ilike => {
            try validateGeneratedPatternBooleanConditionStrict(context, tokens, parsed_condition, generated_expression, false, .ilike);
            return true;
        },
        .regex_match => {
            try validateGeneratedRegexBooleanConditionStrict(context, tokens, parsed_condition, generated_expression, true, false);
            return true;
        },
        .regex_not_match => {
            try validateGeneratedRegexBooleanConditionStrict(context, tokens, parsed_condition, generated_expression, false, false);
            return true;
        },
        .regex_imatch => {
            try validateGeneratedRegexBooleanConditionStrict(context, tokens, parsed_condition, generated_expression, true, true);
            return true;
        },
        .regex_not_imatch => {
            try validateGeneratedRegexBooleanConditionStrict(context, tokens, parsed_condition, generated_expression, false, true);
            return true;
        },
        else => return false,
    }
}

fn validateGeneratedRowExpressionOperandsStrict(
    context: StrictValidationContext,
    tokens: []const Token,
    parsed_expression: db_mod.types.RelationalRowsExpression,
    generated_expression: *const generated_parser.GeneratedSqlExpressionAst,
) anyerror!void {
    if (try validateGeneratedCaseExpressionOperandsStrict(context, tokens, parsed_expression, generated_expression)) return;
    if (try validateGeneratedArrayConstructorExpressionStrict(context, tokens, parsed_expression, generated_expression)) return;
    if (try validateGeneratedTemporalLeafExpressionStrict(context, tokens, parsed_expression, generated_expression)) return;
    if (parsed_expression.operands.len == 0) return;
    if (parsed_expression.kind == .concat and generated_expression.kind == .string_concat) {
        try validateGeneratedConcatExpressionOperandsStrict(context, tokens, parsed_expression, generated_expression);
        return;
    }
    if (try validateGeneratedPositionFunctionOperandsStrict(context, tokens, parsed_expression, generated_expression)) return;
    if (try validateGeneratedJsonExtractPathFunctionOperandsStrict(context, tokens, parsed_expression, generated_expression)) return;
    if (try validateGeneratedJsonKeySetExpressionOperandsStrict(context, tokens, parsed_expression, generated_expression)) return;
    if (try validateGeneratedCastExpressionOperandsStrict(context, tokens, parsed_expression, generated_expression)) return;
    if (try validateGeneratedExtractExpressionOperandsStrict(context, tokens, parsed_expression, generated_expression)) return;
    var expected_operand_count: usize = 0;
    if (generated_expression.left_tokens) |left_tokens| {
        if (parsed_expression.operands.len <= expected_operand_count) return error.UnsupportedSqlShape;
        try validateGeneratedRowExpressionIdentityStrictWithContext(
            context,
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
        try validateGeneratedRowExpressionIdentityStrictWithContext(
            context,
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
            try validateGeneratedRowExpressionIdentityStrictWithContext(
                context,
                tokens,
                argument_tokens.start,
                argument_tokens.end,
                parsed_expression.operands[index],
                generated_expression,
            );
        }
        return;
    }
    if (expected_operand_count != 0 and expected_operand_count != parsed_expression.operands.len) return error.UnsupportedSqlShape;
}

pub fn validateGeneratedRowExpressionIdentity(
    tokens: []const Token,
    start: usize,
    end: usize,
    parsed_expression: db_mod.types.RelationalRowsExpression,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
) !void {
    try validateGeneratedRowExpressionIdentityWithMode(.{}, tokens, start, end, parsed_expression, generated_expression_ast, false);
}

pub fn validateGeneratedRowExpressionIdentityStrict(
    tokens: []const Token,
    start: usize,
    end: usize,
    parsed_expression: db_mod.types.RelationalRowsExpression,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
) !void {
    try validateGeneratedRowExpressionIdentityWithMode(.{}, tokens, start, end, parsed_expression, generated_expression_ast, true);
}

pub fn validateGeneratedRowExpressionIdentityStrictWithContext(
    context: StrictValidationContext,
    tokens: []const Token,
    start: usize,
    end: usize,
    parsed_expression: db_mod.types.RelationalRowsExpression,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
) !void {
    try validateGeneratedRowExpressionIdentityWithMode(context, tokens, start, end, parsed_expression, generated_expression_ast, true);
}

fn validateGeneratedRowExpressionIdentityWithMode(
    context: StrictValidationContext,
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
        try validateGeneratedRowExpressionIdentityWithMode(context, tokens, inner_tokens.start, inner_tokens.end, parsed_expression, inner_expression, require_exact_expression);
        return;
    }
    switch (generated_expression.kind) {
        .function_call => {
            if (require_exact_expression) try validateGeneratedScalarFunctionHasNoAggregateOrWindowPayloads(generated_expression);
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
    if (require_exact_expression) try validateGeneratedRowExpressionOperandsStrict(context, tokens, parsed_expression, generated_expression);
}

pub fn validateGeneratedExpressionConditionIdentity(
    tokens: []const Token,
    start: usize,
    end: usize,
    parsed_condition: db_mod.types.RelationalRowsExpressionCondition,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
) !void {
    try validateGeneratedExpressionConditionIdentityWithMode(.{}, tokens, start, end, parsed_condition, generated_expression_ast, false);
}

pub fn validateGeneratedExpressionConditionIdentityStrict(
    tokens: []const Token,
    start: usize,
    end: usize,
    parsed_condition: db_mod.types.RelationalRowsExpressionCondition,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
) !void {
    try validateGeneratedExpressionConditionIdentityWithMode(.{}, tokens, start, end, parsed_condition, generated_expression_ast, true);
}

pub fn validateGeneratedExpressionConditionIdentityStrictWithContext(
    context: StrictValidationContext,
    tokens: []const Token,
    start: usize,
    end: usize,
    parsed_condition: db_mod.types.RelationalRowsExpressionCondition,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
) !void {
    try validateGeneratedExpressionConditionIdentityWithMode(context, tokens, start, end, parsed_condition, generated_expression_ast, true);
}

fn validateGeneratedExpressionConditionIdentityWithMode(
    context: StrictValidationContext,
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
    if (require_exact_child_expressions and try validateGeneratedBooleanOperatorConditionStrict(context, tokens, parsed_condition, generated_expression)) return;
    const operator_tokens = generated_expression.operator_tokens orelse return error.UnsupportedSqlShape;
    const boolean_literal_tail = generatedBooleanLiteralIsTailForCondition(parsed_condition);
    if (boolean_literal_tail) |is_tail| {
        try validateGeneratedIsTailPredicateExpression(generated_expression, tokens, operator_tokens.start, is_tail);
    } else {
        try validateGeneratedRelationalPredicateExpression(generated_expression, tokens, operator_tokens.start, parsed_condition.op);
    }
    const lhs_tokens = generated_expression.left_tokens orelse return error.UnsupportedSqlShape;
    if (require_exact_child_expressions) {
        try validateGeneratedRowExpressionIdentityStrictWithContext(context, tokens, lhs_tokens.start, lhs_tokens.end, parsed_condition.lhs, generated_expression);
    } else {
        try validateGeneratedRowExpressionIdentity(tokens, lhs_tokens.start, lhs_tokens.end, parsed_condition.lhs, generated_expression);
    }
    switch (parsed_condition.op) {
        .is_null, .is_not_null => {
            if (parsed_condition.rhs.len != 0 or generated_expression.right_tokens != null) return error.UnsupportedSqlShape;
        },
        else => {
            if (boolean_literal_tail != null) {
                if (generated_expression.right_tokens) |rhs_tokens| {
                    if (require_exact_child_expressions) {
                        try validateGeneratedRowExpressionIdentityStrictWithContext(context, tokens, rhs_tokens.start, rhs_tokens.end, parsed_condition.rhs[0], generated_expression);
                    } else {
                        try validateGeneratedRowExpressionIdentity(tokens, rhs_tokens.start, rhs_tokens.end, parsed_condition.rhs[0], generated_expression);
                    }
                }
                return;
            }
            if (parsed_condition.rhs.len != 1) return error.UnsupportedSqlShape;
            const rhs_tokens = generated_expression.right_tokens orelse return error.UnsupportedSqlShape;
            if (require_exact_child_expressions) {
                try validateGeneratedRowExpressionIdentityStrictWithContext(context, tokens, rhs_tokens.start, rhs_tokens.end, parsed_condition.rhs[0], generated_expression);
            } else {
                try validateGeneratedRowExpressionIdentity(tokens, rhs_tokens.start, rhs_tokens.end, parsed_condition.rhs[0], generated_expression);
            }
        },
    }
}

fn generatedBooleanLiteralIsTailForCondition(
    parsed_condition: db_mod.types.RelationalRowsExpressionCondition,
) ?expr_operator.ExpressionIsTail {
    if (parsed_condition.op != .eq or parsed_condition.rhs.len != 1) return null;
    const rhs = parsed_condition.rhs[0];
    if (rhs.kind != .value) return null;
    if (std.mem.eql(u8, rhs.value_json, "true")) {
        return .{ .op = .eq, .kind = .boolean_literal, .boolean_value = true };
    }
    if (std.mem.eql(u8, rhs.value_json, "false")) {
        return .{ .op = .eq, .kind = .boolean_literal, .boolean_value = false };
    }
    return null;
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
    try validateGeneratedInfixLeftPayload(tokens, expression, operator_tokens);
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
    try validateGeneratedInfixLeftPayload(tokens, expression, operator_tokens);
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

fn validateGeneratedInfixLeftPayload(
    tokens: []const Token,
    expression: *const generated_parser.GeneratedSqlExpressionAst,
    operator_tokens: generated_parser.GeneratedSqlTokenRange,
) !void {
    const left_tokens = expression.left_tokens orelse return error.UnsupportedSqlShape;
    const expected_left_end = if (expression.negation_tokens) |negation| blk: {
        if (negation.start >= operator_tokens.start and negation.end <= operator_tokens.end) break :blk operator_tokens.start;
        if (negation.end != operator_tokens.start) return error.UnsupportedSqlShape;
        break :blk negation.start;
    } else operator_tokens.start;
    if (left_tokens.start >= left_tokens.end or left_tokens.end > tokens.len or left_tokens.end != expected_left_end) return error.UnsupportedSqlShape;
    try validateGeneratedChildExpressionPayloadsAllowUnknownKind(tokens, left_tokens, expression.left_expression_kind, expression.left_expression);
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
    try validateGeneratedInfixLeftPayload(tokens, expression, operator_tokens);
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
    try validateGeneratedInfixLeftPayload(tokens, expression, operator_tokens);

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
    try validateGeneratedInfixLeftPayload(tokens, expression, operator_tokens);
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
    try validateGeneratedInfixLeftPayload(tokens, expression, operator_tokens);
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
    const expected_token_kind = try tokenKindForComparisonOp(op);
    if (operator_token_index >= tokens.len or tokens[operator_token_index].kind != expected_token_kind) return error.UnsupportedSqlShape;
    if (root.kind != .comparison) {
        switch (root.kind) {
            .token_range => {
                const left_tokens = root.tokens orelse return error.UnsupportedSqlShape;
                if (left_tokens.start >= left_tokens.end or left_tokens.end != operator_token_index or left_tokens.end > tokens.len) return error.UnsupportedSqlShape;
                if (operator_token_index + 1 >= tokens.len or tokens[operator_token_index + 1].kind != .identifier) return error.UnsupportedSqlShape;
                return;
            },
            .grouped, .logical_not, .quantified_comparison => {
                if (operator_token_index == 0 or operator_token_index + 1 >= tokens.len) return error.UnsupportedSqlShape;
                return;
            },
            else => return error.UnsupportedSqlShape,
        }
    }
    if (root.tokens) |range| {
        if (range.start > operator_token_index or range.end > tokens.len or range.end <= operator_token_index) return error.UnsupportedSqlShape;
    }
    if (root.operator_tokens) |range| {
        if (range.start != operator_token_index or range.end != operator_token_index + 1) return error.UnsupportedSqlShape;
    }
    if (root.left_tokens) |range| {
        if (range.start >= range.end or range.end != operator_token_index or range.end > tokens.len) return error.UnsupportedSqlShape;
    }
    const right_tokens = root.right_tokens orelse return error.UnsupportedSqlShape;
    if (operator_token_index >= tokens.len or right_tokens.start != operator_token_index + 1 or right_tokens.start >= right_tokens.end or right_tokens.end > tokens.len) {
        return error.UnsupportedSqlShape;
    }
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
    try validateGeneratedInfixLeftPayload(tokens, expression, operator_tokens);
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
    const alloc = std.testing.allocator;

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
    var ranged_regex_left_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 0, .end = 1 },
    };
    const ranged_regex_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .regex_not_imatch,
        .tokens = .{ .start = 0, .end = 3 },
        .left_tokens = .{ .start = 0, .end = 1 },
        .left_expression_kind = .token_range,
        .left_expression = &ranged_regex_left_expression,
        .operator_tokens = .{ .start = 1, .end = 2 },
        .right_tokens = .{ .start = 2, .end = 3 },
        .right_expression_kind = .token_range,
        .right_expression = &ranged_regex_right_expression,
    };
    try validateGeneratedRegexPredicateExpression(&ranged_regex_expression, &regex_expression_tokens, 1, true, true);
    const stale_regex_range_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .regex_not_imatch,
        .tokens = .{ .start = 0, .end = 3 },
        .left_tokens = .{ .start = 0, .end = 1 },
        .left_expression_kind = .token_range,
        .left_expression = &ranged_regex_left_expression,
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
    var in_left_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 0, .end = 1 },
    };
    const in_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .in_list,
        .left_tokens = .{ .start = 0, .end = 1 },
        .left_expression_kind = .token_range,
        .left_expression = &in_left_expression,
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
    var not_in_left_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 0, .end = 1 },
    };
    const not_in_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .not_in_list,
        .negation_tokens = .{ .start = 1, .end = 2 },
        .left_tokens = .{ .start = 0, .end = 1 },
        .left_expression_kind = .token_range,
        .left_expression = &not_in_left_expression,
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
    var between_left_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 0, .end = 1 },
    };
    const between_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .between,
        .left_tokens = .{ .start = 0, .end = 1 },
        .left_expression_kind = .token_range,
        .left_expression = &between_left_expression,
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
    var contains_left_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 0, .end = 1 },
    };
    const contains_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .contains,
        .left_tokens = .{ .start = 0, .end = 1 },
        .left_expression_kind = .token_range,
        .left_expression = &contains_left_expression,
        .operator_tokens = .{ .start = 1, .end = 2 },
        .right_tokens = .{ .start = 2, .end = 3 },
        .right_expression_kind = .token_range,
        .right_expression = &contains_right_expression,
    };
    try validateGeneratedSingleOperatorPredicateIdentity(&contains_expression, .contains, &contains_tokens, 1);
    const stale_contains_left_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .contains,
        .left_tokens = .{ .start = 0, .end = 2 },
        .operator_tokens = .{ .start = 1, .end = 2 },
        .right_tokens = .{ .start = 2, .end = 3 },
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedSingleOperatorPredicateIdentity(&stale_contains_left_expression, .contains, &contains_tokens, 1),
    );
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
    var generated_comparison_left_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 0, .end = 1 },
    };
    const generated_comparison_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .comparison,
        .left_tokens = .{ .start = 0, .end = 1 },
        .left_expression_kind = .token_range,
        .left_expression = &generated_comparison_left_expression,
        .operator_tokens = .{ .start = 1, .end = 2 },
        .right_tokens = .{ .start = 2, .end = 3 },
        .right_expression_kind = .token_range,
        .right_expression = &generated_comparison_right_expression,
    };
    try validateGeneratedComparisonPredicateExpression(&generated_comparison_expression, &generated_comparison_tokens, 1, .gte);
    const stale_generated_comparison_left_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .comparison,
        .left_tokens = .{ .start = 0, .end = 2 },
        .operator_tokens = .{ .start = 1, .end = 2 },
        .right_tokens = .{ .start = 2, .end = 3 },
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedComparisonPredicateExpression(&stale_generated_comparison_left_expression, &generated_comparison_tokens, 1, .gte),
    );
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
    var relational_distinct_left_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 0, .end = 1 },
    };
    const relational_distinct_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .is_not_distinct_from,
        .tokens = .{ .start = 0, .end = 6 },
        .operator_tokens = .{ .start = 1, .end = 5 },
        .negation_tokens = .{ .start = 2, .end = 3 },
        .left_tokens = .{ .start = 0, .end = 1 },
        .left_expression_kind = .token_range,
        .left_expression = &relational_distinct_left_expression,
        .right_tokens = .{ .start = 5, .end = 6 },
        .right_expression_kind = .token_range,
        .right_expression = &relational_distinct_right_expression,
    };
    try validateGeneratedRelationalPredicateIdentity(&relational_distinct_expression, &distinct_tokens, 1, .is_not_distinct);
    const stale_relational_distinct_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .is_not_distinct_from,
        .operator_tokens = .{ .start = 2, .end = 5 },
        .left_tokens = .{ .start = 0, .end = 1 },
        .right_tokens = .{ .start = 5, .end = 6 },
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedRelationalPredicateIdentity(&stale_relational_distinct_expression, &distinct_tokens, 1, .is_not_distinct),
    );
    const stale_relational_distinct_left_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .is_not_distinct_from,
        .operator_tokens = .{ .start = 1, .end = 5 },
        .left_tokens = .{ .start = 0, .end = 2 },
        .right_tokens = .{ .start = 5, .end = 6 },
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedRelationalPredicateIdentity(&stale_relational_distinct_left_expression, &distinct_tokens, 1, .is_not_distinct),
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

    const grouped_tokens = [_]Token{
        .{ .kind = .lparen, .text = "(" },
        .{ .kind = .identifier, .text = "amount" },
        .{ .kind = .rparen, .text = ")" },
    };
    var grouped_inner_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 1, .end = 2 },
    };
    const grouped_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .grouped,
        .tokens = .{ .start = 0, .end = 3 },
        .inner_tokens = .{ .start = 1, .end = 2 },
        .inner_expression = &grouped_inner_expression,
    };
    try validateGeneratedExpressionPayloads(&grouped_tokens, grouped_expression);
    var stale_grouped_inner_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 0, .end = 1 },
    };
    const stale_grouped_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .grouped,
        .tokens = .{ .start = 0, .end = 3 },
        .inner_tokens = .{ .start = 1, .end = 2 },
        .inner_expression = &stale_grouped_inner_expression,
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedExpressionPayloads(&grouped_tokens, stale_grouped_expression),
    );

    const strict_cast_tokens = [_]Token{
        .{ .kind = .identifier, .text = "cast", .keyword = .cast },
        .{ .kind = .lparen, .text = "(" },
        .{ .kind = .identifier, .text = "amount" },
        .{ .kind = .identifier, .text = "as", .keyword = .as },
        .{ .kind = .identifier, .text = "text" },
        .{ .kind = .rparen, .text = ")" },
    };
    var strict_cast_child = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 2, .end = 3 },
    };
    const strict_cast_ast = generated_parser.GeneratedSqlExpressionAst{
        .kind = .cast,
        .tokens = .{ .start = 0, .end = 6 },
        .cast_expression_tokens = .{ .start = 2, .end = 3 },
        .cast_expression = &strict_cast_child,
        .cast_type_tokens = .{ .start = 4, .end = 5 },
    };
    const strict_cast_operands = [_]db_mod.types.RelationalRowsExpression{.{ .kind = .field, .field = "amount" }};
    const strict_cast_expression = db_mod.types.RelationalRowsExpression{
        .kind = .cast,
        .operands = strict_cast_operands[0..],
        .cast_type = .text,
    };
    try validateGeneratedRowExpressionIdentityStrict(&strict_cast_tokens, 0, 6, strict_cast_expression, &strict_cast_ast);
    const stale_strict_cast_expression = db_mod.types.RelationalRowsExpression{
        .kind = .cast,
        .operands = strict_cast_operands[0..],
        .cast_type = .numeric,
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedRowExpressionIdentityStrict(&strict_cast_tokens, 0, 6, stale_strict_cast_expression, &strict_cast_ast),
    );

    const strict_lower_tokens = [_]Token{
        .{ .kind = .identifier, .text = "lower", .keyword = .lower },
        .{ .kind = .lparen, .text = "(" },
        .{ .kind = .identifier, .text = "status" },
        .{ .kind = .rparen, .text = ")" },
    };
    var strict_lower_argument_items = [_]generated_parser.GeneratedSqlTokenRange{.{ .start = 2, .end = 3 }};
    var strict_lower_argument_expressions = [_]generated_parser.GeneratedSqlExpressionAst{.{
        .kind = .token_range,
        .tokens = .{ .start = 2, .end = 3 },
    }};
    var strict_lower_argument_alias_items = [_]?generated_parser.GeneratedSqlTokenRange{null};
    var strict_lower_argument_alias_name_items = [_]?generated_parser.GeneratedSqlTokenRange{null};
    var strict_lower_argument_direction_items = [_]?generated_parser.GeneratedSqlTokenRange{null};
    var strict_lower_argument_directions = [_]?generated_parser.GeneratedSqlOrderDirection{null};
    var strict_lower_argument_order_using_items = [_]?generated_parser.GeneratedSqlTokenRange{null};
    var strict_lower_argument_nulls_order_items = [_]?generated_parser.GeneratedSqlTokenRange{null};
    var strict_lower_argument_nulls_orders = [_]?generated_parser.GeneratedSqlNullsOrder{null};
    const strict_lower_ast = generated_parser.GeneratedSqlExpressionAst{
        .kind = .function_call,
        .tokens = .{ .start = 0, .end = 4 },
        .function_name_tokens = .{ .start = 0, .end = 1 },
        .argument_tokens = .{ .start = 2, .end = 3 },
        .argument_value_tokens = .{ .start = 2, .end = 3 },
        .argument_items = .{
            .first_tokens = strict_lower_argument_items[0],
            .last_tokens = strict_lower_argument_items[0],
            .items = strict_lower_argument_items[0..],
            .expression_items = strict_lower_argument_items[0..],
            .alias_items = strict_lower_argument_alias_items[0..],
            .alias_name_items = strict_lower_argument_alias_name_items[0..],
            .direction_items = strict_lower_argument_direction_items[0..],
            .directions = strict_lower_argument_directions[0..],
            .order_using_operator_items = strict_lower_argument_order_using_items[0..],
            .nulls_order_items = strict_lower_argument_nulls_order_items[0..],
            .nulls_orders = strict_lower_argument_nulls_orders[0..],
            .expressions = strict_lower_argument_expressions[0..],
            .count = 1,
        },
    };
    const strict_lower_operands = [_]db_mod.types.RelationalRowsExpression{.{ .kind = .field, .field = "status" }};
    const strict_lower_expression = db_mod.types.RelationalRowsExpression{
        .kind = .lower,
        .operands = strict_lower_operands[0..],
    };
    try validateGeneratedRowExpressionIdentityStrict(&strict_lower_tokens, 0, 4, strict_lower_expression, &strict_lower_ast);

    const stale_strict_lower_filter_ast = generated_parser.GeneratedSqlExpressionAst{
        .kind = .function_call,
        .tokens = .{ .start = 0, .end = 4 },
        .function_name_tokens = .{ .start = 0, .end = 1 },
        .argument_tokens = .{ .start = 2, .end = 3 },
        .argument_value_tokens = .{ .start = 2, .end = 3 },
        .argument_items = strict_lower_ast.argument_items,
        .filter_tokens = .{ .start = 0, .end = 4 },
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedRowExpressionIdentityStrict(&strict_lower_tokens, 0, 4, strict_lower_expression, &stale_strict_lower_filter_ast),
    );

    const stale_strict_lower_over_ast = generated_parser.GeneratedSqlExpressionAst{
        .kind = .function_call,
        .tokens = .{ .start = 0, .end = 4 },
        .function_name_tokens = .{ .start = 0, .end = 1 },
        .argument_tokens = .{ .start = 2, .end = 3 },
        .argument_value_tokens = .{ .start = 2, .end = 3 },
        .argument_items = strict_lower_ast.argument_items,
        .over_tokens = .{ .start = 0, .end = 4 },
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedRowExpressionIdentityStrict(&strict_lower_tokens, 0, 4, strict_lower_expression, &stale_strict_lower_over_ast),
    );

    const strict_extract_tokens = [_]Token{
        .{ .kind = .identifier, .text = "extract", .keyword = .extract },
        .{ .kind = .lparen, .text = "(" },
        .{ .kind = .identifier, .text = "dow" },
        .{ .kind = .identifier, .text = "from", .keyword = .from },
        .{ .kind = .identifier, .text = "amount" },
        .{ .kind = .rparen, .text = ")" },
    };
    var strict_extract_source = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 4, .end = 5 },
    };
    const strict_extract_ast = generated_parser.GeneratedSqlExpressionAst{
        .kind = .extract_expression,
        .tokens = .{ .start = 0, .end = 6 },
        .extract_field_tokens = .{ .start = 2, .end = 3 },
        .extract_source_tokens = .{ .start = 4, .end = 5 },
        .extract_source_expression = &strict_extract_source,
    };
    const strict_extract_operands = [_]db_mod.types.RelationalRowsExpression{
        .{ .kind = .value, .value_json = "\"dow\"" },
        .{ .kind = .field, .field = "amount" },
    };
    const strict_extract_expression = db_mod.types.RelationalRowsExpression{
        .kind = .date_part,
        .operands = strict_extract_operands[0..],
    };
    try validateGeneratedRowExpressionIdentityStrict(&strict_extract_tokens, 0, 6, strict_extract_expression, &strict_extract_ast);
    const stale_strict_extract_operands = [_]db_mod.types.RelationalRowsExpression{.{ .kind = .value, .value_json = "\"dow\"" }};
    const stale_strict_extract_expression = db_mod.types.RelationalRowsExpression{
        .kind = .date_part,
        .operands = stale_strict_extract_operands[0..],
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedRowExpressionIdentityStrict(&strict_extract_tokens, 0, 6, stale_strict_extract_expression, &strict_extract_ast),
    );

    const strict_case_tokens = [_]Token{
        .{ .kind = .identifier, .text = "case", .keyword = .case },
        .{ .kind = .identifier, .text = "when", .keyword = .when },
        .{ .kind = .identifier, .text = "status" },
        .{ .kind = .identifier, .text = "is", .keyword = .is },
        .{ .kind = .identifier, .text = "null", .keyword = .null },
        .{ .kind = .identifier, .text = "then", .keyword = .then },
        .{ .kind = .string, .text = "missing" },
        .{ .kind = .identifier, .text = "else", .keyword = .@"else" },
        .{ .kind = .string, .text = "fallback" },
        .{ .kind = .identifier, .text = "end", .keyword = .end },
    };
    var strict_case_condition_left = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 2, .end = 3 },
    };
    var strict_case_condition = generated_parser.GeneratedSqlExpressionAst{
        .kind = .is_null,
        .tokens = .{ .start = 2, .end = 5 },
        .left_tokens = .{ .start = 2, .end = 3 },
        .left_expression = &strict_case_condition_left,
        .operator_tokens = .{ .start = 3, .end = 5 },
    };
    var strict_case_result = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 6, .end = 7 },
    };
    var strict_case_else = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 8, .end = 9 },
    };
    var strict_case_condition_items = [_]generated_parser.GeneratedSqlTokenRange{.{ .start = 2, .end = 5 }};
    var strict_case_result_items = [_]generated_parser.GeneratedSqlTokenRange{.{ .start = 6, .end = 7 }};
    var strict_case_condition_expressions = [_]generated_parser.GeneratedSqlExpressionAst{strict_case_condition};
    var strict_case_result_expressions = [_]generated_parser.GeneratedSqlExpressionAst{strict_case_result};
    const strict_case_ast = generated_parser.GeneratedSqlExpressionAst{
        .kind = .case_expression,
        .tokens = .{ .start = 0, .end = 10 },
        .case_branch_count = 1,
        .case_first_when_tokens = .{ .start = 1, .end = 7 },
        .case_last_when_tokens = .{ .start = 1, .end = 7 },
        .case_first_condition_tokens = .{ .start = 2, .end = 5 },
        .case_first_condition_kind = .is_null,
        .case_first_condition = &strict_case_condition,
        .case_first_result_tokens = .{ .start = 6, .end = 7 },
        .case_first_result = &strict_case_result,
        .case_condition_items = .{
            .first_tokens = strict_case_condition_items[0],
            .last_tokens = strict_case_condition_items[0],
            .items = strict_case_condition_items[0..],
            .expression_items = strict_case_condition_items[0..],
            .expressions = strict_case_condition_expressions[0..],
            .count = 1,
        },
        .case_result_items = .{
            .first_tokens = strict_case_result_items[0],
            .last_tokens = strict_case_result_items[0],
            .items = strict_case_result_items[0..],
            .expression_items = strict_case_result_items[0..],
            .expressions = strict_case_result_expressions[0..],
            .count = 1,
        },
        .case_else_tokens = .{ .start = 7, .end = 9 },
        .case_else_expression_tokens = .{ .start = 8, .end = 9 },
        .case_else_expression = &strict_case_else,
    };
    const strict_case_branches = [_]db_mod.types.RelationalRowsExpressionCaseBranch{.{
        .when = .{
            .lhs = .{ .kind = .field, .field = "status" },
            .op = .is_null,
        },
        .then = .{ .kind = .value, .value_json = "\"missing\"" },
    }};
    const strict_case_fallback = [_]db_mod.types.RelationalRowsExpression{.{ .kind = .value, .value_json = "\"fallback\"" }};
    const strict_case_expression = db_mod.types.RelationalRowsExpression{
        .kind = .case,
        .case_branches = strict_case_branches[0..],
        .case_else = strict_case_fallback[0..],
    };
    try validateGeneratedRowExpressionIdentityStrict(&strict_case_tokens, 0, 10, strict_case_expression, &strict_case_ast);
    const stale_strict_case_expression = db_mod.types.RelationalRowsExpression{
        .kind = .case,
        .case_branches = strict_case_branches[0..],
        .case_else = &.{},
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedRowExpressionIdentityStrict(&strict_case_tokens, 0, 10, stale_strict_case_expression, &strict_case_ast),
    );

    const strict_like_tokens = [_]Token{
        .{ .kind = .identifier, .text = "status" },
        .{ .kind = .identifier, .text = "like", .keyword = .like },
        .{ .kind = .string, .text = "ant%" },
    };
    var strict_like_left = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 0, .end = 1 },
    };
    var strict_like_right = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 2, .end = 3 },
    };
    const strict_like_ast = generated_parser.GeneratedSqlExpressionAst{
        .kind = .like,
        .tokens = .{ .start = 0, .end = 3 },
        .left_tokens = .{ .start = 0, .end = 1 },
        .left_expression = &strict_like_left,
        .operator_tokens = .{ .start = 1, .end = 2 },
        .right_tokens = .{ .start = 2, .end = 3 },
        .right_expression = &strict_like_right,
    };
    const strict_like_operands = [_]db_mod.types.RelationalRowsExpression{
        .{ .kind = .field, .field = "status" },
        .{ .kind = .value, .value_json = "\"ant%\"" },
    };
    const strict_like_rhs = [_]db_mod.types.RelationalRowsExpression{.{ .kind = .value, .value_json = "true" }};
    const strict_like_condition = db_mod.types.RelationalRowsExpressionCondition{
        .lhs = .{ .kind = .like, .operands = strict_like_operands[0..] },
        .op = .eq,
        .rhs = strict_like_rhs[0..],
    };
    try validateGeneratedExpressionConditionIdentityStrict(&strict_like_tokens, 0, 3, strict_like_condition, &strict_like_ast);
    const stale_strict_like_rhs = [_]db_mod.types.RelationalRowsExpression{.{ .kind = .value, .value_json = "false" }};
    const stale_strict_like_condition = db_mod.types.RelationalRowsExpressionCondition{
        .lhs = .{ .kind = .like, .operands = strict_like_operands[0..] },
        .op = .eq,
        .rhs = stale_strict_like_rhs[0..],
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedExpressionConditionIdentityStrict(&strict_like_tokens, 0, 3, stale_strict_like_condition, &strict_like_ast),
    );

    const strict_like_escape_tokens = [_]Token{
        .{ .kind = .identifier, .text = "status" },
        .{ .kind = .identifier, .text = "like", .keyword = .like },
        .{ .kind = .string, .text = "a!_%" },
        .{ .kind = .identifier, .text = "escape", .keyword = .escape },
        .{ .kind = .string, .text = "!" },
    };
    var strict_like_escape_left = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 0, .end = 1 },
    };
    var strict_like_escape_right = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 2, .end = 3 },
    };
    var strict_like_escape_escape = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 4, .end = 5 },
    };
    const strict_like_escape_ast = generated_parser.GeneratedSqlExpressionAst{
        .kind = .like,
        .tokens = .{ .start = 0, .end = 5 },
        .left_tokens = .{ .start = 0, .end = 1 },
        .left_expression = &strict_like_escape_left,
        .operator_tokens = .{ .start = 1, .end = 2 },
        .right_tokens = .{ .start = 2, .end = 3 },
        .right_expression = &strict_like_escape_right,
        .escape_tokens = .{ .start = 3, .end = 5 },
        .escape_expression_kind = .token_range,
        .escape_expression = &strict_like_escape_escape,
    };
    const strict_like_escape_operands = [_]db_mod.types.RelationalRowsExpression{
        .{ .kind = .field, .field = "status" },
        .{ .kind = .value, .value_json = "\"a\\\\_%\"" },
    };
    const strict_like_escape_condition = db_mod.types.RelationalRowsExpressionCondition{
        .lhs = .{ .kind = .like, .operands = strict_like_escape_operands[0..] },
        .op = .eq,
        .rhs = strict_like_rhs[0..],
    };
    try validateGeneratedExpressionConditionIdentityStrict(&strict_like_escape_tokens, 0, 5, strict_like_escape_condition, &strict_like_escape_ast);
    const stale_strict_like_escape_operands = [_]db_mod.types.RelationalRowsExpression{
        .{ .kind = .field, .field = "status" },
        .{ .kind = .value, .value_json = "\"a!_%\"" },
    };
    const stale_strict_like_escape_condition = db_mod.types.RelationalRowsExpressionCondition{
        .lhs = .{ .kind = .like, .operands = stale_strict_like_escape_operands[0..] },
        .op = .eq,
        .rhs = strict_like_rhs[0..],
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedExpressionConditionIdentityStrict(&strict_like_escape_tokens, 0, 5, stale_strict_like_escape_condition, &strict_like_escape_ast),
    );

    const strict_not_like_tokens = [_]Token{
        .{ .kind = .identifier, .text = "status" },
        .{ .kind = .identifier, .text = "not", .keyword = .not },
        .{ .kind = .identifier, .text = "like", .keyword = .like },
        .{ .kind = .string, .text = "ant%" },
    };
    var strict_not_like_left = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 0, .end = 1 },
    };
    var strict_not_like_right = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 3, .end = 4 },
    };
    const strict_not_like_ast = generated_parser.GeneratedSqlExpressionAst{
        .kind = .not_like,
        .tokens = .{ .start = 0, .end = 4 },
        .left_tokens = .{ .start = 0, .end = 1 },
        .left_expression = &strict_not_like_left,
        .negation_tokens = .{ .start = 1, .end = 2 },
        .operator_tokens = .{ .start = 2, .end = 3 },
        .right_tokens = .{ .start = 3, .end = 4 },
        .right_expression = &strict_not_like_right,
    };
    const strict_not_like_rhs = [_]db_mod.types.RelationalRowsExpression{.{ .kind = .value, .value_json = "false" }};
    const strict_not_like_condition = db_mod.types.RelationalRowsExpressionCondition{
        .lhs = .{ .kind = .like, .operands = strict_like_operands[0..] },
        .op = .eq,
        .rhs = strict_not_like_rhs[0..],
    };
    try validateGeneratedExpressionConditionIdentityStrict(&strict_not_like_tokens, 0, 4, strict_not_like_condition, &strict_not_like_ast);

    const strict_ilike_tokens = [_]Token{
        .{ .kind = .identifier, .text = "status" },
        .{ .kind = .identifier, .text = "ilike", .keyword = .ilike },
        .{ .kind = .string, .text = "ant%" },
    };
    var strict_ilike_left = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 0, .end = 1 },
    };
    var strict_ilike_right = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 2, .end = 3 },
    };
    const strict_ilike_ast = generated_parser.GeneratedSqlExpressionAst{
        .kind = .ilike,
        .tokens = .{ .start = 0, .end = 3 },
        .left_tokens = .{ .start = 0, .end = 1 },
        .left_expression = &strict_ilike_left,
        .operator_tokens = .{ .start = 1, .end = 2 },
        .right_tokens = .{ .start = 2, .end = 3 },
        .right_expression = &strict_ilike_right,
    };
    const strict_ilike_condition = db_mod.types.RelationalRowsExpressionCondition{
        .lhs = .{ .kind = .ilike, .operands = strict_like_operands[0..] },
        .op = .eq,
        .rhs = strict_like_rhs[0..],
    };
    try validateGeneratedExpressionConditionIdentityStrict(&strict_ilike_tokens, 0, 3, strict_ilike_condition, &strict_ilike_ast);
    const stale_strict_ilike_condition = db_mod.types.RelationalRowsExpressionCondition{
        .lhs = .{ .kind = .like, .operands = strict_like_operands[0..] },
        .op = .eq,
        .rhs = strict_like_rhs[0..],
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedExpressionConditionIdentityStrict(&strict_ilike_tokens, 0, 3, stale_strict_ilike_condition, &strict_ilike_ast),
    );

    const strict_not_ilike_tokens = [_]Token{
        .{ .kind = .identifier, .text = "status" },
        .{ .kind = .identifier, .text = "not", .keyword = .not },
        .{ .kind = .identifier, .text = "ilike", .keyword = .ilike },
        .{ .kind = .string, .text = "ant%" },
    };
    var strict_not_ilike_left = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 0, .end = 1 },
    };
    var strict_not_ilike_right = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 3, .end = 4 },
    };
    const strict_not_ilike_ast = generated_parser.GeneratedSqlExpressionAst{
        .kind = .not_ilike,
        .tokens = .{ .start = 0, .end = 4 },
        .left_tokens = .{ .start = 0, .end = 1 },
        .left_expression = &strict_not_ilike_left,
        .negation_tokens = .{ .start = 1, .end = 2 },
        .operator_tokens = .{ .start = 2, .end = 3 },
        .right_tokens = .{ .start = 3, .end = 4 },
        .right_expression = &strict_not_ilike_right,
    };
    const strict_not_ilike_condition = db_mod.types.RelationalRowsExpressionCondition{
        .lhs = .{ .kind = .ilike, .operands = strict_like_operands[0..] },
        .op = .eq,
        .rhs = strict_not_like_rhs[0..],
    };
    try validateGeneratedExpressionConditionIdentityStrict(&strict_not_ilike_tokens, 0, 4, strict_not_ilike_condition, &strict_not_ilike_ast);

    const strict_regex_tokens = [_]Token{
        .{ .kind = .identifier, .text = "status" },
        .{ .kind = .regex_imatch, .text = "~*" },
        .{ .kind = .string, .text = "ant.*" },
    };
    var strict_regex_left = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 0, .end = 1 },
    };
    var strict_regex_right = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 2, .end = 3 },
    };
    const strict_regex_ast = generated_parser.GeneratedSqlExpressionAst{
        .kind = .regex_imatch,
        .tokens = .{ .start = 0, .end = 3 },
        .left_tokens = .{ .start = 0, .end = 1 },
        .left_expression = &strict_regex_left,
        .operator_tokens = .{ .start = 1, .end = 2 },
        .right_tokens = .{ .start = 2, .end = 3 },
        .right_expression = &strict_regex_right,
    };
    const strict_regex_operands = [_]db_mod.types.RelationalRowsExpression{
        .{ .kind = .field, .field = "status" },
        .{ .kind = .value, .value_json = "\"ant.*\"" },
        .{ .kind = .value, .value_json = "true" },
    };
    const strict_regex_rhs = [_]db_mod.types.RelationalRowsExpression{.{ .kind = .value, .value_json = "true" }};
    const strict_regex_condition = db_mod.types.RelationalRowsExpressionCondition{
        .lhs = .{ .kind = .regexp_match, .operands = strict_regex_operands[0..] },
        .op = .eq,
        .rhs = strict_regex_rhs[0..],
    };
    try validateGeneratedExpressionConditionIdentityStrict(&strict_regex_tokens, 0, 3, strict_regex_condition, &strict_regex_ast);
    const stale_strict_regex_operands = [_]db_mod.types.RelationalRowsExpression{
        .{ .kind = .field, .field = "status" },
        .{ .kind = .value, .value_json = "\"ant.*\"" },
        .{ .kind = .value, .value_json = "false" },
    };
    const stale_strict_regex_condition = db_mod.types.RelationalRowsExpressionCondition{
        .lhs = .{ .kind = .regexp_match, .operands = stale_strict_regex_operands[0..] },
        .op = .eq,
        .rhs = strict_regex_rhs[0..],
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedExpressionConditionIdentityStrict(&strict_regex_tokens, 0, 3, stale_strict_regex_condition, &strict_regex_ast),
    );

    const strict_regex_match_tokens = [_]Token{
        .{ .kind = .identifier, .text = "status" },
        .{ .kind = .regex_match, .text = "~" },
        .{ .kind = .string, .text = "ant.*" },
    };
    var strict_regex_match_left = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 0, .end = 1 },
    };
    var strict_regex_match_right = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 2, .end = 3 },
    };
    const strict_regex_match_ast = generated_parser.GeneratedSqlExpressionAst{
        .kind = .regex_match,
        .tokens = .{ .start = 0, .end = 3 },
        .left_tokens = .{ .start = 0, .end = 1 },
        .left_expression = &strict_regex_match_left,
        .operator_tokens = .{ .start = 1, .end = 2 },
        .right_tokens = .{ .start = 2, .end = 3 },
        .right_expression = &strict_regex_match_right,
    };
    const strict_regex_match_operands = [_]db_mod.types.RelationalRowsExpression{
        .{ .kind = .field, .field = "status" },
        .{ .kind = .value, .value_json = "\"ant.*\"" },
        .{ .kind = .value, .value_json = "false" },
    };
    const strict_regex_match_condition = db_mod.types.RelationalRowsExpressionCondition{
        .lhs = .{ .kind = .regexp_match, .operands = strict_regex_match_operands[0..] },
        .op = .eq,
        .rhs = strict_regex_rhs[0..],
    };
    try validateGeneratedExpressionConditionIdentityStrict(&strict_regex_match_tokens, 0, 3, strict_regex_match_condition, &strict_regex_match_ast);

    const strict_regex_not_match_tokens = [_]Token{
        .{ .kind = .identifier, .text = "status" },
        .{ .kind = .regex_not_match, .text = "!~" },
        .{ .kind = .string, .text = "ant.*" },
    };
    var strict_regex_not_match_left = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 0, .end = 1 },
    };
    var strict_regex_not_match_right = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 2, .end = 3 },
    };
    const strict_regex_not_match_ast = generated_parser.GeneratedSqlExpressionAst{
        .kind = .regex_not_match,
        .tokens = .{ .start = 0, .end = 3 },
        .left_tokens = .{ .start = 0, .end = 1 },
        .left_expression = &strict_regex_not_match_left,
        .operator_tokens = .{ .start = 1, .end = 2 },
        .right_tokens = .{ .start = 2, .end = 3 },
        .right_expression = &strict_regex_not_match_right,
    };
    const strict_regex_not_match_rhs = [_]db_mod.types.RelationalRowsExpression{.{ .kind = .value, .value_json = "false" }};
    const strict_regex_not_match_condition = db_mod.types.RelationalRowsExpressionCondition{
        .lhs = .{ .kind = .regexp_match, .operands = strict_regex_match_operands[0..] },
        .op = .eq,
        .rhs = strict_regex_not_match_rhs[0..],
    };
    try validateGeneratedExpressionConditionIdentityStrict(&strict_regex_not_match_tokens, 0, 3, strict_regex_not_match_condition, &strict_regex_not_match_ast);

    const strict_regex_not_imatch_tokens = [_]Token{
        .{ .kind = .identifier, .text = "status" },
        .{ .kind = .regex_not_imatch, .text = "!~*" },
        .{ .kind = .string, .text = "ant.*" },
    };
    var strict_regex_not_imatch_left = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 0, .end = 1 },
    };
    var strict_regex_not_imatch_right = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 2, .end = 3 },
    };
    const strict_regex_not_imatch_ast = generated_parser.GeneratedSqlExpressionAst{
        .kind = .regex_not_imatch,
        .tokens = .{ .start = 0, .end = 3 },
        .left_tokens = .{ .start = 0, .end = 1 },
        .left_expression = &strict_regex_not_imatch_left,
        .operator_tokens = .{ .start = 1, .end = 2 },
        .right_tokens = .{ .start = 2, .end = 3 },
        .right_expression = &strict_regex_not_imatch_right,
    };
    const strict_regex_not_imatch_condition = db_mod.types.RelationalRowsExpressionCondition{
        .lhs = .{ .kind = .regexp_match, .operands = strict_regex_operands[0..] },
        .op = .eq,
        .rhs = strict_regex_not_match_rhs[0..],
    };
    try validateGeneratedExpressionConditionIdentityStrict(&strict_regex_not_imatch_tokens, 0, 3, strict_regex_not_imatch_condition, &strict_regex_not_imatch_ast);

    const strict_array_tokens = [_]Token{
        .{ .kind = .identifier, .text = "array", .keyword = .array },
        .{ .kind = .lbracket, .text = "[" },
        .{ .kind = .number, .text = "1" },
        .{ .kind = .comma, .text = "," },
        .{ .kind = .string, .text = "two" },
        .{ .kind = .rbracket, .text = "]" },
    };
    const strict_array_first_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 2, .end = 3 },
    };
    const strict_array_second_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 4, .end = 5 },
    };
    var strict_array_items = [_]generated_parser.GeneratedSqlTokenRange{
        .{ .start = 2, .end = 3 },
        .{ .start = 4, .end = 5 },
    };
    var strict_array_expressions = [_]generated_parser.GeneratedSqlExpressionAst{
        strict_array_first_expression,
        strict_array_second_expression,
    };
    const strict_array_ast = generated_parser.GeneratedSqlExpressionAst{
        .kind = .array_constructor,
        .tokens = .{ .start = 0, .end = 6 },
        .array_tokens = .{ .start = 2, .end = 5 },
        .array_items = .{
            .first_tokens = strict_array_items[0],
            .last_tokens = strict_array_items[1],
            .items = strict_array_items[0..],
            .expression_items = strict_array_items[0..],
            .expressions = strict_array_expressions[0..],
            .count = 2,
        },
    };
    const strict_array_expression = db_mod.types.RelationalRowsExpression{
        .kind = .value,
        .value_json = "[1,\"two\"]",
    };
    try validateGeneratedRowExpressionIdentityStrict(&strict_array_tokens, 0, 6, strict_array_expression, &strict_array_ast);
    try validateGeneratedRowExpressionIdentityStrictWithContext(
        .{ .alloc = alloc },
        &strict_array_tokens,
        0,
        6,
        strict_array_expression,
        &strict_array_ast,
    );
    const stale_strict_array_expression = db_mod.types.RelationalRowsExpression{
        .kind = .value,
        .value_json = "[1]",
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedRowExpressionIdentityStrict(&strict_array_tokens, 0, 6, stale_strict_array_expression, &strict_array_ast),
    );
    const stale_strict_array_same_count_expression = db_mod.types.RelationalRowsExpression{
        .kind = .value,
        .value_json = "[1,\"stale\"]",
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedRowExpressionIdentityStrictWithContext(
            .{ .alloc = alloc },
            &strict_array_tokens,
            0,
            6,
            stale_strict_array_same_count_expression,
            &strict_array_ast,
        ),
    );

    const strict_interval_tokens = [_]Token{
        .{ .kind = .identifier, .text = "interval", .keyword = .interval },
        .{ .kind = .string, .text = "2 days" },
    };
    const strict_interval_ast = generated_parser.GeneratedSqlExpressionAst{
        .kind = .interval_literal,
        .tokens = .{ .start = 0, .end = 2 },
        .interval_value_tokens = .{ .start = 1, .end = 2 },
    };
    const strict_interval_operands = [_]db_mod.types.RelationalRowsExpression{.{
        .kind = .value,
        .value_json = "172800000000000",
    }};
    const strict_interval_expression = db_mod.types.RelationalRowsExpression{
        .kind = .interval_ns,
        .operands = strict_interval_operands[0..],
    };
    try validateGeneratedRowExpressionIdentityStrict(&strict_interval_tokens, 0, 2, strict_interval_expression, &strict_interval_ast);
    const stale_strict_interval_operands = [_]db_mod.types.RelationalRowsExpression{.{
        .kind = .value,
        .value_json = "86400000000000",
    }};
    const stale_strict_interval_expression = db_mod.types.RelationalRowsExpression{
        .kind = .interval_ns,
        .operands = stale_strict_interval_operands[0..],
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedRowExpressionIdentityStrict(&strict_interval_tokens, 0, 2, stale_strict_interval_expression, &strict_interval_ast),
    );

    const mixed_interval_tokens = [_]Token{
        .{ .kind = .identifier, .text = "interval", .keyword = .interval },
        .{ .kind = .string, .text = "1 month 1 day" },
    };
    const mixed_interval_ast = generated_parser.GeneratedSqlExpressionAst{
        .kind = .interval_literal,
        .tokens = .{ .start = 0, .end = 2 },
        .interval_value_tokens = .{ .start = 1, .end = 2 },
    };
    const mixed_calendar_interval_operands = [_]db_mod.types.RelationalRowsExpression{.{
        .kind = .value,
        .value_json = "1",
    }};
    const mixed_calendar_interval_expression = db_mod.types.RelationalRowsExpression{
        .kind = .interval_months,
        .operands = mixed_calendar_interval_operands[0..],
    };
    try validateGeneratedRowExpressionIdentityStrict(&mixed_interval_tokens, 0, 2, mixed_calendar_interval_expression, &mixed_interval_ast);
    const mixed_fixed_interval_operands = [_]db_mod.types.RelationalRowsExpression{.{
        .kind = .value,
        .value_json = "86400000000000",
    }};
    const mixed_fixed_interval_expression = db_mod.types.RelationalRowsExpression{
        .kind = .interval_ns,
        .operands = mixed_fixed_interval_operands[0..],
    };
    try validateGeneratedRowExpressionIdentityStrict(&mixed_interval_tokens, 0, 2, mixed_fixed_interval_expression, &mixed_interval_ast);
    const stale_mixed_fixed_interval_operands = [_]db_mod.types.RelationalRowsExpression{.{
        .kind = .value,
        .value_json = "86400000000001",
    }};
    const stale_mixed_fixed_interval_expression = db_mod.types.RelationalRowsExpression{
        .kind = .interval_ns,
        .operands = stale_mixed_fixed_interval_operands[0..],
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedRowExpressionIdentityStrict(&mixed_interval_tokens, 0, 2, stale_mixed_fixed_interval_expression, &mixed_interval_ast),
    );

    const strict_current_date_tokens = [_]Token{.{ .kind = .identifier, .text = "current_date", .keyword = .current_date }};
    const strict_current_date_ast = generated_parser.GeneratedSqlExpressionAst{
        .kind = .current_date,
        .tokens = .{ .start = 0, .end = 1 },
    };
    const strict_current_date_operands = [_]db_mod.types.RelationalRowsExpression{
        .{ .kind = .value, .value_json = "\"day\"" },
        .{ .kind = .now },
    };
    const strict_current_date_expression = db_mod.types.RelationalRowsExpression{
        .kind = .date_trunc,
        .operands = strict_current_date_operands[0..],
    };
    try validateGeneratedRowExpressionIdentityStrict(&strict_current_date_tokens, 0, 1, strict_current_date_expression, &strict_current_date_ast);
    const stale_strict_current_date_operands = [_]db_mod.types.RelationalRowsExpression{
        .{ .kind = .value, .value_json = "\"hour\"" },
        .{ .kind = .now },
    };
    const stale_strict_current_date_expression = db_mod.types.RelationalRowsExpression{
        .kind = .date_trunc,
        .operands = stale_strict_current_date_operands[0..],
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedRowExpressionIdentityStrict(&strict_current_date_tokens, 0, 1, stale_strict_current_date_expression, &strict_current_date_ast),
    );

    const strict_current_timestamp_tokens = [_]Token{.{ .kind = .identifier, .text = "current_timestamp", .keyword = .current_timestamp }};
    const strict_current_timestamp_ast = generated_parser.GeneratedSqlExpressionAst{
        .kind = .current_timestamp,
        .tokens = .{ .start = 0, .end = 1 },
    };
    try validateGeneratedRowExpressionIdentityStrict(
        &strict_current_timestamp_tokens,
        0,
        1,
        .{ .kind = .now, .value_json = "1" },
        &strict_current_timestamp_ast,
    );
    const stale_strict_current_timestamp_operands = [_]db_mod.types.RelationalRowsExpression{.{ .kind = .value, .value_json = "1" }};
    const stale_strict_current_timestamp_expression = db_mod.types.RelationalRowsExpression{
        .kind = .now,
        .operands = stale_strict_current_timestamp_operands[0..],
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedRowExpressionIdentityStrict(&strict_current_timestamp_tokens, 0, 1, stale_strict_current_timestamp_expression, &strict_current_timestamp_ast),
    );

    const strict_timestamp_literal_tokens = [_]Token{
        .{ .kind = .identifier, .text = "timestamp", .keyword = .timestamp },
        .{ .kind = .string, .text = "1970-01-01T00:00:01Z" },
    };
    const strict_timestamp_literal_ast = generated_parser.GeneratedSqlExpressionAst{
        .kind = .timestamp_literal,
        .tokens = .{ .start = 0, .end = 2 },
        .timestamp_type_tokens = .{ .start = 0, .end = 1 },
        .timestamp_value_tokens = .{ .start = 1, .end = 2 },
    };
    const strict_timestamp_literal_expression = db_mod.types.RelationalRowsExpression{
        .kind = .value,
        .value_json = "1000000000",
    };
    try validateGeneratedRowExpressionIdentityStrictWithContext(
        .{ .alloc = alloc },
        &strict_timestamp_literal_tokens,
        0,
        2,
        strict_timestamp_literal_expression,
        &strict_timestamp_literal_ast,
    );
    const stale_strict_timestamp_literal_expression = db_mod.types.RelationalRowsExpression{
        .kind = .value,
        .value_json = "0",
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedRowExpressionIdentityStrictWithContext(
            .{ .alloc = alloc },
            &strict_timestamp_literal_tokens,
            0,
            2,
            stale_strict_timestamp_literal_expression,
            &strict_timestamp_literal_ast,
        ),
    );

    const additive_tokens = [_]Token{
        .{ .kind = .identifier, .text = "amount" },
        .{ .kind = .plus, .text = "+" },
        .{ .kind = .identifier, .text = "bonus" },
    };
    var additive_left_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 0, .end = 1 },
    };
    var additive_right_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 2, .end = 3 },
    };
    const additive_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .additive,
        .tokens = .{ .start = 0, .end = 3 },
        .left_tokens = .{ .start = 0, .end = 1 },
        .left_expression = &additive_left_expression,
        .operator_tokens = .{ .start = 1, .end = 2 },
        .right_tokens = .{ .start = 2, .end = 3 },
        .right_expression = &additive_right_expression,
    };
    try validateGeneratedExpressionPayloads(&additive_tokens, additive_expression);
    var stale_additive_right_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 0, .end = 1 },
    };
    const stale_additive_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .additive,
        .tokens = .{ .start = 0, .end = 3 },
        .left_tokens = .{ .start = 0, .end = 1 },
        .left_expression = &additive_left_expression,
        .operator_tokens = .{ .start = 1, .end = 2 },
        .right_tokens = .{ .start = 2, .end = 3 },
        .right_expression = &stale_additive_right_expression,
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedExpressionPayloads(&additive_tokens, stale_additive_expression),
    );

    const function_tokens = [_]Token{
        .{ .kind = .identifier, .text = "lower" },
        .{ .kind = .lparen, .text = "(" },
        .{ .kind = .identifier, .text = "status" },
        .{ .kind = .comma, .text = "," },
        .{ .kind = .identifier, .text = "fallback" },
        .{ .kind = .rparen, .text = ")" },
    };
    var function_argument_items = [_]generated_parser.GeneratedSqlTokenRange{
        .{ .start = 2, .end = 3 },
        .{ .start = 4, .end = 5 },
    };
    var function_argument_expressions = [_]generated_parser.GeneratedSqlExpressionAst{
        .{ .kind = .token_range, .tokens = .{ .start = 2, .end = 3 } },
        .{ .kind = .token_range, .tokens = .{ .start = 4, .end = 5 } },
    };
    var function_argument_alias_items = [_]?generated_parser.GeneratedSqlTokenRange{ null, null };
    var function_argument_alias_name_items = [_]?generated_parser.GeneratedSqlTokenRange{ null, null };
    var function_argument_direction_items = [_]?generated_parser.GeneratedSqlTokenRange{ null, null };
    var function_argument_directions = [_]?generated_parser.GeneratedSqlOrderDirection{ null, null };
    var function_argument_order_using_items = [_]?generated_parser.GeneratedSqlTokenRange{ null, null };
    var function_argument_nulls_order_items = [_]?generated_parser.GeneratedSqlTokenRange{ null, null };
    var function_argument_nulls_orders = [_]?generated_parser.GeneratedSqlNullsOrder{ null, null };
    const function_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .function_call,
        .tokens = .{ .start = 0, .end = 6 },
        .function_name_tokens = .{ .start = 0, .end = 1 },
        .argument_tokens = .{ .start = 2, .end = 5 },
        .argument_value_tokens = .{ .start = 2, .end = 5 },
        .argument_items = .{
            .first_tokens = function_argument_items[0],
            .last_tokens = function_argument_items[1],
            .items = function_argument_items[0..],
            .expression_items = function_argument_items[0..],
            .alias_items = function_argument_alias_items[0..],
            .alias_name_items = function_argument_alias_name_items[0..],
            .direction_items = function_argument_direction_items[0..],
            .directions = function_argument_directions[0..],
            .order_using_operator_items = function_argument_order_using_items[0..],
            .nulls_order_items = function_argument_nulls_order_items[0..],
            .nulls_orders = function_argument_nulls_orders[0..],
            .expressions = function_argument_expressions[0..],
            .count = 2,
        },
    };
    try validateGeneratedExpressionPayloads(&function_tokens, function_expression);

    var stale_function_argument_expressions = [_]generated_parser.GeneratedSqlExpressionAst{
        .{ .kind = .token_range, .tokens = .{ .start = 4, .end = 5 } },
        .{ .kind = .token_range, .tokens = .{ .start = 4, .end = 5 } },
    };
    const stale_function_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .function_call,
        .tokens = .{ .start = 0, .end = 6 },
        .function_name_tokens = .{ .start = 0, .end = 1 },
        .argument_tokens = .{ .start = 2, .end = 5 },
        .argument_value_tokens = .{ .start = 2, .end = 5 },
        .argument_items = .{
            .first_tokens = function_argument_items[0],
            .last_tokens = function_argument_items[1],
            .items = function_argument_items[0..],
            .expression_items = function_argument_items[0..],
            .alias_items = function_argument_alias_items[0..],
            .alias_name_items = function_argument_alias_name_items[0..],
            .direction_items = function_argument_direction_items[0..],
            .directions = function_argument_directions[0..],
            .order_using_operator_items = function_argument_order_using_items[0..],
            .nulls_order_items = function_argument_nulls_order_items[0..],
            .nulls_orders = function_argument_nulls_orders[0..],
            .expressions = stale_function_argument_expressions[0..],
            .count = 2,
        },
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedExpressionPayloads(&function_tokens, stale_function_expression),
    );

    const cast_tokens = [_]Token{
        .{ .kind = .identifier, .text = "cast", .keyword = .cast },
        .{ .kind = .lparen, .text = "(" },
        .{ .kind = .identifier, .text = "amount" },
        .{ .kind = .identifier, .text = "as", .keyword = .as },
        .{ .kind = .identifier, .text = "integer" },
        .{ .kind = .rparen, .text = ")" },
    };
    var cast_child_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 2, .end = 3 },
    };
    const cast_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .cast,
        .tokens = .{ .start = 0, .end = 6 },
        .cast_expression_tokens = .{ .start = 2, .end = 3 },
        .cast_expression = &cast_child_expression,
        .cast_type_tokens = .{ .start = 4, .end = 5 },
    };
    try validateGeneratedExpressionPayloads(&cast_tokens, cast_expression);

    var stale_cast_child_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 4, .end = 5 },
    };
    const stale_cast_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .cast,
        .tokens = .{ .start = 0, .end = 6 },
        .cast_expression_tokens = .{ .start = 2, .end = 3 },
        .cast_expression = &stale_cast_child_expression,
        .cast_type_tokens = .{ .start = 4, .end = 5 },
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedExpressionPayloads(&cast_tokens, stale_cast_expression),
    );

    const extract_tokens = [_]Token{
        .{ .kind = .identifier, .text = "extract", .keyword = .extract },
        .{ .kind = .lparen, .text = "(" },
        .{ .kind = .identifier, .text = "dow" },
        .{ .kind = .identifier, .text = "from", .keyword = .from },
        .{ .kind = .identifier, .text = "amount" },
        .{ .kind = .rparen, .text = ")" },
    };
    var extract_source_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 4, .end = 5 },
    };
    const extract_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .extract_expression,
        .tokens = .{ .start = 0, .end = 6 },
        .extract_field_tokens = .{ .start = 2, .end = 3 },
        .extract_source_tokens = .{ .start = 4, .end = 5 },
        .extract_source_expression = &extract_source_expression,
    };
    try validateGeneratedExpressionPayloads(&extract_tokens, extract_expression);

    var stale_extract_source_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 2, .end = 3 },
    };
    const stale_extract_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .extract_expression,
        .tokens = .{ .start = 0, .end = 6 },
        .extract_field_tokens = .{ .start = 2, .end = 3 },
        .extract_source_tokens = .{ .start = 4, .end = 5 },
        .extract_source_expression = &stale_extract_source_expression,
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedExpressionPayloads(&extract_tokens, stale_extract_expression),
    );

    const interval_tokens = [_]Token{
        .{ .kind = .identifier, .text = "interval", .keyword = .interval },
        .{ .kind = .string, .text = "1 day" },
    };
    const interval_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .interval_literal,
        .tokens = .{ .start = 0, .end = 2 },
        .interval_value_tokens = .{ .start = 1, .end = 2 },
    };
    try validateGeneratedExpressionPayloads(&interval_tokens, interval_expression);
    const stale_interval_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .interval_literal,
        .tokens = .{ .start = 0, .end = 2 },
        .interval_value_tokens = .{ .start = 0, .end = 1 },
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedExpressionPayloads(&interval_tokens, stale_interval_expression),
    );

    const timestamp_tokens = [_]Token{
        .{ .kind = .identifier, .text = "timestamp", .keyword = .timestamp },
        .{ .kind = .string, .text = "2026-07-02 10:30:00" },
    };
    const timestamp_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .timestamp_literal,
        .tokens = .{ .start = 0, .end = 2 },
        .timestamp_type_tokens = .{ .start = 0, .end = 1 },
        .timestamp_value_tokens = .{ .start = 1, .end = 2 },
    };
    try validateGeneratedExpressionPayloads(&timestamp_tokens, timestamp_expression);
    const stale_timestamp_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .timestamp_literal,
        .tokens = .{ .start = 0, .end = 2 },
        .timestamp_type_tokens = .{ .start = 0, .end = 1 },
        .timestamp_value_tokens = .{ .start = 0, .end = 1 },
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedExpressionPayloads(&timestamp_tokens, stale_timestamp_expression),
    );

    const current_timestamp_tokens = [_]Token{
        .{ .kind = .identifier, .text = "current_timestamp", .keyword = .current_timestamp },
        .{ .kind = .lparen, .text = "(" },
        .{ .kind = .number, .text = "6" },
        .{ .kind = .rparen, .text = ")" },
    };
    const current_timestamp_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .current_timestamp,
        .tokens = .{ .start = 0, .end = 4 },
        .current_timestamp_precision_tokens = .{ .start = 2, .end = 3 },
    };
    try validateGeneratedExpressionPayloads(&current_timestamp_tokens, current_timestamp_expression);
    const stale_current_timestamp_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .current_timestamp,
        .tokens = .{ .start = 0, .end = 4 },
        .current_timestamp_precision_tokens = .{ .start = 1, .end = 2 },
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedExpressionPayloads(&current_timestamp_tokens, stale_current_timestamp_expression),
    );

    const current_date_tokens = [_]Token{
        .{ .kind = .identifier, .text = "current_date", .keyword = .current_date },
    };
    const current_date_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .current_date,
        .tokens = .{ .start = 0, .end = 1 },
    };
    try validateGeneratedExpressionPayloads(&current_date_tokens, current_date_expression);
    const stale_current_date_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .current_date,
        .tokens = .{ .start = 0, .end = 1 },
        .current_timestamp_precision_tokens = .{ .start = 0, .end = 1 },
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedExpressionPayloads(&current_date_tokens, stale_current_date_expression),
    );

    const case_tokens = [_]Token{
        .{ .kind = .identifier, .text = "case", .keyword = .case },
        .{ .kind = .identifier, .text = "when", .keyword = .when },
        .{ .kind = .identifier, .text = "status" },
        .{ .kind = .identifier, .text = "is", .keyword = .is },
        .{ .kind = .identifier, .text = "null", .keyword = .null },
        .{ .kind = .identifier, .text = "then", .keyword = .then },
        .{ .kind = .string, .text = "missing" },
        .{ .kind = .identifier, .text = "else", .keyword = .@"else" },
        .{ .kind = .identifier, .text = "fallback" },
        .{ .kind = .identifier, .text = "end", .keyword = .end },
    };
    var case_first_condition_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .is_null,
        .tokens = .{ .start = 2, .end = 5 },
        .operator_tokens = .{ .start = 3, .end = 5 },
    };
    var case_first_result_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 6, .end = 7 },
    };
    var case_else_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .token_range,
        .tokens = .{ .start = 8, .end = 9 },
    };
    var case_condition_items = [_]generated_parser.GeneratedSqlTokenRange{.{ .start = 2, .end = 5 }};
    var case_condition_expressions = [_]generated_parser.GeneratedSqlExpressionAst{.{
        .kind = .is_null,
        .tokens = .{ .start = 2, .end = 5 },
        .operator_tokens = .{ .start = 3, .end = 5 },
    }};
    var case_result_items = [_]generated_parser.GeneratedSqlTokenRange{.{ .start = 6, .end = 7 }};
    var case_result_expressions = [_]generated_parser.GeneratedSqlExpressionAst{.{
        .kind = .token_range,
        .tokens = .{ .start = 6, .end = 7 },
    }};
    const case_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .case_expression,
        .tokens = .{ .start = 0, .end = 10 },
        .case_branch_count = 1,
        .case_first_when_tokens = .{ .start = 1, .end = 7 },
        .case_last_when_tokens = .{ .start = 1, .end = 7 },
        .case_first_condition_tokens = .{ .start = 2, .end = 5 },
        .case_first_condition_kind = .is_null,
        .case_first_condition = &case_first_condition_expression,
        .case_first_result_tokens = .{ .start = 6, .end = 7 },
        .case_first_result = &case_first_result_expression,
        .case_condition_items = .{
            .first_tokens = case_condition_items[0],
            .last_tokens = case_condition_items[0],
            .items = case_condition_items[0..],
            .expression_items = case_condition_items[0..],
            .expressions = case_condition_expressions[0..],
            .count = 1,
        },
        .case_result_items = .{
            .first_tokens = case_result_items[0],
            .last_tokens = case_result_items[0],
            .items = case_result_items[0..],
            .expression_items = case_result_items[0..],
            .expressions = case_result_expressions[0..],
            .count = 1,
        },
        .case_else_tokens = .{ .start = 7, .end = 9 },
        .case_else_expression_tokens = .{ .start = 8, .end = 9 },
        .case_else_expression = &case_else_expression,
    };
    try validateGeneratedExpressionPayloads(&case_tokens, case_expression);

    var stale_case_condition_expressions = [_]generated_parser.GeneratedSqlExpressionAst{.{
        .kind = .token_range,
        .tokens = .{ .start = 6, .end = 7 },
    }};
    const stale_case_expression = generated_parser.GeneratedSqlExpressionAst{
        .kind = .case_expression,
        .tokens = .{ .start = 0, .end = 10 },
        .case_branch_count = 1,
        .case_first_when_tokens = .{ .start = 1, .end = 7 },
        .case_last_when_tokens = .{ .start = 1, .end = 7 },
        .case_first_condition_tokens = .{ .start = 2, .end = 5 },
        .case_first_condition_kind = .is_null,
        .case_first_condition = &case_first_condition_expression,
        .case_first_result_tokens = .{ .start = 6, .end = 7 },
        .case_first_result = &case_first_result_expression,
        .case_condition_items = .{
            .first_tokens = case_condition_items[0],
            .last_tokens = case_condition_items[0],
            .items = case_condition_items[0..],
            .expression_items = case_condition_items[0..],
            .expressions = stale_case_condition_expressions[0..],
            .count = 1,
        },
        .case_result_items = .{
            .first_tokens = case_result_items[0],
            .last_tokens = case_result_items[0],
            .items = case_result_items[0..],
            .expression_items = case_result_items[0..],
            .expressions = case_result_expressions[0..],
            .count = 1,
        },
        .case_else_tokens = .{ .start = 7, .end = 9 },
        .case_else_expression_tokens = .{ .start = 8, .end = 9 },
        .case_else_expression = &case_else_expression,
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedExpressionPayloads(&case_tokens, stale_case_expression),
    );
}

test "sql expr generated validation checks predicate and row-expression identity" {
    try testGeneratedValidationChecksPredicateAndRowExpressionIdentity();
}
