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

const db_mod = @import("../storage/db/mod.zig");
const expr_generated = @import("expr/generated.zig");
const expr_generated_validate = @import("expr/generated_validate.zig");
const expr_parse = @import("expr/parse.zig");
const expr_projection = @import("expr/projection.zig");
const expr_token = @import("expr/token.zig");
const grammar = @import("grammar.zig");
const generated_parser = @import("generated_parser.zig");
const parser = @import("parser.zig");
const plan_mod = @import("plan.zig");
const strings = @import("strings.zig");
const token_mod = @import("token.zig");
const value_mod = @import("value.zig");

pub const Token = token_mod.Token;
pub const TokenKeyword = token_mod.TokenKeyword;

pub const GeneratedLimitValue = struct {
    value: ?u32,
};

fn parseGeneratedPaginationNullableU32Expression(
    tokens: []const Token,
    expression: generated_parser.GeneratedSqlExpressionAst,
    expected_range: generated_parser.GeneratedSqlTokenRange,
    params: []const value_mod.SqlValue,
) !?u32 {
    const expression_tokens = expression.tokens orelse return error.UnsupportedSqlShape;
    if (!expr_generated.generatedTokenRangeEqual(expression_tokens, expected_range)) return error.UnsupportedSqlShape;
    switch (expression.kind) {
        .token_range => {
            var generated_pos = expected_range.start;
            const value = try value_mod.parseNullableSqlU32Value(tokens, &generated_pos, params);
            if (generated_pos != expected_range.end) return error.UnsupportedSqlShape;
            return value;
        },
        .unary_positive => {
            const operator_tokens = expression.operator_tokens orelse return error.UnsupportedSqlShape;
            if (operator_tokens.start != expected_range.start or operator_tokens.end != expected_range.start + 1) return error.UnsupportedSqlShape;
            if (operator_tokens.end > tokens.len or tokens[operator_tokens.start].kind != .plus) return error.UnsupportedSqlShape;
            const value_range = expression.right_tokens orelse return error.UnsupportedSqlShape;
            if (value_range.start != operator_tokens.end or value_range.end != expected_range.end) return error.UnsupportedSqlShape;
            const right_expression = expression.right_expression orelse return error.UnsupportedSqlShape;
            if (right_expression.kind != .token_range) return error.UnsupportedSqlShape;
            if (!expr_generated.generatedTokenRangeEqual(right_expression.tokens orelse return error.UnsupportedSqlShape, value_range)) return error.UnsupportedSqlShape;
            var generated_pos = value_range.start;
            const value = try value_mod.parseNullableSqlU32Value(tokens, &generated_pos, params);
            if (generated_pos != value_range.end) return error.UnsupportedSqlShape;
            return value;
        },
        else => return error.UnsupportedSqlShape,
    }
}

fn validateGeneratedOptionalChildExpressionGroup(
    range: ?generated_parser.GeneratedSqlTokenRange,
    kind: ?generated_parser.GeneratedSqlExpressionKind,
    expression: ?*const generated_parser.GeneratedSqlExpressionAst,
) !void {
    if (range == null and (kind != null or expression != null)) return error.UnsupportedSqlShape;
}

pub fn validateGeneratedOrderListForClause(
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
        try validateGeneratedExpressionPayloads(tokens, list.expressions[index]);

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

pub fn generatedOrderClauseEnd(
    tokens: []const Token,
    keyword_index: usize,
    pos: usize,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
) !?usize {
    const read = generated_read_ast orelse return null;
    if (keyword_index + 1 >= tokens.len or !tokens[keyword_index].matchesKeywordTag(.order) or !tokens[keyword_index + 1].matchesKeywordTag(.by)) return null;
    const range = read.order_tokens orelse return error.UnsupportedSqlShape;
    if (range.start != pos or range.end > tokens.len) return error.UnsupportedSqlShape;
    try validateGeneratedOrderListForClause(tokens, range, read.order_items);
    return range.end;
}

pub fn validateGeneratedExpressionListForClause(
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
        try validateGeneratedExpressionPayloads(tokens, list.expressions[index]);
    }
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

fn validateGeneratedChildExpressionPayloads(
    tokens: []const Token,
    expected_range: generated_parser.GeneratedSqlTokenRange,
    expected_kind: ?generated_parser.GeneratedSqlExpressionKind,
    expression: ?*const generated_parser.GeneratedSqlExpressionAst,
) anyerror!void {
    const child = expression orelse return error.UnsupportedSqlShape;
    const child_tokens = child.tokens orelse return error.UnsupportedSqlShape;
    if (!expr_generated.generatedTokenRangeEqual(child_tokens, expected_range)) return error.UnsupportedSqlShape;
    if (expected_kind) |kind| {
        if (child.kind != kind) return error.UnsupportedSqlShape;
    }
    try validateGeneratedExpressionPayloads(tokens, child.*);
}

pub fn validateGeneratedEmptyList(list: generated_parser.GeneratedSqlListAst) !void {
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

fn validateGeneratedEmptyExpression(expression: generated_parser.GeneratedSqlExpressionAst) !void {
    if (!std.meta.eql(expression, generated_parser.GeneratedSqlExpressionAst{})) return error.UnsupportedSqlShape;
}

fn validateGeneratedExpressionRangeListPayloads(
    tokens: []const Token,
    owner: generated_parser.GeneratedSqlTokenRange,
    list: generated_parser.GeneratedSqlListAst,
    expected_count: usize,
) !void {
    if (expected_count == 0) return validateGeneratedEmptyList(list);
    if (list.count != expected_count or list.items.len != expected_count or list.expression_items.len != expected_count or list.expressions.len != expected_count) return error.UnsupportedSqlShape;
    if (list.alias_items.len != 0 or list.alias_name_items.len != 0) return error.UnsupportedSqlShape;
    if (list.direction_items.len != 0 or list.directions.len != 0) return error.UnsupportedSqlShape;
    if (list.order_using_operator_items.len != 0 or list.nulls_order_items.len != 0 or list.nulls_orders.len != 0) return error.UnsupportedSqlShape;
    if (list.first_tokens == null or !expr_generated.generatedTokenRangeEqual(list.first_tokens.?, list.items[0])) return error.UnsupportedSqlShape;
    if (list.last_tokens == null or !expr_generated.generatedTokenRangeEqual(list.last_tokens.?, list.items[list.count - 1])) return error.UnsupportedSqlShape;
    for (list.items, 0..) |item, index| {
        if (item.start < owner.start or item.end > owner.end or item.start >= item.end) return error.UnsupportedSqlShape;
        if (!expr_generated.generatedTokenRangeEqual(list.expression_items[index], item)) return error.UnsupportedSqlShape;
        if (!expr_generated.generatedTokenRangeEqual(list.expressions[index].tokens orelse return error.UnsupportedSqlShape, item)) return error.UnsupportedSqlShape;
        try validateGeneratedExpressionPayloads(tokens, list.expressions[index]);
    }
}

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

pub fn validateGeneratedExpressionFamilyPayloads(
    tokens: []const Token,
    expression: generated_parser.GeneratedSqlExpressionAst,
    expression_tokens: generated_parser.GeneratedSqlTokenRange,
) anyerror!void {
    if (expression.kind == .grouped) {
        if (expression.inner_tokens == null) return error.UnsupportedSqlShape;
    } else if (expression.kind == .subquery) {
        if (expression.inner_tokens == null or expression.inner_expression_kind != null or expression.inner_expression != null) return error.UnsupportedSqlShape;
    } else if (expression.inner_tokens != null or expression.inner_expression_kind != null or expression.inner_expression != null) {
        return error.UnsupportedSqlShape;
    }

    if (expression.kind != .subquery and
        (expression.subquery_read_kind != null or
            expression.subquery_select_tokens != null or
            expression.subquery_projection_tokens != null or
            expression.subquery_source_tokens != null or
            expression.subquery_where_tokens != null or
            expression.subquery_where_expression_kind != null or
            expression.subquery_where_expression != null or
            expression.subquery_set_operation_tokens != null or
            expression.subquery_set_operation != null or
            expression.subquery_tail != null))
    {
        return error.UnsupportedSqlShape;
    }
    if (expression.kind != .subquery) try validateGeneratedEmptyList(expression.subquery_projection_items);

    if (expression.kind != .function_call and
        (expression.function_name_tokens != null or
            expression.aggregate_function_kind != null or
            expression.window_function_kind != null or
            expression.argument_tokens != null or
            expression.argument_distinct_tokens != null or
            expression.argument_value_tokens != null or
            expression.argument_order_tokens != null or
            expression.within_group_tokens != null or
            expression.within_group_order_tokens != null or
            expression.filter_tokens != null or
            expression.filter_predicate_tokens != null or
            expression.filter_expression_kind != null or
            expression.filter_expression != null or
            expression.over_tokens != null or
            expression.over_name_tokens != null or
            expression.over_definition_tokens != null or
            expression.over_partition_tokens != null or
            expression.over_order_tokens != null or
            expression.over_frame_tokens != null or
            expression.over_frame_unit != null or
            expression.over_frame_start_bound != null or
            expression.over_frame_start_expression_tokens != null or
            expression.over_frame_start_expression_kind != null or
            expression.over_frame_start_expression != null or
            expression.over_frame_end_bound != null or
            expression.over_frame_end_expression_tokens != null or
            expression.over_frame_end_expression_kind != null or
            expression.over_frame_end_expression != null))
    {
        return error.UnsupportedSqlShape;
    }
    if (expression.kind != .function_call) {
        if (expression.kind == .grouped and expression.argument_items.count != 0) {
            const inner_tokens = expression.inner_tokens orelse return error.UnsupportedSqlShape;
            try validateGeneratedExpressionListForClause(tokens, inner_tokens, expression.argument_items);
        } else {
            try validateGeneratedEmptyList(expression.argument_items);
        }
        try validateGeneratedEmptyList(expression.argument_order_items);
        try validateGeneratedEmptyList(expression.within_group_order_items);
        try validateGeneratedEmptyList(expression.over_partition_items);
        try validateGeneratedEmptyList(expression.over_order_items);
    }

    if (expression.kind == .array_constructor) {
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
    } else {
        if (expression.array_tokens != null) return error.UnsupportedSqlShape;
        try validateGeneratedEmptyList(expression.array_items);
    }

    if (expression.kind == .cast) {
        if (expression.cast_expression_tokens == null or expression.cast_type_tokens == null) return error.UnsupportedSqlShape;
    } else if (expression.cast_expression_tokens != null or expression.cast_expression_kind != null or expression.cast_expression != null or expression.cast_type_tokens != null) {
        return error.UnsupportedSqlShape;
    }

    if (expression.kind == .case_expression) {
        if (expression.case_branch_count == 0 or
            expression.case_first_when_tokens == null or
            expression.case_last_when_tokens == null or
            expression.case_first_condition_tokens == null or
            expression.case_first_result_tokens == null)
        {
            return error.UnsupportedSqlShape;
        }
        try validateGeneratedExpressionRangeListPayloads(tokens, expression_tokens, expression.case_condition_items, expression.case_branch_count);
        try validateGeneratedExpressionRangeListPayloads(tokens, expression_tokens, expression.case_result_items, expression.case_branch_count);
    } else if (expression.case_branch_count != 0 or
        expression.case_first_when_tokens != null or
        expression.case_last_when_tokens != null or
        expression.case_first_condition_tokens != null or
        expression.case_first_condition_kind != null or
        expression.case_first_condition != null or
        expression.case_first_result_tokens != null or
        expression.case_first_result_kind != null or
        expression.case_first_result != null or
        expression.case_else_tokens != null or
        expression.case_else_expression_tokens != null or
        expression.case_else_expression_kind != null or
        expression.case_else_expression != null)
    {
        return error.UnsupportedSqlShape;
    }
    if (expression.kind != .case_expression) {
        try validateGeneratedEmptyList(expression.case_condition_items);
        try validateGeneratedEmptyList(expression.case_result_items);
    }

    if (expression.kind == .logical_and or expression.kind == .logical_or) {
        if (expression.boolean_condition_count == 0 or
            expression.boolean_first_condition_tokens == null or
            expression.boolean_last_condition_tokens == null)
        {
            return error.UnsupportedSqlShape;
        }
        try validateGeneratedExpressionRangeListPayloads(tokens, expression_tokens, expression.boolean_condition_items, expression.boolean_condition_count);
    } else if (expression.boolean_condition_count != 0 or
        expression.boolean_first_condition_tokens != null or
        expression.boolean_first_condition_kind != null or
        expression.boolean_first_condition != null or
        expression.boolean_last_condition_tokens != null or
        expression.boolean_last_condition_kind != null or
        expression.boolean_last_condition != null)
    {
        return error.UnsupportedSqlShape;
    }
    if (expression.kind != .logical_and and expression.kind != .logical_or) try validateGeneratedEmptyList(expression.boolean_condition_items);

    if (expression.kind == .interval_literal) {
        if (expression.interval_value_tokens == null) return error.UnsupportedSqlShape;
        const value_tokens = expression.interval_value_tokens.?;
        if (expression_tokens.end != expression_tokens.start + 2 or
            expression_tokens.end > tokens.len or
            !tokens[expression_tokens.start].matchesKeywordTag(.interval) or
            tokens[expression_tokens.start + 1].kind != .string or
            value_tokens.start != expression_tokens.start + 1 or
            value_tokens.end != expression_tokens.end)
        {
            return error.UnsupportedSqlShape;
        }
    } else if (expression.interval_value_tokens != null) {
        return error.UnsupportedSqlShape;
    }

    if (expression.kind == .timestamp_literal) {
        if (expression.timestamp_type_tokens == null or expression.timestamp_value_tokens == null) return error.UnsupportedSqlShape;
        const type_tokens = expression.timestamp_type_tokens.?;
        const value_tokens = expression.timestamp_value_tokens.?;
        if (expression_tokens.end != expression_tokens.start + 2 or
            expression_tokens.end > tokens.len or
            (!tokens[expression_tokens.start].matchesKeywordTag(.timestamp) and !tokens[expression_tokens.start].matchesKeywordTag(.timestamptz)) or
            tokens[expression_tokens.start + 1].kind != .string or
            type_tokens.start != expression_tokens.start or
            type_tokens.end != expression_tokens.start + 1 or
            value_tokens.start != expression_tokens.start + 1 or
            value_tokens.end != expression_tokens.end)
        {
            return error.UnsupportedSqlShape;
        }
    } else if (expression.timestamp_type_tokens != null or expression.timestamp_value_tokens != null) {
        return error.UnsupportedSqlShape;
    }

    switch (expression.kind) {
        .current_date => {
            if (expression_tokens.end != expression_tokens.start + 1 or
                expression_tokens.end > tokens.len or
                !tokens[expression_tokens.start].matchesKeywordTag(.current_date) or
                expression.current_timestamp_precision_tokens != null)
            {
                return error.UnsupportedSqlShape;
            }
        },
        .current_timestamp => {
            if (expression_tokens.end > tokens.len or
                expression_tokens.start >= expression_tokens.end or
                !tokens[expression_tokens.start].matchesKeywordTag(.current_timestamp))
            {
                return error.UnsupportedSqlShape;
            }
            if (expression.current_timestamp_precision_tokens) |precision_tokens| {
                if (expression_tokens.end != expression_tokens.start + 4 or
                    tokens[expression_tokens.start + 1].kind != .lparen or
                    tokens[expression_tokens.start + 2].kind != .number or
                    tokens[expression_tokens.start + 3].kind != .rparen or
                    precision_tokens.start != expression_tokens.start + 2 or
                    precision_tokens.end != expression_tokens.start + 3)
                {
                    return error.UnsupportedSqlShape;
                }
            } else if (expression_tokens.end != expression_tokens.start + 1) {
                return error.UnsupportedSqlShape;
            }
        },
        else => if (expression.current_timestamp_precision_tokens != null) return error.UnsupportedSqlShape,
    }

    if (expression.kind == .extract_expression) {
        if (expression.extract_field_tokens == null or expression.extract_source_tokens == null) return error.UnsupportedSqlShape;
        const field_tokens = expression.extract_field_tokens.?;
        const source_tokens = expression.extract_source_tokens.?;
        if (expression_tokens.end - expression_tokens.start < 6 or
            expression_tokens.end > tokens.len or
            !tokens[expression_tokens.start].matchesKeywordTag(.extract) or
            tokens[expression_tokens.start + 1].kind != .lparen or
            tokens[expression_tokens.start + 2].kind != .identifier or
            !tokens[expression_tokens.start + 3].matchesKeywordTag(.from) or
            tokens[expression_tokens.end - 1].kind != .rparen or
            field_tokens.start != expression_tokens.start + 2 or
            field_tokens.end != expression_tokens.start + 3 or
            source_tokens.start != expression_tokens.start + 4 or
            source_tokens.end != expression_tokens.end - 1 or
            source_tokens.start >= source_tokens.end)
        {
            return error.UnsupportedSqlShape;
        }
    } else if (expression.extract_field_tokens != null or expression.extract_source_tokens != null or expression.extract_source_expression_kind != null or expression.extract_source_expression != null) {
        return error.UnsupportedSqlShape;
    }

    if (expr_generated.generatedExpressionKindUsesOperatorPayload(expression.kind)) {
        const operator_tokens = expression.operator_tokens orelse return error.UnsupportedSqlShape;
        if (operator_tokens.start < expression_tokens.start or operator_tokens.end > expression_tokens.end or operator_tokens.start >= operator_tokens.end) return error.UnsupportedSqlShape;
        try validateGeneratedExpressionOperatorTokens(tokens, expression.kind, operator_tokens);
        if (expression.negation_tokens) |negation_tokens| {
            if (negation_tokens.start < expression_tokens.start or negation_tokens.end > expression_tokens.end or negation_tokens.start >= negation_tokens.end) return error.UnsupportedSqlShape;
        }
        if (expression.quantifier_tokens) |quantifier_tokens| {
            if (quantifier_tokens.start < expression_tokens.start or quantifier_tokens.end > expression_tokens.end or quantifier_tokens.end != quantifier_tokens.start + 1) return error.UnsupportedSqlShape;
            if (!tokens[quantifier_tokens.start].matchesKeywordTag(.any) and !tokens[quantifier_tokens.start].matchesKeywordTag(.some) and !tokens[quantifier_tokens.start].matchesKeywordTag(.all)) return error.UnsupportedSqlShape;
        }
        if (expression.between_modifier_tokens) |modifier_tokens| {
            if (expression.kind != .between and expression.kind != .not_between) return error.UnsupportedSqlShape;
            if (modifier_tokens.start < expression_tokens.start or modifier_tokens.end > expression_tokens.end or modifier_tokens.end != modifier_tokens.start + 1) return error.UnsupportedSqlShape;
            const modifier = expression.between_modifier orelse return error.UnsupportedSqlShape;
            switch (modifier) {
                .asymmetric => if (!tokens[modifier_tokens.start].matchesKeywordTag(.asymmetric)) return error.UnsupportedSqlShape,
                .symmetric => if (!tokens[modifier_tokens.start].matchesKeywordTag(.symmetric)) return error.UnsupportedSqlShape,
            }
        } else if (expression.between_modifier != null) {
            return error.UnsupportedSqlShape;
        }
        if (expression.escape_tokens != null and !expr_generated.generatedExpressionKindAllowsEscapePayload(expression.kind)) return error.UnsupportedSqlShape;
    } else if (expression.negation_tokens != null or
        expression.operator_tokens != null or
        expression.between_modifier_tokens != null or
        expression.between_modifier != null or
        expression.quantifier_tokens != null or
        expression.left_tokens != null or
        expression.left_expression_kind != null or
        expression.left_expression != null or
        expression.between_lower_tokens != null or
        expression.between_lower_expression_kind != null or
        expression.between_lower_expression != null or
        expression.between_upper_tokens != null or
        expression.between_upper_expression_kind != null or
        expression.between_upper_expression != null or
        expression.right_tokens != null or
        expression.right_expression_kind != null or
        expression.right_expression != null or
        expression.escape_tokens != null or
        expression.escape_expression_kind != null or
        expression.escape_expression != null)
    {
        return error.UnsupportedSqlShape;
    }
}

fn validateGeneratedSetOperationPayloads(
    tokens: []const Token,
    range: generated_parser.GeneratedSqlTokenRange,
    set_operation: generated_parser.GeneratedSqlSetOperationAst,
) anyerror!void {
    if (range.start >= range.end or range.end > tokens.len) return error.UnsupportedSqlShape;
    if (set_operation.tokens == null or !expr_generated.generatedTokenRangeEqual(set_operation.tokens.?, range)) return error.UnsupportedSqlShape;
    const operator_tokens = set_operation.operator_tokens orelse return error.UnsupportedSqlShape;
    if (operator_tokens.start != range.start or operator_tokens.end != range.start + 1) return error.UnsupportedSqlShape;
    if (set_operation.kind == null) return error.UnsupportedSqlShape;
    switch (set_operation.kind.?) {
        .@"union" => if (!tokens[operator_tokens.start].matchesKeywordTag(.@"union")) return error.UnsupportedSqlShape,
        .intersect => if (!tokens[operator_tokens.start].matchesKeywordTag(.intersect)) return error.UnsupportedSqlShape,
        .except => if (!tokens[operator_tokens.start].matchesKeywordTag(.except)) return error.UnsupportedSqlShape,
    }
    const right_query_tokens = set_operation.right_query_tokens orelse return error.UnsupportedSqlShape;
    const right_select_tokens = set_operation.right_select_tokens orelse return error.UnsupportedSqlShape;
    if (right_query_tokens.start < range.start or right_query_tokens.end > range.end or right_query_tokens.start >= right_query_tokens.end) return error.UnsupportedSqlShape;
    if (right_query_tokens.end < range.end and !grammar.nextIsSelectSetOperationKeyword(tokens, right_query_tokens.end)) return error.UnsupportedSqlShape;
    if (right_select_tokens.start != right_query_tokens.start or right_select_tokens.end != right_select_tokens.start + 1) return error.UnsupportedSqlShape;
    if (!tokens[right_select_tokens.start].matchesKeywordTag(.select)) return error.UnsupportedSqlShape;
    if (set_operation.all_tokens) |all_tokens| {
        if (all_tokens.start != operator_tokens.end or all_tokens.end != all_tokens.start + 1) return error.UnsupportedSqlShape;
        if (!tokens[all_tokens.start].matchesKeywordTag(.all)) return error.UnsupportedSqlShape;
        if (right_query_tokens.start != all_tokens.end) return error.UnsupportedSqlShape;
    } else if (right_query_tokens.start != operator_tokens.end) {
        return error.UnsupportedSqlShape;
    }
    const right_projection_start = if (right_select_tokens.end < right_query_tokens.end and tokens[right_select_tokens.end].matchesKeywordTag(.all)) all_start: {
        if (set_operation.right_distinct_tokens != null) return error.UnsupportedSqlShape;
        try validateGeneratedEmptyList(set_operation.right_distinct_on_items);
        break :all_start right_select_tokens.end + 1;
    } else if (set_operation.right_distinct_tokens) |distinct_range| distinct_start: {
        if (distinct_range.start != right_select_tokens.end) return error.UnsupportedSqlShape;
        if (distinct_range.end > right_query_tokens.end) return error.UnsupportedSqlShape;
        const distinct = try validateGeneratedDistinctClause(tokens, distinct_range, set_operation.right_distinct_on_items);
        break :distinct_start distinct.end;
    } else no_distinct: {
        try validateGeneratedEmptyList(set_operation.right_distinct_on_items);
        break :no_distinct right_select_tokens.end;
    };
    if (set_operation.right_projection_tokens) |projection_range| {
        if (projection_range.start < right_query_tokens.start or projection_range.end > right_query_tokens.end) return error.UnsupportedSqlShape;
        if (projection_range.start != right_projection_start) return error.UnsupportedSqlShape;
        try validateGeneratedProjectionListForClause(tokens, projection_range, set_operation.right_projection_items);
        try validateGeneratedReadListBoundaryExpressions(tokens, set_operation.right_projection_items, set_operation.right_projection_first_expression, set_operation.right_projection_last_expression);
    } else {
        try validateGeneratedEmptyList(set_operation.right_projection_items);
        if (set_operation.right_projection_first_expression.tokens != null or set_operation.right_projection_last_expression.tokens != null) {
            return error.UnsupportedSqlShape;
        }
    }
    var previous_end = if (set_operation.right_projection_tokens) |projection_range| projection_range.end else right_projection_start;
    if (set_operation.right_source_tokens) |source_range| {
        if (source_range.start >= source_range.end or source_range.end > right_query_tokens.end) return error.UnsupportedSqlShape;
        if (previous_end + 1 != source_range.start or !tokens[previous_end].matchesKeywordTag(.from)) return error.UnsupportedSqlShape;
        try validateGeneratedOptionalRangeInside(set_operation.right_source_table_tokens, source_range);
        try validateGeneratedOptionalRangeInside(set_operation.right_source_alias_tokens, source_range);
        try validateGeneratedOptionalRangeInside(set_operation.right_source_alias_name_tokens, source_range);
        try validateGeneratedOptionalRangeInside(set_operation.right_source_system_time_tokens, source_range);
        try validateGeneratedOptionalRangeInside(set_operation.right_source_system_time_sequence_tokens, source_range);
        try validateGeneratedReadSystemTimePayloads(tokens, set_operation.right_source_tokens, set_operation.right_source_system_time_tokens, set_operation.right_source_system_time_sequence_tokens);
        try validateGeneratedSetOperationRightSourcePayload(tokens, set_operation);
        previous_end = source_range.end;
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
    if (set_operation.right_where_tokens) |where_range| {
        if (where_range.start < right_query_tokens.start or where_range.end > right_query_tokens.end) return error.UnsupportedSqlShape;
        if (previous_end + 1 != where_range.start or !tokens[previous_end].matchesKeywordTag(.where)) return error.UnsupportedSqlShape;
        if (!expr_generated.generatedTokenRangeEqual(set_operation.right_where_expression.tokens orelse return error.UnsupportedSqlShape, where_range)) return error.UnsupportedSqlShape;
        try validateGeneratedExpressionPayloads(tokens, set_operation.right_where_expression);
        previous_end = where_range.end;
    } else if (set_operation.right_where_expression.tokens != null) {
        return error.UnsupportedSqlShape;
    }
    if (previous_end != right_query_tokens.end) return error.UnsupportedSqlShape;
}

fn validateGeneratedSetOperationRightSourcePayload(
    tokens: []const Token,
    set_operation: generated_parser.GeneratedSqlSetOperationAst,
) !void {
    if (set_operation.right_join_items.len != 0 or set_operation.right_join_tree_root_index != null or set_operation.right_join_tree_depth != 0) {
        const source = set_operation.right_source_tokens orelse return error.UnsupportedSqlShape;
        if (set_operation.right_source_table_tokens != null or
            set_operation.right_source_alias_tokens != null or
            set_operation.right_source_alias_name_tokens != null or
            set_operation.right_source_system_time_tokens != null or
            set_operation.right_source_system_time_sequence_tokens != null)
        {
            return error.UnsupportedSqlShape;
        }
        var read = generated_parser.GeneratedSqlReadAst{
            .kind = .join,
            .statement_span = .{ .start = source.start, .end = source.end },
            .command_span = .{ .start = source.start, .end = source.end },
            .source_tokens = source,
            .join_tokens = source,
            .join_items = set_operation.right_join_items,
            .join_tree_root_index = set_operation.right_join_tree_root_index,
            .join_tree_depth = set_operation.right_join_tree_depth,
        };
        if (set_operation.right_join_items.len == 0) return error.UnsupportedSqlShape;
        const first = set_operation.right_join_items[0];
        read.join_operator_tokens = first.operator_tokens;
        read.join_kind = first.kind;
        read.join_left_tokens = first.left_tokens;
        read.join_right_tokens = first.right_tokens;
        read.join_predicate_tokens = first.predicate_tokens;
        try validateGeneratedJoinItemsMetadata(tokens, read);
        return;
    }
    const source_table = set_operation.right_source_table_tokens orelse {
        if (set_operation.right_source_alias_tokens != null or
            set_operation.right_source_alias_name_tokens != null or
            set_operation.right_source_system_time_tokens != null or
            set_operation.right_source_system_time_sequence_tokens != null)
        {
            return error.UnsupportedSqlShape;
        }
        const source = set_operation.right_source_tokens orelse return;
        if (generatedReadSourceLooksLikeSingleTableSource(tokens, source)) return error.UnsupportedSqlShape;
        return;
    };
    const source = set_operation.right_source_tokens orelse return error.UnsupportedSqlShape;
    try validateGeneratedRangeInside(source_table, source);
    var expected_table_start = source.start;
    if (tokens[expected_table_start].matchesKeywordTag(.only)) expected_table_start += 1;
    if (source_table.start != expected_table_start or source_table.end != expected_table_start + 1) return error.UnsupportedSqlShape;
    if (tokens[source_table.start].kind != .identifier) return error.UnsupportedSqlShape;
    const alias_end = try generatedSingleSourceAliasEnd(
        tokens,
        source_table,
        set_operation.right_source_alias_tokens,
        set_operation.right_source_alias_name_tokens,
    );
    const source_body_end = if (set_operation.right_source_system_time_tokens) |system_time| system_time.start else source.end;
    if (alias_end != source_body_end) return error.UnsupportedSqlShape;
}

pub fn validateGeneratedReadListBoundaryExpressions(
    tokens: []const Token,
    list: generated_parser.GeneratedSqlListAst,
    first_expression: generated_parser.GeneratedSqlExpressionAst,
    last_expression: generated_parser.GeneratedSqlExpressionAst,
) !void {
    if (list.count == 0) {
        if (first_expression.tokens != null or last_expression.tokens != null) return error.UnsupportedSqlShape;
        return;
    }
    if (list.expression_items.len != list.count) return error.UnsupportedSqlShape;
    if (!expr_generated.generatedTokenRangeEqual(first_expression.tokens orelse return error.UnsupportedSqlShape, list.expression_items[0])) return error.UnsupportedSqlShape;
    if (!expr_generated.generatedTokenRangeEqual(last_expression.tokens orelse return error.UnsupportedSqlShape, list.expression_items[list.count - 1])) return error.UnsupportedSqlShape;
    try validateGeneratedExpressionPayloads(tokens, first_expression);
    try validateGeneratedExpressionPayloads(tokens, last_expression);
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
    const exact_expression = expr_generated_validate.generatedExpressionForExactRange(expression, range) orelse return false;
    try validateGeneratedOptionalExpression(tokens, range, exact_expression.*);
    return true;
}

pub fn validateGeneratedOptionalReadExpression(
    tokens: []const Token,
    range: ?generated_parser.GeneratedSqlTokenRange,
    expression: generated_parser.GeneratedSqlExpressionAst,
) !void {
    try validateGeneratedOptionalExpression(tokens, range, expression);
}

pub fn validateGeneratedRangeInside(
    range: generated_parser.GeneratedSqlTokenRange,
    owner: generated_parser.GeneratedSqlTokenRange,
) !void {
    if (range.start < owner.start or range.end > owner.end or range.start >= range.end) return error.UnsupportedSqlShape;
}

pub fn validateGeneratedOptionalRangeInside(
    range: ?generated_parser.GeneratedSqlTokenRange,
    owner: generated_parser.GeneratedSqlTokenRange,
) !void {
    if (range) |value| try validateGeneratedRangeInside(value, owner);
}

pub fn validateGeneratedOptionalRangePrecededByKeyword(
    tokens: []const Token,
    range: ?generated_parser.GeneratedSqlTokenRange,
    keyword: TokenKeyword,
) !void {
    const value = range orelse return;
    if (value.start == 0 or !tokens[value.start - 1].matchesKeywordTag(keyword)) return error.UnsupportedSqlShape;
}

pub fn validateGeneratedOptionalRangePrecededByKeywordPair(
    tokens: []const Token,
    range: ?generated_parser.GeneratedSqlTokenRange,
    first: TokenKeyword,
    second: TokenKeyword,
) !void {
    const value = range orelse return;
    if (value.start < 2 or !tokens[value.start - 2].matchesKeywordTag(first) or !tokens[value.start - 1].matchesKeywordTag(second)) return error.UnsupportedSqlShape;
}

pub fn validateGeneratedRangeAfterKeyword(
    tokens: []const Token,
    previous_end: usize,
    range: generated_parser.GeneratedSqlTokenRange,
    keyword: TokenKeyword,
) !usize {
    if (previous_end >= tokens.len or previous_end + 1 != range.start) return error.UnsupportedSqlShape;
    if (!tokens[previous_end].matchesKeywordTag(keyword)) return error.UnsupportedSqlShape;
    return range.end;
}

pub fn validateGeneratedRangeAfterKeywordPair(
    tokens: []const Token,
    previous_end: usize,
    range: generated_parser.GeneratedSqlTokenRange,
    first: TokenKeyword,
    second: TokenKeyword,
) !usize {
    if (previous_end + 1 >= tokens.len or previous_end + 2 != range.start) return error.UnsupportedSqlShape;
    if (!tokens[previous_end].matchesKeywordTag(first) or !tokens[previous_end + 1].matchesKeywordTag(second)) return error.UnsupportedSqlShape;
    return range.end;
}

fn validateGeneratedNonSetReadClauseCursor(
    tokens: []const Token,
    projection_tokens: generated_parser.GeneratedSqlTokenRange,
    owner_end: usize,
    source_tokens: ?generated_parser.GeneratedSqlTokenRange,
    where_tokens: ?generated_parser.GeneratedSqlTokenRange,
    group_tokens: ?generated_parser.GeneratedSqlTokenRange,
    having_tokens: ?generated_parser.GeneratedSqlTokenRange,
    window_tokens: ?generated_parser.GeneratedSqlTokenRange,
    order_tokens: ?generated_parser.GeneratedSqlTokenRange,
    limit_tokens: ?generated_parser.GeneratedSqlTokenRange,
    offset_tokens: ?generated_parser.GeneratedSqlTokenRange,
    fetch_tokens: ?generated_parser.GeneratedSqlTokenRange,
    row_lock_tokens: ?generated_parser.GeneratedSqlTokenRange,
) !void {
    var previous_end = projection_tokens.end;
    if (source_tokens) |range| previous_end = try validateGeneratedRangeAfterKeyword(tokens, previous_end, range, .from);
    if (where_tokens) |range| previous_end = try validateGeneratedRangeAfterKeyword(tokens, previous_end, range, .where);
    if (group_tokens) |range| previous_end = try validateGeneratedRangeAfterKeywordPair(tokens, previous_end, range, .group, .by);
    if (having_tokens) |range| previous_end = try validateGeneratedRangeAfterKeyword(tokens, previous_end, range, .having);
    if (window_tokens) |range| previous_end = try validateGeneratedRangeAfterKeyword(tokens, previous_end, range, .window);
    if (order_tokens) |range| previous_end = try validateGeneratedRangeAfterKeywordPair(tokens, previous_end, range, .order, .by);
    if (limit_tokens) |range| previous_end = try validateGeneratedRangeAfterKeyword(tokens, previous_end, range, .limit);
    if (offset_tokens) |range| previous_end = try validateGeneratedRangeAfterKeyword(tokens, previous_end, range, .offset);
    if (fetch_tokens) |range| previous_end = try validateGeneratedRangeAfterKeyword(tokens, previous_end, range, .fetch);
    if (row_lock_tokens) |range| {
        if (previous_end != range.start or previous_end >= tokens.len) return error.UnsupportedSqlShape;
        if (!tokens[previous_end].matchesKeywordTag(.@"for")) return error.UnsupportedSqlShape;
        previous_end = range.end;
    }
    if (previous_end != owner_end) {
        if (previous_end >= tokens.len or previous_end + 1 != owner_end or tokens[previous_end].kind != .semicolon) return error.UnsupportedSqlShape;
    }
}

pub fn validateGeneratedReadResultTailCursor(
    tokens: []const Token,
    previous_end: usize,
    owner_end: usize,
    order_tokens: ?generated_parser.GeneratedSqlTokenRange,
    limit_tokens: ?generated_parser.GeneratedSqlTokenRange,
    offset_tokens: ?generated_parser.GeneratedSqlTokenRange,
    fetch_tokens: ?generated_parser.GeneratedSqlTokenRange,
) !void {
    var cursor = previous_end;
    if (order_tokens) |range| cursor = try validateGeneratedRangeAfterKeywordPair(tokens, cursor, range, .order, .by);
    if (limit_tokens) |range| cursor = try validateGeneratedRangeAfterKeyword(tokens, cursor, range, .limit);
    if (offset_tokens) |range| cursor = try validateGeneratedRangeAfterKeyword(tokens, cursor, range, .offset);
    if (fetch_tokens) |range| cursor = try validateGeneratedRangeAfterKeyword(tokens, cursor, range, .fetch);
    if (cursor != owner_end) {
        if (cursor >= tokens.len or cursor + 1 != owner_end or tokens[cursor].kind != .semicolon) return error.UnsupportedSqlShape;
    }
}

fn validateGeneratedReadPaginationPayloads(
    tokens: []const Token,
    owner: generated_parser.GeneratedSqlTokenRange,
    limit_tokens: ?generated_parser.GeneratedSqlTokenRange,
    limit_expression: generated_parser.GeneratedSqlExpressionAst,
    limit_all: bool,
    offset_tokens: ?generated_parser.GeneratedSqlTokenRange,
    offset_expression: generated_parser.GeneratedSqlExpressionAst,
    fetch_tokens: ?generated_parser.GeneratedSqlTokenRange,
    fetch_count_tokens: ?generated_parser.GeneratedSqlTokenRange,
    fetch_count_expression: generated_parser.GeneratedSqlExpressionAst,
) !void {
    if (limit_tokens) |range| {
        try validateGeneratedRangeInside(range, owner);
        try validateGeneratedOptionalRangePrecededByKeyword(tokens, range, .limit);
        if (limit_all) {
            try validateGeneratedLimitAllRangeLayout(tokens, range);
            try validateGeneratedOptionalExpression(tokens, null, limit_expression);
        } else {
            if (!expr_generated.generatedTokenRangeEqual(limit_expression.tokens orelse return error.UnsupportedSqlShape, range)) return error.UnsupportedSqlShape;
            try validateGeneratedExpressionPayloads(tokens, limit_expression);
        }
    } else {
        if (limit_all) return error.UnsupportedSqlShape;
        try validateGeneratedOptionalExpression(tokens, null, limit_expression);
    }

    if (offset_tokens) |range| {
        try validateGeneratedRangeInside(range, owner);
        try validateGeneratedOptionalRangePrecededByKeyword(tokens, range, .offset);
        const expression_tokens = offset_expression.tokens orelse return error.UnsupportedSqlShape;
        if (expression_tokens.start != range.start or expression_tokens.end > range.end) return error.UnsupportedSqlShape;
        if (expression_tokens.end != range.end) {
            if (expression_tokens.end + 1 != range.end) return error.UnsupportedSqlShape;
            if (!tokens[expression_tokens.end].matchesKeywordTag(.row) and !tokens[expression_tokens.end].matchesKeywordTag(.rows)) return error.UnsupportedSqlShape;
        }
        try validateGeneratedExpressionPayloads(tokens, offset_expression);
    } else {
        try validateGeneratedOptionalExpression(tokens, null, offset_expression);
    }

    if (fetch_tokens) |range| {
        try validateGeneratedRangeInside(range, owner);
        try validateGeneratedOptionalRangePrecededByKeyword(tokens, range, .fetch);
        try validateGeneratedFetchRangeLayout(tokens, range, fetch_count_tokens);
        if (fetch_count_tokens) |count_tokens| {
            try validateGeneratedRangeInside(count_tokens, range);
            if (!expr_generated.generatedTokenRangeEqual(fetch_count_expression.tokens orelse return error.UnsupportedSqlShape, count_tokens)) return error.UnsupportedSqlShape;
            try validateGeneratedExpressionPayloads(tokens, fetch_count_expression);
        } else {
            try validateGeneratedOptionalExpression(tokens, null, fetch_count_expression);
        }
    } else {
        if (fetch_count_tokens != null) return error.UnsupportedSqlShape;
        try validateGeneratedOptionalExpression(tokens, null, fetch_count_expression);
    }
}

pub fn validateGeneratedLimitAllRangeLayout(
    tokens: []const Token,
    range: generated_parser.GeneratedSqlTokenRange,
) !void {
    if (range.end != range.start + 1 or range.end > tokens.len or !tokens[range.start].matchesKeywordTag(.all)) {
        return error.UnsupportedSqlShape;
    }
}

pub fn validateGeneratedFetchRangeLayout(
    tokens: []const Token,
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

pub fn validateGeneratedRowLockClauseLayout(
    tokens: []const Token,
    range: ?generated_parser.GeneratedSqlTokenRange,
    mode: ?generated_parser.GeneratedSqlRowLockMode,
    wait_policy: ?generated_parser.GeneratedSqlRowLockWaitPolicy,
) !void {
    const lock_tokens = range orelse {
        if (mode != null or wait_policy != null) return error.UnsupportedSqlShape;
        return;
    };
    const generated_mode = mode orelse return error.UnsupportedSqlShape;
    const generated_wait_policy = wait_policy orelse return error.UnsupportedSqlShape;
    if (lock_tokens.start >= lock_tokens.end or lock_tokens.end > tokens.len) return error.UnsupportedSqlShape;
    if (!tokens[lock_tokens.start].matchesKeywordTag(.@"for")) return error.UnsupportedSqlShape;

    var cursor = lock_tokens.start + 1;
    if (cursor >= lock_tokens.end) return error.UnsupportedSqlShape;
    const parsed_mode: generated_parser.GeneratedSqlRowLockMode = if (tokens[cursor].matchesKeywordTag(.update)) blk: {
        cursor += 1;
        break :blk .update;
    } else if (tokens[cursor].matchesKeywordTag(.share)) blk: {
        cursor += 1;
        break :blk .share;
    } else if (cursor + 2 < lock_tokens.end and
        tokens[cursor].matchesKeywordTag(.no) and
        tokens[cursor + 1].matchesKeywordTag(.key) and
        tokens[cursor + 2].matchesKeywordTag(.update))
    blk: {
        cursor += 3;
        break :blk .no_key_update;
    } else if (cursor + 1 < lock_tokens.end and
        tokens[cursor].matchesKeywordTag(.key) and
        tokens[cursor + 1].matchesKeywordTag(.share))
    blk: {
        cursor += 2;
        break :blk .key_share;
    } else return error.UnsupportedSqlShape;
    if (parsed_mode != generated_mode) return error.UnsupportedSqlShape;

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

    var parsed_wait_policy = generated_parser.GeneratedSqlRowLockWaitPolicy.wait;
    if (cursor < lock_tokens.end) {
        if (tokens[cursor].matchesKeywordTag(.nowait)) {
            parsed_wait_policy = .nowait;
            cursor += 1;
        } else if (cursor + 1 < lock_tokens.end and
            tokens[cursor].matchesKeywordTag(.skip) and
            tokens[cursor + 1].matchesKeywordTag(.locked))
        {
            parsed_wait_policy = .skip_locked;
            cursor += 2;
        } else {
            return error.UnsupportedSqlShape;
        }
    }

    if (cursor != lock_tokens.end) return error.UnsupportedSqlShape;
    if (parsed_wait_policy != generated_wait_policy) return error.UnsupportedSqlShape;
}

fn generatedGraphTableFunctionKindForAntfly(
    kind: generated_parser.GeneratedSqlAntflyTableFunctionKind,
) ?generated_parser.GeneratedSqlGraphTableFunctionKind {
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

fn validateGeneratedNamedArgumentAst(
    tokens: []const Token,
    argument_tokens: generated_parser.GeneratedSqlTokenRange,
    argument: generated_parser.GeneratedSqlNamedArgumentAst,
) !void {
    if (argument.tokens.start < argument_tokens.start or argument.tokens.end > argument_tokens.end or argument.tokens.start >= argument.tokens.end) return error.UnsupportedSqlShape;
    if (argument.name_tokens.start != argument.tokens.start or argument.name_tokens.end != argument.operator_tokens.start) return error.UnsupportedSqlShape;
    if (argument.operator_tokens.end != argument.value_tokens.start or argument.value_tokens.end != argument.tokens.end) return error.UnsupportedSqlShape;
    if (argument.name_tokens.end != argument.name_tokens.start + 1 or argument.name_tokens.end > tokens.len) return error.UnsupportedSqlShape;
    if (argument.operator_tokens.start >= argument.operator_tokens.end or argument.operator_tokens.end > tokens.len) return error.UnsupportedSqlShape;
    if (tokens[argument.operator_tokens.start].kind != .eq) return error.UnsupportedSqlShape;
    if (argument.operator_tokens.end != argument.operator_tokens.start + 1 and
        (argument.operator_tokens.end != argument.operator_tokens.start + 2 or tokens[argument.operator_tokens.start + 1].kind != .gt))
    {
        return error.UnsupportedSqlShape;
    }
    if (argument.value_tokens.start >= argument.value_tokens.end or argument.value_tokens.end > tokens.len) return error.UnsupportedSqlShape;
}

fn validateGeneratedAntflyTableFunctionAst(
    tokens: []const Token,
    source_tokens: generated_parser.GeneratedSqlTokenRange,
    item: generated_parser.GeneratedSqlAntflyTableFunctionAst,
) !void {
    if (item.tokens.start < source_tokens.start or item.tokens.end > source_tokens.end or item.tokens.start >= item.tokens.end) return error.UnsupportedSqlShape;
    if (item.name_tokens.start != item.tokens.start or item.name_tokens.end != item.name_tokens.start + 1 or item.name_tokens.end >= item.tokens.end) return error.UnsupportedSqlShape;
    if (item.argument_tokens.start != item.name_tokens.end + 1 or item.argument_tokens.end + 1 != item.tokens.end) return error.UnsupportedSqlShape;
    if (item.name_tokens.end >= tokens.len or tokens[item.name_tokens.end].kind != .lparen or tokens[item.tokens.end - 1].kind != .rparen) return error.UnsupportedSqlShape;
    if (item.argument_count != item.argument_items.len) return error.UnsupportedSqlShape;
    if (item.argument_items.len == 0) {
        if (item.argument_tokens.start != item.argument_tokens.end) return error.UnsupportedSqlShape;
        return;
    }
    var expected_start = item.argument_tokens.start;
    for (item.argument_items, 0..) |argument, index| {
        if (argument.tokens.start != expected_start) return error.UnsupportedSqlShape;
        try validateGeneratedNamedArgumentAst(tokens, item.argument_tokens, argument);
        if (index + 1 < item.argument_items.len) {
            if (argument.tokens.end >= item.argument_tokens.end or tokens[argument.tokens.end].kind != .comma) return error.UnsupportedSqlShape;
            expected_start = argument.tokens.end + 1;
        } else {
            expected_start = argument.tokens.end;
        }
    }
    if (expected_start != item.argument_tokens.end) return error.UnsupportedSqlShape;
}

fn generatedGraphItemMatchesAntflyItem(
    graph_item: generated_parser.GeneratedSqlGraphTableFunctionAst,
    antfly_item: generated_parser.GeneratedSqlAntflyTableFunctionAst,
) bool {
    const expected = generatedGraphTableFunctionKindForAntfly(antfly_item.kind) orelse return false;
    return graph_item.kind == expected and
        expr_generated.generatedTokenRangeEqual(graph_item.tokens, antfly_item.tokens) and
        expr_generated.generatedTokenRangeEqual(graph_item.name_tokens, antfly_item.name_tokens) and
        expr_generated.generatedTokenRangeEqual(graph_item.argument_tokens, antfly_item.argument_tokens) and
        graph_item.argument_count == antfly_item.argument_count;
}

fn validateGeneratedGraphValueRange(
    argument_tokens: generated_parser.GeneratedSqlTokenRange,
    maybe_range: ?generated_parser.GeneratedSqlTokenRange,
) !void {
    const range = maybe_range orelse return;
    if (range.start < argument_tokens.start or range.end > argument_tokens.end or range.start >= range.end) return error.UnsupportedSqlShape;
}

fn validateGeneratedGraphTableFunctionAst(
    source_tokens: generated_parser.GeneratedSqlTokenRange,
    antfly_items: []const generated_parser.GeneratedSqlAntflyTableFunctionAst,
    graph_item: generated_parser.GeneratedSqlGraphTableFunctionAst,
) !void {
    if (graph_item.tokens.start < source_tokens.start or graph_item.tokens.end > source_tokens.end or graph_item.tokens.start >= graph_item.tokens.end) return error.UnsupportedSqlShape;
    var matched = false;
    for (antfly_items) |antfly_item| {
        if (generatedGraphItemMatchesAntflyItem(graph_item, antfly_item)) {
            matched = true;
            break;
        }
    }
    if (!matched) return error.UnsupportedSqlShape;
    try validateGeneratedGraphValueRange(graph_item.argument_tokens, graph_item.table_name_value_tokens);
    try validateGeneratedGraphValueRange(graph_item.argument_tokens, graph_item.index_value_tokens);
    try validateGeneratedGraphValueRange(graph_item.argument_tokens, graph_item.start_value_tokens);
    try validateGeneratedGraphValueRange(graph_item.argument_tokens, graph_item.start_result_ref_value_tokens);
    try validateGeneratedGraphValueRange(graph_item.argument_tokens, graph_item.target_value_tokens);
    try validateGeneratedGraphValueRange(graph_item.argument_tokens, graph_item.target_result_ref_value_tokens);
    try validateGeneratedGraphValueRange(graph_item.argument_tokens, graph_item.pattern_value_tokens);
    try validateGeneratedGraphValueRange(graph_item.argument_tokens, graph_item.return_value_tokens);
    try validateGeneratedGraphValueRange(graph_item.argument_tokens, graph_item.metric_value_tokens);
    try validateGeneratedGraphValueRange(graph_item.argument_tokens, graph_item.query_value_tokens);
}

fn validateGeneratedSourceTableFunctionPayloads(
    tokens: []const Token,
    source_tokens: generated_parser.GeneratedSqlTokenRange,
    read: generated_parser.GeneratedSqlReadAst,
) !void {
    try validateGeneratedSourceTableFunctionItemsPayloads(
        tokens,
        source_tokens,
        read.source_antfly_function_items,
        read.source_antfly_function_count,
        read.source_graph_function_items,
        read.source_graph_function_count,
    );
    if (read.source_graph_function_items.len == 0) {
        if (read.source_graph_function_tokens != null or
            read.source_graph_function_name_tokens != null or
            read.source_graph_function_argument_tokens != null or
            read.source_graph_function_kind != null)
        {
            return error.UnsupportedSqlShape;
        }
        return;
    }
    const first = read.source_graph_function_items[0];
    if (!expr_generated.generatedTokenRangeEqual(read.source_graph_function_tokens orelse return error.UnsupportedSqlShape, first.tokens) or
        !expr_generated.generatedTokenRangeEqual(read.source_graph_function_name_tokens orelse return error.UnsupportedSqlShape, first.name_tokens) or
        !expr_generated.generatedTokenRangeEqual(read.source_graph_function_argument_tokens orelse return error.UnsupportedSqlShape, first.argument_tokens) or
        read.source_graph_function_kind == null or read.source_graph_function_kind.? != first.kind)
    {
        return error.UnsupportedSqlShape;
    }
}

fn validateGeneratedSourceTableFunctionItemsPayloads(
    tokens: []const Token,
    source_tokens: generated_parser.GeneratedSqlTokenRange,
    antfly_items: []const generated_parser.GeneratedSqlAntflyTableFunctionAst,
    antfly_count: usize,
    graph_items: []const generated_parser.GeneratedSqlGraphTableFunctionAst,
    graph_count: usize,
) !void {
    if (antfly_count != antfly_items.len) return error.UnsupportedSqlShape;
    if (graph_count != graph_items.len) return error.UnsupportedSqlShape;
    for (antfly_items) |item| try validateGeneratedAntflyTableFunctionAst(tokens, source_tokens, item);
    for (graph_items) |item| try validateGeneratedGraphTableFunctionAst(source_tokens, antfly_items, item);
}

fn validateGeneratedReadSystemTimePayloads(
    tokens: []const Token,
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
    if (source.start >= source.end or source.end > tokens.len) return error.UnsupportedSqlShape;
    if (system_time.start < source.start or system_time.end != source.end or system_time.end > tokens.len) return error.UnsupportedSqlShape;
    if (system_time.end < system_time.start + 5) return error.UnsupportedSqlShape;
    if (!tokens[system_time.start].matchesKeywordTag(.@"for")) return error.UnsupportedSqlShape;
    if (!std.ascii.eqlIgnoreCase(tokens[system_time.start + 1].text, "system_time")) return error.UnsupportedSqlShape;
    if (!tokens[system_time.start + 2].matchesKeywordTag(.as)) return error.UnsupportedSqlShape;
    if (!tokens[system_time.start + 3].matchesKeywordTag(.of)) return error.UnsupportedSqlShape;
    if (sequence.start != system_time.start + 4 or sequence.end != system_time.end) return error.UnsupportedSqlShape;
    try validateGeneratedSystemTimePayloadTokenRange(tokens, sequence);
}

fn validateGeneratedSystemTimePayloadTokenRange(
    tokens: []const Token,
    range: generated_parser.GeneratedSqlTokenRange,
) !void {
    if (range.start >= range.end or range.end > tokens.len) return error.UnsupportedSqlShape;
    if (range.end == range.start + 1) {
        const token = tokens[range.start];
        if (token.kind == .number) {
            if (std.mem.indexOfAny(u8, token.text, ".eE+-") != null) return error.UnsupportedSqlShape;
            return;
        }
        if (token.kind == .string) return;
        return error.UnsupportedSqlShape;
    }
    if (range.end == range.start + 2 and
        (tokens[range.start].matchesKeywordTag(.date) or
            tokens[range.start].matchesKeywordTag(.timestamp) or
            tokens[range.start].matchesKeywordTag(.timestamptz)) and
        tokens[range.start + 1].kind == .string) return;
    return error.UnsupportedSqlShape;
}

fn validateGeneratedCteBodyJoinPayloads(
    tokens: []const Token,
    cte: generated_parser.GeneratedSqlCteAst,
) !void {
    if (cte.body_join_tokens) |range| {
        if (range.end > tokens.len) return error.UnsupportedSqlShape;
        const body = cte.body_tokens orelse return error.UnsupportedSqlShape;
        try validateGeneratedRangeInside(range, body);
        const body_read = generated_parser.GeneratedSqlReadAst{
            .kind = cte.body_kind orelse return error.UnsupportedSqlShape,
            .statement_span = tokens[body.start].sourceSpan(),
            .command_span = tokens[body.start].sourceSpan(),
            .join_tokens = cte.body_join_tokens,
            .join_operator_tokens = cte.body_join_operator_tokens,
            .join_kind = cte.body_join_kind,
            .join_left_tokens = cte.body_join_left_tokens,
            .join_right_tokens = cte.body_join_right_tokens,
            .join_predicate_tokens = cte.body_join_predicate_tokens,
            .join_predicate_expression = cte.body_join_predicate_expression,
            .join_items = cte.body_join_items,
            .join_tree_root_index = cte.body_join_tree_root_index,
            .join_tree_depth = cte.body_join_tree_depth,
        };
        try validateGeneratedJoinItemsMetadata(tokens, body_read);
    } else if (cte.body_join_items.len != 0 or
        cte.body_join_operator_tokens != null or
        cte.body_join_kind != null or
        cte.body_join_left_tokens != null or
        cte.body_join_right_tokens != null or
        cte.body_join_predicate_tokens != null or
        cte.body_join_predicate_expression.tokens != null or
        cte.body_join_tree_root_index != null or
        cte.body_join_tree_depth != 0)
    {
        return error.UnsupportedSqlShape;
    }
}

fn validateGeneratedCteBodyReadPayloads(
    tokens: []const Token,
    cte: generated_parser.GeneratedSqlCteAst,
) !void {
    const body = cte.body_tokens orelse return error.UnsupportedSqlShape;
    if (body.start >= body.end or body.end > tokens.len) return error.UnsupportedSqlShape;

    if (cte.body_distinct_tokens) |range| {
        try validateGeneratedRangeInside(range, body);
        _ = try validateGeneratedDistinctClause(tokens, range, cte.body_distinct_on_items);
    } else {
        try validateGeneratedEmptyList(cte.body_distinct_on_items);
    }

    if (cte.body_projection_tokens) |range| {
        try validateGeneratedRangeInside(range, body);
        try validateGeneratedProjectionStartFromTopLevelSelect(tokens, body, range);
        try validateGeneratedProjectionListForClause(tokens, range, cte.body_projection_items);
        try validateGeneratedReadListBoundaryExpressions(tokens, cte.body_projection_items, cte.body_projection_first_expression, cte.body_projection_last_expression);
    } else {
        return error.UnsupportedSqlShape;
    }

    if (cte.body_source_tokens) |range| {
        try validateGeneratedRangeInside(range, body);
        try validateGeneratedOptionalRangePrecededByKeyword(tokens, range, .from);
        try validateGeneratedSourceTableFunctionItemsPayloads(
            tokens,
            range,
            cte.body_source_antfly_function_items,
            cte.body_source_antfly_function_count,
            cte.body_source_graph_function_items,
            cte.body_source_graph_function_count,
        );
    } else if (cte.body_source_antfly_function_count != 0 or
        cte.body_source_antfly_function_items.len != 0 or
        cte.body_source_graph_function_count != 0 or
        cte.body_source_graph_function_items.len != 0 or
        cte.body_source_system_time_tokens != null or
        cte.body_source_system_time_sequence_tokens != null)
    {
        return error.UnsupportedSqlShape;
    }
    try validateGeneratedReadSystemTimePayloads(tokens, cte.body_source_tokens, cte.body_source_system_time_tokens, cte.body_source_system_time_sequence_tokens);

    try validateGeneratedCteBodyJoinPayloads(tokens, cte);

    try validateGeneratedOptionalRangeInside(cte.body_where_tokens, body);
    try validateGeneratedOptionalRangePrecededByKeyword(tokens, cte.body_where_tokens, .where);
    try validateGeneratedOptionalReadExpression(tokens, cte.body_where_tokens, cte.body_where_expression);

    if (cte.body_group_tokens) |range| {
        try validateGeneratedRangeInside(range, body);
        try validateGeneratedOptionalRangePrecededByKeywordPair(tokens, range, .group, .by);
        try validateGeneratedExpressionListForClause(tokens, range, cte.body_group_items);
        try validateGeneratedReadListBoundaryExpressions(tokens, cte.body_group_items, cte.body_group_first_expression, cte.body_group_last_expression);
    } else {
        try validateGeneratedEmptyList(cte.body_group_items);
        if (cte.body_group_first_expression.tokens != null or cte.body_group_last_expression.tokens != null) return error.UnsupportedSqlShape;
    }

    try validateGeneratedOptionalRangeInside(cte.body_having_tokens, body);
    try validateGeneratedOptionalRangePrecededByKeyword(tokens, cte.body_having_tokens, .having);
    try validateGeneratedOptionalReadExpression(tokens, cte.body_having_tokens, cte.body_having_expression);

    if (cte.body_window_tokens) |range| {
        try validateGeneratedRangeInside(range, body);
        try validateGeneratedOptionalRangePrecededByKeyword(tokens, range, .window);
        if (cte.body_window_items.len != cte.body_window_count) return error.UnsupportedSqlShape;
        try validateGeneratedWindowListForClause(tokens, range, cte.body_window_items);
    } else if (cte.body_window_count != 0 or cte.body_window_items.len != 0) {
        return error.UnsupportedSqlShape;
    }

    if (cte.body_order_tokens) |range| {
        try validateGeneratedRangeInside(range, body);
        try validateGeneratedOptionalRangePrecededByKeywordPair(tokens, range, .order, .by);
        try validateGeneratedOrderListForClause(tokens, range, cte.body_order_items);
        try validateGeneratedReadListBoundaryExpressions(tokens, cte.body_order_items, cte.body_order_first_expression, cte.body_order_last_expression);
    } else {
        try validateGeneratedEmptyList(cte.body_order_items);
        if (cte.body_order_first_expression.tokens != null or cte.body_order_last_expression.tokens != null) return error.UnsupportedSqlShape;
    }

    try validateGeneratedReadPaginationPayloads(
        tokens,
        body,
        cte.body_limit_tokens,
        cte.body_limit_expression,
        cte.body_limit_all,
        cte.body_offset_tokens,
        cte.body_offset_expression,
        cte.body_fetch_tokens,
        cte.body_fetch_count_tokens,
        cte.body_fetch_count_expression,
    );

    try validateGeneratedOptionalRangeInside(cte.body_row_lock_tokens, body);
    try validateGeneratedRowLockClauseLayout(tokens, cte.body_row_lock_tokens, cte.body_row_lock_mode, cte.body_row_lock_wait_policy);

    if (cte.body_set_operation_tokens) |range| {
        try validateGeneratedRangeInside(range, body);
        try validateGeneratedSetOperationPayloads(tokens, range, cte.body_set_operation);
        try validateGeneratedReadResultTailCursor(
            tokens,
            range.end,
            body.end,
            cte.body_order_tokens,
            cte.body_limit_tokens,
            cte.body_offset_tokens,
            cte.body_fetch_tokens,
        );
    } else if (cte.body_set_operation.tokens != null) {
        return error.UnsupportedSqlShape;
    } else {
        const projection = cte.body_projection_tokens orelse return error.UnsupportedSqlShape;
        try validateGeneratedNonSetReadClauseCursor(
            tokens,
            projection,
            body.end,
            cte.body_source_tokens,
            cte.body_where_tokens,
            cte.body_group_tokens,
            cte.body_having_tokens,
            cte.body_window_tokens,
            cte.body_order_tokens,
            cte.body_limit_tokens,
            cte.body_offset_tokens,
            cte.body_fetch_tokens,
            cte.body_row_lock_tokens,
        );
    }
}

fn validateGeneratedCteItemPayloads(
    tokens: []const Token,
    list_tokens: generated_parser.GeneratedSqlTokenRange,
    cte: generated_parser.GeneratedSqlCteAst,
) !void {
    if (cte.name_tokens.start < list_tokens.start or cte.name_tokens.end > list_tokens.end or cte.name_tokens.end != cte.name_tokens.start + 1) return error.UnsupportedSqlShape;
    if (cte.name_tokens.end > tokens.len or tokens[cte.name_tokens.start].kind != .identifier) return error.UnsupportedSqlShape;

    var cursor = cte.name_tokens.end;
    if (cte.column_tokens) |column_tokens| {
        if (column_tokens.start != cursor or column_tokens.end > list_tokens.end or column_tokens.end <= column_tokens.start + 1) return error.UnsupportedSqlShape;
        if (tokens[column_tokens.start].kind != .lparen or tokens[column_tokens.end - 1].kind != .rparen) return error.UnsupportedSqlShape;
        const column_name_tokens = cte.column_name_tokens orelse return error.UnsupportedSqlShape;
        if (column_name_tokens.start != column_tokens.start + 1 or column_name_tokens.end != column_tokens.end - 1) return error.UnsupportedSqlShape;
        try validateGeneratedExpressionListForClause(tokens, column_name_tokens, cte.column_names);
        cursor = column_tokens.end;
    } else if (cte.column_name_tokens != null or cte.column_names.count != 0 or cte.column_names.items.len != 0) {
        return error.UnsupportedSqlShape;
    }

    if (cursor >= list_tokens.end or !tokens[cursor].matchesKeywordTag(.as)) return error.UnsupportedSqlShape;
    cursor += 1;

    if (cte.materialization_tokens) |materialization_tokens| {
        if (materialization_tokens.start != cursor or materialization_tokens.end > list_tokens.end or materialization_tokens.start >= materialization_tokens.end) return error.UnsupportedSqlShape;
        const materialization = cte.materialization orelse return error.UnsupportedSqlShape;
        switch (materialization) {
            .materialized => {
                if (materialization_tokens.end != materialization_tokens.start + 1 or !tokens[materialization_tokens.start].matchesKeywordTag(.materialized)) return error.UnsupportedSqlShape;
            },
            .not_materialized => {
                if (materialization_tokens.end != materialization_tokens.start + 2 or
                    !tokens[materialization_tokens.start].matchesKeywordTag(.not) or
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

    const body_tokens = cte.body_tokens orelse return error.UnsupportedSqlShape;
    if (cursor >= list_tokens.end or tokens[cursor].kind != .lparen) return error.UnsupportedSqlShape;
    if (body_tokens.start != cursor + 1 or body_tokens.end >= list_tokens.end or tokens[body_tokens.end].kind != .rparen) return error.UnsupportedSqlShape;
    if (body_tokens.start >= body_tokens.end or body_tokens.end > tokens.len) return error.UnsupportedSqlShape;
    if (cte.body_kind == null or cte.body_select_tokens == null) return error.UnsupportedSqlShape;
    const select_tokens = cte.body_select_tokens.?;
    if (select_tokens.start != body_tokens.start or select_tokens.end != select_tokens.start + 1 or !tokens[select_tokens.start].matchesKeywordTag(.select)) return error.UnsupportedSqlShape;
    try validateGeneratedCteBodyReadPayloads(tokens, cte);
}

fn validateGeneratedCteReadPayloads(
    tokens: []const Token,
    read: generated_parser.GeneratedSqlReadAst,
) !void {
    if (read.kind != .cte) {
        if (read.cte_tokens != null or
            read.cte_list_tokens != null or
            read.cte_name_tokens != null or
            read.cte_body_tokens != null or
            read.cte_last_name_tokens != null or
            read.cte_last_body_tokens != null or
            read.cte_items.len != 0 or
            read.cte_count != 0 or
            read.cte_recursive)
        {
            return error.UnsupportedSqlShape;
        }
        return;
    }

    const cte_tokens = read.cte_tokens orelse return error.UnsupportedSqlShape;
    const list_tokens = read.cte_list_tokens orelse return error.UnsupportedSqlShape;
    if (cte_tokens.start >= cte_tokens.end or cte_tokens.end > tokens.len) return error.UnsupportedSqlShape;
    if (list_tokens.start < cte_tokens.start or list_tokens.end > cte_tokens.end or list_tokens.start >= list_tokens.end) return error.UnsupportedSqlShape;
    if (read.cte_recursive) {
        if (cte_tokens.start >= list_tokens.start or !tokens[cte_tokens.start].matchesKeywordTag(.recursive) or list_tokens.start != cte_tokens.start + 1) return error.UnsupportedSqlShape;
    } else if (list_tokens.start != cte_tokens.start) {
        return error.UnsupportedSqlShape;
    }

    if (read.cte_count == 0 or read.cte_items.len != read.cte_count) return error.UnsupportedSqlShape;
    const first = read.cte_items[0];
    const last = read.cte_items[read.cte_items.len - 1];
    if (!expr_generated.generatedTokenRangeEqual(read.cte_name_tokens orelse return error.UnsupportedSqlShape, first.name_tokens)) return error.UnsupportedSqlShape;
    if (!expr_generated.generatedTokenRangeEqual(read.cte_body_tokens orelse return error.UnsupportedSqlShape, first.body_tokens orelse return error.UnsupportedSqlShape)) return error.UnsupportedSqlShape;
    if (!expr_generated.generatedTokenRangeEqual(read.cte_last_name_tokens orelse return error.UnsupportedSqlShape, last.name_tokens)) return error.UnsupportedSqlShape;
    if (!expr_generated.generatedTokenRangeEqual(read.cte_last_body_tokens orelse return error.UnsupportedSqlShape, last.body_tokens orelse return error.UnsupportedSqlShape)) return error.UnsupportedSqlShape;

    for (read.cte_items, 0..) |cte, index| {
        try validateGeneratedCteItemPayloads(tokens, list_tokens, cte);
        if (index == 0) {
            if (cte.name_tokens.start != list_tokens.start) return error.UnsupportedSqlShape;
        } else {
            const previous_body = read.cte_items[index - 1].body_tokens orelse return error.UnsupportedSqlShape;
            const comma_index = previous_body.end + 1;
            if (comma_index >= tokens.len or tokens[comma_index].kind != .comma or cte.name_tokens.start != comma_index + 1) return error.UnsupportedSqlShape;
        }
        if (index + 1 == read.cte_items.len) {
            const body = cte.body_tokens orelse return error.UnsupportedSqlShape;
            if (body.end + 1 != list_tokens.end) return error.UnsupportedSqlShape;
        }
    }
}

pub fn validateGeneratedReadAstPayloads(
    tokens: []const Token,
    read: generated_parser.GeneratedSqlReadAst,
) !void {
    try validateGeneratedCteReadPayloads(tokens, read);

    if (read.distinct_tokens) |range| {
        _ = try validateGeneratedDistinctClause(tokens, range, read.distinct_on_items);
    } else {
        try validateGeneratedEmptyList(read.distinct_on_items);
    }

    if (read.projection_tokens) |range| {
        try validateGeneratedProjectionStartFromTopLevelSelect(tokens, .{ .start = 0, .end = tokens.len }, range);
        try validateGeneratedProjectionListForClause(tokens, range, read.projection_items);
        try validateGeneratedReadListBoundaryExpressions(tokens, read.projection_items, read.projection_first_expression, read.projection_last_expression);
    } else {
        try validateGeneratedEmptyList(read.projection_items);
        if (read.projection_first_expression.tokens != null or read.projection_last_expression.tokens != null) return error.UnsupportedSqlShape;
    }

    if (read.source_tokens) |range| {
        if (range.start >= range.end or range.end > tokens.len) return error.UnsupportedSqlShape;
        try validateGeneratedOptionalRangePrecededByKeyword(tokens, range, .from);
        try validateGeneratedSourceTableFunctionPayloads(tokens, range, read);
    } else if (read.source_antfly_function_count != 0 or
        read.source_antfly_function_items.len != 0 or
        read.source_graph_function_count != 0 or
        read.source_graph_function_items.len != 0 or
        read.source_system_time_tokens != null or
        read.source_system_time_sequence_tokens != null)
    {
        return error.UnsupportedSqlShape;
    }
    try validateGeneratedReadSystemTimePayloads(tokens, read.source_tokens, read.source_system_time_tokens, read.source_system_time_sequence_tokens);

    if (read.join_tokens) |range| {
        if (range.start >= range.end or range.end > tokens.len) return error.UnsupportedSqlShape;
        try validateGeneratedJoinExecutableContract(&read, tokens, read.kind);
    } else if (read.join_items.len != 0 or read.join_predicate_tokens != null or read.join_predicate_expression.tokens != null) {
        return error.UnsupportedSqlShape;
    }

    try validateGeneratedOptionalRangePrecededByKeyword(tokens, read.where_tokens, .where);
    try validateGeneratedOptionalReadExpression(tokens, read.where_tokens, read.where_expression);

    if (read.group_tokens) |range| {
        try validateGeneratedOptionalRangePrecededByKeywordPair(tokens, range, .group, .by);
        try validateGeneratedExpressionListForClause(tokens, range, read.group_items);
        try validateGeneratedReadListBoundaryExpressions(tokens, read.group_items, read.group_first_expression, read.group_last_expression);
    } else {
        try validateGeneratedEmptyList(read.group_items);
        if (read.group_first_expression.tokens != null or read.group_last_expression.tokens != null) return error.UnsupportedSqlShape;
    }

    try validateGeneratedOptionalRangePrecededByKeyword(tokens, read.having_tokens, .having);
    try validateGeneratedOptionalReadExpression(tokens, read.having_tokens, read.having_expression);

    if (read.window_tokens) |range| {
        try validateGeneratedOptionalRangePrecededByKeyword(tokens, range, .window);
        if (read.window_items.len != read.window_count) return error.UnsupportedSqlShape;
        try validateGeneratedWindowListForClause(tokens, range, read.window_items);
    } else if (read.window_count != 0 or read.window_items.len != 0) {
        return error.UnsupportedSqlShape;
    }

    if (read.order_tokens) |range| {
        try validateGeneratedOptionalRangePrecededByKeywordPair(tokens, range, .order, .by);
        try validateGeneratedOrderListForClause(tokens, range, read.order_items);
        try validateGeneratedReadListBoundaryExpressions(tokens, read.order_items, read.order_first_expression, read.order_last_expression);
    } else {
        try validateGeneratedEmptyList(read.order_items);
        if (read.order_first_expression.tokens != null or read.order_last_expression.tokens != null) return error.UnsupportedSqlShape;
    }

    try validateGeneratedReadPaginationPayloads(
        tokens,
        .{ .start = 0, .end = tokens.len },
        read.limit_tokens,
        read.limit_expression,
        read.limit_all,
        read.offset_tokens,
        read.offset_expression,
        read.fetch_tokens,
        read.fetch_count_tokens,
        read.fetch_count_expression,
    );

    try validateGeneratedRowLockClauseLayout(tokens, read.row_lock_tokens, read.row_lock_mode, read.row_lock_wait_policy);

    if (read.set_operation_tokens) |range| {
        try validateGeneratedSetOperationPayloads(tokens, range, read.set_operation);
        try validateGeneratedReadResultTailCursor(
            tokens,
            range.end,
            tokens.len,
            read.order_tokens,
            read.limit_tokens,
            read.offset_tokens,
            read.fetch_tokens,
        );
    } else if (read.set_operation.tokens != null) {
        return error.UnsupportedSqlShape;
    } else if (read.projection_tokens) |projection| {
        try validateGeneratedNonSetReadClauseCursor(
            tokens,
            projection,
            tokens.len,
            read.source_tokens,
            read.where_tokens,
            read.group_tokens,
            read.having_tokens,
            read.window_tokens,
            read.order_tokens,
            read.limit_tokens,
            read.offset_tokens,
            read.fetch_tokens,
            read.row_lock_tokens,
        );
    }
}

fn validateGeneratedSubqueryTailPayloads(
    tokens: []const Token,
    inner_tokens: generated_parser.GeneratedSqlTokenRange,
    tail: generated_parser.GeneratedSqlSubqueryTailAst,
    previous_end_before_tail: usize,
) anyerror!void {
    var previous_end = previous_end_before_tail;
    var saw_tail = false;
    if (tail.order_tokens) |order_tokens| {
        saw_tail = true;
        if (order_tokens.start < inner_tokens.start or order_tokens.end > inner_tokens.end or order_tokens.start >= order_tokens.end) return error.UnsupportedSqlShape;
        if (previous_end + 2 != order_tokens.start or order_tokens.start < 2) return error.UnsupportedSqlShape;
        if (!tokens[previous_end].matchesKeywordTag(.order) or !tokens[previous_end + 1].matchesKeywordTag(.by)) return error.UnsupportedSqlShape;
        try validateGeneratedOrderListForClause(tokens, order_tokens, tail.order_items);
        const first_expression = tail.order_first_expression orelse return error.UnsupportedSqlShape;
        const last_expression = tail.order_last_expression orelse return error.UnsupportedSqlShape;
        if (!expr_generated.generatedTokenRangeEqual(first_expression.tokens orelse return error.UnsupportedSqlShape, tail.order_items.expression_items[0])) return error.UnsupportedSqlShape;
        if (!expr_generated.generatedTokenRangeEqual(last_expression.tokens orelse return error.UnsupportedSqlShape, tail.order_items.expression_items[tail.order_items.count - 1])) return error.UnsupportedSqlShape;
        try validateGeneratedExpressionPayloads(tokens, first_expression.*);
        try validateGeneratedExpressionPayloads(tokens, last_expression.*);
        previous_end = order_tokens.end;
    } else {
        try validateGeneratedEmptyList(tail.order_items);
        if (tail.order_first_expression != null or tail.order_last_expression != null) return error.UnsupportedSqlShape;
    }

    if (tail.limit_tokens) |limit_tokens| {
        saw_tail = true;
        if (limit_tokens.start < inner_tokens.start or limit_tokens.end > inner_tokens.end or limit_tokens.start >= limit_tokens.end) return error.UnsupportedSqlShape;
        if (previous_end + 1 != limit_tokens.start or !tokens[previous_end].matchesKeywordTag(.limit)) return error.UnsupportedSqlShape;
        if (tail.limit_all) {
            if (limit_tokens.end != limit_tokens.start + 1 or !tokens[limit_tokens.start].matchesKeywordTag(.all)) return error.UnsupportedSqlShape;
            if (tail.limit_expression != null) return error.UnsupportedSqlShape;
        } else {
            const expression = tail.limit_expression orelse return error.UnsupportedSqlShape;
            if (!expr_generated.generatedTokenRangeEqual(expression.tokens orelse return error.UnsupportedSqlShape, limit_tokens)) return error.UnsupportedSqlShape;
            try validateGeneratedExpressionPayloads(tokens, expression.*);
        }
        previous_end = limit_tokens.end;
    } else if (tail.limit_expression != null or tail.limit_all) {
        return error.UnsupportedSqlShape;
    }

    if (tail.offset_tokens) |offset_tokens| {
        saw_tail = true;
        if (offset_tokens.start < inner_tokens.start or offset_tokens.end > inner_tokens.end or offset_tokens.start >= offset_tokens.end) return error.UnsupportedSqlShape;
        if (previous_end + 1 != offset_tokens.start or !tokens[previous_end].matchesKeywordTag(.offset)) return error.UnsupportedSqlShape;
        const expression = tail.offset_expression orelse return error.UnsupportedSqlShape;
        const expression_tokens = expression.tokens orelse return error.UnsupportedSqlShape;
        if (expression_tokens.start != offset_tokens.start or expression_tokens.end > offset_tokens.end) return error.UnsupportedSqlShape;
        if (expression_tokens.end != offset_tokens.end) {
            if (expression_tokens.end + 1 != offset_tokens.end) return error.UnsupportedSqlShape;
            if (!tokens[expression_tokens.end].matchesKeywordTag(.row) and !tokens[expression_tokens.end].matchesKeywordTag(.rows)) return error.UnsupportedSqlShape;
        }
        try validateGeneratedExpressionPayloads(tokens, expression.*);
        previous_end = offset_tokens.end;
    } else if (tail.offset_expression != null) {
        return error.UnsupportedSqlShape;
    }

    if (tail.fetch_tokens) |fetch_tokens| {
        saw_tail = true;
        if (fetch_tokens.start < inner_tokens.start or fetch_tokens.end > inner_tokens.end or fetch_tokens.start >= fetch_tokens.end) return error.UnsupportedSqlShape;
        if (previous_end + 1 != fetch_tokens.start or !tokens[previous_end].matchesKeywordTag(.fetch)) return error.UnsupportedSqlShape;
        try validateGeneratedFetchRangeLayout(tokens, fetch_tokens, tail.fetch_count_tokens);
        if (tail.fetch_count_tokens) |count_tokens| {
            if (count_tokens.start < fetch_tokens.start or count_tokens.end > fetch_tokens.end or count_tokens.start >= count_tokens.end) return error.UnsupportedSqlShape;
            const expression = tail.fetch_count_expression orelse return error.UnsupportedSqlShape;
            if (!expr_generated.generatedTokenRangeEqual(expression.tokens orelse return error.UnsupportedSqlShape, count_tokens)) return error.UnsupportedSqlShape;
            try validateGeneratedExpressionPayloads(tokens, expression.*);
        } else if (tail.fetch_count_expression != null) {
            return error.UnsupportedSqlShape;
        }
        previous_end = fetch_tokens.end;
    } else if (tail.fetch_count_tokens != null or tail.fetch_count_expression != null) {
        return error.UnsupportedSqlShape;
    }
    if (!saw_tail or previous_end != inner_tokens.end) return error.UnsupportedSqlShape;
}

fn generatedMatchingParenInRange(
    tokens: []const Token,
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

fn validateGeneratedProjectionStartAfterSelect(
    tokens: []const Token,
    owner_tokens: generated_parser.GeneratedSqlTokenRange,
    select_tokens: generated_parser.GeneratedSqlTokenRange,
    projection_tokens: generated_parser.GeneratedSqlTokenRange,
) !void {
    if (select_tokens.start < owner_tokens.start or select_tokens.end > owner_tokens.end or select_tokens.end != select_tokens.start + 1) return error.UnsupportedSqlShape;
    if (!tokens[select_tokens.start].matchesKeywordTag(.select)) return error.UnsupportedSqlShape;
    if (projection_tokens.start <= select_tokens.start or projection_tokens.end > owner_tokens.end) return error.UnsupportedSqlShape;
    if (projection_tokens.start == select_tokens.end) return;
    const modifier_start = select_tokens.end;
    if (modifier_start >= projection_tokens.start or projection_tokens.start > owner_tokens.end) return error.UnsupportedSqlShape;
    if (tokens[modifier_start].matchesKeywordTag(.all)) {
        if (projection_tokens.start != modifier_start + 1) return error.UnsupportedSqlShape;
        return;
    }
    if (!tokens[modifier_start].matchesKeywordTag(.distinct)) return error.UnsupportedSqlShape;
    if (projection_tokens.start == modifier_start + 1) return;
    if (modifier_start + 2 >= projection_tokens.start or
        !tokens[modifier_start + 1].matchesKeywordTag(.on) or
        tokens[modifier_start + 2].kind != .lparen)
    {
        return error.UnsupportedSqlShape;
    }
    const close = generatedMatchingParenInRange(tokens, modifier_start + 2, owner_tokens.end) orelse return error.UnsupportedSqlShape;
    if (projection_tokens.start != close + 1) return error.UnsupportedSqlShape;
}

fn validateGeneratedProjectionStartFromTopLevelSelect(
    tokens: []const Token,
    owner_tokens: generated_parser.GeneratedSqlTokenRange,
    projection_tokens: generated_parser.GeneratedSqlTokenRange,
) !void {
    const select_index = parser.findTopLevelKeywordTagFromIndex(tokens, owner_tokens.start, .select) orelse return error.UnsupportedSqlShape;
    if (select_index >= owner_tokens.end) return error.UnsupportedSqlShape;
    try validateGeneratedProjectionStartAfterSelect(
        tokens,
        owner_tokens,
        .{ .start = select_index, .end = select_index + 1 },
        projection_tokens,
    );
}

fn validateGeneratedSubqueryPayloads(
    tokens: []const Token,
    expression: generated_parser.GeneratedSqlExpressionAst,
) anyerror!void {
    const expression_tokens = expression.tokens orelse return error.UnsupportedSqlShape;
    const inner_tokens = expression.inner_tokens orelse return error.UnsupportedSqlShape;
    if (expression.kind != .subquery or expression_tokens.start >= expression_tokens.end or expression_tokens.end > tokens.len) return error.UnsupportedSqlShape;
    if (inner_tokens.start <= expression_tokens.start or inner_tokens.end >= expression_tokens.end or inner_tokens.start >= inner_tokens.end) return error.UnsupportedSqlShape;
    if (tokens[expression_tokens.start].kind != .lparen or tokens[expression_tokens.end - 1].kind != .rparen) return error.UnsupportedSqlShape;
    const retained_read_kind = expression.subquery_read_kind orelse return error.UnsupportedSqlShape;
    const select_tokens = expression.subquery_select_tokens orelse return error.UnsupportedSqlShape;
    if (select_tokens.start < inner_tokens.start or select_tokens.end > inner_tokens.end or select_tokens.end != select_tokens.start + 1) return error.UnsupportedSqlShape;
    if (!tokens[select_tokens.start].matchesKeywordTag(.select)) return error.UnsupportedSqlShape;
    const projection_tokens = expression.subquery_projection_tokens orelse return error.UnsupportedSqlShape;
    if (projection_tokens.start <= select_tokens.start or projection_tokens.end > inner_tokens.end) return error.UnsupportedSqlShape;
    try validateGeneratedProjectionStartAfterSelect(tokens, inner_tokens, select_tokens, projection_tokens);
    try validateGeneratedProjectionListForClause(tokens, projection_tokens, expression.subquery_projection_items);
    var previous_end = projection_tokens.end;
    if (expression.subquery_source_tokens) |source_tokens| {
        if (source_tokens.start < inner_tokens.start or source_tokens.end > inner_tokens.end or source_tokens.start >= source_tokens.end) return error.UnsupportedSqlShape;
        if (previous_end + 1 != source_tokens.start or !tokens[previous_end].matchesKeywordTag(.from)) return error.UnsupportedSqlShape;
        previous_end = source_tokens.end;
    }
    if (expression.subquery_where_tokens) |where_tokens| {
        if (where_tokens.start < inner_tokens.start or where_tokens.end > inner_tokens.end or where_tokens.start >= where_tokens.end) return error.UnsupportedSqlShape;
        if (previous_end + 1 != where_tokens.start or !tokens[previous_end].matchesKeywordTag(.where)) return error.UnsupportedSqlShape;
        try validateGeneratedChildExpressionPayloads(tokens, where_tokens, expression.subquery_where_expression_kind, expression.subquery_where_expression);
        previous_end = where_tokens.end;
    } else if (expression.subquery_where_expression != null or expression.subquery_where_expression_kind != null) {
        return error.UnsupportedSqlShape;
    }
    if (expression.subquery_set_operation_tokens) |set_operation_tokens| {
        if (retained_read_kind != .set_operation) return error.UnsupportedSqlShape;
        if (set_operation_tokens.start < inner_tokens.start or set_operation_tokens.end > inner_tokens.end) return error.UnsupportedSqlShape;
        if (set_operation_tokens.start != previous_end) return error.UnsupportedSqlShape;
        const set_operation = expression.subquery_set_operation orelse return error.UnsupportedSqlShape;
        try validateGeneratedSetOperationPayloads(tokens, set_operation_tokens, set_operation.*);
        previous_end = set_operation_tokens.end;
    } else if (expression.subquery_set_operation != null) {
        return error.UnsupportedSqlShape;
    } else if (retained_read_kind != .query) {
        return error.UnsupportedSqlShape;
    }
    if (expression.subquery_tail) |tail| {
        try validateGeneratedSubqueryTailPayloads(tokens, inner_tokens, tail.*, previous_end);
    } else if (previous_end != inner_tokens.end) {
        return error.UnsupportedSqlShape;
    }
}

fn validateGeneratedArrayConstructorPayloads(
    tokens: []const Token,
    expression: generated_parser.GeneratedSqlExpressionAst,
) anyerror!void {
    const expression_tokens = expression.tokens orelse return error.UnsupportedSqlShape;
    const array_tokens = expression.array_tokens orelse {
        if (expression_tokens.end != expression_tokens.start + 3 or
            !tokens[expression_tokens.start].matchesKeywordTag(.array) or
            tokens[expression_tokens.start + 1].kind != .lbracket or
            tokens[expression_tokens.end - 1].kind != .rbracket)
        {
            return error.UnsupportedSqlShape;
        }
        try validateGeneratedEmptyList(expression.array_items);
        return;
    };
    if (array_tokens.start != expression_tokens.start + 2 or array_tokens.end != expression_tokens.end - 1) return error.UnsupportedSqlShape;
    try validateGeneratedExpressionListForClause(tokens, array_tokens, expression.array_items);
    for (expression.array_items.expressions) |item_expression| {
        try validateGeneratedExpressionPayloads(tokens, item_expression);
    }
}

fn validateGeneratedFunctionCallPayloads(
    tokens: []const Token,
    expression: generated_parser.GeneratedSqlExpressionAst,
) anyerror!void {
    const expression_tokens = expression.tokens orelse return error.UnsupportedSqlShape;
    const name_tokens = expression.function_name_tokens orelse return error.UnsupportedSqlShape;
    if (name_tokens.start != expression_tokens.start or name_tokens.end >= expression_tokens.end or name_tokens.start >= name_tokens.end) return error.UnsupportedSqlShape;
    if (name_tokens.end >= tokens.len or tokens[name_tokens.end].kind != .lparen) return error.UnsupportedSqlShape;

    const function_close = if (expression.argument_tokens) |argument_tokens| blk: {
        if (argument_tokens.start != name_tokens.end + 1 or argument_tokens.end >= expression_tokens.end) return error.UnsupportedSqlShape;
        if (argument_tokens.start >= argument_tokens.end or tokens[argument_tokens.end].kind != .rparen) return error.UnsupportedSqlShape;
        const value_start = if (expression.argument_distinct_tokens) |distinct_tokens| distinct: {
            if (distinct_tokens.start != argument_tokens.start or distinct_tokens.end != distinct_tokens.start + 1) return error.UnsupportedSqlShape;
            if (!tokens[distinct_tokens.start].matchesKeywordTag(.distinct)) return error.UnsupportedSqlShape;
            break :distinct distinct_tokens.end;
        } else argument_tokens.start;
        const value_tokens = expression.argument_value_tokens orelse return error.UnsupportedSqlShape;
        if (value_tokens.start != value_start or value_tokens.end > argument_tokens.end or value_tokens.start >= value_tokens.end) return error.UnsupportedSqlShape;
        if (expression.argument_order_tokens) |order_tokens| {
            if (value_tokens.end + 2 > argument_tokens.end or value_tokens.end >= tokens.len) return error.UnsupportedSqlShape;
            if (!tokens[value_tokens.end].matchesKeywordTag(.order) or !tokens[value_tokens.end + 1].matchesKeywordTag(.by)) return error.UnsupportedSqlShape;
            if (order_tokens.start != value_tokens.end + 2 or order_tokens.end != argument_tokens.end) return error.UnsupportedSqlShape;
            try validateGeneratedOrderListForClause(tokens, order_tokens, expression.argument_order_items);
        } else {
            if (value_tokens.end != argument_tokens.end) return error.UnsupportedSqlShape;
            try validateGeneratedEmptyList(expression.argument_order_items);
        }
        try validateGeneratedExpressionListForClause(tokens, value_tokens, expression.argument_items);
        break :blk argument_tokens.end;
    } else blk: {
        if (name_tokens.end + 1 >= expression_tokens.end or tokens[name_tokens.end + 1].kind != .rparen) return error.UnsupportedSqlShape;
        if (expression.argument_distinct_tokens != null or
            expression.argument_value_tokens != null or
            expression.argument_order_tokens != null)
        {
            return error.UnsupportedSqlShape;
        }
        try validateGeneratedEmptyList(expression.argument_items);
        try validateGeneratedEmptyList(expression.argument_order_items);
        break :blk name_tokens.end + 1;
    };

    var cursor = function_close + 1;
    if (expression.within_group_tokens) |within_group_tokens| {
        const order_tokens = expression.within_group_order_tokens orelse return error.UnsupportedSqlShape;
        if (within_group_tokens.start != cursor or within_group_tokens.end > expression_tokens.end) return error.UnsupportedSqlShape;
        if (within_group_tokens.start + 6 > within_group_tokens.end) return error.UnsupportedSqlShape;
        if (!tokens[within_group_tokens.start].matchesKeywordTag(.within) or
            !tokens[within_group_tokens.start + 1].matchesKeywordTag(.group) or
            tokens[within_group_tokens.start + 2].kind != .lparen or
            !tokens[within_group_tokens.start + 3].matchesKeywordTag(.order) or
            !tokens[within_group_tokens.start + 4].matchesKeywordTag(.by) or
            tokens[within_group_tokens.end - 1].kind != .rparen)
        {
            return error.UnsupportedSqlShape;
        }
        if (order_tokens.start != within_group_tokens.start + 5 or order_tokens.end != within_group_tokens.end - 1) return error.UnsupportedSqlShape;
        try validateGeneratedOrderListForClause(tokens, order_tokens, expression.within_group_order_items);
        cursor = within_group_tokens.end;
    } else {
        if (expression.within_group_order_tokens != null) return error.UnsupportedSqlShape;
        try validateGeneratedEmptyList(expression.within_group_order_items);
    }

    if (expression.filter_tokens) |filter_tokens| {
        const predicate_tokens = expression.filter_predicate_tokens orelse return error.UnsupportedSqlShape;
        if (filter_tokens.start != cursor or filter_tokens.end > expression_tokens.end) return error.UnsupportedSqlShape;
        if (filter_tokens.start + 4 > filter_tokens.end or
            !tokens[filter_tokens.start].matchesKeywordTag(.filter) or
            tokens[filter_tokens.start + 1].kind != .lparen or
            !tokens[filter_tokens.start + 2].matchesKeywordTag(.where) or
            tokens[filter_tokens.end - 1].kind != .rparen)
        {
            return error.UnsupportedSqlShape;
        }
        if (predicate_tokens.start != filter_tokens.start + 3 or predicate_tokens.end != filter_tokens.end - 1) return error.UnsupportedSqlShape;
        try validateGeneratedChildExpressionPayloads(tokens, predicate_tokens, expression.filter_expression_kind, expression.filter_expression);
        cursor = filter_tokens.end;
    } else if (expression.filter_predicate_tokens != null or expression.filter_expression != null or expression.filter_expression_kind != null) {
        return error.UnsupportedSqlShape;
    }

    if (expression.over_tokens) |over_tokens| {
        if (over_tokens.start != cursor or over_tokens.end != expression_tokens.end) return error.UnsupportedSqlShape;
        try validateGeneratedWindowOverClauseForSpec(tokens, null, over_tokens.start, over_tokens.end, &expression);
        cursor = over_tokens.end;
    } else if (expression.over_name_tokens != null or
        expression.over_definition_tokens != null or
        expression.over_partition_tokens != null or
        expression.over_order_tokens != null or
        expression.over_frame_tokens != null or
        expression.over_frame_unit != null or
        expression.over_frame_start_bound != null or
        expression.over_frame_start_expression_tokens != null or
        expression.over_frame_start_expression != null or
        expression.over_frame_start_expression_kind != null or
        expression.over_frame_end_bound != null or
        expression.over_frame_end_expression_tokens != null or
        expression.over_frame_end_expression != null or
        expression.over_frame_end_expression_kind != null)
    {
        return error.UnsupportedSqlShape;
    }
    if (expression.over_tokens == null) {
        try validateGeneratedEmptyList(expression.over_partition_items);
        try validateGeneratedEmptyList(expression.over_order_items);
    }

    if (cursor != expression_tokens.end) return error.UnsupportedSqlShape;
}

fn validateGeneratedExpressionPayloads(
    tokens: []const Token,
    expression: generated_parser.GeneratedSqlExpressionAst,
) anyerror!void {
    const expression_tokens = expression.tokens orelse return error.UnsupportedSqlShape;
    if (expression_tokens.start >= expression_tokens.end or expression_tokens.end > tokens.len) return error.UnsupportedSqlShape;
    try validateGeneratedExpressionFamilyPayloads(tokens, expression, expression_tokens);

    if (expression.kind != .subquery) {
        try validateGeneratedOptionalChildExpressionGroup(expression.inner_tokens, expression.inner_expression_kind, expression.inner_expression);
    }
    try validateGeneratedOptionalChildExpressionGroup(expression.subquery_where_tokens, expression.subquery_where_expression_kind, expression.subquery_where_expression);
    try validateGeneratedOptionalChildExpressionGroup(expression.filter_predicate_tokens, expression.filter_expression_kind, expression.filter_expression);
    try validateGeneratedOptionalChildExpressionGroup(expression.over_frame_start_expression_tokens, expression.over_frame_start_expression_kind, expression.over_frame_start_expression);
    try validateGeneratedOptionalChildExpressionGroup(expression.over_frame_end_expression_tokens, expression.over_frame_end_expression_kind, expression.over_frame_end_expression);
    try validateGeneratedOptionalChildExpressionGroup(expression.cast_expression_tokens, expression.cast_expression_kind, expression.cast_expression);
    try validateGeneratedOptionalChildExpressionGroup(expression.case_first_condition_tokens, expression.case_first_condition_kind, expression.case_first_condition);
    try validateGeneratedOptionalChildExpressionGroup(expression.case_first_result_tokens, expression.case_first_result_kind, expression.case_first_result);
    try validateGeneratedOptionalChildExpressionGroup(expression.case_else_expression_tokens, expression.case_else_expression_kind, expression.case_else_expression);
    try validateGeneratedOptionalChildExpressionGroup(expression.boolean_first_condition_tokens, expression.boolean_first_condition_kind, expression.boolean_first_condition);
    try validateGeneratedOptionalChildExpressionGroup(expression.boolean_last_condition_tokens, expression.boolean_last_condition_kind, expression.boolean_last_condition);
    try validateGeneratedOptionalChildExpressionGroup(expression.extract_source_tokens, expression.extract_source_expression_kind, expression.extract_source_expression);
    try validateGeneratedOptionalChildExpressionGroup(expression.left_tokens, expression.left_expression_kind, expression.left_expression);
    try validateGeneratedOptionalChildExpressionGroup(expression.between_lower_tokens, expression.between_lower_expression_kind, expression.between_lower_expression);
    try validateGeneratedOptionalChildExpressionGroup(expression.between_upper_tokens, expression.between_upper_expression_kind, expression.between_upper_expression);
    try validateGeneratedOptionalChildExpressionGroup(expression.right_tokens, expression.right_expression_kind, expression.right_expression);
    try validateGeneratedOptionalChildExpressionGroup(expression.escape_tokens, expression.escape_expression_kind, expression.escape_expression);

    switch (expression.kind) {
        .quantified_comparison => {
            const left_tokens = expression.left_tokens orelse return error.UnsupportedSqlShape;
            const operator_tokens = expression.operator_tokens orelse return error.UnsupportedSqlShape;
            const quantifier_tokens = expression.quantifier_tokens orelse return error.UnsupportedSqlShape;
            const right_tokens = expression.right_tokens orelse return error.UnsupportedSqlShape;
            if (left_tokens.start != expression_tokens.start or left_tokens.end != operator_tokens.start) return error.UnsupportedSqlShape;
            if (operator_tokens.end != quantifier_tokens.start or quantifier_tokens.end != right_tokens.start or right_tokens.end != expression_tokens.end) return error.UnsupportedSqlShape;
            if (quantifier_tokens.end != quantifier_tokens.start + 1) return error.UnsupportedSqlShape;
            if (!tokens[quantifier_tokens.start].matchesKeywordTag(.any) and !tokens[quantifier_tokens.start].matchesKeywordTag(.some) and !tokens[quantifier_tokens.start].matchesKeywordTag(.all)) return error.UnsupportedSqlShape;
            try validateGeneratedChildExpressionPayloads(tokens, left_tokens, expression.left_expression_kind, expression.left_expression);
            try validateGeneratedChildExpressionPayloads(tokens, right_tokens, expression.right_expression_kind, expression.right_expression);
        },
        .exists_subquery, .not_exists_subquery => {
            if (expression.left_tokens != null or expression.left_expression != null or expression.left_expression_kind != null) return error.UnsupportedSqlShape;
            const operator_tokens = expression.operator_tokens orelse return error.UnsupportedSqlShape;
            const right_tokens = expression.right_tokens orelse return error.UnsupportedSqlShape;
            if (expression.kind == .not_exists_subquery) {
                const negation_tokens = expression.negation_tokens orelse return error.UnsupportedSqlShape;
                if (negation_tokens.start != expression_tokens.start or negation_tokens.end != operator_tokens.start) return error.UnsupportedSqlShape;
                if (!tokens[negation_tokens.start].matchesKeywordTag(.not)) return error.UnsupportedSqlShape;
            } else if (expression.negation_tokens != null or operator_tokens.start != expression_tokens.start) {
                return error.UnsupportedSqlShape;
            }
            if (operator_tokens.end != right_tokens.start or right_tokens.end != expression_tokens.end) return error.UnsupportedSqlShape;
            if (!tokens[operator_tokens.start].matchesKeywordTag(.exists)) return error.UnsupportedSqlShape;
            try validateGeneratedChildExpressionPayloads(tokens, right_tokens, .subquery, expression.right_expression);
        },
        .like, .ilike, .not_like, .not_ilike => {
            const operator_tokens = expression.operator_tokens orelse return error.UnsupportedSqlShape;
            const right_tokens = expression.right_tokens orelse return error.UnsupportedSqlShape;
            if (right_tokens.start >= right_tokens.end) return error.UnsupportedSqlShape;
            if (expression.quantifier_tokens != null) {
                const quantifier_tokens = expression.quantifier_tokens.?;
                if (operator_tokens.end != quantifier_tokens.start or quantifier_tokens.end != right_tokens.start) return error.UnsupportedSqlShape;
                if (expression.escape_tokens != null or expression.escape_expression_kind != null or expression.escape_expression != null) return error.UnsupportedSqlShape;
                if (right_tokens.end != expression_tokens.end) return error.UnsupportedSqlShape;
            } else if (right_tokens.start != operator_tokens.end) {
                return error.UnsupportedSqlShape;
            } else if (expression.escape_tokens) |escape_tokens| {
                if (right_tokens.end != escape_tokens.start or escape_tokens.end != expression_tokens.end) return error.UnsupportedSqlShape;
            } else if (right_tokens.end != expression_tokens.end) {
                return error.UnsupportedSqlShape;
            }
        },
        .subquery => try validateGeneratedSubqueryPayloads(tokens, expression),
        .grouped => {
            const inner_tokens = expression.inner_tokens orelse return error.UnsupportedSqlShape;
            if (inner_tokens.start <= expression_tokens.start or inner_tokens.end >= expression_tokens.end) return error.UnsupportedSqlShape;
            try validateGeneratedChildExpressionPayloads(tokens, inner_tokens, expression.inner_expression_kind, expression.inner_expression);
        },
        .array_constructor => try validateGeneratedArrayConstructorPayloads(tokens, expression),
        .function_call => try validateGeneratedFunctionCallPayloads(tokens, expression),
        else => {},
    }

    if (expression.left_tokens) |range| try validateGeneratedChildExpressionPayloads(tokens, range, expression.left_expression_kind, expression.left_expression);
    if (expression.right_tokens) |range| try validateGeneratedChildExpressionPayloads(tokens, range, expression.right_expression_kind, expression.right_expression);
    if (expression.kind == .grouped) {
        if (expression.inner_tokens) |range| try validateGeneratedChildExpressionPayloads(tokens, range, expression.inner_expression_kind, expression.inner_expression);
    }
    if (expression.cast_expression_tokens) |range| try validateGeneratedChildExpressionPayloads(tokens, range, expression.cast_expression_kind, expression.cast_expression);
    if (expression.between_lower_tokens) |range| try validateGeneratedChildExpressionPayloads(tokens, range, expression.between_lower_expression_kind, expression.between_lower_expression);
    if (expression.between_upper_tokens) |range| try validateGeneratedChildExpressionPayloads(tokens, range, expression.between_upper_expression_kind, expression.between_upper_expression);
    if (expression.escape_tokens) |range| {
        if (range.start + 1 >= range.end or !tokens[range.start].matchesKeywordTag(.escape)) return error.UnsupportedSqlShape;
        try validateGeneratedChildExpressionPayloads(tokens, .{ .start = range.start + 1, .end = range.end }, expression.escape_expression_kind, expression.escape_expression);
    }
    if (expression.filter_predicate_tokens) |range| try validateGeneratedChildExpressionPayloads(tokens, range, expression.filter_expression_kind, expression.filter_expression);
}

fn validateGeneratedExpressionPayloadsIfRetained(
    tokens: []const Token,
    expression: generated_parser.GeneratedSqlExpressionAst,
) !void {
    if (expression.tokens != null) try validateGeneratedExpressionPayloads(tokens, expression);
}

pub fn generatedProjectionClauseEnd(
    tokens: []const Token,
    pos: usize,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
    use_set_operation_right_side: bool,
) !?usize {
    const read = generated_read_ast orelse return null;
    if (use_set_operation_right_side) {
        if (read.set_operation_tokens == null) return error.UnsupportedSqlShape;
        if (read.set_operation.right_projection_tokens) |right_range| {
            if (right_range.start == pos) {
                if (right_range.end > tokens.len) return error.UnsupportedSqlShape;
                try validateGeneratedProjectionListForClause(tokens, right_range, read.set_operation.right_projection_items);
                return right_range.end;
            }
            if (right_range.end <= pos) return null;
        }
        if (try generatedSetOperationRightQueryEndedBefore(read, pos)) return null;
        return error.UnsupportedSqlShape;
    }
    if (read.projection_tokens) |range| {
        if (range.start == pos) {
            if (range.end > tokens.len) return error.UnsupportedSqlShape;
            try validateGeneratedProjectionListForClause(tokens, range, read.projection_items);
            return range.end;
        }
    }
    if (read.kind == .set_operation) {
        if (read.set_operation.right_projection_tokens) |right_range| {
            if (right_range.start == pos) {
                if (right_range.end > tokens.len) return error.UnsupportedSqlShape;
                try validateGeneratedProjectionListForClause(tokens, right_range, read.set_operation.right_projection_items);
                return right_range.end;
            }
        }
        return null;
    }
    return error.UnsupportedSqlShape;
}

pub const GeneratedDistinctClause = struct {
    end: usize,
    kind: enum { plain, distinct_on },
};

pub fn validateGeneratedDistinctClause(
    tokens: []const Token,
    range: generated_parser.GeneratedSqlTokenRange,
    list: generated_parser.GeneratedSqlListAst,
) !GeneratedDistinctClause {
    if (range.start >= range.end or range.end > tokens.len) return error.UnsupportedSqlShape;
    if (!tokens[range.start].matchesKeywordTag(.distinct)) return error.UnsupportedSqlShape;
    if (range.end == range.start + 1) {
        try validateGeneratedEmptyList(list);
        return .{ .end = range.end, .kind = .plain };
    }
    if (range.start + 4 > range.end or
        !tokens[range.start + 1].matchesKeywordTag(.on) or
        tokens[range.start + 2].kind != .lparen or
        tokens[range.end - 1].kind != .rparen)
    {
        return error.UnsupportedSqlShape;
    }
    try validateGeneratedExpressionListForClause(tokens, .{ .start = range.start + 3, .end = range.end - 1 }, list);
    return .{ .end = range.end, .kind = .distinct_on };
}

pub fn generatedDistinctClauseEnd(
    tokens: []const Token,
    pos: usize,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
    use_set_operation_right_side: bool,
) !?GeneratedDistinctClause {
    const read = generated_read_ast orelse return null;
    if (use_set_operation_right_side) {
        if (read.set_operation_tokens == null) return error.UnsupportedSqlShape;
        if (read.set_operation.right_distinct_tokens) |right_range| {
            if (right_range.start == pos) {
                return try validateGeneratedDistinctClause(tokens, right_range, read.set_operation.right_distinct_on_items);
            }
            if (right_range.end <= pos) return null;
            return error.UnsupportedSqlShape;
        }
        if (try generatedSetOperationRightQueryEndedBefore(read, pos)) return null;
        if (pos < tokens.len and tokens[pos].matchesKeywordTag(.distinct)) return error.UnsupportedSqlShape;
        return null;
    }
    if (read.distinct_tokens) |range| {
        if (range.start == pos) {
            return try validateGeneratedDistinctClause(tokens, range, read.distinct_on_items);
        }
        if (read.kind != .set_operation) return error.UnsupportedSqlShape;
    }
    if (read.kind == .set_operation) {
        if (read.set_operation.right_distinct_tokens) |right_range| {
            if (right_range.start == pos) {
                return try validateGeneratedDistinctClause(tokens, right_range, read.set_operation.right_distinct_on_items);
            }
        }
    }
    if (pos < tokens.len and tokens[pos].matchesKeywordTag(.distinct)) return error.UnsupportedSqlShape;
    return null;
}

pub fn generatedSourceClauseEnd(
    tokens: []const Token,
    pos: usize,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
    use_set_operation_right_side: bool,
) !?usize {
    const read = generated_read_ast orelse return null;
    if (use_set_operation_right_side) {
        if (read.set_operation_tokens == null) return error.UnsupportedSqlShape;
        if (read.set_operation.right_source_tokens) |right_range| {
            if (right_range.start == pos) {
                if (right_range.end > tokens.len) return error.UnsupportedSqlShape;
                if (read.set_operation.right_source_system_time_tokens) |system_time| {
                    if (system_time.start <= right_range.start or system_time.end != right_range.end) return error.UnsupportedSqlShape;
                    return system_time.start;
                }
                return right_range.end;
            }
            if (right_range.end <= pos) return null;
        }
        if (try generatedSetOperationRightQueryEndedBefore(read, pos)) return null;
        return error.UnsupportedSqlShape;
    }
    if (read.source_tokens) |range| {
        if (range.start == pos) {
            if (range.end > tokens.len) return error.UnsupportedSqlShape;
            if (read.source_system_time_tokens) |system_time| {
                if (system_time.start <= range.start or system_time.end != range.end) return error.UnsupportedSqlShape;
                return system_time.start;
            }
            return range.end;
        }
    }
    if (read.kind == .set_operation) {
        if (read.set_operation.right_source_tokens) |right_range| {
            if (right_range.start == pos) {
                if (right_range.end > tokens.len) return error.UnsupportedSqlShape;
                return right_range.end;
            }
        }
        return null;
    }
    return error.UnsupportedSqlShape;
}

pub const GeneratedReadTableAlias = struct {
    table_ref: plan_mod.TableAlias,
    source_body_end: usize,
};

fn generatedTableAliasFromReadSourceMetadataAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    read: *const generated_parser.GeneratedSqlReadAst,
) !?GeneratedReadTableAlias {
    return try generatedTableAliasFromSourceMetadataAlloc(
        alloc,
        tokens,
        read.source_tokens,
        read.source_table_tokens,
        read.source_alias_tokens,
        read.source_alias_name_tokens,
        read.source_system_time_tokens,
    );
}

fn generatedTableAliasFromSetOperationRightSourceMetadataAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    read: *const generated_parser.GeneratedSqlReadAst,
) !?GeneratedReadTableAlias {
    if (read.set_operation_tokens == null) return error.UnsupportedSqlShape;
    return try generatedTableAliasFromSourceMetadataAlloc(
        alloc,
        tokens,
        read.set_operation.right_source_tokens,
        read.set_operation.right_source_table_tokens,
        read.set_operation.right_source_alias_tokens,
        read.set_operation.right_source_alias_name_tokens,
        read.set_operation.right_source_system_time_tokens,
    );
}

fn generatedReadSourceRangeForSide(
    read: *const generated_parser.GeneratedSqlReadAst,
    use_set_operation_right_side: bool,
) !?generated_parser.GeneratedSqlTokenRange {
    if (use_set_operation_right_side) {
        if (read.set_operation_tokens == null) return error.UnsupportedSqlShape;
        return read.set_operation.right_source_tokens orelse error.UnsupportedSqlShape;
    }
    return read.source_tokens;
}

fn generatedTableAliasFromJoinSideMetadataAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    join: generated_parser.GeneratedSqlJoinAst,
    side: GeneratedJoinSide,
) !?GeneratedReadTableAlias {
    return switch (side) {
        .left => try generatedTableAliasFromSourceMetadataAlloc(
            alloc,
            tokens,
            join.left_tokens,
            join.left_table_tokens,
            join.left_alias_tokens,
            join.left_alias_name_tokens,
            null,
        ),
        .right => try generatedTableAliasFromSourceMetadataAlloc(
            alloc,
            tokens,
            join.right_tokens,
            join.right_table_tokens,
            join.right_alias_tokens,
            join.right_alias_name_tokens,
            null,
        ),
    };
}

fn generatedTableAliasFromSourceMetadataAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    maybe_source: ?generated_parser.GeneratedSqlTokenRange,
    maybe_source_table: ?generated_parser.GeneratedSqlTokenRange,
    maybe_source_alias: ?generated_parser.GeneratedSqlTokenRange,
    maybe_source_alias_name: ?generated_parser.GeneratedSqlTokenRange,
    maybe_source_system_time: ?generated_parser.GeneratedSqlTokenRange,
) !?GeneratedReadTableAlias {
    const source = maybe_source orelse return null;
    if (source.start >= source.end or source.end > tokens.len) return error.UnsupportedSqlShape;
    const source_body_end = if (maybe_source_system_time) |system_time| blk: {
        if (system_time.start <= source.start or system_time.end != source.end) return error.UnsupportedSqlShape;
        break :blk system_time.start;
    } else source.end;

    const source_table = maybe_source_table orelse {
        if (maybe_source_alias != null or maybe_source_alias_name != null or maybe_source_system_time != null) return error.UnsupportedSqlShape;
        if (generatedReadSourceLooksLikeSingleTableSource(tokens, .{ .start = source.start, .end = source_body_end })) return error.UnsupportedSqlShape;
        return null;
    };
    var expected_table_start = source.start;
    if (tokens[expected_table_start].matchesKeywordTag(.only)) expected_table_start += 1;
    if (source_table.start != expected_table_start or source_table.end != expected_table_start + 1 or source_table.end > source_body_end) return error.UnsupportedSqlShape;
    if (tokens[source_table.start].kind != .identifier) return error.UnsupportedSqlShape;

    const name = try grammar.normalizeSqlObjectIdentifierAlloc(alloc, tokens[source_table.start].text);
    var name_transferred = false;
    errdefer if (!name_transferred) alloc.free(name);

    const alias = if (maybe_source_alias_name) |alias_name_tokens| alias: {
        if (alias_name_tokens.start >= alias_name_tokens.end or alias_name_tokens.end > source_body_end) return error.UnsupportedSqlShape;
        if (alias_name_tokens.end != alias_name_tokens.start + 1) return error.UnsupportedSqlShape;
        if (tokens[alias_name_tokens.start].kind != .identifier) return error.UnsupportedSqlShape;
        break :alias try alloc.dupe(u8, tokens[alias_name_tokens.start].text);
    } else try alloc.dupe(u8, name);
    var alias_transferred = false;
    errdefer if (!alias_transferred) alloc.free(alias);

    const alias_end = try generatedSingleSourceAliasEnd(
        tokens,
        source_table,
        maybe_source_alias,
        maybe_source_alias_name,
    );
    if (alias_end != source_body_end) return error.UnsupportedSqlShape;

    name_transferred = true;
    alias_transferred = true;
    return .{
        .table_ref = .{ .name = name, .alias = alias },
        .source_body_end = source_body_end,
    };
}

pub fn generatedTableAliasFromReadSourceAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
    use_set_operation_right_side: bool,
) !?plan_mod.TableAlias {
    const read = generated_read_ast orelse return null;
    const source = (try generatedReadSourceRangeForSide(read, use_set_operation_right_side)) orelse return null;
    if (source.start != pos.*) {
        if (use_set_operation_right_side and source.end <= pos.*) return null;
        return error.UnsupportedSqlShape;
    }
    const generated = if (use_set_operation_right_side)
        (try generatedTableAliasFromSetOperationRightSourceMetadataAlloc(alloc, tokens, read)) orelse return null
    else
        (try generatedTableAliasFromReadSourceMetadataAlloc(alloc, tokens, read)) orelse return null;
    if (generated.source_body_end < pos.*) {
        plan_mod.freeTableAlias(alloc, generated.table_ref);
        return error.UnsupportedSqlShape;
    }
    pos.* = generated.source_body_end;
    return generated.table_ref;
}

pub fn inferGeneratedTableAliasFromReadSourceAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    select_body_pos: usize,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
    use_set_operation_right_side: bool,
) !?plan_mod.TableAlias {
    const read = generated_read_ast orelse return null;
    const source = (try generatedReadSourceRangeForSide(read, use_set_operation_right_side)) orelse return null;
    if (source.start <= select_body_pos) {
        if (use_set_operation_right_side and source.end <= select_body_pos) return null;
        return error.UnsupportedSqlShape;
    }
    const generated = if (use_set_operation_right_side)
        (try generatedTableAliasFromSetOperationRightSourceMetadataAlloc(alloc, tokens, read)) orelse return null
    else
        (try generatedTableAliasFromReadSourceMetadataAlloc(alloc, tokens, read)) orelse return null;
    return generated.table_ref;
}

fn generatedSetOperationRightQueryEndedBefore(
    read: *const generated_parser.GeneratedSqlReadAst,
    pos: usize,
) !bool {
    const right_query = read.set_operation.right_query_tokens orelse return error.UnsupportedSqlShape;
    return right_query.end <= pos;
}

const GeneratedJoinSide = enum { left, right };

pub fn generatedTableAliasFromJoinSideAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
    side: GeneratedJoinSide,
) !?plan_mod.TableAlias {
    const read = generated_read_ast orelse return null;
    if (read.kind != .join and read.kind != .lateral and read.kind != .cte) return error.UnsupportedSqlShape;
    try validateGeneratedJoinItemsMetadata(tokens, read.*);
    const root_index = read.join_tree_root_index orelse return error.UnsupportedSqlShape;
    if (read.join_items.len != 1 or root_index != 0 or read.join_tree_depth != 1) return error.UnsupportedSqlShape;
    const join = read.join_items[0];
    const source = switch (side) {
        .left => join.left_tokens,
        .right => join.right_tokens,
    };
    if (source.start != pos.*) return error.UnsupportedSqlShape;
    if (source.start >= source.end or source.end > tokens.len) return error.UnsupportedSqlShape;
    if (try generatedTableAliasFromJoinSideMetadataAlloc(alloc, tokens, join, side)) |generated| {
        if (generated.source_body_end != source.end) {
            plan_mod.freeTableAlias(alloc, generated.table_ref);
            return error.UnsupportedSqlShape;
        }
        pos.* = source.end;
        return generated.table_ref;
    }
    return null;
}

pub fn validateGeneratedSingleSourceAliasForParsedTable(
    tokens: []const Token,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
    source_start: usize,
    parsed_end: usize,
) !void {
    const read = generated_read_ast orelse return;
    const source = read.source_tokens orelse return;
    if (source.start != source_start) return;
    if (source.start >= source.end or source.end > tokens.len) return error.UnsupportedSqlShape;
    const source_body_end = if (read.source_system_time_tokens) |system_time| blk: {
        if (system_time.start <= source.start or system_time.end != source.end) return error.UnsupportedSqlShape;
        break :blk system_time.start;
    } else source.end;

    const source_table = read.source_table_tokens orelse {
        if (read.source_alias_tokens != null or read.source_alias_name_tokens != null) return error.UnsupportedSqlShape;
        if (read.source_antfly_function_count != 0 or read.source_graph_function_count != 0 or read.join_items.len != 0) return;
        if (!generatedReadSourceLooksLikeSingleTableSource(tokens, source)) return;
        if (parsed_end != source_body_end) return;
        return error.UnsupportedSqlShape;
    };
    var expected_table_start = source.start;
    if (tokens[expected_table_start].matchesKeywordTag(.only)) expected_table_start += 1;
    if (source_table.start != expected_table_start or source_table.end != expected_table_start + 1 or source_table.end > source.end) return error.UnsupportedSqlShape;
    if (tokens[source_table.start].kind != .identifier) return error.UnsupportedSqlShape;

    const alias_end = try generatedSingleSourceAliasEnd(
        tokens,
        source_table,
        read.source_alias_tokens,
        read.source_alias_name_tokens,
    );
    if (alias_end != source_body_end or parsed_end != alias_end) return error.UnsupportedSqlShape;
}

pub fn validateGeneratedSystemTimeForParsedSource(
    tokens: []const Token,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
    source_start: usize,
    parsed_table_end: usize,
    parsed_end: usize,
    parsed_system_time: expr_parse.ParsedSystemTimeAsOf,
) !void {
    const read = generated_read_ast orelse {
        if (parsed_system_time.present()) return error.UnsupportedSqlShape;
        return;
    };
    const source = read.source_tokens orelse {
        if (parsed_system_time.present()) return error.UnsupportedSqlShape;
        return;
    };
    if (source.start != source_start) return;
    const system_time = read.source_system_time_tokens orelse {
        if (parsed_system_time.present()) return error.UnsupportedSqlShape;
        return;
    };
    const sequence = read.source_system_time_sequence_tokens orelse return error.UnsupportedSqlShape;
    if (system_time.start != parsed_table_end or system_time.end != parsed_end or system_time.end != source.end) return error.UnsupportedSqlShape;
    if (system_time.end > tokens.len or system_time.end < system_time.start + 5) return error.UnsupportedSqlShape;
    if (!tokens[system_time.start].matchesKeywordTag(.@"for")) return error.UnsupportedSqlShape;
    if (!std.ascii.eqlIgnoreCase(tokens[system_time.start + 1].text, "system_time")) return error.UnsupportedSqlShape;
    if (!tokens[system_time.start + 2].matchesKeywordTag(.as)) return error.UnsupportedSqlShape;
    if (!tokens[system_time.start + 3].matchesKeywordTag(.of)) return error.UnsupportedSqlShape;
    if (sequence.start != system_time.start + 4 or sequence.end != system_time.end) return error.UnsupportedSqlShape;
    try validateGeneratedSystemTimePayloadTokenRange(tokens, sequence);
    if (!parsed_system_time.present()) return error.UnsupportedSqlShape;
}

fn generatedReadSourceLooksLikeSingleTableSource(
    tokens: []const Token,
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
        !plan_mod.nextIsJoinClauseKeyword(tokens, table_end))
    {
        return true;
    }
    return false;
}

fn generatedSingleSourceAliasEnd(
    tokens: []const Token,
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
        !plan_mod.nextIsJoinClauseKeyword(tokens, source_table.end))
    blk: {
        break :blk alias;
    } else return error.UnsupportedSqlShape;
    if (!expr_generated.generatedTokenRangeEqual(alias_name, expected_name)) return error.UnsupportedSqlShape;
    return alias.end;
}

pub fn generatedGroupClauseEnd(
    tokens: []const Token,
    keyword_index: usize,
    pos: usize,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
) !?usize {
    const read = generated_read_ast orelse return null;
    if (keyword_index + 1 >= tokens.len or !tokens[keyword_index].matchesKeywordTag(.group) or !tokens[keyword_index + 1].matchesKeywordTag(.by)) return null;
    const range = read.group_tokens orelse return error.UnsupportedSqlShape;
    if (range.start != pos or range.end > tokens.len) return error.UnsupportedSqlShape;
    try validateGeneratedExpressionListForClause(tokens, range, read.group_items);
    return range.end;
}

pub fn generatedHavingClauseEnd(
    tokens: []const Token,
    keyword_index: usize,
    pos: usize,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
) !?usize {
    const read = generated_read_ast orelse return null;
    if (keyword_index >= tokens.len or !tokens[keyword_index].matchesKeywordTag(.having)) return null;
    const range = read.having_tokens orelse return error.UnsupportedSqlShape;
    if (range.start != pos or range.end > tokens.len) return error.UnsupportedSqlShape;
    if (!expr_generated.generatedTokenRangeEqual(read.having_expression.tokens orelse return error.UnsupportedSqlShape, range)) return error.UnsupportedSqlShape;
    try validateGeneratedExpressionPayloads(tokens, read.having_expression);
    return range.end;
}

pub fn validateGeneratedWindowListForClause(
    tokens: []const Token,
    range: generated_parser.GeneratedSqlTokenRange,
    windows: []const generated_parser.GeneratedSqlWindowAst,
) !void {
    if (range.start >= range.end or range.end > tokens.len or windows.len == 0) return error.UnsupportedSqlShape;
    for (windows, 0..) |window, index| {
        if (window.tokens.start >= window.tokens.end or window.tokens.start < range.start or window.tokens.end > range.end) return error.UnsupportedSqlShape;
        if (index == 0) {
            if (window.tokens.start != range.start) return error.UnsupportedSqlShape;
        } else {
            const previous = windows[index - 1].tokens;
            if (previous.end + 1 != window.tokens.start or previous.end >= tokens.len or tokens[previous.end].kind != .comma) return error.UnsupportedSqlShape;
        }
        if (index + 1 == windows.len and window.tokens.end != range.end) return error.UnsupportedSqlShape;
        if (window.name_tokens.start != window.tokens.start or window.name_tokens.start >= window.name_tokens.end or window.name_tokens.end + 2 > window.tokens.end) return error.UnsupportedSqlShape;
        if (!tokens[window.name_tokens.end].matchesKeywordTag(.as) or tokens[window.name_tokens.end + 1].kind != .lparen or tokens[window.tokens.end - 1].kind != .rparen) return error.UnsupportedSqlShape;
        if (window.definition_tokens.start != window.name_tokens.end + 2 or window.definition_tokens.end != window.tokens.end - 1) return error.UnsupportedSqlShape;

        if (window.partition_tokens) |partition_range| {
            if (partition_range.start < window.definition_tokens.start or partition_range.end > window.definition_tokens.end) return error.UnsupportedSqlShape;
            try validateGeneratedExpressionListForClause(tokens, partition_range, window.partition_items);
        } else {
            try validateGeneratedEmptyList(window.partition_items);
        }

        if (window.order_tokens) |order_range| {
            if (order_range.start < window.definition_tokens.start or order_range.end > window.definition_tokens.end) return error.UnsupportedSqlShape;
            try validateGeneratedOrderListForClause(tokens, order_range, window.order_items);
        } else {
            try validateGeneratedEmptyList(window.order_items);
        }

        if (window.frame_tokens) |frame_range| {
            if (window.frame_unit == null or window.frame_start_bound == null or window.frame_end_bound == null) return error.UnsupportedSqlShape;
            if (frame_range.start < window.definition_tokens.start or frame_range.end > window.definition_tokens.end) return error.UnsupportedSqlShape;
            try validateGeneratedWindowFrameClauseLayout(
                tokens,
                frame_range,
                window.frame_start_expression_kind,
                window.frame_start_expression_tokens,
                window.frame_start_expression,
                window.frame_end_expression_kind,
                window.frame_end_expression_tokens,
                window.frame_end_expression,
            );
            if (window.frame_start_expression_tokens) |expression_range| {
                if (expression_range.start < frame_range.start or expression_range.end > frame_range.end) return error.UnsupportedSqlShape;
                if (!expr_generated.generatedTokenRangeEqual((window.frame_start_expression orelse return error.UnsupportedSqlShape).tokens orelse return error.UnsupportedSqlShape, expression_range)) return error.UnsupportedSqlShape;
            } else if (window.frame_start_expression != null or window.frame_start_expression_kind != null) {
                return error.UnsupportedSqlShape;
            }
            if (window.frame_end_expression_tokens) |expression_range| {
                if (expression_range.start < frame_range.start or expression_range.end > frame_range.end) return error.UnsupportedSqlShape;
                if (!expr_generated.generatedTokenRangeEqual((window.frame_end_expression orelse return error.UnsupportedSqlShape).tokens orelse return error.UnsupportedSqlShape, expression_range)) return error.UnsupportedSqlShape;
            } else if (window.frame_end_expression != null or window.frame_end_expression_kind != null) {
                return error.UnsupportedSqlShape;
            }
        } else if (window.frame_unit != null or window.frame_start_bound != null or window.frame_end_bound != null or
            window.frame_start_expression_tokens != null or window.frame_start_expression != null or window.frame_start_expression_kind != null or
            window.frame_end_expression_tokens != null or window.frame_end_expression != null or window.frame_end_expression_kind != null)
        {
            return error.UnsupportedSqlShape;
        }
    }
}

pub fn generatedWindowClauseEnd(
    tokens: []const Token,
    keyword_index: usize,
    pos: usize,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
) !?usize {
    const read = generated_read_ast orelse return null;
    if (keyword_index >= tokens.len or !tokens[keyword_index].matchesKeywordTag(.window)) return null;
    const range = read.window_tokens orelse return error.UnsupportedSqlShape;
    if (range.start != pos or range.end > tokens.len or read.window_items.len != read.window_count) return error.UnsupportedSqlShape;
    try validateGeneratedWindowListForClause(tokens, range, read.window_items);
    return range.end;
}

pub const GeneratedExpressionItem = struct {
    tokens: generated_parser.GeneratedSqlTokenRange,
    expression: *const generated_parser.GeneratedSqlExpressionAst,
};

fn generatedExpressionListItemAtStart(
    tokens: []const Token,
    pos: usize,
    items: *const generated_parser.GeneratedSqlListAst,
) !?GeneratedExpressionItem {
    if (items.items.len != items.count or items.expressions.len != items.count) return error.UnsupportedSqlShape;
    for (items.items, 0..) |item, index| {
        if (item.start == pos) {
            try validateGeneratedExpressionPayloads(tokens, items.expressions[index]);
            return .{
                .tokens = item,
                .expression = &items.expressions[index],
            };
        }
    }
    return null;
}

pub fn generatedProjectionItemAtStart(
    tokens: []const Token,
    pos: usize,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
) !?GeneratedExpressionItem {
    const read = generated_read_ast orelse return null;
    if (read.projection_items.items.len != read.projection_items.count or read.projection_items.expressions.len != read.projection_items.count) return error.UnsupportedSqlShape;
    for (read.projection_items.items, 0..) |item, index| {
        if (item.start == pos) {
            try validateGeneratedExpressionPayloads(tokens, read.projection_items.expressions[index]);
            return .{
                .tokens = item,
                .expression = &read.projection_items.expressions[index],
            };
        }
    }
    if (read.set_operation_tokens != null) {
        if (read.set_operation.right_projection_items.items.len != read.set_operation.right_projection_items.count or
            read.set_operation.right_projection_items.expressions.len != read.set_operation.right_projection_items.count)
        {
            return error.UnsupportedSqlShape;
        }
        for (read.set_operation.right_projection_items.items, 0..) |item, index| {
            if (item.start == pos) {
                try validateGeneratedExpressionPayloads(tokens, read.set_operation.right_projection_items.expressions[index]);
                return .{
                    .tokens = item,
                    .expression = &read.set_operation.right_projection_items.expressions[index],
                };
            }
        }
    }
    return null;
}

pub fn generatedProjectionExpressionAtItemStart(
    tokens: []const Token,
    pos: usize,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
) !?*const generated_parser.GeneratedSqlExpressionAst {
    const item = try generatedProjectionItemAtStart(tokens, pos, generated_read_ast);
    return if (item) |generated_item| generated_item.expression else null;
}

pub fn generatedGroupItemAtStart(
    tokens: []const Token,
    pos: usize,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
) !?GeneratedExpressionItem {
    const read = generated_read_ast orelse return null;
    if (read.group_items.items.len != read.group_items.count or read.group_items.expressions.len != read.group_items.count) return error.UnsupportedSqlShape;
    for (read.group_items.items, 0..) |item, index| {
        if (item.start == pos) {
            try validateGeneratedExpressionPayloads(tokens, read.group_items.expressions[index]);
            return .{
                .tokens = item,
                .expression = &read.group_items.expressions[index],
            };
        }
    }
    return null;
}

pub fn generatedGroupExpressionAtItemStart(
    tokens: []const Token,
    pos: usize,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
) !?*const generated_parser.GeneratedSqlExpressionAst {
    const item = try generatedGroupItemAtStart(tokens, pos, generated_read_ast);
    return if (item) |generated_item| generated_item.expression else null;
}

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

pub fn generatedOrderExpressionAtItemStart(
    tokens: []const Token,
    pos: usize,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
) !?*const generated_parser.GeneratedSqlExpressionAst {
    const item = try generatedOrderItemAtStart(tokens, pos, generated_read_ast);
    return if (item) |generated_item| generated_item.expression else null;
}

pub fn generatedReturningItemAtStart(
    tokens: []const Token,
    pos: usize,
    generated_returning_items: ?*const generated_parser.GeneratedSqlListAst,
) !?GeneratedExpressionItem {
    const list = generated_returning_items orelse return null;
    if (list.count == 0 or list.items.len != list.count or list.expressions.len != list.count) return error.UnsupportedSqlShape;
    const first = list.first_tokens orelse return error.UnsupportedSqlShape;
    const last = list.last_tokens orelse return error.UnsupportedSqlShape;
    try validateGeneratedProjectionListForClause(tokens, .{ .start = first.start, .end = last.end }, list.*);
    for (list.items, 0..) |item, index| {
        if (item.start == pos) {
            try validateGeneratedExpressionPayloads(tokens, list.expressions[index]);
            return .{
                .tokens = item,
                .expression = &list.expressions[index],
            };
        }
    }
    return error.UnsupportedSqlShape;
}

pub fn validateGeneratedExpressionItemEnd(generated_item: ?GeneratedExpressionItem, pos: usize) !void {
    if (generated_item) |item| {
        if (pos != item.tokens.end) return error.UnsupportedSqlShape;
    }
}

fn generatedSelectItemStartAllowsExpressionKind(
    start: expr_projection.SelectItemStart,
    kind: generated_parser.GeneratedSqlExpressionKind,
) bool {
    return switch (start) {
        .pipe_concat => kind == .string_concat,
        .unary_negative => kind == .unary_negative,
        .boolean_not => kind == .logical_not,
        .extension_function,
        .routine_expression,
        .uuid_v4,
        .json_extract_path,
        .json_typeof,
        .json_array_length,
        .json_build_object,
        .convert_from,
        .to_jsonb,
        .array_length,
        .array_position,
        .array_element_transform,
        .array_to_string,
        .string_to_array,
        .coalesce,
        .case_fold,
        .replace,
        .regexp_replace,
        .regexp_substr,
        .regexp_match,
        .regexp_count,
        .regexp_instr,
        .translate,
        .concat,
        .nullif,
        .text_length,
        .ascii,
        .chr,
        .substring,
        .overlay,
        .split_part,
        .strpos,
        .left_right,
        .pad,
        .repeat,
        .reverse,
        .md5,
        .starts_with,
        .ends_with,
        .date_trunc,
        .date_bin,
        .abs,
        .round,
        .trunc,
        .floor,
        .ceil,
        .sqrt,
        .sign,
        .mod,
        .power,
        .greatest_least,
        => kind == .function_call,
        .date_part => kind == .function_call or kind == .extract_expression,
        .now => kind == .function_call or kind == .current_timestamp,
        .current_date => kind == .current_date,
        .typed_datetime_literal => kind == .timestamp_literal,
        .case => kind == .case_expression,
        .cast => kind == .cast,
        .parenthesized => kind == .grouped,
    };
}

fn generatedExpressionFunctionNameToken(
    tokens: []const Token,
    expression: generated_parser.GeneratedSqlExpressionAst,
) !Token {
    if (expression.kind != .function_call) return error.UnsupportedSqlShape;
    const name_tokens = expression.function_name_tokens orelse return error.UnsupportedSqlShape;
    if (name_tokens.end != name_tokens.start + 1 or name_tokens.end > tokens.len) return error.UnsupportedSqlShape;
    return tokens[name_tokens.start];
}

fn windowFunctionForGeneratedKind(
    kind: generated_parser.GeneratedSqlWindowFunctionKind,
) db_mod.types.RelationalRowsWindowFunction {
    return switch (kind) {
        .row_number => .row_number,
        .rank => .rank,
        .dense_rank => .dense_rank,
        .percent_rank => .percent_rank,
        .cume_dist => .cume_dist,
        .ntile => .ntile,
        .lag => .lag,
        .lead => .lead,
        .first_value => .first_value,
        .last_value => .last_value,
        .nth_value => .nth_value,
        .count => .count,
        .sum => .sum,
        .avg => .avg,
        .min => .min,
        .max => .max,
        .bool_or => .bool_or,
        .bool_and => .bool_and,
    };
}

pub fn generatedWindowFunctionForExpression(
    tokens: []const Token,
    expression: *const generated_parser.GeneratedSqlExpressionAst,
) !?db_mod.types.RelationalRowsWindowFunction {
    try validateGeneratedExpressionPayloads(tokens, expression.*);
    if (expression.kind != .function_call) return null;
    const function = windowFunctionForGeneratedKind(expression.window_function_kind orelse return null);
    if (expression.over_tokens == null) return null;
    return function;
}

fn validateGeneratedSelectItemStartFunctionName(
    tokens: []const Token,
    start: expr_projection.SelectItemStart,
    expression: generated_parser.GeneratedSqlExpressionAst,
) !void {
    switch (start) {
        .uuid_v4,
        .json_extract_path,
        .json_typeof,
        .json_array_length,
        .json_build_object,
        .convert_from,
        .to_jsonb,
        .array_length,
        .array_position,
        .array_to_string,
        .string_to_array,
        .coalesce,
        .replace,
        .regexp_replace,
        .regexp_substr,
        .regexp_match,
        .regexp_count,
        .regexp_instr,
        .translate,
        .nullif,
        .ascii,
        .chr,
        .overlay,
        .repeat,
        .reverse,
        .md5,
        .starts_with,
        .ends_with,
        .date_trunc,
        .date_bin,
        .abs,
        .round,
        .mod,
        .power,
        => {},
        else => return,
    }

    const token = try generatedExpressionFunctionNameToken(tokens, expression);
    const valid = switch (start) {
        .uuid_v4 => expr_token.sqlTokenIsUuidV4Function(token),
        .json_extract_path => expr_token.sqlTokenIsJsonExtractPathFunction(token),
        .json_typeof => expr_token.sqlTokenIsJsonTypeofFunction(token),
        .json_array_length => expr_token.sqlTokenIsJsonArrayLengthFunction(token),
        .json_build_object => expr_token.sqlTokenIsJsonBuildObjectFunction(token),
        .convert_from => token.matchesKeywordTag(.convert_from),
        .to_jsonb => token.matchesKeywordTag(.to_jsonb),
        .array_length => expr_token.sqlTokenIsArrayLengthFunction(token),
        .array_position => expr_token.sqlTokenIsArrayPositionFunction(token),
        .array_to_string => expr_token.sqlTokenIsArrayToStringFunction(token),
        .string_to_array => token.matchesKeywordTag(.string_to_array),
        .coalesce => token.matchesKeywordTag(.coalesce),
        .replace => token.matchesKeywordTag(.replace),
        .regexp_replace => token.matchesKeywordTag(.regexp_replace),
        .regexp_substr => expr_token.sqlTokenIsRegexpSubstrFunction(token),
        .regexp_match => expr_token.sqlTokenIsRegexpMatchFunction(token),
        .regexp_count => expr_token.sqlTokenIsRegexpCountFunction(token),
        .regexp_instr => expr_token.sqlTokenIsRegexpInstrFunction(token),
        .translate => expr_token.sqlTokenIsTranslateFunction(token),
        .nullif => token.matchesKeywordTag(.nullif),
        .ascii => expr_token.sqlTokenIsAsciiFunction(token),
        .chr => expr_token.sqlTokenIsChrFunction(token),
        .overlay => expr_token.sqlTokenIsOverlayFunction(token),
        .repeat => expr_token.sqlTokenIsRepeatFunction(token),
        .reverse => expr_token.sqlTokenIsReverseFunction(token),
        .md5 => expr_token.sqlTokenIsMd5Function(token),
        .soundex => std.ascii.eqlIgnoreCase(token.text, "soundex"),
        .starts_with => expr_token.sqlTokenIsStartsWithFunction(token),
        .ends_with => expr_token.sqlTokenIsEndsWithFunction(token),
        .date_trunc => expr_token.sqlTokenIsDateTruncFunction(token),
        .date_bin => expr_token.sqlTokenIsDateBinFunction(token),
        .abs => token.matchesKeywordTag(.abs),
        .round => token.matchesKeywordTag(.round),
        .mod => token.matchesKeywordTag(.mod),
        .power => token.matchesKeywordTag(.power),
        else => unreachable,
    };
    if (!valid) return error.UnsupportedSqlShape;
}

fn validateGeneratedSelectItemStartForExpression(
    tokens: []const Token,
    start: expr_projection.SelectItemStart,
    generated_expression: ?*const generated_parser.GeneratedSqlExpressionAst,
) !void {
    const expression = generated_expression orelse return;
    try validateGeneratedExpressionPayloads(tokens, expression.*);
    if (!generatedSelectItemStartAllowsExpressionKind(start, expression.kind)) return error.UnsupportedSqlShape;
    try validateGeneratedSelectItemStartFunctionName(tokens, start, expression.*);
}

pub fn validateGeneratedSimpleGroupExpression(
    tokens: []const Token,
    generated_expression: ?*const generated_parser.GeneratedSqlExpressionAst,
) !void {
    const expression = generated_expression orelse return;
    try validateGeneratedExpressionPayloads(tokens, expression.*);
    if (expression.kind != .token_range) return error.UnsupportedSqlShape;
}

pub fn validateGeneratedSimpleReturningExpression(
    tokens: []const Token,
    generated_expression: ?*const generated_parser.GeneratedSqlExpressionAst,
) !void {
    const expression = generated_expression orelse return;
    try validateGeneratedExpressionPayloads(tokens, expression.*);
    if (expression.kind != .token_range) return error.UnsupportedSqlShape;
}

pub fn validateGeneratedWindowOverClauseForSpec(
    tokens: []const Token,
    function: ?db_mod.types.RelationalRowsWindowFunction,
    over_start: usize,
    over_end: usize,
    generated_expression: ?*const generated_parser.GeneratedSqlExpressionAst,
) !void {
    const expression = generated_expression orelse return;
    const expression_tokens = expression.tokens orelse return error.UnsupportedSqlShape;
    try validateGeneratedExpressionFamilyPayloads(tokens, expression.*, expression_tokens);
    if (expression.kind != .function_call) return error.UnsupportedSqlShape;
    if (function) |expected_function| {
        const generated_function = windowFunctionForGeneratedKind(expression.window_function_kind orelse return error.UnsupportedSqlShape);
        if (generated_function != expected_function) return error.UnsupportedSqlShape;
    }
    const over_range = expression.over_tokens orelse return error.UnsupportedSqlShape;
    if (over_range.start != over_start or over_range.end != over_end or over_range.end > tokens.len) return error.UnsupportedSqlShape;
    if (over_range.start >= over_range.end or !tokens[over_range.start].matchesKeywordTag(.over)) return error.UnsupportedSqlShape;
    if (expression.over_name_tokens) |name_range| {
        if (expression.over_definition_tokens != null or
            expression.over_partition_tokens != null or
            expression.over_order_tokens != null or
            expression.over_frame_tokens != null or
            expression.over_frame_unit != null or
            expression.over_frame_start_bound != null or
            expression.over_frame_end_bound != null)
        {
            return error.UnsupportedSqlShape;
        }
        try validateGeneratedEmptyList(expression.over_partition_items);
        try validateGeneratedEmptyList(expression.over_order_items);
        if (name_range.start != over_range.start + 1 or name_range.end != over_range.end or name_range.start >= name_range.end) return error.UnsupportedSqlShape;
        return;
    }
    if (over_range.start + 2 > over_range.end or tokens[over_range.start + 1].kind != .lparen or tokens[over_range.end - 1].kind != .rparen) return error.UnsupportedSqlShape;
    const definition_range = expression.over_definition_tokens orelse {
        if (over_range.end != over_range.start + 3 or
            expression.over_partition_tokens != null or
            expression.over_order_tokens != null or
            expression.over_frame_tokens != null or
            expression.over_frame_unit != null or
            expression.over_frame_start_bound != null or
            expression.over_frame_end_bound != null)
        {
            return error.UnsupportedSqlShape;
        }
        try validateGeneratedEmptyList(expression.over_partition_items);
        try validateGeneratedEmptyList(expression.over_order_items);
        return;
    };
    if (definition_range.start != over_range.start + 2 or definition_range.end != over_range.end - 1) return error.UnsupportedSqlShape;

    if (expression.over_partition_tokens) |partition_range| {
        if (partition_range.start < definition_range.start or partition_range.end > definition_range.end) return error.UnsupportedSqlShape;
        try validateGeneratedExpressionListForClause(tokens, partition_range, expression.over_partition_items);
    } else {
        try validateGeneratedEmptyList(expression.over_partition_items);
    }

    if (expression.over_order_tokens) |order_range| {
        if (order_range.start < definition_range.start or order_range.end > definition_range.end) return error.UnsupportedSqlShape;
        try validateGeneratedOrderListForClause(tokens, order_range, expression.over_order_items);
    } else {
        try validateGeneratedEmptyList(expression.over_order_items);
    }

    if (expression.over_frame_tokens) |frame_range| {
        if (expression.over_frame_unit == null or expression.over_frame_start_bound == null or expression.over_frame_end_bound == null) return error.UnsupportedSqlShape;
        if (frame_range.start < definition_range.start or frame_range.end > definition_range.end) return error.UnsupportedSqlShape;
        try validateGeneratedWindowFrameClauseLayout(
            tokens,
            frame_range,
            expression.over_frame_start_expression_kind,
            expression.over_frame_start_expression_tokens,
            expression.over_frame_start_expression,
            expression.over_frame_end_expression_kind,
            expression.over_frame_end_expression_tokens,
            expression.over_frame_end_expression,
        );
        if (expression.over_frame_start_expression_tokens) |expression_range| {
            if (expression_range.start < frame_range.start or expression_range.end > frame_range.end) return error.UnsupportedSqlShape;
            if (!expr_generated.generatedTokenRangeEqual((expression.over_frame_start_expression orelse return error.UnsupportedSqlShape).tokens orelse return error.UnsupportedSqlShape, expression_range)) return error.UnsupportedSqlShape;
        } else if (expression.over_frame_start_expression != null or expression.over_frame_start_expression_kind != null) {
            return error.UnsupportedSqlShape;
        }
        if (expression.over_frame_end_expression_tokens) |expression_range| {
            if (expression_range.start < frame_range.start or expression_range.end > frame_range.end) return error.UnsupportedSqlShape;
            if (!expr_generated.generatedTokenRangeEqual((expression.over_frame_end_expression orelse return error.UnsupportedSqlShape).tokens orelse return error.UnsupportedSqlShape, expression_range)) return error.UnsupportedSqlShape;
        } else if (expression.over_frame_end_expression != null or expression.over_frame_end_expression_kind != null) {
            return error.UnsupportedSqlShape;
        }
    } else if (expression.over_frame_unit != null or expression.over_frame_start_bound != null or expression.over_frame_end_bound != null or
        expression.over_frame_start_expression_tokens != null or expression.over_frame_start_expression != null or expression.over_frame_start_expression_kind != null or
        expression.over_frame_end_expression_tokens != null or expression.over_frame_end_expression != null or expression.over_frame_end_expression_kind != null)
    {
        return error.UnsupportedSqlShape;
    }
}

fn validateGeneratedWindowFrameClauseLayout(
    tokens: []const Token,
    frame_range: generated_parser.GeneratedSqlTokenRange,
    start_expression_kind: ?generated_parser.GeneratedSqlExpressionKind,
    start_expression_tokens: ?generated_parser.GeneratedSqlTokenRange,
    start_expression: ?*const generated_parser.GeneratedSqlExpressionAst,
    end_expression_kind: ?generated_parser.GeneratedSqlExpressionKind,
    end_expression_tokens: ?generated_parser.GeneratedSqlTokenRange,
    end_expression: ?*const generated_parser.GeneratedSqlExpressionAst,
) !void {
    if (frame_range.start + 1 >= frame_range.end or frame_range.end > tokens.len) return error.UnsupportedSqlShape;
    if (!tokens[frame_range.start].matchesKeywordTag(.rows) and !tokens[frame_range.start].matchesKeywordTag(.range)) {
        return error.UnsupportedSqlShape;
    }

    var cursor = frame_range.start + 1;
    if (tokens[cursor].matchesKeywordTag(.between)) {
        cursor = try validateGeneratedWindowFrameBoundLayout(
            tokens,
            cursor + 1,
            frame_range.end,
            start_expression_kind,
            start_expression_tokens,
            start_expression,
        );
        if (cursor >= frame_range.end or !tokens[cursor].matchesKeywordTag(.@"and")) return error.UnsupportedSqlShape;
        cursor = try validateGeneratedWindowFrameBoundLayout(
            tokens,
            cursor + 1,
            frame_range.end,
            end_expression_kind,
            end_expression_tokens,
            end_expression,
        );
    } else {
        cursor = try validateGeneratedWindowFrameBoundLayout(
            tokens,
            cursor,
            frame_range.end,
            start_expression_kind,
            start_expression_tokens,
            start_expression,
        );
        if (end_expression_kind != null or end_expression_tokens != null or end_expression != null) return error.UnsupportedSqlShape;
    }
    if (cursor != frame_range.end) return error.UnsupportedSqlShape;
}

fn validateGeneratedWindowFrameBoundLayout(
    tokens: []const Token,
    start: usize,
    end: usize,
    expression_kind: ?generated_parser.GeneratedSqlExpressionKind,
    expression_tokens: ?generated_parser.GeneratedSqlTokenRange,
    expression: ?*const generated_parser.GeneratedSqlExpressionAst,
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
    const child = expression orelse return error.UnsupportedSqlShape;
    if (expression_kind) |expected_kind| {
        if (child.kind != expected_kind) return error.UnsupportedSqlShape;
    } else if (child.kind != .token_range) {
        return error.UnsupportedSqlShape;
    }
    if (!expr_generated.generatedTokenRangeEqual(child.tokens orelse return error.UnsupportedSqlShape, value_tokens)) return error.UnsupportedSqlShape;
    try validateGeneratedChildExpressionPayloads(tokens, value_tokens, expression_kind, expression);
    if (!tokens[value_tokens.end].matchesKeywordTag(.preceding) and !tokens[value_tokens.end].matchesKeywordTag(.following)) {
        return error.UnsupportedSqlShape;
    }
    return value_tokens.end + 1;
}

fn generatedWindowFunctionArgumentCountBounds(function: db_mod.types.RelationalRowsWindowFunction) struct { min: usize, max: usize } {
    return switch (function) {
        .row_number, .rank, .dense_rank, .percent_rank, .cume_dist => .{ .min = 0, .max = 0 },
        .ntile, .first_value, .last_value, .sum, .avg, .min, .max, .bool_or, .bool_and => .{ .min = 1, .max = 1 },
        .nth_value => .{ .min = 2, .max = 2 },
        .lag, .lead => .{ .min = 1, .max = 3 },
        .count => .{ .min = 0, .max = 1 },
    };
}

pub fn validateGeneratedWindowFunctionArgumentPayloads(
    tokens: []const Token,
    function: db_mod.types.RelationalRowsWindowFunction,
    function_start: usize,
    argument_start: usize,
    argument_end: usize,
    generated_expression: ?*const generated_parser.GeneratedSqlExpressionAst,
) !void {
    const expression = generated_expression orelse return;
    if (expression.kind != .function_call) return error.UnsupportedSqlShape;
    const expression_tokens = expression.tokens orelse return error.UnsupportedSqlShape;
    if (expression_tokens.start != function_start or expression_tokens.end > tokens.len) return error.UnsupportedSqlShape;
    const name_tokens = expression.function_name_tokens orelse return error.UnsupportedSqlShape;
    if (name_tokens.start != function_start or name_tokens.end >= expression_tokens.end) return error.UnsupportedSqlShape;
    const generated_function = windowFunctionForGeneratedKind(expression.window_function_kind orelse return error.UnsupportedSqlShape);
    if (generated_function != function) return error.UnsupportedSqlShape;
    if (expression.argument_distinct_tokens != null or
        expression.argument_order_tokens != null or
        expression.within_group_tokens != null or
        expression.within_group_order_tokens != null)
    {
        return error.UnsupportedSqlShape;
    }
    try validateGeneratedEmptyList(expression.argument_order_items);
    try validateGeneratedEmptyList(expression.within_group_order_items);

    const bounds = generatedWindowFunctionArgumentCountBounds(function);
    if (argument_start == argument_end) {
        if (bounds.min != 0) return error.UnsupportedSqlShape;
        if (expression.argument_tokens != null or expression.argument_value_tokens != null) return error.UnsupportedSqlShape;
        try validateGeneratedEmptyList(expression.argument_items);
        return;
    }

    const argument_tokens = expression.argument_tokens orelse return error.UnsupportedSqlShape;
    const value_tokens = expression.argument_value_tokens orelse return error.UnsupportedSqlShape;
    if (argument_tokens.start != argument_start or argument_tokens.end != argument_end) return error.UnsupportedSqlShape;
    if (value_tokens.start != argument_start or value_tokens.end != argument_end) return error.UnsupportedSqlShape;

    if (function == .count and argument_start + 1 == argument_end and tokens[argument_start].kind == .star) {
        if (expression.argument_items.count > 1) return error.UnsupportedSqlShape;
        if (expression.argument_items.count == 0) try validateGeneratedEmptyList(expression.argument_items);
        if (expression.argument_items.count == 1) try validateGeneratedCountStarArgumentList(tokens, expression.argument_items, value_tokens);
        return;
    }

    if (expression.argument_items.count < bounds.min or expression.argument_items.count > bounds.max) return error.UnsupportedSqlShape;
    if (expression.argument_items.count == 0) return error.UnsupportedSqlShape;
    if (expression.argument_items.items.len != expression.argument_items.count or expression.argument_items.expressions.len != expression.argument_items.count) return error.UnsupportedSqlShape;
    if (!expr_generated.generatedTokenRangeEqual(expression.argument_items.first_tokens orelse return error.UnsupportedSqlShape, expression.argument_items.items[0])) return error.UnsupportedSqlShape;
    if (!expr_generated.generatedTokenRangeEqual(expression.argument_items.last_tokens orelse return error.UnsupportedSqlShape, expression.argument_items.items[expression.argument_items.count - 1])) return error.UnsupportedSqlShape;
}

fn validateGeneratedCountStarArgumentList(
    tokens: []const Token,
    list: generated_parser.GeneratedSqlListAst,
    star_tokens: generated_parser.GeneratedSqlTokenRange,
) !void {
    if (star_tokens.end != star_tokens.start + 1 or star_tokens.end > tokens.len or tokens[star_tokens.start].kind != .star) return error.UnsupportedSqlShape;
    if (list.count != 1 or
        list.items.len != 1 or
        list.expression_items.len != 1 or
        list.expressions.len != 1 or
        list.alias_items.len != 1 or
        list.alias_name_items.len != 1 or
        list.direction_items.len != 1 or
        list.directions.len != 1 or
        list.order_using_operator_items.len != 1 or
        list.nulls_order_items.len != 1 or
        list.nulls_orders.len != 1)
    {
        return error.UnsupportedSqlShape;
    }
    if (!expr_generated.generatedTokenRangeEqual(list.items[0], star_tokens) or
        !expr_generated.generatedTokenRangeEqual(list.expression_items[0], star_tokens) or
        !expr_generated.generatedTokenRangeEqual(list.first_tokens orelse return error.UnsupportedSqlShape, star_tokens) or
        !expr_generated.generatedTokenRangeEqual(list.last_tokens orelse return error.UnsupportedSqlShape, star_tokens) or
        list.alias_items[0] != null or
        list.alias_name_items[0] != null or
        list.direction_items[0] != null or
        list.directions[0] != null or
        list.order_using_operator_items[0] != null or
        list.nulls_order_items[0] != null or
        list.nulls_orders[0] != null)
    {
        return error.UnsupportedSqlShape;
    }
    if (!expr_generated.generatedTokenRangeEqual(list.expressions[0].tokens orelse return error.UnsupportedSqlShape, star_tokens)) return error.UnsupportedSqlShape;
    try validateGeneratedExpressionPayloads(tokens, list.expressions[0]);
}

pub fn generatedWhereClauseEnd(
    tokens: []const Token,
    keyword_index: usize,
    pos: usize,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
    use_set_operation_right_side: bool,
) !?usize {
    const read = generated_read_ast orelse return null;
    if (keyword_index >= tokens.len or !tokens[keyword_index].matchesKeywordTag(.where)) return null;
    if (use_set_operation_right_side) {
        if (read.set_operation_tokens == null) return error.UnsupportedSqlShape;
        if (read.set_operation.right_where_tokens) |right_range| {
            if (right_range.start == pos) {
                if (right_range.end > tokens.len) return error.UnsupportedSqlShape;
                if (!expr_generated.generatedTokenRangeEqual(read.set_operation.right_where_expression.tokens orelse return error.UnsupportedSqlShape, right_range)) return error.UnsupportedSqlShape;
                try validateGeneratedExpressionPayloads(tokens, read.set_operation.right_where_expression);
                return right_range.end;
            }
            if (right_range.end <= pos) return null;
        }
        if (try generatedSetOperationRightQueryEndedBefore(read, pos)) return null;
        return error.UnsupportedSqlShape;
    }
    if (read.where_tokens) |range| {
        if (range.start == pos) {
            if (range.end > tokens.len) return error.UnsupportedSqlShape;
            if (!expr_generated.generatedTokenRangeEqual(read.where_expression.tokens orelse return error.UnsupportedSqlShape, range)) return error.UnsupportedSqlShape;
            try validateGeneratedExpressionPayloads(tokens, read.where_expression);
            return range.end;
        }
    }
    if (read.kind == .set_operation) {
        if (read.set_operation.right_where_tokens) |right_range| {
            if (right_range.start == pos) {
                if (right_range.end > tokens.len) return error.UnsupportedSqlShape;
                if (!expr_generated.generatedTokenRangeEqual(read.set_operation.right_where_expression.tokens orelse return error.UnsupportedSqlShape, right_range)) return error.UnsupportedSqlShape;
                try validateGeneratedExpressionPayloads(tokens, read.set_operation.right_where_expression);
                return right_range.end;
            }
        }
        return null;
    }
    return error.UnsupportedSqlShape;
}

pub fn generatedWhereExpressionForClause(
    pos: usize,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
    use_set_operation_right_side: bool,
) !?*const generated_parser.GeneratedSqlExpressionAst {
    const read = generated_read_ast orelse return null;
    if (use_set_operation_right_side) {
        if (read.set_operation_tokens == null) return error.UnsupportedSqlShape;
        if (read.set_operation.right_where_tokens) |right_range| {
            if (right_range.start == pos) return &read.set_operation.right_where_expression;
        }
        return null;
    }
    if (read.where_tokens) |range| {
        if (range.start == pos) return &read.where_expression;
    }
    if (read.kind == .set_operation) {
        if (read.set_operation.right_where_tokens) |right_range| {
            if (right_range.start == pos) return &read.set_operation.right_where_expression;
        }
    }
    return null;
}

pub fn requireGeneratedWhereExpressionForClause(
    pos: usize,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
    use_set_operation_right_side: bool,
    generated_where_end: ?usize,
) !?*const generated_parser.GeneratedSqlExpressionAst {
    const expression = try generatedWhereExpressionForClause(pos, generated_read_ast, use_set_operation_right_side);
    if (generated_where_end != null and expression == null) return error.UnsupportedSqlShape;
    return expression;
}

pub fn parseGeneratedLimitValueForClause(
    tokens: []const Token,
    keyword_index: usize,
    pos: *usize,
    params: []const value_mod.SqlValue,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
) !?GeneratedLimitValue {
    const read = generated_read_ast orelse return null;
    if (keyword_index >= tokens.len or !tokens[keyword_index].matchesKeywordTag(.limit)) return null;
    const range = read.limit_tokens orelse return error.UnsupportedSqlShape;
    if (range.start != pos.* or range.end > tokens.len) return error.UnsupportedSqlShape;
    if (read.limit_all) {
        try validateGeneratedLimitAllRangeLayout(tokens, range);
        try validateGeneratedOptionalExpression(tokens, null, read.limit_expression);
        pos.* = range.end;
        return .{ .value = null };
    }
    const limit = try parseGeneratedPaginationNullableU32Expression(tokens, read.limit_expression, range, params);
    pos.* = range.end;
    return .{ .value = limit };
}

pub fn parseGeneratedOffsetValueForClause(
    tokens: []const Token,
    keyword_index: usize,
    pos: *usize,
    params: []const value_mod.SqlValue,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
) !?u32 {
    const read = generated_read_ast orelse return null;
    if (keyword_index >= tokens.len or !tokens[keyword_index].matchesKeywordTag(.offset)) return null;
    const range = read.offset_tokens orelse return error.UnsupportedSqlShape;
    if (range.start != pos.* or range.end > tokens.len) return error.UnsupportedSqlShape;
    const expression_range = read.offset_expression.tokens orelse return error.UnsupportedSqlShape;
    if (expression_range.start != range.start or expression_range.end > range.end) return error.UnsupportedSqlShape;
    if (expression_range.end != range.end) {
        if (expression_range.end + 1 != range.end) return error.UnsupportedSqlShape;
        if (!tokens[expression_range.end].matchesKeywordTag(.row) and !tokens[expression_range.end].matchesKeywordTag(.rows)) return error.UnsupportedSqlShape;
    }
    const offset = (try parseGeneratedPaginationNullableU32Expression(tokens, read.offset_expression, expression_range, params)) orelse 0;
    pos.* = range.end;
    return offset;
}

pub fn parseGeneratedFetchLimitValueForClause(
    tokens: []const Token,
    keyword_index: usize,
    pos: *usize,
    params: []const value_mod.SqlValue,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
) !?GeneratedLimitValue {
    const read = generated_read_ast orelse return null;
    if (keyword_index >= tokens.len or !tokens[keyword_index].matchesKeywordTag(.fetch)) return null;
    const range = read.fetch_tokens orelse return error.UnsupportedSqlShape;
    if (range.start != pos.* or range.end > tokens.len) return error.UnsupportedSqlShape;
    try validateGeneratedFetchRangeLayout(tokens, range, read.fetch_count_tokens);
    const limit = if (read.fetch_count_tokens) |count_range| blk: {
        const expression_range = read.fetch_count_expression.tokens orelse return error.UnsupportedSqlShape;
        if (expression_range.start != count_range.start or expression_range.end != count_range.end) return error.UnsupportedSqlShape;
        const generated_limit = try parseGeneratedPaginationNullableU32Expression(tokens, read.fetch_count_expression, expression_range, params);
        break :blk generated_limit;
    } else blk: {
        try validateGeneratedOptionalExpression(tokens, null, read.fetch_count_expression);
        break :blk @as(?u32, 1);
    };
    pos.* = range.end;
    return .{ .value = limit };
}

pub fn parseLimitValueForClause(
    tokens: []const Token,
    keyword_index: usize,
    pos: *usize,
    params: []const value_mod.SqlValue,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
) !GeneratedLimitValue {
    if (try parseGeneratedLimitValueForClause(tokens, keyword_index, pos, params, generated_read_ast)) |generated_limit| return generated_limit;
    if (generated_read_ast != null) return error.UnsupportedSqlShape;
    return .{ .value = try value_mod.parseLimitValue(tokens, pos, params) };
}

pub fn parseOffsetValueForClause(
    tokens: []const Token,
    keyword_index: usize,
    pos: *usize,
    params: []const value_mod.SqlValue,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
) !u32 {
    if (try parseGeneratedOffsetValueForClause(tokens, keyword_index, pos, params, generated_read_ast)) |generated_offset| return generated_offset;
    if (generated_read_ast != null) return error.UnsupportedSqlShape;
    return try value_mod.parseOffsetValue(tokens, pos, params);
}

pub fn parseFetchLimitValueForClause(
    tokens: []const Token,
    keyword_index: usize,
    pos: *usize,
    params: []const value_mod.SqlValue,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
) !GeneratedLimitValue {
    if (try parseGeneratedFetchLimitValueForClause(tokens, keyword_index, pos, params, generated_read_ast)) |generated_limit| return generated_limit;
    if (generated_read_ast != null) return error.UnsupportedSqlShape;
    return .{ .value = try value_mod.parseFetchLimitValue(tokens, pos, params) };
}

pub fn validateGeneratedSingleJoinForClause(
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
    expected_read_kind: generated_parser.GeneratedSqlReadKind,
    tokens: []const Token,
    left_tokens: generated_parser.GeneratedSqlTokenRange,
    operator_tokens: generated_parser.GeneratedSqlTokenRange,
    join_type: db_mod.types.RelationalRowsJoinType,
    right_tokens: generated_parser.GeneratedSqlTokenRange,
    condition_tokens: generated_parser.GeneratedSqlTokenRange,
    predicate_tokens: generated_parser.GeneratedSqlTokenRange,
) !void {
    const expected_kind: generated_parser.GeneratedSqlJoinKind = switch (join_type) {
        .inner => .inner,
        .left => .left,
        .full => .full,
    };
    try validateGeneratedSingleJoinForClauseKind(
        generated_read_ast,
        expected_read_kind,
        tokens,
        left_tokens,
        operator_tokens,
        expected_kind,
        right_tokens,
        condition_tokens,
        predicate_tokens,
    );
}

pub fn validateGeneratedSingleJoinForClauseKind(
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
    expected_read_kind: generated_parser.GeneratedSqlReadKind,
    tokens: []const Token,
    left_tokens: generated_parser.GeneratedSqlTokenRange,
    operator_tokens: generated_parser.GeneratedSqlTokenRange,
    expected_kind: generated_parser.GeneratedSqlJoinKind,
    right_tokens: generated_parser.GeneratedSqlTokenRange,
    condition_tokens: generated_parser.GeneratedSqlTokenRange,
    predicate_tokens: generated_parser.GeneratedSqlTokenRange,
) !void {
    const read = generated_read_ast orelse return;
    if (read.kind != expected_read_kind and read.kind != .cte) return error.UnsupportedSqlShape;
    try validateGeneratedJoinItemsMetadata(tokens, read.*);
    const root_index = read.join_tree_root_index orelse return error.UnsupportedSqlShape;
    if (read.join_items.len != 1 or root_index != 0 or read.join_tree_depth != 1) return error.UnsupportedSqlShape;
    const join = read.join_items[0];
    if (join.tree_index != 0 or join.tree_depth != 1 or join.left_child_index != null) return error.UnsupportedSqlShape;
    if (join.condition_kind != .on) return error.UnsupportedSqlShape;
    if (join.tokens.start != left_tokens.start or join.tokens.end != predicate_tokens.end) return error.UnsupportedSqlShape;
    if (join.left_tokens.start != left_tokens.start or join.left_tokens.end != left_tokens.end) return error.UnsupportedSqlShape;
    if (join.operator_tokens.start != operator_tokens.start or join.operator_tokens.end != operator_tokens.end) return error.UnsupportedSqlShape;
    if (join.right_tokens.start != right_tokens.start or join.right_tokens.end != right_tokens.end) return error.UnsupportedSqlShape;
    if (join.condition_tokens.start != condition_tokens.start or join.condition_tokens.end != condition_tokens.end) return error.UnsupportedSqlShape;
    const join_predicate_tokens = join.predicate_tokens orelse return error.UnsupportedSqlShape;
    if (join_predicate_tokens.start != predicate_tokens.start or join_predicate_tokens.end != predicate_tokens.end) return error.UnsupportedSqlShape;
    if (join.kind != expected_kind) return error.UnsupportedSqlShape;
    if (join.tokens.end > tokens.len) return error.UnsupportedSqlShape;
}

pub fn validateGeneratedSingleJoinUsingForClause(
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
    expected_read_kind: generated_parser.GeneratedSqlReadKind,
    tokens: []const Token,
    left_tokens: generated_parser.GeneratedSqlTokenRange,
    operator_tokens: generated_parser.GeneratedSqlTokenRange,
    join_type: db_mod.types.RelationalRowsJoinType,
    right_tokens: generated_parser.GeneratedSqlTokenRange,
    using_tokens: generated_parser.GeneratedSqlTokenRange,
    column_tokens: generated_parser.GeneratedSqlTokenRange,
) !void {
    const expected_kind: generated_parser.GeneratedSqlJoinKind = switch (join_type) {
        .inner => .inner,
        .left => .left,
        .full => .full,
    };
    try validateGeneratedSingleJoinUsingForClauseKind(
        generated_read_ast,
        expected_read_kind,
        tokens,
        left_tokens,
        operator_tokens,
        expected_kind,
        right_tokens,
        using_tokens,
        column_tokens,
    );
}

pub fn validateGeneratedSingleJoinUsingForClauseKind(
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
    expected_read_kind: generated_parser.GeneratedSqlReadKind,
    tokens: []const Token,
    left_tokens: generated_parser.GeneratedSqlTokenRange,
    operator_tokens: generated_parser.GeneratedSqlTokenRange,
    expected_kind: generated_parser.GeneratedSqlJoinKind,
    right_tokens: generated_parser.GeneratedSqlTokenRange,
    using_tokens: generated_parser.GeneratedSqlTokenRange,
    column_tokens: generated_parser.GeneratedSqlTokenRange,
) !void {
    const read = generated_read_ast orelse return;
    if (read.kind != expected_read_kind and read.kind != .cte) return error.UnsupportedSqlShape;
    try validateGeneratedJoinItemsMetadata(tokens, read.*);
    const root_index = read.join_tree_root_index orelse return error.UnsupportedSqlShape;
    if (read.join_items.len != 1 or root_index != 0 or read.join_tree_depth != 1) return error.UnsupportedSqlShape;
    const join = read.join_items[0];
    if (join.tree_index != 0 or join.tree_depth != 1 or join.left_child_index != null) return error.UnsupportedSqlShape;
    if (join.condition_kind != .using) return error.UnsupportedSqlShape;
    if (join.tokens.start != left_tokens.start or join.tokens.end != using_tokens.end) return error.UnsupportedSqlShape;
    if (join.left_tokens.start != left_tokens.start or join.left_tokens.end != left_tokens.end) return error.UnsupportedSqlShape;
    if (join.operator_tokens.start != operator_tokens.start or join.operator_tokens.end != operator_tokens.end) return error.UnsupportedSqlShape;
    if (join.right_tokens.start != right_tokens.start or join.right_tokens.end != right_tokens.end) return error.UnsupportedSqlShape;
    if (join.condition_tokens.start != using_tokens.start or join.condition_tokens.end != using_tokens.end) return error.UnsupportedSqlShape;
    const generated_using_tokens = join.using_tokens orelse return error.UnsupportedSqlShape;
    const generated_column_tokens = join.using_column_tokens orelse return error.UnsupportedSqlShape;
    if (generated_using_tokens.start != using_tokens.start or generated_using_tokens.end != using_tokens.end) return error.UnsupportedSqlShape;
    if (generated_column_tokens.start != column_tokens.start or generated_column_tokens.end != column_tokens.end) return error.UnsupportedSqlShape;
    if (join.using_columns.count == 0) return error.UnsupportedSqlShape;
    if (join.kind != expected_kind) return error.UnsupportedSqlShape;
    if (join.tokens.end > tokens.len) return error.UnsupportedSqlShape;
}

pub fn generatedJoinUsingColumnsAlloc(
    alloc: std.mem.Allocator,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
    expected_read_kind: generated_parser.GeneratedSqlReadKind,
    tokens: []const Token,
    pos: *usize,
) !?[]const []const u8 {
    const read = generated_read_ast orelse return null;
    if (read.kind != expected_read_kind and read.kind != .cte) return error.UnsupportedSqlShape;
    try validateGeneratedJoinItemsMetadata(tokens, read.*);
    const root_index = read.join_tree_root_index orelse return error.UnsupportedSqlShape;
    if (read.join_items.len != 1 or root_index != 0 or read.join_tree_depth != 1) return error.UnsupportedSqlShape;
    const join = read.join_items[0];
    if (join.condition_kind != .using) return null;
    const column_tokens = join.using_column_tokens orelse return error.UnsupportedSqlShape;
    if (column_tokens.start != pos.*) return error.UnsupportedSqlShape;
    if (join.using_columns.count == 0 or join.using_columns.items.len != join.using_columns.count) return error.UnsupportedSqlShape;

    const columns = try alloc.alloc([]const u8, join.using_columns.count);
    var initialized: usize = 0;
    errdefer {
        for (columns[0..initialized]) |column| alloc.free(column);
        alloc.free(columns);
    }
    for (join.using_columns.items, 0..) |item, index| {
        if (item.start < column_tokens.start or item.end > column_tokens.end or item.end != item.start + 1) return error.UnsupportedSqlShape;
        if (tokens[item.start].kind != .identifier) return error.UnsupportedSqlShape;
        columns[index] = try alloc.dupe(u8, tokens[item.start].text);
        initialized += 1;
    }
    pos.* = column_tokens.end;
    return columns;
}

pub fn generatedJoinProjectionListAlloc(
    alloc: std.mem.Allocator,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
    tokens: []const Token,
    pos: *usize,
) !?[]const plan_mod.QualifiedProjection {
    const read = generated_read_ast orelse return null;
    if (read.kind != .join and read.kind != .lateral and read.kind != .cte) return error.UnsupportedSqlShape;
    const projection_tokens = read.projection_tokens orelse return error.UnsupportedSqlShape;
    if (projection_tokens.start != pos.*) return error.UnsupportedSqlShape;
    const items = read.projection_items;
    if (items.count == 0 or
        items.items.len != items.count or
        items.expression_items.len != items.count or
        items.alias_items.len != items.count or
        items.alias_name_items.len != items.count)
    {
        return error.UnsupportedSqlShape;
    }

    const projections = try alloc.alloc(plan_mod.QualifiedProjection, items.count);
    var initialized: usize = 0;
    errdefer {
        for (projections[0..initialized]) |projection| {
            plan_mod.freeQualifiedField(alloc, projection.source);
            alloc.free(projection.output);
        }
        alloc.free(projections);
    }

    for (items.items, 0..) |item, index| {
        if (item.start < projection_tokens.start or item.end > projection_tokens.end or item.start >= item.end) return error.UnsupportedSqlShape;
        const expression_item = items.expression_items[index];
        if (expression_item.start != item.start or expression_item.end > item.end or expression_item.end != expression_item.start + 1) return error.UnsupportedSqlShape;
        const source_token = tokens[expression_item.start];
        if (source_token.kind != .identifier) return error.UnsupportedSqlShape;
        const dot = std.mem.indexOfScalar(u8, source_token.text, '.') orelse return error.UnsupportedSqlShape;
        if (dot == 0 or dot + 1 >= source_token.text.len) return error.UnsupportedSqlShape;
        const field_text = source_token.text[dot + 1 ..];
        if (std.mem.indexOfScalar(u8, field_text, '.') != null) return error.UnsupportedSqlShape;

        const qualifier = try alloc.dupe(u8, source_token.text[0..dot]);
        var qualifier_transferred = false;
        errdefer if (!qualifier_transferred) alloc.free(qualifier);
        const field = try alloc.dupe(u8, field_text);
        var field_transferred = false;
        errdefer if (!field_transferred) alloc.free(field);

        const output = if (items.alias_items[index]) |alias_item| output: {
            const alias_name = items.alias_name_items[index] orelse return error.UnsupportedSqlShape;
            if (alias_item.start != expression_item.end or alias_item.end != item.end) return error.UnsupportedSqlShape;
            if (alias_item.start + 2 != alias_item.end or !tokens[alias_item.start].matchesKeywordTag(.as)) return error.UnsupportedSqlShape;
            if (alias_name.start != alias_item.start + 1 or alias_name.end != alias_item.end or tokens[alias_name.start].kind != .identifier) return error.UnsupportedSqlShape;
            break :output try alloc.dupe(u8, tokens[alias_name.start].text);
        } else output: {
            if (items.alias_name_items[index] != null) return error.UnsupportedSqlShape;
            if (expression_item.end != item.end) return error.UnsupportedSqlShape;
            break :output try alloc.dupe(u8, field_text);
        };
        var output_transferred = false;
        errdefer if (!output_transferred) alloc.free(output);

        qualifier_transferred = true;
        field_transferred = true;
        output_transferred = true;
        projections[index] = .{
            .source = .{ .qualifier = qualifier, .field = field },
            .output = output,
        };
        initialized += 1;
    }
    pos.* = projection_tokens.end;
    return projections;
}

pub fn validateGeneratedSingleConditionlessJoinForClause(
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
    expected_read_kind: generated_parser.GeneratedSqlReadKind,
    tokens: []const Token,
    left_tokens: generated_parser.GeneratedSqlTokenRange,
    operator_tokens: generated_parser.GeneratedSqlTokenRange,
    expected_kind: generated_parser.GeneratedSqlJoinKind,
    right_tokens: generated_parser.GeneratedSqlTokenRange,
) !void {
    const read = generated_read_ast orelse return;
    if (read.kind != expected_read_kind and read.kind != .cte) return error.UnsupportedSqlShape;
    try validateGeneratedJoinItemsMetadata(tokens, read.*);
    const root_index = read.join_tree_root_index orelse return error.UnsupportedSqlShape;
    if (read.join_items.len != 1 or root_index != 0 or read.join_tree_depth != 1) return error.UnsupportedSqlShape;
    const join = read.join_items[0];
    if (join.tree_index != 0 or join.tree_depth != 1 or join.left_child_index != null) return error.UnsupportedSqlShape;
    if (join.kind != expected_kind or join.condition_kind != .none) return error.UnsupportedSqlShape;
    if (join.tokens.start != left_tokens.start or join.tokens.end != right_tokens.end) return error.UnsupportedSqlShape;
    if (join.left_tokens.start != left_tokens.start or join.left_tokens.end != left_tokens.end) return error.UnsupportedSqlShape;
    if (join.operator_tokens.start != operator_tokens.start or join.operator_tokens.end != operator_tokens.end) return error.UnsupportedSqlShape;
    if (join.right_tokens.start != right_tokens.start or join.right_tokens.end != right_tokens.end) return error.UnsupportedSqlShape;
    if (join.condition_tokens.start != right_tokens.end or join.condition_tokens.end != right_tokens.end) return error.UnsupportedSqlShape;
    if (join.tokens.end > tokens.len) return error.UnsupportedSqlShape;
}

pub fn generatedSingleJoinPredicateExpression(
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
) !?*const generated_parser.GeneratedSqlExpressionAst {
    const read = generated_read_ast orelse return null;
    if (read.kind != .join and read.kind != .cte) return error.UnsupportedSqlShape;
    const root_index = read.join_tree_root_index orelse return error.UnsupportedSqlShape;
    if (read.join_items.len != 1 or root_index != 0 or read.join_tree_depth != 1) return error.UnsupportedSqlShape;
    const join = read.join_items[0];
    if (join.condition_kind != .on) return error.UnsupportedSqlShape;
    const predicate_tokens = join.predicate_tokens orelse return error.UnsupportedSqlShape;
    const predicate_expression_tokens = join.predicate_expression.tokens orelse return null;
    if (!expr_generated.generatedTokenRangeEqual(predicate_expression_tokens, predicate_tokens)) return error.UnsupportedSqlShape;
    return &join.predicate_expression;
}

fn validateGeneratedJoinKindForOperator(tokens: []const Token, operator_tokens: generated_parser.GeneratedSqlTokenRange, kind: generated_parser.GeneratedSqlJoinKind) !void {
    if (operator_tokens.start >= operator_tokens.end or operator_tokens.end > tokens.len) return error.UnsupportedSqlShape;
    switch (kind) {
        .inner => {
            if (operator_tokens.end == operator_tokens.start + 1) {
                if (!tokens[operator_tokens.start].matchesKeywordTag(.join)) return error.UnsupportedSqlShape;
            } else if (operator_tokens.end == operator_tokens.start + 2) {
                if (!tokens[operator_tokens.start].matchesKeywordTag(.inner) or !tokens[operator_tokens.start + 1].matchesKeywordTag(.join)) return error.UnsupportedSqlShape;
            } else {
                return error.UnsupportedSqlShape;
            }
        },
        .left, .right, .full => {
            const expected_keyword: TokenKeyword = switch (kind) {
                .left => .left,
                .right => .right,
                .full => .full,
                else => unreachable,
            };
            if (!tokens[operator_tokens.start].matchesKeywordTag(expected_keyword)) return error.UnsupportedSqlShape;
            if (operator_tokens.end == operator_tokens.start + 2) {
                if (!tokens[operator_tokens.start + 1].matchesKeywordTag(.join)) return error.UnsupportedSqlShape;
            } else if (operator_tokens.end == operator_tokens.start + 3) {
                if (!tokens[operator_tokens.start + 1].matchesKeywordTag(.outer) or !tokens[operator_tokens.start + 2].matchesKeywordTag(.join)) return error.UnsupportedSqlShape;
            } else {
                return error.UnsupportedSqlShape;
            }
        },
        .cross, .natural => {
            const expected_keyword: TokenKeyword = switch (kind) {
                .cross => .cross,
                .natural => .natural,
                else => unreachable,
            };
            if (operator_tokens.end != operator_tokens.start + 2) return error.UnsupportedSqlShape;
            if (!tokens[operator_tokens.start].matchesKeywordTag(expected_keyword) or !tokens[operator_tokens.start + 1].matchesKeywordTag(.join)) return error.UnsupportedSqlShape;
        },
    }
}

fn validateGeneratedJoinItemsMetadata(tokens: []const Token, read: generated_parser.GeneratedSqlReadAst) !void {
    const join_tokens = read.join_tokens orelse return error.UnsupportedSqlShape;
    if (join_tokens.start >= join_tokens.end or join_tokens.end > tokens.len) return error.UnsupportedSqlShape;
    if (read.join_items.len == 0) return error.UnsupportedSqlShape;
    const root_index = read.join_tree_root_index orelse return error.UnsupportedSqlShape;
    if (root_index != read.join_items.len - 1 or read.join_tree_depth != read.join_items.len) return error.UnsupportedSqlShape;
    for (read.join_items, 0..) |join, index| {
        if (join.tree_index != index or join.tree_depth != index + 1) return error.UnsupportedSqlShape;
        if (index == 0) {
            if (join.left_child_index != null) return error.UnsupportedSqlShape;
            if (join.tokens.start != join_tokens.start) return error.UnsupportedSqlShape;
        } else {
            if (join.left_child_index == null or join.left_child_index.? != index - 1) return error.UnsupportedSqlShape;
            if (!expr_generated.generatedTokenRangeEqual(join.left_tokens, read.join_items[index - 1].tokens)) return error.UnsupportedSqlShape;
            if (join.tokens.start != join_tokens.start) return error.UnsupportedSqlShape;
        }
        if (join.tokens.end > join_tokens.end or join.tokens.start >= join.tokens.end) return error.UnsupportedSqlShape;
        if (join.left_tokens.start != join.tokens.start or join.left_tokens.end != join.operator_tokens.start) return error.UnsupportedSqlShape;
        if (join.right_tokens.start < join.operator_tokens.end or join.right_tokens.end > join.condition_tokens.start or join.right_tokens.start >= join.right_tokens.end) return error.UnsupportedSqlShape;
        try validateGeneratedJoinLateralSubqueryMetadata(tokens, join);
        try validateGeneratedJoinSideSourceMetadata(
            tokens,
            join.left_tokens,
            join.left_table_tokens,
            join.left_alias_tokens,
            join.left_alias_name_tokens,
        );
        try validateGeneratedJoinSideSourceMetadata(
            tokens,
            join.right_tokens,
            join.right_table_tokens,
            join.right_alias_tokens,
            join.right_alias_name_tokens,
        );
        try validateGeneratedJoinKindForOperator(tokens, join.operator_tokens, join.kind);
        switch (join.condition_kind) {
            .none => {
                if (join.predicate_tokens != null or join.predicate_expression.tokens != null) return error.UnsupportedSqlShape;
                if (join.using_tokens != null or join.using_column_tokens != null) return error.UnsupportedSqlShape;
                try validateGeneratedEmptyList(join.using_columns);
                if (join.kind != .cross and join.kind != .natural) return error.UnsupportedSqlShape;
                if (join.condition_tokens.start != join.tokens.end or join.condition_tokens.end != join.tokens.end) return error.UnsupportedSqlShape;
            },
            .on => {
                if (join.using_tokens != null or join.using_column_tokens != null) return error.UnsupportedSqlShape;
                try validateGeneratedEmptyList(join.using_columns);
                const predicate_tokens = join.predicate_tokens orelse return error.UnsupportedSqlShape;
                if (join.condition_tokens.start + 1 != predicate_tokens.start or join.condition_tokens.end != predicate_tokens.end) return error.UnsupportedSqlShape;
                if (!tokens[join.condition_tokens.start].matchesKeywordTag(.on)) return error.UnsupportedSqlShape;
                if (!expr_generated.generatedTokenRangeEqual(join.predicate_expression.tokens orelse return error.UnsupportedSqlShape, predicate_tokens)) return error.UnsupportedSqlShape;
                try validateGeneratedExpressionPayloads(tokens, join.predicate_expression);
            },
            .using => {
                if (join.predicate_tokens != null or join.predicate_expression.tokens != null) return error.UnsupportedSqlShape;
                const using_tokens = join.using_tokens orelse return error.UnsupportedSqlShape;
                const column_tokens = join.using_column_tokens orelse return error.UnsupportedSqlShape;
                if (!expr_generated.generatedTokenRangeEqual(using_tokens, join.condition_tokens)) return error.UnsupportedSqlShape;
                if (using_tokens.start + 3 > using_tokens.end or !tokens[using_tokens.start].matchesKeywordTag(.using)) return error.UnsupportedSqlShape;
                if (tokens[using_tokens.start + 1].kind != .lparen or tokens[using_tokens.end - 1].kind != .rparen) return error.UnsupportedSqlShape;
                if (column_tokens.start != using_tokens.start + 2 or column_tokens.end != using_tokens.end - 1) return error.UnsupportedSqlShape;
                try validateGeneratedExpressionListForClause(tokens, column_tokens, join.using_columns);
            },
        }
        if (join.condition_tokens.end != join.tokens.end) return error.UnsupportedSqlShape;
    }
    const first = read.join_items[0];
    if (read.join_operator_tokens == null or !expr_generated.generatedTokenRangeEqual(read.join_operator_tokens.?, first.operator_tokens)) return error.UnsupportedSqlShape;
    if (read.join_kind == null or read.join_kind.? != first.kind) return error.UnsupportedSqlShape;
    if (read.join_left_tokens == null or !expr_generated.generatedTokenRangeEqual(read.join_left_tokens.?, first.left_tokens)) return error.UnsupportedSqlShape;
    if (read.join_right_tokens == null or !expr_generated.generatedTokenRangeEqual(read.join_right_tokens.?, first.right_tokens)) return error.UnsupportedSqlShape;
    if (first.predicate_tokens) |predicate_tokens| {
        if (read.join_predicate_tokens == null or !expr_generated.generatedTokenRangeEqual(read.join_predicate_tokens.?, predicate_tokens)) return error.UnsupportedSqlShape;
    } else if (read.join_predicate_tokens != null) {
        return error.UnsupportedSqlShape;
    }
}

fn validateGeneratedJoinLateralSubqueryMetadata(
    tokens: []const Token,
    join: generated_parser.GeneratedSqlJoinAst,
) anyerror!void {
    if (join.right_tokens.start >= join.right_tokens.end or join.right_tokens.end > tokens.len) return error.UnsupportedSqlShape;
    const operator_has_lateral = generatedTokenRangeContainsKeyword(tokens, join.operator_tokens, .lateral);
    const right_starts_lateral = tokens[join.right_tokens.start].matchesKeywordTag(.lateral);
    if (!right_starts_lateral and !operator_has_lateral) {
        if (join.right_lateral_subquery_tokens != null or
            join.right_lateral_subquery_read_ast != null or
            join.right_lateral_alias_tokens != null or
            join.right_lateral_alias_name_tokens != null)
        {
            return error.UnsupportedSqlShape;
        }
        return;
    }
    if (generatedJoinLooksLikeLateralTableFunction(tokens, join)) {
        return try validateGeneratedJoinLateralTableFunctionMetadata(tokens, join);
    }
    if (!right_starts_lateral) return error.UnsupportedSqlShape;
    const subquery_tokens = join.right_lateral_subquery_tokens orelse return error.UnsupportedSqlShape;
    const subquery_read = join.right_lateral_subquery_read_ast orelse return error.UnsupportedSqlShape;
    if (join.right_tokens.start + 2 != subquery_tokens.start) return error.UnsupportedSqlShape;
    if (subquery_tokens.start >= subquery_tokens.end or subquery_tokens.end >= join.right_tokens.end) return error.UnsupportedSqlShape;
    if (tokens[join.right_tokens.start + 1].kind != .lparen) return error.UnsupportedSqlShape;
    if (tokens[subquery_tokens.end].kind != .rparen) return error.UnsupportedSqlShape;
    try validateGeneratedLateralSubqueryAlias(
        tokens,
        subquery_tokens.end + 1,
        join.right_tokens.end,
        join.right_lateral_alias_tokens,
        join.right_lateral_alias_name_tokens,
    );
    try validateGeneratedReadAstPayloads(tokens[subquery_tokens.start..subquery_tokens.end], subquery_read.*);
}

fn generatedTokenRangeContainsKeyword(
    tokens: []const Token,
    range: generated_parser.GeneratedSqlTokenRange,
    keyword: TokenKeyword,
) bool {
    if (range.end > tokens.len or range.start > range.end) return false;
    var index = range.start;
    while (index < range.end) : (index += 1) {
        if (tokens[index].matchesKeywordTag(keyword)) return true;
    }
    return false;
}

fn generatedJoinLooksLikeLateralTableFunction(
    tokens: []const Token,
    join: generated_parser.GeneratedSqlJoinAst,
) bool {
    if (join.right_tokens.end > tokens.len or join.right_tokens.start >= join.right_tokens.end) return false;
    var function_start = join.right_tokens.start;
    if (tokens[function_start].matchesKeywordTag(.lateral)) function_start += 1;
    if (function_start + 1 >= join.right_tokens.end) return false;
    return tokens[function_start].kind == .identifier and
        tokens[function_start + 1].kind == .lparen;
}

fn validateGeneratedJoinLateralTableFunctionMetadata(
    tokens: []const Token,
    join: generated_parser.GeneratedSqlJoinAst,
) !void {
    if (join.right_lateral_subquery_tokens != null or
        join.right_lateral_subquery_read_ast != null or
        join.right_lateral_alias_tokens != null or
        join.right_lateral_alias_name_tokens != null)
    {
        return error.UnsupportedSqlShape;
    }
    if (!generatedTokenRangeContainsKeyword(tokens, join.operator_tokens, .lateral) and
        !tokens[join.right_tokens.start].matchesKeywordTag(.lateral)) return error.UnsupportedSqlShape;
    if (join.right_tokens.end > tokens.len or join.right_tokens.start >= join.right_tokens.end) return error.UnsupportedSqlShape;
    var function_start = join.right_tokens.start;
    if (tokens[function_start].matchesKeywordTag(.lateral)) function_start += 1;
    if (function_start + 1 >= join.right_tokens.end) return error.UnsupportedSqlShape;
    if (tokens[function_start].kind != .identifier) return error.UnsupportedSqlShape;
    const lparen_index = function_start + 1;
    if (tokens[lparen_index].kind != .lparen) return error.UnsupportedSqlShape;
    const rparen_index = generatedMatchingParenInRange(tokens, lparen_index, join.right_tokens.end) orelse return error.UnsupportedSqlShape;
    const alias_start = rparen_index + 1;
    if (alias_start == join.right_tokens.end) return;
    if (alias_start + 1 == join.right_tokens.end) {
        if (tokens[alias_start].kind != .identifier) return error.UnsupportedSqlShape;
        return;
    }
    if (alias_start + 2 != join.right_tokens.end or
        !tokens[alias_start].matchesKeywordTag(.as) or
        tokens[alias_start + 1].kind != .identifier)
    {
        return error.UnsupportedSqlShape;
    }
}

fn validateGeneratedLateralSubqueryAlias(
    tokens: []const Token,
    start: usize,
    end: usize,
    maybe_alias_tokens: ?generated_parser.GeneratedSqlTokenRange,
    maybe_alias_name_tokens: ?generated_parser.GeneratedSqlTokenRange,
) !void {
    const alias_tokens = maybe_alias_tokens orelse return error.UnsupportedSqlShape;
    const alias_name_tokens = maybe_alias_name_tokens orelse return error.UnsupportedSqlShape;
    if (start >= end or end > tokens.len) return error.UnsupportedSqlShape;
    if (alias_tokens.start != start or alias_tokens.end != end) return error.UnsupportedSqlShape;
    if (alias_name_tokens.end != alias_name_tokens.start + 1 or alias_name_tokens.end > end) return error.UnsupportedSqlShape;
    if (tokens[alias_name_tokens.start].kind != .identifier) return error.UnsupportedSqlShape;
    if (start + 2 == end) {
        if (!tokens[start].matchesKeywordTag(.as) or alias_name_tokens.start != start + 1) return error.UnsupportedSqlShape;
        return;
    }
    if (start + 1 == end and tokens[start].kind == .identifier) {
        if (alias_name_tokens.start != start) return error.UnsupportedSqlShape;
        return;
    }
    return error.UnsupportedSqlShape;
}

pub fn generatedLateralAliasAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
) !?[]const u8 {
    const read = generated_read_ast orelse return null;
    if (read.join_items.len != 1) return error.UnsupportedSqlShape;
    const join = read.join_items[0];
    const subquery_tokens = join.right_lateral_subquery_tokens orelse return error.UnsupportedSqlShape;
    if (pos.* != subquery_tokens.end + 1) return error.UnsupportedSqlShape;
    const alias_tokens = join.right_lateral_alias_tokens orelse return error.UnsupportedSqlShape;
    const alias_name_tokens = join.right_lateral_alias_name_tokens orelse return error.UnsupportedSqlShape;
    if (alias_tokens.start != pos.* or alias_tokens.end > tokens.len) return error.UnsupportedSqlShape;
    if (alias_name_tokens.end != alias_name_tokens.start + 1 or alias_name_tokens.end > alias_tokens.end) return error.UnsupportedSqlShape;
    if (tokens[alias_name_tokens.start].kind != .identifier) return error.UnsupportedSqlShape;
    if (alias_tokens.end == alias_tokens.start + 2) {
        if (!tokens[alias_tokens.start].matchesKeywordTag(.as) or alias_name_tokens.start != alias_tokens.start + 1) return error.UnsupportedSqlShape;
    } else if (alias_tokens.end == alias_tokens.start + 1) {
        if (alias_name_tokens.start != alias_tokens.start) return error.UnsupportedSqlShape;
    } else {
        return error.UnsupportedSqlShape;
    }
    pos.* = alias_tokens.end;
    return try alloc.dupe(u8, tokens[alias_name_tokens.start].text);
}

fn validateGeneratedJoinSideSourceMetadata(
    tokens: []const Token,
    source: generated_parser.GeneratedSqlTokenRange,
    maybe_table: ?generated_parser.GeneratedSqlTokenRange,
    maybe_alias: ?generated_parser.GeneratedSqlTokenRange,
    maybe_alias_name: ?generated_parser.GeneratedSqlTokenRange,
) !void {
    if (source.start >= source.end or source.end > tokens.len) return error.UnsupportedSqlShape;
    const source_table = maybe_table orelse {
        if (maybe_alias != null or maybe_alias_name != null) return error.UnsupportedSqlShape;
        if (generatedReadSourceLooksLikeSingleTableSource(tokens, source)) return error.UnsupportedSqlShape;
        return;
    };
    var expected_table_start = source.start;
    if (tokens[expected_table_start].matchesKeywordTag(.only)) expected_table_start += 1;
    if (source_table.start != expected_table_start or source_table.end != expected_table_start + 1 or source_table.end > source.end) return error.UnsupportedSqlShape;
    if (tokens[source_table.start].kind != .identifier) return error.UnsupportedSqlShape;
    const alias_end = try generatedSingleSourceAliasEnd(tokens, source_table, maybe_alias, maybe_alias_name);
    if (alias_end != source.end) return error.UnsupportedSqlShape;
}

pub fn validateGeneratedJoinExecutableContract(
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
    tokens: []const Token,
    expected_read_kind: generated_parser.GeneratedSqlReadKind,
) !void {
    const read = generated_read_ast orelse return;
    if (read.kind != expected_read_kind and read.kind != .cte) return error.UnsupportedSqlShape;
    try validateGeneratedJoinItemsMetadata(tokens, read.*);
    const root_index = read.join_tree_root_index orelse return error.UnsupportedSqlShape;
    if (expected_read_kind == .join and read.join_items.len > 1) {
        if (root_index != read.join_items.len - 1 or read.join_tree_depth != read.join_items.len) return error.UnsupportedSqlShape;
        for (read.join_items) |join| {
            if (join.tree_index >= read.join_items.len or join.tree_depth != join.tree_index + 1) return error.UnsupportedSqlShape;
            if (join.tree_index == 0) {
                if (join.left_child_index != null) return error.UnsupportedSqlShape;
            } else if (join.left_child_index != join.tree_index - 1) {
                return error.UnsupportedSqlShape;
            }
            if (join.kind != .inner or join.condition_kind != .on or join.predicate_tokens == null) return error.UnsupportedSqlShape;
        }
        return;
    }
    if (read.join_items.len != 1 or root_index != 0 or read.join_tree_depth != 1) return error.UnsupportedSqlShape;
    const join = read.join_items[0];
    if (join.tree_index != 0 or join.tree_depth != 1 or join.left_child_index != null) return error.UnsupportedSqlShape;
    switch (join.kind) {
        .inner, .left, .right, .full, .cross, .natural => {},
    }
    switch (join.condition_kind) {
        .none => if (join.kind != .cross and join.kind != .natural) return error.UnsupportedSqlShape,
        .on => if (join.predicate_tokens == null) return error.UnsupportedSqlShape,
        .using => if (join.using_tokens == null or join.using_column_tokens == null or join.using_columns.count == 0) return error.UnsupportedSqlShape,
    }
}

pub fn generatedLateralSubqueryReadAstForRange(
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
    start: usize,
    end: usize,
) !?*const generated_parser.GeneratedSqlReadAst {
    const read = generated_read_ast orelse return null;
    if (read.join_items.len != 1) return error.UnsupportedSqlShape;
    const join = read.join_items[0];
    const subquery_tokens = join.right_lateral_subquery_tokens orelse return error.UnsupportedSqlShape;
    if (subquery_tokens.start != start or subquery_tokens.end != end) return error.UnsupportedSqlShape;
    return join.right_lateral_subquery_read_ast orelse error.UnsupportedSqlShape;
}
