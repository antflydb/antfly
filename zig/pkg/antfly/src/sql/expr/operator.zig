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
const parser = @import("../parser.zig");
const runtime_schema = @import("../../storage/schema.zig");
const token_mod = @import("../token.zig");

pub const Token = token_mod.Token;
pub const TokenKind = token_mod.TokenKind;

pub const RegexPredicateOperator = struct {
    case_insensitive: bool,
    negated: bool,
};

pub fn matchRegexPredicateOperator(tokens: []const Token, pos: *usize) ?RegexPredicateOperator {
    if (parser.matchToken(tokens, pos, .regex_match) != null) return .{ .case_insensitive = false, .negated = false };
    if (parser.matchToken(tokens, pos, .regex_imatch) != null) return .{ .case_insensitive = true, .negated = false };
    if (parser.matchToken(tokens, pos, .regex_not_match) != null) return .{ .case_insensitive = false, .negated = true };
    if (parser.matchToken(tokens, pos, .regex_not_imatch) != null) return .{ .case_insensitive = true, .negated = true };
    return null;
}

pub const ArithmeticOperator = struct {
    token: TokenKind,
    kind: db_mod.types.RelationalRowsExpressionKind,
    precedence: u8,
};

pub const BooleanOperator = struct {
    keyword: []const u8,
    kind: db_mod.types.RelationalRowsExpressionKind,
    precedence: u8,
};

pub fn peekArithmeticOperator(tokens: []const Token, pos: usize) ?ArithmeticOperator {
    if (parser.peekKind(tokens, pos, .plus)) return .{ .token = .plus, .kind = .add, .precedence = 1 };
    if (parser.peekKind(tokens, pos, .minus)) return .{ .token = .minus, .kind = .sub, .precedence = 1 };
    if (parser.peekKind(tokens, pos, .star)) return .{ .token = .star, .kind = .mul, .precedence = 2 };
    if (parser.peekKind(tokens, pos, .slash)) return .{ .token = .slash, .kind = .div, .precedence = 2 };
    if (parser.peekKind(tokens, pos, .percent)) return .{ .token = .percent, .kind = .mod, .precedence = 2 };
    return null;
}

pub fn peekArithmeticRhsKeyword(tokens: []const Token, pos: usize, keyword: []const u8) bool {
    if (pos + 2 >= tokens.len) return false;
    const op = tokens[pos + 1].kind;
    if (op != .plus and op != .minus and op != .star and op != .slash and op != .percent) return false;
    const rhs = tokens[pos + 2];
    return rhs.kind == .identifier and std.ascii.eqlIgnoreCase(rhs.text, keyword);
}

pub fn peekConflictExistingFieldIncrement(
    tokens: []const Token,
    pos: usize,
    field: []const u8,
    column: runtime_schema.RelationalColumn,
) bool {
    if (column.field_type != .numeric) return false;
    if (pos + 1 >= tokens.len or tokens[pos].kind != .identifier) return false;
    if (!std.mem.eql(u8, tokens[pos].text, field)) return false;
    const op = tokens[pos + 1].kind;
    if (op != .plus and op != .minus) return false;
    return !peekArithmeticRhsKeyword(tokens, pos, "interval");
}

pub fn peekBooleanOperator(tokens: []const Token, pos: usize) ?BooleanOperator {
    if (parser.peekKeyword(tokens, pos, "or")) return .{ .keyword = "or", .kind = .bool_or, .precedence = 1 };
    if (parser.peekKeyword(tokens, pos, "and")) return .{ .keyword = "and", .kind = .bool_and, .precedence = 2 };
    return null;
}

pub fn tokenKindIsJsonExtractOperator(kind: TokenKind) bool {
    return kind == .arrow_json or kind == .arrow_text or kind == .path_arrow_json or kind == .path_arrow_text;
}

pub fn tokenKindIsJsonExtractTextOperator(kind: TokenKind) bool {
    return kind == .arrow_text or kind == .path_arrow_text;
}

pub fn tokenKindIsJsonExtractPathOperator(kind: TokenKind) bool {
    return kind == .path_arrow_json or kind == .path_arrow_text;
}

pub fn peekJsonExtractOperator(tokens: []const Token, pos: usize) bool {
    return pos < tokens.len and tokenKindIsJsonExtractOperator(tokens[pos].kind);
}

pub fn matchJsonExtractOperator(tokens: []const Token, pos: *usize) ?TokenKind {
    if (!peekJsonExtractOperator(tokens, pos.*)) return null;
    const operator = tokens[pos.*].kind;
    pos.* += 1;
    return operator;
}

pub fn jsonExtractExpressionCanStartAt(tokens: []const Token, index: usize) bool {
    if (index + 2 >= tokens.len) return false;
    if (tokens[index].kind != .identifier) return false;
    if (!tokenKindIsJsonExtractOperator(tokens[index + 1].kind)) return false;
    return tokens[index + 2].kind == .string or tokens[index + 2].kind == .placeholder;
}

pub fn jsonExtractExpressionPredicateCanStartAt(tokens: []const Token, index: usize) bool {
    if (index + 3 >= tokens.len) return false;
    if (!jsonExtractExpressionCanStartAt(tokens, index)) return false;
    return switch (tokens[index + 3].kind) {
        .eq, .neq => true,
        else => false,
    };
}

pub fn jsonExtractNullSafeDistinctPredicateCanStartAt(tokens: []const Token, index: usize) bool {
    if (index + 5 >= tokens.len) return false;
    if (!jsonExtractExpressionCanStartAt(tokens, index)) return false;
    if (!tokens[index + 3].matchesKeywordTag(.is)) {
        return false;
    }
    var distinct_index = index + 4;
    if (tokens[distinct_index].matchesKeywordTag(.not)) {
        distinct_index += 1;
    }
    return distinct_index + 1 < tokens.len and
        tokens[distinct_index].matchesKeywordTag(.distinct) and
        tokens[distinct_index + 1].matchesKeywordTag(.from);
}

pub fn jsonExtractNullTestPredicateCanStartAt(tokens: []const Token, index: usize) bool {
    if (index + 4 >= tokens.len) return false;
    if (!jsonExtractExpressionCanStartAt(tokens, index)) return false;
    if (!tokens[index + 3].matchesKeywordTag(.is)) {
        return false;
    }
    var null_index = index + 4;
    if (tokens[null_index].matchesKeywordTag(.not)) {
        null_index += 1;
    }
    return null_index < tokens.len and
        tokens[null_index].matchesKeywordTag(.null);
}

pub fn jsonExtractMembershipPredicateCanStartAt(tokens: []const Token, index: usize) bool {
    if (index + 3 >= tokens.len) return false;
    if (!jsonExtractExpressionCanStartAt(tokens, index)) return false;
    const op = tokens[index + 3];
    if (op.matchesKeywordTag(.in)) return true;
    if (op.matchesKeywordTag(.not)) {
        return index + 4 < tokens.len and
            tokens[index + 4].matchesKeywordTag(.in);
    }
    if (op.kind == .eq or op.kind == .neq) {
        return index + 4 < tokens.len and
            tokens[index + 4].kind == .identifier and
            (tokens[index + 4].matchesKeywordTag(.any) or
                tokens[index + 4].matchesKeywordTag(.some) or
                tokens[index + 4].matchesKeywordTag(.all));
    }
    return false;
}

pub fn jsonKeySetExpressionCanStartAt(tokens: []const Token, index: usize) bool {
    return index + 1 < tokens.len and
        tokens[index].kind == .identifier and
        (tokens[index + 1].kind == .question_any or tokens[index + 1].kind == .question_all);
}

pub fn matchPostfixNullTest(tokens: []const Token, pos: *usize) ?runtime_schema.RelationalCheckOp {
    if (parser.matchKeyword(tokens, pos, "isnull")) return .is_null;
    if (parser.matchKeyword(tokens, pos, "notnull")) return .is_not_null;
    return null;
}

pub const ExpressionIsTailKind = enum {
    null_test,
    distinct_comparison,
    boolean_unknown,
    boolean_literal,
};

pub const ExpressionIsTailOptions = struct {
    allow_distinct: bool = true,
    allow_boolean_unknown: bool = false,
    allow_boolean_literal: bool = false,
    allow_boolean_literal_negation: bool = false,
};

pub const ExpressionIsTail = struct {
    op: runtime_schema.RelationalCheckOp,
    kind: ExpressionIsTailKind,
    boolean_value: bool = false,
    boolean_negated: bool = false,
};

pub fn parseExpressionIsTailIf(
    tokens: []const Token,
    pos: *usize,
    options: ExpressionIsTailOptions,
) !?ExpressionIsTail {
    if (!parser.matchKeyword(tokens, pos, "is")) return null;
    const not = parser.matchKeyword(tokens, pos, "not");
    if (parser.matchKeyword(tokens, pos, "distinct")) {
        if (!options.allow_distinct) return error.UnsupportedSqlShape;
        try parser.expectKeyword(tokens, pos, "from");
        return .{
            .op = if (not) .is_not_distinct else .is_distinct,
            .kind = .distinct_comparison,
        };
    }
    if (options.allow_boolean_unknown and parser.matchKeyword(tokens, pos, "unknown")) {
        return .{
            .op = if (not) .is_not_null else .is_null,
            .kind = .boolean_unknown,
        };
    }
    if (options.allow_boolean_literal) {
        if (parser.matchKeyword(tokens, pos, "true")) {
            if (not and !options.allow_boolean_literal_negation) return error.UnsupportedSqlShape;
            return .{
                .op = .eq,
                .kind = .boolean_literal,
                .boolean_value = true,
                .boolean_negated = not,
            };
        }
        if (parser.matchKeyword(tokens, pos, "false")) {
            if (not and !options.allow_boolean_literal_negation) return error.UnsupportedSqlShape;
            return .{
                .op = .eq,
                .kind = .boolean_literal,
                .boolean_value = false,
                .boolean_negated = not,
            };
        }
    }
    try parser.expectKeyword(tokens, pos, "null");
    return .{
        .op = if (not) .is_not_null else .is_null,
        .kind = .null_test,
    };
}

pub fn parseComparisonOp(tokens: []const Token, pos: *usize) !runtime_schema.RelationalCheckOp {
    if (parser.matchToken(tokens, pos, .eq) != null) return .eq;
    if (parser.matchToken(tokens, pos, .neq) != null) return .ne;
    if (parser.matchToken(tokens, pos, .gt) != null) return .gt;
    if (parser.matchToken(tokens, pos, .gte) != null) return .gte;
    if (parser.matchToken(tokens, pos, .lt) != null) return .lt;
    if (parser.matchToken(tokens, pos, .lte) != null) return .lte;
    return error.UnsupportedSqlShape;
}

pub fn parseExpressionCastType(tokens: []const Token, pos: *usize) !db_mod.types.RelationalRowsExpressionCastType {
    const token = parser.matchToken(tokens, pos, .identifier) orelse return error.UnsupportedSqlShape;
    if (std.ascii.eqlIgnoreCase(token.text, "text")) return .text;
    if (std.ascii.eqlIgnoreCase(token.text, "numeric")) return .numeric;
    if (std.ascii.eqlIgnoreCase(token.text, "bool") or std.ascii.eqlIgnoreCase(token.text, "boolean")) return .bool;
    if (std.ascii.eqlIgnoreCase(token.text, "datetime") or
        std.ascii.eqlIgnoreCase(token.text, "timestamp") or
        std.ascii.eqlIgnoreCase(token.text, "timestamptz") or
        std.ascii.eqlIgnoreCase(token.text, "date")) return .datetime;
    return error.UnsupportedSqlShape;
}

test "sql expr_operator handles expression operator helpers" {
    const arithmetic_tokens = [_]Token{
        .{ .kind = .identifier, .text = "amount" },
        .{ .kind = .plus, .text = "+" },
        .{ .kind = .identifier, .text = "interval" },
    };
    try std.testing.expectEqual(TokenKind.plus, peekArithmeticOperator(&arithmetic_tokens, 1).?.token);
    try std.testing.expect(peekArithmeticRhsKeyword(&arithmetic_tokens, 0, "interval"));
    try std.testing.expect(!peekArithmeticRhsKeyword(&arithmetic_tokens, 1, "interval"));

    const increment_tokens = [_]Token{
        .{ .kind = .identifier, .text = "amount" },
        .{ .kind = .plus, .text = "+" },
        .{ .kind = .number, .text = "1" },
    };
    const numeric_column = runtime_schema.RelationalColumn{ .name = "amount", .path = "amount", .field_type = .numeric };
    try std.testing.expect(peekConflictExistingFieldIncrement(&increment_tokens, 0, "amount", numeric_column));

    var comparison_pos: usize = 0;
    const comparison_tokens = [_]Token{.{ .kind = .gte, .text = ">=" }};
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.gte, try parseComparisonOp(&comparison_tokens, &comparison_pos));
    try std.testing.expectEqual(@as(usize, 1), comparison_pos);

    var postfix_pos: usize = 0;
    const postfix_tokens = [_]Token{.{ .kind = .identifier, .text = "notnull" }};
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.is_not_null, matchPostfixNullTest(&postfix_tokens, &postfix_pos).?);
    try std.testing.expectEqual(@as(usize, 1), postfix_pos);

    try std.testing.expect(tokenKindIsJsonExtractOperator(.arrow_json));
    try std.testing.expect(tokenKindIsJsonExtractOperator(.path_arrow_text));
    try std.testing.expect(tokenKindIsJsonExtractTextOperator(.arrow_text));
    try std.testing.expect(!tokenKindIsJsonExtractTextOperator(.arrow_json));
    try std.testing.expect(tokenKindIsJsonExtractPathOperator(.path_arrow_json));
    try std.testing.expect(!tokenKindIsJsonExtractPathOperator(.arrow_json));

    const json_extract_tokens = [_]Token{
        .{ .kind = .identifier, .text = "metadata" },
        .{ .kind = .arrow_text, .text = "->>" },
        .{ .kind = .string, .text = "status" },
        .{ .kind = .identifier, .text = "is" },
        .{ .kind = .identifier, .text = "not" },
        .{ .kind = .identifier, .text = "distinct" },
        .{ .kind = .identifier, .text = "from" },
        .{ .kind = .string, .text = "active" },
    };
    try std.testing.expect(jsonExtractExpressionCanStartAt(&json_extract_tokens, 0));
    try std.testing.expect(!jsonExtractExpressionPredicateCanStartAt(&json_extract_tokens, 0));
    try std.testing.expect(jsonExtractNullSafeDistinctPredicateCanStartAt(&json_extract_tokens, 0));

    const json_null_tokens = [_]Token{
        .{ .kind = .identifier, .text = "metadata" },
        .{ .kind = .arrow_json, .text = "->" },
        .{ .kind = .placeholder, .text = "$1" },
        .{ .kind = .identifier, .text = "is" },
        .{ .kind = .identifier, .text = "null" },
    };
    try std.testing.expect(jsonExtractNullTestPredicateCanStartAt(&json_null_tokens, 0));

    const json_membership_tokens = [_]Token{
        .{ .kind = .identifier, .text = "metadata" },
        .{ .kind = .arrow_text, .text = "->>" },
        .{ .kind = .string, .text = "status" },
        .{ .kind = .identifier, .text = "not" },
        .{ .kind = .identifier, .text = "in" },
    };
    try std.testing.expect(jsonExtractMembershipPredicateCanStartAt(&json_membership_tokens, 0));

    const json_all_membership_tokens = [_]Token{
        .{ .kind = .identifier, .text = "metadata" },
        .{ .kind = .arrow_text, .text = "->>" },
        .{ .kind = .string, .text = "status" },
        .{ .kind = .eq, .text = "=" },
        .{ .kind = .identifier, .text = "ALL", .keyword = .all },
    };
    try std.testing.expect(jsonExtractMembershipPredicateCanStartAt(&json_all_membership_tokens, 0));

    var regex_pos: usize = 0;
    const regex_tokens = [_]Token{.{ .kind = .regex_not_imatch, .text = "!~*" }};
    const regex_operator = matchRegexPredicateOperator(&regex_tokens, &regex_pos) orelse return error.TestUnexpectedResult;
    try std.testing.expect(regex_operator.case_insensitive);
    try std.testing.expect(regex_operator.negated);
    try std.testing.expectEqual(@as(usize, 1), regex_pos);

    const json_key_set_tokens = [_]Token{
        .{ .kind = .identifier, .text = "metadata" },
        .{ .kind = .question_any, .text = "?|" },
    };
    try std.testing.expect(jsonKeySetExpressionCanStartAt(&json_key_set_tokens, 0));
}
