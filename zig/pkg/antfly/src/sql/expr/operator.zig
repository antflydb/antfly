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
