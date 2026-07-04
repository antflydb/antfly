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

const expr_operator = @import("operator.zig");
const expr_row_parse = @import("row_parse.zig");
const expr_token = @import("token.zig");
const parser = @import("../parser.zig");
const token_mod = @import("../token.zig");
const value_mod = @import("../value.zig");

const Token = token_mod.Token;

pub const RowExpressionOperandStart = enum {
    parenthesized,
    unary_negative,
    boolean_not,
    extension_function,
    routine_expression,
    cast,
    case,
    now,
    current_date,
    typed_datetime_literal,
    uuid_v4,
    interval,
    case_fold,
    replace,
    regexp_replace,
    regexp_substr,
    regexp_match,
    regexp_count,
    regexp_instr,
    translate,
    concat,
    coalesce,
    nullif,
    text_length,
    ascii,
    chr,
    substring,
    overlay,
    split_part,
    strpos,
    left_right,
    pad,
    repeat,
    reverse,
    md5,
    soundex,
    starts_with,
    ends_with,
    date_trunc,
    date_bin,
    date_part,
    abs,
    round,
    trunc,
    floor,
    ceil,
    sqrt,
    sign,
    mod,
    power,
    greatest_least,
    json_extract_path,
    json_typeof,
    json_array_length,
    json_build_object,
    to_jsonb,
    convert_from,
    array_length,
    array_position,
    array_element_transform,
    array_to_string,
    string_to_array,
};

pub fn rowExpressionHasTopLevelPipeConcat(tokens: []const Token, pos: usize) bool {
    var depth: usize = 0;
    var i = pos;
    while (i < tokens.len) : (i += 1) {
        const token = tokens[i];
        switch (token.kind) {
            .lparen => depth += 1,
            .rparen => {
                if (depth == 0) return false;
                depth -= 1;
            },
            .pipe_concat => if (depth == 0) return true,
            .comma, .semicolon, .eq, .neq, .lt, .lte, .gt, .gte, .at_contains, .range_overlap, .question, .arrow_json, .arrow_text, .path_arrow_json, .path_arrow_text, .regex_match, .regex_imatch, .regex_not_match, .regex_not_imatch => if (depth == 0) return false,
            .identifier => if (depth == 0 and expr_token.rowExpressionBoundaryKeywordToken(token)) return false,
            else => {},
        }
    }
    return false;
}

pub fn identifierContainsQualifier(identifier: []const u8) bool {
    const dot = std.mem.indexOfScalar(u8, identifier, '.') orelse return false;
    return dot > 0 and dot + 1 < identifier.len;
}

pub fn tailMentionsAnyQualifierBeforeClose(
    tokens: []const Token,
    pos: usize,
    qualifiers: []const []const u8,
) bool {
    var depth: usize = 0;
    var i = pos;
    while (i < tokens.len) : (i += 1) {
        const token = tokens[i];
        switch (token.kind) {
            .lparen => depth += 1,
            .rparen => {
                if (depth == 0) return false;
                depth -= 1;
            },
            .identifier => {
                const dot = std.mem.indexOfScalar(u8, token.text, '.') orelse continue;
                const qualifier = token.text[0..dot];
                for (qualifiers) |candidate| {
                    if (std.mem.eql(u8, qualifier, candidate)) return true;
                }
            },
            else => {},
        }
    }
    return false;
}

pub fn nextIsUnsupportedQueryKeyword(tokens: []const Token, pos: usize) bool {
    if (pos >= tokens.len or tokens[pos].kind != .identifier) return false;
    const token = tokens[pos];
    return token.matchesKeywordTag(.join) or
        token.matchesKeywordTag(.left) or
        token.matchesKeywordTag(.inner) or
        token.matchesKeywordTag(.group) or
        token.matchesKeywordTag(.with) or
        token.matchesKeywordTag(.over) or
        token.matchesKeywordTag(.lateral);
}

pub fn peekSqlTypedDatetimeLiteral(tokens: []const Token, pos: usize) bool {
    return pos + 1 < tokens.len and
        tokens[pos].kind == .identifier and
        tokens[pos + 1].kind == .string and
        (std.ascii.eqlIgnoreCase(tokens[pos].text, "date") or
            std.ascii.eqlIgnoreCase(tokens[pos].text, "timestamp") or
            std.ascii.eqlIgnoreCase(tokens[pos].text, "timestamptz"));
}

pub const ParsedSystemTimeAsOf = struct {
    sequence: ?u64 = null,
    timestamp_ns: ?u64 = null,

    pub fn present(self: @This()) bool {
        return self.sequence != null or self.timestamp_ns != null;
    }
};

pub fn parseOptionalSystemTimeAsOf(tokens: []const Token, pos: *usize) !ParsedSystemTimeAsOf {
    const checkpoint = pos.*;
    if (!parser.matchKeyword(tokens, pos, "for")) return .{};
    if (!parser.matchKeyword(tokens, pos, "system_time")) {
        pos.* = checkpoint;
        return .{};
    }
    try parser.expectKeyword(tokens, pos, "as");
    try parser.expectKeyword(tokens, pos, "of");
    if (parser.matchToken(tokens, pos, .number)) |token| {
        if (std.mem.indexOfAny(u8, token.text, ".eE+-") != null) return error.UnsupportedSqlShape;
        return .{ .sequence = std.fmt.parseUnsigned(u64, token.text, 10) catch return error.UnsupportedSqlShape };
    }
    const timestamp_text = if (parser.matchToken(tokens, pos, .string)) |token| token.text else timestamp_text: {
        if (!parser.matchKeywordTag(tokens, pos, .date) and
            !parser.matchKeywordTag(tokens, pos, .timestamp) and
            !parser.matchKeywordTag(tokens, pos, .timestamptz)) return error.UnsupportedSqlShape;
        const token = parser.matchToken(tokens, pos, .string) orelse return error.UnsupportedSqlShape;
        break :timestamp_text token.text;
    };
    const parsed_timestamp_ns = try value_mod.parseSqlTimestampLiteralNs(timestamp_text);
    if (parsed_timestamp_ns < 0) return error.UnsupportedSqlShape;
    return .{ .timestamp_ns = @intCast(parsed_timestamp_ns) };
}

pub fn parseOptionalSystemTimeAsOfSequence(tokens: []const Token, pos: *usize) !?u64 {
    const parsed = try parseOptionalSystemTimeAsOf(tokens, pos);
    if (parsed.timestamp_ns != null) return error.UnsupportedSqlShape;
    return parsed.sequence;
}

fn expressionStartKeywordToken(token: Token) bool {
    return token.matchesKeywordTag(.abs) or
        token.matchesKeywordTag(.array_append) or
        token.matchesKeywordTag(.array_cat) or
        token.matchesKeywordTag(.array_prepend) or
        token.matchesKeywordTag(.array_remove) or
        token.matchesKeywordTag(.array_replace) or
        token.matchesKeywordTag(.array_to_string) or
        token.matchesKeywordTag(.ascii) or
        token.matchesKeywordTag(.bit_length) or
        token.matchesKeywordTag(.case) or
        token.matchesKeywordTag(.cast) or
        token.matchesKeywordTag(.chr) or
        token.matchesKeywordTag(.coalesce) or
        token.matchesKeywordTag(.concat) or
        token.matchesKeywordTag(.concat_ws) or
        token.matchesKeywordTag(.convert_from) or
        token.matchesKeywordTag(.current_date) or
        token.matchesKeywordTag(.current_timestamp) or
        token.matchesKeywordTag(.false) or
        token.matchesKeywordTag(.length) or
        token.matchesKeywordTag(.lower) or
        token.matchesKeywordTag(.now) or
        token.matchesKeywordTag(.null) or
        token.matchesKeywordTag(.nullif) or
        token.matchesKeywordTag(.octet_length) or
        token.matchesKeywordTag(.replace) or
        token.matchesKeywordTag(.round) or
        token.matchesKeywordTag(.string_to_array) or
        token.matchesKeywordTag(.trim) or
        token.matchesKeywordTag(.true) or
        token.matchesKeywordTag(.upper);
}

pub fn expressionCanStartAt(tokens: []const Token, index: usize) bool {
    if (index >= tokens.len) return false;
    const token = tokens[index];
    if (token.kind == .minus) return true;
    if (token.kind == .number or token.kind == .string or token.kind == .placeholder) return true;
    if (token.kind != .identifier) return false;
    if (expr_token.functionCallStartsAtTokenIf(tokens, index, expr_token.sqlTokenIsArrayLengthFunction) or
        expr_token.functionCallStartsAtTokenIf(tokens, index, expr_token.sqlTokenIsArrayPositionFunction) or
        expressionStartKeywordToken(token) or
        expr_token.functionCallStartsAtTag(tokens, index, .gen_random_uuid) or
        expr_token.functionCallStartsAtTag(tokens, index, .uuid_generate_v4) or
        expr_token.functionCallStartsAtTag(tokens, index, .floor) or
        expr_token.functionCallStartsAtTag(tokens, index, .ceil) or
        expr_token.functionCallStartsAtTag(tokens, index, .sqrt) or
        expr_token.functionCallStartsAtTag(tokens, index, .sign) or
        expr_token.functionCallStartsAtTag(tokens, index, .mod) or
        expr_token.functionCallStartsAtTag(tokens, index, .power) or
        expr_token.sqlTokenIsOverlayFunction(token) or
        expr_token.functionCallStartsAtTag(tokens, index, .trunc) or
        expr_token.sqlTokenIsTrimVariantFunction(token) or
        expr_token.sqlTokenIsPadFunction(token) or
        expr_token.sqlTokenIsRepeatFunction(token) or
        expr_token.sqlTokenIsReverseFunction(token) or
        expr_token.sqlTokenIsInitcapFunction(token) or
        expr_token.sqlTokenIsMd5Function(token) or
        expr_token.sqlTokenIsStartsWithFunction(token) or
        expr_token.sqlTokenIsEndsWithFunction(token) or
        expr_token.sqlTokenIsDateTruncFunction(token) or
        expr_token.sqlTokenIsDateBinFunction(token) or
        expr_token.sqlTokenIsDatePartFunction(token) or
        (expr_token.sqlTokenIsJsonExtractPathFunction(token) and index + 1 < tokens.len and tokens[index + 1].kind == .lparen) or
        (expr_token.sqlTokenIsJsonTypeofFunction(token) and index + 1 < tokens.len and tokens[index + 1].kind == .lparen) or
        (expr_token.sqlTokenIsJsonArrayLengthFunction(token) and index + 1 < tokens.len and tokens[index + 1].kind == .lparen) or
        (expr_token.sqlTokenIsJsonBuildObjectFunction(token) and index + 1 < tokens.len and tokens[index + 1].kind == .lparen) or
        (token.matchesKeywordTag(.to_jsonb) and index + 1 < tokens.len and tokens[index + 1].kind == .lparen) or
        expr_token.sqlTokenIsRegexpMatchFunction(token) or
        expr_token.sqlTokenIsRegexpCountFunction(token) or
        expr_token.sqlTokenIsRegexpSubstrFunction(token) or
        expr_token.sqlTokenIsRegexpInstrFunction(token) or
        expr_token.sqlTokenIsTranslateFunction(token))
    {
        return true;
    }
    if (expr_operator.jsonExtractMembershipPredicateCanStartAt(tokens, index) or
        expr_operator.jsonExtractNullSafeDistinctPredicateCanStartAt(tokens, index) or
        expr_operator.jsonExtractNullTestPredicateCanStartAt(tokens, index))
    {
        return true;
    }
    return index + 1 < tokens.len and switch (tokens[index + 1].kind) {
        .plus, .minus, .star, .slash, .percent => true,
        else => false,
    };
}

pub fn rowExpressionOperandStartAt(tokens: []const Token, pos: usize) ?RowExpressionOperandStart {
    if (expr_token.peekParenthesizedExpressionSyntax(tokens, pos)) return .parenthesized;
    if (expr_token.peekUnaryNegativeExpressionSyntax(tokens, pos)) return .unary_negative;
    if (expr_token.peekBooleanNotExpressionSyntax(tokens, pos)) return .boolean_not;
    if (expr_token.peekCastExpressionSyntax(tokens, pos)) return .cast;
    if (expr_token.peekCaseExpressionSyntax(tokens, pos)) return .case;
    if (expr_token.peekSqlNowExpressionSyntax(tokens, pos)) return .now;
    if (expr_token.peekSqlCurrentDateExpressionSyntax(tokens, pos)) return .current_date;
    if (peekSqlTypedDatetimeLiteral(tokens, pos)) return .typed_datetime_literal;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsUuidV4Function)) return .uuid_v4;
    if (expr_token.peekSqlIntervalExpressionSyntax(tokens, pos)) return .interval;
    if (expr_token.peekCaseFoldFunctionCall(tokens, pos)) return .case_fold;
    if (expr_token.peekReplaceFunctionCall(tokens, pos)) return .replace;
    if (expr_token.peekRegexpReplaceFunctionCall(tokens, pos)) return .regexp_replace;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsRegexpSubstrFunction)) return .regexp_substr;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsRegexpMatchFunction)) return .regexp_match;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsRegexpCountFunction)) return .regexp_count;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsRegexpInstrFunction)) return .regexp_instr;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsTranslateFunction)) return .translate;
    if (expr_token.peekConcatFunctionCall(tokens, pos)) return .concat;
    if (expr_token.peekCoalesceFunctionCall(tokens, pos)) return .coalesce;
    if (expr_token.peekNullifFunctionCall(tokens, pos)) return .nullif;
    if (expr_token.peekTextLengthFunctionKeyword(tokens, pos)) return .text_length;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsAsciiFunction)) return .ascii;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsChrFunction)) return .chr;
    if (expr_token.peekSubstringFunctionKeyword(tokens, pos)) return .substring;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsOverlayFunction)) return .overlay;
    if (expr_token.peekSplitPartFunctionKeyword(tokens, pos)) return .split_part;
    if (expr_token.peekStrposFunctionKeyword(tokens, pos) or expr_token.peekPositionFunctionSyntax(tokens, pos)) return .strpos;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsLeftRightFunction)) return .left_right;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsPadFunction)) return .pad;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsRepeatFunction)) return .repeat;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsReverseFunction)) return .reverse;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsMd5Function)) return .md5;
    if (expr_token.peekFixedUnaryFunctionCall(tokens, pos, .soundex)) return .soundex;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsStartsWithFunction)) return .starts_with;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsEndsWithFunction)) return .ends_with;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsDateTruncFunction)) return .date_trunc;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsDateBinFunction)) return .date_bin;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsDatePartFunction)) return .date_part;
    if (expr_token.peekFixedUnaryFunctionCall(tokens, pos, .abs)) return .abs;
    if (expr_token.peekFixedUnaryFunctionCall(tokens, pos, .round)) return .round;
    if (expr_token.peekFixedUnaryFunctionCall(tokens, pos, .trunc)) return .trunc;
    if (expr_token.peekFixedUnaryFunctionCall(tokens, pos, .floor)) return .floor;
    if (expr_token.peekFixedUnaryFunctionCall(tokens, pos, .ceil)) return .ceil;
    if (expr_token.peekFixedUnaryFunctionCall(tokens, pos, .sqrt)) return .sqrt;
    if (expr_token.peekFixedUnaryFunctionCall(tokens, pos, .sign)) return .sign;
    if (expr_token.peekFixedBinaryFunctionCall(tokens, pos, .mod)) return .mod;
    if (expr_token.peekFixedBinaryFunctionCall(tokens, pos, .power)) return .power;
    if (expr_token.peekGreatestLeastFunctionCall(tokens, pos)) return .greatest_least;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsJsonExtractPathFunction)) return .json_extract_path;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsJsonTypeofFunction)) return .json_typeof;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsJsonArrayLengthFunction)) return .json_array_length;
    if (expr_token.peekFunctionCallTokenIf(tokens, pos, expr_token.sqlTokenIsJsonBuildObjectFunction)) return .json_build_object;
    if (value_mod.peekToJsonbFunctionCall(tokens, pos)) return .to_jsonb;
    if (value_mod.peekConvertFromFunctionCall(tokens, pos)) return .convert_from;
    if (expr_token.functionCallStartsAtTokenIf(tokens, pos, expr_token.sqlTokenIsArrayLengthFunction)) return .array_length;
    if (expr_token.functionCallStartsAtTokenIf(tokens, pos, expr_token.sqlTokenIsArrayPositionFunction)) return .array_position;
    if (expr_token.peekArrayElementTransformFunctionCall(tokens, pos)) return .array_element_transform;
    if (expr_token.peekArrayToStringFunctionCall(tokens, pos)) return .array_to_string;
    if (expr_token.peekStringToArrayFunctionCall(tokens, pos)) return .string_to_array;
    return null;
}

pub fn rowExpressionOperandStartWithFunctionBindingsAt(
    tokens: []const Token,
    pos: usize,
    bindings: expr_row_parse.SqlFunctionBindings,
) ?RowExpressionOperandStart {
    if (rowExpressionOperandStartAt(tokens, pos)) |start| return start;
    if (expr_row_parse.peekExtensionFunctionCall(tokens, pos, bindings.extension_functions)) return .extension_function;
    if (expr_row_parse.peekRoutineExpressionCall(tokens, pos, bindings.routine_expressions)) return .routine_expression;
    return null;
}

fn testToken(kind: token_mod.TokenKind, text: []const u8) Token {
    return .{ .kind = kind, .text = text };
}

test "sql expr_parse detects row expression operand starts" {
    const tagged_expression_starts = [_]Token{
        .{ .kind = .identifier, .text = "CONCAT_WS", .keyword = .concat_ws },
        .{ .kind = .identifier, .text = "STRING_TO_ARRAY", .keyword = .string_to_array },
        .{ .kind = .identifier, .text = "CURRENT_TIMESTAMP", .keyword = .current_timestamp },
        .{ .kind = .identifier, .text = "REPLACE", .keyword = .replace },
    };
    for (tagged_expression_starts) |start| {
        try std.testing.expect(expressionStartKeywordToken(start));
        try std.testing.expect(expressionCanStartAt(&.{start}, 0));
    }
    const token_helper_expression_starts = [_]Token{
        .{ .kind = .identifier, .text = "MD5", .keyword = .md5 },
    };
    for (token_helper_expression_starts) |start| {
        try std.testing.expect(!expressionStartKeywordToken(start));
        try std.testing.expect(expressionCanStartAt(&.{start}, 0));
    }
    const quoted_expression_name = Token{ .kind = .identifier, .text = "concat_ws", .source_start = 0, .source_end = 11 };
    try std.testing.expect(!expressionStartKeywordToken(quoted_expression_name));

    const typed_date = [_]Token{
        testToken(.identifier, "DATE"),
        testToken(.string, "'2026-01-01'"),
    };
    try std.testing.expectEqual(RowExpressionOperandStart.typed_datetime_literal, rowExpressionOperandStartAt(&typed_date, 0).?);

    const negative = [_]Token{
        testToken(.minus, "-"),
        testToken(.number, "1"),
    };
    try std.testing.expectEqual(RowExpressionOperandStart.unary_negative, rowExpressionOperandStartAt(&negative, 0).?);

    const lower_call = [_]Token{
        testToken(.identifier, "lower"),
        testToken(.lparen, "("),
        testToken(.identifier, "status"),
        testToken(.rparen, ")"),
    };
    try std.testing.expectEqual(RowExpressionOperandStart.case_fold, rowExpressionOperandStartAt(&lower_call, 0).?);

    const abs_call = [_]Token{
        testToken(.identifier, "abs"),
        testToken(.lparen, "("),
        testToken(.identifier, "amount"),
        testToken(.rparen, ")"),
    };
    try std.testing.expectEqual(RowExpressionOperandStart.abs, rowExpressionOperandStartAt(&abs_call, 0).?);

    const string_to_array_call = [_]Token{
        testToken(.identifier, "string_to_array"),
        testToken(.lparen, "("),
        testToken(.identifier, "tags"),
        testToken(.comma, ","),
        testToken(.string, "','"),
        testToken(.rparen, ")"),
    };
    try std.testing.expectEqual(RowExpressionOperandStart.string_to_array, rowExpressionOperandStartAt(&string_to_array_call, 0).?);
    try std.testing.expect(rowExpressionOperandStartAt(&string_to_array_call, 2) == null);

    const unsupported_tail_tokens = [_]Token{.{ .kind = .identifier, .text = "LATERAL", .keyword = .lateral }};
    try std.testing.expect(nextIsUnsupportedQueryKeyword(unsupported_tail_tokens[0..], 0));
    try std.testing.expect(!nextIsUnsupportedQueryKeyword(unsupported_tail_tokens[0..], 1));

    const binding_call = [_]Token{
        testToken(.identifier, "safe_slug"),
        testToken(.lparen, "("),
        testToken(.identifier, "status"),
        testToken(.rparen, ")"),
    };
    try std.testing.expectEqual(RowExpressionOperandStart.extension_function, rowExpressionOperandStartWithFunctionBindingsAt(&binding_call, 0, .{
        .extension_functions = &.{.{
            .sql_name = "safe_slug",
            .native_expression_kind = .lower,
            .arity = 1,
        }},
    }).?);
    try std.testing.expectEqual(RowExpressionOperandStart.routine_expression, rowExpressionOperandStartWithFunctionBindingsAt(&binding_call, 0, .{
        .routine_expressions = &.{.{
            .sql_name = "safe_slug",
            .arity = 1,
            .expression = .{ .kind = .field, .field = "status" },
        }},
    }).?);

    try std.testing.expect(identifierContainsQualifier("left.status"));
    try std.testing.expect(!identifierContainsQualifier("status"));
    const qualified_tail_tokens = [_]Token{
        .{ .kind = .identifier, .text = "source.status", .source_start = 0, .source_end = 13 },
        .{ .kind = .identifier, .text = "and", .source_start = 14, .source_end = 17 },
        .{ .kind = .lparen, .text = "(", .source_start = 18, .source_end = 19 },
        .{ .kind = .identifier, .text = "target.id", .source_start = 19, .source_end = 28 },
        .{ .kind = .rparen, .text = ")", .source_start = 28, .source_end = 29 },
    };
    try std.testing.expect(tailMentionsAnyQualifierBeforeClose(qualified_tail_tokens[0..], 0, &.{"target"}));
    try std.testing.expect(!tailMentionsAnyQualifierBeforeClose(qualified_tail_tokens[0..], 0, &.{"missing"}));
    const close_tail_tokens = [_]Token{
        .{ .kind = .rparen, .text = ")", .source_start = 0, .source_end = 1 },
        .{ .kind = .identifier, .text = "target.id", .source_start = 2, .source_end = 11 },
    };
    try std.testing.expect(!tailMentionsAnyQualifierBeforeClose(close_tail_tokens[0..], 0, &.{"target"}));

    const system_time_sequence = [_]Token{
        testToken(.identifier, "for"),
        testToken(.identifier, "system_time"),
        testToken(.identifier, "as"),
        testToken(.identifier, "of"),
        testToken(.number, "42"),
    };
    var sequence_pos: usize = 0;
    const parsed_sequence = try parseOptionalSystemTimeAsOf(system_time_sequence[0..], &sequence_pos);
    try std.testing.expect(parsed_sequence.present());
    try std.testing.expectEqual(@as(?u64, 42), parsed_sequence.sequence);
    try std.testing.expectEqual(@as(usize, system_time_sequence.len), sequence_pos);

    var sequence_only_pos: usize = 0;
    try std.testing.expectEqual(@as(?u64, 42), try parseOptionalSystemTimeAsOfSequence(system_time_sequence[0..], &sequence_only_pos));
    try std.testing.expectEqual(@as(usize, system_time_sequence.len), sequence_only_pos);

    const no_system_time = [_]Token{
        testToken(.identifier, "for"),
        testToken(.identifier, "update"),
    };
    var no_system_time_pos: usize = 0;
    const parsed_absent = try parseOptionalSystemTimeAsOf(no_system_time[0..], &no_system_time_pos);
    try std.testing.expect(!parsed_absent.present());
    try std.testing.expectEqual(@as(usize, 0), no_system_time_pos);

    const system_time_timestamp = [_]Token{
        testToken(.identifier, "for"),
        testToken(.identifier, "system_time"),
        testToken(.identifier, "as"),
        testToken(.identifier, "of"),
        testToken(.identifier, "timestamp"),
        testToken(.string, "'2026-01-01T00:00:00Z'"),
    };
    var timestamp_pos: usize = 0;
    const parsed_timestamp = try parseOptionalSystemTimeAsOf(system_time_timestamp[0..], &timestamp_pos);
    try std.testing.expect(parsed_timestamp.present());
    try std.testing.expect(parsed_timestamp.timestamp_ns != null);
    try std.testing.expectEqual(@as(usize, system_time_timestamp.len), timestamp_pos);

    var timestamp_sequence_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseOptionalSystemTimeAsOfSequence(system_time_timestamp[0..], &timestamp_sequence_pos));
}
