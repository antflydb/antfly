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

const expr_token = @import("token.zig");
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

fn peekSqlTypedDatetimeLiteral(tokens: []const Token, pos: usize) bool {
    return pos + 1 < tokens.len and
        tokens[pos].kind == .identifier and
        tokens[pos + 1].kind == .string and
        (std.ascii.eqlIgnoreCase(tokens[pos].text, "date") or
            std.ascii.eqlIgnoreCase(tokens[pos].text, "timestamp") or
            std.ascii.eqlIgnoreCase(tokens[pos].text, "timestamptz"));
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

fn testToken(kind: token_mod.TokenKind, text: []const u8) Token {
    return .{ .kind = kind, .text = text };
}

test "sql expr_parse detects row expression operand starts" {
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
}
