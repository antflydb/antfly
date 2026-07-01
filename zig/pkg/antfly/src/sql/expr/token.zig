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

const ast = @import("../ast.zig");
const db_mod = @import("../../storage/db/mod.zig");
const parser = @import("../parser.zig");
const token_mod = @import("../token.zig");

pub const Token = token_mod.Token;
pub const TokenKeyword = token_mod.TokenKeyword;

pub fn sqlKeywordIsAnyOrSome(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "any") or std.ascii.eqlIgnoreCase(text, "some");
}

pub fn sqlKeywordStartsScalarPredicate(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "is") or
        std.ascii.eqlIgnoreCase(text, "isnull") or
        std.ascii.eqlIgnoreCase(text, "in") or
        std.ascii.eqlIgnoreCase(text, "between") or
        std.ascii.eqlIgnoreCase(text, "like") or
        std.ascii.eqlIgnoreCase(text, "ilike") or
        std.ascii.eqlIgnoreCase(text, "notnull") or
        sqlKeywordIsAnyOrSome(text) or
        std.ascii.eqlIgnoreCase(text, "all");
}

pub fn sqlJoinedSourceAliasTerminator(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "where") or
        std.ascii.eqlIgnoreCase(text, "returning") or
        std.ascii.eqlIgnoreCase(text, "order") or
        std.ascii.eqlIgnoreCase(text, "limit") or
        std.ascii.eqlIgnoreCase(text, "offset") or
        std.ascii.eqlIgnoreCase(text, "for");
}

pub fn sqlJoinedSourceAliasTerminatorToken(token: Token) bool {
    return token.matchesKeywordTag(.where) or
        token.matchesKeywordTag(.returning) or
        token.matchesKeywordTag(.order) or
        token.matchesKeywordTag(.limit) or
        token.matchesKeywordTag(.offset) or
        token.matchesKeywordTag(.@"for");
}

pub fn sqlAssignmentTailKeyword(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "where") or
        std.ascii.eqlIgnoreCase(text, "from") or
        std.ascii.eqlIgnoreCase(text, "returning") or
        std.ascii.eqlIgnoreCase(text, "order") or
        std.ascii.eqlIgnoreCase(text, "limit") or
        std.ascii.eqlIgnoreCase(text, "offset") or
        std.ascii.eqlIgnoreCase(text, "for");
}

pub fn sqlAssignmentTailKeywordToken(token: Token) bool {
    return token.matchesKeywordTag(.where) or
        token.matchesKeywordTag(.from) or
        token.matchesKeywordTag(.returning) or
        token.matchesKeywordTag(.order) or
        token.matchesKeywordTag(.limit) or
        token.matchesKeywordTag(.offset) or
        token.matchesKeywordTag(.@"for");
}

pub fn sqlKeywordIsLengthFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "length") or
        std.ascii.eqlIgnoreCase(text, "char_length") or
        std.ascii.eqlIgnoreCase(text, "character_length");
}

pub fn sqlKeywordIsOctetLengthFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "octet_length");
}

pub fn sqlKeywordIsBitLengthFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "bit_length");
}

pub fn sqlKeywordIsJsonArrayLengthFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "json_array_length") or
        std.ascii.eqlIgnoreCase(text, "jsonb_array_length");
}

pub fn sqlKeywordIsCardinalityFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "cardinality");
}

pub fn sqlKeywordIsArrayLengthFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "array_length") or
        sqlKeywordIsCardinalityFunction(text);
}

pub fn sqlKeywordIsArrayPositionFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "array_position") or
        std.ascii.eqlIgnoreCase(text, "array_positions");
}

pub fn sqlKeywordIsArrayToStringFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "array_to_string");
}

pub fn arrayLengthDefaultOutput(keyword: []const u8) []const u8 {
    if (sqlKeywordIsCardinalityFunction(keyword)) return "cardinality";
    return "array_length";
}

pub fn sqlKeywordIsJsonTypeofFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "json_typeof") or
        std.ascii.eqlIgnoreCase(text, "jsonb_typeof");
}

pub fn sqlKeywordIsJsonExtractPathFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "json_extract_path") or
        std.ascii.eqlIgnoreCase(text, "json_extract_path_text") or
        std.ascii.eqlIgnoreCase(text, "jsonb_extract_path") or
        std.ascii.eqlIgnoreCase(text, "jsonb_extract_path_text");
}

pub fn sqlKeywordIsJsonBuildObjectFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "json_build_object") or
        std.ascii.eqlIgnoreCase(text, "jsonb_build_object");
}

pub fn sqlJsonExtractPathFunctionAsText(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "json_extract_path_text") or
        std.ascii.eqlIgnoreCase(text, "jsonb_extract_path_text");
}

pub fn sqlJsonExtractPathTokenAsText(token: Token) bool {
    return token.matchesKeywordTag(.json_extract_path_text) or
        token.matchesKeywordTag(.jsonb_extract_path_text);
}

pub fn sqlKeywordIsAsciiFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "ascii");
}

pub fn sqlKeywordIsChrFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "chr");
}

pub fn sqlKeywordIsSubstringFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "substring") or
        std.ascii.eqlIgnoreCase(text, "substr");
}

pub fn sqlKeywordIsOverlayFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "overlay");
}

pub fn sqlKeywordIsTranslateFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "translate");
}

pub fn sqlKeywordIsSplitPartFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "split_part");
}

pub fn sqlKeywordIsStrposFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "strpos");
}

pub fn sqlKeywordIsLeftRightFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "left") or
        std.ascii.eqlIgnoreCase(text, "right");
}

pub fn sqlKeywordIsPadFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "lpad") or
        std.ascii.eqlIgnoreCase(text, "rpad");
}

pub fn sqlKeywordIsRepeatFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "repeat");
}

pub fn sqlKeywordIsReverseFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "reverse");
}

pub fn sqlKeywordIsInitcapFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "initcap");
}

pub fn sqlKeywordIsMd5Function(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "md5");
}

pub fn sqlKeywordIsStartsWithFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "starts_with");
}

pub fn sqlKeywordIsEndsWithFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "ends_with");
}

pub fn sqlKeywordIsDateTruncFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "date_trunc");
}

pub fn sqlKeywordIsDateBinFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "date_bin");
}

pub fn sqlKeywordIsDatePartFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "date_part") or
        std.ascii.eqlIgnoreCase(text, "extract");
}

pub fn sqlKeywordIsTrimVariantFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "btrim") or
        std.ascii.eqlIgnoreCase(text, "ltrim") or
        std.ascii.eqlIgnoreCase(text, "rtrim");
}

pub fn sqlKeywordIsUuidV4Function(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "gen_random_uuid") or
        std.ascii.eqlIgnoreCase(text, "uuid_generate_v4");
}

pub fn sqlKeywordIsRegexpMatchFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "regexp_match") or
        std.ascii.eqlIgnoreCase(text, "regexp_like");
}

pub fn sqlKeywordIsRegexpCountFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "regexp_count");
}

pub fn sqlKeywordIsRegexpSubstrFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "regexp_substr");
}

pub fn sqlKeywordIsRegexpInstrFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "regexp_instr");
}

fn tokenMatchesKeywordTags(token: Token, comptime tags: []const TokenKeyword) bool {
    inline for (tags) |tag| {
        if (token.matchesKeywordTag(tag)) return true;
    }
    return false;
}

pub fn sqlTokenIsLengthFunction(token: Token) bool {
    return tokenMatchesKeywordTags(token, &.{ .length, .char_length, .character_length });
}

pub fn sqlTokenIsOctetLengthFunction(token: Token) bool {
    return token.matchesKeywordTag(.octet_length);
}

pub fn sqlTokenIsBitLengthFunction(token: Token) bool {
    return token.matchesKeywordTag(.bit_length);
}

pub fn sqlTokenIsJsonArrayLengthFunction(token: Token) bool {
    return tokenMatchesKeywordTags(token, &.{ .json_array_length, .jsonb_array_length });
}

pub fn sqlTokenIsCardinalityFunction(token: Token) bool {
    return token.matchesKeywordTag(.cardinality);
}

pub fn sqlTokenIsArrayLengthFunction(token: Token) bool {
    return token.matchesKeywordTag(.array_length) or sqlTokenIsCardinalityFunction(token);
}

pub fn sqlTokenIsArrayPositionFunction(token: Token) bool {
    return tokenMatchesKeywordTags(token, &.{ .array_position, .array_positions });
}

pub fn sqlTokenIsArrayToStringFunction(token: Token) bool {
    return token.matchesKeywordTag(.array_to_string);
}

pub fn sqlTokenIsJsonTypeofFunction(token: Token) bool {
    return tokenMatchesKeywordTags(token, &.{ .json_typeof, .jsonb_typeof });
}

pub fn sqlTokenIsJsonExtractPathFunction(token: Token) bool {
    return tokenMatchesKeywordTags(token, &.{ .json_extract_path, .json_extract_path_text, .jsonb_extract_path, .jsonb_extract_path_text });
}

pub fn sqlTokenIsJsonBuildObjectFunction(token: Token) bool {
    return tokenMatchesKeywordTags(token, &.{ .json_build_object, .jsonb_build_object });
}

pub fn sqlTokenIsAsciiFunction(token: Token) bool {
    return token.matchesKeywordTag(.ascii);
}

pub fn sqlTokenIsChrFunction(token: Token) bool {
    return token.matchesKeywordTag(.chr);
}

pub fn sqlTokenIsSubstringFunction(token: Token) bool {
    return tokenMatchesKeywordTags(token, &.{ .substring, .substr });
}

pub fn sqlTokenIsOverlayFunction(token: Token) bool {
    return token.matchesKeywordTag(.overlay);
}

pub fn sqlTokenIsTranslateFunction(token: Token) bool {
    return token.matchesKeywordTag(.translate);
}

pub fn sqlTokenIsSplitPartFunction(token: Token) bool {
    return token.matchesKeywordTag(.split_part);
}

pub fn sqlTokenIsStrposFunction(token: Token) bool {
    return token.matchesKeywordTag(.strpos);
}

pub fn sqlTokenIsLeftRightFunction(token: Token) bool {
    return token.matchesKeywordTag(.left) or token.matchesKeywordTag(.right);
}

pub fn sqlTokenIsPadFunction(token: Token) bool {
    return token.matchesKeywordTag(.lpad) or token.matchesKeywordTag(.rpad);
}

pub fn sqlTokenIsRepeatFunction(token: Token) bool {
    return token.matchesKeywordTag(.repeat);
}

pub fn sqlTokenIsReverseFunction(token: Token) bool {
    return token.matchesKeywordTag(.reverse);
}

pub fn sqlTokenIsInitcapFunction(token: Token) bool {
    return token.matchesKeywordTag(.initcap);
}

pub fn sqlTokenIsMd5Function(token: Token) bool {
    return token.matchesKeywordTag(.md5);
}

pub fn sqlTokenIsStartsWithFunction(token: Token) bool {
    return token.matchesKeywordTag(.starts_with);
}

pub fn sqlTokenIsEndsWithFunction(token: Token) bool {
    return token.matchesKeywordTag(.ends_with);
}

pub fn sqlTokenIsDateTruncFunction(token: Token) bool {
    return token.matchesKeywordTag(.date_trunc);
}

pub fn sqlTokenIsDateBinFunction(token: Token) bool {
    return token.matchesKeywordTag(.date_bin);
}

pub fn sqlTokenIsDatePartFunction(token: Token) bool {
    return token.matchesKeywordTag(.date_part) or token.matchesKeywordTag(.extract);
}

pub fn sqlTokenIsTrimVariantFunction(token: Token) bool {
    return tokenMatchesKeywordTags(token, &.{ .btrim, .ltrim, .rtrim });
}

pub fn sqlTokenIsUuidV4Function(token: Token) bool {
    return token.matchesKeywordTag(.gen_random_uuid) or token.matchesKeywordTag(.uuid_generate_v4);
}

pub fn sqlTokenIsRegexpMatchFunction(token: Token) bool {
    return token.matchesKeywordTag(.regexp_match) or token.matchesKeywordTag(.regexp_like);
}

pub fn sqlTokenIsRegexpCountFunction(token: Token) bool {
    return token.matchesKeywordTag(.regexp_count);
}

pub fn sqlTokenIsRegexpSubstrFunction(token: Token) bool {
    return token.matchesKeywordTag(.regexp_substr);
}

pub fn sqlTokenIsRegexpInstrFunction(token: Token) bool {
    return token.matchesKeywordTag(.regexp_instr);
}

pub fn matchAnyOrSomeKeyword(tokens: []const Token, pos: *usize) bool {
    if (pos.* >= tokens.len) return false;
    const token = tokens[pos.*];
    if (!token.matchesKeywordTag(.any) and !token.matchesKeywordTag(.some)) return false;
    pos.* += 1;
    return true;
}

pub fn tokenAtIsAnySomeOrAll(tokens: []const Token, index: usize) bool {
    if (index >= tokens.len) return false;
    const token = tokens[index];
    return token.matchesKeywordTag(.any) or
        token.matchesKeywordTag(.some) or
        token.matchesKeywordTag(.all);
}

pub fn matchAnySomeOrAllKeyword(tokens: []const Token, pos: *usize) ?ast.SqlPatternQuantifier {
    if (pos.* >= tokens.len) return null;
    const token = tokens[pos.*];
    if (token.matchesKeywordTag(.any) or token.matchesKeywordTag(.some)) {
        pos.* += 1;
        return .any;
    }
    if (token.matchesKeywordTag(.all)) {
        pos.* += 1;
        return .all;
    }
    return null;
}

pub fn matchBetweenSymmetricMode(tokens: []const Token, pos: *usize) bool {
    const cursor = parser.Cursor.init(tokens, pos);
    if (cursor.matchKeyword("symmetric")) return true;
    _ = cursor.matchKeyword("asymmetric");
    return false;
}

pub fn matchLeftRightFunctionKind(tokens: []const Token, pos: *usize) ?db_mod.types.RelationalRowsExpressionKind {
    if (pos.* >= tokens.len) return null;
    const token = tokens[pos.*];
    if (token.kind != .identifier) return null;
    const kind: db_mod.types.RelationalRowsExpressionKind = if (token.matchesKeywordTag(.left))
        .left
    else if (token.matchesKeywordTag(.right))
        .right
    else
        return null;
    pos.* += 1;
    return kind;
}

pub fn matchPadFunctionKind(tokens: []const Token, pos: *usize) ?db_mod.types.RelationalRowsExpressionKind {
    if (pos.* >= tokens.len) return null;
    const token = tokens[pos.*];
    if (token.kind != .identifier) return null;
    const kind: db_mod.types.RelationalRowsExpressionKind = if (token.matchesKeywordTag(.lpad))
        .lpad
    else if (token.matchesKeywordTag(.rpad))
        .rpad
    else
        return null;
    pos.* += 1;
    return kind;
}

pub fn matchTrimVariantFunctionKind(tokens: []const Token, pos: *usize) ?db_mod.types.RelationalRowsExpressionKind {
    if (pos.* >= tokens.len) return null;
    const token = tokens[pos.*];
    if (token.kind != .identifier) return null;
    const kind: db_mod.types.RelationalRowsExpressionKind = if (token.matchesKeywordTag(.ltrim))
        .ltrim
    else if (token.matchesKeywordTag(.rtrim))
        .rtrim
    else if (token.matchesKeywordTag(.btrim))
        .trim
    else
        return null;
    pos.* += 1;
    return kind;
}

pub fn matchFunctionKeyword(
    tokens: []const Token,
    pos: *usize,
    comptime predicate: fn ([]const u8) bool,
) bool {
    return parser.Cursor.init(tokens, pos).matchIdentifierIf(predicate) != null;
}

pub fn matchFunctionKeywordText(
    tokens: []const Token,
    pos: *usize,
    comptime predicate: fn ([]const u8) bool,
) ?[]const u8 {
    const token = parser.Cursor.init(tokens, pos).matchIdentifierIf(predicate) orelse return null;
    return token.text;
}

pub fn matchFunctionKeywordToken(
    tokens: []const Token,
    pos: *usize,
    comptime predicate: fn (Token) bool,
) ?Token {
    if (pos.* >= tokens.len) return null;
    const token = tokens[pos.*];
    if (token.kind != .identifier or !predicate(token)) return null;
    pos.* += 1;
    return token;
}

pub fn parseFunctionCallStartIf(
    tokens: []const Token,
    pos: *usize,
    comptime predicate: fn ([]const u8) bool,
) !void {
    _ = matchFunctionKeywordText(tokens, pos, predicate) orelse return error.UnsupportedSqlShape;
    try parser.expectToken(tokens, pos, .lparen);
}

pub fn parseFunctionCallStartTokenIf(
    tokens: []const Token,
    pos: *usize,
    comptime predicate: fn (Token) bool,
) !void {
    _ = matchFunctionKeywordToken(tokens, pos, predicate) orelse return error.UnsupportedSqlShape;
    try parser.expectToken(tokens, pos, .lparen);
}

pub fn parseBooleanNotExpressionStart(tokens: []const Token, pos: *usize) !void {
    try parser.expectKeyword(tokens, pos, "not");
}

pub fn parseCaseExpressionStart(tokens: []const Token, pos: *usize) !void {
    try parser.expectKeyword(tokens, pos, "case");
}

pub fn matchCaseExpressionWhen(tokens: []const Token, pos: *usize) bool {
    return parser.matchKeyword(tokens, pos, "when");
}

pub fn parseCaseExpressionThen(tokens: []const Token, pos: *usize) !void {
    try parser.expectKeyword(tokens, pos, "then");
}

pub fn parseCaseExpressionElse(tokens: []const Token, pos: *usize) !void {
    try parser.expectKeyword(tokens, pos, "else");
}

pub fn parseCaseExpressionEnd(tokens: []const Token, pos: *usize) !void {
    try parser.expectKeyword(tokens, pos, "end");
}

pub fn parseCastExpressionCallStart(tokens: []const Token, pos: *usize) !void {
    try parser.expectKeyword(tokens, pos, "cast");
    try parser.expectToken(tokens, pos, .lparen);
}

pub fn parseCastExpressionAs(tokens: []const Token, pos: *usize) !void {
    try parser.expectKeyword(tokens, pos, "as");
}

pub fn parseDatePartFunctionCallStart(tokens: []const Token, pos: *usize) !bool {
    const extract_syntax = parser.matchKeyword(tokens, pos, "extract");
    if (!extract_syntax and !parser.matchKeyword(tokens, pos, "date_part")) return error.UnsupportedSqlShape;
    try parser.expectToken(tokens, pos, .lparen);
    return extract_syntax;
}

pub fn parseDatePartExtractSeparator(tokens: []const Token, pos: *usize) !void {
    try parser.expectKeyword(tokens, pos, "from");
}

pub fn functionCallStartsAt(tokens: []const Token, index: usize, keyword: []const u8) bool {
    var pos: usize = 0;
    return parser.Cursor.init(tokens, &pos).functionCallStartsAt(index, keyword);
}

pub fn functionCallStartsAtTag(tokens: []const Token, index: usize, keyword: parser.TokenKeyword) bool {
    var pos: usize = 0;
    return parser.Cursor.init(tokens, &pos).functionCallStartsAtTag(index, keyword);
}

pub fn functionCallStartsAtIf(
    tokens: []const Token,
    index: usize,
    comptime predicate: fn ([]const u8) bool,
) bool {
    var pos: usize = 0;
    return parser.Cursor.init(tokens, &pos).functionCallStartsAtIf(index, predicate);
}

pub fn functionCallStartsAtTokenIf(
    tokens: []const Token,
    index: usize,
    comptime predicate: fn (Token) bool,
) bool {
    if (index + 1 >= tokens.len) return false;
    const token = tokens[index];
    return token.kind == .identifier and predicate(token) and tokens[index + 1].kind == .lparen;
}

pub fn peekFunctionCall(tokens: []const Token, pos: usize, keyword: []const u8) bool {
    return functionCallStartsAt(tokens, pos, keyword);
}

pub fn peekFunctionCallTag(tokens: []const Token, pos: usize, keyword: parser.TokenKeyword) bool {
    return functionCallStartsAtTag(tokens, pos, keyword);
}

pub fn peekFunctionCallIf(
    tokens: []const Token,
    pos: usize,
    comptime predicate: fn ([]const u8) bool,
) bool {
    return functionCallStartsAtIf(tokens, pos, predicate);
}

pub fn peekFunctionCallTokenIf(
    tokens: []const Token,
    pos: usize,
    comptime predicate: fn (Token) bool,
) bool {
    return functionCallStartsAtTokenIf(tokens, pos, predicate);
}

pub fn peekCaseFoldFunctionCall(tokens: []const Token, pos: usize) bool {
    return peekFunctionCallTag(tokens, pos, .lower) or
        peekFunctionCallTag(tokens, pos, .upper) or
        peekFunctionCallTokenIf(tokens, pos, sqlTokenIsInitcapFunction) or
        peekFunctionCallTag(tokens, pos, .trim) or
        peekFunctionCallTokenIf(tokens, pos, sqlTokenIsTrimVariantFunction);
}

pub fn peekCaseExpressionSyntax(tokens: []const Token, pos: usize) bool {
    return parser.peekKeyword(tokens, pos, "case");
}

pub fn peekCastExpressionSyntax(tokens: []const Token, pos: usize) bool {
    return peekFunctionCallTag(tokens, pos, .cast);
}

pub fn peekBooleanNotExpressionSyntax(tokens: []const Token, pos: usize) bool {
    return parser.peekKeywordTag(tokens, pos, .not);
}

pub fn peekUnaryNegativeExpressionSyntax(tokens: []const Token, pos: usize) bool {
    return pos < tokens.len and tokens[pos].kind == .minus;
}

pub fn peekParenthesizedExpressionSyntax(tokens: []const Token, pos: usize) bool {
    return pos < tokens.len and tokens[pos].kind == .lparen;
}

pub fn peekReplaceFunctionCall(tokens: []const Token, pos: usize) bool {
    return peekFunctionCallTag(tokens, pos, .replace);
}

pub fn peekRegexpReplaceFunctionCall(tokens: []const Token, pos: usize) bool {
    return peekFunctionCallTag(tokens, pos, .regexp_replace);
}

pub fn peekConcatFunctionCall(tokens: []const Token, pos: usize) bool {
    return peekFunctionCallTag(tokens, pos, .concat) or
        peekFunctionCallTag(tokens, pos, .concat_ws);
}

pub fn peekCoalesceFunctionCall(tokens: []const Token, pos: usize) bool {
    return peekFunctionCallTag(tokens, pos, .coalesce);
}

pub fn peekNullifFunctionCall(tokens: []const Token, pos: usize) bool {
    return peekFunctionCallTag(tokens, pos, .nullif);
}

pub fn peekArrayElementTransformFunctionCall(tokens: []const Token, pos: usize) bool {
    return peekFunctionCallTag(tokens, pos, .array_append) or
        peekFunctionCallTag(tokens, pos, .array_prepend) or
        peekFunctionCallTag(tokens, pos, .array_cat) or
        peekFunctionCallTag(tokens, pos, .array_remove) or
        peekFunctionCallTag(tokens, pos, .array_replace);
}

pub fn peekArrayToStringFunctionCall(tokens: []const Token, pos: usize) bool {
    return peekFunctionCallTag(tokens, pos, .array_to_string);
}

pub fn peekStringToArrayFunctionCall(tokens: []const Token, pos: usize) bool {
    return peekFunctionCallTag(tokens, pos, .string_to_array);
}

pub fn peekFixedUnaryFunctionCall(tokens: []const Token, pos: usize, kind: db_mod.types.RelationalRowsExpressionKind) bool {
    const keyword = fixedUnaryFunctionKeyword(kind) orelse return false;
    return peekFunctionCall(tokens, pos, keyword);
}

pub fn peekFixedBinaryFunctionCall(tokens: []const Token, pos: usize, kind: db_mod.types.RelationalRowsExpressionKind) bool {
    const keyword = fixedBinaryFunctionKeyword(kind) orelse return false;
    return peekFunctionCall(tokens, pos, keyword);
}

pub fn peekGreatestLeastFunctionCall(tokens: []const Token, pos: usize) bool {
    return peekFunctionCallTag(tokens, pos, .greatest) or
        peekFunctionCallTag(tokens, pos, .least);
}

pub fn peekPositionFunctionSyntax(tokens: []const Token, pos: usize) bool {
    return peekFunctionCallTag(tokens, pos, .position);
}

pub fn peekSqlNowExpressionSyntax(tokens: []const Token, pos: usize) bool {
    return peekFunctionCallTag(tokens, pos, .now) or
        parser.peekKeywordTag(tokens, pos, .current_timestamp);
}

pub fn peekSqlCurrentDateExpressionSyntax(tokens: []const Token, pos: usize) bool {
    return parser.peekKeywordTag(tokens, pos, .current_date);
}

pub fn peekSqlIntervalExpressionSyntax(tokens: []const Token, pos: usize) bool {
    return parser.peekKeywordTag(tokens, pos, .interval);
}

pub fn peekTextLengthFunctionKeyword(tokens: []const Token, pos: usize) bool {
    if (pos >= tokens.len) return false;
    const token = tokens[pos];
    return token.kind == .identifier and
        (sqlTokenIsLengthFunction(token) or
            sqlTokenIsOctetLengthFunction(token) or
            sqlTokenIsBitLengthFunction(token));
}

pub fn peekSubstringFunctionKeyword(tokens: []const Token, pos: usize) bool {
    if (pos >= tokens.len) return false;
    const token = tokens[pos];
    return token.kind == .identifier and sqlTokenIsSubstringFunction(token);
}

pub fn peekSplitPartFunctionKeyword(tokens: []const Token, pos: usize) bool {
    if (pos >= tokens.len) return false;
    const token = tokens[pos];
    return token.kind == .identifier and sqlTokenIsSplitPartFunction(token);
}

pub fn peekStrposFunctionKeyword(tokens: []const Token, pos: usize) bool {
    if (pos >= tokens.len) return false;
    const token = tokens[pos];
    return token.kind == .identifier and sqlTokenIsStrposFunction(token);
}

pub fn matchTextLengthFunctionKind(tokens: []const Token, pos: *usize) ?db_mod.types.RelationalRowsExpressionKind {
    if (pos.* >= tokens.len) return null;
    const token = tokens[pos.*];
    if (token.kind != .identifier) return null;
    const kind: db_mod.types.RelationalRowsExpressionKind = if (sqlTokenIsLengthFunction(token))
        .length
    else if (sqlTokenIsOctetLengthFunction(token))
        .octet_length
    else if (sqlTokenIsBitLengthFunction(token))
        .bit_length
    else
        return null;
    pos.* += 1;
    return kind;
}

pub fn parseTextLengthFunctionCallStart(tokens: []const Token, pos: *usize) !db_mod.types.RelationalRowsExpressionKind {
    const kind = matchTextLengthFunctionKind(tokens, pos) orelse return error.UnsupportedSqlShape;
    try parser.expectToken(tokens, pos, .lparen);
    return kind;
}

pub fn parseFixedUnaryFunctionCallStart(
    tokens: []const Token,
    pos: *usize,
    kind: db_mod.types.RelationalRowsExpressionKind,
) !void {
    const keyword = fixedUnaryFunctionKeyword(kind) orelse return error.UnsupportedSqlShape;
    try parser.expectKeyword(tokens, pos, keyword);
    try parser.expectToken(tokens, pos, .lparen);
}

pub fn parseFixedBinaryFunctionCallStart(
    tokens: []const Token,
    pos: *usize,
    kind: db_mod.types.RelationalRowsExpressionKind,
) !void {
    const keyword = fixedBinaryFunctionKeyword(kind) orelse return error.UnsupportedSqlShape;
    try parser.expectKeyword(tokens, pos, keyword);
    try parser.expectToken(tokens, pos, .lparen);
}

pub fn parseGreatestLeastFunctionCallStart(tokens: []const Token, pos: *usize) !db_mod.types.RelationalRowsExpressionKind {
    const cursor = parser.Cursor.init(tokens, pos);
    const kind: db_mod.types.RelationalRowsExpressionKind = if (cursor.matchKeyword("greatest"))
        .greatest
    else if (cursor.matchKeyword("least"))
        .least
    else
        return error.UnsupportedSqlShape;
    try cursor.expectToken(.lparen);
    return kind;
}

pub fn parseCaseFoldFunctionCallStart(tokens: []const Token, pos: *usize) !db_mod.types.RelationalRowsExpressionKind {
    const kind = matchCaseFoldFunctionKind(tokens, pos) orelse return error.UnsupportedSqlShape;
    try parser.expectToken(tokens, pos, .lparen);
    return kind;
}

pub fn matchCaseFoldFunctionKind(tokens: []const Token, pos: *usize) ?db_mod.types.RelationalRowsExpressionKind {
    if (pos.* >= tokens.len) return null;
    const token = tokens[pos.*];
    if (token.kind != .identifier) return null;
    const kind: db_mod.types.RelationalRowsExpressionKind = if (token.matchesKeywordTag(.lower))
        .lower
    else if (token.matchesKeywordTag(.upper))
        .upper
    else if (token.matchesKeywordTag(.initcap))
        .initcap
    else if (token.matchesKeywordTag(.trim))
        .trim
    else if (token.matchesKeywordTag(.ltrim))
        .ltrim
    else if (token.matchesKeywordTag(.rtrim))
        .rtrim
    else if (token.matchesKeywordTag(.btrim))
        .trim
    else
        return null;
    pos.* += 1;
    return kind;
}

pub fn parseConcatFunctionCallStart(tokens: []const Token, pos: *usize) !db_mod.types.RelationalRowsExpressionKind {
    const cursor = parser.Cursor.init(tokens, pos);
    const kind: db_mod.types.RelationalRowsExpressionKind = if (cursor.matchKeyword("concat_ws"))
        .concat_ws
    else if (cursor.matchKeyword("concat"))
        .concat
    else
        return error.UnsupportedSqlShape;
    try cursor.expectToken(.lparen);
    return kind;
}

pub fn parseCoalesceFunctionCallStart(tokens: []const Token, pos: *usize) !void {
    try parser.expectKeyword(tokens, pos, "coalesce");
    try parser.expectToken(tokens, pos, .lparen);
}

pub fn parseNullifFunctionCallStart(tokens: []const Token, pos: *usize) !void {
    try parser.expectKeyword(tokens, pos, "nullif");
    try parser.expectToken(tokens, pos, .lparen);
}

pub fn parseReplaceFunctionCallStart(tokens: []const Token, pos: *usize) !void {
    try parser.expectKeyword(tokens, pos, "replace");
    try parser.expectToken(tokens, pos, .lparen);
}

pub fn parseRegexpReplaceFunctionCallStart(tokens: []const Token, pos: *usize) !void {
    try parser.expectKeyword(tokens, pos, "regexp_replace");
    try parser.expectToken(tokens, pos, .lparen);
}

pub fn parseJsonArrayLengthFunctionCallStart(tokens: []const Token, pos: *usize) !void {
    _ = matchFunctionKeywordToken(tokens, pos, sqlTokenIsJsonArrayLengthFunction) orelse return error.UnsupportedSqlShape;
    try parser.expectToken(tokens, pos, .lparen);
}

pub fn parseJsonTypeofFunctionCallStart(tokens: []const Token, pos: *usize) !void {
    _ = matchFunctionKeywordToken(tokens, pos, sqlTokenIsJsonTypeofFunction) orelse return error.UnsupportedSqlShape;
    try parser.expectToken(tokens, pos, .lparen);
}

pub fn parseJsonBuildObjectFunctionCallStart(tokens: []const Token, pos: *usize) !void {
    _ = matchFunctionKeywordToken(tokens, pos, sqlTokenIsJsonBuildObjectFunction) orelse return error.UnsupportedSqlShape;
    try parser.expectToken(tokens, pos, .lparen);
}

pub fn parseArrayLengthFunctionCallStart(tokens: []const Token, pos: *usize) ![]const u8 {
    const keyword = (matchFunctionKeywordToken(tokens, pos, sqlTokenIsArrayLengthFunction) orelse return error.UnsupportedSqlShape).text;
    try parser.expectToken(tokens, pos, .lparen);
    return keyword;
}

pub fn parseArrayPositionFunctionCallStart(tokens: []const Token, pos: *usize) !db_mod.types.RelationalRowsExpressionKind {
    const token = matchFunctionKeywordToken(tokens, pos, sqlTokenIsArrayPositionFunction) orelse return error.UnsupportedSqlShape;
    try parser.expectToken(tokens, pos, .lparen);
    return if (token.matchesKeywordTag(.array_positions)) .array_positions else .array_position;
}

pub fn parseStringToArrayFunctionCallStart(tokens: []const Token, pos: *usize) !void {
    try parser.expectKeyword(tokens, pos, "string_to_array");
    try parser.expectToken(tokens, pos, .lparen);
}

pub fn parseArrayToStringFunctionCallStart(tokens: []const Token, pos: *usize) !void {
    try parser.expectKeyword(tokens, pos, "array_to_string");
    try parser.expectToken(tokens, pos, .lparen);
}

pub fn parseArrayElementTransformFunctionCallStart(tokens: []const Token, pos: *usize) !db_mod.types.RelationalRowsExpressionKind {
    const cursor = parser.Cursor.init(tokens, pos);
    const kind: db_mod.types.RelationalRowsExpressionKind = if (cursor.matchKeyword("array_append"))
        .array_append
    else if (cursor.matchKeyword("array_prepend"))
        .array_prepend
    else if (cursor.matchKeyword("array_cat"))
        .array_cat
    else if (cursor.matchKeyword("array_replace"))
        .array_replace
    else blk: {
        try cursor.expectKeyword("array_remove");
        break :blk .array_remove;
    };
    try cursor.expectToken(.lparen);
    return kind;
}

fn fixedUnaryFunctionKeyword(kind: db_mod.types.RelationalRowsExpressionKind) ?[]const u8 {
    return switch (kind) {
        .ascii => "ascii",
        .chr => "chr",
        .abs => "abs",
        .round => "round",
        .trunc => "trunc",
        .floor => "floor",
        .ceil => "ceil",
        .sqrt => "sqrt",
        .sign => "sign",
        .reverse => "reverse",
        .md5 => "md5",
        .to_jsonb => "to_jsonb",
        else => null,
    };
}

fn fixedBinaryFunctionKeyword(kind: db_mod.types.RelationalRowsExpressionKind) ?[]const u8 {
    return switch (kind) {
        .mod => "mod",
        .power => "power",
        else => null,
    };
}

pub fn rowExpressionBoundaryKeyword(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "as") or
        std.ascii.eqlIgnoreCase(text, "from") or
        std.ascii.eqlIgnoreCase(text, "where") or
        std.ascii.eqlIgnoreCase(text, "and") or
        std.ascii.eqlIgnoreCase(text, "or") or
        std.ascii.eqlIgnoreCase(text, "group") or
        std.ascii.eqlIgnoreCase(text, "having") or
        std.ascii.eqlIgnoreCase(text, "order") or
        std.ascii.eqlIgnoreCase(text, "limit") or
        std.ascii.eqlIgnoreCase(text, "offset") or
        std.ascii.eqlIgnoreCase(text, "fetch") or
        std.ascii.eqlIgnoreCase(text, "for") or
        std.ascii.eqlIgnoreCase(text, "asc") or
        std.ascii.eqlIgnoreCase(text, "desc") or
        std.ascii.eqlIgnoreCase(text, "nulls") or
        std.ascii.eqlIgnoreCase(text, "then") or
        std.ascii.eqlIgnoreCase(text, "else") or
        std.ascii.eqlIgnoreCase(text, "end") or
        std.ascii.eqlIgnoreCase(text, "when") or
        std.ascii.eqlIgnoreCase(text, "filter") or
        std.ascii.eqlIgnoreCase(text, "over");
}

pub fn rowExpressionBoundaryKeywordToken(token: Token) bool {
    return token.matchesKeywordTag(.as) or
        token.matchesKeywordTag(.from) or
        token.matchesKeywordTag(.where) or
        token.matchesKeywordTag(.@"and") or
        token.matchesKeywordTag(.@"or") or
        token.matchesKeywordTag(.group) or
        token.matchesKeywordTag(.having) or
        token.matchesKeywordTag(.order) or
        token.matchesKeywordTag(.limit) or
        token.matchesKeywordTag(.offset) or
        token.matchesKeywordTag(.fetch) or
        token.matchesKeywordTag(.@"for") or
        token.matchesKeywordTag(.asc) or
        token.matchesKeywordTag(.desc) or
        token.matchesKeywordTag(.nulls) or
        token.matchesKeywordTag(.then) or
        token.matchesKeywordTag(.@"else") or
        token.matchesKeywordTag(.end) or
        token.matchesKeywordTag(.when) or
        token.matchesKeywordTag(.filter) or
        token.matchesKeywordTag(.over);
}

pub fn sqlWhereTailClauseKeyword(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "order") or
        std.ascii.eqlIgnoreCase(text, "limit") or
        std.ascii.eqlIgnoreCase(text, "offset") or
        std.ascii.eqlIgnoreCase(text, "for") or
        std.ascii.eqlIgnoreCase(text, "group") or
        std.ascii.eqlIgnoreCase(text, "having") or
        std.ascii.eqlIgnoreCase(text, "returning") or
        std.ascii.eqlIgnoreCase(text, "join") or
        std.ascii.eqlIgnoreCase(text, "left") or
        std.ascii.eqlIgnoreCase(text, "inner") or
        std.ascii.eqlIgnoreCase(text, "with") or
        std.ascii.eqlIgnoreCase(text, "over") or
        std.ascii.eqlIgnoreCase(text, "lateral");
}

pub fn sqlWhereTailClauseKeywordToken(token: Token) bool {
    return token.matchesKeywordTag(.order) or
        token.matchesKeywordTag(.limit) or
        token.matchesKeywordTag(.offset) or
        token.matchesKeywordTag(.@"for") or
        token.matchesKeywordTag(.group) or
        token.matchesKeywordTag(.having) or
        token.matchesKeywordTag(.returning) or
        token.matchesKeywordTag(.join) or
        token.matchesKeywordTag(.left) or
        token.matchesKeywordTag(.inner) or
        token.matchesKeywordTag(.with) or
        token.matchesKeywordTag(.over) or
        token.matchesKeywordTag(.lateral);
}

pub fn sqlWindowTailClauseKeyword(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "order") or
        std.ascii.eqlIgnoreCase(text, "limit") or
        std.ascii.eqlIgnoreCase(text, "offset") or
        std.ascii.eqlIgnoreCase(text, "fetch") or
        std.ascii.eqlIgnoreCase(text, "for");
}

pub fn sqlWindowTailClauseKeywordToken(token: Token) bool {
    return token.matchesKeywordTag(.order) or
        token.matchesKeywordTag(.limit) or
        token.matchesKeywordTag(.offset) or
        token.matchesKeywordTag(.fetch) or
        token.matchesKeywordTag(.@"for");
}

pub fn parenthesizedPredicateGroupCanStartAt(tokens: []const Token, index: usize) bool {
    if (index >= tokens.len or tokens[index].kind != .lparen) return false;
    const close = parser.findMatchingRParenIndex(tokens, index) orelse return false;
    if (close + 1 >= tokens.len) return true;
    return !tokenContinuesScalarExpressionPredicate(tokens, close + 1);
}

fn tokenContinuesScalarExpressionPredicate(tokens: []const Token, index: usize) bool {
    const token = tokens[index];
    switch (token.kind) {
        .eq, .neq, .gt, .gte, .lt, .lte, .arrow_text, .arrow_json, .path_arrow_text, .path_arrow_json => return true,
        .identifier => {
            if (token.matchesKeywordTag(.is) or
                token.matchesKeywordTag(.like) or
                token.matchesKeywordTag(.ilike) or
                token.matchesKeywordTag(.in) or
                token.matchesKeywordTag(.between))
            {
                return true;
            }
            if (token.matchesKeywordTag(.not) and index + 1 < tokens.len and tokens[index + 1].kind == .identifier) {
                const after_not = tokens[index + 1];
                if (after_not.matchesKeywordTag(.like) or
                    after_not.matchesKeywordTag(.ilike) or
                    after_not.matchesKeywordTag(.in) or
                    after_not.matchesKeywordTag(.between))
                {
                    return true;
                }
            }
        },
        else => {},
    }
    return false;
}

test "sql expr token predicates classify function and tail tokens" {
    try std.testing.expect(sqlKeywordIsAnyOrSome("SOME"));
    try std.testing.expect(sqlKeywordStartsScalarPredicate("between"));
    try std.testing.expect(sqlKeywordIsArrayLengthFunction("cardinality"));
    try std.testing.expectEqualStrings("cardinality", arrayLengthDefaultOutput("cardinality"));
    try std.testing.expect(sqlKeywordIsJsonExtractPathFunction("jsonb_extract_path_text"));
    try std.testing.expect(sqlJsonExtractPathFunctionAsText("json_extract_path_text"));
    try std.testing.expect(sqlKeywordIsRegexpMatchFunction("regexp_like"));
    try std.testing.expect(rowExpressionBoundaryKeyword("returning") == false);
    try std.testing.expect(sqlWhereTailClauseKeyword("returning"));
    try std.testing.expect(sqlWindowTailClauseKeyword("fetch"));
    try std.testing.expect(rowExpressionBoundaryKeywordToken(.{ .kind = .identifier, .text = "THEN", .keyword = .then }));
    try std.testing.expect(rowExpressionBoundaryKeywordToken(.{ .kind = .identifier, .text = "ELSE", .keyword = .@"else" }));
    try std.testing.expect(rowExpressionBoundaryKeywordToken(.{ .kind = .identifier, .text = "END", .keyword = .end }));
}
