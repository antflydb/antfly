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

const lexer = @import("lexer.zig");
const parser = @import("parser.zig");
const token_mod = @import("token.zig");

pub const Token = token_mod.Token;

pub const RowSecurityAlterSyntax = struct {
    table_identifier: []const u8,
    enabled: bool,
};

pub const AdapterNoopTransactionBoundaryTail = struct {
    work: bool = false,
    transaction: bool = false,
};

pub const SavepointNameSyntax = struct {
    savepoint_name: []const u8,
};

pub fn parseAlterRowSecurity(tokens: []const Token, pos: *usize) !?RowSecurityAlterSyntax {
    const start = pos.*;
    var cursor = parser.Cursor.init(tokens, pos);
    if (!cursor.matchKeyword("table")) return null;
    const table_token = cursor.matchToken(.identifier) orelse {
        pos.* = start;
        return error.UnsupportedSqlShape;
    };
    const enabled = if (cursor.matchKeyword("enable"))
        true
    else if (cursor.matchKeyword("disable"))
        false
    else {
        pos.* = start;
        return null;
    };
    try cursor.expectKeyword("row");
    try cursor.expectKeyword("level");
    try cursor.expectKeyword("security");
    if (cursor.matchToken(.semicolon) != null and !cursor.atEnd()) return error.UnsupportedSqlShape;
    if (!cursor.atEnd()) return error.UnsupportedSqlShape;
    return .{ .table_identifier = table_token.text, .enabled = enabled };
}

pub fn parseAdapterNoopSetStatementTail(tokens: []const Token, pos: *usize) !void {
    var cursor = parser.Cursor.init(tokens, pos);
    if (!cursor.matchKeyword("local")) _ = cursor.matchKeyword("session");

    const setting = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
    if (std.ascii.eqlIgnoreCase(setting.text, "search_path")) {
        try parseAdapterNoopPublicSearchPathTail(cursor);
        return;
    }
    if (!adapterNoopSetSessionSettingAllowed(setting.text)) return error.UnsupportedSqlShape;

    if (cursor.matchToken(.eq) == null and !cursor.matchKeyword("to")) return error.UnsupportedSqlShape;
    try parseAdapterNoopSetValueTail(cursor, setting.text);
}

pub fn parseAdapterNoopResetStatementTail(tokens: []const Token, pos: *usize) !void {
    var cursor = parser.Cursor.init(tokens, pos);
    if (cursor.matchKeyword("all")) {
        try parseAdapterNoopStatementEnd(cursor);
        return;
    }

    const setting = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
    if (!adapterNoopResetSessionSettingAllowed(setting.text)) return error.UnsupportedSqlShape;
    try parseAdapterNoopStatementEnd(cursor);
}

pub fn parseAdapterNoopShowStatementTail(tokens: []const Token, pos: *usize) !void {
    var cursor = parser.Cursor.init(tokens, pos);
    const setting = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
    if (!adapterNoopShowSessionSettingAllowed(setting.text)) return error.UnsupportedSqlShape;
    try parseAdapterNoopStatementEnd(cursor);
}

pub fn parseAdapterNoopDiscardStatementTail(tokens: []const Token, pos: *usize) !void {
    var cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("all");
    try parseAdapterNoopStatementEnd(cursor);
}

pub fn matchAdapterNoopTransactionBoundaryTail(
    tokens: []const Token,
    pos: *usize,
    options: AdapterNoopTransactionBoundaryTail,
) !bool {
    var cursor = parser.Cursor.init(tokens, pos);
    const checkpoint = cursor.checkpoint();
    if (try matchAdapterNoopStatementEnd(cursor)) return true;
    if ((options.work and cursor.matchKeyword("work")) or
        (options.transaction and cursor.matchKeyword("transaction")))
    {
        if (try matchAdapterNoopStatementEnd(cursor)) return true;
    }
    cursor.restore(checkpoint);
    return false;
}

pub fn parseSavepointTransactionTail(tokens: []const Token, pos: *usize) !SavepointNameSyntax {
    return try parseSavepointNameTail(tokens, pos);
}

pub fn parseReleaseSavepointTail(tokens: []const Token, pos: *usize) !SavepointNameSyntax {
    var cursor = parser.Cursor.init(tokens, pos);
    _ = cursor.matchKeyword("savepoint");
    return try parseSavepointNameTailFromCursor(cursor);
}

pub fn parseRollbackToSavepointTail(tokens: []const Token, pos: *usize) !SavepointNameSyntax {
    var cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("to");
    _ = cursor.matchKeyword("savepoint");
    return try parseSavepointNameTailFromCursor(cursor);
}

fn parseAdapterNoopPublicSearchPathTail(cursor: parser.Cursor) !void {
    if (cursor.matchToken(.eq) == null and !cursor.matchKeyword("to")) return error.UnsupportedSqlShape;
    const path = cursor.matchToken(.identifier) orelse cursor.matchToken(.string) orelse return error.UnsupportedSqlShape;
    if (!std.ascii.eqlIgnoreCase(path.text, "public")) return error.UnsupportedSqlShape;
    try parseAdapterNoopStatementEnd(cursor);
}

fn parseAdapterNoopSetValueTail(cursor: parser.Cursor, setting: []const u8) !void {
    const value = cursor.matchToken(.identifier) orelse cursor.matchToken(.string) orelse cursor.matchToken(.number) orelse return error.UnsupportedSqlShape;
    if (!adapterNoopSetSessionSettingValueAllowed(setting, value.text)) return error.UnsupportedSqlShape;
    try parseAdapterNoopStatementEnd(cursor);
}

fn parseAdapterNoopStatementEnd(cursor: parser.Cursor) !void {
    if (!cursor.atEnd() and !cursor.peekKind(.semicolon)) return error.UnsupportedSqlShape;
    if (cursor.matchToken(.semicolon) != null and !cursor.atEnd()) return error.UnsupportedSqlShape;
    if (!cursor.atEnd()) return error.UnsupportedSqlShape;
}

fn matchAdapterNoopStatementEnd(cursor: parser.Cursor) !bool {
    if (cursor.matchToken(.semicolon) != null) {
        if (!cursor.atEnd()) return error.UnsupportedSqlShape;
        return true;
    }
    return cursor.atEnd();
}

fn parseSavepointNameTail(tokens: []const Token, pos: *usize) !SavepointNameSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    return try parseSavepointNameTailFromCursor(cursor);
}

fn parseSavepointNameTailFromCursor(cursor: parser.Cursor) !SavepointNameSyntax {
    const name = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
    try parseAdapterNoopStatementEnd(cursor);
    return .{ .savepoint_name = name.text };
}

fn adapterNoopSetSessionSettingAllowed(setting: []const u8) bool {
    return std.ascii.eqlIgnoreCase(setting, "client_encoding") or
        std.ascii.eqlIgnoreCase(setting, "standard_conforming_strings") or
        std.ascii.eqlIgnoreCase(setting, "check_function_bodies") or
        std.ascii.eqlIgnoreCase(setting, "xmloption") or
        std.ascii.eqlIgnoreCase(setting, "client_min_messages");
}

fn adapterNoopResetSessionSettingAllowed(setting: []const u8) bool {
    return adapterNoopSetSessionSettingAllowed(setting);
}

fn adapterNoopShowSessionSettingAllowed(setting: []const u8) bool {
    return adapterNoopSetSessionSettingAllowed(setting) or std.ascii.eqlIgnoreCase(setting, "search_path");
}

fn adapterNoopSetSessionSettingValueAllowed(setting: []const u8, value: []const u8) bool {
    if (std.ascii.eqlIgnoreCase(setting, "client_encoding")) {
        return std.ascii.eqlIgnoreCase(value, "UTF8") or std.ascii.eqlIgnoreCase(value, "UTF-8");
    }
    if (std.ascii.eqlIgnoreCase(setting, "standard_conforming_strings")) {
        return std.ascii.eqlIgnoreCase(value, "on") or std.ascii.eqlIgnoreCase(value, "true");
    }
    if (std.ascii.eqlIgnoreCase(setting, "check_function_bodies")) {
        return std.ascii.eqlIgnoreCase(value, "off") or std.ascii.eqlIgnoreCase(value, "false");
    }
    if (std.ascii.eqlIgnoreCase(setting, "xmloption")) {
        return std.ascii.eqlIgnoreCase(value, "content");
    }
    if (std.ascii.eqlIgnoreCase(setting, "client_min_messages")) {
        return std.ascii.eqlIgnoreCase(value, "warning") or
            std.ascii.eqlIgnoreCase(value, "notice") or
            std.ascii.eqlIgnoreCase(value, "error");
    }
    return false;
}

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

pub fn sqlAssignmentTailKeyword(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "where") or
        std.ascii.eqlIgnoreCase(text, "from") or
        std.ascii.eqlIgnoreCase(text, "returning") or
        std.ascii.eqlIgnoreCase(text, "order") or
        std.ascii.eqlIgnoreCase(text, "limit") or
        std.ascii.eqlIgnoreCase(text, "offset") or
        std.ascii.eqlIgnoreCase(text, "for");
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

pub fn sqlWindowTailClauseKeyword(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "order") or
        std.ascii.eqlIgnoreCase(text, "limit") or
        std.ascii.eqlIgnoreCase(text, "offset") or
        std.ascii.eqlIgnoreCase(text, "fetch") or
        std.ascii.eqlIgnoreCase(text, "for");
}

test "sql adapter grammar parses alter row security" {
    const alloc = std.testing.allocator;
    var tokens = try lexer.tokenizeAlloc(alloc, "TABLE public.usage_records DISABLE ROW LEVEL SECURITY;");
    defer lexer.freeTokens(alloc, &tokens);

    var pos: usize = 0;
    const syntax = (try parseAlterRowSecurity(tokens.items, &pos)) orelse return error.TestUnexpectedResult;
    try std.testing.expect(!syntax.enabled);
    try std.testing.expectEqualStrings("public.usage_records", syntax.table_identifier);
    try std.testing.expectEqual(tokens.items.len, pos);
}

test "sql adapter grammar leaves non row security alter table to ddl parser" {
    const alloc = std.testing.allocator;
    var tokens = try lexer.tokenizeAlloc(alloc, "TABLE usage_records ADD COLUMN status text;");
    defer lexer.freeTokens(alloc, &tokens);

    var pos: usize = 0;
    try std.testing.expect((try parseAlterRowSecurity(tokens.items, &pos)) == null);
    try std.testing.expectEqual(@as(usize, 0), pos);
}

test "sql adapter grammar accepts allowlisted adapter session cleanup" {
    const alloc = std.testing.allocator;

    var set_tokens = try lexer.tokenizeAlloc(alloc, "LOCAL client_min_messages = warning;");
    defer lexer.freeTokens(alloc, &set_tokens);
    var set_pos: usize = 0;
    try parseAdapterNoopSetStatementTail(set_tokens.items, &set_pos);
    try std.testing.expectEqual(set_tokens.items.len, set_pos);

    var search_path_tokens = try lexer.tokenizeAlloc(alloc, "search_path TO public;");
    defer lexer.freeTokens(alloc, &search_path_tokens);
    var search_path_pos: usize = 0;
    try parseAdapterNoopSetStatementTail(search_path_tokens.items, &search_path_pos);
    try std.testing.expectEqual(search_path_tokens.items.len, search_path_pos);

    var reset_tokens = try lexer.tokenizeAlloc(alloc, "client_min_messages;");
    defer lexer.freeTokens(alloc, &reset_tokens);
    var reset_pos: usize = 0;
    try parseAdapterNoopResetStatementTail(reset_tokens.items, &reset_pos);
    try std.testing.expectEqual(reset_tokens.items.len, reset_pos);

    var show_tokens = try lexer.tokenizeAlloc(alloc, "search_path;");
    defer lexer.freeTokens(alloc, &show_tokens);
    var show_pos: usize = 0;
    try parseAdapterNoopShowStatementTail(show_tokens.items, &show_pos);
    try std.testing.expectEqual(show_tokens.items.len, show_pos);

    var discard_tokens = try lexer.tokenizeAlloc(alloc, "ALL;");
    defer lexer.freeTokens(alloc, &discard_tokens);
    var discard_pos: usize = 0;
    try parseAdapterNoopDiscardStatementTail(discard_tokens.items, &discard_pos);
    try std.testing.expectEqual(discard_tokens.items.len, discard_pos);
}

test "sql adapter grammar rejects semantic session changes as noops" {
    const alloc = std.testing.allocator;

    var tenant_path_tokens = try lexer.tokenizeAlloc(alloc, "search_path TO tenant_schema;");
    defer lexer.freeTokens(alloc, &tenant_path_tokens);
    var tenant_path_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseAdapterNoopSetStatementTail(tenant_path_tokens.items, &tenant_path_pos));

    var latin1_tokens = try lexer.tokenizeAlloc(alloc, "client_encoding = 'LATIN1';");
    defer lexer.freeTokens(alloc, &latin1_tokens);
    var latin1_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseAdapterNoopSetStatementTail(latin1_tokens.items, &latin1_pos));

    var timeout_tokens = try lexer.tokenizeAlloc(alloc, "statement_timeout = '1ms';");
    defer lexer.freeTokens(alloc, &timeout_tokens);
    var timeout_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseAdapterNoopSetStatementTail(timeout_tokens.items, &timeout_pos));

    var show_all_tokens = try lexer.tokenizeAlloc(alloc, "ALL;");
    defer lexer.freeTokens(alloc, &show_all_tokens);
    var show_all_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseAdapterNoopShowStatementTail(show_all_tokens.items, &show_all_pos));

    var discard_temp_tokens = try lexer.tokenizeAlloc(alloc, "TEMP;");
    defer lexer.freeTokens(alloc, &discard_temp_tokens);
    var discard_temp_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseAdapterNoopDiscardStatementTail(discard_temp_tokens.items, &discard_temp_pos));
}

test "sql adapter grammar matches transaction boundary noops" {
    const alloc = std.testing.allocator;

    var bare_tokens = try lexer.tokenizeAlloc(alloc, ";");
    defer lexer.freeTokens(alloc, &bare_tokens);
    var bare_pos: usize = 0;
    try std.testing.expect(try matchAdapterNoopTransactionBoundaryTail(bare_tokens.items, &bare_pos, .{}));
    try std.testing.expectEqual(bare_tokens.items.len, bare_pos);

    var work_tokens = try lexer.tokenizeAlloc(alloc, "WORK;");
    defer lexer.freeTokens(alloc, &work_tokens);
    var work_pos: usize = 0;
    try std.testing.expect(try matchAdapterNoopTransactionBoundaryTail(work_tokens.items, &work_pos, .{ .work = true }));
    try std.testing.expectEqual(work_tokens.items.len, work_pos);

    var transaction_tokens = try lexer.tokenizeAlloc(alloc, "TRANSACTION;");
    defer lexer.freeTokens(alloc, &transaction_tokens);
    var transaction_pos: usize = 0;
    try std.testing.expect(try matchAdapterNoopTransactionBoundaryTail(transaction_tokens.items, &transaction_pos, .{ .transaction = true }));
    try std.testing.expectEqual(transaction_tokens.items.len, transaction_pos);

    var prepared_tokens = try lexer.tokenizeAlloc(alloc, "PREPARED 'x';");
    defer lexer.freeTokens(alloc, &prepared_tokens);
    var prepared_pos: usize = 0;
    try std.testing.expect(!try matchAdapterNoopTransactionBoundaryTail(prepared_tokens.items, &prepared_pos, .{ .work = true, .transaction = true }));
    try std.testing.expectEqual(@as(usize, 0), prepared_pos);

    var extra_tokens = try lexer.tokenizeAlloc(alloc, "; SELECT 1");
    defer lexer.freeTokens(alloc, &extra_tokens);
    var extra_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, matchAdapterNoopTransactionBoundaryTail(extra_tokens.items, &extra_pos, .{}));
}

test "sql adapter grammar parses savepoint transaction tails" {
    const alloc = std.testing.allocator;

    var savepoint_tokens = try lexer.tokenizeAlloc(alloc, "before_retry;");
    defer lexer.freeTokens(alloc, &savepoint_tokens);
    var savepoint_pos: usize = 0;
    const savepoint = try parseSavepointTransactionTail(savepoint_tokens.items, &savepoint_pos);
    try std.testing.expectEqualStrings("before_retry", savepoint.savepoint_name);
    try std.testing.expectEqual(savepoint_tokens.items.len, savepoint_pos);

    var release_tokens = try lexer.tokenizeAlloc(alloc, "SAVEPOINT before_retry;");
    defer lexer.freeTokens(alloc, &release_tokens);
    var release_pos: usize = 0;
    const release = try parseReleaseSavepointTail(release_tokens.items, &release_pos);
    try std.testing.expectEqualStrings("before_retry", release.savepoint_name);
    try std.testing.expectEqual(release_tokens.items.len, release_pos);

    var release_shorthand_tokens = try lexer.tokenizeAlloc(alloc, "before_retry;");
    defer lexer.freeTokens(alloc, &release_shorthand_tokens);
    var release_shorthand_pos: usize = 0;
    const release_shorthand = try parseReleaseSavepointTail(release_shorthand_tokens.items, &release_shorthand_pos);
    try std.testing.expectEqualStrings("before_retry", release_shorthand.savepoint_name);
    try std.testing.expectEqual(release_shorthand_tokens.items.len, release_shorthand_pos);

    var rollback_tokens = try lexer.tokenizeAlloc(alloc, "TO SAVEPOINT before_retry;");
    defer lexer.freeTokens(alloc, &rollback_tokens);
    var rollback_pos: usize = 0;
    const rollback = try parseRollbackToSavepointTail(rollback_tokens.items, &rollback_pos);
    try std.testing.expectEqualStrings("before_retry", rollback.savepoint_name);
    try std.testing.expectEqual(rollback_tokens.items.len, rollback_pos);

    var rollback_shorthand_tokens = try lexer.tokenizeAlloc(alloc, "TO before_retry;");
    defer lexer.freeTokens(alloc, &rollback_shorthand_tokens);
    var rollback_shorthand_pos: usize = 0;
    const rollback_shorthand = try parseRollbackToSavepointTail(rollback_shorthand_tokens.items, &rollback_shorthand_pos);
    try std.testing.expectEqualStrings("before_retry", rollback_shorthand.savepoint_name);
    try std.testing.expectEqual(rollback_shorthand_tokens.items.len, rollback_shorthand_pos);

    var extra_tokens = try lexer.tokenizeAlloc(alloc, "before_retry RELEASE;");
    defer lexer.freeTokens(alloc, &extra_tokens);
    var extra_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseSavepointTransactionTail(extra_tokens.items, &extra_pos));
}
