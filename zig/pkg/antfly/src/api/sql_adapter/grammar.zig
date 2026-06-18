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

const ast = @import("ast.zig");
const db_mod = @import("../../storage/db/mod.zig");
const lexer = @import("lexer.zig");
const lower_expr = @import("lower_expr.zig");
const parser = @import("parser.zig");
const token_mod = @import("token.zig");

pub const Token = token_mod.Token;

pub const SqlExplainPrefix = ast.SqlExplainPrefix;

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

pub const RowClaimSyntax = struct {
    clause: ast.SqlRowClaimClause,
    targets: []const []const u8 = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.targets) |target| alloc.free(@constCast(target));
        if (self.targets.len > 0) alloc.free(self.targets);
        self.* = undefined;
    }
};

pub const NamedOrAllSyntax = struct {
    name: ?[]const u8 = null,
    all: bool = false,
};

pub const RelationPopulationMode = enum {
    create_table_as,
    select_into,
};

pub const RelationLifetimeKind = enum {
    temporary,
    unlogged,
};

pub const RelationPopulationSyntax = struct {
    mode: RelationPopulationMode,
    target_identifier: []const u8,
    target_lifetime: ?RelationLifetimeKind = null,
    if_not_exists: bool = false,
    source_sql: []u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.source_sql);
        self.* = undefined;
    }
};

pub fn parseExplainPrefix(sql: []const u8) !SqlExplainPrefix {
    var index = skipSqlWhitespace(sql, 0);
    if (!consumeSqlKeyword(sql, &index, "explain")) return error.UnsupportedSqlShape;
    index = skipSqlWhitespace(sql, index);
    if (index >= sql.len) return error.UnsupportedSqlShape;

    var prefix = SqlExplainPrefix{ .inner_sql = "" };
    if (sql[index] == '(') {
        try parseExplainOptions(sql, &index, &prefix);
        index = skipSqlWhitespace(sql, index);
        if (index >= sql.len) return error.UnsupportedSqlShape;
    }

    if (consumeSqlKeyword(sql, &index, "analyze")) {
        prefix.analyze = true;
        index = skipSqlWhitespace(sql, index);
        if (index >= sql.len) return error.UnsupportedSqlShape;
    }

    const inner = std.mem.trim(u8, sql[index..], " \t\r\n;");
    if (inner.len == 0) return error.UnsupportedSqlShape;
    prefix.inner_sql = inner;
    return prefix;
}

fn parseExplainOptions(sql: []const u8, index: *usize, prefix: *SqlExplainPrefix) !void {
    if (index.* >= sql.len or sql[index.*] != '(') return error.UnsupportedSqlShape;
    index.* += 1;
    while (true) {
        index.* = skipSqlWhitespace(sql, index.*);
        if (index.* >= sql.len) return error.UnsupportedSqlShape;
        if (consumeSqlKeyword(sql, index, "format")) {
            index.* = skipSqlWhitespace(sql, index.*);
            if (consumeSqlKeyword(sql, index, "json")) {
                prefix.format = .json;
            } else if (consumeSqlKeyword(sql, index, "text")) {
                prefix.format = .text;
            } else {
                return error.UnsupportedSqlShape;
            }
        } else if (consumeSqlKeyword(sql, index, "verbose")) {
            prefix.verbose = try parseOptionalExplainBool(sql, index, true);
        } else if (consumeSqlKeyword(sql, index, "costs")) {
            prefix.costs = try parseOptionalExplainBool(sql, index, true);
        } else if (consumeSqlKeyword(sql, index, "analyze")) {
            prefix.analyze = try parseOptionalExplainBool(sql, index, true);
        } else {
            return error.UnsupportedSqlShape;
        }

        index.* = skipSqlWhitespace(sql, index.*);
        if (index.* >= sql.len) return error.UnsupportedSqlShape;
        if (sql[index.*] == ',') {
            index.* += 1;
            continue;
        }
        if (sql[index.*] == ')') {
            index.* += 1;
            return;
        }
        return error.UnsupportedSqlShape;
    }
}

fn parseOptionalExplainBool(sql: []const u8, index: *usize, default_value: bool) !bool {
    const before = index.*;
    index.* = skipSqlWhitespace(sql, index.*);
    if (consumeSqlKeyword(sql, index, "true") or
        consumeSqlKeyword(sql, index, "on") or
        consumeSqlKeyword(sql, index, "yes"))
    {
        return true;
    }
    if (consumeSqlKeyword(sql, index, "false") or
        consumeSqlKeyword(sql, index, "off") or
        consumeSqlKeyword(sql, index, "no"))
    {
        return false;
    }
    index.* = before;
    return default_value;
}

fn skipSqlWhitespace(sql: []const u8, start: usize) usize {
    var index = start;
    while (index < sql.len and std.ascii.isWhitespace(sql[index])) : (index += 1) {}
    return index;
}

fn consumeSqlKeyword(sql: []const u8, index: *usize, keyword: []const u8) bool {
    const start = index.*;
    const end = start + keyword.len;
    if (end > sql.len) return false;
    if (!std.ascii.eqlIgnoreCase(sql[start..end], keyword)) return false;
    if (end < sql.len and isSqlIdentifierByte(sql[end])) return false;
    if (start > 0 and isSqlIdentifierByte(sql[start - 1])) return false;
    index.* = end;
    return true;
}

fn isSqlIdentifierByte(byte: u8) bool {
    return std.ascii.isAlphanumeric(byte) or byte == '_';
}

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

pub fn parseForRowClaimClauseAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !RowClaimSyntax {
    var cursor = parser.Cursor.init(tokens, pos);
    const mode: db_mod.types.RowClaimMode = if (cursor.matchKeyword("no")) blk: {
        try cursor.expectKeyword("key");
        try cursor.expectKeyword("update");
        break :blk .for_no_key_update;
    } else if (cursor.matchKeyword("key")) blk: {
        try cursor.expectKeyword("share");
        break :blk .for_key_share;
    } else if (cursor.matchKeyword("share")) blk: {
        break :blk .for_share;
    } else blk: {
        try cursor.expectKeyword("update");
        break :blk .for_update;
    };

    var targets = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (targets.items) |target| alloc.free(@constCast(target));
        targets.deinit(alloc);
    }
    if (cursor.matchKeyword("of")) {
        while (true) {
            _ = cursor.matchKeyword("only");
            const target = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
            try targets.append(alloc, try alloc.dupe(u8, target.text));
            if (cursor.matchToken(.comma) == null) break;
        }
    }

    const wait_policy: db_mod.types.RowClaimWaitPolicy = if (cursor.matchKeyword("skip")) blk: {
        try cursor.expectKeyword("locked");
        break :blk .skip_locked;
    } else if (cursor.matchKeyword("nowait"))
        .nowait
    else
        .wait;

    return .{
        .clause = .{ .mode = mode, .wait_policy = wait_policy },
        .targets = try targets.toOwnedSlice(alloc),
    };
}

pub fn parseDeallocatePreparedStatementTail(tokens: []const Token, pos: *usize) !NamedOrAllSyntax {
    var cursor = parser.Cursor.init(tokens, pos);
    _ = cursor.matchKeyword("prepare");
    return try parseNamedOrAllTail(cursor);
}

pub fn parseCloseCursorPortalTail(tokens: []const Token, pos: *usize) !NamedOrAllSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    return try parseNamedOrAllTail(cursor);
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

fn parseNamedOrAllTail(cursor: parser.Cursor) !NamedOrAllSyntax {
    if (cursor.matchKeyword("all")) {
        try parseAdapterNoopStatementEnd(cursor);
        return .{ .all = true };
    }
    const name = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
    try parseAdapterNoopStatementEnd(cursor);
    return .{ .name = name.text };
}

pub fn parseRelationPopulationSqlAlloc(alloc: std.mem.Allocator, sql: []const u8) !RelationPopulationSyntax {
    var tokens = try lexer.tokenizeAlloc(alloc, sql);
    defer lexer.freeTokens(alloc, &tokens);
    if (tokens.items.len == 0 or tokens.items[0].kind != .identifier) return error.UnsupportedSqlShape;
    if (std.ascii.eqlIgnoreCase(tokens.items[0].text, "select")) {
        return try parseSelectIntoPopulationSqlAlloc(alloc, sql, tokens.items);
    }
    if (std.ascii.eqlIgnoreCase(tokens.items[0].text, "create")) {
        return try parseCreateTableAsPopulationSqlAlloc(alloc, sql, tokens.items);
    }
    return error.UnsupportedSqlShape;
}

fn parseSelectIntoPopulationSqlAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    tokens: []const Token,
) !RelationPopulationSyntax {
    const into_relative = parser.findTopLevelKeyword(tokens[1..], "into") orelse return error.UnsupportedSqlShape;
    const into_index = 1 + into_relative;
    const from_relative = parser.findTopLevelKeyword(tokens[into_index + 1 ..], "from") orelse return error.UnsupportedSqlShape;
    const from_index = into_index + 1 + from_relative;
    if (from_index != into_index + 2) return error.UnsupportedSqlShape;
    if (tokens[into_index + 1].kind != .identifier) return error.UnsupportedSqlShape;

    const into_start = try tokenStartOffset(sql, tokens[into_index]);
    const from_start = try tokenStartOffset(sql, tokens[from_index]);
    const source_sql = try std.fmt.allocPrint(
        alloc,
        "{s} {s}",
        .{ std.mem.trim(u8, sql[0..into_start], " \t\r\n"), sql[from_start..] },
    );
    return .{
        .mode = .select_into,
        .target_identifier = tokens[into_index + 1].text,
        .target_lifetime = null,
        .if_not_exists = false,
        .source_sql = source_sql,
    };
}

fn parseCreateTableAsPopulationSqlAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    tokens: []const Token,
) !RelationPopulationSyntax {
    var index: usize = 1;
    const target_lifetime: ?RelationLifetimeKind = if (parser.matchKeyword(tokens, &index, "temporary") or parser.matchKeyword(tokens, &index, "temp"))
        .temporary
    else if (parser.matchKeyword(tokens, &index, "unlogged"))
        .unlogged
    else
        null;
    if (!parser.matchKeyword(tokens, &index, "table")) return error.UnsupportedSqlShape;
    var if_not_exists = false;
    if (parser.matchKeyword(tokens, &index, "if")) {
        try parser.expectKeyword(tokens, &index, "not");
        try parser.expectKeyword(tokens, &index, "exists");
        if_not_exists = true;
    }
    if (index >= tokens.len or tokens[index].kind != .identifier) return error.UnsupportedSqlShape;
    const target_identifier = tokens[index].text;
    index += 1;
    if (!parser.matchKeyword(tokens, &index, "as")) return error.UnsupportedSqlShape;
    if (index >= tokens.len or tokens[index].kind != .identifier or !std.ascii.eqlIgnoreCase(tokens[index].text, "select")) return error.UnsupportedSqlShape;
    const select_start = try tokenStartOffset(sql, tokens[index]);
    const source_sql = try alloc.dupe(u8, sql[select_start..]);
    return .{
        .mode = .create_table_as,
        .target_identifier = target_identifier,
        .target_lifetime = target_lifetime,
        .if_not_exists = if_not_exists,
        .source_sql = source_sql,
    };
}

fn tokenStartOffset(sql: []const u8, token: Token) !usize {
    if (token.source_end > token.source_start and token.source_end <= sql.len) return token.source_start;
    const sql_start = @intFromPtr(sql.ptr);
    const sql_end = sql_start + sql.len;
    const token_start = @intFromPtr(token.text.ptr);
    if (token_start < sql_start or token_start > sql_end) return error.UnsupportedSqlShape;
    return token_start - sql_start;
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

pub const sqlKeywordIsAnyOrSome = lower_expr.sqlKeywordIsAnyOrSome;
pub const sqlKeywordStartsScalarPredicate = lower_expr.sqlKeywordStartsScalarPredicate;
pub const sqlJoinedSourceAliasTerminator = lower_expr.sqlJoinedSourceAliasTerminator;
pub const sqlAssignmentTailKeyword = lower_expr.sqlAssignmentTailKeyword;
pub const sqlKeywordIsLengthFunction = lower_expr.sqlKeywordIsLengthFunction;
pub const sqlKeywordIsOctetLengthFunction = lower_expr.sqlKeywordIsOctetLengthFunction;
pub const sqlKeywordIsBitLengthFunction = lower_expr.sqlKeywordIsBitLengthFunction;
pub const sqlKeywordIsJsonArrayLengthFunction = lower_expr.sqlKeywordIsJsonArrayLengthFunction;
pub const sqlKeywordIsCardinalityFunction = lower_expr.sqlKeywordIsCardinalityFunction;
pub const sqlKeywordIsArrayLengthFunction = lower_expr.sqlKeywordIsArrayLengthFunction;
pub const sqlKeywordIsArrayPositionFunction = lower_expr.sqlKeywordIsArrayPositionFunction;
pub const sqlKeywordIsArrayToStringFunction = lower_expr.sqlKeywordIsArrayToStringFunction;
pub const arrayLengthDefaultOutput = lower_expr.arrayLengthDefaultOutput;
pub const sqlKeywordIsJsonTypeofFunction = lower_expr.sqlKeywordIsJsonTypeofFunction;
pub const sqlKeywordIsJsonExtractPathFunction = lower_expr.sqlKeywordIsJsonExtractPathFunction;
pub const sqlKeywordIsJsonBuildObjectFunction = lower_expr.sqlKeywordIsJsonBuildObjectFunction;
pub const sqlJsonExtractPathFunctionAsText = lower_expr.sqlJsonExtractPathFunctionAsText;
pub const sqlKeywordIsAsciiFunction = lower_expr.sqlKeywordIsAsciiFunction;
pub const sqlKeywordIsChrFunction = lower_expr.sqlKeywordIsChrFunction;
pub const sqlKeywordIsSubstringFunction = lower_expr.sqlKeywordIsSubstringFunction;
pub const sqlKeywordIsOverlayFunction = lower_expr.sqlKeywordIsOverlayFunction;
pub const sqlKeywordIsTranslateFunction = lower_expr.sqlKeywordIsTranslateFunction;
pub const sqlKeywordIsSplitPartFunction = lower_expr.sqlKeywordIsSplitPartFunction;
pub const sqlKeywordIsStrposFunction = lower_expr.sqlKeywordIsStrposFunction;
pub const sqlKeywordIsLeftRightFunction = lower_expr.sqlKeywordIsLeftRightFunction;
pub const sqlKeywordIsPadFunction = lower_expr.sqlKeywordIsPadFunction;
pub const sqlKeywordIsRepeatFunction = lower_expr.sqlKeywordIsRepeatFunction;
pub const sqlKeywordIsReverseFunction = lower_expr.sqlKeywordIsReverseFunction;
pub const sqlKeywordIsInitcapFunction = lower_expr.sqlKeywordIsInitcapFunction;
pub const sqlKeywordIsMd5Function = lower_expr.sqlKeywordIsMd5Function;
pub const sqlKeywordIsStartsWithFunction = lower_expr.sqlKeywordIsStartsWithFunction;
pub const sqlKeywordIsEndsWithFunction = lower_expr.sqlKeywordIsEndsWithFunction;
pub const sqlKeywordIsDateTruncFunction = lower_expr.sqlKeywordIsDateTruncFunction;
pub const sqlKeywordIsDateBinFunction = lower_expr.sqlKeywordIsDateBinFunction;
pub const sqlKeywordIsDatePartFunction = lower_expr.sqlKeywordIsDatePartFunction;
pub const sqlKeywordIsTrimVariantFunction = lower_expr.sqlKeywordIsTrimVariantFunction;
pub const sqlKeywordIsUuidV4Function = lower_expr.sqlKeywordIsUuidV4Function;
pub const sqlKeywordIsRegexpMatchFunction = lower_expr.sqlKeywordIsRegexpMatchFunction;
pub const sqlKeywordIsRegexpCountFunction = lower_expr.sqlKeywordIsRegexpCountFunction;
pub const sqlKeywordIsRegexpSubstrFunction = lower_expr.sqlKeywordIsRegexpSubstrFunction;
pub const sqlKeywordIsRegexpInstrFunction = lower_expr.sqlKeywordIsRegexpInstrFunction;
pub const rowExpressionBoundaryKeyword = lower_expr.rowExpressionBoundaryKeyword;
pub const sqlWhereTailClauseKeyword = lower_expr.sqlWhereTailClauseKeyword;
pub const sqlWindowTailClauseKeyword = lower_expr.sqlWindowTailClauseKeyword;

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

test "sql adapter grammar parses protocol cleanup tails" {
    const alloc = std.testing.allocator;

    var deallocate_tokens = try lexer.tokenizeAlloc(alloc, "usage_plan;");
    defer lexer.freeTokens(alloc, &deallocate_tokens);
    var deallocate_pos: usize = 0;
    const deallocate = try parseDeallocatePreparedStatementTail(deallocate_tokens.items, &deallocate_pos);
    try std.testing.expectEqualStrings("usage_plan", deallocate.name.?);
    try std.testing.expect(!deallocate.all);
    try std.testing.expectEqual(deallocate_tokens.items.len, deallocate_pos);

    var deallocate_prepare_tokens = try lexer.tokenizeAlloc(alloc, "PREPARE usage_plan;");
    defer lexer.freeTokens(alloc, &deallocate_prepare_tokens);
    var deallocate_prepare_pos: usize = 0;
    const deallocate_prepare = try parseDeallocatePreparedStatementTail(deallocate_prepare_tokens.items, &deallocate_prepare_pos);
    try std.testing.expectEqualStrings("usage_plan", deallocate_prepare.name.?);
    try std.testing.expect(!deallocate_prepare.all);
    try std.testing.expectEqual(deallocate_prepare_tokens.items.len, deallocate_prepare_pos);

    var deallocate_all_tokens = try lexer.tokenizeAlloc(alloc, "ALL;");
    defer lexer.freeTokens(alloc, &deallocate_all_tokens);
    var deallocate_all_pos: usize = 0;
    const deallocate_all = try parseDeallocatePreparedStatementTail(deallocate_all_tokens.items, &deallocate_all_pos);
    try std.testing.expect(deallocate_all.all);
    try std.testing.expect(deallocate_all.name == null);
    try std.testing.expectEqual(deallocate_all_tokens.items.len, deallocate_all_pos);

    var close_tokens = try lexer.tokenizeAlloc(alloc, "usage_cursor;");
    defer lexer.freeTokens(alloc, &close_tokens);
    var close_pos: usize = 0;
    const close = try parseCloseCursorPortalTail(close_tokens.items, &close_pos);
    try std.testing.expectEqualStrings("usage_cursor", close.name.?);
    try std.testing.expect(!close.all);
    try std.testing.expectEqual(close_tokens.items.len, close_pos);

    var close_all_tokens = try lexer.tokenizeAlloc(alloc, "ALL;");
    defer lexer.freeTokens(alloc, &close_all_tokens);
    var close_all_pos: usize = 0;
    const close_all = try parseCloseCursorPortalTail(close_all_tokens.items, &close_all_pos);
    try std.testing.expect(close_all.all);
    try std.testing.expect(close_all.name == null);
    try std.testing.expectEqual(close_all_tokens.items.len, close_all_pos);

    var extra_tokens = try lexer.tokenizeAlloc(alloc, "usage_cursor; CLOSE ALL;");
    defer lexer.freeTokens(alloc, &extra_tokens);
    var extra_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseCloseCursorPortalTail(extra_tokens.items, &extra_pos));
}

test "sql adapter grammar parses explain prefixes and options" {
    const basic = try parseExplainPrefix("EXPLAIN SELECT id FROM usage_records;");
    try std.testing.expect(!basic.analyze);
    try std.testing.expectEqual(ast.SqlExplainFormat.text, basic.format);
    try std.testing.expect(!basic.verbose);
    try std.testing.expect(basic.costs);
    try std.testing.expectEqualStrings("SELECT id FROM usage_records", basic.inner_sql);

    const options = try parseExplainPrefix("EXPLAIN (FORMAT JSON, VERBOSE, COSTS OFF, ANALYZE ON) SELECT id FROM usage_records");
    try std.testing.expect(options.analyze);
    try std.testing.expectEqual(ast.SqlExplainFormat.json, options.format);
    try std.testing.expect(options.verbose);
    try std.testing.expect(!options.costs);
    try std.testing.expectEqualStrings("SELECT id FROM usage_records", options.inner_sql);

    const analyze = try parseExplainPrefix("EXPLAIN ANALYZE INSERT INTO usage_records (id) VALUES ('u1')");
    try std.testing.expect(analyze.analyze);
    try std.testing.expectEqualStrings("INSERT INTO usage_records (id) VALUES ('u1')", analyze.inner_sql);

    try std.testing.expectError(error.UnsupportedSqlShape, parseExplainPrefix("EXPLAIN (FORMAT YAML) SELECT 1"));
    try std.testing.expectError(error.UnsupportedSqlShape, parseExplainPrefix("EXPLAINED SELECT 1"));
    try std.testing.expectError(error.UnsupportedSqlShape, parseExplainPrefix("EXPLAIN"));
}

test "sql adapter grammar parses row claim clauses" {
    const alloc = std.testing.allocator;

    var skip_tokens = try lexer.tokenizeAlloc(alloc, "NO KEY UPDATE OF usage_records, public.jobs SKIP LOCKED");
    defer lexer.freeTokens(alloc, &skip_tokens);
    var skip_pos: usize = 0;
    var skip_clause = try parseForRowClaimClauseAlloc(alloc, skip_tokens.items, &skip_pos);
    defer skip_clause.deinit(alloc);
    try std.testing.expectEqual(skip_tokens.items.len, skip_pos);
    try std.testing.expectEqual(db_mod.types.RowClaimMode.for_no_key_update, skip_clause.clause.mode);
    try std.testing.expectEqual(db_mod.types.RowClaimWaitPolicy.skip_locked, skip_clause.clause.wait_policy);
    try std.testing.expectEqual(@as(usize, 2), skip_clause.targets.len);
    try std.testing.expectEqualStrings("usage_records", skip_clause.targets[0]);
    try std.testing.expectEqualStrings("public.jobs", skip_clause.targets[1]);

    var share_tokens = try lexer.tokenizeAlloc(alloc, "KEY SHARE OF ONLY usage_records NOWAIT");
    defer lexer.freeTokens(alloc, &share_tokens);
    var share_pos: usize = 0;
    var share_clause = try parseForRowClaimClauseAlloc(alloc, share_tokens.items, &share_pos);
    defer share_clause.deinit(alloc);
    try std.testing.expectEqual(share_tokens.items.len, share_pos);
    try std.testing.expectEqual(db_mod.types.RowClaimMode.for_key_share, share_clause.clause.mode);
    try std.testing.expectEqual(db_mod.types.RowClaimWaitPolicy.nowait, share_clause.clause.wait_policy);
    try std.testing.expectEqual(@as(usize, 1), share_clause.targets.len);
    try std.testing.expectEqualStrings("usage_records", share_clause.targets[0]);

    var default_tokens = try lexer.tokenizeAlloc(alloc, "UPDATE");
    defer lexer.freeTokens(alloc, &default_tokens);
    var default_pos: usize = 0;
    var default_clause = try parseForRowClaimClauseAlloc(alloc, default_tokens.items, &default_pos);
    defer default_clause.deinit(alloc);
    try std.testing.expectEqual(default_tokens.items.len, default_pos);
    try std.testing.expectEqual(db_mod.types.RowClaimMode.for_update, default_clause.clause.mode);
    try std.testing.expectEqual(db_mod.types.RowClaimWaitPolicy.wait, default_clause.clause.wait_policy);

    var invalid_tokens = try lexer.tokenizeAlloc(alloc, "SHARE SKIP");
    defer lexer.freeTokens(alloc, &invalid_tokens);
    var invalid_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseForRowClaimClauseAlloc(alloc, invalid_tokens.items, &invalid_pos));
}

test "sql adapter grammar parses relation population syntax" {
    const alloc = std.testing.allocator;

    var select_into = try parseRelationPopulationSqlAlloc(
        alloc,
        "SELECT account_id, total INTO public.usage_archive FROM usage_records WHERE total > 10",
    );
    defer select_into.deinit(alloc);
    try std.testing.expectEqual(RelationPopulationMode.select_into, select_into.mode);
    try std.testing.expectEqualStrings("public.usage_archive", select_into.target_identifier);
    try std.testing.expect(select_into.target_lifetime == null);
    try std.testing.expect(!select_into.if_not_exists);
    try std.testing.expectEqualStrings("SELECT account_id, total FROM usage_records WHERE total > 10", select_into.source_sql);

    var create_as = try parseRelationPopulationSqlAlloc(
        alloc,
        "CREATE TEMP TABLE IF NOT EXISTS usage_session_archive AS SELECT account_id FROM usage_records",
    );
    defer create_as.deinit(alloc);
    try std.testing.expectEqual(RelationPopulationMode.create_table_as, create_as.mode);
    try std.testing.expectEqualStrings("usage_session_archive", create_as.target_identifier);
    try std.testing.expectEqual(RelationLifetimeKind.temporary, create_as.target_lifetime.?);
    try std.testing.expect(create_as.if_not_exists);
    try std.testing.expectEqualStrings("SELECT account_id FROM usage_records", create_as.source_sql);

    try std.testing.expectError(
        error.UnsupportedSqlShape,
        parseRelationPopulationSqlAlloc(alloc, "CREATE TABLE usage_archive SELECT account_id FROM usage_records"),
    );
}
