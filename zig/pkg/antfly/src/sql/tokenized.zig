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
const classifier = @import("classifier.zig");
const generated_parser = @import("generated_parser.zig");
const lexer = @import("lexer.zig");
const token_mod = @import("token.zig");

pub const Token = token_mod.Token;
const TokenKeyword = token_mod.TokenKeyword;
pub const SourceSpan = token_mod.SourceSpan;

pub const TokenRange = struct {
    start: usize,
    end: usize,
};

pub const RawSqlStatement = struct {
    family: ?classifier.SqlStatementFamily = null,
    token_start: usize = 0,
    token_end: usize = 0,
    source_span: SourceSpan = .{},

    pub fn sql(self: RawSqlStatement, source_sql: []const u8) []const u8 {
        if (self.source_span.end <= self.source_span.start or self.source_span.end > source_sql.len) return "";
        return source_sql[self.source_span.start..self.source_span.end];
    }
};

pub const ParsedReadStatement = struct {
    kind: classifier.SqlReadStatementKind,
    raw: RawSqlStatement,
};

pub const ParsedWriteStatement = struct {
    kind: classifier.SqlWriteStatementKind,
    raw: RawSqlStatement,
    recursive: bool = false,
};

pub const ParsedDdlStatement = struct {
    raw: RawSqlStatement,
};

pub const ParsedPreparedStatement = struct {
    raw: RawSqlStatement,
};

pub const ParsedExplainStatement = struct {
    raw: RawSqlStatement,
    analyze: bool = false,
    format: ast.SqlExplainFormat = .text,
    verbose: bool = false,
    costs: bool = true,
    buffers: bool = false,
    timing: bool = true,
    summary: bool = true,
    settings: bool = false,
    wal: bool = false,
    inner_token_start: ?usize = null,
    inner_token_end: ?usize = null,
};

pub const ParsedTransactionStatement = struct {
    raw: RawSqlStatement,
};

pub const ParsedSessionStatement = struct {
    raw: RawSqlStatement,
};

pub const ParsedUnsupportedStatement = struct {
    kind: generated_parser.GeneratedSqlUnsupportedKind,
    raw: RawSqlStatement,
};

pub const GeneratedRawSqlStatement = struct {
    raw: RawSqlStatement,
    statement: generated_parser.GeneratedSqlStatement,
    ast: ?generated_parser.GeneratedSqlAst = null,

    pub fn kind(self: GeneratedRawSqlStatement) generated_parser.GeneratedSqlStatementKind {
        return std.meta.activeTag(self.statement);
    }

    pub fn deinit(self: *GeneratedRawSqlStatement, alloc: std.mem.Allocator) void {
        if (self.ast) |*generated_ast| generated_ast.deinit(alloc);
        self.* = undefined;
    }
};

pub const ParsedStatement = union(enum) {
    read: ParsedReadStatement,
    write: ParsedWriteStatement,
    ddl: ParsedDdlStatement,
    explain: ParsedExplainStatement,
    transaction: ParsedTransactionStatement,
    prepared: ParsedPreparedStatement,
    session: ParsedSessionStatement,
    unsupported: ParsedUnsupportedStatement,
    unknown: RawSqlStatement,

    pub fn raw(self: ParsedStatement) RawSqlStatement {
        return switch (self) {
            .read => |statement| statement.raw,
            .write => |statement| statement.raw,
            .ddl => |statement| statement.raw,
            .explain => |statement| statement.raw,
            .transaction => |statement| statement.raw,
            .prepared => |statement| statement.raw,
            .session => |statement| statement.raw,
            .unsupported => |statement| statement.raw,
            .unknown => |statement| statement,
        };
    }

    pub fn readKind(self: ParsedStatement) ?classifier.SqlReadStatementKind {
        return switch (self) {
            .read => |statement| statement.kind,
            else => null,
        };
    }

    pub fn writeKind(self: ParsedStatement) ?classifier.SqlWriteStatementKind {
        return switch (self) {
            .write => |statement| statement.kind,
            else => null,
        };
    }

    pub fn isRecursiveWrite(self: ParsedStatement) bool {
        return switch (self) {
            .write => |statement| statement.recursive,
            else => false,
        };
    }
};

pub const TokenizedSql = struct {
    sql: []const u8,
    tokens: std.ArrayListUnmanaged(Token),
    statement_family: ?classifier.SqlStatementFamily = null,
    read_statement_kind: ?classifier.SqlReadStatementKind = null,
    write_statement_kind: ?classifier.SqlWriteStatementKind = null,

    pub fn initAlloc(alloc: std.mem.Allocator, sql: []const u8) !TokenizedSql {
        var tokens = try lexer.tokenizeAlloc(alloc, sql);
        errdefer lexer.freeTokens(alloc, &tokens);
        return .{
            .sql = sql,
            .tokens = tokens,
            .statement_family = classifier.classifyStatementFamily(tokens.items),
            .read_statement_kind = classifier.classifyReadStatement(tokens.items),
            .write_statement_kind = classifier.classifyWriteStatement(tokens.items),
        };
    }

    pub fn initFromTokenSliceAlloc(alloc: std.mem.Allocator, sql: []const u8, source_tokens: []const Token) !TokenizedSql {
        var tokens = try cloneTokensAlloc(alloc, source_tokens);
        errdefer lexer.freeTokens(alloc, &tokens);
        return .{
            .sql = sql,
            .tokens = tokens,
            .statement_family = classifier.classifyStatementFamily(tokens.items),
            .read_statement_kind = classifier.classifyReadStatement(tokens.items),
            .write_statement_kind = classifier.classifyWriteStatement(tokens.items),
        };
    }

    pub fn initFromTokenRangesAlloc(
        alloc: std.mem.Allocator,
        sql: []const u8,
        source_tokens: []const Token,
        ranges: []const TokenRange,
    ) !TokenizedSql {
        var tokens = try cloneTokenRangesAlloc(alloc, source_tokens, ranges);
        errdefer lexer.freeTokens(alloc, &tokens);
        return .{
            .sql = sql,
            .tokens = tokens,
            .statement_family = classifier.classifyStatementFamily(tokens.items),
            .read_statement_kind = classifier.classifyReadStatement(tokens.items),
            .write_statement_kind = classifier.classifyWriteStatement(tokens.items),
        };
    }

    pub fn deinit(self: *TokenizedSql, alloc: std.mem.Allocator) void {
        lexer.freeTokens(alloc, &self.tokens);
        self.* = undefined;
    }

    pub fn items(self: *const TokenizedSql) []const Token {
        return self.tokens.items;
    }
};

pub const ParsedSql = struct {
    tokenized_sql: TokenizedSql,
    raw_statement: RawSqlStatement,
    generated_statement: ?GeneratedRawSqlStatement = null,
    statement: ParsedStatement,

    pub fn initAlloc(alloc: std.mem.Allocator, source_sql: []const u8) !ParsedSql {
        var tokenized_sql = try TokenizedSql.initAlloc(alloc, source_sql);
        errdefer tokenized_sql.deinit(alloc);
        const raw_statement = try parseRawStatement(tokenized_sql.items(), tokenized_sql.statement_family);
        const generated_statement = try parseGeneratedRawStatementAlloc(alloc, tokenized_sql.items(), raw_statement);
        return .{
            .tokenized_sql = tokenized_sql,
            .raw_statement = raw_statement,
            .generated_statement = generated_statement,
            .statement = parseStatement(raw_statement, generated_statement, &tokenized_sql),
        };
    }

    pub fn initFromTokenSliceAlloc(alloc: std.mem.Allocator, source_sql: []const u8, source_tokens: []const Token) !ParsedSql {
        var tokenized_sql = try TokenizedSql.initFromTokenSliceAlloc(alloc, source_sql, source_tokens);
        errdefer tokenized_sql.deinit(alloc);
        const raw_statement = try parseRawStatement(tokenized_sql.items(), tokenized_sql.statement_family);
        const generated_statement = try parseGeneratedRawStatementAlloc(alloc, tokenized_sql.items(), raw_statement);
        return .{
            .tokenized_sql = tokenized_sql,
            .raw_statement = raw_statement,
            .generated_statement = generated_statement,
            .statement = parseStatement(raw_statement, generated_statement, &tokenized_sql),
        };
    }

    pub fn initChildStatementAlloc(
        alloc: std.mem.Allocator,
        parent: *const ParsedSql,
        token_start: usize,
        token_end: usize,
    ) !ParsedSql {
        if (token_start >= token_end or token_end > parent.items().len) return error.UnsupportedSqlShape;
        return try initFromTokenSliceAlloc(alloc, parent.sql(), parent.items()[token_start..token_end]);
    }

    pub fn initChildStatementFromTokenRangesAlloc(
        alloc: std.mem.Allocator,
        parent: *const ParsedSql,
        ranges: []const TokenRange,
    ) !ParsedSql {
        var tokenized_sql = try TokenizedSql.initFromTokenRangesAlloc(alloc, parent.sql(), parent.items(), ranges);
        errdefer tokenized_sql.deinit(alloc);
        const raw_statement = try parseRawStatement(tokenized_sql.items(), tokenized_sql.statement_family);
        const generated_statement = try parseGeneratedRawStatementAlloc(alloc, tokenized_sql.items(), raw_statement);
        return .{
            .tokenized_sql = tokenized_sql,
            .raw_statement = raw_statement,
            .generated_statement = generated_statement,
            .statement = parseStatement(raw_statement, generated_statement, &tokenized_sql),
        };
    }

    pub fn deinit(self: *ParsedSql, alloc: std.mem.Allocator) void {
        if (self.generated_statement) |*generated_statement| generated_statement.deinit(alloc);
        self.tokenized_sql.deinit(alloc);
        self.* = undefined;
    }

    pub fn sql(self: *const ParsedSql) []const u8 {
        return self.tokenized_sql.sql;
    }

    pub fn items(self: *const ParsedSql) []const Token {
        return self.tokenized_sql.items();
    }

    pub fn statementSql(self: *const ParsedSql) []const u8 {
        return self.raw_statement.sql(self.sql());
    }

    pub fn readStatementKind(self: *const ParsedSql) ?classifier.SqlReadStatementKind {
        return self.statement.readKind();
    }

    pub fn writeStatementKind(self: *const ParsedSql) ?classifier.SqlWriteStatementKind {
        return self.statement.writeKind();
    }

    pub fn isRecursiveWriteStatement(self: *const ParsedSql) bool {
        return self.statement.isRecursiveWrite();
    }

    pub fn generatedStatementKind(self: *const ParsedSql) ?generated_parser.GeneratedSqlStatementKind {
        if (self.generated_statement) |statement| return statement.kind();
        return null;
    }
};

fn parseGeneratedRawStatementAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    raw_statement: RawSqlStatement,
) !?GeneratedRawSqlStatement {
    const result = if (allowsGeneratedGrammarFallback(tokens, raw_statement))
        try generated_parser.parseGeneratedGateTokensAlloc(alloc, tokens)
    else
        try generated_parser.parseGeneratedGateTokensStrictAlloc(alloc, tokens);
    if (result) |parsed| {
        return .{ .raw = raw_statement, .statement = parsed.statement, .ast = parsed.ast };
    }
    return null;
}

fn allowsGeneratedGrammarFallback(tokens: []const Token, raw_statement: RawSqlStatement) bool {
    if (raw_statement.token_start >= raw_statement.token_end or raw_statement.token_end > tokens.len) return false;
    if (tokens[raw_statement.token_end - 1].kind == .eq or tokens[raw_statement.token_end - 1].kind == .comma) return false;
    if (tokenMatchesKeyword(tokens[raw_statement.token_end - 1], .to) or tokenMatchesKeyword(tokens[raw_statement.token_end - 1], .as)) return false;
    if (isGeneratedGraphDdlHead(tokens, raw_statement)) return false;
    if (isGeneratedCatalogDdlHead(tokens, raw_statement)) return false;
    if (isIncompleteGeneratedDdlBoundary(tokens, raw_statement)) return false;
    if (isIncompleteGeneratedDmlBoundary(tokens, raw_statement)) return false;
    if (isIncompleteGeneratedReadBoundary(tokens, raw_statement)) return false;

    const first = tokens[raw_statement.token_start];
    if (tokenMatchesKeyword(first, .set)) return raw_statement.token_end > raw_statement.token_start + 2;
    if (tokenMatchesKeyword(first, .reset) or tokenMatchesKeyword(first, .show) or tokenMatchesKeyword(first, .discard)) return raw_statement.token_end > raw_statement.token_start + 1;
    if (tokenMatchesKeyword(first, .prepare)) return raw_statement.token_end > raw_statement.token_start + 2;
    if (tokenMatchesKeyword(first, .execute) or tokenMatchesKeyword(first, .deallocate)) return raw_statement.token_end > raw_statement.token_start + 1;
    if (tokenMatchesKeyword(first, .commit) or tokenMatchesKeyword(first, .rollback)) return raw_statement.token_end > raw_statement.token_start + 1;
    if (tokenMatchesText(first, "start") or tokenMatchesText(first, "lock")) return raw_statement.token_end > raw_statement.token_start + 1;
    if (tokenMatchesKeyword(first, .begin)) return true;
    if (tokenMatchesKeyword(first, .savepoint)) return raw_statement.token_end > raw_statement.token_start + 1;
    if (tokenMatchesText(first, "release")) return raw_statement.token_end > raw_statement.token_start + 1;
    if (tokenMatchesText(first, "declare") or tokenMatchesText(first, "close") or tokenMatchesText(first, "fetch") or tokenMatchesText(first, "move")) {
        return raw_statement.token_end > raw_statement.token_start + 1;
    }
    return switch (raw_statement.family orelse return false) {
        .insert, .update, .delete, .truncate, .merge => true,
        .ddl => true,
        .select, .with => true,
    };
}

fn isGeneratedGraphDdlHead(tokens: []const Token, raw_statement: RawSqlStatement) bool {
    const start = raw_statement.token_start;
    if (start + 1 >= raw_statement.token_end or raw_statement.token_end > tokens.len) return false;
    return (tokenMatchesKeyword(tokens[start], .create) or tokenMatchesKeyword(tokens[start], .alter)) and
        tokenMatchesKeyword(tokens[start + 1], .graph);
}

fn isGeneratedCatalogDdlHead(tokens: []const Token, raw_statement: RawSqlStatement) bool {
    const start = raw_statement.token_start;
    if (start + 1 >= raw_statement.token_end or raw_statement.token_end > tokens.len) return false;
    if (!tokenMatchesKeyword(tokens[start], .create) and !tokenMatchesKeyword(tokens[start], .drop)) return false;
    return tokenMatchesKeyword(tokens[start + 1], .database) or
        tokenMatchesKeyword(tokens[start + 1], .schema) or
        tokenMatchesKeyword(tokens[start + 1], .extension);
}

fn isIncompleteGeneratedDdlBoundary(tokens: []const Token, raw_statement: RawSqlStatement) bool {
    const start = raw_statement.token_start;
    const end = raw_statement.token_end;
    if (start >= end or end > tokens.len) return false;
    const first = tokens[start];
    const last = tokens[end - 1];
    if (end == start + 1) {
        return tokenMatchesKeyword(first, .create) or
            tokenMatchesKeyword(first, .alter) or
            tokenMatchesKeyword(first, .drop);
    }
    if (tokenMatchesKeyword(first, .create)) {
        if (tokenMatchesKeyword(tokens[start + 1], .unique)) {
            if (end <= start + 2) return true;
            if (tokenMatchesKeyword(tokens[start + 2], .index) and end <= start + 4) return true;
            return isGeneratedDdlTrailingBoundary(last);
        }
        if (tokenMatchesKeyword(tokens[start + 1], .table)) {
            if (end <= start + 3) return true;
            return isGeneratedDdlTrailingBoundary(last);
        }
        if (tokenMatchesKeyword(tokens[start + 1], .index)) {
            if (end <= start + 3) return true;
            return isGeneratedDdlTrailingBoundary(last);
        }
        if (tokenMatchesKeyword(tokens[start + 1], .database) or
            tokenMatchesKeyword(tokens[start + 1], .schema) or
            tokenMatchesKeyword(tokens[start + 1], .extension))
        {
            if (end <= start + 2) return true;
            return isGeneratedDdlTrailingBoundary(last);
        }
    }
    if (tokenMatchesKeyword(first, .alter) and tokenMatchesKeyword(tokens[start + 1], .table)) {
        if (end <= start + 3) return true;
        return isGeneratedDdlTrailingBoundary(last);
    }
    if (tokenMatchesKeyword(first, .drop)) {
        if (tokenMatchesKeyword(tokens[start + 1], .table) or
            tokenMatchesKeyword(tokens[start + 1], .index) or
            tokenMatchesKeyword(tokens[start + 1], .database) or
            tokenMatchesKeyword(tokens[start + 1], .schema) or
            tokenMatchesKeyword(tokens[start + 1], .extension))
        {
            if (end <= start + 2) return true;
            return isGeneratedDdlTrailingBoundary(last);
        }
    }
    return false;
}

fn isGeneratedDdlTrailingBoundary(token: Token) bool {
    if (token.kind == .comma or token.kind == .lparen or token.kind == .eq) return true;
    return tokenMatchesKeyword(token, .@"if") or
        tokenMatchesKeyword(token, .not) or
        tokenMatchesKeyword(token, .exists) or
        tokenMatchesKeyword(token, .only) or
        tokenMatchesKeyword(token, .on) or
        tokenMatchesKeyword(token, .using) or
        tokenMatchesKeyword(token, .with) or
        tokenMatchesKeyword(token, .where) or
        tokenMatchesKeyword(token, .include) or
        tokenMatchesKeyword(token, .add) or
        tokenMatchesKeyword(token, .drop) or
        tokenMatchesKeyword(token, .rename) or
        tokenMatchesKeyword(token, .validate) or
        tokenMatchesKeyword(token, .column) or
        tokenMatchesKeyword(token, .constraint) or
        tokenMatchesKeyword(token, .to) or
        tokenMatchesKeyword(token, .as);
}

fn isIncompleteGeneratedReadBoundary(tokens: []const Token, raw_statement: RawSqlStatement) bool {
    const start = raw_statement.token_start;
    const end = raw_statement.token_end;
    if (start >= end or end > tokens.len) return false;
    if (raw_statement.family != .select and raw_statement.family != .with) return false;
    const first = tokens[start];
    const last = tokens[end - 1];
    if (end == start + 1) {
        return tokenMatchesKeyword(first, .select) or tokenMatchesKeyword(first, .with);
    }
    if (tokenMatchesKeyword(first, .select) and tokenMatchesKeyword(last, .distinct)) return true;
    if (last.kind == .lparen and end > start + 1 and tokenMatchesKeyword(tokens[end - 2], .on)) return true;
    if (generatedReadHasCompleteSourceBefore(tokens, start, end - 1) and
        (tokenMatchesKeyword(last, .@"union") or
            tokenMatchesKeyword(last, .intersect) or
            tokenMatchesKeyword(last, .except)))
    {
        return true;
    }
    if (tokenMatchesKeyword(last, .all) and end > start + 1 and tokenMatchesKeyword(tokens[end - 2], .@"union")) return true;
    if (tokenMatchesKeyword(last, .select) or
        tokenMatchesKeyword(last, .from) or
        tokenMatchesKeyword(last, .where) or
        tokenMatchesKeyword(last, .having) or
        tokenMatchesKeyword(last, .join) or
        tokenMatchesKeyword(last, .lateral) or
        tokenMatchesKeyword(last, .on) or
        tokenMatchesKeyword(last, .using))
    {
        return true;
    }
    if ((tokenMatchesKeyword(last, .limit) or
        tokenMatchesKeyword(last, .offset) or
        tokenMatchesKeyword(last, .fetch) or
        tokenMatchesKeyword(last, .first) or
        tokenMatchesKeyword(last, .next)) and
        generatedReadHasPriorResultTail(tokens, start, end - 1))
    {
        return true;
    }
    if (tokenMatchesKeyword(last, .by) and end > start + 1) {
        const previous = tokens[end - 2];
        return tokenMatchesKeyword(previous, .group) or tokenMatchesKeyword(previous, .order);
    }
    if (generatedReadHasPriorResultTail(tokens, start, end - 1) and
        (tokenMatchesKeyword(last, .window) or
            tokenMatchesKeyword(last, .nulls) or
            tokenMatchesKeyword(last, .between) or
            tokenMatchesKeyword(last, .preceding) or
            tokenMatchesKeyword(last, .following)))
    {
        return true;
    }
    if (tokenMatchesKeyword(first, .with)) {
        return last.kind == .lparen or
            tokenMatchesKeyword(last, .as) or
            tokenMatchesKeyword(last, .recursive) or
            tokenMatchesKeyword(last, .materialized) or
            tokenMatchesKeyword(last, .not);
    }
    return false;
}

fn generatedReadHasCompleteSourceBefore(tokens: []const Token, start: usize, end: usize) bool {
    var depth: usize = 0;
    var index = start;
    while (index < end and index < tokens.len) : (index += 1) {
        switch (tokens[index].kind) {
            .lparen, .lbracket => depth += 1,
            .rparen, .rbracket => {
                if (depth > 0) depth -= 1;
            },
            else => {},
        }
        if (depth == 0 and tokenMatchesKeyword(tokens[index], .from) and index + 1 < end) {
            return true;
        }
    }
    return false;
}

fn generatedReadHasPriorResultTail(tokens: []const Token, start: usize, end: usize) bool {
    var depth: usize = 0;
    var index = start;
    while (index < end and index < tokens.len) : (index += 1) {
        switch (tokens[index].kind) {
            .lparen, .lbracket => depth += 1,
            .rparen, .rbracket => {
                if (depth > 0) depth -= 1;
            },
            else => {},
        }
        if (depth != 0) continue;
        if (tokenMatchesKeyword(tokens[index], .where) or
            tokenMatchesKeyword(tokens[index], .having) or
            tokenMatchesKeyword(tokens[index], .limit) or
            tokenMatchesKeyword(tokens[index], .offset) or
            tokenMatchesKeyword(tokens[index], .fetch))
        {
            return true;
        }
        if ((tokenMatchesKeyword(tokens[index], .group) or tokenMatchesKeyword(tokens[index], .order)) and
            index + 1 < end and
            tokenMatchesKeyword(tokens[index + 1], .by))
        {
            return true;
        }
    }
    return false;
}

fn isIncompleteGeneratedDmlBoundary(tokens: []const Token, raw_statement: RawSqlStatement) bool {
    const start = raw_statement.token_start;
    const end = raw_statement.token_end;
    if (start >= end or end > tokens.len) return false;
    const first = tokens[start];
    const last = tokens[end - 1];
    if (end == start + 1) {
        return tokenMatchesKeyword(first, .insert) or
            tokenMatchesKeyword(first, .update) or
            tokenMatchesKeyword(first, .delete) or
            tokenMatchesKeyword(first, .truncate) or
            tokenMatchesKeyword(first, .merge);
    }
    if (tokenMatchesKeyword(first, .insert)) {
        return tokenMatchesKeyword(last, .into) or
            tokenMatchesKeyword(last, .default) or
            tokenMatchesKeyword(last, .values) or
            tokenMatchesKeyword(last, .select) or
            tokenMatchesKeyword(last, .on) or
            tokenMatchesKeyword(last, .conflict) or
            tokenMatchesText(last, "do") or
            tokenMatchesKeyword(last, .returning);
    }
    if (tokenMatchesKeyword(first, .update)) {
        return tokenMatchesKeyword(last, .set) or
            tokenMatchesKeyword(last, .from) or
            tokenMatchesKeyword(last, .where) or
            tokenMatchesKeyword(last, .returning);
    }
    if (tokenMatchesKeyword(first, .delete)) {
        return tokenMatchesKeyword(last, .from) or
            tokenMatchesKeyword(last, .using) or
            tokenMatchesKeyword(last, .where) or
            tokenMatchesKeyword(last, .returning);
    }
    if (tokenMatchesKeyword(first, .truncate)) {
        return tokenMatchesKeyword(last, .table) or
            tokenMatchesKeyword(last, .restart) or
            tokenMatchesKeyword(last, .@"continue");
    }
    if (tokenMatchesKeyword(first, .merge)) {
        return tokenMatchesKeyword(last, .into) or
            tokenMatchesKeyword(last, .using) or
            tokenMatchesKeyword(last, .on) or
            tokenMatchesKeyword(last, .when) or
            tokenMatchesKeyword(last, .matched) or
            tokenMatchesKeyword(last, .then) or
            tokenMatchesKeyword(last, .update) or
            tokenMatchesKeyword(last, .insert) or
            tokenMatchesKeyword(last, .set) or
            tokenMatchesKeyword(last, .values);
    }
    return false;
}

fn parseStatement(
    raw_statement: RawSqlStatement,
    generated_statement: ?GeneratedRawSqlStatement,
    tokenized_sql: *const TokenizedSql,
) ParsedStatement {
    if (generated_statement) |generated_raw| {
        switch (generated_raw.statement) {
            .session => return .{ .session = .{ .raw = raw_statement } },
            .transaction => return .{ .transaction = .{ .raw = raw_statement } },
            .prepared => return .{ .prepared = .{ .raw = raw_statement } },
            .ddl => return .{ .ddl = .{ .raw = raw_statement } },
            .extension_index => return .{ .ddl = .{ .raw = raw_statement } },
            .dml => if (generatedDmlStatementKind(generated_raw)) |generated| {
                const classified_recursive_kind = if (generated.recursive) classifier.classifyRecursiveWriteStatement(tokenized_sql.items()) else null;
                if (classified_recursive_kind orelse tokenized_sql.write_statement_kind) |classified_kind| {
                    if (!generatedDmlStatementKindMatchesWriteKind(generated.kind, classified_kind)) return .{ .unknown = raw_statement };
                    return .{ .write = .{ .kind = classified_kind, .raw = raw_statement, .recursive = generated.recursive } };
                }
                return .{ .write = .{ .kind = generated.defaultWriteKind(), .raw = raw_statement, .recursive = generated.recursive } };
            } else if (tokenized_sql.write_statement_kind) |kind| {
                return .{ .write = .{ .kind = kind, .raw = raw_statement } };
            },
            .read => if (generatedReadStatementKind(tokenized_sql.items(), generated_raw)) |kind| {
                if (tokenized_sql.read_statement_kind) |classified_kind| {
                    if (classified_kind != kind) return .{ .unknown = raw_statement };
                }
                return .{ .read = .{ .kind = kind, .raw = raw_statement } };
            } else if (tokenized_sql.read_statement_kind) |kind| {
                return .{ .read = .{ .kind = kind, .raw = raw_statement } };
            },
            .graph => return .{ .ddl = .{ .raw = raw_statement } },
            .unsupported => |kind| if (!generatedUnsupportedUsesLegacyPlanner(kind)) return .{ .unsupported = .{ .kind = kind, .raw = raw_statement } },
            .other => {},
        }
    }
    if (tokenized_sql.read_statement_kind) |kind| {
        return .{ .read = .{ .kind = kind, .raw = raw_statement } };
    }
    if (tokenized_sql.write_statement_kind) |kind| {
        return .{ .write = .{ .kind = kind, .raw = raw_statement } };
    }
    if (classifier.classifyRecursiveWriteStatement(tokenized_sql.items())) |kind| {
        return .{ .write = .{ .kind = kind, .raw = raw_statement, .recursive = true } };
    }
    return switch (tokenized_sql.statement_family orelse return .{ .unknown = raw_statement }) {
        .ddl => classifyDdlLikeStatement(raw_statement, tokenized_sql.items()),
        else => .{ .unknown = raw_statement },
    };
}

const GeneratedDmlStatementKind = struct {
    kind: generated_parser.GeneratedSqlDmlKind,
    recursive: bool = false,

    fn defaultWriteKind(self: @This()) classifier.SqlWriteStatementKind {
        return switch (self.kind) {
            .insert_values => .insert,
            .insert_select => .insert_source,
            .update => .update,
            .delete => .delete,
            .truncate => .truncate,
            .merge => .merge,
        };
    }
};

fn generatedDmlStatementKind(
    generated_raw: GeneratedRawSqlStatement,
) ?GeneratedDmlStatementKind {
    const ast_value = generated_raw.ast orelse return null;
    const dml_ast = switch (ast_value) {
        .dml => |dml| dml,
        else => return null,
    };
    return .{ .kind = dml_ast.kind, .recursive = dml_ast.cte_recursive };
}

fn generatedDmlStatementKindMatchesWriteKind(
    generated_kind: generated_parser.GeneratedSqlDmlKind,
    write_kind: classifier.SqlWriteStatementKind,
) bool {
    return switch (generated_kind) {
        .insert_values => write_kind == .insert,
        .insert_select => write_kind == .insert_source,
        .update => switch (write_kind) {
            .update, .update_source, .update_joined_source => true,
            else => false,
        },
        .delete => switch (write_kind) {
            .delete, .delete_source, .delete_joined_source => true,
            else => false,
        },
        .truncate => write_kind == .truncate,
        .merge => write_kind == .merge,
    };
}

fn generatedReadStatementKind(
    tokens: []const Token,
    generated_raw: GeneratedRawSqlStatement,
) ?classifier.SqlReadStatementKind {
    const ast_value = generated_raw.ast orelse return null;
    const read_ast = switch (ast_value) {
        .read => |read| read,
        else => return null,
    };
    return switch (read_ast.kind) {
        .query => .query,
        .aggregate => .aggregate,
        .join => .join,
        .lateral => .lateral,
        .window => .window,
        .set_operation => .set_operation,
        .cte => generatedCteReadStatementKind(tokens, read_ast),
    };
}

fn generatedCteReadStatementKind(
    tokens: []const Token,
    read_ast: generated_parser.GeneratedSqlReadAst,
) ?classifier.SqlReadStatementKind {
    if (read_ast.cte_recursive) return .recursive_cte;
    if (read_ast.projection_tokens == null or read_ast.source_tokens == null) return null;
    if (read_ast.set_operation_tokens != null) return .set_operation;
    if (read_ast.source_tokens) |source| {
        if (generatedReadRangeContainsKeyword(tokens, source, .lateral)) return .lateral;
    }
    if (read_ast.projection_tokens) |projection| {
        if (generatedReadRangeContainsKeyword(tokens, projection, .over)) return .window;
    }
    const aggregate_projection = if (read_ast.projection_tokens) |projection|
        generatedReadRangeHasAggregateFunction(tokens, projection)
    else
        false;
    if ((read_ast.distinct_tokens != null and read_ast.distinct_on_items.count == 0) or
        read_ast.group_tokens != null or
        read_ast.having_tokens != null or
        aggregate_projection)
    {
        return .aggregate;
    }
    if (read_ast.source_tokens) |source| {
        if (generatedReadRangeContainsKeyword(tokens, source, .join)) return .join;
    }
    return .query;
}

fn generatedReadRangeContainsKeyword(
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

fn generatedReadRangeHasAggregateFunction(
    tokens: []const Token,
    range: generated_parser.GeneratedSqlTokenRange,
) bool {
    if (range.end > tokens.len or range.start > range.end) return false;
    var depth: usize = 0;
    var index = range.start;
    while (index < range.end) : (index += 1) {
        const token = tokens[index];
        switch (token.kind) {
            .lparen, .lbracket => depth += 1,
            .rparen, .rbracket => {
                if (depth > 0) depth -= 1;
            },
            .identifier => if (depth == 0 and index + 1 < range.end and tokens[index + 1].kind == .lparen and generatedSqlAggregateFunctionName(token)) {
                return true;
            },
            else => {},
        }
    }
    return false;
}

fn generatedSqlAggregateFunctionName(token: Token) bool {
    return token.matchesKeywordTag(.count) or
        token.matchesKeywordTag(.sum) or
        token.matchesKeywordTag(.avg) or
        token.matchesKeywordTag(.min) or
        token.matchesKeywordTag(.max) or
        token.matchesKeywordTag(.bool_or) or
        token.matchesKeywordTag(.bool_and) or
        token.matchesKeywordTag(.array_agg) or
        token.matchesKeywordTag(.string_agg);
}

fn generatedUnsupportedUsesLegacyPlanner(kind: generated_parser.GeneratedSqlUnsupportedKind) bool {
    return switch (kind) {
        .alter_policy,
        .alter_publication,
        .alter_subscription,
        .analyze,
        .call,
        .close,
        .cluster,
        .comment,
        .copy,
        .create_materialized_view,
        .create_policy,
        .create_publication,
        .create_subscription,
        .create_trigger,
        .declare,
        .drop_materialized_view,
        .drop_policy,
        .drop_publication,
        .drop_subscription,
        .drop_trigger,
        .explain,
        .fetch,
        .grant,
        .listen,
        .lock,
        .notify,
        .refresh,
        .reindex,
        .release,
        .revoke,
        .savepoint,
        .unlisten,
        .vacuum,
        => true,
        .alter_foreign_table,
        .alter_materialized_view,
        .alter_rule,
        .alter_server,
        .alter_trigger,
        .checkpoint,
        .create_foreign_table,
        .create_rule,
        .create_server,
        .do_block,
        .drop_foreign_table,
        .drop_rule,
        .drop_server,
        .load,
        .move,
        .security_label,
        => false,
    };
}

fn classifyDdlLikeStatement(raw_statement: RawSqlStatement, tokens: []const Token) ParsedStatement {
    if (tokens.len == 0 or tokens[0].kind != .identifier) return .{ .unknown = raw_statement };
    if (tokens[0].isKeyword(.explain)) return .{ .explain = parseExplainStatement(raw_statement, tokens) catch .{ .raw = raw_statement } };
    if (tokens[0].isKeyword(.begin) or tokens[0].isKeyword(.commit) or tokens[0].isKeyword(.rollback)) {
        return .{ .transaction = .{ .raw = raw_statement } };
    }
    if (tokens[0].isKeyword(.set) or tokens[0].isKeyword(.reset) or tokens[0].isKeyword(.show) or tokens[0].isKeyword(.discard)) {
        return .{ .session = .{ .raw = raw_statement } };
    }
    if (tokens[0].isKeyword(.prepare) or tokens[0].isKeyword(.execute) or tokens[0].isKeyword(.deallocate)) {
        return .{ .prepared = .{ .raw = raw_statement } };
    }
    return .{ .ddl = .{ .raw = raw_statement } };
}

fn parseExplainStatement(raw_statement: RawSqlStatement, tokens: []const Token) !ParsedExplainStatement {
    var index = raw_statement.token_start;
    if (!matchKeywordTag(tokens, &index, raw_statement.token_end, .explain)) return error.UnsupportedSqlShape;
    if (index >= raw_statement.token_end) return error.UnsupportedSqlShape;

    var statement = ParsedExplainStatement{ .raw = raw_statement };
    if (matchToken(tokens, &index, raw_statement.token_end, .lparen)) {
        try parseExplainOptions(tokens, &index, raw_statement.token_end, &statement);
        if (index >= raw_statement.token_end) return error.UnsupportedSqlShape;
    }

    if (matchKeywordTag(tokens, &index, raw_statement.token_end, .analyze)) {
        statement.analyze = true;
        if (index >= raw_statement.token_end) return error.UnsupportedSqlShape;
    }

    statement.inner_token_start = index;
    statement.inner_token_end = raw_statement.token_end;
    return statement;
}

fn parseExplainOptions(
    tokens: []const Token,
    index: *usize,
    end: usize,
    statement: *ParsedExplainStatement,
) !void {
    while (true) {
        if (index.* >= end) return error.UnsupportedSqlShape;
        if (matchKeywordTag(tokens, index, end, .format)) {
            if (matchKeywordTag(tokens, index, end, .json)) {
                statement.format = .json;
            } else if (matchKeywordTag(tokens, index, end, .text)) {
                statement.format = .text;
            } else {
                return error.UnsupportedSqlShape;
            }
        } else if (matchKeywordTag(tokens, index, end, .verbose)) {
            statement.verbose = parseOptionalExplainBool(tokens, index, end, true);
        } else if (matchKeywordTag(tokens, index, end, .costs)) {
            statement.costs = parseOptionalExplainBool(tokens, index, end, true);
        } else if (matchKeywordTag(tokens, index, end, .analyze)) {
            statement.analyze = parseOptionalExplainBool(tokens, index, end, true);
        } else if (matchKeywordTag(tokens, index, end, .buffers)) {
            statement.buffers = parseOptionalExplainBool(tokens, index, end, true);
        } else if (matchKeywordTag(tokens, index, end, .timing)) {
            statement.timing = parseOptionalExplainBool(tokens, index, end, true);
        } else if (matchKeywordTag(tokens, index, end, .summary)) {
            statement.summary = parseOptionalExplainBool(tokens, index, end, true);
        } else if (matchKeywordTag(tokens, index, end, .settings)) {
            statement.settings = parseOptionalExplainBool(tokens, index, end, true);
        } else if (matchKeywordTag(tokens, index, end, .wal)) {
            statement.wal = parseOptionalExplainBool(tokens, index, end, true);
        } else {
            return error.UnsupportedSqlShape;
        }

        if (matchToken(tokens, index, end, .comma)) continue;
        if (matchToken(tokens, index, end, .rparen)) return;
        return error.UnsupportedSqlShape;
    }
}

fn parseOptionalExplainBool(tokens: []const Token, index: *usize, end: usize, default_value: bool) bool {
    const before = index.*;
    if (matchKeywordTag(tokens, index, end, .true) or
        matchKeywordTag(tokens, index, end, .on) or
        matchKeywordTag(tokens, index, end, .yes))
    {
        return true;
    }
    index.* = before;
    if (matchKeywordTag(tokens, index, end, .false) or
        matchKeywordTag(tokens, index, end, .off) or
        matchKeywordTag(tokens, index, end, .no))
    {
        return false;
    }
    index.* = before;
    return default_value;
}

fn matchKeywordTag(tokens: []const Token, index: *usize, end: usize, keyword: TokenKeyword) bool {
    if (index.* >= end or index.* >= tokens.len) return false;
    if (!tokens[index.*].matchesKeywordTag(keyword)) return false;
    index.* += 1;
    return true;
}

fn tokenMatchesKeyword(token: Token, keyword: TokenKeyword) bool {
    return token.matchesKeywordTag(keyword);
}

fn tokenMatchesText(token: Token, text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(token.text, text);
}

fn matchToken(tokens: []const Token, index: *usize, end: usize, kind: token_mod.TokenKind) bool {
    if (index.* >= end or index.* >= tokens.len or tokens[index.*].kind != kind) return false;
    index.* += 1;
    return true;
}

fn cloneTokensAlloc(alloc: std.mem.Allocator, source_tokens: []const Token) !std.ArrayListUnmanaged(Token) {
    var out = try std.ArrayListUnmanaged(Token).initCapacity(alloc, source_tokens.len);
    errdefer lexer.freeTokens(alloc, &out);
    for (source_tokens) |token| {
        var cloned = token;
        if (token.owned) {
            cloned.text = try alloc.dupe(u8, token.text);
            cloned.owned = true;
        } else {
            cloned.owned = false;
        }
        out.appendAssumeCapacity(cloned);
    }
    return out;
}

fn cloneTokenRangesAlloc(
    alloc: std.mem.Allocator,
    source_tokens: []const Token,
    ranges: []const TokenRange,
) !std.ArrayListUnmanaged(Token) {
    var total: usize = 0;
    for (ranges) |range| {
        if (range.start >= range.end or range.end > source_tokens.len) return error.UnsupportedSqlShape;
        total += range.end - range.start;
    }
    var out = try std.ArrayListUnmanaged(Token).initCapacity(alloc, total);
    errdefer lexer.freeTokens(alloc, &out);
    for (ranges) |range| {
        for (source_tokens[range.start..range.end]) |token| {
            var cloned = token;
            if (token.owned) {
                cloned.text = try alloc.dupe(u8, token.text);
                cloned.owned = true;
            } else {
                cloned.owned = false;
            }
            out.appendAssumeCapacity(cloned);
        }
    }
    return out;
}

fn parseRawStatement(tokens: []const Token, family: ?classifier.SqlStatementFamily) !RawSqlStatement {
    if (tokens.len == 0) return .{ .family = family };
    var token_end = try rawStatementTokenEnd(tokens);
    while (token_end > 0 and tokens[token_end - 1].kind == .semicolon) token_end -= 1;
    if (token_end == 0) return .{ .family = family };
    return .{
        .family = family,
        .token_start = 0,
        .token_end = token_end,
        .source_span = .{
            .start = tokens[0].source_start,
            .end = tokens[token_end - 1].source_end,
        },
    };
}

fn rawStatementTokenEnd(tokens: []const Token) !usize {
    var depth: usize = 0;
    for (tokens, 0..) |token, i| {
        switch (token.kind) {
            .lparen, .lbracket => depth += 1,
            .rparen, .rbracket => {
                if (depth > 0) depth -= 1;
            },
            .semicolon => if (depth == 0) {
                var next = i + 1;
                while (next < tokens.len and tokens[next].kind == .semicolon) next += 1;
                if (next < tokens.len) return error.UnsupportedSqlShape;
                return i;
            },
            else => {},
        }
    }
    return tokens.len;
}

test "sql adapter tokenized sql classifies read and write statements once" {
    const alloc = std.testing.allocator;

    var query = try TokenizedSql.initAlloc(alloc, "SELECT id FROM usage_records WHERE status = 'open'");
    defer query.deinit(alloc);
    try std.testing.expectEqual(classifier.SqlStatementFamily.select, query.statement_family.?);
    try std.testing.expectEqual(classifier.SqlReadStatementKind.query, query.read_statement_kind.?);
    try std.testing.expect(query.write_statement_kind == null);

    var joined = try TokenizedSql.initAlloc(alloc, "SELECT o.id FROM usage_records AS o JOIN customers AS c ON o.customer_id = c.id");
    defer joined.deinit(alloc);
    try std.testing.expectEqual(classifier.SqlReadStatementKind.join, joined.read_statement_kind.?);

    var distinct_on = try ParsedSql.initAlloc(alloc, "SELECT DISTINCT ON (organization_id) organization_id, id FROM usage_records ORDER BY organization_id ASC, created_at DESC");
    defer distinct_on.deinit(alloc);
    try std.testing.expectEqual(classifier.SqlReadStatementKind.query, distinct_on.readStatementKind().?);

    var write = try TokenizedSql.initAlloc(alloc, "WITH source_rows AS (SELECT id FROM usage_records) UPDATE usage_records SET status = 'done' WHERE id IN (SELECT id FROM source_rows)");
    defer write.deinit(alloc);
    try std.testing.expectEqual(classifier.SqlStatementFamily.with, write.statement_family.?);
    try std.testing.expect(write.read_statement_kind == null);
    try std.testing.expectEqual(classifier.SqlWriteStatementKind.update, write.write_statement_kind.?);
}

test "sql adapter parsed sql exposes raw statement source spans" {
    const alloc = std.testing.allocator;
    const sql = "  SELECT id FROM usage_records;  ";

    var parsed = try ParsedSql.initAlloc(alloc, sql);
    defer parsed.deinit(alloc);

    try std.testing.expectEqual(classifier.SqlStatementFamily.select, parsed.raw_statement.family.?);
    try std.testing.expectEqualStrings("SELECT id FROM usage_records", parsed.statementSql());
    try std.testing.expectEqual(@as(usize, 2), parsed.raw_statement.source_span.start);
    try std.testing.expectEqual(@as(usize, 30), parsed.raw_statement.source_span.end);

    var trailing_semicolons = try ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records;;");
    defer trailing_semicolons.deinit(alloc);
    try std.testing.expectEqualStrings("SELECT id FROM usage_records", trailing_semicolons.statementSql());

    try std.testing.expectError(
        error.UnsupportedSqlShape,
        ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records; DROP TABLE usage_records"),
    );

    var nested_semicolon = try ParsedSql.initAlloc(alloc, "SELECT ';' AS separator");
    defer nested_semicolon.deinit(alloc);
    try std.testing.expectEqualStrings("SELECT ';' AS separator", nested_semicolon.statementSql());
}

test "sql adapter parsed sql does not require generated grammar parity" {
    const alloc = std.testing.allocator;

    var ddl = try ParsedSql.initAlloc(alloc, "ALTER TABLE audit_log ALTER COLUMN amount TYPE numeric USING amount + 1;");
    defer ddl.deinit(alloc);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .ddl), std.meta.activeTag(ddl.statement));

    var select = try ParsedSql.initAlloc(alloc, "SELECT id FROM docs WHERE status = 'active' LIMIT 5");
    defer select.deinit(alloc);
    try std.testing.expectEqual(classifier.SqlReadStatementKind.query, select.readStatementKind().?);
}

test "sql adapter parsed sql requires generated grammar for first migrated control family" {
    const alloc = std.testing.allocator;

    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SET search_path TO"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "PREPARE read_stmt AS"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "CREATE TABLE usage_records ("));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "CREATE INDEX usage_status_idx ON"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "ALTER TABLE usage_records ADD"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "DROP TABLE IF EXISTS"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "CREATE DATABASE tenant_ops WITH OWNER app"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "CREATE SCHEMA analytics AUTHORIZATION app_user"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "CREATE EXTENSION vector FROM unpackaged"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "DROP DATABASE tenant_ops WITH (OWNER)"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "DROP SCHEMA analytics, reporting"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "DROP EXTENSION vector, postgis"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "CREATE GRAPH INDEX docs_edge_graph ON"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "ALTER GRAPH INDEX docs_edge_graph ADD"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "INSERT INTO usage_records VALUES"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "INSERT INTO usage_records (id) VALUES ('u1') ON CONFLICT (id) DO"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "UPDATE usage_records SET"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "DELETE FROM usage_records WHERE"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "TRUNCATE TABLE"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "MERGE INTO usage_records USING source_rows ON"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "MERGE INTO usage_records USING source_rows ON usage_records.id = source_rows.id WHEN MATCHED THEN UPDATE SET"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "MERGE INTO usage_records USING source_rows ON usage_records.id = source_rows.id WHEN NOT MATCHED THEN INSERT"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "MERGE INTO usage_records USING source_rows ON usage_records.id = source_rows.id WHEN NOT MATCHED THEN INSERT (id) VALUES"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "INSERT INTO usage_records OVERRIDING SYSTEM VALUE VALUES ('u1')"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SELECT"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SELECT DISTINCT"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SELECT DISTINCT ON ("));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SELECT id FROM"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records WHERE"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SELECT status, COUNT(*) FROM usage_records GROUP BY"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SELECT status, COUNT(*) FROM usage_records GROUP BY status HAVING"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records ORDER BY"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records ORDER BY id NULLS"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records ORDER BY id ROWS BETWEEN 1 PRECEDING"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records ORDER BY id LIMIT"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records WHERE status = 'active' OFFSET"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records ORDER BY id FETCH"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SELECT usage_records.id FROM usage_records JOIN"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SELECT usage_records.id FROM usage_records JOIN accounts ON"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records UNION"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records UNION ALL"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records INTERSECT"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records EXCEPT"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SELECT row_number() OVER usage_window FROM usage_records WINDOW"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "WITH"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "WITH RECURSIVE"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "WITH source_rows AS"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "WITH source_rows AS ("));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "WITH source_rows AS (SELECT id FROM usage_records) SELECT"));

    var complex_ddl = try ParsedSql.initAlloc(alloc, "ALTER TABLE audit_log ALTER COLUMN amount TYPE numeric USING amount + 1;");
    defer complex_ddl.deinit(alloc);
    try std.testing.expect(complex_ddl.generated_statement == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .ddl), std.meta.activeTag(complex_ddl.statement));

    var generated_catalog_ddl = try ParsedSql.initAlloc(alloc, "CREATE DATABASE tenant_ops");
    defer generated_catalog_ddl.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.extension_index, generated_catalog_ddl.generatedStatementKind().?);
    switch (generated_catalog_ddl.generated_statement.?.ast.?) {
        .extension_index => |ddl_ast| try std.testing.expectEqual(generated_parser.GeneratedSqlDdlKind.create_database, ddl_ast.kind),
        else => return error.TestUnexpectedResult,
    }
}

test "sql adapter parsed sql builds non-contiguous child statements from parent tokens" {
    const alloc = std.testing.allocator;

    var parent = try ParsedSql.initAlloc(
        alloc,
        "SELECT account_id, total INTO usage_archive FROM usage_records WHERE total > 10",
    );
    defer parent.deinit(alloc);

    const ranges = [_]TokenRange{
        .{ .start = 0, .end = 4 },
        .{ .start = 6, .end = parent.items().len },
    };
    var child = try ParsedSql.initChildStatementFromTokenRangesAlloc(alloc, &parent, &ranges);
    defer child.deinit(alloc);

    try std.testing.expectEqual(classifier.SqlStatementFamily.select, child.raw_statement.family.?);
    try std.testing.expectEqual(classifier.SqlReadStatementKind.query, child.readStatementKind().?);
    try std.testing.expectEqual(@as(usize, parent.items().len - 2), child.items().len);
    try std.testing.expectEqualStrings("SELECT", child.items()[0].text);
    try std.testing.expectEqualStrings("FROM", child.items()[4].text);
    try std.testing.expectEqualStrings("usage_records", child.items()[5].text);
}

test "sql adapter parsed sql owns typed statement variants" {
    const alloc = std.testing.allocator;

    var read = try ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records");
    defer read.deinit(alloc);
    switch (read.statement) {
        .read => |statement| {
            try std.testing.expectEqual(classifier.SqlReadStatementKind.query, statement.kind);
            try std.testing.expectEqualStrings("SELECT id FROM usage_records", statement.raw.sql(read.sql()));
        },
        else => return error.TestUnexpectedResult,
    }

    var write = try ParsedSql.initAlloc(alloc, "UPDATE usage_records SET status = 'done' WHERE id = 'u1'");
    defer write.deinit(alloc);
    switch (write.statement) {
        .write => |statement| {
            try std.testing.expectEqual(classifier.SqlWriteStatementKind.update, statement.kind);
            try std.testing.expect(!statement.recursive);
            try std.testing.expect(!write.isRecursiveWriteStatement());
        },
        else => return error.TestUnexpectedResult,
    }

    var recursive_write = try ParsedSql.initAlloc(alloc, "WITH RECURSIVE source_rows AS (SELECT id FROM usage_records) INSERT INTO archive(id) SELECT id FROM source_rows");
    defer recursive_write.deinit(alloc);
    switch (recursive_write.statement) {
        .write => |statement| {
            try std.testing.expectEqual(classifier.SqlWriteStatementKind.insert_source, statement.kind);
            try std.testing.expect(statement.recursive);
            try std.testing.expect(recursive_write.isRecursiveWriteStatement());
        },
        else => return error.TestUnexpectedResult,
    }

    var explain = try ParsedSql.initAlloc(alloc, "EXPLAIN SELECT id FROM usage_records;");
    defer explain.deinit(alloc);
    switch (explain.statement) {
        .explain => |statement| {
            try std.testing.expectEqualStrings("EXPLAIN SELECT id FROM usage_records", statement.raw.sql(explain.sql()));
            try std.testing.expect(!statement.analyze);
            try std.testing.expectEqual(ast.SqlExplainFormat.text, statement.format);
            try std.testing.expect(!statement.verbose);
            try std.testing.expect(statement.costs);
            try std.testing.expectEqual(@as(?usize, 1), statement.inner_token_start);
            try std.testing.expectEqual(@as(?usize, 5), statement.inner_token_end);
            try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.unsupported, explain.generatedStatementKind().?);
            switch (explain.generated_statement.?.ast.?) {
                .unsupported => |unsupported| {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlUnsupportedKind.explain, unsupported.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlUnsupportedReason.explain_not_planned_by_generated_parser, unsupported.reason);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 5 }, unsupported.subject_tokens.?);
                },
                else => return error.TestUnexpectedResult,
            }
        },
        else => return error.TestUnexpectedResult,
    }

    var explain_options = try ParsedSql.initAlloc(alloc, "EXPLAIN (FORMAT JSON, VERBOSE, COSTS OFF, ANALYZE ON, BUFFERS, TIMING OFF, SUMMARY OFF, SETTINGS ON, WAL) SELECT id FROM usage_records");
    defer explain_options.deinit(alloc);
    switch (explain_options.statement) {
        .explain => |statement| {
            try std.testing.expect(statement.analyze);
            try std.testing.expectEqual(ast.SqlExplainFormat.json, statement.format);
            try std.testing.expect(statement.verbose);
            try std.testing.expect(!statement.costs);
            try std.testing.expect(statement.buffers);
            try std.testing.expect(!statement.timing);
            try std.testing.expect(!statement.summary);
            try std.testing.expect(statement.settings);
            try std.testing.expect(statement.wal);
            try std.testing.expect(statement.inner_token_start != null);
            try std.testing.expect(statement.inner_token_end != null);
            try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.unsupported, explain_options.generatedStatementKind().?);
            switch (explain_options.generated_statement.?.ast.?) {
                .unsupported => |unsupported| {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlUnsupportedKind.explain, unsupported.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlUnsupportedReason.explain_not_planned_by_generated_parser, unsupported.reason);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = statement.inner_token_start.?, .end = statement.inner_token_end.? }, unsupported.subject_tokens.?);
                },
                else => return error.TestUnexpectedResult,
            }
        },
        else => return error.TestUnexpectedResult,
    }

    var explain_analyze = try ParsedSql.initAlloc(alloc, "EXPLAIN ANALYZE INSERT INTO usage_records (id) VALUES ('u1')");
    defer explain_analyze.deinit(alloc);
    switch (explain_analyze.statement) {
        .explain => |statement| {
            try std.testing.expect(statement.analyze);
            try std.testing.expectEqualStrings("INSERT", explain_analyze.items()[statement.inner_token_start.?].text);
            try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.unsupported, explain_analyze.generatedStatementKind().?);
            switch (explain_analyze.generated_statement.?.ast.?) {
                .unsupported => |unsupported| {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlUnsupportedKind.explain, unsupported.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlUnsupportedReason.explain_not_planned_by_generated_parser, unsupported.reason);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = statement.inner_token_start.?, .end = statement.inner_token_end.? }, unsupported.subject_tokens.?);
                },
                else => return error.TestUnexpectedResult,
            }
        },
        else => return error.TestUnexpectedResult,
    }

    var invalid_explain = try ParsedSql.initAlloc(alloc, "EXPLAIN (FORMAT YAML) SELECT 1");
    defer invalid_explain.deinit(alloc);
    switch (invalid_explain.statement) {
        .explain => |statement| {
            try std.testing.expect(statement.inner_token_start == null);
            try std.testing.expect(statement.inner_token_end == null);
            try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.unsupported, invalid_explain.generatedStatementKind().?);
            switch (invalid_explain.generated_statement.?.ast.?) {
                .unsupported => |unsupported| {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlUnsupportedKind.explain, unsupported.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlUnsupportedReason.explain_not_planned_by_generated_parser, unsupported.reason);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 7 }, unsupported.subject_tokens.?);
                },
                else => return error.TestUnexpectedResult,
            }
        },
        else => return error.TestUnexpectedResult,
    }

    var empty_explain = try ParsedSql.initAlloc(alloc, "EXPLAIN");
    defer empty_explain.deinit(alloc);
    switch (empty_explain.statement) {
        .explain => |statement| {
            try std.testing.expect(statement.inner_token_start == null);
            try std.testing.expect(statement.inner_token_end == null);
            try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.unsupported, empty_explain.generatedStatementKind().?);
            switch (empty_explain.generated_statement.?.ast.?) {
                .unsupported => |unsupported| {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlUnsupportedKind.explain, unsupported.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlUnsupportedReason.explain_not_planned_by_generated_parser, unsupported.reason);
                    try std.testing.expect(unsupported.subject_tokens == null);
                },
                else => return error.TestUnexpectedResult,
            }
        },
        else => return error.TestUnexpectedResult,
    }

    const unsupported_diagnostics = [_]struct {
        sql: []const u8,
        kind: generated_parser.GeneratedSqlUnsupportedKind,
        reason: generated_parser.GeneratedSqlUnsupportedReason,
    }{
        .{
            .sql = "ALTER FOREIGN TABLE foreign_usage_records RENAME TO foreign_usage_archive",
            .kind = .alter_foreign_table,
            .reason = .alter_foreign_table_not_planned_by_generated_parser,
        },
        .{
            .sql = "ALTER MATERIALIZED VIEW usage_summary RENAME TO usage_summary_v2",
            .kind = .alter_materialized_view,
            .reason = .alter_materialized_view_not_planned_by_generated_parser,
        },
        .{
            .sql = "ALTER POLICY usage_policy ON usage_records RENAME TO usage_policy_v2",
            .kind = .alter_policy,
            .reason = .alter_policy_not_planned_by_generated_parser,
        },
        .{
            .sql = "ALTER PUBLICATION usage_pub ADD TABLE usage_records",
            .kind = .alter_publication,
            .reason = .alter_publication_not_planned_by_generated_parser,
        },
        .{
            .sql = "ALTER RULE usage_insert ON usage_records RENAME TO usage_insert_v2",
            .kind = .alter_rule,
            .reason = .alter_rule_not_planned_by_generated_parser,
        },
        .{
            .sql = "ALTER SERVER usage_server VERSION '15'",
            .kind = .alter_server,
            .reason = .alter_server_not_planned_by_generated_parser,
        },
        .{
            .sql = "ALTER SUBSCRIPTION usage_sub DISABLE",
            .kind = .alter_subscription,
            .reason = .alter_subscription_not_planned_by_generated_parser,
        },
        .{
            .sql = "ALTER TRIGGER usage_audit ON usage_records RENAME TO usage_audit_v2",
            .kind = .alter_trigger,
            .reason = .alter_trigger_not_planned_by_generated_parser,
        },
        .{
            .sql = "CALL refresh_usage_records()",
            .kind = .call,
            .reason = .call_not_planned_by_generated_parser,
        },
        .{
            .sql = "CHECKPOINT",
            .kind = .checkpoint,
            .reason = .checkpoint_not_planned_by_generated_parser,
        },
        .{
            .sql = "CLOSE usage_cursor",
            .kind = .close,
            .reason = .close_not_planned_by_generated_parser,
        },
        .{
            .sql = "CLUSTER usage_records USING usage_status_idx",
            .kind = .cluster,
            .reason = .cluster_not_planned_by_generated_parser,
        },
        .{
            .sql = "COMMENT ON TABLE usage_records IS 'billing rows'",
            .kind = .comment,
            .reason = .comment_not_planned_by_generated_parser,
        },
        .{
            .sql = "COPY usage_records (id, status) FROM STDIN WITH (FORMAT csv)",
            .kind = .copy,
            .reason = .copy_not_planned_by_generated_parser,
        },
        .{
            .sql = "CREATE FOREIGN TABLE foreign_usage_records (id text) SERVER usage_fdw",
            .kind = .create_foreign_table,
            .reason = .create_foreign_table_not_planned_by_generated_parser,
        },
        .{
            .sql = "CREATE MATERIALIZED VIEW usage_summary AS SELECT status FROM usage_records",
            .kind = .create_materialized_view,
            .reason = .create_materialized_view_not_planned_by_generated_parser,
        },
        .{
            .sql = "CREATE POLICY usage_policy ON usage_records USING (tenant_id = current_user)",
            .kind = .create_policy,
            .reason = .create_policy_not_planned_by_generated_parser,
        },
        .{
            .sql = "CREATE PUBLICATION usage_pub FOR TABLE usage_records",
            .kind = .create_publication,
            .reason = .create_publication_not_planned_by_generated_parser,
        },
        .{
            .sql = "CREATE RULE usage_insert AS ON INSERT TO usage_records DO ALSO NOTIFY usage_events",
            .kind = .create_rule,
            .reason = .create_rule_not_planned_by_generated_parser,
        },
        .{
            .sql = "CREATE SERVER usage_server FOREIGN DATA WRAPPER postgres_fdw",
            .kind = .create_server,
            .reason = .create_server_not_planned_by_generated_parser,
        },
        .{
            .sql = "CREATE SUBSCRIPTION usage_sub CONNECTION 'host=example dbname=usage' PUBLICATION usage_pub",
            .kind = .create_subscription,
            .reason = .create_subscription_not_planned_by_generated_parser,
        },
        .{
            .sql = "CREATE TRIGGER usage_audit BEFORE INSERT ON usage_records FOR EACH ROW EXECUTE FUNCTION audit_usage()",
            .kind = .create_trigger,
            .reason = .create_trigger_not_planned_by_generated_parser,
        },
        .{
            .sql = "DECLARE usage_cursor NO SCROLL CURSOR FOR SELECT id FROM usage_records",
            .kind = .declare,
            .reason = .declare_not_planned_by_generated_parser,
        },
        .{
            .sql = "DO 'BEGIN NULL; END'",
            .kind = .do_block,
            .reason = .do_block_not_planned_by_generated_parser,
        },
        .{
            .sql = "DROP FOREIGN TABLE IF EXISTS foreign_usage_records",
            .kind = .drop_foreign_table,
            .reason = .drop_foreign_table_not_planned_by_generated_parser,
        },
        .{
            .sql = "FETCH FROM usage_cursor",
            .kind = .fetch,
            .reason = .fetch_not_planned_by_generated_parser,
        },
        .{
            .sql = "GRANT SELECT ON TABLE usage_records TO readonly",
            .kind = .grant,
            .reason = .grant_not_planned_by_generated_parser,
        },
        .{
            .sql = "LISTEN usage_events",
            .kind = .listen,
            .reason = .listen_not_planned_by_generated_parser,
        },
        .{
            .sql = "LOAD 'auto_explain'",
            .kind = .load,
            .reason = .load_not_planned_by_generated_parser,
        },
        .{
            .sql = "LOCK TABLE usage_records IN SHARE MODE",
            .kind = .lock,
            .reason = .lock_not_planned_by_generated_parser,
        },
        .{
            .sql = "MOVE FROM usage_cursor",
            .kind = .move,
            .reason = .move_not_planned_by_generated_parser,
        },
        .{
            .sql = "NOTIFY usage_events, 'changed'",
            .kind = .notify,
            .reason = .notify_not_planned_by_generated_parser,
        },
        .{
            .sql = "REFRESH MATERIALIZED VIEW usage_summary",
            .kind = .refresh,
            .reason = .refresh_not_planned_by_generated_parser,
        },
        .{
            .sql = "VACUUM (FULL, VERBOSE, ANALYZE) public.usage_records",
            .kind = .vacuum,
            .reason = .vacuum_not_planned_by_generated_parser,
        },
        .{
            .sql = "REINDEX INDEX CONCURRENTLY public.usage_status_idx",
            .kind = .reindex,
            .reason = .reindex_not_planned_by_generated_parser,
        },
        .{
            .sql = "RELEASE SAVEPOINT usage_batch",
            .kind = .release,
            .reason = .release_not_planned_by_generated_parser,
        },
        .{
            .sql = "REVOKE SELECT ON TABLE usage_records FROM readonly",
            .kind = .revoke,
            .reason = .revoke_not_planned_by_generated_parser,
        },
        .{
            .sql = "SAVEPOINT usage_batch",
            .kind = .savepoint,
            .reason = .savepoint_not_planned_by_generated_parser,
        },
        .{
            .sql = "SECURITY LABEL ON TABLE usage_records IS 'internal'",
            .kind = .security_label,
            .reason = .security_label_not_planned_by_generated_parser,
        },
        .{
            .sql = "DROP MATERIALIZED VIEW IF EXISTS usage_summary CASCADE",
            .kind = .drop_materialized_view,
            .reason = .drop_materialized_view_not_planned_by_generated_parser,
        },
        .{
            .sql = "DROP POLICY IF EXISTS usage_policy ON usage_records",
            .kind = .drop_policy,
            .reason = .drop_policy_not_planned_by_generated_parser,
        },
        .{
            .sql = "DROP PUBLICATION IF EXISTS usage_pub",
            .kind = .drop_publication,
            .reason = .drop_publication_not_planned_by_generated_parser,
        },
        .{
            .sql = "DROP RULE IF EXISTS usage_insert ON usage_records",
            .kind = .drop_rule,
            .reason = .drop_rule_not_planned_by_generated_parser,
        },
        .{
            .sql = "DROP SERVER IF EXISTS usage_server CASCADE",
            .kind = .drop_server,
            .reason = .drop_server_not_planned_by_generated_parser,
        },
        .{
            .sql = "DROP SUBSCRIPTION IF EXISTS usage_sub",
            .kind = .drop_subscription,
            .reason = .drop_subscription_not_planned_by_generated_parser,
        },
        .{
            .sql = "DROP TRIGGER IF EXISTS usage_audit ON usage_records",
            .kind = .drop_trigger,
            .reason = .drop_trigger_not_planned_by_generated_parser,
        },
        .{
            .sql = "UNLISTEN *",
            .kind = .unlisten,
            .reason = .unlisten_not_planned_by_generated_parser,
        },
    };
    for (unsupported_diagnostics) |case| {
        var parsed = try ParsedSql.initAlloc(alloc, case.sql);
        defer parsed.deinit(alloc);
        try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.unsupported, parsed.generatedStatementKind().?);
        switch (parsed.generated_statement.?.ast.?) {
            .unsupported => |unsupported| {
                try std.testing.expectEqual(case.kind, unsupported.kind);
                try std.testing.expectEqual(case.reason, unsupported.reason);
                if (parsed.items().len == 1) {
                    try std.testing.expect(unsupported.subject_tokens == null);
                } else {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = parsed.items().len }, unsupported.subject_tokens.?);
                }
            },
            else => return error.TestUnexpectedResult,
        }
        if (generatedUnsupportedUsesLegacyPlanner(case.kind)) {
            switch (parsed.statement) {
                .unsupported => return error.TestUnexpectedResult,
                else => {},
            }
        } else {
            switch (parsed.statement) {
                .unsupported => |unsupported| {
                    try std.testing.expectEqual(case.kind, unsupported.kind);
                    try std.testing.expectEqualStrings(case.sql, unsupported.raw.sql(parsed.sql()));
                },
                else => return error.TestUnexpectedResult,
            }
            try std.testing.expect(parsed.readStatementKind() == null);
            try std.testing.expect(parsed.writeStatementKind() == null);
        }
    }

    var session = try ParsedSql.initAlloc(alloc, "SET search_path TO public");
    defer session.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.session, session.generatedStatementKind().?);
    switch (session.generated_statement.?.ast.?) {
        .session => |generated_session| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlSessionKind.set, generated_session.kind);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 2 }, generated_session.name_tokens.?);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 4 }, generated_session.value_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (session.statement) {
        .session => {},
        else => return error.TestUnexpectedResult,
    }

    var prepared = try ParsedSql.initAlloc(alloc, "PREPARE read_stmt AS SELECT id FROM usage_records");
    defer prepared.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.prepared, prepared.generatedStatementKind().?);
    switch (prepared.statement) {
        .prepared => |statement| try std.testing.expectEqualStrings("PREPARE read_stmt AS SELECT id FROM usage_records", statement.raw.sql(prepared.sql())),
        else => return error.TestUnexpectedResult,
    }

    var ddl = try ParsedSql.initAlloc(alloc, "CREATE TABLE usage_records (id text)");
    defer ddl.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.ddl, ddl.generatedStatementKind().?);
    switch (ddl.statement) {
        .ddl => {},
        else => return error.TestUnexpectedResult,
    }

    var extension_index = try ParsedSql.initAlloc(alloc, "CREATE INDEX usage_status_idx ON usage_records (status)");
    defer extension_index.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.extension_index, extension_index.generatedStatementKind().?);
    switch (extension_index.generated_statement.?.ast.?) {
        .extension_index => |extension_index_ast| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlDdlKind.create_index, extension_index_ast.kind);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 2, .end = 3 }, extension_index_ast.object_name_tokens.?);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 4, .end = 5 }, extension_index_ast.index_table_tokens.?);
            try std.testing.expect(extension_index_ast.index_method_tokens == null);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, extension_index_ast.index_elements_tokens.?);
            try std.testing.expect(extension_index_ast.index_include_tokens == null);
            try std.testing.expect(extension_index_ast.index_options_tokens == null);
            try std.testing.expect(extension_index_ast.index_where_tokens == null);
            try std.testing.expect(!extension_index_ast.unique);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (extension_index.statement) {
        .ddl => {},
        else => return error.TestUnexpectedResult,
    }

    var drop_table = try ParsedSql.initAlloc(alloc, "DROP TABLE IF EXISTS usage_records CASCADE");
    defer drop_table.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.ddl, drop_table.generatedStatementKind().?);
    switch (drop_table.generated_statement.?.ast.?) {
        .ddl => |ddl_ast| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlDdlKind.drop_table, ddl_ast.kind);
            try std.testing.expect(ddl_ast.if_exists);
            try std.testing.expect(ddl_ast.cascade);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 4, .end = 5 }, ddl_ast.object_name_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (drop_table.statement) {
        .ddl => {},
        else => return error.TestUnexpectedResult,
    }

    var drop_index = try ParsedSql.initAlloc(alloc, "DROP INDEX IF EXISTS usage_status_idx RESTRICT");
    defer drop_index.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.extension_index, drop_index.generatedStatementKind().?);
    switch (drop_index.generated_statement.?.ast.?) {
        .extension_index => |ddl_ast| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlDdlKind.drop_index, ddl_ast.kind);
            try std.testing.expect(ddl_ast.if_exists);
            try std.testing.expect(!ddl_ast.cascade);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 4, .end = 5 }, ddl_ast.object_name_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (drop_index.statement) {
        .ddl => {},
        else => return error.TestUnexpectedResult,
    }

    var alter_table = try ParsedSql.initAlloc(alloc, "ALTER TABLE IF EXISTS ONLY usage_records DROP COLUMN IF EXISTS status RESTRICT");
    defer alter_table.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.ddl, alter_table.generatedStatementKind().?);
    switch (alter_table.generated_statement.?.ast.?) {
        .ddl => |ddl_ast| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlDdlKind.alter_table, ddl_ast.kind);
            try std.testing.expect(ddl_ast.if_exists);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, ddl_ast.object_name_tokens.?);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 12 }, ddl_ast.alter_table_operation_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (alter_table.statement) {
        .ddl => {},
        else => return error.TestUnexpectedResult,
    }

    var covering_partial_index = try ParsedSql.initAlloc(alloc, "CREATE UNIQUE INDEX usage_status_active_idx ON usage_records (status) INCLUDE (tenant_id, amount) WHERE deleted_at IS NULL");
    defer covering_partial_index.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.extension_index, covering_partial_index.generatedStatementKind().?);
    switch (covering_partial_index.generated_statement.?.ast.?) {
        .extension_index => |extension_index_ast| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlDdlKind.create_index, extension_index_ast.kind);
            try std.testing.expect(extension_index_ast.unique);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 4 }, extension_index_ast.object_name_tokens.?);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, extension_index_ast.index_table_tokens.?);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, extension_index_ast.index_elements_tokens.?);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 11, .end = 14 }, extension_index_ast.index_include_tokens.?);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 16, .end = 19 }, extension_index_ast.index_where_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (covering_partial_index.statement) {
        .ddl => {},
        else => return error.TestUnexpectedResult,
    }

    var extension = try ParsedSql.initAlloc(alloc, "CREATE EXTENSION vector");
    defer extension.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.extension_index, extension.generatedStatementKind().?);
    switch (extension.generated_statement.?.ast.?) {
        .extension_index => |extension_index_ast| try std.testing.expectEqual(generated_parser.GeneratedSqlDdlKind.create_extension, extension_index_ast.kind),
        else => return error.TestUnexpectedResult,
    }
    switch (extension.statement) {
        .ddl => {},
        else => return error.TestUnexpectedResult,
    }

    var generated_read = try ParsedSql.initAlloc(alloc, "SELECT id FROM docs WHERE status = 'active' LIMIT 5");
    defer generated_read.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.read, generated_read.generatedStatementKind().?);
    switch (generated_read.generated_statement.?.ast.?) {
        .read => |read_ast| try std.testing.expectEqual(generated_parser.GeneratedSqlReadKind.query, read_ast.kind),
        else => return error.TestUnexpectedResult,
    }
}

test "sql adapter parsed sql retains generated DML nodes for covered write corpus" {
    const alloc = std.testing.allocator;

    const cases = [_]struct {
        sql: []const u8,
        generated: generated_parser.GeneratedSqlDmlKind,
        write: classifier.SqlWriteStatementKind,
    }{
        .{ .sql = "INSERT INTO usage_records (id, status) VALUES ('u1', 'open')", .generated = .insert_values, .write = .insert },
        .{ .sql = "INSERT INTO usage_records (id) SELECT id FROM incoming_usage", .generated = .insert_select, .write = .insert_source },
        .{ .sql = "UPDATE usage_records SET status = 'done' WHERE id = 'u1'", .generated = .update, .write = .update },
        .{ .sql = "DELETE FROM usage_records WHERE id = 'u1'", .generated = .delete, .write = .delete },
        .{ .sql = "TRUNCATE usage_records", .generated = .truncate, .write = .truncate },
        .{ .sql = "MERGE INTO usage_records USING source_rows ON usage_records.id = source_rows.id WHEN MATCHED THEN UPDATE SET status = source_rows.status", .generated = .merge, .write = .merge },
    };

    for (cases) |case| {
        var parsed = try ParsedSql.initAlloc(alloc, case.sql);
        defer parsed.deinit(alloc);
        try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.dml, parsed.generatedStatementKind().?);
        try std.testing.expectEqual(case.write, parsed.writeStatementKind().?);
        switch (parsed.generated_statement.?.statement) {
            .dml => |kind| try std.testing.expectEqual(case.generated, kind),
            else => return error.TestUnexpectedResult,
        }
        switch (parsed.generated_statement.?.ast.?) {
            .dml => |dml_ast| try std.testing.expectEqual(case.generated, dml_ast.kind),
            else => return error.TestUnexpectedResult,
        }
        switch (parsed.statement) {
            .write => |statement| try std.testing.expectEqual(case.write, statement.kind),
            else => return error.TestUnexpectedResult,
        }
    }
}

test "sql adapter parsed sql write statement kind can come from generated AST" {
    const alloc = std.testing.allocator;

    var generated_insert_source = try ParsedSql.initAlloc(alloc, "INSERT INTO usage_records (id) SELECT id FROM incoming_usage");
    defer generated_insert_source.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.dml, generated_insert_source.generatedStatementKind().?);

    generated_insert_source.tokenized_sql.write_statement_kind = null;
    generated_insert_source.statement = parseStatement(generated_insert_source.raw_statement, generated_insert_source.generated_statement, &generated_insert_source.tokenized_sql);
    try std.testing.expectEqual(classifier.SqlWriteStatementKind.insert_source, generated_insert_source.writeStatementKind().?);

    var generated_recursive_insert = try ParsedSql.initAlloc(alloc, "WITH RECURSIVE source_rows AS (SELECT id FROM usage_records) INSERT INTO archive(id) SELECT id FROM source_rows");
    defer generated_recursive_insert.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.dml, generated_recursive_insert.generatedStatementKind().?);

    generated_recursive_insert.tokenized_sql.write_statement_kind = null;
    generated_recursive_insert.statement = parseStatement(generated_recursive_insert.raw_statement, generated_recursive_insert.generated_statement, &generated_recursive_insert.tokenized_sql);
    try std.testing.expectEqual(classifier.SqlWriteStatementKind.insert_source, generated_recursive_insert.writeStatementKind().?);
    try std.testing.expect(generated_recursive_insert.isRecursiveWriteStatement());

    var generated_recursive_update = try ParsedSql.initAlloc(
        alloc,
        "WITH RECURSIVE source_rows AS (SELECT id, status FROM incoming_usage) UPDATE usage_records SET status = source_rows.status FROM source_rows WHERE usage_records.id = source_rows.id",
    );
    defer generated_recursive_update.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.dml, generated_recursive_update.generatedStatementKind().?);
    try std.testing.expectEqual(classifier.SqlWriteStatementKind.update_joined_source, generated_recursive_update.writeStatementKind().?);
    try std.testing.expect(generated_recursive_update.isRecursiveWriteStatement());
}

test "sql adapter parsed sql write statement kind fails closed on classifier disagreement" {
    const alloc = std.testing.allocator;

    var generated_insert = try ParsedSql.initAlloc(alloc, "INSERT INTO usage_records (id, status) VALUES ('u1', 'open')");
    defer generated_insert.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.dml, generated_insert.generatedStatementKind().?);

    generated_insert.tokenized_sql.write_statement_kind = .delete;
    generated_insert.statement = parseStatement(generated_insert.raw_statement, generated_insert.generated_statement, &generated_insert.tokenized_sql);
    try std.testing.expect(generated_insert.writeStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_insert.statement));
}

test "sql adapter parsed sql retains generated read nodes for covered query corpus" {
    const alloc = std.testing.allocator;

    const cases = [_]struct {
        sql: []const u8,
        generated: generated_parser.GeneratedSqlReadKind,
        read: classifier.SqlReadStatementKind,
    }{
        .{ .sql = "SELECT id, status FROM usage_records WHERE status = 'open' ORDER BY id LIMIT 10", .generated = .query, .read = .query },
        .{ .sql = "SELECT status AS state, id FROM usage_records", .generated = .query, .read = .query },
        .{ .sql = "SELECT status state, id FROM usage_records", .generated = .query, .read = .query },
        .{ .sql = "SELECT CAST(id AS text) AS id_text FROM usage_records WHERE id = 'u1'", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE CAST(amount + 1 AS text) = '2'", .generated = .query, .read = .query },
        .{ .sql = "SELECT id::text AS id_text FROM usage_records WHERE id::text = 'u1'", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE metadata->'flags' = $1::jsonb", .generated = .query, .read = .query },
        .{ .sql = "SELECT metadata #>> '{billing,plan}' AS plan FROM usage_records WHERE metadata #> '{flags}' = $1::jsonb", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE metadata #>> '{billing,plan}' = 'pro'", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE status = ANY($1::text[])", .generated = .query, .read = .query },
        .{ .sql = "SELECT date_bin(INTERVAL '1 hour', amount, 0) AS amount_bucket FROM usage_records WHERE date_bin(INTERVAL '1 day', amount, 0) = $1", .generated = .query, .read = .query },
        .{ .sql = "SELECT date_bin(INTERVAL '1 hour', TIMESTAMPTZ '2025-01-01T01:30:00+01:30', TIMESTAMP '2025-01-01T00:00:00') AS planned_bucket FROM usage_records WHERE id = $1", .generated = .query, .read = .query },
        .{ .sql = "SELECT EXTRACT(dow FROM amount) AS amount_dow FROM usage_records WHERE EXTRACT(hour FROM amount) = $1", .generated = .query, .read = .query },
        .{ .sql = "SELECT date_part('hour', amount) AS amount_hour, EXTRACT(dow FROM amount) AS amount_dow FROM usage_records WHERE EXTRACT(dow FROM amount) = $1 ORDER BY date_part('month', amount) ASC LIMIT 5", .generated = .query, .read = .query },
        .{ .sql = "SELECT CURRENT_TIMESTAMP(6) AS planned_at_ns FROM users WHERE id = $1", .generated = .query, .read = .query },
        .{ .sql = "SELECT CURRENT_DATE AS planned_day_ns FROM users WHERE id = $1", .generated = .query, .read = .query },
        .{ .sql = "SELECT lower(p.valid_at) AS valid_start, upper(p.valid_at) AS valid_end FROM price_intervals AS p WHERE lower(p.valid_at) >= 1 AND upper(p.valid_at) IS NOT NULL ORDER BY upper(p.valid_at) DESC LIMIT 5", .generated = .query, .read = .query },
        .{ .sql = "SELECT CASE WHEN email IS NULL THEN 'missing' WHEN email = 'blocked@example.test' THEN 'blocked' ELSE lower(status) END AS email_bucket FROM usage_records WHERE id = 'u1'", .generated = .query, .read = .query },
        .{ .sql = "SELECT CASE WHEN email IS NULL THEN NULL ELSE email END AS maybe_email FROM usage_records WHERE id = 'u1'", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE status LIKE 'open%'", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE status ILIKE 'open%'", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE status LIKE 'op!_%' ESCAPE '!'", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE lower(status) ILIKE 'op!_%' ESCAPE '!'", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE lower(status) LIKE ANY(ARRAY['op%', 'ready%'])", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE status LIKE SOME(ARRAY['op%', 'ready%'])", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE name ILIKE ALL(ARRAY['ada%', 'grace%'])", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE id IN ('u1', 'u2')", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE score BETWEEN 1 AND 10", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE priority BETWEEN SYMMETRIC 20 AND 10", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE array_length(tags, 1) BETWEEN SYMMETRIC 3 AND 1", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE status NOT LIKE 'closed%'", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE status NOT ILIKE 'closed%'", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE status NOT LIKE 'cl!_%' ESCAPE '!'", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE lower(status) NOT ILIKE 'cl!_%' ESCAPE '!'", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE lower(name) NOT ILIKE ALL(ARRAY['bot%', 'sys%'])", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE id NOT IN ('u1', 'u2')", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE score NOT BETWEEN 1 AND 10", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE priority NOT BETWEEN ASYMMETRIC 10 AND 20", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE priority NOT BETWEEN SYMMETRIC 20 AND 10", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE score = ANY (1, 2)", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE score <> ALL (1, 2)", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE score > SOME (1, 2)", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE status = ANY(ARRAY['active','pending']::text[])", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE score = ANY (SELECT score FROM thresholds WHERE active IS TRUE)", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE score <> ALL (SELECT score FROM archived_thresholds)", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE tags @> ARRAY['hot','new']", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE tags && ARRAY['hot','new']", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE metadata ? 'flags'", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE metadata ?| ARRAY['flags','billing']", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE metadata ?& ARRAY['flags','billing']", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE status ~ 'op.*'", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE status ~* 'op.*'", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE status !~ 'closed.*'", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE status !~* 'closed.*'", .generated = .query, .read = .query },
        .{ .sql = "SELECT first_name || ' ' || last_name FROM usage_records", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE status || ':' || id = 'open:u1'", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE deleted_at IS NULL", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE deleted_at IS NOT NULL", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE status ISNULL", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE lower(status) NOTNULL", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE active IS TRUE", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE active IS NOT FALSE", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE active IS UNKNOWN", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE active IS NOT UNKNOWN", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE status IS DISTINCT FROM previous_status", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE status IS NOT DISTINCT FROM previous_status", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE status = 'open' OR deleted_at IS NULL", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE status = 'open' AND deleted_at IS NULL", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE NOT deleted_at IS NULL", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE (status = 'open')", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE NOT (deleted_at IS NULL)", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE score + bonus > 10", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE score * weight > 10", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE payload ->> 'status' = 'open'", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records WHERE lower(status) = 'open'", .generated = .query, .read = .query },
        .{ .sql = "SELECT concat_ws(',', status), id FROM usage_records ORDER BY status, id", .generated = .query, .read = .query },
        .{ .sql = "SELECT COUNT(*) AS total FROM usage_records", .generated = .aggregate, .read = .aggregate },
        .{ .sql = "SELECT customer, COUNT(*) FILTER (WHERE status = 'open') AS open_count FROM usage_records GROUP BY customer", .generated = .aggregate, .read = .aggregate },
        .{ .sql = "SELECT customer, COUNT(DISTINCT status) AS status_count FROM usage_records GROUP BY customer", .generated = .aggregate, .read = .aggregate },
        .{ .sql = "SELECT customer, ARRAY_AGG(DISTINCT status ORDER BY amount DESC) AS statuses FROM usage_records GROUP BY customer", .generated = .aggregate, .read = .aggregate },
        .{ .sql = "SELECT customer, percentile_cont(0.5) WITHIN GROUP (ORDER BY amount DESC NULLS LAST) AS median_amount FROM usage_records GROUP BY customer", .generated = .aggregate, .read = .aggregate },
        .{ .sql = "SELECT id, row_number() OVER (PARTITION BY tenant, account ORDER BY id) AS rn FROM usage_records ORDER BY id, tenant", .generated = .window, .read = .window },
        .{ .sql = "SELECT DISTINCT status FROM usage_records ORDER BY status", .generated = .aggregate, .read = .aggregate },
        .{ .sql = "SELECT DISTINCT ON (organization_id) organization_id, id FROM usage_records ORDER BY organization_id ASC, created_at DESC", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records ORDER BY id LIMIT ALL OFFSET 2 ROWS", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records ORDER BY created_at DESC NULLS LAST, score ASC NULLS FIRST", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records ORDER BY 1 USING > LIMIT 5", .generated = .query, .read = .query },
        .{ .sql = "SELECT id FROM usage_records FETCH FIRST ROWS ONLY", .generated = .query, .read = .query },
        .{ .sql = "SELECT status FROM usage_records GROUP BY status HAVING status = 'open'", .generated = .aggregate, .read = .aggregate },
        .{ .sql = "SELECT usage_records.id FROM usage_records JOIN accounts ON usage_records.account_id = accounts.id", .generated = .join, .read = .join },
        .{ .sql = "SELECT usage_records.id FROM usage_records JOIN accounts ON usage_records.account_id = accounts.id JOIN tenants ON accounts.tenant_id = tenants.id", .generated = .join, .read = .join },
        .{ .sql = "SELECT usage_records.id FROM usage_records LEFT OUTER JOIN accounts ON usage_records.account_id = accounts.id", .generated = .join, .read = .join },
        .{ .sql = "SELECT id FROM LATERAL (SELECT id FROM usage_records) AS source_rows", .generated = .lateral, .read = .lateral },
        .{ .sql = "SELECT id, row_number() OVER (ORDER BY id) AS rn FROM usage_records", .generated = .window, .read = .window },
        .{ .sql = "SELECT id, row_number() OVER (PARTITION BY tenant ORDER BY id) AS rn FROM usage_records", .generated = .window, .read = .window },
        .{ .sql = "SELECT id, row_number() OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rn FROM usage_records", .generated = .window, .read = .window },
        .{ .sql = "SELECT id, row_number() OVER usage_window AS rn FROM usage_records WINDOW usage_window AS (ORDER BY id)", .generated = .window, .read = .window },
        .{ .sql = "SELECT id, row_number() OVER usage_window AS rn FROM usage_records WINDOW usage_window AS (PARTITION BY tenant ORDER BY id)", .generated = .window, .read = .window },
        .{ .sql = "SELECT id, row_number() OVER usage_window AS rn FROM usage_records WINDOW usage_window AS (ORDER BY id RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)", .generated = .window, .read = .window },
        .{ .sql = "SELECT id FROM usage_records UNION SELECT id FROM usage_archive", .generated = .set_operation, .read = .set_operation },
        .{ .sql = "WITH source_rows AS (SELECT id FROM usage_records) SELECT id FROM source_rows", .generated = .cte, .read = .query },
        .{ .sql = "WITH source_rows AS MATERIALIZED (SELECT id FROM usage_records) SELECT id FROM source_rows", .generated = .cte, .read = .query },
        .{ .sql = "WITH source_rows(source_id) AS NOT MATERIALIZED (SELECT id FROM usage_records) SELECT source_id FROM source_rows", .generated = .cte, .read = .query },
        .{ .sql = "WITH first_rows AS (SELECT id FROM usage_records), second_rows AS (SELECT id FROM first_rows) SELECT id FROM second_rows", .generated = .cte, .read = .query },
        .{ .sql = "WITH RECURSIVE source_rows AS (SELECT id FROM usage_records) SELECT id FROM source_rows", .generated = .cte, .read = .recursive_cte },
    };

    for (cases) |case| {
        var parsed = try ParsedSql.initAlloc(alloc, case.sql);
        defer parsed.deinit(alloc);
        try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.read, parsed.generatedStatementKind().?);
        try std.testing.expectEqual(case.read, parsed.readStatementKind().?);
        switch (parsed.generated_statement.?.statement) {
            .read => |kind| try std.testing.expectEqual(case.generated, kind),
            else => return error.TestUnexpectedResult,
        }
        switch (parsed.generated_statement.?.ast.?) {
            .read => |read_ast| {
                try std.testing.expectEqual(case.generated, read_ast.kind);
                if (std.mem.eql(u8, case.sql, "SELECT id, status FROM usage_records WHERE status = 'open' ORDER BY id LIMIT 10")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 4 }, read_ast.projection_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.source_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 10 }, read_ast.where_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read_ast.where_expression.right_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 12, .end = 13 }, read_ast.order_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 14, .end = 15 }, read_ast.limit_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.token_range, read_ast.limit_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 14, .end = 15 }, read_ast.limit_expression.tokens.?);
                    try std.testing.expectEqual(@as(usize, 2), read_ast.projection_items.count);
                    try std.testing.expectEqual(@as(usize, 2), read_ast.projection_items.items.len);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read_ast.projection_items.items[0]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read_ast.projection_items.items[1]);
                    try std.testing.expectEqual(@as(usize, 2), read_ast.projection_items.expressions.len);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.token_range, read_ast.projection_items.expressions[0].kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.token_range, read_ast.projection_items.expressions[1].kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.token_range, read_ast.projection_first_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read_ast.projection_first_expression.tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.token_range, read_ast.projection_last_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read_ast.projection_last_expression.tokens.?);
                    try std.testing.expectEqual(@as(usize, 1), read_ast.order_items.count);
                    try std.testing.expectEqual(@as(usize, 1), read_ast.order_items.items.len);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 12, .end = 13 }, read_ast.order_items.items[0]);
                    try std.testing.expectEqual(@as(usize, 1), read_ast.order_items.expressions.len);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.token_range, read_ast.order_items.expressions[0].kind);
                } else if (std.mem.eql(u8, case.sql, "SELECT status AS state, id FROM usage_records")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 6 }, read_ast.projection_tokens.?);
                    try std.testing.expectEqual(@as(usize, 2), read_ast.projection_items.count);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 4 }, read_ast.projection_items.items[0]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read_ast.projection_items.expression_items[0]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 2, .end = 4 }, read_ast.projection_items.alias_items[0].?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read_ast.projection_items.alias_name_items[0].?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.projection_items.items[1]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.projection_items.expression_items[1]);
                    try std.testing.expect(read_ast.projection_items.alias_items[1] == null);
                    try std.testing.expect(read_ast.projection_items.alias_name_items[1] == null);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.token_range, read_ast.projection_items.expressions[0].kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read_ast.projection_items.expressions[0].tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT status state, id FROM usage_records")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 5 }, read_ast.projection_tokens.?);
                    try std.testing.expectEqual(@as(usize, 2), read_ast.projection_items.count);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 3 }, read_ast.projection_items.items[0]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read_ast.projection_items.expression_items[0]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 2, .end = 3 }, read_ast.projection_items.alias_items[0].?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 2, .end = 3 }, read_ast.projection_items.alias_name_items[0].?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 4, .end = 5 }, read_ast.projection_items.items[1]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 4, .end = 5 }, read_ast.projection_items.expression_items[1]);
                    try std.testing.expect(read_ast.projection_items.alias_items[1] == null);
                    try std.testing.expect(read_ast.projection_items.alias_name_items[1] == null);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.token_range, read_ast.projection_items.expressions[0].kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read_ast.projection_items.expressions[0].tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT CAST(id AS text) AS id_text FROM usage_records WHERE id = 'u1'")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 9 }, read_ast.projection_tokens.?);
                    try std.testing.expectEqual(@as(usize, 1), read_ast.projection_items.count);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.cast, read_ast.projection_items.expressions[0].kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read_ast.projection_items.expressions[0].cast_expression_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.projection_items.expressions[0].cast_type_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read_ast.projection_items.alias_name_items[0].?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE CAST(amount + 1 AS text) = '2'")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.cast, read_ast.where_expression.left_expression_kind.?);
                    const cast_expression = read_ast.where_expression.left_expression orelse return error.TestUnexpectedResult;
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 10 }, cast_expression.cast_expression_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.additive, cast_expression.cast_expression_kind.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 11, .end = 12 }, cast_expression.cast_type_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id::text AS id_text FROM usage_records WHERE id::text = 'u1'")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.token_range, read_ast.projection_items.expressions[0].kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read_ast.projection_items.expressions[0].tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.where_expression.kind);
                    try std.testing.expect(read_ast.where_expression.left_expression_kind == null);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE metadata->'flags' = $1::jsonb")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.json_access, read_ast.where_expression.left_expression_kind.?);
                    try std.testing.expect(read_ast.where_expression.right_expression_kind == null);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE status = ANY($1::text[])")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.quantified_comparison, read_ast.where_expression.kind);
                    const grouped = read_ast.where_expression.right_expression orelse return error.TestUnexpectedResult;
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.token_range, grouped.inner_expression.?.kind);
                } else if (std.mem.eql(u8, case.sql, "SELECT date_bin(INTERVAL '1 hour', amount, 0) AS amount_bucket FROM usage_records WHERE date_bin(INTERVAL '1 day', amount, 0) = $1")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.function_call, read_ast.projection_first_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 9 }, read_ast.projection_first_expression.argument_tokens.?);
                    try std.testing.expectEqual(@as(usize, 3), read_ast.projection_first_expression.argument_items.count);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.interval_literal, read_ast.projection_first_expression.argument_items.expressions[0].kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 4, .end = 5 }, read_ast.projection_first_expression.argument_items.expressions[0].interval_value_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.function_call, read_ast.where_expression.left_expression_kind.?);
                    const predicate_call = read_ast.where_expression.left_expression orelse return error.TestUnexpectedResult;
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.function_call, predicate_call.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 17, .end = 23 }, predicate_call.argument_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.interval_literal, predicate_call.argument_items.expressions[0].kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 18, .end = 19 }, predicate_call.argument_items.expressions[0].interval_value_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 24, .end = 25 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 25, .end = 26 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT date_bin(INTERVAL '1 hour', TIMESTAMPTZ '2025-01-01T01:30:00+01:30', TIMESTAMP '2025-01-01T00:00:00') AS planned_bucket FROM usage_records WHERE id = $1")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.function_call, read_ast.projection_first_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 11 }, read_ast.projection_first_expression.argument_tokens.?);
                    try std.testing.expectEqual(@as(usize, 3), read_ast.projection_first_expression.argument_items.count);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.interval_literal, read_ast.projection_first_expression.argument_items.expressions[0].kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.timestamp_literal, read_ast.projection_first_expression.argument_items.expressions[1].kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.projection_first_expression.argument_items.expressions[1].timestamp_type_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.projection_first_expression.argument_items.expressions[1].timestamp_value_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.timestamp_literal, read_ast.projection_first_expression.argument_items.expressions[2].kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read_ast.projection_first_expression.argument_items.expressions[2].timestamp_type_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read_ast.projection_first_expression.argument_items.expressions[2].timestamp_value_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 18, .end = 19 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 19, .end = 20 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT EXTRACT(dow FROM amount) AS amount_dow FROM usage_records WHERE EXTRACT(hour FROM amount) = $1")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.extract_expression, read_ast.projection_first_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read_ast.projection_first_expression.extract_field_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.projection_first_expression.extract_source_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.extract_expression, read_ast.where_expression.left_expression_kind.?);
                    const predicate_extract = read_ast.where_expression.left_expression orelse return error.TestUnexpectedResult;
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.extract_expression, predicate_extract.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 14, .end = 15 }, predicate_extract.extract_field_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 16, .end = 17 }, predicate_extract.extract_source_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 18, .end = 19 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 19, .end = 20 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT date_part('hour', amount) AS amount_hour, EXTRACT(dow FROM amount) AS amount_dow FROM usage_records WHERE EXTRACT(dow FROM amount) = $1 ORDER BY date_part('month', amount) ASC LIMIT 5")) {
                    try std.testing.expectEqual(@as(usize, 2), read_ast.projection_items.count);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.function_call, read_ast.projection_items.expressions[0].kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read_ast.projection_items.expressions[0].function_name_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 6 }, read_ast.projection_items.expressions[0].argument_tokens.?);
                    try std.testing.expectEqual(@as(usize, 2), read_ast.projection_items.expressions[0].argument_items.count);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.extract_expression, read_ast.projection_items.expressions[1].kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 12, .end = 13 }, read_ast.projection_items.expressions[1].extract_field_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 14, .end = 15 }, read_ast.projection_items.expressions[1].extract_source_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.extract_expression, read_ast.where_expression.left_expression_kind.?);
                    const predicate_extract = read_ast.where_expression.left_expression orelse return error.TestUnexpectedResult;
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 23, .end = 24 }, predicate_extract.extract_field_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 25, .end = 26 }, predicate_extract.extract_source_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 27, .end = 28 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 28, .end = 29 }, read_ast.where_expression.right_tokens.?);
                    try std.testing.expectEqual(@as(usize, 1), read_ast.order_items.count);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.function_call, read_ast.order_first_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 31, .end = 32 }, read_ast.order_first_expression.function_name_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 33, .end = 36 }, read_ast.order_first_expression.argument_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 37, .end = 38 }, read_ast.order_items.direction_items[0].?);
                } else if (std.mem.eql(u8, case.sql, "SELECT CURRENT_TIMESTAMP(6) AS planned_at_ns FROM users WHERE id = $1")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.current_timestamp, read_ast.projection_first_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read_ast.projection_first_expression.current_timestamp_precision_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 11, .end = 12 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 12, .end = 13 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT CURRENT_DATE AS planned_day_ns FROM users WHERE id = $1")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.current_date, read_ast.projection_first_expression.kind);
                    try std.testing.expect(read_ast.projection_first_expression.current_timestamp_precision_tokens == null);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT lower(p.valid_at) AS valid_start, upper(p.valid_at) AS valid_end FROM price_intervals AS p WHERE lower(p.valid_at) >= 1 AND upper(p.valid_at) IS NOT NULL ORDER BY upper(p.valid_at) DESC LIMIT 5")) {
                    try std.testing.expectEqual(@as(usize, 2), read_ast.projection_items.count);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.function_call, read_ast.projection_items.expressions[0].kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read_ast.projection_items.expressions[0].function_name_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read_ast.projection_items.expressions[0].argument_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.function_call, read_ast.projection_items.expressions[1].kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read_ast.projection_items.expressions[1].function_name_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read_ast.projection_items.expressions[1].argument_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 15, .end = 18 }, read_ast.source_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.logical_and, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.where_expression.left_expression_kind.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.is_not_null, read_ast.where_expression.right_expression_kind.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.function_call, read_ast.order_first_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 35, .end = 36 }, read_ast.order_first_expression.function_name_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 37, .end = 38 }, read_ast.order_first_expression.argument_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 39, .end = 40 }, read_ast.order_items.direction_items[0].?);
                } else if (std.mem.eql(u8, case.sql, "SELECT CASE WHEN email IS NULL THEN 'missing' WHEN email = 'blocked@example.test' THEN 'blocked' ELSE lower(status) END AS email_bucket FROM usage_records WHERE id = 'u1'")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.case_expression, read_ast.projection_items.expressions[0].kind);
                    try std.testing.expectEqual(@as(usize, 2), read_ast.projection_items.expressions[0].case_branch_count);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 2, .end = 8 }, read_ast.projection_items.expressions[0].case_first_when_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.is_null, read_ast.projection_items.expressions[0].case_first_condition_kind.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 15, .end = 19 }, read_ast.projection_items.expressions[0].case_else_expression_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.function_call, read_ast.projection_items.expressions[0].case_else_expression_kind.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT CASE WHEN email IS NULL THEN NULL ELSE email END AS maybe_email FROM usage_records WHERE id = 'u1'")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.case_expression, read_ast.projection_items.expressions[0].kind);
                    try std.testing.expectEqual(@as(usize, 1), read_ast.projection_items.expressions[0].case_branch_count);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.projection_items.expressions[0].case_first_result_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read_ast.projection_items.expressions[0].case_else_expression_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE status LIKE 'open%'")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.like, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE status ILIKE 'open%'")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.ilike, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE status LIKE 'op!_%' ESCAPE '!'")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.like, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.right_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 10 }, read_ast.where_expression.escape_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read_ast.where_expression.escape_expression.?.tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE lower(status) ILIKE 'op!_%' ESCAPE '!'")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.ilike, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 9 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read_ast.where_expression.right_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 11, .end = 13 }, read_ast.where_expression.escape_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 12, .end = 13 }, read_ast.where_expression.escape_expression.?.tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE lower(status) LIKE ANY(ARRAY['op%', 'ready%'])")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.like, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 9 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read_ast.where_expression.quantifier_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 11, .end = 19 }, read_ast.where_expression.right_tokens.?);
                    const grouped = read_ast.where_expression.right_expression orelse return error.TestUnexpectedResult;
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.grouped, grouped.kind);
                    const array_constructor = grouped.inner_expression orelse return error.TestUnexpectedResult;
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.array_constructor, array_constructor.kind);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE status LIKE SOME(ARRAY['op%', 'ready%'])")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.like, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.quantifier_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 16 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE name ILIKE ALL(ARRAY['ada%', 'grace%'])")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.ilike, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.quantifier_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 16 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE id IN ('u1', 'u2')")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.in_list, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 12 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE score BETWEEN 1 AND 10")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.between, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 10 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE priority BETWEEN SYMMETRIC 20 AND 10")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.between, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.between_modifier_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlBetweenModifier.symmetric, read_ast.where_expression.between_modifier.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 11 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE array_length(tags, 1) BETWEEN SYMMETRIC 3 AND 1")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.between, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 11 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.function_call, read_ast.where_expression.left_expression_kind.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 11, .end = 12 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 12, .end = 13 }, read_ast.where_expression.between_modifier_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlBetweenModifier.symmetric, read_ast.where_expression.between_modifier.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 13, .end = 16 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE status NOT LIKE 'closed%'")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.not_like, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.negation_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.operator_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE status NOT ILIKE 'closed%'")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.not_ilike, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.negation_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.operator_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE status NOT LIKE 'cl!_%' ESCAPE '!'")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.not_like, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.negation_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read_ast.where_expression.right_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 11 }, read_ast.where_expression.escape_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read_ast.where_expression.escape_expression.?.tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE lower(status) NOT ILIKE 'cl!_%' ESCAPE '!'")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.not_ilike, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 9 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read_ast.where_expression.negation_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 11, .end = 12 }, read_ast.where_expression.right_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 12, .end = 14 }, read_ast.where_expression.escape_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 13, .end = 14 }, read_ast.where_expression.escape_expression.?.tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE lower(name) NOT ILIKE ALL(ARRAY['bot%', 'sys%'])")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.not_ilike, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 9 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read_ast.where_expression.negation_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 11, .end = 12 }, read_ast.where_expression.quantifier_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 12, .end = 20 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE id NOT IN ('u1', 'u2')")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.not_in_list, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.negation_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 13 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE score NOT BETWEEN 1 AND 10")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.not_between, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.negation_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 11 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE priority NOT BETWEEN ASYMMETRIC 10 AND 20")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.not_between, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.negation_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read_ast.where_expression.between_modifier_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlBetweenModifier.asymmetric, read_ast.where_expression.between_modifier.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 12 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE priority NOT BETWEEN SYMMETRIC 20 AND 10")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.not_between, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.negation_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read_ast.where_expression.between_modifier_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlBetweenModifier.symmetric, read_ast.where_expression.between_modifier.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 12 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE score = ANY (1, 2)")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.quantified_comparison, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.quantifier_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 13 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE score <> ALL (1, 2)")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.quantified_comparison, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.quantifier_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 13 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE score > SOME (1, 2)")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.quantified_comparison, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.quantifier_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 13 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE status = ANY(ARRAY['active','pending']::text[])")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.quantified_comparison, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.quantifier_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 16 }, read_ast.where_expression.right_tokens.?);
                    const grouped = read_ast.where_expression.right_expression orelse return error.TestUnexpectedResult;
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.grouped, grouped.kind);
                    const array_constructor = grouped.inner_expression orelse return error.TestUnexpectedResult;
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.array_constructor, array_constructor.kind);
                    try std.testing.expectEqual(@as(usize, 2), array_constructor.array_items.count);
                    try std.testing.expectEqual(@as(usize, 2), array_constructor.array_items.expressions.len);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE score = ANY (SELECT score FROM thresholds WHERE active IS TRUE)")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.quantified_comparison, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.quantifier_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 18 }, read_ast.where_expression.right_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.subquery, read_ast.where_expression.right_expression_kind.?);
                    const subquery = read_ast.where_expression.right_expression orelse return error.TestUnexpectedResult;
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.subquery, subquery.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 17 }, subquery.inner_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE score <> ALL (SELECT score FROM archived_thresholds)")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.quantified_comparison, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.quantifier_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 14 }, read_ast.where_expression.right_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.subquery, read_ast.where_expression.right_expression_kind.?);
                    const subquery = read_ast.where_expression.right_expression orelse return error.TestUnexpectedResult;
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.subquery, subquery.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 13 }, subquery.inner_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE tags @> ARRAY['hot','new']")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.contains, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 13 }, read_ast.where_expression.right_tokens.?);
                    const array_constructor = read_ast.where_expression.right_expression orelse return error.TestUnexpectedResult;
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.array_constructor, array_constructor.kind);
                    try std.testing.expectEqual(@as(usize, 2), array_constructor.array_items.count);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE tags && ARRAY['hot','new']")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.overlaps, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 13 }, read_ast.where_expression.right_tokens.?);
                    const array_constructor = read_ast.where_expression.right_expression orelse return error.TestUnexpectedResult;
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.array_constructor, array_constructor.kind);
                    try std.testing.expectEqual(@as(usize, 2), array_constructor.array_items.count);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE metadata ? 'flags'")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.json_key_exists, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE metadata ?| ARRAY['flags','billing']")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.json_key_any, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 13 }, read_ast.where_expression.right_tokens.?);
                    const array_constructor = read_ast.where_expression.right_expression orelse return error.TestUnexpectedResult;
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.array_constructor, array_constructor.kind);
                    try std.testing.expectEqual(@as(usize, 2), array_constructor.array_items.count);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE metadata ?& ARRAY['flags','billing']")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.json_key_all, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 13 }, read_ast.where_expression.right_tokens.?);
                    const array_constructor = read_ast.where_expression.right_expression orelse return error.TestUnexpectedResult;
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.array_constructor, array_constructor.kind);
                    try std.testing.expectEqual(@as(usize, 2), array_constructor.array_items.count);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE status ~ 'op.*'")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.regex_match, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE status ~* 'op.*'")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.regex_imatch, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE status !~ 'closed.*'")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.regex_not_match, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE status !~* 'closed.*'")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.regex_not_imatch, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT first_name || ' ' || last_name FROM usage_records")) {
                    try std.testing.expectEqual(@as(usize, 1), read_ast.projection_items.count);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.string_concat, read_ast.projection_items.expressions[0].kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read_ast.projection_items.expressions[0].left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 2, .end = 3 }, read_ast.projection_items.expressions[0].operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 6 }, read_ast.projection_items.expressions[0].right_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.string_concat, read_ast.projection_items.expressions[0].right_expression_kind.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.string_concat, read_ast.projection_first_expression.kind);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE status || ':' || id = 'open:u1'")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 10 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.string_concat, read_ast.where_expression.left_expression_kind.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 11, .end = 12 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE deleted_at IS NULL")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.is_null, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE deleted_at IS NOT NULL")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.is_not_null, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 9 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE status ISNULL")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.is_null, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expect(read_ast.where_expression.right_tokens == null);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE lower(status) NOTNULL")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.is_not_null, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 9 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.function_call, read_ast.where_expression.left_expression_kind.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expect(read_ast.where_expression.right_tokens == null);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE active IS TRUE")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.is_true, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE active IS NOT FALSE")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.is_not_false, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 9 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE active IS UNKNOWN")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.is_unknown, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE active IS NOT UNKNOWN")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.is_not_unknown, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 9 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE status IS DISTINCT FROM previous_status")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.is_distinct_from, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 9 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE status IS NOT DISTINCT FROM previous_status")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.is_not_distinct_from, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.where_expression.negation_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 10 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE status = 'open' OR deleted_at IS NULL")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.logical_or, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.where_expression.left_expression_kind.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.where_expression.left_expression.?.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read_ast.where_expression.left_expression.?.tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 12 }, read_ast.where_expression.right_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.is_null, read_ast.where_expression.right_expression_kind.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.is_null, read_ast.where_expression.right_expression.?.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 12 }, read_ast.where_expression.right_expression.?.tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE status = 'open' AND deleted_at IS NULL")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.logical_and, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.where_expression.left_expression_kind.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 12 }, read_ast.where_expression.right_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.is_null, read_ast.where_expression.right_expression_kind.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE NOT deleted_at IS NULL")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.logical_not, read_ast.where_expression.kind);
                    try std.testing.expect(read_ast.where_expression.left_tokens == null);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 9 }, read_ast.where_expression.right_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.is_null, read_ast.where_expression.right_expression_kind.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE (status = 'open')")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.grouped, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 9 }, read_ast.where_expression.inner_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.where_expression.inner_expression_kind.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE NOT (deleted_at IS NULL)")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.logical_not, read_ast.where_expression.kind);
                    try std.testing.expect(read_ast.where_expression.left_tokens == null);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 11 }, read_ast.where_expression.right_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.grouped, read_ast.where_expression.right_expression_kind.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE score + bonus > 10")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.additive, read_ast.where_expression.left_expression_kind.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE score * weight > 10")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.multiplicative, read_ast.where_expression.left_expression_kind.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE payload ->> 'status' = 'open'")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.json_text_access, read_ast.where_expression.left_expression_kind.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT metadata #>> '{billing,plan}' AS plan FROM usage_records WHERE metadata #> '{flags}' = $1::jsonb")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 6 }, read_ast.projection_tokens.?);
                    try std.testing.expectEqual(@as(usize, 1), read_ast.projection_items.count);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.json_path_text_access, read_ast.projection_first_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 4 }, read_ast.projection_items.expression_items[0]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 4, .end = 6 }, read_ast.projection_items.alias_items[0].?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 12 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.json_path_access, read_ast.where_expression.left_expression_kind.?);
                    const path_left = read_ast.where_expression.left_expression orelse return error.TestUnexpectedResult;
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.json_path_access, path_left.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 10, .end = 11 }, path_left.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 11, .end = 12 }, path_left.right_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 12, .end = 13 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 13, .end = 14 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE metadata #>> '{billing,plan}' = 'pro'")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.json_path_text_access, read_ast.where_expression.left_expression_kind.?);
                    const path_left = read_ast.where_expression.left_expression orelse return error.TestUnexpectedResult;
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.json_path_text_access, path_left.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, path_left.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, path_left.right_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records WHERE lower(status) = 'open'")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.where_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 9 }, read_ast.where_expression.left_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.function_call, read_ast.where_expression.left_expression_kind.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read_ast.where_expression.operator_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read_ast.where_expression.right_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT concat_ws(',', status), id FROM usage_records ORDER BY status, id")) {
                    try std.testing.expectEqual(@as(usize, 2), read_ast.projection_items.count);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 7 }, read_ast.projection_items.first_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read_ast.projection_items.last_tokens.?);
                    try std.testing.expectEqual(@as(usize, 2), read_ast.projection_items.items.len);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 7 }, read_ast.projection_items.items[0]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read_ast.projection_items.items[1]);
                    try std.testing.expectEqual(@as(usize, 2), read_ast.projection_items.expressions.len);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.function_call, read_ast.projection_items.expressions[0].kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.token_range, read_ast.projection_items.expressions[1].kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.function_call, read_ast.projection_first_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read_ast.projection_first_expression.function_name_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 6 }, read_ast.projection_first_expression.argument_tokens.?);
                    try std.testing.expectEqual(@as(usize, 2), read_ast.projection_first_expression.argument_items.count);
                    try std.testing.expectEqual(@as(usize, 2), read_ast.projection_first_expression.argument_items.items.len);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read_ast.projection_first_expression.argument_items.items[0]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.projection_first_expression.argument_items.items[1]);
                    try std.testing.expectEqual(@as(usize, 2), read_ast.projection_first_expression.argument_items.expressions.len);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.token_range, read_ast.projection_last_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read_ast.projection_last_expression.tokens.?);
                    try std.testing.expectEqual(@as(usize, 2), read_ast.order_items.count);
                    try std.testing.expectEqual(@as(usize, 2), read_ast.order_items.items.len);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 13, .end = 14 }, read_ast.order_items.items[0]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 15, .end = 16 }, read_ast.order_items.items[1]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 13, .end = 14 }, read_ast.order_items.first_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 15, .end = 16 }, read_ast.order_items.last_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.token_range, read_ast.order_first_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 13, .end = 14 }, read_ast.order_first_expression.tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.token_range, read_ast.order_last_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 15, .end = 16 }, read_ast.order_last_expression.tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT customer, COUNT(*) FILTER (WHERE status = 'open') AS open_count FROM usage_records GROUP BY customer")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 16 }, read_ast.projection_tokens.?);
                    try std.testing.expectEqual(@as(usize, 2), read_ast.projection_items.count);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 16 }, read_ast.projection_items.items[1]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 14 }, read_ast.projection_items.expression_items[1]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 14, .end = 16 }, read_ast.projection_items.alias_items[1].?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.function_call, read_ast.projection_items.expressions[1].kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read_ast.projection_items.expressions[1].function_name_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.projection_items.expressions[1].argument_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 14 }, read_ast.projection_items.expressions[1].filter_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 10, .end = 13 }, read_ast.projection_items.expressions[1].filter_predicate_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.projection_items.expressions[1].filter_expression_kind.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.projection_items.expressions[1].filter_expression.?.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 20, .end = 21 }, read_ast.group_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT customer, COUNT(DISTINCT status) AS status_count FROM usage_records GROUP BY customer")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 10 }, read_ast.projection_items.items[1]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.function_call, read_ast.projection_items.expressions[1].kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 7 }, read_ast.projection_items.expressions[1].argument_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.projection_items.expressions[1].argument_distinct_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.projection_items.expressions[1].argument_value_tokens.?);
                    try std.testing.expectEqual(@as(usize, 1), read_ast.projection_items.expressions[1].argument_items.count);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 14, .end = 15 }, read_ast.group_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT customer, ARRAY_AGG(DISTINCT status ORDER BY amount DESC) AS statuses FROM usage_records GROUP BY customer")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 14 }, read_ast.projection_items.items[1]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.function_call, read_ast.projection_items.expressions[1].kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 11 }, read_ast.projection_items.expressions[1].argument_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.projection_items.expressions[1].argument_distinct_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.projection_items.expressions[1].argument_value_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 11 }, read_ast.projection_items.expressions[1].argument_order_tokens.?);
                    try std.testing.expectEqual(@as(usize, 1), read_ast.projection_items.expressions[1].argument_order_items.count);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlOrderDirection.desc, read_ast.projection_items.expressions[1].argument_order_items.directions[0].?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 18, .end = 19 }, read_ast.group_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT customer, percentile_cont(0.5) WITHIN GROUP (ORDER BY amount DESC NULLS LAST) AS median_amount FROM usage_records GROUP BY customer")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 19 }, read_ast.projection_tokens.?);
                    try std.testing.expectEqual(@as(usize, 2), read_ast.projection_items.count);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 19 }, read_ast.projection_items.items[1]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 17 }, read_ast.projection_items.expression_items[1]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.function_call, read_ast.projection_items.expressions[1].kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 17 }, read_ast.projection_items.expressions[1].within_group_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 12, .end = 16 }, read_ast.projection_items.expressions[1].within_group_order_tokens.?);
                    try std.testing.expectEqual(@as(usize, 1), read_ast.projection_items.expressions[1].within_group_order_items.count);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 12, .end = 16 }, read_ast.projection_items.expressions[1].within_group_order_items.items[0]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlOrderDirection.desc, read_ast.projection_items.expressions[1].within_group_order_items.directions[0].?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlNullsOrder.last, read_ast.projection_items.expressions[1].within_group_order_items.nulls_orders[0].?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 23, .end = 24 }, read_ast.group_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id, row_number() OVER (PARTITION BY tenant, account ORDER BY id) AS rn FROM usage_records ORDER BY id, tenant")) {
                    try std.testing.expectEqual(@as(usize, 2), read_ast.projection_items.count);
                    try std.testing.expectEqual(@as(usize, 2), read_ast.projection_items.items.len);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read_ast.projection_items.first_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 19 }, read_ast.projection_items.last_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read_ast.projection_items.expression_items[0]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 17 }, read_ast.projection_items.expression_items[1]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 17, .end = 19 }, read_ast.projection_items.alias_items[1].?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 18, .end = 19 }, read_ast.projection_items.alias_name_items[1].?);
                    try std.testing.expectEqual(@as(usize, 2), read_ast.order_items.count);
                    try std.testing.expectEqual(@as(usize, 2), read_ast.order_items.items.len);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 23, .end = 24 }, read_ast.order_items.first_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 25, .end = 26 }, read_ast.order_items.last_tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 7 }, read_ast.offset_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.token_range, read_ast.offset_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.offset_expression.tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 12 }, read_ast.fetch_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read_ast.fetch_count_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.token_range, read_ast.fetch_count_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read_ast.fetch_count_expression.tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records ORDER BY id LIMIT ALL OFFSET 2 ROWS")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.order_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read_ast.limit_tokens.?);
                    try std.testing.expect(read_ast.limit_all);
                    try std.testing.expect(read_ast.limit_expression.tokens == null);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 10, .end = 12 }, read_ast.offset_tokens.?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.token_range, read_ast.offset_expression.kind);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read_ast.offset_expression.tokens.?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records FETCH FIRST ROWS ONLY")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read_ast.fetch_tokens.?);
                    try std.testing.expect(read_ast.fetch_count_tokens == null);
                    try std.testing.expect(read_ast.fetch_count_expression.tokens == null);
                } else if (std.mem.eql(u8, case.sql, "SELECT DISTINCT ON (organization_id) organization_id, id FROM usage_records ORDER BY organization_id ASC, created_at DESC")) {
                    try std.testing.expect(read_ast.distinct_tokens != null);
                    try std.testing.expect(read_ast.projection_tokens != null);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records ORDER BY created_at DESC NULLS LAST, score ASC NULLS FIRST")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 15 }, read_ast.order_tokens.?);
                    try std.testing.expectEqual(@as(usize, 2), read_ast.order_items.count);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 10 }, read_ast.order_items.items[0]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.order_items.expression_items[0]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.order_items.direction_items[0].?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlOrderDirection.desc, read_ast.order_items.directions[0].?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 10 }, read_ast.order_items.nulls_order_items[0].?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlNullsOrder.last, read_ast.order_items.nulls_orders[0].?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 11, .end = 15 }, read_ast.order_items.items[1]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 11, .end = 12 }, read_ast.order_items.expression_items[1]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 12, .end = 13 }, read_ast.order_items.direction_items[1].?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlOrderDirection.asc, read_ast.order_items.directions[1].?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 13, .end = 15 }, read_ast.order_items.nulls_order_items[1].?);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlNullsOrder.first, read_ast.order_items.nulls_orders[1].?);
                } else if (std.mem.eql(u8, case.sql, "SELECT id FROM usage_records ORDER BY 1 USING > LIMIT 5")) {
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 9 }, read_ast.order_tokens.?);
                    try std.testing.expectEqual(@as(usize, 1), read_ast.order_items.count);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 9 }, read_ast.order_items.items[0]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.order_items.expression_items[0]);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 9 }, read_ast.order_items.direction_items[0].?);
                    try std.testing.expect(read_ast.order_items.directions[0] == null);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read_ast.order_items.order_using_operator_items[0].?);
                    try std.testing.expect(read_ast.order_items.nulls_order_items[0] == null);
                    try std.testing.expect(read_ast.order_items.nulls_orders[0] == null);
                } else if (case.generated == .join) {
                    if (std.mem.indexOf(u8, case.sql, " JOIN tenants ")) |_| {
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 16 }, read_ast.join_tokens.?);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 4, .end = 5 }, read_ast.join_operator_tokens.?);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlJoinKind.inner, read_ast.join_kind.?);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read_ast.join_left_tokens.?);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.join_right_tokens.?);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 10 }, read_ast.join_predicate_tokens.?);
                        try std.testing.expectEqual(@as(usize, 2), read_ast.join_items.len);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 10 }, read_ast.join_items[0].tokens);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 4, .end = 5 }, read_ast.join_items[0].operator_tokens);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlJoinKind.inner, read_ast.join_items[0].kind);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read_ast.join_items[0].left_tokens);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.join_items[0].right_tokens);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlJoinConditionKind.on, read_ast.join_items[0].condition_kind);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 10 }, read_ast.join_items[0].condition_tokens);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 10 }, read_ast.join_items[0].predicate_tokens.?);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.join_items[0].predicate_expression.kind);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 16 }, read_ast.join_items[1].tokens);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read_ast.join_items[1].operator_tokens);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlJoinKind.inner, read_ast.join_items[1].kind);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 10 }, read_ast.join_items[1].left_tokens);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 11, .end = 12 }, read_ast.join_items[1].right_tokens);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlJoinConditionKind.on, read_ast.join_items[1].condition_kind);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 12, .end = 16 }, read_ast.join_items[1].condition_tokens);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 13, .end = 16 }, read_ast.join_items[1].predicate_tokens.?);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.join_items[1].predicate_expression.kind);
                    } else if (std.mem.indexOf(u8, case.sql, "LEFT OUTER")) |_| {
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 12 }, read_ast.join_tokens.?);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 4, .end = 7 }, read_ast.join_operator_tokens.?);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlJoinKind.left, read_ast.join_kind.?);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read_ast.join_left_tokens.?);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.join_right_tokens.?);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 12 }, read_ast.join_predicate_tokens.?);
                        try std.testing.expectEqual(@as(usize, 1), read_ast.join_items.len);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 12 }, read_ast.join_items[0].tokens);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read_ast.join_predicate_expression.left_tokens.?);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read_ast.join_predicate_expression.operator_tokens.?);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 11, .end = 12 }, read_ast.join_predicate_expression.right_tokens.?);
                    } else {
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 10 }, read_ast.join_tokens.?);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 4, .end = 5 }, read_ast.join_operator_tokens.?);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlJoinKind.inner, read_ast.join_kind.?);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read_ast.join_left_tokens.?);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read_ast.join_right_tokens.?);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 10 }, read_ast.join_predicate_tokens.?);
                        try std.testing.expectEqual(@as(usize, 1), read_ast.join_items.len);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 10 }, read_ast.join_items[0].tokens);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read_ast.join_predicate_expression.left_tokens.?);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read_ast.join_predicate_expression.operator_tokens.?);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read_ast.join_predicate_expression.right_tokens.?);
                    }
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.join_predicate_expression.kind);
                } else if (case.generated == .aggregate) {
                    if (std.mem.indexOf(u8, case.sql, "DISTINCT")) |_| {
                        try std.testing.expect(read_ast.distinct_tokens != null);
                    } else {
                        try std.testing.expect(read_ast.group_tokens != null);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.token_range, read_ast.group_first_expression.kind);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.group_first_expression.tokens.?);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.token_range, read_ast.group_last_expression.kind);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read_ast.group_last_expression.tokens.?);
                        try std.testing.expect(read_ast.having_tokens != null);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlExpressionKind.comparison, read_ast.having_expression.kind);
                    }
                } else if (case.generated == .window) {
                    try std.testing.expect(read_ast.projection_tokens != null);
                    try std.testing.expect(read_ast.source_tokens != null);
                    if (std.mem.indexOf(u8, case.sql, " WINDOW ")) |_| {
                        try std.testing.expect(read_ast.window_tokens != null);
                    }
                } else if (case.generated == .cte) {
                    try std.testing.expect(read_ast.cte_tokens != null);
                    try std.testing.expect(read_ast.cte_list_tokens != null);
                    try std.testing.expect(read_ast.cte_name_tokens != null);
                    try std.testing.expect(read_ast.cte_body_tokens != null);
                    try std.testing.expect(read_ast.cte_count > 0);
                    try std.testing.expectEqual(read_ast.cte_count, read_ast.cte_items.len);
                    try std.testing.expectEqual(read_ast.cte_name_tokens.?, read_ast.cte_items[0].name_tokens);
                    try std.testing.expectEqual(read_ast.cte_body_tokens.?, read_ast.cte_items[0].body_tokens.?);
                    if (std.mem.indexOf(u8, case.sql, " second_rows ")) |_| {
                        try std.testing.expectEqual(@as(usize, 2), read_ast.cte_count);
                        try std.testing.expect(read_ast.cte_last_name_tokens != null);
                        try std.testing.expect(read_ast.cte_last_body_tokens != null);
                        try std.testing.expectEqual(read_ast.cte_last_name_tokens.?, read_ast.cte_items[1].name_tokens);
                        try std.testing.expectEqual(read_ast.cte_last_body_tokens.?, read_ast.cte_items[1].body_tokens.?);
                    }
                    if (std.mem.indexOf(u8, case.sql, "WITH RECURSIVE")) |_| {
                        try std.testing.expect(read_ast.cte_recursive);
                    }
                    if (std.mem.indexOf(u8, case.sql, "AS MATERIALIZED")) |_| {
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read_ast.cte_items[0].materialization_tokens.?);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlCteMaterialization.materialized, read_ast.cte_items[0].materialization.?);
                    }
                    if (std.mem.indexOf(u8, case.sql, "AS NOT MATERIALIZED")) |_| {
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 2, .end = 5 }, read_ast.cte_items[0].column_tokens.?);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read_ast.cte_items[0].column_name_tokens.?);
                        try std.testing.expectEqual(@as(usize, 1), read_ast.cte_items[0].column_names.count);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read_ast.cte_items[0].column_names.items[0]);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 8 }, read_ast.cte_items[0].materialization_tokens.?);
                        try std.testing.expectEqual(generated_parser.GeneratedSqlCteMaterialization.not_materialized, read_ast.cte_items[0].materialization.?);
                    }
                    try std.testing.expect(read_ast.projection_tokens != null);
                } else if (case.generated == .set_operation) {
                    try std.testing.expect(read_ast.set_operation_tokens != null);
                }
            },
            else => return error.TestUnexpectedResult,
        }
        switch (parsed.statement) {
            .read => |statement| try std.testing.expectEqual(case.read, statement.kind),
            else => return error.TestUnexpectedResult,
        }
    }

    var generated_distinct_on = try ParsedSql.initAlloc(alloc, "SELECT DISTINCT ON (organization_id) organization_id, id FROM usage_records ORDER BY organization_id ASC, created_at DESC");
    defer generated_distinct_on.deinit(alloc);
    try std.testing.expect(generated_distinct_on.generated_statement != null);
    try std.testing.expectEqual(classifier.SqlReadStatementKind.query, generated_distinct_on.readStatementKind().?);
}

test "sql adapter parsed sql read statement kind can come from generated AST" {
    const alloc = std.testing.allocator;

    var generated_query = try ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records WHERE status = 'open'");
    defer generated_query.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.read, generated_query.generatedStatementKind().?);

    generated_query.tokenized_sql.read_statement_kind = null;
    generated_query.statement = parseStatement(generated_query.raw_statement, generated_query.generated_statement, &generated_query.tokenized_sql);
    try std.testing.expectEqual(classifier.SqlReadStatementKind.query, generated_query.readStatementKind().?);

    var generated_cte_aggregate = try ParsedSql.initAlloc(alloc, "WITH source_rows AS (SELECT status FROM usage_records) SELECT count(*) FROM source_rows");
    defer generated_cte_aggregate.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.read, generated_cte_aggregate.generatedStatementKind().?);

    generated_cte_aggregate.tokenized_sql.read_statement_kind = null;
    generated_cte_aggregate.statement = parseStatement(generated_cte_aggregate.raw_statement, generated_cte_aggregate.generated_statement, &generated_cte_aggregate.tokenized_sql);
    try std.testing.expectEqual(classifier.SqlReadStatementKind.aggregate, generated_cte_aggregate.readStatementKind().?);
}

test "sql adapter parsed sql read statement kind fails closed on classifier disagreement" {
    const alloc = std.testing.allocator;

    var generated_query = try ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records WHERE status = 'open'");
    defer generated_query.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.read, generated_query.generatedStatementKind().?);

    generated_query.tokenized_sql.read_statement_kind = .aggregate;
    generated_query.statement = parseStatement(generated_query.raw_statement, generated_query.generated_statement, &generated_query.tokenized_sql);
    try std.testing.expect(generated_query.readStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_query.statement));
}

test "sql adapter parsed sql retains generated graph nodes as DDL" {
    const alloc = std.testing.allocator;

    const cases = [_]struct {
        sql: []const u8,
        generated: generated_parser.GeneratedSqlGraphKind,
    }{
        .{ .sql = "CREATE GRAPH INDEX docs_edge_graph ON doc_edges", .generated = .create_index },
        .{ .sql = "CREATE GRAPH INDEX docs_edge_graph_syntax ON doc_edges EDGE (source_doc -> target_doc) TYPE edge_type WEIGHT confidence WITH (edge_policy = 'all')", .generated = .create_index },
        .{ .sql = "CREATE GRAPH METRIC docs_pagerank ON doc_edges WITH (metric = 'pagerank')", .generated = .create_metric },
        .{ .sql = "ALTER GRAPH INDEX docs_edge_graph ADD METRIC pagerank_v1 USING pagerank WITH (damping = 0.85, max_iterations = 40)", .generated = .alter_metric },
    };

    for (cases) |case| {
        var parsed = try ParsedSql.initAlloc(alloc, case.sql);
        defer parsed.deinit(alloc);
        try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.graph, parsed.generatedStatementKind().?);
        switch (parsed.generated_statement.?.statement) {
            .graph => |kind| try std.testing.expectEqual(case.generated, kind),
            else => return error.TestUnexpectedResult,
        }
        switch (parsed.generated_statement.?.ast.?) {
            .graph => |graph_ast| try std.testing.expectEqual(case.generated, graph_ast.kind),
            else => return error.TestUnexpectedResult,
        }
        switch (parsed.statement) {
            .ddl => {},
            else => return error.TestUnexpectedResult,
        }
    }
}
