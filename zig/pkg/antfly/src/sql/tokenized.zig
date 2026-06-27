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
    const generated_fallback_allowed = allowsGeneratedGrammarFallback(tokens, raw_statement);
    const result = if (generated_fallback_allowed)
        try generated_parser.parseGeneratedGateTokensAlloc(alloc, tokens)
    else
        generated_parser.parseGeneratedGateTokensStrictAlloc(alloc, tokens) catch |err| switch (err) {
            error.UnsupportedSqlShape, error.UnexpectedToken => {
                if (try generated_parser.parseUnsupportedInsertOverridingTokensAlloc(alloc, tokens)) |parsed| {
                    return .{ .raw = raw_statement, .statement = parsed.statement, .ast = parsed.ast };
                }
                if (generatedStrictParseFailureShouldPropagate(tokens, raw_statement)) return err;
                return null;
            },
            else => return err,
        };
    if (result) |parsed| {
        return .{ .raw = raw_statement, .statement = parsed.statement, .ast = parsed.ast };
    }
    if (!generated_fallback_allowed and generatedStrictParseFailureShouldPropagate(tokens, raw_statement)) return error.UnexpectedToken;
    return null;
}

fn generatedStrictParseFailureShouldPropagate(tokens: []const Token, raw_statement: RawSqlStatement) bool {
    if (raw_statement.token_start >= raw_statement.token_end or raw_statement.token_end > tokens.len) return true;
    if (tokens[raw_statement.token_end - 1].kind == .eq or tokens[raw_statement.token_end - 1].kind == .comma) return true;
    if (tokenMatchesKeyword(tokens[raw_statement.token_end - 1], .to) or tokenMatchesKeyword(tokens[raw_statement.token_end - 1], .as)) return true;
    if (isGeneratedDmlStatementHead(tokens, raw_statement)) return true;
    if (isGeneratedTableOrIndexDdlHead(tokens, raw_statement)) return true;
    if (isGeneratedUnsupportedHead(tokens, raw_statement)) return true;
    return isIncompleteGeneratedDdlBoundary(tokens, raw_statement) or
        isIncompleteGeneratedDmlBoundary(tokens, raw_statement) or
        isIncompleteGeneratedReadBoundary(tokens, raw_statement) or
        isIncompleteGeneratedUnsupportedBoundary(tokens, raw_statement);
}

fn allowsGeneratedGrammarFallback(tokens: []const Token, raw_statement: RawSqlStatement) bool {
    if (raw_statement.token_start >= raw_statement.token_end or raw_statement.token_end > tokens.len) return false;
    if (tokens[raw_statement.token_end - 1].kind == .eq or tokens[raw_statement.token_end - 1].kind == .comma) return false;
    if (tokenMatchesKeyword(tokens[raw_statement.token_end - 1], .to) or tokenMatchesKeyword(tokens[raw_statement.token_end - 1], .as)) return false;
    if (isGeneratedGraphDdlHead(tokens, raw_statement)) return false;
    if (isGeneratedCatalogDdlHead(tokens, raw_statement)) return false;
    if (isGeneratedRelationPopulationHead(tokens, raw_statement)) return false;
    if (isGeneratedTableOrIndexDdlHead(tokens, raw_statement)) return false;
    if (isGeneratedUnsupportedHead(tokens, raw_statement)) return false;
    if (isGeneratedRoleDdlHead(tokens, raw_statement)) return false;
    if (isGeneratedTypeSystemDdlHead(tokens, raw_statement)) return false;
    if (isGeneratedExtendedCatalogDdlHead(tokens, raw_statement)) return false;
    if (isGeneratedCursorStatementHead(tokens, raw_statement)) return false;
    if (isGeneratedSessionStatementHead(tokens, raw_statement)) return false;
    if (isGeneratedPreparedStatementHead(tokens, raw_statement)) return false;
    if (isGeneratedTransactionControlStatement(tokens, raw_statement)) return false;
    if (isIncompleteGeneratedDdlBoundary(tokens, raw_statement)) return false;
    if (isIncompleteGeneratedDmlBoundary(tokens, raw_statement)) return false;
    if (isIncompleteGeneratedReadBoundary(tokens, raw_statement)) return false;
    if (isIncompleteGeneratedUnsupportedBoundary(tokens, raw_statement)) return false;

    const first = tokens[raw_statement.token_start];
    if (tokenMatchesKeyword(first, .set)) return raw_statement.token_end > raw_statement.token_start + 2;
    if (tokenMatchesKeyword(first, .reset) or tokenMatchesKeyword(first, .show) or tokenMatchesKeyword(first, .discard)) return raw_statement.token_end > raw_statement.token_start + 1;
    if (tokenMatchesKeyword(first, .prepare)) return raw_statement.token_end > raw_statement.token_start + 2;
    if (tokenMatchesKeyword(first, .execute) or tokenMatchesKeyword(first, .deallocate)) return raw_statement.token_end > raw_statement.token_start + 1;
    if (tokenMatchesKeyword(first, .commit) or tokenMatchesKeyword(first, .end) or tokenMatchesKeyword(first, .rollback)) return raw_statement.token_end > raw_statement.token_start + 1;
    if (tokenMatchesText(first, "start") or tokenMatchesText(first, "lock")) return raw_statement.token_end > raw_statement.token_start + 1;
    if (tokenMatchesKeyword(first, .begin)) return true;
    if (tokenMatchesKeyword(first, .savepoint)) return raw_statement.token_end > raw_statement.token_start + 1;
    if (tokenMatchesText(first, "release")) return raw_statement.token_end > raw_statement.token_start + 1;
    if (tokenMatchesText(first, "declare") or tokenMatchesText(first, "close") or tokenMatchesText(first, "fetch") or tokenMatchesText(first, "move")) {
        return raw_statement.token_end > raw_statement.token_start + 1;
    }
    return switch (raw_statement.family orelse return false) {
        .insert, .update, .delete, .truncate, .merge => false,
        .ddl => true,
        .select, .with => false,
    };
}

fn isGeneratedTransactionControlStatement(tokens: []const Token, raw_statement: RawSqlStatement) bool {
    const start = raw_statement.token_start;
    const end = raw_statement.token_end;
    if (start >= end or end > tokens.len) return false;
    const first = tokens[start];
    if (isPreparedTransactionStatement(tokens, raw_statement)) return true;
    if (tokenMatchesKeyword(first, .set)) {
        if (start + 1 < end and tokenMatchesText(tokens[start + 1], "transaction")) return true;
        return start + 4 < end and
            tokenMatchesText(tokens[start + 1], "session") and
            tokenMatchesText(tokens[start + 2], "characteristics") and
            tokenMatchesKeyword(tokens[start + 3], .as) and
            tokenMatchesText(tokens[start + 4], "transaction");
    }
    if (tokenMatchesText(first, "start")) return true;
    if (tokenMatchesKeyword(first, .begin)) return true;
    if (tokenMatchesKeyword(first, .savepoint)) return true;
    if (tokenMatchesText(first, "release")) {
        if (end <= start + 1) return true;
        if (end == start + 2) return tokens[start + 1].kind == .identifier;
        return end == start + 3 and
            tokenMatchesKeyword(tokens[start + 1], .savepoint) and
            tokens[start + 2].kind == .identifier;
    }
    if (tokenMatchesKeyword(first, .rollback) and start + 1 < end and tokenMatchesKeyword(tokens[start + 1], .to)) {
        if (end <= start + 2) return true;
        if (end == start + 3) return tokens[start + 2].kind == .identifier;
        return end == start + 4 and
            tokenMatchesKeyword(tokens[start + 2], .savepoint) and
            tokens[start + 3].kind == .identifier;
    }
    return tokenMatchesKeyword(first, .commit) or tokenMatchesKeyword(first, .end) or tokenMatchesKeyword(first, .rollback);
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
    const first = tokens[start];
    const second = tokens[start + 1];
    if (tokenMatchesKeyword(first, .create) or tokenMatchesKeyword(first, .drop)) {
        return tokenMatchesKeyword(second, .database) or
            tokenMatchesKeyword(second, .schema) or
            tokenMatchesKeyword(second, .extension);
    }
    if (tokenMatchesKeyword(first, .alter)) {
        return tokenMatchesKeyword(second, .database) or
            tokenMatchesKeyword(second, .extension);
    }
    return false;
}

fn isGeneratedRelationPopulationHead(tokens: []const Token, raw_statement: RawSqlStatement) bool {
    const start = raw_statement.token_start;
    const end = raw_statement.token_end;
    if (start >= end or end > tokens.len) return false;
    if (tokenMatchesKeyword(tokens[start], .select)) {
        return findTopLevelKeyword(tokens, start + 1, end, .into) != null;
    }
    if (!tokenMatchesKeyword(tokens[start], .create)) return false;
    var index = start + 1;
    consumeRelationLifetime(tokens, &index, end);
    if (index >= end or !tokenMatchesKeyword(tokens[index], .table)) return false;
    return findTopLevelKeyword(tokens, index + 1, end, .as) != null;
}

fn isGeneratedTableOrIndexDdlHead(tokens: []const Token, raw_statement: RawSqlStatement) bool {
    const start = raw_statement.token_start;
    const end = raw_statement.token_end;
    if (start + 1 >= end or end > tokens.len) return false;
    const first = tokens[start];
    const second = tokens[start + 1];
    if (tokenMatchesKeyword(first, .create)) {
        var object_index = start + 1;
        consumeRelationLifetime(tokens, &object_index, end);
        if (object_index >= end) return false;
        const object = tokens[object_index];
        return tokenMatchesKeyword(object, .table) or
            tokenMatchesKeyword(object, .index) or
            (tokenMatchesKeyword(object, .unique) and object_index + 1 < end and tokenMatchesKeyword(tokens[object_index + 1], .index));
    }
    if (tokenMatchesKeyword(first, .alter)) {
        return tokenMatchesKeyword(second, .table);
    }
    if (tokenMatchesKeyword(first, .drop)) {
        return tokenMatchesKeyword(second, .table) or tokenMatchesKeyword(second, .index);
    }
    return false;
}

fn isGeneratedUnsupportedHead(tokens: []const Token, raw_statement: RawSqlStatement) bool {
    const start = raw_statement.token_start;
    const end = raw_statement.token_end;
    if (start >= end or end > tokens.len) return false;
    const first = tokens[start];
    if (tokenMatchesKeyword(first, .analyze) or
        tokenMatchesKeyword(first, .call) or
        tokenMatchesKeyword(first, .checkpoint) or
        tokenMatchesKeyword(first, .cluster) or
        tokenMatchesKeyword(first, .comment) or
        tokenMatchesKeyword(first, .copy) or
        tokenMatchesText(first, "do") or
        tokenMatchesKeyword(first, .explain) or
        tokenMatchesKeyword(first, .grant) or
        tokenMatchesKeyword(first, .listen) or
        tokenMatchesText(first, "load") or
        tokenMatchesText(first, "lock") or
        tokenMatchesKeyword(first, .match) or
        tokenMatchesKeyword(first, .notify) or
        tokenMatchesKeyword(first, .reindex) or
        tokenMatchesKeyword(first, .revoke) or
        tokenMatchesKeyword(first, .security) or
        tokenMatchesKeyword(first, .unlisten) or
        tokenMatchesKeyword(first, .vacuum))
    {
        return true;
    }
    if (tokenMatchesKeyword(first, .import)) {
        return start + 2 < end and
            tokenMatchesKeyword(tokens[start + 1], .foreign) and
            tokenMatchesKeyword(tokens[start + 2], .schema);
    }
    if (tokenMatchesKeyword(first, .reassign)) {
        return start + 1 < end and tokenMatchesKeyword(tokens[start + 1], .owned);
    }
    if (tokenMatchesKeyword(first, .alter)) return isGeneratedUnsupportedAlterHead(tokens, start, end);
    if (tokenMatchesKeyword(first, .create)) return isGeneratedUnsupportedCreateHead(tokens, start, end);
    if (tokenMatchesKeyword(first, .drop)) return isGeneratedUnsupportedDropHead(tokens, start, end);
    return false;
}

fn isGeneratedRoleDdlHead(tokens: []const Token, raw_statement: RawSqlStatement) bool {
    const start = raw_statement.token_start;
    const end = raw_statement.token_end;
    if (start + 1 >= end or end > tokens.len) return false;
    const first = tokens[start];
    if (!tokenMatchesKeyword(first, .create) and
        !tokenMatchesKeyword(first, .alter) and
        !tokenMatchesKeyword(first, .drop))
    {
        return false;
    }
    const second = tokens[start + 1];
    return tokenMatchesKeyword(second, .role) or
        tokenMatchesText(second, "user") or
        tokenMatchesKeyword(second, .group);
}

fn isGeneratedTypeSystemDdlHead(tokens: []const Token, raw_statement: RawSqlStatement) bool {
    const start = raw_statement.token_start;
    const end = raw_statement.token_end;
    if (start + 1 >= end or end > tokens.len) return false;
    const first = tokens[start];
    const second = tokens[start + 1];
    if (tokenMatchesKeyword(first, .create)) {
        return tokenMatchesKeyword(second, .collation) or
            tokenMatchesKeyword(second, .operator) or
            tokenMatchesKeyword(second, .aggregate) or
            tokenMatchesKeyword(second, .cast);
    }
    if (tokenMatchesKeyword(first, .alter)) {
        return tokenMatchesKeyword(second, .collation);
    }
    if (tokenMatchesKeyword(first, .drop)) {
        return tokenMatchesKeyword(second, .collation) or
            tokenMatchesKeyword(second, .operator) or
            tokenMatchesKeyword(second, .aggregate) or
            tokenMatchesKeyword(second, .cast);
    }
    return false;
}

fn isGeneratedExtendedCatalogDdlHead(tokens: []const Token, raw_statement: RawSqlStatement) bool {
    const start = raw_statement.token_start;
    const end = raw_statement.token_end;
    if (start + 1 >= end or end > tokens.len) return false;
    const first = tokens[start];
    if (tokenMatchesKeyword(first, .create)) {
        if (tokenMatchesKeyword(tokens[start + 1], .@"or")) {
            return start + 3 < end and
                tokenMatchesKeyword(tokens[start + 2], .replace) and
                (tokenMatchesKeyword(tokens[start + 3], .view) or tokenMatchesKeyword(tokens[start + 3], .function));
        }
        return isGeneratedExtendedCatalogObject(tokens, start + 1, end, .create);
    }
    if (tokenMatchesKeyword(first, .alter)) {
        return isGeneratedExtendedCatalogObject(tokens, start + 1, end, .alter);
    }
    if (tokenMatchesKeyword(first, .drop)) {
        return isGeneratedExtendedCatalogObject(tokens, start + 1, end, .drop);
    }
    if (tokenMatchesKeyword(first, .refresh)) {
        return start + 2 < end and
            tokenMatchesKeyword(tokens[start + 1], .materialized) and
            tokenMatchesKeyword(tokens[start + 2], .view);
    }
    return false;
}

const GeneratedExtendedCatalogOperation = enum { create, alter, drop };

fn isGeneratedExtendedCatalogObject(tokens: []const Token, object_index: usize, end: usize, operation: GeneratedExtendedCatalogOperation) bool {
    if (object_index >= end or end > tokens.len) return false;
    const object = tokens[object_index];
    if (tokenMatchesKeyword(object, .materialized)) {
        return object_index + 1 < end and tokenMatchesKeyword(tokens[object_index + 1], .view);
    }
    if (tokenMatchesKeyword(object, .view) or
        tokenMatchesKeyword(object, .domain) or
        tokenMatchesKeyword(object, .sequence) or
        tokenMatchesKeyword(object, .type) or
        tokenMatchesKeyword(object, .tablespace) or
        tokenMatchesKeyword(object, .publication) or
        tokenMatchesKeyword(object, .subscription) or
        tokenMatchesKeyword(object, .policy))
    {
        return true;
    }
    return switch (operation) {
        .create, .drop => tokenMatchesKeyword(object, .function) or tokenMatchesKeyword(object, .procedure),
        .alter => false,
    };
}

fn isGeneratedCursorStatementHead(tokens: []const Token, raw_statement: RawSqlStatement) bool {
    const start = raw_statement.token_start;
    const end = raw_statement.token_end;
    if (start >= end or end > tokens.len) return false;
    const first = tokens[start];
    return tokenMatchesText(first, "declare") or
        tokenMatchesKeyword(first, .fetch) or
        tokenMatchesText(first, "move") or
        tokenMatchesKeyword(first, .close);
}

fn isGeneratedSessionStatementHead(tokens: []const Token, raw_statement: RawSqlStatement) bool {
    const start = raw_statement.token_start;
    const end = raw_statement.token_end;
    if (start >= end or end > tokens.len) return false;
    const first = tokens[start];
    return tokenMatchesKeyword(first, .set) or
        tokenMatchesKeyword(first, .reset) or
        tokenMatchesKeyword(first, .show) or
        tokenMatchesKeyword(first, .discard);
}

fn isGeneratedPreparedStatementHead(tokens: []const Token, raw_statement: RawSqlStatement) bool {
    const start = raw_statement.token_start;
    const end = raw_statement.token_end;
    if (start >= end or end > tokens.len) return false;
    if (isPreparedTransactionStatement(tokens, raw_statement)) return true;
    const first = tokens[start];
    return tokenMatchesKeyword(first, .prepare) or
        tokenMatchesKeyword(first, .execute) or
        tokenMatchesKeyword(first, .deallocate);
}

fn isPreparedTransactionStatement(tokens: []const Token, raw_statement: RawSqlStatement) bool {
    const start = raw_statement.token_start;
    const end = raw_statement.token_end;
    if (start + 1 >= end or end > tokens.len) return false;
    const first = tokens[start];
    const second = tokens[start + 1];
    return (tokenMatchesKeyword(first, .prepare) and tokenMatchesText(second, "transaction")) or
        (tokenMatchesKeyword(first, .commit) and tokenMatchesText(second, "prepared")) or
        (tokenMatchesKeyword(first, .rollback) and tokenMatchesText(second, "prepared"));
}

fn isGeneratedUnsupportedAlterHead(tokens: []const Token, start: usize, end: usize) bool {
    if (start + 1 >= end) return false;
    const second = tokens[start + 1];
    if (tokenMatchesKeyword(second, .aggregate) or
        tokenMatchesText(second, "conversion") or
        tokenMatchesKeyword(second, .function) or
        tokenMatchesKeyword(second, .index) or
        tokenMatchesText(second, "language") or
        tokenMatchesKeyword(second, .operator) or
        tokenMatchesKeyword(second, .procedure) or
        tokenMatchesText(second, "routine") or
        tokenMatchesKeyword(second, .rule) or
        tokenMatchesKeyword(second, .server) or
        tokenMatchesKeyword(second, .system) or
        tokenMatchesText(second, "statistics") or
        tokenMatchesKeyword(second, .trigger) or
        tokenMatchesText(second, "transform"))
    {
        return true;
    }
    if (tokenMatchesKeyword(second, .default)) {
        return start + 2 < end and tokenMatchesKeyword(tokens[start + 2], .privileges);
    }
    if (tokenMatchesText(second, "event")) {
        return start + 2 < end and tokenMatchesKeyword(tokens[start + 2], .trigger);
    }
    if (tokenMatchesKeyword(second, .foreign)) {
        return start + 2 < end and
            (tokenMatchesKeyword(tokens[start + 2], .table) or
                (start + 3 < end and tokenMatchesKeyword(tokens[start + 2], .data) and tokenMatchesText(tokens[start + 3], "wrapper")));
    }
    if (tokenMatchesText(second, "large")) {
        return start + 2 < end and tokenMatchesText(tokens[start + 2], "object");
    }
    if (tokenMatchesKeyword(second, .materialized)) {
        return start + 2 < end and tokenMatchesKeyword(tokens[start + 2], .view);
    }
    if (tokenMatchesKeyword(second, .text)) {
        if (start + 2 >= end or !tokenMatchesText(tokens[start + 2], "search")) return false;
        return start + 3 >= end or
            (tokenMatchesText(tokens[start + 3], "configuration") or
                tokenMatchesText(tokens[start + 3], "dictionary") or
                tokenMatchesText(tokens[start + 3], "parser") or
                tokenMatchesText(tokens[start + 3], "template"));
    }
    if (tokenMatchesText(second, "user")) {
        return start + 2 < end and tokenMatchesText(tokens[start + 2], "mapping");
    }
    return false;
}

fn isGeneratedUnsupportedCreateHead(tokens: []const Token, start: usize, end: usize) bool {
    if (start + 1 >= end) return false;
    const second = tokens[start + 1];
    if (tokenMatchesText(second, "conversion") or
        tokenMatchesText(second, "language") or
        tokenMatchesKeyword(second, .rule) or
        tokenMatchesKeyword(second, .server) or
        tokenMatchesText(second, "statistics") or
        tokenMatchesText(second, "transform") or
        tokenMatchesKeyword(second, .trigger))
    {
        return true;
    }
    if (tokenMatchesKeyword(second, .access)) {
        return start + 2 < end and tokenMatchesKeyword(tokens[start + 2], .method);
    }
    if (tokenMatchesText(second, "event")) {
        return start + 2 < end and tokenMatchesKeyword(tokens[start + 2], .trigger);
    }
    if (tokenMatchesKeyword(second, .foreign)) {
        return start + 2 < end and
            (tokenMatchesKeyword(tokens[start + 2], .table) or
                (start + 3 < end and tokenMatchesKeyword(tokens[start + 2], .data) and tokenMatchesText(tokens[start + 3], "wrapper")));
    }
    if (tokenMatchesKeyword(second, .operator)) {
        return start + 2 < end and
            (tokenMatchesText(tokens[start + 2], "class") or tokenMatchesText(tokens[start + 2], "family"));
    }
    if (tokenMatchesKeyword(second, .text)) {
        if (start + 2 >= end or !tokenMatchesText(tokens[start + 2], "search")) return false;
        return start + 3 >= end or
            (tokenMatchesText(tokens[start + 3], "configuration") or
                tokenMatchesText(tokens[start + 3], "dictionary") or
                tokenMatchesText(tokens[start + 3], "parser") or
                tokenMatchesText(tokens[start + 3], "template"));
    }
    if (tokenMatchesText(second, "user")) {
        return start + 2 < end and tokenMatchesText(tokens[start + 2], "mapping");
    }
    return false;
}

fn isGeneratedUnsupportedDropHead(tokens: []const Token, start: usize, end: usize) bool {
    if (start + 1 >= end) return false;
    const second = tokens[start + 1];
    if (tokenMatchesText(second, "conversion") or
        tokenMatchesText(second, "language") or
        tokenMatchesKeyword(second, .owned) or
        tokenMatchesText(second, "routine") or
        tokenMatchesKeyword(second, .rule) or
        tokenMatchesKeyword(second, .server) or
        tokenMatchesText(second, "statistics") or
        tokenMatchesText(second, "transform") or
        tokenMatchesKeyword(second, .trigger))
    {
        return true;
    }
    if (tokenMatchesKeyword(second, .access)) {
        return start + 2 < end and tokenMatchesKeyword(tokens[start + 2], .method);
    }
    if (tokenMatchesText(second, "event")) {
        return start + 2 < end and tokenMatchesKeyword(tokens[start + 2], .trigger);
    }
    if (tokenMatchesKeyword(second, .foreign)) {
        return start + 2 < end and
            (tokenMatchesKeyword(tokens[start + 2], .table) or
                (start + 3 < end and tokenMatchesKeyword(tokens[start + 2], .data) and tokenMatchesText(tokens[start + 3], "wrapper")));
    }
    if (tokenMatchesKeyword(second, .operator)) {
        return start + 2 < end and
            (tokenMatchesText(tokens[start + 2], "class") or tokenMatchesText(tokens[start + 2], "family"));
    }
    if (tokenMatchesKeyword(second, .text)) {
        if (start + 2 >= end or !tokenMatchesText(tokens[start + 2], "search")) return false;
        return start + 3 >= end or
            (tokenMatchesText(tokens[start + 3], "configuration") or
                tokenMatchesText(tokens[start + 3], "dictionary") or
                tokenMatchesText(tokens[start + 3], "parser") or
                tokenMatchesText(tokens[start + 3], "template"));
    }
    if (tokenMatchesText(second, "user")) {
        return start + 2 < end and tokenMatchesText(tokens[start + 2], "mapping");
    }
    return false;
}

fn consumeRelationLifetime(tokens: []const Token, index: *usize, end: usize) void {
    if (index.* >= end) return;
    if (tokenMatchesRelationLifetime(tokens[index.*])) {
        index.* += 1;
    }
}

fn tokenMatchesRelationLifetime(token: Token) bool {
    return tokenMatchesKeyword(token, .temp) or
        tokenMatchesKeyword(token, .temporary) or
        tokenMatchesKeyword(token, .unlogged);
}

fn findTopLevelKeyword(tokens: []const Token, start: usize, end: usize, keyword: token_mod.TokenKeyword) ?usize {
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
        if (depth == 0 and tokenMatchesKeyword(tokens[index], keyword)) return index;
    }
    return null;
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
        if (tokenMatchesKeyword(tokens[start + 1], .@"or")) {
            if (end <= start + 3) return true;
            if (tokenMatchesKeyword(tokens[start + 2], .replace) and tokenMatchesKeyword(tokens[start + 3], .view)) {
                if (end <= start + 5) return true;
                return isGeneratedDdlTrailingBoundary(last);
            }
        }
        var table_index = start + 1;
        if (table_index < end and tokenMatchesRelationLifetime(tokens[table_index])) {
            table_index += 1;
            if (table_index >= end) return true;
            if (tokenMatchesKeyword(tokens[table_index], .table)) {
                if (end <= table_index + 2) return true;
                return isGeneratedDdlTrailingBoundary(last);
            }
        }
        if (tokenMatchesKeyword(tokens[start + 1], .unique)) {
            if (end <= start + 2) return true;
            if (tokenMatchesKeyword(tokens[start + 2], .index) and end <= start + 4) return true;
            return isGeneratedDdlTrailingBoundary(last);
        }
        if (tokenMatchesKeyword(tokens[start + 1], .table)) {
            if (end <= start + 3) return true;
            return isGeneratedDdlTrailingBoundary(last);
        }
        if (tokenMatchesKeyword(tokens[start + 1], .view)) {
            if (end <= start + 3) return true;
            return isGeneratedDdlTrailingBoundary(last);
        }
        if (tokenMatchesKeyword(tokens[start + 1], .materialized)) {
            if (end <= start + 3) return true;
            if (tokenMatchesKeyword(tokens[start + 2], .view)) {
                if (end <= start + 4) return true;
                return isGeneratedDdlTrailingBoundary(last);
            }
        }
        if (tokenMatchesKeyword(tokens[start + 1], .domain)) {
            if (end <= start + 3) return true;
            return isGeneratedDdlTrailingBoundary(last);
        }
        if (tokenMatchesKeyword(tokens[start + 1], .sequence)) {
            if (end <= start + 3) return true;
            return isGeneratedDdlTrailingBoundary(last);
        }
        if (tokenMatchesKeyword(tokens[start + 1], .type)) {
            if (end <= start + 3) return true;
            return isGeneratedDdlTrailingBoundary(last);
        }
        if (tokenMatchesKeyword(tokens[start + 1], .tablespace)) {
            if (end <= start + 3) return true;
            return isGeneratedDdlTrailingBoundary(last);
        }
        if (tokenMatchesKeyword(tokens[start + 1], .publication) or
            tokenMatchesKeyword(tokens[start + 1], .subscription))
        {
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
        if (tokenMatchesText(tokens[start + 1], "transform")) {
            if (end <= start + 3) return true;
            return isGeneratedDdlTrailingBoundary(last);
        }
    }
    if (tokenMatchesKeyword(first, .alter) and tokenMatchesKeyword(tokens[start + 1], .table)) {
        if (end <= start + 3) return true;
        return isGeneratedDdlTrailingBoundary(last);
    }
    if (tokenMatchesKeyword(first, .alter) and tokenMatchesKeyword(tokens[start + 1], .schema)) {
        if (end <= start + 3) return true;
        return isGeneratedDdlTrailingBoundary(last);
    }
    if (tokenMatchesKeyword(first, .alter) and tokenMatchesKeyword(tokens[start + 1], .tablespace)) {
        if (end <= start + 3) return true;
        return isGeneratedDdlTrailingBoundary(last);
    }
    if (tokenMatchesKeyword(first, .alter) and tokenMatchesKeyword(tokens[start + 1], .view)) {
        if (end <= start + 3) return true;
        return isGeneratedDdlTrailingBoundary(last);
    }
    if (tokenMatchesKeyword(first, .alter) and tokenMatchesKeyword(tokens[start + 1], .domain)) {
        if (end <= start + 3) return true;
        return isGeneratedDdlTrailingBoundary(last);
    }
    if (tokenMatchesKeyword(first, .alter) and tokenMatchesKeyword(tokens[start + 1], .sequence)) {
        if (end <= start + 3) return true;
        return isGeneratedDdlTrailingBoundary(last);
    }
    if (tokenMatchesKeyword(first, .alter) and tokenMatchesKeyword(tokens[start + 1], .type)) {
        if (end <= start + 3) return true;
        return isGeneratedDdlTrailingBoundary(last);
    }
    if (tokenMatchesKeyword(first, .alter) and
        (tokenMatchesKeyword(tokens[start + 1], .publication) or tokenMatchesKeyword(tokens[start + 1], .subscription)))
    {
        if (end <= start + 3) return true;
        return isGeneratedDdlTrailingBoundary(last);
    }
    if (tokenMatchesKeyword(first, .alter) and tokenMatchesText(tokens[start + 1], "transform")) {
        if (end <= start + 3) return true;
        return isGeneratedDdlTrailingBoundary(last);
    }
    if (tokenMatchesKeyword(first, .drop)) {
        if (tokenMatchesKeyword(tokens[start + 1], .materialized)) {
            if (end <= start + 3) return true;
            if (tokenMatchesKeyword(tokens[start + 2], .view)) {
                if (end <= start + 3) return true;
                return isGeneratedDdlTrailingBoundary(last);
            }
        }
        if (tokenMatchesKeyword(tokens[start + 1], .table) or
            tokenMatchesKeyword(tokens[start + 1], .view) or
            tokenMatchesKeyword(tokens[start + 1], .domain) or
            tokenMatchesKeyword(tokens[start + 1], .sequence) or
            tokenMatchesKeyword(tokens[start + 1], .type) or
            tokenMatchesKeyword(tokens[start + 1], .tablespace) or
            tokenMatchesKeyword(tokens[start + 1], .publication) or
            tokenMatchesKeyword(tokens[start + 1], .subscription) or
            tokenMatchesKeyword(tokens[start + 1], .index) or
            tokenMatchesKeyword(tokens[start + 1], .database) or
            tokenMatchesKeyword(tokens[start + 1], .schema) or
            tokenMatchesKeyword(tokens[start + 1], .extension))
        {
            if (end <= start + 2) return true;
            return isGeneratedDdlTrailingBoundary(last);
        }
        if (tokenMatchesText(tokens[start + 1], "transform")) {
            if (end <= start + 2) return true;
            return isGeneratedDdlTrailingBoundary(last);
        }
    }
    if (tokenMatchesKeyword(first, .refresh)) {
        if (end <= start + 3) return true;
        if (tokenMatchesKeyword(tokens[start + 1], .materialized) and tokenMatchesKeyword(tokens[start + 2], .view)) {
            if (end <= start + 4) return true;
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
        tokenMatchesKeyword(token, .@"for") or
        tokenMatchesKeyword(token, .with) or
        tokenMatchesKeyword(token, .where) or
        tokenMatchesKeyword(token, .include) or
        tokenMatchesKeyword(token, .add) or
        tokenMatchesKeyword(token, .before) or
        tokenMatchesKeyword(token, .drop) or
        tokenMatchesKeyword(token, .rename) or
        tokenMatchesKeyword(token, .validate) or
        tokenMatchesKeyword(token, .column) or
        tokenMatchesKeyword(token, .constraint) or
        tokenMatchesKeyword(token, .set) or
        tokenMatchesKeyword(token, .default) or
        tokenMatchesKeyword(token, .null) or
        tokenMatchesKeyword(token, .to) or
        tokenMatchesKeyword(token, .table) or
        tokenMatchesKeyword(token, .publication) or
        tokenMatchesKeyword(token, .as) or
        tokenMatchesKeyword(token, .restart) or
        tokenMatchesKeyword(token, .by) or
        tokenMatchesKeyword(token, .no) or
        tokenMatchesKeyword(token, .data) or
        tokenMatchesText(token, "connection") or
        tokenMatchesText(token, "concurrently") or
        tokenMatchesText(token, "enable") or
        tokenMatchesText(token, "disable");
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
    if (last.kind == .lparen or isGeneratedSqlTrailingOperatorToken(last)) return true;
    if (isGeneratedReadTrailingQuantifier(tokens, start, end)) return true;
    if (generatedReadHasCompleteSourceBefore(tokens, start, end - 1) and
        (tokenMatchesKeyword(last, .@"union") or
            tokenMatchesKeyword(last, .intersect) or
            tokenMatchesKeyword(last, .except)))
    {
        return true;
    }
    if (isIncompleteGeneratedReadRowLockTail(tokens, start, end)) return true;
    if (isIncompleteGeneratedReadWindowClauseTail(tokens, start, end)) return true;
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
            tokenMatchesKeyword(last, .@"and") or
            tokenMatchesKeyword(last, .@"or") or
            tokenMatchesKeyword(last, .is) or
            tokenMatchesKeyword(last, .not) or
            tokenMatchesKeyword(last, .in) or
            tokenMatchesKeyword(last, .exists) or
            tokenMatchesKeyword(last, .like) or
            tokenMatchesKeyword(last, .ilike) or
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

fn isIncompleteGeneratedReadWindowClauseTail(tokens: []const Token, start: usize, end: usize) bool {
    if (end <= start + 1 or end > tokens.len) return false;
    const last = tokens[end - 1];
    if (last.kind != .identifier) return false;

    var depth: usize = 0;
    var index = start;
    var window_index: ?usize = null;
    while (index < end and index < tokens.len) : (index += 1) {
        switch (tokens[index].kind) {
            .lparen, .lbracket => depth += 1,
            .rparen, .rbracket => {
                if (depth > 0) depth -= 1;
            },
            else => {},
        }
        if (depth != 0 or !tokenMatchesKeyword(tokens[index], .window)) continue;
        if (generatedReadHasCompleteSourceBefore(tokens, start, index)) window_index = index;
    }
    const found_window = window_index orelse return false;
    if (end <= found_window + 1) return false;
    const previous = tokens[end - 2];
    return tokenMatchesKeyword(previous, .window) or previous.kind == .comma;
}

fn isGeneratedSqlTrailingOperatorToken(token: Token) bool {
    return switch (token.kind) {
        .eq,
        .neq,
        .gt,
        .gte,
        .lt,
        .lte,
        .plus,
        .minus,
        .slash,
        .percent,
        .at_contains,
        .range_overlap,
        .pipe_concat,
        .question,
        .question_any,
        .question_all,
        .arrow_json,
        .arrow_text,
        .path_arrow_json,
        .path_arrow_text,
        .regex_match,
        .regex_imatch,
        .regex_not_match,
        .regex_not_imatch,
        => true,
        else => false,
    };
}

fn isGeneratedReadTrailingQuantifier(tokens: []const Token, start: usize, end: usize) bool {
    if (end <= start + 1 or end > tokens.len) return false;
    const last = tokens[end - 1];
    if (!tokenMatchesKeyword(last, .any) and !tokenMatchesKeyword(last, .some) and !tokenMatchesKeyword(last, .all)) return false;
    const previous = tokens[end - 2];
    return isGeneratedSqlTrailingOperatorToken(previous) or
        tokenMatchesKeyword(previous, .like) or
        tokenMatchesKeyword(previous, .ilike);
}

fn isIncompleteGeneratedReadRowLockTail(tokens: []const Token, start: usize, end: usize) bool {
    if (start >= end or end > tokens.len) return false;
    const lock_start = generatedReadRowLockStart(tokens, start, end) orelse return false;
    const lock_len = end - lock_start;
    if (lock_len == 1) return true;
    const last = tokens[end - 1];
    if (tokenMatchesKeyword(last, .of)) {
        return generatedReadLockModeEndsBefore(tokens, lock_start + 1, end - 1);
    }
    if (tokenMatchesText(last, "skip")) {
        return generatedReadLockModeEndsBefore(tokens, lock_start + 1, end - 1);
    }
    if (lock_len == 2 and
        (tokenMatchesKeyword(last, .no) or tokenMatchesKeyword(last, .key)))
    {
        return true;
    }
    if (lock_len == 3 and
        tokenMatchesKeyword(tokens[lock_start + 1], .no) and
        tokenMatchesKeyword(last, .key))
    {
        return true;
    }
    return false;
}

fn generatedReadRowLockStart(tokens: []const Token, start: usize, end: usize) ?usize {
    var depth: usize = 0;
    var index = start;
    var candidate: ?usize = null;
    while (index < end and index < tokens.len) : (index += 1) {
        switch (tokens[index].kind) {
            .lparen, .lbracket => depth += 1,
            .rparen, .rbracket => {
                if (depth > 0) depth -= 1;
            },
            else => {},
        }
        if (depth != 0 or !tokenMatchesKeyword(tokens[index], .@"for")) continue;
        if (generatedReadHasCompleteSourceBefore(tokens, start, index)) candidate = index;
    }
    return candidate;
}

fn generatedReadLockModeEndsBefore(tokens: []const Token, start: usize, end: usize) bool {
    if (start >= end or end > tokens.len) return false;
    const len = end - start;
    if (len == 1) {
        return tokenMatchesKeyword(tokens[start], .update) or
            tokenMatchesKeyword(tokens[start], .share);
    }
    if (len == 2 and
        tokenMatchesKeyword(tokens[start], .key) and
        tokenMatchesKeyword(tokens[start + 1], .share))
    {
        return true;
    }
    if (len == 3 and
        tokenMatchesKeyword(tokens[start], .no) and
        tokenMatchesKeyword(tokens[start + 1], .key) and
        tokenMatchesKeyword(tokens[start + 2], .update))
    {
        return true;
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
    if (last.kind == .lparen or isGeneratedSqlTrailingOperatorToken(last)) return true;
    if (isGeneratedDmlTrailingQuantifier(tokens, start, end)) return true;
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

fn isGeneratedDmlStatementHead(tokens: []const Token, raw_statement: RawSqlStatement) bool {
    const start = raw_statement.token_start;
    if (start >= raw_statement.token_end or raw_statement.token_end > tokens.len) return false;
    const first = tokens[start];
    return tokenMatchesKeyword(first, .insert) or
        tokenMatchesKeyword(first, .update) or
        tokenMatchesKeyword(first, .delete) or
        tokenMatchesKeyword(first, .truncate) or
        tokenMatchesKeyword(first, .merge);
}

fn isGeneratedDmlTrailingQuantifier(tokens: []const Token, start: usize, end: usize) bool {
    if (end <= start + 1 or end > tokens.len) return false;
    const last = tokens[end - 1];
    if (!tokenMatchesKeyword(last, .any) and !tokenMatchesKeyword(last, .some) and !tokenMatchesKeyword(last, .all)) return false;
    const previous = tokens[end - 2];
    return isGeneratedSqlTrailingOperatorToken(previous) or
        tokenMatchesKeyword(previous, .like) or
        tokenMatchesKeyword(previous, .ilike);
}

fn isIncompleteGeneratedUnsupportedBoundary(tokens: []const Token, raw_statement: RawSqlStatement) bool {
    const start = raw_statement.token_start;
    const end = raw_statement.token_end;
    if (start >= end or end > tokens.len) return false;
    if (end != start + 1) return false;
    const first = tokens[start];
    return tokenMatchesKeyword(first, .call) or
        tokenMatchesKeyword(first, .cluster) or
        tokenMatchesKeyword(first, .comment) or
        tokenMatchesKeyword(first, .copy) or
        tokenMatchesText(first, "do") or
        tokenMatchesKeyword(first, .grant) or
        tokenMatchesKeyword(first, .listen) or
        tokenMatchesText(first, "load") or
        tokenMatchesText(first, "lock") or
        tokenMatchesKeyword(first, .match) or
        tokenMatchesKeyword(first, .notify) or
        tokenMatchesKeyword(first, .reindex) or
        tokenMatchesKeyword(first, .revoke) or
        tokenMatchesKeyword(first, .security) or
        tokenMatchesKeyword(first, .unlisten);
}

fn parseStatement(
    raw_statement: RawSqlStatement,
    generated_statement: ?GeneratedRawSqlStatement,
    tokenized_sql: *const TokenizedSql,
) ParsedStatement {
    if (generated_statement) |generated_raw| {
        switch (generated_raw.statement) {
            .session => |kind| if (generatedSessionAstHasValidClassificationPayload(tokenized_sql.items(), generated_raw, kind))
                return .{ .session = .{ .raw = raw_statement } }
            else
                return .{ .unknown = raw_statement },
            .transaction => |kind| if (generatedTransactionAstHasValidClassificationPayload(tokenized_sql.items(), generated_raw, kind))
                return .{ .transaction = .{ .raw = raw_statement } }
            else
                return .{ .unknown = raw_statement },
            .prepared => |kind| if (generatedPreparedAstHasValidClassificationPayload(tokenized_sql.items(), generated_raw, kind))
                return .{ .prepared = .{ .raw = raw_statement } }
            else
                return .{ .unknown = raw_statement },
            .prepared_transaction => |kind| if (generatedPreparedTransactionAstHasValidClassificationPayload(tokenized_sql.items(), generated_raw, kind))
                return .{ .ddl = .{ .raw = raw_statement } }
            else
                return .{ .unknown = raw_statement },
            .ddl => |kind| if (generatedDdlAstHasValidClassificationPayload(tokenized_sql.items(), generated_raw, kind))
                return .{ .ddl = .{ .raw = raw_statement } }
            else
                return .{ .unknown = raw_statement },
            .extension_index => |kind| if (generatedDdlAstHasValidClassificationPayload(tokenized_sql.items(), generated_raw, generatedDdlKindFromExtensionIndexKind(kind)))
                return .{ .ddl = .{ .raw = raw_statement } }
            else
                return .{ .unknown = raw_statement },
            .dml => if (generatedDmlStatementKind(tokenized_sql.items(), generated_raw)) |generated| {
                const classified_recursive_kind = if (generated.recursive) classifier.classifyRecursiveWriteStatement(tokenized_sql.items()) else null;
                if (classified_recursive_kind orelse tokenized_sql.write_statement_kind) |classified_kind| {
                    if (!generatedDmlStatementKindMatchesWriteKind(generated.kind, classified_kind)) return .{ .unknown = raw_statement };
                    return .{ .write = .{ .kind = classified_kind, .raw = raw_statement, .recursive = generated.recursive } };
                }
                return .{ .write = .{ .kind = generated.defaultWriteKind(), .raw = raw_statement, .recursive = generated.recursive } };
            } else return .{ .unknown = raw_statement },
            .read => if (generatedReadStatementKind(tokenized_sql.items(), generated_raw)) |kind| {
                if (tokenized_sql.read_statement_kind) |classified_kind| {
                    if (classified_kind != kind) return .{ .unknown = raw_statement };
                }
                return .{ .read = .{ .kind = kind, .raw = raw_statement } };
            } else return .{ .unknown = raw_statement },
            .graph => |kind| if (generatedGraphAstHasValidClassificationPayload(tokenized_sql.items(), generated_raw, kind))
                return .{ .ddl = .{ .raw = raw_statement } }
            else
                return .{ .unknown = raw_statement },
            .cursor => |kind| if (generatedCursorAstHasValidClassificationPayload(tokenized_sql.items(), generated_raw, kind))
                return .{ .ddl = .{ .raw = raw_statement } }
            else
                return .{ .unknown = raw_statement },
            .unsupported => |kind| {
                if (!generatedUnsupportedAstHasValidClassificationPayload(tokenized_sql.items(), generated_raw, kind)) return .{ .unknown = raw_statement };
                if (generatedUnsupportedUsesDdlPlanBoundary(kind)) return .{ .ddl = .{ .raw = raw_statement } };
                if (kind == .explain) return .{ .explain = parseGeneratedExplainStatement(raw_statement, tokenized_sql.items(), generated_raw) };
                return .{ .unsupported = .{ .kind = kind, .raw = raw_statement } };
            },
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

fn generatedSessionAstHasValidClassificationPayload(
    tokens: []const Token,
    generated_raw: GeneratedRawSqlStatement,
    expected_kind: generated_parser.GeneratedSqlSessionKind,
) bool {
    const ast_value = generated_raw.ast orelse return false;
    const session_ast = switch (ast_value) {
        .session => |session| session,
        else => return false,
    };
    if (session_ast.kind != expected_kind) return false;
    const end = generatedControlStatementEnd(tokens, session_ast.statement_span) orelse return false;
    if (!std.meta.eql(session_ast.command_span, tokens[0].sourceSpan())) return false;
    if (!generatedControlOptionalTokenRangeIsValid(tokens, end, session_ast.name_tokens)) return false;
    if (!generatedControlOptionalTokenRangeIsValid(tokens, end, session_ast.value_tokens)) return false;
    return switch (session_ast.kind) {
        .set, .reset, .show => session_ast.name_tokens != null,
        .discard_all => session_ast.name_tokens == null and session_ast.value_tokens == null,
    };
}

fn generatedTransactionAstHasValidClassificationPayload(
    tokens: []const Token,
    generated_raw: GeneratedRawSqlStatement,
    expected_kind: generated_parser.GeneratedSqlTransactionKind,
) bool {
    const ast_value = generated_raw.ast orelse return false;
    const transaction_ast = switch (ast_value) {
        .transaction => |transaction| transaction,
        else => return false,
    };
    if (transaction_ast.kind != expected_kind) return false;
    const end = generatedControlStatementEnd(tokens, transaction_ast.statement_span) orelse return false;
    if (!std.meta.eql(transaction_ast.command_span, tokens[0].sourceSpan())) return false;
    if (!generatedControlOptionalTokenRangeIsValid(tokens, end, transaction_ast.boundary_tail_tokens)) return false;
    if (!generatedControlOptionalTokenRangeIsValid(tokens, end, transaction_ast.mode_tokens)) return false;
    if (!generatedControlOptionalTokenRangeIsValid(tokens, end, transaction_ast.name_tokens)) return false;
    return switch (transaction_ast.kind) {
        .set_transaction => transaction_ast.mode_tokens != null,
        .savepoint, .release_savepoint, .rollback_to_savepoint => transaction_ast.name_tokens != null,
        .start_transaction, .begin, .commit, .rollback => true,
    };
}

fn generatedPreparedAstHasValidClassificationPayload(
    tokens: []const Token,
    generated_raw: GeneratedRawSqlStatement,
    expected_kind: generated_parser.GeneratedSqlPreparedKind,
) bool {
    const ast_value = generated_raw.ast orelse return false;
    const prepared_ast = switch (ast_value) {
        .prepared => |prepared| prepared,
        else => return false,
    };
    if (prepared_ast.kind != expected_kind) return false;
    const end = generatedControlStatementEnd(tokens, prepared_ast.statement_span) orelse return false;
    if (!std.meta.eql(prepared_ast.command_span, tokens[0].sourceSpan())) return false;
    if (!generatedControlOptionalTokenRangeIsValid(tokens, end, prepared_ast.name_tokens)) return false;
    if (!generatedControlOptionalTokenRangeIsValid(tokens, end, prepared_ast.parameter_tokens)) return false;
    if (!generatedControlOptionalTokenRangeIsValid(tokens, end, prepared_ast.argument_tokens)) return false;
    if (!generatedControlOptionalTokenRangeIsValid(tokens, end, prepared_ast.inner_statement_tokens)) return false;
    return switch (prepared_ast.kind) {
        .prepare => prepared_ast.name_tokens != null and prepared_ast.inner_statement_tokens != null,
        .execute, .deallocate => prepared_ast.name_tokens != null,
    };
}

fn generatedPreparedTransactionAstHasValidClassificationPayload(
    tokens: []const Token,
    generated_raw: GeneratedRawSqlStatement,
    expected_kind: generated_parser.GeneratedSqlPreparedTransactionKind,
) bool {
    const ast_value = generated_raw.ast orelse return false;
    const prepared_transaction_ast = switch (ast_value) {
        .prepared_transaction => |prepared_transaction| prepared_transaction,
        else => return false,
    };
    if (prepared_transaction_ast.kind != expected_kind) return false;
    const end = generatedControlStatementEnd(tokens, prepared_transaction_ast.statement_span) orelse return false;
    if (!std.meta.eql(prepared_transaction_ast.command_span, tokens[0].sourceSpan())) return false;
    if (!generatedControlOptionalTokenRangeIsValid(tokens, end, prepared_transaction_ast.gid_tokens)) return false;
    return prepared_transaction_ast.gid_tokens != null;
}

fn generatedCursorAstHasValidClassificationPayload(
    tokens: []const Token,
    generated_raw: GeneratedRawSqlStatement,
    expected_kind: generated_parser.GeneratedSqlCursorKind,
) bool {
    const ast_value = generated_raw.ast orelse return false;
    const cursor_ast = switch (ast_value) {
        .cursor => |cursor| cursor,
        else => return false,
    };
    if (cursor_ast.kind != expected_kind) return false;
    const end = generatedControlStatementEnd(tokens, cursor_ast.statement_span) orelse return false;
    if (!std.meta.eql(cursor_ast.command_span, tokens[0].sourceSpan())) return false;
    if (!generatedControlOptionalTokenRangeIsValid(tokens, end, cursor_ast.tail_tokens)) return false;
    return cursor_ast.tail_tokens != null;
}

fn generatedControlStatementEnd(tokens: []const Token, statement_span: SourceSpan) ?usize {
    if (tokens.len == 0) return null;
    var end = tokens.len;
    while (end > 0 and tokens[end - 1].kind == .semicolon) {
        end -= 1;
    }
    if (end == 0) return null;
    if (tokens[0].sourceSpan().start != statement_span.start) return null;
    if (tokens[end - 1].sourceSpan().end != statement_span.end) return null;
    return end;
}

fn generatedControlOptionalTokenRangeIsValid(
    tokens: []const Token,
    end: usize,
    range: ?generated_parser.GeneratedSqlTokenRange,
) bool {
    if (range) |value| return end <= tokens.len and value.start < value.end and value.end <= end;
    return true;
}

fn generatedDdlKindFromExtensionIndexKind(kind: generated_parser.GeneratedSqlExtensionIndexKind) generated_parser.GeneratedSqlDdlKind {
    return switch (kind) {
        .create_index => .create_index,
        .drop_index => .drop_index,
        .create_extension => .create_extension,
        .drop_extension => .drop_extension,
    };
}

fn generatedDdlAstHasValidClassificationPayload(
    tokens: []const Token,
    generated_raw: GeneratedRawSqlStatement,
    expected_kind: generated_parser.GeneratedSqlDdlKind,
) bool {
    const ast_value = generated_raw.ast orelse return false;
    const ddl_ast = switch (ast_value) {
        .ddl => |ddl| ddl,
        .extension_index => |ddl| ddl,
        else => return false,
    };
    if (ddl_ast.kind != expected_kind) return false;
    const end = generatedDdlStatementEnd(tokens, ddl_ast.statement_span) orelse return false;
    if (!std.meta.eql(ddl_ast.command_span, tokens[0].sourceSpan())) return false;
    if (!generatedDdlOptionalTokenRangeIsValid(tokens, end, ddl_ast.object_name_tokens)) return false;
    if (!generatedDdlOptionalTokenRangeIsValid(tokens, end, ddl_ast.schema_name_tokens)) return false;
    if (!generatedDdlOptionalTokenRangeIsValid(tokens, end, ddl_ast.version_tokens)) return false;
    if (!generatedDdlOptionalTokenRangeIsValid(tokens, end, ddl_ast.index_table_tokens)) return false;
    if (!generatedDdlOptionalTokenRangeIsValid(tokens, end, ddl_ast.index_method_tokens)) return false;
    if (!generatedDdlOptionalTokenRangeIsValid(tokens, end, ddl_ast.index_elements_tokens)) return false;
    if (!generatedDdlOptionalTokenRangeIsValid(tokens, end, ddl_ast.index_include_tokens)) return false;
    if (!generatedDdlOptionalTokenRangeIsValid(tokens, end, ddl_ast.index_options_tokens)) return false;
    if (!generatedDdlOptionalTokenRangeIsValid(tokens, end, ddl_ast.index_where_tokens)) return false;
    if (!generatedDdlOptionalTokenRangeIsValid(tokens, end, ddl_ast.alter_table_operation_tokens)) return false;
    if (!generatedDdlShapePayloadIsValid(tokens, end, ddl_ast)) return false;
    return generatedDdlRequiredRangesArePresent(ddl_ast);
}

fn generatedDdlStatementEnd(tokens: []const Token, statement_span: SourceSpan) ?usize {
    if (tokens.len == 0) return null;
    var end = tokens.len;
    while (end > 0 and tokens[end - 1].kind == .semicolon) {
        end -= 1;
    }
    if (end == 0) return null;
    if (tokens[0].sourceSpan().start != statement_span.start) return null;
    if (tokens[end - 1].sourceSpan().end != statement_span.end) return null;
    return end;
}

fn generatedDdlOptionalTokenRangeIsValid(
    tokens: []const Token,
    end: usize,
    range: ?generated_parser.GeneratedSqlTokenRange,
) bool {
    if (range) |value| return end <= tokens.len and value.start < value.end and value.end <= end;
    return true;
}

fn generatedDdlShapePayloadIsValid(
    tokens: []const Token,
    end: usize,
    ddl_ast: generated_parser.GeneratedSqlDdlAst,
) bool {
    return switch (ddl_ast.kind) {
        .create_index => generatedDdlCreateIndexPayloadIsValid(tokens, end, ddl_ast),
        .create_policy, .alter_policy, .drop_policy => generatedDdlPolicyPayloadIsValid(tokens, end, ddl_ast),
        else => true,
    };
}

fn generatedDdlCreateIndexPayloadIsValid(
    tokens: []const Token,
    end: usize,
    ddl_ast: generated_parser.GeneratedSqlDdlAst,
) bool {
    if (end < 6 or !tokens[0].matchesKeywordTag(.create)) return false;
    var cursor: usize = 1;
    if (ddl_ast.unique) {
        if (cursor >= end or !tokens[cursor].matchesKeywordTag(.unique)) return false;
        cursor += 1;
    }
    if (cursor >= end or !tokens[cursor].matchesKeywordTag(.index)) return false;
    cursor += 1;
    if (ddl_ast.if_not_exists) {
        if (cursor + 2 >= end or !tokens[cursor].matchesKeywordTag(.@"if") or !tokens[cursor + 1].matchesKeywordTag(.not) or !tokens[cursor + 2].matchesKeywordTag(.exists)) return false;
        cursor += 3;
    }

    const object = ddl_ast.object_name_tokens orelse return false;
    if (object.start != cursor or object.end <= object.start or object.end > end) return false;
    cursor = object.end;
    if (cursor >= end or !tokens[cursor].matchesKeywordTag(.on)) return false;
    cursor += 1;

    const table = ddl_ast.index_table_tokens orelse return false;
    if (table.start != cursor or table.end <= table.start or table.end > end) return false;
    cursor = table.end;

    if (ddl_ast.index_method_tokens) |method| {
        if (cursor >= end or !tokens[cursor].matchesKeywordTag(.using)) return false;
        if (method.start != cursor + 1 or method.end != method.start + 1 or method.end > end) return false;
        cursor = method.end;
    }

    const elements = ddl_ast.index_elements_tokens orelse return false;
    if (cursor >= end or tokens[cursor].kind != .lparen) return false;
    if (elements.start != cursor + 1 or elements.end <= elements.start or elements.end >= end) return false;
    if (tokens[elements.end].kind != .rparen) return false;
    cursor = elements.end + 1;

    if (ddl_ast.index_include_tokens) |include| {
        if (cursor + 2 >= end or !tokens[cursor].matchesKeywordTag(.include) or tokens[cursor + 1].kind != .lparen) return false;
        if (include.start != cursor + 2 or include.end <= include.start or include.end >= end) return false;
        if (tokens[include.end].kind != .rparen) return false;
        cursor = include.end + 1;
    }

    if (ddl_ast.index_options_tokens) |options| {
        if (options.start != cursor or options.end <= options.start or options.end > end) return false;
        if (!tokens[options.start].matchesKeywordTag(.with)) return false;
        if (options.start + 1 < options.end and tokens[options.start + 1].kind == .lparen) {
            if (tokens[options.end - 1].kind != .rparen) return false;
        } else if (options.end != end) {
            return false;
        }
        cursor = options.end;
    }

    if (ddl_ast.index_where_tokens) |where| {
        if (cursor >= end or !tokens[cursor].matchesKeywordTag(.where)) return false;
        if (where.start != cursor + 1 or where.end <= where.start or where.end != end) return false;
        cursor = where.end;
    }

    return cursor == end;
}

fn generatedDdlPolicyPayloadIsValid(
    tokens: []const Token,
    end: usize,
    ddl_ast: generated_parser.GeneratedSqlDdlAst,
) bool {
    if (end < 5) return false;
    var cursor: usize = 0;
    switch (ddl_ast.kind) {
        .create_policy => {
            if (!tokens[0].matchesKeywordTag(.create) or !tokens[1].matchesKeywordTag(.policy)) return false;
            cursor = 2;
        },
        .alter_policy => {
            if (!tokens[0].matchesKeywordTag(.alter) or !tokens[1].matchesKeywordTag(.policy)) return false;
            cursor = 2;
        },
        .drop_policy => {
            if (!tokens[0].matchesKeywordTag(.drop) or !tokens[1].matchesKeywordTag(.policy)) return false;
            cursor = 2;
            if (ddl_ast.if_exists) {
                if (cursor + 2 >= end or !tokens[cursor].matchesKeywordTag(.@"if") or !tokens[cursor + 1].matchesKeywordTag(.exists)) return false;
                cursor += 2;
            }
        },
        else => return false,
    }

    const policy = ddl_ast.object_name_tokens orelse return false;
    if (policy.start != cursor or policy.end != policy.start + 1 or policy.end > end) return false;
    cursor = policy.end;
    if (cursor >= end or !tokens[cursor].matchesKeywordTag(.on)) return false;
    cursor += 1;

    const table = ddl_ast.index_table_tokens orelse return false;
    if (table.start != cursor or table.end <= table.start or table.end > end) return false;
    cursor = table.end;

    switch (ddl_ast.kind) {
        .create_policy, .alter_policy => {
            if (ddl_ast.if_exists or ddl_ast.if_not_exists or ddl_ast.index_elements_tokens != null or
                ddl_ast.index_include_tokens != null or ddl_ast.index_method_tokens != null or
                ddl_ast.index_options_tokens != null or ddl_ast.index_where_tokens != null)
            {
                return false;
            }
            if (ddl_ast.alter_table_operation_tokens) |tail| {
                return tail.start == cursor and tail.end == end and tail.start < tail.end;
            }
            return cursor == end;
        },
        .drop_policy => {
            if (ddl_ast.if_not_exists or ddl_ast.alter_table_operation_tokens != null or
                ddl_ast.index_elements_tokens != null or ddl_ast.index_include_tokens != null or
                ddl_ast.index_method_tokens != null or ddl_ast.index_options_tokens != null or
                ddl_ast.index_where_tokens != null)
            {
                return false;
            }
            if (cursor == end) return !ddl_ast.cascade;
            if (cursor + 1 != end) return false;
            if (tokens[cursor].matchesKeywordTag(.cascade)) return ddl_ast.cascade;
            if (tokens[cursor].matchesKeywordTag(.restrict)) return !ddl_ast.cascade;
            return false;
        },
        else => return false,
    }
}

fn generatedDdlRequiredRangesArePresent(ddl_ast: generated_parser.GeneratedSqlDdlAst) bool {
    return switch (ddl_ast.kind) {
        .create_database,
        .create_schema,
        .create_table,
        .create_view,
        .create_materialized_view,
        .create_domain,
        .create_sequence,
        .create_enum_type,
        .create_tablespace,
        .create_publication,
        .create_subscription,
        .create_function,
        .create_procedure,
        .create_role,
        .create_collation,
        .create_aggregate,
        .create_index,
        .create_extension,
        .alter_table,
        .alter_database,
        .alter_extension,
        .alter_schema,
        .alter_tablespace,
        .alter_view,
        .alter_domain,
        .alter_sequence,
        .alter_enum_type,
        .alter_publication,
        .alter_subscription,
        .alter_role,
        .alter_collation,
        .drop_table,
        .drop_view,
        .drop_materialized_view,
        .drop_domain,
        .drop_sequence,
        .drop_enum_type,
        .drop_tablespace,
        .drop_publication,
        .drop_subscription,
        .drop_function,
        .drop_procedure,
        .drop_role,
        .drop_collation,
        .drop_aggregate,
        .drop_index,
        .drop_schema,
        .drop_database,
        .drop_extension,
        .refresh_materialized_view,
        .relation_population,
        => ddl_ast.object_name_tokens != null,
        .create_policy,
        .alter_policy,
        .drop_policy,
        => ddl_ast.object_name_tokens != null and ddl_ast.index_table_tokens != null,
        .create_operator,
        .create_cast,
        .drop_operator,
        .drop_cast,
        .create_graph_index,
        .create_graph_metric,
        => true,
    };
}

fn generatedGraphAstHasValidClassificationPayload(
    tokens: []const Token,
    generated_raw: GeneratedRawSqlStatement,
    expected_kind: generated_parser.GeneratedSqlGraphKind,
) bool {
    const ast_value = generated_raw.ast orelse return false;
    const graph_ast = switch (ast_value) {
        .graph => |graph| graph,
        else => return false,
    };
    if (graph_ast.kind != expected_kind) return false;
    const end = generatedGraphStatementEnd(tokens, graph_ast.statement_span) orelse return false;
    if (!std.meta.eql(graph_ast.command_span, tokens[0].sourceSpan())) return false;
    if (!generatedGraphOptionalTokenRangeIsValid(tokens, end, graph_ast.object_name_tokens)) return false;
    if (!generatedGraphOptionalTokenRangeIsValid(tokens, end, graph_ast.source_name_tokens)) return false;
    if (!generatedGraphOptionalTokenRangeIsValid(tokens, end, graph_ast.algorithm_tokens)) return false;
    if (!generatedGraphOptionalTokenRangeIsValid(tokens, end, graph_ast.option_tokens)) return false;
    if (!generatedGraphOptionalTokenRangeIsValid(tokens, end, graph_ast.edge_tokens)) return false;
    if (!generatedGraphOptionalTokenRangeIsValid(tokens, end, graph_ast.edge_source_tokens)) return false;
    if (!generatedGraphOptionalTokenRangeIsValid(tokens, end, graph_ast.edge_target_tokens)) return false;
    if (!generatedGraphOptionalTokenRangeIsValid(tokens, end, graph_ast.edge_type_tokens)) return false;
    if (!generatedGraphOptionalTokenRangeIsValid(tokens, end, graph_ast.edge_weight_tokens)) return false;
    if (!generatedGraphOptionalTokenRangeIsValid(tokens, end, graph_ast.extraction_enrichment_tokens)) return false;
    if (!generatedGraphOptionalTokenRangeIsValid(tokens, end, graph_ast.extraction_input_tokens)) return false;
    if (!generatedGraphOptionalTokenRangeIsValid(tokens, end, graph_ast.extraction_model_tokens)) return false;
    if (!generatedGraphOptionalTokenRangeIsValid(tokens, end, graph_ast.extraction_edges_path_tokens)) return false;
    if (!generatedGraphOptionalTokenRangeIsValid(tokens, end, graph_ast.extraction_source_tokens)) return false;
    if (!generatedGraphOptionalTokenRangeIsValid(tokens, end, graph_ast.extraction_target_tokens)) return false;
    if (!generatedGraphOptionalTokenRangeIsValid(tokens, end, graph_ast.extraction_type_tokens)) return false;
    if (!generatedGraphOptionalTokenRangeIsValid(tokens, end, graph_ast.extraction_weight_tokens)) return false;
    return generatedGraphRequiredPayloadIsValid(tokens, end, graph_ast);
}

fn generatedGraphStatementEnd(tokens: []const Token, statement_span: SourceSpan) ?usize {
    if (tokens.len == 0) return null;
    var end = tokens.len;
    while (end > 0 and tokens[end - 1].kind == .semicolon) {
        end -= 1;
    }
    if (end == 0) return null;
    if (tokens[0].sourceSpan().start != statement_span.start) return null;
    if (tokens[end - 1].sourceSpan().end != statement_span.end) return null;
    return end;
}

fn generatedGraphOptionalTokenRangeIsValid(
    tokens: []const Token,
    end: usize,
    range: ?generated_parser.GeneratedSqlTokenRange,
) bool {
    if (range) |value| return end <= tokens.len and value.start < value.end and value.end <= end;
    return true;
}

fn generatedGraphRequiredPayloadIsValid(
    tokens: []const Token,
    end: usize,
    graph_ast: generated_parser.GeneratedSqlGraphAst,
) bool {
    return switch (graph_ast.kind) {
        .create_index, .create_metric => {
            if (end < 6) return false;
            if (!tokens[0].matchesKeywordTag(.create) or !tokens[1].matchesKeywordTag(.graph)) return false;
            if (graph_ast.kind == .create_index and !tokens[2].matchesKeywordTag(.index)) return false;
            if (graph_ast.kind == .create_metric and !tokens[2].matchesKeywordTag(.metric)) return false;
            var object_start: usize = 3;
            if (graph_ast.if_not_exists) {
                if (graph_ast.kind != .create_index) return false;
                if (end < 9 or !tokens[3].matchesKeywordTag(.@"if") or !tokens[4].matchesKeywordTag(.not) or !tokens[5].matchesKeywordTag(.exists)) return false;
                object_start = 6;
            }
            if (!std.meta.eql(graph_ast.object_name_tokens orelse return false, generated_parser.GeneratedSqlTokenRange{ .start = object_start, .end = object_start + 1 })) return false;
            const source = graph_ast.source_name_tokens orelse return false;
            if (source.start == 0 or source.end > end or !tokens[source.start - 1].matchesKeywordTag(.on)) return false;
            if (graph_ast.kind == .create_metric) return generatedGraphOptionsFollow(end, source.end, graph_ast.option_tokens) and
                graph_ast.edge_tokens == null and
                graph_ast.edge_source_tokens == null and
                graph_ast.edge_target_tokens == null and
                graph_ast.edge_type_tokens == null and
                graph_ast.edge_weight_tokens == null and
                generatedGraphExtractionPayloadIsEmpty(graph_ast);
            return generatedGraphIndexEdgePayloadIsValid(tokens, end, source.end, graph_ast);
        },
        .alter_metric => {
            if (end < 8) return false;
            if (!tokens[0].matchesKeywordTag(.alter) or !tokens[1].matchesKeywordTag(.graph) or !tokens[2].matchesKeywordTag(.index)) return false;
            if (!std.meta.eql(graph_ast.source_name_tokens orelse return false, generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 4 })) return false;
            const metric = graph_ast.object_name_tokens orelse return false;
            if (metric.start < 2 or metric.end > end) return false;
            if (!tokens[metric.start - 2].matchesKeywordTag(.add) or !tokens[metric.start - 1].matchesKeywordTag(.metric)) return false;
            const algorithm = graph_ast.algorithm_tokens orelse return false;
            if (algorithm.start == 0 or algorithm.end > end or !tokens[algorithm.start - 1].matchesKeywordTag(.using)) return false;
            if (!generatedGraphOptionsFollow(end, algorithm.end, graph_ast.option_tokens)) return false;
            return true;
        },
    };
}

fn generatedGraphIndexEdgePayloadIsValid(
    tokens: []const Token,
    end: usize,
    source_end: usize,
    graph_ast: generated_parser.GeneratedSqlGraphAst,
) bool {
    if (graph_ast.extraction_enrichment_tokens != null) return generatedGraphIndexExtractionPayloadIsValid(tokens, end, source_end, graph_ast);
    const edge = graph_ast.edge_tokens orelse {
        return graph_ast.edge_source_tokens == null and
            graph_ast.edge_target_tokens == null and
            graph_ast.edge_type_tokens == null and
            graph_ast.edge_weight_tokens == null and
            generatedGraphExtractionPayloadIsEmpty(graph_ast);
    };
    if (!generatedGraphExtractionPayloadIsEmpty(graph_ast)) return false;
    if (edge.start != source_end or edge.end > end or edge.start + 4 > edge.end) return false;
    if (!tokens[edge.start].matchesKeyword("edge") or tokens[edge.start + 1].kind != .lparen or tokens[edge.end - 1].kind != .rparen) return false;
    const source = graph_ast.edge_source_tokens orelse return false;
    const target = graph_ast.edge_target_tokens orelse return false;
    if (source.start != edge.start + 2 or source.end >= target.start) return false;
    if (target.end != edge.end - 1 or target.start == 0 or tokens[target.start - 1].kind != .arrow_json) return false;
    if (!generatedGraphNestedRangeIsValid(edge, source) or !generatedGraphNestedRangeIsValid(edge, target)) return false;
    if (!generatedGraphOptionalTailFieldIsValid(edge.end, end, graph_ast.edge_type_tokens)) return false;
    if (!generatedGraphOptionalTailFieldIsValid(edge.end, end, graph_ast.edge_weight_tokens)) return false;
    const tail_end = generatedGraphIndexTailEnd(edge.end, graph_ast.edge_type_tokens, graph_ast.edge_weight_tokens);
    return generatedGraphOptionsFollow(end, tail_end, graph_ast.option_tokens);
}

fn generatedGraphIndexExtractionPayloadIsValid(
    tokens: []const Token,
    end: usize,
    source_end: usize,
    graph_ast: generated_parser.GeneratedSqlGraphAst,
) bool {
    if (graph_ast.edge_tokens != null or
        graph_ast.edge_source_tokens != null or
        graph_ast.edge_target_tokens != null or
        graph_ast.edge_type_tokens != null or
        graph_ast.edge_weight_tokens != null)
    {
        return false;
    }
    const enrichment = graph_ast.extraction_enrichment_tokens orelse return false;
    const input = graph_ast.extraction_input_tokens orelse return false;
    const model = graph_ast.extraction_model_tokens orelse return false;
    const path = graph_ast.extraction_edges_path_tokens orelse return false;
    const source = graph_ast.extraction_source_tokens orelse return false;
    const target = graph_ast.extraction_target_tokens orelse return false;
    if (source_end + 2 > enrichment.start) return false;
    if (!tokens[source_end].matchesKeywordTag(.source) or !tokens[source_end + 1].matchesKeyword("enrichment")) return false;
    if (enrichment.end >= input.start or !tokens[input.start - 1].matchesKeywordTag(.from)) return false;
    if (input.end + 3 > model.start) return false;
    if (!tokens[input.end].matchesKeywordTag(.using) or !tokens[input.end + 1].matchesKeyword("extractor") or !tokens[input.end + 2].matchesKeyword("model")) return false;
    if (model.start + 1 != model.end or (tokens[model.start].kind != .string and tokens[model.start].kind != .identifier)) return false;
    if (model.end + 2 > path.start) return false;
    if (!tokens[model.end].matchesKeyword("edges") or !tokens[model.end + 1].matchesKeyword("json_path")) return false;
    if (path.start + 1 != path.end or tokens[path.start].kind != .string) return false;
    if (path.end >= source.start or !tokens[source.start - 1].matchesKeywordTag(.source)) return false;
    if (source.end >= target.start or !tokens[target.start - 1].matchesKeyword("target")) return false;
    if (!generatedGraphOptionalTailFieldIsValid(target.end, end, graph_ast.extraction_type_tokens)) return false;
    if (!generatedGraphOptionalTailFieldIsValid(target.end, end, graph_ast.extraction_weight_tokens)) return false;
    const tail_end = generatedGraphIndexTailEnd(target.end, graph_ast.extraction_type_tokens, graph_ast.extraction_weight_tokens);
    return generatedGraphOptionsFollow(end, tail_end, graph_ast.option_tokens);
}

fn generatedGraphExtractionPayloadIsEmpty(graph_ast: generated_parser.GeneratedSqlGraphAst) bool {
    return graph_ast.extraction_enrichment_tokens == null and
        graph_ast.extraction_input_tokens == null and
        graph_ast.extraction_model_tokens == null and
        graph_ast.extraction_edges_path_tokens == null and
        graph_ast.extraction_source_tokens == null and
        graph_ast.extraction_target_tokens == null and
        graph_ast.extraction_type_tokens == null and
        graph_ast.extraction_weight_tokens == null;
}

fn generatedGraphNestedRangeIsValid(
    outer: generated_parser.GeneratedSqlTokenRange,
    inner: generated_parser.GeneratedSqlTokenRange,
) bool {
    return inner.start >= outer.start and inner.start < inner.end and inner.end <= outer.end;
}

fn generatedGraphOptionalTailFieldIsValid(
    min_start: usize,
    end: usize,
    range: ?generated_parser.GeneratedSqlTokenRange,
) bool {
    if (range) |value| return value.start >= min_start and value.start < value.end and value.end <= end;
    return true;
}

fn generatedGraphIndexTailEnd(
    edge_end: usize,
    type_tokens: ?generated_parser.GeneratedSqlTokenRange,
    weight_tokens: ?generated_parser.GeneratedSqlTokenRange,
) usize {
    var tail_end = edge_end;
    if (type_tokens) |value| tail_end = @max(tail_end, value.end);
    if (weight_tokens) |value| tail_end = @max(tail_end, value.end);
    return tail_end;
}

fn generatedGraphOptionsFollow(
    end: usize,
    required_end: usize,
    options: ?generated_parser.GeneratedSqlTokenRange,
) bool {
    if (options) |range| {
        return range.start == required_end and range.end == end and range.start < range.end;
    }
    return required_end == end;
}

fn generatedUnsupportedAstHasValidClassificationPayload(
    tokens: []const Token,
    generated_raw: GeneratedRawSqlStatement,
    expected_kind: generated_parser.GeneratedSqlUnsupportedKind,
) bool {
    const ast_value = generated_raw.ast orelse return false;
    const unsupported_ast = switch (ast_value) {
        .unsupported => |unsupported| unsupported,
        else => return false,
    };
    if (unsupported_ast.kind != expected_kind) return false;
    if (unsupported_ast.reason != generated_parser.generatedSqlUnsupportedReasonForKind(unsupported_ast.kind)) return false;
    const end = generatedUnsupportedStatementEnd(tokens, unsupported_ast.statement_span) orelse return false;
    if (!std.meta.eql(unsupported_ast.command_span, tokens[0].sourceSpan())) return false;
    if (!generatedUnsupportedOptionalTokenRangeIsValid(tokens, end, unsupported_ast.subject_tokens)) return false;
    if (!generatedUnsupportedOptionalTokenRangeIsValid(tokens, end, unsupported_ast.explain_options_tokens)) return false;
    if (!generatedUnsupportedShapePayloadIsValid(tokens, end, unsupported_ast)) return false;
    return true;
}

fn generatedUnsupportedShapePayloadIsValid(
    tokens: []const Token,
    end: usize,
    unsupported_ast: generated_parser.GeneratedSqlUnsupportedAst,
) bool {
    if (!generatedUnsupportedSubjectRangeMatchesKind(end, unsupported_ast)) return false;
    return switch (unsupported_ast.kind) {
        .graph_query => generatedGraphQueryUnsupportedPayloadIsValid(tokens, end, unsupported_ast),
        else => true,
    };
}

fn generatedUnsupportedSubjectRangeMatchesKind(
    end: usize,
    unsupported_ast: generated_parser.GeneratedSqlUnsupportedAst,
) bool {
    if (unsupported_ast.kind == .explain) return true;
    const start = generatedUnsupportedSubjectStartForKind(unsupported_ast.kind, end) orelse
        return unsupported_ast.subject_tokens == null;
    const subject = unsupported_ast.subject_tokens orelse return false;
    return subject.start == start and subject.end == end;
}

fn generatedUnsupportedSubjectStartForKind(kind: generated_parser.GeneratedSqlUnsupportedKind, end: usize) ?usize {
    const start: usize = switch (kind) {
        .create_trigger,
        .drop_trigger,
        => 2,
        else => 1,
    };
    return if (end > start) start else null;
}

fn generatedGraphQueryUnsupportedPayloadIsValid(
    tokens: []const Token,
    end: usize,
    unsupported_ast: generated_parser.GeneratedSqlUnsupportedAst,
) bool {
    if (unsupported_ast.reason != .graph_query_not_planned_by_generated_parser) return false;
    if (end < 4 or !tokens[0].matchesKeywordTag(.match)) return false;
    const subject = unsupported_ast.subject_tokens orelse return false;
    if (subject.start != 1 or subject.end != end) return false;
    var index = subject.start;
    while (index < subject.end) : (index += 1) {
        if (tokens[index].matchesKeyword("return")) return true;
    }
    return false;
}

fn generatedUnsupportedStatementEnd(tokens: []const Token, statement_span: SourceSpan) ?usize {
    if (tokens.len == 0) return null;
    var end = tokens.len;
    while (end > 0 and tokens[end - 1].kind == .semicolon) {
        end -= 1;
    }
    if (end == 0) return null;
    if (tokens[0].sourceSpan().start != statement_span.start) return null;
    if (tokens[end - 1].sourceSpan().end != statement_span.end) return null;
    return end;
}

fn generatedUnsupportedOptionalTokenRangeIsValid(
    tokens: []const Token,
    end: usize,
    range: ?generated_parser.GeneratedSqlTokenRange,
) bool {
    if (range) |value| return end <= tokens.len and value.start < value.end and value.end <= end;
    return true;
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
    tokens: []const Token,
    generated_raw: GeneratedRawSqlStatement,
) ?GeneratedDmlStatementKind {
    const ast_value = generated_raw.ast orelse return null;
    const dml_ast = switch (ast_value) {
        .dml => |dml| dml,
        else => return null,
    };
    if (!generatedDmlAstHasValidClassificationPayload(tokens, dml_ast)) return null;
    return .{ .kind = dml_ast.kind, .recursive = dml_ast.cte_recursive };
}

fn generatedDmlAstHasValidClassificationPayload(
    tokens: []const Token,
    dml_ast: generated_parser.GeneratedSqlDmlAst,
) bool {
    const end = generatedDmlStatementEnd(tokens, dml_ast.statement_span) orelse return false;
    const command_start = generatedDmlCommandStart(tokens, dml_ast, end) orelse return false;
    if (!std.meta.eql(dml_ast.command_span, tokens[command_start].sourceSpan())) return false;

    if (!generatedDmlOptionalTokenRangeIsValid(tokens, end, dml_ast.target_table_tokens)) return false;
    if (!generatedDmlOptionalTokenRangeIsValid(tokens, end, dml_ast.insert_columns_tokens)) return false;
    if (!generatedDmlOptionalTokenRangeIsValid(tokens, end, dml_ast.values_tokens)) return false;
    if (!generatedDmlOptionalTokenRangeIsValid(tokens, end, dml_ast.source_tokens)) return false;
    if (!generatedDmlOptionalTokenRangeIsValid(tokens, end, dml_ast.assignments_tokens)) return false;
    if (!generatedDmlOptionalTokenRangeIsValid(tokens, end, dml_ast.where_tokens)) return false;
    if (!generatedDmlOptionalTokenRangeIsValid(tokens, end, dml_ast.conflict_tokens)) return false;
    if (!generatedDmlOptionalTokenRangeIsValid(tokens, end, dml_ast.returning_tokens)) return false;
    if (!generatedDmlOptionalTokenRangeIsValid(tokens, end, dml_ast.additional_target_tokens)) return false;

    if (!generatedDmlSourcePayloadIsValid(tokens, end, dml_ast.source_tokens, dml_ast.source_read, dml_ast.kind)) return false;
    if (!generatedDmlTopLevelLayoutIsValid(tokens, end, command_start, dml_ast)) return false;

    const target = dml_ast.target_table_tokens orelse return false;
    if (target.start <= command_start) return false;

    return switch (dml_ast.kind) {
        .insert_values => dml_ast.source_tokens == null and dml_ast.source_read == null and (dml_ast.default_values or dml_ast.values_tokens != null),
        .insert_select => dml_ast.values_tokens == null and dml_ast.source_tokens != null and dml_ast.source_read != null,
        .update => dml_ast.assignments_tokens != null,
        .delete, .truncate => true,
        .merge => dml_ast.source_tokens != null and dml_ast.source_read != null and dml_ast.where_tokens != null,
    };
}

fn generatedDmlTopLevelLayoutIsValid(
    tokens: []const Token,
    end: usize,
    command_start: usize,
    dml_ast: generated_parser.GeneratedSqlDmlAst,
) bool {
    if (command_start >= end or !generatedDmlCommandKeywordMatchesKind(tokens[command_start], dml_ast.kind)) return false;
    return switch (dml_ast.kind) {
        .insert_values, .insert_select => generatedDmlInsertLayoutIsValid(tokens, end, command_start, dml_ast),
        .update => generatedDmlUpdateLayoutIsValid(tokens, end, command_start, dml_ast),
        .delete => generatedDmlDeleteLayoutIsValid(tokens, end, command_start, dml_ast),
        .truncate => generatedDmlTruncateLayoutIsValid(tokens, end, command_start, dml_ast),
        .merge => generatedDmlMergeLayoutIsValid(tokens, end, command_start, dml_ast),
    };
}

fn generatedDmlInsertLayoutIsValid(
    tokens: []const Token,
    end: usize,
    command_start: usize,
    dml_ast: generated_parser.GeneratedSqlDmlAst,
) bool {
    if (command_start + 2 >= end or !tokens[command_start + 1].matchesKeywordTag(.into)) return false;
    const target_start = generatedDmlOptionalOnlyTargetStart(tokens, command_start + 2, end);
    const target = dml_ast.target_table_tokens orelse return false;
    if (target.start != target_start or target.end > end) return false;
    var body_start = generatedDmlAfterOptionalAlias(tokens, target.end, end);

    if (dml_ast.insert_columns_tokens) |columns| {
        if (columns.start != body_start or columns.end <= columns.start + 1) return false;
        if (tokens[columns.start].kind != .lparen or tokens[columns.end - 1].kind != .rparen) return false;
        body_start = columns.end;
    }

    const body_end = generatedDmlInsertTailStart(dml_ast, end) orelse return false;
    if (!generatedDmlConflictReturningTailIsValid(tokens, end, body_end, dml_ast.conflict_tokens, dml_ast.returning_tokens)) return false;

    switch (dml_ast.kind) {
        .insert_values => {
            if (dml_ast.source_tokens != null or dml_ast.source_read != null) return false;
            if (dml_ast.default_values) {
                if (dml_ast.insert_columns_tokens != null or dml_ast.values_tokens != null) return false;
                if (body_start + 2 != body_end) return false;
                return tokens[body_start].matchesKeywordTag(.default) and tokens[body_start + 1].matchesKeywordTag(.values);
            }
            const values = dml_ast.values_tokens orelse return false;
            if (values.start == 0 or values.start > values.end or values.end != body_end) return false;
            if (!tokens[values.start - 1].matchesKeywordTag(.values)) return false;
            return values.start - 1 == body_start;
        },
        .insert_select => {
            if (dml_ast.values_tokens != null or dml_ast.default_values) return false;
            const source = dml_ast.source_tokens orelse return false;
            if (source.start != body_start or source.end != body_end) return false;
            return source.start < source.end and tokens[source.start].matchesKeywordTag(.select);
        },
        else => return false,
    }
}

fn generatedDmlUpdateLayoutIsValid(
    tokens: []const Token,
    end: usize,
    command_start: usize,
    dml_ast: generated_parser.GeneratedSqlDmlAst,
) bool {
    if (dml_ast.insert_columns_tokens != null or dml_ast.values_tokens != null or dml_ast.conflict_tokens != null or
        dml_ast.additional_target_tokens != null or dml_ast.default_values)
    {
        return false;
    }
    const target_start = generatedDmlOptionalOnlyTargetStart(tokens, command_start + 1, end);
    const target = dml_ast.target_table_tokens orelse return false;
    if (target.start != target_start or target.end > end) return false;
    const assignments = dml_ast.assignments_tokens orelse return false;
    if (assignments.start == 0 or assignments.start >= assignments.end or assignments.end > end) return false;
    if (!tokens[assignments.start - 1].matchesKeywordTag(.set)) return false;
    if (assignments.start - 1 < generatedDmlAfterOptionalAlias(tokens, target.end, end)) return false;
    if (dml_ast.source_tokens) |source| {
        if (source.start == 0 or source.start >= source.end or source.end > end) return false;
        if (!tokens[source.start - 1].matchesKeywordTag(.from)) return false;
        if (source.start - 1 != assignments.end) return false;
        return generatedDmlWhereReturningTailIsValid(tokens, end, source.end, dml_ast.where_tokens, dml_ast.returning_tokens, true);
    }
    return generatedDmlWhereReturningTailIsValid(tokens, end, assignments.end, dml_ast.where_tokens, dml_ast.returning_tokens, false);
}

fn generatedDmlDeleteLayoutIsValid(
    tokens: []const Token,
    end: usize,
    command_start: usize,
    dml_ast: generated_parser.GeneratedSqlDmlAst,
) bool {
    if (command_start + 2 >= end or !tokens[command_start + 1].matchesKeywordTag(.from)) return false;
    if (dml_ast.insert_columns_tokens != null or dml_ast.values_tokens != null or dml_ast.assignments_tokens != null or
        dml_ast.conflict_tokens != null or dml_ast.additional_target_tokens != null or dml_ast.default_values)
    {
        return false;
    }
    const target_start = generatedDmlOptionalOnlyTargetStart(tokens, command_start + 2, end);
    const target = dml_ast.target_table_tokens orelse return false;
    if (target.start != target_start or target.end > end) return false;
    if (dml_ast.source_tokens) |source| {
        if (source.start == 0 or source.start >= source.end or source.end > end) return false;
        if (!tokens[source.start - 1].matchesKeywordTag(.using)) return false;
        if (source.start - 1 < target.end) return false;
        return generatedDmlWhereReturningTailIsValid(tokens, end, source.end, dml_ast.where_tokens, dml_ast.returning_tokens, true);
    }
    return generatedDmlWhereReturningTailIsValid(tokens, end, target.end, dml_ast.where_tokens, dml_ast.returning_tokens, false);
}

fn generatedDmlTruncateLayoutIsValid(
    tokens: []const Token,
    end: usize,
    command_start: usize,
    dml_ast: generated_parser.GeneratedSqlDmlAst,
) bool {
    if (dml_ast.insert_columns_tokens != null or dml_ast.values_tokens != null or dml_ast.source_tokens != null or
        dml_ast.source_read != null or dml_ast.assignments_tokens != null or dml_ast.where_tokens != null or
        dml_ast.conflict_tokens != null or dml_ast.returning_tokens != null or dml_ast.default_values)
    {
        return false;
    }
    var target_start = command_start + 1;
    if (target_start < end and tokens[target_start].matchesKeywordTag(.table)) target_start += 1;
    target_start = generatedDmlOptionalOnlyTargetStart(tokens, target_start, end);
    const target = dml_ast.target_table_tokens orelse return false;
    if (target.start != target_start or target.end > end) return false;
    if (dml_ast.additional_target_tokens) |additional| {
        if (additional.start != target.end or additional.end > end) return false;
        var cursor = additional.start;
        while (cursor < additional.end) {
            if (tokens[cursor].kind != .comma) return false;
            cursor += 1;
            if (cursor >= additional.end or tokens[cursor].kind != .identifier) return false;
            cursor += 1;
        }
    }
    return true;
}

fn generatedDmlMergeLayoutIsValid(
    tokens: []const Token,
    end: usize,
    command_start: usize,
    dml_ast: generated_parser.GeneratedSqlDmlAst,
) bool {
    if (command_start + 3 >= end or !tokens[command_start + 1].matchesKeywordTag(.into)) return false;
    if (dml_ast.insert_columns_tokens != null or dml_ast.values_tokens != null or dml_ast.assignments_tokens != null or
        dml_ast.conflict_tokens != null or dml_ast.returning_tokens != null or dml_ast.additional_target_tokens != null or dml_ast.default_values)
    {
        return false;
    }
    const target = dml_ast.target_table_tokens orelse return false;
    if (target.start != command_start + 2 or target.end > end) return false;
    const source = dml_ast.source_tokens orelse return false;
    if (source.start == 0 or source.start >= source.end or source.end > end) return false;
    if (!tokens[source.start - 1].matchesKeywordTag(.using)) return false;
    if (source.start - 1 < target.end) return false;
    const predicate = dml_ast.where_tokens orelse return false;
    if (predicate.start == 0 or predicate.start >= predicate.end or predicate.end > end) return false;
    if (!tokens[predicate.start - 1].matchesKeywordTag(.on)) return false;
    return source.end == predicate.start - 1;
}

fn generatedDmlOptionalOnlyTargetStart(tokens: []const Token, start: usize, end: usize) usize {
    return if (start < end and tokens[start].matchesKeywordTag(.only)) start + 1 else start;
}

fn generatedDmlAfterOptionalAlias(tokens: []const Token, start: usize, end: usize) usize {
    if (start + 1 < end and tokens[start].matchesKeywordTag(.as) and tokens[start + 1].kind == .identifier) return start + 2;
    return start;
}

fn generatedDmlInsertTailStart(
    dml_ast: generated_parser.GeneratedSqlDmlAst,
    end: usize,
) ?usize {
    if (dml_ast.conflict_tokens) |conflict| return if (conflict.start > 0) conflict.start - 1 else null;
    if (dml_ast.returning_tokens) |returning| return if (returning.start > 0) returning.start - 1 else null;
    return end;
}

fn generatedDmlConflictReturningTailIsValid(
    tokens: []const Token,
    end: usize,
    body_end: usize,
    conflict_tokens: ?generated_parser.GeneratedSqlTokenRange,
    returning_tokens: ?generated_parser.GeneratedSqlTokenRange,
) bool {
    if (body_end > end) return false;
    const returning_start = if (returning_tokens) |returning| blk: {
        if (returning.start == 0 or returning.start > returning.end or returning.end != end) return false;
        if (!tokens[returning.start - 1].matchesKeywordTag(.returning)) return false;
        break :blk returning.start - 1;
    } else null;
    if (conflict_tokens) |conflict| {
        if (conflict.start == 0 or conflict.start > conflict.end or conflict.end > end) return false;
        if (!tokens[conflict.start - 1].matchesKeywordTag(.on) or !tokens[conflict.start].matchesKeywordTag(.conflict)) return false;
        if (conflict.start - 1 != body_end) return false;
        if (returning_start) |returning_keyword| {
            return conflict.end == returning_keyword;
        }
        return conflict.end == end;
    }
    if (returning_start) |returning_keyword| return body_end == returning_keyword;
    return body_end == end;
}

fn generatedDmlWhereReturningTailIsValid(
    tokens: []const Token,
    end: usize,
    tail_start: usize,
    where_tokens: ?generated_parser.GeneratedSqlTokenRange,
    returning_tokens: ?generated_parser.GeneratedSqlTokenRange,
    require_where: bool,
) bool {
    if (tail_start > end) return false;
    const returning_start = if (returning_tokens) |returning| blk: {
        if (returning.start == 0 or returning.start > returning.end or returning.end != end) return false;
        if (!tokens[returning.start - 1].matchesKeywordTag(.returning)) return false;
        break :blk returning.start - 1;
    } else null;
    if (where_tokens) |where| {
        if (where.start == 0 or where.start >= where.end or where.end > end) return false;
        if (!tokens[where.start - 1].matchesKeywordTag(.where)) return false;
        if (where.start - 1 < tail_start) return false;
        if (require_where and where.start - 1 != tail_start) return false;
        if (returning_start) |returning_keyword| return where.end == returning_keyword;
        return where.end == end;
    }
    if (require_where) return false;
    if (returning_start) |returning_keyword| return tail_start == returning_keyword;
    return tail_start == end;
}

fn generatedDmlStatementEnd(tokens: []const Token, statement_span: SourceSpan) ?usize {
    if (tokens.len == 0) return null;
    var end = tokens.len;
    while (end > 0 and tokens[end - 1].kind == .semicolon) {
        end -= 1;
    }
    if (end == 0) return null;
    if (tokens[0].sourceSpan().start != statement_span.start) return null;
    if (tokens[end - 1].sourceSpan().end != statement_span.end) return null;
    return end;
}

fn generatedDmlCommandStart(
    tokens: []const Token,
    dml_ast: generated_parser.GeneratedSqlDmlAst,
    end: usize,
) ?usize {
    if (end == 0 or end > tokens.len) return null;
    if (dml_ast.cte_tokens) |cte_tokens| {
        if (!generatedDmlTokenRangeIsValid(tokens, end, cte_tokens)) return null;
        if (cte_tokens.start != 1 or cte_tokens.end >= end) return null;
        if (!tokens[0].matchesKeywordTag(.with)) return null;
        const recursive = cte_tokens.start < cte_tokens.end and tokens[cte_tokens.start].matchesKeywordTag(.recursive);
        if (recursive != dml_ast.cte_recursive) return null;
        if (!generatedDmlCtePrefixIsValid(tokens, end, cte_tokens, dml_ast.cte_prefix, dml_ast.cte_recursive)) return null;
        return if (generatedDmlCommandKeywordMatchesKind(tokens[cte_tokens.end], dml_ast.kind)) cte_tokens.end else null;
    }
    if (dml_ast.cte_recursive or dml_ast.cte_prefix != null) return null;
    if (tokens[0].matchesKeywordTag(.with)) return null;
    return if (generatedDmlCommandKeywordMatchesKind(tokens[0], dml_ast.kind)) 0 else null;
}

fn generatedDmlCtePrefixIsValid(
    tokens: []const Token,
    end: usize,
    cte_tokens: generated_parser.GeneratedSqlTokenRange,
    cte_prefix: ?generated_parser.GeneratedSqlDmlCteAst,
    recursive: bool,
) bool {
    const prefix = cte_prefix orelse return false;
    if (!std.meta.eql(prefix.tokens, cte_tokens)) return false;
    if (prefix.recursive != recursive) return false;
    if (!generatedDmlTokenRangeIsValid(tokens, end, prefix.list_tokens)) return false;
    if (prefix.list_tokens.start < cte_tokens.start or prefix.list_tokens.end > cte_tokens.end) return false;
    if (prefix.count == 0 or prefix.count != prefix.items.len) return false;
    if (!std.meta.eql(prefix.items[0].name_tokens, prefix.first_name_tokens)) return false;
    if (!std.meta.eql(prefix.items[0].body_tokens, prefix.first_body_tokens)) return false;
    if (!std.meta.eql(prefix.items[prefix.items.len - 1].name_tokens, prefix.last_name_tokens)) return false;
    if (!std.meta.eql(prefix.items[prefix.items.len - 1].body_tokens, prefix.last_body_tokens)) return false;
    if (!generatedDmlReadBodyIsValid(tokens, end, prefix.first_body_tokens, prefix.first_body_read, .select_body)) return false;
    if (!generatedDmlReadBodyIsValid(tokens, end, prefix.last_body_tokens, prefix.last_body_read, .select_body)) return false;
    var expected_start = prefix.list_tokens.start;
    for (prefix.items, 0..) |item, index| {
        if (!generatedDmlTokenRangeIsValid(tokens, end, item.name_tokens)) return false;
        if (!generatedDmlCteItemLayoutIsValid(tokens, end, prefix.list_tokens, item, expected_start)) return false;
        if (!generatedDmlReadBodyIsValid(tokens, end, item.body_tokens, item.body_read, .select_body)) return false;
        if (item.body_tokens.start == 0 or item.body_tokens.end >= end) return false;
        if (tokens[item.body_tokens.start - 1].kind != .lparen or tokens[item.body_tokens.end].kind != .rparen) return false;
        expected_start = item.body_tokens.end + 1;
        if (index + 1 < prefix.items.len) {
            if (expected_start >= prefix.list_tokens.end or tokens[expected_start].kind != .comma) return false;
            expected_start += 1;
        }
    }
    return expected_start == prefix.list_tokens.end;
}

fn generatedDmlCteItemLayoutIsValid(
    tokens: []const Token,
    end: usize,
    list_tokens: generated_parser.GeneratedSqlTokenRange,
    item: generated_parser.GeneratedSqlDmlCteItemAst,
    expected_start: usize,
) bool {
    if (!generatedDmlTokenRangeIsValid(tokens, end, item.name_tokens)) return false;
    if (!generatedDmlTokenRangeIsValid(tokens, end, item.body_tokens)) return false;
    if (item.name_tokens.start != expected_start or item.name_tokens.end <= item.name_tokens.start) return false;
    if (item.name_tokens.start < list_tokens.start or item.body_tokens.end > list_tokens.end) return false;

    var cursor = item.name_tokens.end;
    if (cursor < list_tokens.end and tokens[cursor].kind == .lparen) {
        cursor = (generatedDmlMatchingParenInRange(tokens, cursor, list_tokens.end) orelse return false) + 1;
    }
    if (cursor >= list_tokens.end or !tokens[cursor].matchesKeywordTag(.as)) return false;
    cursor += 1;
    if (cursor < list_tokens.end and tokens[cursor].matchesKeywordTag(.materialized)) {
        cursor += 1;
    } else if (cursor + 1 < list_tokens.end and tokens[cursor].matchesKeywordTag(.not) and tokens[cursor + 1].matchesKeywordTag(.materialized)) {
        cursor += 2;
    }
    if (cursor >= list_tokens.end or tokens[cursor].kind != .lparen) return false;
    if (item.body_tokens.start != cursor + 1) return false;
    if (item.body_tokens.end >= list_tokens.end or tokens[item.body_tokens.end].kind != .rparen) return false;
    return true;
}

fn generatedDmlMatchingParenInRange(tokens: []const Token, open_index: usize, end: usize) ?usize {
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

const GeneratedDmlReadBodyShape = enum {
    select_body,
    relation_source,
};

fn generatedDmlSourcePayloadIsValid(
    tokens: []const Token,
    end: usize,
    source_tokens: ?generated_parser.GeneratedSqlTokenRange,
    source_read: ?generated_parser.GeneratedSqlDmlReadBodyAst,
    dml_kind: generated_parser.GeneratedSqlDmlKind,
) bool {
    if (source_tokens) |range| {
        const read = source_read orelse return false;
        const shape: GeneratedDmlReadBodyShape = switch (dml_kind) {
            .insert_select => .select_body,
            .update, .delete, .merge => .relation_source,
            else => return false,
        };
        return generatedDmlReadBodyIsValid(tokens, end, range, read, shape);
    }
    return source_read == null;
}

fn generatedDmlReadBodyIsValid(
    tokens: []const Token,
    end: usize,
    range: generated_parser.GeneratedSqlTokenRange,
    read_body: generated_parser.GeneratedSqlDmlReadBodyAst,
    shape: GeneratedDmlReadBodyShape,
) bool {
    if (!generatedDmlTokenRangeIsValid(tokens, end, range)) return false;
    if (!std.meta.eql(read_body.tokens, range)) return false;
    if (!std.meta.eql(read_body.statement_span, generatedDmlSourceSpanForTokenRange(tokens, range))) return false;
    if (!std.meta.eql(read_body.command_span, tokens[range.start].sourceSpan())) return false;
    if (!generatedDmlReadBodyKindIsValid(read_body, shape)) return false;

    switch (shape) {
        .select_body => {
            if (!tokens[range.start].matchesKeywordTag(.select)) return false;
            if (read_body.projection_tokens) |projection| {
                if (!generatedDmlTokenRangeIsValid(tokens, end, projection)) return false;
                if (projection.start <= range.start or projection.end > range.end) return false;
            } else return false;
            if (!generatedDmlOptionalNestedRangeIsValid(tokens, end, range, read_body.source_tokens)) return false;
            if (!generatedDmlOptionalNestedRangeIsValid(tokens, end, range, read_body.where_tokens)) return false;
            if (!generatedDmlOptionalNestedRangeIsValid(tokens, end, range, read_body.set_operation_tokens)) return false;
            if (!generatedDmlSelectReadBodyClauseLayoutIsValid(tokens, range, read_body)) return false;
            if (read_body.wrapper_projection_star) return false;
        },
        .relation_source => {
            if (!std.meta.eql(read_body.source_tokens orelse return false, range)) return false;
            if (read_body.projection_tokens != null or read_body.where_tokens != null or read_body.set_operation_tokens != null) return false;
            if (!read_body.wrapper_projection_star) return false;
        },
    }
    return true;
}

fn generatedDmlReadBodyKindIsValid(
    read_body: generated_parser.GeneratedSqlDmlReadBodyAst,
    shape: GeneratedDmlReadBodyShape,
) bool {
    return switch (shape) {
        .select_body => switch (read_body.kind) {
            .query, .aggregate, .join, .lateral, .window => read_body.set_operation_tokens == null,
            .set_operation => read_body.set_operation_tokens != null,
            .cte => false,
        },
        .relation_source => switch (read_body.kind) {
            .query, .join, .lateral => read_body.set_operation_tokens == null,
            .aggregate, .window, .set_operation, .cte => false,
        },
    };
}

fn generatedDmlSelectReadBodyClauseLayoutIsValid(
    tokens: []const Token,
    range: generated_parser.GeneratedSqlTokenRange,
    read_body: generated_parser.GeneratedSqlDmlReadBodyAst,
) bool {
    const projection = read_body.projection_tokens orelse return false;
    if (projection.start <= range.start or projection.end > range.end) return false;

    var previous_end = projection.end;
    if (read_body.source_tokens) |source| {
        if (source.start == 0 or !tokens[source.start - 1].matchesKeywordTag(.from)) return false;
        if (source.start != projection.end + 1) return false;
        if (source.end > range.end) return false;
        previous_end = source.end;
    }

    if (read_body.where_tokens) |where| {
        if (where.start == 0 or !tokens[where.start - 1].matchesKeywordTag(.where)) return false;
        if (where.start <= previous_end) return false;
        if (where.end > range.end) return false;
        previous_end = where.end;
    }

    if (read_body.set_operation_tokens) |set_operation| {
        if (set_operation.start != previous_end or set_operation.end > range.end) return false;
        if (!tokens[set_operation.start].matchesKeywordTag(.@"union") and
            !tokens[set_operation.start].matchesKeywordTag(.intersect) and
            !tokens[set_operation.start].matchesKeywordTag(.except))
        {
            return false;
        }
        previous_end = set_operation.end;
    }

    return generatedDmlSelectReadBodyTailIsValid(tokens, range, previous_end);
}

fn generatedDmlSelectReadBodyTailIsValid(
    tokens: []const Token,
    range: generated_parser.GeneratedSqlTokenRange,
    tail_start: usize,
) bool {
    if (tail_start == range.end) return true;
    if (tail_start > range.end or tail_start >= tokens.len) return false;
    if (tokens[tail_start].matchesKeywordTag(.group)) {
        return tail_start + 1 < range.end and tokens[tail_start + 1].matchesKeywordTag(.by);
    }
    return tokens[tail_start].matchesKeywordTag(.having) or
        tokens[tail_start].matchesKeywordTag(.window) or
        tokens[tail_start].matchesKeywordTag(.order) or
        tokens[tail_start].matchesKeywordTag(.limit) or
        tokens[tail_start].matchesKeywordTag(.offset) or
        tokens[tail_start].matchesKeywordTag(.fetch);
}

fn generatedDmlOptionalNestedRangeIsValid(
    tokens: []const Token,
    end: usize,
    outer: generated_parser.GeneratedSqlTokenRange,
    range: ?generated_parser.GeneratedSqlTokenRange,
) bool {
    if (range) |inner| {
        if (!generatedDmlTokenRangeIsValid(tokens, end, inner)) return false;
        return inner.start >= outer.start and inner.end <= outer.end;
    }
    return true;
}

fn generatedDmlSourceSpanForTokenRange(tokens: []const Token, range: generated_parser.GeneratedSqlTokenRange) SourceSpan {
    return .{
        .start = tokens[range.start].sourceSpan().start,
        .end = tokens[range.end - 1].sourceSpan().end,
    };
}

fn generatedDmlOptionalTokenRangeIsValid(
    tokens: []const Token,
    end: usize,
    range: ?generated_parser.GeneratedSqlTokenRange,
) bool {
    if (range) |value| return generatedDmlTokenRangeIsValid(tokens, end, value);
    return true;
}

fn generatedDmlTokenRangeIsValid(
    tokens: []const Token,
    end: usize,
    range: generated_parser.GeneratedSqlTokenRange,
) bool {
    return end <= tokens.len and range.start < range.end and range.end <= end;
}

fn generatedDmlCommandKeywordMatchesKind(token: Token, kind: generated_parser.GeneratedSqlDmlKind) bool {
    return switch (kind) {
        .insert_values, .insert_select => token.matchesKeywordTag(.insert),
        .update => token.matchesKeywordTag(.update),
        .delete => token.matchesKeywordTag(.delete),
        .truncate => token.matchesKeywordTag(.truncate),
        .merge => token.matchesKeywordTag(.merge),
    };
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
    if (!generatedReadAstHasValidClassificationPayload(tokens, read_ast)) return null;
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

fn generatedReadAstHasValidClassificationPayload(
    tokens: []const Token,
    read_ast: *const generated_parser.GeneratedSqlReadAst,
) bool {
    const end = generatedReadStatementEnd(tokens, read_ast.statement_span) orelse return false;
    const select_index = generatedReadSelectIndex(tokens, end, read_ast) orelse return false;
    if (!std.meta.eql(read_ast.command_span, tokens[select_index].sourceSpan())) return false;
    if (read_ast.projection_tokens) |projection| {
        if (!generatedReadTokenRangeIsValidThrough(tokens, end, projection)) return false;
        if (projection.start <= select_index) return false;
        if (!generatedReadListPayloadIsValid(tokens, end, projection, read_ast.projection_items, .{
            .allow_aliases = true,
            .reject_order_modifiers = true,
            .first_expression = read_ast.projection_first_expression,
            .last_expression = read_ast.projection_last_expression,
        })) return false;
    } else return false;

    if (!generatedReadOptionalTokenRangeIsValidThrough(tokens, end, read_ast.distinct_tokens)) return false;
    if (!generatedReadDistinctPayloadIsValid(tokens, end, read_ast.distinct_tokens, read_ast.distinct_on_items)) return false;
    if (!generatedReadOptionalTokenRangeIsValidThrough(tokens, end, read_ast.source_tokens)) return false;
    if (!generatedReadOptionalRangeIsPrecededByKeyword(tokens, read_ast.source_tokens, .from)) return false;
    if (!generatedReadOptionalTokenRangeIsValidThrough(tokens, end, read_ast.source_graph_function_tokens)) return false;
    if (!generatedReadOptionalTokenRangeIsValidThrough(tokens, end, read_ast.source_graph_function_name_tokens)) return false;
    if (!generatedReadOptionalTokenRangeIsValidThrough(tokens, end, read_ast.source_graph_function_argument_tokens)) return false;
    if (!generatedReadAntflySourceItemsAreValid(tokens, end, read_ast.source_tokens, read_ast.source_antfly_function_items, read_ast.source_antfly_function_count)) return false;
    if (!generatedReadGraphSourceItemsAreValid(tokens, end, read_ast.source_tokens, read_ast.source_antfly_function_items, read_ast.source_graph_function_items, read_ast.source_graph_function_count)) return false;
    if (!generatedReadGraphSourceCompatibilityFieldsAreValid(read_ast)) return false;
    if (!generatedReadOptionalTokenRangeIsValidThrough(tokens, end, read_ast.join_tokens)) return false;
    if (!generatedReadOptionalTokenRangeIsValidThrough(tokens, end, read_ast.join_operator_tokens)) return false;
    if (!generatedReadOptionalTokenRangeIsValidThrough(tokens, end, read_ast.join_left_tokens)) return false;
    if (!generatedReadOptionalTokenRangeIsValidThrough(tokens, end, read_ast.join_right_tokens)) return false;
    if (!generatedReadOptionalTokenRangeIsValidThrough(tokens, end, read_ast.join_predicate_tokens)) return false;
    if (!generatedReadJoinPayloadIsValid(
        tokens,
        end,
        read_ast.source_tokens,
        read_ast.join_tokens,
        read_ast.join_operator_tokens,
        read_ast.join_kind,
        read_ast.join_left_tokens,
        read_ast.join_right_tokens,
        read_ast.join_predicate_tokens,
        read_ast.join_predicate_expression,
        read_ast.join_items,
        read_ast.join_tree_root_index,
        read_ast.join_tree_depth,
    )) return false;
    if (!generatedReadOptionalTokenRangeIsValidThrough(tokens, end, read_ast.where_tokens)) return false;
    if (!generatedReadOptionalRangeIsPrecededByKeyword(tokens, read_ast.where_tokens, .where)) return false;
    if (!generatedReadOptionalExpressionTokensMatchMaybeRange(read_ast.where_expression, read_ast.where_tokens)) return false;
    if (!generatedReadOptionalTokenRangeIsValidThrough(tokens, end, read_ast.group_tokens)) return false;
    if (!generatedReadOptionalRangeIsPrecededByKeywordPair(tokens, read_ast.group_tokens, .group, .by)) return false;
    if (!generatedReadOptionalListPayloadIsValid(tokens, end, read_ast.group_tokens, read_ast.group_items, .{
        .reject_aliases = true,
        .reject_order_modifiers = true,
        .first_expression = read_ast.group_first_expression,
        .last_expression = read_ast.group_last_expression,
    })) return false;
    if (!generatedReadOptionalTokenRangeIsValidThrough(tokens, end, read_ast.having_tokens)) return false;
    if (!generatedReadOptionalRangeIsPrecededByKeyword(tokens, read_ast.having_tokens, .having)) return false;
    if (!generatedReadOptionalExpressionTokensMatchMaybeRange(read_ast.having_expression, read_ast.having_tokens)) return false;
    if (!generatedReadOptionalTokenRangeIsValidThrough(tokens, end, read_ast.window_tokens)) return false;
    if (!generatedReadOptionalRangeIsPrecededByKeyword(tokens, read_ast.window_tokens, .window)) return false;
    if (!generatedReadWindowPayloadIsValid(tokens, end, read_ast.window_tokens, read_ast.window_items, read_ast.window_count)) return false;
    if (!generatedReadOptionalTokenRangeIsValidThrough(tokens, end, read_ast.order_tokens)) return false;
    if (!generatedReadOptionalRangeIsPrecededByKeywordPair(tokens, read_ast.order_tokens, .order, .by)) return false;
    if (!generatedReadOptionalListPayloadIsValid(tokens, end, read_ast.order_tokens, read_ast.order_items, .{
        .reject_aliases = true,
        .allow_order_modifiers = true,
        .first_expression = read_ast.order_first_expression,
        .last_expression = read_ast.order_last_expression,
    })) return false;
    if (!generatedReadOptionalTokenRangeIsValidThrough(tokens, end, read_ast.limit_tokens)) return false;
    if (!generatedReadOptionalRangeIsPrecededByKeyword(tokens, read_ast.limit_tokens, .limit)) return false;
    if (!generatedReadOptionalTokenRangeIsValidThrough(tokens, end, read_ast.offset_tokens)) return false;
    if (!generatedReadOptionalRangeIsPrecededByKeyword(tokens, read_ast.offset_tokens, .offset)) return false;
    if (!generatedReadOptionalTokenRangeIsValidThrough(tokens, end, read_ast.fetch_tokens)) return false;
    if (!generatedReadOptionalRangeIsPrecededByKeyword(tokens, read_ast.fetch_tokens, .fetch)) return false;
    if (!generatedReadOptionalTokenRangeIsValidThrough(tokens, end, read_ast.fetch_count_tokens)) return false;
    if (!generatedReadOptionalTokenRangeIsValidThrough(tokens, end, read_ast.row_lock_tokens)) return false;
    if (!generatedReadRowLockPayloadIsValid(tokens, end, read_ast.row_lock_tokens)) return false;
    if (!generatedReadOptionalTokenRangeIsValidThrough(tokens, end, read_ast.set_operation_tokens)) return false;
    if (!generatedReadSetOperationPayloadIsValid(tokens, end, read_ast.set_operation_tokens, read_ast.set_operation)) return false;
    if (!generatedReadCtePayloadIsValid(tokens, end, select_index, read_ast)) return false;

    if (!generatedReadPaginationPayloadIsValid(
        tokens,
        read_ast.limit_tokens,
        read_ast.limit_expression,
        read_ast.limit_all,
        read_ast.offset_tokens,
        read_ast.offset_expression,
        read_ast.fetch_tokens,
        read_ast.fetch_count_tokens,
        read_ast.fetch_count_expression,
    )) return false;

    for (read_ast.cte_items) |cte| {
        if (!generatedReadPaginationPayloadIsValid(
            tokens,
            cte.body_limit_tokens,
            cte.body_limit_expression,
            cte.body_limit_all,
            cte.body_offset_tokens,
            cte.body_offset_expression,
            cte.body_fetch_tokens,
            cte.body_fetch_count_tokens,
            cte.body_fetch_count_expression,
        )) return false;
    }
    return true;
}

fn generatedReadStatementEnd(tokens: []const Token, statement_span: SourceSpan) ?usize {
    if (tokens.len == 0) return null;
    var end = tokens.len;
    while (end > 0 and tokens[end - 1].kind == .semicolon) {
        end -= 1;
    }
    if (end == 0) return null;
    if (tokens[0].sourceSpan().start != statement_span.start) return null;
    if (tokens[end - 1].sourceSpan().end != statement_span.end) return null;
    return end;
}

fn generatedReadSelectIndex(
    tokens: []const Token,
    end: usize,
    read_ast: *const generated_parser.GeneratedSqlReadAst,
) ?usize {
    if (end == 0 or end > tokens.len) return null;
    if (read_ast.cte_tokens) |cte_tokens| {
        if (!generatedReadTokenRangeIsValidThrough(tokens, end, cte_tokens)) return null;
        if (cte_tokens.start != 1 or cte_tokens.end >= end) return null;
        if (!tokens[0].matchesKeywordTag(.with)) return null;
        const recursive = cte_tokens.start < cte_tokens.end and tokens[cte_tokens.start].matchesKeywordTag(.recursive);
        if (recursive != read_ast.cte_recursive) return null;
        return if (tokens[cte_tokens.end].matchesKeywordTag(.select)) cte_tokens.end else null;
    }
    if (read_ast.cte_recursive or read_ast.cte_items.len != 0 or read_ast.cte_count != 0) return null;
    if (tokens[0].matchesKeywordTag(.with)) return null;
    return if (tokens[0].matchesKeywordTag(.select)) 0 else null;
}

fn generatedReadCtePayloadIsValid(
    tokens: []const Token,
    end: usize,
    select_index: usize,
    read_ast: *const generated_parser.GeneratedSqlReadAst,
) bool {
    if (read_ast.cte_tokens == null) {
        return read_ast.cte_list_tokens == null and
            read_ast.cte_name_tokens == null and
            read_ast.cte_body_tokens == null and
            read_ast.cte_last_name_tokens == null and
            read_ast.cte_last_body_tokens == null and
            read_ast.cte_items.len == 0 and
            read_ast.cte_count == 0 and
            !read_ast.cte_recursive;
    }

    const cte_tokens = read_ast.cte_tokens.?;
    if (cte_tokens.end != select_index) return false;
    const list_tokens = read_ast.cte_list_tokens orelse return false;
    if (!generatedReadTokenRangeIsValidThrough(tokens, end, list_tokens)) return false;
    if (list_tokens.start < cte_tokens.start or list_tokens.end != cte_tokens.end) return false;
    if (read_ast.cte_count == 0 or read_ast.cte_count != read_ast.cte_items.len) return false;
    if (read_ast.cte_name_tokens == null or read_ast.cte_body_tokens == null or read_ast.cte_last_name_tokens == null or read_ast.cte_last_body_tokens == null) return false;
    if (!std.meta.eql(read_ast.cte_items[0].name_tokens, read_ast.cte_name_tokens.?)) return false;
    if (!std.meta.eql(read_ast.cte_items[read_ast.cte_items.len - 1].name_tokens, read_ast.cte_last_name_tokens.?)) return false;
    if (!std.meta.eql(read_ast.cte_items[0].body_tokens orelse return false, read_ast.cte_body_tokens.?)) return false;
    if (!std.meta.eql(read_ast.cte_items[read_ast.cte_items.len - 1].body_tokens orelse return false, read_ast.cte_last_body_tokens.?)) return false;

    var expected_start = list_tokens.start;
    for (read_ast.cte_items, 0..) |cte, index| {
        if (!generatedReadTokenRangeIsValidThrough(tokens, end, cte.name_tokens)) return false;
        if (!generatedReadCteItemLayoutIsValid(tokens, end, list_tokens, cte, expected_start)) return false;
        if (!generatedReadOptionalNestedRangeIsValid(tokens, end, list_tokens, cte.column_tokens)) return false;
        if (!generatedReadOptionalNestedRangeIsValid(tokens, end, list_tokens, cte.column_name_tokens)) return false;
        if (!generatedReadOptionalNestedRangeIsValid(tokens, end, list_tokens, cte.materialization_tokens)) return false;
        const body = cte.body_tokens orelse return false;
        if (!generatedReadTokenRangeIsValidThrough(tokens, end, body)) return false;
        if (body.start == 0 or body.end >= select_index) return false;
        if (tokens[body.start - 1].kind != .lparen or tokens[body.end].kind != .rparen) return false;
        if (cte.body_kind == null) return false;
        if (!std.meta.eql(cte.body_select_tokens orelse return false, generated_parser.GeneratedSqlTokenRange{ .start = body.start, .end = body.start + 1 })) return false;
        if (!tokens[body.start].matchesKeywordTag(.select)) return false;
        if (!generatedReadOptionalNestedRangeIsValid(tokens, end, body, cte.body_distinct_tokens)) return false;
        if (!generatedReadDistinctPayloadIsValid(tokens, end, cte.body_distinct_tokens, cte.body_distinct_on_items)) return false;
        if (!generatedReadOptionalNestedRangeIsValid(tokens, end, body, cte.body_projection_tokens)) return false;
        if (!generatedReadOptionalListPayloadIsValid(tokens, end, cte.body_projection_tokens, cte.body_projection_items, .{
            .allow_aliases = true,
            .reject_order_modifiers = true,
            .first_expression = cte.body_projection_first_expression,
            .last_expression = cte.body_projection_last_expression,
        })) return false;
        if (!generatedReadOptionalNestedRangeIsValid(tokens, end, body, cte.body_source_tokens)) return false;
        if (!generatedReadOptionalRangeIsPrecededByKeyword(tokens, cte.body_source_tokens, .from)) return false;
        if (!generatedReadOptionalNestedRangeIsValid(tokens, end, body, cte.body_join_tokens)) return false;
        if (!generatedReadOptionalNestedRangeIsValid(tokens, end, body, cte.body_join_operator_tokens)) return false;
        if (!generatedReadOptionalNestedRangeIsValid(tokens, end, body, cte.body_join_left_tokens)) return false;
        if (!generatedReadOptionalNestedRangeIsValid(tokens, end, body, cte.body_join_right_tokens)) return false;
        if (!generatedReadOptionalNestedRangeIsValid(tokens, end, body, cte.body_join_predicate_tokens)) return false;
        if (!generatedReadJoinPayloadIsValid(
            tokens,
            end,
            cte.body_source_tokens,
            cte.body_join_tokens,
            cte.body_join_operator_tokens,
            cte.body_join_kind,
            cte.body_join_left_tokens,
            cte.body_join_right_tokens,
            cte.body_join_predicate_tokens,
            cte.body_join_predicate_expression,
            cte.body_join_items,
            cte.body_join_tree_root_index,
            cte.body_join_tree_depth,
        )) return false;
        if (!generatedReadOptionalNestedRangeIsValid(tokens, end, body, cte.body_where_tokens)) return false;
        if (!generatedReadOptionalRangeIsPrecededByKeyword(tokens, cte.body_where_tokens, .where)) return false;
        if (!generatedReadOptionalExpressionTokensMatchMaybeRange(cte.body_where_expression, cte.body_where_tokens)) return false;
        if (!generatedReadOptionalNestedRangeIsValid(tokens, end, body, cte.body_group_tokens)) return false;
        if (!generatedReadOptionalRangeIsPrecededByKeywordPair(tokens, cte.body_group_tokens, .group, .by)) return false;
        if (!generatedReadOptionalListPayloadIsValid(tokens, end, cte.body_group_tokens, cte.body_group_items, .{
            .reject_aliases = true,
            .reject_order_modifiers = true,
            .first_expression = cte.body_group_first_expression,
            .last_expression = cte.body_group_last_expression,
        })) return false;
        if (!generatedReadOptionalNestedRangeIsValid(tokens, end, body, cte.body_having_tokens)) return false;
        if (!generatedReadOptionalRangeIsPrecededByKeyword(tokens, cte.body_having_tokens, .having)) return false;
        if (!generatedReadOptionalExpressionTokensMatchMaybeRange(cte.body_having_expression, cte.body_having_tokens)) return false;
        if (!generatedReadOptionalNestedRangeIsValid(tokens, end, body, cte.body_window_tokens)) return false;
        if (!generatedReadOptionalRangeIsPrecededByKeyword(tokens, cte.body_window_tokens, .window)) return false;
        if (!generatedReadWindowPayloadIsValid(tokens, end, cte.body_window_tokens, cte.body_window_items, cte.body_window_count)) return false;
        if (!generatedReadOptionalNestedRangeIsValid(tokens, end, body, cte.body_order_tokens)) return false;
        if (!generatedReadOptionalRangeIsPrecededByKeywordPair(tokens, cte.body_order_tokens, .order, .by)) return false;
        if (!generatedReadOptionalListPayloadIsValid(tokens, end, cte.body_order_tokens, cte.body_order_items, .{
            .reject_aliases = true,
            .allow_order_modifiers = true,
            .first_expression = cte.body_order_first_expression,
            .last_expression = cte.body_order_last_expression,
        })) return false;
        if (!generatedReadOptionalNestedRangeIsValid(tokens, end, body, cte.body_limit_tokens)) return false;
        if (!generatedReadOptionalRangeIsPrecededByKeyword(tokens, cte.body_limit_tokens, .limit)) return false;
        if (!generatedReadOptionalNestedRangeIsValid(tokens, end, body, cte.body_offset_tokens)) return false;
        if (!generatedReadOptionalRangeIsPrecededByKeyword(tokens, cte.body_offset_tokens, .offset)) return false;
        if (!generatedReadOptionalNestedRangeIsValid(tokens, end, body, cte.body_fetch_tokens)) return false;
        if (!generatedReadOptionalRangeIsPrecededByKeyword(tokens, cte.body_fetch_tokens, .fetch)) return false;
        if (!generatedReadOptionalNestedRangeIsValid(tokens, end, body, cte.body_fetch_count_tokens)) return false;
        if (!generatedReadOptionalNestedRangeIsValid(tokens, end, body, cte.body_row_lock_tokens)) return false;
        if (!generatedReadRowLockPayloadIsValid(tokens, end, cte.body_row_lock_tokens)) return false;
        if (!generatedReadOptionalNestedRangeIsValid(tokens, end, body, cte.body_set_operation_tokens)) return false;
        if (!generatedReadSetOperationPayloadIsValid(tokens, end, cte.body_set_operation_tokens, cte.body_set_operation)) return false;
        if (!generatedReadAntflySourceItemsAreValid(tokens, end, cte.body_source_tokens, cte.body_source_antfly_function_items, cte.body_source_antfly_function_count)) return false;
        if (!generatedReadGraphSourceItemsAreValid(tokens, end, cte.body_source_tokens, cte.body_source_antfly_function_items, cte.body_source_graph_function_items, cte.body_source_graph_function_count)) return false;
        if (cte.body_projection_tokens == null) return false;
        expected_start = body.end + 1;
        if (index + 1 < read_ast.cte_items.len) {
            if (expected_start >= list_tokens.end or tokens[expected_start].kind != .comma) return false;
            expected_start += 1;
        }
    }
    return expected_start == list_tokens.end;
}

fn generatedReadCteItemLayoutIsValid(
    tokens: []const Token,
    end: usize,
    list_tokens: generated_parser.GeneratedSqlTokenRange,
    cte: generated_parser.GeneratedSqlCteAst,
    expected_start: usize,
) bool {
    const body = cte.body_tokens orelse return false;
    if (!generatedReadTokenRangeIsValidThrough(tokens, end, cte.name_tokens)) return false;
    if (!generatedReadTokenRangeIsValidThrough(tokens, end, body)) return false;
    if (cte.name_tokens.start != expected_start or cte.name_tokens.start >= cte.name_tokens.end) return false;
    if (cte.name_tokens.start < list_tokens.start or body.end > list_tokens.end) return false;

    var cursor = cte.name_tokens.end;
    if (cte.column_tokens) |column_tokens| {
        if (!generatedReadNestedRangeIsValid(tokens, end, list_tokens, column_tokens)) return false;
        if (column_tokens.start != cursor or column_tokens.end <= column_tokens.start + 1 or column_tokens.end > body.start) return false;
        if (tokens[column_tokens.start].kind != .lparen or tokens[column_tokens.end - 1].kind != .rparen) return false;
        const column_name_tokens = cte.column_name_tokens orelse return false;
        if (!generatedReadNestedRangeIsValid(tokens, end, column_tokens, column_name_tokens)) return false;
        if (cte.column_names.count == 0) return false;
        if (column_name_tokens.start != column_tokens.start + 1 or column_name_tokens.end != column_tokens.end - 1) return false;
        if (!generatedReadDelimitedListIsValid(tokens, end, column_name_tokens, cte.column_names, .{
            .single_token_items = true,
            .reject_aliases = true,
            .reject_order_modifiers = true,
        })) return false;
        cursor = column_tokens.end;
    } else if (cte.column_name_tokens != null or cte.column_names.count != 0) {
        return false;
    }

    if (cursor >= list_tokens.end or !tokens[cursor].matchesKeywordTag(.as)) return false;
    cursor += 1;

    if (cte.materialization_tokens) |materialization_tokens| {
        if (!generatedReadNestedRangeIsValid(tokens, end, list_tokens, materialization_tokens)) return false;
        if (materialization_tokens.start != cursor or materialization_tokens.end > body.start) return false;
        switch (cte.materialization orelse return false) {
            .materialized => {
                if (materialization_tokens.end != materialization_tokens.start + 1) return false;
                if (!tokens[materialization_tokens.start].matchesKeywordTag(.materialized)) return false;
            },
            .not_materialized => {
                if (materialization_tokens.end != materialization_tokens.start + 2) return false;
                if (!tokens[materialization_tokens.start].matchesKeywordTag(.not) or
                    !tokens[materialization_tokens.start + 1].matchesKeywordTag(.materialized))
                {
                    return false;
                }
            },
        }
        cursor = materialization_tokens.end;
    } else if (cte.materialization != null) {
        return false;
    }

    return body.start == cursor + 1 and
        body.end < list_tokens.end and
        tokens[cursor].kind == .lparen and
        tokens[body.end].kind == .rparen;
}

fn generatedReadJoinPayloadIsValid(
    tokens: []const Token,
    end: usize,
    maybe_source_tokens: ?generated_parser.GeneratedSqlTokenRange,
    maybe_join_tokens: ?generated_parser.GeneratedSqlTokenRange,
    maybe_operator_tokens: ?generated_parser.GeneratedSqlTokenRange,
    maybe_kind: ?generated_parser.GeneratedSqlJoinKind,
    maybe_left_tokens: ?generated_parser.GeneratedSqlTokenRange,
    maybe_right_tokens: ?generated_parser.GeneratedSqlTokenRange,
    maybe_predicate_tokens: ?generated_parser.GeneratedSqlTokenRange,
    predicate_expression: generated_parser.GeneratedSqlExpressionAst,
    join_items: []const generated_parser.GeneratedSqlJoinAst,
    maybe_root_index: ?usize,
    tree_depth: usize,
) bool {
    if (join_items.len == 0) {
        return maybe_join_tokens == null and
            maybe_operator_tokens == null and
            maybe_kind == null and
            maybe_left_tokens == null and
            maybe_right_tokens == null and
            maybe_predicate_tokens == null and
            predicate_expression.tokens == null and
            maybe_root_index == null and
            tree_depth == 0;
    }

    const source_tokens = maybe_source_tokens orelse return false;
    if (!generatedReadTokenRangeIsValidThrough(tokens, end, source_tokens)) return false;
    const join_tokens = maybe_join_tokens orelse return false;
    if (!std.meta.eql(join_tokens, source_tokens)) return false;
    if (maybe_root_index == null or maybe_root_index.? != join_items.len - 1) return false;
    if (tree_depth != join_items.len) return false;

    const first = join_items[0];
    if (!std.meta.eql(maybe_operator_tokens orelse return false, first.operator_tokens)) return false;
    if ((maybe_kind orelse return false) != first.kind) return false;
    if (!std.meta.eql(maybe_left_tokens orelse return false, first.left_tokens)) return false;
    if (!std.meta.eql(maybe_right_tokens orelse return false, first.right_tokens)) return false;
    if (!generatedReadOptionalExpressionTokensMatchMaybeRange(predicate_expression, maybe_predicate_tokens)) return false;
    if (!generatedReadOptionalRangesEqual(maybe_predicate_tokens, first.predicate_tokens)) return false;

    var index: usize = 0;
    while (index < join_items.len) : (index += 1) {
        const item = join_items[index];
        if (!generatedReadJoinItemIsValid(tokens, end, source_tokens, join_items, index, item)) return false;
    }
    return true;
}

fn generatedReadJoinItemIsValid(
    tokens: []const Token,
    end: usize,
    source_tokens: generated_parser.GeneratedSqlTokenRange,
    join_items: []const generated_parser.GeneratedSqlJoinAst,
    index: usize,
    item: generated_parser.GeneratedSqlJoinAst,
) bool {
    if (!generatedReadNestedRangeIsValid(tokens, end, source_tokens, item.tokens)) return false;
    if (!generatedReadNestedRangeIsValid(tokens, end, item.tokens, item.operator_tokens)) return false;
    if (!generatedReadNestedRangeIsValid(tokens, end, item.tokens, item.left_tokens)) return false;
    if (!generatedReadNestedRangeIsValid(tokens, end, item.tokens, item.right_tokens)) return false;
    if (item.tree_index != index or item.tree_depth != index + 1) return false;
    if (index == 0) {
        if (item.left_child_index != null) return false;
        if (item.left_tokens.start != source_tokens.start) return false;
    } else {
        if ((item.left_child_index orelse return false) != index - 1) return false;
        if (!std.meta.eql(item.left_tokens, join_items[index - 1].tokens)) return false;
    }
    if (item.tokens.start != source_tokens.start) return false;
    if (item.left_tokens.end != item.operator_tokens.start) return false;
    if (item.operator_tokens.end != item.right_tokens.start) return false;
    if (item.right_tokens.end > item.tokens.end) return false;
    if (!generatedReadJoinKindMatchesOperator(tokens, item.operator_tokens, item.kind)) return false;

    return switch (item.condition_kind) {
        .none => item.condition_tokens.start == item.right_tokens.end and
            item.condition_tokens.end == item.right_tokens.end and
            (item.kind == .cross or item.kind == .natural) and
            item.predicate_tokens == null and
            item.predicate_expression.tokens == null and
            item.using_tokens == null and
            item.using_column_tokens == null and
            item.using_columns.count == 0,
        .on => blk: {
            if (!generatedReadNestedRangeIsValid(tokens, end, item.tokens, item.condition_tokens)) break :blk false;
            if (item.condition_tokens.start != item.right_tokens.end or item.condition_tokens.end != item.tokens.end) break :blk false;
            if (item.condition_tokens.start >= tokens.len or !tokens[item.condition_tokens.start].matchesKeywordTag(.on)) break :blk false;
            const predicate_tokens = item.predicate_tokens orelse break :blk false;
            if (!generatedReadNestedRangeIsValid(tokens, end, item.condition_tokens, predicate_tokens)) break :blk false;
            if (predicate_tokens.start != item.condition_tokens.start + 1 or predicate_tokens.end != item.condition_tokens.end) break :blk false;
            if (!generatedReadExpressionTokensEqualRange(item.predicate_expression, predicate_tokens)) break :blk false;
            if (item.using_tokens != null or item.using_column_tokens != null or item.using_columns.count != 0) break :blk false;
            break :blk true;
        },
        .using => blk: {
            if (!generatedReadNestedRangeIsValid(tokens, end, item.tokens, item.condition_tokens)) break :blk false;
            if (item.condition_tokens.start != item.right_tokens.end or item.condition_tokens.end != item.tokens.end) break :blk false;
            if (item.condition_tokens.start >= tokens.len or !tokens[item.condition_tokens.start].matchesKeywordTag(.using)) break :blk false;
            if (!std.meta.eql(item.using_tokens orelse break :blk false, item.condition_tokens)) break :blk false;
            const column_tokens = item.using_column_tokens orelse break :blk false;
            if (!generatedReadNestedRangeIsValid(tokens, end, item.condition_tokens, column_tokens)) break :blk false;
            if (!generatedReadJoinUsingColumnListIsValid(tokens, item.condition_tokens, column_tokens, item.using_columns)) break :blk false;
            if (item.predicate_tokens != null or item.predicate_expression.tokens != null) break :blk false;
            break :blk true;
        },
    };
}

fn generatedReadJoinUsingColumnListIsValid(
    tokens: []const Token,
    condition_tokens: generated_parser.GeneratedSqlTokenRange,
    column_tokens: generated_parser.GeneratedSqlTokenRange,
    columns: generated_parser.GeneratedSqlListAst,
) bool {
    if (condition_tokens.end > tokens.len or condition_tokens.start + 4 > condition_tokens.end) return false;
    if (!tokens[condition_tokens.start].matchesKeywordTag(.using)) return false;
    if (tokens[condition_tokens.start + 1].kind != .lparen or tokens[condition_tokens.end - 1].kind != .rparen) return false;
    if (column_tokens.start != condition_tokens.start + 2 or column_tokens.end != condition_tokens.end - 1) return false;
    return generatedReadDelimitedListIsValid(tokens, tokens.len, column_tokens, columns, .{
        .single_token_items = true,
        .reject_aliases = true,
        .reject_order_modifiers = true,
    });
}

fn generatedReadJoinKindMatchesOperator(
    tokens: []const Token,
    operator_tokens: generated_parser.GeneratedSqlTokenRange,
    kind: generated_parser.GeneratedSqlJoinKind,
) bool {
    if (operator_tokens.start >= operator_tokens.end or operator_tokens.end > tokens.len) return false;
    switch (kind) {
        .inner => {
            if (operator_tokens.end == operator_tokens.start + 1) {
                return tokens[operator_tokens.start].matchesKeywordTag(.join);
            }
            if (operator_tokens.end == operator_tokens.start + 2) {
                return tokens[operator_tokens.start].matchesKeywordTag(.inner) and
                    tokens[operator_tokens.start + 1].matchesKeywordTag(.join);
            }
            return false;
        },
        .left, .right, .full => {
            const expected_keyword: TokenKeyword = switch (kind) {
                .left => .left,
                .right => .right,
                .full => .full,
                else => unreachable,
            };
            if (!tokens[operator_tokens.start].matchesKeywordTag(expected_keyword)) return false;
            if (operator_tokens.end == operator_tokens.start + 2) {
                return tokens[operator_tokens.start + 1].matchesKeywordTag(.join);
            }
            if (operator_tokens.end == operator_tokens.start + 3) {
                return tokens[operator_tokens.start + 1].matchesKeywordTag(.outer) and
                    tokens[operator_tokens.start + 2].matchesKeywordTag(.join);
            }
            return false;
        },
        .cross, .natural => {
            const expected_keyword: TokenKeyword = switch (kind) {
                .cross => .cross,
                .natural => .natural,
                else => unreachable,
            };
            return operator_tokens.end == operator_tokens.start + 2 and
                tokens[operator_tokens.start].matchesKeywordTag(expected_keyword) and
                tokens[operator_tokens.start + 1].matchesKeywordTag(.join);
        },
    }
}

fn generatedReadSetOperationPayloadIsValid(
    tokens: []const Token,
    end: usize,
    maybe_range: ?generated_parser.GeneratedSqlTokenRange,
    set_operation: generated_parser.GeneratedSqlSetOperationAst,
) bool {
    const range = maybe_range orelse return generatedReadSetOperationPayloadIsEmpty(set_operation);
    if (!std.meta.eql(set_operation.tokens orelse return false, range)) return false;
    if (!generatedReadTokenRangeIsValidThrough(tokens, end, range)) return false;

    const operator_tokens = set_operation.operator_tokens orelse return false;
    if (!std.meta.eql(operator_tokens, generated_parser.GeneratedSqlTokenRange{ .start = range.start, .end = range.start + 1 })) return false;
    const kind = set_operation.kind orelse return false;
    switch (kind) {
        .@"union" => if (!tokens[range.start].matchesKeywordTag(.@"union")) return false,
        .intersect => if (!tokens[range.start].matchesKeywordTag(.intersect)) return false,
        .except => if (!tokens[range.start].matchesKeywordTag(.except)) return false,
    }

    var right_start = operator_tokens.end;
    if (set_operation.all_tokens) |all_tokens| {
        if (!std.meta.eql(all_tokens, generated_parser.GeneratedSqlTokenRange{ .start = right_start, .end = right_start + 1 })) return false;
        if (!tokens[all_tokens.start].matchesKeywordTag(.all)) return false;
        right_start = all_tokens.end;
    }

    const right_query = set_operation.right_query_tokens orelse return false;
    if (!std.meta.eql(right_query, generated_parser.GeneratedSqlTokenRange{ .start = right_start, .end = range.end })) return false;
    if (!generatedReadNestedRangeIsValid(tokens, end, range, right_query)) return false;

    const right_select = set_operation.right_select_tokens orelse return false;
    if (!std.meta.eql(right_select, generated_parser.GeneratedSqlTokenRange{ .start = right_query.start, .end = right_query.start + 1 })) return false;
    if (!tokens[right_select.start].matchesKeywordTag(.select)) return false;

    if (!generatedReadOptionalNestedRangeIsValid(tokens, end, right_query, set_operation.right_distinct_tokens)) return false;
    if (!generatedReadDistinctPayloadIsValid(tokens, end, set_operation.right_distinct_tokens, set_operation.right_distinct_on_items)) return false;

    const right_projection = set_operation.right_projection_tokens orelse return false;
    if (!generatedReadNestedRangeIsValid(tokens, end, right_query, right_projection)) return false;
    if (set_operation.right_distinct_tokens) |distinct_tokens| {
        if (distinct_tokens.start != right_select.end or right_projection.start != distinct_tokens.end) return false;
    } else if (right_projection.start != right_select.end) {
        return false;
    }
    if (!generatedReadListPayloadIsValid(tokens, end, right_projection, set_operation.right_projection_items, .{
        .allow_aliases = true,
        .reject_order_modifiers = true,
        .first_expression = set_operation.right_projection_first_expression,
        .last_expression = set_operation.right_projection_last_expression,
    })) return false;

    if (!generatedReadOptionalNestedRangeIsValid(tokens, end, right_query, set_operation.right_source_tokens)) return false;
    if (!generatedReadOptionalRangeIsPrecededByKeyword(tokens, set_operation.right_source_tokens, .from)) return false;
    if (!generatedReadOptionalNestedRangeIsValid(tokens, end, right_query, set_operation.right_where_tokens)) return false;
    if (!generatedReadOptionalRangeIsPrecededByKeyword(tokens, set_operation.right_where_tokens, .where)) return false;
    if (!generatedReadOptionalExpressionTokensMatchMaybeRange(set_operation.right_where_expression, set_operation.right_where_tokens)) return false;
    return true;
}

fn generatedReadSetOperationPayloadIsEmpty(set_operation: generated_parser.GeneratedSqlSetOperationAst) bool {
    return set_operation.tokens == null and
        set_operation.operator_tokens == null and
        set_operation.kind == null and
        set_operation.all_tokens == null and
        set_operation.right_query_tokens == null and
        set_operation.right_select_tokens == null and
        set_operation.right_distinct_tokens == null and
        generatedReadListPayloadIsEmpty(set_operation.right_distinct_on_items, .{}, .{}) and
        set_operation.right_projection_tokens == null and
        generatedReadListPayloadIsEmpty(set_operation.right_projection_items, set_operation.right_projection_first_expression, set_operation.right_projection_last_expression) and
        set_operation.right_source_tokens == null and
        set_operation.right_where_tokens == null and
        set_operation.right_where_expression.tokens == null;
}

fn generatedReadRowLockPayloadIsValid(
    tokens: []const Token,
    end: usize,
    maybe_range: ?generated_parser.GeneratedSqlTokenRange,
) bool {
    const range = maybe_range orelse return true;
    if (!generatedReadTokenRangeIsValidThrough(tokens, end, range)) return false;
    if (!tokens[range.start].matchesKeywordTag(.@"for")) return false;

    var cursor = range.start + 1;
    if (cursor >= range.end) return false;
    if (tokens[cursor].matchesKeywordTag(.update) or tokens[cursor].matchesKeywordTag(.share)) {
        cursor += 1;
    } else if (cursor + 1 < range.end and
        tokens[cursor].matchesKeywordTag(.key) and
        tokens[cursor + 1].matchesKeywordTag(.share))
    {
        cursor += 2;
    } else if (cursor + 2 < range.end and
        tokens[cursor].matchesKeywordTag(.no) and
        tokens[cursor + 1].matchesKeywordTag(.key) and
        tokens[cursor + 2].matchesKeywordTag(.update))
    {
        cursor += 3;
    } else {
        return false;
    }

    var wait_start = range.end;
    if (range.end > cursor and tokens[range.end - 1].matchesKeywordTag(.nowait)) {
        wait_start = range.end - 1;
    } else if (range.end > cursor + 1 and
        tokens[range.end - 2].matchesKeywordTag(.skip) and
        tokens[range.end - 1].matchesKeywordTag(.locked))
    {
        wait_start = range.end - 2;
    } else if (range.end > cursor and tokens[range.end - 1].matchesKeywordTag(.skip)) {
        return false;
    }

    if (cursor < wait_start) {
        if (!tokens[cursor].matchesKeywordTag(.of)) return false;
        cursor += 1;
        if (!generatedReadRowLockTargetListIsValid(tokens, cursor, wait_start)) return false;
        cursor = wait_start;
    }

    if (cursor == range.end) return true;
    if (cursor + 1 == range.end and tokens[cursor].matchesKeywordTag(.nowait)) return true;
    return cursor + 2 == range.end and
        tokens[cursor].matchesKeywordTag(.skip) and
        tokens[cursor + 1].matchesKeywordTag(.locked);
}

fn generatedReadRowLockTargetListIsValid(tokens: []const Token, start: usize, end: usize) bool {
    if (start >= end or end > tokens.len) return false;
    var expect_target = true;
    var index = start;
    while (index < end) : (index += 1) {
        if (tokens[index].kind == .comma) {
            if (expect_target) return false;
            expect_target = true;
        } else {
            expect_target = false;
        }
    }
    return !expect_target;
}

fn generatedReadWindowPayloadIsValid(
    tokens: []const Token,
    end: usize,
    maybe_range: ?generated_parser.GeneratedSqlTokenRange,
    items: []const generated_parser.GeneratedSqlWindowAst,
    count: usize,
) bool {
    const range = maybe_range orelse return items.len == 0 and count == 0;
    if (!generatedReadTokenRangeIsValidThrough(tokens, end, range)) return false;
    if (count == 0 or items.len != count) return false;

    var expected_start = range.start;
    for (items, 0..) |item, index| {
        if (!generatedReadNestedRangeIsValid(tokens, end, range, item.tokens)) return false;
        if (item.tokens.start != expected_start or item.tokens.start >= item.tokens.end) return false;
        if (!generatedReadWindowItemIsValid(tokens, end, item)) return false;
        expected_start = item.tokens.end;
        if (index + 1 < items.len) {
            if (expected_start >= range.end or tokens[expected_start].kind != .comma) return false;
            expected_start += 1;
        }
    }
    return expected_start == range.end;
}

fn generatedReadWindowItemIsValid(
    tokens: []const Token,
    end: usize,
    item: generated_parser.GeneratedSqlWindowAst,
) bool {
    if (!generatedReadNestedRangeIsValid(tokens, end, item.tokens, item.name_tokens)) return false;
    if (!generatedReadNestedRangeIsValid(tokens, end, item.tokens, item.definition_tokens)) return false;
    if (item.name_tokens.start != item.tokens.start or item.name_tokens.start >= item.name_tokens.end) return false;
    const as_index = item.name_tokens.end;
    if (as_index + 1 >= item.tokens.end or !tokens[as_index].matchesKeywordTag(.as) or tokens[as_index + 1].kind != .lparen) return false;
    if (tokens[item.tokens.end - 1].kind != .rparen) return false;
    if (!std.meta.eql(item.definition_tokens, generated_parser.GeneratedSqlTokenRange{ .start = as_index + 2, .end = item.tokens.end - 1 })) return false;

    if (!generatedReadOptionalWindowListPayloadIsValid(tokens, end, item.definition_tokens, item.partition_tokens, item.partition_items, .partition)) return false;
    if (!generatedReadOptionalWindowListPayloadIsValid(tokens, end, item.definition_tokens, item.order_tokens, item.order_items, .order)) return false;
    if (!generatedReadWindowFramePayloadIsValid(tokens, end, item.definition_tokens, item.frame_tokens, item.frame_start_expression_tokens, item.frame_start_expression_kind, item.frame_start_expression, item.frame_end_expression_tokens, item.frame_end_expression_kind, item.frame_end_expression)) return false;
    return true;
}

const GeneratedReadWindowListKind = enum {
    partition,
    order,
};

fn generatedReadOptionalWindowListPayloadIsValid(
    tokens: []const Token,
    end: usize,
    definition_tokens: generated_parser.GeneratedSqlTokenRange,
    maybe_range: ?generated_parser.GeneratedSqlTokenRange,
    list: generated_parser.GeneratedSqlListAst,
    kind: GeneratedReadWindowListKind,
) bool {
    const range = maybe_range orelse return generatedReadListPayloadIsEmpty(list, .{}, .{});
    if (!generatedReadNestedRangeIsValid(tokens, end, definition_tokens, range)) return false;
    if (range.start < definition_tokens.start + 2) return false;
    switch (kind) {
        .partition => {
            if (!tokens[range.start - 2].matchesKeywordTag(.partition) or !tokens[range.start - 1].matchesKeywordTag(.by)) return false;
            return generatedReadDelimitedListIsValid(tokens, end, range, list, .{
                .reject_aliases = true,
                .reject_order_modifiers = true,
            });
        },
        .order => {
            if (!tokens[range.start - 2].matchesKeywordTag(.order) or !tokens[range.start - 1].matchesKeywordTag(.by)) return false;
            return generatedReadDelimitedListIsValid(tokens, end, range, list, .{
                .reject_aliases = true,
                .allow_order_modifiers = true,
            });
        },
    }
}

fn generatedReadWindowFramePayloadIsValid(
    tokens: []const Token,
    end: usize,
    definition_tokens: generated_parser.GeneratedSqlTokenRange,
    maybe_frame_tokens: ?generated_parser.GeneratedSqlTokenRange,
    maybe_start_tokens: ?generated_parser.GeneratedSqlTokenRange,
    start_kind: ?generated_parser.GeneratedSqlExpressionKind,
    maybe_start_expression: ?*generated_parser.GeneratedSqlExpressionAst,
    maybe_end_tokens: ?generated_parser.GeneratedSqlTokenRange,
    end_kind: ?generated_parser.GeneratedSqlExpressionKind,
    maybe_end_expression: ?*generated_parser.GeneratedSqlExpressionAst,
) bool {
    const frame_tokens = maybe_frame_tokens orelse return maybe_start_tokens == null and
        start_kind == null and
        maybe_start_expression == null and
        maybe_end_tokens == null and
        end_kind == null and
        maybe_end_expression == null;

    if (!generatedReadNestedRangeIsValid(tokens, end, definition_tokens, frame_tokens)) return false;
    if (frame_tokens.start >= frame_tokens.end) return false;
    if (!tokens[frame_tokens.start].matchesKeywordTag(.rows) and !tokens[frame_tokens.start].matchesKeywordTag(.range)) return false;
    if (!generatedReadWindowFrameExpressionPayloadIsValid(tokens, end, frame_tokens, maybe_start_tokens, start_kind, maybe_start_expression)) return false;
    if (!generatedReadWindowFrameExpressionPayloadIsValid(tokens, end, frame_tokens, maybe_end_tokens, end_kind, maybe_end_expression)) return false;
    return true;
}

fn generatedReadWindowFrameExpressionPayloadIsValid(
    tokens: []const Token,
    end: usize,
    frame_tokens: generated_parser.GeneratedSqlTokenRange,
    maybe_expression_tokens: ?generated_parser.GeneratedSqlTokenRange,
    maybe_kind: ?generated_parser.GeneratedSqlExpressionKind,
    maybe_expression: ?*generated_parser.GeneratedSqlExpressionAst,
) bool {
    const expression_tokens = maybe_expression_tokens orelse return maybe_kind == null and maybe_expression == null;
    if (!generatedReadNestedRangeIsValid(tokens, end, frame_tokens, expression_tokens)) return false;
    const expression = maybe_expression orelse return false;
    if (!generatedReadExpressionTokensEqualRange(expression.*, expression_tokens)) return false;
    if (maybe_kind) |kind| {
        if (expression.kind != kind) return false;
    }
    return true;
}

const GeneratedReadDelimitedListValidationOptions = struct {
    single_token_items: bool = false,
    allow_aliases: bool = false,
    reject_aliases: bool = false,
    allow_order_modifiers: bool = false,
    reject_order_modifiers: bool = false,
    first_expression: generated_parser.GeneratedSqlExpressionAst = .{},
    last_expression: generated_parser.GeneratedSqlExpressionAst = .{},
};

fn generatedReadOptionalListPayloadIsValid(
    tokens: []const Token,
    end: usize,
    maybe_range: ?generated_parser.GeneratedSqlTokenRange,
    list: generated_parser.GeneratedSqlListAst,
    options: GeneratedReadDelimitedListValidationOptions,
) bool {
    if (maybe_range) |range| return generatedReadListPayloadIsValid(tokens, end, range, list, options);
    return generatedReadListPayloadIsEmpty(list, options.first_expression, options.last_expression);
}

fn generatedReadListPayloadIsValid(
    tokens: []const Token,
    end: usize,
    range: generated_parser.GeneratedSqlTokenRange,
    list: generated_parser.GeneratedSqlListAst,
    options: GeneratedReadDelimitedListValidationOptions,
) bool {
    if (!generatedReadDelimitedListIsValid(tokens, end, range, list, options)) return false;
    if (!generatedReadExpressionTokensEqualRange(options.first_expression, list.expression_items[0])) return false;
    if (!generatedReadExpressionTokensEqualRange(options.last_expression, list.expression_items[list.expression_items.len - 1])) return false;
    return true;
}

fn generatedReadListPayloadIsEmpty(
    list: generated_parser.GeneratedSqlListAst,
    first_expression: generated_parser.GeneratedSqlExpressionAst,
    last_expression: generated_parser.GeneratedSqlExpressionAst,
) bool {
    return list.count == 0 and
        list.items.len == 0 and
        list.expression_items.len == 0 and
        list.alias_items.len == 0 and
        list.alias_name_items.len == 0 and
        list.direction_items.len == 0 and
        list.directions.len == 0 and
        list.order_using_operator_items.len == 0 and
        list.nulls_order_items.len == 0 and
        list.nulls_orders.len == 0 and
        list.expressions.len == 0 and
        list.first_tokens == null and
        list.last_tokens == null and
        first_expression.tokens == null and
        last_expression.tokens == null;
}

fn generatedReadDistinctPayloadIsValid(
    tokens: []const Token,
    end: usize,
    maybe_distinct_tokens: ?generated_parser.GeneratedSqlTokenRange,
    distinct_on_items: generated_parser.GeneratedSqlListAst,
) bool {
    const distinct_tokens = maybe_distinct_tokens orelse return generatedReadListPayloadIsEmpty(distinct_on_items, .{}, .{});
    if (!generatedReadTokenRangeIsValidThrough(tokens, end, distinct_tokens)) return false;
    if (distinct_tokens.start >= distinct_tokens.end or !tokens[distinct_tokens.start].matchesKeywordTag(.distinct)) return false;
    if (distinct_tokens.end == distinct_tokens.start + 1) return generatedReadListPayloadIsEmpty(distinct_on_items, .{}, .{});
    if (distinct_tokens.start + 4 > distinct_tokens.end) return false;
    if (!tokens[distinct_tokens.start + 1].matchesKeywordTag(.on)) return false;
    if (tokens[distinct_tokens.start + 2].kind != .lparen or tokens[distinct_tokens.end - 1].kind != .rparen) return false;
    const expression_tokens = generated_parser.GeneratedSqlTokenRange{ .start = distinct_tokens.start + 3, .end = distinct_tokens.end - 1 };
    return generatedReadDelimitedListIsValid(tokens, end, expression_tokens, distinct_on_items, .{
        .reject_aliases = true,
        .reject_order_modifiers = true,
    });
}

fn generatedReadDelimitedListIsValid(
    tokens: []const Token,
    end: usize,
    range: generated_parser.GeneratedSqlTokenRange,
    list: generated_parser.GeneratedSqlListAst,
    options: GeneratedReadDelimitedListValidationOptions,
) bool {
    if (!generatedReadTokenRangeIsValidThrough(tokens, end, range)) return false;
    if (list.count == 0 or
        list.items.len != list.count or
        list.expression_items.len != list.count or
        list.alias_items.len != list.count or
        list.alias_name_items.len != list.count or
        list.direction_items.len != list.count or
        list.directions.len != list.count or
        list.order_using_operator_items.len != list.count or
        list.nulls_order_items.len != list.count or
        list.nulls_orders.len != list.count or
        list.expressions.len != list.count)
    {
        return false;
    }
    if (!std.meta.eql(list.first_tokens orelse return false, list.items[0])) return false;
    if (!std.meta.eql(list.last_tokens orelse return false, list.items[list.items.len - 1])) return false;

    var expected_start = range.start;
    for (list.items, 0..) |item, index| {
        if (!generatedReadNestedRangeIsValid(tokens, end, range, item)) return false;
        if (item.start != expected_start or item.start >= item.end) return false;
        if (options.single_token_items and item.end != item.start + 1) return false;
        if (!generatedReadNestedRangeIsValid(tokens, end, item, list.expression_items[index])) return false;
        if (list.expression_items[index].start != item.start) return false;
        if (!generatedReadExpressionTokensEqualRange(list.expressions[index], list.expression_items[index])) return false;
        if (!generatedReadListAliasPayloadIsValid(tokens, end, item, list.expression_items[index], list.alias_items[index], list.alias_name_items[index], options)) return false;
        if (!generatedReadListOrderPayloadIsValid(tokens, end, item, list.expression_items[index], list.direction_items[index], list.directions[index], list.order_using_operator_items[index], list.nulls_order_items[index], list.nulls_orders[index], options)) return false;
        if (options.reject_aliases and (list.alias_items[index] != null or list.alias_name_items[index] != null)) return false;
        if (options.reject_order_modifiers and
            (list.direction_items[index] != null or
                list.directions[index] != null or
                list.order_using_operator_items[index] != null or
                list.nulls_order_items[index] != null or
                list.nulls_orders[index] != null))
        {
            return false;
        }

        expected_start = item.end;
        if (index + 1 < list.items.len) {
            if (expected_start >= range.end or tokens[expected_start].kind != .comma) return false;
            expected_start += 1;
        }
    }
    return expected_start == range.end;
}

fn generatedReadListAliasPayloadIsValid(
    tokens: []const Token,
    end: usize,
    item: generated_parser.GeneratedSqlTokenRange,
    expression: generated_parser.GeneratedSqlTokenRange,
    maybe_alias: ?generated_parser.GeneratedSqlTokenRange,
    maybe_alias_name: ?generated_parser.GeneratedSqlTokenRange,
    options: GeneratedReadDelimitedListValidationOptions,
) bool {
    if (maybe_alias == null and maybe_alias_name == null) return expression.end == item.end;
    if (!options.allow_aliases) return false;
    const alias = maybe_alias orelse return false;
    const alias_name = maybe_alias_name orelse return false;
    if (!generatedReadNestedRangeIsValid(tokens, end, item, alias)) return false;
    if (!generatedReadNestedRangeIsValid(tokens, end, alias, alias_name)) return false;
    if (alias.start != expression.end or alias.end != item.end) return false;
    if (alias.start >= alias.end or alias_name.start >= alias_name.end) return false;
    if (tokens[alias.start].matchesKeywordTag(.as)) return alias_name.start == alias.start + 1 and alias_name.end == alias.end;
    return alias_name.start == alias.start and alias_name.end == alias.end;
}

fn generatedReadListOrderPayloadIsValid(
    tokens: []const Token,
    end: usize,
    item: generated_parser.GeneratedSqlTokenRange,
    expression: generated_parser.GeneratedSqlTokenRange,
    maybe_direction: ?generated_parser.GeneratedSqlTokenRange,
    direction: ?generated_parser.GeneratedSqlOrderDirection,
    maybe_using_operator: ?generated_parser.GeneratedSqlTokenRange,
    maybe_nulls_order: ?generated_parser.GeneratedSqlTokenRange,
    nulls_order: ?generated_parser.GeneratedSqlNullsOrder,
    options: GeneratedReadDelimitedListValidationOptions,
) bool {
    if (maybe_direction == null and direction == null and maybe_using_operator == null and maybe_nulls_order == null and nulls_order == null) {
        return expression.end == item.end;
    }
    if (!options.allow_order_modifiers) return false;
    var cursor = expression.end;
    if (maybe_direction) |direction_tokens| {
        if (!generatedReadNestedRangeIsValid(tokens, end, item, direction_tokens)) return false;
        if (direction_tokens.start != cursor) return false;
        if (maybe_using_operator) |operator_tokens| {
            if (direction != null) return false;
            if (!std.meta.eql(direction_tokens, generated_parser.GeneratedSqlTokenRange{ .start = cursor, .end = cursor + 2 })) return false;
            if (!tokens[cursor].matchesKeywordTag(.using)) return false;
            if (!std.meta.eql(operator_tokens, generated_parser.GeneratedSqlTokenRange{ .start = cursor + 1, .end = cursor + 2 })) return false;
            cursor += 2;
        } else {
            const value = direction orelse return false;
            if (!std.meta.eql(direction_tokens, generated_parser.GeneratedSqlTokenRange{ .start = cursor, .end = cursor + 1 })) return false;
            switch (value) {
                .asc => if (!tokens[cursor].matchesKeywordTag(.asc)) return false,
                .desc => if (!tokens[cursor].matchesKeywordTag(.desc)) return false,
            }
            cursor += 1;
        }
    } else if (direction != null or maybe_using_operator != null) {
        return false;
    }
    if (maybe_nulls_order) |nulls_tokens| {
        const value = nulls_order orelse return false;
        if (!generatedReadNestedRangeIsValid(tokens, end, item, nulls_tokens)) return false;
        if (!std.meta.eql(nulls_tokens, generated_parser.GeneratedSqlTokenRange{ .start = cursor, .end = cursor + 2 })) return false;
        if (!tokens[cursor].matchesKeywordTag(.nulls)) return false;
        switch (value) {
            .first => if (!tokens[cursor + 1].matchesKeywordTag(.first)) return false,
            .last => if (!tokens[cursor + 1].matchesKeywordTag(.last)) return false,
        }
        cursor += 2;
    } else if (nulls_order != null) {
        return false;
    }
    return cursor == item.end;
}

fn generatedReadOptionalRangesEqual(
    left: ?generated_parser.GeneratedSqlTokenRange,
    right: ?generated_parser.GeneratedSqlTokenRange,
) bool {
    if (left == null and right == null) return true;
    return std.meta.eql(left orelse return false, right orelse return false);
}

fn generatedReadOptionalExpressionTokensMatchMaybeRange(
    expression: generated_parser.GeneratedSqlExpressionAst,
    maybe_range: ?generated_parser.GeneratedSqlTokenRange,
) bool {
    if (maybe_range) |range| return generatedReadExpressionTokensEqualRange(expression, range);
    return expression.tokens == null;
}

fn generatedReadAntflySourceItemsAreValid(
    tokens: []const Token,
    end: usize,
    maybe_source_tokens: ?generated_parser.GeneratedSqlTokenRange,
    items: []const generated_parser.GeneratedSqlAntflyTableFunctionAst,
    count: usize,
) bool {
    if (items.len != count) return false;
    const source_tokens = maybe_source_tokens orelse return items.len == 0 and count == 0;
    if (!generatedReadTokenRangeIsValidThrough(tokens, end, source_tokens)) return false;
    var previous_end = source_tokens.start;
    for (items) |item| {
        if (item.tokens.start < previous_end) return false;
        if (!generatedReadAntflySourceItemIsValid(tokens, end, source_tokens, item)) return false;
        previous_end = item.tokens.end;
    }
    return true;
}

fn generatedReadAntflySourceItemIsValid(
    tokens: []const Token,
    end: usize,
    source_tokens: generated_parser.GeneratedSqlTokenRange,
    item: generated_parser.GeneratedSqlAntflyTableFunctionAst,
) bool {
    if (!generatedReadNestedRangeIsValid(tokens, end, source_tokens, item.tokens)) return false;
    if (item.tokens.end < item.tokens.start + 3) return false;
    if (!std.meta.eql(item.name_tokens, generated_parser.GeneratedSqlTokenRange{ .start = item.tokens.start, .end = item.tokens.start + 1 })) return false;
    if (!std.meta.eql(item.argument_tokens, generated_parser.GeneratedSqlTokenRange{ .start = item.tokens.start + 2, .end = item.tokens.end - 1 })) return false;
    if (tokens[item.tokens.start + 1].kind != .lparen or tokens[item.tokens.end - 1].kind != .rparen) return false;
    if (!generatedReadTokenRangeIsValidThrough(tokens, end, item.name_tokens)) return false;
    if (!generatedReadTokenRangeIsValidThrough(tokens, end, item.argument_tokens)) return false;
    if (generatedReadAntflyTableFunctionKindForToken(tokens[item.name_tokens.start]) != item.kind) return false;
    if (item.argument_items.len != item.argument_count) return false;
    var previous_end = item.argument_tokens.start;
    for (item.argument_items, 0..) |argument, index| {
        if (index == 0) {
            if (argument.tokens.start != item.argument_tokens.start) return false;
        } else {
            if (previous_end >= item.argument_tokens.end or tokens[previous_end].kind != .comma) return false;
            if (argument.tokens.start != previous_end + 1) return false;
        }
        if (!generatedReadNamedArgumentIsValid(tokens, end, item.argument_tokens, argument)) return false;
        if (generatedReadAntflySourceItemHasPriorArgumentName(tokens, item.argument_items[0..index], argument)) return false;
        previous_end = argument.tokens.end;
    }
    if (item.argument_items.len == 0) return item.argument_tokens.start == item.argument_tokens.end;
    if (previous_end != item.argument_tokens.end) return false;
    return true;
}

fn generatedReadAntflySourceItemHasPriorArgumentName(
    tokens: []const Token,
    prior_arguments: []const generated_parser.GeneratedSqlNamedArgumentAst,
    argument: generated_parser.GeneratedSqlNamedArgumentAst,
) bool {
    if (argument.name_tokens.end != argument.name_tokens.start + 1 or argument.name_tokens.start >= tokens.len) return true;
    for (prior_arguments) |prior| {
        if (prior.name_tokens.end != prior.name_tokens.start + 1 or prior.name_tokens.start >= tokens.len) return true;
        if (tokenMatchesText(tokens[prior.name_tokens.start], tokens[argument.name_tokens.start].text)) return true;
    }
    return false;
}

fn generatedReadNamedArgumentIsValid(
    tokens: []const Token,
    end: usize,
    argument_tokens: generated_parser.GeneratedSqlTokenRange,
    argument: generated_parser.GeneratedSqlNamedArgumentAst,
) bool {
    if (!generatedReadNestedRangeIsValid(tokens, end, argument_tokens, argument.tokens)) return false;
    if (!generatedReadNestedRangeIsValid(tokens, end, argument.tokens, argument.name_tokens)) return false;
    if (!generatedReadNestedRangeIsValid(tokens, end, argument.tokens, argument.operator_tokens)) return false;
    if (!generatedReadNestedRangeIsValid(tokens, end, argument.tokens, argument.value_tokens)) return false;
    if (argument.name_tokens.start != argument.tokens.start) return false;
    if (argument.name_tokens.end != argument.name_tokens.start + 1) return false;
    if (tokens[argument.name_tokens.start].kind != .identifier) return false;
    if (argument.value_tokens.end != argument.tokens.end) return false;
    if (argument.operator_tokens.start < argument.name_tokens.end or argument.operator_tokens.end > argument.value_tokens.start) return false;
    if (argument.operator_tokens.end != argument.value_tokens.start) return false;
    if (!generatedReadNamedArgumentOperatorIsValid(tokens, argument.operator_tokens)) return false;
    return true;
}

fn generatedReadNamedArgumentOperatorIsValid(
    tokens: []const Token,
    operator_tokens: generated_parser.GeneratedSqlTokenRange,
) bool {
    if (operator_tokens.start >= operator_tokens.end or operator_tokens.end > tokens.len) return false;
    if (operator_tokens.end == operator_tokens.start + 1) {
        return tokens[operator_tokens.start].kind == .eq;
    }
    if (operator_tokens.end == operator_tokens.start + 2) {
        return tokens[operator_tokens.start].kind == .eq and tokens[operator_tokens.start + 1].kind == .gt;
    }
    return false;
}

fn generatedReadGraphSourceItemsAreValid(
    tokens: []const Token,
    end: usize,
    maybe_source_tokens: ?generated_parser.GeneratedSqlTokenRange,
    antfly_items: []const generated_parser.GeneratedSqlAntflyTableFunctionAst,
    graph_items: []const generated_parser.GeneratedSqlGraphTableFunctionAst,
    count: usize,
) bool {
    if (graph_items.len != count) return false;
    const source_tokens = maybe_source_tokens orelse return graph_items.len == 0 and count == 0;
    if (!generatedReadTokenRangeIsValidThrough(tokens, end, source_tokens)) return false;
    var previous_end = source_tokens.start;
    for (graph_items) |item| {
        if (item.tokens.start < previous_end) return false;
        if (!generatedReadGraphSourceItemIsValid(tokens, end, source_tokens, item)) return false;
        const matching_antfly_item = generatedReadMatchingAntflySourceItem(item, antfly_items) orelse return false;
        if (!generatedReadGraphSourceSemanticPayloadIsValid(tokens, end, item, matching_antfly_item)) return false;
        previous_end = item.tokens.end;
    }
    return true;
}

fn generatedReadGraphSourceItemIsValid(
    tokens: []const Token,
    end: usize,
    source_tokens: generated_parser.GeneratedSqlTokenRange,
    item: generated_parser.GeneratedSqlGraphTableFunctionAst,
) bool {
    if (!generatedReadNestedRangeIsValid(tokens, end, source_tokens, item.tokens)) return false;
    if (item.tokens.end < item.tokens.start + 3) return false;
    if (!std.meta.eql(item.name_tokens, generated_parser.GeneratedSqlTokenRange{ .start = item.tokens.start, .end = item.tokens.start + 1 })) return false;
    if (!std.meta.eql(item.argument_tokens, generated_parser.GeneratedSqlTokenRange{ .start = item.tokens.start + 2, .end = item.tokens.end - 1 })) return false;
    if (tokens[item.tokens.start + 1].kind != .lparen or tokens[item.tokens.end - 1].kind != .rparen) return false;
    if (!generatedReadTokenRangeIsValidThrough(tokens, end, item.name_tokens)) return false;
    if (!generatedReadTokenRangeIsValidThrough(tokens, end, item.argument_tokens)) return false;
    if (generatedReadGraphTableFunctionKindForToken(tokens[item.name_tokens.start]) != item.kind) return false;
    return true;
}

fn generatedReadMatchingAntflySourceItem(
    graph_item: generated_parser.GeneratedSqlGraphTableFunctionAst,
    antfly_items: []const generated_parser.GeneratedSqlAntflyTableFunctionAst,
) ?generated_parser.GeneratedSqlAntflyTableFunctionAst {
    for (antfly_items) |antfly_item| {
        if (!std.meta.eql(graph_item.tokens, antfly_item.tokens)) continue;
        if (!std.meta.eql(graph_item.name_tokens, antfly_item.name_tokens)) continue;
        if (!std.meta.eql(graph_item.argument_tokens, antfly_item.argument_tokens)) continue;
        if (generatedReadGraphTableFunctionKindFromAntfly(antfly_item.kind) != graph_item.kind) continue;
        return antfly_item;
    }
    return null;
}

fn generatedReadGraphSourceSemanticPayloadIsValid(
    tokens: []const Token,
    end: usize,
    graph_item: generated_parser.GeneratedSqlGraphTableFunctionAst,
    antfly_item: generated_parser.GeneratedSqlAntflyTableFunctionAst,
) bool {
    if (graph_item.argument_count != antfly_item.argument_count) return false;
    if (!generatedReadGraphSemanticValueMatches(tokens, end, graph_item.argument_tokens, graph_item.table_name_value_tokens, generatedReadAntflyArgumentValueByNames(tokens, antfly_item, &.{ "table_name", "table" }))) return false;
    if (!generatedReadGraphSemanticValueMatches(tokens, end, graph_item.argument_tokens, graph_item.index_value_tokens, generatedReadAntflyArgumentValueByNames(tokens, antfly_item, &.{ "index", "graph_index" }))) return false;
    if (!generatedReadGraphSemanticValueMatches(tokens, end, graph_item.argument_tokens, graph_item.start_value_tokens, generatedReadAntflyArgumentValueByNames(tokens, antfly_item, &.{ "start", "start_node" }))) return false;
    if (!generatedReadGraphSemanticValueMatches(tokens, end, graph_item.argument_tokens, graph_item.start_result_ref_value_tokens, generatedReadAntflyArgumentValueByNames(tokens, antfly_item, &.{ "start_result_ref", "result_ref" }))) return false;
    if (!generatedReadGraphSemanticValueMatches(tokens, end, graph_item.argument_tokens, graph_item.target_value_tokens, generatedReadAntflyArgumentValueByNames(tokens, antfly_item, &.{ "target", "target_node" }))) return false;
    if (!generatedReadGraphSemanticValueMatches(tokens, end, graph_item.argument_tokens, graph_item.target_result_ref_value_tokens, generatedReadAntflyArgumentValueByNames(tokens, antfly_item, &.{"target_result_ref"}))) return false;
    if (!generatedReadGraphSemanticValueMatches(tokens, end, graph_item.argument_tokens, graph_item.pattern_value_tokens, generatedReadAntflyArgumentValueByNames(tokens, antfly_item, &.{"pattern"}))) return false;
    if (!generatedReadGraphSemanticValueMatches(tokens, end, graph_item.argument_tokens, graph_item.return_value_tokens, generatedReadAntflyArgumentValueByNames(tokens, antfly_item, &.{ "return", "return_aliases" }))) return false;
    if (!generatedReadGraphSemanticValueMatches(tokens, end, graph_item.argument_tokens, graph_item.metric_value_tokens, generatedReadAntflyArgumentValueByNames(tokens, antfly_item, &.{ "metric", "graph_metric" }))) return false;
    if (!generatedReadGraphSemanticValueMatches(tokens, end, graph_item.argument_tokens, graph_item.query_value_tokens, generatedReadAntflyArgumentValueByNames(tokens, antfly_item, &.{ "query", "text" }))) return false;

    return switch (graph_item.kind) {
        .traverse, .neighbors => graph_item.index_value_tokens != null and (graph_item.start_value_tokens != null or graph_item.start_result_ref_value_tokens != null),
        .shortest_path, .k_shortest_paths => graph_item.index_value_tokens != null and
            (graph_item.start_value_tokens != null or graph_item.start_result_ref_value_tokens != null) and
            (graph_item.target_value_tokens != null or graph_item.target_result_ref_value_tokens != null),
        .match => graph_item.index_value_tokens != null and
            (graph_item.start_value_tokens != null or graph_item.start_result_ref_value_tokens != null) and
            graph_item.pattern_value_tokens != null,
        .metric, .metric_rerank => graph_item.index_value_tokens != null and graph_item.metric_value_tokens != null,
    };
}

fn generatedReadGraphSemanticValueMatches(
    tokens: []const Token,
    end: usize,
    argument_tokens: generated_parser.GeneratedSqlTokenRange,
    actual: ?generated_parser.GeneratedSqlTokenRange,
    expected: ?generated_parser.GeneratedSqlTokenRange,
) bool {
    if (actual == null and expected == null) return true;
    const actual_value = actual orelse return false;
    const expected_value = expected orelse return false;
    if (!std.meta.eql(actual_value, expected_value)) return false;
    return generatedReadNestedRangeIsValid(tokens, end, argument_tokens, actual_value);
}

fn generatedReadAntflyArgumentValueByNames(
    tokens: []const Token,
    item: generated_parser.GeneratedSqlAntflyTableFunctionAst,
    names: []const []const u8,
) ?generated_parser.GeneratedSqlTokenRange {
    for (item.argument_items) |argument| {
        if (argument.name_tokens.start >= argument.name_tokens.end or argument.name_tokens.start >= tokens.len) continue;
        for (names) |name| {
            if (tokenMatchesText(tokens[argument.name_tokens.start], name)) return argument.value_tokens;
        }
    }
    return null;
}

fn generatedReadGraphSourceCompatibilityFieldsAreValid(read_ast: *const generated_parser.GeneratedSqlReadAst) bool {
    if (read_ast.source_graph_function_items.len == 0) {
        return read_ast.source_graph_function_count == 0 and
            read_ast.source_graph_function_tokens == null and
            read_ast.source_graph_function_name_tokens == null and
            read_ast.source_graph_function_argument_tokens == null and
            read_ast.source_graph_function_kind == null;
    }
    const first = read_ast.source_graph_function_items[0];
    return std.meta.eql(read_ast.source_graph_function_tokens orelse return false, first.tokens) and
        std.meta.eql(read_ast.source_graph_function_name_tokens orelse return false, first.name_tokens) and
        std.meta.eql(read_ast.source_graph_function_argument_tokens orelse return false, first.argument_tokens) and
        (read_ast.source_graph_function_kind orelse return false) == first.kind;
}

fn generatedReadAntflyTableFunctionKindForToken(token: Token) ?generated_parser.GeneratedSqlAntflyTableFunctionKind {
    if (token.matchesQualifiedKeywordTag("antfly", .full_text_search)) return .full_text_search;
    if (token.matchesQualifiedKeywordTag("antfly", .semantic_search)) return .semantic_search;
    if (token.matchesQualifiedKeywordTag("antfly", .vector_search)) return .vector_search;
    if (token.matchesQualifiedKeywordTag("antfly", .graph_traverse)) return .graph_traverse;
    if (token.matchesQualifiedKeywordTag("antfly", .graph_neighbors)) return .graph_neighbors;
    if (token.matchesQualifiedKeywordTag("antfly", .graph_shortest_path)) return .graph_shortest_path;
    if (token.matchesQualifiedKeywordTag("antfly", .graph_k_shortest_paths)) return .graph_k_shortest_paths;
    if (token.matchesQualifiedKeywordTag("antfly", .graph_match)) return .graph_match;
    if (token.matchesQualifiedKeywordTag("antfly", .graph_metric)) return .graph_metric;
    if (token.matchesQualifiedKeywordTag("antfly", .graph_metric_rerank)) return .graph_metric_rerank;
    if (token.matchesQualifiedKeywordTag("antfly", .hybrid_search)) return .hybrid_search;
    return null;
}

fn generatedReadGraphTableFunctionKindForToken(token: Token) ?generated_parser.GeneratedSqlGraphTableFunctionKind {
    if (token.matchesQualifiedKeywordTag("antfly", .graph_traverse)) return .traverse;
    if (token.matchesQualifiedKeywordTag("antfly", .graph_neighbors)) return .neighbors;
    if (token.matchesQualifiedKeywordTag("antfly", .graph_shortest_path)) return .shortest_path;
    if (token.matchesQualifiedKeywordTag("antfly", .graph_k_shortest_paths)) return .k_shortest_paths;
    if (token.matchesQualifiedKeywordTag("antfly", .graph_match)) return .match;
    if (token.matchesQualifiedKeywordTag("antfly", .graph_metric)) return .metric;
    if (token.matchesQualifiedKeywordTag("antfly", .graph_metric_rerank)) return .metric_rerank;
    return null;
}

fn generatedReadGraphTableFunctionKindFromAntfly(kind: generated_parser.GeneratedSqlAntflyTableFunctionKind) ?generated_parser.GeneratedSqlGraphTableFunctionKind {
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

fn generatedReadOptionalRangeIsPrecededByKeyword(
    tokens: []const Token,
    range: ?generated_parser.GeneratedSqlTokenRange,
    keyword: TokenKeyword,
) bool {
    const actual = range orelse return true;
    return actual.start > 0 and tokens[actual.start - 1].matchesKeywordTag(keyword);
}

fn generatedReadOptionalRangeIsPrecededByKeywordPair(
    tokens: []const Token,
    range: ?generated_parser.GeneratedSqlTokenRange,
    first: TokenKeyword,
    second: TokenKeyword,
) bool {
    const actual = range orelse return true;
    return actual.start >= 2 and
        tokens[actual.start - 2].matchesKeywordTag(first) and
        tokens[actual.start - 1].matchesKeywordTag(second);
}

fn generatedReadOptionalNestedRangeIsValid(
    tokens: []const Token,
    end: usize,
    outer: generated_parser.GeneratedSqlTokenRange,
    range: ?generated_parser.GeneratedSqlTokenRange,
) bool {
    if (range) |inner| {
        if (!generatedReadTokenRangeIsValidThrough(tokens, end, inner)) return false;
        return inner.start >= outer.start and inner.end <= outer.end;
    }
    return true;
}

fn generatedReadNestedRangeIsValid(
    tokens: []const Token,
    end: usize,
    outer: generated_parser.GeneratedSqlTokenRange,
    inner: generated_parser.GeneratedSqlTokenRange,
) bool {
    if (!generatedReadTokenRangeIsValidThrough(tokens, end, inner)) return false;
    return inner.start >= outer.start and inner.end <= outer.end;
}

fn generatedReadOptionalTokenRangeIsValidThrough(
    tokens: []const Token,
    end: usize,
    range: ?generated_parser.GeneratedSqlTokenRange,
) bool {
    if (range) |value| return generatedReadTokenRangeIsValidThrough(tokens, end, value);
    return true;
}

fn generatedReadTokenRangeIsValidThrough(
    tokens: []const Token,
    end: usize,
    range: generated_parser.GeneratedSqlTokenRange,
) bool {
    return end <= tokens.len and range.start < range.end and range.end <= end;
}

fn generatedReadPaginationPayloadIsValid(
    tokens: []const Token,
    limit_tokens: ?generated_parser.GeneratedSqlTokenRange,
    limit_expression: generated_parser.GeneratedSqlExpressionAst,
    limit_all: bool,
    offset_tokens: ?generated_parser.GeneratedSqlTokenRange,
    offset_expression: generated_parser.GeneratedSqlExpressionAst,
    fetch_tokens: ?generated_parser.GeneratedSqlTokenRange,
    fetch_count_tokens: ?generated_parser.GeneratedSqlTokenRange,
    fetch_count_expression: generated_parser.GeneratedSqlExpressionAst,
) bool {
    if (limit_tokens) |range| {
        if (!generatedReadTokenRangeIsValid(tokens, range)) return false;
        if (limit_all) {
            if (limit_expression.tokens != null) return false;
        } else if (!generatedReadExpressionTokensEqualRange(limit_expression, range)) {
            return false;
        }
    } else if (limit_all or limit_expression.tokens != null) {
        return false;
    }

    if (offset_tokens) |range| {
        if (!generatedReadTokenRangeIsValid(tokens, range)) return false;
        if (!generatedReadOffsetExpressionTokensMatchRange(tokens, offset_expression, range)) return false;
    } else if (offset_expression.tokens != null) {
        return false;
    }

    if (fetch_tokens) |range| {
        if (!generatedReadTokenRangeIsValid(tokens, range)) return false;
        if (!generatedReadFetchRangeLayoutIsValid(tokens, range, fetch_count_tokens)) return false;
        if (fetch_count_tokens) |count_range| {
            if (!generatedReadTokenRangeIsValid(tokens, count_range)) return false;
            if (count_range.start < range.start or count_range.end > range.end) return false;
            if (!generatedReadExpressionTokensEqualRange(fetch_count_expression, count_range)) return false;
        } else if (fetch_count_expression.tokens != null) {
            return false;
        }
    } else if (fetch_count_tokens != null or fetch_count_expression.tokens != null) {
        return false;
    }
    return true;
}

fn generatedReadFetchRangeLayoutIsValid(
    tokens: []const Token,
    range: generated_parser.GeneratedSqlTokenRange,
    fetch_count_tokens: ?generated_parser.GeneratedSqlTokenRange,
) bool {
    if (range.start + 3 > range.end or range.end > tokens.len) return false;
    if (!tokens[range.start].matchesKeywordTag(.first) and !tokens[range.start].matchesKeywordTag(.next)) return false;
    if (!tokens[range.end - 1].matchesKeywordTag(.only)) return false;
    const row_index = range.end - 2;
    if (!tokens[row_index].matchesKeywordTag(.row) and !tokens[row_index].matchesKeywordTag(.rows)) return false;
    if (fetch_count_tokens) |count_range| {
        return count_range.start == range.start + 1 and count_range.end == row_index;
    }
    return row_index == range.start + 1;
}

fn generatedReadTokenRangeIsValid(tokens: []const Token, range: generated_parser.GeneratedSqlTokenRange) bool {
    return range.start < range.end and range.end <= tokens.len;
}

fn generatedReadExpressionTokensEqualRange(
    expression: generated_parser.GeneratedSqlExpressionAst,
    range: generated_parser.GeneratedSqlTokenRange,
) bool {
    const expression_tokens = expression.tokens orelse return false;
    return std.meta.eql(expression_tokens, range);
}

fn generatedReadOffsetExpressionTokensMatchRange(
    tokens: []const Token,
    expression: generated_parser.GeneratedSqlExpressionAst,
    range: generated_parser.GeneratedSqlTokenRange,
) bool {
    const expression_tokens = expression.tokens orelse return false;
    if (expression_tokens.start != range.start) return false;
    if (expression_tokens.end == range.end) return true;
    if (expression_tokens.end + 1 != range.end or expression_tokens.end >= tokens.len) return false;
    return tokens[expression_tokens.end].matchesKeywordTag(.row) or tokens[expression_tokens.end].matchesKeywordTag(.rows);
}

fn generatedCteReadStatementKind(
    tokens: []const Token,
    read_ast: *const generated_parser.GeneratedSqlReadAst,
) ?classifier.SqlReadStatementKind {
    if (read_ast.projection_tokens == null or read_ast.source_tokens == null) return null;
    if (read_ast.cte_recursive) return .recursive_cte;
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

fn generatedUnsupportedUsesDdlPlanBoundary(kind: generated_parser.GeneratedSqlUnsupportedKind) bool {
    return switch (kind) {
        .alter_publication,
        .alter_subscription,
        .analyze,
        .call,
        .close,
        .cluster,
        .comment,
        .copy,
        .create_publication,
        .create_subscription,
        .create_trigger,
        .declare,
        .drop_publication,
        .drop_subscription,
        .drop_trigger,
        .fetch,
        .grant,
        .listen,
        .lock,
        .notify,
        .reindex,
        .release,
        .revoke,
        .savepoint,
        .unlisten,
        .vacuum,
        => true,
        .explain,
        .alter_aggregate,
        .alter_index,
        .alter_conversion,
        .alter_default_privileges,
        .alter_event_trigger,
        .alter_foreign_data_wrapper,
        .alter_foreign_table,
        .alter_function,
        .alter_large_object,
        .alter_language,
        .alter_materialized_view,
        .alter_operator,
        .alter_operator_class,
        .alter_operator_family,
        .alter_policy,
        .alter_procedure,
        .alter_routine,
        .alter_rule,
        .alter_server,
        .alter_system,
        .alter_statistics,
        .alter_text_search_configuration,
        .alter_text_search_dictionary,
        .alter_text_search_parser,
        .alter_text_search_template,
        .alter_trigger,
        .alter_transform,
        .alter_user_mapping,
        .checkpoint,
        .discard,
        .create_access_method,
        .create_conversion,
        .create_database_options,
        .create_event_trigger,
        .create_foreign_data_wrapper,
        .create_foreign_table,
        .create_language,
        .create_materialized_view,
        .create_operator_class,
        .create_operator_family,
        .create_policy,
        .create_rule,
        .create_schema_options,
        .create_server,
        .create_statistics,
        .create_text_search_configuration,
        .create_text_search_dictionary,
        .create_text_search_parser,
        .create_text_search_template,
        .create_transform,
        .create_user_mapping,
        .do_block,
        .drop_access_method,
        .drop_collation_multi,
        .drop_conversion,
        .drop_domain_multi,
        .drop_event_trigger,
        .drop_extension_multi,
        .drop_foreign_data_wrapper,
        .drop_foreign_table,
        .drop_index_multi,
        .drop_language,
        .drop_materialized_view_multi,
        .drop_materialized_view,
        .drop_owned,
        .drop_operator_class,
        .drop_operator_family,
        .drop_policy,
        .drop_publication_multi,
        .drop_routine,
        .drop_role_multi,
        .drop_rule,
        .drop_schema_multi,
        .drop_sequence_multi,
        .drop_server,
        .drop_statistics,
        .drop_table_multi,
        .drop_text_search_configuration,
        .drop_text_search_dictionary,
        .drop_text_search_parser,
        .drop_text_search_template,
        .drop_transform,
        .drop_type_multi,
        .drop_user_mapping,
        .drop_view_multi,
        .graph_query,
        .insert_overriding_value,
        .import_foreign_schema,
        .load,
        .reassign_owned,
        .role_session_control,
        .security_label,
        => false,
    };
}

fn classifyDdlLikeStatement(raw_statement: RawSqlStatement, tokens: []const Token) ParsedStatement {
    if (tokens.len == 0 or tokens[0].kind != .identifier) return .{ .unknown = raw_statement };
    if (tokens[0].isKeyword(.explain)) return .{ .explain = parseExplainStatement(raw_statement, tokens) catch .{ .raw = raw_statement } };
    if (tokens[0].isKeyword(.begin) or tokens[0].isKeyword(.commit) or tokens[0].isKeyword(.end) or tokens[0].isKeyword(.rollback)) {
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

fn parseGeneratedExplainStatement(
    raw_statement: RawSqlStatement,
    tokens: []const Token,
    generated_raw: GeneratedRawSqlStatement,
) ParsedExplainStatement {
    const ast_value = generated_raw.ast orelse return .{ .raw = raw_statement };
    const unsupported = switch (ast_value) {
        .unsupported => |value| value,
        else => return .{ .raw = raw_statement },
    };
    if (unsupported.kind != .explain or unsupported.reason != .explain_not_planned_by_generated_parser) return .{ .raw = raw_statement };
    if (!std.meta.eql(unsupported.command_span, tokens[raw_statement.token_start].sourceSpan())) return .{ .raw = raw_statement };
    if (!unsupported.explain_options_valid) return .{ .raw = raw_statement };
    if (unsupported.explain_options_tokens) |options| {
        if (!generatedExplainTokenRangeIsValid(tokens, raw_statement, options)) return .{ .raw = raw_statement };
        if (options.start <= raw_statement.token_start or options.end > raw_statement.token_end) return .{ .raw = raw_statement };
        if (tokens[options.start].kind != .lparen or tokens[options.end - 1].kind != .rparen) return .{ .raw = raw_statement };
    }

    var statement = ParsedExplainStatement{
        .raw = raw_statement,
        .analyze = unsupported.explain_analyze,
        .format = generatedExplainFormat(unsupported.explain_format),
        .verbose = unsupported.explain_verbose,
        .costs = unsupported.explain_costs,
        .buffers = unsupported.explain_buffers,
        .timing = unsupported.explain_timing,
        .summary = unsupported.explain_summary,
        .settings = unsupported.explain_settings,
        .wal = unsupported.explain_wal,
    };
    if (unsupported.subject_tokens) |subject| {
        if (!generatedExplainTokenRangeIsValid(tokens, raw_statement, subject)) return .{ .raw = raw_statement };
        if (subject.start <= raw_statement.token_start or subject.end > raw_statement.token_end) return .{ .raw = raw_statement };
        statement.inner_token_start = subject.start;
        statement.inner_token_end = subject.end;
    }
    return statement;
}

fn generatedExplainFormat(format: generated_parser.GeneratedSqlExplainFormat) ast.SqlExplainFormat {
    return switch (format) {
        .text => .text,
        .json => .json,
    };
}

fn generatedExplainTokenRangeIsValid(
    tokens: []const Token,
    raw_statement: RawSqlStatement,
    range: generated_parser.GeneratedSqlTokenRange,
) bool {
    return range.start < range.end and
        range.start >= raw_statement.token_start and
        range.end <= raw_statement.token_end and
        range.end <= tokens.len;
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
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SET search_path TO public extra"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SET LOCAL search_path TO"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "RESET search_path TO"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SHOW search_path EXTRA"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "PREPARE read_stmt AS"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "PREPARE read_stmt(text AS SELECT id FROM usage_records"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "EXECUTE read_stmt("));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "DEALLOCATE read_stmt extra"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "DEALLOCATE PREPARE read_stmt extra"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "START WORK"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "COMMIT NOW"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "COMMIT WORK NOW"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "ROLLBACK LATER"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "CREATE TABLE usage_records ("));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "CREATE INDEX usage_status_idx ON"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "CREATE INDEX docs_body_fts ON docs USING antfly_full_text"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "CREATE INDEX docs_body_fts ON docs USING antfly_full_text ("));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "CREATE INDEX docs_body_fts ON docs USING antfly_full_text (body) WITH ("));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "ALTER TABLE usage_records ADD"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "DROP TABLE IF EXISTS"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "CREATE EXTENSION vector FROM unpackaged"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "ALTER DATABASE tenant_ops SET"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "ALTER EXTENSION vector UPDATE TO"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "CREATE ROLE"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "ALTER USER"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "DROP GROUP IF EXISTS"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "DROP DATABASE tenant_ops WITH (OWNER)"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "CREATE GRAPH INDEX docs_edge_graph ON"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "ALTER GRAPH INDEX docs_edge_graph ADD"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "CALL"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "COPY"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "GRANT"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "LISTEN"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "LOCK"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "MATCH"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "NOTIFY"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "REINDEX"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "REVOKE"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SAVEPOINT"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SAVEPOINT before retry"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "RELEASE SAVEPOINT"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "ROLLBACK TO SAVEPOINT"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "UNLISTEN"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "INSERT INTO usage_records VALUES"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "INSERT INTO usage_records (id) VALUES ('u1') ON CONFLICT (id) DO"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "INSERT INTO usage_records (id) SELECT id FROM source_rows WHERE status ="));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "INSERT INTO usage_records (id) VALUES ('u1') ON CONFLICT (id) WHERE status ="));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "INSERT INTO usage_records (id) VALUES ('u1') RETURNING id ||"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "UPDATE usage_records SET"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "UPDATE usage_records SET status ="));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "UPDATE usage_records SET status = 'done' WHERE id ="));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "UPDATE usage_records SET status = 'done' WHERE status = ANY"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "DELETE FROM usage_records WHERE"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "DELETE FROM usage_records WHERE id ="));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "DELETE FROM usage_records RETURNING id ||"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "TRUNCATE TABLE"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "MERGE INTO usage_records USING source_rows ON"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "MERGE INTO usage_records USING source_rows ON usage_records.id ="));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "MERGE INTO usage_records USING source_rows ON usage_records.id = source_rows.id WHEN MATCHED THEN UPDATE SET"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "MERGE INTO usage_records USING source_rows ON usage_records.id = source_rows.id WHEN MATCHED THEN UPDATE SET status ="));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "MERGE INTO usage_records USING source_rows ON usage_records.id = source_rows.id WHEN NOT MATCHED THEN INSERT"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "MERGE INTO usage_records USING source_rows ON usage_records.id = source_rows.id WHEN NOT MATCHED THEN INSERT (id) VALUES"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "CREATE TEMP"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "CREATE TEMP TABLE"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "CREATE TEMPORARY TABLE usage_session_records ("));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "CREATE UNLOGGED TABLE IF NOT EXISTS"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "CREATE VIEW"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "CREATE VIEW active_usage"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "CREATE OR REPLACE VIEW active_usage AS"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "CREATE MATERIALIZED VIEW"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "CREATE MATERIALIZED VIEW usage_summary"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "CREATE MATERIALIZED VIEW usage_summary AS"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "ALTER SCHEMA analytics RENAME TO"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "ALTER VIEW active_usage RENAME TO"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "DROP VIEW IF EXISTS"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "DROP MATERIALIZED VIEW IF EXISTS"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "REFRESH"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "REFRESH MATERIALIZED VIEW"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "REFRESH MATERIALIZED VIEW usage_summary WITH"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "CREATE DOMAIN"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "CREATE DOMAIN positive_amount"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "CREATE DOMAIN positive_amount AS"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "ALTER DOMAIN positive_amount SET"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "DROP DOMAIN IF EXISTS"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "CREATE SEQUENCE"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "CREATE SEQUENCE order_id_seq AS"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "ALTER SEQUENCE order_id_seq RESTART"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "DROP SEQUENCE IF EXISTS"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "CREATE TYPE"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "CREATE TYPE usage_status AS"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "ALTER TYPE usage_status ADD"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "DROP TYPE IF EXISTS"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "CREATE TABLESPACE"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "CREATE TABLESPACE fastspace"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "ALTER TABLESPACE fastspace RENAME TO"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "DROP TABLESPACE IF EXISTS"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "CREATE PUBLICATION"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "CREATE PUBLICATION usage_pub FOR"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "CREATE PUBLICATION usage_pub FOR TABLE"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "ALTER PUBLICATION usage_pub ADD"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "DROP PUBLICATION IF EXISTS"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "CREATE SUBSCRIPTION"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "CREATE SUBSCRIPTION usage_sub"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "CREATE SUBSCRIPTION usage_sub CONNECTION"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "ALTER SUBSCRIPTION usage_sub"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "DROP SUBSCRIPTION IF EXISTS"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "CREATE POLICY"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "CREATE POLICY usage_policy ON"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "ALTER POLICY usage_policy ON"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "DROP POLICY IF EXISTS"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "DROP POLICY IF EXISTS usage_policy ON"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "CREATE COLLATION"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "ALTER COLLATION case_insensitive RENAME TO"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "DROP COLLATION IF EXISTS"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "CREATE OPERATOR"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "DROP OPERATOR ==="));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "CREATE AGGREGATE"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "DROP AGGREGATE first_value_text"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "CREATE CAST"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "DROP CAST ("));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "CREATE FUNCTION touch_updated_at"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "CREATE OR REPLACE FUNCTION touch_updated_at() RETURNS"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "CREATE PROCEDURE rotate_usage"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "DROP FUNCTION IF EXISTS"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "DROP PROCEDURE IF EXISTS"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "DECLARE usage_cursor CURSOR FOR"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "FETCH FROM"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "CLOSE ALL EXTRA"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SELECT"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SELECT DISTINCT"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SELECT DISTINCT ON ("));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SELECT id FROM"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records WHERE"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records WHERE status IS"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records WHERE status IS NOT"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records WHERE status IN"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records WHERE status LIKE"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records WHERE status ="));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records WHERE score >"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records WHERE tags @>"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records WHERE name ~"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SELECT first_name ||"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SELECT lower("));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records WHERE status = ANY"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records WHERE status LIKE ANY"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records WHERE status = 'active' AND"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SELECT status, COUNT(*) FROM usage_records GROUP BY status HAVING COUNT(*) > 1 OR"));
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
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SELECT row_number() OVER usage_window FROM usage_records WINDOW usage_window"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SELECT row_number() OVER first_window FROM usage_records WINDOW first_window AS (ORDER BY id), second_window"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records FOR"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records FOR UPDATE OF"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records FOR SHARE OF"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records FOR NO"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records FOR NO KEY"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records FOR KEY"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records FOR KEY SHARE OF"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records FOR UPDATE SKIP"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "WITH"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "WITH RECURSIVE"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "WITH source_rows AS"));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "WITH source_rows AS ("));
    try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, "WITH source_rows AS (SELECT id FROM usage_records) SELECT"));

    var complex_ddl = try ParsedSql.initAlloc(alloc, "ALTER TABLE audit_log ALTER COLUMN amount TYPE numeric USING amount + 1;");
    defer complex_ddl.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.ddl, complex_ddl.generatedStatementKind().?);
    switch (complex_ddl.generated_statement.?.ast.?) {
        .ddl => |ddl_ast| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlDdlKind.alter_table, ddl_ast.kind);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 4 }, ddl_ast.object_name_tokens.?);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 4, .end = 11 }, ddl_ast.alter_table_operation_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .ddl), std.meta.activeTag(complex_ddl.statement));

    var generated_catalog_ddl = try ParsedSql.initAlloc(alloc, "CREATE DATABASE tenant_ops");
    defer generated_catalog_ddl.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.extension_index, generated_catalog_ddl.generatedStatementKind().?);
    switch (generated_catalog_ddl.generated_statement.?.ast.?) {
        .extension_index => |ddl_ast| try std.testing.expectEqual(generated_parser.GeneratedSqlDdlKind.create_database, ddl_ast.kind),
        else => return error.TestUnexpectedResult,
    }

    var generated_select_into = try ParsedSql.initAlloc(alloc, "SELECT account_id, total INTO usage_archive FROM usage_records WHERE total > 10");
    defer generated_select_into.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.ddl, generated_select_into.generatedStatementKind().?);
    switch (generated_select_into.generated_statement.?.ast.?) {
        .ddl => |ddl_ast| try std.testing.expectEqual(generated_parser.GeneratedSqlDdlKind.relation_population, ddl_ast.kind),
        else => return error.TestUnexpectedResult,
    }
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .ddl), std.meta.activeTag(generated_select_into.statement));

    var generated_create_table_as = try ParsedSql.initAlloc(alloc, "CREATE TEMP TABLE IF NOT EXISTS usage_session_archive AS SELECT account_id FROM usage_records WITH NO DATA");
    defer generated_create_table_as.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.ddl, generated_create_table_as.generatedStatementKind().?);
    switch (generated_create_table_as.generated_statement.?.ast.?) {
        .ddl => |ddl_ast| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlDdlKind.relation_population, ddl_ast.kind);
            try std.testing.expect(ddl_ast.if_not_exists);
        },
        else => return error.TestUnexpectedResult,
    }
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .ddl), std.meta.activeTag(generated_create_table_as.statement));

    var generated_lifetime_table = try ParsedSql.initAlloc(alloc, "CREATE UNLOGGED TABLE IF NOT EXISTS usage_ingest_records (id uuid)");
    defer generated_lifetime_table.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.ddl, generated_lifetime_table.generatedStatementKind().?);
    switch (generated_lifetime_table.generated_statement.?.ast.?) {
        .ddl => |ddl_ast| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlDdlKind.create_table, ddl_ast.kind);
            try std.testing.expect(ddl_ast.if_not_exists);
        },
        else => return error.TestUnexpectedResult,
    }
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .ddl), std.meta.activeTag(generated_lifetime_table.statement));
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
                    try std.testing.expect(unsupported.explain_options_tokens == null);
                    try std.testing.expect(unsupported.explain_options_valid);
                    try std.testing.expect(!unsupported.explain_analyze);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExplainFormat.text, unsupported.explain_format);
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
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 26 }, unsupported.explain_options_tokens.?);
                    try std.testing.expect(unsupported.explain_options_valid);
                    try std.testing.expect(unsupported.explain_analyze);
                    try std.testing.expectEqual(generated_parser.GeneratedSqlExplainFormat.json, unsupported.explain_format);
                    try std.testing.expect(unsupported.explain_verbose);
                    try std.testing.expect(!unsupported.explain_costs);
                    try std.testing.expect(unsupported.explain_buffers);
                    try std.testing.expect(!unsupported.explain_timing);
                    try std.testing.expect(!unsupported.explain_summary);
                    try std.testing.expect(unsupported.explain_settings);
                    try std.testing.expect(unsupported.explain_wal);
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
                    try std.testing.expect(unsupported.explain_options_tokens == null);
                    try std.testing.expect(unsupported.explain_options_valid);
                    try std.testing.expect(unsupported.explain_analyze);
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
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 5 }, unsupported.explain_options_tokens.?);
                    try std.testing.expect(!unsupported.explain_options_valid);
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
            .sql = "ALTER AGGREGATE first_value_text(text) OWNER TO app_role",
            .kind = .alter_aggregate,
            .reason = .alter_aggregate_not_planned_by_generated_parser,
        },
        .{
            .sql = "ALTER CONVERSION usage_conv RENAME TO usage_conv_v2",
            .kind = .alter_conversion,
            .reason = .alter_conversion_not_planned_by_generated_parser,
        },
        .{
            .sql = "ALTER EVENT TRIGGER usage_ddl_start DISABLE",
            .kind = .alter_event_trigger,
            .reason = .alter_event_trigger_not_planned_by_generated_parser,
        },
        .{
            .sql = "ALTER INDEX usage_status_idx RENAME TO usage_status_idx_v2",
            .kind = .alter_index,
            .reason = .alter_index_not_planned_by_generated_parser,
        },
        .{
            .sql = "ALTER FOREIGN TABLE foreign_usage_records RENAME TO foreign_usage_archive",
            .kind = .alter_foreign_table,
            .reason = .alter_foreign_table_not_planned_by_generated_parser,
        },
        .{
            .sql = "ALTER FOREIGN DATA WRAPPER usage_fdw OPTIONS (ADD host 'localhost')",
            .kind = .alter_foreign_data_wrapper,
            .reason = .alter_foreign_data_wrapper_not_planned_by_generated_parser,
        },
        .{
            .sql = "ALTER FUNCTION normalize_status(text) OWNER TO app_role",
            .kind = .alter_function,
            .reason = .alter_function_not_planned_by_generated_parser,
        },
        .{
            .sql = "ALTER LANGUAGE usage_lang OWNER TO app_role",
            .kind = .alter_language,
            .reason = .alter_language_not_planned_by_generated_parser,
        },
        .{
            .sql = "ALTER MATERIALIZED VIEW usage_summary RENAME TO usage_summary_v2",
            .kind = .alter_materialized_view,
            .reason = .alter_materialized_view_not_planned_by_generated_parser,
        },
        .{
            .sql = "ALTER OPERATOR === (text, text) OWNER TO app_role",
            .kind = .alter_operator,
            .reason = .alter_operator_not_planned_by_generated_parser,
        },
        .{
            .sql = "ALTER OPERATOR CLASS usage_ops USING btree RENAME TO usage_ops_v2",
            .kind = .alter_operator_class,
            .reason = .alter_operator_class_not_planned_by_generated_parser,
        },
        .{
            .sql = "ALTER OPERATOR FAMILY usage_family USING btree RENAME TO usage_family_v2",
            .kind = .alter_operator_family,
            .reason = .alter_operator_family_not_planned_by_generated_parser,
        },
        .{
            .sql = "ALTER PROCEDURE refresh_usage_records() OWNER TO app_role",
            .kind = .alter_procedure,
            .reason = .alter_procedure_not_planned_by_generated_parser,
        },
        .{
            .sql = "ALTER LARGE OBJECT 12345 OWNER TO app_role",
            .kind = .alter_large_object,
            .reason = .alter_large_object_not_planned_by_generated_parser,
        },
        .{
            .sql = "ALTER ROUTINE normalize_status(text) OWNER TO app_role",
            .kind = .alter_routine,
            .reason = .alter_routine_not_planned_by_generated_parser,
        },
        .{
            .sql = "ALTER TRANSFORM FOR jsonb LANGUAGE plpgsql OWNER TO app_role",
            .kind = .alter_transform,
            .reason = .alter_transform_not_planned_by_generated_parser,
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
            .sql = "ALTER SYSTEM SET work_mem = '64MB'",
            .kind = .alter_system,
            .reason = .alter_system_not_planned_by_generated_parser,
        },
        .{
            .sql = "ALTER STATISTICS usage_stats SET STATISTICS 100",
            .kind = .alter_statistics,
            .reason = .alter_statistics_not_planned_by_generated_parser,
        },
        .{
            .sql = "ALTER TEXT SEARCH CONFIGURATION usage_search RENAME TO usage_search_v2",
            .kind = .alter_text_search_configuration,
            .reason = .alter_text_search_configuration_not_planned_by_generated_parser,
        },
        .{
            .sql = "ALTER TEXT SEARCH DICTIONARY usage_dict OWNER TO app_role",
            .kind = .alter_text_search_dictionary,
            .reason = .alter_text_search_dictionary_not_planned_by_generated_parser,
        },
        .{
            .sql = "ALTER TEXT SEARCH PARSER usage_parser RENAME TO usage_parser_v2",
            .kind = .alter_text_search_parser,
            .reason = .alter_text_search_parser_not_planned_by_generated_parser,
        },
        .{
            .sql = "ALTER TEXT SEARCH TEMPLATE usage_template RENAME TO usage_template_v2",
            .kind = .alter_text_search_template,
            .reason = .alter_text_search_template_not_planned_by_generated_parser,
        },
        .{
            .sql = "ALTER TRIGGER usage_audit ON usage_records RENAME TO usage_audit_v2",
            .kind = .alter_trigger,
            .reason = .alter_trigger_not_planned_by_generated_parser,
        },
        .{
            .sql = "ALTER USER MAPPING FOR usage_user SERVER usage_server OPTIONS (SET user 'remote')",
            .kind = .alter_user_mapping,
            .reason = .alter_user_mapping_not_planned_by_generated_parser,
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
            .sql = "DISCARD TEMP",
            .kind = .discard,
            .reason = .discard_not_planned_by_generated_parser,
        },
        .{
            .sql = "DISCARD TEMPORARY",
            .kind = .discard,
            .reason = .discard_not_planned_by_generated_parser,
        },
        .{
            .sql = "DISCARD PLANS",
            .kind = .discard,
            .reason = .discard_not_planned_by_generated_parser,
        },
        .{
            .sql = "DISCARD SEQUENCES",
            .kind = .discard,
            .reason = .discard_not_planned_by_generated_parser,
        },
        .{
            .sql = "INSERT INTO usage_records OVERRIDING SYSTEM VALUE VALUES ('u1')",
            .kind = .insert_overriding_value,
            .reason = .insert_overriding_value_not_planned_by_generated_parser,
        },
        .{
            .sql = "INSERT INTO usage_records OVERRIDING USER VALUE VALUES ('u1')",
            .kind = .insert_overriding_value,
            .reason = .insert_overriding_value_not_planned_by_generated_parser,
        },
        .{
            .sql = "CREATE ACCESS METHOD usage_am TYPE INDEX HANDLER usage_handler",
            .kind = .create_access_method,
            .reason = .create_access_method_not_planned_by_generated_parser,
        },
        .{
            .sql = "CREATE CONVERSION usage_conv FOR 'UTF8' TO 'LATIN1' FROM utf8_to_latin1",
            .kind = .create_conversion,
            .reason = .create_conversion_not_planned_by_generated_parser,
        },
        .{
            .sql = "CREATE DATABASE tenant_ops WITH OWNER app",
            .kind = .create_database_options,
            .reason = .create_database_options_not_planned_by_generated_parser,
        },
        .{
            .sql = "CREATE EVENT TRIGGER usage_ddl_start ON ddl_command_start EXECUTE FUNCTION audit_ddl()",
            .kind = .create_event_trigger,
            .reason = .create_event_trigger_not_planned_by_generated_parser,
        },
        .{
            .sql = "CREATE FOREIGN TABLE foreign_usage_records (id text) SERVER usage_fdw",
            .kind = .create_foreign_table,
            .reason = .create_foreign_table_not_planned_by_generated_parser,
        },
        .{
            .sql = "CREATE FOREIGN DATA WRAPPER usage_fdw HANDLER usage_fdw_handler",
            .kind = .create_foreign_data_wrapper,
            .reason = .create_foreign_data_wrapper_not_planned_by_generated_parser,
        },
        .{
            .sql = "CREATE LANGUAGE usage_lang",
            .kind = .create_language,
            .reason = .create_language_not_planned_by_generated_parser,
        },
        .{
            .sql = "CREATE OPERATOR CLASS usage_ops DEFAULT FOR TYPE text USING btree AS OPERATOR 1 < (text, text)",
            .kind = .create_operator_class,
            .reason = .create_operator_class_not_planned_by_generated_parser,
        },
        .{
            .sql = "CREATE OPERATOR FAMILY usage_family USING btree",
            .kind = .create_operator_family,
            .reason = .create_operator_family_not_planned_by_generated_parser,
        },
        .{
            .sql = "CREATE RULE usage_insert AS ON INSERT TO usage_records DO ALSO NOTIFY usage_events",
            .kind = .create_rule,
            .reason = .create_rule_not_planned_by_generated_parser,
        },
        .{
            .sql = "CREATE SCHEMA analytics AUTHORIZATION app_user",
            .kind = .create_schema_options,
            .reason = .create_schema_options_not_planned_by_generated_parser,
        },
        .{
            .sql = "CREATE SERVER usage_server FOREIGN DATA WRAPPER postgres_fdw",
            .kind = .create_server,
            .reason = .create_server_not_planned_by_generated_parser,
        },
        .{
            .sql = "CREATE STATISTICS usage_stats ON tenant_id, status FROM usage_records",
            .kind = .create_statistics,
            .reason = .create_statistics_not_planned_by_generated_parser,
        },
        .{
            .sql = "CREATE TEXT SEARCH CONFIGURATION usage_search (COPY = pg_catalog.english)",
            .kind = .create_text_search_configuration,
            .reason = .create_text_search_configuration_not_planned_by_generated_parser,
        },
        .{
            .sql = "CREATE TEXT SEARCH DICTIONARY usage_dict (TEMPLATE = simple)",
            .kind = .create_text_search_dictionary,
            .reason = .create_text_search_dictionary_not_planned_by_generated_parser,
        },
        .{
            .sql = "CREATE TEXT SEARCH PARSER usage_parser (START = prsd_start)",
            .kind = .create_text_search_parser,
            .reason = .create_text_search_parser_not_planned_by_generated_parser,
        },
        .{
            .sql = "CREATE TEXT SEARCH TEMPLATE usage_template (LEXIZE = dsimple_lexize)",
            .kind = .create_text_search_template,
            .reason = .create_text_search_template_not_planned_by_generated_parser,
        },
        .{
            .sql = "CREATE TRANSFORM FOR jsonb LANGUAGE plpgsql FROM SQL WITH FUNCTION jsonb_to_plpgsql(internal)",
            .kind = .create_transform,
            .reason = .create_transform_not_planned_by_generated_parser,
        },
        .{
            .sql = "CREATE TRIGGER usage_audit BEFORE INSERT ON usage_records FOR EACH ROW EXECUTE FUNCTION audit_usage()",
            .kind = .create_trigger,
            .reason = .create_trigger_not_planned_by_generated_parser,
        },
        .{
            .sql = "CREATE USER MAPPING FOR usage_user SERVER usage_server OPTIONS (user 'remote')",
            .kind = .create_user_mapping,
            .reason = .create_user_mapping_not_planned_by_generated_parser,
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
            .sql = "DROP FOREIGN DATA WRAPPER IF EXISTS usage_fdw CASCADE",
            .kind = .drop_foreign_data_wrapper,
            .reason = .drop_foreign_data_wrapper_not_planned_by_generated_parser,
        },
        .{
            .sql = "IMPORT FOREIGN SCHEMA public FROM SERVER usage_server INTO local_schema",
            .kind = .import_foreign_schema,
            .reason = .import_foreign_schema_not_planned_by_generated_parser,
        },
        .{
            .sql = "DROP ACCESS METHOD IF EXISTS usage_am",
            .kind = .drop_access_method,
            .reason = .drop_access_method_not_planned_by_generated_parser,
        },
        .{
            .sql = "DROP CONVERSION IF EXISTS usage_conv",
            .kind = .drop_conversion,
            .reason = .drop_conversion_not_planned_by_generated_parser,
        },
        .{
            .sql = "DROP EVENT TRIGGER IF EXISTS usage_ddl_start",
            .kind = .drop_event_trigger,
            .reason = .drop_event_trigger_not_planned_by_generated_parser,
        },
        .{
            .sql = "DROP COLLATION case_insensitive, accent_insensitive",
            .kind = .drop_collation_multi,
            .reason = .drop_collation_multi_not_planned_by_generated_parser,
        },
        .{
            .sql = "DROP DOMAIN positive_amount, nonempty_text CASCADE",
            .kind = .drop_domain_multi,
            .reason = .drop_domain_multi_not_planned_by_generated_parser,
        },
        .{
            .sql = "DROP EXTENSION vector, postgis",
            .kind = .drop_extension_multi,
            .reason = .drop_extension_multi_not_planned_by_generated_parser,
        },
        .{
            .sql = "DROP INDEX usage_status_idx, usage_tenant_idx CASCADE",
            .kind = .drop_index_multi,
            .reason = .drop_index_multi_not_planned_by_generated_parser,
        },
        .{
            .sql = "DROP INDEX CONCURRENTLY usage_status_idx, usage_tenant_idx",
            .kind = .drop_index_multi,
            .reason = .drop_index_multi_not_planned_by_generated_parser,
        },
        .{
            .sql = "DROP LANGUAGE IF EXISTS usage_lang CASCADE",
            .kind = .drop_language,
            .reason = .drop_language_not_planned_by_generated_parser,
        },
        .{
            .sql = "DROP MATERIALIZED VIEW usage_summary, old_usage_summary CASCADE",
            .kind = .drop_materialized_view_multi,
            .reason = .drop_materialized_view_multi_not_planned_by_generated_parser,
        },
        .{
            .sql = "DROP OWNED BY usage_role CASCADE",
            .kind = .drop_owned,
            .reason = .drop_owned_not_planned_by_generated_parser,
        },
        .{
            .sql = "DROP OPERATOR CLASS IF EXISTS usage_ops USING btree",
            .kind = .drop_operator_class,
            .reason = .drop_operator_class_not_planned_by_generated_parser,
        },
        .{
            .sql = "DROP OPERATOR FAMILY IF EXISTS usage_family USING btree",
            .kind = .drop_operator_family,
            .reason = .drop_operator_family_not_planned_by_generated_parser,
        },
        .{
            .sql = "DROP PUBLICATION usage_pub, old_usage_pub",
            .kind = .drop_publication_multi,
            .reason = .drop_publication_multi_not_planned_by_generated_parser,
        },
        .{
            .sql = "DROP ROUTINE IF EXISTS normalize_status(text) CASCADE",
            .kind = .drop_routine,
            .reason = .drop_routine_not_planned_by_generated_parser,
        },
        .{
            .sql = "DROP ROLE app_writer, app_reader",
            .kind = .drop_role_multi,
            .reason = .drop_role_multi_not_planned_by_generated_parser,
        },
        .{
            .sql = "DROP SCHEMA analytics, reporting",
            .kind = .drop_schema_multi,
            .reason = .drop_schema_multi_not_planned_by_generated_parser,
        },
        .{
            .sql = "DROP SEQUENCE order_id_seq, old_order_id_seq CASCADE",
            .kind = .drop_sequence_multi,
            .reason = .drop_sequence_multi_not_planned_by_generated_parser,
        },
        .{
            .sql = "DROP STATISTICS IF EXISTS usage_stats",
            .kind = .drop_statistics,
            .reason = .drop_statistics_not_planned_by_generated_parser,
        },
        .{
            .sql = "DROP TABLE usage_records, archived_usage_records",
            .kind = .drop_table_multi,
            .reason = .drop_table_multi_not_planned_by_generated_parser,
        },
        .{
            .sql = "DROP TEXT SEARCH CONFIGURATION IF EXISTS usage_search",
            .kind = .drop_text_search_configuration,
            .reason = .drop_text_search_configuration_not_planned_by_generated_parser,
        },
        .{
            .sql = "DROP TEXT SEARCH DICTIONARY IF EXISTS usage_dict",
            .kind = .drop_text_search_dictionary,
            .reason = .drop_text_search_dictionary_not_planned_by_generated_parser,
        },
        .{
            .sql = "DROP TEXT SEARCH PARSER IF EXISTS usage_parser",
            .kind = .drop_text_search_parser,
            .reason = .drop_text_search_parser_not_planned_by_generated_parser,
        },
        .{
            .sql = "DROP TEXT SEARCH TEMPLATE IF EXISTS usage_template",
            .kind = .drop_text_search_template,
            .reason = .drop_text_search_template_not_planned_by_generated_parser,
        },
        .{
            .sql = "DROP TRANSFORM FOR jsonb LANGUAGE plpgsql",
            .kind = .drop_transform,
            .reason = .drop_transform_not_planned_by_generated_parser,
        },
        .{
            .sql = "DROP TYPE usage_status, archived_status CASCADE",
            .kind = .drop_type_multi,
            .reason = .drop_type_multi_not_planned_by_generated_parser,
        },
        .{
            .sql = "DROP VIEW active_usage, archived_usage",
            .kind = .drop_view_multi,
            .reason = .drop_view_multi_not_planned_by_generated_parser,
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
            .sql = "MATCH (doc) RETURN doc",
            .kind = .graph_query,
            .reason = .graph_query_not_planned_by_generated_parser,
        },
        .{
            .sql = "NOTIFY usage_events, 'changed'",
            .kind = .notify,
            .reason = .notify_not_planned_by_generated_parser,
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
            .sql = "REASSIGN OWNED BY old_role TO new_role",
            .kind = .reassign_owned,
            .reason = .reassign_owned_not_planned_by_generated_parser,
        },
        .{
            .sql = "REVOKE SELECT ON TABLE usage_records FROM readonly",
            .kind = .revoke,
            .reason = .revoke_not_planned_by_generated_parser,
        },
        .{
            .sql = "SET ROLE app_user",
            .kind = .role_session_control,
            .reason = .role_session_control_not_planned_by_generated_parser,
        },
        .{
            .sql = "SET ROLE DEFAULT",
            .kind = .role_session_control,
            .reason = .role_session_control_not_planned_by_generated_parser,
        },
        .{
            .sql = "SET SESSION AUTHORIZATION app_user",
            .kind = .role_session_control,
            .reason = .role_session_control_not_planned_by_generated_parser,
        },
        .{
            .sql = "SET SESSION AUTHORIZATION DEFAULT",
            .kind = .role_session_control,
            .reason = .role_session_control_not_planned_by_generated_parser,
        },
        .{
            .sql = "RESET ROLE",
            .kind = .role_session_control,
            .reason = .role_session_control_not_planned_by_generated_parser,
        },
        .{
            .sql = "RESET SESSION AUTHORIZATION",
            .kind = .role_session_control,
            .reason = .role_session_control_not_planned_by_generated_parser,
        },
        .{
            .sql = "SECURITY LABEL ON TABLE usage_records IS 'internal'",
            .kind = .security_label,
            .reason = .security_label_not_planned_by_generated_parser,
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
            .sql = "DROP TRIGGER IF EXISTS usage_audit ON usage_records",
            .kind = .drop_trigger,
            .reason = .drop_trigger_not_planned_by_generated_parser,
        },
        .{
            .sql = "DROP USER MAPPING IF EXISTS FOR usage_user SERVER usage_server",
            .kind = .drop_user_mapping,
            .reason = .drop_user_mapping_not_planned_by_generated_parser,
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
        const generated_kind = parsed.generatedStatementKind() orelse {
            std.debug.print("missing generated unsupported statement for SQL: {s}\n", .{case.sql});
            if (try generated_parser.diagnosticAlloc(alloc, parsed.items())) |diagnostic| {
                defer alloc.free(diagnostic.expected);
                std.debug.print("generated diagnostic at token {d} actual {s}; expected:", .{ diagnostic.token_index, diagnostic.actual });
                for (diagnostic.expected) |expected| std.debug.print(" {s}", .{expected});
                std.debug.print("\n", .{});
            }
            return error.TestUnexpectedResult;
        };
        try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.unsupported, generated_kind);
        switch (parsed.generated_statement.?.ast.?) {
            .unsupported => |unsupported| {
                try std.testing.expectEqual(case.kind, unsupported.kind);
                try std.testing.expectEqual(case.reason, unsupported.reason);
                if (parsed.items().len == 1) {
                    try std.testing.expect(unsupported.subject_tokens == null);
                } else {
                    const expected_subject_start: usize = switch (case.kind) {
                        .create_trigger,
                        .drop_trigger,
                        => 2,
                        else => 1,
                    };
                    try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = expected_subject_start, .end = parsed.items().len }, unsupported.subject_tokens.?);
                }
            },
            else => return error.TestUnexpectedResult,
        }
        if (generatedUnsupportedUsesDdlPlanBoundary(case.kind)) {
            switch (parsed.statement) {
                .ddl => {},
                else => return error.TestUnexpectedResult,
            }
        } else if (case.kind == .explain) {
            switch (parsed.statement) {
                .explain => {},
                else => return error.TestUnexpectedResult,
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

    const strict_unsupported_rejections = [_][]const u8{
        "ALTER TRANSFORM FOR",
        "DROP ROUTINE IF EXISTS",
        "CREATE TEXT SEARCH",
        "IMPORT FOREIGN SCHEMA public FROM SERVER",
    };
    for (strict_unsupported_rejections) |sql| {
        try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, sql));
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

    var local_session = try ParsedSql.initAlloc(alloc, "SET LOCAL antfly.sync_level = 'propose'");
    defer local_session.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.session, local_session.generatedStatementKind().?);
    switch (local_session.generated_statement.?.ast.?) {
        .session => |generated_session| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlSessionKind.set, generated_session.kind);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 2, .end = 3 }, generated_session.name_tokens.?);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 4, .end = 5 }, generated_session.value_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    var reset_all = try ParsedSql.initAlloc(alloc, "RESET ALL");
    defer reset_all.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.session, reset_all.generatedStatementKind().?);
    switch (reset_all.generated_statement.?.ast.?) {
        .session => |generated_session| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlSessionKind.reset, generated_session.kind);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 2 }, generated_session.name_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    var show_all = try ParsedSql.initAlloc(alloc, "SHOW ALL");
    defer show_all.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.session, show_all.generatedStatementKind().?);
    switch (show_all.generated_statement.?.ast.?) {
        .session => |generated_session| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlSessionKind.show, generated_session.kind);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 2 }, generated_session.name_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    var transaction = try ParsedSql.initAlloc(alloc, "COMMIT WORK");
    defer transaction.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.transaction, transaction.generatedStatementKind().?);
    switch (transaction.generated_statement.?.ast.?) {
        .transaction => |generated_transaction| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlTransactionKind.commit, generated_transaction.kind);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 2 }, generated_transaction.boundary_tail_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (transaction.statement) {
        .transaction => {},
        else => return error.TestUnexpectedResult,
    }

    var end_transaction = try ParsedSql.initAlloc(alloc, "END WORK");
    defer end_transaction.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.transaction, end_transaction.generatedStatementKind().?);
    switch (end_transaction.generated_statement.?.ast.?) {
        .transaction => |generated_transaction| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlTransactionKind.commit, generated_transaction.kind);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 2 }, generated_transaction.boundary_tail_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (end_transaction.statement) {
        .transaction => {},
        else => return error.TestUnexpectedResult,
    }

    var transaction_mode = try ParsedSql.initAlloc(alloc, "SET TRANSACTION READ ONLY");
    defer transaction_mode.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.transaction, transaction_mode.generatedStatementKind().?);
    switch (transaction_mode.generated_statement.?.ast.?) {
        .transaction => |generated_transaction| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlTransactionKind.set_transaction, generated_transaction.kind);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 4 }, generated_transaction.mode_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    var savepoint = try ParsedSql.initAlloc(alloc, "SAVEPOINT before_retry");
    defer savepoint.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.transaction, savepoint.generatedStatementKind().?);
    switch (savepoint.generated_statement.?.ast.?) {
        .transaction => |generated_transaction| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlTransactionKind.savepoint, generated_transaction.kind);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 2 }, generated_transaction.name_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (savepoint.statement) {
        .transaction => {},
        else => return error.TestUnexpectedResult,
    }

    var rollback_to_savepoint = try ParsedSql.initAlloc(alloc, "ROLLBACK TO before_retry");
    defer rollback_to_savepoint.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.transaction, rollback_to_savepoint.generatedStatementKind().?);
    switch (rollback_to_savepoint.generated_statement.?.ast.?) {
        .transaction => |generated_transaction| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlTransactionKind.rollback_to_savepoint, generated_transaction.kind);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 2, .end = 3 }, generated_transaction.name_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    var cursor = try ParsedSql.initAlloc(alloc, "FETCH FROM usage_cursor");
    defer cursor.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.cursor, cursor.generatedStatementKind().?);
    switch (cursor.generated_statement.?.ast.?) {
        .cursor => |generated_cursor| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlCursorKind.fetch, generated_cursor.kind);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 3 }, generated_cursor.tail_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (cursor.statement) {
        .ddl => {},
        else => return error.TestUnexpectedResult,
    }

    var move_cursor = try ParsedSql.initAlloc(alloc, "MOVE FORWARD 10 IN usage_cursor");
    defer move_cursor.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.cursor, move_cursor.generatedStatementKind().?);
    switch (move_cursor.generated_statement.?.ast.?) {
        .cursor => |generated_cursor| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlCursorKind.move, generated_cursor.kind);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 1, .end = 5 }, generated_cursor.tail_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (move_cursor.statement) {
        .ddl => {},
        else => return error.TestUnexpectedResult,
    }

    var session_characteristics = try ParsedSql.initAlloc(alloc, "SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE");
    defer session_characteristics.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.session, session_characteristics.generatedStatementKind().?);
    switch (session_characteristics.statement) {
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

    var deallocate_prepare = try ParsedSql.initAlloc(alloc, "DEALLOCATE PREPARE read_stmt");
    defer deallocate_prepare.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.prepared, deallocate_prepare.generatedStatementKind().?);
    switch (deallocate_prepare.statement) {
        .prepared => |statement| try std.testing.expectEqualStrings("DEALLOCATE PREPARE read_stmt", statement.raw.sql(deallocate_prepare.sql())),
        else => return error.TestUnexpectedResult,
    }

    var prepare_transaction = try ParsedSql.initAlloc(alloc, "PREPARE TRANSACTION 'usage_batch'");
    defer prepare_transaction.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.prepared_transaction, prepare_transaction.generatedStatementKind().?);
    switch (prepare_transaction.statement) {
        .ddl => {},
        else => return error.TestUnexpectedResult,
    }
    switch (prepare_transaction.generated_statement.?.ast.?) {
        .prepared_transaction => |generated| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlPreparedTransactionKind.prepare, generated.kind);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 2, .end = 3 }, generated.gid_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    var commit_prepared = try ParsedSql.initAlloc(alloc, "COMMIT PREPARED 'usage_batch'");
    defer commit_prepared.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.prepared_transaction, commit_prepared.generatedStatementKind().?);
    switch (commit_prepared.statement) {
        .ddl => {},
        else => return error.TestUnexpectedResult,
    }
    switch (commit_prepared.generated_statement.?.ast.?) {
        .prepared_transaction => |generated| try std.testing.expectEqual(generated_parser.GeneratedSqlPreparedTransactionKind.commit, generated.kind),
        else => return error.TestUnexpectedResult,
    }

    var rollback_prepared = try ParsedSql.initAlloc(alloc, "ROLLBACK PREPARED 'usage_batch'");
    defer rollback_prepared.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.prepared_transaction, rollback_prepared.generatedStatementKind().?);
    switch (rollback_prepared.statement) {
        .ddl => {},
        else => return error.TestUnexpectedResult,
    }
    switch (rollback_prepared.generated_statement.?.ast.?) {
        .prepared_transaction => |generated| try std.testing.expectEqual(generated_parser.GeneratedSqlPreparedTransactionKind.rollback, generated.kind),
        else => return error.TestUnexpectedResult,
    }

    var ddl = try ParsedSql.initAlloc(alloc, "CREATE TABLE usage_records (id text)");
    defer ddl.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.ddl, ddl.generatedStatementKind().?);
    switch (ddl.statement) {
        .ddl => {},
        else => return error.TestUnexpectedResult,
    }

    var create_function = try ParsedSql.initAlloc(alloc, "CREATE OR REPLACE FUNCTION touch_updated_at() RETURNS trigger LANGUAGE plpgsql");
    defer create_function.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.ddl, create_function.generatedStatementKind().?);
    switch (create_function.generated_statement.?.ast.?) {
        .ddl => |ddl_ast| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlDdlKind.create_function, ddl_ast.kind);
            try std.testing.expect(ddl_ast.replace_existing);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 4, .end = 5 }, ddl_ast.object_name_tokens.?);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 11 }, ddl_ast.alter_table_operation_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (create_function.statement) {
        .ddl => {},
        else => return error.TestUnexpectedResult,
    }

    var drop_function = try ParsedSql.initAlloc(alloc, "DROP FUNCTION IF EXISTS audit_changes(text) CASCADE");
    defer drop_function.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.ddl, drop_function.generatedStatementKind().?);
    switch (drop_function.generated_statement.?.ast.?) {
        .ddl => |ddl_ast| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlDdlKind.drop_function, ddl_ast.kind);
            try std.testing.expect(ddl_ast.if_exists);
            try std.testing.expect(ddl_ast.cascade);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 4, .end = 5 }, ddl_ast.object_name_tokens.?);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 9 }, ddl_ast.alter_table_operation_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (drop_function.statement) {
        .ddl => {},
        else => return error.TestUnexpectedResult,
    }

    var create_procedure = try ParsedSql.initAlloc(alloc, "CREATE PROCEDURE rotate_usage() LANGUAGE plpgsql");
    defer create_procedure.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.ddl, create_procedure.generatedStatementKind().?);
    switch (create_procedure.generated_statement.?.ast.?) {
        .ddl => |ddl_ast| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlDdlKind.create_procedure, ddl_ast.kind);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 2, .end = 3 }, ddl_ast.object_name_tokens.?);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 7 }, ddl_ast.alter_table_operation_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (create_procedure.statement) {
        .ddl => {},
        else => return error.TestUnexpectedResult,
    }

    var drop_procedure = try ParsedSql.initAlloc(alloc, "DROP PROCEDURE rotate_usage()");
    defer drop_procedure.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.ddl, drop_procedure.generatedStatementKind().?);
    switch (drop_procedure.generated_statement.?.ast.?) {
        .ddl => |ddl_ast| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlDdlKind.drop_procedure, ddl_ast.kind);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 2, .end = 3 }, ddl_ast.object_name_tokens.?);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 5 }, ddl_ast.alter_table_operation_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (drop_procedure.statement) {
        .ddl => {},
        else => return error.TestUnexpectedResult,
    }

    var create_role = try ParsedSql.initAlloc(alloc, "CREATE ROLE app_writer");
    defer create_role.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.ddl, create_role.generatedStatementKind().?);
    switch (create_role.generated_statement.?.ast.?) {
        .ddl => |ddl_ast| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlDdlKind.create_role, ddl_ast.kind);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 2, .end = 3 }, ddl_ast.object_name_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (create_role.statement) {
        .ddl => {},
        else => return error.TestUnexpectedResult,
    }

    var create_user = try ParsedSql.initAlloc(alloc, "CREATE USER app_writer");
    defer create_user.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.ddl, create_user.generatedStatementKind().?);
    switch (create_user.generated_statement.?.ast.?) {
        .ddl => |ddl_ast| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlDdlKind.create_role, ddl_ast.kind);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 2, .end = 3 }, ddl_ast.object_name_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    var create_group = try ParsedSql.initAlloc(alloc, "CREATE GROUP app_readers");
    defer create_group.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.ddl, create_group.generatedStatementKind().?);
    switch (create_group.generated_statement.?.ast.?) {
        .ddl => |ddl_ast| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlDdlKind.create_role, ddl_ast.kind);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 2, .end = 3 }, ddl_ast.object_name_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    var alter_role = try ParsedSql.initAlloc(alloc, "ALTER ROLE app_writer IN DATABASE appdb SET app.tenant_id = current_setting('app.tenant_id')");
    defer alter_role.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.ddl, alter_role.generatedStatementKind().?);
    switch (alter_role.generated_statement.?.ast.?) {
        .ddl => |ddl_ast| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlDdlKind.alter_role, ddl_ast.kind);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 2, .end = 3 }, ddl_ast.object_name_tokens.?);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 13 }, ddl_ast.alter_table_operation_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (alter_role.statement) {
        .ddl => {},
        else => return error.TestUnexpectedResult,
    }

    var alter_user = try ParsedSql.initAlloc(alloc, "ALTER USER app_writer RESET statement_timeout");
    defer alter_user.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.ddl, alter_user.generatedStatementKind().?);
    switch (alter_user.generated_statement.?.ast.?) {
        .ddl => |ddl_ast| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlDdlKind.alter_role, ddl_ast.kind);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 2, .end = 3 }, ddl_ast.object_name_tokens.?);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 5 }, ddl_ast.alter_table_operation_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    var alter_group = try ParsedSql.initAlloc(alloc, "ALTER GROUP app_readers RESET statement_timeout");
    defer alter_group.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.ddl, alter_group.generatedStatementKind().?);
    switch (alter_group.generated_statement.?.ast.?) {
        .ddl => |ddl_ast| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlDdlKind.alter_role, ddl_ast.kind);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 2, .end = 3 }, ddl_ast.object_name_tokens.?);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 5 }, ddl_ast.alter_table_operation_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    var drop_role = try ParsedSql.initAlloc(alloc, "DROP ROLE IF EXISTS app_writer");
    defer drop_role.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.ddl, drop_role.generatedStatementKind().?);
    switch (drop_role.generated_statement.?.ast.?) {
        .ddl => |ddl_ast| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlDdlKind.drop_role, ddl_ast.kind);
            try std.testing.expect(ddl_ast.if_exists);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 4, .end = 5 }, ddl_ast.object_name_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (drop_role.statement) {
        .ddl => {},
        else => return error.TestUnexpectedResult,
    }

    var drop_user = try ParsedSql.initAlloc(alloc, "DROP USER IF EXISTS app_writer");
    defer drop_user.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.ddl, drop_user.generatedStatementKind().?);
    switch (drop_user.generated_statement.?.ast.?) {
        .ddl => |ddl_ast| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlDdlKind.drop_role, ddl_ast.kind);
            try std.testing.expect(ddl_ast.if_exists);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 4, .end = 5 }, ddl_ast.object_name_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    var drop_group = try ParsedSql.initAlloc(alloc, "DROP GROUP IF EXISTS app_readers");
    defer drop_group.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.ddl, drop_group.generatedStatementKind().?);
    switch (drop_group.generated_statement.?.ast.?) {
        .ddl => |ddl_ast| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlDdlKind.drop_role, ddl_ast.kind);
            try std.testing.expect(ddl_ast.if_exists);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 4, .end = 5 }, ddl_ast.object_name_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    var create_materialized_view = try ParsedSql.initAlloc(alloc, "CREATE MATERIALIZED VIEW IF NOT EXISTS usage_summary AS SELECT status FROM usage_records WITH NO DATA");
    defer create_materialized_view.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.ddl, create_materialized_view.generatedStatementKind().?);
    switch (create_materialized_view.generated_statement.?.ast.?) {
        .ddl => |ddl_ast| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlDdlKind.create_materialized_view, ddl_ast.kind);
            try std.testing.expect(ddl_ast.if_not_exists);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, ddl_ast.object_name_tokens.?);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 15 }, ddl_ast.alter_table_operation_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (create_materialized_view.statement) {
        .ddl => {},
        else => return error.TestUnexpectedResult,
    }

    var drop_materialized_view = try ParsedSql.initAlloc(alloc, "DROP MATERIALIZED VIEW IF EXISTS usage_summary CASCADE");
    defer drop_materialized_view.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.ddl, drop_materialized_view.generatedStatementKind().?);
    switch (drop_materialized_view.generated_statement.?.ast.?) {
        .ddl => |ddl_ast| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlDdlKind.drop_materialized_view, ddl_ast.kind);
            try std.testing.expect(ddl_ast.if_exists);
            try std.testing.expect(ddl_ast.cascade);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, ddl_ast.object_name_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (drop_materialized_view.statement) {
        .ddl => {},
        else => return error.TestUnexpectedResult,
    }

    var refresh_materialized_view = try ParsedSql.initAlloc(alloc, "REFRESH MATERIALIZED VIEW CONCURRENTLY usage_summary WITH NO DATA");
    defer refresh_materialized_view.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.ddl, refresh_materialized_view.generatedStatementKind().?);
    switch (refresh_materialized_view.generated_statement.?.ast.?) {
        .ddl => |ddl_ast| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlDdlKind.refresh_materialized_view, ddl_ast.kind);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 4, .end = 5 }, ddl_ast.object_name_tokens.?);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 8 }, ddl_ast.alter_table_operation_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (refresh_materialized_view.statement) {
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

    var antfly_extension_index = try ParsedSql.initAlloc(alloc, "CREATE INDEX docs_body_fts ON docs USING antfly_full_text (body) WITH (analyzer = 'standard')");
    defer antfly_extension_index.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.extension_index, antfly_extension_index.generatedStatementKind().?);
    switch (antfly_extension_index.generated_statement.?.ast.?) {
        .extension_index => |extension_index_ast| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlDdlKind.create_index, extension_index_ast.kind);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 2, .end = 3 }, extension_index_ast.object_name_tokens.?);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 4, .end = 5 }, extension_index_ast.index_table_tokens.?);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, extension_index_ast.index_method_tokens.?);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 8, .end = 9 }, extension_index_ast.index_elements_tokens.?);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 10, .end = 15 }, extension_index_ast.index_options_tokens.?);
            try std.testing.expect(!extension_index_ast.unique);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (antfly_extension_index.statement) {
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

    var create_sequence = try ParsedSql.initAlloc(alloc, "CREATE SEQUENCE IF NOT EXISTS order_id_seq AS bigint START WITH 10");
    defer create_sequence.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.ddl, create_sequence.generatedStatementKind().?);
    switch (create_sequence.generated_statement.?.ast.?) {
        .ddl => |ddl_ast| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlDdlKind.create_sequence, ddl_ast.kind);
            try std.testing.expect(ddl_ast.if_not_exists);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, ddl_ast.object_name_tokens.?);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 11 }, ddl_ast.alter_table_operation_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (create_sequence.statement) {
        .ddl => {},
        else => return error.TestUnexpectedResult,
    }

    var alter_sequence = try ParsedSql.initAlloc(alloc, "ALTER SEQUENCE IF EXISTS order_id_seq RESTART WITH 1000");
    defer alter_sequence.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.ddl, alter_sequence.generatedStatementKind().?);
    switch (alter_sequence.generated_statement.?.ast.?) {
        .ddl => |ddl_ast| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlDdlKind.alter_sequence, ddl_ast.kind);
            try std.testing.expect(ddl_ast.if_exists);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 4, .end = 5 }, ddl_ast.object_name_tokens.?);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 8 }, ddl_ast.alter_table_operation_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (alter_sequence.statement) {
        .ddl => {},
        else => return error.TestUnexpectedResult,
    }

    var drop_sequence = try ParsedSql.initAlloc(alloc, "DROP SEQUENCE IF EXISTS order_id_seq CASCADE");
    defer drop_sequence.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.ddl, drop_sequence.generatedStatementKind().?);
    switch (drop_sequence.generated_statement.?.ast.?) {
        .ddl => |ddl_ast| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlDdlKind.drop_sequence, ddl_ast.kind);
            try std.testing.expect(ddl_ast.if_exists);
            try std.testing.expect(ddl_ast.cascade);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 4, .end = 5 }, ddl_ast.object_name_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (drop_sequence.statement) {
        .ddl => {},
        else => return error.TestUnexpectedResult,
    }

    var create_enum_type = try ParsedSql.initAlloc(alloc, "CREATE TYPE usage_status AS ENUM ('open', 'done')");
    defer create_enum_type.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.ddl, create_enum_type.generatedStatementKind().?);
    switch (create_enum_type.generated_statement.?.ast.?) {
        .ddl => |ddl_ast| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlDdlKind.create_enum_type, ddl_ast.kind);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 2, .end = 3 }, ddl_ast.object_name_tokens.?);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 10 }, ddl_ast.alter_table_operation_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (create_enum_type.statement) {
        .ddl => {},
        else => return error.TestUnexpectedResult,
    }

    var alter_enum_type = try ParsedSql.initAlloc(alloc, "ALTER TYPE usage_status ADD VALUE IF NOT EXISTS 'archived' AFTER 'done'");
    defer alter_enum_type.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.ddl, alter_enum_type.generatedStatementKind().?);
    switch (alter_enum_type.generated_statement.?.ast.?) {
        .ddl => |ddl_ast| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlDdlKind.alter_enum_type, ddl_ast.kind);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 2, .end = 3 }, ddl_ast.object_name_tokens.?);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 11 }, ddl_ast.alter_table_operation_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (alter_enum_type.statement) {
        .ddl => {},
        else => return error.TestUnexpectedResult,
    }

    var drop_enum_type = try ParsedSql.initAlloc(alloc, "DROP TYPE IF EXISTS usage_status CASCADE");
    defer drop_enum_type.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.ddl, drop_enum_type.generatedStatementKind().?);
    switch (drop_enum_type.generated_statement.?.ast.?) {
        .ddl => |ddl_ast| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlDdlKind.drop_enum_type, ddl_ast.kind);
            try std.testing.expect(ddl_ast.if_exists);
            try std.testing.expect(ddl_ast.cascade);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 4, .end = 5 }, ddl_ast.object_name_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (drop_enum_type.statement) {
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

    var create_index_concurrently = try ParsedSql.initAlloc(alloc, "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS usage_status_idx ON usage_records (lower(status))");
    defer create_index_concurrently.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.extension_index, create_index_concurrently.generatedStatementKind().?);
    switch (create_index_concurrently.generated_statement.?.ast.?) {
        .extension_index => |ddl_ast| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlDdlKind.create_index, ddl_ast.kind);
            try std.testing.expect(ddl_ast.unique);
            try std.testing.expect(ddl_ast.if_not_exists);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 7, .end = 8 }, ddl_ast.object_name_tokens.?);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 9, .end = 10 }, ddl_ast.index_table_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    var drop_index_concurrently = try ParsedSql.initAlloc(alloc, "DROP INDEX CONCURRENTLY IF EXISTS usage_status_idx RESTRICT");
    defer drop_index_concurrently.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.extension_index, drop_index_concurrently.generatedStatementKind().?);
    switch (drop_index_concurrently.generated_statement.?.ast.?) {
        .extension_index => |ddl_ast| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlDdlKind.drop_index, ddl_ast.kind);
            try std.testing.expect(ddl_ast.if_exists);
            try std.testing.expect(!ddl_ast.cascade);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 6 }, ddl_ast.object_name_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (drop_index_concurrently.statement) {
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

    var alter_schema = try ParsedSql.initAlloc(alloc, "ALTER SCHEMA analytics RENAME TO reporting");
    defer alter_schema.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.ddl, alter_schema.generatedStatementKind().?);
    switch (alter_schema.generated_statement.?.ast.?) {
        .ddl => |ddl_ast| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlDdlKind.alter_schema, ddl_ast.kind);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 2, .end = 3 }, ddl_ast.object_name_tokens.?);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 6 }, ddl_ast.alter_table_operation_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (alter_schema.statement) {
        .ddl => {},
        else => return error.TestUnexpectedResult,
    }

    var create_tablespace = try ParsedSql.initAlloc(alloc, "CREATE TABLESPACE fastspace LOCATION '/var/lib/antfly/fastspace'");
    defer create_tablespace.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.ddl, create_tablespace.generatedStatementKind().?);
    switch (create_tablespace.generated_statement.?.ast.?) {
        .ddl => |ddl_ast| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlDdlKind.create_tablespace, ddl_ast.kind);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 2, .end = 3 }, ddl_ast.object_name_tokens.?);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 5 }, ddl_ast.alter_table_operation_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (create_tablespace.statement) {
        .ddl => {},
        else => return error.TestUnexpectedResult,
    }

    var alter_tablespace = try ParsedSql.initAlloc(alloc, "ALTER TABLESPACE fastspace RENAME TO fastspace_archive");
    defer alter_tablespace.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.ddl, alter_tablespace.generatedStatementKind().?);
    switch (alter_tablespace.generated_statement.?.ast.?) {
        .ddl => |ddl_ast| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlDdlKind.alter_tablespace, ddl_ast.kind);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 2, .end = 3 }, ddl_ast.object_name_tokens.?);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 6 }, ddl_ast.alter_table_operation_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (alter_tablespace.statement) {
        .ddl => {},
        else => return error.TestUnexpectedResult,
    }

    var drop_tablespace = try ParsedSql.initAlloc(alloc, "DROP TABLESPACE IF EXISTS fastspace_archive");
    defer drop_tablespace.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.ddl, drop_tablespace.generatedStatementKind().?);
    switch (drop_tablespace.generated_statement.?.ast.?) {
        .ddl => |ddl_ast| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlDdlKind.drop_tablespace, ddl_ast.kind);
            try std.testing.expect(ddl_ast.if_exists);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 4, .end = 5 }, ddl_ast.object_name_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (drop_tablespace.statement) {
        .ddl => {},
        else => return error.TestUnexpectedResult,
    }

    var create_publication = try ParsedSql.initAlloc(alloc, "CREATE PUBLICATION usage_pub FOR TABLE usage_records");
    defer create_publication.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.ddl, create_publication.generatedStatementKind().?);
    switch (create_publication.generated_statement.?.ast.?) {
        .ddl => |ddl_ast| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlDdlKind.create_publication, ddl_ast.kind);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 2, .end = 3 }, ddl_ast.object_name_tokens.?);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 6 }, ddl_ast.alter_table_operation_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (create_publication.statement) {
        .ddl => {},
        else => return error.TestUnexpectedResult,
    }

    var alter_publication = try ParsedSql.initAlloc(alloc, "ALTER PUBLICATION usage_pub ADD TABLE usage_events");
    defer alter_publication.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.ddl, alter_publication.generatedStatementKind().?);
    switch (alter_publication.generated_statement.?.ast.?) {
        .ddl => |ddl_ast| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlDdlKind.alter_publication, ddl_ast.kind);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 2, .end = 3 }, ddl_ast.object_name_tokens.?);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 6 }, ddl_ast.alter_table_operation_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (alter_publication.statement) {
        .ddl => {},
        else => return error.TestUnexpectedResult,
    }

    var drop_publication = try ParsedSql.initAlloc(alloc, "DROP PUBLICATION IF EXISTS usage_pub");
    defer drop_publication.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.ddl, drop_publication.generatedStatementKind().?);
    switch (drop_publication.generated_statement.?.ast.?) {
        .ddl => |ddl_ast| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlDdlKind.drop_publication, ddl_ast.kind);
            try std.testing.expect(ddl_ast.if_exists);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 4, .end = 5 }, ddl_ast.object_name_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (drop_publication.statement) {
        .ddl => {},
        else => return error.TestUnexpectedResult,
    }

    var create_subscription = try ParsedSql.initAlloc(alloc, "CREATE SUBSCRIPTION usage_sub CONNECTION 'host=db' PUBLICATION usage_pub");
    defer create_subscription.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.ddl, create_subscription.generatedStatementKind().?);
    switch (create_subscription.generated_statement.?.ast.?) {
        .ddl => |ddl_ast| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlDdlKind.create_subscription, ddl_ast.kind);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 2, .end = 3 }, ddl_ast.object_name_tokens.?);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 7 }, ddl_ast.alter_table_operation_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (create_subscription.statement) {
        .ddl => {},
        else => return error.TestUnexpectedResult,
    }

    var alter_subscription = try ParsedSql.initAlloc(alloc, "ALTER SUBSCRIPTION usage_sub DISABLE");
    defer alter_subscription.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.ddl, alter_subscription.generatedStatementKind().?);
    switch (alter_subscription.generated_statement.?.ast.?) {
        .ddl => |ddl_ast| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlDdlKind.alter_subscription, ddl_ast.kind);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 2, .end = 3 }, ddl_ast.object_name_tokens.?);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 3, .end = 4 }, ddl_ast.alter_table_operation_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (alter_subscription.statement) {
        .ddl => {},
        else => return error.TestUnexpectedResult,
    }

    var drop_subscription = try ParsedSql.initAlloc(alloc, "DROP SUBSCRIPTION IF EXISTS usage_sub");
    defer drop_subscription.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.ddl, drop_subscription.generatedStatementKind().?);
    switch (drop_subscription.generated_statement.?.ast.?) {
        .ddl => |ddl_ast| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlDdlKind.drop_subscription, ddl_ast.kind);
            try std.testing.expect(ddl_ast.if_exists);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 4, .end = 5 }, ddl_ast.object_name_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (drop_subscription.statement) {
        .ddl => {},
        else => return error.TestUnexpectedResult,
    }

    var create_policy = try ParsedSql.initAlloc(alloc, "CREATE POLICY usage_policy ON usage_records USING (tenant_id = current_user)");
    defer create_policy.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.ddl, create_policy.generatedStatementKind().?);
    switch (create_policy.generated_statement.?.ast.?) {
        .ddl => |ddl_ast| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlDdlKind.create_policy, ddl_ast.kind);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 2, .end = 3 }, ddl_ast.object_name_tokens.?);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 4, .end = 5 }, ddl_ast.index_table_tokens.?);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 11 }, ddl_ast.alter_table_operation_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (create_policy.statement) {
        .ddl => {},
        else => return error.TestUnexpectedResult,
    }

    var alter_policy = try ParsedSql.initAlloc(alloc, "ALTER POLICY usage_policy ON usage_records WITH CHECK (status = 'ready')");
    defer alter_policy.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.ddl, alter_policy.generatedStatementKind().?);
    switch (alter_policy.generated_statement.?.ast.?) {
        .ddl => |ddl_ast| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlDdlKind.alter_policy, ddl_ast.kind);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 2, .end = 3 }, ddl_ast.object_name_tokens.?);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 4, .end = 5 }, ddl_ast.index_table_tokens.?);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 5, .end = 12 }, ddl_ast.alter_table_operation_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (alter_policy.statement) {
        .ddl => {},
        else => return error.TestUnexpectedResult,
    }

    var drop_policy = try ParsedSql.initAlloc(alloc, "DROP POLICY IF EXISTS usage_policy ON usage_records CASCADE");
    defer drop_policy.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.ddl, drop_policy.generatedStatementKind().?);
    switch (drop_policy.generated_statement.?.ast.?) {
        .ddl => |ddl_ast| {
            try std.testing.expectEqual(generated_parser.GeneratedSqlDdlKind.drop_policy, ddl_ast.kind);
            try std.testing.expect(ddl_ast.if_exists);
            try std.testing.expect(ddl_ast.cascade);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 4, .end = 5 }, ddl_ast.object_name_tokens.?);
            try std.testing.expectEqual(generated_parser.GeneratedSqlTokenRange{ .start = 6, .end = 7 }, ddl_ast.index_table_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (drop_policy.statement) {
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

test "sql adapter parsed sql rejects malformed generated classification payloads" {
    const alloc = std.testing.allocator;

    var session = try ParsedSql.initAlloc(alloc, "SET work_mem = '64MB'");
    defer session.deinit(alloc);
    var missing_session_name = session.generated_statement.?;
    if (missing_session_name.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .session => |*session_ast| session_ast.name_tokens = null,
            else => return error.TestUnexpectedResult,
        }
    }
    try std.testing.expectEqual(
        ParsedStatement.unknown,
        std.meta.activeTag(parseStatement(session.raw_statement, missing_session_name, &session.tokenized_sql)),
    );

    var transaction = try ParsedSql.initAlloc(alloc, "SAVEPOINT usage_batch");
    defer transaction.deinit(alloc);
    var missing_savepoint_name = transaction.generated_statement.?;
    if (missing_savepoint_name.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .transaction => |*transaction_ast| transaction_ast.name_tokens = null,
            else => return error.TestUnexpectedResult,
        }
    }
    try std.testing.expectEqual(
        ParsedStatement.unknown,
        std.meta.activeTag(parseStatement(transaction.raw_statement, missing_savepoint_name, &transaction.tokenized_sql)),
    );

    var prepared = try ParsedSql.initAlloc(alloc, "PREPARE usage_plan AS SELECT id FROM usage_records");
    defer prepared.deinit(alloc);
    var missing_prepared_inner = prepared.generated_statement.?;
    if (missing_prepared_inner.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .prepared => |*prepared_ast| prepared_ast.inner_statement_tokens = null,
            else => return error.TestUnexpectedResult,
        }
    }
    try std.testing.expectEqual(
        ParsedStatement.unknown,
        std.meta.activeTag(parseStatement(prepared.raw_statement, missing_prepared_inner, &prepared.tokenized_sql)),
    );

    var cursor = try ParsedSql.initAlloc(alloc, "FETCH FROM usage_cursor");
    defer cursor.deinit(alloc);
    var missing_cursor_tail = cursor.generated_statement.?;
    if (missing_cursor_tail.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .cursor => |*cursor_ast| cursor_ast.tail_tokens = null,
            else => return error.TestUnexpectedResult,
        }
    }
    try std.testing.expectEqual(
        ParsedStatement.unknown,
        std.meta.activeTag(parseStatement(cursor.raw_statement, missing_cursor_tail, &cursor.tokenized_sql)),
    );

    var publication = try ParsedSql.initAlloc(alloc, "CREATE PUBLICATION usage_pub FOR TABLE usage_records");
    defer publication.deinit(alloc);
    var missing_name = publication.generated_statement.?;
    if (missing_name.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .ddl => |*ddl_ast| ddl_ast.object_name_tokens = null,
            else => return error.TestUnexpectedResult,
        }
    }
    try std.testing.expectEqual(
        ParsedStatement.unknown,
        std.meta.activeTag(parseStatement(publication.raw_statement, missing_name, &publication.tokenized_sql)),
    );

    var policy = try ParsedSql.initAlloc(alloc, "CREATE POLICY usage_policy ON usage_records USING (tenant_id = current_user)");
    defer policy.deinit(alloc);
    var missing_policy_table = policy.generated_statement.?;
    if (missing_policy_table.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .ddl => |*ddl_ast| ddl_ast.index_table_tokens = null,
            else => return error.TestUnexpectedResult,
        }
    }
    try std.testing.expectEqual(
        ParsedStatement.unknown,
        std.meta.activeTag(parseStatement(policy.raw_statement, missing_policy_table, &policy.tokenized_sql)),
    );

    var malformed_policy_tail = policy.generated_statement.?;
    if (malformed_policy_tail.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .ddl => |*ddl_ast| ddl_ast.alter_table_operation_tokens.?.start += 1,
            else => return error.TestUnexpectedResult,
        }
    }
    try std.testing.expectEqual(
        ParsedStatement.unknown,
        std.meta.activeTag(parseStatement(policy.raw_statement, malformed_policy_tail, &policy.tokenized_sql)),
    );

    var drop_policy = try ParsedSql.initAlloc(alloc, "DROP POLICY usage_policy ON usage_records CASCADE");
    defer drop_policy.deinit(alloc);
    var malformed_drop_policy_behavior = drop_policy.generated_statement.?;
    if (malformed_drop_policy_behavior.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .ddl => |*ddl_ast| ddl_ast.cascade = false,
            else => return error.TestUnexpectedResult,
        }
    }
    try std.testing.expectEqual(
        ParsedStatement.unknown,
        std.meta.activeTag(parseStatement(drop_policy.raw_statement, malformed_drop_policy_behavior, &drop_policy.tokenized_sql)),
    );

    var extension_index = try ParsedSql.initAlloc(alloc, "CREATE INDEX usage_status_idx ON usage_records (status)");
    defer extension_index.deinit(alloc);
    var mismatched_extension_kind = extension_index.generated_statement.?;
    if (mismatched_extension_kind.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .extension_index => |*ddl_ast| ddl_ast.kind = .drop_index,
            else => return error.TestUnexpectedResult,
        }
    }
    try std.testing.expectEqual(
        ParsedStatement.unknown,
        std.meta.activeTag(parseStatement(extension_index.raw_statement, mismatched_extension_kind, &extension_index.tokenized_sql)),
    );

    var covering_index = try ParsedSql.initAlloc(alloc, "CREATE UNIQUE INDEX usage_status_active_idx ON usage_records (status) INCLUDE (tenant_id, amount) WHERE deleted_at IS NULL");
    defer covering_index.deinit(alloc);
    var malformed_index_include = covering_index.generated_statement.?;
    if (malformed_index_include.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .extension_index => |*ddl_ast| ddl_ast.index_include_tokens.?.start += 1,
            else => return error.TestUnexpectedResult,
        }
    }
    try std.testing.expectEqual(
        ParsedStatement.unknown,
        std.meta.activeTag(parseStatement(covering_index.raw_statement, malformed_index_include, &covering_index.tokenized_sql)),
    );

    var malformed_index_where = covering_index.generated_statement.?;
    if (malformed_index_where.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .extension_index => |*ddl_ast| ddl_ast.index_where_tokens.?.end -= 1,
            else => return error.TestUnexpectedResult,
        }
    }
    try std.testing.expectEqual(
        ParsedStatement.unknown,
        std.meta.activeTag(parseStatement(covering_index.raw_statement, malformed_index_where, &covering_index.tokenized_sql)),
    );

    var graph_index = try ParsedSql.initAlloc(alloc, "CREATE GRAPH INDEX docs_edge_graph ON doc_edges");
    defer graph_index.deinit(alloc);
    var mismatched_graph_kind = graph_index.generated_statement.?;
    if (mismatched_graph_kind.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .graph => |*graph_ast| graph_ast.kind = .create_metric,
            else => return error.TestUnexpectedResult,
        }
    }
    try std.testing.expectEqual(
        ParsedStatement.unknown,
        std.meta.activeTag(parseStatement(graph_index.raw_statement, mismatched_graph_kind, &graph_index.tokenized_sql)),
    );

    var graph_metric = try ParsedSql.initAlloc(alloc, "CREATE GRAPH METRIC docs_pagerank ON doc_edges");
    defer graph_metric.deinit(alloc);
    var malformed_graph_span = graph_metric.generated_statement.?;
    if (malformed_graph_span.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .graph => |*graph_ast| graph_ast.statement_span.end -= 1,
            else => return error.TestUnexpectedResult,
        }
    }
    try std.testing.expectEqual(
        ParsedStatement.unknown,
        std.meta.activeTag(parseStatement(graph_metric.raw_statement, malformed_graph_span, &graph_metric.tokenized_sql)),
    );

    var unsupported_copy = try ParsedSql.initAlloc(alloc, "COPY usage_records FROM STDIN");
    defer unsupported_copy.deinit(alloc);
    var mismatched_unsupported_kind = unsupported_copy.generated_statement.?;
    if (mismatched_unsupported_kind.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .unsupported => |*unsupported_ast| unsupported_ast.kind = .vacuum,
            else => return error.TestUnexpectedResult,
        }
    }
    try std.testing.expectEqual(
        ParsedStatement.unknown,
        std.meta.activeTag(parseStatement(unsupported_copy.raw_statement, mismatched_unsupported_kind, &unsupported_copy.tokenized_sql)),
    );

    var mismatched_unsupported_reason = unsupported_copy.generated_statement.?;
    if (mismatched_unsupported_reason.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .unsupported => |*unsupported_ast| unsupported_ast.reason = .vacuum_not_planned_by_generated_parser,
            else => return error.TestUnexpectedResult,
        }
    }
    try std.testing.expectEqual(
        ParsedStatement.unknown,
        std.meta.activeTag(parseStatement(unsupported_copy.raw_statement, mismatched_unsupported_reason, &unsupported_copy.tokenized_sql)),
    );

    var malformed_unsupported_subject = unsupported_copy.generated_statement.?;
    if (malformed_unsupported_subject.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .unsupported => |*unsupported_ast| unsupported_ast.subject_tokens = .{ .start = 2, .end = unsupported_copy.items().len },
            else => return error.TestUnexpectedResult,
        }
    }
    try std.testing.expectEqual(
        ParsedStatement.unknown,
        std.meta.activeTag(parseStatement(unsupported_copy.raw_statement, malformed_unsupported_subject, &unsupported_copy.tokenized_sql)),
    );

    var unsupported_trigger = try ParsedSql.initAlloc(alloc, "CREATE TRIGGER usage_audit BEFORE INSERT ON usage_records FOR EACH ROW EXECUTE FUNCTION audit_usage()");
    defer unsupported_trigger.deinit(alloc);
    var malformed_trigger_subject = unsupported_trigger.generated_statement.?;
    if (malformed_trigger_subject.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .unsupported => |*unsupported_ast| unsupported_ast.subject_tokens = .{ .start = 1, .end = unsupported_trigger.items().len },
            else => return error.TestUnexpectedResult,
        }
    }
    try std.testing.expectEqual(
        ParsedStatement.unknown,
        std.meta.activeTag(parseStatement(unsupported_trigger.raw_statement, malformed_trigger_subject, &unsupported_trigger.tokenized_sql)),
    );

    var graph_query = try ParsedSql.initAlloc(alloc, "MATCH (doc) RETURN doc");
    defer graph_query.deinit(alloc);
    var malformed_graph_query_subject = graph_query.generated_statement.?;
    if (malformed_graph_query_subject.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .unsupported => |*unsupported_ast| unsupported_ast.subject_tokens = null,
            else => return error.TestUnexpectedResult,
        }
    }
    try std.testing.expectEqual(
        ParsedStatement.unknown,
        std.meta.activeTag(parseStatement(graph_query.raw_statement, malformed_graph_query_subject, &graph_query.tokenized_sql)),
    );

    var malformed_graph_query_reason = graph_query.generated_statement.?;
    if (malformed_graph_query_reason.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .unsupported => |*unsupported_ast| unsupported_ast.reason = .copy_not_planned_by_generated_parser,
            else => return error.TestUnexpectedResult,
        }
    }
    try std.testing.expectEqual(
        ParsedStatement.unknown,
        std.meta.activeTag(parseStatement(graph_query.raw_statement, malformed_graph_query_reason, &graph_query.tokenized_sql)),
    );

    var explain = try ParsedSql.initAlloc(alloc, "EXPLAIN (FORMAT JSON) SELECT id FROM usage_records");
    defer explain.deinit(alloc);
    var malformed_explain_options = explain.generated_statement.?;
    if (malformed_explain_options.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .unsupported => |*unsupported_ast| unsupported_ast.explain_options_tokens = .{ .start = 1, .end = explain.items().len + 1 },
            else => return error.TestUnexpectedResult,
        }
    }
    try std.testing.expectEqual(
        ParsedStatement.unknown,
        std.meta.activeTag(parseStatement(explain.raw_statement, malformed_explain_options, &explain.tokenized_sql)),
    );
}

test "sql adapter parsed sql retains generated type system DDL nodes" {
    const alloc = std.testing.allocator;

    const Case = struct {
        sql: []const u8,
        kind: generated_parser.GeneratedSqlDdlKind,
        object_name_tokens: ?generated_parser.GeneratedSqlTokenRange = null,
        tail_start: ?usize = null,
        if_exists: bool = false,
    };

    const cases = [_]Case{
        .{
            .sql = "CREATE COLLATION case_insensitive (provider = icu, locale = 'und-u-ks-level2')",
            .kind = .create_collation,
            .object_name_tokens = .{ .start = 2, .end = 3 },
            .tail_start = 3,
        },
        .{
            .sql = "ALTER COLLATION case_insensitive RENAME TO ci_text",
            .kind = .alter_collation,
            .object_name_tokens = .{ .start = 2, .end = 3 },
            .tail_start = 3,
        },
        .{
            .sql = "DROP COLLATION IF EXISTS ci_text",
            .kind = .drop_collation,
            .object_name_tokens = .{ .start = 4, .end = 5 },
            .if_exists = true,
        },
        .{
            .sql = "CREATE OPERATOR === (FUNCTION = text_eq, LEFTARG = text, RIGHTARG = text)",
            .kind = .create_operator,
            .tail_start = 2,
        },
        .{
            .sql = "DROP OPERATOR === (text, text)",
            .kind = .drop_operator,
            .tail_start = 2,
        },
        .{
            .sql = "CREATE AGGREGATE first_value_text(text) (SFUNC = first_sfunc, STYPE = text)",
            .kind = .create_aggregate,
            .object_name_tokens = .{ .start = 2, .end = 3 },
            .tail_start = 3,
        },
        .{
            .sql = "DROP AGGREGATE first_value_text(text)",
            .kind = .drop_aggregate,
            .object_name_tokens = .{ .start = 2, .end = 3 },
            .tail_start = 3,
        },
        .{
            .sql = "CREATE CAST (jsonb AS text) WITH FUNCTION jsonb_to_text(jsonb) AS ASSIGNMENT",
            .kind = .create_cast,
            .tail_start = 2,
        },
        .{
            .sql = "DROP CAST (jsonb AS text)",
            .kind = .drop_cast,
            .tail_start = 2,
        },
    };

    for (cases) |case| {
        var parsed = try ParsedSql.initAlloc(alloc, case.sql);
        defer parsed.deinit(alloc);
        try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.ddl, parsed.generatedStatementKind().?);
        switch (parsed.generated_statement.?.ast.?) {
            .ddl => |ddl_ast| {
                try std.testing.expectEqual(case.kind, ddl_ast.kind);
                try std.testing.expectEqual(case.object_name_tokens, ddl_ast.object_name_tokens);
                try std.testing.expectEqual(case.if_exists, ddl_ast.if_exists);
                if (case.tail_start) |tail_start| {
                    try std.testing.expectEqual(
                        generated_parser.GeneratedSqlTokenRange{ .start = tail_start, .end = parsed.items().len },
                        ddl_ast.alter_table_operation_tokens.?,
                    );
                } else {
                    try std.testing.expect(ddl_ast.alter_table_operation_tokens == null);
                }
            },
            else => return error.TestUnexpectedResult,
        }
        switch (parsed.statement) {
            .ddl => {},
            else => return error.TestUnexpectedResult,
        }
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

    var malformed_generated = generated_insert.generated_statement.?;
    malformed_generated.ast = null;
    generated_insert.tokenized_sql.write_statement_kind = .insert;
    generated_insert.statement = parseStatement(generated_insert.raw_statement, malformed_generated, &generated_insert.tokenized_sql);
    try std.testing.expect(generated_insert.writeStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_insert.statement));

    var malformed_command_span = generated_insert.generated_statement.?;
    switch (malformed_command_span.ast.?) {
        .dml => |*dml_ast| dml_ast.command_span = .{ .start = dml_ast.command_span.start + 1, .end = dml_ast.command_span.end },
        else => return error.TestUnexpectedResult,
    }
    generated_insert.tokenized_sql.write_statement_kind = .insert;
    generated_insert.statement = parseStatement(generated_insert.raw_statement, malformed_command_span, &generated_insert.tokenized_sql);
    try std.testing.expect(generated_insert.writeStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_insert.statement));

    var generated_insert_select = try ParsedSql.initAlloc(alloc, "INSERT INTO usage_records (id) SELECT id FROM incoming_usage");
    defer generated_insert_select.deinit(alloc);
    var malformed_source_read = generated_insert_select.generated_statement.?;
    switch (malformed_source_read.ast.?) {
        .dml => |*dml_ast| dml_ast.source_read = null,
        else => return error.TestUnexpectedResult,
    }
    generated_insert_select.statement = parseStatement(generated_insert_select.raw_statement, malformed_source_read, &generated_insert_select.tokenized_sql);
    try std.testing.expect(generated_insert_select.writeStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_insert_select.statement));

    var malformed_source_clause = generated_insert_select.generated_statement.?;
    switch (malformed_source_clause.ast.?) {
        .dml => |*dml_ast| dml_ast.source_read.?.source_tokens.?.start -= 1,
        else => return error.TestUnexpectedResult,
    }
    generated_insert_select.statement = parseStatement(generated_insert_select.raw_statement, malformed_source_clause, &generated_insert_select.tokenized_sql);
    try std.testing.expect(generated_insert_select.writeStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_insert_select.statement));

    var malformed_source_kind = generated_insert_select.generated_statement.?;
    switch (malformed_source_kind.ast.?) {
        .dml => |*dml_ast| dml_ast.source_read.?.kind = .set_operation,
        else => return error.TestUnexpectedResult,
    }
    generated_insert_select.statement = parseStatement(generated_insert_select.raw_statement, malformed_source_kind, &generated_insert_select.tokenized_sql);
    try std.testing.expect(generated_insert_select.writeStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_insert_select.statement));

    var generated_insert_select_no_from = try ParsedSql.initAlloc(alloc, "INSERT INTO usage_records (id, amount) SELECT 1, 2");
    defer generated_insert_select_no_from.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.dml, generated_insert_select_no_from.generatedStatementKind().?);

    var malformed_select_projection_tail = generated_insert_select_no_from.generated_statement.?;
    switch (malformed_select_projection_tail.ast.?) {
        .dml => |*dml_ast| dml_ast.source_read.?.projection_tokens.?.end -= 1,
        else => return error.TestUnexpectedResult,
    }
    generated_insert_select_no_from.statement = parseStatement(generated_insert_select_no_from.raw_statement, malformed_select_projection_tail, &generated_insert_select_no_from.tokenized_sql);
    try std.testing.expect(generated_insert_select_no_from.writeStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_insert_select_no_from.statement));

    var generated_insert_set_operation = try ParsedSql.initAlloc(alloc, "INSERT INTO usage_records (id) SELECT id FROM incoming_usage UNION SELECT id FROM archived_usage");
    defer generated_insert_set_operation.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.dml, generated_insert_set_operation.generatedStatementKind().?);

    var malformed_source_set_operation = generated_insert_set_operation.generated_statement.?;
    switch (malformed_source_set_operation.ast.?) {
        .dml => |*dml_ast| dml_ast.source_read.?.set_operation_tokens.?.start = dml_ast.source_read.?.projection_tokens.?.start,
        else => return error.TestUnexpectedResult,
    }
    generated_insert_set_operation.statement = parseStatement(generated_insert_set_operation.raw_statement, malformed_source_set_operation, &generated_insert_set_operation.tokenized_sql);
    try std.testing.expect(generated_insert_set_operation.writeStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_insert_set_operation.statement));

    var malformed_set_operation_kind = generated_insert_set_operation.generated_statement.?;
    switch (malformed_set_operation_kind.ast.?) {
        .dml => |*dml_ast| dml_ast.source_read.?.kind = .query,
        else => return error.TestUnexpectedResult,
    }
    generated_insert_set_operation.statement = parseStatement(generated_insert_set_operation.raw_statement, malformed_set_operation_kind, &generated_insert_set_operation.tokenized_sql);
    try std.testing.expect(generated_insert_set_operation.writeStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_insert_set_operation.statement));

    var generated_update_from = try ParsedSql.initAlloc(alloc, "UPDATE usage_records SET status = source_rows.status FROM source_rows WHERE usage_records.id = source_rows.id");
    defer generated_update_from.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.dml, generated_update_from.generatedStatementKind().?);

    var malformed_relation_source_kind = generated_update_from.generated_statement.?;
    switch (malformed_relation_source_kind.ast.?) {
        .dml => |*dml_ast| dml_ast.source_read.?.kind = .aggregate,
        else => return error.TestUnexpectedResult,
    }
    generated_update_from.statement = parseStatement(generated_update_from.raw_statement, malformed_relation_source_kind, &generated_update_from.tokenized_sql);
    try std.testing.expect(generated_update_from.writeStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_update_from.statement));

    var generated_insert_conflict = try ParsedSql.initAlloc(alloc, "INSERT INTO usage_records (id, status) VALUES ('u1', 'open') ON CONFLICT (id) DO NOTHING RETURNING id");
    defer generated_insert_conflict.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.dml, generated_insert_conflict.generatedStatementKind().?);

    var malformed_conflict_layout = generated_insert_conflict.generated_statement.?;
    switch (malformed_conflict_layout.ast.?) {
        .dml => |*dml_ast| dml_ast.conflict_tokens.?.start += 1,
        else => return error.TestUnexpectedResult,
    }
    generated_insert_conflict.statement = parseStatement(generated_insert_conflict.raw_statement, malformed_conflict_layout, &generated_insert_conflict.tokenized_sql);
    try std.testing.expect(generated_insert_conflict.writeStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_insert_conflict.statement));

    var generated_update_returning = try ParsedSql.initAlloc(alloc, "UPDATE usage_records SET status = 'closed' WHERE id = 'u1' RETURNING id, status");
    defer generated_update_returning.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.dml, generated_update_returning.generatedStatementKind().?);

    var malformed_update_returning_layout = generated_update_returning.generated_statement.?;
    switch (malformed_update_returning_layout.ast.?) {
        .dml => |*dml_ast| dml_ast.returning_tokens.?.end -= 1,
        else => return error.TestUnexpectedResult,
    }
    generated_update_returning.statement = parseStatement(generated_update_returning.raw_statement, malformed_update_returning_layout, &generated_update_returning.tokenized_sql);
    try std.testing.expect(generated_update_returning.writeStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_update_returning.statement));

    var generated_delete_using = try ParsedSql.initAlloc(alloc, "DELETE FROM usage_records USING source_rows WHERE usage_records.id = source_rows.id");
    defer generated_delete_using.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.dml, generated_delete_using.generatedStatementKind().?);

    var malformed_delete_using_layout = generated_delete_using.generated_statement.?;
    switch (malformed_delete_using_layout.ast.?) {
        .dml => |*dml_ast| dml_ast.source_tokens.?.start += 1,
        else => return error.TestUnexpectedResult,
    }
    generated_delete_using.statement = parseStatement(generated_delete_using.raw_statement, malformed_delete_using_layout, &generated_delete_using.tokenized_sql);
    try std.testing.expect(generated_delete_using.writeStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_delete_using.statement));

    var generated_truncate_multi = try ParsedSql.initAlloc(alloc, "TRUNCATE usage_records, archive_records");
    defer generated_truncate_multi.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.dml, generated_truncate_multi.generatedStatementKind().?);

    var malformed_truncate_target_layout = generated_truncate_multi.generated_statement.?;
    switch (malformed_truncate_target_layout.ast.?) {
        .dml => |*dml_ast| dml_ast.additional_target_tokens.?.start += 1,
        else => return error.TestUnexpectedResult,
    }
    generated_truncate_multi.statement = parseStatement(generated_truncate_multi.raw_statement, malformed_truncate_target_layout, &generated_truncate_multi.tokenized_sql);
    try std.testing.expect(generated_truncate_multi.writeStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_truncate_multi.statement));

    var generated_merge = try ParsedSql.initAlloc(alloc, "MERGE INTO usage_records USING source_rows ON usage_records.id = source_rows.id WHEN MATCHED THEN DELETE");
    defer generated_merge.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.dml, generated_merge.generatedStatementKind().?);

    var malformed_merge_layout = generated_merge.generated_statement.?;
    switch (malformed_merge_layout.ast.?) {
        .dml => |*dml_ast| dml_ast.where_tokens.?.start += 1,
        else => return error.TestUnexpectedResult,
    }
    generated_merge.statement = parseStatement(generated_merge.raw_statement, malformed_merge_layout, &generated_merge.tokenized_sql);
    try std.testing.expect(generated_merge.writeStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_merge.statement));

    var generated_recursive_insert = try ParsedSql.initAlloc(alloc, "WITH RECURSIVE source_rows AS (SELECT id FROM usage_records) INSERT INTO archive(id) SELECT id FROM source_rows");
    defer generated_recursive_insert.deinit(alloc);
    var malformed_cte_prefix = generated_recursive_insert.generated_statement.?;
    switch (malformed_cte_prefix.ast.?) {
        .dml => |*dml_ast| dml_ast.cte_recursive = false,
        else => return error.TestUnexpectedResult,
    }
    generated_recursive_insert.statement = parseStatement(generated_recursive_insert.raw_statement, malformed_cte_prefix, &generated_recursive_insert.tokenized_sql);
    try std.testing.expect(generated_recursive_insert.writeStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_recursive_insert.statement));

    var generated_multi_cte_update = try ParsedSql.initAlloc(alloc, "WITH RECURSIVE seed_rows AS (SELECT id FROM usage_records), source_rows AS (SELECT id FROM seed_rows), final_rows AS (SELECT id FROM source_rows) UPDATE usage_records SET status = 'done' WHERE id IN (SELECT id FROM final_rows)");
    defer generated_multi_cte_update.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.dml, generated_multi_cte_update.generatedStatementKind().?);

    var malformed_middle_cte_item = generated_multi_cte_update.generated_statement.?;
    switch (malformed_middle_cte_item.ast.?) {
        .dml => |*dml_ast| {
            const cte_prefix = if (dml_ast.cte_prefix) |*cte_prefix| cte_prefix else return error.TestUnexpectedResult;
            try std.testing.expectEqual(@as(usize, 3), cte_prefix.items.len);
            cte_prefix.items[1].name_tokens.start -= 1;
        },
        else => return error.TestUnexpectedResult,
    }
    generated_multi_cte_update.statement = parseStatement(generated_multi_cte_update.raw_statement, malformed_middle_cte_item, &generated_multi_cte_update.tokenized_sql);
    try std.testing.expect(generated_multi_cte_update.writeStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_multi_cte_update.statement));
}

test "sql adapter parsed sql requires generated parser success for plain DML heads" {
    const alloc = std.testing.allocator;

    const cases = [_][]const u8{
        "INSERT usage_records (id) VALUES ('u1')",
        "UPDATE usage_records WHERE id = 'u1'",
        "DELETE usage_records WHERE id = 'u1'",
        "TRUNCATE TABLE",
        "MERGE INTO usage_records USING source_rows WHEN MATCHED THEN DELETE",
    };

    for (cases) |sql| {
        try std.testing.expectError(error.UnexpectedToken, ParsedSql.initAlloc(alloc, sql));
    }
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
        .{ .sql = "SELECT id FROM usage_records FOR UPDATE SKIP LOCKED", .generated = .query, .read = .query },
        .{ .sql = "WITH source_rows AS (SELECT id FROM usage_records) SELECT id FROM source_rows FOR SHARE NOWAIT", .generated = .cte, .read = .query },
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

    var generated_where_query = try ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records WHERE status = 'open'");
    defer generated_where_query.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.read, generated_where_query.generatedStatementKind().?);

    var malformed_where_generated = generated_where_query.generated_statement.?;
    if (malformed_where_generated.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .read => |read_ast| read_ast.where_expression.tokens = read_ast.projection_tokens,
            else => return error.TestUnexpectedResult,
        }
    } else {
        return error.TestUnexpectedResult;
    }
    generated_where_query.statement = parseStatement(generated_where_query.raw_statement, malformed_where_generated, &generated_where_query.tokenized_sql);
    try std.testing.expect(generated_where_query.readStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_where_query.statement));

    var generated_having_query = try ParsedSql.initAlloc(alloc, "SELECT status, count(*) FROM usage_records GROUP BY status HAVING count(*) > 1");
    defer generated_having_query.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.read, generated_having_query.generatedStatementKind().?);

    var malformed_having_generated = generated_having_query.generated_statement.?;
    if (malformed_having_generated.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .read => |read_ast| read_ast.having_expression.tokens = read_ast.group_tokens,
            else => return error.TestUnexpectedResult,
        }
    } else {
        return error.TestUnexpectedResult;
    }
    generated_having_query.statement = parseStatement(generated_having_query.raw_statement, malformed_having_generated, &generated_having_query.tokenized_sql);
    try std.testing.expect(generated_having_query.readStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_having_query.statement));

    var generated_projection_alias_query = try ParsedSql.initAlloc(alloc, "SELECT status AS state, id FROM usage_records");
    defer generated_projection_alias_query.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.read, generated_projection_alias_query.generatedStatementKind().?);

    var malformed_projection_alias_generated = generated_projection_alias_query.generated_statement.?;
    if (malformed_projection_alias_generated.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .read => |read_ast| read_ast.projection_items.alias_name_items[0].?.start -= 1,
            else => return error.TestUnexpectedResult,
        }
    } else {
        return error.TestUnexpectedResult;
    }
    generated_projection_alias_query.statement = parseStatement(generated_projection_alias_query.raw_statement, malformed_projection_alias_generated, &generated_projection_alias_query.tokenized_sql);
    try std.testing.expect(generated_projection_alias_query.readStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_projection_alias_query.statement));

    var generated_distinct_on_query = try ParsedSql.initAlloc(alloc, "SELECT DISTINCT ON (status) status FROM usage_records");
    defer generated_distinct_on_query.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.read, generated_distinct_on_query.generatedStatementKind().?);

    var malformed_distinct_on_generated = generated_distinct_on_query.generated_statement.?;
    if (malformed_distinct_on_generated.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .read => |read_ast| read_ast.distinct_on_items.items[0].end += 1,
            else => return error.TestUnexpectedResult,
        }
    } else {
        return error.TestUnexpectedResult;
    }
    generated_distinct_on_query.statement = parseStatement(generated_distinct_on_query.raw_statement, malformed_distinct_on_generated, &generated_distinct_on_query.tokenized_sql);
    try std.testing.expect(generated_distinct_on_query.readStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_distinct_on_query.statement));

    var generated_order_query = try ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records ORDER BY status DESC NULLS LAST");
    defer generated_order_query.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.read, generated_order_query.generatedStatementKind().?);

    var malformed_order_generated = generated_order_query.generated_statement.?;
    if (malformed_order_generated.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .read => |read_ast| read_ast.order_items.nulls_orders[0] = .first,
            else => return error.TestUnexpectedResult,
        }
    } else {
        return error.TestUnexpectedResult;
    }
    generated_order_query.statement = parseStatement(generated_order_query.raw_statement, malformed_order_generated, &generated_order_query.tokenized_sql);
    try std.testing.expect(generated_order_query.readStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_order_query.statement));

    var generated_window_query = try ParsedSql.initAlloc(alloc, "SELECT id, row_number() OVER usage_window AS rn FROM usage_records WINDOW usage_window AS (ORDER BY id RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)");
    defer generated_window_query.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.read, generated_window_query.generatedStatementKind().?);

    var malformed_window_generated = generated_window_query.generated_statement.?;
    if (malformed_window_generated.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .read => |read_ast| read_ast.window_items[0].frame_start_expression.?.tokens = read_ast.window_items[0].name_tokens,
            else => return error.TestUnexpectedResult,
        }
    } else {
        return error.TestUnexpectedResult;
    }
    generated_window_query.statement = parseStatement(generated_window_query.raw_statement, malformed_window_generated, &generated_window_query.tokenized_sql);
    try std.testing.expect(generated_window_query.readStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_window_query.statement));

    var generated_set_operation_where_query = try ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records UNION ALL SELECT id FROM usage_archive WHERE status = 'open'");
    defer generated_set_operation_where_query.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.read, generated_set_operation_where_query.generatedStatementKind().?);

    var malformed_set_operation_where_generated = generated_set_operation_where_query.generated_statement.?;
    if (malformed_set_operation_where_generated.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .read => |read_ast| read_ast.set_operation.right_where_expression.tokens = read_ast.set_operation.right_projection_tokens,
            else => return error.TestUnexpectedResult,
        }
    } else {
        return error.TestUnexpectedResult;
    }
    generated_set_operation_where_query.statement = parseStatement(generated_set_operation_where_query.raw_statement, malformed_set_operation_where_generated, &generated_set_operation_where_query.tokenized_sql);
    try std.testing.expect(generated_set_operation_where_query.readStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_set_operation_where_query.statement));

    var malformed_set_operation_where_clause_generated = generated_set_operation_where_query.generated_statement.?;
    if (malformed_set_operation_where_clause_generated.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .read => |read_ast| {
                read_ast.set_operation.right_where_tokens = read_ast.set_operation.right_projection_tokens;
                read_ast.set_operation.right_where_expression.tokens = read_ast.set_operation.right_projection_tokens;
            },
            else => return error.TestUnexpectedResult,
        }
    } else {
        return error.TestUnexpectedResult;
    }
    generated_set_operation_where_query.statement = parseStatement(generated_set_operation_where_query.raw_statement, malformed_set_operation_where_clause_generated, &generated_set_operation_where_query.tokenized_sql);
    try std.testing.expect(generated_set_operation_where_query.readStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_set_operation_where_query.statement));

    var generated_limit_query = try ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records LIMIT 5");
    defer generated_limit_query.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.read, generated_limit_query.generatedStatementKind().?);

    var malformed_limit_generated = generated_limit_query.generated_statement.?;
    if (malformed_limit_generated.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .read => |read_ast| {
                read_ast.limit_tokens = read_ast.projection_tokens;
                read_ast.limit_expression.tokens = read_ast.projection_tokens;
            },
            else => return error.TestUnexpectedResult,
        }
    } else {
        return error.TestUnexpectedResult;
    }
    generated_limit_query.statement = parseStatement(generated_limit_query.raw_statement, malformed_limit_generated, &generated_limit_query.tokenized_sql);
    try std.testing.expect(generated_limit_query.readStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_limit_query.statement));

    var generated_offset_query = try ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records OFFSET 2 ROWS");
    defer generated_offset_query.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.read, generated_offset_query.generatedStatementKind().?);

    var malformed_offset_generated = generated_offset_query.generated_statement.?;
    if (malformed_offset_generated.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .read => |read_ast| read_ast.offset_expression.tokens = read_ast.offset_tokens.?,
            else => return error.TestUnexpectedResult,
        }
    } else {
        return error.TestUnexpectedResult;
    }
    generated_offset_query.statement = parseStatement(generated_offset_query.raw_statement, malformed_offset_generated, &generated_offset_query.tokenized_sql);
    try std.testing.expect(generated_offset_query.readStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_offset_query.statement));

    var generated_fetch_query = try ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records FETCH FIRST 3 ROWS ONLY");
    defer generated_fetch_query.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.read, generated_fetch_query.generatedStatementKind().?);

    var malformed_fetch_generated = generated_fetch_query.generated_statement.?;
    if (malformed_fetch_generated.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .read => |read_ast| read_ast.fetch_tokens.?.start += 1,
            else => return error.TestUnexpectedResult,
        }
    } else {
        return error.TestUnexpectedResult;
    }
    generated_fetch_query.statement = parseStatement(generated_fetch_query.raw_statement, malformed_fetch_generated, &generated_fetch_query.tokenized_sql);
    try std.testing.expect(generated_fetch_query.readStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_fetch_query.statement));

    var generated_row_lock_query = try ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records FOR UPDATE SKIP LOCKED");
    defer generated_row_lock_query.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.read, generated_row_lock_query.generatedStatementKind().?);

    var malformed_row_lock_generated = generated_row_lock_query.generated_statement.?;
    if (malformed_row_lock_generated.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .read => |read_ast| read_ast.row_lock_tokens = read_ast.source_tokens,
            else => return error.TestUnexpectedResult,
        }
    } else {
        return error.TestUnexpectedResult;
    }
    generated_row_lock_query.statement = parseStatement(generated_row_lock_query.raw_statement, malformed_row_lock_generated, &generated_row_lock_query.tokenized_sql);
    try std.testing.expect(generated_row_lock_query.readStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_row_lock_query.statement));

    var generated_truncated_row_lock_query = try ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records FOR UPDATE SKIP LOCKED");
    defer generated_truncated_row_lock_query.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.read, generated_truncated_row_lock_query.generatedStatementKind().?);

    var malformed_truncated_row_lock_generated = generated_truncated_row_lock_query.generated_statement.?;
    if (malformed_truncated_row_lock_generated.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .read => |read_ast| read_ast.row_lock_tokens.?.end -= 1,
            else => return error.TestUnexpectedResult,
        }
    } else {
        return error.TestUnexpectedResult;
    }
    generated_truncated_row_lock_query.statement = parseStatement(generated_truncated_row_lock_query.raw_statement, malformed_truncated_row_lock_generated, &generated_truncated_row_lock_query.tokenized_sql);
    try std.testing.expect(generated_truncated_row_lock_query.readStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_truncated_row_lock_query.statement));

    var generated_cte_row_lock_query = try ParsedSql.initAlloc(alloc, "WITH source_rows AS (SELECT id FROM usage_records FOR UPDATE SKIP LOCKED) SELECT id FROM source_rows");
    defer generated_cte_row_lock_query.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.read, generated_cte_row_lock_query.generatedStatementKind().?);

    var malformed_cte_row_lock_generated = generated_cte_row_lock_query.generated_statement.?;
    if (malformed_cte_row_lock_generated.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .read => |read_ast| read_ast.cte_items[0].body_row_lock_tokens = read_ast.cte_items[0].body_source_tokens,
            else => return error.TestUnexpectedResult,
        }
    } else {
        return error.TestUnexpectedResult;
    }
    generated_cte_row_lock_query.statement = parseStatement(generated_cte_row_lock_query.raw_statement, malformed_cte_row_lock_generated, &generated_cte_row_lock_query.tokenized_sql);
    try std.testing.expect(generated_cte_row_lock_query.readStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_cte_row_lock_query.statement));

    var generated_cte_of_row_lock_query = try ParsedSql.initAlloc(alloc, "WITH source_rows AS (SELECT id FROM usage_records FOR NO KEY UPDATE OF usage_records NOWAIT) SELECT id FROM source_rows");
    defer generated_cte_of_row_lock_query.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.read, generated_cte_of_row_lock_query.generatedStatementKind().?);

    var malformed_cte_of_row_lock_generated = generated_cte_of_row_lock_query.generated_statement.?;
    if (malformed_cte_of_row_lock_generated.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .read => |read_ast| read_ast.cte_items[0].body_row_lock_tokens.?.end = read_ast.cte_items[0].body_row_lock_tokens.?.start + 5,
            else => return error.TestUnexpectedResult,
        }
    } else {
        return error.TestUnexpectedResult;
    }
    generated_cte_of_row_lock_query.statement = parseStatement(generated_cte_of_row_lock_query.raw_statement, malformed_cte_of_row_lock_generated, &generated_cte_of_row_lock_query.tokenized_sql);
    try std.testing.expect(generated_cte_of_row_lock_query.readStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_cte_of_row_lock_query.statement));

    var generated_set_operation_query = try ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records UNION ALL SELECT id FROM usage_archive");
    defer generated_set_operation_query.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.read, generated_set_operation_query.generatedStatementKind().?);

    var malformed_set_operation_generated = generated_set_operation_query.generated_statement.?;
    if (malformed_set_operation_generated.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .read => |read_ast| read_ast.set_operation.right_projection_items.alias_items = &.{},
            else => return error.TestUnexpectedResult,
        }
    } else {
        return error.TestUnexpectedResult;
    }
    generated_set_operation_query.statement = parseStatement(generated_set_operation_query.raw_statement, malformed_set_operation_generated, &generated_set_operation_query.tokenized_sql);
    try std.testing.expect(generated_set_operation_query.readStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_set_operation_query.statement));

    var generated_cte_set_operation_query = try ParsedSql.initAlloc(alloc, "WITH source_rows AS (SELECT id FROM usage_records UNION SELECT id FROM usage_archive) SELECT id FROM source_rows");
    defer generated_cte_set_operation_query.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.read, generated_cte_set_operation_query.generatedStatementKind().?);

    var malformed_cte_set_operation_generated = generated_cte_set_operation_query.generated_statement.?;
    if (malformed_cte_set_operation_generated.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .read => |read_ast| read_ast.cte_items[0].body_set_operation.right_select_tokens = null,
            else => return error.TestUnexpectedResult,
        }
    } else {
        return error.TestUnexpectedResult;
    }
    generated_cte_set_operation_query.statement = parseStatement(generated_cte_set_operation_query.raw_statement, malformed_cte_set_operation_generated, &generated_cte_set_operation_query.tokenized_sql);
    try std.testing.expect(generated_cte_set_operation_query.readStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_cte_set_operation_query.statement));

    var generated_multi_cte_query = try ParsedSql.initAlloc(alloc, "WITH first_rows AS (SELECT id FROM usage_records), second_rows AS (SELECT id FROM first_rows) SELECT id FROM second_rows");
    defer generated_multi_cte_query.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.read, generated_multi_cte_query.generatedStatementKind().?);

    var malformed_multi_cte_generated = generated_multi_cte_query.generated_statement.?;
    if (malformed_multi_cte_generated.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .read => |read_ast| read_ast.cte_items[1].name_tokens.start -= 1,
            else => return error.TestUnexpectedResult,
        }
    } else {
        return error.TestUnexpectedResult;
    }
    generated_multi_cte_query.statement = parseStatement(generated_multi_cte_query.raw_statement, malformed_multi_cte_generated, &generated_multi_cte_query.tokenized_sql);
    try std.testing.expect(generated_multi_cte_query.readStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_multi_cte_query.statement));

    var generated_materialized_cte_query = try ParsedSql.initAlloc(alloc, "WITH source_rows AS MATERIALIZED (SELECT id FROM usage_records) SELECT id FROM source_rows");
    defer generated_materialized_cte_query.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.read, generated_materialized_cte_query.generatedStatementKind().?);

    var malformed_materialized_cte_generated = generated_materialized_cte_query.generated_statement.?;
    if (malformed_materialized_cte_generated.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .read => |read_ast| read_ast.cte_items[0].materialization = .not_materialized,
            else => return error.TestUnexpectedResult,
        }
    } else {
        return error.TestUnexpectedResult;
    }
    generated_materialized_cte_query.statement = parseStatement(generated_materialized_cte_query.raw_statement, malformed_materialized_cte_generated, &generated_materialized_cte_query.tokenized_sql);
    try std.testing.expect(generated_materialized_cte_query.readStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_materialized_cte_query.statement));

    var generated_column_cte_query = try ParsedSql.initAlloc(alloc, "WITH source_rows(row_id) AS (SELECT id FROM usage_records) SELECT row_id FROM source_rows");
    defer generated_column_cte_query.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.read, generated_column_cte_query.generatedStatementKind().?);

    var malformed_column_cte_generated = generated_column_cte_query.generated_statement.?;
    if (malformed_column_cte_generated.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .read => |read_ast| read_ast.cte_items[0].column_names.items[0].end += 1,
            else => return error.TestUnexpectedResult,
        }
    } else {
        return error.TestUnexpectedResult;
    }
    generated_column_cte_query.statement = parseStatement(generated_column_cte_query.raw_statement, malformed_column_cte_generated, &generated_column_cte_query.tokenized_sql);
    try std.testing.expect(generated_column_cte_query.readStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_column_cte_query.statement));

    var generated_cte_projection_query = try ParsedSql.initAlloc(alloc, "WITH source_rows AS (SELECT status AS state FROM usage_records) SELECT state FROM source_rows");
    defer generated_cte_projection_query.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.read, generated_cte_projection_query.generatedStatementKind().?);

    var malformed_cte_projection_generated = generated_cte_projection_query.generated_statement.?;
    if (malformed_cte_projection_generated.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .read => |read_ast| read_ast.cte_items[0].body_projection_items.alias_items[0].?.end -= 1,
            else => return error.TestUnexpectedResult,
        }
    } else {
        return error.TestUnexpectedResult;
    }
    generated_cte_projection_query.statement = parseStatement(generated_cte_projection_query.raw_statement, malformed_cte_projection_generated, &generated_cte_projection_query.tokenized_sql);
    try std.testing.expect(generated_cte_projection_query.readStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_cte_projection_query.statement));

    var malformed_cte_projection_alias_slice = generated_cte_projection_query.generated_statement.?;
    if (malformed_cte_projection_alias_slice.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .read => |read_ast| read_ast.cte_items[0].body_projection_items.alias_items = &.{},
            else => return error.TestUnexpectedResult,
        }
    } else {
        return error.TestUnexpectedResult;
    }
    generated_cte_projection_query.statement = parseStatement(generated_cte_projection_query.raw_statement, malformed_cte_projection_alias_slice, &generated_cte_projection_query.tokenized_sql);
    try std.testing.expect(generated_cte_projection_query.readStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_cte_projection_query.statement));

    var generated_cte_where_query = try ParsedSql.initAlloc(alloc, "WITH source_rows AS (SELECT id FROM usage_records WHERE status = 'open') SELECT id FROM source_rows");
    defer generated_cte_where_query.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.read, generated_cte_where_query.generatedStatementKind().?);

    var malformed_cte_where_generated = generated_cte_where_query.generated_statement.?;
    if (malformed_cte_where_generated.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .read => |read_ast| read_ast.cte_items[0].body_where_expression.tokens = read_ast.cte_items[0].body_projection_tokens,
            else => return error.TestUnexpectedResult,
        }
    } else {
        return error.TestUnexpectedResult;
    }
    generated_cte_where_query.statement = parseStatement(generated_cte_where_query.raw_statement, malformed_cte_where_generated, &generated_cte_where_query.tokenized_sql);
    try std.testing.expect(generated_cte_where_query.readStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_cte_where_query.statement));

    var malformed_cte_where_clause_generated = generated_cte_where_query.generated_statement.?;
    if (malformed_cte_where_clause_generated.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .read => |read_ast| {
                read_ast.cte_items[0].body_where_tokens = read_ast.cte_items[0].body_projection_tokens;
                read_ast.cte_items[0].body_where_expression.tokens = read_ast.cte_items[0].body_projection_tokens;
            },
            else => return error.TestUnexpectedResult,
        }
    } else {
        return error.TestUnexpectedResult;
    }
    generated_cte_where_query.statement = parseStatement(generated_cte_where_query.raw_statement, malformed_cte_where_clause_generated, &generated_cte_where_query.tokenized_sql);
    try std.testing.expect(generated_cte_where_query.readStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_cte_where_query.statement));

    var generated_cte_window_query = try ParsedSql.initAlloc(alloc, "WITH source_rows AS (SELECT id, row_number() OVER usage_window AS rn FROM usage_records WINDOW usage_window AS (ORDER BY id)) SELECT id FROM source_rows");
    defer generated_cte_window_query.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.read, generated_cte_window_query.generatedStatementKind().?);

    var malformed_cte_window_generated = generated_cte_window_query.generated_statement.?;
    if (malformed_cte_window_generated.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .read => |read_ast| read_ast.cte_items[0].body_window_items[0].order_items.items[0].end += 1,
            else => return error.TestUnexpectedResult,
        }
    } else {
        return error.TestUnexpectedResult;
    }
    generated_cte_window_query.statement = parseStatement(generated_cte_window_query.raw_statement, malformed_cte_window_generated, &generated_cte_window_query.tokenized_sql);
    try std.testing.expect(generated_cte_window_query.readStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_cte_window_query.statement));

    var generated_join_query = try ParsedSql.initAlloc(alloc, "SELECT usage_records.id FROM usage_records JOIN accounts ON usage_records.account_id = accounts.id");
    defer generated_join_query.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.read, generated_join_query.generatedStatementKind().?);

    var malformed_join_tree_generated = generated_join_query.generated_statement.?;
    if (malformed_join_tree_generated.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .read => |read_ast| read_ast.join_items[0].right_tokens.start += 1,
            else => return error.TestUnexpectedResult,
        }
    } else {
        return error.TestUnexpectedResult;
    }
    generated_join_query.statement = parseStatement(generated_join_query.raw_statement, malformed_join_tree_generated, &generated_join_query.tokenized_sql);
    try std.testing.expect(generated_join_query.readStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_join_query.statement));

    var malformed_join_kind_generated = generated_join_query.generated_statement.?;
    if (malformed_join_kind_generated.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .read => |read_ast| read_ast.join_items[0].kind = .cross,
            else => return error.TestUnexpectedResult,
        }
    } else {
        return error.TestUnexpectedResult;
    }
    generated_join_query.statement = parseStatement(generated_join_query.raw_statement, malformed_join_kind_generated, &generated_join_query.tokenized_sql);
    try std.testing.expect(generated_join_query.readStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_join_query.statement));

    var malformed_join_condition_generated = generated_join_query.generated_statement.?;
    if (malformed_join_condition_generated.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .read => |read_ast| {
                read_ast.join_items[0].condition_kind = .none;
                read_ast.join_items[0].condition_tokens = .{ .start = read_ast.join_items[0].right_tokens.end, .end = read_ast.join_items[0].right_tokens.end };
                read_ast.join_items[0].predicate_tokens = null;
                read_ast.join_items[0].predicate_expression = .{};
            },
            else => return error.TestUnexpectedResult,
        }
    } else {
        return error.TestUnexpectedResult;
    }
    generated_join_query.statement = parseStatement(generated_join_query.raw_statement, malformed_join_condition_generated, &generated_join_query.tokenized_sql);
    try std.testing.expect(generated_join_query.readStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_join_query.statement));

    var generated_using_join_query = try ParsedSql.initAlloc(alloc, "SELECT usage_records.id FROM usage_records JOIN accounts USING (account_id)");
    defer generated_using_join_query.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.read, generated_using_join_query.generatedStatementKind().?);

    var malformed_using_join_generated = generated_using_join_query.generated_statement.?;
    if (malformed_using_join_generated.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .read => |read_ast| read_ast.join_items[0].using_columns.items[0].end += 1,
            else => return error.TestUnexpectedResult,
        }
    } else {
        return error.TestUnexpectedResult;
    }
    generated_using_join_query.statement = parseStatement(generated_using_join_query.raw_statement, malformed_using_join_generated, &generated_using_join_query.tokenized_sql);
    try std.testing.expect(generated_using_join_query.readStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_using_join_query.statement));

    var generated_cte_join_query = try ParsedSql.initAlloc(alloc, "WITH joined_rows AS (SELECT usage_records.id FROM usage_records JOIN accounts ON usage_records.account_id = accounts.id) SELECT id FROM joined_rows");
    defer generated_cte_join_query.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.read, generated_cte_join_query.generatedStatementKind().?);

    var malformed_cte_join_tree_generated = generated_cte_join_query.generated_statement.?;
    if (malformed_cte_join_tree_generated.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .read => |read_ast| read_ast.cte_items[0].body_join_tree_depth += 1,
            else => return error.TestUnexpectedResult,
        }
    } else {
        return error.TestUnexpectedResult;
    }
    generated_cte_join_query.statement = parseStatement(generated_cte_join_query.raw_statement, malformed_cte_join_tree_generated, &generated_cte_join_query.tokenized_sql);
    try std.testing.expect(generated_cte_join_query.readStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_cte_join_query.statement));

    var generated_cte_using_join_query = try ParsedSql.initAlloc(alloc, "WITH joined_rows AS (SELECT usage_records.id FROM usage_records JOIN accounts USING (account_id)) SELECT id FROM joined_rows");
    defer generated_cte_using_join_query.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.read, generated_cte_using_join_query.generatedStatementKind().?);

    var malformed_cte_using_join_generated = generated_cte_using_join_query.generated_statement.?;
    if (malformed_cte_using_join_generated.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .read => |read_ast| read_ast.cte_items[0].body_join_items[0].using_column_tokens.?.start -= 1,
            else => return error.TestUnexpectedResult,
        }
    } else {
        return error.TestUnexpectedResult;
    }
    generated_cte_using_join_query.statement = parseStatement(generated_cte_using_join_query.raw_statement, malformed_cte_using_join_generated, &generated_cte_using_join_query.tokenized_sql);
    try std.testing.expect(generated_cte_using_join_query.readStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_cte_using_join_query.statement));

    var generated_graph_source_query = try ParsedSql.initAlloc(alloc, "SELECT id FROM antfly.graph_match(table_name => 'usage_records', index => 'docs_edge_graph', start => 'doc:root', pattern => '(a)-[:cites]->(b)', return => 'b') AS gm");
    defer generated_graph_source_query.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.read, generated_graph_source_query.generatedStatementKind().?);

    var malformed_antfly_source_count = generated_graph_source_query.generated_statement.?;
    if (malformed_antfly_source_count.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .read => |read_ast| read_ast.source_antfly_function_count += 1,
            else => return error.TestUnexpectedResult,
        }
    } else {
        return error.TestUnexpectedResult;
    }
    generated_graph_source_query.statement = parseStatement(generated_graph_source_query.raw_statement, malformed_antfly_source_count, &generated_graph_source_query.tokenized_sql);
    try std.testing.expect(generated_graph_source_query.readStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_graph_source_query.statement));

    var malformed_antfly_argument_operator = generated_graph_source_query.generated_statement.?;
    if (malformed_antfly_argument_operator.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .read => |read_ast| read_ast.source_antfly_function_items[0].argument_items[0].operator_tokens.start += 1,
            else => return error.TestUnexpectedResult,
        }
    } else {
        return error.TestUnexpectedResult;
    }
    generated_graph_source_query.statement = parseStatement(generated_graph_source_query.raw_statement, malformed_antfly_argument_operator, &generated_graph_source_query.tokenized_sql);
    try std.testing.expect(generated_graph_source_query.readStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_graph_source_query.statement));

    var generated_antfly_source_query = try ParsedSql.initAlloc(alloc, "SELECT id FROM antfly.full_text_search(index => 'docs_body_fts', query => 'refund', limit => 10) AS hits");
    defer generated_antfly_source_query.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.read, generated_antfly_source_query.generatedStatementKind().?);

    var malformed_antfly_argument_delimiter = generated_antfly_source_query.generated_statement.?;
    if (malformed_antfly_argument_delimiter.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .read => |read_ast| {
                read_ast.source_antfly_function_items[0].argument_items[0].tokens.end += 1;
                read_ast.source_antfly_function_items[0].argument_items[0].value_tokens.end += 1;
            },
            else => return error.TestUnexpectedResult,
        }
    } else {
        return error.TestUnexpectedResult;
    }
    generated_antfly_source_query.statement = parseStatement(generated_antfly_source_query.raw_statement, malformed_antfly_argument_delimiter, &generated_antfly_source_query.tokenized_sql);
    try std.testing.expect(generated_antfly_source_query.readStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_antfly_source_query.statement));

    var duplicate_antfly_argument_query = try ParsedSql.initAlloc(alloc, "SELECT id FROM antfly.full_text_search(index => 'docs_body_fts', index => 'docs_body_fts_v2', query => 'refund') AS hits");
    defer duplicate_antfly_argument_query.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.read, duplicate_antfly_argument_query.generatedStatementKind().?);
    try std.testing.expect(duplicate_antfly_argument_query.readStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(duplicate_antfly_argument_query.statement));

    var malformed_graph_source_metric = generated_graph_source_query.generated_statement.?;
    if (malformed_graph_source_metric.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .read => |read_ast| read_ast.source_graph_function_items[0].pattern_value_tokens = null,
            else => return error.TestUnexpectedResult,
        }
    } else {
        return error.TestUnexpectedResult;
    }
    generated_graph_source_query.statement = parseStatement(generated_graph_source_query.raw_statement, malformed_graph_source_metric, &generated_graph_source_query.tokenized_sql);
    try std.testing.expect(generated_graph_source_query.readStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_graph_source_query.statement));

    var generated_cte_graph_source_query = try ParsedSql.initAlloc(alloc, "WITH ranked AS (SELECT * FROM antfly.graph_metric(table_name => 'usage_records', index => 'docs_edge_graph', metric => 'pagerank', top_k => 5) AS gm) SELECT id FROM ranked");
    defer generated_cte_graph_source_query.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.read, generated_cte_graph_source_query.generatedStatementKind().?);

    var malformed_cte_graph_source = generated_cte_graph_source_query.generated_statement.?;
    if (malformed_cte_graph_source.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .read => |read_ast| read_ast.cte_items[0].body_source_graph_function_items[0].metric_value_tokens = null,
            else => return error.TestUnexpectedResult,
        }
    } else {
        return error.TestUnexpectedResult;
    }
    generated_cte_graph_source_query.statement = parseStatement(generated_cte_graph_source_query.raw_statement, malformed_cte_graph_source, &generated_cte_graph_source_query.tokenized_sql);
    try std.testing.expect(generated_cte_graph_source_query.readStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_cte_graph_source_query.statement));

    var malformed_command_span = generated_query.generated_statement.?;
    if (malformed_command_span.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .read => |read_ast| read_ast.command_span = .{ .start = read_ast.command_span.start + 1, .end = read_ast.command_span.end },
            else => return error.TestUnexpectedResult,
        }
    } else {
        return error.TestUnexpectedResult;
    }
    generated_query.tokenized_sql.read_statement_kind = .query;
    generated_query.statement = parseStatement(generated_query.raw_statement, malformed_command_span, &generated_query.tokenized_sql);
    try std.testing.expect(generated_query.readStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_query.statement));

    var missing_ast = generated_query.generated_statement.?;
    missing_ast.ast = null;
    generated_query.tokenized_sql.read_statement_kind = .query;
    generated_query.statement = parseStatement(generated_query.raw_statement, missing_ast, &generated_query.tokenized_sql);
    try std.testing.expect(generated_query.readStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_query.statement));

    var generated_cte = try ParsedSql.initAlloc(alloc, "WITH RECURSIVE source_rows AS (SELECT id FROM usage_records) SELECT id FROM source_rows");
    defer generated_cte.deinit(alloc);
    try std.testing.expectEqual(generated_parser.GeneratedSqlStatementKind.read, generated_cte.generatedStatementKind().?);

    var malformed_generated = generated_cte.generated_statement.?;
    if (malformed_generated.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .read => |read_ast| read_ast.projection_tokens = null,
            else => return error.TestUnexpectedResult,
        }
    } else {
        return error.TestUnexpectedResult;
    }
    generated_cte.statement = parseStatement(generated_cte.raw_statement, malformed_generated, &generated_cte.tokenized_sql);
    try std.testing.expect(generated_cte.readStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_cte.statement));

    var malformed_cte_count = generated_cte.generated_statement.?;
    if (malformed_cte_count.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .read => |read_ast| read_ast.cte_count += 1,
            else => return error.TestUnexpectedResult,
        }
    } else {
        return error.TestUnexpectedResult;
    }
    generated_cte.statement = parseStatement(generated_cte.raw_statement, malformed_cte_count, &generated_cte.tokenized_sql);
    try std.testing.expect(generated_cte.readStatementKind() == null);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(generated_cte.statement));
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
            .graph => |graph_ast| {
                try std.testing.expectEqual(case.generated, graph_ast.kind);
                try std.testing.expect(graph_ast.object_name_tokens != null);
                switch (case.generated) {
                    .create_index => {
                        try std.testing.expect(graph_ast.source_name_tokens != null);
                        if (std.mem.indexOf(u8, case.sql, " EDGE ") != null) {
                            try std.testing.expect(graph_ast.edge_tokens != null);
                            try std.testing.expect(graph_ast.edge_source_tokens != null);
                            try std.testing.expect(graph_ast.edge_target_tokens != null);
                        }
                    },
                    .create_metric => try std.testing.expect(graph_ast.source_name_tokens != null),
                    .alter_metric => {
                        try std.testing.expect(graph_ast.source_name_tokens != null);
                        try std.testing.expect(graph_ast.algorithm_tokens != null);
                    },
                }
            },
            else => return error.TestUnexpectedResult,
        }
        switch (parsed.statement) {
            .ddl => {},
            else => return error.TestUnexpectedResult,
        }
    }

    var malformed = try ParsedSql.initAlloc(alloc, "ALTER GRAPH INDEX docs_edge_graph ADD METRIC pagerank_v1 USING pagerank");
    defer malformed.deinit(alloc);
    var malformed_generated = malformed.generated_statement.?;
    if (malformed_generated.ast) |*generated_ast| {
        switch (generated_ast.*) {
            .graph => |*graph| graph.algorithm_tokens = null,
            else => return error.TestUnexpectedResult,
        }
    } else {
        return error.TestUnexpectedResult;
    }
    malformed.statement = parseStatement(malformed.raw_statement, malformed_generated, &malformed.tokenized_sql);
    try std.testing.expectEqual(@as(std.meta.Tag(ParsedStatement), .unknown), std.meta.activeTag(malformed.statement));
}
