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

const classifier = @import("classifier.zig");
const lexer = @import("lexer.zig");
const token_mod = @import("token.zig");

pub const Token = token_mod.Token;
pub const SourceSpan = token_mod.SourceSpan;

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

    pub fn initAlloc(alloc: std.mem.Allocator, source_sql: []const u8) !ParsedSql {
        var tokenized_sql = try TokenizedSql.initAlloc(alloc, source_sql);
        errdefer tokenized_sql.deinit(alloc);
        return .{
            .tokenized_sql = tokenized_sql,
            .raw_statement = parseRawStatement(tokenized_sql.items(), tokenized_sql.statement_family),
        };
    }

    pub fn deinit(self: *ParsedSql, alloc: std.mem.Allocator) void {
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
};

fn parseRawStatement(tokens: []const Token, family: ?classifier.SqlStatementFamily) RawSqlStatement {
    if (tokens.len == 0) return .{ .family = family };
    var token_end = tokens.len;
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
}
