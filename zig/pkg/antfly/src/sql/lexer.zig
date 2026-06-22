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

const token_mod = @import("token.zig");

pub const Token = token_mod.Token;
pub const TokenKind = token_mod.TokenKind;

pub fn tokenizeAlloc(alloc: std.mem.Allocator, sql: []const u8) !std.ArrayListUnmanaged(Token) {
    var tokens = std.ArrayListUnmanaged(Token).empty;
    errdefer freeTokens(alloc, &tokens);
    const estimated_capacity = estimateTokenCapacity(sql);
    if (estimated_capacity > 0) try tokens.ensureTotalCapacity(alloc, estimated_capacity);

    var i: usize = 0;
    while (i < sql.len) {
        const ch = sql[i];
        if (std.ascii.isWhitespace(ch)) {
            i += 1;
            continue;
        }
        if (ch == '-' and i + 1 < sql.len and sql[i + 1] == '-') {
            i += 2;
            while (i < sql.len and sql[i] != '\n' and sql[i] != '\r') i += 1;
            continue;
        }
        if (ch == '/' and i + 1 < sql.len and sql[i + 1] == '*') {
            i += 2;
            while (i + 1 < sql.len and !(sql[i] == '*' and sql[i + 1] == '/')) i += 1;
            if (i + 1 >= sql.len) return error.UnsupportedSqlShape;
            i += 2;
            continue;
        }
        if (std.ascii.isAlphabetic(ch) or ch == '_') {
            const start = i;
            i += 1;
            while (i < sql.len and (std.ascii.isAlphanumeric(sql[i]) or sql[i] == '_' or sql[i] == '.')) i += 1;
            const end = i;
            i = skipSqlCast(sql, i);
            try tokens.append(alloc, .{
                .kind = .identifier,
                .text = sql[start..end],
                .source_start = start,
                .source_end = end,
                .keyword = token_mod.keywordFromIdentifier(sql[start..end]),
            });
            continue;
        }
        if (ch == '"') {
            const source_start = i;
            const start = i + 1;
            i += 1;
            while (i < sql.len and sql[i] != '"') i += 1;
            if (i >= sql.len) return error.UnsupportedSqlShape;
            i += 1;
            const source_end = i;
            i = skipSqlCast(sql, i);
            try tokens.append(alloc, .{
                .kind = .identifier,
                .text = sql[start .. source_end - 1],
                .source_start = source_start,
                .source_end = source_end,
            });
            continue;
        }
        if (ch == '\'') {
            const source_start = i;
            var out = std.ArrayListUnmanaged(u8).empty;
            errdefer out.deinit(alloc);
            i += 1;
            while (i < sql.len) {
                if (sql[i] == '\'') {
                    if (i + 1 < sql.len and sql[i + 1] == '\'') {
                        try out.append(alloc, '\'');
                        i += 2;
                        continue;
                    }
                    break;
                }
                try out.append(alloc, sql[i]);
                i += 1;
            }
            if (i >= sql.len) return error.UnsupportedSqlShape;
            const owned = try out.toOwnedSlice(alloc);
            var owned_transferred = false;
            errdefer if (!owned_transferred) alloc.free(owned);
            i += 1;
            if (sqlCastTypeAt(sql, i)) |cast_type| {
                i = cast_type.end;
                if (sqlCastTypeIsNumeric(cast_type.name)) {
                    if (!sqlStringIsJsonNumber(owned)) return error.UnsupportedSqlShape;
                    try tokens.append(alloc, .{ .kind = .number, .text = owned, .owned = true, .source_start = source_start, .source_end = i });
                } else if (sqlCastTypeIsBoolean(cast_type.name)) {
                    if (!std.ascii.eqlIgnoreCase(owned, "true") and !std.ascii.eqlIgnoreCase(owned, "false")) return error.UnsupportedSqlShape;
                    try tokens.append(alloc, .{ .kind = .identifier, .text = owned, .owned = true, .source_start = source_start, .source_end = i, .keyword = token_mod.keywordFromIdentifier(owned) });
                } else {
                    try tokens.append(alloc, .{ .kind = .string, .text = owned, .owned = true, .source_start = source_start, .source_end = i });
                }
            } else {
                try tokens.append(alloc, .{ .kind = .string, .text = owned, .owned = true, .source_start = source_start, .source_end = i });
            }
            owned_transferred = true;
            continue;
        }
        if (std.ascii.isDigit(ch)) {
            const start = i;
            i += 1;
            while (i < sql.len and (std.ascii.isDigit(sql[i]) or sql[i] == '.')) i += 1;
            const end = i;
            i = skipSqlNumericCast(sql, i);
            try tokens.append(alloc, .{ .kind = .number, .text = sql[start..end] });
            continue;
        }
        if (ch == '$') {
            if (sqlDollarQuoteAt(sql, i)) |quote| {
                if (!quote.closed) return error.UnsupportedSqlShape;
                try tokens.append(alloc, .{
                    .kind = .string,
                    .text = sql[quote.body_start..quote.body_end],
                    .source_start = i,
                    .source_end = quote.token_end,
                });
                i = quote.token_end;
                continue;
            }
            const start = i;
            i += 1;
            while (i < sql.len and std.ascii.isDigit(sql[i])) i += 1;
            if (i == start + 1) return error.UnsupportedSqlShape;
            if (i < sql.len and (std.ascii.isAlphanumeric(sql[i]) or sql[i] == '_')) return error.UnsupportedSqlShape;
            if (i + 1 < sql.len and sql[i] == ':' and sql[i + 1] == ':') {
                i += 2;
                while (i < sql.len and (std.ascii.isAlphanumeric(sql[i]) or sql[i] == '_' or sql[i] == '[' or sql[i] == ']')) i += 1;
            }
            try tokens.append(alloc, .{ .kind = .placeholder, .text = sql[start..i] });
            continue;
        }
        switch (ch) {
            ',' => {
                try tokens.append(alloc, .{ .kind = .comma, .text = sql[i .. i + 1] });
                i += 1;
            },
            '*' => {
                try tokens.append(alloc, .{ .kind = .star, .text = sql[i .. i + 1] });
                i += 1;
            },
            '+' => {
                try tokens.append(alloc, .{ .kind = .plus, .text = sql[i .. i + 1] });
                i += 1;
            },
            '/' => {
                try tokens.append(alloc, .{ .kind = .slash, .text = sql[i .. i + 1] });
                i += 1;
            },
            '%' => {
                try tokens.append(alloc, .{ .kind = .percent, .text = sql[i .. i + 1] });
                i += 1;
            },
            '(' => {
                try tokens.append(alloc, .{ .kind = .lparen, .text = sql[i .. i + 1] });
                i += 1;
            },
            ')' => {
                try tokens.append(alloc, .{ .kind = .rparen, .text = sql[i .. i + 1] });
                i += 1;
                i = skipSqlCast(sql, i);
            },
            '[' => {
                try tokens.append(alloc, .{ .kind = .lbracket, .text = sql[i .. i + 1] });
                i += 1;
            },
            ']' => {
                try tokens.append(alloc, .{ .kind = .rbracket, .text = sql[i .. i + 1] });
                i += 1;
                i = skipSqlCast(sql, i);
            },
            '@' => {
                if (i + 1 >= sql.len or sql[i + 1] != '>') return error.UnsupportedSqlShape;
                try tokens.append(alloc, .{ .kind = .at_contains, .text = sql[i .. i + 2] });
                i += 2;
            },
            '&' => {
                if (i + 1 >= sql.len or sql[i + 1] != '&') return error.UnsupportedSqlShape;
                try tokens.append(alloc, .{ .kind = .range_overlap, .text = sql[i .. i + 2] });
                i += 2;
            },
            '|' => {
                if (i + 1 >= sql.len or sql[i + 1] != '|') return error.UnsupportedSqlShape;
                try tokens.append(alloc, .{ .kind = .pipe_concat, .text = sql[i .. i + 2] });
                i += 2;
            },
            '?' => {
                if (i + 1 < sql.len and sql[i + 1] == '|') {
                    try tokens.append(alloc, .{ .kind = .question_any, .text = sql[i .. i + 2] });
                    i += 2;
                } else if (i + 1 < sql.len and sql[i + 1] == '&') {
                    try tokens.append(alloc, .{ .kind = .question_all, .text = sql[i .. i + 2] });
                    i += 2;
                } else {
                    try tokens.append(alloc, .{ .kind = .question, .text = sql[i .. i + 1] });
                    i += 1;
                }
            },
            '~' => {
                if (i + 1 < sql.len and sql[i + 1] == '*') {
                    try tokens.append(alloc, .{ .kind = .regex_imatch, .text = sql[i .. i + 2] });
                    i += 2;
                } else {
                    try tokens.append(alloc, .{ .kind = .regex_match, .text = sql[i .. i + 1] });
                    i += 1;
                }
            },
            '#' => {
                if (i + 2 < sql.len and sql[i + 1] == '>' and sql[i + 2] == '>') {
                    try tokens.append(alloc, .{ .kind = .path_arrow_text, .text = sql[i .. i + 3] });
                    i += 3;
                } else if (i + 1 < sql.len and sql[i + 1] == '>') {
                    try tokens.append(alloc, .{ .kind = .path_arrow_json, .text = sql[i .. i + 2] });
                    i += 2;
                } else {
                    return error.UnsupportedSqlShape;
                }
            },
            ';' => {
                try tokens.append(alloc, .{ .kind = .semicolon, .text = sql[i .. i + 1] });
                i += 1;
            },
            '=' => {
                try tokens.append(alloc, .{ .kind = .eq, .text = sql[i .. i + 1] });
                i += 1;
            },
            '!' => {
                if (i + 1 < sql.len and sql[i + 1] == '=') {
                    try tokens.append(alloc, .{ .kind = .neq, .text = sql[i .. i + 2] });
                    i += 2;
                } else if (i + 1 < sql.len and sql[i + 1] == '~') {
                    if (i + 2 < sql.len and sql[i + 2] == '*') {
                        try tokens.append(alloc, .{ .kind = .regex_not_imatch, .text = sql[i .. i + 3] });
                        i += 3;
                    } else {
                        try tokens.append(alloc, .{ .kind = .regex_not_match, .text = sql[i .. i + 2] });
                        i += 2;
                    }
                } else return error.UnsupportedSqlShape;
            },
            '<' => {
                if (i + 1 < sql.len and sql[i + 1] == '=') {
                    try tokens.append(alloc, .{ .kind = .lte, .text = sql[i .. i + 2] });
                    i += 2;
                } else if (i + 1 < sql.len and sql[i + 1] == '>') {
                    try tokens.append(alloc, .{ .kind = .neq, .text = sql[i .. i + 2] });
                    i += 2;
                } else {
                    try tokens.append(alloc, .{ .kind = .lt, .text = sql[i .. i + 1] });
                    i += 1;
                }
            },
            '>' => {
                if (i + 1 < sql.len and sql[i + 1] == '=') {
                    try tokens.append(alloc, .{ .kind = .gte, .text = sql[i .. i + 2] });
                    i += 2;
                } else {
                    try tokens.append(alloc, .{ .kind = .gt, .text = sql[i .. i + 1] });
                    i += 1;
                }
            },
            '-' => {
                if (i + 1 < sql.len and sql[i + 1] == '>' and i + 2 < sql.len and sql[i + 2] == '>') {
                    try tokens.append(alloc, .{ .kind = .arrow_text, .text = sql[i .. i + 3] });
                    i += 3;
                } else if (i + 1 < sql.len and sql[i + 1] == '>') {
                    try tokens.append(alloc, .{ .kind = .arrow_json, .text = sql[i .. i + 2] });
                    i += 2;
                } else {
                    try tokens.append(alloc, .{ .kind = .minus, .text = sql[i .. i + 1] });
                    i += 1;
                }
            },
            else => return error.UnsupportedSqlShape,
        }
    }
    try finalizeTokenSourceSpans(sql, tokens.items);
    return tokens;
}

fn estimateTokenCapacity(sql: []const u8) usize {
    if (sql.len == 0) return 0;
    return @min(sql.len, sql.len / 4 + 8);
}

pub fn freeTokens(alloc: std.mem.Allocator, tokens: *std.ArrayListUnmanaged(Token)) void {
    for (tokens.items) |token| {
        if (token.owned) alloc.free(token.text);
    }
    tokens.deinit(alloc);
}

fn finalizeTokenSourceSpans(sql: []const u8, tokens: []Token) !void {
    const sql_start = @intFromPtr(sql.ptr);
    const sql_end = sql_start + sql.len;
    for (tokens) |*token| {
        if (token.source_end > token.source_start or token.text.len == 0) continue;
        const token_start = @intFromPtr(token.text.ptr);
        if (token_start < sql_start or token_start + token.text.len > sql_end) return error.UnsupportedSqlShape;
        token.source_start = token_start - sql_start;
        token.source_end = token.source_start + token.text.len;
    }
}

const SqlDollarQuote = struct {
    body_start: usize,
    body_end: usize,
    token_end: usize,
    closed: bool,
};

fn sqlDollarQuoteAt(sql: []const u8, dollar: usize) ?SqlDollarQuote {
    if (dollar + 1 >= sql.len or sql[dollar] != '$') return null;
    if (std.ascii.isDigit(sql[dollar + 1])) return null;

    var delimiter_end = dollar + 1;
    if (sql[delimiter_end] == '$') {
        delimiter_end += 1;
    } else {
        if (!sqlDollarQuoteTagStart(sql[delimiter_end])) return null;
        delimiter_end += 1;
        while (delimiter_end < sql.len and sqlDollarQuoteTagContinue(sql[delimiter_end])) : (delimiter_end += 1) {}
        if (delimiter_end >= sql.len or sql[delimiter_end] != '$') return null;
        delimiter_end += 1;
    }

    const delimiter = sql[dollar..delimiter_end];
    const body_start = delimiter_end;
    const body_end = std.mem.indexOfPos(u8, sql, body_start, delimiter) orelse return .{
        .body_start = body_start,
        .body_end = sql.len,
        .token_end = sql.len,
        .closed = false,
    };
    return .{
        .body_start = body_start,
        .body_end = body_end,
        .token_end = body_end + delimiter.len,
        .closed = true,
    };
}

fn sqlDollarQuoteTagStart(ch: u8) bool {
    return std.ascii.isAlphabetic(ch) or ch == '_';
}

fn sqlDollarQuoteTagContinue(ch: u8) bool {
    return std.ascii.isAlphanumeric(ch) or ch == '_';
}

fn skipSqlCast(sql: []const u8, start: usize) usize {
    return if (sqlCastTypeAt(sql, start)) |cast_type| cast_type.end else start;
}

fn skipSqlNumericCast(sql: []const u8, start: usize) usize {
    const cast_type = sqlCastTypeAt(sql, start) orelse return start;
    if (!sqlCastTypeIsNumeric(cast_type.name)) return start;
    return cast_type.end;
}

const SqlCastType = struct {
    name: []const u8,
    end: usize,
};

fn sqlCastTypeAt(sql: []const u8, start: usize) ?SqlCastType {
    if (start + 1 >= sql.len or sql[start] != ':' or sql[start + 1] != ':') return null;
    var i = start + 2;
    const type_start = i;
    while (i < sql.len and (std.ascii.isAlphanumeric(sql[i]) or sql[i] == '_')) i += 1;
    if (i == type_start) return null;
    const type_name = sql[type_start..i];
    if (i < sql.len and sql[i] == '[') {
        i += 1;
        if (i >= sql.len or sql[i] != ']') return null;
        i += 1;
    }
    return .{ .name = type_name, .end = i };
}

fn sqlCastTypeIsNumeric(type_name: []const u8) bool {
    return std.ascii.eqlIgnoreCase(type_name, "numeric") or
        std.ascii.eqlIgnoreCase(type_name, "decimal") or
        std.ascii.eqlIgnoreCase(type_name, "int") or
        std.ascii.eqlIgnoreCase(type_name, "int2") or
        std.ascii.eqlIgnoreCase(type_name, "int4") or
        std.ascii.eqlIgnoreCase(type_name, "int8") or
        std.ascii.eqlIgnoreCase(type_name, "integer") or
        std.ascii.eqlIgnoreCase(type_name, "smallint") or
        std.ascii.eqlIgnoreCase(type_name, "bigint") or
        std.ascii.eqlIgnoreCase(type_name, "real") or
        std.ascii.eqlIgnoreCase(type_name, "float4") or
        std.ascii.eqlIgnoreCase(type_name, "float8");
}

fn sqlCastTypeIsBoolean(type_name: []const u8) bool {
    return std.ascii.eqlIgnoreCase(type_name, "bool") or
        std.ascii.eqlIgnoreCase(type_name, "boolean");
}

fn sqlStringIsJsonNumber(text: []const u8) bool {
    if (text.len == 0) return false;
    var i: usize = 0;
    if (text[i] == '-') {
        i += 1;
        if (i == text.len) return false;
    }

    if (text[i] == '0') {
        i += 1;
        if (i < text.len and std.ascii.isDigit(text[i])) return false;
    } else if (text[i] >= '1' and text[i] <= '9') {
        i += 1;
        while (i < text.len and std.ascii.isDigit(text[i])) i += 1;
    } else {
        return false;
    }

    if (i < text.len and text[i] == '.') {
        i += 1;
        const fraction_start = i;
        while (i < text.len and std.ascii.isDigit(text[i])) i += 1;
        if (i == fraction_start) return false;
    }

    if (i < text.len and (text[i] == 'e' or text[i] == 'E')) {
        i += 1;
        if (i < text.len and (text[i] == '+' or text[i] == '-')) i += 1;
        const exponent_start = i;
        while (i < text.len and std.ascii.isDigit(text[i])) i += 1;
        if (i == exponent_start) return false;
    }

    return i == text.len;
}

test "sql adapter lexer records source spans and dollar quoted literals" {
    const alloc = std.testing.allocator;
    const sql = "SELECT $$a $1 body$$ AS body, $tag$quoted 'text'$tag$ AS tagged FROM users WHERE id = $1";

    var tokens = try tokenizeAlloc(alloc, sql);
    defer freeTokens(alloc, &tokens);

    try std.testing.expect(tokens.items.len > 0);
    try std.testing.expectEqual(TokenKind.identifier, tokens.items[0].kind);
    try std.testing.expectEqualStrings("SELECT", tokens.items[0].text);
    try std.testing.expectEqualStrings("SELECT", sql[tokens.items[0].source_start..tokens.items[0].source_end]);

    try std.testing.expectEqual(TokenKind.string, tokens.items[1].kind);
    try std.testing.expectEqualStrings("a $1 body", tokens.items[1].text);
    try std.testing.expectEqualStrings("$$a $1 body$$", sql[tokens.items[1].source_start..tokens.items[1].source_end]);
    try std.testing.expect(!tokens.items[1].owned);

    try std.testing.expectEqual(TokenKind.string, tokens.items[5].kind);
    try std.testing.expectEqualStrings("quoted 'text'", tokens.items[5].text);
    try std.testing.expectEqualStrings("$tag$quoted 'text'$tag$", sql[tokens.items[5].source_start..tokens.items[5].source_end]);
    try std.testing.expect(!tokens.items[5].owned);

    try std.testing.expectEqual(TokenKind.placeholder, tokens.items[tokens.items.len - 1].kind);
    try std.testing.expectEqualStrings("$1", tokens.items[tokens.items.len - 1].text);
    try std.testing.expectEqualStrings("$1", sql[tokens.items[tokens.items.len - 1].source_start..tokens.items[tokens.items.len - 1].source_end]);
}

test "sql adapter lexer rejects unterminated dollar quoted literals" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(error.UnsupportedSqlShape, tokenizeAlloc(alloc, "SELECT $tag$unterminated"));
}

test "sql adapter lexer validates numeric casts without JSON allocation" {
    try std.testing.expect(sqlStringIsJsonNumber("42"));
    try std.testing.expect(sqlStringIsJsonNumber("-42.5e+2"));
    try std.testing.expect(sqlStringIsJsonNumber("0"));
    try std.testing.expect(!sqlStringIsJsonNumber(""));
    try std.testing.expect(!sqlStringIsJsonNumber("+1"));
    try std.testing.expect(!sqlStringIsJsonNumber("01"));
    try std.testing.expect(!sqlStringIsJsonNumber("1."));
    try std.testing.expect(!sqlStringIsJsonNumber("1e"));
}
