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

// Reserve enough slots to keep ordinary statements allocation-efficient, but
// never size a Token array directly from untrusted SQL byte length. Large
// comments, literals, and identifiers may contain many bytes while producing
// zero or one token.
const eager_token_capacity_limit = 256;

pub const Token = token_mod.Token;
pub const TokenKind = token_mod.TokenKind;

pub const LexErrorKind = enum {
    unterminated_block_comment,
    unterminated_quoted_identifier,
    empty_quoted_identifier,
    unterminated_string_literal,
    unterminated_dollar_quote,
    malformed_numeric_literal,
    invalid_placeholder,
    invalid_operator,
    invalid_utf8,
    invalid_character,

    pub fn message(self: @This()) []const u8 {
        return switch (self) {
            .unterminated_block_comment => "unterminated block comment",
            .unterminated_quoted_identifier => "unterminated quoted identifier",
            .empty_quoted_identifier => "quoted identifiers must not be empty",
            .unterminated_string_literal => "unterminated string literal",
            .unterminated_dollar_quote => "unterminated dollar-quoted string",
            .malformed_numeric_literal => "malformed numeric literal",
            .invalid_placeholder => "invalid positional placeholder",
            .invalid_operator => "invalid or incomplete SQL operator",
            .invalid_utf8 => "invalid UTF-8 in SQL input",
            .invalid_character => "invalid character in SQL input",
        };
    }
};

pub const LexDiagnostic = struct {
    kind: LexErrorKind,
    source_start: usize,
    source_end: usize,

    pub fn sourceSpan(self: @This()) token_mod.SourceSpan {
        return .{ .start = self.source_start, .end = self.source_end };
    }

    pub fn message(self: @This()) []const u8 {
        return self.kind.message();
    }
};

pub const TokenizeResult = union(enum) {
    tokens: std.ArrayListUnmanaged(Token),
    diagnostic: LexDiagnostic,
};

pub fn tokenizeAlloc(alloc: std.mem.Allocator, sql: []const u8) !std.ArrayListUnmanaged(Token) {
    return switch (try tokenizeDiagnosticAlloc(alloc, sql)) {
        .tokens => |tokens| tokens,
        .diagnostic => error.UnsupportedSqlShape,
    };
}

/// Tokenizes SQL while returning user-actionable lexical failures as bounded,
/// allocation-free diagnostics. A caller that receives `.tokens` owns them
/// and must call `freeTokens`.
pub fn tokenizeDiagnosticAlloc(alloc: std.mem.Allocator, sql: []const u8) !TokenizeResult {
    var diagnostic: LexDiagnostic = undefined;
    const tokens = tokenizeImpl(alloc, sql, &diagnostic) catch |err| switch (err) {
        error.UnsupportedSqlShape => return .{ .diagnostic = diagnostic },
        else => return err,
    };
    return .{ .tokens = tokens };
}

fn tokenizeImpl(alloc: std.mem.Allocator, sql: []const u8, diagnostic: *LexDiagnostic) !std.ArrayListUnmanaged(Token) {
    var tokens = std.ArrayListUnmanaged(Token).empty;
    errdefer freeTokens(alloc, &tokens);
    const estimated_capacity = estimateTokenCapacity(sql);
    if (estimated_capacity > 0) try tokens.ensureTotalCapacityPrecise(alloc, estimated_capacity);

    var i: usize = 0;
    while (i < sql.len) {
        const ch = sql[i];
        if (std.ascii.isWhitespace(ch)) {
            i += 1;
            continue;
        }
        if (ch == '-' and i + 1 < sql.len and sql[i + 1] == '-') {
            i += 2;
            while (i < sql.len and sql[i] != '\n' and sql[i] != '\r') {
                i += if (sql[i] >= 0x80) try utf8SequenceWidthAt(sql, i, diagnostic) else 1;
            }
            continue;
        }
        if (ch == '/' and i + 1 < sql.len and sql[i + 1] == '*') {
            const comment_start = i;
            i += 2;
            var depth: usize = 1;
            while (depth != 0) {
                if (i >= sql.len) return lexError(diagnostic, .unterminated_block_comment, comment_start, sql.len);
                if (sql[i] >= 0x80) {
                    i += try utf8SequenceWidthAt(sql, i, diagnostic);
                    continue;
                }
                if (i + 1 >= sql.len) return lexError(diagnostic, .unterminated_block_comment, comment_start, sql.len);
                if (sql[i] == '/' and sql[i + 1] == '*') {
                    depth += 1;
                    i += 2;
                } else if (sql[i] == '*' and sql[i + 1] == '/') {
                    depth -= 1;
                    i += 2;
                } else i += 1;
            }
            continue;
        }
        if (std.ascii.isAlphabetic(ch) or ch == '_' or ch >= 0x80) {
            const start = i;
            i += if (ch >= 0x80) try utf8SequenceWidthAt(sql, i, diagnostic) else 1;
            while (i < sql.len) {
                if (std.ascii.isAlphanumeric(sql[i]) or sql[i] == '_' or sql[i] == '$') {
                    i += 1;
                } else if (sql[i] >= 0x80) {
                    i += try utf8SequenceWidthAt(sql, i, diagnostic);
                } else break;
            }
            const end = i;
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
            var out = std.ArrayListUnmanaged(u8).empty;
            errdefer out.deinit(alloc);
            i += 1;
            while (i < sql.len) {
                if (sql[i] == '"') {
                    if (i + 1 < sql.len and sql[i + 1] == '"') {
                        try out.append(alloc, '"');
                        i += 2;
                        continue;
                    }
                    break;
                }
                if (sql[i] >= 0x80) {
                    const width = try utf8SequenceWidthAt(sql, i, diagnostic);
                    try out.appendSlice(alloc, sql[i .. i + width]);
                    i += width;
                } else {
                    try out.append(alloc, sql[i]);
                    i += 1;
                }
            }
            if (i >= sql.len) return lexError(diagnostic, .unterminated_quoted_identifier, source_start, sql.len);
            if (out.items.len == 0) return lexError(diagnostic, .empty_quoted_identifier, source_start, i + 1);
            const owned = try out.toOwnedSlice(alloc);
            var owned_transferred = false;
            errdefer if (!owned_transferred) alloc.free(owned);
            i += 1;
            const source_end = i;
            try tokens.append(alloc, .{
                .kind = .identifier,
                .text = owned,
                .owned = true,
                .source_start = source_start,
                .source_end = source_end,
            });
            owned_transferred = true;
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
                if (sql[i] >= 0x80) {
                    const width = try utf8SequenceWidthAt(sql, i, diagnostic);
                    try out.appendSlice(alloc, sql[i .. i + width]);
                    i += width;
                } else {
                    try out.append(alloc, sql[i]);
                    i += 1;
                }
            }
            if (i >= sql.len) return lexError(diagnostic, .unterminated_string_literal, source_start, sql.len);
            const owned = try out.toOwnedSlice(alloc);
            var owned_transferred = false;
            errdefer if (!owned_transferred) alloc.free(owned);
            i += 1;
            try tokens.append(alloc, .{ .kind = .string, .text = owned, .owned = true, .source_start = source_start, .source_end = i });
            owned_transferred = true;
            continue;
        }
        if (std.ascii.isDigit(ch) or (ch == '.' and i + 1 < sql.len and std.ascii.isDigit(sql[i + 1]))) {
            const start = i;
            i = try scanNumberEnd(sql, start, diagnostic);
            const end = i;
            try tokens.append(alloc, .{ .kind = .number, .text = sql[start..end] });
            continue;
        }
        if (ch == '$') {
            if (try sqlDollarQuoteAt(sql, i, diagnostic)) |quote| {
                try validateUtf8Range(sql, quote.body_start, quote.body_end, diagnostic);
                if (!quote.closed) return lexError(diagnostic, .unterminated_dollar_quote, i, sql.len);
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
            if (i == start + 1) return lexError(diagnostic, .invalid_placeholder, start, @min(start + 1, sql.len));
            if (i < sql.len and (std.ascii.isAlphanumeric(sql[i]) or sql[i] == '_' or sql[i] >= 0x80)) {
                const end = if (sql[i] >= 0x80) i + try utf8SequenceWidthAt(sql, i, diagnostic) else i + 1;
                return lexError(diagnostic, .invalid_placeholder, start, end);
            }
            try tokens.append(alloc, .{ .kind = .placeholder, .text = sql[start..i] });
            continue;
        }
        switch (ch) {
            ',' => {
                try tokens.append(alloc, .{ .kind = .comma, .text = sql[i .. i + 1] });
                i += 1;
            },
            '.' => {
                try tokens.append(alloc, .{ .kind = .dot, .text = sql[i .. i + 1] });
                i += 1;
            },
            ':' => {
                if (i + 1 < sql.len and sql[i + 1] == ':') {
                    try tokens.append(alloc, .{ .kind = .colon_colon, .text = sql[i .. i + 2] });
                    i += 2;
                } else {
                    try tokens.append(alloc, .{ .kind = .colon, .text = sql[i .. i + 1] });
                    i += 1;
                }
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
            },
            '[' => {
                try tokens.append(alloc, .{ .kind = .lbracket, .text = sql[i .. i + 1] });
                i += 1;
            },
            ']' => {
                try tokens.append(alloc, .{ .kind = .rbracket, .text = sql[i .. i + 1] });
                i += 1;
            },
            '@' => {
                if (i + 1 >= sql.len or sql[i + 1] != '>') return lexError(diagnostic, .invalid_operator, i, @min(i + 2, sql.len));
                try tokens.append(alloc, .{ .kind = .at_contains, .text = sql[i .. i + 2] });
                i += 2;
            },
            '&' => {
                if (i + 1 >= sql.len or sql[i + 1] != '&') return lexError(diagnostic, .invalid_operator, i, @min(i + 2, sql.len));
                try tokens.append(alloc, .{ .kind = .range_overlap, .text = sql[i .. i + 2] });
                i += 2;
            },
            '|' => {
                if (i + 1 >= sql.len or sql[i + 1] != '|') return lexError(diagnostic, .invalid_operator, i, @min(i + 2, sql.len));
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
                    return lexError(diagnostic, .invalid_operator, i, @min(i + 3, sql.len));
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
                } else return lexError(diagnostic, .invalid_operator, i, @min(i + 2, sql.len));
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
            else => return lexError(diagnostic, .invalid_character, i, i + 1),
        }
    }
    try finalizeTokenSourceSpans(sql, tokens.items);
    return tokens;
}

fn estimateTokenCapacity(sql: []const u8) usize {
    if (sql.len == 0) return 0;
    return @min(eager_token_capacity_limit, @min(sql.len, sql.len / 4 + 8));
}

fn scanNumberEnd(sql: []const u8, start: usize, diagnostic: *LexDiagnostic) !usize {
    var i = start;
    var has_decimal_point = false;

    if (sql[i] == '.') {
        has_decimal_point = true;
        i += 1;
        std.debug.assert(i < sql.len and std.ascii.isDigit(sql[i]));
        while (i < sql.len and std.ascii.isDigit(sql[i])) i += 1;
    } else {
        std.debug.assert(std.ascii.isDigit(sql[i]));
        while (i < sql.len and std.ascii.isDigit(sql[i])) i += 1;
        if (i < sql.len and sql[i] == '.') {
            if (i + 1 < sql.len and sql[i + 1] == '.') return lexError(diagnostic, .malformed_numeric_literal, start, i + 2);
            has_decimal_point = true;
            i += 1;
            while (i < sql.len and std.ascii.isDigit(sql[i])) i += 1;
        }
    }

    if (i < sql.len and (sql[i] == 'e' or sql[i] == 'E')) {
        i += 1;
        if (i < sql.len and (sql[i] == '+' or sql[i] == '-')) i += 1;
        if (i >= sql.len or !std.ascii.isDigit(sql[i])) {
            return lexError(diagnostic, .malformed_numeric_literal, start, @min(i + 1, sql.len));
        }
        while (i < sql.len and std.ascii.isDigit(sql[i])) i += 1;
    }

    // Never split a malformed decimal into two NUMBER tokens: doing so can
    // make invalid input appear valid once optional aliases are considered.
    if (has_decimal_point and i + 1 < sql.len and sql[i] == '.' and std.ascii.isDigit(sql[i + 1])) {
        return lexError(diagnostic, .malformed_numeric_literal, start, i + 2);
    }
    if (i < sql.len and (std.ascii.isAlphabetic(sql[i]) or sql[i] == '_' or sql[i] == '$' or sql[i] >= 0x80)) {
        const end = if (sql[i] >= 0x80) i + try utf8SequenceWidthAt(sql, i, diagnostic) else i + 1;
        return lexError(diagnostic, .malformed_numeric_literal, start, end);
    }
    return i;
}

fn utf8SequenceWidthAt(sql: []const u8, index: usize, diagnostic: *LexDiagnostic) !usize {
    const width = std.unicode.utf8ByteSequenceLength(sql[index]) catch
        return lexError(diagnostic, .invalid_utf8, index, index + 1);
    const end = index + width;
    if (end > sql.len) return lexError(diagnostic, .invalid_utf8, index, sql.len);
    _ = std.unicode.utf8Decode(sql[index..end]) catch
        return lexError(diagnostic, .invalid_utf8, index, end);
    return width;
}

fn validateUtf8Range(sql: []const u8, start: usize, end: usize, diagnostic: *LexDiagnostic) !void {
    var index = start;
    while (index < end) {
        index += if (sql[index] >= 0x80) try utf8SequenceWidthAt(sql, index, diagnostic) else 1;
    }
}

fn lexError(
    diagnostic: *LexDiagnostic,
    kind: LexErrorKind,
    source_start: usize,
    source_end: usize,
) error{UnsupportedSqlShape} {
    diagnostic.* = .{
        .kind = kind,
        .source_start = source_start,
        .source_end = source_end,
    };
    return error.UnsupportedSqlShape;
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
        if (token_start < sql_start or token_start + token.text.len > sql_end) return error.InvalidTokenSourceSpan;
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

fn sqlDollarQuoteAt(sql: []const u8, dollar: usize, diagnostic: *LexDiagnostic) !?SqlDollarQuote {
    if (dollar + 1 >= sql.len or sql[dollar] != '$') return null;
    if (std.ascii.isDigit(sql[dollar + 1])) return null;

    var delimiter_end = dollar + 1;
    if (sql[delimiter_end] == '$') {
        delimiter_end += 1;
    } else {
        if (!sqlDollarQuoteTagStart(sql[delimiter_end])) return null;
        delimiter_end += if (sql[delimiter_end] >= 0x80)
            try utf8SequenceWidthAt(sql, delimiter_end, diagnostic)
        else
            1;
        while (delimiter_end < sql.len and sqlDollarQuoteTagContinue(sql[delimiter_end])) {
            delimiter_end += if (sql[delimiter_end] >= 0x80)
                try utf8SequenceWidthAt(sql, delimiter_end, diagnostic)
            else
                1;
        }
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
    return std.ascii.isAlphabetic(ch) or ch == '_' or ch >= 0x80;
}

fn sqlDollarQuoteTagContinue(ch: u8) bool {
    return std.ascii.isAlphanumeric(ch) or ch == '_' or ch >= 0x80;
}

fn expectLexDiagnostic(
    alloc: std.mem.Allocator,
    sql: []const u8,
    expected_kind: LexErrorKind,
    expected_start: usize,
    expected_end: usize,
) !void {
    switch (try tokenizeDiagnosticAlloc(alloc, sql)) {
        .diagnostic => |diagnostic| {
            try std.testing.expectEqual(expected_kind, diagnostic.kind);
            try std.testing.expectEqual(expected_start, diagnostic.source_start);
            try std.testing.expectEqual(expected_end, diagnostic.source_end);
            try std.testing.expect(diagnostic.message().len != 0);
        },
        .tokens => |tokens_value| {
            var tokens = tokens_value;
            freeTokens(alloc, &tokens);
            return error.ExpectedLexDiagnostic;
        },
    }
}

test "sql adapter lexer returns structured source-aware diagnostics" {
    const alloc = std.testing.allocator;
    try expectLexDiagnostic(alloc, "SELECT /* unterminated", .unterminated_block_comment, 7, 22);
    try expectLexDiagnostic(alloc, "SELECT \"unterminated", .unterminated_quoted_identifier, 7, 20);
    try expectLexDiagnostic(alloc, "SELECT \"\"", .empty_quoted_identifier, 7, 9);
    try expectLexDiagnostic(alloc, "SELECT 'unterminated", .unterminated_string_literal, 7, 20);
    try expectLexDiagnostic(alloc, "SELECT $tag$unterminated", .unterminated_dollar_quote, 7, 24);
    try expectLexDiagnostic(alloc, "SELECT 1e+", .malformed_numeric_literal, 7, 10);
    try expectLexDiagnostic(alloc, "SELECT $name", .invalid_placeholder, 7, 8);
    try expectLexDiagnostic(alloc, "SELECT @", .invalid_operator, 7, 8);
    try expectLexDiagnostic(alloc, "SELECT \\", .invalid_character, 7, 8);
}

test "sql adapter lexer is allocation-failure safe" {
    const Runner = struct {
        fn run(alloc: std.mem.Allocator) !void {
            switch (try tokenizeDiagnosticAlloc(alloc, "SELECT \"a\"\"b\", 'it''s safe' FROM schema.docs")) {
                .diagnostic => return error.UnexpectedLexDiagnostic,
                .tokens => |tokens_value| {
                    var tokens = tokens_value;
                    defer freeTokens(alloc, &tokens);
                    try std.testing.expect(tokens.items.len != 0);
                },
            }
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
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

test "sql adapter lexer emits qualified-name and postfix-cast grammar terminals" {
    const alloc = std.testing.allocator;
    const sql = "SELECT docs.amount::text, $1::int";

    var tokens = try tokenizeAlloc(alloc, sql);
    defer freeTokens(alloc, &tokens);

    const expected_kinds = [_]TokenKind{
        .identifier,
        .identifier,
        .dot,
        .identifier,
        .colon_colon,
        .identifier,
        .comma,
        .placeholder,
        .colon_colon,
        .identifier,
    };
    const expected_text = [_][]const u8{
        "SELECT",
        "docs",
        ".",
        "amount",
        "::",
        "text",
        ",",
        "$1",
        "::",
        "int",
    };
    try std.testing.expectEqual(expected_kinds.len, tokens.items.len);
    for (tokens.items, expected_kinds, expected_text) |token, kind, text| {
        try std.testing.expectEqual(kind, token.kind);
        try std.testing.expectEqualStrings(text, token.text);
    }
    try std.testing.expectEqualStrings("docs", sql[tokens.items[1].source_start..tokens.items[1].source_end]);
}

test "sql adapter lexer rejects unterminated dollar quoted literals" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(error.UnsupportedSqlShape, tokenizeAlloc(alloc, "SELECT $tag$unterminated"));
}

test "sql adapter lexer handles PostgreSQL numeric literal forms" {
    const alloc = std.testing.allocator;
    const sql = "SELECT .5, 1., 1e2, 1.25E-3";

    var tokens = try tokenizeAlloc(alloc, sql);
    defer freeTokens(alloc, &tokens);

    const expected = [_][]const u8{ ".5", "1.", "1e2", "1.25E-3" };
    var number_index: usize = 0;
    for (tokens.items) |token| {
        if (token.kind != .number) continue;
        try std.testing.expect(number_index < expected.len);
        try std.testing.expectEqualStrings(expected[number_index], token.text);
        try std.testing.expectEqualStrings(token.text, sql[token.source_start..token.source_end]);
        number_index += 1;
    }
    try std.testing.expectEqual(expected.len, number_index);
}

test "sql adapter lexer rejects malformed numeric literals" {
    const alloc = std.testing.allocator;
    const invalid = [_][]const u8{
        "SELECT 1.2.3",
        "SELECT .5.6",
        "SELECT 1e",
        "SELECT 1e+",
        "SELECT 1alias",
        "SELECT .5alias",
    };
    for (invalid) |sql| {
        try std.testing.expectError(error.UnsupportedSqlShape, tokenizeAlloc(alloc, sql));
    }
}

test "sql adapter lexer decodes escaped quoted identifiers" {
    const alloc = std.testing.allocator;
    const sql = "SELECT \"a\"\"b\" FROM \"select\"";

    var tokens = try tokenizeAlloc(alloc, sql);
    defer freeTokens(alloc, &tokens);

    try std.testing.expectEqualStrings("a\"b", tokens.items[1].text);
    try std.testing.expect(tokens.items[1].owned);
    try std.testing.expectEqual(@as(?token_mod.TokenKeyword, null), tokens.items[1].keyword);
    try std.testing.expectEqualStrings("\"a\"\"b\"", sql[tokens.items[1].source_start..tokens.items[1].source_end]);
    try std.testing.expectEqualStrings("select", tokens.items[3].text);
    try std.testing.expectEqual(@as(?token_mod.TokenKeyword, null), tokens.items[3].keyword);
    try std.testing.expectError(error.UnsupportedSqlShape, tokenizeAlloc(alloc, "SELECT \"\""));
    try std.testing.expectError(error.UnsupportedSqlShape, tokenizeAlloc(alloc, "SELECT \"unterminated"));
}

test "sql adapter lexer accepts dollar signs inside unquoted identifiers" {
    const alloc = std.testing.allocator;
    const sql = "SELECT account$region FROM tenants";

    var tokens = try tokenizeAlloc(alloc, sql);
    defer freeTokens(alloc, &tokens);

    try std.testing.expectEqualStrings("account$region", tokens.items[1].text);
    try std.testing.expectEqual(TokenKind.identifier, tokens.items[1].kind);
    try std.testing.expectEqualStrings("account$region", sql[tokens.items[1].source_start..tokens.items[1].source_end]);
}

test "sql adapter lexer accepts validated Unicode unquoted identifiers" {
    var tokens = try tokenizeAlloc(std.testing.allocator, "SELECT café, \"日本語\", 'données', $étiquette$naïve$étiquette$ FROM données");
    defer freeTokens(std.testing.allocator, &tokens);
    try std.testing.expectEqualStrings("café", tokens.items[1].text);
    try std.testing.expectEqualStrings("日本語", tokens.items[3].text);
    try std.testing.expectEqualStrings("données", tokens.items[5].text);
    try std.testing.expectEqualStrings("naïve", tokens.items[7].text);
    try std.testing.expectEqualStrings("données", tokens.items[9].text);
    try std.testing.expectEqual(@as(?token_mod.TokenKeyword, null), tokens.items[1].keyword);
}

test "sql adapter lexer diagnoses invalid UTF-8 precisely" {
    try expectLexDiagnostic(std.testing.allocator, "SELECT \xff", .invalid_utf8, 7, 8);
    try expectLexDiagnostic(std.testing.allocator, "SELECT \xe2\x82", .invalid_utf8, 7, 9);
    try expectLexDiagnostic(std.testing.allocator, "SELECT \"\xff\"", .invalid_utf8, 8, 9);
    try expectLexDiagnostic(std.testing.allocator, "SELECT '\xff'", .invalid_utf8, 8, 9);
    try expectLexDiagnostic(std.testing.allocator, "SELECT -- \xff", .invalid_utf8, 10, 11);
    try expectLexDiagnostic(std.testing.allocator, "SELECT /* \xff */ 1", .invalid_utf8, 10, 11);
    try expectLexDiagnostic(std.testing.allocator, "SELECT /*\xff", .invalid_utf8, 9, 10);
    try expectLexDiagnostic(std.testing.allocator, "SELECT $$\xff$$", .invalid_utf8, 9, 10);
    try expectLexDiagnostic(std.testing.allocator, "SELECT $$\xff", .invalid_utf8, 9, 10);
    try expectLexDiagnostic(std.testing.allocator, "SELECT $é\xff$body$é\xff$", .invalid_utf8, 10, 11);
}

test "sql adapter lexer reports Unicode token continuations as one lexical failure" {
    try expectLexDiagnostic(std.testing.allocator, "SELECT $1é", .invalid_placeholder, 7, 11);
    try expectLexDiagnostic(std.testing.allocator, "SELECT 1é", .malformed_numeric_literal, 7, 10);
}

test "sql adapter lexer bounds speculative token capacity for large single-token input" {
    const alloc = std.testing.allocator;
    const sql = try alloc.alloc(u8, 64 * 1024);
    defer alloc.free(sql);
    @memset(sql, 'x');

    var tokens = try tokenizeAlloc(alloc, sql);
    defer freeTokens(alloc, &tokens);

    try std.testing.expectEqual(@as(usize, 1), tokens.items.len);
    try std.testing.expect(tokens.capacity <= eager_token_capacity_limit);
}

test "sql adapter lexer accepts nested block comments" {
    const alloc = std.testing.allocator;
    const sql = "SELECT /* outer /* nested */ still outer */ 1";

    var tokens = try tokenizeAlloc(alloc, sql);
    defer freeTokens(alloc, &tokens);

    try std.testing.expectEqual(@as(usize, 2), tokens.items.len);
    try std.testing.expectEqualStrings("SELECT", tokens.items[0].text);
    try std.testing.expectEqualStrings("1", tokens.items[1].text);
    try std.testing.expectError(error.UnsupportedSqlShape, tokenizeAlloc(alloc, "SELECT /* outer /* nested */"));
}

test "sql adapter lexer tokenizes graph relationship labels" {
    const alloc = std.testing.allocator;
    const sql_cases = [_][]const u8{
        "MATCH (doc)-[:cites]->(target) RETURN target",
        "MATCH (target)<-[:cites]-(doc) RETURN target",
        "MATCH (doc)-[:cites]->(target) WITH GRAPH docs_edge_graph ON usage_records START 'doc:root' RETURN doc.key AS source_id, target.key AS target_id ORDER BY target.depth ASC LIMIT 5",
    };

    for (sql_cases) |sql| {
        var tokens = try tokenizeAlloc(alloc, sql);
        defer freeTokens(alloc, &tokens);

        var saw_colon = false;
        var saw_edge_operator = false;
        var saw_match = false;
        var saw_graph = false;
        var saw_start = false;
        var saw_return = false;
        var saw_dot = false;
        for (tokens.items, 0..) |token, index| {
            saw_colon = saw_colon or token.kind == .colon;
            saw_edge_operator = saw_edge_operator or token.kind == .arrow_json;
            saw_edge_operator = saw_edge_operator or (token.kind == .lt and
                index + 1 < tokens.items.len and tokens.items[index + 1].kind == .minus);
            saw_match = saw_match or token.matchesKeywordTag(.match);
            saw_graph = saw_graph or token.matchesKeywordTag(.graph);
            saw_start = saw_start or token.matchesKeywordTag(.start);
            saw_return = saw_return or token.matchesKeywordTag(.@"return");
            saw_dot = saw_dot or token.kind == .dot;
        }
        try std.testing.expect(saw_colon);
        try std.testing.expect(saw_edge_operator);
        try std.testing.expect(saw_match);
        try std.testing.expect(saw_return);
        if (std.mem.indexOf(u8, sql, "WITH GRAPH") != null) {
            try std.testing.expect(saw_graph);
            try std.testing.expect(saw_start);
            try std.testing.expect(saw_dot);
        }
    }
}
