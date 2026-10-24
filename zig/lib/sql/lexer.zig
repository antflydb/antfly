// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const std = @import("std");

const token_mod = @import("token.zig");

// Reserve enough slots to keep ordinary statements allocation-efficient, but
// never size a Token array directly from untrusted SQL byte length. Large
// comments, literals, and identifiers may contain many bytes while producing
// zero or one token.
const eager_token_capacity_limit = 256;

/// Optional request-local preparation checkpoint. The caller preserves the
/// original interruption error outside the lexer. Token buffers never retain it.
pub const WorkControl = struct {
    ptr: *anyopaque,
    checkpoint: *const fn (*anyopaque) error{SqlPreparationInterrupted}!void,
};
const Work = struct {
    control: ?WorkControl,
    remaining: usize = 1024,
    fn poll(self: *Work) error{SqlPreparationInterrupted}!void {
        if (self.control == null) return;
        self.remaining -= 1;
        if (self.remaining == 0) {
            self.remaining = 1024;
            try self.check();
        }
    }
    fn check(self: *Work) error{SqlPreparationInterrupted}!void {
        if (self.control) |control| try control.checkpoint(control.ptr);
    }
};

pub fn copyControlled(comptime T: type, destination: []T, source: []const T, control: ?WorkControl) !void {
    std.debug.assert(destination.len == source.len);
    if (control == null) {
        @memcpy(destination, source);
        return;
    }
    const chunk = @max(@as(usize, 1), @as(usize, 4096) / @max(@as(usize, 1), @sizeOf(T)));
    var offset: usize = 0;
    while (offset < source.len) {
        const value = control.?;
        try value.checkpoint(value.ptr);
        const end = offset + @min(chunk, source.len - offset);
        @memcpy(destination[offset..end], source[offset..end]);
        offset = end;
    }
}

/// Preserve the original list until a replacement has been fully copied.
/// Only in-place resize may bypass the controlled copy; remap may move bytes.
pub fn ensureControlledCapacity(comptime T: type, alloc: std.mem.Allocator, list: *std.ArrayList(T), minimum: usize, control: ?WorkControl) !void {
    if (control == null or list.capacity >= minimum) return;
    const value = control.?;
    try value.checkpoint(value.ptr);
    const capacity = std.ArrayList(T).growCapacity(minimum);
    const original = list.allocatedSlice();
    if (list.capacity != 0 and alloc.resize(original, capacity)) {
        list.capacity = capacity;
        return;
    }
    const replacement = try alloc.alloc(T, capacity);
    errdefer alloc.free(replacement);
    try copyControlled(T, replacement[0..list.items.len], list.items, control);
    alloc.free(original);
    list.items.ptr = replacement.ptr;
    list.capacity = capacity;
}

const DecodedBuffer = struct {
    buffer: std.ArrayList(u8) = .empty,
    work: *Work,
    fn deinit(self: *DecodedBuffer, alloc: std.mem.Allocator) void {
        self.buffer.deinit(alloc);
    }
    fn append(self: *DecodedBuffer, alloc: std.mem.Allocator, byte: u8) !void {
        const required = std.math.add(usize, self.buffer.items.len, 1) catch return error.OutOfMemory;
        try ensureControlledCapacity(u8, alloc, &self.buffer, required, self.work.control);
        try self.buffer.append(alloc, byte);
    }
    fn appendSlice(self: *DecodedBuffer, alloc: std.mem.Allocator, bytes: []const u8) !void {
        // Escape and UTF-8 appends are at most four bytes. Large buffer copies
        // occur only in ensureControlledCapacity and toOwnedSlice below.
        std.debug.assert(bytes.len <= 4);
        const required = std.math.add(usize, self.buffer.items.len, bytes.len) catch return error.OutOfMemory;
        try ensureControlledCapacity(u8, alloc, &self.buffer, required, self.work.control);
        try self.buffer.appendSlice(alloc, bytes);
    }
    fn toOwnedSlice(self: *DecodedBuffer, alloc: std.mem.Allocator) ![]u8 {
        if (self.work.control == null) return self.buffer.toOwnedSlice(alloc);
        try self.work.check();
        const original = self.buffer.allocatedSlice();
        if (self.buffer.items.len == 0) {
            self.buffer.deinit(alloc);
            self.buffer = .empty;
            return &.{};
        }
        if (alloc.resize(original, self.buffer.items.len)) {
            const result = original[0..self.buffer.items.len];
            self.buffer = .empty;
            return result;
        }
        const replacement = try alloc.alloc(u8, self.buffer.items.len);
        errdefer alloc.free(replacement);
        try copyControlled(u8, replacement, self.buffer.items, self.work.control);
        self.buffer.deinit(alloc);
        self.buffer = .empty;
        return replacement;
    }
};

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
    invalid_escape_sequence,
    invalid_unicode_escape,
    invalid_string_encoding,

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
            .invalid_escape_sequence => "Unicode escape requires four or eight hexadecimal digits",
            .invalid_unicode_escape => "invalid Unicode codepoint or surrogate pair",
            .invalid_string_encoding => "string literal contains invalid UTF-8 or a zero byte",
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
    return tokenizeBoundedDiagnosticAlloc(alloc, sql, std.math.maxInt(usize));
}

/// Enforces the token quota before allocating or decoding the next token.
/// Whitespace/comments do not consume the quota, including trailing trivia.
pub fn tokenizeBoundedDiagnosticAlloc(alloc: std.mem.Allocator, sql: []const u8, max_tokens: usize) !TokenizeResult {
    return tokenizeBoundedDiagnosticControlledAlloc(alloc, sql, max_tokens, null);
}

pub fn tokenizeBoundedDiagnosticControlledAlloc(alloc: std.mem.Allocator, sql: []const u8, max_tokens: usize, control: ?WorkControl) !TokenizeResult {
    var work: Work = .{ .control = control };
    try work.check();
    var diagnostic: LexDiagnostic = undefined;
    const tokens = tokenizeImpl(alloc, sql, &diagnostic, max_tokens, &work) catch |err| switch (err) {
        error.UnsupportedSqlShape => return .{ .diagnostic = diagnostic },
        else => return err,
    };
    return .{ .tokens = tokens };
}

fn tokenizeImpl(alloc: std.mem.Allocator, sql: []const u8, diagnostic: *LexDiagnostic, max_tokens: usize, work: *Work) !std.ArrayListUnmanaged(Token) {
    var tokens = std.ArrayListUnmanaged(Token).empty;
    errdefer freeTokens(alloc, &tokens);
    const estimated_capacity = @min(max_tokens, estimateTokenCapacity(sql));
    if (estimated_capacity > 0) try tokens.ensureTotalCapacityPrecise(alloc, estimated_capacity);

    var i: usize = 0;
    while (i < sql.len) {
        try work.poll();
        const ch = sql[i];
        if (std.ascii.isWhitespace(ch)) {
            i += 1;
            continue;
        }
        if (ch == '-' and i + 1 < sql.len and sql[i + 1] == '-') {
            i += 2;
            while (i < sql.len and sql[i] != '\n' and sql[i] != '\r') {
                try work.poll();
                i += if (sql[i] >= 0x80) try utf8SequenceWidthAt(sql, i, diagnostic) else 1;
            }
            continue;
        }
        if (ch == '/' and i + 1 < sql.len and sql[i + 1] == '*') {
            const comment_start = i;
            i += 2;
            var depth: usize = 1;
            while (depth != 0) {
                try work.poll();
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
        if (tokens.items.len >= max_tokens) return error.SqlTokenLimitExceeded;
        const escape_string = (ch == 'e' or ch == 'E') and i + 1 < sql.len and sql[i + 1] == '\'';
        if (!escape_string and (std.ascii.isAlphabetic(ch) or ch == '_' or ch >= 0x80)) {
            const start = i;
            i += if (ch >= 0x80) try utf8SequenceWidthAt(sql, i, diagnostic) else 1;
            while (i < sql.len) {
                try work.poll();
                if (std.ascii.isAlphanumeric(sql[i]) or sql[i] == '_' or sql[i] == '$') {
                    i += 1;
                } else if (sql[i] >= 0x80) {
                    i += try utf8SequenceWidthAt(sql, i, diagnostic);
                } else break;
            }
            const end = i;
            try ensureControlledCapacity(Token, alloc, &tokens, tokens.items.len + 1, work.control);
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
            var out: DecodedBuffer = .{ .work = work };
            errdefer out.deinit(alloc);
            i += 1;
            while (i < sql.len) {
                try work.poll();
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
            if (out.buffer.items.len == 0) return lexError(diagnostic, .empty_quoted_identifier, source_start, i + 1);
            const owned = try out.toOwnedSlice(alloc);
            var owned_transferred = false;
            errdefer if (!owned_transferred) alloc.free(owned);
            i += 1;
            const source_end = i;
            try ensureControlledCapacity(Token, alloc, &tokens, tokens.items.len + 1, work.control);
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
        if (ch == '\'' or escape_string) {
            const source_start = i;
            var out: DecodedBuffer = .{ .work = work };
            errdefer out.deinit(alloc);
            i += if (escape_string) @as(usize, 2) else 1;
            while (i < sql.len) {
                try work.poll();
                if (sql[i] == '\'') {
                    if (i + 1 < sql.len and sql[i + 1] == '\'') {
                        try out.append(alloc, '\'');
                        i += 2;
                        continue;
                    }
                    // SQL continuation requires a newline in whitespace or a
                    // line comment. Block comments are not continuation trivia.
                    if (try stringContinuation(sql, i + 1, diagnostic, work)) |next| {
                        i = next + 1;
                        continue;
                    }
                    break;
                }
                if (escape_string and sql[i] == '\\') {
                    try appendStringEscape(alloc, &out, sql, &i, diagnostic);
                    continue;
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
            if (!try validStringEncoding(out.buffer.items, work))
                return lexError(diagnostic, .invalid_string_encoding, source_start, i + 1);
            const owned = try out.toOwnedSlice(alloc);
            var owned_transferred = false;
            errdefer if (!owned_transferred) alloc.free(owned);
            i += 1;
            try ensureControlledCapacity(Token, alloc, &tokens, tokens.items.len + 1, work.control);
            try tokens.append(alloc, .{ .kind = .string, .text = owned, .owned = true, .source_start = source_start, .source_end = i });
            owned_transferred = true;
            continue;
        }
        if (std.ascii.isDigit(ch) or (ch == '.' and i + 1 < sql.len and std.ascii.isDigit(sql[i + 1]))) {
            const start = i;
            i = try scanNumberEnd(sql, start, diagnostic, work);
            const end = i;
            try ensureControlledCapacity(Token, alloc, &tokens, tokens.items.len + 1, work.control);
            try tokens.append(alloc, .{ .kind = .number, .text = sql[start..end] });
            continue;
        }
        if (ch == '$') {
            if (try sqlDollarQuoteAt(alloc, sql, i, diagnostic, work)) |quote| {
                try validateUtf8Range(sql, quote.body_start, quote.body_end, diagnostic, work);
                if (!quote.closed) return lexError(diagnostic, .unterminated_dollar_quote, i, sql.len);
                try ensureControlledCapacity(Token, alloc, &tokens, tokens.items.len + 1, work.control);
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
            while (i < sql.len and std.ascii.isDigit(sql[i])) {
                try work.poll();
                i += 1;
            }
            if (i == start + 1) return lexError(diagnostic, .invalid_placeholder, start, @min(start + 1, sql.len));
            if (i < sql.len and (std.ascii.isAlphanumeric(sql[i]) or sql[i] == '_' or sql[i] >= 0x80)) {
                const end = if (sql[i] >= 0x80) i + try utf8SequenceWidthAt(sql, i, diagnostic) else i + 1;
                return lexError(diagnostic, .invalid_placeholder, start, end);
            }
            try ensureControlledCapacity(Token, alloc, &tokens, tokens.items.len + 1, work.control);
            try tokens.append(alloc, .{ .kind = .placeholder, .text = sql[start..i] });
            continue;
        }
        switch (ch) {
            ',' => {
                try ensureControlledCapacity(Token, alloc, &tokens, tokens.items.len + 1, work.control);
                try tokens.append(alloc, .{ .kind = .comma, .text = sql[i .. i + 1] });
                i += 1;
            },
            '.' => {
                try ensureControlledCapacity(Token, alloc, &tokens, tokens.items.len + 1, work.control);
                try tokens.append(alloc, .{ .kind = .dot, .text = sql[i .. i + 1] });
                i += 1;
            },
            ':' => {
                if (i + 1 < sql.len and sql[i + 1] == ':') {
                    try ensureControlledCapacity(Token, alloc, &tokens, tokens.items.len + 1, work.control);
                    try tokens.append(alloc, .{ .kind = .colon_colon, .text = sql[i .. i + 2] });
                    i += 2;
                } else {
                    try ensureControlledCapacity(Token, alloc, &tokens, tokens.items.len + 1, work.control);
                    try tokens.append(alloc, .{ .kind = .colon, .text = sql[i .. i + 1] });
                    i += 1;
                }
            },
            '*' => {
                try ensureControlledCapacity(Token, alloc, &tokens, tokens.items.len + 1, work.control);
                try tokens.append(alloc, .{ .kind = .star, .text = sql[i .. i + 1] });
                i += 1;
            },
            '+' => {
                try ensureControlledCapacity(Token, alloc, &tokens, tokens.items.len + 1, work.control);
                try tokens.append(alloc, .{ .kind = .plus, .text = sql[i .. i + 1] });
                i += 1;
            },
            '/' => {
                try ensureControlledCapacity(Token, alloc, &tokens, tokens.items.len + 1, work.control);
                try tokens.append(alloc, .{ .kind = .slash, .text = sql[i .. i + 1] });
                i += 1;
            },
            '%' => {
                try ensureControlledCapacity(Token, alloc, &tokens, tokens.items.len + 1, work.control);
                try tokens.append(alloc, .{ .kind = .percent, .text = sql[i .. i + 1] });
                i += 1;
            },
            '(' => {
                try ensureControlledCapacity(Token, alloc, &tokens, tokens.items.len + 1, work.control);
                try tokens.append(alloc, .{ .kind = .lparen, .text = sql[i .. i + 1] });
                i += 1;
            },
            ')' => {
                try ensureControlledCapacity(Token, alloc, &tokens, tokens.items.len + 1, work.control);
                try tokens.append(alloc, .{ .kind = .rparen, .text = sql[i .. i + 1] });
                i += 1;
            },
            '[' => {
                try ensureControlledCapacity(Token, alloc, &tokens, tokens.items.len + 1, work.control);
                try tokens.append(alloc, .{ .kind = .lbracket, .text = sql[i .. i + 1] });
                i += 1;
            },
            ']' => {
                try ensureControlledCapacity(Token, alloc, &tokens, tokens.items.len + 1, work.control);
                try tokens.append(alloc, .{ .kind = .rbracket, .text = sql[i .. i + 1] });
                i += 1;
            },
            '@' => {
                if (i + 1 >= sql.len or sql[i + 1] != '>') return lexError(diagnostic, .invalid_operator, i, @min(i + 2, sql.len));
                try ensureControlledCapacity(Token, alloc, &tokens, tokens.items.len + 1, work.control);
                try tokens.append(alloc, .{ .kind = .at_contains, .text = sql[i .. i + 2] });
                i += 2;
            },
            '&' => {
                if (i + 1 >= sql.len or sql[i + 1] != '&') return lexError(diagnostic, .invalid_operator, i, @min(i + 2, sql.len));
                try ensureControlledCapacity(Token, alloc, &tokens, tokens.items.len + 1, work.control);
                try tokens.append(alloc, .{ .kind = .range_overlap, .text = sql[i .. i + 2] });
                i += 2;
            },
            '|' => {
                if (i + 1 >= sql.len or sql[i + 1] != '|') return lexError(diagnostic, .invalid_operator, i, @min(i + 2, sql.len));
                try ensureControlledCapacity(Token, alloc, &tokens, tokens.items.len + 1, work.control);
                try tokens.append(alloc, .{ .kind = .pipe_concat, .text = sql[i .. i + 2] });
                i += 2;
            },
            '?' => {
                if (i + 1 < sql.len and sql[i + 1] == '|') {
                    try ensureControlledCapacity(Token, alloc, &tokens, tokens.items.len + 1, work.control);
                    try tokens.append(alloc, .{ .kind = .question_any, .text = sql[i .. i + 2] });
                    i += 2;
                } else if (i + 1 < sql.len and sql[i + 1] == '&') {
                    try ensureControlledCapacity(Token, alloc, &tokens, tokens.items.len + 1, work.control);
                    try tokens.append(alloc, .{ .kind = .question_all, .text = sql[i .. i + 2] });
                    i += 2;
                } else {
                    try ensureControlledCapacity(Token, alloc, &tokens, tokens.items.len + 1, work.control);
                    try tokens.append(alloc, .{ .kind = .question, .text = sql[i .. i + 1] });
                    i += 1;
                }
            },
            '~' => {
                if (i + 1 < sql.len and sql[i + 1] == '*') {
                    try ensureControlledCapacity(Token, alloc, &tokens, tokens.items.len + 1, work.control);
                    try tokens.append(alloc, .{ .kind = .regex_imatch, .text = sql[i .. i + 2] });
                    i += 2;
                } else {
                    try ensureControlledCapacity(Token, alloc, &tokens, tokens.items.len + 1, work.control);
                    try tokens.append(alloc, .{ .kind = .regex_match, .text = sql[i .. i + 1] });
                    i += 1;
                }
            },
            '#' => {
                if (i + 2 < sql.len and sql[i + 1] == '>' and sql[i + 2] == '>') {
                    try ensureControlledCapacity(Token, alloc, &tokens, tokens.items.len + 1, work.control);
                    try tokens.append(alloc, .{ .kind = .path_arrow_text, .text = sql[i .. i + 3] });
                    i += 3;
                } else if (i + 1 < sql.len and sql[i + 1] == '>') {
                    try ensureControlledCapacity(Token, alloc, &tokens, tokens.items.len + 1, work.control);
                    try tokens.append(alloc, .{ .kind = .path_arrow_json, .text = sql[i .. i + 2] });
                    i += 2;
                } else {
                    return lexError(diagnostic, .invalid_operator, i, @min(i + 3, sql.len));
                }
            },
            ';' => {
                try ensureControlledCapacity(Token, alloc, &tokens, tokens.items.len + 1, work.control);
                try tokens.append(alloc, .{ .kind = .semicolon, .text = sql[i .. i + 1] });
                i += 1;
            },
            '=' => {
                try ensureControlledCapacity(Token, alloc, &tokens, tokens.items.len + 1, work.control);
                try tokens.append(alloc, .{ .kind = .eq, .text = sql[i .. i + 1] });
                i += 1;
            },
            '!' => {
                if (i + 1 < sql.len and sql[i + 1] == '=') {
                    try ensureControlledCapacity(Token, alloc, &tokens, tokens.items.len + 1, work.control);
                    try tokens.append(alloc, .{ .kind = .neq, .text = sql[i .. i + 2] });
                    i += 2;
                } else if (i + 1 < sql.len and sql[i + 1] == '~') {
                    if (i + 2 < sql.len and sql[i + 2] == '*') {
                        try ensureControlledCapacity(Token, alloc, &tokens, tokens.items.len + 1, work.control);
                        try tokens.append(alloc, .{ .kind = .regex_not_imatch, .text = sql[i .. i + 3] });
                        i += 3;
                    } else {
                        try ensureControlledCapacity(Token, alloc, &tokens, tokens.items.len + 1, work.control);
                        try tokens.append(alloc, .{ .kind = .regex_not_match, .text = sql[i .. i + 2] });
                        i += 2;
                    }
                } else return lexError(diagnostic, .invalid_operator, i, @min(i + 2, sql.len));
            },
            '<' => {
                if (i + 1 < sql.len and sql[i + 1] == '=') {
                    try ensureControlledCapacity(Token, alloc, &tokens, tokens.items.len + 1, work.control);
                    try tokens.append(alloc, .{ .kind = .lte, .text = sql[i .. i + 2] });
                    i += 2;
                } else if (i + 1 < sql.len and sql[i + 1] == '>') {
                    try ensureControlledCapacity(Token, alloc, &tokens, tokens.items.len + 1, work.control);
                    try tokens.append(alloc, .{ .kind = .neq, .text = sql[i .. i + 2] });
                    i += 2;
                } else {
                    try ensureControlledCapacity(Token, alloc, &tokens, tokens.items.len + 1, work.control);
                    try tokens.append(alloc, .{ .kind = .lt, .text = sql[i .. i + 1] });
                    i += 1;
                }
            },
            '>' => {
                if (i + 1 < sql.len and sql[i + 1] == '=') {
                    try ensureControlledCapacity(Token, alloc, &tokens, tokens.items.len + 1, work.control);
                    try tokens.append(alloc, .{ .kind = .gte, .text = sql[i .. i + 2] });
                    i += 2;
                } else {
                    try ensureControlledCapacity(Token, alloc, &tokens, tokens.items.len + 1, work.control);
                    try tokens.append(alloc, .{ .kind = .gt, .text = sql[i .. i + 1] });
                    i += 1;
                }
            },
            '-' => {
                if (i + 1 < sql.len and sql[i + 1] == '>' and i + 2 < sql.len and sql[i + 2] == '>') {
                    try ensureControlledCapacity(Token, alloc, &tokens, tokens.items.len + 1, work.control);
                    try tokens.append(alloc, .{ .kind = .arrow_text, .text = sql[i .. i + 3] });
                    i += 3;
                } else if (i + 1 < sql.len and sql[i + 1] == '>') {
                    try ensureControlledCapacity(Token, alloc, &tokens, tokens.items.len + 1, work.control);
                    try tokens.append(alloc, .{ .kind = .arrow_json, .text = sql[i .. i + 2] });
                    i += 2;
                } else {
                    try ensureControlledCapacity(Token, alloc, &tokens, tokens.items.len + 1, work.control);
                    try tokens.append(alloc, .{ .kind = .minus, .text = sql[i .. i + 1] });
                    i += 1;
                }
            },
            else => return lexError(diagnostic, .invalid_character, i, i + 1),
        }
    }
    try finalizeTokenSourceSpans(sql, tokens.items, work);
    try work.check();
    return tokens;
}

fn estimateTokenCapacity(sql: []const u8) usize {
    if (sql.len == 0) return 0;
    return @min(eager_token_capacity_limit, @min(sql.len, sql.len / 4 + 8));
}

fn stringContinuation(sql: []const u8, start: usize, diagnostic: *LexDiagnostic, work: *Work) !?usize {
    var pos = start;
    var newline = false;
    while (pos < sql.len) {
        try work.poll();
        if (std.ascii.isWhitespace(sql[pos])) {
            newline = newline or sql[pos] == '\n' or sql[pos] == '\r';
            pos += 1;
        } else if (sql[pos] == '-' and pos + 1 < sql.len and sql[pos + 1] == '-') {
            pos += 2;
            while (pos < sql.len and sql[pos] != '\n' and sql[pos] != '\r') {
                try work.poll();
                pos += if (sql[pos] >= 0x80) try utf8SequenceWidthAt(sql, pos, diagnostic) else 1;
            }
        } else break;
    }
    return if (newline and pos < sql.len and sql[pos] == '\'') pos else null;
}

fn hexDigit(byte: u8) ?u4 {
    return switch (byte) {
        '0'...'9' => @intCast(byte - '0'),
        'a'...'f' => @intCast(byte - 'a' + 10),
        'A'...'F' => @intCast(byte - 'A' + 10),
        else => null,
    };
}

fn unicodeEscape(sql: []const u8, pos: *usize, diagnostic: *LexDiagnostic) !u32 {
    const start = pos.*;
    std.debug.assert(sql[start] == '\\');
    const digits: usize = if (sql[start + 1] == 'u') 4 else 8;
    pos.* += 2;
    var value: u32 = 0;
    for (0..digits) |_| {
        if (pos.* >= sql.len) return lexError(diagnostic, .invalid_escape_sequence, start, sql.len);
        const digit = hexDigit(sql[pos.*]) orelse return lexError(diagnostic, .invalid_escape_sequence, start, pos.* + 1);
        value = (value << 4) | digit;
        pos.* += 1;
    }
    return value;
}

/// Decodes into the token's single owned buffer. Escapes never increase its
/// size beyond input bytes. Validate byte escapes together at literal end so
/// independently escaped bytes may form one valid UTF-8 character.
fn appendStringEscape(alloc: std.mem.Allocator, out: *DecodedBuffer, sql: []const u8, pos: *usize, diagnostic: *LexDiagnostic) !void {
    const start = pos.*;
    if (start + 1 >= sql.len) return lexError(diagnostic, .unterminated_string_literal, start, sql.len);
    const ch = sql[start + 1];
    if (ch == 'u' or ch == 'U') {
        var value = try unicodeEscape(sql, pos, diagnostic);
        if (value >= 0xd800 and value <= 0xdbff) {
            if (pos.* + 1 >= sql.len or sql[pos.*] != '\\' or (sql[pos.* + 1] != 'u' and sql[pos.* + 1] != 'U'))
                return lexError(diagnostic, .invalid_unicode_escape, start, pos.*);
            const tail = try unicodeEscape(sql, pos, diagnostic);
            if (tail < 0xdc00 or tail > 0xdfff) return lexError(diagnostic, .invalid_unicode_escape, start, pos.*);
            value = 0x10000 + ((value - 0xd800) << 10) + tail - 0xdc00;
        }
        if (value == 0 or value > 0x10ffff or (value >= 0xd800 and value <= 0xdfff))
            return lexError(diagnostic, .invalid_unicode_escape, start, pos.*);
        var buffer: [4]u8 = undefined;
        const width = std.unicode.utf8Encode(@intCast(value), &buffer) catch unreachable;
        try out.appendSlice(alloc, buffer[0..width]);
        return;
    }
    pos.* += 2;
    if (ch >= '0' and ch <= '7') {
        var value: u16 = ch - '0';
        var digits: usize = 1;
        while (digits < 3 and pos.* < sql.len and sql[pos.*] >= '0' and sql[pos.*] <= '7') : (digits += 1) {
            value = value * 8 + sql[pos.*] - '0';
            pos.* += 1;
        }
        try out.append(alloc, @truncate(value));
    } else if (ch == 'x' and pos.* < sql.len and hexDigit(sql[pos.*]) != null) {
        var value: u8 = hexDigit(sql[pos.*]).?;
        pos.* += 1;
        if (pos.* < sql.len) if (hexDigit(sql[pos.*])) |digit| {
            value = value * 16 + digit;
            pos.* += 1;
        };
        try out.append(alloc, value);
    } else if (ch >= 0x80) {
        const width = try utf8SequenceWidthAt(sql, start + 1, diagnostic);
        try out.appendSlice(alloc, sql[start + 1 .. start + 1 + width]);
        pos.* = start + 1 + width;
    } else try out.append(alloc, switch (ch) {
        'b' => 8,
        'f' => 12,
        'n' => '\n',
        'r' => '\r',
        't' => '\t',
        else => ch,
    });
}

test "PostgreSQL escape strings preserve oracle values and lexical failures" {
    const Golden = struct { format: u32, entries: []const struct { id: []const u8, expression: []const u8, value: ?[]const u8, sqlstate: ?[]const u8 } };
    const a = std.testing.allocator;
    const golden = try std.json.parseFromSlice(Golden, a, @embedFile("testdata/escape-postgres.json"), .{});
    defer golden.deinit();
    try std.testing.expectEqual(@as(u32, 1), golden.value.format);
    for (golden.value.entries) |case| {
        errdefer std.debug.print("PostgreSQL lexical fixture {s}: {s}\n", .{ case.id, case.expression });
        switch (try tokenizeDiagnosticAlloc(a, case.expression)) {
            .tokens => |value| {
                var tokens = value;
                defer freeTokens(a, &tokens);
                if (case.sqlstate) |state| {
                    // These lexical tokens are individually valid, but not a
                    // single literal expression (e.g. space-only adjacency).
                    try std.testing.expectEqualStrings("42601", state);
                    try std.testing.expect(tokens.items.len != 1);
                } else {
                    try std.testing.expectEqual(@as(usize, 1), tokens.items.len);
                    try std.testing.expectEqual(TokenKind.string, tokens.items[0].kind);
                    try std.testing.expectEqualStrings(case.value.?, tokens.items[0].text);
                    try std.testing.expectEqual(@as(usize, 0), tokens.items[0].source_start);
                    try std.testing.expectEqual(case.expression.len, tokens.items[0].source_end);
                }
            },
            .diagnostic => |failure| {
                const state = switch (failure.kind) {
                    .invalid_escape_sequence => "22025",
                    .invalid_string_encoding => "22021",
                    else => "42601",
                };
                try std.testing.expectEqualStrings(case.sqlstate orelse return error.UnexpectedLexicalFailure, state);
                try std.testing.expect(failure.source_start <= failure.source_end);
                try std.testing.expect(failure.source_end <= case.expression.len);
            },
        }
    }
}

fn escapeStringAllocationCase(a: std.mem.Allocator) !void {
    var tokens = try tokenizeAlloc(a, "E'a\\n\\xC3\\xA9\\uD83D\\uDE00'\n'\\t' \"quoted\" E'last'");
    defer freeTokens(a, &tokens);
    try std.testing.expectEqualStrings("a\né😀\t", tokens.items[0].text);
    try std.testing.expectEqual(@as(usize, 3), tokens.items.len);
}

test "PostgreSQL escape string ownership unwinds allocation failures" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, escapeStringAllocationCase, .{});
    try std.testing.expectError(error.SqlTokenLimitExceeded, tokenizeBoundedDiagnosticAlloc(std.testing.allocator, "SELECT E'\\uZZZZ'", 1));
    // Continuation trivia must not bypass the usual comment UTF-8 validator.
    const invalid = try tokenizeDiagnosticAlloc(std.testing.allocator, "E'a' -- \xff\n'b'");
    try std.testing.expectEqual(LexErrorKind.invalid_utf8, invalid.diagnostic.kind);
}

test "bounded lexer rejects before decoding excess token and allows trailing trivia" {
    const allocator = std.testing.allocator;
    var accepted = (try tokenizeBoundedDiagnosticAlloc(allocator, "select x -- trailing comment\n /* nested /* comment */ */", 2)).tokens;
    defer freeTokens(allocator, &accepted);
    try std.testing.expectEqual(@as(usize, 2), accepted.items.len);
    // The excess token is an unterminated string; quota must win before scanning
    // or allocating it, rather than performing work merely to reject afterward.
    try std.testing.expectError(error.SqlTokenLimitExceeded, tokenizeBoundedDiagnosticAlloc(allocator, "select x 'unterminated", 2));
    var empty_buffer: [0]u8 = .{};
    var fixed = std.heap.FixedBufferAllocator.init(&empty_buffer);
    const empty = try tokenizeBoundedDiagnosticAlloc(fixed.allocator(), " -- no tokens", 0);
    try std.testing.expectEqual(@as(usize, 0), empty.tokens.items.len);
    try std.testing.expectError(error.SqlTokenLimitExceeded, tokenizeBoundedDiagnosticAlloc(fixed.allocator(), "select", 0));
}

test "bounded lexer releases owned tokens on quota failure" {
    try std.testing.expectError(error.SqlTokenLimitExceeded, tokenizeBoundedDiagnosticAlloc(std.testing.allocator, "\"quoted\" 'string' excess", 2));
}

fn scanDecimalDigitsEnd(sql: []const u8, start: usize, work: *Work) !usize {
    var i = start;
    while (i < sql.len and std.ascii.isDigit(sql[i])) {
        try work.poll();
        i += 1;
        if (i + 1 < sql.len and sql[i] == '_' and std.ascii.isDigit(sql[i + 1])) i += 1;
    }
    return i;
}

fn scanNumberEnd(sql: []const u8, start: usize, diagnostic: *LexDiagnostic, work: *Work) !usize {
    var i = start;
    var has_decimal_point = false;

    if (sql[i] == '.') {
        has_decimal_point = true;
        i += 1;
        std.debug.assert(i < sql.len and std.ascii.isDigit(sql[i]));
        i = try scanDecimalDigitsEnd(sql, i, work);
    } else {
        std.debug.assert(std.ascii.isDigit(sql[i]));
        i = try scanDecimalDigitsEnd(sql, i, work);
        if (i < sql.len and sql[i] == '.') {
            if (i + 1 < sql.len and sql[i + 1] == '.') return lexError(diagnostic, .malformed_numeric_literal, start, i + 2);
            has_decimal_point = true;
            i += 1;
            i = try scanDecimalDigitsEnd(sql, i, work);
        }
    }

    if (i < sql.len and (sql[i] == 'e' or sql[i] == 'E')) {
        i += 1;
        if (i < sql.len and (sql[i] == '+' or sql[i] == '-')) i += 1;
        if (i >= sql.len or !std.ascii.isDigit(sql[i])) {
            return lexError(diagnostic, .malformed_numeric_literal, start, @min(i + 1, sql.len));
        }
        i = try scanDecimalDigitsEnd(sql, i, work);
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

fn validateUtf8Range(sql: []const u8, start: usize, end: usize, diagnostic: *LexDiagnostic, work: *Work) !void {
    var index = start;
    while (index < end) {
        try work.poll();
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

fn finalizeTokenSourceSpans(sql: []const u8, tokens: []Token, work: *Work) !void {
    const sql_start = @intFromPtr(sql.ptr);
    const sql_end = sql_start + sql.len;
    for (tokens) |*token| {
        try work.poll();
        if (token.source_end > token.source_start or token.text.len == 0) continue;
        const token_start = @intFromPtr(token.text.ptr);
        if (token_start < sql_start or token_start + token.text.len > sql_end) return error.InvalidTokenSourceSpan;
        token.source_start = token_start - sql_start;
        token.source_end = token.source_start + token.text.len;
    }
}

fn validStringEncoding(bytes: []const u8, work: *Work) !bool {
    var index: usize = 0;
    while (index < bytes.len) {
        try work.poll();
        if (bytes[index] == 0) return false;
        const width = std.unicode.utf8ByteSequenceLength(bytes[index]) catch return false;
        if (width > bytes.len - index) return false;
        _ = std.unicode.utf8Decode(bytes[index .. index + width]) catch return false;
        index += width;
    }
    return true;
}

// KMP bounds both delimiter preprocessing and search work, including repeated
// delimiter prefixes. A large delimiter cannot hide one unbounded comparison.
fn findDelimiter(alloc: std.mem.Allocator, sql: []const u8, start: usize, delimiter: []const u8, work: *Work) !?usize {
    if (work.control == null) return std.mem.indexOfPos(u8, sql, start, delimiter);
    const prefix = try alloc.alloc(usize, delimiter.len);
    defer alloc.free(prefix);
    prefix[0] = 0;
    var matched: usize = 0;
    var index: usize = 1;
    while (index < delimiter.len) {
        try work.poll();
        if (delimiter[index] == delimiter[matched]) {
            matched += 1;
            prefix[index] = matched;
            index += 1;
        } else if (matched != 0) {
            matched = prefix[matched - 1];
        } else {
            prefix[index] = 0;
            index += 1;
        }
    }
    matched = 0;
    index = start;
    while (index < sql.len) {
        try work.poll();
        if (sql[index] == delimiter[matched]) {
            matched += 1;
            index += 1;
            if (matched == delimiter.len) return index - delimiter.len;
        } else if (matched != 0) {
            matched = prefix[matched - 1];
        } else index += 1;
    }
    return null;
}

const SqlDollarQuote = struct {
    body_start: usize,
    body_end: usize,
    token_end: usize,
    closed: bool,
};

fn sqlDollarQuoteAt(alloc: std.mem.Allocator, sql: []const u8, dollar: usize, diagnostic: *LexDiagnostic, work: *Work) !?SqlDollarQuote {
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
            try work.poll();
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
    const body_end = (try findDelimiter(alloc, sql, body_start, delimiter, work)) orelse return .{
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
    const sql = "SELECT .5, 1., 1e2, 1.25E-3, 1_0, .5_6, 1_0., 1.2_3, 1e1_0";

    var tokens = try tokenizeAlloc(alloc, sql);
    defer freeTokens(alloc, &tokens);

    const expected = [_][]const u8{ ".5", "1.", "1e2", "1.25E-3", "1_0", ".5_6", "1_0.", "1.2_3", "1e1_0" };
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
        "SELECT 1_",
        "SELECT 1__0",
        "SELECT 1._0",
        "SELECT 1e_2",
        "SELECT .5_",
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

test "controlled SQL lexer interrupts long trivia and single token scans without leaked tokens" {
    const Interrupt = struct {
        checks: usize = 0,
        fn checkpoint(raw: *anyopaque) error{SqlPreparationInterrupted}!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.checks == 3) return error.SqlPreparationInterrupted;
        }
    };
    const body: [8192]u8 = @splat('x');
    const digits: [8192]u8 = @splat('1');
    for ([_][]const u8{
        "/*" ++ body ++ "*/ SELECT 1",
        "SELECT 'owned', /*" ++ body ++ "*/ 1",
        "--" ++ body ++ "\nSELECT 1",
        "SELECT '" ++ body ++ "'",
        "SELECT \"" ++ body ++ "\"",
        "SELECT $$" ++ body ++ "$$",
        "SELECT $" ++ body ++ "$literal$" ++ body ++ "$",
        "SELECT " ++ digits,
        "SELECT $" ++ digits,
        "SELECT 'a'\n--" ++ body ++ "\n'b'",
    }) |statement| {
        var interrupt: Interrupt = .{};
        try std.testing.expectError(error.SqlPreparationInterrupted, tokenizeBoundedDiagnosticControlledAlloc(std.testing.allocator, statement, 128, .{ .ptr = &interrupt, .checkpoint = Interrupt.checkpoint }));
        try std.testing.expectEqual(@as(usize, 3), interrupt.checks);
    }
}

test "controlled lexer interrupted buffer relocation preserves original owner and frees replacement" {
    const Interrupt = struct {
        checks: usize = 0,
        fn checkpoint(raw: *anyopaque) error{SqlPreparationInterrupted}!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.checks == 3) return error.SqlPreparationInterrupted;
        }
    };
    var no_resize = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
    const alloc = no_resize.allocator();
    var bytes: std.ArrayList(u8) = .empty;
    defer bytes.deinit(alloc);
    try bytes.ensureTotalCapacityPrecise(alloc, 8192);
    const contents: [8192]u8 = @splat('q');
    bytes.appendSliceAssumeCapacity(&contents);
    const original_pointer = bytes.items.ptr;
    var interrupt: Interrupt = .{};
    try std.testing.expectError(error.SqlPreparationInterrupted, ensureControlledCapacity(u8, alloc, &bytes, 8193, .{ .ptr = &interrupt, .checkpoint = Interrupt.checkpoint }));
    try std.testing.expectEqual(original_pointer, bytes.items.ptr);
    try std.testing.expectEqual(@as(usize, 8192), bytes.capacity);
    try std.testing.expectEqualSlices(u8, &contents, bytes.items);
    try std.testing.expectEqual(@as(usize, 8192), no_resize.allocated_bytes - no_resize.freed_bytes);
}

test "controlled lexer interrupted owned slice copy retains original decoded buffer" {
    const Interrupt = struct {
        checks: usize = 0,
        fn checkpoint(raw: *anyopaque) error{SqlPreparationInterrupted}!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.checks == 3) return error.SqlPreparationInterrupted;
        }
    };
    var no_resize = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
    const alloc = no_resize.allocator();
    var interrupt: Interrupt = .{};
    var work: Work = .{ .control = .{ .ptr = &interrupt, .checkpoint = Interrupt.checkpoint } };
    var decoded: DecodedBuffer = .{ .work = &work };
    defer decoded.deinit(alloc);
    try decoded.buffer.ensureTotalCapacityPrecise(alloc, 16384);
    const contents: [8192]u8 = @splat('q');
    decoded.buffer.appendSliceAssumeCapacity(&contents);
    const original_pointer = decoded.buffer.items.ptr;
    try std.testing.expectError(error.SqlPreparationInterrupted, decoded.toOwnedSlice(alloc));
    try std.testing.expectEqual(original_pointer, decoded.buffer.items.ptr);
    try std.testing.expectEqual(@as(usize, 16384), decoded.buffer.capacity);
    try std.testing.expectEqualSlices(u8, &contents, decoded.buffer.items);
    try std.testing.expectEqual(@as(usize, 16384), no_resize.allocated_bytes - no_resize.freed_bytes);
}
