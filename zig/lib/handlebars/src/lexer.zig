// Copyright 2026 Antfly, Inc.
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

pub const TokenKind = enum {
    // Content
    content,

    // Mustache delimiters
    open, // {{
    close, // }}
    open_unescaped, // {{{
    close_unescaped, // }}}
    open_block, // {{#
    open_end_block, // {{/
    open_inverse, // {{^
    open_partial, // {{>

    // Expression tokens
    open_sexpr, // (
    close_sexpr, // )
    equals, // =
    data, // @
    sep, // . or /
    open_block_params, // as |
    close_block_params, // |

    // Values
    id, // identifier
    string, // "..." or '...'
    number, // 123 or 1.5
    boolean, // true or false

    // Special
    comment, // {{!...}} or {{!--...--}}
    open_partial_block, // {{#>
    open_inline, // {{#*inline
    eof,
    err,
};

pub const Token = struct {
    kind: TokenKind,
    value: []const u8,
    line: u32,
    col: u32,
};

pub const Lexer = struct {
    source: []const u8,
    pos: usize = 0,
    line: u32 = 1,
    col: u32 = 1,
    in_expression: bool = false,

    pub fn init(source: []const u8) Lexer {
        return .{ .source = source };
    }

    pub fn next(self: *Lexer) Token {
        if (self.in_expression) {
            return self.lexExpression();
        } else {
            return self.lexContent();
        }
    }

    fn lexContent(self: *Lexer) Token {
        if (self.pos >= self.source.len) {
            return self.makeToken(.eof, "");
        }

        const start = self.pos;
        const start_line = self.line;
        const start_col = self.col;

        while (self.pos < self.source.len) {
            if (self.startsWith("\\{{")) {
                // Escaped mustache - include the backslash, skip ahead
                self.advanceN(3);
                continue;
            }
            if (self.startsWith("{{")) {
                if (self.pos > start) {
                    return .{
                        .kind = .content,
                        .value = self.source[start..self.pos],
                        .line = start_line,
                        .col = start_col,
                    };
                }
                return self.lexOpenDelimiter();
            }
            self.advance();
        }

        if (self.pos > start) {
            return .{
                .kind = .content,
                .value = self.source[start..self.pos],
                .line = start_line,
                .col = start_col,
            };
        }

        return self.makeToken(.eof, "");
    }

    fn lexOpenDelimiter(self: *Lexer) Token {
        const line = self.line;
        const col = self.col;

        // {{{ → unescaped
        if (self.startsWith("{{{")) {
            self.advanceN(3);
            self.in_expression = true;
            return .{ .kind = .open_unescaped, .value = "{{{", .line = line, .col = col };
        }

        self.advanceN(2); // consume {{

        if (self.pos < self.source.len) {
            switch (self.source[self.pos]) {
                '#' => {
                    self.advance();
                    // Check for {{#> (partial block) or {{#* (inline)
                    if (self.pos < self.source.len) {
                        if (self.source[self.pos] == '>') {
                            self.advance();
                            self.in_expression = true;
                            return .{ .kind = .open_partial_block, .value = "{{#>", .line = line, .col = col };
                        }
                        if (self.source[self.pos] == '*') {
                            self.advance();
                            self.in_expression = true;
                            return .{ .kind = .open_inline, .value = "{{#*", .line = line, .col = col };
                        }
                    }
                    self.in_expression = true;
                    return .{ .kind = .open_block, .value = "{{#", .line = line, .col = col };
                },
                '/' => {
                    self.advance();
                    self.in_expression = true;
                    return .{ .kind = .open_end_block, .value = "{{/", .line = line, .col = col };
                },
                '^' => {
                    self.advance();
                    self.in_expression = true;
                    return .{ .kind = .open_inverse, .value = "{{^", .line = line, .col = col };
                },
                '>' => {
                    self.advance();
                    self.in_expression = true;
                    return .{ .kind = .open_partial, .value = "{{>", .line = line, .col = col };
                },
                '&' => {
                    self.advance();
                    self.in_expression = true;
                    return .{ .kind = .open, .value = "{{&", .line = line, .col = col };
                },
                '!' => {
                    return self.lexComment(line, col);
                },
                '~' => {
                    self.advance(); // consume ~
                    if (self.pos < self.source.len) {
                        switch (self.source[self.pos]) {
                            '#' => {
                                self.advance();
                                if (self.pos < self.source.len) {
                                    if (self.source[self.pos] == '>') {
                                        self.advance();
                                        self.in_expression = true;
                                        return .{ .kind = .open_partial_block, .value = "{{~#>", .line = line, .col = col };
                                    }
                                    if (self.source[self.pos] == '*') {
                                        self.advance();
                                        self.in_expression = true;
                                        return .{ .kind = .open_inline, .value = "{{~#*", .line = line, .col = col };
                                    }
                                }
                                self.in_expression = true;
                                return .{ .kind = .open_block, .value = "{{~#", .line = line, .col = col };
                            },
                            '/' => {
                                self.advance();
                                self.in_expression = true;
                                return .{ .kind = .open_end_block, .value = "{{~/", .line = line, .col = col };
                            },
                            '>' => {
                                self.advance();
                                self.in_expression = true;
                                return .{ .kind = .open_partial, .value = "{{~>", .line = line, .col = col };
                            },
                            '^' => {
                                self.advance();
                                self.in_expression = true;
                                return .{ .kind = .open_inverse, .value = "{{~^", .line = line, .col = col };
                            },
                            '!' => return self.lexComment(line, col),
                            else => {},
                        }
                    }
                    self.in_expression = true;
                    return .{ .kind = .open, .value = "{{~", .line = line, .col = col };
                },
                else => {},
            }
        }

        self.in_expression = true;
        return .{ .kind = .open, .value = "{{", .line = line, .col = col };
    }

    fn lexComment(self: *Lexer, line: u32, col: u32) Token {
        self.advance(); // consume '!'

        const long = self.startsWith("--");
        if (long) self.advanceN(2);

        const start = self.pos;

        if (long) {
            while (self.pos < self.source.len) {
                if (self.startsWith("--}}")) {
                    const value = self.source[start..self.pos];
                    self.advanceN(4);
                    return .{ .kind = .comment, .value = value, .line = line, .col = col };
                }
                self.advance();
            }
        } else {
            while (self.pos < self.source.len) {
                if (self.startsWith("}}")) {
                    const value = self.source[start..self.pos];
                    self.advanceN(2);
                    return .{ .kind = .comment, .value = value, .line = line, .col = col };
                }
                self.advance();
            }
        }

        return .{ .kind = .err, .value = "unterminated comment", .line = line, .col = col };
    }

    fn lexExpression(self: *Lexer) Token {
        self.skipWhitespace();

        if (self.pos >= self.source.len) {
            return self.makeToken(.err, "unexpected end of template in expression");
        }

        const line = self.line;
        const col = self.col;

        // Close delimiters
        if (self.startsWith("}}}")) {
            self.advanceN(3);
            self.in_expression = false;
            return .{ .kind = .close_unescaped, .value = "}}}", .line = line, .col = col };
        }
        if (self.startsWith("~}}")) {
            self.advanceN(3);
            self.in_expression = false;
            return .{ .kind = .close, .value = "~}}", .line = line, .col = col };
        }
        if (self.startsWith("}}")) {
            self.advanceN(2);
            self.in_expression = false;
            return .{ .kind = .close, .value = "}}", .line = line, .col = col };
        }

        const ch = self.source[self.pos];

        return switch (ch) {
            '(' => blk: {
                self.advance();
                break :blk .{ .kind = .open_sexpr, .value = "(", .line = line, .col = col };
            },
            ')' => blk: {
                self.advance();
                break :blk .{ .kind = .close_sexpr, .value = ")", .line = line, .col = col };
            },
            '=' => blk: {
                self.advance();
                break :blk .{ .kind = .equals, .value = "=", .line = line, .col = col };
            },
            '@' => blk: {
                self.advance();
                break :blk .{ .kind = .data, .value = "@", .line = line, .col = col };
            },
            '|' => blk: {
                self.advance();
                break :blk .{ .kind = .close_block_params, .value = "|", .line = line, .col = col };
            },
            '.' => self.lexDot(line, col),
            '/' => blk: {
                self.advance();
                break :blk .{ .kind = .sep, .value = "/", .line = line, .col = col };
            },
            '[' => self.lexBracketLiteral(),
            '"', '\'' => self.lexString(),
            else => blk: {
                if (std.ascii.isDigit(ch) or
                    (ch == '-' and self.pos + 1 < self.source.len and std.ascii.isDigit(self.source[self.pos + 1])))
                {
                    break :blk self.lexNumber();
                }
                if (isIdStart(ch)) {
                    break :blk self.lexIdentifier();
                }
                self.advance();
                break :blk .{ .kind = .err, .value = self.source[self.pos - 1 .. self.pos], .line = line, .col = col };
            },
        };
    }

    fn lexDot(self: *Lexer, line: u32, col: u32) Token {
        if (self.pos + 1 < self.source.len and self.source[self.pos + 1] == '.') {
            // ".." parent reference
            self.advanceN(2);
            return .{ .kind = .id, .value = "..", .line = line, .col = col };
        }
        if (self.pos + 1 < self.source.len and
            (isIdStart(self.source[self.pos + 1]) or self.source[self.pos + 1] == '.' or
            std.ascii.isDigit(self.source[self.pos + 1]) or self.source[self.pos + 1] == '['))
        {
            // Separator before identifier or bracket literal
            self.advance();
            return .{ .kind = .sep, .value = ".", .line = line, .col = col };
        }
        // Standalone "." means "this"
        self.advance();
        return .{ .kind = .id, .value = ".", .line = line, .col = col };
    }

    fn lexBracketLiteral(self: *Lexer) Token {
        const line = self.line;
        const col = self.col;
        self.advance(); // consume '['
        const start = self.pos;

        while (self.pos < self.source.len) {
            if (self.source[self.pos] == ']') {
                const value = self.source[start..self.pos];
                self.advance(); // consume ']'
                return .{ .kind = .id, .value = value, .line = line, .col = col };
            }
            self.advance();
        }

        return .{ .kind = .err, .value = "unterminated bracket literal", .line = line, .col = col };
    }

    fn lexString(self: *Lexer) Token {
        const line = self.line;
        const col = self.col;
        const quote = self.source[self.pos];
        self.advance(); // opening quote
        const start = self.pos;

        while (self.pos < self.source.len) {
            const c = self.source[self.pos];
            if (c == '\\' and self.pos + 1 < self.source.len) {
                self.advanceN(2);
                continue;
            }
            if (c == quote) {
                const value = self.source[start..self.pos];
                self.advance(); // closing quote
                return .{ .kind = .string, .value = value, .line = line, .col = col };
            }
            self.advance();
        }

        return .{ .kind = .err, .value = "unterminated string", .line = line, .col = col };
    }

    fn lexNumber(self: *Lexer) Token {
        const line = self.line;
        const col = self.col;
        const start = self.pos;

        if (self.source[self.pos] == '-') self.advance();

        while (self.pos < self.source.len and std.ascii.isDigit(self.source[self.pos])) {
            self.advance();
        }

        if (self.pos < self.source.len and self.source[self.pos] == '.') {
            self.advance();
            while (self.pos < self.source.len and std.ascii.isDigit(self.source[self.pos])) {
                self.advance();
            }
        }

        return .{ .kind = .number, .value = self.source[start..self.pos], .line = line, .col = col };
    }

    fn lexIdentifier(self: *Lexer) Token {
        const line = self.line;
        const col = self.col;
        const start = self.pos;

        while (self.pos < self.source.len and isIdPart(self.source[self.pos])) {
            self.advance();
        }

        const value = self.source[start..self.pos];

        // Keywords
        if (std.mem.eql(u8, value, "true") or std.mem.eql(u8, value, "false")) {
            return .{ .kind = .boolean, .value = value, .line = line, .col = col };
        }

        // "as |" block params
        if (std.mem.eql(u8, value, "as")) {
            const saved_pos = self.pos;
            const saved_line = self.line;
            const saved_col = self.col;
            self.skipWhitespace();
            if (self.pos < self.source.len and self.source[self.pos] == '|') {
                self.advance();
                return .{ .kind = .open_block_params, .value = "as |", .line = line, .col = col };
            }
            self.pos = saved_pos;
            self.line = saved_line;
            self.col = saved_col;
        }

        return .{ .kind = .id, .value = value, .line = line, .col = col };
    }

    // --- Utilities ---

    fn advance(self: *Lexer) void {
        if (self.pos < self.source.len) {
            if (self.source[self.pos] == '\n') {
                self.line += 1;
                self.col = 1;
            } else {
                self.col += 1;
            }
            self.pos += 1;
        }
    }

    fn advanceN(self: *Lexer, n: usize) void {
        for (0..n) |_| self.advance();
    }

    fn startsWith(self: *const Lexer, prefix: []const u8) bool {
        if (self.pos + prefix.len > self.source.len) return false;
        return std.mem.eql(u8, self.source[self.pos .. self.pos + prefix.len], prefix);
    }

    fn skipWhitespace(self: *Lexer) void {
        while (self.pos < self.source.len and std.ascii.isWhitespace(self.source[self.pos])) {
            self.advance();
        }
    }

    fn makeToken(self: *const Lexer, kind: TokenKind, value: []const u8) Token {
        return .{ .kind = kind, .value = value, .line = self.line, .col = self.col };
    }
};

fn isIdStart(ch: u8) bool {
    return std.ascii.isAlphabetic(ch) or ch == '_' or ch == '$';
}

fn isIdPart(ch: u8) bool {
    return std.ascii.isAlphanumeric(ch) or ch == '_' or ch == '-' or ch == '$';
}

// -----------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------

test "lex simple variable" {
    var lex = Lexer.init("Hello, {{name}}!");

    const t1 = lex.next();
    try std.testing.expectEqual(.content, t1.kind);
    try std.testing.expectEqualStrings("Hello, ", t1.value);

    const t2 = lex.next();
    try std.testing.expectEqual(.open, t2.kind);

    const t3 = lex.next();
    try std.testing.expectEqual(.id, t3.kind);
    try std.testing.expectEqualStrings("name", t3.value);

    const t4 = lex.next();
    try std.testing.expectEqual(.close, t4.kind);

    const t5 = lex.next();
    try std.testing.expectEqual(.content, t5.kind);
    try std.testing.expectEqualStrings("!", t5.value);

    const t6 = lex.next();
    try std.testing.expectEqual(.eof, t6.kind);
}

test "lex unescaped" {
    var lex = Lexer.init("{{{html}}}");

    try std.testing.expectEqual(.open_unescaped, lex.next().kind);
    const id_tok = lex.next();
    try std.testing.expectEqual(.id, id_tok.kind);
    try std.testing.expectEqualStrings("html", id_tok.value);
    try std.testing.expectEqual(.close_unescaped, lex.next().kind);
    try std.testing.expectEqual(.eof, lex.next().kind);
}

test "lex block" {
    var lex = Lexer.init("{{#if x}}yes{{else}}no{{/if}}");

    try std.testing.expectEqual(.open_block, lex.next().kind);
    try std.testing.expectEqual(.id, lex.next().kind); // "if"
    try std.testing.expectEqual(.id, lex.next().kind); // "x"
    try std.testing.expectEqual(.close, lex.next().kind);
    try std.testing.expectEqual(.content, lex.next().kind); // "yes"
    try std.testing.expectEqual(.open, lex.next().kind); // {{
    const else_tok = lex.next();
    try std.testing.expectEqual(.id, else_tok.kind); // "else"
    try std.testing.expectEqualStrings("else", else_tok.value);
    try std.testing.expectEqual(.close, lex.next().kind);
    try std.testing.expectEqual(.content, lex.next().kind); // "no"
    try std.testing.expectEqual(.open_end_block, lex.next().kind);
    try std.testing.expectEqual(.id, lex.next().kind); // "if"
    try std.testing.expectEqual(.close, lex.next().kind);
    try std.testing.expectEqual(.eof, lex.next().kind);
}

test "lex dotted path" {
    var lex = Lexer.init("{{user.name}}");

    try std.testing.expectEqual(.open, lex.next().kind);
    const id1 = lex.next();
    try std.testing.expectEqual(.id, id1.kind);
    try std.testing.expectEqualStrings("user", id1.value);
    try std.testing.expectEqual(.sep, lex.next().kind);
    const id2 = lex.next();
    try std.testing.expectEqual(.id, id2.kind);
    try std.testing.expectEqualStrings("name", id2.value);
    try std.testing.expectEqual(.close, lex.next().kind);
}

test "lex data reference" {
    var lex = Lexer.init("{{@index}}");

    try std.testing.expectEqual(.open, lex.next().kind);
    try std.testing.expectEqual(.data, lex.next().kind);
    const id = lex.next();
    try std.testing.expectEqual(.id, id.kind);
    try std.testing.expectEqualStrings("index", id.value);
    try std.testing.expectEqual(.close, lex.next().kind);
}

test "lex sub-expression" {
    var lex = Lexer.init("{{#if (eq a b)}}");

    try std.testing.expectEqual(.open_block, lex.next().kind);
    try std.testing.expectEqual(.id, lex.next().kind); // "if"
    try std.testing.expectEqual(.open_sexpr, lex.next().kind);
    try std.testing.expectEqual(.id, lex.next().kind); // "eq"
    try std.testing.expectEqual(.id, lex.next().kind); // "a"
    try std.testing.expectEqual(.id, lex.next().kind); // "b"
    try std.testing.expectEqual(.close_sexpr, lex.next().kind);
    try std.testing.expectEqual(.close, lex.next().kind);
}

test "lex hash params" {
    var lex = Lexer.init("{{helper key=\"value\" num=42}}");

    try std.testing.expectEqual(.open, lex.next().kind);
    try std.testing.expectEqual(.id, lex.next().kind); // "helper"
    const key = lex.next();
    try std.testing.expectEqual(.id, key.kind);
    try std.testing.expectEqualStrings("key", key.value);
    try std.testing.expectEqual(.equals, lex.next().kind);
    const str_tok = lex.next();
    try std.testing.expectEqual(.string, str_tok.kind);
    try std.testing.expectEqualStrings("value", str_tok.value);
    try std.testing.expectEqual(.id, lex.next().kind); // "num"
    try std.testing.expectEqual(.equals, lex.next().kind);
    const num = lex.next();
    try std.testing.expectEqual(.number, num.kind);
    try std.testing.expectEqualStrings("42", num.value);
    try std.testing.expectEqual(.close, lex.next().kind);
}

test "lex comment" {
    var lex = Lexer.init("{{! short comment }}text");

    const c = lex.next();
    try std.testing.expectEqual(.comment, c.kind);
    try std.testing.expectEqualStrings(" short comment ", c.value);

    const t = lex.next();
    try std.testing.expectEqual(.content, t.kind);
    try std.testing.expectEqualStrings("text", t.value);
}

test "lex long comment" {
    var lex = Lexer.init("{{!-- long {{comment}} --}}after");

    const c = lex.next();
    try std.testing.expectEqual(.comment, c.kind);
    try std.testing.expectEqualStrings(" long {{comment}} ", c.value);

    const t = lex.next();
    try std.testing.expectEqual(.content, t.kind);
    try std.testing.expectEqualStrings("after", t.value);
}

test "lex parent path" {
    var lex = Lexer.init("{{../name}}");

    try std.testing.expectEqual(.open, lex.next().kind);
    const parent = lex.next();
    try std.testing.expectEqual(.id, parent.kind);
    try std.testing.expectEqualStrings("..", parent.value);
    try std.testing.expectEqual(.sep, lex.next().kind);
    const name = lex.next();
    try std.testing.expectEqual(.id, name.kind);
    try std.testing.expectEqualStrings("name", name.value);
    try std.testing.expectEqual(.close, lex.next().kind);
}

test "lex boolean" {
    var lex = Lexer.init("{{#if true}}");

    try std.testing.expectEqual(.open_block, lex.next().kind);
    try std.testing.expectEqual(.id, lex.next().kind); // "if"
    const b = lex.next();
    try std.testing.expectEqual(.boolean, b.kind);
    try std.testing.expectEqualStrings("true", b.value);
    try std.testing.expectEqual(.close, lex.next().kind);
}

test "lex partial" {
    var lex = Lexer.init("{{> myPartial}}");

    try std.testing.expectEqual(.open_partial, lex.next().kind);
    const name = lex.next();
    try std.testing.expectEqual(.id, name.kind);
    try std.testing.expectEqualStrings("myPartial", name.value);
    try std.testing.expectEqual(.close, lex.next().kind);
}

test "lex ampersand unescaped" {
    var lex = Lexer.init("{{&html}}");

    const open = lex.next();
    try std.testing.expectEqual(.open, open.kind);
    try std.testing.expectEqualStrings("{{&", open.value);
    const id_tok = lex.next();
    try std.testing.expectEqual(.id, id_tok.kind);
    try std.testing.expectEqualStrings("html", id_tok.value);
    try std.testing.expectEqual(.close, lex.next().kind);
}

test "lex inverse block" {
    var lex = Lexer.init("{{^}}fallback{{/if}}");

    try std.testing.expectEqual(.open_inverse, lex.next().kind);
    try std.testing.expectEqual(.close, lex.next().kind);
    const fb = lex.next();
    try std.testing.expectEqual(.content, fb.kind);
    try std.testing.expectEqualStrings("fallback", fb.value);
    try std.testing.expectEqual(.open_end_block, lex.next().kind);
    try std.testing.expectEqual(.id, lex.next().kind);
    try std.testing.expectEqual(.close, lex.next().kind);
}
