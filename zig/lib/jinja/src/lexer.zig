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

// Jinja2 lexer.
//
// Tokenizes Jinja2 template syntax: {{ expr }}, {% stmt %}, {# comment #}.
// Handles whitespace control ({%- -%}, {{- -}}).

const std = @import("std");

pub const TokenKind = enum {
    // Content
    text, // literal text between tags

    // Tag delimiters
    expr_open, // {{ or {{-
    expr_close, // }} or -}}
    stmt_open, // {% or {%-
    stmt_close, // %} or -%}

    // Values
    ident, // identifier
    string, // 'str' or "str"
    integer, // 123
    float_lit, // 1.5

    // Operators
    plus, // +
    minus, // -
    star, // *
    slash, // /
    percent, // %
    tilde, // ~
    eq, // ==
    ne, // !=
    lt, // <
    gt, // >
    le, // <=
    ge, // >=
    assign, // =
    pipe, // |
    dot, // .
    comma, // ,
    colon, // :
    lparen, // (
    rparen, // )
    lbracket, // [
    rbracket, // ]

    eof,
    err,
};

pub const Token = struct {
    kind: TokenKind,
    value: []const u8,
    /// true if the tag had whitespace stripping (- prefix/suffix)
    strip: bool = false,
};

pub const Lexer = struct {
    source: []const u8,
    pos: usize = 0,
    in_tag: bool = false,

    pub fn init(source: []const u8) Lexer {
        return .{ .source = source };
    }

    pub fn next(self: *Lexer) Token {
        if (self.in_tag) {
            return self.lexInTag();
        } else {
            return self.lexContent();
        }
    }

    /// Peek at the next token without consuming it.
    pub fn peek(self: *Lexer) Token {
        const saved_pos = self.pos;
        const saved_in_tag = self.in_tag;
        const tok = self.next();
        self.pos = saved_pos;
        self.in_tag = saved_in_tag;
        return tok;
    }

    fn lexContent(self: *Lexer) Token {
        if (self.pos >= self.source.len) {
            return .{ .kind = .eof, .value = "" };
        }

        const start = self.pos;

        while (self.pos < self.source.len) {
            // Check for tag openings
            if (self.pos + 1 < self.source.len) {
                const ch = self.source[self.pos];
                const ch2 = self.source[self.pos + 1];

                if (ch == '{' and (ch2 == '{' or ch2 == '%' or ch2 == '#')) {
                    // Emit any accumulated text first
                    if (self.pos > start) {
                        return .{ .kind = .text, .value = self.source[start..self.pos] };
                    }

                    // Comment: {# ... #}
                    if (ch2 == '#') {
                        return self.lexComment();
                    }

                    // Expression or statement tag
                    return self.lexTagOpen();
                }
            }
            self.pos += 1;
        }

        if (self.pos > start) {
            return .{ .kind = .text, .value = self.source[start..self.pos] };
        }
        return .{ .kind = .eof, .value = "" };
    }

    fn lexComment(self: *Lexer) Token {
        const start = self.pos;
        self.pos += 2; // skip {#

        while (self.pos + 1 < self.source.len) {
            if (self.source[self.pos] == '#' and self.source[self.pos + 1] == '}') {
                self.pos += 2; // skip #}
                // Comments are discarded — return next token
                return self.lexContent();
            }
            self.pos += 1;
        }
        // Unterminated comment — treat rest as text
        return .{ .kind = .text, .value = self.source[start..] };
    }

    fn lexTagOpen(self: *Lexer) Token {
        const ch2 = self.source[self.pos + 1];
        self.pos += 2; // skip {{ or {%

        // Check for whitespace stripping: {{- or {%-
        var strip = false;
        if (self.pos < self.source.len and self.source[self.pos] == '-') {
            strip = true;
            self.pos += 1;
        }

        self.in_tag = true;

        if (ch2 == '{') {
            return .{ .kind = .expr_open, .value = if (strip) "{{-" else "{{", .strip = strip };
        } else {
            return .{ .kind = .stmt_open, .value = if (strip) "{%-" else "{%", .strip = strip };
        }
    }

    fn lexInTag(self: *Lexer) Token {
        self.skipWhitespace();

        if (self.pos >= self.source.len) {
            return .{ .kind = .err, .value = "unexpected end of template inside tag" };
        }

        // Check for close delimiters
        // -}} or -%}
        if (self.source[self.pos] == '-' and self.pos + 2 < self.source.len) {
            if (self.source[self.pos + 1] == '}' and self.source[self.pos + 2] == '}') {
                self.pos += 3;
                self.in_tag = false;
                return .{ .kind = .expr_close, .value = "-}}", .strip = true };
            }
            if (self.source[self.pos + 1] == '%' and self.source[self.pos + 2] == '}') {
                self.pos += 3;
                self.in_tag = false;
                return .{ .kind = .stmt_close, .value = "-%}", .strip = true };
            }
        }
        // }} or %}
        if (self.pos + 1 < self.source.len) {
            if (self.source[self.pos] == '}' and self.source[self.pos + 1] == '}') {
                self.pos += 2;
                self.in_tag = false;
                return .{ .kind = .expr_close, .value = "}}" };
            }
            if (self.source[self.pos] == '%' and self.source[self.pos + 1] == '}') {
                self.pos += 2;
                self.in_tag = false;
                return .{ .kind = .stmt_close, .value = "%}" };
            }
        }

        const ch = self.source[self.pos];

        // Two-character operators
        if (self.pos + 1 < self.source.len) {
            const ch2 = self.source[self.pos + 1];
            const pair = [2]u8{ ch, ch2 };
            if (std.mem.eql(u8, &pair, "==")) {
                self.pos += 2;
                return .{ .kind = .eq, .value = "==" };
            }
            if (std.mem.eql(u8, &pair, "!=")) {
                self.pos += 2;
                return .{ .kind = .ne, .value = "!=" };
            }
            if (std.mem.eql(u8, &pair, "<=")) {
                self.pos += 2;
                return .{ .kind = .le, .value = "<=" };
            }
            if (std.mem.eql(u8, &pair, ">=")) {
                self.pos += 2;
                return .{ .kind = .ge, .value = ">=" };
            }
        }

        // Single-character tokens
        switch (ch) {
            '+' => {
                self.pos += 1;
                return .{ .kind = .plus, .value = "+" };
            },
            '*' => {
                self.pos += 1;
                return .{ .kind = .star, .value = "*" };
            },
            '/' => {
                self.pos += 1;
                return .{ .kind = .slash, .value = "/" };
            },
            '%' => {
                self.pos += 1;
                return .{ .kind = .percent, .value = "%" };
            },
            '~' => {
                self.pos += 1;
                return .{ .kind = .tilde, .value = "~" };
            },
            '<' => {
                self.pos += 1;
                return .{ .kind = .lt, .value = "<" };
            },
            '>' => {
                self.pos += 1;
                return .{ .kind = .gt, .value = ">" };
            },
            '=' => {
                self.pos += 1;
                return .{ .kind = .assign, .value = "=" };
            },
            '|' => {
                self.pos += 1;
                return .{ .kind = .pipe, .value = "|" };
            },
            '.' => {
                self.pos += 1;
                return .{ .kind = .dot, .value = "." };
            },
            ',' => {
                self.pos += 1;
                return .{ .kind = .comma, .value = "," };
            },
            ':' => {
                self.pos += 1;
                return .{ .kind = .colon, .value = ":" };
            },
            '(' => {
                self.pos += 1;
                return .{ .kind = .lparen, .value = "(" };
            },
            ')' => {
                self.pos += 1;
                return .{ .kind = .rparen, .value = ")" };
            },
            '[' => {
                self.pos += 1;
                return .{ .kind = .lbracket, .value = "[" };
            },
            ']' => {
                self.pos += 1;
                return .{ .kind = .rbracket, .value = "]" };
            },
            '-' => {
                // Could be unary minus or subtraction
                self.pos += 1;
                return .{ .kind = .minus, .value = "-" };
            },
            '\'', '"' => return self.lexString(),
            else => {
                if (std.ascii.isDigit(ch)) {
                    return self.lexNumber();
                }
                if (isIdentStart(ch)) {
                    return self.lexIdent();
                }
                self.pos += 1;
                return .{ .kind = .err, .value = self.source[self.pos - 1 .. self.pos] };
            },
        }
    }

    fn lexString(self: *Lexer) Token {
        const quote = self.source[self.pos];
        self.pos += 1; // skip opening quote
        const start = self.pos;

        while (self.pos < self.source.len) {
            const c = self.source[self.pos];
            if (c == '\\' and self.pos + 1 < self.source.len) {
                self.pos += 2; // skip escape sequence
                continue;
            }
            if (c == quote) {
                const value = self.source[start..self.pos];
                self.pos += 1; // skip closing quote
                return .{ .kind = .string, .value = value };
            }
            self.pos += 1;
        }
        return .{ .kind = .err, .value = "unterminated string" };
    }

    fn lexNumber(self: *Lexer) Token {
        const start = self.pos;
        while (self.pos < self.source.len and std.ascii.isDigit(self.source[self.pos])) {
            self.pos += 1;
        }
        if (self.pos < self.source.len and self.source[self.pos] == '.') {
            self.pos += 1;
            while (self.pos < self.source.len and std.ascii.isDigit(self.source[self.pos])) {
                self.pos += 1;
            }
            return .{ .kind = .float_lit, .value = self.source[start..self.pos] };
        }
        return .{ .kind = .integer, .value = self.source[start..self.pos] };
    }

    fn lexIdent(self: *Lexer) Token {
        const start = self.pos;
        while (self.pos < self.source.len and isIdentPart(self.source[self.pos])) {
            self.pos += 1;
        }
        const value = self.source[start..self.pos];

        // Keywords map to ident token — parser handles keyword matching
        return .{ .kind = .ident, .value = value };
    }

    fn skipWhitespace(self: *Lexer) void {
        while (self.pos < self.source.len and std.ascii.isWhitespace(self.source[self.pos])) {
            self.pos += 1;
        }
    }
};

fn isIdentStart(ch: u8) bool {
    return std.ascii.isAlphabetic(ch) or ch == '_';
}

fn isIdentPart(ch: u8) bool {
    return std.ascii.isAlphanumeric(ch) or ch == '_';
}

// --- Tests ---

test "lex simple expression" {
    var lex = Lexer.init("Hello {{ name }}!");

    const t1 = lex.next();
    try std.testing.expectEqual(.text, t1.kind);
    try std.testing.expectEqualStrings("Hello ", t1.value);

    const t2 = lex.next();
    try std.testing.expectEqual(.expr_open, t2.kind);

    const t3 = lex.next();
    try std.testing.expectEqual(.ident, t3.kind);
    try std.testing.expectEqualStrings("name", t3.value);

    const t4 = lex.next();
    try std.testing.expectEqual(.expr_close, t4.kind);

    const t5 = lex.next();
    try std.testing.expectEqual(.text, t5.kind);
    try std.testing.expectEqualStrings("!", t5.value);

    try std.testing.expectEqual(.eof, lex.next().kind);
}

test "lex statement" {
    var lex = Lexer.init("{% if x %}yes{% endif %}");

    try std.testing.expectEqual(.stmt_open, lex.next().kind);
    const if_tok = lex.next();
    try std.testing.expectEqual(.ident, if_tok.kind);
    try std.testing.expectEqualStrings("if", if_tok.value);
    try std.testing.expectEqual(.ident, lex.next().kind); // x
    try std.testing.expectEqual(.stmt_close, lex.next().kind);

    const text = lex.next();
    try std.testing.expectEqual(.text, text.kind);
    try std.testing.expectEqualStrings("yes", text.value);

    try std.testing.expectEqual(.stmt_open, lex.next().kind);
    const endif_tok = lex.next();
    try std.testing.expectEqual(.ident, endif_tok.kind);
    try std.testing.expectEqualStrings("endif", endif_tok.value);
    try std.testing.expectEqual(.stmt_close, lex.next().kind);
    try std.testing.expectEqual(.eof, lex.next().kind);
}

test "lex whitespace control" {
    var lex = Lexer.init("{%- if x -%}");

    const open = lex.next();
    try std.testing.expectEqual(.stmt_open, open.kind);
    try std.testing.expect(open.strip);

    _ = lex.next(); // if
    _ = lex.next(); // x

    const close = lex.next();
    try std.testing.expectEqual(.stmt_close, close.kind);
    try std.testing.expect(close.strip);
}

test "lex operators" {
    var lex = Lexer.init("{{ a == b != c }}");

    try std.testing.expectEqual(.expr_open, lex.next().kind);
    try std.testing.expectEqual(.ident, lex.next().kind);
    try std.testing.expectEqual(.eq, lex.next().kind);
    try std.testing.expectEqual(.ident, lex.next().kind);
    try std.testing.expectEqual(.ne, lex.next().kind);
    try std.testing.expectEqual(.ident, lex.next().kind);
    try std.testing.expectEqual(.expr_close, lex.next().kind);
}

test "lex string with escapes" {
    var lex = Lexer.init("{{ 'hello\\nworld' }}");

    try std.testing.expectEqual(.expr_open, lex.next().kind);
    const str = lex.next();
    try std.testing.expectEqual(.string, str.kind);
    try std.testing.expectEqualStrings("hello\\nworld", str.value);
    try std.testing.expectEqual(.expr_close, lex.next().kind);
}

test "lex subscript" {
    var lex = Lexer.init("{{ messages[0] }}");

    try std.testing.expectEqual(.expr_open, lex.next().kind);
    try std.testing.expectEqual(.ident, lex.next().kind);
    try std.testing.expectEqual(.lbracket, lex.next().kind);
    const num = lex.next();
    try std.testing.expectEqual(.integer, num.kind);
    try std.testing.expectEqualStrings("0", num.value);
    try std.testing.expectEqual(.rbracket, lex.next().kind);
    try std.testing.expectEqual(.expr_close, lex.next().kind);
}

test "lex dot access" {
    var lex = Lexer.init("{{ loop.first }}");

    try std.testing.expectEqual(.expr_open, lex.next().kind);
    try std.testing.expectEqual(.ident, lex.next().kind);
    try std.testing.expectEqual(.dot, lex.next().kind);
    try std.testing.expectEqual(.ident, lex.next().kind);
    try std.testing.expectEqual(.expr_close, lex.next().kind);
}

test "lex comment" {
    var lex = Lexer.init("before{# comment #}after");

    const t1 = lex.next();
    try std.testing.expectEqual(.text, t1.kind);
    try std.testing.expectEqualStrings("before", t1.value);

    const t2 = lex.next();
    try std.testing.expectEqual(.text, t2.kind);
    try std.testing.expectEqualStrings("after", t2.value);
}

test "lex filter pipe" {
    var lex = Lexer.init("{{ name | trim }}");

    try std.testing.expectEqual(.expr_open, lex.next().kind);
    try std.testing.expectEqual(.ident, lex.next().kind);
    try std.testing.expectEqual(.pipe, lex.next().kind);
    try std.testing.expectEqual(.ident, lex.next().kind);
    try std.testing.expectEqual(.expr_close, lex.next().kind);
}
