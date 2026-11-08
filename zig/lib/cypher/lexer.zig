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
const Span = @import("ast.zig").Span;
pub const Kind = enum { word, quoted_name, string, integer, number, parameter, lparen, rparen, lbracket, rbracket, lbrace, rbrace, comma, colon, dot, range, plus, minus, star, slash, percent, caret, eq, neq, lt, lte, gt, gte, regex, pipe, semicolon, eof };
pub const Token = struct { kind: Kind, text: []const u8, span: Span };
pub const Diagnostic = struct { span: Span = .{}, message: []const u8 = "" };
pub const Error = std.mem.Allocator.Error || error{ InvalidCypherSyntax, CypherLimitExceeded };

pub fn scan(a: std.mem.Allocator, source: []const u8, max_tokens: usize, diagnostic: *Diagnostic) Error![]const Token {
    var tokens: std.ArrayList(Token) = .empty;
    var i: usize = 0;
    while (i < source.len) {
        const start = i;
        const c = source[i];
        if (std.ascii.isWhitespace(c)) {
            i += 1;
            continue;
        }
        if (c == '/' and i + 1 < source.len and source[i + 1] == '/') {
            while (i < source.len and source[i] != '\n') i += 1;
            continue;
        }
        if (c == '/' and i + 1 < source.len and source[i + 1] == '*') {
            i += 2;
            var depth: usize = 1;
            while (i + 1 < source.len and depth > 0) {
                if (source[i] == '/' and source[i + 1] == '*') {
                    depth += 1;
                    i += 2;
                } else if (source[i] == '*' and source[i + 1] == '/') {
                    depth -= 1;
                    i += 2;
                } else i += 1;
            }
            if (depth != 0) return fail(diagnostic, start, source.len, "unterminated comment");
            continue;
        }
        var kind: Kind = undefined;
        var value: []const u8 = undefined;
        if (c == '\'' or c == '"' or c == '`' or (c == '$' and i + 1 < source.len and source[i + 1] == '`')) {
            const quote = if (c == '$') source[i + 1] else c;
            kind = if (c == '$') .parameter else if (quote == '`') .quoted_name else .string;
            i += if (c == '$') @as(usize, 2) else 1;
            var decoded: std.ArrayList(u8) = .empty;
            var closed = false;
            while (i < source.len) {
                if (source[i] == quote) {
                    i += 1;
                    if (i < source.len and source[i] == quote) {
                        try decoded.append(a, quote);
                        i += 1;
                        continue;
                    }
                    closed = true;
                    break;
                }
                if (source[i] == '\\' and quote != '`') {
                    i += 1;
                    if (i == source.len) break;
                    if (source[i] == 'u' or source[i] == 'U') {
                        const escape_start = i - 1;
                        const digits: usize = if (source[i] == 'u') 4 else 8;
                        i += 1;
                        if (source.len - i < digits) return fail(diagnostic, escape_start, source.len, "incomplete Unicode escape");
                        var codepoint = std.fmt.parseUnsigned(u32, source[i..][0..digits], 16) catch return fail(diagnostic, escape_start, i + digits, "invalid Unicode escape");
                        i += digits;
                        if (codepoint >= 0xd800 and codepoint <= 0xdbff) {
                            if (source.len - i < 6 or source[i] != '\\' or source[i + 1] != 'u') return fail(diagnostic, escape_start, i, "unpaired Unicode surrogate");
                            const low = std.fmt.parseUnsigned(u32, source[i + 2 ..][0..4], 16) catch return fail(diagnostic, escape_start, i + 6, "invalid Unicode escape");
                            if (low < 0xdc00 or low > 0xdfff) return fail(diagnostic, escape_start, i + 6, "unpaired Unicode surrogate");
                            codepoint = 0x10000 + ((codepoint - 0xd800) << 10) + (low - 0xdc00);
                            i += 6;
                        }
                        if (codepoint > 0x10ffff or (codepoint >= 0xdc00 and codepoint <= 0xdfff)) return fail(diagnostic, escape_start, i, "invalid Unicode code point");
                        var encoded: [4]u8 = undefined;
                        const length = std.unicode.utf8Encode(@intCast(codepoint), &encoded) catch return fail(diagnostic, escape_start, i, "invalid Unicode code point");
                        try decoded.appendSlice(a, encoded[0..length]);
                        continue;
                    }
                    const escaped: u8 = switch (source[i]) {
                        'n' => '\n',
                        'r' => '\r',
                        't' => '\t',
                        '\\' => '\\',
                        '\'' => '\'',
                        '"' => '"',
                        else => return fail(diagnostic, i - 1, i + 1, "unsupported string escape"),
                    };
                    try decoded.append(a, escaped);
                    i += 1;
                } else {
                    try decoded.append(a, source[i]);
                    i += 1;
                }
            }
            if (!closed) return fail(diagnostic, start, i, "unterminated quoted value");
            value = try decoded.toOwnedSlice(a);
            if (!std.unicode.utf8ValidateSlice(value)) return fail(diagnostic, start, i, "invalid UTF-8");
        } else if (std.ascii.isAlphabetic(c) or c == '_' or c >= 128 or c == '$') {
            kind = if (c == '$') .parameter else .word;
            i += 1;
            if (c == '$' and (i == source.len or !(std.ascii.isAlphanumeric(source[i]) or source[i] == '_' or source[i] >= 128))) return fail(diagnostic, start, i, "expected parameter after $");
            while (i < source.len and (std.ascii.isAlphanumeric(source[i]) or source[i] == '_' or source[i] >= 128)) i += 1;
            value = try a.dupe(u8, source[start + @as(usize, if (c == '$') 1 else 0) .. i]);
            if (!std.unicode.utf8ValidateSlice(value)) return fail(diagnostic, start, i, "invalid UTF-8 identifier");
        } else if (std.ascii.isDigit(c)) {
            kind = .integer;
            i += 1;
            while (i < source.len and std.ascii.isDigit(source[i])) i += 1;
            if (i + 1 < source.len and source[i] == '.' and std.ascii.isDigit(source[i + 1])) {
                kind = .number;
                i += 1;
                while (i < source.len and std.ascii.isDigit(source[i])) i += 1;
            }
            if (i < source.len and (source[i] == 'e' or source[i] == 'E')) {
                kind = .number;
                i += 1;
                if (i < source.len and (source[i] == '+' or source[i] == '-')) i += 1;
                const exponent = i;
                while (i < source.len and std.ascii.isDigit(source[i])) i += 1;
                if (i == exponent) return fail(diagnostic, start, i, "expected exponent digits");
            }
            value = try a.dupe(u8, source[start..i]);
        } else {
            i += 1;
            kind = switch (c) {
                '(' => .lparen,
                ')' => .rparen,
                '[' => .lbracket,
                ']' => .rbracket,
                '{' => .lbrace,
                '}' => .rbrace,
                ',' => .comma,
                ':' => .colon,
                '.' => if (i < source.len and source[i] == '.') blk: {
                    i += 1;
                    break :blk .range;
                } else .dot,
                '+' => .plus,
                '-' => .minus,
                '*' => .star,
                '/' => .slash,
                '%' => .percent,
                '^' => .caret,
                '|' => .pipe,
                ';' => .semicolon,
                '=' => if (i < source.len and source[i] == '~') blk: {
                    i += 1;
                    break :blk .regex;
                } else .eq,
                '<' => if (i < source.len and source[i] == '=') blk: {
                    i += 1;
                    break :blk .lte;
                } else if (i < source.len and source[i] == '>') blk: {
                    i += 1;
                    break :blk .neq;
                } else .lt,
                '>' => if (i < source.len and source[i] == '=') blk: {
                    i += 1;
                    break :blk .gte;
                } else .gt,
                '!' => if (i < source.len and source[i] == '=') blk: {
                    i += 1;
                    break :blk .neq;
                } else return fail(diagnostic, start, i, "expected !="),
                else => return fail(diagnostic, start, i, "invalid Cypher character"),
            };
            value = source[start..i];
        }
        if (tokens.items.len >= max_tokens) {
            diagnostic.* = .{ .span = .{ .start = start, .end = i }, .message = "Cypher token budget exceeded" };
            return error.CypherLimitExceeded;
        }
        try tokens.append(a, .{ .kind = kind, .text = value, .span = .{ .start = start, .end = i } });
    }
    try tokens.append(a, .{ .kind = .eof, .text = "", .span = .{ .start = i, .end = i } });
    return tokens.toOwnedSlice(a);
}
fn fail(diagnostic: *Diagnostic, start: usize, end: usize, message: []const u8) Error {
    diagnostic.* = .{ .span = .{ .start = start, .end = end }, .message = message };
    return error.InvalidCypherSyntax;
}
