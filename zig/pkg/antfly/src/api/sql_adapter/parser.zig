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

pub fn expectKeyword(tokens: []const Token, pos: *usize, keyword: []const u8) !void {
    if (!matchKeyword(tokens, pos, keyword)) return error.UnsupportedSqlShape;
}

pub fn expectToken(tokens: []const Token, pos: *usize, kind: TokenKind) !void {
    if (matchToken(tokens, pos, kind) == null) return error.UnsupportedSqlShape;
}

pub fn matchKeyword(tokens: []const Token, pos: *usize, keyword: []const u8) bool {
    if (pos.* >= tokens.len) return false;
    const token = tokens[pos.*];
    if (token.kind != .identifier) return false;
    if (!std.ascii.eqlIgnoreCase(token.text, keyword)) return false;
    pos.* += 1;
    return true;
}

pub fn matchToken(tokens: []const Token, pos: *usize, kind: TokenKind) ?Token {
    if (pos.* >= tokens.len) return null;
    const token = tokens[pos.*];
    if (token.kind != kind) return null;
    pos.* += 1;
    return token;
}

pub fn peekKeyword(tokens: []const Token, pos: usize, keyword: []const u8) bool {
    if (pos >= tokens.len) return false;
    const token = tokens[pos];
    return token.kind == .identifier and std.ascii.eqlIgnoreCase(token.text, keyword);
}

pub fn peekKind(tokens: []const Token, pos: usize, kind: TokenKind) bool {
    return pos < tokens.len and tokens[pos].kind == kind;
}

pub fn atEnd(tokens: []const Token, pos: usize) bool {
    return pos >= tokens.len;
}

pub fn findTopLevelKeyword(tokens: []const Token, keyword: []const u8) ?usize {
    var depth: usize = 0;
    for (tokens, 0..) |token, i| {
        switch (token.kind) {
            .lparen => depth += 1,
            .rparen => {
                if (depth == 0) return null;
                depth -= 1;
            },
            .semicolon => if (depth == 0) return null,
            .identifier => if (depth == 0 and std.ascii.eqlIgnoreCase(token.text, keyword)) return i,
            else => {},
        }
    }
    return null;
}

pub fn findMatchingRParenIndex(tokens: []const Token, lparen_index: usize) ?usize {
    if (lparen_index >= tokens.len or tokens[lparen_index].kind != .lparen) return null;
    var depth: usize = 1;
    var index = lparen_index + 1;
    while (index < tokens.len) : (index += 1) {
        switch (tokens[index].kind) {
            .lparen => depth += 1,
            .rparen => {
                depth -= 1;
                if (depth == 0) return index;
            },
            else => {},
        }
    }
    return null;
}
