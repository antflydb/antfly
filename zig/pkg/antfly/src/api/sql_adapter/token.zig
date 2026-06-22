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

pub const TokenKind = enum {
    identifier,
    string,
    number,
    placeholder,
    comma,
    star,
    eq,
    neq,
    gt,
    gte,
    lt,
    lte,
    plus,
    minus,
    slash,
    percent,
    lparen,
    rparen,
    lbracket,
    rbracket,
    at_contains,
    range_overlap,
    pipe_concat,
    question,
    question_any,
    question_all,
    arrow_json,
    arrow_text,
    path_arrow_json,
    path_arrow_text,
    regex_match,
    regex_imatch,
    regex_not_match,
    regex_not_imatch,
    semicolon,
};

pub const TokenKeyword = enum {
    add,
    all,
    alter,
    analyze,
    @"and",
    as,
    asc,
    begin,
    by,
    cascade,
    case,
    cast,
    coalesce,
    conflict,
    constraint,
    create,
    delete,
    desc,
    distinct,
    do,
    drop,
    except,
    exists,
    explain,
    false,
    fetch,
    @"for",
    from,
    group,
    having,
    @"if",
    ilike,
    in,
    insert,
    intersect,
    into,
    join,
    lateral,
    like,
    limit,
    materialized,
    merge,
    not,
    null,
    offset,
    on,
    only,
    @"or",
    order,
    over,
    recursive,
    returning,
    select,
    set,
    table,
    to,
    true,
    truncate,
    @"union",
    update,
    using,
    values,
    when,
    where,
    with,
};

pub const Token = struct {
    kind: TokenKind,
    text: []const u8,
    owned: bool = false,
    source_start: usize = 0,
    source_end: usize = 0,
    keyword: ?TokenKeyword = null,

    pub fn isKeyword(self: Token, keyword: TokenKeyword) bool {
        return self.kind == .identifier and self.keyword == keyword;
    }

    pub fn matchesKeyword(self: Token, keyword_text: []const u8) bool {
        if (self.kind != .identifier) return false;
        if (keywordFromIdentifier(keyword_text)) |keyword| {
            if (self.keyword == keyword) return true;
            if (self.keyword == null and self.source_start == 0 and self.source_end == 0) {
                return std.ascii.eqlIgnoreCase(self.text, keyword_text);
            }
            return false;
        }
        return std.ascii.eqlIgnoreCase(self.text, keyword_text);
    }

    pub fn sourceSpan(self: Token) SourceSpan {
        return .{ .start = self.source_start, .end = self.source_end };
    }
};

pub const SourceSpan = struct {
    start: usize = 0,
    end: usize = 0,
};

pub fn keywordFromIdentifier(identifier: []const u8) ?TokenKeyword {
    if (identifier.len == 0 or identifier.len > 32) return null;
    var lower: [32]u8 = undefined;
    for (identifier, 0..) |ch, i| {
        if (!(std.ascii.isAlphanumeric(ch) or ch == '_')) return null;
        lower[i] = std.ascii.toLower(ch);
    }
    return std.meta.stringToEnum(TokenKeyword, lower[0..identifier.len]);
}
