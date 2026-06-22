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
    any,
    array,
    array_agg,
    as,
    asc,
    avg,
    base_weight,
    begin,
    between,
    bool_and,
    bool_or,
    by,
    cascade,
    case,
    cast,
    column,
    coalesce,
    conflict,
    constraint,
    commit,
    @"continue",
    copy,
    count,
    create,
    data,
    default,
    delete,
    desc,
    distinct,
    do,
    discard,
    drop,
    except,
    exists,
    explain,
    false,
    fetch,
    field,
    filter,
    first,
    @"for",
    foreign,
    freshness,
    full_text_search,
    from,
    graph_k_shortest_paths,
    graph_match,
    graph_metric,
    graph_metric_rerank,
    graph_neighbors,
    graph_shortest_path,
    graph_traverse,
    group,
    having,
    hybrid_search,
    @"if",
    identity,
    ilike,
    in,
    inner,
    insert,
    intersect,
    into,
    is,
    join,
    key,
    kind,
    lateral,
    left,
    like,
    limit,
    materialized,
    merge,
    metric,
    metric_freshness,
    max,
    min,
    missing_score,
    name,
    no,
    not,
    nothing,
    null,
    nullif,
    nulls,
    of,
    offset,
    oids,
    on,
    only,
    @"or",
    order,
    outer,
    over,
    percentile_cont,
    percentile_disc,
    period,
    portion,
    prepare,
    program,
    recursive,
    rename,
    returning,
    reset,
    restart,
    rollback,
    select,
    set,
    show,
    share,
    semantic_search,
    some,
    source,
    sources,
    stdin,
    string_agg,
    sum,
    system,
    table,
    to,
    true,
    trigger,
    type,
    truncate,
    @"union",
    unique,
    unknown,
    update,
    using,
    validate,
    vector_search,
    versioning,
    values,
    weight,
    when,
    where,
    with,
    within,
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

    pub fn matchesKeywordTag(self: Token, keyword: TokenKeyword) bool {
        if (self.kind != .identifier) return false;
        if (self.keyword) |token_keyword| return token_keyword == keyword;
        if (self.source_start == 0 and self.source_end == 0) {
            return std.ascii.eqlIgnoreCase(self.text, @tagName(keyword));
        }
        return false;
    }

    pub fn matchesKeyword(self: Token, keyword_text: []const u8) bool {
        if (self.kind != .identifier) return false;
        if (keywordFromIdentifier(keyword_text)) |keyword| {
            return self.matchesKeywordTag(keyword);
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

test "sql adapter tokens match keyword tags without treating quoted identifiers as keywords" {
    const source_keyword = Token{
        .kind = .identifier,
        .text = "SELECT",
        .source_start = 0,
        .source_end = 6,
        .keyword = keywordFromIdentifier("SELECT"),
    };
    try std.testing.expect(source_keyword.matchesKeywordTag(.select));
    try std.testing.expect(source_keyword.matchesKeyword("select"));

    const synthetic_keyword = Token{ .kind = .identifier, .text = "select" };
    try std.testing.expect(synthetic_keyword.matchesKeywordTag(.select));

    const quoted_identifier = Token{
        .kind = .identifier,
        .text = "select",
        .source_start = 0,
        .source_end = 8,
    };
    try std.testing.expect(!quoted_identifier.matchesKeywordTag(.select));
    try std.testing.expect(!quoted_identifier.matchesKeyword("select"));

    const aggregate_function = Token{
        .kind = .identifier,
        .text = "array_agg",
        .source_start = 0,
        .source_end = 9,
        .keyword = keywordFromIdentifier("array_agg"),
    };
    try std.testing.expect(aggregate_function.matchesKeywordTag(.array_agg));
}
