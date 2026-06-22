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
    abs,
    analyze,
    @"and",
    any,
    array,
    array_agg,
    array_append,
    array_cat,
    array_length,
    array_position,
    array_positions,
    array_prepend,
    array_remove,
    array_replace,
    array_to_string,
    ascii,
    as,
    asc,
    avg,
    base_weight,
    begin,
    between,
    bit_length,
    bool_and,
    bool_or,
    btrim,
    by,
    cascade,
    cardinality,
    case,
    cast,
    ceil,
    char_length,
    character_length,
    chr,
    column,
    coalesce,
    conflict,
    concat,
    concat_ws,
    constraint,
    commit,
    @"continue",
    convert_from,
    copy,
    count,
    create,
    current_date,
    current_timestamp,
    data,
    default,
    delete,
    desc,
    distinct,
    date_bin,
    date_part,
    date_trunc,
    do,
    discard,
    drop,
    @"else",
    end,
    ends_with,
    except,
    exists,
    explain,
    extract,
    false,
    fetch,
    field,
    filter,
    first,
    floor,
    @"for",
    foreign,
    freshness,
    full_text_search,
    from,
    gen_random_uuid,
    greatest,
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
    initcap,
    inner,
    insert,
    intersect,
    into,
    is,
    join,
    json_array_length,
    json_build_object,
    json_extract_path,
    json_extract_path_text,
    json_typeof,
    jsonb_array_length,
    jsonb_build_object,
    jsonb_extract_path,
    jsonb_extract_path_text,
    jsonb_typeof,
    key,
    kind,
    lateral,
    least,
    left,
    length,
    like,
    limit,
    lower,
    lpad,
    ltrim,
    materialized,
    md5,
    merge,
    metric,
    metric_freshness,
    max,
    min,
    missing_score,
    mod,
    name,
    no,
    not,
    nothing,
    now,
    null,
    nullif,
    nulls,
    of,
    offset,
    octet_length,
    oids,
    on,
    only,
    @"or",
    order,
    outer,
    overlay,
    over,
    percentile_cont,
    percentile_disc,
    period,
    portion,
    power,
    prepare,
    prepared,
    program,
    regexp_count,
    regexp_instr,
    regexp_like,
    regexp_match,
    regexp_replace,
    regexp_substr,
    recursive,
    rename,
    replace,
    repeat,
    returning,
    reverse,
    right,
    reset,
    restart,
    rollback,
    round,
    sign,
    savepoint,
    select,
    set,
    show,
    share,
    semantic_search,
    some,
    source,
    sources,
    split_part,
    sqrt,
    starts_with,
    stdin,
    strpos,
    string_agg,
    string_to_array,
    substr,
    substring,
    sum,
    system,
    table,
    then,
    to,
    to_jsonb,
    true,
    trigger,
    trim,
    trunc,
    rpad,
    rtrim,
    translate,
    type,
    truncate,
    @"union",
    unique,
    unknown,
    update,
    upper,
    using,
    validate,
    vector_search,
    versioning,
    values,
    weight,
    when,
    where,
    window,
    with,
    within,
    uuid_generate_v4,
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
    try std.testing.expectEqual(TokenKeyword.then, keywordFromIdentifier("THEN").?);
    try std.testing.expectEqual(TokenKeyword.@"else", keywordFromIdentifier("ELSE").?);
    try std.testing.expectEqual(TokenKeyword.end, keywordFromIdentifier("END").?);
    try std.testing.expectEqual(TokenKeyword.current_date, keywordFromIdentifier("CURRENT_DATE").?);
    try std.testing.expectEqual(TokenKeyword.current_timestamp, keywordFromIdentifier("CURRENT_TIMESTAMP").?);
    try std.testing.expectEqual(TokenKeyword.btrim, keywordFromIdentifier("BTRIM").?);
    try std.testing.expectEqual(TokenKeyword.concat_ws, keywordFromIdentifier("CONCAT_WS").?);
    try std.testing.expectEqual(TokenKeyword.initcap, keywordFromIdentifier("INITCAP").?);
    try std.testing.expectEqual(TokenKeyword.length, keywordFromIdentifier("LENGTH").?);
    try std.testing.expectEqual(TokenKeyword.lpad, keywordFromIdentifier("LPAD").?);
    try std.testing.expectEqual(TokenKeyword.ltrim, keywordFromIdentifier("LTRIM").?);
    try std.testing.expectEqual(TokenKeyword.md5, keywordFromIdentifier("MD5").?);
    try std.testing.expectEqual(TokenKeyword.now, keywordFromIdentifier("NOW").?);
    try std.testing.expectEqual(TokenKeyword.right, keywordFromIdentifier("RIGHT").?);
    try std.testing.expectEqual(TokenKeyword.rpad, keywordFromIdentifier("RPAD").?);
    try std.testing.expectEqual(TokenKeyword.rtrim, keywordFromIdentifier("RTRIM").?);
    try std.testing.expectEqual(TokenKeyword.char_length, keywordFromIdentifier("CHAR_LENGTH").?);
    try std.testing.expectEqual(TokenKeyword.jsonb_extract_path_text, keywordFromIdentifier("JSONB_EXTRACT_PATH_TEXT").?);
    try std.testing.expectEqual(TokenKeyword.regexp_substr, keywordFromIdentifier("REGEXP_SUBSTR").?);
    try std.testing.expectEqual(TokenKeyword.string_to_array, keywordFromIdentifier("STRING_TO_ARRAY").?);
    try std.testing.expectEqual(TokenKeyword.to_jsonb, keywordFromIdentifier("TO_JSONB").?);
    try std.testing.expectEqual(TokenKeyword.window, keywordFromIdentifier("WINDOW").?);
}
