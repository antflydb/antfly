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
    access,
    add,
    aggregate,
    all,
    always,
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
    asymmetric,
    avg,
    base_weight,
    before,
    begin,
    between,
    bit_length,
    bool_and,
    bool_or,
    btrim,
    buffers,
    by,
    cascade,
    call,
    cardinality,
    case,
    cast,
    check,
    ceil,
    checkpoint,
    char_length,
    character_length,
    chr,
    cluster,
    close,
    column,
    collation,
    coalesce,
    comment,
    conflict,
    concat,
    concat_ws,
    constraint,
    commit,
    current,
    @"continue",
    convert_from,
    copy,
    count,
    costs,
    create,
    cross,
    current_date,
    current_timestamp,
    data,
    database,
    date,
    deallocate,
    declare,
    default,
    delete,
    desc,
    distinct,
    date_bin,
    date_part,
    date_trunc,
    do,
    domain,
    discard,
    drop,
    @"else",
    each,
    end,
    ends_with,
    escape,
    except,
    exists,
    explain,
    execute,
    extension,
    extract,
    false,
    fetch,
    field,
    filter,
    first,
    floor,
    following,
    @"for",
    format,
    foreign,
    freshness,
    full,
    full_text_search,
    function,
    from,
    generated,
    gen_random_uuid,
    greatest,
    grant,
    graph,
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
    include,
    import,
    in,
    index,
    initcap,
    inner,
    insert,
    intersect,
    interval,
    into,
    is,
    isnull,
    join,
    json,
    json_array_length,
    json_build_object,
    json_extract_path,
    json_extract_path_text,
    json_typeof,
    jsonb_array_length,
    jsonb_build_object,
    jsonb_extract_path,
    jsonb_extract_path_text,
    jsonb_set,
    jsonb_typeof,
    key,
    kind,
    label,
    lateral,
    last,
    least,
    left,
    length,
    like,
    limit,
    listen,
    load,
    locked,
    lock,
    lower,
    lpad,
    ltrim,
    match,
    materialized,
    matched,
    md5,
    method,
    merge,
    metric,
    metric_freshness,
    max,
    min,
    missing_score,
    mod,
    move,
    name,
    natural,
    no,
    not,
    notnull,
    next,
    notify,
    nothing,
    nowait,
    now,
    null,
    nullif,
    nulls,
    of,
    offset,
    octet_length,
    oids,
    off,
    on,
    only,
    operator,
    @"or",
    order,
    outer,
    owned,
    overriding,
    overlaps,
    overlay,
    over,
    partition,
    percentile_cont,
    percentile_disc,
    period,
    placing,
    policy,
    portion,
    position,
    power,
    preceding,
    prepare,
    prepared,
    primary,
    privileges,
    procedure,
    program,
    publication,
    regexp_count,
    regexp_instr,
    regexp_like,
    regexp_match,
    regexp_replace,
    regexp_substr,
    reassign,
    reindex,
    recursive,
    refresh,
    release,
    rename,
    replace,
    repeat,
    range,
    returning,
    revoke,
    reverse,
    right,
    reset,
    restrict,
    restart,
    rollback,
    role,
    round,
    row,
    rows,
    rule,
    sign,
    savepoint,
    schema,
    security,
    sequence,
    select,
    server,
    set,
    settings,
    show,
    share,
    skip,
    semantic_search,
    some,
    source,
    sources,
    split_part,
    sqrt,
    starts_with,
    stdin,
    stored,
    strpos,
    string_agg,
    string_to_array,
    substr,
    substring,
    subscription,
    sum,
    summary,
    symmetric,
    system,
    table,
    tablespace,
    temp,
    temporary,
    then,
    text,
    timing,
    timestamp,
    timestamptz,
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
    unlisten,
    unbounded,
    unlogged,
    unique,
    unknown,
    update,
    upper,
    user,
    using,
    validate,
    value,
    values,
    vacuum,
    verbose,
    vector_search,
    versioning,
    virtual,
    view,
    wal,
    weight,
    when,
    where,
    window,
    with,
    within,
    without,
    uuid_generate_v4,
    yes,
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

    pub fn matchesQualifiedKeywordTag(self: Token, qualifier: []const u8, keyword: TokenKeyword) bool {
        if (self.matchesKeywordTag(keyword)) return true;
        if (self.kind != .identifier) return false;
        const dot = std.mem.indexOfScalar(u8, self.text, '.') orelse return false;
        if (std.mem.indexOfScalar(u8, self.text[dot + 1 ..], '.') != null) return false;
        if (!std.ascii.eqlIgnoreCase(self.text[0..dot], qualifier)) return false;
        const member = self.text[dot + 1 ..];
        if (keywordFromIdentifier(member)) |member_keyword| return member_keyword == keyword;
        return false;
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

    const qualified_function = Token{
        .kind = .identifier,
        .text = "antfly.full_text_search",
        .source_start = 0,
        .source_end = 23,
    };
    try std.testing.expect(qualified_function.matchesQualifiedKeywordTag("antfly", .full_text_search));
    try std.testing.expect(!qualified_function.matchesKeywordTag(.full_text_search));
    try std.testing.expect(!qualified_function.matchesQualifiedKeywordTag("public", .full_text_search));

    try std.testing.expectEqual(TokenKeyword.access, keywordFromIdentifier("ACCESS").?);
    try std.testing.expectEqual(TokenKeyword.before, keywordFromIdentifier("BEFORE").?);
    try std.testing.expectEqual(TokenKeyword.between, keywordFromIdentifier("BETWEEN").?);
    try std.testing.expectEqual(TokenKeyword.then, keywordFromIdentifier("THEN").?);
    try std.testing.expectEqual(TokenKeyword.@"else", keywordFromIdentifier("ELSE").?);
    try std.testing.expectEqual(TokenKeyword.end, keywordFromIdentifier("END").?);
    try std.testing.expectEqual(TokenKeyword.current_date, keywordFromIdentifier("CURRENT_DATE").?);
    try std.testing.expectEqual(TokenKeyword.current_timestamp, keywordFromIdentifier("CURRENT_TIMESTAMP").?);
    try std.testing.expectEqual(TokenKeyword.current, keywordFromIdentifier("CURRENT").?);
    try std.testing.expectEqual(TokenKeyword.call, keywordFromIdentifier("CALL").?);
    try std.testing.expectEqual(TokenKeyword.checkpoint, keywordFromIdentifier("CHECKPOINT").?);
    try std.testing.expectEqual(TokenKeyword.cluster, keywordFromIdentifier("CLUSTER").?);
    try std.testing.expectEqual(TokenKeyword.close, keywordFromIdentifier("CLOSE").?);
    try std.testing.expectEqual(TokenKeyword.collation, keywordFromIdentifier("COLLATION").?);
    try std.testing.expectEqual(TokenKeyword.comment, keywordFromIdentifier("COMMENT").?);
    try std.testing.expectEqual(TokenKeyword.declare, keywordFromIdentifier("DECLARE").?);
    try std.testing.expectEqual(TokenKeyword.domain, keywordFromIdentifier("DOMAIN").?);
    try std.testing.expectEqual(TokenKeyword.each, keywordFromIdentifier("EACH").?);
    try std.testing.expectEqual(TokenKeyword.execute, keywordFromIdentifier("EXECUTE").?);
    try std.testing.expectEqual(TokenKeyword.buffers, keywordFromIdentifier("BUFFERS").?);
    try std.testing.expectEqual(TokenKeyword.costs, keywordFromIdentifier("COSTS").?);
    try std.testing.expectEqual(TokenKeyword.cross, keywordFromIdentifier("CROSS").?);
    try std.testing.expectEqual(TokenKeyword.format, keywordFromIdentifier("FORMAT").?);
    try std.testing.expectEqual(TokenKeyword.grant, keywordFromIdentifier("GRANT").?);
    try std.testing.expectEqual(TokenKeyword.label, keywordFromIdentifier("LABEL").?);
    try std.testing.expectEqual(TokenKeyword.btrim, keywordFromIdentifier("BTRIM").?);
    try std.testing.expectEqual(TokenKeyword.concat_ws, keywordFromIdentifier("CONCAT_WS").?);
    try std.testing.expectEqual(TokenKeyword.function, keywordFromIdentifier("FUNCTION").?);
    try std.testing.expectEqual(TokenKeyword.initcap, keywordFromIdentifier("INITCAP").?);
    try std.testing.expectEqual(TokenKeyword.all, keywordFromIdentifier("ALL").?);
    try std.testing.expectEqual(TokenKeyword.aggregate, keywordFromIdentifier("AGGREGATE").?);
    try std.testing.expectEqual(TokenKeyword.@"and", keywordFromIdentifier("AND").?);
    try std.testing.expectEqual(TokenKeyword.any, keywordFromIdentifier("ANY").?);
    try std.testing.expectEqual(TokenKeyword.ilike, keywordFromIdentifier("ILIKE").?);
    try std.testing.expectEqual(TokenKeyword.include, keywordFromIdentifier("INCLUDE").?);
    try std.testing.expectEqual(TokenKeyword.import, keywordFromIdentifier("IMPORT").?);
    try std.testing.expectEqual(TokenKeyword.in, keywordFromIdentifier("IN").?);
    try std.testing.expectEqual(TokenKeyword.interval, keywordFromIdentifier("INTERVAL").?);
    try std.testing.expectEqual(TokenKeyword.is, keywordFromIdentifier("IS").?);
    try std.testing.expectEqual(TokenKeyword.json, keywordFromIdentifier("JSON").?);
    try std.testing.expectEqual(TokenKeyword.length, keywordFromIdentifier("LENGTH").?);
    try std.testing.expectEqual(TokenKeyword.like, keywordFromIdentifier("LIKE").?);
    try std.testing.expectEqual(TokenKeyword.listen, keywordFromIdentifier("LISTEN").?);
    try std.testing.expectEqual(TokenKeyword.load, keywordFromIdentifier("LOAD").?);
    try std.testing.expectEqual(TokenKeyword.lock, keywordFromIdentifier("LOCK").?);
    try std.testing.expectEqual(TokenKeyword.lpad, keywordFromIdentifier("LPAD").?);
    try std.testing.expectEqual(TokenKeyword.ltrim, keywordFromIdentifier("LTRIM").?);
    try std.testing.expectEqual(TokenKeyword.null, keywordFromIdentifier("NULL").?);
    try std.testing.expectEqual(TokenKeyword.@"or", keywordFromIdentifier("OR").?);
    try std.testing.expectEqual(TokenKeyword.operator, keywordFromIdentifier("OPERATOR").?);
    try std.testing.expectEqual(TokenKeyword.some, keywordFromIdentifier("SOME").?);
    try std.testing.expectEqual(TokenKeyword.match, keywordFromIdentifier("MATCH").?);
    try std.testing.expectEqual(TokenKeyword.matched, keywordFromIdentifier("MATCHED").?);
    try std.testing.expectEqual(TokenKeyword.md5, keywordFromIdentifier("MD5").?);
    try std.testing.expectEqual(TokenKeyword.method, keywordFromIdentifier("METHOD").?);
    try std.testing.expectEqual(TokenKeyword.move, keywordFromIdentifier("MOVE").?);
    try std.testing.expectEqual(TokenKeyword.natural, keywordFromIdentifier("NATURAL").?);
    try std.testing.expectEqual(TokenKeyword.not, keywordFromIdentifier("NOT").?);
    try std.testing.expectEqual(TokenKeyword.now, keywordFromIdentifier("NOW").?);
    try std.testing.expectEqual(TokenKeyword.next, keywordFromIdentifier("NEXT").?);
    try std.testing.expectEqual(TokenKeyword.notify, keywordFromIdentifier("NOTIFY").?);
    try std.testing.expectEqual(TokenKeyword.owned, keywordFromIdentifier("OWNED").?);
    try std.testing.expectEqual(TokenKeyword.overriding, keywordFromIdentifier("OVERRIDING").?);
    try std.testing.expectEqual(TokenKeyword.following, keywordFromIdentifier("FOLLOWING").?);
    try std.testing.expectEqual(TokenKeyword.fetch, keywordFromIdentifier("FETCH").?);
    try std.testing.expectEqual(TokenKeyword.full, keywordFromIdentifier("FULL").?);
    try std.testing.expectEqual(TokenKeyword.last, keywordFromIdentifier("LAST").?);
    try std.testing.expectEqual(TokenKeyword.placing, keywordFromIdentifier("PLACING").?);
    try std.testing.expectEqual(TokenKeyword.procedure, keywordFromIdentifier("PROCEDURE").?);
    try std.testing.expectEqual(TokenKeyword.preceding, keywordFromIdentifier("PRECEDING").?);
    try std.testing.expectEqual(TokenKeyword.range, keywordFromIdentifier("RANGE").?);
    try std.testing.expectEqual(TokenKeyword.recursive, keywordFromIdentifier("RECURSIVE").?);
    try std.testing.expectEqual(TokenKeyword.restrict, keywordFromIdentifier("RESTRICT").?);
    try std.testing.expectEqual(TokenKeyword.right, keywordFromIdentifier("RIGHT").?);
    try std.testing.expectEqual(TokenKeyword.row, keywordFromIdentifier("ROW").?);
    try std.testing.expectEqual(TokenKeyword.rows, keywordFromIdentifier("ROWS").?);
    try std.testing.expectEqual(TokenKeyword.rpad, keywordFromIdentifier("RPAD").?);
    try std.testing.expectEqual(TokenKeyword.rtrim, keywordFromIdentifier("RTRIM").?);
    try std.testing.expectEqual(TokenKeyword.char_length, keywordFromIdentifier("CHAR_LENGTH").?);
    try std.testing.expectEqual(TokenKeyword.jsonb_extract_path_text, keywordFromIdentifier("JSONB_EXTRACT_PATH_TEXT").?);
    try std.testing.expectEqual(TokenKeyword.jsonb_set, keywordFromIdentifier("JSONB_SET").?);
    try std.testing.expectEqual(TokenKeyword.regexp_substr, keywordFromIdentifier("REGEXP_SUBSTR").?);
    try std.testing.expectEqual(TokenKeyword.reassign, keywordFromIdentifier("REASSIGN").?);
    try std.testing.expectEqual(TokenKeyword.reindex, keywordFromIdentifier("REINDEX").?);
    try std.testing.expectEqual(TokenKeyword.refresh, keywordFromIdentifier("REFRESH").?);
    try std.testing.expectEqual(TokenKeyword.release, keywordFromIdentifier("RELEASE").?);
    try std.testing.expectEqual(TokenKeyword.revoke, keywordFromIdentifier("REVOKE").?);
    try std.testing.expectEqual(TokenKeyword.role, keywordFromIdentifier("ROLE").?);
    try std.testing.expectEqual(TokenKeyword.savepoint, keywordFromIdentifier("SAVEPOINT").?);
    try std.testing.expectEqual(TokenKeyword.security, keywordFromIdentifier("SECURITY").?);
    try std.testing.expectEqual(TokenKeyword.sequence, keywordFromIdentifier("SEQUENCE").?);
    try std.testing.expectEqual(TokenKeyword.settings, keywordFromIdentifier("SETTINGS").?);
    try std.testing.expectEqual(TokenKeyword.string_to_array, keywordFromIdentifier("STRING_TO_ARRAY").?);
    try std.testing.expectEqual(TokenKeyword.summary, keywordFromIdentifier("SUMMARY").?);
    try std.testing.expectEqual(TokenKeyword.system, keywordFromIdentifier("SYSTEM").?);
    try std.testing.expectEqual(TokenKeyword.text, keywordFromIdentifier("TEXT").?);
    try std.testing.expectEqual(TokenKeyword.timing, keywordFromIdentifier("TIMING").?);
    try std.testing.expectEqual(TokenKeyword.timestamp, keywordFromIdentifier("TIMESTAMP").?);
    try std.testing.expectEqual(TokenKeyword.timestamptz, keywordFromIdentifier("TIMESTAMPTZ").?);
    try std.testing.expectEqual(TokenKeyword.to_jsonb, keywordFromIdentifier("TO_JSONB").?);
    try std.testing.expectEqual(TokenKeyword.unlisten, keywordFromIdentifier("UNLISTEN").?);
    try std.testing.expectEqual(TokenKeyword.vacuum, keywordFromIdentifier("VACUUM").?);
    try std.testing.expectEqual(TokenKeyword.verbose, keywordFromIdentifier("VERBOSE").?);
    try std.testing.expectEqual(TokenKeyword.view, keywordFromIdentifier("VIEW").?);
    try std.testing.expectEqual(TokenKeyword.wal, keywordFromIdentifier("WAL").?);
    try std.testing.expectEqual(TokenKeyword.partition, keywordFromIdentifier("PARTITION").?);
    try std.testing.expectEqual(TokenKeyword.policy, keywordFromIdentifier("POLICY").?);
    try std.testing.expectEqual(TokenKeyword.publication, keywordFromIdentifier("PUBLICATION").?);
    try std.testing.expectEqual(TokenKeyword.rule, keywordFromIdentifier("RULE").?);
    try std.testing.expectEqual(TokenKeyword.server, keywordFromIdentifier("SERVER").?);
    try std.testing.expectEqual(TokenKeyword.subscription, keywordFromIdentifier("SUBSCRIPTION").?);
    try std.testing.expectEqual(TokenKeyword.tablespace, keywordFromIdentifier("TABLESPACE").?);
    try std.testing.expectEqual(TokenKeyword.unbounded, keywordFromIdentifier("UNBOUNDED").?);
    try std.testing.expectEqual(TokenKeyword.user, keywordFromIdentifier("USER").?);
    try std.testing.expectEqual(TokenKeyword.value, keywordFromIdentifier("VALUE").?);
    try std.testing.expectEqual(TokenKeyword.window, keywordFromIdentifier("WINDOW").?);
    try std.testing.expectEqual(TokenKeyword.yes, keywordFromIdentifier("YES").?);
    try std.testing.expectEqual(TokenKeyword.off, keywordFromIdentifier("OFF").?);
}
