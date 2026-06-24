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

const generated = @import("grammar/generated/root.zig");
const lexer = @import("lexer.zig");
const token_mod = @import("token.zig");

pub const GeneratedSqlStatementKind = enum {
    session,
    transaction,
    prepared,
    ddl,
    dml,
    read,
    graph,
    unsupported,
    other,
};

pub const GeneratedSqlSessionKind = enum {
    set,
    reset,
    show,
    discard_all,
};

pub const GeneratedSqlTransactionKind = enum {
    begin,
    commit,
    rollback,
};

pub const GeneratedSqlPreparedKind = enum {
    prepare,
    execute,
    deallocate,
};

pub const GeneratedSqlDdlKind = enum {
    create_database,
    create_schema,
    create_table,
    create_index,
    create_extension,
    alter_table,
    drop_table,
    drop_index,
    drop_schema,
    drop_database,
    drop_extension,
    create_graph_index,
    create_graph_metric,
};

pub const GeneratedSqlDmlKind = enum {
    insert_values,
    insert_select,
    update,
    delete,
    truncate,
    merge,
};

pub const GeneratedSqlReadKind = enum {
    query,
    aggregate,
    join,
    lateral,
    window,
    set_operation,
    cte,
};

pub const GeneratedSqlGraphKind = enum {
    create_index,
    create_metric,
};

pub const GeneratedSqlUnsupportedKind = enum {
    analyze,
    explain,
};

pub const GeneratedSqlUnsupportedReason = enum {
    analyze_not_planned_by_generated_parser,
    explain_not_planned_by_generated_parser,
};

pub const GeneratedSqlStatement = union(GeneratedSqlStatementKind) {
    session: GeneratedSqlSessionKind,
    transaction: GeneratedSqlTransactionKind,
    prepared: GeneratedSqlPreparedKind,
    ddl: GeneratedSqlDdlKind,
    dml: GeneratedSqlDmlKind,
    read: GeneratedSqlReadKind,
    graph: GeneratedSqlGraphKind,
    unsupported: GeneratedSqlUnsupportedKind,
    other: void,
};

pub const GeneratedSqlTokenRange = struct {
    start: usize,
    end: usize,
};

pub const GeneratedSqlListAst = struct {
    first_tokens: ?GeneratedSqlTokenRange = null,
    last_tokens: ?GeneratedSqlTokenRange = null,
    items: []GeneratedSqlTokenRange = &.{},
    expressions: []GeneratedSqlExpressionAst = &.{},
    count: usize = 0,

    pub fn deinit(self: *GeneratedSqlListAst, alloc: std.mem.Allocator) void {
        for (self.expressions) |*expression| expression.deinit(alloc);
        if (self.expressions.len > 0) alloc.free(self.expressions);
        if (self.items.len > 0) alloc.free(self.items);
        self.* = .{};
    }
};

pub const GeneratedSqlExpressionKind = enum {
    token_range,
    comparison,
    like,
    ilike,
    in_list,
    between,
    not_like,
    not_ilike,
    not_in_list,
    not_between,
    quantified_comparison,
    is_null,
    is_not_null,
    is_true,
    is_false,
    is_unknown,
    is_not_true,
    is_not_false,
    is_not_unknown,
    is_distinct_from,
    is_not_distinct_from,
    logical_or,
    logical_and,
    logical_not,
    grouped,
    additive,
    subtractive,
    multiplicative,
    divisive,
    modulo,
    json_access,
    json_text_access,
    json_path_access,
    json_path_text_access,
    function_call,
};

pub const GeneratedSqlExpressionAst = struct {
    kind: GeneratedSqlExpressionKind = .token_range,
    tokens: ?GeneratedSqlTokenRange = null,
    inner_tokens: ?GeneratedSqlTokenRange = null,
    inner_expression_kind: ?GeneratedSqlExpressionKind = null,
    inner_expression: ?*GeneratedSqlExpressionAst = null,
    function_name_tokens: ?GeneratedSqlTokenRange = null,
    argument_tokens: ?GeneratedSqlTokenRange = null,
    argument_items: GeneratedSqlListAst = .{},
    left_tokens: ?GeneratedSqlTokenRange = null,
    left_expression_kind: ?GeneratedSqlExpressionKind = null,
    left_expression: ?*GeneratedSqlExpressionAst = null,
    negation_tokens: ?GeneratedSqlTokenRange = null,
    operator_tokens: ?GeneratedSqlTokenRange = null,
    quantifier_tokens: ?GeneratedSqlTokenRange = null,
    right_tokens: ?GeneratedSqlTokenRange = null,
    right_expression_kind: ?GeneratedSqlExpressionKind = null,
    right_expression: ?*GeneratedSqlExpressionAst = null,

    pub fn deinit(self: *GeneratedSqlExpressionAst, alloc: std.mem.Allocator) void {
        if (self.inner_expression) |inner| {
            inner.deinit(alloc);
            alloc.destroy(inner);
        }
        if (self.left_expression) |left| {
            left.deinit(alloc);
            alloc.destroy(left);
        }
        if (self.right_expression) |right| {
            right.deinit(alloc);
            alloc.destroy(right);
        }
        self.argument_items.deinit(alloc);
        self.* = .{};
    }
};

pub const GeneratedSqlJoinKind = enum {
    inner,
    left,
    right,
    full,
};

pub const GeneratedSqlJoinAst = struct {
    tokens: GeneratedSqlTokenRange,
    operator_tokens: GeneratedSqlTokenRange,
    kind: GeneratedSqlJoinKind,
    left_tokens: GeneratedSqlTokenRange,
    right_tokens: GeneratedSqlTokenRange,
    predicate_tokens: GeneratedSqlTokenRange,
    predicate_expression: GeneratedSqlExpressionAst = .{},

    pub fn deinit(self: *GeneratedSqlJoinAst, alloc: std.mem.Allocator) void {
        self.predicate_expression.deinit(alloc);
        self.* = undefined;
    }
};

pub const GeneratedSqlCteAst = struct {
    name_tokens: GeneratedSqlTokenRange,
    body_tokens: ?GeneratedSqlTokenRange = null,
};

pub const GeneratedSqlSessionAst = struct {
    kind: GeneratedSqlSessionKind,
    statement_span: token_mod.SourceSpan,
    command_span: token_mod.SourceSpan,
    name_tokens: ?GeneratedSqlTokenRange = null,
    value_tokens: ?GeneratedSqlTokenRange = null,
};

pub const GeneratedSqlTransactionAst = struct {
    kind: GeneratedSqlTransactionKind,
    statement_span: token_mod.SourceSpan,
    command_span: token_mod.SourceSpan,
};

pub const GeneratedSqlPreparedAst = struct {
    kind: GeneratedSqlPreparedKind,
    statement_span: token_mod.SourceSpan,
    command_span: token_mod.SourceSpan,
    name_tokens: ?GeneratedSqlTokenRange = null,
    parameter_tokens: ?GeneratedSqlTokenRange = null,
    argument_tokens: ?GeneratedSqlTokenRange = null,
    inner_statement_tokens: ?GeneratedSqlTokenRange = null,
};

pub const GeneratedSqlDdlAst = struct {
    kind: GeneratedSqlDdlKind,
    statement_span: token_mod.SourceSpan,
    command_span: token_mod.SourceSpan,
    object_name_tokens: ?GeneratedSqlTokenRange = null,
    schema_name_tokens: ?GeneratedSqlTokenRange = null,
    version_tokens: ?GeneratedSqlTokenRange = null,
    if_not_exists: bool = false,
    if_exists: bool = false,
    cascade: bool = false,
    force: bool = false,
};

pub const GeneratedSqlDmlAst = struct {
    kind: GeneratedSqlDmlKind,
    statement_span: token_mod.SourceSpan,
    command_span: token_mod.SourceSpan,
    target_table_tokens: ?GeneratedSqlTokenRange = null,
    insert_columns_tokens: ?GeneratedSqlTokenRange = null,
    values_tokens: ?GeneratedSqlTokenRange = null,
    source_tokens: ?GeneratedSqlTokenRange = null,
    assignments_tokens: ?GeneratedSqlTokenRange = null,
    where_tokens: ?GeneratedSqlTokenRange = null,
    conflict_tokens: ?GeneratedSqlTokenRange = null,
    returning_tokens: ?GeneratedSqlTokenRange = null,
    additional_target_tokens: ?GeneratedSqlTokenRange = null,
    default_values: bool = false,
    restart_identity: bool = false,
    cascade: bool = false,
};

pub const GeneratedSqlReadAst = struct {
    kind: GeneratedSqlReadKind,
    statement_span: token_mod.SourceSpan,
    command_span: token_mod.SourceSpan,
    cte_tokens: ?GeneratedSqlTokenRange = null,
    cte_list_tokens: ?GeneratedSqlTokenRange = null,
    cte_name_tokens: ?GeneratedSqlTokenRange = null,
    cte_body_tokens: ?GeneratedSqlTokenRange = null,
    cte_last_name_tokens: ?GeneratedSqlTokenRange = null,
    cte_last_body_tokens: ?GeneratedSqlTokenRange = null,
    cte_items: []GeneratedSqlCteAst = &.{},
    cte_count: usize = 0,
    cte_recursive: bool = false,
    distinct_tokens: ?GeneratedSqlTokenRange = null,
    projection_tokens: ?GeneratedSqlTokenRange = null,
    projection_items: GeneratedSqlListAst = .{},
    projection_first_expression: GeneratedSqlExpressionAst = .{},
    projection_last_expression: GeneratedSqlExpressionAst = .{},
    source_tokens: ?GeneratedSqlTokenRange = null,
    join_tokens: ?GeneratedSqlTokenRange = null,
    join_operator_tokens: ?GeneratedSqlTokenRange = null,
    join_kind: ?GeneratedSqlJoinKind = null,
    join_left_tokens: ?GeneratedSqlTokenRange = null,
    join_right_tokens: ?GeneratedSqlTokenRange = null,
    join_predicate_tokens: ?GeneratedSqlTokenRange = null,
    join_predicate_expression: GeneratedSqlExpressionAst = .{},
    join_items: []GeneratedSqlJoinAst = &.{},
    where_tokens: ?GeneratedSqlTokenRange = null,
    where_expression: GeneratedSqlExpressionAst = .{},
    group_tokens: ?GeneratedSqlTokenRange = null,
    group_items: GeneratedSqlListAst = .{},
    group_first_expression: GeneratedSqlExpressionAst = .{},
    group_last_expression: GeneratedSqlExpressionAst = .{},
    having_tokens: ?GeneratedSqlTokenRange = null,
    having_expression: GeneratedSqlExpressionAst = .{},
    window_tokens: ?GeneratedSqlTokenRange = null,
    order_tokens: ?GeneratedSqlTokenRange = null,
    order_items: GeneratedSqlListAst = .{},
    order_first_expression: GeneratedSqlExpressionAst = .{},
    order_last_expression: GeneratedSqlExpressionAst = .{},
    limit_tokens: ?GeneratedSqlTokenRange = null,
    limit_expression: GeneratedSqlExpressionAst = .{},
    limit_all: bool = false,
    offset_tokens: ?GeneratedSqlTokenRange = null,
    offset_expression: GeneratedSqlExpressionAst = .{},
    fetch_tokens: ?GeneratedSqlTokenRange = null,
    fetch_count_tokens: ?GeneratedSqlTokenRange = null,
    fetch_count_expression: GeneratedSqlExpressionAst = .{},
    set_operation_tokens: ?GeneratedSqlTokenRange = null,

    pub fn deinit(self: *GeneratedSqlReadAst, alloc: std.mem.Allocator) void {
        if (self.cte_items.len > 0) alloc.free(self.cte_items);
        for (self.join_items) |*join| join.deinit(alloc);
        if (self.join_items.len > 0) alloc.free(self.join_items);
        self.projection_items.deinit(alloc);
        self.projection_first_expression.deinit(alloc);
        self.projection_last_expression.deinit(alloc);
        self.join_predicate_expression.deinit(alloc);
        self.where_expression.deinit(alloc);
        self.group_items.deinit(alloc);
        self.group_first_expression.deinit(alloc);
        self.group_last_expression.deinit(alloc);
        self.having_expression.deinit(alloc);
        self.order_items.deinit(alloc);
        self.order_first_expression.deinit(alloc);
        self.order_last_expression.deinit(alloc);
        self.limit_expression.deinit(alloc);
        self.offset_expression.deinit(alloc);
        self.fetch_count_expression.deinit(alloc);
        self.* = undefined;
    }
};

pub const GeneratedSqlGraphAst = struct {
    kind: GeneratedSqlGraphKind,
    statement_span: token_mod.SourceSpan,
    command_span: token_mod.SourceSpan,
};

pub const GeneratedSqlUnsupportedAst = struct {
    kind: GeneratedSqlUnsupportedKind,
    reason: GeneratedSqlUnsupportedReason,
    statement_span: token_mod.SourceSpan,
    command_span: token_mod.SourceSpan,
    subject_tokens: ?GeneratedSqlTokenRange = null,
};

pub const GeneratedSqlAst = union(enum) {
    session: GeneratedSqlSessionAst,
    transaction: GeneratedSqlTransactionAst,
    prepared: GeneratedSqlPreparedAst,
    ddl: GeneratedSqlDdlAst,
    dml: GeneratedSqlDmlAst,
    read: GeneratedSqlReadAst,
    graph: GeneratedSqlGraphAst,
    unsupported: GeneratedSqlUnsupportedAst,

    pub fn deinit(self: *GeneratedSqlAst, alloc: std.mem.Allocator) void {
        switch (self.*) {
            .read => |*read| read.deinit(alloc),
            else => {},
        }
        self.* = undefined;
    }
};

pub const GeneratedSqlParseResult = struct {
    kind: GeneratedSqlStatementKind,
    statement: GeneratedSqlStatement,
    ast: ?GeneratedSqlAst = null,

    pub fn deinit(self: *GeneratedSqlParseResult, alloc: std.mem.Allocator) void {
        if (self.ast) |*ast| ast.deinit(alloc);
        self.* = undefined;
    }
};

pub const GeneratedSqlDiagnostic = struct {
    state: u16,
    lookahead: u16,
    token_index: usize,
    source_start: usize,
    source_end: usize,
    expected: []const []const u8,
    actual: []const u8,
};

const DiagnosticSpan = struct {
    start: usize,
    end: usize,
    actual: []const u8,
};

pub const GeneratedSqlCorpusCase = struct {
    sql: []const u8,
    kind: GeneratedSqlStatementKind,
};

pub const first_family_corpus = [_]GeneratedSqlCorpusCase{
    .{ .sql = "SET antfly.sync_level = 'write'", .kind = .session },
    .{ .sql = "SET search_path public", .kind = .session },
    .{ .sql = "SET search_path TO public", .kind = .session },
    .{ .sql = "RESET search_path", .kind = .session },
    .{ .sql = "SHOW search_path", .kind = .session },
    .{ .sql = "DISCARD ALL", .kind = .session },
    .{ .sql = "BEGIN", .kind = .transaction },
    .{ .sql = "COMMIT", .kind = .transaction },
    .{ .sql = "ROLLBACK", .kind = .transaction },
    .{ .sql = "PREPARE read_stmt AS SELECT id FROM usage_records", .kind = .prepared },
    .{ .sql = "PREPARE read_stmt(text) AS SELECT id FROM usage_records WHERE status = $1", .kind = .prepared },
    .{ .sql = "EXECUTE read_stmt()", .kind = .prepared },
    .{ .sql = "DEALLOCATE read_stmt", .kind = .prepared },
};

pub const simple_ddl_corpus = [_]GeneratedSqlCorpusCase{
    .{ .sql = "CREATE DATABASE tenant_ops", .kind = .ddl },
    .{ .sql = "CREATE SCHEMA analytics", .kind = .ddl },
    .{ .sql = "CREATE SCHEMA IF NOT EXISTS analytics", .kind = .ddl },
    .{ .sql = "CREATE TABLE usage_records (id text PRIMARY KEY, status text DEFAULT 'open')", .kind = .ddl },
    .{ .sql = "CREATE TABLE IF NOT EXISTS usage_records (id text PRIMARY KEY)", .kind = .ddl },
    .{ .sql = "CREATE INDEX usage_records_status_idx ON usage_records (status)", .kind = .ddl },
    .{ .sql = "CREATE INDEX IF NOT EXISTS usage_records_status_idx ON usage_records (status)", .kind = .ddl },
    .{ .sql = "CREATE EXTENSION vector", .kind = .ddl },
    .{ .sql = "DROP TABLE usage_records", .kind = .ddl },
    .{ .sql = "DROP TABLE IF EXISTS usage_records", .kind = .ddl },
    .{ .sql = "DROP INDEX usage_records_status_idx", .kind = .ddl },
    .{ .sql = "DROP SCHEMA analytics CASCADE", .kind = .ddl },
    .{ .sql = "DROP DATABASE tenant_ops", .kind = .ddl },
};

pub const simple_dml_corpus = [_]GeneratedSqlCorpusCase{
    .{ .sql = "INSERT INTO usage_records (id, status) VALUES ('u1', 'open')", .kind = .dml },
    .{ .sql = "INSERT INTO usage_records (id) SELECT id FROM incoming_usage", .kind = .dml },
    .{ .sql = "UPDATE usage_records SET status = 'done' WHERE id = 'u1' RETURNING id", .kind = .dml },
    .{ .sql = "DELETE FROM usage_records WHERE id = 'u1' RETURNING id", .kind = .dml },
    .{ .sql = "TRUNCATE usage_records", .kind = .dml },
    .{ .sql = "MERGE INTO usage_records USING source_rows ON usage_records.id = source_rows.id WHEN MATCHED THEN UPDATE SET status = source_rows.status", .kind = .dml },
};

pub const simple_read_corpus = [_]GeneratedSqlCorpusCase{
    .{ .sql = "SELECT id, status FROM usage_records WHERE status = 'open' ORDER BY id LIMIT 10", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE status LIKE 'open%'", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE status ILIKE 'open%'", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE id IN ('u1', 'u2')", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE score BETWEEN 1 AND 10", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE status NOT LIKE 'closed%'", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE status NOT ILIKE 'closed%'", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE id NOT IN ('u1', 'u2')", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE score NOT BETWEEN 1 AND 10", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE score = ANY (1, 2)", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE score <> ALL (1, 2)", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE score > SOME (1, 2)", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE deleted_at IS NULL", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE deleted_at IS NOT NULL", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE active IS TRUE", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE active IS NOT FALSE", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE active IS UNKNOWN", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE active IS NOT UNKNOWN", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE status IS DISTINCT FROM previous_status", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE status IS NOT DISTINCT FROM previous_status", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE status = 'open' OR deleted_at IS NULL", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE status = 'open' AND deleted_at IS NULL", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE NOT deleted_at IS NULL", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE (status = 'open')", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE NOT (deleted_at IS NULL)", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE score + bonus > 10", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE score * weight > 10", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE payload ->> 'status' = 'open'", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE lower(status) = 'open'", .kind = .read },
    .{ .sql = "SELECT concat_ws(',', status), id FROM usage_records ORDER BY status, id", .kind = .read },
    .{ .sql = "SELECT id, row_number() OVER (PARTITION BY tenant, account ORDER BY id) AS rn FROM usage_records ORDER BY id, tenant", .kind = .read },
    .{ .sql = "SELECT DISTINCT status FROM usage_records ORDER BY status", .kind = .read },
    .{ .sql = "SELECT DISTINCT ON (organization_id) organization_id, id FROM usage_records ORDER BY organization_id ASC, created_at DESC", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records ORDER BY id LIMIT ALL OFFSET 2 ROWS", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records FETCH FIRST ROWS ONLY", .kind = .read },
    .{ .sql = "SELECT status FROM usage_records GROUP BY status HAVING status = 'open'", .kind = .read },
    .{ .sql = "SELECT usage_records.id FROM usage_records JOIN accounts ON usage_records.account_id = accounts.id", .kind = .read },
    .{ .sql = "SELECT usage_records.id FROM usage_records JOIN accounts ON usage_records.account_id = accounts.id JOIN tenants ON accounts.tenant_id = tenants.id", .kind = .read },
    .{ .sql = "SELECT usage_records.id FROM usage_records LEFT OUTER JOIN accounts ON usage_records.account_id = accounts.id", .kind = .read },
    .{ .sql = "SELECT id FROM LATERAL (SELECT id FROM usage_records) AS source_rows", .kind = .read },
    .{ .sql = "SELECT id, row_number() OVER (ORDER BY id) AS rn FROM usage_records", .kind = .read },
    .{ .sql = "SELECT id, row_number() OVER (PARTITION BY tenant ORDER BY id) AS rn FROM usage_records", .kind = .read },
    .{ .sql = "SELECT id, row_number() OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rn FROM usage_records", .kind = .read },
    .{ .sql = "SELECT id, row_number() OVER usage_window AS rn FROM usage_records WINDOW usage_window AS (ORDER BY id)", .kind = .read },
    .{ .sql = "SELECT id, row_number() OVER usage_window AS rn FROM usage_records WINDOW usage_window AS (PARTITION BY tenant ORDER BY id)", .kind = .read },
    .{ .sql = "SELECT id, row_number() OVER usage_window AS rn FROM usage_records WINDOW usage_window AS (ORDER BY id RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records UNION SELECT id FROM usage_archive", .kind = .read },
    .{ .sql = "WITH source_rows AS (SELECT id FROM usage_records) SELECT id FROM source_rows", .kind = .read },
    .{ .sql = "WITH first_rows AS (SELECT id FROM usage_records), second_rows AS (SELECT id FROM first_rows) SELECT id FROM second_rows", .kind = .read },
    .{ .sql = "WITH RECURSIVE source_rows AS (SELECT id FROM usage_records) SELECT id FROM source_rows", .kind = .read },
};

pub const simple_graph_corpus = [_]GeneratedSqlCorpusCase{
    .{ .sql = "CREATE GRAPH INDEX docs_edge_graph ON doc_edges", .kind = .graph },
    .{ .sql = "CREATE GRAPH METRIC docs_pagerank ON doc_edges WITH (metric = 'pagerank')", .kind = .graph },
};

pub const unsupported_corpus = [_]GeneratedSqlCorpusCase{
    .{ .sql = "ANALYZE", .kind = .unsupported },
    .{ .sql = "EXPLAIN SELECT id FROM usage_records", .kind = .unsupported },
};

pub fn parseSqlAlloc(alloc: std.mem.Allocator, sql: []const u8) !GeneratedSqlParseResult {
    var tokens = try lexer.tokenizeAlloc(alloc, sql);
    defer lexer.freeTokens(alloc, &tokens);
    return try parseTokensAlloc(alloc, tokens.items);
}

pub fn parseTokensAlloc(alloc: std.mem.Allocator, tokens: []const token_mod.Token) !GeneratedSqlParseResult {
    const token_ids = try tokenIdsAlloc(alloc, tokens);
    defer alloc.free(token_ids);
    try generated.parse(alloc, token_ids);
    const statement = classifyStatement(tokens);
    return .{
        .kind = std.meta.activeTag(statement),
        .statement = statement,
        .ast = try buildGeneratedAst(alloc, tokens, statement),
    };
}

pub fn parseFirstFamilyTokensAlloc(alloc: std.mem.Allocator, tokens: []const token_mod.Token) !?GeneratedSqlParseResult {
    if (!isFirstFamilyTokens(tokens)) return null;
    return try parseTokensAlloc(alloc, tokens);
}

pub fn parseGeneratedGateTokensAlloc(alloc: std.mem.Allocator, tokens: []const token_mod.Token) !?GeneratedSqlParseResult {
    const kind = classifyTokens(tokens);
    if (kind == .other) return null;
    return parseTokensAlloc(alloc, tokens) catch |err| switch (err) {
        error.UnsupportedSqlShape, error.UnexpectedToken => if (kind == .ddl or kind == .dml or kind == .read or kind == .unsupported) null else err,
        else => err,
    };
}

pub fn isFirstFamilyTokens(tokens: []const token_mod.Token) bool {
    const kind = classifyTokens(tokens);
    return kind == .session or kind == .transaction or kind == .prepared;
}

pub fn isGeneratedGateTokens(tokens: []const token_mod.Token) bool {
    return classifyTokens(tokens) != .other;
}

pub fn diagnosticAlloc(alloc: std.mem.Allocator, tokens: []const token_mod.Token) !?GeneratedSqlDiagnostic {
    const token_ids = try tokenIdsAlloc(alloc, tokens);
    defer alloc.free(token_ids);
    const info = try generated.parseError(alloc, token_ids) orelse return null;
    const actions = generated.actionsForState(info.state);
    const expected = try alloc.alloc([]const u8, actions.len);
    for (actions, 0..) |action, idx| expected[idx] = generated.symbolName(action.terminal);
    const span: DiagnosticSpan = if (info.token_index < tokens.len)
        .{ .start = tokens[info.token_index].source_start, .end = tokens[info.token_index].source_end, .actual = tokens[info.token_index].text }
    else
        .{ .start = if (tokens.len == 0) 0 else tokens[tokens.len - 1].source_end, .end = if (tokens.len == 0) 0 else tokens[tokens.len - 1].source_end, .actual = "$end" };
    return .{
        .state = info.state,
        .lookahead = info.lookahead,
        .token_index = info.token_index,
        .source_start = span.start,
        .source_end = span.end,
        .expected = expected,
        .actual = span.actual,
    };
}

pub fn tokenIdsAlloc(alloc: std.mem.Allocator, tokens: []const token_mod.Token) ![]u16 {
    var ids: std.ArrayListUnmanaged(u16) = .empty;
    errdefer ids.deinit(alloc);
    for (tokens, 0..) |tok, index| {
        if (tok.kind == .semicolon and trailingSemicolonOnly(tokens, index)) break;
        try appendTokenIds(alloc, &ids, tok);
    }
    return try ids.toOwnedSlice(alloc);
}

fn appendTokenIds(alloc: std.mem.Allocator, ids: *std.ArrayListUnmanaged(u16), tok: token_mod.Token) !void {
    switch (tok.kind) {
        .identifier => {
            if (try keywordSymbolIdAlloc(alloc, tok)) |id| {
                try ids.append(alloc, id);
                return;
            }
            try appendIdentifierIds(alloc, ids, tok.text);
        },
        .string => try appendSymbol(ids, alloc, "STRING"),
        .number => try appendSymbol(ids, alloc, "NUMBER"),
        .placeholder => try appendSymbol(ids, alloc, "PLACEHOLDER"),
        .comma => try appendSymbol(ids, alloc, "COMMA"),
        .star => try appendSymbol(ids, alloc, "STAR"),
        .eq => try appendSymbol(ids, alloc, "EQ"),
        .neq => try appendSymbol(ids, alloc, "NEQ"),
        .gt => try appendSymbol(ids, alloc, "GT"),
        .gte => try appendSymbol(ids, alloc, "GTE"),
        .lt => try appendSymbol(ids, alloc, "LT"),
        .lte => try appendSymbol(ids, alloc, "LTE"),
        .plus => try appendSymbol(ids, alloc, "PLUS"),
        .minus => try appendSymbol(ids, alloc, "MINUS"),
        .slash => try appendSymbol(ids, alloc, "SLASH"),
        .percent => try appendSymbol(ids, alloc, "PERCENT"),
        .lparen => try appendSymbol(ids, alloc, "LPAREN"),
        .rparen => try appendSymbol(ids, alloc, "RPAREN"),
        .lbracket => try appendSymbol(ids, alloc, "LBRACKET"),
        .rbracket => try appendSymbol(ids, alloc, "RBRACKET"),
        .arrow_json => try appendSymbol(ids, alloc, "ARROW_JSON"),
        .arrow_text => try appendSymbol(ids, alloc, "ARROW_TEXT"),
        .path_arrow_json => try appendSymbol(ids, alloc, "PATH_ARROW_JSON"),
        .path_arrow_text => try appendSymbol(ids, alloc, "PATH_ARROW_TEXT"),
        .semicolon => try appendSymbol(ids, alloc, "SEMICOLON"),
        else => return error.UnsupportedSqlShape,
    }
}

fn appendIdentifierIds(alloc: std.mem.Allocator, ids: *std.ArrayListUnmanaged(u16), text: []const u8) !void {
    var parts = std.mem.splitScalar(u8, text, '.');
    var emitted = false;
    while (parts.next()) |part| {
        if (part.len == 0) return error.UnsupportedSqlShape;
        if (emitted) try appendSymbol(ids, alloc, "DOT");
        try appendSymbol(ids, alloc, "IDENT");
        emitted = true;
    }
}

fn appendSymbol(ids: *std.ArrayListUnmanaged(u16), alloc: std.mem.Allocator, name: []const u8) !void {
    const id = generated.symbolId(name) orelse return error.UnsupportedSqlShape;
    try ids.append(alloc, id);
}

fn keywordSymbolIdAlloc(alloc: std.mem.Allocator, tok: token_mod.Token) !?u16 {
    if (tok.keyword == null) return null;
    const name = try uppercaseKeywordAlloc(alloc, tok.text);
    defer alloc.free(name);
    return generated.symbolId(name);
}

fn uppercaseKeywordAlloc(alloc: std.mem.Allocator, text: []const u8) ![]u8 {
    var out = try alloc.alloc(u8, text.len);
    for (text, 0..) |ch, idx| {
        out[idx] = switch (ch) {
            'a'...'z' => ch - 'a' + 'A',
            else => ch,
        };
    }
    return out;
}

fn trailingSemicolonOnly(tokens: []const token_mod.Token, index: usize) bool {
    if (tokens[index].kind != .semicolon) return false;
    for (tokens[index + 1 ..]) |tok| {
        if (tok.kind != .semicolon) return false;
    }
    return true;
}

fn classifyTokens(tokens: []const token_mod.Token) GeneratedSqlStatementKind {
    return std.meta.activeTag(classifyStatement(tokens));
}

fn classifyStatement(tokens: []const token_mod.Token) GeneratedSqlStatement {
    if (tokens.len == 0) return .other;
    const first = tokens[0];
    if (first.matchesKeywordTag(.set)) return .{ .session = .set };
    if (first.matchesKeywordTag(.reset)) return .{ .session = .reset };
    if (first.matchesKeywordTag(.show)) return .{ .session = .show };
    if (first.matchesKeywordTag(.discard)) return .{ .session = .discard_all };
    if (first.matchesKeywordTag(.begin)) return .{ .transaction = .begin };
    if (first.matchesKeywordTag(.commit)) return .{ .transaction = .commit };
    if (first.matchesKeywordTag(.rollback)) return .{ .transaction = .rollback };
    if (first.matchesKeywordTag(.prepare)) return .{ .prepared = .prepare };
    if (first.matchesKeywordTag(.execute)) return .{ .prepared = .execute };
    if (first.matchesKeywordTag(.deallocate)) return .{ .prepared = .deallocate };
    if (first.matchesKeywordTag(.create) and tokens.len > 1) {
        const second = tokens[1];
        if (second.matchesKeywordTag(.database)) return .{ .ddl = .create_database };
        if (second.matchesKeywordTag(.schema)) return .{ .ddl = .create_schema };
        if (second.matchesKeywordTag(.table)) return .{ .ddl = .create_table };
        if (second.matchesKeywordTag(.index)) return .{ .ddl = .create_index };
        if (second.matchesKeywordTag(.graph) and tokens.len > 2) {
            if (tokens[2].matchesKeywordTag(.index)) return .{ .graph = .create_index };
            if (tokens[2].matchesKeywordTag(.metric)) return .{ .graph = .create_metric };
        }
        if (second.matchesKeywordTag(.extension)) return .{ .ddl = .create_extension };
    }
    if (first.matchesKeywordTag(.alter) and tokens.len > 1 and tokens[1].matchesKeywordTag(.table)) {
        return .{ .ddl = .alter_table };
    }
    if (first.matchesKeywordTag(.drop) and tokens.len > 1) {
        const second = tokens[1];
        if (second.matchesKeywordTag(.table)) return .{ .ddl = .drop_table };
        if (second.matchesKeywordTag(.index)) return .{ .ddl = .drop_index };
        if (second.matchesKeywordTag(.schema)) return .{ .ddl = .drop_schema };
        if (second.matchesKeywordTag(.database)) return .{ .ddl = .drop_database };
        if (second.matchesKeywordTag(.extension)) return .{ .ddl = .drop_extension };
    }
    if (first.matchesKeywordTag(.insert)) {
        for (tokens) |token| {
            if (token.matchesKeywordTag(.select)) return .{ .dml = .insert_select };
        }
        return .{ .dml = .insert_values };
    }
    if (first.matchesKeywordTag(.update)) return .{ .dml = .update };
    if (first.matchesKeywordTag(.delete)) return .{ .dml = .delete };
    if (first.matchesKeywordTag(.truncate)) return .{ .dml = .truncate };
    if (first.matchesKeywordTag(.merge)) return .{ .dml = .merge };
    if (first.matchesKeywordTag(.select) or first.matchesKeywordTag(.with)) {
        return .{ .read = classifyReadKind(tokens) };
    }
    if (first.matchesKeywordTag(.analyze)) return .{ .unsupported = .analyze };
    if (first.matchesKeywordTag(.explain)) return .{ .unsupported = .explain };
    return .other;
}

fn classifyReadKind(tokens: []const token_mod.Token) GeneratedSqlReadKind {
    if (tokens.len > 0 and tokens[0].matchesKeywordTag(.with)) return .cte;
    if (firstTopLevelSetOperation(tokens, 1, statementTokenEnd(tokens)) != null) return .set_operation;
    for (tokens) |token| {
        if (token.matchesKeywordTag(.lateral)) return .lateral;
    }
    for (tokens) |token| {
        if (token.matchesKeywordTag(.over)) return .window;
    }
    if (tokens.len > 1 and tokens[0].matchesKeywordTag(.select) and tokens[1].matchesKeywordTag(.distinct)) {
        if (tokens.len > 2 and tokens[2].matchesKeywordTag(.on)) return .query;
        return .aggregate;
    }
    for (tokens) |token| {
        if (token.matchesKeywordTag(.join)) return .join;
    }
    for (tokens) |token| {
        if (token.matchesKeywordTag(.group) or token.matchesKeywordTag(.having)) return .aggregate;
    }
    return .query;
}

fn buildUnsupportedAst(
    tokens: []const token_mod.Token,
    end: usize,
    kind: GeneratedSqlUnsupportedKind,
    statement_span: token_mod.SourceSpan,
    command_span: token_mod.SourceSpan,
) GeneratedSqlUnsupportedAst {
    _ = tokens;
    var ast = GeneratedSqlUnsupportedAst{
        .kind = kind,
        .reason = switch (kind) {
            .analyze => .analyze_not_planned_by_generated_parser,
            .explain => .explain_not_planned_by_generated_parser,
        },
        .statement_span = statement_span,
        .command_span = command_span,
    };
    if (kind == .explain and end > 1) ast.subject_tokens = .{ .start = 1, .end = end };
    return ast;
}

fn buildGeneratedAst(alloc: std.mem.Allocator, tokens: []const token_mod.Token, statement: GeneratedSqlStatement) !?GeneratedSqlAst {
    const end = statementTokenEnd(tokens);
    if (end == 0) return null;
    const statement_span = sourceSpanForTokenRange(tokens, .{ .start = 0, .end = end }) orelse return null;
    const command_span = tokens[0].sourceSpan();
    return switch (statement) {
        .session => |kind| .{ .session = buildSessionAst(tokens, end, kind, statement_span, command_span) },
        .transaction => |kind| .{ .transaction = .{
            .kind = kind,
            .statement_span = statement_span,
            .command_span = command_span,
        } },
        .prepared => |kind| .{ .prepared = buildPreparedAst(tokens, end, kind, statement_span, command_span) },
        .ddl => |kind| .{ .ddl = buildDdlAst(tokens, end, kind, statement_span, command_span) },
        .dml => |kind| .{ .dml = buildDmlAst(tokens, end, kind, statement_span, command_span) },
        .read => |kind| .{ .read = try buildReadAst(alloc, tokens, end, kind, statement_span, command_span) },
        .graph => |kind| .{ .graph = .{
            .kind = kind,
            .statement_span = statement_span,
            .command_span = command_span,
        } },
        .unsupported => |kind| .{ .unsupported = buildUnsupportedAst(tokens, end, kind, statement_span, command_span) },
        else => null,
    };
}

fn buildSessionAst(
    tokens: []const token_mod.Token,
    end: usize,
    kind: GeneratedSqlSessionKind,
    statement_span: token_mod.SourceSpan,
    command_span: token_mod.SourceSpan,
) GeneratedSqlSessionAst {
    var ast = GeneratedSqlSessionAst{
        .kind = kind,
        .statement_span = statement_span,
        .command_span = command_span,
    };
    switch (kind) {
        .set => {
            const value_start = findSetValueStart(tokens, end) orelse end;
            if (value_start > 1) ast.name_tokens = .{ .start = 1, .end = value_start - 1 };
            if (value_start < end) ast.value_tokens = .{ .start = value_start, .end = end };
        },
        .reset, .show => {
            if (end > 1) ast.name_tokens = .{ .start = 1, .end = end };
        },
        .discard_all => {},
    }
    return ast;
}

fn findSetValueStart(tokens: []const token_mod.Token, end: usize) ?usize {
    var index: usize = 1;
    while (index < end) : (index += 1) {
        if (tokens[index].kind == .eq or tokens[index].matchesKeywordTag(.to)) return index + 1;
    }
    return if (end > 2) 2 else null;
}

fn buildPreparedAst(
    tokens: []const token_mod.Token,
    end: usize,
    kind: GeneratedSqlPreparedKind,
    statement_span: token_mod.SourceSpan,
    command_span: token_mod.SourceSpan,
) GeneratedSqlPreparedAst {
    var ast = GeneratedSqlPreparedAst{
        .kind = kind,
        .statement_span = statement_span,
        .command_span = command_span,
    };
    switch (kind) {
        .prepare => {
            if (end > 1) ast.name_tokens = .{ .start = 1, .end = 2 };
            if (findKeyword(tokens, 2, end, .as)) |as_index| {
                if (as_index > 2) ast.parameter_tokens = .{ .start = 2, .end = as_index };
                if (as_index + 1 < end) ast.inner_statement_tokens = .{ .start = as_index + 1, .end = end };
            }
        },
        .execute => {
            if (end > 1) ast.name_tokens = .{ .start = 1, .end = 2 };
            if (end > 2) ast.argument_tokens = .{ .start = 2, .end = end };
        },
        .deallocate => {
            if (end > 1) ast.name_tokens = .{ .start = 1, .end = end };
        },
    }
    return ast;
}

fn buildDdlAst(
    tokens: []const token_mod.Token,
    end: usize,
    kind: GeneratedSqlDdlKind,
    statement_span: token_mod.SourceSpan,
    command_span: token_mod.SourceSpan,
) GeneratedSqlDdlAst {
    var ast = GeneratedSqlDdlAst{
        .kind = kind,
        .statement_span = statement_span,
        .command_span = command_span,
    };
    var index: usize = 2;
    switch (kind) {
        .create_database => {
            ast.object_name_tokens = generatedSingleTokenRangeIfIdentifier(tokens, index, end);
        },
        .create_schema => {
            ast.if_not_exists = consumeGeneratedIfNotExists(tokens, &index, end);
            ast.object_name_tokens = generatedSingleTokenRangeIfIdentifier(tokens, index, end);
        },
        .create_extension => {
            ast.if_not_exists = consumeGeneratedIfNotExists(tokens, &index, end);
            ast.object_name_tokens = generatedSingleTokenRangeIfIdentifier(tokens, index, end);
            if (ast.object_name_tokens) |_| index += 1;
            if (index < end and tokens[index].matchesKeywordTag(.with)) index += 1;
            if (index + 1 < end and tokens[index].matchesKeywordTag(.schema)) {
                ast.schema_name_tokens = generatedSingleTokenRangeIfIdentifier(tokens, index + 1, end);
                index += 2;
            }
            if (index + 1 < end and tokens[index].matchesKeyword("version") and tokens[index + 1].kind == .string) {
                ast.version_tokens = .{ .start = index + 1, .end = index + 2 };
            }
        },
        .drop_database => {
            ast.if_exists = consumeGeneratedIfExists(tokens, &index, end);
            ast.object_name_tokens = generatedSingleTokenRangeIfIdentifier(tokens, index, end);
            if (findKeywordText(tokens, index + 1, end, "force") != null) ast.force = true;
        },
        .drop_schema => {
            ast.if_exists = consumeGeneratedIfExists(tokens, &index, end);
            ast.object_name_tokens = generatedSingleTokenRangeIfIdentifier(tokens, index, end);
            ast.cascade = findKeyword(tokens, index + 1, end, .cascade) != null;
        },
        .drop_extension => {
            ast.if_exists = consumeGeneratedIfExists(tokens, &index, end);
            ast.object_name_tokens = generatedSingleTokenRangeIfIdentifier(tokens, index, end);
            ast.cascade = findKeyword(tokens, index + 1, end, .cascade) != null;
        },
        else => {},
    }
    return ast;
}

fn buildDmlAst(
    tokens: []const token_mod.Token,
    end: usize,
    kind: GeneratedSqlDmlKind,
    statement_span: token_mod.SourceSpan,
    command_span: token_mod.SourceSpan,
) GeneratedSqlDmlAst {
    var ast = GeneratedSqlDmlAst{
        .kind = kind,
        .statement_span = statement_span,
        .command_span = command_span,
    };
    switch (kind) {
        .insert_values, .insert_select => buildInsertDmlAst(tokens, end, &ast),
        .update => buildUpdateDmlAst(tokens, end, &ast),
        .delete => buildDeleteDmlAst(tokens, end, &ast),
        .truncate => buildTruncateDmlAst(tokens, end, &ast),
        .merge => buildMergeDmlAst(tokens, end, &ast),
    }
    return ast;
}

fn buildReadAst(
    alloc: std.mem.Allocator,
    tokens: []const token_mod.Token,
    end: usize,
    kind: GeneratedSqlReadKind,
    statement_span: token_mod.SourceSpan,
    command_span: token_mod.SourceSpan,
) !GeneratedSqlReadAst {
    var ast = GeneratedSqlReadAst{
        .kind = kind,
        .statement_span = statement_span,
        .command_span = command_span,
    };
    errdefer ast.deinit(alloc);
    const select_index = findTopLevelKeyword(tokens, 0, end, .select) orelse return ast;
    if (select_index > 0 and tokens[0].matchesKeywordTag(.with)) {
        ast.cte_tokens = .{ .start = 1, .end = select_index };
        try buildReadCteAst(alloc, tokens, select_index, &ast);
    }

    const body_end = firstTopLevelSetOperation(tokens, select_index + 1, end) orelse end;
    if (body_end < end) ast.set_operation_tokens = .{ .start = body_end, .end = end };

    const projection_start = generatedReadProjectionStart(tokens, select_index, body_end, &ast);
    const from_index = findTopLevelKeyword(tokens, projection_start, body_end, .from);
    const where_index = findTopLevelKeyword(tokens, projection_start, body_end, .where);
    const group_index = findTopLevelKeyword(tokens, projection_start, body_end, .group);
    const having_index = findTopLevelKeyword(tokens, projection_start, body_end, .having);
    const window_index = findTopLevelKeyword(tokens, projection_start, body_end, .window);
    const order_index = findTopLevelKeyword(tokens, projection_start, body_end, .order);
    const limit_index = findTopLevelKeyword(tokens, projection_start, body_end, .limit);
    const offset_index = findTopLevelKeyword(tokens, projection_start, body_end, .offset);
    const fetch_index = findTopLevelKeyword(tokens, projection_start, body_end, .fetch);

    const projection_end = firstOptionalIndex(&[_]?usize{ from_index, where_index, group_index, having_index, window_index, order_index, limit_index, offset_index, fetch_index }) orelse body_end;
    if (projection_start < projection_end) {
        const projection_tokens = GeneratedSqlTokenRange{ .start = projection_start, .end = projection_end };
        ast.projection_tokens = projection_tokens;
        ast.projection_items = try buildTopLevelListAst(alloc, tokens, projection_tokens);
        if (ast.projection_items.first_tokens) |first_tokens| {
            ast.projection_first_expression = try buildGeneratedExpressionAst(alloc, tokens, first_tokens);
        }
        if (ast.projection_items.last_tokens) |last_tokens| {
            ast.projection_last_expression = try buildGeneratedExpressionAst(alloc, tokens, last_tokens);
        }
    }

    if (from_index) |idx| {
        const source_end = firstOptionalIndex(&[_]?usize{ where_index, group_index, having_index, window_index, order_index, limit_index, offset_index, fetch_index }) orelse body_end;
        if (idx + 1 < source_end) {
            const source_tokens = GeneratedSqlTokenRange{ .start = idx + 1, .end = source_end };
            ast.source_tokens = source_tokens;
            try buildReadJoinAst(alloc, tokens, source_tokens, &ast);
        }
    }
    if (where_index) |idx| {
        const where_end = firstOptionalIndex(&[_]?usize{ group_index, having_index, window_index, order_index, limit_index, offset_index, fetch_index }) orelse body_end;
        if (idx + 1 < where_end) {
            const where_tokens = GeneratedSqlTokenRange{ .start = idx + 1, .end = where_end };
            ast.where_tokens = where_tokens;
            ast.where_expression = try buildGeneratedExpressionAst(alloc, tokens, where_tokens);
        }
    }
    if (group_index) |idx| {
        const group_start = if (idx + 1 < body_end and tokens[idx + 1].matchesKeywordTag(.by)) idx + 2 else idx + 1;
        const group_end = firstOptionalIndex(&[_]?usize{ having_index, window_index, order_index, limit_index, offset_index, fetch_index }) orelse body_end;
        if (group_start < group_end) {
            const group_tokens = GeneratedSqlTokenRange{ .start = group_start, .end = group_end };
            ast.group_tokens = group_tokens;
            ast.group_items = try buildTopLevelListAst(alloc, tokens, group_tokens);
            if (ast.group_items.first_tokens) |first_tokens| {
                ast.group_first_expression = try buildGeneratedExpressionAst(alloc, tokens, first_tokens);
            }
            if (ast.group_items.last_tokens) |last_tokens| {
                ast.group_last_expression = try buildGeneratedExpressionAst(alloc, tokens, last_tokens);
            }
        }
    }
    if (having_index) |idx| {
        const having_end = firstOptionalIndex(&[_]?usize{ window_index, order_index, limit_index, offset_index, fetch_index }) orelse body_end;
        if (idx + 1 < having_end) {
            const having_tokens = GeneratedSqlTokenRange{ .start = idx + 1, .end = having_end };
            ast.having_tokens = having_tokens;
            ast.having_expression = try buildGeneratedExpressionAst(alloc, tokens, having_tokens);
        }
    }
    if (window_index) |idx| {
        const window_end = firstOptionalIndex(&[_]?usize{ order_index, limit_index, offset_index, fetch_index }) orelse body_end;
        if (idx + 1 < window_end) ast.window_tokens = .{ .start = idx + 1, .end = window_end };
    }
    if (order_index) |idx| {
        const order_start = if (idx + 1 < body_end and tokens[idx + 1].matchesKeywordTag(.by)) idx + 2 else idx + 1;
        const order_end = firstOptionalIndex(&[_]?usize{ limit_index, offset_index, fetch_index }) orelse body_end;
        if (order_start < order_end) {
            const order_tokens = GeneratedSqlTokenRange{ .start = order_start, .end = order_end };
            ast.order_tokens = order_tokens;
            ast.order_items = try buildTopLevelListAst(alloc, tokens, order_tokens);
            if (ast.order_items.first_tokens) |first_tokens| {
                ast.order_first_expression = try buildGeneratedExpressionAst(alloc, tokens, first_tokens);
            }
            if (ast.order_items.last_tokens) |last_tokens| {
                ast.order_last_expression = try buildGeneratedExpressionAst(alloc, tokens, last_tokens);
            }
        }
    }
    if (limit_index) |idx| {
        const limit_end = firstOptionalIndex(&[_]?usize{ offset_index, fetch_index }) orelse body_end;
        if (idx + 1 < limit_end) {
            const limit_tokens = GeneratedSqlTokenRange{ .start = idx + 1, .end = limit_end };
            ast.limit_tokens = limit_tokens;
            if (limit_tokens.end == limit_tokens.start + 1 and tokens[limit_tokens.start].matchesKeywordTag(.all)) {
                ast.limit_all = true;
            } else {
                ast.limit_expression = try buildGeneratedExpressionAst(alloc, tokens, limit_tokens);
            }
        }
    }
    if (offset_index) |idx| {
        const offset_end = firstOptionalIndex(&[_]?usize{fetch_index}) orelse body_end;
        if (idx + 1 < offset_end) {
            const offset_tokens = GeneratedSqlTokenRange{ .start = idx + 1, .end = offset_end };
            ast.offset_tokens = offset_tokens;
            if (generatedOffsetExpressionTokens(tokens, offset_tokens)) |expression_tokens| {
                ast.offset_expression = try buildGeneratedExpressionAst(alloc, tokens, expression_tokens);
            }
        }
    }
    if (fetch_index) |idx| {
        if (idx + 1 < body_end) {
            const fetch_tokens = GeneratedSqlTokenRange{ .start = idx + 1, .end = body_end };
            ast.fetch_tokens = fetch_tokens;
            if (generatedFetchCountTokens(tokens, fetch_tokens)) |count_tokens| {
                ast.fetch_count_tokens = count_tokens;
                ast.fetch_count_expression = try buildGeneratedExpressionAst(alloc, tokens, count_tokens);
            }
        }
    }

    return ast;
}

fn generatedOffsetExpressionTokens(tokens: []const token_mod.Token, range: GeneratedSqlTokenRange) ?GeneratedSqlTokenRange {
    if (range.start >= range.end or range.end > tokens.len) return null;
    var end = range.end;
    if (end > range.start and (tokens[end - 1].matchesKeywordTag(.row) or tokens[end - 1].matchesKeywordTag(.rows))) {
        end -= 1;
    }
    if (range.start >= end) return null;
    return .{ .start = range.start, .end = end };
}

fn generatedFetchCountTokens(tokens: []const token_mod.Token, range: GeneratedSqlTokenRange) ?GeneratedSqlTokenRange {
    if (range.start + 2 > range.end or range.end > tokens.len) return null;
    if (!tokens[range.start].matchesKeywordTag(.first) and !tokens[range.start].matchesKeywordTag(.next)) return null;
    var end = range.end;
    if (end > range.start and tokens[end - 1].matchesKeywordTag(.only)) end -= 1;
    if (end > range.start and (tokens[end - 1].matchesKeywordTag(.row) or tokens[end - 1].matchesKeywordTag(.rows))) {
        end -= 1;
    }
    const start = range.start + 1;
    if (start >= end) return null;
    return .{ .start = start, .end = end };
}

fn generatedReadProjectionStart(
    tokens: []const token_mod.Token,
    select_index: usize,
    body_end: usize,
    ast: *GeneratedSqlReadAst,
) usize {
    const distinct_index = select_index + 1;
    if (distinct_index >= body_end or !tokens[distinct_index].matchesKeywordTag(.distinct)) return distinct_index;
    if (distinct_index + 2 < body_end and tokens[distinct_index + 1].matchesKeywordTag(.on) and tokens[distinct_index + 2].kind == .lparen) {
        if (findMatchingParen(tokens, distinct_index + 2, body_end)) |close| {
            ast.distinct_tokens = .{ .start = distinct_index, .end = close + 1 };
            return close + 1;
        }
    }
    ast.distinct_tokens = .{ .start = distinct_index, .end = distinct_index + 1 };
    return distinct_index + 1;
}

fn buildReadCteAst(alloc: std.mem.Allocator, tokens: []const token_mod.Token, final_select_index: usize, ast: *GeneratedSqlReadAst) !void {
    if (final_select_index < 5 or !tokens[0].matchesKeywordTag(.with)) return;
    var index: usize = 1;
    if (index < final_select_index and tokens[index].matchesKeywordTag(.recursive)) {
        ast.cte_recursive = true;
        index += 1;
    }
    if (index >= final_select_index) return;
    ast.cte_list_tokens = .{ .start = index, .end = final_select_index };

    var items: std.ArrayListUnmanaged(GeneratedSqlCteAst) = .empty;
    var items_owned = false;
    defer if (!items_owned) items.deinit(alloc);
    var count: usize = 0;
    while (index < final_select_index) {
        if (tokens[index].kind != .identifier) return;
        if (index + 2 >= final_select_index) return;
        if (!tokens[index + 1].matchesKeywordTag(.as) or tokens[index + 2].kind != .lparen) return;
        const close = findMatchingParen(tokens, index + 2, final_select_index) orelse return;
        if (close >= final_select_index) return;

        const name_tokens = GeneratedSqlTokenRange{ .start = index, .end = index + 1 };
        const body_tokens: ?GeneratedSqlTokenRange = if (index + 3 < close)
            .{ .start = index + 3, .end = close }
        else
            null;

        try items.append(alloc, .{
            .name_tokens = name_tokens,
            .body_tokens = body_tokens,
        });
        count += 1;
        if (count == 1) {
            ast.cte_name_tokens = name_tokens;
            ast.cte_body_tokens = body_tokens;
        }
        ast.cte_last_name_tokens = name_tokens;
        ast.cte_last_body_tokens = body_tokens;

        index = close + 1;
        if (index == final_select_index) break;
        if (tokens[index].kind != .comma) return;
        index += 1;
    }
    ast.cte_items = try items.toOwnedSlice(alloc);
    items_owned = true;
    ast.cte_count = count;
}

fn buildReadJoinAst(alloc: std.mem.Allocator, tokens: []const token_mod.Token, source_tokens: GeneratedSqlTokenRange, ast: *GeneratedSqlReadAst) !void {
    var items: std.ArrayListUnmanaged(GeneratedSqlJoinAst) = .empty;
    var items_owned = false;
    defer if (!items_owned) {
        for (items.items) |*item| item.deinit(alloc);
        items.deinit(alloc);
    };

    var scan = source_tokens.start;
    while (findTopLevelKeyword(tokens, scan, source_tokens.end, .join)) |join_index| {
        const on_index = findTopLevelKeyword(tokens, join_index + 1, source_tokens.end, .on) orelse return;
        const operator = generatedJoinOperator(tokens, source_tokens, join_index) orelse return;
        if (source_tokens.start >= operator.tokens.start or join_index + 1 >= on_index) return;

        const next_join_index = findTopLevelKeyword(tokens, on_index + 1, source_tokens.end, .join);
        const predicate_end = if (next_join_index) |next_join|
            (generatedJoinOperator(tokens, source_tokens, next_join) orelse return).tokens.start
        else
            source_tokens.end;
        if (on_index + 1 >= predicate_end) return;

        try items.ensureUnusedCapacity(alloc, 1);
        const predicate_tokens = GeneratedSqlTokenRange{ .start = on_index + 1, .end = predicate_end };
        const item = GeneratedSqlJoinAst{
            .tokens = .{ .start = source_tokens.start, .end = predicate_end },
            .operator_tokens = operator.tokens,
            .kind = operator.kind,
            .left_tokens = .{ .start = source_tokens.start, .end = operator.tokens.start },
            .right_tokens = .{ .start = join_index + 1, .end = on_index },
            .predicate_tokens = predicate_tokens,
            .predicate_expression = try buildGeneratedExpressionAst(alloc, tokens, predicate_tokens),
        };
        items.appendAssumeCapacity(item);

        scan = predicate_end;
        if (scan >= source_tokens.end) break;
    }

    if (items.items.len == 0) return;

    ast.join_items = try items.toOwnedSlice(alloc);
    items_owned = true;

    const first = ast.join_items[0];
    ast.join_tokens = source_tokens;
    ast.join_operator_tokens = first.operator_tokens;
    ast.join_kind = first.kind;
    ast.join_left_tokens = first.left_tokens;
    ast.join_right_tokens = first.right_tokens;
    ast.join_predicate_tokens = first.predicate_tokens;
    ast.join_predicate_expression = try buildGeneratedExpressionAst(alloc, tokens, first.predicate_tokens);
}

const GeneratedJoinOperator = struct {
    tokens: GeneratedSqlTokenRange,
    kind: GeneratedSqlJoinKind,
};

fn generatedJoinOperator(tokens: []const token_mod.Token, source_tokens: GeneratedSqlTokenRange, join_index: usize) ?GeneratedJoinOperator {
    if (join_index >= source_tokens.end or !tokens[join_index].matchesKeywordTag(.join)) return null;
    if (join_index >= source_tokens.start + 2 and tokens[join_index - 1].matchesKeywordTag(.outer)) {
        if (tokens[join_index - 2].matchesKeywordTag(.left)) return .{
            .tokens = .{ .start = join_index - 2, .end = join_index + 1 },
            .kind = .left,
        };
        if (tokens[join_index - 2].matchesKeywordTag(.right)) return .{
            .tokens = .{ .start = join_index - 2, .end = join_index + 1 },
            .kind = .right,
        };
        if (tokens[join_index - 2].matchesKeywordTag(.full)) return .{
            .tokens = .{ .start = join_index - 2, .end = join_index + 1 },
            .kind = .full,
        };
    }
    if (join_index >= source_tokens.start + 1) {
        if (tokens[join_index - 1].matchesKeywordTag(.inner)) return .{
            .tokens = .{ .start = join_index - 1, .end = join_index + 1 },
            .kind = .inner,
        };
        if (tokens[join_index - 1].matchesKeywordTag(.left)) return .{
            .tokens = .{ .start = join_index - 1, .end = join_index + 1 },
            .kind = .left,
        };
        if (tokens[join_index - 1].matchesKeywordTag(.right)) return .{
            .tokens = .{ .start = join_index - 1, .end = join_index + 1 },
            .kind = .right,
        };
        if (tokens[join_index - 1].matchesKeywordTag(.full)) return .{
            .tokens = .{ .start = join_index - 1, .end = join_index + 1 },
            .kind = .full,
        };
    }
    return .{
        .tokens = .{ .start = join_index, .end = join_index + 1 },
        .kind = .inner,
    };
}

fn buildInsertDmlAst(tokens: []const token_mod.Token, end: usize, ast: *GeneratedSqlDmlAst) void {
    if (end < 4 or !tokens[1].matchesKeywordTag(.into)) return;
    ast.target_table_tokens = generatedSingleTokenRangeIfIdentifier(tokens, 2, end);
    var index: usize = 3;
    if (index + 1 < end and tokens[index].matchesKeywordTag(.default) and tokens[index + 1].matchesKeywordTag(.values)) {
        ast.default_values = true;
        const conflict_index = findTopLevelKeywordSequence(tokens, index + 2, end, .on, .conflict);
        const returning_index = findTopLevelKeyword(tokens, index + 2, end, .returning) orelse end;
        if (conflict_index) |idx| {
            const conflict_end = if (returning_index < end) returning_index else end;
            if (idx + 1 < conflict_end) ast.conflict_tokens = .{ .start = idx + 1, .end = conflict_end };
        }
        if (returning_index < end) ast.returning_tokens = .{ .start = returning_index + 1, .end = end };
        return;
    }
    if (index < end and tokens[index].kind == .lparen) {
        if (findMatchingParen(tokens, index, end)) |close| {
            ast.insert_columns_tokens = .{ .start = index, .end = close + 1 };
            index = close + 1;
        }
    }
    if (findTopLevelKeyword(tokens, index, end, .values)) |values_index| {
        const conflict_index = findTopLevelKeywordSequence(tokens, values_index + 1, end, .on, .conflict);
        const returning_index = findTopLevelKeyword(tokens, values_index + 1, end, .returning) orelse end;
        const values_end = conflict_index orelse returning_index;
        ast.values_tokens = .{ .start = values_index + 1, .end = values_end };
        if (conflict_index) |idx| {
            const conflict_end = if (returning_index < end) returning_index else end;
            if (idx + 1 < conflict_end) ast.conflict_tokens = .{ .start = idx + 1, .end = conflict_end };
        }
        if (returning_index < end) ast.returning_tokens = .{ .start = returning_index + 1, .end = end };
    } else if (findTopLevelKeyword(tokens, index, end, .select)) |select_index| {
        const conflict_index = findTopLevelKeywordSequence(tokens, select_index + 1, end, .on, .conflict);
        const returning_index = findTopLevelKeyword(tokens, select_index + 1, end, .returning) orelse end;
        const source_end = conflict_index orelse returning_index;
        ast.source_tokens = .{ .start = select_index, .end = source_end };
        if (conflict_index) |idx| {
            const conflict_end = if (returning_index < end) returning_index else end;
            if (idx + 1 < conflict_end) ast.conflict_tokens = .{ .start = idx + 1, .end = conflict_end };
        }
        if (returning_index < end) ast.returning_tokens = .{ .start = returning_index + 1, .end = end };
    }
}

fn buildUpdateDmlAst(tokens: []const token_mod.Token, end: usize, ast: *GeneratedSqlDmlAst) void {
    ast.target_table_tokens = generatedSingleTokenRangeIfIdentifier(tokens, 1, end);
    const set_index = findTopLevelKeyword(tokens, 2, end, .set) orelse return;
    const from_index = findTopLevelKeyword(tokens, set_index + 1, end, .from);
    const where_index = findTopLevelKeyword(tokens, set_index + 1, end, .where);
    const returning_index = findTopLevelKeyword(tokens, set_index + 1, end, .returning);
    const assignments_end = minOptionalIndex(from_index, minOptionalIndex(where_index, returning_index) orelse end) orelse end;
    if (set_index + 1 < assignments_end) ast.assignments_tokens = .{ .start = set_index + 1, .end = assignments_end };
    if (from_index) |idx| {
        const source_end = minOptionalIndex(where_index, returning_index) orelse end;
        if (idx + 1 < source_end) ast.source_tokens = .{ .start = idx + 1, .end = source_end };
    }
    if (where_index) |idx| {
        const where_end = returning_index orelse end;
        if (idx + 1 < where_end) ast.where_tokens = .{ .start = idx + 1, .end = where_end };
    }
    if (returning_index) |idx| {
        if (idx + 1 < end) ast.returning_tokens = .{ .start = idx + 1, .end = end };
    }
}

fn buildDeleteDmlAst(tokens: []const token_mod.Token, end: usize, ast: *GeneratedSqlDmlAst) void {
    if (end < 3 or !tokens[1].matchesKeywordTag(.from)) return;
    ast.target_table_tokens = generatedSingleTokenRangeIfIdentifier(tokens, 2, end);
    const using_index = findTopLevelKeyword(tokens, 3, end, .using);
    const where_index = findTopLevelKeyword(tokens, 3, end, .where);
    const returning_index = findTopLevelKeyword(tokens, 3, end, .returning);
    if (using_index) |idx| {
        const source_end = minOptionalIndex(where_index, returning_index) orelse end;
        if (idx + 1 < source_end) ast.source_tokens = .{ .start = idx + 1, .end = source_end };
    }
    if (where_index) |idx| {
        const where_end = returning_index orelse end;
        if (idx + 1 < where_end) ast.where_tokens = .{ .start = idx + 1, .end = where_end };
    }
    if (returning_index) |idx| {
        if (idx + 1 < end) ast.returning_tokens = .{ .start = idx + 1, .end = end };
    }
}

fn buildTruncateDmlAst(tokens: []const token_mod.Token, end: usize, ast: *GeneratedSqlDmlAst) void {
    var index: usize = 1;
    if (index < end and tokens[index].matchesKeywordTag(.table)) index += 1;
    ast.target_table_tokens = generatedSingleTokenRangeIfIdentifier(tokens, index, end);
    if (ast.target_table_tokens) |target| index = target.end;

    const option_index = firstTopLevelTruncateOption(tokens, index, end) orelse end;
    if (index < option_index and tokens[index].kind == .comma) {
        ast.additional_target_tokens = .{ .start = index, .end = option_index };
    }
    ast.restart_identity = generatedTruncateHasRestartIdentity(tokens, option_index, end);
    ast.cascade = findTopLevelKeyword(tokens, option_index, end, .cascade) != null;
}

fn buildMergeDmlAst(tokens: []const token_mod.Token, end: usize, ast: *GeneratedSqlDmlAst) void {
    if (end < 4 or !tokens[1].matchesKeywordTag(.into)) return;
    ast.target_table_tokens = generatedSingleTokenRangeIfIdentifier(tokens, 2, end);
    if (findTopLevelKeyword(tokens, 3, end, .using)) |using_index| {
        const on_index = findTopLevelKeyword(tokens, using_index + 1, end, .on) orelse end;
        if (using_index + 1 < on_index) ast.source_tokens = .{ .start = using_index + 1, .end = on_index };
        if (on_index + 1 < end) ast.where_tokens = .{ .start = on_index + 1, .end = end };
    }
}

fn firstTopLevelTruncateOption(tokens: []const token_mod.Token, start: usize, end: usize) ?usize {
    var best: ?usize = null;
    const candidates = [_]token_mod.TokenKeyword{ .restart, .@"continue", .identity, .cascade, .restrict };
    for (candidates) |keyword| {
        if (findTopLevelKeyword(tokens, start, end, keyword)) |idx| {
            if (best == null or idx < best.?) best = idx;
        }
    }
    return best;
}

fn generatedTruncateHasRestartIdentity(tokens: []const token_mod.Token, start: usize, end: usize) bool {
    return start + 1 < end and
        tokens[start].matchesKeywordTag(.restart) and
        tokens[start + 1].matchesKeywordTag(.identity);
}

fn firstTopLevelSetOperation(tokens: []const token_mod.Token, start: usize, end: usize) ?usize {
    var best: ?usize = null;
    const candidates = [_]token_mod.TokenKeyword{ .@"union", .intersect, .except };
    for (candidates) |keyword| {
        if (findTopLevelKeyword(tokens, start, end, keyword)) |idx| {
            if (best == null or idx < best.?) best = idx;
        }
    }
    return best;
}

fn consumeGeneratedIfNotExists(tokens: []const token_mod.Token, index: *usize, end: usize) bool {
    if (index.* + 2 >= end) return false;
    if (!tokens[index.*].matchesKeywordTag(.@"if") or
        !tokens[index.* + 1].matchesKeywordTag(.not) or
        !tokens[index.* + 2].matchesKeywordTag(.exists))
    {
        return false;
    }
    index.* += 3;
    return true;
}

fn consumeGeneratedIfExists(tokens: []const token_mod.Token, index: *usize, end: usize) bool {
    if (index.* + 1 >= end) return false;
    if (!tokens[index.*].matchesKeywordTag(.@"if") or
        !tokens[index.* + 1].matchesKeywordTag(.exists))
    {
        return false;
    }
    index.* += 2;
    return true;
}

fn generatedSingleTokenRangeIfIdentifier(tokens: []const token_mod.Token, index: usize, end: usize) ?GeneratedSqlTokenRange {
    if (index >= end or tokens[index].kind != .identifier) return null;
    return .{ .start = index, .end = index + 1 };
}

fn buildTopLevelListAst(alloc: std.mem.Allocator, tokens: []const token_mod.Token, range: GeneratedSqlTokenRange) !GeneratedSqlListAst {
    var ast = GeneratedSqlListAst{};
    if (range.start >= range.end or range.end > tokens.len) return ast;

    var items: std.ArrayListUnmanaged(GeneratedSqlTokenRange) = .empty;
    errdefer items.deinit(alloc);
    var item_start = range.start;
    var depth: usize = 0;
    var index = range.start;
    while (index < range.end) : (index += 1) {
        switch (tokens[index].kind) {
            .lparen, .lbracket => depth += 1,
            .rparen, .rbracket => {
                if (depth == 0) {
                    items.deinit(alloc);
                    return ast;
                }
                depth -= 1;
            },
            .comma => if (depth == 0) {
                try recordGeneratedListItem(alloc, &items, &ast, .{ .start = item_start, .end = index });
                item_start = index + 1;
            },
            else => {},
        }
    }
    try recordGeneratedListItem(alloc, &items, &ast, .{ .start = item_start, .end = range.end });
    ast.items = try items.toOwnedSlice(alloc);
    errdefer {
        alloc.free(ast.items);
        ast.items = &.{};
    }
    if (ast.items.len > 0) {
        ast.expressions = try alloc.alloc(GeneratedSqlExpressionAst, ast.items.len);
        var expression_count: usize = 0;
        errdefer {
            for (ast.expressions[0..expression_count]) |*expression| expression.deinit(alloc);
            alloc.free(ast.expressions);
            ast.expressions = &.{};
        }
        for (ast.items) |item| {
            ast.expressions[expression_count] = try buildGeneratedExpressionAst(alloc, tokens, item);
            expression_count += 1;
        }
    }
    return ast;
}

fn recordGeneratedListItem(
    alloc: std.mem.Allocator,
    items: *std.ArrayListUnmanaged(GeneratedSqlTokenRange),
    ast: *GeneratedSqlListAst,
    range: GeneratedSqlTokenRange,
) !void {
    if (range.start >= range.end) return;
    try items.append(alloc, range);
    if (ast.count == 0) ast.first_tokens = range;
    ast.last_tokens = range;
    ast.count += 1;
}

fn buildGeneratedExpressionAst(alloc: std.mem.Allocator, tokens: []const token_mod.Token, range: GeneratedSqlTokenRange) anyerror!GeneratedSqlExpressionAst {
    var ast = GeneratedSqlExpressionAst{ .tokens = range };
    errdefer ast.deinit(alloc);
    if (generatedWrappedExpressionInnerRange(tokens, range)) |inner_range| {
        ast.kind = .grouped;
        ast.inner_tokens = inner_range;
        ast.inner_expression_kind = generatedExpressionKindForRange(tokens, inner_range);
        ast.inner_expression = try buildGeneratedExpressionNodeAlloc(alloc, tokens, inner_range);
        return ast;
    }
    if (generatedFunctionCallExpression(tokens, range)) |function_call| {
        ast.kind = .function_call;
        ast.function_name_tokens = function_call.name_tokens;
        ast.argument_tokens = function_call.argument_tokens;
        if (function_call.argument_tokens) |argument_tokens| {
            ast.argument_items = try buildTopLevelListAst(alloc, tokens, argument_tokens);
        }
        return ast;
    }
    const operator = findTopLevelExpressionOperator(tokens, range) orelse return ast;
    ast.kind = operator.kind;
    if (!operator.prefix) {
        if (range.start >= operator.index or operator.index + 1 >= range.end) return ast;
        const left_end = if (operator.negation_index) |negation_index|
            if (negation_index < operator.index) negation_index else operator.index
        else
            operator.index;
        if (range.start >= left_end) return ast;
        ast.left_tokens = .{ .start = range.start, .end = left_end };
        ast.left_expression_kind = generatedExpressionKindForRange(tokens, ast.left_tokens.?);
        ast.left_expression = try buildGeneratedExpressionNodeAlloc(alloc, tokens, ast.left_tokens.?);
    } else if (operator.index + 1 >= range.end) {
        return ast;
    }
    if (operator.negation_index) |negation_index| ast.negation_tokens = .{ .start = negation_index, .end = negation_index + 1 };
    const operator_end = operator.operator_end_index orelse operator.index + 1;
    ast.operator_tokens = .{ .start = operator.index, .end = operator_end };
    const right_start = if (operator.quantifier_index) |quantifier_index| blk: {
        ast.quantifier_tokens = .{ .start = quantifier_index, .end = quantifier_index + 1 };
        break :blk quantifier_index + 1;
    } else operator_end;
    if (right_start >= range.end) return ast;
    ast.right_tokens = .{ .start = right_start, .end = range.end };
    ast.right_expression_kind = generatedExpressionKindForRange(tokens, ast.right_tokens.?);
    ast.right_expression = try buildGeneratedExpressionNodeAlloc(alloc, tokens, ast.right_tokens.?);
    return ast;
}

fn buildGeneratedExpressionNodeAlloc(
    alloc: std.mem.Allocator,
    tokens: []const token_mod.Token,
    range: GeneratedSqlTokenRange,
) !*GeneratedSqlExpressionAst {
    const node = try alloc.create(GeneratedSqlExpressionAst);
    errdefer alloc.destroy(node);
    node.* = try buildGeneratedExpressionAst(alloc, tokens, range);
    return node;
}

fn generatedExpressionKindForRange(tokens: []const token_mod.Token, range: GeneratedSqlTokenRange) ?GeneratedSqlExpressionKind {
    if (generatedWrappedExpressionInnerRange(tokens, range) != null) return .grouped;
    if (generatedFunctionCallExpression(tokens, range) != null) return .function_call;
    return if (findTopLevelExpressionOperator(tokens, range)) |operator| operator.kind else null;
}

const GeneratedFunctionCallExpression = struct {
    name_tokens: GeneratedSqlTokenRange,
    argument_tokens: ?GeneratedSqlTokenRange = null,
};

fn generatedFunctionCallExpression(tokens: []const token_mod.Token, range: GeneratedSqlTokenRange) ?GeneratedFunctionCallExpression {
    if (range.start + 2 > range.end or range.end > tokens.len) return null;
    if (tokens[range.end - 1].kind != .rparen) return null;

    var depth: usize = 0;
    var lparen_index: ?usize = null;
    var index = range.start;
    while (index < range.end) : (index += 1) {
        switch (tokens[index].kind) {
            .lparen => {
                if (depth == 0 and lparen_index == null) lparen_index = index;
                depth += 1;
            },
            .rparen => {
                if (depth == 0) return null;
                depth -= 1;
                if (depth == 0 and index + 1 != range.end) return null;
            },
            else => {},
        }
    }
    if (depth != 0) return null;
    const open_index = lparen_index orelse return null;
    if (open_index <= range.start) return null;
    if (isGeneratedFunctionCallBlockedName(tokens[open_index - 1])) return null;
    if (!isGeneratedQualifiedNameRange(tokens, .{ .start = range.start, .end = open_index })) return null;
    return .{
        .name_tokens = .{ .start = range.start, .end = open_index },
        .argument_tokens = if (open_index + 1 < range.end - 1) .{ .start = open_index + 1, .end = range.end - 1 } else null,
    };
}

fn isGeneratedFunctionCallBlockedName(token: token_mod.Token) bool {
    return token.matchesKeywordTag(.in) or
        token.matchesKeywordTag(.not) or
        token.matchesKeywordTag(.any) or
        token.matchesKeywordTag(.all) or
        token.matchesKeywordTag(.some);
}

fn isGeneratedQualifiedNameRange(tokens: []const token_mod.Token, range: GeneratedSqlTokenRange) bool {
    if (range.start >= range.end or range.end > tokens.len) return false;
    var index = range.start;
    while (index < range.end) : (index += 1) {
        if (tokens[index].kind != .identifier) return false;
    }
    return true;
}

fn generatedWrappedExpressionInnerRange(tokens: []const token_mod.Token, range: GeneratedSqlTokenRange) ?GeneratedSqlTokenRange {
    if (range.start + 2 > range.end or range.end > tokens.len) return null;
    if (tokens[range.start].kind != .lparen or tokens[range.end - 1].kind != .rparen) return null;
    var depth: usize = 0;
    var index = range.start;
    while (index < range.end) : (index += 1) {
        switch (tokens[index].kind) {
            .lparen => depth += 1,
            .rparen => {
                if (depth == 0) return null;
                depth -= 1;
                if (depth == 0 and index + 1 != range.end) return null;
            },
            else => {},
        }
    }
    if (depth != 0) return null;
    return .{ .start = range.start + 1, .end = range.end - 1 };
}

const GeneratedSqlExpressionOperator = struct {
    kind: GeneratedSqlExpressionKind,
    index: usize,
    operator_end_index: ?usize = null,
    negation_index: ?usize = null,
    quantifier_index: ?usize = null,
    prefix: bool = false,
};

fn findTopLevelExpressionOperator(tokens: []const token_mod.Token, range: GeneratedSqlTokenRange) ?GeneratedSqlExpressionOperator {
    if (range.start >= range.end or range.end > tokens.len) return null;
    var depth: usize = 0;
    var index = range.start;
    while (index < range.end) : (index += 1) {
        switch (tokens[index].kind) {
            .lparen, .lbracket => depth += 1,
            .rparen, .rbracket => {
                if (depth == 0) return null;
                depth -= 1;
            },
            else => if (depth == 0 and tokens[index].matchesKeywordTag(.@"or")) return .{ .kind = .logical_or, .index = index },
        }
    }
    depth = 0;
    index = range.start;
    var skip_next_between_and = false;
    while (index < range.end) : (index += 1) {
        switch (tokens[index].kind) {
            .lparen, .lbracket => depth += 1,
            .rparen, .rbracket => {
                if (depth == 0) return null;
                depth -= 1;
            },
            else => if (depth == 0) {
                if (tokens[index].matchesKeywordTag(.between)) {
                    skip_next_between_and = true;
                } else if (tokens[index].matchesKeywordTag(.@"and")) {
                    if (skip_next_between_and) {
                        skip_next_between_and = false;
                    } else {
                        return .{ .kind = .logical_and, .index = index };
                    }
                }
            },
        }
    }
    depth = 0;
    index = range.start;
    if (tokens[index].matchesKeywordTag(.not)) return .{ .kind = .logical_not, .index = index, .prefix = true };
    while (index < range.end) : (index += 1) {
        switch (tokens[index].kind) {
            .lparen, .lbracket => depth += 1,
            .rparen, .rbracket => {
                if (depth == 0) return null;
                depth -= 1;
            },
            .eq, .neq, .lt, .lte, .gt, .gte => if (depth == 0) {
                if (index + 1 < range.end and isGeneratedQuantifiedOperator(tokens[index + 1])) {
                    return .{ .kind = .quantified_comparison, .index = index, .quantifier_index = index + 1 };
                }
                return .{ .kind = .comparison, .index = index };
            },
            else => if (depth == 0) {
                if (tokens[index].matchesKeywordTag(.not) and index + 1 < range.end) {
                    if (tokens[index + 1].matchesKeywordTag(.like)) return .{ .kind = .not_like, .index = index + 1, .negation_index = index };
                    if (tokens[index + 1].matchesKeywordTag(.ilike)) return .{ .kind = .not_ilike, .index = index + 1, .negation_index = index };
                    if (tokens[index + 1].matchesKeywordTag(.in)) return .{ .kind = .not_in_list, .index = index + 1, .negation_index = index };
                    if (tokens[index + 1].matchesKeywordTag(.between)) return .{ .kind = .not_between, .index = index + 1, .negation_index = index };
                }
                if (tokens[index].matchesKeywordTag(.like)) return .{ .kind = .like, .index = index };
                if (tokens[index].matchesKeywordTag(.ilike)) return .{ .kind = .ilike, .index = index };
                if (tokens[index].matchesKeywordTag(.in)) return .{ .kind = .in_list, .index = index };
                if (tokens[index].matchesKeywordTag(.between)) return .{ .kind = .between, .index = index };
                if (tokens[index].matchesKeywordTag(.is) and index + 1 < range.end) {
                    if (tokens[index + 1].matchesKeywordTag(.null)) return .{ .kind = .is_null, .index = index };
                    if (tokens[index + 1].matchesKeywordTag(.true)) return .{ .kind = .is_true, .index = index };
                    if (tokens[index + 1].matchesKeywordTag(.false)) return .{ .kind = .is_false, .index = index };
                    if (tokens[index + 1].matchesKeywordTag(.unknown)) return .{ .kind = .is_unknown, .index = index };
                    if (index + 3 < range.end and tokens[index + 1].matchesKeywordTag(.distinct) and tokens[index + 2].matchesKeywordTag(.from)) {
                        return .{ .kind = .is_distinct_from, .index = index, .operator_end_index = index + 3 };
                    }
                    if (index + 2 < range.end and tokens[index + 1].matchesKeywordTag(.not) and tokens[index + 2].matchesKeywordTag(.null)) {
                        return .{ .kind = .is_not_null, .index = index };
                    }
                    if (index + 2 < range.end and tokens[index + 1].matchesKeywordTag(.not) and tokens[index + 2].matchesKeywordTag(.true)) {
                        return .{ .kind = .is_not_true, .index = index };
                    }
                    if (index + 2 < range.end and tokens[index + 1].matchesKeywordTag(.not) and tokens[index + 2].matchesKeywordTag(.false)) {
                        return .{ .kind = .is_not_false, .index = index };
                    }
                    if (index + 2 < range.end and tokens[index + 1].matchesKeywordTag(.not) and tokens[index + 2].matchesKeywordTag(.unknown)) {
                        return .{ .kind = .is_not_unknown, .index = index };
                    }
                    if (index + 4 < range.end and tokens[index + 1].matchesKeywordTag(.not) and tokens[index + 2].matchesKeywordTag(.distinct) and tokens[index + 3].matchesKeywordTag(.from)) {
                        return .{ .kind = .is_not_distinct_from, .index = index, .operator_end_index = index + 4, .negation_index = index + 1 };
                    }
                }
            },
        }
    }
    depth = 0;
    index = range.start;
    while (index < range.end) : (index += 1) {
        switch (tokens[index].kind) {
            .lparen, .lbracket => depth += 1,
            .rparen, .rbracket => {
                if (depth == 0) return null;
                depth -= 1;
            },
            .plus => if (depth == 0 and index > range.start) return .{ .kind = .additive, .index = index },
            .minus => if (depth == 0 and index > range.start) return .{ .kind = .subtractive, .index = index },
            else => {},
        }
    }
    depth = 0;
    index = range.start;
    while (index < range.end) : (index += 1) {
        switch (tokens[index].kind) {
            .lparen, .lbracket => depth += 1,
            .rparen, .rbracket => {
                if (depth == 0) return null;
                depth -= 1;
            },
            .star => if (depth == 0 and index > range.start) return .{ .kind = .multiplicative, .index = index },
            .slash => if (depth == 0 and index > range.start) return .{ .kind = .divisive, .index = index },
            .percent => if (depth == 0 and index > range.start) return .{ .kind = .modulo, .index = index },
            else => {},
        }
    }
    depth = 0;
    index = range.start;
    while (index < range.end) : (index += 1) {
        switch (tokens[index].kind) {
            .lparen, .lbracket => depth += 1,
            .rparen, .rbracket => {
                if (depth == 0) return null;
                depth -= 1;
            },
            .arrow_json => if (depth == 0 and index > range.start) return .{ .kind = .json_access, .index = index },
            .arrow_text => if (depth == 0 and index > range.start) return .{ .kind = .json_text_access, .index = index },
            .path_arrow_json => if (depth == 0 and index > range.start) return .{ .kind = .json_path_access, .index = index },
            .path_arrow_text => if (depth == 0 and index > range.start) return .{ .kind = .json_path_text_access, .index = index },
            else => {},
        }
    }
    return null;
}

fn isGeneratedQuantifiedOperator(token: token_mod.Token) bool {
    return token.matchesKeywordTag(.any) or token.matchesKeywordTag(.all) or token.matchesKeywordTag(.some);
}

fn findKeyword(tokens: []const token_mod.Token, start: usize, end: usize, keyword: token_mod.TokenKeyword) ?usize {
    var index = start;
    while (index < end) : (index += 1) {
        if (tokens[index].matchesKeywordTag(keyword)) return index;
    }
    return null;
}

fn findTopLevelKeyword(tokens: []const token_mod.Token, start: usize, end: usize, keyword: token_mod.TokenKeyword) ?usize {
    var depth: usize = 0;
    var index = start;
    while (index < end) : (index += 1) {
        switch (tokens[index].kind) {
            .lparen, .lbracket => depth += 1,
            .rparen, .rbracket => {
                if (depth == 0) return null;
                depth -= 1;
            },
            else => if (depth == 0 and tokens[index].matchesKeywordTag(keyword)) return index,
        }
    }
    return null;
}

fn findTopLevelKeywordSequence(tokens: []const token_mod.Token, start: usize, end: usize, first: token_mod.TokenKeyword, second: token_mod.TokenKeyword) ?usize {
    var depth: usize = 0;
    var index = start;
    while (index + 1 < end) : (index += 1) {
        switch (tokens[index].kind) {
            .lparen, .lbracket => depth += 1,
            .rparen, .rbracket => {
                if (depth == 0) return null;
                depth -= 1;
            },
            else => if (depth == 0 and tokens[index].matchesKeywordTag(first) and tokens[index + 1].matchesKeywordTag(second)) return index,
        }
    }
    return null;
}

fn findKeywordText(tokens: []const token_mod.Token, start: usize, end: usize, keyword: []const u8) ?usize {
    var index = start;
    while (index < end) : (index += 1) {
        if (tokens[index].matchesKeyword(keyword)) return index;
    }
    return null;
}

fn findMatchingParen(tokens: []const token_mod.Token, open_index: usize, end: usize) ?usize {
    if (open_index >= end or tokens[open_index].kind != .lparen) return null;
    var depth: usize = 1;
    var index = open_index + 1;
    while (index < end) : (index += 1) {
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

fn minOptionalIndex(a: ?usize, b: ?usize) ?usize {
    if (a == null) return b;
    if (b == null) return a;
    return @min(a.?, b.?);
}

fn firstOptionalIndex(indices: []const ?usize) ?usize {
    var best: ?usize = null;
    for (indices) |index| {
        if (index) |idx| {
            if (best == null or idx < best.?) best = idx;
        }
    }
    return best;
}

fn statementTokenEnd(tokens: []const token_mod.Token) usize {
    var end = tokens.len;
    while (end > 0 and tokens[end - 1].kind == .semicolon) end -= 1;
    return end;
}

fn sourceSpanForTokenRange(tokens: []const token_mod.Token, range: GeneratedSqlTokenRange) ?token_mod.SourceSpan {
    if (range.start >= range.end or range.end > tokens.len) return null;
    return .{
        .start = tokens[range.start].source_start,
        .end = tokens[range.end - 1].source_end,
    };
}

test "generated SQL parser accepts session and control statements" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    for (first_family_corpus) |case| {
        const result = try parseSqlAlloc(alloc, case.sql);
        try std.testing.expectEqual(case.kind, result.kind);
    }
}

test "generated SQL parser facade classifies gated corpus" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const corpus = first_family_corpus ++ simple_ddl_corpus ++ simple_dml_corpus ++ simple_read_corpus ++ simple_graph_corpus ++ unsupported_corpus;
    for (corpus) |case| {
        const generated_result = try parseSqlAlloc(alloc, case.sql);
        try std.testing.expectEqual(case.kind, generated_result.kind);
    }
}

test "generated SQL parser facade exposes typed statement nodes" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    try std.testing.expectEqual(GeneratedSqlStatement{ .session = .set }, (try parseSqlAlloc(alloc, "SET search_path TO public")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .transaction = .rollback }, (try parseSqlAlloc(alloc, "ROLLBACK")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .prepared = .execute }, (try parseSqlAlloc(alloc, "EXECUTE read_stmt()")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .create_table }, (try parseSqlAlloc(alloc, "CREATE TABLE usage_records (id text)")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .drop_schema }, (try parseSqlAlloc(alloc, "DROP SCHEMA analytics CASCADE")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .dml = .insert_values }, (try parseSqlAlloc(alloc, "INSERT INTO usage_records (id) VALUES ('u1')")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .dml = .insert_values }, (try parseSqlAlloc(alloc, "INSERT INTO usage_records DEFAULT VALUES")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .dml = .update }, (try parseSqlAlloc(alloc, "UPDATE usage_records SET status = 'done' WHERE id = 'u1'")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .read = .query }, (try parseSqlAlloc(alloc, "SELECT id FROM usage_records")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .read = .cte }, (try parseSqlAlloc(alloc, "WITH source_rows AS (SELECT id FROM usage_records) SELECT id FROM source_rows")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .graph = .create_index }, (try parseSqlAlloc(alloc, "CREATE GRAPH INDEX docs_edge_graph ON doc_edges")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .graph = .create_metric }, (try parseSqlAlloc(alloc, "CREATE GRAPH METRIC docs_pagerank ON doc_edges")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .analyze }, (try parseSqlAlloc(alloc, "ANALYZE")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .explain }, (try parseSqlAlloc(alloc, "EXPLAIN SELECT id FROM usage_records")).statement);
}

test "generated SQL parser facade builds control AST spans" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const set_sql = "  SET antfly.sync_level = 'write';";
    var set_tokens = try lexer.tokenizeAlloc(alloc, set_sql);
    defer lexer.freeTokens(alloc, &set_tokens);
    const set_result = try parseTokensAlloc(alloc, set_tokens.items);
    switch (set_result.ast.?) {
        .session => |session| {
            try std.testing.expectEqual(GeneratedSqlSessionKind.set, session.kind);
            try std.testing.expectEqualStrings("SET antfly.sync_level = 'write'", spanText(set_sql, session.statement_span));
            try std.testing.expectEqualStrings("SET", spanText(set_sql, session.command_span));
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, session.name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, session.value_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const transaction_sql = "ROLLBACK;";
    const transaction_result = try parseSqlAlloc(alloc, transaction_sql);
    switch (transaction_result.ast.?) {
        .transaction => |transaction| {
            try std.testing.expectEqual(GeneratedSqlTransactionKind.rollback, transaction.kind);
            try std.testing.expectEqualStrings("ROLLBACK", spanText(transaction_sql, transaction.statement_span));
            try std.testing.expectEqualStrings("ROLLBACK", spanText(transaction_sql, transaction.command_span));
        },
        else => return error.TestUnexpectedResult,
    }

    const prepare_sql = "PREPARE read_stmt(text) AS SELECT id FROM usage_records WHERE status = $1";
    var prepare_tokens = try lexer.tokenizeAlloc(alloc, prepare_sql);
    defer lexer.freeTokens(alloc, &prepare_tokens);
    const prepare_result = try parseTokensAlloc(alloc, prepare_tokens.items);
    switch (prepare_result.ast.?) {
        .prepared => |prepared| {
            try std.testing.expectEqual(GeneratedSqlPreparedKind.prepare, prepared.kind);
            try std.testing.expectEqualStrings("PREPARE", spanText(prepare_sql, prepared.command_span));
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, prepared.name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 5 }, prepared.parameter_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 14 }, prepared.inner_statement_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const ddl_sql = "CREATE SCHEMA IF NOT EXISTS analytics";
    const ddl_result = try parseSqlAlloc(alloc, ddl_sql);
    switch (ddl_result.ast.?) {
        .ddl => |ddl| {
            try std.testing.expectEqual(GeneratedSqlDdlKind.create_schema, ddl.kind);
            try std.testing.expectEqualStrings("CREATE SCHEMA IF NOT EXISTS analytics", spanText(ddl_sql, ddl.statement_span));
            try std.testing.expectEqualStrings("CREATE", spanText(ddl_sql, ddl.command_span));
            try std.testing.expect(ddl.if_not_exists);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, ddl.object_name_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const extension_sql = "CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public VERSION '1.3'";
    const extension_result = try parseSqlAlloc(alloc, extension_sql);
    switch (extension_result.ast.?) {
        .ddl => |ddl| {
            try std.testing.expectEqual(GeneratedSqlDdlKind.create_extension, ddl.kind);
            try std.testing.expect(ddl.if_not_exists);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, ddl.object_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, ddl.schema_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, ddl.version_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const drop_sql = "DROP DATABASE IF EXISTS tenant_ops WITH (FORCE)";
    const drop_result = try parseSqlAlloc(alloc, drop_sql);
    switch (drop_result.ast.?) {
        .ddl => |ddl| {
            try std.testing.expectEqual(GeneratedSqlDdlKind.drop_database, ddl.kind);
            try std.testing.expect(ddl.if_exists);
            try std.testing.expect(ddl.force);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 5 }, ddl.object_name_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const dml_sql = "UPDATE usage_records SET status = 'done' WHERE id = 'u1'";
    const dml_result = try parseSqlAlloc(alloc, dml_sql);
    switch (dml_result.ast.?) {
        .dml => |dml| {
            try std.testing.expectEqual(GeneratedSqlDmlKind.update, dml.kind);
            try std.testing.expectEqualStrings("UPDATE usage_records SET status = 'done' WHERE id = 'u1'", spanText(dml_sql, dml.statement_span));
            try std.testing.expectEqualStrings("UPDATE", spanText(dml_sql, dml.command_span));
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, dml.target_table_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 6 }, dml.assignments_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 10 }, dml.where_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const default_insert_sql = "INSERT INTO usage_records DEFAULT VALUES";
    const default_insert_result = try parseSqlAlloc(alloc, default_insert_sql);
    switch (default_insert_result.ast.?) {
        .dml => |dml| {
            try std.testing.expectEqual(GeneratedSqlDmlKind.insert_values, dml.kind);
            try std.testing.expect(dml.default_values);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, dml.target_table_tokens.?);
            try std.testing.expect(dml.insert_columns_tokens == null);
            try std.testing.expect(dml.values_tokens == null);
        },
        else => return error.TestUnexpectedResult,
    }

    const conflict_insert_sql = "INSERT INTO usage_records (id, status) VALUES ('u1', 'ready') ON CONFLICT (id) DO NOTHING RETURNING id";
    const conflict_insert_result = try parseSqlAlloc(alloc, conflict_insert_sql);
    switch (conflict_insert_result.ast.?) {
        .dml => |dml| {
            try std.testing.expectEqual(GeneratedSqlDmlKind.insert_values, dml.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 8 }, dml.insert_columns_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 14 }, dml.values_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 15, .end = 21 }, dml.conflict_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 22, .end = 23 }, dml.returning_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const partial_conflict_sql = "INSERT INTO usage_records (id, status) VALUES ('u1', 'ready') ON CONFLICT (id) WHERE status = 'ready' DO NOTHING";
    const partial_conflict_result = try parseSqlAlloc(alloc, partial_conflict_sql);
    switch (partial_conflict_result.ast.?) {
        .dml => |dml| {
            try std.testing.expectEqual(GeneratedSqlDmlKind.insert_values, dml.kind);
            try std.testing.expect(dml.conflict_tokens != null);
            try std.testing.expect(dml.returning_tokens == null);
        },
        else => return error.TestUnexpectedResult,
    }

    const named_conflict_sql = "INSERT INTO usage_records (id, status) VALUES ('u1', 'ready') ON CONFLICT ON CONSTRAINT usage_records_pkey DO NOTHING";
    const named_conflict_result = try parseSqlAlloc(alloc, named_conflict_sql);
    switch (named_conflict_result.ast.?) {
        .dml => |dml| {
            try std.testing.expectEqual(GeneratedSqlDmlKind.insert_values, dml.kind);
            try std.testing.expect(dml.conflict_tokens != null);
            try std.testing.expect(dml.returning_tokens == null);
        },
        else => return error.TestUnexpectedResult,
    }

    const truncate_sql = "TRUNCATE TABLE public.usage_records, usage_archive RESTART IDENTITY CASCADE";
    const truncate_result = try parseSqlAlloc(alloc, truncate_sql);
    switch (truncate_result.ast.?) {
        .dml => |dml| {
            try std.testing.expectEqual(GeneratedSqlDmlKind.truncate, dml.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, dml.target_table_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 5 }, dml.additional_target_tokens.?);
            try std.testing.expect(dml.restart_identity);
            try std.testing.expect(dml.cascade);
        },
        else => return error.TestUnexpectedResult,
    }

    const read_sql = "SELECT id, status FROM usage_records WHERE status = 'open' ORDER BY id LIMIT 10";
    const read_result = try parseSqlAlloc(alloc, read_sql);
    switch (read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqualStrings("SELECT id, status FROM usage_records WHERE status = 'open' ORDER BY id LIMIT 10", spanText(read_sql, read.statement_span));
            try std.testing.expectEqualStrings("SELECT", spanText(read_sql, read.command_span));
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 4 }, read.projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.projection_items.first_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read.projection_items.last_tokens.?);
            try std.testing.expectEqual(@as(usize, 2), read.projection_items.count);
            try std.testing.expectEqual(@as(usize, 2), read.projection_items.items.len);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.projection_items.items[0]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read.projection_items.items[1]);
            try std.testing.expectEqual(@as(usize, 2), read.projection_items.expressions.len);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.projection_items.expressions[0].kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.projection_items.expressions[0].tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.projection_items.expressions[1].kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read.projection_items.expressions[1].tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.projection_first_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.projection_first_expression.tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.projection_last_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read.projection_last_expression.tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.source_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 10 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read.where_expression.right_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 12, .end = 13 }, read.order_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 12, .end = 13 }, read.order_items.first_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 12, .end = 13 }, read.order_items.last_tokens.?);
            try std.testing.expectEqual(@as(usize, 1), read.order_items.count);
            try std.testing.expectEqual(@as(usize, 1), read.order_items.items.len);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 12, .end = 13 }, read.order_items.items[0]);
            try std.testing.expectEqual(@as(usize, 1), read.order_items.expressions.len);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.order_items.expressions[0].kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 12, .end = 13 }, read.order_items.expressions[0].tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 14, .end = 15 }, read.limit_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.limit_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 14, .end = 15 }, read.limit_expression.tokens.?);
            try std.testing.expect(read.group_tokens == null);
            try std.testing.expect(read.having_tokens == null);
        },
        else => return error.TestUnexpectedResult,
    }

    const like_read_sql = "SELECT id FROM usage_records WHERE status LIKE 'open%'";
    const like_read_result = try parseSqlAlloc(alloc, like_read_sql);
    switch (like_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.like, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const ilike_read_sql = "SELECT id FROM usage_records WHERE status ILIKE 'open%'";
    const ilike_read_result = try parseSqlAlloc(alloc, ilike_read_sql);
    switch (ilike_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.ilike, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const in_list_read_sql = "SELECT id FROM usage_records WHERE id IN ('u1', 'u2')";
    const in_list_read_result = try parseSqlAlloc(alloc, in_list_read_sql);
    switch (in_list_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 12 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.in_list, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 12 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const between_read_sql = "SELECT id FROM usage_records WHERE score BETWEEN 1 AND 10";
    const between_read_result = try parseSqlAlloc(alloc, between_read_sql);
    switch (between_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 10 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.between, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 10 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const not_like_read_sql = "SELECT id FROM usage_records WHERE status NOT LIKE 'closed%'";
    const not_like_read_result = try parseSqlAlloc(alloc, not_like_read_sql);
    switch (not_like_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 9 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.not_like, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.negation_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const not_ilike_read_sql = "SELECT id FROM usage_records WHERE status NOT ILIKE 'closed%'";
    const not_ilike_read_result = try parseSqlAlloc(alloc, not_ilike_read_sql);
    switch (not_ilike_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 9 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.not_ilike, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.negation_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const not_in_list_read_sql = "SELECT id FROM usage_records WHERE id NOT IN ('u1', 'u2')";
    const not_in_list_read_result = try parseSqlAlloc(alloc, not_in_list_read_sql);
    switch (not_in_list_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 13 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.not_in_list, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.negation_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 13 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const not_between_read_sql = "SELECT id FROM usage_records WHERE score NOT BETWEEN 1 AND 10";
    const not_between_read_result = try parseSqlAlloc(alloc, not_between_read_sql);
    switch (not_between_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 11 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.not_between, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.negation_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 11 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const any_read_sql = "SELECT id FROM usage_records WHERE score = ANY (1, 2)";
    const any_read_result = try parseSqlAlloc(alloc, any_read_sql);
    switch (any_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 13 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.quantified_comparison, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.where_expression.quantifier_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 13 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const all_read_sql = "SELECT id FROM usage_records WHERE score <> ALL (1, 2)";
    const all_read_result = try parseSqlAlloc(alloc, all_read_sql);
    switch (all_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 13 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.quantified_comparison, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.where_expression.quantifier_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 13 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const some_read_sql = "SELECT id FROM usage_records WHERE score > SOME (1, 2)";
    const some_read_result = try parseSqlAlloc(alloc, some_read_sql);
    switch (some_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 13 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.quantified_comparison, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.where_expression.quantifier_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 13 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const is_null_read_sql = "SELECT id FROM usage_records WHERE deleted_at IS NULL";
    const is_null_read_result = try parseSqlAlloc(alloc, is_null_read_sql);
    switch (is_null_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.is_null, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const is_not_null_read_sql = "SELECT id FROM usage_records WHERE deleted_at IS NOT NULL";
    const is_not_null_read_result = try parseSqlAlloc(alloc, is_not_null_read_sql);
    switch (is_not_null_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 9 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.is_not_null, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 9 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const is_true_read_sql = "SELECT id FROM usage_records WHERE active IS TRUE";
    const is_true_read_result = try parseSqlAlloc(alloc, is_true_read_sql);
    switch (is_true_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.is_true, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const is_not_false_read_sql = "SELECT id FROM usage_records WHERE active IS NOT FALSE";
    const is_not_false_read_result = try parseSqlAlloc(alloc, is_not_false_read_sql);
    switch (is_not_false_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 9 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.is_not_false, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 9 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const is_unknown_read_sql = "SELECT id FROM usage_records WHERE active IS UNKNOWN";
    const is_unknown_read_result = try parseSqlAlloc(alloc, is_unknown_read_sql);
    switch (is_unknown_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.is_unknown, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const is_not_unknown_read_sql = "SELECT id FROM usage_records WHERE active IS NOT UNKNOWN";
    const is_not_unknown_read_result = try parseSqlAlloc(alloc, is_not_unknown_read_sql);
    switch (is_not_unknown_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 9 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.is_not_unknown, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 9 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const is_distinct_read_sql = "SELECT id FROM usage_records WHERE status IS DISTINCT FROM previous_status";
    const is_distinct_read_result = try parseSqlAlloc(alloc, is_distinct_read_sql);
    switch (is_distinct_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 10 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.is_distinct_from, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 9 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const is_not_distinct_read_sql = "SELECT id FROM usage_records WHERE status IS NOT DISTINCT FROM previous_status";
    const is_not_distinct_read_result = try parseSqlAlloc(alloc, is_not_distinct_read_sql);
    switch (is_not_distinct_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 11 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.is_not_distinct_from, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.where_expression.negation_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 10 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const logical_or_read_sql = "SELECT id FROM usage_records WHERE status = 'open' OR deleted_at IS NULL";
    const logical_or_read_result = try parseSqlAlloc(alloc, logical_or_read_sql);
    switch (logical_or_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 12 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.logical_or, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.where_expression.left_expression_kind.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.where_expression.left_expression.?.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read.where_expression.left_expression.?.tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 12 }, read.where_expression.right_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.is_null, read.where_expression.right_expression_kind.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.is_null, read.where_expression.right_expression.?.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 12 }, read.where_expression.right_expression.?.tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const logical_and_read_sql = "SELECT id FROM usage_records WHERE status = 'open' AND deleted_at IS NULL";
    const logical_and_read_result = try parseSqlAlloc(alloc, logical_and_read_sql);
    switch (logical_and_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 12 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.logical_and, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.where_expression.left_expression_kind.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 12 }, read.where_expression.right_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.is_null, read.where_expression.right_expression_kind.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const logical_not_read_sql = "SELECT id FROM usage_records WHERE NOT deleted_at IS NULL";
    const logical_not_read_result = try parseSqlAlloc(alloc, logical_not_read_sql);
    switch (logical_not_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 9 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.logical_not, read.where_expression.kind);
            try std.testing.expect(read.where_expression.left_tokens == null);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 9 }, read.where_expression.right_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.is_null, read.where_expression.right_expression_kind.?);
            try std.testing.expect(read.where_expression.left_expression == null);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.is_null, read.where_expression.right_expression.?.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 9 }, read.where_expression.right_expression.?.tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const grouped_read_sql = "SELECT id FROM usage_records WHERE (status = 'open')";
    const grouped_read_result = try parseSqlAlloc(alloc, grouped_read_sql);
    switch (grouped_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 10 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.grouped, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 9 }, read.where_expression.inner_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.where_expression.inner_expression_kind.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.where_expression.inner_expression.?.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 9 }, read.where_expression.inner_expression.?.tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const logical_not_grouped_read_sql = "SELECT id FROM usage_records WHERE NOT (deleted_at IS NULL)";
    const logical_not_grouped_read_result = try parseSqlAlloc(alloc, logical_not_grouped_read_sql);
    switch (logical_not_grouped_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 11 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.logical_not, read.where_expression.kind);
            try std.testing.expect(read.where_expression.left_tokens == null);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 11 }, read.where_expression.right_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.grouped, read.where_expression.right_expression_kind.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const additive_comparison_read_sql = "SELECT id FROM usage_records WHERE score + bonus > 10";
    const additive_comparison_read_result = try parseSqlAlloc(alloc, additive_comparison_read_sql);
    switch (additive_comparison_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 10 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.additive, read.where_expression.left_expression_kind.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.additive, read.where_expression.left_expression.?.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read.where_expression.left_expression.?.tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read.where_expression.right_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.where_expression.right_expression.?.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read.where_expression.right_expression.?.tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const multiplicative_comparison_read_sql = "SELECT id FROM usage_records WHERE score * weight > 10";
    const multiplicative_comparison_read_result = try parseSqlAlloc(alloc, multiplicative_comparison_read_sql);
    switch (multiplicative_comparison_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 10 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.multiplicative, read.where_expression.left_expression_kind.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const json_text_comparison_read_sql = "SELECT id FROM usage_records WHERE payload ->> 'status' = 'open'";
    const json_text_comparison_read_result = try parseSqlAlloc(alloc, json_text_comparison_read_sql);
    switch (json_text_comparison_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 10 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.json_text_access, read.where_expression.left_expression_kind.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const function_comparison_read_sql = "SELECT id FROM usage_records WHERE lower(status) = 'open'";
    const function_comparison_read_result = try parseSqlAlloc(alloc, function_comparison_read_sql);
    switch (function_comparison_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 11 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 9 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.function_call, read.where_expression.left_expression_kind.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    var function_tokens = try lexer.tokenizeAlloc(alloc, "lower(status, fallback)");
    defer lexer.freeTokens(alloc, &function_tokens);
    var function_expression = try buildGeneratedExpressionAst(alloc, function_tokens.items, .{ .start = 0, .end = function_tokens.items.len });
    defer function_expression.deinit(alloc);
    try std.testing.expectEqual(GeneratedSqlExpressionKind.function_call, function_expression.kind);
    try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 0, .end = 1 }, function_expression.function_name_tokens.?);
    try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 5 }, function_expression.argument_tokens.?);
    try std.testing.expectEqual(@as(usize, 2), function_expression.argument_items.count);
    try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, function_expression.argument_items.first_tokens.?);
    try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 5 }, function_expression.argument_items.last_tokens.?);
    try std.testing.expectEqual(@as(usize, 2), function_expression.argument_items.items.len);
    try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, function_expression.argument_items.items[0]);
    try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 5 }, function_expression.argument_items.items[1]);
    try std.testing.expectEqual(@as(usize, 2), function_expression.argument_items.expressions.len);
    try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, function_expression.argument_items.expressions[0].kind);
    try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, function_expression.argument_items.expressions[0].tokens.?);
    try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, function_expression.argument_items.expressions[1].kind);
    try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 5 }, function_expression.argument_items.expressions[1].tokens.?);

    const nested_list_read_sql = "SELECT id, row_number() OVER (PARTITION BY tenant, account ORDER BY id) AS rn FROM usage_records ORDER BY id, tenant";
    const nested_list_read_result = try parseSqlAlloc(alloc, nested_list_read_sql);
    switch (nested_list_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.window, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 19 }, read.projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.projection_items.first_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 19 }, read.projection_items.last_tokens.?);
            try std.testing.expectEqual(@as(usize, 2), read.projection_items.count);
            try std.testing.expectEqual(@as(usize, 2), read.projection_items.items.len);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.projection_items.items[0]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 19 }, read.projection_items.items[1]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 20, .end = 21 }, read.source_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 23, .end = 26 }, read.order_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 23, .end = 24 }, read.order_items.first_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 25, .end = 26 }, read.order_items.last_tokens.?);
            try std.testing.expectEqual(@as(usize, 2), read.order_items.count);
            try std.testing.expectEqual(@as(usize, 2), read.order_items.items.len);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 23, .end = 24 }, read.order_items.items[0]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 25, .end = 26 }, read.order_items.items[1]);
        },
        else => return error.TestUnexpectedResult,
    }

    const function_call_read_sql = "SELECT concat_ws(',', status), id FROM usage_records ORDER BY status, id";
    const function_call_read_result = try parseSqlAlloc(alloc, function_call_read_sql);
    switch (function_call_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 9 }, read.projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 7 }, read.projection_items.first_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read.projection_items.last_tokens.?);
            try std.testing.expectEqual(@as(usize, 2), read.projection_items.count);
            try std.testing.expectEqual(@as(usize, 2), read.projection_items.items.len);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 7 }, read.projection_items.items[0]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read.projection_items.items[1]);
            try std.testing.expectEqual(@as(usize, 2), read.projection_items.expressions.len);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.function_call, read.projection_items.expressions[0].kind);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.projection_items.expressions[1].kind);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.function_call, read.projection_first_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.projection_first_expression.function_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 6 }, read.projection_first_expression.argument_tokens.?);
            try std.testing.expectEqual(@as(usize, 2), read.projection_first_expression.argument_items.count);
            try std.testing.expectEqual(@as(usize, 2), read.projection_first_expression.argument_items.items.len);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read.projection_first_expression.argument_items.items[0]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.projection_first_expression.argument_items.items[1]);
            try std.testing.expectEqual(@as(usize, 2), read.projection_first_expression.argument_items.expressions.len);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.projection_first_expression.argument_items.expressions[0].kind);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.projection_first_expression.argument_items.expressions[1].kind);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.projection_last_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read.projection_last_expression.tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read.source_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 13, .end = 16 }, read.order_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 13, .end = 14 }, read.order_items.first_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 15, .end = 16 }, read.order_items.last_tokens.?);
            try std.testing.expectEqual(@as(usize, 2), read.order_items.count);
            try std.testing.expectEqual(@as(usize, 2), read.order_items.items.len);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 13, .end = 14 }, read.order_items.items[0]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 15, .end = 16 }, read.order_items.items[1]);
            try std.testing.expectEqual(@as(usize, 2), read.order_items.expressions.len);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.order_items.expressions[0].kind);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.order_items.expressions[1].kind);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.order_first_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 13, .end = 14 }, read.order_first_expression.tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.order_last_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 15, .end = 16 }, read.order_last_expression.tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const aggregate_read_sql = "SELECT status FROM usage_records GROUP BY status HAVING count > 1";
    const aggregate_read_result = try parseSqlAlloc(alloc, aggregate_read_sql);
    switch (aggregate_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.aggregate, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read.source_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.group_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.group_items.first_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.group_items.last_tokens.?);
            try std.testing.expectEqual(@as(usize, 1), read.group_items.count);
            try std.testing.expectEqual(@as(usize, 1), read.group_items.items.len);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.group_items.items[0]);
            try std.testing.expectEqual(@as(usize, 1), read.group_items.expressions.len);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.group_items.expressions[0].kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.group_items.expressions[0].tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.group_first_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.group_first_expression.tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.group_last_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.group_last_expression.tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 11 }, read.having_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.having_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read.having_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read.having_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read.having_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const joined_read_sql = "SELECT usage_records.id FROM usage_records JOIN accounts ON usage_records.account_id = accounts.id";
    const joined_read_result = try parseSqlAlloc(alloc, joined_read_sql);
    switch (joined_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.join, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 10 }, read.source_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 10 }, read.join_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 5 }, read.join_operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlJoinKind.inner, read.join_kind.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read.join_left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.join_right_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 10 }, read.join_predicate_tokens.?);
            try std.testing.expectEqual(@as(usize, 1), read.join_items.len);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 10 }, read.join_items[0].tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 5 }, read.join_items[0].operator_tokens);
            try std.testing.expectEqual(GeneratedSqlJoinKind.inner, read.join_items[0].kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read.join_items[0].left_tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.join_items[0].right_tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 10 }, read.join_items[0].predicate_tokens);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.join_items[0].predicate_expression.kind);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.join_predicate_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.join_predicate_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read.join_predicate_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read.join_predicate_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const multi_joined_read_sql = "SELECT usage_records.id FROM usage_records JOIN accounts ON usage_records.account_id = accounts.id JOIN tenants ON accounts.tenant_id = tenants.id";
    const multi_joined_read_result = try parseSqlAlloc(alloc, multi_joined_read_sql);
    switch (multi_joined_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.join, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 16 }, read.source_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 16 }, read.join_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 5 }, read.join_operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlJoinKind.inner, read.join_kind.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read.join_left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.join_right_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 10 }, read.join_predicate_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.join_predicate_expression.kind);
            try std.testing.expectEqual(@as(usize, 2), read.join_items.len);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 10 }, read.join_items[0].tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 5 }, read.join_items[0].operator_tokens);
            try std.testing.expectEqual(GeneratedSqlJoinKind.inner, read.join_items[0].kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read.join_items[0].left_tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.join_items[0].right_tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 10 }, read.join_items[0].predicate_tokens);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.join_items[0].predicate_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 16 }, read.join_items[1].tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read.join_items[1].operator_tokens);
            try std.testing.expectEqual(GeneratedSqlJoinKind.inner, read.join_items[1].kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 10 }, read.join_items[1].left_tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 11, .end = 12 }, read.join_items[1].right_tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 13, .end = 16 }, read.join_items[1].predicate_tokens);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.join_items[1].predicate_expression.kind);
        },
        else => return error.TestUnexpectedResult,
    }

    const left_outer_joined_read_sql = "SELECT usage_records.id FROM usage_records LEFT OUTER JOIN accounts ON usage_records.account_id = accounts.id";
    const left_outer_joined_read_result = try parseSqlAlloc(alloc, left_outer_joined_read_sql);
    switch (left_outer_joined_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.join, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 12 }, read.source_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 12 }, read.join_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 7 }, read.join_operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlJoinKind.left, read.join_kind.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read.join_left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.join_right_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 12 }, read.join_predicate_tokens.?);
            try std.testing.expectEqual(@as(usize, 1), read.join_items.len);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 12 }, read.join_items[0].tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 7 }, read.join_items[0].operator_tokens);
            try std.testing.expectEqual(GeneratedSqlJoinKind.left, read.join_items[0].kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read.join_items[0].left_tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.join_items[0].right_tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 12 }, read.join_items[0].predicate_tokens);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.join_items[0].predicate_expression.kind);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.join_predicate_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read.join_predicate_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read.join_predicate_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 11, .end = 12 }, read.join_predicate_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const distinct_read_sql = "SELECT DISTINCT status FROM usage_records ORDER BY status";
    const distinct_read_result = try parseSqlAlloc(alloc, distinct_read_sql);
    switch (distinct_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.aggregate, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.distinct_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, read.projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 5 }, read.source_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.order_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const distinct_on_read_sql = "SELECT DISTINCT ON (organization_id) organization_id, id FROM usage_records ORDER BY organization_id ASC, created_at DESC";
    const distinct_on_read_result = try parseSqlAlloc(alloc, distinct_on_read_sql);
    switch (distinct_on_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 6 }, read.distinct_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 9 }, read.projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read.source_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const paginated_read_sql = "SELECT id FROM usage_records OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY";
    const paginated_read_result = try parseSqlAlloc(alloc, paginated_read_sql);
    switch (paginated_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read.source_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 7 }, read.offset_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.offset_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.offset_expression.tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 12 }, read.fetch_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read.fetch_count_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.fetch_count_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read.fetch_count_expression.tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const limit_all_read_sql = "SELECT id FROM usage_records ORDER BY id LIMIT ALL OFFSET 2 ROWS";
    const limit_all_read_result = try parseSqlAlloc(alloc, limit_all_read_sql);
    switch (limit_all_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read.source_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.order_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read.limit_tokens.?);
            try std.testing.expect(read.limit_all);
            try std.testing.expect(read.limit_expression.tokens == null);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 12 }, read.offset_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.offset_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read.offset_expression.tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const fetch_default_read_sql = "SELECT id FROM usage_records FETCH FIRST ROWS ONLY";
    const fetch_default_read_result = try parseSqlAlloc(alloc, fetch_default_read_sql);
    switch (fetch_default_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read.source_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read.fetch_tokens.?);
            try std.testing.expect(read.fetch_count_tokens == null);
            try std.testing.expect(read.fetch_count_expression.tokens == null);
        },
        else => return error.TestUnexpectedResult,
    }

    const window_read_sql = "SELECT id, row_number() OVER (ORDER BY id) AS rn FROM usage_records";
    const window_read_result = try parseSqlAlloc(alloc, window_read_sql);
    switch (window_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.window, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 14 }, read.projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 15, .end = 16 }, read.source_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const named_window_read_sql = "SELECT id, row_number() OVER usage_window AS rn FROM usage_records WINDOW usage_window AS (ORDER BY id)";
    const named_window_read_result = try parseSqlAlloc(alloc, named_window_read_sql);
    switch (named_window_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.window, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 10 }, read.projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 11, .end = 12 }, read.source_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 13, .end = 20 }, read.window_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const partitioned_window_read_sql = "SELECT id, row_number() OVER (PARTITION BY tenant ORDER BY id) AS rn FROM usage_records";
    const partitioned_window_read_result = try parseSqlAlloc(alloc, partitioned_window_read_sql);
    switch (partitioned_window_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.window, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 17 }, read.projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 18, .end = 19 }, read.source_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const partitioned_named_window_read_sql = "SELECT id, row_number() OVER usage_window AS rn FROM usage_records WINDOW usage_window AS (PARTITION BY tenant ORDER BY id)";
    const partitioned_named_window_read_result = try parseSqlAlloc(alloc, partitioned_named_window_read_sql);
    switch (partitioned_named_window_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.window, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 13, .end = 23 }, read.window_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const framed_window_read_sql = "SELECT id, row_number() OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rn FROM usage_records";
    const framed_window_read_result = try parseSqlAlloc(alloc, framed_window_read_sql);
    switch (framed_window_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.window, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 21 }, read.projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 22, .end = 23 }, read.source_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const framed_named_window_read_sql = "SELECT id, row_number() OVER usage_window AS rn FROM usage_records WINDOW usage_window AS (ORDER BY id RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)";
    const framed_named_window_read_result = try parseSqlAlloc(alloc, framed_named_window_read_sql);
    switch (framed_named_window_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.window, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 13, .end = 27 }, read.window_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const cte_read_sql = "WITH source_rows AS (SELECT id FROM usage_records) SELECT id FROM source_rows";
    const cte_read_result = try parseSqlAlloc(alloc, cte_read_sql);
    switch (cte_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.cte, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 9 }, read.cte_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 9 }, read.cte_list_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.cte_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 8 }, read.cte_body_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.cte_last_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 8 }, read.cte_last_body_tokens.?);
            try std.testing.expectEqual(@as(usize, 1), read.cte_count);
            try std.testing.expectEqual(@as(usize, 1), read.cte_items.len);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.cte_items[0].name_tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 8 }, read.cte_items[0].body_tokens.?);
            try std.testing.expect(!read.cte_recursive);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read.projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 12, .end = 13 }, read.source_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const multi_cte_read_sql = "WITH first_rows AS (SELECT id FROM usage_records), second_rows AS (SELECT id FROM first_rows) SELECT id FROM second_rows";
    const multi_cte_read_result = try parseSqlAlloc(alloc, multi_cte_read_sql);
    switch (multi_cte_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.cte, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 18 }, read.cte_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 18 }, read.cte_list_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.cte_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 8 }, read.cte_body_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read.cte_last_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 13, .end = 17 }, read.cte_last_body_tokens.?);
            try std.testing.expectEqual(@as(usize, 2), read.cte_count);
            try std.testing.expectEqual(@as(usize, 2), read.cte_items.len);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.cte_items[0].name_tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 8 }, read.cte_items[0].body_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read.cte_items[1].name_tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 13, .end = 17 }, read.cte_items[1].body_tokens.?);
            try std.testing.expect(!read.cte_recursive);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 19, .end = 20 }, read.projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 21, .end = 22 }, read.source_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const recursive_cte_read_sql = "WITH RECURSIVE source_rows AS (SELECT id FROM usage_records) SELECT id FROM source_rows";
    const recursive_cte_read_result = try parseSqlAlloc(alloc, recursive_cte_read_sql);
    switch (recursive_cte_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.cte, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 10 }, read.cte_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 10 }, read.cte_list_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, read.cte_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 9 }, read.cte_body_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, read.cte_last_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 9 }, read.cte_last_body_tokens.?);
            try std.testing.expectEqual(@as(usize, 1), read.cte_count);
            try std.testing.expectEqual(@as(usize, 1), read.cte_items.len);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, read.cte_items[0].name_tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 9 }, read.cte_items[0].body_tokens.?);
            try std.testing.expect(read.cte_recursive);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 11, .end = 12 }, read.projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 13, .end = 14 }, read.source_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const set_operation_read_sql = "SELECT id FROM usage_records UNION SELECT id FROM usage_archive";
    const set_operation_read_result = try parseSqlAlloc(alloc, set_operation_read_sql);
    switch (set_operation_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.set_operation, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read.source_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 9 }, read.set_operation_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const graph_sql = "CREATE GRAPH METRIC docs_pagerank ON doc_edges WITH (metric = 'pagerank')";
    const graph_result = try parseSqlAlloc(alloc, graph_sql);
    switch (graph_result.ast.?) {
        .graph => |graph| {
            try std.testing.expectEqual(GeneratedSqlGraphKind.create_metric, graph.kind);
            try std.testing.expectEqualStrings("CREATE GRAPH METRIC docs_pagerank ON doc_edges WITH (metric = 'pagerank')", spanText(graph_sql, graph.statement_span));
            try std.testing.expectEqualStrings("CREATE", spanText(graph_sql, graph.command_span));
        },
        else => return error.TestUnexpectedResult,
    }

    const analyze_sql = "ANALYZE";
    const analyze_result = try parseSqlAlloc(alloc, analyze_sql);
    switch (analyze_result.ast.?) {
        .unsupported => |unsupported| {
            try std.testing.expectEqual(GeneratedSqlUnsupportedKind.analyze, unsupported.kind);
            try std.testing.expectEqual(GeneratedSqlUnsupportedReason.analyze_not_planned_by_generated_parser, unsupported.reason);
            try std.testing.expectEqualStrings("ANALYZE", spanText(analyze_sql, unsupported.statement_span));
            try std.testing.expectEqualStrings("ANALYZE", spanText(analyze_sql, unsupported.command_span));
            try std.testing.expect(unsupported.subject_tokens == null);
        },
        else => return error.TestUnexpectedResult,
    }

    const explain_sql = "EXPLAIN SELECT id FROM usage_records";
    const explain_result = try parseSqlAlloc(alloc, explain_sql);
    switch (explain_result.ast.?) {
        .unsupported => |unsupported| {
            try std.testing.expectEqual(GeneratedSqlUnsupportedKind.explain, unsupported.kind);
            try std.testing.expectEqual(GeneratedSqlUnsupportedReason.explain_not_planned_by_generated_parser, unsupported.reason);
            try std.testing.expectEqualStrings("EXPLAIN SELECT id FROM usage_records", spanText(explain_sql, unsupported.statement_span));
            try std.testing.expectEqualStrings("EXPLAIN", spanText(explain_sql, unsupported.command_span));
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 5 }, unsupported.subject_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "generated SQL parser reports source-aware diagnostics" {
    var tokens = try lexer.tokenizeAlloc(std.testing.allocator, "CREATE TABLE usage_records (id)");
    defer lexer.freeTokens(std.testing.allocator, &tokens);
    const diagnostic = try diagnosticAlloc(std.testing.allocator, tokens.items) orelse return error.ExpectedDiagnostic;
    defer std.testing.allocator.free(diagnostic.expected);
    try std.testing.expect(diagnostic.expected.len > 0);
    try std.testing.expectEqualStrings(")", diagnostic.actual);
    try std.testing.expect(diagnostic.source_end >= diagnostic.source_start);
}

test "generated SQL parser reports bounded diagnostics for malformed corpus" {
    const cases = [_][]const u8{
        "SELECT id FROM",
        "SELECT id FROM usage_records WHERE",
        "WITH source_rows AS (SELECT id FROM usage_records SELECT id FROM source_rows",
        "CREATE TABLE usage_records (id text",
        "INSERT INTO usage_records (id VALUES ('u1')",
        "EXPLAIN",
    };

    for (cases) |sql| {
        var tokens = try lexer.tokenizeAlloc(std.testing.allocator, sql);
        defer lexer.freeTokens(std.testing.allocator, &tokens);
        const diagnostic = try diagnosticAlloc(std.testing.allocator, tokens.items) orelse return error.ExpectedDiagnostic;
        defer std.testing.allocator.free(diagnostic.expected);
        try std.testing.expect(diagnostic.token_index <= tokens.items.len);
        try std.testing.expect(diagnostic.source_end >= diagnostic.source_start);
        try std.testing.expect(diagnostic.expected.len > 0);
    }
}

test "generated SQL parser rejects unsupported token shapes" {
    try std.testing.expectError(error.UnsupportedSqlShape, parseSqlAlloc(std.testing.allocator, "SELECT a @> b"));
}

fn spanText(sql: []const u8, span: token_mod.SourceSpan) []const u8 {
    return sql[span.start..span.end];
}
