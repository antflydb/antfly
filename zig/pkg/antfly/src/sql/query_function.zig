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

const db_mod = @import("../storage/db/mod.zig");
const generated_parser = @import("generated_parser.zig");
const query_contract = @import("../api/query_contract.zig");
const lexer_mod = @import("lexer.zig");
const token_mod = @import("token.zig");
const tokenized = @import("tokenized.zig");

const Token = token_mod.Token;
const TokenKeyword = token_mod.TokenKeyword;
const TokenKind = token_mod.TokenKind;

pub const AntflyQueryFunction = enum {
    full_text_search,
    semantic_search,
    vector_search,
    graph_traverse,
    graph_neighbors,
    graph_shortest_path,
    graph_k_shortest_paths,
    graph_match,
    graph_metric,
    graph_metric_rerank,
    hybrid_search,
};

fn antflyQueryFunctionFromKeyword(keyword: TokenKeyword) ?AntflyQueryFunction {
    return switch (keyword) {
        .full_text_search => .full_text_search,
        .semantic_search => .semantic_search,
        .vector_search => .vector_search,
        .graph_traverse => .graph_traverse,
        .graph_neighbors => .graph_neighbors,
        .graph_shortest_path => .graph_shortest_path,
        .graph_k_shortest_paths => .graph_k_shortest_paths,
        .graph_match => .graph_match,
        .graph_metric => .graph_metric,
        .graph_metric_rerank => .graph_metric_rerank,
        .hybrid_search => .hybrid_search,
        else => null,
    };
}

fn antflyQueryFunctionFromGeneratedKind(kind: generated_parser.GeneratedSqlAntflyTableFunctionKind) AntflyQueryFunction {
    return switch (kind) {
        .full_text_search => .full_text_search,
        .semantic_search => .semantic_search,
        .vector_search => .vector_search,
        .graph_traverse => .graph_traverse,
        .graph_neighbors => .graph_neighbors,
        .graph_shortest_path => .graph_shortest_path,
        .graph_k_shortest_paths => .graph_k_shortest_paths,
        .graph_match => .graph_match,
        .graph_metric => .graph_metric,
        .graph_metric_rerank => .graph_metric_rerank,
        .hybrid_search => .hybrid_search,
    };
}

fn generatedGraphTableFunctionKindFromAntfly(kind: generated_parser.GeneratedSqlAntflyTableFunctionKind) ?generated_parser.GeneratedSqlGraphTableFunctionKind {
    return switch (kind) {
        .graph_traverse => .traverse,
        .graph_neighbors => .neighbors,
        .graph_shortest_path => .shortest_path,
        .graph_k_shortest_paths => .k_shortest_paths,
        .graph_match => .match,
        .graph_metric => .metric,
        .graph_metric_rerank => .metric_rerank,
        else => null,
    };
}

pub const SqlQueryFunctionArgValue = union(enum) {
    string: []const u8,
    number: []const u8,
    boolean: bool,
    source_specs: []const HybridSqlSourceSpec,
};

pub const SqlQueryFunctionArg = struct {
    name: []const u8,
    value: SqlQueryFunctionArgValue,
};

const HybridSqlSourceSpec = struct {
    index: []const u8,
    kind: []const u8,
    name: ?[]const u8 = null,
    field: ?[]const u8 = null,
    metric: ?[]const u8 = null,
    freshness: ?[]const u8 = null,
    weight: ?[]const u8 = null,
    base_weight: ?[]const u8 = null,
    missing_score: ?[]const u8 = null,
};

fn deinitAntflyQueryFunctionArgs(alloc: std.mem.Allocator, args: []const SqlQueryFunctionArg) void {
    for (args) |arg| {
        switch (arg.value) {
            .source_specs => |sources| alloc.free(sources),
            else => {},
        }
    }
}

pub fn parseAntflyQueryFunctionCall(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    args: *std.ArrayListUnmanaged(SqlQueryFunctionArg),
) !AntflyQueryFunction {
    try expectSqlKeyword(tokens, pos, .select);
    _ = try expectSqlToken(tokens, pos, .star);
    try expectSqlKeyword(tokens, pos, .from);
    const function = try parseAntflyQueryFunctionExpressionAlloc(alloc, tokens, pos, args);
    _ = matchSqlToken(tokens, pos, .semicolon);
    if (pos.* != tokens.len) return error.UnsupportedSqlShape;
    return function;
}

pub fn parseAntflyQueryFunctionReadCall(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    args: *std.ArrayListUnmanaged(SqlQueryFunctionArg),
    projection_columns: *std.ArrayListUnmanaged([]const u8),
) !AntflyQueryFunction {
    try expectSqlKeyword(tokens, pos, .select);
    if (matchSqlToken(tokens, pos, .star) == null) {
        while (true) {
            const column = try expectSqlToken(tokens, pos, .identifier);
            try projection_columns.append(alloc, column.text);
            if (matchSqlToken(tokens, pos, .comma) == null) break;
        }
    }
    try expectSqlKeyword(tokens, pos, .from);
    const function = try parseAntflyQueryFunctionExpressionAlloc(alloc, tokens, pos, args);
    _ = matchSqlToken(tokens, pos, .semicolon);
    if (pos.* != tokens.len) return error.UnsupportedSqlShape;
    return function;
}

pub fn parseAntflyQueryFunctionExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    args: *std.ArrayListUnmanaged(SqlQueryFunctionArg),
) !AntflyQueryFunction {
    const function_token = try expectSqlToken(tokens, pos, .identifier);
    const function = antflyQueryFunctionFromSqlToken(function_token) orelse return error.UnsupportedSqlShape;
    _ = try expectSqlToken(tokens, pos, .lparen);
    if (matchSqlToken(tokens, pos, .rparen) == null) {
        while (true) {
            const name_token = try expectSqlToken(tokens, pos, .identifier);
            const name = name_token.text;
            _ = try expectSqlToken(tokens, pos, .eq);
            _ = matchSqlToken(tokens, pos, .gt);
            const value = try parseAntflyQueryFunctionArgValueAlloc(alloc, tokens, pos, name_token);
            if (antflyQueryFunctionArg(args.items, name) != null) return error.UnsupportedSqlShape;
            try args.append(alloc, .{ .name = name, .value = value });
            if (matchSqlToken(tokens, pos, .comma) == null) break;
        }
        _ = try expectSqlToken(tokens, pos, .rparen);
    }
    return function;
}

fn parseAntflyQueryFunctionArgValueAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    name_token: Token,
) !SqlQueryFunctionArgValue {
    if (name_token.matchesKeywordTag(.sources) and antflySourcesArrayCanStart(tokens, pos.*)) {
        return .{ .source_specs = try parseAntflySourceArrayAlloc(alloc, tokens, pos) };
    }
    const value_token = if (matchSqlToken(tokens, pos, .string)) |token|
        token
    else if (matchSqlToken(tokens, pos, .number)) |token|
        token
    else
        try expectSqlToken(tokens, pos, .identifier);
    return switch (value_token.kind) {
        .string => .{ .string = value_token.text },
        .number => .{ .number = value_token.text },
        .identifier => blk: {
            if (value_token.matchesKeywordTag(.true)) break :blk .{ .boolean = true };
            if (value_token.matchesKeywordTag(.false)) break :blk .{ .boolean = false };
            break :blk .{ .string = value_token.text };
        },
        else => unreachable,
    };
}

fn antflySourcesArrayCanStart(tokens: []const Token, pos: usize) bool {
    if (pos >= tokens.len) return false;
    return tokens[pos].kind == .lbracket or
        tokens[pos].matchesKeywordTag(.array);
}

fn parseAntflySourceArrayAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) ![]const HybridSqlSourceSpec {
    if (matchSqlKeyword(tokens, pos, .array)) |_| {}
    _ = try expectSqlToken(tokens, pos, .lbracket);
    var sources = std.ArrayListUnmanaged(HybridSqlSourceSpec).empty;
    errdefer sources.deinit(alloc);
    if (matchSqlToken(tokens, pos, .rbracket) == null) {
        while (true) {
            try sources.append(alloc, try parseAntflySourceSpec(tokens, pos));
            if (matchSqlToken(tokens, pos, .comma) == null) break;
        }
        _ = try expectSqlToken(tokens, pos, .rbracket);
    }
    if (sources.items.len == 0) return error.UnsupportedSqlShape;
    return try sources.toOwnedSlice(alloc);
}

fn parseAntflySourceSpec(tokens: []const Token, pos: *usize) !HybridSqlSourceSpec {
    const function_token = try expectSqlToken(tokens, pos, .identifier);
    if (!antflySourceFunctionToken(function_token)) return error.UnsupportedSqlShape;
    _ = try expectSqlToken(tokens, pos, .lparen);
    const index = try parseAntflySourceStringLikeValue(tokens, pos);
    var source = HybridSqlSourceSpec{
        .index = index,
        .kind = "",
    };
    while (matchSqlToken(tokens, pos, .comma) != null) {
        const field_token = try expectSqlToken(tokens, pos, .identifier);
        _ = try expectSqlToken(tokens, pos, .eq);
        _ = matchSqlToken(tokens, pos, .gt);
        const field = antflySourceFieldFromToken(field_token) orelse return error.UnsupportedSqlShape;
        switch (field) {
            .kind => {
                if (source.kind.len != 0) return error.UnsupportedSqlShape;
                source.kind = try parseAntflySourceStringLikeValue(tokens, pos);
            },
            .name => {
                if (source.name != null) return error.UnsupportedSqlShape;
                source.name = try parseAntflySourceStringLikeValue(tokens, pos);
            },
            .field => {
                if (source.field != null) return error.UnsupportedSqlShape;
                source.field = try parseAntflySourceStringLikeValue(tokens, pos);
            },
            .metric => {
                if (source.metric != null) return error.UnsupportedSqlShape;
                source.metric = try parseAntflySourceStringLikeValue(tokens, pos);
            },
            .freshness => {
                if (source.freshness != null) return error.UnsupportedSqlShape;
                source.freshness = try parseAntflySourceStringLikeValue(tokens, pos);
            },
            .weight => {
                if (source.weight != null) return error.UnsupportedSqlShape;
                source.weight = try parseAntflySourceNumberValue(tokens, pos);
            },
            .base_weight => {
                if (source.base_weight != null) return error.UnsupportedSqlShape;
                source.base_weight = try parseAntflySourceNumberValue(tokens, pos);
            },
            .missing_score => {
                if (source.missing_score != null) return error.UnsupportedSqlShape;
                source.missing_score = try parseAntflySourceNumberValue(tokens, pos);
            },
        }
    }
    _ = try expectSqlToken(tokens, pos, .rparen);
    if (source.kind.len == 0) {
        source.kind = if (source.metric != null)
            "graph_metric"
        else if (source.field != null)
            "full_text"
        else
            "semantic";
    }
    return source;
}

fn parseAntflySourceStringLikeValue(tokens: []const Token, pos: *usize) ![]const u8 {
    if (matchSqlToken(tokens, pos, .string)) |token| return token.text;
    return (try expectSqlToken(tokens, pos, .identifier)).text;
}

fn parseAntflySourceNumberValue(tokens: []const Token, pos: *usize) ![]const u8 {
    return (try expectSqlToken(tokens, pos, .number)).text;
}

pub fn antflyQueryFunctionFromSqlName(name: []const u8) ?AntflyQueryFunction {
    const dot = std.mem.indexOfScalar(u8, name, '.');
    const local = if (dot) |idx| blk: {
        if (std.mem.indexOfScalar(u8, name[idx + 1 ..], '.') != null) return null;
        if (!std.ascii.eqlIgnoreCase(name[0..idx], "antfly")) return null;
        break :blk name[idx + 1 ..];
    } else name;
    const keyword = token_mod.keywordFromIdentifier(local) orelse return null;
    return antflyQueryFunctionFromKeyword(keyword);
}

pub fn antflyQueryFunctionFromSqlToken(token: Token) ?AntflyQueryFunction {
    if (token.kind != .identifier) return null;
    if (token.keyword) |keyword| if (antflyQueryFunctionFromKeyword(keyword)) |function| return function;
    return antflyQueryFunctionFromSqlName(token.text);
}

fn antflySourceFunctionToken(token: Token) bool {
    return token.matchesQualifiedKeywordTag("antfly", .source);
}

const AntflySourceField = enum {
    kind,
    name,
    field,
    metric,
    freshness,
    weight,
    base_weight,
    missing_score,
};

fn antflySourceFieldFromToken(token: Token) ?AntflySourceField {
    if (token.matchesKeywordTag(.kind) or token.matchesKeywordTag(.type)) return .kind;
    if (token.matchesKeywordTag(.name)) return .name;
    if (token.matchesKeywordTag(.field)) return .field;
    if (token.matchesKeywordTag(.metric) or token.matchesKeywordTag(.graph_metric)) return .metric;
    if (token.matchesKeywordTag(.freshness) or token.matchesKeywordTag(.metric_freshness)) return .freshness;
    if (token.matchesKeywordTag(.weight)) return .weight;
    if (token.matchesKeywordTag(.base_weight)) return .base_weight;
    if (token.matchesKeywordTag(.missing_score)) return .missing_score;
    return null;
}

fn expectSqlKeyword(tokens: []const Token, pos: *usize, keyword: TokenKeyword) !void {
    const token = try expectSqlToken(tokens, pos, .identifier);
    if (!token.matchesKeywordTag(keyword)) return error.UnsupportedSqlShape;
}

fn expectSqlToken(tokens: []const Token, pos: *usize, kind: TokenKind) !Token {
    return matchSqlToken(tokens, pos, kind) orelse error.UnsupportedSqlShape;
}

fn matchSqlToken(tokens: []const Token, pos: *usize, kind: TokenKind) ?Token {
    if (pos.* >= tokens.len or tokens[pos.*].kind != kind) return null;
    const token = tokens[pos.*];
    pos.* += 1;
    return token;
}

fn matchSqlKeyword(tokens: []const Token, pos: *usize, keyword: TokenKeyword) ?Token {
    if (pos.* >= tokens.len or tokens[pos.*].kind != .identifier) return null;
    if (!tokens[pos.*].matchesKeywordTag(keyword)) return null;
    const token = tokens[pos.*];
    pos.* += 1;
    return token;
}

pub fn antflyQueryFunctionArg(args: []const SqlQueryFunctionArg, name: []const u8) ?SqlQueryFunctionArgValue {
    for (args) |arg| {
        if (std.ascii.eqlIgnoreCase(arg.name, name)) return arg.value;
    }
    return null;
}

pub fn antflyQueryFunctionStringArg(args: []const SqlQueryFunctionArg, name: []const u8) ?[]const u8 {
    const value = antflyQueryFunctionArg(args, name) orelse return null;
    return switch (value) {
        .string => |text| text,
        .number => |text| text,
        .boolean => null,
        .source_specs => null,
    };
}

pub fn antflyQueryFunctionNumberArg(args: []const SqlQueryFunctionArg, name: []const u8) ?[]const u8 {
    const value = antflyQueryFunctionArg(args, name) orelse return null;
    return switch (value) {
        .number => |text| text,
        else => null,
    };
}

pub fn antflyQueryFunctionBoolArg(args: []const SqlQueryFunctionArg, name: []const u8) ?bool {
    const value = antflyQueryFunctionArg(args, name) orelse return null;
    return switch (value) {
        .boolean => |value_bool| value_bool,
        else => null,
    };
}

fn antflyQueryFunctionSourcesArg(args: []const SqlQueryFunctionArg, name: []const u8) ?[]const HybridSqlSourceSpec {
    const value = antflyQueryFunctionArg(args, name) orelse return null;
    return switch (value) {
        .source_specs => |sources| sources,
        else => null,
    };
}

pub fn requireAntflyQueryFunctionStringArg(args: []const SqlQueryFunctionArg, name: []const u8) ![]const u8 {
    const value = antflyQueryFunctionStringArg(args, name) orelse return error.UnsupportedSqlShape;
    if (value.len == 0) return error.UnsupportedSqlShape;
    return value;
}

pub fn lowerAntflyQueryFunctionSqlAlloc(
    alloc: std.mem.Allocator,
    semantic_resolver: ?query_contract.SemanticResolver,
    sql: []const u8,
) !query_contract.OwnedQueryRequest {
    var parsed_sql = try tokenized.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerAntflyQueryFunctionParsedSqlAlloc(alloc, semantic_resolver, &parsed_sql);
}

pub fn lowerAntflyQueryFunctionParsedSqlAlloc(
    alloc: std.mem.Allocator,
    semantic_resolver: ?query_contract.SemanticResolver,
    parsed_sql: *const tokenized.ParsedSql,
) !query_contract.OwnedQueryRequest {
    var args = std.ArrayListUnmanaged(SqlQueryFunctionArg).empty;
    defer {
        deinitAntflyQueryFunctionArgs(alloc, args.items);
        args.deinit(alloc);
    }
    var pos: usize = 0;
    const function = try parseAntflyQueryFunctionCall(alloc, parsed_sql.items(), &pos, &args);

    return try lowerParsedAntflyQueryFunctionAlloc(alloc, semantic_resolver, function, args.items);
}

pub const LoweredAntflyQueryFunctionRead = struct {
    table_name: []const u8,
    projection_columns: []const []const u8 = &.{},
    request: query_contract.OwnedQueryRequest,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.table_name));
        for (self.projection_columns) |column| alloc.free(@constCast(column));
        if (self.projection_columns.len > 0) alloc.free(@constCast(self.projection_columns));
        self.request.deinit(alloc);
        self.* = undefined;
    }
};

pub fn lowerAntflyQueryFunctionReadParsedSqlAlloc(
    alloc: std.mem.Allocator,
    semantic_resolver: ?query_contract.SemanticResolver,
    parsed_sql: *const tokenized.ParsedSql,
) !LoweredAntflyQueryFunctionRead {
    var args = std.ArrayListUnmanaged(SqlQueryFunctionArg).empty;
    defer {
        deinitAntflyQueryFunctionArgs(alloc, args.items);
        args.deinit(alloc);
    }
    var projection_columns = std.ArrayListUnmanaged([]const u8).empty;
    defer projection_columns.deinit(alloc);
    var pos: usize = 0;
    const function = try parseAntflyQueryFunctionReadCall(alloc, parsed_sql.items(), &pos, &args, &projection_columns);
    const table_name = antflyQueryFunctionStringArg(args.items, "table_name") orelse
        antflyQueryFunctionStringArg(args.items, "table") orelse return error.UnsupportedSqlShape;
    if (table_name.len == 0) return error.UnsupportedSqlShape;

    const owned_table_name = try alloc.dupe(u8, table_name);
    errdefer alloc.free(owned_table_name);
    const owned_projection_columns = try ownAntflyQueryFunctionProjectionColumnsAlloc(alloc, projection_columns.items);
    errdefer freeAntflyQueryFunctionProjectionColumns(alloc, owned_projection_columns);
    var request = try lowerParsedAntflyQueryFunctionAlloc(alloc, semantic_resolver, function, args.items);
    errdefer request.deinit(alloc);
    return .{
        .table_name = owned_table_name,
        .projection_columns = owned_projection_columns,
        .request = request,
    };
}

pub fn antflyQueryFunctionReadTableNameAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
) ![]const u8 {
    var args = std.ArrayListUnmanaged(SqlQueryFunctionArg).empty;
    defer {
        deinitAntflyQueryFunctionArgs(alloc, args.items);
        args.deinit(alloc);
    }
    var projection_columns = std.ArrayListUnmanaged([]const u8).empty;
    defer projection_columns.deinit(alloc);
    var pos: usize = 0;
    _ = try parseAntflyQueryFunctionReadCall(alloc, parsed_sql.items(), &pos, &args, &projection_columns);
    const table_name = antflyQueryFunctionStringArg(args.items, "table_name") orelse
        antflyQueryFunctionStringArg(args.items, "table") orelse return error.UnsupportedSqlShape;
    if (table_name.len == 0) return error.UnsupportedSqlShape;
    return try alloc.dupe(u8, table_name);
}

fn ownAntflyQueryFunctionProjectionColumnsAlloc(
    alloc: std.mem.Allocator,
    columns: []const []const u8,
) ![]const []const u8 {
    if (columns.len == 0) return &.{};
    const owned = try alloc.alloc([]const u8, columns.len);
    errdefer alloc.free(owned);
    var initialized: usize = 0;
    errdefer {
        for (owned[0..initialized]) |column| alloc.free(@constCast(column));
    }
    for (columns, 0..) |column, i| {
        owned[i] = try alloc.dupe(u8, column);
        initialized += 1;
    }
    return owned;
}

fn freeAntflyQueryFunctionProjectionColumns(
    alloc: std.mem.Allocator,
    columns: []const []const u8,
) void {
    for (columns) |column| alloc.free(@constCast(column));
    if (columns.len > 0) alloc.free(@constCast(columns));
}

pub fn lowerAntflyQueryFunctionExpressionSqlAlloc(
    alloc: std.mem.Allocator,
    semantic_resolver: ?query_contract.SemanticResolver,
    sql: []const u8,
) !query_contract.OwnedQueryRequest {
    var parsed_sql = try tokenized.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerAntflyQueryFunctionExpressionParsedSqlAlloc(alloc, semantic_resolver, &parsed_sql);
}

pub fn lowerAntflyQueryFunctionExpressionParsedSqlAlloc(
    alloc: std.mem.Allocator,
    semantic_resolver: ?query_contract.SemanticResolver,
    parsed_sql: *const tokenized.ParsedSql,
) !query_contract.OwnedQueryRequest {
    var args = std.ArrayListUnmanaged(SqlQueryFunctionArg).empty;
    defer {
        deinitAntflyQueryFunctionArgs(alloc, args.items);
        args.deinit(alloc);
    }
    var pos: usize = 0;
    const function = try parseAntflyQueryFunctionExpressionAlloc(alloc, parsed_sql.items(), &pos, &args);
    _ = matchSqlToken(parsed_sql.items(), &pos, .semicolon);
    if (pos != parsed_sql.items().len) return error.UnsupportedSqlShape;

    return try lowerParsedAntflyQueryFunctionAlloc(alloc, semantic_resolver, function, args.items);
}

pub fn lowerAntflyGraphTableFunctionTokensAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
) !db_mod.types.RelationalRowsTableFunction {
    var args = std.ArrayListUnmanaged(SqlQueryFunctionArg).empty;
    defer {
        deinitAntflyQueryFunctionArgs(alloc, args.items);
        args.deinit(alloc);
    }
    var pos: usize = 0;
    const function = try parseAntflyQueryFunctionCall(alloc, tokens, &pos, &args);
    if (function != .graph_traverse and
        function != .graph_neighbors and
        function != .graph_shortest_path and
        function != .graph_k_shortest_paths and
        function != .graph_match and
        function != .graph_metric and
        function != .graph_metric_rerank)
    {
        return error.UnsupportedSqlShape;
    }

    const table_name = antflyQueryFunctionStringArg(args.items, "table_name") orelse
        antflyQueryFunctionStringArg(args.items, "table") orelse return error.UnsupportedSqlShape;
    var lowered = try lowerParsedAntflyQueryFunctionAlloc(alloc, null, function, args.items);
    defer lowered.deinit(alloc);
    if (lowered.req.dense != null or lowered.req.sparse != null or lowered.req.merge_config != null) {
        return error.UnsupportedSqlShape;
    }

    const owned_table_name = try alloc.dupe(u8, table_name);
    errdefer alloc.free(owned_table_name);
    switch (function) {
        .graph_traverse, .graph_neighbors, .graph_shortest_path, .graph_k_shortest_paths, .graph_match => {
            if (lowered.req.full_text != null or lowered.req.graph_metric_rerank != null) return error.UnsupportedSqlShape;
            if (lowered.req.graph_queries.len != 1 or lowered.req.graph_metric_queries.len != 0) return error.UnsupportedSqlShape;
            const graph_queries = lowered.req.graph_queries;
            const graph_query = graph_queries[0];
            lowered.req.graph_queries = &.{};
            if (graph_queries.len > 0) alloc.free(@constCast(graph_queries));
            return .{ .graph_query = .{
                .table_name = owned_table_name,
                .query = graph_query,
            } };
        },
        .graph_metric => {
            if (lowered.req.full_text != null or lowered.req.graph_metric_rerank != null) return error.UnsupportedSqlShape;
            if (lowered.req.graph_metric_queries.len != 1 or lowered.req.graph_queries.len != 0) return error.UnsupportedSqlShape;
            const graph_metric_queries = lowered.req.graph_metric_queries;
            const graph_metric_query = graph_metric_queries[0];
            lowered.req.graph_metric_queries = &.{};
            if (graph_metric_queries.len > 0) alloc.free(@constCast(graph_metric_queries));
            return .{ .graph_metric_query = .{
                .table_name = owned_table_name,
                .query = graph_metric_query,
            } };
        },
        .graph_metric_rerank => {
            if (lowered.req.full_text == null or lowered.req.graph_metric_rerank == null) return error.UnsupportedSqlShape;
            if (lowered.req.graph_queries.len != 0 or lowered.req.graph_metric_queries.len != 0) return error.UnsupportedSqlShape;
            const request = lowered.req;
            lowered.req = .{};
            return .{ .graph_metric_rerank_query = .{
                .table_name = owned_table_name,
                .request = request,
            } };
        },
        else => return error.UnsupportedSqlShape,
    }
}

pub fn lowerAntflyGraphTableFunctionGeneratedAstAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    antfly_item: generated_parser.GeneratedSqlAntflyTableFunctionAst,
    graph_item: generated_parser.GeneratedSqlGraphTableFunctionAst,
) !db_mod.types.RelationalRowsTableFunction {
    if (!std.meta.eql(antfly_item.tokens, graph_item.tokens) or
        !std.meta.eql(antfly_item.name_tokens, graph_item.name_tokens) or
        !std.meta.eql(antfly_item.argument_tokens, graph_item.argument_tokens))
    {
        return error.UnsupportedSqlShape;
    }
    const expected_graph_kind = generatedGraphTableFunctionKindFromAntfly(antfly_item.kind) orelse return error.UnsupportedSqlShape;
    if (graph_item.kind != expected_graph_kind) return error.UnsupportedSqlShape;

    var args = std.ArrayListUnmanaged(SqlQueryFunctionArg).empty;
    defer args.deinit(alloc);
    try args.ensureTotalCapacity(alloc, antfly_item.argument_items.len);
    for (antfly_item.argument_items) |argument| {
        if (argument.name_tokens.end != argument.name_tokens.start + 1 or argument.name_tokens.end > tokens.len) return error.UnsupportedSqlShape;
        args.appendAssumeCapacity(.{
            .name = tokens[argument.name_tokens.start].text,
            .value = try generatedAntflyQueryFunctionArgValue(tokens, argument.value_tokens),
        });
    }

    const function = antflyQueryFunctionFromGeneratedKind(antfly_item.kind);
    if (function != .graph_traverse and
        function != .graph_neighbors and
        function != .graph_shortest_path and
        function != .graph_k_shortest_paths and
        function != .graph_match and
        function != .graph_metric and
        function != .graph_metric_rerank)
    {
        return error.UnsupportedSqlShape;
    }

    const table_name = antflyQueryFunctionStringArg(args.items, "table_name") orelse
        antflyQueryFunctionStringArg(args.items, "table") orelse return error.UnsupportedSqlShape;
    var lowered = try lowerParsedAntflyQueryFunctionAlloc(alloc, null, function, args.items);
    defer lowered.deinit(alloc);
    if (lowered.req.dense != null or lowered.req.sparse != null or lowered.req.merge_config != null) {
        return error.UnsupportedSqlShape;
    }

    const owned_table_name = try alloc.dupe(u8, table_name);
    errdefer alloc.free(owned_table_name);
    switch (function) {
        .graph_traverse, .graph_neighbors, .graph_shortest_path, .graph_k_shortest_paths, .graph_match => {
            if (lowered.req.full_text != null or lowered.req.graph_metric_rerank != null) return error.UnsupportedSqlShape;
            if (lowered.req.graph_queries.len != 1 or lowered.req.graph_metric_queries.len != 0) return error.UnsupportedSqlShape;
            const graph_queries = lowered.req.graph_queries;
            const graph_query = graph_queries[0];
            lowered.req.graph_queries = &.{};
            if (graph_queries.len > 0) alloc.free(@constCast(graph_queries));
            return .{ .graph_query = .{
                .table_name = owned_table_name,
                .query = graph_query,
            } };
        },
        .graph_metric => {
            if (lowered.req.full_text != null or lowered.req.graph_metric_rerank != null) return error.UnsupportedSqlShape;
            if (lowered.req.graph_metric_queries.len != 1 or lowered.req.graph_queries.len != 0) return error.UnsupportedSqlShape;
            const graph_metric_queries = lowered.req.graph_metric_queries;
            const graph_metric_query = graph_metric_queries[0];
            lowered.req.graph_metric_queries = &.{};
            if (graph_metric_queries.len > 0) alloc.free(@constCast(graph_metric_queries));
            return .{ .graph_metric_query = .{
                .table_name = owned_table_name,
                .query = graph_metric_query,
            } };
        },
        .graph_metric_rerank => {
            if (lowered.req.full_text == null or lowered.req.graph_metric_rerank == null) return error.UnsupportedSqlShape;
            if (lowered.req.graph_queries.len != 0 or lowered.req.graph_metric_queries.len != 0) return error.UnsupportedSqlShape;
            const request = lowered.req;
            lowered.req = .{};
            return .{ .graph_metric_rerank_query = .{
                .table_name = owned_table_name,
                .request = request,
            } };
        },
        else => return error.UnsupportedSqlShape,
    }
}

fn generatedAntflyQueryFunctionArgValue(
    tokens: []const Token,
    range: generated_parser.GeneratedSqlTokenRange,
) !SqlQueryFunctionArgValue {
    if (range.end != range.start + 1 or range.end > tokens.len) return error.UnsupportedSqlShape;
    const value_token = tokens[range.start];
    return switch (value_token.kind) {
        .string => .{ .string = value_token.text },
        .number => .{ .number = value_token.text },
        .identifier => blk: {
            if (value_token.matchesKeywordTag(.true)) break :blk .{ .boolean = true };
            if (value_token.matchesKeywordTag(.false)) break :blk .{ .boolean = false };
            break :blk .{ .string = value_token.text };
        },
        else => error.UnsupportedSqlShape,
    };
}

fn lowerParsedAntflyQueryFunctionAlloc(
    alloc: std.mem.Allocator,
    semantic_resolver: ?query_contract.SemanticResolver,
    function: AntflyQueryFunction,
    args: []const SqlQueryFunctionArg,
) !query_contract.OwnedQueryRequest {
    const table_name = antflyQueryFunctionStringArg(args, "table_name") orelse
        antflyQueryFunctionStringArg(args, "table") orelse return error.UnsupportedSqlShape;
    const structured_primary_text_index_name = if (function == .hybrid_search)
        try hybridSourcesPrimaryTextIndexAlloc(alloc, args)
    else
        null;
    defer if (structured_primary_text_index_name) |index_name| alloc.free(index_name);
    const primary_text_index_name = antflyQueryFunctionStringArg(args, "full_text_index") orelse
        antflyQueryFunctionStringArg(args, "text_index") orelse
        if (function == .full_text_search) antflyQueryFunctionStringArg(args, "index") else structured_primary_text_index_name;

    var body = std.ArrayListUnmanaged(u8).empty;
    defer body.deinit(alloc);
    try body.append(alloc, '{');
    var first = true;
    switch (function) {
        .full_text_search => try appendFullTextFunctionBody(alloc, &body, &first, args),
        .semantic_search => try appendSemanticFunctionBody(alloc, &body, &first, args),
        .vector_search => try appendVectorFunctionBody(alloc, &body, &first, args),
        .graph_traverse, .graph_neighbors, .graph_shortest_path, .graph_k_shortest_paths => try appendGraphSearchFunctionBody(alloc, &body, &first, function, args),
        .graph_match => try appendGraphMatchFunctionBody(alloc, &body, &first, args),
        .graph_metric => try appendGraphMetricFunctionBody(alloc, &body, &first, args),
        .graph_metric_rerank => try appendGraphMetricRerankFunctionBody(alloc, &body, &first, args),
        .hybrid_search => try appendHybridFunctionBody(alloc, &body, &first, args),
    }
    try appendCommonAntflyQueryFunctionOptions(alloc, &body, &first, args);
    try body.append(alloc, '}');

    var lowered = try query_contract.parseQueryRequest(alloc, semantic_resolver, table_name, body.items);
    errdefer lowered.deinit(alloc);
    if (primary_text_index_name) |index_name| {
        if (lowered.req.primary_text_index_name != null) return error.UnsupportedSqlShape;
        lowered.req.primary_text_index_name = try alloc.dupe(u8, index_name);
    }
    return lowered;
}

fn appendAntflySqlJsonString(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    value: []const u8,
) !void {
    const escaped = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
    defer alloc.free(escaped);
    try out.appendSlice(alloc, escaped);
}

fn appendAntflySqlJsonFieldName(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
) !void {
    if (!first.*) try out.append(alloc, ',');
    first.* = false;
    try appendAntflySqlJsonString(alloc, out, name);
    try out.append(alloc, ':');
}

fn appendAntflySqlJsonStringField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    value: []const u8,
) !void {
    try appendAntflySqlJsonFieldName(alloc, out, first, name);
    try appendAntflySqlJsonString(alloc, out, value);
}

fn appendAntflySqlJsonNumberField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    value: []const u8,
) !void {
    try appendAntflySqlJsonFieldName(alloc, out, first, name);
    try out.appendSlice(alloc, value);
}

fn appendAntflySqlJsonBoolField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    value: bool,
) !void {
    try appendAntflySqlJsonFieldName(alloc, out, first, name);
    try out.appendSlice(alloc, if (value) "true" else "false");
}

fn appendAntflySqlJsonStringArrayField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    values: []const []const u8,
) !void {
    try appendAntflySqlJsonFieldName(alloc, out, first, name);
    try out.append(alloc, '[');
    for (values, 0..) |value, i| {
        if (i > 0) try out.append(alloc, ',');
        try appendAntflySqlJsonString(alloc, out, value);
    }
    try out.append(alloc, ']');
}

fn appendAntflySqlJsonCommaStringArrayField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    csv: []const u8,
) !void {
    try appendAntflySqlJsonFieldName(alloc, out, first, name);
    try out.append(alloc, '[');
    var split = std.mem.splitScalar(u8, csv, ',');
    var item_index: usize = 0;
    while (split.next()) |raw_item| {
        const item = std.mem.trim(u8, raw_item, " \t\r\n");
        if (item.len == 0) return error.UnsupportedSqlShape;
        if (item_index > 0) try out.append(alloc, ',');
        try appendAntflySqlJsonString(alloc, out, item);
        item_index += 1;
    }
    if (item_index == 0) return error.UnsupportedSqlShape;
    try out.append(alloc, ']');
}

fn appendFullTextFunctionBody(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    args: []const SqlQueryFunctionArg,
) !void {
    const query = requireAntflyQueryFunctionStringArg(args, "query") catch try requireAntflyQueryFunctionStringArg(args, "text");
    try appendAntflySqlJsonFieldName(alloc, out, first, "full_text_search");
    try out.append(alloc, '{');
    var inner_first = true;
    if (antflyQueryFunctionStringArg(args, "field")) |field| {
        try appendAntflySqlJsonStringField(alloc, out, &inner_first, "match", query);
        try appendAntflySqlJsonStringField(alloc, out, &inner_first, "field", field);
    } else {
        try appendAntflySqlJsonStringField(alloc, out, &inner_first, "query", query);
    }
    try out.append(alloc, '}');
}

fn appendSemanticFunctionBody(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    args: []const SqlQueryFunctionArg,
) !void {
    const query = requireAntflyQueryFunctionStringArg(args, "query") catch try requireAntflyQueryFunctionStringArg(args, "text");
    const index_name = requireAntflyQueryFunctionStringArg(args, "index") catch try requireAntflyQueryFunctionStringArg(args, "semantic_index");
    try appendAntflySqlJsonStringField(alloc, out, first, "semantic_search", query);
    try appendAntflySqlJsonStringArrayField(alloc, out, first, "indexes", &.{index_name});
    if (antflyQueryFunctionStringArg(args, "embedding_template")) |template| {
        try appendAntflySqlJsonStringField(alloc, out, first, "embedding_template", template);
    }
}

fn appendVectorFunctionBody(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    args: []const SqlQueryFunctionArg,
) !void {
    const index_name = requireAntflyQueryFunctionStringArg(args, "index") catch try requireAntflyQueryFunctionStringArg(args, "vector_index");
    const vector = requireAntflyQueryFunctionStringArg(args, "vector") catch try requireAntflyQueryFunctionStringArg(args, "embedding");
    try appendAntflySqlJsonFieldName(alloc, out, first, "embeddings");
    try out.append(alloc, '{');
    var inner_first = true;
    try appendAntflySqlJsonFieldName(alloc, out, &inner_first, index_name);
    try out.appendSlice(alloc, vector);
    try out.append(alloc, '}');
    try appendAntflySqlJsonStringArrayField(alloc, out, first, "indexes", &.{index_name});
}

fn graphQueryTypeName(function: AntflyQueryFunction) []const u8 {
    return switch (function) {
        .graph_traverse => "traverse",
        .graph_neighbors => "neighbors",
        .graph_shortest_path => "shortest_path",
        .graph_k_shortest_paths => "k_shortest_paths",
        else => unreachable,
    };
}

fn appendGraphNodeSelectorObject(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    key: ?[]const u8,
    result_ref: ?[]const u8,
    result_ref_limit: ?[]const u8,
) !void {
    try out.append(alloc, '{');
    var first = true;
    if (key) |node_key| {
        try appendAntflySqlJsonStringArrayField(alloc, out, &first, "keys", &.{node_key});
    } else if (result_ref) |ref| {
        try appendAntflySqlJsonStringField(alloc, out, &first, "result_ref", ref);
        if (result_ref_limit) |limit| try appendAntflySqlJsonNumberField(alloc, out, &first, "limit", limit);
    } else {
        return error.UnsupportedSqlShape;
    }
    try out.append(alloc, '}');
}

fn appendGraphSearchFunctionBody(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    function: AntflyQueryFunction,
    args: []const SqlQueryFunctionArg,
) !void {
    const index_name = antflyQueryFunctionStringArg(args, "graph_index") orelse try requireAntflyQueryFunctionStringArg(args, "index");
    const query_name = antflyQueryFunctionStringArg(args, "name") orelse graphQueryTypeName(function);
    const start_key = antflyQueryFunctionStringArg(args, "start") orelse antflyQueryFunctionStringArg(args, "start_node");
    const start_result_ref = antflyQueryFunctionStringArg(args, "start_result_ref") orelse antflyQueryFunctionStringArg(args, "result_ref");
    const target_key = antflyQueryFunctionStringArg(args, "target") orelse antflyQueryFunctionStringArg(args, "target_node");
    const target_result_ref = antflyQueryFunctionStringArg(args, "target_result_ref");

    try appendAntflySqlJsonFieldName(alloc, out, first, "graph_searches");
    try out.append(alloc, '{');
    var searches_first = true;
    try appendAntflySqlJsonFieldName(alloc, out, &searches_first, query_name);
    try out.append(alloc, '{');
    var query_first = true;
    try appendAntflySqlJsonStringField(alloc, out, &query_first, "type", graphQueryTypeName(function));
    try appendAntflySqlJsonStringField(alloc, out, &query_first, "index_name", index_name);

    try appendAntflySqlJsonFieldName(alloc, out, &query_first, "start_nodes");
    try appendGraphNodeSelectorObject(alloc, out, start_key, start_result_ref, antflyQueryFunctionNumberArg(args, "start_limit"));

    if (function == .graph_shortest_path or function == .graph_k_shortest_paths) {
        if (target_key == null and target_result_ref == null) return error.UnsupportedSqlShape;
    }
    if (target_key != null or target_result_ref != null) {
        try appendAntflySqlJsonFieldName(alloc, out, &query_first, "target_nodes");
        try appendGraphNodeSelectorObject(alloc, out, target_key, target_result_ref, antflyQueryFunctionNumberArg(args, "target_limit"));
    }

    try appendAntflySqlJsonFieldName(alloc, out, &query_first, "params");
    try out.append(alloc, '{');
    var params_first = true;
    if (antflyQueryFunctionStringArg(args, "direction")) |direction| try appendAntflySqlJsonStringField(alloc, out, &params_first, "direction", direction);
    if (antflyQueryFunctionNumberArg(args, "max_depth")) |max_depth| {
        try appendAntflySqlJsonNumberField(alloc, out, &params_first, "max_depth", max_depth);
    } else if (function == .graph_neighbors) {
        try appendAntflySqlJsonNumberField(alloc, out, &params_first, "max_depth", "1");
    }
    if (antflyQueryFunctionNumberArg(args, "max_results")) |max_results| try appendAntflySqlJsonNumberField(alloc, out, &params_first, "max_results", max_results);
    if (antflyQueryFunctionNumberArg(args, "min_weight")) |min_weight| try appendAntflySqlJsonNumberField(alloc, out, &params_first, "min_weight", min_weight);
    if (antflyQueryFunctionNumberArg(args, "max_weight")) |max_weight| try appendAntflySqlJsonNumberField(alloc, out, &params_first, "max_weight", max_weight);
    if (antflyQueryFunctionStringArg(args, "edge_types")) |edge_types| try appendAntflySqlJsonCommaStringArrayField(alloc, out, &params_first, "edge_types", edge_types);
    if (antflyQueryFunctionBoolArg(args, "deduplicate_nodes")) |deduplicate| try appendAntflySqlJsonBoolField(alloc, out, &params_first, "deduplicate_nodes", deduplicate);
    if (antflyQueryFunctionBoolArg(args, "include_paths")) |include_paths| try appendAntflySqlJsonBoolField(alloc, out, &params_first, "include_paths", include_paths);
    if (antflyQueryFunctionStringArg(args, "weight_mode")) |weight_mode| try appendAntflySqlJsonStringField(alloc, out, &params_first, "weight_mode", weight_mode);
    if (antflyQueryFunctionNumberArg(args, "k")) |k| try appendAntflySqlJsonNumberField(alloc, out, &params_first, "k", k);
    try out.append(alloc, '}');

    if (antflyQueryFunctionStringArg(args, "metrics")) |metrics| try appendAntflySqlJsonCommaStringArrayField(alloc, out, &query_first, "metrics", metrics);
    if (antflyQueryFunctionStringArg(args, "freshness")) |freshness| {
        try appendAntflySqlJsonStringField(alloc, out, &query_first, "metric_freshness", freshness);
    } else if (antflyQueryFunctionStringArg(args, "metric_freshness")) |freshness| {
        try appendAntflySqlJsonStringField(alloc, out, &query_first, "metric_freshness", freshness);
    }
    if (antflyQueryFunctionBoolArg(args, "include_metric_status")) |include_metric_status| try appendAntflySqlJsonBoolField(alloc, out, &query_first, "include_metric_status", include_metric_status);
    if (antflyQueryFunctionBoolArg(args, "include_documents")) |include_documents| try appendAntflySqlJsonBoolField(alloc, out, &query_first, "include_documents", include_documents);
    if (antflyQueryFunctionStringArg(args, "fields")) |fields| try appendAntflySqlJsonCommaStringArrayField(alloc, out, &query_first, "fields", fields);
    try out.append(alloc, '}');
    try out.append(alloc, '}');
}

const GraphPatternEdge = struct {
    direction: []const u8,
    spec: []const u8 = "",
};

const GraphPatternCursor = struct {
    text: []const u8,
    pos: usize = 0,

    fn skipSpace(self: *@This()) void {
        while (self.pos < self.text.len and std.ascii.isWhitespace(self.text[self.pos])) self.pos += 1;
    }

    fn done(self: *@This()) bool {
        self.skipSpace();
        return self.pos >= self.text.len;
    }

    fn startsWith(self: *@This(), value: []const u8) bool {
        return std.mem.startsWith(u8, self.text[self.pos..], value);
    }

    fn consume(self: *@This(), value: []const u8) !void {
        self.skipSpace();
        if (!self.startsWith(value)) return error.UnsupportedSqlShape;
        self.pos += value.len;
    }

    fn consumeOptionalEdgeSpec(self: *@This()) ![]const u8 {
        self.skipSpace();
        if (self.pos >= self.text.len or self.text[self.pos] != '[') return "";
        const start = self.pos + 1;
        self.pos += 1;
        while (self.pos < self.text.len and self.text[self.pos] != ']') self.pos += 1;
        if (self.pos >= self.text.len) return error.UnsupportedSqlShape;
        const spec = std.mem.trim(u8, self.text[start..self.pos], " \t\r\n");
        self.pos += 1;
        return spec;
    }

    fn parseNodeAlias(self: *@This()) ![]const u8 {
        try self.consume("(");
        const start = self.pos;
        while (self.pos < self.text.len and self.text[self.pos] != ')') self.pos += 1;
        if (self.pos >= self.text.len) return error.UnsupportedSqlShape;
        const alias = std.mem.trim(u8, self.text[start..self.pos], " \t\r\n");
        self.pos += 1;
        return alias;
    }

    fn parseEdge(self: *@This()) !GraphPatternEdge {
        self.skipSpace();
        if (self.startsWith("<-")) {
            self.pos += 2;
            const spec = try self.consumeOptionalEdgeSpec();
            try self.consume("-");
            return .{ .direction = "in", .spec = spec };
        }
        try self.consume("-");
        const spec = try self.consumeOptionalEdgeSpec();
        self.skipSpace();
        if (self.startsWith("->")) {
            self.pos += 2;
            return .{ .direction = "out", .spec = spec };
        }
        if (self.startsWith("-")) {
            self.pos += 1;
            return .{ .direction = "both", .spec = spec };
        }
        return error.UnsupportedSqlShape;
    }
};

fn graphPatternDigitsOnly(value: []const u8) bool {
    if (value.len == 0) return false;
    for (value) |ch| {
        if (!std.ascii.isDigit(ch)) return false;
    }
    return true;
}

fn graphPatternIdentifierValid(value: []const u8) bool {
    if (value.len == 0) return false;
    if (!std.ascii.isAlphabetic(value[0]) and value[0] != '_') return false;
    for (value[1..]) |ch| {
        if (!std.ascii.isAlphanumeric(ch) and ch != '_') return false;
    }
    return true;
}

fn graphPatternEdgeTypeValid(value: []const u8) bool {
    if (value.len == 0) return false;
    for (value) |ch| {
        if (!std.ascii.isAlphanumeric(ch) and ch != '_' and ch != '-' and ch != '.') return false;
    }
    return true;
}

fn graphPatternHopCount(value: []const u8) !u32 {
    if (!graphPatternDigitsOnly(value)) return error.UnsupportedSqlShape;
    const parsed = try std.fmt.parseUnsigned(u32, value, 10);
    if (parsed == 0) return error.UnsupportedSqlShape;
    return parsed;
}

fn appendGraphPatternEdgeSpec(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    spec: []const u8,
) !void {
    const trimmed = std.mem.trim(u8, spec, " \t\r\n");
    const star_index = std.mem.indexOfScalar(u8, trimmed, '*');
    const raw_types_and_constraints = if (star_index) |index| trimmed[0..index] else trimmed;
    const edge_head = try parseGraphPatternEdgeHead(raw_types_and_constraints);
    const raw_types = edge_head.types;
    const types_text = std.mem.trim(u8, if (std.mem.startsWith(u8, raw_types, ":")) raw_types[1..] else raw_types, " \t\r\n");

    if (types_text.len > 0) {
        try appendAntflySqlJsonFieldName(alloc, out, first, "types");
        try out.append(alloc, '[');
        var split = std.mem.splitScalar(u8, types_text, '|');
        var item_index: usize = 0;
        while (split.next()) |raw_item| {
            const item = std.mem.trim(u8, raw_item, " \t\r\n");
            if (!graphPatternEdgeTypeValid(item)) return error.UnsupportedSqlShape;
            if (item_index > 0) try out.append(alloc, ',');
            try appendAntflySqlJsonString(alloc, out, item);
            item_index += 1;
        }
        if (item_index == 0) return error.UnsupportedSqlShape;
        try out.append(alloc, ']');
    }

    if (edge_head.constraints) |constraints| try appendGraphPatternEdgeConstraints(alloc, out, first, constraints);

    if (star_index) |index| {
        const quantifier = std.mem.trim(u8, trimmed[index + 1 ..], " \t\r\n");
        if (quantifier.len == 0) return error.UnsupportedSqlShape;
        if (std.mem.indexOf(u8, quantifier, "..")) |range_index| {
            const min = std.mem.trim(u8, quantifier[0..range_index], " \t\r\n");
            const max = std.mem.trim(u8, quantifier[range_index + 2 ..], " \t\r\n");
            const min_hops = try graphPatternHopCount(min);
            const max_hops = try graphPatternHopCount(max);
            if (max_hops < min_hops) return error.UnsupportedSqlShape;
            try appendAntflySqlJsonNumberField(alloc, out, first, "min_hops", min);
            try appendAntflySqlJsonNumberField(alloc, out, first, "max_hops", max);
        } else {
            _ = try graphPatternHopCount(quantifier);
            try appendAntflySqlJsonNumberField(alloc, out, first, "min_hops", quantifier);
            try appendAntflySqlJsonNumberField(alloc, out, first, "max_hops", quantifier);
        }
    }
}

const GraphPatternEdgeHead = struct {
    types: []const u8,
    constraints: ?[]const u8 = null,
};

fn parseGraphPatternEdgeHead(raw: []const u8) !GraphPatternEdgeHead {
    const trimmed = std.mem.trim(u8, raw, " \t\r\n");
    const open = std.mem.indexOfScalar(u8, trimmed, '{') orelse return .{ .types = trimmed };
    const close = std.mem.lastIndexOfScalar(u8, trimmed, '}') orelse return error.UnsupportedSqlShape;
    if (close <= open) return error.UnsupportedSqlShape;
    const tail = std.mem.trim(u8, trimmed[close + 1 ..], " \t\r\n");
    if (tail.len != 0) return error.UnsupportedSqlShape;
    return .{
        .types = std.mem.trim(u8, trimmed[0..open], " \t\r\n"),
        .constraints = std.mem.trim(u8, trimmed[open + 1 .. close], " \t\r\n"),
    };
}

const GraphPatternEdgeWeightConstraints = struct {
    min_weight: ?[]const u8 = null,
    max_weight: ?[]const u8 = null,
    min_value: ?f64 = null,
    max_value: ?f64 = null,
};

fn appendGraphPatternEdgeConstraints(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    constraints: []const u8,
) !void {
    const parsed = try parseGraphPatternEdgeWeightConstraints(constraints);
    if (parsed.min_value != null and parsed.max_value != null and parsed.max_value.? < parsed.min_value.?) return error.UnsupportedSqlShape;
    if (parsed.min_weight) |value| try appendAntflySqlJsonNumberField(alloc, out, first, "min_weight", value);
    if (parsed.max_weight) |value| try appendAntflySqlJsonNumberField(alloc, out, first, "max_weight", value);
}

fn parseGraphPatternEdgeWeightConstraints(constraints: []const u8) !GraphPatternEdgeWeightConstraints {
    if (constraints.len == 0) return error.UnsupportedSqlShape;
    var parsed = GraphPatternEdgeWeightConstraints{};
    var split = std.mem.splitScalar(u8, constraints, ',');
    while (split.next()) |raw_part| {
        const part = std.mem.trim(u8, raw_part, " \t\r\n");
        if (part.len == 0) return error.UnsupportedSqlShape;
        const item = try parseGraphPatternEdgeWeightConstraint(part);
        if (std.ascii.eqlIgnoreCase(item.name, "min_weight") or
            std.ascii.eqlIgnoreCase(item.name, "weight_min") or
            (std.ascii.eqlIgnoreCase(item.name, "weight") and item.kind == .min))
        {
            if (parsed.min_weight != null) return error.UnsupportedSqlShape;
            parsed.min_weight = item.value;
            parsed.min_value = item.numeric;
        } else if (std.ascii.eqlIgnoreCase(item.name, "max_weight") or
            std.ascii.eqlIgnoreCase(item.name, "weight_max") or
            (std.ascii.eqlIgnoreCase(item.name, "weight") and item.kind == .max))
        {
            if (parsed.max_weight != null) return error.UnsupportedSqlShape;
            parsed.max_weight = item.value;
            parsed.max_value = item.numeric;
        } else {
            return error.UnsupportedSqlShape;
        }
    }
    if (parsed.min_weight == null and parsed.max_weight == null) return error.UnsupportedSqlShape;
    return parsed;
}

const GraphPatternEdgeWeightConstraintKind = enum {
    exact,
    min,
    max,
};

const GraphPatternEdgeWeightConstraint = struct {
    name: []const u8,
    value: []const u8,
    numeric: f64,
    kind: GraphPatternEdgeWeightConstraintKind,
};

fn parseGraphPatternEdgeWeightConstraint(part: []const u8) !GraphPatternEdgeWeightConstraint {
    if (std.mem.indexOf(u8, part, ">=")) |index| {
        return try graphPatternEdgeWeightConstraint(part[0..index], part[index + 2 ..], .min);
    }
    if (std.mem.indexOf(u8, part, "<=")) |index| {
        return try graphPatternEdgeWeightConstraint(part[0..index], part[index + 2 ..], .max);
    }
    if (std.mem.indexOfScalar(u8, part, ':')) |index| {
        return try graphPatternEdgeWeightConstraint(part[0..index], part[index + 1 ..], .exact);
    }
    if (std.mem.indexOfScalar(u8, part, '=')) |index| {
        return try graphPatternEdgeWeightConstraint(part[0..index], part[index + 1 ..], .exact);
    }
    return error.UnsupportedSqlShape;
}

fn graphPatternEdgeWeightConstraint(
    raw_name: []const u8,
    raw_value: []const u8,
    kind: GraphPatternEdgeWeightConstraintKind,
) !GraphPatternEdgeWeightConstraint {
    const name = std.mem.trim(u8, raw_name, " \t\r\n");
    const value = std.mem.trim(u8, raw_value, " \t\r\n");
    if (name.len == 0 or value.len == 0) return error.UnsupportedSqlShape;
    const numeric = std.fmt.parseFloat(f64, value) catch return error.UnsupportedSqlShape;
    if (!std.math.isFinite(numeric) or numeric < 0) return error.UnsupportedSqlShape;
    return .{
        .name = name,
        .value = value,
        .numeric = numeric,
        .kind = kind,
    };
}

fn appendGraphPatternStep(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    alias: []const u8,
    edge: ?GraphPatternEdge,
) !void {
    if (!graphPatternIdentifierValid(alias)) return error.UnsupportedSqlShape;
    try out.append(alloc, '{');
    var first = true;
    try appendAntflySqlJsonStringField(alloc, out, &first, "alias", alias);
    if (edge) |edge_spec| {
        try appendAntflySqlJsonFieldName(alloc, out, &first, "edge");
        try out.append(alloc, '{');
        var edge_first = true;
        try appendAntflySqlJsonStringField(alloc, out, &edge_first, "direction", edge_spec.direction);
        if (edge_spec.spec.len > 0) try appendGraphPatternEdgeSpec(alloc, out, &edge_first, edge_spec.spec);
        try out.append(alloc, '}');
    }
    try out.append(alloc, '}');
}

fn appendGraphPatternSteps(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    pattern: []const u8,
) !void {
    var cursor = GraphPatternCursor{ .text = pattern };
    try out.append(alloc, '[');
    const first_alias = try cursor.parseNodeAlias();
    try appendGraphPatternStep(alloc, out, first_alias, null);
    var step_count: usize = 1;
    while (!cursor.done()) {
        const edge = try cursor.parseEdge();
        const alias = try cursor.parseNodeAlias();
        try out.append(alloc, ',');
        try appendGraphPatternStep(alloc, out, alias, edge);
        step_count += 1;
    }
    if (step_count < 2) return error.UnsupportedSqlShape;
    try out.append(alloc, ']');
}

fn appendGraphMetricOrderFilterArgs(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    args: []const SqlQueryFunctionArg,
) !void {
    const order_metric = antflyQueryFunctionStringArg(args, "order_metric") orelse antflyQueryFunctionStringArg(args, "order_by_metric");
    if (order_metric) |metric| {
        try appendAntflySqlJsonFieldName(alloc, out, first, "order_by");
        try out.appendSlice(alloc, "[{");
        var order_first = true;
        try appendAntflySqlJsonStringField(alloc, out, &order_first, "metric", metric);
        if (antflyQueryFunctionStringArg(args, "order_direction")) |direction| try appendAntflySqlJsonStringField(alloc, out, &order_first, "direction", direction);
        if (antflyQueryFunctionStringArg(args, "order_nulls")) |nulls| try appendAntflySqlJsonStringField(alloc, out, &order_first, "nulls", nulls);
        try out.appendSlice(alloc, "}]");
    } else if (antflyQueryFunctionStringArg(args, "order_direction") != null or antflyQueryFunctionStringArg(args, "order_nulls") != null) {
        return error.UnsupportedSqlShape;
    }

    const filter_metric = antflyQueryFunctionStringArg(args, "where_metric") orelse antflyQueryFunctionStringArg(args, "filter_metric");
    if (filter_metric) |metric| {
        const op = antflyQueryFunctionStringArg(args, "where_op") orelse antflyQueryFunctionStringArg(args, "filter_op") orelse return error.UnsupportedSqlShape;
        const value = antflyQueryFunctionNumberArg(args, "where_value") orelse antflyQueryFunctionNumberArg(args, "filter_value") orelse return error.UnsupportedSqlShape;
        try appendAntflySqlJsonFieldName(alloc, out, first, "where_metric");
        try out.appendSlice(alloc, "[{");
        var filter_first = true;
        try appendAntflySqlJsonStringField(alloc, out, &filter_first, "metric", metric);
        try appendAntflySqlJsonStringField(alloc, out, &filter_first, "op", op);
        try appendAntflySqlJsonNumberField(alloc, out, &filter_first, "value", value);
        try out.appendSlice(alloc, "}]");
    } else if (antflyQueryFunctionStringArg(args, "where_op") != null or
        antflyQueryFunctionStringArg(args, "filter_op") != null or
        antflyQueryFunctionNumberArg(args, "where_value") != null or
        antflyQueryFunctionNumberArg(args, "filter_value") != null)
    {
        return error.UnsupportedSqlShape;
    }
}

fn appendGraphMatchFunctionBody(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    args: []const SqlQueryFunctionArg,
) !void {
    const index_name = antflyQueryFunctionStringArg(args, "graph_index") orelse try requireAntflyQueryFunctionStringArg(args, "index");
    const query_name = antflyQueryFunctionStringArg(args, "name") orelse "graph_match";
    const pattern = try requireAntflyQueryFunctionStringArg(args, "pattern");
    const start_key = antflyQueryFunctionStringArg(args, "start") orelse antflyQueryFunctionStringArg(args, "start_node");
    const start_result_ref = antflyQueryFunctionStringArg(args, "start_result_ref") orelse antflyQueryFunctionStringArg(args, "result_ref");

    try appendAntflySqlJsonFieldName(alloc, out, first, "graph_searches");
    try out.append(alloc, '{');
    var searches_first = true;
    try appendAntflySqlJsonFieldName(alloc, out, &searches_first, query_name);
    try out.append(alloc, '{');
    var query_first = true;
    try appendAntflySqlJsonStringField(alloc, out, &query_first, "type", "pattern");
    try appendAntflySqlJsonStringField(alloc, out, &query_first, "index_name", index_name);
    try appendAntflySqlJsonFieldName(alloc, out, &query_first, "start_nodes");
    try appendGraphNodeSelectorObject(alloc, out, start_key, start_result_ref, antflyQueryFunctionNumberArg(args, "start_limit"));
    try appendAntflySqlJsonFieldName(alloc, out, &query_first, "pattern");
    try appendGraphPatternSteps(alloc, out, pattern);

    if (antflyQueryFunctionStringArg(args, "return")) |return_aliases| {
        try appendAntflySqlJsonCommaStringArrayField(alloc, out, &query_first, "return_aliases", return_aliases);
    } else if (antflyQueryFunctionStringArg(args, "return_aliases")) |return_aliases| {
        try appendAntflySqlJsonCommaStringArrayField(alloc, out, &query_first, "return_aliases", return_aliases);
    }
    if (antflyQueryFunctionNumberArg(args, "max_results")) |max_results| {
        try appendAntflySqlJsonFieldName(alloc, out, &query_first, "params");
        try out.append(alloc, '{');
        var params_first = true;
        try appendAntflySqlJsonNumberField(alloc, out, &params_first, "max_results", max_results);
        try out.append(alloc, '}');
    }
    if (antflyQueryFunctionStringArg(args, "metrics")) |metrics| try appendAntflySqlJsonCommaStringArrayField(alloc, out, &query_first, "metrics", metrics);
    try appendGraphMetricOrderFilterArgs(alloc, out, &query_first, args);
    if (antflyQueryFunctionStringArg(args, "freshness")) |freshness| {
        try appendAntflySqlJsonStringField(alloc, out, &query_first, "metric_freshness", freshness);
    } else if (antflyQueryFunctionStringArg(args, "metric_freshness")) |freshness| {
        try appendAntflySqlJsonStringField(alloc, out, &query_first, "metric_freshness", freshness);
    }
    if (antflyQueryFunctionBoolArg(args, "include_metric_status")) |include_metric_status| try appendAntflySqlJsonBoolField(alloc, out, &query_first, "include_metric_status", include_metric_status);
    if (antflyQueryFunctionBoolArg(args, "include_documents")) |include_documents| try appendAntflySqlJsonBoolField(alloc, out, &query_first, "include_documents", include_documents);
    if (antflyQueryFunctionStringArg(args, "fields")) |fields| try appendAntflySqlJsonCommaStringArrayField(alloc, out, &query_first, "fields", fields);
    try out.append(alloc, '}');
    try out.append(alloc, '}');
}

fn appendGraphMetricFunctionBody(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    args: []const SqlQueryFunctionArg,
) !void {
    try appendAntflySqlJsonFieldName(alloc, out, first, "graph_metric");
    try appendGraphMetricObjectFields(alloc, out, args, true);
}

fn appendGraphMetricRerankFunctionBody(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    args: []const SqlQueryFunctionArg,
) !void {
    if (antflyQueryFunctionStringArg(args, "query") != null) {
        try appendFullTextFunctionBody(alloc, out, first, args);
    }
    try appendAntflySqlJsonFieldName(alloc, out, first, "graph_metric_rerank");
    try appendGraphMetricObjectFields(alloc, out, args, false);
}

fn appendHybridFunctionBody(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    args: []const SqlQueryFunctionArg,
) !void {
    if (antflyQueryFunctionStringArg(args, "sources_json")) |sources_json| {
        return try appendStructuredHybridFunctionBody(alloc, out, first, args, sources_json);
    }
    if (antflyQueryFunctionSourcesArg(args, "sources")) |sources| {
        const sources_json = try hybridSourceSpecsJsonAlloc(alloc, sources);
        defer alloc.free(sources_json);
        return try appendStructuredHybridFunctionBody(alloc, out, first, args, sources_json);
    }

    const query = requireAntflyQueryFunctionStringArg(args, "query") catch try requireAntflyQueryFunctionStringArg(args, "text");
    var semantic_indexes = std.ArrayListUnmanaged([]const u8).empty;
    defer semantic_indexes.deinit(alloc);
    if (antflyQueryFunctionStringArg(args, "semantic_index")) |index_name| {
        try appendAntflySqlJsonStringField(alloc, out, first, "semantic_search", query);
        try semantic_indexes.append(alloc, index_name);
    }
    if (antflyQueryFunctionStringArg(args, "vector_index")) |index_name| {
        if (antflyQueryFunctionStringArg(args, "vector")) |vector| {
            try appendAntflySqlJsonFieldName(alloc, out, first, "embeddings");
            try out.append(alloc, '{');
            var inner_first = true;
            try appendAntflySqlJsonFieldName(alloc, out, &inner_first, index_name);
            try out.appendSlice(alloc, vector);
            try out.append(alloc, '}');
            try semantic_indexes.append(alloc, index_name);
        }
    }
    if (semantic_indexes.items.len > 0) {
        try appendAntflySqlJsonStringArrayField(alloc, out, first, "indexes", semantic_indexes.items);
    }
    if (antflyQueryFunctionStringArg(args, "full_text_index") != null or antflyQueryFunctionStringArg(args, "field") != null) {
        try appendFullTextFunctionBody(alloc, out, first, args);
    }
    if (antflyQueryFunctionStringArg(args, "graph_index") != null or antflyQueryFunctionStringArg(args, "graph_metric") != null or antflyQueryFunctionStringArg(args, "metric") != null) {
        try appendAntflySqlJsonFieldName(alloc, out, first, "graph_metric_rerank");
        try appendGraphMetricObjectFields(alloc, out, args, false);
    }
    try appendAntflySqlJsonFieldName(alloc, out, first, "merge_config");
    try out.append(alloc, '{');
    var merge_first = true;
    try appendAntflySqlJsonStringField(alloc, out, &merge_first, "strategy", antflyQueryFunctionStringArg(args, "fusion") orelse "rrf");
    if (antflyQueryFunctionNumberArg(args, "window_size")) |window_size| try appendAntflySqlJsonNumberField(alloc, out, &merge_first, "window_size", window_size);
    try out.append(alloc, '}');
}

fn appendStructuredHybridFunctionBody(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    args: []const SqlQueryFunctionArg,
    sources_json: []const u8,
) !void {
    const query = requireAntflyQueryFunctionStringArg(args, "query") catch try requireAntflyQueryFunctionStringArg(args, "text");
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, sources_json, .{});
    defer parsed.deinit();
    if (parsed.value != .array or parsed.value.array.items.len == 0) return error.UnsupportedSqlShape;

    var semantic_indexes = std.ArrayListUnmanaged([]const u8).empty;
    defer semantic_indexes.deinit(alloc);
    var weights = std.ArrayListUnmanaged(HybridSourceWeight).empty;
    defer weights.deinit(alloc);
    var saw_full_text = false;
    var saw_graph_metric = false;

    for (parsed.value.array.items) |source_value| {
        if (source_value != .object) return error.UnsupportedSqlShape;
        const source = source_value.object;
        const kind = jsonObjectString(source, "kind") orelse jsonObjectString(source, "type") orelse return error.UnsupportedSqlShape;
        const index_name = jsonObjectString(source, "index") orelse jsonObjectString(source, "index_name") orelse return error.UnsupportedSqlShape;
        const source_name = jsonObjectString(source, "name") orelse hybridSourceDefaultName(kind, index_name);
        if (source_name.len == 0) return error.UnsupportedSqlShape;
        if (jsonObjectNumber(source, "weight")) |weight| try weights.append(alloc, .{ .name = source_name, .weight = weight });

        if (std.ascii.eqlIgnoreCase(kind, "full_text") or std.ascii.eqlIgnoreCase(kind, "text")) {
            if (saw_full_text) return error.UnsupportedSqlShape;
            saw_full_text = true;
            try appendAntflySqlJsonFieldName(alloc, out, first, "full_text_search");
            try out.append(alloc, '{');
            var text_first = true;
            const field = jsonObjectString(source, "field") orelse antflyQueryFunctionStringArg(args, "field");
            if (field) |field_name| {
                try appendAntflySqlJsonStringField(alloc, out, &text_first, "match", query);
                try appendAntflySqlJsonStringField(alloc, out, &text_first, "field", field_name);
            } else {
                try appendAntflySqlJsonStringField(alloc, out, &text_first, "query", query);
            }
            try out.append(alloc, '}');
        } else if (std.ascii.eqlIgnoreCase(kind, "semantic") or std.ascii.eqlIgnoreCase(kind, "vector")) {
            try semantic_indexes.append(alloc, index_name);
        } else if (std.ascii.eqlIgnoreCase(kind, "graph_metric") or std.ascii.eqlIgnoreCase(kind, "graph")) {
            if (saw_graph_metric) return error.UnsupportedSqlShape;
            saw_graph_metric = true;
            const metric = jsonObjectString(source, "metric") orelse jsonObjectString(source, "graph_metric") orelse return error.UnsupportedSqlShape;
            try appendAntflySqlJsonFieldName(alloc, out, first, "graph_metric_rerank");
            try out.append(alloc, '{');
            var graph_first = true;
            try appendAntflySqlJsonStringField(alloc, out, &graph_first, "index", index_name);
            try appendAntflySqlJsonStringField(alloc, out, &graph_first, "metric", metric);
            if (jsonObjectNumber(source, "weight")) |weight| try appendAntflySqlJsonNumberValueField(alloc, out, &graph_first, "weight", weight);
            if (jsonObjectNumber(source, "base_weight")) |base_weight| try appendAntflySqlJsonNumberValueField(alloc, out, &graph_first, "base_weight", base_weight);
            if (jsonObjectNumber(source, "missing_score")) |missing_score| try appendAntflySqlJsonNumberValueField(alloc, out, &graph_first, "missing_score", missing_score);
            if (jsonObjectString(source, "freshness")) |freshness| try appendAntflySqlJsonStringField(alloc, out, &graph_first, "metric_freshness", freshness);
            if (jsonObjectString(source, "metric_freshness")) |freshness| {
                if (jsonObjectString(source, "freshness") != null) return error.UnsupportedSqlShape;
                try appendAntflySqlJsonStringField(alloc, out, &graph_first, "metric_freshness", freshness);
            }
            try out.append(alloc, '}');
        } else {
            return error.UnsupportedSqlShape;
        }
    }

    if (semantic_indexes.items.len > 0) {
        try appendAntflySqlJsonStringField(alloc, out, first, "semantic_search", query);
        try appendAntflySqlJsonStringArrayField(alloc, out, first, "indexes", semantic_indexes.items);
    }

    try appendAntflySqlJsonFieldName(alloc, out, first, "merge_config");
    try out.append(alloc, '{');
    var merge_first = true;
    try appendAntflySqlJsonStringField(alloc, out, &merge_first, "strategy", antflyQueryFunctionStringArg(args, "fusion") orelse "rrf");
    if (antflyQueryFunctionNumberArg(args, "window_size")) |window_size| try appendAntflySqlJsonNumberField(alloc, out, &merge_first, "window_size", window_size);
    if (weights.items.len > 0) {
        try appendAntflySqlJsonFieldName(alloc, out, &merge_first, "weights");
        try out.append(alloc, '{');
        var weights_first = true;
        for (weights.items) |weight| {
            try appendAntflySqlJsonNumberValueField(alloc, out, &weights_first, weight.name, weight.weight);
        }
        try out.append(alloc, '}');
    }
    try out.append(alloc, '}');
}

const HybridSourceWeight = struct {
    name: []const u8,
    weight: std.json.Value,
};

fn hybridSourcesPrimaryTextIndexAlloc(
    alloc: std.mem.Allocator,
    args: []const SqlQueryFunctionArg,
) !?[]const u8 {
    if (antflyQueryFunctionSourcesArg(args, "sources")) |sources| {
        var primary: ?[]const u8 = null;
        for (sources) |source| {
            if (!std.ascii.eqlIgnoreCase(source.kind, "full_text") and !std.ascii.eqlIgnoreCase(source.kind, "text")) continue;
            if (primary != null) return error.UnsupportedSqlShape;
            primary = source.index;
        }
        return if (primary) |index_name| try alloc.dupe(u8, index_name) else null;
    }

    const sources_json = antflyQueryFunctionStringArg(args, "sources_json") orelse return null;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, sources_json, .{});
    defer parsed.deinit();
    if (parsed.value != .array) return error.UnsupportedSqlShape;
    var primary: ?[]const u8 = null;
    for (parsed.value.array.items) |source_value| {
        if (source_value != .object) return error.UnsupportedSqlShape;
        const source = source_value.object;
        const kind = jsonObjectString(source, "kind") orelse jsonObjectString(source, "type") orelse return error.UnsupportedSqlShape;
        if (!std.ascii.eqlIgnoreCase(kind, "full_text") and !std.ascii.eqlIgnoreCase(kind, "text")) continue;
        if (primary != null) return error.UnsupportedSqlShape;
        primary = jsonObjectString(source, "index") orelse jsonObjectString(source, "index_name") orelse return error.UnsupportedSqlShape;
    }
    return if (primary) |index_name| try alloc.dupe(u8, index_name) else null;
}

fn hybridSourceSpecsJsonAlloc(
    alloc: std.mem.Allocator,
    sources: []const HybridSqlSourceSpec,
) ![]const u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.append(alloc, '[');
    for (sources, 0..) |source, i| {
        if (i > 0) try out.append(alloc, ',');
        try out.append(alloc, '{');
        var first = true;
        try appendAntflySqlJsonStringField(alloc, &out, &first, "kind", source.kind);
        try appendAntflySqlJsonStringField(alloc, &out, &first, "index", source.index);
        if (source.name) |name| try appendAntflySqlJsonStringField(alloc, &out, &first, "name", name);
        if (source.field) |field| try appendAntflySqlJsonStringField(alloc, &out, &first, "field", field);
        if (source.metric) |metric| try appendAntflySqlJsonStringField(alloc, &out, &first, "metric", metric);
        if (source.freshness) |freshness| try appendAntflySqlJsonStringField(alloc, &out, &first, "freshness", freshness);
        if (source.weight) |weight| try appendAntflySqlJsonNumberField(alloc, &out, &first, "weight", weight);
        if (source.base_weight) |base_weight| try appendAntflySqlJsonNumberField(alloc, &out, &first, "base_weight", base_weight);
        if (source.missing_score) |missing_score| try appendAntflySqlJsonNumberField(alloc, &out, &first, "missing_score", missing_score);
        try out.append(alloc, '}');
    }
    try out.append(alloc, ']');
    return try out.toOwnedSlice(alloc);
}

fn hybridSourceDefaultName(kind: []const u8, index_name: []const u8) []const u8 {
    if (std.ascii.eqlIgnoreCase(kind, "full_text") or std.ascii.eqlIgnoreCase(kind, "text")) return "full_text_search";
    if (std.ascii.eqlIgnoreCase(kind, "graph_metric") or std.ascii.eqlIgnoreCase(kind, "graph")) return "graph_metric_rerank";
    return index_name;
}

fn jsonObjectString(object: std.json.ObjectMap, field_name: []const u8) ?[]const u8 {
    const value = object.get(field_name) orelse return null;
    return switch (value) {
        .string => |text| if (text.len > 0) text else null,
        else => null,
    };
}

fn jsonObjectNumber(object: std.json.ObjectMap, field_name: []const u8) ?std.json.Value {
    const value = object.get(field_name) orelse return null;
    return switch (value) {
        .integer, .float, .number_string => value,
        else => null,
    };
}

fn appendAntflySqlJsonNumberValueField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    value: std.json.Value,
) !void {
    try appendAntflySqlJsonFieldName(alloc, out, first, name);
    switch (value) {
        .integer => |integer| {
            const rendered = try std.fmt.allocPrint(alloc, "{}", .{integer});
            defer alloc.free(rendered);
            try out.appendSlice(alloc, rendered);
        },
        .float => |float| {
            if (!std.math.isFinite(float)) return error.UnsupportedSqlShape;
            const rendered = try std.fmt.allocPrint(alloc, "{d}", .{float});
            defer alloc.free(rendered);
            try out.appendSlice(alloc, rendered);
        },
        .number_string => |text| {
            if (text.len == 0) return error.UnsupportedSqlShape;
            try out.appendSlice(alloc, text);
        },
        else => return error.UnsupportedSqlShape,
    }
}

fn appendGraphMetricObjectFields(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    args: []const SqlQueryFunctionArg,
    include_top_k: bool,
) !void {
    const index_name = antflyQueryFunctionStringArg(args, "graph_index") orelse try requireAntflyQueryFunctionStringArg(args, "index");
    const metric_name = antflyQueryFunctionStringArg(args, "graph_metric") orelse try requireAntflyQueryFunctionStringArg(args, "metric");
    try out.append(alloc, '{');
    var first = true;
    try appendAntflySqlJsonStringField(alloc, out, &first, "index", index_name);
    try appendAntflySqlJsonStringField(alloc, out, &first, "metric", metric_name);
    if (include_top_k) {
        if (antflyQueryFunctionNumberArg(args, "top_k")) |top_k| try appendAntflySqlJsonNumberField(alloc, out, &first, "top_k", top_k);
    }
    if (antflyQueryFunctionStringArg(args, "freshness")) |freshness| {
        try appendAntflySqlJsonStringField(alloc, out, &first, "metric_freshness", freshness);
    } else if (antflyQueryFunctionStringArg(args, "metric_freshness")) |freshness| {
        try appendAntflySqlJsonStringField(alloc, out, &first, "metric_freshness", freshness);
    }
    if (!include_top_k) {
        if (antflyQueryFunctionNumberArg(args, "base_weight")) |base_weight| try appendAntflySqlJsonNumberField(alloc, out, &first, "base_weight", base_weight);
        if (antflyQueryFunctionNumberArg(args, "weight")) |weight| try appendAntflySqlJsonNumberField(alloc, out, &first, "weight", weight);
        if (antflyQueryFunctionNumberArg(args, "missing_score")) |missing_score| try appendAntflySqlJsonNumberField(alloc, out, &first, "missing_score", missing_score);
    }
    try out.append(alloc, '}');
}

fn appendCommonAntflyQueryFunctionOptions(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    args: []const SqlQueryFunctionArg,
) !void {
    if (antflyQueryFunctionNumberArg(args, "limit")) |limit| try appendAntflySqlJsonNumberField(alloc, out, first, "limit", limit);
    if (antflyQueryFunctionNumberArg(args, "offset")) |offset| try appendAntflySqlJsonNumberField(alloc, out, first, "offset", offset);
    if (antflyQueryFunctionBoolArg(args, "profile")) |profile| try appendAntflySqlJsonBoolField(alloc, out, first, "profile", profile);
    if (antflyQueryFunctionBoolArg(args, "include_stored")) |include_stored| try appendAntflySqlJsonBoolField(alloc, out, first, "include_stored", include_stored);
}

test "sql adapter query function dispatch uses token keyword metadata" {
    try std.testing.expectEqual(AntflyQueryFunction.full_text_search, antflyQueryFunctionFromSqlName("full_text_search").?);
    try std.testing.expectEqual(AntflyQueryFunction.full_text_search, antflyQueryFunctionFromSqlName("ANTFLY.FULL_TEXT_SEARCH").?);
    try std.testing.expect(antflyQueryFunctionFromSqlName("public.full_text_search") == null);
    try std.testing.expect(antflyQueryFunctionFromSqlName("antfly.public.full_text_search") == null);

    const keyword_token = Token{
        .kind = .identifier,
        .text = "FULL_TEXT_SEARCH",
        .keyword = token_mod.keywordFromIdentifier("FULL_TEXT_SEARCH"),
    };
    try std.testing.expectEqual(AntflyQueryFunction.full_text_search, antflyQueryFunctionFromSqlToken(keyword_token).?);

    const qualified_token = Token{
        .kind = .identifier,
        .text = "ANTFLY.HYBRID_SEARCH",
    };
    try std.testing.expectEqual(AntflyQueryFunction.hybrid_search, antflyQueryFunctionFromSqlToken(qualified_token).?);

    try std.testing.expect(antflySourceFunctionToken(.{
        .kind = .identifier,
        .text = "ANTFLY.SOURCE",
    }));
}

test "sql adapter query function lowers antfly query functions into native search requests" {
    const alloc = std.testing.allocator;

    const Resolver = struct {
        fn resolve(
            _: *anyopaque,
            allocator: std.mem.Allocator,
            table_name: []const u8,
            index_name: []const u8,
            semantic_search: []const u8,
            embedding_template: ?[]const u8,
            limit: u32,
        ) !db_mod.types.DenseKnnQuery {
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expect(index_name.len > 0);
            try std.testing.expect(semantic_search.len > 0);
            try std.testing.expect(embedding_template == null or embedding_template.?.len > 0);
            return .{
                .vector = try allocator.dupe(f32, &[_]f32{ 0.25, 0.5, 0.75 }),
                .k = limit,
            };
        }
    };
    var resolver_state: u8 = 0;
    const resolver = query_contract.SemanticResolver{
        .ptr = &resolver_state,
        .vtable = &.{ .resolve_dense_query = Resolver.resolve },
    };

    var full_text = try lowerAntflyQueryFunctionSqlAlloc(
        alloc,
        null,
        "SELECT * FROM antfly.full_text_search(table_name => 'docs', index => 'docs_body_fts', field => 'body', query => 'refund policy', limit => 5);",
    );
    defer full_text.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 5), full_text.req.limit);
    try std.testing.expectEqualStrings("docs_body_fts", full_text.req.primary_text_index_name.?);
    try std.testing.expect(full_text.req.full_text.? == .match);
    try std.testing.expectEqualStrings("body", full_text.req.full_text.?.match.field);
    try std.testing.expectEqualStrings("refund policy", full_text.req.full_text.?.match.text);

    var full_text_expression = try lowerAntflyQueryFunctionExpressionSqlAlloc(
        alloc,
        null,
        "antfly.full_text_search(table_name => 'docs', index => 'docs_body_fts', field => 'body', query => 'refund policy', limit => 5)",
    );
    defer full_text_expression.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 5), full_text_expression.req.limit);
    try std.testing.expectEqualStrings("docs_body_fts", full_text_expression.req.primary_text_index_name.?);
    try std.testing.expect(full_text_expression.req.full_text.? == .match);
    try std.testing.expectEqualStrings("body", full_text_expression.req.full_text.?.match.field);
    try std.testing.expectEqualStrings("refund policy", full_text_expression.req.full_text.?.match.text);

    var semantic = try lowerAntflyQueryFunctionSqlAlloc(
        alloc,
        resolver,
        "SELECT * FROM antfly.semantic_search(table => 'docs', index => 'docs_body_semantic', query => 'automatic embeddings', limit => 7);",
    );
    defer semantic.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), semantic.req.dense_queries.len);
    try std.testing.expectEqualStrings("docs_body_semantic", semantic.req.dense_queries[0].index_name);
    try std.testing.expectEqual(@as(u32, 7), semantic.req.dense_queries[0].query.k);
    try std.testing.expectEqual(@as(usize, 3), semantic.req.dense_queries[0].query.vector.len);

    var vector = try lowerAntflyQueryFunctionSqlAlloc(
        alloc,
        null,
        "SELECT * FROM antfly.vector_search(table_name = 'docs', index = 'docs_embedding_hnsw', vector = '[1.0,0.0,0.5]', limit = 3);",
    );
    defer vector.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), vector.req.dense_queries.len);
    try std.testing.expectEqualStrings("docs_embedding_hnsw", vector.req.dense_queries[0].index_name);
    try std.testing.expectEqual(@as(u32, 3), vector.req.dense_queries[0].query.k);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), vector.req.dense_queries[0].query.vector[2], 0.0001);

    var traverse = try lowerAntflyQueryFunctionSqlAlloc(
        alloc,
        null,
        "SELECT * FROM antfly.graph_traverse(table_name => 'docs', name => 'citation_walk', index => 'docs_edge_graph', start => 'doc:root', direction => 'out', edge_types => 'cites, references', max_depth => 2, max_results => 11, metrics => 'pagerank', freshness => 'fresh', include_metric_status => true, include_paths => true);",
    );
    defer traverse.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), traverse.req.graph_queries.len);
    try std.testing.expectEqualStrings("citation_walk", traverse.req.graph_queries[0].name);
    const traverse_query = traverse.req.graph_queries[0].query;
    try std.testing.expectEqual(@as(@TypeOf(traverse_query.query_type), .traverse), traverse_query.query_type);
    try std.testing.expectEqualStrings("docs_edge_graph", traverse_query.index_name);
    switch (traverse_query.start_nodes) {
        .keys => |keys| {
            try std.testing.expectEqual(@as(usize, 1), keys.len);
            try std.testing.expectEqualStrings("doc:root", keys[0]);
        },
        else => return error.TestUnexpectedResult,
    }
    try std.testing.expectEqual(@as(@TypeOf(traverse_query.params.direction), .out), traverse_query.params.direction);
    try std.testing.expectEqual(@as(u32, 2), traverse_query.params.max_depth);
    try std.testing.expectEqual(@as(u32, 11), traverse_query.params.max_results);
    try std.testing.expectEqual(@as(usize, 2), traverse_query.params.edge_types.len);
    try std.testing.expectEqualStrings("cites", traverse_query.params.edge_types[0]);
    try std.testing.expectEqualStrings("references", traverse_query.params.edge_types[1]);
    try std.testing.expect(traverse_query.params.include_paths);
    try std.testing.expect(traverse_query.include_metric_status);
    try std.testing.expectEqual(@as(usize, 1), traverse_query.metrics.len);
    try std.testing.expectEqualStrings("pagerank", traverse_query.metrics[0].name);
    try std.testing.expect(traverse_query.metrics[0].freshness == .fresh);

    var shortest_path = try lowerAntflyQueryFunctionSqlAlloc(
        alloc,
        null,
        "SELECT * FROM antfly.graph_shortest_path(table_name => 'docs', index => 'docs_edge_graph', start => 'doc:a', target => 'doc:z', direction => 'both', max_depth => 4, weight_mode => 'min_weight');",
    );
    defer shortest_path.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), shortest_path.req.graph_queries.len);
    const shortest_path_query = shortest_path.req.graph_queries[0].query;
    try std.testing.expectEqual(@as(@TypeOf(shortest_path_query.query_type), .shortest_path), shortest_path_query.query_type);
    try std.testing.expectEqual(@as(@TypeOf(shortest_path_query.params.direction), .both), shortest_path_query.params.direction);
    try std.testing.expectEqual(@as(u32, 4), shortest_path_query.params.max_depth);
    try std.testing.expectEqual(@as(@TypeOf(shortest_path_query.params.weight_mode), .min_weight), shortest_path_query.params.weight_mode);
    const shortest_path_target = shortest_path_query.target_nodes orelse return error.TestUnexpectedResult;
    switch (shortest_path_target) {
        .keys => |keys| {
            try std.testing.expectEqual(@as(usize, 1), keys.len);
            try std.testing.expectEqualStrings("doc:z", keys[0]);
        },
        else => return error.TestUnexpectedResult,
    }

    var k_shortest_paths = try lowerAntflyQueryFunctionSqlAlloc(
        alloc,
        null,
        "SELECT * FROM antfly.graph_k_shortest_paths(table_name => 'docs', index => 'docs_edge_graph', result_ref => '$full_text_results', target_result_ref => '$graph_results.targets', start_limit => 5, target_limit => 2, k => 3, max_depth => 6);",
    );
    defer k_shortest_paths.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), k_shortest_paths.req.graph_queries.len);
    const k_shortest_paths_query = k_shortest_paths.req.graph_queries[0].query;
    try std.testing.expectEqual(@as(@TypeOf(k_shortest_paths_query.query_type), .k_shortest_paths), k_shortest_paths_query.query_type);
    try std.testing.expectEqual(@as(u32, 3), k_shortest_paths_query.k);
    try std.testing.expectEqual(@as(u32, 6), k_shortest_paths_query.params.max_depth);
    switch (k_shortest_paths_query.start_nodes) {
        .result_ref => |ref| {
            try std.testing.expectEqualStrings("$full_text_results", ref.ref);
            try std.testing.expectEqual(@as(u32, 5), ref.limit);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (k_shortest_paths_query.target_nodes orelse return error.TestUnexpectedResult) {
        .result_ref => |ref| {
            try std.testing.expectEqualStrings("$graph_results.targets", ref.ref);
            try std.testing.expectEqual(@as(u32, 2), ref.limit);
        },
        else => return error.TestUnexpectedResult,
    }

    var graph_match = try lowerAntflyQueryFunctionSqlAlloc(
        alloc,
        null,
        "SELECT * FROM antfly.graph_match(table_name => 'docs', name => 'citation_pattern', index => 'docs_edge_graph', start => 'doc:root', pattern => '(a)-[:cites|references {min_weight:0.25,max_weight:2.5}*1..3]->(b)<-[:mentions {weight >= 0.1, weight <= 1.0}]-(c)', return => 'b,c', metrics => 'pagerank', order_metric => 'pagerank', order_direction => 'desc', order_nulls => 'last', where_metric => 'pagerank', where_op => '>=', where_value => 0.25, freshness => 'published', include_metric_status => true, fields => 'title,url', max_results => 17);",
    );
    defer graph_match.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), graph_match.req.graph_queries.len);
    try std.testing.expectEqualStrings("citation_pattern", graph_match.req.graph_queries[0].name);
    const graph_match_query = graph_match.req.graph_queries[0].query;
    try std.testing.expectEqual(@as(@TypeOf(graph_match_query.query_type), .pattern), graph_match_query.query_type);
    try std.testing.expectEqualStrings("docs_edge_graph", graph_match_query.index_name);
    try std.testing.expectEqual(@as(usize, 3), graph_match_query.pattern.len);
    try std.testing.expectEqualStrings("a", graph_match_query.pattern[0].alias);
    try std.testing.expectEqualStrings("b", graph_match_query.pattern[1].alias);
    try std.testing.expectEqual(@as(@TypeOf(graph_match_query.pattern[1].edge.direction), .out), graph_match_query.pattern[1].edge.direction);
    try std.testing.expectEqual(@as(u32, 1), graph_match_query.pattern[1].edge.min_hops);
    try std.testing.expectEqual(@as(u32, 3), graph_match_query.pattern[1].edge.max_hops);
    try std.testing.expectEqual(@as(usize, 2), graph_match_query.pattern[1].edge.types.len);
    try std.testing.expectEqualStrings("cites", graph_match_query.pattern[1].edge.types[0]);
    try std.testing.expectEqualStrings("references", graph_match_query.pattern[1].edge.types[1]);
    try std.testing.expectApproxEqAbs(@as(f64, 0.25), graph_match_query.pattern[1].edge.min_weight, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 2.5), graph_match_query.pattern[1].edge.max_weight, 0.0001);
    try std.testing.expectEqualStrings("c", graph_match_query.pattern[2].alias);
    try std.testing.expectEqual(@as(@TypeOf(graph_match_query.pattern[2].edge.direction), .in), graph_match_query.pattern[2].edge.direction);
    try std.testing.expectEqual(@as(usize, 1), graph_match_query.pattern[2].edge.types.len);
    try std.testing.expectEqualStrings("mentions", graph_match_query.pattern[2].edge.types[0]);
    try std.testing.expectApproxEqAbs(@as(f64, 0.1), graph_match_query.pattern[2].edge.min_weight, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), graph_match_query.pattern[2].edge.max_weight, 0.0001);
    try std.testing.expectEqual(@as(usize, 2), graph_match_query.return_aliases.len);
    try std.testing.expectEqualStrings("b", graph_match_query.return_aliases[0]);
    try std.testing.expectEqualStrings("c", graph_match_query.return_aliases[1]);
    try std.testing.expectEqual(@as(u32, 17), graph_match_query.params.max_results);
    try std.testing.expectEqual(@as(usize, 1), graph_match_query.metrics.len);
    try std.testing.expectEqualStrings("pagerank", graph_match_query.metrics[0].name);
    try std.testing.expectEqual(@as(usize, 1), graph_match_query.order_by.len);
    try std.testing.expectEqualStrings("pagerank", graph_match_query.order_by[0].name);
    try std.testing.expectEqual(@as(@TypeOf(graph_match_query.order_by[0].direction), .desc), graph_match_query.order_by[0].direction);
    try std.testing.expectEqual(@as(@TypeOf(graph_match_query.order_by[0].nulls), .last), graph_match_query.order_by[0].nulls);
    try std.testing.expectEqual(@as(usize, 1), graph_match_query.where_metric.len);
    try std.testing.expectEqualStrings("pagerank", graph_match_query.where_metric[0].name);
    try std.testing.expectEqual(@as(@TypeOf(graph_match_query.where_metric[0].op), .gte), graph_match_query.where_metric[0].op);
    try std.testing.expectApproxEqAbs(@as(f64, 0.25), graph_match_query.where_metric[0].value, 0.0001);
    try std.testing.expectEqual(@as(usize, 2), graph_match_query.fields.len);
    try std.testing.expectEqualStrings("title", graph_match_query.fields[0]);
    try std.testing.expectEqualStrings("url", graph_match_query.fields[1]);
    try std.testing.expect(graph_match_query.include_metric_status);

    var graph_match_expression = try lowerAntflyQueryFunctionExpressionSqlAlloc(
        alloc,
        null,
        "antfly.graph_match(table_name => 'docs', name => 'citation_pattern', index => 'docs_edge_graph', start => 'doc:root', pattern => '(a)-[:cites]->(b)', return => 'b', max_results => 5)",
    );
    defer graph_match_expression.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), graph_match_expression.req.graph_queries.len);
    try std.testing.expectEqualStrings("citation_pattern", graph_match_expression.req.graph_queries[0].name);
    try std.testing.expectEqual(@as(@TypeOf(graph_match_expression.req.graph_queries[0].query.query_type), .pattern), graph_match_expression.req.graph_queries[0].query.query_type);
    try std.testing.expectEqual(@as(u32, 5), graph_match_expression.req.graph_queries[0].query.params.max_results);

    var expression_tokens = try lexer_mod.tokenizeAlloc(alloc, "antfly.graph_match(table_name => 'docs', index => 'docs_edge_graph', start => 'doc:a', pattern => '(a)') AS gm");
    defer lexer_mod.freeTokens(alloc, &expression_tokens);
    var expression_args = std.ArrayListUnmanaged(SqlQueryFunctionArg).empty;
    defer {
        deinitAntflyQueryFunctionArgs(alloc, expression_args.items);
        expression_args.deinit(alloc);
    }
    var expression_pos: usize = 0;
    const embedded_function = try parseAntflyQueryFunctionExpressionAlloc(alloc, expression_tokens.items, &expression_pos, &expression_args);
    try std.testing.expectEqual(AntflyQueryFunction.graph_match, embedded_function);
    try std.testing.expect(expression_pos < expression_tokens.items.len);
    try std.testing.expect(std.ascii.eqlIgnoreCase(expression_tokens.items[expression_pos].text, "as"));

    var graph_match_ref = try lowerAntflyQueryFunctionSqlAlloc(
        alloc,
        null,
        "SELECT * FROM antfly.graph_match(table_name => 'docs', graph_index => 'docs_edge_graph', result_ref => '$full_text_results', start_limit => 4, pattern => '(seed)--(neighbor)', return_aliases => 'neighbor');",
    );
    defer graph_match_ref.deinit(alloc);
    const graph_match_ref_query = graph_match_ref.req.graph_queries[0].query;
    try std.testing.expectEqual(@as(@TypeOf(graph_match_ref_query.query_type), .pattern), graph_match_ref_query.query_type);
    switch (graph_match_ref_query.start_nodes) {
        .result_ref => |ref| {
            try std.testing.expectEqualStrings("$full_text_results", ref.ref);
            try std.testing.expectEqual(@as(u32, 4), ref.limit);
        },
        else => return error.TestUnexpectedResult,
    }
    try std.testing.expectEqual(@as(usize, 2), graph_match_ref_query.pattern.len);
    try std.testing.expectEqual(@as(@TypeOf(graph_match_ref_query.pattern[1].edge.direction), .both), graph_match_ref_query.pattern[1].edge.direction);
    try std.testing.expectEqualStrings("neighbor", graph_match_ref_query.return_aliases[0]);

    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerAntflyQueryFunctionSqlAlloc(
            alloc,
            null,
            "SELECT * FROM antfly.graph_match(table_name => 'docs', index => 'docs_edge_graph', start => 'doc:a', pattern => '(a)');",
        ),
    );
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerAntflyQueryFunctionSqlAlloc(
            alloc,
            null,
            "SELECT * FROM antfly.graph_match(table_name => 'docs', index => 'docs_edge_graph', start => 'doc:a', pattern => '(a:Document)-[:cites]->(b)');",
        ),
    );
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerAntflyQueryFunctionSqlAlloc(
            alloc,
            null,
            "SELECT * FROM antfly.graph_match(table_name => 'docs', index => 'docs_edge_graph', start => 'doc:a', pattern => '(a)-[:cites|]->(b)');",
        ),
    );
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerAntflyQueryFunctionSqlAlloc(
            alloc,
            null,
            "SELECT * FROM antfly.graph_match(table_name => 'docs', index => 'docs_edge_graph', start => 'doc:a', pattern => '(a)-[:cites*0]->(b)');",
        ),
    );
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerAntflyQueryFunctionSqlAlloc(
            alloc,
            null,
            "SELECT * FROM antfly.graph_match(table_name => 'docs', index => 'docs_edge_graph', start => 'doc:a', pattern => '(a)-[:cites*3..1]->(b)');",
        ),
    );
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerAntflyQueryFunctionSqlAlloc(
            alloc,
            null,
            "SELECT * FROM antfly.graph_match(table_name => 'docs', index => 'docs_edge_graph', start => 'doc:a', pattern => '(a)-[:cites {min_weight:3,max_weight:1}]->(b)');",
        ),
    );
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerAntflyQueryFunctionSqlAlloc(
            alloc,
            null,
            "SELECT * FROM antfly.graph_match(table_name => 'docs', index => 'docs_edge_graph', start => 'doc:a', pattern => '(a)-[:cites {confidence:0.7}]->(b)');",
        ),
    );
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerAntflyQueryFunctionSqlAlloc(
            alloc,
            null,
            "SELECT * FROM antfly.graph_match(table_name => 'docs', index => 'docs_edge_graph', start => 'doc:a', pattern => '(a)-[:cites {min_weight:bad}]->(b)');",
        ),
    );
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerAntflyQueryFunctionSqlAlloc(
            alloc,
            null,
            "SELECT * FROM antfly.graph_match(table_name => 'docs', index => 'docs_edge_graph', start => 'doc:a', pattern => '(a)-[:cites]->(b)', where_metric => 'pagerank', where_op => '>=');",
        ),
    );
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerAntflyQueryFunctionSqlAlloc(
            alloc,
            null,
            "SELECT * FROM antfly.graph_match(table_name => 'docs', index => 'docs_edge_graph', start => 'doc:a', pattern => '(a)-[:cites]->(b)', order_direction => 'desc');",
        ),
    );

    var graph_metric = try lowerAntflyQueryFunctionSqlAlloc(
        alloc,
        null,
        "SELECT * FROM antfly.graph_metric(table_name => 'docs', index => 'docs_edge_graph', metric => 'pagerank', top_k => 2, freshness => 'fresh');",
    );
    defer graph_metric.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), graph_metric.req.graph_metric_queries.len);
    try std.testing.expectEqualStrings("docs_edge_graph", graph_metric.req.graph_metric_queries[0].query.index_name);
    try std.testing.expectEqualStrings("pagerank", graph_metric.req.graph_metric_queries[0].query.metric_name);
    try std.testing.expectEqual(@as(u32, 2), graph_metric.req.graph_metric_queries[0].query.top_k);
    try std.testing.expectEqual(db_mod.types.GraphMetricFreshness.fresh, graph_metric.req.graph_metric_queries[0].query.freshness);

    var graph_metric_tokens = try lexer_mod.tokenizeAlloc(
        alloc,
        "SELECT * FROM antfly.graph_metric(table_name => 'docs', index => 'docs_edge_graph', metric => 'pagerank', top_k => 2, freshness => 'fresh')",
    );
    defer lexer_mod.freeTokens(alloc, &graph_metric_tokens);
    var graph_metric_table_function = try lowerAntflyGraphTableFunctionTokensAlloc(alloc, graph_metric_tokens.items);
    defer graph_metric_table_function.deinit(alloc);
    switch (graph_metric_table_function) {
        .graph_metric_query => |metric_table_function| {
            try std.testing.expectEqualStrings("docs", metric_table_function.table_name);
            try std.testing.expectEqualStrings("docs_edge_graph", metric_table_function.query.query.index_name);
            try std.testing.expectEqualStrings("pagerank", metric_table_function.query.query.metric_name);
            try std.testing.expectEqual(@as(u32, 2), metric_table_function.query.query.top_k);
            try std.testing.expectEqual(db_mod.types.GraphMetricFreshness.fresh, metric_table_function.query.query.freshness);
        },
        else => return error.TestUnexpectedResult,
    }

    var graph_metric_rerank_tokens = try lexer_mod.tokenizeAlloc(
        alloc,
        "SELECT * FROM antfly.graph_metric_rerank(table_name => 'docs', full_text_index => 'docs_body_fts', field => 'body', query => 'refund', graph_index => 'docs_edge_graph', graph_metric => 'pagerank', weight => 1.5, base_weight => 0.25)",
    );
    defer lexer_mod.freeTokens(alloc, &graph_metric_rerank_tokens);
    var graph_metric_rerank_table_function = try lowerAntflyGraphTableFunctionTokensAlloc(alloc, graph_metric_rerank_tokens.items);
    defer graph_metric_rerank_table_function.deinit(alloc);
    switch (graph_metric_rerank_table_function) {
        .graph_metric_rerank_query => |rerank_table_function| {
            try std.testing.expectEqualStrings("docs", rerank_table_function.table_name);
            try std.testing.expectEqualStrings("docs_body_fts", rerank_table_function.request.primary_text_index_name.?);
            try std.testing.expect(rerank_table_function.request.full_text != null);
            try std.testing.expect(rerank_table_function.request.graph_metric_rerank != null);
            try std.testing.expectEqualStrings("docs_edge_graph", rerank_table_function.request.graph_metric_rerank.?.index_name);
            try std.testing.expectEqualStrings("pagerank", rerank_table_function.request.graph_metric_rerank.?.metric_name);
        },
        else => return error.TestUnexpectedResult,
    }

    var rerank = try lowerAntflyQueryFunctionSqlAlloc(
        alloc,
        null,
        "SELECT * FROM antfly.graph_metric_rerank(table_name => 'docs', full_text_index => 'docs_body_fts', field => 'body', query => 'refund', graph_index => 'docs_edge_graph', graph_metric => 'pagerank', weight => 1.5, base_weight => 0.25);",
    );
    defer rerank.deinit(alloc);
    try std.testing.expectEqualStrings("docs_body_fts", rerank.req.primary_text_index_name.?);
    try std.testing.expect(rerank.req.graph_metric_rerank != null);
    try std.testing.expectEqualStrings("docs_edge_graph", rerank.req.graph_metric_rerank.?.index_name);
    try std.testing.expectApproxEqAbs(@as(f64, 1.5), rerank.req.graph_metric_rerank.?.weight, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 0.25), rerank.req.graph_metric_rerank.?.base_weight, 0.0001);

    var hybrid = try lowerAntflyQueryFunctionSqlAlloc(
        alloc,
        resolver,
        "SELECT * FROM antfly.hybrid_search(table_name => 'docs', full_text_index => 'docs_body_fts', semantic_index => 'docs_body_semantic', graph_index => 'docs_edge_graph', graph_metric => 'pagerank', field => 'body', query => 'hybrid refund', fusion => 'rrf', limit => 9);",
    );
    defer hybrid.deinit(alloc);
    try std.testing.expectEqualStrings("docs_body_fts", hybrid.req.primary_text_index_name.?);
    try std.testing.expect(hybrid.req.full_text != null);
    try std.testing.expectEqual(@as(usize, 1), hybrid.req.dense_queries.len);
    try std.testing.expect(hybrid.req.graph_metric_rerank != null);
    try std.testing.expect(hybrid.req.merge_config != null);
    try std.testing.expectEqual(@as(u32, 9), hybrid.req.limit);

    var structured_hybrid = try lowerAntflyQueryFunctionSqlAlloc(alloc, resolver,
        \\SELECT * FROM antfly.hybrid_search(table_name => 'docs', query => 'hybrid refund', fusion => 'rrf', window_size => 25, sources_json => '[{"kind":"full_text","index":"docs_body_fts","field":"body","weight":0.25},{"kind":"semantic","index":"docs_body_semantic","weight":0.6},{"kind":"graph_metric","index":"docs_edge_graph","metric":"pagerank","weight":0.15,"base_weight":0.5,"missing_score":0.1,"freshness":"fresh"}]', limit => 9);
    );
    defer structured_hybrid.deinit(alloc);
    try std.testing.expectEqualStrings("docs_body_fts", structured_hybrid.req.primary_text_index_name.?);
    try std.testing.expect(structured_hybrid.req.full_text != null);
    try std.testing.expect(structured_hybrid.req.full_text.? == .match);
    try std.testing.expectEqualStrings("body", structured_hybrid.req.full_text.?.match.field);
    try std.testing.expectEqualStrings("hybrid refund", structured_hybrid.req.full_text.?.match.text);
    try std.testing.expectEqual(@as(usize, 1), structured_hybrid.req.dense_queries.len);
    try std.testing.expectEqualStrings("docs_body_semantic", structured_hybrid.req.dense_queries[0].index_name);
    try std.testing.expect(structured_hybrid.req.graph_metric_rerank != null);
    try std.testing.expectEqualStrings("docs_edge_graph", structured_hybrid.req.graph_metric_rerank.?.index_name);
    try std.testing.expectEqualStrings("pagerank", structured_hybrid.req.graph_metric_rerank.?.metric_name);
    try std.testing.expectEqual(db_mod.types.GraphMetricFreshness.fresh, structured_hybrid.req.graph_metric_rerank.?.freshness);
    try std.testing.expectApproxEqAbs(@as(f64, 0.15), structured_hybrid.req.graph_metric_rerank.?.weight, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 0.5), structured_hybrid.req.graph_metric_rerank.?.base_weight, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 0.1), structured_hybrid.req.graph_metric_rerank.?.missing_score, 0.0001);
    const structured_merge = structured_hybrid.req.merge_config orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(u32, 25), structured_merge.window_size);
    try std.testing.expectEqual(@as(usize, 3), structured_merge.weights.len);
    try expectMergeWeight(structured_merge.weights, "full_text_search", 0.25);
    try expectMergeWeight(structured_merge.weights, "docs_body_semantic", 0.6);
    try expectMergeWeight(structured_merge.weights, "graph_metric_rerank", 0.15);

    var helper_hybrid = try lowerAntflyQueryFunctionSqlAlloc(alloc, resolver,
        \\SELECT * FROM antfly.hybrid_search(table_name => 'docs', query => 'hybrid refund', fusion => 'rrf', window_size => 25, sources => ARRAY[antfly.source('docs_body_fts', field => 'body', weight => 0.25), antfly.source('docs_body_semantic', weight => 0.6), antfly.source('docs_edge_graph', metric => 'pagerank', weight => 0.15, base_weight => 0.5, missing_score => 0.1, freshness => 'fresh')], limit => 9);
    );
    defer helper_hybrid.deinit(alloc);
    try std.testing.expectEqualStrings("docs_body_fts", helper_hybrid.req.primary_text_index_name.?);
    try std.testing.expect(helper_hybrid.req.full_text != null);
    try std.testing.expect(helper_hybrid.req.full_text.? == .match);
    try std.testing.expectEqualStrings("body", helper_hybrid.req.full_text.?.match.field);
    try std.testing.expectEqual(@as(usize, 1), helper_hybrid.req.dense_queries.len);
    try std.testing.expectEqualStrings("docs_body_semantic", helper_hybrid.req.dense_queries[0].index_name);
    try std.testing.expect(helper_hybrid.req.graph_metric_rerank != null);
    try std.testing.expectEqualStrings("docs_edge_graph", helper_hybrid.req.graph_metric_rerank.?.index_name);
    try std.testing.expectEqualStrings("pagerank", helper_hybrid.req.graph_metric_rerank.?.metric_name);
    try std.testing.expectEqual(db_mod.types.GraphMetricFreshness.fresh, helper_hybrid.req.graph_metric_rerank.?.freshness);
    const helper_merge = helper_hybrid.req.merge_config orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(u32, 25), helper_merge.window_size);
    try std.testing.expectEqual(@as(usize, 3), helper_merge.weights.len);
    try expectMergeWeight(helper_merge.weights, "full_text_search", 0.25);
    try expectMergeWeight(helper_merge.weights, "docs_body_semantic", 0.6);
    try expectMergeWeight(helper_merge.weights, "graph_metric_rerank", 0.15);
}

test "sql adapter query function read accepts projected hit columns" {
    const alloc = std.testing.allocator;

    var parsed_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT _id, _score FROM antfly.full_text_search(table_name => 'docs', index => 'docs_body_fts', field => 'body', query => 'refund policy', limit => 5);",
    );
    defer parsed_sql.deinit(alloc);

    var lowered = try lowerAntflyQueryFunctionReadParsedSqlAlloc(alloc, null, &parsed_sql);
    defer lowered.deinit(alloc);
    try std.testing.expectEqualStrings("docs", lowered.table_name);
    try std.testing.expectEqual(@as(usize, 2), lowered.projection_columns.len);
    try std.testing.expectEqualStrings("_id", lowered.projection_columns[0]);
    try std.testing.expectEqualStrings("_score", lowered.projection_columns[1]);
    try std.testing.expectEqual(@as(u32, 5), lowered.request.req.limit);
    try std.testing.expectEqualStrings("docs_body_fts", lowered.request.req.primary_text_index_name.?);
}

test "sql adapter query function read keeps projected columns for derived search functions" {
    const alloc = std.testing.allocator;

    const Resolver = struct {
        fn resolve(
            _: *anyopaque,
            allocator: std.mem.Allocator,
            table_name: []const u8,
            index_name: []const u8,
            semantic_search: []const u8,
            embedding_template: ?[]const u8,
            limit: u32,
        ) !db_mod.types.DenseKnnQuery {
            _ = embedding_template;
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expect(index_name.len > 0);
            try std.testing.expect(semantic_search.len > 0);
            return .{
                .vector = try allocator.dupe(f32, &[_]f32{ 0.25, 0.5, 0.75 }),
                .k = limit,
            };
        }
    };
    var resolver_state: u8 = 0;
    const resolver = query_contract.SemanticResolver{
        .ptr = &resolver_state,
        .vtable = &.{ .resolve_dense_query = Resolver.resolve },
    };

    var semantic_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT _id, _score, _source FROM antfly.semantic_search(table_name => 'docs', index => 'docs_body_semantic', query => 'automatic embeddings', limit => 7);",
    );
    defer semantic_sql.deinit(alloc);
    var semantic = try lowerAntflyQueryFunctionReadParsedSqlAlloc(alloc, resolver, &semantic_sql);
    defer semantic.deinit(alloc);
    try std.testing.expectEqualStrings("docs", semantic.table_name);
    try std.testing.expectEqual(@as(usize, 3), semantic.projection_columns.len);
    try std.testing.expectEqualStrings("_id", semantic.projection_columns[0]);
    try std.testing.expectEqualStrings("_score", semantic.projection_columns[1]);
    try std.testing.expectEqualStrings("_source", semantic.projection_columns[2]);
    try std.testing.expectEqual(@as(u32, 7), semantic.request.req.limit);
    try std.testing.expectEqual(@as(usize, 1), semantic.request.req.dense_queries.len);
    try std.testing.expectEqualStrings("docs_body_semantic", semantic.request.req.dense_queries[0].index_name);
    try std.testing.expectEqual(@as(u32, 7), semantic.request.req.dense_queries[0].query.k);

    var hybrid_sql = try tokenized.ParsedSql.initAlloc(alloc,
        \\SELECT _id, _score FROM antfly.hybrid_search(table_name => 'docs', query => 'hybrid refund', fusion => 'rrf', window_size => 25, sources => ARRAY[antfly.source('docs_body_fts', field => 'body', weight => 0.25), antfly.source('docs_body_semantic', weight => 0.6), antfly.source('docs_edge_graph', metric => 'pagerank', weight => 0.15, base_weight => 0.5, missing_score => 0.1, freshness => 'fresh')], limit => 9);
    );
    defer hybrid_sql.deinit(alloc);
    var hybrid = try lowerAntflyQueryFunctionReadParsedSqlAlloc(alloc, resolver, &hybrid_sql);
    defer hybrid.deinit(alloc);
    try std.testing.expectEqualStrings("docs", hybrid.table_name);
    try std.testing.expectEqual(@as(usize, 2), hybrid.projection_columns.len);
    try std.testing.expectEqualStrings("_id", hybrid.projection_columns[0]);
    try std.testing.expectEqualStrings("_score", hybrid.projection_columns[1]);
    try std.testing.expectEqual(@as(u32, 9), hybrid.request.req.limit);
    try std.testing.expectEqualStrings("docs_body_fts", hybrid.request.req.primary_text_index_name.?);
    try std.testing.expect(hybrid.request.req.full_text != null);
    try std.testing.expectEqual(@as(usize, 1), hybrid.request.req.dense_queries.len);
    try std.testing.expect(hybrid.request.req.graph_metric_rerank != null);
    try std.testing.expect(hybrid.request.req.merge_config != null);

    var graph_metric_sql = try tokenized.ParsedSql.initAlloc(
        alloc,
        "SELECT _id, _score FROM antfly.graph_metric(table_name => 'docs', index => 'docs_edge_graph', metric => 'pagerank', top_k => 2, freshness => 'fresh', limit => 2);",
    );
    defer graph_metric_sql.deinit(alloc);
    var graph_metric = try lowerAntflyQueryFunctionReadParsedSqlAlloc(alloc, null, &graph_metric_sql);
    defer graph_metric.deinit(alloc);
    try std.testing.expectEqualStrings("docs", graph_metric.table_name);
    try std.testing.expectEqual(@as(usize, 2), graph_metric.projection_columns.len);
    try std.testing.expectEqualStrings("_id", graph_metric.projection_columns[0]);
    try std.testing.expectEqualStrings("_score", graph_metric.projection_columns[1]);
    try std.testing.expectEqual(@as(u32, 2), graph_metric.request.req.limit);
    try std.testing.expectEqual(@as(usize, 1), graph_metric.request.req.graph_metric_queries.len);
    try std.testing.expectEqualStrings("docs_edge_graph", graph_metric.request.req.graph_metric_queries[0].query.index_name);
    try std.testing.expectEqualStrings("pagerank", graph_metric.request.req.graph_metric_queries[0].query.metric_name);
}

fn expectMergeWeight(weights: []const @import("../search/fusion.zig").NamedWeight, name: []const u8, expected: f64) !void {
    for (weights) |weight| {
        if (std.mem.eql(u8, weight.name, name)) {
            try std.testing.expectApproxEqAbs(expected, weight.weight, 0.0001);
            return;
        }
    }
    return error.TestUnexpectedResult;
}
