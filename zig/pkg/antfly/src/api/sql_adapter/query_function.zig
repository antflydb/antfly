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

const db_mod = @import("../../storage/db/mod.zig");
const query_contract = @import("../query_contract.zig");
const lexer_mod = @import("lexer.zig");
const token_mod = @import("token.zig");

const Token = token_mod.Token;
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
    try expectSqlKeyword(tokens, pos, "select");
    _ = try expectSqlToken(tokens, pos, .star);
    try expectSqlKeyword(tokens, pos, "from");
    const function_token = try expectSqlToken(tokens, pos, .identifier);
    const function = antflyQueryFunctionFromSqlName(function_token.text) orelse return error.UnsupportedSqlShape;
    _ = try expectSqlToken(tokens, pos, .lparen);
    if (matchSqlToken(tokens, pos, .rparen) == null) {
        while (true) {
            const name = (try expectSqlToken(tokens, pos, .identifier)).text;
            _ = try expectSqlToken(tokens, pos, .eq);
            _ = matchSqlToken(tokens, pos, .gt);
            const value = try parseAntflyQueryFunctionArgValueAlloc(alloc, tokens, pos, name);
            if (antflyQueryFunctionArg(args.items, name) != null) return error.UnsupportedSqlShape;
            try args.append(alloc, .{ .name = name, .value = value });
            if (matchSqlToken(tokens, pos, .comma) == null) break;
        }
        _ = try expectSqlToken(tokens, pos, .rparen);
    }
    _ = matchSqlToken(tokens, pos, .semicolon);
    if (pos.* != tokens.len) return error.UnsupportedSqlShape;
    return function;
}

fn parseAntflyQueryFunctionArgValueAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    name: []const u8,
) !SqlQueryFunctionArgValue {
    if (std.ascii.eqlIgnoreCase(name, "sources") and antflySourcesArrayCanStart(tokens, pos.*)) {
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
            if (std.ascii.eqlIgnoreCase(value_token.text, "true")) break :blk .{ .boolean = true };
            if (std.ascii.eqlIgnoreCase(value_token.text, "false")) break :blk .{ .boolean = false };
            break :blk .{ .string = value_token.text };
        },
        else => unreachable,
    };
}

fn antflySourcesArrayCanStart(tokens: []const Token, pos: usize) bool {
    if (pos >= tokens.len) return false;
    return tokens[pos].kind == .lbracket or
        (tokens[pos].kind == .identifier and std.ascii.eqlIgnoreCase(tokens[pos].text, "array"));
}

fn parseAntflySourceArrayAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) ![]const HybridSqlSourceSpec {
    if (matchSqlKeyword(tokens, pos, "array")) |_| {}
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
    const function_name = (try expectSqlToken(tokens, pos, .identifier)).text;
    const prefix = "antfly.";
    const local = if (std.mem.startsWith(u8, function_name, prefix)) function_name[prefix.len..] else function_name;
    if (!std.ascii.eqlIgnoreCase(local, "source")) return error.UnsupportedSqlShape;
    _ = try expectSqlToken(tokens, pos, .lparen);
    const index = try parseAntflySourceStringLikeValue(tokens, pos);
    var source = HybridSqlSourceSpec{
        .index = index,
        .kind = "",
    };
    while (matchSqlToken(tokens, pos, .comma) != null) {
        const field_name = (try expectSqlToken(tokens, pos, .identifier)).text;
        _ = try expectSqlToken(tokens, pos, .eq);
        _ = matchSqlToken(tokens, pos, .gt);
        if (std.ascii.eqlIgnoreCase(field_name, "kind") or std.ascii.eqlIgnoreCase(field_name, "type")) {
            if (source.kind.len != 0) return error.UnsupportedSqlShape;
            source.kind = try parseAntflySourceStringLikeValue(tokens, pos);
        } else if (std.ascii.eqlIgnoreCase(field_name, "name")) {
            if (source.name != null) return error.UnsupportedSqlShape;
            source.name = try parseAntflySourceStringLikeValue(tokens, pos);
        } else if (std.ascii.eqlIgnoreCase(field_name, "field")) {
            if (source.field != null) return error.UnsupportedSqlShape;
            source.field = try parseAntflySourceStringLikeValue(tokens, pos);
        } else if (std.ascii.eqlIgnoreCase(field_name, "metric") or std.ascii.eqlIgnoreCase(field_name, "graph_metric")) {
            if (source.metric != null) return error.UnsupportedSqlShape;
            source.metric = try parseAntflySourceStringLikeValue(tokens, pos);
        } else if (std.ascii.eqlIgnoreCase(field_name, "freshness") or std.ascii.eqlIgnoreCase(field_name, "metric_freshness")) {
            if (source.freshness != null) return error.UnsupportedSqlShape;
            source.freshness = try parseAntflySourceStringLikeValue(tokens, pos);
        } else if (std.ascii.eqlIgnoreCase(field_name, "weight")) {
            if (source.weight != null) return error.UnsupportedSqlShape;
            source.weight = try parseAntflySourceNumberValue(tokens, pos);
        } else if (std.ascii.eqlIgnoreCase(field_name, "base_weight")) {
            if (source.base_weight != null) return error.UnsupportedSqlShape;
            source.base_weight = try parseAntflySourceNumberValue(tokens, pos);
        } else if (std.ascii.eqlIgnoreCase(field_name, "missing_score")) {
            if (source.missing_score != null) return error.UnsupportedSqlShape;
            source.missing_score = try parseAntflySourceNumberValue(tokens, pos);
        } else {
            return error.UnsupportedSqlShape;
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
    const prefix = "antfly.";
    const local = if (std.mem.startsWith(u8, name, prefix)) name[prefix.len..] else name;
    if (std.ascii.eqlIgnoreCase(local, "full_text_search")) return .full_text_search;
    if (std.ascii.eqlIgnoreCase(local, "semantic_search")) return .semantic_search;
    if (std.ascii.eqlIgnoreCase(local, "vector_search")) return .vector_search;
    if (std.ascii.eqlIgnoreCase(local, "graph_traverse")) return .graph_traverse;
    if (std.ascii.eqlIgnoreCase(local, "graph_neighbors")) return .graph_neighbors;
    if (std.ascii.eqlIgnoreCase(local, "graph_shortest_path")) return .graph_shortest_path;
    if (std.ascii.eqlIgnoreCase(local, "graph_k_shortest_paths")) return .graph_k_shortest_paths;
    if (std.ascii.eqlIgnoreCase(local, "graph_match")) return .graph_match;
    if (std.ascii.eqlIgnoreCase(local, "graph_metric")) return .graph_metric;
    if (std.ascii.eqlIgnoreCase(local, "graph_metric_rerank")) return .graph_metric_rerank;
    if (std.ascii.eqlIgnoreCase(local, "hybrid_search")) return .hybrid_search;
    return null;
}

fn expectSqlKeyword(tokens: []const Token, pos: *usize, keyword: []const u8) !void {
    const token = try expectSqlToken(tokens, pos, .identifier);
    if (!std.ascii.eqlIgnoreCase(token.text, keyword)) return error.UnsupportedSqlShape;
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

fn matchSqlKeyword(tokens: []const Token, pos: *usize, keyword: []const u8) ?Token {
    if (pos.* >= tokens.len or tokens[pos.*].kind != .identifier) return null;
    if (!std.ascii.eqlIgnoreCase(tokens[pos.*].text, keyword)) return null;
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
    var tokens = try lexer_mod.tokenizeAlloc(alloc, sql);
    defer lexer_mod.freeTokens(alloc, &tokens);

    var args = std.ArrayListUnmanaged(SqlQueryFunctionArg).empty;
    defer {
        deinitAntflyQueryFunctionArgs(alloc, args.items);
        args.deinit(alloc);
    }
    var pos: usize = 0;
    const function = try parseAntflyQueryFunctionCall(alloc, tokens.items, &pos, &args);

    const table_name = antflyQueryFunctionStringArg(args.items, "table_name") orelse
        antflyQueryFunctionStringArg(args.items, "table") orelse return error.UnsupportedSqlShape;
    const structured_primary_text_index_name = if (function == .hybrid_search)
        try hybridSourcesPrimaryTextIndexAlloc(alloc, args.items)
    else
        null;
    defer if (structured_primary_text_index_name) |index_name| alloc.free(index_name);
    const primary_text_index_name = antflyQueryFunctionStringArg(args.items, "full_text_index") orelse
        antflyQueryFunctionStringArg(args.items, "text_index") orelse
        if (function == .full_text_search) antflyQueryFunctionStringArg(args.items, "index") else structured_primary_text_index_name;

    var body = std.ArrayListUnmanaged(u8).empty;
    defer body.deinit(alloc);
    try body.append(alloc, '{');
    var first = true;
    switch (function) {
        .full_text_search => try appendFullTextFunctionBody(alloc, &body, &first, args.items),
        .semantic_search => try appendSemanticFunctionBody(alloc, &body, &first, args.items),
        .vector_search => try appendVectorFunctionBody(alloc, &body, &first, args.items),
        .graph_traverse, .graph_neighbors, .graph_shortest_path, .graph_k_shortest_paths => try appendGraphSearchFunctionBody(alloc, &body, &first, function, args.items),
        .graph_match => try appendGraphMatchFunctionBody(alloc, &body, &first, args.items),
        .graph_metric => try appendGraphMetricFunctionBody(alloc, &body, &first, args.items),
        .graph_metric_rerank => try appendGraphMetricRerankFunctionBody(alloc, &body, &first, args.items),
        .hybrid_search => try appendHybridFunctionBody(alloc, &body, &first, args.items),
    }
    try appendCommonAntflyQueryFunctionOptions(alloc, &body, &first, args.items);
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
    const raw_types = if (star_index) |index| trimmed[0..index] else trimmed;
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
        "SELECT * FROM antfly.graph_match(table_name => 'docs', name => 'citation_pattern', index => 'docs_edge_graph', start => 'doc:root', pattern => '(a)-[:cites|references*1..3]->(b)<-[:mentions]-(c)', return => 'b,c', metrics => 'pagerank', order_metric => 'pagerank', order_direction => 'desc', order_nulls => 'last', where_metric => 'pagerank', where_op => '>=', where_value => 0.25, freshness => 'published', include_metric_status => true, fields => 'title,url', max_results => 17);",
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
    try std.testing.expectEqualStrings("c", graph_match_query.pattern[2].alias);
    try std.testing.expectEqual(@as(@TypeOf(graph_match_query.pattern[2].edge.direction), .in), graph_match_query.pattern[2].edge.direction);
    try std.testing.expectEqual(@as(usize, 1), graph_match_query.pattern[2].edge.types.len);
    try std.testing.expectEqualStrings("mentions", graph_match_query.pattern[2].edge.types[0]);
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

fn expectMergeWeight(weights: []const @import("../../search/fusion.zig").NamedWeight, name: []const u8, expected: f64) !void {
    for (weights) |weight| {
        if (std.mem.eql(u8, weight.name, name)) {
            try std.testing.expectApproxEqAbs(expected, weight.weight, 0.0001);
            return;
        }
    }
    return error.TestUnexpectedResult;
}
