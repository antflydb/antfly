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
};

pub const SqlQueryFunctionArg = struct {
    name: []const u8,
    value: SqlQueryFunctionArgValue,
};

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
            const value_token = if (matchSqlToken(tokens, pos, .string)) |token|
                token
            else if (matchSqlToken(tokens, pos, .number)) |token|
                token
            else
                try expectSqlToken(tokens, pos, .identifier);
            const value: SqlQueryFunctionArgValue = switch (value_token.kind) {
                .string => .{ .string = value_token.text },
                .number => .{ .number = value_token.text },
                .identifier => blk: {
                    if (std.ascii.eqlIgnoreCase(value_token.text, "true")) break :blk .{ .boolean = true };
                    if (std.ascii.eqlIgnoreCase(value_token.text, "false")) break :blk .{ .boolean = false };
                    break :blk .{ .string = value_token.text };
                },
                else => unreachable,
            };
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
    defer args.deinit(alloc);
    var pos: usize = 0;
    const function = try parseAntflyQueryFunctionCall(alloc, tokens.items, &pos, &args);

    const table_name = antflyQueryFunctionStringArg(args.items, "table_name") orelse
        antflyQueryFunctionStringArg(args.items, "table") orelse return error.UnsupportedSqlShape;
    const primary_text_index_name = antflyQueryFunctionStringArg(args.items, "full_text_index") orelse
        antflyQueryFunctionStringArg(args.items, "text_index") orelse
        if (function == .full_text_search) antflyQueryFunctionStringArg(args.items, "index") else null;

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
            if (item.len == 0) return error.UnsupportedSqlShape;
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
            if (!graphPatternDigitsOnly(min) or !graphPatternDigitsOnly(max)) return error.UnsupportedSqlShape;
            try appendAntflySqlJsonNumberField(alloc, out, first, "min_hops", min);
            try appendAntflySqlJsonNumberField(alloc, out, first, "max_hops", max);
        } else {
            if (!graphPatternDigitsOnly(quantifier)) return error.UnsupportedSqlShape;
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
    try out.append(alloc, '{');
    var first = true;
    if (alias.len > 0) try appendAntflySqlJsonStringField(alloc, out, &first, "alias", alias);
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
