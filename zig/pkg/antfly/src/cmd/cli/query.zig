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
const antfly_client = @import("antfly-client");
const cli = @import("mod.zig");

pub fn run(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    var table_name: ?[]const u8 = null;
    var catalog = cli.CatalogFlags.defaultsFromEnv();
    var full_text_search: ?[]const u8 = null;
    var full_text_search_json: ?[]const u8 = null;
    var semantic_search: ?[]const u8 = null;
    var fields_str: ?[]const u8 = null;
    var limit: ?i64 = null;
    var offset: ?i64 = null;
    var indexes_str: ?[]const u8 = null;
    var filter_query: ?[]const u8 = null;
    var exclusion_query: ?[]const u8 = null;
    var aggregations_json: ?[]const u8 = null;
    var reranker_json: ?[]const u8 = null;
    var pruner_json: ?[]const u8 = null;

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
            table_name = args.next();
        } else if (cli.parseCatalogFlag(&catalog, arg, args)) {
            continue;
        } else if (std.mem.eql(u8, arg, "--full-text-search")) {
            full_text_search = args.next();
        } else if (std.mem.eql(u8, arg, "--full-text-search-json")) {
            full_text_search_json = args.next();
        } else if (std.mem.eql(u8, arg, "--semantic-search")) {
            semantic_search = args.next();
        } else if (std.mem.eql(u8, arg, "--fields")) {
            fields_str = args.next();
        } else if (std.mem.eql(u8, arg, "--limit")) {
            if (args.next()) |s| limit = std.fmt.parseInt(i64, s, 10) catch null;
        } else if (std.mem.eql(u8, arg, "--offset")) {
            if (args.next()) |s| offset = std.fmt.parseInt(i64, s, 10) catch null;
        } else if (std.mem.eql(u8, arg, "--indexes") or std.mem.eql(u8, arg, "-i")) {
            indexes_str = args.next();
        } else if (std.mem.eql(u8, arg, "--filter-query")) {
            filter_query = args.next();
        } else if (std.mem.eql(u8, arg, "--exclusion-query")) {
            exclusion_query = args.next();
        } else if (std.mem.eql(u8, arg, "--aggregations")) {
            aggregations_json = args.next();
        } else if (std.mem.eql(u8, arg, "--reranker")) {
            reranker_json = args.next();
        } else if (std.mem.eql(u8, arg, "--pruner")) {
            pruner_json = args.next();
        } else {
            cli.fatal("unknown query flag: {s}", .{arg});
        }
    }

    if (full_text_search != null and full_text_search_json != null) {
        cli.fatal("only one of --full-text-search or --full-text-search-json may be provided", .{});
    }

    var full_text_value: ?std.json.Parsed(std.json.Value) = null;
    defer if (full_text_value) |*parsed| parsed.deinit();
    if (full_text_search) |q| {
        full_text_value = buildFullTextSearchValue(allocator, q);
    } else if (full_text_search_json) |q| {
        full_text_value = parseJsonArg(std.json.Value, allocator, "--full-text-search-json", q);
    }

    var filter_value: ?std.json.Parsed(std.json.Value) = null;
    defer if (filter_value) |*parsed| parsed.deinit();
    if (filter_query) |raw| filter_value = parseJsonArg(std.json.Value, allocator, "--filter-query", raw);

    var exclusion_value: ?std.json.Parsed(std.json.Value) = null;
    defer if (exclusion_value) |*parsed| parsed.deinit();
    if (exclusion_query) |raw| exclusion_value = parseJsonArg(std.json.Value, allocator, "--exclusion-query", raw);

    var aggregations_value: ?std.json.Parsed(std.json.ArrayHashMap(antfly_client.types.AggregationRequest)) = null;
    defer if (aggregations_value) |*parsed| parsed.deinit();
    if (aggregations_json) |raw| {
        aggregations_value = parseJsonArg(std.json.ArrayHashMap(antfly_client.types.AggregationRequest), allocator, "--aggregations", raw);
    }

    var reranker_value: ?std.json.Parsed(antfly_client.types.RerankerConfig) = null;
    defer if (reranker_value) |*parsed| parsed.deinit();
    if (reranker_json) |raw| reranker_value = parseJsonArg(antfly_client.types.RerankerConfig, allocator, "--reranker", raw);

    var pruner_value: ?std.json.Parsed(antfly_client.types.Pruner) = null;
    defer if (pruner_value) |*parsed| parsed.deinit();
    if (pruner_json) |raw| pruner_value = parseJsonArg(antfly_client.types.Pruner, allocator, "--pruner", raw);

    var fields: ?[]const []const u8 = null;
    defer if (fields) |slice| allocator.free(slice);
    if (fields_str) |raw| fields = try cli.splitCommaListAlloc(allocator, raw);

    var indexes: ?[]const []const u8 = null;
    defer if (indexes) |slice| allocator.free(slice);
    if (indexes_str) |raw| indexes = try cli.splitCommaListAlloc(allocator, raw);

    const body = antfly_client.types.QueryRequest{
        .full_text_search = if (full_text_value) |*parsed| parsed.value else null,
        .semantic_search = semantic_search,
        .indexes = indexes,
        .fields = fields,
        .limit = limit,
        .offset = offset,
        .filter_query = if (filter_value) |*parsed| parsed.value else null,
        .exclusion_query = if (exclusion_value) |*parsed| parsed.value else null,
        .aggregations = if (aggregations_value) |*parsed| parsed.value else null,
        .reranker = if (reranker_value) |*parsed| parsed.value else null,
        .pruner = if (pruner_value) |*parsed| parsed.value else null,
    };

    if (table_name) |tbl| {
        var resp = if (catalog.explicit()) |explicit|
            try client.inner.queryNamespaceTable(explicit.database, explicit.namespace, tbl, body)
        else
            try client.queryTable(tbl, body);
        defer resp.deinit();
        if (resp.data) |data| {
            try cli.writeJson(allocator, io, data.value);
        }
    } else {
        if (catalog.explicit() != null) {
            cli.fatal("--database/--namespace require --table for query commands", .{});
        }
        var resp = try client.query(body);
        defer resp.deinit();
        if (resp.data) |data| {
            try cli.writeJson(allocator, io, data.value);
        }
    }
}

pub fn lookup(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    var table_name: ?[]const u8 = null;
    var catalog = cli.CatalogFlags.defaultsFromEnv();
    var key: ?[]const u8 = null;

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
            table_name = args.next();
        } else if (cli.parseCatalogFlag(&catalog, arg, args)) {
            continue;
        } else if (std.mem.eql(u8, arg, "--key") or std.mem.eql(u8, arg, "-k")) {
            key = args.next();
        }
    }

    const tbl = table_name orelse cli.fatal("--table is required", .{});
    const k = key orelse cli.fatal("--key is required", .{});

    var resp = if (catalog.explicit()) |explicit|
        try client.inner.lookupNamespaceTableDocument(explicit.database, explicit.namespace, tbl, k, .{})
    else
        try client.lookupKey(tbl, k, .{});
    defer resp.deinit();
    if (resp.data) |data| {
        try cli.writeJson(allocator, io, data.value);
    }
}

fn buildFullTextSearchValue(allocator: std.mem.Allocator, query: []const u8) std.json.Parsed(std.json.Value) {
    const escaped = std.json.Stringify.valueAlloc(allocator, query, .{}) catch |err| {
        cli.fatal("failed to encode --full-text-search: {}", .{err});
    };
    defer allocator.free(escaped);

    const json_body = std.fmt.allocPrint(allocator, "{{\"query\":{s}}}", .{escaped}) catch |err| {
        cli.fatal("failed to build --full-text-search value: {}", .{err});
    };
    defer allocator.free(json_body);

    return parseJsonArg(std.json.Value, allocator, "--full-text-search", json_body);
}

fn parseJsonArg(comptime T: type, allocator: std.mem.Allocator, flag: []const u8, raw: []const u8) std.json.Parsed(T) {
    return std.json.parseFromSlice(T, allocator, raw, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    }) catch |err| {
        cli.fatal("invalid JSON for {s}: {}", .{ flag, err });
    };
}
