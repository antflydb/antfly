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
const index_readiness = @import("index_readiness.zig");

// Readiness is advisory for a query: keep its control-plane lookup bounded so
// an unhealthy status endpoint cannot hold the data-plane request hostage.
const semantic_readiness_timeout_ms: u64 = 1_500;

const QueryOptions = struct {
    table_name: ?[]const u8 = null,
    full_text_search: ?[]const u8 = null,
    full_text_search_json: ?[]const u8 = null,
    semantic_search: ?[]const u8 = null,
    fields_str: ?[]const u8 = null,
    limit: ?i64 = null,
    offset: ?i64 = null,
    indexes_str: ?[]const u8 = null,
    filter_query: ?[]const u8 = null,
    exclusion_query: ?[]const u8 = null,
    aggregations_json: ?[]const u8 = null,
    reranker_json: ?[]const u8 = null,
    pruner_json: ?[]const u8 = null,
};

const QueryParseIssue = union(enum) {
    missing_value: []const u8,
    duplicate: []const u8,
    unknown: []const u8,
    invalid_integer: struct { flag: []const u8, value: []const u8 },
    non_positive: struct { flag: []const u8, value: []const u8 },
    negative: struct { flag: []const u8, value: []const u8 },
    too_large: struct { flag: []const u8, value: []const u8 },
    conflicting_search: void,
    semantic_offset: void,
    missing_table: void,
};

const QueryParseResult = union(enum) { value: QueryOptions, issue: QueryParseIssue };

fn parseQueryOptions(iterator: std.process.Args.Iterator) QueryParseResult {
    var args = iterator;
    var options: QueryOptions = .{};
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
            if (options.table_name != null) return .{ .issue = .{ .duplicate = arg } };
            options.table_name = args.next() orelse return .{ .issue = .{ .missing_value = arg } };
        } else if (std.mem.eql(u8, arg, "--full-text-search")) {
            if (options.full_text_search != null) return .{ .issue = .{ .duplicate = arg } };
            options.full_text_search = args.next() orelse return .{ .issue = .{ .missing_value = arg } };
        } else if (std.mem.eql(u8, arg, "--full-text-search-json")) {
            if (options.full_text_search_json != null) return .{ .issue = .{ .duplicate = arg } };
            options.full_text_search_json = args.next() orelse return .{ .issue = .{ .missing_value = arg } };
        } else if (std.mem.eql(u8, arg, "--semantic-search")) {
            if (options.semantic_search != null) return .{ .issue = .{ .duplicate = arg } };
            options.semantic_search = args.next() orelse return .{ .issue = .{ .missing_value = arg } };
        } else if (std.mem.eql(u8, arg, "--fields")) {
            if (options.fields_str != null) return .{ .issue = .{ .duplicate = arg } };
            options.fields_str = args.next() orelse return .{ .issue = .{ .missing_value = arg } };
        } else if (std.mem.eql(u8, arg, "--limit")) {
            if (options.limit != null) return .{ .issue = .{ .duplicate = arg } };
            const raw = args.next() orelse return .{ .issue = .{ .missing_value = arg } };
            const value = std.fmt.parseInt(i64, raw, 10) catch return .{ .issue = .{ .invalid_integer = .{ .flag = arg, .value = raw } } };
            if (value <= 0) return .{ .issue = .{ .non_positive = .{ .flag = arg, .value = raw } } };
            if (value > std.math.maxInt(u32)) return .{ .issue = .{ .too_large = .{ .flag = arg, .value = raw } } };
            options.limit = value;
        } else if (std.mem.eql(u8, arg, "--offset")) {
            if (options.offset != null) return .{ .issue = .{ .duplicate = arg } };
            const raw = args.next() orelse return .{ .issue = .{ .missing_value = arg } };
            const value = std.fmt.parseInt(i64, raw, 10) catch return .{ .issue = .{ .invalid_integer = .{ .flag = arg, .value = raw } } };
            if (value < 0) return .{ .issue = .{ .negative = .{ .flag = arg, .value = raw } } };
            if (value > std.math.maxInt(u32)) return .{ .issue = .{ .too_large = .{ .flag = arg, .value = raw } } };
            options.offset = value;
        } else if (std.mem.eql(u8, arg, "--indexes") or std.mem.eql(u8, arg, "-i")) {
            if (options.indexes_str != null) return .{ .issue = .{ .duplicate = arg } };
            options.indexes_str = args.next() orelse return .{ .issue = .{ .missing_value = arg } };
        } else if (std.mem.eql(u8, arg, "--filter-query")) {
            if (options.filter_query != null) return .{ .issue = .{ .duplicate = arg } };
            options.filter_query = args.next() orelse return .{ .issue = .{ .missing_value = arg } };
        } else if (std.mem.eql(u8, arg, "--exclusion-query")) {
            if (options.exclusion_query != null) return .{ .issue = .{ .duplicate = arg } };
            options.exclusion_query = args.next() orelse return .{ .issue = .{ .missing_value = arg } };
        } else if (std.mem.eql(u8, arg, "--aggregations")) {
            if (options.aggregations_json != null) return .{ .issue = .{ .duplicate = arg } };
            options.aggregations_json = args.next() orelse return .{ .issue = .{ .missing_value = arg } };
        } else if (std.mem.eql(u8, arg, "--reranker")) {
            if (options.reranker_json != null) return .{ .issue = .{ .duplicate = arg } };
            options.reranker_json = args.next() orelse return .{ .issue = .{ .missing_value = arg } };
        } else if (std.mem.eql(u8, arg, "--pruner")) {
            if (options.pruner_json != null) return .{ .issue = .{ .duplicate = arg } };
            options.pruner_json = args.next() orelse return .{ .issue = .{ .missing_value = arg } };
        } else {
            return .{ .issue = .{ .unknown = arg } };
        }
    }
    if (options.full_text_search != null and options.full_text_search_json != null) {
        return .{ .issue = .{ .conflicting_search = {} } };
    }
    if (options.semantic_search != null and (options.offset orelse 0) != 0) {
        return .{ .issue = .{ .semantic_offset = {} } };
    }
    if (options.semantic_search != null and options.table_name == null) {
        return .{ .issue = .{ .missing_table = {} } };
    }
    return .{ .value = options };
}

fn fatalQueryParseIssue(issue: QueryParseIssue) noreturn {
    switch (issue) {
        .missing_value => |flag| cli.fatal("{s} requires a value", .{flag}),
        .duplicate => |flag| cli.fatal("{s} may only be provided once", .{flag}),
        .unknown => |flag| cli.fatal("unknown query flag: {s}", .{flag}),
        .invalid_integer => |value| cli.fatal("invalid integer for {s}: {s}", .{ value.flag, value.value }),
        .non_positive => |value| cli.fatal("{s} must be greater than zero: {s}", .{ value.flag, value.value }),
        .negative => |value| cli.fatal("{s} must not be negative: {s}", .{ value.flag, value.value }),
        .too_large => |value| cli.fatal("{s} exceeds the supported maximum: {s}", .{ value.flag, value.value }),
        .conflicting_search => cli.fatal("only one of --full-text-search or --full-text-search-json may be provided", .{}),
        .semantic_offset => cli.fatal("--offset is not supported with --semantic-search", .{}),
        .missing_table => cli.fatal("--table is required", .{}),
    }
}

pub fn run(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const options = switch (parseQueryOptions(args.*)) {
        .value => |value| value,
        .issue => |issue| fatalQueryParseIssue(issue),
    };
    const table_name = options.table_name;
    const full_text_search = options.full_text_search;
    const full_text_search_json = options.full_text_search_json;
    const semantic_search = options.semantic_search;
    const fields_str = options.fields_str;
    const limit = options.limit;
    const offset = options.offset;
    const indexes_str = options.indexes_str;
    const filter_query = options.filter_query;
    const exclusion_query = options.exclusion_query;
    const aggregations_json = options.aggregations_json;
    const reranker_json = options.reranker_json;
    const pruner_json = options.pruner_json;

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
        if (semantic_search != null) warnIfSemanticIndexesAreNotReady(client, tbl, indexes);
        var resp = try client.queryTable(tbl, body);
        defer resp.deinit();
        if (resp.data) |data| {
            try cli.writeJson(allocator, io, data.value);
        }
    } else {
        var resp = try client.query(body);
        defer resp.deinit();
        if (resp.data) |data| {
            try cli.writeJson(allocator, io, data.value);
        }
    }
}

fn warnIfSemanticIndexesAreNotReady(
    client: *antfly_client.AntflyClient,
    table_name: []const u8,
    selected_indexes: ?[]const []const u8,
) void {
    var resp = client.listIndexesResponseWithTimeout(table_name, semantic_readiness_timeout_ms) catch |err| {
        std.debug.print("warning: unable to verify semantic index readiness: {s}\n", .{@errorName(err)});
        return;
    };
    defer resp.deinit();
    const parsed = resp.data orelse {
        std.debug.print("warning: unable to verify semantic index readiness (HTTP {d})\n", .{resp.status_code});
        return;
    };
    for (parsed.value) |index| {
        if (index.config.type != .embeddings) continue;
        if (selected_indexes) |selected| {
            var matched = false;
            for (selected) |name| {
                if (std.mem.eql(u8, name, index.config.name)) {
                    matched = true;
                    break;
                }
            }
            if (!matched) continue;
        }
        const stats = switch (index.status) {
            .embeddings_index_stats => |value| value,
            else => continue,
        };
        if (index_readiness.embeddingIndexReady(stats)) continue;

        const state = if (stats.coverage) |coverage|
            if (coverage.config_mismatch_group_count > 0) "config_mismatch" else stats.backfill_state orelse "not_ready"
        else if (stats.backfill_state != null and std.mem.eql(u8, stats.backfill_state.?, "ready"))
            "coverage_unavailable"
        else
            stats.backfill_state orelse if (stats.rebuilding orelse true) "running" else "not_ready";
        if (stats.coverage) |coverage| {
            std.debug.print(
                "warning: semantic index {s} is {s} (coverage {d}/{d}, complete={any}); results may be incomplete. Run `antfly index wait --table {s} --index {s}`.\n",
                .{ index.config.name, state, coverage.produced, coverage.source_total, coverage.complete, table_name, index.config.name },
            );
        } else if (stats.backfill_progress) |progress| {
            std.debug.print(
                "warning: semantic index {s} is {s} ({d:.1}%); results may be incomplete. Run `antfly index wait --table {s} --index {s}`.\n",
                .{ index.config.name, state, @max(0.0, @min(1.0, progress)) * 100.0, table_name, index.config.name },
            );
        } else {
            std.debug.print(
                "warning: semantic index {s} is {s}; results may be incomplete. Run `antfly index wait --table {s} --index {s}`.\n",
                .{ index.config.name, state, table_name, index.config.name },
            );
        }
    }
}

test "query parser fails closed for missing malformed duplicate and incompatible options" {
    var valid_argv = [_][*:0]const u8{ "--table", "docs", "--semantic-search", "alpha", "--limit", "5", "--indexes", "dense" };
    const valid = parseQueryOptions(std.process.Args.Iterator.init(.{ .vector = valid_argv[0..] }));
    try std.testing.expectEqualStrings("alpha", valid.value.semantic_search.?);
    try std.testing.expectEqual(@as(?i64, 5), valid.value.limit);

    var missing_argv = [_][*:0]const u8{"--semantic-search"};
    const missing = parseQueryOptions(std.process.Args.Iterator.init(.{ .vector = missing_argv[0..] }));
    try std.testing.expectEqualStrings("--semantic-search", missing.issue.missing_value);

    var malformed_argv = [_][*:0]const u8{ "--limit", "many" };
    const malformed = parseQueryOptions(std.process.Args.Iterator.init(.{ .vector = malformed_argv[0..] }));
    try std.testing.expectEqualStrings("many", malformed.issue.invalid_integer.value);

    var duplicate_argv = [_][*:0]const u8{ "--table", "docs", "-t", "other" };
    const duplicate = parseQueryOptions(std.process.Args.Iterator.init(.{ .vector = duplicate_argv[0..] }));
    try std.testing.expectEqualStrings("-t", duplicate.issue.duplicate);

    var offset_argv = [_][*:0]const u8{ "--semantic-search", "alpha", "--offset", "1" };
    const offset = parseQueryOptions(std.process.Args.Iterator.init(.{ .vector = offset_argv[0..] }));
    try std.testing.expect(offset.issue == .semantic_offset);

    var typo_argv = [_][*:0]const u8{ "--semantic-serach", "alpha" };
    const typo = parseQueryOptions(std.process.Args.Iterator.init(.{ .vector = typo_argv[0..] }));
    try std.testing.expectEqualStrings("--semantic-serach", typo.issue.unknown);

    var tableless_argv = [_][*:0]const u8{ "--semantic-search", "alpha" };
    const tableless = parseQueryOptions(std.process.Args.Iterator.init(.{ .vector = tableless_argv[0..] }));
    try std.testing.expect(tableless.issue == .missing_table);

    var global_full_text_argv = [_][*:0]const u8{ "--full-text-search", "alpha" };
    const global_full_text = parseQueryOptions(std.process.Args.Iterator.init(.{ .vector = global_full_text_argv[0..] }));
    try std.testing.expect(global_full_text == .value);
    try std.testing.expect(global_full_text.value.table_name == null);
}

test "semantic readiness requires compatible policy-aware coverage" {
    try std.testing.expect(!index_readiness.embeddingIndexReady(.{
        .index_type = .embeddings,
        .backfill_state = "ready",
        .rebuilding = false,
    }));
    try std.testing.expect(!index_readiness.embeddingIndexReady(.{
        .index_type = .embeddings,
        .backfill_state = "running",
        .rebuilding = true,
    }));

    try std.testing.expect(index_readiness.embeddingIndexReady(.{
        .index_type = .embeddings,
        .backfill_state = "ready",
        .rebuilding = false,
        .coverage = .{
            .policy = .strict,
            .observation_complete = true,
            .observation_incomplete_reasons = &.{},
            .config_fingerprint = "0123456789abcdef",
            .summary_ready = true,
            .config_mismatch_group_count = 0,
            .source_total = 1,
            .produced = 1,
            .skipped = 0,
            .terminal_failed = 0,
            .covered = 1,
            .settled = 1,
            .uncovered = 0,
            .pending = 0,
            .complete = true,
            .healthy = true,
            .degraded = false,
        },
    }));

    try std.testing.expect(!index_readiness.embeddingIndexReady(.{
        .index_type = .embeddings,
        .backfill_state = "ready",
        .rebuilding = false,
        .coverage = .{
            .policy = .strict,
            .observation_complete = true,
            .observation_incomplete_reasons = &.{.config_mismatch},
            .config_fingerprint = "0123456789abcdef",
            .summary_ready = true,
            .config_mismatch_group_count = 1,
            .source_total = 1,
            .produced = 1,
            .skipped = 0,
            .terminal_failed = 0,
            .covered = 1,
            .settled = 1,
            .uncovered = 0,
            .pending = 0,
            .complete = true,
            .healthy = false,
            .degraded = true,
        },
    }));
}

const LookupOptions = struct {
    table_name: ?[]const u8 = null,
    key: ?[]const u8 = null,
};

const LookupParseIssue = union(enum) {
    missing_value: []const u8,
    duplicate: []const u8,
    unknown: []const u8,
};

const LookupParseResult = union(enum) { value: LookupOptions, issue: LookupParseIssue };

fn parseLookupOptions(iterator: std.process.Args.Iterator) LookupParseResult {
    var args = iterator;
    var options: LookupOptions = .{};
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
            if (options.table_name != null) return .{ .issue = .{ .duplicate = arg } };
            options.table_name = args.next() orelse return .{ .issue = .{ .missing_value = arg } };
        } else if (std.mem.eql(u8, arg, "--key") or std.mem.eql(u8, arg, "-k")) {
            if (options.key != null) return .{ .issue = .{ .duplicate = arg } };
            options.key = args.next() orelse return .{ .issue = .{ .missing_value = arg } };
        } else {
            return .{ .issue = .{ .unknown = arg } };
        }
    }
    return .{ .value = options };
}

fn fatalLookupParseIssue(issue: LookupParseIssue) noreturn {
    switch (issue) {
        .missing_value => |flag| cli.fatal("{s} requires a value", .{flag}),
        .duplicate => |flag| cli.fatal("{s} may only be provided once", .{flag}),
        .unknown => |flag| cli.fatal("unknown lookup option: {s}", .{flag}),
    }
}

pub fn lookup(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const options = switch (parseLookupOptions(args.*)) {
        .value => |value| value,
        .issue => |issue| fatalLookupParseIssue(issue),
    };

    const tbl = options.table_name orelse cli.fatal("--table is required", .{});
    const k = options.key orelse cli.fatal("--key is required", .{});

    var resp = try client.lookupKey(tbl, k, .{});
    defer resp.deinit();
    if (resp.data) |data| {
        try cli.writeJson(allocator, io, data.value);
    }
}

test "lookup parser rejects unknown duplicate and missing options" {
    var valid_argv = [_][*:0]const u8{ "--table", "docs", "--key", "doc:a" };
    const valid = parseLookupOptions(std.process.Args.Iterator.init(.{ .vector = valid_argv[0..] }));
    try std.testing.expectEqualStrings("doc:a", valid.value.key.?);

    var unknown_argv = [_][*:0]const u8{ "--table", "docs", "--key", "doc:a", "--typo" };
    const unknown = parseLookupOptions(std.process.Args.Iterator.init(.{ .vector = unknown_argv[0..] }));
    try std.testing.expectEqualStrings("--typo", unknown.issue.unknown);

    var duplicate_argv = [_][*:0]const u8{ "--key", "a", "-k", "b" };
    const duplicate = parseLookupOptions(std.process.Args.Iterator.init(.{ .vector = duplicate_argv[0..] }));
    try std.testing.expectEqualStrings("-k", duplicate.issue.duplicate);

    var missing_argv = [_][*:0]const u8{"--key"};
    const missing = parseLookupOptions(std.process.Args.Iterator.init(.{ .vector = missing_argv[0..] }));
    try std.testing.expectEqualStrings("--key", missing.issue.missing_value);
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
        .ignore_unknown_fields = false,
    }) catch |err| {
        cli.fatal("invalid JSON for {s}: {}", .{ flag, err });
    };
}
