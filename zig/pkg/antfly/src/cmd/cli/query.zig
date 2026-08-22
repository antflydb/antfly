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
const httpx = antfly_client.httpx;
const cli = @import("mod.zig");
const index_readiness = @import("index_readiness.zig");

fn shouldRunSemanticReadinessAdvisory(
    semantic_search: ?[]const u8,
    indexes: ?[]const []const u8,
) bool {
    // With implicit selection only the server knows which index actually ran;
    // warning about every table index creates false positives. The query
    // response remains authoritative in that case.
    return semantic_search != null and indexes != null;
}

const SemanticReadinessAdvisory = struct {
    group: std.Io.Group = .init,
    active: bool = false,

    fn start(
        self: *@This(),
        io: std.Io,
        client: *antfly_client.AntflyClient,
        table_name: []const u8,
        indexes: []const []const u8,
    ) !void {
        const Task = struct {
            fn run(
                _: std.Io,
                task_client: *antfly_client.AntflyClient,
                task_table_name: []const u8,
                task_indexes: []const []const u8,
            ) std.Io.Cancelable!void {
                index_readiness.warnIfSemanticIndexesAreNotReady(task_client, task_table_name, task_indexes);
            }
        };
        try self.group.concurrent(io, Task.run, .{ io, client, table_name, indexes });
        self.active = true;
    }

    fn cancel(self: *@This(), io: std.Io) void {
        if (!self.active) return;
        self.group.cancel(io);
        self.active = false;
    }

    fn awaitOnQueryFailure(self: *@This(), io: std.Io) bool {
        if (!self.active) return false;
        self.group.await(io) catch {};
        self.active = false;
        return true;
    }
};

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
        // Readiness is diagnostic, so overlap it with the data-plane request
        // and cancel it as soon as a successful query returns. On query
        // failure, allow the bounded advisory to finish so users retain useful
        // diagnostics for missing or incompatible explicitly selected indexes.
        var advisory: SemanticReadinessAdvisory = .{};
        defer advisory.cancel(io);
        if (shouldRunSemanticReadinessAdvisory(semantic_search, indexes)) {
            // Never fail a real query because its best-effort diagnostic could
            // not be scheduled on this I/O backend.
            advisory.start(io, client, tbl, indexes.?) catch {};
        }
        var resp = client.queryTable(tbl, body) catch |err| {
            // Explicit selection overlaps the bounded readiness lookup with
            // the query. With implicit selection, defer the lookup until the
            // query actually fails: successful requests keep the fast path,
            // while failures still explain an empty table index catalog.
            if (!advisory.awaitOnQueryFailure(io) and semantic_search != null) {
                index_readiness.warnIfSemanticIndexesAreNotReady(client, tbl, indexes);
            }
            return err;
        };
        advisory.cancel(io);
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

test "semantic readiness advisory only observes explicitly selected indexes" {
    const selected = [_][]const u8{"dense"};
    try std.testing.expect(shouldRunSemanticReadinessAdvisory("alpha", &selected));
    try std.testing.expect(!shouldRunSemanticReadinessAdvisory("alpha", null));
    try std.testing.expect(!shouldRunSemanticReadinessAdvisory(null, &selected));
}

test "successful semantic query cancels a slow readiness advisory" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    const State = struct {
        var test_io: std.Io = undefined;
        var status_entered = std.atomic.Value(bool).init(false);
        var release_status = std.atomic.Value(bool).init(false);

        fn statusRequest(_: httpx.testing_mod.RequestInfo) !void {
            status_entered.store(true, .release);
            while (!release_status.load(.acquire)) {
                try test_io.sleep(std.Io.Duration.fromMilliseconds(1), .awake);
            }
        }

        fn queryRequest(_: httpx.testing_mod.RequestInfo) !void {
            // Ensure the advisory owns a live, deliberately stalled request
            // before the data-plane response is allowed to complete.
            while (!status_entered.load(.acquire)) {
                try test_io.sleep(std.Io.Duration.fromMilliseconds(1), .awake);
            }
        }
    };
    State.test_io = io;
    State.status_entered.store(false, .release);
    State.release_status.store(false, .release);

    var server = try httpx.TestServer.start(alloc, io, &.{
        .{
            .method = .GET,
            .path = "/db/v1/tables/docs/indexes",
            .respond = .{ .body = "[]" },
            .assert_request = State.statusRequest,
        },
        .{
            .method = .POST,
            .path = "/db/v1/tables/docs/query",
            .respond = .{ .body = "{\"responses\":[]}" },
            .assert_request = State.queryRequest,
        },
    });
    defer server.deinit();

    var http = httpx.Client.initWithConfig(alloc, io, .{
        .keep_alive = false,
        .retry_policy = .{ .max_retries = 0 },
    });
    defer http.deinit();
    var client = try antfly_client.AntflyClient.init(alloc, &http, server.baseUrl());
    defer client.deinit();

    const ServerTask = struct {
        fn runServer(test_server: *httpx.TestServer) std.Io.Cancelable!void {
            test_server.handleOne() catch return;
        }
    };
    var server_group: std.Io.Group = .init;
    try server_group.concurrent(io, ServerTask.runServer, .{&server});
    try server_group.concurrent(io, ServerTask.runServer, .{&server});
    var server_group_active = true;
    defer if (server_group_active) server_group.cancel(io);

    var query_succeeded = std.atomic.Value(bool).init(false);
    var query_failed = std.atomic.Value(bool).init(false);
    const QueryTask = struct {
        fn runQuery(
            test_io: std.Io,
            test_client: *antfly_client.AntflyClient,
            succeeded: *std.atomic.Value(bool),
            failed: *std.atomic.Value(bool),
        ) std.Io.Cancelable!void {
            var argv = [_][*:0]const u8{
                "--table",
                "docs",
                "--semantic-search",
                "alpha",
                "--indexes",
                "dense",
            };
            var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
            run(std.testing.allocator, test_io, test_client, &args) catch {
                failed.store(true, .release);
                return;
            };
            succeeded.store(true, .release);
        }
    };
    var query_group: std.Io.Group = .init;
    try query_group.concurrent(io, QueryTask.runQuery, .{ io, &client, &query_succeeded, &query_failed });
    var query_group_active = true;
    defer if (query_group_active) query_group.cancel(io);

    // The status handler remains blocked until after this loop. Completion
    // therefore proves that a successful query canceled instead of awaiting
    // the advisory request.
    for (0..500) |_| {
        if (query_succeeded.load(.acquire) or query_failed.load(.acquire)) break;
        try io.sleep(std.Io.Duration.fromMilliseconds(1), .awake);
    }
    const completed_before_status = query_succeeded.load(.acquire);
    State.release_status.store(true, .release);
    try query_group.await(io);
    query_group_active = false;
    server_group.await(io) catch {};
    server_group_active = false;
    try std.testing.expect(!query_failed.load(.acquire));
    try std.testing.expect(completed_before_status);
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
